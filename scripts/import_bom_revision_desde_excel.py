"""
Importa datos de Excel a la tabla bom_revision.

Uso (desde la raíz del proyecto):
  python scripts/import_bom_revision_desde_excel.py
  python scripts/import_bom_revision_desde_excel.py --file bom_revision.xlsx --sheet Sheet1
  python scripts/import_bom_revision_desde_excel.py --dry-run

Comportamiento:
  - Lee columnas (acepta espacios/underscore):
      - bom id (obligatoria)
      - revision no (obligatoria)
      - effective from (obligatoria)
      - effective to (opcional)
      - source (opcional)
      - hash (obligatoria)
      - detalle (opcional; JSON string)
  - Hace upsert por (bom_id, revision_no):
      - Si no existe: inserta
      - Si existe: actualiza campos modificados
"""

import argparse
import asyncio
import json
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select, text

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.base import AsyncSessionLocal, engine  # noqa: E402
from app.db.models import Bom, BomRevision  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Importar bom_revision desde un archivo Excel.")
    parser.add_argument("--file", default="bom_revision.xlsx", help="Ruta del archivo Excel (default: bom_revision.xlsx)")
    parser.add_argument("--sheet", default="Sheet1", help="Nombre de hoja (default: Sheet1)")
    parser.add_argument("--dry-run", action="store_true", help="Simula la importación sin guardar cambios")
    return parser.parse_args()


def normalize_col(name: Any) -> str:
    if name is None:
        return ""
    return " ".join(str(name).strip().lower().replace("_", " ").split())


def is_nan(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def parse_int(value: Any) -> int | None:
    if is_nan(value):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def parse_text(value: Any) -> str | None:
    if is_nan(value):
        return None
    s = str(value).strip()
    return s or None


def parse_date(value: Any) -> date | None:
    if is_nan(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None


def parse_detalle(value: Any) -> dict[str, Any]:
    if is_nan(value):
        return {}
    if isinstance(value, dict):
        return value
    s = str(value).strip()
    if not s:
        return {}
    try:
        parsed = json.loads(s)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


async def run_import(file_path: Path, sheet_name: str, dry_run: bool) -> None:
    if not file_path.is_file():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print("=" * 72)
    print("Importar BOM revisions desde Excel")
    print("=" * 72)
    print(f"Archivo: {file_path}")
    print(f"Hoja:    {sheet_name}")
    print(f"Modo:    {'DRY-RUN (sin guardar)' if dry_run else 'ESCRITURA'}")

    df = pd.read_excel(file_path, sheet_name=sheet_name)
    if df.empty:
        print("\nEl archivo no contiene filas.")
        return

    col_map = {normalize_col(c): c for c in df.columns}
    col_bom_id = col_map.get("bom id") or col_map.get("bom_id")
    col_revision_no = col_map.get("revision no") or col_map.get("revision_no")
    col_effective_from = col_map.get("effective from") or col_map.get("effective_from")
    col_effective_to = col_map.get("effective to") or col_map.get("effective_to")
    col_source = col_map.get("source")
    col_hash = col_map.get("hash")
    col_detalle = col_map.get("detalle")

    missing = []
    if not col_bom_id:
        missing.append("bom id")
    if not col_revision_no:
        missing.append("revision no")
    if not col_effective_from:
        missing.append("effective from")
    if not col_hash:
        missing.append("hash")
    if missing:
        raise ValueError(f"No se encontraron columnas obligatorias: {', '.join(missing)}")

    total_filas = len(df)
    filas_invalidas = 0
    filas_fechas_invalidas = 0
    candidatos: list[dict[str, Any]] = []
    seen = set()
    duplicados_archivo = 0

    for _, row in df.iterrows():
        bom_id = parse_int(row.get(col_bom_id))
        revision_no = parse_int(row.get(col_revision_no))
        effective_from = parse_date(row.get(col_effective_from))
        effective_to = parse_date(row.get(col_effective_to)) if col_effective_to else None
        hash_value = parse_text(row.get(col_hash))

        if bom_id is None or revision_no is None or effective_from is None or not hash_value:
            filas_invalidas += 1
            continue

        if effective_to is not None and effective_to <= effective_from:
            filas_fechas_invalidas += 1
            continue

        rec = {
            "bom_id": bom_id,
            "revision_no": revision_no,
            "effective_from": effective_from,
            "effective_to": effective_to,
            "source": parse_text(row.get(col_source)) if col_source else None,
            "hash": hash_value,
            "detalle": parse_detalle(row.get(col_detalle)) if col_detalle else {},
        }

        key = (bom_id, revision_no)
        if key in seen:
            duplicados_archivo += 1
            for i in range(len(candidatos) - 1, -1, -1):
                if candidatos[i]["bom_id"] == bom_id and candidatos[i]["revision_no"] == revision_no:
                    candidatos[i] = rec
                    break
            continue

        seen.add(key)
        candidatos.append(rec)

    insertados = 0
    actualizados = 0
    sin_cambios = 0
    errores = 0
    omitidos_fk = 0

    async with AsyncSessionLocal() as db:
        try:
            await db.execute(
                text(
                    "SELECT setval(pg_get_serial_sequence('bom_revision', 'id'), "
                    "COALESCE((SELECT MAX(id) FROM bom_revision), 1))"
                )
            )
            if not dry_run:
                await db.commit()
            else:
                await db.rollback()

            bom_ids = {rec["bom_id"] for rec in candidatos}
            existing_bom_ids = set(
                (
                    await db.execute(
                        select(Bom.id).where(Bom.id.in_(bom_ids))
                    )
                ).scalars().all()
            )

            for rec in candidatos:
                if rec["bom_id"] not in existing_bom_ids:
                    omitidos_fk += 1
                    continue

                savepoint = await db.begin_nested()
                try:
                    existing = (
                        await db.execute(
                            select(BomRevision).where(
                                BomRevision.bom_id == rec["bom_id"],
                                BomRevision.revision_no == rec["revision_no"],
                            )
                        )
                    ).scalar_one_or_none()

                    if existing is None:
                        db.add(
                            BomRevision(
                                bom_id=rec["bom_id"],
                                revision_no=rec["revision_no"],
                                effective_from=rec["effective_from"],
                                effective_to=rec["effective_to"],
                                source=rec["source"],
                                hash=rec["hash"],
                                detalle=rec["detalle"],
                            )
                        )
                        insertados += 1
                    else:
                        changed = False
                        for field in ("effective_from", "effective_to", "source", "hash", "detalle"):
                            if getattr(existing, field) != rec[field]:
                                setattr(existing, field, rec[field])
                                changed = True
                        if changed:
                            actualizados += 1
                        else:
                            sin_cambios += 1

                    await db.flush()
                    await savepoint.commit()
                except Exception:
                    await savepoint.rollback()
                    errores += 1

            if dry_run:
                await db.rollback()
            else:
                await db.commit()
        except Exception:
            await db.rollback()
            raise

    print("\n" + "=" * 72)
    print("RESUMEN")
    print("=" * 72)
    print(f"  Total filas en Excel:                 {total_filas}")
    print(f"  Filas inválidas:                      {filas_invalidas}")
    print(f"  Filas con fechas inválidas:           {filas_fechas_invalidas}")
    print(f"  Duplicados en archivo:                {duplicados_archivo}")
    print(f"  Candidatos únicos:                    {len(candidatos)}")
    print(f"  Omitidos por FK inexistente:          {omitidos_fk}")
    print(f"  Insertados:                           {insertados}")
    print(f"  Actualizados:                         {actualizados}")
    print(f"  Sin cambios:                          {sin_cambios}")
    print(f"  Errores:                              {errores}")
    print("=" * 72)


async def main() -> None:
    args = parse_args()
    file_path = Path(args.file)
    if not file_path.is_absolute():
        file_path = (ROOT / file_path).resolve()
    await run_import(file_path=file_path, sheet_name=args.sheet, dry_run=args.dry_run)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
