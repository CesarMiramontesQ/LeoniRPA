"""
Importa datos de Excel a la tabla bom.

Uso (desde la raíz del proyecto):
  python scripts/import_bom_desde_excel.py
  python scripts/import_bom_desde_excel.py --file boom.xlsx --sheet Sheet1
  python scripts/import_bom_desde_excel.py --dry-run

Comportamiento:
  - Lee columnas (acepta espacios/underscore):
      - parte id (obligatoria)
      - plant (obligatoria)
      - usage (obligatoria)
      - alternative (obligatoria)
      - base qty (opcional)
      - reqd qty (opcional)
      - base unit (opcional)
      - detalle (opcional; JSON string)
  - Hace upsert por (parte_id, plant, usage, alternative):
      - Si no existe: inserta
      - Si existe: actualiza campos modificados
"""

import argparse
import asyncio
import json
import math
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select, text

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.base import AsyncSessionLocal, engine  # noqa: E402
from app.db.models import Bom, Parte  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Importar BOM desde un archivo Excel.")
    parser.add_argument("--file", default="boom.xlsx", help="Ruta del archivo Excel (default: boom.xlsx)")
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


def parse_decimal(value: Any) -> Decimal | None:
    if is_nan(value):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def parse_text(value: Any) -> str | None:
    if is_nan(value):
        return None
    s = str(value).strip()
    return s or None


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
    print("Importar BOM desde Excel")
    print("=" * 72)
    print(f"Archivo: {file_path}")
    print(f"Hoja:    {sheet_name}")
    print(f"Modo:    {'DRY-RUN (sin guardar)' if dry_run else 'ESCRITURA'}")

    df = pd.read_excel(file_path, sheet_name=sheet_name)
    if df.empty:
        print("\nEl archivo no contiene filas.")
        return

    col_map = {normalize_col(c): c for c in df.columns}
    col_parte_id = col_map.get("parte id") or col_map.get("parte_id")
    col_plant = col_map.get("plant")
    col_usage = col_map.get("usage")
    col_alternative = col_map.get("alternative")
    col_base_qty = col_map.get("base qty") or col_map.get("base_qty")
    col_reqd_qty = col_map.get("reqd qty") or col_map.get("reqd_qty")
    col_base_unit = col_map.get("base unit") or col_map.get("base_unit")
    col_detalle = col_map.get("detalle")

    missing = []
    if not col_parte_id:
        missing.append("parte id")
    if not col_plant:
        missing.append("plant")
    if not col_usage:
        missing.append("usage")
    if not col_alternative:
        missing.append("alternative")
    if missing:
        raise ValueError(f"No se encontraron columnas obligatorias: {', '.join(missing)}")

    total_filas = len(df)
    filas_invalidas = 0
    candidatos: list[dict[str, Any]] = []
    seen = set()
    duplicados_archivo = 0

    for _, row in df.iterrows():
        parte_id = parse_int(row.get(col_parte_id))
        plant = parse_text(row.get(col_plant))
        usage = parse_text(row.get(col_usage))
        alternative = parse_text(row.get(col_alternative))

        if parte_id is None or not plant or not usage or not alternative:
            filas_invalidas += 1
            continue

        rec = {
            "parte_id": parte_id,
            "plant": plant,
            "usage": usage,
            "alternative": alternative,
            "base_qty": parse_decimal(row.get(col_base_qty)) if col_base_qty else None,
            "reqd_qty": parse_decimal(row.get(col_reqd_qty)) if col_reqd_qty else None,
            "base_unit": parse_text(row.get(col_base_unit)) if col_base_unit else None,
            "detalle": parse_detalle(row.get(col_detalle)) if col_detalle else {},
        }

        key = (parte_id, plant, usage, alternative)
        if key in seen:
            duplicados_archivo += 1
            for i in range(len(candidatos) - 1, -1, -1):
                if (
                    candidatos[i]["parte_id"] == parte_id
                    and candidatos[i]["plant"] == plant
                    and candidatos[i]["usage"] == usage
                    and candidatos[i]["alternative"] == alternative
                ):
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
                text("SELECT setval(pg_get_serial_sequence('bom', 'id'), COALESCE((SELECT MAX(id) FROM bom), 1))")
            )
            if not dry_run:
                await db.commit()
            else:
                await db.rollback()

            parte_ids = {rec["parte_id"] for rec in candidatos}
            existing_parte_ids = set(
                (
                    await db.execute(
                        select(Parte.id).where(Parte.id.in_(parte_ids))
                    )
                ).scalars().all()
            )

            for rec in candidatos:
                if rec["parte_id"] not in existing_parte_ids:
                    omitidos_fk += 1
                    continue

                savepoint = await db.begin_nested()
                try:
                    existing = (
                        await db.execute(
                            select(Bom).where(
                                Bom.parte_id == rec["parte_id"],
                                Bom.plant == rec["plant"],
                                Bom.usage == rec["usage"],
                                Bom.alternative == rec["alternative"],
                            )
                        )
                    ).scalar_one_or_none()

                    if existing is None:
                        db.add(
                            Bom(
                                parte_id=rec["parte_id"],
                                plant=rec["plant"],
                                usage=rec["usage"],
                                alternative=rec["alternative"],
                                base_qty=rec["base_qty"],
                                reqd_qty=rec["reqd_qty"],
                                base_unit=rec["base_unit"],
                                detalle=rec["detalle"],
                            )
                        )
                        insertados += 1
                    else:
                        changed = False
                        for field in ("base_qty", "reqd_qty", "base_unit", "detalle"):
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
