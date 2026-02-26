"""
Importa datos de Excel a la tabla bom_item.

Uso (desde la raíz del proyecto):
  python scripts/import_bom_items_desde_excel.py
  python scripts/import_bom_items_desde_excel.py --file bom_item.xlsx --sheet Sheet1
  python scripts/import_bom_items_desde_excel.py --dry-run

Comportamiento:
  - Lee columnas del Excel (acepta variantes con espacios/underscore):
      - Bom revision ID (obligatoria)
      - Componente ID (obligatoria)
      - qty (obligatoria)
      - Item No (opcional)
      - measure (opcional)
      - origin (opcional)
      - detalle (opcional, JSON string)
      - Comm code (opcional)
  - Hace upsert por (bom_revision_id, componente_id):
      - Si no existe: inserta el registro
      - Si existe: actualiza columnas modificadas
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
from app.db.models import BomItem, BomRevision, Parte  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Importar bom_item desde un archivo Excel.")
    parser.add_argument("--file", default="bom_item.xlsx", help="Ruta del archivo Excel (default: bom_item.xlsx)")
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


def parse_comm_code(value: Any) -> str | None:
    if is_nan(value):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
        return str(value)
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
    print("Importar BOM items desde Excel")
    print("=" * 72)
    print(f"Archivo: {file_path}")
    print(f"Hoja:    {sheet_name}")
    print(f"Modo:    {'DRY-RUN (sin guardar)' if dry_run else 'ESCRITURA'}")

    df = pd.read_excel(file_path, sheet_name=sheet_name)
    if df.empty:
        print("\nEl archivo no contiene filas.")
        return

    col_map = {normalize_col(c): c for c in df.columns}
    col_bom_revision_id = col_map.get("bom revision id") or col_map.get("bom_revision_id")
    col_componente_id = col_map.get("componente id") or col_map.get("componente_id")
    col_qty = col_map.get("qty") or col_map.get("cantidad")

    col_item_no = col_map.get("item no") or col_map.get("item_no")
    col_measure = col_map.get("measure")
    col_origin = col_map.get("origin")
    col_detalle = col_map.get("detalle")
    col_comm_code = col_map.get("comm code") or col_map.get("comm_code")

    missing = []
    if not col_bom_revision_id:
        missing.append("Bom revision ID")
    if not col_componente_id:
        missing.append("Componente ID")
    if not col_qty:
        missing.append("qty")
    if missing:
        raise ValueError(f"No se encontraron columnas obligatorias: {', '.join(missing)}")

    total_filas = len(df)
    filas_invalidas = 0
    candidatos: list[dict[str, Any]] = []
    seen = set()
    duplicados_archivo = 0

    for _, row in df.iterrows():
        bom_revision_id = parse_int(row.get(col_bom_revision_id))
        componente_id = parse_int(row.get(col_componente_id))
        qty = parse_decimal(row.get(col_qty))

        if bom_revision_id is None or componente_id is None or qty is None:
            filas_invalidas += 1
            continue

        rec = {
            "bom_revision_id": bom_revision_id,
            "componente_id": componente_id,
            "item_no": parse_text(row.get(col_item_no)) if col_item_no else None,
            "qty": qty,
            "measure": parse_text(row.get(col_measure)) if col_measure else None,
            "origin": parse_text(row.get(col_origin)) if col_origin else None,
            "detalle": parse_detalle(row.get(col_detalle)) if col_detalle else {},
            "comm_code": parse_comm_code(row.get(col_comm_code)) if col_comm_code else None,
        }

        key = (bom_revision_id, componente_id)
        if key in seen:
            duplicados_archivo += 1
            # Conserva la última ocurrencia en el archivo.
            for i in range(len(candidatos) - 1, -1, -1):
                if (
                    candidatos[i]["bom_revision_id"] == bom_revision_id
                    and candidatos[i]["componente_id"] == componente_id
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
            # Ajusta secuencia por seguridad (si hubo cargas manuales previas).
            await db.execute(
                text("SELECT setval(pg_get_serial_sequence('bom_item', 'id'), COALESCE((SELECT MAX(id) FROM bom_item), 1))")
            )
            if not dry_run:
                await db.commit()
            else:
                await db.rollback()

            rev_ids = {rec["bom_revision_id"] for rec in candidatos}
            comp_ids = {rec["componente_id"] for rec in candidatos}

            existing_rev_ids = set(
                (
                    await db.execute(
                        select(BomRevision.id).where(BomRevision.id.in_(rev_ids))
                    )
                ).scalars().all()
            )
            existing_comp_ids = set(
                (
                    await db.execute(
                        select(Parte.id).where(Parte.id.in_(comp_ids))
                    )
                ).scalars().all()
            )

            for rec in candidatos:
                if rec["bom_revision_id"] not in existing_rev_ids or rec["componente_id"] not in existing_comp_ids:
                    omitidos_fk += 1
                    continue
                savepoint = await db.begin_nested()
                try:
                    existing = (
                        await db.execute(
                            select(BomItem).where(
                                BomItem.bom_revision_id == rec["bom_revision_id"],
                                BomItem.componente_id == rec["componente_id"],
                            )
                        )
                    ).scalar_one_or_none()

                    if existing is None:
                        db.add(
                            BomItem(
                                bom_revision_id=rec["bom_revision_id"],
                                componente_id=rec["componente_id"],
                                item_no=rec["item_no"],
                                qty=rec["qty"],
                                measure=rec["measure"],
                                origin=rec["origin"],
                                detalle=rec["detalle"],
                                comm_code=rec["comm_code"],
                            )
                        )
                        insertados += 1
                    else:
                        changed = False
                        for field in ("item_no", "qty", "measure", "origin", "detalle", "comm_code"):
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
