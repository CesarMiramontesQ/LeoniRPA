"""
Importa datos de CrossRef.xlsx a la tabla cross_reference.

Uso (desde la raíz del proyecto):
  python scripts/import_cross_reference_desde_excel.py
  python scripts/import_cross_reference_desde_excel.py --file CrossRef.xlsx --sheet Datos
  python scripts/import_cross_reference_desde_excel.py --dry-run

Comportamiento:
  - Lee columnas:
      - Customer (obligatoria)
      - Material (obligatoria)
      - Customer Material (obligatoria)
  - Hace upsert por PK compuesta (customer, material, customer_material):
      - Si no existe: inserta
      - Si existe: actualiza updated_at
"""

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.base import AsyncSessionLocal, engine  # noqa: E402


UPSERT_SQL = text(
    """
    INSERT INTO cross_reference (
        customer,
        material,
        customer_material,
        updated_at
    ) VALUES (
        :customer,
        :material,
        :customer_material,
        now()
    )
    ON CONFLICT (customer, material, customer_material) DO UPDATE
    SET updated_at = now()
    """
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Importar Cross Reference desde Excel.")
    parser.add_argument("--file", default="CrossRef.xlsx", help="Ruta del archivo Excel (default: CrossRef.xlsx)")
    parser.add_argument("--sheet", default="Datos", help="Nombre de hoja (default: Datos)")
    parser.add_argument("--dry-run", action="store_true", help="Simula la importación sin guardar cambios")
    parser.add_argument("--chunk-size", type=int, default=2000, help="Tamaño de lote para upsert (default: 2000)")
    return parser.parse_args()


def normalize_col(name: Any) -> str:
    if name is None:
        return ""
    return " ".join(str(name).strip().lower().split())


def as_clean_string(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    # Evita serialización tipo 2005203.0 cuando viene numérico desde Excel.
    if s.endswith(".0"):
        s = s[:-2]
    return s


async def run_import(file_path: Path, sheet_name: str, dry_run: bool, chunk_size: int) -> None:
    if not file_path.is_file():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print("=" * 68)
    print("Importar Cross Reference desde Excel")
    print("=" * 68)
    print(f"Archivo: {file_path}")
    print(f"Hoja:    {sheet_name}")
    print(f"Modo:    {'DRY-RUN (sin guardar)' if dry_run else 'ESCRITURA'}")
    print(f"Lote:    {chunk_size}")

    excel = pd.ExcelFile(file_path)
    hojas = list(excel.sheet_names)
    if sheet_name in hojas:
        hoja_objetivo = sheet_name
    elif hojas:
        hoja_objetivo = hojas[0]
        print(f"Hoja '{sheet_name}' no existe. Se usará: {hoja_objetivo}")
    else:
        raise ValueError("El archivo no contiene hojas.")

    df = pd.read_excel(file_path, sheet_name=hoja_objetivo, dtype=object)
    if df.empty:
        print("\nEl archivo no contiene filas.")
        return

    col_map = {normalize_col(c): c for c in df.columns}
    col_customer = col_map.get("customer")
    col_material = col_map.get("material")
    col_customer_material = col_map.get("customer material") or col_map.get("customer_material")

    if not col_customer or not col_material or not col_customer_material:
        raise ValueError(
            "No se encontraron columnas requeridas: Customer, Material, Customer Material."
        )

    total_filas = len(df)
    filas_invalidas = 0
    duplicados_archivo = 0
    seen = set()
    candidatos = []

    for _, row in df.iterrows():
        customer = as_clean_string(row.get(col_customer))
        material = as_clean_string(row.get(col_material))
        customer_material = as_clean_string(row.get(col_customer_material))

        if not customer or not material or not customer_material:
            filas_invalidas += 1
            continue

        key = (customer, material, customer_material)
        if key in seen:
            duplicados_archivo += 1
            continue

        seen.add(key)
        candidatos.append(
            {
                "customer": customer,
                "material": material,
                "customer_material": customer_material,
            }
        )

    upserts = 0
    async with AsyncSessionLocal() as db:
        try:
            step = max(1, int(chunk_size or 1))
            for i in range(0, len(candidatos), step):
                chunk = candidatos[i : i + step]
                await db.execute(UPSERT_SQL, chunk)
                upserts += len(chunk)

            if dry_run:
                await db.rollback()
            else:
                await db.commit()
        except Exception:
            await db.rollback()
            raise

    print("\n" + "=" * 68)
    print("RESUMEN")
    print("=" * 68)
    print(f"  Total filas en Excel:                 {total_filas}")
    print(f"  Filas inválidas (campos vacíos):      {filas_invalidas}")
    print(f"  Duplicados en archivo:                {duplicados_archivo}")
    print(f"  Candidatos únicos:                    {len(candidatos)}")
    print(f"  Upserts ejecutados:                   {upserts}")
    print("=" * 68)


async def main() -> None:
    args = parse_args()
    file_path = Path(args.file)
    if not file_path.is_absolute():
        file_path = (ROOT / file_path).resolve()
    await run_import(
        file_path=file_path,
        sheet_name=args.sheet,
        dry_run=args.dry_run,
        chunk_size=args.chunk_size,
    )
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
