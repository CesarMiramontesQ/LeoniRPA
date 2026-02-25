"""
Importa números de parte desde Excel a la tabla partes, forzando valido=True.

Uso (desde la raíz del proyecto):
  python scripts/import_partes_actualizado_true.py
  python scripts/import_partes_actualizado_true.py --file faltantes_sheet2_vs_sheet1.xlsx --sheet Faltantes
  python scripts/import_partes_actualizado_true.py --dry-run

Comportamiento:
  - Lee columna de número de parte (obligatoria):
      - numero_parte_faltante / Numero de parte / numero_parte / part_no
  - Hace upsert por partes.numero_parte:
      - Si no existe: inserta (numero_parte, descripcion=None, valido=True)
      - Si existe: actualiza solo valido=True (no toca descripcion)
"""

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.base import AsyncSessionLocal, engine  # noqa: E402
from app.db.models import Parte  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Importar números de parte desde Excel y forzar valido=True."
    )
    parser.add_argument(
        "--file",
        default="faltantes_sheet2_vs_sheet1.xlsx",
        help="Ruta del archivo Excel (default: faltantes_sheet2_vs_sheet1.xlsx)",
    )
    parser.add_argument("--sheet", default="Faltantes", help="Nombre de hoja (default: Faltantes)")
    parser.add_argument("--dry-run", action="store_true", help="Simula la importación sin guardar cambios")
    return parser.parse_args()


def normalize_col(name: Any) -> str:
    if name is None:
        return ""
    return " ".join(str(name).strip().lower().split())


async def run_import(file_path: Path, sheet_name: str, dry_run: bool) -> None:
    if not file_path.is_file():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print("=" * 68)
    print("Importar partes desde Excel (valido=True)")
    print("=" * 68)
    print(f"Archivo: {file_path}")
    print(f"Modo:    {'DRY-RUN (sin guardar)' if dry_run else 'ESCRITURA'}")

    excel = pd.ExcelFile(file_path)
    hojas = list(excel.sheet_names)
    if sheet_name in hojas:
        hoja_objetivo = sheet_name
    elif "Faltantes" in hojas:
        hoja_objetivo = "Faltantes"
        print(f"Hoja solicitada '{sheet_name}' no existe. Se usará: {hoja_objetivo}")
    elif hojas:
        hoja_objetivo = hojas[0]
        print(f"Hoja solicitada '{sheet_name}' no existe. Se usará la primera hoja: {hoja_objetivo}")
    else:
        raise ValueError("El archivo no contiene hojas.")

    print(f"Hoja:    {hoja_objetivo}")
    df = pd.read_excel(file_path, sheet_name=hoja_objetivo, dtype=str)
    if df.empty:
        print("\nEl archivo no contiene filas.")
        return

    col_map = {normalize_col(c): c for c in df.columns}
    col_numero = (
        col_map.get("numero_parte_faltante")
        or col_map.get("numero parte faltante")
        or col_map.get("numero de parte")
        or col_map.get("numero_parte")
        or col_map.get("part_no")
    )
    if not col_numero:
        raise ValueError("No se encontró la columna 'Numero de parte'.")

    total_filas = len(df)
    filas_invalidas = 0
    candidatos = []
    seen = set()
    duplicados_archivo = 0

    for _, row in df.iterrows():
        numero_raw = row.get(col_numero)
        numero = str(numero_raw).strip() if numero_raw is not None else ""
        if not numero or numero.lower() == "nan":
            filas_invalidas += 1
            continue

        if numero in seen:
            duplicados_archivo += 1
            continue

        seen.add(numero)
        candidatos.append(numero)

    insertados = 0
    actualizados = 0
    sin_cambios = 0
    errores = 0

    async with AsyncSessionLocal() as db:
        try:
            # Ajusta secuencia id por seguridad (si hubo cargas manuales previas).
            await db.execute(
                text("SELECT setval(pg_get_serial_sequence('partes', 'id'), COALESCE((SELECT MAX(id) FROM partes), 1))")
            )
            if not dry_run:
                await db.commit()
            else:
                await db.rollback()

            for numero in candidatos:
                savepoint = await db.begin_nested()
                try:
                    existing = (
                        await db.execute(select(Parte).where(Parte.numero_parte == numero))
                    ).scalar_one_or_none()

                    if existing is None:
                        db.add(Parte(numero_parte=numero, descripcion=None, valido=True))
                        insertados += 1
                    else:
                        if not bool(existing.valido):
                            existing.valido = True
                            actualizados += 1
                        else:
                            sin_cambios += 1

                    await db.flush()
                    await savepoint.commit()
                except IntegrityError:
                    await savepoint.rollback()
                    errores += 1
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

    print("\n" + "=" * 68)
    print("RESUMEN")
    print("=" * 68)
    print(f"  Total filas en Excel:                 {total_filas}")
    print(f"  Filas inválidas (sin número parte):   {filas_invalidas}")
    print(f"  Duplicados en archivo:                {duplicados_archivo}")
    print(f"  Candidatos únicos:                    {len(candidatos)}")
    print(f"  Insertados:                           {insertados}")
    print(f"  Actualizados a valido=True:           {actualizados}")
    print(f"  Sin cambios:                          {sin_cambios}")
    print(f"  Errores:                              {errores}")
    print("=" * 68)


async def main() -> None:
    args = parse_args()
    file_path = Path(args.file)
    if not file_path.is_absolute():
        file_path = (ROOT / file_path).resolve()
    await run_import(file_path=file_path, sheet_name=args.sheet, dry_run=args.dry_run)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
