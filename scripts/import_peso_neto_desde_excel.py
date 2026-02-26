"""
Importa datos de Net Weight a la tabla peso_neto.

Uso (desde la raíz del proyecto):
  python scripts/import_peso_neto_desde_excel.py
  python scripts/import_peso_neto_desde_excel.py --file "Net Weight LCM.xlsx" --sheet "Net Weight"
  python scripts/import_peso_neto_desde_excel.py --dry-run

Comportamiento:
  - Lee columnas (obligatoria: número de parte):
      - Numeros de parte / numero_parte / Numero de parte / part_no
      - Material Description / descripcion
      - Gross Weight / gross
      - Net Weight / net
      - Kgs x Metro / kgm
  - Hace upsert por peso_neto.numero_parte:
      - Si no existe: inserta registro
      - Si existe: actualiza descripcion, gross, net, kgm
"""

import argparse
import asyncio
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.base import AsyncSessionLocal, engine  # noqa: E402
from app.db.models import PesoNeto  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Importar Net Weight a la tabla peso_neto.")
    parser.add_argument(
        "--file",
        default="Net Weight LCM.xlsx",
        help='Ruta del archivo Excel (default: "Net Weight LCM.xlsx")',
    )
    parser.add_argument("--sheet", default="Net Weight", help='Nombre de hoja (default: "Net Weight")')
    parser.add_argument("--dry-run", action="store_true", help="Simula la importación sin guardar cambios")
    return parser.parse_args()


def normalize_col(name: Any) -> str:
    if name is None:
        return ""
    return " ".join(str(name).strip().lower().split())


def to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))

    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    # Soporta formatos con coma decimal.
    s = s.replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


async def run_import(file_path: Path, sheet_name: str, dry_run: bool) -> None:
    if not file_path.is_file():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print("=" * 68)
    print("Importar Net Weight a tabla peso_neto")
    print("=" * 68)
    print(f"Archivo: {file_path}")
    print(f"Hoja:    {sheet_name}")
    print(f"Modo:    {'DRY-RUN (sin guardar)' if dry_run else 'ESCRITURA'}")

    excel = pd.ExcelFile(file_path)
    hojas = list(excel.sheet_names)
    if sheet_name in hojas:
        hoja_objetivo = sheet_name
    elif "Net Weight" in hojas:
        hoja_objetivo = "Net Weight"
        print(f"Hoja solicitada '{sheet_name}' no existe. Se usará: {hoja_objetivo}")
    elif hojas:
        hoja_objetivo = hojas[0]
        print(f"Hoja solicitada '{sheet_name}' no existe. Se usará la primera hoja: {hoja_objetivo}")
    else:
        raise ValueError("El archivo no contiene hojas.")

    df = pd.read_excel(file_path, sheet_name=hoja_objetivo, dtype=object)
    if df.empty:
        print("\nEl archivo no contiene filas.")
        return

    col_map = {normalize_col(c): c for c in df.columns}
    col_numero = (
        col_map.get("numeros de parte")
        or col_map.get("numero de parte")
        or col_map.get("numero_parte")
        or col_map.get("part_no")
    )
    col_descripcion = col_map.get("material description") or col_map.get("descripcion")
    col_gross = col_map.get("gross weight") or col_map.get("gross")
    col_net = col_map.get("net weight") or col_map.get("net")
    col_kgm = (
        col_map.get("kgs x metro")
        or col_map.get("kg x metro")
        or col_map.get("kgm")
    )

    if not col_numero:
        raise ValueError("No se encontró la columna de número de parte.")

    total_filas = len(df)
    filas_invalidas = 0
    duplicados_archivo = 0
    seen = set()
    candidatos = []

    for _, row in df.iterrows():
        numero_raw = row.get(col_numero)
        numero = str(numero_raw).strip() if numero_raw is not None else ""
        if not numero or numero.lower() == "nan":
            filas_invalidas += 1
            continue

        rec = {
            "numero_parte": numero,
            "descripcion": str(row.get(col_descripcion)).strip() if col_descripcion and row.get(col_descripcion) is not None else None,
            "gross": to_decimal(row.get(col_gross)) if col_gross else None,
            "net": to_decimal(row.get(col_net)) if col_net else None,
            "kgm": to_decimal(row.get(col_kgm)) if col_kgm else None,
        }

        if numero in seen:
            duplicados_archivo += 1
            # Conserva la última ocurrencia.
            for i in range(len(candidatos) - 1, -1, -1):
                if candidatos[i]["numero_parte"] == numero:
                    candidatos[i] = rec
                    break
            continue

        seen.add(numero)
        candidatos.append(rec)

    insertados = 0
    actualizados = 0
    sin_cambios = 0
    errores = 0

    async with AsyncSessionLocal() as db:
        try:
            for rec in candidatos:
                savepoint = await db.begin_nested()
                try:
                    existing = (
                        await db.execute(
                            select(PesoNeto).where(PesoNeto.numero_parte == rec["numero_parte"])
                        )
                    ).scalar_one_or_none()

                    if existing is None:
                        db.add(
                            PesoNeto(
                                numero_parte=rec["numero_parte"],
                                descripcion=rec["descripcion"],
                                gross=rec["gross"],
                                net=rec["net"],
                                kgm=rec["kgm"],
                            )
                        )
                        insertados += 1
                    else:
                        changed = False
                        if (existing.descripcion or None) != (rec["descripcion"] or None):
                            existing.descripcion = rec["descripcion"]
                            changed = True
                        if existing.gross != rec["gross"]:
                            existing.gross = rec["gross"]
                            changed = True
                        if existing.net != rec["net"]:
                            existing.net = rec["net"]
                            changed = True
                        if existing.kgm != rec["kgm"]:
                            existing.kgm = rec["kgm"]
                            changed = True

                        if changed:
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
    print(f"  Actualizados:                         {actualizados}")
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
