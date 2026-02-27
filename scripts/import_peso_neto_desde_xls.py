"""
Importa datos a la tabla peso_neto desde un archivo tipo XLS/TSV.

Regla de negocio:
  - kgm SIEMPRE se calcula como: net / 1000

Uso:
  python scripts/import_peso_neto_desde_xls.py
  python scripts/import_peso_neto_desde_xls.py --file "peso_neto..XLS"
  python scripts/import_peso_neto_desde_xls.py --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import math
import sys
import unicodedata
from decimal import Decimal, InvalidOperation
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
    INSERT INTO peso_neto (numero_parte, descripcion, gross, net, kgm, updated_at)
    VALUES (:numero_parte, :descripcion, :gross, :net, :kgm, now())
    ON CONFLICT (numero_parte) DO UPDATE
    SET
        descripcion = EXCLUDED.descripcion,
        gross = EXCLUDED.gross,
        net = EXCLUDED.net,
        kgm = EXCLUDED.kgm,
        updated_at = now()
    """
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Importar peso_neto desde archivo XLS/TSV.")
    parser.add_argument(
        "--file",
        default="peso_neto..XLS",
        help='Ruta del archivo origen (default: "peso_neto..XLS")',
    )
    parser.add_argument("--dry-run", action="store_true", help="Simula la importación sin guardar cambios")
    parser.add_argument("--chunk-size", type=int, default=2000, help="Tamaño de lote para UPSERT (default: 2000)")
    return parser.parse_args()


def normalize_col(name: Any) -> str:
    if name is None:
        return ""
    s = str(name).strip().lower()
    s = s.replace("\ufeff", "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.split())


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return Decimal(str(value))

    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    s = s.replace(" ", "")
    # Soporta coma decimal.
    if "," in s and "." in s:
        s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def load_source_dataframe(file_path: Path) -> pd.DataFrame:
    errors: list[str] = []

    # 1) Muchos .XLS "exportados" son texto tabulado (frecuente en UTF-16).
    for enc in ("utf-16", "utf-16le", "utf-8-sig", "cp1252", "latin1"):
        try:
            df = pd.read_csv(file_path, sep="\t", dtype=object, encoding=enc)
            if not df.empty and len(df.columns) >= 3:
                return df
        except Exception as exc:  # pragma: no cover
            errors.append(f"read_csv({enc}): {exc}")

    # 2) Intento como Excel real.
    try:
        df = pd.read_excel(file_path, dtype=object)
        if not df.empty:
            return df
    except Exception as exc:  # pragma: no cover
        errors.append(f"read_excel: {exc}")

    # 3) Algunos .XLS vienen como tabla HTML.
    try:
        tables = pd.read_html(file_path)
        if tables:
            tables_sorted = sorted(tables, key=lambda t: len(t), reverse=True)
            return tables_sorted[0]
    except Exception as exc:  # pragma: no cover
        errors.append(f"read_html: {exc}")

    raise ValueError(
        "No se pudo leer el archivo como TSV, Excel o HTML.\n"
        + "\n".join(errors[-5:])
    )


async def run_import(file_path: Path, dry_run: bool, chunk_size: int) -> None:
    if not file_path.is_file():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print("=" * 68)
    print("Importar peso_neto desde archivo XLS/TSV")
    print("=" * 68)
    print(f"Archivo: {file_path}")
    print(f"Modo:    {'DRY-RUN (sin guardar)' if dry_run else 'ESCRITURA'}")
    print(f"Lote:    {chunk_size}")

    df = load_source_dataframe(file_path)
    if df.empty:
        print("\nEl archivo no contiene filas.")
        return

    col_map = {normalize_col(c): c for c in df.columns}

    col_numero = (
        col_map.get("material")
        or col_map.get("numeros de parte")
        or col_map.get("numero de parte")
        or col_map.get("numero_parte")
        or col_map.get("part_no")
    )
    col_descripcion = (
        col_map.get("descripcion del material")
        or col_map.get("material description")
        or col_map.get("descripcion")
    )
    col_gross = col_map.get("peso bruto") or col_map.get("gross weight") or col_map.get("gross")
    col_net = col_map.get("peso neto") or col_map.get("net weight") or col_map.get("net")

    if not col_numero:
        raise ValueError("No se encontró la columna de número de parte (ej. 'Material').")
    if not col_net:
        raise ValueError("No se encontró la columna de neto (ej. 'Peso neto').")

    total_filas = len(df)
    filas_invalidas = 0
    duplicados_archivo = 0
    candidatos: list[dict[str, Any]] = []
    idx_by_numero: dict[str, int] = {}

    for _, row in df.iterrows():
        numero_raw = row.get(col_numero)
        numero = str(numero_raw).strip() if numero_raw is not None else ""
        if not numero or numero.lower() == "nan":
            filas_invalidas += 1
            continue

        numero = numero.split(".")[0] if numero.endswith(".0") else numero
        descripcion = (
            str(row.get(col_descripcion)).strip()
            if col_descripcion and row.get(col_descripcion) is not None
            else None
        )
        gross = to_decimal(row.get(col_gross)) if col_gross else None
        net = to_decimal(row.get(col_net))
        if net is None:
            kgm = None
        elif net == 0:
            kgm = Decimal("0")
        else:
            kgm = net / Decimal("1000")

        rec = {
            "numero_parte": numero,
            "descripcion": descripcion,
            "gross": gross,
            "net": net,
            "kgm": kgm,
        }

        if numero in idx_by_numero:
            duplicados_archivo += 1
            candidatos[idx_by_numero[numero]] = rec
        else:
            idx_by_numero[numero] = len(candidatos)
            candidatos.append(rec)

    upserts = 0
    async with AsyncSessionLocal() as db:
        try:
            for i in range(0, len(candidatos), max(1, chunk_size)):
                chunk = candidatos[i : i + max(1, chunk_size)]
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
    print(f"  Total filas en archivo:               {total_filas}")
    print(f"  Filas inválidas (sin número parte):   {filas_invalidas}")
    print(f"  Duplicados en archivo:                {duplicados_archivo}")
    print(f"  Candidatos únicos:                    {len(candidatos)}")
    print(f"  Upserts ejecutados:                   {upserts}")
    print("=" * 68)


async def main() -> None:
    args = parse_args()
    file_path = Path(args.file)
    if not file_path.is_absolute():
        file_path = (ROOT / file_path).resolve()
    await run_import(file_path=file_path, dry_run=args.dry_run, chunk_size=args.chunk_size)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
