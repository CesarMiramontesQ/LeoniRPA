"""
Importa datos de precios venta desde Excel a la tabla precios_venta.

Uso (desde la raíz del proyecto):
  python scripts/import_precios_venta_desde_excel.py
  python scripts/import_precios_venta_desde_excel.py --file "precios venta.xlsx" --sheet "Sheet1"
  python scripts/import_precios_venta_desde_excel.py --dry-run

Comportamiento:
  - Lee columnas (obligatorias):
      - Codigo Cliente
      - Numero parte
      - Tipo de cable
      - Precio
      - Comentario (opcional)
      - Comentario 2 (opcional)
      - Comentario 3 (opcional)
  - Valida llaves foráneas:
      - clientes.codigo_cliente
      - partes.numero_parte
  - Hace upsert por constraint único:
      - (codigo_cliente, numero_parte, tipo_cable)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import unicodedata
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select, text

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.base import AsyncSessionLocal, engine  # noqa: E402
from app.db.models import Cliente, Parte  # noqa: E402


UPSERT_SQL = text(
    """
    INSERT INTO precios_venta (
        codigo_cliente,
        numero_parte,
        tipo_cable,
        precio_venta,
        comentario,
        comentario_2,
        comentario_3,
        updated_at
    ) VALUES (
        :codigo_cliente,
        :numero_parte,
        :tipo_cable,
        :precio_venta,
        :comentario,
        :comentario_2,
        :comentario_3,
        now()
    )
    ON CONFLICT ON CONSTRAINT uq_precios_venta_cliente_parte_tipo_cable DO UPDATE
    SET
        precio_venta = EXCLUDED.precio_venta,
        comentario = EXCLUDED.comentario,
        comentario_2 = EXCLUDED.comentario_2,
        comentario_3 = EXCLUDED.comentario_3,
        updated_at = now()
    """
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Importar precios de venta desde Excel.")
    parser.add_argument(
        "--file",
        default="precios venta.xlsx",
        help='Ruta del archivo Excel (default: "precios venta.xlsx")',
    )
    parser.add_argument("--sheet", default="Sheet1", help='Nombre de hoja (default: "Sheet1")')
    parser.add_argument("--dry-run", action="store_true", help="Simula la importación sin guardar cambios")
    parser.add_argument("--chunk-size", type=int, default=2000, help="Tamaño de lote para UPSERT (default: 2000)")
    return parser.parse_args()


def normalize_col(name: Any) -> str:
    if name is None:
        return ""
    s = str(name).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.split())


def clean_string(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    return s


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != value:  # NaN
            return None
        return int(value)

    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    if s.endswith(".0"):
        s = s[:-2]
    s = s.replace(",", "").replace(" ", "")
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if value != value:  # NaN
            return None
        return Decimal(str(value))

    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    s = s.replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


async def run_import(file_path: Path, sheet_name: str, dry_run: bool, chunk_size: int) -> None:
    if not file_path.is_file():
        raise FileNotFoundError(f"No existe el archivo: {file_path}")

    print("=" * 74)
    print("Importar precios de venta desde Excel")
    print("=" * 74)
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
    col_codigo_cliente = col_map.get("codigo cliente")
    col_numero_parte = col_map.get("numero parte")
    col_tipo_cable = col_map.get("tipo de cable")
    col_precio = col_map.get("precio")
    col_comentario = col_map.get("comentario")
    col_comentario_2 = col_map.get("comentario 2")
    col_comentario_3 = col_map.get("comentario 3")

    if not col_codigo_cliente or not col_numero_parte or not col_tipo_cable or not col_precio:
        raise ValueError(
            "No se encontraron columnas requeridas: Codigo Cliente, Numero parte, Tipo de cable, Precio."
        )

    total_filas = len(df)
    filas_invalidas = 0
    duplicados_archivo = 0
    sin_fk_cliente = 0
    sin_fk_parte = 0
    seen: set[tuple[int, str, str]] = set()
    candidatos: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        codigo_cliente = to_int(row.get(col_codigo_cliente))
        numero_parte = clean_string(row.get(col_numero_parte))
        tipo_cable = clean_string(row.get(col_tipo_cable))
        precio_venta = to_decimal(row.get(col_precio))

        if codigo_cliente is None or not numero_parte or not tipo_cable:
            filas_invalidas += 1
            continue

        key = (codigo_cliente, numero_parte, tipo_cable)
        rec = {
            "codigo_cliente": codigo_cliente,
            "numero_parte": numero_parte,
            "tipo_cable": tipo_cable,
            "precio_venta": precio_venta,
            "comentario": clean_string(row.get(col_comentario)) or None if col_comentario else None,
            "comentario_2": clean_string(row.get(col_comentario_2)) or None if col_comentario_2 else None,
            "comentario_3": clean_string(row.get(col_comentario_3)) or None if col_comentario_3 else None,
        }

        if key in seen:
            duplicados_archivo += 1
            # Conserva la última ocurrencia en archivo.
            for i in range(len(candidatos) - 1, -1, -1):
                c = candidatos[i]
                if (
                    c["codigo_cliente"] == codigo_cliente
                    and c["numero_parte"] == numero_parte
                    and c["tipo_cable"] == tipo_cable
                ):
                    candidatos[i] = rec
                    break
            continue

        seen.add(key)
        candidatos.append(rec)

    upserts = 0
    validos: list[dict[str, Any]] = []
    async with AsyncSessionLocal() as db:
        try:
            codigos = sorted({c["codigo_cliente"] for c in candidatos})
            partes = sorted({c["numero_parte"] for c in candidatos})

            clientes_existentes = set()
            if codigos:
                clientes_existentes = set(
                    (await db.execute(select(Cliente.codigo_cliente).where(Cliente.codigo_cliente.in_(codigos)))).scalars().all()
                )

            partes_existentes = set()
            if partes:
                partes_existentes = set(
                    (await db.execute(select(Parte.numero_parte).where(Parte.numero_parte.in_(partes)))).scalars().all()
                )

            for rec in candidatos:
                if rec["codigo_cliente"] not in clientes_existentes:
                    sin_fk_cliente += 1
                    continue
                if rec["numero_parte"] not in partes_existentes:
                    sin_fk_parte += 1
                    continue
                validos.append(rec)

            step = max(1, int(chunk_size or 1))
            for i in range(0, len(validos), step):
                chunk = validos[i : i + step]
                await db.execute(UPSERT_SQL, chunk)
                upserts += len(chunk)

            if dry_run:
                await db.rollback()
            else:
                await db.commit()
        except Exception:
            await db.rollback()
            raise

    print("\n" + "=" * 74)
    print("RESUMEN")
    print("=" * 74)
    print(f"  Total filas en Excel:                     {total_filas}")
    print(f"  Filas inválidas:                          {filas_invalidas}")
    print(f"  Duplicados en archivo:                    {duplicados_archivo}")
    print(f"  Candidatos únicos:                        {len(candidatos)}")
    print(f"  Omitidos por cliente inexistente (FK):    {sin_fk_cliente}")
    print(f"  Omitidos por número de parte inexistente: {sin_fk_parte}")
    print(f"  Registros válidos para upsert:            {len(validos)}")
    print(f"  Upserts ejecutados:                       {upserts}")
    print("=" * 74)


async def main() -> None:
    args = parse_args()
    file_path = Path(args.file)
    if not file_path.is_absolute():
        file_path = (ROOT / file_path).resolve()

    # Verificación rápida de existencia de tabla y constraint esperados.
    async with AsyncSessionLocal() as db:
        table_exists = (
            await db.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name = 'precios_venta'
                    )
                    """
                )
            )
        ).scalar()
        if not table_exists:
            raise RuntimeError("La tabla precios_venta no existe. Ejecuta primero run_migrations.py.")

    await run_import(
        file_path=file_path,
        sheet_name=args.sheet,
        dry_run=args.dry_run,
        chunk_size=args.chunk_size,
    )
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
