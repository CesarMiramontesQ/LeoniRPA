#!/usr/bin/env python3
"""
Script para importar datos de Master_virtuales.xlsx en la tabla master_unificado_virtuales.

Uso:
    python import_master_virtuales.py [ruta_al_excel]
    python import_master_virtuales.py                    # usa ./Master_virtuales.xlsx

Ejecutar desde la raíz del proyecto (donde está app/) y con el venv activado.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import pandas as pd

# Asegurar que el proyecto esté en el path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db.base import AsyncSessionLocal
from app.db.crud import create_master_unificado_virtuales, get_master_unificado_virtuales_by_numero


# Mapeo Excel -> modelo (columna Excel: columna modelo)
COLUMNAS_EXCEL_A_MODELO = {
    "solicitud de previo": "solicitud_previo",
    "agente": "agente",
    "pedimento": "pedimento",
    "ADUANA": "aduana",
    "PATENTE": "patente",
    "DESTINO": "destino",
    "CLIENTE SPACE": "cliente_space",
    "IMPO/EXPO": "impo_expo",
    "PROVEEDOR-CLIENTE": "proveedor_cliente",
    "MES": "mes",
    "FIRMA": "firma",
    "COMPLEMENTO": "complemento",
    "TIPO IMMEX": "tipo_immex",
    "FACTURA": "factura",
    "FECHA DE PAGO": "fecha_pago",
    "INFORMACION": "informacion",
    "ESTATUS": "estatus",
    "OP REGULAR": "op_regular",
    "NUMERO DE CLIENTE": "numero",
    "CARRETE": "carretes",
    "SERVICIO AL CLIENTE": "servicio_cliente",
    "PLAZO": "plazo",
    "TIPO": "tipo",
}


def _a_str(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    # Evitar notación científica en enteros (ej. 2016271 -> "2.01627e+06")
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        try:
            if float(val) == int(float(val)):
                return str(int(float(val)))
        except (ValueError, TypeError):
            pass
    s = str(val).strip()
    return s if s else None


def _a_int(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _a_bool(val) -> bool | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().upper()
    if s in ("SI", "SÍ", "YES", "1", "TRUE"):
        return True
    if s in ("NO", "0", "FALSE") or not s:
        return False
    return None


def _a_fecha(val):
    """Fecha o None. La fecha puede ir vacía (NaT, celda vacía); no hay problema."""
    if val is None or pd.isna(val):
        return None
    if hasattr(val, "date"):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def _fila_a_kwargs(df: pd.DataFrame, idx: int) -> dict:
    """Convierte una fila del DataFrame en kwargs para create_master_unificado_virtuales."""
    kw: dict = {}
    for col_excel, col_modelo in COLUMNAS_EXCEL_A_MODELO.items():
        if col_excel not in df.columns:
            continue
        raw = df.iloc[idx][col_excel]

        if col_modelo in ("solicitud_previo", "op_regular", "carretes"):
            v = _a_bool(raw)
        elif col_modelo in ("pedimento", "aduana", "patente", "destino", "numero"):
            v = _a_int(raw)
        elif col_modelo == "fecha_pago":
            v = _a_fecha(raw)
        else:
            v = _a_str(raw)

        if v is not None or col_modelo in ("solicitud_previo", "op_regular", "carretes"):
            kw[col_modelo] = v
    return kw


async def importar(ruta_excel: Path, skip_duplicados: bool, dry_run: bool) -> tuple[int, int, list[str]]:
    """
    Importa el Excel en master_unificado_virtuales.
    Devuelve (insertados, omitidos, errores).
    """
    df = pd.read_excel(ruta_excel, engine="openpyxl")
    insertados = 0
    omitidos = 0
    errores: list[str] = []

    async with AsyncSessionLocal() as db:
        for idx in range(len(df)):
            kw = _fila_a_kwargs(df, idx)
            numero = kw.get("numero")
            if numero is None:
                omitidos += 1
                errores.append(f"Fila {idx + 2}: sin NUMERO DE CLIENTE, se omite.")
                continue

            if skip_duplicados:
                existente = await get_master_unificado_virtuales_by_numero(db, int(numero))
                if existente:
                    omitidos += 1
                    continue

            if dry_run:
                insertados += 1
                continue

            try:
                await create_master_unificado_virtuales(db, **kw)
                insertados += 1
            except Exception as e:
                omitidos += 1
                errores.append(f"Fila {idx + 2} (numero={numero}): {e!s}")

    return insertados, omitidos, errores


def main():
    parser = argparse.ArgumentParser(
        description="Importa Master_virtuales.xlsx en la tabla master_unificado_virtuales."
    )
    parser.add_argument(
        "excel",
        nargs="?",
        type=Path,
        default=Path("Master_virtuales.xlsx"),
        help="Ruta al Excel (por defecto: Master_virtuales.xlsx)",
    )
    parser.add_argument(
        "--skip-duplicados",
        action="store_true",
        help="No insertar si ya existe un registro con el mismo numero (NUMERO DE CLIENTE).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo simular: no escribe en la base de datos.",
    )
    args = parser.parse_args()

    ruta = args.excel if args.excel.is_absolute() else (PROJECT_ROOT / args.excel)
    if not ruta.exists():
        print(f"Error: no se encontró el archivo {ruta}")
        sys.exit(1)

    print(f"Leyendo {ruta} ...")
    insertados, omitidos, errores = asyncio.run(
        importar(ruta, skip_duplicados=args.skip_duplicados, dry_run=args.dry_run)
    )

    if args.dry_run:
        print("[DRY RUN] Sin cambios en la base de datos.")
    print(f"Insertados: {insertados} | Omitidos: {omitidos}")
    if errores:
        for e in errores[:50]:
            print(f"  - {e}")
        if len(errores) > 50:
            print(f"  ... y {len(errores) - 50} más.")
    print("Listo.")


if __name__ == "__main__":
    main()
