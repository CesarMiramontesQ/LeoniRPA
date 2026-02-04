"""
Script para actualizar registros de virtuales (clientes) desde un Excel.

Actualiza los campos: incoterm, tipo_exportacion, servicio_cliente, carretes y estatus ("En Captura").
Si un cliente tiene registros IMPO y EXPO, se pueden actualizar por separado
si el Excel incluye la columna impo_expo; si no, se actualizan todos los registros de ese numero.

Uso:
  python actualizar_virtuales_desde_excel.py
  python actualizar_virtuales_desde_excel.py --archivo ruta/al/archivo.xlsx

El Excel debe tener al menos una columna con el código de cliente (numero) y las columnas
a actualizar: incoterm, tipo_exportacion, servicio_cliente, carretes.
Opcional: impo_expo (IMPO o EXPO) para aplicar solo a ese tipo.
"""

import asyncio
import os
import sys

# Asegurar que el directorio del script está en el path para importar app
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

import pandas as pd
from sqlalchemy import select

from app.db.base import AsyncSessionLocal
from app.db.models import MasterUnificadoVirtuales


# Mapeo de posibles nombres de columna en el Excel a nombres internos
COL_NUMERO = ["numero", "codigo_cliente", "codigo", "número", "numero de cliente", "sold to no.", "sold to no"]
COL_IMPO_EXPO = ["impo_expo", "impo/expo", "impo expo", "tipo"]
COL_INCOTERM = ["incoterm"]
COL_TIPO_EXPORTACION = ["tipo_exportacion", "tipo exportacion", "tipo exportación"]
COL_SERVICIO_CLIENTE = ["servicio_cliente", "servicio cliente"]
COL_CARRETES = ["carretes"]


def _normalize_col(name):
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    return str(name).strip().lower().replace(" ", "_").replace("/", "_")


def _find_column(df, posibles):
    cols_lower = {_normalize_col(c): c for c in df.columns}
    for p in posibles:
        pn = p.lower().replace(" ", "_").replace("/", "_")
        if pn in cols_lower:
            return cols_lower[pn]
    return None


def _parse_carretes(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("sí", "si", "yes", "true", "1", "x"):
        return True
    if s in ("no", "false", "0", ""):
        return False
    return None


def _str_or_none(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None


def load_excel(archivo: str):
    """Carga el Excel y normaliza columnas."""
    df = pd.read_excel(archivo, engine="openpyxl")
    # Mapear columnas
    col_numero = _find_column(df, COL_NUMERO)
    if not col_numero:
        raise ValueError(
            f"No se encontró columna de código de cliente. "
            f"Pruebe con: numero, codigo_cliente, codigo. Columnas en el archivo: {list(df.columns)}"
        )
    col_impo = _find_column(df, COL_IMPO_EXPO)
    col_incoterm = _find_column(df, COL_INCOTERM)
    col_tipo_exp = _find_column(df, COL_TIPO_EXPORTACION)
    col_servicio = _find_column(df, COL_SERVICIO_CLIENTE)
    col_carretes = _find_column(df, COL_CARRETES)

    if not any([col_incoterm, col_tipo_exp, col_servicio, col_carretes]):
        raise ValueError(
            "Debe haber al menos una columna a actualizar: incoterm, tipo_exportacion, servicio_cliente, carretes. "
            f"Columnas en el archivo: {list(df.columns)}"
        )

    rows = []
    for _, row in df.iterrows():
        try:
            num_raw = row.get(col_numero)
            if num_raw is None or (isinstance(num_raw, float) and pd.isna(num_raw)):
                continue
            numero = int(float(num_raw))
        except (TypeError, ValueError):
            continue

        impo_expo = _str_or_none(row.get(col_impo)) if col_impo else None
        if impo_expo and impo_expo.upper() not in ("IMPO", "EXPO"):
            impo_expo = None

        updates = {}
        # Siempre poner estatus en "En Captura"
        updates["estatus"] = "En Captura"
        if col_incoterm:
            v = _str_or_none(row.get(col_incoterm))
            if v is not None:
                updates["incoterm"] = v
        if col_tipo_exp:
            v = _str_or_none(row.get(col_tipo_exp))
            if v is not None:
                updates["tipo_exportacion"] = v
        if col_servicio:
            v = _str_or_none(row.get(col_servicio))
            if v is not None:
                updates["servicio_cliente"] = v
        if col_carretes:
            v = _parse_carretes(row.get(col_carretes))
            if v is not None:
                updates["carretes"] = v
        rows.append({"numero": numero, "impo_expo": impo_expo, "updates": updates})
    return rows


async def actualizar_registros(archivo: str, dry_run: bool = False):
    """Lee el Excel y actualiza los registros de virtuales (solo tipo Cliente)."""
    datos = load_excel(archivo)
    print(f"Filas a procesar: {len(datos)}")

    async with AsyncSessionLocal() as db:
        total_actualizados = 0
        numeros_sin_registro = []

        for item in datos:
            numero = item["numero"]
            impo_expo = item["impo_expo"]
            updates = item["updates"]

            # Buscar todos los registros tipo Cliente con este numero (y opcionalmente impo_expo)
            q = (
                select(MasterUnificadoVirtuales)
                .where(MasterUnificadoVirtuales.numero == numero)
                .where(MasterUnificadoVirtuales.tipo.ilike("%cliente%"))
            )
            if impo_expo:
                q = q.where(MasterUnificadoVirtuales.impo_expo == impo_expo)
            result = await db.execute(q)
            registros = list(result.scalars().all())

            if not registros:
                numeros_sin_registro.append((numero, impo_expo or "todos"))
                continue

            for r in registros:
                if "estatus" in updates:
                    r.estatus = updates["estatus"]
                if "incoterm" in updates:
                    r.incoterm = updates["incoterm"]
                if "tipo_exportacion" in updates:
                    r.tipo_exportacion = updates["tipo_exportacion"]
                if "servicio_cliente" in updates:
                    r.servicio_cliente = updates["servicio_cliente"]
                if "carretes" in updates:
                    r.carretes = updates["carretes"]
                total_actualizados += 1

        if numeros_sin_registro:
            print(f"\nCódigos sin registro tipo Cliente en la tabla: {numeros_sin_registro[:20]}")
            if len(numeros_sin_registro) > 20:
                print(f"  ... y {len(numeros_sin_registro) - 20} más.")

        if dry_run:
            print(f"\n[DRY RUN] Se actualizarían {total_actualizados} registros. No se guardaron cambios.")
            return
        await db.commit()
        print(f"\nRegistros actualizados: {total_actualizados}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Actualizar virtuales (incoterm, tipo_exportacion, servicio_cliente, carretes) desde Excel.")
    parser.add_argument("--archivo", "-a", default=None, help="Ruta al archivo Excel (por defecto: virtuales_nuevos.xlsx en el mismo directorio)")
    parser.add_argument("--dry-run", action="store_true", help="Solo mostrar qué se actualizaría, sin guardar")
    args = parser.parse_args()

    archivo = args.archivo or os.path.join(script_dir, "virtuales_nuevos.xlsx")
    if not os.path.isfile(archivo):
        print(f"Error: no se encontró el archivo {archivo}")
        sys.exit(1)

    asyncio.run(actualizar_registros(archivo, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
