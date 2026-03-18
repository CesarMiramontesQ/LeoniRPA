"""
Script: actualizar la tabla trading_goods desde un Excel.

Columnas esperadas:
  - Número de parte (o columna que contenga "parte", "part", "number", "numero")
  - Trading good / Is trading good (o columna que contenga "trading", "good").
    Valores considerados True: sí, si, yes, true, 1, x, verdadero, s.

Ejecutar desde la raíz del proyecto:
    python scripts/actualizar_trading_goods_excel.py [ruta_al_excel]

Si no se pasa ruta, se usa "trading goods.xlsx" en la raíz del proyecto.
"""

import asyncio
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))


def _normalize_bool(value) -> bool:
    """Convierte valor del Excel a bool para is_trading_good."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value and value != 0)
    s = str(value).strip().lower()
    if not s:
        return False
    if s in ("1", "true", "yes", "si", "sí", "s", "x", "verdadero", "y"):
        return True
    if s in ("0", "false", "no", "n", "falso", "n"):
        return False
    # Cualquier otro texto no vacío se considera True (ej. "Trading Good")
    return True


def run():
    import pandas as pd
    from app.db.base import AsyncSessionLocal
    from app.db import crud

    async def main_async():
        if len(sys.argv) >= 2:
            path_excel = Path(sys.argv[1])
        else:
            path_excel = root / "trading goods.xlsx"
        if not path_excel.exists():
            print(f"Error: no existe el archivo {path_excel}")
            sys.exit(1)
        suffix = path_excel.suffix.lower()
        engine = "openpyxl" if suffix == ".xlsx" else "xlrd"
        df = pd.read_excel(path_excel, engine=engine)
        df.columns = [str(c).strip() for c in df.columns]

        col_parte = None
        col_trading = None
        for c in df.columns:
            if not c:
                continue
            cl = c.lower()
            if "parte" in cl or "part" in cl or "number" in cl or "numero" in cl or "número" in cl:
                col_parte = c
                break
        if not col_parte and len(df.columns) >= 1:
            col_parte = df.columns[0]
        for c in df.columns:
            if not c:
                continue
            cl = c.lower()
            if "trading" in cl or "good" in cl:
                col_trading = c
                break
        if not col_trading and len(df.columns) >= 2:
            col_trading = df.columns[1]

        if not col_parte:
            print("Error: no se encontró columna para número de parte.")
            sys.exit(1)
        if not col_trading:
            print("Error: no se encontró columna para trading good. Use un encabezado que contenga 'trading' o 'good'.")
            sys.exit(1)

        filas = []
        for _, row in df.iterrows():
            try:
                np_val = row.get(col_parte)
                if pd.isna(np_val) or np_val is None:
                    continue
                numero_parte = str(np_val).strip()
                if not numero_parte:
                    continue
                tg_val = row.get(col_trading)
                is_tg = _normalize_bool(tg_val)
                filas.append({"numero_parte": numero_parte, "is_trading_good": is_tg})
            except Exception:
                continue

        print(f"Filas a procesar: {len(filas)}")
        insertados = 0
        actualizados = 0
        async with AsyncSessionLocal() as db:
            for f in filas:
                existente = await crud.get_trading_good_by_numero_parte(db, f["numero_parte"])
                await crud.upsert_trading_good(
                    db,
                    numero_parte=f["numero_parte"],
                    is_trading_good=f["is_trading_good"],
                )
                if existente:
                    actualizados += 1
                else:
                    insertados += 1

        print(f"Insertados: {insertados}")
        print(f"Actualizados: {actualizados}")

    asyncio.run(main_async())


if __name__ == "__main__":
    run()
