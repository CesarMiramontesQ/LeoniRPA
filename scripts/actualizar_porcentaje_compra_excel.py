"""
Script: actualizar porcentaje_compra en pais_origen_material desde un Excel.

Columnas esperadas: Proveedor, Material Number, Porcentaje de Compra.

Ejecutar desde la raíz del proyecto:
    python scripts/actualizar_porcentaje_compra_excel.py [ruta_al_excel]

Si no se pasa ruta, se usa "pais origen.xlsx" en la raíz del proyecto.
"""

import asyncio
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

def run():
    import pandas as pd
    from app.db.base import AsyncSessionLocal
    from app.db import crud

    async def main_async():
        if len(sys.argv) >= 2:
            path_excel = Path(sys.argv[1])
        else:
            path_excel = root / "pais origen.xlsx"
        if not path_excel.exists():
            print(f"Error: no existe el archivo {path_excel}")
            sys.exit(1)
        suffix = path_excel.suffix.lower()
        engine = "openpyxl" if suffix == ".xlsx" else "xlrd"
        df = pd.read_excel(path_excel, engine=engine)
        df.columns = [str(c).strip() for c in df.columns]
        col_proveedor = col_material = col_pct = None
        for c in df.columns:
            if c and "proveedor" in c.lower():
                col_proveedor = c
            if c and "material" in c.lower() and "number" in c.lower():
                col_material = c
            if c and "porcentaje" in c.lower() and "compra" in c.lower():
                col_pct = c
        if not col_proveedor or not col_material or not col_pct:
            print("Error: el Excel debe tener columnas: Proveedor, Material Number, Porcentaje de Compra")
            sys.exit(1)
        filas = []
        for _, row in df.iterrows():
            try:
                prov = row.get(col_proveedor)
                mat = row.get(col_material)
                pct = row.get(col_pct)
                if pd.isna(prov) or pd.isna(mat):
                    continue
                prov = int(float(prov))
                mat = str(mat).strip()
                if mat == "":
                    continue
                if pct is None or (isinstance(pct, float) and pd.isna(pct)):
                    pct = None
                else:
                    try:
                        pct = float(pct)
                    except (TypeError, ValueError):
                        pct = None
                filas.append({
                    "codigo_proveedor": prov,
                    "numero_material": mat,
                    "porcentaje_compra": pct,
                })
            except Exception:
                continue
        print(f"Filas a procesar: {len(filas)}")
        async with AsyncSessionLocal() as db:
            resultado = await crud.actualizar_porcentaje_compra_desde_filas(
                db=db,
                filas=filas,
                user_id=None,
            )
        print(f"Actualizados: {resultado['actualizados']}")
        print(f"No encontrados: {resultado['no_encontrados']}")
        if resultado["errores"]:
            print(f"Errores: {len(resultado['errores'])}")
            for e in resultado["errores"][:10]:
                print(f"  - {e}")
            if len(resultado["errores"]) > 10:
                print(f"  ... y {len(resultado['errores']) - 10} más")

    asyncio.run(main_async())


if __name__ == "__main__":
    run()
