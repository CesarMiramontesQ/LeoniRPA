"""
Script: actualizar solo el campo comentario en pais_origen_material desde un Excel.

Columnas esperadas: Proveedor, Material Number, Comentario.
Solo se actualiza el comentario; país de origen y porcentaje de compra no se modifican.

Ejecutar desde la raíz del proyecto:
    python scripts/actualizar_comentario_pais_origen_excel.py [ruta_al_excel]

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

        col_proveedor = col_material = col_comentario = None
        for c in df.columns:
            if c and "proveedor" in c.lower() and "supplier" not in c.lower():
                col_proveedor = c
            if c and "material" in c.lower() and "number" in c.lower():
                col_material = c
            if c and "comentario" in c.lower():
                col_comentario = c
        if not col_proveedor or not col_material:
            print("Error: el Excel debe tener columnas: Proveedor, Material Number")
            sys.exit(1)
        if not col_comentario:
            print("Error: el Excel debe tener una columna 'Comentario'")
            sys.exit(1)

        filas = []
        for _, row in df.iterrows():
            try:
                prov = row.get(col_proveedor)
                mat = row.get(col_material)
                com = row.get(col_comentario)
                if pd.isna(prov) or pd.isna(mat):
                    continue
                prov = int(float(prov))
                mat = str(mat).strip()
                if mat == "":
                    continue
                if pd.isna(com) or com is None:
                    com = None
                else:
                    com = str(com).strip() or None
                filas.append({
                    "codigo_proveedor": prov,
                    "numero_material": mat,
                    "comentario": com,
                })
            except Exception:
                continue

        print(f"Filas a procesar: {len(filas)}")
        actualizados = 0
        no_encontrados = 0
        async with AsyncSessionLocal() as db:
            for f in filas:
                registro = await crud.get_pais_origen_material_by_proveedor_material(
                    db, f["codigo_proveedor"], f["numero_material"]
                )
                if not registro:
                    no_encontrados += 1
                    continue
                actualizado = await crud.update_pais_origen_material(
                    db,
                    pais_id=registro.id,
                    comentario=f["comentario"],
                    user_id=None,
                )
                if actualizado:
                    actualizados += 1

        print(f"Actualizados: {actualizados}")
        print(f"No encontrados: {no_encontrados}")

    asyncio.run(main_async())


if __name__ == "__main__":
    run()
