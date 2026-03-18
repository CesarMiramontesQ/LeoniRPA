"""
Script: actualizar el campo tipo en pais_origen_material desde un Excel.

Columnas esperadas: id (opcional), codigo_proveedor, numero_material, Tipo.
Si hay columna id, se usa para identificar el registro; si no, se usa (codigo_proveedor, numero_material).

Ejecutar desde la raíz del proyecto:
    python scripts/actualizar_tipo_pais_origen_excel.py [ruta_al_excel]

Si no se pasa ruta, se usa "Tipo de material.xlsx" en la raíz del proyecto.
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
            path_excel = root / "Tipo de material.xlsx"
        if not path_excel.exists():
            print(f"Error: no existe el archivo {path_excel}")
            sys.exit(1)
        suffix = path_excel.suffix.lower()
        engine = "openpyxl" if suffix == ".xlsx" else "xlrd"
        df = pd.read_excel(path_excel, engine=engine)
        df.columns = [str(c).strip() for c in df.columns]

        col_id = None
        col_proveedor = None
        col_material = None
        col_tipo = None
        for c in df.columns:
            if c and c.lower() == "id":
                col_id = c
            if c and "proveedor" in c.lower() and "codigo" in c.lower():
                col_proveedor = c
            if c and "material" in c.lower() and "numero" in c.lower():
                col_material = c
            if c and "tipo" in c.lower():
                col_tipo = c
        if not col_tipo:
            print("Error: el Excel debe tener una columna 'Tipo'")
            sys.exit(1)
        use_id = col_id is not None
        if not use_id and (not col_proveedor or not col_material):
            print("Error: sin columna 'id', el Excel debe tener codigo_proveedor y numero_material")
            sys.exit(1)

        filas = []
        for _, row in df.iterrows():
            try:
                tipo_val = row.get(col_tipo)
                if pd.isna(tipo_val) or tipo_val is None:
                    tipo_val = None
                else:
                    tipo_val = str(tipo_val).strip() or None

                if use_id:
                    raw_id = row.get(col_id)
                    if pd.isna(raw_id):
                        continue
                    try:
                        pid = int(float(raw_id))
                    except (TypeError, ValueError):
                        continue
                    filas.append({"pais_id": pid, "tipo": tipo_val})
                else:
                    prov = row.get(col_proveedor)
                    mat = row.get(col_material)
                    if pd.isna(prov) or pd.isna(mat):
                        continue
                    try:
                        prov = int(float(prov))
                    except (TypeError, ValueError):
                        continue
                    mat = str(mat).strip()
                    if not mat:
                        continue
                    filas.append({
                        "codigo_proveedor": prov,
                        "numero_material": mat,
                        "tipo": tipo_val,
                    })
            except Exception:
                continue

        print(f"Filas a procesar: {len(filas)}")
        actualizados = 0
        no_encontrados = 0
        async with AsyncSessionLocal() as db:
            for f in filas:
                if use_id:
                    registro = await crud.get_pais_origen_material_by_id(db, f["pais_id"])
                else:
                    registro = await crud.get_pais_origen_material_by_proveedor_material(
                        db, f["codigo_proveedor"], f["numero_material"]
                    )
                if not registro:
                    no_encontrados += 1
                    continue
                actualizado = await crud.update_pais_origen_material(
                    db,
                    pais_id=registro.id,
                    tipo=f["tipo"],
                    user_id=None,
                )
                if actualizado:
                    actualizados += 1

        print(f"Actualizados: {actualizados}")
        print(f"No encontrados: {no_encontrados}")

    asyncio.run(main_async())


if __name__ == "__main__":
    run()
