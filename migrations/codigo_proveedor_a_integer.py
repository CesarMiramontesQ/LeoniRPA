"""
Migración: convertir codigo_proveedor de VARCHAR a INTEGER en todas las tablas.

Ejecutar desde la raíz del proyecto:
    python -m migrations.codigo_proveedor_a_integer

La migración elimina temporalmente las FKs a proveedores, convierte las columnas
y vuelve a crear las FKs.
"""

import asyncio
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from sqlalchemy import text
from app.db.base import engine


# Tablas que tienen FK a proveedores.codigo_proveedor
TABLAS_CON_FK = ["precios_materiales", "pais_origen_material", "carga_proveedores"]

TABLAS = [
    "proveedores",
    "proveedores_historial",
    "compras",
    "precios_materiales",
    "precios_materiales_historial",
    "pais_origen_material",
    "pais_origen_material_historial",
    "carga_proveedores",
    "carga_proveedores_historial",
    "carga_proveedores_nacional",
    "carga_proveedores_nacional_historial",
]


async def main():
    try:
        async with engine.begin() as conn:
            # 1. Obtener y eliminar FKs que referencian proveedores.codigo_proveedor
            fks = await conn.execute(text("""
                SELECT conname, conrelid::regclass::text
                FROM pg_constraint
                WHERE confrelid = 'proveedores'::regclass
                  AND contype = 'f'
                  AND conname LIKE '%codigo_proveedor%'
            """))
            fks_rows = fks.fetchall()
            for conname, tablename in fks_rows:
                await conn.execute(text(f'ALTER TABLE {tablename} DROP CONSTRAINT IF EXISTS "{conname}"'))
                print(f"  FK eliminada: {conname}")

            # 2. Convertir columnas a INTEGER (proveedores primero, luego el resto)
            for tabla in TABLAS:
                res = await conn.execute(text("""
                    SELECT data_type FROM information_schema.columns
                    WHERE table_name = :t AND column_name = 'codigo_proveedor'
                """), {"t": tabla})
                row = res.fetchone()
                if not row:
                    print(f"  - {tabla}: no tiene codigo_proveedor")
                    continue
                if row[0] in ("integer", "bigint"):
                    print(f"  - {tabla}.codigo_proveedor ya es INTEGER")
                    continue
                await conn.execute(text(f"""
                    ALTER TABLE {tabla}
                    ALTER COLUMN codigo_proveedor TYPE INTEGER
                    USING codigo_proveedor::integer
                """))
                print(f"  ✓ {tabla}.codigo_proveedor -> INTEGER")

            # 3. Recrear FKs
            for tabla in TABLAS_CON_FK:
                await conn.execute(text(f"""
                    ALTER TABLE {tabla}
                    ADD CONSTRAINT {tabla}_codigo_proveedor_fkey
                    FOREIGN KEY (codigo_proveedor) REFERENCES proveedores(codigo_proveedor)
                """))
                print(f"  FK recreada: {tabla} -> proveedores")
    finally:
        await engine.dispose()
    print("Migración finalizada.")


if __name__ == "__main__":
    asyncio.run(main())
