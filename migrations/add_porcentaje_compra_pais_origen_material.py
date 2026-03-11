"""
Migración: agregar columna porcentaje_compra a pais_origen_material.

Ejecutar desde la raíz del proyecto:
    python -m migrations.add_porcentaje_compra_pais_origen_material
"""

import asyncio
import sys
from pathlib import Path

# Asegurar que el proyecto esté en el path
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from sqlalchemy import text
from app.db.base import engine


async def upgrade():
    """Agrega la columna porcentaje_compra a pais_origen_material."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE pais_origen_material
            ADD COLUMN IF NOT EXISTS porcentaje_compra NUMERIC(18, 6) NULL
        """))
    print("Columna 'porcentaje_compra' agregada a pais_origen_material.")


async def main():
    try:
        await upgrade()
        print("Migración completada correctamente.")
    except Exception as e:
        print(f"Error en la migración: {e}")
        raise
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
