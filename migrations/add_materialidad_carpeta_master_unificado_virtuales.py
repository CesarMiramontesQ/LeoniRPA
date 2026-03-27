"""
Migración: agregar columna materialidad_carpeta (TEXT) a master_unificado_virtuales.

Ejecutar desde la raíz del proyecto:
    python -m migrations.add_materialidad_carpeta_master_unificado_virtuales
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
    """Agrega la columna materialidad_carpeta a master_unificado_virtuales."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE master_unificado_virtuales
            ADD COLUMN IF NOT EXISTS materialidad_carpeta TEXT
        """))
    print("Columna 'materialidad_carpeta' agregada a master_unificado_virtuales.")


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
