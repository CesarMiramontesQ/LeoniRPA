"""
Migración: agregar columna detalles a purcharsing_execution_history.

Ejecutar desde la raíz del proyecto:
    python -m migrations.add_detalles_execution_history
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
    """Agrega la columna detalles a purcharsing_execution_history."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE purcharsing_execution_history
            ADD COLUMN IF NOT EXISTS detalles TEXT
        """))
    print("Columna 'detalles' agregada a purcharsing_execution_history.")


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
