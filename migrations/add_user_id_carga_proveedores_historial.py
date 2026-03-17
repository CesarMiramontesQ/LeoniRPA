"""
Migración: agregar columna user_id a carga_proveedores_historial.

Ejecutar desde la raíz del proyecto:
    python -m migrations.add_user_id_carga_proveedores_historial
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
    """Agrega la columna user_id y el índice a carga_proveedores_historial."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE carga_proveedores_historial
            ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_carga_proveedores_historial_user_id
            ON carga_proveedores_historial (user_id)
        """))
    print("Columna 'user_id' e índice agregados a carga_proveedores_historial.")


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
