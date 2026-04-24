"""
Migración: crear tabla kathoden y su historial de cambios.

Ejecutar desde la raíz del proyecto:
    python -m migrations.add_kathoden
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
    """Crea las tablas kathoden y kathoden_historial si no existen."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS kathoden (
                id BIGSERIAL PRIMARY KEY,
                mes VARCHAR(50) NOT NULL,
                anio INTEGER,
                precio DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
        await conn.execute(text("""
            ALTER TABLE kathoden
            ADD COLUMN IF NOT EXISTS anio INTEGER
        """))
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS kathoden_historial (
                id BIGSERIAL PRIMARY KEY,
                kathoden_id BIGINT NULL REFERENCES kathoden(id) ON DELETE SET NULL,
                mes VARCHAR(50) NOT NULL,
                anio INTEGER NOT NULL,
                precio_anterior DOUBLE PRECISION NULL,
                precio_nuevo DOUBLE PRECISION NOT NULL,
                operacion VARCHAR(20) NOT NULL DEFAULT 'CREATE',
                user_id INTEGER NULL REFERENCES users(id),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_kathoden_historial_kathoden_id ON kathoden_historial (kathoden_id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_kathoden_historial_user_id ON kathoden_historial (user_id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_kathoden_historial_created_at ON kathoden_historial (created_at DESC)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_kathoden_historial_anio ON kathoden_historial (anio)
        """))
    print("Tablas 'kathoden' y 'kathoden_historial' creadas correctamente.")


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
