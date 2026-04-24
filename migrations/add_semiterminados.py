"""
Migración: crear tabla semiterminados.

Ejecutar desde la raíz del proyecto:
    python -m migrations.add_semiterminados
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
    """Crea la tabla semiterminados si no existe."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS semiterminados (
                id BIGSERIAL PRIMARY KEY,
                numero_material VARCHAR NOT NULL REFERENCES materiales(numero_material),
                is_active BOOLEAN NOT NULL DEFAULT true,
                cut DOUBLE PRECISION,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT uq_semiterminados_numero_material UNIQUE (numero_material)
            )
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_semiterminados_numero_material
            ON semiterminados (numero_material)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_semiterminados_is_active
            ON semiterminados (is_active)
        """))
    print("Tabla 'semiterminados' creada correctamente.")


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
