"""
Migración: crear tabla semiterminados_historial.

Ejecutar desde la raíz del proyecto:
    python -m migrations.add_semiterminados_historial
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
    """Crea la tabla semiterminados_historial si no existe."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS semiterminados_historial (
                id BIGSERIAL PRIMARY KEY,
                semiterminado_id BIGINT NULL REFERENCES semiterminados(id) ON DELETE SET NULL,
                numero_material VARCHAR NOT NULL,
                is_active_anterior BOOLEAN NULL,
                is_active_nuevo BOOLEAN NOT NULL,
                cut_anterior DOUBLE PRECISION NULL,
                cut_nuevo DOUBLE PRECISION NULL,
                operacion VARCHAR(20) NOT NULL DEFAULT 'CREATE',
                user_id INTEGER NULL REFERENCES users(id),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_semiterminados_historial_semiterminado_id
            ON semiterminados_historial (semiterminado_id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_semiterminados_historial_numero_material
            ON semiterminados_historial (numero_material)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_semiterminados_historial_user_id
            ON semiterminados_historial (user_id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_semiterminados_historial_created_at
            ON semiterminados_historial (created_at DESC)
        """))
    print("Tabla 'semiterminados_historial' creada correctamente.")


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
