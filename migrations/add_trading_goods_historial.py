"""
Migración: crear tabla trading_goods_historial (estado anterior, estado nuevo, quien realizó el cambio).

Ejecutar desde la raíz del proyecto:
    python -m migrations.add_trading_goods_historial
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
    """Crea la tabla trading_goods_historial si no existe."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS trading_goods_historial (
                id BIGSERIAL PRIMARY KEY,
                numero_parte TEXT NOT NULL,
                estado_anterior BOOLEAN NULL,
                estado_nuevo BOOLEAN NOT NULL,
                user_id INTEGER NULL REFERENCES users(id),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_trading_goods_historial_numero_parte ON trading_goods_historial (numero_parte)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_trading_goods_historial_user_id ON trading_goods_historial (user_id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_trading_goods_historial_created_at ON trading_goods_historial (created_at)
        """))
    print("Tabla 'trading_goods_historial' creada correctamente.")


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
