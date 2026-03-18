"""
Migración: crear tabla trading_goods (numero_parte, is_trading_good, timestamps).

Ejecutar desde la raíz del proyecto:
    python -m migrations.add_trading_goods
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
    """Crea la tabla trading_goods si no existe."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS trading_goods (
                id BIGSERIAL PRIMARY KEY,
                numero_parte TEXT NOT NULL UNIQUE,
                is_trading_good BOOLEAN NOT NULL DEFAULT false,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_trading_goods_numero_parte ON trading_goods (numero_parte)
        """))
    print("Tabla 'trading_goods' creada correctamente.")


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
