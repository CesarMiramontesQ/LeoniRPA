"""
Migración: agregar valor EJECUCION al enum carga_cliente_operacion_enum.

Ejecutar desde la raíz del proyecto:
    python -m migrations.add_ejecucion_carga_cliente_operacion
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
    """Agrega el valor EJECUCION al enum carga_cliente_operacion_enum."""
    async with engine.connect() as conn:
        await conn.execute(text(
            "ALTER TYPE carga_cliente_operacion_enum ADD VALUE IF NOT EXISTS 'EJECUCION'"
        ))
        await conn.commit()
    print("Valor 'EJECUCION' agregado al enum carga_cliente_operacion_enum.")


async def main():
    try:
        await upgrade()
        print("Migración completada correctamente.")
    except Exception as e:
        err_msg = str(e).lower()
        if "already exists" in err_msg or "duplicate" in err_msg:
            print("El valor 'EJECUCION' ya existe en el enum. Nada que hacer.")
        else:
            print(f"Error en la migración: {e}")
            raise
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
