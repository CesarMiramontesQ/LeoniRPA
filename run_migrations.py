"""
Script para ejecutar todas las migraciones de la base de datos.

1. Crea todas las tablas desde los modelos (init_db).
2. Ejecuta las migraciones adicionales en orden (enums, columnas, etc.).

Ejecutar desde la raíz del proyecto:
    python run_migrations.py
"""

import asyncio
import sys
from pathlib import Path

# Asegurar que el proyecto esté en el path
root = Path(__file__).resolve().parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))


async def run_init_db():
    """Crea todas las tablas desde los modelos SQLAlchemy."""
    from app.db.init_db import init_db
    await init_db()


async def run_migration_add_ejecucion():
    """Agrega el valor EJECUCION al enum carga_cliente_operacion_enum."""
    from app.db.base import engine
    from sqlalchemy import text
    try:
        async with engine.connect() as conn:
            await conn.execute(text(
                "ALTER TYPE carga_cliente_operacion_enum ADD VALUE IF NOT EXISTS 'EJECUCION'"
            ))
            await conn.commit()
        print("  ✓ Valor 'EJECUCION' agregado al enum carga_cliente_operacion_enum.")
    except Exception as e:
        err_msg = str(e).lower()
        if "already exists" in err_msg or "duplicate" in err_msg:
            print("  ✓ El valor 'EJECUCION' ya existe en el enum. Nada que hacer.")
        else:
            raise


async def run_migration_escenario():
    """Agrega la columna escenario a master_unificado_virtuales."""
    from app.db.base import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE master_unificado_virtuales
            ADD COLUMN IF NOT EXISTS escenario VARCHAR
        """))
    print("  ✓ Columna 'escenario' agregada a master_unificado_virtuales.")


async def run_migration_materialidad():
    """Agrega la columna materialidad a master_unificado_virtuales."""
    from app.db.base import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE master_unificado_virtuales
            ADD COLUMN IF NOT EXISTS materialidad BOOLEAN
        """))
    print("  ✓ Columna 'materialidad' agregada a master_unificado_virtuales.")


async def main():
    from app.db.base import engine

    print("=" * 60)
    print("Ejecutando migraciones de la base de datos")
    print("=" * 60)

    try:
        # 1. Crear todas las tablas
        print("\n[1/4] Creando tablas desde modelos...")
        await run_init_db()

        # 2. Migración: enum EJECUCION
        print("\n[2/4] Migración: enum carga_cliente_operacion_enum...")
        await run_migration_add_ejecucion()

        # 3. Migración: columna escenario
        print("\n[3/4] Migración: columna escenario en master_unificado_virtuales...")
        await run_migration_escenario()

        # 4. Migración: columna materialidad
        print("\n[4/4] Migración: columna materialidad en master_unificado_virtuales...")
        await run_migration_materialidad()

        print("\n" + "=" * 60)
        print("Todas las migraciones se completaron correctamente.")
        print("=" * 60)

    except Exception as e:
        print(f"\nError durante las migraciones: {e}")
        raise
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
