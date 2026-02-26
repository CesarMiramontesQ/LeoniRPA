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


async def run_migration_partes():
    """Crea la tabla partes si no existe."""
    from app.db.base import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS partes (
                id           BIGSERIAL PRIMARY KEY,
                numero_parte TEXT NOT NULL UNIQUE,
                descripcion  TEXT,
                created_at   TIMESTAMPTZ DEFAULT now()
            )
        """))
    print("  ✓ Tabla 'partes' creada o ya existía.")


async def run_migration_partes_valido():
    """Agrega la columna valido a partes (true por defecto; false cuando SAP no encuentra el material)."""
    from app.db.base import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE partes
            ADD COLUMN IF NOT EXISTS valido BOOLEAN NOT NULL DEFAULT true
        """))
    print("  ✓ Columna 'valido' agregada a partes (o ya existía).")


async def run_migration_partes_qty_total():
    """Agrega qty_total a partes y la recalcula como SUM(qty/1000) desde BOM vigente."""
    from app.db.base import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE partes
            ADD COLUMN IF NOT EXISTS qty_total NUMERIC(18, 6) NOT NULL DEFAULT 0
        """))
        await conn.execute(text("""
            UPDATE partes p
            SET qty_total = COALESCE(agg.total_qty, 0)
            FROM (
                SELECT
                    b.parte_id,
                    SUM(bi.qty / 1000.0) AS total_qty
                FROM bom b
                JOIN bom_revision br ON br.bom_id = b.id
                JOIN bom_item bi ON bi.bom_revision_id = br.id
                WHERE br.effective_to IS NULL
                GROUP BY b.parte_id
            ) agg
            WHERE agg.parte_id = p.id
        """))
        await conn.execute(text("""
            UPDATE partes
            SET qty_total = 0
            WHERE qty_total IS NULL
        """))
    print("  ✓ Columna 'qty_total' agregada/recalculada en partes.")


async def run_migration_bom():
    """Crea la tabla bom y el índice ix_bom_parte_id si no existen."""
    from app.db.base import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bom (
                id          BIGSERIAL PRIMARY KEY,
                parte_id    BIGINT NOT NULL REFERENCES partes(id),
                plant       TEXT NOT NULL,
                usage       TEXT NOT NULL,
                alternative TEXT NOT NULL,
                base_qty    NUMERIC,
                reqd_qty    NUMERIC,
                base_unit   TEXT,
                detalle     JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (parte_id, plant, usage, alternative)
            )
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bom_parte_id ON bom (parte_id)
        """))
    print("  ✓ Tabla 'bom' e índice ix_bom_parte_id creados o ya existían.")


async def run_migration_bom_revision():
    """Crea la tabla bom_revision y sus índices si no existen."""
    from app.db.base import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bom_revision (
                id             BIGSERIAL PRIMARY KEY,
                bom_id         BIGINT NOT NULL REFERENCES bom(id) ON DELETE CASCADE,
                revision_no    INT NOT NULL,
                effective_from DATE NOT NULL,
                effective_to   DATE,
                source         TEXT,
                hash           TEXT NOT NULL,
                detalle        JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (bom_id, revision_no),
                CHECK (effective_to IS NULL OR effective_to > effective_from)
            )
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bom_revision_current
            ON bom_revision (bom_id)
            WHERE effective_to IS NULL
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bom_revision_bom_id ON bom_revision (bom_id)
        """))
    print("  ✓ Tabla 'bom_revision' e índices creados o ya existían.")


async def run_migration_bom_item():
    """Crea la tabla bom_item y sus índices si no existen."""
    from app.db.base import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bom_item (
                id              BIGSERIAL PRIMARY KEY,
                bom_revision_id BIGINT NOT NULL REFERENCES bom_revision(id) ON DELETE CASCADE,
                componente_id   BIGINT NOT NULL REFERENCES partes(id),
                item_no         TEXT,
                qty             NUMERIC NOT NULL,
                measure         TEXT,
                origin          TEXT,
                detalle         JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (bom_revision_id, componente_id)
            )
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bom_item_revision ON bom_item (bom_revision_id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bom_item_componente ON bom_item (componente_id)
        """))
    print("  ✓ Tabla 'bom_item' e índices creados o ya existían.")


async def run_migration_bom_item_comm_code():
    """Agrega la columna comm_code a bom_item."""
    from app.db.base import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE bom_item
            ADD COLUMN IF NOT EXISTS comm_code TEXT
        """))
    print("  ✓ Columna 'comm_code' agregada a bom_item (o ya existía).")


async def run_migration_peso_neto():
    """Crea la tabla peso_neto si no existe."""
    from app.db.base import engine
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS peso_neto (
                numero_parte TEXT PRIMARY KEY,
                descripcion  TEXT,
                gross        NUMERIC(18, 6),
                net          NUMERIC(18, 6),
                kgm          NUMERIC(18, 6),
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
    print("  ✓ Tabla 'peso_neto' creada o ya existía.")


async def main():
    from app.db.base import engine

    print("=" * 60)
    print("Ejecutando migraciones de la base de datos")
    print("=" * 60)

    try:
        # 1. Crear todas las tablas
        print("\n[1/12] Creando tablas desde modelos...")
        await run_init_db()

        # 2. Migración: enum EJECUCION
        print("\n[2/12] Migración: enum carga_cliente_operacion_enum...")
        await run_migration_add_ejecucion()

        # 3. Migración: columna escenario
        print("\n[3/12] Migración: columna escenario en master_unificado_virtuales...")
        await run_migration_escenario()

        # 4. Migración: columna materialidad
        print("\n[4/12] Migración: columna materialidad en master_unificado_virtuales...")
        await run_migration_materialidad()

        # 5. Migración: tabla partes
        print("\n[5/12] Migración: tabla partes...")
        await run_migration_partes()

        # 6. Migración: tabla bom
        print("\n[6/12] Migración: tabla bom...")
        await run_migration_bom()

        # 7. Migración: tabla bom_revision
        print("\n[7/12] Migración: tabla bom_revision...")
        await run_migration_bom_revision()

        # 8. Migración: tabla bom_item
        print("\n[8/12] Migración: tabla bom_item...")
        await run_migration_bom_item()

        # 9. Migración: columna valido en partes
        print("\n[9/12] Migración: columna valido en partes...")
        await run_migration_partes_valido()

        # 10. Migración: columna qty_total en partes
        print("\n[10/12] Migración: columna qty_total en partes...")
        await run_migration_partes_qty_total()

        # 11. Migración: columna comm_code en bom_item
        print("\n[11/12] Migración: columna comm_code en bom_item...")
        await run_migration_bom_item_comm_code()

        # 12. Migración: tabla peso_neto
        print("\n[12/12] Migración: tabla peso_neto...")
        await run_migration_peso_neto()

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
