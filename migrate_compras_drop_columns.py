"""
Migración: alinear la tabla compras con el modelo Compra.

- Añade columnas que están en el modelo pero no en la BD (ej. codigo_proveedor).
- Elimina columnas que están en la BD pero no en el modelo.

Uso:
  python -m migrate_compras_drop_columns           # ejecutar migración
  python -m migrate_compras_drop_columns --dry-run # solo mostrar qué se haría
"""
from __future__ import annotations

import argparse
import asyncio
import sys

from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import settings
from app.db.models import Compra


def _ddl_add_column(column, dialect) -> str:
    """Genera ADD COLUMN ... para PostgreSQL."""
    type_str = column.type.compile(dialect=dialect)
    null_str = " NULL" if column.nullable else " NOT NULL"
    return f'ALTER TABLE compras ADD COLUMN "{column.name}" {type_str}{null_str}'


def _ddl_create_index(column_name: str) -> str:
    """Nombre de índice por convención SQLAlchemy (index=True)."""
    return f'CREATE INDEX IF NOT EXISTS ix_compras_{column_name} ON compras ("{column_name}")'


async def run(dry_run: bool) -> None:
    engine = create_async_engine(settings.DB_URL, echo=False)
    dialect = postgresql.dialect()

    async with engine.begin() as conn:
        r = await conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'compras'
                ORDER BY ordinal_position;
            """)
        )
        db_columns = {row[0] for row in r.fetchall()}

    model_columns = {c.name for c in Compra.__table__.c}
    to_drop = sorted(db_columns - model_columns)
    to_add = sorted(model_columns - db_columns)

    if not to_drop and not to_add:
        print("✓ La tabla compras ya coincide con el modelo. No hay cambios que aplicar.")
        await engine.dispose()
        return

    if to_add:
        print(f"Columnas a añadir (en modelo, no en BD): {', '.join(to_add)}")
    if to_drop:
        print(f"Columnas a eliminar (en BD, no en modelo): {', '.join(to_drop)}")

    if dry_run:
        print("Modo --dry-run: no se aplicarán cambios. Ejecuta sin --dry-run para aplicar.")
        await engine.dispose()
        return

    async with engine.begin() as conn:
        for name in to_add:
            col = Compra.__table__.c[name]
            ddl = _ddl_add_column(col, dialect)
            await conn.execute(text(ddl))
            print(f"  ✓ Añadida columna: {name}")
            if col.index:
                idx_ddl = _ddl_create_index(name)
                await conn.execute(text(idx_ddl))
                print(f"    ✓ Índice: ix_compras_{name}")

        for col in to_drop:
            stmt = text(f'ALTER TABLE compras DROP COLUMN IF EXISTS "{col}"')
            await conn.execute(stmt)
            print(f"  ✓ Eliminada columna: {col}")

    print("✓ Migración aplicada correctamente.")
    await engine.dispose()


def main() -> None:
    ap = argparse.ArgumentParser(description="Alinear tabla compras con el modelo: añadir y eliminar columnas.")
    ap.add_argument("--dry-run", action="store_true", help="Solo mostrar qué se haría, sin aplicar.")
    args = ap.parse_args()

    try:
        asyncio.run(run(dry_run=args.dry_run))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
