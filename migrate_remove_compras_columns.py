"""
Script de migración para eliminar columnas de la tabla compras.

Columnas a eliminar:
- gr_blck_stock_oun
- gr_blocked_stck_opun
- delivery_completed
- fisc_year_ref_doc
- reference_document
- reference_doc_item

Uso:
    python migrate_remove_compras_columns.py
"""
import asyncio
import sys
from sqlalchemy import text
from app.db.base import AsyncSessionLocal


async def check_column_exists(table_name: str, column_name: str) -> bool:
    """Verifica si una columna existe en una tabla."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                    AND column_name = :column_name
                )
            """),
            {"table_name": table_name, "column_name": column_name}
        )
        return result.scalar()


async def remove_columns():
    """Elimina las columnas especificadas de la tabla compras."""
    columns_to_remove = [
        "gr_blck_stock_oun",
        "gr_blocked_stck_opun",
        "delivery_completed",
        "fisc_year_ref_doc",
        "reference_document",
        "reference_doc_item"
    ]
    
    async with AsyncSessionLocal() as session:
        removed_count = 0
        skipped_count = 0
        
        for column_name in columns_to_remove:
            exists = await check_column_exists("compras", column_name)
            
            if exists:
                print(f"  → Eliminando columna '{column_name}'...")
                try:
                    await session.execute(
                        text(f'ALTER TABLE compras DROP COLUMN IF EXISTS "{column_name}"')
                    )
                    removed_count += 1
                    print(f"    ✓ Columna '{column_name}' eliminada exitosamente")
                except Exception as e:
                    print(f"    ✗ Error al eliminar '{column_name}': {e}")
                    raise
            else:
                print(f"  ⚠ Columna '{column_name}' no existe, se omite")
                skipped_count += 1
        
        await session.commit()
        
        print(f"\n📊 Resumen:")
        print(f"   - Columnas eliminadas: {removed_count}")
        print(f"   - Columnas omitidas (no existían): {skipped_count}")


async def show_table_structure():
    """Muestra la estructura actual de la tabla compras."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'compras'
            ORDER BY ordinal_position
        """))
        
        print("\n" + "="*60)
        print("ESTRUCTURA ACTUAL DE compras")
        print("="*60)
        print(f"{'Columna':<30} {'Tipo':<20} {'Nullable'}")
        print("-"*60)
        for row in result:
            print(f"{row[0]:<30} {row[1]:<20} {row[2]}")


async def main():
    """Función principal de migración."""
    print("\n" + "="*60)
    print("MIGRACIÓN: Eliminar columnas de la tabla compras")
    print("="*60 + "\n")
    
    print("Columnas a eliminar:")
    print("  - gr_blck_stock_oun")
    print("  - gr_blocked_stck_opun")
    print("  - delivery_completed")
    print("  - fisc_year_ref_doc")
    print("  - reference_document")
    print("  - reference_doc_item")
    print()
    
    try:
        await remove_columns()
        await show_table_structure()
        
        print("\n" + "="*60)
        print("✓ MIGRACIÓN COMPLETADA")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n✗ Error durante la migración: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
