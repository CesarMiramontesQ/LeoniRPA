"""
Script de migración para renombrar component_part_no a material 
y agregar material_description en la tabla bom_flat.

Uso:
    python migrate_bom_rename_column.py
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


async def rename_column_and_add_description():
    """Renombra component_part_no a material y agrega material_description."""
    async with AsyncSessionLocal() as session:
        # Verificar si la columna component_part_no existe
        component_exists = await check_column_exists("bom_flat", "component_part_no")
        material_exists = await check_column_exists("bom_flat", "material")
        material_desc_exists = await check_column_exists("bom_flat", "material_description")
        
        if component_exists and not material_exists:
            print("Renombrando columna 'component_part_no' a 'material'...")
            
            # Eliminar el constraint único antiguo si existe (debe ser antes de eliminar el índice)
            await session.execute(text("""
                ALTER TABLE bom_flat DROP CONSTRAINT IF EXISTS uq_bom_flat_fg_plant_component_run
            """))
            
            # Eliminar el índice antiguo si existe
            await session.execute(text("""
                DROP INDEX IF EXISTS uq_bom_flat_fg_plant_component_run
            """))
            
            # Renombrar la columna
            await session.execute(text("""
                ALTER TABLE bom_flat RENAME COLUMN component_part_no TO material
            """))
            
            # Renombrar el índice si existe
            await session.execute(text("""
                ALTER INDEX IF EXISTS ix_bom_flat_component_part_no RENAME TO ix_bom_flat_material
            """))
            
            # Crear el nuevo constraint único
            await session.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bom_flat_fg_plant_material_run 
                ON bom_flat(fg_part_no, plant_code, material, COALESCE(run_id, 0))
            """))
            
            print("✓ Columna renombrada exitosamente")
        elif material_exists:
            print("⚠ La columna 'material' ya existe, no es necesario renombrar")
        else:
            print("⚠ La columna 'component_part_no' no existe")
        
        # Agregar la columna material_description si no existe
        if not material_desc_exists:
            print("Agregando columna 'material_description'...")
            await session.execute(text("""
                ALTER TABLE bom_flat ADD COLUMN material_description TEXT NULL
            """))
            print("✓ Columna 'material_description' agregada exitosamente")
        else:
            print("⚠ La columna 'material_description' ya existe")
        
        await session.commit()


async def show_table_structure():
    """Muestra la estructura actual de la tabla bom_flat."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'bom_flat'
            ORDER BY ordinal_position
        """))
        
        print("\n" + "="*50)
        print("ESTRUCTURA ACTUAL DE bom_flat")
        print("="*50)
        print(f"{'Columna':<25} {'Tipo':<20} {'Nullable'}")
        print("-"*50)
        for row in result:
            print(f"{row[0]:<25} {row[1]:<20} {row[2]}")


async def main():
    """Función principal de migración."""
    print("\n" + "="*50)
    print("MIGRACIÓN: Renombrar columna en bom_flat")
    print("="*50 + "\n")
    
    try:
        await rename_column_and_add_description()
        await show_table_structure()
        
        print("\n" + "="*50)
        print("✓ MIGRACIÓN COMPLETADA")
        print("="*50 + "\n")
        
    except Exception as e:
        print(f"\n✗ Error durante la migración: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
