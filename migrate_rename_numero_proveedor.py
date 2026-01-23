"""
Script de migración para renombrar numero_proveedor a codigo_proveedor 
y cambiar el tipo de dato de BigInteger a String en la tabla compras.

Uso:
    python migrate_rename_numero_proveedor.py
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


async def get_column_type(table_name: str, column_name: str) -> str:
    """Obtiene el tipo de dato de una columna."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
                AND column_name = :column_name
            """),
            {"table_name": table_name, "column_name": column_name}
        )
        return result.scalar()


async def rename_column():
    """Renombra numero_proveedor a codigo_proveedor y cambia el tipo a String."""
    async with AsyncSessionLocal() as session:
        numero_exists = await check_column_exists("compras", "numero_proveedor")
        codigo_exists = await check_column_exists("compras", "codigo_proveedor")
        
        if numero_exists and not codigo_exists:
            print("Renombrando columna 'numero_proveedor' a 'codigo_proveedor'...")
            
            # Obtener el tipo actual
            current_type = await get_column_type("compras", "numero_proveedor")
            print(f"  Tipo actual: {current_type}")
            
            # Si es BigInteger, necesitamos convertir los datos primero
            if current_type == 'bigint':
                print("  Convirtiendo datos de BigInteger a String...")
                # Crear una columna temporal para la conversión
                await session.execute(text("""
                    ALTER TABLE compras 
                    ADD COLUMN codigo_proveedor_temp VARCHAR
                """))
                
                # Convertir los valores numéricos a string
                await session.execute(text("""
                    UPDATE compras 
                    SET codigo_proveedor_temp = CAST(numero_proveedor AS VARCHAR)
                    WHERE numero_proveedor IS NOT NULL
                """))
                
                # Eliminar la columna antigua
                await session.execute(text("""
                    ALTER TABLE compras DROP COLUMN numero_proveedor
                """))
                
                # Renombrar la columna temporal
                await session.execute(text("""
                    ALTER TABLE compras 
                    RENAME COLUMN codigo_proveedor_temp TO codigo_proveedor
                """))
                
                # Recrear el índice si existía
                await session.execute(text("""
                    CREATE INDEX IF NOT EXISTS ix_compras_codigo_proveedor 
                    ON compras(codigo_proveedor)
                """))
                
                print("  ✓ Columna renombrada y tipo cambiado exitosamente")
            else:
                # Si ya es String, solo renombrar
                await session.execute(text("""
                    ALTER TABLE compras 
                    RENAME COLUMN numero_proveedor TO codigo_proveedor
                """))
                
                # Renombrar el índice si existe
                await session.execute(text("""
                    ALTER INDEX IF EXISTS ix_compras_numero_proveedor 
                    RENAME TO ix_compras_codigo_proveedor
                """))
                
                print("  ✓ Columna renombrada exitosamente")
            
            await session.commit()
        elif codigo_exists:
            print("⚠ La columna 'codigo_proveedor' ya existe")
            # Verificar si el tipo es correcto
            current_type = await get_column_type("compras", "codigo_proveedor")
            if current_type not in ['character varying', 'varchar', 'text']:
                print(f"  ⚠ Advertencia: El tipo actual es {current_type}, debería ser VARCHAR")
                print("  Considera ejecutar una migración adicional para cambiar el tipo")
        else:
            print("⚠ La columna 'numero_proveedor' no existe")
            print("  La tabla puede no tener esta columna o ya fue migrada")


async def show_table_structure():
    """Muestra la estructura actual de la tabla compras relacionada con proveedores."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'compras'
            AND column_name IN ('nombre_proveedor', 'numero_proveedor', 'codigo_proveedor')
            ORDER BY ordinal_position
        """))
        
        print("\n" + "="*60)
        print("COLUMNAS DE PROVEEDOR EN compras")
        print("="*60)
        print(f"{'Columna':<30} {'Tipo':<20} {'Nullable'}")
        print("-"*60)
        rows = result.fetchall()
        if rows:
            for row in rows:
                print(f"{row[0]:<30} {row[1]:<20} {row[2]}")
        else:
            print("No se encontraron columnas relacionadas con proveedores")


async def main():
    """Función principal de migración."""
    print("\n" + "="*60)
    print("MIGRACIÓN: Renombrar numero_proveedor a codigo_proveedor")
    print("="*60 + "\n")
    
    try:
        await rename_column()
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
