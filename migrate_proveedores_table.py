"""
Script de migración para actualizar la tabla proveedores.

Uso:
    python migrate_proveedores_table.py

Este script:
1. Renombra la columna codigo_cliente a codigo_proveedor
2. Elimina la columna id y hace codigo_proveedor la clave primaria
3. Agrega las nuevas columnas: poblacion, estatus_compras, cp
4. Actualiza la tabla precios_materiales para usar codigo_proveedor
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


async def check_table_exists(table_name: str) -> bool:
    """Verifica si una tabla existe en la base de datos."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                )
            """),
            {"table_name": table_name}
        )
        return result.scalar()


async def migrate_proveedores_table():
    """Migra la tabla proveedores."""
    async with AsyncSessionLocal() as session:
        try:
            # Verificar que la tabla existe
            if not await check_table_exists("proveedores"):
                print("✗ Error: La tabla 'proveedores' no existe")
                return False
            
            # 1. Primero, actualizar la tabla precios_materiales si existe
            #    para eliminar la foreign key constraint antes de renombrar
            precios_table_exists = await check_table_exists("precios_materiales")
            if precios_table_exists:
                print("1. Actualizando tabla precios_materiales...")
                
                # Verificar si existe la foreign key constraint
                fk_exists = await session.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.table_constraints 
                            WHERE table_name = 'precios_materiales'
                            AND constraint_type = 'FOREIGN KEY'
                            AND constraint_name LIKE '%codigo_cliente%'
                        )
                    """)
                )
                
                if fk_exists.scalar():
                    # Eliminar la foreign key constraint
                    await session.execute(text("""
                        ALTER TABLE precios_materiales 
                        DROP CONSTRAINT IF EXISTS precios_materiales_codigo_cliente_fkey
                    """))
                    print("   ✓ Foreign key constraint eliminada")
                
                # Renombrar la columna en precios_materiales si existe
                if await check_column_exists("precios_materiales", "codigo_cliente"):
                    await session.execute(text("""
                        ALTER TABLE precios_materiales 
                        RENAME COLUMN codigo_cliente TO codigo_proveedor
                    """))
                    print("   ✓ Columna renombrada en precios_materiales")
            
            # 2. Agregar las nuevas columnas a proveedores si no existen
            print("2. Agregando nuevas columnas a proveedores...")
            
            if not await check_column_exists("proveedores", "poblacion"):
                await session.execute(text("""
                    ALTER TABLE proveedores 
                    ADD COLUMN poblacion TEXT
                """))
                print("   ✓ Columna 'poblacion' agregada")
            
            if not await check_column_exists("proveedores", "cp"):
                await session.execute(text("""
                    ALTER TABLE proveedores 
                    ADD COLUMN cp TEXT
                """))
                print("   ✓ Columna 'cp' agregada")
            
            if not await check_column_exists("proveedores", "estatus_compras"):
                await session.execute(text("""
                    ALTER TABLE proveedores 
                    ADD COLUMN estatus_compras TEXT
                """))
                print("   ✓ Columna 'estatus_compras' agregada")
            
            # 3. Asegurar que codigo_cliente no sea NULL antes de convertirlo en PK
            print("3. Verificando datos de codigo_cliente...")
            
            # Contar registros sin codigo_cliente
            null_count = await session.execute(text("""
                SELECT COUNT(*) FROM proveedores WHERE codigo_cliente IS NULL
            """))
            null_count_value = null_count.scalar()
            
            if null_count_value > 0:
                print(f"   ⚠ Advertencia: {null_count_value} proveedores sin codigo_cliente")
                print("   Generando códigos temporales para registros sin código...")
                
                # Generar códigos temporales para los que no tienen
                await session.execute(text("""
                    UPDATE proveedores 
                    SET codigo_cliente = 'TEMP_' || id::TEXT 
                    WHERE codigo_cliente IS NULL
                """))
                print("   ✓ Códigos temporales generados")
            
            # 4. Hacer codigo_cliente NOT NULL
            print("4. Haciendo codigo_cliente NOT NULL...")
            await session.execute(text("""
                ALTER TABLE proveedores 
                ALTER COLUMN codigo_cliente SET NOT NULL
            """))
            print("   ✓ Columna codigo_cliente ahora es NOT NULL")
            
            # 5. Renombrar codigo_cliente a codigo_proveedor
            print("5. Renombrando codigo_cliente a codigo_proveedor...")
            await session.execute(text("""
                ALTER TABLE proveedores 
                RENAME COLUMN codigo_cliente TO codigo_proveedor
            """))
            print("   ✓ Columna renombrada")
            
            # 6. Eliminar la columna id y hacer codigo_proveedor la PK
            print("6. Eliminando columna id y estableciendo codigo_proveedor como PK...")
            
            # Primero eliminar la constraint de primary key en id
            await session.execute(text("""
                ALTER TABLE proveedores 
                DROP CONSTRAINT IF EXISTS proveedores_pkey
            """))
            
            # Eliminar la columna id
            await session.execute(text("""
                ALTER TABLE proveedores 
                DROP COLUMN IF EXISTS id
            """))
            print("   ✓ Columna id eliminada")
            
            # Establecer codigo_proveedor como primary key
            await session.execute(text("""
                ALTER TABLE proveedores 
                ADD PRIMARY KEY (codigo_proveedor)
            """))
            print("   ✓ codigo_proveedor establecido como PRIMARY KEY")
            
            # 7. Recrear la foreign key en precios_materiales
            if precios_table_exists:
                print("7. Recreando foreign key en precios_materiales...")
                await session.execute(text("""
                    ALTER TABLE precios_materiales 
                    ADD CONSTRAINT precios_materiales_codigo_proveedor_fkey 
                    FOREIGN KEY (codigo_proveedor) 
                    REFERENCES proveedores(codigo_proveedor)
                """))
                print("   ✓ Foreign key recreada")
            
            # 8. Actualizar el unique constraint en precios_materiales si existe
            if precios_table_exists:
                print("8. Actualizando unique constraint en precios_materiales...")
                # Eliminar el constraint antiguo si existe
                await session.execute(text("""
                    ALTER TABLE precios_materiales 
                    DROP CONSTRAINT IF EXISTS uq_precios_materiales_proveedor_material
                """))
                # Crear el nuevo constraint
                await session.execute(text("""
                    ALTER TABLE precios_materiales 
                    ADD CONSTRAINT uq_precios_materiales_proveedor_material 
                    UNIQUE (codigo_proveedor, numero_material)
                """))
                print("   ✓ Unique constraint actualizado")
            
            await session.commit()
            print("\n✓ Migración de tabla proveedores completada exitosamente")
            return True
            
        except Exception as e:
            await session.rollback()
            raise e


async def show_table_info():
    """Muestra información de la tabla actualizada."""
    async with AsyncSessionLocal() as session:
        # Información de la tabla proveedores
        result = await session.execute(text("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'proveedores'
            ORDER BY ordinal_position;
        """))
        
        print("\n" + "="*60)
        print("TABLA: proveedores")
        print("="*60)
        print(f"{'Columna':<25} {'Tipo':<25} {'Nullable':<10} {'Default'}")
        print("-"*60)
        for row in result:
            default = str(row[3])[:20] if row[3] else ''
            print(f"{row[0]:<25} {row[1]:<25} {row[2]:<10} {default}")
        
        # Mostrar constraints
        result = await session.execute(text("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'proveedores'
            ORDER BY constraint_type, constraint_name;
        """))
        
        print("\n" + "="*60)
        print("CONSTRAINTS")
        print("="*60)
        for row in result:
            print(f"• {row[0]} ({row[1]})")


async def main():
    """Función principal de migración."""
    print("\n" + "="*60)
    print("MIGRACIÓN DE TABLA PROVEEDORES")
    print("="*60 + "\n")
    
    try:
        # Ejecutar migración
        success = await migrate_proveedores_table()
        
        if success:
            # Mostrar información de la tabla
            await show_table_info()
            
            print("\n" + "="*60)
            print("✓ MIGRACIÓN COMPLETADA EXITOSAMENTE")
            print("="*60 + "\n")
        else:
            print("\n✗ La migración falló")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n✗ Error durante la migración: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

