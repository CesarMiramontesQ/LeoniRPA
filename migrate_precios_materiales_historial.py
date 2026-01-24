"""
Script de migración para crear la tabla precios_materiales_historial.

Uso:
    python migrate_precios_materiales_historial.py

Este script:
1. Crea el enum precio_material_operacion_enum si no existe
2. Crea la tabla precios_materiales_historial con todos los campos necesarios
3. Crea los índices necesarios
"""
import asyncio
import sys
from sqlalchemy import text
from app.db.base import AsyncSessionLocal


async def check_enum_exists(enum_name: str) -> bool:
    """Verifica si un enum existe en la base de datos."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM pg_type 
                    WHERE typname = :enum_name
                )
            """),
            {"enum_name": enum_name}
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


async def check_index_exists(index_name: str) -> bool:
    """Verifica si un índice existe en la base de datos."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM pg_indexes 
                    WHERE indexname = :index_name
                )
            """),
            {"index_name": index_name}
        )
        return result.scalar()


async def migrate_precios_materiales_historial():
    """Crea la tabla precios_materiales_historial."""
    async with AsyncSessionLocal() as session:
        try:
            # 1. Crear el enum si no existe
            enum_name = "precio_material_operacion_enum"
            if not await check_enum_exists(enum_name):
                print("1. Creando enum precio_material_operacion_enum...")
                await session.execute(
                    text("""
                        CREATE TYPE precio_material_operacion_enum AS ENUM ('CREATE', 'UPDATE', 'DELETE')
                    """)
                )
                await session.commit()
                print("   ✓ Enum creado exitosamente")
            else:
                print("1. ✓ El enum precio_material_operacion_enum ya existe")
            
            # 2. Crear la tabla si no existe
            table_name = "precios_materiales_historial"
            if not await check_table_exists(table_name):
                print("2. Creando tabla precios_materiales_historial...")
                await session.execute(
                    text("""
                        CREATE TABLE precios_materiales_historial (
                            id SERIAL PRIMARY KEY,
                            precio_material_id INTEGER,
                            codigo_proveedor VARCHAR,
                            numero_material VARCHAR,
                            operacion precio_material_operacion_enum NOT NULL,
                            user_id INTEGER NOT NULL,
                            datos_antes JSONB,
                            datos_despues JSONB,
                            campos_modificados JSONB,
                            comentario TEXT,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                            CONSTRAINT fk_precios_materiales_historial_user 
                                FOREIGN KEY (user_id) 
                                REFERENCES users(id)
                        )
                    """)
                )
                await session.commit()
                print("   ✓ Tabla creada exitosamente")
            else:
                print("2. ✓ La tabla precios_materiales_historial ya existe")
            
            # 3. Crear índices si no existen
            print("3. Creando índices...")
            
            # Índice en precio_material_id
            index_name = "ix_precios_materiales_historial_precio_material_id"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON precios_materiales_historial(precio_material_id)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            # Índice en codigo_proveedor
            index_name = "ix_precios_materiales_historial_codigo_proveedor"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON precios_materiales_historial(codigo_proveedor)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            # Índice en numero_material
            index_name = "ix_precios_materiales_historial_numero_material"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON precios_materiales_historial(numero_material)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            # Índice en operacion
            index_name = "ix_precios_materiales_historial_operacion"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON precios_materiales_historial(operacion)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            # Índice en user_id
            index_name = "ix_precios_materiales_historial_user_id"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON precios_materiales_historial(user_id)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            # Índice en created_at
            index_name = "ix_precios_materiales_historial_created_at"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON precios_materiales_historial(created_at)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            await session.commit()
            print("\n   ✓ Todos los índices verificados/creados exitosamente")
            
            print("\n" + "="*50)
            print("✓ MIGRACIÓN COMPLETADA EXITOSAMENTE")
            print("="*50)
            return True
            
        except Exception as e:
            await session.rollback()
            print(f"\n✗ Error durante la migración: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


async def main():
    """Función principal."""
    print("="*50)
    print("MIGRACIÓN: Tabla precios_materiales_historial")
    print("="*50)
    print()
    
    success = await migrate_precios_materiales_historial()
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
