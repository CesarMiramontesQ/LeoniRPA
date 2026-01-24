"""
Script de migración para crear la tabla pais_origen_material_historial.

Uso:
    python migrate_paises_origen_historial.py

Este script:
1. Crea el enum pais_origen_material_operacion_enum si no existe
2. Crea la tabla pais_origen_material_historial con todos los campos necesarios
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


async def migrate_paises_origen_historial():
    """Crea la tabla pais_origen_material_historial."""
    async with AsyncSessionLocal() as session:
        try:
            # 1. Crear el enum si no existe
            enum_name = "pais_origen_material_operacion_enum"
            if not await check_enum_exists(enum_name):
                print("1. Creando enum pais_origen_material_operacion_enum...")
                await session.execute(
                    text("""
                        CREATE TYPE pais_origen_material_operacion_enum AS ENUM ('CREATE', 'UPDATE', 'DELETE')
                    """)
                )
                await session.commit()
                print("   ✓ Enum creado exitosamente")
            else:
                print("1. ✓ El enum pais_origen_material_operacion_enum ya existe")
            
            # 2. Crear la tabla si no existe
            table_name = "pais_origen_material_historial"
            if not await check_table_exists(table_name):
                print("2. Creando tabla pais_origen_material_historial...")
                await session.execute(
                    text("""
                        CREATE TABLE pais_origen_material_historial (
                            id SERIAL PRIMARY KEY,
                            pais_origen_id INTEGER,
                            codigo_proveedor VARCHAR,
                            numero_material VARCHAR,
                            operacion pais_origen_material_operacion_enum NOT NULL,
                            user_id INTEGER NOT NULL,
                            datos_antes JSONB,
                            datos_despues JSONB,
                            campos_modificados JSONB,
                            comentario TEXT,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                            CONSTRAINT fk_pais_origen_material_historial_user 
                                FOREIGN KEY (user_id) 
                                REFERENCES users(id)
                        )
                    """)
                )
                await session.commit()
                print("   ✓ Tabla creada exitosamente")
            else:
                print("2. ✓ La tabla pais_origen_material_historial ya existe")
            
            # 3. Crear índices si no existen
            print("3. Creando índices...")
            
            # Índice en pais_origen_id
            index_name = "ix_pais_origen_material_historial_pais_origen_id"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON pais_origen_material_historial(pais_origen_id)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            # Índice en codigo_proveedor
            index_name = "ix_pais_origen_material_historial_codigo_proveedor"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON pais_origen_material_historial(codigo_proveedor)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            # Índice en numero_material
            index_name = "ix_pais_origen_material_historial_numero_material"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON pais_origen_material_historial(numero_material)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            # Índice en operacion
            index_name = "ix_pais_origen_material_historial_operacion"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON pais_origen_material_historial(operacion)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            # Índice en user_id
            index_name = "ix_pais_origen_material_historial_user_id"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON pais_origen_material_historial(user_id)
                    """)
                )
                print(f"   ✓ Índice {index_name} creado")
            else:
                print(f"   ✓ Índice {index_name} ya existe")
            
            # Índice en created_at
            index_name = "ix_pais_origen_material_historial_created_at"
            if not await check_index_exists(index_name):
                await session.execute(
                    text(f"""
                        CREATE INDEX {index_name} 
                        ON pais_origen_material_historial(created_at)
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
    print("MIGRACIÓN: Tabla pais_origen_material_historial")
    print("="*50)
    print()
    
    success = await migrate_paises_origen_historial()
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
