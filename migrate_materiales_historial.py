"""
Script de migración para crear la tabla materiales_historial.

Uso:
    python migrate_materiales_historial.py

Este script:
1. Crea el enum material_operacion_enum si no existe
2. Crea la tabla materiales_historial con todos los campos necesarios
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


async def migrate_materiales_historial():
    """Crea la tabla materiales_historial."""
    async with AsyncSessionLocal() as session:
        try:
            # 1. Crear el enum si no existe
            enum_name = "material_operacion_enum"
            if not await check_enum_exists(enum_name):
                print("1. Creando enum material_operacion_enum...")
                await session.execute(
                    text("""
                        CREATE TYPE material_operacion_enum AS ENUM ('CREATE', 'UPDATE', 'DELETE')
                    """)
                )
                await session.commit()
                print("   ✓ Enum creado exitosamente")
            else:
                print("1. ✓ El enum material_operacion_enum ya existe")
            
            # 2. Crear la tabla si no existe
            table_name = "materiales_historial"
            if not await check_table_exists(table_name):
                print("2. Creando tabla materiales_historial...")
                await session.execute(
                    text("""
                        CREATE TABLE materiales_historial (
                            id SERIAL PRIMARY KEY,
                            numero_material VARCHAR,
                            material_id INTEGER,
                            operacion material_operacion_enum NOT NULL,
                            user_id INTEGER NOT NULL,
                            datos_antes JSONB,
                            datos_despues JSONB,
                            campos_modificados JSONB,
                            comentario TEXT,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                            CONSTRAINT fk_materiales_historial_user 
                                FOREIGN KEY (user_id) 
                                REFERENCES users(id)
                        )
                    """)
                )
                await session.commit()
                print("   ✓ Tabla creada exitosamente")
            else:
                print("2. ✓ La tabla materiales_historial ya existe")
            
            # 3. Crear índices si no existen
            print("3. Creando índices...")
            
            # Índice en numero_material
            await session.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS ix_materiales_historial_numero_material 
                    ON materiales_historial(numero_material)
                """)
            )
            
            # Índice en material_id
            await session.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS ix_materiales_historial_material_id 
                    ON materiales_historial(material_id)
                """)
            )
            
            # Índice en operacion
            await session.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS ix_materiales_historial_operacion 
                    ON materiales_historial(operacion)
                """)
            )
            
            # Índice en user_id
            await session.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS ix_materiales_historial_user_id 
                    ON materiales_historial(user_id)
                """)
            )
            
            # Índice en created_at
            await session.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS ix_materiales_historial_created_at 
                    ON materiales_historial(created_at)
                """)
            )
            
            await session.commit()
            print("   ✓ Índices creados exitosamente")
            
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
    print("MIGRACIÓN: Tabla materiales_historial")
    print("="*50)
    print()
    
    success = await migrate_materiales_historial()
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
