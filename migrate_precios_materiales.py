"""
Script de migración para crear la tabla de precios de materiales.

Uso:
    python migrate_precios_materiales.py

Este script:
1. Crea la tabla `precios_materiales` con todas sus columnas
2. Crea los índices y restricciones necesarios
3. Establece las claves foráneas a proveedores y materiales
"""
import asyncio
import sys
from sqlalchemy import text
from app.db.base import engine, AsyncSessionLocal


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


async def create_precios_materiales_table():
    """Crea la tabla precios_materiales si no existe."""
    async with AsyncSessionLocal() as session:
        # Crear la tabla precios_materiales
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS precios_materiales (
                id SERIAL PRIMARY KEY,
                codigo_cliente TEXT NOT NULL REFERENCES proveedores(codigo_cliente),
                numero_material TEXT NOT NULL REFERENCES materiales(numero_material),
                precio NUMERIC(18, 6) NOT NULL,
                currency_uom TEXT NULL,
                country_origin TEXT NULL,
                "Porcentaje_Compra" NUMERIC(18, 6) NULL,
                "Comentario" TEXT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                
                -- Constraint único: un solo precio vigente por combinación de proveedor y material
                CONSTRAINT uq_precios_materiales_proveedor_material 
                    UNIQUE (codigo_cliente, numero_material)
            )
        """))
        
        # Crear índice en codigo_cliente
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_precios_materiales_codigo_cliente 
            ON precios_materiales(codigo_cliente)
        """))
        
        # Crear índice en numero_material
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_precios_materiales_numero_material 
            ON precios_materiales(numero_material)
        """))
        
        # Crear índice compuesto para búsquedas rápidas
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_precios_materiales_proveedor_material 
            ON precios_materiales(codigo_cliente, numero_material)
        """))
        
        await session.commit()
        print("✓ Tabla 'precios_materiales' creada exitosamente")


async def create_update_timestamp_trigger():
    """Crea un trigger para actualizar updated_at automáticamente."""
    async with AsyncSessionLocal() as session:
        # Crear la función del trigger (si no existe)
        await session.execute(text("""
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = now();
                RETURN NEW;
            END;
            $$ language 'plpgsql'
        """))
        
        # Eliminar el trigger si existe
        await session.execute(text("""
            DROP TRIGGER IF EXISTS update_precios_materiales_updated_at ON precios_materiales
        """))
        
        # Crear el trigger en la tabla precios_materiales
        await session.execute(text("""
            CREATE TRIGGER update_precios_materiales_updated_at
                BEFORE UPDATE ON precios_materiales
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column()
        """))
        
        await session.commit()
        print("✓ Trigger 'update_updated_at' creado exitosamente")


async def show_table_info():
    """Muestra información de la tabla creada."""
    async with AsyncSessionLocal() as session:
        # Información de la tabla precios_materiales
        result = await session.execute(text("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'precios_materiales'
            ORDER BY ordinal_position;
        """))
        
        print("\n" + "="*60)
        print("TABLA: precios_materiales")
        print("="*60)
        print(f"{'Columna':<25} {'Tipo':<25} {'Nullable':<10} {'Default'}")
        print("-"*60)
        for row in result:
            default = str(row[3])[:20] if row[3] else ''
            print(f"{row[0]:<25} {row[1]:<25} {row[2]:<10} {default}")
        
        # Mostrar índices
        result = await session.execute(text("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'precios_materiales'
            ORDER BY indexname;
        """))
        
        print("\n" + "="*60)
        print("ÍNDICES")
        print("="*60)
        for row in result:
            print(f"• {row[0]}")
        
        # Mostrar constraints
        result = await session.execute(text("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'precios_materiales'
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
    print("MIGRACIÓN DE TABLA PRECIOS_MATERIALES")
    print("="*60 + "\n")
    
    try:
        # Verificar si la tabla ya existe
        table_exists = await check_table_exists("precios_materiales")
        
        if table_exists:
            print("⚠ La tabla 'precios_materiales' ya existe")
        else:
            await create_precios_materiales_table()
        
        # Crear trigger para updated_at
        await create_update_timestamp_trigger()
        
        # Mostrar información de la tabla
        await show_table_info()
        
        print("\n" + "="*60)
        print("✓ MIGRACIÓN COMPLETADA EXITOSAMENTE")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n✗ Error durante la migración: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

