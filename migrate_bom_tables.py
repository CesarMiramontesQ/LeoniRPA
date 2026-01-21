"""
Script de migración para crear las tablas de BOM (parts y bom_flat).

Uso:
    python migrate_bom_tables.py

Este script:
1. Crea la tabla `parts` (catálogo único de números de parte)
2. Crea la tabla `bom_flat` (relaciones BOM planas)
3. Crea los índices y restricciones necesarios
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


async def create_parts_table():
    """Crea la tabla parts si no existe."""
    async with AsyncSessionLocal() as session:
        # Crear el tipo ENUM para part_role si no existe
        await session.execute(text("""
            DO $$ BEGIN
                CREATE TYPE part_role_enum AS ENUM ('FG', 'COMP', 'UNKNOWN');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """))
        
        # Crear la tabla parts
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS parts (
                part_no TEXT PRIMARY KEY,
                description TEXT NULL,
                part_role part_role_enum NULL DEFAULT 'UNKNOWN',
                raw_data JSONB NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        
        # Crear índice en part_no (ya es PK pero agregamos si se necesita búsqueda adicional)
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_parts_part_no ON parts(part_no);
        """))
        
        # Crear índice en part_role para filtrar por tipo
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_parts_part_role ON parts(part_role);
        """))
        
        await session.commit()
        print("✓ Tabla 'parts' creada exitosamente")


async def create_bom_flat_table():
    """Crea la tabla bom_flat si no existe."""
    async with AsyncSessionLocal() as session:
        # Crear la tabla bom_flat
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS bom_flat (
                id BIGSERIAL PRIMARY KEY,
                fg_part_no TEXT NOT NULL REFERENCES parts(part_no),
                plant_code TEXT NOT NULL,
                base_mts NUMERIC(18,3) NULL,
                req_d NUMERIC(18,3) NULL,
                material TEXT NOT NULL REFERENCES parts(part_no),
                material_description TEXT NULL,
                qty NUMERIC(18,6) NOT NULL,
                uom TEXT NOT NULL,
                origin_country TEXT NULL,
                sale_price NUMERIC(18,6) NULL,
                run_id BIGINT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                
                -- Constraint: qty debe ser mayor a 0
                CONSTRAINT ck_bom_flat_qty_positive CHECK (qty > 0)
            )
        """))
        
        # Crear índice en fg_part_no
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bom_flat_fg_part_no ON bom_flat(fg_part_no)
        """))
        
        # Crear índice en material
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bom_flat_material ON bom_flat(material)
        """))
        
        # Crear índice compuesto en plant_code y fg_part_no
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bom_flat_plant_fg ON bom_flat(plant_code, fg_part_no)
        """))
        
        # Crear índice en run_id para histórico
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bom_flat_run_id ON bom_flat(run_id)
        """))
        
        # Crear constraint único para evitar duplicados
        # Usamos COALESCE para manejar NULL en run_id
        await session.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_bom_flat_fg_plant_material_run 
            ON bom_flat(fg_part_no, plant_code, material, COALESCE(run_id, 0))
        """))
        
        await session.commit()
        print("✓ Tabla 'bom_flat' creada exitosamente")


async def create_update_timestamp_trigger():
    """Crea un trigger para actualizar updated_at automáticamente en la tabla parts."""
    async with AsyncSessionLocal() as session:
        # Crear la función del trigger
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
            DROP TRIGGER IF EXISTS update_parts_updated_at ON parts
        """))
        
        # Crear el trigger en la tabla parts
        await session.execute(text("""
            CREATE TRIGGER update_parts_updated_at
                BEFORE UPDATE ON parts
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column()
        """))
        
        await session.commit()
        print("✓ Trigger 'update_updated_at' creado exitosamente")


async def show_table_info():
    """Muestra información de las tablas creadas."""
    async with AsyncSessionLocal() as session:
        # Información de la tabla parts
        result = await session.execute(text("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'parts'
            ORDER BY ordinal_position;
        """))
        
        print("\n" + "="*60)
        print("TABLA: parts")
        print("="*60)
        print(f"{'Columna':<20} {'Tipo':<25} {'Nullable':<10} {'Default'}")
        print("-"*60)
        for row in result:
            default = str(row[3])[:20] if row[3] else ''
            print(f"{row[0]:<20} {row[1]:<25} {row[2]:<10} {default}")
        
        # Información de la tabla bom_flat
        result = await session.execute(text("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'bom_flat'
            ORDER BY ordinal_position;
        """))
        
        print("\n" + "="*60)
        print("TABLA: bom_flat")
        print("="*60)
        print(f"{'Columna':<20} {'Tipo':<25} {'Nullable':<10} {'Default'}")
        print("-"*60)
        for row in result:
            default = str(row[3])[:20] if row[3] else ''
            print(f"{row[0]:<20} {row[1]:<25} {row[2]:<10} {default}")
        
        # Mostrar índices
        result = await session.execute(text("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename IN ('parts', 'bom_flat')
            ORDER BY tablename, indexname;
        """))
        
        print("\n" + "="*60)
        print("ÍNDICES")
        print("="*60)
        for row in result:
            print(f"• {row[0]}")


async def main():
    """Función principal de migración."""
    print("\n" + "="*60)
    print("MIGRACIÓN DE TABLAS BOM")
    print("="*60 + "\n")
    
    try:
        # Verificar si las tablas ya existen
        parts_exists = await check_table_exists("parts")
        bom_flat_exists = await check_table_exists("bom_flat")
        
        if parts_exists:
            print("⚠ La tabla 'parts' ya existe")
        else:
            await create_parts_table()
        
        if bom_flat_exists:
            print("⚠ La tabla 'bom_flat' ya existe")
        else:
            await create_bom_flat_table()
        
        # Crear trigger para updated_at
        await create_update_timestamp_trigger()
        
        # Mostrar información de las tablas
        await show_table_info()
        
        print("\n" + "="*60)
        print("✓ MIGRACIÓN COMPLETADA EXITOSAMENTE")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n✗ Error durante la migración: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
