"""
Script para importar datos de BOM desde un archivo Excel a las tablas parts y bom_flat.

Uso:
    python import_bom_excel.py <ruta_archivo_excel>
    
Ejemplo:
    python import_bom_excel.py BillMaterialesDATA_7JB99.xlsx

Columnas esperadas del Excel:
    - Parte No
    - Plant
    - Description
    - Base Mts
    - Req D
    - Material
    - Description Material
    - Qty
    - Measure
    - Origen
    - Precio Venta
"""
import asyncio
import sys
import os
from decimal import Decimal, InvalidOperation
import pandas as pd
from sqlalchemy import text
from app.db.base import AsyncSessionLocal
from app.db.models import PartRole


def clean_decimal(value, default=None):
    """Convierte un valor a Decimal, manejando valores nulos y errores."""
    if pd.isna(value) or value == '' or value is None:
        return default
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return default


def clean_string(value):
    """Limpia un valor string, manejando valores nulos."""
    if pd.isna(value) or value is None:
        return None
    return str(value).strip()


async def clear_tables():
    """Limpia las tablas antes de importar."""
    async with AsyncSessionLocal() as session:
        print("Limpiando tablas existentes...")
        await session.execute(text("DELETE FROM bom_flat"))
        await session.execute(text("DELETE FROM parts"))
        await session.commit()
        print("✓ Tablas limpiadas")


async def import_parts(df):
    """Importa las partes únicas (FG y materiales) a la tabla parts."""
    print("\nImportando partes...")
    
    # Recopilar todas las partes únicas
    parts_data = {}
    
    for _, row in df.iterrows():
        parte_no = clean_string(row.get('Parte No'))
        description = clean_string(row.get('Description'))
        material = clean_string(row.get('Material'))
        description_material = clean_string(row.get('Description Material'))
        
        # Agregar Parte No como FG
        if parte_no and parte_no not in parts_data:
            parts_data[parte_no] = {
                'part_no': parte_no,
                'description': description,
                'part_role': PartRole.FG.value
            }
        
        # Agregar Material como COMP
        if material and material not in parts_data:
            parts_data[material] = {
                'part_no': material,
                'description': description_material,
                'part_role': PartRole.COMP.value
            }
    
    # Insertar en lotes
    parts_list = list(parts_data.values())
    batch_size = 500
    inserted = 0
    
    async with AsyncSessionLocal() as session:
        for i in range(0, len(parts_list), batch_size):
            batch = parts_list[i:i + batch_size]
            
            for part in batch:
                await session.execute(
                    text("""
                        INSERT INTO parts (part_no, description, part_role, created_at, updated_at)
                        VALUES (:part_no, :description, :part_role, now(), now())
                        ON CONFLICT (part_no) DO UPDATE SET
                            description = COALESCE(EXCLUDED.description, parts.description),
                            updated_at = now()
                    """),
                    part
                )
            
            await session.commit()
            inserted += len(batch)
            print(f"  Insertadas {inserted}/{len(parts_list)} partes...")
    
    print(f"✓ {len(parts_list)} partes importadas")
    return len(parts_data)


async def import_bom_flat(df):
    """Importa los registros BOM a la tabla bom_flat."""
    print("\nImportando registros BOM...")
    
    batch_size = 500
    inserted = 0
    errors = 0
    skipped = 0
    
    async with AsyncSessionLocal() as session:
        batch = []
        
        for index, row in df.iterrows():
            parte_no = clean_string(row.get('Parte No'))
            plant = clean_string(row.get('Plant'))
            material = clean_string(row.get('Material'))
            qty = clean_decimal(row.get('Qty'))
            measure = clean_string(row.get('Measure'))
            
            # Validar campos requeridos
            if not parte_no or not material or not qty or not measure or not plant:
                skipped += 1
                continue
            
            # Validar que qty sea mayor a 0
            if qty <= 0:
                skipped += 1
                continue
            
            bom_data = {
                "fg_part_no": parte_no,
                "plant_code": plant,
                "base_mts": float(clean_decimal(row.get('Base Mts')) or 0),
                "req_d": float(clean_decimal(row.get('Req D')) or 0),
                "material": material,
                "material_description": clean_string(row.get('Description Material')),
                "qty": float(qty),
                "uom": measure,
                "origin_country": clean_string(row.get('Origen')),
                "sale_price": float(clean_decimal(row.get('Precio Venta')) or 0)
            }
            
            batch.append(bom_data)
            
            # Insertar en lotes
            if len(batch) >= batch_size:
                try:
                    for bom in batch:
                        await session.execute(
                            text("""
                                INSERT INTO bom_flat (
                                    fg_part_no, plant_code, base_mts, req_d, material, 
                                    material_description, qty, uom, origin_country, sale_price, created_at
                                )
                                VALUES (
                                    :fg_part_no, :plant_code, :base_mts, :req_d, :material,
                                    :material_description, :qty, :uom, :origin_country, :sale_price, now()
                                )
                            """),
                            bom
                        )
                    await session.commit()
                    inserted += len(batch)
                    print(f"  Insertados {inserted} registros BOM...")
                except Exception as e:
                    await session.rollback()
                    errors += len(batch)
                    print(f"  ✗ Error en lote: {e}")
                
                batch = []
        
        # Insertar el último lote
        if batch:
            try:
                for bom in batch:
                    await session.execute(
                        text("""
                            INSERT INTO bom_flat (
                                fg_part_no, plant_code, base_mts, req_d, material, 
                                material_description, qty, uom, origin_country, sale_price, created_at
                            )
                            VALUES (
                                :fg_part_no, :plant_code, :base_mts, :req_d, :material,
                                :material_description, :qty, :uom, :origin_country, :sale_price, now()
                            )
                        """),
                        bom
                    )
                await session.commit()
                inserted += len(batch)
            except Exception as e:
                await session.rollback()
                errors += len(batch)
                print(f"  ✗ Error en último lote: {e}")
    
    print(f"✓ {inserted} registros BOM importados")
    if skipped > 0:
        print(f"  ⚠ {skipped} filas saltadas (datos incompletos)")
    if errors > 0:
        print(f"  ✗ {errors} errores")
    
    return inserted, errors, skipped


async def import_excel(file_path: str):
    """Importa datos del Excel a las tablas de BOM."""
    
    # Verificar que el archivo existe
    if not os.path.exists(file_path):
        print(f"✗ Error: El archivo '{file_path}' no existe")
        sys.exit(1)
    
    print(f"\nLeyendo archivo: {file_path}")
    
    # Leer el archivo Excel
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
    except Exception as e:
        print(f"✗ Error al leer el archivo Excel: {e}")
        sys.exit(1)
    
    print(f"Filas encontradas: {len(df)}")
    print(f"Columnas encontradas: {list(df.columns)}")
    
    # Verificar columnas requeridas
    required_columns = ['Parte No', 'Plant', 'Material', 'Qty', 'Measure']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"✗ Error: Faltan columnas requeridas: {missing_columns}")
        sys.exit(1)
    
    # Limpiar tablas existentes
    await clear_tables()
    
    # Importar partes
    parts_count = await import_parts(df)
    
    # Importar BOM
    bom_inserted, bom_errors, bom_skipped = await import_bom_flat(df)
    
    # Resumen
    print("\n" + "="*50)
    print("RESUMEN DE IMPORTACIÓN")
    print("="*50)
    print(f"Total filas en Excel: {len(df)}")
    print(f"Partes únicas importadas: {parts_count}")
    print(f"Registros BOM importados: {bom_inserted}")
    print(f"Filas saltadas: {bom_skipped}")
    print(f"Errores: {bom_errors}")
    print("="*50)
    
    if bom_errors == 0:
        print("\n✓ IMPORTACIÓN COMPLETADA EXITOSAMENTE")
    else:
        print(f"\n⚠ IMPORTACIÓN COMPLETADA CON {bom_errors} ERRORES")


async def show_sample_data():
    """Muestra una muestra de los datos importados."""
    async with AsyncSessionLocal() as session:
        # Contar registros
        result = await session.execute(text("SELECT COUNT(*) FROM parts"))
        parts_count = result.scalar()
        
        result = await session.execute(text("SELECT COUNT(*) FROM bom_flat"))
        bom_count = result.scalar()
        
        print(f"\nDatos en la base de datos:")
        print(f"  - Partes: {parts_count}")
        print(f"  - Registros BOM: {bom_count}")
        
        # Muestra de bom_flat
        result = await session.execute(text("""
            SELECT fg_part_no, plant_code, material, material_description, qty, uom
            FROM bom_flat
            LIMIT 5
        """))
        
        rows = result.fetchall()
        if rows:
            print("\nMuestra de registros BOM:")
            print("-"*80)
            for row in rows:
                desc = row[3][:30] + "..." if row[3] and len(row[3]) > 30 else (row[3] or 'N/A')
                print(f"  FG: {row[0]}, Plant: {row[1]}, Material: {row[2]}, Desc: {desc}, Qty: {row[4]} {row[5]}")


async def main():
    """Función principal."""
    if len(sys.argv) < 2:
        print("Uso: python import_bom_excel.py <ruta_archivo_excel>")
        print("Ejemplo: python import_bom_excel.py BillMaterialesDATA_7JB99.xlsx")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    print("\n" + "="*50)
    print("IMPORTACIÓN DE BOM DESDE EXCEL")
    print("="*50)
    
    await import_excel(file_path)
    await show_sample_data()


if __name__ == "__main__":
    asyncio.run(main())
