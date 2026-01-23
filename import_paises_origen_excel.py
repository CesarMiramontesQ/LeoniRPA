"""
Script para importar datos de países de origen de materiales desde un archivo Excel a la tabla pais_origen_material.

Uso:
    python import_paises_origen_excel.py <ruta_archivo_excel>
    
Ejemplo:
    python import_paises_origen_excel.py "paises_origen.xlsx"

Columnas esperadas del Excel:
    - Proveedor (requerido) - FK a proveedores.codigo_proveedor
    - Material Number (requerido) - FK a materiales.numero_material
    - Country Origin (requerido) - País de origen del material

Si un registro ya existe (por codigo_proveedor + numero_material), se actualiza.
Si un proveedor o material no existe, se reporta como error.
"""
import asyncio
import sys
import os
import pandas as pd
from app.db.base import AsyncSessionLocal
from app.db import crud


def clean_string(value):
    """Limpia un valor string, manejando valores nulos."""
    if pd.isna(value) or value is None:
        return None
    result = str(value).strip()
    return result if result else None


async def import_paises_origen(file_path: str):
    """Importa datos del Excel a la tabla de pais_origen_material."""
    
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
    
    # Normalizar nombres de columnas (sin espacios extra)
    df.columns = [str(col).strip() for col in df.columns]
    
    # Buscar columnas por diferentes nombres posibles
    proveedor_col = None
    material_number_col = None
    country_origin_col = None
    
    # Buscar columna de Proveedor
    for col in df.columns:
        col_lower = col.lower().replace('_', ' ').replace('-', ' ')
        if any(keyword in col_lower for keyword in ['proveedor', 'supplier', 'codigo proveedor', 
                                                     'código proveedor', 'codigo_proveedor', 
                                                     'código_proveedor', 'codigo cliente', 
                                                     'código cliente', 'codigo_cliente', 
                                                     'código_cliente']):
            proveedor_col = col
            break
    
    # Buscar columna de Material Number
    for col in df.columns:
        col_lower = col.lower().replace('_', ' ').replace('-', ' ')
        if any(keyword in col_lower for keyword in ['material number', 'material_number', 
                                                     'numero de material', 'número de material',
                                                     'numero material', 'número material',
                                                     'numero_material', 'número_material',
                                                     'material_no', 'nro material']):
            material_number_col = col
            break
    
    # Buscar columna de Country Origin
    for col in df.columns:
        col_lower = col.lower().replace('_', ' ').replace('-', ' ')
        if any(keyword in col_lower for keyword in ['country origin', 'country_origin', 
                                                     'país origen', 'pais origen', 
                                                     'origen', 'origin', 'country',
                                                     'pais_origen', 'país_origen']):
            country_origin_col = col
            break
    
    # Verificar columnas requeridas
    if proveedor_col is None:
        print(f"✗ Error: No se encontró la columna 'Proveedor' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    if material_number_col is None:
        print(f"✗ Error: No se encontró la columna 'Material Number' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    if country_origin_col is None:
        print(f"✗ Error: No se encontró la columna 'Country Origin' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    print(f"\nColumnas mapeadas:")
    print(f"  - Proveedor: {proveedor_col}")
    print(f"  - Material Number: {material_number_col}")
    print(f"  - Country Origin: {country_origin_col}")
    
    # Importar países de origen
    inserted = 0
    updated = 0
    errors = 0
    skipped = 0
    not_found_proveedor = 0
    not_found_material = 0
    
    async with AsyncSessionLocal() as session:
        for index, row in df.iterrows():
            try:
                # Obtener valores
                codigo_proveedor = clean_string(row.get(proveedor_col))
                numero_material = clean_string(row.get(material_number_col))
                pais_origen = clean_string(row.get(country_origin_col))
                
                # Validar campos requeridos
                if not codigo_proveedor:
                    skipped += 1
                    print(f"  Fila {index + 2}: Saltada (sin código de proveedor)")
                    continue
                
                if not numero_material:
                    skipped += 1
                    print(f"  Fila {index + 2}: Saltada (sin número de material)")
                    continue
                
                if not pais_origen:
                    skipped += 1
                    print(f"  Fila {index + 2}: Saltada (sin país de origen)")
                    continue
                
                # Verificar que el proveedor existe
                proveedor = await crud.get_proveedor_by_codigo_proveedor(session, codigo_proveedor)
                if not proveedor:
                    not_found_proveedor += 1
                    print(f"  Fila {index + 2}: Error - Proveedor con código '{codigo_proveedor}' no encontrado")
                    errors += 1
                    continue
                
                # Verificar que el material existe
                material = await crud.get_material_by_numero(session, numero_material)
                if not material:
                    not_found_material += 1
                    print(f"  Fila {index + 2}: Error - Material con número '{numero_material}' no encontrado")
                    errors += 1
                    continue
                
                # Verificar si el registro ya existe
                existing = await crud.get_pais_origen_material_by_proveedor_material(
                    session,
                    codigo_proveedor,
                    numero_material
                )
                
                if existing:
                    # Actualizar registro existente
                    await crud.update_pais_origen_material(
                        session,
                        existing.id,
                        pais_origen=pais_origen
                    )
                    updated += 1
                    print(f"  Fila {index + 2}: Actualizado - {codigo_proveedor} / {numero_material} = {pais_origen}")
                else:
                    # Crear nuevo registro
                    await crud.create_pais_origen_material(
                        session,
                        codigo_proveedor=codigo_proveedor,
                        numero_material=numero_material,
                        pais_origen=pais_origen
                    )
                    inserted += 1
                    print(f"  Fila {index + 2}: Insertado - {codigo_proveedor} / {numero_material} = {pais_origen}")
                    
            except Exception as e:
                errors += 1
                print(f"  Fila {index + 2}: Error - {str(e)}")
                import traceback
                traceback.print_exc()
                continue
    
    # Resumen
    print("\n" + "="*50)
    print("RESUMEN DE IMPORTACIÓN")
    print("="*50)
    print(f"Total filas en Excel: {len(df)}")
    print(f"Registros insertados: {inserted}")
    print(f"Registros actualizados: {updated}")
    print(f"Filas saltadas (datos faltantes): {skipped}")
    print(f"Errores (proveedor no encontrado): {not_found_proveedor}")
    print(f"Errores (material no encontrado): {not_found_material}")
    print(f"Errores totales: {errors}")
    print("="*50)
    
    if errors == 0:
        print("\n✓ IMPORTACIÓN COMPLETADA EXITOSAMENTE")
    else:
        print(f"\n⚠ IMPORTACIÓN COMPLETADA CON {errors} ERRORES")
        print("\nNota: Asegúrate de que:")
        print("  - Los proveedores estén importados con sus códigos correctos")
        print("  - Los materiales estén importados con sus números correctos")


async def show_sample_data():
    """Muestra una muestra de los países de origen importados."""
    print("\n" + "="*50)
    print("MUESTRA DE PAÍSES DE ORIGEN IMPORTADOS")
    print("="*50)
    
    async with AsyncSessionLocal() as session:
        paises = await crud.list_paises_origen_material(session, limit=10)
        
        if not paises:
            print("No hay países de origen en la base de datos.")
        else:
            print(f"\nMostrando {len(paises)} registros:\n")
            for pais in paises:
                print(f"  ID: {pais.id}")
                print(f"  Código Proveedor: {pais.codigo_proveedor}")
                print(f"  Número Material: {pais.numero_material}")
                print(f"  País Origen: {pais.pais_origen}")
                print(f"  Creado: {pais.created_at}")
                print(f"  Actualizado: {pais.updated_at}")
                print("-" * 50)


async def main():
    """Función principal."""
    if len(sys.argv) < 2:
        print("Uso: python import_paises_origen_excel.py <ruta_archivo_excel>")
        print('Ejemplo: python import_paises_origen_excel.py "paises_origen.xlsx"')
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    print("\n" + "="*50)
    print("IMPORTACIÓN DE PAÍSES DE ORIGEN DE MATERIALES DESDE EXCEL")
    print("="*50)
    
    await import_paises_origen(file_path)
    await show_sample_data()


if __name__ == "__main__":
    asyncio.run(main())

