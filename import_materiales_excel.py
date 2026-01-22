"""
Script para importar datos de materiales desde un archivo Excel a la tabla materiales.

Uso:
    python import_materiales_excel.py <ruta_archivo_excel>
    
Ejemplo:
    python import_materiales_excel.py Materiales.xlsx

Columnas esperadas del Excel:
    - numero_material (requerido)
    - descripcion_material (opcional)
    
Si un material ya existe (por numero_material), se omite.
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


async def import_materiales(file_path: str):
    """Importa datos del Excel a la tabla de materiales."""
    
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
    
    # Normalizar nombres de columnas (case-insensitive, sin espacios extra)
    df.columns = [str(col).strip() for col in df.columns]
    
    # Buscar columnas por diferentes nombres posibles
    numero_material_col = None
    descripcion_material_col = None
    
    # Buscar columna de numero_material
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ['numero_material', 'numero material', 'numero', 'material', 'codigo', 'código', 
                         'material number', 'material_no', 'nro_material']:
            numero_material_col = col
            break
    
    # Buscar columna de descripcion_material
    # Primero buscar específicamente "Material Description" (como está en el Excel)
    for col in df.columns:
        if col.strip() == "Material Description":
            descripcion_material_col = col
            break
    
    # Si no se encuentra, buscar variaciones
    if descripcion_material_col is None:
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['descripcion_material', 'descripción_material', 'descripcion material', 
                             'descripción material', 'material description', 'descripcion', 'descripción', 
                             'description', 'desc_material', 'material_desc']:
                descripcion_material_col = col
                break
    
    # Verificar que existe la columna numero_material
    if numero_material_col is None:
        print(f"✗ Error: No se encontró la columna 'numero_material' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    print(f"\nColumnas mapeadas:")
    print(f"  - Número Material: {numero_material_col}")
    print(f"  - Descripción Material: {descripcion_material_col if descripcion_material_col else 'No encontrada'}")
    
    # Importar materiales
    inserted = 0
    errors = 0
    skipped = 0
    duplicates = 0
    
    async with AsyncSessionLocal() as session:
        for index, row in df.iterrows():
            try:
                # Obtener valores
                numero_material = clean_string(row.get(numero_material_col))
                
                # Si no hay numero_material, saltar esta fila
                if not numero_material:
                    skipped += 1
                    print(f"  Fila {index + 2}: Saltada (sin número de material)")
                    continue
                
                descripcion_material = clean_string(row.get(descripcion_material_col)) if descripcion_material_col else None
                
                # Verificar si el material ya existe (por numero_material)
                existing_material = await crud.get_material_by_numero(session, numero_material)
                
                if existing_material:
                    # Si el material existe pero no tiene descripción y ahora tenemos una, actualizarla
                    if descripcion_material and not existing_material.descripcion_material:
                        await crud.update_material(
                            session,
                            existing_material.id,
                            descripcion_material=descripcion_material
                        )
                        duplicates += 1
                        print(f"  Fila {index + 2}: Actualizado (descripción agregada) - {numero_material}")
                    else:
                        # Material ya existe, omitirlo
                        duplicates += 1
                        print(f"  Fila {index + 2}: Omitido (duplicado) - {numero_material}")
                else:
                    # Crear nuevo material
                    await crud.create_material(
                        session,
                        numero_material=numero_material,
                        descripcion_material=descripcion_material
                    )
                    inserted += 1
                    print(f"  Fila {index + 2}: Insertado - {numero_material}")
                    
            except Exception as e:
                errors += 1
                print(f"  Fila {index + 2}: Error - {str(e)}")
                continue
    
    # Resumen
    print("\n" + "="*50)
    print("RESUMEN DE IMPORTACIÓN")
    print("="*50)
    print(f"Total filas en Excel: {len(df)}")
    print(f"Materiales insertados: {inserted}")
    print(f"Materiales omitidos (duplicados): {duplicates}")
    print(f"Filas saltadas (sin número): {skipped}")
    print(f"Errores: {errors}")
    print("="*50)
    
    if errors == 0:
        print("\n✓ IMPORTACIÓN COMPLETADA EXITOSAMENTE")
    else:
        print(f"\n⚠ IMPORTACIÓN COMPLETADA CON {errors} ERRORES")


async def show_sample_data():
    """Muestra una muestra de los materiales importados."""
    print("\n" + "="*50)
    print("MUESTRA DE MATERIALES IMPORTADOS")
    print("="*50)
    
    async with AsyncSessionLocal() as session:
        materiales = await crud.list_materiales(session, limit=10)
        
        if not materiales:
            print("No hay materiales en la base de datos.")
        else:
            print(f"\nMostrando {len(materiales)} materiales:\n")
            for mat in materiales:
                desc_str = mat.descripcion_material if mat.descripcion_material else "(sin descripción)"
                print(f"  ID: {mat.id}")
                print(f"  Número Material: {mat.numero_material}")
                print(f"  Descripción: {desc_str}")
                print(f"  Creado: {mat.created_at}")
                print("-" * 50)


async def main():
    """Función principal."""
    if len(sys.argv) < 2:
        print("Uso: python import_materiales_excel.py <ruta_archivo_excel>")
        print("Ejemplo: python import_materiales_excel.py Materiales.xlsx")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    print("\n" + "="*50)
    print("IMPORTACIÓN DE MATERIALES DESDE EXCEL")
    print("="*50)
    
    await import_materiales(file_path)
    await show_sample_data()


if __name__ == "__main__":
    asyncio.run(main())

