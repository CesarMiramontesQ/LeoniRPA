"""
Script para actualizar las descripciones de materiales desde un archivo Excel.

Este script actualiza SOLO las descripciones de materiales existentes.
Si un material no existe, se reporta pero no se crea.

Uso:
    python actualizar_descripciones_materiales.py <ruta_archivo_excel>
    
Ejemplo:
    python actualizar_descripciones_materiales.py "actualizar materiales.xlsx"

Columnas esperadas del Excel:
    - Material Number (requerido) - Número del material
    - Material Description (requerido) - Nueva descripción del material
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


async def actualizar_descripciones_materiales(file_path: str):
    """Actualiza las descripciones de materiales desde el Excel."""
    
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
    
    # Normalizar nombres de columnas
    df.columns = [str(col).strip() for col in df.columns]
    
    # Buscar columnas
    numero_material_col = None
    descripcion_material_col = None
    
    # Buscar columna de numero_material - primero buscar exactamente "Material Number"
    for col in df.columns:
        col_stripped = col.strip()
        if col_stripped == "Material Number" or col_stripped.lower() == "material number":
            numero_material_col = col
            break
    
    # Si no se encuentra, buscar variaciones
    if numero_material_col is None:
        for col in df.columns:
            col_lower = col.lower().strip().replace('_', ' ').replace('-', ' ')
            if any(keyword in col_lower for keyword in ['numero material', 'número material',
                                                         'numero de material', 'número de material',
                                                         'numero_material', 'número_material',
                                                         'material number', 'material_no', 'nro material',
                                                         'materialnumber', 'material_number']):
                numero_material_col = col
                break
    
    # Buscar columna de descripcion_material - primero buscar exactamente "Material Description"
    for col in df.columns:
        col_stripped = col.strip()
        if col_stripped == "Material Description" or col_stripped.lower() == "material description":
            descripcion_material_col = col
            break
    
    # Si no se encuentra, buscar variaciones
    if descripcion_material_col is None:
        for col in df.columns:
            col_lower = col.lower().strip().replace('_', ' ').replace('-', ' ')
            if any(keyword in col_lower for keyword in ['descripcion material', 'descripción material',
                                                         'descripcion de material', 'descripción de material',
                                                         'descripcion_material', 'descripción_material',
                                                         'material description', 'material_desc', 'description',
                                                         'material desc', 'desc material']):
                descripcion_material_col = col
                break
    
    # Verificar columnas requeridas
    if numero_material_col is None:
        print(f"✗ Error: No se encontró la columna 'Material Number' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    if descripcion_material_col is None:
        print(f"✗ Error: No se encontró la columna 'Material Description' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    print(f"\nColumnas mapeadas:")
    print(f"  - Número Material: {numero_material_col}")
    print(f"  - Descripción Material: {descripcion_material_col}")
    
    # Actualizar descripciones
    updated = 0
    errors = 0
    skipped = 0
    not_found = 0
    
    async with AsyncSessionLocal() as session:
        for index, row in df.iterrows():
            try:
                # Obtener valores
                numero_material = clean_string(row.get(numero_material_col))
                descripcion_material = clean_string(row.get(descripcion_material_col))
                
                # Validar campos requeridos
                if not numero_material:
                    skipped += 1
                    if index < 10:
                        print(f"  Fila {index + 2}: Saltada (sin número de material)")
                    continue
                
                if not descripcion_material:
                    skipped += 1
                    if index < 10:
                        print(f"  Fila {index + 2}: Saltada (sin descripción) - Material: {numero_material}")
                    continue
                
                # Verificar si el material existe
                material = await crud.get_material_by_numero(session, numero_material)
                
                if not material:
                    not_found += 1
                    if index < 10 or not_found <= 20:
                        print(f"  Fila {index + 2}: Material no encontrado - {numero_material}")
                    continue
                
                # Verificar si la descripción es diferente
                current_desc = material.descripcion_material or ""
                if current_desc == descripcion_material:
                    # No hay cambios
                    if index < 5:
                        print(f"  Fila {index + 2}: Sin cambios - {numero_material}")
                    continue
                
                # Actualizar la descripción
                await crud.update_material(
                    session,
                    material.id,
                    descripcion_material=descripcion_material
                )
                updated += 1
                
                # Mostrar actualización (primeras 10 o primeras 20 actualizadas)
                if index < 10 or updated <= 20:
                    desc_prev = current_desc[:50] + "..." if len(current_desc) > 50 else current_desc or "(sin descripción)"
                    desc_new = descripcion_material[:50] + "..." if len(descripcion_material) > 50 else descripcion_material
                    print(f"  Fila {index + 2}: Actualizado - {numero_material}")
                    print(f"    Anterior: {desc_prev}")
                    print(f"    Nueva: {desc_new}")
                    
            except Exception as e:
                errors += 1
                print(f"  Fila {index + 2}: Error - {str(e)}")
                import traceback
                if errors <= 5:
                    traceback.print_exc()
                continue
    
    # Resumen
    print("\n" + "="*70)
    print("RESUMEN DE ACTUALIZACIÓN")
    print("="*70)
    print(f"Total filas en Excel: {len(df)}")
    print(f"Descripciones actualizadas: {updated}")
    print(f"Materiales no encontrados: {not_found}")
    print(f"Filas saltadas (datos faltantes): {skipped}")
    print(f"Errores: {errors}")
    print("="*70)
    
    if errors == 0 and not_found == 0:
        print("\n✓ ACTUALIZACIÓN COMPLETADA EXITOSAMENTE")
    elif errors == 0:
        print(f"\n✓ ACTUALIZACIÓN COMPLETADA (con {not_found} materiales no encontrados)")
    else:
        print(f"\n⚠ ACTUALIZACIÓN COMPLETADA CON {errors} ERRORES")
    
    if not_found > 0:
        print(f"\nNota: {not_found} materiales del Excel no existen en la base de datos.")
        print("      Estos materiales no fueron actualizados.")


async def show_sample_data():
    """Muestra una muestra de los materiales actualizados."""
    print("\n" + "="*70)
    print("MUESTRA DE MATERIALES ACTUALIZADOS (últimos 10)")
    print("="*70)
    
    async with AsyncSessionLocal() as session:
        materiales = await crud.list_materiales(session, limit=10)
        
        if not materiales:
            print("No hay materiales en la base de datos.")
        else:
            print(f"\nMostrando {len(materiales)} materiales:\n")
            for mat in materiales:
                desc_str = mat.descripcion_material if mat.descripcion_material else "(sin descripción)"
                desc_display = desc_str[:80] + "..." if len(desc_str) > 80 else desc_str
                print(f"  Número Material: {mat.numero_material}")
                print(f"  Descripción: {desc_display}")
                print(f"  Actualizado: {mat.updated_at}")
                print("-" * 70)


async def main():
    """Función principal."""
    if len(sys.argv) < 2:
        print("Uso: python actualizar_descripciones_materiales.py <ruta_archivo_excel>")
        print('Ejemplo: python actualizar_descripciones_materiales.py "actualizar materiales.xlsx"')
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    print("\n" + "="*70)
    print("ACTUALIZACIÓN DE DESCRIPCIONES DE MATERIALES DESDE EXCEL")
    print("="*70)
    
    await actualizar_descripciones_materiales(file_path)
    await show_sample_data()


if __name__ == "__main__":
    asyncio.run(main())

