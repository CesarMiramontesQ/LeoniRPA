"""
Script para verificar qué materiales del archivo Excel no existen en la base de datos.

Uso:
    python verificar_materiales_faltantes.py <ruta_archivo_excel>
    
Ejemplo:
    python verificar_materiales_faltantes.py "Precios Compras.xlsx"
"""
import asyncio
import sys
import os
import pandas as pd
from app.db.base import AsyncSessionLocal
from app.db import crud
from sqlalchemy import select
from app.db.models import Material


def clean_string(value):
    """Limpia un valor string, manejando valores nulos."""
    if pd.isna(value) or value is None:
        return None
    result = str(value).strip()
    return result if result else None


async def verificar_materiales_faltantes(file_path: str):
    """Verifica qué materiales del Excel no existen en la base de datos."""
    
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
    
    # Buscar columna de numero_material
    numero_material_col = None
    for col in df.columns:
        col_lower = col.lower().replace('_', ' ').replace('-', ' ')
        if any(keyword in col_lower for keyword in ['numero de material', 'número de material',
                                                     'numero material', 'número material',
                                                     'numero_material', 'número_material',
                                                     'material number', 'material_no', 'nro material']):
            numero_material_col = col
            break
    
    if numero_material_col is None:
        print(f"✗ Error: No se encontró la columna 'numero_material' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    print(f"\nColumna de material encontrada: {numero_material_col}")
    
    # Obtener todos los números de materiales únicos del Excel
    materiales_excel = set()
    for index, row in df.iterrows():
        numero_material = clean_string(row.get(numero_material_col))
        if numero_material:
            materiales_excel.add(numero_material)
    
    print(f"\nMateriales únicos en el Excel: {len(materiales_excel)}")
    
    # Obtener todos los materiales de la base de datos
    print("\nConsultando materiales en la base de datos...")
    async with AsyncSessionLocal() as session:
        # Obtener todos los materiales (sin límite)
        query = select(Material.numero_material)
        result = await session.execute(query)
        materiales_db = {row[0] for row in result.all()}
    
    print(f"Materiales en la base de datos: {len(materiales_db)}")
    
    # Encontrar materiales faltantes
    materiales_faltantes = materiales_excel - materiales_db
    
    # Mostrar resultados
    print("\n" + "="*70)
    print("RESUMEN DE VERIFICACIÓN")
    print("="*70)
    print(f"Total materiales en Excel: {len(materiales_excel)}")
    print(f"Materiales encontrados en BD: {len(materiales_excel - materiales_faltantes)}")
    print(f"Materiales NO encontrados en BD: {len(materiales_faltantes)}")
    print("="*70)
    
    if materiales_faltantes:
        print("\n" + "="*70)
        print("MATERIALES QUE NO EXISTEN EN LA BASE DE DATOS")
        print("="*70)
        print(f"\nTotal: {len(materiales_faltantes)} materiales\n")
        
        # Ordenar alfabéticamente
        materiales_ordenados = sorted(materiales_faltantes)
        
        # Mostrar en columnas para mejor legibilidad
        for i, material in enumerate(materiales_ordenados, 1):
            print(f"{i:3d}. {material}")
        
        print("\n" + "="*70)
        
        # También mostrar con información del proveedor si está disponible
        codigo_cliente_col = None
        for col in df.columns:
            col_lower = col.lower().replace('_', ' ').replace('-', ' ')
            if any(keyword in col_lower for keyword in ['codigo cliente', 'código cliente', 'codigo_cliente', 
                                                         'código_cliente', 'codigo', 'código', 'cliente']):
                codigo_cliente_col = col
                break
        
        if codigo_cliente_col:
            print("\n" + "="*70)
            print("MATERIALES FALTANTES CON SU PROVEEDOR")
            print("="*70)
            print(f"{'Material':<40} {'Proveedor':<20}")
            print("-"*70)
            
            materiales_con_proveedor = {}
            for index, row in df.iterrows():
                numero_material = clean_string(row.get(numero_material_col))
                codigo_cliente = clean_string(row.get(codigo_cliente_col)) if codigo_cliente_col else None
                
                if numero_material in materiales_faltantes:
                    if numero_material not in materiales_con_proveedor:
                        materiales_con_proveedor[numero_material] = set()
                    if codigo_cliente:
                        materiales_con_proveedor[numero_material].add(codigo_cliente)
            
            for material in sorted(materiales_con_proveedor.keys()):
                proveedores = ', '.join(sorted(materiales_con_proveedor[material])) if materiales_con_proveedor[material] else 'N/A'
                print(f"{material:<40} {proveedores:<20}")
            
            print("="*70)
    else:
        print("\n✓ Todos los materiales del Excel existen en la base de datos")
    
    return materiales_faltantes


async def main():
    """Función principal."""
    if len(sys.argv) < 2:
        print("Uso: python verificar_materiales_faltantes.py <ruta_archivo_excel>")
        print('Ejemplo: python verificar_materiales_faltantes.py "Precios Compras.xlsx"')
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    print("\n" + "="*70)
    print("VERIFICACIÓN DE MATERIALES FALTANTES")
    print("="*70)
    
    await verificar_materiales_faltantes(file_path)


if __name__ == "__main__":
    asyncio.run(main())

