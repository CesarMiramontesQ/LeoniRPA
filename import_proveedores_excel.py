"""
Script para importar datos de proveedores desde un archivo Excel a la tabla proveedores.

Uso:
    python import_proveedores_excel.py <ruta_archivo_excel>
    
Ejemplo:
    python import_proveedores_excel.py proveedores.xlsx

Columnas esperadas del Excel:
    - Nombre (requerido)
    - País (opcional)
    - Domicilio (opcional)
    - Estatus (opcional, por defecto True/Activo)
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


def clean_boolean(value, default=True):
    """Convierte un valor a booleano, manejando valores nulos."""
    if pd.isna(value) or value is None or value == '':
        return default
    
    # Convertir a string y normalizar
    str_value = str(value).strip().lower()
    
    # Valores que representan True
    if str_value in ['true', '1', 'yes', 'sí', 'si', 'activo', 'active', 'verdadero']:
        return True
    
    # Valores que representan False
    if str_value in ['false', '0', 'no', 'inactivo', 'inactive', 'falso']:
        return False
    
    # Por defecto, si no se puede determinar, usar el default
    return default


async def import_proveedores(file_path: str):
    """Importa datos del Excel a la tabla de proveedores."""
    
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
    nombre_col = None
    pais_col = None
    domicilio_col = None
    estatus_col = None
    
    # Buscar columna de Nombre - buscar específicamente "Nombre o Razón social"
    for col in df.columns:
        if col.strip() == "Nombre o Razón social":
            nombre_col = col
            break
    
    # Si no se encuentra, buscar variaciones
    if nombre_col is None:
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['nombre o razón social', 'nombre o razon social', 'nombre', 'name', 'proveedor', 'supplier']:
                nombre_col = col
                break
    
    # Buscar columna de País
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ['país', 'pais', 'country', 'pais_origin']:
            pais_col = col
            break
    
    # Buscar columna de Domicilio
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ['domicilio', 'dirección', 'direccion', 'address', 'domicilio_completo']:
            domicilio_col = col
            break
    
    # Buscar columna de Estatus
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ['estatus', 'status', 'estado', 'activo', 'active']:
            estatus_col = col
            break
    
    # Buscar columna de Código Proveedor
    codigo_proveedor_col = None
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ['código proveedor', 'codigo proveedor', 'codigo_proveedor', 'código_proveedor', 
                         'codigo cliente', 'código cliente', 'codigo_cliente', 'código_cliente']:
            codigo_proveedor_col = col
            break
    
    # Verificar que existe la columna Nombre
    if nombre_col is None:
        print(f"✗ Error: No se encontró la columna 'Nombre o Razón social' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    print(f"\nColumnas mapeadas:")
    print(f"  - Nombre: {nombre_col}")
    print(f"  - Código Proveedor: {codigo_proveedor_col if codigo_proveedor_col else 'No encontrada (se generará automáticamente)'}")
    print(f"  - País: {pais_col if pais_col else 'No encontrada'}")
    print(f"  - Domicilio: {domicilio_col if domicilio_col else 'No encontrada'}")
    print(f"  - Estatus: {estatus_col if estatus_col else 'No encontrada (usará True por defecto)'}")
    
    # Importar proveedores
    inserted = 0
    updated = 0
    errors = 0
    skipped = 0
    
    async with AsyncSessionLocal() as session:
        for index, row in df.iterrows():
            try:
                # Obtener valores
                nombre = clean_string(row.get(nombre_col))
                
                # Si no hay nombre, saltar esta fila
                if not nombre:
                    skipped += 1
                    print(f"  Fila {index + 2}: Saltada (sin nombre)")
                    continue
                
                pais = clean_string(row.get(pais_col)) if pais_col else None
                domicilio = clean_string(row.get(domicilio_col)) if domicilio_col else None
                estatus = clean_boolean(row.get(estatus_col), default=True) if estatus_col else True
                
                # Obtener código proveedor del Excel o generar uno automáticamente
                codigo_proveedor = clean_string(row.get(codigo_proveedor_col)) if codigo_proveedor_col else None
                
                # Si no hay código proveedor, generar uno basado en el nombre
                if not codigo_proveedor:
                    # Generar código basado en el nombre (primeras letras + número de fila)
                    codigo_proveedor = f"PROV_{index + 1:04d}"
                    print(f"  Fila {index + 2}: Generado código automático: {codigo_proveedor}")
                
                # Verificar si el código proveedor ya existe
                proveedor_existente = await crud.get_proveedor_by_codigo_proveedor(session, codigo_proveedor)
                
                if proveedor_existente:
                    # Actualizar proveedor existente
                    await crud.update_proveedor(
                        session,
                        codigo_proveedor,
                        nombre=nombre,
                        pais=pais,
                        domicilio=domicilio,
                        estatus=estatus
                    )
                    updated += 1
                    print(f"  Fila {index + 2}: Actualizado - {nombre} ({codigo_proveedor})")
                else:
                    # Crear nuevo proveedor
                    await crud.create_proveedor(
                        session,
                        codigo_proveedor=codigo_proveedor,
                        nombre=nombre,
                        pais=pais,
                        domicilio=domicilio,
                        estatus=estatus
                    )
                    inserted += 1
                    print(f"  Fila {index + 2}: Insertado - {nombre} ({codigo_proveedor})")
                    
            except Exception as e:
                errors += 1
                print(f"  Fila {index + 2}: Error - {str(e)}")
                continue
    
    # Resumen
    print("\n" + "="*50)
    print("RESUMEN DE IMPORTACIÓN")
    print("="*50)
    print(f"Total filas en Excel: {len(df)}")
    print(f"Proveedores insertados: {inserted}")
    print(f"Proveedores actualizados: {updated}")
    print(f"Filas saltadas: {skipped}")
    print(f"Errores: {errors}")
    print("="*50)
    
    if errors == 0:
        print("\n✓ IMPORTACIÓN COMPLETADA EXITOSAMENTE")
    else:
        print(f"\n⚠ IMPORTACIÓN COMPLETADA CON {errors} ERRORES")


async def show_sample_data():
    """Muestra una muestra de los proveedores importados."""
    print("\n" + "="*50)
    print("MUESTRA DE PROVEEDORES IMPORTADOS")
    print("="*50)
    
    async with AsyncSessionLocal() as session:
        proveedores = await crud.list_proveedores(session, limit=10)
        
        if not proveedores:
            print("No hay proveedores en la base de datos.")
        else:
            print(f"\nMostrando {len(proveedores)} proveedores:\n")
            for prov in proveedores:
                estatus_str = "Activo" if prov.estatus else "Inactivo"
                codigo_str = prov.codigo_proveedor if prov.codigo_proveedor else "(sin código)"
                print(f"  ID: {prov.id}")
                print(f"  Nombre: {prov.nombre}")
                print(f"  País: {prov.pais or 'N/A'}")
                print(f"  Domicilio: {prov.domicilio or 'N/A'}")
                print(f"  Estatus: {estatus_str}")
                print(f"  Código Cliente: {codigo_str}")
                print(f"  Creado: {prov.created_at}")
                print("-" * 50)


async def main():
    """Función principal."""
    if len(sys.argv) < 2:
        print("Uso: python import_proveedores_excel.py <ruta_archivo_excel>")
        print("Ejemplo: python import_proveedores_excel.py proveedores.xlsx")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    print("\n" + "="*50)
    print("IMPORTACIÓN DE PROVEEDORES DESDE EXCEL")
    print("="*50)
    
    await import_proveedores(file_path)
    await show_sample_data()


if __name__ == "__main__":
    asyncio.run(main())

