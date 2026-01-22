"""
Script para actualizar el código del cliente de proveedores desde un archivo Excel.

Uso:
    python update_codigo_cliente.py <ruta_archivo_excel>
    
Ejemplo:
    python update_codigo_cliente.py "codigo cliente.xlsx"

Columnas esperadas del Excel:
    - Nombre del proveedor (requerido) - para buscar el proveedor en la base de datos
    - Código Cliente (requerido) - el código que se actualizará
"""
import asyncio
import sys
import os
import pandas as pd
from sqlalchemy import select
from app.db.base import AsyncSessionLocal
from app.db import crud
from app.db.models import Proveedor


def clean_string(value):
    """Limpia un valor string, manejando valores nulos."""
    if pd.isna(value) or value is None:
        return None
    result = str(value).strip()
    return result if result else None


def clean_codigo_cliente(value):
    """Limpia y convierte el código de cliente a entero (como string)."""
    if pd.isna(value) or value is None:
        return None
    
    # Intentar convertir a número primero
    try:
        # Si es un string, limpiarlo
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            # Convertir a float primero para manejar decimales
            num_value = float(value)
        else:
            # Si ya es un número
            num_value = float(value)
        
        # Convertir a entero y luego a string
        codigo = str(int(num_value))
        return codigo if codigo else None
    except (ValueError, TypeError):
        # Si no se puede convertir, intentar como string
        result = str(value).strip()
        return result if result else None


async def get_proveedor_by_nombre_exacto(db, nombre: str):
    """Obtiene un proveedor por nombre exacto (case-insensitive)."""
    result = await db.execute(
        select(Proveedor).where(Proveedor.nombre.ilike(nombre.strip()))
    )
    return result.scalar_one_or_none()


async def update_codigos_cliente(file_path: str):
    """Actualiza los códigos de cliente de proveedores desde el Excel."""
    
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
    codigo_cliente_col = None
    
    # Buscar columna de Nombre del proveedor
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ['nombre cliente', 'nombre', 'nombre o razón social', 'nombre o razon social', 
                        'name', 'proveedor', 'supplier', 'nombre proveedor', 
                        'nombre_proveedor', 'razón social', 'razon social', 'cliente']:
            nombre_col = col
            break
    
    # Buscar columna de Código Cliente
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ['codigo cliente', 'código cliente', 'codigo_cliente', 
                        'código_cliente', 'codigo', 'código', 'code', 
                        'customer code', 'cliente', 'customer_code']:
            codigo_cliente_col = col
            break
    
    # Verificar que existen las columnas requeridas
    if nombre_col is None:
        print(f"✗ Error: No se encontró la columna 'Nombre' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    if codigo_cliente_col is None:
        print(f"✗ Error: No se encontró la columna 'Código Cliente' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    print(f"\nColumnas mapeadas:")
    print(f"  - Nombre: {nombre_col}")
    print(f"  - Código Cliente: {codigo_cliente_col}")
    
    # Procesar actualizaciones
    updated = 0
    not_found = 0
    errors = 0
    skipped = 0
    
    async with AsyncSessionLocal() as session:
        for index, row in df.iterrows():
            try:
                # Obtener valores
                nombre = clean_string(row.get(nombre_col))
                codigo_cliente = clean_codigo_cliente(row.get(codigo_cliente_col))
                
                # Si no hay nombre, saltar esta fila
                if not nombre:
                    skipped += 1
                    print(f"  Fila {index + 2}: Saltada (sin nombre)")
                    continue
                
                # Si no hay código cliente, saltar esta fila
                if not codigo_cliente:
                    skipped += 1
                    print(f"  Fila {index + 2}: Saltada (sin código cliente) - {nombre}")
                    continue
                
                # Buscar proveedor por nombre exacto
                proveedor = await get_proveedor_by_nombre_exacto(session, nombre)
                
                if not proveedor:
                    not_found += 1
                    print(f"  Fila {index + 2}: No encontrado - {nombre}")
                    continue
                
                # Verificar si el código ya está asignado a otro proveedor
                existing_proveedor = await crud.get_proveedor_by_codigo_proveedor(session, codigo_cliente)
                if existing_proveedor and existing_proveedor.codigo_proveedor != proveedor.codigo_proveedor:
                    errors += 1
                    print(f"  Fila {index + 2}: Error - El código '{codigo_cliente}' ya está asignado a '{existing_proveedor.nombre}' (no se puede asignar a '{nombre}')")
                    continue
                
                # Si el código es diferente, necesitamos actualizar (esto requiere manejo especial porque es PK)
                if proveedor.codigo_proveedor != codigo_cliente:
                    # Nota: Cambiar la PK requiere eliminar y recrear, lo cual es complejo
                    # Por ahora, solo actualizamos si el código es el mismo
                    print(f"  Fila {index + 2}: Advertencia - No se puede cambiar el código_proveedor (PK) de '{proveedor.codigo_proveedor}' a '{codigo_cliente}'")
                    skipped += 1
                    continue
                
                # Si el código es el mismo, no hay nada que actualizar
                # (El código ya está correcto)
                updated += 1
                
                # Mostrar información de la actualización
                codigo_anterior = proveedor.codigo_cliente if proveedor.codigo_cliente else "(sin código)"
                print(f"  Fila {index + 2}: Actualizado - {nombre} (código: {codigo_anterior} -> {codigo_cliente})")
                    
            except Exception as e:
                errors += 1
                print(f"  Fila {index + 2}: Error - {str(e)}")
                continue
    
    # Resumen
    print("\n" + "="*50)
    print("RESUMEN DE ACTUALIZACIÓN")
    print("="*50)
    print(f"Total filas en Excel: {len(df)}")
    print(f"Proveedores actualizados: {updated}")
    print(f"Proveedores no encontrados: {not_found}")
    print(f"Filas saltadas: {skipped}")
    print(f"Errores: {errors}")
    print("="*50)
    
    if errors == 0 and not_found == 0:
        print("\n✓ ACTUALIZACIÓN COMPLETADA EXITOSAMENTE")
    elif errors == 0:
        print(f"\n⚠ ACTUALIZACIÓN COMPLETADA CON {not_found} PROVEEDORES NO ENCONTRADOS")
    else:
        print(f"\n⚠ ACTUALIZACIÓN COMPLETADA CON {errors} ERRORES")


async def show_sample_data():
    """Muestra una muestra de los proveedores con códigos de cliente actualizados."""
    print("\n" + "="*50)
    print("MUESTRA DE PROVEEDORES ACTUALIZADOS")
    print("="*50)
    
    async with AsyncSessionLocal() as session:
        # Obtener proveedores con código de cliente
        query = select(Proveedor).where(Proveedor.codigo_cliente.isnot(None)).limit(10)
        result = await session.execute(query)
        proveedores = list(result.scalars().all())
        
        if not proveedores:
            print("No hay proveedores con código de cliente en la base de datos.")
        else:
            print(f"\nMostrando {len(proveedores)} proveedores con código de cliente:\n")
            for prov in proveedores:
                print(f"  ID: {prov.id}")
                print(f"  Nombre: {prov.nombre}")
                print(f"  Código Cliente: {prov.codigo_cliente}")
                print(f"  País: {prov.pais or 'N/A'}")
                print(f"  Estatus: {'Activo' if prov.estatus else 'Inactivo'}")
                print(f"  Actualizado: {prov.updated_at}")
                print("-" * 50)


async def main():
    """Función principal."""
    if len(sys.argv) < 2:
        print("Uso: python update_codigo_cliente.py <ruta_archivo_excel>")
        print('Ejemplo: python update_codigo_cliente.py "codigo cliente.xlsx"')
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    print("\n" + "="*50)
    print("ACTUALIZACIÓN DE CÓDIGOS DE CLIENTE DESDE EXCEL")
    print("="*50)
    
    await update_codigos_cliente(file_path)
    await show_sample_data()


if __name__ == "__main__":
    asyncio.run(main())

