"""
Script para importar datos de precios de materiales desde un archivo Excel a la tabla precios_materiales.

Uso:
    python import_precios_materiales_excel.py <ruta_archivo_excel>
    
Ejemplo:
    python import_precios_materiales_excel.py "Precios Compras.xlsx"

Columnas esperadas del Excel:
    - codigo_cliente (requerido) - FK a proveedores.codigo_cliente
    - numero_material (requerido) - Si el material no existe, se crea automáticamente
    - descripcion_material (opcional) - Descripción del material (se usa al crear materiales nuevos)
    - precio (requerido)
    - currency_uom (opcional) - Moneda y unidad de medida (ej. USD/KG, EUR/KG)
    - country_origin (opcional) - País de origen
    - Porcentaje_Compra (opcional)
    - Comentario (opcional)

Si un precio ya existe (por codigo_cliente + numero_material), se actualiza.
Si un material no existe en la tabla materiales, se crea automáticamente usando
el numero_material y la descripcion_material (si está disponible).
"""
import asyncio
import sys
import os
import pandas as pd
from decimal import Decimal, InvalidOperation
from app.db.base import AsyncSessionLocal
from app.db import crud


def clean_string(value):
    """Limpia un valor string, manejando valores nulos."""
    if pd.isna(value) or value is None:
        return None
    result = str(value).strip()
    return result if result else None


def clean_decimal(value, default=None):
    """Convierte un valor a Decimal, manejando valores nulos y errores."""
    if pd.isna(value) or value == '' or value is None:
        return default
    try:
        # Convertir a string y limpiar
        str_value = str(value).strip()
        # Remover comas y espacios
        str_value = str_value.replace(',', '').replace(' ', '')
        return Decimal(str_value)
    except (InvalidOperation, ValueError):
        return default


async def import_precios_materiales(file_path: str):
    """Importa datos del Excel a la tabla de precios_materiales."""
    
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
    codigo_cliente_col = None
    numero_material_col = None
    descripcion_material_col = None
    precio_col = None
    currency_uom_col = None
    country_origin_col = None
    porcentaje_compra_col = None
    comentario_col = None
    
    # Buscar columna de codigo_cliente
    for col in df.columns:
        col_lower = col.lower().replace('_', ' ').replace('-', ' ')
        # Buscar variaciones de "codigo cliente"
        if any(keyword in col_lower for keyword in ['codigo cliente', 'código cliente', 'codigo_cliente', 
                                                     'código_cliente', 'codigo', 'código', 'cliente']):
            codigo_cliente_col = col
            break
    
    # Buscar columna de numero_material
    for col in df.columns:
        col_lower = col.lower().replace('_', ' ').replace('-', ' ')
        # Buscar variaciones de "numero material" o "numero de material"
        if any(keyword in col_lower for keyword in ['numero de material', 'número de material',
                                                     'numero material', 'número material',
                                                     'numero_material', 'número_material',
                                                     'material number', 'material_no', 'nro material']):
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
            # Buscar variaciones de "descripción material" o "material description"
            if any(keyword in col_lower for keyword in ['descripcion material', 'descripción material',
                                                         'descripcion de material', 'descripción de material',
                                                         'descripcion_material', 'descripción_material',
                                                         'material description', 'material_desc', 'description',
                                                         'material desc', 'desc material']):
                descripcion_material_col = col
                break
    
    # Buscar columna de precio - primero buscar exactamente "Price"
    for col in df.columns:
        if col.strip() == "Price":
            precio_col = col
            break
    
    # Si no se encuentra, buscar variaciones
    if precio_col is None:
        for col in df.columns:
            col_lower = col.lower().replace('_', ' ').replace('-', ' ')
            if any(keyword in col_lower for keyword in ['price', 'precio', 'precio unitario', 'cost', 'costo']):
                precio_col = col
                break
    
    # Buscar columna de currency_uom (Moneda)
    for col in df.columns:
        col_lower = col.lower().replace('_', ' ').replace('-', ' ').replace('/', ' ')
        if any(keyword in col_lower for keyword in ['currency uom', 'currency_uom', 'moneda', 'currency', 'uom']):
            currency_uom_col = col
            break
    
    # Buscar columna de country_origin
    for col in df.columns:
        col_lower = col.lower().replace('_', ' ').replace('-', ' ')
        if any(keyword in col_lower for keyword in ['country origin', 'country_origin', 'país origen', 
                                                     'pais origen', 'origen', 'origin', 'country']):
            country_origin_col = col
            break
    
    # Buscar columna de Porcentaje_Compra
    for col in df.columns:
        col_lower = col.lower().replace('_', ' ').replace('-', ' ')
        if any(keyword in col_lower for keyword in ['porcentaje de compra', 'porcentaje compra', 
                                                     'porcentaje_compra', 'porcentaje', 'percentage']):
            porcentaje_compra_col = col
            break
    
    # Buscar columna de Comentario
    for col in df.columns:
        col_lower = col.lower().replace('_', ' ').replace('-', ' ')
        if any(keyword in col_lower for keyword in ['comentario', 'comment', 'comentarios', 'comments', 
                                                     'notas', 'notes', 'observaciones']):
            comentario_col = col
            break
    
    # Verificar columnas requeridas
    if codigo_cliente_col is None:
        print(f"✗ Error: No se encontró la columna 'codigo_cliente' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    if numero_material_col is None:
        print(f"✗ Error: No se encontró la columna 'numero_material' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    if precio_col is None:
        print(f"✗ Error: No se encontró la columna 'precio' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    print(f"\nColumnas mapeadas:")
    print(f"  - Código Cliente: {codigo_cliente_col}")
    print(f"  - Número Material: {numero_material_col}")
    print(f"  - Descripción Material: {descripcion_material_col if descripcion_material_col else 'No encontrada'}")
    print(f"  - Precio: {precio_col}")
    print(f"  - Currency/UOM: {currency_uom_col if currency_uom_col else 'No encontrada'}")
    print(f"  - País Origen: {country_origin_col if country_origin_col else 'No encontrada'}")
    print(f"  - Porcentaje Compra: {porcentaje_compra_col if porcentaje_compra_col else 'No encontrada'}")
    print(f"  - Comentario: {comentario_col if comentario_col else 'No encontrada'}")
    
    # Importar precios
    inserted = 0
    updated = 0
    errors = 0
    skipped = 0
    not_found_proveedor = 0
    materiales_creados = 0
    materiales_actualizados = 0
    
    async with AsyncSessionLocal() as session:
        for index, row in df.iterrows():
            try:
                # Obtener valores
                codigo_cliente = clean_string(row.get(codigo_cliente_col))
                numero_material = clean_string(row.get(numero_material_col))
                precio_raw = row.get(precio_col)
                precio_val = clean_decimal(precio_raw)
                
                # Debug: mostrar el precio crudo y procesado
                if index < 3:  # Solo para las primeras 3 filas
                    print(f"  DEBUG Fila {index + 2}: precio_raw={precio_raw}, precio_val={precio_val}, tipo_raw={type(precio_raw)}")
                
                # Validar campos requeridos
                if not codigo_cliente:
                    skipped += 1
                    print(f"  Fila {index + 2}: Saltada (sin código cliente)")
                    continue
                
                if not numero_material:
                    skipped += 1
                    print(f"  Fila {index + 2}: Saltada (sin número de material)")
                    continue
                
                if precio_val is None:
                    skipped += 1
                    print(f"  Fila {index + 2}: Saltada (sin precio válido) - valor crudo: {precio_raw}")
                    continue
                
                # Verificar que el proveedor existe
                proveedor = await crud.get_proveedor_by_codigo_proveedor(session, codigo_cliente)
                if not proveedor:
                    not_found_proveedor += 1
                    print(f"  Fila {index + 2}: Error - Proveedor con código '{codigo_cliente}' no encontrado")
                    errors += 1
                    continue
                
                # Obtener descripción del material si está disponible
                descripcion_material = None
                if descripcion_material_col:
                    descripcion_raw = row.get(descripcion_material_col)
                    descripcion_material = clean_string(descripcion_raw)
                    # Debug: mostrar la descripción obtenida para las primeras filas
                    if index < 3:
                        print(f"  DEBUG Fila {index + 2}: descripcion_raw={descripcion_raw}, descripcion_material={descripcion_material}, columna={descripcion_material_col}")
                
                # Verificar que el material existe, si no existe, crearlo
                material = await crud.get_material_by_numero(session, numero_material)
                if not material:
                    # Crear el material automáticamente
                    try:
                        material = await crud.create_material(
                            session,
                            numero_material=numero_material,
                            descripcion_material=descripcion_material
                        )
                        materiales_creados += 1
                        desc_str = f" (descripción: {descripcion_material})" if descripcion_material else " (sin descripción)"
                        print(f"  Fila {index + 2}: Material creado - {numero_material}{desc_str}")
                    except Exception as e:
                        print(f"  Fila {index + 2}: Error al crear material '{numero_material}' - {str(e)}")
                        errors += 1
                        continue
                else:
                    # Material existe, verificar si necesita actualizar la descripción
                    # Si el material no tiene descripción y el Excel sí tiene una, actualizarlo
                    if descripcion_material and (not material.descripcion_material or not material.descripcion_material.strip()):
                        try:
                            await crud.update_material(
                                session,
                                material.id,
                                descripcion_material=descripcion_material
                            )
                            materiales_actualizados += 1
                            print(f"  Fila {index + 2}: Descripción actualizada - {numero_material} (descripción: {descripcion_material})")
                        except Exception as e:
                            print(f"  Fila {index + 2}: Error al actualizar descripción de material '{numero_material}' - {str(e)}")
                            # No incrementar errors aquí, solo registrar el warning
                
                # Obtener valores opcionales
                currency_uom = clean_string(row.get(currency_uom_col)) if currency_uom_col else None
                country_origin = clean_string(row.get(country_origin_col)) if country_origin_col else None
                porcentaje_compra = clean_decimal(row.get(porcentaje_compra_col)) if porcentaje_compra_col else None
                comentario = clean_string(row.get(comentario_col)) if comentario_col else None
                
                # Verificar si el precio ya existe
                existing_precio = await crud.get_precio_material_by_proveedor_material(
                    session,
                    codigo_cliente,  # Este es el valor del Excel, se pasa como codigo_proveedor
                    numero_material
                )
                
                if existing_precio:
                    # Actualizar precio existente
                    await crud.update_precio_material(
                        session,
                        existing_precio.id,
                        precio=precio_val,
                        currency_uom=currency_uom,
                        country_origin=country_origin,
                        Porcentaje_Compra=porcentaje_compra,
                        Comentario=comentario
                    )
                    updated += 1
                    print(f"  Fila {index + 2}: Actualizado - {codigo_cliente} / {numero_material} = {precio_val}")
                else:
                    # Crear nuevo precio
                    await crud.create_precio_material(
                        session,
                        codigo_proveedor=codigo_cliente,  # El valor del Excel se pasa como codigo_proveedor
                        numero_material=numero_material,
                        precio=precio_val,
                        currency_uom=currency_uom,
                        country_origin=country_origin,
                        Porcentaje_Compra=porcentaje_compra,
                        Comentario=comentario
                    )
                    inserted += 1
                    print(f"  Fila {index + 2}: Insertado - {codigo_cliente} / {numero_material} = {precio_val}")
                    
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
    print(f"Precios insertados: {inserted}")
    print(f"Precios actualizados: {updated}")
    print(f"Materiales creados: {materiales_creados}")
    print(f"Materiales con descripción actualizada: {materiales_actualizados}")
    print(f"Filas saltadas (datos faltantes): {skipped}")
    print(f"Errores (proveedor no encontrado): {not_found_proveedor}")
    print(f"Errores totales: {errors}")
    print("="*50)
    
    if errors == 0:
        print("\n✓ IMPORTACIÓN COMPLETADA EXITOSAMENTE")
    else:
        print(f"\n⚠ IMPORTACIÓN COMPLETADA CON {errors} ERRORES")
        print("\nNota: Asegúrate de que:")
        print("  - Los proveedores estén importados con sus códigos de cliente")
        print("  - Los materiales se crean automáticamente si no existen")


async def show_sample_data():
    """Muestra una muestra de los precios importados."""
    print("\n" + "="*50)
    print("MUESTRA DE PRECIOS IMPORTADOS")
    print("="*50)
    
    async with AsyncSessionLocal() as session:
        precios = await crud.list_precios_materiales(session, limit=10)
        
        if not precios:
            print("No hay precios en la base de datos.")
        else:
            print(f"\nMostrando {len(precios)} precios:\n")
            for precio in precios:
                currency_str = precio.currency_uom if precio.currency_uom else "N/A"
                country_str = precio.country_origin if precio.country_origin else "N/A"
                porcentaje_str = str(precio.Porcentaje_Compra) if precio.Porcentaje_Compra else "N/A"
                comentario_str = precio.Comentario[:50] + "..." if precio.Comentario and len(precio.Comentario) > 50 else (precio.Comentario or "N/A")
                
                print(f"  ID: {precio.id}")
                print(f"  Código Proveedor: {precio.codigo_proveedor}")
                print(f"  Número Material: {precio.numero_material}")
                print(f"  Precio: {precio.precio}")
                print(f"  Currency/UOM: {currency_str}")
                print(f"  País Origen: {country_str}")
                print(f"  Porcentaje Compra: {porcentaje_str}")
                print(f"  Comentario: {comentario_str}")
                print(f"  Actualizado: {precio.updated_at}")
                print("-" * 50)


async def main():
    """Función principal."""
    if len(sys.argv) < 2:
        print("Uso: python import_precios_materiales_excel.py <ruta_archivo_excel>")
        print('Ejemplo: python import_precios_materiales_excel.py "Precios Compras.xlsx"')
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    print("\n" + "="*50)
    print("IMPORTACIÓN DE PRECIOS DE MATERIALES DESDE EXCEL")
    print("="*50)
    
    await import_precios_materiales(file_path)
    await show_sample_data()


if __name__ == "__main__":
    asyncio.run(main())

