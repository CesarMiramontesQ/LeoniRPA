"""
Script para importar datos de proveedores desde el archivo listado.XLS.xlsx.

Uso:
    python import_listado_proveedores.py [ruta_archivo]
    
Ejemplo:
    python import_listado_proveedores.py listado.XLS.xlsx

El script detecta automáticamente las columnas del Excel y las mapea a:
    - codigo_proveedor (PK, requerido - se genera si no existe)
    - nombre (requerido)
    - pais (opcional)
    - domicilio (opcional)
    - poblacion (opcional)
    - cp (opcional)
    - estatus (opcional, por defecto True/Activo)
    - estatus_compras (opcional)
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
    if str_value in ['true', '1', 'yes', 'sí', 'si', 'activo', 'active', 'verdadero', 'v']:
        return True
    
    # Valores que representan False
    if str_value in ['false', '0', 'no', 'inactivo', 'inactive', 'falso', 'f']:
        return False
    
    # Por defecto, si no se puede determinar, usar el default
    return default


async def import_listado_proveedores(file_path: str = "listado.XLS.xlsx"):
    """Importa datos del Excel listado.XLS.xlsx a la tabla de proveedores."""
    
    # Verificar que el archivo existe
    if not os.path.exists(file_path):
        print(f"✗ Error: El archivo '{file_path}' no existe")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print("IMPORTACIÓN DE PROVEEDORES DESDE LISTADO.XLS.XLSX")
    print(f"{'='*60}\n")
    print(f"Leyendo archivo: {file_path}")
    
    # Leer el archivo Excel
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
    except Exception as e:
        print(f"✗ Error al leer el archivo Excel: {e}")
        sys.exit(1)
    
    print(f"✓ Filas encontradas: {len(df)}")
    print(f"✓ Columnas encontradas: {len(df.columns)}")
    print(f"\nColumnas del archivo:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    # Normalizar nombres de columnas (case-insensitive, sin espacios extra)
    df.columns = [str(col).strip() for col in df.columns]
    
    # Buscar columnas por diferentes nombres posibles
    nombre_col = None
    codigo_proveedor_col = None
    pais_col = None
    domicilio_col = None
    poblacion_col = None
    cp_col = None
    estatus_col = None
    estatus_compras_col = None
    
    # Buscar columna de Código Proveedor (PRIMERO, es la más importante)
    for col in df.columns:
        col_clean = str(col).strip()
        col_lower = col_clean.lower()
        if col_lower in ['codigo proveedor', 'código proveedor', 'codigo_proveedor', 'código_proveedor',
                         'codigo cliente', 'código cliente', 'codigo_cliente', 'código_cliente']:
            codigo_proveedor_col = col
            break
    
    # Buscar columna de Nombre proveedor
    for col in df.columns:
        col_clean = str(col).strip()
        col_lower = col_clean.lower()
        if col_lower in ['nombre proveedor', 'nombre_proveedor', 'nombre', 'name', 'proveedor', 'supplier',
                         'razón social', 'razon social', 'razon_social', 'razón_social']:
            nombre_col = col
            break
    
    # Buscar columna de Domicilio
    for col in df.columns:
        col_clean = str(col).strip()
        col_lower = col_clean.lower()
        if col_lower in ['domicilio', 'dirección', 'direccion', 'address', 'domicilio_completo']:
            domicilio_col = col
            break
    
    # Buscar columna de CP
    for col in df.columns:
        col_clean = str(col).strip()
        col_lower = col_clean.lower()
        if col_lower in ['cp', 'código postal', 'codigo postal', 'postal', 'zip',
                         'código_postal', 'codigo_postal']:
            cp_col = col
            break
    
    # Buscar columna de Población
    for col in df.columns:
        col_clean = str(col).strip()
        col_lower = col_clean.lower()
        if col_lower in ['población', 'poblacion', 'ciudad', 'city', 'municipio', 'localidad']:
            poblacion_col = col
            break
    
    # Buscar columna de País
    for col in df.columns:
        col_clean = str(col).strip()
        col_lower = col_clean.lower()
        if col_lower in ['pais', 'país', 'country', 'pais_origin', 'país_origin']:
            pais_col = col
            break
    
    # Buscar columna de Estatus Compras
    for col in df.columns:
        col_clean = str(col).strip()
        col_lower = col_clean.lower()
        if col_lower in ['estatus compras', 'estatus_compras', 'status compras', 'status_compras',
                         'estado compras', 'compras']:
            estatus_compras_col = col
            break
    
    # Buscar columna de Estatus (opcional, puede no existir)
    for col in df.columns:
        col_clean = str(col).strip()
        col_lower = col_clean.lower()
        if col_lower in ['estatus', 'status', 'estado', 'activo', 'active']:
            estatus_col = col
            break
    
    # Verificar que existen las columnas esenciales
    if nombre_col is None:
        print(f"\n✗ Error: No se encontró la columna 'Nombre proveedor' en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
        sys.exit(1)
    
    if codigo_proveedor_col is None:
        print(f"\n⚠ Advertencia: No se encontró la columna 'Codigo Proveedor' en el archivo")
        print(f"Se generarán códigos automáticamente")
        print(f"Columnas disponibles: {list(df.columns)}")
    
    # Mostrar mapeo de columnas
    print(f"\n{'='*60}")
    print("MAPEO DE COLUMNAS")
    print(f"{'='*60}")
    print(f"  {'✓' if codigo_proveedor_col else '✗'} Codigo Proveedor: {codigo_proveedor_col if codigo_proveedor_col else 'No encontrada (se generará automáticamente)'}")
    print(f"  ✓ Nombre proveedor: {nombre_col}")
    print(f"  {'✓' if domicilio_col else '✗'} Domicilio: {domicilio_col if domicilio_col else 'No encontrada'}")
    print(f"  {'✓' if cp_col else '✗'} CP: {cp_col if cp_col else 'No encontrada'}")
    print(f"  {'✓' if poblacion_col else '✗'} Población: {poblacion_col if poblacion_col else 'No encontrada'}")
    print(f"  {'✓' if pais_col else '✗'} Pais: {pais_col if pais_col else 'No encontrada'}")
    print(f"  {'✓' if estatus_compras_col else '✗'} Estatus Compras: {estatus_compras_col if estatus_compras_col else 'No encontrada'}")
    print(f"  {'✓' if estatus_col else '✗'} Estatus: {estatus_col if estatus_col else 'No encontrada (usará True por defecto)'}")
    print(f"{'='*60}\n")
    
    # Importar proveedores
    inserted = 0
    updated = 0
    errors = 0
    skipped = 0
    
    # Procesar cada fila con su propia sesión para evitar problemas de transacción
    for index, row in df.iterrows():
        async with AsyncSessionLocal() as session:
            try:
                # Obtener valores
                nombre = clean_string(row.get(nombre_col))
                
                # Si no hay nombre, saltar esta fila
                if not nombre:
                    skipped += 1
                    if index < 5:  # Solo mostrar para las primeras 5 filas
                        print(f"  Fila {index + 2}: ⚠ Saltada (sin nombre)")
                    continue
                
                # Obtener código proveedor del Excel o generar uno automáticamente
                codigo_proveedor = clean_string(row.get(codigo_proveedor_col)) if codigo_proveedor_col else None
                
                # Si no hay código proveedor, generar uno basado en el nombre y número de fila
                if not codigo_proveedor:
                    # Generar código único basado en el nombre (primeras letras) + número de fila
                    nombre_sin_espacios = ''.join(c for c in nombre.upper() if c.isalnum())[:8]
                    codigo_proveedor = f"{nombre_sin_espacios}_{index + 1:04d}"
                    if index < 5:  # Solo mostrar para las primeras 5 filas
                        print(f"  Fila {index + 2}: ℹ Generado código automático: {codigo_proveedor}")
                
                # Validar que el código proveedor no esté vacío
                if not codigo_proveedor or len(codigo_proveedor.strip()) == 0:
                    skipped += 1
                    if index < 5:
                        print(f"  Fila {index + 2}: ⚠ Saltada (código proveedor inválido)")
                    continue
                
                # Obtener valores opcionales
                pais = clean_string(row.get(pais_col)) if pais_col else None
                domicilio = clean_string(row.get(domicilio_col)) if domicilio_col else None
                poblacion = clean_string(row.get(poblacion_col)) if poblacion_col else None
                cp = clean_string(row.get(cp_col)) if cp_col else None
                estatus = clean_boolean(row.get(estatus_col), default=True) if estatus_col else True
                estatus_compras = clean_string(row.get(estatus_compras_col)) if estatus_compras_col else None
                
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
                        poblacion=poblacion,
                        cp=cp,
                        estatus=estatus,
                        estatus_compras=estatus_compras
                    )
                    updated += 1
                else:
                    # Crear nuevo proveedor
                    await crud.create_proveedor(
                        session,
                        codigo_proveedor=codigo_proveedor,
                        nombre=nombre,
                        pais=pais,
                        domicilio=domicilio,
                        poblacion=poblacion,
                        cp=cp,
                        estatus=estatus,
                        estatus_compras=estatus_compras
                    )
                    inserted += 1
                
                # Mostrar progreso periódico
                if (index + 1) % 100 == 0:
                    print(f"  ✓ Procesadas {index + 2} filas... (Insertados: {inserted}, Actualizados: {updated}, Errores: {errors})")
                
                # Mostrar progreso para las primeras filas
                if index < 5:
                    accion = "Actualizado" if proveedor_existente else "Insertado"
                    print(f"  Fila {index + 2}: ✓ {accion} - {nombre[:50]} ({codigo_proveedor})")
                    
            except Exception as e:
                # Hacer rollback en caso de error
                try:
                    await session.rollback()
                except:
                    pass  # Ignorar errores en rollback
                
                errors += 1
                error_msg = str(e)
                # Truncar mensajes de error muy largos
                if len(error_msg) > 200:
                    error_msg = error_msg[:200] + "..."
                
                print(f"  Fila {index + 2}: ✗ Error - {error_msg}")
                
                # Mostrar traceback solo para los primeros 5 errores
                if errors <= 5:
                    import traceback
                    traceback.print_exc()
                
                # Continuar con la siguiente fila (la sesión se cierra automáticamente)
                continue
    
    # Resumen
    print(f"\n{'='*60}")
    print("RESUMEN DE IMPORTACIÓN")
    print(f"{'='*60}")
    print(f"Total filas en Excel: {len(df)}")
    print(f"Proveedores insertados: {inserted}")
    print(f"Proveedores actualizados: {updated}")
    print(f"Filas saltadas: {skipped}")
    print(f"Errores: {errors}")
    print(f"{'='*60}")
    
    if errors == 0:
        print("\n✓ IMPORTACIÓN COMPLETADA EXITOSAMENTE")
    else:
        print(f"\n⚠ IMPORTACIÓN COMPLETADA CON {errors} ERRORES")
    
    return inserted, updated, errors, skipped


async def show_sample_data():
    """Muestra una muestra de los proveedores importados."""
    async with AsyncSessionLocal() as session:
        proveedores = await crud.list_proveedores(session, limit=10)
        
        if not proveedores:
            print("\nNo hay proveedores en la base de datos.")
        else:
            print(f"\n{'='*60}")
            print(f"MUESTRA DE PROVEEDORES (mostrando {len(proveedores)} de los primeros)")
            print(f"{'='*60}\n")
            for prov in proveedores:
                estatus_str = "Activo" if prov.estatus else "Inactivo"
                print(f"  • {prov.codigo_proveedor} - {prov.nombre}")
                print(f"    País: {prov.pais or 'N/A'}, Población: {prov.poblacion or 'N/A'}, CP: {prov.cp or 'N/A'}")
                print(f"    Estatus: {estatus_str}, Estatus Compras: {prov.estatus_compras or 'N/A'}")
                print()


async def main():
    """Función principal."""
    # Obtener ruta del archivo desde argumentos o usar la predeterminada
    file_path = sys.argv[1] if len(sys.argv) > 1 else "listado.XLS.xlsx"
    
    try:
        # Importar proveedores
        inserted, updated, errors, skipped = await import_listado_proveedores(file_path)
        
        # Mostrar muestra de datos si la importación fue exitosa
        if errors == 0 and (inserted > 0 or updated > 0):
            await show_sample_data()
        
    except KeyboardInterrupt:
        print("\n\n⚠ Importación cancelada por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

