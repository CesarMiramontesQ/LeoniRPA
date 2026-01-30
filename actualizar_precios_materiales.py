"""
Script para actualizar Porcentaje de Compra y Comentario en la tabla precios_materiales.

Para ejecutar este script, abre una terminal y ejecuta:
    python3 actualizar_precios_materiales.py

Opciones:
    python3 actualizar_precios_materiales.py --ver       # Solo ver datos del Excel sin cargar
    python3 actualizar_precios_materiales.py --dry-run   # Simular la carga sin insertar datos
    python3 actualizar_precios_materiales.py --tabla     # Ver datos actuales en la tabla

El Excel debe contener las columnas:
    - numero_material (o 'Numero Material', 'NUMERO_MATERIAL', 'Material')
    - codigo_proveedor (o 'Codigo Proveedor', 'CODIGO_PROVEEDOR', 'Proveedor')
    - porcentaje_compra (o 'Porcentaje Compra', '%Compra', 'Porcentaje')
    - comentario (o 'Comentario', 'Comentarios', 'Notas', 'Observaciones')
"""
import asyncio
import pandas as pd
from sqlalchemy import text
from app.db.base import AsyncSessionLocal


# Archivo Excel a leer
ARCHIVO_EXCEL = "precios materiales.xlsx"

# Mapeo flexible de columnas del Excel a columnas de la tabla
COLUMN_MAPPINGS = {
    'numero_material': [
        'numero_material', 'Numero Material', 'NUMERO_MATERIAL',
        'Numero_Material', 'Material', 'material', 'MATERIAL',
        'Part Number', 'part_number', 'PART_NUMBER', 'Material Number',
        'NumeroMaterial', 'No. Material', 'No Material'
    ],
    'codigo_proveedor': [
        'codigo_proveedor', 'Codigo Proveedor', 'CODIGO_PROVEEDOR', 
        'Codigo_Proveedor', 'Proveedor', 'proveedor', 'PROVEEDOR',
        'Vendor', 'vendor', 'VENDOR', 'Supplier', 'supplier',
        'CodigoProveedor', 'Cod. Proveedor', 'Cod Proveedor'
    ],
    'porcentaje_compra': [
        'porcentaje_compra', 'Porcentaje_Compra', 'Porcentaje Compra',
        'PORCENTAJE_COMPRA', 'PORCENTAJE COMPRA', '%Compra', '% Compra',
        'PorcentajeCompra', 'Porcentaje', 'porcentaje', 'PORCENTAJE',
        '% de Compra', 'Pct Compra', 'PCT_COMPRA', 'Purchase %',
        'Purchase Percentage', 'Buy %', 'Buy Percentage'
    ],
    'comentario': [
        'comentario', 'Comentario', 'COMENTARIO', 'Comentarios',
        'comentarios', 'COMENTARIOS', 'Notas', 'notas', 'NOTAS',
        'Observaciones', 'observaciones', 'OBSERVACIONES',
        'Notes', 'notes', 'NOTES', 'Comment', 'comment', 'COMMENT',
        'Comments', 'comments', 'COMMENTS', 'Observacion', 'Nota'
    ]
}


def find_column_match(excel_columns: list, db_column: str) -> str | None:
    """
    Busca una coincidencia entre las columnas del Excel y los posibles nombres
    de una columna de la base de datos.
    """
    possible_names = COLUMN_MAPPINGS.get(db_column, [])
    
    for excel_col in excel_columns:
        col_clean = str(excel_col).strip()
        if col_clean in possible_names:
            return excel_col
        # Comparación sin distinción de mayúsculas/minúsculas
        col_lower = col_clean.lower().replace(' ', '_').replace('%', 'porcentaje')
        for possible in possible_names:
            if possible.lower().replace(' ', '_').replace('%', 'porcentaje') == col_lower:
                return excel_col
    
    return None


async def actualizar_precios_materiales(dry_run: bool = False):
    """Actualiza los campos Porcentaje_Compra y Comentario en la tabla precios_materiales."""
    
    # Leer el archivo Excel
    print(f"📄 Leyendo archivo: {ARCHIVO_EXCEL}")
    try:
        df = pd.read_excel(ARCHIVO_EXCEL, engine='openpyxl')
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el archivo '{ARCHIVO_EXCEL}'")
        return
    except Exception as e:
        print(f"❌ ERROR al leer el archivo: {e}")
        return
    
    print(f"\n📋 Columnas encontradas en el Excel:")
    for i, col in enumerate(df.columns.tolist()):
        print(f"   {i+1}. '{col}'")
    
    print(f"\n📊 Total de registros en el Excel: {len(df)}")
    
    # Buscar las columnas requeridas
    excel_columns = df.columns.tolist()
    column_map = {}
    
    # Columnas requeridas para búsqueda
    required_columns = ['numero_material', 'codigo_proveedor']
    # Columnas opcionales para actualización (al menos una debe existir)
    optional_columns = ['porcentaje_compra', 'comentario']
    
    print(f"\n🔍 Buscando columnas...")
    
    for db_col in required_columns + optional_columns:
        match = find_column_match(excel_columns, db_col)
        if match:
            column_map[db_col] = match
            print(f"✅ Columna '{db_col}' → Excel '{match}'")
        else:
            if db_col in required_columns:
                print(f"❌ No se encontró columna requerida para '{db_col}'")
            else:
                print(f"⚠️  No se encontró columna opcional para '{db_col}'")
    
    # Verificar que tenemos las columnas requeridas
    missing_required = [col for col in required_columns if col not in column_map]
    
    if missing_required:
        print(f"\n❌ ERROR: Faltan las siguientes columnas requeridas: {missing_required}")
        print(f"\n💡 Asegúrate de que el Excel tenga columnas con nombres similares a:")
        for col in missing_required:
            print(f"   - {col}: {COLUMN_MAPPINGS[col][:5]}...")
        return
    
    # Verificar que tenemos al menos una columna para actualizar
    update_columns = [col for col in optional_columns if col in column_map]
    
    if not update_columns:
        print(f"\n❌ ERROR: No se encontró ninguna columna para actualizar")
        print(f"\n💡 El Excel debe tener al menos una de estas columnas:")
        for col in optional_columns:
            print(f"   - {col}: {COLUMN_MAPPINGS[col][:5]}...")
        return
    
    print(f"\n✅ Columnas a actualizar: {update_columns}")
    
    # Mostrar preview de los datos
    print(f"\n🔍 Primeras 5 filas del Excel:")
    preview_cols = [column_map[c] for c in required_columns + update_columns]
    print(df[preview_cols].head().to_string())
    
    if dry_run:
        print(f"\n⚠️  MODO DRY-RUN: No se realizarán cambios en la base de datos")
        print(f"\n📊 Resumen de datos a procesar:")
        print(f"   - Total registros: {len(df)}")
        print(f"   - Proveedores únicos: {df[column_map['codigo_proveedor']].nunique()}")
        print(f"   - Materiales únicos: {df[column_map['numero_material']].nunique()}")
        
        if 'porcentaje_compra' in column_map:
            porcentaje_col = column_map['porcentaje_compra']
            valores_no_nulos = df[porcentaje_col].notna().sum()
            print(f"   - Registros con porcentaje: {valores_no_nulos}")
        
        if 'comentario' in column_map:
            comentario_col = column_map['comentario']
            valores_no_nulos = df[comentario_col].notna().sum()
            print(f"   - Registros con comentario: {valores_no_nulos}")
        return
    
    async with AsyncSessionLocal() as session:
        try:
            updated = 0
            not_found = 0
            errors_vacios = 0
            errors_otros = 0
            error_details_vacios = []
            error_details_otros = []
            
            print(f"\n🚀 Iniciando actualización de datos en tabla precios_materiales...")
            
            for idx, row in df.iterrows():
                try:
                    # Obtener valores de búsqueda
                    codigo_proveedor = row[column_map['codigo_proveedor']]
                    numero_material = row[column_map['numero_material']]
                    
                    # Convertir NaN a None y limpiar valores
                    if pd.isna(codigo_proveedor):
                        codigo_proveedor = None
                    else:
                        codigo_proveedor = str(codigo_proveedor).strip()
                    
                    if pd.isna(numero_material):
                        numero_material = None
                    else:
                        numero_material = str(numero_material).strip()
                    
                    # Validar que tenemos los campos de búsqueda
                    if not codigo_proveedor or not numero_material:
                        errors_vacios += 1
                        error_details_vacios.append(f"Fila {idx+2}: proveedor={codigo_proveedor}, material={numero_material}")
                        continue
                    
                    # Preparar los valores a actualizar
                    update_values = {}
                    
                    if 'porcentaje_compra' in column_map:
                        porcentaje = row[column_map['porcentaje_compra']]
                        if pd.isna(porcentaje):
                            update_values['porcentaje_compra'] = None
                        else:
                            # Intentar convertir a número
                            try:
                                update_values['porcentaje_compra'] = float(porcentaje)
                            except (ValueError, TypeError):
                                update_values['porcentaje_compra'] = None
                    
                    if 'comentario' in column_map:
                        comentario = row[column_map['comentario']]
                        if pd.isna(comentario):
                            update_values['comentario'] = None
                        else:
                            update_values['comentario'] = str(comentario).strip()
                    
                    # Construir la consulta UPDATE dinámica
                    set_clauses = []
                    params = {
                        'codigo_proveedor': codigo_proveedor,
                        'numero_material': numero_material
                    }
                    
                    if 'porcentaje_compra' in update_values:
                        set_clauses.append('"Porcentaje_Compra" = :porcentaje_compra')
                        params['porcentaje_compra'] = update_values['porcentaje_compra']
                    
                    if 'comentario' in update_values:
                        set_clauses.append('"Comentario" = :comentario')
                        params['comentario'] = update_values['comentario']
                    
                    if not set_clauses:
                        continue
                    
                    # Agregar updated_at
                    set_clauses.append('updated_at = NOW()')
                    
                    query = text(f"""
                        UPDATE precios_materiales 
                        SET {', '.join(set_clauses)}
                        WHERE codigo_proveedor = :codigo_proveedor 
                          AND numero_material = :numero_material
                    """)
                    
                    result = await session.execute(query, params)
                    
                    if result.rowcount > 0:
                        updated += 1
                    else:
                        # El registro no existe en la tabla
                        not_found += 1
                    
                    if (not_found + updated) % 50 == 0:
                        print(f"   Procesados: {not_found + updated} registros (actualizados: {updated}, no encontrados: {not_found})")
                
                except Exception as e:
                    errors_otros += 1
                    error_details_otros.append(f"Fila {idx+2}: {str(e)[:150]}")
                    continue
            
            await session.commit()
            
            print(f"\n{'='*60}")
            print(f"✅ ACTUALIZACIÓN COMPLETADA")
            print(f"{'='*60}")
            print(f"   🔄 Registros actualizados: {updated}")
            print(f"   ⚠️  No encontrados en la tabla: {not_found}")
            print(f"   ❌ Campos de búsqueda vacíos en Excel: {errors_vacios}")
            
            if error_details_vacios:
                print(f"\n📋 Registros con campos de búsqueda vacíos (primeros 10):")
                for err in error_details_vacios[:10]:
                    print(f"   - {err}")
                if len(error_details_vacios) > 10:
                    print(f"   ... y {len(error_details_vacios) - 10} más")
            
            if errors_otros > 0:
                print(f"\n⚠️  Otros errores: {errors_otros}")
                for err in error_details_otros[:5]:
                    print(f"   - {err}")
            
        except Exception as e:
            await session.rollback()
            print(f"\n❌ Error durante la actualización: {e}")
            raise


async def ver_datos_excel():
    """Solo muestra los datos del Excel sin cargarlos."""
    
    print(f"📄 Leyendo archivo: {ARCHIVO_EXCEL}")
    try:
        df = pd.read_excel(ARCHIVO_EXCEL, engine='openpyxl')
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el archivo '{ARCHIVO_EXCEL}'")
        return
    except Exception as e:
        print(f"❌ ERROR al leer el archivo: {e}")
        return
    
    print(f"\n📋 Columnas: {df.columns.tolist()}")
    print(f"📊 Total filas: {len(df)}")
    print(f"\n📄 Contenido completo:")
    print(df.to_string())


async def ver_datos_tabla():
    """Muestra los datos actuales en la tabla precios_materiales."""
    async with AsyncSessionLocal() as session:
        query = text("""
            SELECT codigo_proveedor, numero_material, "Porcentaje_Compra", "Comentario", updated_at
            FROM precios_materiales
            ORDER BY updated_at DESC
            LIMIT 20
        """)
        result = await session.execute(query)
        rows = result.fetchall()
        
        print(f"\n📊 Últimos 20 registros en la tabla precios_materiales:")
        print(f"{'Código Proveedor':<18} {'Número Material':<18} {'% Compra':<12} {'Comentario':<25} {'Actualizado':<22}")
        print("=" * 95)
        
        for row in rows:
            proveedor = str(row[0])[:16] if row[0] else '-'
            material = str(row[1])[:16] if row[1] else '-'
            porcentaje = str(row[2])[:10] if row[2] else '-'
            comentario = str(row[3])[:23] if row[3] else '-'
            updated = str(row[4])[:20] if row[4] else '-'
            print(f"{proveedor:<18} {material:<18} {porcentaje:<12} {comentario:<25} {updated:<22}")
        
        # Contar total
        count_query = text("SELECT COUNT(*) FROM precios_materiales")
        count_result = await session.execute(count_query)
        total = count_result.scalar()
        print(f"\n📈 Total registros en la tabla: {total}")
        
        # Contar con porcentaje/comentario
        count_porcentaje = text('SELECT COUNT(*) FROM precios_materiales WHERE "Porcentaje_Compra" IS NOT NULL')
        count_comentario = text('SELECT COUNT(*) FROM precios_materiales WHERE "Comentario" IS NOT NULL AND "Comentario" != \'\'')
        
        result_p = await session.execute(count_porcentaje)
        result_c = await session.execute(count_comentario)
        
        print(f"   - Con porcentaje de compra: {result_p.scalar()}")
        print(f"   - Con comentario: {result_c.scalar()}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--ver":
            # Solo ver datos del Excel sin cargar
            asyncio.run(ver_datos_excel())
        elif sys.argv[1] == "--dry-run":
            # Simular la carga sin insertar datos
            asyncio.run(actualizar_precios_materiales(dry_run=True))
        elif sys.argv[1] == "--tabla":
            # Ver datos actuales en la tabla
            asyncio.run(ver_datos_tabla())
        else:
            print(f"❌ Opción no reconocida: {sys.argv[1]}")
            print("Opciones disponibles:")
            print("   --ver       Ver datos del Excel")
            print("   --dry-run   Simular carga sin insertar")
            print("   --tabla     Ver datos actuales en la tabla")
    else:
        # Cargar/actualizar datos en la base de datos
        asyncio.run(actualizar_precios_materiales())
