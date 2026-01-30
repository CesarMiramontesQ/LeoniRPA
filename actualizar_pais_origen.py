"""
Script para actualizar/cargar datos de país de origen de materiales desde Excel.

Para ejecutar este script, abre una terminal y ejecuta:
    python3 actualizar_pais_origen.py

Opciones:
    python3 actualizar_pais_origen.py --ver       # Solo ver datos del Excel sin cargar
    python3 actualizar_pais_origen.py --dry-run   # Simular la carga sin insertar datos

El Excel debe contener las columnas:
    - codigo_proveedor (o 'Codigo Proveedor', 'CODIGO_PROVEEDOR', 'Proveedor')
    - numero_material (o 'Numero Material', 'NUMERO_MATERIAL', 'Material')
    - pais_origen (o 'Pais Origen', 'PAIS_ORIGEN', 'Pais', 'Country')
"""
import asyncio
import pandas as pd
from sqlalchemy import text
from app.db.base import AsyncSessionLocal


# Mapeo flexible de columnas del Excel a columnas de la tabla
COLUMN_MAPPINGS = {
    'codigo_proveedor': [
        'codigo_proveedor', 'Codigo Proveedor', 'CODIGO_PROVEEDOR', 
        'Codigo_Proveedor', 'Proveedor', 'proveedor', 'PROVEEDOR',
        'Vendor', 'vendor', 'VENDOR', 'Supplier', 'supplier'
    ],
    'numero_material': [
        'numero_material', 'Numero Material', 'NUMERO_MATERIAL',
        'Numero_Material', 'Material', 'material', 'MATERIAL',
        'Part Number', 'part_number', 'PART_NUMBER', 'Material Number'
    ],
    'pais_origen': [
        'pais_origen', 'Pais Origen', 'PAIS_ORIGEN', 'Pais_Origen',
        'Pais', 'pais', 'PAIS', 'Country', 'country', 'COUNTRY',
        'Country of Origin', 'country_origin', 'Origin', 'origin'
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
        col_lower = col_clean.lower().replace(' ', '_')
        for possible in possible_names:
            if possible.lower().replace(' ', '_') == col_lower:
                return excel_col
    
    return None


async def actualizar_pais_origen(dry_run: bool = False):
    """Actualiza/carga los datos del Excel a la tabla pais_origen_material."""
    
    archivo = "pais_origen.xlsx"
    
    # Leer el archivo Excel
    print(f"📄 Leyendo archivo: {archivo}")
    try:
        df = pd.read_excel(archivo, engine='openpyxl')
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el archivo '{archivo}'")
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
    
    for db_col in ['codigo_proveedor', 'numero_material', 'pais_origen']:
        match = find_column_match(excel_columns, db_col)
        if match:
            column_map[db_col] = match
            print(f"✅ Columna '{db_col}' → Excel '{match}'")
        else:
            print(f"❌ No se encontró columna para '{db_col}'")
    
    # Verificar que tenemos todas las columnas requeridas
    required_columns = ['codigo_proveedor', 'numero_material', 'pais_origen']
    missing = [col for col in required_columns if col not in column_map]
    
    if missing:
        print(f"\n❌ ERROR: Faltan las siguientes columnas: {missing}")
        print(f"\n💡 Asegúrate de que el Excel tenga columnas con nombres similares a:")
        for col in missing:
            print(f"   - {col}: {COLUMN_MAPPINGS[col][:5]}...")
        return
    
    print(f"\n🔍 Primeras 5 filas del Excel:")
    preview_cols = [column_map[c] for c in required_columns]
    print(df[preview_cols].head().to_string())
    
    if dry_run:
        print(f"\n⚠️  MODO DRY-RUN: No se realizarán cambios en la base de datos")
        print(f"\n📊 Resumen de datos a procesar:")
        print(f"   - Total registros: {len(df)}")
        print(f"   - Proveedores únicos: {df[column_map['codigo_proveedor']].nunique()}")
        print(f"   - Materiales únicos: {df[column_map['numero_material']].nunique()}")
        print(f"   - Países únicos: {df[column_map['pais_origen']].nunique()}")
        return
    
    async with AsyncSessionLocal() as session:
        try:
            updated = 0
            not_found = 0
            errors_vacios = 0
            errors_otros = 0
            error_details_vacios = []
            error_details_otros = []
            
            print(f"\n🚀 Iniciando actualización de datos...")
            
            for idx, row in df.iterrows():
                try:
                    # Obtener valores
                    codigo_proveedor = row[column_map['codigo_proveedor']]
                    numero_material = row[column_map['numero_material']]
                    pais_origen = row[column_map['pais_origen']]
                    
                    # Convertir NaN a None y limpiar valores
                    if pd.isna(codigo_proveedor):
                        codigo_proveedor = None
                    else:
                        codigo_proveedor = str(codigo_proveedor).strip()
                    
                    if pd.isna(numero_material):
                        numero_material = None
                    else:
                        numero_material = str(numero_material).strip()
                    
                    if pd.isna(pais_origen):
                        pais_origen = None
                    else:
                        pais_origen = str(pais_origen).strip()
                    
                    # Validar que tenemos los campos requeridos
                    if not codigo_proveedor or not numero_material or not pais_origen:
                        errors_vacios += 1
                        error_details_vacios.append(f"Fila {idx+2}: proveedor={codigo_proveedor}, material={numero_material}, pais={pais_origen}")
                        continue
                    
                    # Solo UPDATE de registros existentes
                    query = text("""
                        UPDATE pais_origen_material 
                        SET pais_origen = :pais_origen,
                            updated_at = NOW()
                        WHERE codigo_proveedor = :codigo_proveedor 
                          AND numero_material = :numero_material
                    """)
                    
                    result = await session.execute(query, {
                        'codigo_proveedor': codigo_proveedor,
                        'numero_material': numero_material,
                        'pais_origen': pais_origen
                    })
                    
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
            print(f"   ❌ Campos vacíos en Excel: {errors_vacios}")
            
            if error_details_vacios:
                print(f"\n📋 Registros con país vacío en el Excel (primeros 10):")
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
    archivo = "pais_origen.xlsx"
    
    print(f"📄 Leyendo archivo: {archivo}")
    try:
        df = pd.read_excel(archivo, engine='openpyxl')
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el archivo '{archivo}'")
        return
    except Exception as e:
        print(f"❌ ERROR al leer el archivo: {e}")
        return
    
    print(f"\n📋 Columnas: {df.columns.tolist()}")
    print(f"📊 Total filas: {len(df)}")
    print(f"\n📄 Contenido completo:")
    print(df.to_string())


async def ver_datos_tabla():
    """Muestra los datos actuales en la tabla pais_origen_material."""
    async with AsyncSessionLocal() as session:
        query = text("""
            SELECT codigo_proveedor, numero_material, pais_origen, updated_at
            FROM pais_origen_material
            ORDER BY updated_at DESC
            LIMIT 20
        """)
        result = await session.execute(query)
        rows = result.fetchall()
        
        print(f"\n📊 Últimos 20 registros en la tabla pais_origen_material:")
        print(f"{'Código Proveedor':<20} {'Número Material':<20} {'País Origen':<15} {'Actualizado':<25}")
        print("=" * 80)
        
        for row in rows:
            print(f"{str(row[0]):<20} {str(row[1]):<20} {str(row[2]):<15} {str(row[3]):<25}")
        
        # Contar total
        count_query = text("SELECT COUNT(*) FROM pais_origen_material")
        count_result = await session.execute(count_query)
        total = count_result.scalar()
        print(f"\n📈 Total registros en la tabla: {total}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--ver":
            # Solo ver datos del Excel sin cargar
            asyncio.run(ver_datos_excel())
        elif sys.argv[1] == "--dry-run":
            # Simular la carga sin insertar datos
            asyncio.run(actualizar_pais_origen(dry_run=True))
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
        asyncio.run(actualizar_pais_origen())
