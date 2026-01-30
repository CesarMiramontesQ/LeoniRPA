"""
Script para cargar datos de proveedores desde Excel a la tabla carga_proveedores.

Para ejecutar este script, abre una nueva terminal y ejecuta:
    python3 cargar_proveedores.py

O desde la misma carpeta del proyecto si tienes un entorno virtual:
    source venv/bin/activate  # si aplica
    python cargar_proveedores.py
"""
import asyncio
import pandas as pd
from sqlalchemy import text
from app.db.base import AsyncSessionLocal


async def cargar_proveedores():
    """Carga los datos del Excel a la tabla carga_proveedores."""
    
    archivo = "carga_proveedores_con_codigo.xlsx"
    
    # Leer el archivo Excel
    print(f"📄 Leyendo archivo: {archivo}")
    df = pd.read_excel(archivo, engine='openpyxl')
    
    print(f"\n📋 Columnas encontradas en el Excel:")
    for i, col in enumerate(df.columns.tolist()):
        print(f"   {i+1}. {col}")
    
    print(f"\n📊 Total de registros: {len(df)}")
    print("\n🔍 Primeras 5 filas del Excel:")
    print(df.head().to_string())
    
    # Mapeo de columnas del Excel a columnas de la tabla
    # Basado en las columnas reales del archivo Excel
    column_mapping = {
        'Nombre o Razón social': 'nombre',
        'Apellido Paterno': 'apellido_paterno',
        'Apellido Materno': 'apellido_materno',
        'País ': 'pais',  # tiene espacio al final
        'País': 'pais',   # sin espacio
        'Domicilio': 'domicilio',
        'Cliente/Proveedor': 'cliente_proveedor',
        'Estatus': 'estatus',
        'codigo_proveedor': 'codigo_proveedor',
    }
    
    # Determinar qué columnas del Excel mapean a la tabla
    excel_to_db = {}
    for excel_col in df.columns:
        # Buscar coincidencia exacta o con strip
        col_clean = str(excel_col).strip()
        if excel_col in column_mapping:
            excel_to_db[excel_col] = column_mapping[excel_col]
        elif col_clean in column_mapping:
            excel_to_db[excel_col] = column_mapping[col_clean]
    
    print(f"\n🔗 Mapeo de columnas detectado:")
    for excel_col, db_col in excel_to_db.items():
        print(f"   Excel '{excel_col}' → DB '{db_col}'")
    
    if 'codigo_proveedor' not in excel_to_db.values():
        print("\n❌ ERROR: No se encontró la columna 'codigo_proveedor' o 'codigo' en el Excel")
        print("   Esta columna es requerida para la carga.")
        return
    
    async with AsyncSessionLocal() as session:
        try:
            inserted = 0
            errors = 0
            error_details = []
            
            print(f"\n🚀 Iniciando carga de datos...")
            
            for idx, row in df.iterrows():
                try:
                    # Construir diccionario de valores
                    values = {}
                    for excel_col, db_col in excel_to_db.items():
                        val = row[excel_col]
                        # Convertir NaN a None
                        if pd.isna(val):
                            val = None
                        # Convertir a string si no es None
                        elif val is not None:
                            val = str(val).strip()
                            if val == '':
                                val = None
                        values[db_col] = val
                    
                    # Solo insertar si tiene codigo_proveedor
                    codigo = values.get('codigo_proveedor')
                    if codigo:
                        # Construir la consulta de inserción
                        columns = ', '.join(values.keys())
                        placeholders = ', '.join([f':{k}' for k in values.keys()])
                        
                        query = text(f"""
                            INSERT INTO carga_proveedores ({columns})
                            VALUES ({placeholders})
                        """)
                        
                        await session.execute(query, values)
                        inserted += 1
                        
                        if inserted % 10 == 0:
                            print(f"   Insertados: {inserted} registros...")
                    else:
                        errors += 1
                        error_details.append(f"Fila {idx+2}: codigo_proveedor vacío")
                    
                except Exception as e:
                    errors += 1
                    error_details.append(f"Fila {idx+2}: {str(e)}")
                    continue
            
            await session.commit()
            
            print(f"\n{'='*50}")
            print(f"✅ CARGA COMPLETADA")
            print(f"{'='*50}")
            print(f"   📥 Registros insertados: {inserted}")
            print(f"   ❌ Errores: {errors}")
            
            if error_details and len(error_details) <= 10:
                print(f"\n⚠️  Detalles de errores:")
                for err in error_details:
                    print(f"   - {err}")
            elif error_details:
                print(f"\n⚠️  Primeros 10 errores:")
                for err in error_details[:10]:
                    print(f"   - {err}")
                print(f"   ... y {len(error_details) - 10} errores más")
            
        except Exception as e:
            await session.rollback()
            print(f"\n❌ Error durante la carga: {e}")
            raise


async def ver_datos_excel():
    """Solo muestra los datos del Excel sin cargarlos."""
    archivo = "carga_proveedores_con_codigo.xlsx"
    print(f"📄 Leyendo archivo: {archivo}")
    df = pd.read_excel(archivo, engine='openpyxl')
    
    print(f"\n📋 Columnas: {df.columns.tolist()}")
    print(f"📊 Total filas: {len(df)}")
    print(f"\n📄 Contenido completo:")
    print(df.to_string())


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--ver":
        # Solo ver datos sin cargar
        asyncio.run(ver_datos_excel())
    else:
        # Cargar datos a la base de datos
        asyncio.run(cargar_proveedores())
