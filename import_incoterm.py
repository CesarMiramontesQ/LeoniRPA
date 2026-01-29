"""
Script para importar datos de incoterm.xlsx y actualizar la tabla master_unificado_virtuales.

Lee la columna "SOLD TO NO." y la compara con la columna "numero" de master_unificado_virtuales.
Si existe, actualiza las columnas "incoterm" y "tipo_exportacion".
Genera un reporte de los números de cliente que no existen.
"""

import asyncio
import pandas as pd
from pathlib import Path

# Importar dependencias de la app
from app.db.base import AsyncSessionLocal
from app.db.models import MasterUnificadoVirtuales
from sqlalchemy import select, update


async def get_existing_numeros(db) -> set:
    """Obtiene todos los números existentes en la tabla master_unificado_virtuales."""
    result = await db.execute(
        select(MasterUnificadoVirtuales.numero).where(MasterUnificadoVirtuales.numero.isnot(None))
    )
    return set(row[0] for row in result.fetchall())


async def update_incoterm_data(db, numero: int, incoterm: str, tipo_exportacion: str) -> bool:
    """Actualiza los datos de incoterm y tipo_exportacion para un número dado."""
    stmt = (
        update(MasterUnificadoVirtuales)
        .where(MasterUnificadoVirtuales.numero == numero)
        .values(incoterm=incoterm, tipo_exportacion=tipo_exportacion)
    )
    result = await db.execute(stmt)
    await db.commit()
    return result.rowcount > 0


async def main():
    """Función principal que ejecuta la importación."""
    # Ruta del archivo Excel
    excel_path = Path(__file__).parent / "incoterm.xlsx"
    
    if not excel_path.exists():
        print(f"Error: No se encontró el archivo {excel_path}")
        return
    
    # Leer el archivo Excel
    print(f"Leyendo archivo: {excel_path}")
    df = pd.read_excel(excel_path)
    
    print(f"Total de registros en el Excel: {len(df)}")
    print(f"Columnas encontradas: {list(df.columns)}")
    
    # Verificar que las columnas necesarias existan
    columnas_requeridas = ['SOLD TO NO.', 'INCOTERM', 'VIRTUAL O SE EXPORTA + COMENTARIOS ADICIONALES']
    for col in columnas_requeridas:
        if col not in df.columns:
            print(f"Error: No se encontró la columna '{col}' en el archivo Excel")
            return
    
    # Conectar a la base de datos
    async with AsyncSessionLocal() as db:
        # Obtener los números existentes en la tabla
        numeros_existentes = await get_existing_numeros(db)
        print(f"Números existentes en master_unificado_virtuales: {len(numeros_existentes)}")
        
        # Contadores
        actualizados = 0
        no_encontrados = []
        errores = []
        
        # Procesar cada fila del Excel
        for idx, row in df.iterrows():
            sold_to_no = row['SOLD TO NO.']
            incoterm = row['INCOTERM']
            tipo_exportacion = row['VIRTUAL O SE EXPORTA + COMENTARIOS ADICIONALES']
            
            # Validar que sold_to_no sea un número válido
            if pd.isna(sold_to_no):
                continue
            
            try:
                # Convertir a entero
                numero = int(sold_to_no)
            except (ValueError, TypeError):
                errores.append(f"Fila {idx + 2}: '{sold_to_no}' no es un número válido")
                continue
            
            # Convertir valores a string (manejo de NaN)
            incoterm_str = str(incoterm) if pd.notna(incoterm) else ''
            tipo_exp_str = str(tipo_exportacion) if pd.notna(tipo_exportacion) else ''
            
            # Verificar si el número existe en la tabla
            if numero in numeros_existentes:
                # Actualizar los datos
                try:
                    success = await update_incoterm_data(db, numero, incoterm_str, tipo_exp_str)
                    if success:
                        actualizados += 1
                except Exception as e:
                    errores.append(f"Fila {idx + 2}: Error al actualizar {numero}: {str(e)}")
            else:
                no_encontrados.append(numero)
        
        # Reporte final
        print("\n" + "=" * 60)
        print("REPORTE DE IMPORTACIÓN")
        print("=" * 60)
        print(f"Registros actualizados exitosamente: {actualizados}")
        print(f"Registros no encontrados en la tabla: {len(no_encontrados)}")
        print(f"Errores durante el proceso: {len(errores)}")
        
        # Mostrar números no encontrados (sin duplicados)
        if no_encontrados:
            numeros_unicos = sorted(set(no_encontrados))
            print(f"\n--- Números de cliente NO encontrados en master_unificado_virtuales ({len(numeros_unicos)} únicos) ---")
            for num in numeros_unicos:
                print(f"  - {num}")
        
        # Mostrar errores
        if errores:
            print("\n--- Errores encontrados ---")
            for error in errores:
                print(f"  - {error}")
        
        print("\n" + "=" * 60)
        print("Importación completada")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
