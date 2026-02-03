"""
Script para agregar columnas INCOTERM y VIRTUAL O SE EXPORTA + COMENTARIOS ADICIONALES
al archivo Master_virtuales.csv basándose en la relación con incoterm.xlsx
"""

import pandas as pd
import os

def merge_incoterm_data():
    # Rutas de los archivos
    script_dir = os.path.dirname(os.path.abspath(__file__))
    master_path = os.path.join(script_dir, 'Master_virtuales.csv')
    incoterm_path = os.path.join(script_dir, 'incoterm.xlsx')
    output_path = os.path.join(script_dir, 'Master_virtuales_con_incoterm.csv')
    
    print("Leyendo archivos...")
    
    # Leer el archivo Master_virtuales.csv
    master_df = pd.read_csv(master_path)
    print(f"Master_virtuales.csv: {len(master_df)} filas")
    print(f"Columnas: {master_df.columns.tolist()}")
    
    # Leer el archivo incoterm.xlsx
    incoterm_df = pd.read_excel(incoterm_path)
    print(f"\nincoterm.xlsx: {len(incoterm_df)} filas")
    print(f"Columnas: {incoterm_df.columns.tolist()}")
    
    # Verificar que existan las columnas necesarias
    if 'NUMERO DE CLIENTE' not in master_df.columns:
        print("ERROR: No se encontró la columna 'NUMERO DE CLIENTE' en Master_virtuales.csv")
        return
    
    if 'SOLD TO NO.' not in incoterm_df.columns:
        print("ERROR: No se encontró la columna 'SOLD TO NO.' en incoterm.xlsx")
        print("Columnas disponibles en incoterm.xlsx:", incoterm_df.columns.tolist())
        return
    
    # Verificar las columnas a agregar
    columnas_a_agregar = ['INCOTERM', 'VIRTUAL O SE EXPORTA + COMENTARIOS ADICIONALES']
    for col in columnas_a_agregar:
        if col not in incoterm_df.columns:
            print(f"ADVERTENCIA: No se encontró la columna '{col}' en incoterm.xlsx")
            print("Columnas disponibles:", incoterm_df.columns.tolist())
    
    # Convertir las columnas de unión a string para evitar problemas de tipo
    master_df['NUMERO DE CLIENTE'] = master_df['NUMERO DE CLIENTE'].astype(str).str.strip()
    incoterm_df['SOLD TO NO.'] = incoterm_df['SOLD TO NO.'].astype(str).str.strip()
    
    # Seleccionar solo las columnas necesarias del archivo incoterm
    columnas_disponibles = ['SOLD TO NO.'] + [col for col in columnas_a_agregar if col in incoterm_df.columns]
    incoterm_subset = incoterm_df[columnas_disponibles].drop_duplicates(subset=['SOLD TO NO.'])
    
    print(f"\nRegistros únicos en incoterm por SOLD TO NO.: {len(incoterm_subset)}")
    
    # Hacer el merge (left join para mantener todos los registros del master)
    resultado_df = master_df.merge(
        incoterm_subset,
        left_on='NUMERO DE CLIENTE',
        right_on='SOLD TO NO.',
        how='left'
    )
    
    # Eliminar la columna duplicada SOLD TO NO. si existe
    if 'SOLD TO NO.' in resultado_df.columns:
        resultado_df = resultado_df.drop(columns=['SOLD TO NO.'])
    
    # Contar coincidencias
    columnas_agregadas = [col for col in columnas_a_agregar if col in resultado_df.columns]
    if columnas_agregadas:
        coincidencias = resultado_df[columnas_agregadas[0]].notna().sum()
        print(f"\nCoincidencias encontradas: {coincidencias} de {len(master_df)} registros")
    
    # Guardar el resultado
    resultado_df.to_csv(output_path, index=False)
    print(f"\nArchivo guardado: {output_path}")
    print(f"Total de filas en el resultado: {len(resultado_df)}")
    print(f"Columnas en el resultado: {resultado_df.columns.tolist()}")
    
    # Mostrar algunos ejemplos de coincidencias
    if columnas_agregadas:
        print("\n--- Ejemplos de registros con datos de incoterm ---")
        ejemplos = resultado_df[resultado_df[columnas_agregadas[0]].notna()].head(5)
        for _, row in ejemplos.iterrows():
            print(f"Cliente: {row['NUMERO DE CLIENTE']}")
            for col in columnas_agregadas:
                print(f"  {col}: {row[col]}")
            print()

if __name__ == "__main__":
    merge_incoterm_data()
