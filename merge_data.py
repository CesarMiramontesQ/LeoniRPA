import pandas as pd

# Leer el archivo CSV
csv_file = "Master_virtuales.csv"
df_csv = pd.read_csv(csv_file)

# Leer el archivo Excel
excel_file = "incorterm.xlsx"
df_excel = pd.read_excel(excel_file)

print(f"Registros en CSV: {len(df_csv)}")
print(f"Registros en Excel: {len(df_excel)}")

# Convertir la columna NUMERO DE CLIENTE a numérico para asegurar coincidencia
df_csv['NUMERO DE CLIENTE'] = pd.to_numeric(df_csv['NUMERO DE CLIENTE'], errors='coerce')
df_excel['SOLD TO NO.'] = pd.to_numeric(df_excel['SOLD TO NO.'], errors='coerce')

# Crear un diccionario de búsqueda desde el Excel
# Agrupar por SOLD TO NO. y tomar el primer valor de INCOTERM y VIRTUAL
lookup_df = df_excel.drop_duplicates(subset=['SOLD TO NO.'])[['SOLD TO NO.', 'INCOTERM', 'VIRTUAL O SE EXPORTA + COMENTARIOS ADICIONALES']]
lookup_dict_incoterm = dict(zip(lookup_df['SOLD TO NO.'], lookup_df['INCOTERM']))
lookup_dict_virtual = dict(zip(lookup_df['SOLD TO NO.'], lookup_df['VIRTUAL O SE EXPORTA + COMENTARIOS ADICIONALES']))

# Agregar las nuevas columnas al CSV
df_csv['INCOTERM'] = df_csv['NUMERO DE CLIENTE'].map(lookup_dict_incoterm)
df_csv['VIRTUAL O SE EXPORTA + COMENTARIOS ADICIONALES'] = df_csv['NUMERO DE CLIENTE'].map(lookup_dict_virtual)

# Contar coincidencias
coincidencias = df_csv['INCOTERM'].notna().sum()
sin_coincidencia = df_csv['INCOTERM'].isna().sum()

print(f"\nResultados del cruce:")
print(f"- Registros con coincidencia: {coincidencias}")
print(f"- Registros sin coincidencia: {sin_coincidencia}")

# Guardar el archivo actualizado
output_file = "Master_virtuales_actualizado.csv"
df_csv.to_csv(output_file, index=False)
print(f"\nArchivo guardado como: {output_file}")

# Mostrar algunos ejemplos de coincidencias
print("\nEjemplos de coincidencias encontradas:")
matches = df_csv[df_csv['INCOTERM'].notna()][['NUMERO DE CLIENTE', 'PROVEEDOR-CLIENTE', 'INCOTERM', 'VIRTUAL O SE EXPORTA + COMENTARIOS ADICIONALES']].head(10)
print(matches.to_string())

# Mostrar los registros sin coincidencia
print("\n" + "="*80)
print("Registros SIN coincidencia (NUMERO DE CLIENTE no encontrado en el Excel):")
print("="*80)
no_matches = df_csv[df_csv['INCOTERM'].isna()][['NUMERO DE CLIENTE', 'PROVEEDOR-CLIENTE']].drop_duplicates()
print(no_matches.to_string())
