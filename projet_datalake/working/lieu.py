﻿import duckdb
import pandas as pd
from pandasgui import show

# Specify the path to your DuckDB database file
database_path = 'TD_DATALAKE/DATALAKE/3_PRODUCTION_ZONE/database.duckdb'

# Connect to the DuckDB database
con = duckdb.connect(database_path)

# Specify the name of the table you want to load
table_name = 'emplois_linkedin'

# Load the table into a pandas DataFrame
df = con.execute(f'SELECT * FROM public.{table_name}').fetchdf()

# Display the DataFrame

df['Lieu'] = df['Lieu'].str.replace(r'\d+, ', '', regex=True)
df['Lieu'] = df['Lieu'].str.split(',').str[0]
df['Lieu'] = df['Lieu'].str.replace(' FR','')
df['Lieu'] = df['Lieu'].str.replace(' Area','')
df['Lieu'] = df['Lieu'].str.replace('Région de ','')

mapping_dict = {
    "Paris La Défense": "Paris",
    "Paris et périphérie": "Paris",
    "Paris Île-de-France": "Paris",
}

df['Lieu'] = df['Lieu'].replace(mapping_dict)
df['Lieu'] = df['Lieu'].str.strip()

df = df.drop_duplicates(subset='Lieu', keep='first')

show(df)


