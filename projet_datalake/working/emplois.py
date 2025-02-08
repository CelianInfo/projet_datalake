import duckdb
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

df.columns = ['Nom_Entreprise','Titre_Offre','Lieu','Anciennete_Publication','Nombre_Candidats','Url','Description_Offre','Niveau_Hierarchique','Type_Emplois','Fonction','Secteurs']

df['Anciennete_Publication'] = df['Anciennete_Publication'].fillna('Inconnue')
df['Nombre_Candidats'] = df['Nombre_Candidats'].fillna('Inconnue')

# df_junk_anciennete_nbcandidats = df[['Anciennete_Publication','Nombre_Candidats']].sort_values(by=['Anciennete_Publication','Nombre_Candidats'])

df['Url'] = df['Url'].fillna('Inconnue')

df['Niveau_Hierarchique'] = df['Niveau_Hierarchique'].apply(lambda x: x[0])
df['Fonction'] = df['Fonction'].apply(lambda x: x[0])
df['Type_Emplois'] = df['Type_Emplois'].apply(lambda x: x[0])

# df_junk_hierarchie_fonction = df[['Type_Emplois','Niveau_Hierarchique','Fonction']]
# df_junk_hierarchie_fonction = df_junk_hierarchie_fonction.drop_duplicates().sort_values(by=['Type_Emplois','Niveau_Hierarchique','Fonction'])

df_dim_texte_emplois = df['Description_Offre']


show(df)