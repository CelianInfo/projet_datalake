import duckdb
import pandas as pd
from pandasgui import show

# Specify the path to your DuckDB database file
database_path = 'TD_DATALAKE/DATALAKE/3_PRODUCTION_ZONE/database.duckdb'

# Connect to the DuckDB database
con = duckdb.connect(database_path)

# Specify the name of the table you want to load
table_name = 'avis_glassdoor'

# Load the table into a pandas DataFrame
df = con.execute(f'SELECT * FROM public.{table_name}').fetchdf()

df.columns = ['Origine','Id_Entreprise','Nom_Entreprise','Date_Timestamp','Titre','Note','Description_Employe','Anciennete_Employe','Recommandation','Point_De_Vue','Approbation_PDG','Avantages','Inconvenients','Conseils_Direction']

df = df.dropna(subset=['Date_Timestamp'])

df['Date'] = df['Date_Timestamp'].str.split(' ').apply(lambda x: x[0])
df['Heure'] = df['Date_Timestamp'].str.split(' ').apply(lambda x: x[1])

df = df.reset_index(drop=False)

df.rename(columns={'index': 'ID_Avis'}, inplace=True)

# df_junk_recommandations = df[['ID_Avis','Avantages','Inconvenients','Conseils_Direction']]
# df_junk_status_employe = df[['Description_Employe','Anciennete_Employe']]

show(df)