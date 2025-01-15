import os
import json
import pandas as pd
from pandasgui import show


directory_path = r"C:\Users\eelmakoul\Documents\GitHub\projet_datalake\TD_DATALAKE\DATALAKE\2_CURATED_ZONE\LINKEDIN\EMP"

dataframes = []

# Parcourir tous les fichiers du répertoire
for filename in os.listdir(directory_path):
    if filename.endswith(".json"):
        file_path = os.path.join(directory_path, filename)
           
        with open(file_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
            
            # Convertir le JSON en DataFrame
            df = pd.json_normalize(json_data)
            
            dataframes.append(df)

final_df = pd.concat(dataframes, ignore_index=True)

# Vérifier les colonnes vides et les remplir avec NaN si elles sont vides dans un autre fichier
for column in final_df.columns:
    if final_df[column].isnull().any():
        final_df[column] = final_df[column].fillna(pd.NA)

show(final_df)

## final_df.to_csv(r"C:\Users\eelmakoul\Documents\GitHub\projet_datalake\TD_DATALAKE\DATALAKE\2_CURATED_ZONE\LINKEDIN\EMP\final_data.csv", index=False, encoding="utf-8")
