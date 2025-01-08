import os
import json
import pandas as pd

# Définir le chemin du répertoire contenant les fichiers JSON
directory_path = r"C:\Users\eelmakoul\Documents\GitHub\projet_datalake\TD_DATALAKE\DATALAKE\2_CURATED_ZONE\LINKEDIN\EMP"

# Liste pour stocker les DataFrames de chaque fichier JSON
dataframes = []

# Parcourir tous les fichiers du répertoire
for filename in os.listdir(directory_path):
    if filename.endswith(".json"):
        file_path = os.path.join(directory_path, filename)
           
        # Ouvrir et charger le fichier JSON
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

print(final_df)

final_df.to_csv(r"C:\Users\eelmakoul\Documents\GitHub\projet_datalake\TD_DATALAKE\DATALAKE\2_CURATED_ZONE\LINKEDIN\EMP\final_data.csv", index=False, encoding="utf-8")
