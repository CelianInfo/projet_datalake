import os
import json
import pandas as pd
from pandasgui import show

# Chemin du répertoire
directory_path = r"C:\Users\eelmakoul\Documents\GitHub\projet_datalake\TD_DATALAKE\DATALAKE\2_CURATED_ZONE\LINKEDIN\EMP"

dataframes = []

# Fonction pour transformer les données complexes en valeurs simples
def flatten_job_criteria(job_criteria):
    # Si 'job_criteria' est un dictionnaire, on aplatit ses valeurs
    flattened = {}
    for key, value in job_criteria.items():
        if isinstance(value, list):
            # Si la valeur est une liste, on prend le premier élément (en supposant que la liste a une seule valeur)
            flattened[key] = value[0] if value else None
        else:
            flattened[key] = value
    return flattened

# Parcourir tous les fichiers du répertoire
for filename in os.listdir(directory_path):
    if filename.endswith(".json"):
        file_path = os.path.join(directory_path, filename)
        
        with open(file_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
            
            # Aplatir les données de 'job_criteria' en valeurs simples
            job_criteria_flat = flatten_job_criteria(json_data.get("job_criteria", {}))
            
            # Fusionner les données JSON avec les données de job_criteria aplaties
            json_data["job_criteria"] = job_criteria_flat
            
            # Convertir le JSON en DataFrame
            df = pd.json_normalize(json_data)
            
            dataframes.append(df)

# Combiner tous les DataFrames
final_df = pd.concat(dataframes, ignore_index=True)

# Vérifier les colonnes vides et les remplir avec NaN si elles sont vides dans un autre fichier
for column in final_df.columns:
    if final_df[column].isnull().any():
        final_df[column] = final_df[column].fillna(pd.NA)

# Afficher le DataFrame avec PandasGUI
show(final_df)

## final_df.to_csv(r"C:\Users\eelmakoul\Documents\GitHub\projet_datalake\TD_DATALAKE\DATALAKE\2_CURATED_ZONE\LINKEDIN\EMP\final_data.csv", index=False, encoding="utf-8")
