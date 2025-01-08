import os
import json
import pandas as pd

# Définir le chemin du répertoire contenant les fichiers JSON
directory_path = r"C:\Users\eelmakoul\Documents\GitHub\projet_datalake\TD_DATALAKE\DATALAKE\2_CURATED_ZONE\LINKEDIN\EMP"

# Liste pour stocker les données de tous les fichiers JSON
data = []

# Parcourir tous les fichiers du répertoire
for filename in os.listdir(directory_path):
    if filename.endswith(".json"):
        file_path = os.path.join(directory_path, filename)
        
            
             # Ouvrir et charger le fichier JSON
        with open(file_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
            
            df = pd.json_normalize(json_data)
            
            dataframes[filename] = df

pd.read_csv(df);
