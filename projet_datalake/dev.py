import json
import pandas as pd
from pandasgui import show

file_path = r"C:\Users\ctoureille\Desktop\projet_datalake\TD_DATALAKE\DATALAKE\2_CURATED_ZONE\GLASSDOOR\AVI\13546-AVIS-SOC-GLASSDOOR-E12966_P1.json"

liste_avis = []
liste_contenu_avis = []

with open(file_path, 'r') as f:
  liste_avis_glassdoor = json.load(f)

for avis_glassdoor in liste_avis_glassdoor:
  contenu_avis_glassdoor = avis_glassdoor.pop('contenu')

  liste_avis.append(avis_glassdoor)
  liste_contenu_avis.append(contenu_avis_glassdoor)

df_avis = pd.json_normalize(liste_avis)
list_df_contenu_avis = []

for contenu_avis in liste_contenu_avis:
  list_df_contenu_avis.append(pd.json_normalize(contenu_avis))

print(df_avis)
show(list_df_contenu_avis[0])