import pandas as pd
from pandasgui import show

import json

file_path = r"C:\Users\538128\Desktop\github\projet_datalake\TD_DATALAKE\DATALAKE\2_CURATED_ZONE\LINKEDIN\EMP\13546-INFO-EMP-LINKEDIN-FR-1599984246.json"

with open(file_path, 'r') as file:
    data = json.load(file)
    df_avis = pd.json_normalize(data)
    show(df_avis)