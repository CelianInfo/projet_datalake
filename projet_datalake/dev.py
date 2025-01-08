import json
import pandas as pd
from pandasgui import show

file_path = r"C:\Users\ctoureille\Desktop\projet_datalake\TD_DATALAKE\DATALAKE\2_CURATED_ZONE\GLASSDOOR\AVI\13546-AVIS-SOC-GLASSDOOR-E12966_P1.json"

with open(file_path, 'r') as f:
  json_data = json.load(f)

df = pd.json_normalize(json_data)

show(df.melt(id_vars=['object','unique_key'], var_name='column_column_name', value_name='value'))