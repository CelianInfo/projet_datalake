import os, json
import pandas as pd
from pandasgui import show

def list_files_in_folder(folder_path: str) -> list[str]:
    """
    Lists all files within a specified folder.

    Args:
        context: The Dagster op context.
        folder_path: The path to the folder to list files from.

    Returns:
        A list of file paths.
    """
    files = os.listdir(folder_path)
    file_paths = [os.path.join(folder_path, file) for file in files if '.gitkeep' not in file]

    return file_paths


def avis_glassdoor():
    current_dir = os.path.dirname(__file__) 
    glassdoor_avis_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','GLASSDOOR','AVI')

    glassdoor_avis = [os.path.normpath(f) for f in list_files_in_folder(glassdoor_avis_folder)]
    
    glassdoor_avis_jsons = []
    
    for path in glassdoor_avis:
        with open(path, 'r', encoding='utf-8') as file:
            glassdoor_avis_jsons.append(pd.json_normalize(json.load(file)))
    
    result = pd.concat(glassdoor_avis_jsons, axis=0, ignore_index=True)