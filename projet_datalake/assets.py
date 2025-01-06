from dagster import asset, op, Output, OpExecutionContext
import os
import shutil
from bs4 import BeautifulSoup
# soup = BeautifulSoup(html_doc, 'html.parser')

@op
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
    file_paths = [os.path.join(folder_path, file) for file in files]
    return file_paths

@op
def copy_file(file_path, destination_folder):
    """
    Copies a single file to a specified destination folder.

    Args:
        context: The Dagster op context.
        file_path: The path to the file to copy.
        destination_folder: The path to the destination folder.
    """
    destination_file = os.path.join(destination_folder, os.path.basename(file_path))
    if not os.path.exists(destination_file):
        shutil.copy(file_path, destination_folder)

@op
def parse_html_glassdoor_avis(file_paths: list[str]) -> list[dict]:
    """
    Parses HTML files and extracts the desired element.

    Args:
        file_paths: A list of paths to HTML files.

    Returns:
        A list of dictionaries, where each dictionary represents the extracted 
        element from a single HTML file.
    """
    data = []
    for file_path in file_paths:
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
            # Replace this with your actual element selection logic
            element = soup.find('h1').text 
            data.append({'file': file_path, 'element': element})
    return data

@asset
def html_files_to_process(context):
    """
    Lists all files in the source folder.

    This asset represents the list of files that need to be processed.
    """
    current_dir = os.path.dirname(__file__) 
    source_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','0_SOURCE_WEB')
    return list_files_in_folder(source_folder)

@asset
def processed_html_files(context, html_files_to_process):
    """
    Processes files based on their name.

    This asset copies files to different destination folders based on their names.
    """
    current_dir = os.path.dirname(__file__) 
    generic_path = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','1_LANDING_ZONE')



    for file_path in html_files_to_process:
        if "GLASSDOOR" in file_path:
            if "AVIS" in file_path:
                destination_folder = os.path.join(generic_path, 'GLASSDOOR','AVI')
            if "INFO" in file_path:
                destination_folder = os.path.join(generic_path, 'GLASSDOOR','SOC')
        
        if "LINKEDIN" in file_path:
            destination_folder = os.path.join(generic_path, 'LINKEDIN','EMP')

        copy_file(file_path, destination_folder) 

    return None

