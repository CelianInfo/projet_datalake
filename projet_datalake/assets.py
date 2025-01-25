import os, shutil, json
import pandas as pd
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor

from dagster import asset, op, Output, OpExecutionContext, AssetSpec, multi_asset, get_dagster_logger
from bs4 import BeautifulSoup

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
    file_paths = [os.path.join(folder_path, file) for file in files if '.gitkeep' not in file]

    return file_paths

@op
def copy_files(file_paths, destination_folder, max_workers=5):
    """
    Copies multiple files to a specified destination folder concurrently.

    Args:
        file_paths: A list of file paths to copy.
        destination_folder: The path to the destination folder.
        max_workers: The maximum number of threads to use for concurrent copying.
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder, exist_ok=True)  # Create the folder if it doesn't exist

    def copy_single_file(file_path):
        destination_file = os.path.join(destination_folder, os.path.basename(file_path))
        if not os.path.exists(destination_file):
            shutil.copy(file_path, destination_folder)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(copy_single_file, file_path) for file_path in file_paths]
        for future in futures:
            future.result()  # Wait for all futures to complete

@op
def parse_html_glassdoor_avis(file_path:str) -> list[dict]:
    
    logger = get_dagster_logger()

    with open(file_path, 'r', encoding='utf-8') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')

        file_name = file_path[:-5].split('\\')[-1]
        id_entreprise = file_name.replace('_','-').split('-')[-2]

        nom_entreprise = soup.find('div', class_='header cell info').text

        # Extraction des avis
        employeeReviews = soup.findAll('li', class_='empReview')

        result = [] # contient une liste d'avis
        for review in employeeReviews:
            review_data = {}

            review_data['Origine'] = 'avis_glassdoor'
            review_data['Id_Entreprise'] = id_entreprise
            review_data['Nom_Entreprise'] = nom_entreprise
            if review_date := review.find('time'):
                review_date_value = review_date.get('datetime').split(' GMT')[0]
                review_data['Date'] = datetime.strptime(review_date_value, "%a %b %d %Y %H:%M:%S").strftime("%y/%m/%d %H:%M:%S")
            review_data['Titre'] = review.find('a', class_='reviewLink').find('span').text.strip()[2:-2]
            review_data['Note'] = float(review.find('span', class_='value-title').get('title'))
            review_data['Description_Employe'] = review.find('span', class_='authorJobTitle middle reviewer').text.strip()
            review_data['Anciennete_Employe'] = review.find('p', class_='mainText').text
            
            review_data['Recommandation'] = 'Non indiqué'
            review_data['Point_De_Vue'] = 'Non indiqué'
            review_data['Approbation_PDG'] = 'Non indiqué'
            
            def association_review(dictionnaire, texte):
                if texte == 'Recommande':
                    review_data['Recommandation'] = texte
                elif texte == 'Ne recommande pas':
                    review_data['Recommandation'] = texte
                    
                elif texte == 'Point de vue positif':
                    review_data['Point_De_Vue'] = texte
                elif texte == 'Point de vue neutre':
                    review_data['Point_De_Vue'] = texte
                elif texte == 'Point de vue négatif':
                    review_data['Point_De_Vue'] = texte
                    
                elif texte == 'Approuve le PDG':
                    review_data['Approbation_PDG'] = texte
                elif texte == "Pas d'avis sur le PDG":
                    review_data['Approbation_PDG'] = texte
                elif texte == "N'approuve pas le PDG":
                    review_data['Approbation_PDG'] = texte
                else:
                    logger.info(texte)
            
            if review.find('div', class_='row reviewBodyCell recommends'):
                if elements := review.find('div', class_='row reviewBodyCell recommends').findAll('span'):
                    for element in elements:
                        association_review(review_data, element.text)
                        
            if review.find('div', class_='mt-md'):
                for review_element in review.findAll('div', class_='mt-md'):
                    element = review_element.findAll('p')

                    dict_review_element = {
                        'Avantages':'Avantages',
                        'Inconv\u00e9nients':'Inconvenients',
                        'Conseils \u00e0 la direction':'Conseils a la direction'
                    }

                    review_data[dict_review_element[element[0].text]] = element[1].text

            result.append(review_data)

    return result

@op
def parse_html_glassdoor_societe(file_path):
    with open(file_path, 'r', encoding='utf-8') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
        
    # Initialisation du dictionnaire pour stocker les résultats
        presentation = soup.find('div', id='EmpBasicInfo')

        presentation_elements = presentation.findAll('div', class_='infoEntity')

        result = {}

        file_name = file_path[:-5].split('\\')[-1]
        result['Id_Entreprise'] = file_name.replace('_','-').split('-')[-2]
        result['Nom_Entreprise'] = presentation.find('h2').text.replace('Présentation de','').strip()
        
        if description := presentation.find('div', class_='margTop empDescription'):
            result['Description'] = description.text
                    
        for element in presentation_elements:
            element_label = element.find('label').text.strip()
            element_valeur = element.find('span').text.strip()

            result[element_label] = element_valeur
            
    
    return result

@op
def parse_html_linkedin_offers(file_path):

    result = {'EMP': dict(), 'description': '', 'job_criteria': dict()}

    with open(file_path, 'r', encoding='utf-8') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')

        # Extraire les détails
        topcard = soup.find('section', class_='topcard')

        if entreprise := topcard.find('a', class_='topcard__org-name-link'):
            result['EMP']['entreprise'] = entreprise.text.strip()   

    
        
        if poste := topcard.find('h1', class_='topcard__title'):
            result['EMP']['poste'] = poste.text.strip()  
        
        if location := topcard.find('span', class_='topcard__flavor--bullet'):
            result['EMP']['location'] = location.text.strip()  

        if posted_time := topcard.find('span', class_='topcard__flavor--metadata posted-time-ago__text'):
            result['EMP']['posted_time'] = posted_time.text.strip()

        if num_applicants := topcard.find('figcaption', class_='num-applicants__caption'):
            result['EMP']['num_applicants'] = num_applicants.text.strip()  

        if apply_link := topcard.find('a', class_='apply-button apply-button--link'):
            result['EMP']['apply_link'] = apply_link.get('href')

        # Extraire la description
        description_section = soup.find('section', class_='description')
        if description_section:
            result['description'] = description_section.find('div', class_='description__text description__text--rich').text.strip()  

        # Extraire les critéres de l'offers
        job_criteria_section = soup.find('ul', class_='job-criteria__list')
        job_criteria = {}

        if job_criteria_section:
            criteria_items = job_criteria_section.find_all('li', class_='job-criteria__item')
            for item in criteria_items:
                subheader = item.find('h3', class_='job-criteria__subheader').text.strip()
                criteria_text = [span.text.strip() for span in item.find_all('span', class_='job-criteria__text job-criteria__text--criteria')]
                job_criteria[subheader] = criteria_text

        result['job_criteria'] = job_criteria

        return result

@asset
def processed_html_files(context) -> None:
    """
    Processes files based on their name.

    This asset copies files to different destination folders based on their names.
    """
    current_dir = os.path.dirname(__file__) 
    source_folder = os.path.join(current_dir,'..','TD_DATALAKE','DATALAKE','0_SOURCE_WEB')

    html_files_to_process = list_files_in_folder(source_folder)

    generic_path = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','1_LANDING_ZONE')
    
    glassdoor_avis = [f for f in html_files_to_process if 'GLASSDOOR' in f and 'AVIS' in f]
    glassdoor_info = [f for f in html_files_to_process if 'GLASSDOOR' in f and 'INFO' in f]
    linkedin_emp   = [f for f in html_files_to_process if 'LINKEDIN'  in f]
    
    copy_files(glassdoor_avis, os.path.join(generic_path, 'GLASSDOOR','AVI'))
    copy_files(glassdoor_info, os.path.join(generic_path, 'GLASSDOOR','SOC'))
    copy_files(linkedin_emp  , os.path.join(generic_path, 'LINKEDIN','EMP'))

@asset(deps=[processed_html_files])
def json_avis_glassdoor(context) -> None:
    current_dir = os.path.dirname(__file__) 
    source_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','1_LANDING_ZONE','GLASSDOOR','AVI')
    output_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','GLASSDOOR','AVI')

    file_path_list = list_files_in_folder(source_folder)

    for file_path in file_path_list:
        result = parse_html_glassdoor_avis(file_path)
        file_name = file_path[:-5].split('\\')[-1]

        output_file_path = os.path.join(output_folder, f"{file_name}.json")

        with open(output_file_path, "w", encoding='utf-8') as outfile:
            json.dump(result, outfile, ensure_ascii=False, indent=4)

@asset(deps=[processed_html_files])
def json_societe_glassdoor(context) -> None:
    current_dir = os.path.dirname(__file__) 
    source_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','1_LANDING_ZONE','GLASSDOOR','SOC')
    output_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','GLASSDOOR','SOC')

    file_path_list = list_files_in_folder(source_folder)

    for file_path in file_path_list:
        result = parse_html_glassdoor_societe(file_path)
        file_name = file_path[:-5].split('\\')[-1]

        output_file_path = os.path.join(output_folder, f"{file_name}.json")

        with open(output_file_path, "w", encoding='utf-8') as outfile:
            json.dump(result, outfile, ensure_ascii=False, indent=4)

@asset(deps=[processed_html_files])
def json_emplois_linkedin(context) -> None:
    current_dir = os.path.dirname(__file__) 
    source_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','1_LANDING_ZONE','LINKEDIN','EMP')
    output_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','LINKEDIN','EMP')

    file_path_list = list_files_in_folder(source_folder)

    for file_path in file_path_list:
        result = parse_html_linkedin_offers(file_path)
        file_name = file_path[:-5].split('\\')[-1]

        output_file_path = os.path.join(output_folder, f"{file_name}.json")

        with open(output_file_path, "w", encoding='utf-8') as outfile:
            json.dump(result, outfile, ensure_ascii=False, indent=4)

# @multi_asset(specs=[
#     AssetSpec("avis_glassdoor", deps=[json_avis_glassdoor]), 
#     AssetSpec("avis_glassdoor_contenu",deps=[json_avis_glassdoor])
# ])
# def tables_avis_glassdoor(context):

#     current_dir = os.path.dirname(__file__) 
#     source_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','LINKEDIN','EMP')

#     file_path_list = list_files_in_folder(source_folder)

#     df_avis = pd.json_normalize(liste_avis)

#     return df_avis


# @asset
# def create_metadata_table():
#     # Créer la table sous duckdb
#     pass


# @asset(deps=[json_avis_glassdoor, create_metadata_table])
# def metadata_glassdoor():
#     pass