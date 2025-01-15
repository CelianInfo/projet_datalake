from dagster import asset, op, Output, OpExecutionContext
import os
import shutil
from bs4 import BeautifulSoup
from collections import defaultdict
import json

import hashlib

def hash_values_sha256(*args):
    # Convertit les valeurs en bytes pour le hachage
    values_bytes = str(args).encode('utf-8')
    # Crée un objet de hachage SHA-256
    m = hashlib.sha256()
    m.update(values_bytes)
    return m.hexdigest()

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
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder, exist_ok=True)  # Create the folder if it doesn't exist

    destination_file = os.path.join(destination_folder, os.path.basename(file_path))
    if not os.path.exists(destination_file):
        shutil.copy(file_path, destination_folder)

@op
def parse_html_glassdoor_avis(file_path:str) -> list[dict]:
    result = [] # contient une liste d'avis

    with open(file_path, 'r', encoding='utf-8') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')

        file_name = file_path[:-5].split('\\')[-1]
        id_societe = file_name.replace('_','-').split('-')[-2]

        # Extraction du haut de page

        # result['nom'] = soup.find('div', class_='header cell info').text   

        # Extraction du tableau des notations utilisateurs

        # statsBody = soup.find('div', class_='empStatsBody')

        # if element := statsBody.find('div', class_='v2__EIReviewsRatingsStylesV2__ratingNum v2__EIReviewsRatingsStylesV2__large'):
        #     result['stats']['notation_employes'] = float(element.text)

        # if element := statsBody.find('div', id='EmpStats_Recommend'):
        #     result['stats']['pourc_recommandation'] = int(element.get('data-percentage'))

        # if element := statsBody.find('div', id='EmpStats_Approve'):
        #     result['stats']['pourc_approbation'] = int(element.get('data-percentage'))
        
        # fondateur = statsBody.find('div', class_='donut-text d-lg-table-cell pt-sm pt-lg-0 pl-lg-sm').find('div').text.strip()

        # nb_eval_fondateur_txt = statsBody.find('div', class_='numCEORatings').text
        # nb_eval_fondateur = int(''.join(c for c in nb_eval_fondateur_txt if c.isdigit()))

        # Extraction des avis

        employeeReviews = soup.findAll('li', class_='empReview')

        for review in employeeReviews:
            review_data = {}

            review_data['object'] = 'avis_glassdoor'

            review_data['id_societe'] = id_societe

            review_data['note'] = float(review.find('span', class_='value-title').get('title'))
            review_data['titre'] = review.find('a', class_='reviewLink').find('span').text.strip()[2:-2]

            description_employe = review.find('span', class_='authorJobTitle middle reviewer').text
            review_data['description_employe'] = description_employe.strip()

            if review.find('div', class_='row reviewBodyCell recommends'):
                for color in ('green', 'yellow', 'red'):
                    review_data[f'{color}_recommandations'] = []
                    for element in review.find('div', class_='row reviewBodyCell recommends').findAll('i', class_=color):
                        review_data[f'{color}_recommandations'].append(element.parent.find('span').text)

            review_data['anciennete'] = review.find('p', class_='mainText').text

            review_data['Avantages'] = ""
            review_data['Inconvenients'] = ""
            review_data['Conseils a la direction'] = ""

            if review.find('div', class_='mt-md'):
                for review_element in review.findAll('div', class_='mt-md'):
                    element = review_element.findAll('p')

                    dict_review_element = {
                        'Avantages':'Avantages',
                        'Inconv\u00e9nients':'Inconvenients',
                        'Conseils \u00e0 la direction':'Conseils a la direction'
                    }

                    review_key = dict_review_element[element[0].text]

                    review_data[review_key] = element[1].text

            review_data['unique_key'] = hash_values_sha256(review_data['id_societe'],review_data['note'],review_data['titre'])
        
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
        result['id_societe'] = file_name.replace('_','-').split('-')[-2]

        
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

        # Extraire la déscription
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
def processed_html_files(context):
    """
    Processes files based on their name.

    This asset copies files to different destination folders based on their names.
    """
    current_dir = os.path.dirname(__file__) 
    source_folder = os.path.join(current_dir,'..','TD_DATALAKE','DATALAKE','0_SOURCE_WEB')

    html_files_to_process = list_files_in_folder(source_folder)

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

@asset(deps=[processed_html_files])
def json_avis_glassdoor(context):
    current_dir = os.path.dirname(__file__) 
    source_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','1_LANDING_ZONE','GLASSDOOR','AVI')
    output_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','GLASSDOOR','AVI')

    file_path_list = list_files_in_folder(source_folder)

    for file_path in file_path_list:
        result = parse_html_glassdoor_avis(file_path)
        file_name = file_path[:-5].split('\\')[-1]

        output_file_path = os.path.join(output_folder, f"{file_name}.json")

        with open(output_file_path, "w") as outfile:
            json.dump(result, outfile, indent=4)
    
    return None

@asset(deps=[processed_html_files])
def json_societe_glassdoor(context):
    current_dir = os.path.dirname(__file__) 
    source_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','1_LANDING_ZONE','GLASSDOOR','SOC')
    output_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','GLASSDOOR','SOC')

    file_path_list = list_files_in_folder(source_folder)

    for file_path in file_path_list:
        result = parse_html_glassdoor_societe(file_path)
        file_name = file_path[:-5].split('\\')[-1]

        output_file_path = os.path.join(output_folder, f"{file_name}.json")

        with open(output_file_path, "w") as outfile:
            json.dump(result, outfile, indent=4)
    
    return None

@asset(deps=[processed_html_files])
def json_emplois_linkedin(context):
    current_dir = os.path.dirname(__file__) 
    source_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','1_LANDING_ZONE','LINKEDIN','EMP')
    output_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','LINKEDIN','EMP')

    file_path_list = list_files_in_folder(source_folder)

    for file_path in file_path_list:
        result = parse_html_linkedin_offers(file_path)
        file_name = file_path[:-5].split('\\')[-1]

        output_file_path = os.path.join(output_folder, f"{file_name}.json")

        with open(output_file_path, "w") as outfile:
            json.dump(result, outfile, indent=4)
    
    return None

@asset
def create_metadata_table():
    # Créer la table sous duckdb
    pass


@asset(deps=[json_avis_glassdoor, create_metadata_table])
def metadata_glassdoor():
    pass