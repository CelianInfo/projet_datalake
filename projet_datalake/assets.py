from dagster import asset, op, Output, OpExecutionContext
import os
import shutil
from bs4 import BeautifulSoup
from collections import defaultdict
import json

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
def parse_html_glassdoor_avis(file_path:str) -> dict:
    result = {'societe':dict(),'stats':dict(),'avis':list()}

    with open(file_path, 'r', encoding='utf-8') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')

        # Extraction du haut de page

        result['societe']['nom'] = soup.find('div', class_='header cell info').text

        compagnieHeader = soup.find('div', id='EIProductHeaders')

        result['societe']['nb_avis'] = int(compagnieHeader
                .find('a', class_='eiCell cell reviews active')
                .find('span', class_='num h2')
                .text
                .strip())
        
        result['societe']['nb_emplois'] = int(compagnieHeader
                .find('a', class_='eiCell cell jobs')
                .find('span', class_='num h2')
                .text
                .strip())
        
        result['societe']['nb_salaires'] = int(compagnieHeader
                .find('a', class_='eiCell cell salaries')
                .find('span', class_='num h2')
                .text
                .strip())
        
        result['societe']['nb_entretiens'] = int(compagnieHeader
                .find('a', class_='eiCell cell interviews')
                .find('span', class_='num h2')
                .text
                .strip())
        
        result['societe']['nb_avantages'] = int(compagnieHeader
                .find('a', class_='eiCell cell benefits')
                .find('span', class_='num h2')
                .text
                .strip())
        
        result['societe']['nb_photos'] = int(compagnieHeader
                .find('a', class_='eiCell cell photos')
                .find('span', class_='num h2')
                .text
                .strip())

        # Extraction du tableau des notations utilisateurs

        statsBody = soup.find('div', class_='empStatsBody')

        result['stats']['notation_employes'] = float(statsBody.find('div', class_='v2__EIReviewsRatingsStylesV2__ratingNum v2__EIReviewsRatingsStylesV2__large').text)
        result['stats']['pourc_recommandation'] = int(statsBody.find('div', id='EmpStats_Recommend').get('data-percentage'))
        result['stats']['pourc_approbation'] = int(statsBody.find('div', id='EmpStats_Approve').get('data-percentage'))
        
        fondateur = statsBody.find('div', class_='donut-text d-lg-table-cell pt-sm pt-lg-0 pl-lg-sm').find('div').text.strip()

        nb_eval_fondateur_txt = statsBody.find('div', class_='numCEORatings').text
        nb_eval_fondateur = int(''.join(c for c in nb_eval_fondateur_txt if c.isdigit()))

        # Extraction des avis

        employeeReviews = soup.findAll('li', class_='empReview')

        reviews_data = result['avis']
        for review in employeeReviews:
            review_data = {}

            review_data['note'] = float(review.find('span', class_='value-title').get('title'))
            review_data['titre'] = review.find('a', class_='reviewLink').find('span').text.strip()[2:-2]

            description_employe = review.find('span', class_='authorJobTitle middle reviewer').text.split('-')
            review_data['employe_status'] = description_employe[0].strip()
            review_data['employe_poste'] = description_employe[1].strip()


            review_data['recommandations'] = defaultdict(list) 
            if review.find('div', class_='row reviewBodyCell recommends'):
                for color in ('green', 'yellow', 'red'):
                    for element in review.find('div', class_='row reviewBodyCell recommends').findAll('i', class_=color):
                        review_data['recommandations'][color].append(element.parent.find('span').text)

            review_data['anciennete'] = review.find('p', class_='mainText').text

            review_data['review_body'] = defaultdict(list) 
            if review.find('div', class_='mt-md'):
                for review_element in review.findAll('div', class_='mt-md'):
                    element = review_element.findAll('p')
                    review_data['review_body'][element[0].text] = element[1].text
        
            reviews_data.append(review_data)


    return result

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

@asset
def json_avis_glassdoor(context):
    current_dir = os.path.dirname(__file__) 
    source_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','1_LANDING_ZONE','GLASSDOOR','AVI')
    output_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','GLASSDOOR','AVI')

    file_path_list = list_files_in_folder(source_folder)

    for file_path in file_path_list:
        result = parse_html_glassdoor_avis(file_path)
        file_name = file_path[:-5]
        with open(f"{output_folder}/{file_name}.json", "w") as outfile:
            json.dump(result, outfile, indent=4)