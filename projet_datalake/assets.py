import os, shutil, json
import pandas as pd
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor

from dagster import asset, op, Output, OpExecutionContext, AssetSpec, multi_asset, get_dagster_logger, AssetIn, MaterializeResult, AssetOut
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
            review_data['Note'] = review.find('span', class_='value-title').get('title')
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
    result = {}
    with open(file_path, 'r', encoding='utf-8') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')

        # Extraire les détails
        topcard = soup.find('section', class_='topcard')

        if entreprise := topcard.find('a', class_='topcard__org-name-link'):
            result['Entreprise'] = entreprise.text.strip()   

        if poste := topcard.find('h1', class_='topcard__title'):
            result['Poste'] = poste.text.strip()  
        
        if location := topcard.find('span', class_='topcard__flavor--bullet'):
            result['Lieu'] = location.text.strip()  

        if posted_time := topcard.find('span', class_='topcard__flavor--metadata posted-time-ago__text'):
            result['Date'] = posted_time.text.strip()

        if num_applicants := topcard.find('figcaption', class_='num-applicants__caption'):
            result['Nombre_Candidats'] = num_applicants.text.strip()  

        if apply_link := topcard.find('a', class_='apply-button apply-button--link'):
            result['Url'] = apply_link.get('href')

        # Extraire la description
        description_section = soup.find('section', class_='description')
        if description_section:
            result['Description'] = description_section.find('div', class_='description__text description__text--rich').text.strip()  

        # Extraire les critéres de l'offers
        job_criteria_section = soup.find('ul', class_='job-criteria__list')

        if job_criteria_section:
            criteria_items = job_criteria_section.find_all('li', class_='job-criteria__item')
            for item in criteria_items:
                subheader = item.find('h3', class_='job-criteria__subheader').text.strip()
                criteria_text = [span.text.strip() for span in item.find_all('span', class_='job-criteria__text job-criteria__text--criteria')]
                result[subheader] = criteria_text

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
    
@asset(deps=[json_avis_glassdoor])
def avis_glassdoor(context):
    current_dir = os.path.dirname(__file__) 
    glassdoor_avis_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','GLASSDOOR','AVI')

    glassdoor_avis_paths = [os.path.normpath(f) for f in list_files_in_folder(glassdoor_avis_folder)]
    
    glassdoor_avis_jsons = []
    
    for path in glassdoor_avis_paths:
        with open(path, 'r', encoding='utf-8') as file:
            glassdoor_avis_jsons.append(pd.json_normalize(json.load(file)))
    
    return pd.concat(glassdoor_avis_jsons, axis=0, ignore_index=True)

@asset(deps=[json_societe_glassdoor])
def societe_glassdoor(context):
    current_dir = os.path.dirname(__file__) 
    glassdoor_societe_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','GLASSDOOR','SOC')

    glassdoor_societe_paths = [os.path.normpath(f) for f in list_files_in_folder(glassdoor_societe_folder)]
    
    glassdoor_societe_jsons = []
    
    for path in glassdoor_societe_paths:
        with open(path, 'r', encoding='utf-8') as file:
            glassdoor_societe_jsons.append(pd.json_normalize(json.load(file)))
    
    return pd.concat(glassdoor_societe_jsons, axis=0, ignore_index=True)

@asset(deps=[json_emplois_linkedin])
def emplois_linkedin(context):
    current_dir = os.path.dirname(__file__) 
    linkedin_emplois_folder = os.path.join(current_dir, '..','TD_DATALAKE','DATALAKE','2_CURATED_ZONE','LINKEDIN','EMP')

    linkedin_emplois_paths = [os.path.normpath(f) for f in list_files_in_folder(linkedin_emplois_folder)]
    
    linkedin_emplois_jsons = []
    
    for path in linkedin_emplois_paths:
        with open(path, 'r', encoding='utf-8') as file:
            linkedin_emplois_jsons.append(pd.json_normalize(json.load(file)))
    
    return pd.concat(linkedin_emplois_jsons, axis=0, ignore_index=True)

@op
def bind_dataframes(df1, df2, df1_columns, df2_columns, id_column_name):
    """
    Binds two dataframes based on the specified columns and returns the modified dataframes.

    Parameters:
    df1 (pd.DataFrame): The first dataframe.
    df2 (pd.DataFrame): The second dataframe.
    df1_columns (list): The columns in df1 to merge on.
    df2_columns (list): The columns in df2 to merge on.
    id_column_name (str): The name of the id column to be added.

    Returns:
    pd.DataFrame: The modified first dataframe.
    pd.DataFrame: The modified second dataframe.
    """
    # Add an id column to the second dataframe
    df2[id_column_name] = range(1, len(df2) + 1)

    # Merge the first dataframe with the second dataframe on the specified columns
    df1_merged = df1.merge(df2, left_on=df1_columns, right_on=df2_columns, how='left')

    # Select the required columns for the first dataframe
    df1_result = df1_merged[df1.columns.tolist() + [id_column_name]]

    # Select the required columns for the second dataframe
    df2_result = df2[[id_column_name] + df2_columns]

    return df1_result, df2_result

@asset(ins={"entreprise": AssetIn(key="societe_glassdoor")})
def dim_entreprise(entreprise: pd.DataFrame)-> pd.DataFrame:
    
    entreprise.columns = ['ID_Entreprise','Nom_Entreprise','Description','Site_Web','Siege_Social','Taille','Annee_Fondation','Type_Entreprise','Secteur','Revenu','Groupe','Nouveau_Nom_Entreprise']
    
    # Remplace le nom de l'entreprise par le nouveau quand disponible
    entreprise['Nom_Entreprise'] =  entreprise['Nouveau_Nom_Entreprise'].where(entreprise['Nouveau_Nom_Entreprise'].notna(), entreprise['Nom_Entreprise'])
    
    entreprise['Annee_Fondation'] = entreprise['Annee_Fondation'].str.replace('Inconnue','')
    
    # Suppression Groupe et Nouveau_Nom_Entreprise + Réorganisation colonnes
    return entreprise[['ID_Entreprise','Nom_Entreprise','Description','Site_Web','Siege_Social','Taille','Revenu','Annee_Fondation','Type_Entreprise','Secteur']]

@multi_asset(
    outs={
        'fait_avis_glassdoor' : AssetOut(),
        'jk_status_employe' : AssetOut(),
        'dim_texte_avis' : AssetOut(),
        'jk_recommandations' : AssetOut()
    },
    ins={"avis_glassdoor": AssetIn(key="avis_glassdoor")})
def assets_avis_glassdoor(context: OpExecutionContext, avis_glassdoor: pd.DataFrame):
        
    avis_glassdoor.columns = ['Origine','Id_Entreprise','Nom_Entreprise','Date_Timestamp','Titre','Note','Description_Employe','Anciennete_Employe','Recommandation','Point_De_Vue','Approbation_PDG','Avantages','Inconvenients','Conseils_Direction']
        
    # Sortir l'avis quand a date n'est pas renseignée (1 dans notre cas)
    avis_glassdoor = avis_glassdoor.dropna(subset=['Date_Timestamp'])
    
    avis_glassdoor['ID_Date_Publication'] = avis_glassdoor['Date_Timestamp'].str.split(' ').apply(lambda x: x[0])
    avis_glassdoor['Heure_Publication'] = avis_glassdoor['Date_Timestamp'].str.split(' ').apply(lambda x: x[1])
    
    dim_columns = ['Description_Employe','Anciennete_Employe']
    dim_infos_employe = avis_glassdoor[dim_columns]
    avis_glassdoor, dim_infos_employe = bind_dataframes(avis_glassdoor, dim_infos_employe, dim_columns, dim_columns, 'ID_Infos_Employe')
    
    dim_columns = ['Avantages','Inconvenients','Conseils_Direction']
    dim_texte_avis = avis_glassdoor[dim_columns].drop_duplicates()
    avis_glassdoor, dim_texte_avis = bind_dataframes(avis_glassdoor, dim_texte_avis, dim_columns, dim_columns, 'ID_Texte_Avis')
    
    jk_columns = ['Recommandation','Point_De_Vue','Approbation_PDG']
    junk_recommandations = avis_glassdoor[jk_columns].drop_duplicates()
    avis_glassdoor, junk_recommandations = bind_dataframes(avis_glassdoor, junk_recommandations, jk_columns, jk_columns, 'JK_Recommandations')

    
    fait_avis_glassdoor = avis_glassdoor[['Id_Entreprise','ID_Date_Publication','ID_Texte_Avis','ID_Infos_Employe','JK_Recommandations','Heure_Publication','Titre','Note']]

    return fait_avis_glassdoor, dim_infos_employe, dim_texte_avis, junk_recommandations

@asset(ins={"emplois": AssetIn(key="emplois_linkedin")})
def fait_emplois_linkedin(context: OpExecutionContext, emplois: pd.DataFrame):
    
    emplois.columns = ['Nom_Entreprise','Titre_Offre','Lieu','Anciennete_Publication','Nombre_Candidats','Url','Description_Offre','Niveau_Hierarchique','Type_Emplois','Fonction','Secteurs']
    
    emplois['Anciennete_Publication'] = emplois['Anciennete_Publication'].fillna('Inconnue')
    emplois['Nombre_Candidats'] = emplois['Nombre_Candidats'].fillna('Inconnue')
    emplois['Url'] = emplois['Url'].fillna('Inconnue')
    
    emplois['Niveau_Hierarchique'] = emplois['Niveau_Hierarchique'].apply(lambda x: x[0])
    emplois['Type_Emplois'] = emplois['Type_Emplois'].apply(lambda x: x[0])

    return emplois