from bs4 import BeautifulSoup
from collections import defaultdict
from pprint import pprint
import json

file_path = "C:/Users/ctoureille/Desktop/projet_datalake/TD_DATALAKE/DATALAKE/1_LANDING_ZONE/GLASSDOOR/SOC/13546-INFO-SOC-GLASSDOOR-E12966_P1.html"

def parse_html_glassdoor_societe(file_path):
    with open(file_path, 'r', encoding='utf-8') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
        
    # Initialisation du dictionnaire pour stocker les résultats
        presentation = soup.find('div', id='EmpBasicInfo')

        presentation_elements = presentation.findAll('div', class_='infoEntity')

        result = {}
        for element in presentation_elements:
            element_label = element.find('label').text.strip()
            element_valeur = element.find('span').text.strip()

            result[element_label] = element_valeur
    
    return result

print(parse_html_glassdoor_societe(file_path))