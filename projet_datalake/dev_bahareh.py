from bs4 import BeautifulSoup
from collections import defaultdict
from pprint import pprint
import json

file_path = "C:/Users/bmadahian1/Documents/GitHub/66/projet_datalake/TD_DATALAKE/DATALAKE/0_SOURCE_WEB/13554-INFO-SOC-GLASSDOOR-E855284_P1.html"


with open(file_path, 'r', encoding='utf-8') as html_file:
    soup = BeautifulSoup(html_file, 'html.parser')
    
# Initialisation du dictionnaire pour stocker les résultats
    presentation = soup.find('div', id='EmpBasicInfo')

    presentation_elements = presentation.findAll('div', class_='infoEntity')

    for element in presentation_elements:
        element_label = element.find('label').text.strip()
        element_valeur = element.find('span').text.strip()
        print(element_label, " ---- ", element_valeur)

    # print(presentation_elements)