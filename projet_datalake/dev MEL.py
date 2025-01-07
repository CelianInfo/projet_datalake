from bs4 import BeautifulSoup
from collections import defaultdict
from pprint import pprint
import json

file_path = "C:/Users/ctoureille/Desktop/projet_datalake/TD_DATALAKE/DATALAKE/1_LANDING_ZONE/GLASSDOOR/AVI/13546-AVIS-SOC-GLASSDOOR-E12966_P1.html"

def parse_html_glassdoor_avis(file_path):

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

print(parse_html_glassdoor_avis(file_path))