from bs4 import BeautifulSoup

# files = os.listdir(folder_path)

file_path = "C:/Users/ctoureille/Desktop/projet_datalake/TD_DATALAKE/DATALAKE/1_LANDING_ZONE/GLASSDOOR/AVI/13546-AVIS-SOC-GLASSDOOR-E12966_P1.html"

with open(file_path, 'r', encoding='utf-8') as html_file:
    soup = BeautifulSoup(html_file, 'html.parser')

    # Extraction du haut de page

    compagnie_name = soup.find('div', class_='header cell info').text

    compagnieHeader = soup.find('div', id='EIProductHeaders')

    nb_avis = int(compagnieHeader
               .find('a', class_='eiCell cell reviews active')
               .find('span', class_='num h2')
               .text
               .strip())
    
    nb_emplois = int(compagnieHeader
               .find('a', class_='eiCell cell jobs')
               .find('span', class_='num h2')
               .text
               .strip())
    
    nb_salaires = int(compagnieHeader
               .find('a', class_='eiCell cell salaries')
               .find('span', class_='num h2')
               .text
               .strip())
    
    nb_entretiens = int(compagnieHeader
               .find('a', class_='eiCell cell interviews')
               .find('span', class_='num h2')
               .text
               .strip())
    
    nb_avantages = int(compagnieHeader
               .find('a', class_='eiCell cell benefits')
               .find('span', class_='num h2')
               .text
               .strip())
    
    nb_photos = int(compagnieHeader
               .find('a', class_='eiCell cell photos')
               .find('span', class_='num h2')
               .text
               .strip())

    # Extraction du tableau des notations utilisateurs

    statsBody = soup.find('div', class_='empStatsBody')

    employees_notation = float(statsBody.find('div', class_='v2__EIReviewsRatingsStylesV2__ratingNum v2__EIReviewsRatingsStylesV2__large').text)
    pourc_recommandation = int(statsBody.find('div', id='EmpStats_Recommend').get('data-percentage'))
    pourc_approbation = int(statsBody.find('div', id='EmpStats_Approve').get('data-percentage'))
    
    fondateur = statsBody.find('div', class_='donut-text d-lg-table-cell pt-sm pt-lg-0 pl-lg-sm').find('div').text.strip()

    nb_eval_fondateur_txt = statsBody.find('div', class_='numCEORatings').text
    nb_eval_fondateur = int(''.join(c for c in nb_eval_fondateur_txt if c.isdigit()))

    # Extraction des avis

    employeeReviews = soup.findAll('li', class_='empReview')

    reviews_data = []
    for review in employeeReviews:
        review_data = {}

        review_data['note'] = float(review.find('span', class_='value-title').get('title'))

        reviews_data.append(review_data)

    print(reviews_data)

    # print({
    #     'nb_avis' : nb_avis,
    #     'nb_emplois' : nb_emplois,
    #     'nb_salaires' : nb_salaires,
    #     'nb_entretiens' : nb_entretiens,
    #     'nb_avantages' : nb_avantages,
    #     'nb_photos' : nb_photos
    #     })
    