from bs4 import BeautifulSoup
from collections import defaultdict
from pprint import pprint
import json

file_path = "C:/Users/ctoureille/Desktop/projet_datalake/TD_DATALAKE/DATALAKE/1_LANDING_ZONE/LINKEDIN/EMP/13546-INFO-EMP-LINKEDIN-FR-1599984246.html"

def parse_html_linkedin_offers(file_path):

    result = {'EMP': dict(), 'stats': dict(), 'avis': list(), 'description': '', 'job_criteria': dict()}

    with open(file_path, 'r', encoding='utf-8') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')

        # Extraction of top card details
        topcard = soup.find('section', class_='topcard')

        result['EMP']['nom'] = topcard.find('a', class_='topcard__logo-container').text.strip()   
        
        result['EMP']['poste'] = topcard.find('h1', class_='topcard__title').text.strip()  
        result['EMP']['location'] = topcard.find('span', class_='topcard__flavor--bullet').text.strip()  

        # Time posted and number of applicants
        topcard_flavor = topcard.find_all('h3', class_='topcard__flavor-row')
        result['EMP']['posted_time'] = topcard_flavor[1].find('span', class_='topcard__flavor--metadata').text.strip()  
        result['EMP']['num_applicants'] = topcard_flavor[1].find('figcaption', class_='num-applicants__caption').text.strip()  

        # Application link
        apply_link = topcard.find('div', class_='topcard__content-right').find('a', class_='apply-button--link')['href']
        result['EMP']['apply_link'] = apply_link

        # Extract the job description
        description_section = soup.find('section', class_='description')
        if description_section:
            result['description'] = description_section.find('div', class_='description__text description__text--rich').text.strip()  

        # Extracting job criteria
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

print(json.dumps(parse_html_linkedin_offers(file_path), indent=4))