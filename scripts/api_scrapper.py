import os
import re
import json
import requests
import logging
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configuration
REGEX_TITLE_NORMALIZATION = r"(?:\(.*?\)|\s*(Free Download|v\d+(\.\d+)*[a-zA-Z0-9\-]*|Build \d+|P2P|GOG|Repack|Edition.*|FLT|TENOKE)\s*)"
FILTRED_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'filtred.json')
API_DIR = os.path.join(os.path.dirname(__file__), '..', 'api')
OUTPUT_PATH = os.path.join(API_DIR, 'gamemetadata.json')
LOG_PATH = os.path.join(os.path.dirname(__file__), '..', 'logs', 'api_scrapper.log')
MAX_WORKERS = 10
LIMIT = 20

# Setup Logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_PATH),
                        logging.StreamHandler()
                    ])

os.makedirs(API_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

def normalize_title(title):
    return re.sub(REGEX_TITLE_NORMALIZATION, '', title, flags=re.IGNORECASE).strip()

def get_image_from_repacklink(session, url):
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        div = soup.find('div', class_='media-single-content')
        if div:
            img = div.find('img')
            if img and img.get('src'):
                logging.info(f'Successfully scraped image from {url}')
                return img['src']
        logging.warning(f'Image not found on {url}')
    except requests.exceptions.RequestException as e:
        status_code = e.response.status_code if e.response else 'N/A'
        logging.error(f'Error scraping {url} (Status: {status_code}): {e}')
    return None

def process_entry(session, entry):
    title = entry.get('title', '')
    repacklink = entry.get('repackLinkSource', '')
    normalized = normalize_title(title)
    image = get_image_from_repacklink(session, repacklink) if repacklink else None
    return {
        'Title': title,
        'NormalizedTitle': normalized,
        'image': image,
        'repackLinkSource': repacklink
    }

def scrap_repacklinks():
    # Carregar dados existentes
    existing_data = []
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, 'r', encoding='utf-8') as f:
            try:
                existing_data = json.load(f)
            except json.JSONDecodeError:
                logging.warning(f'{OUTPUT_PATH} is empty or corrupted. Starting fresh.')

    existing_titles = {normalize_title(entry.get('Title', '')) for entry in existing_data if entry.get('image')}

    with open(FILTRED_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    all_downloads = data.get('downloads', [])
    
    # Filtrar jogos que já possuem imagem e aplicar o limite
    downloads_to_process = [entry for entry in all_downloads if normalize_title(entry.get('title', '')) not in existing_titles]
    
    if LIMIT > 0:
        downloads_to_process = downloads_to_process[:LIMIT]

    if not downloads_to_process:
        logging.info("No new games to process.")
        return existing_data

    results = []
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    with requests.Session() as session:
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_entry, session, entry) for entry in downloads_to_process]
            for future in tqdm(as_completed(futures), total=len(downloads_to_process), desc="Scraping images"):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logging.error(f'Error processing entry: {e}')

    # Combinar resultados novos com os existentes
    final_results = existing_data + results

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(final_results, f, ensure_ascii=False, indent=2)
    
    logging.info(f'Scraping complete. {len(results)} new entries processed. Total entries: {len(final_results)}')
    return final_results

if __name__ == "__main__":
    scrap_repacklinks()