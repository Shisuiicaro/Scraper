import json
import random
import re
from datetime import datetime
from colorama import Fore, init
import httpx  # Para requisições HTTP
from bs4 import BeautifulSoup  # Corrigido para importar de bs4
import asyncio
from concurrent.futures import ThreadPoolExecutor  # Import necessário para ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from queue import Queue
from typing import List, Tuple  # Adicionado para corrigir o erro de tipagem
import subprocess  # Adicionado para executar comandos do sistema

init(autoreset=True)

SOURCE_JSON = "source.json"
BLACKLIST_JSON = "blacklist.json"
SHISUY_SOURCE_JSON = "shisuyssource.json"
REGEX_TITLE_NORMALIZATION = r"\s*\(.*?\)"  # Remove sufixos como "(Multiplayer)", "(VR)", etc.

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
}

def normalize_title(title):
    """Normaliza o título removendo sufixos e espaços extras."""
    return re.sub(REGEX_TITLE_NORMALIZATION, "", title).strip().lower()

def load_json(filename):
    """Carrega um arquivo JSON."""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"downloads": []}

def save_json(filename, data):
    """Salva dados em um arquivo JSON."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def is_valid_link(link):
    """Verifica se o link é válido."""
    return any(domain in link for domain in ["1fichier.com", "pixeldrain.com", "mediafire.com", "datanodes.to", "qiwi.gg"])

async def is_valid_qiwi_link(link, client):
    """Verifica se o link do Qiwi é válido e extrai o tamanho do arquivo."""
    try:
        response = await client.get(link, timeout=10)
        if response.status_code != 200:  # Verifica se o status HTTP é válido
            print(f"{Fore.RED}[INVALID LINK] Qiwi link {link} returned status {response.status_code}")
            return False, None

        soup = BeautifulSoup(response.text, 'html.parser')

        # Verificar se o nome do arquivo contém "TRNT.rar" ou ".torrent"
        title_element = soup.find('h1', class_='page_TextHeading__VsM7r')
        if title_element:
            file_name = title_element.get_text(strip=True)
            if "TRNT.rar" in file_name or ".torrent" in file_name:
                print(f"{Fore.RED}[INVALID LINK] Qiwi link {link} contains a torrent file ({file_name})")
                return False, None

        # Extrair o tamanho do arquivo
        size_element = soup.find(string=re.compile(r"Download\s+\d+(\.\d+)?\s*(GB|MB)", re.IGNORECASE))
        if size_element:
            file_size = size_element.strip().replace("Download ", "")
            print(f"{Fore.GREEN}[VALID LINK] Qiwi link {link} with file size {file_size}")
            return True, file_size

        print(f"{Fore.RED}[INVALID LINK] Qiwi link {link} does not contain file size information")
        return False, None
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Failed to validate Qiwi link {link}: {e}")
        return False, None

async def is_valid_datanodes_link(link, client):
    """Verifica se o link do Datanodes é válido e extrai o tamanho do arquivo."""
    try:
        response = await client.get(link, timeout=10)
        if response.status_code != 200:  # Verifica se o status HTTP é válido
            print(f"{Fore.RED}[INVALID LINK] Datanodes link {link} returned status {response.status_code}")
            return False, None

        soup = BeautifulSoup(response.text, 'html.parser')

        # Verificar se o nome do arquivo contém "TRNT.rar" ou ".torrent"
        title_element = soup.find('span', class_='block truncate w-auto')
        if title_element:
            file_name = title_element.get_text(strip=True)
            if "TRNT.rar" in file_name or ".torrent" in file_name:
                print(f"{Fore.RED}[INVALID LINK] Datanodes link {link} contains a torrent file ({file_name})")
                return False, None

        # Extrair o tamanho do arquivo
        size_element = soup.find('small', class_='m-0 text-xs text-gray-500 font-bold')
        if size_element:
            file_size = size_element.get_text(strip=True)  # Corrigido o parêntese
            print(f"{Fore.GREEN}[VALID LINK] Datanodes link {link} with file size {file_size}")
            return True, file_size

        print(f"{Fore.RED}[INVALID LINK] Datanodes link {link} does not contain file size information")
        return False, None
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Failed to validate Datanodes link {link}: {e}")
        return False, None

async def is_valid_pixeldrain_link(link, client):
    """Verifica se o link do Pixeldrain é válido e extrai o tamanho do arquivo usando a API."""
    try:
        # Extrair o file_id do link
        file_id = link.split("/")[-1]
        api_url = f"https://pixeldrain.com/api/file/{file_id}/info"

        # Fazer a requisição para a API do Pixeldrain
        response = await client.get(api_url, timeout=10)
        if response.status_code != 200:  # Verifica se o status HTTP é válido
            print(f"{Fore.RED}[INVALID LINK] Pixeldrain API for {link} returned status {response.status_code}")
            return False, None

        # Parsear a resposta JSON
        file_info = response.json()
        if file_info.get("success") is not True:
            print(f"{Fore.RED}[INVALID LINK] Pixeldrain API for {link} returned an error")
            return False, None

        # Verificar se o nome do arquivo contém "TRNT.rar", ".torrent" ou "bittorrent"
        file_name = file_info.get("name", "").lower()
        if any(indicator in file_name for indicator in ["trnt.rar", ".torrent", "bittorrent"]):
            print(f"{Fore.RED}[INVALID LINK] Pixeldrain link {link} contains a torrent indicator ({file_name})")
            return False, None

        # Extrair o tamanho do arquivo
        file_size_bytes = file_info.get("size", 0)
        if file_size_bytes > 0:
            # Converter o tamanho do arquivo para MB ou GB
            file_size = f"{file_size_bytes / (1024 ** 2):.2f} MB" if file_size_bytes < (1024 ** 3) else f"{file_size_bytes / (1024 ** 3):.2f} GB"
            print(f"{Fore.GREEN}[VALID LINK] Pixeldrain link {link} with size {file_size}")
            return True, file_size

        print(f"{Fore.RED}[INVALID LINK] Pixeldrain link {link} does not contain valid size information")
        return False, None
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Failed to validate Pixeldrain link {link}: {e}")
        return False, None

def extract_mediafire_key(url):
    """Extract the file key from a MediaFire URL."""
    try:
        # Handle both formats:
        # https://www.mediafire.com/file/KEY/filename/file
        # https://www.mediafire.com/file/KEY
        if "/file/" in url:
            parts = url.split("/file/")[1].split("/")
            return parts[0]
    except Exception:
        pass
    return None

def check_mediafire_link(link):
    """Verifica se o link do MediaFire é válido usando WebDriver."""
    driver = driver_pool.get_driver()
    try:
        driver.set_page_load_timeout(10)
        driver.get(link)

        # Verificar se houve redirecionamento para uma página de erro
        if "error.php" in driver.current_url:
            print(f"{Fore.RED}[INVALID LINK] MediaFire redirected to error page: {driver.current_url}")
            return None

        # Verificar se o título da página indica que o arquivo é inválido
        if "File sharing and storage made simple" in driver.title:
            print(f"{Fore.RED}[INVALID LINK] MediaFire: {link} (Invalid file)")
            return None

        print(f"{Fore.GREEN}[VALID LINK] MediaFire: {link}")
        return link
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Falha ao validar {link}: {e}")
        return None
    finally:
        driver_pool.return_driver(driver)

async def validate_mediafire_link(session, link):
    """Valida um link do MediaFire usando WebDriver e, em seguida, a API para obter informações."""
    # Primeiro, validar o link com WebDriver
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=3) as executor:
        result = await loop.run_in_executor(executor, check_mediafire_link, link)
        if not result:
            print(f"{Fore.RED}[INVALID] MediaFire: {link} (WebDriver validation failed)")
            return None, ""

    # Se o WebDriver validar, usar a API para obter informações
    quick_key = extract_mediafire_key(link)
    if not quick_key:
        print(f"{Fore.RED}[INVALID] MediaFire: {link} (Invalid quick key)")
        return None, ""

    api_url = f"https://www.mediafire.com/api/1.1/file/get_info.php?quick_key={quick_key}&response_format=json"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(api_url, headers=HEADERS)
            if response.status_code == 200:
                data = response.json()
                if data.get("response", {}).get("result") == "Success":
                    file_info = data["response"]["file_info"]
                    file_size = file_info.get("size", 0)
                    file_name = file_info.get("filename", "").lower()

                    # Verificar se o nome do arquivo contém ".torrent"
                    if ".torrent" in file_name:
                        print(f"{Fore.RED}[INVALID] MediaFire: {link} (File name contains '.torrent')")
                        return None, ""

                    formatted_size = format_size(int(file_size))
                    print(f"{Fore.GREEN}[VALID] MediaFire: {link} ({formatted_size})")
                    return link, formatted_size
                else:
                    print(f"{Fore.RED}[INVALID] MediaFire API failed for {link}")
            else:
                print(f"{Fore.RED}[ERROR] MediaFire API returned status {response.status_code} for {link}")
    except Exception as e:
        print(f"{Fore.RED}[ERROR] MediaFire API error for {link}: {e}")

    return None, ""

def format_size(size_in_bytes):
    """Formata o tamanho do arquivo em MB ou GB."""
    if size_in_bytes < 1024 ** 2:
        return f"{size_in_bytes / 1024:.2f} KB"
    elif size_in_bytes < 1024 ** 3:
        return f"{size_in_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_in_bytes / (1024 ** 3)::.2f} GB"

PROXY_API_URL = "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=protocolipport&format=text&anonymity=Elite,Anonymous&timeout=1019"

async def fetch_proxies() -> List[str]:
    """Obtém uma lista de proxies HTTP do formato texto retornado pela API."""
    retries = 3  # Número máximo de tentativas
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=10) as client:  # Timeout de 10 segundos
                response = await client.get(PROXY_API_URL)
                if response.status_code == 200:
                    proxies = response.text.strip().split("\n")
                    if proxies:
                        return [f"http://{proxy}" for proxy in proxies]  # Adicionar prefixo http://
                raise Exception("Failed to fetch proxies: No proxies in response")
        except (httpx.ReadTimeout, httpx.RequestError) as e:
            print(f"[WARNING] Attempt {attempt + 1} to fetch proxies failed: {e}")
            await asyncio.sleep(2 ** attempt)  # Backoff exponencial
    raise Exception("Failed to fetch proxies after multiple attempts")

async def validate_links(game, total_games, current_index):
    """Valida os links de um jogo e atualiza o tamanho do arquivo."""
    print(f"{Fore.BLUE}[PROGRESS] Validating game {current_index}/{total_games}")
    valid_links = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = []
        link_mapping = {}  # Mapeia índices para links para exibir logs corretamente
        for index, link in enumerate(game["uris"]):
            if "qiwi.gg" in link:
                tasks.append(is_valid_qiwi_link(link, client))
                link_mapping[len(tasks) - 1] = link
            elif "datanodes.to" in link:
                tasks.append(is_valid_datanodes_link(link, client))
                link_mapping[len(tasks) - 1] = link
            elif "pixeldrain.com" in link:
                tasks.append(is_valid_pixeldrain_link(link, client))
                link_mapping[len(tasks) - 1] = link
            elif "mediafire.com" in link:
                tasks.append(validate_mediafire_link(client, link))
                link_mapping[len(tasks) - 1] = link
            elif is_valid_link(link):  # Para outros links, apenas verifica o domínio
                valid_links.append(link)
        if tasks:
            results = await asyncio.gather(*tasks)
            for task_index, (is_valid, file_size) in enumerate(results):
                link = link_mapping[task_index]
                if is_valid:
                    print(f"{Fore.GREEN}[VALID LINK] {link}")
                    game["fileSize"] = file_size  # Atualiza o tamanho do arquivo
                    valid_links.append(link)
                else:
                    print(f"{Fore.RED}[INVALID LINK] {link}")
    # Atualiza links válidos
    game["uris"] = valid_links
    # Se único link é de 1fichier, remove e marca como inválido
    if len(valid_links) == 1 and "1fichier.com" in valid_links[0]:
        print(f"{Fore.RED}[INVALID LINK] {valid_links[0]} is the only download and is from 1fichier; marking game as invalid.")
        game["uris"] = []
    return game

def decide_game_to_keep(existing_game, new_game):
    """Decide qual jogo manter entre dois duplicados."""
    existing_links = [link for link in existing_game["uris"] if is_valid_link(link)]
    new_links = [link for link in new_game["uris"] if is_valid_link(link)]

    # Priorizar jogos com links válidos
    if not new_links and existing_links:
        return existing_game
    if not existing_links and new_links:
        return new_game

    # Priorizar jogos com versão online
    existing_is_online = "0xdeadcode" in existing_game["title"].lower() or "multiplayer" in existing_game["title"].lower()
    new_is_online = "0xdeadcode" in new_game["title"].lower() or "multiplayer" in new_game["title"].lower()

    if existing_is_online and not new_is_online:
        return existing_game
    if new_is_online and not existing_is_online:
        return new_game

    # Priorizar jogos mais novos
    existing_date = datetime.fromisoformat(existing_game["uploadDate"]) if existing_game.get("uploadDate") else None
    new_date = datetime.fromisoformat(new_game["uploadDate"]) if new_game.get("uploadDate") else None

    if existing_date and new_date:
        return new_game if new_date > existing_date else existing_game

    # Caso não haja critério claro, manter o existente
    return existing_game

async def process_duplicates(games):
    """Processa duplicatas e decide quais jogos manter."""
    grouped_games = {}
    for game in games:
        normalized_title = normalize_title(game["title"])
        if normalized_title not in grouped_games:
            grouped_games[normalized_title] = []
        grouped_games[normalized_title].append(game)

    valid_games = []
    removed_games = []

    total_games = len(games)  # Total de jogos para o progresso
    for current_index, (title, duplicates) in enumerate(grouped_games.items(), start=1):
        if len(duplicates) == 1:
            game = await validate_links(duplicates[0], total_games, current_index)  # Valida os links do jogo
            valid_games.append(game)
        else:
            # Decidir qual jogo manter
            best_game = duplicates[0]
            for game in duplicates[1:]:
                best_game = decide_game_to_keep(best_game, game)
            best_game = await validate_links(best_game, total_games, current_index)  # Valida os links do jogo escolhido
            valid_games.append(best_game)

            # Adicionar os jogos removidos à blacklist
            removed_games.extend([game for game in duplicates if game != best_game])

    return valid_games, removed_games

class DriverPool:
    """Gerencia um pool de WebDrivers para reutilização."""
    def __init__(self, size=3):
        self.pool = Queue(maxsize=size)
        for _ in range(size):
            driver = self._create_driver()
            self.pool.put(driver)
        
    def _create_driver(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.page_load_strategy = 'eager'
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=chrome_options)
    
    def get_driver(self):
        return self.pool.get()
        
    def return_driver(self, driver):
        self.pool.put(driver)
        
    def cleanup(self):
        while not self.pool.empty():
            driver = self.pool.get()
            driver.quit()

# Inicializar o pool de WebDrivers
driver_pool = DriverPool(size=3)

async def main():
    # Carregar o JSON original
    shisuy_data = load_json(SHISUY_SOURCE_JSON)
    games = shisuy_data.get("downloads", [])

    # Processar duplicatas
    valid_games, removed_games = await process_duplicates(games)

    # Salvar resultados
    save_json(SOURCE_JSON, {"downloads": valid_games})
    save_json(BLACKLIST_JSON, {"removed": removed_games})

    print(f"{Fore.GREEN}Processing complete!")
    print(f"{Fore.GREEN}Valid games saved to {SOURCE_JSON}")
    print(f"{Fore.YELLOW}Removed games saved to {BLACKLIST_JSON}")

if __name__ == "__main__":
    asyncio.run(main())
