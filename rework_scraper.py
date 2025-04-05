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
from stem import Signal
from stem.control import Controller
from httpx_socks import AsyncProxyTransport
import os
from time import time
import math
from datetime import timedelta

init(autoreset=True)

SOURCE_JSON = "source.json"
BLACKLIST_JSON = "blacklist.json"
SHISUY_SOURCE_JSON = "shisuyssource.json"
GOFILE_TIMEOUTS_JSON = "gofile_timeouts.json"
VALID_LINKS_JSON = "valid_links.json"
INVALID_LINKS_JSON = "invalid_links.json"
PROGRESS_JSON = "validation_progress.json"
# Updated title normalization regex to remove version/build info
REGEX_TITLE_NORMALIZATION = r"\s*\([^)]*(?:v\d+(?:\.\d+){1,}|Build \d+|R\d+\.\d+|Ch\.\s*\d+\s*v\d+(?:\.\d+)?|Executive Edition Free Download)[^)]*\)"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
}

def normalize_title(title):
    return re.sub(REGEX_TITLE_NORMALIZATION, "", title, flags=re.IGNORECASE).strip().lower()

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
    return any(domain in link for domain in ["1fichier.com", "gofile.io", "pixeldrain.com", "mediafire.com", "datanodes.to", "qiwi.gg"])

async def is_valid_qiwi_link(link, client):
    """Verifica se o link do Qiwi é válido e extrai o tamanho do arquivo."""
    try:
        response = await client.get(link, timeout=10)
        if response.status_code != 200:  # Verifica se o status HTTP é válido
            return False, None

        soup = BeautifulSoup(response.text, 'html.parser')

        # Verificar se o nome do arquivo contém "TRNT.rar" ou ".torrent"
        title_element = soup.find('h1', class_='page_TextHeading__VsM7r')
        if title_element:
            file_name = title_element.get_text(strip=True)
            if "TRNT.rar" in file_name or ".torrent" in file_name:
                return False, None

        # Extrair o tamanho do arquivo
        size_element = soup.find(string=re.compile(r"Download\s+\d+(\.\d+)?\s*(GB|MB)", re.IGNORECASE))
        if size_element:
            file_size = size_element.strip().replace("Download ", "")
            return True, file_size

        return False, None
    except Exception:
        return False, None

async def is_valid_datanodes_link(link, client):
    """Verifica se o link do Datanodes é válido e extrai o tamanho do arquivo."""
    try:
        response = await client.get(link, timeout=10)
        if response.status_code != 200:  # Verifica se o status HTTP é válido
            return False, None

        soup = BeautifulSoup(response.text, 'html.parser')

        # Verificar se o nome do arquivo contém "TRNT.rar" ou ".torrent"
        title_element = soup.find('span', class_='block truncate w-auto')
        if title_element:
            file_name = title_element.get_text(strip=True)
            if "TRNT.rar" in file_name or ".torrent" in file_name:
                return False, None

        # Extrair o tamanho do arquivo
        size_element = soup.find('small', class_='m-0 text-xs text-gray-500 font-bold')
        if size_element:
            file_size = size_element.get_text(strip=True)  # Corrigido o parêntese
            return True, file_size

        return False, None
    except Exception:
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
            return False, None

        # Parsear a resposta JSON
        file_info = response.json()
        if file_info.get("success") is not True:
            return False, None

        # Verificar se o nome do arquivo contém "TRNT.rar", ".torrent" ou "bittorrent"
        file_name = file_info.get("name", "").lower()
        if any(indicator in file_name for indicator in ["trnt.rar", ".torrent", "bittorrent"]):
            return False, None

        # Extrair o tamanho do arquivo
        file_size_bytes = file_info.get("size", 0)
        if file_size_bytes > 0:
            # Converter o tamanho do arquivo para MB ou GB
            file_size = f"{file_size_bytes / (1024 ** 2):.2f} MB" if file_size_bytes < (1024 ** 3) else f"{file_size_bytes / (1024 ** 3):.2f} GB"
            return True, file_size

        return False, None
    except Exception:
        return False, None

def extract_mediafire_key(url):
    """Extract the file key from a MediaFire URL.
    Handles extra '/file' at the end of the URL."""
    # Remove trailing '/file' (with or without a slash)
    url = re.sub(r'/file/?$', '', url)
    try:
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
            return None

        # Verificar se o título da página indica que o arquivo é inválido
        if "File sharing and storage made simple" in driver.title:
            return None

        return link
    except Exception:
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
            return None, ""

    # Se o WebDriver validar, usar a API para obter informações
    quick_key = extract_mediafire_key(link)
    if not quick_key:
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
                        return None, ""

                    formatted_size = format_size(int(file_size))
                    return link, formatted_size
                else:
                    return None, ""
            else:
                return None, ""
    except Exception:
        return None, ""

def format_size(size_in_bytes):
    """Formata o tamanho do arquivo em MB ou GB."""
    if size_in_bytes < 1024 ** 2:
        return f"{size_in_bytes / 1024:.2f} KB"
    elif size_in_bytes < 1024 ** 3:
        return f"{size_in_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_in_bytes / (1024 ** 3):.2f} GB"

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
        except (httpx.ReadTimeout, httpx.RequestError):
            await asyncio.sleep(2 ** attempt)  # Backoff exponencial
    raise Exception("Failed to fetch proxies after multiple attempts")

def fetch_page(scraper, url, retries=3):
    # Updated fetch_page logging with colorama
    for attempt in range(retries):
        try:
            response = scraper.get(url, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                return response.text
            print(f"{Fore.YELLOW}Attempt {attempt + 1} failed for {url} with status {response.status_code}")
        except Exception as e:
            print(f"{Fore.RED}Attempt {attempt + 1} failed for {url}: {str(e)}")
        asyncio.sleep(2 ** attempt)
    print(f"{Fore.RED}Failed to fetch {url} after {retries} retries")
    return None

async def validate_links(game, total_games, current_index):
    """Valida os links de um jogo e atualiza o tamanho do arquivo."""
    valid_links = []
    invalid_links = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = []
        link_mapping = {}  # Mapeia índices para links para exibir logs
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
            elif "gofile.io" in link:
                tasks.append(validate_gofile_link_api(link))
                link_mapping[len(tasks) - 1] = link
            elif is_valid_link(link):
                valid_links.append(link)
        if tasks:
            results = await asyncio.gather(*tasks)
            for task_index, (is_valid, file_size) in enumerate(results):
                link = link_mapping[task_index]
                if is_valid:
                    print(f"{Fore.GREEN}[VALID LINK] {link} - {file_size}")
                    game["fileSize"] = file_size
                    valid_links.append(link)
                    # Save valid link immediately
                    save_progress({**load_progress()[0], game["title"]: valid_links}, 
                                load_progress()[1], current_index)
                else:
                    print(f"{Fore.RED}[INVALID LINK] {link}")
                    invalid_links.append(link)
                    # Save invalid link immediately
                    save_progress(load_progress()[0],
                                {**load_progress()[1], game["title"]: invalid_links}, 
                                current_index)
                # Log progress
                print(f"{Fore.CYAN}Progress: Validated {task_index + 1}/{len(tasks)} links for game {current_index + 1}/{total_games}")
    game["uris"] = valid_links
    if len(valid_links) == 1 and "1fichier.com" in valid_links[0]:
        game["uris"] = []
    # Log game validation summary with correct count
    print(f"{Fore.BLUE}{current_index + 1}/{total_games} games validated")
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

    return existing_game

MAX_CONCURRENT_TASKS = 5
BATCH_SIZE = 10

class ProgressTracker:
    def __init__(self, total):
        self.total = total
        self.current = 0
        self.start_time = time()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    
    def update(self, amount=1):
        self.current += amount
        elapsed_time = time() - self.start_time
        items_per_second = self.current / elapsed_time if elapsed_time > 0 else 0
        remaining_items = self.total - self.current
        eta_seconds = remaining_items / items_per_second if items_per_second > 0 else 0
        eta = str(timedelta(seconds=math.ceil(eta_seconds)))
        
        percent = (self.current / self.total) * 100
        print(f"{Fore.CYAN}Progress: {percent:.1f}% ({self.current}/{self.total}) - ETA: {eta}")

async def process_duplicates(games):
    """Processa duplicatas com processamento em paralelo e tracking de progresso."""
    # Load previous progress
    valid_links_dict, invalid_links_dict, last_processed = load_progress()
    
    grouped_games = {}
    tracker = ProgressTracker(len(games))
    tracker.current = last_processed  # Resume from last position
    
    # Group games by normalized title
    for game in games:
        normalized_title = normalize_title(game["title"])
        if normalized_title not in grouped_games:
            grouped_games[normalized_title] = []
        grouped_games[normalized_title].append(game)

    valid_games = []
    removed_games = []
    
    async def process_game_group(group_games):
        nonlocal valid_games, removed_games
        async with tracker.semaphore:
            sorted_games = sorted(
                group_games,
                key=lambda g: datetime.fromisoformat(g.get("uploadDate", "1970-01-01T00:00:00")),
                reverse=True
            )
            
            # Skip already processed games
            game_title = normalize_title(sorted_games[0]["title"])
            if game_title in valid_links_dict:
                print(f"{Fore.CYAN}Skipping already processed game: {game_title}")
                if valid_links_dict[game_title]:  # If has valid links
                    valid_games.append(sorted_games[0])
                    removed_games.extend(sorted_games[1:])
                else:
                    removed_games.extend(sorted_games)
                return
                
            for game in sorted_games:
                validated = await validate_links(game, total_games=len(games), current_index=tracker.current)
                tracker.update()
                if validated["uris"]:
                    valid_games.append(validated)
                    removed_games.extend([g for g in group_games if g != validated])
                    break
            else:
                removed_games.extend(group_games)

    # Process groups in batches
    tasks = []
    for group in grouped_games.values():
        tasks.append(process_game_group(group))
        if len(tasks) >= BATCH_SIZE:
            await asyncio.gather(*tasks)
            tasks = []
    
    if tasks:
        await asyncio.gather(*tasks)

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

def rotate_tor_identity():
    """Solicita um novo ip ao Tor enviando o sinal NEWNYM."""
    try:
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()  # Ajuste se for necessário senha
            controller.signal(Signal.NEWNYM)
            print(f"{Fore.BLUE}[TOR] New tor identity issued.")
    except Exception:
        print(f"{Fore.RED}[TOR ERROR] Failed to rotate Tor identity")

async def validate_gofile_link_tor(link: str, retries: int = 3) -> Tuple[bool, str]:
    """Valida um link do Gofile usando Tor com IP rotativo.
    Usa BeautifulSoup para scraping da página para extrair o tamanho do arquivo (GB ou MB)
    e rejeita links com palavras indesejadas como 'torrent', 'this content does not exist' ou 'cold'."""
    proxy_url = "socks5://127.0.0.1:9050"
    transport = AsyncProxyTransport.from_url(proxy_url)
    attempt = 0
    while attempt < retries:
        try:
            rotate_tor_identity()
            async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
                response = await client.get(link, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    content_text = soup.get_text(separator=" ", strip=True).lower()
                    for bad in ["torrent", "this content does not exist", "cold"]:
                        if bad in content_text:
                            return False, ""
                    size_match = re.search(r"(\d+(?:\.\d+)?\s*(GB|MB))", content_text, re.IGNORECASE)
                    file_size = size_match.group(1) if size_match else ""
                    if not file_size:
                        return False, ""
                    return True, file_size
                else:
                    return False, ""
        except Exception:
            if "Proxy connection timed out" in str(e):
                await asyncio.sleep(2 ** attempt)
                attempt += 1
            else:
                return False, ""
    return False, ""

WT = "4fd6sg89d7s6"  # Constante para uso na API do Gofile
GOFILE_TOKEN = None

async def authorize_gofile():
    """Authorize with Gofile API and store the token globally."""
    global GOFILE_TOKEN
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post("https://api.gofile.io/accounts", headers=HEADERS)
        if response.status_code == 200 and response.json().get("status") == "ok":
            GOFILE_TOKEN = response.json()["data"]["token"]
            return GOFILE_TOKEN
        else:
            return ""
    except Exception:
        return ""

def save_gofile_timeout(link, error):
    data = {"timeouts": []}
    if os.path.exists(GOFILE_TIMEOUTS_JSON):
        try:
            with open(GOFILE_TIMEOUTS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {"timeouts": []}
    data["timeouts"].append({
        "link": link,
        "error": error,
        "timestamp": datetime.now().isoformat()
    })
    with open(GOFILE_TIMEOUTS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

async def cleanup_gofile_timeouts(max_age_hours=24):
    """Remove timeout entries older than max_age_hours."""
    if os.path.exists(GOFILE_TIMEOUTS_JSON):
        try:
            with open(GOFILE_TIMEOUTS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            now = datetime.now()
            data["timeouts"] = [
                timeout for timeout in data["timeouts"]
                if (now - datetime.fromisoformat(timeout["timestamp"])).total_seconds() < max_age_hours * 3600
            ]
            with open(GOFILE_TIMEOUTS_JSON, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"{Fore.RED}Error cleaning up timeouts: {str(e)}")

async def validate_gofile_link_api(link: str, retries: int = 3) -> Tuple[bool, str]:
    await cleanup_gofile_timeouts()
    m = re.search(r"gofile\.io/d/([^/?]+)", link)
    if not m:
        return False, ""
    
    file_id = m.group(1)
    api_url = f"https://api.gofile.io/contents/{file_id}?wt={WT}"
    
    if not GOFILE_TOKEN:
        await authorize_gofile()
    
    headers = {**HEADERS, "Authorization": f"Bearer {GOFILE_TOKEN}"}
    transport = AsyncProxyTransport.from_url("socks5://127.0.0.1:9050")
    last_error = ""
    
    # Add rate limiting delay
    await asyncio.sleep(1)
    
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=15, transport=transport) as client:
                response = await client.get(api_url, headers=headers)
                
            if response.status_code == 200:
                try:
                    data = response.json()
                    if not isinstance(data, dict):
                        last_error = "Invalid JSON response"
                        continue
                        
                    if data.get("status") == "ok":
                        content = data.get("data", {})
                        if isinstance(content, dict) and content.get("type") == "folder":
                            children = content.get("children", {})
                            if children:
                                first_child = next(iter(children.values()))
                                name = first_child.get("name", "").lower()
                                if any(bad in name for bad in ["torrent", "this content does not exist", "cold"]):
                                    return False, ""
                                    
                                size_bytes = first_child.get("size")
                                if size_bytes and str(size_bytes).isdigit():
                                    bytes_val = int(size_bytes)
                                    if bytes_val > 0:
                                        if bytes_val < 1024:
                                            size_str = f"{bytes_val} B"
                                        elif bytes_val < 1024 ** 2:
                                            size_str = f"{(bytes_val / 1024):.2f} KB"
                                        elif bytes_val < 1024 ** 3:
                                            size_str = f"{(bytes_val / (1024 ** 2)):.2f} MB"
                                        else:
                                            size_str = f"{(bytes_val / (1024 ** 3)):.2f} GB"
                                        return True, size_str
                
                except json.JSONDecodeError:
                    last_error = "Invalid JSON response"
                    continue
                    
            last_error = f"Status code {response.status_code}"
            
        except Exception as e:
            last_error = str(e)
            if "timed out" in last_error.lower():
                await asyncio.sleep(2 ** attempt)
                continue
                
        # Exponential backoff between retries
        if attempt < retries - 1:
            await asyncio.sleep(2 ** attempt)
            
    save_gofile_timeout(link, last_error)
    return False, ""

def log_game_status(status, page, game_title, error=""):
    # Updated log output with colorama stamps for all statuses.
    if status == "NEW":
        print(f"{Fore.GREEN}[VALID] Page {page}: {game_title} - New game added")
    elif status == "UPDATED":
        print(f"{Fore.YELLOW}[UPDATED] Page {page}: {game_title}")
    elif status == "IGNORED":
        print(f"{Fore.CYAN}[SKIPPED] Page {page}: {game_title} - Duplicate or ignored")
    elif status == "NO_LINKS":
        print(f"{Fore.RED}[INVALID] Page {page}: {game_title} - No links found")
    elif status == "ERROR":
        print(f"{Fore.MAGENTA}[ERROR] Page {page}: {game_title} - {error}")

def save_progress(valid_links, invalid_links, current_index):
    """Save validation progress to files."""
    try:
        with open(VALID_LINKS_JSON, "w", encoding="utf-8") as f:
            json.dump(valid_links, f, ensure_ascii=False, indent=4)
        with open(INVALID_LINKS_JSON, "w", encoding="utf-8") as f:
            json.dump(invalid_links, f, ensure_ascii=False, indent=4)
        with open(PROGRESS_JSON, "w", encoding="utf-8") as f:
            json.dump({"last_index": current_index}, f)
    except Exception as e:
        print(f"{Fore.RED}Error saving progress: {str(e)}")

def load_progress():
    """Load validation progress from files."""
    valid_links = {}
    invalid_links = {}
    last_index = 0
    
    try:
        if os.path.exists(VALID_LINKS_JSON):
            with open(VALID_LINKS_JSON, "r", encoding="utf-8") as f:
                valid_links = json.load(f)
        if os.path.exists(INVALID_LINKS_JSON):
            with open(INVALID_LINKS_JSON, "r", encoding="utf-8") as f:
                invalid_links = json.load(f)
        if os.path.exists(PROGRESS_JSON):
            with open(PROGRESS_JSON, "r", encoding="utf-8") as f:
                last_index = json.load(f)["last_index"]
    except Exception as e:
        print(f"{Fore.YELLOW}Warning loading progress: {str(e)}")
        
    return valid_links, invalid_links, last_index

async def main():
    # Carregar o JSON original
    shisuy_data = load_json(SHISUY_SOURCE_JSON)
    games = shisuy_data.get("downloads", [])

    # Processar duplicatas
    valid_games, removed_games = await process_duplicates(games)

    # Salvar resultados
    save_json(SOURCE_JSON, {"downloads": valid_games})
    save_json(BLACKLIST_JSON, {"removed": removed_games})
    total_valid = len(valid_games)
    total_removed = len(removed_games)
    print(f"Summary: Validated = {total_valid} games; Removed = {total_removed} games.")

if __name__ == "__main__":
    asyncio.run(main())
