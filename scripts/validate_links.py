import json
import re
import os
import json
import time
import logging
import asyncio
import signal
import sys
from datetime import datetime
from typing import List, Tuple, Set, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor

import httpx
from bs4 import BeautifulSoup
from colorama import Fore, Style, init
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Inicialização do colorama para formatação de cores no terminal
init(autoreset=True)

# Estatísticas globais para acompanhamento do progresso
stats = {
    "total_games": 0,
    "valid_games": 0,
    "removed_games": 0,
    "valid_links": 0,
    "invalid_links": 0,
    "groups_processed": 0,
    "total_groups": 0
}

# Tempo de início para cálculo de duração
start_time = 0

def setup_logging():
    """Configura o sistema de logging para arquivo e console"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler('validator.log', encoding='utf-8', mode='a'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    return logging.getLogger('validator')


logger = setup_logging()

# Arquivos de dados
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_JSON = os.path.join(BASE_DIR, "data", "raw", "filtred.json")
BLACKLIST_JSON = os.path.join(BASE_DIR, "data", "config", "untracked.json")
RAW_LINKS = os.path.join(BASE_DIR, "data", "raw", "games.json")
INVALID_LINKS_JSON = os.path.join(BASE_DIR, "data", "config", "invalids.json")

# Regex para normalização de títulos
REGEX_TITLE_NORMALIZATION = r"(?:\(.*?\)|\s*(Free Download|v\d+(\.\d+)*[a-zA-Z0-9\-]*|Build \d+|P2P|GOG|Repack|Edition.*|FLT|TENOKE)\s*)"

# Headers HTTP padrão
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

# Configuração para tratamento de interrupção (Ctrl+C)
shutdown_requested = False

def signal_handler(sig, frame):
    """Manipulador de sinal para capturar Ctrl+C e realizar um desligamento limpo"""
    global shutdown_requested
    if not shutdown_requested:
        print(f"\n{Fore.YELLOW}Interrupção detectada. Finalizando após a validação atual...{Style.RESET_ALL}")
        shutdown_requested = True
    else:
        print(f"\n{Fore.RED}Forçando encerramento imediato!{Style.RESET_ALL}")
        print_final_stats()
        sys.exit(1)

# Registra o manipulador de sinal
signal.signal(signal.SIGINT, signal_handler)

def print_stats(clear_screen=False):
    """Exibe estatísticas atuais do processo de validação"""
    global start_time, stats
    
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if clear_screen:
        os.system('cls' if os.name == 'nt' else 'clear')
    
    print(f"\n{Style.BRIGHT}{Fore.CYAN}=== Status da Validação ==={Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Tempo decorrido: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
    print(f"{Fore.GREEN}Jogos válidos: {stats['valid_games']}")
    print(f"{Fore.RED}Jogos removidos: {stats['removed_games']}")
    print(f"{Fore.GREEN}Links válidos: {stats['valid_links']}")
    print(f"{Fore.RED}Links inválidos: {stats['invalid_links']}")
    print(f"{Fore.MAGENTA}Grupos processados: {stats['groups_processed']}/{stats['total_groups']}")
    
    # Calcular taxa de processamento
    if elapsed_time > 0:
        groups_per_hour = stats['groups_processed'] / (elapsed_time / 3600)
        print(f"{Fore.WHITE}Taxa: {groups_per_hour:.1f} grupos/hora")
        
        # Estimativa de tempo restante
        if stats['groups_processed'] > 0 and stats['total_groups'] > 0:
            remaining_groups = stats['total_groups'] - stats['groups_processed']
            eta_seconds = (elapsed_time / stats['groups_processed']) * remaining_groups
            eta_hours, eta_remainder = divmod(eta_seconds, 3600)
            eta_minutes, eta_seconds = divmod(eta_remainder, 60)
            print(f"{Fore.CYAN}Tempo restante estimado: {int(eta_hours):02}:{int(eta_minutes):02}:{int(eta_seconds):02}")
    
    print(f"{Style.BRIGHT}{Fore.CYAN}========================{Style.RESET_ALL}\n")

def print_final_stats():
    """Exibe estatísticas finais ao término do processo"""
    global start_time, stats
    
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print(f"\n{Style.BRIGHT}{Fore.GREEN}=== Validação Concluída ==={Style.RESET_ALL}")
    print(f"Tempo total: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
    print(f"{Fore.GREEN}Jogos válidos: {stats['valid_games']}")
    print(f"{Fore.RED}Jogos removidos: {stats['removed_games']}")
    print(f"{Fore.GREEN}Links válidos: {stats['valid_links']}")
    print(f"{Fore.RED}Links inválidos: {stats['invalid_links']}")
    print(f"{Fore.MAGENTA}Grupos processados: {stats['groups_processed']}/{stats['total_groups']}")
    
    # Calcular estatísticas de desempenho
    if elapsed_time > 0 and stats['groups_processed'] > 0:
        groups_per_hour = stats['groups_processed'] / (elapsed_time / 3600)
        print(f"Taxa média: {groups_per_hour:.1f} grupos/hora")
    
    print(f"{Style.BRIGHT}{Fore.GREEN}========================{Style.RESET_ALL}\n")

def log_link_status(status: str, link: str, file_size: str = ""):
    """Registra o status de um link com formatação colorida"""
    global stats
    
    if status == "VALID":
        stats["valid_links"] += 1
        size_info = f" - Size: {file_size}" if file_size else ""
        print(f"{Fore.GREEN}[VALID LINK] {link}{size_info}")
    elif status == "INVALID":
        stats["invalid_links"] += 1
        print(f"{Fore.RED}[INVALID LINK] {link}")
    elif status == "SKIP":
        print(f"{Fore.YELLOW}[SKIP LINK] {link}")
    elif status == "VALIDATING":
        print(f"{Fore.CYAN}[VALIDATING] {link}")

def log_game_status(status: str, game_title: str, details: str = ""):
    """Registra o status de um jogo com formatação colorida"""
    global stats
    
    if status == "VALID":
        stats["valid_games"] += 1
        print(f"{Fore.GREEN}[VALIDATED] {Style.BRIGHT}{game_title}{Style.RESET_ALL}")
    elif status == "REMOVED":
        stats["removed_games"] += 1
        print(f"{Fore.RED}[REMOVED] {Style.BRIGHT}{game_title}{Style.RESET_ALL}")
    elif status == "BLACKLISTED":
        print(f"{Fore.YELLOW}[BLACKLISTED] {Style.BRIGHT}{game_title}{Style.RESET_ALL}")
    elif status == "SKIP":
        print(f"{Fore.YELLOW}[SKIP] {details}: {Style.BRIGHT}{game_title}{Style.RESET_ALL}")
    elif status == "GROUP":
        print(f"{Fore.MAGENTA}[GROUP {details}] Processing: {Style.BRIGHT}{game_title}{Style.RESET_ALL}")
    elif status == "SKIPPED_ALTERNATIVES":
        print(f"{Fore.BLUE}[SKIPPED ALTERNATIVES] {details}")
    elif status == "FILE_SIZE":
        print(f"{Fore.BLUE}[FILE SIZE] Set to {details} for {Style.BRIGHT}{game_title}{Style.RESET_ALL}")


def normalize_title(title: str) -> str:
    """Normaliza o título removendo informações desnecessárias como versões, tags, etc."""
    return re.sub(REGEX_TITLE_NORMALIZATION, "", title, flags=re.IGNORECASE).strip().lower()


def load_json(filename: str) -> Dict[str, Any]:
    """Carrega dados de um arquivo JSON ou retorna estrutura padrão se o arquivo não existir"""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"downloads": []}


def save_json(filename: str, data: Dict[str, Any]) -> None:
    """Salva dados em um arquivo JSON com formatação adequada"""
    if "downloads" in data and "name" not in data:
        data["name"] = "Shisuy's source"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def is_valid_link(link: str) -> bool:
    """Verifica se o link pertence a um dos hosts suportados"""
    valid_domains = ["gofile.io", "pixeldrain.com", "mediafire.com", "datanodes.to", "qiwi.gg"]
    return any(domain in link for domain in valid_domains)


async def is_valid_qiwi_link(link: str, client: httpx.AsyncClient) -> Tuple[bool, str]:
    """Verifica se um link do qiwi.gg é válido e retorna o tamanho do arquivo"""
    try:
        response = await client.get(link, timeout=10)
        if response.status_code != 200:
            return False, ""
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Verifica se o arquivo é um torrent (não desejado)
        title_element = soup.find('h1', class_='page_TextHeading__VsM7r')
        if title_element:
            file_name = title_element.get_text(strip=True)
            if "TRNT.rar" in file_name or ".torrent" in file_name:
                return False, ""
                
        # Tenta encontrar o tamanho do arquivo
        size_element = soup.find(string=re.compile(r"Download\s+\d+(\.\d+)?\s*(GB|MB)", re.IGNORECASE))
        if size_element:
            file_size = size_element.strip().replace("Download ", "")
            return True, file_size
            
        # Busca alternativa pelo tamanho do arquivo
        text = soup.get_text(" ", strip=True)
        match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB|KB|TB))", text, re.IGNORECASE)
        if match:
            return True, match.group(1)
            
        return False, ""
    except Exception:
        return False, ""

async def is_valid_datanodes_link(link: str, client: httpx.AsyncClient) -> Tuple[bool, str]:
    """Verifica se um link do datanodes.to é válido e retorna o tamanho do arquivo"""
    try:
        response = await client.get(link, timeout=10)
        if response.status_code != 200:
            return False, ""
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Verifica se o arquivo é um torrent (não desejado)
        title_element = soup.find('span', class_='block truncate w-auto')
        if title_element:
            file_name = title_element.get_text(strip=True)
            if "TRNT.rar" in file_name or ".torrent" in file_name:
                return False, ""
                
        # Tenta encontrar o tamanho do arquivo
        size_element = soup.find('small', class_='m-0 text-xs text-gray-500 font-bold')
        if size_element:
            file_size = size_element.get_text(strip=True)
            return True, file_size
            
        # Busca alternativa pelo tamanho do arquivo
        text = soup.get_text(" ", strip=True)
        match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB|KB|TB))", text, re.IGNORECASE)
        if match:
            return True, match.group(1)
            
        return False, ""
    except Exception:
        return False, ""

async def is_valid_pixeldrain_link(link: str, client: httpx.AsyncClient) -> Tuple[bool, str]:
    """Verifica se um link do pixeldrain.com é válido e retorna o tamanho do arquivo"""
    try:
        file_id = link.split("/")[-1]
        api_url = f"https://pixeldrain.com/api/file/{file_id}/info"
        response = await client.get(api_url, timeout=10)
        
        if response.status_code != 200:
            return False, ""
            
        file_info = response.json()
        if file_info.get("success") is not True:
            return False, ""
            
        # Verifica se o arquivo é um torrent (não desejado)
        file_name = file_info.get("name", "").lower()
        if any(indicator in file_name for indicator in ["trnt.rar", ".torrent", "bittorrent"]):
            return False, ""
            
        # Calcula o tamanho do arquivo
        file_size_bytes = file_info.get("size", 0)
        if file_size_bytes > 0:
            file_size = f"{file_size_bytes / (1024 ** 2):.2f} MB" if file_size_bytes < (1024 ** 3) else f"{file_size_bytes / (1024 ** 3):.2f} GB"
            return True, file_size
            
        # Busca alternativa pelo tamanho do arquivo
        for v in file_info.values():
            if isinstance(v, str):
                match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB|KB|TB))", v, re.IGNORECASE)
                if match:
                    return True, match.group(1)
                    
        return False, ""
    except Exception:
        return False, ""

def extract_mediafire_key(url: str) -> str:
    """Extrai a chave de identificação de um link do MediaFire"""
    url = re.sub(r'/file/?$', '', url)
    try:
        if "/file/" in url:
            parts = url.split("/file/")[1].split("/")
            return parts[0]
    except Exception:
        pass
    return ""

def create_chromium_driver() -> webdriver.Chrome:
    """Cria e configura uma instância do Chrome WebDriver para automação"""
    # Configuração das opções do Chrome
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--remote-debugging-port=9222')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.page_load_strategy = 'eager'
    chrome_options.binary_location = "/usr/bin/chromium-browser"
    
    try:
        import subprocess
        import sys
        
        def get_chromium_full_version() -> str:
            """Obtém a versão completa do Chromium instalado"""
            try:
                output = subprocess.check_output(["/usr/bin/chromium-browser", "--version"]).decode()
                version = re.search(r"(\d+\.\d+\.\d+\.\d+)", output)
                return version.group(1) if version else ""
            except Exception:
                return ""

        # Tenta usar a versão específica do Chromium instalado
        chromium_full_version = get_chromium_full_version()
        if chromium_full_version:
            service = Service(ChromeDriverManager(driver_version=chromium_full_version).install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(30)
            return driver

        # Fallback para a versão padrão do ChromeDriver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver

    except Exception as e:
        logger.error(f"Failed to create Chrome driver: {str(e)}")
        print(f"FATAL: Could not start ChromeDriver: {e}", flush=True)
        sys.exit(1)

def check_mediafire_link(link: str, driver: webdriver.Chrome) -> str:
    """Verifica se um link do MediaFire é válido usando o Selenium"""
    try:
        driver.set_page_load_timeout(10)
        driver.get(link)
        
        # Verifica condições de erro
        if "error.php" in driver.current_url:
            return ""
        if "File sharing and storage made simple" in driver.title:
            return ""
        if "Dangerous File Blocked" in driver.page_source:
            return ""
            
        return link
    except Exception:
        return ""


async def validate_mediafire_link(session: httpx.AsyncClient, link: str, driver: webdriver.Chrome) -> Tuple[str, str]:
    """Valida um link do MediaFire e retorna o link e o tamanho do arquivo se válido"""
    # Verifica o link usando Selenium em uma thread separada
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=1) as executor:
        result = await loop.run_in_executor(executor, check_mediafire_link, link, driver)
        if not result:
            return "", ""
            
    # Extrai a chave do MediaFire e verifica via API
    quick_key = extract_mediafire_key(link)
    if not quick_key:
        return "", ""
        
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
                    
                    # Verifica se é um torrent
                    if ".torrent" in file_name:
                        return "", ""
                        
                    # Processa o tamanho do arquivo
                    if not file_size or int(file_size) <= 0:
                        # Busca alternativa pelo tamanho do arquivo
                        for v in file_info.values():
                            if isinstance(v, str):
                                match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB|KB|TB))", v, re.IGNORECASE)
                                if match:
                                    return link, match.group(1)
                        return "", ""
                        
                    formatted_size = format_size(int(file_size))
                    return link, formatted_size
                    
            return "", ""
    except Exception:
        return "", ""

def format_size(size_in_bytes: int) -> str:
    """Formata o tamanho em bytes para uma representação legível (KB, MB, GB)"""
    if size_in_bytes < 1024 ** 2:
        return f"{size_in_bytes / 1024:.2f} KB"
    elif size_in_bytes < 1024 ** 3:
        return f"{size_in_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_in_bytes / (1024 ** 3):.2f} GB"


def load_invalid_links() -> Set[str]:
    """Carrega a lista de links inválidos do arquivo JSON"""
    if os.path.exists(INVALID_LINKS_JSON):
        try:
            with open(INVALID_LINKS_JSON, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_invalid_links(invalid_links: Set[str]) -> None:
    """Salva a lista de links inválidos em um arquivo JSON"""
    # Garante que o diretório existe
    os.makedirs(os.path.dirname(INVALID_LINKS_JSON), exist_ok=True)
    with open(INVALID_LINKS_JSON, "w", encoding="utf-8") as f:
        json.dump(list(invalid_links), f, ensure_ascii=False, indent=4)

async def validate_links(game: Dict[str, Any], invalid_links: Set[str], driver: webdriver.Chrome) -> Tuple[Dict[str, Any], Set[str]]:
    """Valida os links de um jogo e retorna o jogo atualizado e novos links inválidos"""
    global shutdown_requested
    
    valid_links = []
    new_invalid_links = set()
    uris = game.get("uris", [])
    game_title = game.get('title', game.get('repackLinkSource', 'SEM TITULO'))
    file_sizes = []  # Armazena tamanhos de arquivo para cada link válido

    # Verifica se o jogo tem links para validar
    if not uris:
        log_game_status("SKIP", game_title, "Game without 'uris'")
        logger.info(f"[SKIP] Game without 'uris': {game_title}")
        return game, new_invalid_links

    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = []
        link_mapping = {}

        # Prepara tarefas de validação para cada link
        for link in uris:
            # Verifica se foi solicitada interrupção
            if shutdown_requested:
                break
                
            # Pula links já conhecidos como inválidos
            if link in invalid_links:
                log_link_status("SKIP", link, "Already invalid")
                logger.info(f"[SKIP LINK] Already invalid: {link}")
                continue

            log_link_status("VALIDATING", link)
            logger.info(f"[VALIDATING LINK] {link}")

            # Seleciona o método de validação apropriado para cada tipo de link
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
                tasks.append(validate_mediafire_link(client, link, driver))
                link_mapping[len(tasks) - 1] = link
            elif "gofile.io" in link:
                tasks.append(validate_gofile_link_api(link))
                link_mapping[len(tasks) - 1] = link
            elif is_valid_link(link):
                # Links de domínios conhecidos são aceitos diretamente
                log_link_status("VALID", link)
                logger.info(f"[DIRECT ACCEPT LINK] {link}")
                valid_links.append(link)

        # Processa os resultados das validações
        if tasks and not shutdown_requested:
            results = await asyncio.gather(*tasks)
            for task_index, result in enumerate(results):
                link = link_mapping[task_index]
                
                # Trata diferentes formatos de resultado
                if isinstance(result, tuple):
                    is_valid, file_size = result
                    # Para links do MediaFire que retornam (link, tamanho)
                    if isinstance(is_valid, str):
                        valid_link, file_size = result
                        is_valid = bool(valid_link)
                else:
                    # Caso inesperado
                    is_valid, file_size = False, ""
                    
                if is_valid:
                    log_link_status("VALID", link, file_size)
                    logger.info(f"[VALID LINK] {link} - Size: {file_size}")
                    valid_links.append(link)
                    if file_size:  # Adiciona apenas tamanhos válidos
                        file_sizes.append(file_size)
                else:
                    log_link_status("INVALID", link)
                    logger.info(f"[INVALID LINK] {link}")
                    new_invalid_links.add(link)

    # Atualiza o jogo com os links válidos
    game["uris"] = valid_links
    
    # Define o tamanho do arquivo se disponível
    if file_sizes:
        # Usa o maior tamanho de arquivo encontrado (mais confiável)
        game["fileSize"] = get_largest_file_size(file_sizes)
        log_game_status("FILE_SIZE", game_title, game["fileSize"])
        logger.info(f"[FILE SIZE] Set to {game['fileSize']} for {game_title}")
        
    return game, new_invalid_links

def get_largest_file_size(file_sizes: List[str]) -> str:
    """
    Encontra o maior tamanho de arquivo de uma lista de strings de tamanho.
    Ex: ["20 MB", "2 GB", "500 KB"] retornaria "2 GB"
    """
    if not file_sizes:
        return ""
        
    def size_to_bytes(size_str: str) -> float:
        """Converte uma string de tamanho (ex: '10 MB') para bytes"""
        size_str = size_str.strip().upper()
        match = re.search(r"([\d.]+)\s*([KMGT]?B)", size_str)
        if not match:
            return 0.0
            
        value, unit = match.groups()
        value = float(value)
        multipliers = {
            "B": 1,
            "KB": 1024,
            "MB": 1024**2,
            "GB": 1024**3,
            "TB": 1024**4
        }
        return value * multipliers.get(unit, 1)
    
    # Converte tamanhos para bytes para comparação
    sizes_in_bytes = [(size, size_to_bytes(size)) for size in file_sizes]
    
    # Encontra o maior tamanho
    largest_size = max(sizes_in_bytes, key=lambda x: x[1], default=("", 0))
    
    return largest_size[0]

async def process_duplicates(games: List[Dict[str, Any]], driver: webdriver.Chrome) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Processa jogos duplicados seguindo estas regras:
    1. Agrupa jogos com títulos similares (após normalização)
    2. Ordena cada grupo por data de upload (mais recente primeiro)
    3. Valida apenas a versão mais recente de cada jogo
    4. Se encontrar uma versão com pelo menos um link válido, ignora as versões mais antigas
    5. Apenas jogos sem links válidos são adicionados à blacklist
    """
    global stats, start_time, shutdown_requested
    
    blacklist = load_blacklist()
    invalid_links = load_invalid_links()
    grouped_games: Dict[str, List[Dict[str, Any]]] = {}

    # Agrupa jogos por título normalizado
    stats["total_games"] = len(games)
    print(f"{Fore.BLUE}Total de jogos recebidos: {stats['total_games']}{Style.RESET_ALL}")
    
    for game in games:
        # Verifica se foi solicitada interrupção
        if shutdown_requested:
            break
            
        # Pula jogos já na blacklist
        if is_blacklisted(game, blacklist):
            log_game_status("BLACKLISTED", game.get('title', game.get('repackLinkSource', 'SEM TITULO')))
            continue
            
        # Verifica se o jogo tem título
        title = game.get("title", None)
        if not title:
            log_game_status("SKIP", game.get('repackLinkSource', 'SEM TITULO'), "Sem título")
            continue
            
        # Agrupa por título normalizado
        normalized_title = normalize_title(title)
        if normalized_title not in grouped_games:
            grouped_games[normalized_title] = []
        grouped_games[normalized_title].append(game)

    stats["total_groups"] = len(grouped_games)
    print(f"{Fore.BLUE}Total de grupos de jogos: {stats['total_groups']}{Style.RESET_ALL}")

    valid_games: List[Dict[str, Any]] = []
    removed_games: List[Dict[str, Any]] = []
    all_new_invalid_links: Set[str] = set()

    group_keys = list(grouped_games.keys())
    start_time = time.time()
    
    async def process_game_group(group_games: List[Dict[str, Any]], group_idx: int) -> None:
        """Processa um grupo de jogos com o mesmo título normalizado"""
        global stats, shutdown_requested
        nonlocal valid_games, removed_games, all_new_invalid_links
        
        # Verifica se foi solicitada interrupção
        if shutdown_requested:
            return
            
        group_title = group_games[0].get('title', group_games[0].get('repackLinkSource', 'SEM TITULO'))
        stats["groups_processed"] += 1
        log_game_status("GROUP", group_title, f"{stats['groups_processed']}/{stats['total_groups']} ({len(group_games)} games)")
        logger.info(f"[{stats['groups_processed']}/{stats['total_groups']}] Processing group: {group_title} ({len(group_games)} games)")
        
        # Atualiza estatísticas periodicamente
        if stats["groups_processed"] % 5 == 0:
            print_stats()
        
        # Ordena os jogos por data de upload (mais recente primeiro)
        sorted_games = sorted(
            group_games,
            key=lambda g: datetime.fromisoformat(g.get("uploadDate", "1970-01-01T00:00:00")) if g.get("uploadDate") else datetime.min,
            reverse=True
        )
        
        # Processa cada jogo do grupo, começando pelo mais recente
        for game in sorted_games:
            # Verifica se foi solicitada interrupção
            if shutdown_requested:
                return
                
            game_title = game.get('title', game.get('repackLinkSource', 'SEM TITULO'))
            print(f"{Fore.CYAN}Validando jogo: {Style.BRIGHT}{game_title}{Style.RESET_ALL}")
            logger.info(f"Validating game: {game_title}")
            
            # Valida os links do jogo
            validated, new_invalid_links = await validate_links(game, invalid_links, driver)
            all_new_invalid_links.update(new_invalid_links)
            uris = validated.get("uris", [])
            
            # Se não tem links válidos, marca como removido
            if not uris:
                log_game_status("REMOVED", game_title)
                logger.info(f"[REMOVED] {game_title}")
                removed_games.append(validated)
                stats["removed_games"] += 1
                continue
            
            # Se tem links válidos, adiciona à lista de jogos válidos
            log_game_status("VALID", game_title)
            logger.info(f"[VALIDATED] {game_title}")
            valid_games.append(validated)
            stats["valid_games"] += 1
            stats["valid_links"] += len(uris)
            
            # Se encontrou uma versão válida, ignora as outras versões do mesmo jogo
            if len(group_games) > 1:
                skipped_titles = [g.get('title', g.get('repackLinkSource', 'SEM TITULO')) for g in group_games if g != validated]
                skipped_info = f"{len(skipped_titles)} versões alternativas ignoradas: {', '.join(skipped_titles[:3])}{' e mais...' if len(skipped_titles) > 3 else ''}"
                log_game_status("SKIPPED_ALTERNATIVES", "", skipped_info)
                logger.info(f"[SKIPPED ALTERNATIVES] {skipped_info}")
                
                # Adiciona os jogos ignorados à contagem de removidos
                stats["removed_games"] += len(skipped_titles)
            return
        
        # Se chegou aqui, nenhum jogo do grupo tem links válidos
        log_game_status("REMOVED", group_title, "GRUPO INTEIRO - Nenhum link válido")
        logger.info(f"[REMOVED ENTIRE GROUP] {group_title}")
        removed_games.extend(group_games)
        stats["removed_games"] += len(group_games)

    # Inicializa estatísticas
    stats["groups_processed"] = 0
    
    try:
        with tqdm(total=stats["total_groups"], desc="Grupos de jogos validados", ncols=100) as pbar:
            for idx, group_key in enumerate(group_keys):
                # Verifica se foi solicitada interrupção
                if shutdown_requested:
                    print(f"\n{Fore.YELLOW}Interrupção detectada. Salvando progresso...{Style.RESET_ALL}")
                    break
                    
                await process_game_group(grouped_games[group_key], idx)
                
                # Atualiza a barra de progresso
                elapsed = time.time() - start_time
                if idx > 0:  # Evita divisão por zero
                    avg_time = elapsed / idx
                    eta = avg_time * (stats["total_groups"] - idx)
                    pbar.set_postfix({"ETA": f"{int(eta//60)}m{int(eta%60)}s"})
                pbar.update(1)
                
                # Salvamento incremental após cada grupo
                save_json(SOURCE_JSON, {"downloads": valid_games})
                # Apenas jogos que realmente não têm links válidos são adicionados à blacklist
                # Jogos alternativos de versões válidas não são adicionados à lista removed_games
                save_json(BLACKLIST_JSON, {"removed": blacklist + [g for g in removed_games if not is_blacklisted(g, blacklist)]})
                invalid_links.update(all_new_invalid_links)
                save_invalid_links(invalid_links)
                
                # A cada 20 grupos, exibe estatísticas completas
                if idx % 20 == 0 and idx > 0:
                    print_stats()
    except Exception as e:
        print(f"\n{Fore.RED}Erro durante o processamento: {str(e)}{Style.RESET_ALL}")
        logger.error(f"Erro durante o processamento: {str(e)}")
        # Mesmo em caso de erro, tenta salvar o progresso
        save_json(SOURCE_JSON, {"downloads": valid_games})
        save_json(BLACKLIST_JSON, {"removed": blacklist + [g for g in removed_games if not is_blacklisted(g, blacklist)]})
        invalid_links.update(all_new_invalid_links)
        save_invalid_links(invalid_links)

    invalid_links.update(all_new_invalid_links)
    save_invalid_links(invalid_links)
    return valid_games, removed_games

def load_blacklist() -> List[Dict[str, Any]]:
    """Carrega a lista de jogos na blacklist do arquivo JSON"""
    if os.path.exists(BLACKLIST_JSON):
        try:
            with open(BLACKLIST_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("removed", [])
        except Exception:
            return []
    return []

def is_blacklisted(game: Dict[str, Any], blacklist: List[Dict[str, Any]]) -> bool:
    """Verifica se um jogo está na blacklist comparando o repackLinkSource"""
    repack = game.get("repackLinkSource")
    for removed in blacklist:
        if repack and repack == removed.get("repackLinkSource"):
            return True
    return False

# Constantes para Gofile API
WT = "4fd6sg89d7s6"
GOFILE_TOKEN = None

async def authorize_gofile() -> str:
    """Obtém um token de autorização para a API do Gofile"""
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

async def validate_gofile_link_api(link: str, retries: int = 3) -> Tuple[bool, str]:
    """Valida um link do Gofile usando a API e retorna status e tamanho do arquivo
    
    Args:
        link: URL do Gofile para validar
        retries: Número de tentativas em caso de falha
        
    Returns:
        Tupla com (é_válido, tamanho_formatado)
    """
    # Extrai o ID do arquivo da URL
    m = re.search(r"gofile\.io/d/([^/?]+)", link)
    if not m:
        return False, ""
        
    file_id = m.group(1)
    api_url = f"https://api.gofile.io/contents/{file_id}?wt={WT}"
    
    # Garante que temos um token de autorização
    global GOFILE_TOKEN
    if not GOFILE_TOKEN:
        await authorize_gofile()
        
    # Prepara os headers com o token se disponível
    headers = {**HEADERS}
    if GOFILE_TOKEN:
        headers["Authorization"] = f"Bearer {GOFILE_TOKEN}"
        
    # Configura o proxy SOCKS5 para contornar bloqueios
    from httpx_socks import AsyncProxyTransport
    transport = AsyncProxyTransport.from_url("socks5://127.0.0.1:9050")
    
    await asyncio.sleep(1)  # Pequena pausa para evitar rate limiting
    
    # Tenta validar o link com retentativas em caso de falha
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=15, transport=transport) as client:
                response = await client.get(api_url, headers=headers)
                
            if response.status_code == 200:
                try:
                    data = response.json()
                    if not isinstance(data, dict):
                        continue
                        
                    if data.get("status") == "ok":
                        content = data.get("data", {})
                        
                        # Verifica se é uma pasta e tem arquivos
                        if isinstance(content, dict) and content.get("type") == "folder":
                            children = content.get("children", {})
                            
                            if children:
                                # Pega o primeiro arquivo da pasta
                                first_child = next(iter(children.values()))
                                name = first_child.get("name", "").lower()
                                
                                # Filtra arquivos indesejados
                                if any(bad in name for bad in ["torrent", "this content does not exist", "cold"]):
                                    return False, ""
                                    
                                # Obtém e formata o tamanho do arquivo
                                size_bytes = first_child.get("size")
                                if size_bytes and str(size_bytes).isdigit():
                                    bytes_val = int(size_bytes)
                                    
                                    if bytes_val > 0:
                                        # Formata o tamanho em unidades apropriadas
                                        if bytes_val < 1024:
                                            size_str = f"{bytes_val} B"
                                        elif bytes_val < 1024 ** 2:
                                            size_str = f"{(bytes_val / 1024):.2f} KB"
                                        elif bytes_val < 1024 ** 3:
                                            size_str = f"{(bytes_val / (1024 ** 2)):.2f} MB"
                                        else:
                                            size_str = f"{(bytes_val / (1024 ** 3)):.2f} GB"
                                        return True, size_str
                except Exception:
                    continue
        except Exception:
            # Espera exponencial entre tentativas
            await asyncio.sleep(2 ** attempt)
            continue
            
        # Espera entre tentativas se não for a última
        if attempt < retries - 1:
            await asyncio.sleep(2 ** attempt)
            
    return False, ""  # Retorna falso se todas as tentativas falharem

async def main() -> None:
    """Função principal que carrega os dados, processa os jogos e salva os resultados"""
    global start_time, stats, shutdown_requested
    
    # Inicializa o tempo de início
    start_time = time.time()
    
    # Exibe mensagem de início
    print(f"\n{Style.BRIGHT}{Fore.CYAN}=== Iniciando Validação de Links ==={Style.RESET_ALL}\n")
    
    # Carrega os dados dos jogos do arquivo JSON
    shisuy_data = load_json(RAW_LINKS)
    
    # Extrai a lista de jogos dependendo da estrutura do JSON
    if "downloads" in shisuy_data:
        games = shisuy_data["downloads"]
    elif "games" in shisuy_data:
        games = shisuy_data["games"]
    else:
        games = []
    
    # Atualiza estatísticas
    stats["total_games"] = len(games)
    print(f"{Fore.BLUE}Total de jogos carregados: {stats['total_games']}")

    try:
        # Inicializa o driver do Chrome para validação de links
        driver = create_chromium_driver()
        try:
            # Processa os jogos para identificar duplicatas e validar links
            valid_games, removed_games = await process_duplicates(games, driver)
            
            # Atualiza estatísticas finais
            stats["valid_games"] = len(valid_games)
            stats["removed_games"] = len(removed_games)
            
        except Exception as e:
            print(f"\n{Fore.RED}Erro durante o processamento: {str(e)}{Style.RESET_ALL}")
            logger.error(f"Erro durante o processamento: {str(e)}")
            raise
        finally:
            # Garante que o driver seja fechado mesmo em caso de erro
            driver.quit()

        # Atualiza a blacklist com os jogos removidos
        blacklist = load_blacklist()
        new_blacklist_entries = [g for g in removed_games if not is_blacklisted(g, blacklist)]
        blacklist.extend(new_blacklist_entries)
        
        # Salva os jogos válidos e a blacklist atualizada
        save_json(SOURCE_JSON, {"downloads": valid_games})
        save_json(BLACKLIST_JSON, {"removed": blacklist})
        
        print(f"\n{Fore.GREEN}Dados salvos com sucesso:{Style.RESET_ALL}")
        print(f"- Jogos válidos: {len(valid_games)}")
        print(f"- Jogos na blacklist: {len(blacklist)}")
        
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Processo interrompido pelo usuário.{Style.RESET_ALL}")
    except Exception as e:
        print(f"\n{Fore.RED}Erro fatal: {str(e)}{Style.RESET_ALL}")
        logger.error(f"Erro fatal: {str(e)}")
    finally:
        # Exibe estatísticas finais
        print_final_stats()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Processo interrompido pelo usuário.{Style.RESET_ALL}")
        print_final_stats()
    except Exception as e:
        print(f"\n{Fore.RED}Erro fatal: {str(e)}{Style.RESET_ALL}")
        logger.error(f"Erro fatal: {str(e)}")
        print_final_stats()
