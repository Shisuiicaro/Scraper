import json
import re
from datetime import datetime
import httpx
from bs4 import BeautifulSoup
import asyncio
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from typing import List, Tuple
import os
from colorama import Fore, Style, init
from tqdm import tqdm
import time
import logging

def setup_logging():
    import sys
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

init(autoreset=True)

SOURCE_JSON = "./data/raw/filtred.json"
BLACKLIST_JSON = "./data/config/untracked.json"
RAW_LINKS = "./data/raw/games.json"
INVALID_LINKS_JSON = "./data/config/invalids.json"
REGEX_TITLE_NORMALIZATION = r"(?:\(.*?\)|\s*(Free Download|v\d+(\.\d+)*[a-zA-Z0-9\-]*|Build \d+|P2P|GOG|Repack|Edition.*|FLT|TENOKE)\s*)"

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
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"downloads": []}

def save_json(filename, data):
    if "downloads" in data and "name" not in data:
        data["name"] = "Shisuy's source"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def is_valid_link(link):
    return any(domain in link for domain in ["gofile.io", "pixeldrain.com", "mediafire.com", "datanodes.to", "qiwi.gg"])

async def is_valid_qiwi_link(link, client):
    try:
        response = await client.get(link, timeout=10)
        if response.status_code != 200:
            return False, None
        soup = BeautifulSoup(response.text, 'html.parser')
        title_element = soup.find('h1', class_='page_TextHeading__VsM7r')
        if title_element:
            file_name = title_element.get_text(strip=True)
            if "TRNT.rar" in file_name or ".torrent" in file_name:
                return False, None
        size_element = soup.find(string=re.compile(r"Download\s+\d+(\.\d+)?\s*(GB|MB)", re.IGNORECASE))
        if size_element:
            file_size = size_element.strip().replace("Download ", "")
            return True, file_size
        text = soup.get_text(" ", strip=True)
        match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB|KB|TB))", text, re.IGNORECASE)
        if match:
            return True, match.group(1)
        return False, None
    except Exception:
        return False, None

async def is_valid_datanodes_link(link, client):
    try:
        response = await client.get(link, timeout=10)
        if response.status_code != 200:
            return False, None
        soup = BeautifulSoup(response.text, 'html.parser')
        title_element = soup.find('span', class_='block truncate w-auto')
        if title_element:
            file_name = title_element.get_text(strip=True)
            if "TRNT.rar" in file_name or ".torrent" in file_name:
                return False, None
        size_element = soup.find('small', class_='m-0 text-xs text-gray-500 font-bold')
        if size_element:
            file_size = size_element.get_text(strip=True)
            return True, file_size
        text = soup.get_text(" ", strip=True)
        match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB|KB|TB))", text, re.IGNORECASE)
        if match:
            return True, match.group(1)
        return False, None
    except Exception:
        return False, None

async def is_valid_pixeldrain_link(link, client):
    try:
        file_id = link.split("/")[-1]
        api_url = f"https://pixeldrain.com/api/file/{file_id}/info"
        response = await client.get(api_url, timeout=10)
        if response.status_code != 200:
            return False, None
        file_info = response.json()
        if file_info.get("success") is not True:
            return False, None
        file_name = file_info.get("name", "").lower()
        if any(indicator in file_name for indicator in ["trnt.rar", ".torrent", "bittorrent"]):
            return False, None
        file_size_bytes = file_info.get("size", 0)
        if file_size_bytes > 0:
            file_size = f"{file_size_bytes / (1024 ** 2):.2f} MB" if file_size_bytes < (1024 ** 3) else f"{file_size_bytes / (1024 ** 3):.2f} GB"
            return True, file_size
        for v in file_info.values():
            if isinstance(v, str):
                match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB|KB|TB))", v, re.IGNORECASE)
                if match:
                    return True, match.group(1)
        return False, None
    except Exception:
        return False, None

def extract_mediafire_key(url):
    url = re.sub(r'/file/?$', '', url)
    try:
        if "/file/" in url:
            parts = url.split("/file/")[1].split("/")
            return parts[0]
    except Exception:
        pass
    return None

def create_chromium_driver():
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
        import re
        from webdriver_manager.chrome import ChromeDriverManager

        def get_chromium_full_version():
            try:
                output = subprocess.check_output(["/usr/bin/chromium-browser", "--version"]).decode()
                version = re.search(r"(\d+\.\d+\.\d+\.\d+)", output)
                return version.group(1) if version else None
            except Exception:
                return None

        chromium_full_version = get_chromium_full_version()
        if chromium_full_version:
            service = Service(ChromeDriverManager(driver_version=chromium_full_version).install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(30)
            return driver

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver

    except Exception as e:
        logger.error(f"Failed to create Chrome driver: {str(e)}")
        print(f"FATAL: Could not start ChromeDriver: {e}", flush=True)
        import sys
        sys.exit(1)

def check_mediafire_link(link, driver):
    try:
        driver.set_page_load_timeout(10)
        driver.get(link)
        if "error.php" in driver.current_url:
            return None
        if "File sharing and storage made simple" in driver.title:
            return None
        if "Dangerous File Blocked" in driver.page_source:
            return None
        return link
    except Exception:
        return None

async def validate_mediafire_link(session, link, driver):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=1) as executor:
        result = await loop.run_in_executor(executor, check_mediafire_link, link, driver)
        if not result:
            return None, ""
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
                    if ".torrent" in file_name:
                        return None, ""
                    if not file_size or int(file_size) <= 0:
                        for v in file_info.values():
                            if isinstance(v, str):
                                match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB|KB|TB))", v, re.IGNORECASE)
                                if match:
                                    return link, match.group(1)
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
    if size_in_bytes < 1024 ** 2:
        return f"{size_in_bytes / 1024:.2f} KB"
    elif size_in_bytes < 1024 ** 3:
        return f"{size_in_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_in_bytes / (1024 ** 3):.2f} GB"

def load_invalid_links():
    if os.path.exists(INVALID_LINKS_JSON):
        try:
            with open(INVALID_LINKS_JSON, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()

def save_invalid_links(invalid_links):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(INVALID_LINKS_JSON), exist_ok=True)
    with open(INVALID_LINKS_JSON, "w", encoding="utf-8") as f:
        json.dump(list(invalid_links), f, ensure_ascii=False, indent=4)

async def validate_links(game, invalid_links, driver):
    valid_links = []
    new_invalid_links = set()
    uris = game.get("uris", [])
    game_title = game.get('title', game.get('repackLinkSource', 'SEM TITULO'))
    file_sizes = []  # Store file sizes for each valid link

    if not uris:
        log_msg = f"[SKIP] Game without 'uris': {game_title}"
        print(f"{Fore.YELLOW}{log_msg}", flush=True)
        logger.info(log_msg)
        return game, new_invalid_links

    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = []
        link_mapping = {}

        for index, link in enumerate(uris):
            if link in invalid_links:
                log_msg = f"[SKIP LINK] Already invalid: {link}"
                print(f"{Fore.YELLOW}{log_msg}", flush=True)
                logger.info(log_msg)
                continue

            log_msg = f"[VALIDATING LINK] {link}"
            print(f"{Fore.CYAN}{log_msg}", flush=True)
            logger.info(log_msg)

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
                log_msg = f"[DIRECT ACCEPT LINK] {link}"
                print(f"{Fore.GREEN}{log_msg}", flush=True)
                logger.info(log_msg)
                valid_links.append(link)

        if tasks:
            results = await asyncio.gather(*tasks)
            for task_index, result in enumerate(results):
                link = link_mapping[task_index]
                
                # Handle different result formats
                if isinstance(result, tuple):
                    is_valid, file_size = result
                else:
                    # For mediafire links that return link, size
                    valid_link, file_size = result
                    is_valid = valid_link is not None
                    
                if is_valid:
                    log_msg = f"[VALID LINK] {link} - Size: {file_size}"
                    print(f"{Fore.GREEN}{log_msg}", flush=True)
                    logger.info(log_msg)
                    valid_links.append(link)
                    if file_size:  # Only add valid file sizes
                        file_sizes.append(file_size)
                else:
                    log_msg = f"[INVALID LINK] {link}"
                    print(f"{Fore.RED}{log_msg}", flush=True)
                    logger.info(log_msg)
                    new_invalid_links.add(link)

    game["uris"] = valid_links
    
    # Set the file size if we have it
    if file_sizes:
        # Use the largest file size found (most reliable)
        game["fileSize"] = get_largest_file_size(file_sizes)
        log_msg = f"[FILE SIZE] Set to {game['fileSize']} for {game_title}"
        print(f"{Fore.BLUE}{log_msg}", flush=True)
        logger.info(log_msg)
        
    return game, new_invalid_links

def get_largest_file_size(file_sizes):
    """
    Find the largest file size from a list of size strings.
    e.g. ["20 MB", "2 GB", "500 KB"] would return "2 GB"
    """
    if not file_sizes:
        return None
        
    # Helper function to convert size string to bytes
    def size_to_bytes(size_str):
        size_str = size_str.strip().upper()
        match = re.search(r"([\d.]+)\s*([KMGT]?B)", size_str)
        if not match:
            return 0
            
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
    
    # Convert sizes to bytes for comparison
    sizes_in_bytes = [(size, size_to_bytes(size)) for size in file_sizes]
    
    # Find the largest size
    largest_size = max(sizes_in_bytes, key=lambda x: x[1], default=(None, 0))
    
    return largest_size[0]

async def process_duplicates(games, driver):
    """
    Processa jogos duplicados seguindo estas regras:
    1. Agrupa jogos com títulos similares (após normalização)
    2. Ordena cada grupo por data de upload (mais recente primeiro)
    3. Valida apenas a versão mais recente de cada jogo
    4. Se encontrar uma versão com pelo menos um link válido, ignora as versões mais antigas
    5. Apenas jogos sem links válidos são adicionados à blacklist
    """
    blacklist = load_blacklist()
    invalid_links = load_invalid_links()
    grouped_games = {}

    total_games = len(games)
    print(f"{Fore.BLUE}Total de jogos recebidos: {total_games}", flush=True)
    for game in games:
        if is_blacklisted(game, blacklist):
            print(f"{Fore.YELLOW}[BLACKLISTED] {game.get('title', game.get('repackLinkSource', 'SEM TITULO'))}", flush=True)
            continue
        title = game.get("title", None)
        if not title:
            print(f"{Fore.YELLOW}[SEM TITLE] {game.get('repackLinkSource', 'SEM TITULO')}", flush=True)
            continue
        normalized_title = normalize_title(title)
        if normalized_title not in grouped_games:
            grouped_games[normalized_title] = []
        grouped_games[normalized_title].append(game)

    total_groups = len(grouped_games)
    print(f"{Fore.BLUE}Total de grupos de jogos: {total_groups}", flush=True)

    valid_games = []
    removed_games = []
    all_new_invalid_links = set()

    group_keys = list(grouped_games.keys())
    start_time = time.time()
    
    async def process_game_group(group_games, group_idx):
        nonlocal valid_games, removed_games, all_new_invalid_links
        group_title = group_games[0].get('title', group_games[0].get('repackLinkSource', 'SEM TITULO'))
        log_msg = f"[{group_idx+1}/{total_groups}] Processing group: {group_title} ({len(group_games)} games)"
        print(f"{Fore.MAGENTA}{log_msg}", flush=True)
        logger.info(log_msg)
        
        # Ordena os jogos por data de upload (mais recente primeiro)
        sorted_games = sorted(
            group_games,
            key=lambda g: datetime.fromisoformat(g.get("uploadDate", "1970-01-01T00:00:00")) if g.get("uploadDate") else datetime.min,
            reverse=True
        )
        # Usa diretamente os jogos ordenados por data, sem priorizar jogos multiplayer
        candidates = sorted_games
        
        for game in candidates:
            game_title = game.get('title', game.get('repackLinkSource', 'SEM TITULO'))
            log_msg = f"Validating game: {game_title}"
            print(f"{Fore.CYAN}{log_msg}", flush=True)
            logger.info(log_msg)
            
            validated, new_invalid_links = await validate_links(game, invalid_links, driver)
            all_new_invalid_links.update(new_invalid_links)
            uris = validated.get("uris", [])
            
            if not uris:
                log_msg = f"[REMOVED] {game_title}"
                print(f"{Fore.RED}{log_msg}", flush=True)
                logger.info(log_msg)
                removed_games.append(validated)
                continue
                
            log_msg = f"[VALIDATED] {game_title}"
            print(f"{Fore.GREEN}{log_msg}", flush=True)
            logger.info(log_msg)
            valid_games.append(validated)
            
            # Não adiciona as outras versões à lista de removed_games para evitar que sejam blacklistadas
            # Apenas ignora as outras versões e passa para o próximo grupo
            if len(group_games) > 1:
                skipped_titles = [g.get('title', g.get('repackLinkSource', 'SEM TITULO')) for g in group_games if g != validated]
                log_msg = f"[SKIPPED ALTERNATIVES] {len(skipped_titles)} versões alternativas ignoradas: {', '.join(skipped_titles[:3])}{' e mais...' if len(skipped_titles) > 3 else ''}"
                print(f"{Fore.BLUE}{log_msg}", flush=True)
                logger.info(log_msg)
            return
            
        log_msg = f"[REMOVED ENTIRE GROUP] {group_title}"
        print(f"{Fore.RED}{log_msg}", flush=True)
        logger.info(log_msg)
        removed_games.extend(group_games)

    with tqdm(total=total_groups, desc="Grupos de jogos validados", ncols=100) as pbar:
        for idx, group_key in enumerate(group_keys):
            await process_game_group(grouped_games[group_key], idx)
            elapsed = time.time() - start_time
            avg_time = elapsed / (idx + 1)
            eta = avg_time * (total_groups - (idx + 1))
            pbar.set_postfix({"ETA": f"{int(eta//60)}m{int(eta%60)}s"})
            pbar.update(1)
            # Salvamento incremental após cada grupo
            save_json(SOURCE_JSON, {"downloads": valid_games})
            # Apenas jogos que realmente não têm links válidos são adicionados à blacklist
            # Jogos alternativos de versões válidas não são adicionados à lista removed_games
            save_json(BLACKLIST_JSON, {"removed": blacklist + [g for g in removed_games if not is_blacklisted(g, blacklist)]})
            invalid_links.update(all_new_invalid_links)
            save_invalid_links(invalid_links)

    invalid_links.update(all_new_invalid_links)
    save_invalid_links(invalid_links)
    return valid_games, removed_games

def load_blacklist():
    if os.path.exists(BLACKLIST_JSON):
        try:
            with open(BLACKLIST_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("removed", [])
        except Exception:
            return []
    return []

def is_blacklisted(game, blacklist):
    repack = game.get("repackLinkSource")
    for removed in blacklist:
        if repack and repack == removed.get("repackLinkSource"):
            return True
    return False

WT = "4fd6sg89d7s6"
GOFILE_TOKEN = None

async def authorize_gofile():
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
    m = re.search(r"gofile\.io/d/([^/?]+)", link)
    if not m:
        return False, ""
    file_id = m.group(1)
    api_url = f"https://api.gofile.io/contents/{file_id}?wt={WT}"
    global GOFILE_TOKEN
    if not GOFILE_TOKEN:
        await authorize_gofile()
    headers = {**HEADERS}
    if GOFILE_TOKEN:
        headers["Authorization"] = f"Bearer {GOFILE_TOKEN}"
    from httpx_socks import AsyncProxyTransport
    transport = AsyncProxyTransport.from_url("socks5://127.0.0.1:9050")
    await asyncio.sleep(1)
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
                except Exception:
                    continue
        except Exception:
            await asyncio.sleep(2 ** attempt)
            continue
        if attempt < retries - 1:
            await asyncio.sleep(2 ** attempt)
    return False, ""

async def main():
    shisuy_data = load_json(RAW_LINKS)
    if "downloads" in shisuy_data:
        games = shisuy_data["downloads"]
    elif "games" in shisuy_data:
        games = shisuy_data["games"]
    else:
        games = []

    driver = create_chromium_driver()
    try:
        valid_games, removed_games = await process_duplicates(games, driver)
    finally:
        driver.quit()

    blacklist = load_blacklist()
    blacklist.extend([g for g in removed_games if not is_blacklisted(g, blacklist)])
    save_json(SOURCE_JSON, {"downloads": valid_games})
    save_json(BLACKLIST_JSON, {"removed": blacklist})

if __name__ == "__main__":
    print("Iniciando validação de links...", flush=True)
    asyncio.run(main())
