import aiohttp
import asyncio
import json
import requests
from colorama import Fore, init
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

init(autoreset=True)

REMOTE_JSON_URL = "https://raw.githubusercontent.com/Shisuiicaro/Scraper/refs/heads/update/shisuyssource.json"
CONCURRENT_REQUESTS = 100
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

GOFILE_WT = "4fd6sg89d7s6"
_gofile_token = None

async def get_gofile_token(session):
    global _gofile_token
    if _gofile_token:
        return _gofile_token
        
    async with session.post("https://api.gofile.io/accounts") as response:
        if response.status != 200:
            return None
            
        data = await response.json()
        if data.get("status") == "ok":
            _gofile_token = data["data"]["token"]
            return _gofile_token
    return None

async def validate_gofile_link(session, link):
    try:
        gofile_id = link.split("/")[-1]
        
        token = await get_gofile_token(session)
        if not token:
            print(f"{Fore.RED}[ERROR] Falha ao obter token do Gofile")
            return None

        params = {"wt": GOFILE_WT}
        headers = {"Authorization": f"Bearer {token}"}
        
        async with session.get(
            f"https://api.gofile.io/contents/{gofile_id}",
            params=params,
            headers=headers
        ) as response:
            if response.status != 200:
                print(f"{Fore.RED}[INVALID] {link} (Status: {response.status})")
                return None
                
            data = await response.json()
            if data.get("status") != "ok":
                print(f"{Fore.RED}[INVALID] {link} (API Error)")
                return None

            content_data = data.get("data", {})
            children = content_data.get("children", {})
            
            if not children:
                print(f"{Fore.RED}[INVALID] {link} (Pasta vazia)")
                return None
                
            if len(children) > 1:
                print(f"{Fore.YELLOW}[WARNING] {link} (Múltiplos arquivos)")
                
            for child in children.values():
                if any(ext in child.get("name", "").lower() for ext in [".torrent", "magnet"]):
                    print(f"{Fore.RED}[INVALID] {link} (Torrent detectado)")
                    return None
            
            total_size = sum(int(child.get("size", 0)) for child in children.values())
            formatted_size = format_size(total_size)
            print(f"{Fore.GREEN}[VALID] {link} ({formatted_size})")
            return link, formatted_size

    except Exception as e:
        print(f"{Fore.RED}[ERROR] Falha ao validar {link}: {e}")
        return None, ""

async def fetch_json(session):
    async with session.get(REMOTE_JSON_URL, headers=HEADERS) as response:
        if response.status != 200:
            print(f"{Fore.RED}Erro ao buscar JSON: Status {response.status}")
            return None
        text = await response.text()
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            print(f"{Fore.RED}Erro na conversão do JSON: {e}")
            return None

def extract_mediafire_key(url):
    if "file/" in url:
        return url.split("file/")[1].split("/")[0]
    return None

def extract_download_link(content):
    for line in content.splitlines():
        m = re.search(r'href="((http|https)://download[^"]+)', line)
        if m:
            return m.groups()[0]
    return None

class DriverPool:
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

driver_pool = DriverPool(size=3)

def check_mediafire_link(link):
    driver = driver_pool.get_driver()
    try:
        driver.set_page_load_timeout(10)
        driver.get(link)
        
        try:
            WebDriverWait(driver, 5).until(
                lambda d: d.title and len(d.title) > 0
            )
        except TimeoutException:
            return None
            
        status = 200 if "File sharing and storage made simple" not in driver.title else 404
        return link if status == 200 else None
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Falha ao validar {link}: {e}")
        return None
    finally:
        driver_pool.return_driver(driver)

def format_size(size_bytes):
    if size_bytes > 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    else:
        return f"{size_bytes / (1024 * 1024):.2f} MB"

async def validate_mediafire_link(session, link):
    quick_key = extract_mediafire_key(link)
    if quick_key:
        api_url = f"https://www.mediafire.com/api/1.1/file/get_info.php?quick_key={quick_key}&response_format=json"
        try:
            async with session.get(api_url, headers=HEADERS) as response:
                data = await response.json()
                if data.get("response", {}).get("result") == "Success":
                    file_size = data["response"]["file_info"].get("size", 0)
                    formatted_size = format_size(int(file_size))
                else:
                    formatted_size = ""
        except:
            formatted_size = ""
    else:
        formatted_size = ""

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=3) as executor:
        result = await loop.run_in_executor(executor, check_mediafire_link, link)
        
        if result:
            print(f"{Fore.GREEN}[VALID] {link} ({formatted_size})")
            return link, formatted_size
        else:
            print(f"{Fore.RED}[INVALID] {link}")
            return None, ""

async def validate_qiwi_link(session, link):
    try:
        async with session.get(
            link,
            headers={"User-Agent": "Mozilla/5.0"},
            allow_redirects=True,
            ssl=False
        ) as response:
            if response.status == 200:
                print(f"{Fore.GREEN}[VALID] {link}")
                return link, ""
            else:
                print(f"{Fore.RED}[INVALID] {link}")
                return None, ""
    except Exception as e:
        print(f"{Fore.RED}[ERROR] {link}: {e}")
        return None, ""

async def validate_single_link(session, link, semaphore):
    try:
        async with semaphore:
            if "mediafire.com" in link.lower():
                return await validate_mediafire_link(session, link)
            elif "qiwi.gg" in link.lower():
                return await validate_qiwi_link(session, link)
            elif "gofile.io" in link.lower():
                return await validate_gofile_link(session, link)

            timeout = aiohttp.ClientTimeout(total=30)
            async with session.get(link, headers=HEADERS, timeout=timeout) as response:
                if response.status != 200:
                    print(f"{Fore.RED}[INVALID] {link} (Status {response.status})")
                    return None, ""
                    
                content = await response.text()
                error_indicators = [
                    "file could not be found",
                    "unavailable for legal reasons",
                    "unavailable",
                    "torrent",
                    "magnet:",
                    ".torrent",
                    "file has been deleted",
                    "file does not exist",
                    "error-page-premium",
                    "file has been removed"
                ]

                if any(indicator in content.lower() for indicator in error_indicators):
                    print(f"{Fore.RED}[INVALID] {link} (Indicador de erro detectado)")
                    return None, ""
                    
                print(f"{Fore.GREEN}[VALID] {link}")
                return link, ""

    except Exception as e:
        print(f"{Fore.RED}[ERROR] Falha ao validar {link}: {e}")
        return None, ""

async def validate_all_links():
    try:
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession() as session:
            data = await fetch_json(session)
            if not data or "downloads" not in data:
                print(f"{Fore.RED}JSON inválido")
                return

            total_games = len(data["downloads"])
            
            for game in data["downloads"]:
                uris = [uri for uri in game.get("uris", []) if uri is not None]
                results = await asyncio.gather(*(validate_single_link(session, uri, semaphore) for uri in uris))
                
                valid_results = []
                for result in results:
                    if result and isinstance(result, tuple) and len(result) == 2 and result[0]:
                        valid_results.append(result)
                
                game["uris"] = [link for link, _ in valid_results]
                valid_sizes = [size for _, size in valid_results if size]
                game["fileSize"] = max(valid_sizes, default="") if valid_sizes else ""

            def is_valid_game(game):
                links = game.get("uris", [])
                if not links:
                    return False
                if all(link and "1fichier.com" in link for link in links):
                    return False
                if all(link and "torrent" in link.lower() for link in links):
                    return False
                return True

            data["downloads"] = [game for game in data["downloads"] if is_valid_game(game)]
            valid_games = len(data["downloads"])
            removed_games = total_games - valid_games

            with open("shisuyssource.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                
            print(f"\n{Fore.CYAN}Estatísticas:")
            print(f"{Fore.CYAN}Total de jogos: {total_games}")
            print(f"{Fore.GREEN}Jogos válidos: {valid_games}")
            print(f"{Fore.RED}Jogos removidos: {removed_games}")
            print(f"{Fore.GREEN}JSON atualizado com sucesso")
    finally:
        driver_pool.cleanup()

def main():
    asyncio.run(validate_all_links())

if __name__ == "__main__":
    main()
