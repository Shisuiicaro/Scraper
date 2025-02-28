import aiohttp
import asyncio
import json
import requests
import re
from colorama import Fore, init
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from aiohttp_socks import ProxyConnector

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

def format_size(size_bytes):
    """Convert bytes to human readable format."""
    if size_bytes > 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    else:
        return f"{size_bytes / (1024 * 1024):.2f} MB"

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
    
class ProxyManager:
    def __init__(self):
        self.proxies = []
        self.current_index = 0
        
    async def fetch_proxies(self, session):
        try:
            async with session.get("https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=json") as response:
                if response.status == 200:
                    data = await response.json()
                    # Extract only the proxy URL from the response
                    self.proxies = []
                    for proxy_data in data.get("proxies", []):
                        try:
                            if isinstance(proxy_data, dict) and "proxy" in proxy_data:
                                self.proxies.append(proxy_data["proxy"])
                            elif isinstance(proxy_data, str):
                                # If it's just a string like "ip:port", format it
                                self.proxies.append(f"http://{proxy_data}")
                        except:
                            continue
                    print(f"{Fore.YELLOW}[INFO] Carregados {len(self.proxies)} proxies")
                else:
                    print(f"{Fore.RED}[ERROR] Falha ao carregar proxies: {response.status}")
        except Exception as e:
            print(f"{Fore.RED}[ERROR] Erro ao carregar proxies: {e}")
    
    def get_next_proxy(self):
        if not self.proxies:
            return None
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        # Return proxy URL as string
        return str(proxy) if proxy else None

# Create global proxy manager
proxy_manager = ProxyManager()

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

async def validate_mediafire_link(session, link):
    quick_key = extract_mediafire_key(link)
    if (quick_key):
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
            print(f"{Fore.GREEN}[VALID] MediaFire: {link} ({formatted_size})")
            return link, formatted_size
        else:
            print(f"{Fore.RED}[INVALID] MediaFire: {link}")
            return None, ""

async def get_pixeldrain_size(session, link):
    try:
        # Extrair ID do arquivo do link do Pixeldrain
        file_id = link.split("/")[-1]
        api_url = f"https://pixeldrain.com/api/file/{file_id}/info"
        
        async with session.get(api_url, headers=HEADERS) as response:
            if response.status == 200:
                data = await response.json()
                if "size" in data:
                    return format_size(int(data["size"]))
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Falha ao obter tamanho Pixeldrain: {e}")
    return ""

async def get_qiwi_size(session, url):
    try:
        async with session.get(url, headers=HEADERS, allow_redirects=True, ssl=False) as response:
            if response.status == 200:
                text = await response.text()
                # Procurar pelo padrão de tamanho no HTML
                size_match = re.search(r'">Size:</td>\s*<td[^>]*>([^<]+)</td>', text)
                if size_match:
                    return size_match.group(1).strip()
                # Padrão alternativo
                size_match = re.search(r'(\d+(?:\.\d+)?\s*(?:GB|MB|KB))', text)
                if size_match:
                    return size_match.group(1)
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Falha ao obter tamanho Qiwi: {e}")
    return ""

async def validate_qiwi_link(session, link):
    try:
        async with session.get(
            link,
            headers={"User-Agent": "Mozilla/5.0"},
            allow_redirects=True,
            ssl=False
        ) as response:
            if response.status == 200:
                size = await get_qiwi_size(session, link)
                print(f"{Fore.GREEN}[VALID] Qiwi: {link} ({size})")
                return link, size
            else:
                print(f"{Fore.RED}[INVALID] Qiwi: {link}")
                return None, ""
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Qiwi: {link}: {e}")
        return None, ""

async def validate_pixeldrain_link(session, link):
    try:
        async with session.get(link, headers=HEADERS) as response:
            if response.status == 200:
                size = await get_pixeldrain_size(session, link)
                print(f"{Fore.GREEN}[VALID] Pixeldrain: {link} ({size})")
                return link, size
            else:
                print(f"{Fore.RED}[INVALID] Pixeldrain: {link}")
                return None, ""
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Pixeldrain: {link}: {e}")
        return None, ""

async def validate_single_link(session, link, semaphore):
    try:
        async with semaphore:
            if "mediafire.com" in link.lower():
                return await validate_mediafire_link(session, link)
            elif "qiwi.gg" in link.lower():
                return await validate_qiwi_link(session, link)
            elif "pixeldrain.com" in link.lower():
                return await validate_pixeldrain_link(session, link)

            # Fallback para outros hosts
            timeout = aiohttp.ClientTimeout(total=30)
            async with session.get(link, headers=HEADERS, timeout=timeout) as response:
                if response.status != 200:
                    print(f"{Fore.RED}[INVALID] Other: {link} (Status {response.status})")
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
                    print(f"{Fore.RED}[INVALID] Other: {link} (Indicador de erro detectado)")
                    return None, ""
                    
                print(f"{Fore.GREEN}[VALID] Other: {link}")
                return link, ""

    except Exception as e:
        print(f"{Fore.RED}[ERROR] Other: {link}: {e}")
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

class ProgressTracker:
    def __init__(self, total_games):
        self.total_games = total_games
        self.processed_games = 0
        self.valid_links = 0
        self.invalid_links = 0
        
    def update(self, valid=True):
        self.processed_games += 1
        if valid:
            self.valid_links += 1
        else:
            self.invalid_links += 1
            
    def print_progress(self):
        percentage = (self.processed_games / self.total_games) * 100
        print(f"\r{Fore.CYAN}Progresso: {percentage:.1f}% | "
              f"Jogos: {self.processed_games}/{self.total_games} | "
              f"Links válidos: {self.valid_links} | "
              f"Links inválidos: {self.invalid_links}", end="")

async def validate_all_links():
    try:
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        # Create session with longer timeout and keep-alive
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        conn = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
        async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
            # Fetch proxies before starting validation
            await proxy_manager.fetch_proxies(session)
            
            data = await fetch_json(session)
            if not data or "downloads" not in data:
                print(f"{Fore.RED}JSON inválido")
                return

            total_games = len(data["downloads"])
            progress = ProgressTracker(total_games)
            print(f"\n{Fore.YELLOW}Iniciando validação de {total_games} jogos...")
            
            for game in data["downloads"]:
                uris = [uri for uri in game.get("uris", []) if uri is not None]
                results = await asyncio.gather(*(validate_single_link(session, uri, semaphore) for uri in uris))
                
                valid_results = []
                for result in results:
                    if result and isinstance(result, tuple) and len(result) == 2 and result[0]:
                        valid_results.append(result)
                        progress.update(valid=True)
                    else:
                        progress.update(valid=False)
                
                game["uris"] = [link for link, _ in valid_results]
                valid_sizes = [size for _, size in valid_results if size]
                game["fileSize"] = max(valid_sizes, default="") if valid_sizes else ""
                
                progress.print_progress()

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
