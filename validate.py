import aiohttp
import asyncio
import json
import requests
import re  # Add missing import
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

def check_gofile_link(link):
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
            
        # Check if the page has content or shows errors
        error_texts = [
            "File not found",
            "404",
            "Access denied",
            "File has been deleted"
        ]
        
        page_source = driver.page_source.lower()
        if any(error in page_source.lower() for error in error_texts):
            return None
            
        # Try to get file size from page
        try:
            size_element = WebDriverWait(driver, 3).until(
                lambda d: d.find_element("class name", "file-size")
            )
            file_size = size_element.text
        except:
            file_size = ""
            
        return link, file_size
        
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Falha ao validar {link}: {e}")
        return None
    finally:
        driver_pool.return_driver(driver)

async def validate_gofile_link(session, link):
    try:
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=3) as executor:
            result = await loop.run_in_executor(executor, check_gofile_link, link)
            
            if result:
                link, size = result
                print(f"{Fore.GREEN}[VALID] {link} ({size})")
                return link, size
            else:
                print(f"{Fore.RED}[INVALID] {link}")
                return None, ""
                
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Falha ao validar {link}: {e}")
        return None, ""

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
