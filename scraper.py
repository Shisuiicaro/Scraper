import cloudscraper
import asyncio
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import re
from colorama import Fore, init

init(autoreset=True)

# Reorganizar BASE_URLS para colocar latest-updates primeiro
BASE_URLS = ["https://repack-games.com/category/latest-updates/"] + [
    "https://repack-games.com/category/" + url for url in [
        "action-games/", "anime-games/", "adventure-games/",
        "building-games/", "exploration/", "multiplayer-games/", "open-world-game/",
        "fighting-games/", "horror-games/", "racing-game/", "shooting-games/",
        "rpg-pc-games/", "puzzle/", "sport-game/", "survival-games/",
        "simulation-game/", "strategy-games/", "sci-fi-games/", "emulator-games/", 
        "vr-games/", "nudity/"
    ]
]

JSON_FILENAME = "shisuyssource.json"
INVALID_JSON_FILENAME = "invalid_games.json"
MAX_GAMES = 100000000
CONCURRENT_REQUESTS = 6000
REGEX_TITLE = r"(?:\(.*?\)|\s*(Free Download|v\d+(\.\d+)*[a-zA-Z0-9\-]*|Build \d+|P2P|GOG|Repack|Edition.*|FLT|TENOKE)\s*)"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://repack-games.com/"  # Adicionado para simular navegação
}

processed_games_count = 0

class GameLimitReached(Exception):
    pass

def normalize_title(title):
    return re.sub(REGEX_TITLE, "", title).strip()

def save_data(json_filename, data):
    with open(json_filename, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

def parse_relative_date(date_str):
    now = datetime.now()
    try:
        if "hour" in date_str:
            result_date = now - timedelta(hours=int(re.search(r'(\d+)', date_str).group(1)))
        elif "day" in date_str:
            result_date = now - timedelta(days=int(re.search(r'(\d+)', date_str).group(1)))
        elif "week" in date_str:
            result_date = now - timedelta(weeks=int(re.search(r'(\d+)', date_str).group(1)))
        elif "month" in date_str:
            result_date = now - timedelta(days=30 * int(re.search(r'(\d+)', date_str).group(1)))
        elif "year" in date_str:
            result_date = now - timedelta(days=365 * int(re.search(r'(\d+)', date_str).group(1)))
        else:
            return now.isoformat()
        return result_date.isoformat()
    except Exception:
        return now.isoformat()

def log_game_status(status, page, game_title):
    global processed_games_count
    if status == "NEW":
        processed_games_count += 1
        print(f"{Fore.GREEN}[NEW GAME] Page {page}: {game_title} - Games Processed: {processed_games_count}")
    elif status == "UPDATED":
        print(f"{Fore.YELLOW}[UPDATED] Page {page}: {game_title}")
    elif status == "IGNORED":
        print(f"{Fore.CYAN}[IGNORED] Page {page}: {game_title}")
    elif status == "NO_LINKS":
        print(f"{Fore.RED}[NO LINKS] Page {page}: {game_title}")

def load_invalid_games():
    try:
        with open(INVALID_JSON_FILENAME, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"updated": datetime.now().isoformat(), "invalid_games": []}

def save_invalid_game(title, reason, links=None):
    invalid_data = load_invalid_games()
    invalid_data["updated"] = datetime.now().isoformat()
    invalid_game = {"title": title, "reason": reason, "date": datetime.now().isoformat()}
    if links:
        invalid_game["links"] = links
    invalid_data["invalid_games"].append(invalid_game)
    with open(INVALID_JSON_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(invalid_data, f, ensure_ascii=False, indent=4)

async def fetch_page(scraper, url, retries=3):
    """Fetch a page with retries in case of temporary failures."""
    for attempt in range(retries):
        try:
            response = scraper.get(url, headers=HEADERS, timeout=10)  # Reduzido timeout para 10 segundos
            if response.status_code == 200:
                return response.text
            print(f"Attempt {attempt + 1} failed for {url} with status {response.status_code}")
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
        await asyncio.sleep(2 ** attempt)  # Exponential backoff
    print(f"Failed to fetch {url} after {retries} retries")
    return None

def mark_special_categories(title, url):
    if "emulator-games" in url.lower() and not any(x in title.lower() for x in ["emulator", "emu", "(emu)"]):
        title = f"{title} (Emulator)"
    if "multiplayer-games" in url.lower() and not any(x in title.lower() for x in ["multiplayer", "multi", "(mp)"]):
        title = f"{title} (Multiplayer)"
    if "vr-games" in url.lower() and not any(x in title.lower() for x in ["vr", "(vr)", "virtual reality"]):
        title = f"{title} (VR)"
    return title

def normalize_special_titles(title):
    if "The Headliners" in title:
        title = title.replace("The Headliners", "Headliners")
    if "0xdeadcode" in title:
        title = title.replace("0xdeadcode", "Multiplayer")
    if "0xdeadc0de" in title:
        title = title.replace("0xdeadc0de", "Multiplayer")       
    return title

def is_valid_qiwi_link(link):
    return "qiwi.gg" in link and "/file/" in link and "/folder/" not in link

def is_deadcode_version(title):
    title_lower = title.lower()
    return "0xdeadcode" in title_lower or "0xdeadc0de" in title_lower

def find_duplicate_game(data, repack_link_source):
    """Verifica se existe um jogo duplicado pelo link da página."""
    for i, game in enumerate(data["downloads"]):
        # Ignorar imediatamente se o link for exatamente igual
        if game.get("repackLinkSource") == repack_link_source:
            return i, game, "IGNORE"
    return None, None, "NEW"

def is_valid_datanodes_link(link):
    """Verifica se o link é válido para datanodes.to."""
    return "datanodes.to" in link  # Removida a verificação de '/file/'

async def fetch_game_details(scraper, game_url):
    page_content = await fetch_page(scraper, game_url)
    if not page_content:
        return None, None, [], None, None

    soup = BeautifulSoup(page_content, 'html.parser')
    title = soup.find('h1', class_='entry-title').get_text(strip=True) if soup.find('h1', class_='entry-title') else "Unknown Title"
    
    title = mark_special_categories(title, game_url)

    date_element = soup.select_one('.time-article.updated a')
    if date_element and date_element.text.strip():
        relative_date_str = date_element.text.strip()
        upload_date = parse_relative_date(relative_date_str)
    else:
        upload_date = None

    file_size = None
    size_element = soup.find(string=re.compile(r"(\d+(\.\d+)?\s*(GB|MB))", re.IGNORECASE))
    if size_element:
        file_size = size_element.strip()

    all_links = []
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        if "1fichier.com" in href or "pixeldrain.com" in href or "mediafire.com" in href or "datanodes.to" in href:
            all_links.append(href)
        elif "qiwi.gg" in href and is_valid_qiwi_link(href):
            all_links.append(href)

    # Ordenar os links de acordo com a hierarquia
    priority_order = ["1fichier", "datanodes", "mediafire", "qiwi", "pixeldrain"]
    filtered_links = {key: None for key in priority_order}

    for link in all_links:
        if "1fichier.com" in link:
            filtered_links["1fichier"] = link
        elif "datanodes.to" in link:
            filtered_links["datanodes"] = link
        elif "mediafire.com" in link:
            filtered_links["mediafire"] = link
        elif "qiwi.gg" in link:
            filtered_links["qiwi"] = link
        elif "pixeldrain.com" in link:
            filtered_links["pixeldrain"] = link

    # Remover entradas vazias e manter a ordem
    download_links = [filtered_links[key] for key in priority_order if filtered_links[key] is not None]

    return title, file_size, download_links, upload_date, game_url

async def fetch_last_page_num(scraper, base_url):
    page_content = await fetch_page(scraper, base_url)
    if not page_content:
        return 1

    soup = BeautifulSoup(page_content, 'html.parser')
    last_page_tag = soup.find('a', class_='last', string='Last »')
    if last_page_tag:
        match = re.search(r'page/(\d+)', last_page_tag['href'])
        if match:
            return int(match.group(1))
    return 1

async def process_page(scraper, page_url, data, page_num, retry_queue, existing_links):
    global processed_games_count
    if processed_games_count >= MAX_GAMES:
        raise GameLimitReached()

    page_content = await fetch_page(scraper, page_url)
    if not page_content:
        return

    soup = BeautifulSoup(page_content, 'html.parser')
    articles = soup.find_all('div', class_='articles-content')
    if not articles:
        return

    remaining_games = MAX_GAMES - processed_games_count
    tasks = []
    
    for article in articles:
        if len(tasks) >= remaining_games:
            break
        
        for li in article.find_all('li'):
            if len(tasks) >= remaining_games:
                break
                
            a_tag = li.find('a', href=True)
            if a_tag and 'href' in a_tag.attrs:
                game_url = a_tag['href']
                # Pular jogos já presentes no JSON
                if game_url in existing_links:
                    print(f"{Fore.CYAN}[SKIPPED] Page {page_num}: {game_url} already in JSON")
                    continue
                tasks.append(fetch_game_details(scraper, game_url))

    games = await asyncio.gather(*tasks, return_exceptions=True)
    for game in games:
        if isinstance(game, Exception):
            print(f"{Fore.RED}Exception occurred while fetching game details: {game}")
            continue
            
        if game is None or not isinstance(game, tuple) or len(game) != 5:
            print(f"{Fore.RED}Invalid game data received")
            continue

        if processed_games_count >= MAX_GAMES:
            break

        title, _, links, upload_date, repack_link_source = game

        if not title:  # Add check for None/empty title
            print(f"{Fore.RED}[ERROR] Game with empty title skipped")
            continue

        # Verificar duplicatas pelo link imediatamente
        duplicate_index, existing_game, action = find_duplicate_game(data, repack_link_source)
        
        if action == "IGNORE":
            log_game_status("IGNORED", page_num, title)
            continue

        title = normalize_special_titles(title)
        if not links:
            log_game_status("NO_LINKS", page_num, title)
            continue

        if "FULL UNLOCKED" in title.upper() or "CRACKSTATUS" in title.upper():
            save_invalid_game(title, "Ignored title pattern")
            print(f"Ignoring game with title: {title}")
            continue

        # Adicionar novo jogo
        data["downloads"].append({
            "title": title,
            "uris": links,
            "fileSize": "",
            "uploadDate": upload_date,
            "repackLinkSource": repack_link_source
        })
        log_game_status("NEW", page_num, title)

    # Adicionar jogos que falharam para a fila de retries
    for game in games:
        if isinstance(game, Exception) or game is None:
            retry_queue.append(page_url)

async def retry_failed_games(scraper, retry_queue, data):
    """Processa novamente os jogos que falharam."""
    while retry_queue:
        page_url = retry_queue.pop(0)
        print(f"{Fore.YELLOW}Retrying failed game: {page_url}")
        try:
            await process_page(scraper, page_url, data, page_num=0, retry_queue=[])
        except Exception as e:
            print(f"{Fore.RED}Retry failed for {page_url}: {e}")

async def process_category(scraper, base_url, data, page_semaphore, game_semaphore, existing_links):
    global processed_games_count
    retry_queue = []  # Fila para jogos que falharam

    if processed_games_count >= MAX_GAMES:
        return

    try:
        last_page_num = await fetch_last_page_num(scraper, base_url)
        pages = list(range(1, last_page_num + 1))
        
        print(f"\nProcessing category: {base_url}")
        print(f"Total pages to process: {len(pages)}")
        
        # Processa páginas em paralelo
        for i in range(0, len(pages), PAGE_SEMAPHORE_LIMIT):
            if processed_games_count >= MAX_GAMES:
                break
                
            batch = pages[i:i + PAGE_SEMAPHORE_LIMIT]
            tasks = []
            
            for page_num in batch:
                page_url = f"{base_url}/page/{page_num}"
                tasks.append(process_page(scraper, page_url, data, page_num, retry_queue, existing_links))  # Adicionado retry_queue
            
            if tasks:
                await asyncio.gather(*tasks)
                
            print(f"Processed pages {i+1} to {min(i+PAGE_SEMAPHORE_LIMIT, len(pages))} of {len(pages)}")

        # Processar jogos que falharam
        await retry_failed_games(scraper, retry_queue, data)

    except GameLimitReached:
        return
    except Exception as e:
        print(f"Error processing category {base_url}: {str(e)}")

async def cleanup():
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# Ajustar semáforos para melhor performance
CATEGORY_SEMAPHORE_LIMIT = 1  # Reduzido para processar uma categoria por vez
PAGE_SEMAPHORE_LIMIT = 5     # Reduzido para processar menos páginas em paralelo
GAME_SEMAPHORE_LIMIT = 10    # Reduzido para maior estabilidade

def load_existing_data(json_filename):
    """Carrega o JSON existente do arquivo local."""
    try:
        with open(json_filename, "r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"name": "Shisuy's source", "downloads": []}

def load_existing_links(json_filename):
    """Carrega os links existentes do JSON para evitar duplicatas."""
    try:
        with open(json_filename, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
            return {game.get("repackLinkSource") for game in data.get("downloads", []) if game.get("repackLinkSource")}
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

async def scrape_games():
    global processed_games_count
    
    # Semáforos para controle de concorrência
    category_semaphore = asyncio.Semaphore(CATEGORY_SEMAPHORE_LIMIT)
    page_semaphore = asyncio.Semaphore(PAGE_SEMAPHORE_LIMIT)
    game_semaphore = asyncio.Semaphore(GAME_SEMAPHORE_LIMIT)
    
    # Carregar dados existentes do JSON
    data = load_existing_data(JSON_FILENAME)
    existing_links = load_existing_links(JSON_FILENAME)  # Carregar links existentes

    try:
        scraper = cloudscraper.create_scraper()  # Substitui o ClientSession do aiohttp
        category_tasks = []
        
        # Processa categorias em paralelo
        for i in range(0, len(BASE_URLS), CATEGORY_SEMAPHORE_LIMIT):
            if processed_games_count >= MAX_GAMES:
                break
                
            batch = BASE_URLS[i:i + CATEGORY_SEMAPHORE_LIMIT]
            tasks = []
            
            for base_url in batch:
                async with category_semaphore:
                    tasks.append(
                        process_category(
                            scraper,
                            base_url,
                            data,
                            page_semaphore=None,  # Semáforos não são mais necessários
                            game_semaphore=None,
                            existing_links=existing_links  # Passar links existentes
                        )
                    )
            
            if tasks:
                await asyncio.gather(*tasks)
                    
            save_data(JSON_FILENAME, data)
            print(f"\nScraping finished. Total games processed: {processed_games_count}")
    
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        await cleanup()

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(scrape_games())
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        
        try:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
            
        loop.close()
        print("Script terminated.")
        
if __name__ == "__main__":
    main()