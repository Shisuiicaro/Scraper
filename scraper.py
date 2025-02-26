import aiohttp
import asyncio
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import re
from colorama import Fore, init

init(autoreset=True)

BASE_URLS = ["https://repack-games.com/category/" + url for url in [
    "latest-updates/", "action-games/", "anime-games/", "adventure-games/",
    "building-games/", "exploration/", "multiplayer-games/", "open-world-game/",
    "fighting-games/", "horror-games/", "racing-game/", "shooting-games/",
    "rpg-pc-games/", "puzzle/", "sport-game/", "survival-games/",
    "simulation-game/", "strategy-games/", "sci-fi-games/", "emulator-games/", "vr-games/"
    "nudity/"
]]

JSON_FILENAME = "shisuyssource.json"
INVALID_JSON_FILENAME = "invalid_games.json"
MAX_GAMES = 99999999
CONCURRENT_REQUESTS = 2000
REGEX_TITLE = r"(?:\(.*?\)|\s*(Free Download|v\d+(\.\d+)*[a-zA-Z0-9\-]*|Build \d+|P2P|GOG|Repack|Edition.*|FLT|TENOKE)\s*)"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
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

async def fetch_page(session, url, semaphore):
    async with semaphore:
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with session.get(url, headers=HEADERS, timeout=timeout) as response:
                if response.status == 200:
                    return await response.text()
                return None
        except Exception as e:
            print(f"Error fetching {url}: {str(e)}")
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
    if title == "The Headliners":
        return "Headliners"
    if "0xdeadcode" in title:
        return title.replace("0xdeadcode", "Multiplayer")
    return title

def is_deadcode_version(title):
    return "0xdeadcode" in title.lower()

async def fetch_game_details(session, game_url, semaphore):
    page_content = await fetch_page(session, game_url, semaphore)
    if not page_content:
        return None, None, [], None

    soup = BeautifulSoup(page_content, 'html.parser')
    title = soup.find('h1', class_='entry-title').get_text(strip=True) if soup.find('h1', class_='entry-title') else "Unknown Title"
    
    title = mark_special_categories(title, game_url)

    date_element = soup.select_one('.time-article.updated a')
    if date_element and date_element.text.strip():
        relative_date_str = date_element.text.strip()
        upload_date = parse_relative_date(relative_date_str)
    else:
        upload_date = None

    all_links = []
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        if any(domain in href for domain in ["1fichier.com", "qiwi.gg", "pixeldrain.com", "mediafire.com", "gofile.io"]):
            all_links.append(href)

    filtered_links = {}
    for link in all_links:
        domain = None
        if "1fichier.com" in link:
            domain = "1fichier"
        elif "qiwi.gg" in link:
            domain = "qiwi"
        elif "pixeldrain.com" in link:
            domain = "pixeldrain"
        elif "mediafire.com" in link:
            domain = "mediafire"
        elif "gofile.io" in link:
            domain = "gofile"

        if domain and domain not in filtered_links:
            filtered_links[domain] = link

    download_links = list(filtered_links.values())

    return title, "", download_links, upload_date

async def fetch_last_page_num(session, semaphore, base_url):
    page_content = await fetch_page(session, base_url, semaphore)
    if not page_content:
        return 1

    soup = BeautifulSoup(page_content, 'html.parser')
    last_page_tag = soup.find('a', class_='last', string='Last »')
    if last_page_tag:
        match = re.search(r'page/(\d+)', last_page_tag['href'])
        if match:
            return int(match.group(1))
    return 1

async def process_page(session, page_url, semaphore, data, page_num):
    global processed_games_count
    if processed_games_count >= MAX_GAMES:
        raise GameLimitReached()

    page_content = await fetch_page(session, page_url, semaphore)
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
                tasks.append(fetch_game_details(session, a_tag['href'], semaphore))

    games = await asyncio.gather(*tasks, return_exceptions=True)
    for game in games:
        if isinstance(game, Exception):
            print(f"{Fore.RED}Exception occurred while fetching game details: {game}")
            continue
            
        if game is None or not isinstance(game, tuple) or len(game) != 4:
            print(f"{Fore.RED}Invalid game data received")
            continue

        if processed_games_count >= MAX_GAMES:
            break

        title, size, links, upload_date = game

        if not title:  # Add check for None/empty title
            print(f"{Fore.RED}[ERROR] Game with empty title skipped")
            continue

        title = normalize_special_titles(title)
        if not links:
            log_game_status("NO_LINKS", page_num, title)
            continue

        if "FULL UNLOCKED" in title.upper() or "CRACKSTATUS" in title.upper():
            save_invalid_game(title, "Ignored title pattern")
            print(f"Ignoring game with title: {title}")
            continue

        # Add game directly without checking for duplicates
        data["downloads"].append({
            "title": title,
            "uris": links,
            "fileSize": "",
            "uploadDate": upload_date
        })
        log_game_status("NEW", page_num, title)

async def cleanup():
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# Ajuste estes valores conforme necessário
CATEGORY_SEMAPHORE_LIMIT = 3  # Número de categorias em paralelo
PAGE_SEMAPHORE_LIMIT = 30      # Número de páginas por categoria em paralelo
GAME_SEMAPHORE_LIMIT = 60     # Número de jogos em paralelo por página

async def process_category(session, base_url, data, page_semaphore, game_semaphore):
    global processed_games_count
    
    if processed_games_count >= MAX_GAMES:
        return

    try:
        last_page_num = await fetch_last_page_num(session, game_semaphore, base_url)
        page_tasks = []
        
        # Cria lista de páginas a processar
        pages = list(range(1, min(last_page_num + 1, 10)))  # Limita a 10 páginas por categoria
        
        # Processa páginas em paralelo
        for i in range(0, len(pages), PAGE_SEMAPHORE_LIMIT):
            batch = pages[i:i + PAGE_SEMAPHORE_LIMIT]
            tasks = []
            
            for page_num in batch:
                if processed_games_count >= MAX_GAMES:
                    break
                    
                page_url = f"{base_url}/page/{page_num}"
                tasks.append(process_page(session, page_url, game_semaphore, data, page_num))
            
            if tasks:
                await asyncio.gather(*tasks)
                
            if processed_games_count >= MAX_GAMES:
                break

    except GameLimitReached:
        return
    except Exception as e:
        print(f"Error processing category {base_url}: {str(e)}")

async def scrape_games():
    global processed_games_count
    
    # Semáforos para controle de concorrência
    category_semaphore = asyncio.Semaphore(CATEGORY_SEMAPHORE_LIMIT)
    page_semaphore = asyncio.Semaphore(PAGE_SEMAPHORE_LIMIT)
    game_semaphore = asyncio.Semaphore(GAME_SEMAPHORE_LIMIT)
    
    data = {
        "name": "Shisuy's source",
        "downloads": []
    }

    try:
        async with aiohttp.ClientSession() as session:
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
                                session, 
                                base_url, 
                                data, 
                                page_semaphore, 
                                game_semaphore
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