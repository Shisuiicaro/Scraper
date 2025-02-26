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

REGEX_TITLE = r"(?:\(.*?\)|\s*(Free Download|v\d+(\.\d+)*[a-zA-Z0-9\-]*|Build \d+|P2P|GOG|Repack|Edition.*|FLT|TENOKE)\s*)"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

processed_games_count = 0

def normalize_title(title):
    return re.sub(REGEX_TITLE, "", title).strip()

async def load_existing_data(session):
    try:
        async with session.get(REMOTE_JSON_URL, headers=HEADERS) as response:
            text = await response.text()
            try:
                return json.loads(text)
            except json.JSONDecodeError as e:
                print(f"{Fore.RED}JSON decoding error: {e}")
                return {"name": "Shisuy's source", "downloads": []}
    except Exception as e:
        print(f"{Fore.RED}Connection error: {e}")
        return {"name": "Shisuy's source", "downloads": []}

def get_latest_date(existing_data):
    latest_date = None
    for game in existing_data.get("downloads", []):
        date_str = game.get("uploadDate")
        if date_str:
            try:
                game_date = datetime.fromisoformat(date_str)
                if not latest_date or game_date > latest_date:
                    latest_date = game_date
            except ValueError:
                continue
    return latest_date

def save_data(json_filename, data):
    with open(json_filename, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

def parse_relative_date(date_str):
    now = datetime.now()
    try:
        num = int(re.search(r'(\d+)', date_str).group(1))
        if "hour" in date_str:
            return (now - timedelta(hours=num)).isoformat()
        elif "day" in date_str:
            return (now - timedelta(days=num)).isoformat()
        elif "week" in date_str:
            return (now - timedelta(weeks=num)).isoformat()
        elif "month" in date_str:
            return (now - timedelta(days=30*num)).isoformat()
        elif "year" in date_str:
            return (now - timedelta(days=365*num)).isoformat()
        return now.isoformat()
    except:
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
            async with session.get(url, headers=HEADERS, timeout=30) as response:
                return await response.text() if response.status == 200 else None
        except Exception:
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
        return None

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


    if not links or (len(links) == 1 and "1fichier.com" in links[0]):
        return None

    return (title, links, size, upload_date)

async def process_page(session, page_url, semaphore, existing_data, latest_date, new_games):
    global processed_games_count
    page_content = await fetch_page(session, page_url, semaphore)
    if not page_content:
        return False

    soup = BeautifulSoup(page_content, 'html.parser')
    articles = soup.find_all('div', class_='articles-content')
    if not articles:
        return True

    has_new_games = False
    tasks = []
    for article in articles:
        for li in article.find_all('li'):
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

        title_normalized = normalize_title(title)
        same_games = [g for g in existing_data["downloads"] if normalize_title(g["title"]) == title_normalized]
        
        if same_games:
            same_games.sort(key=lambda x: (is_deadcode_version(x["title"]), x.get("uploadDate", "")), reverse=True)
            most_recent = same_games[0]
            
            should_update = (
                (is_deadcode_version(title) and not is_deadcode_version(most_recent["title"])) or
                (upload_date and upload_date > most_recent.get("uploadDate", ""))
            )
            
            if should_update:
                most_recent.update({
                    "title": title,
                    "uris": links,
                    "fileSize": "",
                    "uploadDate": upload_date
                })
                log_game_status("UPDATED", page_num, title)
                
                existing_data["downloads"] = [g for g in existing_data["downloads"] 
                                           if normalize_title(g["title"]) != title_normalized or g == most_recent]
            else:
                log_game_status("IGNORED", page_num, title)
        else:
            existing_data["downloads"].append({

                "title": title,
                "uris": links,
                "fileSize": "",
                "uploadDate": upload_date
            }
            existing_data["downloads"].append(new_game_entry)
            new_games.append(new_game_entry)
            processed_games_count += 1
            has_new_games = True
            print(f"{Fore.GREEN}[NEW] {title} - Total: {processed_games_count}")


async def cleanup():
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def scrape_games():
    global processed_games_count
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession() as session:
        existing_data = await load_existing_data(session)
        latest_date = get_latest_date(existing_data)
        print(f"{Fore.CYAN}Latest known game date: {latest_date}")
        
        new_games = []

        for base_url in BASE_URLS:
            if processed_games_count >= MAX_GAMES:
                break
                
            print(f"{Fore.MAGENTA}\nProcessing category: {base_url}")
            page_num = 1
            while True:
                page_url = f"{base_url}page/{page_num}/" if page_num > 1 else base_url
                should_continue = await process_page(session, page_url, semaphore, existing_data, latest_date, new_games)
                if not should_continue or processed_games_count >= MAX_GAMES:
                    break
                page_num += 1

            save_data(JSON_FILENAME, existing_data)
            print(f"\nScraping finished. Total games processed: {processed_games_count}")
    
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        await cleanup()


def main():
    asyncio.run(scrape_games())

if __name__ == "__main__":
    main()
