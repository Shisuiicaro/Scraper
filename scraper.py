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
    "simulation-game/", "strategy-games/", "sci-fi-games/", "adult/"
]]

REMOTE_JSON_URL = "https://raw.githubusercontent.com/Shisuiicaro/source/main/shisuyssource.json"
MAX_GAMES = 999999
CONCURRENT_REQUESTS = 100
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
        async with session.get(REMOTE_JSON_URL) as response:
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
            if a_tag:
                tasks.append(process_game(session, a_tag['href'], semaphore, latest_date))

    game_results = await asyncio.gather(*tasks)
    for game in game_results:
        if game and processed_games_count < MAX_GAMES:
            title, links, size, upload_date = game
            norm_title = normalize_title(title)
            new_game_date = datetime.fromisoformat(upload_date)

            # Verifica no JSON existente se já há um jogo com o mesmo título normalizado
            existing_indices = [
                i for i, g in enumerate(existing_data["downloads"])
                if normalize_title(g["title"]) == norm_title
            ]
            if existing_indices:
                existing_game = existing_data["downloads"][existing_indices[0]]
                try:
                    existing_date = datetime.fromisoformat(existing_game["uploadDate"])
                except Exception:
                    existing_date = None

                # Se o jogo novo não for mais recente, pula sua adição
                if existing_date and new_game_date <= existing_date:
                    continue
                else:
                    # Remove todas as entradas antigas com o mesmo título
                    for index in sorted(existing_indices, reverse=True):
                        del existing_data["downloads"][index]

            # Verifica duplicatas entre os novos jogos já coletados
            new_existing = next((g for g in new_games if normalize_title(g["title"]) == norm_title), None)
            if new_existing:
                try:
                    existing_new_date = datetime.fromisoformat(new_existing["uploadDate"])
                except Exception:
                    existing_new_date = None
                # Se o jogo já coletado for mais recente ou igual, pula a adição
                if existing_new_date and new_game_date <= existing_new_date:
                    continue
                else:
                    # Remove a versão antiga dos novos jogos
                    new_games.remove(new_existing)

            new_game_entry = {
                "title": title,
                "uris": links,
                "fileSize": size,
                "uploadDate": upload_date
            }
            existing_data["downloads"].append(new_game_entry)
            new_games.append(new_game_entry)
            processed_games_count += 1
            has_new_games = True
            print(f"{Fore.GREEN}[NEW] {title} - Total: {processed_games_count}")

    return has_new_games

async def process_game(session, game_url, semaphore, latest_date):
    page_content = await fetch_page(session, game_url, semaphore)
    if not page_content:
        return None

    soup = BeautifulSoup(page_content, 'html.parser')
    title_tag = soup.find('h1', class_='entry-title')
    if not title_tag:
        return None
    title = title_tag.get_text(strip=True)

    date_element = soup.select_one('.time-article.updated a')
    upload_date = parse_relative_date(date_element.text.strip()) if date_element else datetime.now().isoformat()
    
    if latest_date and datetime.fromisoformat(upload_date) <= latest_date:
        return None

    size = "Undefined"
    for pattern in [r"(\d+(\.\d+)?)\s*(GB|MB)\s+available space", r"Storage:\s*(\d+(\.\d+)?)\s*(GB|MB)"]:
        match = re.search(pattern, page_content, re.IGNORECASE)
        if match:
            size = f"{match.group(1)} {match.group(3).upper()}"
            break

    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if any(d in href for d in ["1fichier.com", "qiwi.gg", "pixeldrain.com"]):
            links.append(href)

    # Se não houver links suficientes ou se for somente o link do 1fichier, ignora
    if not links or (len(links) == 1 and "1fichier.com" in links[0]):
        return None

    return (title, links, size, upload_date)

async def fetch_page(session, url, semaphore):
    async with semaphore:
        try:
            async with session.get(url, headers=HEADERS, timeout=30) as response:
                return await response.text() if response.status == 200 else None
        except Exception as e:
            # Você pode descomentar a linha abaixo para debug
            # print(f"{Fore.RED}Error fetching {url}: {str(e)}")
            return None

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

        await validate_links(session, new_games)
        
        save_data("shisuyssource.json", existing_data)

async def validate_single_link(session, link, semaphore, game_title):
    try:
        async with semaphore:
            timeout = aiohttp.ClientTimeout(total=30)
            
            if "pixeldrain.com" in link:
                file_id = link.split('/')[-1]
                api_url = f"https://pixeldrain.com/api/file/{file_id}/info"
                try:
                    async with session.get(api_url, headers=HEADERS, timeout=timeout) as response:
                        if response.status == 200:
                            json_data = await response.json()
                            if json_data.get('name', '').lower().endswith(('.torrent', '.magnet')):
                                print(f"{Fore.RED}[TORRENT DETECTED] {game_title}: {link}")
                                return (None, None)
                            if 'size' in json_data:
                                size_bytes = int(json_data['size'])
                                if size_bytes > 1073741824:
                                    file_size = f"{size_bytes / 1073741824:.2f} GB"
                                else:
                                    file_size = f"{size_bytes / 1048576:.2f} MB"
                                print(f"{Fore.GREEN}[VALID - Size: {file_size}] {game_title} - pixeldrain: {link}")
                                return (link, file_size)
                except Exception as e:
                    print(f"{Fore.YELLOW}[DEBUG] Pixeldrain API error: {str(e)}")
            
            async with session.get(link, headers=HEADERS, timeout=timeout) as response:
                if response.status != 200:
                    print(f"{Fore.RED}[INVALID] {game_title} - Status {response.status}: {link}")
                    return (None, None)
                
                result = await response.text()
                if any(text in result.lower() for text in [
                    "file could not be found",
                    "unavailable for legal reasons",
                    "unavailable",
                    "qbittorrent",
                    "torrent",
                    "magnet:",
                    ".torrent"
                ]):
                    print(f"{Fore.RED}[INVALID/TORRENT] {game_title}: {link}")
                    return (None, None)

                file_size = None
                if "qiwi.gg" in link:
                    download_span = BeautifulSoup(result, 'html.parser').find('span', string=re.compile(r'Download \d+'))
                    if download_span:
                        size_match = re.search(r'(\d+\.?\d*)\s*(GB|MB|KB)', download_span.text)
                        if size_match:
                            file_size = f"{size_match.group(1)} {size_match.group(2)}"

                domain = "1fichier" if "1fichier.com" in link else "qiwi" if "qiwi.gg" in link else "pixeldrain"
                size_info = f" - Size: {file_size}" if file_size else ""
                print(f"{Fore.GREEN}[VALID{size_info}] {game_title} - {domain}: {link}")
                return (link, file_size)

    except Exception as e:
        print(f"{Fore.RED}[ERROR] {game_title} - {link}: {str(e)}")
        return (None, None)

async def validate_links(session, games):
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    print(f"\n{Fore.YELLOW}Starting link validation (somente novos jogos)...{Fore.RESET}")
    
    games_to_keep = []
    total_links = sum(len(game["uris"]) for game in games)
    validated = 0
    
    for game in games:
        if not game["uris"]:
            continue
            
        print(f"\n{Fore.CYAN}Validating: {game['title']}{Fore.RESET}")
        tasks = [validate_single_link(session, link, semaphore, game['title']) for link in game["uris"]]
        results = await asyncio.gather(*tasks)
        
        valid_links = []
        sizes = []
        
        for link, size in results:
            if link:
                valid_links.append(link)
                if size:
                    sizes.append(size)
        
        # Se após a validação houver links válidos e não for somente o link do 1fichier
        if valid_links and not (len(valid_links) == 1 and "1fichier.com" in valid_links[0]):
            game["uris"] = valid_links
            if sizes:
                # Seleciona o tamanho máximo dentre os disponíveis
                max_size = max(sizes, key=lambda x: float(x.split()[0]) * (1024 if x.upper().endswith('GB') else 1))
                game["fileSize"] = max_size
                print(f"{Fore.BLUE}[SIZE UPDATE] {game['title']} - Set to {max_size}")
            games_to_keep.append(game)
            validated += len(valid_links)
        else:
            print(f"{Fore.RED}[REMOVED] {game['title']}")
        
        print(f"Progress: {validated}/{total_links} links checked")
    
    games[:] = games_to_keep
    print(f"\n{Fore.GREEN}Validation completed: {validated} valid links found")
    print(f"Games remaining after validation: {len(games_to_keep)}")

def main():
    asyncio.run(scrape_games())

if __name__ == "__main__":
    main()
