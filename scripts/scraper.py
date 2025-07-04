import cloudscraper
import asyncio
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import re
import time
import os
import random
from colorama import Fore, Style, init
from tqdm import tqdm

# Melhorias implementadas para evitar duplicações:
# 1. Uso de conjunto (set) para garantir URLs únicas em cada página
# 2. Verificação adicional antes de adicionar um jogo para evitar duplicações entre categorias
# 3. Uso de lock para proteger o acesso concorrente a existing_links e data
# 4. Proteção com lock ao salvar dados periodicamente

init(autoreset=True)

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

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_FILENAME = os.path.join(BASE_DIR, "data", "raw", "games.json")
BLACKLIST_JSON = os.path.join(BASE_DIR, "data", "config", "untracked.json")
MAX_GAMES = 1000000
CONCURRENT_REQUESTS = 260  # Reduzido para evitar bloqueios
CATEGORY_SEMAPHORE_LIMIT = 1
PAGE_SEMAPHORE_LIMIT = 10     # Reduzido para evitar muitas requisições simultâneas
GAME_SEMAPHORE_LIMIT = 85  # Reduzido para evitar muitas requisições simultâneas
MAX_RETRIES = 5  # Número máximo de tentativas para páginas com falha
REGEX_TITLE = r"(?:\(.*?\)|\s*(Free Download|v\d+(\.\d+)*[a-zA-Z0-9\-]*|Build \d+|P2P|GOG|Repack|Edition.*|FLT|TENOKE)\s*)"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://repack-games.com/"
}

processed_games_count = 0
start_time = 0
stats = {
    "new_games": 0,
    "no_links": 0,
    "ignored": 0,
    "categories_processed": 0
}

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
    global processed_games_count, stats
    if status == "NEW":
        processed_games_count += 1
        stats["new_games"] += 1
        print(f"{Fore.GREEN}[NEW GAME] {Style.BRIGHT}Page {page}{Style.RESET_ALL}: {game_title}")
    elif status == "NO_LINKS":
        stats["no_links"] += 1
        print(f"{Fore.RED}[NO LINKS] {Style.BRIGHT}Page {page}{Style.RESET_ALL}: {game_title}")
    elif status == "IGNORED":
        stats["ignored"] += 1

def print_stats(clear_screen=True):
    global start_time, stats
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if clear_screen:
        os.system('cls' if os.name == 'nt' else 'clear')
    
    print(f"\n{Style.BRIGHT}{Fore.CYAN}=== Scraper Status ==={Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Tempo decorrido: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
    print(f"{Fore.GREEN}Jogos novos: {stats['new_games']}")
    print(f"{Fore.RED}Jogos sem links: {stats['no_links']}")
    print(f"{Fore.CYAN}Jogos ignorados: {stats['ignored']}")
    print(f"{Fore.MAGENTA}Categorias processadas: {stats['categories_processed']}/{len(BASE_URLS)}")
    
    # Calcular taxa de processamento
    if elapsed_time > 0:
        games_per_hour = (stats['new_games'] + stats['no_links']) / (elapsed_time / 3600)
        print(f"{Fore.WHITE}Taxa: {games_per_hour:.1f} jogos/hora")
        
    print(f"{Style.BRIGHT}{Fore.CYAN}====================={Style.RESET_ALL}\n")

def load_blacklist():
    try:
        with open(BLACKLIST_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {game.get("repackLinkSource") for game in data.get("removed", []) if game.get("repackLinkSource")}
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def save_blacklist(blacklist):
    pass

async def fetch_page(scraper, url, retries=3):
    for attempt in range(retries):
        try:
            response = scraper.get(url, headers=HEADERS, timeout=15)
            if response.status_code == 200:
                return response.text
            elif response.status_code == 429:  # Too Many Requests
                # Espera mais tempo quando receber 429 (rate limiting)
                await asyncio.sleep(10 + 5 * attempt)
                continue
        except Exception:
            pass
        # Backoff exponencial com jitter para evitar sincronização de requisições
        await asyncio.sleep(2 ** attempt + (random.random() * 2))
    return None

def mark_special_categories(title, url):
    return title

def normalize_special_titles(title):
    if "The Headliners" in title:
        title = title.replace("The Headliners", "Headliners")
    return title

def find_duplicate_game(data, repack_link_source):
    for i, game in enumerate(data["downloads"]):
        if game.get("repackLinkSource") == repack_link_source:
            return i, game, "IGNORE"
    return None, None, "NEW"

def is_valid_datanodes_link(link):
    return "datanodes.to" in link

def extract_category_from_url(url):
    match = re.search(r'/category/([^/]+)/?', url)
    if match:
        return match.group(1)
    return "unknown"

def load_existing_links(_):
    try:
        with open(JSON_FILENAME, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
            return set(game.get("repackLinkSource") for game in data.get("downloads", []) if game.get("repackLinkSource"))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def load_existing_data(json_filename):
    try:
        with open(json_filename, "r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"name": "Shisuy's source", "downloads": []}

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

async def fetch_game_details(scraper, game_url, category):
    blacklist = load_blacklist()
    if game_url in blacklist:
        log_game_status("IGNORED", 0, game_url)
        return None

    page_content = await fetch_page(scraper, game_url)
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
        if ("gofile.io" in href or "pixeldrain.com" in href or 
            "mediafire.com" in href or "datanodes.to" in href):
            all_links.append(href)

    priority_order = ["datanodes", "gofile", "mediafire", "pixeldrain"]
    filtered_links = {key: None for key in priority_order}
    for link in all_links:
        
        if "datanodes.to" in link:
            filtered_links["datanodes"] = link
        elif "mediafire.com" in link:
            filtered_links["mediafire"] = link
        elif "gofile.io" in link:
            filtered_links["gofile"] = link
        elif "pixeldrain.com" in link:
            filtered_links["pixeldrain"] = link
    download_links = [filtered_links[key] for key in priority_order if filtered_links[key] is not None]

    return {
        "title": title,
        "uris": download_links,
        "fileSize": "",
        "uploadDate": upload_date,
        "repackLinkSource": game_url,
        "category": category
    }

async def process_page(scraper, page_url, data, page_num, retry_queue, existing_links, existing_links_lock, category):
    global processed_games_count
    if processed_games_count >= MAX_GAMES:
        raise GameLimitReached()

    blacklist = load_blacklist()
    page_content = await fetch_page(scraper, page_url)
    if not page_content:
        return

    soup = BeautifulSoup(page_content, 'html.parser')
    articles = soup.find_all('div', class_='articles-content')
    if not articles:
        return

    remaining_games = MAX_GAMES - processed_games_count
    tasks = []
    game_urls = []

    all_links_on_page = []
    for article in articles:
        for li in article.find_all('li'):
            a_tag = li.find('a', href=True)
            if a_tag and 'href' in a_tag.attrs:
                game_url = a_tag['href']
                all_links_on_page.append(game_url)

    # Usar um conjunto para garantir que não processamos URLs duplicadas na mesma página
    unique_game_urls = set(all_links_on_page)
    
    for game_url in unique_game_urls:
        if game_url in existing_links or game_url in blacklist:
            log_game_status("IGNORED", page_num, game_url)
            continue
        if len(tasks) >= remaining_games:
            break
        tasks.append(fetch_game_details(scraper, game_url, category))
        game_urls.append(game_url)
    if not tasks:
        return

    games = await asyncio.gather(*tasks, return_exceptions=True)
    for idx, game in enumerate(games):
        if isinstance(game, Exception) or not game or not isinstance(game, dict):
            continue
        if processed_games_count >= MAX_GAMES:
            break

        title = game["title"]
        links = game["uris"]
        repack_link_source = game["repackLinkSource"]

        if not title:
            continue

        title = normalize_special_titles(title)
        if not links:
            log_game_status("NO_LINKS", page_num, title)
            continue

        if "FULL UNLOCKED" in title.upper() or "CRACKSTATUS" in title.upper():
            continue
            
        # Usar lock para verificar e atualizar existing_links de forma thread-safe
        async with existing_links_lock:
            # Verificar novamente se o link já existe para evitar duplicações entre categorias
            if repack_link_source in existing_links:
                log_game_status("IGNORED", page_num, title)
                continue

            data["downloads"].append(game)
            existing_links.add(repack_link_source)
            log_game_status("NEW", page_num, title)

    for idx, game in enumerate(games):
        if isinstance(game, Exception) or game is None:
            pass

async def retry_failed_games(scraper, retry_queue, data, existing_links, existing_links_lock, category):
    if not retry_queue:
        return
        
    print(f"\n{Fore.YELLOW}Tentando recuperar {len(retry_queue)} páginas com falha...{Style.RESET_ALL}")
    retry_progress = tqdm(total=len(retry_queue), desc="Páginas com falha", unit="página", ncols=100)
    
    retry_count = 0
    max_retries = min(len(retry_queue), MAX_RETRIES)
    
    while retry_queue and retry_count < max_retries:
        page_url = retry_queue.pop(0)
        try:
            await process_page(scraper, page_url, data, page_num=0, retry_queue=[], existing_links=existing_links, existing_links_lock=existing_links_lock, category=category)
        except Exception:
            pass
        retry_progress.update(1)
        retry_count += 1
        
        # Pequena pausa entre tentativas
        await asyncio.sleep(2)
    
    retry_progress.close()

async def process_category(scraper, base_url, data, page_semaphore, game_semaphore, existing_links, existing_links_lock):
    global processed_games_count, stats
    retry_queue = []
    if processed_games_count >= MAX_GAMES:
        return

    try:
        last_page_num = await fetch_last_page_num(scraper, base_url)
        pages = list(range(1, last_page_num + 1))
            
        category = extract_category_from_url(base_url)
        
        print(f"\n{Fore.CYAN}Processando categoria: {Style.BRIGHT}{category}{Style.RESET_ALL}")
        print(f"Total de páginas: {len(pages)} de {last_page_num}\n")
        
        progress_bar = tqdm(total=len(pages), desc=f"Páginas de {category}", unit="página", ncols=100, bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") 
        
        # Processar páginas em lotes menores com pausa entre lotes
        for i in range(0, len(pages), PAGE_SEMAPHORE_LIMIT):
            if processed_games_count >= MAX_GAMES:
                break
                
            batch = pages[i:i + PAGE_SEMAPHORE_LIMIT]
            tasks = []
            
            for page_num in batch:
                page_url = f"{base_url}/page/{page_num}"
                tasks.append(process_page(scraper, page_url, data, page_num, retry_queue, existing_links, existing_links_lock, category))
                
            if tasks:
                # Processar o lote atual
                await asyncio.gather(*tasks)
                
            # Atualizar a barra de progresso
            progress_bar.update(len(batch))
            
            # Salvar dados periodicamente com proteção de lock
            if i % 20 == 0 and i > 0:
                async with existing_links_lock:
                    save_data(JSON_FILENAME, data)
                
            # Pequena pausa entre lotes para evitar sobrecarga
            if i + PAGE_SEMAPHORE_LIMIT < len(pages):
                await asyncio.sleep(1)
            
        progress_bar.close()
        stats["categories_processed"] += 1
        print_stats()

        await retry_failed_games(scraper, retry_queue, data, existing_links, existing_links_lock, category)

    except GameLimitReached:
        return
    except Exception:
        pass

async def scrape_games():
    global processed_games_count, start_time
    start_time = time.time()
    category_semaphore = asyncio.Semaphore(CATEGORY_SEMAPHORE_LIMIT)
    
    data = load_existing_data(JSON_FILENAME)
    # Usar um conjunto para rastrear links já processados
    existing_links = load_existing_links(JSON_FILENAME)
    
    # Adicionar um lock para proteger o acesso concorrente a existing_links e data
    existing_links_lock = asyncio.Lock()

    print(f"{Style.BRIGHT}{Fore.CYAN}=== Iniciando Scraper ==={Style.RESET_ALL}")
    print(f"Total de categorias: {len(BASE_URLS)}")
    print(f"Jogos existentes: {len(data.get('downloads', []))}")
    print(f"Limite de jogos: {MAX_GAMES}")
    print(f"Requisições concorrentes: {CONCURRENT_REQUESTS}")
    print(f"Limite de semáforo por categoria: {CATEGORY_SEMAPHORE_LIMIT}")
    print(f"Limite de semáforo por página: {PAGE_SEMAPHORE_LIMIT}")
    print(f"Limite de semáforo por jogo: {GAME_SEMAPHORE_LIMIT}\n")

    try:
        # Criar um scraper com configurações otimizadas
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            delay=1.5  # Adicionar um pequeno atraso entre requisições
        )
        
        # Configurar um temporizador para salvar dados periodicamente
        last_save_time = time.time()
        save_interval = 300  # Salvar a cada 5 minutos
        
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
                            page_semaphore=None,
                            game_semaphore=None,
                            existing_links=existing_links,
                            existing_links_lock=existing_links_lock
                        )
                    )
                    
            if tasks:
                await asyncio.gather(*tasks)
                
            # Salvar dados após cada categoria com proteção de lock
            async with existing_links_lock:
                save_data(JSON_FILENAME, data)
            
            # Verificar se é hora de salvar dados periodicamente
            current_time = time.time()
            if current_time - last_save_time > save_interval:
                print(f"{Fore.YELLOW}Salvando dados periodicamente...{Style.RESET_ALL}")
                async with existing_links_lock:
                    save_data(JSON_FILENAME, data)
                last_save_time = current_time
                
            # Pequena pausa entre categorias
            if i + CATEGORY_SEMAPHORE_LIMIT < len(BASE_URLS):
                print(f"{Fore.YELLOW}Pausa entre categorias para evitar bloqueios...{Style.RESET_ALL}")
                await asyncio.sleep(5)
        
        # Exibir estatísticas finais
        elapsed_time = time.time() - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        print(f"\n{Style.BRIGHT}{Fore.GREEN}=== Scraping Concluído ==={Style.RESET_ALL}")
        print(f"Tempo total: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
        print(f"Jogos novos adicionados: {stats['new_games']}")
        print(f"Jogos sem links: {stats['no_links']}")
        print(f"Jogos ignorados: {stats['ignored']}")
        print(f"Total de jogos na base: {len(data.get('downloads', []))}")
        
        # Calcular estatísticas de desempenho
        if elapsed_time > 0:
            games_per_hour = (stats['new_games'] + stats['no_links']) / (elapsed_time / 3600)
            print(f"Taxa média: {games_per_hour:.1f} jogos/hora")
            
        print(f"{Style.BRIGHT}{Fore.GREEN}========================{Style.RESET_ALL}\n")

    except Exception:
        pass
    finally:
        await cleanup()

async def cleanup():
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(scrape_games())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Scraping interrompido pelo usuário.{Style.RESET_ALL}")
    except Exception as e:
        print(f"\n{Fore.RED}Erro inesperado: {str(e)}{Style.RESET_ALL}")
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        
        try:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
            
        loop.close()
        print(f"\n{Fore.CYAN}Script finalizado.{Style.RESET_ALL}")
        
if __name__ == "__main__":
    main()