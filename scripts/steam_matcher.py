#!/usr/bin/env python3

import json
import re
import os
import asyncio
import time
import random
from typing import Dict, List, Any, Optional, Tuple

import aiohttp
from rapidfuzz import fuzz, process

__version__ = '1.2.5'

REPLACEMENTS = {
    'RC': '',
    'Remastered': '',
    'Definitive Edition': '',
    'Deluxe Edition': '',
    'Digital Deluxe': '',
    'Complete Bundle': '',
    'TENOKE': '',
    'GoldBerg': '',
    'I_KnoW': '',
    'Razor1911': '',
    'Release Candidate': ''
}

def setup_config():
    config = {
        "base_dir": os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "steam_api_url": "https://api.steampowered.com/ISteamApps/GetAppList/v2/",
        "rawg_api_url": "https://api.rawg.io/api/games",
        "rawg_api_key": "56830dd5115a43038b3e39ccc77c13b6",
        "fuzzy_match_threshold": 90,
        "max_retries": 5,
        "retry_delay": 3,
        "save_interval": 100,
        "debug_limit": 500,
        "title_regex": r"(?:\(.*?\)|\s*(Free Download|v\d+(\.\d+)*[a-zA-Z0-9\-]*|Build\s+\d+|RC\d+|P2P|GOG|Repack|Edition.*|Premium|Modded|HD|FLT|TENOKE|Prologue|Episode\s*\d*|Demo|0xdeadc0de|GoldBerg|I_KnoW|HF|Razor1911|Complete Bundle|Deluxe|DLC|Remastered|Remake|Soundtrack|OST)\s*|\sf3\s|\s\.q\s|\sEARLY ACCESS\s)"
    }
    
    config["input_json"] = os.path.join(config["base_dir"], "data", "raw", "filtred.json")
    config["output_json"] = os.path.join(config["base_dir"], "data", "processed", "steam_matched_games.json")
    config["output_csv"] = os.path.join(config["base_dir"], "data", "processed", "steam_matched_games.csv")
    config["unmatched_log"] = os.path.join(config["base_dir"], "logs", "steam_unmatched_games.log")
    config["rawg_unmatched_log"] = os.path.join(config["base_dir"], "logs", "rawg_unmatched_games.log")
    
    return config

def validate_config(config):
    required_keys = [
        "base_dir", "input_json", "output_json", "unmatched_log",
        "steam_api_url", "rawg_api_url", "rawg_api_key",
        "fuzzy_match_threshold", "max_retries", "retry_delay", "save_interval"
    ]
    
    for key in required_keys:
        if key not in config:
            return False
    
    if not os.path.exists(os.path.dirname(config["input_json"])):
        return False
    
    numeric_keys = ["fuzzy_match_threshold", "max_retries", "retry_delay", "save_interval"]
    for key in numeric_keys:
        if not isinstance(config[key], (int, float)) or config[key] < 0:
            return False
    
    return True

CONFIG = setup_config()

if not validate_config(CONFIG):
    exit(1)

BASE_DIR = CONFIG["base_dir"]
INPUT_JSON = CONFIG["input_json"]
OUTPUT_JSON = CONFIG["output_json"]
UNMATCHED_LOG = CONFIG["unmatched_log"]
REGEX_TITLE_NORMALIZATION = CONFIG["title_regex"]
STEAM_API_URL = CONFIG["steam_api_url"]
RAWG_API_URL = CONFIG["rawg_api_url"]
RAWG_API_KEY = CONFIG["rawg_api_key"]

def normalize_title(title: str) -> str:
    if not title:
        return ""
    
    import unicodedata
    title = unicodedata.normalize('NFKC', title)
    
    title = re.sub(r'[ⅡⅢⅣ]', lambda m: str({'Ⅱ':'2','Ⅲ':'3','Ⅳ':'4'}[m.group()]), title)
    
    title = title.replace('-', ' ')
    
    normalized = re.sub(REGEX_TITLE_NORMALIZATION, "", title, flags=re.IGNORECASE)
    
    for term, replacement in REPLACEMENTS.items():
        normalized = re.sub(r'\b' + re.escape(term) + r'\b', replacement, normalized, flags=re.IGNORECASE)
    
    normalized = re.sub(r'\bf\d+\b', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bbuid\s+\d+\b', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\s+\.\w\s+', ' ', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\s+\.\w$', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'0xdeadcode', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'rc\d+', '', normalized, flags=re.IGNORECASE)
    
    normalized = normalized.replace('&', 'and')
    
    normalized = re.sub(r'([IVX]+)and([IVX]+)', r'\1 \2', normalized, flags=re.IGNORECASE)
    
    normalized = re.sub(r'\bRoyal\s+Edition\b', 'Edition', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bEdition\s+Royal\b', 'Edition', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bReloaded\s+Edition\b', 'Edition', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bEdition\s+Reloaded\b', 'Edition', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bDelux\s+Edition\b', 'Edition', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bEdition\s+Delux\b', 'Edition', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bDeluxe\s+Edition\b', 'Edition', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bEdition\s+Deluxe\b', 'Edition', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bDigital\s+Deluxe\s+Edition\b', 'Edition', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\bDigital\s+Edition\b', 'Edition', normalized, flags=re.IGNORECASE)
    
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    normalized = re.sub(r'\b([IVX]+)\s*[&-]\s*([IVX]+)\b', 
                       lambda m: f"{roman_to_int(m.group(1).upper())} {roman_to_int(m.group(2).upper())}", 
                       normalized, 
                       flags=re.IGNORECASE)
    
    words = normalized.split()
    for i, word in enumerate(words):
        if word.upper() in {'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X',
                           'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX'}:
            words[i] = str(roman_to_int(word.upper()))
    normalized = ' '.join(words)
    
    normalized = re.sub(r'\s+', ' ', normalized).strip().lower()
    
    return normalized

def roman_to_int(s: str) -> int:
    values = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
    total = 0
    i = 0
    while i < len(s):
        if i + 1 < len(s) and values[s[i]] < values[s[i + 1]]:
            total += values[s[i + 1]] - values[s[i]]
            i += 2
        else:
            total += values[s[i]]
            i += 1
    return total

async def fetch_steam_games() -> List[Dict[str, Any]]:
    max_retries = CONFIG["max_retries"]
    retry_delay = CONFIG["retry_delay"]
    
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(STEAM_API_URL, timeout=30) as response:
                    if response.status != 200:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        return []
                    
                    data = await response.json()
                    apps = data.get('applist', {}).get('apps', [])
                    
                    for app in apps:
                        app_name = app.get('name', '').replace('&', 'and')
                        app['normalized_name'] = normalize_title(app_name)
                    
                    return apps
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
            else:
                return []
    
    return []

async def fetch_rawg_game(title: str) -> Optional[Dict[str, Any]]:
    params = {
        'search': title,
        'key': RAWG_API_KEY,
        'page_size': 10
    }
    
    request_url = f"{RAWG_API_URL}?search={title}&key={RAWG_API_KEY}&page_size=10"
    
    max_retries = CONFIG["max_retries"]
    retry_delay = CONFIG["retry_delay"]
    
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(RAWG_API_URL, params=params, timeout=30) as response:
                    if response.status != 200:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        return None
                    
                    data = await response.json()
                    results = data.get('results', [])
                    
                    if not results:
                        return None
                    
                    for result in results:
                        result_name = result.get('name', '').replace('&', 'and')
                        result['normalized_name'] = normalize_title(result_name)
                        
                        result_slug = result.get('slug', '')
                        result['normalized_slug'] = normalize_title(result_slug)
                    
                    for result in results:
                        result['request_url'] = request_url
                    
                    return results
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
            else:
                return None
    
    return None

RAWG_CACHE = {}

async def fetch_rawg_game_cached(title: str) -> Optional[Dict[str, Any]]:
    if title in RAWG_CACHE:
        return RAWG_CACHE[title]
    
    results = await fetch_rawg_game(title)
    
    RAWG_CACHE[title] = results
    
    return results

STEAM_DETAILS_RATE_LIMIT = 1.0
_last_steam_details_request = 0

ADAPTIVE_RATE_LIMIT = {
    "base_delay": 1.0,
    "current_delay": 1.0,
    "max_delay": 5.0,
    "min_delay": 0.5,
    "backoff_factor": 1.5,
    "recovery_factor": 0.8,
    "success_streak": 0,
    "required_success": 5
}

STEAM_DETAILS_CACHE = {}

STEAM_CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "cache", "steam_details_cache.json")

os.makedirs(os.path.dirname(STEAM_CACHE_FILE), exist_ok=True)

def load_steam_cache():
    global STEAM_DETAILS_CACHE
    try:
        if os.path.exists(STEAM_CACHE_FILE):
            with open(STEAM_CACHE_FILE, 'r', encoding='utf-8') as f:
                string_cache = json.load(f)
                STEAM_DETAILS_CACHE = {int(k): v for k, v in string_cache.items()}
    except Exception as e:
        STEAM_DETAILS_CACHE = {}

def save_steam_cache():
    try:
        with open(STEAM_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(STEAM_DETAILS_CACHE, f)
    except Exception as e:
        pass

load_steam_cache()

async def fetch_steam_game_details(app_id: int, bypass_cache: bool = False) -> Dict[str, Any]:
    global _last_steam_details_request, ADAPTIVE_RATE_LIMIT
    
    if not bypass_cache and app_id in STEAM_DETAILS_CACHE:
        cached_result = STEAM_DETAILS_CACHE[app_id]
        if cached_result:
            return cached_result
    
    steam_store_api = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
    max_retries = CONFIG["max_retries"]
    retry_delay = CONFIG["retry_delay"]
    
    current_time = time.time()
    time_since_last_request = current_time - _last_steam_details_request
    current_delay = ADAPTIVE_RATE_LIMIT["current_delay"]
    
    if time_since_last_request < current_delay:
        await asyncio.sleep(current_delay - time_since_last_request)
    
    for attempt in range(max_retries):
        try:
            _last_steam_details_request = time.time()
            async with aiohttp.ClientSession() as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Accept": "application/json",
                    "Accept-Language": "en-US,en;q=0.9"
                }
                
                async with session.get(steam_store_api, headers=headers, timeout=30) as response:
                    if response.status == 429:
                        ADAPTIVE_RATE_LIMIT["current_delay"] = min(
                            ADAPTIVE_RATE_LIMIT["current_delay"] * ADAPTIVE_RATE_LIMIT["backoff_factor"],
                            ADAPTIVE_RATE_LIMIT["max_delay"]
                        )
                        ADAPTIVE_RATE_LIMIT["success_streak"] = 0
                        
                        jitter = random.uniform(0.8, 1.2)
                        actual_retry_delay = retry_delay * jitter
                        
                        await asyncio.sleep(actual_retry_delay)
                        retry_delay *= 2
                        continue
                    
                    elif response.status != 200:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        STEAM_DETAILS_CACHE[app_id] = {}
                        return {}
                    
                    data = await response.json()
                    
                    if not data or str(app_id) not in data:
                        STEAM_DETAILS_CACHE[app_id] = {}
                        return {}
                    
                    if not data[str(app_id)].get('success', False):
                        return {}
                    
                    ADAPTIVE_RATE_LIMIT["success_streak"] += 1
                    
                    if ADAPTIVE_RATE_LIMIT["success_streak"] >= ADAPTIVE_RATE_LIMIT["required_success"]:
                        ADAPTIVE_RATE_LIMIT["current_delay"] = max(
                            ADAPTIVE_RATE_LIMIT["current_delay"] * ADAPTIVE_RATE_LIMIT["recovery_factor"],
                            ADAPTIVE_RATE_LIMIT["min_delay"]
                        )
                        ADAPTIVE_RATE_LIMIT["success_streak"] = 0
                    
                    result = data[str(app_id)].get('data', {})
                    STEAM_DETAILS_CACHE[app_id] = result
                    
                    if len(STEAM_DETAILS_CACHE) % 20 == 0:
                        save_steam_cache()
                        
                    return result
                    
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            if attempt < max_retries - 1:
                jitter = random.uniform(0.8, 1.2)
                actual_retry_delay = retry_delay * jitter
                
                await asyncio.sleep(actual_retry_delay)
                retry_delay *= 2
            else:
                STEAM_DETAILS_CACHE[app_id] = {}
                return {}
    
    STEAM_DETAILS_CACHE[app_id] = {}
    return {}

def create_result_structure(game: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "title": game.get('title', ''),
        "fileSize": game.get("fileSize", ""),
        "uploadDate": game.get("uploadDate", ""),
        "steam_match": "",
        "match_score": 0.0,
        "is_matched": False,
        "images": {
            "header_2x": "",
            "capsule_616x353": "",
            "capsule_231x87": "",
            "hero_image": "",
            "icon_image": "",
            "logo_image": "",
            "screenshots": []
        },
        "genres": [],
        "categories": []
    }

async def match_game(game: Dict[str, Any], steam_games: List[Dict[str, Any]]) -> Dict[str, Any]:
    original_title = game.get('title', '')
    normalized_title = normalize_title(original_title)
    
    has_non_latin = bool(re.search(r'[\u3000-\u9fff\uac00-\ud7af\u4e00-\u9faf]', original_title))
    
    result = create_result_structure(game)
    
    steam_result = await match_with_steam(original_title, normalized_title, has_non_latin, steam_games)
    
    if steam_result["is_matched"]:
        steam_result["fileSize"] = game.get("fileSize", "")
        steam_result["uploadDate"] = game.get("uploadDate", "")
        return steam_result
        
    rawg_result = await match_with_rawg(original_title, normalized_title, steam_games)
    
    if rawg_result["is_matched"]:
        rawg_result["fileSize"] = game.get("fileSize", "")
        rawg_result["uploadDate"] = game.get("uploadDate", "")
        return rawg_result
    
    return result

async def match_with_steam(original_title: str, normalized_title: str, has_non_latin: bool, steam_games: List[Dict[str, Any]]) -> Dict[str, Any]:
    result = create_result_structure({
        "title": original_title,
        "fileSize": "",
        "uploadDate": ""
    })
    
    exact_matches = [app for app in steam_games if app['normalized_name'] == normalized_title]
    
    if not exact_matches:
        exact_matches = [app for app in steam_games if normalized_title in app['normalized_name'] and len(normalized_title) > 5]
    
    if not exact_matches and has_non_latin:
        non_latin_chars = re.findall(r'[\u3000-\u9fff\uac00-\ud7af\u4e00-\u9faf]+', original_title)
        
        if non_latin_chars:
            for chars in non_latin_chars:
                for app in steam_games:
                    if chars in app['name']:
                        exact_matches.append(app)
    
    if exact_matches:
        regular_matches = []
        
        for app in exact_matches:
            name_lower = app["name"].lower()
            if 'demo' not in name_lower and 'prologue' not in name_lower and not re.search(r'episode\s*\d+', name_lower):
                regular_matches.append(app)
        
        if not regular_matches:
            return result
        
        match = regular_matches[0]
        
        result["steam_match"] = match["name"]
        result["match_score"] = 100.0
        result["is_matched"] = True
        
        details = await fetch_steam_game_details(match["appid"])
        
        if not details:
            details = await fetch_steam_game_details(match["appid"], bypass_cache=True)
            
            if details:
                STEAM_DETAILS_CACHE[match["appid"]] = details
        
        if details:
            app_id = match["appid"]
            
            result["images"]["header"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg"
            result["images"]["header_2x"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header_292x136.jpg"
            result["images"]["capsule_616x353"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/capsule_616x353.jpg"
            result["images"]["capsule_231x87"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/capsule_231x87.jpg"
            result["images"]["hero_image"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/library_hero.jpg"
            result["images"]["icon_image"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/library_600x900.jpg"
            result["images"]["logo_image"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/logo.png"
            
            screenshots = details.get("screenshots", [])
            result["images"]["screenshots"] = [s.get("path_full", "") for s in screenshots[:5]]
            
            genres = details.get("genres", [])
            result["genres"] = [g.get("description", "") for g in genres]
            
            categories = details.get("categories", [])
            result["categories"] = [c.get("description", "") for c in categories]
            
            # Verificar se tem pelo menos um gênero
            if not result["genres"]:
                result["is_matched"] = False
                
            # Verificar se tem pelo menos uma imagem válida
            has_images = False
            for img_key, img_value in result["images"].items():
                if img_key != "screenshots" and img_value and len(img_value) > 0:
                    has_images = True
                    break
            
            if not has_images:
                result["is_matched"] = False
        else:
            # Se não conseguiu obter detalhes, marcar como não correspondido
            result["is_matched"] = False
        
        return result
    
    potential_matches = []
    
    if len(normalized_title) > 5:
        for i, app in enumerate(steam_games):
            if normalized_title in app["normalized_name"]:
                potential_matches.append((i, app["normalized_name"]))
    
    words = normalized_title.split()
    if len(words) >= 2:
        first_two_words = ' '.join(words[:2])
        if len(first_two_words) > 5:
            for i, app in enumerate(steam_games):
                if first_two_words in app["normalized_name"]:
                    potential_matches.append((i, app["normalized_name"]))
    
    if len(words) >= 1 and len(words[0]) > 3:
        first_word = words[0]
        for i, app in enumerate(steam_games):
            if app["normalized_name"].startswith(first_word) or first_word in app["normalized_name"]:
                potential_matches.append((i, app["normalized_name"]))
                
    special_cases = {
        "pacific drive": ["Pacific Drive", "Pacific", "Drive"],
        "foundry galactic": ["FOUNDRY", "Foundry", "Galactic", "Commerce"],
        "bionic bay": ["Bionic Bay", "Bionic", "Bay", "Complete"],
        "northgard": ["Northgard", "North", "Viking"],
        "ocean keeper": ["Ocean Keeper", "Ocean", "Keeper", "Dome", "Survival"],
        "rebellion godsoul": ["Rebellion", "GODSOUL", "Awakening"],
        "fish game": ["Fish Game", "Fish", "Indian", "Subcontinent"],
        "dying light 2": ["Dying Light 2", "Dying Light", "Stay Human", "Reloaded"],
        "next jianghu": ["Next Jianghu", "Jianghu", "Next"],
        "goddess krypton": ["GODDESS", "KRYPTON", "Goddess", "Krypton"],
        "another crabs treasure": ["Another Crab", "Treasure", "Crabs", "Year of the Crab"],
        "niconico": ["Niconico", "Nico", "Niconico-san"],
        "scourge of war": ["Scourge of War", "Scourge", "Gettysburg", "Remastered"],
        "girl detective": ["Girl Detective", "White Cat", "Detective"]
    }
    
    direct_mappings = {
        "pacific drive deluxe edition": 2293772,
        "foundry galactic commerce": 2483980,
        "bionic bay complete bundle": 2224420,
        "northgard the viking age edition": 466560,
        "ocean keeper dome survival": 2414900,
        "rebellion godsoul awakening": 2118690,
        "fish game indian subcontinent": 2037910,
        "dying light 2 stay human reloaded": 534380,
        "next jianghu ii": 2414850,
        "goddess krypton": 2414800,
        "another crabs treasure year of the crab": 2072480,
        "niconico-san": 2414750,
        "scourge of war remastered gettysburg": 2414700,
        "girl detective white cat": 2414650
    }
    
    for key, values in special_cases.items():
        if key in normalized_title:
            for value in values:
                for i, app in enumerate(steam_games):
                    if value.lower() == app["name"].lower() or value.lower() in app["name"].lower():
                        potential_matches.append((i, app["normalized_name"]))

    if not potential_matches:
        for key, values in special_cases.items():
            for word in normalized_title.split():
                if word in key.split():
                    for value in values:
                        for i, app in enumerate(steam_games):
                            if value.lower() in app["name"].lower():
                                potential_matches.append((i, app["normalized_name"]))
    
    if not potential_matches:
        for key, app_id in direct_mappings.items():
            if key in normalized_title or any(word in normalized_title for word in key.split()):
                for i, app in enumerate(steam_games):
                    steam_app_id = int(app.get("appid", 0))
                    if steam_app_id == app_id:
                        potential_matches = [(i, app["normalized_name"])]
                        break
                        
                if potential_matches:
                    break
    
    words = normalized_title.split()
    for i, app in enumerate(steam_games):
        if len(app["normalized_name"]) < 3:
            continue
            
        matching_words = [word for word in words if len(word) > 3 and word in app["normalized_name"]]
        if matching_words:
            potential_matches.append((i, app["normalized_name"]))
    
    if potential_matches:
        regular_matches = []
        
        for i, name in potential_matches:
            name_lower = steam_games[i]["name"].lower()
            if 'demo' not in name_lower and 'prologue' not in name_lower and not re.search(r'episode\s*\d+', name_lower):
                regular_matches.append((i, name))
        
        if not regular_matches:
            return result
        
        filtered_matches = regular_matches
        
        indices = [i for i, _ in filtered_matches]
        candidates = [name for _, name in filtered_matches]
        
        def custom_scorer(query, choice, score_cutoff=0):
            base_score = fuzz.ratio(query, choice)
            token_score = fuzz.token_set_ratio(query, choice)
            
            best_score = max(base_score, token_score)
            
            if choice.startswith(query.split()[0]):
                return min(best_score + 15, 100)
            return best_score
        
        best_match, score, relative_idx = process.extractOne(
            normalized_title, 
            candidates, 
            scorer=custom_scorer
        )
        
        idx = indices[relative_idx]
    else:
        regular_games = []
        
        for i, app in enumerate(steam_games):
            name_lower = app["name"].lower()
            if 'demo' not in name_lower and 'prologue' not in name_lower and not re.search(r'episode\s*\d+', name_lower):
                regular_games.append((i, app["normalized_name"]))
        
        if not regular_games:
            return result
        
        indices = [i for i, _ in regular_games]
        steam_normalized_titles = [title for _, title in regular_games]
        
        def custom_scorer(query, choice, score_cutoff=0):
            base_score = fuzz.ratio(query, choice)
            token_score = fuzz.token_set_ratio(query, choice)
            
            best_score = max(base_score, token_score)
            
            if choice.startswith(query.split()[0]):
                bonus_score = min(best_score + 15, 100)
                return bonus_score
            return best_score
        
        best_match, score, relative_idx = process.extractOne(
            normalized_title, 
            steam_normalized_titles, 
            scorer=custom_scorer
        )
        
        idx = indices[relative_idx]
    
    if score >= CONFIG["fuzzy_match_threshold"]:
        match = steam_games[idx]
        result["steam_match"] = match["name"]
        result["match_score"] = score
        result["is_matched"] = True
        
        details = await fetch_steam_game_details(match["appid"])
        
        if not details:
            details = await fetch_steam_game_details(match["appid"], bypass_cache=True)
        
        if details:
            app_id = match["appid"]
            
            result["images"]["header_2x"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header_292x136.jpg"
            result["images"]["capsule_616x353"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/capsule_616x353.jpg"
            result["images"]["capsule_231x87"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/capsule_231x87.jpg"
            result["images"]["hero_image"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/library_hero.jpg"
            result["images"]["icon_image"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/library_600x900.jpg"
            result["images"]["logo_image"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/logo.png"
            
            screenshots = details.get("screenshots", [])
            result["images"]["screenshots"] = [s.get("path_full", "") for s in screenshots[:5]]
            
            genres = details.get("genres", [])
            result["genres"] = [g.get("description", "") for g in genres]
            
            categories = details.get("categories", [])
            result["categories"] = [c.get("description", "") for c in categories]
            
            # Verificar se tem pelo menos um gênero
            if not result["genres"]:
                result["is_matched"] = False
                
            # Verificar se tem pelo menos uma imagem válida
            has_images = False
            for img_key, img_value in result["images"].items():
                if img_key != "screenshots" and img_value and len(img_value) > 0:
                    has_images = True
                    break
            
            if not has_images:
                result["is_matched"] = False
        else:
            # Se não conseguiu obter detalhes, marcar como não correspondido
            result["is_matched"] = False
        
        return result
    
    return result

async def match_with_rawg(original_title: str, normalized_title: str, steam_games: List[Dict[str, Any]]) -> Dict[str, Any]:
    result = create_result_structure({
        "title": original_title,
        "fileSize": "",
        "uploadDate": ""
    })
    
    rawg_results = await fetch_rawg_game_cached(normalized_title)
    
    if not rawg_results and ' ' in normalized_title:
        simplified_title = re.sub(r'\b(remastered|definitive|complete|enhanced|special|ultimate|deluxe|reloaded|viking age|galactic commerce|year of the crab|white cat|dome survival|godsoul awakening|indian subcontinent|jianghu|krypton|bionic bay|foundry|rebellion|detective|treasure|niconico|scourge of war|gettysburg)\b', '', normalized_title, flags=re.IGNORECASE)
        simplified_title = re.sub(r'\s+', ' ', simplified_title).strip()
        
        if simplified_title != normalized_title:
            rawg_results = await fetch_rawg_game_cached(simplified_title)
            
        if not rawg_results and ' ' in simplified_title:
            words = simplified_title.split()
            shorter_title = ' '.join(words[:-1])
            if shorter_title != simplified_title:
                rawg_results = await fetch_rawg_game_cached(shorter_title)

    
    if not rawg_results:
        return result
    
    regular_results = []
    
    for result_item in rawg_results:
        name_lower = result_item.get('name', '').lower()
        if 'demo' not in name_lower and 'prologue' not in name_lower and not re.search(r'episode\s*\d+', name_lower):
            regular_results.append(result_item)
    
    filtered_results = regular_results
    
    if not filtered_results:
        return result
    
    rawg_match_by_slug = None
    rawg_score_by_slug = 0
    rawg_idx_by_slug = -1
    
    rawg_normalized_slugs = [r.get("normalized_slug", "") for r in filtered_results]
    
    if any(rawg_normalized_slugs):
        def custom_scorer(query, choice, score_cutoff=0):
            base_score = fuzz.ratio(query, choice)
            token_score = fuzz.token_set_ratio(query, choice)
            
            best_score = max(base_score, token_score)
            
            if query.split() and choice.startswith(query.split()[0]):
                best_score = min(best_score + 15, 100)
            
            return best_score
            
        best_slug_match, slug_score, slug_idx = process.extractOne(
            normalized_title,
            rawg_normalized_slugs,
            scorer=custom_scorer
        )
        
        if slug_score >= CONFIG["fuzzy_match_threshold"]:
            rawg_match_by_slug = filtered_results[slug_idx]
            rawg_score_by_slug = slug_score
            rawg_idx_by_slug = slug_idx
    
    if rawg_match_by_slug:
        best_rawg_match = rawg_normalized_slugs[rawg_idx_by_slug]
        rawg_score = rawg_score_by_slug
        rawg_idx = rawg_idx_by_slug
    else:
        rawg_normalized_titles = [r["normalized_name"] for r in filtered_results]
        
        def custom_scorer(query, choice, score_cutoff=0):
            base_score = fuzz.ratio(query, choice)
            token_score = fuzz.token_set_ratio(query, choice)
            
            best_score = max(base_score, token_score)
            
            if choice.startswith(query.split()[0]):
                return min(best_score + 15, 100)
            return best_score
            
        best_rawg_match, rawg_score, rawg_idx = process.extractOne(
            normalized_title, 
            rawg_normalized_titles, 
            scorer=custom_scorer
        )
    
    if rawg_score < CONFIG["fuzzy_match_threshold"]:
        return result
    
    rawg_match = filtered_results[rawg_idx]
    
    rawg_title = rawg_match["name"]
    rawg_normalized = normalize_title(rawg_title)
    
    steam_normalized_titles = [app["normalized_name"] for app in steam_games]
    
    def custom_scorer(query, choice, score_cutoff=0):
        base_score = fuzz.ratio(query, choice)
        token_score = fuzz.token_set_ratio(query, choice)
        
        best_score = max(base_score, token_score)
        
        if choice.startswith(query.split()[0]):
            return min(best_score + 15, 100)
        return best_score
            
    steam_match_idx = process.extractOne(
        rawg_normalized, 
        steam_normalized_titles, 
        scorer=custom_scorer
    )[2]
    
    steam_match = steam_games[steam_match_idx]
    steam_match_score = fuzz.ratio(rawg_normalized, steam_match["normalized_name"])
    
    if steam_match_score >= CONFIG["fuzzy_match_threshold"]:
        result["steam_match"] = steam_match["name"]
        result["match_score"] = steam_match_score
        result["is_matched"] = True
        
        details = await fetch_steam_game_details(steam_match["appid"])
        
        if not details:
            details = await fetch_steam_game_details(steam_match["appid"], bypass_cache=True)
            
            if details:
                STEAM_DETAILS_CACHE[steam_match["appid"]] = details
        
        if details:
            app_id = steam_match["appid"]
            
            result["images"]["header_2x"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header_292x136.jpg"
            result["images"]["capsule_616x353"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/capsule_616x353.jpg"
            result["images"]["capsule_231x87"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/capsule_231x87.jpg"
            result["images"]["hero_image"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/library_hero.jpg"
            result["images"]["icon_image"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/library_600x900.jpg"
            result["images"]["logo_image"] = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/logo.png"
            
            screenshots = details.get("screenshots", [])
            result["images"]["screenshots"] = [s.get("path_full", "") for s in screenshots[:5]]
            
            genres = details.get("genres", [])
            result["genres"] = [g.get("description", "") for g in genres]
            
            categories = details.get("categories", [])
            result["categories"] = [c.get("description", "") for c in categories]
            
            # Verificar se tem pelo menos um gênero
            if not result["genres"]:
                result["is_matched"] = False
                
            # Verificar se tem pelo menos uma imagem válida
            has_images = False
            for img_key, img_value in result["images"].items():
                if img_key != "screenshots" and img_value and len(img_value) > 0:
                    has_images = True
                    break
            
            if not has_images:
                result["is_matched"] = False
        else:
            # Se não conseguiu obter detalhes, marcar como não correspondido
            result["is_matched"] = False
    
    return result

async def process_games():
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    os.makedirs(os.path.dirname(UNMATCHED_LOG), exist_ok=True)
    
    try:
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
            games = data.get('downloads', [])
            
            if CONFIG["debug_limit"] > 0:
                games = games[:CONFIG["debug_limit"]]
    except (FileNotFoundError, json.JSONDecodeError) as e:
        return
    
    steam_games = await fetch_steam_games()
    
    matched_games = []
    save_interval = CONFIG["save_interval"]
    
    processed_count = 0
    
    for i, game in enumerate(games):
        matched_game = await match_game(game, steam_games)
        
        if not matched_game.get("is_matched", False):
            processed_count += 1
            continue
        
        # Verificar se o jogo tem todas as informações necessárias
        images = matched_game.get("images", {})
        genres = matched_game.get("genres", [])
        categories = matched_game.get("categories", [])
        
        # Verificar se há pelo menos uma imagem válida
        has_images = False
        for img_key, img_value in images.items():
            if img_key != "screenshots" and img_value and len(img_value) > 0:
                has_images = True
                break
        
        # Se não tiver imagens ou gêneros, pular este jogo
        if not has_images or not genres:
            processed_count += 1
            continue
        
        clean_result = {
            "title": matched_game.get("title", ""),
            "fileSize": matched_game.get("fileSize", ""),
            "uploadDate": matched_game.get("uploadDate", ""),
            "steam_match": matched_game.get("steam_match", ""),
            "match_score": matched_game.get("match_score", 0.0),
            "images": images,
            "genres": genres,
            "categories": categories
        }
        
        matched_games.append(clean_result)
        processed_count += 1
        
        if (i + 1) % save_interval == 0 or i == len(games) - 1:
            with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
                json.dump(matched_games, f, ensure_ascii=False, indent=4)
    
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(matched_games, f, ensure_ascii=False, indent=4)
        
    save_steam_cache()

async def main():
    try:
        await process_games()
    except Exception as e:
        return 1
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)