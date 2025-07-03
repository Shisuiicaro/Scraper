import json
import os
import re
import time
import sys
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import lru_cache
from fuzzywuzzy import fuzz, process
from tqdm import tqdm
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)

# Configurar o método de inicialização do multiprocessing
if sys.platform == 'win32':
    # No Windows, usar 'spawn' (padrão)
    mp_context = mp.get_context('spawn')
else:
    # No Linux/macOS, usar 'fork'
    mp_context = mp.get_context('fork')

FUZZY_MATCH_THRESHOLD = 90
MIN_CATEGORY_TITLE_LENGTH = 5
MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION = 0.45
MIN_LENGTH_RATIO_FOR_NORMAL_THRESHOLD = 0.65
STRICT_SCORE_THRESHOLD = 96
PRELIMINARY_SCORE_THRESHOLD = 80
MAX_PRELIMINARY_CANDIDATES = 5
MAX_GAMES_TO_PROCESS = 9999999  # Número de jogos a processar

BLACKLIST_KEYWORDS = ['reflection'] # Termos a serem ignorados na categorização

# Configurações de otimização
NUM_PROCESSES = 3  # Número de processos paralelos
BATCH_SIZE = 10  # Tamanho do lote para processamento paralelo
CACHE_SIZE = 1024  # Tamanho do cache para funções com @lru_cache

@lru_cache(maxsize=CACHE_SIZE)
def cross_language_match(title1, title2):
    lang1 = detect_language(title1)
    lang2 = detect_language(title2)
    
    if lang1 == lang2:
        return fuzz.WRatio(title1, title2)
    
    if (lang1 in ["chinese", "japanese", "korean"] and lang2 == "latin") or \
       (lang2 in ["chinese", "japanese", "korean"] and lang1 == "latin"):
        
        asian_title = title1 if lang1 in ["chinese", "japanese", "korean"] else title2
        latin_title = title2 if lang2 == "latin" else title1
        
        parenthesis_match = re.search(r'\(([^)]+)\)', asian_title)
        if parenthesis_match:
            english_version = parenthesis_match.group(1).strip().lower()
            return fuzz.WRatio(english_version, latin_title)
        
        if lang1 == "chinese" or lang2 == "chinese":
            transliterated = transliterate_chinese(asian_title)
            if transliterated != asian_title:
                return fuzz.WRatio(transliterated, latin_title)
    
    norm1 = normalize_special_chars(title1)
    norm2 = normalize_special_chars(title2)
    return fuzz.WRatio(norm1, norm2)

VR_KEYWORDS = ['vr', 'virtual reality', 'oculus', 'htc vive', 'valve index', 'psvr']

def is_vr_title(title):
    if not title:
        return False
    
    title_lower = title.lower()
    for keyword in VR_KEYWORDS:
        if re.search(r'\b' + re.escape(keyword) + r'\b', title_lower):
            return True
    
    if re.search(r'[\(\[]\s*VR\s*[\)\]]', title, re.IGNORECASE):
        return True
        
    return False

def compare_base_titles_for_vr(game_title, vr_title):
    base_vr_title = re.sub(r'\bVR\b', '', vr_title, flags=re.IGNORECASE).strip()
    base_vr_title = re.sub(r'\s+', ' ', base_vr_title).strip()
    
    similarity_score = fuzz.ratio(game_title.lower(), base_vr_title.lower())
    return similarity_score >= 90

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CATEGORIES_DIR = os.path.join(DATA_DIR, 'categories')

VALID_GAMES_FILE = os.path.join(DATA_DIR, 'raw', 'filtred.json')
ADULT_GAMES_FILE = os.path.join(CATEGORIES_DIR, 'adult_games.json')
SOFTWARE_FILE = os.path.join(CATEGORIES_DIR, 'software.json')
VR_GAMES_FILE = os.path.join(CATEGORIES_DIR, 'vr_games.json')

OUTPUT_DIR = os.path.join(DATA_DIR, 'processed')
os.makedirs(OUTPUT_DIR, exist_ok=True)
CATEGORIZED_SHISUY_SOURCE_FILE = os.path.join(OUTPUT_DIR, 'aio_shisuy.json')
CATEGORIZED_ADULT_GAMES_FILE = os.path.join(OUTPUT_DIR, 'shisuys_adult.json')
CATEGORIZED_SOFTWARE_FILE = os.path.join(OUTPUT_DIR, 'shisuys_software.json')
CATEGORIZED_VR_GAMES_FILE = os.path.join(OUTPUT_DIR, 'shisuys_vr.json')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
UNMATCHED_GAMES_FILE = os.path.join(LOGS_DIR, 'unmatched_games.log')

def load_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"{Fore.RED}Error: File not found - {file_path}{Style.RESET_ALL}")
        return None
    except json.JSONDecodeError:
        print(f"{Fore.RED}Error: Could not decode JSON from - {file_path}{Style.RESET_ALL}")
        return None

def save_json_file(data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f"{Fore.GREEN}Successfully saved: {file_path}{Style.RESET_ALL}")

def get_titles_from_list(game_list_data):
    if not game_list_data or not isinstance(game_list_data, list):
        return [], {}
    titles = []
    original_titles = {}
    
    for game in game_list_data:
        title = game.get('title', '')
        if title:
            cleaned = clean_title(title)
            if len(cleaned) >= MIN_CATEGORY_TITLE_LENGTH:
                titles.append(cleaned)
                original_titles[cleaned] = title
    
    return titles, original_titles

# Regex for cleaning titles, removing common irrelevant parts
REGEX_TITLE_CLEANING = r""" # Using triple quotes for readability
    \(.*?\) |                                      # Text in parentheses (e.g., (Build 123), (Region Free))
    \[.*?\] |                                      # Text in brackets (e.g., [18+], [VR], [Uncensored])
    \s*                                            # Optional leading whitespace
    (?:                                             # Non-capturing group for various terms
        Free\sDownload |                             # "Free Download"
        Build\s\d+ |                                # "Build" followed by numbers (e.g., Build 12345)
        v\d+(\.\d+)*[a-zA-Z0-9\-]* |             # Version numbers (e.g., v1.2.3, v2.0b)
        P2P | GOG | Repack | FLT | TENOKE |         # Common release group tags
        DLC(?:\s*-\s*Free)? |                      # "DLC" or "DLC-Free" or "DLC - Free"
        Demo |                                      # "Demo"
        Update\s\d+ |                              # "Update" followed by numbers
        Goldberg | ElAmigos |                       # Other common tags
        Early\sAccess |                             # "Early Access"
        Collectors?\sEdition |                      # "Collector's Edition" or "Collector Edition"
        Deluxe\sEdition |                           # "Deluxe Edition"
        Ultimate\sEdition |                         # "Ultimate Edition"
        Standard\sEdition |                         # "Standard Edition"
        Game\sOf\sThe\sYear\sEdition | GOTY |       # "Game Of The Year Edition" or "GOTY"
        Anniversary\sEdition |                      # "Anniversary Edition"
        Definitive\sEdition |                       # "Definitive Edition"
        Remastered |                                # "Remastered"
        Remake |                                    # "Remake"
        Bundle |                                    # "Bundle"
        Royalty\sFree\sSprites |                   # Specific to "Indie Graphics Bundle"
        &\sUncensored |                            # "& Uncensored" tag
        \bUncensored\b                             # "Uncensored" tag (com \b para garantir que seja uma palavra completa)
    )
    \s*                                            # Optional trailing whitespace
"""
COMPILED_REGEX_TITLE_CLEANING = re.compile(REGEX_TITLE_CLEANING, flags=re.IGNORECASE | re.VERBOSE)

# Regex para detectar emojis e caracteres especiais
EMOJI_PATTERN = re.compile(
    "[""\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F700-\U0001F77F"  # alchemical symbols
    "\U0001F780-\U0001F7FF"  # Geometric Shapes
    "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
    "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
    "\U0001FA00-\U0001FA6F"  # Chess Symbols
    "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
    "\U00002702-\U000027B0"  # Dingbats
    "\U000024C2-\U0001F251" 
    "🏝🔞🏖️🌊🌴"  # Emojis específicos mencionados nos exemplos
    "]+", flags=re.UNICODE)

# Dicionário de correspondências especiais para casos específicos
SPECIAL_CASE_MAPPINGS = {
    "cumverse": ["cumverse [18+]", "cumverse", "cumverse free download"],
    "dark of chroe": ["暗黑的克蘿薇 (dark of chroe)", "dark of chroe", "dark of chroe free download"],
    "furry sex resort": ["furry sex resort 🏝🔞", "furry sex resort", "furry sex resort free download"],
    "busty milf and summer country sex life": ["busty milf and summer country sex life", "busty milf and summer country sex life free download"]
}

# Mapeamento de caracteres chineses/japoneses comuns para suas versões romanizadas
# Isso ajuda no matching quando os títulos estão em idiomas diferentes
CHARACTER_MAPPINGS = {
    # Mapeamentos chinês -> inglês
    "暗黑": "dark",
    "克蘿薇": "chroe",
    "的": "of",
    "遊戲": "game",
    "戰爭": "war",
    "龍": "dragon",
    "劍": "sword",
    "魔法": "magic",
    "幻想": "fantasy",
    "冒險": "adventure",
    "世界": "world",
    "王國": "kingdom",
    "傳說": "legend",
    "英雄": "hero",
    "時代": "era",
    "命運": "destiny",
    "戰士": "warrior",
    "公主": "princess",
    "皇帝": "emperor",
    "神話": "mythology",
    "夢想": "dream"
}

@lru_cache(maxsize=CACHE_SIZE)
def detect_language(text):
    if not text:
        return "unknown"
    
    chinese_count = japanese_count = korean_count = latin_count = 0
    
    for char in text:
        code = ord(char)
        if (0x4E00 <= code <= 0x9FFF) or (0x3400 <= code <= 0x4DBF):
            chinese_count += 1
        elif (0x3040 <= code <= 0x309F) or (0x30A0 <= code <= 0x30FF):
            japanese_count += 1
        elif 0xAC00 <= code <= 0xD7A3:
            korean_count += 1
        elif (0x0020 <= code <= 0x007F) or (0x00A0 <= code <= 0x024F):
            latin_count += 1
    
    total_chars = len(text.replace(" ", ""))
    if total_chars == 0:
        return "unknown"
    
    chinese_ratio = chinese_count / total_chars
    japanese_ratio = japanese_count / total_chars
    korean_ratio = korean_count / total_chars
    latin_ratio = latin_count / total_chars
    
    max_ratio = max(chinese_ratio, japanese_ratio, korean_ratio, latin_ratio)
    
    if max_ratio == chinese_ratio and chinese_ratio > 0.3:
        return "chinese"
    elif max_ratio == japanese_ratio and japanese_ratio > 0.3:
        return "japanese"
    elif max_ratio == korean_ratio and korean_ratio > 0.3:
        return "korean"
    elif max_ratio == latin_ratio and latin_ratio > 0.5:
        return "latin"
    else:
        return "other"

@lru_cache(maxsize=CACHE_SIZE)
def transliterate_chinese(text):
    if not text:
        return ""
    
    result = text
    for cn_text, en_text in CHARACTER_MAPPINGS.items():
        result = result.replace(cn_text, en_text)
    
    return result

@lru_cache(maxsize=CACHE_SIZE)
def normalize_special_chars(text):
    import unicodedata
    if not text:
        return ""
    
    language = detect_language(text)
    
    parenthesis_match = re.search(r'\(([^)]+)\)', text)
    if parenthesis_match:
        parenthesis_content = parenthesis_match.group(1).strip()
        if all(ord(c) < 128 for c in parenthesis_content):
            return parenthesis_content.lower()
    
    if language == "chinese":
        transliterated = transliterate_chinese(text)
        if transliterated != text and len(transliterated.strip()) >= MIN_CATEGORY_TITLE_LENGTH:
            return transliterated.strip().lower()
    
    normalized = unicodedata.normalize('NFKD', text)
    
    ascii_text = ''.join(c for c in normalized if ord(c) < 128)
    ascii_text = re.sub(r'\s+', ' ', ascii_text).strip().lower()
    
    if ascii_text and len(ascii_text) >= MIN_CATEGORY_TITLE_LENGTH:
        return ascii_text
    
    if language in ["chinese", "japanese", "korean"]:
        return re.sub(r'\s+', ' ', text).strip().lower()
    
    hybrid_text = ''.join([char.lower() if ord(char) < 128 or (i < len(normalized) and ord(normalized[i]) < 128) else char for i, char in enumerate(text)])
    hybrid_text = re.sub(r'\s+', ' ', hybrid_text).strip()
    
    if len(hybrid_text) >= MIN_CATEGORY_TITLE_LENGTH:
        return hybrid_text
    
    return re.sub(r'\s+', ' ', text).strip().lower()

@lru_cache(maxsize=CACHE_SIZE)
def clean_title(title):
    if not title:
        return ""
    
    lower_title = title.lower()
    for key, values in SPECIAL_CASE_MAPPINGS.items():
        if any(special_case.lower() in lower_title for special_case in values):
            return key
    
    if "cumverse free download" in lower_title:
        return "cumverse"
    if "dark of chroe free download" in lower_title:
        return "dark of chroe"
    if "furry sex resort" in lower_title and ("free download" in lower_title or "uncensored" in lower_title):
        return "furry sex resort"
    if "busty milf and summer country sex life" in lower_title:
        return "busty milf and summer country sex life"
    
    language = detect_language(title)
    
    pre_cleaned_title = COMPILED_REGEX_TITLE_CLEANING.sub(" ", title)
    pre_cleaned_title = EMOJI_PATTERN.sub("", pre_cleaned_title)
    pre_cleaned_title = re.sub(r'\s+', ' ', pre_cleaned_title).strip()
    
    if language in ["chinese", "japanese", "korean"]:
        parenthesis_match = re.search(r'\(([^)]+)\)', title)
        if parenthesis_match:
            parenthesis_content = parenthesis_match.group(1).strip()
            if all(ord(c) < 128 for c in parenthesis_content):
                return parenthesis_content.lower()
        
        if language == "chinese":
            transliterated = transliterate_chinese(pre_cleaned_title)
            if transliterated != pre_cleaned_title and len(transliterated.strip()) >= MIN_CATEGORY_TITLE_LENGTH:
                return transliterated.strip().lower()
        
        return re.sub(r'\s+', ' ', pre_cleaned_title).strip().lower()
    
    normalized_title = normalize_special_chars(pre_cleaned_title)
    
    base_title = normalized_title if normalized_title and len(normalized_title) >= MIN_CATEGORY_TITLE_LENGTH else pre_cleaned_title
    cleaned_title = re.sub(r'\s+', ' ', base_title).strip()
    
    tokens = [token for token in cleaned_title.split() 
              if not any(word in token.lower() for word in ['season', 'edition', 'version', 'v\d'])]
    core_title = ' '.join(tokens)
    
    if len(core_title) < MIN_CATEGORY_TITLE_LENGTH and len(cleaned_title) >= MIN_CATEGORY_TITLE_LENGTH:
        return cleaned_title
    
    if len(core_title) < MIN_CATEGORY_TITLE_LENGTH and len(cleaned_title) < MIN_CATEGORY_TITLE_LENGTH:
        if any(ord(c) >= 128 for c in pre_cleaned_title):
            return pre_cleaned_title
    
    return core_title

def process_game(game_obj, adult_games_titles, adult_original_titles, software_titles, software_original_titles, vr_games_titles, vr_original_titles):
    """Processa um único jogo para categorização - função para processamento paralelo"""
    original_game_title = game_obj.get('title', '').strip()
    game_title = clean_title(original_game_title)
    is_categorized = False
    categories = []
    
    if not game_title:
        unmatched_log = f"Skipped (empty title after cleaning): {original_game_title}"
        return {
            'game_obj': game_obj,
            'categories': [],
            'is_categorized': False,
            'unmatched_log': unmatched_log
        }

    # Verificar se o jogo está na blacklist
    for keyword in BLACKLIST_KEYWORDS:
        if keyword in game_title.lower():
            unmatched_log = f"Skipped (blacklisted keyword '{keyword}'): {original_game_title}"
            return {
                'game_obj': game_obj,
                'categories': [],
                'is_categorized': False,
                'unmatched_log': unmatched_log
            }
    
    # Sempre adicionar ao shisuy_source
    categories.append('shisuy_source')
    
    # Verificar jogos VR
    is_vr_match = False
    if vr_games_titles and game_title:
        best_match_vr = None
        score_vr = 0
        length_ratio_vr = 0.0
        
        # Stage 1: Filtro preliminar com token_sort_ratio
        preliminary_candidates_vr = process.extract(game_title, vr_games_titles, scorer=fuzz.token_sort_ratio, limit=MAX_PRELIMINARY_CANDIDATES)
        qualified_preliminary_candidates_vr = [(cand_title, cand_score) for cand_title, cand_score in preliminary_candidates_vr if cand_score >= PRELIMINARY_SCORE_THRESHOLD]
        
        if qualified_preliminary_candidates_vr:
            # Stage 2: Matching refinado com WRatio
            best_refined_match_vr_title = None
            highest_wratio_score_vr = 0
            
            for cand_title, _ in qualified_preliminary_candidates_vr:
                game_tokens = set(game_title.lower().split())
                cand_tokens = set(cand_title.lower().split())
                overlap = len(game_tokens & cand_tokens) / len(game_tokens | cand_tokens) if game_tokens | cand_tokens else 0
                
                if overlap >= 0.5:
                    current_wratio_score = cross_language_match(game_title, cand_title)
                    if current_wratio_score > highest_wratio_score_vr:
                        highest_wratio_score_vr = current_wratio_score
                        best_refined_match_vr_title = cand_title
            
            if best_refined_match_vr_title and highest_wratio_score_vr >= FUZZY_MATCH_THRESHOLD:
                best_match_vr = best_refined_match_vr_title
                score_vr = highest_wratio_score_vr
        
        if best_match_vr:
            is_candidate_vr = is_vr_title(best_match_vr)
            is_original_vr = is_vr_title(original_game_title)
            
            if is_candidate_vr and not is_original_vr:
                if compare_base_titles_for_vr(game_title, best_match_vr):
                    log_messages.append(f"{Fore.YELLOW}  └─ Rejeitado VR: '{best_match_vr}' (Mesmo jogo base, mas um é VR e outro não){Style.RESET_ALL}")
                else:
                    log_messages.append(f"{Fore.YELLOW}  └─ Rejeitado VR: '{best_match_vr}' (Jogo original não é VR){Style.RESET_ALL}")
            elif is_original_vr and not is_candidate_vr:
                if compare_base_titles_for_vr(game_title, best_match_vr):
                    log_messages.append(f"{Fore.YELLOW}  └─ Rejeitado VR: '{best_match_vr}' (Mesmo jogo base, mas um é VR e outro não){Style.RESET_ALL}")
                else:
                    log_messages.append(f"{Fore.YELLOW}  └─ Rejeitado VR: '{best_match_vr}' (Candidato não é VR mas o original é){Style.RESET_ALL}")
            elif score_vr >= FUZZY_MATCH_THRESHOLD:
                len_game_title_val = len(game_title)
                len_best_match_vr_val = len(best_match_vr)
                
                if len_game_title_val > 0 and len_best_match_vr_val > 0:
                    length_ratio_vr = min(len_game_title_val, len_best_match_vr_val) / max(len_game_title_val, len_best_match_vr_val)
                
                if length_ratio_vr >= MIN_LENGTH_RATIO_FOR_NORMAL_THRESHOLD:
                    is_vr_match = True
                elif length_ratio_vr >= MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION and score_vr >= STRICT_SCORE_THRESHOLD:
                    is_vr_match = True
        
        if is_vr_match:
            categories.append('vr')
            is_categorized = True
    
    # Verificar jogos adultos
    is_adult_match = False
    if adult_games_titles and game_title:
        best_match_adult = None
        score_adult = 0
        length_ratio_adult = 0.0
        
        # Stage 1: Filtro preliminar com token_sort_ratio
        preliminary_candidates_adult = process.extract(game_title, adult_games_titles, scorer=fuzz.token_sort_ratio, limit=MAX_PRELIMINARY_CANDIDATES)
        qualified_preliminary_candidates_adult = [(cand_title, cand_score) for cand_title, cand_score in preliminary_candidates_adult if cand_score >= PRELIMINARY_SCORE_THRESHOLD]
        
        if qualified_preliminary_candidates_adult:
            # Stage 2: Matching refinado com WRatio
            best_refined_match_adult_title = None
            highest_wratio_score_adult = 0
            
            for cand_title, _ in qualified_preliminary_candidates_adult:
                current_wratio_score = cross_language_match(game_title, cand_title)
                if current_wratio_score > highest_wratio_score_adult:
                    highest_wratio_score_adult = current_wratio_score
                    best_refined_match_adult_title = cand_title
            
            if best_refined_match_adult_title and highest_wratio_score_adult >= FUZZY_MATCH_THRESHOLD:
                best_match_adult = best_refined_match_adult_title
                score_adult = highest_wratio_score_adult
        
        if best_match_adult:
            if score_adult >= FUZZY_MATCH_THRESHOLD:
                len_game_title_val = len(game_title)
                len_best_match_adult_val = len(best_match_adult)
                
                if len_game_title_val > 0 and len_best_match_adult_val > 0:
                    length_ratio_adult = min(len_game_title_val, len_best_match_adult_val) / max(len_game_title_val, len_best_match_adult_val)
                
                if length_ratio_adult >= MIN_LENGTH_RATIO_FOR_NORMAL_THRESHOLD:
                    is_adult_match = True
                elif length_ratio_adult >= MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION and score_adult >= STRICT_SCORE_THRESHOLD:
                    is_adult_match = True
        
        if is_adult_match:
            categories.append('adult')
            is_categorized = True
    
    # Verificar software
    is_software_match = False
    if software_titles and game_title:
        best_match_software = None
        score_software = 0
        length_ratio_software = 0.0
        
        # Stage 1: Filtro preliminar com token_sort_ratio
        preliminary_candidates_software = process.extract(game_title, software_titles, scorer=fuzz.token_sort_ratio, limit=MAX_PRELIMINARY_CANDIDATES)
        qualified_preliminary_candidates_software = [(cand_title, cand_score) for cand_title, cand_score in preliminary_candidates_software if cand_score >= PRELIMINARY_SCORE_THRESHOLD]
        
        if qualified_preliminary_candidates_software:
            # Stage 2: Matching refinado com WRatio
            best_refined_match_software_title = None
            highest_wratio_score_software = 0
            
            for cand_title, _ in qualified_preliminary_candidates_software:
                current_wratio_score = cross_language_match(game_title, cand_title)
                if current_wratio_score > highest_wratio_score_software:
                    highest_wratio_score_software = current_wratio_score
                    best_refined_match_software_title = cand_title
            
            if best_refined_match_software_title and highest_wratio_score_software >= FUZZY_MATCH_THRESHOLD:
                best_match_software = best_refined_match_software_title
                score_software = highest_wratio_score_software
        
        if best_match_software:
            if score_software >= FUZZY_MATCH_THRESHOLD:
                len_game_title_val = len(game_title)
                len_best_match_software_val = len(best_match_software)
                
                if len_game_title_val > 0 and len_best_match_software_val > 0:
                    length_ratio_software = min(len_game_title_val, len_best_match_software_val) / max(len_game_title_val, len_best_match_software_val)
                
                if length_ratio_software >= MIN_LENGTH_RATIO_FOR_NORMAL_THRESHOLD:
                    is_software_match = True
                elif length_ratio_software >= MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION and score_software >= STRICT_SCORE_THRESHOLD:
                    is_software_match = True
        
        if is_software_match:
            categories.append('software')
            is_categorized = True
    
    # Lidar com jogos não categorizados
    unmatched_log = None
    if not is_categorized and game_title:
        closest_matches = []
        
        if adult_games_titles:
            adult_match = process.extractOne(game_title, adult_games_titles)
            if adult_match:
                closest_matches.append(("Adult", adult_match[0], adult_match[1]))
        
        if software_titles:
            software_match = process.extractOne(game_title, software_titles)
            if software_match:
                closest_matches.append(("Software", software_match[0], software_match[1]))
        
        if vr_games_titles:
            vr_match = process.extractOne(game_title, vr_games_titles)
            if vr_match:
                closest_matches.append(("VR", vr_match[0], vr_match[1]))
        
        closest_matches.sort(key=lambda x: x[2], reverse=True)
        
        if closest_matches:
            best_category, best_match, best_score = closest_matches[0]
            unmatched_log = f"Unmatched: '{original_game_title}' (Cleaned: '{game_title}') - Closest match: {best_category} - '{best_match}' (Score: {best_score})"
        else:
            unmatched_log = f"Unmatched: '{original_game_title}' (Cleaned: '{game_title}') - No close matches found"
    
    return {
        'game_obj': game_obj,
        'categories': categories,
        'is_categorized': is_categorized,
        'unmatched_log': unmatched_log
    }

def main():
    start_time = time.time()
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    
    print(f"{Fore.CYAN}Starting game categorization with {NUM_PROCESSES} processes...{Style.RESET_ALL}")

    # Load the main games file
    valid_games_data = load_json_file(VALID_GAMES_FILE)
    if not valid_games_data or 'downloads' not in valid_games_data:
        print(f"{Fore.RED}Error: '{VALID_GAMES_FILE}' is missing or not in the expected format.{Style.RESET_ALL}")
        return

    all_games_to_process = valid_games_data.get('downloads', [])[:MAX_GAMES_TO_PROCESS]
    if not all_games_to_process:
        print(f"{Fore.YELLOW}No games found in '{VALID_GAMES_FILE}' to process.{Style.RESET_ALL}")
        return

    # Load category game lists
    print(f"{Fore.BLUE}Loading category lists...{Style.RESET_ALL}")
    adult_games_titles, adult_original_titles = get_titles_from_list(load_json_file(ADULT_GAMES_FILE))
    software_titles, software_original_titles = get_titles_from_list(load_json_file(SOFTWARE_FILE))
    vr_games_titles, vr_original_titles = get_titles_from_list(load_json_file(VR_GAMES_FILE))

    # Initialize lists for categorized games
    categorized_shisuy_source = []
    categorized_adult = []
    categorized_software = []
    categorized_vr = []
    unmatched_games_log = [] # For logging titles of unmatched games

    # Process games in parallel
    print(f"\n{Fore.CYAN}Processing {len(all_games_to_process)} games in parallel...{Style.RESET_ALL}")
    
    # Dividir jogos em lotes para processamento paralelo
    batches = [all_games_to_process[i:i+BATCH_SIZE] for i in range(0, len(all_games_to_process), BATCH_SIZE)]
    
    with tqdm(total=len(all_games_to_process), desc="Categorizing Games", unit="game") as pbar:
        for batch in batches:
            # Processar o lote atual em paralelo
            with ProcessPoolExecutor(max_workers=NUM_PROCESSES, mp_context=mp_context) as executor:
                # Preparar argumentos para cada jogo no lote
                futures = [executor.submit(
                    process_game, 
                    game_obj, 
                    adult_games_titles, 
                    adult_original_titles, 
                    software_titles, 
                    software_original_titles, 
                    vr_games_titles, 
                    vr_original_titles
                ) for game_obj in batch]
                
                # Coletar resultados à medida que são concluídos
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        game_obj = result['game_obj']
                        categories = result['categories']
                        unmatched_log = result['unmatched_log']
                        
                        # Categorizar o jogo com base nos resultados
                        if 'shisuy_source' in categories:
                            categorized_shisuy_source.append(game_obj)
                        if 'adult' in categories:
                            categorized_adult.append(game_obj)
                        if 'software' in categories:
                            categorized_software.append(game_obj)
                        if 'vr' in categories:
                            categorized_vr.append(game_obj)
                        
                        # Registrar jogos não correspondidos
                        if unmatched_log:
                            unmatched_games_log.append(unmatched_log)
                        
                        # Atualizar a barra de progresso
                        pbar.update(1)
                    except Exception as e:
                        print(f"{Fore.RED}Error processing game: {str(e)}{Style.RESET_ALL}")



    # Save the categorized lists
    save_json_file({'name': 'AIO | Shisuys', 'downloads': categorized_shisuy_source}, CATEGORIZED_SHISUY_SOURCE_FILE)
    save_json_file({'name': 'Shisuys Adult', 'downloads': categorized_adult}, CATEGORIZED_ADULT_GAMES_FILE)
    save_json_file({'name': 'Shisuys Software', 'downloads': categorized_software}, CATEGORIZED_SOFTWARE_FILE)
    save_json_file({'name': 'Shisuys VR', 'downloads': categorized_vr}, CATEGORIZED_VR_GAMES_FILE)

    # Log unmatched games
    if unmatched_games_log:
        with open(UNMATCHED_GAMES_FILE, 'w', encoding='utf-8') as f_log:
            for log_entry in unmatched_games_log:
                f_log.write(log_entry + "\n")
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Exibir estatísticas
    print(f"\n{Fore.GREEN}=== Estatísticas de Categorização ==={Style.RESET_ALL}")
    print(f"{Fore.CYAN}Total de jogos processados: {len(all_games_to_process)}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Jogos categorizados: {len(categorized_shisuy_source)}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Jogos adultos: {len(categorized_adult)} ({len(categorized_adult)/len(all_games_to_process)*100:.1f}%){Style.RESET_ALL}")
    print(f"{Fore.CYAN}Software: {len(categorized_software)} ({len(categorized_software)/len(all_games_to_process)*100:.1f}%){Style.RESET_ALL}")
    print(f"{Fore.GREEN}Jogos VR: {len(categorized_vr)} ({len(categorized_vr)/len(all_games_to_process)*100:.1f}%){Style.RESET_ALL}")
    print(f"{Fore.RED}Jogos não categorizados: {len(unmatched_games_log)} ({len(unmatched_games_log)/len(all_games_to_process)*100:.1f}%){Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Tempo total de execução: {execution_time:.2f} segundos{Style.RESET_ALL}")

if __name__ == "__main__":
    main()