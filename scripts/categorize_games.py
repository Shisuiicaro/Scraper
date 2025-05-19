import json
import os
import re # Added for regex operations
from fuzzywuzzy import fuzz, process
from tqdm import tqdm # Added for progress bar
from colorama import Fore, Style, init as colorama_init # Added for colored output

# Initialize colorama
colorama_init(autoreset=True)

# --- Configuration ---
# Adjust this threshold for matching sensitivity (0-100). Higher means stricter matching.
FUZZY_MATCH_THRESHOLD = 90
MIN_CATEGORY_TITLE_LENGTH = 5  # Minimum length for a title from category lists to be considered for matching

# Additional thresholds for refining fuzzy matches based on length ratios
MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION = 0.45  # If (len(category_title) / len(game_title)) is below this, reject match outright.
MIN_LENGTH_RATIO_FOR_NORMAL_THRESHOLD = 0.65     # If length_ratio is below this (but >= MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION), require STRICT_SCORE_THRESHOLD.
                                                 # If length_ratio is above this, FUZZY_MATCH_THRESHOLD applies.
STRICT_SCORE_THRESHOLD = 96                      # Stricter score for moderately short category titles relative to game title.

# Configuration for two-stage matching
PRELIMINARY_SCORE_THRESHOLD = 80  # Threshold for the first pass with token_sort_ratio
MAX_PRELIMINARY_CANDIDATES = 5    # Max candidates to consider from the first pass

def cross_language_match(title1, title2):
    """Realiza matching entre títulos que podem estar em idiomas diferentes.
    Retorna um score de similaridade aprimorado para títulos multilíngues."""
    # Detectar idiomas dos títulos
    lang1 = detect_language(title1)
    lang2 = detect_language(title2)
    
    # Se ambos são do mesmo idioma, usar matching padrão
    if lang1 == lang2:
        return fuzz.WRatio(title1, title2)
    
    # Se um é chinês/japonês/coreano e o outro é latino, tentar abordagens especiais
    if (lang1 in ["chinese", "japanese", "korean"] and lang2 == "latin") or \
       (lang2 in ["chinese", "japanese", "korean"] and lang1 == "latin"):
        
        # Determinar qual é o título asiático e qual é o latino
        asian_title = title1 if lang1 in ["chinese", "japanese", "korean"] else title2
        latin_title = title2 if lang2 == "latin" else title1
        
        # Verificar se o título asiático tem uma versão em inglês entre parênteses
        parenthesis_match = re.search(r'\(([^)]+)\)', asian_title)
        if parenthesis_match:
            english_version = parenthesis_match.group(1).strip().lower()
            # Comparar a versão em inglês com o título latino
            return fuzz.WRatio(english_version, latin_title)
        
        # Para chinês, tentar transliteração
        if lang1 == "chinese" or lang2 == "chinese":
            transliterated = transliterate_chinese(asian_title)
            if transliterated != asian_title:
                return fuzz.WRatio(transliterated, latin_title)
    
    # Caso padrão: normalizar ambos e comparar
    norm1 = normalize_special_chars(title1)
    norm2 = normalize_special_chars(title2)
    return fuzz.WRatio(norm1, norm2)

# Configuração para verificação de jogos VR
VR_KEYWORDS = ['vr', 'virtual reality', 'oculus', 'htc vive', 'valve index', 'psvr']

def is_vr_title(title):
    """Verifica se um título contém indicadores de que é um jogo VR."""
    if not title:
        return False
    
    # Verificar se o título contém alguma das palavras-chave de VR
    title_lower = title.lower()
    for keyword in VR_KEYWORDS:
        if re.search(r'\b' + re.escape(keyword) + r'\b', title_lower):
            return True
    
    # Verificar se o título contém VR entre parênteses ou colchetes
    if re.search(r'[\(\[]\s*VR\s*[\)\]]', title, re.IGNORECASE):
        return True
        
    return False

def compare_base_titles_for_vr(game_title, vr_title):
    """Compara os títulos base para verificar se são o mesmo jogo, mas um é VR e o outro não.
    Retorna True se forem o mesmo jogo base (ignorando o 'VR'), False caso contrário."""
    # Remove 'VR' e espaços extras do título VR para comparação
    base_vr_title = re.sub(r'\bVR\b', '', vr_title, flags=re.IGNORECASE).strip()
    base_vr_title = re.sub(r'\s+', ' ', base_vr_title).strip()
    
    # Compara os títulos base
    # Se o score for muito alto, são provavelmente o mesmo jogo
    similarity_score = fuzz.ratio(game_title.lower(), base_vr_title.lower())
    return similarity_score >= 90  # Threshold alto para evitar falsos positivos

# --- File Paths ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
SOURCE_DATA_DIR = os.path.join(DATA_DIR, 'source_data')
GAMES_LIST_DIR = os.path.join(DATA_DIR, 'games_list')

VALID_GAMES_FILE = os.path.join(SOURCE_DATA_DIR, 'valid_games.json')
ADULT_GAMES_FILE = os.path.join(GAMES_LIST_DIR, 'adult_games.json')
SOFTWARE_FILE = os.path.join(GAMES_LIST_DIR, 'software.json')
VR_GAMES_FILE = os.path.join(GAMES_LIST_DIR, 'vr_games.json')

# Output files
OUTPUT_DIR = os.path.join(DATA_DIR, 'categorized') # Mover para uma pasta separada fora de source_data
os.makedirs(OUTPUT_DIR, exist_ok=True) # Garantir que a pasta existe
CATEGORIZED_SHISUY_SOURCE_FILE = os.path.join(OUTPUT_DIR, 'categorized_shisuy_source.json')
CATEGORIZED_ADULT_GAMES_FILE = os.path.join(OUTPUT_DIR, 'categorized_adult_games.json')
CATEGORIZED_SOFTWARE_FILE = os.path.join(OUTPUT_DIR, 'categorized_software.json')
CATEGORIZED_VR_GAMES_FILE = os.path.join(OUTPUT_DIR, 'categorized_vr_games.json')
UNMATCHED_GAMES_FILE = os.path.join(OUTPUT_DIR, 'unmatched_games.log') # Para registrar jogos não correspondentes

def load_json_file(file_path):
    """Loads a JSON file and returns its content."""
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
    """Saves data to a JSON file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f"{Fore.GREEN}Successfully saved: {file_path}{Style.RESET_ALL}")

def get_titles_from_list(game_list_data):
    """Extrai títulos de uma lista de objetos de jogos, filtrando títulos muito curtos."""
    if not game_list_data or not isinstance(game_list_data, list):
        return []
    titles = []
    original_titles = {}  # Dicionário para mapear títulos limpos para originais
    
    for game in game_list_data:
        title = game.get('title', '')
        if title:
            cleaned = clean_title(title)
            if len(cleaned) >= MIN_CATEGORY_TITLE_LENGTH:
                titles.append(cleaned)
                # Armazenar o mapeamento do título limpo para o original
                original_titles[cleaned] = title
            # else:
            #     print(f"Skipping short category title: '{cleaned}' (original: '{title}')") # Optional: for debugging
    
    # Retornar tanto a lista de títulos limpos quanto o mapeamento para os originais
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

def detect_language(text):
    """Detecta o idioma principal do texto com base nos caracteres.
    Retorna 'chinese', 'japanese', 'korean', 'latin' ou 'other'."""
    if not text:
        return "unknown"
    
    # Contadores para diferentes faixas de caracteres
    chinese_count = 0
    japanese_count = 0
    korean_count = 0
    latin_count = 0
    
    for char in text:
        code = ord(char)
        # Caracteres chineses (simplificado e tradicional)
        if (0x4E00 <= code <= 0x9FFF) or (0x3400 <= code <= 0x4DBF):
            chinese_count += 1
        # Caracteres japoneses específicos (hiragana, katakana)
        elif (0x3040 <= code <= 0x309F) or (0x30A0 <= code <= 0x30FF):
            japanese_count += 1
        # Caracteres coreanos (Hangul)
        elif 0xAC00 <= code <= 0xD7A3:
            korean_count += 1
        # Caracteres latinos (incluindo acentuados)
        elif (0x0020 <= code <= 0x007F) or (0x00A0 <= code <= 0x024F):
            latin_count += 1
    
    # Determinar o idioma predominante
    total_chars = len(text.replace(" ", ""))  # Ignorar espaços
    if total_chars == 0:
        return "unknown"
    
    chinese_ratio = chinese_count / total_chars
    japanese_ratio = japanese_count / total_chars
    korean_ratio = korean_count / total_chars
    latin_ratio = latin_count / total_chars
    
    # Determinar o idioma com base na maior proporção
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

def transliterate_chinese(text):
    """Tenta transliterar texto chinês para equivalentes em inglês usando mapeamentos conhecidos."""
    if not text:
        return ""
    
    result = text
    for cn_text, en_text in CHARACTER_MAPPINGS.items():
        result = result.replace(cn_text, en_text)
    
    return result

def normalize_special_chars(text):
    """Normaliza caracteres especiais e não-ASCII para melhorar o matching.
    Preserva caracteres originais enquanto cria versões alternativas para matching.
    Agora com suporte aprimorado para chinês e outros idiomas não-latinos."""
    import unicodedata
    if not text:
        return ""
    
    # Detectar o idioma principal do texto
    language = detect_language(text)
    
    # Primeiro, tenta extrair texto entre parênteses que pode conter a versão em inglês
    # Útil para casos como "暗黑的克蘿薇 (DARK OF CHROE)"
    parenthesis_match = re.search(r'\(([^)]+)\)', text)
    english_version = ""
    if parenthesis_match:
        parenthesis_content = parenthesis_match.group(1).strip()
        # Se o conteúdo entre parênteses parece ser em inglês (caracteres ASCII), guarde-o
        if all(ord(c) < 128 for c in parenthesis_content):
            english_version = parenthesis_content.lower()
    
    # Se temos uma versão em inglês entre parênteses, use-a como principal
    if english_version:
        return english_version
    
    # Para textos em chinês, tente transliterar usando mapeamentos conhecidos
    if language == "chinese":
        transliterated = transliterate_chinese(text)
        # Se a transliteração produziu mudanças significativas, use-a
        if transliterated != text and len(transliterated.strip()) >= MIN_CATEGORY_TITLE_LENGTH:
            return transliterated.strip().lower()
    
    # Normaliza caracteres Unicode para suas formas decompostas
    normalized = unicodedata.normalize('NFKD', text)
    
    # Cria uma versão ASCII do texto (para compatibilidade com o método anterior)
    ascii_text = ''.join(c for c in normalized if ord(c) < 128)
    ascii_text = re.sub(r'\s+', ' ', ascii_text).strip().lower()
    
    # Se a versão ASCII não está vazia e tem comprimento razoável, use-a
    if ascii_text and len(ascii_text) >= MIN_CATEGORY_TITLE_LENGTH:
        return ascii_text
    
    # Para idiomas não-latinos, preserve os caracteres originais
    if language in ["chinese", "japanese", "korean"]:
        # Apenas normalize espaços e case
        preserved_text = re.sub(r'\s+', ' ', text).strip().lower()
        return preserved_text
    
    # Para outros idiomas, tente uma abordagem híbrida
    # Manter caracteres não-ASCII que não puderam ser normalizados
    hybrid_text = ''
    for i, char in enumerate(text):
        # Se o caractere é ASCII ou foi normalizado para ASCII, use a versão ASCII
        if ord(char) < 128 or (i < len(normalized) and ord(normalized[i]) < 128):
            hybrid_text += char.lower()
        # Caso contrário, mantenha o caractere original
        else:
            hybrid_text += char
    
    hybrid_text = re.sub(r'\s+', ' ', hybrid_text).strip()
    
    # Se o texto híbrido tem comprimento razoável, use-o
    if len(hybrid_text) >= MIN_CATEGORY_TITLE_LENGTH:
        return hybrid_text
    
    # Último recurso: preservar o texto original normalizado
    preserved_text = re.sub(r'\s+', ' ', text).strip().lower()
    return preserved_text

def clean_title(title):
    """Limpa o título do jogo usando regex para remover partes irrelevantes comuns.
    Melhorado para lidar com caracteres especiais e títulos em diferentes idiomas,
    com suporte especial para chinês, japonês e coreano."""
    if not title:
        return ""
    
    # Verificar se é um caso especial antes de qualquer limpeza
    lower_title = title.lower()
    for key, values in SPECIAL_CASE_MAPPINGS.items():
        if any(special_case.lower() in lower_title for special_case in values):
            return key
    
    # Casos específicos para os problemas mencionados
    if "cumverse free download" in lower_title:
        return "cumverse"
    if "dark of chroe free download" in lower_title:
        return "dark of chroe"
    if "furry sex resort" in lower_title and ("free download" in lower_title or "uncensored" in lower_title):
        return "furry sex resort"
    if "busty milf and summer country sex life" in lower_title:
        return "busty milf and summer country sex life"
    
    # Detectar o idioma do título original
    language = detect_language(title)
    
    # Primeiro, remover partes correspondentes ao regex e espaços em branco do título original
    # Isso ajuda a limpar o título antes da normalização
    pre_cleaned_title = COMPILED_REGEX_TITLE_CLEANING.sub(" ", title)
    pre_cleaned_title = EMOJI_PATTERN.sub("", pre_cleaned_title)
    pre_cleaned_title = re.sub(r'\s+', ' ', pre_cleaned_title).strip()
    
    # Para idiomas asiáticos, preservar mais caracteres originais
    if language in ["chinese", "japanese", "korean"]:
        # Verificar se há uma versão em inglês entre parênteses
        parenthesis_match = re.search(r'\(([^)]+)\)', title)
        if parenthesis_match:
            parenthesis_content = parenthesis_match.group(1).strip()
            # Se o conteúdo entre parênteses parece ser em inglês, use-o
            if all(ord(c) < 128 for c in parenthesis_content):
                return parenthesis_content.lower()
        
        # Para chinês, tentar transliteração
        if language == "chinese":
            transliterated = transliterate_chinese(pre_cleaned_title)
            if transliterated != pre_cleaned_title and len(transliterated.strip()) >= MIN_CATEGORY_TITLE_LENGTH:
                return transliterated.strip().lower()
        
        # Se não houver versão em inglês ou transliteração, preservar os caracteres originais
        # Apenas normalizar espaços e case
        return re.sub(r'\s+', ' ', pre_cleaned_title).strip().lower()
    
    # Para outros idiomas, usar o processo normal de normalização
    normalized_title = normalize_special_chars(pre_cleaned_title)
    
    # Se a normalização produziu um resultado válido, use-o como base
    if normalized_title and len(normalized_title) >= MIN_CATEGORY_TITLE_LENGTH:
        base_title = normalized_title
    else:
        # Caso contrário, continue com o título pré-limpo
        base_title = pre_cleaned_title
    
    # Remover múltiplos espaços que podem resultar das substituições
    cleaned_title = re.sub(r'\s+', ' ', base_title).strip()
    
    # Processamento adicional para nomes base semelhantes com descritores diferentes
    # Dividir em tokens e manter apenas as palavras principais
    tokens = [token for token in cleaned_title.split() 
              if not any(word in token.lower() for word in ['season', 'edition', 'version', 'v\d'])]
    core_title = ' '.join(tokens)
    
    # Se o título principal é muito curto, voltar para o título limpo
    if len(core_title) < MIN_CATEGORY_TITLE_LENGTH and len(cleaned_title) >= MIN_CATEGORY_TITLE_LENGTH:
        return cleaned_title
    
    # Se ambos são muito curtos, mas temos caracteres não-ASCII, preservar o título original limpo
    if len(core_title) < MIN_CATEGORY_TITLE_LENGTH and len(cleaned_title) < MIN_CATEGORY_TITLE_LENGTH:
        # Verificar se o título original tem caracteres não-ASCII que devem ser preservados
        has_non_ascii = any(ord(c) >= 128 for c in pre_cleaned_title)
        if has_non_ascii:
            return pre_cleaned_title
    
    return core_title

def main():
    # Set stdout encoding to UTF-8 to handle Unicode characters
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    
    print(f"{Fore.CYAN}Starting game categorization script...{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Ensure 'fuzzywuzzy', 'python-Levenshtein', 'tqdm', and 'colorama' are installed.{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}pip install fuzzywuzzy python-Levenshtein tqdm colorama{Style.RESET_ALL}")

    # Load the main games file
    valid_games_data = load_json_file(VALID_GAMES_FILE)
    if not valid_games_data or 'downloads' not in valid_games_data:
        print(f"{Fore.RED}Error: '{VALID_GAMES_FILE}' is missing or not in the expected format.{Style.RESET_ALL}")
        return

    all_games_to_process = valid_games_data.get('downloads', [])
    if not all_games_to_process:
        print(f"{Fore.YELLOW}No games found in '{VALID_GAMES_FILE}' to process.{Style.RESET_ALL}")
        return

    # Load category game lists
    adult_games_titles, adult_original_titles = get_titles_from_list(load_json_file(ADULT_GAMES_FILE))
    software_titles, software_original_titles = get_titles_from_list(load_json_file(SOFTWARE_FILE))
    vr_games_titles, vr_original_titles = get_titles_from_list(load_json_file(VR_GAMES_FILE))

    print(f"{Fore.BLUE}Loaded {len(adult_games_titles)} titles from {ADULT_GAMES_FILE}{Style.RESET_ALL}")
    print(f"{Fore.BLUE}Loaded {len(software_titles)} titles from {SOFTWARE_FILE}{Style.RESET_ALL}")
    print(f"{Fore.BLUE}Loaded {len(vr_games_titles)} titles from {VR_GAMES_FILE}{Style.RESET_ALL}")

    # Initialize lists for categorized games
    categorized_shisuy_source = []
    categorized_adult = []
    categorized_software = []
    categorized_vr = []
    unmatched_games_log = [] # For logging titles of unmatched games

    # Process each game with tqdm for progress bar
    print(f"\n{Fore.CYAN}Processing {len(all_games_to_process)} games...{Style.RESET_ALL}")
    for game_obj in tqdm(all_games_to_process, desc="Categorizing Games", unit="game"):
        original_game_title = game_obj.get('title', '').strip() # Keep original for output if needed
        game_title = clean_title(original_game_title) # Clean title for matching
        is_categorized = False # Flag to track if game was categorized

        tqdm.write(f"\n{Fore.WHITE}Processing: '{original_game_title}'{Style.RESET_ALL}")
        if original_game_title != game_title:
            tqdm.write(f"{Fore.LIGHTBLACK_EX}Cleaned:    '{game_title}'{Style.RESET_ALL}")
        else:
            tqdm.write(f"{Fore.LIGHTBLACK_EX}Cleaned:    (No changes){Style.RESET_ALL}")

        if not game_title:
            log_message = f"Skipped (empty title after cleaning): {original_game_title}"
            tqdm.write(f"{Fore.YELLOW}  └─ {log_message}{Style.RESET_ALL}")
            unmatched_games_log.append(log_message)
            continue

        # All games go into shisuy_source
        categorized_shisuy_source.append(game_obj)

        # Check for VR games
        if vr_games_titles:
            best_match_vr = None
            score_vr = 0
            if game_title: # Ensure game_title is not empty for matching
                # Stage 1: Preliminary filter with token_sort_ratio
                preliminary_candidates_vr = process.extract(game_title, vr_games_titles, scorer=fuzz.token_sort_ratio, limit=MAX_PRELIMINARY_CANDIDATES)
                
                qualified_preliminary_candidates_vr = [
                    (cand_title, cand_score) for cand_title, cand_score in preliminary_candidates_vr if cand_score >= PRELIMINARY_SCORE_THRESHOLD
                ]

                if qualified_preliminary_candidates_vr:
                    # Stage 2: Refined matching with token overlap and WRatio
                    best_refined_match_vr_title = None
                    highest_wratio_score_vr = 0
                    
                    # Iterate through titles from qualified candidates
                    for cand_title, _ in qualified_preliminary_candidates_vr:
                        # Check token overlap first
                        game_tokens = set(game_title.lower().split())
                        cand_tokens = set(cand_title.lower().split())
                        overlap = len(game_tokens & cand_tokens) / len(game_tokens | cand_tokens)
                        
                        # Only proceed with WRatio if there's significant token overlap
                        if overlap >= 0.5:
                            # Use cross-language matching for better handling of different languages
                            current_wratio_score = cross_language_match(game_title, cand_title)
                            if current_wratio_score > highest_wratio_score_vr:
                                highest_wratio_score_vr = current_wratio_score
                                best_refined_match_vr_title = cand_title
                    
                    # If a good match is found after WRatio refinement
                    if best_refined_match_vr_title and highest_wratio_score_vr >= FUZZY_MATCH_THRESHOLD:
                        best_match_vr = best_refined_match_vr_title
                        score_vr = highest_wratio_score_vr
            
            is_vr_match = False
            length_ratio_vr = 0.0 # Initialize for printing
            if best_match_vr and game_title: # Ensure titles are not empty
                # Verificação especial para jogos VR - garantir que não haja falsos positivos
                # Usar a função especializada para detectar jogos VR
                is_candidate_vr = is_vr_title(best_match_vr)
                is_original_vr = is_vr_title(original_game_title)
                
                # Se o candidato é VR mas o original não é, verificar se são o mesmo jogo base
                if is_candidate_vr and not is_original_vr:
                    # Verificar se são o mesmo jogo base (ex: GREEN HELL vs GREEN HELL VR)
                    if compare_base_titles_for_vr(game_title, best_match_vr):
                        tqdm.write(f"{Fore.YELLOW}  └─ Rejeitado VR: '{best_match_vr}' (Mesmo jogo base, mas um é VR e outro não){Style.RESET_ALL}")
                        is_vr_match = False
                    else:
                        tqdm.write(f"{Fore.YELLOW}  └─ Rejeitado VR: '{best_match_vr}' (Jogo original não é VR){Style.RESET_ALL}")
                        is_vr_match = False
                # Se o original é VR mas o candidato não é, também verificar se são o mesmo jogo base
                elif is_original_vr and not is_candidate_vr:
                    if compare_base_titles_for_vr(game_title, best_match_vr):
                        tqdm.write(f"{Fore.YELLOW}  └─ Rejeitado VR: '{best_match_vr}' (Mesmo jogo base, mas um é VR e outro não){Style.RESET_ALL}")
                        is_vr_match = False
                    else:
                        tqdm.write(f"{Fore.YELLOW}  └─ Rejeitado VR: '{best_match_vr}' (Candidato não é VR mas o original é){Style.RESET_ALL}")
                        is_vr_match = False
                # Caso contrário, aplicar a lógica normal de matching
                elif score_vr >= FUZZY_MATCH_THRESHOLD: # Basic score qualification
                    len_game_title_val = len(game_title)
                    len_best_match_vr_val = len(best_match_vr)
                    
                    if len_game_title_val > 0 and len_best_match_vr_val > 0: # Ensure both lengths are positive
                        length_ratio_vr = min(len_game_title_val, len_best_match_vr_val) / max(len_game_title_val, len_best_match_vr_val)
                    else:
                        length_ratio_vr = 0 # Avoid division by zero if one title is empty (should be caught earlier)

                    if length_ratio_vr >= MIN_LENGTH_RATIO_FOR_NORMAL_THRESHOLD:
                        is_vr_match = True 
                    elif length_ratio_vr >= MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION:
                            if score_vr >= STRICT_SCORE_THRESHOLD:
                                is_vr_match = True
                        # else: length_ratio_vr < MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION, is_vr_match remains False
            
            if is_vr_match:
                # Mostrar o título original do jogo VR para melhor compreensão
                original_vr_title = vr_original_titles.get(best_match_vr, best_match_vr)
                tqdm.write(f"{Fore.GREEN}  └─ Matched VR: '{best_match_vr}' (Original: '{original_vr_title}', Score: {score_vr}, Ratio: {length_ratio_vr:.2f}){Style.RESET_ALL}")
                categorized_vr.append(game_obj)
                is_categorized = True

        # Check for Adult games
        if adult_games_titles:
            best_match_adult = None
            score_adult = 0
            if game_title: # Ensure game_title is not empty for matching
                # Stage 1: Preliminary filter with token_sort_ratio
                preliminary_candidates_adult = process.extract(game_title, adult_games_titles, scorer=fuzz.token_sort_ratio, limit=MAX_PRELIMINARY_CANDIDATES)

                qualified_preliminary_candidates_adult = [
                    (cand_title, cand_score) for cand_title, cand_score in preliminary_candidates_adult if cand_score >= PRELIMINARY_SCORE_THRESHOLD
                ]

                if qualified_preliminary_candidates_adult:
                    # Stage 2: Refined matching with WRatio on the filtered candidates
                    best_refined_match_adult_title = None
                    highest_wratio_score_adult = 0

                    for cand_title, _ in qualified_preliminary_candidates_adult:
                        # Use cross-language matching for better handling of different languages
                        current_wratio_score = cross_language_match(game_title, cand_title)
                        if current_wratio_score > highest_wratio_score_adult:
                            highest_wratio_score_adult = current_wratio_score
                            best_refined_match_adult_title = cand_title
                    
                    if best_refined_match_adult_title and highest_wratio_score_adult >= FUZZY_MATCH_THRESHOLD:
                        best_match_adult = best_refined_match_adult_title
                        score_adult = highest_wratio_score_adult

            is_adult_match = False
            length_ratio_adult = 0.0 # Initialize for printing
            if best_match_adult and game_title:
                if score_adult >= FUZZY_MATCH_THRESHOLD:
                    len_game_title_val = len(game_title)
                    len_best_match_adult_val = len(best_match_adult)

                    if len_game_title_val > 0 and len_best_match_adult_val > 0: # Ensure both lengths are positive
                        length_ratio_adult = min(len_game_title_val, len_best_match_adult_val) / max(len_game_title_val, len_best_match_adult_val)
                    else:
                        length_ratio_adult = 0 # Avoid division by zero

                    if length_ratio_adult >= MIN_LENGTH_RATIO_FOR_NORMAL_THRESHOLD:
                        is_adult_match = True
                    elif length_ratio_adult >= MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION:
                            if score_adult >= STRICT_SCORE_THRESHOLD:
                                is_adult_match = True
                        # else: length_ratio_adult < MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION, is_adult_match remains False

            if is_adult_match:
                # Mostrar o título original do jogo adulto para melhor compreensão
                original_adult_title = adult_original_titles.get(best_match_adult, best_match_adult)
                tqdm.write(f"{Fore.MAGENTA}  └─ Matched Adult: '{best_match_adult}' (Original: '{original_adult_title}', Score: {score_adult}, Ratio: {length_ratio_adult:.2f}){Style.RESET_ALL}")
                categorized_adult.append(game_obj)
                is_categorized = True

        # Check for Software
        if software_titles:
            best_match_software = None
            score_software = 0
            if game_title: # Ensure game_title is not empty for matching
                # Stage 1: Preliminary filter with token_sort_ratio
                preliminary_candidates_software = process.extract(game_title, software_titles, scorer=fuzz.token_sort_ratio, limit=MAX_PRELIMINARY_CANDIDATES)

                qualified_preliminary_candidates_software = [
                    (cand_title, cand_score) for cand_title, cand_score in preliminary_candidates_software if cand_score >= PRELIMINARY_SCORE_THRESHOLD
                ]

                if qualified_preliminary_candidates_software:
                    # Stage 2: Refined matching with WRatio on the filtered candidates
                    best_refined_match_software_title = None
                    highest_wratio_score_software = 0

                    for cand_title, _ in qualified_preliminary_candidates_software:
                        # Use cross-language matching for better handling of different languages
                        current_wratio_score = cross_language_match(game_title, cand_title)
                        if current_wratio_score > highest_wratio_score_software:
                            highest_wratio_score_software = current_wratio_score
                            best_refined_match_software_title = cand_title

                    if best_refined_match_software_title and highest_wratio_score_software >= FUZZY_MATCH_THRESHOLD:
                        best_match_software = best_refined_match_software_title
                        score_software = highest_wratio_score_software
            
            is_software_match = False
            length_ratio_software = 0.0 # Initialize for printing
            if best_match_software and game_title:
                if score_software >= FUZZY_MATCH_THRESHOLD:
                    len_game_title_val = len(game_title)
                    len_best_match_software_val = len(best_match_software)

                    if len_game_title_val > 0 and len_best_match_software_val > 0: # Ensure both lengths are positive
                        length_ratio_software = min(len_game_title_val, len_best_match_software_val) / max(len_game_title_val, len_best_match_software_val)
                    else:
                        length_ratio_software = 0 # Avoid division by zero

                    if length_ratio_software >= MIN_LENGTH_RATIO_FOR_NORMAL_THRESHOLD:
                        is_software_match = True
                    elif length_ratio_software >= MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION:
                            if score_software >= STRICT_SCORE_THRESHOLD:
                                is_software_match = True
                        # else: length_ratio_software < MIN_LENGTH_RATIO_FOR_MATCH_CONSIDERATION, is_software_match remains False

            if is_software_match:
                tqdm.write(f"{Fore.CYAN}  └─ Matched Software: '{best_match_software}' (Score: {score_software}, Ratio: {length_ratio_software:.2f}){Style.RESET_ALL}")
                categorized_software.append(game_obj)
                is_categorized = True
        
        if not is_categorized and game_title: # Only log as unmatched if it wasn't skipped due to empty title
            log_message = f"Unmatched: '{original_game_title}' (Cleaned: '{game_title}')"
            # Check if it was already logged as skipped to avoid double logging in console for this specific case
            # This check is more for console clarity; the log file will be correct based on prior logic.
            if not (not game_title and f"Skipped (empty title after cleaning): {original_game_title}" in unmatched_games_log):
                 tqdm.write(f"{Fore.RED}  └─ {log_message}{Style.RESET_ALL}")
            unmatched_games_log.append(log_message)

    # Save the categorized lists
    save_json_file({'downloads': categorized_shisuy_source}, CATEGORIZED_SHISUY_SOURCE_FILE)
    save_json_file({'downloads': categorized_adult}, CATEGORIZED_ADULT_GAMES_FILE)
    save_json_file({'downloads': categorized_software}, CATEGORIZED_SOFTWARE_FILE)
    save_json_file({'downloads': categorized_vr}, CATEGORIZED_VR_GAMES_FILE)

    # Log unmatched games
    if unmatched_games_log:
        print(f"\n{Fore.MAGENTA}--- Unmatched Games ({len(unmatched_games_log)}) ---{Style.RESET_ALL}")
        with open(UNMATCHED_GAMES_FILE, 'w', encoding='utf-8') as f_log:
            for log_entry in unmatched_games_log:
                print(f"{Fore.YELLOW}{log_entry}{Style.RESET_ALL}")
                f_log.write(log_entry + "\n")
        print(f"{Fore.MAGENTA}Unmatched games logged to: {UNMATCHED_GAMES_FILE}{Style.RESET_ALL}")
    else:
        print(f"\n{Fore.GREEN}All games were categorized or skipped due to empty titles after cleaning.{Style.RESET_ALL}")

    print(f"\n{Fore.CYAN}Categorization complete.{Style.RESET_ALL}")

if __name__ == "__main__":
    main()