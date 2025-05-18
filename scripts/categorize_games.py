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
OUTPUT_DIR = SOURCE_DATA_DIR # Output to the same directory as valid_games.json
CATEGORIZED_SHISUY_SOURCE_FILE = os.path.join(OUTPUT_DIR, 'categorized_shisuy_source.json')
CATEGORIZED_ADULT_GAMES_FILE = os.path.join(OUTPUT_DIR, 'categorized_adult_games.json')
CATEGORIZED_SOFTWARE_FILE = os.path.join(OUTPUT_DIR, 'categorized_software.json')
CATEGORIZED_VR_GAMES_FILE = os.path.join(OUTPUT_DIR, 'categorized_vr_games.json')
UNMATCHED_GAMES_FILE = os.path.join(OUTPUT_DIR, 'unmatched_games.log') # For logging unmatched games

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
    """Extracts titles from a list of game objects, filtering out very short titles."""
    if not game_list_data or not isinstance(game_list_data, list):
        return []
    titles = []
    for game in game_list_data:
        title = game.get('title', '')
        if title:
            cleaned = clean_title(title)
            if len(cleaned) >= MIN_CATEGORY_TITLE_LENGTH:
                titles.append(cleaned)
            # else:
            #     print(f"Skipping short category title: '{cleaned}' (original: '{title}')") # Optional: for debugging
    return titles

# Regex for cleaning titles, removing common irrelevant parts
REGEX_TITLE_CLEANING = r""" # Using triple quotes for readability
    \(.*?\) |                                      # Text in parentheses (e.g., (Build 123), (Region Free))
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
        Royalty\sFree\sSprites                     # Specific to "Indie Graphics Bundle"
    )
    \s*                                            # Optional trailing whitespace
"""
COMPILED_REGEX_TITLE_CLEANING = re.compile(REGEX_TITLE_CLEANING, flags=re.IGNORECASE | re.VERBOSE)

def clean_title(title):
    """Cleans the game title using regex to remove common irrelevant parts."""
    if not title:
        return ""
    # Remove parts matched by regex, then strip leading/trailing whitespace
    cleaned_title = COMPILED_REGEX_TITLE_CLEANING.sub("", title) # Use pre-compiled regex
    # Remove multiple spaces that might result from substitutions
    cleaned_title = re.sub(r'\s+', ' ', cleaned_title).strip()
    
    # Additional processing for similar base names with different descriptors
    # Split into tokens and keep only the core words (excluding version numbers, editions etc.)
    tokens = [token for token in cleaned_title.split() 
              if not any(word in token.lower() for word in ['season', 'edition', 'version', 'v\d'])]
    core_title = ' '.join(tokens)
    
    return core_title if len(core_title) >= MIN_CATEGORY_TITLE_LENGTH else cleaned_title

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
    adult_games_titles = get_titles_from_list(load_json_file(ADULT_GAMES_FILE))
    software_titles = get_titles_from_list(load_json_file(SOFTWARE_FILE))
    vr_games_titles = get_titles_from_list(load_json_file(VR_GAMES_FILE))

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
                            current_wratio_score = fuzz.WRatio(game_title, cand_title)
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
                if score_vr >= FUZZY_MATCH_THRESHOLD: # Basic score qualification
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
                tqdm.write(f"{Fore.GREEN}  └─ Matched VR: '{best_match_vr}' (Score: {score_vr}, Ratio: {length_ratio_vr:.2f}){Style.RESET_ALL}")
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
                        current_wratio_score = fuzz.WRatio(game_title, cand_title)
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
                tqdm.write(f"{Fore.MAGENTA}  └─ Matched Adult: '{best_match_adult}' (Score: {score_adult}, Ratio: {length_ratio_adult:.2f}){Style.RESET_ALL}")
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
                        current_wratio_score = fuzz.WRatio(game_title, cand_title)
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