import os
import json
import time
from datetime import datetime
from typing import Dict, List, Any

# Base directory path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Data file paths
DATA_DIR = os.path.join(BASE_DIR, 'data')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
CONFIG_DIR = os.path.join(DATA_DIR, 'config')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')

# Processed data files
AIO_JSON = os.path.join(PROCESSED_DIR, 'aio_shisuy.json')
ADULT_JSON = os.path.join(PROCESSED_DIR, 'shisuys_adult.json')
SOFTWARE_JSON = os.path.join(PROCESSED_DIR, 'shisuys_software.json')
VR_JSON = os.path.join(PROCESSED_DIR, 'shisuys_vr.json')
STEAM_MATCHED_JSON = os.path.join(PROCESSED_DIR, 'steam_matched_games.json')

# Config files
INVALIDS_JSON = os.path.join(CONFIG_DIR, 'invalids.json')

# Log files
UNMATCHED_LOG = os.path.join(LOGS_DIR, 'unmatched_games.log')

# Cache for monitoring stats
STATS_CACHE = {}
STATS_CACHE_TIMESTAMP = 0
CACHE_DURATION = 300  # 5 minutes

def get_file_modification_time(file_path: str) -> float:
    """Get the last modification time of a file"""
    try:
        return os.path.getmtime(file_path)
    except (FileNotFoundError, PermissionError):
        return 0

def load_json_file(file_path: str) -> Dict:
    """Load a JSON file with error handling"""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    except (json.JSONDecodeError, PermissionError):
        return {}

def count_new_games(data_file: str, days: int = 7) -> int:
    """Count games added in the last X days"""
    data = load_json_file(data_file)
    if not data or 'downloads' not in data:
        return 0
    
    count = 0
    current_time = time.time()
    cutoff_time = current_time - (days * 24 * 60 * 60)  # Convert days to seconds
    
    for game in data.get('downloads', []):
        try:
            upload_date = game.get('uploadDate')
            if upload_date:
                # Handle both timestamp and ISO format
                if isinstance(upload_date, (int, float)):
                    game_time = upload_date
                else:
                    game_time = datetime.fromisoformat(upload_date.replace('Z', '+00:00')).timestamp()
                
                if game_time > cutoff_time:
                    count += 1
        except (ValueError, TypeError):
            continue
    
    return count

def count_invalidated_games() -> Dict[str, Any]:
    """Count invalidated games and calculate link integration percentage"""
    invalids_data = load_json_file(INVALIDS_JSON)
    total_invalid = len(invalids_data.get('invalid_games', []))
    
    # Calculate link integration percentage
    total_links = 0
    valid_links = 0
    
    for game in invalids_data.get('invalid_games', []):
        links = game.get('uris', [])
        total_links += len(links)
        valid_links += len([link for link in links if not link.get('invalid', False)])
    
    percentage = 0
    if total_links > 0:
        percentage = round((valid_links / total_links) * 100)
    
    return {
        'total': total_invalid,
        'percentage': percentage
    }

def count_new_matches(days: int = 7) -> Dict[str, int]:
    """Count new matches from categorizer for each source"""
    # This would require parsing logs or storing match data
    # For now, return placeholder data
    return {
        'adult': 0,
        'software': 0,
        'aio': 0,
        'vr': 0
    }

def get_recent_games(limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent games with images from steam_matched_games.json"""
    matched_data = load_json_file(STEAM_MATCHED_JSON)
    
    if not matched_data or not isinstance(matched_data, list):
        return []
    
    # Sort by upload date (newest first)
    sorted_games = sorted(
        matched_data, 
        key=lambda x: x.get('uploadDate', ''), 
        reverse=True
    )
    
    # Take the most recent games
    recent_games = sorted_games[:limit]
    
    # Format the response
    result = []
    for game in recent_games:
        result.append({
            'title': game.get('title', 'Unknown'),
            'uploadDate': game.get('uploadDate', ''),
            'category': game.get('category', 'Unknown'),
            'image': game.get('steam_data', {}).get('header_image', '')
        })
    
    return result

def get_monitoring_stats() -> Dict[str, Any]:
    """Get all monitoring statistics"""
    global STATS_CACHE, STATS_CACHE_TIMESTAMP
    
    current_time = time.time()
    
    # Return cached data if it's still valid
    if current_time - STATS_CACHE_TIMESTAMP < CACHE_DURATION and STATS_CACHE:
        return STATS_CACHE
    
    # Calculate new stats
    stats = {
        'newGames': {
            'adult': count_new_games(ADULT_JSON),
            'software': count_new_games(SOFTWARE_JSON),
            'aio': count_new_games(AIO_JSON),
            'vr': count_new_games(VR_JSON)
        },
        'invalidated': count_invalidated_games(),
        'newMatches': count_new_matches()
    }
    
    # Update cache
    STATS_CACHE = stats
    STATS_CACHE_TIMESTAMP = current_time
    
    return stats