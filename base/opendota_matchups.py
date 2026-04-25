"""
OpenDota API parser for hero matchups and synergies.
API: https://api.opendota.com/api/heroes/{hero_id}/matchups

Данные доступны без Cloudflare блокировки.
"""

import json
import time
import requests
from typing import Dict, List, Optional, Tuple

BASE_URL = "https://api.opendota.com/api"
import os as _os
CACHE_DIR = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "hero_opendota_data")
MIN_GAMES_THRESHOLD = 10  # Минимум игр для статистики

# Hero ID to name mapping
HERO_ID_TO_NAME = {}
HERO_NAME_TO_ID = {}

# Position weights (same as dota2protracker.py)
PRO_EARLY_POSITION_WEIGHTS = {
    'pos1': 1.4, 'pos2': 1.6, 'pos3': 1.4, 'pos4': 1.2, 'pos5': 0.8,
}
PRO_LATE_POSITION_WEIGHTS = {
    'pos1': 2.4, 'pos2': 2.2, 'pos3': 1.4, 'pos4': 1.2, 'pos5': 0.6,
}

CORE_POSITIONS = ('pos1', 'pos2', 'pos3')
PRO_CP1VS1_PAIR_WEIGHTS = {
    ('pos1', 'pos1'): 3.0, ('pos1', 'pos2'): 2.2, ('pos2', 'pos1'): 2.2,
    ('pos1', 'pos3'): 1.6, ('pos3', 'pos1'): 1.6, ('pos2', 'pos2'): 2.2,
    ('pos2', 'pos3'): 1.6, ('pos3', 'pos2'): 1.6, ('pos3', 'pos3'): 1.6,
}

TOTAL_CP_1VS1 = len(CORE_POSITIONS) * len(CORE_POSITIONS)  # 9 matchups
DUO_COMBINATIONS_PER_TEAM = 3
DUO_VALID_THRESHOLD = 0.8


def _load_hero_mapping():
    """Load hero ID to name mapping from OpenDota API."""
    global HERO_ID_TO_NAME, HERO_NAME_TO_ID
    if HERO_ID_TO_NAME:
        return

    cache_file = f"{CACHE_DIR}/heroes.json"
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                HERO_ID_TO_NAME.update(data['id_to_name'])
                HERO_NAME_TO_ID.update(data['name_to_id'])
                return
        except:
            pass

    try:
        resp = requests.get(f"{BASE_URL}/heroes", timeout=30)
        resp.raise_for_status()
        heroes = resp.json()
        for hero in heroes:
            hero_id = hero['id']
            hero_id_str = str(hero_id)
            name = hero['name'].replace('npc_dota_hero_', '').replace('_', ' ').title()
            HERO_ID_TO_NAME[hero_id_str] = name
            HERO_NAME_TO_ID[name.lower()] = hero_id_str
            HERO_NAME_TO_ID[name.lower().replace(' ', '')] = hero_id_str

        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump({'id_to_name': HERO_ID_TO_NAME, 'name_to_id': HERO_NAME_TO_ID}, f)
        print(f"   📊 Loaded {len(HERO_ID_TO_NAME)} heroes from OpenDota")
    except Exception as e:
        print(f"   ⚠️ Failed to load hero mapping: {e}")


import os

def get_hero_id(hero_name: str):
    """Convert hero name to OpenDota hero ID (as string for consistency)."""
    _load_hero_mapping()
    name_lower = hero_name.lower()
    name_nospace = name_lower.replace(' ', '')

    # Direct match
    if name_lower in HERO_NAME_TO_ID:
        return HERO_NAME_TO_ID[name_lower]
    if name_nospace in HERO_NAME_TO_ID:
        return HERO_NAME_TO_ID[name_nospace]

    # Try partial match
    for name, hero_id in HERO_NAME_TO_ID.items():
        if name_lower in name or name in name_lower:
            return hero_id
        # Handle underscore variants
        if name_lower.replace('_', '') in name or name.replace('_', '') in name_lower:
            return hero_id

    return None


def get_hero_name(hero_id) -> str:
    """Convert OpenDota hero ID to name."""
    _load_hero_mapping()
    return HERO_ID_TO_NAME.get(str(hero_id), f"Unknown_{hero_id}")


def fetch_hero_matchups(hero_id: int, use_cache: bool = True) -> Dict:
    """Fetch matchups for a hero from OpenDota API."""
    cache_file = f"{CACHE_DIR}/matchups_{hero_id}.json"

    if use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except:
            pass

    try:
        print(f"   📊 Fetching OpenDota matchups for hero {hero_id}")
        resp = requests.get(f"{BASE_URL}/heroes/{hero_id}/matchups", timeout=30)
        resp.raise_for_status()
        data = resp.json()

        result = {}
        for item in data:
            enemy_id = item['hero_id']
            games = item['games_played']
            wins = item['wins']
            wr = (wins / games * 100) if games > 0 else 50.0
            diff = wr - 50.0

            # Use string key for consistency with how data is stored
            result[str(enemy_id)] = {
                'games': games,
                'wins': wins,
                'wr': round(wr, 1),
                'diff': round(diff, 1)
            }

        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(result, f)

        print(f"   📊 Got {len(result)} matchups for hero {hero_id}")
        return result

    except Exception as e:
        print(f"   ⚠️ Error fetching matchups: {e}")
        return {}


def calculate_cp1vs1(radiant_cores: List[str], dire_cores: List[str],
                     hero_matchups: Dict, min_games: int = 10) -> Tuple[bool, Dict]:
    """Calculate cp1vs1 score for radiant cores vs dire cores."""
    weighted_scores = []
    matchup_count = 0
    games_sum = 0

    for r_idx, r_hero in enumerate(radiant_cores):
        r_pos = CORE_POSITIONS[r_idx]
        r_id = get_hero_id(r_hero)
        if not r_id:
            print(f"   ⚠️ Unknown hero: {r_hero}")
            continue

        # Get matchups for this hero (indexed by enemy hero_id)
        r_matchups = hero_matchups.get(r_id, {})

        for d_idx, d_hero in enumerate(dire_cores):
            d_pos = CORE_POSITIONS[d_idx]
            d_id = get_hero_id(d_hero)
            if not d_id:
                print(f"   ⚠️ Unknown hero: {d_hero}")
                continue

            pair_key = (r_pos, d_pos)
            pair_weight = PRO_CP1VS1_PAIR_WEIGHTS.get(pair_key, 1.0)

            # Check if this matchup exists in r_hero's matchups (keys are strings)
            d_id_str = str(d_id)
            if d_id_str in r_matchups:
                data = r_matchups[d_id_str]
                if data['games'] >= min_games:
                    diff = data['diff']
                    weighted_scores.append(diff * pair_weight)
                    matchup_count += 1
                    games_sum += data['games']
                    print(f"   📊 {r_hero}({r_pos}) vs {d_hero}({d_pos}): {data['games']} games, {data['wr']}% WR")

    is_valid = matchup_count >= TOTAL_CP_1VS1

    return is_valid, {
        'scores': weighted_scores,
        'count': matchup_count,
        'games': games_sum
    }


def enrich_with_opendota(
    radiant_heroes_and_pos: Dict,
    dire_heroes_and_pos: Dict,
    synergy_dict: Dict,
    min_games: int = 10
) -> Dict:
    """Enrich synergy dict with OpenDota matchup data."""
    result = dict(synergy_dict)
    result['pro_cp1vs1_early'] = 0
    result['pro_cp1vs1_late'] = 0
    result['pro_duo_synergy_early'] = 0
    result['pro_duo_synergy_late'] = 0
    result['pro_cp1vs1_early_games'] = 0
    result['pro_cp1vs1_late_games'] = 0
    result['pro_duo_synergy_early_games'] = 0
    result['pro_duo_synergy_late_games'] = 0
    result['pro_cp1vs1_valid'] = False
    result['pro_duo_synergy_valid'] = False

    _load_hero_mapping()

    # Extract core heroes
    radiant_cores = []
    dire_cores = []

    for pos in CORE_POSITIONS:
        r_data = radiant_heroes_and_pos.get(pos, {})
        d_data = dire_heroes_and_pos.get(pos, {})
        if isinstance(r_data, dict) and r_data.get('hero_name'):
            radiant_cores.append(r_data['hero_name'].lower())
        if isinstance(d_data, dict) and d_data.get('hero_name'):
            dire_cores.append(d_data['hero_name'].lower())

    if len(radiant_cores) < 3 or len(dire_cores) < 3:
        print("   ⚠️ OpenDota: insufficient core heroes")
        return result

    # Fetch matchups for all heroes - indexed by hero_id
    all_heroes = set(radiant_cores + dire_cores)
    hero_id_map = {}  # hero_name -> hero_id
    hero_matchups = {}  # hero_id -> matchup_data

    for hero_name in all_heroes:
        hero_id = get_hero_id(hero_name)
        if hero_id:
            hero_id_map[hero_name] = hero_id
            hero_matchups[hero_id] = fetch_hero_matchups(hero_id)
        time.sleep(0.5)  # Rate limit

    # Calculate cp1vs1
    cp_valid, cp_data = calculate_cp1vs1(radiant_cores, dire_cores, hero_matchups, min_games)

    if cp_valid:
        result['pro_cp1vs1_valid'] = True
        scores = cp_data['scores']
        result['pro_cp1vs1_early_games'] = cp_data['games']
        result['pro_cp1vs1_late_games'] = cp_data['games']

        if scores:
            cp_score = sum(scores) / len(scores)
            result['pro_cp1vs1_early'] = cp_score
            result['pro_cp1vs1_late'] = cp_score
            print(f"   📊 OpenDota cp1vs1: {cp_data['count']} matchups, score={cp_score:+.1f}%")

    return result


if __name__ == '__main__':
    # Test
    _load_hero_mapping()

    # Test matchup for Puck (ID 13)
    puck_id = get_hero_id('puck')
    print(f"Puck ID: {puck_id}")

    # Test Night Stalker
    ns_id = get_hero_id('night_stalker')
    print(f"Night Stalker ID: {ns_id}")

    # Get matchups
    if puck_id:
        matchups = fetch_hero_matchups(puck_id)
        if ns_id and ns_id in matchups:
            print(f"\nPuck vs Night Stalker: {matchups[ns_id]}")

    # Full test with draft
    radiant = {'pos1': {'hero_name': 'sniper'}, 'pos2': {'hero_name': 'puck'}, 'pos3': {'hero_name': 'mars'}}
    dire = {'pos1': {'hero_name': 'phantom_assassin'}, 'pos2': {'hero_name': 'night_stalker'}, 'pos3': {'hero_name': 'dawnbreaker'}}

    result = enrich_with_opendota(radiant, dire, {}, min_games=10)
    print(f"\nDraft result: cp1vs1_valid={result['pro_cp1vs1_valid']}, score={result['pro_cp1vs1_early']:.1f}%")