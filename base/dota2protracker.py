"""
Dota2ProTracker parser for hero matchups and synergies.
Website: https://dota2protracker.com/hero/{hero_name}

Используется в cyberscore_try.py для получения pro-level статистики:
- cp1vs1: counterpick 1v1 winrate (только от 10+ матчей)
- duo_synergy: synergy winrate для пар героев
"""

import json
import time
import re
import os
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Optional Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

import requests

BASE_URL = "https://dota2protracker.com"
CACHE_DIR = "hero_dota2protracker_data"
MIN_GAMES_THRESHOLD = 10  # Минимум игр для статистики

# Коэффициенты позиций (同步 с functions.py)
PRO_EARLY_POSITION_WEIGHTS = {
    'pos1': 1.4,
    'pos2': 1.6,
    'pos3': 1.4,
    'pos4': 1.2,
    'pos5': 0.8,
}
PRO_LATE_POSITION_WEIGHTS = {
    'pos1': 2.4,
    'pos2': 2.2,
    'pos3': 1.4,
    'pos4': 1.2,
    'pos5': 0.6,
}

CORE_POSITIONS = ('pos1', 'pos2', 'pos3')
TOTAL_CP_1VS1 = len(CORE_POSITIONS) * len(CORE_POSITIONS)  # 9 matchups для валидации
DUO_COMBINATIONS_PER_TEAM = 3  # C(3,2) = 3 пары на команду
DUO_VALID_THRESHOLD = 0.8  # 80% комбинаций должны быть

# Pair weights для cp1vs1 (sync с functions.py)
PRO_CP1VS1_PAIR_WEIGHTS = {
    ('pos1', 'pos1'): 3.0,
    ('pos1', 'pos2'): 2.2,
    ('pos2', 'pos1'): 2.2,
    ('pos1', 'pos3'): 1.6,
    ('pos3', 'pos1'): 1.6,
    ('pos2', 'pos2'): 2.2,
    ('pos2', 'pos3'): 1.6,
    ('pos3', 'pos2'): 1.6,
    ('pos3', 'pos3'): 1.6,
}

# Hero name to URL slug mapping
HERO_NAME_MAP = {
    'night stalker': 'Night_Stalker',
    'ursa': 'Ursa',
    'sand king': 'Sand_King',
    'shadow demon': 'Shadow_Demon',
    'warlock': 'Warlock',
    'lone druid': 'Lone_Druid',
    'puck': 'Puck',
    'underlord': 'Underlord',
    'abyssal underlord': 'Underlord',
    'muerta': 'Muerta',
    'treant protector': 'Treant_Protector',
    'ns': 'Night_Stalker',
    'sk': 'Sand_King',
    'sd': 'Shadow_Demon',
    'ld': 'Lone_Druid',
    'mu': 'Muerta',
}

POSITION_MAP = {
    'pos1': '1', '1': '1',
    'pos2': '2', '2': '2',
    'pos3': '3', '3': '3',
    'pos4': '4', '4': '4',
    'pos5': '5', '5': '5',
}


def _get_proxy_from_pool() -> Optional[str]:
    """Get a working proxy from DLTV pool. Returns None if no proxies available."""
    # Check for local testing - no proxy needed
    if os.getenv('DOTA2PROTRACKER_NO_PROXY'):
        return None

    try:
        import sys
        sys.path.insert(0, 'base')
        from keys import get_dltv_proxy_pool
        pool = get_dltv_proxy_pool()
        if pool:
            import random
            return random.choice(pool)
    except Exception:
        pass
    return None


def _create_driver(proxy: Optional[str] = None):
    """Create headless Chrome driver with optional proxy."""
    if not SELENIUM_AVAILABLE:
        raise RuntimeError("Selenium not available")

    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_argument('--accept-lang=en-US,en;q=0.9')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)

    if proxy:
        options.add_argument(f'--proxy-server={proxy}')

    service = Service()
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(3)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def _slug_to_hero(slug: str) -> str:
    """Convert URL slug to hero name."""
    return slug.replace('_', ' ').replace('-', ' ').title()


def _parse_matchups_from_html(html: str) -> Dict[str, Dict[str, Dict]]:
    """Parse matchups table from page HTML."""
    matchups: Dict[str, Dict[str, Dict]] = {}

    matchup_match = re.search(r'Matchups.*?<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
    if not matchup_match:
        return matchups

    table_html = matchup_match.group(1)
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)

    for row in rows:
        hero_name = None
        hero_img_match = re.search(r'<img[^>]+src="[^"]*hero/([^./"]+)', row, re.IGNORECASE)
        if hero_img_match:
            hero_name = _slug_to_hero(hero_img_match.group(1))

        if not hero_name:
            continue

        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)

        matchups[hero_name] = {}
        for cell_idx, cell_html in enumerate(cells):
            if cell_idx == 0:
                continue

            position = str(cell_idx)
            cell_text = re.sub(r'<[^>]+>', ' ', cell_html)

            wr_match = re.search(r'(\d+\.?\d*)%', cell_text)
            diff_match = re.search(r'([+-]?\d+\.?\d*)%', cell_text)
            games_match = re.search(r'(\d+)', cell_text)

            if wr_match and games_match:
                games = int(games_match.group(1))
                if games >= MIN_GAMES_THRESHOLD:
                    matchups[hero_name][position] = {
                        'wr': float(wr_match.group(1)),
                        'diff': float(diff_match.group(1)) if diff_match else 0.0,
                        'games': games
                    }

    return matchups


def _parse_synergies_from_html(html: str) -> Dict[str, Dict[str, Dict]]:
    """Parse synergies table from page HTML."""
    synergies: Dict[str, Dict[str, Dict]] = {}

    synergy_match = re.search(r'Synergies.*?<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
    if not synergy_match:
        return synergies

    table_html = synergy_match.group(1)
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)

    for row in rows:
        hero_name = None
        hero_img_match = re.search(r'<img[^>]+src="[^"]*hero/([^./"]+)', row, re.IGNORECASE)
        if hero_img_match:
            hero_name = _slug_to_hero(hero_img_match.group(1))

        if not hero_name:
            continue

        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)

        synergies[hero_name] = {}
        for cell_idx, cell_html in enumerate(cells):
            if cell_idx == 0:
                continue

            position = str(cell_idx)
            cell_text = re.sub(r'<[^>]+>', ' ', cell_html)

            wr_match = re.search(r'(\d+\.?\d*)%', cell_text)
            games_match = re.search(r'(\d+)', cell_text)

            if wr_match and games_match:
                games = int(games_match.group(1))
                if games >= MIN_GAMES_THRESHOLD:
                    synergies[hero_name][position] = {
                        'wr': float(wr_match.group(1)),
                        'games': games
                    }

    return synergies


def parse_hero_matchups(hero_name: str, use_cache: bool = True,
                        proxy: Optional[str] = None) -> Dict:
    """
    Parse matchups for a hero from dota2protracker.com.

    Returns: {'matchups': {...}, 'synergies': {...}}
    """
    cache_file = f"{CACHE_DIR}/{hero_name.replace(' ', '_').lower()}.json"

    if use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception:
            pass

    if not SELENIUM_AVAILABLE:
        return {'hero': hero_name, 'matchups': {}, 'synergies': {}, 'error': 'Selenium not available'}

    slug = HERO_NAME_MAP.get(hero_name.lower(), hero_name.replace(' ', '_'))
    url = f"{BASE_URL}/hero/{slug}"

    driver = None
    result = {
        'hero': hero_name,
        'url': url,
        'matchups': {},
        'synergies': {},
        'timestamp': time.time()
    }

    try:
        if proxy is None:
            proxy = _get_proxy_from_pool()

        print(f"   📊 Fetching pro-tracker: {hero_name} (proxy: {proxy or 'direct'})")
        driver = _create_driver(proxy)
        driver.get(url)

        # Wait for Cloudflare challenge to pass (looking for actual content)
        max_wait = 30
        start_time = time.time()
        while time.time() - start_time < max_wait:
            html = driver.page_source
            if 'Matchups' in html or 'Synergies' in html:
                break
            time.sleep(1)

        html = driver.page_source

        # Check if we hit Cloudflare
        if 'Один момент' in html or 'Just a moment' in html or 'challenge' in html.lower():
            print(f"   ⚠️ Cloudflare challenge detected, page not loaded")
            result['error'] = 'Cloudflare challenge'
            driver.quit()
            return result

        result['matchups'] = _parse_matchups_from_html(html)
        result['synergies'] = _parse_synergies_from_html(html)

        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(result, f, indent=2)

        print(f"   📊 Parsed {hero_name}: {len(result['matchups'])} matchups, {len(result['synergies'])} synergies")

    except Exception as e:
        print(f"   ⚠️ Error parsing {hero_name}: {e}")
        result['error'] = str(e)
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

    return result


def _calculate_cp1vs1(radiant_cores: List[str], dire_cores: List[str],
                       hero_data: Dict, min_games: int) -> Tuple[bool, Dict]:
    """
    Расчёт cp1vs1 с pair_weights.

    Требуем все 9 матчапов (3x3 cores).
    Для каждого матчапа: diff = wr - 50
    Умножаем на pair_weight для пары позиций.
    Суммируем и усредняем.
    """
    weighted_scores = []
    matchup_count = 0
    games_sum = 0

    for r_idx, r_hero in enumerate(radiant_cores):
        r_pos = CORE_POSITIONS[r_idx]
        r_matchups = hero_data.get(r_hero, {}).get('matchups', {})

        for d_idx, d_hero in enumerate(dire_cores):
            d_pos = CORE_POSITIONS[d_idx]
            pair_key = (r_pos, d_pos)
            pair_weight = PRO_CP1VS1_PAIR_WEIGHTS.get(pair_key, 1.0)

            # Radiant hero vs Dire hero
            d_hero_title = d_hero.title()
            if d_hero_title in r_matchups:
                pos_data = r_matchups[d_hero_title].get(str(d_idx + 1), {})
                if pos_data.get('games', 0) >= min_games:
                    diff = pos_data['wr'] - 50
                    weighted_scores.append(diff * pair_weight)
                    matchup_count += 1
                    games_sum += pos_data['games']

            # Dire hero vs Radiant hero (obverse matchup)
            d_hero_lower = d_hero.lower()
            if d_hero_lower in hero_data:
                d_matchups = hero_data[d_hero_lower].get('matchups', {})
                r_hero_title = r_hero.title()
                if r_hero_title in d_matchups:
                    pos_data = d_matchups[r_hero_title].get(str(r_idx + 1), {})
                    if pos_data.get('games', 0) >= min_games:
                        diff = 50 - pos_data['wr']  # radiant perspective
                        weighted_scores.append(diff * pair_weight)
                        matchup_count += 1
                        games_sum += pos_data['games']

    # Требуем все 9 матчапов
    is_valid = matchup_count >= TOTAL_CP_1VS1

    return is_valid, {
        'scores': weighted_scores,
        'count': matchup_count,
        'games': games_sum // 2 if matchup_count else 0  # делим на 2 т.к. bidirectional
    }


def _calculate_duo_synergy(cores: List[str], hero_data: Dict, min_games: int,
                            position_weights: Dict) -> Tuple[bool, Dict]:
    """
    Расчёт duo synergy.

    Duo валиден если хотя бы 80% комбинаций присутствуют.
    Для каждой пары: diff = wr - 50
    Умножаем на сумму position_weights для двух позиций.
    """
    weighted_scores = []
    matchup_count = 0
    games_sum = 0

    for i in range(len(cores)):
        for j in range(i + 1, len(cores)):
            hero1, hero2 = cores[i], cores[j]
            pos1, pos2 = CORE_POSITIONS[i], CORE_POSITIONS[j]
            weight = position_weights.get(pos1, 1.0) + position_weights.get(pos2, 1.0)

            # synergy от hero1 к hero2
            synergies = hero_data.get(hero1, {}).get('synergies', {})
            hero2_title = hero2.title()

            if hero2_title in synergies:
                for pos_key, pos_data in synergies[hero2_title].items():
                    if pos_data.get('games', 0) >= min_games:
                        diff = pos_data['wr'] - 50
                        weighted_scores.append(diff * weight)
                        matchup_count += 1
                        games_sum += pos_data['games']

    # Требуем 80% комбинаций (2 из 3)
    required = int(DUO_COMBINATIONS_PER_TEAM * DUO_VALID_THRESHOLD)
    is_valid = matchup_count >= required

    return is_valid, {
        'scores': weighted_scores,
        'count': matchup_count,
        'games': games_sum
    }


def enrich_with_pro_tracker(
    radiant_heroes_and_pos: Dict,
    dire_heroes_and_pos: Dict,
    synergy_dict: Dict,
    min_games: int = 10
) -> Dict:
    """
    Обогащает synergy_dict данными с dota2protracker.com.

    Правила валидации:
    - cp1vs1: все 9 матчапов (3x3 cores) должны быть
    - duo_synergy: минимум 80% комбинаций (2 из 3 пар)

    Aggregation:
    - cp1vs1: sum(scores * pair_weight) / count
    - duo_synergy: avg(r_scores) - avg(d_scores)
    """
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

    # Собираем cores героев
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
        print("   ⚠️ ProTracker: insufficient core heroes")
        return result

    # Парсим данные для всех героев
    all_heroes = set(radiant_cores + dire_cores)
    hero_data = {}

    for hero_name in all_heroes:
        hero_data[hero_name] = parse_hero_matchups(hero_name)
        time.sleep(2)

    # ===== CP1VS1 =====
    r_cp_valid, r_cp_data = _calculate_cp1vs1(radiant_cores, dire_cores, hero_data, min_games)

    if r_cp_valid:
        result['pro_cp1vs1_valid'] = True
        scores = r_cp_data['scores']
        result['pro_cp1vs1_early_games'] = r_cp_data['games']
        result['pro_cp1vs1_late_games'] = r_cp_data['games']

        if scores:
            # Сумма weighted scores / count
            cp_score = sum(scores) / len(scores)
            result['pro_cp1vs1_early'] = cp_score
            result['pro_cp1vs1_late'] = cp_score

            print(f"   📊 ProTracker cp1vs1: {r_cp_data['count']} matchups, score={cp_score:+.1f}%, games={r_cp_data['games']}")

    # ===== DUO SYNERGY =====
    r_duo_valid, r_duo_data = _calculate_duo_synergy(
        radiant_cores, hero_data, min_games, PRO_EARLY_POSITION_WEIGHTS
    )
    d_duo_valid, d_duo_data = _calculate_duo_synergy(
        dire_cores, hero_data, min_games, PRO_EARLY_POSITION_WEIGHTS
    )

    if r_duo_valid and d_duo_valid:
        result['pro_duo_synergy_valid'] = True
        r_scores = r_duo_data['scores']
        d_scores = d_duo_data['scores']
        result['pro_duo_synergy_early_games'] = r_duo_data['games'] + d_duo_data['games']
        result['pro_duo_synergy_late_games'] = result['pro_duo_synergy_early_games']

        if r_scores and d_scores:
            r_avg = sum(r_scores) / len(r_scores)
            d_avg = sum(d_scores) / len(d_scores)
            duo_score = r_avg - d_avg

            result['pro_duo_synergy_early'] = duo_score
            result['pro_duo_synergy_late'] = duo_score

            print(f"   📊 ProTracker duo: R={r_duo_data['count']} pairs ({r_avg:+.1f}%), D={d_duo_data['count']} pairs ({d_avg:+.1f}%)")

    return result


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        hero = sys.argv[1]
        data = parse_hero_matchups(hero, use_cache=False)
        print(json.dumps(data, indent=2))
    else:
        data = parse_hero_matchups('Puck', use_cache=False)
        print(json.dumps(data, indent=2))
