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
from dataclasses import dataclass, asdict

# Optional Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

BASE_URL = "https://dota2protracker.com"
CACHE_DIR = "hero_dota2protracker_data"
MIN_GAMES_THRESHOLD = 10  # Минимум игр для статистики

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
    # Aliases
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

POSITION_REVERSE = {v: k for k, v in POSITION_MAP.items()}


def _get_proxy_from_pool() -> Optional[str]:
    """Get a working proxy from DLTV pool."""
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

    driver = webdriver.Chrome(options=options)
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

    # Find the matchups section
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

        # Extract position data
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
        time.sleep(6)

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
            )
        except:
            pass

        html = driver.page_source
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
            driver.quit()

    return result


def enrich_with_pro_tracker(
    radiant_heroes_and_pos: Dict,
    dire_heroes_and_pos: Dict,
    synergy_dict: Dict,
    min_games: int = 10
) -> Dict:
    """
    Обогащает synergy_dict данными с dota2protracker.com.

    Вызывается после synergy_and_counterpick().

    Args:
        radiant_heroes_and_pos: {'pos1': {'hero_name': 'ursa', 'hero_id': 70}, ...}
        dire_heroes_and_pos: аналогично для dire
        synergy_dict: результат от synergy_and_counterpick()
        min_games: минимум игр для включения статистики

    Returns:
        Обогащённый synergy_dict с полями:
        - pro_cp1vs1_early, pro_cp1vs1_late
        - pro_duo_synergy_early, pro_duo_synergy_late
        - pro_matchups: детальные данные по каждой паре
    """
    result = dict(synergy_dict)
    result['pro_matchups'] = {}
    result['pro_duo_synergy_early'] = 0
    result['pro_duo_synergy_late'] = 0
    result['pro_cp1vs1_early'] = 0
    result['pro_cp1vs1_late'] = 0
    result['pro_cp1vs1_early_games'] = 0
    result['pro_cp1vs1_late_games'] = 0
    result['pro_duo_synergy_early_games'] = 0
    result['pro_duo_synergy_late_games'] = 0

    # Собираем всех героев
    all_heroes = {}
    for side_name, side in [('radiant', radiant_heroes_and_pos), ('dire', dire_heroes_and_pos)]:
        for pos, data in side.items():
            if isinstance(data, dict) and data.get('hero_name'):
                hero_name = data['hero_name'].lower()
                all_heroes[hero_name] = {
                    'name': data['hero_name'],
                    'id': data.get('hero_id'),
                    'pos': pos,
                    'side': side_name
                }

    # Парсим данные для всех героев (один раз)
    hero_data = {}
    for hero_name, hero_info in all_heroes.items():
        if hero_name not in hero_data:
            hero_data[hero_name] = parse_hero_matchups(hero_info['name'])
            time.sleep(2)  # Rate limiting

    # Вычисляем cp1vs1
    radiant_cp_scores = []
    dire_cp_scores = []
    radiant_cp_games = []
    dire_cp_games = []

    radiant_team = [(pos, data.get('hero_name', '').lower()) for pos, data in radiant_heroes_and_pos.items() if isinstance(data, dict)]
    dire_team = [(pos, data.get('hero_name', '').lower()) for pos, data in dire_heroes_and_pos.items() if isinstance(data, dict)]

    for r_pos, r_hero in radiant_team:
        for d_pos, d_hero in dire_team:
            if r_hero not in hero_data:
                continue
            matchups = hero_data[r_hero].get('matchups', {})
            d_hero_title = d_hero.title()

            if d_hero_title in matchups:
                pos_num = r_pos[-1]  # 'pos1' -> '1'
                pos_data = matchups[d_hero_title].get(pos_num, {})
                if pos_data.get('games', 0) >= min_games:
                    wr = pos_data['wr']
                    diff = wr - 50
                    radiant_cp_scores.append(diff)
                    radiant_cp_games.append(pos_data['games'])

            # Обратный matchup (dire vs radiant)
            if d_hero in hero_data:
                d_matchups = hero_data[d_hero].get('matchups', {})
                r_hero_title = r_hero.title()
                if r_hero_title in d_matchups:
                    pos_num = d_pos[-1]
                    pos_data = d_matchups[r_hero_title].get(pos_num, {})
                    if pos_data.get('games', 0) >= min_games:
                        wr = pos_data['wr']
                        diff = 50 - wr
                        dire_cp_scores.append(diff)
                        dire_cp_games.append(pos_data['games'])

    if radiant_cp_scores:
        result['pro_cp1vs1_early'] = sum(radiant_cp_scores) / len(radiant_cp_scores) if radiant_cp_scores else 0
        result['pro_cp1vs1_late'] = result['pro_cp1vs1_early']
        result['pro_cp1vs1_early_games'] = min(radiant_cp_games) if radiant_cp_games else 0
        result['pro_cp1vs1_late_games'] = result['pro_cp1vs1_early_games']

    if dire_cp_scores:
        result['pro_cp1vs1_early'] -= sum(dire_cp_scores) / len(dire_cp_scores) if dire_cp_scores else 0
        result['pro_cp1vs1_late'] = result['pro_cp1vs1_early']

    # Вычисляем duo synergy
    def calc_duo_synergy(team, hero_data):
        scores = []
        games = []
        for i, (pos1, hero1) in enumerate(team):
            for j, (pos2, hero2) in enumerate(team):
                if i >= j:
                    continue
                if hero1 not in hero_data or hero2 not in hero_data:
                    continue
                synergies = hero_data[hero1].get('synergies', {})
                hero2_title = hero2.title()
                if hero2_title in synergies:
                    pos_num = pos1[-1]
                    pos_data = synergies[hero2_title].get(pos_num, {})
                    if pos_data.get('games', 0) >= min_games:
                        wr = pos_data['wr']
                        diff = wr - 50
                        scores.append(diff)
                        games.append(pos_data['games'])
        return scores, games

    r_scores, r_games = calc_duo_synergy(radiant_team, hero_data)
    d_scores, d_games = calc_duo_synergy(dire_team, hero_data)

    if r_scores:
        result['pro_duo_synergy_early'] = sum(r_scores) / len(r_scores)
        result['pro_duo_synergy_late'] = result['pro_duo_synergy_early']
        result['pro_duo_synergy_early_games'] = min(r_games) if r_games else 0
        result['pro_duo_synergy_late_games'] = result['pro_duo_synergy_early_games']

    if d_scores:
        result['pro_duo_synergy_early'] -= sum(d_scores) / len(d_scores)
        result['pro_duo_synergy_late'] = result['pro_duo_synergy_early']

    # Сохраняем детальные данные
    result['pro_matchups'] = {
        'radiant_heroes': {h: hero_data[h] for h in [t[1] for t in radiant_team] if h in hero_data},
        'dire_heroes': {h: hero_data[h] for h in [t[1] for t in dire_team] if h in hero_data},
    }

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
