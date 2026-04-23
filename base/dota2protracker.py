"""
Dota2ProTracker parser for hero matchups and synergies.
Website: https://dota2protracker.com/hero/{hero_name}

Используется в cyberscore_try.py для получения pro-level статистики:
- cp1vs1: counterpick 1v1 winrate (только от 10+ матчей)
- duo_synergy: synergy winrate для пар героев

Поддерживает два браузера:
- Camoufox (рекомендуется, Playwright-based)
- Selenium Chrome (fallback)
"""

import json
import time
import re
import os
import math
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse

# Optional Camoufox imports (preferred)
try:
    import camoufox
    CAMOUFOX_AVAILABLE = True
except ImportError:
    CAMOUFOX_AVAILABLE = False

# Optional Selenium imports (fallback)
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
CACHE_SCHEMA_VERSION = 2

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
ALL_POSITIONS = ('pos1', 'pos2', 'pos3', 'pos4', 'pos5')
TOTAL_CP_1VS1 = len(CORE_POSITIONS) * len(CORE_POSITIONS)  # 9 matchups для валидации
DUO_COMBINATIONS_PER_TEAM = 3  # C(3,2) = 3 пары на команду
DUO_VALID_THRESHOLD = 0.8  # 80% комбинаций должны быть
PRO_POSITION_COVERAGE_THRESHOLD = 2 / 3

# Lane definitions for lane-specific cp1vs1
LANE_CP1VS1_PAIRS = {
    'mid': [(('pos2', 'pos2'),)],  # pos2 vs pos2
    'top': [
        (('pos3', 'pos1'),),
        (('pos3', 'pos5'),),
        (('pos4', 'pos1'),),
        (('pos4', 'pos5'),),
    ],
    'bot': [
        (('pos1', 'pos3'),),
        (('pos1', 'pos4'),),
        (('pos5', 'pos3'),),
        (('pos5', 'pos4'),),
    ],
}
LANE_CP1VS1_MIN_MATCHUPS = {
    'mid': 1,
    'top': 2,
    'bot': 2,
}

# Lane definitions for duo synergy (2v2 pairs)
LANE_DUO_PAIRS = {
    'mid': None,  # no duo synergy for mid
    'top': {
        'radiant': ('pos3', 'pos4'),
        'dire': ('pos1', 'pos5'),
    },
    'bot': {
        'radiant': ('pos1', 'pos5'),
        'dire': ('pos3', 'pos4'),
    },
}

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

HERO_REGISTRY_PATH = os.path.join(os.path.dirname(__file__), 'hero_features_processed.json')
_HERO_REGISTRY = None

def get_hero_registry() -> dict:
    """Load hero registry from base/hero_features_processed.json. Returns {id: {id, name, slug}}"""
    global _HERO_REGISTRY
    if _HERO_REGISTRY is None:
        try:
            with open(HERO_REGISTRY_PATH, 'r') as f:
                data = json.load(f)
            _HERO_REGISTRY = {int(k): {'id': int(k), 'name': v['hero_name'], 'slug': v['hero_slug']}
                             for k, v in data.items()}
            print(f"   Loaded {len(_HERO_REGISTRY)} heroes from registry")
        except Exception as e:
            print(f"   ⚠️ Failed to load hero registry: {e}")
            _HERO_REGISTRY = {}
    return _HERO_REGISTRY

def get_hero_id(hero_name: str) -> int:
    """Get OpenDota ID for a hero name."""
    registry = get_hero_registry()
    # Try exact match (case insensitive)
    for hid, hero in registry.items():
        if hero['name'].lower() == hero_name.lower():
            return hid
    # Try underscore variant
    variant = hero_name.lower().replace(' ', '_')
    for hid, hero in registry.items():
        slug = hero['slug'].lower()
        if slug == variant or hero['name'].lower().replace(' ', '_') == variant:
            return hid
    return 0

def get_hero_name(hero_id: int) -> str:
    """Get hero name from ID."""
    registry = get_hero_registry()
    return registry.get(hero_id, {}).get('name', '')


def _extract_team_positions_and_cores(team_heroes_and_pos: Dict) -> Tuple[List[Tuple[str, str]], List[str], Dict[str, Any]]:
    """Build [(pos, hero_name)] and core hero list from parsed draft payload."""
    positions: List[Tuple[str, str]] = []
    cores: List[str] = []
    debug_payload: Dict[str, Any] = {}

    for pos in ALL_POSITIONS:
        raw_data = team_heroes_and_pos.get(pos, {})
        debug_payload[pos] = raw_data
        if not isinstance(raw_data, dict):
            continue

        hero_name = str(raw_data.get('hero_name') or '').strip()
        if not hero_name:
            hero_id = int(raw_data.get('hero_id', 0) or 0)
            if hero_id > 0:
                hero_name = str(get_hero_name(hero_id) or '').strip()

        if not hero_name:
            continue

        normalized = hero_name.lower()
        positions.append((pos, normalized))
        if pos in CORE_POSITIONS:
            cores.append(normalized)

    return positions, cores, debug_payload

def get_hero_slug(hero_name: str) -> str:
    """Get URL slug for dota2protracker.com (e.g., 'lonedruid' -> 'Lone_Druid')."""
    registry = get_hero_registry()
    # Try exact match on name
    for hid, hero in registry.items():
        if hero['name'].lower() == hero_name.lower():
            slug = hero['slug']
            # Convert 'lonedruid' -> 'Lone_Druid' (title case with underscores)
            return '_'.join(word.capitalize() for word in slug.replace('-', ' ').split())
    # Try match on slug
    variant = hero_name.lower().replace(' ', '_')
    for hid, hero in registry.items():
        if hero['slug'].lower() == variant:
            slug = hero['slug']
            return '_'.join(word.capitalize() for word in slug.replace('-', ' ').split())
    # Fallback: title case with underscores
    return '_'.join(word.capitalize() for word in hero_name.split())


def _hero_norm_key(hero_name: str) -> str:
    return hero_name.strip().lower().replace('-', ' ').replace(' ', '_')


def _hero_data_entry(hero_data: Dict, hero_name: str) -> Dict:
    variants = []
    raw = str(hero_name or "").strip()
    if raw:
        variants.extend([
            raw,
            raw.lower(),
            raw.lower().replace('_', ' '),
            raw.lower().replace(' ', '_'),
            _hero_norm_key(raw),
        ])
    seen = set()
    for key in variants:
        if not key or key in seen:
            continue
        seen.add(key)
        if key in hero_data and isinstance(hero_data[key], dict):
            return hero_data[key]
    return {}

POSITION_MAP = {
    'pos1': '1', '1': '1',
    'pos2': '2', '2': '2',
    'pos3': '3', '3': '3',
    'pos4': '4', '4': '4',
    'pos5': '5', '5': '5',
}


def _get_proxy_from_pool() -> Optional[str]:
    """Get a proxy for dota2protracker Camoufox sessions."""
    # Check for local testing - no proxy needed
    if os.getenv('DOTA2PROTRACKER_NO_PROXY'):
        return None

    try:
        import sys
        base_dir = os.path.dirname(os.path.abspath(__file__))
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        try:
            from base.keys import get_dota2protracker_proxy_pool
        except Exception:
            from keys import get_dota2protracker_proxy_pool
        pool = get_dota2protracker_proxy_pool()
        if pool:
            import random
            return random.choice(pool)
    except Exception:
        pass
    return None


def _camoufox_proxy_kwargs(proxy_url: Optional[str]) -> Dict[str, Any]:
    if not proxy_url:
        return {}
    parsed = urlparse(str(proxy_url or ""))
    host = (parsed.hostname or "").strip()
    port = parsed.port
    username = parsed.username
    password = parsed.password
    if not host or not port:
        return {}
    proxy_kwargs: Dict[str, Any] = {
        "proxy": {
            "server": f"http://{host}:{port}",
        }
    }
    if username:
        proxy_kwargs["proxy"]["username"] = username
    if password:
        proxy_kwargs["proxy"]["password"] = password
    return proxy_kwargs


def _dota2protracker_candidate_proxies(preferred_proxy: Optional[str] = None) -> List[Optional[str]]:
    candidates: List[Optional[str]] = []
    seen: set[str] = set()

    def _push(value: Optional[str]) -> None:
        key = str(value or "__direct__")
        if key in seen:
            return
        seen.add(key)
        candidates.append(value)

    if preferred_proxy:
        _push(preferred_proxy)

    try:
        import sys
        base_dir = os.path.dirname(os.path.abspath(__file__))
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        try:
            from base.keys import get_dota2protracker_proxy_pool
        except Exception:
            from keys import get_dota2protracker_proxy_pool
        pool = list(get_dota2protracker_proxy_pool() or [])
    except Exception:
        pool = []

    for item in pool:
        _push(item)

    _push(None)
    return candidates


def _fetch_protracker_payload_via_subprocess(
    slug: str,
    hero_id: int,
    proxy_candidate: Optional[str],
) -> Dict[str, Any]:
    """
    Fetch all 5 positions for one hero in a clean child process.

    This avoids Playwright/Camoufox sync API conflicts with a parent interpreter
    that may already be running inside an asyncio loop.
    """
    helper_code = r"""
import json
import sys
from urllib.parse import urlparse
import camoufox

BASE_URL = "https://dota2protracker.com"
slug = sys.argv[1]
hero_id = int(sys.argv[2])
proxy_url = sys.argv[3] or None

def proxy_kwargs(proxy_url):
    if not proxy_url:
        return {}
    parsed = urlparse(str(proxy_url or ""))
    host = (parsed.hostname or "").strip()
    port = parsed.port
    username = parsed.username
    password = parsed.password
    if not host or not port:
        return {}
    proxy_data = {"server": f"http://{host}:{port}"}
    if username:
        proxy_data["username"] = username
    if password:
        proxy_data["password"] = password
    return {"proxy": proxy_data}

payload = {"matchups": {}, "synergies": {}}

with camoufox.Camoufox(
    headless=True,
    args=["--disable-blink-features=AutomationControlled"],
    **proxy_kwargs(proxy_url),
) as browser:
    page = browser.new_page()
    page.goto(f"{BASE_URL}/hero/{slug}", wait_until="networkidle", timeout=30000)
    for pos in ["1", "2", "3", "4", "5"]:
        api_url = f"{BASE_URL}/hero/{slug}/api/matchup-payload?heroId={hero_id}&position=pos+{pos}"
        response = page.evaluate(
            '''async (apiUrl) => {
                const r = await fetch(apiUrl);
                return await r.json();
            }''',
            api_url,
        )
        payload["matchups"][pos] = response.get("matchups", []) if isinstance(response, dict) else []
        payload["synergies"][pos] = response.get("synergies", []) if isinstance(response, dict) else []

print(json.dumps(payload))
"""

    completed = subprocess.run(
        [sys.executable, "-c", helper_code, slug, str(hero_id), str(proxy_candidate or "")],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        detail = stderr or stdout or f"subprocess exited with code {completed.returncode}"
        raise RuntimeError(detail)
    stdout = (completed.stdout or "").strip()
    if not stdout:
        raise RuntimeError("empty subprocess stdout")
    return json.loads(stdout)


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


def _extract_matchups_from_js(driver) -> Dict:
    """Extract matchup data using JavaScript execution."""
    matchups = {}
    synergies = {}

    script = """
    var results = {matchups: {}, synergies: {}};

    // Get all text content
    var text = document.body.innerText;
    var lines = text.split('\\n');

    // Look for hero links
    var heroLinks = document.querySelectorAll('a[href*="/hero/"]');
    var heroes = {};
    heroLinks.forEach(link => {
        var href = link.getAttribute('href');
        var name = href.split('/hero/')[1].replace(/_/g, ' ').replace(/-/g, ' ');
        // Clean name - take only valid hero names
        name = name.charAt(0).toUpperCase() + name.slice(1);
        if (name.length > 2 && name.length < 30) {
            heroes[name] = name;
        }
    });

    // Look for percentage patterns that indicate matchup data
    // Pattern: hero name followed by percentage and games count
    for (var i = 0; i < lines.length; i++) {
        var line = lines[i].trim();
        var nextLine = i + 1 < lines.length ? lines[i + 1].trim() : '';
        var prevLine = i > 0 ? lines[i - 1].trim() : '';

        // Matchup pattern: percentage with vs or with keyword nearby
        if (line.includes('%')) {
            // Look for patterns like "HeroName 55.5% 123 games vs"
            var wrMatch = line.match(/(\\d+\\.?\\d*)%/);
            var gamesMatch = line.match(/(\\d+)\\s*(?:games|matches)/i);
            var diffMatch = line.match(/([+-]?\\d+\\.?\\d*)%/);

            if (wrMatch && gamesMatch) {
                var wr = parseFloat(wrMatch[1]);
                var games = parseInt(gamesMatch[1]);

                // Check context for matchup vs synergy
                var prev200 = lines.slice(Math.max(0, i-20), i).join(' ').toLowerCase();
                var next200 = lines.slice(i+1, Math.min(lines.length, i+20)).join(' ').toLowerCase();
                var context = prev200 + ' ' + next200;

                if (context.includes('versus') || context.includes(' vs ') || context.includes('counter')) {
                    // Check which hero this belongs to
                    for (var name in heroes) {
                        if (prev200.includes(name.toLowerCase()) || next200.includes(name.toLowerCase())) {
                            var position = '1';  // Default to position 1
                            if (context.includes('pos2') || context.includes('mid')) position = '2';
                            else if (context.includes('pos3') || context.includes('offlane')) position = '3';
                            else if (context.includes('pos4')) position = '4';
                            else if (context.includes('pos5')) position = '5';

                            if (!results.matchups[name]) results.matchups[name] = {};
                            results.matchups[name][position] = {
                                wr: wr,
                                diff: diffMatch ? parseFloat(diffMatch[1]) : 0,
                                games: games
                            };
                            break;
                        }
                    }
                } else if (context.includes('synerg') || context.includes(' with ')) {
                    for (var name in heroes) {
                        if (prev200.includes(name.toLowerCase()) || next200.includes(name.toLowerCase())) {
                            var position = '1';
                            if (context.includes('pos2')) position = '2';
                            else if (context.includes('pos3')) position = '3';
                            else if (context.includes('pos4')) position = '4';
                            else if (context.includes('pos5')) position = '5';

                            if (!results.synergies[name]) results.synergies[name] = {};
                            results.synergies[name][position] = {
                                wr: wr,
                                games: games
                            };
                            break;
                        }
                    }
                }
            }
        }
    }

    return results;
    """

    try:
        data = driver.execute_script(script)
        matchups = data.get('matchups', {})
        synergies = data.get('synergies', {})
    except Exception as e:
        print(f"   ⚠️ JS extraction error: {e}")

    return {'matchups': matchups, 'synergies': synergies}


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
    Parse matchups for a hero from dota2protracker.com using Camoufox.

    Uses direct API calls via page.evaluate() fetch:
    /hero/{slug}/api/matchup-payload?heroId={id}&position=pos+{pos}

    Returns: {'matchups': {...}, 'synergies': {...}}
    """
    cache_file = f"{CACHE_DIR}/{hero_name.replace(' ', '_').lower()}.json"

    # Cache TTL: expire at midnight (next day)
    def _cache_expired(cache_file):
        """Check if cache is expired (older than today)."""
        if not os.path.exists(cache_file):
            return True
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            if data.get('cache_schema_version') != CACHE_SCHEMA_VERSION:
                return True
            cached_ts = data.get('timestamp', 0)
            cached_date = time.strftime('%Y-%m-%d', time.localtime(cached_ts))
            today = time.strftime('%Y-%m-%d')
            return cached_date < today  # Expired if cached before today
        except Exception:
            return True

    if use_cache and os.path.exists(cache_file) and not _cache_expired(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception:
            pass

    if not CAMOUFOX_AVAILABLE:
        print("   ⚠️ Camoufox not available. Run: pip install camoufox")
        return {'hero': hero_name, 'matchups': {}, 'synergies': {}, 'error': 'Camoufox not available'}

    slug = get_hero_slug(hero_name)
    hero_id = get_hero_id(hero_name)
    url = f"{BASE_URL}/hero/{slug}"

    result = {
        'hero': hero_name,
        'url': url,
        'matchups': {},
        'synergies': {},
        'timestamp': time.time(),
        'cache_schema_version': CACHE_SCHEMA_VERSION,
    }

    if not hero_id:
        print(f"   ⚠️ Unknown hero: {hero_name}")
        return result

    last_error: Optional[Exception] = None
    proxy_candidates = _dota2protracker_candidate_proxies(proxy or _get_proxy_from_pool())
    matchup_by_hero_pos = {}
    synergy_by_hero_pos = {}

    try:
        for proxy_candidate in proxy_candidates:
            try:
                if proxy_candidate:
                    print(f"   📊 Fetching pro-tracker: {hero_name} (Camoufox subprocess via proxy {proxy_candidate})")
                else:
                    print(f"   📊 Fetching pro-tracker: {hero_name} (Camoufox subprocess direct)")

                raw_payload = _fetch_protracker_payload_via_subprocess(slug, hero_id, proxy_candidate)

                matchup_by_hero_pos = {}
                synergy_by_hero_pos = {}

                for pos in ['1', '2', '3', '4', '5']:
                    resp_matchups = ((raw_payload.get('matchups') or {}).get(pos) or [])
                    resp_synergies = ((raw_payload.get('synergies') or {}).get(pos) or [])

                    for m in resp_matchups:
                        other_name = m.get('other_hero_name', '')
                        if not other_name:
                            continue

                        other_name_norm = other_name.lower().replace(' ', '_')
                        opp_position = m.get('other_position', 'pos 1')
                        opp_pos_num = opp_position.replace('pos ', '')

                        wr = m.get('win_rate', 50)
                        games = m.get('matches', 0)
                        wins = m.get('wins', 0)

                        if games >= MIN_GAMES_THRESHOLD:
                            if other_name_norm not in matchup_by_hero_pos:
                                matchup_by_hero_pos[other_name_norm] = {}
                            if opp_pos_num not in matchup_by_hero_pos[other_name_norm]:
                                matchup_by_hero_pos[other_name_norm][opp_pos_num] = {}

                            matchup_by_hero_pos[other_name_norm][opp_pos_num][pos] = {
                                'wr': wr,
                                'games': games,
                                'wins': wins,
                                'diff': wr - 50
                            }

                    for s in resp_synergies:
                        other_name = s.get('other_hero_name', '')
                        if not other_name:
                            continue

                        other_name_norm = _hero_norm_key(other_name)
                        ally_position = s.get('other_position', 'pos 1')
                        ally_pos_num = ally_position.replace('pos ', '')

                        wr = s.get('win_rate', 50)
                        games = s.get('matches', 0)
                        wins = s.get('wins', 0)

                        if games >= MIN_GAMES_THRESHOLD:
                            if other_name_norm not in synergy_by_hero_pos:
                                synergy_by_hero_pos[other_name_norm] = {}
                            if ally_pos_num not in synergy_by_hero_pos[other_name_norm]:
                                synergy_by_hero_pos[other_name_norm][ally_pos_num] = {}

                            synergy_by_hero_pos[other_name_norm][ally_pos_num][pos] = {
                                'wr': wr,
                                'games': games,
                                'wins': wins
                            }

                if matchup_by_hero_pos or synergy_by_hero_pos:
                    break
            except Exception as e:
                last_error = e
                print(f"   ⚠️ Pro-tracker fetch attempt failed for {hero_name} via {proxy_candidate or 'direct'}: {e}")
                continue

        if not matchup_by_hero_pos and not synergy_by_hero_pos and last_error is not None:
            raise last_error

        # Convert to legacy format for backward compatibility
        # Legacy: {opponent: {opp_pos: {wr, games, wins}}} (aggregate across hero positions)
        # This is what get_matchup_data expects
        for opponent, opp_data in matchup_by_hero_pos.items():
            if opponent not in result['matchups']:
                result['matchups'][opponent] = {}

            for opp_pos, hero_pos_data in opp_data.items():
                # Aggregate across hero positions for this opponent position
                # If multiple hero positions have data, aggregate them
                total_wins = sum(h.get('wins', 0) for h in hero_pos_data.values())
                total_games = sum(h.get('games', 0) for h in hero_pos_data.values())

                if total_games >= MIN_GAMES_THRESHOLD:
                    wr = 100 * total_wins / total_games if total_games > 0 else 50
                    result['matchups'][opponent][opp_pos] = {
                        'wr': round(wr, 2),
                        'games': total_games,
                        'wins': total_wins,
                        'diff': round(wr - 50, 2)
                    }

        # Also store position-specific data for accurate lookup
        # This allows getting exact "hero pos X vs opponent pos Y" data
        result['_matchups_by_hero_pos'] = matchup_by_hero_pos

        for ally, ally_data in synergy_by_hero_pos.items():
            if ally not in result['synergies']:
                result['synergies'][ally] = {}

            for ally_pos, hero_pos_data in ally_data.items():
                total_wins = sum(h.get('wins', 0) for h in hero_pos_data.values())
                total_games = sum(h.get('games', 0) for h in hero_pos_data.values())

                if total_games >= MIN_GAMES_THRESHOLD:
                    wr = 100 * total_wins / total_games if total_games > 0 else 50
                    result['synergies'][ally][ally_pos] = {
                        'wr': round(wr, 2),
                        'games': total_games,
                        'wins': total_wins
                    }

        result['_synergies_by_hero_pos'] = synergy_by_hero_pos

        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(result, f, indent=2)

        print(f"   📊 Parsed {hero_name}: {len(result['matchups'])} matchups, {len(result['synergies'])} synergies")

    except Exception as e:
        print(f"   ⚠️ Error parsing {hero_name}: {e}")
        import traceback
        traceback.print_exc()
        result['error'] = str(e)

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
        r_entry = _hero_data_entry(hero_data, r_hero)
        r_precise = r_entry.get('_matchups_by_hero_pos', {})

        for d_idx, d_hero in enumerate(dire_cores):
            d_pos = CORE_POSITIONS[d_idx]
            pair_key = (r_pos, d_pos)
            pair_weight = PRO_CP1VS1_PAIR_WEIGHTS.get(pair_key, 1.0)
            d_key = _hero_norm_key(d_hero)
            r_key = _hero_norm_key(r_hero)
            hero_pos_num = POSITION_MAP.get(r_pos, r_pos[-1])
            opp_pos_num = POSITION_MAP.get(d_pos, d_pos[-1])
            pair_diffs = []
            pair_games = []

            # Radiant hero vs Dire hero
            pos_data = r_precise.get(d_key, {}).get(opp_pos_num, {}).get(hero_pos_num, {})
            if pos_data.get('games', 0) >= min_games:
                pair_diffs.append(pos_data['wr'] - 50)
                pair_games.append(pos_data['games'])

            # Dire hero vs Radiant hero (obverse matchup)
            d_entry = _hero_data_entry(hero_data, d_hero)
            d_precise = d_entry.get('_matchups_by_hero_pos', {})
            pos_data = d_precise.get(r_key, {}).get(hero_pos_num, {}).get(opp_pos_num, {})
            if pos_data.get('games', 0) >= min_games:
                pair_diffs.append(50 - pos_data['wr'])  # radiant perspective
                pair_games.append(pos_data['games'])

            if pair_diffs:
                weighted_scores.append((sum(pair_diffs) / len(pair_diffs)) * pair_weight)
                matchup_count += 1
                games_sum += int(sum(pair_games) / len(pair_games))

    # Требуем все 9 матчапов
    is_valid = matchup_count >= TOTAL_CP_1VS1

    return is_valid, {
        'scores': weighted_scores,
        'count': matchup_count,
        'games': games_sum
    }


def _required_coverage(possible: int) -> int:
    if possible <= 0:
        return 0
    return max(1, math.ceil(possible * PRO_POSITION_COVERAGE_THRESHOLD))


def _calculate_cp1vs1_all_positions(
    radiant_positions: List[Tuple[str, str]],
    dire_positions: List[Tuple[str, str]],
    hero_data: Dict,
    min_games: int
) -> Tuple[bool, Dict]:
    """
    Считаем все позиции 1..5 против 1..5.

    Валидность:
    - для каждой core-позиции Radiant должно быть покрыто >= 2/3 opponent positions
    - для каждой core-позиции Dire должно быть покрыто >= 2/3 opponent positions

    Pair score берётся из среднего forward/reverse diff, если доступны оба.
    """
    weighted_scores = []
    games_sum = 0
    matchup_count = 0

    rad_core_coverage = {pos: 0 for pos in CORE_POSITIONS if any(p == pos for p, _ in radiant_positions)}
    dire_core_coverage = {pos: 0 for pos in CORE_POSITIONS if any(p == pos for p, _ in dire_positions)}
    rad_core_vs_core_coverage = {pos: 0 for pos in CORE_POSITIONS if any(p == pos for p, _ in radiant_positions)}
    dire_core_vs_core_coverage = {pos: 0 for pos in CORE_POSITIONS if any(p == pos for p, _ in dire_positions)}

    for r_pos, r_hero in radiant_positions:
        r_entry = _hero_data_entry(hero_data, r_hero)
        r_precise = r_entry.get('_matchups_by_hero_pos', {})
        r_pos_num = POSITION_MAP.get(r_pos, r_pos[-1])

        for d_pos, d_hero in dire_positions:
            pair_weight = PRO_CP1VS1_PAIR_WEIGHTS.get((r_pos, d_pos), 1.0)
            d_pos_num = POSITION_MAP.get(d_pos, d_pos[-1])
            d_key = _hero_norm_key(d_hero)
            r_key = _hero_norm_key(r_hero)
            pair_diffs = []
            pair_games = []

            pos_data = r_precise.get(d_key, {}).get(d_pos_num, {}).get(r_pos_num, {})
            if pos_data.get('games', 0) >= min_games:
                pair_diffs.append(pos_data['wr'] - 50)
                pair_games.append(pos_data['games'])

            d_entry = _hero_data_entry(hero_data, d_hero)
            d_precise = d_entry.get('_matchups_by_hero_pos', {})
            pos_data = d_precise.get(r_key, {}).get(r_pos_num, {}).get(d_pos_num, {})
            if pos_data.get('games', 0) >= min_games:
                pair_diffs.append(50 - pos_data['wr'])
                pair_games.append(pos_data['games'])

            if pair_diffs:
                weighted_scores.append((sum(pair_diffs) / len(pair_diffs)) * pair_weight)
                matchup_count += 1
                games_sum += int(sum(pair_games) / len(pair_games))
                if r_pos in rad_core_coverage:
                    rad_core_coverage[r_pos] += 1
                if d_pos in dire_core_coverage:
                    dire_core_coverage[d_pos] += 1
                if r_pos in rad_core_vs_core_coverage and d_pos in CORE_POSITIONS:
                    rad_core_vs_core_coverage[r_pos] += 1
                if d_pos in dire_core_vs_core_coverage and r_pos in CORE_POSITIONS:
                    dire_core_vs_core_coverage[d_pos] += 1

    required_core_vs_core = _required_coverage(len(CORE_POSITIONS))

    radiant_valid = all(count >= required_core_vs_core for count in rad_core_vs_core_coverage.values()) if rad_core_vs_core_coverage else False
    dire_valid = all(count >= required_core_vs_core for count in dire_core_vs_core_coverage.values()) if dire_core_vs_core_coverage else False
    is_valid = radiant_valid and dire_valid and bool(weighted_scores)

    return is_valid, {
        'scores': weighted_scores,
        'count': matchup_count,
        'games': games_sum,
        'radiant_core_coverage': rad_core_coverage,
        'dire_core_coverage': dire_core_coverage,
        'radiant_core_vs_core_coverage': rad_core_vs_core_coverage,
        'dire_core_vs_core_coverage': dire_core_vs_core_coverage,
        'required_core_vs_core': required_core_vs_core,
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
            pos1_num = POSITION_MAP.get(pos1, pos1[-1])
            pos2_num = POSITION_MAP.get(pos2, pos2[-1])
            hero2_key = _hero_norm_key(hero2)

            # synergy от hero1 к hero2
            hero1_entry = _hero_data_entry(hero_data, hero1)
            precise_synergies = hero1_entry.get('_synergies_by_hero_pos', {})

            pos_data = precise_synergies.get(hero2_key, {}).get(pos2_num, {}).get(pos1_num, {})
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


def _calculate_duo_synergy_all_positions(
    team_positions: List[Tuple[str, str]],
    hero_data: Dict,
    min_games: int,
    position_weights: Dict
) -> Tuple[bool, Dict]:
    """
    Считаем synergy для всех 5 позиций внутри команды.

    Валидность:
    - на каждую core-позицию должно приходиться >= 2/3 доступных союзных пар.
    """
    weighted_scores = []
    matchup_count = 0
    games_sum = 0
    core_coverage = {pos: 0 for pos in CORE_POSITIONS if any(p == pos for p, _ in team_positions)}

    for i in range(len(team_positions)):
        for j in range(i + 1, len(team_positions)):
            pos1, hero1 = team_positions[i]
            pos2, hero2 = team_positions[j]
            weight = position_weights.get(pos1, 1.0) + position_weights.get(pos2, 1.0)
            pos1_num = POSITION_MAP.get(pos1, pos1[-1])
            pos2_num = POSITION_MAP.get(pos2, pos2[-1])
            hero2_key = _hero_norm_key(hero2)

            hero1_entry = _hero_data_entry(hero_data, hero1)
            precise_synergies = hero1_entry.get('_synergies_by_hero_pos', {})

            pos_data = precise_synergies.get(hero2_key, {}).get(pos2_num, {}).get(pos1_num, {})
            if pos_data.get('games', 0) >= min_games:
                diff = pos_data['wr'] - 50
                weighted_scores.append(diff * weight)
                matchup_count += 1
                games_sum += pos_data['games']
                if pos1 in core_coverage:
                    core_coverage[pos1] += 1
                if pos2 in core_coverage:
                    core_coverage[pos2] += 1

    required_pairs = _required_coverage(max(0, len(team_positions) - 1))
    is_valid = all(count >= required_pairs for count in core_coverage.values()) if core_coverage else False
    is_valid = is_valid and bool(weighted_scores)

    return is_valid, {
        'scores': weighted_scores,
        'count': matchup_count,
        'games': games_sum,
        'core_coverage': core_coverage,
        'required_per_core': required_pairs,
    }


def _get_matchup_1v1(hero_data: Dict, r_hero: str, d_hero: str, r_pos: str, d_pos: str, min_games: int) -> Tuple[float, int]:
    """Get 1v1 matchup diff from Radiant perspective.

    Takes the best available direction (more games wins), doesn't average symmetrically.
    """
    r_key = _hero_norm_key(r_hero)
    d_key = _hero_norm_key(d_hero)
    r_pos_num = POSITION_MAP.get(r_pos, r_pos[-1])
    d_pos_num = POSITION_MAP.get(d_pos, d_pos[-1])

    # Radiant hero vs Dire hero (Radiant perspective = win% - 50)
    r_entry = _hero_data_entry(hero_data, r_hero)
    r_precise = r_entry.get('_matchups_by_hero_pos', {})
    forward_data = r_precise.get(d_key, {}).get(d_pos_num, {}).get(r_pos_num, {})

    # Dire hero vs Radiant hero (invert for Radiant perspective = 50 - win%)
    d_entry = _hero_data_entry(hero_data, d_hero)
    d_precise = d_entry.get('_matchups_by_hero_pos', {})
    reverse_data = d_precise.get(r_key, {}).get(r_pos_num, {}).get(d_pos_num, {})

    forward_games = forward_data.get('games', 0)
    reverse_games = reverse_data.get('games', 0)

    # Take the direction with more games (or forward if equal)
    if forward_games >= min_games and forward_games >= reverse_games:
        diff = forward_data['wr'] - 50
        return diff, forward_games
    elif reverse_games >= min_games:
        diff = 50 - reverse_data['wr']
        return diff, reverse_games

    return None, 0


def _get_duo_synergy(hero_data: Dict, hero1: str, hero2: str, pos1: str, pos2: str, min_games: int) -> Tuple[float, int]:
    """Get duo synergy score for hero1 with hero2 at positions pos1/pos2.

    Returns diff from 50% and games. Takes best available direction.
    """
    pos1_num = POSITION_MAP.get(pos1, pos1[-1])
    pos2_num = POSITION_MAP.get(pos2, pos2[-1])
    hero2_key = _hero_norm_key(hero2)

    hero1_entry = _hero_data_entry(hero_data, hero1)
    precise_synergies = hero1_entry.get('_synergies_by_hero_pos', {})

    pos_data = precise_synergies.get(hero2_key, {}).get(pos2_num, {}).get(pos1_num, {})
    if pos_data.get('games', 0) >= min_games:
        diff = pos_data['wr'] - 50
        return diff, pos_data['games']
    return None, 0


def _get_duo_synergy_pair(
    hero_data: Dict,
    r_hero1: str, r_hero2: str, r_pos1: str, r_pos2: str,
    d_hero1: str, d_hero2: str, d_pos1: str, d_pos2: str,
    min_games: int
) -> Tuple[Optional[float], int]:
    """Get duo synergy advantage for Radiant pair over Dire pair.

    Takes best available direction per pair (more games wins).
    """
    # Get both directions for radiant pair
    r_fwd1, r_games1 = _get_duo_synergy(hero_data, r_hero1, r_hero2, r_pos1, r_pos2, min_games)
    r_rev1, r_games2 = _get_duo_synergy(hero_data, r_hero2, r_hero1, r_pos2, r_pos1, min_games)

    # Pick the best direction for Radiant
    r_diff = r_fwd1 if (r_fwd1 is not None and r_games1 >= r_games2) else r_rev1
    r_games = max(r_games1, r_games2)

    # Get both directions for dire pair
    d_fwd1, d_games1 = _get_duo_synergy(hero_data, d_hero1, d_hero2, d_pos1, d_pos2, min_games)
    d_rev1, d_games2 = _get_duo_synergy(hero_data, d_hero2, d_hero1, d_pos2, d_pos1, min_games)

    # Pick the best direction for Dire
    d_diff = d_fwd1 if (d_fwd1 is not None and d_games1 >= d_games2) else d_rev1
    d_games = max(d_games1, d_games2)

    if r_diff is not None and d_diff is not None:
        return r_diff - d_diff, r_games + d_games

    return None, 0


def calculate_lane_advantage(
    radiant_positions: List[Tuple[str, str]],
    dire_positions: List[Tuple[str, str]],
    hero_data: Dict,
    min_games: int = 10
) -> Dict:
    """
    Calculate lane-specific cp1vs1 and duo synergy advantages.

    Returns:
        {
            'mid': {'cp1vs1': float, 'duo': None, 'cp1vs1_valid': bool, 'cp1vs1_games': int},
            'top': {'cp1vs1': float, 'duo': float, 'cp1vs1_valid': bool, 'duo_valid': bool, ...},
            'bot': {'cp1vs1': float, 'duo': float, 'cp1vs1_valid': bool, 'duo_valid': bool, ...},
            'lane_advantage': float,  # weighted average
            'cp1vs1_valid': bool,  # all lanes valid
            'duo_valid': bool,  # top+bot valid
        }
    """
    result = {}
    cp1vs1_values = []
    duo_values = []

    for lane in ('mid', 'top', 'bot'):
        lane_result = {
            'cp1vs1': 0.0,
            'cp1vs1_valid': False,
            'cp1vs1_games': 0,
            'duo': 0.0,
            'duo_valid': False,
            'duo_games': 0,
        }

        # Get hero for each position
        pos_to_hero = {pos: hero for pos, hero in radiant_positions + dire_positions}

        # --- CP1VS1 ---
        matchups = LANE_CP1VS1_PAIRS.get(lane, [])
        min_required = LANE_CP1VS1_MIN_MATCHUPS.get(lane, 1)

        matchup_diffs = []
        matchup_games = []

        for matchup in matchups:
            r_pos, d_pos = matchup[0]
            r_hero = pos_to_hero.get(r_pos)
            d_hero = pos_to_hero.get(d_pos)

            if not r_hero or not d_hero:
                continue

            diff, games = _get_matchup_1v1(hero_data, r_hero, d_hero, r_pos, d_pos, min_games)
            if diff is not None:
                matchup_diffs.append(diff)
                matchup_games.append(games)

        if len(matchup_diffs) >= min_required:
            lane_result['cp1vs1'] = sum(matchup_diffs) / len(matchup_diffs)
            lane_result['cp1vs1_valid'] = True
            lane_result['cp1vs1_games'] = int(sum(matchup_games) / len(matchup_games)) if matchup_games else 0
            cp1vs1_values.append(lane_result['cp1vs1'])

        # --- DUO SYNERGY ---
        duo_config = LANE_DUO_PAIRS.get(lane)
        if duo_config is not None:
            r_pos1, r_pos2 = duo_config['radiant']
            d_pos1, d_pos2 = duo_config['dire']

            r_hero1 = pos_to_hero.get(r_pos1)
            r_hero2 = pos_to_hero.get(r_pos2)
            d_hero1 = pos_to_hero.get(d_pos1)
            d_hero2 = pos_to_hero.get(d_pos2)

            if r_hero1 and r_hero2 and d_hero1 and d_hero2:
                duo_adv, duo_g = _get_duo_synergy_pair(
                    hero_data,
                    r_hero1, r_hero2, r_pos1, r_pos2,
                    d_hero1, d_hero2, d_pos1, d_pos2,
                    min_games
                )

                if duo_adv is not None:
                    lane_result['duo'] = duo_adv
                    lane_result['duo_valid'] = True
                    lane_result['duo_games'] = duo_g
                    duo_values.append(duo_adv)

        result[lane] = lane_result

    # Calculate overall lane_advantage (weighted by presence)
    all_values = cp1vs1_values + duo_values
    if all_values:
        result['lane_advantage'] = sum(all_values) / len(all_values)
    else:
        result['lane_advantage'] = 0.0

    # Overall validity
    result['cp1vs1_valid'] = all(r.get('cp1vs1_valid', False) for r in result.values() if 'cp1vs1_valid' in r)
    result['duo_valid'] = all(r.get('duo_valid', False) for r in result.values() if 'duo_valid' in r)

    return result


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
    result['pro_cp1vs1_reason'] = 'not_computed'
    result['pro_duo_synergy_reason'] = 'not_computed'
    result['pro_cp1vs1_diagnostics'] = {}
    result['pro_duo_synergy_diagnostics'] = {}

    # Собираем всех героев по позициям.
    # Держим raw payload в диагностике, потому что здесь уже ловили странный кейс:
    # полный 5/5 драфт есть, но старый путь неожиданно давал 0 core heroes.
    radiant_positions, radiant_cores, radiant_raw_payload = _extract_team_positions_and_cores(radiant_heroes_and_pos)
    dire_positions, dire_cores, dire_raw_payload = _extract_team_positions_and_cores(dire_heroes_and_pos)

    if len(radiant_cores) < 3 or len(dire_cores) < 3:
        diagnostics = {
            'radiant_positions': [pos for pos, _hero in radiant_positions],
            'dire_positions': [pos for pos, _hero in dire_positions],
            'radiant_cores': list(radiant_cores),
            'dire_cores': list(dire_cores),
            'radiant_core_count': len(radiant_cores),
            'dire_core_count': len(dire_cores),
            'radiant_raw_payload': radiant_raw_payload,
            'dire_raw_payload': dire_raw_payload,
        }
        result['pro_cp1vs1_reason'] = 'insufficient_core_heroes'
        result['pro_duo_synergy_reason'] = 'insufficient_core_heroes'
        result['pro_cp1vs1_diagnostics'] = diagnostics
        result['pro_duo_synergy_diagnostics'] = diagnostics
        print(f"   ⚠️ ProTracker: insufficient core heroes {diagnostics}")
        return result

    # Парсим данные для всех героев
    all_heroes = set(radiant_cores + dire_cores)
    hero_data = {}

    for hero_name in all_heroes:
        hero_data[hero_name] = parse_hero_matchups(hero_name)
        time.sleep(2)

    # ===== CP1VS1 =====
    r_cp_valid, r_cp_data = _calculate_cp1vs1_all_positions(
        radiant_positions, dire_positions, hero_data, min_games
    )

    if r_cp_valid:
        result['pro_cp1vs1_valid'] = True
        result['pro_cp1vs1_reason'] = 'ok'
        scores = r_cp_data['scores']
        result['pro_cp1vs1_early_games'] = r_cp_data['games']
        result['pro_cp1vs1_late_games'] = r_cp_data['games']
        result['pro_cp1vs1_diagnostics'] = {
            'count': r_cp_data['count'],
            'games': r_cp_data['games'],
            'radiant_core_coverage': r_cp_data['radiant_core_coverage'],
            'dire_core_coverage': r_cp_data['dire_core_coverage'],
            'radiant_core_vs_core_coverage': r_cp_data['radiant_core_vs_core_coverage'],
            'dire_core_vs_core_coverage': r_cp_data['dire_core_vs_core_coverage'],
            'required_core_vs_core': r_cp_data['required_core_vs_core'],
        }

        if scores:
            # Сумма weighted scores / count
            cp_score = sum(scores) / len(scores)
            result['pro_cp1vs1_early'] = cp_score
            result['pro_cp1vs1_late'] = cp_score

            print(
                f"   📊 ProTracker cp1vs1: {r_cp_data['count']} matchups, "
                f"score={cp_score:+.1f}%, games={r_cp_data['games']}, "
                f"rad_core_coverage={r_cp_data['radiant_core_coverage']}, "
                f"dire_core_coverage={r_cp_data['dire_core_coverage']}, "
                f"rad_core_vs_core={r_cp_data['radiant_core_vs_core_coverage']}, "
                f"dire_core_vs_core={r_cp_data['dire_core_vs_core_coverage']}"
            )
    else:
        result['pro_cp1vs1_reason'] = 'insufficient_core_vs_core_coverage'
        result['pro_cp1vs1_diagnostics'] = {
            'count': r_cp_data['count'],
            'games': r_cp_data['games'],
            'radiant_core_coverage': r_cp_data['radiant_core_coverage'],
            'dire_core_coverage': r_cp_data['dire_core_coverage'],
            'radiant_core_vs_core_coverage': r_cp_data['radiant_core_vs_core_coverage'],
            'dire_core_vs_core_coverage': r_cp_data['dire_core_vs_core_coverage'],
            'required_core_vs_core': r_cp_data['required_core_vs_core'],
        }
        print(
            "   ⚠️ ProTracker cp1vs1 invalid: "
            f"count={r_cp_data['count']}, games={r_cp_data['games']}, "
            f"rad_core_vs_core={r_cp_data['radiant_core_vs_core_coverage']}, "
            f"dire_core_vs_core={r_cp_data['dire_core_vs_core_coverage']}, "
            f"required={r_cp_data['required_core_vs_core']}"
        )

    # ===== DUO SYNERGY =====
    r_duo_valid, r_duo_data = _calculate_duo_synergy_all_positions(
        radiant_positions, hero_data, min_games, PRO_EARLY_POSITION_WEIGHTS
    )
    d_duo_valid, d_duo_data = _calculate_duo_synergy_all_positions(
        dire_positions, hero_data, min_games, PRO_EARLY_POSITION_WEIGHTS
    )

    if r_duo_valid and d_duo_valid:
        result['pro_duo_synergy_valid'] = True
        result['pro_duo_synergy_reason'] = 'ok'
        r_scores = r_duo_data['scores']
        d_scores = d_duo_data['scores']
        result['pro_duo_synergy_early_games'] = r_duo_data['games'] + d_duo_data['games']
        result['pro_duo_synergy_late_games'] = result['pro_duo_synergy_early_games']
        result['pro_duo_synergy_diagnostics'] = {
            'radiant_count': r_duo_data['count'],
            'dire_count': d_duo_data['count'],
            'games': result['pro_duo_synergy_early_games'],
            'radiant_core_coverage': r_duo_data['core_coverage'],
            'dire_core_coverage': d_duo_data['core_coverage'],
            'required_per_core': max(
                r_duo_data.get('required_per_core', 0),
                d_duo_data.get('required_per_core', 0),
            ),
        }

        if r_scores and d_scores:
            r_avg = sum(r_scores) / len(r_scores)
            d_avg = sum(d_scores) / len(d_scores)
            duo_score = r_avg - d_avg

            result['pro_duo_synergy_early'] = duo_score
            result['pro_duo_synergy_late'] = duo_score

            print(
                f"   📊 ProTracker duo: R={r_duo_data['count']} pairs ({r_avg:+.1f}%, coverage={r_duo_data['core_coverage']}), "
                f"D={d_duo_data['count']} pairs ({d_avg:+.1f}%, coverage={d_duo_data['core_coverage']})"
            )
    else:
        result['pro_duo_synergy_reason'] = 'insufficient_duo_core_coverage'
        result['pro_duo_synergy_diagnostics'] = {
            'radiant_count': r_duo_data['count'],
            'dire_count': d_duo_data['count'],
            'games': r_duo_data['games'] + d_duo_data['games'],
            'radiant_core_coverage': r_duo_data['core_coverage'],
            'dire_core_coverage': d_duo_data['core_coverage'],
            'required_per_core': max(
                r_duo_data.get('required_per_core', 0),
                d_duo_data.get('required_per_core', 0),
            ),
        }
        print(
            "   ⚠️ ProTracker duo invalid: "
            f"R_count={r_duo_data['count']}, D_count={d_duo_data['count']}, "
            f"R_coverage={r_duo_data['core_coverage']}, "
            f"D_coverage={d_duo_data['core_coverage']}, "
            f"required={max(r_duo_data.get('required_per_core', 0), d_duo_data.get('required_per_core', 0))}"
        )

    # ===== LANE ADVANTAGE =====
    lane_data = calculate_lane_advantage(
        radiant_positions, dire_positions, hero_data, min_games
    )

    result['pro_lane_mid_cp1vs1'] = lane_data['mid']['cp1vs1']
    result['pro_lane_top_cp1vs1'] = lane_data['top']['cp1vs1']
    result['pro_lane_bot_cp1vs1'] = lane_data['bot']['cp1vs1']
    result['pro_lane_mid_cp1vs1_valid'] = lane_data['mid']['cp1vs1_valid']
    result['pro_lane_top_cp1vs1_valid'] = lane_data['top']['cp1vs1_valid']
    result['pro_lane_bot_cp1vs1_valid'] = lane_data['bot']['cp1vs1_valid']
    result['pro_lane_mid_cp1vs1_games'] = lane_data['mid']['cp1vs1_games']
    result['pro_lane_top_cp1vs1_games'] = lane_data['top']['cp1vs1_games']
    result['pro_lane_bot_cp1vs1_games'] = lane_data['bot']['cp1vs1_games']

    result['pro_lane_top_duo'] = lane_data['top']['duo']
    result['pro_lane_bot_duo'] = lane_data['bot']['duo']
    result['pro_lane_top_duo_valid'] = lane_data['top']['duo_valid']
    result['pro_lane_bot_duo_valid'] = lane_data['bot']['duo_valid']
    result['pro_lane_top_duo_games'] = lane_data['top']['duo_games']
    result['pro_lane_bot_duo_games'] = lane_data['bot']['duo_games']

    result['pro_lane_advantage'] = lane_data['lane_advantage']
    result['pro_lane_cp1vs1_valid'] = lane_data['cp1vs1_valid']
    result['pro_lane_duo_valid'] = lane_data['duo_valid']

    # Print lane advantage summary
    lane_summary_parts = []
    for lane in ('mid', 'top', 'bot'):
        cp = lane_data[lane]['cp1vs1']
        cp_v = lane_data[lane]['cp1vs1_valid']
        cp_str = f"{cp:+.2f}" if cp != 0 or cp_v else "N/A"
        duo = lane_data[lane]['duo']
        duo_v = lane_data[lane]['duo_valid']
        duo_str = f"{duo:+.2f}" if duo != 0 or duo_v else "N/A"
        lane_summary_parts.append(f"{lane.upper()} cp1vs1={cp_str}({'v' if cp_v else 'inv'}), duo={duo_str}({'v' if duo_v else 'inv'})")

    print(f"   📊 ProTracker lane_advantage: {lane_data['lane_advantage']:+.2f} | {' | '.join(lane_summary_parts)}")

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
