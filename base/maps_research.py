import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', message='.*urllib3.*')

import json
import shutil
from urllib.parse import quote
# from analyze_maps import new_proceed_map, send_message
import ijson
import orjson
import os
from pathlib import Path
from keys import api_to_proxy, start_date_time
import asyncio
import aiohttp
from collections import deque
from datetime import datetime, timedelta
import time
import urllib3
urllib3.disable_warnings()

# Загрузка валидных позиций героев
HERO_VALID_POSITIONS = {}
try:
    # Путь относительно корня проекта
    hero_positions_path = Path('/Users/alex/Documents/ingame/base/hero_valid_positions_simple.json')
    with open(hero_positions_path, 'r', encoding='utf-8') as f:
        HERO_VALID_POSITIONS = json.load(f)
        # Конвертируем ключи в int для быстрого поиска
        HERO_VALID_POSITIONS = {int(k): v for k, v in HERO_VALID_POSITIONS.items()}
except Exception as e:
    print(f"Предупреждение: не удалось загрузить hero_valid_positions_simple.json: {e}")
    print("Проверка валидности позиций будет отключена")

# Rate limits для API
RATE_LIMITS = {
    'second': 7,
    'minute': 138,
    'hour': 1488,
    'day': 14988
}

CONCURRENCY_LIMIT = 20  # Одновременных запросов


def _build_tier_team_ids():
    from id_to_names import tier_one_teams, tier_two_teams
    ids = set()
    for team_ids in list(tier_one_teams.values()) + list(tier_two_teams.values()):
        if isinstance(team_ids, set):
            ids.update(team_ids)
        else:
            ids.add(team_ids)
    return ids


class RateLimitTracker:
    """Отслеживает использование API для одной пары прокси-API"""
    
    def __init__(self, proxy_url, api_token):
        self.proxy_url = proxy_url
        self.api_token = api_token
        self.requests_log = {
            'second': deque(),
            'minute': deque(),
            'hour': deque(),
            'day': deque()
        }
        self.lock = asyncio.Lock()
        self.is_rate_limited = False  # Флаг, что API вернул ошибку лимита
        self.rate_limit_time = 0  # Время когда был достигнут лимит
    
    async def can_make_request(self):
        """Проверяет, можно ли сделать запрос с учетом всех лимитов"""
        async with self.lock:
            now = time.time()
            
            # Проверяем, прошло ли 3 минуты с момента rate limit от API
            if self.is_rate_limited:
                if now - self.rate_limit_time < 180:  # 3 минуты = 180 секунд
                    return False
                else:
                    # Прошло 3 минуты, сбрасываем флаг
                    self.is_rate_limited = False
                    print(f"✅ Восстановление пары: прошло 3 минуты с момента rate limit")
            
            # Очищаем старые записи и проверяем лимиты
            time_windows = {
                'second': 1,
                'minute': 60,
                'hour': 3600,
                'day': 86400
            }
            
            for period, window in time_windows.items():
                # Удаляем старые записи
                while self.requests_log[period] and now - self.requests_log[period][0] > window:
                    self.requests_log[period].popleft()
                
                # Проверяем лимит
                if len(self.requests_log[period]) >= RATE_LIMITS[period]:
                    return False
            
            return True
    
    async def mark_rate_limited(self):
        """Помечает tracker как достигший лимита по ответу API"""
        async with self.lock:
            self.is_rate_limited = True
            self.rate_limit_time = time.time()
            proxy_short = self.proxy_url.split('@')[-1] if '@' in self.proxy_url else self.proxy_url[:30]
            print(f"⛔ API вернул rate limit для прокси {proxy_short}, блокируем на 3 минуты")
    
    async def record_request(self):
        """Записывает факт выполнения запроса"""
        async with self.lock:
            now = time.time()
            for period in self.requests_log:
                self.requests_log[period].append(now)


class ProxyAPIPool:
    """Управляет пулом прокси-API пар с автоматическим переключением"""
    
    def __init__(self, api_to_proxy_dict):
        self.trackers = [
            RateLimitTracker(proxy_url, api_token)
            for proxy_url, api_token in api_to_proxy_dict.items()
        ]
        self.current_index = 0
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async def get_available_tracker(self):
        """Получает доступный tracker или ждет, пока он освободится"""
        max_attempts = len(self.trackers) * 10
        attempt = 0
        previous_index = self.current_index
        
        while attempt < max_attempts:
            # Проверяем все trackers начиная с текущего
            for i in range(len(self.trackers)):
                idx = (self.current_index + i) % len(self.trackers)
                tracker = self.trackers[idx]
                
                if await tracker.can_make_request():
                    # Выводим информацию при смене пары
                    if idx != previous_index:
                        proxy_short = tracker.proxy_url.split('@')[-1] if '@' in tracker.proxy_url else tracker.proxy_url[:30]
                        api_short = tracker.api_token[:20] + '...' if len(tracker.api_token) > 20 else tracker.api_token
                        print(f"🔄 Переключение на пару #{idx + 1}: Прокси={proxy_short}, API={api_short}")
                    
                    self.current_index = idx
                    return tracker
            
            # Если ни один не доступен, ждем немного
            print(f"⏳ Все API достигли лимитов, ожидание 0.5 сек...")
            await asyncio.sleep(0.5)
            attempt += 1
        
        # Если после всех попыток все еще нет доступных, берем первый
        print("⚠️ Превышено время ожидания, использую первый доступный tracker")
        return self.trackers[0]
    
    async def make_request(self, url, **kwargs):
        """Выполняет запрос с автоматическим выбором tracker и rate limiting"""
        max_retries = len(self.trackers) + 1  # Попробуем все пары + 1 попытка после сна
        retry_count = 0
        
        while retry_count < max_retries:
            async with self.semaphore:
                tracker = await self.get_available_tracker()
                
                # Обновляем headers с нужным токеном
                if 'headers' in kwargs:
                    kwargs['headers']['Authorization'] = f"Bearer {tracker.api_token}"
                
                try:
                    # Используем стандартный aiohttp с HTTP прокси
                    async with aiohttp.ClientSession() as proxy_session:
                        async with proxy_session.post(url, proxy=tracker.proxy_url, ssl=False, **kwargs) as response:
                            data = await response.json()
                            
                            # Проверяем на rate limit от API
                            if isinstance(data, dict) and data.get('message') == 'API rate limit exceeded':
                                proxy_short = tracker.proxy_url.split('@')[-1] if '@' in tracker.proxy_url else tracker.proxy_url[:30]
                                print(f"⛔ API rate limit exceeded для прокси {proxy_short}")
                                
                                # Помечаем tracker как заблокированный
                                await tracker.mark_rate_limited()
                                
                                # Переключаемся на следующий tracker
                                old_index = self.current_index
                                self.current_index = (self.current_index + 1) % len(self.trackers)
                                retry_count += 1
                                
                                # Проверяем, все ли пары заблокированы
                                all_limited = all(t.is_rate_limited for t in self.trackers)
                                if all_limited:
                                    print(f"😴 Все пары заблокированы rate limit! Сон на 3 минуты...")
                                    await asyncio.sleep(180)  # 3 минуты
                                    # Сбрасываем флаги после сна
                                    for t in self.trackers:
                                        t.is_rate_limited = False
                                    print(f"⏰ Пробуждение! Продолжаем работу...")
                                    retry_count = 0  # Сбрасываем счетчик после сна
                                
                                continue  # Пробуем следующую пару
                            
                            # Если все ок, записываем запрос и возвращаем данные
                            await tracker.record_request()
                            return data
                            
                except Exception as e:
                    proxy_short = tracker.proxy_url.split('@')[-1] if '@' in tracker.proxy_url else tracker.proxy_url[:30]
                    print(f"❌ Ошибка запроса через прокси {proxy_short}: {e}")
                    
                    # При ошибке переключаемся на следующий tracker
                    old_index = self.current_index
                    self.current_index = (self.current_index + 1) % len(self.trackers)
                    next_tracker = self.trackers[self.current_index]
                    next_proxy_short = next_tracker.proxy_url.split('@')[-1] if '@' in next_tracker.proxy_url else next_tracker.proxy_url[:30]
                    next_api_short = next_tracker.api_token[:20] + '...' if len(next_tracker.api_token) > 20 else next_tracker.api_token
                    print(f"🔄 Переключение на пару #{self.current_index + 1} (из-за ошибки): Прокси={next_proxy_short}, API={next_api_short}")
                    retry_count += 1
                    
                    if retry_count < max_retries:
                        continue
                    else:
                        raise
        
        raise Exception("Исчерпаны все попытки запроса")


# ВАЖНО: НЕ создаем proxy_pool глобально, так как это открывает соединения
# Создаем его только когда нужен (для Stratz API)
proxy_pool = None

def get_proxy_pool():
    """Lazy initialization для proxy_pool"""
    global proxy_pool
    if proxy_pool is None:
        proxy_pool = ProxyAPIPool(api_to_proxy)
    return proxy_pool


_STRATZ_SCHEMA_CACHE = None
_LEAGUE_TIER_CACHE = None
_LEAGUE_TIER_CACHE_PATH = Path("/Users/alex/Documents/ingame/data/opendota_league_tiers.json")


def _load_stratz_schema():
    global _STRATZ_SCHEMA_CACHE
    if _STRATZ_SCHEMA_CACHE is not None:
        return _STRATZ_SCHEMA_CACHE
    schema_path = Path("/Users/alex/Documents/ingame/docs/stratz_schema_match_types.json")
    if not schema_path.exists():
        _STRATZ_SCHEMA_CACHE = {}
        return _STRATZ_SCHEMA_CACHE
    try:
        with schema_path.open("r", encoding="utf-8") as f:
            _STRATZ_SCHEMA_CACHE = json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to load Stratz schema: {e}")
        _STRATZ_SCHEMA_CACHE = {}
    return _STRATZ_SCHEMA_CACHE


def _load_league_tier_map():
    global _LEAGUE_TIER_CACHE
    if _LEAGUE_TIER_CACHE is not None:
        return _LEAGUE_TIER_CACHE
    if _LEAGUE_TIER_CACHE_PATH.exists():
        try:
            with _LEAGUE_TIER_CACHE_PATH.open("r", encoding="utf-8") as f:
                cached = json.load(f)
            _LEAGUE_TIER_CACHE = {
                int(k): v for k, v in cached.items() if k is not None
            }
            return _LEAGUE_TIER_CACHE
        except Exception as e:
            print(f"⚠️ Failed to load OpenDota league tiers cache: {e}")

    try:
        import urllib.request
        with urllib.request.urlopen("https://api.opendota.com/api/leagues", timeout=30) as resp:
            leagues = json.load(resp)
    except Exception as e:
        print(f"⚠️ Failed to fetch OpenDota leagues: {e}")
        _LEAGUE_TIER_CACHE = {}
        return _LEAGUE_TIER_CACHE

    tier_map = {}
    for league in leagues:
        league_id = league.get("leagueid")
        if league_id is None:
            continue
        tier_map[int(league_id)] = {
            "id": int(league_id),
            "name": league.get("name"),
            "tier": get_league_tier(league),
        }

    try:
        _LEAGUE_TIER_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _LEAGUE_TIER_CACHE_PATH.open("w", encoding="utf-8") as f:
            json.dump(tier_map, f)
    except Exception as e:
        print(f"⚠️ Failed to save OpenDota league tiers cache: {e}")

    _LEAGUE_TIER_CACHE = tier_map
    return _LEAGUE_TIER_CACHE


def _unwrap_stratz_type(type_node):
    kind = type_node.get("kind")
    name = type_node.get("name")
    of = type_node.get("ofType")
    while kind in ("NON_NULL", "LIST"):
        type_node = of or {}
        kind = type_node.get("kind")
        name = type_node.get("name")
        of = type_node.get("ofType")
    return kind, name


def _build_stratz_selection(type_name, depth=1, visited=None):
    schema = _load_stratz_schema() or {}
    type_map = schema.get("types") or {}
    if not type_map:
        return ""
    if visited is None:
        visited = set()
    if type_name in visited or depth <= 0:
        return "__typename"
    visited = set(visited)
    visited.add(type_name)
    t = type_map.get(type_name) or {}
    fields = t.get("fields") or []
    parts = []
    for f in fields:
        kind, name = _unwrap_stratz_type(f.get("type") or {})
        fname = f.get("name")
        if kind in ("SCALAR", "ENUM"):
            parts.append(fname)
        elif kind == "OBJECT" and name:
            sub = _build_stratz_selection(name, depth=depth - 1, visited=visited)
            parts.append(f"{fname} {{ {sub} }}")
        else:
            parts.append(f"{fname} {{ __typename }}")
    return " ".join(parts)


def _build_pro_matches_extended_query(match_ids, stats_depth=2, playback_depth=2):
    ids_str = "[" + ",".join(str(int(i)) for i in match_ids) + "]"
    stats_sel = _build_stratz_selection("MatchPlayerStatsType", depth=stats_depth)
    playback_sel = _build_stratz_selection("MatchPlayerPlaybackDataType", depth=playback_depth)
    hero_avg_sel = _build_stratz_selection("HeroPositionTimeDetailType", depth=1)
    add_unit_sel = _build_stratz_selection("MatchPlayerAdditionalUnitType", depth=1)
    dota_plus_sel = _build_stratz_selection("HeroDotaPlusLeaderboardRankType", depth=1)
    ability_sel = _build_stratz_selection("PlayerAbilityType", depth=1)

    if not stats_sel:
        stats_sel = "matchId steamAccountId gameVersionId level lastHitsPerMinute goldPerMinute experiencePerMinute"
    if not playback_sel:
        playback_sel = "__typename"
    if not hero_avg_sel:
        hero_avg_sel = "heroId time position kills deaths assists networth xp cs dn goldPerMinute level"
    if not add_unit_sel:
        add_unit_sel = "item0Id item1Id item2Id item3Id item4Id item5Id backpack0Id backpack1Id backpack2Id neutral0Id"
    if not dota_plus_sel:
        dota_plus_sel = "heroId steamAccountId level totalActions createdDateTime"
    if not ability_sel:
        ability_sel = "abilityId time level gameVersionId isTalent"

    query = f"""
    query {{
      matches(ids: {ids_str}) {{
        id
        didRadiantWin
        durationSeconds
        startDateTime
        endDateTime
        firstBloodTime
        lobbyType
        gameMode
        actualRank
        averageRank
        averageImp
        regionId
        rank
        bracket
        analysisOutcome
        tournamentRound
        gameVersionId
        leagueId
        league {{
          id
          name
          tier
        }}
        seriesId
        series {{
          id
          type
        }}
        radiantTeam {{
          id
          name
        }}
        direTeam {{
          id
          name
        }}
        radiantNetworthLeads
        radiantExperienceLeads
        radiantKills
        direKills
        pickBans {{
          isPick
          heroId
          order
          bannedHeroId
          isRadiant
        }}
        towerDeaths {{
          isRadiant
          npcId
        }}
        bottomLaneOutcome
        midLaneOutcome
        topLaneOutcome
        winRates
        players {{
          matchId
          match {{
            id
          }}
          playerSlot
          steamAccountId
          steamAccount {{
            id
            smurfFlag
            isAnonymous
            name
          }}
          isRadiant
          isVictory
          heroId
          hero {{
            id
            name
            displayName
            shortName
          }}
          gameVersionId
          kills
          deaths
          assists
          leaverStatus
          numLastHits
          numDenies
          goldPerMinute
          networth
          experiencePerMinute
          level
          gold
          goldSpent
          heroDamage
          towerDamage
          heroHealing
          partyId
          isRandom
          lane
          position
          streakPrediction
          intentionalFeeding
          role
          roleBasic
          imp
          award
          item0Id
          item1Id
          item2Id
          item3Id
          item4Id
          item5Id
          backpack0Id
          backpack1Id
          backpack2Id
          neutral0Id
          behavior
          invisibleSeconds
          dotaPlusHeroXp
          variant
          stats {{ {stats_sel} }}
          playbackData {{ {playback_sel} }}
          heroAverage {{ {hero_avg_sel} }}
          additionalUnit {{ {add_unit_sel} }}
          dotaPlus {{ {dota_plus_sel} }}
          abilities {{ {ability_sel} }}
        }}
      }}
    }}
    """
    return query


async def fetch_pro_matches_extended(match_ids, chunk_size=15, stats_depth=2, playback_depth=2):
    if not match_ids:
        return {}
    pool = get_proxy_pool()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Origin": "https://api.stratz.com",
        "Referer": "https://api.stratz.com/graphiql",
        "User-Agent": "STRATZ_API",
    }
    result = {}
    for i in range(0, len(match_ids), chunk_size):
        chunk = match_ids[i:i + chunk_size]
        query = _build_pro_matches_extended_query(
            chunk,
            stats_depth=stats_depth,
            playback_depth=playback_depth,
        )
        data = await pool.make_request(
            url="https://api.stratz.com/graphql",
            json={"query": query},
            headers=headers,
        )
        matches = (data or {}).get("data", {}).get("matches") or []
        for m in matches:
            if not m:
                continue
            mid = m.get("id")
            if mid is not None:
                result[str(mid)] = m
    return result

def load_json_file(filepath, default):
    """Загружает JSON файл с кешированием"""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Ошибка загрузки {filepath}: {e}")
    return default

def load_and_process_json_files(mkdir, **kwargs):
    """Загружает JSON файлы ПОСЛЕДОВАТЕЛЬНО (убрана параллельная обработка)"""
    result = {}
    
    # Собираем файлы для загрузки
    files_to_load = [(key, f'./{mkdir}/{key}') for key, flag in kwargs.items() if flag]
    
    if files_to_load:
        # ПОСЛЕДОВАТЕЛЬНАЯ загрузка файлов (убран ThreadPoolExecutor)
        for key, filepath in files_to_load:
            try:
                result[key] = load_json_file(filepath, {})
            except Exception as e:
                print(f"⚠️ Ошибка при загрузке {key}: {e}")
                result[key] = {}
    
    # Добавляем None для незапрошенных ключей
    for key, flag in kwargs.items():
        if not flag:
            result[key] = None
    
    return result


def save_get_maps_state(maps_to_save, processed_ids, output_data, player_ids, all_teams, phase):
    """
    Сохраняет текущее состояние get_maps_new
    
    Args:
        maps_to_save: базовый путь для сохранения
        processed_ids: список обработанных ID
        output_data: собранные карты
        player_ids: собранные ID игроков
        all_teams: словарь команд
        phase: фаза обработки (1 - первый проход, 2 - второй проход)
    """
    state_file = f'{maps_to_save}_state.json'
    state = {
        'processed_ids': list(processed_ids),
        'output_data': output_data,
        'player_ids': list(player_ids),
        'all_teams': all_teams,
        'phase': phase,
        'timestamp': time.time()
    }
    
    temp_file = f'{state_file}.tmp'
    # ОПТИМИЗАЦИЯ: Убрали indent=2 для ускорения записи на 20-30%
    with open(temp_file, 'w') as f:
        json.dump(state, f)
    
    # Атомарная замена файла
    if os.path.exists(state_file):
        os.replace(temp_file, state_file)
    else:
        os.rename(temp_file, state_file)
    
    print(f"💾 Состояние сохранено: {len(processed_ids)} ID обработано, {len(output_data)} карт собрано")


def load_get_maps_state():
    """
    Загружает сохраненное состояние get_maps_new
    
    Returns:
        tuple: (processed_ids, output_data, player_ids, all_teams, phase) или None
    """
    state_file = f'./analise_pub_matches/maps_state.json'
    
    if not os.path.exists(state_file):
        return None
    
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        processed_ids = set(state['processed_ids'])
        output_data = state['output_data']
        player_ids = set(state['player_ids'])
        all_teams = state['all_teams']
        phase = state['phase']
        timestamp = state.get('timestamp', 0)
        
        print(f"📂 Восстановлено состояние: {len(processed_ids)} ID обработано, {len(output_data)} карт собрано")
        print(f"   Фаза: {phase}, Сохранено: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}")
        
        return processed_ids, output_data, player_ids, all_teams, phase
    except Exception as e:
        print(f"⚠️ Ошибка загрузки состояния: {e}")
        return None


def clear_get_maps_state(maps_to_save):
    """Удаляет файл состояния после успешного завершения"""
    state_file = f'{maps_to_save}_state.json'
    if os.path.exists(state_file):
        os.remove(state_file)
        print(f"🗑️  Файл состояния удален: {state_file}")

async def retry_request_with_proxy_rotation(request_func, *args, max_retries=None, sleep_time=300, **kwargs):
    """
    Retry функция с перебором прокси и сном при исчерпании всех прокси
    
    Args:
        request_func: асинхронная функция для выполнения запроса
        max_retries: максимальное количество попыток (None = бесконечно)
        sleep_time: время сна в секундах при исчерпании всех прокси (по умолчанию 5 минут)
    """
    attempt = 0
    proxies_tried_in_cycle = 0
    pool = get_proxy_pool()  # Получаем proxy_pool только когда нужен
    total_proxies = len(pool.trackers) if hasattr(pool, 'trackers') else 1
    
    while max_retries is None or attempt < max_retries:
        try:
            result = await request_func(*args, **kwargs)
            return result
        except Exception as e:
            attempt += 1
            proxies_tried_in_cycle += 1
            print(f"⚠️ Ошибка при запросе (попытка {attempt}, прокси {proxies_tried_in_cycle}/{total_proxies}): {e}")
            
            # Пробуем переключиться на следующий прокси
            if hasattr(pool, 'trackers') and len(pool.trackers) > 0:
                # Принудительно переключаемся на следующий прокси
                pool.current_index = (pool.current_index + 1) % len(pool.trackers)
                next_tracker = pool.trackers[pool.current_index]
                proxy_short = next_tracker.proxy_url.split('@')[-1] if '@' in next_tracker.proxy_url else next_tracker.proxy_url[:30]
                print(f"🔄 Переключение на прокси #{pool.current_index + 1}: {proxy_short}")
                
                # Если перепробовали все прокси в цикле
                if proxies_tried_in_cycle >= total_proxies:
                    print(f"❌ Все {total_proxies} прокси были перепробованы, спим {sleep_time} секунд (5 минут)...")
                    await asyncio.sleep(sleep_time)
                    
                    # Сбрасываем rate limits для всех прокси
                    for tracker in pool.trackers:
                        tracker.is_rate_limited = False
                        tracker.rate_limit_time = 0
                    
                    proxies_tried_in_cycle = 0  # Сбрасываем счетчик для нового цикла
                    print("⏰ Пробуждение! Сбросили rate limits, продолжаем с первого прокси...")
                else:
                    # Небольшая задержка перед повторной попыткой
                    await asyncio.sleep(2)
            else:
                # Если нет информации о прокси, просто ждем
                await asyncio.sleep(5)
    
    raise Exception(f"Не удалось выполнить запрос после {max_retries} попыток")


async def get_maps_new(ids, mkdir,
                 show_prints=True, skip=0, count=0, pro=False, use_opendota=False, skip_auxiliary_files=False):
    """
    Объединенная функция для сбора и обработки матчей.
    Включает логику фильтрации trash maps и сохранения во временные файлы.
    
    Args:
        use_opendota: если True, использует OpenDota API вместо Stratz
        skip_auxiliary_files: если True, пропускает создание trash_maps, player_ids, all_teams, processed_ids_to_graph
                             (для live обновлений про матчей в cyberscore)
    """
    # Базовое имя для сохранения состояния
    maps_to_save = 'maps'
    
    # ОПТИМИЗАЦИЯ: Настраиваемый интервал чекпоинтов (было 100, стало 500)
    CHECKPOINT_INTERVAL = 500
    
    # Создаём основную папку если её нет
    if not os.path.exists(mkdir):
        os.makedirs(mkdir)
        print(f"📁 Создана папка: {mkdir}")
    
    # Предварительно формируем пути к файлам
    trash_maps_file = f'{mkdir}/trash_maps.txt'
    all_teams_file = f'{mkdir}/all_teams.txt'
    player_ids_file = f'{mkdir}/player_ids.txt'
    processed_graph_ids_file = f"{mkdir}/processed_ids_to_graph.txt"
    
    # Загрузка trash_maps (пропускаем для live обновлений)
    trash_maps = set()
    processed_graph_ids = set()
    
    if not skip_auxiliary_files:
        try:
            with open(trash_maps_file, 'r') as f:
                loaded_trash = json.load(f)
                # Преобразуем все ID в int
                trash_maps = set(int(id) if isinstance(id, str) else id for id in loaded_trash)
            print(f"📋 Загружено {len(trash_maps)} trash maps")
        except:
            trash_maps = set()
            print("📋 Файл trash_maps.txt не найден, начинаем с пустого набора")
        
        # Загружаем уже обработанные ID из ids_to_graph (для накопительного хранения)
        if os.path.exists(processed_graph_ids_file):
            try:
                with open(processed_graph_ids_file, 'r') as f:
                    loaded_ids = json.load(f)
                    # Преобразуем все ID в int для консистентности
                    processed_graph_ids = set(int(id) for id in loaded_ids)
                print(f"📋 Загружено {len(processed_graph_ids)} уже обработанных IDs из ids_to_graph (накопительно)")
            except Exception as e:
                print(f"⚠️ Ошибка при загрузке processed_graph_ids: {e}")
                print(f"⚠️ Создаем резервную копию поврежденного файла...")
                if os.path.exists(processed_graph_ids_file):
                    backup_file = f"{processed_graph_ids_file}.backup"
                    shutil.copy(processed_graph_ids_file, backup_file)
                    print(f"📋 Резервная копия сохранена: {backup_file}")
                processed_graph_ids = set()
        else:
            print(f"📋 Файл {processed_graph_ids_file} не найден, начинаем с нуля")
    else:
        print(f"⏭️  Пропускаем загрузку вспомогательных файлов (skip_auxiliary_files=True)")

    # Попытка восстановить состояние
    saved_state = load_get_maps_state()

    if saved_state:
        processed_ids, output_data, player_ids, all_teams, phase = saved_state
        count = len(processed_ids)
        print(f"♻️  Восстановлено состояние: {len(processed_ids)} ID обработано, фаза {phase}")
    else:
        processed_ids = set()
        output_data = {}  # Теперь словарь для хранения полных данных матчей
        player_ids = set()
        all_teams = {}
        phase = 1

    ids_to_graph = []
    # Преобразуем все входные ID в int для консистентности
    ids_set = set(int(id) for id in ids)
    maps_counter = 0
    allowed_team_ids = _build_tier_team_ids() if pro else None
    existing_match_ids = set()
    if pro:
        processed_ids_file = f"{mkdir}/json_parts_split_from_object/processed_ids.txt"
        if os.path.exists(processed_ids_file):
            try:
                with open(processed_ids_file, 'r', encoding='utf-8') as f:
                    loaded_ids = json.load(f)
                existing_match_ids = set(int(mid) for mid in loaded_ids)
                print(f"📋 Загружено {len(existing_match_ids)} processed match IDs")
            except Exception as e:
                print(f"⚠️ Ошибка при загрузке processed match IDs: {e}")
    league_tier_map = _load_league_tier_map() if pro else None

    # Создаём папку temp_files если её нет
    temp_folder = f"{mkdir}/temp_files"
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
        print(f"📁 Создана папка: {temp_folder}")

    # Фаза 1: Обработка исходных ID
    if phase == 1:
        # Фильтруем ID которые уже были обработаны ранее
        original_count = len(ids_set)
        ids_set = ids_set - processed_graph_ids
        filtered_count = original_count - len(ids_set)
        print(f"🔄 Фаза 1: Обработка {len(ids_set)} новых ID")
        print(f"   📊 Всего входных ID: {original_count}")
        print(f"   ✅ Уже обработано ранее: {filtered_count}")
        print(f"   🆕 К обработке: {len(ids_set)}")
        remaining_ids = ids_set - processed_ids

        for check_id in remaining_ids:
            count += 1
            # ОПТИМИЗАЦИЯ: Преобразуем int только один раз
            check_id_int = int(check_id)
            ids_to_graph.append(check_id_int)
            processed_ids.add(check_id_int)
            processed_graph_ids.add(check_id_int)  # Добавляем в обработанные

            if show_prints:
                print(f'{count}/{len(ids_set)}')

            if len(ids_to_graph) == 5 or count == len(ids_set):
                # Получаем полные данные матчей с retry логикой
                # Используем Stratz API
                matches, new_player_ids = await retry_request_with_proxy_rotation(
                    proceed_get_maps_with_data,
                    skip=skip,
                    ids_to_graph=ids_to_graph, player_ids_check=True,
                    pro=pro,
                    existing_match_ids=existing_match_ids,
                )

                # Обрабатываем каждый матч
                for match in matches:
                    map_id = int(match['id'])

                    # Проверяем качество матча
                    is_valid, reason = check_match_quality(match)

                    if pro:
                        league = match.get('league') or {}
                        if (not league or not league.get('tier')) and league_tier_map is not None:
                            league_id = match.get('leagueId') or league.get('id')
                            if league_id is not None:
                                league_info = league_tier_map.get(int(league_id))
                                if league_info:
                                    match['league'] = {
                                        'id': league_info.get('id', int(league_id)),
                                        'name': league_info.get('name'),
                                        'tier': league_info.get('tier'),
                                    }
                                    league = match['league']
                        if not league or league.get('id') is None or not league.get('tier'):
                            continue
                        if league.get('tier') == 'AMATEUR':
                            continue
                        if allowed_team_ids is not None:
                            r_team = (match.get('radiantTeam') or {}).get('id')
                            d_team = (match.get('direTeam') or {}).get('id')
                            if r_team not in allowed_team_ids or d_team not in allowed_team_ids:
                                continue
                        output_data[str(map_id)] = match
                        maps_counter += 1
                    else:
                        # Для pub матчей - записываем только валидные
                        if is_valid:
                            output_data[str(map_id)] = match
                            maps_counter += 1
                    
                    if not is_valid:
                        trash_maps.add(map_id)

                player_ids.update(new_player_ids)
                ids_to_graph = []

            # ОПТИМИЗАЦИЯ: Сохранение каждые CHECKPOINT_INTERVAL ID (было 100)
            if len(processed_ids) % CHECKPOINT_INTERVAL == 0:
                # Сохраняем временные данные
                if len(output_data) > 0:
                    save_temp_file(output_data, mkdir, count)
                    output_data = {}  # Очищаем после сохранения

                # ОПТИМИЗАЦИЯ: Группируем сохранения и убираем форматирование + сортировку
                # Сохраняем trash_maps (без форматирования)
                with open(trash_maps_file, 'w') as f:
                    json.dump(list(trash_maps), f)

                # Сохраняем обработанные ID БЕЗ сортировки (отложим на финал)
                with open(processed_graph_ids_file, 'w', encoding='utf-8') as f:
                    json.dump(list(processed_graph_ids), f)

                # Сохраняем состояние
                save_get_maps_state(maps_to_save, processed_ids, {}, player_ids, all_teams, phase=1)
                
                # ОПТИМИЗАЦИЯ: Группируем все принты в один
                print(f"💾 Чекпоинт #{len(processed_ids)}: {len(output_data)} матчей сохранено, {len(trash_maps)} trash, {len(processed_graph_ids)} IDs")

        # Финальное сохранение после фазы 1
        if len(output_data) > 0:
            save_temp_file(output_data, mkdir, count)
            print(f"💾 Финально сохранено {len(output_data)} матчей фазы 1")
            output_data = {}

        # ОПТИМИЗАЦИЯ: Финальное сохранение с сортировкой ТОЛЬКО ОДИН РАЗ
        if not skip_auxiliary_files:
            with open(processed_graph_ids_file, 'w', encoding='utf-8') as f:
                json.dump(sorted(list(processed_graph_ids)), f)
            print(f"💾 Финальное сохранение фазы 1: {len(processed_graph_ids)} ID в {processed_graph_ids_file}")

        save_get_maps_state(maps_to_save, processed_ids, {}, player_ids, all_teams, phase=1)

    # ОПТИМИЗАЦИЯ: Финальные сохранения с использованием переменных путей
    # Пропускаем для live обновлений
    if not skip_auxiliary_files:
        with open(player_ids_file, 'w') as f:
            json.dump(list(player_ids), f)

        with open(all_teams_file, 'w') as f:
            json.dump(all_teams, f)

        with open(trash_maps_file, 'w') as f:
            json.dump(list(trash_maps), f)

        # ФИНАЛЬНОЕ сохранение с сортировкой (без форматирования для скорости)
        with open(processed_graph_ids_file, 'w', encoding='utf-8') as f:
            json.dump(sorted(list(processed_graph_ids)), f)
        
        print(f"💾 Сохранены вспомогательные файлы: player_ids, all_teams, trash_maps, processed_ids_to_graph")
    else:
        print(f"⏭️  Пропущено сохранение вспомогательных файлов (skip_auxiliary_files=True)")

    # Удаление файла состояния после успешного завершения
    clear_get_maps_state(maps_to_save)

    print(f"\n✅ Обработка завершена!")
    print(f"🎮 Собрано валидных матчей: {maps_counter}")
    print(f"🗑️  Отклонено trash maps: {len(trash_maps)}")
    if not skip_auxiliary_files:
        print(f"📋 ВСЕГО уникальных IDs в базе (накопительно): {len(processed_graph_ids)}")
        print(f"📄 IDs сохранены в: {processed_graph_ids_file}")
        print(f"✨ При следующем запуске эти {len(processed_graph_ids)} ID будут автоматически отфильтрованы")

    # Объединение temp_files в файлы по 500 МБ
    print(f"\n📦 Объединяем временные файлы...")
    merged_files = merge_temp_files_by_size(
        mkdir=mkdir,
        max_size_mb=500,
        cleanup=True  # Удаляем temp_files после объединения
    )

    if merged_files:
        print(f"✅ Временные файлы объединены: {len(merged_files)} файлов")


def _process_single_json_file(file_path, maps, output):
    """Обрабатывает один JSON файл и возвращает результат"""
    result_set = set() if maps else None
    result_dict = {} if output else None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            loaded_json_content = json.load(f)

            if isinstance(loaded_json_content, list):
                for item_in_list in loaded_json_content:
                    if isinstance(item_in_list, dict) and 'id' in item_in_list:
                        if maps:
                            result_set.add(int(item_in_list['id']))
                        if output:
                            result_dict[str(item_in_list['id'])] = item_in_list
            elif isinstance(loaded_json_content, dict):
                for map_id in loaded_json_content:
                    if maps:
                        raise EnvironmentError
                    if output:
                        result_dict[str(map_id)] = loaded_json_content[map_id]
    except json.JSONDecodeError:
        print(f"Ошибка декодирования JSON в файле: {file_path}. Файл пропущен.")
    except Exception as e:
        print(f"Непредвиденная ошибка при обработке файла {file_path}: {e}. Файл пропущен.")
    
    return result_set, result_dict


def collect_all_maps(folder_path, maps=None, output=None):
    if maps:
        ids_to_exclude_from_json = set()
    else:  # output or default - возвращаем словарь
        ids_to_exclude_from_json = {}
    
    if os.path.exists(folder_path):
        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        print(f"Найдено {len(json_files)} JSON файлов для обработки.")

        if not json_files:
            print("В указанной папке нет JSON файлов.")
            return ids_to_exclude_from_json

        file_paths = [os.path.join(folder_path, f) for f in json_files]
        
        # ПОСЛЕДОВАТЕЛЬНАЯ обработка файлов (убран ThreadPoolExecutor)
        for i, file_path in enumerate(file_paths):
            result_set, result_dict = _process_single_json_file(file_path, maps=maps, output=output)
        
        # Объединяем результаты
            if maps and result_set:
                ids_to_exclude_from_json.update(result_set)
            if output and result_dict:
                ids_to_exclude_from_json.update(result_dict)
            
            if (i + 1) % 100 == 0 or (i + 1) == len(json_files):
                print(f"Обработано {i + 1}/{len(json_files)} файлов. Собрано {len(ids_to_exclude_from_json)} уникальных ID для исключения из JSON.")
    else:
        print(f"Папка {folder_path} не найдена.")

    print(f"Всего собрано {len(ids_to_exclude_from_json)} уникальных ID для исключения из JSON файлов.")
    return ids_to_exclude_from_json



async def proceed_get_maps_with_data(skip=0, only_in_ids=False, ids_to_graph=None,
                                    game_mods=None, all_teams=None, player_ids_check=False,
                                    pro=False, pro_extended=None, pro_extended_chunk=5,
                                    pro_stats_depth=2, pro_playback_depth=2,
                                    existing_match_ids=None):
    """
    Получает полные данные матчей вместо только ID.
    Возвращает: (matches, player_ids) - список матчей и множество ID игроков

    """
    from keys import start_date_time
    matches = []
    player_ids = set()
    check = True
    if pro_extended is None:
        pro_extended = bool(pro)

    while check:
        query = f'''
        {{
          players(steamAccountIds: {ids_to_graph}) {{
            steamAccount {{
                  id
                  smurfFlag
                  isAnonymous
                }}
            matches(request:
             {{startDateTime: {start_date_time},
             take: 100,
              skip: {skip},
               gameModeIds: [22],
                 bracketIds: [8],
                  isStats: true}}) {{
              startDateTime
              analysisOutcome
              towerDeaths{{
                isRadiant
                npcId
                isRadiant
              }}
              didRadiantWin
              bottomLaneOutcome
              topLaneOutcome
              midLaneOutcome
              winRates
              firstBloodTime
              actualRank
              averageImp
              averageRank
              regionId
              rank
              id
              direKills
              radiantNetworthLeads
              players {{
                position
                isRadiant
                kills
                assists
                numDenies
                numLastHits
                goldPerMinute
                networth
                experiencePerMinute
                level
                heroDamage
                heroHealing
                towerDamage
                item0Id
                item1Id
                item2Id
                item3Id
                item4Id
                item5Id
                backpack0Id
                backpack1Id
                backpack2Id
                heroId
                neutral0Id
                invisibleSeconds
                dotaPlusHeroXp
                imp
                deaths
                intentionalFeeding
                steamAccount {{
                  id
                  smurfFlag
                  isAnonymous
                }}
              }}
            }}
          }}
        }}
        '''
        if pro:
            from keys import start_date_time_736
            start_date_time = start_date_time_736
            player_fields = """
                        position
                        isRadiant
                        kills
                        assists
                        deaths
                        goldPerMinute
                        networth
                        experiencePerMinute
                        level
                        heroDamage
                        heroHealing
                        towerDamage
                        item0Id
                        item1Id
                        item2Id
                        item3Id
                        item4Id
                        item5Id
                        backpack0Id
                        backpack1Id
                        backpack2Id
                        heroId
                        neutral0Id
                        invisibleSeconds
                        dotaPlusHeroXp
                        imp
                        lane
                        role
                        roleBasic
                        steamAccount {
                          id
                          smurfFlag
                          isAnonymous
                        }
            """
            match_fields = f"""
                      id
                      didRadiantWin
                      gameVersionId
                      lobbyType
                      radiantTeam {{
                        name
                        id
                      }}
                      direTeam {{
                        name
                        id
                      }}
                      startDateTime
                      durationSeconds
                      leagueId
                      seriesId
                      series {{
                        id
                        type
                      }}
                      direKills
                      radiantKills
            """
            if pro_extended:
                match_fields += f"""
                      players {{
            {player_fields}
                      }}
                """
            query = f'''
                query {{
                  teams(teamIds: {ids_to_graph}) {{
                    matches(request: {{startDateTime: {start_date_time}, take: 100, skip: {skip}, isStats:true}}) {{
            {match_fields}
                    }}
                  }}
                }}
                '''

        encoded_query = quote(query)
        referer = f"https://api.stratz.com/graphiql?query={encoded_query}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Origin": "https://api.stratz.com",
            "Referer": f'{referer}',
            "User-Agent": "STRATZ_API"
        }

        try:
            pool = get_proxy_pool()  # Получаем proxy_pool для Stratz
            data = await pool.make_request(
                url='https://api.stratz.com/graphql',
                json={"query": query},
                headers=headers
            )
            
            # Проверяем, что data не None и содержит нужную структуру
            if data is None:
                print("⚠️ API вернул None, пропускаем итерацию")
                check = False
                continue
            
            if not isinstance(data, dict):
                print(f"⚠️ API вернул некорректный тип данных: {type(data)}")
                check = False
                continue
            
            if 'data' not in data:
                print(f"⚠️ API ответ не содержит ключ 'data': {data}")
                check = False
                continue
            
            if pro:
                teams_data = data.get('data', {}).get('teams')
                if teams_data and any(team.get('matches') for team in teams_data):
                    skip += 100
                    batch_matches = []
                    for team in data['data']['teams']:
                        if team and 'matches' in team:
                            for match in team['matches']:
                                batch_matches.append(match)

                    if existing_match_ids is not None:
                        filtered = []
                        for match in batch_matches:
                            match_id = match.get('id')
                            if match_id is None:
                                continue
                            match_id_int = int(match_id)
                            if match_id_int in existing_match_ids:
                                continue
                            existing_match_ids.add(match_id_int)
                            filtered.append(match)
                        batch_matches = filtered

                    matches.extend(batch_matches)

                    if player_ids_check:
                        for match in batch_matches:
                            for p in match.get('players', []) or []:
                                sa = p.get('steamAccount') or {}
                                pid = sa.get('id')
                                if pid:
                                    player_ids.add(int(pid))
                else:
                    check = False
            else:
                players_data = data.get('data', {}).get('players', [])
                if players_data and any(player.get('matches') for player in players_data):
                    skip += 100
                    for player in players_data:
                        if not player.get('steamAccount'):
                            continue
                        if player['steamAccount'].get('smurfFlag') not in [0, 2] or player['steamAccount'].get('isAnonymous'):
                            continue
                        for match in player.get('matches', []):
                            matches.append(match)
                            if player_ids_check:
                                for extra_player in match.get('players', []):
                                    steam_account = extra_player.get('steamAccount')
                                    if (not extra_player.get('intentionalFeeding') and
                                        steam_account and
                                        steam_account.get('smurfFlag') in [0, 2] and
                                        not steam_account.get('isAnonymous')):
                                        player_ids.add(int(steam_account['id']))
                else:
                    check = False


        except Exception as e:
            print(f"Unexpected error in proceed_get_maps_with_data: {e}")
            # Пробрасываем ошибку дальше для retry логики
            raise

    return matches, player_ids


async def proceed_get_maps_with_data_opendota(skip=0, ids_to_graph=None, 
                                               player_ids_check=False, pro=False,
                                               concurrent_requests=10):
    """
    Альтернативная функция для получения матчей через OpenDota API.
    Возвращает: (matches, player_ids) - список матчей и множество ID игроков
    
    OpenDota API endpoints:
    - /teams/{team_id}/matches - матчи команды (краткая инфа)
    - /matches/{match_id} - детали матча (полная инфа)
    
    Args:
        concurrent_requests: количество параллельных запросов (по умолчанию 10)
                           Увеличь до 30-60 если хочешь быстрее (но можешь упереться в rate limit)
    """
    from keys import start_date_time
    import time
    
    matches = []
    player_ids = set()
    
    if not pro:
        print("⚠️ OpenDota API поддерживает только режим pro=True (команды)")
        return matches, player_ids
    
    print(f"🔍 Получение матчей через OpenDota API для {len(ids_to_graph)} команд...")
    print(f"⚡ Параллельных запросов: {concurrent_requests}")
    
    # OpenDota использует Unix timestamp в секундах
    start_timestamp = int(start_date_time)
    
    # Вспомогательная функция для получения деталей одного матча
    async def fetch_match_details(session, match_id, semaphore, retry_count=0):
        """Получает детали одного матча с rate limiting через semaphore"""
        async with semaphore:  # Ограничиваем количество параллельных запросов
            try:
                match_url = f'https://api.opendota.com/api/matches/{match_id}'
                async with session.get(match_url) as match_response:
                    if match_response.status == 200:
                        match_details = await match_response.json()
                        converted_match = convert_opendota_to_stratz_format(match_details)
                        
                        # Собираем player IDs если требуется
                        match_player_ids = set()
                        if player_ids_check and converted_match:
                            for player in converted_match.get('players', []):
                                steam_account = player.get('steamAccount')
                                if steam_account and steam_account.get('id'):
                                    match_player_ids.add(int(steam_account['id']))
                        
                        # ВАЖНО: Задержка для соблюдения rate limit OpenDota
                        # OpenDota: 60 запросов/минуту = 1 запрос/секунду
                        # При concurrent_requests=5 это даст ~5 запросов в секунду
                        # Поэтому задержка 1 секунда на запрос = 60/минуту - ровно на границе
                        await asyncio.sleep(1.0)
                        
                        return converted_match, match_player_ids
                    
                    elif match_response.status == 429:
                        # Rate limit exceeded - exponential backoff
                        wait_time = min(30, 10 * (2 ** retry_count))  # 10, 20, 30 секунд
                        print(f"   ⚠️ Rate limit #{match_id}, жду {wait_time} сек (попытка {retry_count+1})...")
                        await asyncio.sleep(wait_time)
                        
                        # Retry с увеличенной задержкой
                        if retry_count < 3:
                            return await fetch_match_details(session, match_id, semaphore, retry_count + 1)
                        else:
                            print(f"   ❌ Матч {match_id} пропущен после {retry_count+1} попыток")
                            return None, set()
                    
                    else:
                        error_text = await match_response.text()
                        if match_response.status != 404:  # 404 - матч не найден, это нормально
                            print(f"   ⚠️ Ошибка {match_response.status} для матча {match_id}: {error_text[:100]}")
                        return None, set()
            
            except Exception as e:
                print(f"   ⚠️ Ошибка при получении матча {match_id}: {e}")
                return None, set()
    
    # Создаем семафор для ограничения параллельных запросов
    semaphore = asyncio.Semaphore(concurrent_requests)
    
    # ВАЖНО: Создаем сессию БЕЗ прокси для прямого подключения к OpenDota
    # Полностью отключаем прокси: trust_env=False и явно создаем connector без прокси
    connector = aiohttp.TCPConnector(limit=concurrent_requests * 2, force_close=False)
    # ЭКСТРЕМАЛЬНО увеличиваем таймауты для OpenDota (API может отвечать ОЧЕНЬ медленно)
    # sock_read=600 (10 минут) для больших команд с 200+ матчами
    timeout = aiohttp.ClientTimeout(total=900, connect=60, sock_read=600)
    # Явно отключаем все прокси
    async with aiohttp.ClientSession(
        connector=connector, 
        trust_env=False,  # Игнорировать переменные окружения
        timeout=timeout
    ) as session:
        # КРИТИЧЕСКИ ВАЖНО: Проверяем что НЕ используется прокси
        print(f"   🔍 Сессия создана без прокси (trust_env=False)")
        # Для каждой команды получаем матчи
        for team_id in ids_to_graph:
            try:
                # Получаем список матчей команды С RETRY ЛОГИКОЙ
                url = f'https://api.opendota.com/api/teams/{team_id}/matches'
                
                print(f"\n🌐 Запрашиваем матчи для команды {team_id}...")
                
                # Retry логика для получения списка матчей
                team_matches = None
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        async with session.get(url) as response:
                            if response.status != 200:
                                error_text = await response.text()
                                print(f"⚠️ Ошибка {response.status} для команды {team_id}: {error_text}")
                                break
                            
                            team_matches = await response.json()
                            break  # Успешно получили данные
                    except asyncio.TimeoutError:
                        if retry < max_retries - 1:
                            wait_time = 10 * (retry + 1)
                            print(f"   ⏱️ Таймаут при получении списка матчей, попытка {retry+1}/{max_retries}, жду {wait_time} сек...")
                            await asyncio.sleep(wait_time)
                        else:
                            print(f"   ❌ Не удалось получить список матчей после {max_retries} попыток")
                            break
                
                if team_matches is None:
                    continue
                
                # Фильтруем матчи по дате
                filtered_matches = [
                    m for m in team_matches 
                    if m.get('start_time', 0) >= start_timestamp
                ]
                
                print(f"✅ Команда {team_id}: найдено {len(filtered_matches)} матчей после {datetime.fromtimestamp(start_timestamp)}")
                
                if not filtered_matches:
                    print(f"   ⏭️ Пропускаем команду - нет подходящих матчей")
                    continue
                
                # Ограничиваем количество матчей для обработки
                matches_to_process = filtered_matches[:100]  # Максимум 100 как в Stratz
                print(f"   📥 Обработаем {len(matches_to_process)} матчей ПАРАЛЛЕЛЬНО...")
                
                # Получаем match_ids
                match_ids = [m.get('match_id') for m in matches_to_process if m.get('match_id')]
                
                # ПАРАЛЛЕЛЬНО получаем детали всех матчей
                print(f"   ⚡ Запускаем {len(match_ids)} параллельных запросов (по {concurrent_requests} одновременно)...")
                tasks = [fetch_match_details(session, match_id, semaphore) for match_id in match_ids]
                results = await asyncio.gather(*tasks)
                
                # Обрабатываем результаты
                team_matches_count = 0
                for converted_match, match_player_ids in results:
                    if converted_match:
                        matches.append(converted_match)
                        player_ids.update(match_player_ids)
                        team_matches_count += 1
                
                print(f"   ✅ Получено {team_matches_count}/{len(match_ids)} матчей для команды {team_id}")
                
                # Небольшая задержка между командами
                await asyncio.sleep(0.5)
            
            except Exception as e:
                print(f"⚠️ Ошибка при обработке команды {team_id}: {e}")
                import traceback
                traceback.print_exc()
                continue
    
    print(f"✅ OpenDota: получено {len(matches)} матчей, {len(player_ids)} игроков")
    return matches, player_ids


def convert_opendota_to_stratz_format(opendota_match):
    """
    Конвертирует структуру данных OpenDota в формат Stratz для совместимости.
    МАКСИМАЛЬНО ДЕТАЛЬНАЯ версия с извлечением ВСЕХ доступных данных.
    """
    try:
        # Базовая информация о матче
        match_id = opendota_match.get('match_id')
        if not match_id:
            return None
        
        # Определяем победителя (radiant_win)
        radiant_win = opendota_match.get('radiant_win', False)
        
        # Получаем информацию о командах
        radiant_team_id = opendota_match.get('radiant_team_id')
        dire_team_id = opendota_match.get('dire_team_id')
        radiant_name = opendota_match.get('radiant_name', f'Team {radiant_team_id}')
        dire_name = opendota_match.get('dire_name', f'Team {dire_team_id}')
        
        # Получаем детальную информацию о командах
        radiant_team_info = opendota_match.get('radiant_team', {})
        dire_team_info = opendota_match.get('dire_team', {})
        
        # Базовая структура матча в формате Stratz (РАСШИРЕННАЯ)
        stratz_match = {
            # Основная информация
            'id': str(match_id),
            'didRadiantWin': radiant_win,
            'startDateTime': str(opendota_match.get('start_time', 0)),
            'durationSeconds': opendota_match.get('duration', 0),
            'gameVersionId': opendota_match.get('patch', 0),
            'lobbyType': opendota_match.get('lobby_type', 0),
            'regionId': opendota_match.get('region', 0),
            'cluster': opendota_match.get('cluster', 0),
            'gameMode': opendota_match.get('game_mode', 0),
            
            # Команды
            'radiantTeam': {
                'id': radiant_team_id if radiant_team_id else 0,
                'name': radiant_name,
                'logo': radiant_team_info.get('logo_url'),
                'tag': radiant_team_info.get('tag')
            },
            'direTeam': {
                'id': dire_team_id if dire_team_id else 0,
                'name': dire_name,
                'logo': dire_team_info.get('logo_url'),
                'tag': dire_team_info.get('tag')
            },
            
            # Счет и статистика
            'radiantKills': opendota_match.get('radiant_score', 0),
            'direKills': opendota_match.get('dire_score', 0),
            'radiantScore': opendota_match.get('radiant_score', 0),
            'direScore': opendota_match.get('dire_score', 0),
            
            # ВАЖНО: Преимущество по золоту и опыту (есть в OpenDota!)
            'radiantNetworthLeads': opendota_match.get('radiant_gold_adv', []),
            'radiantExperienceLeads': opendota_match.get('radiant_xp_adv', []),
            
            # Статус зданий
            'towerStatusRadiant': opendota_match.get('tower_status_radiant', 0),
            'towerStatusDire': opendota_match.get('tower_status_dire', 0),
            'barracksStatusRadiant': opendota_match.get('barracks_status_radiant', 0),
            'barracksStatusDire': opendota_match.get('barracks_status_dire', 0),
            
            # Лига и серия
            'league': {
                'id': opendota_match.get('leagueid', 0),
                'name': opendota_match.get('league', {}).get('name', 'Unknown'),
                'tier': get_league_tier(opendota_match.get('league', {}))
            },
            'seriesId': opendota_match.get('series_id', 0),
            'seriesType': opendota_match.get('series_type', 0),
            
            # Капитаны команд
            'radiantCaptain': opendota_match.get('radiant_captain'),
            'direCaptain': opendota_match.get('dire_captain'),
            
            # Таймлайн драфта
            'draftTimings': opendota_match.get('draft_timings', []),
            
            # Цели (башни, бараки, рошан и т.д.)
            'objectives': opendota_match.get('objectives', []),
            
            # Командные бои
            'teamfights': opendota_match.get('teamfights', []),
            
            # Чат
            'chat': opendota_match.get('chat', []),
            
            # Паузы
            'pauses': opendota_match.get('pauses', []),
            
            # Реплей
            'replayUrl': opendota_match.get('replay_url'),
            'replaySalt': opendota_match.get('replay_salt'),
            
            # Дополнительные данные OpenDota
            'odData': opendota_match.get('od_data', {}),
            
            # Игроки
            'players': [],
            
            # Pick/Bans
            'pickBans': [],
            
            # Башни (конвертируем из objectives)
            'towerDeaths': []
        }
        
        # Обрабатываем objectives для получения towerDeaths
        for obj in opendota_match.get('objectives', []):
            if obj.get('type') in ['CHAT_MESSAGE_TOWER_KILL', 'building_kill']:
                tower_death = {
                    'time': obj.get('time'),
                    'npcId': obj.get('key', 0),
                    'isRadiant': obj.get('team', 0) == 2  # В OpenDota 2=Radiant, 3=Dire
                }
                stratz_match['towerDeaths'].append(tower_death)
        
        # Обрабатываем игроков - МАКСИМАЛЬНО ДЕТАЛЬНО
        players_data = opendota_match.get('players', [])
        for idx, player in enumerate(players_data):
            is_radiant = player.get('isRadiant', idx < 5)
            
            # Определяем позицию
            position = player.get('lane_role', idx if is_radiant else idx - 5)
            
            player_data = {
                # Основная информация
                'steamAccount': {
                    'id': player.get('account_id', 0),
                    'name': player.get('personaname'),
                    'smurfFlag': 0,
                    'isAnonymous': player.get('account_id') is None
                },
                'heroId': player.get('hero_id', 0),
                'heroVariant': player.get('hero_variant', 0),
                'isRadiant': is_radiant,
                'position': position,
                'lane': player.get('lane', 0),
                'laneRole': player.get('lane_role', 0),
                'role': player.get('role'),
                'roleBasic': player.get('role_basic'),
                
                # KDA
                'kills': player.get('kills', 0),
                'deaths': player.get('deaths', 0),
                'assists': player.get('assists', 0),
                'heroKills': player.get('hero_kills', 0),
                
                # Фарм
                'numLastHits': player.get('last_hits', 0),
                'numDenies': player.get('denies', 0),
                'goldPerMinute': player.get('gold_per_min', 0),
                'experiencePerMinute': player.get('xp_per_min', 0),
                'networth': player.get('net_worth', 0),
                'gold': player.get('gold', 0),
                'goldSpent': player.get('gold_spent', 0),
                'totalGold': player.get('total_gold', 0),
                'totalXp': player.get('total_xp', 0),
                
                # Урон и лечение
                'heroDamage': player.get('hero_damage', 0),
                'towerDamage': player.get('tower_damage', 0),
                'heroHealing': player.get('hero_healing', 0),
                
                # Уровень и способности
                'level': player.get('level', 0),
                'abilityUpgrades': player.get('ability_upgrades_arr', []),
                'abilityUses': player.get('ability_uses', {}),
                'abilityTargets': player.get('ability_targets', {}),
                
                # Предметы
                'item0Id': player.get('item_0', 0),
                'item1Id': player.get('item_1', 0),
                'item2Id': player.get('item_2', 0),
                'item3Id': player.get('item_3', 0),
                'item4Id': player.get('item_4', 0),
                'item5Id': player.get('item_5', 0),
                'backpack0Id': player.get('backpack_0', 0),
                'backpack1Id': player.get('backpack_1', 0),
                'backpack2Id': player.get('backpack_2', 0),
                'neutral0Id': player.get('item_neutral', 0),
                'neutral1Id': player.get('item_neutral2', 0),
                
                # Предметы - дополнительно
                'aghanimsScepter': player.get('aghanims_scepter', 0),
                'aghanimsShard': player.get('aghanims_shard', 0),
                'moonshard': player.get('moonshard', 0),
                'itemUsage': player.get('item_usage', {}),
                'itemUses': player.get('item_uses', {}),
                'purchaseLog': player.get('purchase_log', []),
                
                # Действия
                'actionsPerMin': player.get('actions_per_min', 0),
                'actions': player.get('actions', {}),
                
                # Убийства
                'towerKills': player.get('tower_kills', 0),
                'roshanKills': player.get('roshan_kills', 0),
                'ancientKills': player.get('ancient_kills', 0),
                'neutralKills': player.get('neutral_kills', 0),
                'courierKills': player.get('courier_kills', 0),
                'observerKills': player.get('observer_kills', 0),
                'sentryKills': player.get('sentry_kills', 0),
                'laneKills': player.get('lane_kills', 0),
                
                # Варды
                'obsPlaced': player.get('obs_placed', 0),
                'senPlaced': player.get('sen_placed', 0),
                'observerUses': player.get('observer_uses', 0),
                'sentryUses': player.get('sentry_uses', 0),
                'obsLog': player.get('obs_log', []),
                'senLog': player.get('sen_log', []),
                
                # Стаки
                'campsStacked': player.get('camps_stacked', 0),
                'creepsStacked': player.get('creeps_stacked', 0),
                
                # Байбэки и другое
                'buybackCount': player.get('buyback_count', 0),
                'buybackLog': player.get('buyback_log', []),
                'runePickups': player.get('rune_pickups', 0),
                'runes': player.get('runes', {}),
                'firstbloodClaimed': player.get('firstblood_claimed', 0),
                
                # Бенчмарки
                'benchmarks': player.get('benchmarks', {}),
                
                # Урон детально
                'damage': player.get('damage', {}),
                'damageInflictor': player.get('damage_inflictor', {}),
                'damageTaken': player.get('damage_taken', {}),
                'damageTargets': player.get('damage_targets', {}),
                
                # Лечение детально
                'healing': player.get('healing', {}),
                
                # Позиция на карте
                'lanePos': player.get('lane_pos', {}),
                'obs': player.get('obs', {}),
                'sen': player.get('sen', {}),
                
                # Временные ряды
                'goldTimeline': player.get('gold_t', []),
                'xpTimeline': player.get('xp_t', []),
                'lhTimeline': player.get('lh_t', []),
                'dnTimeline': player.get('dn_t', []),
                'times': player.get('times', []),
                
                # Эффективность
                'laneEfficiency': player.get('lane_efficiency', 0),
                'laneEfficiencyPct': player.get('lane_efficiency_pct', 0),
                'teamfightParticipation': player.get('teamfight_participation', 0),
                
                # Флаги
                'intentionalFeeding': False,
                'isRoaming': player.get('is_roaming', False),
                'leaverStatus': player.get('leaver_status', 0),
                'randomed': player.get('randomed', False),
                
                # Ранг
                'rankTier': player.get('rank_tier', 80),
                
                # Дополнительные данные
                'imp': 0,  # OpenDota не предоставляет IMP
                'dotaPlusHeroXp': 0,
                'invisibleSeconds': 0,
                'pings': player.get('pings', 0),
                'stuns': player.get('stuns', 0),
                'kda': player.get('kda', 0),
                'killsPerMin': player.get('kills_per_min', 0),
                'permanentBuffs': player.get('permanent_buffs', []),
                'cosmetics': player.get('cosmetics', []),
                'partyId': player.get('party_id', 0),
                'partySize': player.get('party_size', 0),
            }
            
            stratz_match['players'].append(player_data)
        
        # Pick/Bans
        picks_bans = opendota_match.get('picks_bans', [])
        for pb in picks_bans:
            stratz_match['pickBans'].append({
                'isPick': pb.get('is_pick', False),
                'heroId': pb.get('hero_id', 0),
                'bannedHeroId': pb.get('hero_id', 0) if not pb.get('is_pick') else None,
                'isRadiant': pb.get('team', 0) == 0,
                'order': pb.get('order', 0)
            })
        
        # Дополнительные поля для совместимости со Stratz
        stratz_match['analysisOutcome'] = None
        stratz_match['bottomLaneOutcome'] = None
        stratz_match['topLaneOutcome'] = None
        stratz_match['midLaneOutcome'] = None
        stratz_match['winRates'] = []
        stratz_match['firstBloodTime'] = opendota_match.get('first_blood_time', 0)
        stratz_match['actualRank'] = 80
        stratz_match['averageRank'] = 80
        stratz_match['averageImp'] = 0
        stratz_match['rank'] = 80
        stratz_match['bracket'] = 8
        stratz_match['tournamentRound'] = None
        
        # Дополнительные метаданные из OpenDota
        stratz_match['_opendota'] = {
            'throw': opendota_match.get('throw', 0),
            'loss': opendota_match.get('loss', 0),
            'version': opendota_match.get('version', 0),
            'engine': opendota_match.get('engine', 0),
            'preGameDuration': opendota_match.get('pre_game_duration', 0),
            'humanPlayers': opendota_match.get('human_players', 10),
            'flags': opendota_match.get('flags', 0),
        }
        
        # ДОБАВЛЕНО: Поля для enhanced predictor и team_indirect_strength
        stratz_match['radiant_name'] = radiant_name
        stratz_match['dire_name'] = dire_name
        stratz_match['radiant_team'] = str(radiant_team_id) if radiant_team_id else ''
        stratz_match['dire_team'] = str(dire_team_id) if dire_team_id else ''
        
        return stratz_match
        
    except Exception as e:
        print(f"⚠️ Ошибка конвертации матча: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_league_tier(league_info):
    """
    Определяет tier лиги на основе информации OpenDota.
    """
    tier = league_info.get('tier')
    
    if tier == 'premium':
        return 'PREMIUM'
    elif tier == 'professional':
        return 'PROFESSIONAL'
    elif tier == 'amateur':
        return 'AMATEUR'
    else:
        return 'AMATEUR'


async def proceed_get_maps(skip, only_in_ids, output_data,
                           player_ids=None, all_teams=None, ids_to_graph=None, game_mods=None,
                     check=True, player_ids_check=False, pro=False):
    while check:
        if pro:
            query = f'''
            {{
              teams(teamIds: {ids_to_graph}) {{
                matches(request: {{startDateTime: {start_date_time}, take: 100, skip: {skip}, isStats:true}}) {{
                  id
                  radiantTeam {{
                    name
                    id
                  }}
                  direTeam {{
                    name
                    id
                  }}
                }}
              }}
            }}'''
        else:
            query = f'''
            {{
              players(steamAccountIds: {ids_to_graph}) {{
                steamAccount{{
                    smurfFlag
                    isAnonymous
                    id
                    name 
                }}
                
                matches(request:
                 {{startDateTime: {start_date_time},
                 take: 100,
                  skip: {skip},
                   gameModeIds: {game_mods},
                    regionIds: [1, 2, 3, 5],
                     bracketIds: [8],
                      isStats: true}}) {{
                  id
                  regionId
                  averageRank
                  lobbyType
                  players{{
                  intentionalFeeding
                  steamAccount{{
                    smurfFlag
                    id
                    isAnonymous
                  }}
                  }}
              }}
              }}
            }}
            '''

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Origin": "https://api.stratz.com",
            "Referer": "https://api.stratz.com/graphiql",
            "User-Agent": "STRATZ_API",
            "Authorization": "Bearer placeholder"  # Будет заменен в make_request
        }
        try:
            pool = get_proxy_pool()  # Получаем proxy_pool для Stratz
            data = await pool.make_request(
                url='https://api.stratz.com/graphql',
                                   json={"query": query},
                headers=headers
            )

            if game_mods == [2, 22]:
                if any(player['matches'] for player in data['data']['players']):
                    skip += 100
                    for player in data['data']['players']:
                        if player['steamAccount']['smurfFlag'] not in [0, 2] and player['steamAccount']['isAnonymous']:
                            continue
                        for match in player['matches']:
                            output_data.append(match['id'])
                            if not player_ids_check: continue
                            for extra_player in match['players']:
                                if extra_player['intentionalFeeding'] or extra_player['steamAccount']['smurfFlag'] not in [0, 2] or extra_player['steamAccount']['isAnonymous']:
                                    continue
                                player_ids.add(int(extra_player['steamAccount']['id']))
                else:
                    check = False
            else:
                check = False
                for team in data['data']['teams']:
                    for match in team['matches']:
                        if match['radiantTeam']['name'] not in all_teams:
                            all_teams[match['radiantTeam']['name']] = match['radiantTeam']['id']
                        elif match['direTeam']['name'] not in all_teams:
                            all_teams[match['direTeam']['name']] = match['direTeam']['id']
                        if only_in_ids:
                            output_data.append(match['id'])
                        else:
                            output_data.append(match['id'])

        except Exception as e:
            print(f"Unexpected error: {e}")
            check = False

    if player_ids_check:
        return player_ids
    else:
        return None


def eat_temp_files(mkdir, file_data, file_name):
    folder_path = f"{mkdir}/temp_files"
    if os.path.exists(folder_path):
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for map_id in data:
                        if map_id not in file_data:
                            file_data[map_id] = data[map_id]
            except:
                pass
        with open(f'{mkdir}/{file_name}_new.txt', 'w') as f:
            json.dump(file_data, f)
        os.remove(f'{mkdir}/{file_name}.txt')
        os.rename(f'{mkdir}/{file_name}_new.txt', f'{mkdir}/{file_name}.txt')
        shutil.rmtree(f'{mkdir}/temp_files')
        return file_data


def merge_temp_files_by_size(mkdir, max_size_mb=500, output_dir=None, cleanup=False):
    """
    Объединяет temp_files в файлы с ограничением по размеру, фильтруя дубликаты

    Args:
        mkdir: папка с temp_files (например 'analise_pub_matches')
        max_size_mb: максимальный размер выходного файла в МБ (по умолчанию 500)
        output_dir: директория для сохранения (по умолчанию '{mkdir}/json_parts_split_from_object')
        cleanup: удалить temp_files после объединения (по умолчанию False)

    Returns:
        list: список путей к созданным файлам
    """
    temp_folder = f"{mkdir}/temp_files"

    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
        print(f"📁 Создана папка: {temp_folder} (была пустая, нечего объединять)")
        return []

    # Создаём выходную директорию
    if output_dir is None:
        output_dir = f"{mkdir}/json_parts_split_from_object"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 Создана папка: {output_dir}")

    # Файл с ID уже обработанных матчей
    processed_ids_file = f"{output_dir}/processed_ids.txt"

    # Загружаем уже обработанные ID из файла
    processed_ids = set()
    if os.path.exists(processed_ids_file):
        try:
            with open(processed_ids_file, 'r', encoding='utf-8') as f:
                loaded_ids = json.load(f)
                # Преобразуем все ID в int
                processed_ids = set(int(id) if isinstance(id, str) else id for id in loaded_ids)
            print(f"📋 Загружено {len(processed_ids)} уже обработанных ID из {processed_ids_file}")
        except Exception as e:
            print(f"⚠️ Ошибка при загрузке processed_ids: {e}")
            processed_ids = set()
    else:
        print(f"📋 Файл {processed_ids_file} не найден, начинаем с нуля")

    # Находим максимальный номер файла combined{N}.json
    existing_files = [f for f in os.listdir(output_dir) if f.startswith('combined') and f.endswith('.json')]
    max_number = 0
    for f in existing_files:
        try:
            # Извлекаем число из имени файла combined{N}.json
            num = int(f.replace('combined', '').replace('.json', ''))
            max_number = max(max_number, num)
        except:
            continue

    part_number = max_number + 1
    print(f"🔢 Начинаем нумерацию с combined{part_number}.json")

    # Получаем все temp файлы
    temp_files = sorted([f for f in os.listdir(temp_folder) if f.endswith('.txt')])

    if not temp_files:
        print(f"⚠️ В папке {temp_folder} нет .txt файлов")
        return []

    print(f"📊 Найдено {len(temp_files)} временных файлов")
    print(f"🎯 Объединяем в файлы по {max_size_mb} МБ с фильтрацией дубликатов...")

    max_size_bytes = max_size_mb * 1024 * 1024
    current_data = {}
    current_size = 0
    output_files = []
    total_matches = 0
    duplicates_count = 0
    new_ids_in_session = set()  # ID добавленные в текущей сессии

    for i, filename in enumerate(temp_files, 1):
        file_path = os.path.join(temp_folder, filename)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not isinstance(data, dict):
                continue

            # Фильтруем дубликаты - добавляем только новые match_id
            new_entries = {}
            for match_id, match_data in data.items():
                # Преобразуем match_id в int для проверки
                match_id_int = int(match_id) if isinstance(match_id, str) else match_id
                # Проверяем и против старых processed_ids, и против новых в текущей сессии
                if match_id_int not in processed_ids and match_id_int not in new_ids_in_session:
                    new_entries[match_id] = match_data
                    new_ids_in_session.add(match_id_int)
                else:
                    duplicates_count += 1

            if not new_entries:
                continue  # Все записи из этого файла - дубликаты

            # Определяем размер новых данных в JSON
            new_data_json = json.dumps(new_entries, ensure_ascii=False)
            new_data_size = len(new_data_json.encode('utf-8'))

            # Если добавление этих данных превысит лимит и у нас уже есть данные
            if current_size + new_data_size > max_size_bytes and current_data:
                # Сохраняем текущий файл
                output_filename = f"{output_dir}/combined{part_number}.json"
                with open(output_filename, 'w', encoding='utf-8') as f:
                    json.dump(current_data, f, ensure_ascii=False)

                file_size_mb = current_size / (1024 * 1024)
                print(f"  ✅ {output_filename}: {len(current_data)} матчей ({file_size_mb:.1f} МБ)")

                output_files.append(output_filename)
                total_matches += len(current_data)

                # Обновляем файл processed_ids
                processed_ids.update(new_ids_in_session)
                with open(processed_ids_file, 'w', encoding='utf-8') as f:
                    json.dump(list(processed_ids), f, ensure_ascii=False)

                # Начинаем новый файл
                current_data = {}
                current_size = 0
                part_number += 1

            # Добавляем только новые данные
            current_data.update(new_entries)
            current_size += new_data_size

            # Прогресс
            if i % 50 == 0 or i == len(temp_files):
                print(f"  📈 Обработано {i}/{len(temp_files)} файлов | Новых: {len(new_ids_in_session)} | Дубликатов: {duplicates_count}")

        except Exception as e:
            print(f"  ⚠️ Ошибка при чтении {file_path}: {e}")
            continue

    # Сохраняем остаток
    if current_data:
        output_filename = f"{output_dir}/combined{part_number}.json"
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(current_data, f, ensure_ascii=False)

        file_size_mb = current_size / (1024 * 1024)
        print(f"  ✅ {output_filename}: {len(current_data)} матчей ({file_size_mb:.1f} МБ)")

        output_files.append(output_filename)
        total_matches += len(current_data)

    # Финальное обновление файла processed_ids
    processed_ids.update(new_ids_in_session)
    with open(processed_ids_file, 'w', encoding='utf-8') as f:
        json.dump(list(processed_ids), f, ensure_ascii=False)

    print(f"\n🎉 Объединение завершено!")
    print(f"📦 Создано файлов: {len(output_files)}")
    print(f"🎮 Новых матчей добавлено: {len(new_ids_in_session)}")
    print(f"🔄 Дубликатов отфильтровано: {duplicates_count}")
    print(f"💾 Всего уникальных ID в базе: {len(processed_ids)}")
    print(f"📄 ID сохранены в: {processed_ids_file}")

    # Очистка temp_files если запрошено
    if cleanup:
        try:
            shutil.rmtree(temp_folder)
            print(f"🗑️  Папка {temp_folder} удалена")
        except Exception as e:
            print(f"⚠️ Не удалось удалить {temp_folder}: {e}")

    return output_files




def check_match_quality(match, check_old_maps=False):
    """Проверяет качество данных карты"""
    if match.get('direKills') is None:
        return False, 'kills None'
    if len(match.get('radiantNetworthLeads', [])) < 20:
        return False, 'too short'
    for player in match.get('players', []):
        if None == player.get('position'):
            return False, 'position'
        if None == player.get('heroId', {}):
            return False, 'hero id None'
        if None == player.get('steamAccount'):
            return False, 'steamAccount None'
        if player['intentionalFeeding']:
            return False, 'intentionalFeeding'
        if player['steamAccount']['smurfFlag'] not in [0, 2]:
            return False, 'smurf'

        # Проверка валидности позиции героя
        if HERO_VALID_POSITIONS:  # Проверяем только если словарь загружен
            hero_id = player.get('heroId')
            position = player.get('position')
            if hero_id in HERO_VALID_POSITIONS:
                valid_positions = HERO_VALID_POSITIONS[hero_id]
                if position not in valid_positions:
                    return False, f'invalid position {position} for hero {hero_id}'


    # Проверка на аномальную разницу в deaths между игроками команды
    # Проверяем только если максимальное deaths >= 10
    players = match.get('players', [])
    radiant_deaths = [p.get('deaths', 0) for p in players if p.get('isRadiant')]
    dire_deaths = [p.get('deaths', 0) for p in players if not p.get('isRadiant')]

    for team_deaths, team_name in [(radiant_deaths, 'radiant'), (dire_deaths, 'dire')]:
        if len(team_deaths) >= 5:
            # Сортируем по возрастанию и берем 4-й и 5-й элементы (индексы 3 и 4)
            sorted_deaths = sorted(team_deaths)
            min_first = sorted_deaths[3]
            min_second = sorted_deaths[4]

            # Находим максимум для проверки порога
            max_deaths = max(team_deaths)

            # Проверяем только если максимум >= 10
            if max_deaths >= 10:
                # Если у игрока со вторым минимальным deaths в 2+ раза больше чем у первого минимального
                if min_first > 0 and min_second >= min_first * 2:
                    return False, f'death anomaly {team_name} ({min_first}/{min_second})'
    
    # ========== ФИЛЬТРАЦИЯ МАТЧЕЙ ПРОИГРАННЫХ ИЗ-ЗА РАЗНИЦЫ В СКИЛЕ ==========
    # Фокусируемся на ранней фазе игры, где драфт еще не проявился
    
    networth_leads = match.get('radiantNetworthLeads', [])
    
    # 1. Раннее доминирование: огромный разрыв по нетворту на 10-15 минутах
    # Драфт еще не проявился так сильно, так что это скорее скил
    if len(networth_leads) >= 15:
        early_networth_diff = abs(networth_leads[14])  # 15-я минута (индекс 14)
        # Если на 15 минуте разница > 15k - это слишком рано для драфта, скорее скил
        if early_networth_diff > 15000:
            return False, f'skill gap: early dominance ({early_networth_diff:.0f} networth diff at 15min)'
        
        # Проверяем средний lead на 10-15 минутах - если стабильно огромный разрыв
        early_slice = [abs(lead) for lead in networth_leads[10:15]]
        avg_early_lead = sum(early_slice) / len(early_slice) if early_slice else 0
        if avg_early_lead > 12000:
            return False, f'skill gap: consistent early dominance (avg {avg_early_lead:.0f} lead 10-15min)'
    
    # 2. Очень раннее доминирование: разрыв на первых 10 минутах
    # На первых 10 минутах драфт практически не влияет, это чистая механика
    if len(networth_leads) >= 10:
        very_early_slice = [abs(lead) for lead in networth_leads[5:10]]  # 6-10 минуты
        avg_very_early_lead = sum(very_early_slice) / len(very_early_slice) if very_early_slice else 0
        # Если на 6-10 минутах уже разрыв > 10k - это точно скил
        if avg_very_early_lead > 10000:
            return False, f'skill gap: very early dominance (avg {avg_very_early_lead:.0f} lead 6-10min)'
    
    # 3. Сочетание раннего доминирования и огромной разницы в kills
    # Если команда доминировала по нетворту рано И имеет огромное преимущество в киллах - это скил
    radiant_kills = match.get('radiantKills', 0)
    dire_kills = match.get('direKills', 0)

    # 4. Скорость роста преимущества: если нетворт растет очень быстро в ранней фазе
    # Резкий рост преимущества в первые минуты - признак скила, а не драфта
    if len(networth_leads) >= 10:
        # Проверяем градиент: как быстро растет разрыв с 5 по 10 минуту
        minute_5_lead = abs(networth_leads[5]) if len(networth_leads) > 5 else 0
        minute_10_lead = abs(networth_leads[9]) if len(networth_leads) > 9 else 0
        growth_rate = minute_10_lead - minute_5_lead  # Рост за 5 минут
        
        # Если разрыв вырос более чем на 8k за 5 минут (с 5 по 10) - это слишком быстро
        if growth_rate > 8000:
            return False, f'skill gap: rapid advantage growth ({growth_rate:.0f} growth 5-10min)'
        
        # Также проверяем скорость с 0 по 5 минуту
        if len(networth_leads) >= 6:
            minute_0_lead = abs(networth_leads[0]) if len(networth_leads) > 0 else 0
            early_growth = minute_5_lead - minute_0_lead
            # Если с 0 по 5 минуту разрыв вырос более чем на 6k - слишком быстро
            if early_growth > 6000:
                return False, f'skill gap: very rapid early growth ({early_growth:.0f} growth 0-5min)'
    
    # 5. Пропорциональные deaths: если матч короткий, но deaths очень высокие
    # Это может означать что игроки делали много ошибок (скил)
    match_duration = len(networth_leads)
    radiant_total_deaths = sum(radiant_deaths) if radiant_deaths else 0
    dire_total_deaths = sum(dire_deaths) if dire_deaths else 0
    
    if match_duration >= 20 and match_duration <= 30:  # Короткий-средний матч
        total_deaths = radiant_total_deaths + dire_total_deaths
        deaths_per_minute = total_deaths / match_duration
        
        # Если >= 2.5 deaths в минуту в коротком матче - слишком много ошибок
        if deaths_per_minute >= 2.5:
            # Если при этом было раннее доминирование одной команды
            if len(networth_leads) >= 15:
                early_lead = abs(networth_leads[14])
                if early_lead > 10000:
                    return False, f'skill gap: high deaths rate ({deaths_per_minute:.2f}/min) + early dominance in short match'
    
    # # 6. Экстремальная разница в KDA между командами
    # # KDA = (kills + assists) / max(deaths, 1)
    # radiant_players = [p for p in players if p.get('isRadiant')]
    # dire_players = [p for p in players if not p.get('isRadiant')]
    #
    # if len(radiant_players) == 5 and len(dire_players) == 5:
    #     # Вычисляем средний KDA для каждой команды
    #     def calc_team_kda(team_players):
    #         total_kda = 0.0
    #         for p in team_players:
    #             kills = p.get('kills', 0)
    #             assists = p.get('assists', 0)
    #             deaths = max(p.get('deaths', 0), 1)  # Избегаем деления на 0
    #             kda = (kills + assists) / deaths
    #             total_kda += kda
    #         return total_kda / len(team_players) if team_players else 0.0
    #
    #     radiant_avg_kda = calc_team_kda(radiant_players)
    #     dire_avg_kda = calc_team_kda(dire_players)
    #
    #     # Проверяем экстремальную разницу (если одна команда имеет KDA в 2+ раза выше)
    #     if radiant_avg_kda > 0 and dire_avg_kda > 0:
    #         kda_ratio = max(radiant_avg_kda, dire_avg_kda) / min(radiant_avg_kda, dire_avg_kda)
    #         # Если разница в KDA >= 2.0 - это катастрофическая разница в скиле
    #         if kda_ratio >= 2.0:
    #             return False, f'skill gap: extreme KDA difference (R:{radiant_avg_kda:.2f} vs D:{dire_avg_kda:.2f}, ratio:{kda_ratio:.2f})'
    
    return True, 'ok'


def save_temp_file(new_data, mkdir, another_counter):
    # Создание папки для временных файлов
    temp_folder = f"{mkdir}/temp_files"
    if not os.path.isdir(temp_folder):
        os.makedirs(temp_folder)

    # ОПТИМИЗАЦИЯ: Используем timestamp для гарантии уникальности вместо цикла проверок
    unique_suffix = int(time.time() * 1000)  # Миллисекунды для уникальности
    path = f'{temp_folder}/{another_counter}_{unique_suffix}.txt'

    # Сохранение данных во временный файл
    with open(path, 'w') as f:
        json.dump(new_data, f)


def save_json_file(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f)



async def get_maps_new_multiworker(ids, mkdir, show_prints=None, pro=False, 
                                   use_opendota=False, num_workers=3, concurrent_requests=10):
    """
    МУЛЬТИВОРКЕРНАЯ версия get_maps_new для параллельной обработки команд.
    
    Args:
        num_workers: количество параллельных воркеров (по умолчанию 3)
        concurrent_requests: параллельных запросов на воркера
    """
    import math
    
    print(f"🚀 МУЛЬТИВОРКЕРНЫЙ РЕЖИМ: {num_workers} воркеров × {concurrent_requests} параллельных запросов")
    print(f"📊 Всего команд: {len(ids)}")
    
    # Разделяем команды на группы для воркеров
    chunk_size = math.ceil(len(ids) / num_workers)
    id_chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]
    
    print(f"📦 Разделено на {len(id_chunks)} групп:")
    for i, chunk in enumerate(id_chunks, 1):
        print(f"   Воркер #{i}: {len(chunk)} команд")
    
    # Создаем задачи для каждого воркера
    async def worker_task(worker_id, team_ids):
        print(f"\n👷 Воркер #{worker_id} начал работу с {len(team_ids)} командами")
        
        matches, player_ids = await retry_request_with_proxy_rotation(
            proceed_get_maps_with_data,
            skip=0,
            ids_to_graph=team_ids,
            player_ids_check=True,
            pro=pro
        )
        
        print(f"✅ Воркер #{worker_id} завершил: {len(matches)} матчей, {len(player_ids)} игроков")
        return matches, player_ids
    
    # Запускаем все воркеры параллельно
    print(f"\n⚡ Запускаем {len(id_chunks)} воркеров параллельно...")
    tasks = [worker_task(i+1, chunk) for i, chunk in enumerate(id_chunks)]
    results = await asyncio.gather(*tasks)
    
    # Объединяем результаты от всех воркеров
    all_matches = []
    all_player_ids = set()
    
    for matches, player_ids in results:
        all_matches.extend(matches)
        all_player_ids.update(player_ids)
    
    print(f"\n{'='*80}")
    print(f"🎉 ВСЕ ВОРКЕРЫ ЗАВЕРШИЛИ РАБОТУ!")
    print(f"📊 Итого получено:")
    print(f"   🎮 Матчей: {len(all_matches)}")
    print(f"   👥 Уникальных игроков: {len(all_player_ids)}")
    print(f"{'='*80}")
    
    # Теперь вызываем обычную обработку для сохранения
    # Но передаем уже готовые матчи через модифицированную версию
    # Для простоты сохраним напрямую
    
    # Создаём папку если нет
    import os
    if not os.path.exists(mkdir):
        os.makedirs(mkdir)
    
    temp_folder = f"{mkdir}/temp_files"
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
    
    # Сохраняем матчи
    import json
    from datetime import datetime
    timestamp = int(datetime.now().timestamp())
    
    output_data = {str(match['id']): match for match in all_matches}
    
    temp_file = f"{temp_folder}/worker_combined_{timestamp}.json"
    with open(temp_file, 'w') as f:
        json.dump(output_data, f)
    
    print(f"\n💾 Матчи сохранены в: {temp_file}")
    
    # Сохраняем player IDs
    player_ids_file = f'{mkdir}/player_ids.txt'
    with open(player_ids_file, 'w') as f:
        json.dump(list(all_player_ids), f)
    
    print(f"💾 Player IDs сохранены в: {player_ids_file}")
    
    # Объединяем temp файлы
    from maps_research import merge_temp_files_by_size
    print(f"\n📦 Объединяем временные файлы...")
    merged_files = merge_temp_files_by_size(
        mkdir=mkdir,
        max_size_mb=500,
        cleanup=True
    )
    
    if merged_files:
        print(f"✅ Создано {len(merged_files)} объединенных файлов")
    
    return all_matches, all_player_ids



def get_pros():
    from id_to_names import tier_one_teams, tier_two_teams
    ids = list(tier_one_teams.values()) + list(tier_two_teams.values())
    ids_clean = []
    for i in ids:
        if isinstance(i, set):
            for foo in i:
                ids_clean.append(foo)
        else:
            ids_clean.append(i)
    asyncio.run(get_maps_new(ids=ids_clean, pro=True,
                             mkdir='/Users/alex/Documents/ingame/pro_heroes_data', skip_auxiliary_files=True))


def get_pubs():
    from concurrent.futures import ProcessPoolExecutor
    import multiprocessing
    
    json_dir = '/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object'
    files = [os.path.join(json_dir, f) for f in os.listdir(json_dir) if f.endswith('.json')]
    
    num_workers = min(multiprocessing.cpu_count(), len(files))
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = executor.map(_process_json_file, files)
    
    ids = set()
    for result in results:
        ids.update(result)

    asyncio.run(get_maps_new(ids=ids,
                             mkdir='/Users/alex/Documents/ingame/bets_data/analise_pub_matches'))


def _process_json_file(filepath):
    # Подавляем warnings в дочерних процессах (ProcessPoolExecutor)
    import warnings
    warnings.filterwarnings('ignore')
    
    ids = set()
    with open(filepath, 'rb') as f:
        parser = ijson.kvitems(f, '', use_float=True)
        for match_id, match_data in parser:
            players = match_data.get('players')
            if not players:
                continue
            for player in players:
                steam_account = player.get('steamAccount')
                if steam_account and not steam_account.get('isAnonymous', True):
                    player_id = steam_account.get('id')
                    if player_id:
                        ids.add(player_id)
    return ids

def update_my_protracker(show_prints=True, use_opendota=False, num_workers=1, concurrent_requests=5):
    # get_pubs()
    get_pros()



if __name__ == "__main__":
    # with open('teams_stat_dict.txt', 'r+') as f:
    #     data = json.load(f)
    # teams_ids = set()
    # for team in data:
    #     id = data[team]['id']
    #     if id > 0:
    #         teams_ids.add(id)
    # set(teams_ids)
    # pass
    # with open('./all_teams/1722505765_top600_output.json', 'r+') as f:
    #     data = json.load(f)
    # with open('./pro_heroes_data/pro_output.txt', 'r') as f:
    #     to_be_merged = json.load(f)
    # for map_id in to_be_merged:
    #     if map_id not in data:
    #         data[map_id] = to_be_merged[map_id]
    # with open('./all_teams/1722505765_top600_output.json', 'w') as f:
    #     json.dump(data, f)
    
    # ЗАПУСК: для использования OpenDota API вместо Stratz добавь use_opendota=True
    update_my_protracker(show_prints=True)
