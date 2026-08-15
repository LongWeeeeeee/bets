import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', message='.*urllib3.*')

import json
import re
import shutil
from urllib.parse import quote
# from analyze_maps import new_proceed_map, send_message
try:
    import ijson
except Exception:
    ijson = None
import orjson
import os
from pathlib import Path
from keys import STRATZ_PROXY_MAP, start_date_time, start_date_time_739, start_date_time_736
try:
    from keys import DOTA_PATCH_SPECS
except ImportError:
    DOTA_PATCH_SPECS = (
        ("7.39", 1747785600, 1748476800),
        ("7.39b", 1748476800, 1750723200),
        ("7.39c", 1750723200, 1754352000),
        ("7.39d", 1754352000, 1759363200),
        ("7.39e", 1759363200, 1765756800),
        ("7.40", 1765756800, 1766448000),
        ("7.40b", 1766448000, 1768953600),
        ("7.40c", 1768953600, 1774310400),
        ("7.41", 1774310400, 1774656000),
        ("7.41a", 1774656000, 1775606400),
        ("7.41b", 1775606400, 1778025600),
        ("7.41c", 1778025600, 1780531200),
        ("7.41d", 1780531200, 1782950400),
        ("7.41e", 1782950400, None),
    )
import asyncio
try:
    import aiohttp
except Exception:
    aiohttp = None
import requests
from curl_cffi import requests as cf_requests
from collections import deque, Counter
from datetime import datetime, timedelta
import time
import urllib3
urllib3.disable_warnings()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = PROJECT_ROOT / "base"
DOCS_DIR = PROJECT_ROOT / "docs"
PRO_HEROES_DIR = PROJECT_ROOT / "pro_heroes_data"
ANALYSE_PUB_DIR = PROJECT_ROOT / "bets_data" / "analise_pub_matches"
PUBS_SOURCE_DIR = ANALYSE_PUB_DIR / "json_parts_split_from_object"

# Загрузка валидных позиций героев
HERO_VALID_POSITIONS = {}
HERO_POSITION_STATS = {}
try:
    HERO_POSITION_STATS_MIN_PERCENTAGE = float(os.getenv("HERO_POSITION_STATS_MIN_PERCENTAGE", "1"))
except (TypeError, ValueError):
    HERO_POSITION_STATS_MIN_PERCENTAGE = 1.0
try:
    hero_position_stats_path = BASE_DIR / 'hero_position_stats.json'
    with open(hero_position_stats_path, 'r', encoding='utf-8') as f:
        HERO_POSITION_STATS = json.load(f)
        HERO_POSITION_STATS = {
            int(k): v
            for k, v in HERO_POSITION_STATS.items()
            if str(k).isdigit() and isinstance(v, dict)
        }
except Exception as e:
    print(f"Предупреждение: не удалось загрузить hero_position_stats.json: {e}")
    HERO_POSITION_STATS = {}


def _normalize_position_label(position):
    if not isinstance(position, str):
        return None
    value = position.strip().lower()
    if value.startswith('position_'):
        suffix = value.split('_', 1)[1]
        if suffix.isdigit() and 1 <= int(suffix) <= 5:
            return f'pos{suffix}'
    if value.startswith('pos'):
        suffix = value[3:]
        if suffix.isdigit() and 1 <= int(suffix) <= 5:
            return f'pos{suffix}'
    return value or None


def _hero_allowed_positions(hero_id):
    try:
        hero_id = int(hero_id)
    except (TypeError, ValueError):
        return set()

    allowed = set()

    raw_stats = HERO_POSITION_STATS.get(hero_id)
    if isinstance(raw_stats, dict):
        raw_positions = raw_stats.get('positions')
        if isinstance(raw_positions, dict):
            for raw_position, position_stats in raw_positions.items():
                if not isinstance(position_stats, dict):
                    continue
                try:
                    percentage = float(position_stats.get('percentage', 0))
                except (TypeError, ValueError):
                    continue
                if percentage < HERO_POSITION_STATS_MIN_PERCENTAGE:
                    continue
                normalized = _normalize_position_label(f'pos{raw_position}')
                if normalized:
                    allowed.add(normalized)

    raw_positions = HERO_VALID_POSITIONS.get(hero_id)
    if isinstance(raw_positions, (list, tuple, set)):
        for raw_position in raw_positions:
            normalized = _normalize_position_label(raw_position)
            if normalized:
                allowed.add(normalized)

    return allowed


def _position_is_valid_for_hero(hero_id, position):
    allowed_positions = _hero_allowed_positions(hero_id)
    if not allowed_positions:
        return None
    normalized_position = _normalize_position_label(position)
    if normalized_position is None:
        return False
    return normalized_position in allowed_positions


def _has_position_catalog():
    return bool(HERO_POSITION_STATS or HERO_VALID_POSITIONS)


def _normalize_map_id(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit() or (stripped.startswith("-") and stripped[1:].isdigit()):
            try:
                return int(stripped)
            except Exception:
                return None
        return None
    try:
        return int(value)
    except Exception:
        return None


def _iter_json_object_keys(file_path):
    if ijson is not None:
        with open(file_path, "rb") as f:
            for prefix, event, value in ijson.parse(f):
                if prefix == "" and event == "map_key":
                    yield value
        return

    with open(file_path, "rb") as f:
        data = orjson.loads(f.read())
    if isinstance(data, dict):
        yield from data.keys()


def _scan_output_dir_match_ids(output_dir) -> set:
    """match_id, уже лежащие в part-файлах каталога — источник правды для дедупа.

    Читает только top-level ключи (значения не разбираются), поэтому проход по
    десятку гигабайт стоит минуты, а не десятков минут.
    """
    ids = set()
    for path in sorted(Path(output_dir).glob("*_part*.json")):
        if path.stat().st_size < 10:
            continue
        try:
            for raw_key in _iter_json_object_keys(path):
                normalized = _normalize_map_id(raw_key)
                if normalized is not None:
                    ids.add(normalized)
        except Exception as e:
            print(f"⚠️ Не удалось просканировать {path.name} для дедупа: {e}")
    return ids


def _load_hero_ids_from_json(path: Path) -> set[int]:
    hero_ids = set()
    for key in _iter_json_object_keys(path):
        map_id = _normalize_map_id(key)
        if map_id is not None:
            hero_ids.add(map_id)
    return hero_ids


# Реальные лимиты Stratz НА КЛЮЧ, сняты 2026-08-05 из заголовков ответа
# (x-ratelimit-limit-*): 8/сек, 150/мин, 1500/час, 15000/сутки. Суточное окно —
# главный потолок: 5 ключей = 75к запросов в день на весь сбор. Локальный
# самотормоз держим чуть ниже реального: 429-ответы наш счётчик не пишет, а API
# их считает, поэтому идти впритык нельзя.
RATE_LIMITS = {
    'second': 7,
    'minute': 145,
    'hour': 1450,
    'day': 14800
}

# Блокировка пары, если API не прислал ни retry-after, ни ratelimit-reset.
RATE_LIMIT_FALLBACK_BLOCK = 180
# Потолок одной блокировки: даже при retry-after на 4 часа перепроверяем раз в час,
# чтобы не залипнуть навсегда на кривом заголовке.
RATE_LIMIT_MAX_BLOCK = 3600
# Пауза по окну, когда его остаток вышел в 0, а reset брать нельзя: в ответе
# Stratz reset привязан к суточному окну.
RATE_LIMIT_WINDOW_BLOCK = {'second': 2, 'minute': 65, 'hour': 300, 'day': 900}


def _header_int(headers, name):
    """Целое из заголовка ответа без учёта регистра; None, если нет или не число."""
    if not headers:
        return None
    try:
        value = headers.get(name)
    except AttributeError:
        return None
    if value is None:
        try:
            lowered = {str(k).lower(): v for k, v in headers.items()}
        except Exception:
            return None
        value = lowered.get(name.lower())
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


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
    """Отслеживает использование API для одной пары прокси-API.
    Трекеры с одинаковым api_token делят общий rate-limit state."""
    
    def __init__(self, proxy_url, api_token, shared_state=None):
        self.proxy_url = proxy_url
        self.api_token = api_token
        if shared_state is None:
            shared_state = {
                'requests_log': {
                    'second': deque(),
                    'minute': deque(),
                    'hour': deque(),
                    'day': deque()
                },
                'lock': asyncio.Lock(),
                'blocked_until': 0.0,
                'block_scope': '',
            }
        self._shared = shared_state
        self.requests_log = shared_state['requests_log']
        self.lock = shared_state['lock']

    @property
    def short_name(self):
        proxy = self.proxy_url or 'direct'
        return proxy.split('@')[-1] if '@' in proxy else proxy[:30]

    @property
    def blocked_until(self):
        return self._shared['blocked_until']

    @property
    def block_scope(self):
        return self._shared['block_scope']

    @property
    def is_rate_limited(self):
        return time.time() < self._shared['blocked_until']

    async def can_make_request(self):
        """Проверяет, можно ли сделать запрос с учетом всех лимитов"""
        async with self.lock:
            now = time.time()

            # Блокировка от API держится ровно столько, сколько сказал сам API,
            # а не фиксированные 3 минуты: суточное окно так не переждать.
            if now < self._shared['blocked_until']:
                return False
            if self._shared['block_scope']:
                print(f"✅ Восстановление пары {self.short_name}: "
                      f"блокировка «{self._shared['block_scope']}» истекла")
                self._shared['block_scope'] = ''

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

    async def mark_rate_limited(self, retry_after=None, scope=None):
        """Блокирует пару до конца выбитого окна.

        retry_after — секунды из заголовка API (авторитет), scope — какое окно
        кончилось. Без заголовка откатываемся на фиксированную паузу.
        """
        async with self.lock:
            if retry_after is not None:
                delay = float(retry_after)
            else:
                delay = float(RATE_LIMIT_WINDOW_BLOCK.get(scope, RATE_LIMIT_FALLBACK_BLOCK))
            delay = max(1.0, min(delay, float(RATE_LIMIT_MAX_BLOCK)))
            self._shared['blocked_until'] = time.time() + delay
            self._shared['block_scope'] = scope or 'unknown'
            until = time.strftime('%H:%M:%S', time.localtime(self._shared['blocked_until']))
            print(f"⛔ {self.short_name}: выбито окно «{self._shared['block_scope']}», "
                  f"пара заблокирована на {int(delay)} c (до {until})")

    async def apply_rate_limit_headers(self, headers):
        """Синхронизирует пару с заголовками Stratz (x-ratelimit-*).

        Возвращает имя выбитого окна или None. Для секунды/минуты retry-after не
        берём: в ответе он привязан к суточному окну и загнал бы пару в час
        простоя из-за секундного всплеска.
        """
        if not headers:
            return None
        reset = _header_int(headers, 'retry-after')
        if reset is None:
            reset = _header_int(headers, 'ratelimit-reset')
        await self._sync_day_window(headers)
        for window in ('day', 'hour', 'minute', 'second'):
            remaining = _header_int(headers, f'x-ratelimit-remaining-{window}')
            if remaining is not None and remaining <= 0:
                await self.mark_rate_limited(
                    reset if window in ('day', 'hour') else None,
                    scope=window,
                )
                return window
        return None

    async def _sync_day_window(self, headers):
        """Приводит локальный суточный счётчик к серверному остатку.

        Зачем. Локально сутки считаются СКОЛЬЗЯЩИМ окном по нашему же журналу
        запросов, а у Stratz окно ФИКСИРОВАННОЕ и откатывается разом. После
        отката сервер отдаёт свежие 15 000, но наш журнал всё ещё помнит вчерашние
        отметки — и пара стоит в «локальном самотормозе» при полностью свободной
        квоте. Замер 06.08: три аккаунта имели остаток 14 998 из 15 000, пока
        сбор писал «все пары упёрлись в локальный самотормоз».

        Авторитет здесь — сервер: он единственный знает, сколько израсходовано в
        ТЕКУЩЕМ окне. Подрезаем журнал до серверного `used`, лишнего не
        добавляем — счётчик только опускается, поэтому наш запас (14 800 против
        реальных 15 000) сохраняется.
        """
        remaining = _header_int(headers, 'x-ratelimit-remaining-day')
        if remaining is None:
            return
        limit = _header_int(headers, 'x-ratelimit-limit-day') or RATE_LIMITS['day']
        used = max(0, int(limit) - int(remaining))
        async with self.lock:
            log = self.requests_log['day']
            extra = len(log) - used
            for _ in range(extra):
                log.popleft()

    async def record_request(self):
        """Записывает факт выполнения запроса"""
        async with self.lock:
            now = time.time()
            for period in self.requests_log:
                self.requests_log[period].append(now)


class ProxyAPIPool:
    """Управляет пулом прокси-API пар с автоматическим переключением"""
    
    def __init__(self, api_to_proxy_dict):
        token_states = {}
        self.trackers = []
        for proxy_url, api_token in api_to_proxy_dict.items():
            state = token_states.get(api_token)
            tracker = RateLimitTracker(proxy_url, api_token, shared_state=state)
            if api_token not in token_states:
                token_states[api_token] = tracker._shared
            self.trackers.append(tracker)
        self.current_index = 0
        self.selection_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async def get_available_tracker(self):
        """Получает доступный tracker (равномерный round-robin по всем парам)"""
        max_attempts = len(self.trackers) * 10
        attempt = 0

        while attempt < max_attempts:
            async with self.selection_lock:
                # Round-robin: всегда начинаем со следующего индекса
                start_idx = self.current_index
                for i in range(len(self.trackers)):
                    idx = (start_idx + i) % len(self.trackers)
                    tracker = self.trackers[idx]

                    if await tracker.can_make_request():
                        # Продвигаем индекс для следующего вызова
                        self.current_index = (idx + 1) % len(self.trackers)
                        return tracker

            # Если пары блокированы самим API — спим до конца блокировки, а не
            # крутим полусекундный цикл: запрос в заблокированную пару только
            # добавляет 429. Локальный самотормоз ждём как раньше.
            if any(t.is_rate_limited for t in self.trackers):
                await self._sleep_until_first_free()
            else:
                if attempt % 20 == 0:
                    print("⏳ Все пары упёрлись в локальный самотормоз, ожидание...")
                await asyncio.sleep(0.5)
            attempt += 1

        # Раньше здесь безусловно бралась первая пара — то есть запрос уходил в
        # заведомо заблокированную пару. Теперь дожидаемся ближайшей свободной.
        await self._sleep_until_first_free()
        return min(self.trackers, key=lambda t: t.blocked_until)

    async def _sleep_until_first_free(self, cap=300):
        """Спит до освобождения ближайшей пары, но не дольше cap секунд."""
        now = time.time()
        unblock = min((t.blocked_until for t in self.trackers), default=now)
        wait = max(1.0, min(unblock - now, float(cap)))
        scopes = ", ".join(sorted({t.block_scope for t in self.trackers if t.block_scope}))
        until = time.strftime('%H:%M:%S', time.localtime(unblock))
        print(f"😴 Все пары заблокированы ({scopes or 'rate limit'}); ближайшая свободна "
              f"в {until}, спим {int(wait)} c...")
        await asyncio.sleep(wait)

    @staticmethod
    def _post_json_with_requests(url, proxy_url, **kwargs):
        request_kwargs = dict(kwargs)
        headers = dict(request_kwargs.get('headers') or {})
        request_kwargs['headers'] = headers
        request_kwargs.pop('ssl', None)
        timeout = request_kwargs.pop('timeout', 120)
        proxies = {'http': proxy_url, 'https': proxy_url} if proxy_url else None
        response = cf_requests.post(
            url,
            proxies=proxies,
            timeout=timeout,
            impersonate='chrome',
            **request_kwargs,
        )
        try:
            return response.json(), response.headers
        except ValueError as exc:
            body = response.text[:300].replace('\n', ' ')
            raise RuntimeError(f"HTTP {response.status_code}: {body}") from exc
    
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
                    # Используем requests с HTTP прокси
                    data, headers = await asyncio.to_thread(
                        self._post_json_with_requests,
                        url,
                        tracker.proxy_url,
                        **kwargs,
                    )

                    # Проверяем на rate limit от API
                    if isinstance(data, dict) and data.get('message') == 'API rate limit exceeded':
                        scope = await tracker.apply_rate_limit_headers(headers)
                        if scope is None:
                            await tracker.mark_rate_limited(scope='unknown')

                        # Переключаемся на следующий tracker
                        self.current_index = (self.current_index + 1) % len(self.trackers)
                        retry_count += 1

                        # Проверяем, все ли пары заблокированы
                        if all(t.is_rate_limited for t in self.trackers):
                            await self._sleep_until_first_free()
                            retry_count = 0  # Сбрасываем счетчик после сна

                        continue  # Пробуем следующую пару

                    # Если все ок — пишем запрос в счётчики и сверяем остаток окна
                    # по заголовкам: остаток 0 при HTTP 200 блокирует пару заранее,
                    # не дожидаясь 429.
                    await tracker.record_request()
                    await tracker.apply_rate_limit_headers(headers)
                    return data

                except Exception as e:
                    proxy_short = tracker.proxy_url.split('@')[-1] if '@' in (tracker.proxy_url or '') else (tracker.proxy_url or 'direct')[:30]
                    print(f"❌ Ошибка запроса через прокси {proxy_short}: {e}")
                    
                    # При ошибке переключаемся на следующий tracker
                    old_index = self.current_index
                    self.current_index = (self.current_index + 1) % len(self.trackers)
                    next_tracker = self.trackers[self.current_index]
                    next_proxy_short = next_tracker.proxy_url.split('@')[-1] if '@' in (next_tracker.proxy_url or '') else (next_tracker.proxy_url or 'direct')[:30]
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
    """Lazy initialization для proxy_pool (Stratz: proxy → token, 1:1)"""
    global proxy_pool
    if proxy_pool is None:
        proxy_pool = ProxyAPIPool(STRATZ_PROXY_MAP)
    return proxy_pool


_STRATZ_SCHEMA_CACHE = None


def _load_stratz_schema():
    global _STRATZ_SCHEMA_CACHE
    if _STRATZ_SCHEMA_CACHE is not None:
        return _STRATZ_SCHEMA_CACHE
    schema_path = DOCS_DIR / "stratz_schema_match_types.json"
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
    
    # Собираем файлы для загрузки (не добавляем './' к абсолютным путям)
    base_dir = mkdir if os.path.isabs(mkdir) else f'./{mkdir}'
    files_to_load = [(key, os.path.join(base_dir, key)) for key, flag in kwargs.items() if flag]
    
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


def save_get_maps_state(maps_to_save, processed_ids, output_data, player_ids, all_teams, phase, saved_count=None):
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
    
    shown_count = len(output_data) if saved_count is None else int(saved_count)
    print(f"💾 Состояние сохранено: {len(processed_ids)} ID обработано, {shown_count} карт собрано")


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
                proxy_short = next_tracker.proxy_url.split('@')[-1] if '@' in (next_tracker.proxy_url or '') else (next_tracker.proxy_url or 'direct')[:30]
                print(f"🔄 Переключение на прокси #{pool.current_index + 1}: {proxy_short}")
                
                # Если перепробовали все прокси в цикле
                if proxies_tried_in_cycle >= total_proxies:
                    print(f"❌ Все {total_proxies} прокси были перепробованы, спим {sleep_time} секунд (5 минут)...")
                    await asyncio.sleep(sleep_time)
                    
                    # Блокировки от API здесь больше НЕ сбрасываем: их держит сам
                    # Stratz (retry-after), и принудительный сброс возвращал нас в
                    # тот же 429 на первом же запросе.
                    proxies_tried_in_cycle = 0  # Сбрасываем счетчик для нового цикла
                    print("⏰ Пробуждение! Продолжаем с первого прокси...")
                else:
                    # Небольшая задержка перед повторной попыткой
                    await asyncio.sleep(2)
            else:
                # Если нет информации о прокси, просто ждем
                await asyncio.sleep(5)
    
    raise Exception(f"Не удалось выполнить запрос после {max_retries} попыток")


async def get_maps_new(ids, mkdir,
                 show_prints=True, skip=0, count=0, pro=False,
                 skip_auxiliary_files=False, batch_size=5, batch_concurrency=1,
                 start_date_time=None):
    """
    Объединенная функция для сбора и обработки матчей.
    Включает логику фильтрации trash maps и сохранения во временные файлы.
    
    Args:
        skip_auxiliary_files: если True, пропускает создание trash_maps, player_ids, all_teams
                             (для live обновлений про матчей в cyberscore)
        batch_size: размер батча ID для одного запроса (ускоряет при больших выборках)
        batch_concurrency: сколько батчей выполнять параллельно
    """
    # Базовое имя для сохранения состояния
    maps_to_save = 'maps'
    
    # ОПТИМИЗАЦИЯ: Настраиваемый интервал чекпоинтов (было 100, стало 500)
    CHECKPOINT_INTERVAL = 500
    # Параметры батчей/параллелизма (для ускорения pub сбора)
    batch_size = max(1, int(batch_size))
    if batch_size > 5:
        batch_size = 5
    batch_concurrency = max(1, int(batch_concurrency))
    
    # Создаём основную папку если её нет
    if not os.path.exists(mkdir):
        os.makedirs(mkdir)
        print(f"📁 Создана папка: {mkdir}")
    
    # Предварительно формируем пути к файлам
    trash_maps_file = f'{mkdir}/trash_maps.txt'
    all_teams_file = f'{mkdir}/all_teams.txt'
    player_ids_file = f'{mkdir}/player_ids.txt'
    processed_graph_ids_file = f"{mkdir}/processed_ids_to_graph.txt"
    invalid_positions_file = f'{mkdir}/invalid_positions_matches.json'
    
    # Загрузка trash_maps (пропускаем для live обновлений)
    trash_maps = set()
    trash_reasons = Counter()
    processed_graph_ids = set()
    invalid_positions_matches = {}
    
    if not skip_auxiliary_files:
        if os.path.exists(processed_graph_ids_file):
            try:
                with open(processed_graph_ids_file, 'r') as f:
                    loaded_ids = json.load(f)
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
        print("⏭️  processed_ids_to_graph.txt отключен (skip_auxiliary_files=True)")

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

        if os.path.exists(invalid_positions_file):
            try:
                with open(invalid_positions_file, 'r', encoding='utf-8') as f:
                    loaded_invalid = json.load(f)
                if isinstance(loaded_invalid, dict):
                    invalid_positions_matches = loaded_invalid
            except Exception as e:
                print(f"⚠️ Ошибка при загрузке invalid_positions_matches: {e}")
                invalid_positions_matches = {}
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

    # Преобразуем все входные ID в int для консистентности
    ids_set = set(int(id) for id in ids)
    maps_counter = 0
    failed_batches = 0
    run_map_ids = set()
    for map_id in output_data.keys():
        try:
            run_map_ids.add(int(map_id))
        except Exception:
            continue
    allowed_team_ids = _build_tier_team_ids() if pro else None
    existing_match_ids = set()
    processed_ids_file = f"{mkdir}/json_parts_split_from_object/processed_ids.txt"
    if os.path.exists(processed_ids_file):
        try:
            with open(processed_ids_file, 'r', encoding='utf-8') as f:
                loaded_ids = json.load(f)
            existing_match_ids = set(int(mid) for mid in loaded_ids)
            print(f"📋 Загружено {len(existing_match_ids)} processed match IDs")
        except Exception as e:
            print(f"⚠️ Ошибка при загрузке processed match IDs: {e}")
    # Создаём папку temp_files если её нет
    temp_folder = f"{mkdir}/temp_files"
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
        print(f"📁 Создана папка: {temp_folder}")
    else:
        temp_seen = 0
        for temp_path in sorted(Path(temp_folder).glob("*.txt")):
            try:
                for raw_map_id in _iter_json_object_keys(temp_path):
                    map_id = _normalize_map_id(raw_map_id)
                    if map_id is not None:
                        run_map_ids.add(map_id)
                        temp_seen += 1
            except Exception as e:
                print(f"⚠️ Не удалось просканировать temp файл {temp_path}: {e}")
        if temp_seen:
            print(f"📋 Загружено {len(run_map_ids)} map_id из текущих temp_files")

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
        remaining_ids_list = list(remaining_ids)
        total_ids = len(remaining_ids_list)
        next_checkpoint = ((len(processed_ids) // CHECKPOINT_INTERVAL) + 1) * CHECKPOINT_INTERVAL

        for group_start in range(0, total_ids, batch_size * batch_concurrency):
            group_end = min(total_ids, group_start + batch_size * batch_concurrency)
            batch_group = []

            for i in range(group_start, group_end, batch_size):
                batch = remaining_ids_list[i:i + batch_size]
                if not batch:
                    continue
                batch_group.append(batch)

            # Получаем полные данные матчей с retry логикой (параллельно по батчам).
            # return_exceptions=True: упавший батч не роняет остальные и, главное,
            # не помечает своих игроков обработанными — иначе сбой прокси навсегда
            # выкидывал бы игрока из будущих прогонов с недокачанными матчами.
            tasks = [
                retry_request_with_proxy_rotation(
                    proceed_get_maps_with_data,
                    skip=skip,
                    ids_to_graph=batch,
                    player_ids_check=True,
                    pro=pro,
                    existing_match_ids=existing_match_ids,
                    start_date_time=start_date_time,
                )
                for batch in batch_group
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            ok_results = []
            for batch, result in zip(batch_group, results):
                if isinstance(result, BaseException):
                    failed_batches += 1
                    print(f"⚠️ Батч из {len(batch)} игроков не скачан ({type(result).__name__}: "
                          f"{result}); игроки остаются неопрошенными")
                    continue
                # Игрок считается обработанным ТОЛЬКО после успешного ответа.
                for check_id in batch:
                    count += 1
                    check_id_int = int(check_id)
                    processed_ids.add(check_id_int)
                    processed_graph_ids.add(check_id_int)
                    if show_prints:
                        print(f'{count}/{len(ids_set)}')
                ok_results.append(result)

            for matches, new_player_ids in ok_results:
                # Обрабатываем каждый матч
                for match in matches:
                    map_id = int(match['id'])

                    # Проверяем качество матча
                    is_valid, reason = check_match_quality(match)

                    if pro:
                        # Матчи БЕЗ лиги и матчи любительских лиг больше не
                        # выбрасываются: для обучения нужен весь доступный объём,
                        # а история игрока одинаково полезна из любого матча.
                        # Вернуть прежнее поведение — PRO_REQUIRE_LEAGUE=1.
                        league = match.get('league') or {}
                        league_id = match.get('leagueId') or league.get('id')
                        if PRO_REQUIRE_LEAGUE and league_id is None:
                            continue
                        if not league:
                            league = {}
                        if league.get('id') is None and league_id is not None:
                            league['id'] = int(league_id)
                        if not league.get('tier'):
                            league['tier'] = 'UNKNOWN'
                        match['league'] = league
                        if PRO_REQUIRE_LEAGUE and league.get('tier') == 'AMATEUR':
                            continue
                        if map_id not in run_map_ids:
                            output_data[str(map_id)] = match
                            run_map_ids.add(map_id)
                            maps_counter += 1
                    else:
                        # Для pub матчей - записываем только валидные
                        if is_valid:
                            if map_id not in run_map_ids:
                                output_data[str(map_id)] = match
                                run_map_ids.add(map_id)
                                maps_counter += 1
                    
                    if not is_valid:
                        trash_maps.add(map_id)
                        trash_reasons[reason] += 1
                        if (not skip_auxiliary_files) and reason.startswith('invalid positions'):
                            if str(map_id) not in invalid_positions_matches:
                                invalid_positions_matches[str(map_id)] = {
                                    'reason': reason,
                                    'match': match,
                                }

                player_ids.update(new_player_ids)

            # ОПТИМИЗАЦИЯ: Сохранение каждые CHECKPOINT_INTERVAL ID
            while len(processed_ids) >= next_checkpoint:
                saved_count = 0
                if len(output_data) > 0:
                    saved_count = len(output_data)
                    save_temp_file(output_data, mkdir, count)
                    output_data = {}

                with open(trash_maps_file, 'w') as f:
                    json.dump(list(trash_maps), f)

                if not skip_auxiliary_files:
                    with open(processed_graph_ids_file, 'w', encoding='utf-8') as f:
                        json.dump(list(processed_graph_ids), f)

                if not skip_auxiliary_files:
                    with open(invalid_positions_file, 'w', encoding='utf-8') as f:
                        json.dump(invalid_positions_matches, f)

                save_get_maps_state(maps_to_save, processed_ids, {}, player_ids, all_teams, phase=1, saved_count=saved_count)
                if trash_reasons:
                    top_reasons = ", ".join(f"{k}:{v}" for k, v in trash_reasons.most_common(5))
                    print(f"💾 Чекпоинт #{len(processed_ids)}: сохранено {saved_count}, trash {len(trash_maps)} (top: {top_reasons}), IDs {len(processed_graph_ids)}")
                else:
                    print(f"💾 Чекпоинт #{len(processed_ids)}: сохранено {saved_count}, trash {len(trash_maps)}, IDs {len(processed_graph_ids)}")
                next_checkpoint += CHECKPOINT_INTERVAL

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

        save_get_maps_state(
            maps_to_save,
            processed_ids,
            {},
            player_ids,
            all_teams,
            phase=1,
            saved_count=maps_counter,
        )

    # ОПТИМИЗАЦИЯ: Финальные сохранения с использованием переменных путей
    # Пропускаем для live обновлений
    if not skip_auxiliary_files:
        with open(player_ids_file, 'w') as f:
            json.dump(list(player_ids), f)

        with open(all_teams_file, 'w') as f:
            json.dump(all_teams, f)

        with open(trash_maps_file, 'w') as f:
            json.dump(list(trash_maps), f)

        with open(invalid_positions_file, 'w', encoding='utf-8') as f:
            json.dump(invalid_positions_matches, f)

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
    merged_files = merge_temp_files_by_patch(
        mkdir=mkdir,
        max_size_mb=500,
        cleanup=False  # Удаляем temp_files после объединения
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



# Матчи вне известных границ патчей. Спецификация начинается с 2025-05-21, а
# исторический про-сбор уходит на годы назад: при прежнем поведении две трети
# скачанного молча выбрасывалось (волна 1 от 11.08: 59 649 матчей из 87 293).
# Для истории игроков и ростеров патч не важен вовсе — важен сам факт матча,
# поэтому такие карты складываются в отдельный бакет, а не удаляются.
# Вернуть прежнее поведение — MERGE_DROP_OUTSIDE_PATCH=1.
OUTSIDE_PATCH_BUCKET = os.getenv("OUTSIDE_PATCH_BUCKET", "historical")
MERGE_DROP_OUTSIDE_PATCH = os.getenv("MERGE_DROP_OUTSIDE_PATCH", "0") == "1"


PRO_REQUIRE_LEAGUE = os.getenv("PRO_REQUIRE_LEAGUE", "0") == "1"


def _start_date_for(pro: bool) -> int:
    """Нижняя граница даты сбора: у паблика и у про она СВОЯ.

    Раньше про-ветка молча перезаписывала `start_date_time` значением
    `start_date_time_736`, поэтому правка паблик-константы на про не влияла
    вовсе — и наоборот. Теперь имена разведены:

        start_date_time_pro      — про-сбор (фолбэк: start_date_time_736)
        start_date_time_publick  — паблик-сбор (фолбэк: start_date_time)

    Фолбэки оставлены, чтобы старый `keys.py` на другой машине не ломал сбор.
    """
    import keys as _keys
    names = (("start_date_time_pro", "start_date_time_736")
             if pro else ("start_date_time_publick", "start_date_time"))
    for name in names:
        value = getattr(_keys, name, None)
        if value is not None:
            return int(value)
    raise RuntimeError(f"в keys.py нет ни одной из констант даты: {names}")


async def proceed_get_maps_with_data(skip=0, only_in_ids=False, ids_to_graph=None,
                                    game_mods=None, all_teams=None, player_ids_check=False,
                                    pro=False, existing_match_ids=None,
                                    start_date_time=None):
    """
    Получает полные данные матчей вместо только ID.
    Возвращает: (matches, player_ids) - список матчей и множество ID игроков

    """
    # Замер 05.08 на 25 игроках, одна страница (runtime/artifacts/misc/
    # stratz_isstats_bracket_probe.md): `isStats: false` отдаёт 0 матчей, то есть
    # флаг не «парсенность», а взаимоисключающий фильтр. Без него страница даёт
    # +6% матчей при том же качестве полей (позиции 89%, networth>=20м 80% против
    # 81%), пригодны 81% добавленных. Брекет 7 (Divine) добавляет +68% матчей,
    # пригодны 85%. Вместе: 1317 -> 2276 матчей, 88% пригодных.
    if start_date_time is None:
        start_date_time = _start_date_for(pro=False)
    # Клиентский порог даты: серверный startDateTime у Stratz таймаутит на свежих
    # датах (NPGSQL TIMEOUT), поэтому в pub-ветке матчи фильтруем по дате на стороне
    # клиента (Stratz отдаёт матчи newest-first), а не через серверный startDateTime.
    threshold = int(start_date_time)
    matches = []
    player_ids = set()
    check = True
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
             {{take: 100,
              skip: {skip},
               gameModeIds: [22],
                 bracketIds: [7, 8]}}) {{
              id
              startDateTime
              durationSeconds
              didRadiantWin
              bottomLaneOutcome
              topLaneOutcome
              midLaneOutcome
              winRates
              radiantNetworthLeads
              radiantExperienceLeads
              radiantKills
              direKills
              towerDeaths {{
                        time
                        isRadiant
                        npcId
                      }}
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
                heroId
                deaths
                imp
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
            start_date_time = _start_date_for(pro=True)
            query = f'''
                query {{
                  teams(teamIds: {ids_to_graph}) {{
                    matches(request: {{startDateTime: {start_date_time}, take: 100, skip: {skip}, isStats:true}}) {{
                      id
                      didRadiantWin
                      towerDeaths {{
                        time
                        isRadiant
                        npcId
                      }}
                      bottomLaneOutcome
                      topLaneOutcome
                      midLaneOutcome
                      winRates
                      firstBloodTime
                      averageImp
                      regionId
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
                      series {{
                        id
                        type
                      }}
                      direKills
                      radiantKills
                      radiantNetworthLeads
                      radiantExperienceLeads
                      
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
                # Серверный startDateTime убран (таймаутит на свежих датах) → фильтр по
                # дате на клиенте. Stratz отдаёт матчи newest-first (desc): собираем
                # матчи >= threshold и пагинируем игрока, пока на странице есть свежие.
                need_more = False
                for player in players_data or []:
                    page_matches = player.get('matches') or []
                    kept_in_window = 0
                    in_window = 0
                    for match in page_matches:
                        sdt = match.get('startDateTime')
                        if sdt is None or int(sdt) < threshold:
                            continue
                        # Матч в окне независимо от того, есть он у нас уже или нет:
                        # пагинацию двигает именно это. Считать только НОВЫЕ значило
                        # вставать на первой полностью известной странице, а дыра под
                        # ней (оборванный прошлый прогон) не заполнялась бы никогда.
                        in_window += 1
                        match_id = match.get('id')
                        if match_id is not None:
                            match_id_int = int(match_id)
                            if existing_match_ids is not None and match_id_int in existing_match_ids:
                                continue
                            if existing_match_ids is not None:
                                existing_match_ids.add(match_id_int)
                        kept_in_window += 1
                        matches.append(match)
                        sa = player.get('steamAccount') or {}
                        if not sa:
                            continue
                        if sa.get('smurfFlag') not in [0, 2] or sa.get('isAnonymous'):
                            continue
                        if player_ids_check:
                            for extra_player in match.get('players', []):
                                steam_account = extra_player.get('steamAccount')
                                if (not extra_player.get('intentionalFeeding') and
                                    steam_account and
                                    steam_account.get('smurfFlag') in [0, 2] and
                                    not steam_account.get('isAnonymous')):
                                    player_ids.add(int(steam_account['id']))
                    # Пагинируем дальше, если страница полная (==100) И на ней есть хотя бы
                    # один матч в окне (>= threshold). НЕ опираемся на page-min: null или
                    # неупорядоченная дата в середине страницы ложно оборвала бы пагинацию
                    # и потеряла бы свежие матчи дальше. Лишняя страница за границей дёшева;
                    # skip<100000 — backstop от рантэвея, если порядок Stratz окажется не desc.
                    if len(page_matches) == 100 and in_window > 0:
                        need_more = True
                if need_more and skip < 100000:
                    skip += 100
                else:
                    check = False


        except Exception as e:
            print(f"Unexpected error in proceed_get_maps_with_data: {e}")
            # Пробрасываем ошибку дальше для retry логики
            raise

    return matches, player_ids
async def proceed_get_maps(skip, only_in_ids, output_data,
                           player_ids=None, all_teams=None, ids_to_graph=None, game_mods=None,
                     check=True, player_ids_check=False, pro=False):
    while check:
        if pro:
            query = f'''
            {{
              teams(teamIds: {ids_to_graph}) {{
                matches(request: {{startDateTime: {start_date_time_736}, take: 100, skip: {skip}, isStats:true}}) {{
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
                 {{startDateTime: {start_date_time_739},
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
    return merge_temp_files_by_patch(
        mkdir=mkdir,
        max_size_mb=max_size_mb,
        output_dir=output_dir,
        cleanup=cleanup,
    )

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

    def _normalize_match_id(value):
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                try:
                    return int(value)
                except Exception:
                    return value
            return value
        try:
            return int(value)
        except Exception:
            return value

    def _iter_ids_from_existing_file(file_path):
        # Стриминг, чтобы не держать файл целиком в памяти
        if ijson is not None:
            try:
                with open(file_path, 'rb') as f:
                    first_non_ws = None
                    for chunk in iter(lambda: f.read(64), b''):
                        for b in chunk:
                            if b in b' \r\n\t':
                                continue
                            first_non_ws = b
                            break
                        if first_non_ws is not None:
                            break
                    f.seek(0)

                    if first_non_ws == ord('{'):
                        for prefix, event, value in ijson.parse(f):
                            if prefix == '' and event == 'map_key':
                                yield value
                        return
                    if first_non_ws == ord('['):
                        for prefix, event, value in ijson.parse(f):
                            if event in ('string', 'number') and (
                                prefix == 'item' or prefix == 'item.match_id' or prefix == 'item.id'
                            ):
                                yield value
                        return
            except Exception:
                pass

        # Фолбэк без стриминга (может быть тяжелым по памяти)
        try:
            with open(file_path, 'rb') as f:
                data = orjson.loads(f.read())
        except Exception:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

        if isinstance(data, dict):
            for k in data.keys():
                yield k
        elif isinstance(data, list):
            for v in data:
                if isinstance(v, dict):
                    yield v.get('match_id') or v.get('id')
                else:
                    yield v

    # Находим существующие combined*.json и собираем из них ID
    existing_files = [
        entry.name
        for entry in os.scandir(output_dir)
        if entry.is_file() and entry.name.startswith('combined') and entry.name.endswith('.json')
    ]

    processed_ids = set()
    if existing_files:
        existing_files_sorted = sorted(existing_files)
        print(f"🔍 Сканируем {len(existing_files_sorted)} файлов в {output_dir} для сбора ID...")
        for i, filename in enumerate(existing_files_sorted, 1):
            file_path = os.path.join(output_dir, filename)
            try:
                for match_id in _iter_ids_from_existing_file(file_path):
                    match_id_norm = _normalize_match_id(match_id)
                    if match_id_norm is not None:
                        processed_ids.add(match_id_norm)
            except Exception as e:
                print(f"  ⚠️ Ошибка при сканировании {file_path}: {e}")
            if i % 10 == 0 or i == len(existing_files_sorted):
                print(f"  📥 Сканировано {i}/{len(existing_files_sorted)} файлов | Уникальных ID: {len(processed_ids)}")
    else:
        print(f"📋 В {output_dir} нет combined*.json, начинаем с нуля")

    # Находим максимальный номер файла combined{N}.json
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

    def _load_temp_file_dict_with_recovery(file_path):
        """
        Возвращает (data_dict, recovered_partially, error_message).
        - data_dict: dict с матчами или None, если файл не удалось прочитать даже частично
        - recovered_partially: True, если использован recovery-парсинг (например, обрезанный JSON)
        - error_message: текст исходной ошибки для диагностики
        """
        from decimal import Decimal

        def _sanitize_recovered_value(value):
            # ijson может вернуть Decimal — приводим к JSON-совместимым типам
            if isinstance(value, Decimal):
                if value == value.to_integral_value():
                    try:
                        return int(value)
                    except Exception:
                        return float(value)
                return float(value)
            if isinstance(value, list):
                return [_sanitize_recovered_value(v) for v in value]
            if isinstance(value, dict):
                return {k: _sanitize_recovered_value(v) for k, v in value.items()}
            return value

        primary_error = None

        # 1) Быстрый путь: orjson (строгий парсер)
        try:
            with open(file_path, 'rb') as f:
                data = orjson.loads(f.read())
            if isinstance(data, dict):
                return data, False, None
            return None, False, f"root_is_{type(data).__name__}"
        except Exception as e:
            primary_error = str(e)

        # 2) Фолбэк: стандартный json.load (иногда устойчивее к краевым кейсам кодировки)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data, False, None
            return None, False, f"root_is_{type(data).__name__}"
        except Exception as e:
            if not primary_error:
                primary_error = str(e)

        # 3) Recovery: потоково читаем пары key/value до места обрыва JSON
        # Это позволяет спасти валидную часть файла при premature EOF.
        if ijson is not None:
            recovered = {}
            stream_error = None
            try:
                with open(file_path, 'rb') as f:
                    for key, value in ijson.kvitems(f, '', use_float=True):
                        recovered[str(key)] = _sanitize_recovered_value(value)
            except Exception as e:
                stream_error = str(e)
            if recovered:
                err_msg = primary_error or stream_error or "partial_recovery_without_explicit_error"
                return recovered, True, err_msg

        return None, False, primary_error or "unknown_parse_error"

    max_size_bytes = max_size_mb * 1024 * 1024
    current_data = {}
    current_size = 0
    output_files = []
    total_matches = 0
    duplicates_count = 0
    invalid_id_count = 0
    non_dict_files_count = 0
    broken_files_skipped = 0
    recovered_files_count = 0
    recovered_records_count = 0
    new_ids_in_session = set()  # ID добавленные в текущей сессии

    for i, filename in enumerate(temp_files, 1):
        file_path = os.path.join(temp_folder, filename)
        data, recovered_partially, parse_err = _load_temp_file_dict_with_recovery(file_path)
        if data is None:
            broken_files_skipped += 1
            print(f"  ⚠️ Ошибка при чтении {file_path}: {parse_err}")
            continue

        if not isinstance(data, dict):
            non_dict_files_count += 1
            continue

        if recovered_partially:
            recovered_files_count += 1
            recovered_records_count += len(data)
            print(
                f"  🩹 Частичное восстановление {file_path}: "
                f"{len(data)} записей (исходная ошибка: {parse_err})"
            )

        # Фильтруем дубликаты - добавляем только новые match_id
        new_entries = {}
        for match_id, match_data in data.items():
            match_id_norm = _normalize_match_id(match_id)
            if match_id_norm is None:
                invalid_id_count += 1
                continue

            # Проверяем и против старых processed_ids, и против новых в текущей сессии
            if match_id_norm not in processed_ids and match_id_norm not in new_ids_in_session:
                new_entries[match_id] = match_data
                new_ids_in_session.add(match_id_norm)
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
    if invalid_id_count:
        print(f"🆔 Пропущено невалидных match_id: {invalid_id_count}")
    if recovered_files_count:
        print(f"🩹 Частично восстановлено файлов: {recovered_files_count} (записей: {recovered_records_count})")
    if broken_files_skipped:
        print(f"⚠️ Полностью пропущено битых файлов: {broken_files_skipped}")
    if non_dict_files_count:
        print(f"⚠️ Пропущено non-dict файлов: {non_dict_files_count}")
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


def merge_temp_files_by_patch(
    mkdir,
    max_size_mb=500,
    output_dir=None,
    cleanup=False,
    clear_output_dir=False,
    patch_specs=None,
):
    """
    Объединяет temp_files в patch-part файлы с глобальной дедупликацией по normalized match_id.

    Пишет файлы вида:
      - 7.40_part001.json
      - 7.41_part001.json

    Фильтрация по startDateTime:
      - 7.40: 2025-12-15 <= ts < 2026-03-24
      - 7.41: 2026-03-24 <= ts
    """
    return merge_temp_files_by_patch_streaming(
        mkdir=mkdir,
        max_size_mb=max_size_mb,
        output_dir=output_dir,
        cleanup=cleanup,
        clear_output_dir=clear_output_dir,
        patch_specs=patch_specs,
    )

    from decimal import Decimal

    temp_folder = f"{mkdir}/temp_files"
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
        print(f"📁 Создана папка: {temp_folder} (была пустая, нечего объединять)")
        return []

    if output_dir is None:
        output_dir = f"{mkdir}/json_parts_split_from_object"
    os.makedirs(output_dir, exist_ok=True)

    if patch_specs is None:
        patch_specs = DOTA_PATCH_SPECS

    def _normalize_match_id(value):
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            s = value.strip()
            if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
                try:
                    return int(s)
                except Exception:
                    return None
            return None
        try:
            return int(value)
        except Exception:
            return None

    def _sanitize_recovered_value(value):
        if isinstance(value, Decimal):
            if value == value.to_integral_value():
                try:
                    return int(value)
                except Exception:
                    return float(value)
            return float(value)
        if isinstance(value, list):
            return [_sanitize_recovered_value(v) for v in value]
        if isinstance(value, dict):
            return {k: _sanitize_recovered_value(v) for k, v in value.items()}
        return value

    def _load_temp_file_dict_with_recovery(file_path):
        primary_error = None
        try:
            with open(file_path, 'rb') as f:
                data = orjson.loads(f.read())
            if isinstance(data, dict):
                return data, False, None
            return None, False, f"root_is_{type(data).__name__}"
        except Exception as e:
            primary_error = str(e)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data, False, None
            return None, False, f"root_is_{type(data).__name__}"
        except Exception as e:
            if not primary_error:
                primary_error = str(e)

        if ijson is not None:
            recovered = {}
            stream_error = None
            try:
                with open(file_path, 'rb') as f:
                    for key, value in ijson.kvitems(f, '', use_float=True):
                        recovered[str(key)] = _sanitize_recovered_value(value)
            except Exception as e:
                stream_error = str(e)
            if recovered:
                return recovered, True, primary_error or stream_error or "partial_recovery"

        return None, False, primary_error or "unknown_parse_error"

    def _resolve_patch_name(start_ts):
        try:
            ts = int(start_ts)
        except Exception:
            return None
        for patch_name, start_ts_inclusive, end_ts_exclusive in patch_specs:
            if ts < int(start_ts_inclusive):
                continue
            if end_ts_exclusive is not None and ts >= int(end_ts_exclusive):
                continue
            return str(patch_name)
        return None

    def _patch_sort_key(name):
        try:
            return tuple(int(part) for part in str(name).split("."))
        except Exception:
            return (9999,)

    def _flush_patch_state(patch_name, state, output_files):
        if not state["current_data"]:
            return
        filename = f"{patch_name}_part{state['part_number']:03d}.json"
        output_path = os.path.join(output_dir, filename)
        payload = orjson.dumps(state["current_data"])
        with open(output_path, 'wb') as f:
            f.write(payload)
        file_size_mb = len(payload) / (1024 * 1024)
        print(f"  ✅ {output_path}: {len(state['current_data'])} матчей ({file_size_mb:.1f} МБ)")
        output_files.append(output_path)
        state["written_matches"] += len(state["current_data"])
        state["current_data"] = {}
        state["current_size"] = 0
        state["part_number"] += 1

    existing_json_files = list(Path(output_dir).glob("*.json"))
    non_summary_json_files = [p for p in existing_json_files if p.name != "processed_ids.txt"]
    if clear_output_dir and non_summary_json_files:
        backup_dir = Path(output_dir).parent / f"{Path(output_dir).name}__backup_before_patch_merge_{int(time.time())}"
        backup_dir.mkdir(parents=True, exist_ok=True)
        for path in non_summary_json_files:
            shutil.move(str(path), str(backup_dir / path.name))
        processed_ids_candidate = Path(output_dir) / "processed_ids.txt"
        if processed_ids_candidate.exists():
            shutil.move(str(processed_ids_candidate), str(backup_dir / processed_ids_candidate.name))
        print(f"🗄️ Старые JSON перенесены в backup: {backup_dir}")

    processed_ids = set()
    processed_ids_file = os.path.join(output_dir, "processed_ids.txt")
    if os.path.exists(processed_ids_file):
        try:
            with open(processed_ids_file, 'rb') as f:
                existing_ids = orjson.loads(f.read())
            if isinstance(existing_ids, list):
                for value in existing_ids:
                    normalized = _normalize_match_id(value)
                    if normalized is not None:
                        processed_ids.add(normalized)
        except Exception as e:
            print(f"⚠️ Не удалось прочитать processed_ids.txt, начинаю с пустого набора: {e}")

    # Источник правды — сами part-файлы, а не processed_ids.txt. Раньше дедуп
    # опирался только на сайдкар: стоило ему отстать, потеряться или уехать в
    # backup (clear_output_dir), и уже записанные матчи писались повторно.
    # Именно так корпус набрал 466 708 дублей = 23.9% записей при
    # «Дубликатов=0» в отчёте merge.
    from_parts = _scan_output_dir_match_ids(output_dir)
    if from_parts:
        missed = len(from_parts - processed_ids)
        processed_ids |= from_parts
        print(f"🧾 Сверка с part-файлами: {len(from_parts)} match_id на диске, "
              f"из них не было в processed_ids.txt: {missed}")

    patch_names_all = [str(p[0]) for p in patch_specs]
    if not MERGE_DROP_OUTSIDE_PATCH and OUTSIDE_PATCH_BUCKET not in patch_names_all:
        patch_names_all.append(OUTSIDE_PATCH_BUCKET)   # бакет «вне патчей»
    patch_part_numbers = {}
    for patch_name in patch_names_all:
        existing_parts = sorted(Path(output_dir).glob(f"{patch_name}_part*.json"))
        max_part = 0
        for path in existing_parts:
            m = re.match(rf"^{re.escape(str(patch_name))}_part(\d+)\.json$", path.name)
            if not m:
                continue
            try:
                max_part = max(max_part, int(m.group(1)))
            except Exception:
                continue
        patch_part_numbers[str(patch_name)] = max_part + 1

    max_size_bytes = max_size_mb * 1024 * 1024
    patch_states = {
        str(patch_name): {
            "current_data": {},
            "current_size": 0,
            "part_number": patch_part_numbers[str(patch_name)],
            "written_matches": 0,
        }
        for patch_name in patch_names_all
    }
    output_files = []
    session_ids = set()
    duplicates_count = 0
    invalid_id_count = 0
    skipped_outside_patch = 0
    broken_files_skipped = 0
    recovered_files_count = 0
    recovered_records_count = 0

    temp_files = sorted([f for f in os.listdir(temp_folder) if f.endswith('.txt')])
    if not temp_files:
        print(f"⚠️ В папке {temp_folder} нет .txt файлов")
        return []

    print(f"📊 Найдено {len(temp_files)} временных файлов")
    print(
        "🎯 Объединяем с фильтрацией по патчам "
        f"{', '.join(name for name, *_ in sorted(patch_specs, key=lambda x: _patch_sort_key(x[0])))} "
        f"и дедупликацией по match_id; лимит {max_size_mb} МБ"
    )

    for index, filename in enumerate(temp_files, 1):
        file_path = os.path.join(temp_folder, filename)
        data, recovered_partially, parse_err = _load_temp_file_dict_with_recovery(file_path)
        if data is None:
            broken_files_skipped += 1
            print(f"  ⚠️ Ошибка при чтении {file_path}: {parse_err}")
            continue
        if recovered_partially:
            recovered_files_count += 1
            recovered_records_count += len(data)
            print(
                f"  🩹 Частичное восстановление {file_path}: "
                f"{len(data)} записей (исходная ошибка: {parse_err})"
            )

        for raw_match_id, raw_match_data in data.items():
            match_id_norm = _normalize_match_id(raw_match_id)
            if match_id_norm is None:
                match_id_norm = _normalize_match_id((raw_match_data or {}).get("id") if isinstance(raw_match_data, dict) else None)
            if match_id_norm is None:
                invalid_id_count += 1
                continue

            if match_id_norm in processed_ids or match_id_norm in session_ids:
                duplicates_count += 1
                continue

            if not isinstance(raw_match_data, dict):
                continue

            patch_name = _resolve_patch_name(raw_match_data.get("startDateTime"))
            if patch_name is None:
                skipped_outside_patch += 1
                if MERGE_DROP_OUTSIDE_PATCH:
                    continue
                patch_name = OUTSIDE_PATCH_BUCKET

            match_data = dict(raw_match_data)
            match_data["id"] = int(match_id_norm)
            canonical_key = str(match_id_norm)
            estimated_size = len(canonical_key.encode("utf-8")) + len(orjson.dumps(match_data)) + 6

            state = patch_states[patch_name]
            if state["current_data"] and state["current_size"] + estimated_size > max_size_bytes:
                _flush_patch_state(patch_name, state, output_files)

            state["current_data"][canonical_key] = match_data
            state["current_size"] += estimated_size
            session_ids.add(match_id_norm)

        if index % 25 == 0 or index == len(temp_files):
            patch_progress = ", ".join(
                f"{patch}={len(state['current_data']) + state['written_matches']}"
                for patch, state in sorted(patch_states.items(), key=lambda item: _patch_sort_key(item[0]))
            )
            print(
                f"  📈 Обработано {index}/{len(temp_files)} файлов | "
                f"Уникальных={len(session_ids)} | Дубликатов={duplicates_count} | {patch_progress}"
            )

    for patch_name, state in sorted(patch_states.items(), key=lambda item: _patch_sort_key(item[0])):
        _flush_patch_state(patch_name, state, output_files)

    processed_ids.update(session_ids)
    with open(processed_ids_file, 'wb') as f:
        f.write(orjson.dumps(sorted(processed_ids)))

    summary = {
        "patches": {
            patch: {
                "matches": state["written_matches"],
                "next_part_number": state["part_number"],
            }
            for patch, state in sorted(patch_states.items(), key=lambda item: _patch_sort_key(item[0]))
        },
        "unique_matches_added": len(session_ids),
        "duplicates_filtered": duplicates_count,
        "invalid_ids_skipped": invalid_id_count,
        "outside_patch_skipped": skipped_outside_patch,
        "broken_files_skipped": broken_files_skipped,
        "recovered_files_count": recovered_files_count,
        "recovered_records_count": recovered_records_count,
    }
    summary_path = os.path.join(output_dir, "merge_patch_summary.json")
    with open(summary_path, "wb") as f:
        f.write(orjson.dumps(summary, option=orjson.OPT_INDENT_2))

    print("\n🎉 Patch merge завершён!")
    for patch, state in sorted(patch_states.items(), key=lambda item: _patch_sort_key(item[0])):
        print(f"   {patch}: {state['written_matches']} матчей")
    print(f"🔄 Дубликатов отфильтровано: {duplicates_count}")
    print(f"🆔 Невалидных ID пропущено: {invalid_id_count}")
    print(f"🗓️ Вне 7.40/7.41 пропущено: {skipped_outside_patch}")
    if recovered_files_count:
        print(f"🩹 Частично восстановлено файлов: {recovered_files_count} (записей: {recovered_records_count})")
    if broken_files_skipped:
        print(f"⚠️ Полностью пропущено битых файлов: {broken_files_skipped}")
    print(f"📄 summary: {summary_path}")

    if cleanup:
        try:
            shutil.rmtree(temp_folder)
            print(f"🗑️ Папка {temp_folder} удалена")
        except Exception as e:
            print(f"⚠️ Не удалось удалить {temp_folder}: {e}")

    return output_files

def _scan_manifest_path(output_dir) -> Path:
    return Path(output_dir) / "scan_manifest.json"


def _load_scan_manifest(output_dir):
    """{имя файла: [размер, mtime, [id...]]} — что уже сканировали и в каком виде."""
    path = _scan_manifest_path(output_dir)
    if not path.exists():
        return {}
    try:
        with open(path, "rb") as fh:
            data = orjson.loads(fh.read())
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"⚠️ Не читается {path.name}: {e} — пересканирую всё заново")
        return {}


def _save_scan_manifest(output_dir, manifest):
    path = _scan_manifest_path(output_dir)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "wb") as fh:
        fh.write(orjson.dumps(manifest))
    tmp.replace(path)


def _file_stamp(path):
    st = os.stat(path)
    return [int(st.st_size), int(st.st_mtime)]


def merge_temp_files_by_patch_streaming(
    mkdir,
    max_size_mb=500,
    output_dir=None,
    cleanup=False,
    clear_output_dir=False,
    patch_specs=None,
):
    """
    Быстро объединяет temp_files в patch-part JSON без дублей.

    Записи пишутся сразу в compact JSON, поэтому нет 500MB dict-буфера
    и повторной сериализации целого файла.
    """
    temp_folder = Path(mkdir) / "temp_files"
    if not temp_folder.exists():
        temp_folder.mkdir(parents=True, exist_ok=True)
        print(f"📁 Создана папка: {temp_folder} (была пустая, нечего объединять)")
        return []

    if output_dir is None:
        output_dir = Path(mkdir) / "json_parts_split_from_object"
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if patch_specs is None:
        patch_specs = DOTA_PATCH_SPECS

    if clear_output_dir and any(output_dir.iterdir()):
        backup_dir = output_dir.parent / f"{output_dir.name}__backup_before_stream_merge_{int(time.time())}"
        backup_dir.mkdir(parents=True, exist_ok=True)
        for path in list(output_dir.iterdir()):
            shutil.move(str(path), str(backup_dir / path.name))
        print(f"🗄️ Старый output перенесён в backup: {backup_dir}")

    def _normalize_match_id(value):
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            s = value.strip()
            if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
                try:
                    return int(s)
                except Exception:
                    return None
            return None
        try:
            return int(value)
        except Exception:
            return None

    def _resolve_patch_name(start_ts):
        try:
            ts = int(start_ts)
        except Exception:
            return None
        for patch_name, start_ts_inclusive, end_ts_exclusive in patch_specs:
            if ts < int(start_ts_inclusive):
                continue
            if end_ts_exclusive is not None and ts >= int(end_ts_exclusive):
                continue
            return str(patch_name)
        return None

    def _patch_sort_key(name):
        try:
            return tuple(int(part) for part in str(name).split("."))
        except Exception:
            return (9999,)

    def _load_temp_file(file_path):
        with open(file_path, "rb") as f:
            data = orjson.loads(f.read())
        if not isinstance(data, dict):
            raise ValueError(f"root_is_{type(data).__name__}")
        return data

    max_size_bytes = int(max_size_mb * 1024 * 1024)
    output_files = []
    processed_ids = set()
    processed_ids_file = output_dir / "processed_ids.txt"
    if processed_ids_file.exists():
        try:
            existing_ids = orjson.loads(processed_ids_file.read_bytes())
            if isinstance(existing_ids, list):
                for value in existing_ids:
                    normalized = _normalize_match_id(value)
                    if normalized is not None:
                        processed_ids.add(normalized)
        except Exception as e:
            print(f"⚠️ Не удалось прочитать processed_ids.txt, начинаю с пустого набора: {e}")

    # Пересборка дедуп-набора из уже лежащих patch-part файлов.
    # processed_ids.txt — только КЭШ; если он отсутствует, побился или отстал от
    # набора файлов (например патч-файлы дописали, а .txt не обновили), то без
    # пересборки merge зальёт уже собранные матчи повторно. Именно так 29.07.2026
    # прогон записал 308127 дублей при duplicates_filtered=0.
    # Источник правды — сами <patch>_part*.json, поэтому сверяемся с ними.
    existing_part_files = sorted(
        path
        for patch_name, *_ in patch_specs
        for path in output_dir.glob(f"{patch_name}_part*.json")
        if path.is_file()
    )
    if existing_part_files:
        # Раньше файл пропускался, если его ПЕРВЫЕ 200 ключей уже были в сайдкаре.
        # Проба — выборка, а не доказательство: у файла с учтённым началом и
        # неучтённым хвостом хвост молча выпадал из дедуп-набора, и его матчи
        # писались повторно. Так 31.07.2026 прошло 12 дублей уже ПОСЛЕ фикса
        # сайдкара. Сканируем ключи целиком — читаются только ключи, значения
        # не разбираются.
        # Полный скан корректен, но линеен по числу файлов: на 147 частях он
        # стоит минуты В КАЖДОМ слиянии, а слияние идёт после каждой волны сбора.
        # Кеш держит id уже просканированных файлов и переснимает только те, у
        # которых изменились размер или mtime. Корректность та же: изменившийся
        # или новый файл читается целиком.
        manifest = _load_scan_manifest(output_dir)
        fresh = {}
        rescanned = 0
        for i, path in enumerate(existing_part_files, 1):
            name = path.name
            try:
                stamp = _file_stamp(path)
            except OSError:
                stamp = None
            cached = manifest.get(name)
            if stamp is not None and cached and cached[:2] == stamp:
                processed_ids.update(int(x) for x in cached[2])
                fresh[name] = cached
                continue
            ids = []
            try:
                for raw_key in _iter_json_object_keys(path):
                    normalized = _normalize_match_id(raw_key)
                    if normalized is not None:
                        ids.append(int(normalized))
            except Exception as e:
                # Битый/недописанный part нельзя молча проглатывать: без его id
                # merge продублирует его матчи. Сообщаем явно и в кеш не пишем.
                print(f"  ⚠️ Не удалось просканировать {name}: {e} — его матчи могут продублироваться")
                continue
            processed_ids.update(ids)
            if stamp is not None:
                fresh[name] = [stamp[0], stamp[1], ids]
            rescanned += 1
            print(f"  📥 {i}/{len(existing_part_files)} {name}: {len(ids)} id "
                  f"(всего {len(processed_ids)})")
        print(f"🔁 дедуп-набор: {len(processed_ids)} id, пересканировано файлов "
              f"{rescanned} из {len(existing_part_files)} (остальные из кеша)")
        _save_scan_manifest(output_dir, fresh)

    existing_processed_ids_count = len(processed_ids)
    patch_names_all = [str(p[0]) for p in patch_specs]
    if not MERGE_DROP_OUTSIDE_PATCH and OUTSIDE_PATCH_BUCKET not in patch_names_all:
        patch_names_all.append(OUTSIDE_PATCH_BUCKET)   # бакет «вне патчей»
    patch_part_numbers = {}
    for patch_name in patch_names_all:
        max_part = 0
        for path in output_dir.glob(f"{patch_name}_part*.json"):
            match = re.match(rf"^{re.escape(str(patch_name))}_part(\d+)\.json$", path.name)
            if match:
                max_part = max(max_part, int(match.group(1)))
        patch_part_numbers[str(patch_name)] = max_part + 1
    duplicates_count = 0
    invalid_id_count = 0
    skipped_outside_patch = 0
    broken_files_skipped = 0

    states = {
        str(patch_name): {
            "fh": None,
            "path": None,
            "part_number": patch_part_numbers[str(patch_name)],
            "current_size": 0,
            "current_matches": 0,
            "written_matches": 0,
            "written_files": 0,
        }
        for patch_name in patch_names_all
    }

    def _open_part(patch_name):
        state = states[patch_name]
        filename = f"{patch_name}_part{state['part_number']:03d}.json"
        path = output_dir / filename
        fh = open(path, "wb")
        fh.write(b"{")
        state["fh"] = fh
        state["path"] = path
        state["current_size"] = 1
        state["current_matches"] = 0

    def _close_part(patch_name):
        state = states[patch_name]
        fh = state["fh"]
        if fh is None:
            return
        fh.write(b"}")
        fh.close()
        path = state["path"]
        file_size = path.stat().st_size
        if state["current_matches"] == 0:
            path.unlink(missing_ok=True)
        else:
            output_files.append(str(path))
            state["written_files"] += 1
            state["written_matches"] += state["current_matches"]
            print(
                f"  ✅ {path}: {state['current_matches']} матчей "
                f"({file_size / (1024 * 1024):.1f} МБ)"
            )
        state["fh"] = None
        state["path"] = None
        state["current_size"] = 0
        state["current_matches"] = 0
        state["part_number"] += 1

    def _write_entry(patch_name, canonical_key, match_data):
        state = states[patch_name]
        key_bytes = orjson.dumps(canonical_key)
        value_bytes = orjson.dumps(match_data)
        entry_size = len(key_bytes) + 1 + len(value_bytes)
        comma_size = 1 if state["current_matches"] else 0

        if state["fh"] is None:
            _open_part(patch_name)
            state = states[patch_name]
            comma_size = 0

        projected_size = state["current_size"] + comma_size + entry_size + 1
        if state["current_matches"] and projected_size > max_size_bytes:
            _close_part(patch_name)
            _open_part(patch_name)
            state = states[patch_name]
            comma_size = 0

        if comma_size:
            state["fh"].write(b",")
            state["current_size"] += 1
        state["fh"].write(key_bytes)
        state["fh"].write(b":")
        state["fh"].write(value_bytes)
        state["current_size"] += entry_size
        state["current_matches"] += 1

    temp_files = sorted(temp_folder.glob("*.txt"))
    if not temp_files:
        print(f"⚠️ В папке {temp_folder} нет .txt файлов")
        return []

    print(f"📊 Найдено {len(temp_files)} временных файлов")
    print(
        "🎯 Streaming merge по патчам "
        f"{', '.join(name for name, *_ in sorted(patch_specs, key=lambda x: _patch_sort_key(x[0])))}; "
        f"лимит {max_size_mb} МБ"
    )

    try:
        for index, file_path in enumerate(temp_files, 1):
            try:
                data = _load_temp_file(file_path)
            except Exception as e:
                broken_files_skipped += 1
                print(f"  ⚠️ Ошибка при чтении {file_path}: {e}")
                continue

            for raw_match_id, raw_match_data in data.items():
                if not isinstance(raw_match_data, dict):
                    continue

                match_id_norm = _normalize_match_id(raw_match_id)
                if match_id_norm is None:
                    match_id_norm = _normalize_match_id(raw_match_data.get("id"))
                if match_id_norm is None:
                    invalid_id_count += 1
                    continue

                if match_id_norm in processed_ids:
                    duplicates_count += 1
                    continue

                patch_name = _resolve_patch_name(raw_match_data.get("startDateTime"))
                if patch_name is None:
                    skipped_outside_patch += 1
                    if MERGE_DROP_OUTSIDE_PATCH:
                        continue
                    patch_name = OUTSIDE_PATCH_BUCKET

                match_data = dict(raw_match_data)
                match_data["id"] = int(match_id_norm)
                _write_entry(patch_name, str(match_id_norm), match_data)
                processed_ids.add(match_id_norm)

            if index % 25 == 0 or index == len(temp_files):
                patch_progress = ", ".join(
                    f"{patch}={state['written_matches'] + state['current_matches']}"
                    for patch, state in sorted(states.items(), key=lambda item: _patch_sort_key(item[0]))
                )
                print(
                    f"  📈 Обработано {index}/{len(temp_files)} файлов | "
                    f"Уникальных={len(processed_ids)} | Дубликатов={duplicates_count} | {patch_progress}"
                )
    finally:
        for patch_name in sorted(states, key=_patch_sort_key):
            _close_part(patch_name)

    with open(processed_ids_file, "wb") as f:
        f.write(orjson.dumps(sorted(processed_ids)))

    summary = {
        "patches": {
            patch: {
                "matches": state["written_matches"],
                "files": state["written_files"],
                "next_part_number": state["part_number"],
            }
            for patch, state in sorted(states.items(), key=lambda item: _patch_sort_key(item[0]))
        },
        "unique_matches_added": len(processed_ids) - existing_processed_ids_count,
        "duplicates_filtered": duplicates_count,
        "invalid_ids_skipped": invalid_id_count,
        "outside_patch_skipped": skipped_outside_patch,
        "broken_files_skipped": broken_files_skipped,
        "source_temp_files": len(temp_files),
    }
    summary_path = output_dir / "merge_patch_summary.json"
    with open(summary_path, "wb") as f:
        f.write(orjson.dumps(summary, option=orjson.OPT_INDENT_2))

    print("\n🎉 Streaming patch merge завершён!")
    for patch, state in sorted(states.items(), key=lambda item: _patch_sort_key(item[0])):
        print(f"   {patch}: {state['written_matches']} матчей, файлов: {state['written_files']}")
    print(f"🔄 Дубликатов отфильтровано: {duplicates_count}")
    print(f"🆔 Невалидных ID пропущено: {invalid_id_count}")
    print(f"🗓️ Вне 7.40/7.41 пропущено: {skipped_outside_patch}")
    if broken_files_skipped:
        print(f"⚠️ Полностью пропущено битых файлов: {broken_files_skipped}")
    print(f"📄 summary: {summary_path}")

    if cleanup:
        try:
            shutil.rmtree(temp_folder)
            print(f"🗑️ Папка {temp_folder} удалена")
        except Exception as e:
            print(f"⚠️ Не удалось удалить {temp_folder}: {e}")

    return output_files


def check_match_quality(
    match,
    check_old_maps=False,
    enable_skill_gap_filters=False,
    enable_death_anomaly_filter=False,
    strict_lane_positions=False,
    enable_smurf_pair_filter=False,
    smurf_min_flags=2,
):
    """Проверяет качество данных карты.

    СОСТОЯНИЕ ФИЛЬТРОВ (замеры — docs/EXPERIMENTS.md E-20, E-21).

    Активны всегда:
      * `position is None` — три четверти всего отсева (791k карт). Скорее всего
        непаршенные матчи, а не мусор по качеству;
      * `heroId is None`;
      * `intentionalFeeding` у любого игрока (флаг Stratz) — 5.3k;
      * 2+ невалидные для героя позиции: сначала попытка починить перестановкой
        кора и саппорта в паре линий, иначе отказ (`lane mismatch`, `nonstandard`,
        `networth order`).

    По параметру:
      * `strict_lane_positions` — любая невалидная для героя позиция = отказ.
        Включён при сборке словарей;
      * `enable_smurf_pair_filter` — отказ, если помеченных смурф-флагом игроков
        не меньше `smurf_min_flags` (по умолчанию 2). Пара — лучшее правило по
        цене: 4.1% корпуса за +0.0024 AUC, вдвое эффективнее полного фильтра
        (`smurf_min_flags=1`: 14.6% за +0.0056). Выключен по умолчанию, потому
        что при сборе отказ НЕОБРАТИМ — матч не сохраняется вовсе. Порог вынесен
        в параметр ради E-38: сумма агрессивных фильтров впервые проходит критерий
        E-37, и её надо померить на прод-отборе, а не на паблик-AUC;
      * `enable_death_anomaly_filter` — ИЗМЕРЕН И ВРЕДЕН. Отсекает 4.6% карт,
        которые НЕ грязнее остальных: драфт предсказывает на них так же
        (AUC 0.6281 против 0.6265). Не включать;
      * `enable_skill_gap_filters` — ИЗМЕРЕН И БЕСПОЛЕЗЕН на паблике: пороги
        (15k нетворта на 15-й минуте) калибровались под про и срабатывают на
        13 картах из 50 000. Либо перекалибровать, либо не трогать.

    Не реализовано здесь: изолированный провал по GPM (`iso >= 0.4`, ещё +1.6%
    цены за +0.0008) — ему нужны бейзлайны герой+позиция, которых на этапе сбора
    нет. Его место — этап сборки словарей.
    """
    if strict_lane_positions and not _has_position_catalog():
        return False, 'hero valid positions unavailable'

    # if match.get('direKills') is None:
    #     return False, 'kills None'
    # iytf len(match.get('radiantNetworthLeads', [])) < 20:
    #     return False, 'too short'
    # Смурфы: помеченный игрок ломает связь драфта с исходом, и ОДИН уже даёт
    # просадку (AUC 0.6030 против 0.6266). Но отсекать по одному дорого — это
    # 14.6% корпуса; по двум и более цена падает до 4.1% при половине выигрыша.
    # Флаги 0 и 2 считаются чистыми (так же было в исходной закомментированной
    # проверке). Анонимность слепой зоной НЕ является — проверено: анонимных
    # помечают ВДВОЕ чаще (4.44% против 2.15%), и smurfFlag у них выставлен.
    if enable_smurf_pair_filter:
        flagged = 0
        for player in match.get('players', []):
            account = player.get('steamAccount') or {}
            flag = account.get('smurfFlag')
            if flag is not None and flag not in (0, 2):
                flagged += 1
        if flagged >= max(1, int(smurf_min_flags)):
            return False, f'smurf pair ({flagged})'

    invalid_positions = 0
    invalid_by_team = {True: {}, False: {}}
    for player in match.get('players', []):
        if None == player.get('position'):
            return False, 'position'
        if None == player.get('heroId', {}):
            return False, 'hero id None'
        if player['intentionalFeeding']:
            return False, 'intentionalFeeding'
        # if None == player.get('steamAccount'):
        #     return False, 'steamAccount None'
        # if player['steamAccount']['smurfFlag'] not in [0, 2]:
        #     return False, 'smurf'

        # Проверка валидности позиции героя: отсекаем матч если 2+ неверных позиций
        if _has_position_catalog():
            hero_id = player.get('heroId')
            position = player.get('position')
            is_valid_position = _position_is_valid_for_hero(hero_id, position)
            if is_valid_position is False:
                invalid_positions += 1
                team_key = bool(player.get('isRadiant'))
                normalized_position = _normalize_position_label(position)
                invalid_by_team[team_key][normalized_position or position] = player

    if invalid_positions >= 2:
        lane_pairs = (('pos1', 'pos5'), ('pos3', 'pos4'))

        def _hero_can_play(hero_id, position):
            return _position_is_valid_for_hero(hero_id, position) is True

        for team_key, invalid_map in invalid_by_team.items():
            if not invalid_map:
                continue

            remaining = set(invalid_map.keys())
            pairs_to_fix = []
            for core_pos, support_pos in lane_pairs:
                pair = {core_pos, support_pos}
                if pair.issubset(remaining):
                    pairs_to_fix.append((core_pos, support_pos))
                    remaining -= pair

            if remaining:
                return False, 'invalid positions lane mismatch'

            for core_pos, support_pos in pairs_to_fix:
                core_player = invalid_map.get(core_pos)
                support_player = invalid_map.get(support_pos)
                if not core_player or not support_player:
                    return False, 'invalid positions lane mismatch'

                core_net = core_player.get('networth')
                support_net = support_player.get('networth')
                if core_net is None or support_net is None:
                    return False, 'invalid positions networth missing'

                # Если у core меньше networth - роли вероятно перепутаны
                if core_net < support_net:
                    core_hero = core_player.get('heroId')
                    support_hero = support_player.get('heroId')
                    if _hero_can_play(core_hero, support_pos) and _hero_can_play(support_hero, core_pos):
                        # Перестановка мутирует матч ДО сохранения, исходные
                        # позиции нигде не остаются — из-за этого починку
                        # невозможно сравнить с отбраковкой на корпусе (E-24).
                        # Метка даёт выборку на будущее: через месяц сбора по
                        # ней можно проверить главный риск — что перестановка
                        # иногда делается неверно и матч заезжает в словарь с
                        # чужими позиционными ключами.
                        match.setdefault('positions_repaired', []).append({
                            'team_radiant': bool(team_key),
                            'core_pos': core_pos,
                            'support_pos': support_pos,
                            'core_hero': core_hero,
                            'support_hero': support_hero,
                            'core_networth': core_net,
                            'support_networth': support_net,
                        })
                        core_player['position'] = support_pos
                        support_player['position'] = core_pos
                    else:
                        return False, 'invalid positions nonstandard'
                else:
                    return False, 'invalid positions networth order'

    if strict_lane_positions and _has_position_catalog():
        remaining_invalid = 0
        for player in match.get('players', []):
            hero_id = player.get('heroId')
            position = player.get('position')
            if hero_id is None or position is None:
                continue
            is_valid_position = _position_is_valid_for_hero(hero_id, position)
            if is_valid_position is False:
                remaining_invalid += 1
        if remaining_invalid > 0:
            return False, 'invalid positions strict'


    if enable_skill_gap_filters or enable_death_anomaly_filter:
        players = match.get('players', [])
        radiant_deaths = [p.get('deaths', 0) for p in players if p.get('isRadiant')]
        dire_deaths = [p.get('deaths', 0) for p in players if not p.get('isRadiant')]

        # Отдельный фильтр аномалий по deaths
        if enable_death_anomaly_filter:
            for team_deaths, team_name in ((radiant_deaths, 'radiant'), (dire_deaths, 'dire')):
                if len(team_deaths) >= 5:
                    sorted_deaths = sorted(team_deaths)
                    min_first = sorted_deaths[3]
                    min_second = sorted_deaths[4]
                    max_deaths = max(team_deaths)
                    if max_deaths >= 10 and min_first > 0 and min_second >= min_first * 2:
                        return False, f'death anomaly {team_name} ({min_first}/{min_second})'

        # Фильтрация матчей с сильным skill gap по раннему нетворту
        if enable_skill_gap_filters:
            raw_leads = match.get('radiantNetworthLeads', [])
            networth_leads = []
            for lead in raw_leads:
                try:
                    networth_leads.append(float(lead))
                except Exception:
                    networth_leads.append(0.0)

            if len(networth_leads) >= 15:
                early_networth_diff = abs(networth_leads[14])  # 15-я минута
                if early_networth_diff > 15000:
                    return False, f'skill gap: early dominance ({early_networth_diff:.0f} networth diff at 15min)'

                early_slice = [abs(lead) for lead in networth_leads[10:15]]
                avg_early_lead = sum(early_slice) / len(early_slice) if early_slice else 0.0
                if avg_early_lead > 12000:
                    return False, f'skill gap: consistent early dominance (avg {avg_early_lead:.0f} lead 10-15min)'

            if len(networth_leads) >= 10:
                very_early_slice = [abs(lead) for lead in networth_leads[5:10]]  # 6-10 минуты
                avg_very_early_lead = sum(very_early_slice) / len(very_early_slice) if very_early_slice else 0.0
                if avg_very_early_lead > 10000:
                    return False, f'skill gap: very early dominance (avg {avg_very_early_lead:.0f} lead 6-10min)'

                minute_5_lead = abs(networth_leads[5]) if len(networth_leads) > 5 else 0.0
                minute_10_lead = abs(networth_leads[9]) if len(networth_leads) > 9 else 0.0
                growth_rate = minute_10_lead - minute_5_lead
                if growth_rate > 8000:
                    return False, f'skill gap: rapid advantage growth ({growth_rate:.0f} growth 5-10min)'

                if len(networth_leads) >= 6:
                    minute_0_lead = abs(networth_leads[0]) if len(networth_leads) > 0 else 0.0
                    early_growth = minute_5_lead - minute_0_lead
                    if early_growth > 6000:
                        return False, f'skill gap: very rapid early growth ({early_growth:.0f} growth 0-5min)'

            match_duration = len(networth_leads)
            radiant_total_deaths = sum(radiant_deaths) if radiant_deaths else 0
            dire_total_deaths = sum(dire_deaths) if dire_deaths else 0

            if 20 <= match_duration <= 30:
                total_deaths = radiant_total_deaths + dire_total_deaths
                deaths_per_minute = total_deaths / match_duration if match_duration > 0 else 0.0
                if deaths_per_minute >= 2.5 and len(networth_leads) >= 15:
                    early_lead = abs(networth_leads[14])
                    if early_lead > 10000:
                        return False, (
                            f'skill gap: high deaths rate ({deaths_per_minute:.2f}/min) '
                            f'+ early dominance in short match'
                        )
    
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
                                   num_workers=3, concurrent_requests=10):
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
            player_ids_check=False,
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



PRO_VISITED_TEAMS_FILE = "visited_teams.json"


def _seed_team_ids():
    """Стартовый набор: команды из tier_one_teams / tier_two_teams."""
    from id_to_names import tier_one_teams, tier_two_teams
    out = []
    for mapping in (tier_one_teams, tier_two_teams):
        for value in mapping.values():
            if isinstance(value, (set, list, tuple)):
                out.extend(int(v) for v in value if isinstance(v, int))
            elif isinstance(value, int):
                out.append(int(value))
    return sorted(set(out))


def _load_visited_teams(path):
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return {int(x) for x in json.load(fh)}
    except Exception as e:
        print(f"⚠️ Не удалось прочитать {path}: {e} — начинаю с пустого набора")
        return set()


def _save_visited_teams(path, visited):
    """rebuild-then-replace: набор посещённых нельзя потерять при обрыве."""
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(sorted(int(x) for x in visited), fh)
    os.replace(tmp, path)


def _teams_from_corpus(output_dir):
    """Id команд, уже встречавшихся в собранных матчах — соперники прошлых волн."""
    found = set()
    if not os.path.isdir(output_dir):
        return found
    for name in os.listdir(output_dir):
        if not name.endswith(".json") or name in ("merge_patch_summary.json",):
            continue
        try:
            with open(os.path.join(output_dir, name), "rb") as fh:
                data = orjson.loads(fh.read())
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        for match in data.values():
            if not isinstance(match, dict):
                continue
            for key in ("radiantTeam", "direTeam"):
                team = match.get(key) or {}
                tid = team.get("id")
                if isinstance(tid, int) and tid > 0:
                    found.add(tid)
    return found


def get_pros(max_waves=None):
    """Снежный ком по КОМАНДАМ: сиды -> их соперники -> соперники соперников.

    Раньше собирались только команды из tier1/tier2, то есть те, кто считается
    сильным СЕЙЧАС. История игрока, наигранная в другой команде, и матчи против
    команд вне списков в корпус не попадали вовсе.

    Волна = один прогон сбора по очереди неопрошенных команд. После него из
    свежесобранных матчей вынимаются id обеих команд, новые попадают в очередь
    следующей волны. Конец — когда новых команд не появилось.

    Каждая команда опрашивается ОДИН раз за всё время: набор посещённых лежит в
    `visited_teams.json` рядом с корпусом и переживает перезапуск. Без него
    повторные волны гоняли бы одни и те же команды и жгли квоту Stratz.
    """
    output_dir = os.path.join(str(PRO_HEROES_DIR), "json_parts_split_from_object")
    visited_path = os.path.join(str(PRO_HEROES_DIR), PRO_VISITED_TEAMS_FILE)
    visited = _load_visited_teams(visited_path)

    known = set(_seed_team_ids()) | _teams_from_corpus(output_dir)
    queue = sorted(known - visited)
    print(f"🌱 команд известно {len(known):,}, уже опрошено {len(visited):,}, "
          f"в очереди {len(queue):,}")

    try:
        proxy_count = len(STRATZ_PROXY_MAP) if STRATZ_PROXY_MAP else 1
    except Exception:
        proxy_count = 1
    batch_concurrency = max(1, min(10, proxy_count))

    wave = 0
    while queue:
        wave += 1
        if max_waves is not None and wave > int(max_waves):
            print(f"⏹️ достигнут предел волн ({max_waves}), в очереди осталось {len(queue):,}")
            break
        print(f"\n🌊 волна {wave}: опрашиваю {len(queue):,} команд", flush=True)
        asyncio.run(get_maps_new(ids=list(queue), pro=True,
                                 mkdir=str(PRO_HEROES_DIR), skip_auxiliary_files=True,
                                 batch_concurrency=batch_concurrency))
        visited |= set(queue)
        _save_visited_teams(visited_path, visited)

        discovered = _teams_from_corpus(output_dir)
        queue = sorted(discovered - visited)
        print(f"🔎 волна {wave} закончена: всего команд в корпусе {len(discovered):,}, "
              f"новых для следующей волны {len(queue):,}", flush=True)
    print(f"\n✅ снежный ком остановился: опрошено команд {len(visited):,}, волн {wave}")


# ----------------------------------------------------------------------------
# playbackData: события внутри матча
# ----------------------------------------------------------------------------
# Старое тело запроса (ветка pro в get_maps_new) НЕ трогаем — оно собирает итоги
# карты. Здесь отдельный запрос за тем, чего в итогах нет в принципе:
#   * deathEvents  — кто кого убил и чем (attacker/target/byAbility/byItem);
#   * purchaseEvents — во сколько куплен предмет;
#   * playerUpdateLevelEvents — во сколько взят уровень (значит и талант).
#
# Три вещи, которые без этого не считаются: «садится на саппортов» (нужна жертва),
# «с какой минуты у героя сейв со скипетра» и «когда включается талант 15».
#
# Оговорка по хранению: детальный playback живёт у Stratz около 85 суток —
# на матче возрастом 84 дня события ещё есть, на 88 уже пусто при parsedDateTime.
# Карты старше собирать бессмысленно.
#
# Лимиты. Проверено 15.08: счётчики у всех четырёх ключей ОДИНАКОВЫ
# (час 0/1500, сутки 7500/15000) — квота общая на аккаунт, а не на ключ.
# Поэтому смысл имеет не параллелизм по парам, а батч: по 5 матчей в запросе.
PLAYBACK_QUERY = """query($ids: [Long]!) {
  matches(ids: $ids) {
    id parsedDateTime durationSeconds
    players {
      heroId isRadiant position
      playbackData {
        deathEvents { time attacker target byAbility byItem isBurst isFeed }
        purchaseEvents { time itemId }
        playerUpdateLevelEvents { time level }
      }
    }
  }
}"""


def _playback_compact(match):
    """Только нужное: сырой ответ на 43 тысячи карт весит под гигабайт."""
    out = {"id": match.get("id"), "parsed": match.get("parsedDateTime"),
           "dur": match.get("durationSeconds"), "players": []}
    for p in match.get("players") or []:
        pb = p.get("playbackData") or {}
        out["players"].append({
            "hero": p.get("heroId"),
            "rad": p.get("isRadiant"),
            "pos": (p.get("position") or "").replace("POSITION_", ""),
            "deaths": [[d.get("time"), d.get("attacker"), d.get("target"),
                        d.get("byAbility"), d.get("byItem"),
                        int(bool(d.get("isBurst"))), int(bool(d.get("isFeed")))]
                       for d in (pb.get("deathEvents") or [])],
            "buys": [[e.get("time"), e.get("itemId")] for e in (pb.get("purchaseEvents") or [])],
            "levels": [[e.get("time"), e.get("level")]
                       for e in (pb.get("playerUpdateLevelEvents") or [])],
        })
    return out


def _playback_done_ids(out_dir):
    """Обработано = карта РЕАЛЬНО лежит в данных, а не «мы к ней ходили».

    Отметка по факту похода теряет карты: при исчерпании попыток id уходил в
    обработанные пустым. Плюс убитый прогон оставляет недописанный gzip-член, и
    всё, что дописано после него, читателю уже не видно — поэтому каждый прогон
    пишет в СВОЙ файл.
    """
    import gzip as _gzip
    done = set()
    for name in os.listdir(out_dir) if os.path.isdir(out_dir) else []:
        if not name.endswith('.jsonl.gz'):
            continue
        try:
            with _gzip.open(os.path.join(out_dir, name), 'rt', encoding='utf-8') as fh:
                for line in fh:
                    try:
                        done.add(int(json.loads(line)['id']))
                    except Exception:
                        continue
        except Exception:
            pass
    return done


async def get_playback_new(ids, out_dir, batch_size=5, concurrency=2, show_prints=True):
    """Сбор playbackData пачками по match id. Возобновляемый."""
    import gzip as _gzip

    os.makedirs(out_dir, exist_ok=True)
    done = _playback_done_ids(out_dir)
    todo = [int(i) for i in ids if int(i) not in done]
    if show_prints:
        print(f"🎬 playback: карт всего {len(ids):,}, уже собрано {len(done):,}, "
              f"к сбору {len(todo):,}", flush=True)
    if not todo:
        return 0

    batch_size = max(1, min(5, int(batch_size)))
    batches = [todo[i:i + batch_size] for i in range(0, len(todo), batch_size)]
    run_tag = time.strftime('%Y%m%d_%H%M%S')
    path = os.path.join(out_dir, f'playback_{run_tag}.jsonl.gz')
    pool = get_proxy_pool()
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    stats = {'ok': 0, 'unparsed': 0, 'err': 0, 'batches': 0}
    lock = asyncio.Lock()
    fh = _gzip.open(path, 'wt', encoding='utf-8')

    async def one(batch):
        async with sem:
            try:
                data = await pool.make_request(
                    url='https://api.stratz.com/graphql',
                    json={"query": PLAYBACK_QUERY, "variables": {"ids": batch}},
                    headers={"Content-Type": "application/json", "Accept": "application/json",
                             "User-Agent": "STRATZ_API"},
                )
            except Exception as exc:
                async with lock:
                    stats['err'] += 1
                if show_prints and stats['err'] % 20 == 0:
                    print(f"   ⚠️ ошибок {stats['err']}: {str(exc)[:80]}", flush=True)
                return
            matches = ((data or {}).get('data') or {}).get('matches') or []
            async with lock:
                for m in matches:
                    if not m:
                        continue
                    row = _playback_compact(m)
                    fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                    stats['ok'] += 1
                    if not row['parsed']:
                        stats['unparsed'] += 1
                fh.flush()          # без сброса убитый прогон теряет всё, что в буфере
                stats['batches'] += 1
                if show_prints and stats['batches'] % 50 == 0:
                    print(f"   собрано {stats['ok']:,} карт ({stats['unparsed']:,} без разбора), "
                          f"пачек {stats['batches']:,} из {len(batches):,}, ошибок {stats['err']}",
                          flush=True)

    await asyncio.gather(*(one(b) for b in batches))
    fh.close()
    if show_prints:
        print(f"✅ playback: собрано {stats['ok']:,} карт, без разбора {stats['unparsed']:,}, "
              f"ошибок {stats['err']}, файл {path}", flush=True)
    return stats['ok']


def get_pros_playback(max_age_days=85, out_dir=None, limit=None):
    """playbackData по про-картам корпуса, у которых он ещё существует.

    Возраст берётся не с потолка: у Stratz детальный playback живёт около 85
    суток (проверено на матчах 84 и 88 дней), у старых собирать нечего.
    """
    corpus = os.path.join(str(PRO_HEROES_DIR), "json_parts_split_from_object")
    out_dir = out_dir or os.path.join(str(PRO_HEROES_DIR), "playback")
    cutoff = time.time() - float(max_age_days) * 86400
    rows = []
    for name in sorted(os.listdir(corpus)):
        if not name.endswith('.json') or name == 'merge_patch_summary.json':
            continue
        try:
            with open(os.path.join(corpus, name), 'rb') as fh:
                for _key, match in ijson.kvitems(fh, '', use_float=True):
                    if not isinstance(match, dict):
                        continue
                    ts = match.get('startDateTime') or 0
                    if ts and ts >= cutoff and match.get('id'):
                        rows.append((int(ts), int(match['id'])))
        except Exception as exc:
            print(f"⚠️ {name}: {exc}")
    rows = sorted(set(rows), reverse=True)      # сначала самые свежие
    ids = [i for _ts, i in rows]
    if limit:
        ids = ids[:int(limit)]
    print(f"🎬 карт моложе {max_age_days} дней: {len(ids):,}")
    return asyncio.run(get_playback_new(ids=ids, out_dir=out_dir))


def get_pubs():
    from concurrent.futures import ProcessPoolExecutor
    import multiprocessing

    json_dir = os.getenv(
        "PUBS_IDS_SOURCE_DIR",
        str(PUBS_SOURCE_DIR),
    )
    if not os.path.isdir(json_dir):
        raise FileNotFoundError(f"PUBS ids source dir not found: {json_dir}")

    files = [
        os.path.join(json_dir, f)
        for f in os.listdir(json_dir)
        if f.startswith('combined') and f.endswith('.json')
    ]
    if not files:
        files = [
            os.path.join(json_dir, f)
            for f in os.listdir(json_dir)
            if f.endswith('.json') and f not in {'processed_ids.txt', 'merge_patch_summary.json'}
        ]
    if not files:
        raise RuntimeError(f"No source json files found in PUBS ids source dir: {json_dir}")

    print(f"📂 PUB ids source dir: {json_dir}")
    print(f"📄 PUB ids source files: {len(files)}")
    
    num_workers = min(multiprocessing.cpu_count(), len(files))
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = executor.map(_process_json_file, files)
    
    ids = set()
    for result in results:
        ids.update(result)

    batch_size = 5
    # Ограничиваем параллелизм числом доступных Stratz-прокси, но не более 10
    try:
        proxy_count = len(STRATZ_PROXY_MAP) if STRATZ_PROXY_MAP else 1
    except Exception:
        proxy_count = 1
    batch_concurrency = max(1, min(7, proxy_count * 7))
    print(f"⚡ PUB сбор: batch_size={batch_size}, batch_concurrency={batch_concurrency}")
    asyncio.run(get_maps_new(ids=ids,
                             mkdir=str(ANALYSE_PUB_DIR),
                             show_prints=False,
                             batch_size=batch_size,
                             batch_concurrency=batch_concurrency,
                             start_date_time=start_date_time))


def _process_json_file(filepath):
    # Подавляем warnings в дочерних процессах (ProcessPoolExecutor)
    import warnings
    warnings.filterwarnings('ignore')
    
    ids = set()
    try:
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
    except Exception as e:
        print(f"⚠️ Ошибка парсинга {filepath}: {e}")
    return ids

def update_my_protracker(show_prints=True, num_workers=1, concurrent_requests=5):
    get_pubs()
    # get_pros()



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
    
    update_my_protracker(show_prints=True)
