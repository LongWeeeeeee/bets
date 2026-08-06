#!/usr/bin/env python3
"""Bookmaker parser for Dota2 odds/presence/deeplinks.

Requirements (already installed in venv_catboost):
  - selenium
  - selenium-wire
  - bs4
  - camoufox (optional, enabled via env flags)

Usage:
  source venv_catboost/bin/activate
  python base/bookmaker_selenium_odds.py --team1 "Lynx" --team2 "Yellow Submarine"
  python base/bookmaker_selenium_odds.py --manual-map-check --team1 "Avalanche" --team2 "Under Effect" --map-num 2
"""

from __future__ import annotations

import inspect
import asyncio
import argparse
import concurrent.futures
import contextlib
import hashlib
import json
import logging
import os
import re
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from seleniumwire import webdriver

try:  # общий справочник написаний команд; модуль обязан работать и без него
    from team_name_aliases import alias_spellings as _alias_spellings
    from team_name_aliases import fold_confusables as _fold_confusables
except ImportError:  # pragma: no cover — зависит от sys.path процесса
    try:
        from base.team_name_aliases import alias_spellings as _alias_spellings
        from base.team_name_aliases import fold_confusables as _fold_confusables
    except ImportError:

        def _alias_spellings(_name: str) -> List[str]:
            return []

        def _fold_confusables(value: str) -> str:
            return str(value or "").lower()


async def _maybe_await(value):
    """Разворачивает результат page.*: корутину ждём, готовое значение отдаём.

    Благодаря этому один и тот же код работает и с настоящей async-страницей
    Playwright, и с синхронными тестовыми дублёрами, которые возвращают
    готовые значения.
    """
    if inspect.isawaitable(value):
        return await value
    return value


def _run_coroutine_blocking(coro):
    """Выполнить корутину из синхронного контекста.

    Используется обёртками точек входа: standalone-монитор и тесты зовут их
    по-старому, синхронно. Внутри async-сессии вызываются *_async напрямую,
    поэтому вложенного цикла событий здесь не бывает.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    # Мы внутри уже работающего цикла: его держит sync-API Playwright в рабочем
    # потоке общей Camoufox-сессии. asyncio.run() здесь невозможен, а прежний
    # raise ломал единственный путь снятия кэфов Winline: ошибка принималась за
    # проблему прокси, браузер пересоздавался, и так по кругу.
    #
    # В этом модуле нет ни одной настоящей точки приостановки (ни asyncio.sleep,
    # ни gather) — все await'ы это _maybe_await поверх синхронных объектов
    # Playwright, отдающих готовые значения. Поэтому корутина проходит до конца
    # с первого же send(None), и цикл событий ей не нужен.
    try:
        coro.send(None)
    except StopIteration as stop:
        return stop.value
    # Корутина всё-таки приостановилась — значит в модуле появился настоящий
    # await, и эту обёртку больше нельзя крутить вручную.
    coro.close()
    raise RuntimeError(
        "sync entrypoint hit a real suspension point inside a running event loop; "
        "call the *_async variant from async code instead"
    )

try:
    import camoufox
    CAMOUFOX_AVAILABLE = True
except Exception:
    camoufox = None
    CAMOUFOX_AVAILABLE = False

try:
    from base.keys import BOOKMAKER_PROXY_URL
except Exception:
    from keys import BOOKMAKER_PROXY_URL  # type: ignore

HEADLESS_DEFAULT = os.getenv("BOOKMAKER_SELENIUM_HEADLESS", "1").strip().lower()
BOOKMAKER_SELENIUM_HEADLESS = HEADLESS_DEFAULT not in {"0", "false", "no", "off"}
BOOKMAKER_CAMOUFOX_ENABLED = (
    os.getenv("BOOKMAKER_CAMOUFOX_ENABLED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
) and CAMOUFOX_AVAILABLE
BOOKMAKER_CAMOUFOX_PRESENCE_ENABLED = (
    os.getenv("BOOKMAKER_CAMOUFOX_PRESENCE_ENABLED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
) and CAMOUFOX_AVAILABLE
if BOOKMAKER_CAMOUFOX_ENABLED:
    BOOKMAKER_CAMOUFOX_PRESENCE_ENABLED = True

BOOKMAKER_URLS: Dict[str, Dict[str, str]] = {
    "live": {
        "betboom": "https://betboom.ru/esport/live/dota-2",
        "pari": "https://pari.ru/esports-live/category/dota2",
        # Дотовская страница, а не общий live-фид: общий фид рендерит ленту
        # порциями (~9 блоков) во ВНУТРЕННЕМ контейнере `div.main__wrapper`,
        # тело страницы там не скроллится вовсе, поэтому дотовский матч ниже
        # первой порции просто отсутствовал в DOM ("match not found").
        # Проверено 05.08.2026: на `dota_2` вся дотовская лента попадает в DOM
        # без единой прокрутки. Цена — на странице соседствует линия
        # (`Завтра 15:00`), её отсекает `_looks_future_context`.
        "winline": "https://winline.ru/stavki/sport/kibersport/dota_2",
    },
    "all": {
        "betboom": "https://betboom.ru/esport/dota-2?period=all",
        "pari": "https://pari.ru/esports/category/dota2",
        "winline": "https://winline.ru/stavki/sport/kibersport",
    },
}
SUPPORTED_BOOKMAKER_SITES: Tuple[str, ...] = tuple(BOOKMAKER_URLS.get("live", {}).keys()) or (
    "betboom",
    "pari",
    "winline",
)
CHROME_BIN = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
ODD_RE = re.compile(r"(?<!\d)(\d{1,2}[.,]\d{1,2})(?!\d)")
FUTURE_MARKERS = ("завтра", "tomorrow")
MAP_MARKERS = ("карта", "1к", "2к", "3к", "map 1", "map 2", "map 3")
LOCK_MARKERS = (
    "🔒",
    "lock",
    "locked",
    "закрыт",
    "закрыты",
    "прием ставок приостановлен",
    "недоступно",
    "suspend",
)
SOURCE_PREFERENCE = {
    "dom_body_text": 0,
    "dom_visible_text": 1,
    "network_response": 2,
    "dom_html": 3,
}

# Silence noisy selenium-wire/mitmproxy transport logs ("Capturing request", websocket spam, etc.)
for _logger_name in (
    "seleniumwire",
    "seleniumwire.handler",
    "seleniumwire.server",
    "mitmproxy",
    "urllib3.connectionpool",
):
    logging.getLogger(_logger_name).setLevel(logging.WARNING)


def _norm(s: str) -> str:
    # `ё` обязана пережить нормализацию: без неё имя рвётся на два куска
    # (`Королёв` -> `корол в`) и не находится на странице ни в каком виде.
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9а-яё]+", " ", s.lower())).strip()


def _team_name_search_variants(team: str) -> List[str]:
    """DOM lookup variants for punctuation, CamelCase and known bookmaker spellings."""
    raw = str(team or "").strip()
    if not raw:
        return []
    out: List[str] = []
    seen = set()
    # Букмекер пишет ту же команду по-своему (`BoomBoys` -> `BB TEAM`), поэтому
    # известные написания ищем наравне с нашим названием.
    for name in [raw] + list(_alias_spellings(raw)):
        # SourceTV may emit `_PowerRangers`, while Winline renders
        # `POWER RANGERS`. This is one identity with different typography.
        camel_spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", name)
        for value in (name, name.lstrip("_"), camel_spaced, camel_spaced.lstrip("_")):
            normalized = _norm(value)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
    return out


# Родовые слова: их наличие ничего не говорит о том, ЧТО это за команда.
# Внутри полного названия они безобидны, отдельным поисковым словом — нет.
GENERIC_TEAM_TOKENS = {
    "team",
    "gaming",
    "esports",
    "esport",
    "club",
    "squad",
    "the",
}

# Слова, которыми второй состав отличается от основного. Отбрасывать их нельзя:
# `Team Spirit Academy` без `academy` превращается в `Team Spirit`, то есть в
# другую команду, а по её карточке пришли бы чужие кэфы.
ROSTER_QUALIFIER_TOKENS = {
    "academy",
    "junior",
    "juniors",
    "youth",
}


def _fallback_search_tokens(team: str) -> List[str]:
    """Сокращённые написания названия — на случай, когда полного на странице нет.

    Отдаём только однозначные формы. Родовое слово (`team`) и обрубки в один-два
    символа искать нельзя: 31.07.2026 `L1GA TEAM` искалось словом `team`, ловило
    `TEAM VOODOOSH` из соседней карточки, и кэфы возвращались от чужого матча.
    """
    variants = _team_name_search_variants(team)
    if not variants:
        return []
    seen = set(variants)  # полные написания ищутся до токенов, дублировать нечего
    out: List[str] = []

    def _add(value: str) -> None:
        if value and value not in seen:
            seen.add(value)
            out.append(value)

    for variant in variants:
        tokens = [token for token in variant.split() if token]
        if not tokens:
            continue
        core = [token for token in tokens if token not in GENERIC_TEAM_TOKENS]
        if core and core != tokens and (len(core) > 1 or len(core[0]) >= 3):
            # Название без родовых слов: `Team Liquid` -> `liquid`,
            # `Level UP esports` -> `level up`, `L1GA TEAM` -> `l1ga`.
            _add(" ".join(core))
        if any(token in ROSTER_QUALIFIER_TOKENS for token in tokens):
            # У второго состава отдельные слова искать нельзя: по `spirit`
            # нашёлся бы основной ростер.
            continue
        for token in sorted(core, key=len, reverse=True):
            # Порог в 3 символа проверен на корпусе: у `LGD.Pinghu` на странице
            # написано `LGD GAMING`, и без токена `lgd` карточка не находится.
            # Ломали поиск не короткие значащие слова, а родовые (`team`)
            # и обрубки в 1-2 символа из разбитого по цифре названия.
            if len(token) >= 3:
                _add(token)
    return out


def _literal_team_positions(text: str, value: str) -> List[int]:
    """Find a team name/token without accepting it inside a longer word.

    Обе стороны приводим к одному алфавиту: Winline пишет `TEAM TPABOMAH`
    латинскими двойниками кириллических букв. Замена посимвольная, длина строки
    не меняется, поэтому найденные позиции валидны для исходного текста.
    """
    needle = _fold_confusables(value or "")
    if not needle:
        return []
    left = r"(?<![a-z0-9а-яё])" if needle[0].isalnum() else ""
    right = r"(?![a-z0-9а-яё])" if needle[-1].isalnum() else ""
    return [
        match.start()
        for match in re.finditer(
            left + re.escape(needle) + right, _fold_confusables(text), re.I
        )
    ]


def _find_positions_with_fallback(low: str, team: str) -> List[int]:
    for variant in _team_name_search_variants(team):
        direct = _literal_team_positions(low, variant)
        if direct:
            return direct
    for token in _fallback_search_tokens(team):
        positions = _literal_team_positions(low, token)
        if positions:
            return positions
    return []


def _first_index_with_fallback(low: str, team: str) -> int:
    """Первая позиция команды в тексте — по тем же правилам, что и поиск карточки.

    Раньше здесь был обычный `find` без границ слова, и порядок сторон рынка мог
    определиться подстрокой: `Pari` внутри `PARIVISION`, `1w` внутри `1WIN`.
    Порядок решает, какой команде достанется какой кэф, поэтому правило одно.
    """
    positions = _find_positions_with_fallback(low, team)
    return min(positions) if positions else -1


def _parse_proxy(proxy_url: str) -> Dict[str, str]:
    parsed = urlparse(proxy_url)
    if not parsed.hostname or not parsed.port:
        raise ValueError(f"Invalid proxy URL: {proxy_url}")
    if parsed.username is None or parsed.password is None:
        raise ValueError("Proxy URL must contain auth credentials")
    return {
        "host": parsed.hostname,
        "port": str(parsed.port),
        "username": parsed.username,
        "password": parsed.password,
    }


def _camoufox_proxy_kwargs(proxy_url: Optional[str]) -> Dict[str, Any]:
    if not proxy_url:
        return {}
    parsed = _parse_proxy(proxy_url)
    return {
        "proxy": {
            "server": f"http://{parsed['host']}:{parsed['port']}",
            "username": parsed["username"],
            "password": parsed["password"],
        }
    }


def _extract_numeric_odds(text: str, max_count: int = 8) -> List[float]:
    vals: List[float] = []
    for m in ODD_RE.finditer(text):
        # Skip date-like fragments: 18.03.26, 1.03.26 etc.
        left_ctx = text[max(0, m.start() - 3):m.start()]
        right_ctx = text[m.end():m.end() + 4]
        if right_ctx.startswith(".") and re.match(r"\.\d{2}", right_ctx):
            continue
        if re.search(r"\d$", left_ctx) and text[m.start():m.end()].count(".") == 1:
            pass
        v = float(m.group(1).replace(",", "."))
        if 1.01 <= v <= 200.0:
            vals.append(v)
    uniq: List[float] = []
    seen = set()
    for v in vals:
        if v in seen:
            continue
        seen.add(v)
        uniq.append(v)
        if len(uniq) >= max_count:
            break
    return uniq


def _extract_odds_near_teams(snippet: str, team1: str, team2: str) -> List[float]:
    low = snippet.lower()
    t1 = team1.lower()
    t2 = team2.lower()
    i1 = _first_index_with_fallback(low, t1)
    i2 = _first_index_with_fallback(low, t2)
    if i1 == -1 or i2 == -1:
        return []
    left = min(i1, i2)
    right = max(i1 + len(team1), i2 + len(team2))
    # Take mostly the tail after team names to avoid odds from previous matches.
    lo = max(0, left)
    hi = min(len(snippet), right + 380)
    return _extract_numeric_odds(snippet[lo:hi], max_count=6)


def _context_around_teams(snippet: str, team1: str, team2: str, radius: int = 500) -> str:
    low = snippet.lower()
    t1 = team1.lower()
    t2 = team2.lower()
    i1 = _first_index_with_fallback(low, t1)
    i2 = _first_index_with_fallback(low, t2)
    if i1 == -1 or i2 == -1:
        return snippet[:600]
    center = (i1 + i2) // 2
    lo = max(0, center - radius)
    hi = min(len(snippet), center + radius)
    return snippet[lo:hi]


def _context_local_to_teams(
    text: str,
    team1: str,
    team2: str,
    left: int = 24,
    right: int = 520,
) -> Optional[str]:
    if not text:
        return None
    low = text.lower()
    t1 = (team1 or "").strip().lower()
    t2 = (team2 or "").strip().lower()
    if not t1 or not t2:
        return None
    i1 = _first_index_with_fallback(low, t1)
    i2 = _first_index_with_fallback(low, t2)
    if i1 == -1 or i2 == -1:
        return None
    lo_ref = min(i1, i2)
    hi_ref = max(i1 + len(t1), i2 + len(t2))
    lo = max(0, lo_ref - max(0, int(left)))
    hi = min(len(text), hi_ref + max(80, int(right)))
    return text[lo:hi]


def _text_matches_teams(text: str, team1: str, team2: str) -> bool:
    return _snippet_by_teams(
        text or "",
        team1 or "",
        team2 or "",
        radius=260,
        max_team_distance=2500,
    ) is not None


def _unique_team_names(names: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in names:
        value = str(raw or "").strip()
        norm = _norm(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(value)
    return out


def _find_presence_from_sources(
    team1: str,
    team2: str,
    sources: List[Tuple[str, str]],
    *,
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
) -> Tuple[bool, str, str]:
    team1_candidates = _unique_team_names([team1, *(team1_aliases or [])])
    team2_candidates = _unique_team_names([team2, *(team2_aliases or [])])
    best_source = ""
    best_detail = ""
    best_score: Optional[Tuple[int, int]] = None

    for source_name, text in sources:
        for candidate_team1 in team1_candidates:
            for candidate_team2 in team2_candidates:
                if _norm(candidate_team1) == _norm(candidate_team2):
                    continue
                snippet = _snippet_by_teams(text, candidate_team1, candidate_team2)
                if not snippet:
                    continue
                detail = _context_around_teams(snippet, candidate_team1, candidate_team2)
                score = (-len(_norm(candidate_team1)) - len(_norm(candidate_team2)), len(detail or ""))
                if best_score is None or score < best_score:
                    best_score = score
                    best_source = source_name
                    best_detail = (
                        f"matched_as={candidate_team1} vs {candidate_team2}; "
                        f"{detail or 'match found'}"
                    )

    return best_score is not None, best_source, best_detail


def _presence_should_open_match_details(
    site: str,
    current_url: str,
    *,
    body_len: int,
    source_count: int,
) -> bool:
    if site not in {"betboom", "pari", "winline"}:
        return False
    target_url = str(current_url or "").strip()
    if target_url and _href_looks_match_page(site, target_url):
        return False
    if site == "pari":
        return body_len < 5000 or source_count < 4
    if site == "betboom":
        return body_len < 2500 or source_count < 3
    return body_len < 6000 or source_count < 3


def _presence_collect_probe_snapshot(drv, *, url: str) -> Tuple[str, str, str, int, List[Tuple[str, str]]]:
    current_url = ""
    ready_state = ""
    page_title = ""
    body_text = ""
    try:
        current_url = str(drv.current_url or "")
    except Exception:
        current_url = ""
    try:
        ready_state = str(drv.execute_script("return document.readyState") or "")
    except Exception:
        ready_state = ""
    try:
        page_title = str(drv.title or "")
    except Exception:
        page_title = ""
    try:
        body_text = drv.find_element(By.TAG_NAME, "body").text or ""
    except Exception:
        body_text = ""
    host = urlparse(url).netloc
    sources = _presence_sources_from_current_tab(drv, host=host)
    return current_url, ready_state, page_title, len(body_text.strip()), sources


async def _camoufox_collect_probe_snapshot(page, *, url: str) -> Tuple[str, str, str, int, List[Tuple[str, str]]]:
    current_url = ""
    ready_state = ""
    page_title = ""
    html = ""
    body_text = ""
    try:
        current_url = str(page.url or "")
    except Exception:
        current_url = ""
    try:
        ready_state = str(await _maybe_await(page.evaluate("() => document.readyState")) or "")
    except Exception:
        ready_state = ""
    try:
        page_title = str(await _maybe_await(page.title()) or "")
    except Exception:
        page_title = ""
    try:
        html = await _maybe_await(page.content()) or ""
    except Exception:
        html = ""
    try:
        body_text = str(await _maybe_await(page.locator("body").inner_text(timeout=5000)) or "")
    except Exception:
        body_text = ""
    visible = ""
    if html:
        try:
            soup = BeautifulSoup(html, "html.parser")
            visible = " ".join(soup.stripped_strings)
        except Exception:
            visible = ""
    sources: List[Tuple[str, str]] = []
    if body_text:
        sources.append(("dom_body_text", body_text))
    if visible:
        sources.append(("dom_visible_text", visible))
    if html:
        sources.append(("dom_html", html))
    return current_url, ready_state, page_title, len(body_text.strip()), sources


async def _camoufox_body_text(page) -> str:
    try:
        return str(await _maybe_await(page.locator("body").inner_text(timeout=5000)) or "")
    except Exception:
        return ""


# Explicit Camoufox acquisition modes (Winline poller). Default/None = legacy always-goto.
ACQUISITION_MODES = frozenset({"initial_goto", "dynamic_dom", "controlled_reload"})
_ACQUISITION_ERROR_MAX = 300
_DOM_SIGNATURE_MAX = 64
_PAGE_URL_DIAG_MAX = 500
# Current-map polling shares one browser worker across every live match. A
# default Playwright navigation timeout (60s) therefore blocks every following
# match. Keep bounded Winline acquisition below the monitor's 30s ceiling;
# legacy/non-Winline callers retain the historical 60s timeout.
WINLINE_BOUNDED_NAVIGATION_TIMEOUT_MS = 12_000

# Winline рисует ленту событий во ВНУТРЕННЕМ контейнере (`div.main`,
# `div.main__wrapper`): само тело страницы не скроллится (`document.body`
# scrollHeight равен innerHeight), поэтому `window.scrollTo(...)` ничего не
# двигает, а в DOM попадает только первая порция карточек. Прокрутка контейнера
# дорисовывает следующие; уже отрисованные при этом НЕ выгружаются — замер
# 05.08.2026 на живой странице: 9 блоков после загрузки, 12 после прокрутки,
# включая первые.
# Шагов столько, чтобы хватило на длинную ленту: у дотовской страницы к живым
# матчам снизу приклеена вся линия на завтра, и шести экранов до нижних карточек
# не хватало (05.08.2026 `RE ARISE — Team Synapse` не находился весь матч, хотя
# на сайте был). Выход из цикла — по остановке контейнера, по найденной паре
# (`probe`) или по бюджету времени, а не по числу шагов.
WINLINE_FEED_SWEEP_STEPS = 24
WINLINE_FEED_SWEEP_PAUSE_SECONDS = 0.2
WINLINE_FEED_SWEEP_BUDGET_SECONDS = 6.0
WINLINE_FEED_SWEEP_MIN_INTERVAL_SECONDS = 20.0
_FEED_SWEEP_JS = """() => {
  let moved = false;
  for (const el of document.querySelectorAll('*')) {
    if (el.scrollHeight - el.clientHeight > 300 && el.clientHeight > 300) {
      const before = el.scrollTop;
      el.scrollTop = before + Math.max(400, el.clientHeight);
      if (el.scrollTop > before) { moved = true; }
    }
  }
  window.scrollBy(0, 800);
  return moved;
}"""
# Прокрутка привязана к физической странице: поллер живёт на одной и той же
# сколь угодно долго, поэтому троттлим по id(page), а протухшие записи чистим.
_feed_sweep_last_run: Dict[int, float] = {}
_FEED_SWEEP_STATE_TTL_SECONDS = 600.0


def _feed_sweep_due(page, *, now: Optional[float] = None) -> bool:
    """True, если по этой странице прокрутку можно делать снова."""
    stamp = float(now if now is not None else time.monotonic())
    for key, last in list(_feed_sweep_last_run.items()):
        if stamp - last > _FEED_SWEEP_STATE_TTL_SECONDS:
            _feed_sweep_last_run.pop(key, None)
    last_run = _feed_sweep_last_run.get(id(page))
    if last_run is None:
        return True
    return (stamp - last_run) >= max(0.0, float(WINLINE_FEED_SWEEP_MIN_INTERVAL_SECONDS))


async def _sweep_camoufox_feed(
    page,
    *,
    force: bool = False,
    now: Optional[float] = None,
    probe: Any = None,
) -> bool:
    """Дорисовать ленту прокруткой внутреннего контейнера до конца.

    `probe` — необязательная проверка «искомое уже в DOM»: как только она
    возвращает True, прокрутка прекращается, чтобы не тратить шаги впустую.
    Возвращает True, если контейнер сдвигался — то есть снимок имеет смысл
    перечитать.
    """
    if page is None:
        return False
    stamp = float(now if now is not None else time.monotonic())
    if not force and not _feed_sweep_due(page, now=stamp):
        return False
    _feed_sweep_last_run[id(page)] = stamp
    started = time.monotonic()
    moved_any = False
    for _ in range(max(1, int(WINLINE_FEED_SWEEP_STEPS))):
        try:
            moved = bool(await _maybe_await(page.evaluate(_FEED_SWEEP_JS)))
        except Exception:
            break
        if not moved:
            break
        moved_any = True
        if probe is not None:
            try:
                if bool(await _maybe_await(probe())):
                    break
            except Exception:
                pass
        if (time.monotonic() - started) >= max(0.0, float(WINLINE_FEED_SWEEP_BUDGET_SECONDS)):
            break
        time.sleep(max(0.0, float(WINLINE_FEED_SWEEP_PAUSE_SECONDS)))
    return moved_any


def _bounded_dom_signature(text: str, *, max_len: int = _DOM_SIGNATURE_MAX) -> str:
    """Stable bounded SHA-256 of whitespace-normalized DOM text (no raw dumps)."""
    limit = max(8, min(int(max_len or _DOM_SIGNATURE_MAX), 128))
    normalized = " ".join(str(text or "").split())
    digest = hashlib.sha256(normalized.encode("utf-8", errors="replace")).hexdigest()
    return digest[:limit]


def _sanitize_acquisition_error(exc: BaseException | str, *, max_len: int = _ACQUISITION_ERROR_MAX) -> str:
    raw = str(exc or "").strip()
    if not raw:
        return ""
    # Drop accidental HTML blobs and bound length for poller diagnostics.
    if "<" in raw and ">" in raw:
        raw = re.sub(r"<[^>]+>", " ", raw)
    raw = " ".join(raw.split())
    return raw[: max(1, int(max_len or _ACQUISITION_ERROR_MAX))]


def _normalize_page_url(url: str) -> str:
    return str(url or "").strip().rstrip("/")


def _urls_equivalent(left: str, right: str) -> bool:
    a = _normalize_page_url(left)
    b = _normalize_page_url(right)
    return bool(a and b and a == b)


def _page_current_url(page) -> str:
    try:
        return str(getattr(page, "url", "") or "")
    except Exception:
        return ""


def _page_needs_navigation(page, target_url: str) -> bool:
    """True when page is blank/error/missing or not already on the target URL."""
    current = _page_current_url(page)
    cur_low = current.strip().lower()
    if not cur_low:
        return True
    if cur_low in {"about:blank", "about:error", "about:srcdoc"}:
        return True
    if cur_low.startswith("chrome-error:") or cur_low.startswith("data:"):
        return True
    return not _urls_equivalent(current, target_url)

async def _load_site_render_payload_camoufox_async(
    page,
    url: str,
    *,
    initial_wait_seconds: float = 7.0,
    scroll_wait_seconds: float = 2.0,
    acquisition_mode: Optional[str] = None,
) -> Tuple[str, str, str, str, str, Dict[str, Any]]:
    """Load or re-read page content for Camoufox parsers.

    acquisition_mode:
      - None: legacy always page.goto (default, preserves non-Winline behavior)
      - initial_goto: navigate only when blank/wrong/new URL
      - dynamic_dom: read live DOM only when already on expected URL; if blank/root/
        wrong-host/wrong URL, perform exactly one goto repair (no reload, no sleep)
      - controlled_reload: reload an already-correct page; repair via one goto
        when the page is already wrong or a failed reload redirects it

    Returns (load_status, load_error, html, visible, body_text, acquisition_diag).
    """
    started = time.monotonic()
    mode = str(acquisition_mode or "").strip().lower() or None
    if mode and mode not in ACQUISITION_MODES:
        mode = None
    navigation_timeout_ms = (
        WINLINE_BOUNDED_NAVIGATION_TIMEOUT_MS if mode else 60_000
    )

    load_status = "ok"
    load_error = ""
    acq_error = ""
    navigated = False

    try:
        if mode == "dynamic_dom":
            # Live DOM on the expected URL only. Blank/root/wrong URL is not a
            # stable miss: repair with exactly one goto, then read DOM. No reload,
            # no synthetic sleep (call budget is the caller's).
            if _page_needs_navigation(page, url):
                navigated = True
                try:
                    await _maybe_await(
                        page.goto(
                            url,
                            wait_until="domcontentloaded",
                            timeout=navigation_timeout_ms,
                        )
                    )
                except Exception as exc:
                    load_status = "partial_load"
                    acq_error = _sanitize_acquisition_error(exc)
                    load_error = acq_error
        elif mode == "controlled_reload":
            # Reload only an already-correct page.  Reload failures sometimes
            # redirect Winline to its root page; retrying reload there can never
            # recover the live market, so repair the existing named page with a
            # bounded goto instead.
            navigated = True
            if _page_needs_navigation(page, url):
                await _maybe_await(
                    page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=navigation_timeout_ms,
                    )
                )
            else:
                try:
                    await _maybe_await(
                        page.reload(
                            wait_until="domcontentloaded",
                            timeout=navigation_timeout_ms,
                        )
                    )
                except Exception as reload_exc:
                    if _page_needs_navigation(page, url):
                        try:
                            await _maybe_await(
                                page.goto(
                                    url,
                                    wait_until="domcontentloaded",
                                    timeout=navigation_timeout_ms,
                                )
                            )
                        except Exception as repair_exc:
                            load_status = "partial_load"
                            acq_error = _sanitize_acquisition_error(repair_exc)
                            load_error = acq_error
                    else:
                        load_status = "partial_load"
                        acq_error = _sanitize_acquisition_error(reload_exc)
                        load_error = acq_error
        elif mode == "initial_goto":
            if _page_needs_navigation(page, url):
                navigated = True
                await _maybe_await(
                    page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=navigation_timeout_ms,
                    )
                )
                time.sleep(max(0.0, float(initial_wait_seconds)))
                try:
                    await _maybe_await(page.evaluate(
                        "() => {"
                        " window.scrollTo(0, 0);"
                        " window.scrollTo(0, document.body.scrollHeight * 0.5);"
                        " window.scrollTo(0, document.body.scrollHeight);"
                        "}"
                    ))
                    time.sleep(max(0.0, float(scroll_wait_seconds)))
                except Exception:
                    pass
            # already on target: skip goto/reload
        else:
            # Legacy default: always navigate.
            navigated = True
            await _maybe_await(page.goto(url, wait_until="domcontentloaded", timeout=60000))
            time.sleep(max(0.0, float(initial_wait_seconds)))
            try:
                await _maybe_await(page.evaluate(
                    "() => {"
                    " window.scrollTo(0, 0);"
                    " window.scrollTo(0, document.body.scrollHeight * 0.5);"
                    " window.scrollTo(0, document.body.scrollHeight);"
                    "}"
                ))
                time.sleep(max(0.0, float(scroll_wait_seconds)))
            except Exception:
                pass
    except Exception as exc:
        load_status = "partial_load"
        acq_error = _sanitize_acquisition_error(exc)
        load_error = acq_error

    # После свежей навигации в DOM только первая порция карточек: прокрутки выше
    # адресованы окну, а лента Winline скроллится внутренним контейнером. Режимы
    # acquisition — это Winline, для остальных букмекеров поведение не меняется.
    if mode and navigated:
        with contextlib.suppress(Exception):
            await _sweep_camoufox_feed(page, force=True)

    html = ""
    visible = ""
    body_text = ""
    try:
        html = await _maybe_await(page.content()) or ""
    except Exception:
        html = ""
    if html:
        try:
            soup = BeautifulSoup(html, "html.parser")
            visible = " ".join(soup.stripped_strings)
        except Exception:
            visible = ""
    body_text = await _camoufox_body_text(page)

    page_url = _page_current_url(page) or str(url or "")
    if len(page_url) > _PAGE_URL_DIAG_MAX:
        page_url = page_url[:_PAGE_URL_DIAG_MAX]
    sig_source = body_text or visible or html or ""
    latency_ms = max(0.0, (time.monotonic() - started) * 1000.0)
    acquisition_diag: Dict[str, Any] = {
        "acquisition_mode": mode,  # None for legacy callers
        "page_url": page_url,
        "dom_signature": _bounded_dom_signature(sig_source),
        "acquisition_latency_ms": round(latency_ms, 3),
        "acquisition_error": acq_error or None,
    }

    return load_status, load_error, html, visible or body_text, body_text, acquisition_diag


def _apply_acquisition_diag(result: SiteResult, diag: Optional[Dict[str, Any]]) -> SiteResult:
    """Attach bounded acquisition diagnostics onto a SiteResult (in-place)."""
    if not diag:
        return result
    mode = diag.get("acquisition_mode")
    if mode is not None:
        result.acquisition_mode = str(mode)
    page_url = diag.get("page_url")
    if page_url is not None:
        result.page_url = str(page_url)[:_PAGE_URL_DIAG_MAX]
    dom_sig = diag.get("dom_signature")
    if dom_sig is not None:
        result.dom_signature = str(dom_sig)[:128]
    latency = diag.get("acquisition_latency_ms")
    if latency is not None:
        try:
            result.acquisition_latency_ms = float(latency)
        except (TypeError, ValueError):
            pass
    acq_err = diag.get("acquisition_error")
    if acq_err:
        result.acquisition_error = _sanitize_acquisition_error(str(acq_err))
    return result


async def _camoufox_try_click_text(page, text_candidates: List[str]) -> bool:
    script = """
    ([labels]) => {
      const normalize = (value) => String(value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
      const elements = Array.from(
        document.querySelectorAll(
          'button,a,[role="tab"],[role="button"],div,span,li'
        )
      );
      for (const rawLabel of labels) {
        const label = normalize(rawLabel);
        if (!label) continue;
        for (const el of elements) {
          const text = normalize(el.innerText || el.textContent || '');
          if (!text) continue;
          if (text === label || text.includes(label)) {
            el.scrollIntoView({block:'center'});
            el.click();
            return true;
          }
        }
      }
      return false;
    }
    """
    for label in list(text_candidates or []):
        if not str(label or "").strip():
            continue
        try:
            clicked = bool(await _maybe_await(page.evaluate(script, [[str(label)]])))
        except Exception:
            clicked = False
        if clicked:
            time.sleep(1.0)
            return True
    return False


async def _camoufox_click_map_tab_on_current_page(page, site: str, map_num: Optional[int]) -> bool:
    if map_num is None:
        return False
    labels: List[str] = []
    if site == "betboom":
        labels = [f"Карта {map_num}", f"Карта{map_num}", f"{map_num} карта"]
    elif site == "pari":
        labels = [f"{map_num}-Я КАРТА", f"{map_num}-я карта", f"{map_num} карта", f"Карта {map_num}"]
    elif site == "winline":
        labels = [
            f"{map_num}К",
            f"{map_num} К",
            f"{map_num}-я карта",
            f"{map_num} карта",
            f"Победитель {map_num} карты",
            f"Победитель {map_num} карт",
        ]
    return await _camoufox_try_click_text(page, labels)


async def _parse_map_market_on_current_camoufox_page_async(
    page,
    site: str,
    team1: str,
    team2: str,
    forced_map_num: Optional[int] = None,
) -> Tuple[List[float], str]:
    body_text = await _camoufox_body_text(page)
    map_num = _resolve_map_num_for_site(site, body_text, forced_map_num)
    clicked_tab = await _camoufox_click_map_tab_on_current_page(page, site, map_num)
    if not clicked_tab and site == "betboom" and map_num is not None:
        await _camoufox_try_click_text(page, [f"Карта {map_num}", f"Карта{map_num}", f"{map_num} карта"])
    elif not clicked_tab and site == "pari" and map_num is not None:
        await _camoufox_try_click_text(page, [f"{map_num}-Я КАРТА", f"{map_num}-я карта", f"{map_num} карта", f"Карта {map_num}"])
    elif not clicked_tab and site == "winline" and map_num is not None:
        await _camoufox_try_click_text(
            page,
            [f"{map_num}К", f"{map_num} К", f"{map_num}-я карта", f"{map_num} карта", f"Победитель {map_num} карты", f"Победитель {map_num} карт"],
        )
    body_text = await _camoufox_body_text(page)
    odds = _extract_map_odds_deeplink(
        site,
        " ".join((body_text or "").split()),
        team1,
        team2,
        forced_map_num=forced_map_num,
    )
    # Pari-specific: if no odds after first click, retry with stronger labels (uppercase) + reload
    if site == "pari" and not odds and map_num is not None:
        for attempt in range(3):
            time.sleep(1.5)
            await _camoufox_try_click_text(page, [f"{map_num}-Я КАРТА", f"Карта {map_num}"])
            time.sleep(1.5)
            body_text = await _camoufox_body_text(page)
            odds = _extract_map_odds_deeplink(
                site,
                " ".join((body_text or "").split()),
                team1,
                team2,
                forced_map_num=forced_map_num,
            )
            if odds:
                break
            # Reload and retry on last attempt
            if attempt == 2:
                with contextlib.suppress(Exception):
                    await _maybe_await(page.reload(wait_until="domcontentloaded", timeout=30000))
                    time.sleep(2.5)
                    body_text = await _camoufox_body_text(page)
                    odds = _extract_map_odds_deeplink(
                        site,
                        " ".join((body_text or "").split()),
                        team1,
                        team2,
                        forced_map_num=forced_map_num,
                    )
    return odds, body_text


async def _camoufox_find_match_by_urls_async(page, site: str, urls: List[str], team1: str, team2: str) -> Optional[str]:
    if not urls:
        return None
    t1 = (team1 or "").strip().lower()
    t2 = (team2 or "").strip().lower()
    t1s = t1.split()[0] if t1 else ""
    t2s = t2.split()[0] if t2 else ""
    for target in urls[:12]:
        try:
            await _maybe_await(page.goto(target, wait_until="domcontentloaded", timeout=25000))
            time.sleep(1.5)
            body = " ".join((await _maybe_await(page.locator("body").inner_text(timeout=4000)) or "").lower().split())
        except Exception:
            continue
        if (
            (t1 and t1 in body and t2 and t2 in body)
            or (t1s and t1s in body and t2s and t2s in body)
            or _text_matches_teams(body, team1, team2)
        ):
            return target
    return None


async def _probe_presence_site_in_camoufox_page(
    page,
    *,
    site: str,
    url: str,
    team1: str,
    team2: str,
    mode: str,
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
) -> SiteResult:
    try:
        await _maybe_await(page.goto(url, wait_until="domcontentloaded", timeout=30000))
        time.sleep(2.0)
        try:
            await _maybe_await(page.evaluate(
                "() => { window.scrollTo(0, 0); window.scrollTo(0, document.body.scrollHeight * 0.5); window.scrollTo(0, document.body.scrollHeight); }"
            ))
        except Exception:
            pass
        time.sleep(1.0)
    except Exception as exc:
        return SiteResult(
            site=site,
            url=url,
            status="request_error",
            match_found=False,
            odds=[],
            source="camoufox_goto_error",
            details=str(exc),
            market_closed=False,
            match_odds=[],
        )

    current_url, ready_state, page_title, body_len, sources = await _camoufox_collect_probe_snapshot(
        page,
        url=url,
    )
    found, source_name, details = _find_presence_from_sources(
        team1,
        team2,
        sources,
        team1_aliases=team1_aliases,
        team2_aliases=team2_aliases,
    )

    status = "ok"
    if not sources:
        status = "loading"
    elif ready_state and ready_state != "complete":
        status = "loading"
    elif body_len < 240:
        status = "loading"

    if (
        not found
        and _presence_should_open_match_details(
            site,
            current_url or url,
            body_len=body_len,
            source_count=len(sources),
        )
    ):
        html_text = ""
        for source_name_candidate, text in sources:
            if source_name_candidate == "dom_html" and text:
                html_text = text
                break
        candidate_urls = _candidate_match_urls_from_html(site, current_url or url, html_text)
        opened_match_url = await _camoufox_find_match_by_urls_async(page, site, candidate_urls, team1, team2) or ""
        if opened_match_url:
            current_url, ready_state, page_title, body_len, sources = await _camoufox_collect_probe_snapshot(
                page,
                url=url,
            )
            found, source_name, details = _find_presence_from_sources(
                team1,
                team2,
                sources,
                team1_aliases=team1_aliases,
                team2_aliases=team2_aliases,
            )
            status = "ok"
            if not sources:
                status = "loading"
            elif ready_state and ready_state != "complete":
                status = "loading"
            elif body_len < 240:
                status = "loading"

    details = details or "match not found in rendered DOM payload"
    meta_bits = []
    if current_url:
        meta_bits.append(f"current_url={current_url[:220]}")
    if ready_state:
        meta_bits.append(f"ready_state={ready_state}")
    if page_title:
        meta_bits.append(f"title={page_title[:160]}")
    meta_bits.append(f"sources={len(sources)}")
    meta_bits.append(f"body_len={body_len}")
    if meta_bits:
        details = f"{details} | {'; '.join(meta_bits)}"

    if found:
        return SiteResult(
            site=site,
            url=url,
            status=status,
            match_found=True,
            odds=[],
            source=source_name or "presence_found_camoufox",
            details=details,
            market_closed=False,
            match_odds=[],
        )

    return SiteResult(
        site=site,
        url=url,
        status=status,
        match_found=False,
        odds=[],
        source="presence_missing",
        details=details,
        market_closed=False,
        match_odds=[],
    )


async def _run_presence_sites_in_camoufox(
    *,
    selected_sites: List[str],
    urls: Dict[str, str],
    team1: str,
    team2: str,
    mode: str,
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
) -> List[SiteResult]:
    if not CAMOUFOX_AVAILABLE:
        return _run_presence_sites_in_browser(
            selected_sites=selected_sites,
            urls=urls,
            team1=team1,
            team2=team2,
            mode=mode,
            team1_aliases=team1_aliases,
            team2_aliases=team2_aliases,
        )

    proxy_kwargs = _camoufox_proxy_kwargs(BOOKMAKER_PROXY_URL)
    results: List[SiteResult] = []
    with camoufox.Camoufox(headless=True, **proxy_kwargs) as browser:
        for site in selected_sites:
            page = await _maybe_await(browser.new_page())
            try:
                results.append(
                    await _probe_presence_site_in_camoufox_page(
                        page,
                        site=site,
                        url=urls[site],
                        team1=team1,
                        team2=team2,
                        mode=mode,
                        team1_aliases=team1_aliases,
                        team2_aliases=team2_aliases,
                    )
                )
            finally:
                with contextlib.suppress(Exception):
                    await _maybe_await(page.close())
    return results


def _run_presence_sites_in_camoufox_sync(
    *,
    selected_sites: List[str],
    urls: Dict[str, str],
    team1: str,
    team2: str,
    mode: str,
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
) -> List[SiteResult]:
    """Run sync Camoufox outside the caller's asyncio loop."""
    if not CAMOUFOX_AVAILABLE:
        return _run_presence_sites_in_browser(
            selected_sites=selected_sites,
            urls=urls,
            team1=team1,
            team2=team2,
            mode=mode,
            team1_aliases=team1_aliases,
            team2_aliases=team2_aliases,
        )

    proxy_kwargs = _camoufox_proxy_kwargs(BOOKMAKER_PROXY_URL)
    results: List[SiteResult] = []
    with camoufox.Camoufox(headless=True, **proxy_kwargs) as browser:
        for site in selected_sites:
            page = browser.new_page()
            try:
                results.append(
                    _run_coroutine_blocking(
                        _probe_presence_site_in_camoufox_page(
                            page,
                            site=site,
                            url=urls[site],
                            team1=team1,
                            team2=team2,
                            mode=mode,
                            team1_aliases=team1_aliases,
                            team2_aliases=team2_aliases,
                        )
                    )
                )
            finally:
                with contextlib.suppress(Exception):
                    page.close()
    return results


def _current_page_matches_teams(
    drv,
    team1: str,
    team2: str,
    *,
    attempts: int = 3,
    delay: float = 0.8,
) -> bool:
    for attempt in range(max(1, int(attempts))):
        try:
            body_text = " ".join(drv.find_element(By.TAG_NAME, "body").text.split())
        except Exception:
            body_text = ""
        if _text_matches_teams(body_text, team1, team2):
            return True
        if attempt + 1 < max(1, int(attempts)):
            time.sleep(max(0.0, float(delay)))
    return False


# Живая карточка Winline всегда несёт СТАТУС игры: слитное `2карта`, счётчик
# сыгранных карт `2К`, игровой таймер `18'` или счёт в скобках. Эти маркеры у
# линии не встречаются.
_LIVE_STRONG_MARKER_RE = re.compile(
    r"\b\d{1,2}'|\(\d{1,2}\s*-\s*\d{1,2}\)|\b[1-5]карта\b|\b[1-5]\s*к\b"
)
# Слабые маркеры двусмысленны: `1 карта` (с пробелом) — это НАЗВАНИЕ РЫНКА, оно
# есть и у предматчевой карточки; `15:00` одинаково читается и как счёт, и как
# время начала. Они не доказывают live.
_LIVE_WEAK_MARKER_RE = re.compile(r"\b[1-5]\s*карта\b|\b\d{1,2}\s*:\s*\d{1,2}\b")
# Часы старта у карточки линии: `Завтра 15:00`, `Сегодня 21:00`.
_PREMATCH_CLOCK_RE = re.compile(
    r"(?:завтра|сегодня|tomorrow|today)\s*\d{1,2}\s*:\s*\d{2}"
)


def _looks_future_context(context: str) -> bool:
    """True для карточки линии (предматч), False для живой карточки.

    Раньше live-маркером считались и `1 карта`, и любое `\\d+:\\d+`. На дотовской
    странице Winline live-карточки соседствуют с линией на завтра, у которой ЕСТЬ
    и рынок `1 карта` с кэфами, и часы старта с двоеточием, — то есть обе
    «улики» предматч сам себе и создавал, и опровергал. Поэтому часы старта
    отменяются только СИЛЬНЫМИ маркерами живой игры.
    """
    low = context.lower()
    strong_live = bool(_LIVE_STRONG_MARKER_RE.search(low))
    weak_live = bool(_LIVE_WEAK_MARKER_RE.search(low))
    if _PREMATCH_CLOCK_RE.search(low):
        return not strong_live
    if any(m in low for m in FUTURE_MARKERS):
        return not (strong_live or weak_live)
    # Explicit future date token like 18.03.26 close to teams is usually prematch.
    if re.search(r"\b\d{1,2}\.\d{2}\.\d{2}\b", low):
        return not (strong_live or weak_live)
    return False


def _looks_map_context(context: str) -> bool:
    low = (context or "").lower()
    return any(marker in low for marker in MAP_MARKERS)


def _snippet_by_teams(
    text: str,
    team1: str,
    team2: str,
    radius: int = 900,
    max_team_distance: int = 1200,
) -> Optional[str]:
    low = text.lower()
    t1 = str(team1 or "")
    t2 = str(team2 or "")
    pos1 = _find_positions_with_fallback(low, t1)
    pos2 = _find_positions_with_fallback(low, t2)
    if not pos1 or not pos2:
        return None

    best_i1 = -1
    best_i2 = -1
    best_dist = 10**9
    for i1 in pos1:
        for i2 in pos2:
            d = abs(i1 - i2)
            if d < best_dist:
                best_dist = d
                best_i1 = i1
                best_i2 = i2

    if best_i1 < 0 or best_i2 < 0 or best_dist > max_team_distance:
        return None

    center = (best_i1 + best_i2) // 2
    lo = max(0, center - radius)
    hi = min(len(text), center + radius)
    sn = re.sub(r"\s+", " ", text[lo:hi]).strip()
    low_sn = sn.lower()
    # Проверяем теми же правилами, что и поиск позиций: подстрока без границ
    # слова принимала `DOWN` внутри `countdown` и склеивала чужие карточки.
    has_t1 = bool(_find_positions_with_fallback(low_sn, t1))
    has_t2 = bool(_find_positions_with_fallback(low_sn, t2))
    if not has_t1 or not has_t2:
        return None
    return sn




@dataclass
class SiteResult:
    site: str
    url: str
    status: str
    match_found: bool
    odds: List[float]
    source: str
    details: str
    market_closed: bool = False
    match_odds: List[float] = field(default_factory=list)
    # Optional strict current-map provenance (backward-compatible defaults).
    market_kind: Optional[str] = None
    map_num: Optional[int] = None
    p1_team: Optional[str] = None
    p2_team: Optional[str] = None
    # Сырой порядок карточки Winline: имена в порядке карточки и НЕразвёрнутая
    # пара цен. Нужны, чтобы неверную сторону можно было доказать по одному
    # снимку, а не сверкой с параллельным парсером.
    card_team_order: Optional[str] = None
    card_odds: List[float] = field(default_factory=list)
    # Почему пары не оказалось в снимке (заполняется только на промахе).
    miss_fingerprint: Optional[str] = None
    # Optional acquisition diagnostics for Winline dynamic poller (bounded).
    acquisition_mode: Optional[str] = None
    page_url: Optional[str] = None
    dom_signature: Optional[str] = None
    acquisition_latency_ms: Optional[float] = None
    acquisition_error: Optional[str] = None


def _extract_current_map_num(text: str) -> Optional[int]:
    low = " ".join((text or "").lower().split())
    if not low:
        return None

    live_score_match = re.search(
        r"\b\d{1,2}:\d{2}\b\s+(\d)\s*:\s*(\d)(?:\s*\(\d{1,2}\s*-\s*\d{1,2}\))?",
        low,
    )
    if live_score_match:
        try:
            inferred = int(live_score_match.group(1)) + int(live_score_match.group(2)) + 1
        except Exception:
            inferred = None
        if inferred is not None and 1 <= inferred <= 5:
            return inferred

    candidates: List[Tuple[int, int, int]] = []

    patterns = [
        # strongest: explicit Russian ordinal map labels
        r"\b([1-5])\s*-\s*я\s*карта\b",
        r"\b([1-5])\s*я\s*карта\b",
        # winline shorthand: "1К"
        r"\b([1-5])\s*к\b",
        # compact form: "1карта"
        r"\b([1-5])карта\b",
    ]

    for pat_idx, pat in enumerate(patterns):
        for m in re.finditer(pat, low):
            try:
                map_num = int(m.group(1))
            except Exception:
                continue
            score = 10 - pat_idx
            window = low[max(0, m.start() - 25): min(len(low), m.end() + 35)]
            # If map marker is close to game timer (e.g. "1-я карта 08:55"), prefer it.
            if re.search(r"\b\d{1,2}:\d{2}\b", window):
                score += 8
            # Penalize static tabs list like "... карта 1 карта 2 карта 3 ..."
            if re.search(r"карта\s*1\s*карта\s*2", window):
                score -= 5
            candidates.append((score, m.start(), map_num))

    if not candidates:
        return None
    candidates.sort(key=lambda x: (-x[0], x[1]))
    return candidates[0][2]


def _extract_market_map_num(site: str, text: str) -> Optional[int]:
    flat = " ".join((text or "").split())
    low = flat.lower()
    patterns: List[str] = []
    if site == "betboom":
        patterns = [
            r"исход\s+карта\s*([1-5])",
            r"тотал убийств на карте\s+карта\s*([1-5])",
            r"тотал команды на карте\s+карта\s*([1-5])",
            r"\b([1-5])\s*-\s*я\s*карта\b",
        ]
    elif site == "pari":
        patterns = [
            r"исход\s+([1-5])\s*-\s*й\s*карт[аы]",
            r"тотал на\s+([1-5])\s*-\s*й\s*карт[ае]",
            r"победа и тотал на\s+([1-5])\s*-\s*й\s*карт[ае]",
            r"\b([1-5])\s*-\s*я\s*карта\b",
        ]
    elif site == "winline":
        patterns = [
            r"популярные на карту.*?победитель\s+([1-5])\s*карта",
            r"победитель\s+([1-5])\s*карт[аы]",
            r"\b([1-5])\s*карта\b",
            r"\b([1-5])\s*к\b",
        ]
    for pattern in patterns:
        m = re.search(pattern, low, re.I | re.S)
        if not m:
            continue
        try:
            value = int(m.group(1))
        except Exception:
            continue
        if 1 <= value <= 5:
            return value
    return None


def _normalize_map_num(value: Optional[int]) -> Optional[int]:
    try:
        v = int(value) if value is not None else None
    except Exception:
        return None
    if v is None:
        return None
    if 1 <= v <= 5:
        return v
    return None


def _resolve_map_num(text: str, forced_map_num: Optional[int]) -> Optional[int]:
    forced = _normalize_map_num(forced_map_num)
    if forced is not None:
        return forced
    return _extract_current_map_num(text)


def _resolve_map_num_for_site(site: str, text: str, forced_map_num: Optional[int]) -> Optional[int]:
    forced = _normalize_map_num(forced_map_num)
    if forced is not None:
        return forced
    market_map = _extract_market_map_num(site, text)
    if market_map is not None:
        return market_map
    return _extract_current_map_num(text)


def _try_click_text(drv, text_candidates: List[str]) -> bool:
    for label in text_candidates:
        xps = [
            f"//*[contains(normalize-space(text()), '{label}')]",
            f"//*[contains(., '{label}')]",
        ]
        for xp in xps:
            els = drv.find_elements(By.XPATH, xp)
            for el in els[:5]:
                try:
                    drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    time.sleep(0.1)
                    el.click()
                    time.sleep(1.2)
                    return True
                except Exception:
                    continue
    return False


def _try_click_xpath(drv, xpath_candidates: List[str]) -> bool:
    for xp in xpath_candidates:
        try:
            els = drv.find_elements(By.XPATH, xp)
        except Exception:
            continue
        for el in els[:8]:
            if _safe_click(el, drv):
                return True
    return False



@dataclass
class _WinlineMapExtract:
    odds: List[float] = field(default_factory=list)
    market_closed: bool = False
    reason: str = ""
    map_num: Optional[int] = None
    market_kind: Optional[str] = None
    p1_team: Optional[str] = None
    p2_team: Optional[str] = None
    details: str = ""
    # True — кэфы взяты из рынка «Матч» на ПОСЛЕДНЕЙ карте серии, где победитель
    # карты и победитель матча — одно событие. Провенанс обязателен: во всех
    # остальных случаях подставлять матчевые кэфы в поток карты запрещено.
    promoted_from_match: bool = False
    # Сырой порядок так, как его написал Winline: имена (наши, из SourceTV) в
    # порядке карточки и НЕразвёрнутая пара цен. `odds` уже приведены к порядку
    # запроса, поэтому по ним одним нельзя доказать, что сторона не уехала;
    # с этой парой доказательство помещается в один файл evidence.
    card_team_order: Optional[str] = None
    card_odds: Optional[List[float]] = None


_WINLINE_EVENT_BOUNDARY_RE = re.compile(
    r"(?:^|\s)(?:dota\s*2|counter[-\s]*strike(?:\s*2)?|cs(?:\s*2)?|lol|"
    r"valorant|mobile\s+legends|king\s+of\s+glory)\s*\|",
    re.I,
)


_WINLINE_LIVE_CARD_LABEL_RE = re.compile(r"\b[1-5]\s*карта\s*\d+\s*['\u2032]", re.I)


# Строка матч-рынка: в карточке живого события она ровно одна, поэтому её счёт —
# надёжная граница карточки там, где заголовок дисциплины общий для нескольких
# матчей подряд. Регистр значим: со строчной 'матч' входит в витринные
# формулировки ('Тотал матч', 'Популярные на матч'), которые границами не являются.
_WINLINE_MATCH_ROW_LABEL_RE = re.compile(r"\bМатч\b")


_WINLINE_PRICE_PAIR_RE = re.compile(r"[0-9]+[.,][0-9]+\s+[0-9]+[.,][0-9]+")


# Шапка живой карточки пишет номер карты СЛИТНО (`BB TEAM TEAM LIQUID 2карта +15`),
# а строка рынка — раздельно (`2 карта 1.44 2.67`). Это и есть признак начала
# нового события в плоском тексте страницы: заголовок турнира общий для нескольких
# матчей подряд и границей события быть не может.
_WINLINE_CARD_HEADER_MARKER_RE = re.compile(r"(?<![0-9])[1-5]карта(?![а-яё])", re.I)

# Хвост предыдущей карточки: цены, прочерки-заполнители, разделитель дисциплины.
# Названия команд следующей карточки начинаются после них.
_WINLINE_CARD_TAIL_TOKEN_RE = re.compile(r"(?:[0-9]+[.,][0-9]+|[-—–]|\|)")


def _winline_token_is_name_like(token: str) -> bool:
    """Токен похож на часть названия команды: буквы только в верхнем регистре."""
    letters = [char for char in token if char.isalpha()]
    if not letters:
        # Цифры и знаки (`+16`, `27'`, `0`) встречаются и внутри шапки карточки.
        return True
    return all(char.isupper() for char in letters)


def _winline_card_start(flat: str, floor: int, marker_start: int) -> int:
    """Начало карточки: блок названий команд перед слитным маркером карты."""
    head = flat[floor:marker_start]
    if not head.strip():
        return floor
    tail = None
    for match in _WINLINE_CARD_TAIL_TOKEN_RE.finditer(head):
        tail = match
    lower_bound = floor + (tail.end() if tail else 0)
    tokens = list(re.finditer(r"\S+", flat[lower_bound:marker_start]))
    for index, token in enumerate(tokens):
        if all(_winline_token_is_name_like(item.group()) for item in tokens[index:]):
            # Название турнира набрано не капсом (`Games of the Future 2`),
            # поэтому остаётся снаружи карточки и не путает порядок команд.
            return lower_bound + token.start()
    return lower_bound


def _winline_event_boundaries(flat: str) -> List[int]:
    """Позиции начала карточек: заголовок дисциплины плюс шапка каждого события."""
    boundaries = {match.start() for match in _WINLINE_EVENT_BOUNDARY_RE.finditer(flat)}
    floor = 0
    for match in _WINLINE_CARD_HEADER_MARKER_RE.finditer(flat):
        boundaries.add(_winline_card_start(flat, floor, match.start()))
        floor = match.end()
    return sorted(boundaries)


def _winline_price_bearing_children(node) -> int:
    """How many direct child subtrees carry a price pair (i.e. look like markets)."""
    count = 0
    for child in getattr(node, "children", []):
        if getattr(child, "name", None) is None:
            continue
        if _WINLINE_PRICE_PAIR_RE.search(" ".join(child.stripped_strings)):
            count += 1
    return count


def _winline_single_card_scope(text: str) -> bool:
    """True while the container still spans a single Winline event card."""
    if not text:
        return False
    if len(_WINLINE_EVENT_BOUNDARY_RE.findall(text)) > 1:
        return False
    if len(_WINLINE_LIVE_CARD_LABEL_RE.findall(text)) > 1:
        return False
    if len(_WINLINE_MATCH_ROW_LABEL_RE.findall(text)) > 1:
        # Несколько строк матч-рынка = несколько матчей подряд под общим
        # заголовком турнира. Именно так узел с тремя матчами отдавал кэфы соседа.
        return False
    return True


def _winline_market_row_re(map_num: int) -> "re.Pattern[str]":
    """Row of the exact current-map winner market: marker followed by two prices."""
    n = int(map_num)
    return re.compile(
        rf"(?:победитель\s*{n}\s*карт[аы]|\b{n}\s*карта\b|\b{n}\s*к\b)"
        rf"\s*([0-9]+[.,][0-9]+|—|-|–)\s*([0-9]+[.,][0-9]+|—|-|–)",
        re.I,
    )


def _winline_matched_card_context(
    text: str,
    team1: str,
    team2: str,
    *,
    html: str = "",
    map_num: Optional[int] = None,
) -> Optional[str]:
    """Return only the Winline event segment containing both requested teams."""
    if html:
        try:
            soup = BeautifulSoup(html, "html.parser")
            market_re = _winline_market_row_re(map_num) if map_num else None
            candidates: List[Tuple[int, Any, str]] = []
            for element in soup.find_all(True):
                card_text = " ".join(element.stripped_strings)
                if not card_text or not _text_matches_teams(card_text, team1, team2):
                    continue
                candidates.append((len(card_text), element, card_text))
            if candidates:
                # Сортировка стабильная: при равной длине сохраняется порядок
                # документа, поэтому fail-closed возврат ниже — тот же текст, что
                # отдавался до правки.
                candidates.sort(key=lambda item: item[0])
                base_text = candidates[0][2]
                # Пара команд встречается на странице во многих элементах: карточка
                # в списке, строка в закреплённом баре, витрина. Подъём удаётся не
                # от каждого — от бара он приходит в контейнер с навигацией, где
                # предохранители обязаны отказать. Поэтому пробуем всех кандидатов
                # от самых узких к широким и берём первую карточку, прошедшую те же
                # предохранители. Ограничивать список нельзя: иначе непроверенный
                # хвост превращается в ложное доказательство отсутствия рынка.
                for _cand_len, element, _cand_text in candidates:
                    # Climb from the proven container until the requested-map market
                    # row appears. The first container that carries the row wins, but
                    # only while it still describes a single Winline card: more than
                    # one price-bearing child subtree means we swallowed a neighbour.
                    node = element
                    while node is not None:
                        node_text = " ".join(node.stripped_strings)
                        if market_re is not None:
                            hit = bool(market_re.search(node_text))
                        else:
                            hit = bool(
                                re.search(r"(?:\b[1-5]\s*к\b|\b[1-5]\s*карта\b)", node_text, re.I)
                            )
                        if hit:
                            node_name = str(getattr(node, "name", "") or "").lower()
                            node_classes = {
                                str(value or "").strip().lower()
                                for value in (node.get("class") or [])
                            }
                            # The expanded selected-event panel is itself the
                            # hard event boundary. It legitimately contains
                            # several price-bearing child markets (match,
                            # current-map winner, totals), unlike a list
                            # container that swallowed neighbouring cards.
                            selected_event_panel = bool(
                                node_name.startswith("ww-feature-event-live-center")
                                or "event-live-center" in node_classes
                            )
                            if (
                                _winline_single_card_scope(node_text)
                                and (
                                    selected_event_panel
                                    or _winline_price_bearing_children(node) <= 1
                                )
                            ):
                                return node_text
                            break
                        node = node.parent
                # Ни у одного кандидата строка рынка нужной карты не лежит внутри
                # границ его собственной карточки. Это и есть доказанное отсутствие
                # рынка на странице: кэфы оставляем пустыми (fail closed), а не
                # заимствуем у соседней карточки.
                return base_text
        except Exception:
            pass

    flat = " ".join((text or "").split())
    if not flat or not team1 or not team2:
        return None
    low = flat.lower()
    positions1 = _find_positions_with_fallback(low, team1)
    positions2 = _find_positions_with_fallback(low, team2)
    if not positions1 or not positions2:
        return None
    pair = min(
        ((abs(i1 - i2), min(i1, i2), max(i1, i2)) for i1 in positions1 for i2 in positions2),
        default=None,
    )
    if pair is None:
        return None
    _, pair_start, pair_end = pair
    boundaries = _winline_event_boundaries(flat)
    if not boundaries:
        return flat
    card_start = max((pos for pos in boundaries if pos <= pair_start), default=0)
    card_end = min((pos for pos in boundaries if pos > pair_end), default=len(flat))
    card = flat[card_start:card_end].strip()
    if not card or not _text_matches_teams(card, team1, team2):
        return None
    if not _winline_single_card_scope(card):
        # Кусок всё ещё накрывает соседние матчи: отдать его — значит отдать
        # чужую строку рынка. Именно так запрос `REKONIX vs L1GA TEAM`
        # 31.07.2026 получал кэфы карточки `ENJOY GLYPH`.
        return None
    return card


# Заголовок «дисциплина + турнир»: в ленте он пишется через `|`
# (`DOTA 2 | 1w Essence`), а в закреплённой карточке сверху — через запятую
# (`DOTA 2, 1w Essence`). Вариант с запятой не был границей карточки, поэтому
# название турнира попадало внутрь текста события.
_WINLINE_DISCIPLINE_HEADER_RE = re.compile(
    r"^\s*(?:dota\s*2|counter[-\s]*strike(?:\s*2)?|cs(?:\s*2)?|lol|"
    r"valorant|mobile\s+legends|king\s+of\s+glory)\s*[|,]\s*",
    re.I,
)


def _winline_strip_discipline_header(text: str) -> str:
    """Убрать ведущий заголовок дисциплины и название турнира перед командами.

    Турнир может содержать токен, равный названию команды: у `DOTA 2, 1w Essence`
    токен `1w` — это лига, а не команда `1w`, и порядок сторон определялся по
    ней. Итог 05.08.2026: `Team Liquid` при лидерстве +6158 нетворса уезжал в
    сообщение аутсайдером (3.13 против 1.33). Название турнира набрано НЕ капсом,
    поэтому режем до первого «капсового» токена — он уже принадлежит команде.
    """
    if not text:
        return text
    header = _WINLINE_DISCIPLINE_HEADER_RE.match(text)
    if header is None:
        return text
    rest = text[header.end():]
    for token in re.finditer(r"\S+", rest):
        if _winline_token_is_name_like(token.group()):
            return rest[token.start():]
    return rest


def _winline_team_order(text: str, team1: str, team2: str) -> Optional[str]:
    """Return 'direct', 'reverse', or None when order is ambiguous."""
    if not text or not team1 or not team2:
        return None
    low = _winline_strip_discipline_header(text).lower()
    i1 = _first_index_with_fallback(low, team1)
    i2 = _first_index_with_fallback(low, team2)
    if i1 == -1 or i2 == -1:
        return None
    if i1 == i2:
        return None
    return "direct" if i1 < i2 else "reverse"


def _winline_map_marker_patterns(map_num: int) -> List[str]:
    n = int(map_num)
    return [
        rf"победитель\s*{n}\s*карт[аы]",
        rf"\b{n}\s*карта\b",
        rf"\b{n}\s*к\b",
    ]


# Winline размечает недоступность исхода ТОЛЬКО классом кнопки: в CSS все три
# состояния получают `pointer-events: none`, а `_locked` вдобавок сохраняет
# видимое число — поэтому в тексте страницы заморозка неотличима от рабочего
# рынка, и текстовый детектор (LOCK_MARKERS) её принципиально не видит.
_WINLINE_UNBETTABLE_BUTTON_CLASSES = frozenset(
    {
        "coefficient-button_locked",
        "coefficient-button--is-blank",
        "coefficient-button_empty",
        "coef-btn--locked",
    }
)

_WINLINE_PERIOD_NAME_CLASS = "period-name"
_WINLINE_COEFF_BUTTON_CLASS = "coefficient-button"

# Класс рынка «победитель» (исход из двух). Тот же контейнер `card__coeffs`
# несёт фору (`_handicap2`), тотал (`_total2`) и ячейки-заполнители под
# непредложенные рынки — `coefficient-button_empty` БЕЗ класса рынка. На живой
# странице заполнителей большинство (54 из 90 кнопок в дампе), поэтому общий
# `any(...)` по всему контейнеру объявлял замороженным почти каждый рабочий
# рынок: карточка с живым 1.14/4.80 читалась как locked из-за пустых ячеек
# соседних колонок. Судить о доступности ставки можно ТОЛЬКО по кнопкам того
# рынка, из которого разобраны кэфы.
_WINLINE_WINNER_MARKET_BUTTON_CLASS = "coefficient-button_generic2"

# Рынок из ТРЁХ исходов (с ничьей): на Bo2 «Матч» рисуется именно так. Победитель
# карты — исход из двух, поэтому трёхисходный рынок подставлять вместо него нельзя.
# Проверено на живой странице 02.08.2026: у VICI GAMING vs OG (Bo2, карта 2)
# `Матч 5.81 1.10 -` набран классом `_generic3`, а строка `2 карта 5.87 1.11` —
# `_generic2`.
_WINLINE_THREE_WAY_MARKET_BUTTON_CLASS = "coefficient-button_generic3"


def _winline_node_classes(node: Any) -> set:
    raw = node.get("class") if hasattr(node, "get") else None
    if not raw:
        return set()
    if isinstance(raw, str):
        return {raw}
    return {str(item) for item in raw}


def _winline_button_is_unbettable(button: Any) -> bool:
    return bool(_winline_node_classes(button) & _WINLINE_UNBETTABLE_BUTTON_CLASSES)


# Классы кнопки исхода в обеих живых разметках: карточка ленты рисует
# `coefficient-button`, панель выбранного события — `odd-btn` (её модификатор
# заморозки в том же компоненте называется `coef-btn--locked`).
_WINLINE_PRICE_BUTTON_CLASSES = frozenset(
    {
        "coefficient-button",
        "odd-btn",
        "coef-btn",
    }
)


def _winline_price_node_unbettable(node: Any) -> bool:
    """Заморожена ли кнопка, с которой снято число.

    Судим ТОЛЬКО по узлу, несущему цену, и по кнопкам внутри него. Соседние
    ячейки контейнера сюда не входят намеренно: `any(...)` по всему
    `card__coeffs` однажды уже объявлял замороженным почти каждый рабочий рынок
    из-за заполнителей `coefficient-button_empty` соседних колонок.
    """
    if _winline_button_is_unbettable(node):
        return True
    for inner in node.find_all(True):
        if not (_winline_node_classes(inner) & _WINLINE_PRICE_BUTTON_CLASSES):
            continue
        if _winline_button_is_unbettable(inner):
            return True
    return False


def _winline_winner_market_buttons(container: Any) -> List[Any]:
    """Кнопки рынка «победитель карты» внутри контейнера коэффициентов.

    Фора, тотал и пустые ячейки-заполнители сюда не попадают: их состояние
    ничего не говорит о том, принимает ли БК ставку на победителя.
    """
    return [
        node
        for node in container.find_all(
            lambda tag: _WINLINE_COEFF_BUTTON_CLASS in _winline_node_classes(tag)
        )
        if _WINLINE_WINNER_MARKET_BUTTON_CLASS in _winline_node_classes(node)
    ]


_WINLINE_MATCH_MARKET_LABEL_RE = re.compile(r"^\s*матч\s*$", re.I)


def _winline_map_row_present(text: str, map_num: int) -> bool:
    """Есть ли в тексте подпись рынка запрошенной карты (пусть и без цен).

    Только подпись РЫНКА: она пишется с пробелом (`3 карта 1.87 1.83`). Шапка
    живой карточки (`3карта 28'`) и счётчик по картам (`3К 39 30`) подписями
    рынка не являются — иначе карточка, где рынка карты нет вовсе, выглядела бы
    как карточка с рынком.
    """
    return bool(
        re.search(
            rf"(?:победитель\s*{int(map_num)}\s*карт[аы]|\b{int(map_num)}\s+карта\b)",
            text or "",
            re.I,
        )
    )


def _winline_match_market_winner_prices(scope: Any) -> Optional[List[float]]:
    """Две цены рынка «Матч» внутри карточки — только если исходов ровно два.

    Трёхисходный рынок (`_generic3`, с ничьей) не годится: победитель карты —
    исход из двух. На Bo2 «Матч» рисуется именно трёхисходным.
    """
    for label in scope.find_all(True):
        if not _WINLINE_MATCH_MARKET_LABEL_RE.match(" ".join(label.stripped_strings) or ""):
            continue
        containers = [
            candidate
            for candidate in (label.find_next_sibling(), label.parent)
            if candidate is not None
        ]
        for container in containers:
            if any(
                _WINLINE_THREE_WAY_MARKET_BUTTON_CLASS in _winline_node_classes(node)
                for node in container.find_all(True)
            ):
                return None
            buttons = _winline_winner_market_buttons(container)
            if len(buttons) != 2 or any(_winline_button_is_unbettable(b) for b in buttons):
                continue
            prices: List[float] = []
            for button in buttons:
                match = re.search(
                    r"(?<!\d)([0-9]+[.,][0-9]+)(?!\d)",
                    " ".join(button.stripped_strings),
                )
                if not match:
                    prices = []
                    break
                price = float(match.group(1).replace(",", "."))
                if price <= 1.01:
                    prices = []
                    break
                prices.append(price)
            if len(prices) == 2:
                return prices
    return None


def _winline_promote_last_map_match_market(
    soup: Any,
    team1: str,
    team2: str,
    map_num: int,
) -> Optional["_WinlineMapExtract"]:
    """Рынок «Матч» как рынок ПОСЛЕДНЕЙ карты серии.

    На решающей карте Winline иногда не выставляет рынок карты вовсе и оставляет
    только «Матч» — победитель этой карты и победитель матча тогда одно событие.
    Проверено на живой странице 02.08.2026: у REKONIX vs YAKULT BROTHERS (Bo3,
    счёт 1:1, карта 3) в карточке единственная подпись рынка — `Матч 3.30 1.25`,
    и обе цены набраны классом двухисходного рынка.

    Предохранители: подставляем только когда подписи рынка запрошенной карты нет
    НИ В ОДНОЙ карточке пары (приостановленный рынок карты — это не отсутствие),
    рынок «Матч» двухисходный, обе кнопки принимают ставку, а порядок сторон
    доказан по тексту карточки.
    """
    candidates: List[Tuple[int, Any, str]] = []
    for element in soup.find_all(True):
        scope_text = " ".join(element.stripped_strings)
        if not scope_text or not _text_matches_teams(scope_text, team1, team2):
            continue
        if not _winline_single_card_scope(scope_text):
            # Широкий контейнер накрывает соседние матчи: его подписи рынков
            # ничего не говорят о нашей карточке.
            continue
        if _winline_map_row_present(scope_text, map_num):
            return None
        candidates.append((len(scope_text), element, scope_text))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    for _length, element, scope_text in candidates:
        header = _WINLINE_CARD_HEADER_MARKER_RE.search(scope_text)
        if header is None or int(header.group()[0]) != int(map_num):
            # Карточка сама пишет, какая карта идёт (`3карта`). Если она молчит
            # или идёт другая карта — подставлять матчевые кэфы нельзя.
            continue
        order = _winline_team_order(scope_text, team1, team2)
        if order is None:
            continue
        prices = _winline_match_market_winner_prices(element)
        if not prices:
            continue
        card_prices = list(prices)
        if order == "reverse":
            prices.reverse()
        return _WinlineMapExtract(
            odds=prices,
            map_num=map_num,
            market_kind="current_map_winner",
            p1_team="team1",
            p2_team="team2",
            promoted_from_match=True,
            card_team_order=_winline_card_order_label(order, team1, team2),
            card_odds=card_prices,
            details=(
                "winline last map of series: map market not offered, match winner "
                f"promoted | {scope_text[:600]}"
            ),
        )
    return None


def _winline_miss_fingerprint(
    *,
    body_text: str,
    html: str,
    team1: str,
    team2: str,
) -> str:
    """Почему пары нет в снимке: сколько блоков дорисовано и кто из двух виден.

    Различает три исхода, которые в логе выглядели одинаково («match not found»):
    лента коротка (мало блоков), букмекер не выставил матч (блоков много, ни
    одного имени), написание разошлось (видно только одно из двух имён).
    """
    text = str(body_text or "")
    blocks = len(_WINLINE_EVENT_BOUNDARY_RE.findall(text))
    seen1 = bool(_first_index_with_fallback(text.lower(), team1) >= 0)
    seen2 = bool(_first_index_with_fallback(text.lower(), team2) >= 0)
    # HTML проверяем отдельно: имя может быть в разметке, но не в видимом тексте.
    html_low = str(html or "").lower()
    html1 = bool(team1 and _first_index_with_fallback(html_low, team1) >= 0)
    html2 = bool(team2 and _first_index_with_fallback(html_low, team2) >= 0)
    return (
        f"feed_blocks={blocks} body_len={len(text)} "
        f"t1_text={int(seen1)} t2_text={int(seen2)} "
        f"t1_html={int(html1)} t2_html={int(html2)}"
    )


def _winline_card_order_label(order: Optional[str], team1: str, team2: str) -> str:
    """Наши имена (из SourceTV) в том порядке, в каком их написал Winline."""
    if order == "reverse":
        return f"{team2}|{team1}"
    if order == "direct":
        return f"{team1}|{team2}"
    return ""


def _winline_structured_current_map_winner(
    html: str,
    team1: str,
    team2: str,
    map_num: Optional[int],
    series_last_map: bool = False,
) -> Optional["_WinlineMapExtract"]:
    """Extract only the two DOM buttons of the current-map winner market.

    A live Winline map row contains winner, handicap and total markets next to
    each other. When winner buttons disappear, flattened page text starts with
    handicap/total prices, so a text regex can silently promote the wrong
    market. A non-None result from this helper is therefore authoritative:
    once the requested event/map row is found structurally, callers must not
    fall back to adjacent numeric text.
    """
    map_num = _normalize_map_num(map_num)
    if not html or not team1 or not team2 or map_num is None:
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None

    # Подпись строки рынка пишется с пробелом (`3 карта`), а шапка живой карточки
    # — слитно (`3карта 28'`). Без этого различия шапка принималась за подпись
    # рынка: карточка с единственным рынком «Матч» выглядела как «рынок карты есть,
    # но без кнопок», и промоция матчевого рынка на последней карте не срабатывала.
    exact_label_re = re.compile(rf"^{int(map_num)}\s+карта$", re.I)
    glued_label_re = re.compile(rf"^{int(map_num)}\s*карта$", re.I)

    def _is_map_market_label(node: Any) -> bool:
        text = " ".join(node.stripped_strings)
        if exact_label_re.fullmatch(text):
            return True
        # Слитную подпись принимаем только у настоящей строки рынка, опознаваемой
        # классом периода: у шапки карточки другой класс (header-left__time).
        return bool(
            glued_label_re.fullmatch(text)
            and _WINLINE_PERIOD_NAME_CLASS in _winline_node_classes(node)
        )
    saw_requested_row = False
    saw_unbettable_winner = False
    evidence = ""
    valid: List[Tuple[List[float], str]] = []

    def _append_legacy_prices(
        container: Any,
        *,
        order: str,
        scope_text: str,
    ) -> None:
        """Read one proven winner container from the pre-CSS-class Winline DOM."""
        nonlocal saw_unbettable_winner
        prices: List[float] = []
        for node in container.find_all(["div", "span"], recursive=False):
            token = " ".join(node.stripped_strings)
            match = re.fullmatch(r"\s*([0-9]+[.,][0-9]+)\s*", token)
            if not match:
                continue
            price = float(match.group(1).replace(",", "."))
            if price <= 1.01:
                continue
            # Число несёт сама кнопка исхода (`coefficient-button_generic2` в
            # карточке ленты, `odd-btn` в панели события), а заморозка видна
            # только её классом — при `_locked` цена остаётся на месте. Без этой
            # проверки ветка отдавала цену рынка, на который БК ставку не берёт:
            # детектор `_winline_map_odds_bettable` знает лишь разметку карточки
            # ленты и на странице события вердикта не выносит (fail-open None).
            if _winline_price_node_unbettable(node):
                saw_unbettable_winner = True
                return
            prices.append(price)
        if len(prices) != 2:
            saw_unbettable_winner = True
            return
        card_prices = list(prices)
        if order == "reverse":
            prices.reverse()
        valid.append((prices, scope_text[:700], order, card_prices))

    # The selected-event panel uses a different DOM from listing cards:
    # `odd-btn` under an explicitly named "Популярные на карту / Победитель"
    # line. Keep the market-name and period checks structural so the following
    # totals line cannot be promoted either.
    expanded_roots = soup.select(
        "ww-feature-event-live-center-dsk, .event-live-center"
    )
    for root in expanded_roots:
        scope_text = " ".join(root.stripped_strings)
        if not _text_matches_teams(scope_text, team1, team2):
            continue
        order = _winline_team_order(scope_text, team1, team2)
        if order is None:
            continue
        for wrapper in root.select(".fast-bets__wrapper"):
            title = wrapper.select_one(".fast-bets__title")
            if title is None or " ".join(title.stripped_strings).lower() != "популярные на карту":
                continue
            for line in wrapper.select(".bet-line"):
                name = line.select_one(".bet-line__market-name")
                period = line.select_one(".bet-line__period")
                if name is None or period is None:
                    continue
                if " ".join(name.stripped_strings).lower() != "победитель":
                    continue
                if not _is_map_market_label(period):
                    continue
                saw_requested_row = True
                evidence = evidence or scope_text[:700]
                buttons = line.select(".bet-line__coefs-wrapper .odd-btn")
                if len(buttons) != 2 or any(
                    _winline_button_is_unbettable(button) for button in buttons
                ):
                    saw_unbettable_winner = True
                    continue
                prices: List[float] = []
                for button in buttons:
                    button_text = " ".join(button.stripped_strings)
                    match = re.search(
                        r"(?<!\d)([0-9]+[.,][0-9]+)(?!\d)",
                        button_text,
                    )
                    if not match:
                        prices = []
                        break
                    price = float(match.group(1).replace(",", "."))
                    if price <= 1.01:
                        prices = []
                        break
                    prices.append(price)
                if len(prices) != 2:
                    saw_unbettable_winner = True
                    continue
                card_prices = list(prices)
                if order == "reverse":
                    prices.reverse()
                valid.append((prices, scope_text[:700], order, card_prices))

        # Older Winline snapshots do not expose the newer fast-bet CSS
        # classes. The semantic component names and the exact
        # "Популярные на карту -> Победитель -> N карта" hierarchy still
        # identify the market without consulting adjacent handicap/total rows.
        for top_markets in root.select("ww-feature-event-top-markets-dsk"):
            for title in top_markets.find_all(
                lambda tag: (
                    tag.name in {"div", "span"}
                    and " ".join(tag.stripped_strings).lower()
                    == "популярные на карту"
                )
            ):
                header = title.parent
                group = header.parent if header is not None else None
                if group is None:
                    continue
                for name in group.find_all(
                    lambda tag: (
                        tag.name in {"div", "span"}
                        and " ".join(tag.stripped_strings).lower() == "победитель"
                    )
                ):
                    meta = name.parent
                    row = meta.parent if meta is not None else None
                    if meta is None or row is None:
                        continue
                    periods = [
                        " ".join(node.stripped_strings)
                        for node in meta.find_all(["div", "span"], recursive=False)
                    ]
                    if not any(exact_label_re.fullmatch(value) for value in periods):
                        continue
                    saw_requested_row = True
                    evidence = evidence or scope_text[:700]
                    odds_container = meta.find_next_sibling()
                    if odds_container is None:
                        saw_unbettable_winner = True
                        continue
                    _append_legacy_prices(
                        odds_container,
                        order=order,
                        scope_text=scope_text,
                    )

    # The legacy live listing has an exact map row whose first
    # ww-feature-event-market-dsk child is the winner market. Keeping the
    # component boundary is essential: later siblings are handicap and total.
    for event_scope in soup.select("ww-feature-block-event-dsk"):
        scope_text = " ".join(event_scope.stripped_strings)
        if not _text_matches_teams(scope_text, team1, team2):
            continue
        order = _winline_team_order(scope_text, team1, team2)
        if order is None:
            continue
        for label in event_scope.find_all(
            lambda tag: (
                tag.name in {"div", "span"}
                and _is_map_market_label(tag)
            )
        ):
            row = label.parent
            if row is None:
                continue
            markets = row.find_all(
                "ww-feature-event-market-dsk",
                recursive=True,
            )
            if not markets:
                continue
            saw_requested_row = True
            evidence = evidence or scope_text[:700]
            _append_legacy_prices(
                markets[0],
                order=order,
                scope_text=scope_text,
            )

    for label in soup.find_all(
        lambda tag: _WINLINE_PERIOD_NAME_CLASS in _winline_node_classes(tag)
    ):
        if not _is_map_market_label(label):
            continue

        # The period row itself does not contain team names. Climb only to the
        # smallest ancestor proven to be this one event, never to a tournament
        # container that may include neighbouring matches.
        event_scope = None
        node = label.parent
        while node is not None:
            scope_text = " ".join(node.stripped_strings)
            if _text_matches_teams(scope_text, team1, team2):
                if _winline_single_card_scope(scope_text):
                    event_scope = node
                break
            node = node.parent
        if event_scope is None:
            continue

        scope_text = " ".join(event_scope.stripped_strings)
        order = _winline_team_order(scope_text, team1, team2)
        if order is None:
            continue
        saw_requested_row = True
        evidence = evidence or scope_text[:700]

        containers = [
            candidate
            for candidate in (label.find_next_sibling(), label.parent)
            if candidate is not None
        ]
        buttons: List[Any] = []
        for container in containers:
            buttons = _winline_winner_market_buttons(container)
            if buttons:
                break
        if not buttons:
            continue

        if len(buttons) != 2 or any(_winline_button_is_unbettable(b) for b in buttons):
            saw_unbettable_winner = True
            continue

        prices: List[float] = []
        for button in buttons:
            button_text = " ".join(button.stripped_strings)
            match = re.search(r"(?<!\d)([0-9]+[.,][0-9]+)(?!\d)", button_text)
            if not match:
                prices = []
                break
            try:
                price = float(match.group(1).replace(",", "."))
            except Exception:
                prices = []
                break
            if price <= 1.01:
                prices = []
                break
            prices.append(price)
        if len(prices) != 2:
            saw_unbettable_winner = True
            continue
        card_prices = list(prices)
        if order == "reverse":
            prices.reverse()
        valid.append((prices, scope_text[:700], order, card_prices))

    unique = {
        (round(prices[0], 6), round(prices[1], 6))
        for prices, _details, _order, _card_prices in valid
    }
    if len(unique) == 1:
        prices, details, order, card_prices = valid[0]
        return _WinlineMapExtract(
            odds=prices,
            map_num=map_num,
            market_kind="current_map_winner",
            p1_team="team1",
            p2_team="team2",
            details=details,
            card_team_order=_winline_card_order_label(order, team1, team2),
            card_odds=list(card_prices or []),
        )
    if len(unique) > 1:
        return _WinlineMapExtract(
            reason="ambiguous",
            map_num=map_num,
            market_kind="current_map_winner",
            details="winline conflicting structured winner prices",
        )
    if saw_unbettable_winner:
        return _WinlineMapExtract(
            market_closed=True,
            reason="closed",
            map_num=map_num,
            market_kind="current_map_winner",
            details=evidence or "winline current map winner buttons unavailable",
        )
    if saw_requested_row:
        return _WinlineMapExtract(
            reason="map",
            map_num=map_num,
            market_kind="current_map_winner",
            details=(
                evidence
                or (
                    "winline current map row has no winner buttons; "
                    "adjacent markets rejected"
                )
            ),
        )
    if series_last_map:
        # Рынка запрошенной карты в карточке нет вовсе, а карта последняя в серии:
        # победитель этой карты и победитель матча — одно событие.
        promoted = _winline_promote_last_map_match_market(soup, team1, team2, map_num)
        if promoted is not None:
            return promoted
    return None


def _winline_map_odds_bettable(
    html: str,
    team1: str,
    team2: str,
    map_num: Optional[int],
) -> Optional[bool]:
    """Можно ли реально поставить на исход рынка карты `map_num`.

    True/False возвращается только по найденным кнопкам исходов; во всех
    остальных случаях — None («не смогли определить»). Fail-open намеренный:
    отсутствие доказательства блокировки не должно превращаться в отказ от
    ставки, иначе дефект разметки молча выключит поток сигналов.
    """
    map_num = _normalize_map_num(map_num)
    if not html or map_num is None:
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None

    marker_res = [re.compile(p, re.I) for p in _winline_map_marker_patterns(map_num)]

    def _labels_this_map(text: str) -> bool:
        flat = " ".join((text or "").split())
        return bool(flat) and any(rx.search(flat) for rx in marker_res)

    # Сужаемся до карточки нужного матча, иначе поймаем метку '1 карта' соседа.
    if team1 and team2:
        scopes = []
        for element in soup.find_all(True):
            card_text = " ".join(element.stripped_strings)
            if not card_text or not _text_matches_teams(card_text, team1, team2):
                continue
            if not _winline_single_card_scope(card_text):
                # Общий feed/tournament ancestor содержит нужные команды, но
                # одновременно охватывает соседние матчи и не является
                # доказанной карточкой целевого события.
                continue
            if not any(
                _labels_this_map(" ".join(label.stripped_strings))
                for label in element.find_all(
                    lambda tag: (
                        _WINLINE_PERIOD_NAME_CLASS in _winline_node_classes(tag)
                    )
                )
            ):
                continue
            scopes.append((len(card_text), element))
        if not scopes:
            # Карточки этих команд на странице нет — судить о доступности
            # исхода не по чему. Чужой рынок читать нельзя.
            return None
        scopes.sort(key=lambda item: item[0])
        scope_ids = {id(element) for _, element in scopes}
        # Оставляем минимальные точные карточки. Их может быть несколько
        # (stale pinned/shadow + актуальная full card), но общий ancestor,
        # содержащий такую карточку и соседние события, читать нельзя.
        search_roots = [
            element
            for _, element in scopes
            if not any(id(child) in scope_ids for child in element.find_all(True))
        ]
    else:
        search_roots = [soup]

    saw_winner_buttons = False
    for root in search_roots:
        for label in root.find_all(
            lambda tag: _WINLINE_PERIOD_NAME_CLASS in _winline_node_classes(tag)
        ):
            if not _labels_this_map(" ".join(label.stripped_strings)):
                continue
            # Кнопки лежат в соседнем `card__coeffs`; при смене вёрстки
            # поднимаемся к общему родителю, но не выше карточки.
            containers = [
                node
                for node in (label.find_next_sibling(), label.parent)
                if node is not None
            ]
            for container in containers:
                buttons = _winline_winner_market_buttons(container)
                if not buttons:
                    # Кнопок победителя тут нет (закреплённый бар, витрина,
                    # чужая разметка) — ответа не получено, ищем дальше, а не
                    # выдаём вердикт по чужим рынкам.
                    continue
                saw_winner_buttons = True
                if not any(_winline_button_is_unbettable(b) for b in buttons):
                    # Angular может одновременно держать stale locked-тень и
                    # актуальную полную карточку. Любое доказанное открытое
                    # представление точного рынка сильнее старой тени.
                    return True
    # False допустим только когда кнопки точного winner-market действительно
    # найдены и каждое найденное представление locked. Одна лишь строка карты
    # без кнопок остаётся неопределённым состоянием.
    return False if saw_winner_buttons else None


async def _winline_page_odds_bettable(
    page: Any,
    team1: str,
    team2: str,
    map_num: Optional[int],
) -> Optional[bool]:
    """Доступность исхода по живой странице. None — определить не удалось."""
    try:
        html = await _maybe_await(page.content()) or ""
    except Exception:
        return None
    try:
        return _winline_map_odds_bettable(html, team1, team2, map_num)
    except Exception:
        return None


def _winline_driver_odds_bettable(
    drv: Any,
    team1: str,
    team2: str,
    map_num: Optional[int],
) -> Optional[bool]:
    """Версия для legacy Selenium-пути. None — определить не удалось."""
    try:
        html = drv.page_source or ""
    except Exception:
        return None
    try:
        return _winline_map_odds_bettable(html, team1, team2, map_num)
    except Exception:
        return None


def _winline_mark_locked_as_closed(wl: "_WinlineMapExtract") -> "_WinlineMapExtract":
    """Замороженный исход = закрытый рынок для всех потребителей ставки.

    Числа снимаем намеренно: ниже по конвейеру наличие odds означает
    «можно ставить», и оставить их — значит предложить цену, которую БК
    не примет.
    """
    wl.odds = []
    wl.market_closed = True
    wl.reason = "closed"
    wl.details = f"{wl.details or ''} | winline outcome locked (not bettable)".strip(" |")
    return wl


def _extract_winline_current_map_winner(
    text: str,
    team1: str,
    team2: str,
    forced_map_num: Optional[int] = None,
    *,
    html: str = "",
    series_last_map: bool = False,
) -> _WinlineMapExtract:
    """Strict Winline current-map winner only.

    Матчевые кэфы в поток карты не подставляются НИКОГДА, кроме одного случая:
    `series_last_map=True` и рынка карты в карточке нет вовсе. На последней карте
    серии победитель карты и победитель матча — одно событие, и Winline тогда
    оставляет только «Матч». Решение принимается только по DOM (`html`), потому
    что двухисходность рынка видна лишь по классам кнопок: в плоском тексте
    трёхисходный «Матч» на Bo2 выглядит так же, как двухисходный.
    """
    map_num = _normalize_map_num(forced_map_num)
    flat = " ".join((text or "").split())
    low = flat.lower()
    if (not flat and not html) or map_num is None:
        return _WinlineMapExtract(reason="map", details="winline missing/invalid map_num")

    structured = _winline_structured_current_map_winner(
        html,
        team1,
        team2,
        map_num,
        series_last_map=series_last_map,
    )
    if structured is not None:
        return structured
    if html:
        # A full DOM snapshot is stronger evidence than flattened text. If its
        # exact winner buttons cannot be proven structurally, fail closed:
        # falling back here is precisely how handicap/total prices leaked into
        # the current-map winner stream when the winner market disappeared.
        return _WinlineMapExtract(
            reason="map",
            map_num=map_num,
            market_kind="current_map_winner",
            details="winline structured current map winner market unavailable",
        )

    card_context = _winline_matched_card_context(
        flat,
        team1,
        team2,
        html=html,
        map_num=map_num,
    )
    if not card_context:
        # Карточка запрошенной пары не доказана. Читать рынок по всей странице
        # нельзя: строка `N карта a b` берётся ПЕРВАЯ, а принадлежит она чужому
        # матчу — 31.07.2026 запрос `REKONIX vs L1GA TEAM` так получал кэфы
        # карточки `ENJOY GLYPH` из той же лиги (11 записей из 419 с кэфами).
        return _WinlineMapExtract(
            reason="no_card",
            map_num=map_num,
            details="winline card for requested teams not proven in page text",
        )
    working = card_context
    order = _winline_team_order(working, team1, team2)
    has_teams = order is not None
    working_low = working.lower()

    # Locate exact current-map market row.
    row_re = _winline_market_row_re(map_num)
    row_m = row_re.search(working_low if working is working_low else working)
    # search on original working with case preserved for numbers; use working for both
    row_m = row_re.search(working)

    def _is_dash(tok: str) -> bool:
        return str(tok or "").strip() in {"—", "-", "–", "−"}

    def _to_odd(tok: str) -> Optional[float]:
        try:
            return float(str(tok).replace(",", "."))
        except Exception:
            return None

    # Closed market: map marker present with dash placeholders or lock markers nearby.
    if row_m and (_is_dash(row_m.group(1)) or _is_dash(row_m.group(2))):
        return _WinlineMapExtract(
            market_closed=True,
            reason="closed",
            map_num=map_num,
            market_kind="current_map_winner",
            details="winline current map market closed",
        )
    window_for_lock = working
    if any(marker in working_low for marker in LOCK_MARKERS) and any(
        re.search(pat, working_low, re.I) for pat in _winline_map_marker_patterns(map_num)
    ):
        # only if no numeric pair for this map
        if not row_m or _is_dash(row_m.group(1)) or _is_dash(row_m.group(2)):
            return _WinlineMapExtract(
                market_closed=True,
                reason="closed",
                map_num=map_num,
                market_kind="current_map_winner",
                details="winline current map market closed/suspended",
            )

    if row_m and not _is_dash(row_m.group(1)) and not _is_dash(row_m.group(2)):
        o1 = _to_odd(row_m.group(1))
        o2 = _to_odd(row_m.group(2))
        if o1 is not None and o2 is not None and o1 > 1.01 and o2 > 1.01:
            if not has_teams:
                return _WinlineMapExtract(
                    reason="ambiguous",
                    map_num=map_num,
                    details="winline ambiguous team order for current map market",
                )
            # DOM odds follow local team order; canonicalize to team1/team2.
            if order == "reverse":
                odds = [o2, o1]
            else:
                odds = [o1, o2]
            return _WinlineMapExtract(
                odds=odds,
                map_num=map_num,
                market_kind="current_map_winner",
                p1_team="team1",
                p2_team="team2",
                details=working[:700],
            )

    # No exact current-map row: classify rejection reason without promoting match odds.
    other_map = False
    for other in range(1, 6):
        if other == map_num:
            continue
        if any(re.search(pat, working_low, re.I) for pat in _winline_map_marker_patterns(other)):
            other_map = True
            break
    has_match = bool(re.search(r"\bматч\b", working_low, re.I))
    if other_map:
        return _WinlineMapExtract(
            reason="map",
            map_num=map_num,
            details="winline wrong/other map market only; exact current map missing",
        )
    if has_match:
        return _WinlineMapExtract(
            reason="match",
            map_num=map_num,
            details="winline match market only; current map winner missing",
        )
    if not has_teams and re.search(rf"\b{map_num}\s*к\b|\b{map_num}\s*карта\b", low, re.I):
        return _WinlineMapExtract(
            reason="ambiguous",
            map_num=map_num,
            details="winline ambiguous team order for current map market",
        )
    return _WinlineMapExtract(
        reason="map",
        map_num=map_num,
        details="winline current map winner market missing",
    )


def _site_result_with_provenance(
    *,
    site: str,
    url: str,
    status: str,
    match_found: bool,
    odds: List[float],
    source: str,
    details: str,
    market_closed: bool = False,
    match_odds: Optional[List[float]] = None,
    market_kind: Optional[str] = None,
    map_num: Optional[int] = None,
    p1_team: Optional[str] = None,
    p2_team: Optional[str] = None,
    card_team_order: Optional[str] = None,
    card_odds: Optional[List[float]] = None,
) -> SiteResult:
    return SiteResult(
        site=site,
        url=url,
        status=status,
        match_found=match_found,
        odds=list(odds or []),
        source=source,
        details=details,
        market_closed=bool(market_closed),
        match_odds=list(match_odds or []),
        market_kind=market_kind,
        map_num=map_num,
        p1_team=p1_team,
        p2_team=p2_team,
        card_team_order=card_team_order,
        card_odds=list(card_odds or []),
    )


def _extract_map_odds_deeplink(
    site: str,
    text: str,
    team1: str,
    team2: str,
    forced_map_num: Optional[int] = None,
) -> List[float]:
    map_num = _resolve_map_num_for_site(site, text, forced_map_num)
    if map_num is None:
        return []
    local_ctx = _context_local_to_teams(text or "", team1, team2, left=80, right=1200)
    if team1 and team2 and not local_ctx:
        return []
    flat = " ".join((local_ctx if local_ctx else (text or "")).split())
    if site == "betboom":
        pat = re.compile(
            rf"Исход\s+Карта\s*{map_num}\s+П1\s+([0-9]+[.,][0-9]+)\s+П2\s+([0-9]+[.,][0-9]+)",
            re.I,
        )
        m = pat.search(flat)
        if m:
            return [float(m.group(1).replace(",", ".")), float(m.group(2).replace(",", "."))]
        # Fallback: compact live row like "1-я карта 15:8 1.65 - 2.20".
        m_row = re.search(
            rf"{map_num}\s*[-–]?\s*я\s*карта(?:\s+\d+:\d+)?\s+([0-9]+[.,][0-9]+)\s*-\s*([0-9]+[.,][0-9]+)",
            flat,
            re.I,
        )
        if m_row:
            return [float(m_row.group(1).replace(",", ".")), float(m_row.group(2).replace(",", "."))]
    if site == "pari":
        m_row = re.search(
            rf"{map_num}\s*[-–]?\s*я\s*карта(?:\s+\d+:\d+)?\s+([0-9]+[.,][0-9]+)\s*-\s*([0-9]+[.,][0-9]+)",
            flat,
            re.I,
        )
        if m_row:
            return [float(m_row.group(1).replace(",", ".")), float(m_row.group(2).replace(",", "."))]
        m_p1p2 = re.search(
            rf"{map_num}\s*[-–]?\s*я\s*карта.*?п1\s*([0-9]+[.,][0-9]+).*?п2\s*([0-9]+[.,][0-9]+)",
            flat,
            re.I,
        )
        if m_p1p2:
            return [float(m_p1p2.group(1).replace(",", ".")), float(m_p1p2.group(2).replace(",", "."))]
        block_re = re.compile(
            rf"{map_num}\s*-\s*я\s*карта(.*?)(?:[1-5]\s*-\s*я\s*карта|$)",
            re.I | re.S,
        )
        block_m = block_re.search(flat)
        if block_m:
            block = block_m.group(1)
            # Map-only mode for Pari: do not fallback to generic first-numeric extraction,
            # because it can leak match-level odds when map row is missing.
            if re.search(r"п1\s*[0-9]+[.,][0-9]+.*?п2\s*[0-9]+[.,][0-9]+", block, re.I):
                m_block = re.search(
                    r"п1\s*([0-9]+[.,][0-9]+).*?п2\s*([0-9]+[.,][0-9]+)",
                    block,
                    re.I,
                )
                if m_block:
                    return [float(m_block.group(1).replace(",", ".")), float(m_block.group(2).replace(",", "."))]
    if site == "winline":
        # Strict current-map winner only; canonical team1/team2 order.
        extracted = _extract_winline_current_map_winner(
            text,
            team1,
            team2,
            forced_map_num=map_num,
        )
        return list(extracted.odds or [])
    return []


def _extract_map_odds_from_feed_context(
    site: str,
    context: str,
    team1: str = "",
    team2: str = "",
    forced_map_num: Optional[int] = None,
) -> List[float]:
    if not context:
        return []
    local_right = 220 if site == "winline" else 260
    local_ctx = _context_local_to_teams(context, team1, team2, left=24, right=local_right)
    working = local_ctx if local_ctx else context
    flat = " ".join(working.split())
    low = flat.lower()
    map_num = _resolve_map_num_for_site(site, flat, forced_map_num)
    if map_num is None:
        return []

    if site == "betboom":
        block_match = re.search(
            rf"Исход\s+Карта\s*{map_num}(.*?)(?:Распределение ставок|Тотал|Фора|Купон|$)",
            flat,
            re.I | re.S,
        )
        if block_match:
            block = block_match.group(0)
            m_block = re.search(
                r"П1\s*([0-9]+[.,][0-9]+)\s*П2\s*([0-9]+[.,][0-9]+)",
                block,
                re.I,
            )
            if m_block:
                return [float(m_block.group(1).replace(",", ".")), float(m_block.group(2).replace(",", "."))]
        # Example: "2-я карта ... П1 1.65 П2 2.20"
        m = re.search(
            rf"(?:{map_num}\s*-\s*я\s*карта|{map_num}\s*карта)\s+п1\s*([0-9]+[.,][0-9]+)\s+п2\s*([0-9]+[.,][0-9]+)(?:\s+ещё|\s*$)",
            flat,
            re.I,
        )
        if m:
            return [float(m.group(1).replace(",", ".")), float(m.group(2).replace(",", "."))]
        # Fallback: row format without explicit П1/П2 labels.
        m_row = re.search(
            rf"{map_num}\s*[-–]?\s*я\s*карта(?:\s+\d+:\d+)?\s+([0-9]+[.,][0-9]+)\s*-\s*([0-9]+[.,][0-9]+)",
            flat,
            re.I,
        )
        if m_row:
            return [float(m_row.group(1).replace(",", ".")), float(m_row.group(2).replace(",", "."))]

    if site == "pari":
        # Prefer odds exactly in "<N>-я карта" row:
        # "... 2-я карта 0:0 2.70 - 1.40 ..."
        m = re.search(
            rf"{map_num}\s*[-–]?\s*я\s*карта(?:\s+\d+:\d+)?\s+([0-9]+[.,][0-9]+)\s*-\s*([0-9]+[.,][0-9]+)",
            flat,
            re.I,
        )
        if m:
            return [float(m.group(1).replace(",", ".")), float(m.group(2).replace(",", "."))]

    if site == "winline":
        extracted = _extract_winline_current_map_winner(
            working,
            team1,
            team2,
            forced_map_num=map_num,
        )
        return list(extracted.odds or [])

    return []


def _extract_match_odds_from_context(
    site: str,
    context: str,
    team1: str = "",
    team2: str = "",
) -> List[float]:
    if not context:
        return []
    local_ctx = _context_local_to_teams(context, team1, team2, left=32, right=900)
    flat = " ".join((local_ctx if local_ctx else context).split())
    if not flat:
        return []

    # Winline keeps current live card odds under explicit "Матч" label.
    if site == "winline":
        m = re.search(
            r"\bматч\b\s*([0-9]+[.,][0-9]+)\s+([0-9]+[.,][0-9]+)",
            flat,
            re.I,
        )
        if m:
            return [float(m.group(1).replace(",", ".")), float(m.group(2).replace(",", "."))]

    return []


def _extract_first_match_odds(
    site: str,
    team1: str,
    team2: str,
    *contexts: str,
) -> List[float]:
    for context in contexts:
        odds = _extract_match_odds_from_context(
            site,
            context or "",
            team1=team1,
            team2=team2,
        )
        if len(odds) >= 2:
            return odds[:2]
    return []


def _is_map_market_closed(site: str, text: str, forced_map_num: Optional[int] = None) -> bool:
    if not text:
        return False
    map_num = _resolve_map_num_for_site(site, text, forced_map_num)
    if map_num is None:
        return False
    flat = " ".join((text or "").split())
    low = flat.lower()

    if site == "betboom":
        m = re.search(rf"Исход\s+Карта\s*{map_num}", flat, re.I)
        if m:
            block_match = re.search(
                rf"Исход\s+Карта\s*{map_num}(.*?)(?:Распределение ставок|Тотал|Фора|Купон|$)",
                flat,
                re.I | re.S,
            )
            block = block_match.group(0) if block_match else flat[m.start(): min(len(flat), m.start() + 220)]
            block_low = block.lower()
            explicit_lock = any(marker in block_low for marker in LOCK_MARKERS)
            has_outcome_labels = ("п1" in block_low and "п2" in block_low)
            has_outcome_odds = bool(
                re.search(
                    r"п1\s*[0-9]+[.,][0-9]+\s*п2\s*[0-9]+[.,][0-9]+",
                    block,
                    re.I,
                )
            )
            if explicit_lock:
                return True
            if has_outcome_labels and not has_outcome_odds:
                return True
            return False
        else:
            # Some BetBoom map rows expose lock markers without the "Исход Карта N" prefix.
            m_row = re.search(
                rf"(?:{map_num}\s*[-–]?\s*я\s*карта|карта\s*{map_num})",
                flat,
                re.I,
            )
            if not m_row:
                return False
            block = flat[m_row.start(): min(len(flat), m_row.start() + 280)]
            return any(marker in block.lower() for marker in LOCK_MARKERS)

    if site == "pari":
        m = re.search(
            rf"{map_num}\s*-\s*я\s*карта(.*?)(?:[1-5]\s*-\s*я\s*карта|$)",
            flat,
            re.I | re.S,
        )
        if not m:
            return False
        block = m.group(1)
        block_low = block.lower()
        if "исход" not in block_low:
            return False
        odds = _extract_numeric_odds(block, max_count=6)
        if len(odds) < 2:
            return True
        if any(marker in block_low for marker in LOCK_MARKERS):
            return True
        return False

    if site == "winline":
        extracted = _extract_winline_current_map_winner(
            text,
            "",
            "",
            forced_map_num=forced_map_num if forced_map_num is not None else map_num,
        )
        # When teams unknown here, still detect dash-closed rows for forced map.
        dash_closed = bool(
            re.search(
                rf"(?:победитель\s*{map_num}\s*карт[аы]|\b{map_num}\s*карта\b|\b{map_num}\s*к\b)"
                rf"\s*(?:—|-|–)\s*(?:—|-|–)",
                low,
                re.I,
            )
        )
        if dash_closed:
            return True
        if not _looks_map_context(low):
            return False
        if any(marker in low for marker in LOCK_MARKERS):
            return True
        return False

    return False


def _is_deeplink(site: str, url: str) -> bool:
    low = url.lower()
    if site == "betboom":
        return bool(re.search(r"/esport/dota-2/\d+(?:/\d+)?", low))
    if site == "pari":
        return bool(re.search(r"/esports/\d+/\d+", low))
    if site == "winline":
        return "/stavki/sport/kibersport/dota_2/" in low
    return False


def _build_driver(proxy_url: str, *, page_load_timeout: int = 60):
    parsed = _parse_proxy(proxy_url)

    chrome_options = Options()
    chrome_options.page_load_strategy = "eager"
    chrome_binary = None
    if CHROME_BIN and Path(CHROME_BIN).exists():
        chrome_binary = CHROME_BIN
    else:
        for candidate in (
            "google-chrome",
            "google-chrome-stable",
            "chromium",
            "chromium-browser",
            "chrome",
        ):
            resolved = shutil.which(candidate)
            if resolved:
                chrome_binary = resolved
                break
    if chrome_binary:
        chrome_options.binary_location = chrome_binary
    if BOOKMAKER_SELENIUM_HEADLESS:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-popup-blocking")
    # Presence checks can reuse a single browser session with background tabs.
    # These flags reduce Chrome's tendency to throttle hidden tabs in headless mode.
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    # Reuse same Chrome profile to avoid spawning multiple instances
    chrome_options.add_argument("--user-data-dir=/tmp/selenium_presence_profile")

    sw_options = {
        "proxy": {
            "http": f"http://{parsed['username']}:{parsed['password']}@{parsed['host']}:{parsed['port']}",
            "https": f"https://{parsed['username']}:{parsed['password']}@{parsed['host']}:{parsed['port']}",
            "no_proxy": "localhost,127.0.0.1",
        },
        "verify_ssl": False,
        "suppress_connection_errors": True,
        "request_storage": "memory",
        "request_storage_max_size": 150,
    }
    drv = webdriver.Chrome(options=chrome_options, seleniumwire_options=sw_options)
    drv.set_page_load_timeout(max(5, int(page_load_timeout)))
    try:
        drv.set_script_timeout(max(5, int(page_load_timeout)))
    except Exception:
        pass
    return drv


def _iter_request_texts(drv) -> Iterable[str]:
    for req in drv.requests:
        resp = req.response
        if not resp:
            continue
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if not any(x in ctype for x in ("json", "javascript", "text", "html")):
            continue
        body = resp.body
        if not body:
            continue
        if len(body) > 2_500_000:
            continue
        try:
            txt = body.decode("utf-8", errors="ignore")
        except Exception:
            continue
        if txt:
            yield txt


def _iter_request_texts_for_host(drv, host: str) -> Iterable[str]:
    host = str(host or "").strip().lower()
    if not host:
        yield from _iter_request_texts(drv)
        return
    for req in drv.requests:
        resp = req.response
        if not resp:
            continue
        req_host = urlparse(str(req.url or "")).netloc.strip().lower()
        if not req_host:
            continue
        if host not in req_host and req_host not in host:
            continue
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if not any(x in ctype for x in ("json", "javascript", "text", "html")):
            continue
        body = resp.body
        if not body or len(body) > 2_500_000:
            continue
        try:
            txt = body.decode("utf-8", errors="ignore")
        except Exception:
            continue
        if txt:
            yield txt


def _load_site_render_payload(
    drv,
    url: str,
    *,
    initial_wait_seconds: float = 7.0,
    scroll_wait_seconds: float = 2.0,
) -> Tuple[str, str, str, str]:
    load_status = "ok"
    load_error = ""
    try:
        try:
            host = urlparse(url).netloc
            if host:
                drv.scopes = [rf".*{re.escape(host)}.*"]
        except Exception:
            pass
        drv.requests.clear()
        drv.get(url)
        time.sleep(max(0.0, float(initial_wait_seconds)))
        drv.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.5);")
        time.sleep(max(0.0, float(scroll_wait_seconds)))
        drv.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(max(0.0, float(scroll_wait_seconds)))
    except Exception as exc:
        load_status = "partial_load"
        load_error = str(exc)

    html = drv.page_source or ""
    soup = BeautifulSoup(html, "html.parser")
    visible = " ".join(soup.stripped_strings)
    body_text = ""
    try:
        body_text = drv.find_element(By.TAG_NAME, "body").text
    except Exception:
        body_text = ""
    return load_status, load_error, html, visible or body_text


def _find_from_sources(team1: str, team2: str, sources: List[Tuple[str, str]]) -> Tuple[bool, List[float], str, str]:
    first_match_detail = ""
    first_match_source = ""
    best_match_score: Optional[Tuple[int, int, int]] = None
    best_odds: List[float] = []
    for source_name, text in sources:
        sn = _snippet_by_teams(text, team1, team2)
        if not sn:
            continue

        odds = _extract_odds_near_teams(sn, team1, team2)
        detail = _context_around_teams(sn, team1, team2)
        source_rank = SOURCE_PREFERENCE.get(source_name, 99)
        score = (source_rank, len(detail or ""), len(sn))
        if odds:
            if best_match_score is None or score < best_match_score:
                best_match_score = score
                first_match_detail = detail
                first_match_source = source_name
                best_odds = odds[:2]
            continue
        if not first_match_detail or score < best_match_score:
            best_match_score = score
            first_match_detail = detail
            first_match_source = source_name

    if best_match_score is not None and best_odds:
        return True, best_odds, first_match_source, first_match_detail
    if first_match_detail:
        return True, [], first_match_source, first_match_detail

    return False, [], "", "match not found in rendered DOM/network payload"


def _safe_click(el, drv) -> bool:
    try:
        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.15)
    except Exception:
        pass
    try:
        el.click()
        time.sleep(1.2)
        return True
    except Exception:
        pass
    try:
        drv.execute_script("arguments[0].click();", el)
        time.sleep(1.2)
        return True
    except Exception:
        return False


def _href_looks_match_page(site: str, href: str) -> bool:
    low = (href or "").lower()
    if site == "betboom":
        return bool(re.search(r"/esport/dota-2/\d+(?:/\d+)?", low))
    if site == "pari":
        return bool(re.search(r"/esports/\d+/\d+", low))
    if site == "winline":
        return bool(
            re.search(
                r"/stavki/(?:event/\d+|sport/kibersport/dota_2/[a-z0-9_/-]*/\d+)",
                low,
                re.I,
            )
        )
    return False


def _candidate_match_urls_from_html(site: str, base_url: str, html: str) -> List[str]:
    if not html:
        return []
    if site == "betboom":
        pat = re.compile(r"(?:https?://[^\"'\\s>]+)?/esport/dota-2/\d+(?:/\d+)?")
    elif site == "pari":
        pat = re.compile(r"(?:https?://[^\"'\\s>]+)?/esports/\d+/\d+")
    elif site == "winline":
        pat = re.compile(
            r"(?:https?://[^\"'\\s>]+)?/stavki/(?:event/\d+|sport/kibersport/dota_2/[a-z0-9_/-]*/\d+)",
            re.I,
        )
    else:
        return []
    out: List[str] = []
    seen = set()
    for m in pat.finditer(html):
        raw = m.group(0)
        target = urljoin(base_url, raw)
        if not _href_looks_match_page(site, target):
            continue
        if target in seen:
            continue
        seen.add(target)
        out.append(target)
        if len(out) >= 20:
            break
    return out


def _find_match_by_urls(drv, site: str, urls: List[str], team1: str, team2: str) -> Optional[str]:
    if not urls:
        return None
    t1 = (team1 or "").strip().lower()
    t2 = (team2 or "").strip().lower()
    t1s = t1.split()[0] if t1 else ""
    t2s = t2.split()[0] if t2 else ""
    for target in urls[:12]:
        try:
            drv.get(target)
            time.sleep(1.8)
            body = " ".join(drv.find_element(By.TAG_NAME, "body").text.lower().split())
        except Exception:
            continue
        if (
            (t1 and t1 in body and t2 and t2 in body)
            or (t1s and t1s in body and t2s and t2s in body)
            or _current_page_matches_teams(drv, team1, team2, attempts=1, delay=0.0)
        ):
            return target
    return None


def _open_match_details_by_teams(drv, site: str, team1: str, team2: str) -> Optional[str]:
    t1 = (team1 or "").strip().lower()
    t2 = (team2 or "").strip().lower()
    if not t1 or not t2:
        return None
    team_tokens = [t1, t1.split()[0], t2, t2.split()[0]]
    before_url = drv.current_url
    xpath = (
        "//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
        f"'{team_tokens[0]}') and contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
        f"'{team_tokens[2]}')]"
    )
    candidates = drv.find_elements(By.XPATH, xpath)
    if not candidates:
        # Fallback by first token pair.
        xpath2 = (
            "//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
            f"'{team_tokens[1]}') and contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
            f"'{team_tokens[3]}')]"
        )
        candidates = drv.find_elements(By.XPATH, xpath2)

    for el in candidates[:20]:
        if site == "betboom":
            betboom_openers = [
                ".//a[@role='link' and contains(@class,'bb-rM')]",
                ".//a[@role='link' and not(normalize-space(.))]",
            ]
            for opener_xp in betboom_openers:
                try:
                    openers = el.find_elements(By.XPATH, opener_xp)
                except Exception:
                    openers = []
                for opener in openers[:3]:
                    if not _safe_click(opener, drv):
                        continue
                    try:
                        now_url = drv.current_url
                    except Exception:
                        now_url = before_url
                    if now_url != before_url and _current_page_matches_teams(drv, team1, team2):
                        return now_url
                    try:
                        body_text = drv.find_element(By.TAG_NAME, "body").text.lower()
                    except Exception:
                        body_text = ""
                    if _text_matches_teams(body_text, team1, team2) and "карта" in body_text:
                        return now_url
        try:
            links = el.find_elements(By.XPATH, ".//a[@href]")
        except Exception:
            links = []
        for a in links[:6]:
            try:
                href = a.get_attribute("href")
            except Exception:
                href = ""
            if not href:
                continue
            if not _href_looks_match_page(site, href):
                continue
            try:
                target = urljoin(before_url, href)
                drv.get(target)
                time.sleep(1.8)
                if _current_page_matches_teams(drv, team1, team2):
                    return drv.current_url
            except Exception:
                continue
        try:
            clickable = el.find_elements(
                By.XPATH,
                ".//ancestor-or-self::*[self::a or self::button or @role='button' or contains(@class,'match') or contains(@class,'event') or contains(@class,'row')]",
            )
        except Exception:
            clickable = []
        chain = clickable[:4] if clickable else [el]
        for c in chain:
            if not _safe_click(c, drv):
                continue
            try:
                now_url = drv.current_url
            except Exception:
                now_url = before_url
            if now_url != before_url and _current_page_matches_teams(drv, team1, team2):
                return now_url
            try:
                body_text = drv.find_element(By.TAG_NAME, "body").text.lower()
            except Exception:
                body_text = ""
            if ("карта" in body_text or "исход" in body_text) and _text_matches_teams(body_text, team1, team2):
                return now_url
    return None


def _click_map_tab_on_current_page(drv, site: str, map_num: Optional[int]) -> bool:
    if map_num is None:
        return False
    xpath_candidates: List[str] = []
    if site == "betboom":
        xpath_candidates = [
            f"//button[normalize-space()='Карта {map_num}']",
            f"//a[normalize-space()='Карта {map_num}']",
            f"//*[@role='tab' and normalize-space()='Карта {map_num}']",
            f"//*[self::button or self::a or self::div][normalize-space()='Карта {map_num}']",
        ]
    elif site == "pari":
        xpath_candidates = [
            f"//*[self::button or self::a or self::div][normalize-space()='{map_num}-Я КАРТА']",
            f"//*[self::button or self::a or self::div][normalize-space()='{map_num}-я карта']",
            f"//*[self::button or self::a or self::div][normalize-space()='Карта {map_num}']",
            f"//*[self::button or self::a or self::div][normalize-space()='{map_num} карта']",
        ]
    elif site == "winline":
        xpath_candidates = [
            f"//*[self::button or self::a or self::div][normalize-space()='{map_num} карта']",
            f"//*[self::button or self::a or self::div][normalize-space()='Карта {map_num}']",
            f"//*[self::button or self::a or self::div][normalize-space()='{map_num}К']",
            f"//*[self::button or self::a or self::div][normalize-space()='{map_num} К']",
        ]
    if _try_click_xpath(drv, xpath_candidates):
        time.sleep(1.0)
        return True
    return False


def _parse_map_market_on_current_page(
    drv,
    site: str,
    team1: str,
    team2: str,
    forced_map_num: Optional[int] = None,
) -> Tuple[List[float], str]:
    try:
        body_text = " ".join(drv.find_element(By.TAG_NAME, "body").text.split())
    except Exception:
        body_text = ""
    map_num = _resolve_map_num_for_site(site, body_text, forced_map_num)
    clicked_tab = _click_map_tab_on_current_page(drv, site, map_num)
    if not clicked_tab and site == "betboom" and map_num is not None:
        _try_click_text(drv, [f"Карта {map_num}", f"Карта{map_num}", f"{map_num} карта"])
    elif not clicked_tab and site == "pari" and map_num is not None:
        _try_click_text(
            drv,
            [
                f"{map_num}-Я КАРТА",
                f"{map_num}-я карта",
                f"{map_num} карта",
                f"Карта {map_num}",
            ],
        )
    elif not clicked_tab and site == "winline" and map_num is not None:
        _try_click_text(
            drv,
            [
                f"{map_num}К",
                f"{map_num} К",
                f"{map_num}-я карта",
                f"{map_num} карта",
                f"Победитель {map_num} карты",
                f"Победитель {map_num} карт",
            ],
        )
    try:
        body_text = " ".join(drv.find_element(By.TAG_NAME, "body").text.split())
    except Exception:
        pass
    return _extract_map_odds_deeplink(
        site,
        body_text,
        team1,
        team2,
        forced_map_num=forced_map_num,
    ), body_text


def _is_map_context_active(text: str, forced_map_num: Optional[int]) -> bool:
    return _resolve_map_num(text or "", forced_map_num) is not None


def _map_missing_source(site: str) -> str:
    base = (site or "bookmaker").strip().lower()
    return f"{base}_map_market_missing"


def _map_closed_source(site: str) -> str:
    base = (site or "bookmaker").strip().lower()
    return f"{base}_map_market_closed"


def _match_level_rejected_source(site: str) -> str:
    base = (site or "bookmaker").strip().lower()
    return f"{base}_match_level_rejected"


def _map_context_details(site: str, reason_kind: str) -> str:
    base = (site or "bookmaker").strip().lower()
    if reason_kind == "match_level_rejected":
        return f"{base} map-only context: rejected non-map fallback"
    if reason_kind == "map_market_closed":
        return f"{base} map market is closed in map-only context"
    return f"{base} map market not found in map-only context"


async def parse_site_in_camoufox_page_async(
    page,
    site: str,
    url: str,
    team1: str,
    team2: str,
    mode: str,
    forced_map_num: Optional[int] = None,
    acquisition_mode: Optional[str] = None,
    series_last_map: bool = False,
) -> SiteResult:
    # Acquisition mode is honored only for Winline; other bookmakers keep legacy goto.
    effective_acq = acquisition_mode if site == "winline" else None
    _payload = await _load_site_render_payload_camoufox_async(
        page,
        url,
        initial_wait_seconds=7.0,
        scroll_wait_seconds=2.0,
        acquisition_mode=effective_acq,
    )
    # Backward compatible with tests/mocks that still return the legacy 5-tuple.
    if len(_payload) >= 6:
        load_status, load_error, html, visible, body_text, acq_diag = _payload[:6]
    else:
        load_status, load_error, html, visible, body_text = _payload[:5]
        acq_diag = {}

    # Пары нет в снимке — возможно, её карточка просто не дорисована: поллер
    # живёт на одной странице сутками и не навигируется, а лента подгружается
    # порциями при прокрутке контейнера. Один проход прокрутки и перечитывание
    # снимка; троттлинг не даёт делать это на каждом промахе подряд.
    miss_fingerprint = ""
    if (
        site == "winline"
        and effective_acq
        and team1
        and team2
        and not _text_matches_teams(body_text or visible or "", team1, team2)
    ):
        async def _teams_rendered() -> bool:
            return _text_matches_teams(await _camoufox_body_text(page), team1, team2)

        if await _sweep_camoufox_feed(page, probe=_teams_rendered):
            _payload = await _load_site_render_payload_camoufox_async(
                page,
                url,
                initial_wait_seconds=0.0,
                scroll_wait_seconds=0.0,
                acquisition_mode=effective_acq,
            )
            if len(_payload) >= 6:
                load_status, load_error, html, visible, body_text, acq_diag = _payload[:6]
            else:
                load_status, load_error, html, visible, body_text = _payload[:5]
                acq_diag = {}
        if not _text_matches_teams(body_text or visible or "", team1, team2):
            # Пары нет даже после дорисовки. Дальше по evidence будет видно
            # только «match not found», из чего нельзя понять, чего не хватило:
            # ленты, конкретного названия или страницы целиком.
            miss_fingerprint = _winline_miss_fingerprint(
                body_text=body_text,
                html=html,
                team1=team1,
                team2=team2,
            )

    initial_body_text = body_text
    match_fallback_odds: List[float] = []

    def _with_acq(result: SiteResult) -> SiteResult:
        if miss_fingerprint:
            result.miss_fingerprint = miss_fingerprint
        return _apply_acquisition_diag(result, acq_diag)

    if _is_deeplink(site, url):
        if not body_text:
            for _ in range(8):
                time.sleep(1.0)
                body_text = " ".join(await _camoufox_body_text(page).split())
                if body_text:
                    break
        if site == "pari":
            for i in range(8):
                if ("КИБЕРСПОРТ / DOTA 2" in body_text or "КИБЕРСПОРТ / DOTA2" in body_text) and "Исход" in body_text:
                    break
                if i == 3:
                    with contextlib.suppress(Exception):
                        await _maybe_await(page.reload(wait_until="domcontentloaded", timeout=30000))
                time.sleep(1.0)
                body_text = " ".join(await _camoufox_body_text(page).split())
        map_odds, body_text = await _parse_map_market_on_current_camoufox_page_async(
            page,
            site,
            team1,
            team2,
            forced_map_num=forced_map_num,
        )
        if site == "winline":
            wl = _extract_winline_current_map_winner(
                body_text or visible or "",
                team1,
                team2,
                forced_map_num=forced_map_num,
            )
            match_diag = _extract_first_match_odds(site, team1, team2, body_text, visible)
            if wl.odds and await _winline_page_odds_bettable(
                page, team1, team2, wl.map_num or forced_map_num
            ) is False:
                wl = _winline_mark_locked_as_closed(wl)
            if wl.odds:
                return _with_acq(_site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=wl.odds[:2],
                    source="deeplink_map_market",
                    details=str(wl.details or body_text or "")[:700],
                    market_kind=wl.market_kind,
                    map_num=wl.map_num,
                    p1_team=wl.p1_team,
                    p2_team=wl.p2_team,
                    card_team_order=wl.card_team_order,
                    card_odds=wl.card_odds,
                    match_odds=match_diag,
                ))
            if wl.market_closed or wl.reason == "closed":
                return _with_acq(_site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=[],
                    source=_map_closed_source(site),
                    details=str(wl.details or body_text or "")[:700],
                    market_closed=True,
                    market_kind="current_map_winner",
                    map_num=wl.map_num or _normalize_map_num(forced_map_num),
                    match_odds=match_diag,
                ))
            reason = wl.reason or "map"
            if reason == "ambiguous":
                source_name = "winline_ambiguous_order_rejected"
            elif reason == "match":
                source_name = _match_level_rejected_source(site)
            else:
                source_name = "winline_map_rejected"
            return _with_acq(_site_result_with_provenance(
                site=site,
                url=url,
                status=load_status,
                match_found=bool(team1 and team2 and _text_matches_teams(body_text or visible or "", team1, team2)),
                odds=[],
                source=source_name,
                details=str(wl.details or body_text or "")[:700],
                map_num=wl.map_num or _normalize_map_num(forced_map_num),
                match_odds=match_diag,
            ))
        if map_odds:
            return _with_acq(SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=map_odds[:2],
                source="deeplink_map_market",
                details=str(body_text or "")[:700],
            ))
        if _is_map_market_closed(site, body_text, forced_map_num=forced_map_num):
            map_context_active = _is_map_context_active(body_text or visible, forced_map_num)
            source_name = "deeplink_map_market_closed"
            if map_context_active:
                source_name = _map_closed_source(site)
            return _with_acq(SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source=source_name,
                details=str(body_text or "")[:700],
                market_closed=True,
            ))

        deep_sources: List[Tuple[str, str]] = []
        if body_text:
            deep_sources.append(("dom_body_text", body_text))
        if visible:
            deep_sources.append(("dom_visible_text", visible))
        if html:
            deep_sources.append(("dom_html", html))
        found_deep, odds_deep, source_deep, details_deep = _find_from_sources(team1, team2, deep_sources)
        deep_context_text = details_deep or body_text or visible
        map_context_active = _is_map_context_active(deep_context_text, forced_map_num)
        if map_context_active and found_deep and odds_deep:
            match_fallback_odds = _extract_first_match_odds(
                site,
                team1,
                team2,
                body_text,
                visible,
                details_deep,
            )
        if map_context_active:
            if found_deep and odds_deep:
                return _with_acq(SiteResult(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=[],
                    source=_match_level_rejected_source(site),
                    details=(details_deep or body_text or _map_context_details(site, "match_level_rejected"))[:700],
                    match_odds=match_fallback_odds,
                ))
            if _is_map_market_closed(site, deep_context_text, forced_map_num=forced_map_num):
                return _with_acq(SiteResult(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=found_deep,
                    odds=[],
                    source=_map_closed_source(site),
                    details=(details_deep or body_text or _map_context_details(site, "map_market_closed"))[:700],
                    market_closed=True,
                    match_odds=match_fallback_odds,
                ))
            return _with_acq(SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=found_deep,
                odds=[],
                source=_map_missing_source(site),
                details=(details_deep or body_text or _map_context_details(site, "map_market_missing"))[:700],
                match_odds=match_fallback_odds,
            ))
        if found_deep and odds_deep and _looks_map_context(details_deep):
            return _with_acq(SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=odds_deep[:2],
                source=f"deeplink_{source_deep}",
                details=details_deep,
            ))
        if found_deep and _is_map_market_closed(site, details_deep or body_text, forced_map_num=forced_map_num):
            return _with_acq(SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source=f"deeplink_{source_deep or 'map_market'}_closed",
                details=(details_deep or body_text)[:700],
                market_closed=True,
            ))
        if map_odds:
            return _with_acq(SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=map_odds[:2],
                source=f"deeplink_{source_deep}",
                details=details_deep,
            ))
        return _with_acq(SiteResult(
            site=site,
            url=url,
            status=load_status,
            match_found=False,
            odds=[],
            source="",
            details="deeplink loaded but map market odds not found",
        ))

    candidate_urls = _candidate_match_urls_from_html(
        site,
        str(getattr(page, "url", "") or url),
        html,
    )
    # The dedicated Winline pollers deliberately share one live-listing page.
    # Following a candidate href here moves that physical page away from the
    # listing and can add another 20+ seconds after a bounded reload timeout.
    # The listing DOM below already contains the team-scoped current-map card,
    # so acquisition-mode callers must classify that snapshot in place.
    href_opened = ""
    if not (site == "winline" and effective_acq in ACQUISITION_MODES):
        href_opened = await _camoufox_find_match_by_urls_async(
            page,
            site,
            candidate_urls,
            team1,
            team2,
        )
    if href_opened:
        map_odds, body_text = await _parse_map_market_on_current_camoufox_page_async(
            page,
            site,
            team1,
            team2,
            forced_map_num=forced_map_num,
        )
        if site == "winline":
            wl = _extract_winline_current_map_winner(
                body_text or "",
                team1,
                team2,
                forced_map_num=forced_map_num,
            )
            match_diag = _extract_first_match_odds(site, team1, team2, body_text, visible)
            if wl.odds and await _winline_page_odds_bettable(
                page, team1, team2, wl.map_num or forced_map_num
            ) is False:
                wl = _winline_mark_locked_as_closed(wl)
            if wl.odds:
                return _with_acq(_site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=wl.odds[:2],
                    source="feed_href_map_market",
                    details=str(wl.details or body_text or "")[:700],
                    market_kind=wl.market_kind,
                    map_num=wl.map_num,
                    p1_team=wl.p1_team,
                    p2_team=wl.p2_team,
                    card_team_order=wl.card_team_order,
                    card_odds=wl.card_odds,
                    match_odds=match_diag,
                ))
            if wl.market_closed or wl.reason == "closed":
                return _with_acq(_site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=[],
                    source="feed_href_map_market_closed",
                    details=str(wl.details or body_text or "")[:700],
                    market_closed=True,
                    market_kind="current_map_winner",
                    map_num=wl.map_num or _normalize_map_num(forced_map_num),
                    match_odds=match_diag,
                ))
            if map_odds:
                # Non-strict generic map_odds must not pass the Winline gate without provenance.
                return _with_acq(_site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=[],
                    source="winline_map_rejected",
                    details=str(wl.details or body_text or "")[:700],
                    map_num=wl.map_num or _normalize_map_num(forced_map_num),
                    match_odds=match_diag,
                ))
            if _is_map_context_active(body_text, forced_map_num):
                return _with_acq(_site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=[],
                    source=_map_missing_source(site),
                    details=str(body_text or "")[:700],
                    map_num=_normalize_map_num(forced_map_num),
                    match_odds=match_diag,
                ))
        elif map_odds:
            return _with_acq(SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=map_odds[:2],
                source="feed_href_map_market",
                details=str(body_text or "")[:700],
            ))
        if site != "winline" and _is_map_market_closed(site, body_text, forced_map_num=forced_map_num):
            return _with_acq(SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source="feed_href_map_market_closed",
                details=str(body_text or "")[:700],
                market_closed=True,
            ))
        if site != "winline" and _is_map_context_active(body_text, forced_map_num):
            return _with_acq(SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source=_map_missing_source(site),
                details=str(body_text or "")[:700],
            ))

    feed_sources: List[Tuple[str, str]] = []
    if body_text:
        feed_sources.append(("dom_body_text", body_text))
    if visible:
        feed_sources.append(("dom_visible_text", visible))
    if html:
        feed_sources.append(("dom_html", html))

    feed_found, _feed_odds_ignored, feed_source_name, feed_details = _find_from_sources(team1, team2, feed_sources)
    feed_map_odds: List[float] = []
    for context in (body_text, visible, feed_details):
        feed_map_odds = _extract_map_odds_from_feed_context(
            site,
            context or "",
            team1=team1,
            team2=team2,
            forced_map_num=forced_map_num,
        )
        if feed_map_odds:
            break
    if feed_found and feed_map_odds:
        if site == "winline":
            winline_card = _winline_matched_card_context(
                body_text or visible or feed_details or "",
                team1,
                team2,
                html=html,
                map_num=_normalize_map_num(forced_map_num),
            )
            wl = _extract_winline_current_map_winner(
                winline_card or "",
                team1,
                team2,
                forced_map_num=forced_map_num,
            )
            if wl.odds and _winline_map_odds_bettable(
                html, team1, team2, wl.map_num or forced_map_num
            ) is False:
                wl = _winline_mark_locked_as_closed(wl)
            if wl.odds:
                return _with_acq(_site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=wl.odds[:2],
                    source=f"{feed_source_name or 'feed'}_map_row",
                    details=(wl.details or feed_details or body_text or visible)[:700],
                    market_kind=wl.market_kind,
                    map_num=wl.map_num,
                    p1_team=wl.p1_team,
                    p2_team=wl.p2_team,
                    card_team_order=wl.card_team_order,
                    card_odds=wl.card_odds,
                ))
        else:
            return _with_acq(SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=feed_map_odds[:2],
                source=f"{feed_source_name or 'feed'}_map_row",
                details=(feed_details or body_text or visible)[:700],
            ))

    sources: List[Tuple[str, str]] = []
    if body_text:
        sources.append(("dom_body_text", body_text))
    if visible:
        sources.append(("dom_visible_text", visible))
    if html:
        sources.append(("dom_html", html))

    found, _odds_ignored, source_name, details = _find_from_sources(team1, team2, sources)
    winline_context = body_text or visible or details or ""
    winline_card = _winline_matched_card_context(
        winline_context,
        team1,
        team2,
        html=html,
        map_num=_normalize_map_num(forced_map_num),
    )
    if (
        site == "winline"
        and found
        and winline_card
        and not (mode == "live" and _looks_future_context(winline_card))
    ):
        if load_error:
            winline_card = f"{winline_card} | load_error={load_error[:300]}"
        wl = _extract_winline_current_map_winner(
            winline_card,
            team1,
            team2,
            forced_map_num=forced_map_num,
        )
        if not wl.odds and series_last_map and html:
            # Решающая карта серии: рынка карты Winline может не выставить вовсе
            # и оставить только «Матч» — это одно и то же событие. Промоция живёт
            # в структурном (DOM) разборе, потому что двухисходность рынка видна
            # только по классам кнопок. Добор строго АДДИТИВНЫЙ: текстовый разбор
            # уже отработал, промоция может лишь добавить кэфы, но не отнять.
            promoted = _extract_winline_current_map_winner(
                winline_card,
                team1,
                team2,
                forced_map_num=forced_map_num,
                html=html,
                series_last_map=True,
            )
            if promoted.odds:
                wl = promoted
        if wl.odds and _winline_map_odds_bettable(
            html, team1, team2, wl.map_num or forced_map_num
        ) is False:
            wl = _winline_mark_locked_as_closed(wl)
        if wl.odds:
            return _with_acq(_site_result_with_provenance(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=wl.odds[:2],
                source=f"{source_name or 'dom'}_map_row",
                details=str(wl.details or details or winline_context)[:700],
                market_kind=wl.market_kind,
                map_num=wl.map_num,
                p1_team=wl.p1_team,
                p2_team=wl.p2_team,
                card_team_order=wl.card_team_order,
                card_odds=wl.card_odds,
            ))
        if wl.reason == "closed" or wl.market_closed:
            winline_source = _map_closed_source(site)
        elif wl.reason == "match":
            winline_source = _match_level_rejected_source(site)
        elif wl.reason == "ambiguous":
            winline_source = "winline_ambiguous_order_rejected"
        else:
            winline_source = _map_missing_source(site)
        return _with_acq(_site_result_with_provenance(
            site=site,
            url=url,
            status=load_status,
            match_found=True,
            odds=[],
            source=winline_source,
            details=str(wl.details or details or winline_context)[:700],
            market_closed=bool(wl.market_closed),
            market_kind="current_map_winner" if wl.market_closed else None,
            map_num=wl.map_num or _normalize_map_num(forced_map_num),
        ))

    strict_map_odds: List[float] = []
    # Для Winline сюда попадают только случаи, где карточка пары НЕ доказана
    # (иначе строгая ветка выше уже вернула результат). Общий добор по окну
    # вокруг названий брал первую строку рынка в этом окне — то есть кэфы
    # соседнего матча, — поэтому для Winline его нет.
    if site != "winline":
        for context in (body_text, visible, details):
            strict_map_odds = _extract_map_odds_from_feed_context(
                site,
                context or "",
                team1=team1,
                team2=team2,
                forced_map_num=forced_map_num,
            )
            if strict_map_odds:
                break
    odds: List[float] = []
    if strict_map_odds:
        odds = strict_map_odds[:2]
        source_name = f"{source_name or 'dom'}_map_row"
    market_closed = False
    context_text = details or body_text or visible or ""
    map_context_active = _is_map_context_active(context_text, forced_map_num)
    if map_context_active and found and _odds_ignored:
        match_fallback_odds = _extract_first_match_odds(
            site,
            team1,
            team2,
            initial_body_text,
            visible,
            body_text,
            details,
        )
    if found and not odds and _is_map_market_closed(site, context_text, forced_map_num=forced_map_num):
        market_closed = True
        source_name = _map_closed_source(site) if map_context_active else (source_name or "map_market_closed")
        details = (details or body_text or _map_context_details(site, "map_market_closed"))[:700]
    if map_context_active and not odds:
        if market_closed:
            source_name = source_name or _map_closed_source(site)
        elif found and _odds_ignored:
            source_name = _match_level_rejected_source(site)
            details = (details or body_text or _map_context_details(site, "match_level_rejected"))[:700]
        else:
            source_name = _map_missing_source(site)
            details = (details or body_text or _map_context_details(site, "map_market_missing"))[:700]
    if mode == "live" and found and _looks_future_context(details):
        found = False
        odds = []
        match_fallback_odds = []
        source_name = ""
        details = "match found but filtered as non-live (future context)"
    if load_error:
        details = f"{details} | load_error={load_error[:300]}"
    return _with_acq(SiteResult(
        site=site,
        url=url,
        status=load_status,
        match_found=found,
        odds=odds,
        source=source_name,
        details=details,
        market_closed=market_closed,
        match_odds=match_fallback_odds,
    ))


def parse_site(
    drv,
    site: str,
    url: str,
    team1: str,
    team2: str,
    mode: str,
    forced_map_num: Optional[int] = None,
) -> SiteResult:
    load_status, load_error, html, visible = _load_site_render_payload(
        drv,
        url,
        initial_wait_seconds=7.0,
        scroll_wait_seconds=2.0,
    )
    soup = BeautifulSoup(html, "html.parser")
    body_text = ""
    try:
        body_text = drv.find_element(By.TAG_NAME, "body").text
    except Exception:
        body_text = ""
    initial_body_text = body_text
    match_fallback_odds: List[float] = []

    # Deep-link mode: parse map-level odds directly from match page.
    if _is_deeplink(site, url):
        if not body_text:
            for _ in range(8):
                time.sleep(1.0)
                try:
                    body_text = " ".join(drv.find_element(By.TAG_NAME, "body").text.split())
                except Exception:
                    body_text = ""
                if body_text:
                    break
        if site == "pari":
            for i in range(8):
                if ("КИБЕРСПОРТ / DOTA 2" in body_text or "КИБЕРСПОРТ / DOTA2" in body_text) and "Исход" in body_text:
                    break
                if i == 3:
                    try:
                        drv.refresh()
                    except Exception:
                        pass
                time.sleep(1.0)
                try:
                    body_text = " ".join(drv.find_element(By.TAG_NAME, "body").text.split())
                except Exception:
                    pass
        map_odds, body_text = _parse_map_market_on_current_page(
            drv,
            site,
            team1,
            team2,
            forced_map_num=forced_map_num,
        )
        if map_odds:
            detail = body_text[:700]
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=map_odds[:2],
                source="deeplink_map_market",
                details=detail,
            )
        if _is_map_market_closed(site, body_text, forced_map_num=forced_map_num):
            map_context_active = _is_map_context_active(body_text or visible, forced_map_num)
            source_name = "deeplink_map_market_closed"
            if map_context_active:
                source_name = _map_closed_source(site)
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source=source_name,
                details=body_text[:700],
                market_closed=True,
            )

        deep_sources: List[Tuple[str, str]] = []
        if body_text:
            deep_sources.append(("dom_body_text", body_text))
        if visible:
            deep_sources.append(("dom_visible_text", visible))
        if html:
            deep_sources.append(("dom_html", html))
        for txt in _iter_request_texts(drv):
            deep_sources.append(("network_response", txt))
        found_deep, odds_deep, source_deep, details_deep = _find_from_sources(team1, team2, deep_sources)
        deep_context_text = details_deep or body_text or visible
        map_context_active = _is_map_context_active(deep_context_text, forced_map_num)
        if map_context_active and found_deep and odds_deep:
            match_fallback_odds = _extract_first_match_odds(
                site,
                team1,
                team2,
                body_text,
                visible,
                details_deep,
            )
        if map_context_active:
            if found_deep and odds_deep:
                return SiteResult(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=[],
                    source=_match_level_rejected_source(site),
                    details=(details_deep or body_text or _map_context_details(site, "match_level_rejected"))[:700],
                    match_odds=match_fallback_odds,
                )
            if _is_map_market_closed(
                site,
                deep_context_text,
                forced_map_num=forced_map_num,
            ):
                return SiteResult(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=found_deep,
                    odds=[],
                    source=_map_closed_source(site),
                    details=(details_deep or body_text or _map_context_details(site, "map_market_closed"))[:700],
                    market_closed=True,
                    match_odds=match_fallback_odds,
                )
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=found_deep,
                odds=[],
                source=_map_missing_source(site),
                details=(details_deep or body_text or _map_context_details(site, "map_market_missing"))[:700],
                match_odds=match_fallback_odds,
            )
        if found_deep and odds_deep and _looks_map_context(details_deep):
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=odds_deep[:2],
                source=f"deeplink_{source_deep}",
                details=details_deep,
            )
        if found_deep and _is_map_market_closed(
            site,
            details_deep or body_text,
            forced_map_num=forced_map_num,
        ):
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source=f"deeplink_{source_deep or 'map_market'}_closed",
                details=(details_deep or body_text)[:700],
                market_closed=True,
            )
        if map_odds:
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=map_odds[:2],
                source=f"deeplink_{source_deep}",
                details=details_deep,
            )
        return SiteResult(
            site=site,
            url=url,
            status=load_status,
            match_found=False,
            odds=[],
            source="",
            details="deeplink loaded but map market odds not found",
        )

    # Feed mode: open match details by team names and parse map market there.
    candidate_urls = _candidate_match_urls_from_html(site, drv.current_url, html)
    href_opened = _find_match_by_urls(drv, site, candidate_urls, team1, team2)
    if href_opened:
        map_odds, body_text = _parse_map_market_on_current_page(
            drv,
            site,
            team1,
            team2,
            forced_map_num=forced_map_num,
        )
        if site == "winline":
            wl = _extract_winline_current_map_winner(
                body_text or "",
                team1,
                team2,
                forced_map_num=forced_map_num,
            )
            match_diag = _extract_first_match_odds(site, team1, team2, body_text, visible)
            if wl.odds and _winline_driver_odds_bettable(
                drv, team1, team2, wl.map_num or forced_map_num
            ) is False:
                wl = _winline_mark_locked_as_closed(wl)
            if wl.odds:
                return _site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=wl.odds[:2],
                    source="feed_href_map_market",
                    details=str(wl.details or body_text or "")[:700],
                    market_kind=wl.market_kind,
                    map_num=wl.map_num,
                    p1_team=wl.p1_team,
                    p2_team=wl.p2_team,
                    card_team_order=wl.card_team_order,
                    card_odds=wl.card_odds,
                    match_odds=match_diag,
                )
            if wl.market_closed or wl.reason == "closed":
                return _site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=[],
                    source="feed_href_map_market_closed",
                    details=str(wl.details or body_text or "")[:700],
                    market_closed=True,
                    market_kind="current_map_winner",
                    map_num=wl.map_num or _normalize_map_num(forced_map_num),
                    match_odds=match_diag,
                )
            if map_odds:
                return _site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=[],
                    source="winline_map_rejected",
                    details=str(wl.details or body_text or "")[:700],
                    map_num=wl.map_num or _normalize_map_num(forced_map_num),
                    match_odds=match_diag,
                )
            if _is_map_context_active(body_text, forced_map_num):
                return _site_result_with_provenance(
                    site=site,
                    url=url,
                    status=load_status,
                    match_found=True,
                    odds=[],
                    source=_map_missing_source(site),
                    details=str(body_text or "")[:700],
                    map_num=_normalize_map_num(forced_map_num),
                    match_odds=match_diag,
                )
        elif map_odds:
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=map_odds[:2],
                source="feed_href_map_market",
                details=body_text[:700],
            )
        if site != "winline" and _is_map_market_closed(site, body_text, forced_map_num=forced_map_num):
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source="feed_href_map_market_closed",
                details=body_text[:700],
                market_closed=True,
            )
        if site != "winline" and _is_map_context_active(body_text, forced_map_num):
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source=_map_missing_source(site),
                details=body_text[:700],
            )

    opened_url = _open_match_details_by_teams(drv, site, team1, team2)
    if opened_url:
        time.sleep(1.5)
        map_odds, body_text = _parse_map_market_on_current_page(
            drv,
            site,
            team1,
            team2,
            forced_map_num=forced_map_num,
        )
        if map_odds:
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=map_odds[:2],
                source="feed_click_map_market",
                details=body_text[:700],
            )
        if _is_map_market_closed(site, body_text, forced_map_num=forced_map_num):
            map_context_active = _is_map_context_active(body_text or visible, forced_map_num)
            source_name = "feed_click_map_market_closed"
            if map_context_active:
                source_name = _map_closed_source(site)
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source=source_name,
                details=body_text[:700],
                market_closed=True,
            )
        if _is_map_context_active(body_text, forced_map_num):
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source=_map_missing_source(site),
                details=body_text[:700],
            )

    feed_sources: List[Tuple[str, str]] = []
    if body_text:
        feed_sources.append(("dom_body_text", body_text))
    if visible:
        feed_sources.append(("dom_visible_text", visible))
    if html:
        feed_sources.append(("dom_html", html))
    for txt in _iter_request_texts(drv):
        feed_sources.append(("network_response", txt))

    feed_found, _feed_odds_ignored, feed_source_name, feed_details = _find_from_sources(team1, team2, feed_sources)
    feed_map_odds: List[float] = []
    for context in (body_text, visible, feed_details):
        feed_map_odds = _extract_map_odds_from_feed_context(
            site,
            context or "",
            team1=team1,
            team2=team2,
            forced_map_num=forced_map_num,
        )
        if feed_map_odds:
            break
    if feed_found and feed_map_odds:
        return SiteResult(
            site=site,
            url=url,
            status=load_status,
            match_found=True,
            odds=feed_map_odds[:2],
            source=f"{feed_source_name or 'feed'}_map_row",
            details=(feed_details or body_text or visible)[:700],
        )

    sources: List[Tuple[str, str]] = []
    if body_text:
        sources.append(("dom_body_text", body_text))
    if visible:
        sources.append(("dom_visible_text", visible))
    if html:
        sources.append(("dom_html", html))
    for txt in _iter_request_texts(drv):
        sources.append(("network_response", txt))

    found, _odds_ignored, source_name, details = _find_from_sources(team1, team2, sources)
    # Fallback odds are disabled: only strict map-row/deeplink parsing is allowed.
    strict_map_odds: List[float] = []
    # Для Winline сюда попадают только случаи, где карточка пары НЕ доказана
    # (иначе строгая ветка выше уже вернула результат). Общий добор по окну
    # вокруг названий брал первую строку рынка в этом окне — то есть кэфы
    # соседнего матча, — поэтому для Winline его нет.
    if site != "winline":
        for context in (body_text, visible, details):
            strict_map_odds = _extract_map_odds_from_feed_context(
                site,
                context or "",
                team1=team1,
                team2=team2,
                forced_map_num=forced_map_num,
            )
            if strict_map_odds:
                break
    odds: List[float] = []
    if strict_map_odds:
        odds = strict_map_odds[:2]
        source_name = f"{source_name or 'dom'}_map_row"
    market_closed = False
    context_text = details or body_text or visible or ""
    map_context_active = _is_map_context_active(context_text, forced_map_num)
    if map_context_active and found and _odds_ignored:
        match_fallback_odds = _extract_first_match_odds(
            site,
            team1,
            team2,
            initial_body_text,
            visible,
            body_text,
            details,
        )
    if found and not odds and _is_map_market_closed(
        site,
        context_text,
        forced_map_num=forced_map_num,
    ):
        market_closed = True
        source_name = _map_closed_source(site) if map_context_active else (source_name or "map_market_closed")
        details = (details or body_text or _map_context_details(site, "map_market_closed"))[:700]
    if map_context_active and not odds:
        if market_closed:
            source_name = source_name or _map_closed_source(site)
        elif found and _odds_ignored:
            source_name = _match_level_rejected_source(site)
            details = (details or body_text or _map_context_details(site, "match_level_rejected"))[:700]
        else:
            source_name = _map_missing_source(site)
            details = (details or body_text or _map_context_details(site, "map_market_missing"))[:700]
    if mode == "live" and found and _looks_future_context(details):
        found = False
        odds = []
        match_fallback_odds = []
        source_name = ""
        details = "match found but filtered as non-live (future context)"
    if load_error:
        details = f"{details} | load_error={load_error[:300]}"
    return SiteResult(
        site=site,
        url=url,
        status=load_status,
        match_found=found,
        odds=odds,
        source=source_name,
        details=details,
        market_closed=market_closed,
        match_odds=match_fallback_odds,
    )


def parse_presence_site(
    drv,
    site: str,
    url: str,
    team1: str,
    team2: str,
    mode: str = "live",
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
) -> SiteResult:
    last_status = "ok"
    last_error = ""
    last_details = "match not found in rendered DOM/network payload"
    for attempt_idx in range(2):
        load_status, load_error, html, visible = _load_site_render_payload(
            drv,
            url,
            initial_wait_seconds=10.0 + (2.0 * attempt_idx),
            scroll_wait_seconds=2.0,
        )
        body_text = ""
        try:
            body_text = drv.find_element(By.TAG_NAME, "body").text
        except Exception:
            body_text = ""

        sources: List[Tuple[str, str]] = []
        if body_text:
            sources.append(("dom_body_text", body_text))
        if visible:
            sources.append(("dom_visible_text", visible))
        if html:
            sources.append(("dom_html", html))
        for txt in _iter_request_texts(drv):
            sources.append(("network_response", txt))

        found, source_name, details = _find_presence_from_sources(
            team1,
            team2,
            sources,
            team1_aliases=team1_aliases,
            team2_aliases=team2_aliases,
        )
        if load_error:
            details = f"{details} | load_error={load_error[:300]}" if details else f"load_error={load_error[:300]}"
        last_status = load_status
        last_error = load_error
        last_details = details or "match not found in rendered DOM/network payload"
        if found:
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source=source_name or "presence_found",
                details=last_details,
                market_closed=False,
                match_odds=[],
            )
        if attempt_idx == 0:
            try:
                drv.refresh()
                time.sleep(2.0)
            except Exception:
                pass
    if last_error and "load_error=" not in last_details:
        last_details = f"{last_details} | load_error={last_error[:300]}"
    return SiteResult(
        site=site,
        url=url,
        status=last_status,
        match_found=False,
        odds=[],
        source="presence_missing",
        details=last_details,
        market_closed=False,
        match_odds=[],
    )


def _run_presence_site_task(
    *,
    site: str,
    url: str,
    team1: str,
    team2: str,
    mode: str,
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
) -> SiteResult:
    drv = _build_driver(BOOKMAKER_PROXY_URL)
    try:
        return parse_presence_site(
            drv,
            site=site,
            url=url,
            team1=team1,
            team2=team2,
            mode=mode,
            team1_aliases=team1_aliases,
            team2_aliases=team2_aliases,
        )
    finally:
        try:
            drv.quit()
        except Exception:
            pass


def _presence_sources_from_current_tab(drv, *, host: str) -> List[Tuple[str, str]]:
    html = ""
    visible = ""
    body_text = ""
    try:
        html = drv.page_source or ""
    except Exception:
        html = ""
    if html:
        try:
            soup = BeautifulSoup(html, "html.parser")
            visible = " ".join(soup.stripped_strings)
        except Exception:
            visible = ""
    try:
        body_text = drv.find_element(By.TAG_NAME, "body").text or ""
    except Exception:
        body_text = ""

    sources: List[Tuple[str, str]] = []
    if body_text:
        sources.append(("dom_body_text", body_text))
    if visible:
        sources.append(("dom_visible_text", visible))
    if html:
        sources.append(("dom_html", html))
    for txt in _iter_request_texts_for_host(drv, host):
        sources.append(("network_response", txt))
    return sources


def _probe_presence_site_in_current_tab(
    drv,
    *,
    site: str,
    url: str,
    team1: str,
    team2: str,
    mode: str,
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
    extra_reload_on_empty: bool = False,
    extra_scroll_passes: int = 0,
) -> SiteResult:
    try:
        drv.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.2)
        drv.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.5);")
        time.sleep(0.2)
        drv.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except Exception:
        pass
    if extra_scroll_passes:
        for _ in range(max(0, int(extra_scroll_passes))):
            try:
                drv.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.6)
                drv.execute_script("window.scrollTo(0, 0);")
                time.sleep(0.4)
                drv.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.5);")
                time.sleep(0.4)
            except Exception:
                break

    current_url, ready_state, page_title, body_len, sources = _presence_collect_probe_snapshot(
        drv,
        url=url,
    )
    found, source_name, details = _find_presence_from_sources(
        team1,
        team2,
        sources,
        team1_aliases=team1_aliases,
        team2_aliases=team2_aliases,
    )

    status = "ok"
    if not sources:
        status = "loading"
    elif ready_state and ready_state != "complete":
        status = "loading"
    elif body_len < 240:
        status = "loading"

    opened_match_url = ""
    if (
        not found
        and _presence_should_open_match_details(
            site,
            current_url or url,
            body_len=body_len,
            source_count=len(sources),
        )
    ):
        html_text = ""
        for source_name_candidate, text in sources:
            if source_name_candidate == "dom_html" and text:
                html_text = text
                break
        candidate_urls = _candidate_match_urls_from_html(site, current_url or url, html_text)
        opened_match_url = _find_match_by_urls(drv, site, candidate_urls, team1, team2) or ""
        if not opened_match_url:
            opened_match_url = _open_match_details_by_teams(drv, site, team1, team2) or ""
        if opened_match_url:
            time.sleep(1.5 if site == "pari" else 1.0)
            if site == "pari":
                try:
                    drv.execute_script("window.scrollTo(0, 0);")
                    time.sleep(0.4)
                    drv.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.35);")
                    time.sleep(0.4)
                except Exception:
                    pass
            current_url, ready_state, page_title, body_len, sources = _presence_collect_probe_snapshot(
                drv,
                url=url,
            )
            found, source_name, details = _find_presence_from_sources(
                team1,
                team2,
                sources,
                team1_aliases=team1_aliases,
                team2_aliases=team2_aliases,
            )
            status = "ok"
            if not sources:
                status = "loading"
            elif ready_state and ready_state != "complete":
                status = "loading"
            elif body_len < 240:
                status = "loading"

    details = details or "match not found in rendered DOM/network payload"
    meta_bits = []
    if current_url:
        meta_bits.append(f"current_url={current_url[:220]}")
    if ready_state:
        meta_bits.append(f"ready_state={ready_state}")
    if page_title:
        meta_bits.append(f"title={page_title[:160]}")
    meta_bits.append(f"sources={len(sources)}")
    meta_bits.append(f"body_len={body_len}")
    if opened_match_url:
        meta_bits.append(f"opened_match_url={opened_match_url[:220]}")
    if meta_bits:
        details = f"{details} | {'; '.join(meta_bits)}"

    if found:
        return SiteResult(
            site=site,
            url=url,
            status=status,
            match_found=True,
            odds=[],
            source=source_name or "presence_found_tab",
            details=details,
            market_closed=False,
            match_odds=[],
        )

    if mode == "live" and _looks_future_context(details):
        details = f"match found but filtered as non-live (future context) | {'; '.join(meta_bits)}"

    if extra_reload_on_empty and body_len == 0:
        try:
            drv.refresh()
            time.sleep(3.0)
            return _probe_presence_site_in_current_tab(
                drv,
                site=site,
                url=url,
                team1=team1,
                team2=team2,
                mode=mode,
                team1_aliases=team1_aliases,
                team2_aliases=team2_aliases,
                extra_reload_on_empty=False,
            )
        except Exception:
            pass

    # OCR fallback: try screenshots if teams not found
    if not found and status == "loading":
        try:
            from base.bookmaker_ocr import check_bookmaker_presence_via_ocr
            ocr_result = check_bookmaker_presence_via_ocr(
                drv, site, url, team1, team2,
                team1_aliases=team1_aliases,
                team2_aliases=team2_aliases
            )
            if ocr_result.match_found:
                return SiteResult(
                    site=site,
                    url=url,
                    status="ok",
                    match_found=True,
                    odds=[],
                    source="ocr_fallback",
                    details=ocr_result.details,
                    market_closed=False,
                    match_odds=[],
                )
        except Exception:
            pass

    return SiteResult(
        site=site,
        url=url,
        status=status,
        match_found=False,
        odds=[],
        source="presence_missing",
        details=details,
        market_closed=False,
        match_odds=[],
    )


def _open_presence_site_tabs(
    drv,
    *,
    selected_sites: List[str],
    urls: Dict[str, str],
) -> Dict[str, str]:
    handle_by_site: Dict[str, str] = {}
    if not selected_sites:
        return handle_by_site

    drv.get("about:blank")
    base_handle = drv.current_window_handle

    for site in selected_sites:
        before = set(drv.window_handles)
        drv.execute_script("window.open(arguments[0], '_blank');", urls[site])
        new_handles = [handle for handle in drv.window_handles if handle not in before]
        new_handle = new_handles[0] if new_handles else drv.window_handles[-1]
        handle_by_site[site] = new_handle

        # Wait for tab to load before moving to next
        drv.switch_to.window(new_handle)
        try:
            WebDriverWait(drv, 15).until(lambda d: d.execute_script("return document.readyState") == "complete")
            time.sleep(1.0)  # Extra wait for JS rendering
        except Exception:
            time.sleep(4.0)  # Fallback wait
        time.sleep(0.15)

    try:
        drv.switch_to.window(base_handle)
        drv.close()
    except Exception:
        pass

    return handle_by_site


# Singleton driver and base tabs for reuse across calls
_presence_driver: Optional[Any] = None
_presence_base_handles: Dict[str, str] = {}
_presence_base_initialized = False


def _get_presence_driver() -> Any:
    """Get or create singleton presence driver."""
    global _presence_driver
    if _presence_driver is None:
        _presence_driver = _build_driver(BOOKMAKER_PROXY_URL, page_load_timeout=25)
    return _presence_driver


def _ensure_presence_base_tabs(
    drv: Any,
    urls: Dict[str, str],
    selected_sites: List[str],
) -> Dict[str, str]:
    """Open base tabs once, reuse for all presence checks."""
    global _presence_base_handles, _presence_base_initialized
    if _presence_base_initialized:
        return _presence_base_handles

    drv.get("about:blank")
    base_handle = drv.current_window_handle

    for site in selected_sites:
        before = set(drv.window_handles)
        try:
            drv.execute_script("window.open(arguments[0], '_blank');", urls[site])
            new_handles = [h for h in drv.window_handles if h not in before]
            if new_handles:
                _presence_base_handles[site] = new_handles[0]
            time.sleep(0.2)
        except Exception:
            continue

    _presence_base_initialized = True
    return _presence_base_handles


def _run_presence_sites_in_browser(
    *,
    selected_sites: List[str],
    urls: Dict[str, str],
    team1: str,
    team2: str,
    mode: str,
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
) -> List[SiteResult]:
    drv = _get_presence_driver()
    _ensure_presence_base_tabs(drv, urls, selected_sites)

    pending = list(selected_sites)
    results_by_site: Dict[str, SiteResult] = {}
    deadline = time.monotonic() + 180.0
    time.sleep(2.0)

    while pending and time.monotonic() < deadline:
        next_pending: List[str] = []
        for site in pending:
            handle = _presence_base_handles.get(site)
            if not handle:
                results_by_site[site] = SiteResult(
                    site=site,
                    url=urls[site],
                    status="request_error",
                    match_found=False,
                    odds=[],
                    source="tab_handle_missing",
                    details="window handle missing",
                    market_closed=False,
                    match_odds=[],
                )
                continue
            try:
                drv.switch_to.window(handle)
                try:
                    drv.execute_script("window.focus();")
                except Exception:
                    pass
                time.sleep(6.0)
                result = _probe_presence_site_in_current_tab(
                    drv,
                    site=site,
                    url=urls[site],
                    team1=team1,
                    team2=team2,
                    mode=mode,
                    team1_aliases=team1_aliases,
                    team2_aliases=team2_aliases,
                    extra_reload_on_empty=(site == "pari"),
                    extra_scroll_passes=(2 if site in {"betboom", "winline"} else 1 if site == "pari" else 0),
                )
            except Exception as exc:
                result = SiteResult(
                    site=site,
                    url=urls[site],
                    status="request_error",
                    match_found=False,
                    odds=[],
                    source="tab_probe_error",
                    details=str(exc),
                    market_closed=False,
                    match_odds=[],
                )
            results_by_site[site] = result
            if not result.match_found and result.status == "loading":
                next_pending.append(site)
        pending = next_pending
        if pending:
            time.sleep(3.0)

    for site in selected_sites:
        if site in results_by_site:
            continue
        results_by_site[site] = SiteResult(
            site=site,
            url=urls[site],
            status="request_error",
            match_found=False,
            odds=[],
            source="presence_missing",
            details="no probe result collected",
            market_closed=False,
            match_odds=[],
        )

    # OCR fallback: try screenshots for sites that timed out or didn't find match
    try:
        from base.bookmaker_ocr import check_bookmaker_presence_via_ocr
        for site in selected_sites:
            result = results_by_site.get(site)
            if result and not result.match_found and result.status in {"loading", "request_error", "ok"}:
                # Try OCR only if not already confirmed as not found
                tab_handle = _presence_base_handles.get(site)
                ocr_result = check_bookmaker_presence_via_ocr(
                    drv, site, urls[site], team1, team2,
                    team1_aliases=team1_aliases,
                    team2_aliases=team2_aliases,
                    tab_handle=tab_handle
                )
                if ocr_result.match_found:
                    results_by_site[site] = SiteResult(
                        site=site,
                        url=urls[site],
                        status="ok",
                        match_found=True,
                        odds=[],
                        source="ocr_fallback",
                        details=ocr_result.details,
                        market_closed=False,
                        match_odds=[],
                    )
    except Exception:
        pass

    return [results_by_site[site] for site in selected_sites]


async def run_presence_sites_parallel_async(
    *,
    selected_sites: List[str],
    urls: Dict[str, str],
    team1: str,
    team2: str,
    mode: str,
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
) -> List[SiteResult]:
    if BOOKMAKER_CAMOUFOX_PRESENCE_ENABLED:
        return await asyncio.to_thread(
            _run_presence_sites_in_camoufox_sync,
            selected_sites=selected_sites,
            urls=urls,
            team1=team1,
            team2=team2,
            mode=mode,
            team1_aliases=team1_aliases,
            team2_aliases=team2_aliases,
        )
    return _run_presence_sites_in_browser(
        selected_sites=selected_sites,
        urls=urls,
        team1=team1,
        team2=team2,
        mode=mode,
        team1_aliases=team1_aliases,
        team2_aliases=team2_aliases,
    )


async def run_sites_in_camoufox_async(
    *,
    selected_sites: List[str],
    urls: Dict[str, str],
    team1: str,
    team2: str,
    mode: str,
    forced_map_num: Optional[int] = None,
) -> List[SiteResult]:
    """Parse all selected sites without nesting sync Playwright in asyncio."""
    return await asyncio.to_thread(
        _run_sites_in_camoufox_sync,
        selected_sites=selected_sites,
        urls=urls,
        team1=team1,
        team2=team2,
        mode=mode,
        forced_map_num=forced_map_num,
    )


def _run_sites_in_camoufox_sync(
    *,
    selected_sites: List[str],
    urls: Dict[str, str],
    team1: str,
    team2: str,
    mode: str,
    forced_map_num: Optional[int] = None,
) -> List[SiteResult]:
    """Synchronous Camoufox owner used by CLI/subprocess callers."""
    if not CAMOUFOX_AVAILABLE:
        raise RuntimeError("Camoufox is unavailable")
    proxy_kwargs = _camoufox_proxy_kwargs(BOOKMAKER_PROXY_URL)
    results: List[SiteResult] = []
    with camoufox.Camoufox(headless=True, **proxy_kwargs) as browser:
        for site in selected_sites:
            page = browser.new_page()
            try:
                results.append(
                    _run_coroutine_blocking(
                        parse_site_in_camoufox_page_async(
                            page,
                            site=site,
                            url=urls[site],
                            team1=team1,
                            team2=team2,
                            mode=mode,
                            forced_map_num=forced_map_num,
                            # The CLI/manual path must use the same bounded
                            # Winline navigation policy as production.  Legacy
                            # acquisition waits up to 60s before parsing and can
                            # exceed the subprocess deadline without output.
                            acquisition_mode=(
                                "initial_goto" if site == "winline" else None
                            ),
                        )
                    )
                )
            finally:
                with contextlib.suppress(Exception):
                    page.close()
    return results


def verify_proxy(drv) -> str:
    drv.get("https://httpbin.org/ip")
    time.sleep(2)
    txt = drv.find_element(By.TAG_NAME, "body").text.strip()
    try:
        data = json.loads(txt)
        return str(data.get("origin") or txt)
    except Exception:
        return txt


def _parse_bool_arg(raw_value: Optional[str], default: bool = True) -> bool:
    if raw_value is None:
        return bool(default)
    return str(raw_value).strip().lower() in {"1", "true", "yes", "y", "on"}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--team1", required=True)
    parser.add_argument("--team2", required=True)
    parser.add_argument("--team1-alias", action="append", default=[])
    parser.add_argument("--team2-alias", action="append", default=[])
    parser.add_argument(
        "--manual-map-check",
        action="store_true",
        help="Explicit manual map-check path: uses team1/team2/map-num inputs without DLTV name dependency.",
    )
    parser.add_argument("--mode", choices=["live", "all"], default="live")
    parser.add_argument(
        "--match-url",
        action="append",
        default=[],
        help="Override site URL in format site=https://... (can be repeated)",
    )
    parser.add_argument(
        "--sites",
        nargs="*",
        default=None,
        choices=list(SUPPORTED_BOOKMAKER_SITES),
    )
    parser.add_argument("--map-num", type=int, default=None)
    parser.add_argument(
        "--odds",
        default="true",
        help="Enable Selenium odds collection (true/false).",
    )
    parser.add_argument(
        "--presence-only",
        action="store_true",
        help="Rendered DOM presence check only; ignores odds and market parsing.",
    )
    args = parser.parse_args()
    if args.manual_map_check and args.map_num is None:
        parser.error("--manual-map-check requires --map-num")

    urls = dict(BOOKMAKER_URLS[args.mode])
    selected_sites = list(args.sites or SUPPORTED_BOOKMAKER_SITES)
    for raw in args.match_url:
        if "=" not in raw:
            continue
        site, site_url = raw.split("=", 1)
        site = site.strip().lower()
        site_url = site_url.strip()
        if site in urls and site_url:
            urls[site] = site_url

    odds_enabled = _parse_bool_arg(args.odds, default=True)
    proxy_origin: Optional[str] = None
    if odds_enabled or args.presence_only:
        if args.presence_only:
            proxy_origin = "camoufox_presence_proxy_only" if BOOKMAKER_CAMOUFOX_PRESENCE_ENABLED else "parallel_presence_proxy_only"
            results = await run_presence_sites_parallel_async(
                selected_sites=selected_sites,
                urls=urls,
                team1=args.team1,
                team2=args.team2,
                mode=args.mode,
                team1_aliases=args.team1_alias,
                team2_aliases=args.team2_alias,
            )
        else:
            if BOOKMAKER_CAMOUFOX_ENABLED:
                proxy_origin = "camoufox_proxy_only"
                results = await run_sites_in_camoufox_async(
                    selected_sites=selected_sites,
                    urls=urls,
                    team1=args.team1,
                    team2=args.team2,
                    mode=args.mode,
                    forced_map_num=args.map_num,
                )
            else:
                drv = _build_driver(BOOKMAKER_PROXY_URL)
                try:
                    proxy_origin = verify_proxy(drv)
                    results = [
                        parse_site(
                            drv,
                            site=site,
                            url=urls[site],
                            team1=args.team1,
                            team2=args.team2,
                            mode=args.mode,
                            forced_map_num=args.map_num,
                        )
                        for site in selected_sites
                    ]
                finally:
                    drv.quit()
    else:
        proxy_origin = "disabled"
        results = [
            SiteResult(
                site=site,
                url=urls[site],
                status="disabled",
                match_found=False,
                odds=[],
                source="",
                details="odds disabled by flag",
                market_closed=False,
            )
            for site in selected_sites
        ]

    payload = {
        "proxy_url": BOOKMAKER_PROXY_URL,
        "proxy_origin_check": proxy_origin,
        "mode": args.mode,
        "query": {
            "team1": args.team1,
            "team2": args.team2,
            "team1_aliases": list(args.team1_alias or []),
            "team2_aliases": list(args.team2_alias or []),
            "map_num": args.map_num,
            "manual_map_check": bool(args.manual_map_check),
            "presence_only": bool(args.presence_only),
            "sites": selected_sites,
        },
        "results": [r.__dict__ for r in results],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())


def parse_site_in_camoufox_page(*args, **kwargs):
    """Синхронная обёртка над parse_site_in_camoufox_page_async для standalone-вызовов и тестов."""
    return _run_coroutine_blocking(parse_site_in_camoufox_page_async(*args, **kwargs))


def run_sites_in_camoufox(*args, **kwargs):
    """Синхронная обёртка над run_sites_in_camoufox_async для standalone-вызовов и тестов."""
    return _run_coroutine_blocking(run_sites_in_camoufox_async(*args, **kwargs))


def _camoufox_find_match_by_urls(*args, **kwargs):
    """Синхронная обёртка над _camoufox_find_match_by_urls_async (standalone-вызовы и тесты)."""
    return _run_coroutine_blocking(_camoufox_find_match_by_urls_async(*args, **kwargs))


def _load_site_render_payload_camoufox(*args, **kwargs):
    """Синхронная обёртка над _load_site_render_payload_camoufox_async (standalone-вызовы и тесты)."""
    return _run_coroutine_blocking(_load_site_render_payload_camoufox_async(*args, **kwargs))


def _parse_map_market_on_current_camoufox_page(*args, **kwargs):
    """Синхронная обёртка над _parse_map_market_on_current_camoufox_page_async (standalone-вызовы и тесты)."""
    return _run_coroutine_blocking(_parse_map_market_on_current_camoufox_page_async(*args, **kwargs))


def run_presence_sites_parallel(*args, **kwargs):
    """Синхронная обёртка над run_presence_sites_parallel_async (standalone-вызовы и тесты)."""
    return _run_coroutine_blocking(run_presence_sites_parallel_async(*args, **kwargs))
