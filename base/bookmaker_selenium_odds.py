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

import argparse
import concurrent.futures
import contextlib
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
    os.getenv("BOOKMAKER_CAMOUFOX_ENABLED", "0").strip().lower()
    in {"1", "true", "yes", "on"}
) and CAMOUFOX_AVAILABLE
BOOKMAKER_CAMOUFOX_PRESENCE_ENABLED = (
    os.getenv("BOOKMAKER_CAMOUFOX_PRESENCE_ENABLED", "0").strip().lower()
    in {"1", "true", "yes", "on"}
) and CAMOUFOX_AVAILABLE
if BOOKMAKER_CAMOUFOX_ENABLED:
    BOOKMAKER_CAMOUFOX_PRESENCE_ENABLED = True

BOOKMAKER_URLS: Dict[str, Dict[str, str]] = {
    "live": {
        "betboom": "https://betboom.ru/esport/live/dota-2",
        "pari": "https://pari.ru/esports-live/category/dota2",
        "winline": "https://winline.ru/stavki/sport/kibersport",
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
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9а-я]+", " ", s.lower())).strip()


GENERIC_TEAM_TOKENS = {
    "team",
    "gaming",
    "esports",
    "esport",
    "club",
    "squad",
    "academy",
    "junior",
    "juniors",
    "youth",
    "the",
}


def _fallback_search_tokens(team: str) -> List[str]:
    norm = _norm(team or "")
    if not norm:
        return []
    raw_tokens = [token for token in norm.split() if token]
    preferred = [token for token in raw_tokens if len(token) >= 3 and token not in GENERIC_TEAM_TOKENS]
    if not preferred:
        preferred = [token for token in raw_tokens if len(token) >= 2]
    preferred.sort(key=len, reverse=True)
    seen = set()
    out: List[str] = []
    for token in preferred:
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _find_positions_with_fallback(low: str, team: str) -> List[int]:
    direct = [m.start() for m in re.finditer(re.escape((team or "").lower()), low)] if team else []
    if direct:
        return direct
    for token in _fallback_search_tokens(team):
        positions = [m.start() for m in re.finditer(re.escape(token), low)]
        if positions:
            return positions
    return []


def _first_index_with_fallback(low: str, team: str) -> int:
    direct = low.find((team or "").lower()) if team else -1
    if direct != -1:
        return direct
    for token in _fallback_search_tokens(team):
        pos = low.find(token)
        if pos != -1:
            return pos
    return -1


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


def _camoufox_collect_probe_snapshot(page, *, url: str) -> Tuple[str, str, str, int, List[Tuple[str, str]]]:
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
        ready_state = str(page.evaluate("() => document.readyState") or "")
    except Exception:
        ready_state = ""
    try:
        page_title = str(page.title() or "")
    except Exception:
        page_title = ""
    try:
        html = page.content() or ""
    except Exception:
        html = ""
    try:
        body_text = str(page.locator("body").inner_text(timeout=5000) or "")
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


def _camoufox_body_text(page) -> str:
    try:
        return str(page.locator("body").inner_text(timeout=5000) or "")
    except Exception:
        return ""


def _load_site_render_payload_camoufox(
    page,
    url: str,
    *,
    initial_wait_seconds: float = 7.0,
    scroll_wait_seconds: float = 2.0,
) -> Tuple[str, str, str, str, str]:
    load_status = "ok"
    load_error = ""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(max(0.0, float(initial_wait_seconds)))
        try:
            page.evaluate(
                "() => {"
                " window.scrollTo(0, 0);"
                " window.scrollTo(0, document.body.scrollHeight * 0.5);"
                " window.scrollTo(0, document.body.scrollHeight);"
                "}"
            )
            time.sleep(max(0.0, float(scroll_wait_seconds)))
        except Exception:
            pass
    except Exception as exc:
        load_status = "partial_load"
        load_error = str(exc)

    html = ""
    visible = ""
    body_text = ""
    try:
        html = page.content() or ""
    except Exception:
        html = ""
    if html:
        try:
            soup = BeautifulSoup(html, "html.parser")
            visible = " ".join(soup.stripped_strings)
        except Exception:
            visible = ""
    body_text = _camoufox_body_text(page)
    return load_status, load_error, html, visible or body_text, body_text


def _camoufox_try_click_text(page, text_candidates: List[str]) -> bool:
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
            clicked = bool(page.evaluate(script, [[str(label)]]))
        except Exception:
            clicked = False
        if clicked:
            time.sleep(1.0)
            return True
    return False


def _camoufox_click_map_tab_on_current_page(page, site: str, map_num: Optional[int]) -> bool:
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
    return _camoufox_try_click_text(page, labels)


def _parse_map_market_on_current_camoufox_page(
    page,
    site: str,
    team1: str,
    team2: str,
    forced_map_num: Optional[int] = None,
) -> Tuple[List[float], str]:
    body_text = _camoufox_body_text(page)
    map_num = _resolve_map_num_for_site(site, body_text, forced_map_num)
    clicked_tab = _camoufox_click_map_tab_on_current_page(page, site, map_num)
    if not clicked_tab and site == "betboom" and map_num is not None:
        _camoufox_try_click_text(page, [f"Карта {map_num}", f"Карта{map_num}", f"{map_num} карта"])
    elif not clicked_tab and site == "pari" and map_num is not None:
        _camoufox_try_click_text(page, [f"{map_num}-Я КАРТА", f"{map_num}-я карта", f"{map_num} карта", f"Карта {map_num}"])
    elif not clicked_tab and site == "winline" and map_num is not None:
        _camoufox_try_click_text(
            page,
            [f"{map_num}К", f"{map_num} К", f"{map_num}-я карта", f"{map_num} карта", f"Победитель {map_num} карты", f"Победитель {map_num} карт"],
        )
    body_text = _camoufox_body_text(page)
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
            _camoufox_try_click_text(page, [f"{map_num}-Я КАРТА", f"Карта {map_num}"])
            time.sleep(1.5)
            body_text = _camoufox_body_text(page)
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
                    page.reload(wait_until="domcontentloaded", timeout=30000)
                    time.sleep(2.5)
                    body_text = _camoufox_body_text(page)
                    odds = _extract_map_odds_deeplink(
                        site,
                        " ".join((body_text or "").split()),
                        team1,
                        team2,
                        forced_map_num=forced_map_num,
                    )
    return odds, body_text


def _camoufox_find_match_by_urls(page, site: str, urls: List[str], team1: str, team2: str) -> Optional[str]:
    if not urls:
        return None
    t1 = (team1 or "").strip().lower()
    t2 = (team2 or "").strip().lower()
    t1s = t1.split()[0] if t1 else ""
    t2s = t2.split()[0] if t2 else ""
    for target in urls[:12]:
        try:
            page.goto(target, wait_until="domcontentloaded", timeout=25000)
            time.sleep(1.5)
            body = " ".join((page.locator("body").inner_text(timeout=4000) or "").lower().split())
        except Exception:
            continue
        if (
            (t1 and t1 in body and t2 and t2 in body)
            or (t1s and t1s in body and t2s and t2s in body)
            or _text_matches_teams(body, team1, team2)
        ):
            return target
    return None


def _probe_presence_site_in_camoufox_page(
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
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(2.0)
        try:
            page.evaluate(
                "() => { window.scrollTo(0, 0); window.scrollTo(0, document.body.scrollHeight * 0.5); window.scrollTo(0, document.body.scrollHeight); }"
            )
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

    current_url, ready_state, page_title, body_len, sources = _camoufox_collect_probe_snapshot(
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
        opened_match_url = _camoufox_find_match_by_urls(page, site, candidate_urls, team1, team2) or ""
        if opened_match_url:
            current_url, ready_state, page_title, body_len, sources = _camoufox_collect_probe_snapshot(
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


def _run_presence_sites_in_camoufox(
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
            page = browser.new_page()
            try:
                results.append(
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


def _looks_future_context(context: str) -> bool:
    low = context.lower()
    live_markers_present = bool(
        re.search(r"\b\d{1,2}'\b", low)
        or re.search(r"\(\d{1,2}\s*-\s*\d{1,2}\)", low)
        or re.search(r"\b\d{1,2}\s*:\s*\d{1,2}\b", low)
        or re.search(r"\b[1-5]\s*карта\b", low)
        or re.search(r"\b[1-5]карта\b", low)
        or re.search(r"\b[1-5]\s*к\b", low)
    )
    if any(m in low for m in FUTURE_MARKERS):
        if live_markers_present:
            return False
        return True
    # Explicit future date token like 18.03.26 close to teams is usually prematch.
    if re.search(r"\b\d{1,2}\.\d{2}\.\d{2}\b", low):
        if live_markers_present:
            return False
        return True
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
    t1 = team1.lower()
    t2 = team2.lower()
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
    has_t1 = (t1 in low_sn) or any(token in low_sn for token in _fallback_search_tokens(t1))
    has_t2 = (t2 in low_sn) or any(token in low_sn for token in _fallback_search_tokens(t2))
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
        # Prefer explicit map winner market.
        pat = re.compile(
            rf"Победитель\s*{map_num}\s*карт[аы]\s*([0-9]+[.,][0-9]+)\s+([0-9]+[.,][0-9]+)",
            re.I,
        )
        m = pat.search(flat)
        if m:
            return [float(m.group(1).replace(",", ".")), float(m.group(2).replace(",", "."))]
        m_short = re.search(
            rf"\b{map_num}\s*к\b\s*([0-9]+[.,][0-9]+)\s+([0-9]+[.,][0-9]+)",
            flat.lower(),
            re.I,
        )
        if m_short:
            return [float(m_short.group(1).replace(",", ".")), float(m_short.group(2).replace(",", "."))]
        m_row = re.search(
            rf"\b{map_num}\s*карта\b\s*([0-9]+[.,][0-9]+)\s+([0-9]+[.,][0-9]+)",
            flat.lower(),
            re.I,
        )
        if m_row:
            return [float(m_row.group(1).replace(",", ".")), float(m_row.group(2).replace(",", "."))]
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
        # Feed row often has "<N>К 1.52 2.55" for current map winner.
        m = re.search(
            rf"\b{map_num}\s*к\b\s*([0-9]+[.,][0-9]+)\s+([0-9]+[.,][0-9]+)",
            low,
            re.I,
        )
        if m:
            return [float(m.group(1).replace(",", ".")), float(m.group(2).replace(",", "."))]
        # Alternative format: "<N>К <N> карта 2.45 1.47"
        m_mid = re.search(
            rf"\b{map_num}\s*к\b\s*{map_num}\s*карта\s*([0-9]+[.,][0-9]+)\s+([0-9]+[.,][0-9]+)",
            low,
            re.I,
        )
        if m_mid:
            return [float(m_mid.group(1).replace(",", ".")), float(m_mid.group(2).replace(",", "."))]
        m_row = re.search(
            rf"\b{map_num}\s*карта\b\s*([0-9]+[.,][0-9]+)\s+([0-9]+[.,][0-9]+)",
            low,
            re.I,
        )
        if m_row:
            return [float(m_row.group(1).replace(",", ".")), float(m_row.group(2).replace(",", "."))]

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
        # Conservative detection: if map marker exists near teams, but no odds and lock-like markers exist.
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


def parse_site_in_camoufox_page(
    page,
    site: str,
    url: str,
    team1: str,
    team2: str,
    mode: str,
    forced_map_num: Optional[int] = None,
) -> SiteResult:
    load_status, load_error, html, visible, body_text = _load_site_render_payload_camoufox(
        page,
        url,
        initial_wait_seconds=7.0,
        scroll_wait_seconds=2.0,
    )
    initial_body_text = body_text
    match_fallback_odds: List[float] = []

    if _is_deeplink(site, url):
        if not body_text:
            for _ in range(8):
                time.sleep(1.0)
                body_text = " ".join(_camoufox_body_text(page).split())
                if body_text:
                    break
        if site == "pari":
            for i in range(8):
                if ("КИБЕРСПОРТ / DOTA 2" in body_text or "КИБЕРСПОРТ / DOTA2" in body_text) and "Исход" in body_text:
                    break
                if i == 3:
                    with contextlib.suppress(Exception):
                        page.reload(wait_until="domcontentloaded", timeout=30000)
                time.sleep(1.0)
                body_text = " ".join(_camoufox_body_text(page).split())
        map_odds, body_text = _parse_map_market_on_current_camoufox_page(
            page,
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
                source="deeplink_map_market",
                details=str(body_text or "")[:700],
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
                details=str(body_text or "")[:700],
                market_closed=True,
            )

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
            if _is_map_market_closed(site, deep_context_text, forced_map_num=forced_map_num):
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
        if found_deep and _is_map_market_closed(site, details_deep or body_text, forced_map_num=forced_map_num):
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
        # If map odds not found but match-level odds exist, use them as fallback
        if match_fallback_odds and not map_odds:
            map_odds = match_fallback_odds[:2]
            source_deep = "match_fallback"
            details_deep = body_text[:700]
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

    candidate_urls = _candidate_match_urls_from_html(site, str(getattr(page, "url", "") or url), html)
    href_opened = _camoufox_find_match_by_urls(page, site, candidate_urls, team1, team2)
    if href_opened:
        map_odds, body_text = _parse_map_market_on_current_camoufox_page(
            page,
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
                source="feed_href_map_market",
                details=str(body_text or "")[:700],
            )
        if _is_map_market_closed(site, body_text, forced_map_num=forced_map_num):
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=[],
                source="feed_href_map_market_closed",
                details=str(body_text or "")[:700],
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
                details=str(body_text or "")[:700],
            )

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

    found, _odds_ignored, source_name, details = _find_from_sources(team1, team2, sources)
    strict_map_odds: List[float] = []
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
    # If map_odds not found but match-level odds exist, use them as fallback
    if not odds and match_fallback_odds:
        odds = match_fallback_odds[:2]
        source_name = f"{source_name}_match_fallback" if source_name else "match_fallback"
        details = (details or body_text)[:700]
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
        # If map odds not found but match-level odds exist, use them as fallback
        if match_fallback_odds and not map_odds:
            map_odds = match_fallback_odds[:2]
            source_deep = "match_fallback"
            details_deep = body_text[:700]
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
        if map_odds:
            return SiteResult(
                site=site,
                url=url,
                status=load_status,
                match_found=True,
                odds=map_odds[:2],
                source="feed_href_map_market",
                details=body_text[:700],
            )
        if _is_map_market_closed(site, body_text, forced_map_num=forced_map_num):
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


def run_presence_sites_parallel(
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
        return _run_presence_sites_in_camoufox(
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


def run_sites_in_camoufox(
    *,
    selected_sites: List[str],
    urls: Dict[str, str],
    team1: str,
    team2: str,
    mode: str,
    forced_map_num: Optional[int] = None,
) -> List[SiteResult]:
    """Parse all selected bookmaker sites. Creates one browser instance per call."""
    if not CAMOUFOX_AVAILABLE:
        raise RuntimeError("Camoufox is unavailable")
    proxy_kwargs = _camoufox_proxy_kwargs(BOOKMAKER_PROXY_URL)
    results: List[SiteResult] = []
    with camoufox.Camoufox(headless=True, **proxy_kwargs) as browser:
        for site in selected_sites:
            page = browser.new_page()
            try:
                results.append(
                    parse_site_in_camoufox_page(
                        page,
                        site=site,
                        url=urls[site],
                        team1=team1,
                        team2=team2,
                        mode=mode,
                        forced_map_num=forced_map_num,
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


def main() -> None:
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
            results = run_presence_sites_parallel(
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
                results = run_sites_in_camoufox(
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
    main()
