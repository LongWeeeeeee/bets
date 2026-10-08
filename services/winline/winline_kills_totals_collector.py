#!/usr/bin/env python3
"""One-shot event-page team kills totals collector; never uses the prod poller.

Run from the repository root with python -m services.winline.winline_kills_totals_collector.
Only neutral IP echo uses requests. All Winline acquisition uses a verified,
authenticated project proxy in Camoufox; browser/IP failures never fall back.
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import importlib.util
import io
import ipaddress
import json
import math
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

REPO = Path(__file__).resolve().parents[2]
LIST_URL = "https://winline.ru/stavki/sport/kibersport/dota_2"
IP_ECHO = "https://api.ipify.org"
ALLOWED_COUNTRIES = {"DE", "US", "CA"}
VALUE_FIELDS = tuple("kills_%s_%s" % (side, value)
                     for side in ("t1", "t2") for value in ("line", "over", "under"))
LADDER_FIELDS = ("kills_t1_ladder", "kills_t2_ladder")
# Rows written before ladders were recorded lack LADDER_FIELDS; they replay as None.
DEDUPE_FIELDS = VALUE_FIELDS + LADDER_FIELDS

OPEN_EVENT_JS = """(id) => {
  const el = document.getElementById('eventId-' + id);
  if (!el) return false;
  const target = el.querySelector('.body-left__names') || el;
  target.scrollIntoView({block: 'center'});
  target.click();
  return true;
}"""
OPEN_HERO_EVENT_JS = """() => {
  const target = document.querySelector('ww-feature-event-live-center-dsk .fast-bets__all-markets');
  if (!target) return false;
  target.scrollIntoView({block: 'center'});
  target.click();
  return true;
}"""
EXPAND_JS = """() => {
  const labels = ['ещё', 'еще', 'показать все', 'все рынки', 'развернуть'];
  let n = 0;
  for (const el of document.querySelectorAll('button,[role=button],div,span')) {
    const t = (el.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase();
    if (t && t.length < 20 && labels.includes(t)) {
      el.click(); n++; if (n >= 10) break;
    }
  }
  return n;
}"""
EVENT_READY_JS = """([listing, team1, team2]) => {
  const normalize = s => s.replace(/\\s+/g, ' ').trim().toUpperCase();
  const names = Array.from(document.querySelectorAll(
    '.event-scoreboard-widget .match-card__team-name')).map(el => normalize(el.innerText));
  const text = (document.body.innerText || '').toUpperCase();
  return location.href !== listing && names.includes(normalize(team1))
    && names.includes(normalize(team2)) && text.includes('КАРТА');
}"""
QUICK_TAB_JS = """() => {
  const text = el => (el.innerText || '').trim();
  const visible = el => el.getClientRects().length > 0
    && getComputedStyle(el).visibility !== 'hidden';
  const elements = Array.from(document.querySelectorAll(
    'button,[role=button],[role=tab],a,div,span')).filter(visible);
  const children = el => el && el.parentElement
    ? Array.from(el.parentElement.children).filter(visible) : [];
  const isQuick = el => text(el).toLowerCase() === 'быстрые';
  const allTabs = elements.filter(el => text(el) === 'Все');
  const all = allTabs.find(el => children(el).some(isQuick)) || allTabs[0];
  let strip = children(all);
  let quick = strip.find(isQuick);
  if (!quick) {
    quick = elements.find(isQuick);
    if (quick) strip = children(quick);
  }
  const tabs = strip.map(text).filter(t => t && t.length <= 20);
  if (quick) quick.click();
  return {tabs: tabs, clicked: Boolean(quick)};
}"""


# The event page first renders only the "Популярные на матч/карту" block; the full list (a line
# "Все" followed by every market, incl. "N карта тотал убийств <TEAM>") arrives seconds later.
# Measured on serv1 05.10: a ~5.5 s fixed wait read 105-line bodies (0 team headers) on BLAST
# pages that render 485-487 lines with 9 team headers after ~16 s.
FULL_MARKETS_JS = """() => /(^|\\n)Все(\\n|$)/.test(document.body.innerText || '')"""
BODY_LENGTH_JS = """() => (document.body.innerText || '').length"""
FULL_MARKETS_TIMEOUT_MS = 20000
SETTLE_POLLS = 10


def hero_event_id(html):
    """First logo event ID inside the featured live block, if present."""
    start = html.find("<ww-feature-event-live-center")
    if start < 0:
        return None
    end = html.find("</ww-feature-event-live-center", start)
    if end < 0:
        return None
    match = re.search(r"/api/cls/event/\d+/(\d+)", html[start:end])
    return match.group(1) if match else None


def wait_full_markets(page):
    """True once the full market list is on the page and its text stopped growing."""
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    try:
        page.wait_for_function(FULL_MARKETS_JS, timeout=FULL_MARKETS_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        return False
    last, stable = None, 0
    for _ in range(SETTLE_POLLS):
        length = page.evaluate(BODY_LENGTH_JS)
        stable = stable + 1 if length == last else 0
        if stable >= 2:
            break
        last = length
        time.sleep(1.0)
    return True


_NUMBER = r"[0-9]+(?:[.,][0-9]+)?"
# One block of a team kills total: "Больше" + rungs "б <line> <price>", then "Меньше" + rungs
# "м <line> <price>". BLAST pages show one block with one rung per side; tier-2/3 pages (e.g. PR
# Universe, captured 05.10) show a ladder: a block with four rungs and a second block with one.
_LADDER_BLOCK_RE = re.compile(
    rf"\s+Больше((?:\s+б\s+{_NUMBER}\s+{_NUMBER})+)\s+Меньше((?:\s+м\s+{_NUMBER}\s+{_NUMBER})+)"
    r"(?=\s|$)", re.I)
_RUNG_RE = re.compile(rf"[бм]\s+({_NUMBER})\s+({_NUMBER})", re.I)


def _number(text):
    return float(text.replace(",", "."))


def parse_kills_ladder(text, start):
    """[[line, over, under], ...] sorted by line from consecutive blocks at start, else None.

    Fail closed: a duplicate rung, a line priced on one side only, a price <= 1.0 or a
    non-finite number makes the whole side None rather than a partial ladder.
    """
    over, under = {}, {}
    pos = start
    while True:
        block = _LADDER_BLOCK_RE.match(text, pos)
        if block is None:
            break
        for rungs, side in ((block.group(1), over), (block.group(2), under)):
            for line_text, price_text in _RUNG_RE.findall(rungs):
                line, price = _number(line_text), _number(price_text)
                if line in side:
                    return None
                side[line] = price
        pos = block.end()
    if not over or set(over) != set(under):
        return None
    ladder = [[line, over[line], under[line]] for line in sorted(over)]
    if not all(math.isfinite(x) for rung in ladder for x in rung):
        return None
    if any(price <= 1.0 for rung in ladder for price in rung[1:]):
        return None
    return ladder


def main_rung(ladder):
    """The most balanced rung (|over - under| smallest); ties go to the lower line."""
    return min(ladder, key=lambda rung: (round(abs(rung[1] - rung[2]), 6), rung[0]))


def parse_winline_team_kills_totals(body_text, map_num, team1, team2):
    """Exact literal card names, p1/p2 in card order; malformed sides stay null.

    line/over/under are the main (most balanced) rung; ladder holds every rung offered.
    """
    result = {"p1": None, "p2": None}
    if not isinstance(body_text, str) or type(map_num) is not int or not 1 <= map_num <= 3:
        return result
    text = " ".join(body_text.split())
    for side, team in (("p1", team1), ("p2", team2)):
        if not isinstance(team, str) or not team.strip():
            continue
        name = " ".join(team.split())
        header_re = re.compile(
            rf"(?<!\S){map_num} карта тотал убийств ({re.escape(name)})(?=\s+Больше\s)", re.I)
        headers = list(header_re.finditer(text))
        if len(headers) != 1:
            continue
        header = headers[0]
        ladder = parse_kills_ladder(text, header.end())
        if ladder is None:
            continue
        line, over, under = main_rung(ladder)
        result[side] = dict(team=header.group(1), line=line, over=over, under=under, ladder=ladder)
    return result


_QUICK_HEADER_RE = re.compile(
    r"(\d+) карта (исход 1X2|чет/нечет|фора) убийств в интервале (\d+)[-–−](\d+) минут", re.I)
_QUICK_KINDS = {"исход 1x2": "1x2", "чет/нечет": "odd_even", "фора": "handicap"}
_QUICK_FIELDS = {"1x2": ("p_t1", "p_draw", "p_t2"),
                 "odd_even": ("p_even", "p_odd"),
                 "handicap": ("line_t1", "p_t1", "line_t2", "p_t2")}


def parse_winline_quick_windows(body_text, team1, team2):
    """Line-based window markets, exact card orientation; malformed blocks have null fields.

    Only full market titles anchor parsing, never prices in popular blocks or tab strips.
    Options end at the next map-market title or WINLINE footer; unknown layouts fail closed.
    Duplicate (map, window, market) titles also invalidate that market rather than pick a price.
    """
    if not isinstance(body_text, str):
        return []
    lines = [" ".join(line.split()) for line in body_text.splitlines() if line.strip()]
    boundaries = [i for i, line in enumerate(lines)
                  if re.match(r"\d+ карта .+", line, re.I) or line.casefold() == "winline"]
    boundaries.append(len(lines))
    teams = [" ".join(team.split()).casefold() if isinstance(team, str) else ""
             for team in (team1, team2)]
    rows, seen = [], {}
    for start, end in zip(boundaries, boundaries[1:]):
        header = _QUICK_HEADER_RE.fullmatch(lines[start])
        if header is None:
            continue
        map_num, kind, lower, upper = header.groups()
        if int(map_num) < 1 or int(lower) >= int(upper):
            continue
        market = _QUICK_KINDS[kind.casefold()]
        row = dict(map_num=int(map_num), window="%d-%d" % (int(lower), int(upper)), market=market)
        row.update(dict.fromkeys(_QUICK_FIELDS[market]))
        key = (row["map_num"], row["window"], market)
        if key in seen:
            seen[key].update(dict.fromkeys(_QUICK_FIELDS[market]))
            continue
        seen[key] = row
        rows.append(row)
        options = lines[start + 1:end]
        width, count = (3, 2) if market == "handicap" else (2, 3 if market == "1x2" else 2)
        if len(options) != width * count:
            continue
        labels = [options[i].casefold() for i in range(0, len(options), width)]
        expected = ["чет", "нечет"] if market == "odd_even" else teams + (["ничья"] if market == "1x2" else [])
        if not all(expected) or len(set(expected)) != count or sorted(labels) != sorted(expected):
            continue
        values = {}
        for i, label in zip(range(0, len(options), width), labels):
            price_text = options[i + width - 1]
            line_text = options[i + 1].replace("−", "-") if width == 3 else None
            if not re.fullmatch(_NUMBER, price_text):
                break
            price = _number(price_text)
            if not math.isfinite(price) or price <= 1.0:
                break
            if line_text is not None:
                if not re.fullmatch(rf"[+-]?{_NUMBER}", line_text):
                    break
                line = _number(line_text)
                if not math.isfinite(line):
                    break
                values[label] = (line, price)
            else:
                values[label] = price
        else:
            if market == "odd_even":
                row.update(p_even=values["чет"], p_odd=values["нечет"])
            elif market == "1x2":
                row.update(p_t1=values[teams[0]], p_draw=values["ничья"], p_t2=values[teams[1]])
            else:
                row.update(line_t1=values[teams[0]][0], p_t1=values[teams[0]][1],
                           line_t2=values[teams[1]][0], p_t2=values[teams[1]][1])
    return rows


def build_quick_window_rows(entry, body_text, source_dump):
    """Use capture-index metadata for timestamp, identity and card-side orientation."""
    return [dict(row, ts_utc=entry["ts_utc"], event_id=str(entry["event_id"]),
                 team1=entry["team1"], team2=entry["team2"], live=bool(entry["live"]),
                 source_dump=source_dump)
            for row in parse_winline_quick_windows(body_text, entry["team1"], entry["team2"])]


def _append_json_rows(path, rows):
    if not rows:
        return
    with path.open("a", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def parse_quick_dumps(directory):
    """Offline append-only backfill; index.jsonl is required, repeated captures are skipped.

    Never infer teams/live from the body. Only indexed successful clicks with a matching
    *_quick.txt are eligible; existing (source_dump, map, window, market) rows are preserved.
    Run between collector cycles, as with the existing capture index there is no writer lock.
    """
    directory = Path(directory)
    path = directory / "windows.jsonl"

    def key(row):
        return (row["source_dump"], row["map_num"], row["window"], row["market"])

    seen = set()
    if path.exists():
        with path.open(encoding="utf-8") as stream:
            seen = {key(json.loads(line)) for line in stream}
    dumps = {dump.name: dump for dump in directory.glob("*_quick.txt")}
    rows = []
    with (directory / "index.jsonl").open(encoding="utf-8") as stream:
        for line in stream:
            entry = json.loads(line)
            if not entry.get("quick_clicked"):
                continue
            event_id = re.sub(r"[^A-Za-z0-9_-]", "_", str(entry["event_id"]))
            name = "%s_%s_quick.txt" % (event_id, entry["ts_utc"])
            if name not in dumps:
                continue
            for row in build_quick_window_rows(entry, dumps[name].read_text(encoding="utf-8"), name):
                if key(row) not in seen:
                    rows.append(row)
                    seen.add(key(row))
    _append_json_rows(path, rows)
    return len(rows)


def enumerate_cards(html):
    """Reuse only the existing pure listing-card helper, imported lazily."""
    if str(REPO / "base") not in sys.path:
        sys.path.insert(0, str(REPO / "base"))
    from bookmaker_selenium_odds import winline_enumerate_live_cards
    return winline_enumerate_live_cards(html)


def build_rows(card, body_text, wall=None):
    """Observe each map mentioned on the event page, limited to maps 1..3."""
    maps = sorted({int(m) for m in re.findall(r"(?<!\d)([1-3])\s+карта\b", body_text, re.I)})
    if not maps and card.get("header_map") in (1, 2, 3):
        maps = [card["header_map"]]
    rows = []
    wall = time.time() if wall is None else wall
    for map_num in maps:
        totals = parse_winline_team_kills_totals(body_text, map_num, card["team1"], card["team2"])
        row = dict(wall=wall, event_id=str(card["event_id"]),
                   kind="live" if card.get("live") else "prematch", league=card.get("league", ""),
                   team1=card["team1"], team2=card["team2"], map_num=map_num,
                   source="winline_event_page")
        for target, side in (("t1", "p1"), ("t2", "p2")):
            for value in ("line", "over", "under", "ladder"):
                row["kills_%s_%s" % (target, value)] = (totals[side] or {}).get(value)
        if card.get("score_text") is not None:
            row["score_text"] = card["score_text"]
        rows.append(row)
    return rows


class HistoryWriter:
    """Serialize append + atomic dedupe state; recover an interrupted state save.

    A history-size mismatch replays history once, rather than dropping an
    observation or duplicating the last flushed row after a process crash.
    """
    def __init__(self, history):
        self.path = Path(history)
        self.state_path = self.path.with_suffix(".state.json")
        self.lock = None
        self.values = {}
        self.rows_written = 0

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = self.path.with_suffix(".lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            size = self.path.stat().st_size if self.path.exists() else 0
            state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
            if state.get("history_size") == size:
                self.values = state["values"]
            elif size:
                with self.path.open(encoding="utf-8") as stream:
                    for line in stream:
                        row = json.loads(line)
                        self.values[self.key(row)] = [row.get(field) for field in DEDUPE_FIELDS]
            return self
        except BaseException:
            self.lock.close()
            raise

    @staticmethod
    def key(row):
        return "%s:%s" % (row["event_id"], row["map_num"])

    def write(self, row):
        values = [row.get(field) for field in DEDUPE_FIELDS]
        key = self.key(row)
        if key in self.values and self.values[key] == values:
            return False
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        self.rows_written += 1
        self.values[key] = values
        tmp = self.state_path.with_name(self.state_path.name + ".tmp")
        with tmp.open("w", encoding="utf-8") as stream:
            json.dump(dict(history_size=self.path.stat().st_size, values=self.values), stream,
                      ensure_ascii=False, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(str(tmp), str(self.state_path))
        return True

    def __exit__(self, *exc):
        self.lock.close()


def proxy_candidates(pool_index=None):
    """Authenticated HTTP(S) BOOKMAKER proxies cross-checked against inventory."""
    spec = importlib.util.spec_from_file_location("winline_kills_keys", REPO / "base/keys.py")
    keys = importlib.util.module_from_spec(spec)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        spec.loader.exec_module(keys)
    pool = list(getattr(keys, "BOOKMAKER_PROXY_POOL", ()) or ())
    inventory = {str(e.get("ip", "")).lower(): str(e.get("country", "")).upper()
                 for e in getattr(keys, "PROXY_INVENTORY", ()) if isinstance(e, dict)}
    if pool_index is not None:
        if not 0 <= pool_index < len(pool):
            return []
        pool = [pool[pool_index]]
    result = []
    seen = set()
    for url in pool:
        if not isinstance(url, str) or url in seen:
            continue
        seen.add(url)
        try:
            parsed = urlparse(url)
            country = inventory.get((parsed.hostname or "").lower(), "")
            if (parsed.scheme in ("http", "https") and parsed.hostname and parsed.port
                    and parsed.username and parsed.password and country in ALLOWED_COUNTRIES):
                result.append((url, country))
        except ValueError:
            continue
    return result[:2]


def proxy_kwargs(url):
    parsed = urlparse(url)
    return {"proxy": dict(server="%s://%s:%s" % (parsed.scheme, parsed.hostname, parsed.port),
                          username=unquote(parsed.username), password=unquote(parsed.password))}


def requests_ip_echo(url=None):
    """Neutral endpoint only; ignore environment proxies for the direct check."""
    import requests
    with requests.Session() as session:
        session.trust_env = False
        proxies = {"http": url, "https": url} if url else {}
        response = session.get(IP_ECHO, proxies=proxies, timeout=15, allow_redirects=False)
        response.raise_for_status()
        return response.text.strip()


# Owner rule (05.10): Winline never without the proxy. Firefox may fail over to a DIRECT
# connection when a proxy dies (`network.proxy.failover_direct`, default true) - forbid it.
NO_DIRECT_FAILOVER_PREFS = {"network.proxy.failover_direct": False}


def browser_factory(**kwargs):
    from camoufox.sync_api import Camoufox
    return Camoufox(**kwargs)


@contextlib.contextmanager
def quiet_output():
    """Mute Python diagnostics and inherited child-process stdout/stderr alike."""
    saved = [os.dup(1), os.dup(2)]
    try:
        with open(os.devnull, "w") as sink:
            os.dup2(sink.fileno(), 1)
            os.dup2(sink.fileno(), 2)
            with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
                yield
    finally:
        for descriptor, original in zip((1, 2), saved):
            os.dup2(original, descriptor)
            os.close(original)


class Loads:
    """Count Winline listing navigations and event clicks, never IP echo."""
    def __init__(self, maximum):
        self.maximum = maximum
        self.n = 0
        self.last = None

    def gate(self):
        if self.n >= self.maximum:
            raise RuntimeError("load cap reached")
        if self.last is not None:
            wait = 3.0 - (time.monotonic() - self.last)
            if wait > 0:
                time.sleep(wait)
        self.n += 1
        self.last = time.monotonic()


def _write_dump(path, text):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(str(tmp), str(path))


def capture_quick_tab(page, directory, card, body_text, full_markets, private_values,
                      check_connection):
    """Capture default/quick views and append window prices after a verified quick click."""
    secrets = sorted({str(value) for value in private_values if value}, key=len, reverse=True)

    def safe(text):
        for secret in secrets:
            text = text.replace(secret, "[redacted]")
        return text

    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    ts_utc = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    event_id = safe(str(card["event_id"]))
    # Listing IDs are numeric in production; keep any unexpected card text inside DIR.
    stem = "%s_%s" % (re.sub(r"[^A-Za-z0-9_-]", "_", event_id), ts_utc)
    all_text = safe(body_text)
    _write_dump(directory / (stem + "_all.txt"), all_text)
    result = page.evaluate(QUICK_TAB_JS)
    quick_clicked = bool(result.get("clicked"))
    quick_text = ""
    if quick_clicked:
        time.sleep(3.0)
        quick_text = safe(page.locator("body").inner_text(timeout=20000))
        check_connection()
        _write_dump(directory / (stem + "_quick.txt"), quick_text)
    else:
        check_connection()
    entry = dict(ts_utc=ts_utc, event_id=event_id,
                 team1=safe(card["team1"]), team2=safe(card["team2"]),
                 live=bool(card.get("live")), full_markets=full_markets,
                 tabs=[safe(tab) for tab in result.get("tabs", [])],
                 quick_clicked=quick_clicked, chars_all=len(all_text), chars_quick=len(quick_text))
    _append_json_rows(directory / "index.jsonl", [entry])
    if quick_clicked:
        _append_json_rows(directory / "windows.jsonl",
                          build_quick_window_rows(entry, quick_text, stem + "_quick.txt"))


def _cycle(args, stats):
    # No browser factory call, let alone Winline, before the requests checks pass.
    try:
        direct_ip = ipaddress.ip_address(requests_ip_echo())
        chosen = None
        for url, country in proxy_candidates(args.pool_index):
            try:
                exit_ip = ipaddress.ip_address(requests_ip_echo(url))
                if exit_ip != direct_ip:
                    chosen = (url, country, exit_ip)
                    break
            except Exception:
                continue
        if chosen is None:
            return 2
    except Exception:
        return 2
    url, country, exit_ip = chosen
    stats["country"] = country
    if args.quick_dump_dir:
        parsed_proxy = urlparse(url)
        private_values = (url, parsed_proxy.hostname, parsed_proxy.username, parsed_proxy.password,
                          unquote(parsed_proxy.username), unquote(parsed_proxy.password), direct_ip, exit_ip)
    loads = Loads(args.max_loads)
    winline_started = False
    writer = None
    try:
        with browser_factory(headless=True, block_webrtc=True, enable_cache=False,
                             firefox_user_prefs=dict(NO_DIRECT_FAILOVER_PREFS),
                             **proxy_kwargs(url)) as browser:
            page = browser.new_page()
            failed = [False]

            def request_failed(request):
                failure = str(request.failure or "").lower()
                if any(word in failure for word in (
                        "proxy", "tunnel", "connection", "net::err_", "ns_error_net")):
                    failed[0] = True

            page.on("requestfailed", request_failed)

            def check_connection():
                if failed[0]:
                    raise RuntimeError("browser connection failed")

            page.goto(IP_ECHO, timeout=30000, wait_until="domcontentloaded")
            browser_ip = ipaddress.ip_address(page.locator("body").inner_text(timeout=10000).strip())
            check_connection()
            if browser_ip == direct_ip or browser_ip != exit_ip:
                return 2

            def listing():
                loads.gate()
                page.goto(LIST_URL, wait_until="domcontentloaded", timeout=45000)
                time.sleep(4.0)
                try:
                    page.wait_for_selector("[id^=eventId-]", timeout=30000)
                except Exception as exc:
                    # Only a selector timeout may represent an empty feed.
                    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
                    if not isinstance(exc, PlaywrightTimeoutError):
                        raise
                time.sleep(3.0)
                check_connection()

            winline_started = True
            listing()
            cards = enumerate_cards(page.content())
            stats["cards"] = len(cards)
            selected = select_cards(cards, args.kinds, args.max_events)
            if not selected:
                return 0
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            with HistoryWriter(args.history) as writer:
                for index, card in enumerate(selected):
                    if not card["event_id"].isdigit():
                        stats["events_missing"] = stats.get("events_missing", 0) + 1
                        continue
                    # Initial listing is reusable; subsequent events need a fresh listing.
                    needed = 1 if index == 0 else 2
                    if loads.n + needed > loads.maximum:
                        break
                    if index:
                        listing()
                        wait_started = time.monotonic()
                        try:
                            page.wait_for_selector("#eventId-%s" % card["event_id"], timeout=15000)
                        except PlaywrightTimeoutError:
                            pass
                        finally:
                            stats["card_wait_seconds_max"] = max(
                                stats.get("card_wait_seconds_max", 0),
                                int(round(time.monotonic() - wait_started)))
                    loads.gate()
                    opened_hero = False
                    opened = page.evaluate(OPEN_EVENT_JS, card["event_id"])
                    if not opened:
                        listing_html = page.content()
                        fresh_cards = enumerate_cards(listing_html)
                        present = any(c["event_id"] == card["event_id"] for c in fresh_cards)
                        # One retry in the current DOM; no additional listing loads.
                        if present:
                            opened = page.evaluate(OPEN_EVENT_JS, card["event_id"])
                        if not opened and hero_event_id(listing_html) == card["event_id"]:
                            opened_hero = bool(page.evaluate(OPEN_HERO_EVENT_JS))
                            opened = opened_hero
                        if not opened:
                            stats["events_missing"] = stats.get("events_missing", 0) + 1
                            reason = "events_not_clickable" if present else "events_left_listing"
                            stats[reason] = stats.get(reason, 0) + 1
                            continue
                    stats["events_opened"] += 1
                    try:
                        page.wait_for_function(EVENT_READY_JS,
                                               arg=[LIST_URL, card["team1"], card["team2"]], timeout=30000)
                    except PlaywrightTimeoutError:
                        if not opened_hero:
                            raise
                        # A proxy failure during the hero click must not end the run as status=0.
                        check_connection()
                        stats["events_hero_open_failed"] = stats.get("events_hero_open_failed", 0) + 1
                        continue
                    if opened_hero:
                        stats["events_opened_hero"] = stats.get("events_opened_hero", 0) + 1
                    time.sleep(4.0)
                    full_markets = wait_full_markets(page)
                    if not full_markets:
                        # Only the "Популярные" block rendered: nulls would be indistinguishable
                        # from "market absent", so this event writes no history rows this cycle.
                        check_connection()
                        stats["events_unrendered"] = stats.get("events_unrendered", 0) + 1
                        if not args.quick_dump_dir:
                            continue
                    elif page.evaluate(EXPAND_JS):
                        time.sleep(1.5)
                    body_text = page.locator("body").inner_text(timeout=20000)
                    check_connection()
                    if full_markets:
                        for row in build_rows(card, body_text):
                            if writer.write(row):
                                stats["rows_written"] += 1
                    if args.quick_dump_dir:
                        capture_quick_tab(page, args.quick_dump_dir, card, body_text, full_markets,
                                          private_values, check_connection)
        return 0
    except Exception as exc:
        # Class name only: messages of third-party errors may carry proxy credentials.
        stats["error"] = type(exc).__name__
        return 5 if winline_started else 2
    finally:
        stats["loads"] = loads.n
        if writer is not None:
            stats["rows_written"] = writer.rows_written


CARD_KINDS = ("prematch", "live", "all")


def select_cards(cards, kinds="prematch", max_events=12):
    """Cards to open this cycle: owner decision 05.10 = prematch only (live first when 'all').

    Player-duel prop cards ("BLAST Slam. Дуэль игроков. Убийства", card['prop_duel'] from
    the listing parser) hold player kill props, not team totals: never opened and never
    counted against max_events (card ingame-vl91: 18/116 history rows were duels)."""
    cards = [c for c in cards if not c.get("prop_duel")]
    if kinds == "prematch":
        pool = [c for c in cards if not c.get("live")]
    elif kinds == "live":
        pool = [c for c in cards if c.get("live")]
    else:
        pool = sorted(cards, key=lambda c: not c.get("live"))
    return pool[:max(0, int(max_events))]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", default=os.getenv("WINLINE_KILLS_TOTALS_HISTORY_PATH")
                        or str(REPO / "runtime/winline_kills_totals_history.jsonl"))
    parser.add_argument("--max-events", type=int, default=12)
    parser.add_argument("--kinds", choices=CARD_KINDS, default="prematch",
                        help="which listing cards to open (owner 05.10: prematch only, every 3 h)")
    parser.add_argument("--max-loads", type=int, default=20)
    parser.add_argument("--pool-index", type=int, default=None,
                        help="BOOKMAKER_PROXY_POOL index, inventory-verified; default: at most two candidates")
    parser.add_argument("--quick-dump-dir", default=os.getenv("WINLINE_QUICK_DUMP_DIR"),
                        help="opt-in default/Быстрые event body capture directory (WINLINE_QUICK_DUMP_DIR)")
    parser.add_argument("--parse-quick-dumps", metavar="DIR",
                        help="offline backfill DIR/windows.jsonl from indexed quick dumps; no browser/network")
    args = parser.parse_args(argv)
    if args.parse_quick_dumps:
        print("windows_written=%d" % parse_quick_dumps(args.parse_quick_dumps), flush=True)
        return 0
    if args.max_events < 0 or args.max_loads < 1:
        parser.error("--max-events must be >=0 and --max-loads must be >=1")
    stats = dict(cards=0, events_opened=0, rows_written=0, loads=0, country="unknown")
    # Third-party imports/browser code may print credential-bearing diagnostics.
    # Never expose their output or exception messages; report only bounded counters.
    with quiet_output():
        code = _cycle(args, stats)
    # events_opened counts clicks sent (feed or hero); pages actually loaded =
    # events_opened - events_hero_open_failed (and - 1 when status=5 aborted on a load).
    # events_missing = events_left_listing + events_not_clickable (+ non-digit ids).
    print("cards={cards} events_opened={events_opened} events_missing={missing} events_unrendered={unrendered} "
          "rows_written={rows_written} loads={loads} country={country} status={status} error={error} "
          "events_left_listing={left} events_not_clickable={not_clickable} "
          "card_wait_seconds_max={wait_seconds} "
          "events_opened_hero={opened_hero} events_hero_open_failed={hero_failed}".format(
              status=code, missing=stats.get("events_missing", 0), unrendered=stats.get("events_unrendered", 0),
              error=stats.get("error", "-"), left=stats.get("events_left_listing", 0),
              not_clickable=stats.get("events_not_clickable", 0),
              wait_seconds=stats.get("card_wait_seconds_max", 0),
              opened_hero=stats.get("events_opened_hero", 0),
              hero_failed=stats.get("events_hero_open_failed", 0),
              **{k: v for k, v in stats.items() if k not in ("events_missing", "events_unrendered", "error")}),
          flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
