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


# The event page first renders only the "Популярные на матч/карту" block; the full list (a line
# "Все" followed by every market, incl. "N карта тотал убийств <TEAM>") arrives seconds later.
# Measured on serv1 05.10: a ~5.5 s fixed wait read 105-line bodies (0 team headers) on BLAST
# pages that render 485-487 lines with 9 team headers after ~16 s.
FULL_MARKETS_JS = """() => /(^|\\n)Все(\\n|$)/.test(document.body.innerText || '')"""
BODY_LENGTH_JS = """() => (document.body.innerText || '').length"""
FULL_MARKETS_TIMEOUT_MS = 20000
SETTLE_POLLS = 10


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
            with HistoryWriter(args.history) as writer:
                for index, card in enumerate(selected):
                    # Initial listing is reusable; subsequent events need a fresh listing.
                    needed = 1 if index == 0 else 2
                    if loads.n + needed > loads.maximum:
                        break
                    if index:
                        listing()
                    loads.gate()
                    if not page.evaluate(OPEN_EVENT_JS, card["event_id"]):
                        # The card left the listing between reloads (e.g. went live): skip it,
                        # it is not a proxy/route failure.
                        stats["events_missing"] = stats.get("events_missing", 0) + 1
                        continue
                    stats["events_opened"] += 1
                    page.wait_for_function(EVENT_READY_JS,
                                           arg=[LIST_URL, card["team1"], card["team2"]], timeout=30000)
                    time.sleep(4.0)
                    if not wait_full_markets(page):
                        # Only the "Популярные" block rendered: nulls would be indistinguishable
                        # from "market absent", so this event writes nothing this cycle.
                        check_connection()
                        stats["events_unrendered"] = stats.get("events_unrendered", 0) + 1
                        continue
                    if page.evaluate(EXPAND_JS):
                        time.sleep(1.5)
                    body_text = page.locator("body").inner_text(timeout=20000)
                    check_connection()
                    for row in build_rows(card, body_text):
                        if writer.write(row):
                            stats["rows_written"] += 1
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
    args = parser.parse_args(argv)
    if args.max_events < 0 or args.max_loads < 1:
        parser.error("--max-events must be >=0 and --max-loads must be >=1")
    stats = dict(cards=0, events_opened=0, rows_written=0, loads=0, country="unknown")
    # Third-party imports/browser code may print credential-bearing diagnostics.
    # Never expose their output or exception messages; report only bounded counters.
    with quiet_output():
        code = _cycle(args, stats)
    print("cards={cards} events_opened={events_opened} events_missing={missing} events_unrendered={unrendered} "
          "rows_written={rows_written} loads={loads} country={country} status={status} error={error}".format(
              status=code, missing=stats.get("events_missing", 0), unrendered=stats.get("events_unrendered", 0),
              error=stats.get("error", "-"),
              **{k: v for k, v in stats.items() if k not in ("events_missing", "events_unrendered", "error")}),
          flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
