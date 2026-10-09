"""Passive recorder of Winline overview-listing prices (card ingame-yxst).

Why: the bridge starts polling a map only when it is live in SourceTV, so 0 of the 90
maps the bot followed in 7 days (measured 09.10.2026 on serv1) have a priced Winline
quote in [ts-600, ts].  The overview listing that ``_winline_overview_loop`` already
fetches every ~45 s always shows priced ``Матч`` / ``N карта`` rows, but nothing stored
them.  ``winline_listing_price_rows`` (pure parser) and ``_winline_record_listing_prices``
(JSONL recorder called from ``_winline_sweep_cards_from_snapshot``) fix that.

Inputs are REAL captured overview pages (``{wall, text, html}``, provenance beside each
fixture): 05.10 LEGION-BLASTERBI live map 2 + four pre-match cards, 08.10 hero TEAM AURORA
vs 1W (map 2) + player-duel cards, 10.09 (locked / blank rows), 15.09 (.json.gz).
``winline_enumerate_cards_head_48e923a2.json`` is the output of the UNCHANGED enumerator at
HEAD 48e923a2 on those four pages, produced with
``winline_enumerate_live_cards(json.loads(fixture)["html"])`` on a copy of HEAD's
``bookmaker_selenium_odds.py``; the new parser must not change a byte of it.
"""
from __future__ import annotations

import gzip
import json
import sys
import time
import types
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    import keys  # noqa: F401,E402
except ImportError:
    _test_keys = types.ModuleType("keys")
    _test_keys.api_to_proxy = {}
    _test_keys.BOOKMAKER_PROXY_URL = None
    _test_keys.BOOKMAKER_PROXY_POOL = []
    _test_keys.DLTV_PROXY_POOL = []
    sys.modules["keys"] = _test_keys

import bookmaker_selenium_odds as odds_mod  # noqa: E402
import cyberscore_try as C  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
P0910 = "winline_overview_snapshot_20260910.json"
P0915 = "winline_unpriced_overview_20260915.json.gz"
P1005 = "winline_overview_legion_blasterbi_20261005.json"
P1008 = "winline_overview_snapshot_20261008_blast_duel_cards.json"
ALL_PAGES = (P1005, P0910, P1008, P0915)
GOLDEN = "winline_enumerate_cards_head_48e923a2.json"


def _html(name: str) -> str:
    path = FIXTURES / name
    raw = (gzip.open(path, "rt", encoding="utf-8").read() if name.endswith(".gz")
           else path.read_text(encoding="utf-8"))
    return json.loads(raw)["html"]


def _rows(name: str, stats=None):
    return odds_mod.winline_listing_price_rows(_html(name), stats)


def _pick(rows, team1, kind, map_num=None, source=None):
    hit = [r for r in rows if r["team1"] == team1 and r["kind"] == kind
           and r["map_num"] == map_num and (source is None or r["source"] == source)]
    assert len(hit) == 1, (team1, kind, map_num, hit)
    return hit[0]


# --- parser: exact prices on captured rows ---------------------------------------------

def test_hero_card_0810_exact_match_and_map_prices_and_total_row_is_not_used():
    rows = _rows(P1008)
    match = _pick(rows, "TEAM AURORA", "match", source="hero")
    map2 = _pick(rows, "TEAM AURORA", "map", 2, source="hero")
    assert (match["p1"], match["p2"], match["locked"]) == (1.22, 3.90, False)
    assert (map2["p1"], map2["p2"], map2["locked"]) == (1.61, 2.22, False)
    # The hero panel also holds ``Тотал / период матч`` showing 1.61/2.22 (same numbers as
    # the map winner); taking it as the match winner would give 1.61/2.22 for "match".
    assert (match["p1"], match["p2"]) != (map2["p1"], map2["p2"])
    for row in (match, map2):
        assert (row["team2"], row["live"], row["header_map"], row["prop_duel"]) == (
            "1W", True, 2, False)
        assert row["league"] == "BLAST Slam" and row["event_id"] == "16855095"
    assert len([r for r in rows if r["source"] == "hero"]) == 2


def test_live_card_0510_exact_prices_in_card_order():
    rows = _rows(P1005)
    map2 = _pick(rows, "LEGION", "map", 2)
    match = _pick(rows, "LEGION", "match")
    # Card order (LEGION first): provenance text 'LEGION BLASTERBI 2карта ... 2 карта 2.02 1.70'.
    assert (map2["p1"], map2["p2"], map2["locked"]) == (2.02, 1.70, False)
    assert (match["p1"], match["p2"], match["locked"]) == (3.50, 1.22, False)
    assert map2["live"] is True and map2["header_map"] == 2 and map2["source"] == "feed"
    assert map2["team2"] == "BLASTERBI" and map2["event_id"] == "16858346"


@pytest.mark.parametrize("team1,match_prices,map1_prices", [
    ("MOUZ", (1.15, 4.69), (1.29, 3.28)),
    ("HULIGANI", (1.61, 2.17), (1.68, 2.06)),
    ("TEAM YANDEX", (1.14, 5.77), (1.27, 3.79)),
    ("TEAM AURORA", (1.80, 2.02), (1.84, 1.98)),
])
def test_prematch_cards_0510_carry_match_and_first_map_prices(team1, match_prices, map1_prices):
    rows = _rows(P1005)
    match = _pick(rows, team1, "match")
    map1 = _pick(rows, team1, "map", 1)
    for row, expected in ((match, match_prices), (map1, map1_prices)):
        assert (row["p1"], row["p2"]) == expected and row["locked"] is False
        assert row["live"] is False and row["header_map"] is None


def test_rows_per_fixture():
    counts = {name: len(_rows(name)) for name in ALL_PAGES}
    assert counts == {P1005: 10, P0910: 15, P1008: 12, P0915: 9}, counts


def test_locked_and_blank_rows_have_no_price_but_are_reported():
    rows = _rows(P0910)
    # RECRENT CLUB 'Матч': three '-' placeholders; PEACEKEEPERS 'Матч': generic2 buttons
    # with coefficient-button--is-blank.
    for team in ("RECRENT CLUB", "PEACEKEEPERS"):
        row = _pick(rows, team, "match")
        assert (row["p1"], row["p2"], row["locked"]) == (None, None, True)
    # ...while the live map row of the same RECRENT card is priced.
    live = _pick(rows, "RECRENT CLUB", "map", 3)
    assert (live["p1"], live["p2"], live["locked"]) == (2.25, 1.57, False)


def test_card_wide_has_prices_is_the_trap_row_prices_are_row_scoped():
    """The duel map rows have ``has_prices=True`` in the enumerator (decimals of the kills
    handicap/total in the same card body) but no winner price: the row parser says so."""
    html = _html(P1008)
    cards = {c["event_id"]: c for c in odds_mod.winline_enumerate_live_cards(html)}
    rows = odds_mod.winline_listing_price_rows(html)
    for event_id in ("16882199", "16882198", "16882197", "16889471"):
        map_flags = [r["has_prices"] for r in cards[event_id]["rows"] if r["kind"] == "map"]
        assert map_flags == [True], event_id
        mine = [r for r in rows if r["event_id"] == event_id and r["kind"] == "map"]
        assert len(mine) == 1 and mine[0]["locked"] is True
        assert (mine[0]["p1"], mine[0]["p2"]) == (None, None)
        assert mine[0]["prop_duel"] is True


def test_duel_cards_are_flagged_prop_duel():
    rows = _rows(P1008)
    duels = {r["event_id"] for r in rows if r["prop_duel"]}
    assert duels == {"16882199", "16882198", "16882197", "16889471"}
    assert not [r for r in rows if r["prop_duel"] and not r["locked"]]
    assert not [r for r in rows if r["event_id"] == "16889364" and r["prop_duel"]]


def test_three_way_row_is_skipped_and_counted():
    soup = BeautifulSoup(_html(P1005), "html.parser")
    node = soup.find(id="eventId-16858347")  # MOUZ pre-match
    first_match_row = node.select(".card__coeffs")[0].find("ww-feature-event-market-dsk")
    for btn in first_match_row.find_all(True, recursive=False):
        btn["class"] = [c.replace("generic2", "generic3") for c in btn["class"]]
    stats = {}
    rows = odds_mod.winline_listing_price_rows(str(soup), stats)
    assert [r for r in rows if r["team1"] == "MOUZ" and r["kind"] == "match"] == []
    assert [r for r in rows if r["team1"] == "MOUZ" and r["kind"] == "map"]
    assert stats.get("skipped_three_way") == 1, stats


def test_locked_button_class_hides_the_price():
    soup = BeautifulSoup(_html(P1005), "html.parser")
    node = soup.find(id="eventId-16858347")
    market = node.select(".card__coeffs")[1].find("ww-feature-event-market-dsk")  # 1 карта
    market.find_all(True, recursive=False)[0]["class"].append("coefficient-button_locked")
    row = _pick(odds_mod.winline_listing_price_rows(str(soup)), "MOUZ", "map", 1)
    assert (row["p1"], row["p2"], row["locked"]) == (None, None, True)


# --- parser == bridge's proven row parser on every captured row ------------------------

@pytest.mark.parametrize("name", ALL_PAGES)
def test_map_rows_equal_the_bridge_structured_parser(name):
    html = _html(name)
    soup = BeautifulSoup(html, "html.parser")
    checked = 0
    for row in odds_mod.winline_listing_price_rows(html):
        if row["kind"] != "map":
            continue
        if row["source"] == "feed":
            node = soup.find(id="eventId-" + row["event_id"])
        else:
            node = soup.select_one("ww-feature-event-live-center-dsk")
        fragment = "<html><body>" + str(node) + "</body></html>"
        sub = odds_mod._winline_structured_current_map_winner(
            fragment, row["team1"], row["team2"], row["map_num"], diag=[],
            _force_skip_props=True)
        bridge = (list(sub.odds) if sub is not None and not sub.reason
                  and not sub.market_closed else None)
        mine = None if row["locked"] else [row["p1"], row["p2"]]
        assert mine == bridge, (name, row)
        checked += 1
    assert checked >= 4


@pytest.mark.parametrize("name", ALL_PAGES)
def test_feed_match_rows_equal_the_bridge_match_market_scan(name):
    html = _html(name)
    soup = BeautifulSoup(html, "html.parser")
    checked = 0
    for row in odds_mod.winline_listing_price_rows(html):
        if row["kind"] != "match" or row["source"] != "feed":
            continue
        prices, three_way, _suspended = odds_mod._winline_match_market_scan(
            soup.find(id="eventId-" + row["event_id"]))
        assert not three_way
        assert (None if row["locked"] else [row["p1"], row["p2"]]) == prices, (name, row)
        checked += 1
    assert checked >= 4


# --- the enumerator is untouched --------------------------------------------------------

def test_enumerator_output_is_byte_identical_to_head():
    golden = json.loads((FIXTURES / GOLDEN).read_text(encoding="utf-8"))
    current = {name: odds_mod.winline_enumerate_live_cards(_html(name)) for name in ALL_PAGES}
    assert json.dumps(current, sort_keys=True) == json.dumps(golden, sort_keys=True)


def test_enumerator_output_is_unchanged_by_the_price_parser_sinks():
    for name in ALL_PAGES:
        html = _html(name)
        plain = odds_mod.winline_enumerate_live_cards(html)
        soup = BeautifulSoup(html, "html.parser")
        sinked = odds_mod.winline_enumerate_live_cards(
            html, _soup=soup, _feed_nodes={}, _hero_nodes={})
        assert sinked == plain


# --- recorder ---------------------------------------------------------------------------

@pytest.fixture
def rec(monkeypatch, tmp_path):
    path = tmp_path / "listing_prices.jsonl"
    monkeypatch.setenv("WINLINE_LISTING_PRICES_PATH", str(path))
    monkeypatch.delenv("WINLINE_LISTING_PRICES", raising=False)
    monkeypatch.delenv("WINLINE_LISTING_PRICES_MAX_MB", raising=False)
    monkeypatch.setattr(C, "_winline_listing_state", {})
    monkeypatch.setattr(C, "_winline_listing_last_wall", 0.0)
    monkeypatch.setattr(C, "_winline_listing_errors", {})
    monkeypatch.setattr(C, "_winline_listing_stats", {})
    monkeypatch.setattr(C, "_winline_listing_cap_logged", False)

    def read():
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

    return path, read


WALL0 = 1_790_000_000.0


def test_recorder_writes_non_duel_rows_with_snapshot_wall(rec):
    path, read = rec
    assert C._winline_record_listing_prices(_html(P1008), WALL0) == 4
    rows = read()
    assert len(rows) == 4
    assert {r["wall"] for r in rows} == {WALL0}
    assert {(r["team1"], r["kind"], r["map_num"]) for r in rows} == {
        ("TEAM AURORA", "match", None), ("TEAM AURORA", "map", 2),
        ("PARIVISION", "match", None), ("PARIVISION", "map", 1)}
    assert not [r for r in rows if r["prop_duel"] or "(" in r["team1"]]
    assert not [r for r in rows if r.get("keepalive")]
    expected = {"wall", "event_id", "league", "team1", "team2", "live", "header_map",
                "prop_duel", "source", "kind", "map_num", "p1", "p2", "locked"}
    assert all(set(r) == expected for r in rows)
    assert C._winline_listing_stats["skipped_duel"] == 8


def test_recorder_row_counts_on_the_other_pages(rec):
    path, read = rec
    assert C._winline_record_listing_prices(_html(P1005), WALL0) == 10
    assert C._winline_record_listing_prices(_html(P0910), WALL0 + 1) == 15
    assert C._winline_record_listing_prices(_html(P0915), WALL0 + 2) == 9


def test_same_snapshot_twice_and_unchanged_page_write_once(rec):
    path, read = rec
    html = _html(P1005)
    assert C._winline_record_listing_prices(html, WALL0) == 10
    size = path.stat().st_size
    assert C._winline_record_listing_prices(html, WALL0) == 0  # same snapshot
    assert C._winline_record_listing_prices(html, WALL0 + 45) == 0  # new snapshot, same state
    assert C._winline_record_listing_prices(html, WALL0 + 90) == 0
    assert path.stat().st_size == size and len(read()) == 10


def _with_price(html, event_id, row_index, new_text):
    soup = BeautifulSoup(html, "html.parser")
    node = soup.find(id="eventId-" + event_id)
    market = node.select(".card__coeffs")[row_index].find("ww-feature-event-market-dsk")
    market.find_all(True, recursive=False)[0].find("span").string = new_text
    return str(soup)


def test_price_change_writes_only_the_changed_row(rec):
    path, read = rec
    html = _html(P1005)
    C._winline_record_listing_prices(html, WALL0)
    changed = _with_price(html, "16858347", 1, "1.31")  # MOUZ '1 карта' 1.29 -> 1.31
    assert C._winline_record_listing_prices(changed, WALL0 + 45) == 1
    last = read()[-1]
    assert (last["team1"], last["kind"], last["map_num"]) == ("MOUZ", "map", 1)
    assert (last["p1"], last["p2"], last["wall"]) == (1.31, 3.28, WALL0 + 45)
    assert "keepalive" not in last
    # price back -> another row (state is compared with the last WRITTEN state)
    assert C._winline_record_listing_prices(html, WALL0 + 90) == 1
    assert read()[-1]["p1"] == 1.29


def test_going_live_or_header_map_change_is_a_state_change(rec):
    path, read = rec
    html = _html(P1005)
    C._winline_record_listing_prices(html, WALL0)
    soup = BeautifulSoup(html, "html.parser")
    node = soup.find(id="eventId-16858346")  # LEGION live, header '2карта'
    node.select_one(".header-left__time").string = "3карта 05'"
    wrote = C._winline_record_listing_prices(str(soup), WALL0 + 45)
    assert wrote == 2  # both rows of the card carry header_map
    assert {r["header_map"] for r in read()[-2:]} == {3}


def test_locked_row_is_recorded_as_locked_without_prices(rec):
    path, read = rec
    html = _html(P1005)
    C._winline_record_listing_prices(html, WALL0)
    soup = BeautifulSoup(html, "html.parser")
    node = soup.find(id="eventId-16858347")
    market = node.select(".card__coeffs")[1].find("ww-feature-event-market-dsk")
    for btn in market.find_all(True, recursive=False)[:2]:
        btn["class"].append("coefficient-button_locked")
    assert C._winline_record_listing_prices(str(soup), WALL0 + 45) == 1
    last = read()[-1]
    assert (last["p1"], last["p2"], last["locked"]) == (None, None, True)


def test_keepalive_every_600s_per_key_and_only_then(rec):
    path, read = rec
    html = _html(P1005)
    C._winline_record_listing_prices(html, WALL0)
    assert C._winline_record_listing_prices(html, WALL0 + 599) == 0
    assert C._winline_record_listing_prices(html, WALL0 + 600) == 10
    keep = read()[10:]
    assert len(keep) == 10 and all(r["keepalive"] is True for r in keep)
    assert {r["wall"] for r in keep} == {WALL0 + 600}
    # the clock for the next keepalive restarts at the keepalive row
    assert C._winline_record_listing_prices(html, WALL0 + 1199) == 0
    assert C._winline_record_listing_prices(html, WALL0 + 1200) == 10


def test_state_forgets_keys_unseen_for_six_hours(rec):
    path, read = rec
    C._winline_record_listing_prices(_html(P1005), WALL0)
    assert len(C._winline_listing_state) == 10
    C._winline_record_listing_prices(_html(P0910), WALL0 + 6 * 3600 + 60)
    keys_now = {k[1] for k in C._winline_listing_state}
    assert len(C._winline_listing_state) == 15 and "LEGION" not in keys_now


@pytest.mark.parametrize("value", ["0", "false", "off", "FALSE"])
def test_flag_off_writes_nothing(rec, monkeypatch, value):
    path, read = rec
    monkeypatch.setenv("WINLINE_LISTING_PRICES", value)
    assert C._winline_record_listing_prices(_html(P1005), WALL0) == 0
    assert not path.exists() and C._winline_listing_state == {}


def test_size_cap_stops_writing_with_one_log_line_and_keeps_the_file(rec, monkeypatch, capsys):
    path, read = rec
    html = _html(P1005)
    C._winline_record_listing_prices(html, WALL0)
    before = path.read_bytes()
    monkeypatch.setenv("WINLINE_LISTING_PRICES_MAX_MB", str(len(before) / (1024 * 1024) / 2))
    capsys.readouterr()
    changed = _with_price(html, "16858347", 1, "1.31")
    for i in range(1, 4):
        assert C._winline_record_listing_prices(changed, WALL0 + 45 * i) == 0
    out = capsys.readouterr().out
    assert len([l for l in out.splitlines() if "WINLINE_LISTING_PRICES_CAP" in l]) == 1, out
    assert path.read_bytes() == before  # not truncated, nothing appended
    # raising the cap resumes recording
    monkeypatch.setenv("WINLINE_LISTING_PRICES_MAX_MB", "512")
    assert C._winline_record_listing_prices(changed, WALL0 + 300) == 1


def test_recorder_error_is_counted_logged_once_and_never_raises(rec, monkeypatch, capsys):
    path, read = rec

    def boom(html, stats=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(odds_mod, "winline_listing_price_rows", boom)
    for i in range(3):
        assert C._winline_record_listing_prices(_html(P1005), WALL0 + i) == 0
    out = capsys.readouterr().out
    assert len([l for l in out.splitlines() if "WINLINE_LISTING_PRICES_ERROR" in l]) == 1, out
    assert C._winline_listing_errors == {"parse:RuntimeError": 3}
    assert not path.exists()


def test_unwritable_path_is_fail_open_and_state_is_not_advanced(rec, monkeypatch, tmp_path):
    path, read = rec
    blocker = tmp_path / "a_file"
    blocker.write_text("x")
    monkeypatch.setenv("WINLINE_LISTING_PRICES_PATH", str(blocker / "sub" / "p.jsonl"))
    assert C._winline_record_listing_prices(_html(P1005), WALL0) == 0
    assert C._winline_listing_state == {}
    assert list(C._winline_listing_errors) and list(C._winline_listing_errors)[0].startswith("write:")
    # the same rows are written once the path works
    monkeypatch.setenv("WINLINE_LISTING_PRICES_PATH", str(path))
    assert C._winline_record_listing_prices(_html(P1005), WALL0 + 45) == 10


# --- wiring in the sweep ----------------------------------------------------------------

@pytest.fixture
def sweep(monkeypatch):
    html = _html(P1008)
    monkeypatch.setitem(C._winline_overview_state, "html", html)
    monkeypatch.setitem(C._winline_overview_state, "fetched_at", time.time())
    created = []
    real_enumerate = odds_mod.winline_enumerate_live_cards

    def enumerate_with_real_card_live(html, *args, **kwargs):
        cards = real_enumerate(html, *args, **kwargs)
        for card in cards:
            if (card.get("team1"), card.get("team2")) == ("PARIVISION", "TEAM YANDEX"):
                card["live"] = True
        return cards

    monkeypatch.setattr(odds_mod, "winline_enumerate_live_cards",
                        enumerate_with_real_card_live)
    monkeypatch.setattr(C, "ensure_winline_current_map_polling",
                        lambda **kw: created.append(kw) or True)
    monkeypatch.setattr(C, "_winline_first_active", lambda: True)
    monkeypatch.setattr(C, "_dltv_live_series_snapshot", lambda *a, **k: None)
    monkeypatch.setattr(C, "_winline_card_dltv_draft_notify", lambda **k: None)
    monkeypatch.setattr(C, "_winline_bridge_live_pairs", lambda *a, **k: set())
    monkeypatch.setattr(C, "_winline_bridge_owns_card_pair", lambda *a, **k: False)
    monkeypatch.setattr(C, "_winline_prop_skip_logged_keys", set(), raising=False)
    monkeypatch.setattr(C, "WINLINE_CARD_SWEEP_MAX_CARDS", 6)
    return C._winline_sweep_cards_from_snapshot, created


def test_sweep_records_listing_prices_and_its_summary_does_not_change(rec, sweep, monkeypatch):
    path, read = rec
    run, created = sweep
    monkeypatch.setenv("WINLINE_LISTING_PRICES", "0")
    summary_off = run()
    created_off = [dict(c) for c in created]
    assert not path.exists()
    created.clear()
    monkeypatch.delenv("WINLINE_LISTING_PRICES")
    summary_on = run()
    assert summary_on == summary_off and created == created_off
    assert len(read()) == 4  # the pre-map rows of the pre-match cards included
    run()  # same snapshot again: no new rows
    assert len(read()) == 4


def test_recorder_exception_leaves_the_sweep_summary_unchanged(rec, sweep, monkeypatch):
    path, read = rec
    run, created = sweep
    monkeypatch.setenv("WINLINE_LISTING_PRICES", "0")
    summary_off = run()
    created_off = [dict(c) for c in created]
    created.clear()
    monkeypatch.delenv("WINLINE_LISTING_PRICES")

    def boom(*a, **k):
        raise RuntimeError("recorder bug")

    monkeypatch.setattr(C, "_winline_record_listing_prices", boom)
    assert run() == summary_off and created == created_off


def test_parser_exception_inside_the_recorder_leaves_the_sweep_unchanged(rec, sweep, monkeypatch):
    path, read = rec
    run, created = sweep
    monkeypatch.setenv("WINLINE_LISTING_PRICES", "0")
    summary_off = run()
    created_off = [dict(c) for c in created]
    created.clear()
    monkeypatch.delenv("WINLINE_LISTING_PRICES")

    def boom(html, stats=None):
        raise RuntimeError("parser bug")

    monkeypatch.setattr(odds_mod, "winline_listing_price_rows", boom)
    assert run() == summary_off and created == created_off
    assert not path.exists()


# --- cost -------------------------------------------------------------------------------

def test_parser_time_on_the_largest_capture_is_one_parse_scale():
    html = _html(P1008)  # ~690 KB html, the largest captured page
    odds_mod.winline_listing_price_rows(html)  # warm-up
    t0 = time.perf_counter()
    odds_mod.winline_listing_price_rows(html)
    elapsed = time.perf_counter() - t0
    t0 = time.perf_counter()
    odds_mod.winline_enumerate_live_cards(html)
    enumerate_elapsed = time.perf_counter() - t0
    # price rows = one enumeration (one soup) + O(rows): never a second page walk.
    assert elapsed < enumerate_elapsed * 2.0 + 0.05, (elapsed, enumerate_elapsed)


# --- round 2: row ownership, filters, price floor, cap with the pending batch -----------
# Fixtures below are the REAL captures (08.10 hero page + the 05.10 feed card of the same
# event, see test_winline_enumerate_hero_card.py); each case edits the captured markup in
# exactly one place.

import test_winline_enumerate_hero_card as HT  # noqa: E402

AURORA_ID = HT.HERO_EVENT_ID  # 16855095


def _event_rows(rows, event_id=AURORA_ID):
    return {(r["kind"], r["map_num"]): r for r in rows if r["event_id"] == event_id}


def _lock_feed_row(soup, event_id, row_index):
    node = soup.find(id="eventId-" + event_id)
    market = node.select(".card__coeffs")[row_index].find("ww-feature-event-market-dsk")
    for btn in market.find_all(True, recursive=False)[:2]:
        btn["class"].append("coefficient-button_locked")


def test_p1_live_hero_merged_into_prematch_feed_card_writes_only_hero_rows():
    """Enumerator: the pre-match feed card of the event is replaced by the live hero's rows.
    The recorder must do the same: map 2 1.61/2.22 live, and NO pre-match map 1 1.84/1.98
    (nor the pre-match match row 1.80/2.02) marked live."""
    html = HT._page_with_feed_copy_of_hero_event()
    cards = [c for c in odds_mod.winline_enumerate_live_cards(html)
             if c["event_id"] == AURORA_ID]
    assert len(cards) == 1 and cards[0]["live"] is True and cards[0]["header_map"] == 2
    all_rows = odds_mod.winline_listing_price_rows(html)
    mine = _event_rows(all_rows)
    assert set(mine) == {("match", None), ("map", 2)}, sorted(mine, key=str)
    assert (mine[("map", 2)]["p1"], mine[("map", 2)]["p2"]) == (1.61, 2.22)
    assert (mine[("match", None)]["p1"], mine[("match", None)]["p2"]) == (1.22, 3.90)
    for row in mine.values():
        assert (row["live"], row["header_map"], row["source"], row["locked"]) == (
            True, 2, "hero", False)
    assert not [r for r in all_rows if r["event_id"] == AURORA_ID
                and (r["p1"], r["p2"]) in {(1.84, 1.98), (1.80, 2.02)}]


def test_p1_recorder_never_writes_a_stale_prematch_price_marked_live(rec):
    path, read = rec
    html = HT._page_with_feed_copy_of_hero_event()
    C._winline_record_listing_prices(html, WALL0)
    mine = [r for r in read() if r["event_id"] == AURORA_ID]
    assert {(r["kind"], r["map_num"], r["p1"], r["p2"]) for r in mine} == {
        ("match", None, 1.22, 3.90), ("map", 2, 1.61, 2.22)}
    assert all(r["live"] is True and r["header_map"] == 2 and r["source"] == "hero"
               for r in mine)


def _both_live_page(lock_feed_match=False, lock_hero_match=False):
    html = HT._page_with_feed_copy_of_hero_event(live=True)
    soup = BeautifulSoup(html, "html.parser")
    if lock_feed_match:
        _lock_feed_row(soup, AURORA_ID, 0)
    if lock_hero_match:
        hero = soup.select_one("ww-feature-event-live-center-dsk")
        wrapper = hero.select(".event-live-center__markets .fast-bets__wrapper")[0]
        line = wrapper.select(".bet-line")[0]  # Победитель / матч 1.22 3.90
        for btn in line.select(".bet-line__coefs-wrapper .odd-btn"):
            btn["class"] = list(btn["class"]) + ["coef-btn--locked"]
    return str(soup)


def test_both_live_rows_come_from_the_node_that_owns_them_with_its_own_metadata():
    html = _both_live_page()
    cards = [c for c in odds_mod.winline_enumerate_live_cards(html)
             if c["event_id"] == AURORA_ID]
    assert len(cards) == 1 and cards[0]["live"] is True and cards[0]["header_map"] == 2
    mine = _event_rows(odds_mod.winline_listing_price_rows(html))
    assert set(mine) == {("match", None), ("map", 1), ("map", 2)}, sorted(mine, key=str)
    # present in both nodes and priced in the feed: the feed row (comment in the parser)
    match, map1, map2 = mine[("match", None)], mine[("map", 1)], mine[("map", 2)]
    assert (match["source"], match["p1"], match["p2"]) == ("feed", 1.80, 2.02)
    assert (map1["source"], map1["p1"], map1["p2"]) == ("feed", 1.84, 1.98)
    assert (map2["source"], map2["p1"], map2["p2"]) == ("hero", 1.61, 2.22)
    # metadata of the owning node: the feed card shows no map in its header, the hero shows 2
    assert (match["header_map"], map1["header_map"]) == (None, None)
    assert map2["header_map"] == 2
    assert all(r["live"] is True for r in mine.values())


def test_both_live_locked_feed_row_yields_to_the_priced_hero_row():
    mine = _event_rows(odds_mod.winline_listing_price_rows(
        _both_live_page(lock_feed_match=True)))
    match = mine[("match", None)]
    assert (match["source"], match["p1"], match["p2"], match["locked"], match["header_map"]) == (
        "hero", 1.22, 3.90, False, 2)
    assert mine[("map", 1)]["source"] == "feed"


def test_both_live_locked_hero_row_does_not_displace_the_priced_feed_row():
    mine = _event_rows(odds_mod.winline_listing_price_rows(
        _both_live_page(lock_hero_match=True)))
    match = mine[("match", None)]
    assert (match["source"], match["p1"], match["p2"], match["locked"]) == (
        "feed", 1.80, 2.02, False)


def test_same_pair_under_two_event_ids_are_distinct_recorder_keys(rec):
    path, read = rec
    html = _html(P1005)
    soup = BeautifulSoup(html, "html.parser")
    node = soup.find(id="eventId-16858347")  # MOUZ pre-match
    twin = BeautifulSoup(str(node), "html.parser").find(id=True)
    twin["id"] = "eventId-99999999"
    node.insert_after(twin)
    twin_html = str(soup)
    assert C._winline_record_listing_prices(twin_html, WALL0) == 12  # 10 + 2 rows of the twin
    mouz = [r for r in read() if r["team1"] == "MOUZ"]
    assert sorted((r["event_id"], r["kind"]) for r in mouz) == [
        ("16858347", "map"), ("16858347", "match"), ("99999999", "map"), ("99999999", "match")]
    assert len(C._winline_listing_state) == 12
    # a change of ONE twin writes one row for that event id only
    changed = _with_price(twin_html, "99999999", 1, "1.31")
    assert C._winline_record_listing_prices(changed, WALL0 + 45) == 1
    last = read()[-1]
    assert (last["event_id"], last["kind"], last["map_num"], last["p1"]) == (
        "99999999", "map", 1, 1.31)


def _hero_soup():
    return BeautifulSoup(_html(P1008), "html.parser")


def _hero_wrappers(soup):
    hero = soup.select_one("ww-feature-event-live-center-dsk")
    return hero.select(".event-live-center__markets .fast-bets__wrapper")


def test_hero_winner_name_filter_total_line_before_the_winner_does_not_change_prices():
    base = _pick(_rows(P1008), "TEAM AURORA", "match", source="hero")
    soup = _hero_soup()
    wrapper = _hero_wrappers(soup)[0]  # 'Популярные на матч': Победитель, then Тотал / матч
    winner, total = wrapper.select(".bet-line")[:2]
    winner.insert_before(total.extract())  # Тотал (1.61/2.22, period 'матч') FIRST
    assert [" ".join(l.select_one(".bet-line__market-name").stripped_strings)
            for l in wrapper.select(".bet-line")][:2] == ["Тотал", "Победитель"]
    rows = odds_mod.winline_listing_price_rows(str(soup))
    match = _pick(rows, "TEAM AURORA", "match", source="hero")
    assert (match["p1"], match["p2"]) == (base["p1"], base["p2"]) == (1.22, 3.90)


def test_hero_match_period_filter_a_map_period_winner_line_is_not_the_match_row():
    soup = _hero_soup()
    match_wrapper, map_wrapper = _hero_wrappers(soup)[:2]
    winner = match_wrapper.select(".bet-line")[0]
    decoy = BeautifulSoup(str(winner), "html.parser").find(class_="bet-line")
    decoy.select_one(".bet-line__period").string = "3 карта"
    for btn, text in zip(decoy.select(".odd-btn"), ("9.91", "9.92")):
        for node in btn.find_all(string=True):
            if "1.22" in node or "3.90" in node:
                node.replace_with(text)
    winner.insert_before(decoy)  # decoy FIRST
    # and a map-wrapper Победитель line with period 'матч' ahead of the real map 2 line
    map_winner = map_wrapper.select(".bet-line")[0]
    decoy2 = BeautifulSoup(str(map_winner), "html.parser").find(class_="bet-line")
    decoy2.select_one(".bet-line__period").string = "матч"
    map_winner.insert_before(decoy2)
    rows = odds_mod.winline_listing_price_rows(str(soup))
    hero = [r for r in rows if r["source"] == "hero"]
    assert {(r["kind"], r["map_num"]) for r in hero} == {("match", None), ("map", 2)}
    match = _pick(rows, "TEAM AURORA", "match", source="hero")
    map2 = _pick(rows, "TEAM AURORA", "map", 2, source="hero")
    assert (match["p1"], match["p2"]) == (1.22, 3.90)
    assert (map2["p1"], map2["p2"]) == (1.61, 2.22)


def _set_feed_prices(html, event_id, row_index, p1, p2):
    soup = BeautifulSoup(html, "html.parser")
    node = soup.find(id="eventId-" + event_id)
    market = node.select(".card__coeffs")[row_index].find("ww-feature-event-market-dsk")
    for btn, text in zip(market.find_all(True, recursive=False)[:2], (p1, p2)):
        btn.find("span").string = text
    return str(soup)


@pytest.mark.parametrize("p1,p2,expected", [
    ("1.01", "7.50", (None, None, True)),   # at the floor: not a usable price
    ("2.10", "1.01", (None, None, True)),   # either side
    ("1.02", "7.50", (1.02, 7.5, False)),   # one tick above the floor is priced
    ("1.00", "7.50", (None, None, True)),
])
def test_price_floor_1_01_is_unusable_and_1_02_is_priced(p1, p2, expected):
    html = _set_feed_prices(_html(P1005), "16858347", 1, p1, p2)  # MOUZ '1 карта'
    row = _pick(odds_mod.winline_listing_price_rows(html), "MOUZ", "map", 1)
    assert (row["p1"], row["p2"], row["locked"]) == expected


def _locked_twin_of_map1_body(soup):
    body = soup.find(id="eventId-16858347").select_one(".card__body--second")  # 1 карта
    twin = BeautifulSoup(str(body), "html.parser").find(class_="card__body")
    market = twin.select(".card__coeffs ww-feature-event-market-dsk")[0]
    for btn in market.find_all(True, recursive=False)[:2]:
        btn["class"].append("coefficient-button_locked")
    return body, twin


@pytest.mark.parametrize("locked_first", [True, False])
def test_duplicate_label_prefers_the_priced_row_over_a_locked_one(locked_first):
    soup = BeautifulSoup(_html(P1005), "html.parser")
    body, twin = _locked_twin_of_map1_body(soup)
    (body.insert_before if locked_first else body.insert_after)(twin)
    row = _pick(odds_mod.winline_listing_price_rows(str(soup)), "MOUZ", "map", 1)
    assert (row["p1"], row["p2"], row["locked"]) == (1.29, 3.28, False)


def test_cap_smaller_than_the_pending_batch_writes_nothing_and_logs_once(
        rec, monkeypatch, capsys):
    path, read = rec
    html = _html(P1005)
    monkeypatch.setenv("WINLINE_LISTING_PRICES_MAX_MB", "0.0001")  # 104.8576 bytes
    capsys.readouterr()
    for i in range(3):
        assert C._winline_record_listing_prices(html, WALL0 + 45 * i) == 0
    assert not path.exists()  # the 10-row batch (~2.4 KB) must not be written at all
    out = capsys.readouterr().out
    assert len([l for l in out.splitlines() if "WINLINE_LISTING_PRICES_CAP" in l]) == 1, out
    assert C._winline_listing_state == {}
    # the same rows are written once the cap allows them
    monkeypatch.setenv("WINLINE_LISTING_PRICES_MAX_MB", "512")
    assert C._winline_record_listing_prices(html, WALL0 + 300) == 10


def _reset_recorder(monkeypatch, path):
    if path.exists():
        path.unlink()
    monkeypatch.setattr(C, "_winline_listing_state", {})
    monkeypatch.setattr(C, "_winline_listing_last_wall", 0.0)
    monkeypatch.setattr(C, "_winline_listing_cap_logged", False)


def test_cap_counts_the_batch_exactly_and_never_exceeds_it(rec, monkeypatch):
    path, read = rec
    html = _html(P1005)
    assert C._winline_record_listing_prices(html, WALL0) == 10
    size = path.stat().st_size  # bytes of the 10-row batch
    # cap == file + batch exactly: written, the file ends AT the cap
    _reset_recorder(monkeypatch, path)
    monkeypatch.setenv("WINLINE_LISTING_PRICES_MAX_MB", str(size / 1048576.0))
    assert C._winline_record_listing_prices(html, WALL0) == 10
    assert path.stat().st_size == size
    # cap one byte smaller: the batch is refused whole, nothing is written or truncated
    _reset_recorder(monkeypatch, path)
    monkeypatch.setenv("WINLINE_LISTING_PRICES_MAX_MB", str((size - 1) / 1048576.0))
    assert C._winline_record_listing_prices(html, WALL0) == 0
    assert not path.exists()
    # a non-empty file: existing bytes + the pending batch decide, the file is kept as is
    _reset_recorder(monkeypatch, path)
    monkeypatch.setenv("WINLINE_LISTING_PRICES_MAX_MB", "512")
    assert C._winline_record_listing_prices(html, WALL0) == 10
    kept = path.read_bytes()
    changed = _with_price(html, "16858347", 1, "1.31")
    monkeypatch.setenv("WINLINE_LISTING_PRICES_MAX_MB", str((len(kept) + 10) / 1048576.0))
    assert C._winline_record_listing_prices(changed, WALL0 + 45) == 0  # one row > 10 bytes
    assert path.read_bytes() == kept


@pytest.mark.parametrize("locked_first", [True, False])
def test_hero_duplicate_winner_line_prefers_the_priced_one_over_a_locked_one(locked_first):
    soup = _hero_soup()
    map_wrapper = _hero_wrappers(soup)[1]
    winner = map_wrapper.select(".bet-line")[0]  # Победитель / 2 карта 1.61 2.22
    twin = BeautifulSoup(str(winner), "html.parser").find(class_="bet-line")
    for btn in twin.select(".bet-line__coefs-wrapper .odd-btn"):
        btn["class"] = list(btn["class"]) + ["coef-btn--locked"]
    (winner.insert_before if locked_first else winner.insert_after)(twin)
    map2 = _pick(odds_mod.winline_listing_price_rows(str(soup)), "TEAM AURORA", "map", 2,
                 source="hero")
    assert (map2["p1"], map2["p2"], map2["locked"]) == (1.61, 2.22, False)


# --- round 3: every written row is owned by ONE DOM node ---------------------------------
# Round 2 took p1/p2 from the hero widget but team1/team2 from the feed card; when the feed
# listed the pair in the other order ('1W - TEAM AURORA') the recorder wrote
# ('1W', 'TEAM AURORA', map 2, 1.61, 2.22): TEAM AURORA's price attributed to 1W (review
# astra + native Opus, 09.10.2026).  The pages below are the REAL 08.10 hero capture plus the
# REAL 05.10 feed card of the same event (HT helpers), each edited in the one place named.

import re  # noqa: E402

_NUM_RE = re.compile(r"(?<!\d)(\d+[.,]\d+)(?!\d)")


def _swap_feed_names(html, event_id=AURORA_ID):
    """Feed card of ``event_id`` lists the pair in the OTHER order ('1W' first)."""
    soup = BeautifulSoup(html, "html.parser")
    names = soup.find(id="eventId-" + event_id).select(".body-left__names .name")
    first, second = names[0].get_text(" ", strip=True), names[1].get_text(" ", strip=True)
    names[0].string, names[1].string = second, first
    return str(soup)


def _swap_hero_names(html):
    """The hero widget lists the pair in the other order (its left logo alt follows)."""
    soup = BeautifulSoup(html, "html.parser")
    hero = soup.select_one("ww-feature-event-live-center-dsk")
    cells = [t.select_one(".match-card__team-name > div")
             for t in hero.select(".match-card__team")]
    first, second = cells[0].get_text(strip=True), cells[1].get_text(strip=True)
    cells[0].string, cells[1].string = second, first
    hero.select_one(".match-card__logo-image")["alt"] = second
    return str(soup)


def _node_names(node, source):
    if source == "feed":
        els = node.select(".body-left__names .name")
    else:
        els = node.select(".match-card__team")
    return [el.get_text(" ", strip=True) for el in els][:2]


def _row_numbers(node, source, kind, map_num):
    """First two decimal numbers of the visible text of THIS node's winner row."""
    if source == "feed":
        for label in node.select(".match-row-label, .period-name"):
            text = label.get_text(" ", strip=True)
            hit = re.search(r"(\d+)\s*карта", text)
            if (hit and kind == "map" and int(hit.group(1)) == map_num) or (
                    not hit and kind == "match" and "матч" in text.lower()):
                coeffs = label.find_next_sibling(True)
                if coeffs is not None:
                    nums = _NUM_RE.findall(coeffs.get_text(" ", strip=True))
                    if nums:
                        return [float(n.replace(",", ".")) for n in nums[:2]]
        return None
    found = []
    for wrapper in node.select(".event-live-center__markets .fast-bets__wrapper"):
        for line in wrapper.select(".bet-line"):
            name = line.select_one(".bet-line__market-name")
            if name is None or name.get_text(" ", strip=True).lower() != "победитель":
                continue
            period_el = line.select_one(".bet-line__period")
            period = period_el.get_text(" ", strip=True).lower() if period_el else ""
            hit = re.fullmatch(r"(\d+)\s*карта", period)
            if (kind == "map" and hit and int(hit.group(1)) == map_num) or (
                    kind == "match" and not hit and period in {"", "матч"}):
                nums = _NUM_RE.findall(" ".join(
                    b.get_text(" ", strip=True) for b in line.select(".odd-btn")))
                found.append([float(n.replace(",", ".")) for n in nums[:2]])
    return found[0] if found else None


_MAP_RE = re.compile(r"(\d+)\s*карта")
_ID_RE = re.compile(r"/api/cls/event/\d+/(\d+)")


def _node_header_map(soup, node, source, event_id):
    """The map number the OWNING node shows, read independently of the parser (round 4).

    feed: its own ``.header-left__time``.  hero: the page's selected pinned card ONLY if that
    card names the same two teams (and, when both carry one, the same event id) -- otherwise the
    first ``N карта`` period of the hero's own bet lines (the rows rule)."""
    if source == "feed":
        el = node.select_one(".header-left__time")
        hit = _MAP_RE.search(el.get_text(" ", strip=True)) if el else None
        return int(hit.group(1)) if hit else None
    hero_names = {
        c.get_text(" ", strip=True).casefold()
        for c in (t.select_one(".match-card__team-name > div")
                  for t in node.select(".match-card__team")) if c is not None}
    for card in soup.select("ww-pinned-card .new-card--selected"):
        pinned_names = {s.get_text(" ", strip=True).casefold()
                        for s in card.select(".card-teams__names > span")}
        pinned_ids = set(_ID_RE.findall(" ".join(
            str(i.get("src") or "") for i in card.select("img"))))
        right = card.select_one(".card-top__right")
        hit = _MAP_RE.search(right.get_text(" ", strip=True)) if right else None
        if (hit and pinned_names == hero_names
                and (not pinned_ids or not event_id or pinned_ids == {event_id})):
            return int(hit.group(1))
    for line in node.select(".event-live-center__markets .fast-bets__wrapper .bet-line"):
        period = line.select_one(".bet-line__period")
        hit = _MAP_RE.search(period.get_text(" ", strip=True)) if period else None
        if hit:
            return int(hit.group(1))
    return None


def _assert_rows_follow_their_own_node(html):
    """For EVERY written row: the owning node's visible text lists team1 before team2, its
    winner row's first two numbers are p1, p2 in that order, and header_map is the one the
    owning node shows (round 4: not a foreign pinned card's)."""
    soup = BeautifulSoup(html, "html.parser")
    rows = odds_mod.winline_listing_price_rows(html)
    hero_nodes = soup.select("ww-feature-event-live-center-dsk")
    checked = 0
    for row in rows:
        if row["source"] == "feed":
            node = soup.find(id="eventId-" + row["event_id"])
        else:
            assert len(hero_nodes) == 1, "ambiguous hero owner"
            node = hero_nodes[0]
        assert node is not None, row
        names = _node_names(node, row["source"])
        assert len(names) == 2 and row["team1"] in names[0] and row["team2"] in names[1], (
            row, names)
        assert row["header_map"] == _node_header_map(
            soup, node, row["source"], row["event_id"]), (row, "header_map not from own node")
        if row["locked"]:
            continue
        nums = _row_numbers(node, row["source"], row["kind"], row["map_num"])
        assert nums == [row["p1"], row["p2"]], (row, nums)
        checked += 1
    return rows, checked


def _swapped_pages():
    both = HT._page_with_feed_copy_of_hero_event(live=True)
    pre = HT._page_with_feed_copy_of_hero_event()
    return {
        "both_live_feed_swapped": _swap_feed_names(both),
        "both_live_hero_swapped": _swap_hero_names(both),
        "both_live_both_swapped": _swap_hero_names(_swap_feed_names(both)),
        "replaced_feed_swapped": _swap_feed_names(pre),
        "replaced_hero_swapped": _swap_hero_names(pre),
        "both_live": both,
        "replaced": pre,
    }


def test_r3_swapped_feed_names_every_hero_row_keeps_the_hero_order_and_its_own_prices():
    html = _swap_feed_names(_both_live_page())
    soup = BeautifulSoup(html, "html.parser")
    feed_names = _node_names(soup.find(id="eventId-" + AURORA_ID), "feed")
    hero_names = _node_names(soup.select_one("ww-feature-event-live-center-dsk"), "hero")
    assert feed_names == ["1W", "TEAM AURORA"] and hero_names[0].startswith("TEAM AURORA")
    mine = _event_rows(odds_mod.winline_listing_price_rows(html))
    assert set(mine) == {("match", None), ("map", 1), ("map", 2)}
    map2 = mine[("map", 2)]
    # hero row: hero's names in the hero's order, hero's prices (TEAM AURORA 1.61 first)
    assert (map2["source"], map2["team1"], map2["team2"], map2["p1"], map2["p2"]) == (
        "hero", "TEAM AURORA", "1W", 1.61, 2.22)
    # feed rows: the feed card's own order ('1W' first) with the feed's own first/second price
    for key, prices in ((("match", None), (1.80, 2.02)), (("map", 1), (1.84, 1.98))):
        row = mine[key]
        assert (row["source"], row["team1"], row["team2"], (row["p1"], row["p2"])) == (
            "feed", "1W", "TEAM AURORA", prices)
    # no row pairs one node's names with the other node's prices
    for row in mine.values():
        owner_names = ("TEAM AURORA", "1W") if row["source"] == "hero" else ("1W", "TEAM AURORA")
        assert (row["team1"], row["team2"]) == owner_names, row


def test_r3_swapped_feed_names_replaced_merge_writes_hero_rows_in_hero_order():
    mine = _event_rows(odds_mod.winline_listing_price_rows(
        _swap_feed_names(HT._page_with_feed_copy_of_hero_event())))
    assert set(mine) == {("match", None), ("map", 2)}
    for row in mine.values():
        assert (row["source"], row["team1"], row["team2"]) == ("hero", "TEAM AURORA", "1W")
    assert (mine[("map", 2)]["p1"], mine[("map", 2)]["p2"]) == (1.61, 2.22)
    assert (mine[("match", None)]["p1"], mine[("match", None)]["p2"]) == (1.22, 3.90)


def test_r3_swapped_feed_names_recorder_file_has_no_wrong_side_price(rec):
    path, read = rec
    assert C._winline_record_listing_prices(_swap_feed_names(_both_live_page()), WALL0) == 5
    assert len([r for r in read() if r["event_id"] == AURORA_ID]) == 3  # + 2 PARIVISION rows
    mine = {(r["kind"], r["map_num"]): r for r in read() if r["event_id"] == AURORA_ID}
    hero = mine[("map", 2)]
    assert (hero["team1"], hero["p1"], hero["team2"], hero["p2"]) == (
        "TEAM AURORA", 1.61, "1W", 2.22)
    assert not [r for r in read() if r["source"] == "hero" and r["team1"] == "1W"]


@pytest.mark.parametrize("name", ALL_PAGES)
def test_r3_invariant_rows_follow_their_own_node_on_every_fixture(name):
    rows, checked = _assert_rows_follow_their_own_node(_html(name))
    assert rows and checked > 0


@pytest.mark.parametrize("variant", sorted(_swapped_pages()))
def test_r3_invariant_rows_follow_their_own_node_on_synthetic_pages(variant):
    rows, checked = _assert_rows_follow_their_own_node(_swapped_pages()[variant])
    assert checked > 0
    assert any(r["source"] == "hero" for r in rows)


def test_r3_recorder_key_keeps_map_num_both_live_page_writes_map_1_and_map_2(rec):
    path, read = rec
    assert C._winline_record_listing_prices(_both_live_page(), WALL0) == 5
    assert len([r for r in read() if r["event_id"] == AURORA_ID]) == 3  # + 2 PARIVISION rows
    mine = {(r["kind"], r["map_num"]): r for r in read() if r["event_id"] == AURORA_ID}
    assert set(mine) == {("match", None), ("map", 1), ("map", 2)}
    assert (mine[("map", 1)]["source"], mine[("map", 1)]["p1"], mine[("map", 1)]["p2"]) == (
        "feed", 1.84, 1.98)
    assert (mine[("map", 2)]["source"], mine[("map", 2)]["p1"], mine[("map", 2)]["p2"]) == (
        "hero", 1.61, 2.22)
    assert len({k for k in C._winline_listing_state if k[0] == AURORA_ID}) == 3


# --- round 4: header_map belongs to the hero's own event; first-merge guard --------------------
# astra r3: ``_winline_hero_cards`` took the hero's header_map from the page-global selected
# pinned card without checking it is the hero's event.  Pages below are the REAL 08.10 capture
# (P1008, whose pinned card is TEAM AURORA - 1W, '2карта', event 16855095) with the pinned card
# edited in the one place named.


def _edit_pinned(html, names=None, event_id=None, label=None, drop=False):
    soup = BeautifulSoup(html, "html.parser")
    pin = soup.select_one("ww-pinned-card")
    if drop:
        pin.decompose()
        return str(soup)
    card = pin.select_one(".new-card--selected")
    if names:
        spans = card.select(".card-teams__names > span")
        spans[0].string, spans[1].string = names
    if event_id:
        for img in card.select("img"):
            img["src"] = re.sub(r"(/api/cls/event/\d+/)\d+", r"\g<1>" + event_id, img["src"])
    if label:
        card.select_one(".card-top__right span").string = label
    return str(soup)


FOREIGN_PINNED = {
    # astra's probe: another match's card, 5 карта
    "foreign_names_and_id": dict(names=("OTHER A", "OTHER B"), event_id="99999999",
                                 label="5карта"),
    "foreign_names_only": dict(names=("OTHER A", "OTHER B"), label="5карта"),
    # same two teams but another event (a rematch listed elsewhere): ids disagree
    "same_names_other_event": dict(event_id="99999999", label="5карта"),
    # one of the two teams is foreign
    "one_team_foreign": dict(names=("TEAM AURORA", "OTHER B"), label="5карта"),
}


def _aurora_cards(html):
    return [c for c in odds_mod.winline_enumerate_live_cards(html)
            if c["event_id"] == AURORA_ID]


@pytest.mark.parametrize("variant", sorted(FOREIGN_PINNED))
def test_r4_foreign_pinned_card_does_not_set_the_hero_header_map(variant, rec):
    html = _edit_pinned(_html(P1008), **FOREIGN_PINNED[variant])
    # the edited page really shows the foreign '5 карта' in the pinned bar
    assert "5карта" in BeautifulSoup(html, "html.parser").select_one(
        "ww-pinned-card .card-top__right").get_text()
    (card,) = _aurora_cards(html)
    assert card["source"] == "hero" and card["live"] is True
    assert card["header_map"] == 2  # the rows rule: the hero's own first 'N карта' line
    mine = _event_rows(odds_mod.winline_listing_price_rows(html))
    assert set(mine) == {("match", None), ("map", 2)}
    assert {r["header_map"] for r in mine.values()} == {2}
    path, read = rec
    C._winline_record_listing_prices(html, WALL0)
    written = [r for r in read() if r["event_id"] == AURORA_ID]
    assert len(written) == 2 and {r["header_map"] for r in written} == {2}


def test_r4_unedited_capture_keeps_the_pinned_map_for_the_same_event():
    (card,) = _aurora_cards(_html(P1008))
    assert card["header_map"] == 2


@pytest.mark.parametrize("edit", [
    dict(label="3карта"),                                   # same event, pinned shows map 3
    dict(label="3карта", names=("1W", "TEAM AURORA")),     # same event, pair in other order
    dict(label="3 карта", event_id=AURORA_ID),
])
def test_r4_pinned_card_of_the_same_event_still_wins(edit):
    html = _edit_pinned(_html(P1008), **edit)
    (card,) = _aurora_cards(html)
    assert card["header_map"] == 3
    mine = _event_rows(odds_mod.winline_listing_price_rows(html))
    assert {r["header_map"] for r in mine.values()} == {3}


def test_r4_no_pinned_card_falls_back_to_the_hero_rows():
    html = _edit_pinned(_html(P1008), drop=True)
    assert "ww-pinned-card" not in html
    (card,) = _aurora_cards(html)
    assert card["header_map"] == 2


def test_r4_matching_pinned_card_without_a_map_label_falls_back_to_the_hero_rows():
    html = _edit_pinned(_html(P1008), label="BO3")
    (card,) = _aurora_cards(html)
    assert card["header_map"] == 2


def test_r4_foreign_pinned_card_on_a_replaced_merge_page_keeps_the_hero_map():
    html = _edit_pinned(HT._page_with_feed_copy_of_hero_event(),
                        **FOREIGN_PINNED["foreign_names_and_id"])
    (card,) = _aurora_cards(html)
    assert card["live"] is True and card["header_map"] == 2
    mine = _event_rows(odds_mod.winline_listing_price_rows(html))
    assert {r["header_map"] for r in mine.values()} == {2}


def _foreign_pinned_pages():
    foreign = FOREIGN_PINNED["foreign_names_and_id"]
    return {
        "hero_only": _edit_pinned(_html(P1008), **foreign),
        "replaced": _edit_pinned(HT._page_with_feed_copy_of_hero_event(), **foreign),
        "both_live": _edit_pinned(_both_live_page(), **foreign),
        "same_names_other_event": _edit_pinned(
            _html(P1008), **FOREIGN_PINNED["same_names_other_event"]),
        "same_event_map_3": _edit_pinned(_html(P1008), label="3карта"),
        "no_pinned": _edit_pinned(_html(P1008), drop=True),
    }


@pytest.mark.parametrize("variant", sorted(_foreign_pinned_pages()))
def test_r4_invariant_header_map_comes_from_the_rows_own_node_on_pinned_variants(variant):
    """The round-3 invariant now also pins header_map: a hero row may take the pinned card's
    map only when that card is the hero's event."""
    rows, checked = _assert_rows_follow_their_own_node(_foreign_pinned_pages()[variant])
    assert checked > 0 and any(r["source"] == "hero" for r in rows)


def _reverse_hero_node(hero):
    """The hero widget shows the pair in the other order, buttons included (a REAL reversal)."""
    cells = [t.select_one(".match-card__team-name > div")
             for t in hero.select(".match-card__team")]
    first, second = cells[0].get_text(strip=True), cells[1].get_text(strip=True)
    cells[0].string, cells[1].string = second, first
    hero.select_one(".match-card__logo-image")["alt"] = second
    for line in hero.select(".bet-line"):
        btns = line.select(".odd-btn")[:2]
        if len(btns) < 2:
            continue
        texts = [[s for s in b.find_all(string=True) if _NUM_RE.search(s)] for b in btns]
        if texts[0] and texts[1]:
            a, b = str(texts[0][0]), str(texts[1][0])
            texts[0][0].replace_with(b)
            texts[1][0].replace_with(a)


def _page_with_second_idless_reversed_hero(html):
    """``html`` + a second hero block of the same pair: no event id anywhere, other prices
    (1.71/2.32, 1.32/4.00), listed in the REVERSE order (verifier r3 case K2)."""
    soup = BeautifulSoup(html, "html.parser")
    hero = soup.select_one("ww-feature-event-live-center-dsk")
    twin = BeautifulSoup(HT._strip_hero_ids(str(hero)), "html.parser").find(
        "ww-feature-event-live-center-dsk")
    for text in twin.find_all(string=True):
        for old, new in (("1.61", "1.71"), ("2.22", "2.32"), ("1.22", "1.32"), ("3.90", "4.00")):
            if text.strip() == old:
                text.replace_with(text.replace(old, new))
                break
    _reverse_hero_node(twin)
    hero.insert_after(twin)
    return str(soup)


def test_r4_k2_first_merge_is_kept_a_second_idless_reversed_hero_does_not_replace_it():
    """Replaced merge (hero #1 carries the id, feed card is pre-match) followed by an id-less
    reversed hero #2 of the same pair.  The recorder must still write ONLY hero #1's rows,
    in hero #1's order, with hero #1's prices - no stale pre-match feed row marked live and
    nothing from hero #2.  Red when ``id(target) not in _merges_out`` is dropped."""
    html = _page_with_second_idless_reversed_hero(HT._page_with_feed_copy_of_hero_event())
    heroes = BeautifulSoup(html, "html.parser").select("ww-feature-event-live-center-dsk")
    assert len(heroes) == 2
    mine = [r for r in odds_mod.winline_listing_price_rows(html)
            if r["event_id"] == AURORA_ID]
    got = {(r["source"], r["kind"], r["map_num"], r["team1"], r["team2"], r["p1"], r["p2"],
            r["live"], r["header_map"]) for r in mine}
    assert len(mine) == len(got) == 2, sorted(got, key=str)
    assert got == {
        ("hero", "match", None, "TEAM AURORA", "1W", 1.22, 3.90, True, 2),
        ("hero", "map", 2, "TEAM AURORA", "1W", 1.61, 2.22, True, 2)}, sorted(got, key=str)
