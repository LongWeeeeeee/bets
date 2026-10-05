"""A Winline acquisition error must not evict a still-fresh open quote.

Input: real rows of runtime/winline_odds_history.jsonl captured on serv1
(see fixtures/winline_error_flap_20261005.provenance.json). Only the monotonic
clock is mocked; the real _winline_record_quote_observation writes the state and
the real _ml_dispatch_fresh_winline_price / _ml_dispatch_min_odds_reject_for_delivery
read it (the price the ML WIN floor actually sees).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
import cyberscore_try as C  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "winline_error_flap_20261005.jsonl"
ROWS = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]
KEY = ROWS[0]["canonical_key"]


def _row(status: str, session: str) -> dict:
    """First captured row with this market_status in the 2026-09-15 or 2026-09-20 session."""
    day = {"0915": (1789400000, 1789600000), "0920": (1789900000, 1790000000)}[session]
    return next(r for r in ROWS if r["market_status"] == status and day[0] <= r["wall"] <= day[1])


OPEN = _row("open", "0920")
ERROR = _row("error", "0920")
CLOSED = _row("closed", "0915")
MISSING = _row("missing", "0915")


@pytest.fixture
def clock(monkeypatch):
    monkeypatch.setattr(C, "_winline_odds_orientation_state", {})
    monkeypatch.delenv("WINLINE_ERROR_KEEPS_FRESH_PRICE", raising=False)
    now = {"t": 1000.0}
    monkeypatch.setattr(C.time, "monotonic", lambda: now["t"])
    return now


def _observe(clock, row: dict, at: float) -> None:
    clock["t"] = at
    # The captured row IS the observation payload (market_status, p1_odds, p2_odds, page_valid ...).
    C._winline_record_quote_observation(KEY, dict(row))


def _price(clock, at: float, target: str = "KALMYCHATA", game_time: float = 600.0):
    clock["t"] = at
    ctx = {"radiant_team_name": "KALMYCHATA", "dire_team_name": "TWO MOVE",
           "stake_team_name": target, "game_time_seconds": game_time}
    return C._ml_dispatch_fresh_winline_price(ctx, 1)


def test_fixture_is_the_captured_flap():
    assert ERROR["miss_fingerprint"].startswith("feed_blocks=0")
    # The keep-predicate: a skeleton failure with NO market evidence.
    assert ERROR["page_valid"] is False and ERROR["acquisition_error"]
    assert ERROR["match_found"] is False and "odds_bettable" not in ERROR
    assert 100 < ERROR["wall"] - OPEN["wall"] < 300
    assert OPEN["p1_odds"] == 3.0 and OPEN["p2_odds"] == 1.3


def test_error_after_open_within_ttl_keeps_price(clock):
    _observe(clock, OPEN, 1000.0)
    _observe(clock, ERROR, 1000.0 + (ERROR["wall"] - OPEN["wall"]))
    decision_at = 1000.0 + (ERROR["wall"] - OPEN["wall"]) + 10.0  # 139.5 s after the quote
    assert _price(clock, decision_at) == 3.0
    assert _price(clock, decision_at, target="TWO MOVE") == 1.3


def test_floor_boundary_still_blocks_after_error_flap(clock):
    """Delivery boundary: the floor reject the bot would send is unchanged by an error."""
    _observe(clock, OPEN, 1000.0)
    _observe(clock, ERROR, 1130.0)
    clock["t"] = 1140.0
    ctx = {"origin": "ml_dispatch", "ml_market": "win", "stake_team_name": "TWO MOVE",
           "radiant_team_name": "KALMYCHATA", "dire_team_name": "TWO MOVE",
           "game_time_seconds": 600.0, "calibration": {"expected_wr": 0.8, "min_odds": 1.4}}
    prev = (C.BOOKMAKER_PREFETCH_ENABLED, C.BOOKMAKER_PREFETCH_GATE_MODE)
    C.BOOKMAKER_PREFETCH_ENABLED = False  # prod runs --no-odds: floor reads the in-process quote
    try:
        block = C._ml_dispatch_min_odds_reject_for_delivery("СТАВКА НА TWO MOVE x1\n", ctx, "m", 1)
    finally:
        C.BOOKMAKER_PREFETCH_ENABLED, C.BOOKMAKER_PREFETCH_GATE_MODE = prev
    assert block == {"reason": "ml_min_odds_below_floor", "min_odds": 1.4, "price": 1.3,
                     "price_source": "winline_poll"}


def test_error_after_ttl_gives_no_price(clock):
    _observe(clock, OPEN, 1000.0)
    _observe(clock, ERROR, 1320.0)  # 320 s later: already past the 300 s TTL
    assert _price(clock, 1330.0) is None


def test_ttl_is_not_extended_by_error(clock):
    _observe(clock, OPEN, 1000.0)
    _observe(clock, ERROR, 1100.0)
    assert _price(clock, 1299.0) == 3.0
    assert _price(clock, 1301.0) is None  # TTL counts from the open quote, not from the error


@pytest.mark.parametrize("terminal_row", [CLOSED, MISSING], ids=["closed", "missing"])
def test_closed_or_missing_after_open_evicts_immediately(clock, terminal_row):
    _observe(clock, OPEN, 1000.0)
    _observe(clock, terminal_row, 1050.0)
    assert _price(clock, 1051.0) is None


def test_terminal_flag_evicts_even_if_payload_says_error(clock):
    _observe(clock, OPEN, 1000.0)
    clock["t"] = 1050.0
    C._winline_record_quote_observation(KEY, dict(ERROR), terminal=True)
    assert _price(clock, 1051.0) is None


def test_error_then_new_open_quote_refreshes(clock):
    _observe(clock, OPEN, 1000.0)
    _observe(clock, ERROR, 1100.0)
    _observe(clock, dict(OPEN, p1_odds=3.4, p2_odds=1.25), 1200.0)
    assert _price(clock, 1450.0) == 3.4


def test_env_zero_restores_old_behaviour(clock, monkeypatch):
    monkeypatch.setenv("WINLINE_ERROR_KEEPS_FRESH_PRICE", "0")
    _observe(clock, OPEN, 1000.0)
    _observe(clock, ERROR, 1130.0)
    assert _price(clock, 1140.0) is None


def test_error_without_prior_quote_leaves_no_price(clock):
    _observe(clock, ERROR, 1000.0)
    assert _price(clock, 1001.0) is None


def _without(row: dict, field: str) -> dict:
    out = dict(row)
    out.pop(field)
    return out


@pytest.mark.parametrize(
    "error_row",
    [
        dict(ERROR, match_found=True),              # error WITH a found pair card: market evidence
        dict(ERROR, odds_bettable=False),           # error on a frozen market
        _without(ERROR, "match_found"),             # key absent: no proof the card was missing
        dict(ERROR, match_found=None),              # explicit None, same
        dict(ERROR, page_valid=True),               # page loaded fine: not a skeleton failure
        _without(ERROR, "page_valid"),              # key absent
        dict(ERROR, match_found=True, odds_bettable=False),
    ],
    ids=["match_found_true", "odds_bettable_false", "match_found_absent", "match_found_none",
         "page_valid_true", "page_valid_absent", "found_and_frozen"],
)
def test_error_with_market_evidence_evicts_like_before(clock, error_row):
    _observe(clock, OPEN, 1000.0)
    _observe(clock, error_row, 1130.0)
    assert _price(clock, 1140.0) is None
    assert _price(clock, 1140.0, target="TWO MOVE") is None


def test_error_with_odds_bettable_true_and_skeleton_still_keeps(clock):
    """odds_bettable only vetoes when it is explicitly False (the poller attempt omits it)."""
    _observe(clock, OPEN, 1000.0)
    _observe(clock, dict(ERROR, odds_bettable=True), 1130.0)
    assert _price(clock, 1140.0) == 3.0


# --- round 3: a kept (unconfirmed) price may only stand in when no confirmed fresh quote exists ---

KEY_B = KEY.replace("european pro league", "second poller same pair")
assert KEY_B != KEY and KEY_B.split("|")[1:] == KEY.split("|")[1:]


def _observe_key(clock, key: str, row: dict, at: float) -> None:
    clock["t"] = at
    C._winline_record_quote_observation(key, dict(row))


def _floor(clock, at: float, stake: str = "KALMYCHATA", min_odds: float = 1.4):
    """The ML WIN floor the bot applies in prod (--no-odds: reads the in-process quote)."""
    clock["t"] = at
    ctx = {"origin": "ml_dispatch", "ml_market": "win", "stake_team_name": stake,
           "radiant_team_name": "KALMYCHATA", "dire_team_name": "TWO MOVE",
           "game_time_seconds": 600.0, "calibration": {"expected_wr": 0.8, "min_odds": min_odds}}
    prev = (C.BOOKMAKER_PREFETCH_ENABLED, C.BOOKMAKER_PREFETCH_GATE_MODE)
    C.BOOKMAKER_PREFETCH_ENABLED = False
    try:
        return C._ml_dispatch_min_odds_reject_for_delivery(
            f"СТАВКА НА {stake} x1\n", ctx, "m", 1)
    finally:
        C.BOOKMAKER_PREFETCH_ENABLED, C.BOOKMAKER_PREFETCH_GATE_MODE = prev


def _run(monkeypatch, clock, flag: str, steps, read_at: float, stake="KALMYCHATA", read=None):
    """Fresh state, env flag set, replay steps [(key, row, at)], then read."""
    monkeypatch.setattr(C, "_winline_odds_orientation_state", {})
    monkeypatch.setenv("WINLINE_ERROR_KEEPS_FRESH_PRICE", flag)
    for key, row, at in steps:
        _observe_key(clock, key, row, at)
    return (read or (lambda: _price(clock, read_at, target=stake)))()


def test_astra_two_keys_kept_price_must_not_remove_a_block(clock, monkeypatch):
    """Key A open 3.0 then skeleton error (kept); key B fresh valid 1.3 (age 10 s); floor 1.4."""
    steps = [(KEY, OPEN, 1000.0), (KEY_B, dict(OPEN, p1_odds=1.3, p2_odds=3.0), 1190.0),
             (KEY, ERROR, 1195.0)]
    out = {}
    for flag in ("0", "1"):
        out[flag] = _run(monkeypatch, clock, flag, steps, 1200.0,
                         read=lambda: _floor(clock, 1200.0))
    expected = {"reason": "ml_min_odds_below_floor", "min_odds": 1.4, "price": 1.3,
                "price_source": "winline_poll"}
    assert out["0"] == expected
    assert out["1"] == out["0"]


def test_kept_price_loses_to_a_higher_confirmed_price_exactly_like_flag_off(clock, monkeypatch):
    steps = [(KEY, OPEN, 1000.0), (KEY_B, dict(OPEN, p1_odds=4.0, p2_odds=1.2), 1190.0),
             (KEY, ERROR, 1195.0)]
    res = {f: _run(monkeypatch, clock, f, steps, 1200.0) for f in ("0", "1")}
    assert res == {"0": 4.0, "1": 4.0}
    # and with a LOWER confirmed price than the kept one, the confirmed one still wins
    steps = [(KEY, OPEN, 1000.0), (KEY_B, dict(OPEN, p1_odds=2.0, p2_odds=1.9), 1190.0),
             (KEY, ERROR, 1195.0)]
    res = {f: _run(monkeypatch, clock, f, steps, 1200.0) for f in ("0", "1")}
    assert res == {"0": 2.0, "1": 2.0}


def test_only_kept_quotes_flag_off_none_flag_on_kept_within_ttl_only(clock, monkeypatch):
    steps = [(KEY, OPEN, 1000.0), (KEY, ERROR, 1100.0),
             (KEY_B, dict(OPEN, p1_odds=2.5, p2_odds=1.5), 1010.0), (KEY_B, ERROR, 1110.0)]
    assert _run(monkeypatch, clock, "0", steps, 1200.0) is None
    assert _run(monkeypatch, clock, "1", steps, 1200.0) == 3.0          # max(kept A 3.0, kept B 2.5)
    assert _run(monkeypatch, clock, "1", steps, 1305.0) == 2.5          # A (age 305) past TTL, B (age 295) alive
    assert _run(monkeypatch, clock, "1", steps, 1311.0) is None         # both past 300 s


def test_invariant_flag_on_equals_flag_off_whenever_a_confirmed_quote_exists(clock, monkeypatch):
    """Exhaustive small model over two keys: A in {open, open+error, open+closed}, B in
    {absent, open high/low, open+error, open+closed}; read at several ages."""
    a_modes = {"open": [(KEY, OPEN, 1000.0)],
               "kept": [(KEY, OPEN, 1000.0), (KEY, ERROR, 1100.0)],
               "closed": [(KEY, OPEN, 1000.0), (KEY, CLOSED, 1100.0)]}
    b_open_hi = dict(OPEN, p1_odds=4.0, p2_odds=1.2)
    b_open_lo = dict(OPEN, p1_odds=1.3, p2_odds=3.0)
    b_modes = {"absent": [], "hi": [(KEY_B, b_open_hi, 1150.0)], "lo": [(KEY_B, b_open_lo, 1150.0)],
               "kept": [(KEY_B, b_open_hi, 1150.0), (KEY_B, ERROR, 1160.0)],
               "closed": [(KEY_B, b_open_hi, 1150.0), (KEY_B, MISSING, 1160.0)]}
    confirmed_b = {"hi": 4.0, "lo": 1.3}
    checked = confirmed_only = kept_only = 0
    for an, asteps in a_modes.items():
        for bn, bsteps in b_modes.items():
            for read_at in (1200.0, 1320.0, 1420.0, 1470.0):
                steps = sorted(asteps + bsteps, key=lambda s: s[2])
                off = _run(monkeypatch, clock, "0", steps, read_at)
                on = _run(monkeypatch, clock, "1", steps, read_at)
                checked += 1
                if off is not None:
                    assert on == off, (an, bn, read_at, off, on)
                    confirmed_only += 1
                else:
                    kept = []
                    if an == "kept" and read_at - 1000.0 <= 300.0:
                        kept.append(3.0)
                    if bn == "kept" and read_at - 1150.0 <= 300.0:
                        kept.append(4.0)
                    assert on == (max(kept) if kept else None), (an, bn, read_at, on)
                    kept_only += bool(kept)
    assert checked == 60 and confirmed_only > 10 and kept_only > 3


def test_confirmed_quote_clears_the_kept_marker(clock, monkeypatch):
    monkeypatch.setattr(C, "_winline_odds_orientation_state", {})
    _observe_key(clock, KEY, OPEN, 1000.0)
    _observe_key(clock, KEY, ERROR, 1100.0)
    assert "kept_after_error_mono" in C._winline_odds_orientation_state[KEY]
    _observe_key(clock, KEY, dict(OPEN, p1_odds=2.0, p2_odds=1.9), 1150.0)
    assert "kept_after_error_mono" not in C._winline_odds_orientation_state[KEY]
    # now A is confirmed again: it competes with a confirmed B by max, not as "kept"
    _observe_key(clock, KEY_B, dict(OPEN, p1_odds=1.3, p2_odds=3.0), 1160.0)
    assert _price(clock, 1170.0) == 2.0


def _skeleton_site_result(match_found=False):
    """Duck-typed SiteResult of a skeleton failure, field values from the captured ERROR row."""
    from types import SimpleNamespace
    return SimpleNamespace(
        odds=[], status="ok", market_closed=False, market_kind="",
        source=ERROR["source"], acquisition_error=ERROR["acquisition_error"], error=None,
        load_error=None, match_found=match_found, miss_fingerprint=ERROR["miss_fingerprint"],
        page_url="https://winline.ru/stavki/sport/kibersport/dota-2", details="x" * 64,
        body_text="x" * 64, dom_signature="1234", map_num=1, acquisition_mode="initial_goto",
        parser_failure_reasons=[], p1_team=None, p2_team=None, odds_bettable=None,
        promoted_from_match=False, card_team_order=None, card_odds=None)


def _normalized(result):
    return C._winline_map_site_result_to_collector_dict(
        result, acquisition_mode="initial_goto", series="league:european pro league",
        map_num=1, team1="KALMYCHATA", team2="TWO MOVE", expected_url=None)


def test_real_normalizer_output_for_skeleton_error_matches_captured_row_and_keeps_price(
        clock, monkeypatch):
    """Seam: cyberscore's own normalization (no network/browser needed: the converter is a pure
    function over a SiteResult-shaped object) -> observer -> floor (--no-odds, no reserved price)."""
    payload = _normalized(_skeleton_site_result(match_found=False))
    for field in ("market_status", "page_valid", "match_found", "acquisition_error", "source"):
        assert payload[field] == ERROR[field], field
    assert payload["odds_bettable"] is None

    monkeypatch.setattr(C, "_winline_odds_orientation_state", {})
    _observe_key(clock, KEY, OPEN, 1000.0)
    _observe_key(clock, KEY, payload, 1130.0)
    # TWO MOVE 1.3 < floor 1.4: the kept price blocks; flag off would have passed with no price
    assert _floor(clock, 1140.0, stake="TWO MOVE") == {
        "reason": "ml_min_odds_below_floor", "min_odds": 1.4, "price": 1.3,
        "price_source": "winline_poll"}
    monkeypatch.setenv("WINLINE_ERROR_KEEPS_FRESH_PRICE", "0")
    monkeypatch.setattr(C, "_winline_odds_orientation_state", {})
    _observe_key(clock, KEY, OPEN, 1000.0)
    _observe_key(clock, KEY, payload, 1130.0)
    assert _floor(clock, 1140.0, stake="TWO MOVE") is None


def test_real_normalizer_error_with_found_card_evicts_and_floor_passes(clock, monkeypatch):
    payload = _normalized(_skeleton_site_result(match_found=True))
    assert payload["market_status"] == "error" and payload["match_found"] is True
    monkeypatch.setattr(C, "_winline_odds_orientation_state", {})
    _observe_key(clock, KEY, OPEN, 1000.0)
    _observe_key(clock, KEY, payload, 1130.0)
    assert _price(clock, 1140.0, target="TWO MOVE") is None
    assert _floor(clock, 1140.0, stake="TWO MOVE") is None
