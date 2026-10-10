"""Winline presence gate at the single bet-delivery point (owner 10.10.2026).

Why: a bet "PuckChamp vs Old blood" reached Telegram although the match is not on Winline
(0 occurrences of PuckChamp in the Winline overview snapshot and in 4 976 priced listing
rows since 09.10).  ``_winline_presence_reject_for_delivery`` (called inside
``_deliver_and_persist_signal``) holds a bet whose match has no card in the Winline
overview; a name-parse miss (ONE_SIDE) and any unhealthy listing (UNKNOWN) always pass.

Input is a REAL captured overview page: ``winline_overview_snapshot_20261010_presence_gate.json.gz``
= serv1 ``/root/main/runtime/winline_overview_snapshot.json`` captured 10.10.2026 17:54 MSK with
``ssh serv1 'cd /root/main/runtime && tar czf - winline_overview_snapshot.json' | tar xzf -``
(keys wall/text/html, html 608 545 chars, 7 cards: PARIVISION-TEAM AURORA live, OVEA-POTN9KI,
DARK TAMPLARS-AZURE DRAGONS, TEAM SPIRIT-"PARIVISION / TEAM AURORA" and 3 player-duel props).
The hold-then-release case renames one captured card (OVEA/POTN9KI -> OLD BLOOD/PUCKCHAMP) to
model "the line appears later"; everything else uses the page byte for byte.

Only Telegram (``send_message`` / ``send_winline_odds_message``) and the clock-free bookkeeping
around delivery are stubbed; the gate, the card enumerator and the matcher run for real.
"""
from __future__ import annotations

import gzip
import json
import sys
import time
import types
from pathlib import Path

import pytest

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
from team_name_aliases import names_match_loosely  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "winline_overview_snapshot_20261010_presence_gate.json.gz"
MATCH_KEY = "dltv.org/matches/8999999999.12"


def _snapshot() -> dict:
    with gzip.open(FIXTURE, "rt", encoding="utf-8") as handle:
        return json.loads(handle.read())


@pytest.fixture(scope="module")
def snap() -> dict:
    return _snapshot()


@pytest.fixture(scope="module")
def cards(snap):
    return [c for c in odds_mod.winline_enumerate_live_cards(snap["html"]) if not c.get("prop_duel")]


def test_fixture_is_the_captured_page(snap, cards):
    """Guards the fixture: the cases below pick real pairs from this page."""
    assert len(snap["html"]) == 608_545
    pairs = {(c["team1"], c["team2"]) for c in cards}
    assert ("PARIVISION", "TEAM AURORA") in pairs
    assert ("OVEA", "POTN9KI") in pairs
    assert ("DARK TAMPLARS", "AZURE DRAGONS") in pairs
    assert "puckchamp" not in snap["html"].lower()


# --------------------------------------------------------------------------- matcher

@pytest.mark.parametrize("ours,theirs", [
    ("Blasterbl", "BLASTERBI"),            # 05.10: l read as I
    ("1win", "1W"),                          # alias table
    ("ЯЧЁ123", "YACHE123"),                  # alias table (cyrillic -> translit)
    ("Aurora Gaming", "TEAM AURORA"),        # generic tokens dropped
    ("Iron Wing", "ironwing"),               # spaces
    ("BetBoom Team", "BB TEAM"),             # alias table
    ("Dark Tamplars", "DARK TAMPLARS"),
    ("TEAM TPABOMAH", "ТРАВОМАН"),           # cyrillic look-alikes folded
    ("Natus Vincere", "NAVI"),               # 10.10 review: tag vs full name
    ("LGD Gaming", "PSG.LGD"),
    ("Team Spirit", "Тим Спирит"),
])
def test_matcher_accepts_known_spelling_differences(ours, theirs):
    assert names_match_loosely(ours, theirs) and names_match_loosely(theirs, ours)


@pytest.mark.parametrize("ours,theirs", [
    ("PuckChamp", "OVEA"),
    ("Old blood", "POTN9KI"),
    ("Team Spirit", "Team Spirit Academy"),  # different rosters
    ("Team Spirit", "TEAM SPIRIT ACADEMY"),
    ("Cloud Dawning", "CLOUD RISING"),
    ("Nigma Galaxy", "Team Liquid"),
    ("ABC", "ABD"),                          # too short for the fuzzy rule
    ("", "OVEA"),
])
def test_matcher_rejects_different_teams(ours, theirs):
    assert not names_match_loosely(ours, theirs)


# --------------------------------------------------------------------------- delivery boundary

@pytest.fixture
def gate_env(monkeypatch, tmp_path, snap):
    """Delivery with only Telegram and bookkeeping stubbed; overview state under test control."""
    out = types.SimpleNamespace(sent=[], alerts=[], add_url=[], ledger=[], blocked=[],
                                prepared=[], journal=tmp_path / "presence.jsonl")
    monkeypatch.delenv("BET_REQUIRE_WINLINE_LISTING", raising=False)
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    monkeypatch.setenv("WINLINE_PRESENCE_GATE_PATH", str(out.journal))
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(C, "_is_denylisted_bet_team_name", lambda _name: False)
    monkeypatch.setattr(C, "_bookmaker_prepare_message_for_delivery",
                        lambda _key, text, **_kw: out.prepared.append(text) or (text, True, "disabled", None))
    for name in ("_half_stake_elo_underdog_reject_for_delivery",
                 "_win_model_reject_for_delivery", "_late_win_model_reject_for_delivery"):
        monkeypatch.setattr(C, name, lambda *_a, **_kw: None)
    monkeypatch.setattr(C, "_record_delivery_gate_block",
                        lambda _key, _text, decision, **kw: out.blocked.append((decision, kw)))
    monkeypatch.setattr(C, "_signal_fingerprint_try_reserve", lambda *_a: (True, "test-fp"))
    monkeypatch.setattr(C, "_signal_fingerprint_mark_sent", lambda *_a: None)
    monkeypatch.setattr(C, "_record_bet_dispatch_ledger", lambda *_a, **_kw: out.ledger.append(_a))
    monkeypatch.setattr(C, "decelerate_winline_current_map_polling", lambda *_a: None)
    monkeypatch.setattr(C, "add_url", lambda *_a, **_kw: out.add_url.append(_a))
    monkeypatch.setattr(C, "send_message", lambda text, **_kw: out.sent.append(text))
    monkeypatch.setattr(C, "send_winline_odds_message",
                        lambda text, **_kw: out.alerts.append(text) or True)
    monkeypatch.setattr(C, "_WINLINE_PRESENCE_ALERT_ASYNC", False)
    monkeypatch.setattr(C, "_winline_odds_orientation_state", {})
    monkeypatch.setattr(C, "_winline_presence_cards_cache", {"key": None, "cards": None, "error": ""})
    monkeypatch.setattr(C, "_winline_presence_last_class", {})
    monkeypatch.setattr(C, "_winline_presence_alert_attempts", {})
    monkeypatch.setattr(C, "_winline_presence_alert_inflight", set())
    monkeypatch.setattr(C, "_winline_presence_text_cache", {"key": None, "concat": "", "bounds": None},
                        raising=False)
    monkeypatch.setattr(C, "_winline_presence_exc_last_log", {}, raising=False)

    def set_listing(html=None, fetched_ago=1.0, text=None, status="ok", error=""):
        monkeypatch.setattr(C, "_winline_overview_state", {
            "text": snap["text"] if text is None else text,
            "html": snap["html"] if html is None else html,
            "fetched_at": time.time() - fetched_ago, "status": status, "error": error,
            "refresh_failed_at": 0.0})

    set_listing()
    out.set_listing = set_listing

    def deliver(radiant, dire, *, target=None, market="win", map_num=1, text=None, ctx=True,
                details=None):
        target = target or radiant
        header = f"СТАВКА НА {target} x1" if market == "win" else "СТАВКА НА Тотал килов 55.5 БОЛЬШЕ"
        message = text if text is not None else f"{header}\nСтавить от кэфа 1.60\n"
        context = ({"origin": "ml_dispatch", "ml_market": market, "ml_rule": "test_rule",
                    "stake_team_name": target, "target_side": "radiant",
                    "radiant_team_name": radiant, "dire_team_name": dire,
                    "game_time_seconds": 0} if ctx else None)
        return C._deliver_and_persist_signal(
            MATCH_KEY, message, add_url_reason="ml_dispatch", map_num=map_num,
            selected_side="radiant", stake_multiplier_context=context, add_url_details=details)

    out.deliver = deliver
    return out


def _rows(path: Path) -> list:
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


@pytest.mark.parametrize("radiant,dire", [
    ("OVEA", "POTN9KI"),
    ("Potn9ki", "Ovea"),                    # order does not matter
    ("Parivision", "Aurora Gaming"),         # spelling differs from the card (TEAM AURORA)
])
def test_pair_present_in_listing_is_sent(gate_env, radiant, dire):
    assert gate_env.deliver(radiant, dire) is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    assert len(gate_env.add_url) == 1 and len(gate_env.ledger) == 1
    assert [r["class"] for r in _rows(gate_env.journal)] == ["PRESENT"]


def test_absent_match_is_held_without_side_effects(gate_env):
    """The reported bug: PuckChamp vs Old blood is not on Winline -> nothing leaves."""
    assert gate_env.deliver("PuckChamp", "Old blood") is False
    assert gate_env.sent == [] and gate_env.alerts == []
    assert gate_env.add_url == [] and gate_env.ledger == []
    assert gate_env.prepared == []              # gate runs before the bookmaker reservation
    assert len(gate_env.blocked) == 1
    decision, kw = gate_env.blocked[0]
    assert decision["reason"] == "winline_line_absent" and kw["reason"] == "winline_line_absent"
    assert decision["cards_n"] == 4 and decision["live_cards_n"] == 3
    assert 0 <= decision["listing_age_s"] < 10
    rows = _rows(gate_env.journal)
    assert [r["class"] for r in rows] == ["ABSENT"]
    assert rows[0]["radiant"] == "PuckChamp" and rows[0]["ml_rule"] == "test_rule"


def test_absent_is_recorded_once_and_cards_parsed_once(gate_env, monkeypatch):
    calls = []
    real = odds_mod.winline_enumerate_live_cards
    monkeypatch.setattr(odds_mod, "winline_enumerate_live_cards",
                        lambda html, *a, **k: calls.append(1) or real(html, *a, **k))
    for _ in range(5):
        assert gate_env.deliver("PuckChamp", "Old blood") is False
    assert len(gate_env.blocked) == 1           # one verdict, not one per tick
    assert len(calls) == 1                      # one parse per snapshot
    assert len(_rows(gate_env.journal)) == 1


def test_absent_kills_market_is_held_too(gate_env):
    assert gate_env.deliver("PuckChamp", "Old blood", market="kills_total") is False
    assert gate_env.sent == []


def test_one_side_name_miss_is_sent_with_alert(gate_env):
    """A real card matches ONE of our teams; the other spelling is beyond the matcher."""
    assert gate_env.deliver("Dark Tamplars", "Hydra Collective") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    assert len(gate_env.alerts) == 1
    alert = gate_env.alerts[0]
    assert "Dark Tamplars" in alert and "Hydra Collective" in alert
    assert "DARK TAMPLARS" in alert and "AZURE DRAGONS" in alert and "Mad Dogs League" in alert
    rows = _rows(gate_env.journal)
    assert rows[0]["class"] == "ONE_SIDE" and rows[0]["card"]["team1"] == "DARK TAMPLARS"
    gate_env.deliver("Dark Tamplars", "Hydra Collective")
    assert len(gate_env.alerts) == 1            # one alert per pair


def test_stale_listing_is_sent(gate_env):
    gate_env.set_listing(fetched_ago=C.WINLINE_OVERVIEW_MAX_AGE_S + 60)
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    assert _rows(gate_env.journal)[0]["reason"] == "listing_stale"


def test_zero_cards_listing_is_sent(gate_env):
    gate_env.set_listing(html="<html><body><div>DOTA 2</div></body></html>")
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert len(gate_env.sent) == 1
    assert _rows(gate_env.journal)[0]["reason"] == "no_cards"


def test_empty_html_is_sent(gate_env):
    gate_env.set_listing(html="", text="x")
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert _rows(gate_env.journal)[0]["reason"] == "listing_empty"


def test_parse_exception_is_sent(gate_env, monkeypatch):
    def boom(*_a, **_kw):
        raise RuntimeError("bs4 exploded")
    monkeypatch.setattr(odds_mod, "winline_enumerate_live_cards", boom)
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert _rows(gate_env.journal)[0]["reason"].startswith("parse_error")


def test_gate_exception_is_fail_open(gate_env, monkeypatch):
    def boom(*_a, **_kw):
        raise RuntimeError("gate bug")
    monkeypatch.setattr(C, "_winline_presence_classify", boom)
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert len(gate_env.sent) == 1


def test_env_rollback_disables_gate(gate_env, monkeypatch):
    monkeypatch.setenv("BET_REQUIRE_WINLINE_LISTING", "0")
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked and not _rows(gate_env.journal)


def test_non_bet_message_is_not_gated(gate_env):
    ok = gate_env.deliver("PuckChamp", "Old blood", ctx=False, text="ℹ️ служебное сообщение\n")
    assert ok is True and len(gate_env.sent) == 1 and not _rows(gate_env.journal)


def test_legacy_bet_without_team_names_is_sent(gate_env, monkeypatch):
    """Header-only bet (no ml_dispatch context): our team names are unknown -> pass."""
    monkeypatch.setenv("DISPATCH_MODE", "star")   # in "ml" mode legacy texts never leave at all
    ok = gate_env.deliver("PuckChamp", "Old blood", ctx=False, text="СТАВКА НА PuckChamp x1\n")
    assert ok is True and len(gate_env.sent) == 1
    assert _rows(gate_env.journal)[0]["reason"] == "team_names_missing"


def test_legacy_bet_with_names_in_details_is_held(gate_env, monkeypatch):
    monkeypatch.setenv("DISPATCH_MODE", "star")
    ok = gate_env.deliver("PuckChamp", "Old blood", ctx=False, text="СТАВКА НА PuckChamp x1\n",
                          details={"radiant_team": "PuckChamp", "dire_team": "Old blood"})
    assert ok is False and gate_env.sent == []


def test_hold_then_release_when_line_appears(gate_env, snap):
    """18:00 no line -> held; the card appears later -> the same bet goes out."""
    for _ in range(3):
        assert gate_env.deliver("PuckChamp", "Old blood") is False
    assert gate_env.sent == []
    later = snap["html"].replace("POTN9KI", "PUCKCHAMP").replace("OVEA", "OLD BLOOD")
    assert later != snap["html"]
    gate_env.set_listing(html=later)
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert len(gate_env.sent) == 1 and len(gate_env.add_url) == 1
    assert [r["class"] for r in _rows(gate_env.journal)] == ["ABSENT", "PRESENT"]


def test_fresh_poller_quote_counts_as_present(gate_env, monkeypatch):
    now = time.monotonic()
    key = "series-x|map1|PuckChamp|Old blood"
    monkeypatch.setattr(C, "_winline_odds_orientation_state", {
        key: {"status": "open", "p1": 1.6, "p2": 2.3, "last_quote_mono": now - 120}})
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert _rows(gate_env.journal)[0]["reason"] == "poller_quote"


def test_old_or_closed_poller_quote_does_not_count(gate_env, monkeypatch):
    now = time.monotonic()
    monkeypatch.setattr(C, "_winline_odds_orientation_state", {
        "s|map1|PuckChamp|Old blood": {"status": "open", "last_quote_mono": now - 1900},
        "s|map2|PuckChamp|Old blood": {"status": "closed", "last_quote_mono": now - 5}})
    assert gate_env.deliver("PuckChamp", "Old blood") is False


# --------------------------------------------------------------------------- review fixes (10.10.2026)

def _drop_card_markup(html: str, event_id: str) -> str:
    """Model a markup change of ONE card: the parser no longer recognises it, text stays."""
    out = html.replace(f'id="eventId-{event_id}"', f'id="evt-{event_id}"')
    assert out != html
    return out


def test_fixture_has_no_headerless_cards(snap):
    """Which cards have an empty league on the captured page: none (hero uses its breadcrumb)."""
    allc = odds_mod.winline_enumerate_live_cards(snap["html"])
    assert len(allc) == 7 and [c["event_id"] for c in allc if not c["league"]] == []


# F1: partial parse miss -------------------------------------------------------------------

def test_dropped_card_with_both_names_in_page_text_is_text_pair_not_absent(gate_env, snap):
    gate_env.set_listing(html=_drop_card_markup(snap["html"], "16900608"))   # OVEA - POTN9KI
    assert "OVEA" in snap["text"] and "POTN9KI" in snap["text"]
    assert gate_env.deliver("OVEA", "POTN9KI") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    rows = _rows(gate_env.journal)
    assert [r["class"] for r in rows] == ["TEXT_PAIR"]
    assert rows[0]["reason"] == "names_in_text_not_in_cards"
    assert len(gate_env.alerts) == 1 and "парсер" in gate_env.alerts[0]
    assert "OVEA" in gate_env.alerts[0] and "POTN9KI" in gate_env.alerts[0]
    gate_env.deliver("OVEA", "POTN9KI", market="kills_total")
    gate_env.deliver("Potn9ki", "Ovea")
    assert len(gate_env.alerts) == 1                    # one silent alert per pair
    assert len(_rows(gate_env.journal)) == 1            # one journal row per (series, map)


def test_dropped_card_and_page_text_spells_name_with_a_space_is_text_pair(gate_env, snap):
    """Round 3 (a): card markup lost AND the text spells `POTN 9KI` while our name is `POTN9KI`."""
    assert "POTN9KI" in snap["text"]
    gate_env.set_listing(html=_drop_card_markup(snap["html"], "16900608"),
                         text=snap["text"].replace("POTN9KI", "POTN 9KI"))
    assert gate_env.deliver("OVEA", "POTN9KI") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    rows = _rows(gate_env.journal)
    assert [r["class"] for r in rows] == ["TEXT_PAIR"]
    assert len(gate_env.alerts) == 1 and "парсер" in gate_env.alerts[0]


def test_dropped_card_with_only_one_name_in_text_is_text_one_side(gate_env, snap):
    """Round 3 (b): one name found in the text is no longer a hold (cards already pass ONE_SIDE)."""
    gate_env.set_listing(html=_drop_card_markup(snap["html"], "16900608"),
                         text=snap["text"].replace("POTN9KI", " "))
    assert gate_env.deliver("OVEA", "POTN9KI") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    rows = _rows(gate_env.journal)
    assert [r["class"] for r in rows] == ["TEXT_ONE_SIDE"]
    assert rows[0]["reason"] == "one_name_in_text_not_in_cards" and rows[0]["text_found"] == "radiant"
    assert len(gate_env.alerts) == 1
    assert "одна команда есть в тексте страницы Winline, но не в карточках — парсер или имя?" \
        in gate_env.alerts[0]
    gate_env.deliver("OVEA", "POTN9KI", market="kills_total")
    gate_env.deliver("Potn9ki", "Ovea")
    assert len(gate_env.alerts) == 1                    # one silent alert per pair
    assert len(_rows(gate_env.journal)) == 1            # one journal row per (series, map)


LONG_NAME = "Team Bright Future Esports Club Five"            # 6 tokens: beyond any fixed join window


def test_long_name_in_text_is_not_held(gate_env, snap):
    """A name of 6 words is found by the exact token-aligned search, so a dropped card + the
    name in the text never ends ABSENT."""
    gate_env.set_listing(html=_drop_card_markup(snap["html"], "16900608"),
                         text=snap["text"].replace("POTN9KI", " ").replace("OVEA", " ")
                         + " X " + LONG_NAME.upper() + " Y")
    assert gate_env.deliver(LONG_NAME, "Zork Quxx") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    rows = _rows(gate_env.journal)
    assert [r["class"] for r in rows] == ["TEXT_ONE_SIDE"] and rows[0]["text_found"] == "radiant"
    assert len(gate_env.alerts) == 1


def test_long_name_pair_in_text_is_text_pair(gate_env, snap):
    gate_env.set_listing(html=_drop_card_markup(snap["html"], "16900608"),
                         text=snap["text"].replace("POTN9KI", " ").replace("OVEA", " ")
                         + " X " + LONG_NAME.upper() + " Y " + "-".join(["QWERTYUIOPASDFGHJKL"] * 3)
                         .replace("-", " ").upper() + " Z")
    assert gate_env.deliver(LONG_NAME, " ".join(["Qwertyuiopasdfghjkl"] * 3)) is True
    assert [r["class"] for r in _rows(gate_env.journal)] == ["TEXT_PAIR"]


def test_dropped_card_and_no_name_in_text_stays_absent(gate_env, snap):
    gate_env.set_listing(html=_drop_card_markup(snap["html"], "16900608"),
                         text=snap["text"].replace("POTN9KI", " ").replace("OVEA", " "))
    assert gate_env.deliver("OVEA", "POTN9KI") is False
    assert gate_env.sent == [] and gate_env.alerts == []
    rows = _rows(gate_env.journal)
    assert [r["class"] for r in rows] == ["ABSENT"] and rows[0]["live_cards"] is not None


@pytest.mark.parametrize("extra", [
    "OLDBLOODLINE",                 # our name inside a longer token
    "COLD BLOODED OLDBLOODS",        # ... or inside other words
    "X OLD Y BLOOD Z",              # words not adjacent
])
def test_inside_token_text_hits_do_not_release_the_hold(gate_env, snap, extra):
    """Round 3 (d) at the delivery boundary: the owner's case stays held."""
    gate_env.set_listing(text=snap["text"] + " " + extra)
    assert gate_env.deliver("PuckChamp", "Old blood") is False
    assert gate_env.sent == []
    assert [r["class"] for r in _rows(gate_env.journal)] == ["ABSENT"]


def test_generic_only_name_in_text_never_counts(gate_env, snap):
    """Round 3 (e): `Team Gaming` is only wrapper words; its spaceless join must not match."""
    gate_env.set_listing(text=snap["text"] + " TEAM GAMING TEAMGAMING GAMING")
    assert gate_env.deliver("Team Gaming", "PuckChamp") is False
    assert gate_env.sent == []
    assert [r["class"] for r in _rows(gate_env.journal)] == ["ABSENT"]


def test_text_search_ignores_spacing_but_keeps_word_bounds():
    hits = C._winline_presence_text_hits
    assert hits("X POTN 9KI Y OVEA", 11.0, "Ovea", "Potn9ki") == (2, "")
    assert hits("X OLD BLOOD Y", 12.0, "Oldblood", "Zork Qux") == (1, "radiant")
    assert hits("X OLDBLOOD Y", 13.0, "Old blood", "Zork Qux") == (1, "radiant")
    assert hits("X OLDBLOODLINE Y", 14.0, "Old blood", "Zork Qux") == (0, "")
    assert hits("X FOLDBLOODY Y", 15.0, "Old blood", "Zork Qux") == (0, "")
    assert hits("X TEAM GAMING Y", 16.0, "Team Gaming", "Zork Qux") == (0, "")
    assert hits("X Y", 17.0, "Old blood", "Zork Qux") == (0, "")
    assert hits("X ZORK QUX Y", 18.0, "Old blood", "Zork Qux") == (1, "dire")
    assert hits("", 19.0, "Old blood", "Zork Qux") == (0, "")


def test_owner_case_stays_absent_on_the_real_page_text(gate_env, snap):
    assert "puckchamp" not in snap["text"].lower() and "old blood" not in snap["text"].lower()
    assert gate_env.deliver("PuckChamp", "Old blood") is False
    assert [r["class"] for r in _rows(gate_env.journal)] == ["ABSENT"]


@pytest.mark.parametrize("text,ours,theirs", [
    ("TEAM GAMING ESPORTS CLUB THE GG TEAM", "Team Quux", "Gaming Zork"),   # generic words alone
    ("COLD BLOODED NOVEAU", "Old blood", "OVEA"),                             # inside other words
    ("X 1 Y A B", "A", "1"),                                                  # forms shorter than 2
])
def test_text_pair_search_is_not_loose(text, ours, theirs):
    assert C._winline_presence_text_pair(text, 1.0, ours, theirs) is False


def test_text_pair_search_uses_aliases_and_word_bounds():
    page = "DOTA 2 | X NAVI PSG.LGD 1.5 2.5"
    assert C._winline_presence_text_pair(page, 2.0, "Natus Vincere", "LGD Gaming") is True
    assert C._winline_presence_text_pair(page, 3.0, "Natus Vincere", "LGD Gaming Academy") is False


# F2: overview status ------------------------------------------------------------------------

@pytest.mark.parametrize("status,error", [("partial_load", "TimeoutError"), ("empty", ""),
                                          ("ok", "page.goto: net::ERR_PROXY")])
def test_partial_or_errored_overview_is_unknown(gate_env, status, error):
    gate_env.set_listing(status=status, error=error)
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    assert _rows(gate_env.journal)[0]["reason"] == "listing_partial"


# F3: other sports in the general feed ---------------------------------------------------------

def test_listing_without_dota_cards_is_unknown(gate_env, snap):
    """General feed fallback: no Dota 2 section/breadcrumb -> no evidence of a healthy Dota listing."""
    other = snap["html"].replace("DOTA 2 |", "COUNTER-STRIKE |").replace("DOTA 2,", "COUNTER-STRIKE,")
    assert other.count("DOTA 2 |") == 0 and other != snap["html"]
    gate_env.set_listing(html=other)
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    row = _rows(gate_env.journal)[0]
    assert row["class"] == "UNKNOWN" and row["reason"] == "no_dota_cards"


def test_dota_hero_card_alone_still_counts_as_dota_evidence(gate_env, snap):
    """Only the hero card keeps its Dota breadcrumb: that is still Dota evidence -> ABSENT stays."""
    partial = snap["html"].replace("DOTA 2 |", "COUNTER-STRIKE |")
    gate_env.set_listing(html=partial)
    assert gate_env.deliver("PuckChamp", "Old blood") is False
    row = _rows(gate_env.journal)[0]
    assert row["class"] == "ABSENT" and row["dota_cards_n"] == 1


# F4: storage cap ------------------------------------------------------------------------------

def _padded(html: str, size: int) -> str:
    pad = size - len(html) - len("<!---->")
    assert pad > 0
    return html + "<!--" + "x" * pad + "-->"


def test_html_at_store_cap_is_unknown_truncated(gate_env, snap):
    gate_env.set_listing(html=_padded(snap["html"], 1_500_000))
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    assert _rows(gate_env.journal)[0]["reason"] == "listing_truncated"


def test_html_just_below_store_cap_is_still_judged(gate_env, snap):
    gate_env.set_listing(html=_padded(snap["html"], 1_499_999))
    assert gate_env.deliver("PuckChamp", "Old blood") is False


# F6: alert dedup state is evicted, never wholesale cleared ---------------------------------------

def test_evict_oldest_half_keeps_newest_and_drops_pending_before_delivered():
    state = {i: (99 if i in (0, 1) else 1) for i in range(10)}
    C._winline_presence_evict_oldest_half(state, 99)
    assert len(state) == 5 and 0 in state and 1 in state            # delivered survive
    assert sorted(set(state) - {0, 1}) == [7, 8, 9]                  # newest pending survive
    plain = {i: "c" for i in range(10)}
    C._winline_presence_evict_oldest_half(plain)
    assert list(plain) == [5, 6, 7, 8, 9]


def test_delivered_alert_pair_is_not_realerted_after_state_limit(gate_env, monkeypatch):
    monkeypatch.setattr(C, "_WINLINE_PRESENCE_STATE_LIMIT", 6)
    assert gate_env.deliver("Dark Tamplars", "Hydra Collective") is True
    assert len(gate_env.alerts) == 1
    # churn of other pairs whose Telegram delivery FAILS (pending, not delivered)
    ok_sender = C.send_winline_odds_message
    monkeypatch.setattr(C, "send_winline_odds_message", lambda text, **_kw: False)
    for i in range(12):
        info = {"radiant": f"Team Foo{i}", "dire": f"Team Bar{i}",
                "card": {"team1": "A", "team2": "B", "league": "L", "live": True}}
        C._winline_presence_alert_one_side(info, 1)
    assert len(C._winline_presence_alert_attempts) <= 6
    monkeypatch.setattr(C, "send_winline_odds_message", ok_sender)
    gate_env.deliver("Dark Tamplars", "Hydra Collective")
    assert len(gate_env.alerts) == 1                                 # no second alert for the pair


def test_last_class_state_is_evicted_by_half_not_cleared(gate_env, monkeypatch):
    monkeypatch.setattr(C, "_WINLINE_PRESENCE_STATE_LIMIT", 4)
    for i in range(4):
        C._winline_presence_note_class(f"dltv.org/matches/{i}.1", 1, {"class": "PRESENT"}, {})
    assert len(C._winline_presence_last_class) == 4
    C._winline_presence_note_class("dltv.org/matches/new.1", 1, {"class": "ABSENT"}, {})
    assert 2 < len(C._winline_presence_last_class) <= 4              # half dropped, rest kept


# F7: gate exception logging is rate-limited -----------------------------------------------------

def test_gate_exception_log_is_rate_limited_per_match(gate_env, monkeypatch, caplog):
    def boom(*_a, **_kw):
        raise RuntimeError("gate bug")
    monkeypatch.setattr(C, "_winline_presence_classify", boom)
    with caplog.at_level("ERROR"):
        for _ in range(6):
            assert gate_env.deliver("PuckChamp", "Old blood") is True   # still fail-open
        gate_logs = [r for r in caplog.records if "presence gate failed" in r.getMessage()]
        assert len(gate_logs) == 1
        for key in list(C._winline_presence_exc_last_log):
            C._winline_presence_exc_last_log[key] -= 601.0              # 600 s have passed
        gate_env.deliver("PuckChamp", "Old blood")
        assert len([r for r in caplog.records if "presence gate failed" in r.getMessage()]) == 2
    assert len(gate_env.sent) == 7


# --------------------------------------------------------------------------- round 4 (10.10.2026)

SPACED = "Alpha Beta Gamma Delta Epsilon Zeta"                 # 6 words
GLUED = "AlphaBeta Gamma Delta Epsilon Zeta"                   # 5 words, same letters


def _text_without_our_pair(snap, extra):
    return snap["text"].replace("POTN9KI", " ").replace("OVEA", " ") + " X " + extra + " Y"


def test_r4_text_glues_two_words_of_our_long_name_is_text_one_side(gate_env, snap):
    """R4-1 (a): the page writes `AlphaBeta Gamma ...`, we say `Alpha Beta Gamma ...` (>4 words)."""
    gate_env.set_listing(html=_drop_card_markup(snap["html"], "16900608"),
                         text=_text_without_our_pair(snap, GLUED.upper()))
    assert gate_env.deliver(SPACED, "Zork Quxx") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    rows = _rows(gate_env.journal)
    assert [r["class"] for r in rows] == ["TEXT_ONE_SIDE"] and rows[0]["text_found"] == "radiant"


def test_r4_text_splits_a_word_of_our_long_name_is_text_one_side(gate_env, snap):
    """R4-1 (a), reverse spacing: the page says `ALPHA BETA ...`, we say `AlphaBeta ...`."""
    gate_env.set_listing(html=_drop_card_markup(snap["html"], "16900608"),
                         text=_text_without_our_pair(snap, SPACED.upper()))
    assert gate_env.deliver(GLUED, "Zork Quxx") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    assert [r["class"] for r in _rows(gate_env.journal)] == ["TEXT_ONE_SIDE"]


def test_r4_long_names_with_different_spacing_on_both_sides_are_text_pair(gate_env, snap):
    other_glued = "QwertyuiopAsdf Ghjkl Zxcvb Nmqwer Tyuiop"
    other_spaced = "Qwertyuiop Asdf Ghjkl Zxcvb Nmqwer Tyuiop"
    gate_env.set_listing(html=_drop_card_markup(snap["html"], "16900608"),
                         text=_text_without_our_pair(snap, GLUED.upper() + " VS " + other_spaced.upper()))
    assert gate_env.deliver(SPACED, other_glued) is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    assert [r["class"] for r in _rows(gate_env.journal)] == ["TEXT_PAIR"]


def test_r4_long_name_inside_longer_tokens_stays_absent(gate_env, snap):
    """The exact search is still token-aligned: the same letters inside longer words are no hit."""
    gate_env.set_listing(html=_drop_card_markup(snap["html"], "16900608"),
                         text=_text_without_our_pair(snap, "X" + GLUED.upper() + "S"))
    assert gate_env.deliver(SPACED, "Zork Quxx") is False
    assert [r["class"] for r in _rows(gate_env.journal)] == ["ABSENT"]


@pytest.mark.parametrize("text,ours,expected", [
    ("X ABC Y", "Ab", (0, "")),                        # `ab` is a prefix of the token `abc`
    ("X CAB Y", "Ab", (0, "")),                        # ... or a suffix of another token
    ("X 1WI Y", "1W", (0, "")),                        # `1w` inside `1wi` (the alias `1win` is exact only)
    ("X 1WIN Y", "1W", (1, "radiant")),                # alias `1win` as a whole token
    ("X OLDBLOODLINE Y", "Old blood", (0, "")),        # ends inside a token
    ("X BOLD BLOOD Y", "Old blood", (0, "")),          # starts inside a token
    ("X OLD BLOODY Y", "Old blood", (0, "")),          # last word continues
    ("X FOLDBLOOD Z OLD BLOOD", "Old blood", (1, "radiant")),   # 1st occurrence unaligned, 2nd aligned
    ("X OLD BLOOD Z", "Oldblood", (1, "radiant")),
    ("X ALPHA BETA GAMMA DELTA EPSILON ZETA Y", GLUED, (1, "radiant")),
    ("X ALPHABETA GAMMA DELTA EPSILON ZETA Y", SPACED, (1, "radiant")),
])
def test_r4_exact_token_aligned_text_search(text, ours, expected):
    assert C._winline_presence_text_hits(text, 21.0, ours, "Zork Qux") == expected


def test_r4_one_occurrence_does_not_count_for_both_teams():
    assert C._winline_presence_text_hits("X OLD BLOOD Y", 22.0, "Old blood", "Oldblood") == (1, "radiant")


def _drive_overview_refresh(monkeypatch, snap, *, mode):
    """Run the REAL refresh attempt (what the overview loop calls); only the browser job is mocked."""
    persisted = []
    monkeypatch.setattr(C, "_winline_first_parser_fns",
                        lambda: (odds_mod.collect_winline_live_overview_raw, None, None, None))
    monkeypatch.setattr(C, "_bookmaker_urls_for_mode", lambda _m: {"winline": "https://example.invalid/live"})
    monkeypatch.setattr(C, "_winline_overview_persist_snapshot", lambda t, h: persisted.append(1) or True)

    def job(_name, _fn, timeout=None):
        if mode == "raise":
            raise RuntimeError("camoufox down")
        if mode == "non_dict":
            return None
        if mode == "empty_text":
            return {"text": "", "html": snap["html"], "status": "ok"}
        if mode == "shell":
            return {"text": "DOTA 2 loading", "html": "<html></html>", "status": "ok"}
        return {"text": snap["text"], "html": snap["html"], "status": "ok"}

    monkeypatch.setattr(C, "_run_shared_camoufox_job", job)
    return C._winline_overview_refresh_attempt(), persisted


def test_r4_refresh_failure_makes_the_gate_unknown_then_a_store_restores_it(gate_env, snap, monkeypatch):
    """R4-2 (b): fresh snapshot, then a failed refresh -> the owner case is sent (UNKNOWN), and
    after the next successful store the same pair is ABSENT again."""
    assert gate_env.deliver("PuckChamp", "Old blood") is False           # healthy listing: held
    ok, persisted = _drive_overview_refresh(monkeypatch, snap, mode="raise")
    assert ok is False and persisted == []
    assert C._winline_overview_state["refresh_failed_at"] >= C._winline_overview_state["fetched_at"]
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked[1:]
    last = _rows(gate_env.journal)[-1]
    assert last["class"] == "UNKNOWN" and last["reason"] == "listing_refresh_failed"
    ok, persisted = _drive_overview_refresh(monkeypatch, snap, mode="store")
    assert ok is True and persisted == [1]
    assert C._winline_overview_state["refresh_failed_at"] == 0.0
    sent_before = len(gate_env.sent)
    assert gate_env.deliver("PuckChamp", "Old blood") is False
    assert len(gate_env.sent) == sent_before
    assert _rows(gate_env.journal)[-1]["class"] == "ABSENT"


@pytest.mark.parametrize("mode", ["raise", "non_dict", "empty_text", "shell"])
def test_r4_every_failed_refresh_shape_is_recorded(gate_env, snap, monkeypatch, mode):
    ok, persisted = _drive_overview_refresh(monkeypatch, snap, mode=mode)
    assert ok is False and persisted == []
    assert C._winline_overview_state["refresh_failed_at"] > C._winline_overview_state["fetched_at"]
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert _rows(gate_env.journal)[-1]["reason"] == "listing_refresh_failed"


def test_r4_exception_out_of_refresh_once_is_recorded_and_not_raised(gate_env, monkeypatch):
    def boom():
        raise ValueError("unexpected")
    monkeypatch.setattr(C, "_winline_overview_refresh_once", boom)
    assert C._winline_overview_refresh_attempt() is False
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert _rows(gate_env.journal)[-1]["reason"] == "listing_refresh_failed"


def test_r4_failure_older_than_the_snapshot_does_not_matter(gate_env):
    state = C._winline_overview_state
    state["refresh_failed_at"] = state["fetched_at"] - 5.0
    assert gate_env.deliver("PuckChamp", "Old blood") is False
    assert _rows(gate_env.journal)[-1]["class"] == "ABSENT"


# --- Loop wiring of the refresh-failure marker (card ingame-fioo) -------------------------------
# Round-4 hard-verifier mutant: `_winline_overview_loop` calling `_winline_overview_refresh_once`
# directly (no `refresh_failed_at` marker) kept every test above green, although the gate's
# `listing_refresh_failed` (the bet passes) depends on that call.  These tests run ONE real loop
# iteration (time.sleep raises to leave `while True`) over the captured snapshot; only the browser
# fetch (`_winline_overview_refresh_once`) and the snapshot sweep are replaced.

class _LeaveLoop(BaseException):
    pass


def _one_loop_iteration(monkeypatch, refresh_once, *, age_s=60.0):
    swept = []
    monkeypatch.setattr(C, "_winline_overview_refresh_once", refresh_once)
    monkeypatch.setattr(C, "_winline_sweep_cards_from_snapshot", lambda: swept.append(1))
    monkeypatch.setattr(C, "WINLINE_OVERVIEW_TTL_S", 45.0)
    state = C._winline_overview_state
    state["fetched_at"] = time.time() - age_s        # due (>= TTL 45 s) but not stale (< 300 s)
    state["next_retry_at"] = 0.0

    def leave(_seconds):
        raise _LeaveLoop()

    monkeypatch.setattr(C.time, "sleep", leave)
    started = time.time()
    with pytest.raises(_LeaveLoop):
        C._winline_overview_loop()
    return started, swept


def test_fioo_failed_refresh_in_the_loop_marks_the_listing_and_the_bet_passes(gate_env, monkeypatch):
    assert gate_env.deliver("PuckChamp", "Old blood") is False            # healthy listing: held
    started, swept = _one_loop_iteration(monkeypatch, lambda: False)
    state = C._winline_overview_state
    assert state["refresh_failed_at"] >= started > state["fetched_at"]
    assert state["next_retry_at"] > started                               # backoff armed
    assert swept == []
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    last = _rows(gate_env.journal)[-1]
    assert last["class"] == "UNKNOWN" and last["reason"] == "listing_refresh_failed"


def test_fioo_refresh_raising_inside_the_loop_marks_the_listing(gate_env, monkeypatch):
    def boom():
        raise RuntimeError("camoufox driver died")
    started, _ = _one_loop_iteration(monkeypatch, boom)
    assert C._winline_overview_state["refresh_failed_at"] >= started
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert _rows(gate_env.journal)[-1]["reason"] == "listing_refresh_failed"


def test_fioo_successful_refresh_in_the_loop_leaves_the_hold(gate_env, monkeypatch):
    _, swept = _one_loop_iteration(monkeypatch, lambda: True)
    assert C._winline_overview_state["refresh_failed_at"] == 0.0
    assert swept == [1]
    assert gate_env.deliver("PuckChamp", "Old blood") is False
    assert _rows(gate_env.journal)[-1]["class"] == "ABSENT"


# Suite pollution, 10.10.2026: a listing left in the module global by an earlier test file met
# a fake clock, the listing age came out at -91.7M s, passed the `> max age` check and was
# judged healthy -> ABSENT -> the bet was held.  A listing from the future is uncertain.
def test_listing_from_the_future_is_sent_as_clock_skew(gate_env):
    gate_env.set_listing(fetched_ago=-3600.0)
    assert gate_env.deliver("PuckChamp", "Old blood") is True
    assert len(gate_env.sent) == 1 and not gate_env.blocked
    last = _rows(gate_env.journal)[-1]
    assert last["class"] == "UNKNOWN" and last["reason"] == "listing_clock_skew"
    assert last["listing_age_s"] < -3000


def test_small_negative_listing_age_is_still_judged(gate_env):
    """Seconds of skew between the fetch thread and the gate must not disable the hold."""
    gate_env.set_listing(fetched_ago=-5.0)
    assert gate_env.deliver("PuckChamp", "Old blood") is False
    assert _rows(gate_env.journal)[-1]["class"] == "ABSENT"
