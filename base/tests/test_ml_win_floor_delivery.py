"""ML WIN price floor at the final Telegram delivery boundary."""
from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from base import ml_dispatch as md
from base import cyberscore_try as C


FIXTURE = Path(__file__).parent / "fixtures" / "winline_yangon_yache_map3_20260915.json"


@pytest.fixture
def quote(monkeypatch):
    first = json.loads(FIXTURE.read_text())["messages"][0]
    key = first["canonical_key"]
    p1, p2 = first["observation"]["output_pair"]
    state = {key: {"p1": p1, "p2": p2, "status": "open", "last_quote_mono": time.monotonic()}}
    monkeypatch.setattr(C, "_winline_odds_orientation_state", state)
    return state, key


@pytest.fixture
def delivery(monkeypatch):
    sent, blocked = [], []
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(C, "_bookmaker_prepare_message_for_delivery",
                        lambda _key, text, **_kw: (text, True, "disabled", None))
    for name in ("_half_stake_elo_underdog_reject_for_delivery",
                 "_win_model_reject_for_delivery", "_late_win_model_reject_for_delivery"):
        monkeypatch.setattr(C, name, lambda *_a, **_kw: None)
    monkeypatch.setattr(C, "_record_delivery_gate_block",
                        lambda _key, _text, decision, **_kw: blocked.append(decision))
    monkeypatch.setattr(C, "_signal_fingerprint_try_reserve", lambda *_a: (True, "test-fp"))
    monkeypatch.setattr(C, "_signal_fingerprint_mark_sent", lambda *_a: None)
    monkeypatch.setattr(C, "_record_bet_dispatch_ledger", lambda *_a, **_kw: None)
    monkeypatch.setattr(C, "decelerate_winline_current_map_polling", lambda *_a: None)
    monkeypatch.setattr(C, "add_url", lambda *_a, **_kw: None)
    monkeypatch.setattr(C, "send_message", lambda text, **_kw: sent.append(text))

    def deliver(*, market="win", target="YANGON GALACTICOS", map_num=3,
                game_time=600, text=None):
        floor = md._min_odds(0.74, md.Config.from_env({}))
        message = text or f"СТАВКА НА {target} x1\nСтавить от кэфа {floor:.2f}\n"
        ctx = {"origin": "ml_dispatch", "ml_market": market,
               "stake_team_name": target,
               "radiant_team_name": "YANGON GALACTICOS",
               "dire_team_name": "YACHE123", "game_time_seconds": game_time,
               "calibration": {"expected_wr": 0.74, "min_odds": floor}}
        result = C._deliver_and_persist_signal(
            "dltv.org/matches/yangon-yache.3", message,
            add_url_reason="ml_dispatch", map_num=map_num,
            selected_side="radiant", stake_multiplier_context=ctx)
        return result, sent, blocked

    return deliver


def test_floor_line_reaches_outgoing_win_payload(monkeypatch):
    outgoing = []
    monkeypatch.setattr(C, "_deliver_and_persist_signal",
                        lambda _key, text, **_kw: outgoing.append(text) or True)
    decision = SimpleNamespace(market="win", target_side="Radiant",
        target_team="YANGON GALACTICOS", rule="test", reasons=[], expected_wr=0.74,
        min_odds=md._min_odds(0.74, md.Config.from_env({})))
    ledger = SimpleNamespace(add=lambda _key: None, save=lambda: None)
    C._ml_dispatch_deliver_decision(
        decision, match_key="dltv.org/matches/yangon-yache.3", base_url="series",
        ctx_map_num=3, resolved_map_num=3, radiant_team_name="YANGON GALACTICOS",
        dire_team_name="YACHE123", live_league={}, top="", mid="", bot="",
        protracker_payload=None, team_elo_block="", game_time_seconds=600,
        radiant_lead=0, early_output=None, mid_output=None, all_output=None,
        radiant_heroes_and_pos=None, dire_heroes_and_pos=None,
        full_message_text="СТАВКА НА YANGON GALACTICOS x1\nYANGON GALACTICOS VS YACHE123",
        ml_laning_line="", all_model_line="", ledger=ledger)
    assert outgoing and outgoing[0].splitlines()[1] == "Ставить от кэфа 1.61"


def test_fresh_below_floor_blocks_at_delivery(quote, delivery):
    result, sent, blocked = delivery()
    assert result is False and sent == []
    assert blocked[-1]["reason"] == "ml_min_odds_below_floor"
    assert blocked[-1]["price"] == 1.45
    assert blocked[-1]["price_source"] == "winline_poll"


def test_captured_poller_observation_then_closed_market(quote, delivery):
    state, key = quote
    state.clear()
    observed = json.loads(FIXTURE.read_text())["first_attempt"]
    oriented = C._winline_stabilize_odds_orientation(dict(observed), key)
    C._winline_record_quote_observation(key, oriented)
    assert delivery()[0] is False
    C._winline_record_quote_observation(key, {"market_status": "closed"})
    result, sent, blocked = delivery()
    assert result is True and len(sent) == 1 and len(blocked) == 1


@pytest.mark.parametrize("invalid", [
    {"market_status": "open", "page_valid": False, "p1_odds": 1.45, "p2_odds": 2.5},
    {"market_status": "open", "odds_bettable": False, "p1_odds": 1.45, "p2_odds": 2.5},
    {"market_status": "open"},
])
def test_unusable_poller_observation_invalidates_previous_quote(quote, delivery, invalid):
    state, key = quote
    C._winline_record_quote_observation(key, invalid)
    result, sent, blocked = delivery()
    assert result is True and len(sent) == 1 and not blocked


def test_fresh_above_floor_delivers(quote, delivery):
    result, sent, blocked = delivery(target="YACHE123")
    assert result is True and len(sent) == 1 and not blocked


@pytest.mark.parametrize("age,game_time", [(301, 600), (1801, 0)])
def test_stale_quote_delivers_with_floor(quote, delivery, age, game_time):
    state, key = quote
    state[key]["last_quote_mono"] = time.monotonic() - age
    result, sent, blocked = delivery(game_time=game_time)
    assert result is True and "Ставить от кэфа 1.61" in sent[0] and not blocked


def test_pre_horn_quote_can_block_after_five_minutes(quote, delivery):
    state, key = quote
    state[key]["last_quote_mono"] = time.monotonic() - 1200
    result, sent, blocked = delivery(game_time=0)
    assert result is False and sent == [] and blocked[-1]["price"] == 1.45


def test_missing_quote_delivers_with_floor(quote, delivery):
    quote[0].clear()
    result, sent, blocked = delivery()
    assert result is True and "Ставить от кэфа 1.61" in sent[0] and not blocked


def test_other_map_quote_does_not_block(quote, delivery):
    state, key = quote
    result, sent, blocked = delivery(map_num=2)
    assert result is True and len(sent) == 1 and not blocked


@pytest.mark.parametrize("leftover", [
    {"status": "open", "age": 7200},      # earlier series, never pruned
    {"status": "closed", "age": 10},      # retired card-sweep poller
])
def test_leftover_same_map_pair_entry_does_not_disable_floor(quote, delivery, leftover):
    # serv1 journal: 148 of 564 map slots carry >1 series key for one pair.
    state, key = quote
    state["sourcetv:league:19944|map3|YANGON GALACTICOS|YACHE123"] = {
        **state[key], "p1": 2.9, "status": leftover["status"],
        "last_quote_mono": time.monotonic() - leftover["age"]}
    result, sent, blocked = delivery()
    assert result is False and sent == [] and blocked[-1]["price"] == 1.45


def test_two_fresh_quotes_block_only_if_all_below_floor(quote, delivery):
    state, key = quote
    other = "sourcetv:league:19944|map3|YANGON GALACTICOS|YACHE123"
    state[other] = dict(state[key])
    assert delivery()[0] is False
    state[other]["p1"] = 1.70
    result, sent, blocked = delivery()
    assert result is True and len(sent) == 1


def test_swapped_quote_order_prices_target_team(quote, delivery):
    state, key = quote
    entry = state.pop(key)
    state[key.rsplit("|", 2)[0] + "|YACHE123|YANGON GALACTICOS"] = {
        **entry, "p1": entry["p2"], "p2": entry["p1"]}
    result, sent, blocked = delivery()
    assert result is False and sent == [] and blocked[-1]["price"] == 1.45


@pytest.mark.parametrize("status", ["closed", "terminal", "stopped"])
def test_closed_market_quote_does_not_block(quote, delivery, status):
    state, key = quote
    state[key]["status"] = status
    result, sent, blocked = delivery()
    assert result is True and len(sent) == 1 and not blocked


@pytest.mark.parametrize("market", ["kills_window", "kills_total"])
def test_kills_markets_ignore_win_floor(quote, delivery, market):
    result, sent, blocked = delivery(market=market)
    assert result is True and len(sent) == 1 and not blocked
