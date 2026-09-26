"""Owner's 26.09 lane + early-star kills-window rule, using captured serv1 ticks."""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import sys

import pytest

from base import ml_dispatch as md
from base import laning_serving

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Isolated verification trees exclude the ignored production credentials
# (base/keys.py); same ImportError-only stub as
# base/tests/test_shared_camoufox_wedge_recovery.py. A real keys.py wins.
try:
    import keys  # noqa: F401,E402
except ImportError:
    import types

    _test_keys = types.ModuleType("keys")
    _test_keys.api_to_proxy = {}
    _test_keys.BOOKMAKER_PROXY_URL = None
    _test_keys.BOOKMAKER_PROXY_POOL = []
    _test_keys.DLTV_PROXY_POOL = []
    sys.modules["keys"] = _test_keys

import cyberscore_try as C  # noqa: E402


CAPTURES = {
    row["case"]: row
    for row in (
        json.loads(line)
        for line in (Path(__file__).parent / "fixtures" / "ml_dispatch_lane_kills_20260926.jsonl").read_text().splitlines()
    )
}


def _ctx(case):
    record = CAPTURES[case]["record"]
    verdicts = record["verdicts"]

    def verdict(name):
        value = verdicts.get(name)
        return md.ModelVerdict(**value) if value else None

    kills30 = verdicts.get("kills30") or {}
    return md.Ctx(
        match_key=record["match_key"], base_url=record["base_url"],
        map_num=record["map_num"], game_time=record["game_time"],
        radiant_team=record["teams"]["radiant"], dire_team=record["teams"]["dire"],
        heroes=record["heroes"], elo_radiant=record["elo_r"], elo_dire=record["elo_d"],
        early_nw=verdict("early_nw"), early_win=verdict("early_win"),
        late=verdict("late"), all=verdict("all"), lane=verdict("lane"),
        kills_windows_open=C._ml_dispatch_open_kills_windows(record["game_time"]),
        kills30_radiant=kills30.get("radiant"), kills30_dire=kills30.get("dire"),
        already_sent=set(),
    )


def _windows(result):
    return [decision for decision in result.decisions if decision.market == "kills_window"]


def test_both_early_star_fires_for_lane_side_with_delivery_fields():
    ctx = _ctx("fires_both_early_star")
    result = md.evaluate(ctx, md.Config.from_env())
    windows = _windows(result)
    assert len(windows) == 1
    decision = windows[0]
    assert (decision.target_side, decision.target_team, decision.rule) == (
        "Radiant", "GamerLegion", "kills_lane_early_window")
    assert decision.models_for == ["lane", "early_nw", "early_win"]
    assert decision.models_against == []
    assert decision.timing == "now"
    assert decision.expected_wr == pytest.approx(0.8076)
    assert decision.min_odds == md._min_odds(decision.expected_wr, md.Config.from_env())
    assert len(decision.reasons) == 5
    assert decision.reasons[-1] == "window=5_15"
    assert all(not (s.market == "kills_window" and s.side in (None, "Radiant"))
               for s in result.skipped)
    assert md.evaluate(ctx, md.Config.from_env()) == result


def test_one_early_star_and_weak_opposite_fires():
    ctx = _ctx("fires_one_early_star_other_early_weak_opposite")
    # The captured Dire side is also the ELO underdog. Isolate the new rule
    # from the pre-existing underdog path while retaining every model verdict.
    cfg = replace(md.Config.from_env(), underdog_min_diff=1000.0)
    windows = _windows(md.evaluate(ctx, cfg))
    assert len(windows) == 1
    assert (windows[0].target_side, windows[0].target_team, windows[0].rule) == (
        "Dire", "VooDooSh Club", "kills_lane_early_window")
    assert windows[0].models_for == ["lane", "early_nw"]
    assert windows[0].reasons[-1] == "window=5_15"


def test_overlap_with_underdog_rule_produces_one_window():
    windows = _windows(md.evaluate(_ctx("fires_overlap_underdog_kw_same_side"), md.Config.from_env()))
    assert len(windows) == 1
    assert windows[0].target_side == "Dire"


@pytest.mark.parametrize("case", [
    "fires_but_5_15_closed", "no_fire_early_star_against",
    "no_fire_no_early_star", "no_lane_star_early_both_star",
])
def test_capture_does_not_fire_new_rule(case):
    result = md.evaluate(_ctx(case), md.Config.from_env())
    assert not any(d.rule == "kills_lane_early_window" for d in result.decisions)


def test_closed_5_15_records_skip_only_after_model_condition():
    result = md.evaluate(_ctx("fires_but_5_15_closed"), md.Config.from_env())
    assert any(s.market == "kills_window" and s.side == "Radiant"
               and s.reason == "lane_kills_window_closed" for s in result.skipped)
    assert not any(s.reason == "lane_kills_window_closed" for s in
                   md.evaluate(_ctx("no_fire_early_star_against"), md.Config.from_env()).skipped)


def test_configured_window_labels_follow_upstream_open_order(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_LANE_KILLS_WINDOWS", "10_20,5_15")
    result = md.evaluate(_ctx("fires_both_early_star"), md.Config.from_env())
    assert _windows(result)[0].reasons[-1] == "window=5_15"
    monkeypatch.setenv("ML_DISPATCH_LANE_KILLS_WINDOWS", "10_20")
    result = md.evaluate(_ctx("fires_both_early_star"), md.Config.from_env())
    assert _windows(result)[0].reasons[-1] == "window=10_20"


@pytest.mark.parametrize("value", ["0", "false", "off"])
def test_toggle_restores_old_behavior(monkeypatch, value):
    monkeypatch.setenv("ML_DISPATCH_LANE_KILLS", value)
    result = md.evaluate(_ctx("fires_both_early_star"), md.Config.from_env())
    assert _windows(result) == []


@pytest.mark.parametrize("sent_side,reason", [
    ("Radiant", md.REASON_DEDUP), ("Dire", "kills_window_sent_other_side"),
])
def test_persistent_dedup_both_sides(sent_side, reason):
    ctx = _ctx("fires_both_early_star")
    ctx.already_sent.add(md._dedup_key(ctx, "kills_window", sent_side))
    result = md.evaluate(ctx, md.Config.from_env())
    assert _windows(result) == []
    assert any(s.market == "kills_window" and s.side == "Radiant" and s.reason == reason
               for s in result.skipped)


def test_ml_tick_delivers_5_15_header_from_real_formatter(monkeypatch):
    record = CAPTURES["fires_both_early_star"]["record"]
    verdicts = record["verdicts"]
    delivered = []
    logged = []

    class Ledger:
        def __init__(self):
            self.keys = set()

        def as_set(self):
            return set(self.keys)

        def add(self, key):
            self.keys.add(tuple(key))

        def save(self):
            pass

    monkeypatch.setenv("DISPATCH_MODE", "ml")
    monkeypatch.setattr(C, "_ml_dispatch_extract_index_details",
                        lambda *blocks: (record["prematch_index"], verdicts))
    monkeypatch.setattr(C, "_ml_dispatch_prematch_pair", lambda *a: None)
    monkeypatch.setattr(C.win_model_veto, "_heroes_vector", lambda *a: tuple(record["heroes"]))
    monkeypatch.setattr(C.win_model_veto, "last_kills30", lambda index: None)
    monkeypatch.setattr(laning_serving, "verdicts",
                        lambda *a, **k: {"all": verdicts.get("all"), "lane": verdicts.get("lane")})
    monkeypatch.setattr(C, "_team_elo_base_rating_for_side",
                        lambda meta, side: record["elo_r" if side == "radiant" else "elo_d"])
    monkeypatch.setattr(C, "_bookmaker_infer_map_num", lambda *a, **k: record["map_num"])
    monkeypatch.setattr(C, "_match_has_tier1_team", lambda *a: True)
    monkeypatch.setattr(C, "_ml_dispatch_sent_ledger", lambda: Ledger())
    monkeypatch.setattr(C, "_ml_dispatch_record_decisions",
                        lambda row, *, dedup_view: logged.append(row))
    monkeypatch.setattr(C, "_deliver_and_persist_signal",
                        lambda *a, **k: delivered.append((a, k)) or True)

    C._ml_dispatch_tick(
        match_key=record["match_key"],
        radiant_team_name=record["teams"]["radiant"],
        dire_team_name=record["teams"]["dire"],
        live_league={}, top="", mid="", bot="", protracker_payload=None,
        team_elo_block="", team_elo_meta={}, game_time_seconds=record["game_time"],
        radiant_lead=0, radiant_heroes_and_pos={}, dire_heroes_and_pos={},
        full_message_text="Captured panel text",
    )
    expected_header = C._format_signal_header(
        stake_team_name="GamerLegion", stake_multiplier=None,
        special_header_mode="early_kills", kills_window_label="5_15",
    )
    window_calls = [(args, kwargs) for args, kwargs in delivered
                    if kwargs["stake_multiplier_context"]["ml_market"] == "kills_window"]
    assert len(window_calls) == 1
    args, kwargs = window_calls[0]
    assert args[1].splitlines()[0] == expected_header
    assert "GamerLegion" in args[1].splitlines()[0]
    assert {key: kwargs["stake_multiplier_context"][key]
            for key in ("origin", "ml_market", "ml_rule")} == {
                "origin": "ml_dispatch", "ml_market": "kills_window",
                "ml_rule": "kills_lane_early_window",
            }
    assert logged[0]["delivered"][-1]["status"] == "delivered"
