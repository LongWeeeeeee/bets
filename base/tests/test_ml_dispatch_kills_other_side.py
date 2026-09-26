"""Cross-tick kills-window dedup using captured production decision rows."""
from dataclasses import replace
import json
from pathlib import Path
import sys

import pytest

from base import laning_serving
from base import ml_dispatch as md

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Isolated delivery checks exclude git-ignored base/keys.py. Keep a real keys.py.
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


def _captures(filename):
    path = Path(__file__).parent / "fixtures" / filename
    return {row["case"]: row for row in (json.loads(line) for line in path.read_text().splitlines())}


CAPTURES = {
    **_captures("ml_dispatch_kills_other_side_20260926.jsonl"),
    **_captures("ml_dispatch_lane_kills_20260926.jsonl"),
}
UNDERDOG = "fires_overlap_underdog_kw_same_side"
CONFLICT = "late_conflict_4_3_early_side_radiant"
LANE = "lane_rule_live_delivered_dire"


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


def _market(result, market):
    return [decision for decision in result.decisions if decision.market == market]


@pytest.mark.parametrize("case,side,other,rule", [
    (UNDERDOG, "Dire", "Radiant", "kills_underdog_early_window"),
    (CONFLICT, "Radiant", "Dire", "kills_late_conflict_early_side"),
])
def test_other_side_window_is_skipped_but_kills_total_is_unchanged(
    monkeypatch, case, side, other, rule,
):
    monkeypatch.delenv("ML_DISPATCH_KILLS_WINDOW_ONE_SIDE", raising=False)
    monkeypatch.setenv("ML_DISPATCH_LANE_KILLS", "0")
    ctx = _ctx(case)
    cfg = md.Config.from_env()
    if case == CONFLICT:
        cfg = replace(cfg, underdog_min_diff=1000.0)
    baseline = md.evaluate(ctx, cfg)
    assert [(decision.target_side, decision.rule) for decision in _market(baseline, "kills_window")] == [
        (side, rule)
    ]
    ctx.already_sent.add(md._dedup_key(ctx, "kills_window", other))
    result = md.evaluate(ctx, cfg)
    assert _market(result, "kills_window") == []
    assert any(item.market == "kills_window" and item.side == side
               and item.reason == md.REASON_KILLS_WINDOW_SENT_OTHER_SIDE
               for item in result.skipped)
    assert _market(result, "kills_total") == _market(baseline, "kills_total")
    assert md.evaluate(ctx, cfg) == result

    # The captured kills30 gate suppresses totals in both rows. Exercise the
    # independent total decision with only that existing gate disabled.
    total_cfg = replace(cfg, kills_total_gate_enabled=False)
    ctx.already_sent.clear()
    total_baseline = md.evaluate(ctx, total_cfg)
    ctx.already_sent.add(md._dedup_key(ctx, "kills_window", other))
    total_after = md.evaluate(ctx, total_cfg)
    assert _market(total_baseline, "kills_total")
    assert _market(total_after, "kills_total") == _market(total_baseline, "kills_total")


@pytest.mark.parametrize("case,side,other", [
    (UNDERDOG, "Dire", "Radiant"),
    (CONFLICT, "Radiant", "Dire"),
])
@pytest.mark.parametrize("value", ["0", "false", "off"])
def test_toggle_restores_old_window_behavior(monkeypatch, case, side, other, value):
    monkeypatch.setenv("ML_DISPATCH_LANE_KILLS", "0")
    monkeypatch.setenv("ML_DISPATCH_KILLS_WINDOW_ONE_SIDE", value)
    ctx = _ctx(case)
    ctx.already_sent.add(md._dedup_key(ctx, "kills_window", other))
    cfg = md.Config.from_env()
    if case == CONFLICT:
        cfg = replace(cfg, underdog_min_diff=1000.0)
    assert [decision.target_side for decision in _market(md.evaluate(ctx, cfg), "kills_window")] == [side]


@pytest.mark.parametrize("case,side", [
    (UNDERDOG, "Dire"), (CONFLICT, "Radiant"),
])
def test_same_side_dedup_keeps_original_reason(monkeypatch, case, side):
    monkeypatch.setenv("ML_DISPATCH_LANE_KILLS", "0")
    ctx = _ctx(case)
    ctx.already_sent.add(md._dedup_key(ctx, "kills_window", side))
    ctx.already_sent.add(md._dedup_key(ctx, "kills_window", md._other_side(side)))
    cfg = md.Config.from_env()
    if case == CONFLICT:
        cfg = replace(cfg, underdog_min_diff=1000.0)
    result = md.evaluate(ctx, cfg)
    assert _market(result, "kills_window") == []
    assert any(item.market == "kills_window" and item.side == side
               and item.reason == md.REASON_DEDUP for item in result.skipped)


def test_lane_rule_other_side_check_stays_on_when_old_paths_rolled_back(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_KILLS_WINDOW_ONE_SIDE", "0")
    ctx = _ctx(LANE)
    ctx.already_sent.add(md._dedup_key(ctx, "kills_window", "Radiant"))
    result = md.evaluate(ctx, md.Config.from_env())
    assert _market(result, "kills_window") == []
    assert any(item.market == "kills_window" and item.side == "Dire"
               and item.reason == md.REASON_KILLS_WINDOW_SENT_OTHER_SIDE
               for item in result.skipped)


def test_ml_tick_does_not_deliver_opposite_window(monkeypatch):
    record = CAPTURES[UNDERDOG]["record"]
    verdicts = record["verdicts"]
    delivered = []
    logged = []
    other_key = (record["base_url"], record["map_num"], "kills_window", "Radiant")

    class Ledger:
        def __init__(self):
            self.keys = {other_key}

        def as_set(self):
            return set(self.keys)

        def add(self, key):
            self.keys.add(tuple(key))

        def save(self):
            pass

    monkeypatch.setenv("DISPATCH_MODE", "ml")
    monkeypatch.setenv("ML_DISPATCH_LANE_KILLS", "0")
    monkeypatch.delenv("ML_DISPATCH_KILLS_WINDOW_ONE_SIDE", raising=False)
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
    assert logged
    assert not any(kwargs["stake_multiplier_context"]["ml_market"] == "kills_window"
                   for _, kwargs in delivered)
    assert any(item["market"] == "kills_window" and item["side"] == "Dire"
               and item["reason"] == md.REASON_KILLS_WINDOW_SENT_OTHER_SIDE
               for item in logged[0]["skipped"])
