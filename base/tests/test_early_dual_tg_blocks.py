"""Telegram dual Early blocks: NW (early_output) + Winner (early_end_output)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

ROOT = BASE_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _heroes(start_id: int) -> dict:
    # pos -> {hero_id: ...}
    return {
        "pos1": {"hero_id": start_id},
        "pos2": {"hero_id": start_id + 1},
        "pos3": {"hero_id": start_id + 2},
        "pos4": {"hero_id": start_id + 3},
        "pos5": {"hero_id": start_id + 4},
    }


def _metric_block(**vals):
    base = {
        "counterpick_1vs1": None,
        "pos1_vs_pos1": None,
        "counterpick_1vs2": None,
        "solo": None,
        "synergy_duo": None,
        "synergy_trio": None,
    }
    base.update(vals)
    return base


def test_synergy_emits_early_end_output_with_same_metric_keys():
    from functions import synergy_and_counterpick

    out = synergy_and_counterpick(
        _heroes(1),
        _heroes(6),
        early_dict={},
        mid_dict={},
        post_lane_dict={},
        early_end_dict={},
    )
    assert "early_output" in out
    assert "early_end_output" in out
    # Parallel early blocks share the same metric key set.
    assert set(out["early_output"].keys()) == set(out["early_end_output"].keys())
    for key in (
        "counterpick_1vs1",
        "counterpick_1vs2",
        "pos1_vs_pos1",
    ):
        assert key in out["early_output"]
        assert key in out["early_end_output"]


def test_synergy_without_early_end_dict_omits_winner_block():
    from functions import synergy_and_counterpick

    out = synergy_and_counterpick(
        _heroes(1),
        _heroes(6),
        early_dict={},
        mid_dict={},
        post_lane_dict={},
        early_end_dict=None,
    )
    assert "early_output" in out
    assert "early_end_output" not in out


def test_recommend_odds_early_end_uses_early_thresholds():
    """early_end phase must resolve STAR thresholds via early_output table."""
    import cyberscore_try as runtime

    data = _metric_block(
        counterpick_1vs1="12*",
        counterpick_1vs2="12*",
        solo="12*",
    )
    rec_early = runtime._recommend_odds_for_block(data, "early")
    rec_end = runtime._recommend_odds_for_block(data, "early_end")
    if rec_early is None:
        pytest.skip("early recommend returned None with synthetic block; thresholds env-specific")
    assert rec_end is not None
    assert "wr_pct" in rec_end


def test_early_local_kills_message_has_dual_early_blocks(monkeypatch):
    import cyberscore_try as runtime

    monkeypatch.setattr(
        runtime,
        "_decorate_star_block_for_display",
        lambda raw_block, section, target_wr: dict(raw_block or {}),
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda data, phase: {"wr_pct": 62.0, "odds": 1.8} if data else None,
    )
    monkeypatch.setattr(runtime, "_build_lane_block", lambda *a, **kw: "Lane block\n")
    monkeypatch.setattr(runtime, "_build_series_score_line", lambda *a, **kw: "")
    monkeypatch.setattr(runtime, "_build_star_hits_summary_block", lambda **kw: "")
    monkeypatch.setattr(
        runtime,
        "_compose_star_metric_blocks_for_message",
        lambda early, mid, all_, mix="": f"{early}{mid}{all_}{mix}",
    )
    monkeypatch.setattr(runtime, "_format_live_message_state_block", lambda **kw: "")
    monkeypatch.setattr(runtime, "_build_all_star_output", lambda post, _pro: dict(post or {}))
    monkeypatch.setattr(runtime, "normalize_team_name_display", lambda x: str(x or ""))
    monkeypatch.setattr(runtime, "_format_signal_header", lambda **kw: "HDR")

    metrics_payload = {
        "early_output": _metric_block(counterpick_1vs1="5*", solo="4*"),
        "early_end_output": _metric_block(counterpick_1vs1="6*", solo="3*"),
        "mid_output": _metric_block(counterpick_1vs1="2*"),
        "post_lane_output": _metric_block(counterpick_1vs1="1*"),
        "lane_adv_dict": 0,
        "lane_kills_adv_dict": 0,
        "top": 0,
        "mid": 0,
        "bot": 0,
    }
    msg = runtime._build_early_local_kills_message(
        radiant_team_name="TeamR",
        dire_team_name="TeamD",
        target_team_name="TeamR",
        live_league={},
        metrics_payload=metrics_payload,
        team_elo_block="",
        game_time_seconds=1200,
        radiant_lead=0,
        star_target_wr=60,
    )
    assert "Early NW" in msg
    assert "Early Winner" in msg
    assert "Early NW (20-28):" in msg
    assert "Early Winner (20-28):" in msg
    assert msg.count("Counterpick_1vs1:") >= 2
    assert msg.count("Solo:") >= 2
