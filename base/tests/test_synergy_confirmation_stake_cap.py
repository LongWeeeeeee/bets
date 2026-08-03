"""Regression tests for display-only synergy confirmations and stake veto."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

runtime = importlib.import_module("cyberscore_try")


def _strong_late_stake(**overrides):
    payload = {
        "team_elo_meta": None,
        "target_side": "radiant",
        "selected_early_sign": 1,
        "selected_late_sign": 1,
        "has_selected_early_star": True,
        "has_selected_late_star": True,
        "early_wr_pct": 70.0,
        "late_wr_pct": 75.0,
        "game_time_seconds": 30 * 60,
        "radiant_lead": 2000.0,
        "late_star_hit_count": 3,
        "early_star_hit_count": 3,
        "late_star_hit_metrics": ["counterpick_1vs1", "counterpick_1vs2", "solo"],
    }
    payload.update(overrides)
    return runtime._stake_multiplier_for_signal(**payload)


def test_synergy_confirmation_threshold_is_nine_for_both_metrics() -> None:
    assert runtime.SYNERGY_CONFIRMATION_ABS_THRESHOLD == 9.0

    displayed = runtime._decorate_star_block_for_display(
        {
            "synergy_duo": 9,
            "synergy_trio": -9,
            "solo": 999,
        },
        section="mid_output",
        target_wr=60,
    )

    assert displayed["synergy_duo"] == "9**"
    assert displayed["synergy_trio"] == "-9**"
    assert displayed["solo"].endswith("*")


def test_synergy_below_nine_is_not_marked() -> None:
    displayed = runtime._decorate_star_block_for_display(
        {"synergy_duo": 8.99, "synergy_trio": -8.99},
        section="all_output",
        target_wr=60,
    )

    assert displayed["synergy_duo"] == 8.99
    assert displayed["synergy_trio"] == -8.99


def test_opposite_synergy_confirmation_caps_stake_at_half() -> None:
    assert _strong_late_stake(synergy_opposes_target=False) == 2
    assert _strong_late_stake(synergy_opposes_target=True) == 0.5


def test_synergy_veto_precedes_dispatch_side_fallback() -> None:
    assert _strong_late_stake(
        selected_early_sign=-1,
        selected_late_sign=-1,
        synergy_opposes_target=False,
    ) == 1
    assert _strong_late_stake(
        selected_early_sign=-1,
        selected_late_sign=-1,
        synergy_opposes_target=True,
    ) == 0.5


def test_synergy_snapshot_checks_all_blocks_relative_to_target() -> None:
    snapshot = runtime._synergy_confirmation_snapshot_for_target(
        target_side="radiant",
        early_output={"synergy_duo": 12},
        early_end_output={"synergy_trio": -8},
        mid_output={"synergy_trio": -9},
        all_output={"synergy_duo": -11},
    )

    assert snapshot["threshold"] == 9.0
    assert snapshot["has_opposite"] is True
    assert {(row["phase"], row["metric"]) for row in snapshot["conflicts"]} == {
        ("late", "synergy_trio"),
        ("all", "synergy_duo"),
    }
    assert not any(row["phase"] == "early_winner" for row in snapshot["confirmations"])


def test_delayed_context_preserves_synergy_half_stake_cap() -> None:
    snapshot = runtime._synergy_confirmation_snapshot_for_target(
        target_side="radiant",
        mid_output={"synergy_duo": -9},
    )
    context = runtime._build_stake_multiplier_context(
        stake_team_name="Radiant Team",
        target_side="radiant",
        team_elo_meta=None,
        radiant_team_name="Radiant Team",
        dire_team_name="Dire Team",
        selected_early_sign=1,
        selected_late_sign=1,
        has_selected_early_star=True,
        has_selected_late_star=True,
        early_wr_pct=70.0,
        late_wr_pct=75.0,
        late_star_hit_count=3,
        early_star_hit_count=3,
        late_star_hit_metrics=["counterpick_1vs1", "counterpick_1vs2", "solo"],
        synergy_confirmation_snapshot=snapshot,
    )

    assert context["synergy_opposes_target"] is True
    assert runtime._stake_multiplier_from_context(
        context,
        game_time_seconds=30 * 60,
        radiant_lead=2000.0,
    ) == 0.5


def test_early_kills_retarget_recomputes_synergy_direction() -> None:
    late_target_context = {
        "target_side": "dire",
        "synergy_opposes_target": False,
        "synergy_confirmation_snapshot": {"target_side": "dire", "has_opposite": False},
    }

    retargeted = runtime._retarget_stake_context_synergy_confirmation(
        late_target_context,
        target_side="radiant",
        mid_output={"synergy_duo": -9},
    )

    assert retargeted["target_side"] == "radiant"
    assert retargeted["synergy_opposes_target"] is True
    assert retargeted["synergy_confirmation_snapshot"]["target_side"] == "radiant"
    assert retargeted["synergy_confirmation_snapshot"]["conflicts"] == [
        {
            "phase": "late",
            "metric": "synergy_duo",
            "value": -9.0,
            "sign": -1,
            "side": "dire",
        }
    ]
    # Original late-side context is not mutated.
    assert late_target_context["target_side"] == "dire"
    assert late_target_context["synergy_opposes_target"] is False


def test_early_kills_retarget_removes_stale_synergy_veto() -> None:
    late_target_context = {
        "target_side": "dire",
        "synergy_opposes_target": True,
        "synergy_confirmation_snapshot": {"target_side": "dire", "has_opposite": True},
    }

    retargeted = runtime._retarget_stake_context_synergy_confirmation(
        late_target_context,
        target_side="radiant",
        all_output={"synergy_trio": 9},
    )

    assert retargeted["synergy_opposes_target"] is False
    assert retargeted["synergy_confirmation_snapshot"]["conflicts"] == []


def test_synergy_confirmations_never_enter_star_hits() -> None:
    hits = runtime._collect_star_hits_for_block(
        {"synergy_duo": 99, "synergy_trio": -99},
        "mid_output",
    )

    assert hits == []
