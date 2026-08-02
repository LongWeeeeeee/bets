"""STAR block contract: three independent families with ordered fallbacks."""
from __future__ import annotations

import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402
import functions  # noqa: E402


def test_primary_cp_and_trio_suppress_starred_fallbacks() -> None:
    block = {
        "solo": 8,
        "counterpick_1vs2": 12,
        "counterpick_1vs1": -19,
        "synergy_trio": 14,
        "synergy_duo": -19,
    }

    diag = runtime._star_block_diagnostics(block, 60, "early_output")

    assert diag["valid"] is True
    assert diag["block_index"] == 80
    assert diag["sign"] == 1
    assert diag["hit_metrics"] == ["solo", "counterpick_1vs2", "synergy_trio"]


def test_cp1vs1_and_duo_are_used_only_when_primaries_have_no_star_index() -> None:
    block = {
        "solo": -8,
        "counterpick_1vs2": -3,
        "counterpick_1vs1": -11,
        "synergy_trio": -5,
        "synergy_duo": -12,
    }

    diag = runtime._star_block_diagnostics(block, 60, "early_output")

    assert diag["valid"] is True
    assert diag["hit_metrics"] == ["solo", "counterpick_1vs1", "synergy_duo"]
    assert diag["sign"] == -1


def test_early_and_late_reject_family_blocks_below_index_75() -> None:
    early = {
        "solo": 5,
        "counterpick_1vs2": 8,
        "synergy_trio": 9,
    }
    late = {
        "solo": 6,
        "counterpick_1vs2": 8,
        "synergy_trio": 9,
    }

    assert runtime._star_block_diagnostics(early, 60, "early_output")["status"] == (
        "block_index_below_floor"
    )
    assert runtime._star_block_diagnostics(late, 60, "mid_output")["status"] == (
        "block_index_below_floor"
    )


def test_all_accepts_three_families_from_index_60_and_marks_only_selected() -> None:
    payload = {
        "all_output": {
            "solo": 1,
            "counterpick_1vs2": 3,
            "counterpick_1vs1": 11,
            "synergy_trio": 3,
            "synergy_duo": 19,
        }
    }

    assert functions.format_output_dict(payload, target_wr=60) is True
    block = payload["all_output"]
    assert str(block["solo"]).endswith("*")
    assert str(block["counterpick_1vs2"]).endswith("*")
    assert str(block["synergy_trio"]).endswith("*")
    assert not str(block["counterpick_1vs1"]).endswith("*")
    assert not str(block["synergy_duo"]).endswith("*")


def test_missing_or_conflicting_family_never_forms_a_block() -> None:
    missing = {"solo": 8, "counterpick_1vs2": 12}
    conflict = {"solo": 8, "counterpick_1vs2": 12, "synergy_trio": -14}

    assert runtime._star_block_diagnostics(missing, 60, "early_output")["valid"] is False
    assert runtime._star_block_diagnostics(conflict, 60, "early_output")["valid"] is False


def test_family_index_is_reranked_to_empirical_wr_for_stake(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "STAR_ODDS_USE_CALIBRATION", True)
    rec = runtime._recommend_odds_for_block(
        {
            "solo": "7*",
            "counterpick_1vs2": "10*",
            "synergy_trio": "11*",
        },
        "early",
    )

    assert rec is not None
    assert rec["level"] == 75
    assert rec["wr_pct"] == 58.35
    assert rec["min_odds"] == 1.71


def test_stake_bands_apply_to_early_late_and_all() -> None:
    common = {
        "team_elo_meta": None,
        "target_side": "radiant",
        "selected_early_sign": None,
        "selected_late_sign": None,
        "has_selected_early_star": False,
        "has_selected_late_star": False,
        "early_wr_pct": None,
        "late_wr_pct": None,
        "game_time_seconds": 30 * 60,
        "radiant_lead": 0,
    }
    all_only = {
        **common,
        "selected_all_sign": 1,
        "has_selected_all_star": True,
    }
    assert runtime._stake_multiplier_for_signal(**all_only, all_wr_pct=60.2) == 0.5
    assert runtime._stake_multiplier_for_signal(**all_only, all_wr_pct=64.15) == 1
    assert runtime._stake_multiplier_for_signal(**all_only, all_wr_pct=71.14) == 2

    early_only = {
        **common,
        "selected_early_sign": 1,
        "has_selected_early_star": True,
    }
    assert runtime._stake_multiplier_for_signal(
        **{**early_only, "early_wr_pct": 58.35}
    ) == 0.5
    assert runtime._stake_multiplier_for_signal(
        **{**early_only, "early_wr_pct": 62.64}
    ) == 1


def test_requested_mix_example_is_index75_shadow_and_dltv_below_star_is_ignored() -> None:
    block = {
        "dota2protracker_cp1vs1": -9.60,
        "dota2protracker_duo": -43.52,
        "dota2protracker_solo": -5.31,
        "dota2protracker_solo_overall": -6.30,
        "dltv_rating": 23.7,
    }

    decorated, diag = runtime._decorate_mix_block_for_display(block)

    assert diag["valid"] is True
    assert diag["sign"] == -1
    assert diag["block_index"] == 75
    assert diag["empirical_wr_pct"] == 75.04
    assert diag["dltv_support"] is False
    assert str(decorated["dota2protracker_cp1vs1"]).endswith("*")
    assert str(decorated["dota2protracker_duo"]).endswith("*")
    assert str(decorated["dota2protracker_solo"]).endswith("*")
    assert not str(decorated["dota2protracker_solo_overall"]).endswith("*")
    assert not str(decorated["dltv_rating"]).endswith("*")


def test_mix_uses_overall_solo_fallback_and_dltv_star_can_veto() -> None:
    block = {
        "dota2protracker_cp1vs1": 8.0,
        "dota2protracker_duo": 19.0,
        "dota2protracker_solo": 0.5,
        "dota2protracker_solo_overall": 2.0,
        "dltv_rating": -30.0,
    }

    diag = runtime._mix_block_diagnostics(block)

    assert diag["valid"] is False
    assert diag["status"] == "dltv_star_conflict"
    assert diag["selected_hits"][-1]["metric"] == "dota2protracker_solo_overall"
