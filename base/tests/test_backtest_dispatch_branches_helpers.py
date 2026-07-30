#!/usr/bin/env python3
"""Unit-smoke for backtest_dispatch_branches pure helpers (synthetic dicts only)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from backtest_dispatch_branches import (  # noqa: E402
    METRIC_STAT_NAMES,
    WR_LEVELS_DESC,
    _active_combo_at_floor,
    _block_combo_label,
    _build_all_output_no_d2pt,
    _build_independent_branch_id,
    _check_sign_agreement,
    _determine_target_side,
    _dispatch_mode,
    evaluate_match_independent_wr,
)


def test_wr_levels_desc_high_to_low():
    assert WR_LEVELS_DESC[0] == max(WR_LEVELS_DESC)
    assert WR_LEVELS_DESC[-1] == min(WR_LEVELS_DESC)
    assert WR_LEVELS_DESC == sorted(WR_LEVELS_DESC, reverse=True)


def test_block_combo_label():
    assert _block_combo_label(True, True, True) == "E+L+A"
    assert _block_combo_label(False, True, True) == "L+A"
    assert _block_combo_label(True, False, False) == "E"
    assert _block_combo_label(False, False, False) == "none"


def test_independent_branch_id_format():
    bid = _build_independent_branch_id(65, 60, None, 1, -1, None)
    assert bid == "E65|L60|Anone|E+_L-"
    bid2 = _build_independent_branch_id(None, None, None, None, None, None)
    assert bid2 == "Enone|Lnone|Anone|none"
    bid3 = _build_independent_branch_id(70, 70, 60, 1, 1, 1)
    assert bid3 == "E70|L70|A60|E+_L+_A+"


def test_target_side_priority_late_over_early_over_all():
    # late wins priority
    assert (
        _determine_target_side(True, True, True, 1, -1, 1) == "dire"
    )
    # early when no late
    assert (
        _determine_target_side(True, False, True, 1, None, -1) == "radiant"
    )
    # all only
    assert (
        _determine_target_side(False, False, True, None, None, -1) == "dire"
    )
    assert _determine_target_side(False, False, False, None, None, None) is None


def test_sign_agreement_and_dispatch_mode():
    assert _check_sign_agreement(True, True, False, 1, 1, None) == "same_sign"
    assert _check_sign_agreement(True, True, False, 1, -1, None) == "opposite"
    assert _check_sign_agreement(False, False, False, None, None, None) == "none"

    assert _dispatch_mode(True, True, False, "same_sign") == "immediate_same_sign"
    assert _dispatch_mode(True, True, False, "opposite") == "delayed_opposite_sign"
    assert _dispatch_mode(True, False, False, "none") == "early_only"
    assert _dispatch_mode(False, True, False, "none") == "late_only"
    assert _dispatch_mode(False, False, True, "none") == "all_only"
    assert _dispatch_mode(False, False, False, "none") == "no_star"


def test_active_combo_at_floor():
    assert _active_combo_at_floor(65, 60, None, 60) == "E+L"
    assert _active_combo_at_floor(65, 60, None, 65) == "E"
    assert _active_combo_at_floor(65, 60, None, 70) == "none"
    assert _active_combo_at_floor(70, 70, 65, 65) == "E+L+A"


def test_all_output_no_d2pt_from_post_lane():
    post = {
        "counterpick_1vs1": 8,
        "counterpick_1vs2": 5,
        "solo": 2,
        "synergy_duo": 3,
        "synergy_trio": 4,
        "pos1_vs_pos1": 12,  # non-star — must NOT enter all_output
        "dota2protracker_cp1vs1": 99,  # must NEVER appear
    }
    all_out = _build_all_output_no_d2pt(post)
    assert "dota2protracker_cp1vs1" not in all_out
    assert "pos1_vs_pos1" not in all_out
    assert all_out["counterpick_1vs1"] == 8
    assert all_out["synergy_trio"] == 4
    # empty / None
    assert _build_all_output_no_d2pt(None) == {}
    assert _build_all_output_no_d2pt({}) == {}


def test_evaluate_match_independent_wr_synthetic_strong_early_late():
    """Strong early+late same sign → high max WR, one branch, no d2pt."""
    metrics = {
        "early_output": {
            "counterpick_1vs1": 20,
            "counterpick_1vs2": 18,
            "solo": 10,
            "synergy_duo": 20,
            "synergy_trio": 20,
            "pos1_vs_pos1": 15,
        },
        "mid_output": {
            "counterpick_1vs1": 16,
            "counterpick_1vs2": 15,
            "solo": 9,
            "synergy_duo": 10,
            "synergy_trio": 20,
            "pos1_vs_pos1": 8,
        },
        "post_lane_output": {
            "counterpick_1vs1": 12,
            "counterpick_1vs2": 10,
            "solo": 5,
            "synergy_duo": 6,
            "synergy_trio": 14,
            "pos1_vs_pos1": 7,
            "dota2protracker_cp1vs1": 50,  # must be ignored
        },
    }
    result = evaluate_match_independent_wr(metrics)
    assert "dota2protracker_cp1vs1" not in result["all_output"]
    assert result["has_e"] is True
    assert result["has_l"] is True
    assert result["e_sign"] == 1
    assert result["l_sign"] == 1
    assert result["e_wr"] is not None and result["e_wr"] >= 60
    assert result["l_wr"] is not None and result["l_wr"] >= 60
    assert result["block_combo"] in ("E+L", "E+L+A")
    assert result["agreement"] == "same_sign"
    assert result["target_side"] == "radiant"  # late priority, sign +
    assert result["dispatch_mode"] == "immediate_same_sign"
    # branch id uses independent max WRs
    assert result["branch_id"].startswith("E")
    assert "|L" in result["branch_id"]
    assert result["branch_id"].count("|") == 3  # E|L|A|signs
    # floors at 60 must include E and L
    assert "E" in result["floors"][60]
    assert "L" in result["floors"][60]


def test_evaluate_match_opposite_signs():
    metrics = {
        "early_output": {
            "counterpick_1vs1": 20,
            "counterpick_1vs2": 18,
            "solo": 10,
            "synergy_duo": 20,
            "synergy_trio": 20,
        },
        "mid_output": {
            "counterpick_1vs1": -16,
            "counterpick_1vs2": -15,
            "solo": -9,
            "synergy_duo": -10,
            "synergy_trio": -20,
        },
        "post_lane_output": {},
    }
    result = evaluate_match_independent_wr(metrics)
    assert result["has_e"] and result["has_l"]
    assert result["e_sign"] == 1
    assert result["l_sign"] == -1
    assert result["agreement"] == "opposite"
    assert result["target_side"] == "dire"  # late priority
    assert result["dispatch_mode"] == "delayed_opposite_sign"


def test_evaluate_match_no_star():
    metrics = {
        "early_output": {"counterpick_1vs1": 1, "solo": 0},
        "mid_output": {},
        "post_lane_output": {"counterpick_1vs1": 1},
    }
    result = evaluate_match_independent_wr(metrics)
    assert result["block_combo"] == "none"
    assert result["e_wr"] is None
    assert result["l_wr"] is None
    assert result["a_wr"] is None
    assert result["dispatch_mode"] == "no_star"
    assert result["target_side"] is None
    assert result["branch_id"] == "Enone|Lnone|Anone|none"


def test_metric_stat_names_include_pos1_not_d2pt():
    assert "pos1_vs_pos1" in METRIC_STAT_NAMES
    assert "dota2protracker_cp1vs1" not in METRIC_STAT_NAMES
    assert "counterpick_1vs1" in METRIC_STAT_NAMES


def test_one_branch_per_match_structure():
    """Independent max-WR produces exactly one branch_id (no WR multi-count)."""
    metrics = {
        "early_output": {
            "counterpick_1vs1": 12,
            "counterpick_1vs2": 12,
            "solo": 8,
            "synergy_duo": 16,
            "synergy_trio": 14,
        },
        "mid_output": {
            "counterpick_1vs1": 10,
            "counterpick_1vs2": 8,
            "solo": 6,
            "synergy_trio": 9,
        },
        "post_lane_output": {
            "counterpick_1vs1": 8,
            "counterpick_1vs2": 5,
            "synergy_trio": 7,
        },
    }
    r1 = evaluate_match_independent_wr(metrics)
    r2 = evaluate_match_independent_wr(metrics)
    # deterministic single branch
    assert r1["branch_id"] == r2["branch_id"]
    # single string id — not a list of ladder steps
    assert isinstance(r1["branch_id"], str)
    assert r1["branch_id"].count("|") == 3
