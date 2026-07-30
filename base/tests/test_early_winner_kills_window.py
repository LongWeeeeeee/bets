"""Early Winner STAR + kills_window nearest-block gate (≥3m lead, |ed|≥1)."""

from __future__ import annotations

import importlib
import math
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

cs = importlib.import_module("cyberscore_try")


def test_select_nearest_prefers_soonest_open_window_with_lead():
    # game_time=0 → 5_15 starts at 300s; with lead=180 still ok (0+180<=300)
    ed = {
        "5_15": {"expected_diff": 1.4},
        "10_20": {"expected_diff": 2.0},
        "15_25": {"expected_diff": -3.0},
        "20_30": {"expected_diff": 4.0},
    }
    picked = cs._select_nearest_kills_window(
        game_time_seconds=0,
        ed_by_label=ed,
        star_sign=1,
        min_abs_ed=1.0,
        lead_seconds=180.0,
    )
    assert picked is not None
    assert picked["label"] == "5_15"
    assert picked["expected_diff"] == 1.4
    assert picked["seconds_until_start"] == 300.0


def test_select_nearest_skips_window_without_lead_margin():
    # gt=150s, 5_15 starts 300 → remaining 150 < lead 180 → skip 5_15, take 10_20
    ed = {
        "5_15": {"expected_diff": 2.5},
        "10_20": {"expected_diff": 1.1},
        "15_25": {"expected_diff": 1.0},
    }
    picked = cs._select_nearest_kills_window(
        game_time_seconds=150,
        ed_by_label=ed,
        star_sign=1,
        min_abs_ed=1.0,
        lead_seconds=180.0,
    )
    assert picked is not None
    assert picked["label"] == "10_20"
    assert picked["seconds_until_start"] == 600.0 - 150.0


def test_select_nearest_requires_same_sign_and_abs_ed():
    ed = {
        "5_15": {"expected_diff": -2.0},  # opposite
        "10_20": {"expected_diff": 0.5},  # |ed|<1
        "15_25": {"expected_diff": 1.2},
    }
    picked = cs._select_nearest_kills_window(
        game_time_seconds=0,
        ed_by_label=ed,
        star_sign=1,
        min_abs_ed=1.0,
        lead_seconds=180.0,
    )
    assert picked is not None
    assert picked["label"] == "15_25"


def test_select_nearest_dire_sign():
    ed = {
        "5_15": {"expected_diff": 1.5},
        "10_20": {"expected_diff": -1.8},
    }
    picked = cs._select_nearest_kills_window(
        game_time_seconds=0,
        ed_by_label=ed,
        star_sign=-1,
        min_abs_ed=1.0,
        lead_seconds=180.0,
    )
    assert picked is not None
    assert picked["label"] == "10_20"
    assert picked["expected_diff"] == -1.8


def test_select_nearest_none_when_all_too_late():
    # gt=700, lead=180 → need window_start >= 880; 15_25 starts 900 ok but ed filter
    ed = {
        "5_15": {"expected_diff": 2.0},
        "10_20": {"expected_diff": 2.0},
        "15_25": {"expected_diff": 0.2},  # abs too small
        "20_30": {"expected_diff": 0.0},
    }
    picked = cs._select_nearest_kills_window(
        game_time_seconds=700,
        ed_by_label=ed,
        star_sign=1,
        min_abs_ed=1.0,
        lead_seconds=180.0,
    )
    assert picked is None


def test_kills_window_label():
    assert cs._kills_window_label(5, 15) == "5_15"
    assert cs._kills_window_label(20, 30) == "20_30"


def test_earlier_is_better_when_multiple_open():
    # Both 5_15 and 10_20 open at gt=0; nearest start wins even if later has larger |ed|
    ed = {
        "5_15": {"expected_diff": 1.05},
        "10_20": {"expected_diff": 9.0},
    }
    picked = cs._select_nearest_kills_window(
        game_time_seconds=0,
        ed_by_label=ed,
        star_sign=1,
        min_abs_ed=1.0,
        lead_seconds=180.0,
    )
    assert picked["label"] == "5_15"


def test_payload_scalar_expected_diff_supported():
    ed = {"10_20": 1.7}
    picked = cs._select_nearest_kills_window(
        game_time_seconds=100,
        ed_by_label=ed,
        star_sign=1,
        min_abs_ed=1.0,
        lead_seconds=180.0,
    )
    assert picked is not None
    assert math.isclose(picked["expected_diff"], 1.7)
