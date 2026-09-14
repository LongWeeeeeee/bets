"""Regression checks for offline stopping-policy semantics."""
import importlib.util
import sys
from pathlib import Path

import numpy as np
import pytest

SPEC = importlib.util.spec_from_file_location(
    "research_lane_wait", Path(__file__).resolve().parents[2] / "scripts/ops/research_lane_wait.py")
study = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(study)


def test_first_crossing_keeps_early_mistake_and_never_looks_at_target():
    nw = np.zeros((3, 11))
    nw[0, 4], nw[0, 5], nw[0, 10] = 500, -1500, -2000
    nw[1, 4], nw[1, 5], nw[1, 10] = -500, 600, 2000
    nw[2, 4] = 1000
    trigger, when = study.first_crossing(nw, {4: 500, 5: 500}, np.array([True, True, False]))
    assert trigger.tolist() == [True, True, False]
    assert when.tolist() == [4, 5, 10]
    nw[:, 10] *= -1
    again = study.first_crossing(nw, {4: 500, 5: 500}, np.array([True, True, False]))
    assert np.array_equal(again[0], trigger) and np.array_equal(again[1], when)


def test_exact_gold_threshold_nan_and_direction_are_preserved():
    nw = np.zeros((3, 11))
    nw[:, 4] = [500, -500, np.nan]
    trigger, _ = study.first_crossing(nw, {4: 500}, np.ones(3, dtype=bool))
    assert trigger.tolist() == [True, False, False]


def test_duplicate_maps_fail_closed():
    with pytest.raises(ValueError, match="Duplicate"):
        study.unique_index([123, 123])


def test_missing_or_zero_placeholder_timeline_excluded():
    nw = np.zeros((3, 11))
    nw[0] = np.nan
    nw[2, 4] = 500
    assert study.valid_timeline(nw, np.array([1000, 1000, 1000])).tolist() == [False, False, True]


def test_coverage_counts_unsent_maps_and_tie_is_failure():
    selected = np.array([True, True, False, False])
    result = study.summary(np.array([True, False, True, True]), selected,
                           np.ones(4, dtype=bool), np.array([4, 6, 10, 10]))
    assert result["rate"] == .5
    assert result["coverage"] == .5
    assert result["saved_minutes_per_eligible_map"] == 2.5


def test_isolated_dictionary_source_matches_real_cascade():
    sys.path.insert(0, str(study.ROOT / "scripts/ops"))
    import functions as real
    from research_lane_dictionary import isolated_source
    isolated, _ = isolated_source(study.ROOT / "base/functions.py", ["calculate_lanes", "structure_lane_dict"])
    flat = {"2pos2_vs_7pos2": {"wins": 80, "draws": 5, "games": 100}}
    r = {"pos" + str(i): {"hero_id": i} for i in range(1, 6)}
    d = {"pos" + str(i): {"hero_id": i + 5} for i in range(1, 6)}
    expected = real.calculate_lanes(r, d, real.structure_lane_dict(flat), core_support_side_lanes=True)
    actual = isolated["calculate_lanes"](r, d, isolated["structure_lane_dict"](flat), core_support_side_lanes=True)
    assert actual == expected
    assert "win 79%" in actual[2]
