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


def test_residual_fit_recovers_group_rates_without_floating_errors():
    sys.path.insert(0, str(study.ROOT / "scripts/ops"))
    from research_lane_residual_check import fit, linear, sigmoid
    x = np.column_stack([np.ones(20), np.repeat([-1., 1.], 10)])
    y = np.array([1.] * 3 + [0.] * 7 + [1.] * 8 + [0.] * 2)
    with np.errstate(all="raise"):
        fitted = sigmoid(linear(x, fit(x, y)))
    assert np.allclose(fitted[:10], .3, atol=1e-4)
    assert np.allclose(fitted[10:], .8, atol=1e-4)


def test_minute_schedule_fits_only_still_unsent_discovery_maps():
    sys.path.insert(0, str(study.ROOT / "scripts/ops"))
    from research_lane_minute_schedule import fit_schedule, replay
    # Previously sent winners must not make a later 50/50 cohort look reliable.
    nw = np.zeros((1200, 11))
    nw[:1000, 1] = 100
    nw[:, 2] = 100
    hit = np.array([True] * 1100 + [False] * 100)
    population = np.ones(1200, dtype=bool)
    schedule, cohorts = fit_schedule(nw, hit, population, 1, .90, thresholds=[100])
    assert schedule == {1: 100} and cohorts[1]["n"] == 1000
    triggered, when = replay(nw, population, schedule)
    assert triggered.sum() == 1000 and np.all(when[1000:] == 10)


def test_growth_filter_uses_only_current_and_minute1_observations():
    sys.path.insert(0, str(study.ROOT / "scripts/ops"))
    from research_lane_minute_schedule import replay
    nw = np.zeros((2, 11))
    nw[:, 1], nw[:, 3] = [1000, 100], [1000, 1000]
    trigger, when = replay(nw, np.ones(2, dtype=bool), {3: 800}, growth=250)
    assert trigger.tolist() == [False, True] and when.tolist() == [10, 3]
    nw[:, 10] = [-9999, 9999]
    assert np.array_equal(trigger, replay(nw, np.ones(2, dtype=bool), {3: 800}, growth=250)[0])


def test_clock_sensitivity_shifts_observations_and_target_together():
    sys.path.insert(0, str(study.ROOT / "scripts/ops"))
    from research_lane_minute_schedule import clock_view
    raw = {"nw": np.arange(11)[None, :], "nw10": np.array([10])}
    primary, shifted = clock_view(raw, 0), clock_view(raw, -1)
    assert primary["nw"][0, 1] == 1 and primary["nw10"][0] == 10
    assert shifted["nw"][0, 1] == 0 and shifted["nw10"][0] == 9
    assert raw["nw10"][0] == 10 and np.isnan(shifted["nw"][0, 0])


def test_target_minimum_is_inclusive_directional_and_separate_from_entry():
    sys.path.insert(0, str(study.ROOT / "scripts/ops"))
    from research_lane_minute_schedule import target_hit, replay
    data = {"nw10": np.array([999, 1000, -1000, -999, 0, np.nan]),
            "dispatch_target": np.array([1, 1, -1, -1, 1, 1])}
    assert target_hit(data, 1000).tolist() == [False, True, True, False, False, False]
    assert target_hit(data).tolist() == [True, True, True, True, False, False]
    nw = np.zeros((6, 11)); nw[:, 4] = 1000
    take, _ = replay(nw, np.ones(6, dtype=bool), {4: 1000})
    assert take.all() and target_hit(data, 1000)[take].sum() == 2
    with pytest.raises(ValueError):
        target_hit(data, 0)
