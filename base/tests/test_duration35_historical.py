import numpy as np

from base.tools.duration35_historical import (
    FOLDS,
    THRESHOLD_SECONDS,
    build_history,
    split_masks,
    target,
    utc,
)
import pytest


def test_target_is_literal_inclusive_2100_seconds():
    assert target(np.array([2099, 2100, 2101])).tolist() == [1, 1, 0]
    assert THRESHOLD_SECONDS == 2100


def test_history_has_no_future_map_influence():
    ts = np.array([1_000, 4_000, 8_000], dtype=np.int64)
    duration = np.array([2_400, 2_000, 2_100], dtype=np.int64)
    heroes = np.arange(1, 31, dtype=np.int32).reshape(3, 10)
    accounts = np.arange(100, 130, dtype=np.int64).reshape(3, 10)
    prefix = build_history(ts[:2], duration[:2], heroes[:2], accounts[:2])
    extended = build_history(ts, duration, heroes, accounts)
    assert extended.shape == (3, 24)
    np.testing.assert_allclose(extended[:2], prefix, rtol=0, atol=0)


def test_split_excludes_maps_ending_on_or_after_boundary():
    boundary = utc("2026-06-01")
    ts = np.array([utc("2024-02-01"), utc("2026-03-15"), utc("2026-04-15"),
                   boundary - 60, boundary - 60], dtype=np.int64)
    duration = np.array([600, 600, 600, 59, 60], dtype=np.int64)
    train, early, calibration, test = split_masks(ts, duration, "may")[0]
    assert train.tolist() == [True, False, False, False, False]
    assert early.tolist() == [False, True, False, False, False]
    assert calibration.tolist() == [False, False, True, False, False]
    assert test.tolist() == [False, False, False, True, False]


def test_split_overlap_guard_is_a_real_boolean_sum(monkeypatch):
    from base.tools import duration35_historical as subject

    monkeypatch.setattr(subject, "FOLDS", {"x": ["2026-03-01", "2026-04-01", "2026-05-01", "2026-06-01"]})
    original = np.stack
    monkeypatch.setattr(np, "stack", lambda masks: np.array([[True, False], [True, False]]))
    with pytest.raises(ValueError, match="overlapping"):
        split_masks(np.array([utc("2024-02-01")]), np.array([1]), "x")
    monkeypatch.setattr(np, "stack", original)


def test_actual_fixed_split_masks_are_pairwise_disjoint():
    ts = np.array([utc("2024-02-01"), utc("2026-03-15"), utc("2026-04-15"), utc("2026-05-15")])
    masks, _ = split_masks(ts, np.array([600, 600, 600, 600]), "may")
    for index, left in enumerate(masks):
        for right in masks[index + 1:]:
            assert not np.any(left & right)


def test_fixed_e299_may_to_august_boundaries():
    assert FOLDS == {
        "may": ["2026-03-01", "2026-04-01", "2026-05-01", "2026-06-01"],
        "jun": ["2026-04-01", "2026-05-01", "2026-06-01", "2026-07-01"],
        "jul": ["2026-05-01", "2026-06-01", "2026-07-01", "2026-08-01"],
        "aug": ["2026-06-01", "2026-07-01", "2026-08-01", "2026-09-01"],
    }
