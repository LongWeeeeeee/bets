"""Regression for parallel matches leaking final duration into draft history."""
import numpy as np
import pytest

from base.end_time_prior import EndTimePrior


def test_unfinished_match_result_does_not_change_next_prior():
    starts = np.array([100, 110, 10000])
    keys = np.ones(3, dtype=int)
    for duration in (60., 100.):
        values = np.array([duration, 20., 30.])
        prior = EndTimePrior(keys, starts, starts + values * 60)
        out = prior.compute(values, np.ones(3), 1, 30)
        assert out[1] == 30
        assert out[2] == pytest.approx((duration + 20 + 30) / 3)


def test_strict_end_ties_and_later_start_finishing_first():
    starts = np.array([0, 10, 30, 31, 100, 101])
    ends = np.array([100, 30, 300, 300, 300, 300])
    p = EndTimePrior(np.ones(6), starts, ends)
    sums, counts = p.sum_count(np.array([10, 20, 30, 40, 50, 60]), np.ones(6))
    np.testing.assert_array_equal(counts, [0, 0, 0, 1, 1, 2])
    np.testing.assert_array_equal(sums, [0, 0, 0, 20, 20, 30])


def test_unknown_keys_invalid_durations_and_nan_values_are_not_history():
    keys = np.array([0, 0, 7, 7, 7, 7, 7])
    starts = np.arange(7) * 100
    ends = np.array([1, 101, 200, np.nan, 401, 501, 601])
    p = EndTimePrior(keys, starts, ends)
    sums, counts = p.sum_count([9, 9, 9, 9, np.nan, 5, 6], np.ones(7))
    np.testing.assert_array_equal(counts, [0, 0, 0, 0, 0, 0, 1])
    assert sums[-1] == 5


def test_random_unsorted_events_match_brute_force_asof():
    rng = np.random.default_rng(298)
    keys = rng.integers(0, 6, 150)
    starts = rng.integers(0, 200, 150)
    ends = starts + rng.integers(0, 70, 150)
    values = rng.normal(size=150)
    weights = rng.integers(0, 2, 150)
    p = EndTimePrior(keys, starts, ends)
    sums, counts = p.sum_count(values, weights)
    for i in range(len(keys)):
        use = (keys == keys[i]) & (keys[i] > 0) & (ends > starts) & (ends < starts[i])
        assert sums[i] == pytest.approx(np.sum(values[use] * weights[use]))
        assert counts[i] == weights[use].sum()


def test_empty_history_and_bad_shapes():
    assert EndTimePrior([], [], []).compute([], [], 1, 30).shape == (0,)
    with pytest.raises(ValueError):
        EndTimePrior([1], [1, 2], [3])
    with pytest.raises(ValueError):
        EndTimePrior([1], [1], [2]).compute([4], [1], 0, 30)
