import numpy as np
import pytest

from ELO.benchmark_probabilities import align, prior_map_context


def test_mid_join_reorders_and_rejects_missing_or_duplicate_maps():
    assert align(np.array([30, 10, 20]), np.array([20, 30])).tolist() == [2, 0]
    with pytest.raises(ValueError, match="missing"):
        align(np.array([10, 20]), np.array([30]))
    with pytest.raises(ValueError, match="duplicate"):
        align(np.array([10, 10]), np.array([10]))


def test_previous_map_tracks_team_across_side_swap_and_waits_for_finish():
    rich = {"mids": np.array([1, 2, 3, 4]), "ts": np.array([100, 200, 300, 400]),
            "sids": np.array([1, 1, 1, 1]), "teams": np.array([[10, 20], [20, 10], [10, 20], [20, 10]]),
            "wins": np.array([1, 0, 1, 0]), "durations": np.array([99, 100, 99, 90])}
    # Map 2: team 10 won but is now Dire. Map 3 starts exactly at map 2 end.
    assert prior_map_context(rich).tolist() == [0, -1, 0, -1]
    prefix = {k: v[:2] for k, v in rich.items()}
    assert np.array_equal(prior_map_context(prefix), prior_map_context(rich)[:2])
    rich["wins"][2:] = 0
    assert np.array_equal(prior_map_context(prefix), prior_map_context(rich)[:2])


def test_unknown_duration_or_changed_pair_does_not_invent_context():
    rich = {"mids": np.array([1, 2, 3]), "ts": np.array([100, 300, 500]),
            "sids": np.array([1, 1, 1]), "teams": np.array([[10, 20], [10, 20], [10, 30]]),
            "wins": np.array([1, 1, 0]), "durations": np.array([0, 90, 90])}
    assert prior_map_context(rich).tolist() == [0, 0, 0]
