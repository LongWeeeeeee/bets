"""Regression checks for the disjoint, label-available pro holdout."""
import numpy as np
import pytest

from base.tools.build_fresh_phase_holdout import fresh_rows


def rows(ids, starts, durations):
    return {"mid": np.array(ids), "ts": np.array(starts),
            "duration": np.array(durations), "heroes": np.zeros((len(ids), 10), dtype=int)}


def test_long_training_game_and_strict_boundary_cannot_leak():
    public = rows([1, 2], [100, 200], [10000, 1200])
    old_pro = rows([3], [5000], [1300])
    incoming = rows([4, 5, 6], [10100, 13700, 13701], [1200] * 3)
    selected, info = fresh_rows(incoming, public, old_pro, 3600)
    assert info["fresh_after_ts"] == 13700
    assert selected["mid"].tolist() == [6]
    assert info["not_after_cutoff"] == 2


def test_old_evaluation_ids_are_excluded_even_with_revised_start():
    public = rows([1], [100], [1200])
    old_pro = rows([2], [6000], [1200])
    incoming = rows([1, 2, 3, 4], [6001, 6002, 6000, 6003], [1200] * 4)
    selected, info = fresh_rows(incoming, public, old_pro)
    assert selected["mid"].tolist() == [4]
    assert info["fresh_after_ts"] == 6000
    assert info["public_id_overlap"] == info["previous_pro_id_overlap"] == 1


def test_reference_or_embargo_missing_is_rejected():
    valid = rows([1], [100], [1200])
    with pytest.raises(ValueError, match="nonempty"):
        fresh_rows(valid, rows([], [], []), valid)
    with pytest.raises(ValueError, match="non-negative"):
        fresh_rows(valid, valid, valid, -1)
