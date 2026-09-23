"""Contract checks for the kills v3 serving-lag sensitivity tool."""
import numpy as np
import pytest

from base.tools import kills_v3_delay_eval as de
from base.tools.kills_v3_train import series_features


def _pair():
    n = 3
    base = {"mids": np.arange(n), "starts": np.array([0, 5000, 9000]), "ends": np.array([1800, 6800, 10800]),
            "series_ids": np.array([7, 7, 7]), "split": np.array([1, 1, 1], np.int8),
            "team_ids": np.array([[1, 2], [2, 1], [1, 2]]), "y": np.full((n, 2, 8), np.nan, np.float32),
            "X": np.zeros((n, 2, 3), np.float32)}
    meta = {"feature_names": ["a", "b", "c"], "parameters": {"visibility_delay": 0}}
    delayed = {k: v.copy() for k, v in base.items()}
    dmeta = {"feature_names": ["a", "b", "c"], "parameters": {"visibility_delay": 1200}}
    return base, meta, delayed, dmeta


def test_check_pair_rejects_wrong_delay_and_changed_labels():
    base, meta, delayed, dmeta = _pair()
    de.check_pair(base, meta, delayed, dmeta, 1200)
    with pytest.raises(ValueError, match="visibility_delay"):
        de.check_pair(base, meta, delayed, dmeta, 600)
    delayed["y"][1, 0, 0] = 30
    with pytest.raises(ValueError, match="y differs"):
        de.check_pair(base, meta, delayed, dmeta, 1200)


def test_delayed_series_context_hides_previous_map_inside_delay():
    # Map 1 ends 600 s before map 2 starts: visible without delay, hidden with 1200 s.
    starts, ends = np.array([0, 2400]), np.array([1800, 4200])
    sids, teams = np.array([7, 7]), np.array([[1, 2], [2, 1]])
    y = np.zeros((2, 2, 8), np.float32)
    y[0, :, 0] = (20, 25)
    y[0, :, 2] = 45
    y[0, :, 7] = 1800
    now = series_features(starts, ends, sids, teams, y)
    lag = series_features(starts, ends + 1200, sids, teams, y)
    assert now[1, 0, 0] == lag[1, 0, 0] == 2  # game number does not depend on visibility
    assert now[1, 0, 1] == 45 and now[1, 0, 4] == 25  # own team 2 was side 1 on map 1
    assert np.isnan(lag[1, 0, 1]) and np.isnan(lag[1, 0, 4])


def test_test_split_refused_without_harness_marker(tmp_path):
    (tmp_path / "selection.json").write_text("{}")
    rc = de.main(["--out", str(tmp_path), "--dataset", "x.npz", "--delayed-dataset", "y.npz",
                  "--delay", "1200", "--split", "test", "--output", str(tmp_path / "r.json")])
    assert rc == 2
    assert not (tmp_path / "r.json").exists()
