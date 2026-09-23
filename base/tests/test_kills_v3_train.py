"""Regression checks for the offline kills-v3 research harness."""
import importlib.util
from pathlib import Path

import numpy as np
import pytest


PATH = Path(__file__).resolve().parents[1] / "tools/kills_v3_train.py"
SPEC = importlib.util.spec_from_file_location("kills_v3_train", PATH)
train = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(train)


def test_series_context_requires_same_team_and_strictly_earlier_end():
    starts = np.array([10, 20, 30, 40, 50])
    ends = np.array([19, 30, 35, 55, 58])
    series = np.array([7, 7, 7, 8, 7])
    teams = np.array([[11, 22], [22, 11], [11, 33], [11, 22], [11, 22]])
    y = np.zeros((5, 2, 8), np.float32)
    y[0, :, :3] = [[31, 20, 51], [20, 31, 51]]
    y[0, :, 7] = 1800
    y[1, :, :3] = [[40, 15, 55], [15, 40, 55]]
    y[1, :, 7] = 2000
    y[2, 0, :3] = [15, 20, 35]
    result = train.series_features(starts, ends, series, teams, y)
    assert result[0, 0, 0] == 1
    assert result[1, 1, 0] == 2  # team 11 moved to Dire
    assert result[1, 1, 4] == 31  # previous own kills, not current map or Radiant
    assert result[2, 0, 0] == 3  # game number counts starts; map 1's stats are excluded
    assert result[2, 0, 19] == 1
    assert result[4, 0, 0] == 4  # map 3 is a different series
    assert result[4, 0, 4] == 15  # latest completed map, same team


def test_test_stage_requires_selection_before_labels(tmp_path):
    with pytest.raises(RuntimeError, match="selection.json"):
        train.test_stage(tmp_path, {}, {}, False)
    with pytest.raises(RuntimeError, match="selection.json"):
        train.mark_test_start(tmp_path, False)
    (tmp_path / "selection.json").write_text("{}")
    train.mark_test_start(tmp_path, False)
    # a crash before test.json may retry only the byte-identical selection
    train.mark_test_start(tmp_path, False)
    marker = train.load_json(tmp_path / "test_started.json")
    assert marker["status"] == "retried_after_crash" and len(marker["attempts"]) == 2
    (tmp_path / "selection.json").write_text('{"changed": 1}')
    with pytest.raises(RuntimeError, match="another selection"):
        train.mark_test_start(tmp_path, False)
    (tmp_path / "selection.json").write_text("{}")
    (tmp_path / "test.json").write_text("{}")
    with pytest.raises(RuntimeError, match="evaluated once"):
        train.mark_test_start(tmp_path, False)


def test_selection_without_hashes_is_rejected_in_current_format(tmp_path):
    selection = {"primary": "rank0", "targets": {"side30": [{"id": "x"}]}}
    with pytest.raises(RuntimeError, match="lacks candidate hashes"):
        train.verify_selection_hashes(tmp_path, selection)


def test_orientation_independent_per_mid_loss():
    mids = np.array([1, 1, 2, 2])
    y = np.array([1, 0, 1, 1])
    p = np.array([.9, .2, .8, .7])
    perm = np.array([1, 0, 3, 2])
    common, a, b = train.paired_loss_invariant(y, p, y[perm], p[perm], mids, mids[perm])
    assert common == [1, 2]
    def per_mid(rows):
        return sum(-v*np.log(q) - (1-v)*np.log1p(-q) for v, q in rows)
    assert all(per_mid(a[mid]) == pytest.approx(per_mid(b[mid])) for mid in common)


def test_smoke_uses_synthetic_baseline_files(tmp_path):
    ours = {"mids": np.array([1, 1, 2, 2]), "y": np.array([1, 0, 1, 1]),
            "p": np.array([.8, .3, .9, .6]), "days": np.array([1, 1, 2, 2])}
    result = train.compare(ours, "side30", True, tmp_path)
    assert set(result) == {"E281_synthetic_standin", "E314_synthetic_standin"}
    assert all(item["delta_ll"] == pytest.approx(0) for item in result.values())


def test_derived_probabilities_are_monotone_and_bounded():
    for target, values, dispersion in (("side30", [10, 30, 50], .3),
                                       ("total55", [30, 55, 80], .2),
                                       ("lead_5_15", [-10, 0, 10], 5)):
        p = train.count_prob(target, values, dispersion)
        assert np.all((p > 0) & (p < 1))
        assert np.all(np.diff(p) > 0)


def test_windows_collapse_to_one_radiant_row_for_e314_pairing():
    """E-314 window files hold one radiant row per map (sides=0); ours hold two.
    Before the fix the multiset match [0, 1] != [y] returned zero paired maps."""
    ours = {"mids": np.array([1, 1, 2, 2, 3]), "sides": np.array([0, 1, 0, 1, 0]),
            "y": np.array([1, 0, 0, 1, 1]), "p": np.array([.7, .4, .2, .9, .6]),
            "days": np.array([5, 5, 6, 6, 7])}
    mine, inconsistent = train.collapse_to_radiant(ours)
    assert inconsistent == 0
    assert mine["mids"].tolist() == [1, 2]  # map 3 lacks the Dire row
    assert mine["y"].tolist() == [1, 0]
    assert np.allclose(mine["p"], [(.7 + 1 - .4) / 2, (.2 + 1 - .9) / 2])
    theirs = {"mids": np.array([2, 1]), "y": np.array([0, 1]), "p": np.array([.3, .6])}
    result = train.paired_bootstrap(mine, theirs, mine["mids"], mine["days"], reps=20)
    assert result["n_maps"] == 2
    assert result["rows_per_map_ours"] == result["rows_per_map_theirs"] == 1
    raw = train.paired_bootstrap(ours, theirs, ours["mids"], ours["days"], reps=20)
    assert raw["n_maps"] == 0  # the pre-fix path: two rows never match one


def test_selection_hashes_detect_candidate_change_after_selection(tmp_path):
    base = tmp_path / "candidates" / "select__side30__depth4"
    base.mkdir(parents=True)
    (base / "model.cbm").write_bytes(b"model-a")
    train.write_json(base / "record.json", {"id": "select__side30__depth4", "kind": "binary"})
    choice = {"id": "select__side30__depth4", **train.candidate_hashes(tmp_path, "select__side30__depth4")}
    selection = {"targets": {"side30": [choice]}}
    train.verify_selection_hashes(tmp_path, selection)
    (base / "model.cbm").write_bytes(b"model-b")
    with pytest.raises(RuntimeError, match="changed after selection"):
        train.verify_selection_hashes(tmp_path, selection)


def test_count_platt_is_fitted_on_logit_and_applied_consistently():
    rng = np.random.default_rng(0)
    prob = rng.uniform(.05, .95, 4000)
    y = (rng.uniform(size=4000) < prob).astype(int)
    cal = train.platt_fit(train.logit(prob), y)
    assert abs(cal["coef"] - 1) < .15 and abs(cal["intercept"]) < .15  # identity on a calibrated input
    out = train.platt_predict(cal, train.logit(prob))
    assert np.corrcoef(out, prob)[0, 1] > .999
    assert out.min() < .1 and out.max() > .9  # not squeezed into [sigma(b), sigma(a+b)]


def test_test_json_is_written_even_with_non_finite_values():
    value = {"a": float("nan"), "b": [np.float64(np.inf), np.int64(3)], "c": {"d": 1.5}}
    assert train.finite_json(value) == {"a": None, "b": [None, 3], "c": {"d": 1.5}}
