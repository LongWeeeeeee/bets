import json
from types import SimpleNamespace

import numpy as np
import pytest

from base.team_laning_model import (
    CLASS_NAMES, TeamLaningModel, team_features, team_labels, temperature_scale,
)
from scripts.ops.train_team_laning_model import partition_team, run


def test_target_and_feature_contract_excludes_same_map_outcomes():
    heroes = np.arange(1, 11).reshape(1, 10)
    history = np.arange(120, dtype=float).reshape(1, 10, 12)
    frame = team_features(heroes, history)
    assert CLASS_NAMES == ("dire", "tie", "radiant")
    np.testing.assert_array_equal(team_labels(np.array([-1.0, 0.0, 1.0])), [0, 1, 2])
    assert frame.shape == (1, 190)
    assert list(frame.columns[:10]) == [f"hero_{slot}" for slot in range(10)]
    assert frame["history_9_11"].iloc[0] == 119
    assert frame["radiant_minus_dire_role_0_0"].iloc[0] == -60
    assert not any("target" in column or "label" in column or "outcome" in column
                   for column in frame.columns)
    with pytest.raises(ValueError, match="finite"):
        team_labels([np.nan])
    with pytest.raises(ValueError, match="shape"):
        team_features(heroes, history[:, :, :6])


def test_metadata_autoload_and_temperature_roundtrip(tmp_path):
    from catboost import CatBoostClassifier

    heroes = np.vstack([np.roll(np.arange(1, 11), row % 10) for row in range(18)])
    history = np.broadcast_to(np.arange(18)[:, None, None], (18, 10, 12)).astype(float)
    features = team_features(heroes, history)
    model = CatBoostClassifier(iterations=2, depth=2, verbose=False, allow_writing_files=False,
        metadata={"team_laning_feature_set": "team_history_recent_v1",
                  "team_laning_temperature": "2", "team_laning_history_delay_seconds": "3600",
                  "team_laning_recent_window_seconds": "2592000"})
    model.fit(features, np.tile(np.arange(3), 6), cat_features=[f"hero_{i}" for i in range(10)])
    model.save_model(str(tmp_path / "team.cbm"))
    loaded = TeamLaningModel.load(tmp_path)
    assert loaded.with_history
    assert loaded.history_config == {"availability_delay_seconds": 3600.0,
                                     "recent_window_seconds": 2592000.0}
    raw = model.predict_proba(features.iloc[:3], thread_count=1)
    np.testing.assert_allclose(loaded.predict_proba(heroes[:3], history[:3]),
                               temperature_scale(raw, 2))
    with pytest.raises(ValueError, match="requires causal history"):
        loaded.predict_proba(heroes[:1])


def _write_baselines(root, corpus, v1_rows, v2_rows):
    v1, v2 = root / "v1", root / "v2"
    v1.mkdir()
    v2.mkdir()
    np.savez(v1 / "selected_rows.npz", indices=v1_rows, mid=corpus["mid"][v1_rows])
    np.savez(v2 / "reserved_rows.npz", indices=v2_rows, mid=corpus["mid"][v2_rows])
    np.savez(v2 / "inputs.npz", mid=corpus["mid"][v2_rows],
             hc=np.zeros((len(v2_rows), 10, 12), dtype=np.float32))
    (v1 / "summary.json").write_text(json.dumps({"split": {"sampled_maps": [100, 100, 100]}}))
    (v2 / "summary.json").write_text(json.dumps({"split": {"sampled_maps": [200, 100, 100]}}))
    (v2 / "plan.json").write_text(json.dumps({"split": {"cuts_unix": [500 * 3600, 800 * 3600]}}))
    return v1, v2


def test_partition_excludes_both_old_test_sets(tmp_path):
    n = 1200
    corpus = {"mid": np.arange(n) + 1, "ts": np.arange(n) * 3600,
              "team_nw10": np.resize(np.array([-1.0, 0.0, 1.0]), n)}
    v1_rows = np.r_[np.arange(200), np.arange(800, 900)]
    v2_rows = np.r_[np.arange(300), np.arange(900, 1000)]
    v1, v2 = _write_baselines(tmp_path, corpus, v1_rows, v2_rows)
    rows, report = partition_team(corpus, v1, v2, 200, 100)
    test = rows[300:]
    old = np.r_[corpus["mid"][800:900], corpus["mid"][900:1000]]
    assert len(test) == 100
    assert not np.intersect1d(corpus["mid"][test], old).size
    assert report["reused_v2_sampled_maps"] == [200, 100]


def test_tiny_end_to_end_seals_selection_before_confirmation(tmp_path):
    n = 1200
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    target = np.resize(np.array([-120.0, 0.0, 140.0]), n)
    corpus = {"mid": np.arange(n) + 1, "ts": np.arange(n) * 3600,
              "duration": np.full(n, 600),
              "heroes": np.vstack([np.roll(np.arange(1, 11), row % 10) for row in range(n)]),
              "accounts": np.tile(np.arange(101, 111), (n, 1)),
              "lane_labels": np.tile(np.array([0, 2, 4]), (n, 1)), "team_nw10": target}
    np.savez(corpus_dir / "rows.npz", **corpus)
    (corpus_dir / "manifest.json").write_text(json.dumps({"complete": True, "rows": n}))
    v1_rows = np.r_[np.arange(200), np.arange(800, 900)]
    v2_rows = np.r_[np.arange(300), np.arange(900, 1000)]
    v1, v2 = _write_baselines(tmp_path, corpus, v1_rows, v2_rows)
    out = tmp_path / "out"
    run(SimpleNamespace(corpus=corpus_dir, v1_model=v1, v2_model=v2, output_dir=out,
                        train_maps=200, eval_maps=100, iterations=2, threads=1))
    summary = json.loads((out / "summary.json").read_text())
    assert summary["complete"] and summary["held_out_test"]
    assert summary["reload_max_delta"] < 1e-12
    assert set(summary["test"]) == {"draft", "history", "selected_calibrated", "frequency"}
    assert (out / "selection.json").stat().st_mtime_ns <= (out / "test_predictions.npz").stat().st_mtime_ns
    with np.load(out / "test_predictions.npz") as test:
        assert not np.intersect1d(test["mid"], np.arange(801, 1001)).size
        assert test["selected_calibrated"].shape == (100, 3)
    with np.load(out / "verification_probe.npz") as probe:
        assert probe["schema_version"] == 1
        assert tuple(probe["class_order"]) == CLASS_NAMES
        assert probe["heroes"].shape == (64, 10)
        assert probe["history"].shape == (64, 10, 12)
        assert probe["probabilities"].shape == (64, 3)
    model = TeamLaningModel.load(out / "selected")
    assert model.predict_proba(np.arange(1, 11).reshape(1, 10), np.zeros((1, 10, 12))).shape == (1, 3)
