"""Regression cases for the phase definitions and leakage-safe training path."""
import json

import joblib
import numpy as np
import pytest

from base.draft_phase_model import DraftPhaseModel
from base.train_draft_phase_models import (
    chronological_cuts, csr_view, disk_design, main, phase_rows,
)
from base.draft_features import DraftFeatureEncoder, KIND_POSITION_PAIR


def corpus(n=160):
    rng = np.random.default_rng(59)
    heroes = np.stack([rng.choice(np.arange(1, 17), 10, replace=False) for _ in range(n)])
    return dict(heroes=heroes.astype(np.int32), mid=np.arange(n, dtype=np.int64)+1,
                ts=1700000000+np.arange(n, dtype=np.int64)*60,
                duration=np.resize(np.asarray([1200, 1500, 2040, 2041, 2160, 2700]), n),
                wins=np.resize(np.asarray([0, 1, 0, 1, 1], dtype=np.int8), n),
                early_nw=np.resize(np.asarray([0, 1, 2, 1, 0, 2, -1], dtype=np.int8), n))


def test_requested_duration_boundaries_and_unknown_marker():
    rows = corpus(7)
    rows["duration"] = np.asarray([1199, 1200, 2039, 2040, 2041, 2159, 2160])
    assert phase_rows(rows, "all")[0]["mid"].tolist() == [2, 3, 4, 5, 6, 7]
    assert phase_rows(rows, "early_win")[0]["mid"].tolist() == [2, 3, 4]
    assert phase_rows(rows, "late")[0]["mid"].tolist() == [7]
    nw, key = phase_rows(rows, "early_nw")
    assert 2 in nw[key] and -1 not in nw[key]


def test_timestamp_ties_do_not_cross_splits():
    ts = np.repeat(np.arange(10), 4)
    a, b = chronological_cuts(ts)
    assert ts[a-1] < ts[a] and ts[b-1] < ts[b]
    with pytest.raises(ValueError):
        chronological_cuts(np.ones(30))


def test_disk_csr_and_contiguous_views_match_encoder(tmp_path):
    heroes = corpus(30)["heroes"]
    enc = DraftFeatureEncoder.fit(heroes[:20], KIND_POSITION_PAIR, True, 1)
    expected = enc.transform(heroes)
    disk = disk_design(enc, heroes, tmp_path, chunk_size=7)
    assert np.array_equal(disk.toarray(), expected.toarray())
    view = csr_view(disk, 7, 21)
    assert np.array_equal(view.toarray(), expected[7:21].toarray())
    assert np.shares_memory(view.data, disk.data)


def test_four_phase_training_and_factored_early_probabilities(tmp_path):
    rows = corpus(180)
    source = tmp_path / "rows.npz"
    np.savez(source, **rows)
    out = tmp_path / "models"
    main(["--corpus", str(source), "--output-dir", str(out),
          "--scratch", str(tmp_path/"scratch"), "--c-grid", "0.01",
          "--pair-min-support", "1", "--max-iter", "300", "--skip-baseline"])
    summary = json.loads((out/"summary.json").read_text())
    assert summary["status"] == "complete"
    for phase in ("early_nw", "late", "all", "early_win"):
        path = out/phase/KIND_POSITION_PAIR
        report = json.loads((path/"results.json").read_text())
        assert report["evaluation_artifact"]["held_out_test"] is True
        assert report["final_artifact"]["held_out_test"] is False
        assert report["final_artifact"]["fit_rows"] == report["counts"]["all"]
        model = joblib.load(path/"model.joblib")
        p = model.predict_proba(rows["heroes"][:10], chunk_size=3)
        np.testing.assert_allclose(p.sum(axis=1), 1)
        assert np.all((p >= 0) & (p <= 1))
        if phase == "early_nw":
            assert p.shape == (10, 3)
            q = model.occurrence_classifier.predict_proba(
                model.occurrence_encoder.transform(rows["heroes"][:10]))[:, 1]
            np.testing.assert_allclose(p[:, 2], 1-q)
            assert report["counts"]["all"] > report["selection"]["direction"]["honest_fit_rows"]
