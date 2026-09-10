"""Regression cases for the offline binary phase-tuning boundary."""
import json

import joblib
import numpy as np
import pytest

from base.draft_features import KIND_PAIR, DraftFeatureEncoder
from base.tools import tune_draft_phase_models as tuner
from base.tools.tune_draft_phase_models import (
    _atomic_joblib,
    _full_artifact,
    binary_metrics,
    choose_recipe,
    development_splits,
    incumbent_recipe,
    paired_day_bootstrap,
    phase_rows_before_direction,
    predict_binary,
    _verify_done_output,
)
from base.train_draft_phase_models import fit_logistic


def _rows(n=20):
    rng = np.random.default_rng(17)
    heroes = np.stack([rng.choice(np.arange(1, 18), 10, replace=False) for _ in range(n)]).astype(np.int32)
    return {"heroes": heroes, "mid": np.arange(n, dtype=np.int64),
            "ts": 1_700_000_000 + np.arange(n, dtype=np.int64) * 5_000,
            "duration": np.full(n, 6_000, dtype=np.int32),
            "wins": np.resize(np.array([0, 1], dtype=np.int8), n),
            "early_nw": np.resize(np.array([0, 1, 2, 1], dtype=np.int8), n)}


def test_early_nw_splits_before_marker_drop_and_purges_both_boundaries():
    data = phase_rows_before_direction(_rows(), "early_nw")
    split = development_splits(data, "early_nw", 3600)
    # 60/20 boundaries are based on all 20 phase rows, including no_marker.
    assert split["raw_train_end"].item() == 12
    assert split["raw_test_start"].item() == 16
    # duration + embargo is 9600 seconds: final train and validation rows leak.
    assert 11 not in split["train"]
    assert 15 not in split["validation"]
    assert not np.any(data["early_nw"][split["train"]] == 2)
    assert not np.any(data["early_nw"][split["validation"]] == 2)
    assert not np.any(data["early_nw"][split["test"]] == 2)


def test_early_nw_incumbent_reads_direction_c_not_occurrence(tmp_path):
    directory = tmp_path / "early_nw" / "hero_role_position_pair"
    directory.mkdir(parents=True)
    (directory / "model.joblib").write_bytes(b"unused by this helper")
    (directory / "results.json").write_text(json.dumps({"selection": {
        "occurrence": {"selected_C": 0.0001}, "direction": {"selected_C": 0.003},
    }}))
    recipe, _ = incumbent_recipe(tmp_path, "early_nw")
    assert recipe["C"] == 0.003
    assert recipe["incumbent"] is True


def test_validation_tie_retains_incumbent_recipe():
    incumbent = {"recipe": {"design": "position", "C": .003, "incumbent": True}, "validation_log_loss": .61}
    challenger = {"recipe": {"design": "role", "C": .001, "incumbent": False}, "validation_log_loss": .61}
    assert choose_recipe([challenger, incumbent]) is incumbent


def test_day_bootstrap_is_paired_by_day_not_weighted_rows():
    # Day one has one +1 accuracy delta; day two has three zero-delta rows.
    ts = np.array([0, 86_400, 86_401, 86_402])
    y = np.array([1, 1, 0, 1], dtype=np.int8)
    baseline = np.array([.1, .9, .1, .9])
    candidate = np.array([.9, .9, .1, .9])
    first = paired_day_bootstrap(ts, y, baseline, candidate)
    second = paired_day_bootstrap(ts, y, baseline, candidate)
    assert first == second
    assert first["accuracy"]["days"] == 2
    assert first["accuracy"]["candidate_minus_baseline"] == .5


def test_probability_half_tie_matches_dire_first_argmax():
    # predict_proba columns are [Dire=0, Radiant=1], so .5 resolves to Dire.
    assert binary_metrics(np.array([0, 1]), np.array([.5, .5]))["accuracy"] == .5


def test_incumbent_full_artifact_rejects_label_unavailable_at_cutoff(tmp_path):
    rows = _rows()
    recipe = {"design": "hero_role_position_pair", "C": .003, "incumbent": True}
    # Index 14 finishes only 5,000 seconds before this cutoff; availability
    # requires duration + embargo = 9,600 seconds, so E260 reuse is unsafe.
    with pytest.raises(ValueError, match="cannot reuse incumbent"):
        _full_artifact(rows, "all", recipe, int(rows["ts"][15]), 3600, tmp_path, 100, tmp_path / "baseline.joblib")


def test_frozen_forward_manifest_is_rejected_before_loading_rows(tmp_path, monkeypatch):
    corpus = tmp_path / "frozen"
    corpus.mkdir()
    np.savez(corpus / "rows.npz", **_rows())
    (corpus / "manifest.json").write_text(json.dumps({"complete": True, "purpose": "unscored_frozen_forward_holdout"}))
    baseline = tmp_path / "baseline"
    for design, model_file in (("hero_role_position_pair", "model.joblib"), ("hero_role_pair", "evaluation_model.joblib")):
        directory = baseline / "early_win" / design
        directory.mkdir(parents=True)
        (directory / "results.json").write_text("{}")
        (directory / model_file).write_bytes(b"identity-only")
    called = False
    def must_not_load(_):
        nonlocal called
        called = True
        raise AssertionError("load_rows must not be called")
    monkeypatch.setattr(tuner, "load_rows", must_not_load)
    with pytest.raises(ValueError, match="frozen forward holdout"):
        tuner.main(["--corpus", str(corpus), "--baseline-dir", str(baseline), "--output-dir", str(tmp_path / "out"),
                     "--scratch", str(tmp_path / "scratch"), "--models", "early_win", "--fresh-after-ts", "1701000000"])
    assert called is False


def test_done_output_requires_exact_nonempty_phase_keys(tmp_path):
    with pytest.raises(ValueError, match="phase summaries"):
        _verify_done_output(tmp_path, {"identity": {"models": ["early_win"]}, "phases": {}})


def test_binary_artifact_survives_joblib_reload(tmp_path):
    rows = _rows(30)
    encoder = DraftFeatureEncoder.fit(rows["heroes"], KIND_PAIR, True, pair_min_support=1)
    classifier = fit_logistic(encoder.transform(rows["heroes"]), rows["wins"], .003, 300, "artifact-test")
    artifact = {"phase": "all", "encoder": encoder, "classifier": classifier, "metadata": {}}
    before = predict_binary(artifact, rows["heroes"], chunk_size=7)
    path = tmp_path / "binary.joblib"
    _atomic_joblib(artifact, path)
    after = predict_binary(joblib.load(path), rows["heroes"], chunk_size=7)
    np.testing.assert_allclose(after, before, atol=0, rtol=0)
