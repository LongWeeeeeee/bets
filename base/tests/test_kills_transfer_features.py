"""Regression cases for public/pro leakage and two-sided kills targets."""
import numpy as np
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kills_transfer_features import causal_pro_context, profile_features, public_profiles, swap, temporal_splits
from train_kills_transfer import collapse, targets
from kills_public_transfer import causal_total_targets, public_design


def rows(n=4):
    stats = np.empty((n, 10, 6), dtype=np.float32)
    stats[:] = [6, 7, 8, 12000, 450, 400]
    heroes = np.tile(np.arange(140, 150), (n, 1))
    return dict(mids=np.arange(1, n+1), ts=np.arange(n)*1000+1000,
                ends=np.arange(n)*1000+1500, durations=np.full(n, 500), heroes=heroes,
                accounts=np.tile(np.arange(1, 11), (n, 1)), teams=np.tile([21, 22], (n, 1)),
                sids=np.zeros(n, dtype=int), stats=stats)


def test_running_match_cannot_enter_history_or_its_own_features():
    data = rows()
    data["ts"] = np.array([1000, 1010, 2000, 3000])
    data["ends"] = np.array([1500, 1600, 2500, 3500])
    before, names = causal_pro_context(data)
    i = names.index("team_games")
    assert np.array_equal(before[:, 0, i], [0, 0, 2, 3])
    data["stats"][1, :, 0] = 500
    after, _ = causal_pro_context(data)
    np.testing.assert_allclose(before[:2], after[:2], equal_nan=True)
    assert before[2, 0, 18] != after[2, 0, 18]
    # Exact end=start is still unavailable.
    data["ends"][0] = 2000
    exact, _ = causal_pro_context(data)
    assert exact[2, 0, i] == 1


def test_profiles_are_role_relative_scale_invariant_and_frozen_before_cutoff():
    data = rows()
    before = public_profiles(data, 2500)
    assert before["relative_counts"][140*5, 0] == 1
    scaled = {key: value.copy() for key, value in data.items()}
    scaled["stats"] *= 3
    same = public_profiles(scaled, 2500)
    np.testing.assert_allclose(before["relative"], same["relative"], atol=1e-12)
    data["stats"][1:] = 100000
    future = public_profiles(data, 2500)
    np.testing.assert_allclose(before["relative"], future["relative"])
    unknown = data["heroes"][:1] + 1000
    values, _ = profile_features(unknown, before)
    assert np.all(values == 0)


def test_boundary_maps_and_cross_boundary_series_are_excluded():
    data = rows(6)
    data["sids"] = np.array([0, 17, 17, 0, 18, 18])
    data["ends"][3] = 5050
    split = temporal_splits(data, train_from=1000, val_from=3000, test_from=5000)
    # Series 17 spans train and validation; map 4 ends inside test.
    np.testing.assert_array_equal(split, [0, -1, -1, -1, 2, 2])


def test_side_labels_are_not_complements_and_no_middle_is_removed():
    data = rows(3)
    data["stats"][0, :, 0] = 8       # both sides 40: both true
    data["stats"][1, :, 0] = 2       # both sides 10: both false
    data["stats"][2, :, 0] = 5.4     # total 54, side 27: negative, retained
    np.testing.assert_array_equal(targets(data["stats"], "side"), [1, 0, 0, 1, 0, 0])
    np.testing.assert_array_equal(targets(data["stats"], "total"), [1, 0, 0])
    raw = np.array([.2, .5, .6, .9])
    np.testing.assert_allclose(collapse(raw, 2, "total"), [.4, .7])
    np.testing.assert_allclose(collapse(raw, 2, "side"), raw)
    np.testing.assert_array_equal(swap(swap(data["heroes"])), data["heroes"])


def test_pro_context_rejects_duplicate_or_nonchronological_maps():
    data = rows()
    data["mids"][1] = data["mids"][0]
    with pytest.raises(ValueError, match="chronological and unique"):
        causal_pro_context(data)


def test_public_normalization_uses_same_past_only_rule_across_periods():
    starts = np.array([100, 604900, 1209700, 1814500])
    ends = starts+100
    totals = np.array([20., 40., 60., 80.])
    target, reference, _ = causal_total_targets(totals, starts, ends, minimum=1)
    assert np.isnan(target[0])
    np.testing.assert_allclose(reference[1:], [20, 30, 40])
    np.testing.assert_allclose(target[1:], [2, 2, 2])
    totals[-1] = 900
    _, after, _ = causal_total_targets(totals, starts, ends, minimum=1)
    np.testing.assert_allclose(reference, after, equal_nan=True)
    scaled, _, _ = causal_total_targets(totals*3, starts, ends, minimum=1)
    changed, _, _ = causal_total_targets(totals, starts, ends, minimum=1)
    np.testing.assert_allclose(scaled, changed, equal_nan=True)


def test_batched_public_encoding_matches_full_matrix_with_side_signs():
    from draft_features import DraftFeatureEncoder, KIND_PAIR
    heroes = np.concatenate([rows(5)["heroes"], swap(rows(5)["heroes"])])
    for signed in (False, True):
        encoder = DraftFeatureEncoder.fit(heroes, KIND_PAIR, signed=signed, pair_min_support=1)
        full = encoder.transform(heroes).astype(np.float32)
        batch = public_design(encoder, heroes, batch_size=3)
        assert batch.dtype == np.float32
        assert (batch != full).nnz == 0


def test_training_smoke_saves_reloadable_models_and_two_side_predictions(tmp_path, monkeypatch):
    """Exercise every arm and serialization on a small chronological corpus."""
    import argparse
    import json
    import joblib
    import train_kills_transfer as trainer
    from catboost import CatBoostClassifier
    from kills_transfer_data import timestamp
    from threadpoolctl import threadpool_limits

    rng = np.random.default_rng(19)

    def corpus(n, start, before, first_id):
        data = rows(n)
        data["mids"] += first_id
        data["ts"] = np.linspace(timestamp(start), timestamp(before)-3600, n).astype(np.int64)
        data["ends"] = data["ts"]+1800
        data["durations"][:] = 1800
        data["heroes"] = np.stack([rng.choice(np.arange(1, 31), 10, replace=False) for _ in range(n)])
        data["stats"][:, :, 0] = rng.poisson(5.7, (n, 10))
        data["stats"][:, :, 1] = rng.poisson(6.0, (n, 10))
        return data

    public = corpus(1000, "2026-03-01", "2026-06-01", 10000)
    pro = corpus(1000, "2026-06-01", "2026-09-01", 20000)
    paths = {}
    for name in ("public", "pro"):
        path = tmp_path/name
        path.mkdir()
        (path/"summary.json").write_text("{}")
        paths[name] = str(path)
    monkeypatch.setattr(trainer, "load_rows", lambda path, **kwargs: public if path == paths["public"] else pro)
    out = tmp_path/"output"
    args = argparse.Namespace(public_rows=paths["public"], pro_rows=paths["pro"], output_dir=str(out),
                              threads=1, train_from="2026-06-01", val_from="2026-08-01",
                              test_from="2026-08-20", smoke=True)
    with threadpool_limits(limits=1):
        trainer.train(args)
    report = json.loads((out/"summary.json").read_text())
    assert report["smoke"] is True
    assert min(report["validation_partitions"][k] for k in
               ("early_stop_maps", "calibration_maps", "selection_maps")) >= 40
    cached = np.load(out/"pro_inputs.npz")
    at = np.flatnonzero(cached["split"] == 2)
    profiles = joblib.load(out/"profiles.joblib")
    for target in ("total", "side"):
        selected = report["targets"][target]["chosen_on_validation"]
        model = CatBoostClassifier()
        model.load_model(str(out/f"{target}_{selected}.cbm"))
        cal = joblib.load(out/f"{target}_{selected}_calibration.joblib")
        frame = trainer.feature_frame(cached["heroes"][at], cached["context"][at],
                                      cached["context_names"], cached["public_scores"][at], profiles, selected)
        raw = trainer.collapse(model.predict_proba(frame)[:, 1], len(at), target)
        predictions = np.load(out/f"{target}_test_predictions.npz")
        assert len(predictions["y"]) == len(at)*(2 if target == "side" else 1)
        np.testing.assert_allclose(trainer.calibrate_predict(cal, raw), predictions[selected], atol=1e-12)
