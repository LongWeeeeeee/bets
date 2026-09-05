"""Contracts for replacing the draft component without rebuilding a snapshot."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "refit_prematch_draft_component", ROOT / "base/tools/refit_prematch_draft_component.py"
)
assert SPEC and SPEC.loader
R = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = R
SPEC.loader.exec_module(R)


NAMES = ["draft_logit", "elo", "games", "draft_logit_x_elo_gap", "draft_logit_x_games_exp"]


def test_alignment_uses_mid_not_position_and_rejects_duplicate_contracts():
    heroes = np.arange(30, dtype=np.int32).reshape(3, 10)
    got = R.align_heroes(np.array([30, 10]), np.array([10, 20, 30]), heroes)
    assert np.array_equal(got, heroes[[2, 0]])
    try:
        R.align_heroes(np.array([10, 10]), np.array([10, 20, 30]), heroes)
    except ValueError as exc:
        assert "base matrix" in str(exc)
    else:
        raise AssertionError("duplicate matrix mids must fail")


def test_replace_draft_rebuilds_both_live_interactions_from_frozen_context_scale():
    X = np.array([[1.0, 0.2, -0.5, 9.0, 8.0], [-2.0, -0.4, 1.5, 7.0, 6.0]])
    out = R.replace_draft_columns(X, NAMES, np.array([0.7, -0.3]),
                                  np.array([0.1, 0.5]), np.array([0.2, 0.5]))
    np.testing.assert_allclose(out[:, 0], [0.7, -0.3])
    np.testing.assert_allclose(out[:, 3], [0.7 * 0.5, -0.3 * 1.5])
    np.testing.assert_allclose(out[:, 4], [0.7 * 0.0, -0.3 * 2.0])
    np.testing.assert_allclose(out[:, 1:3], X[:, 1:3])


def test_draft_logit_matches_live_index_rounding_before_log_odds():
    probability = np.array([0.500004, 0.506789, 0.9999999, 0.0])
    index = np.round((probability - 0.5) * 100.0, 3)
    restored = index / 100.0 + 0.5
    expected = np.log(np.maximum(restored, 1e-6) / np.maximum(1.0 - restored, 1e-6))
    np.testing.assert_array_equal(R.live_draft_logit(probability), expected)


class _Encoder:
    n_columns = 3


class _Classifier:
    n_features_in_ = 3


class _Model:
    def __init__(self, phase="all", classes=("dire", "radiant"), width=3):
        self.phase = phase
        self.classes_ = np.asarray(classes)
        self.encoder = _Encoder()
        self.classifier = _Classifier()
        self.encoder.n_columns = width
        self.classifier.n_features_in_ = width


def test_rejects_non_all_or_non_binary_draft_models_before_refit():
    for model, needle in ((_Model(phase="late"), "phase"),
                          (_Model(classes=("dire", "radiant", "no_marker")), "classes")):
        try:
            R.validate_all_draft_model(model)
        except ValueError as exc:
            assert needle in str(exc)
        else:
            raise AssertionError("wrong All model contract must fail")
    mismatched = _Model(width=3)
    mismatched.classifier.n_features_in_ = 4
    try:
        R.validate_all_draft_model(mismatched)
    except ValueError as exc:
        assert "width mismatch" in str(exc)
    else:
        raise AssertionError("width mismatch must fail")


def _weights():
    branches = {
        "full": R.BranchWeights(list(NAMES), np.zeros(5), np.ones(5), np.zeros(5), 0.0),
        "no_org": R.BranchWeights(list(NAMES[:-1]), np.arange(4.), np.arange(1., 5.), np.ones(4), 0.2),
        "pre_draft": R.BranchWeights(["elo", "games"], np.array([4., 5.]), np.ones(2), np.ones(2), -0.1),
        "rating_only": R.BranchWeights(["elo"], np.array([8.]), np.array([2.]), np.array([3.]), 0.4),
    }
    packed = R.pack_branches(branches)
    packed.update({"feature_names": np.asarray(NAMES), "ctx_mu": np.array([0.1, 0.2]),
                   "ctx_sd": np.array([1.0, 2.0])})
    return packed, branches


def test_branch_roundtrip_and_unaffected_branches_preserve_exact_weights():
    packed, original = _weights()
    restored = R.unpack_branches(packed)
    for name, branch in original.items():
        assert restored[name].cols == branch.cols
        np.testing.assert_array_equal(restored[name].mu, branch.mu)
        np.testing.assert_array_equal(restored[name].sd, branch.sd)
        np.testing.assert_array_equal(restored[name].coef, branch.coef)
        assert restored[name].intercept == branch.intercept
    assert not any(column in R.DRAFT_COLUMNS for column in restored["pre_draft"].cols)
    assert not any(column in R.DRAFT_COLUMNS for column in restored["rating_only"].cols)


def test_top_weights_are_the_full_branch_after_packing_contract():
    packed, _ = _weights()
    full = R.unpack_branches(packed)["full"]
    top = {"mu": full.mu[None], "sd": full.sd[None], "coef": full.coef[None],
           "intercept": np.asarray([full.intercept])}
    np.testing.assert_array_equal(top["mu"][0], full.mu)
    np.testing.assert_array_equal(top["sd"][0], full.sd)
    np.testing.assert_array_equal(top["coef"][0], full.coef)
    assert float(top["intercept"][0]) == full.intercept


def test_baseline_gate_requires_exact_top_weights_and_train_scaling():
    packed, _ = _weights()
    full = R.unpack_branches(packed)["full"]
    packed.update({"mu": full.mu[None], "sd": full.sd[None], "coef": full.coef[None],
                   "intercept": np.asarray([full.intercept])})
    X = np.array([[0., 1., 2., 3., 4.], [2., 3., 4., 5., 6.]])
    full.mu, full.sd = X.mean(0), X.std(0) + 1e-9
    packed.update(R.pack_branches({"full": full, **{name: branch for name, branch in R.unpack_branches(packed).items() if name != "full"}}))
    packed["mu"], packed["sd"] = full.mu[None], full.sd[None]
    got = R.baseline_identity_gate(X, NAMES, np.array([100, 101]), packed, 200, 1)
    assert got["train_rows"] == 2
    assert got["max_abs_mu"] == 0.0
    assert got["max_abs_sd"] == 0.0
    packed["coef"] = packed["coef"].copy()
    packed["coef"][0, 0] = 1.0
    try:
        R.baseline_identity_gate(X, NAMES, np.array([100, 101]), packed, 200, 1)
    except ValueError as exc:
        assert "top-level coef" in str(exc)
    else:
        raise AssertionError("top/full divergence must fail")


def test_branch_logit_rejects_nonfinite_result():
    branch = R.BranchWeights(["draft_logit"], np.array([0.0]), np.array([1.0]),
                             np.array([1e308]), 0.0)
    try:
        R.branch_logit(np.array([[1e308]]), ["draft_logit"], branch)
    except ValueError as exc:
        assert "non-finite" in str(exc)
    else:
        raise AssertionError("overflowing branch logit must fail")
