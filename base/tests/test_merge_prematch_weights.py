"""The stream merge may replace weights, but never rebuild or alter snapshots."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pytest


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("merge_prematch_weights", ROOT / "base/tools/merge_prematch_weights.py")
assert SPEC and SPEC.loader
M = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = M
SPEC.loader.exec_module(M)


def _arrays(*, replacement: bool, changed_ctx: bool = False):
    # full has two columns; pre_draft has one and must remain a valid frozen branch.
    full_mu = np.array([2.0, 3.0]) if replacement else np.array([0.0, 1.0])
    full_sd = np.array([1.5, 2.5]) if replacement else np.array([1.0, 2.0])
    full_coef = np.array([0.4, -0.6]) if replacement else np.array([0.1, -0.2])
    full_intercept = 0.7 if replacement else -0.1
    pre_mu = np.array([8.0]) if replacement else np.array([5.0])
    pre_sd = np.array([3.0]) if replacement else np.array([2.0])
    pre_coef = np.array([0.9]) if replacement else np.array([0.3])
    pre_intercept = -0.4 if replacement else 0.2
    return {
        "accounts": np.arange(20, dtype=np.float64).reshape(2, 10),
        "hero_wr30": np.array([[1.0, 0.5], [2.0, 0.6]]),
        "mu": full_mu[None], "sd": full_sd[None], "coef": full_coef[None],
        "intercept": np.array([full_intercept]),
        "ctx_mu": np.array([0.1 + (1.0 if changed_ctx else 0.0), 0.2]),
        "ctx_sd": np.array([0.3, 0.4]),
        "feature_names": np.array(["draft_logit", "elo"]),
        "branch_names": np.array(["full", "pre_draft"]),
        "branch_lens": np.array([2, 1], dtype=np.int64),
        "branch_cols": np.array(["draft_logit", "elo", "elo"]),
        "branch_mu": np.concatenate((full_mu, pre_mu)),
        "branch_sd": np.concatenate((full_sd, pre_sd)),
        "branch_coef": np.concatenate((full_coef, pre_coef)),
        "branch_intercept": np.array([full_intercept, pre_intercept]),
    }


def _write(path: Path, **arrays):
    np.savez_compressed(path, **arrays)
    return path


def _inputs(tmp_path, *, changed_ctx: bool = False):
    snapshot = _write(tmp_path / "snapshot.npz", **_arrays(replacement=False))
    weights = _write(tmp_path / "weights.npz", **_arrays(replacement=True, changed_ctx=changed_ctx))
    return snapshot, weights, tmp_path / "merged.npz", tmp_path / "report.json"


def test_success_replaces_only_weight_members_and_keeps_snapshot_members_byte_identical(tmp_path):
    snapshot, weights, output, report = _inputs(tmp_path)
    before = M.member_hashes(snapshot)
    wanted = M.member_hashes(weights)
    result = M.merge(snapshot, weights, output, report, M.sha256(snapshot))
    after = M.member_hashes(output)
    assert result["status"] == "PASS"
    assert report.exists()
    for member, digest in before.items():
        key = member.removesuffix(".npy")
        if key in M.REPLACE:
            assert after[member] == wanted[member]
        else:
            assert after[member] == digest
    with np.load(output) as merged, np.load(weights) as source:
        np.testing.assert_array_equal(merged["mu"], source["mu"])
        np.testing.assert_array_equal(merged["branch_coef"], source["branch_coef"])
        np.testing.assert_array_equal(merged["accounts"], _arrays(replacement=False)["accounts"])


def test_wrong_snapshot_sha_rejects_before_creating_outputs(tmp_path):
    snapshot, weights, output, report = _inputs(tmp_path)
    with pytest.raises(ValueError, match="snapshot identity changed"):
        M.merge(snapshot, weights, output, report, "0" * 64)
    assert not output.exists()
    assert not report.exists()


def test_changed_frozen_ctx_metadata_rejects_before_creating_outputs(tmp_path):
    snapshot, weights, output, report = _inputs(tmp_path, changed_ctx=True)
    with pytest.raises(ValueError, match="frozen metadata changed: ctx_mu"):
        M.merge(snapshot, weights, output, report, M.sha256(snapshot))
    assert not output.exists()
    assert not report.exists()
