#!/usr/bin/env python3
"""Refit only prematch branches affected by a replacement draft model.

The input matrix is deliberately a finished live-path matrix.  This tool never
rebuilds the expensive prematch snapshot: it replaces ``draft_logit`` and its
two live interactions, then refits only branches that contain those columns.
The output is a *weights-only* NPZ.  It contains no account/hero snapshot and
must be merged into a separately verified snapshot by the deployment step.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DRAFT_COLUMNS = (
    "draft_logit",
    "draft_logit_x_elo_gap",
    "draft_logit_x_games_exp",
)
CONTEXT_COLUMNS = ("elo", "games")
DEFAULT_CUTOFF = 1774742400
DEFAULT_WINDOW_DAYS = 120


@dataclass
class BranchWeights:
    cols: list[str]
    mu: np.ndarray
    sd: np.ndarray
    coef: np.ndarray
    intercept: float


def _names(values: Iterable[Any]) -> list[str]:
    return [str(x) for x in values]


def _index(names: list[str], required: Iterable[str]) -> dict[str, int]:
    positions = {name: i for i, name in enumerate(names)}
    missing = [name for name in required if name not in positions]
    if missing:
        raise ValueError(f"matrix is missing required columns: {missing}")
    return positions


def _unique_positions(keys: np.ndarray, label: str) -> dict[int, int]:
    keys = np.asarray(keys, dtype=np.int64)
    if keys.ndim != 1 or len(np.unique(keys)) != len(keys):
        raise ValueError(f"{label} must have unique one-dimensional mids")
    return {int(key): i for i, key in enumerate(keys)}


def align_heroes(matrix_mids: np.ndarray, compact_mids: np.ndarray,
                compact_heroes: np.ndarray) -> np.ndarray:
    """Return compact heroes in the exact precomputed-matrix order.

    Position is not a join key.  A dict is used only after both inputs have
    been proved unique, so duplicate map IDs cannot silently choose a row.
    """
    matrix_mids = np.asarray(matrix_mids, dtype=np.int64)
    compact_heroes = np.asarray(compact_heroes)
    if compact_heroes.shape != (len(compact_mids), 10):
        raise ValueError("compact heroes must be (n, 10) and align with compact mids")
    pos = _unique_positions(compact_mids, "compact corpus")
    _unique_positions(matrix_mids, "base matrix")
    absent = [int(mid) for mid in matrix_mids if int(mid) not in pos]
    if absent:
        raise ValueError(f"{len(absent)} matrix mids are absent from compact corpus")
    return compact_heroes[np.asarray([pos[int(mid)] for mid in matrix_mids], dtype=np.int64)]


def draft_logits(model: Any, heroes: np.ndarray) -> np.ndarray:
    """Production convention, including the displayed-index rounding in live."""
    probability = np.asarray(model.predict_proba(heroes), dtype=np.float64)
    if probability.shape != (len(heroes), 2) or not np.isfinite(probability).all():
        raise ValueError("draft model must return finite (n, 2) probabilities")
    return live_draft_logit(probability[:, 1])


def validate_all_draft_model(model: Any) -> None:
    """Refit accepts only the binary map-winner All bundle, never a phase peer."""
    if getattr(model, "phase", None) != "all":
        raise ValueError(f"draft model phase must be 'all', got {getattr(model, 'phase', None)!r}")
    classes = [str(value) for value in np.asarray(getattr(model, "classes_", ())).tolist()]
    if classes != ["dire", "radiant"]:
        raise ValueError(f"All draft model classes must be ['dire', 'radiant'], got {classes!r}")
    encoder = getattr(model, "encoder", None)
    classifier = getattr(model, "classifier", None)
    width = getattr(encoder, "n_columns", None)
    classifier_width = getattr(classifier, "n_features_in_", None)
    if not isinstance(width, (int, np.integer)) or not isinstance(classifier_width, (int, np.integer)):
        raise ValueError("All draft model lacks encoder/classifier feature widths")
    if int(width) <= 0 or int(width) != int(classifier_width):
        raise ValueError(f"All draft model encoder/classifier width mismatch: {width!r} != {classifier_width!r}")


def live_draft_logit(probability: np.ndarray) -> np.ndarray:
    """Match ``win_model_veto``: round draft index first, then restore p.

    The live path converts a probability to a three-decimal percentage-point
    index before feeding it to prematch.  Training on unrounded probabilities
    would therefore fit a different feature than the service sees.
    """
    probability = np.asarray(probability, dtype=np.float64)
    if not np.isfinite(probability).all() or ((probability < 0) | (probability > 1)).any():
        raise ValueError("draft probabilities must be finite values in [0, 1]")
    index = np.round((probability - 0.5) * 100.0, 3)
    restored = index / 100.0 + 0.5
    return np.log(np.maximum(restored, 1e-6) / np.maximum(1.0 - restored, 1e-6))


def replace_draft_columns(X: np.ndarray, names: list[str], new_draft_logit: np.ndarray,
                          ctx_mu: np.ndarray, ctx_sd: np.ndarray) -> np.ndarray:
    """Replace the live draft value and its two standardized interactions."""
    X = np.asarray(X, dtype=np.float64)
    if X.ndim != 2 or len(X) != len(new_draft_logit):
        raise ValueError("matrix and draft logits have incompatible shapes")
    positions = _index(names, (*DRAFT_COLUMNS, *CONTEXT_COLUMNS))
    ctx_mu, ctx_sd = np.asarray(ctx_mu, dtype=np.float64), np.asarray(ctx_sd, dtype=np.float64)
    if (ctx_mu.shape != (2,) or ctx_sd.shape != (2,) or
            not np.isfinite(ctx_mu).all() or not np.isfinite(ctx_sd).all() or (ctx_sd <= 0).any()):
        raise ValueError("ctx_mu/ctx_sd must be two finite values with positive sd")
    result = X.copy()
    result[:, positions["draft_logit"]] = new_draft_logit
    context = np.abs(result[:, [positions[name] for name in CONTEXT_COLUMNS]])
    standardized = (context - ctx_mu) / ctx_sd
    result[:, positions["draft_logit_x_elo_gap"]] = new_draft_logit * standardized[:, 0]
    result[:, positions["draft_logit_x_games_exp"]] = new_draft_logit * standardized[:, 1]
    return result


def unpack_branches(weights: Any) -> dict[str, BranchWeights]:
    required = ("branch_names", "branch_lens", "branch_cols", "branch_mu", "branch_sd",
                "branch_coef", "branch_intercept")
    missing = [key for key in required if key not in weights]
    if missing:
        raise ValueError(f"weights artifact has no branch ladder: {missing}")
    names = _names(weights["branch_names"])
    lens = np.asarray(weights["branch_lens"], dtype=np.int64)
    cols = _names(weights["branch_cols"])
    if len(names) != len(lens) or int(lens.sum()) != len(cols):
        raise ValueError("corrupt branch column layout")
    mu, sd, coef = (np.asarray(weights[key], dtype=np.float64) for key in
                    ("branch_mu", "branch_sd", "branch_coef"))
    intercept = np.asarray(weights["branch_intercept"], dtype=np.float64)
    if not (len(mu) == len(sd) == len(coef) == len(cols) and len(intercept) == len(names)):
        raise ValueError("corrupt branch weight layout")
    branches: dict[str, BranchWeights] = {}
    offset = 0
    for i, name in enumerate(names):
        width = int(lens[i])
        if name in branches:
            raise ValueError(f"duplicate branch {name!r}")
        branches[name] = BranchWeights(cols[offset:offset + width], mu[offset:offset + width].copy(),
                                       sd[offset:offset + width].copy(), coef[offset:offset + width].copy(),
                                       float(intercept[i]))
        offset += width
    return branches


def pack_branches(branches: dict[str, BranchWeights]) -> dict[str, np.ndarray]:
    names = list(branches)
    return {
        "branch_names": np.asarray(names, dtype="<U32"),
        "branch_lens": np.asarray([len(branches[name].cols) for name in names], dtype=np.int64),
        "branch_cols": np.asarray([col for name in names for col in branches[name].cols], dtype="<U40"),
        "branch_mu": np.concatenate([branches[name].mu for name in names]),
        "branch_sd": np.concatenate([branches[name].sd for name in names]),
        "branch_coef": np.concatenate([branches[name].coef for name in names]),
        "branch_intercept": np.asarray([branches[name].intercept for name in names], dtype=np.float64),
    }


def fit_branch(X: np.ndarray, y: np.ndarray) -> BranchWeights:
    import warnings

    from sklearn.linear_model import LogisticRegression
    from sklearn.exceptions import ConvergenceWarning

    X, y = np.asarray(X, dtype=np.float64), np.asarray(y)
    if X.ndim != 2 or not np.isfinite(X).all():
        raise ValueError("branch fit matrix must be finite and two-dimensional")
    if len(X) == 0 or len(np.unique(y)) != 2:
        raise ValueError("branch fit needs both classes")
    mu, sd = X.mean(axis=0), X.std(axis=0)
    sd = np.where(sd < 1e-9, 1.0, sd)
    if not np.isfinite(mu).all() or not np.isfinite(sd).all() or (sd <= 0).any():
        raise ValueError("branch standardization is non-finite or non-positive")
    model = LogisticRegression(C=1.0, max_iter=5000)
    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=ConvergenceWarning)
        model.fit((X - mu) / sd, y)
    coef, intercept = model.coef_[0].copy(), float(model.intercept_[0])
    if not np.isfinite(coef).all() or not np.isfinite(intercept):
        raise ValueError("branch fit produced non-finite coefficients")
    return BranchWeights([], mu, sd, coef, intercept)


def branch_logit(X: np.ndarray, names: list[str], branch: BranchWeights) -> np.ndarray:
    positions = _index(names, branch.cols)
    values = X[:, [positions[col] for col in branch.cols]]
    with np.errstate(over="raise", invalid="raise", divide="raise"):
        try:
            standardized = (values - branch.mu) / branch.sd
            # ``@`` dispatches to BLAS.  On this macOS NumPy build it raises
            # spurious invalid flags despite a finite completed result.  This
            # non-BLAS reference path is deliberately used for the small
            # branch matrices and is checked below rather than suppressing a
            # warning from the fit or from another numerical operation.
            logit = np.einsum("ij,j->i", standardized, branch.coef, optimize=False) + branch.intercept
        except FloatingPointError as exc:
            raise ValueError("branch logit calculation became non-finite") from exc
    if not np.isfinite(logit).all():
        raise ValueError("branch logit calculation produced non-finite values")
    return logit


def metrics(y: np.ndarray, logit: np.ndarray) -> dict[str, float | int | None]:
    from sklearn.metrics import accuracy_score, log_loss, roc_auc_score

    p = 1.0 / (1.0 + np.exp(-np.clip(logit, -700, 700)))
    if len(y) == 0:
        return {"n": 0, "auc": None, "log_loss": None, "accuracy": None}
    return {"n": int(len(y)),
            "auc": (float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else None),
            "log_loss": float(log_loss(y, p, labels=[0, 1])),
            "accuracy": float(accuracy_score(y, p >= 0.5))}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as src:
        for part in iter(lambda: src.read(8 * 1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def atomic_npz(path: Path, arrays: dict[str, np.ndarray]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.stem + ".tmp.npz")
    np.savez_compressed(tmp, **arrays)
    os.replace(tmp, path)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def baseline_identity_gate(X: np.ndarray, names: list[str], ts: np.ndarray, weights: Any,
                           cutoff: int, window_days: int) -> dict[str, Any]:
    """Fail before changing draft data if the supplied baseline is not exact."""
    branches = unpack_branches(weights)
    if "full" not in branches:
        raise ValueError("weights artifact lacks full branch")
    full = branches["full"]
    if full.cols != names:
        raise ValueError("full branch columns differ from top-level feature_names")
    for key, branch_value in (("mu", full.mu), ("sd", full.sd), ("coef", full.coef)):
        top = np.asarray(weights[key], dtype=np.float64)
        if top.shape != (1, len(names)) or not np.array_equal(top[0], branch_value):
            raise ValueError(f"top-level {key} differs from packed full branch")
    top_intercept = np.asarray(weights["intercept"], dtype=np.float64)
    if top_intercept.shape != (1,) or not np.array_equal(top_intercept, [full.intercept]):
        raise ValueError("top-level intercept differs from packed full branch")
    train = (ts >= cutoff - int(window_days * 86400)) & (ts < cutoff)
    if not train.any():
        raise ValueError("baseline train window has no rows")
    # The deployed top-level 120-day weights were fit by the original parent
    # recipe, which persisted ``std + 1e-9``.  Retain that exact baseline
    # contract here; branch refits use their own documented branch recipe.
    expected_mu = X[train].mean(axis=0)
    expected_sd = X[train].std(axis=0) + 1e-9
    deviations = {
        "train_rows": int(train.sum()),
        "max_abs_mu": float(np.max(np.abs(expected_mu - full.mu))),
        "max_abs_sd": float(np.max(np.abs(expected_sd - full.sd))),
        "top_weights_equal_full_branch": True,
        "full_columns_equal_feature_names": True,
        "top_sd_recipe": "np.std + 1e-9 (legacy parent baseline)",
    }
    if deviations["max_abs_mu"] > 1e-8 or deviations["max_abs_sd"] > 1e-8:
        raise ValueError("base matrix train-window scaling differs from supplied top weights")
    return deviations


def refit(*, matrix_path: Path, weights_path: Path, compact_path: Path, public_corpus_path: Path,
          model_path: Path, output_path: Path, report_path: Path, cutoff: int = DEFAULT_CUTOFF,
          window_days: int = DEFAULT_WINDOW_DAYS) -> dict[str, Any]:
    """Perform one bounded, leakage-aware refit and write weights/report atomically."""
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    inputs = (matrix_path, weights_path, compact_path, public_corpus_path, model_path)
    resolved_inputs = {path.resolve() for path in inputs}
    if output_path.resolve() in resolved_inputs or report_path.resolve() in resolved_inputs or output_path.resolve() == report_path.resolve():
        raise ValueError("output and report must be distinct from every input")
    if output_path.exists() or report_path.exists():
        raise FileExistsError("refusing to overwrite an existing completed output or report")
    matrix = np.load(matrix_path, allow_pickle=True)
    weights = np.load(weights_path, allow_pickle=True)
    compact = np.load(compact_path, allow_pickle=False)
    public = np.load(public_corpus_path, allow_pickle=False)
    X_old = np.asarray(matrix["X"], dtype=np.float64)
    y, ts, mids = (np.asarray(matrix[key], dtype=np.int64) for key in ("y", "ts", "mids"))
    names = _names(matrix["names"])
    if X_old.shape != (len(y), len(names)) or len(ts) != len(y) or len(mids) != len(y):
        raise ValueError("base matrix arrays have incompatible shapes")
    if not np.isfinite(X_old).all() or not np.isin(y, [0, 1]).all():
        raise ValueError("base matrix has non-finite values or non-binary targets")
    if "ctx_mu" not in weights or "ctx_sd" not in weights or "feature_names" not in weights:
        raise ValueError("weights artifact lacks prematch scaling metadata")
    if _names(weights["feature_names"]) != names:
        raise ValueError("base matrix feature order does not match weights artifact")
    baseline = baseline_identity_gate(X_old, names, ts, weights, cutoff, window_days)
    heroes = align_heroes(mids, compact["mids"], compact["heroes"])
    model = joblib.load(model_path)
    validate_all_draft_model(model)
    X_new = replace_draft_columns(X_old, names, draft_logits(model, heroes),
                                  weights["ctx_mu"], weights["ctx_sd"])
    public_mids = np.asarray(public["mid"], dtype=np.int64)
    if public_mids.ndim != 1:
        raise ValueError("public corpus mids must be one-dimensional")
    shared = np.isin(mids, public_mids)
    window_seconds = int(window_days * 86400)
    fit_mask = (ts >= cutoff - window_seconds) & (ts < cutoff) & ~shared
    eval_mask = (ts >= cutoff) & ~shared
    if not fit_mask.any() or not eval_mask.any():
        raise ValueError("no non-public rows remain for fit or evaluation")

    branches = unpack_branches(weights)
    if "full" not in branches:
        raise ValueError("weights artifact lacks full branch")
    old_branches = {name: BranchWeights(list(branch.cols), branch.mu.copy(), branch.sd.copy(),
                                        branch.coef.copy(), branch.intercept)
                    for name, branch in branches.items()}
    report_branches: dict[str, Any] = {}
    for name, branch in branches.items():
        affected = any(column in DRAFT_COLUMNS for column in branch.cols)
        old = old_branches[name]
        entry: dict[str, Any] = {
            "affected": affected,
            "columns": list(branch.cols),
            "old": metrics(y[eval_mask], branch_logit(X_old[eval_mask], names, old)),
            "frozen_parent": metrics(y[eval_mask], branch_logit(X_new[eval_mask], names, old)),
        }
        if affected:
            positions = _index(names, branch.cols)
            trained = fit_branch(X_new[fit_mask][:, [positions[col] for col in branch.cols]], y[fit_mask])
            trained.cols = list(branch.cols)
            branches[name] = trained
            entry["new"] = metrics(y[eval_mask], branch_logit(X_new[eval_mask], names, trained))
        else:
            entry["new"] = entry["old"]
        report_branches[name] = entry

    full = branches["full"]
    if full.cols != names:
        raise ValueError("full branch must have exactly the top-level feature order")
    arrays: dict[str, np.ndarray] = {
        "mu": full.mu[None, :], "sd": full.sd[None, :], "coef": full.coef[None, :],
        "intercept": np.asarray([full.intercept], dtype=np.float64),
        "ctx_mu": np.asarray(weights["ctx_mu"], dtype=np.float64),
        "ctx_sd": np.asarray(weights["ctx_sd"], dtype=np.float64),
        "feature_names": np.asarray(names, dtype="<U40"),
    }
    arrays.update(pack_branches(branches))
    atomic_npz(output_path, arrays)
    report = {
        "schema_version": 1,
        "kind": "prematch_draft_component_refit",
        "inputs": {"matrix": str(matrix_path), "weights": str(weights_path), "compact": str(compact_path),
                   "public_corpus": str(public_corpus_path), "draft_model": str(model_path)},
        "sha256": {"matrix": sha256(matrix_path), "weights": sha256(weights_path),
                   "draft_model": sha256(model_path)},
        "output": str(output_path), "cutoff": int(cutoff), "window_days": int(window_days),
        "rows": {"matrix": int(len(y)), "fit": int(fit_mask.sum()), "evaluation": int(eval_mask.sum()),
                 "shared_public": int(shared.sum()), "shared_public_excluded_from_fit": int((shared & (ts < cutoff) & (ts >= cutoff-window_seconds)).sum()),
                 "shared_public_excluded_from_evaluation": int((shared & (ts >= cutoff)).sum())},
        "baseline_identity": baseline,
        "draft_columns_recomputed": list(DRAFT_COLUMNS), "ctx_scaling_preserved": True,
        "snapshot_rebuilt": False, "top_weights_equal_full_branch": True,
        "limitations": [
            "historical diagnostic only: the replacement public draft model was fit through 2026-09-04",
            "existing calibration tables and decision thresholds were not re-evaluated by this refit",
        ],
        "branches": report_branches,
    }
    atomic_json(report_path, report)
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--weights", type=Path, required=True)
    parser.add_argument("--compact", type=Path, required=True)
    parser.add_argument("--public-corpus", type=Path, required=True,
                        help="canonical public rows.npz; shared mids are excluded")
    parser.add_argument("--draft-model", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new compact weights-only NPZ")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--cutoff", type=int, default=DEFAULT_CUTOFF)
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    report = refit(matrix_path=args.matrix, weights_path=args.weights, compact_path=args.compact,
                   public_corpus_path=args.public_corpus, model_path=args.draft_model,
                   output_path=args.output, report_path=args.report, cutoff=args.cutoff,
                   window_days=args.window_days)
    print(json.dumps({"output": report["output"], "rows": report["rows"],
                      "affected": [name for name, row in report["branches"].items() if row["affected"]]},
                     ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
