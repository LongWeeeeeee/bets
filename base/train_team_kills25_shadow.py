"""Train the non-sending team-kills>=27 shadow logistic artifact."""

from __future__ import annotations

import argparse
import json
import math
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import sklearn
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler

from team_kills25_shadow import (
    FEATURE_NAMES,
    FEATURE_SCHEMA_HASH,
    ODDS,
    SCHEMA_VERSION,
    TARGET_KILLS,
)


SEED = 20260731


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    temporary.replace(path)


def _prepare_matrix(
    frame: pd.DataFrame,
    *,
    imputer: SimpleImputer,
    scaler: StandardScaler,
    fit: bool,
) -> np.ndarray:
    source = frame.reindex(columns=FEATURE_NAMES).replace([np.inf, -np.inf], np.nan)
    if fit:
        values = imputer.fit_transform(source)
        values = scaler.fit_transform(values)
    else:
        values = scaler.transform(imputer.transform(source))
    if not np.isfinite(values).all():
        raise ValueError("Non-finite team-kills27 training matrix")
    return values


def _safe_auc(labels: np.ndarray, probabilities: np.ndarray) -> float | None:
    try:
        return float(roc_auc_score(labels, probabilities))
    except ValueError:
        return None


def _predict_proba(model: LogisticRegression, matrix: np.ndarray) -> np.ndarray:
    if not np.isfinite(matrix).all() or not np.isfinite(model.coef_).all():
        raise ValueError("Non-finite team-kills27 prediction input")
    # NumPy/Accelerate on macOS can leave liblinear floating status flags set
    # and emit false matmul overflow warnings for small finite arrays.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*encountered in matmul",
            category=RuntimeWarning,
        )
        probabilities = model.predict_proba(matrix)[:, 1]
    if not np.isfinite(probabilities).all():
        raise ValueError("Non-finite team-kills27 predicted probability")
    return probabilities


def _bet_metrics(labels: np.ndarray, probabilities: np.ndarray, threshold: float) -> dict[str, Any]:
    mask = probabilities >= threshold
    selected = labels[mask]
    hits = int(selected.sum())
    bets = int(len(selected))
    returns = np.where(selected == 1, ODDS - 1.0, -1.0)
    equity = np.concatenate([[0.0], np.cumsum(returns)])
    return {
        "bets": bets,
        "hits": hits,
        "hit_rate": float(hits / bets) if bets else None,
        "profit_units": float(returns.sum()),
        "roi": float(returns.sum() / bets) if bets else None,
        "max_drawdown_units": (
            float(np.max(np.maximum.accumulate(equity) - equity)) if bets else 0.0
        ),
    }


def _evaluate(
    frame: pd.DataFrame,
    probabilities: np.ndarray,
    threshold: float,
) -> dict[str, Any]:
    labels = frame.target.astype(int).to_numpy()
    clipped = np.clip(probabilities, 1e-6, 1.0 - 1e-6)
    return {
        "rows": int(len(frame)),
        "positive_rate": float(labels.mean()),
        "auc": _safe_auc(labels, clipped),
        "log_loss": float(log_loss(labels, clipped)),
        "bet": _bet_metrics(labels, clipped, threshold),
    }


def _segment_evaluations(
    frame: pd.DataFrame,
    probabilities: np.ndarray,
    threshold: float,
) -> dict[str, dict[str, Any]]:
    """Report tier/hit-count slices without using them for model selection."""
    masks: dict[str, pd.Series] = {}
    if "tier_segment" in frame:
        for segment in sorted(frame["tier_segment"].dropna().astype(str).unique()):
            masks[f"tier:{segment}"] = frame["tier_segment"].astype(str) == segment
    if {"target_tier", "opponent_tier"} <= set(frame.columns):
        target_tier = pd.to_numeric(frame["target_tier"], errors="coerce")
        opponent_tier = pd.to_numeric(frame["opponent_tier"], errors="coerce")
        masks["includes_tier1"] = (target_tier == 1) | (opponent_tier == 1)
        masks["tier2_sector"] = (target_tier != 1) & (opponent_tier != 1) & (
            (target_tier == 2) | (opponent_tier == 2)
        )
    if "nw_hit_count" in frame:
        hit_count = pd.to_numeric(frame["nw_hit_count"], errors="coerce")
        masks["nw_hits:2"] = hit_count == 2
        masks["nw_hits:3+"] = hit_count >= 3
    if "nw_max_wr" in frame:
        max_wr = pd.to_numeric(frame["nw_max_wr"], errors="coerce")
        masks["nw_wr:65+"] = max_wr >= 65
        masks["nw_wr:70+"] = max_wr >= 70

    output: dict[str, dict[str, Any]] = {}
    for name, mask in masks.items():
        indexes = np.flatnonzero(mask.to_numpy(dtype=bool))
        if len(indexes) == 0:
            continue
        output[name] = _evaluate(
            frame.iloc[indexes],
            np.asarray(probabilities)[indexes],
            threshold,
        )
    return output


def _elo_gate_evaluation(frame: pd.DataFrame) -> dict[str, Any] | None:
    if "elo_target_win_prob" not in frame:
        return None
    probability = pd.to_numeric(
        frame["elo_target_win_prob"], errors="coerce"
    ).to_numpy(dtype=float)
    available = np.isfinite(probability)
    if not available.any():
        return None
    selected = frame.iloc[np.flatnonzero(available)].reset_index(drop=True)
    probability = probability[available]
    threshold = 0.45
    return {
        "threshold": threshold,
        "overall": _evaluate(selected, probability, threshold),
        "segments": _segment_evaluations(selected, probability, threshold),
    }


def _fit_candidate(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    c_value: float,
) -> tuple[float, LogisticRegression, SimpleImputer, StandardScaler, np.ndarray]:
    imputer = SimpleImputer(strategy="median", keep_empty_features=True)
    scaler = StandardScaler()
    train_matrix = _prepare_matrix(train, imputer=imputer, scaler=scaler, fit=True)
    validation_matrix = _prepare_matrix(
        validation,
        imputer=imputer,
        scaler=scaler,
        fit=False,
    )
    model = LogisticRegression(
        C=c_value,
        max_iter=10_000,
        solver="liblinear",
        random_state=SEED,
    )
    model.fit(train_matrix, train.target.astype(int))
    probabilities = _predict_proba(model, validation_matrix)
    return (
        float(log_loss(validation.target.astype(int), probabilities)),
        model,
        imputer,
        scaler,
        probabilities,
    )


def _choose_threshold(labels: pd.Series, probabilities: np.ndarray) -> float:
    candidates = []
    for threshold in (0.0, 0.45, 0.50, 1.0 / ODDS, 0.60, 0.65, 0.70):
        metrics = _bet_metrics(labels.astype(int).to_numpy(), probabilities, threshold)
        if metrics["bets"] < 40:
            continue
        candidates.append(
            (metrics["profit_units"], metrics["roi"], -threshold, threshold)
        )
    if not candidates:
        return 1.0 / ODDS
    return float(max(candidates)[-1])


def _artifact_model(
    frame: pd.DataFrame,
    c_value: float,
) -> tuple[dict[str, Any], LogisticRegression, SimpleImputer, StandardScaler]:
    imputer = SimpleImputer(strategy="median", keep_empty_features=True)
    scaler = StandardScaler()
    matrix = _prepare_matrix(frame, imputer=imputer, scaler=scaler, fit=True)
    model = LogisticRegression(
        C=c_value,
        max_iter=10_000,
        solver="liblinear",
        random_state=SEED,
    )
    model.fit(matrix, frame.target.astype(int))
    medians = np.asarray(imputer.statistics_, dtype=float)
    medians = np.where(np.isfinite(medians), medians, 0.0)
    return (
        {
            "medians": medians.tolist(),
            "means": np.asarray(scaler.mean_, dtype=float).tolist(),
            "scales": np.asarray(scaler.scale_, dtype=float).tolist(),
            "coefficients": np.asarray(model.coef_[0], dtype=float).tolist(),
            "intercept": float(model.intercept_[0]),
        },
        model,
        imputer,
        scaler,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--old", type=Path, required=True)
    parser.add_argument("--forward", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    old = pd.read_csv(args.old).replace([np.inf, -np.inf], np.nan)
    forward = pd.read_csv(args.forward).replace([np.inf, -np.inf], np.nan)
    roster_feature_sources = {
        "roster_patch_games": "patch_roster4_games_30",
        "roster_patch_mean_kills": "patch_roster4_kills_mean_30",
        "roster_patch_ge27_rate": "patch_roster4_ge27_rate_30",
    }
    for frame in (old, forward):
        if "target_kills" not in frame:
            raise ValueError("target_kills is required to rebuild the >=27 label")
        frame["target"] = (frame["target_kills"] >= TARGET_KILLS).astype(int)
        for feature_name, source_name in roster_feature_sources.items():
            if source_name not in frame:
                raise ValueError(f"Missing roster feature source: {source_name}")
            frame[feature_name] = frame[source_name]
    availability = ["block_metrics_available"]
    if "early_win_metrics_available" in old:
        availability.append("early_win_metrics_available")
    for column in availability:
        old = old[old[column] == 1]
        forward = forward[forward[column] == 1]
    old = old.sort_values(["startDateTime", "match_id"]).reset_index(drop=True)
    forward = forward.sort_values(["startDateTime", "match_id"]).reset_index(drop=True)
    if len(old) < 300 or len(forward) < 100:
        raise ValueError(f"Insufficient samples old={len(old)} forward={len(forward)}")

    train_end = int(len(old) * 0.60)
    validation_end = int(len(old) * 0.80)
    train = old.iloc[:train_end].copy()
    validation = old.iloc[train_end:validation_end].copy()
    old_test = old.iloc[validation_end:].copy()

    candidates = []
    for c_value in (0.01, 0.03, 0.1, 0.3, 1.0):
        candidates.append((c_value, *_fit_candidate(train, validation, c_value)))
    c_value, _, model, imputer, scaler, validation_probability = min(
        candidates,
        key=lambda item: item[1],
    )
    threshold = _choose_threshold(validation.target, validation_probability)
    old_test_matrix = _prepare_matrix(
        old_test,
        imputer=imputer,
        scaler=scaler,
        fit=False,
    )
    forward_matrix = _prepare_matrix(
        forward,
        imputer=imputer,
        scaler=scaler,
        fit=False,
    )
    old_test_probability = _predict_proba(model, old_test_matrix)
    forward_probability = _predict_proba(model, forward_matrix)

    frozen_model, refit_model, refit_imputer, refit_scaler = _artifact_model(old, c_value)
    refit_forward = _predict_proba(
        refit_model,
        _prepare_matrix(
            forward,
            imputer=refit_imputer,
            scaler=refit_scaler,
            fit=False,
        ),
    )
    created_at = datetime.now(timezone.utc).isoformat()
    report = {
        "target_kills_threshold": TARGET_KILLS,
        "selection": "C and bet threshold selected on old chronological validation only",
        "forward_used_for_selection": False,
        "old_rows": int(len(old)),
        "forward_rows": int(len(forward)),
        "split_rows": {
            "train": int(len(train)),
            "validation": int(len(validation)),
            "old_test": int(len(old_test)),
        },
        "regularization_c": float(c_value),
        "bet_threshold": float(threshold),
        "validation": _evaluate(validation, validation_probability, threshold),
        "old_test": _evaluate(old_test, old_test_probability, threshold),
        "forward_untouched": _evaluate(forward, forward_probability, threshold),
        "forward_untouched_segments": _segment_evaluations(
            forward, forward_probability, threshold
        ),
        "forward_refit_old_only": _evaluate(forward, refit_forward, threshold),
        "forward_refit_old_only_segments": _segment_evaluations(
            forward, refit_forward, threshold
        ),
        "forward_elo_gate_0_45": _elo_gate_evaluation(forward),
    }
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": created_at,
        "odds": ODDS,
        "target": f"target_team_final_kills_ge{TARGET_KILLS}",
        "target_kills_threshold": TARGET_KILLS,
        "model_type": "standardized_logistic_regression",
        "sklearn_version": sklearn.__version__,
        "feature_names": list(FEATURE_NAMES),
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
        "regularization_c": float(c_value),
        "bet_threshold": float(threshold),
        "fit_scope": "all old rows only; forward excluded",
        "model": frozen_model,
        "evaluation": report,
    }
    _atomic_json(args.artifact, artifact)
    _atomic_json(args.report, report)
    print(f"artifact={args.artifact}")
    print(f"report={args.report}")
    print(
        f"forward={report['forward_untouched']['bet']['bets']} bets "
        f"roi={report['forward_untouched']['bet']['roi']:.3f} "
        f"auc={report['forward_untouched']['auc']:.3f}"
    )


if __name__ == "__main__":
    main()
