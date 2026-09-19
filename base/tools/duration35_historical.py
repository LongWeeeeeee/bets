#!/usr/bin/env python3
"""Strict chronological draft-versus-history evaluation for duration <=35 min.

This is an offline experiment.  Every history value is available only after a
previous map's end, and Platt scaling is fitted only on the fold's calibration
month.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import joblib
import numpy as np
from catboost import CatBoostClassifier
from scipy import sparse
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from base.draft_features import DraftFeatureEncoder, KIND_ROLE
from base.end_time_prior import EndTimePrior

# A verified CoW snapshot in the isolated worktree; the campaign executor makes
# a second immutable declared-input snapshot before fitting.
SOURCE = "data/duration35/pro_corpus_rich.npz"
THRESHOLD_SECONDS = 2100
FOLDS = {
    "may": ["2026-03-01", "2026-04-01", "2026-05-01", "2026-06-01"],
    "jun": ["2026-04-01", "2026-05-01", "2026-06-01", "2026-07-01"],
    "jul": ["2026-05-01", "2026-06-01", "2026-07-01", "2026-08-01"],
    "aug": ["2026-06-01", "2026-07-01", "2026-08-01", "2026-09-01"],
}


def utc(day: str) -> int:
    return int(np.datetime64(day, "s").astype(np.int64))


def atomic_json(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n")
    tmp.replace(path)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def target(duration: np.ndarray) -> np.ndarray:
    """The requested literal target: a 2100-second map is positive."""
    return (duration <= THRESHOLD_SECONDS).astype(np.int8)


def build_history(ts: np.ndarray, duration: np.ndarray, heroes: np.ndarray,
                  accounts: np.ndarray) -> np.ndarray:
    """End-time-causal player/hero aggregates, without E299's >=36/>=43 labels."""
    starts = np.repeat(ts, 10)
    ends = np.repeat(ts + duration, 10)
    values = (np.repeat(duration / 60.0, 10), np.repeat(target(duration), 10))
    features = []
    for keys in (accounts.ravel(), heroes.ravel()):
        history = EndTimePrior(keys, starts, ends)
        per_slot = []
        for value, global_prior in zip(values, (36.0, 0.50)):
            sums, counts = history.sum_count(value, np.ones(len(value)))
            per_slot.append((sums + 20.0 * global_prior) / (counts + 20.0))
        per_slot.append(np.log1p(counts))
        slot = np.column_stack(per_slot).reshape(len(ts), 10, 3)
        features.extend((slot.mean(1), slot.std(1), slot.min(1), slot.max(1)))
    result = np.column_stack(features).astype(np.float32)
    if not np.isfinite(result).all():
        raise ValueError("non-finite causal history")
    return result


def report_metrics(y: np.ndarray, p: np.ndarray) -> dict:
    bins = []
    for lo, hi in zip((0.0, 0.2, 0.3, 0.4, 0.5, 0.6), (0.2, 0.3, 0.4, 0.5, 0.6, 1.01)):
        take = (p >= lo) & (p < hi)
        bins.append({"range": [lo, hi], "n": int(take.sum()),
                     "mean_p": float(p[take].mean()) if take.any() else None,
                     "observed": float(y[take].mean()) if take.any() else None})
    return {"n": int(len(y)), "positive_rate": float(y.mean()), "mean_p": float(p.mean()),
            "auc": float(roc_auc_score(y, p)), "logloss": float(log_loss(y, p)),
            "brier": float(brier_score_loss(y, p)), "bins": bins}


def paired_day_cluster(y: np.ndarray, baseline: np.ndarray, candidate: np.ndarray,
                       ts: np.ndarray) -> dict:
    def loss(probability: np.ndarray) -> np.ndarray:
        p = np.clip(probability, 1e-9, 1 - 1e-9)
        return -(y * np.log(p) + (1 - y) * np.log1p(-p))

    delta = loss(candidate) - loss(baseline)
    days, inverse = np.unique(ts // 86400, return_inverse=True)
    counts = np.bincount(inverse)
    sums = np.bincount(inverse, weights=delta)
    samples = np.random.default_rng(350).integers(len(days), size=(5000, len(days)))
    bootstrap = sums[samples].sum(1) / counts[samples].sum(1)
    return {"delta_logloss": float(delta.mean()), "ci95": np.quantile(bootstrap, (.025, .975)).tolist(),
            "ci99": np.quantile(bootstrap, (.005, .995)).tolist(), "days": int(len(days))}


def load_rows(cutoff: int) -> dict[str, np.ndarray]:
    z = np.load(ROOT / SOURCE)
    valid = ((z["ts"] >= utc("2023-01-01")) & (z["ts"] < cutoff)
             & (z["durations"] > 0) & (z["heroes"] > 0).all(1)
             & (np.diff(np.sort(z["heroes"], axis=1), axis=1) != 0).all(1))
    rows = {key: z[key][valid] for key in ("mids", "ts", "durations", "heroes", "accounts")}
    order = np.lexsort((rows["mids"], rows["ts"]))
    rows = {key: value[order] for key, value in rows.items()}
    if len(np.unique(rows["mids"])) != len(rows["mids"]):
        raise ValueError("duplicate match ids")
    return rows


def split_masks(ts: np.ndarray, duration: np.ndarray, fold: str) -> tuple[list[np.ndarray], list[int]]:
    cuts = list(map(utc, FOLDS[fold]))
    ends = ts + duration
    masks = [(ts >= utc("2024-01-01")) & (ends < cuts[0])]
    masks.extend((ts >= lo) & (ts < hi) & (ends < hi) for lo, hi in zip(cuts[:-1], cuts[1:]))
    if np.stack(masks).sum(axis=0).max() > 1:
        raise ValueError("overlapping temporal splits")
    return masks, cuts


def logits(probability: np.ndarray) -> np.ndarray:
    p = np.clip(probability, 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))[:, None]


def fit_model(x: sparse.csr_matrix, y: np.ndarray, train: np.ndarray,
              early: np.ndarray, threads: int) -> CatBoostClassifier:
    model = CatBoostClassifier(iterations=600, depth=6, learning_rate=0.05, l2_leaf_reg=6,
                                loss_function="Logloss", eval_metric="Logloss", random_seed=350,
                                thread_count=threads, verbose=False, allow_writing_files=False)
    model.fit(x[train], y[train], eval_set=(x[early], y[early]), early_stopping_rounds=80)
    return model


def fold_run(fold: str, output_dir: Path, threads: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    if (output_dir / "metrics.json").exists():
        raise RuntimeError("completed fold is immutable")
    rows = load_rows(utc(FOLDS[fold][-1]))
    ts, duration, heroes = (rows[key] for key in ("ts", "durations", "heroes"))
    (train, early, calibration, test), cuts = split_masks(ts, duration, fold)
    y = target(duration)
    if not all(len(np.unique(y[mask])) == 2 for mask in (train, early, calibration, test)):
        raise ValueError("a split lacks both target classes")
    protocol = {
        "fold": fold, "boundaries": FOLDS[fold], "train_from": "2024-01-01",
        "history_from": "2023-01-01", "target": "duration_seconds <=2100 (inclusive)",
        "history_contract": "Only maps with end_ts < prediction start contribute; no E299 >=36/>=43 label fields.",
        "counts": dict(zip(("train", "early_stop", "calibration", "test"), (int(x.sum()) for x in (train, early, calibration, test)))),
        "rates": dict(zip(("train", "early_stop", "calibration", "test"), (float(y[x].mean()) for x in (train, early, calibration, test)))),
        "model": {"iterations": 600, "depth": 6, "learning_rate": .05, "l2_leaf_reg": 6,
                  "early_stopping_rounds": 80, "random_seed": 350},
        "calibration": "Platt C=1, fitted only on calibration month; raw probabilities also reported.",
        "source_sha256": sha256(ROOT / SOURCE), "test_status": "retrospective fixed May-August windows",
    }
    atomic_json(output_dir / "protocol.json", protocol)
    encoder = DraftFeatureEncoder.fit(heroes[train], KIND_ROLE, signed=False)
    draft = encoder.transform(heroes).astype(np.float32)
    joblib.dump(encoder, output_dir / "encoder.joblib")
    draft_model = fit_model(draft, y, train, early, threads)
    draft_test = draft_model.predict_proba(draft[test], thread_count=threads)[:, 1]
    draft_model.save_model(str(output_dir / "draft_only.cbm"))
    history = build_history(ts, duration, heroes, rows["accounts"])
    causal_x = sparse.hstack((draft, sparse.csr_matrix(history)), format="csr").astype(np.float32)
    causal_model = fit_model(causal_x, y, train, early, threads)
    causal_calibration = causal_model.predict_proba(causal_x[calibration], thread_count=threads)[:, 1]
    causal_test = causal_model.predict_proba(causal_x[test], thread_count=threads)[:, 1]
    causal_model.save_model(str(output_dir / "causal_history.cbm"))
    calibrator = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000, random_state=350).fit(logits(causal_calibration), y[calibration])
    platt_test = calibrator.predict_proba(logits(causal_test))[:, 1]
    joblib.dump(calibrator, output_dir / "calibrator.joblib")
    reloaded = CatBoostClassifier(); reloaded.load_model(str(output_dir / "causal_history.cbm"))
    reloaded_platt = joblib.load(output_dir / "calibrator.joblib")
    replay_raw = reloaded.predict_proba(causal_x[test], thread_count=threads)[:, 1]
    replay_platt = reloaded_platt.predict_proba(logits(replay_raw))[:, 1]
    replay = {"causal_raw_max_abs_error": float(np.max(np.abs(replay_raw - causal_test))),
              "causal_platt_max_abs_error": float(np.max(np.abs(replay_platt - platt_test))),
              "threshold_inclusive_examples": int((duration == THRESHOLD_SECONDS).sum()),
              "all_test_end_before_boundary": bool((ts[test] + duration[test] < cuts[-1]).all())}
    if replay["causal_raw_max_abs_error"] > 1e-12 or replay["causal_platt_max_abs_error"] > 1e-12:
        raise ValueError("model replay mismatch")
    if not replay["all_test_end_before_boundary"]:
        raise ValueError("test contains maps ending after its boundary")
    predictions = {"draft_only": draft_test, "causal_history": causal_test, "causal_platt": platt_test}
    report = {"protocol": protocol, "trees": {"draft_only": int(draft_model.tree_count_), "causal_history": int(causal_model.tree_count_)},
              "models": {name: report_metrics(y[test], probability) for name, probability in predictions.items()},
              "paired": {"draft_only->causal_history": paired_day_cluster(y[test], draft_test, causal_test, ts[test]),
                         "causal_history->causal_platt": paired_day_cluster(y[test], causal_test, platt_test, ts[test])},
              "replay": replay}
    np.savez_compressed(output_dir / "predictions.npz", mids=rows["mids"][test], ts=ts[test], durations=duration[test], y=y[test], **predictions)
    np.savez_compressed(output_dir / "calibration.npz", mids=rows["mids"][calibration], ts=ts[calibration], y=y[calibration], causal_raw=causal_calibration)
    atomic_json(output_dir / "replay.json", replay)
    atomic_json(output_dir / "metrics.json", report)
    print("DONE", fold, {name: round(value["logloss"], 6) for name, value in report["models"].items()}, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fold", choices=FOLDS, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=1)
    args = parser.parse_args()
    if args.threads < 1:
        raise ValueError("threads must be positive")
    from threadpoolctl import threadpool_limits
    threadpool_limits(args.threads)
    fold_run(args.fold, args.output_dir, args.threads)


if __name__ == "__main__":
    main()
