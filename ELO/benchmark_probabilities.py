"""Compare cached prematch ratings on common chronological evaluation rows.

This is a diagnostic, not a trainer or a production model selector. Cached
ratings retain their original training histories; calibration alone is refit
on the preceding 120 days. No current snapshot is used to predict old maps.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from scipy.special import expit
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

ROOT = Path(__file__).resolve().parents[1]
DAY = 86400


def align(source, target):
    """Exact unique-mid join; missing rows must never become a positional join."""
    if len(np.unique(source)) != len(source) or len(np.unique(target)) != len(target):
        raise ValueError("duplicate match id")
    order = np.argsort(source)
    pos = np.searchsorted(source[order], target)
    if np.any(pos >= len(source)) or not np.array_equal(source[order[pos]], target):
        raise ValueError("missing match id")
    return order[pos]


def prior_map_context(rich):
    """Use only a finished preceding map in the same series and team pair.

    Never select series based on their eventual completeness or winner. The
    feature refers to the last *observed* map; missing source maps remain a
    documented limitation. Match end precedes prediction strictly.
    """
    n = len(rich["mids"])
    result = np.zeros(n, dtype=float)
    history = {}
    order = np.lexsort((rich["mids"], rich["ts"]))
    for i in order:
        sid, now = int(rich["sids"][i]), int(rich["ts"][i])
        rad, dire = map(int, rich["teams"][i])
        if sid <= 0 or min(rad, dire) <= 0 or rad == dire:
            continue
        key = sid, min(rad, dire), max(rad, dire)
        prev = history.get(key)
        if prev is not None:
            start, end, winner = prev
            if end < now and now - start <= 3 * DAY:
                result[i] = 1.0 if winner == rad else -1.0
        duration = float(rich["durations"][i])
        if np.isfinite(duration) and duration > 0:
            history[key] = now, now + duration, rad if rich["wins"][i] else dire
        else:
            history.pop(key, None)
    return result


def metrics(y, p):
    p = np.clip(p, 1e-9, 1 - 1e-9)
    return {"n": len(y), "auc": float(roc_auc_score(y, p)),
            "log_loss": float(loss(y, p).mean()),
            "brier": float(np.mean((y - p) ** 2)),
            "accuracy": float(np.mean((p >= .5) == y))}


def loss(y, p):
    p = np.clip(p, 1e-9, 1 - 1e-9)
    return -y * np.log(p) - (1 - y) * np.log1p(-p)


def loss_interval(y, candidate, baseline, groups):
    """Paired bootstrap of log loss, clustered by series (singletons by mid)."""
    _, inv = np.unique(groups, return_inverse=True)
    totals = np.bincount(inv, weights=loss(y, candidate) - loss(y, baseline))
    counts = np.bincount(inv)
    rng = np.random.default_rng(20260905)
    samples = []
    for _ in range(1000):
        chosen = rng.integers(0, len(counts), size=len(counts))
        samples.append(totals[chosen].sum() / counts[chosen].sum())
    return [float(x) for x in np.quantile(samples, [.025, .975])]


def timestamp(date):
    return int(datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=ROOT / "runtime/artifacts/misc")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.artifact_root
    paths = {"base": root / "win_model_base_matrix.npz",
             "hybrid": root / "map_winner_hybrid_quality_forward/hybrid_features.npz",
             "hybrid_tier3": root / "hybrid_strength_tier3.npz",
             "glicko": root / "undercount_glicko_features.npz",
             "rich": root / "pro_corpus_rich.npz"}
    input_stats = {k: {"path": str(p), "size": p.stat().st_size, "mtime_ns": p.stat().st_mtime_ns}
                   for k, p in paths.items()}
    # Only the locally built base matrix has a trusted object array of names.
    base = np.load(paths["base"], allow_pickle=True)
    mids, ts, y = base["mids"], base["ts"], base["y"].astype(int)
    if np.any(ts[1:] < ts[:-1]) or not np.isin(y, [0, 1]).all():
        raise ValueError("invalid chronology or target")
    names = base["names"].tolist()
    X = base["X"]
    elo, hybrid = X[:, names.index("elo")], X[:, names.index("hybrid_strength")]
    # The base cache falls back to tier-aware hybrid before September 2019.
    # All calibration/evaluation rows here must match the serving TIER3 form.
    used = ts >= timestamp("2026-01-01") - 120 * DAY
    ht = np.load(paths["hybrid_tier3"], allow_pickle=False)
    ti = align(ht["mids"], mids[used])
    if not np.array_equal(ht["value"][ti], hybrid[used]):
        raise ValueError("served TIER3 hybrid differs from base matrix")
    h = np.load(paths["hybrid"], allow_pickle=False)
    hi = align(h["mids"], mids)
    if not np.all(h["asof"][hi] < ts):
        raise ValueError("hybrid cache is not before timestamp")
    g = np.load(paths["glicko"], allow_pickle=False)
    gi = align(g["mids"], mids)
    if not np.array_equal(g["ts"][gi], ts):
        raise ValueError("Glicko timestamp mismatch")
    z = np.load(paths["rich"], allow_pickle=False)
    rich = {k: z[k] for k in ("mids", "ts", "wins", "teams", "sids", "durations")}
    ri = align(rich["mids"], mids)
    if not np.array_equal(rich["ts"][ri], ts) or not np.array_equal(rich["wins"][ri], y):
        raise ValueError("rich corpus timestamp/outcome mismatch")
    prev = prior_map_context(rich)[ri]
    sids = rich["sids"][ri]
    groups = np.where(sids > 0, sids, -mids)
    features = {"elo_calibrated": elo[:, None], "hybrid_calibrated": hybrid[:, None],
                "glicko_calibrated": g["glicko"][gi, None],
                "elo_hybrid": np.column_stack([elo, hybrid]),
                "hybrid_context": np.column_stack([hybrid, prev]),
                "elo_hybrid_context": np.column_stack([elo, hybrid, prev])}
    if not all(np.isfinite(v).all() for v in features.values()):
        raise ValueError("nonfinite feature")
    bounds = [timestamp(d) for d in ["2026-01-01", "2026-03-01", "2026-06-01", "2026-08-12"]]
    predictions, selected, windows = {}, [], []
    for lo, hi in zip(bounds, bounds[1:]):
        test = (ts >= lo) & (ts < hi)
        # Purge any series shared with test, in addition to strict time split.
        train = (ts < lo) & (ts >= lo - 120 * DAY) & ~np.isin(groups, groups[test])
        if train.sum() < 100 or test.sum() < 100:
            raise ValueError("insufficient train/test rows")
        row = {"from": datetime.fromtimestamp(lo, timezone.utc).isoformat(),
               "train_n": int(train.sum()), "test_n": int(test.sum()), "models": {}, "fits": {}}
        ps = {"elo_raw": expit(np.log(10) * elo[test]),
              "hybrid_raw": expit(np.log(10) * hybrid[test])}
        for name, values in features.items():
            mu, sd = values[train].mean(0), values[train].std(0)
            sd = np.where(sd > 1e-9, sd, 1)
            model = LogisticRegression(C=1.0, max_iter=1000)
            with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
                model.fit((values[train] - mu) / sd, y[train])
                ps[name] = model.predict_proba((values[test] - mu) / sd)[:, 1]
            row["fits"][name] = {"coef_original_units": (model.coef_[0] / sd).tolist(),
                                   "intercept": float(model.intercept_[0] - model.coef_[0] @ (mu / sd))}
        for name, p in ps.items():
            row["models"][name] = metrics(y[test], p)
            predictions.setdefault(name, []).append(p)
        selected.append(np.flatnonzero(test))
        windows.append(row)
        print(json.dumps(row), flush=True)
    idx = np.concatenate(selected)
    predictions = {k: np.concatenate(v) for k, v in predictions.items()}
    report = {"protocol": "three fixed chronological windows; 120 day calibration; shared series purged",
              "limitations": ["reused historical test, not untouched holdout", "different original rating training histories",
                              "cached rating state updates ordered by start, not verified by finish time",
                              "hybrid asof validates tier-aware producer; TIER3 values verified against original cache",
                              "no odds or production dispatch evaluation"],
              "input_stats": input_stats,
              "mid_sha256": hashlib.sha256(mids.tobytes()).hexdigest(),
              "windows": windows, "aggregate": {}, "context_only": {}}
    for name, p in predictions.items():
        report["aggregate"][name] = metrics(y[idx], p)
        report["aggregate"][name]["log_loss_delta_ci_vs_hybrid_raw"] = loss_interval(
            y[idx], p, predictions["hybrid_raw"], groups[idx])
        mask = prev[idx] != 0
        report["context_only"][name] = metrics(y[idx][mask], p[mask])
    for key, path in paths.items():
        stat = path.stat()
        if (stat.st_size, stat.st_mtime_ns) != (input_stats[key]["size"], input_stats[key]["mtime_ns"]):
            raise ValueError(f"input changed during benchmark: {key}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.output.with_suffix(args.output.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2) + "\n")
    tmp.replace(args.output)
    print("DONE", args.output, json.dumps(report["aggregate"]), flush=True)


if __name__ == "__main__":
    main()
