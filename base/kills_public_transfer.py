"""Public draft pretraining with dimensionless targets, frozen before pro fit."""
from __future__ import annotations

import gc
from pathlib import Path

import joblib
import numpy as np
from scipy.sparse import vstack
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
from threadpoolctl import threadpool_limits

from draft_features import DraftFeatureEncoder, KIND_PAIR
from kills_transfer_data import atomic_json


def public_design(encoder, heroes, batch_size=25000):
    """Bound dense pair-encoding temporaries while retaining the sparse design."""
    return vstack([encoder.transform(heroes[start:start+batch_size]).astype(np.float32)
                   for start in range(0, len(heroes), batch_size)], format="csr")


def causal_total_targets(totals, starts, ends, minimum=100):
    """Normalize every row by completed maps in the preceding 28 days.

    The reference is frozen at the beginning of each UTC week, using exactly
    the same as-of policy in public training and validation. Cold start is
    missing and excluded, never normalized using its own or future outcomes.
    """
    week_seconds = 7 * 86400
    week_start = (starts // week_seconds) * week_seconds
    order = np.argsort(ends, kind="stable")
    end_sorted = ends[order]
    cumulative = np.r_[0.0, np.cumsum(totals[order], dtype=float)]
    reference = np.full(len(starts), np.nan)
    means = {}
    for start in np.unique(week_start):
        lo = np.searchsorted(end_sorted, start-28*86400, side="left")
        hi = np.searchsorted(end_sorted, start, side="left")
        if hi-lo >= minimum:
            mean = (cumulative[hi]-cumulative[lo])/(hi-lo)
            if mean > 0:
                reference[week_start == start] = mean
                means[int(start)] = float(mean)
    return totals/reference, reference, means


def fit_public(data, output_dir, threads=2):
    """Select ridge strength chronologically; return a frozen two-score bundle.

    Total target is relative to completed prior weeks, while the side
    target is the kill-share difference. No pro outcomes choose these models.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts, ends = data["ts"], data["ends"]
    cut = int(np.quantile(ts, .8))
    train, valid = ends < cut, ts >= cut
    if min(train.sum(), valid.sum()) < 100:
        raise ValueError("Public chronological partitions too small")
    scores = data["stats"][:, :, 0].reshape(-1, 2, 5).sum(2)
    totals = scores.sum(1)
    train_mean = float(totals[train].mean())
    if train_mean <= 0:
        raise ValueError("Empty public kill labels")
    total_y, reference, week_means = causal_total_targets(totals, ts, ends)
    train &= np.isfinite(total_y)
    valid &= np.isfinite(total_y)
    if min(train.sum(), valid.sum()) < 100:
        raise ValueError("Insufficient public history after causal normalization warmup")
    share_y = np.divide(scores[:, 0] - scores[:, 1], totals,
                        out=np.zeros(len(totals)), where=totals > 0)
    bundle, report = {}, {"train_maps": int(train.sum()), "validation_maps": int(valid.sum()),
                          "cutoff": cut, "public_mean_total": train_mean,
                          "target": "total relative to prior 28 days at week start; within-map kill-share difference",
                          "normalizer_warmup_excluded": int(np.isnan(reference).sum()),
                          "models": {}}
    for name, signed, target in (("total", False, total_y), ("share", True, share_y)):
        encoder = DraftFeatureEncoder.fit(data["heroes"][train], KIND_PAIR, signed=signed)
        xtrain = public_design(encoder, data["heroes"][train])
        xvalid = public_design(encoder, data["heroes"][valid])
        best = None
        trials = []
        for alpha in (100.0, 1000.0):
            model = Ridge(alpha=alpha, solver="lsqr", tol=1e-4, max_iter=250)
            with threadpool_limits(limits=threads):
                model.fit(xtrain, target[train])
            error = float(mean_squared_error(target[valid], model.predict(xvalid)))
            trials.append({"alpha": alpha, "validation_mse": error})
            if best is None or error < best[0]:
                best = (error, model, alpha)
            print(f"public {name} alpha={alpha}: validation_mse={error:.6f}", flush=True)
        # Use the selected model exactly as validated. The remaining public
        # period can train profiles for later pro maps but does not refit here.
        bundle[name] = {"encoder": encoder, "model": best[1]}
        report["models"][name] = {"selected_alpha": best[2], "trials": trials,
                                   "columns": xtrain.shape[1]}
        del xtrain, xvalid
        gc.collect()
    report["week_means"] = week_means
    temp = out / "public_models.joblib.tmp"
    joblib.dump(bundle, temp, compress=3)
    temp.replace(out / "public_models.joblib")
    atomic_json(out / "public_report.json", report)
    return bundle, report


def predict_public(bundle, heroes):
    result = []
    for name in ("total", "share"):
        part = bundle[name]
        result.append(part["model"].predict(part["encoder"].transform(heroes)))
    return np.column_stack(result).astype(np.float32)
