"""Offline public pretraining and calibrated pro total/side threshold models.

Artifacts are isolated candidates. This entrypoint never publishes a panel or
changes a running service. Test predictions are made after validation selection.
"""
from __future__ import annotations

import argparse
import gc
import os
from pathlib import Path
import time

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score
from threadpoolctl import threadpool_limits

from kills_public_transfer import fit_public, predict_public
from kills_transfer_data import atomic_json, atomic_npz, load_rows, sha256, timestamp
from kills_transfer_features import causal_pro_context, profile_features, public_profiles, swap, temporal_splits


def probability_metrics(y, probability):
    p = np.clip(probability, 1e-6, 1-1e-6)
    return {"n": len(y), "prevalence": float(np.mean(y)),
            "auc": float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else None,
            "log_loss": float(log_loss(y, p, labels=[0, 1])),
            "brier": float(brier_score_loss(y, p)),
            "accuracy": float(accuracy_score(y, p >= .5))}


def calibrate_fit(raw, y):
    logits = np.log(np.clip(raw, 1e-6, 1-1e-6) / np.clip(1-raw, 1e-6, 1-1e-6))[:, None]
    model = LogisticRegression(C=1.0, max_iter=1000)
    model.fit(logits, y)
    return model


def calibrate_predict(model, raw):
    logits = np.log(np.clip(raw, 1e-6, 1-1e-6) / np.clip(1-raw, 1e-6, 1-1e-6))[:, None]
    return model.predict_proba(logits)[:, 1]


def collapse(raw, n, target):
    return (raw[:n] + raw[n:]) / 2 if target == "total" else raw


def targets(stats, target):
    side = stats[:, :, 0].reshape(-1, 2, 5).sum(2)
    if target == "total":
        return (side.sum(1) >= 55).astype(np.int8)
    return np.r_[side[:, 0] >= 30, side[:, 1] >= 30].astype(np.int8)


def feature_frame(heroes, context, context_names, public_scores, profiles, arm):
    n = len(heroes)
    oriented = np.concatenate((heroes, swap(heroes)))
    sides = np.concatenate((context, context[:, ::-1]), axis=0)
    cols = [f"{side}_{name}" for side in ("own", "opponent") for name in context_names]
    frame = pd.DataFrame(sides.reshape(n*2, -1), columns=cols)
    frame["side_is_radiant"] = np.r_[np.ones(n), np.zeros(n)]
    for slot in range(10):
        frame[f"hero{slot}"] = oriented[:, slot].astype(np.int64)
    if arm != "pro_only":
        frame["public_total"] = np.tile(public_scores[:, 0], 2)
        frame["public_share"] = np.r_[public_scores[:, 1], -public_scores[:, 1]]
    if arm.startswith("relative") or arm == "absolute":
        kind = "absolute" if arm == "absolute" else "relative"
        values, names = profile_features(oriented, profiles, kind, no_deaths=arm == "relative_no_deaths")
        frame = pd.concat([frame, pd.DataFrame(values, columns=names)], axis=1)
    return frame


def clustered_loss_delta(y, candidate, baseline, days, draws=2000):
    p, q = np.clip(candidate, 1e-6, 1-1e-6), np.clip(baseline, 1e-6, 1-1e-6)
    delta = -(y*np.log(p)+(1-y)*np.log(1-p)) + y*np.log(q)+(1-y)*np.log(1-q)
    unique, inv = np.unique(days, return_inverse=True)
    sums, counts = np.bincount(inv, weights=delta), np.bincount(inv)
    rng = np.random.default_rng(20260912)
    boots = np.empty(draws)
    for i in range(draws):
        sampled = rng.integers(0, len(unique), len(unique))
        boots[i] = sums[sampled].sum()/counts[sampled].sum()
    return {"mean": float(delta.mean()), "day_cluster_95ci": np.quantile(boots, [.025, .975]).tolist(),
            "days": len(unique)}


def train(args):
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    if (out / "summary.json").exists():
        raise ValueError("Completed experiment exists; inspect it before launching a new run")
    begin = time.monotonic()
    public_cut, val_cut, test_cut = map(timestamp, (args.train_from, args.val_from, args.test_from))
    public = load_rows(args.public_rows, before=public_cut)
    pro = load_rows(args.pro_rows)
    overlap = np.intersect1d(public["mids"], pro["mids"])
    # A pro map accidentally present in the pub source is never pretraining data.
    keep = ~np.isin(public["mids"], overlap)
    public = {key: values[keep] for key, values in public.items()}
    print(f"inputs public={len(keep)} retained={keep.sum()} overlap_removed={len(overlap)} pro={len(pro['mids'])}", flush=True)
    if args.smoke:
        ids = np.linspace(0, len(public["mids"])-1, min(1000, len(public["mids"]))).astype(int)
        public = {key: values[ids] for key, values in public.items()}
    profiles = public_profiles(public, public_cut)
    bundle, public_report = fit_public(public, out, args.threads)
    public_scores = predict_public(bundle, pro["heroes"])
    del public, bundle
    gc.collect()
    temp = out / "profiles.joblib.tmp"
    joblib.dump(profiles, temp, compress=3)
    os.replace(temp, out / "profiles.joblib")
    context, context_names = causal_pro_context(pro)
    split = temporal_splits(pro, public_cut, val_cut, test_cut)
    if any(np.sum(split == part) < 50 for part in (0, 1, 2)):
        raise ValueError(f"Insufficient pro split: {np.unique(split, return_counts=True)}")
    use = np.flatnonzero(split >= 0)
    pro = {key: values[use] for key, values in pro.items()}
    context, public_scores, split = context[use], public_scores[use], split[use]
    print("pro partitions", {str(k): int((split == k).sum()) for k in (0, 1, 2)}, flush=True)
    atomic_npz(out / "pro_inputs.npz", **pro, context=context, public_scores=public_scores, split=split,
               context_names=np.asarray(context_names))
    report = {"schema": "kills-public-relative-transfer-v1", "smoke": bool(args.smoke),
              "public": public_report, "public_cutoff": public_cut, "validation_from": val_cut,
              "test_from": test_cut, "overlap_removed": len(overlap),
              "split_maps": {str(k): int((split == k).sum()) for k in (0, 1, 2)},
              "targets": {}, "current_panel_comparison": "not established: different serving feature set",
              "label_definition": "sum of player kills, same as previous panel; no excluded middle",
              "source_manifests": {name: sha256(Path(path)/"summary.json") for name,path in
                                   (("public",args.public_rows),("pro",args.pro_rows))}}
    arms = ("pro_only", "public_draft", "relative", "relative_no_deaths", "absolute")
    n = len(pro["mids"])
    fit_at, val_at, test_at = [np.flatnonzero(split == k) for k in (0, 1, 2)]
    calibration_cut, selection_cut = [int(x) for x in np.quantile(pro["ts"][val_at], [1/3, 2/3])]
    inner = temporal_splits(pro, val_cut, calibration_cut, selection_cut)
    stop_at, calibrate_at, select_at = [np.flatnonzero((split == 1) & (inner == k)) for k in (0, 1, 2)]
    if min(len(stop_at), len(calibrate_at), len(select_at)) < 40:
        raise ValueError("Insufficient independent early-stop/calibration/selection partitions")
    report["validation_partitions"] = {"early_stop_maps": len(stop_at), "calibration_maps": len(calibrate_at),
                                        "selection_maps": len(select_at), "calibration_from": calibration_cut,
                                        "selection_from": selection_cut}
    report["series_id_unknown_maps"] = int(np.sum(pro["sids"] == 0))
    paired = lambda at: np.r_[at, at+n]
    for target in ("total", "side"):
        y = targets(pro["stats"], target)
        fit_y = np.tile(y[fit_at], 2) if target == "total" else y[paired(fit_at)]
        test_y = y[test_at] if target == "total" else y[paired(test_at)]
        calibration_y = y[calibrate_at] if target == "total" else y[paired(calibrate_at)]
        selection_y = y[select_at] if target == "total" else y[paired(select_at)]
        models, trials = {}, {}
        # All architecture/hyperparameter choices finish before test prediction.
        for arm in arms:
            frame = feature_frame(pro["heroes"], context, context_names, public_scores, profiles, arm)
            best = None
            for depth in ((3,) if args.smoke else (4, 6)):
                model = CatBoostClassifier(iterations=25 if args.smoke else 800, depth=depth,
                    learning_rate=.035, l2_leaf_reg=12, loss_function="Logloss",
                    random_seed=20260912, thread_count=args.threads, verbose=False,
                    allow_writing_files=False, early_stopping_rounds=80)
                cat = [f"hero{i}" for i in range(10)]
                eval_y = np.tile(y[stop_at], 2) if target == "total" else y[paired(stop_at)]
                model.fit(frame.iloc[paired(fit_at)], fit_y, cat_features=cat,
                          eval_set=(frame.iloc[paired(stop_at)], eval_y))
                raw_cal = collapse(model.predict_proba(frame.iloc[paired(calibrate_at)])[:, 1], len(calibrate_at), target)
                cal = calibrate_fit(raw_cal, calibration_y)
                raw_selection = collapse(model.predict_proba(frame.iloc[paired(select_at)])[:, 1], len(select_at), target)
                metrics = probability_metrics(selection_y, calibrate_predict(cal, raw_selection))
                trial = {"depth": depth, "trees": model.tree_count_, "validation": metrics}
                trials.setdefault(arm, []).append(trial)
                print(f"{target} {arm} depth={depth} val_loss={metrics['log_loss']:.6f} trees={model.tree_count_}", flush=True)
                if best is None or metrics["log_loss"] < best[0]:
                    best = (metrics["log_loss"], model, cal, trial)
            models[arm] = best
            del frame
        chosen = min(arms, key=lambda arm: models[arm][0])
        atomic_json(out / f"{target}_selection.json", {"chosen": chosen, "trials": trials,
                                                     "rule": "minimum calibrated validation log loss"})
        test_predictions, metrics = {}, {}
        for arm in arms:
            frame = feature_frame(pro["heroes"], context, context_names, public_scores, profiles, arm)
            _, model, cal, trial = models[arm]
            raw = collapse(model.predict_proba(frame.iloc[paired(test_at)])[:, 1], len(test_at), target)
            test_predictions[arm] = calibrate_predict(cal, raw)
            metrics[arm] = probability_metrics(test_y, test_predictions[arm])
            if arm in {chosen, "relative", "pro_only", "public_draft"}:
                model.save_model(str(out / f"{target}_{arm}.cbm"))
                joblib.dump(cal, out / f"{target}_{arm}_calibration.joblib")
            del frame
        days = pro["ts"][test_at]//86400
        if target == "side":
            days = np.tile(days, 2)
        comparisons = {base: clustered_loss_delta(test_y, test_predictions[chosen], test_predictions[base], days)
                       for base in ("pro_only", "public_draft")}
        report["targets"][target] = {"chosen_on_validation": chosen, "trials": trials, "test": metrics,
                                     "chosen_minus_baselines": comparisons}
        atomic_npz(out / f"{target}_test_predictions.npz", y=test_y, mids=np.tile(pro["mids"][test_at], 2 if target=="side" else 1),
                   days=days, **test_predictions)
        atomic_json(out / "progress.json", report)
        print(f"{target} chosen={chosen} test={metrics[chosen]}", flush=True)
    report["elapsed_seconds"] = time.monotonic()-begin
    atomic_json(out / "summary.json", report)
    print("DONE kills transfer", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-rows", required=True)
    parser.add_argument("--pro-rows", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--train-from", default="2026-06-01")
    parser.add_argument("--val-from", default="2026-08-01")
    parser.add_argument("--test-from", default="2026-08-20")
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    with threadpool_limits(limits=args.threads):
        train(args)
