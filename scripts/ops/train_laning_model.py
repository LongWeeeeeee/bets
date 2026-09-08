#!/usr/bin/env python3
"""Offline lane training: predeclared chronological split and causal history ablation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import numpy as np
from catboost import CatBoostClassifier
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score

from base.laning_model import (CLASS_NAMES, LANE_NAMES, LaningModel,
                               build_history, lane_features)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False))
    os.replace(temporary, path)


def atomic_npz(path, **values):
    with open(str(path) + ".tmp", "wb") as handle:
        np.savez_compressed(handle, **values)
    os.replace(str(path) + ".tmp", path)


def sample_rows(indices, mids, maximum):
    if len(indices) <= maximum:
        return indices
    # Outcome-independent deterministic sample, with all lanes in the same set.
    hashed = mids[indices].astype(np.uint64) * np.uint64(11400714819323198485)
    chosen = np.argpartition(hashed, maximum)[:maximum]
    return np.sort(indices[chosen])


def splits(corpus, train_max, evaluation_max):
    ts = corpus["ts"]
    cuts = [int(ts[int(len(ts) * fraction)]) for fraction in (.6, .8)]
    end = ts + corpus["duration"]
    eligible = np.all(corpus["lane_labels"] >= 0, axis=1)
    masks = [eligible & (ts < cuts[0]) & (end < cuts[0]),
             eligible & (ts >= cuts[0]) & (ts < cuts[1]) & (end < cuts[1]),
             eligible & (ts >= cuts[1])]
    rows = [sample_rows(np.flatnonzero(mask), corpus["mid"], cap)
            for mask, cap in zip(masks, (train_max, evaluation_max, evaluation_max))]
    if any(len(row) < 100 for row in rows):
        raise ValueError("Need at least 100 valid maps in each temporal partition")
    return rows, {"cuts_unix": cuts, "eligible_maps": [int(mask.sum()) for mask in masks],
                  "sampled_maps": [len(row) for row in rows],
                  "purged_boundary_maps": int((eligible & (ts < cuts[1]) & ~np.logical_or(masks[0], masks[1])).sum()),
                  "split": "60/20/20 by full-corpus start time; strict completion purge", "seed": 42}


def collapse(prob):
    return np.stack([prob[..., :2].sum(axis=-1), prob[..., 2], prob[..., 3:].sum(axis=-1)], axis=-1)


def metrics(y, prob):
    y, prob = y.reshape(-1), prob.reshape(-1, 5)
    three = np.sign(y - 2).astype(int) + 1
    p3 = collapse(prob)
    decided = y != 2
    conditional = prob[:, 3:].sum(axis=1) / np.maximum(1 - prob[:, 2], 1e-12)
    out = {"n": len(y), "logloss5": float(log_loss(y, prob, labels=list(range(5)))),
           "accuracy5": float(accuracy_score(y, prob.argmax(axis=1))),
           "logloss3": float(log_loss(three, p3, labels=[0, 1, 2])),
           "accuracy3": float(accuracy_score(three, p3.argmax(axis=1))),
           "class_counts": np.bincount(y, minlength=5).tolist(),
           "decisive_n": int(decided.sum()),
           "decisive_accuracy": float(np.mean((conditional[decided] >= .5) == (y[decided] > 2)))}
    if len(np.unique(y[decided] > 2)) == 2:
        out["decisive_auc"] = float(roc_auc_score(y[decided] > 2, conditional[decided]))
    confidence = np.maximum(p3[:, 0], p3[:, 2])
    direction = np.where(p3[:, 2] >= p3[:, 0], 2, 0)
    out["selection_all_lanes_including_ties"] = {}
    for coverage in (.1, .25, .5, 1):
        take = np.argsort(-confidence, kind="stable")[:max(1, int(len(y) * coverage))]
        out["selection_all_lanes_including_ties"][str(coverage)] = {
            "n": len(take), "win_accuracy_ties_are_errors": float(np.mean(direction[take] == three[take])),
            "mean_declared_win_probability": float(confidence[take].mean()),
            "tie_rate": float(np.mean(three[take] == 1))}
    return out


def daily_loss_comparison(ts, y, draft, history):
    # Resample day blocks; keep all three correlated lanes of each map together.
    rows = np.arange(len(y.reshape(-1)))
    a = -np.log(np.maximum(draft.reshape(-1, 5)[rows, y.reshape(-1)], 1e-12)).reshape(-1, 3).mean(axis=1)
    b = -np.log(np.maximum(history.reshape(-1, 5)[rows, y.reshape(-1)], 1e-12)).reshape(-1, 3).mean(axis=1)
    days, inv = np.unique(ts // 86400, return_inverse=True)
    sums = np.bincount(inv, weights=b - a)
    counts = np.bincount(inv)
    rng = np.random.default_rng(42)
    draws = rng.integers(0, len(days), size=(1000, len(days)))
    boot = sums[draws].sum(axis=1) / counts[draws].sum(axis=1)
    return {"history_minus_draft_logloss": float(np.mean(b - a)),
            "day_block_95ci": np.quantile(boot, [.025, .975]).tolist(), "days": len(days)}


def train(args):
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    if (output / "summary.json").exists():
        raise ValueError("Completed output exists; choose a fresh run directory")
    def stage(name):
        print(f"stage={name} time={time.time():.0f}", flush=True)
        atomic_json(output / "status.json", {"state": "RUNNING", "stage": name, "pid": os.getpid()})
    stage("load_corpus")
    with np.load(args.corpus / "rows.npz") as source:
        corpus = {key: source[key] for key in source.files}
    manifest = json.loads((args.corpus / "manifest.json").read_text())
    if not manifest.get("complete") or manifest["rows"] != len(corpus["mid"]):
        raise ValueError("Incomplete corpus manifest")
    if np.any(corpus["heroes"] >= 1024):
        raise ValueError("Hero key encoding requires ids <1024")
    if np.any(np.diff(corpus["ts"]) < 0):
        raise ValueError("Corpus is not chronological")
    partitions, split_info = splits(corpus, args.train_maps, args.eval_maps)
    selected = np.concatenate(partitions)
    lengths = [len(x) for x in partitions]
    offsets = np.cumsum([0] + lengths)
    atomic_json(output / "plan.json", {"target": "STRATZ five-class lane outcomes",
        "split": split_info, "iterations": args.iterations, "depth": 6,
        "model_selection": "minimum validation five-class logloss; no test tuning",
        "history": "all source maps ending strictly before predicted start; fixed prior10",
        "per_lane_nw10": "unavailable; not inferred from final player networth or team NW"})
    stage("causal_history")
    hist = build_history(corpus, selected)
    atomic_npz(output / "selected_rows.npz", indices=selected, history=hist,
               mid=corpus["mid"][selected], ts=corpus["ts"][selected])
    y = corpus["lane_labels"][selected].astype(int)
    heroes = corpus["heroes"][selected]
    del selected
    validation = {}
    predictions = {}
    for name, use_history in (("draft", False), ("history", True)):
        stage("fit_" + name)
        x = lane_features(heroes, hist if use_history else None)
        categories = [key for key in x.columns if not key.startswith("history_")]
        model = CatBoostClassifier(iterations=args.iterations, depth=6, learning_rate=.08,
            loss_function="MultiClass", eval_metric="MultiClass", thread_count=args.threads,
            random_seed=42, l2_leaf_reg=5, allow_writing_files=False,
            one_hot_max_size=255, max_ctr_complexity=1)
        end_train, end_val = offsets[1] * 3, offsets[2] * 3
        model.fit(x.iloc[:end_train], y[:offsets[1]].reshape(-1), cat_features=categories,
                  eval_set=(x.iloc[end_train:end_val], y[offsets[1]:offsets[2]].reshape(-1)),
                  early_stopping_rounds=40, verbose=50)
        if list(model.classes_) != [0, 1, 2, 3, 4]:
            raise ValueError("Training set does not contain all five outcomes")
        temporary = output / (name + ".cbm.tmp")
        model.save_model(str(temporary))
        os.replace(temporary, output / (name + ".cbm"))
        val = model.predict_proba(x.iloc[end_train:end_val]).reshape(-1, 3, 5)
        validation[name] = metrics(y[offsets[1]:offsets[2]], val)
        validation[name]["trees"] = model.tree_count_
        # Freeze selection on validation before opening test results below.
        predictions[name] = model.predict_proba(x.iloc[end_val:]).reshape(-1, 3, 5)
        probe_heroes = heroes[offsets[2]:offsets[2] + 32]
        probe_hist = hist[offsets[2]:offsets[2] + 32]
        reloaded = LaningModel.load(output, use_history)
        delta = float(np.max(np.abs(reloaded.predict_proba(probe_heroes, probe_hist) - predictions[name][:32])))
        if delta > 1e-12:
            raise ValueError("Serialized inference does not reproduce predictions")
        validation[name]["reload_max_delta"] = delta
        del x, model
    winner = min(validation, key=lambda name: validation[name]["logloss5"])
    atomic_json(output / "selection.json", {"selected": winner, "validation": validation})
    stage("heldout_evaluation")
    test_y = y[offsets[2]:]
    test_rows = partitions[2]
    train_y = y[:offsets[1]]
    frequencies = np.stack([(np.bincount(train_y[:, lane], minlength=5) + 1) /
                            (len(train_y) + 5) for lane in range(3)])
    predictions["frequency"] = np.broadcast_to(frequencies, (len(test_y), 3, 5)).copy()
    results = {name: {"overall": metrics(test_y, pred),
                     "lanes": {lane_name: metrics(test_y[:, lane], pred[:, lane])
                               for lane, lane_name in enumerate(LANE_NAMES)}}
               for name, pred in predictions.items()}
    team = {}
    expected_lane_score = predictions[winner] @ np.arange(-2, 3)
    team_score = expected_lane_score.sum(axis=1)
    for threshold in (1000, 1500):
        leads = corpus["team_nw10"][test_rows]
        take = np.isfinite(leads) & (np.abs(leads) >= threshold)
        team[str(threshold)] = {"n": int(take.sum()), "diagnostic_only": True,
            "direction_accuracy": (float(np.mean((team_score[take] > 0) == (leads[take] > 0)))
                                   if take.any() else None),
            "target": "team NW source index10; not per-lane gold"}
    atomic_npz(output / "test_predictions.npz", mid=corpus["mid"][test_rows], ts=corpus["ts"][test_rows],
               labels=test_y, draft=predictions["draft"], history=predictions["history"])
    summary = {"complete": True, "selected": winner, "held_out_test": True,
        "full_refit": False, "target": "STRATZ lane outcomes", "class_order": CLASS_NAMES,
        "lane_order": LANE_NAMES, "source_lane_order": ["bottom", "mid", "top"],
        "perspective": "Radiant", "corpus_maps": len(corpus["mid"]), "split": split_info,
        "validation": validation, "test": results, "team_nw_diagnostic": team,
        "paired_loss": daily_loss_comparison(corpus["ts"][test_rows], test_y,
                                             predictions["draft"], predictions["history"]),
        "history_coverage_test": float(np.mean(hist[offsets[2]:, :, 2] > 0)),
        "corpus_manifest_sha256": hashlib.sha256((args.corpus / "manifest.json").read_bytes()).hexdigest(),
        "source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in (Path(__file__), ROOT / "base/laning_model.py", ROOT / "base/build_laning_corpus.py")},
        "limitations": ["No individual NW10: no per-lane 1000/1500 gold probabilities",
            "Public temporal holdout only, no independent pro validation",
            "Historical positions are post-match annotations; inference needs known intended positions",
            "End time is an availability proxy; archived ingestion delay unknown",
            "Deterministic training/evaluation caps; history uses all eligible source records"]}
    atomic_json(output / "summary.json", summary)
    atomic_json(output / "status.json", {"state": "DONE", "stage": "complete", "pid": os.getpid()})
    print(json.dumps({"state": "DONE", "selected": winner, "test": results[winner]["overall"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--train-maps", type=int, default=400000)
    parser.add_argument("--eval-maps", type=int, default=100000)
    parser.add_argument("--iterations", type=int, default=300)
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()
    try:
        train(args)
    except BaseException as exc:
        atomic_json(args.output_dir / "status.json", {"state": "FAIL", "stage": str(exc), "pid": os.getpid()})
        raise
