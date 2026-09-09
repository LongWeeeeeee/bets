#!/usr/bin/env python3
"""Train a sealed draft/history model for overall signed team net worth at 10m."""
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
from scipy.optimize import minimize_scalar

from base.laning_model import build_history
from base.team_laning_model import (
    CLASS_NAMES, FEATURE_SET_DRAFT, FEATURE_SET_HISTORY, TeamLaningModel,
    team_features, team_labels, temperature_scale,
)
from scripts.ops.train_laning_model import atomic_json, atomic_npz, sample_rows


HISTORY_DELAY_SECONDS = 3600
RECENT_WINDOW_SECONDS = 30 * 86400


def sha(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def array_sha(values):
    return hashlib.sha256(np.ascontiguousarray(values).tobytes()).hexdigest()


def logloss(labels, probabilities):
    labels = np.asarray(labels, dtype=int)
    probabilities = np.asarray(probabilities, dtype=float)
    if probabilities.shape != (len(labels), 3):
        raise ValueError("probabilities must have shape (maps,3)")
    return float(-np.log(np.maximum(probabilities[np.arange(len(labels)), labels], 1e-15)).mean())


def metrics(labels, probabilities):
    labels = np.asarray(labels, dtype=int)
    probabilities = np.asarray(probabilities, dtype=float)
    observed = np.eye(3)[labels]
    return {
        "n": int(len(labels)),
        "logloss3": logloss(labels, probabilities),
        "accuracy3": float(np.mean(probabilities.argmax(axis=1) == labels)),
        "class_counts": np.bincount(labels, minlength=3).tolist(),
        "brier_multiclass_sum": float(np.mean(np.sum((probabilities - observed) ** 2, axis=1))),
    }


def fit_temperature(labels, probabilities):
    def objective(temperature):
        return logloss(labels, temperature_scale(probabilities, temperature))
    fit = minimize_scalar(objective, bounds=(0.6, 1.6), method="bounded")
    return float(fit.x) if fit.success and objective(fit.x) < objective(1.0) else 1.0


def day_bootstrap_comparison(timestamps, labels, baseline, candidate):
    """Candidate-minus-baseline loss with maps, rather than slots, as blocks."""
    labels = np.asarray(labels, dtype=int)
    rows = np.arange(len(labels))
    delta = (np.log(np.maximum(baseline[rows, labels], 1e-15)) -
             np.log(np.maximum(candidate[rows, labels], 1e-15)))
    days, inverse = np.unique(np.asarray(timestamps) // 86400, return_inverse=True)
    counts = np.bincount(inverse)
    sums = np.bincount(inverse, weights=delta)
    draws = np.random.default_rng(266).integers(0, len(days), (2000, len(days)))
    boot = sums[draws].sum(axis=1) / counts[draws].sum(axis=1)
    return {
        "candidate_minus_baseline_loss": float(delta.mean()),
        "day_block_95ci": np.quantile(boot, [.025, .975]).tolist(),
        "days": int(len(days)),
        "draws": 2000,
    }


def _load_rows(directory, filename):
    with np.load(Path(directory) / filename) as source:
        return {key: source[key] for key in source.files}


def _summary_counts(directory):
    summary = json.loads((Path(directory) / "summary.json").read_text())
    counts = summary.get("split", {}).get("sampled_maps")
    if not isinstance(counts, list) or len(counts) != 3:
        raise ValueError(f"{directory} has no three-way sampled-map split")
    return [int(value) for value in counts]


def partition_team(corpus, v1_model, v2_model, train_maps, eval_maps):
    """Reuse v2 fit/validation IDs and reserve an unseen confirmation before fit."""
    v1 = _load_rows(v1_model, "selected_rows.npz")
    v2 = _load_rows(v2_model, "reserved_rows.npz")
    v1_counts, v2_counts = _summary_counts(v1_model), _summary_counts(v2_model)
    if "indices" not in v1 or "mid" not in v1 or "indices" not in v2 or "mid" not in v2:
        raise ValueError("baseline reserved rows are missing indices or match IDs")
    if len(v1["indices"]) != sum(v1_counts) or len(v2["indices"]) != sum(v2_counts):
        raise ValueError("baseline reserved rows do not match their summary split")
    for name, rows in (("v1", v1), ("v2", v2)):
        indices = rows["indices"].astype(int)
        if np.any(indices < 0) or np.any(indices >= len(corpus["mid"])):
            raise ValueError(f"{name} reserved indices are outside corpus")
        if not np.array_equal(rows["mid"], corpus["mid"][indices]):
            raise ValueError(f"{name} reserved IDs do not match corpus")
    if train_maps > v2_counts[0] or eval_maps > v2_counts[1]:
        raise ValueError("requested train/eval sizes exceed reusable v2 partitions")
    finite = np.isfinite(corpus["team_nw10"])
    train = v2["indices"][:train_maps].astype(int)
    validation = v2["indices"][v2_counts[0]:v2_counts[0] + eval_maps].astype(int)
    train, validation = train[finite[train]], validation[finite[validation]]
    if len(train) < 100 or len(validation) < 100:
        raise ValueError("need at least 100 finite-target reused train and validation maps")
    cuts = json.loads((Path(v2_model) / "plan.json").read_text()).get("split", {}).get("cuts_unix")
    if not isinstance(cuts, list) or len(cuts) != 2:
        raise ValueError("v2 plan has no chronological split cuts")
    v1_test = v1["mid"][sum(v1_counts[:2]):]
    v2_test = v2["mid"][sum(v2_counts[:2]):]
    excluded = np.union1d(v1_test, v2_test)
    future = np.flatnonzero(finite & (corpus["ts"] >= int(cuts[1])))
    fresh = future[~np.isin(corpus["mid"][future], excluded)]
    test = sample_rows(fresh, corpus["mid"], eval_maps)
    if len(test) < eval_maps:
        raise ValueError("insufficient fresh finite-target confirmation IDs")
    if np.intersect1d(corpus["mid"][test], excluded).size:
        raise ValueError("confirmation overlaps a previous test")
    if np.intersect1d(test, np.concatenate((train, validation))).size:
        raise ValueError("confirmation overlaps reusable fit rows")
    info = {
        "reused_v2_sampled_maps": [int(len(train)), int(len(validation))],
        "confirmation_maps": int(len(test)),
        "v1_test_ids_excluded": int(len(v1_test)),
        "v2_test_ids_excluded": int(len(v2_test)),
        "available_fresh_confirmation_maps": int(len(fresh)),
        "cuts_unix": [int(cuts[0]), int(cuts[1])],
        "confirmation": "Unseen IDs after excluding both v1 and v2 tests; historical, not prospective",
    }
    return np.concatenate((train, validation, test)), info


def v2_history_positions(v2_model, selected_fit_rows):
    """Locate reused v2 IDs in the accompanying hc cache without assuming order."""
    reserved = _load_rows(v2_model, "reserved_rows.npz")
    with np.load(Path(v2_model) / "inputs.npz") as source:
        if set(("mid", "hc")) - set(source.files):
            raise ValueError("v2 inputs do not contain hc(10,12) aligned with reserved rows")
        input_mid, input_hc = source["mid"], source["hc"]
    if (input_hc.ndim != 3 or input_hc.shape[1:] != (10, 12) or
            len(input_mid) != len(reserved["mid"])):
        raise ValueError("v2 inputs do not contain hc(10,12) aligned with reserved rows")
    order = np.argsort(reserved["indices"])
    sorted_indices = reserved["indices"][order]
    locations = np.searchsorted(sorted_indices, selected_fit_rows)
    if (np.any(locations >= len(sorted_indices)) or
            not np.array_equal(sorted_indices[locations], selected_fit_rows)):
        raise ValueError("reused team rows are not present in v2 history inputs")
    positions = order[locations]
    if not np.array_equal(input_mid[positions], reserved["mid"][positions]):
        raise ValueError("v2 history input IDs are not aligned with reserved rows")
    return input_hc[positions]


def _candidate_metadata(with_history):
    metadata = {
        "team_laning_feature_set": FEATURE_SET_HISTORY if with_history else FEATURE_SET_DRAFT,
        "team_laning_temperature": "1",
        "team_laning_class_order": ",".join(CLASS_NAMES),
    }
    if with_history:
        metadata.update(team_laning_history_delay_seconds=str(HISTORY_DELAY_SECONDS),
                        team_laning_recent_window_seconds=str(RECENT_WINDOW_SECONDS))
    return metadata


def run(args):
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    if (out / "summary.json").exists():
        raise ValueError("completed output exists; choose a fresh run directory")

    def stage(name):
        print(f"stage={name} time={time.time():.0f}", flush=True)
        atomic_json(out / "status.json", {"state": "RUNNING", "stage": name, "pid": os.getpid()})

    stage("prepare")
    with np.load(args.corpus / "rows.npz") as source:
        corpus = {key: source[key] for key in source.files}
    manifest = json.loads((args.corpus / "manifest.json").read_text())
    required = {"mid", "ts", "duration", "heroes", "accounts", "lane_labels", "team_nw10"}
    if not manifest.get("complete") or manifest.get("rows") != len(corpus["mid"]):
        raise ValueError("incomplete corpus manifest")
    if required - corpus.keys() or np.any(corpus["heroes"] >= 1024) or np.any(np.diff(corpus["ts"]) < 0):
        raise ValueError("unsupported team-laning corpus schema")
    selected, split = partition_team(corpus, args.v1_model, args.v2_model,
                                     args.train_maps, args.eval_maps)
    ntrain, nval = split["reused_v2_sampled_maps"]
    validation_slice = slice(ntrain, ntrain + nval)
    test_slice = slice(ntrain + nval, None)
    plan = {
        "target": "finite signed team_nw10: Dire (<0), exact tie (=0), Radiant (>0)",
        "class_order": CLASS_NAMES,
        "features": {
            "draft": "full positional heroes R1..5,D1..5",
            "history": "draft + flattened causal hc(10,12) + R-minus-D per-role history differences",
            "excluded": "No current-map lane labels, outcomes, or team_nw10 values are features",
        },
        "history": {"availability_delay_seconds": HISTORY_DELAY_SECONDS,
                    "recent_window_seconds": RECENT_WINDOW_SECONDS},
        "split": split,
        "iterations": args.iterations,
        "depth": 6,
        "learning_rate": .08,
        "early_stopping_rounds": 60,
        "threads": args.threads,
        "candidates": ["draft", "history"],
        "selection": "minimum raw validation three-class log loss; scalar temperature on validation only",
        "feature_source_sha256": sha(ROOT / "base/team_laning_model.py"),
        "trainer_source_sha256": sha(__file__),
        "corpus_manifest_sha256": sha(args.corpus / "manifest.json"),
        "v1_reserved_rows_sha256": sha(args.v1_model / "selected_rows.npz"),
        "v2_reserved_rows_sha256": sha(args.v2_model / "reserved_rows.npz"),
        "v2_inputs_sha256": sha(args.v2_model / "inputs.npz"),
        "v1_summary_sha256": sha(args.v1_model / "summary.json"),
        "v2_summary_sha256": sha(args.v2_model / "summary.json"),
        "v2_plan_sha256": sha(args.v2_model / "plan.json"),
        "v1_test_ids_sha256": array_sha(_load_rows(args.v1_model, "selected_rows.npz")["mid"][_summary_counts(args.v1_model)[0] + _summary_counts(args.v1_model)[1]:]),
        "v2_test_ids_sha256": array_sha(_load_rows(args.v2_model, "reserved_rows.npz")["mid"][_summary_counts(args.v2_model)[0] + _summary_counts(args.v2_model)[1]:]),
        "selected_ids_sha256": array_sha(corpus["mid"][selected]),
    }
    if (out / "plan.json").exists() and json.loads((out / "plan.json").read_text()) != plan:
        raise ValueError("plan or input contract changed; choose a fresh run directory")
    atomic_json(out / "plan.json", plan)
    atomic_npz(out / "reserved_rows.npz", indices=selected, mid=corpus["mid"][selected],
               ts=corpus["ts"][selected])
    inputs_path = out / "inputs.npz"
    if inputs_path.exists():
        data = _load_rows(out, "inputs.npz")
        if (set(data) != {"heroes", "target", "ts", "mid", "hc"} or
                not np.array_equal(data["mid"], corpus["mid"][selected]) or
                not np.array_equal(data["target"], team_labels(corpus["team_nw10"][selected]))):
            raise ValueError("cached inputs do not match selected target rows")
    else:
        stage("causal_history_new_confirmation")
        reused_hc = v2_history_positions(args.v2_model, selected[:ntrain + nval])
        new_hc = build_history(corpus, selected[ntrain + nval:], availability_delay_seconds=HISTORY_DELAY_SECONDS,
                           recent_window_seconds=RECENT_WINDOW_SECONDS)
        hc = np.concatenate((reused_hc, new_hc))
        data = {"heroes": corpus["heroes"][selected],
                "target": team_labels(corpus["team_nw10"][selected]),
                "ts": corpus["ts"][selected], "mid": corpus["mid"][selected], "hc": hc}
        atomic_npz(inputs_path, **data)
    selected_accounts = corpus["accounts"][selected]
    del corpus

    validation = {}
    validation_probabilities = {}
    candidate_paths = {}
    for name, with_history in (("draft", False), ("history", True)):
        stage("fit_" + name)
        directory = out / name
        directory.mkdir(exist_ok=True)
        model_path = directory / "team.cbm"
        cache = directory / "validation.npz"
        candidate_paths[name] = model_path
        if model_path.exists() and cache.exists():
            cached = _load_rows(directory, "validation.npz")
            if (set(cached) != {"mid", "target", "probabilities"} or
                    not np.array_equal(cached["mid"], data["mid"][validation_slice]) or
                    not np.array_equal(cached["target"], data["target"][validation_slice])):
                raise ValueError(f"{name} validation cache does not match target rows")
            validation_probabilities[name] = cached["probabilities"]
        else:
            rows = slice(0, ntrain + nval)
            features = team_features(data["heroes"][rows], data["hc"][rows] if with_history else None)
            model = CatBoostClassifier(iterations=args.iterations, depth=6, learning_rate=.08,
                loss_function="MultiClass", eval_metric="MultiClass", thread_count=args.threads,
                random_seed=42, l2_leaf_reg=5, allow_writing_files=False, one_hot_max_size=255,
                max_ctr_complexity=1, metadata=_candidate_metadata(with_history))
            model.fit(features.iloc[:ntrain], data["target"][:ntrain],
                      cat_features=[f"hero_{slot}" for slot in range(10)],
                      eval_set=(features.iloc[ntrain:], data["target"][validation_slice]),
                      early_stopping_rounds=60, verbose=100)
            if list(model.classes_) != [0, 1, 2]:
                raise ValueError("training set does not contain all team-NW10 classes")
            validation_probabilities[name] = model.predict_proba(features.iloc[ntrain:], thread_count=1)
            model.save_model(str(model_path) + ".tmp")
            os.replace(str(model_path) + ".tmp", model_path)
            atomic_npz(cache, mid=data["mid"][validation_slice], target=data["target"][validation_slice],
                       probabilities=validation_probabilities[name])
        loaded = TeamLaningModel.load(directory)
        if loaded.with_history != with_history:
            raise ValueError(f"{name} artifact metadata does not match candidate")
        replay = loaded.predict_proba(data["heroes"][validation_slice][:32],
                                      data["hc"][validation_slice][:32] if with_history else None)
        delta = float(np.max(np.abs(replay - validation_probabilities[name][:32])))
        if delta > 1e-12:
            raise ValueError(f"{name} serialized inference mismatch")
        validation[name] = metrics(data["target"][validation_slice], validation_probabilities[name])
        validation[name].update(trees=int(loaded.model.tree_count_), reload_max_delta=delta)
        atomic_json(directory / "validation.json", validation[name])

    winner = min(validation, key=lambda name: validation[name]["logloss3"])
    temperature = fit_temperature(data["target"][validation_slice], validation_probabilities[winner])
    selected_validation = temperature_scale(validation_probabilities[winner], temperature)
    selection = {
        "selected": winner,
        "temperature": temperature,
        "validation": validation,
        "selected_calibrated_validation": metrics(data["target"][validation_slice], selected_validation),
    }
    atomic_json(out / "selection.json", selection)
    selected_model = CatBoostClassifier()
    selected_model.load_model(str(candidate_paths[winner]))
    selected_model.get_metadata()["team_laning_temperature"] = str(temperature)
    selected_dir = out / "selected"
    selected_dir.mkdir(exist_ok=True)
    selected_path = selected_dir / "team.cbm"
    selected_model.save_model(str(selected_path) + ".tmp")
    os.replace(str(selected_path) + ".tmp", selected_path)
    selected_loaded = TeamLaningModel.load(selected_dir)
    selected_delta = float(np.max(np.abs(
        selected_loaded.predict_proba(data["heroes"][validation_slice][:32],
                                      data["hc"][validation_slice][:32] if winner == "history" else None) -
        selected_validation[:32])))
    if selected_delta > 1e-12:
        raise ValueError("selected serialized inference mismatch")

    # Selection and the calibration parameter are sealed before target labels of
    # the fresh confirmation set are used for evaluation.
    stage("sealed_confirmation")
    predictions = {}
    for name, with_history in (("draft", False), ("history", True)):
        predictions[name] = TeamLaningModel.load(out / name).predict_proba(
            data["heroes"][test_slice], data["hc"][test_slice] if with_history else None)
    predictions["selected_calibrated"] = temperature_scale(predictions[winner], temperature)
    replay = selected_loaded.predict_proba(data["heroes"][test_slice][:64],
                                           data["hc"][test_slice][:64] if winner == "history" else None)
    test_reload_delta = float(np.max(np.abs(replay - predictions["selected_calibrated"][:64])))
    if test_reload_delta > 1e-12:
        raise ValueError("selected confirmation reload mismatch")
    test_target = data["target"][test_slice]
    train_target = data["target"][:ntrain]
    frequencies = (np.bincount(train_target, minlength=3) + 1) / (len(train_target) + 3)
    predictions["frequency"] = np.broadcast_to(frequencies, (len(test_target), 3)).copy()
    result = {name: metrics(test_target, probabilities)
              for name, probabilities in predictions.items()}
    comparisons = {name: day_bootstrap_comparison(data["ts"][test_slice], test_target,
                                                   predictions["frequency"], probabilities)
                   for name, probabilities in predictions.items() if name != "frequency"}
    atomic_npz(out / "test_predictions.npz", mid=data["mid"][test_slice], ts=data["ts"][test_slice],
               target=test_target, **predictions)
    probe_positions = sample_rows(np.arange(len(test_target)), data["mid"][test_slice], 64)
    probe_rows = ntrain + nval + probe_positions
    atomic_npz(out / "verification_probe.npz", schema_version=np.array(1, dtype=np.int8),
               class_order=np.asarray(CLASS_NAMES), selected= np.asarray(winner),
               availability_delay_seconds=np.array(HISTORY_DELAY_SECONDS, dtype=np.int64),
               recent_window_seconds=np.array(RECENT_WINDOW_SECONDS, dtype=np.int64),
               mid=data["mid"][probe_rows], heroes=data["heroes"][probe_rows],
               accounts=selected_accounts[probe_rows], ts=data["ts"][probe_rows],
               history=data["hc"][probe_rows],
               probabilities=predictions["selected_calibrated"][probe_positions])
    summary = {
        "complete": True, "held_out_test": True, "full_refit": False,
        "selected": winner, "temperature": temperature, "class_order": CLASS_NAMES,
        "target": plan["target"], "split": split, "validation": validation,
        "test": result, "frequency_baseline": frequencies.tolist(),
        "paired_day_bootstrap_vs_frequency": comparisons,
        "reload_max_delta": test_reload_delta,
        "verification_probe": {"schema_version": 1, "rows": int(len(probe_rows)),
                               "features": ["heroes", "accounts", "ts", "history"],
                               "probability_artifact": "selected_calibrated"},
        "corpus_manifest_sha256": plan["corpus_manifest_sha256"],
        "source_sha256": {"base/team_laning_model.py": plan["feature_source_sha256"],
                          "scripts/ops/train_team_laning_model.py": plan["trainer_source_sha256"]},
        "limitations": [
            "Exact ties are included but rare, so tie-only confirmation estimates are imprecise",
            "Player position annotations are post-match; serving requires intended positions",
            "History availability uses a modeled one-hour delay, not observed ingestion timestamps",
            "Confirmation is historical public data, not prospective or pro validation",
        ],
    }
    atomic_json(out / "summary.json", summary)
    atomic_json(out / "status.json", {"state": "DONE", "stage": "complete", "pid": os.getpid()})
    print(json.dumps({"state": "DONE", "selected": winner, "test": result["selected_calibrated"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--v1-model", type=Path, required=True)
    parser.add_argument("--v2-model", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--train-maps", type=int, default=800000)
    parser.add_argument("--eval-maps", type=int, default=100000)
    parser.add_argument("--iterations", type=int, default=600)
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()
    try:
        run(args)
    except BaseException as exc:
        atomic_json(args.output_dir / "status.json", {"state": "FAIL", "stage": str(exc), "pid": os.getpid()})
        raise
