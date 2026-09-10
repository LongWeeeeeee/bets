#!/usr/bin/env python3
"""Leakage-safe, offline C/design tuning for binary draft phase directions.

This tool deliberately never reads a fresh-pro corpus.  Its public test is an
already exposed diagnostic, while recipe selection is made only by validation
log loss.  The optional full artifact ends strictly before --fresh-after-ts.
"""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import joblib
import numpy as np
from sklearn.metrics import log_loss, roc_auc_score
from threadpoolctl import threadpool_limits

from base.draft_features import KIND_PAIR, KIND_POSITION_PAIR, DraftFeatureEncoder
from base.train_draft_phase_models import (
    atomic_json,
    chronological_cuts,
    csr_view,
    disk_design,
    fit_logistic,
    load_rows,
)

PHASES = ("early_win", "early_nw", "late", "all")
POSITION_KIND = KIND_POSITION_PAIR
ROLE_PAIR_KIND = KIND_PAIR
DEFAULT_GRID = (0.0001, 0.0003, 0.001, 0.003)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _optional_sha256(path: Path) -> str | None:
    return _sha256(path) if path.exists() else None


def _atomic_joblib(value: object, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    joblib.dump(value, temporary, compress=3)
    os.replace(temporary, path)


def _log(message: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def phase_rows_before_direction(rows: Mapping[str, np.ndarray], phase: str) -> dict[str, np.ndarray]:
    """Filter the phase population, retaining early-NW no_marker for split cuts."""
    keep = rows["duration"] >= 1200
    if phase == "late":
        keep &= rows["duration"] >= 2160
        target = "wins"
    elif phase == "early_win":
        keep &= rows["duration"] <= 2040
        target = "wins"
    elif phase == "all":
        target = "wins"
    elif phase == "early_nw":
        target = "early_nw"
    else:
        raise ValueError(f"unknown phase {phase!r}")
    keep &= rows[target] >= 0
    return {key: np.asarray(rows[key])[keep] for key in ("heroes", "mid", "ts", "duration", target)}


def development_splits(phase_rows: Mapping[str, np.ndarray], phase: str, embargo_seconds: int) -> dict[str, np.ndarray]:
    """Return binary direction splits with original (pre-no-marker) cut points."""
    ts = np.asarray(phase_rows["ts"])
    duration = np.asarray(phase_rows["duration"])
    target_key = "early_nw" if phase == "early_nw" else "wins"
    y_raw = np.asarray(phase_rows[target_key])
    train_end, test_start = chronological_cuts(ts)
    validation_first_ts = ts[train_end]
    public_test_first_ts = ts[test_start]
    groups = {
        "train": np.arange(len(ts)) < train_end,
        "validation": (np.arange(len(ts)) >= train_end) & (np.arange(len(ts)) < test_start),
        "test": np.arange(len(ts)) >= test_start,
    }
    # A label may enter a fold only after the game and its observation window end.
    groups["train"] &= ts + duration + embargo_seconds < validation_first_ts
    groups["validation"] &= ts + duration + embargo_seconds < public_test_first_ts
    if phase == "early_nw":
        direction = y_raw != 2
        for key in groups:
            groups[key] &= direction
    result = {key: np.flatnonzero(mask) for key, mask in groups.items()}
    if not len(result["train"]) or not len(result["validation"]) or not len(result["test"]):
        raise ValueError(f"{phase}: embargo/direction filtering left an empty split")
    for key in ("train", "validation", "test"):
        labels = y_raw[result[key]]
        if set(np.unique(labels)) != {0, 1}:
            raise ValueError(f"{phase}: {key} needs both binary direction classes")
    result["raw_train_end"] = np.asarray([train_end])
    result["raw_test_start"] = np.asarray([test_start])
    return result


def predict_binary(bundle: Mapping[str, object], heroes: np.ndarray, chunk_size: int = 25_000) -> np.ndarray:
    """Predict radiant-direction probability from a binary tuning artifact."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    heroes = np.asarray(heroes)
    if heroes.ndim != 2 or heroes.shape[1] != 10:
        raise ValueError("heroes must be (n, 10)")
    encoder, classifier = bundle["encoder"], bundle["classifier"]
    output = np.empty(len(heroes), dtype=np.float64)
    for start in range(0, len(heroes), chunk_size):
        block = heroes[start:start + chunk_size]
        output[start:start + len(block)] = classifier.predict_proba(encoder.transform(block))[:, 1]
    if not np.isfinite(output).all() or np.any((output < 0) | (output > 1)):
        raise ValueError("binary artifact returned invalid probabilities")
    return output


def binary_metrics(y: np.ndarray, p: np.ndarray) -> dict[str, Any]:
    y, p = np.asarray(y, dtype=np.int8), np.asarray(p, dtype=np.float64)
    if len(y) != len(p) or not len(y):
        raise ValueError("non-empty, aligned labels and probabilities required")
    # sklearn's argmax breaks a .5 tie toward class 0 (Dire).
    prediction = (p > 0.5).astype(np.int8)
    confidence = np.maximum(p, 1 - p)
    result: dict[str, Any] = {
        "rows": int(len(y)),
        "class_counts": np.bincount(y, minlength=2).tolist(),
        "log_loss": float(log_loss(y, p, labels=[0, 1])),
        "accuracy": float((prediction == y).mean()),
        "auc": float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else None,
        "equal_coverage": {},
    }
    for share in (0.1, 0.2, 0.3, 0.5):
        chosen = np.argsort(-confidence, kind="stable")[:max(1, int(len(y) * share))]
        result["equal_coverage"][str(share)] = {
            "rows": int(len(chosen)),
            "accuracy": float((prediction[chosen] == y[chosen]).mean()),
        }
    return result


def paired_day_bootstrap(
    ts: np.ndarray,
    y: np.ndarray,
    baseline: np.ndarray,
    candidate: np.ndarray,
    *,
    repetitions: int = 2000,
    seed: int = 20260910,
) -> dict[str, dict[str, float | int]]:
    """Paired bootstrap over UTC days; candidate minus baseline for both metrics."""
    ts, y = np.asarray(ts), np.asarray(y, dtype=np.int8)
    baseline, candidate = np.asarray(baseline), np.asarray(candidate)
    if not (len(ts) == len(y) == len(baseline) == len(candidate)) or not len(y):
        raise ValueError("aligned non-empty arrays required")
    days, inverse = np.unique(ts // 86_400, return_inverse=True)
    day_loss = np.empty(len(days), dtype=np.float64)
    day_accuracy = np.empty(len(days), dtype=np.float64)
    for index in range(len(days)):
        mask = inverse == index
        day_loss[index] = log_loss(y[mask], candidate[mask], labels=[0, 1]) - log_loss(
            y[mask], baseline[mask], labels=[0, 1]
        )
        day_accuracy[index] = ((candidate[mask] > .5).astype(np.int8) == y[mask]).mean() - (
            (baseline[mask] > .5).astype(np.int8) == y[mask]
        ).mean()
    rng = np.random.default_rng(seed)
    sampled = rng.integers(0, len(days), size=(repetitions, len(days)))
    def interval(values: np.ndarray) -> dict[str, float | int]:
        samples = values[sampled].mean(axis=1)
        return {"days": int(len(days)), "repetitions": int(repetitions),
                "candidate_minus_baseline": float(values.mean()),
                "ci95_low": float(np.quantile(samples, .025)),
                "ci95_high": float(np.quantile(samples, .975))}
    return {"log_loss": interval(day_loss), "accuracy": interval(day_accuracy)}


def _historical_c(report: Mapping[str, object], phase: str) -> float:
    selection = report["selection"]
    if phase == "early_nw":
        selection = selection["direction"]
    return float(selection["selected_C"])


def incumbent_recipe(baseline_dir: Path, phase: str) -> tuple[dict[str, Any], Path]:
    directory = baseline_dir / phase / POSITION_KIND
    report_path = directory / "results.json"
    model_path = directory / "model.joblib"
    if not report_path.exists() or not model_path.exists():
        raise FileNotFoundError(f"missing E260 position baseline for {phase}: {directory}")
    report = json.loads(report_path.read_text())
    return {"design": POSITION_KIND, "C": _historical_c(report, phase), "incumbent": True}, model_path


def role_pair_recipe(baseline_dir: Path, phase: str) -> dict[str, Any]:
    report_path = baseline_dir / phase / ROLE_PAIR_KIND / "results.json"
    if not report_path.exists():
        raise FileNotFoundError(f"missing E260 role-pair metadata for {phase}: {report_path}")
    return {"design": ROLE_PAIR_KIND, "C": _historical_c(json.loads(report_path.read_text()), phase), "incumbent": False}


def choose_recipe(trials: list[dict[str, Any]]) -> dict[str, Any]:
    """Validation only; exact ties intentionally retain the incumbent recipe."""
    if not trials:
        raise ValueError("no trials")
    incumbent = next((trial for trial in trials if trial["recipe"]["incumbent"]), None)
    return min(trials, key=lambda trial: (trial["validation_log_loss"], 0 if trial is incumbent else 1))


def _fit_design(
    heroes: np.ndarray,
    y: np.ndarray,
    split: Mapping[str, np.ndarray],
    recipes: list[dict[str, Any]],
    scratch: Path,
    max_iter: int,
    phase: str,
) -> list[dict[str, Any]]:
    """Build one disk design per feature design and fit its candidates sequentially."""
    train, validation = split["train"], split["validation"]
    design = recipes[0]["design"]
    encoder = DraftFeatureEncoder.fit(heroes[train], design, True)
    matrix = disk_design(encoder, heroes, scratch / phase / design)
    trials: list[dict[str, Any]] = []
    try:
        for recipe in recipes:
            started = time.monotonic()
            model = fit_logistic(_indexed_csr_view(matrix, train), y[train], recipe["C"], max_iter,
                                 f"{phase}/{design}/selection")
            probability = model.predict_proba(_indexed_csr_view(matrix, validation))[:, 1]
            trials.append({"recipe": recipe, "validation_log_loss": float(log_loss(y[validation], probability, labels=[0, 1])),
                           "fit_seconds": float(time.monotonic() - started)})
            del model, probability
    finally:
        del matrix
        gc.collect()
    return trials


def _indexed_csr_view(matrix: object, indices: np.ndarray) -> object:
    """Keep disk CSR contiguous where possible; embargo holes are then indexed."""
    start, stop = int(indices[0]), int(indices[-1]) + 1
    view = csr_view(matrix, start, stop)
    return view if len(indices) == stop - start else view[indices - start]


def _fit_recipe(
    heroes: np.ndarray, y: np.ndarray, split: Mapping[str, np.ndarray], recipe: Mapping[str, Any],
    scratch: Path, max_iter: int, phase: str, *, fit_indices: np.ndarray | None = None,
) -> tuple[DraftFeatureEncoder, object]:
    train = split["train"]
    fit_indices = np.concatenate((split["train"], split["validation"])) if fit_indices is None else fit_indices
    encoder = DraftFeatureEncoder.fit(heroes[train], recipe["design"], True)
    matrix = disk_design(encoder, heroes, scratch / phase / (recipe["design"] + "_refit"))
    try:
        model = fit_logistic(_indexed_csr_view(matrix, fit_indices), y[fit_indices], recipe["C"], max_iter,
                             f"{phase}/{recipe['design']}/refit")
    finally:
        del matrix
        gc.collect()
    return encoder, model


def _binary_bundle(phase: str, encoder: object, classifier: object, metadata: Mapping[str, object]) -> dict[str, object]:
    return {"schema_version": 1, "phase": phase, "target": "radiant_direction_given_marker" if phase == "early_nw" else "radiant_win",
            "encoder": encoder, "classifier": classifier, "metadata": dict(metadata)}


def _validated_binary_prediction(bundle: Mapping[str, object], heroes: np.ndarray) -> np.ndarray:
    classes = np.asarray(bundle["classifier"].classes_)
    if not np.array_equal(classes, np.asarray([0, 1])):
        raise ValueError(f"binary artifact classes must be [0, 1], got {classes.tolist()}")
    return predict_binary(bundle, heroes)


def _full_artifact(
    rows: Mapping[str, np.ndarray], phase: str, recipe: Mapping[str, Any],
    fresh_after_ts: int, embargo_seconds: int, scratch: Path, max_iter: int, baseline_model: Path,
) -> tuple[dict[str, object], dict[str, Any]]:
    phase_data = phase_rows_before_direction(rows, phase)
    target = "early_nw" if phase == "early_nw" else "wins"
    eligible = (phase_data["ts"] < fresh_after_ts) & (
        phase_data["ts"] + phase_data["duration"] + embargo_seconds < fresh_after_ts
    )
    binary = phase_data[target] != 2 if phase == "early_nw" else np.ones(len(phase_data[target]), dtype=bool)
    eligible &= binary
    indices = np.flatnonzero(eligible)
    if not len(indices) or set(np.unique(phase_data[target][indices])) != {0, 1}:
        raise ValueError(f"{phase}: no eligible binary labels before fresh cutoff")
    if recipe["incumbent"]:
        # The E260 full artifact can only stand in for this cutoff when every
        # binary label it might contain was already available then.
        if np.any(binary & ~eligible):
            raise ValueError(f"{phase}: cannot reuse incumbent before fresh cutoff; binary labels are unavailable")
        production = joblib.load(baseline_model)
        bundle = _binary_bundle(phase, production.encoder, production.classifier,
                                {"source_baseline_path": str(baseline_model), "recipe": dict(recipe),
                                 "reused_incumbent": True})
    else:
        # Full fit vocabulary is allowed to see every label available before the
        # exclusive fresh boundary; development selection vocabulary remains train-only.
        encoder = DraftFeatureEncoder.fit(phase_data["heroes"][indices], recipe["design"], True)
        matrix = disk_design(encoder, phase_data["heroes"][indices], scratch / phase / "full")
        try:
            classifier = fit_logistic(matrix, phase_data[target][indices], recipe["C"], max_iter,
                                      f"{phase}/{recipe['design']}/full")
        finally:
            del matrix
            gc.collect()
        bundle = _binary_bundle(phase, encoder, classifier,
                                {"recipe": dict(recipe), "reused_incumbent": False})
    return bundle, {"fit_rows": int(len(indices)), "fit_end_exclusive": int(fresh_after_ts),
                    "reused_incumbent": bool(recipe["incumbent"]),
                    "source_baseline_path": str(baseline_model) if recipe["incumbent"] else None}


def tune_phase(rows: Mapping[str, np.ndarray], phase: str, baseline_dir: Path, output_dir: Path, scratch: Path,
               fresh_after_ts: int, embargo_seconds: int, max_iter: int) -> dict[str, Any]:
    data = phase_rows_before_direction(rows, phase)
    target = "early_nw" if phase == "early_nw" else "wins"
    split = development_splits(data, phase, embargo_seconds)
    heroes, y = data["heroes"], data[target].astype(np.int8)
    incumbent, baseline_model_path = incumbent_recipe(baseline_dir, phase)
    position_recipes = [dict(incumbent)] + [
        {"design": POSITION_KIND, "C": c, "incumbent": False} for c in DEFAULT_GRID if c != incumbent["C"]
    ]
    recipes_by_design = [position_recipes, [role_pair_recipe(baseline_dir, phase)]]
    trials: list[dict[str, Any]] = []
    for recipes in recipes_by_design:
        fitted = _fit_design(heroes, y, split, recipes, scratch, max_iter, phase)
        trials.extend(fitted)
    selected_trial = choose_recipe(trials)
    selected = selected_trial["recipe"]
    # Both public-test recipes deliberately share the train-only vocabulary and
    # train+validation labels whose availability was embargoed at test start.
    selected_encoder, selected_model = _fit_recipe(heroes, y, split, selected, scratch, max_iter, phase)
    if selected["design"] == incumbent["design"] and selected["C"] == incumbent["C"]:
        baseline_encoder, baseline_model = selected_encoder, selected_model
    else:
        baseline_encoder, baseline_model = _fit_recipe(heroes, y, split, incumbent, scratch, max_iter, phase)
    test = split["test"]
    selected_bundle = _binary_bundle(phase, selected_encoder, selected_model, {"recipe": selected, "held_out_test": True})
    baseline_bundle = _binary_bundle(phase, baseline_encoder, baseline_model, {"recipe": incumbent, "held_out_test": True})
    candidate_probability = predict_binary(selected_bundle, heroes[test])
    baseline_probability = predict_binary(baseline_bundle, heroes[test])
    phase_output = output_dir / phase
    evaluation_path = phase_output / "binary_evaluation_model.joblib"
    _atomic_joblib(selected_bundle, evaluation_path)
    reloaded = joblib.load(evaluation_path)
    reload_delta = float(np.max(np.abs(candidate_probability - predict_binary(reloaded, heroes[test]))))
    if reload_delta > 1e-12:
        raise ValueError(f"{phase}: reloaded binary artifact changed predictions")
    prediction_path = phase_output / "public_test_predictions.npz"
    temporary = prediction_path.with_name(prediction_path.name + ".tmp")
    with temporary.open("wb") as stream:
        np.savez_compressed(stream, mid=data["mid"][test], ts=data["ts"][test], y=y[test],
                            baseline=baseline_probability, candidate=candidate_probability)
    os.replace(temporary, prediction_path)
    full_bundle, full_metadata = _full_artifact(rows, phase, selected, fresh_after_ts, embargo_seconds,
                                                scratch, max_iter, baseline_model_path)
    full_path = phase_output / "candidate_full.joblib"
    _atomic_joblib(full_bundle, full_path)
    full_probe = data["heroes"][:min(64, len(data["heroes"]))]
    full_before = _validated_binary_prediction(full_bundle, full_probe)
    full_reloaded = joblib.load(full_path)
    full_reload_delta = float(np.max(np.abs(full_before - _validated_binary_prediction(full_reloaded, full_probe))))
    if full_reload_delta > 1e-12:
        raise ValueError(f"{phase}: reloaded full binary artifact changed predictions")
    report = {
        "phase": phase, "binary_target": "direction_only" if phase == "early_nw" else "radiant_win",
        "selection_metric": "validation_log_loss", "public_test_exposed": True,
        "recipes": {"incumbent": incumbent, "selected": selected},
        "trials": trials,
        "split": {"raw_train_rows": int(split["raw_train_end"][0]), "raw_validation_rows": int(split["raw_test_start"][0] - split["raw_train_end"][0]),
                  "raw_test_rows": int(len(data["ts"]) - split["raw_test_start"][0]),
                  "validation_first_ts": int(data["ts"][split["raw_train_end"][0]]), "test_first_ts": int(data["ts"][split["raw_test_start"][0]]),
                  "eligible_direction_rows": {key: int(len(split[key])) for key in ("train", "validation", "test")},
                  "embargo_seconds": int(embargo_seconds)},
        "public_test": {"baseline": binary_metrics(y[test], baseline_probability), "candidate": binary_metrics(y[test], candidate_probability),
                        "paired_day_bootstrap": paired_day_bootstrap(data["ts"][test], y[test], baseline_probability, candidate_probability)},
        "artifacts": {"evaluation": {"file": evaluation_path.name, "sha256": _sha256(evaluation_path), "reload_max_delta": reload_delta},
                      "predictions": {"file": prediction_path.name, "sha256": _sha256(prediction_path)},
                      "full": {"file": full_path.name, "sha256": _sha256(full_path),
                               "reload_max_delta": full_reload_delta, **full_metadata}},
    }
    atomic_json(report, phase_output / "results.json")
    return report


def _baseline_identity(baseline_dir: Path, phases: list[str]) -> dict[str, dict[str, str | None]]:
    """Pin both historical recipes, including role-pair's evaluation artifact."""
    result: dict[str, dict[str, str | None]] = {}
    for phase in phases:
        for design, model_name in ((POSITION_KIND, "model.joblib"), (ROLE_PAIR_KIND, "evaluation_model.joblib")):
            directory = baseline_dir / phase / design
            for name in ("results.json", model_name):
                path = directory / name
                if not path.exists():
                    raise FileNotFoundError(f"missing baseline identity input: {path}")
                result[f"{phase}/{design}/{name}"] = _sha256(path)
    return result


def _verify_done_output(output_dir: Path, status: Mapping[str, object]) -> None:
    """A matching identity alone is not enough to reuse a completed run."""
    phases = status.get("phases")
    expected_models = status.get("identity", {}).get("models") if isinstance(status.get("identity"), Mapping) else None
    if (not isinstance(phases, Mapping) or not isinstance(expected_models, list) or not expected_models
            or set(phases) != set(expected_models)):
        raise ValueError("completed output has no phase summaries")
    for phase, summary in phases.items():
        if not isinstance(summary, Mapping):
            raise ValueError(f"completed output has invalid summary: {phase}")
        result_path = output_dir / str(phase) / "results.json"
        expected_result_sha = summary.get("results_sha256")
        if not isinstance(expected_result_sha, str) or not result_path.exists() or _sha256(result_path) != expected_result_sha:
            raise ValueError(f"completed output result hash mismatch: {phase}")
        report = json.loads(result_path.read_text())
        for artifact_name in ("evaluation", "predictions", "full"):
            artifact = report.get("artifacts", {}).get(artifact_name, {})
            filename, expected_sha = artifact.get("file"), artifact.get("sha256")
            path = result_path.parent / str(filename)
            if not isinstance(filename, str) or Path(filename).name != filename or not isinstance(expected_sha, str):
                raise ValueError(f"completed output has invalid {artifact_name} metadata: {phase}")
            if not path.exists() or _sha256(path) != expected_sha:
                raise ValueError(f"completed output artifact hash mismatch: {phase}/{artifact_name}")


def main(argv: list[str] | None = None) -> dict[str, Any]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--baseline-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--models", nargs="+", choices=PHASES, default=list(PHASES))
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--fresh-after-ts", type=int, required=True)
    parser.add_argument("--max-iter", type=int, default=1000)
    parser.add_argument("--embargo-seconds", type=int, default=3600)
    args = parser.parse_args(argv)
    if (args.threads < 1 or args.max_iter < 1 or args.embargo_seconds < 0
            or not args.models or len(set(args.models)) != len(args.models)):
        parser.error("threads/max-iter must be positive and embargo-seconds non-negative")
    corpus_path = args.corpus / "rows.npz" if args.corpus.is_dir() else args.corpus
    manifest_path = corpus_path.parent / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("purpose") == "unscored_frozen_forward_holdout":
            raise ValueError("refusing frozen forward holdout as trainer corpus")
    dependency_code = {name: _sha256(ROOT / "base" / name) for name in (
        "train_draft_phase_models.py", "draft_features.py", "draft_phase_model.py",
    )}
    identity = {"corpus_sha256": _sha256(corpus_path), "corpus_manifest_sha256": _optional_sha256(manifest_path),
                "baseline_dir": str(args.baseline_dir.resolve()), "baseline_sha256": _baseline_identity(args.baseline_dir, args.models),
                "models": args.models, "position_C_grid": list(DEFAULT_GRID), "fresh_after_ts": args.fresh_after_ts,
                "embargo_seconds": args.embargo_seconds, "max_iter": args.max_iter,
                "source_sha256": _sha256(Path(__file__)), "dependency_code_sha256": dependency_code}
    status_path = args.output_dir / "status.json"
    if status_path.exists():
        old = json.loads(status_path.read_text())
        if old.get("identity") != identity:
            raise FileExistsError(f"refusing incompatible output reuse: {args.output_dir}")
        if old.get("status") == "DONE":
            _verify_done_output(args.output_dir, old)
            return old
        raise FileExistsError(f"refusing to overwrite unfinished output: {args.output_dir}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    status: dict[str, Any] = {"schema_version": 1, "status": "RUNNING", "identity": identity, "phases": {}}
    atomic_json(status, status_path)
    try:
        rows = load_rows(args.corpus)
        with threadpool_limits(limits=args.threads):
            for phase in args.models:
                _log(f"TUNING {phase}")
                report = tune_phase(rows, phase, args.baseline_dir, args.output_dir, args.scratch,
                                    args.fresh_after_ts, args.embargo_seconds, args.max_iter)
                status["phases"][phase] = {"selected": report["recipes"]["selected"],
                                           "validation_log_loss": next(t["validation_log_loss"] for t in report["trials"] if t["recipe"] == report["recipes"]["selected"]),
                                           "result": str(args.output_dir / phase / "results.json"),
                                           "results_sha256": _sha256(args.output_dir / phase / "results.json")}
                atomic_json(status, status_path)
        status["status"] = "DONE"
        atomic_json(status, status_path)
        return status
    except Exception as exc:
        status["status"] = "FAIL"
        status["error"] = f"{type(exc).__name__}: {exc}"
        atomic_json(status, status_path)
        raise


if __name__ == "__main__":
    main()
