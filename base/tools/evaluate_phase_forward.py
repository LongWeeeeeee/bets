#!/usr/bin/env python3
"""One-shot evaluation of frozen phase candidates against audited production files."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import joblib
import numpy as np

from base.build_draft_phase_corpus import _atomic_json, _atomic_npz
from base.tools.export_draft_phase_serving import FILES, digest
from base.train_draft_phase_models import load_rows, phase_rows, probability_metrics


def predict(encoder, classifier, heroes):
    if not np.array_equal(classifier.classes_, [0, 1]):
        raise ValueError("expected binary classes [Dire=0, Radiant=1]")
    out = np.empty(len(heroes), dtype=float)
    for start in range(0, len(heroes), 25000):
        out[start:start + 25000] = classifier.predict_proba(
            encoder.transform(heroes[start:start + 25000]))[:, 1]
    if not np.isfinite(out).all() or np.any((out < 0) | (out > 1)):
        raise ValueError("invalid probabilities")
    return out


def paired_intervals(y, baseline, candidate, ts, repeats=5000):
    """Resample UTC days, preserving every paired map and unequal daily counts."""
    y, baseline, candidate, ts = map(np.asarray, (y, baseline, candidate, ts))
    if not (len(y) == len(baseline) == len(candidate) == len(ts)) or not len(y):
        raise ValueError("nonempty equally sized paired inputs required")
    if repeats < 1:
        raise ValueError("positive repeat count required")
    def loss(p):
        p = np.clip(p, 1e-15, 1 - 1e-15)
        return -(y * np.log(p) + (1-y) * np.log1p(-p))
    loss_delta = loss(candidate) - loss(baseline)
    accuracy_delta = ((candidate > .5) == y).astype(float) - ((baseline > .5) == y)
    days, indices = np.unique(ts // 86400, return_inverse=True)
    counts = np.bincount(indices)
    daily_loss = np.bincount(indices, weights=loss_delta)
    daily_accuracy = np.bincount(indices, weights=accuracy_delta)
    result = {"days": len(days), "rows": len(y),
              "loss_delta": float(loss_delta.mean()),
              "accuracy_delta": float(accuracy_delta.mean()),
              "daily": [{"day": int(day), "rows": int(counts[i]),
                         "loss_delta_sum": float(daily_loss[i]),
                         "accuracy_delta_sum": float(daily_accuracy[i])}
                        for i, day in enumerate(days)]}
    if len(days) < 2:
        result["interval_unavailable"] = "fewer than two UTC days"
        return result
    sampled = np.random.default_rng(278).integers(0, len(days), (repeats, len(days)))
    denom = counts[sampled].sum(axis=1)
    for name, values in [("loss", daily_loss), ("accuracy", daily_accuracy)]:
        boot = values[sampled].sum(axis=1) / denom
        result[name + "_delta_ci95"] = np.quantile(boot, [.025, .975]).tolist()
        # Four phase comparisons; still descriptive with so few calendar blocks.
        result[name + "_delta_ci98_75"] = np.quantile(boot, [.00625, .99375]).tolist()
    result["interpretation"] = "descriptive day-block intervals; short pro period, correlated series"
    return result


def evaluate(candidates, holdout, production_dir, audit_path, output):
    candidates, holdout, production_dir, output = map(Path, (candidates, holdout, production_dir, output))
    if (output / "forward_results.json").exists():
        raise FileExistsError("forward results already exist; do not retune on this holdout")
    consumed = output / "forward_started.json"
    if consumed.exists():
        raise FileExistsError("forward evaluation already started; inspect its receipt before any retry")
    output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((holdout / "manifest.json").read_text())
    if not manifest.get("complete"):
        raise ValueError("holdout manifest incomplete")
    audit = json.loads(Path(audit_path).read_text())
    if audit.get("status") != "PASS":
        raise ValueError("production audit must pass first")
    status = json.loads((candidates / "status.json").read_text())
    if status.get("status") != "DONE" or set(status.get("phases", {})) != set(FILES):
        raise ValueError("all four phase choices must be complete before forward evaluation")
    if (status["identity"]["corpus_sha256"] != manifest["public_rows_sha256"] or
            status["identity"]["fresh_after_ts"] != manifest["selection"]["fresh_after_ts"]):
        raise ValueError("candidate training identity differs from frozen holdout protocol")
    phase_results = {}
    # Verify all four frozen choices BEFORE reading holdout outcomes.
    for phase in FILES:
        report_path = candidates / phase / "results.json"
        report_sha256 = digest(report_path)
        if report_sha256 != status["phases"][phase].get("results_sha256"):
            raise ValueError(f"training report differs from frozen DONE status: {phase}")
        r = json.loads(report_path.read_text())
        full = candidates / phase / "candidate_full.joblib"
        if not full.is_file() or digest(full) != r["artifacts"]["full"]["sha256"]:
            raise ValueError(f"candidate incomplete or modified: {phase}")
        if r["artifacts"]["full"]["fit_end_exclusive"] != status["identity"]["fresh_after_ts"]:
            raise ValueError(f"candidate cutoff mismatch: {phase}")
        phase_results[phase] = {"training_report_sha256": report_sha256, "training_report": r}
        for name in FILES[phase]:
            if digest(production_dir / phase / name) != audit["models"][phase]["hashes"][name]:
                raise ValueError(f"production file differs from audited server: {phase}/{name}")
        candidate = joblib.load(full)
        if candidate["phase"] != phase:
            raise ValueError("candidate phase mismatch")
        encoder = joblib.load(production_dir / phase / FILES[phase][0])
        classifier = joblib.load(production_dir / phase / FILES[phase][1])
        # Empty arrays validate the classifier contract without inspecting pro maps.
        predict(candidate["encoder"], candidate["classifier"], np.empty((0, 10), dtype=int))
        predict(encoder, classifier, np.empty((0, 10), dtype=int))
        if (candidate["encoder"].n_columns != candidate["classifier"].n_features_in_ or
                encoder.n_columns != classifier.n_features_in_):
            raise ValueError(f"encoder/classifier width mismatch: {phase}")
    receipt = {"started_at": time.time(), "holdout_sha256": manifest["rows_sha256"],
               "status_sha256": digest(candidates / "status.json")}
    # Exclusive creation also prevents two evaluators from consuming the same run.
    with consumed.open("x") as stream:
        json.dump(receipt, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    if digest(holdout / "rows.npz") != manifest["rows_sha256"]:
        raise ValueError("holdout integrity check failed after preflight")
    rows = load_rows(holdout)
    cutoff = manifest["selection"]["fresh_after_ts"]
    if np.any(rows["ts"] <= cutoff):
        raise ValueError("holdout contains pre-cutoff maps")
    result = {"complete": False, "started_at": time.time(), "production_changed": False,
              "holdout_sha256": manifest["rows_sha256"], "holdout_rows": len(rows["mid"]),
              "fresh_after_ts": cutoff, "models": {}}
    for phase, (encoder_file, model_file) in FILES.items():
        for name in [encoder_file, model_file]:
            if digest(production_dir / phase / name) != audit["models"][phase]["hashes"][name]:
                raise ValueError(f"local production file differs from audited server: {phase}/{name}")
        candidate_path = candidates / phase / "candidate_full.joblib"
        candidate = joblib.load(candidate_path)
        if candidate["phase"] != phase:
            raise ValueError("candidate phase mismatch")
        subset, target = phase_rows(rows, phase)
        if phase == "early_nw":
            subset = {k: v[subset[target] != 2] for k, v in subset.items()}
        y, heroes = subset[target], subset["heroes"]
        encoder = joblib.load(production_dir / phase / encoder_file)
        classifier = joblib.load(production_dir / phase / model_file)
        baseline = predict(encoder, classifier, heroes)
        proposed = predict(candidate["encoder"], candidate["classifier"], heroes)
        if digest(candidate_path) != phase_results[phase]["training_report"]["artifacts"]["full"]["sha256"]:
            raise ValueError(f"candidate changed during scoring: {phase}")
        phase_output = {"candidate_sha256": digest(candidate_path),
                        "training_report_sha256": phase_results[phase]["training_report_sha256"],
                        "baseline": probability_metrics(y, np.column_stack((1-baseline, baseline))),
                        "candidate": probability_metrics(y, np.column_stack((1-proposed, proposed))),
                        "changed_side_rows": int(np.sum((baseline > .5) != (proposed > .5)))}
        if len(y):
            phase_output["paired"] = paired_intervals(y, baseline, proposed, subset["ts"])
        _atomic_npz(output / (phase + "_forward_predictions.npz"), mid=subset["mid"],
                    ts=subset["ts"], y=y, baseline=baseline, candidate=proposed)
        result["models"][phase] = phase_output
    result.update(complete=True, finished_at=time.time())
    _atomic_json(output / "forward_results.json", result)
    print(json.dumps({phase: {"rows": v["baseline"]["rows"],
        "baseline_accuracy": v["baseline"].get("accuracy"),
        "candidate_accuracy": v["candidate"].get("accuracy"),
        "paired": {k: value for k, value in v.get("paired", {}).items() if k != "daily"}}
        for phase, v in result["models"].items()}, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--holdout", type=Path, required=True)
    parser.add_argument("--production-dir", type=Path, required=True)
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    evaluate(args.candidates, args.holdout, args.production_dir, args.audit, args.output)


if __name__ == "__main__":
    main()
