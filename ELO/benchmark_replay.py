"""Rebuild rating features from raw maps, then evaluate fixed forward windows.

Uses a fresh model per replay, identical maps/tier priors/calendar in controls,
and one model at a time to limit RAM. Never writes a serving snapshot or weights.
"""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import resource
import sys
import time
from dataclasses import asdict
from pathlib import Path

import numpy as np
from scipy.special import expit
from sklearn.linear_model import LogisticRegression

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ELO.benchmark_probabilities import DAY, loss_interval, metrics, timestamp
from ELO.config import HybridEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier
from ELO.models import HybridPlayerRosterEloModel
from ELO.replay import prepare_prediction, replay_events, result_record
from ELO.tiering import attach_league_tiers_asof


def source_signature(path):
    rows = [(p.name, p.stat().st_size, p.stat().st_mtime_ns) for p in sorted(path.glob("*.json"))]
    return hashlib.sha256(json.dumps(rows).encode()).hexdigest()


def atomic_json(path, value):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2) + "\n")
    tmp.replace(path)


def start_events(matches):
    """Explicit leaky control: whole start batch predicted, then its outcomes."""
    batch = []
    for match in matches:
        if batch and match.timestamp != batch[0].timestamp:
            for item in batch:
                yield "result", item.timestamp, item
            batch = []
        yield "start", match.timestamp, match
        batch.append(match)
    for item in batch:
        yield "result", item.timestamp, item


def rating_features(matches, mode, output, fingerprint):
    path = output / (mode + ".npz")
    if path.exists():
        with np.load(path, allow_pickle=False) as saved:
            if str(saved["fingerprint"].item()) != fingerprint:
                raise ValueError("existing replay cache fingerprint mismatch")
            return saved["features"]
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    features = np.zeros((len(matches), 3))  # hybrid, K24 Elo, previous map winner
    ratings, series_history = {}, {}
    index, updates, latest_result = 0, 0, -1
    began = time.monotonic()
    events = replay_events(matches) if mode == "finished" else start_events(matches)
    for event, now, match in events:
        def mean_rating(players):
            return sum(ratings.get(p, 1500.0) for p in players) / 5
        elo_gap = (mean_rating(match.radiant_player_ids) - mean_rating(match.dire_player_ids)) / 400
        if event == "result":
            model.process_match(result_record(match, now))
            delta = 24 * (float(match.radiant_win) - float(expit(math.log(10) * elo_gap)))
            for p in match.radiant_player_ids:
                ratings[p] = ratings.get(p, 1500.0) + delta
            for p in match.dire_player_ids:
                ratings[p] = ratings.get(p, 1500.0) - delta
            if match.series_id and match.radiant_team_id and match.dire_team_id:
                a, b = match.radiant_team_id, match.dire_team_id
                key = match.series_id, min(a, b), max(a, b)
                series_history[key] = (now, a if match.radiant_win else b)
            latest_result = now
            updates += 1
            continue
        if latest_result >= now:
            raise ValueError("result not strictly before prediction")
        prepare_prediction(model, now)
        def strength(rad):
            return model.preview_team_strength(
                team_id=None, team_name=match.radiant_team_name if rad else match.dire_team_name,
                player_ids=match.radiant_player_ids if rad else match.dire_player_ids,
                player_positions=match.radiant_player_positions if rad else match.dire_player_positions,
                tier=LeagueTier.TIER3, timestamp=now)["team_strength"]
        features[index, :2] = (strength(True) - strength(False)) / 400, elo_gap
        a, b = match.radiant_team_id, match.dire_team_id
        if match.series_id and a and b:
            previous = series_history.get((match.series_id, min(a, b), max(a, b)))
            if previous and previous[0] < now and now - previous[0] <= 3 * DAY:
                features[index, 2] = 1 if previous[1] == a else -1
        index += 1
        if index % 25000 == 0:
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 ** 2 if sys.platform == "darwin" else 1024)
            print(f"{mode}: {index}/{len(matches)} starts; {updates} results; {time.monotonic()-began:.0f}s; peakRSS={rss:.0f}MB", flush=True)
    if index != len(matches) or not np.isfinite(features).all():
        raise ValueError("incomplete or nonfinite replay")
    tmp = path.with_suffix(".tmp.npz")
    np.savez_compressed(tmp, features=features, fingerprint=fingerprint)
    tmp.replace(path)
    del model
    gc.collect()
    return features


def evaluate(matches, start, finished):
    ts = np.array([m.timestamp for m in matches])
    # Calibration labels must also have become available before the cutoff.
    ends = np.array([m.result_timestamp for m in matches])
    y = np.array([m.radiant_win for m in matches], dtype=int)
    groups = np.array([m.series_id if m.series_id and m.series_id > 0 else -m.match_id for m in matches])
    bounds = [timestamp(d) for d in ["2026-01-01", "2026-03-01", "2026-06-01", "2026-08-12"]] + [int(ts.max()) + 1]
    if bounds[-1] <= bounds[-2]:
        raise ValueError("raw corpus has no fresh post-August-12 evaluation")
    X = {"hybrid_start_calibrated": start[:, [0]], "hybrid_finished_calibrated": finished[:, [0]],
         "elo_finished_calibrated": finished[:, [1]],
         "hybrid_finished_context": finished[:, [0, 2]],
         "elo_hybrid_finished_context": finished[:, [0, 1, 2]]}
    windows, all_predictions, indices = [], {}, []
    for lo, hi in zip(bounds, bounds[1:]):
        test = (ts >= lo) & (ts < hi)
        train = (ts >= lo - 120 * DAY) & (ends < lo) & ~np.isin(groups, groups[test])
        if test.sum() < 30 or train.sum() < 100:
            raise ValueError("insufficient train or test rows")
        ps = {"hybrid_start_raw": expit(math.log(10) * start[test, 0]),
              "hybrid_finished_raw": expit(math.log(10) * finished[test, 0]),
              "elo_finished_raw": expit(math.log(10) * finished[test, 1])}
        fits = {}
        for name, values in X.items():
            mu, sd = values[train].mean(0), np.maximum(values[train].std(0), 1e-9)
            model = LogisticRegression(C=1.0, max_iter=1000)
            with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
                model.fit((values[train] - mu) / sd, y[train])
                ps[name] = model.predict_proba((values[test] - mu) / sd)[:, 1]
                fits[name] = {"coef": (model.coef_[0]/sd).tolist(), "intercept": float(model.intercept_[0]-model.coef_[0]@(mu/sd))}
        window = {"from": lo, "to_exclusive": hi, "train_n": int(train.sum()), "test_n": int(test.sum()),
                  "models": {}, "fits": fits}
        for name, p in ps.items():
            window["models"][name] = metrics(y[test], p)
            window["models"][name]["loss_delta_ci_vs_finished_raw"] = loss_interval(y[test], p, ps["hybrid_finished_raw"], groups[test])
            all_predictions.setdefault(name, []).append(p)
        indices.append(np.flatnonzero(test))
        windows.append(window)
        print("WINDOW", json.dumps(window), flush=True)
    idx = np.concatenate(indices)
    preds = {k: np.concatenate(v) for k, v in all_predictions.items()}
    aggregate = {k: metrics(y[idx], v) for k, v in preds.items()}
    return {"windows": windows, "aggregate": aggregate,
            "limitations": ["Steam announcement times proxy patch activation", "curated tier prior is current, not a historical expert snapshot",
                            "archive end=start+duration; API publication latency and draft lock time unknown",
                            "fresh window is post prior rating cache, not a guaranteed never-examined corpus",
                            "no odds, dispatch or trained full-ML replacement evaluated"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=ROOT / "pro_heroes_data/json_parts_split_from_object")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    source = source_signature(args.data_dir)
    print("LOAD raw maps", flush=True)
    loaded, summary = load_matches(args.data_dir, progress=lambda p, n: print("LOAD", p.name, n, flush=True))
    print("LOADED", len(loaded), flush=True)
    unique = {}
    for match in loaded:
        unique.setdefault(match.match_id, match)
    del loaded
    matches = sorted((m for m in unique.values() if m.result_timestamp is not None), key=lambda m: (m.timestamp, m.match_id))
    summary["unique_matches"] = len(unique)
    summary["skipped_unknown_result_time"] = len(unique) - len(matches)
    del unique
    summary["tier_counts"] = attach_league_tiers_asof(matches)
    summary["replay_matches"] = len(matches)
    if source_signature(args.data_dir) != source:
        raise ValueError("source changed while loading; freeze corpus and rerun")
    import ELO.models as models
    metadata = {"source_signature": source, "config": asdict(HybridEloConfig()),
                "calendar": [(p.label, p.release_ts) for p in models._PATCH_RELEASES],
                "code_sha256": {str(p): hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in
                                [Path("ELO/benchmark_replay.py"), Path("ELO/models.py"), Path("ELO/domain.py"), Path("ELO/replay.py"), Path("ELO/tiering.py"), Path("ELO/data_loader.py")]},
                "dataset": summary}
    fingerprint = hashlib.sha256(json.dumps(metadata, sort_keys=True).encode()).hexdigest()
    atomic_json(args.output_dir / "inputs.json", metadata)
    print("STREAM READY", json.dumps(summary), flush=True)
    finished = rating_features(matches, "finished", args.output_dir, fingerprint)
    start = rating_features(matches, "start_control", args.output_dir, fingerprint)
    result = evaluate(matches, start, finished)
    result["input_fingerprint"] = fingerprint
    atomic_json(args.output_dir / "report.json", result)
    print("DONE", args.output_dir / "report.json", flush=True)


if __name__ == "__main__":
    main()
