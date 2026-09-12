"""Freeze E281 candidates and a future cohort; refuse early or unbound reads.

This is an admission check, not a collector, scheduler, trainer or scorer.
It never opens future labels and does not modify production state.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time

import numpy as np

from kills_transfer_data import atomic_json, atomic_npz, sha256


def cohort_window(now, days=30):
    if days < 1:
        raise ValueError("The cohort must contain at least one full UTC day")
    start = (int(now) // 86400 + 1) * 86400
    return start, start + days * 86400


def freeze(candidate_dir, panel_dir, output_dir, now=None):
    now = int(time.time() if now is None else now)
    out, candidate, panel = map(Path, (output_dir, candidate_dir, panel_dir))
    if (out / "protocol.json").exists() or (out / "exposed_ids.npz").exists():
        raise ValueError("Frozen protocol already exists; do not reset its cohort")
    choices, files = {}, []
    for target in ("total", "side"):
        selection = candidate / f"{target}_selection.json"
        choice = json.loads(selection.read_text())["chosen"]
        choices[target] = choice
        files.extend([selection, candidate / f"{target}_{choice}.cbm",
                      candidate / f"{target}_{choice}_calibration.joblib"])
    files.extend(candidate / name for name in ("profiles.joblib", "public_models.joblib", "pro_inputs.npz"))
    panel_files = [panel / name for name in ("manifest.json", "feature_names.json", "panel.json",
                                            "total_55_50.cbm", "rad_30_25.cbm")]
    files.extend(panel_files)
    files.extend(Path(__file__).parent / name for name in (
        "kills_forward_protocol.py", "evaluate_kills_replay.py", "train_kills_transfer.py",
        "kills_public_transfer.py", "kills_transfer_features.py", "kills_transfer_data.py",
        "kills_relative_profiles.py", "draft_features.py"))
    hashes = {str(p.resolve()): sha256(p) for p in files}
    panel_hashes = {p.name: hashes[str(p.resolve())] for p in panel_files}
    bundle_hash = hashlib.sha256(json.dumps(panel_hashes, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    with np.load(candidate / "pro_inputs.npz", allow_pickle=False) as z:
        mids, series = np.unique(z["mids"]), np.unique(z["sids"])
    series = series[series > 0]
    start, end = cohort_window(now)
    atomic_npz(out / "exposed_ids.npz", mids=mids, sids=series)
    hashes[str((out / "exposed_ids.npz").resolve())] = sha256(out / "exposed_ids.npz")
    protocol = {"schema": "kills-forward-admission-v1", "frozen_at": now,
                "cohort_start": start, "cohort_end": end, "candidate_choices": choices,
                "files": hashes, "panel_artifact_sha256": bundle_hash,
                "panel_file_hashes": panel_hashes,
                "exposed_ids": str((out / "exposed_ids.npz").resolve()),
                "primary_targets": {"total": "sum(player kills) >= 55", "radiant": "sum(Radiant player kills) >= 30"},
                "panel_timing": "prediction_ts <= match_start; no substitution with in-play records",
                "selection": "earliest complete panel forecast per map, no confidence or outcome selection",
                "evaluation": "one fixed 30-day cohort; paired day-cluster log-loss CI; no model retuning",
                "limits": ["Current journal has no prematch matches in E281 and no per-record model fingerprint.",
                           "Historical logged probabilities cannot substitute for missing prospective prematch capture.",
                           "Provider snapshots and online input capture are not implemented by this admission check."]}
    atomic_json(out / "protocol.json", protocol)
    return protocol


def check(protocol_path, rows_path=None, journal_path=None, now=None):
    """Read only IDs/times from a completed future corpus, never its stats."""
    now = int(time.time() if now is None else now)
    protocol = json.loads(Path(protocol_path).read_text())
    for path, digest in protocol["files"].items():
        if sha256(Path(path)) != digest:
            raise ValueError(f"Frozen input changed: {path}")
    if now < protocol["cohort_end"]:
        return {"status": "WAITING_WINDOW_END", "cohort_start": protocol["cohort_start"],
                "cohort_end": protocol["cohort_end"], "labels_read": False}
    if rows_path is None or journal_path is None:
        return {"status": "WAITING_CORPUS_AND_PREMATCH_CAPTURE", "labels_read": False}
    with np.load(protocol["exposed_ids"], allow_pickle=False) as z:
        exposed_mids, exposed_sids = z["mids"], z["sids"]
    with np.load(rows_path, allow_pickle=False) as z:
        mids, ts, ends, sids = (z[key] for key in ("mids", "ts", "ends", "sids"))
    if len(np.unique(mids)) != len(mids) or np.any(ends <= ts):
        raise ValueError("Future map IDs must be unique and map intervals valid")
    inside = (ts >= protocol["cohort_start"]) & (ends < protocol["cohort_end"])
    crossing_sids = np.unique(sids[~inside & (sids > 0)])
    eligible = inside & ~np.isin(mids, exposed_mids) & ~np.isin(sids, exposed_sids)
    eligible &= ~np.isin(sids, crossing_sids) & (sids > 0)
    starts = {int(mid): int(start) for mid, start in zip(mids[eligible], ts[eligible])}
    journal = json.loads(Path(journal_path).read_text())
    captured = set()
    for row in journal["rows"]:
        try:
            from evaluate_kills_replay import whole_number
            mid, stamp = whole_number(row.get("map_id")), whole_number(row.get("ts"))
        except (TypeError, ValueError, OverflowError):
            continue
        if mid in starts and stamp <= starts[mid] and row.get("panel_artifact_sha256") == protocol["panel_artifact_sha256"]:
            models = {m.get("key"): m.get("p") for m in row.get("models", [])}
            probabilities = [models.get(k) for k in ("total_55_50", "rad_30_25")]
            if all(isinstance(p, (int, float)) and not isinstance(p, bool) and np.isfinite(p) and 0 <= p <= 1 for p in probabilities):
                captured.add(mid)
    return {"status": "READY_FOR_ONE_SHOT_EVALUATION" if captured else "NEEDS_PREMATCH_PANEL_CAPTURE",
            "eligible_maps": len(starts), "paired_maps": len(captured), "labels_read": False,
            "limits": "Readiness is not an evaluation or an accuracy result."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    f = sub.add_parser("freeze")
    f.add_argument("--candidate-dir", required=True)
    f.add_argument("--panel-dir", required=True)
    f.add_argument("--output-dir", required=True)
    c = sub.add_parser("check")
    c.add_argument("--protocol", required=True)
    c.add_argument("--rows")
    c.add_argument("--journal")
    args = parser.parse_args()
    if args.command == "freeze":
        result = freeze(args.candidate_dir, args.panel_dir, args.output_dir)
        print(json.dumps({k: result[k] for k in ("frozen_at", "cohort_start", "cohort_end")}))
    else:
        print(json.dumps(check(args.protocol, args.rows, args.journal)))
