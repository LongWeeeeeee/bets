"""Pair recorded panel forecasts with frozen E281 predictions, without fitting.

The production journal usually starts during a map. Such rows are diagnostic
only; an empty prematch cohort must remain empty, never become a live cohort.
"""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path

import numpy as np

from kills_transfer_data import atomic_json, sha256
from train_kills_transfer import clustered_loss_delta, probability_metrics

PANEL_KEYS = {"total": "total_55_50", "radiant": "rad_30_25"}


def whole_number(value):
    if isinstance(value, (bool, np.bool_)):
        raise ValueError("Boolean identifier or timestamp")
    number = int(value)
    if number <= 0 or float(value) != number:
        raise ValueError("Expected a positive integer")
    return number


def load_candidate(directory):
    """Verify map orientation and labels before pairing saved predictions."""
    root = Path(directory)
    with np.load(root / "pro_inputs.npz", allow_pickle=False) as z:
        at = np.flatnonzero(z["split"] == 2)
        mids, starts, ends = z["mids"][at], z["ts"][at], z["ends"][at]
        kills = z["stats"][at, :, 0].reshape(-1, 2, 5).sum(2)
    if len(np.unique(mids)) != len(mids) or np.any(ends <= starts):
        raise ValueError("Invalid candidate map identity or timing")
    if not np.isfinite(kills).all() or np.any(kills < 0) or np.any(kills != np.floor(kills)):
        raise ValueError("Kill labels must be finite nonnegative integers")
    labels = {"total": kills.sum(1) >= 55, "radiant": kills[:, 0] >= 30}
    result = {"mids": mids, "starts": starts, "ends": ends, "kills": kills}
    for target, filename in (("total", "total"), ("radiant", "side")):
        selection = json.loads((root / f"{filename}_selection.json").read_text())
        chosen = selection["chosen"]
        with np.load(root / f"{filename}_test_predictions.npz", allow_pickle=False) as z:
            want_mids = mids if target == "total" else np.tile(mids, 2)
            want_y = labels[target] if target == "total" else np.r_[kills[:, 0] >= 30, kills[:, 1] >= 30]
            if not np.array_equal(z["mids"], want_mids) or not np.array_equal(z["y"], want_y):
                raise ValueError(f"{target}: prediction row orientation or labels differ")
            p = z[chosen].copy()
        if p.shape != want_mids.shape or not np.isfinite(p).all() or np.any((p < 0) | (p > 1)):
            raise ValueError("Invalid candidate probabilities")
        result[target] = p[:len(mids)]
        result[target + "_selected"] = chosen
    return result


def select_forecasts(rows, candidate, prematch):
    """Select the first complete forecast per map before its timing boundary.

    The choice depends on ID, timestamp and record validity, never outcome,
    confidence, correctness, or the number of later journal repetitions.
    Conflicting duplicate timestamps fail rather than depend on file order.
    """
    index = {int(mid): i for i, mid in enumerate(candidate["mids"])}
    selected, timestamps, counts = {}, {}, Counter()
    for row in rows:
        counts["rows"] += 1
        try:
            mid, ts = whole_number(row.get("map_id")), whole_number(row.get("ts"))
        except (TypeError, ValueError, OverflowError):
            counts["invalid_id_or_time"] += 1
            continue
        if mid not in index:
            counts["outside_candidate_cohort"] += 1
            continue
        i = index[mid]
        if ts >= candidate["ends"][i] or (prematch and ts > candidate["starts"][i]):
            counts["outside_timing"] += 1
            continue
        models = [m for m in row.get("models", []) if m.get("key") in PANEL_KEYS.values()]
        if len(models) != 2 or len({m["key"] for m in models}) != 2:
            counts["incomplete_models"] += 1
            continue
        by_key = {m["key"]: m for m in models}
        try:
            p = tuple(float(by_key[key]["p"]) for key in PANEL_KEYS.values())
        except (TypeError, ValueError, KeyError):
            counts["invalid_probability"] += 1
            continue
        if not np.isfinite(p).all() or any(v < 0 or v > 1 for v in p):
            counts["invalid_probability"] += 1
            continue
        identity = (mid, ts)
        if identity in timestamps and timestamps[identity] != p:
            raise ValueError(f"Conflicting forecasts at identical map/timestamp: {identity}")
        timestamps[identity] = p
        item = {"mid": mid, "ts": ts, "index": i, "p": p,
                "fill": [by_key[key].get("fill") for key in PANEL_KEYS.values()],
                "missing": [by_key[key].get("missing", []) for key in PANEL_KEYS.values()]}
        if mid not in selected or ts < selected[mid]["ts"]:
            selected[mid] = item
    chosen = sorted(selected.values(), key=lambda x: (candidate["starts"][x["index"]], x["mid"]))
    return chosen, dict(counts)


def compare(rows, candidate, prematch):
    selected, counts = select_forecasts(rows, candidate, prematch)
    report = {"maps": len(selected), "selection_counts": counts,
              "status": "DESCRIPTIVE_CONSUMED_TEST" if selected else "NO_ELIGIBLE_FORECASTS"}
    if not selected:
        return report
    ix = np.asarray([x["index"] for x in selected])
    panel = np.asarray([x["p"] for x in selected])
    kills = candidate["kills"][ix]
    days = candidate["starts"][ix] // 86400
    delays = np.asarray([x["ts"] for x in selected]) - candidate["starts"][ix]
    report["delay_seconds_quantiles"] = np.quantile(delays, [0, .25, .5, .75, 1]).tolist()
    report["targets"] = {}
    for column, target in enumerate(PANEL_KEYS):
        k = kills.sum(1) if target == "total" else kills[:, 0]
        high, low = (55, 50) if target == "total" else (30, 25)
        y = (k >= high).astype(int)
        candidate_p = candidate[target][ix]
        scopes = {}
        for name, mask in (("full_threshold", np.ones(len(y), bool)),
                           ("legacy_excluded_middle", (k <= low) | (k >= high))):
            if not mask.any():
                scopes[name] = {"n": 0}
                continue
            if name == "legacy_excluded_middle":
                # Selecting on observed K changes the conditional probability
                # target. The candidate was never calibrated for that target.
                scopes[name] = {"status": "NOT_COMPARABLE_PROBABILITY_TARGETS",
                                "panel": probability_metrics(y[mask], panel[mask, column]),
                                "candidate_comparison": "omitted: unconditional probability on a censored population"}
            else:
                scopes[name] = {"panel": probability_metrics(y[mask], panel[mask, column]),
                                "candidate": probability_metrics(y[mask], candidate_p[mask]),
                                "candidate_minus_panel": clustered_loss_delta(
                                    y[mask], candidate_p[mask], panel[mask, column], days[mask])}
        report["targets"][target] = scopes
    report["selected_forecasts"] = selected
    return report


def audit(journal, candidate_dir, output_dir):
    out = Path(output_dir)
    if (out / "summary.json").exists():
        raise ValueError("Audit already exists; preserve the previous result")
    journal = Path(journal)
    files = [Path(candidate_dir) / name for name in (
        "pro_inputs.npz", "total_selection.json", "side_selection.json",
        "total_test_predictions.npz", "side_test_predictions.npz")]
    files.extend(Path(__file__).parent / name for name in (
        "evaluate_kills_replay.py", "train_kills_transfer.py", "kills_transfer_data.py"))
    fingerprints = {str(p): sha256(p) for p in [journal] + files}
    candidate = load_candidate(candidate_dir)
    payload = json.loads(journal.read_text())
    report = {"schema": "kills-panel-journal-audit-v1", "inputs": fingerprints,
              "journal_source": {k: v for k, v in payload.items() if k != "rows"},
              "selected_candidate": {t: candidate[t + "_selected"] for t in PANEL_KEYS},
              "strict_prematch": compare(payload["rows"], candidate, prematch=True),
              "first_before_end": compare(payload["rows"], candidate, prematch=False),
              "limits": ["E281 test was already consumed; no new model selection is allowed.",
                         "Panel forecasts may be made during the map; candidate forecasts are prematch.",
                         "Panel probabilities were trained excluding the middle; full-threshold scoring tests the requested event.",
                         "The legacy subset reports only panel metrics; candidate delta/CI are omitted because probability targets differ.",
                         "Radiant only: there is no matching recorded Dire panel model.",
                         "Historical journal rows do not bind a model hash or feature snapshot."]}
    if any(sha256(Path(path)) != digest for path, digest in fingerprints.items()):
        raise ValueError("Input changed during evaluation")
    atomic_json(out / "summary.json", report)
    print(json.dumps({"strict_prematch_maps": report["strict_prematch"]["maps"],
                      "first_before_end_maps": report["first_before_end"]["maps"],
                      "output": str(out / "summary.json")}))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--candidate-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    audit(args.journal, args.candidate_dir, args.output_dir)
