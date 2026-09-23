"""Serving-lag sensitivity for registered kills v3 models.

The same registered models (selection.json) predict the same rows twice:
features built with history visible at end < start (training convention) and
features rebuilt with a visibility delay (end < start - delay; a finished map
reaches the serving feed late). The series-context block S is rebuilt with the
same delay. Output: per-target metrics and a paired day-cluster bootstrap of
(delayed - base).

--split v3 (default) uses validation third V3 and never loads terminal labels.
--split test is a pre-registered secondary: allowed only after the harness test
stage wrote test_started.json and test.json. It never feeds back into selection.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kills_v3_train import (  # noqa: E402
    TARGETS,
    digest,
    finite_json,
    load_json,
    metric,
    predict_candidate,
    series_features,
    target_rows,
    validation_thirds,
    verify_selection_hashes,
    write_json,
)
from kills_v3_prod_compare import day_bootstrap, load_dataset  # noqa: E402

SAME_ARRAYS = ("mids", "starts", "ends", "series_ids", "split", "team_ids")


def check_pair(base, base_meta, delayed, delayed_meta, delay):
    got = int(delayed_meta.get("parameters", {}).get("visibility_delay", 0))
    if got != delay:
        raise ValueError(f"delayed dataset visibility_delay={got}, expected {delay}")
    if int(base_meta.get("parameters", {}).get("visibility_delay", 0)) != 0:
        raise ValueError("base dataset must have visibility_delay=0")
    if base_meta["feature_names"] != delayed_meta["feature_names"]:
        raise ValueError("feature schema differs")
    for key in SAME_ARRAYS + ("y",):
        if key not in base or key not in delayed:
            raise ValueError(f"dataset lacks {key}")
        if not np.array_equal(np.asarray(base[key]), np.asarray(delayed[key]), equal_nan=key == "y"):
            raise ValueError(f"{key} differs between base and delayed datasets")


def evaluate(out, base, base_meta, delayed, delay, split_name, reps):
    split = np.asarray(base["split"])
    y = np.asarray(base["y"], dtype=np.float32).copy()
    if split_name == "v3":
        y[split == 2] = np.nan  # terminal labels are never used on the validation path
        maps = validation_thirds(np.asarray(base["starts"]), split)[2]
    else:
        maps = np.flatnonzero(split == 2)
    starts, ends = np.asarray(base["starts"]), np.asarray(base["ends"])
    sids, teams = np.asarray(base["series_ids"]), np.asarray(base["team_ids"])
    data_base = {"X": np.asarray(base["X"]), "y": y,
                 "S": series_features(starts, ends, sids, teams, y)}
    data_del = {"X": np.asarray(delayed["X"]), "y": y,
                "S": series_features(starts, ends + int(delay), sids, teams, y)}
    xb, xd = data_base["X"][maps], data_del["X"][maps]
    differs = (xb != xd) & ~(np.isnan(xb) & np.isnan(xd))
    changed_x = float(np.mean(np.any(differs, axis=(1, 2))))
    s_b, s_d = data_base["S"][maps, :, 1], data_del["S"][maps, :, 1]
    s_prev_visible = {"base": float(np.mean(np.isfinite(s_b))), "delayed": float(np.mean(np.isfinite(s_d)))}
    selection = load_json(out / "selection.json")
    verify_selection_hashes(out, selection)
    report = {"split": split_name, "delay_seconds": int(delay), "n_maps": int(len(maps)),
              "maps_with_any_X_change_share": changed_x, "S_previous_map_visible_share": s_prev_visible,
              "targets": {}}
    for target in TARGETS:
        rows = []
        for rank, choice in enumerate(selection["targets"][target]):
            rec = load_json(out / "candidates" / choice["id"] / "record.json")
            mm, sides, yy = target_rows(data_base, target, maps)
            p_b = np.asarray(predict_candidate(data_base, base_meta, out, rec, mm, sides), float)
            p_d = np.asarray(predict_candidate(data_del, base_meta, out, rec, mm, sides), float)
            m_b, m_d = metric(yy, p_b), metric(yy, p_d)
            entry = {"rank": rank, "id": choice["id"], "block": rec.get("block"), "kind": rec.get("kind"),
                     "base": m_b, "delayed": m_d,
                     "bootstrap_delayed_minus_base": day_bootstrap(yy, p_d, p_b, starts[mm] // 86400, reps)}
            if split_name == "v3" and "v3" in rec:
                entry["base_reproduces_record_v3_ll"] = bool(abs(m_b["ll"] - rec["v3"]["ll"]) < 1e-9)
            rows.append(entry)
        report["targets"][target] = rows
    return report


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, required=True, help="harness output dir with selection.json")
    ap.add_argument("--dataset", type=Path, required=True, help="base dataset.npz (visibility_delay 0)")
    ap.add_argument("--delayed-dataset", type=Path, required=True)
    ap.add_argument("--delay", type=int, required=True, help="seconds; must equal the delayed build")
    ap.add_argument("--split", choices=("v3", "test"), default="v3")
    ap.add_argument("--reps", type=int, default=2000)
    ap.add_argument("--output", type=Path, required=True, help="JSON report path")
    args = ap.parse_args(argv)
    out = args.out.resolve()
    if not (out / "selection.json").exists():
        print("refusing: selection.json missing", file=sys.stderr)
        return 2
    if args.split == "test" and not ((out / "test_started.json").exists() and (out / "test.json").exists()):
        print("refusing: --split test needs test_started.json and test.json from the harness", file=sys.stderr)
        return 2
    base, base_meta, _ = load_dataset(args.dataset)
    delayed, delayed_meta, _ = load_dataset(args.delayed_dataset)
    check_pair(base, base_meta, delayed, delayed_meta, args.delay)
    report = evaluate(out, base, base_meta, delayed, args.delay, args.split, args.reps)
    report["inputs"] = {"dataset": str(args.dataset), "dataset_sha256": digest(args.dataset),
                        "delayed_dataset": str(args.delayed_dataset),
                        "delayed_dataset_sha256": digest(args.delayed_dataset)}
    write_json(args.output, finite_json(report))
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
