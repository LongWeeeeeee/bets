"""Compare selected kills-v3 models against PRODUCTION panel forecasts.

Offline research only: joins the earliest eligible journal forecast per map
with dataset labels (radiant orientation, side 0) and the primary (rank 0)
selected record's predictions, then scores panel vs v3_online (and
v3_frozen when --frozen-dataset is given) with paired day-cluster bootstrap.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import sys
import warnings
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kills_v3_train import (  # noqa: E402
    TARGETS,
    load_json,
    metric,
    predict_candidate,
    safe_prob,
    series_features,
)

SEED = 0
DEFAULT_SINCE = 1789171200

TARGET_INDEX = {"side30": 0, "total55": 2, "lead_5_15": 3, "lead_10_20": 4,
                "lead_15_25": 5, "lead_20_30": 6}


def target_key(target):
    if target == "side30":
        return "rad_30_25"
    if target == "total55":
        return "total_55_50"
    return "w_" + target[len("lead_"):]  # lead_a_b -> w_a_b


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sanitize(value):
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    if isinstance(value, (np.floating, np.integer)):
        return sanitize(float(value))
    if isinstance(value, dict):
        return {k: sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize(v) for v in value]
    return value


def write_json_atomic(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(sanitize(value), indent=2, sort_keys=True,
                              allow_nan=False) + "\n")
    os.replace(tmp, path)


def valid_p(p):
    return (isinstance(p, (int, float, np.floating, np.integer))
            and not isinstance(p, bool)
            and np.isfinite(float(p)) and 0.0 <= float(p) <= 1.0)


def load_dataset(npz_path):
    npz_path = Path(npz_path)
    meta_path = npz_path.with_name("metadata.json")
    z = np.load(npz_path, allow_pickle=False)
    data = {k: z[k] for k in z.files}
    meta = load_json(meta_path)
    return data, meta, meta_path


def day_bootstrap(y, p_new, p_ref, days, reps):
    """Paired day-cluster bootstrap of (new - ref). y/p are 1-D arrays."""
    from sklearn.metrics import roc_auc_score

    y = np.asarray(y, dtype=float)
    p_new = safe_prob(np.asarray(p_new, dtype=float))
    p_ref = safe_prob(np.asarray(p_ref, dtype=float))
    days = np.asarray(days)
    lld = (-(y * np.log(p_new) + (1 - y) * np.log1p(-p_new))
           - (-(y * np.log(p_ref) + (1 - y) * np.log1p(-p_ref))))

    def auc_delta(yy, a, b):
        if len(np.unique(yy)) != 2:
            return np.nan
        try:
            return float(roc_auc_score(yy, a) - roc_auc_score(yy, b))
        except ValueError:
            return np.nan

    uniq = np.unique(days)
    by_day = {d: np.flatnonzero(days == d) for d in uniq}
    rng = np.random.default_rng(SEED)
    dll = np.empty(reps)
    dau = np.empty(reps)
    for i in range(reps):
        pick = rng.choice(uniq, size=len(uniq), replace=True)
        idx = np.concatenate([by_day[d] for d in pick])
        dll[i] = float(np.mean(lld[idx]))
        dau[i] = auc_delta(y[idx], p_new[idx], p_ref[idx])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        auc_mean = float(np.nanmean(dau)) if np.any(np.isfinite(dau)) else float("nan")
        auc_ci = (np.nanquantile(dau, [0.025, 0.975]).tolist()
                  if np.any(np.isfinite(dau)) else [float("nan"), float("nan")])
    return {"n_maps": int(len(y)),
            "delta_ll_point": float(np.mean(lld)),
            "delta_ll_mean": float(np.mean(dll)),
            "delta_ll_ci95": [float(np.quantile(dll, 0.025)), float(np.quantile(dll, 0.975))],
            "delta_auc_point": sanitize(auc_delta(y, p_new, p_ref)),
            "delta_auc_mean": sanitize(auc_mean),
            "delta_auc_ci95": sanitize(auc_ci)}


def model_metrics(y, p):
    m = metric(np.asarray(y, dtype=float), np.asarray(p, dtype=float))
    return {"n_maps": int(m["n_rows"]), "ll": float(m["ll"]),
            "auc": m["auc"], "brier": float(m["brier"]),
            "ece_10": float(m["ece_10"])}


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Compare kills-v3 primary models vs production panel "
                    "forecasts from a journal, on the same test maps.")
    ap.add_argument("--journal", required=True)
    ap.add_argument("--out", required=True,
                    help="Harness out dir holding selection.json + test_started.json")
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--frozen-dataset", default=None)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--since", type=int, default=DEFAULT_SINCE)
    ap.add_argument("--reps", type=int, default=2000)
    args = ap.parse_args(argv)

    out = Path(args.out)
    sel_path = out / "selection.json"
    lock_path = out / "test_started.json"
    if not sel_path.exists() or not lock_path.exists():
        print(f"refusing: terminal test lock missing ({sel_path}, {lock_path})",
              file=sys.stderr)
        return 2

    selection = load_json(sel_path)
    primaries = {}
    for target in TARGETS:
        try:
            primaries[target] = selection["targets"][target][0]["id"]
        except (KeyError, IndexError, TypeError):
            print(f"refusing: no rank-0 record for target {target} in {sel_path}",
                  file=sys.stderr)
            return 2

    data, meta, _ = load_dataset(args.dataset)
    for k in ("X", "y", "mids", "starts", "ends", "series_ids", "split"):
        if k not in data:
            print(f"refusing: dataset missing key {k}", file=sys.stderr)
            return 2
    mids = data["mids"].astype(np.int64)
    starts = data["starts"].astype(np.int64)
    ends = data["ends"].astype(np.int64)
    split = np.asarray(data["split"])
    test_pos = np.flatnonzero(split == 2)
    pos_of = {str(int(m)): i for i, m in enumerate(mids) if split[i] == 2}
    end_of = {str(int(mids[i])): int(ends[i]) for i in test_pos}

    frozen = None
    if args.frozen_dataset:
        fz, _, _ = load_dataset(args.frozen_dataset)
        frozen = fz

    excl = collections.Counter()
    rows = []
    seen = {}  # (map_id, ts, key) -> token, for conflict detection
    with open(args.journal) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            excl["journal_lines_total"] += 1
            try:
                rec = json.loads(line)
                models = rec["models"]
                ts = rec["ts"]
                map_id = str(rec["map_id"])
                assert isinstance(models, list) and float(ts) == float(ts)
            except (ValueError, KeyError, TypeError, AssertionError):
                excl["journal_row_malformed"] += 1
                continue
            ts = float(ts)
            for entry in models:
                if not isinstance(entry, dict) or "key" not in entry or "p" not in entry:
                    continue
                key = entry["key"]
                try:
                    pf = float(entry["p"])
                except (TypeError, ValueError):
                    pf = float("nan")
                token = "nan" if np.isnan(pf) else pf
                ck = (map_id, ts, key)
                if ck in seen and seen[ck] != token and not (
                        isinstance(seen[ck], float) and isinstance(token, float)
                        and np.isnan(seen[ck]) and np.isnan(token)):
                    print(f"conflicting p for map={map_id} ts={ts} key={key}: "
                          f"{seen[ck]} vs {token}", file=sys.stderr)
                    return 3
                seen[ck] = token
            rows.append({"map_id": map_id, "ts": ts, "models": models})
    by_map = collections.defaultdict(list)
    for r in rows:
        by_map[r["map_id"]].append(r)

    # Per-map time filtering (map-level reasons counted once per row).
    eligible = {}  # str(mid) -> list of rows passing time filters
    for map_id, rlist in by_map.items():
        if map_id not in pos_of:
            try:
                mid_int = int(map_id)
            except ValueError:
                mid_int = None
            if mid_int is None or mid_int not in set(mids.tolist()):
                excl["row_map_unknown"] += len(rlist)
            else:
                excl["row_map_not_test"] += len(rlist)
            continue
        keep = []
        for r in rlist:
            if r["ts"] < args.since:
                excl["row_ts_before_since"] += 1
            elif r["ts"] >= end_of[map_id]:
                excl["row_ts_at_or_after_end"] += 1
            else:
                keep.append(r)
        eligible[map_id] = keep

    # Per (map, target): earliest row with key present and valid p.
    panel = {t: {} for t in TARGETS}  # target -> {str(mid): p}
    for target in TARGETS:
        key = target_key(target)
        for map_id, rlist in eligible.items():
            cands = []
            for r in rlist:
                found = [e for e in r["models"]
                         if isinstance(e, dict) and e.get("key") == key]
                if not found:
                    excl[f"cand_key_missing:{target}"] += 1
                    continue
                p = found[0].get("p")
                if not valid_p(p):
                    excl[f"cand_p_invalid:{target}"] += 1
                    continue
                cands.append((r["ts"], float(p)))
            if not cands:
                excl[f"map_no_panel_row:{target}"] += 1
                continue
            cands.sort(key=lambda c: c[0])
            panel[target][map_id] = cands[0][1]

    # Labels on radiant orientation (side 0), full y including test.
    y = np.asarray(data["y"])
    label_raw, label_bin, ok = {}, {}, {}
    for target in TARGETS:
        idx = TARGET_INDEX[target]
        raw = y[test_pos, 0, idx].astype(float)
        finite = np.isfinite(raw)
        if target in ("side30", "total55"):
            thr = 30 if target == "side30" else 55
            label_raw[target] = raw
            label_bin[target] = (raw >= thr).astype(np.int8)
            ok[target] = finite
            excl[f"label_nonfinite:{target}"] += int(np.sum(~finite))
        else:
            keep = finite & (raw != 0)
            label_raw[target] = raw
            label_bin[target] = (raw > 0).astype(np.int8)
            ok[target] = keep
            excl[f"label_nonfinite:{target}"] += int(np.sum(~finite))
            excl[f"label_tie:{target}"] += int(np.sum(finite & (raw == 0)))

    # v3 online predictions per target over its evaluated maps.
    need_s = False
    records = {}
    for target in TARGETS:
        rec = load_json(out / "candidates" / primaries[target] / "record.json")
        records[target] = rec
        if str(rec.get("block", "")).endswith("S"):
            need_s = True
    data_online = {"X": np.asarray(data["X"])}
    if need_s:
        if "team_ids" not in data:
            print("refusing: block needs S but dataset lacks team_ids", file=sys.stderr)
            return 2
        data_online["S"] = series_features(
            np.asarray(data["starts"]), np.asarray(data["ends"]),
            np.asarray(data["series_ids"]), np.asarray(data["team_ids"]), y)

    pos2j = {int(p): j for j, p in enumerate(test_pos)}
    targets_out = {}
    row_mid, row_start, row_tgt, row_y = [], [], [], []
    row_panel, row_on, row_fz, row_cohort = [], [], [], []
    row_on_sym, row_fz_sym = [], []
    for ti, target in enumerate(TARGETS):
        idx = TARGET_INDEX[target]
        mids_t, y_t, p_panel_t = [], [], []
        for j, pos in enumerate(test_pos):
            map_id = str(int(mids[pos]))
            if map_id not in panel[target] or not ok[target][j]:
                continue
            mids_t.append(pos)
            y_t.append(int(label_bin[target][j]))
            p_panel_t.append(panel[target][map_id])
        mids_t = np.array(mids_t, dtype=np.int64)
        y_t = np.array(y_t, dtype=np.int8)
        p_panel_t = np.array(p_panel_t, dtype=float)

        v3_on = (np.asarray(predict_candidate(
            data_online, meta, out, records[target], mids_t,
            np.zeros(len(mids_t), dtype=np.int8)), dtype=float)
            if len(mids_t) else np.array([]))

        # Pre-registered secondary column for windows (before any test metric):
        # symmetric P(radiant lead > 0) = (p_side0 + 1 - p_side1) / 2.
        is_window = target not in ("side30", "total55")
        v3_on_sym = np.full(len(mids_t), np.nan)
        if is_window and len(mids_t):
            p_dire = np.asarray(predict_candidate(
                data_online, meta, out, records[target], mids_t,
                np.ones(len(mids_t), dtype=np.int8)), dtype=float)
            v3_on_sym = (v3_on + 1 - p_dire) / 2
        v3_fz_sym = np.full(len(mids_t), np.nan)

        v3_fz = np.full(len(mids_t), np.nan)
        if frozen is not None and len(mids_t):
            fmids = np.asarray(frozen["mids"]).astype(np.int64)
            fpos_of = {int(m): i for i, m in enumerate(fmids)}
            fpos = []
            for pos in mids_t:
                mid = int(mids[pos])
                if mid not in fpos_of:
                    print(f"refusing: frozen dataset lacks mid {mid}", file=sys.stderr)
                    return 2
                fp = fpos_of[mid]
                for arr, name in ((data["starts"], "starts"), (data["ends"], "ends")):
                    if int(np.asarray(frozen[name])[fp]) != int(arr[pos]):
                        print(f"refusing: frozen {name} mismatch for mid {mid}",
                              file=sys.stderr)
                        return 2
                if not np.array_equal(np.asarray(frozen["y"])[fp], y[pos], equal_nan=True):
                    print(f"refusing: frozen y mismatch for mid {mid}", file=sys.stderr)
                    return 2
                fpos.append(fp)
            fpos = np.array(fpos, dtype=np.int64)
            fdata = {"X": np.asarray(frozen["X"])}
            if need_s:
                fdata["S"] = series_features(
                    np.asarray(frozen["starts"]), np.asarray(frozen["ends"]),
                    np.asarray(frozen["series_ids"]), np.asarray(frozen["team_ids"]),
                    np.asarray(frozen["y"]))
            v3_fz = np.asarray(predict_candidate(
                fdata, meta, out, records[target], fpos,
                np.zeros(len(fpos), dtype=np.int8)), dtype=float)
            if is_window:
                fz_dire = np.asarray(predict_candidate(
                    fdata, meta, out, records[target], fpos,
                    np.ones(len(fpos), dtype=np.int8)), dtype=float)
                v3_fz_sym = (v3_fz + 1 - fz_dire) / 2

        subsets = ["all"] if target not in ("side30", "total55") else ["all", "cohort"]
        sub_out = {}
        for sub in subsets:
            if sub == "cohort":
                lo, hi = (26, 29) if target == "side30" else (51, 54)
                raw_all = np.array([label_raw[target][pos2j[int(p)]]
                                    for p in mids_t], dtype=float)
                keep = (raw_all < lo) | (raw_all > hi)
                excl[f"cohort_excluded:{target}"] += int(np.sum(~keep))
            else:
                keep = np.ones(len(mids_t), dtype=bool)
            yy, pp, vv = y_t[keep], p_panel_t[keep], v3_on[keep]
            if len(yy) == 0:
                sub_out[sub] = {"models": {"panel": {"n_maps": 0},
                                           "v3_online": {"n_maps": 0}},
                                "bootstrap": {}}
                continue
            entry = {"models": {"panel": model_metrics(yy, pp),
                                "v3_online": model_metrics(yy, vv)},
                     "bootstrap": {"v3_online_minus_panel": day_bootstrap(
                         yy, vv, pp, starts[mids_t[keep]] // 86400, args.reps)}}
            if frozen is not None:
                ff = v3_fz[keep]
                entry["models"]["v3_frozen"] = model_metrics(yy, ff)
                entry["bootstrap"]["v3_frozen_minus_panel"] = day_bootstrap(
                    yy, ff, pp, starts[mids_t[keep]] // 86400, args.reps)
            if is_window:
                for name, col in (("v3_online_sym", v3_on_sym), ("v3_frozen_sym", v3_fz_sym)):
                    cc = col[keep]
                    if np.all(np.isfinite(cc)):
                        entry["models"][name] = model_metrics(yy, cc)
                        entry["bootstrap"][f"{name}_minus_panel"] = day_bootstrap(
                            yy, cc, pp, starts[mids_t[keep]] // 86400, args.reps)
            sub_out[sub] = entry
        targets_out[target] = {"key": target_key(target),
                               "record_id": primaries[target],
                               "subsets": sub_out}

        in_cohort = np.ones(len(mids_t), dtype=bool)
        if target in ("side30", "total55"):
            lo, hi = (26, 29) if target == "side30" else (51, 54)
            raw_all = np.array([label_raw[target][pos2j[int(p)]]
                                for p in mids_t], dtype=float)
            in_cohort = (raw_all < lo) | (raw_all > hi)
        for k, pos in enumerate(mids_t):
            row_mid.append(int(mids[pos]))
            row_start.append(int(starts[pos]))
            row_tgt.append(ti)
            row_y.append(int(y_t[k]))
            row_panel.append(float(p_panel_t[k]))
            row_on.append(float(v3_on[k]))
            row_fz.append(float(v3_fz[k]))
            row_on_sym.append(float(v3_on_sym[k]))
            row_fz_sym.append(float(v3_fz_sym[k]))
            row_cohort.append(bool(in_cohort[k]))

    result = {"journal": {"path": str(args.journal), "sha256": digest(Path(args.journal))},
              "dataset": {"path": str(args.dataset), "sha256": digest(Path(args.dataset))},
              "frozen_dataset": ({"path": str(args.frozen_dataset),
                                  "sha256": digest(Path(args.frozen_dataset))}
                                 if args.frozen_dataset else None),
              "selection": {"path": str(sel_path), "sha256": digest(sel_path)},
              "since": args.since, "reps": args.reps, "seed": SEED,
              "orientation": "radiant (side 0) only; windows also carry the pre-registered secondary *_sym = (p0+1-p1)/2",
              "bootstrap": "paired day-cluster (start day), new-minus-panel",
              "n_test_maps": int(len(test_pos)),
              "exclusions": dict(sorted(excl.items())),
              "targets": targets_out}
    outdir = Path(args.output_dir)
    write_json_atomic(outdir / "prod_compare.json", result)
    outdir.mkdir(parents=True, exist_ok=True)
    npz_tmp = outdir / "prod_compare_rows.tmp.npz"
    np.savez_compressed(npz_tmp, mid=np.array(row_mid, dtype=np.int64),
                        start=np.array(row_start, dtype=np.int64),
                        target=np.array(row_tgt, dtype=np.int64),
                        target_names=np.array(list(TARGETS)),
                        y=np.array(row_y, dtype=np.int8),
                        p_panel=np.array(row_panel, dtype=float),
                        p_v3_online=np.array(row_on, dtype=float),
                        p_v3_frozen=np.array(row_fz, dtype=float),
                        p_v3_online_sym=np.array(row_on_sym, dtype=float),
                        p_v3_frozen_sym=np.array(row_fz_sym, dtype=float),
                        in_cohort=np.array(row_cohort, dtype=bool))
    os.replace(npz_tmp, outdir / "prod_compare_rows.npz")
    print(f"wrote {outdir / 'prod_compare.json'} ({len(row_mid)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
