"""Offline, staged kills-v3 training. Terminal labels are accessed only in test()."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import shutil
import time

import numpy as np
from catboost import CatBoostClassifier, CatBoostRegressor, Pool
from scipy.special import expit, logit
from scipy.stats import nbinom, norm
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score


ROOT = Path(__file__).resolve().parents[2]
INPUT = ROOT / "data/kills_v3_20260923"
OUTPUT = ROOT / "runtime/artifacts/kills/kills_v3_20260923"
MODELS = ROOT / "ml-models/kills_v3_20260923"
TARGETS = ("side30", "total55", "lead_5_15", "lead_10_20", "lead_15_25", "lead_20_30")
WINDOWS = TARGETS[2:]
LEARNING_N = (11000, 25000, 50000, 100000, "all")
BLOCKS = ("G", "GH", "GT", "GTP", "GTPH", "GTP_no_extras_H", "GTPHS")
EXTRAS = ("obs_placed", "sen_placed", "stuns", "teamfight_participation", "camps_stacked")
SEED = 20260923


def epoch(day):
    return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")
    tmp.replace(path)


def load_json(path):
    return json.loads(path.read_text())


def choose_input(out, dataset=None):
    manifest = out / "input.json"
    if manifest.exists():
        info = load_json(manifest)
        path = ROOT / info["dataset"]
        if dataset is not None and Path(dataset).resolve() != path.resolve():
            raise RuntimeError("--dataset differs from the dataset pinned in input.json")
        if digest(path) != info["sha256"]:
            raise RuntimeError("dataset changed since the first stage")
        return path, info
    if dataset is not None:
        path = Path(dataset).resolve()
    else:
        path = INPUT / "od2/dataset.npz" if (INPUT / "od2/metadata.json").exists() else INPUT / "dataset.npz"
    if not path.exists():
        raise FileNotFoundError(path)
    info = {"dataset": str(path.relative_to(ROOT)), "sha256": digest(path),
            "metadata": str(path.with_name("metadata.json").relative_to(ROOT)),
            "metadata_sha256": digest(path.with_name("metadata.json"))}
    write_json(manifest, info)
    return path, info


def validation_thirds(starts, split):
    valid = np.flatnonzero(split == 1)
    days = np.unique(starts[valid] // 86400)
    if len(days) < 3:
        raise ValueError("validation needs at least three distinct start days")
    cuts = (days[len(days)//3], days[2*len(days)//3])
    return tuple(valid[(starts[valid] // 86400 >= lo) & (starts[valid] // 86400 < hi)]
                 for lo, hi in ((-np.inf, cuts[0]), (cuts[0], cuts[1]), (cuts[1], np.inf)))


def series_features(starts, ends, series_ids, team_ids, labels):
    """For each orientation, summarize only completed earlier maps of its own team."""
    n = len(starts)
    result = np.full((n, 2, 20), np.nan, np.float32)
    prior = defaultdict(list)
    for i in np.argsort(starts, kind="stable"):
        sid = int(series_ids[i])
        if sid <= 0:
            continue
        for side in (0, 1):
            team = int(team_ids[i, side])
            if team == 0:  # 0 = unknown; negative STRATZ pseudo-ids are stable team keys
                continue
            hist = [(j, old_side) for j, old_side in prior[sid]
                    if int(ends[j]) < int(starts[i]) and int(team_ids[j, old_side]) == team]
            result[i, side, 0] = len(prior[sid])//2 + 1
            if not hist:
                continue
            def values(j, s):
                own, opp, total = labels[j, s, :3]
                duration = labels[j, s, 7]
                return np.r_[total, total / (duration / 60) if duration > 0 else np.nan,
                             duration, own, opp, labels[j, s, 3:7]]
            arr = np.asarray([values(j, s) for j, s in hist], np.float32)
            result[i, side, 1:10] = arr[-1]
            valid = np.isfinite(arr)
            result[i, side, 10:19] = np.divide(np.where(valid, arr, 0).sum(axis=0), valid.sum(axis=0),
                                                 out=np.full(9, np.nan), where=valid.sum(axis=0) > 0)
            result[i, side, 19] = len(hist)
        prior[sid].extend(((int(i), 0), (int(i), 1)))
    return result


def target_rows(data, target, maps, *, continuous=False):
    y = data["y"]
    sides = np.zeros(len(maps), np.int8) if target == "total55" else np.tile([0, 1], len(maps))
    mm = maps if target == "total55" else np.repeat(maps, 2)
    index = {"side30": 0, "total55": 2, **{name: i+3 for i, name in enumerate(WINDOWS)}}[target]
    values = y[mm, sides, index]
    keep = np.isfinite(values)
    if target in WINDOWS and not continuous:
        keep &= values != 0
    mm, sides, values = mm[keep], sides[keep], values[keep]
    if not continuous:
        values = (values >= (55 if target == "total55" else 30)).astype(np.int8) if target in TARGETS[:2] else (values > 0).astype(np.int8)
    return mm, sides, values


def feature_indices(meta, block):
    names = meta["feature_names"]
    if block == "GTP_no_extras_H":
        return [i for i, name in enumerate(names) if not any(x in name for x in EXTRAS)]
    letters = set(block) & set("GTPH")
    return [i for i, name in enumerate(names) if any(name in meta["blocks"][key] for key in letters)]


def matrix(data, meta, block, maps, sides, label=None, weight=None):
    cols = feature_indices(meta, block)
    x = data["X"][maps, sides][:, cols].astype(object)
    cat = []
    for j, col in enumerate(cols):
        name = meta["feature_names"][col]
        if name.startswith("hero_own_") or name.startswith("hero_opp_"):
            if name.rsplit("_", 1)[-1].isdigit():
                x[:, j] = [str(int(v)) if np.isfinite(v) else "-1" for v in x[:, j]]
                cat.append(j)
    if block.endswith("S"):
        x = np.concatenate((x, data["S"][maps, sides].astype(object)), axis=1)
    return Pool(x, label=label, weight=weight, cat_features=cat)


def safe_prob(p):
    return np.clip(np.asarray(p, float), 1e-7, 1-1e-7)


def platt_fit(raw, y):
    if len(np.unique(y)) < 2:
        return {"constant": float(np.mean(y))}
    model = LogisticRegression(C=1e6, max_iter=1000, random_state=SEED)
    model.fit(np.asarray(raw).reshape(-1, 1), y)
    return {"coef": float(model.coef_[0, 0]), "intercept": float(model.intercept_[0])}


def platt_predict(cal, raw):
    return safe_prob(np.full(len(raw), cal["constant"]) if "constant" in cal else
                     expit(cal["coef"] * np.asarray(raw) + cal["intercept"]))


def count_prob(target, mean, dispersion):
    mean = np.asarray(mean, float)
    if target in TARGETS[:2]:
        mu = np.maximum(mean, 1e-5)
        alpha = max(float(dispersion), 1e-6)
        shape = 1/alpha
        threshold = 55 if target == "total55" else 30
        return safe_prob(nbinom.sf(threshold-1, shape, shape/(shape+mu)))
    return safe_prob(norm.cdf(mean / max(float(dispersion), 1e-6)))


def fit_dispersion(target, mean, truth):
    if target in TARGETS[:2]:
        mu = np.maximum(mean, 1e-5)
        # NB2 variance = mean + alpha * mean^2, fitted on V2 only.
        return float(np.clip(np.mean(((truth-mu)**2-mu)/(mu**2)), 1e-5, 10))
    return float(max(np.sqrt(np.mean((truth-mean)**2)), 1e-5))


def metric(y, p):
    p = safe_prob(p)
    bins = np.minimum((p*10).astype(int), 9)
    ece = sum(np.sum(bins == b)/len(y) * abs(np.mean(y[bins == b])-np.mean(p[bins == b]))
              for b in range(10) if np.any(bins == b))
    return {"ll": float(log_loss(y, p, labels=[0, 1])),
            "auc": float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else None,
            "brier": float(np.mean((y-p)**2)), "accuracy_at_0_5": float(np.mean((p >= .5) == y)),
            "ece_10": float(ece), "n_rows": int(len(y))}


def candidate_id(stage, target, name):
    return f"{stage}__{target}__{name}"


def fit_candidate(data, meta, out, target, spec, parts, max_iters):
    cid = spec["id"]
    base = out / "candidates" / cid
    if (base / "record.json").exists():
        return load_json(base / "record.json")
    block = spec["block"]
    train_maps, v1, v2, v3 = parts
    tr = target_rows(data, target, train_maps, continuous=spec["kind"] == "count")
    stop = target_rows(data, target, v1, continuous=spec["kind"] == "count")
    cal = target_rows(data, target, v2, continuous=spec["kind"] == "count")
    select = target_rows(data, target, v3, continuous=False)
    if min(len(x[2]) for x in (tr, stop, cal, select)) == 0:
        raise ValueError(f"empty train/validation target rows: {cid}")
    weights = None
    if spec.get("weighted"):
        age = (epoch("2026-07-20") - data["starts"][tr[0]])/86400
        weights = 0.5 ** (age/365)
    cat = matrix(data, meta, block, tr[0], tr[1], tr[2], weights)
    eval_pool = matrix(data, meta, block, stop[0], stop[1], stop[2])
    common = dict(iterations=max_iters, depth=spec["depth"], learning_rate=.05,
                  random_seed=SEED, thread_count=6, verbose=False, allow_writing_files=False)
    model = (CatBoostRegressor(loss_function="Poisson" if target in TARGETS[:2] else "RMSE", **common)
             if spec["kind"] == "count" else CatBoostClassifier(loss_function="Logloss", **common))
    began = time.monotonic()
    model.fit(cat, eval_set=eval_pool, early_stopping_rounds=min(200, max(10, max_iters//3)))
    fit_seconds = time.monotonic() - began
    raw_v2 = model.predict(matrix(data, meta, block, cal[0], cal[1]), prediction_type="RawFormulaVal")
    if spec["kind"] == "count":
        if target in TARGETS[:2]:
            raw_v2 = np.exp(np.clip(raw_v2, -20, 20))
        disp = fit_dispersion(target, raw_v2, cal[2])
        raw_prob = count_prob(target, raw_v2, disp)
        y_cal = (cal[2] >= (55 if target == "total55" else 30)).astype(int) if target in TARGETS[:2] else (cal[2] > 0).astype(int)
        if target in WINDOWS:
            usable = cal[2] != 0
            raw_prob, y_cal = raw_prob[usable], y_cal[usable]
        calibration = {"dispersion": disp, "platt": platt_fit(logit(raw_prob), y_cal), "platt_input": "logit"}
    else:
        calibration = {"platt": platt_fit(raw_v2, cal[2])}
    base.mkdir(parents=True, exist_ok=True)
    model.save_model(str(base / "model.cbm"))
    rec = {**spec, "fit_seconds": fit_seconds, "trees": int(model.tree_count_), "calibration": calibration,
           "input": load_json(out / "input.json")}
    pred = predict_candidate(data, meta, out, rec, select[0], select[1])
    rec["v3"] = metric(select[2], pred)
    write_json(base / "record.json", rec)
    return rec


def predict_candidate(data, meta, out, rec, maps, sides):
    path = out / "candidates" / rec["id"] / "model.cbm"
    model = CatBoostRegressor() if rec["kind"] in ("count", "blend") else CatBoostClassifier()
    model.load_model(str(path))
    raw = model.predict(matrix(data, meta, rec["block"], maps, sides), prediction_type="RawFormulaVal")
    if rec["kind"] in ("count", "blend"):
        if rec["target"] in TARGETS[:2]:
            raw = np.exp(np.clip(raw, -20, 20))
        raw = count_prob(rec["target"], raw, rec["calibration"]["dispersion"])
        if rec["calibration"].get("platt_input") == "logit":
            raw = logit(raw)
    result = platt_predict(rec["calibration"]["platt"], raw)
    if rec["kind"] == "blend":
        other = load_json(out / "candidates" / rec["binary_id"] / "record.json")
        result = (result + predict_candidate(data, meta, out, other, maps, sides))/2
    return result


def register_blend(data, meta, out, count_rec, binary_rec, v3, name="blend"):
    rec = {**count_rec, "id": candidate_id("count", count_rec["target"], name),
           "kind": "blend", "binary_id": binary_rec["id"]}
    base = out / "candidates" / rec["id"]
    base.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(out / "candidates" / count_rec["id"] / "model.cbm", base / "model.cbm")
    rows = target_rows(data, rec["target"], v3)
    rec["v3"] = metric(rows[2], predict_candidate(data, meta, out, rec, rows[0], rows[1]))
    write_json(base / "record.json", rec)
    return rec


def candidate_hashes(out, cid):
    base = out / "candidates" / cid
    rec = load_json(base / "record.json")
    hashes = {"record_sha256": digest(base / "record.json"), "model_sha256": digest(base / "model.cbm")}
    if rec["kind"] == "blend":
        other = out / "candidates" / rec["binary_id"]
        hashes.update(binary_record_sha256=digest(other / "record.json"), binary_model_sha256=digest(other / "model.cbm"))
    return hashes


def available_records(out, target):
    return [load_json(p) for p in sorted((out / "candidates").glob(f"*__{target}__*/record.json"))]


def base_parts(data, max_train):
    train = np.flatnonzero((data["split"] == 0) & (data["starts"] < epoch("2026-07-20")))
    train = train[np.argsort(data["starts"][train], kind="stable")]
    if max_train:
        train = train[-max_train:]
    return train, *validation_thirds(data["starts"], data["split"])


def run_stage(args, out, data, meta):
    if not args.smoke and (out / "selection.json").exists():
        raise RuntimeError("full-run selection is frozen; no further training or reselection")
    train, v1, v2, v3 = base_parts(data, args.max_train)
    summary = {"stage": args.stage, "input": load_json(out / "input.json"), "smoke": args.smoke,
               "validation_maps": [len(v1), len(v2), len(v3)], "targets": {}}
    if args.stage == "learning_curve":
        for target in TARGETS:
            records = []
            sizes = (min(500, len(train)), min(2000, len(train)), len(train)) if args.smoke else LEARNING_N
            for size in dict.fromkeys(sizes):
                maps = train if size == "all" else train[-min(size, len(train)):]
                name = f"n{size}"
                spec = {"id": candidate_id("learning_curve", target, name), "target": target,
                        "block": "GTPH", "kind": "binary", "depth": 6, "n_maps": len(maps), "weighted": False}
                records.append(fit_candidate(data, meta, out, target, spec, (maps, v1, v2, v3), args.max_iters))
            spec = {"id": candidate_id("learning_curve", target, "all_weighted"), "target": target,
                    "block": "GTPH", "kind": "binary", "depth": 6, "n_maps": len(train), "weighted": True}
            records.append(fit_candidate(data, meta, out, target, spec, (train, v1, v2, v3), args.max_iters))
            summary["targets"][target] = [{"id": x["id"], "ll": x["v3"]["ll"], "fit_seconds": x["fit_seconds"]} for x in records]
    elif args.stage == "ablation":
        for target in TARGETS:
            prior = [r for r in available_records(out, target) if r["id"].startswith("learning_curve")]
            if not prior: raise RuntimeError("run learning_curve first")
            best = min(prior, key=lambda r: r["v3"]["ll"])
            maps = train[-best["n_maps"]:]
            records = []
            for block in BLOCKS[:(len(BLOCKS) if "team_ids" in data else -1)]:
                if block == best["block"] and best["kind"] == "binary" and best["depth"] == 6:
                    records.append(best)  # identical spec and seed: reuse instead of refitting
                    continue
                spec = {"id": candidate_id("ablation", target, block), "target": target,
                        "block": block, "kind": "binary", "depth": 6, "n_maps": len(maps),
                        "weighted": best["weighted"]}
                records.append(fit_candidate(data, meta, out, target, spec, (maps, v1, v2, v3), args.max_iters))
            summary["targets"][target] = [{"id": x["id"], "ll": x["v3"]["ll"], "fit_seconds": x["fit_seconds"]} for x in records]
        summary["series_context"] = "SERVING-UNVERIFIED; skipped: dataset has no team_ids" if "team_ids" not in data else "SERVING-UNVERIFIED"
    elif args.stage == "count":
        for target in TARGETS:
            recs = available_records(out, target)
            prior = [r for r in recs if r["id"].startswith("ablation")]
            if not prior: raise RuntimeError("run ablation first")
            curve = [r for r in recs if r["id"].startswith("learning_curve")]
            prior.append(min(curve, key=lambda r: r["v3"]["ll"]))  # reused as the ablation GTPH cell
            best = min(prior, key=lambda r: r["v3"]["ll"])
            maps = train[-best["n_maps"]:]
            spec = {"id": candidate_id("count", target, best["block"]), "target": target,
                    "block": best["block"], "kind": "count", "depth": 6, "n_maps": len(maps),
                    "weighted": best["weighted"]}
            rec = fit_candidate(data, meta, out, target, spec, (maps, v1, v2, v3), args.max_iters)
            blend = register_blend(data, meta, out, rec, best, v3)
            summary["targets"][target] = [{"id": r["id"], "ll": r["v3"]["ll"], "fit_seconds": r["fit_seconds"]} for r in (rec, blend)]
    elif args.stage == "select":
        for target in TARGETS:
            records = available_records(out, target)
            if not any(r["id"].startswith("count") for r in records): raise RuntimeError("run count first")
            best = min(records, key=lambda r: r["v3"]["ll"])
            maps = train[-best["n_maps"]:]
            if best["kind"] == "blend":
                binary = load_json(out / "candidates" / best["binary_id"] / "record.json")
                parts = []
                for source in (best, binary):
                    spec = {k: source[k] for k in ("target", "block", "n_maps", "weighted")}
                    spec.update(id=candidate_id("select", target, f"{source['kind']}_depth4"),
                                kind="count" if source["kind"] == "blend" else "binary", depth=4)
                    parts.append(fit_candidate(data, meta, out, target, spec, (maps, v1, v2, v3), args.max_iters))
                records.append(register_blend(data, meta, out, parts[0], parts[1], v3, "blend_depth4"))
            else:
                spec = {k: best[k] for k in ("target", "block", "kind", "n_maps", "weighted")}
                spec.update(id=candidate_id("select", target, "depth4"), depth=4)
                records.append(fit_candidate(data, meta, out, target, spec, (maps, v1, v2, v3), args.max_iters))
            unique = {r["id"]: r for r in records}
            chosen = sorted(unique.values(), key=lambda r: (r["v3"]["ll"], r["id"]))[:3]
            summary["targets"][target] = [{"id": r["id"], "v3_ll": r["v3"]["ll"]} for r in chosen]
        for choices in summary["targets"].values():
            for choice in choices:
                choice.update(candidate_hashes(out, choice["id"]))
        selection = {"input": summary["input"], "rule": "minimum V3 calibrated log loss; at most two alternatives", "smoke": args.smoke,
                     "primary": "rank0 of every target is the pre-registered primary result; rank1/rank2 are secondary",
                     "co_primary_metrics": ["delta_ll", "delta_auc"],
                     "targets": summary["targets"],
                     "series_context": "SERVING-UNVERIFIED" if "team_ids" in data else "SERVING-UNVERIFIED; skipped without team_ids"}
        model_root = (out / "models") if args.smoke else MODELS
        for target, choices in selection["targets"].items():
            for rank, choice in enumerate(choices):
                rec = load_json(out / "candidates" / choice["id"] / "record.json")
                dest = model_root / target / f"rank{rank}"
                dest.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(out / "candidates" / choice["id"] / "model.cbm", dest / "model.cbm")
                write_json(dest / "record.json", rec)
                if rec["kind"] == "blend":
                    shutil.copyfile(out / "candidates" / rec["binary_id"] / "model.cbm", dest / "binary_model.cbm")
                    write_json(dest / "binary_record.json", load_json(out / "candidates" / rec["binary_id"] / "record.json"))
        write_json(out / "selection.json", selection)
    else:
        raise ValueError(args.stage)
    write_json(out / f"{args.stage}.json", summary)
    return summary


def paired_loss_invariant(y_a, p_a, y_b, p_b, mids_a, mids_b):
    """Match mids by outcome multiset; no side ordering assumptions."""
    a = defaultdict(list); b = defaultdict(list)
    for mid, y, p in zip(mids_a, y_a, p_a): a[int(mid)].append((int(y), float(p)))
    for mid, y, p in zip(mids_b, y_b, p_b): b[int(mid)].append((int(y), float(p)))
    agree = sorted(mid for mid in a.keys() & b.keys()
                   if sorted(y for y, _ in a[mid]) == sorted(y for y, _ in b[mid]))
    return agree, a, b


def paired_bootstrap(ours, theirs, mids, days, reps=2000, seed=0):
    common, a, b = paired_loss_invariant(ours["y"], ours["p"], theirs["y"], theirs["p"], ours["mids"], theirs["mids"])
    if not common:
        return {"n_maps": 0, "dropped_label_disagreement": len(set(map(int, ours["mids"])) & set(map(int, theirs["mids"]))) }
    day_by_mid = {int(m): int(d) for m, d in zip(mids, days)}
    groups = defaultdict(list)
    for mid in common: groups[day_by_mid[mid]].append(mid)
    keys = np.array(sorted(groups))
    def calc(sample):
        aa = [pair for mid in sample for pair in a[mid]]
        bb = [pair for mid in sample for pair in b[mid]]
        ya, pa = np.array(aa).T; yb, pb = np.array(bb).T
        la = np.mean([sum(-y*np.log(safe_prob(p))-(1-y)*np.log1p(-safe_prob(p)) for y,p in a[mid]) for mid in sample])
        lb = np.mean([sum(-y*np.log(safe_prob(p))-(1-y)*np.log1p(-safe_prob(p)) for y,p in b[mid]) for mid in sample])
        da = roc_auc_score(ya, pa) - roc_auc_score(yb, pb) if len(np.unique(ya)) == len(np.unique(yb)) == 2 else np.nan
        return la-lb, da
    point = calc(common)
    rows_a = [pair for mid in common for pair in a[mid]]
    rows_b = [pair for mid in common for pair in b[mid]]
    diagnostics = {"mean_y": float(np.mean([y for y, _ in rows_a])),
                   "mean_p_ours": float(np.mean([p for _, p in rows_a])),
                   "mean_p_theirs": float(np.mean([p for _, p in rows_b])),
                   "rows_per_map_ours": len(rows_a)/len(common), "rows_per_map_theirs": len(rows_b)/len(common)}
    rng = np.random.default_rng(seed)
    draws = np.empty((reps, 2))
    for i in range(reps):
        selected_days = rng.choice(keys, len(keys), replace=True)
        draws[i] = calc([mid for day in selected_days for mid in groups[day]])
    overlap = len(set(map(int, ours["mids"])) & set(map(int, theirs["mids"])))
    dropped = overlap-len(common)
    return {"n_maps": len(common), "overlap_maps": overlap, "dropped_label_disagreement": dropped,
            "dropped_share": dropped/overlap, "warning": "dropped_share>0.03" if dropped/overlap > .03 else None,
            **diagnostics,
            "delta_ll": float(point[0]), "delta_ll_ci95": np.quantile(draws[:, 0], [.025, .975]).tolist(),
            "delta_auc": float(point[1]), "delta_auc_ci95": np.nanquantile(draws[:, 1], [.025, .975]).tolist()}


def collapse_to_radiant(ours):
    """One radiant row per map, E-314 window convention: p=(p_r+1-p_d)/2, y=y_r."""
    by = defaultdict(dict)
    for mid, side, y, p, d in zip(ours["mids"], ours["sides"], ours["y"], ours["p"], ours["days"]):
        by[int(mid)][int(side)] = (int(y), float(p), int(d))
    mids, ys, ps, ds, inconsistent = [], [], [], [], 0
    for mid, rows in by.items():
        if 0 not in rows or 1 not in rows:
            continue
        if rows[0][0] + rows[1][0] != 1:
            inconsistent += 1
            continue
        mids.append(mid); ys.append(rows[0][0]); ps.append((rows[0][1]+1-rows[1][1])/2); ds.append(rows[0][2])
    return ({"mids": np.asarray(mids, np.int64), "y": np.asarray(ys, np.int8), "p": np.asarray(ps, float),
             "days": np.asarray(ds, np.int64), "sides": np.zeros(len(mids), np.int8)}, inconsistent)


def compare(ours, target, smoke, out=None):
    result = {}
    reps = 40 if smoke else 2000
    order = np.arange(len(ours["y"]))[::-1]
    if target in ("side30", "total55"):
        name = "side" if target == "side30" else "total"
        path = ROOT / f".orchestra/jobs/run-2536be97ce82c95f51ef5752/train/output/{name}_test_predictions.npz"
        if smoke:
            path = out / f"synthetic_E281_{target}.npz"
            np.savez_compressed(path, mids=ours["mids"][order], y=ours["y"][order],
                                days=ours["days"][order], absolute=ours["p"][order])
        with np.load(path, allow_pickle=False) as z:
            theirs = {"mids": z["mids"], "y": z["y"], "p": z["absolute"]}
        result["E281_absolute" if not smoke else "E281_synthetic_standin"] = paired_bootstrap(
            ours, theirs, ours["mids"], ours["days"], reps=reps)
    path = ROOT / "ml-models/kills_signature_20260922" / target / "predictions.npz"
    selected = "synthetic" if smoke else load_json(
        ROOT / "ml-models/kills_signature_20260922/results.json")["targets"][target]["chosen"]
    mine = ours
    if target in WINDOWS:
        mine, inconsistent = collapse_to_radiant(ours)
        result["E314_window_collapse"] = {"rows_in": int(len(ours["y"])), "maps_out": int(len(mine["y"])),
                                          "inconsistent_pairs": int(inconsistent)}
    if smoke:
        order = np.arange(len(mine["y"]))[::-1]
        path = out / f"synthetic_E314_{target}.npz"
        np.savez_compressed(path, mids=mine["mids"][order], sides=mine.get("sides", np.zeros(len(mine["y"]), np.int8))[order],
                            y=mine["y"][order], **{selected: mine["p"][order]})
    with np.load(path, allow_pickle=False) as z:
        theirs = {"mids": z["mids"], "y": z["y"], "p": z[selected]}
    result["E314_synthetic_standin" if smoke else f"E314_{selected}"] = paired_bootstrap(
        mine, theirs, mine["mids"], mine["days"], reps=reps)
    return result


def test_stage(out, data, meta, smoke):
    selection_path = out / "selection.json"
    if not selection_path.exists():
        raise RuntimeError("selection.json must exist before terminal test access")
    if not smoke and not (out / "test_started.json").exists():
        raise RuntimeError("terminal test start marker is missing")
    if not smoke and load_json(out / "test_started.json")["selection_sha256"] != digest(selection_path):
        raise RuntimeError("selection changed after terminal test start")
    if not smoke and (out / "test.json").exists():
        raise RuntimeError("terminal test was already evaluated once")
    selection = load_json(selection_path)
    if selection["smoke"] != smoke or selection["input"] != load_json(out / "input.json"):
        raise RuntimeError("selection mode or input changed")
    verify_selection_hashes(out, selection)
    maps = validation_thirds(data["starts"], data["split"])[2] if smoke else np.flatnonzero(data["split"] == 2)
    results = {"input": selection["input"], "pseudo_test_v3": smoke, "primary": selection.get("primary"), "targets": {}}
    pred_dir = out / "test_predictions"
    pred_dir.mkdir(parents=True, exist_ok=True)
    for target, choices in selection["targets"].items():
        rows = target_rows(data, target, maps)
        target_result = []
        for rank, choice in enumerate(choices):
            rec = load_json(out / "candidates" / choice["id"] / "record.json")
            p = predict_candidate(data, meta, out, rec, rows[0], rows[1])
            ours = {"mids": data["mids"][rows[0]], "sides": rows[1], "y": rows[2], "p": p,
                    "days": data["starts"][rows[0]]//86400}
            np.savez_compressed(pred_dir / f"{target}__rank{rank}.npz", **ours)
            entry = {"id": choice["id"], "rank": rank,
                     "metrics": {**metric(rows[2], p), "n_maps": int(len(np.unique(ours["mids"]))),
                                 "mean_y": float(np.mean(rows[2])), "mean_p": float(np.mean(p))}}
            try:
                entry["paired"] = compare(ours, target, smoke, out)
            except Exception as exc:  # the one-shot test must still record the locked metrics
                entry["paired_error"] = f"{type(exc).__name__}: {exc}"
            target_result.append(entry)
        results["targets"][target] = target_result
    write_json(out / "test.json", finite_json(results))
    return results


def finite_json(value):
    if isinstance(value, dict):
        return {k: finite_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [finite_json(v) for v in value]
    if isinstance(value, (float, np.floating)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, np.integer):
        return int(value)
    return value


def verify_selection_hashes(out, selection):
    for choices in selection["targets"].values():
        for choice in choices:
            if "model_sha256" not in choice:
                if "primary" in selection:  # current format always carries hashes
                    raise RuntimeError(f"selection lacks candidate hashes: {choice['id']}")
                continue
            if candidate_hashes(out, choice["id"]) != {k: v for k, v in choice.items() if k.endswith("_sha256")}:
                raise RuntimeError(f"candidate files changed after selection: {choice['id']}")


def mark_test_start(out, smoke):
    if smoke:
        return
    selection = out / "selection.json"
    if not selection.exists():
        raise RuntimeError("selection.json must exist before terminal test access")
    if (out / "test.json").exists():
        raise RuntimeError("terminal test was already evaluated once; inspect test.json")
    marker = out / "test_started.json"
    attempts = []
    if marker.exists():
        # A crash after the marker may be retried only for the byte-identical frozen selection
        # (candidate files are hash-verified before this point), so no reselection is possible.
        previous = load_json(marker)
        if previous["selection_sha256"] != digest(selection):
            raise RuntimeError("terminal test has already started under another selection")
        attempts = previous.get("attempts", [previous.get("status", "started_once")])
    attempts = attempts + [datetime.now(timezone.utc).isoformat()]
    write_json(marker, {"selection_sha256": digest(selection), "status": "started_once" if len(attempts) == 1 else "retried_after_crash",
                        "attempts": attempts})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=("learning_curve", "ablation", "count", "select", "test", "all"), required=True)
    parser.add_argument("--max-train", type=int)
    parser.add_argument("--max-iters", type=int, default=3000)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--dataset", type=Path, help="dataset.npz to pin in input.json on the first stage")
    parser.add_argument("--output", type=Path, help="output root (default runtime/artifacts/kills/kills_v3_20260923)")
    args = parser.parse_args()
    root = args.output.resolve() if args.output else OUTPUT
    out = root / "smoke" if args.smoke else root
    if args.stage == "test" and not (out / "selection.json").exists():
        raise RuntimeError("selection.json must exist before loading any terminal data")
    path, info = choose_input(out, args.dataset)
    if args.stage == "test":
        selection = load_json(out / "selection.json")
        if selection["input"] != info or selection["smoke"] != args.smoke:
            raise RuntimeError("selection input or mode changed")
        verify_selection_hashes(out, selection)
        # the start marker is written once, inside the stage loop below
    with np.load(path, allow_pickle=False) as z:
        keys = ("X", "y", "mids", "orient", "starts", "ends", "series_ids", "split", "team_ids")
        data = {k: z[k] for k in keys if k in z.files}
    data["y"][data["split"] == 2] = np.nan  # terminal labels are reloaded only after the test marker
    meta = load_json(ROOT / info["metadata"])
    if data["X"].shape[2] != len(meta["feature_names"]): raise ValueError("feature schema mismatch")
    if "team_ids" in data:
        data["S"] = series_features(data["starts"], data["ends"], data["series_ids"], data["team_ids"], data["y"])
    stages = ("learning_curve", "ablation", "count", "select", "test") if args.stage == "all" else (args.stage,)
    for stage in stages:
        if stage == "test" and not args.smoke:
            mark_test_start(out, False)
            with np.load(path, allow_pickle=False) as z:
                data["y"] = z["y"]
            if "team_ids" in data:
                data["S"] = series_features(data["starts"], data["ends"], data["series_ids"], data["team_ids"], data["y"])
        print(f"stage={stage} input={info['dataset']} sha256={info['sha256']}", flush=True)
        if stage == "test":
            report = test_stage(out, data, meta, args.smoke)
        else:
            args.stage = stage
            report = run_stage(args, out, data, meta)
        print(json.dumps({"stage": stage, "output": str(out / f"{stage}.json"),
                          "targets": list(report["targets"])}), flush=True)


if __name__ == "__main__":
    main()
