#!/usr/bin/env python3
"""Resumable full-corpus team-NW10 experiment; never changes the served artifact."""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import numpy as np
from catboost import CatBoostClassifier, FeaturesData, Pool
from scipy.optimize import minimize_scalar

from base.laning_model import build_history
from base.laning_history_store import LaningHistoryStore
from base.team_laning_model import (CLASS_NAMES, FEATURE_SET_HISTORY, FEATURE_SET_PAIRS,
    LANE_PAIRS, TeamLaningModel, cat_feature_names, team_labels, temperature_scale)

CUTS = (1783909632, 1786587184)
DELAY, WINDOW = 3600, 30 * 86400
V2 = ROOT / "data/laning_models/20260908_stratz_v2"
SERVED = ROOT / "data/laning_models/20260909_team_nw10_v1"
STORE = ROOT / "data/laning_history/20260909_stratz_v1"
PRO = ROOT / "runtime/artifacts/misc/pro_corpus_rich.npz"
PRO_WINDOW = ROOT / "runtime/artifacts/misc/phase_models_pro_window_scores.npz"
STAGES = ("prepare", "fit_C0", "fit_C1", "fit_C2", "select", "evaluate", "refit_check", "refit_final")
PAIR_CTR_MINIMAL = [f"{190+j}:Borders:TargetBorderCount=1:Prior=0.5"
                    for j in range(len(LANE_PAIRS))]
NUM_NAMES = ([f"history_{slot}_{stat}" for slot in range(10) for stat in range(12)] +
             [f"radiant_minus_dire_role_{role}_{stat}" for role in range(5) for stat in range(12)])
CAT_LOOKUP = np.asarray([str(i).encode("utf-8") for i in range(1024)], dtype=object)
PRE_FIX_TRAINER_SHA = "67d4c1e5659148a01b3fdc03d34deeda1ae087fc37bda0db6614ac9d9e2ba12b"


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + "\n")
    os.replace(tmp, path)


def atomic_npz(path, **values):
    path = Path(path)
    tmp = Path(str(path) + ".tmp")
    with open(tmp, "wb") as stream:
        np.savez(stream, **values)
    os.replace(tmp, path)


def atomic_npy(path, values):
    path = Path(path)
    tmp = Path(str(path) + ".tmp")
    with open(tmp, "wb") as stream:
        np.save(stream, values)
    os.replace(tmp, path)


def rss_bytes():
    # macOS reports bytes; Linux reports KiB.
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def split_rows(ts, duration, target, cuts=CUTS):
    """Strict completion purge; return indices and diagnostic counts."""
    ts, duration, target = map(np.asarray, (ts, duration, target))
    finite = np.isfinite(target)
    end = ts + duration.astype(np.int64)
    train = np.flatnonzero(finite & (end < cuts[0]))
    val = np.flatnonzero(finite & (ts >= cuts[0]) & (end < cuts[1]))
    test = np.flatnonzero(finite & (ts >= cuts[1]))
    report = {"train": len(train), "val": len(val), "test": len(test),
              "nonfinite": int((~finite).sum()),
              "purged_cut1": int((finite & (ts < cuts[0]) & (end >= cuts[0])).sum()),
              "purged_cut2": int((finite & (ts < cuts[1]) & (end >= cuts[1]) &
                                  (ts >= cuts[0])).sum())}
    return (train, val, test), report


def staleness_config(days):
    if days not in (0, 7, 14, 20, 30):
        raise ValueError("unsupported staleness gap")
    if days == 0:
        return DELAY, WINDOW
    return days * 86400, (30 - days) * 86400


def exclude_pub_overlap(mids, pub_mids):
    return ~np.isin(mids, pub_mids)


def deterministic_sample(rows, mids, count):
    rows = np.asarray(rows, dtype=np.int64)
    if len(rows) <= count:
        return rows
    # Independent of row order, reproducible across stages and restarts.
    hashed = np.asarray(mids[rows], dtype=np.uint64) * np.uint64(11400714819323198485)
    return np.sort(rows[np.argpartition(hashed, count - 1)[:count]])


def locate(sorted_rows, wanted):
    position = np.searchsorted(sorted_rows, wanted)
    if np.any(position >= len(sorted_rows)) or not np.array_equal(sorted_rows[position], wanted):
        raise ValueError("history row missing from prepared cache")
    return position


def history_chunks(corpus, rows, path, delay, window, chunk=500000):
    """Write a complete float32 history .npy, published only after all chunks."""
    tmp = Path(str(path) + ".tmp")
    mem = np.lib.format.open_memmap(tmp, mode="w+", dtype=np.float32,
                                    shape=(len(rows), 10, 12))
    started = time.monotonic()
    for start in range(0, len(rows), chunk):
        stop = min(start + chunk, len(rows))
        mem[start:stop] = build_history(corpus, rows[start:stop],
            availability_delay_seconds=delay, recent_window_seconds=window)
        mem.flush()
        print(f"history rows={stop}/{len(rows)} seconds={time.monotonic()-started:.1f} "
              f"peak_rss={rss_bytes()}", flush=True)
    del mem
    os.replace(tmp, path)


def matrix_arrays(heroes, history, pairs=False):
    """FeaturesData numeric/categorical arrays; categorical bytes hash as serving strings."""
    n = len(heroes)
    num = np.empty((n, 180), dtype=np.float32)
    num[:, :120] = np.asarray(history, dtype=np.float32).reshape(n, 120)
    num[:, 120:] = (history[:, :5] - history[:, 5:]).reshape(n, 60)
    cat = np.empty((n, 23 if pairs else 10), dtype=object)
    cat[:, :10] = CAT_LOOKUP[heroes]
    if pairs:
        # Pair codes are >=1025 and <1,048,576; one lookup avoids one Python
        # string allocation per cell in the full 4M-row training pool.
        for j, (a, b) in enumerate(LANE_PAIRS, 10):
            codes = heroes[:, a].astype(np.int64) * 1024 + heroes[:, b]
            unique, inverse = np.unique(codes, return_inverse=True)
            strings = np.asarray([str(int(code)).encode() for code in unique], dtype=object)
            cat[:, j] = strings[inverse]
    return num, cat


def features_pool(heroes, history, pairs=False, labels=None):
    num, cat = matrix_arrays(heroes, history, pairs)
    feature_set = FEATURE_SET_PAIRS if pairs else FEATURE_SET_HISTORY
    data = FeaturesData(num_feature_data=num, cat_feature_data=cat,
                        num_feature_names=NUM_NAMES,
                        cat_feature_names=cat_feature_names(feature_set))
    return Pool(data, label=labels), num, cat


def write_tsv(path, corpus, rows, history_at, pairs=False, chunk=25000):
    """Atomic disk-backed CatBoost input; no million-row object frame."""
    import pandas as pd
    path = Path(path)
    write_column_description(path, pairs)
    if path.exists():
        return
    tmp = Path(str(path)+".tmp")
    names = ["Label", *NUM_NAMES,
             *cat_feature_names(FEATURE_SET_PAIRS if pairs else FEATURE_SET_HISTORY)]
    with open(tmp,"w",encoding="utf-8",newline="") as stream:
        stream.write("\t".join(names)+"\n")
        for start in range(0,len(rows),chunk):
            chosen=rows[start:start+chunk]
            heroes=corpus["heroes"][chosen]
            h=history_at(chosen)
            numeric=np.empty((len(chosen),180),dtype=np.float32)
            numeric[:,:120]=h.reshape(len(chosen),120)
            numeric[:,120:]=(h[:,:5]-h[:,5:]).reshape(len(chosen),60)
            columns={"Label":team_labels(corpus["team_nw10"][chosen])}
            columns.update({name:numeric[:,j] for j,name in enumerate(NUM_NAMES)})
            columns.update({f"hero_{j}":heroes[:,j] for j in range(10)})
            if pairs:
                columns.update({f"pair_{a}_{b}":heroes[:,a]*1024+heroes[:,b]
                                for a,b in LANE_PAIRS})
            frame=pd.DataFrame(columns,columns=names)
            frame.to_csv(stream,sep="\t",header=False,index=False,float_format="%.9g")
            print(f"tsv={path.name} rows={start+len(chosen)}/{len(rows)} peak_rss={rss_bytes()}",
                  flush=True)
    os.replace(tmp,path)


def write_column_description(tsv, pairs=False):
    description=Path(str(tsv)+".cd")
    lines=["0\tLabel"]+[f"{1+180+j}\tCateg" for j in range(23 if pairs else 10)]
    expected="\n".join(lines)+"\n"
    if description.exists():
        if description.read_text()!=expected:
            raise ValueError(f"column description mismatch: {description}")
        return
    tmp=Path(str(description)+".tmp")
    tmp.write_text(expected)
    os.replace(tmp,description)


def quantized_pool(tsv, pairs=False, borders=None, threads=4):
    tsv=Path(tsv)
    write_column_description(tsv,pairs)
    description=Path(str(tsv)+".cd")
    # catboost.utils.quantize streams numeric files but CatBoost 1.2.8 rejects
    # categorical columns there. Pool(file) supports them; quantize after read.
    pool=Pool(str(tsv),column_description=str(description),has_header=True,
              thread_count=threads)
    kwargs={"used_ram_limit":"5gb"}
    if borders is not None:
        kwargs["input_borders"]=str(borders)
    pool.quantize(**kwargs)
    return pool


def save_quantized_pool(pool,path):
    path=Path(path)
    tmp=Path(str(path)+".tmp")
    pool.save(str(tmp))
    os.replace(tmp,path)


def load_quantized_pool(path):
    return Pool("quantized://"+str(Path(path)))


def raw_file_pool(tsv, threads=4):
    """Use raw VAL for CatBoost eval; quantized eval mis-scores categorical CTRs."""
    tsv = Path(tsv)
    return Pool(str(tsv), column_description=str(tsv)+".cd",
                has_header=True, thread_count=threads)


def refit_replay_reference(model, heroes, history, pairs):
    replay_pool, _, _ = features_pool(heroes, history, pairs)
    return model.predict_proba(replay_pool, thread_count=1)


def model_meta(pairs, temperature=1.0):
    return {"team_laning_feature_set": FEATURE_SET_PAIRS if pairs else FEATURE_SET_HISTORY,
            "team_laning_temperature": str(temperature),
            "team_laning_class_order": ",".join(CLASS_NAMES),
            "team_laning_history_delay_seconds": str(DELAY),
            "team_laning_recent_window_seconds": str(WINDOW)}


def save_model(model, path):
    tmp = Path(str(path) + ".tmp")
    model.save_model(str(tmp))
    os.replace(tmp, path)


def logloss(labels, p):
    return float(-np.log(np.maximum(p[np.arange(len(labels)), labels], 1e-15)).mean())


def auc_binary(scores, labels):
    labels = np.asarray(labels, dtype=bool)
    npos = int(labels.sum()); nneg = len(labels) - npos
    if not npos or not nneg:
        return None
    order = np.argsort(scores, kind="mergesort")
    sorted_scores = scores[order]
    boundaries = np.r_[0, np.flatnonzero(sorted_scores[1:] != sorted_scores[:-1]) + 1,
                       len(scores)]
    ranks = np.repeat((boundaries[:-1]+1+boundaries[1:])/2,
                      np.diff(boundaries))
    return float((ranks[labels[order]].sum() - npos * (npos + 1) / 2) / (npos * nneg))


def wilson(hits, count):
    if not count:
        return None
    z = 1.959963984540054
    p = hits / count
    den = 1 + z*z/count
    mid = (p + z*z/(2*count))/den
    half = z*math.sqrt(p*(1-p)/count + z*z/(4*count*count))/den
    return [mid-half, mid+half]


def side_view(labels, p):
    side = np.where(p[:, 2] >= p[:, 0], 2, 0)
    confidence = p[np.arange(len(p)), side]
    hit = labels == side
    return confidence, hit


def map_win_side_metrics(wins,p):
    side=np.where(p[:,2]>=p[:,0],1,0)
    confidence=p[np.arange(len(p)),np.where(side==1,2,0)]
    hits=side==wins
    result={"n":len(wins),"wins_semantics":"int(didRadiantWin); 1=Radiant",
            "overall_hit_rate":float(hits.mean()) if len(wins) else None,"thresholds":{}}
    for threshold in (.55,.60,.65):
        chosen=confidence>=threshold
        count=int(chosen.sum())
        result["thresholds"][str(threshold)]={"n":count,
            "coverage":count/len(wins) if len(wins) else None,
            "hit_rate":float(hits[chosen].mean()) if count else None,
            "wilson95":wilson(int(hits[chosen].sum()),count)}
    return result


def metrics(labels, p, served_coverage=None):
    if not len(labels):
        return {"n": 0}
    labels = np.asarray(labels, dtype=np.int8)
    p = np.asarray(p, dtype=np.float64)
    valid = labels != 1
    direction = p[valid, 2] / np.maximum(p[valid, 0] + p[valid, 2], 1e-15)
    conf, hit = side_view(labels, p)
    top = p.max(axis=1); correct = p.argmax(axis=1) == labels
    ece = sum(abs(correct[(top >= k/10) & (top < (k+1)/10)].mean() -
                  top[(top >= k/10) & (top < (k+1)/10)].mean()) *
              ((top >= k/10) & (top < (k+1)/10)).mean()
              for k in range(10) if np.any((top >= k/10) & (top < (k+1)/10)))
    thresholds = {}
    for threshold in (.55, .60, .65):
        chosen = conf >= threshold
        k = int(chosen.sum())
        thresholds[str(threshold)] = {"n": k, "coverage": k/len(labels),
            "hit_rate": float(hit[chosen].mean()) if k else None,
            "wilson95": wilson(int(hit[chosen].sum()), k)}
    matched = {}
    if served_coverage:
        order = np.argsort(-conf, kind="mergesort")
        for threshold, k in served_coverage.items():
            chosen = order[:k]
            matched[threshold] = {"n": int(k),
                "hit_rate": float(hit[chosen].mean()) if k else None,
                "wilson95": wilson(int(hit[chosen].sum()), int(k))}
    return {"n": len(labels), "ll3": logloss(labels, p),
        "acc3": float(correct.mean()),
        "brier": float(np.mean(np.sum((p - np.eye(3)[labels])**2, axis=1))),
        "AUC_dir": auc_binary(direction, labels[valid] == 2), "ece_top10": float(ece),
        "class_counts": np.bincount(labels, minlength=3).tolist(),
        "side_thresholds": thresholds, "matched_coverage": matched}


def fit_temperature(labels, p):
    fit = minimize_scalar(lambda t: logloss(labels, temperature_scale(p, t)),
                          bounds=(.5, 2.0), method="bounded")
    return float(fit.x) if fit.success else 1.0


def day_bootstrap(ts, labels, baseline, candidate, matched_k=None, draws=2000):
    if not len(labels):
        return {"n": 0, "draws": 0}
    days, inverse = np.unique(np.asarray(ts)//86400, return_inverse=True)
    loss = np.log(np.maximum(baseline[np.arange(len(labels)), labels], 1e-15)) - \
           np.log(np.maximum(candidate[np.arange(len(labels)), labels], 1e-15))
    counts = np.bincount(inverse)
    sums = np.bincount(inverse, weights=loss)
    rng = np.random.default_rng(266)
    sampled = rng.integers(0, len(days), (draws, len(days)))
    boot = sums[sampled].sum(axis=1)/counts[sampled].sum(axis=1)
    result = {"ll3_delta": float(loss.mean()), "ll3_ci95": np.quantile(boot, [.025,.975]).tolist(),
              "days": len(days), "draws": draws}
    if matched_k is not None:
        bconf, bhit = side_view(labels, baseline)
        cconf, chit = side_view(labels, candidate)
        for threshold, k in matched_k.items():
            bchosen = np.zeros(len(labels), dtype=bool)
            cchosen = np.zeros(len(labels), dtype=bool)
            bchosen[np.argsort(-bconf, kind="mergesort")[:k]] = True
            cchosen[np.argsort(-cconf, kind="mergesort")[:k]] = True
            bn = np.bincount(inverse, weights=bchosen.astype(int))
            cn = np.bincount(inverse, weights=cchosen.astype(int))
            bs = np.bincount(inverse, weights=(bchosen & bhit).astype(int))
            cs = np.bincount(inverse, weights=(cchosen & chit).astype(int))
            delta = (cs[sampled].sum(axis=1)/np.maximum(cn[sampled].sum(axis=1),1) -
                     bs[sampled].sum(axis=1)/np.maximum(bn[sampled].sum(axis=1),1))
            result[f"matched_hit_delta_{threshold}"] = {
                "point": float(chit[cchosen].mean()-bhit[bchosen].mean()) if k else None,
                "ci95": np.quantile(delta,[.025,.975]).tolist()}
    return result


def pro_rows(pub_mids, smoke=False):
    with np.load(PRO) as source:
        rich = {key: source[key] for key in ("mids", "ts", "heroes", "accounts", "nw", "wins")}
    with np.load(PRO_WINDOW) as source:
        window = source["mid"]
    mids = rich["mids"]
    max_window_ts = int(rich["ts"][np.isin(mids, window)].max())
    group = np.full(len(mids), "", dtype="<U8")
    in_window = np.isin(mids, window)
    group[in_window & (rich["ts"] < CUTS[0])] = "PRO_PRE"
    group[in_window & (rich["ts"] >= CUTS[0])] = "PRO_FWD1"
    group[rich["ts"] > max_window_ts] = "PRO_FWD2"
    chosen = group != ""
    counts = {"window_missing_in_rich": int((~np.isin(window, mids)).sum()),
              "pub_overlap": int((chosen & ~exclude_pub_overlap(mids, pub_mids)).sum())}
    chosen &= exclude_pub_overlap(mids, pub_mids)
    invalid_hero = np.any((rich["heroes"] <= 0) | (rich["heroes"] >= 1024), axis=1)
    counts["hero_range"] = int((chosen & invalid_hero).sum())
    chosen &= ~invalid_hero
    unique = np.array([len(set(row)) == 10 for row in rich["heroes"][chosen]], dtype=bool)
    selected = np.flatnonzero(chosen)
    counts["duplicate_heroes"] = int((~unique).sum())
    chosen[selected[~unique]] = False
    valid_nw = np.isfinite(rich["nw"][:, 10])
    counts["nonfinite_nw10"] = int((chosen & ~valid_nw).sum())
    chosen &= valid_nw
    rows = np.flatnonzero(chosen)
    result = {key: values[rows] for key, values in rich.items() if key != "nw"}
    result["target"] = team_labels(rich["nw"][rows,10])
    result["group"] = group[rows]
    counts["kept"] = {name: int((group[rows] == name).sum()) for name in
                      ("PRO_PRE", "PRO_FWD1", "PRO_FWD2")}
    return result, counts


def pro_history(pro, history_config):
    store = LaningHistoryStore(STORE)
    history = np.empty((len(pro["mids"]), 10, 12), dtype=np.float32)
    valid = np.ones(len(history), dtype=bool)
    for i in range(len(history)):
        try:
            history[i] = store.history(pro["heroes"][i], pro["accounts"][i],
                                       pro["ts"][i], **history_config)
        except Exception:
            valid[i] = False
        if (i+1)%2000==0 or i+1==len(history):
            print(f"pro_history rows={i+1}/{len(history)} peak_rss={rss_bytes()}",flush=True)
    return history[valid], valid


def predict_batched(model, heroes, history, batch=50000):
    result = np.empty((len(heroes), 3), dtype=np.float64)
    for start in range(0, len(heroes), batch):
        stop = min(start+batch, len(heroes))
        result[start:stop] = model.predict_proba(heroes[start:stop], history[start:stop])
    return result


class Runner:
    def __init__(self, args):
        self.args = args
        self.out = args.output_dir
        self.out.mkdir(parents=True, exist_ok=True)
        self.corpus = None
        self.splits = None
        self.hc = None
        self.hc_rows = None
        timing_path=self.out/"stage_timing.json"
        self.timing = json.loads(timing_path.read_text()) if timing_path.exists() else {}
        self.source_hashes = {"trainer":sha(__file__),
            "base/team_laning_model.py":sha(ROOT/"base/team_laning_model.py"),
            "corpus_manifest":sha(args.corpus/"manifest.json")}
        self.plan_verified=False

    def plan(self):
        plan={"schema":"team_laning_full_v1","corpus":str(self.args.corpus.resolve()),
            "cuts_unix":CUTS,"threads":self.args.threads,
            "max_iterations":self.args.max_iterations,"smoke_rows":self.args.smoke_rows,
            "smoke_iterations":self.args.smoke_iterations,
            "val_rows":self.args.val_rows,"pair_ctr":self.args.pair_ctr,
            "candidates":self.args.candidates,"source_sha256":self.source_hashes}
        path=self.out/"plan.json"
        if path.exists():
            saved=json.loads(path.read_text())
            current=json.loads(json.dumps(plan))
            if saved!=current:
                # An already-running prepare may have sealed the exact pre-fix
                # trainer. Accept only that known source transition; keep its
                # plan unchanged and reject every other input/config change.
                previous=json.loads(json.dumps(current))
                previous["source_sha256"]["trainer"]=PRE_FIX_TRAINER_SHA
                if saved!=previous:
                    raise ValueError("run plan or source changed; use a new output directory")
                print("resuming plan sealed by pre-fix trainer",flush=True)
        else:
            if any(self.out.iterdir()):
                raise ValueError("nonempty output directory lacks plan.json; use a new directory")
            atomic_json(path,plan)
        self.plan_verified=True

    def stage(self, name, fn):
        started = time.monotonic()
        atomic_json(self.out/"status.json", {"state":"RUNNING", "stage":name,
            "pid":os.getpid(), "updated":time.time()})
        print(f"stage={name} started={time.time():.0f}", flush=True)
        fn()
        elapsed = time.monotonic()-started
        if name not in self.timing:
            self.timing[name] = {"seconds":elapsed,"peak_rss_bytes":rss_bytes()}
            atomic_json(self.out/"stage_timing.json",self.timing)
        print(f"stage={name} seconds={elapsed:.1f} peak_rss={rss_bytes()}", flush=True)

    def load_corpus(self):
        if self.corpus is not None:
            return
        plan = json.loads((V2/"plan.json").read_text())
        if tuple(plan["split"]["cuts_unix"]) != CUTS:
            raise ValueError("v2 cut timestamps changed")
        with np.load(self.args.corpus/"rows.npz") as source:
            needed = ("mid","ts","duration","heroes","accounts","lane_labels","team_nw10")
            self.corpus = {key:source[key] for key in needed}
        manifest = json.loads((self.args.corpus/"manifest.json").read_text())
        if not manifest.get("complete") or manifest["rows"] != len(self.corpus["mid"]):
            raise ValueError("corpus manifest incomplete")
        heroes=self.corpus["heroes"]
        if (heroes.shape!=(len(self.corpus["mid"]),10) or
                np.any((heroes<=0)|(heroes>=1024)) or
                np.any(np.diff(self.corpus["ts"])<0)):
            raise ValueError("unsupported corpus hero or timestamp contract")
        for start in range(0,len(heroes),500000):
            if np.any(np.diff(np.sort(heroes[start:start+500000],axis=1),axis=1)==0):
                raise ValueError("duplicate heroes in corpus")
        self.splits, report = split_rows(self.corpus["ts"],self.corpus["duration"],
                                         self.corpus["team_nw10"])
        if self.args.smoke_rows:
            n = self.args.smoke_rows
            self.splits = tuple(deterministic_sample(rows,self.corpus["mid"],
                                  n if i==0 else n//2) for i,rows in enumerate(self.splits))
            # The served confirmation IDs were selected separately from the
            # min-hash smoke sample. Include some so its restricted score is tested.
            with np.load(SERVED/"test_predictions.npz") as source:
                confirmation_mids = source["mid"]
            confirmed = np.flatnonzero(np.isin(self.corpus["mid"],confirmation_mids))
            confirmed = deterministic_sample(confirmed,self.corpus["mid"],
                                              min(1000,len(confirmed)))
            if not np.all((self.corpus["ts"][confirmed]>=CUTS[1]) &
                          np.isfinite(self.corpus["team_nw10"][confirmed])):
                raise ValueError("served confirmation IDs outside TEST")
            self.splits = (self.splits[0],self.splits[1],
                           np.unique(np.concatenate((self.splits[2][len(confirmed):],confirmed))))
        if (self.out/"prepare.json").exists() and all(
                (self.out/f"{name}_rows.npy").exists() for name in ("train","val","test","c0")):
            self.splits = tuple(np.load(self.out/f"{name}_rows.npy") for name in
                                ("train","val","test"))
            self.c0_rows = np.load(self.out/"c0_rows.npy")
        self.split_report = report

    def prepare(self):
        self.load_corpus()
        ready = self.out/"prepare.json"
        if ready.exists():
            info = json.loads(ready.read_text())
            self.hc_rows = np.load(self.out/"hc_rows.npy",mmap_mode="r")
            self.hc = np.load(self.out/"hc.npy",mmap_mode="r")
            self.stale_rows = np.load(self.out/"stale_rows.npy")
            self.prepare_info = info
            return
        corpus = self.corpus
        with np.load(V2/"reserved_rows.npz") as source:
            old_rows = source["indices"][:800000]
        if not np.all(np.isin(old_rows,self.splits[0] if not self.args.smoke_rows else
                                    split_rows(corpus["ts"],corpus["duration"],corpus["team_nw10"])[0][0])):
            raise ValueError("C0 reserved rows outside TRAIN")
        if self.args.smoke_rows:
            self.c0_rows = np.sort(old_rows[np.linspace(0,len(old_rows)-1,
                min(self.args.smoke_rows,len(old_rows)),dtype=np.int64)])
        else:
            self.c0_rows = old_rows
        for name, selected_rows in zip(("train","val","test","c0"),
                                        (*self.splits,self.c0_rows)):
            atomic_npy(self.out/f"{name}_rows.npy",selected_rows)
        with np.load(V2/"inputs.npz") as source:
            v2_mid = source["mid"]
            v2_hc = source["hc"]
        probe_path = SERVED/"verification_probe.npz"
        with np.load(probe_path) as source:
            probe_mid, probe_hc = source["mid"],source["history"]
        sorted_mid = np.argsort(corpus["mid"])
        def mids_to_rows(mids):
            pos = np.searchsorted(corpus["mid"][sorted_mid],mids)
            if np.any(pos >= len(sorted_mid)) or not np.array_equal(corpus["mid"][sorted_mid[pos]],mids):
                raise ValueError("parity match ID missing in corpus")
            return sorted_mid[pos]
        parity_rows = mids_to_rows(v2_mid)
        probe_rows = mids_to_rows(probe_mid)
        if self.args.smoke_rows:
            sampled = deterministic_sample(np.arange(len(v2_mid)),v2_mid, min(2000,len(v2_mid)))
            parity_rows = parity_rows[sampled]
            parity_expected = v2_hc[sampled]
        else:
            sampled = deterministic_sample(np.arange(len(v2_mid)),v2_mid, min(200000,len(v2_mid)))
            parity_rows = parity_rows[sampled]
            parity_expected = v2_hc[sampled]
        rows = (np.unique(np.concatenate((*self.splits,self.c0_rows,parity_rows,probe_rows)))
                if self.args.smoke_rows else np.arange(len(corpus["mid"]),dtype=np.int64))
        atomic_npy(self.out/"hc_rows.npy",rows)
        history_chunks(corpus,rows,self.out/"hc.npy",DELAY,WINDOW)
        self.hc_rows = np.load(self.out/"hc_rows.npy",mmap_mode="r")
        self.hc = np.load(self.out/"hc.npy",mmap_mode="r")
        delta_v2 = float(np.max(np.abs(self.hc[locate(rows,parity_rows)]-parity_expected)))
        delta_probe = float(np.max(np.abs(self.hc[locate(rows,probe_rows)]-probe_hc)))
        if delta_v2 != 0 or delta_probe != 0:
            raise ValueError(f"history parity failure: v2={delta_v2} probe={delta_probe}")
        store = LaningHistoryStore(STORE)
        serving_rows = deterministic_sample(self.splits[2],corpus["mid"],
                                             min(2000,len(self.splits[2])))
        delta_store = 0.0
        for row in serving_rows:
            actual = store.history(corpus["heroes"][row],corpus["accounts"][row],
                                   corpus["ts"][row],DELAY,WINDOW)
            delta_store = max(delta_store,float(np.max(np.abs(actual-self.hc[locate(rows,np.array([row]))][0]))))
        if delta_store > 1e-6:
            raise ValueError(f"serving history parity failure: {delta_store}")
        stale_rows = deterministic_sample(self.splits[2],corpus["mid"],
                                           min(300000 if not self.args.smoke_rows else 2000,
                                               len(self.splits[2])))
        atomic_npy(self.out/"stale_rows.npy",stale_rows)
        self.stale_rows = stale_rows
        for days in (7,14,20,30):
            delay,window = staleness_config(days)
            history_chunks(corpus,stale_rows,self.out/f"stale_{days}.npy",delay,window)
        self.prepare_info = {"split":self.split_report,"selected":list(map(len,self.splits)),
            "c0_rows":len(self.c0_rows),"history_rows":len(rows),"parity_v2_n":len(parity_rows),
            "parity_v2_max_abs":delta_v2,"parity_probe_n":len(probe_rows),
            "parity_probe_max_abs":delta_probe,"parity_serving_n":len(serving_rows),
            "parity_serving_max_abs":delta_store,"stale_rows":len(stale_rows)}
        atomic_json(ready,self.prepare_info)

    def get_history(self,rows):
        return np.asarray(self.hc[locate(self.hc_rows,rows)])

    def release_for_fit(self):
        """Keep only the quantized Pool in RAM while fitting full candidates."""
        self.corpus=self.hc=self.hc_rows=None
        gc.collect()

    def restore_after_fit(self):
        self.load_corpus()
        self.hc_rows=np.load(self.out/"hc_rows.npy",mmap_mode="r")
        self.hc=np.load(self.out/"hc.npy",mmap_mode="r")

    def candidate_rows(self,name):
        if name=="C0":
            if not hasattr(self,"c0_rows"):
                with np.load(V2/"reserved_rows.npz") as source:
                    old = source["indices"][:800000]
                self.c0_rows = (np.sort(old[np.linspace(0,len(old)-1,self.args.smoke_rows,
                    dtype=np.int64)]) if self.args.smoke_rows else old)
            return self.c0_rows
        return self.splits[0]

    def fit_val_rows(self):
        val = self.splits[1]
        if self.args.val_rows:
            return deterministic_sample(val,self.corpus["mid"],self.args.val_rows)
        return val

    def learning_rate(self,name):
        """C0 reproduces the served recipe (0.08); other candidates use --learning-rate."""
        return .08 if name=="C0" else float(self.args.learning_rate)

    def ctr_params(self,pairs):
        if pairs and self.args.pair_ctr == "minimal":
            return {"per_feature_ctr":PAIR_CTR_MINIMAL}
        return {}

    def fit(self,name):
        directory = self.out/name
        directory.mkdir(exist_ok=True)
        if (directory/"validation.json").exists() and (directory/"team.cbm").exists() and (directory/"validation.npz").exists():
            cached=json.loads((directory/"validation.json").read_text())
            expected_val_rows=min(self.args.val_rows,len(self.splits[1])) if self.args.val_rows else len(self.splits[1])
            expected={"learning_rate":self.learning_rate(name),
                      "pair_ctr":self.args.pair_ctr if name=="C2" else None,
                      "val_rows":expected_val_rows}
            for key,value in expected.items():
                if cached.get(key)!=value:
                    raise ValueError(f"cached {name} {key} mismatch: {cached.get(key)} != {value}")
            return
        rows = self.candidate_rows(name); val = self.fit_val_rows()
        pairs = name=="C2"
        replay_n=min(256,len(val))
        replay_heroes=self.corpus["heroes"][val[:replay_n]].copy()
        replay_history=self.get_history(val[:replay_n]).copy()
        val_mid=self.corpus["mid"][val].copy()
        val_labels=team_labels(self.corpus["team_nw10"][val])
        if self.args.smoke_rows:
            train_h = self.get_history(rows)
            val_h = self.get_history(val)
            train,tn,tc = features_pool(self.corpus["heroes"][rows],train_h,pairs,
                                       team_labels(self.corpus["team_nw10"][rows]))
            validation,vn,vc = features_pool(self.corpus["heroes"][val],val_h,pairs,
                team_labels(self.corpus["team_nw10"][val]))
        else:
            train_tsv=directory/"train.tsv"
            val_tsv=directory/"validation.tsv"
            write_tsv(train_tsv,self.corpus,rows,self.get_history,pairs)
            write_tsv(val_tsv,self.corpus,val,self.get_history,pairs)
            self.release_for_fit()
            train_q=directory/"train.qbin"
            borders=directory/"borders.tsv"
            if not train_q.exists():
                train=quantized_pool(train_tsv,pairs,threads=self.args.threads)
                tmp=Path(str(borders)+".tmp")
                train.save_quantization_borders(str(tmp))
                os.replace(tmp,borders)
                save_quantized_pool(train,train_q)
                del train
                gc.collect()
            # CatBoost 1.2.8 can fit from a reloaded quantized train pool, but
            # its predictor rejects reloaded quantized categorical eval pools.
            validation=raw_file_pool(val_tsv,self.args.threads)
            train=load_quantized_pool(train_q)
        model = CatBoostClassifier(loss_function="MultiClass",eval_metric="MultiClass",depth=6,
            learning_rate=self.learning_rate(name),l2_leaf_reg=5,one_hot_max_size=255,max_ctr_complexity=1,
            random_seed=42,iterations=self.args.smoke_iterations if self.args.smoke_rows else self.args.max_iterations,
            thread_count=self.args.threads,allow_writing_files=False,
            used_ram_limit="5gb", metadata=model_meta(pairs), **self.ctr_params(pairs))
        started = time.monotonic()
        model.fit(train,eval_set=validation,early_stopping_rounds=100,use_best_model=True,verbose=100)
        fit_seconds = time.monotonic()-started
        if list(model.classes_) != [0,1,2]:
            raise ValueError("missing training class")
        probs = model.predict_proba(validation,thread_count=1)
        save_model(model,directory/"team.cbm")
        atomic_npz(directory/"validation.npz",mid=val_mid,probabilities=probs)
        loaded = TeamLaningModel.load(directory)
        replay = loaded.predict_proba(replay_heroes,replay_history)
        diff = float(np.max(np.abs(replay-probs[:replay_n])))
        if diff>1e-9:
            raise ValueError(f"{name} serving replay mismatch: {diff}")
        score = metrics(val_labels,probs)
        score.update(best_iteration=int(model.best_iteration_),tree_count=int(model.tree_count_),
                     fit_seconds=fit_seconds,seconds_per_100_iter=fit_seconds/max(model.tree_count_,1)*100,
                     peak_rss_bytes=rss_bytes(),replay_max_abs=diff,
                     train_rows=len(rows),val_rows=len(val),pair_ctr=self.args.pair_ctr if pairs else None,
                     learning_rate=self.learning_rate(name))
        atomic_json(directory/"validation.json",score)
        if not self.args.smoke_rows:
            del train,validation,model,loaded
            gc.collect()
            self.restore_after_fit()

    def select(self):
        if (self.out/"selection.json").exists():
            return
        available = [c for c in self.args.candidates if (self.out/c/"validation.json").exists()]
        if len(available)!=len(self.args.candidates):
            raise ValueError(f"missing fitted candidates: {sorted(set(self.args.candidates)-set(available))}")
        scores = {c:json.loads((self.out/c/"validation.json").read_text()) for c in available}
        winner = min(available,key=lambda c:scores[c]["ll3"])
        with np.load(self.out/winner/"validation.npz") as source:
            probs = source["probabilities"]
        labels = team_labels(self.corpus["team_nw10"][self.fit_val_rows()])
        temperature = fit_temperature(labels,probs)
        selected_dir = self.out/"selected"; selected_dir.mkdir(exist_ok=True)
        model = CatBoostClassifier(); model.load_model(str(self.out/winner/"team.cbm"))
        model.get_metadata()["team_laning_temperature"] = str(temperature)
        save_model(model,selected_dir/"team.cbm")
        loaded = TeamLaningModel.load(selected_dir)
        val = self.fit_val_rows()[:min(256,len(self.fit_val_rows()))]
        replay = loaded.predict_proba(self.corpus["heroes"][val],self.get_history(val))
        diff = float(np.max(np.abs(replay-temperature_scale(probs[:len(val)],temperature))))
        if diff>1e-9:
            raise ValueError(f"selected replay mismatch: {diff}")
        atomic_json(self.out/"selection.json",{"winner":winner,"temperature":temperature,
            "validation":scores,"selected_calibrated":metrics(labels,temperature_scale(probs,temperature)),
            "candidate_sha256":{c:sha(self.out/c/"team.cbm") for c in available},
            "selected_sha256":sha(selected_dir/"team.cbm"),"replay_max_abs":diff})

    def evaluate(self):
        if (self.out/"evaluation.json").exists():
            return
        selection_path = self.out/"selection.json"
        if not selection_path.exists():
            raise ValueError("sealed evaluation requires selection.json")
        selection = json.loads(selection_path.read_text())
        test = self.splits[2]
        labels = team_labels(self.corpus["team_nw10"][test])
        heroes = self.corpus["heroes"][test]
        history = self.get_history(test)
        models = {"served":TeamLaningModel.load(SERVED/"selected")}
        models.update({c:TeamLaningModel.load(self.out/c) for c in self.args.candidates
                       if (self.out/c/"team.cbm").exists()})
        models["winner_calibrated"] = TeamLaningModel.load(self.out/"selected")
        if any(model.history_config != models["served"].history_config for model in models.values()):
            raise ValueError("model history configurations differ; PRO needs per-model history")
        probabilities = {name:predict_batched(model,heroes,history) for name,model in models.items()}
        with np.load(SERVED/"test_predictions.npz") as source:
            confirmation_mids = source["mid"]
        subsets = {"TEST":np.arange(len(test)),
                   "TEST_served_confirmation":np.flatnonzero(np.isin(self.corpus["mid"][test],confirmation_mids))}
        report = {"selection_sha256":sha(selection_path),"test":{},"staleness":{},
                  "atypicality":{},"pro":{},"pro_exclusions":{}}
        for key,selected in subsets.items():
            baseline = probabilities["served"][selected]
            y = labels[selected]
            bconf,_ = side_view(y,baseline)
            coverage = {str(t):int((bconf>=t).sum()) for t in (.55,.60)}
            report["test"][key] = {name:metrics(y,p[selected],coverage) for name,p in probabilities.items()}
            report["test"][key]["bootstrap_winner_vs_served"] = day_bootstrap(
                self.corpus["ts"][test[selected]],y,baseline,
                probabilities["winner_calibrated"][selected],coverage)
        stale_rows = self.stale_rows
        for days in (0,7,14,20,30):
            h = self.get_history(stale_rows) if days==0 else np.load(self.out/f"stale_{days}.npy",mmap_mode="r")
            y = team_labels(self.corpus["team_nw10"][stale_rows])
            names = ["served","winner_calibrated"] + (["C1"] if "C1" in models and selection["winner"]!="C1" else [])
            report["staleness"][str(days)] = {name:metrics(y,predict_batched(models[name],
                self.corpus["heroes"][stale_rows],h)) for name in names}
        train = self.splits[0]
        role_counts = np.zeros((1024,5),dtype=np.int64)
        for role in range(5):
            role_counts[:,role] = np.bincount(self.corpus["heroes"][train][:,[role,role+5]].reshape(-1),
                                               minlength=1024)
        role_p = role_counts/np.maximum(role_counts.sum(axis=1,keepdims=True),1)
        atyp = np.min(role_p[heroes,np.tile(np.arange(5),2)],axis=1)
        for key,mask in (("typical",atyp>=.15),("atypical",atyp<.15)):
            y=labels[mask]
            report["atypicality"][key] = {"n":len(y),"radiant_lead_fraction":float((y==2).mean()) if len(y) else None,
                "models":{name:metrics(y,probabilities[name][mask]) for name in
                          ("served","C1","C2","winner_calibrated") if name in models}}
        pro,counts = pro_rows(self.corpus["mid"])
        report["pro_exclusions"] = counts
        ph, hist_ok = pro_history(pro, models["served"].history_config)
        counts["history_lookup_error"] = int((~hist_ok).sum())
        pro = {key: value[hist_ok] for key, value in pro.items()}
        pro_predictions = {name:predict_batched(model,pro["heroes"],ph) for name,model in models.items()}
        for group in ("PRO_PRE","PRO_FWD1","PRO_FWD2"):
            mask = pro["group"]==group
            b=pro_predictions["served"][mask]
            y=pro["target"][mask]
            bconf,_=side_view(y,b)
            coverage={str(t):int((bconf>=t).sum()) for t in (.55,.60)}
            report["pro"][group]={name:metrics(y,p[mask],coverage) for name,p in pro_predictions.items()}
            if group=="PRO_FWD1" and len(y):
                report["pro"][group]["bootstrap_winner_vs_served"]=day_bootstrap(
                    pro["ts"][mask],y,b,pro_predictions["winner_calibrated"][mask],coverage)
            report["pro"][group]["map_win_secondary"]={
                name:map_win_side_metrics(pro["wins"][mask],p[mask])
                for name,p in pro_predictions.items()}
        atomic_json(self.out/"evaluation.json",report)

    def refit(self,final=False):
        selection=json.loads((self.out/"selection.json").read_text())
        winner=selection["winner"]
        iterations=max(1,round(selection["validation"][winner]["best_iteration"]*1.2))
        directory=self.out/("refit_final" if final else "refit_check")
        directory.mkdir(exist_ok=True)
        if (directory/"report.json").exists() and (directory/"team.cbm").exists():
            cached=json.loads((directory/"report.json").read_text())
            for key,value in (("learning_rate",self.learning_rate(winner)),
                              ("iterations",iterations)):
                if cached.get(key)!=value:
                    raise ValueError(f"cached refit {key} mismatch: {cached.get(key)} != {value}")
            return
        if final and self.args.smoke_rows:
            rows=np.unique(np.concatenate(self.splits))
        elif final:
            rows=np.flatnonzero(np.isfinite(self.corpus["team_nw10"]))
        else:
            rows=np.unique(np.concatenate(self.splits[:2]))
        if final and not self.args.smoke_rows and len(self.hc_rows)!=len(self.corpus["mid"]):
            raise ValueError("refit_final requires history for all corpus rows")
        pairs=winner=="C2"
        replay_rows=rows[:min(256,len(rows))]
        replay_heroes=self.corpus["heroes"][replay_rows].copy()
        replay_history=self.get_history(replay_rows).copy()
        if self.args.smoke_rows:
            history=self.get_history(rows)
            pool,num,cat=features_pool(self.corpus["heroes"][rows],history,pairs,
                team_labels(self.corpus["team_nw10"][rows]))
        else:
            tsv=directory/"train.tsv"
            write_tsv(tsv,self.corpus,rows,self.get_history,pairs)
            self.release_for_fit()
            qbin=directory/"train.qbin"
            if not qbin.exists():
                raw_pool=quantized_pool(tsv,pairs,threads=self.args.threads)
                save_quantized_pool(raw_pool,qbin)
                del raw_pool
                gc.collect()
            pool=load_quantized_pool(qbin)
        model=CatBoostClassifier(loss_function="MultiClass",depth=6,learning_rate=self.learning_rate(winner),
            l2_leaf_reg=5,one_hot_max_size=255,max_ctr_complexity=1,random_seed=42,
            iterations=iterations,thread_count=self.args.threads,allow_writing_files=False,
            used_ram_limit="5gb",metadata=model_meta(pairs,selection["temperature"]),
            **self.ctr_params(pairs))
        start=time.monotonic();model.fit(pool,verbose=100);fit_seconds=time.monotonic()-start
        save_model(model,directory/"team.cbm")
        loaded=TeamLaningModel.load(directory)
        raw=refit_replay_reference(model,replay_heroes,replay_history,pairs)
        diff=float(np.max(np.abs(loaded.predict_proba(replay_heroes,replay_history)-
            temperature_scale(raw,selection["temperature"]))))
        if diff>1e-9:
            raise ValueError(f"refit replay mismatch: {diff}")
        if not self.args.smoke_rows:
            del pool,model
            gc.collect()
            self.restore_after_fit()
        report={"train_rows":len(rows),"iterations":iterations,
                "learning_rate":self.learning_rate(winner),"fit_seconds":fit_seconds,
                "peak_rss_bytes":rss_bytes(),"replay_max_abs":diff}
        if final:
            pro,counts=pro_rows(self.corpus["mid"])
            ph,hist_ok=pro_history(pro, loaded.history_config)
            counts["history_lookup_error"] = int((~hist_ok).sum())
            pro={key:value[hist_ok] for key,value in pro.items()}
            report["pro_exclusions"]=counts
            for group in ("PRO_FWD2","PRO_FWD1"):
                mask=pro["group"]==group
                report[group]={"forward_for_refit":group=="PRO_FWD2",
                    "metrics":metrics(pro["target"][mask],predict_batched(loaded,pro["heroes"][mask],ph[mask]))}
        else:
            test=self.splits[2];y=team_labels(self.corpus["team_nw10"][test])
            new=predict_batched(loaded,self.corpus["heroes"][test],self.get_history(test))
            old=predict_batched(TeamLaningModel.load(self.out/"selected"),
                                self.corpus["heroes"][test],self.get_history(test))
            report["TEST"]={"refit":metrics(y,new),"train_only":metrics(y,old),
                "paired_bootstrap":day_bootstrap(self.corpus["ts"][test],y,old,new),
                "diagnostic_test_temperature_do_not_use":fit_temperature(y,new)}
        atomic_json(directory/"report.json",report)

    def run(self):
        self.plan()
        self.load_corpus()
        selected=set(self.args.stages)
        if selected - set(STAGES):
            raise ValueError(f"unknown stages: {sorted(selected-set(STAGES))}")
        if "prepare" in selected:
            self.stage("prepare",self.prepare)
        elif (self.out/"prepare.json").exists():
            self.prepare()
        else:
            raise ValueError("prepare must complete first")
        for name in ("C0","C1","C2"):
            if f"fit_{name}" in selected and name in self.args.candidates:
                self.stage(f"fit_{name}",lambda n=name:self.fit(n))
        for name,fn in (("select",self.select),("evaluate",self.evaluate),
                        ("refit_check",lambda:self.refit(False)),
                        ("refit_final",lambda:self.refit(True))):
            if name in selected:
                self.stage(name,fn)
        if selected == set(STAGES):
            if (sha(__file__)!=self.source_hashes["trainer"] or
                    sha(ROOT/"base/team_laning_model.py")!=self.source_hashes["base/team_laning_model.py"]):
                raise ValueError("trainer or serving source changed during run")
            summary={"complete":True,"smoke_rows":self.args.smoke_rows,
                "prepare":self.prepare_info,"selection":json.loads((self.out/"selection.json").read_text()),
                "selection_sha256":sha(self.out/"selection.json"),
                "evaluation":json.loads((self.out/"evaluation.json").read_text()),
                "refit_check":json.loads((self.out/"refit_check/report.json").read_text()),
                "refit_final":json.loads((self.out/"refit_final/report.json").read_text()),
                "stage_timing":self.timing,
                "source_sha256":self.source_hashes,
                "limitations":["Post-match STRATZ role labels can encode lane outcome",
                    "Causal history delay is modeled at one hour, not observed ingestion",
                    "PRO_FWD2 uses a history store ending 2026-09-04",
                    "PRO_FWD1 is not forward for a final refit on all public maps"]}
            atomic_json(self.out/"summary.json",summary)
        atomic_json(self.out/"status.json",{"state":"DONE","stage":"complete",
            "pid":os.getpid(),"updated":time.time()})


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus",type=Path,default=ROOT/"data/laning_corpus/20260908_stratz_v1")
    p.add_argument("--output-dir",type=Path,required=True)
    p.add_argument("--threads",type=int,default=4)
    p.add_argument("--max-iterations",type=int,default=20000)
    p.add_argument("--stages",default=",".join(STAGES))
    p.add_argument("--candidates",default="C0,C1,C2")
    p.add_argument("--smoke-rows",type=int,default=0)
    p.add_argument("--smoke-iterations",type=int,default=200)
    p.add_argument("--val-rows",type=int,default=400000,
                   help="deterministic VAL sample for fitting and selection; 0 uses all")
    p.add_argument("--pair-ctr",choices=("minimal","default"),default="minimal")
    p.add_argument("--learning-rate",type=float,default=.08,
                   help="learning rate for C1/C2 and refits of a C1/C2 winner; C0 stays 0.08")
    args=p.parse_args()
    if args.val_rows < 0:
        p.error("--val-rows must be nonnegative")
    if not (0 < args.learning_rate <= 1):
        p.error("--learning-rate must be in (0, 1]")
    args.stages=tuple(args.stages.split(","))
    args.candidates=tuple(args.candidates.split(","))
    if set(args.candidates)-{"C0","C1","C2"}:
        p.error("invalid candidates")
    resolved = args.output_dir.resolve()
    models_root = ROOT/"data/laning_models"
    if any(path.parent == models_root and path.name.startswith("2026090")
           for path in (resolved,*resolved.parents)):
        p.error("output directory may not overwrite an existing 2026090* model run")
    if resolved == STORE or STORE in resolved.parents:
        p.error("output directory may not be the serving history store")
    runner=Runner(args)
    try:
        runner.run()
    except BaseException as exc:
        if runner.plan_verified:
            atomic_json(args.output_dir/"status.json",{"state":"FAIL","stage":str(exc),
                "pid":os.getpid(),"updated":time.time()})
        raise


if __name__=="__main__":
    main()
