#!/usr/bin/env python3
"""Train and compare four draft-only phases from a fresh canonical raw corpus.

Chronological, timestamp-grouped 60/20/20; C and vocabulary use train/validation
only. The separately saved evaluation bundle has never fitted test outcomes.
The final bundle is refitted on all rows and explicitly has no held-out score.
Large sparse designs are disk-backed and reused sequentially on a 16 GB host.
"""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import sys
import time
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import joblib
import numpy as np
from scipy import sparse
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score
from threadpoolctl import threadpool_limits

from base.draft_features import DraftFeatureEncoder, KIND_PAIR
from base.draft_phase_model import DraftPhaseModel

PHASES = ("early_nw", "late", "all", "early_win")
POSITION_KIND = "hero_role_position_pair"


def log(message):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def atomic_json(value, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w") as f:
        json.dump(value, f, indent=2, ensure_ascii=False, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_joblib(value, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    joblib.dump(value, tmp, compress=3)
    os.replace(tmp, path)


def atomic_predictions(path, rows, key, probabilities):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as f:
        np.savez_compressed(f, mid=rows["mid"], ts=rows["ts"],
                            y=rows[key], probabilities=probabilities)
    os.replace(tmp, path)


def phase_rows(rows, phase):
    duration = rows["duration"]
    keep = duration >= 1200
    if phase == "late":
        keep &= duration >= 2160
    elif phase == "early_win":
        keep &= duration <= 2040
    elif phase == "early_nw":
        keep &= rows["early_nw"] >= 0
    elif phase != "all":
        raise ValueError(f"unknown phase {phase}")
    key = "early_nw" if phase == "early_nw" else "wins"
    keep &= rows[key] >= 0
    return {k: rows[k][keep] for k in ("heroes", "mid", "ts", "duration", key)}, key


def chronological_cuts(ts):
    ts = np.asarray(ts)
    if len(ts) < 10 or np.any(ts[1:] < ts[:-1]):
        raise ValueError("at least ten chronologically sorted rows required")
    a = int(np.searchsorted(ts, ts[len(ts)*60//100], side="left"))
    b = int(np.searchsorted(ts, ts[len(ts)*80//100], side="left"))
    if not 0 < a < b < len(ts):
        raise ValueError("timestamp ties leave an empty chronological split")
    return a, b


def csr_view(x, start, stop):
    """A contiguous row view without copying gigabytes of sparse values."""
    lo, hi = int(x.indptr[start]), int(x.indptr[stop])
    return sparse.csr_matrix((x.data[lo:hi], x.indices[lo:hi],
                              x.indptr[start:stop+1] - lo),
                             shape=(stop-start, x.shape[1]), copy=False)


def disk_design(encoder, heroes, scratch, chunk_size=25000):
    """Write compact CSR blocks once; never hold the whole matrix on the heap.

    Scratch is deliberately rebuilt, not reused on file existence. Each new
    generation replaces the previous disposable matrix only after all writes.
    """
    scratch = Path(scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    paths = [scratch / (name + ".bin") for name in ("values", "indices", "indptr")]
    tmps = [p.with_name(p.name + ".tmp") for p in paths]
    nnz = 0
    with tmps[0].open("wb") as data, tmps[1].open("wb") as indices, tmps[2].open("wb") as indptr:
        np.asarray([0], dtype=np.int32).tofile(indptr)
        for start in range(0, len(heroes), chunk_size):
            block = encoder.transform(heroes[start:start+chunk_size])
            block.eliminate_zeros()
            if nnz + block.nnz >= np.iinfo(np.int32).max:
                raise ValueError("CSR exceeds int32 capacity; reduce corpus or use int64")
            block.data.astype(np.float64, copy=False).tofile(data)
            block.indices.astype(np.int32, copy=False).tofile(indices)
            (block.indptr[1:].astype(np.int64) + nnz).astype(np.int32).tofile(indptr)
            nnz += block.nnz
            if start == 0 or (start // chunk_size) % 20 == 0:
                log(f"design rows={min(start+chunk_size,len(heroes)):,}/{len(heroes):,} nnz={nnz:,}")
        for f in (data, indices, indptr):
            f.flush()
            os.fsync(f.fileno())
    for tmp, dest in zip(tmps, paths):
        os.replace(tmp, dest)
    values = np.memmap(paths[0], dtype=np.float64, mode="r", shape=(nnz,))
    indices = np.memmap(paths[1], dtype=np.int32, mode="r", shape=(nnz,))
    indptr = np.memmap(paths[2], dtype=np.int32, mode="r", shape=(len(heroes)+1,))
    return sparse.csr_matrix((values, indices, indptr),
                             shape=(len(heroes), encoder.n_columns), copy=False)


def fit_logistic(x, y, c, max_iter, label):
    if set(np.unique(y)) != {0, 1}:
        raise ValueError(f"{label}: both binary classes required")
    log(f"FIT {label} rows={len(y):,} columns={x.shape[1]:,} C={c}")
    start = time.monotonic()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ConvergenceWarning)
        model = LogisticRegression(C=c, solver="lbfgs", max_iter=max_iter,
                                   tol=1e-5, random_state=20260905).fit(x, y)
    if any(isinstance(w.message, ConvergenceWarning) for w in caught):
        raise RuntimeError(f"{label}: optimizer did not converge; increase --max-iter")
    if not np.isfinite(model.coef_).all():
        raise ValueError(f"{label}: non-finite coefficients")
    log(f"FIT_DONE {label} seconds={time.monotonic()-start:.1f} iterations={model.n_iter_.tolist()}")
    return model


def train_component(heroes, y, a, b, kind, signed, args, label):
    """Vocabulary freezes on train for C selection AND honest refit."""
    enc = DraftFeatureEncoder.fit(heroes[:a], kind, signed, args.pair_min_support)
    log(f"ENCODER {label} columns={enc.n_columns:,}")
    x = disk_design(enc, heroes, args.scratch)
    grid = list(args.c_grid)
    trials = []
    best, p_validation = None, None
    for c in grid:
        model = fit_logistic(csr_view(x, 0, a), y[:a], c, args.max_iter, label+"/selection")
        pv = model.predict_proba(csr_view(x, a, b))[:, 1]
        loss = float(log_loss(y[a:b], pv, labels=[0, 1]))
        trials.append({"C": c, "validation_log_loss": loss})
        if best is None or (loss, c) < (best["validation_log_loss"], best["C"]):
            best, p_validation = trials[-1], pv
        log(f"VALIDATION {label} C={c} log_loss={loss:.8f}")
    # A boundary is a recorded limitation, not permission to tune on the test.
    honest = fit_logistic(csr_view(x, 0, b), y[:b], best["C"], args.max_iter, label+"/honest")
    p_test = honest.predict_proba(csr_view(x, b, len(y)))[:, 1]
    del x, model
    gc.collect()
    return enc, honest, p_validation, p_test, {
        "selected_C": best["C"], "trials": trials,
        "grid_boundary": best["C"] in (min(grid), max(grid)),
        "vocabulary_fit_rows": a, "honest_fit_rows": b,
    }


def refit_component(heroes, y, kind, signed, c, args, label):
    enc = DraftFeatureEncoder.fit(heroes, kind, signed, args.pair_min_support)
    x = disk_design(enc, heroes, args.scratch)
    model = fit_logistic(x, y, c, args.max_iter, label+"/full")
    del x
    gc.collect()
    return enc, model


def probability_metrics(y, p):
    if len(y) == 0:
        return {"rows": 0, "reason": "empty_cohort"}
    p = np.asarray(p)
    classes = p.shape[1]
    onehot = np.eye(classes)[y]
    pred = p.argmax(axis=1)
    confidence = p.max(axis=1)
    result = {
        "rows": len(y), "class_counts": np.bincount(y, minlength=classes).tolist(),
        "log_loss": float(log_loss(y, p, labels=list(range(classes)))),
        "brier_multiclass_sum": float(np.square(onehot-p).sum(axis=1).mean()),
        "accuracy": float((pred == y).mean()),
        "baseline_accuracy": float(np.bincount(y, minlength=classes).max()/len(y)),
    }
    if classes == 2:
        result["brier"] = float(np.square(y-p[:, 1]).mean())
        result["auc"] = float(roc_auc_score(y, p[:, 1])) if len(np.unique(y)) == 2 else None
    else:
        hit = y != 2
        result["marker_occurrence_auc"] = (float(roc_auc_score(hit, 1-p[:, 2]))
                                             if len(np.unique(hit)) == 2 else None)
        result["conditional_direction"] = probability_metrics(
            y[hit], p[hit, :2]/np.maximum(p[hit, :2].sum(axis=1, keepdims=True), 1e-15))
    order = np.argsort(-confidence, kind="stable")
    result["equal_coverage"] = {}
    for share in (.1, .2, .3, .5):
        idx = order[:max(1, int(len(y)*share))]
        result["equal_coverage"][str(share)] = {
            "rows": len(idx), "accuracy": float((pred[idx] == y[idx]).mean()),
            "mean_confidence": float(confidence[idx].mean()),
        }
    result["confidence_bins"] = []
    for lo, hi in zip(np.arange(.3, 1., .1), np.arange(.4, 1.1, .1)):
        mask = (confidence >= lo) & (confidence < hi if hi < .999 else confidence <= 1)
        if mask.any():
            result["confidence_bins"].append({"from": round(float(lo), 2), "to": round(float(hi), 2),
                "rows": int(mask.sum()), "confidence": float(confidence[mask].mean()),
                "accuracy": float((pred[mask] == y[mask]).mean())})
    return result


def load_rows(path):
    path = Path(path)
    if path.is_dir():
        path = path / "rows.npz"
    manifest = path.parent / "manifest.json"
    if manifest.exists() and not json.loads(manifest.read_text()).get("complete"):
        raise ValueError(f"incomplete corpus build: {manifest}")
    with np.load(path, allow_pickle=False) as z:
        rows = {k: z[k] for k in ("heroes", "mid", "ts", "duration", "wins", "early_nw")}
    if len(np.unique(rows["mid"])) != len(rows["mid"]):
        raise ValueError("corpus contains duplicate match IDs")
    if np.any(rows["ts"][1:] < rows["ts"][:-1]):
        raise ValueError("corpus must be chronologically sorted")
    n = len(rows["mid"])
    if rows["heroes"].shape != (n, 10) or any(len(v) != n for v in rows.values()):
        raise ValueError("inconsistent corpus shapes")
    if not np.isin(rows["wins"], [-1, 0, 1]).all() or not np.isin(rows["early_nw"], [-1, 0, 1, 2]).all():
        raise ValueError("invalid corpus classes")
    return rows


def train_variant(rows, key, phase, kind, args, pro_rows):
    heroes, y, ts = rows["heroes"], rows[key], rows["ts"]
    a, b = chronological_cuts(ts)
    out = args.output_dir / phase / kind
    out.mkdir(parents=True, exist_ok=True)
    if (out / "results.json").exists():
        raise FileExistsError(f"refusing to overwrite finished training {out}")
    if phase == "early_nw":
        marker = y != 2
        ma, mb = int(marker[:a].sum()), int(marker[:b].sum())
        occurrence = train_component(heroes, marker.astype(np.int8), a, b, kind, False, args, phase+"/occurrence/"+kind)
        direction = train_component(heroes[marker], y[marker], ma, mb, kind, True, args, phase+"/direction/"+kind)
        # Direction predictions are needed for every held-out row, including no-marker rows.
        enc, model, _, _, dmeta = direction
        oenc, omodel, qv, qt, ometa = occurrence
        bundle = DraftPhaseModel(phase, enc, model, oenc, omodel)
        p_test = bundle.predict_proba(heroes[b:])
        # Joint validation CE decomposes exactly into occurrence CE + marker-only direction CE.
        val_loss = (float(log_loss(marker[a:b], qv, labels=[0, 1])) +
                    float(marker[a:b].mean()) * float(log_loss(y[a:b][marker[a:b]], direction[2], labels=[0, 1])))
        selection = {"occurrence": ometa, "direction": dmeta, "validation_log_loss": val_loss}
    else:
        enc, model, pv, pt, selection = train_component(heroes, y, a, b, kind, True, args, phase+"/"+kind)
        bundle = DraftPhaseModel(phase, enc, model)
        p_test = np.column_stack((1-pt, pt))
        selection["validation_log_loss"] = float(log_loss(y[a:b], pv, labels=[0, 1]))
    atomic_joblib(bundle, out / "evaluation_model.joblib")
    atomic_predictions(out / "evaluation_predictions.npz",
                       {k: v[b:] for k, v in rows.items()}, key, p_test)
    report = {
        "schema_version": 1, "phase": phase, "design": kind,
        "training_identity": args.training_identity,
        "classes": bundle.classes_.tolist(), "source": str(args.corpus),
        "pair_min_support": args.pair_min_support, "uses_ingame_features": False,
        "population": {"minimum_duration_seconds": 2160 if phase == "late" else 1200,
                       "maximum_duration_seconds": 2040 if phase == "early_win" else None,
                       "early_lead_window": [20, 28] if phase == "early_nw" else None},
        "counts": {"all": len(y), "train": a, "validation": b-a, "test": len(y)-b},
        "split": {"first_ts": int(ts[0]), "last_ts": int(ts[-1]),
                  "train_last_ts": int(ts[a-1]), "validation_first_ts": int(ts[a]),
                  "evaluation_fit_end_ts": int(ts[b-1]), "test_first_ts": int(ts[b])},
        "selection": selection, "honest_test": probability_metrics(y[b:], p_test),
        "evaluation_artifact": {"file": "evaluation_model.joblib", "held_out_test": True},
    }
    if pro_rows is not None:
        pro, pro_key = phase_rows(pro_rows, phase)
        fresh = pro["ts"] > ts[b-1]
        report["pro_forward_evaluation"] = probability_metrics(
            pro[pro_key][fresh], bundle.predict_proba(pro["heroes"][fresh]))
        report["pro_forward_evaluation"]["after_ts"] = int(ts[b-1])
    atomic_json(report, out / "evaluation_results.json")
    log(f"EVALUATED {phase}/{kind} {json.dumps(report['honest_test'], ensure_ascii=False)}")
    # The old role-free design is a control, not a silently promoted fallback.
    if kind == POSITION_KIND and not args.no_full_refit:
        if phase == "early_nw":
            full_oenc, full_om = refit_component(heroes, marker.astype(np.int8), kind, False,
                                                ometa["selected_C"], args, phase+"/occurrence")
            full_enc, full_m = refit_component(heroes[marker], y[marker], kind, True,
                                              dmeta["selected_C"], args, phase+"/direction")
            final = DraftPhaseModel(phase, full_enc, full_m, full_oenc, full_om)
        else:
            full_enc, full_m = refit_component(heroes, y, kind, True,
                                              selection["selected_C"], args, phase)
            final = DraftPhaseModel(phase, full_enc, full_m)
        atomic_joblib(final, out / "model.joblib")
        reloaded = joblib.load(out / "model.joblib")
        probe = heroes[np.linspace(0, len(heroes)-1, min(200, len(heroes)), dtype=int)]
        delta = float(np.max(np.abs(final.predict_proba(probe)-reloaded.predict_proba(probe))))
        if delta > 1e-12:
            raise ValueError("artifact reload changed probabilities")
        report["final_artifact"] = {"file": "model.joblib", "held_out_test": False,
                                     "fit_rows": len(y), "fit_end_ts": int(ts[-1]),
                                     "reload_max_delta": delta}
        if pro_rows is not None:
            fresh = pro["ts"] > ts[-1]
            report["pro_after_final_fit"] = probability_metrics(
                pro[pro_key][fresh], final.predict_proba(pro["heroes"][fresh]))
    atomic_json(report, out / "results.json")
    return report


def main(argv=None, default_models=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, help="canonical corpus; if omitted, rebuild from --source")
    parser.add_argument("--source", type=Path, default=ROOT/"bets_data/analise_pub_matches/json_parts_split_from_object")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--pro-corpus", type=Path)
    parser.add_argument("--output-dir", "--out", type=Path,
                        default=ROOT/"data/draft_phase_models"/time.strftime("%Y-%m-%d_%H%M%S"))
    parser.add_argument("--scratch", type=Path, default=ROOT/"runtime/artifacts/draft-cp/phase_training_scratch")
    parser.add_argument("--models", nargs="+", choices=PHASES, default=default_models or list(PHASES))
    parser.add_argument("--c-grid", nargs="+", type=float, default=[0.001, 0.003, 0.01, 0.03])
    parser.add_argument("--pair-min-support", type=int, default=30)
    parser.add_argument("--max-iter", type=int, default=1000)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--skip-baseline", action="store_true")
    parser.add_argument("--no-full-refit", action="store_true")
    parser.add_argument("--resume", action="store_true", help="reuse only fully finished, identical training runs")
    args = parser.parse_args(argv)
    if not args.c_grid or min(args.c_grid) <= 0 or args.pair_min_support < 1 or args.threads < 1:
        parser.error("C, support and threads must be positive")
    if args.corpus is None:
        from base.build_draft_phase_corpus import build_corpus
        args.corpus = ROOT/"data/draft_phase_corpus/public"
        build_corpus(args.source, args.corpus, args.workers)
    source_path = args.corpus/"rows.npz" if args.corpus.is_dir() else args.corpus
    digest = hashlib.sha256()
    with source_path.open("rb") as f:
        for part in iter(lambda: f.read(8*1024*1024), b""):
            digest.update(part)
    code_sha = {p: hashlib.sha256((ROOT/"base"/p).read_bytes()).hexdigest()
                for p in ("train_draft_phase_models.py", "draft_features.py", "draft_phase_model.py")}
    args.training_identity = {"corpus_sha256": digest.hexdigest(), "code_sha256": code_sha,
        "C_grid": args.c_grid, "support": args.pair_min_support, "full_refit": not args.no_full_refit}
    rows = load_rows(args.corpus)
    pro = load_rows(args.pro_corpus) if args.pro_corpus else None
    if pro is not None:
        overlap = np.isin(pro["mid"], rows["mid"])
        log(f"public/pro overlap excluded from pro evaluation: {int(overlap.sum()):,}")
        pro = {k: v[~overlap] for k, v in pro.items()}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary = {"status": "running", "started_at": time.time(), "models": {},
               "production_changed": False, "corpus": str(args.corpus)}
    atomic_json(summary, args.output_dir/"summary.json")
    with threadpool_limits(limits=args.threads):
        for phase in args.models:
            subset, key = phase_rows(rows, phase)
            variants = [POSITION_KIND] if args.skip_baseline else [KIND_PAIR, POSITION_KIND]
            summary["models"][phase] = {}
            for kind in variants:
                previous = args.output_dir/phase/kind/"results.json"
                if args.resume and previous.exists():
                    report = json.loads(previous.read_text())
                    if report.get("training_identity") != args.training_identity:
                        raise ValueError(f"resume identity mismatch: {previous}")
                    files = [report["evaluation_artifact"]["file"]]
                    if "final_artifact" in report:
                        files.append(report["final_artifact"]["file"])
                    for file in files:
                        joblib.load(previous.parent/file)
                    log(f"RESUME completed {phase}/{kind}")
                else:
                    report = train_variant(subset, key, phase, kind, args, pro)
                summary["models"][phase][kind] = report
                atomic_json(summary, args.output_dir/"summary.json")
                gc.collect()
            if len(variants) == 2:
                winner = min(variants, key=lambda k: summary["models"][phase][k]["selection"]["validation_log_loss"])
                summary["models"][phase]["validation_preferred_design"] = winner
            del subset
            gc.collect()
    summary.update(status="complete", finished_at=time.time())
    atomic_json(summary, args.output_dir/"summary.json")
    log("DONE all requested phase models trained and verified")


if __name__ == "__main__":
    main()
