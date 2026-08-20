#!/usr/bin/env python3
"""Обучение P(карта ≥ 43 мин) по десяти героям на паблике.

Тот же leakage-safe контракт, что у `train_public_draft_hero10_experiment`:
только hero ID по позициям, unsigned `hero_role`, хронологический 60/20/20.
Данные — готовые шарды `rows_public.shard*.npz` (длительность в секундах).
Дополнительно пишет перенос на про-тест (compact∩rich, с TEST_FROM).

Запуск:
  venv_catboost/bin/python3 base/train_duration_over43.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from base.draft_features import KIND_ROLE, DraftFeatureEncoder  # noqa: E402
from base.duration_over43 import THRESHOLD_SECONDS  # noqa: E402
from base.train_public_draft_hero10_experiment import (  # noqa: E402
    atomic_joblib_dump,
    classification_metrics,
)

KILLS = ROOT / "runtime/artifacts/kills/window_model_v2"
COMPACT = ROOT / "runtime/artifacts/misc/pro_corpus_compact.npz"
RICH = ROOT / "runtime/artifacts/misc/pro_corpus_rich.npz"
G29 = ROOT / "runtime/artifacts/misc/undercount_retry_cache.npz"
TEST_FROM = 1774742400
C_GRID = (0.03, 0.1, 0.3, 1.0)
SEED = 20260814
DEFAULT_OUT = ROOT / "data/duration_over43"


def _atomic_json(payload: Any, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, destination)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def load_public():
    shards = sorted(KILLS.glob("rows_public.shard*.npz"))
    if len(shards) < 1:
        raise FileNotFoundError(f"no public shards in {KILLS}")
    ts = np.concatenate([np.load(p)["ts"] for p in shards])
    heroes = np.concatenate([np.load(p)["heroes"] for p in shards])
    duration = np.concatenate([np.load(p)["duration"] for p in shards])
    if not np.all(ts[1:] >= ts[:-1]):
        order = np.argsort(ts, kind="mergesort")
        ts, heroes, duration = ts[order], heroes[order], duration[order]
    ok = duration > 0
    return ts[ok], heroes[ok].astype(np.int64), duration[ok].astype(np.int64)


def chrono_idx(n: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    a, b = n * 60 // 100, n * 80 // 100
    idx = np.arange(n)
    return idx[:a], idx[a:b], idx[b:]


def fit_logistic(x, y, c: float) -> LogisticRegression:
    return LogisticRegression(
        C=c, max_iter=1000, solver="liblinear", random_state=SEED,
    ).fit(x, y)


def select_c(xtr, ytr, xval, yval) -> tuple[float, float]:
    best_c, best_loss = C_GRID[0], 1e9
    for c in C_GRID:
        model = fit_logistic(xtr, ytr, c)
        p = np.clip(model.predict_proba(xval)[:, 1], 1e-6, 1 - 1e-6)
        loss = float(log_loss(yval, p, labels=[0, 1]))
        print(f"  C={c} val_logloss={loss:.5f} val_auc={roc_auc_score(yval, p):.4f}", flush=True)
        if loss < best_loss:
            best_c, best_loss = float(c), loss
    return best_c, best_loss


def load_pro():
    zc, zr = np.load(COMPACT), np.load(RICH)
    pos = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    keep = np.array([int(m) in pos for m in zc["mids"].tolist()])
    idx = np.array([pos[int(m)] for m in zc["mids"][keep].tolist()])
    ts = zc["ts"][keep]
    heroes = zc["heroes"][keep].astype(np.int64)
    duration = zr["durations"][idx].astype(np.int64)
    ok = duration > 0
    return ts[ok], heroes[ok], duration[ok]


def maybe_pro29(y_pub_metrics: dict) -> dict[str, Any] | None:
    if not (G29.exists() and COMPACT.exists() and RICH.exists()):
        return None
    zc, zr = np.load(COMPACT), np.load(RICH)
    pos = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    keep = np.array([int(m) in pos for m in zc["mids"].tolist()])
    idx = np.array([pos[int(m)] for m in zc["mids"][keep].tolist()])
    ts = zc["ts"][keep]
    duration = zr["durations"][idx].astype(np.int64)
    G = np.load(G29)["G"]
    if len(G) != int(keep.sum()):
        return {"error": f"G29 n={len(G)} keep={int(keep.sum())}"}
    ok = duration > 0
    ts, duration, G = ts[ok], duration[ok], G[ok]
    y = (duration >= THRESHOLD_SECONDS).astype(np.int32)
    train, test = ts < TEST_FROM, ts >= TEST_FROM
    mu, sd = G[train].mean(0), G[train].std(0) + 1e-9
    m = LogisticRegression(C=1.0, max_iter=5000, solver="lbfgs", random_state=SEED)
    m.fit((G[train] - mu) / sd, y[train])
    p = m.predict_proba((G[test] - mu) / sd)[:, 1]
    return {
        "note": "logistic on combat 29, not shipped (needs ratings, not draft-only)",
        "n_train": int(train.sum()),
        "n_test": int(test.sum()),
        **classification_metrics(y[test], p),
        "public_draft_for_compare": y_pub_metrics,
    }


def train(output_dir: Path) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    print("load public shards", flush=True)
    ts, heroes, duration = load_public()
    y = (duration >= THRESHOLD_SECONDS).astype(np.int32)
    n = len(ts)
    tr, va, te = chrono_idx(n)
    print(
        f"public n={n:,} over43={float(y.mean()):.4f} "
        f"train={len(tr):,} val={len(va):,} test={len(te):,}",
        flush=True,
    )
    print("fit encoder on train", flush=True)
    enc_sel = DraftFeatureEncoder.fit(heroes[tr], KIND_ROLE, signed=False)
    print(f"  columns={enc_sel.n_columns}", flush=True)
    xtr = enc_sel.transform(heroes[tr])
    xva = enc_sel.transform(heroes[va])
    xte = enc_sel.transform(heroes[te])
    print("select C on validation", flush=True)
    best_c, val_loss = select_c(xtr, y[tr], xva, y[va])
    print(f"selected C={best_c} val_logloss={val_loss:.5f}", flush=True)
    sel_model = fit_logistic(xtr, y[tr], best_c)
    evaluation = classification_metrics(y[te], sel_model.predict_proba(xte)[:, 1])
    print(f"evaluation (train-only) auc={evaluation['auc']:.4f} acc={evaluation['accuracy']:.4f}", flush=True)
    del xtr, xva, xte, sel_model, enc_sel

    print("refit encoder+model on train+val", flush=True)
    fit_idx = np.concatenate([tr, va])
    enc = DraftFeatureEncoder.fit(heroes[fit_idx], KIND_ROLE, signed=False)
    xfit = enc.transform(heroes[fit_idx])
    xte = enc.transform(heroes[te])
    model = fit_logistic(xfit, y[fit_idx], best_c)
    shipped = classification_metrics(y[te], model.predict_proba(xte)[:, 1])
    print(f"shipped (train+val) auc={shipped['auc']:.4f} acc={shipped['accuracy']:.4f}", flush=True)

    pro_metrics = None
    try:
        pts, pher, pdur = load_pro()
        py = (pdur >= THRESHOLD_SECONDS).astype(np.int32)
        pte = pts >= TEST_FROM
        if int(pte.sum()) > 100:
            pp = model.predict_proba(enc.transform(pher[pte]))[:, 1]
            pro_metrics = classification_metrics(py[pte], pp)
            pro_metrics["n_all"] = int(len(pts))
            pro_metrics["over43_all"] = float(py.mean())
            print(
                f"pro transfer test n={pro_metrics['rows']:,} over43={pro_metrics['positive_rate']:.4f} "
                f"auc={pro_metrics['auc']:.4f} acc={pro_metrics['accuracy']:.4f}",
                flush=True,
            )
    except Exception as exc:  # noqa: BLE001
        pro_metrics = {"error": f"{type(exc).__name__}: {exc}"}
        print(f"pro transfer skipped: {pro_metrics['error']}", flush=True)

    pro29 = None
    try:
        pro29 = maybe_pro29({"auc": shipped.get("auc")})
        if pro29 and "auc" in pro29:
            print(f"pro29 ADD-style auc={pro29['auc']:.4f} (not shipped)", flush=True)
    except Exception as exc:  # noqa: BLE001
        pro29 = {"error": f"{type(exc).__name__}: {exc}"}

    payload = {
        "threshold_minutes": 43,
        "threshold_seconds": THRESHOLD_SECONDS,
        "design": {"kind": KIND_ROLE, "signed": False, "columns": int(enc.n_columns)},
        "C": best_c,
        "validation_log_loss": val_loss,
        "counts": {
            "all": int(n),
            "train": int(len(tr)),
            "validation": int(len(va)),
            "test": int(len(te)),
            "shipped_fit_rows": int(len(fit_idx)),
        },
        "split": {
            "train_last_ts": int(ts[tr[-1]]),
            "validation_first_ts": int(ts[va[0]]),
            "test_first_ts": int(ts[te[0]]),
        },
        "evaluation": evaluation,
        "shipped": shipped,
        "pro_transfer_test": pro_metrics,
        "pro_29_not_shipped": pro29,
        "uses_ingame_or_third_party_stats": False,
    }
    atomic_joblib_dump(enc, output_dir / "encoder.joblib")
    atomic_joblib_dump(model, output_dir / "model.joblib")
    _atomic_json(payload, output_dir / "results.json")
    print(f"wrote {output_dir}", flush=True)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Train P(duration >= 43 min) from 10 hero IDs")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    train(args.output_dir)


if __name__ == "__main__":
    main()
