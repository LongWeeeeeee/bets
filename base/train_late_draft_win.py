#!/usr/bin/env python3
"""Обучение боевой late-модели победы: только драфт, только карты >= 36 минут.

Тот же leakage-safe контракт, что у `train_public_draft_hero10_experiment`:
только hero ID по позициям, дизайн `hero_role_pair` signed, хронологический
сплит 60/20/20 для выбора C и честных метрик, финальный артефакт — на ВСЕЙ
late-популяции.

Порог 36 минут = `LATE_MIN_DURATION` из `base/analise_database.py`, то есть тот
же фильтр, по которому собирается late-словарь.

Обоснование и замеры — E-240. Коротко: общая модель на этой популяции даёт AUC
0.6003 и завышает винрейт на 3.7-5.3 п.п., late-модель — 0.6211 и тот же
фактический винрейт при втрое большем потоке ставок.

Запуск:
  venv_catboost/bin/python3 base/train_late_draft_win.py
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from base.draft_features import KIND_PAIR, DraftFeatureEncoder  # noqa: E402
from base.train_public_draft_hero10_experiment import atomic_joblib_dump  # noqa: E402

SHARDS = ROOT / "runtime/artifacts/kills/window_model_v2/rows_public.shard*.npz"
DEFAULT_OUT = ROOT / "data/late_draft_win/2026-08-29_public_5m_late36"
LATE_MIN_MINUTES = 36
C_GRID = (0.003, 0.01, 0.03, 0.1)
PAIR_MIN_SUPPORT = 30
SEED = 20260829


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


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


def load_late(min_minutes: int):
    """Паблик-корпус, отсортированный по времени, отфильтрованный по длительности."""
    ts, he, du, wi = [], [], [], []
    for path in sorted(glob.glob(str(SHARDS))):
        z = np.load(path)
        ts.append(z["ts"]); he.append(z["heroes"]); du.append(z["duration"]); wi.append(z["wins"])
    ts = np.concatenate(ts)
    he = np.concatenate(he).astype(np.int64)
    du = np.concatenate(du).astype(np.int64)
    wi = np.concatenate(wi).astype(np.int64)
    keep = (du > 0) & (du >= min_minutes * 60)
    ts, he, wi = ts[keep], he[keep], wi[keep]
    order = np.argsort(ts, kind="mergesort")
    return ts[order], he[order], wi[order]


def chrono_idx(n: int):
    a, b = n * 60 // 100, n * 80 // 100
    idx = np.arange(n)
    return idx[:a], idx[a:b], idx[b:]


def metrics(y, p) -> dict:
    return {"rows": int(len(y)),
            "positive_rate": round(float(y.mean()), 6),
            "auc": round(float(roc_auc_score(y, p)), 6),
            "log_loss": round(float(log_loss(y, p)), 6),
            "brier": round(float(brier_score_loss(y, p)), 6),
            "accuracy": round(float(((p >= 0.5) == (y == 1)).mean()), 6)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--min-minutes", type=int, default=LATE_MIN_MINUTES)
    args = ap.parse_args()
    out_dir = Path(args.out)
    t0 = time.time()

    ts, heroes, wins = load_late(args.min_minutes)
    n = len(wins)
    tr, va, te = chrono_idx(n)
    log(f"late-корпус >= {args.min_minutes} мин: {n:,} карт; "
        f"train {len(tr):,} / val {len(va):,} / test {len(te):,}; база WR {wins.mean():.4f}")

    # --- выбор C на валидации, энкодер по обучающей части ---
    sel_enc = DraftFeatureEncoder.fit(heroes[tr], KIND_PAIR, signed=True, pair_min_support=PAIR_MIN_SUPPORT)
    x_tr, x_va = sel_enc.transform(heroes[tr]), sel_enc.transform(heroes[va])
    best_c, best_ll = None, None
    for c in C_GRID:
        m = LogisticRegression(C=c, max_iter=1000, random_state=SEED).fit(x_tr, wins[tr])
        ll = log_loss(wins[va], m.predict_proba(x_va)[:, 1])
        log(f"  C={c}: val_log_loss={ll:.6f}")
        if best_ll is None or ll < best_ll:
            best_c, best_ll = c, ll
    log(f"выбран C={best_c} (val_log_loss {best_ll:.6f}); колонок {sel_enc.n_columns:,}")

    # --- честные метрики: модель на train+val, проверка на отложенном тесте ---
    fit_idx = np.concatenate([tr, va])
    honest_enc = DraftFeatureEncoder.fit(heroes[fit_idx], KIND_PAIR, signed=True,
                                         pair_min_support=PAIR_MIN_SUPPORT)
    honest = LogisticRegression(C=best_c, max_iter=1000, random_state=SEED).fit(
        honest_enc.transform(heroes[fit_idx]), wins[fit_idx])
    p_te = honest.predict_proba(honest_enc.transform(heroes[te]))[:, 1]
    honest_metrics = metrics(wins[te], p_te)
    log(f"честная проверка на отложенных {len(te):,} картах: {honest_metrics}")

    # --- боевой артефакт: фит на ВСЕЙ late-популяции ---
    log(f"боевой фит на всех {n:,} late-картах")
    ship_enc = DraftFeatureEncoder.fit(heroes, KIND_PAIR, signed=True, pair_min_support=PAIR_MIN_SUPPORT)
    ship = LogisticRegression(C=best_c, max_iter=1000, random_state=SEED).fit(
        ship_enc.transform(heroes), wins)
    log(f"боевой энкодер: {ship_enc.n_columns:,} колонок")

    out_dir.mkdir(parents=True, exist_ok=True)
    atomic_joblib_dump(ship, out_dir / "late_win_model.joblib")
    atomic_joblib_dump(ship_enc, out_dir / "late_win_feature_encoder.joblib")
    _atomic_json({
        "experiment": "late_draft_win",
        "version": out_dir.name,
        "journal": "E-240",
        "late_min_minutes": args.min_minutes,
        "late_min_seconds": args.min_minutes * 60,
        "design": {"kind": KIND_PAIR, "signed": True, "columns": int(ship_enc.n_columns),
                   "pair_min_support": PAIR_MIN_SUPPORT},
        "C": best_c,
        "selection_validation_log_loss": round(float(best_ll), 6),
        "counts": {"late_all": int(n), "train": int(len(tr)), "validation": int(len(va)),
                   "test": int(len(te)), "shipped_fit_rows": int(n)},
        "base_wr_radiant": round(float(wins.mean()), 6),
        "held_out_test": True,
        "honest_test": honest_metrics,
        "split_boundaries": {"train_last_ts": int(ts[tr[-1]]),
                             "validation_first_ts": int(ts[va[0]]),
                             "test_first_ts": int(ts[te[0]])},
        "uses_ingame_or_third_party_stats": False,
        "note": "Только драфт: hero_id по позициям. Живого состояния и прочих признаков нет.",
    }, out_dir / "results.json")
    log(f"ГОТОВО за {time.time() - t0:.1f} c -> {out_dir}")


if __name__ == "__main__":
    main()
