#!/usr/bin/env python3
"""Обучение боевой early-NW модели: только драфт, популяция и метка словаря early_dict.

Тот же leakage-safe контракт, что у `train_late_draft_win`: только hero ID по
позициям, дизайн `hero_role_pair` signed, хронологический сплит 60/20/20 для
выбора C и честных метрик, финальный артефакт — на ВСЕЙ популяции.

ОТЛИЧИЕ ОТ LATE-МОДЕЛИ — цель. Там победа карты, здесь СТОРОНА РАННЕГО МАРКЕРА
по нетворту: кто первым в окне 20-28 минут дотянул перевес до растущего порога.
Популяция и метка построены продовой функцией `is_early_nw_match`
(`base/analise_database.py:999`), а не копией правил, поэтому модель отбирает
ровно те карты, что и словарь `early_dict`.

Датасет собирает `runtime/experiments/misc/build_early_nw_public_dataset.py`.

Обоснование цели и замеры на про-корпусе — E-242. Коротко: на предматчевых
признаках «кто возьмёт ранний перевес» и «кто выиграет карту» оказались одной
величиной, и там отдельная модель ничего не добавила. Здесь проверяется другой
дизайн — паблик и только драфт, как у late-модели, где отдельная модель как раз
дала прибавку (E-240: 0.6211 против 0.6003).

Запуск:
  venv_catboost/bin/python3 base/train_early_nw_draft_win.py
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

SHARDS = ROOT / "runtime/artifacts/misc/early_nw_public/rows.shard_*.npz"
DEFAULT_OUT = ROOT / "data/early_nw_draft/2026-09-01_public_early_nw"
C_GRID = (0.003, 0.01, 0.03, 0.1)
PAIR_MIN_SUPPORT = 30
SEED = 20260901


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def _atomic_json(payload: Any, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp",
                                    dir=destination.parent)
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


def load_rows():
    """Датасет early-NW, отсортированный по времени."""
    paths = sorted(glob.glob(str(SHARDS)))
    if not paths:
        raise SystemExit(f"нет shard-ов датасета: {SHARDS} — сперва собери его "
                         "через runtime/experiments/misc/build_early_nw_public_dataset.py")
    ts, he, du, yy = [], [], [], []
    for path in paths:
        z = np.load(path)
        if z["y"].shape[0] == 0:
            continue
        ts.append(z["ts"]); he.append(z["heroes"]); du.append(z["duration"]); yy.append(z["y"])
    ts = np.concatenate(ts)
    he = np.concatenate(he).astype(np.int64)
    du = np.concatenate(du).astype(np.int64)
    yy = np.concatenate(yy).astype(np.int64)
    order = np.argsort(ts, kind="mergesort")
    return ts[order], he[order], du[order], yy[order], len(paths)


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
    args = ap.parse_args()
    out_dir = Path(args.out)
    t0 = time.time()

    ts, heroes, durations, y, n_shards = load_rows()
    n = len(y)
    tr, va, te = chrono_idx(n)
    log(f"early-NW популяция: {n:,} карт из {n_shards} shard-ов; "
        f"train {len(tr):,} / val {len(va):,} / test {len(te):,}; "
        f"доля радианта {y.mean():.4f}")

    # --- выбор C на валидации, энкодер по обучающей части ---
    sel_enc = DraftFeatureEncoder.fit(heroes[tr], KIND_PAIR, signed=True,
                                      pair_min_support=PAIR_MIN_SUPPORT)
    x_tr, x_va = sel_enc.transform(heroes[tr]), sel_enc.transform(heroes[va])
    best_c, best_ll = None, None
    for c in C_GRID:
        m = LogisticRegression(C=c, max_iter=1000, random_state=SEED).fit(x_tr, y[tr])
        ll = log_loss(y[va], m.predict_proba(x_va)[:, 1])
        log(f"  C={c}: val_log_loss={ll:.6f}")
        if best_ll is None or ll < best_ll:
            best_c, best_ll = c, ll
    log(f"выбран C={best_c} (val_log_loss {best_ll:.6f}); колонок {sel_enc.n_columns:,}")

    # --- честные метрики: модель на train+val, проверка на отложенном тесте.
    # Боевой артефакт ниже фитится на ВСЁМ корпусе (запрос alex: без test split);
    # этот прогон нужен только чтобы знать цену строки в панели.
    fit_idx = np.concatenate([tr, va])
    honest_enc = DraftFeatureEncoder.fit(heroes[fit_idx], KIND_PAIR, signed=True,
                                         pair_min_support=PAIR_MIN_SUPPORT)
    honest = LogisticRegression(C=best_c, max_iter=1000, random_state=SEED).fit(
        honest_enc.transform(heroes[fit_idx]), y[fit_idx])
    p_te = honest.predict_proba(honest_enc.transform(heroes[te]))[:, 1]
    honest_metrics = metrics(y[te], p_te)
    log(f"честная проверка на отложенных {len(te):,} картах: {honest_metrics}")

    # Сравнение с базой «всегда радиант» — без неё AUC не с чем соотнести.
    base_acc = float(max(y[te].mean(), 1.0 - y[te].mean()))
    log(f"база «всегда одна сторона» на тесте: {base_acc:.6f}")

    # --- боевой артефакт: фит на ВСЕЙ популяции, без отложенного теста ---
    log(f"боевой фит на всех {n:,} картах")
    ship_enc = DraftFeatureEncoder.fit(heroes, KIND_PAIR, signed=True,
                                       pair_min_support=PAIR_MIN_SUPPORT)
    ship = LogisticRegression(C=best_c, max_iter=1000, random_state=SEED).fit(
        ship_enc.transform(heroes), y)
    log(f"боевой энкодер: {ship_enc.n_columns:,} колонок")

    out_dir.mkdir(parents=True, exist_ok=True)
    atomic_joblib_dump(ship, out_dir / "early_nw_model.joblib")
    atomic_joblib_dump(ship_enc, out_dir / "early_nw_feature_encoder.joblib")
    _atomic_json({
        "experiment": "early_nw_draft",
        "version": out_dir.name,
        "journal": "E-243",
        "target": "сторона раннего маркера по нетворту (доминатор), НЕ победа карты",
        "population": "продовая is_early_nw_match: ряд >= 20 мин, маркер в окне 20-28",
        "design": {"kind": KIND_PAIR, "signed": True, "columns": int(ship_enc.n_columns),
                   "pair_min_support": PAIR_MIN_SUPPORT},
        "C": best_c,
        "selection_validation_log_loss": round(float(best_ll), 6),
        "counts": {"population": int(n), "train": int(len(tr)), "validation": int(len(va)),
                   "test": int(len(te)), "shipped_fit_rows": int(n)},
        "base_radiant_share": round(float(y.mean()), 6),
        "held_out_test": False,
        "shipped_on_full_corpus": True,
        "honest_test": honest_metrics,
        "honest_test_baseline_accuracy": round(base_acc, 6),
        "split_boundaries": {"train_last_ts": int(ts[tr[-1]]),
                             "validation_first_ts": int(ts[va[0]]),
                             "test_first_ts": int(ts[te[0]])},
        "uses_ingame_or_third_party_stats": False,
        "note": "Только драфт: hero_id по позициям, порядок win_model_veto._heroes_vector.",
    }, out_dir / "results.json")
    log(f"ГОТОВО за {time.time() - t0:.1f} c -> {out_dir}")


if __name__ == "__main__":
    main()
