#!/usr/bin/env python3
"""Сколько даёт всё вместе: линейная связка, бустинг, Скеллам и прод-словарь.

Каждый оценщик видит одну и ту же карту по-своему: линейная модель — аддитивно,
бустинг — взаимодействиями, Скеллам — через ожидаемые счётчики сторон, словарь —
попарными ячейками «герой против героя на линии». Смесь имеет смысл ровно
настолько, насколько их ошибки различаются.

Честность замера. Веса смеси нельзя подбирать там же, где меряешь: тестовый срез
делится ПО ВРЕМЕНИ пополам, веса учатся на первой половине, число берётся со
второй. Поэтому итоговое n вдвое меньше, чем у отдельных моделей.

Запуск:
    venv_catboost/bin/python3 runtime/experiments/kills/kills_v6_ensemble.py --corpus pro
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime/experiments/kills"))

from window_model_v2 import auc_of  # noqa: E402

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))
ODDS = 1.8


def rank(v: np.ndarray) -> np.ndarray:
    return np.argsort(np.argsort(v)).astype(np.float64) / max(len(v), 1) - 0.5


def collect(corpus: str, target: str) -> tuple[dict[str, np.ndarray], np.ndarray, np.ndarray]:
    """{имя оценщика: оценка} по общим mid, плюс y и mid."""
    src = {
        "линейная C (v3)": ("v3lin", None),
        "бустинг (v4)": (f"v4gbdt_{corpus}_{target}_test.npz", "p_stack"),
        "Скеллам (v5)": (f"v5skellam_{corpus}_{target}_test.npz", "p_stack"),
        "регрессия разницы (v5)": (f"v5skellam_{corpus}_{target}_test.npz", "p_draft"),
        "драфт линейный": (f"v4gbdt_{corpus}_{target}_test.npz", "p_draft"),
    }
    base = OUT_DIR / f"v4gbdt_{corpus}_{target}_test.npz"
    if not base.exists():
        base = OUT_DIR / f"v5skellam_{corpus}_{target}_test.npz"
    if not base.exists():
        raise SystemExit(f"нет ни одного дампа для {corpus}/{target}")
    z0 = np.load(base)
    mids = z0["mid"]
    y = z0["y"].astype(int)
    order = {int(m): i for i, m in enumerate(mids)}
    got: dict[str, np.ndarray] = {}
    for label, (fname, key) in src.items():
        if key is None:
            continue
        p = OUT_DIR / fname
        if not p.exists():
            continue
        z = np.load(p)
        v = np.full(len(mids), np.nan)
        idx = np.asarray([order.get(int(m), -1) for m in z["mid"]])
        ok = idx >= 0
        v[idx[ok]] = z[key][ok]
        if np.isfinite(v).mean() > 0.9:
            got[label] = v
    # словарь ed как отдельный «оценщик»
    dpath = OUT_DIR / f"dictv4_{corpus}.npz"
    fpath = OUT_DIR / f"featuresv3_{corpus}.npz"
    if dpath.exists() and fpath.exists():
        zz = np.load(dpath, allow_pickle=True)
        names = [str(s) for s in zz["names"]]
        col = names.index(f"dict_ed_{target[2:]}") if f"dict_ed_{target[2:]}" in names else -1
        if col >= 0:
            v = np.full(len(mids), np.nan)
            idx = np.asarray([order.get(int(m), -1) for m in zz["mid"]])
            ok = idx >= 0
            v[idx[ok]] = zz["X"][ok, col]
            if np.isfinite(v).mean() > 0.5:
                got["словарь ed"] = v
    return got, y, mids


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="pro")
    ap.add_argument("--targets", default="w_5_15,w_10_20,w_15_25,w_20_30")
    args = ap.parse_args()
    from sklearn.linear_model import LogisticRegression
    L = [f"# Смесь оценщиков: {args.corpus}", "",
         "Веса смеси учатся на ПЕРВОЙ половине тестового среза по времени, число —",
         "со второй. Отдельные модели показаны на той же второй половине, чтобы",
         "сравнение было на одних картах.", ""]
    report: dict[str, dict] = {}
    for target in [t for t in args.targets.split(",") if t]:
        try:
            got, y, mids = collect(args.corpus, target)
        except SystemExit as e:
            L.append(f"**{target}**: {e}")
            continue
        if len(got) < 2:
            L.append(f"**{target}**: оценщиков меньше двух, смешивать нечего")
            continue
        keys = sorted(got)
        M = np.stack([got[k] for k in keys], axis=1)
        ok = np.isfinite(M).all(axis=1)
        M, yy = M[ok], y[ok]
        half = len(yy) // 2
        R = np.stack([rank(M[:, j]) for j in range(M.shape[1])], axis=1)
        blend = LogisticRegression(C=1.0, max_iter=1000).fit(R[:half], yy[:half])
        pe = blend.predict_proba(R[half:])[:, 1]
        L += [f"## {target} (карт в замере {len(yy) - half:,})", "",
              "| оценщик | AUC на второй половине | вес в смеси |", "|---|---:|---:|"]
        for j, k in enumerate(keys):
            L.append(f"| {k} | {auc_of(yy[half:], M[half:, j]):.4f} | "
                     f"{blend.coef_[0][j]:+.2f} |")
        a_ens = auc_of(yy[half:], pe)
        best_single = max(auc_of(yy[half:], M[half:, j]) for j in range(M.shape[1]))
        L += [f"| **смесь** | **{a_ens:.4f}** | |", "",
              f"Прибавка смеси над лучшим одиночным: **{a_ens - best_single:+.4f}**.", ""]
        report[target] = {"ensemble_auc": a_ens, "best_single": best_single,
                          "n": int(len(yy) - half),
                          "singles": {k: auc_of(yy[half:], M[half:, j])
                                      for j, k in enumerate(keys)}}
    out = ROOT / f"runtime/artifacts/kills/ensemble_{args.corpus}.md"
    out.write_text("\n".join(L) + "\n", encoding="utf-8")
    (OUT_DIR / f"report_ensemble_{args.corpus}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print("\n".join(L))
    print("OUT:", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
