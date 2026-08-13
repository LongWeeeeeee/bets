#!/usr/bin/env python3
"""Честный винрейт по уверенности: что реально даёт модель, если ставить не на всё.

AUC — про ранжирование всей выборки; ставят же не на всю выборку. Здесь то же
самое предсказание разложено так, как оно используется: сортируем карты по
уверенности |p−0.5| и смотрим, какая доля угадана в верхних k процентах.

Всё считается на ТЕСТОВОМ срезе, который в обучении и в отборе не участвовал:
файлы `*_test.npz` пишет `kills_v4_gbdt.py` в момент замера.

Дополнительно печатается калибровка: если модель говорит 0.62, доля побед должна
быть 0.62. Расхождение важнее AUC, когда речь про деньги.

Запуск:
    venv_catboost/bin/python3 runtime/experiments/kills/kills_v4_winrate.py \
        --tag v4gbdt --corpus public
"""
from __future__ import annotations

import argparse
import glob
import math
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/kills"))

from window_model_v2 import auc_of  # noqa: E402

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
ODDS = 1.8
TOPS = (0.01, 0.02, 0.05, 0.10, 0.20, 0.35, 0.50, 1.00)


def wilson(k: int, n: int) -> tuple[float, float]:
    if not n:
        return 0.0, 0.0
    p, z = k / n, 1.96
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, c - h), min(1.0, c + h)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tag", default="v4gbdt")
    ap.add_argument("--corpus", default="public")
    ap.add_argument("--key", default="p_stack", choices=("p_stack", "p_gbdt", "p_draft"))
    args = ap.parse_args()
    paths = sorted(glob.glob(str(OUT_DIR / f"{args.tag}_{args.corpus}_*_test.npz")))
    if not paths:
        raise SystemExit(f"нет дампов {args.tag}_{args.corpus}_*_test.npz")
    L = [f"# Винрейт по уверенности: {args.tag} / {args.corpus} / {args.key}", "",
         "Ставка на сторону, в которую смотрит модель; ничья считается поражением.",
         f"ROI при кэфе {ODDS} — допущение, реальной линии на килы у нас нет.", "",
         "| цель | верхние | карт | доля угадана | 95% ДИ | ROI@1.8 |",
         "|---|---|---:|---:|---|---:|"]
    for path in paths:
        name = Path(path).name.replace(f"{args.tag}_{args.corpus}_", "").replace("_test.npz", "")
        z = np.load(path)
        y, p = z["y"].astype(int), z[args.key].astype(float)
        conf = np.abs(p - 0.5)
        order = np.argsort(-conf)
        hit = ((p > 0.5).astype(int) == y).astype(int)
        for frac in TOPS:
            k = max(1, int(len(y) * frac))
            take = order[:k]
            w = int(hit[take].sum())
            lo, hi = wilson(w, k)
            L.append(f"| {name} | {frac:.0%} | {k:,} | {w / k:.4f} | "
                     f"{lo:.3f}–{hi:.3f} | {w / k * ODDS - 1:+.1%} |")
        L.append(f"| {name} | AUC | {len(y):,} | {auc_of(y, p):.4f} | | |")
    L += ["", "## Калибровка", "",
          "| цель | бин p | карт | предсказано | фактически |", "|---|---|---:|---:|---:|"]
    for path in paths:
        name = Path(path).name.replace(f"{args.tag}_{args.corpus}_", "").replace("_test.npz", "")
        z = np.load(path)
        y, p = z["y"].astype(int), z[args.key].astype(float)
        edges = np.quantile(p, np.linspace(0, 1, 11))
        for i in range(10):
            m = (p >= edges[i]) & (p <= edges[i + 1] if i == 9 else p < edges[i + 1])
            if m.sum() < 50:
                continue
            L.append(f"| {name} | {edges[i]:.3f}–{edges[i+1]:.3f} | {int(m.sum()):,} | "
                     f"{p[m].mean():.4f} | {y[m].mean():.4f} |")
    out = ROOT / f"runtime/artifacts/kills/winrate_{args.tag}_{args.corpus}.md"
    out.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))
    print("OUT:", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
