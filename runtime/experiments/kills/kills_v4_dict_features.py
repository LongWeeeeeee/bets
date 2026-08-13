#!/usr/bin/env python3
"""Значения прод-словаря `ed` как ПРИЗНАКИ модели (а не как соперник по AUC).

Словарь `kills_window_dict_raw.sqlite3` держит попарные ячейки «герой+позиция
против героя+позиции» с оконным перевесом килов, собранные на 5 млн паблик-карт.
Это более дробная величина, чем всё, что есть в v3: там герой знает только свой
средний оконный темп, а не темп ПРОТИВ конкретного оппонента на конкретной линии.

Почему только про. Словарь собран на тех же паблик-картах, на которых учится
паблик-модель, — на паблике это самозасев (E-05: разреженные метрики +0.17…0.20
AUC на своей выборке). Про-карт в словаре нет, поэтому там колонка честная.

Кладём четыре величины на карту (по одной на окно) плюс их модули: модуль
отвечает на «насколько уверенно», а знак — на «в чью сторону».

Запуск:
    venv_catboost/bin/python3 runtime/experiments/kills/kills_v4_dict_features.py --corpus pro
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime/experiments/kills"))

from window_v2_vs_dict_bigsample import StatsLookup, ed_for  # noqa: E402

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
DB = ROOT / "bets_data/analise_pub_matches/kills_window_dict_raw.sqlite3"
WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))


def build(corpus: str) -> None:
    if not DB.exists():
        raise SystemExit(f"нет словаря {DB}")
    z = np.load(OUT_DIR / f"featuresv3_{corpus}.npz", allow_pickle=True)
    heroes, mid = z["heroes"], z["mid"]
    n = len(heroes)
    lookup = StatsLookup(DB)
    tags = [f"{a}_{b}" for a, b in WINDOWS]
    X = np.full((n, len(tags) * 2), np.nan, dtype=np.float32)
    t0 = time.time()
    for i in range(n):
        got = ed_for(heroes[i], lookup)
        for j, t in enumerate(tags):
            v = got.get(t)
            if v is not None and np.isfinite(v):
                X[i, j] = v
                X[i, len(tags) + j] = abs(v)
        if (i + 1) % 20000 == 0:
            print(f"  {i + 1:,}/{n:,} ({(i + 1) / (time.time() - t0):.0f} карт/с)", flush=True)
    names = [f"dict_ed_{t}" for t in tags] + [f"dict_absed_{t}" for t in tags]
    out = OUT_DIR / f"dictv4_{corpus}.npz"
    np.savez_compressed(out, X=X, names=np.asarray(names), mid=mid)
    cov = np.isfinite(X[:, :len(tags)]).mean(0)
    print(f"[{corpus}] покрытие по окнам: "
          + ", ".join(f"{t} {c:.1%}" for t, c in zip(tags, cov)), flush=True)
    print(f"[{corpus}] -> {out}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="pro")
    args = ap.parse_args()
    build(args.corpus)


if __name__ == "__main__":
    main()
