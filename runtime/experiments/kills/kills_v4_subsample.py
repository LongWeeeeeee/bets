#!/usr/bin/env python3
"""Свежий срез корпуса одним файлом — чтобы увезти его на serv2 и там считать.

serv2 — 8 ГБ памяти, полный паблик (4 ГБ признаков) туда не помещается вместе с
обучением. Берём последние N карт: сплит внутри среза остаётся хронологическим,
и абляция блоков на нём отвечает на тот же вопрос, что и на полном корпусе.

Запуск:
    python3 kills_v4_subsample.py --corpus public --rows 1500000 --out subpub
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"


def cut(corpus: str, rows: int, out: str) -> None:
    for kind in ("featuresv3", "extrav4", "dictv4"):
        src = OUT_DIR / f"{kind}_{corpus}.npz"
        if not src.exists():
            print(f"  нет {src.name}, пропуск", flush=True)
            continue
        z = np.load(src, allow_pickle=True)
        n = len(z["X"])
        take = slice(max(0, n - rows), n)
        data = {}
        for k in z.files:
            v = z[k]
            data[k] = v[take] if (v.ndim >= 1 and len(v) == n) else v
        dst = OUT_DIR / f"{kind}_{out}.npz"
        np.savez_compressed(dst, **data)
        print(f"  {src.name}: {n:,} -> {len(data['X']):,} строк, "
              f"{dst.stat().st_size / 2**20:.0f} МБ -> {dst.name}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="public")
    ap.add_argument("--rows", type=int, default=1500000)
    ap.add_argument("--out", default="subpub")
    args = ap.parse_args()
    cut(args.corpus, args.rows, args.out)


if __name__ == "__main__":
    main()
