#!/usr/bin/env python3
"""Сколько разброса оконных килов объясняется УСТОЙЧИВОЙ принадлежностью — ICC.

Замер по блокам одной карты (`kills_v4_persistence.py`) упирается в отбор: дальние
пары блоков существуют только у длинных карт, а длинная карта — это равная карта.
Здесь этой проблемы нет: сравниваются РАЗНЫЕ карты одного и того же игрока /
команды / пары команд. Разные карты независимы, инерция внутри карты в оценку не
попадает вовсе.

ICC (доля дисперсии между группами) = верхняя граница того, что предматчевая
модель может выучить про эту сущность. «Верхняя» — потому что модель не знает
идентичность как таковую, она знает лишь её наблюдаемые следы (историю).

Считается одномерная модель случайных эффектов:
    ICC = (MSB - MSW) / (MSB + (k0 - 1) * MSW),
где k0 — эффективный размер группы. Группы размера 1 в счёт не идут.

Запуск:
    venv_catboost/bin/python3 runtime/experiments/kills/kills_v4_icc.py --corpus pro
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/kills"))

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
OUT = ROOT / "runtime/artifacts/kills/kills_v4_icc.md"
WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))


def icc(groups: np.ndarray, values: np.ndarray, min_size: int = 2) -> tuple[float, int, float]:
    """ICC однофакторной модели; возвращает (icc, число групп, средний размер)."""
    ok = np.isfinite(values) & (groups >= 0)
    g, v = groups[ok], values[ok]
    order = np.argsort(g, kind="stable")
    g, v = g[order], v[order]
    starts = np.flatnonzero(np.concatenate(([True], g[1:] != g[:-1])))
    sizes = np.diff(np.concatenate([starts, [len(g)]]))
    keep = sizes >= min_size
    if keep.sum() < 10:
        return float("nan"), 0, 0.0
    gid = np.repeat(np.arange(len(sizes)), sizes)
    sel = keep[gid]
    g2, v2, sizes = gid[sel], v[sel], sizes[keep]
    _, g2 = np.unique(g2, return_inverse=True)
    n, a = len(v2), len(sizes)
    grand = v2.mean()
    means = np.bincount(g2, weights=v2) / np.bincount(g2)
    ssb = float(np.sum(sizes * (means - grand) ** 2))
    ssw = float(np.sum((v2 - means[g2]) ** 2))
    if a <= 1 or n <= a:
        return float("nan"), a, float(sizes.mean())
    msb, msw = ssb / (a - 1), ssw / (n - a)
    k0 = (n - float(np.sum(sizes ** 2)) / n) / (a - 1)
    val = (msb - msw) / (msb + (k0 - 1) * msw) if (msb + (k0 - 1) * msw) > 0 else 0.0
    return float(max(val, 0.0)), a, float(sizes.mean())


def load(corpus: str) -> dict[str, np.ndarray]:
    parts = sorted(glob.glob(str(OUT_DIR / f"rowsv3_{corpus}.shard*.npz")))
    if not parts:
        raise SystemExit(f"нет шардов rowsv3_{corpus}")
    acc: dict[str, list[np.ndarray]] = {}
    for p in parts:
        z = np.load(p)
        for k in ("diffs", "valid", "accounts", "teams", "ts", "mid", "duration"):
            acc.setdefault(k, []).append(z[k])
    d = {k: np.concatenate(v) for k, v in acc.items()}
    _, first = np.unique(d["mid"], return_index=True)
    keep = np.sort(first)
    return {k: v[keep] for k, v in d.items()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="pro")
    ap.add_argument("--min-size", type=int, default=3)
    args = ap.parse_args()
    d = load(args.corpus)
    n = len(d["ts"])
    L = [f"# ICC оконных килов: {args.corpus}", "",
         f"Карт {n:,}. Сравниваются РАЗНЫЕ карты одной сущности, поэтому инерция",
         "внутри карты в оценку не попадает. Группы меньше "
         f"{args.min_size} карт отброшены.", "",
         "| сущность | окно | групп | средний размер | ICC |", "|---|---|---:|---:|---:|"]

    teams = d["teams"]
    accounts = d["accounts"]
    for wi, (a, b) in enumerate(WINDOWS):
        ok = d["valid"][:, wi].astype(bool)
        diff = np.where(ok, d["diffs"][:, wi], np.nan).astype(np.float64)

        if (teams > 0).any():
            # Одна команда: её собственный перевес (знак по её стороне), все карты.
            g = np.concatenate([teams[:, 0], teams[:, 1]])
            v = np.concatenate([diff, -diff])
            g = np.where(g > 0, g, -1)
            val, cnt, size = icc(g, v, args.min_size)
            L.append(f"| команда | {a}-{b} | {cnt:,} | {size:.1f} | {val:.4f} |")

            # Пара команд: устойчивость именно этого противостояния.
            lo = np.minimum(teams[:, 0], teams[:, 1])
            hi = np.maximum(teams[:, 0], teams[:, 1])
            sign = np.where(teams[:, 0] == lo, 1.0, -1.0)
            uniq, inv = np.unique(np.stack([lo, hi], axis=1), axis=0, return_inverse=True)
            gp = np.where((lo > 0) & (hi > 0), inv, -1)
            val, cnt, size = icc(gp, diff * sign, args.min_size)
            L.append(f"| пара команд | {a}-{b} | {cnt:,} | {size:.1f} | {val:.4f} |")

        # Игрок: его личный вклад — перевес стороны, за которую он играл.
        side = np.concatenate([np.zeros(5, int), np.ones(5, int)])
        gp = accounts.ravel()
        gp = np.where(gp > 0, gp, -1)
        vv = np.where(side[None, :] == 0, diff[:, None], -diff[:, None]).ravel()
        val, cnt, size = icc(gp, vv, args.min_size)
        L.append(f"| игрок | {a}-{b} | {cnt:,} | {size:.1f} | {val:.4f} |")

    L += ["", "## Как читать", "",
          "ICC игрока — доля дисперсии оконной разницы, которую объясняет один лишь",
          "факт «этот игрок здесь играет». В команде их пятеро с каждой стороны, но",
          "их вклады НЕ складываются линейно: игроки одной команды играют вместе, и",
          "их эффекты почти полностью совпадают. Поэтому ориентир для предматчевого",
          "потолка — ICC команды и пары команд, а не сумма десяти ICC игроков.", "",
          "Смещение вверх: карты одной пары команд идут подряд в одной серии одного",
          "турнира, то есть делят и патч, и форму дня, и физическое состояние. Это",
          "не «знание до карты» в смысле модели, а общий контекст."]
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))
    print("OUT:", OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
