#!/usr/bin/env python3
"""Снимок парных приоров для блока F8 живой панели.

Копит суммы и счётчики по ключу ПАРЫ героев для двух метрик (`own_kills`,
`k_10_20`) — тех самых, из которых `catalog_features.py` строит F8. Формула
шринкеджа и способ вычитания одиночных приоров живут в `base/pair_priors.py`,
здесь только накопление, ровно как у `build_prior_snapshot.py` для F6/F7.

Каждая карта даёт 20 парных слотов: по десять пар (C(5,2)) на сторону. Значение
слота — метрика ЕГО стороны, поэтому радиантские пары копят радиантские килы, а
дайровские дайровские.

Отсечка `SNAPSHOT_CUTOFF` нужна для честного замера: снимок, собранный по всему
корпусу, на тестовых картах видел бы будущее.

Запуск: venv_catboost/bin/python3 scripts/pro_chain/build_pair_snapshot.py
Выход:  data/pair_prior_snapshot.npz + runtime/artifacts/misc/build_pair_snapshot.md
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(ROOT / "base"))
from ideas_batch2 import COMPACT, RICH  # noqa: E402
from catalog_features import prior_values  # noqa: E402
from causal_priors import K_PAIR, PRIOR_NAMES  # noqa: E402
from pair_priors import PAIRS, SYN_METRICS, pair_key  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
OUT_NPZ = Path(os.getenv("PAIR_SNAPSHOT",
                         str(ROOT / "data" / "pair_prior_snapshot.npz")))
OUT_MD = ART / "build_pair_snapshot.md"
CUTOFF_TS = int(os.getenv("SNAPSHOT_CUTOFF", "0"))

_lines: list[str] = []


def say(s: str = "") -> None:
    print(s, flush=True)
    _lines.append(s)
    OUT_MD.write_text("\n".join(_lines) + "\n", encoding="utf-8")


def main() -> None:
    t0 = time.time()
    jq = [PRIOR_NAMES.index(m) for m in SYN_METRICS]
    zc, zr = np.load(COMPACT), np.load(RICH)
    rpos = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    have = np.array([int(m) in rpos for m in zc["mids"].tolist()])
    ci = np.flatnonzero(have)
    ri = np.array([rpos[int(m)] for m in zc["mids"][ci].tolist()])
    if CUTOFF_TS:
        keep_t = zc["ts"][ci].astype(np.int64) < CUTOFF_TS
        ci, ri = ci[keep_t], ri[keep_t]
    heroes = zc["heroes"][ci].astype(np.int64)
    ts = zc["ts"][ci].astype(np.int64)
    n = len(ts)
    say("# Снимок парных приоров (F8)")
    say()
    say(f"Карт {n:,}, окно {int(ts.min())}..{int(ts.max())}. "
        f"Метрики: {', '.join(SYN_METRICS)}. Шринкедж K_PAIR={K_PAIR:g}.")
    say()

    sub = {k: zr[k][ri] for k in ("pstats", "durations", "rk", "dk", "nw", "xp")}
    sub["wins"] = zc["wins"][ci]
    Vr, Vd, M = prior_values(sub)
    Vr, Vd, M = Vr[:, jq], Vd[:, jq], M[:, jq]
    print(f"метрики посчитаны {Vr.shape}, {time.time()-t0:.0f} c", flush=True)

    # ключи: 10 пар радианта, затем 10 пар дайра
    keys = np.empty((n, 20), dtype=np.int64)
    for t, (i, j) in enumerate(PAIRS):
        for off, sl in ((0, 0), (10, 5)):
            a, b = heroes[:, sl + i], heroes[:, sl + j]
            keys[:, off + t] = np.minimum(a, b) * 1000 + np.maximum(a, b)
    assert keys.max() < 2 ** 62, "ключ переполнился"

    m = len(SYN_METRICS)
    V = np.empty((n, 20, m), dtype=np.float64)
    V[:, :10, :] = Vr[:, None, :]
    V[:, 10:, :] = Vd[:, None, :]

    flat = keys.ravel()
    ok = flat > 0
    uniq, inv = np.unique(flat[ok], return_inverse=True)
    mask20 = np.repeat(M[:, None, :], 20, axis=1).reshape(-1, m)[ok]
    vals = V.reshape(-1, m)[ok]
    sums = np.zeros((len(uniq), m))
    counts = np.zeros((len(uniq), m))
    for j in range(m):
        w = mask20[:, j]
        sums[:, j] = np.bincount(inv[w], weights=vals[w, j], minlength=len(uniq))
        counts[:, j] = np.bincount(inv[w], minlength=len(uniq))
    print(f"пар: {len(uniq):,}, {time.time()-t0:.0f} c", flush=True)

    gl = np.empty(m)
    for j in range(m):
        w = M[:, j]
        cnt = 2 * int(w.sum())
        gl[j] = ((Vr[w, j].sum() + Vd[w, j].sum()) / cnt) if cnt else 0.0

    tmp = OUT_NPZ.with_suffix(".tmp.npz")
    OUT_NPZ.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(tmp, keys=uniq, sums=sums, counts=counts, globals=gl,
                        shrink=np.float64(K_PAIR),
                        metrics=np.array(SYN_METRICS, dtype="<U16"),
                        built_ts=np.int64(int(ts.max())))
    os.replace(tmp, OUT_NPZ)

    say(f"Пар накоплено: **{len(uniq):,}**. Глобальные средние: "
        + ", ".join(f"{nm} {gl[j]:.4f}" for j, nm in enumerate(SYN_METRICS))
        + ".")
    say()
    say(f"| метрика | пар с ≥1 картой | медиана карт на пару | максимум |")
    say("|---|---|---|---|")
    for j, nm in enumerate(SYN_METRICS):
        c = counts[:, j]
        nz = c[c > 0]
        say(f"| {nm} | {len(nz):,} | {np.median(nz):.0f} | {c.max():.0f} |")
    say()
    say(f"Записано в `{OUT_NPZ}` ({OUT_NPZ.stat().st_size/1048576:.1f} МБ). "
        f"Прогон занял {time.time()-t0:.0f} c.")


if __name__ == "__main__":
    main()
