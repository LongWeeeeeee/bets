#!/usr/bin/env python3
"""Сборка снимка причинных приоров для живого пути.

Приоры F6/F7 — единственное, что предматчевой панели нужно из накопленного
состояния, и единственное, что по замерам окупается (тотал 0.7121 → 0.7442,
сторона 0.7190 → 0.7331, время 0.6188 → 0.6542).

Офлайн приор считается кумулятивной суммой со сбросом на границе группы: для
каждой карты берётся среднее по ПРЕДЫДУЩИМ картам того же ключа. В момент
ставки корпуса нет, но нужна ровно та же величина — а она полностью задаётся
накопленными суммой и счётчиком на момент сборки. Их и сохраняем.

Что здесь НЕ делается и почему. Метрики карты (`prior_values`) живой путь не
пересчитывает вовсе: он читает готовые накопления. Поэтому общими для офлайна
и прода должны быть только три вещи — порядок метрик, формула шринкеджа и
таблица агрегации по пятёрке, — и все три лежат в `base/causal_priors.py`,
откуда импортируются обеими сторонами. Копии этих таблиц не существует.

Снимок собирается по ВСЕМУ корпусу: на проде «прошлое» — это всё, что было до
сейчас. Причинность обучения этим не нарушается, там каждая карта видела только
своё прошлое.

Запуск: nohup venv_catboost/bin/python3 scripts/pro_chain/build_prior_snapshot.py ...
Выход:  data/prior_snapshot.npz + runtime/artifacts/misc/build_prior_snapshot.md
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
from causal_priors import PRIOR_NAMES, save_snapshot  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
OUT_NPZ = Path(os.getenv("PRIOR_SNAPSHOT", str(ROOT / "data" / "prior_snapshot.npz")))
OUT_MD = ART / "build_prior_snapshot.md"
TEAM_SLOTS = 5
# Отсечка: снимок собирается ТОЛЬКО по картам раньше неё. Для прода это «сейчас»,
# для честного замера — начало теста, иначе приор увидит будущее.
CUTOFF_TS = int(os.getenv("SNAPSHOT_CUTOFF", "0"))
# Аккаунты с малым числом карт после шринкеджа (K=60) почти неотличимы от
# глобального среднего: при 2 картах вес собственного значения 3.2%. Их
# хранение — 61% файла ради третьего знака.
MIN_GAMES = int(os.getenv("SNAPSHOT_MIN_GAMES", "3"))

_lines: list[str] = []


def say(s: str = "") -> None:
    print(s, flush=True)
    _lines.append(s)
    OUT_MD.write_text("\n".join(_lines) + "\n", encoding="utf-8")


def accumulate(keys: np.ndarray, V: np.ndarray, M: np.ndarray
               ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Суммы и счётчики по ключу: (уникальные ключи, суммы, счётчики).

    `keys` — (n, 10) ключ каждого слота, `V` — (n, 10, m) величина слота,
    `M` — (n, m) маска валидности метрики на карте.
    """
    flat = keys.ravel()
    valid_key = flat > 0
    uniq, inv = np.unique(flat[valid_key], return_inverse=True)
    m = V.shape[2]
    sums = np.zeros((len(uniq), m), dtype=np.float64)
    counts = np.zeros((len(uniq), m), dtype=np.float64)
    mask10 = np.repeat(M[:, None, :], 10, axis=1).reshape(-1, m)[valid_key]
    vals = V.reshape(-1, m)[valid_key]
    for j in range(m):
        w = mask10[:, j]
        sums[:, j] = np.bincount(inv[w], weights=vals[w, j].astype(np.float64),
                                 minlength=len(uniq))
        counts[:, j] = np.bincount(inv[w], minlength=len(uniq))
    return uniq, sums, counts


def main() -> None:
    t0 = time.time()
    zc, zr = np.load(COMPACT), np.load(RICH)
    rpos = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    have = np.array([int(m) in rpos for m in zc["mids"].tolist()])
    ri = np.array([rpos[int(m)] for m in zc["mids"][have].tolist()])
    # Отсечка применяется ОДНОЙ маской ко всем полям сразу. Первый заход
    # фильтровал heroes/accounts/ts, а wins брал по прежней маске — размеры
    # разошлись на 26 965 карт, и сборка упала на склейке метрик.
    ci = np.flatnonzero(have)
    if CUTOFF_TS:
        keep_t = zc["ts"][ci].astype(np.int64) < CUTOFF_TS
        ci, ri = ci[keep_t], ri[keep_t]
    heroes = zc["heroes"][ci].astype(np.int64)
    accounts = zc["accounts"][ci].astype(np.int64)
    ts = zc["ts"][ci].astype(np.int64)
    n = len(ts)
    say("# Снимок причинных приоров для живого пути")
    say()
    say(f"Карт {n:,}, окно {np.min(ts)}..{np.max(ts)}. Метрик "
        f"{len(PRIOR_NAMES)}, слотов на карту 10.")
    say()

    sub = {k: zr[k][ri] for k in ("pstats", "durations", "rk", "dk", "nw", "xp")}
    sub["wins"] = zc["wins"][ci]
    Vr, Vd, M = prior_values(sub)
    if Vr.shape[1] != len(PRIOR_NAMES):
        raise SystemExit(f"метрик {Vr.shape[1]}, а в PRIOR_NAMES "
                         f"{len(PRIOR_NAMES)} — порядок разошёлся")
    print(f"метрики посчитаны: {Vr.shape}, {time.time()-t0:.0f} c", flush=True)

    V = np.empty((n, 10, Vr.shape[1]), dtype=np.float32)
    V[:, :TEAM_SLOTS, :] = Vr[:, None, :]
    V[:, TEAM_SLOTS:, :] = Vd[:, None, :]

    hk, hs, hc = accumulate(heroes, V, M)
    print(f"герои: {len(hk)} ключей, {time.time()-t0:.0f} c", flush=True)
    pk, ps, pc = accumulate(accounts, V, M)
    before = len(pk)
    if MIN_GAMES > 1:
        keep_p = pc.max(1) >= MIN_GAMES
        pk, ps, pc = pk[keep_p], ps[keep_p], pc[keep_p]
    print(f"аккаунты: {before:,} → {len(pk):,} (порог {MIN_GAMES} карт), "
          f"{time.time()-t0:.0f} c", flush=True)

    # глобальное среднее метрики — то же, что офлайн: по обеим сторонам всех карт
    gl = np.empty(Vr.shape[1], dtype=np.float64)
    for j in range(Vr.shape[1]):
        w = M[:, j]
        cnt = 2 * int(w.sum())
        gl[j] = ((Vr[w, j].sum() + Vd[w, j].sum()) / cnt) if cnt else 0.0

    save_snapshot(OUT_NPZ, metrics=PRIOR_NAMES,
                  hero={int(k): (hs[i], hc[i]) for i, k in enumerate(hk)},
                  player={int(k): (ps[i], pc[i]) for i, k in enumerate(pk)},
                  globals_=gl, built_ts=int(np.max(ts)))
    size_mb = OUT_NPZ.stat().st_size / 1048576
    say(f"Ключей: героев **{len(hk)}**, аккаунтов **{len(pk):,}** из "
        f"{before:,} (порог {MIN_GAMES} карт: при шринкедже K=60 аккаунт с "
        f"двумя картами даёт 97% глобального среднего). Отсечка "
        f"{CUTOFF_TS or 'нет'}. Файл `{OUT_NPZ.name}` — {size_mb:.1f} МБ.")
    say()
    say("| метрика | глобальное среднее | карт с валидной метрикой |")
    say("|---|---|---|")
    for j, nm in enumerate(PRIOR_NAMES):
        say(f"| {nm} | {gl[j]:.4f} | {int(M[:, j].sum()):,} |")
    say()
    say(f"Прогон занял {time.time()-t0:.0f} c.")


if __name__ == "__main__":
    main()
