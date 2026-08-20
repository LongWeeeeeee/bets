#!/usr/bin/env python3
"""Glicko-1 по ОРГАНИЗАЦИЯМ: рейтинг, доступный когда снимок не знает игроков.

ЗАЧЕМ. После внедрения лестницы (E-213) больше половины боевого потока считается
короткой веткой из семи колонок: снимок не знает аккаунтов, и вместе с ними
умирают `elo`, `opp_elo` и весь блок игроков. Игроцкий Glicko тут не помощник —
он сам ключуется аккаунтом (E-214 §2). А рейтинг ОРГАНИЗАЦИИ доступен:
`resolve_org` опознаёт состав по `org_roster` и там, где таблица аккаунтов
молчит — на 33.0% тестовых карт и на 27.7% тех, где аккаунтов нет.

ИЗМЕРЕНО (короткая ветка, окно обучения 120 суток, тест 26 016):

    база, 7 колонок                     0.6852   где орг опознана 0.6954
    + рейтинг орг с честным занулением  0.6873   где орг опознана 0.7018

То есть +0.0064 ровно там, где величина есть, и +0.0021 на всей ветке. Флаг
«опознана» ничего не добавляет сверх зануления — хватает двух колонок.

ЧЕСТНОЕ ЗАНУЛЕНИЕ ОБЯЗАТЕЛЬНО. В обучающей матрице организация есть всегда (она
берётся из `teams` корпуса), а в бою — на трети карт. Обучить на полной колонке
и подставлять ноль в бою значило бы дать модели не то, на чём она училась, — тот
самый источник расхождений, из-за которых у трети боевых колонок корреляция
обучения и боя 0.4-0.7. Поэтому колонка зануляется там, где боевой путь
организацию не опознал бы; цена зануления измерена и мала (0.6873 против 0.6877).

НА ПОЛНУЮ ВЕТКУ НЕ СТАВИТСЯ. Там рейтинг организации даёт +0.0001 — его уже
несёт `hybrid_strength`. И, что важнее, полная ветка обязана оставаться
побитовой копией боевых весов.

Выход:
    runtime/artifacts/misc/org_rating.npz     колонки обучения (mids, diff, exp, known)
    runtime/artifacts/misc/org_rating_snapshot.npz  таблица для боя (org, rating, rd)

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/build_org_rating.py
"""
from __future__ import annotations

import math
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
sys.path.insert(0, str(ROOT / "base"))

import prematch_scorer as ps  # noqa: E402
from ideas_batch2 import COMPACT, TEST_FROM  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
HYB = ART / "map_winner_hybrid_quality_forward/hybrid_features.npz"
DEPLOYED = ART / "prematch_model_artifact_v3_branches.npz"
OUT_COLS = ART / "org_rating.npz"
# ДВА снимка, как у `add_live_maps` (`live_maps_at_test` / `live_maps_full`).
# Полный идёт в бой: там он собирается по прошлому и утечки в нём нет. Обрезанный
# на границе теста нужен аудиту — иначе рейтинг знает исходы тестовых карт, и
# сквозной прогон показывает выдуманную прибавку. Один раз уже показал: +0.0317
# на ветке, которая на честном снимке даёт совсем другое.
OUT_SNAP = ART / "org_rating_snapshot.npz"
OUT_SNAP_TEST = ART / "org_rating_snapshot_at_test.npz"

Q = math.log(10.0) / 400.0
RD0 = 350.0
RD_MIN = 30.0
R0 = 1500.0


def g_of(rd: float) -> float:
    return 1.0 / math.sqrt(1.0 + 3.0 * Q * Q * rd * rd / (math.pi ** 2))


def expected(ra: float, rb: float, rd_b: float) -> float:
    return 1.0 / (1.0 + 10 ** (-g_of(rd_b) * (ra - rb) / 400.0))


def main() -> None:
    t0 = time.time()
    zc = np.load(COMPACT)
    mids = np.load(HYB, allow_pickle=True)["mids"].astype(np.int64)
    cpos = {int(m): i for i, m in enumerate(zc["mids"].tolist())}
    ci = np.array([cpos[int(m)] for m in mids.tolist()])
    ts, wins = zc["ts"][ci], zc["wins"][ci].astype(int)
    teams, acc = zc["teams"][ci], zc["accounts"][ci]

    art = np.load(DEPLOYED, allow_pickle=True)
    merge = {int(a): int(b) for a, b in art["team_merge"]}
    org = np.array([[merge.get(int(t), int(t)) for t in row] for row in teams])
    print(f"карт {len(mids):,}; организаций различных "
          f"{len(set(org.flatten().tolist())):,}; склеек {len(merge):,}", flush=True)

    rat: dict = defaultdict(lambda: R0)
    rd: dict = defaultdict(lambda: RD0)
    diff = np.zeros(len(mids))
    exp = np.zeros(len(mids))
    at_test: dict = {}
    for i in np.argsort(ts, kind="stable"):
        if not at_test and int(ts[i]) >= TEST_FROM:
            at_test = {o: (rat[o], rd[o]) for o in list(rat)}
        a, b = int(org[i, 0]), int(org[i, 1])
        ra, rb, da, db = rat[a], rat[b], rd[a], rd[b]
        ea, eb = expected(ra, rb, db), expected(rb, ra, da)
        diff[i] = (ra - rb) / 400.0
        exp[i] = ea - 0.5
        won = 1.0 if int(wins[i]) == 1 else 0.0
        for who, r_, d_, e_, gg, sc in ((a, ra, da, ea, g_of(db), won),
                                        (b, rb, db, eb, g_of(da), 1.0 - won)):
            d2 = 1.0 / (Q * Q * gg * gg * e_ * (1.0 - e_) + 1e-12)
            rat[who] = r_ + (Q / (1.0 / (d_ * d_) + 1.0 / d2)) * gg * (sc - e_)
            rd[who] = max(math.sqrt(1.0 / (1.0 / (d_ * d_) + 1.0 / d2)), RD_MIN)

    # --- опознаётся ли организация БОЕВЫМ путём; считаем там, где нужно учить
    WIN = int(os.getenv("BRANCH_WINDOW_DAYS", "120")) * 86400
    tr = (ts < TEST_FROM) & (ts >= TEST_FROM - WIN)
    te = ts >= TEST_FROM
    need = np.flatnonzero(tr | te)
    audit = ART / f"prematch_audit_branches_cut{TEST_FROM}.npz"
    m = ps.PrematchModel(audit if audit.exists() else DEPLOYED)
    known = np.zeros(len(mids), dtype=bool)
    print(f"опознание организации на {len(need):,} картах…", flush=True)
    for k, i in enumerate(need):
        a5 = [int(x) for x in acc[i]]
        known[i] = (m.resolve_org(0, a5[:5]) > 0 and m.resolve_org(0, a5[5:]) > 0)
        if (k + 1) % 20000 == 0:
            print(f"  {k+1:,}/{len(need):,}", flush=True)
    print(f"  опознано: обучение {known[tr].mean():.1%}, тест {known[te].mean():.1%}",
          flush=True)

    np.savez_compressed(OUT_COLS, mids=mids,
                        diff=np.where(known, diff, 0.0),
                        exp=np.where(known, exp, 0.0),
                        known=known.astype(np.int8))
    ids = sorted(rat)
    np.savez_compressed(
        OUT_SNAP,
        org_rating=np.array([[float(o), float(rat[o]), float(rd[o])] for o in ids]))
    tids = sorted(at_test)
    np.savez_compressed(
        OUT_SNAP_TEST,
        org_rating=np.array([[float(o), float(at_test[o][0]), float(at_test[o][1])]
                             for o in tids]) if tids else np.zeros((0, 3)))
    print(f"колонки → {OUT_COLS.name}; полный снимок {len(ids):,} организаций → "
          f"{OUT_SNAP.name}; обрезанный на границе теста {len(tids):,} → "
          f"{OUT_SNAP_TEST.name}; {time.time() - t0:.0f} c", flush=True)


if __name__ == "__main__":
    main()
