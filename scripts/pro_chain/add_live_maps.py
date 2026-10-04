#!/usr/bin/env python3
"""Кладёт в артефакт то, без чего шесть новых колонок E-168 нельзя посчитать в бою.

Замер кандидата C дал +0.0044 на тесте, но обучающие колонки брались из кэшей
`ideas_batch5.npz` и `ideas_batch8.npz` — это матрицы «значение на карте», а не
состояние, из которого скорер считает признак для НОВОГО матча. В артефакте
сейчас нет ни того, ни другого:

  a_hdmg_rel_pos, a_nw_rel_pos   накопленное по аккаунту отклонение урона и
                                 нетворса от нормы ПОЗИЦИИ — нужны две новые
                                 колонки в `accounts`;
  a_hdmg_rel_hero                то же от нормы ГЕРОЯ — новая колонка в `acc_hero`;
  cp_lane, syn_pos_mean          позиционные про-ячейки контрпика и синергии с
                                 распадом 45 дней и шринкеджем к позиционно-
                                 агностичной паре — нужны сами словари.

(Шестая, hybrid_strength, состояния не требует: прод и так восстанавливает
`HybridPlayerRosterEloModel` и зовёт `preview_team_strength` — разность двух
`team_strength` делить на 400.)

Накопители скопированы из `ideas_batch5.build` и `ideas_batch8.build` буква в
букву — включая сырые (не поминутные) величины, безусловное обновление норм и
порядок «сначала читаем состояние, потом обновляем». Расхождение здесь стоит
ровно того же, что стоило деление на 100 в E-166: −0.116 AUC.

Пишутся ДВА снимка:
  full   состояние на конец корпуса — идёт в боевой артефакт;
  at_test состояние на границе TEST_FROM — им проверяется, что скорер считает
         то же самое, что видела модель на обучении (см. verify_live_maps.py).

Запуск: venv_catboost/bin/python3 scripts/pro_chain/add_live_maps.py
"""
from __future__ import annotations

import math
import os
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ideas_batch2 import COMPACT, RICH  # noqa: E402

OUT_FULL = ROOT / "runtime/artifacts/misc/live_maps_full.npz"
OUT_TEST = ROOT / "runtime/artifacts/misc/live_maps_at_test.npz"
TEST_FROM = 1774742400
HL = 45.0                       # период полураспада ячеек драфта, дней
P_NW, P_HD = 5, 8               # нумерация из ideas_batch5


def dump(res_pos, res_hero, vs_pos, vs_flat, syn_pos, syn_flat, path: Path) -> None:
    """Снимок накопителей в плоские массивы."""
    accs = sorted({a for (a, _st) in res_pos})
    acc_arr = np.array(
        [[a] + [(res_pos[(a, st)][0] / res_pos[(a, st)][1]) if res_pos.get((a, st), [0, 0])[1] else 0.0
                for st in (P_HD, P_NW)] for a in accs], dtype=np.float64)
    ah = sorted(res_hero)
    ah_arr = np.array([[a, h, res_hero[(a, h, P_HD)][0] / res_hero[(a, h, P_HD)][1]]
                       for (a, h, _st) in ah if res_hero[(a, h, P_HD)][1]], dtype=np.float64)
    vp = np.array([[k[0], k[1], k[2], k[3], v[0], v[1], v[2]] for k, v in vs_pos.items()],
                  dtype=np.float64)
    vf = np.array([[k[0], k[1], v[0], v[1], v[2]] for k, v in vs_flat.items()], dtype=np.float64)
    sp = np.array([[k[0][0], k[0][1], k[1][0], k[1][1], v[0], v[1], v[2]]
                   for k, v in syn_pos.items()], dtype=np.float64)
    sf = np.array([[k[0], k[1], v[0], v[1], v[2]] for k, v in syn_flat.items()], dtype=np.float64)
    np.savez_compressed(path, acc_hdmg_nw=acc_arr, acc_hero_hdmg=ah_arr,
                        vs_pos=vp, vs_flat=vf, syn_pos=sp, syn_flat=sf)
    print(f"  -> {path.name}: аккаунтов {len(acc_arr):,}, ячеек (acc,hero) {len(ah_arr):,}, "
          f"vs_pos {len(vp):,}, vs_flat {len(vf):,}, syn_pos {len(sp):,}, syn_flat {len(sf):,}",
          flush=True)


def main() -> None:
    zc, zr = np.load(COMPACT), np.load(RICH)
    pos_ = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    keep = np.array([int(m) in pos_ for m in zc["mids"].tolist()])
    idx_r = np.array([pos_[int(m)] for m in zc["mids"][keep].tolist()])
    ts, wins = zc["ts"][keep], zc["wins"][keep].astype(int)
    heroes, accounts = zc["heroes"][keep], zc["accounts"][keep]
    pst = zr["pstats"][idx_r][:, :, :12].astype(np.float64)
    del zr
    n = len(ts)
    print(f"карт: {n:,}; тестовая граница {TEST_FROM}", flush=True)

    # ---- накопители ideas_batch5 (только те статистики, что нужны)
    pos_norm = np.zeros((6, 12)); pos_cnt = np.zeros(6)
    hero_norm: dict[int, np.ndarray] = defaultdict(lambda: np.zeros(12))
    hero_cnt: dict[int, float] = defaultdict(float)
    res_pos: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0])
    res_hero: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0])
    # ---- накопители ideas_batch8
    lam = math.log(2) / (HL * 86400.0)
    vs_pos: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0, 0])
    vs_flat: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0, 0])
    syn_pos: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0, 0])
    syn_flat: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0, 0])

    def dec(c, now):
        w, g, t0 = c
        f = math.exp(-lam * (now - t0)) if g > 0 else 0.0
        return w * f, g * f

    dumped_test = False
    for i in range(n):
        now = int(ts[i])
        if not dumped_test and now >= TEST_FROM:
            print(f"[{i:,}] граница теста, снимок at_test", flush=True)
            dump(res_pos, res_hero, vs_pos, vs_flat, syn_pos, syn_flat, OUT_TEST)
            dumped_test = True

        won_r = bool(wins[i])
        # --- обновление ячеек аккаунтов (ideas_batch5)
        for s in range(2):
            accs = [int(a) for a in accounts[i, s * 5:(s + 1) * 5]]
            hrs = [int(h) for h in heroes[i, s * 5:(s + 1) * 5]]
            for p in range(5):
                a, h = accs[p], hrs[p]
                st = pst[i, s * 5 + p]
                posn = p + 1
                if a > 0:
                    if pos_cnt[posn] > 0:
                        pn = pos_norm[posn] / pos_cnt[posn]
                        for k2 in (P_HD, P_NW):
                            c = res_pos[(a, k2)]
                            c[0] += float(st[k2]) - pn[k2]
                            c[1] += 1
                    if hero_cnt[h] > 0:
                        hn = hero_norm[h] / hero_cnt[h]
                        c = res_hero[(a, h, P_HD)]
                        c[0] += float(st[P_HD]) - hn[P_HD]
                        c[1] += 1
                pos_norm[posn] += st
                pos_cnt[posn] += 1
                hero_norm[h] += st
                hero_cnt[h] += 1
        # --- обновление ячеек драфта (ideas_batch8)
        rh = [(int(heroes[i, p]), p + 1) for p in range(5)]
        dh = [(int(heroes[i, 5 + p]), p + 1) for p in range(5)]
        for (h1, p1) in rh:
            for (h2, p2) in dh:
                for key, store in (((h1, p1, h2, p2), vs_pos), ((h1, h2), vs_flat)):
                    w, g = dec(store[key], now)
                    store[key] = [w + int(won_r), g + 1.0, now]
                for key, store in (((h2, p2, h1, p1), vs_pos), ((h2, h1), vs_flat)):
                    w, g = dec(store[key], now)
                    store[key] = [w + int(not won_r), g + 1.0, now]
        for side, res in ((rh, won_r), (dh, not won_r)):
            for (a, pa), (b, pb) in combinations(side, 2):
                key = (min((a, pa), (b, pb)), max((a, pa), (b, pb)))
                w, g = dec(syn_pos[key], now); syn_pos[key] = [w + int(res), g + 1.0, now]
                fk = (min(a, b), max(a, b))
                w, g = dec(syn_flat[fk], now); syn_flat[fk] = [w + int(res), g + 1.0, now]
        if (i + 1) % 100_000 == 0:
            print(f"  {i+1:,}/{n:,}", flush=True)

    print("снимок full", flush=True)
    dump(res_pos, res_hero, vs_pos, vs_flat, syn_pos, syn_flat, OUT_FULL)
    print("готово", flush=True)


if __name__ == "__main__":
    main()
