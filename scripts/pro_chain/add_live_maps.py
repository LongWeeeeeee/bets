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

Арифметика накопителей из `ideas_batch5.build` и `ideas_batch8.build` сохранена
точно — включая сырые (не поминутные) величины, безусловное обновление норм и
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


def account_maps(pst, accounts, heroes, stop):
    """Legacy sums, in player order, without per-account Python containers.

    cumsum is sequential within each position/hero history. add.at performs
    repeated-index additions in input order (ordinary indexed += does not).
    Nonpositive accounts are excluded only from residuals, never from norms.
    """
    flat = pst.reshape(-1, 2)  # HD, NW; unused stat columns never interact.
    acc = accounts.ravel()
    hrs = heroes.ravel()
    pos_res = np.zeros_like(flat)
    pos_valid = np.ones(len(flat), dtype=bool)
    for p in range(5):
        values = flat[p::5]
        history = np.cumsum(values, axis=0)
        pos_res[p + 5::5] = values[1:] - history[:-1] / np.arange(1, len(values))[:, None]
        pos_valid[p] = False

    hero_res = np.zeros(len(flat), dtype=np.float64)
    hero_valid = np.ones(len(flat), dtype=bool)
    order = np.argsort(hrs, kind="stable")
    sorted_heroes = hrs[order]
    edges = np.r_[0, np.flatnonzero(sorted_heroes[1:] != sorted_heroes[:-1]) + 1, len(order)]
    for begin, end in zip(edges[:-1], edges[1:]):
        players = order[begin:end]
        if not len(players):
            continue
        values = flat[players, 0]
        history = np.cumsum(values)
        hero_res[players[1:]] = values[1:] - history[:-1] / np.arange(1, len(players))
        hero_valid[players[0]] = False
    del order, sorted_heroes, history, values

    acc_ids, acc_idx = np.unique(acc, return_inverse=True)
    hero_ids = np.unique(hrs)
    # Packing dense ranks, not raw IDs, avoids account-ID overflow. Sorted
    # packed ranks have exactly the legacy sorted (account, hero) row order.
    packed = acc_idx * len(hero_ids) + np.searchsorted(hero_ids, hrs)
    pair_ids, pair_idx = np.unique(packed, return_inverse=True)
    del packed
    pos_sum = np.zeros((len(acc_ids), 2))
    pos_count = np.zeros(len(acc_ids))
    hero_sum = np.zeros(len(pair_ids))
    hero_count = np.zeros(len(pair_ids))
    pos_valid &= acc > 0
    hero_valid &= acc > 0

    def accumulate(begin, end):
        valid = pos_valid[begin:end]
        idx = acc_idx[begin:end][valid]
        for stat in range(2):
            np.add.at(pos_sum[:, stat], idx, pos_res[begin:end, stat][valid])
        np.add.at(pos_count, idx, 1.0)
        valid = hero_valid[begin:end]
        idx = pair_idx[begin:end][valid]
        np.add.at(hero_sum, idx, hero_res[begin:end][valid])
        np.add.at(hero_count, idx, 1.0)

    def snapshot():
        used = pos_count > 0
        acc_arr = np.column_stack((acc_ids[used], pos_sum[used] / pos_count[used, None]))
        used = hero_count > 0
        pairs = pair_ids[used]
        ah_arr = np.column_stack((acc_ids[pairs // len(hero_ids)],
                                  hero_ids[pairs % len(hero_ids)],
                                  hero_sum[used] / hero_count[used]))
        # np.array([]) in the legacy dump has shape (0,), not (0, width).
        return (acc_arr if len(acc_arr) else np.empty(0),
                ah_arr if len(ah_arr) else np.empty(0))

    if stop is None:
        accumulate(0, len(acc))
        return snapshot(), None
    split = stop * 10
    accumulate(0, split)
    at_test = snapshot()
    accumulate(split, len(acc))
    return snapshot(), at_test


def dump(acc_arr, ah_arr, vs_pos, vs_flat, syn_pos, syn_flat, path: Path) -> None:
    """Flat arrays retain insertion order; avoid transient lists of rows."""
    def rows(store, width, nested=False):
        if not store:
            return np.empty(0, dtype=np.float64)
        result = np.empty((len(store), width), dtype=np.float64)
        for i, (key, cell) in enumerate(store.items()):
            if nested:
                result[i, :4] = key[0] + key[1]
            else:
                result[i, :len(key)] = key
            result[i, -3:] = cell
        return result

    vp, vf = rows(vs_pos, 7), rows(vs_flat, 5)
    sp, sf = rows(syn_pos, 7, nested=True), rows(syn_flat, 5)
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
    pst = zr["pstats"][:, :, (P_HD, P_NW)][idx_r].astype(np.float64)
    zc.close()
    zr.close()
    del zc, zr, pos_, keep, idx_r
    n = len(ts)
    print(f"карт: {n:,}; тестовая граница {TEST_FROM}", flush=True)

    boundary = np.flatnonzero(ts >= TEST_FROM)
    stop = int(boundary[0]) if len(boundary) else None
    full_accounts, test_accounts = account_maps(pst, accounts, heroes, stop)
    del pst, accounts, boundary
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
            dump(*test_accounts, vs_pos, vs_flat, syn_pos, syn_flat, OUT_TEST)
            dumped_test = True

        won_r = bool(wins[i])
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
    dump(*full_accounts, vs_pos, vs_flat, syn_pos, syn_flat, OUT_FULL)
    print("готово", flush=True)


if __name__ == "__main__":
    main()
