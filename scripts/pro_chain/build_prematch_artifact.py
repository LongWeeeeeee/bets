#!/usr/bin/env python3
"""Сборка боевого артефакта предматчевой модели: снимок состояния + веса.

Модель считает 20 признаков из АГРЕГАТОВ по прошлым матчам. В бою корпуса нет,
поэтому нужен снимок: account -> его величины, (account, hero) -> величины на
герое, hero -> винрейт за 30 дней, (hero, hero) -> матчапы, (team, team) ->
остаток личных встреч.

Здесь один хронологический проход по корпусу выписывает КОНЕЧНОЕ состояние всех
счётчиков и обучает ансамбль окон (90/180/365/730 дней), как в E-98. На выходе
один npz: снимок + четыре набора весов со своими mu/sd.

Запуск: venv_catboost/bin/python3 scripts/pro_chain/build_prematch_artifact.py
Выход:  runtime/artifacts/misc/prematch_model_artifact.npz (+ .json со спецификацией)
"""
from __future__ import annotations

import json
import math
import os
import sys
from collections import defaultdict, deque
from itertools import combinations
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression

ROOT = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ideas_batch2 import B1, B1_IDX, COMPACT, EI, EXT, LIVE, RICH, TEST_FROM  # noqa: E402
from ideas_batch2 import CACHE as B2C, NI as B2NI  # noqa: E402
from ideas_batch5 import CACHE as B5C, NI as B5NI  # noqa: E402
from ideas_batch5b import CACHE as B5BC, NI as B5BNI  # noqa: E402
from pro_features_wide import auc  # noqa: E402

# Аудит: снимок, обрезанный по времени. Нужен, чтобы оценить боевой путь честно —
# прод в момент карты имеет состояние ТОЛЬКО из прошлого, а обычный снимок собран
# по всему корпусу и знает тестовое окно. Веса при обрезке не обучаются: их берут
# из боевого артефакта.
CUTOFF = int(os.getenv("PREMATCH_CUTOFF", "0"))
# Режим ночного обновления: снимок пересобрать, веса не трогать. Без него v1
# лезет за кэшами признаков (EXT, драфт-логит), а они привязаны к длине
# корпуса: 14.08 корпус вырос с 482 486 до 1 260 250, и ночная цепочка упала
# на несовпадении форм, ни разу не обновив снимок.
SNAPSHOT_ONLY = os.getenv("PREMATCH_SNAPSHOT_ONLY", "0") == "1"
_SUF = f"_cut{CUTOFF}" if CUTOFF else ""
OUT = ROOT / f"runtime/artifacts/misc/prematch_model_artifact{_SUF}.npz"
SPEC = ROOT / f"runtime/artifacts/misc/prematch_model_spec{_SUF}.json"
FEATURES = ["draft_logit", "elo", "games", "hero_games", "pos_games", "opp_elo",
            "hero_pool", "form", "hero_gpm_rel", "imp_recent", "wr30", "h2h_resid",
            "gpm_rel_pos", "vs_wr", "imp50", "imp_rel_pos", "lh_rel_hero",
            "gpm_ewma", "lh30", "imp30"]
K, HL_PAIR, HL_EWMA, STRONG = 24.0, 45.0, 90.0, 1600.0
P_KILL, P_DEATH, P_ASSIST, P_GPM, P_LH, P_IMP = 0, 1, 2, 3, 6, 11


def team_mean_rating(rating: dict, account_ids) -> float:
    """Средний ELO команды по известным аккаунтам; 1500.0, если известных
    аккаунтов нет (не NaN — `np.mean([]) or 1500.0` возвращает nan, т.к. nan
    truthy)."""
    vals = [rating.get(int(a), 1500.0) for a in account_ids if a > 0]
    return float(np.mean(vals)) if vals else 1500.0


def _cell_index(slots, categories):
    """Sorted (dense account, category) cells and per-occurrence indices."""
    low, high = int(categories.min()), int(categories.max())
    span = high - low + 1
    valid = slots >= 0
    packed = slots[valid].astype(np.int64) * span + categories[valid] - low
    keys, inverse = np.unique(packed, return_inverse=True)
    indices = np.full(slots.shape, -1, dtype=np.int32)
    indices[valid] = inverse
    cells = np.column_stack((keys // span, keys % span + low))
    return cells, indices


def main() -> None:
    zc, zr = np.load(COMPACT), np.load(RICH)
    pos_ = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    keep = np.array([int(m) in pos_ for m in zc["mids"].tolist()])
    idx_r = np.array([pos_[int(m)] for m in zc["mids"][keep].tolist()])
    ts, wins = zc["ts"][keep], zc["wins"][keep].astype(int)
    heroes, accounts, teams = zc["heroes"][keep], zc["accounts"][keep], zc["teams"][keep]
    pst = zr["pstats"][idx_r]
    del pos_, idx_r
    if CUTOFF:
        msk = ts < CUTOFF
        ts, wins, heroes, accounts, teams, pst = (ts[msk], wins[msk], heroes[msk],
                                                  accounts[msk], teams[msk], pst[msk])
    n = len(ts)
    print(f"карт: {n:,}" + (f" (обрезка по ts < {CUTOFF})" if CUTOFF else ""), flush=True)

    # Sorted dense indices replace millions of Python dictionaries/deques.
    # Pair keys are lexicographic (account, category), just like sorted(tuple).
    acc_ids = np.unique(accounts[accounts > 0])
    slots = np.searchsorted(acc_ids, accounts).astype(np.int32)
    slots[accounts <= 0] = -1
    ah, hero_slots = _cell_index(slots, heroes)
    positions = np.broadcast_to(np.tile(np.arange(1, 6), 2), accounts.shape)
    ap, pos_slots = _cell_index(slots, positions)
    count = len(acc_ids)
    rating = np.full(count, 1500.0)
    games = np.zeros(count, dtype=np.int64)
    opp_sum = np.zeros(count)
    # gpm/imp residual sums, count, EWMA sum/weight/timestamp.
    residual = np.zeros((count, 6))
    hero_g = np.zeros(len(ah), dtype=np.int64)
    pos_g = np.zeros(len(ap), dtype=np.int64)
    gpm_hero = np.zeros(len(ah))
    res_hero_lh = np.zeros((len(ah), 2))
    hero_all_gpm: dict[int, list] = defaultdict(lambda: [0.0, 0])
    # One linked occurrence per player slot; bounded tails are reconstructed
    # only at output. No per-account empty deque or stored Python float.
    previous = np.full(accounts.size, -1, dtype=np.int32)
    latest = np.full(count, -1, dtype=np.int32)
    res30 = np.zeros((accounts.size, 2))
    has_residual = np.zeros(accounts.size, dtype=bool)
    pos_sum = np.zeros((6, 12)); pos_cnt = np.zeros(6)
    hero_norm: dict[int, np.ndarray] = defaultdict(lambda: np.zeros(12))
    hero_cnt: dict[int, float] = defaultdict(float)
    ev30 = deque(); w30 = defaultdict(float); g30 = defaultdict(float)
    vs: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0, 0])
    h2h: dict[tuple, list] = defaultdict(lambda: [0.0, 0])
    rating_team: dict[int, float] = {}
    lam_p = math.log(2) / (HL_PAIR * 86400.0)
    lam_e = math.log(2) / (HL_EWMA * 86400.0)

    for i in range(n):
        now = int(ts[i])
        while ev30 and ev30[0][0] < now - 30 * 86400:
            _t, h, o = ev30.popleft(); g30[h] -= 1; w30[h] -= o
        won_r = bool(wins[i])
        mr = []
        for side in range(2):
            known = slots[i, side*5:(side+1)*5]
            known = known[known >= 0]
            mr.append(float(np.mean(rating[known])) if len(known) else 1500.0)
        exp_r = 1.0 / (1.0 + 10 ** ((mr[1] - mr[0]) / 400.0))
        rt, dt = int(teams[i, 0]), int(teams[i, 1])
        for s in range(2):
            won = won_r if s == 0 else not won_r
            expected = exp_r if s == 0 else 1.0 - exp_r
            opp_elo = mr[1 - s]
            hs = [int(h) for h in heroes[i, s*5:(s+1)*5]]
            for p in range(5):
                a, h = int(accounts[i, s*5+p]), hs[p]
                st = pst[i, s*5+p]
                if a > 0:
                    j = int(slots[i, s*5+p])
                    hj = int(hero_slots[i, s*5+p])
                    pj = int(pos_slots[i, s*5+p])
                    event = i*10 + s*5+p
                    rating[j] += K * (float(won) - expected)
                    games[j] += 1
                    hero_g[hj] += 1
                    pos_g[pj] += 1
                    opp_sum[j] += opp_elo
                    previous[event] = latest[j]
                    latest[j] = event
                    gpm_hero[hj] += float(st[P_GPM])
                    if pos_cnt[p+1] > 0:
                        pn = pos_sum[p+1] / pos_cnt[p+1]
                        r_gpm = float(st[P_GPM]) - pn[P_GPM]
                        r_imp = float(st[P_IMP]) - pn[P_IMP]
                        residual[j, 0] += r_gpm
                        residual[j, 1] += r_imp
                        residual[j, 2] += 1
                        res30[event, 0] = r_imp
                        res30[event, 1] = float(st[P_LH]) - pn[P_LH]
                        has_residual[event] = True
                        sm, wt, when = residual[j, 3:6]
                        f = math.exp(-lam_e * (now - when)) if wt > 0 else 0.0
                        residual[j, 3:6] = (sm*f + r_gpm, wt*f + 1.0, now)
                    if hero_cnt[h] > 0:
                        hn = hero_norm[h] / hero_cnt[h]
                        res_hero_lh[hj, 0] += float(st[P_LH]) - hn[P_LH]
                        res_hero_lh[hj, 1] += 1
                ca = hero_all_gpm[h]; ca[0] += float(st[P_GPM]); ca[1] += 1
                pos_sum[p+1] += st[:12]; pos_cnt[p+1] += 1
                hero_norm[h] += st[:12]; hero_cnt[h] += 1
                ev30.append((now, h, int(won))); w30[h] += int(won); g30[h] += 1
            other = [int(x) for x in heroes[i, (1-s)*5:(2-s)*5]]
            for x in hs:
                for y_ in other:
                    c = vs[(x, y_)]
                    f = math.exp(-lam_p * (now - c[2])) if c[1] > 0 else 0.0
                    vs[(x, y_)] = [c[0]*f + int(won), c[1]*f + 1.0, now]
        if rt > 0 and dt > 0:
            er = 1.0/(1.0 + 10 ** ((rating_team.get(dt,1500.0)-rating_team.get(rt,1500.0))/400.0))
            key = (min(rt, dt), max(rt, dt)); sgn = 1.0 if rt < dt else -1.0
            h2h[key][0] += sgn * ((1.0 if won_r else 0.0) - er); h2h[key][1] += 1
            rating_team[rt] = rating_team.get(rt,1500.0) + K*((1.0 if won_r else 0.0) - er)
            rating_team[dt] = rating_team.get(dt,1500.0) + K*((0.0 if won_r else 1.0) - (1-er))
        if (i+1) % 100_000 == 0: print(f"  {i+1:,}/{n:,}", flush=True)

    # Fill outputs directly; never materialise millions of Python row lists.
    acc_arr = np.empty((count, 12), dtype=np.float64)
    extra_arr = np.empty((count, 4), dtype=np.float64)
    pool_size = np.bincount(ah[:, 0], minlength=count)
    flat_stats = pst.reshape(-1, pst.shape[-1])
    for j, a in enumerate(acc_ids):
        tail = []
        event = int(latest[j])
        while event >= 0 and len(tail) < 50:
            tail.append(event)
            event = int(previous[event])
        tail = np.array(tail[::-1], dtype=np.int64)
        # np.mean sees exactly the legacy deque ordered float64 values.
        imp = flat_stats[tail, P_IMP].astype(np.float64)
        lh = flat_stats[tail[-30:], P_LH].astype(np.float64)
        form_tail = tail[-20:]
        outcomes = (wins[form_tail // 10] != 0).astype(np.int64)
        outcomes[form_tail % 10 >= 5] = 1 - outcomes[form_tail % 10 >= 5]
        # At most the first five position observations lack a residual;
        # a 50-appearance tail contains the last 30 valid residuals.
        valid_tail = tail[has_residual[tail]][-30:]
        r = residual[j]
        acc_arr[j] = (a, rating[j], games[j], opp_sum[j]/max(games[j], 1),
                      pool_size[j], float(np.mean(outcomes)), float(np.mean(imp)),
                      float(np.mean(imp[-30:])), r[0]/max(r[2], 1),
                      r[1]/max(r[2], 1), r[3]/r[4] if r[4] > 0 else 0.0,
                      float(np.mean(lh)))
        extra_arr[j] = (a, float(np.mean(imp[-10:])),
                        float(np.mean(res30[valid_tail, 0])) if len(valid_tail) else 0.0,
                        float(np.mean(res30[valid_tail, 1])) if len(valid_tail) else 0.0)
    ah_arr = np.empty((len(ah), 5), dtype=np.float64)
    for j, (a, h) in enumerate(ah):
        h = int(h)
        ca = hero_all_gpm[h]
        ah_arr[j] = (acc_ids[a], h, hero_g[j],
                      gpm_hero[j]/hero_g[j] - ca[0]/max(ca[1], 1)
                      if hero_g[j] and ca[1] else 0.0,
                      res_hero_lh[j, 0]/max(res_hero_lh[j, 1], 1))
    ap_arr = np.empty((len(ap), 3), dtype=np.float64)
    ap_arr[:, 0] = acc_ids[ap[:, 0]]
    ap_arr[:, 1] = ap[:, 1]
    ap_arr[:, 2] = pos_g
    if not count:
        # Legacy list-to-array conversion yields shape (0,), not (0, width).
        acc_arr = extra_arr = ah_arr = ap_arr = np.empty(0, dtype=np.float64)
    hw = np.array([[h, (w30[h]+5.0)/(g30[h]+10.0)] for h in sorted(g30)], dtype=np.float64)
    tnow = int(ts.max())
    vs_arr = np.array([[k[0], k[1], v[0]*math.exp(-lam_p*(tnow-v[2])), v[1]*math.exp(-lam_p*(tnow-v[2]))]
                       for k, v in vs.items() if v[1] > 0], dtype=np.float64)
    h2h_arr = np.array([[k[0], k[1], v[0]/(v[1]+3.0)] for k, v in h2h.items()], dtype=np.float64)

    if CUTOFF or SNAPSHOT_ONLY:
        mus, sds, coefs, ints = [], [], [], []
        np.savez_compressed(OUT, accounts=acc_arr, acc_hero=ah_arr, acc_pos=ap_arr,
                            hero_wr30=hw, vs_pairs=vs_arr, h2h=h2h_arr, acc_extra=extra_arr,
                            mu=np.array(mus), sd=np.array(sds), coef=np.array(coefs),
                            intercept=np.array(ints), snapshot_ts=np.array([tnow]),
                            feature_names=np.array(FEATURES))
        print(f"\nснимок сохранён (без обучения весов): {OUT}")
        print(f"аккаунтов {len(acc_ids):,}, ячеек {len(ah):,}, матчапов {len(vs_arr):,}, "
              f"пар команд {len(h2h_arr):,}; веса НЕ обучались (берутся из боевого артефакта)")
        return

    # Предохранитель рассинхрона. Кэши признаков адресуются ПОЗИЦИЕЙ в массиве,
    # а не `mid`, поэтому любой рост корпуса делает маску `keep` длиннее кэша, и
    # numpy падает с `IndexError: boolean index did not match` где-то в глубине.
    # Так уже дважды теряли время (14.08 и 15.08). Здесь ошибка называет числа и
    # средство сразу.
    _cache_rows = int(np.load(EXT)["F"].shape[0])
    if len(keep) != _cache_rows:
        raise SystemExit(
            f"РАССИНХРОН КОРПУСА И КЭШЕЙ: корпус {len(keep):,} карт, кэш признаков "
            f"{_cache_rows:,}. Переобучение весов на таком входе невозможно.\n"
            "Снимок для прода это НЕ задевает — он идёт веткой PREMATCH_SNAPSHOT_ONLY=1 "
            "и кэшей не читает.\n"
            "Чтобы переобучать веса, нужно пересобрать кэши признаков на текущем "
            "корпусе (ideas_batch*, EXT, драфт-логит) либо выровнять сборщики по `mid`.")

    base = np.load(EXT)["F"][keep]; lgt = np.load(ROOT/"runtime/artifacts/misc/pro_draft_logit_full.npz")["logit"]
    f1, f2 = np.load(B1)["F"], np.load(B2C)["F"]; f5, f5b = np.load(B5C)["F"], np.load(B5BC)["F"]
    LIVE8 = [x for x in LIVE if x != "strong_wr"]
    X = np.column_stack([lgt] + [base[:, EI[x]] for x in LIVE8]
        + [f1[:, B1_IDX["i3_hero_gpm_rel"]], f1[:, B1_IDX["i2_imp_recent"]],
           f2[:, B2NI["i14_wr30"]], f2[:, B2NI["i23_h2h_resid"]]]
        + [f5[:, B5NI[x]] for x in ("a_gpm_rel_pos","b_vs_wr","a_imp50","a_imp_rel_pos","a_lh_rel_hero")]
        + [f5b[:, B5BNI[x]] for x in ("v_gpm_ewma","v_lh30","v_imp30")])
    tmax = ts.max()
    mus, sds, coefs, ints = [], [], [], []
    for dd in (90, 180, 365, 730):
        mk = ts >= tmax - dd*86400
        if mk.sum() < 10000: continue
        mu, sd = X[mk].mean(0), X[mk].std(0)+1e-9
        mdl = LogisticRegression(C=1.0, max_iter=5000).fit((X[mk]-mu)/sd, wins[mk])
        mus.append(mu); sds.append(sd); coefs.append(mdl.coef_[0]); ints.append(mdl.intercept_[0])
    np.savez_compressed(OUT, accounts=acc_arr, acc_hero=ah_arr, acc_pos=ap_arr,
                        hero_wr30=hw, vs_pairs=vs_arr, h2h=h2h_arr, acc_extra=extra_arr,
                        mu=np.array(mus), sd=np.array(sds), coef=np.array(coefs),
                        intercept=np.array(ints), snapshot_ts=np.array([tnow]),
                        feature_names=np.array(FEATURES))
    SPEC.write_text(json.dumps({
        "features": FEATURES, "snapshot_ts": int(tnow), "models": len(mus),
        "accounts": len(acc_ids), "acc_hero_cells": len(ah), "vs_pairs": len(vs_arr),
        "h2h_pairs": len(h2h_arr), "heroes_with_wr30": len(hw),
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nсохранено: {OUT}")
    print(f"аккаунтов {len(acc_ids):,}, ячеек (аккаунт,герой) {len(ah):,}, "
          f"матчапов {len(vs_arr):,}, пар команд {len(h2h_arr):,}, моделей {len(mus)}")


if __name__ == "__main__":
    main()
