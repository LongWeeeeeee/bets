#!/usr/bin/env python3
"""Сборка боевого артефакта предматчевой модели: снимок состояния + веса.

Модель считает 20 признаков из АГРЕГАТОВ по прошлым матчам. В бою корпуса нет,
поэтому нужен снимок: account -> его величины, (account, hero) -> величины на
герое, hero -> винрейт за 30 дней, (hero, hero) -> матчапы, (team, team) ->
остаток личных встреч.

Здесь один хронологический проход по корпусу выписывает КОНЕЧНОЕ состояние всех
счётчиков и обучает ансамбль окон (90/180/365/730 дней), как в E-98. На выходе
один npz: снимок + четыре набора весов со своими mu/sd.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/build_prematch_artifact.py
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

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
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


def main() -> None:
    zc, zr = np.load(COMPACT), np.load(RICH)
    pos_ = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    keep = np.array([int(m) in pos_ for m in zc["mids"].tolist()])
    idx_r = np.array([pos_[int(m)] for m in zc["mids"][keep].tolist()])
    ts, wins = zc["ts"][keep], zc["wins"][keep].astype(int)
    heroes, accounts, teams = zc["heroes"][keep], zc["accounts"][keep], zc["teams"][keep]
    pst = zr["pstats"][idx_r]
    if CUTOFF:
        msk = ts < CUTOFF
        ts, wins, heroes, accounts, teams, pst = (ts[msk], wins[msk], heroes[msk],
                                                  accounts[msk], teams[msk], pst[msk])
    n = len(ts)
    print(f"карт: {n:,}" + (f" (обрезка по ts < {CUTOFF})" if CUTOFF else ""), flush=True)

    rating: dict[int, float] = {}
    games: dict[int, int] = defaultdict(int)
    hero_g: dict[tuple, int] = defaultdict(int)
    pos_g: dict[tuple, int] = defaultdict(int)
    opp_sum: dict[int, float] = defaultdict(float)
    pool: dict[int, set] = defaultdict(set)
    recent: dict[int, deque] = defaultdict(lambda: deque(maxlen=20))
    imp_q: dict[int, deque] = defaultdict(lambda: deque(maxlen=50))
    gpm_hero: dict[tuple, list] = defaultdict(lambda: [0.0, 0])
    hero_all_gpm: dict[int, list] = defaultdict(lambda: [0.0, 0])
    res_pos: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0])       # (acc,stat)
    res_hero_lh: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0])   # (acc,hero) добивания
    ew_gpm: dict[int, list] = defaultdict(lambda: [0.0, 0.0, 0])
    lh_q: dict[int, deque] = defaultdict(lambda: deque(maxlen=30))
    # E-177: три величины, которых снимку не хватало, из-за чего скорер считал
    # НЕ ТО, чему училась модель. Определения взяты один в один из обучающих
    # источников: окно 10 матчей у `ideas_batch1.imp_h`, остаток против нормы
    # позиции у `ideas_batch5b` (`r = st[k] - pn[k]`, окно 30).
    res30_imp: dict[int, deque] = defaultdict(lambda: deque(maxlen=30))
    res30_lh: dict[int, deque] = defaultdict(lambda: deque(maxlen=30))
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
        mr = [float(np.mean([rating.get(int(a), 1500.0) for a in accounts[i, s*5:(s+1)*5] if a > 0])
                    or 1500.0) for s in range(2)]
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
                    rating[a] = rating.get(a, 1500.0) + K * (float(won) - expected)
                    games[a] += 1
                    hero_g[(a, h)] += 1
                    pos_g[(a, p+1)] += 1
                    opp_sum[a] += opp_elo
                    pool[a].add(h)
                    recent[a].append(int(won))
                    imp_q[a].append(float(st[P_IMP]))
                    lh_q[a].append(float(st[P_LH]))
                    c = gpm_hero[(a, h)]; c[0] += float(st[P_GPM]); c[1] += 1
                    if pos_cnt[p+1] > 0:
                        pn = pos_sum[p+1] / pos_cnt[p+1]
                        for k2 in (P_GPM, P_IMP):
                            cc = res_pos[(a, k2)]; cc[0] += float(st[k2]) - pn[k2]; cc[1] += 1
                        res30_imp[a].append(float(st[P_IMP]) - pn[P_IMP])
                        res30_lh[a].append(float(st[P_LH]) - pn[P_LH])
                        sm, wt, when = ew_gpm[a]
                        f = math.exp(-lam_e * (now - when)) if wt > 0 else 0.0
                        ew_gpm[a] = [sm*f + (float(st[P_GPM]) - pn[P_GPM]), wt*f + 1.0, now]
                    if hero_cnt[h] > 0:
                        hn = hero_norm[h] / hero_cnt[h]
                        cc = res_hero_lh[(a, h)]; cc[0] += float(st[P_LH]) - hn[P_LH]; cc[1] += 1
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

    m = lambda q: float(np.mean(q)) if len(q) else 0.0
    acc_ids = sorted(games)
    acc_arr = np.array([[a, rating.get(a,1500.0), games[a], opp_sum[a]/max(games[a],1),
                         len(pool[a]), m(recent[a]), m(imp_q[a]), m(list(imp_q[a])[-30:]),
                         res_pos[(a,P_GPM)][0]/max(res_pos[(a,P_GPM)][1],1),
                         res_pos[(a,P_IMP)][0]/max(res_pos[(a,P_IMP)][1],1),
                         (ew_gpm[a][0]/ew_gpm[a][1] if ew_gpm[a][1] > 0 else 0.0),
                         m(lh_q[a])] for a in acc_ids], dtype=np.float64)
    extra_arr = np.array([[a, m(list(imp_q[a])[-10:]), m(res30_imp[a]), m(res30_lh[a])]
                          for a in acc_ids], dtype=np.float64)
    ah = sorted(hero_g)
    ah_arr = np.array([[k[0], k[1], hero_g[k],
                        (gpm_hero[k][0]/gpm_hero[k][1] - hero_all_gpm[k[1]][0]/max(hero_all_gpm[k[1]][1],1))
                        if gpm_hero[k][1] and hero_all_gpm[k[1]][1] else 0.0,
                        res_hero_lh[k][0]/max(res_hero_lh[k][1],1)] for k in ah], dtype=np.float64)
    ap = sorted(pos_g)
    ap_arr = np.array([[k[0], k[1], pos_g[k]] for k in ap], dtype=np.float64)
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
