#!/usr/bin/env python3
"""Партия 5: добор по правилу E-93, а не по порядку файла.

Правило, выведенное из четырёх партий: работают ровно два типа признаков —
(1) КАЧЕСТВО ИГРЫ вместо её исхода и (2) ИСХОДЫ СУЩНОСТЕЙ, которых нет в
обучении драфт-модели. Из каждого типа мы взяли по одному-двум признакам и на
этом остановились. Здесь обе ветки достраиваются целиком.

Ветка A — качество игры. `hero_gpm_rel` и `imp_recent` сработали, но это две
точки из большого семейства. Добираются: те же остатки для xpm, последних
хитов, урона по героям и нетворта; ВТОРАЯ база сравнения — норма ПОЗИЦИИ, а не
героя (норма позиции устойчивее); окна IMP 5 и 50 вместо одного 10; IMP на этом
герое; IMP команды за последние матчи.

Ветка B — исходы чужих сущностей. `wr30` (винрейт героев в про за 30 дней)
сработал, потому что драфт-логит обучен на паблике. Добираются: калибровка окна
(7 / 30 / 90 / вся история), винрейт ЯЧЕЙКИ (герой, позиция), винрейт ПАР внутри
команды и МАТЧАПОВ между командами в про (с распадом, полураспад 45 дней) —
это ровно те величины, которые словари считают на паблике, а тут они по про, —
и уровень лиги (средний рейтинг участников).

Запуск: venv_catboost/bin/python3 scripts/pro_chain/ideas_batch5.py
Выход:  runtime/artifacts/misc/ideas_batch5.md
"""
from __future__ import annotations

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
from ideas_batch2 import B1, B1_IDX, B1_NAMES, COMPACT, EI, EXT, LIVE, LOGIT, RICH, TEST_FROM  # noqa: E402
from ideas_batch2 import CACHE as B2C, NI as B2NI  # noqa: E402
from pro_features_wide import auc  # noqa: E402

CACHE = ROOT / "runtime/artifacts/misc/ideas_batch5.npz"
OUT = ROOT / "runtime/artifacts/misc/ideas_batch5.md"
B2_KEEP = ["i14_wr30", "i23_h2h_resid"]
K, HL_PAIR = 24.0, 45.0
P_KILL, P_DEATH, P_ASSIST, P_GPM, P_XPM, P_NW, P_LH, P_DN, P_HD, P_TD, P_HEAL, P_IMP = range(12)

NEW = [
    ("a_gpm_rel_pos", "A", "raw100"), ("a_xpm_rel_pos", "A", "raw100"),
    ("a_lh_rel_pos", "A", "raw100"), ("a_nw_rel_pos", "A", "raw1k"),
    ("a_imp_rel_pos", "A", "raw"), ("a_hdmg_rel_pos", "A", "raw1k"),
    ("a_xpm_rel_hero", "A", "raw100"), ("a_lh_rel_hero", "A", "raw100"),
    ("a_hdmg_rel_hero", "A", "raw1k"),
    ("a_imp5", "A", "raw"), ("a_imp50", "A", "raw"), ("a_imp_hero", "A", "raw"),
    ("a_team_imp", "A", "raw"),
    ("b_wr7", "B", "raw"), ("b_wr90", "B", "raw"), ("b_wr_all", "B", "raw"),
    ("b_wr30_pos", "B", "raw"), ("b_pair_wr", "B", "raw"), ("b_vs_wr", "B", "raw"),
    ("b_league_elo", "B", "raw400"),
]
NAMES = [n for n, _g, _s in NEW]
NI = {n: i for i, n in enumerate(NAMES)}


def build(ts, wins, accounts, heroes, teams, leagues, pstats):
    n = len(ts)
    F = np.zeros((n, len(NAMES)), dtype=np.float64)
    rating: dict[int, float] = {}
    pos_norm = np.zeros((6, 12))                     # позиция -> суммы показателей
    pos_cnt = np.zeros(6)
    hero_norm: dict[int, np.ndarray] = defaultdict(lambda: np.zeros(12))
    hero_cnt: dict[int, float] = defaultdict(float)
    res_pos: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0])     # (acc,stat) -> сумма, n
    res_hero: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0])    # (acc,hero,stat)
    imp_h: dict[int, list] = defaultdict(list)
    imp_hero: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0])
    team_imp: dict[int, list] = defaultdict(list)
    lg: dict[int, list] = defaultdict(lambda: [0.0, 0.0])
    ev7, ev30, ev90 = deque(), deque(), deque()
    w7 = defaultdict(float); g7 = defaultdict(float)
    w30 = defaultdict(float); g30 = defaultdict(float)
    w90 = defaultdict(float); g90 = defaultdict(float)
    wc30 = defaultdict(float); gc30 = defaultdict(float)              # ячейка (герой, позиция)
    wall = defaultdict(float); gall = defaultdict(float)
    pair: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0, 0])      # победы, игры, время
    vs: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0, 0])
    lam = math.log(2) / (HL_PAIR * 86400.0)
    POS_STATS = [(P_GPM, "a_gpm_rel_pos"), (P_XPM, "a_xpm_rel_pos"), (P_LH, "a_lh_rel_pos"),
                 (P_NW, "a_nw_rel_pos"), (P_IMP, "a_imp_rel_pos"), (P_HD, "a_hdmg_rel_pos")]
    HERO_STATS = [(P_XPM, "a_xpm_rel_hero"), (P_LH, "a_lh_rel_hero"), (P_HD, "a_hdmg_rel_hero")]

    def sh(w, g, k=10.0):
        return (w + 0.5 * k) / (g + k)

    def dec(cell, now):
        w, g, t0 = cell
        f = math.exp(-lam * (now - t0)) if g > 0 else 0.0
        return w * f, g * f

    for i in range(n):
        now = int(ts[i])
        for ev, wd, gd, days in ((ev7, w7, g7, 7), (ev30, w30, g30, 30), (ev90, w90, g90, 90)):
            while ev and ev[0][0] < now - days * 86400:
                _t, h, p, o = ev.popleft()
                gd[h] -= 1
                wd[h] -= o
                if days == 30:
                    gc30[(h, p)] -= 1
                    wc30[(h, p)] -= o
        side = []
        for s in range(2):
            accs = [int(a) for a in accounts[i, s * 5:(s + 1) * 5]]
            hrs = [int(h) for h in heroes[i, s * 5:(s + 1) * 5]]
            tid = int(teams[i, s])
            row = [0.0] * len(NAMES)
            live = [(a, h, p) for p, (a, h) in enumerate(zip(accs, hrs)) if a > 0]
            if live:
                for st, nm in POS_STATS:
                    row[NI[nm]] = float(np.mean([
                        res_pos[(a, st)][0] / res_pos[(a, st)][1] if res_pos[(a, st)][1] else 0.0
                        for a, _h, _p in live]))
                for st, nm in HERO_STATS:
                    row[NI[nm]] = float(np.mean([
                        res_hero[(a, h, st)][0] / res_hero[(a, h, st)][1]
                        if res_hero[(a, h, st)][1] else 0.0 for a, h, _p in live]))
                row[NI["a_imp5"]] = float(np.mean([
                    float(np.mean(imp_h[a][-5:])) if imp_h[a] else 0.0 for a, _h, _p in live]))
                row[NI["a_imp50"]] = float(np.mean([
                    float(np.mean(imp_h[a])) if imp_h[a] else 0.0 for a, _h, _p in live]))
                row[NI["a_imp_hero"]] = float(np.mean([
                    imp_hero[(a, h)][0] / imp_hero[(a, h)][1] if imp_hero[(a, h)][1] else 0.0
                    for a, h, _p in live]))
            if tid > 0 and team_imp[tid]:
                row[NI["a_team_imp"]] = float(np.mean(team_imp[tid]))
            row[NI["b_wr7"]] = float(np.mean([sh(w7[h], g7[h]) for h in hrs]))
            row[NI["b_wr90"]] = float(np.mean([sh(w90[h], g90[h], 20.0) for h in hrs]))
            row[NI["b_wr_all"]] = float(np.mean([sh(wall[h], gall[h], 50.0) for h in hrs]))
            row[NI["b_wr30_pos"]] = float(np.mean([sh(wc30[(h, p)], gc30[(h, p)])
                                                   for p, h in enumerate(hrs)]))
            pv = []
            for x, y_ in combinations(sorted(hrs), 2):
                w_, g_ = dec(pair[(x, y_)], now)
                pv.append(sh(w_, g_, 10.0))
            row[NI["b_pair_wr"]] = float(np.mean(pv)) if pv else 0.5
            other = [int(h) for h in heroes[i, (1 - s) * 5:(2 - s) * 5]]
            vv = []
            for x in hrs:
                for y_ in other:
                    w_, g_ = dec(vs[(x, y_)], now)
                    vv.append(sh(w_, g_, 10.0))
            row[NI["b_vs_wr"]] = float(np.mean(vv)) if vv else 0.5
            lid = int(leagues[i])
            row[NI["b_league_elo"]] = (lg[lid][0] / lg[lid][1]) if lg[lid][1] else 1500.0
            side.append(row)

        a_, b_ = side
        for j, (_nm, _g, scale) in enumerate(NEW):
            div = {"raw100": 100.0, "raw1k": 1000.0, "raw400": 400.0}.get(scale, 1.0)
            F[i, j] = (a_[j] - b_[j]) / div

        # ---- обновление
        won_r = bool(wins[i])
        m_r = [float(np.mean([rating.get(int(a), 1500.0) for a in accounts[i, s*5:(s+1)*5] if a > 0])
                     or 1500.0) for s in range(2)]
        exp_r = 1.0 / (1.0 + 10 ** ((m_r[1] - m_r[0]) / 400.0))
        lid = int(leagues[i])
        for s in range(2):
            accs = [int(a) for a in accounts[i, s * 5:(s + 1) * 5]]
            hrs = [int(h) for h in heroes[i, s * 5:(s + 1) * 5]]
            tid = int(teams[i, s])
            won = won_r if s == 0 else not won_r
            expected = exp_r if s == 0 else 1.0 - exp_r
            imps = []
            for p in range(5):
                a, h = accs[p], hrs[p]
                st = pstats[i, s * 5 + p][:12]
                pos = p + 1
                if a > 0:
                    rating[a] = rating.get(a, 1500.0) + K * (float(won) - expected)
                    if pos_cnt[pos] > 0:
                        pn = pos_norm[pos] / pos_cnt[pos]
                        for k2, _nm in POS_STATS:
                            c = res_pos[(a, k2)]
                            c[0] += float(st[k2]) - pn[k2]
                            c[1] += 1
                    if hero_cnt[h] > 0:
                        hn = hero_norm[h] / hero_cnt[h]
                        for k2, _nm in HERO_STATS:
                            c = res_hero[(a, h, k2)]
                            c[0] += float(st[k2]) - hn[k2]
                            c[1] += 1
                    ih = imp_h[a]
                    ih.append(float(st[P_IMP]))
                    if len(ih) > 50:
                        del ih[0]
                    c = imp_hero[(a, h)]
                    c[0] += float(st[P_IMP])
                    c[1] += 1
                    imps.append(float(st[P_IMP]))
                    if lid > 0:
                        lg[lid][0] += rating.get(a, 1500.0)
                        lg[lid][1] += 1
                pos_norm[pos] += st
                pos_cnt[pos] += 1
                hero_norm[h] += st
                hero_cnt[h] += 1
                o = int(won)
                ev7.append((now, h, pos - 1, o)); ev30.append((now, h, pos - 1, o))
                ev90.append((now, h, pos - 1, o))
                w7[h] += o; g7[h] += 1
                w30[h] += o; g30[h] += 1
                w90[h] += o; g90[h] += 1
                wall[h] += o; gall[h] += 1
                wc30[(h, pos - 1)] += o
                gc30[(h, pos - 1)] += 1
            if tid > 0 and imps:
                q = team_imp[tid]
                q.append(float(np.mean(imps)))
                if len(q) > 10:
                    del q[0]
            for x, y_ in combinations(sorted(hrs), 2):
                c = pair[(x, y_)]
                w_, g_ = dec(c, now)
                pair[(x, y_)] = [w_ + int(won), g_ + 1.0, now]
            other = [int(h) for h in heroes[i, (1 - s) * 5:(2 - s) * 5]]
            for x in hrs:
                for y_ in other:
                    c = vs[(x, y_)]
                    w_, g_ = dec(c, now)
                    vs[(x, y_)] = [w_ + int(won), g_ + 1.0, now]
        if (i + 1) % 100_000 == 0:
            print(f"  {i+1:,}/{n:,}", flush=True)
    return F


def fit(X, y_tr, train, test):
    mu, sd = X[train].mean(0), X[train].std(0) + 1e-9
    Xs = (X - mu) / sd
    m = LogisticRegression(C=1.0, max_iter=5000).fit(Xs[train], y_tr)
    return m.predict_proba(Xs[test])[:, 1]


def main() -> None:
    zc, zr = np.load(COMPACT), np.load(RICH)
    pos = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    keep = np.array([int(m) in pos for m in zc["mids"].tolist()])
    idx_r = np.array([pos[int(m)] for m in zc["mids"][keep].tolist()])
    ts, wins = zc["ts"][keep], zc["wins"][keep].astype(int)
    heroes, accounts, teams = zc["heroes"][keep], zc["accounts"][keep], zc["teams"][keep]
    leagues = zc["leagues"][keep]
    pstats = zr["pstats"][idx_r]
    base = np.load(EXT)["F"][keep]
    logit = np.load(LOGIT)["logit"][keep].reshape(-1, 1)
    b1 = np.load(B1)["F"][:, [B1_IDX[n] for n in B1_NAMES]]
    b2 = np.load(B2C)["F"][:, [B2NI[n] for n in B2_KEEP]]

    if CACHE.exists():
        F = np.load(CACHE)["F"]
        print("признаки партии 5 взяты из кэша", flush=True)
    else:
        F = build(ts, wins, accounts, heroes, teams, leagues, pstats)
        np.savez_compressed(CACHE, F=F)
        print(f"признаки сохранены: {CACHE}", flush=True)

    test, train = ts >= TEST_FROM, ts < TEST_FROM
    y, y_tr = wins[test], wins[train]
    G = np.column_stack([logit, base[:, [EI[n] for n in LIVE]], b1, b2])
    p_base = fit(G, y_tr, train, test)
    a_base = auc(y, p_base)

    L = ["# Партия 5: добор по правилу E-93", "",
         f"Проверка {int(test.sum()):,} карт. База — G плюс четыре выживших признака "
         f"(E-89/E-91): **AUC {a_base:.4f}**.", "",
         "## 1. По ветвям правила", "", "| ветвь | признаков | AUC | к базе |", "|---|---:|---|---|"]
    A = [NI[n] for n, g, _s in NEW if g == "A"]
    B = [NI[n] for n, g, _s in NEW if g == "B"]
    for nm, cols in (("A. качество игры (остатки по норме, окна IMP)", A),
                     ("B. исходы чужих сущностей (окна, ячейки, пары, матчапы, лига)", B)):
        a = auc(y, fit(np.column_stack([G, F[:, cols]]), y_tr, train, test))
        L.append(f"| {nm} | {len(cols)} | {a:.4f} | {a - a_base:+.4f} |")
    p_all = fit(np.column_stack([G, F]), y_tr, train, test)
    a_all = auc(y, p_all)
    L.append(f"| **обе вместе** | {len(NAMES)} | **{a_all:.4f}** | **{a_all - a_base:+.4f}** |")

    L += ["", "## 2. Вклад признака (leave-one-out)", "",
          "| признак | ветвь | покрытие | вклад |", "|---|---|---:|---:|"]
    full = np.column_stack([G, F])
    rows = []
    for j, (nm, g, _s) in enumerate(NEW):
        cols = [k for k in range(full.shape[1]) if k != G.shape[1] + j]
        a_wo = auc(y, fit(full[:, cols], y_tr, train, test))
        rows.append((nm, g, float(np.mean(np.abs(F[:, j]) > 1e-9)), a_all - a_wo))
    for nm, g, cov, d in sorted(rows, key=lambda r: -r[3]):
        L.append(f"| {nm} | {g} | {cov:.1%} | {d:+.4f} |")

    top = [NI[nm] for nm, _g, _c, d in sorted(rows, key=lambda r: -r[3])[:5] if d > 0]
    if top:
        p_top = fit(np.column_stack([G, F[:, top]]), y_tr, train, test)
        a_top = auc(y, p_top)
        rng = np.random.default_rng(20260812)
        d = []
        for _ in range(500):
            ix = rng.integers(0, len(y), len(y))
            if len(np.unique(y[ix])) > 1:
                d.append(auc(y[ix], p_top[ix]) - auc(y[ix], p_base[ix]))
        d = np.asarray(d)
        L += ["", "## 3. Обрезка до лучших и бутстрап", "",
              f"- лучшие {len(top)}: **{a_top:.4f}** ({a_top - a_base:+.4f} к базе)",
              f"- 500 пересборок: среднее {d.mean():+.4f}, 95% ДИ "
              f"[{np.percentile(d, 2.5):+.4f}, {np.percentile(d, 97.5):+.4f}], "
              f"за набор {float((d > 0).mean()):.1%}"]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L), flush=True)
    print(f"\nотчёт: {OUT}", flush=True)


if __name__ == "__main__":
    main()
