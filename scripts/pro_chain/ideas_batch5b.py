#!/usr/bin/env python3
"""Партия 5б: вариации вокруг победителя — остатка по норме ПОЗИЦИИ.

`a_gpm_rel_pos` дал +0.0041 — больше любого одиночного признака за всю серию.
Это «насколько игрок фармит лучше нормы своей позиции», усреднённое по пятёрке и
по всей карьере. Три ручки у него не проверены ни разу:

  1. ОКНО. Сейчас это среднее за карьеру. У `imp` окно решало (50 лучше 5), у
     `wr30` окно решало тоже. Проверяются последние 10, последние 30 и версия с
     затуханием (полураспад 90 дней).
  2. АГРЕГАЦИЯ ПО ПЯТЁРКЕ. Сейчас простое среднее. Проверяются: отдельно кор
     (pos1-3) и саппорты (pos4-5), слабейший и сильнейший в составе, и пять
     отдельных колонок по позициям — пусть модель сама раздаст веса.
  3. ПОКАЗАТЕЛЬ. Сейчас gpm. Проверяются последние хиты и IMP в той же
     конструкции с окном.

Запуск: venv_catboost/bin/python3 scripts/pro_chain/ideas_batch5b.py
Выход:  runtime/artifacts/misc/ideas_batch5b.md
"""
from __future__ import annotations

import math
import os
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression

ROOT = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ideas_batch2 import B1, B1_IDX, B1_NAMES, COMPACT, EI, EXT, LIVE, LOGIT, RICH, TEST_FROM  # noqa: E402
from ideas_batch2 import CACHE as B2C, NI as B2NI  # noqa: E402
from ideas_batch5 import CACHE as B5C, NI as B5NI  # noqa: E402
from pro_features_wide import auc  # noqa: E402

CACHE = ROOT / "runtime/artifacts/misc/ideas_batch5b.npz"
OUT = ROOT / "runtime/artifacts/misc/ideas_batch5b.md"
B5_KEEP = ["a_gpm_rel_pos", "b_vs_wr", "a_imp50", "a_imp_rel_pos", "a_lh_rel_hero"]
P_GPM, P_LH, P_IMP = 3, 6, 11
HL = 90.0

NEW = [
    ("v_gpm10", "окно"), ("v_gpm30", "окно"), ("v_gpm_ewma", "окно"),
    ("v_gpm_core", "агрегация"), ("v_gpm_supp", "агрегация"),
    ("v_gpm_min", "агрегация"), ("v_gpm_max", "агрегация"),
    ("v_gpm_p1", "позиции"), ("v_gpm_p2", "позиции"), ("v_gpm_p3", "позиции"),
    ("v_gpm_p4", "позиции"), ("v_gpm_p5", "позиции"),
    ("v_lh30", "показатель"), ("v_imp30", "показатель"),
]
NAMES = [n for n, _g in NEW]
NI = {n: i for i, n in enumerate(NAMES)}


def build(ts, wins, accounts, heroes, pstats):
    n = len(ts)
    F = np.zeros((n, len(NAMES)), dtype=np.float64)
    pos_sum = np.zeros((6, 12))
    pos_cnt = np.zeros(6)
    hist: dict[tuple, list] = defaultdict(list)              # (acc,stat) -> последние остатки
    ew: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0, 0])
    lam = math.log(2) / (HL * 86400.0)

    def m(xs):
        return float(sum(xs) / len(xs)) if xs else 0.0

    for i in range(n):
        now = int(ts[i])
        side = []
        for s in range(2):
            accs = [int(a) for a in accounts[i, s * 5:(s + 1) * 5]]
            row = [0.0] * len(NAMES)
            g10, g30, gew, per_pos = [], [], [], []
            for p, a in enumerate(accs):
                h = hist[(a, P_GPM)] if a > 0 else []
                v10, v30 = m(h[-10:]), m(h[-30:])
                g10.append(v10)
                g30.append(v30)
                sm, wt, when = ew[(a, P_GPM)]
                f = math.exp(-lam * (now - when)) if wt > 0 else 0.0
                gew.append((sm * f) / (wt * f) if wt * f > 1e-9 else 0.0)
                per_pos.append(v30)
            row[NI["v_gpm10"]] = m(g10)
            row[NI["v_gpm30"]] = m(g30)
            row[NI["v_gpm_ewma"]] = m(gew)
            row[NI["v_gpm_core"]] = m(per_pos[:3])
            row[NI["v_gpm_supp"]] = m(per_pos[3:])
            row[NI["v_gpm_min"]] = min(per_pos)
            row[NI["v_gpm_max"]] = max(per_pos)
            for p in range(5):
                row[NI[f"v_gpm_p{p+1}"]] = per_pos[p]
            row[NI["v_lh30"]] = m([m(hist[(a, P_LH)][-30:]) for a in accs if a > 0] or [0.0])
            row[NI["v_imp30"]] = m([m(hist[(a, P_IMP)][-30:]) for a in accs if a > 0] or [0.0])
            side.append(row)

        a_, b_ = side
        for j, (nm, _g) in enumerate(NEW):
            div = 1.0 if nm in ("v_imp30",) else 100.0
            F[i, j] = (a_[j] - b_[j]) / div

        for s in range(2):
            for p in range(5):
                a = int(accounts[i, s * 5 + p])
                st = pstats[i, s * 5 + p][:12]
                pos = p + 1
                if a > 0 and pos_cnt[pos] > 0:
                    pn = pos_sum[pos] / pos_cnt[pos]
                    for k in (P_GPM, P_LH, P_IMP):
                        r = float(st[k]) - pn[k]
                        q = hist[(a, k)]
                        q.append(r)
                        if len(q) > 30:
                            del q[0]
                        if k == P_GPM:
                            sm, wt, when = ew[(a, k)]
                            f = math.exp(-lam * (now - when)) if wt > 0 else 0.0
                            ew[(a, k)] = [sm * f + r, wt * f + 1.0, now]
                pos_sum[pos] += st
                pos_cnt[pos] += 1
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
    heroes, accounts = zc["heroes"][keep], zc["accounts"][keep]
    pstats = zr["pstats"][idx_r]
    base = np.load(EXT)["F"][keep]
    logit = np.load(LOGIT)["logit"][keep].reshape(-1, 1)
    b1 = np.load(B1)["F"][:, [B1_IDX[n] for n in B1_NAMES]]
    b2 = np.load(B2C)["F"][:, [B2NI[n] for n in ["i14_wr30", "i23_h2h_resid"]]]
    b5 = np.load(B5C)["F"][:, [B5NI[n] for n in B5_KEEP]]

    if CACHE.exists():
        F = np.load(CACHE)["F"]
        print("признаки 5б из кэша", flush=True)
    else:
        F = build(ts, wins, accounts, heroes, pstats)
        np.savez_compressed(CACHE, F=F)
        print(f"сохранено: {CACHE}", flush=True)

    test, train = ts >= TEST_FROM, ts < TEST_FROM
    y, y_tr = wins[test], wins[train]
    G = np.column_stack([logit, base[:, [EI[n] for n in LIVE]], b1, b2, b5])
    p_base = fit(G, y_tr, train, test)
    a_base = auc(y, p_base)

    L = ["# Партия 5б: ручки у остатка по норме позиции", "",
         f"Проверка {int(test.sum()):,} карт. База — всё выжившее, включая пятёрку E-94: "
         f"**AUC {a_base:.4f}**.", "",
         "| вариант | признаков | AUC | к базе |", "|---|---:|---|---|"]
    groups = {}
    for nm, g in NEW:
        groups.setdefault(g, []).append(NI[nm])
    for g, cols in groups.items():
        a = auc(y, fit(np.column_stack([G, F[:, cols]]), y_tr, train, test))
        L.append(f"| {g} | {len(cols)} | {a:.4f} | {a - a_base:+.4f} |")
    p_all = fit(np.column_stack([G, F]), y_tr, train, test)
    a_all = auc(y, p_all)
    L.append(f"| **все** | {len(NAMES)} | **{a_all:.4f}** | **{a_all - a_base:+.4f}** |")

    L += ["", "## Вклад признака (leave-one-out)", "", "| признак | группа | вклад |", "|---|---|---:|"]
    full = np.column_stack([G, F])
    rows = []
    for j, (nm, g) in enumerate(NEW):
        cols = [k for k in range(full.shape[1]) if k != G.shape[1] + j]
        rows.append((nm, g, a_all - auc(y, fit(full[:, cols], y_tr, train, test))))
    for nm, g, d in sorted(rows, key=lambda r: -r[2]):
        L.append(f"| {nm} | {g} | {d:+.4f} |")

    top = [NI[nm] for nm, _g, d in sorted(rows, key=lambda r: -r[2])[:4] if d > 0]
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
        L += ["", "## Обрезка и бутстрап", "",
              f"- лучшие {len(top)}: **{a_top:.4f}** ({a_top - a_base:+.4f})",
              f"- 500 пересборок: 95% ДИ [{np.percentile(d, 2.5):+.4f}, "
              f"{np.percentile(d, 97.5):+.4f}], за набор {float((d > 0).mean()):.1%}"]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L), flush=True)
    print(f"\nотчёт: {OUT}", flush=True)


if __name__ == "__main__":
    main()
