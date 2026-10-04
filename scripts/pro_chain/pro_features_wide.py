#!/usr/bin/env python3
"""Широкая партия предматчевых признаков: строим много, отбор доверяем модели.

Раньше широкую партию было нельзя: 2 323 обучающие карты на 8 признаков уже
были на грани подгонки. Теперь обучающих 191 тысяча, и можно кинуть два десятка
и посмотреть, что выживет — регуляризация раздаст веса сама.

Всё считается ОДНИМ хронологическим проходом и строго по прошлому: состояние
счётчиков выписывается до того, как в них попадает текущий матч.

Признаки (разница сторон, если не указано иначе):
   1 elo             рейтинг игроков (уже известно, что доминирует)
   2 games           сыграно про-матчей
   3 career_days     СТАЖ: дни от первого про-матча — не то же, что число игр
   4 winrate         пожизненный винрейт со шринкеджем
   5 hero_games      игр именно на этом герое
   6 hero_wr         винрейт на этом герое
   7 pos_games       игр на этой позиции
   8 tier1_games     игр против СИЛЬНЫХ (рейтинг соперника выше 1600)
   9 opp_elo         средний рейтинг соперников за карьеру
  10 hero_pool       широта пула: сколько разных героев играл
  11 rest_days       дней с прошлого матча
  12 cohesion        совместных игр пятёркой (с распадом)
  13 form            доля побед в последних 20
  14 elo_spread      разброс рейтингов внутри пятёрки
"""
from __future__ import annotations

import json
import math
import os
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression

ROOT = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
sys.path.insert(0, str(ROOT / "base"))
from draft_features import DraftFeatureEncoder, KIND_PAIR  # noqa: E402

COMPACT = ROOT / "runtime/artifacts/misc/pro_corpus_compact.npz"
PUB = ROOT / "runtime/artifacts/kills/window_model_v3/player_priors.npz"
TEST_FROM = 1774742400
K, HALF_LIFE, STRONG = 24.0, 180.0, 1600.0
NAMES = ["elo", "games", "career_days", "winrate", "hero_games", "hero_wr", "pos_games",
         "tier1_games", "opp_elo", "hero_pool", "rest_days", "cohesion", "form", "elo_spread"]


def auc(y, p) -> float:
    o = np.argsort(p)
    r = np.empty(len(p), dtype=np.float64)
    r[o] = np.arange(1, len(p) + 1)
    pos, neg = float(y.sum()), float(len(y) - y.sum())
    return float((r[y == 1].sum() - pos * (pos + 1) / 2) / (pos * neg)) if pos and neg else float("nan")


def build(ts, wins, accounts, heroes):
    n = len(ts)
    F = np.zeros((n, len(NAMES)), dtype=np.float64)
    rating: dict[int, float] = {}
    games: dict[int, int] = {}
    wins_by: dict[int, int] = {}
    first_seen: dict[int, int] = {}
    last_seen: dict[int, int] = {}
    hero_g: dict[tuple, int] = defaultdict(int)
    hero_w: dict[tuple, int] = defaultdict(int)
    pos_g: dict[tuple, int] = defaultdict(int)
    strong_g: dict[int, int] = defaultdict(int)
    opp_sum: dict[int, float] = defaultdict(float)
    pool: dict[int, set] = defaultdict(set)
    recent: dict[int, list] = defaultdict(list)
    coh: dict[tuple, list] = defaultdict(lambda: [0.0, 0])
    lam = math.log(2) / (HALF_LIFE * 86400.0)

    for i in range(n):
        now = int(ts[i])
        side_stats = []
        for s in range(2):
            accs = accounts[i, s * 5:(s + 1) * 5]
            hrs = heroes[i, s * 5:(s + 1) * 5]
            live = [(int(a), int(h), p) for p, (a, h) in enumerate(zip(accs, hrs)) if a > 0]
            if not live:
                side_stats.append([1500.0] + [0.0] * 13)
                continue
            el = [rating.get(a, 1500.0) for a, _h, _p in live]
            g = [games.get(a, 0) for a, _h, _p in live]
            side_stats.append([
                float(np.mean(el)),
                float(np.mean(g)),
                float(np.mean([(now - first_seen[a]) / 86400.0 if a in first_seen else 0.0
                               for a, _h, _p in live])),
                float(np.mean([(wins_by.get(a, 0) + 10.0) / (games.get(a, 0) + 20.0) for a, _h, _p in live])),
                float(np.mean([hero_g[(a, h)] for a, h, _p in live])),
                float(np.mean([(hero_w[(a, h)] + 3.0) / (hero_g[(a, h)] + 6.0) for a, h, _p in live])),
                float(np.mean([pos_g[(a, p)] for a, _h, p in live])),
                float(np.mean([strong_g[a] for a, _h, _p in live])),
                float(np.mean([opp_sum[a] / games[a] if games.get(a) else 1500.0 for a, _h, _p in live])),
                float(np.mean([len(pool[a]) for a, _h, _p in live])),
                float(np.mean([min((now - last_seen[a]) / 86400.0, 60.0) if a in last_seen else 0.0
                               for a, _h, _p in live])),
                0.0,   # cohesion — ниже
                float(np.mean([sum(recent[a]) / len(recent[a]) if recent[a] else 0.5
                               for a, _h, _p in live])),
                float(np.std(el)),
            ])
            key = tuple(sorted(a for a, _h, _p in live))
            if len(key) == 5:
                val, when = coh[key]
                side_stats[-1][11] = val * math.exp(-lam * (now - when)) if val else 0.0

        a_, b_ = side_stats
        for j in range(len(NAMES)):
            if j in (0, 8, 13):
                F[i, j] = (a_[j] - b_[j]) / 400.0 if j in (0, 8) else (a_[j] - b_[j]) / 100.0
            elif j == 3 or j == 5 or j == 12:
                F[i, j] = a_[j] - b_[j]
            else:
                F[i, j] = math.log1p(max(a_[j], 0)) - math.log1p(max(b_[j], 0))

        exp_r = 1.0 / (1.0 + 10 ** ((side_stats[1][0] - side_stats[0][0]) / 400.0))
        won_radiant = bool(wins[i])
        for s in range(2):
            accs = accounts[i, s * 5:(s + 1) * 5]
            hrs = heroes[i, s * 5:(s + 1) * 5]
            opp_elo = side_stats[1 - s][0]
            won = won_radiant if s == 0 else not won_radiant
            expected = exp_r if s == 0 else 1.0 - exp_r
            live = [(int(a), int(h), p) for p, (a, h) in enumerate(zip(accs, hrs)) if a > 0]
            for a, h, p in live:
                rating[a] = rating.get(a, 1500.0) + K * (float(won) - expected)
                games[a] = games.get(a, 0) + 1
                wins_by[a] = wins_by.get(a, 0) + int(won)
                first_seen.setdefault(a, now)
                last_seen[a] = now
                hero_g[(a, h)] += 1
                hero_w[(a, h)] += int(won)
                pos_g[(a, p)] += 1
                if opp_elo >= STRONG:
                    strong_g[a] += 1
                opp_sum[a] += opp_elo
                pool[a].add(h)
                r = recent[a]
                r.append(int(won))
                if len(r) > 20:
                    del r[0]
            key = tuple(sorted(a for a, _h, _p in live))
            if len(key) == 5:
                val, when = coh[key]
                coh[key] = [val * math.exp(-lam * (now - when)) + 1.0, now]
        if (i + 1) % 50_000 == 0:
            print(f"  {i+1:,}/{n:,}", flush=True)
    return F


def main() -> None:
    z = np.load(COMPACT)
    ts, wins, heroes, accounts = z["ts"], z["wins"].astype(int), z["heroes"], z["accounts"]
    print(f"про-матчей: {len(ts):,}", flush=True)
    F = build(ts, wins, accounts, heroes)
    test = ts >= TEST_FROM

    zp = np.load(PUB)
    N = len(zp["heroes"])
    take = slice(N - 2_000_000, N)
    enc = DraftFeatureEncoder.fit(zp["heroes"][take], KIND_PAIR, signed=True, pair_min_support=30)
    base = LogisticRegression(C=0.003, max_iter=1500, solver="lbfgs")
    base.fit(enc.transform(zp["heroes"][take]), zp["wins"][take].astype(int))
    p = base.predict_proba(enc.transform(heroes.astype(np.int64)))[:, 1]
    logit = np.log(np.clip(p, 1e-6, 1 - 1e-6) / np.clip(1 - p, 1e-6, 1 - 1e-6))

    X = np.column_stack([logit, F])
    mu, sd = X[~test].mean(0), X[~test].std(0) + 1e-9
    Xs = (X - mu) / sd
    m = LogisticRegression(C=1.0, max_iter=5000).fit(Xs[~test], wins[~test])
    full = auc(wins[test], m.predict_proba(Xs[test])[:, 1])
    print(f"\nобучение {int((~test).sum()):,}, проверка {int(test.sum()):,}")
    print(f"ВСЕ ПРИЗНАКИ: AUC {full:.4f}", flush=True)

    only_h = LogisticRegression(C=1.0, max_iter=3000).fit(Xs[~test][:, :1], wins[~test])
    print(f"только герои: AUC {auc(wins[test], only_h.predict_proba(Xs[test][:, :1])[:, 1]):.4f}")

    print(f"\n{'признак':14s} {'вес':>8s} {'AUC без него':>13s} {'вклад':>8s}")
    w = m.coef_[0]
    for j, name in enumerate(["draft_logit"] + NAMES):
        keep = [k for k in range(X.shape[1]) if k != j]
        mm = LogisticRegression(C=1.0, max_iter=3000).fit(Xs[~test][:, keep], wins[~test])
        a_wo = auc(wins[test], mm.predict_proba(Xs[test][:, keep])[:, 1])
        print(f"{name:14s} {w[j]:>+8.3f} {a_wo:>13.4f} {full - a_wo:>+8.4f}", flush=True)


if __name__ == "__main__":
    main()
