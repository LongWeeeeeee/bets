#!/usr/bin/env python3
"""Идеи 12-17, 20, 22-24, 26-27: стиль, мета, новизна, гибкость, ростер, встречи.

Вторая партия по файлу `ideas`. Пропущены с обоснованием: 6-9 — действующие
словари, 10-11 — отвергнуты (E-74, E-83), 18-19 и 25 — данных нет, 21 —
измерена в E-84 (ноль).

  12 стиль команды: длительность, темп килов, ранний нетворт, башни к 15-й,
     килы в минуту — по прошлым матчам ЭТОЙ команды;
  13 соответствие драфта стилю: поздний драфт у команды долгих игр, темповый —
     у быстрой, пушерский — у той, что рано берёт башни;
  14 мета патча: пикрейт и винрейт героев за 7 и 30 дней и их изменение;
  15 адаптация к мете: близость распределения героев команды к глобальному;
  16 новизна драфта: -log P(герой | история команды);
  17 гибкость: энтропия позиций, на которых играют этих героев;
  20 глубина пула: энтропия распределения героев игрока (эффективный размер);
  22 свежесть ростера: дней и матчей с последней смены, сколько игроков новые;
  23 личные встречи команд: остаток от ожидания по рейтингам, со шринкеджем;
  24 игрок против игрока: остаток по паре мидов (pos2 против pos2);
  26 серия: исход прошлой карты, счёт серии, перенос героев;
  27 усталость: матчей за 24 часа и за 3 дня.

Энтропии считаются инкрементально (H = log N - S/N, где S = сумма c*log c),
иначе десять пересчётов на матч по всему пулу игрока стоят полчаса.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/ideas_batch2.py
Выход:  runtime/artifacts/misc/ideas_batch2.md
"""
from __future__ import annotations

import json
import math
import os
import sys
from collections import defaultdict, deque
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
from pro_features_wide import auc  # noqa: E402

COMPACT = ROOT / "runtime/artifacts/misc/pro_corpus_compact.npz"
RICH = ROOT / "runtime/artifacts/misc/pro_corpus_rich.npz"
EXT = ROOT / "runtime/artifacts/misc/pro_features_ext.npz"
LOGIT = ROOT / "runtime/artifacts/misc/pro_draft_logit.npz"
B1 = ROOT / "runtime/artifacts/misc/ideas_batch1.npz"
HEROF = ROOT / "base/hero_features_processed.json"
CACHE = ROOT / "runtime/artifacts/misc/ideas_batch2.npz"
OUT = ROOT / "runtime/artifacts/misc/ideas_batch2.md"
# Срез теста. Переключается переменной среды, чтобы можно было собрать вторую,
# независимую точку на более раннем окне и отличить настоящий эффект от
# особенности одного окна. По умолчанию — прежнее значение, ничего не меняется.
TEST_FROM = int(os.getenv("TEST_FROM", "1774742400"))
K = 24.0

EXT_NAMES = ["elo", "games", "career_days", "winrate", "hero_games", "hero_wr", "pos_games",
             "tier1_games", "opp_elo", "hero_pool", "rest_days", "cohesion", "form", "elo_spread",
             "strong_wins", "strong_wr", "coh_pair_mean", "coh_pair_min", "coh_days_min",
             "coh_pair_share"]
EI = {n: i for i, n in enumerate(EXT_NAMES)}
LIVE = ["elo", "games", "hero_games", "pos_games", "opp_elo", "hero_pool", "form", "strong_wr"]
B1_NAMES = ["i3_hero_gpm_rel", "i2_imp_recent"]          # выжившие из партии 1 (E-89)
B1_IDX = {"i3_hero_gpm_rel": 15, "i2_imp_recent": 13}    # позиции в ideas_batch1.npz

NEW = [
    ("i12_dur", 12, "raw100"), ("i12_kills10", 12, "raw"), ("i12_nw10", 12, "raw1k"),
    ("i12_tow15", 12, "raw"), ("i12_kpm", 12, "raw"),
    ("i13_late_fit", 13, "raw100"), ("i13_tempo_fit", 13, "raw100"), ("i13_push_fit", 13, "raw100"),
    ("i14_pick7", 14, "raw"), ("i14_pick30", 14, "raw"), ("i14_wr7", 14, "raw"),
    ("i14_wr30", 14, "raw"), ("i14_dpick", 14, "raw"), ("i14_dwr", 14, "raw"),
    ("i15_meta_sim", 15, "raw"),
    ("i16_novelty", 16, "raw"),
    ("i17_pos_entropy", 17, "raw"),
    ("i20_pool_entropy", 20, "raw"),
    ("i22_roster_days", 22, "log"), ("i22_roster_games", 22, "log"), ("i22_changed", 22, "raw"),
    ("i23_h2h_resid", 23, "raw"),
    ("i24_mid_resid", 24, "raw"),
    ("i26_prev_map", 26, "raw"), ("i26_series_score", 26, "raw"), ("i26_hero_rep", 26, "raw"),
    ("i27_games24h", 27, "raw"), ("i27_games3d", 27, "raw"),
]
NAMES = [n for n, _i, _s in NEW]
NI = {n: i for i, n in enumerate(NAMES)}


def hero_style():
    d = json.load(open(HEROF))
    out = {}
    for k, v in d.items():
        try:
            hid = int(v.get("hero_id") or k)
        except (TypeError, ValueError):
            continue
        out[hid] = (float(v.get("late_score") or 0.0), float(v.get("tempo_score") or 0.0),
                    float(v.get("push_score") or 0.0))
    return out


def build(ts, wins, accounts, heroes, teams, rich, idx_r):
    n = len(ts)
    F = np.zeros((n, len(NAMES)), dtype=np.float64)
    style = hero_style()
    dur_a, nw_a, rk_a, dk_a, tow_a = (rich["durations"][idx_r], rich["nw"][idx_r],
                                      rich["rk"][idx_r], rich["dk"][idx_r], rich["towers"][idx_r])
    sid_a, styp_a = rich["sids"][idx_r], rich["stypes"][idx_r]

    tstat: dict[int, list] = defaultdict(lambda: [0.0] * 6)   # dur, kills10, nw10, tow15, kpm, n
    rating_team: dict[int, float] = {}
    th: dict[tuple, int] = defaultdict(int)                   # (team,hero) -> игр
    t_total: dict[int, int] = defaultdict(int)
    t_S: dict[int, float] = defaultdict(float)                # для энтропии пула команды
    p_pick: dict[int, dict] = defaultdict(dict)
    p_tot: dict[int, int] = defaultdict(int)
    p_S: dict[int, float] = defaultdict(float)
    hpos: dict[int, list] = defaultdict(lambda: [0.0] * 5)
    h2h: dict[tuple, list] = defaultdict(lambda: [0.0, 0])    # остаток, игр
    mid2: dict[tuple, list] = defaultdict(lambda: [0.0, 0])
    roster_last: dict[int, tuple] = {}
    roster_since: dict[int, list] = {}                        # team -> [ts, games]
    tmatches: dict[int, deque] = defaultdict(deque)
    ser_hist: dict[int, list] = defaultdict(list)
    ev = deque()                                              # (ts, hero, won) для меты
    pick_c = defaultdict(int)
    win_c = defaultdict(int)
    pick7 = defaultdict(int)
    win7 = defaultdict(int)
    ev7 = deque()
    tot30 = [0]
    tot7 = [0]

    def ent(S, N):
        return (math.log(N) - S / N) if N > 0 else 0.0

    for i in range(n):
        now = int(ts[i])
        # окна меты
        while ev and ev[0][0] < now - 30 * 86400:
            t0, h0, w0 = ev.popleft()
            pick_c[h0] -= 1
            win_c[h0] -= w0
            tot30[0] -= 1
        while ev7 and ev7[0][0] < now - 7 * 86400:
            t0, h0, w0 = ev7.popleft()
            pick7[h0] -= 1
            win7[h0] -= w0
            tot7[0] -= 1

        side = []
        for s in range(2):
            accs = accounts[i, s * 5:(s + 1) * 5]
            hrs = [int(x) for x in heroes[i, s * 5:(s + 1) * 5]]
            tid = int(teams[i, s])
            row = [0.0] * len(NAMES)
            st = tstat[tid] if tid > 0 else None
            if st and st[5] > 0:
                c = st[5]
                row[NI["i12_dur"]] = st[0] / c
                row[NI["i12_kills10"]] = st[1] / c
                row[NI["i12_nw10"]] = st[2] / c
                row[NI["i12_tow15"]] = st[3] / c
                row[NI["i12_kpm"]] = st[4] / c
            d_late = float(np.mean([style.get(h, (0, 0, 0))[0] for h in hrs]))
            d_tempo = float(np.mean([style.get(h, (0, 0, 0))[1] for h in hrs]))
            d_push = float(np.mean([style.get(h, (0, 0, 0))[2] for h in hrs]))
            row[NI["i13_late_fit"]] = d_late * row[NI["i12_dur"]] / 60.0
            row[NI["i13_tempo_fit"]] = d_tempo * row[NI["i12_kills10"]]
            row[NI["i13_push_fit"]] = d_push * row[NI["i12_tow15"]]
            if tot30[0] > 0:
                row[NI["i14_pick30"]] = float(np.mean([pick_c[h] for h in hrs])) / tot30[0] * 100.0
                row[NI["i14_wr30"]] = float(np.mean([(win_c[h] + 5.0) / (pick_c[h] + 10.0) for h in hrs]))
            else:
                row[NI["i14_wr30"]] = 0.5
            if tot7[0] > 0:
                row[NI["i14_pick7"]] = float(np.mean([pick7[h] for h in hrs])) / tot7[0] * 100.0
                row[NI["i14_wr7"]] = float(np.mean([(win7[h] + 5.0) / (pick7[h] + 10.0) for h in hrs]))
            else:
                row[NI["i14_wr7"]] = 0.5
            row[NI["i14_dpick"]] = row[NI["i14_pick7"]] - row[NI["i14_pick30"]]
            row[NI["i14_dwr"]] = row[NI["i14_wr7"]] - row[NI["i14_wr30"]]
            if tid > 0 and t_total[tid] > 0 and tot30[0] > 0:
                num = sum(th[(tid, h)] / t_total[tid] * (pick_c[h] / tot30[0]) for h in set(hrs))
                row[NI["i15_meta_sim"]] = num * 100.0
                row[NI["i16_novelty"]] = -float(np.mean(
                    [math.log((th[(tid, h)] + 1.0) / (t_total[tid] + 127.0)) for h in hrs]))
            row[NI["i17_pos_entropy"]] = float(np.mean([
                ent(sum(c * math.log(c) for c in hpos[h] if c > 0), sum(hpos[h])) for h in hrs]))
            row[NI["i20_pool_entropy"]] = float(np.mean([
                ent(p_S[int(a)], p_tot[int(a)]) for a in accs if a > 0] or [0.0]))
            if tid > 0 and tid in roster_since:
                row[NI["i22_roster_days"]] = (now - roster_since[tid][0]) / 86400.0
                row[NI["i22_roster_games"]] = float(roster_since[tid][1])
            cur = frozenset(int(a) for a in accs if a > 0)
            if tid > 0 and tid in roster_last:
                row[NI["i22_changed"]] = float(len(cur - roster_last[tid]))
            if tid > 0:
                q = tmatches[tid]
                row[NI["i27_games24h"]] = float(sum(1 for t in q if t >= now - 86400))
                row[NI["i27_games3d"]] = float(sum(1 for t in q if t >= now - 3 * 86400))
            side.append(row)

        rt, dt = int(teams[i, 0]), int(teams[i, 1])
        if rt > 0 and dt > 0:
            key = (min(rt, dt), max(rt, dt))
            r_, c_ = h2h[key]
            val = (r_ / (c_ + 3.0)) if c_ else 0.0
            side[0][NI["i23_h2h_resid"]] = val if rt < dt else -val
            side[1][NI["i23_h2h_resid"]] = -side[0][NI["i23_h2h_resid"]]
        a2, d2 = int(accounts[i, 1]), int(accounts[i, 6])      # pos2 обеих сторон
        if a2 > 0 and d2 > 0:
            key = (min(a2, d2), max(a2, d2))
            r_, c_ = mid2[key]
            val = (r_ / (c_ + 3.0)) if c_ else 0.0
            side[0][NI["i24_mid_resid"]] = val if a2 < d2 else -val
            side[1][NI["i24_mid_resid"]] = -side[0][NI["i24_mid_resid"]]
        sid = int(sid_a[i])
        prev = ser_hist.get(sid) if sid > 0 else None
        if prev and rt > 0 and dt > 0:
            lw, lh = prev[-1]
            v = 1.0 if lw == rt else (-1.0 if lw == dt else 0.0)
            sc = float(sum(1 if w == rt else (-1 if w == dt else 0) for w, _h in prev))
            rep = float(len(set(heroes[i, :5].tolist()) & lh.get(rt, set())) -
                        len(set(heroes[i, 5:].tolist()) & lh.get(dt, set())))
            side[0][NI["i26_prev_map"]], side[1][NI["i26_prev_map"]] = v, -v
            side[0][NI["i26_series_score"]], side[1][NI["i26_series_score"]] = sc, -sc
            side[0][NI["i26_hero_rep"]], side[1][NI["i26_hero_rep"]] = rep, -rep

        a_, b_ = side
        for j, (_nm, _idea, scale) in enumerate(NEW):
            if scale == "raw100":
                F[i, j] = (a_[j] - b_[j]) / 100.0
            elif scale == "raw1k":
                F[i, j] = (a_[j] - b_[j]) / 1000.0
            elif scale == "log":
                F[i, j] = math.log1p(max(a_[j], 0)) - math.log1p(max(b_[j], 0))
            else:
                F[i, j] = a_[j] - b_[j]

        # ---- обновление
        won_r = bool(wins[i])
        nw10 = float(nw_a[i][10]) if nw_a.shape[1] > 10 else 0.0
        k10 = float(rk_a[i][10] + dk_a[i][10]) if rk_a.shape[1] > 10 else 0.0
        dur_min = max(dur_a[i] / 60.0, 1.0)
        kpm = float(rk_a[i][-1] + dk_a[i][-1]) / dur_min
        tw = tow_a[i]
        rad_lost15, dire_lost15 = float(tw[2]), float(tw[6])
        for s in range(2):
            tid = int(teams[i, s])
            if tid <= 0:
                continue
            st = tstat[tid]
            st[0] += dur_a[i]
            st[1] += k10
            st[2] += nw10 if s == 0 else -nw10
            st[3] += dire_lost15 if s == 0 else rad_lost15
            st[4] += kpm
            st[5] += 1
            q = tmatches[tid]
            q.append(now)
            while q and q[0] < now - 4 * 86400:
                q.popleft()
            cur = frozenset(int(a) for a in accounts[i, s * 5:(s + 1) * 5] if a > 0)
            if roster_last.get(tid) != cur:
                roster_since[tid] = [now, 0]
                roster_last[tid] = cur
            else:
                roster_since.setdefault(tid, [now, 0])[1] += 1
            for h in heroes[i, s * 5:(s + 1) * 5].tolist():
                th[(tid, int(h))] += 1
                t_total[tid] += 1
        for s in range(2):
            won = won_r if s == 0 else not won_r
            for p in range(5):
                a, h = int(accounts[i, s * 5 + p]), int(heroes[i, s * 5 + p])
                if a > 0:
                    d = p_pick[a]
                    c0 = d.get(h, 0)
                    p_S[a] += (c0 + 1) * math.log(c0 + 1) - (c0 * math.log(c0) if c0 else 0.0)
                    d[h] = c0 + 1
                    p_tot[a] += 1
                hpos[h][p] += 1
                ev.append((now, h, int(won)))
                ev7.append((now, h, int(won)))
                pick_c[h] += 1
                win_c[h] += int(won)
                pick7[h] += 1
                win7[h] += int(won)
                tot30[0] += 1
                tot7[0] += 1
        if rt > 0 and dt > 0:
            er = 1.0 / (1.0 + 10 ** ((rating_team.get(dt, 1500.0) - rating_team.get(rt, 1500.0)) / 400.0))
            key = (min(rt, dt), max(rt, dt))
            sgn = 1.0 if rt < dt else -1.0
            h2h[key][0] += sgn * ((1.0 if won_r else 0.0) - er)
            h2h[key][1] += 1
            rating_team[rt] = rating_team.get(rt, 1500.0) + K * ((1.0 if won_r else 0.0) - er)
            rating_team[dt] = rating_team.get(dt, 1500.0) + K * ((0.0 if won_r else 1.0) - (1 - er))
        if a2 > 0 and d2 > 0:
            key = (min(a2, d2), max(a2, d2))
            sgn = 1.0 if a2 < d2 else -1.0
            mid2[key][0] += sgn * ((1.0 if won_r else 0.0) - 0.5)
            mid2[key][1] += 1
        if sid > 0 and rt > 0 and dt > 0:
            ser_hist[sid].append((rt if won_r else dt,
                                  {rt: set(heroes[i, :5].tolist()), dt: set(heroes[i, 5:].tolist())}))
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
    base = np.load(EXT)["F"][keep]
    logit = np.load(LOGIT)["logit"][keep].reshape(-1, 1)
    b1 = np.load(B1)["F"][:, [B1_IDX[n] for n in B1_NAMES]]

    if CACHE.exists():
        F = np.load(CACHE)["F"]
        print("признаки партии 2 взяты из кэша", flush=True)
    else:
        F = build(ts, wins, accounts, heroes, teams, zr, idx_r)
        np.savez_compressed(CACHE, F=F)
        print(f"признаки сохранены: {CACHE}", flush=True)

    test, train = ts >= TEST_FROM, ts < TEST_FROM
    y, y_tr = wins[test], wins[train]
    G = np.column_stack([logit, base[:, [EI[n] for n in LIVE]], b1])
    base_auc = auc(y, fit(G, y_tr, train, test))

    L = ["# Идеи 12-17, 20, 22-24, 26-27: стиль, мета, новизна, ростер, встречи", "",
         f"Корпус {int(keep.sum()):,} карт; обучение {int(train.sum()):,}, проверка {int(test.sum()):,}. "
         f"База — G из E-88 плюс два выживших признака партии 1 (E-89): **AUC {base_auc:.4f}**.", "",
         "## 1. По группам идей", "", "| идея | признаков | AUC | к базе |", "|---|---:|---|---|"]
    titles = {12: "стиль команды", 13: "драфт против стиля", 14: "мета патча",
              15: "адаптация к мете", 16: "новизна драфта", 17: "гибкость позиций",
              20: "глубина пула", 22: "свежесть ростера", 23: "личные встречи",
              24: "мид против мида", 26: "серия", 27: "усталость"}
    for idea in sorted(titles):
        cols = [NI[n] for n, i2, _s in NEW if i2 == idea]
        a = auc(y, fit(np.column_stack([G, F[:, cols]]), y_tr, train, test))
        L.append(f"| {idea}. {titles[idea]} | {len(cols)} | {a:.4f} | {a - base_auc:+.4f} |")
    all_auc = auc(y, fit(np.column_stack([G, F]), y_tr, train, test))
    L.append(f"| **все вместе** | {len(NAMES)} | **{all_auc:.4f}** | **{all_auc - base_auc:+.4f}** |")

    L += ["", "## 2. Вклад каждого признака (leave-one-out)", "",
          "| признак | идея | покрытие | корр. с elo | вклад |", "|---|---|---:|---:|---:|"]
    full = np.column_stack([G, F])
    rows = []
    for j, (nm, idea, _s) in enumerate(NEW):
        cols = [k for k in range(full.shape[1]) if k != G.shape[1] + j]
        a_wo = auc(y, fit(full[:, cols], y_tr, train, test))
        rows.append((nm, idea, float(np.mean(np.abs(F[:, j]) > 1e-9)),
                     float(np.corrcoef(F[:, j], base[:, EI["elo"]])[0, 1]), all_auc - a_wo))
    for nm, idea, cov, cr, d in sorted(rows, key=lambda r: -r[4]):
        L.append(f"| {nm} | {idea} | {cov:.1%} | {cr:+.2f} | {d:+.4f} |")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L), flush=True)
    print(f"\nотчёт: {OUT}", flush=True)


if __name__ == "__main__":
    main()
