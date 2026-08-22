#!/usr/bin/env python3
"""Накопленный сюрприз серии как ПРИЗНАК, а не жёсткая поправка −0.07.

ПОЧЕМУ ПРИЗНАК. Замер отклика показал, что дело не в спусковом крючке: остаток
монотонно идёт по всему диапазону накопленного сюрприза, от −0.0520 в нижней
десятой до +0.0146 в верхней. То есть ось работает в ОБЕ стороны — команда,
которая уже переигрывала ожидания модели в этой серии, дальше тоже их
переигрывает. Плоская константа на триггере ловит один хвост из двух. И серия
бывает длиннее двух карт: в бо-5 апсетов может быть несколько, а накопленная
сумма их складывает сама, тогда как триггер «был апсет» не различает один и два.

ПРИЗНАКИ, все ориентированы на команду, за которую модель голосует СЕЙЧАС:
  s_sum  — сумма «выиграла минус обещано» по прошлым картам серии;
  s_last — то же по последней сыгранной карте;
  n_prev — сколько карт серии уже сыграно.
Накопленная сумма сильнее последней карты: размах остатка 0.0667 против 0.0552.

ФОРМА ПОПРАВКИ. Логит вероятности модели входит признаком наравне с остальными,
поэтому поправка получается мультипликативной в логит-шкале, а не сдвигом
вероятности на фиксированное число. Веса обучаются на первых 70% карт по
времени, всё меряется на последних 30%.

Источник вероятностей задаётся SRC: `forward` (форвардная модель победителя
карты, 2 года) или `prod` (боевой скорер, тестовое окно).

Запуск: SRC=forward venv_catboost/bin/python3 runtime/experiments/kills/series_surprise_feature.py
"""
from __future__ import annotations

import os, sys, time
from collections import defaultdict
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
import prematch_scorer as ps            # noqa: E402

S = Path("/private/tmp/claude-501/-Users-alex-Documents-ingame/cb51a5f6-8092-496c-b4a9-fbfa39120e3b/scratchpad")
ART = ROOT / "runtime/artifacts/misc"
SRC = os.getenv("SRC", "forward")
OUT = ART / f"series_surprise_feature_{SRC}.md"
GAP, MIN_INDEX, FLAT_D = 10800.0, 8.0, 0.07
say = lambda *a: print(*a, flush=True)
t0 = time.time()

zc = np.load(ART / "pro_corpus_compact.npz", allow_pickle=True)
pos = {int(x): i for i, x in enumerate(zc["mids"].tolist())}
if SRC == "prod":
    z = np.load(S / "prod_prematch_probs.npz")
    ci = np.array([pos[int(m)] for m in z["mids"].tolist()]); P = z["p"]
else:
    hb = np.load(ART / "map_winner_hybrid_quality_forward/hybrid_features.npz",
                 allow_pickle=True)["mids"].astype(np.int64)
    fp = np.load(ART / "forward_predictions.npz", allow_pickle=True)
    fin = np.isfinite(fp["P"])
    ci = np.array([pos[int(m)] for m in hb[fin].tolist()]); P = fp["P"][fin]
T, ts, wins, leagues = zc["teams"][ci], zc["ts"][ci], zc["wins"][ci].astype(int), zc["leagues"][ci]
conf = np.maximum(P, 1 - P)
pick_rad = P >= 0.5
pick_team = np.where(pick_rad, T[:, 0], T[:, 1])
hit = np.where(pick_rad, wins == 1, wins == 0).astype(int)
say(f"источник {SRC}: карт {len(P):,}, AUC {roc_auc_score(wins,P):.4f}")

grp = defaultdict(list)
for i, (a, b, l) in enumerate(zip(T[:, 0], T[:, 1], leagues)):
    if a > 0 and b > 0:
        grp[(int(min(a, b)), int(max(a, b)), int(l))].append(i)
prev = np.full(len(P), -1, int)
for k, idxs in grp.items():
    idxs = sorted(idxs, key=lambda i: ts[i])
    for a, b in zip(idxs, idxs[1:]):
        if 0 < ts[b] - ts[a] <= GAP:
            prev[b] = a

# ОРИЕНТАЦИЯ. Признак считается для РАДИАНТА текущей карты, потому что цель
# модели — «победил радиант». Раньше он считался для команды, за которую голосует
# модель, и на картах с выбором в пользу дайра знак относительно цели
# переворачивался: веса s_sum получались разного знака на двух источниках.
rows = []
for i in np.flatnonzero(prev >= 0):
    tm, s, j = T[i, 0], [], prev[i]
    while j >= 0 and len(s) < 6:
        if T[j, 0] == tm:
            pj, wj = P[j], wins[j] == 1
        elif T[j, 1] == tm:
            pj, wj = 1 - P[j], wins[j] == 0
        else:
            break
        s.append(float(wj) - float(pj)); j = prev[j]
    if s:
        rows.append((i, sum(s), s[0], len(s)))
idx = np.array([r[0] for r in rows], int)
s_sum = np.array([r[1] for r in rows]); s_last = np.array([r[2] for r in rows])
n_prev = np.array([r[3] for r in rows], float)
p0, y, tsx = P[idx], wins[idx], ts[idx]
conf0, pick0, hit0 = conf[idx], pick_rad[idx], hit[idx]
say(f"карт с историей серии: {len(idx):,}")

# --- поправка учится в логит-шкале поверх вероятности модели ---
lg = np.log(np.clip(p0, 1e-4, 1 - 1e-4) / np.clip(1 - p0, 1e-4, 1 - 1e-4))
X = np.column_stack([lg, s_sum, s_last, n_prev])
o = np.argsort(tsx, kind="mergesort")
tr, te = o[:int(len(o) * 0.7)], o[int(len(o) * 0.7):]
mu, sd = X[tr].mean(0), X[tr].std(0) + 1e-9
mdl = LogisticRegression(C=1.0, max_iter=1000).fit((X[tr] - mu) / sd, y[tr])
p_fix = mdl.predict_proba((X - mu) / sd)[:, 1]
say("веса (логит, s_sum, s_last, n_prev): " +
    ", ".join(f"{w:+.3f}" for w in mdl.coef_.ravel()))

# --- плоское правило для сравнения ---
trig = np.zeros(len(idx), bool)
for k, i in enumerate(idx):
    j = prev[i]
    trig[k] = j >= 0 and conf[j] >= (0.624 if SRC == "prod" else 0.80) and hit[j] == 0
p_flat = np.where(pick0, np.where(trig, p0 - FLAT_D, p0), np.where(trig, p0 + FLAT_D, p0))
p_flat = np.clip(p_flat, 0.01, 0.99)


def book(p):
    """Ставочная книга по вероятности p: направление, отбор по индексу, отдача."""
    c = np.maximum(p, 1 - p)
    side = p >= 0.5
    h = np.where(side, y == 1, y == 0).astype(float)
    bet = ((c - 0.5) * 100.0 >= MIN_INDEX)
    mo = np.array([ps.lan_min_odds(min(v, 0.99)) for v in c])
    m = bet[te] if len(te) else bet
    hh, mm = h[te][m], mo[te][m]
    return int(m.sum()), float(hh.mean()), float((hh * mm - 1).mean()), float((h[te] - c[te]).mean())


lines = [f"# Накопленный сюрприз серии как признак ({SRC})", "",
         f"Карт с историей серии {len(idx):,}, обучение {len(tr):,} / тест {len(te):,}. "
         f"Веса поправки: логит {mdl.coef_.ravel()[0]:+.3f}, s_sum {mdl.coef_.ravel()[1]:+.3f}, "
         f"s_last {mdl.coef_.ravel()[2]:+.3f}, n_prev {mdl.coef_.ravel()[3]:+.3f}.", "",
         "| вариант | AUC теста | ставок | винрейт | отдача | средний остаток |",
         "|---|---:|---:|---:|---:|---:|"]
# Контроль: только перекалибровка логита, без признаков серии. Иначе выигрыш
# признака нельзя отличить от общего сжатия вероятности.
Xc = lg.reshape(-1, 1)
muc, sdc = Xc[tr].mean(0), Xc[tr].std(0) + 1e-9
p_cal = (LogisticRegression(C=1.0, max_iter=1000).fit((Xc[tr] - muc) / sdc, y[tr])
         .predict_proba((Xc - muc) / sdc)[:, 1])

for nm, p in (("без поправки", p0), ("только перекалибровка логита", p_cal),
              (f"плоская −{FLAT_D:.2f} на триггере", p_flat),
              ("признак сюрприза", p_fix)):
    n_, wr, roi_, resid = book(p)
    lines.append(f"| {nm} | {roc_auc_score(y[te], p[te]):.4f} | {n_:,} | {wr:.4f} | "
                 f"{roi_:+.2%} | {resid:+.4f} |")
lines.append("")

lines += ["## Отклик по накопленному сюрпризу (тест)", "",
          "| дециль s_sum | s_sum | карт | остаток без поправки | остаток с признаком |",
          "|---|---:|---:|---:|---:|"]
cuts = np.quantile(s_sum[te], [.1, .25, .5, .75, .9])
g = np.digitize(s_sum[te], cuts)
for k, lab in enumerate(("≤10%", "10–25%", "25–50%", "50–75%", "75–90%", ">90%")):
    m = g == k
    if m.sum() < 40:
        continue
    r0 = (hit0[te][m] - conf0[te][m]).mean()
    c1 = np.maximum(p_fix[te][m], 1 - p_fix[te][m])
    h1 = np.where(p_fix[te][m] >= 0.5, y[te][m] == 1, y[te][m] == 0)
    lines.append(f"| {lab} | {s_sum[te][m].mean():+.3f} | {int(m.sum()):,} | {r0:+.4f} | "
                 f"{(h1-c1).mean():+.4f} |")
lines.append(f"\nПрогон {time.time()-t0:.0f} c.")
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
say("\n".join(lines))
