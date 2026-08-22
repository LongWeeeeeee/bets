#!/usr/bin/env python3
"""Богатые признаки ПРЕДЫДУЩЕЙ карты серии: добавляют ли они к скаляру сюрприза.

ЗАМЫСЕЛ alex: спустя ~20 минут предыдущая карта доступна целиком — поминутный
нетворс, килы, линии, длительность. Значит вместо одного числа «сюрприз» можно
пересобрать признаки по факту сыгранной карты и предсказывать следующую.

ЧТО ПРОВЕРЯЕТСЯ. Фаворит, отдавший карту в равной игре, и фаворит, которого
раскатали, — это разные состояния, а скаляр сюрприза их не различает: он смотрит
только на исход и обещанную вероятность. Признаки ниже берутся из уже собранного
`pro_corpus_rich.npz` и ориентируются на РАДИАНТА текущей карты (цель модели
радиантная; на этом месте в прошлой версии был баг с ориентацией на выбранную
модель команду).

  s_last   — исход минус обещано по последней карте;
  s_sum    — то же, накопленно по всей серии;
  nw_end   — итоговый перевес по нетворсу на прошлой карте, тысячи золота;
  nw20     — перевес на 20-й минуте;
  kd_end   — разница килов;
  dur      — длительность прошлой карты, минуты;
  lanes    — сумма исходов линий.

Запуск: SRC=forward venv_catboost/bin/python3 runtime/experiments/kills/series_prev_map_features.py
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
import prematch_scorer as ps  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
S = Path("/private/tmp/claude-501/-Users-alex-Documents-ingame/cb51a5f6-8092-496c-b4a9-fbfa39120e3b/scratchpad")
SRC = os.getenv("SRC", "forward")
OUT = ART / f"series_prev_map_features_{SRC}.md"
GAP, MIN_INDEX = 10800.0, 8.0
t0 = time.time(); say = lambda *a: print(*a, flush=True)

zc = np.load(ART / "pro_corpus_compact.npz", allow_pickle=True)
zr = np.load(ART / "pro_corpus_rich.npz")
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
rpos = {int(x): i for i, x in enumerate(zr["mids"].tolist())}
ri = np.array([rpos[int(m)] for m in zc["mids"][ci].tolist()])
T, ts, wins = zc["teams"][ci], zc["ts"][ci], zc["wins"][ci].astype(int)
NW, RK, DK = zr["nw"][ri], zr["rk"][ri], zr["dk"][ri]
DUR, LAN = zr["durations"][ri].astype(float) / 60.0, zr["lanes"][ri].astype(float)
say(f"{SRC}: карт {len(P):,}; поминутный нетворс есть у {float((np.abs(NW).sum(1)>0).mean()):.1%}")

grp = defaultdict(list)
for i, (a, b, l) in enumerate(zip(T[:, 0], T[:, 1], zc["leagues"][ci])):
    if a > 0 and b > 0:
        grp[(int(min(a, b)), int(max(a, b)), int(l))].append(i)
prev = np.full(len(P), -1, int)
for k, idxs in grp.items():
    idxs = sorted(idxs, key=lambda i: ts[i])
    for a, b in zip(idxs, idxs[1:]):
        if 0 < ts[b] - ts[a] <= GAP:
            prev[b] = a

rows = []
for i in np.flatnonzero(prev >= 0):
    tm = T[i, 0]                                  # ориентир — радиант текущей карты
    j, s = prev[i], []
    while j >= 0 and len(s) < 6:
        if T[j, 0] == tm:
            pj, wj, sg = P[j], wins[j] == 1, 1.0
        elif T[j, 1] == tm:
            pj, wj, sg = 1 - P[j], wins[j] == 0, -1.0
        else:
            break
        s.append((float(wj) - float(pj), j, sg)); j = prev[j]
    if not s:
        continue
    _, j0, sg0 = s[0]
    nw = NW[j0].astype(float)
    live = np.flatnonzero(nw != 0)
    nw_end = float(nw[live[-1]]) / 1000.0 * sg0 if len(live) else 0.0
    nw20 = float(nw[20]) / 1000.0 * sg0 if nw.shape[0] > 20 and nw[20] != 0 else 0.0
    kd = float(RK[j0].max() - DK[j0].max()) * sg0
    rows.append((i, sum(x[0] for x in s), s[0][0], nw_end, nw20, kd,
                 float(DUR[j0]), float(LAN[j0].sum()) * sg0, len(s)))
R = np.array(rows, float)
idx = R[:, 0].astype(int)
names = ["s_sum", "s_last", "nw_end", "nw20", "kd_end", "dur", "lanes"]
F = R[:, 1:8]
y, tsx, p0 = wins[idx], ts[idx], P[idx]
say(f"пар с разобранной прошлой картой: {len(idx):,} ({time.time()-t0:.0f} c)")

lg = np.log(np.clip(p0, 1e-4, 1 - 1e-4) / np.clip(1 - p0, 1e-4, 1 - 1e-4))
o = np.argsort(tsx, kind="mergesort")
tr, te = o[:int(len(o) * 0.7)], o[int(len(o) * 0.7):]


def fit(cols):
    X = np.column_stack([lg] + cols)
    mu, sd = X[tr].mean(0), X[tr].std(0) + 1e-9
    m = LogisticRegression(C=1.0, max_iter=1000).fit((X[tr] - mu) / sd, y[tr])
    return m.predict_proba((X - mu) / sd)[:, 1], m.coef_.ravel()


def book(p):
    c = np.maximum(p, 1 - p)
    h = np.where(p >= 0.5, y == 1, y == 0).astype(float)
    mo = np.array([ps.lan_min_odds(min(v, 0.99)) for v in c])
    m = (c[te] - 0.5) * 100.0 >= MIN_INDEX
    return int(m.sum()), float(h[te][m].mean()), float((h[te][m] * mo[te][m] - 1).mean())


lines = [f"# Богатые признаки прошлой карты серии ({SRC})", "",
         f"Пар карт {len(idx):,}, обучение {len(tr):,} / тест {len(te):,}. "
         f"Все признаки ориентированы на радианта текущей карты.", "",
         "| набор | AUC теста | ставок | винрейт | отдача |", "|---|---:|---:|---:|---:|"]
sets = [("только вероятность модели", []),
        ("+ s_sum", [F[:, 0]]),
        ("+ s_sum, s_last", [F[:, 0], F[:, 1]]),
        ("+ богатые: nw_end, nw20, kd, dur, lanes", [F[:, k] for k in (2, 3, 4, 5, 6)]),
        ("+ s_sum и богатые", [F[:, k] for k in range(7)])]
for nm, cols in sets:
    p, _ = fit(cols)
    n_, wr, roi = book(p)
    lines.append(f"| {nm} | {roc_auc_score(y[te], p[te]):.4f} | {n_:,} | {wr:.4f} | {roi:+.2%} |")
lines.append("")
_, w = fit([F[:, k] for k in range(7)])
lines += ["Веса полного набора (стандартизованные): логит " + f"{w[0]:+.3f}, " +
          ", ".join(f"{n} {v:+.3f}" for n, v in zip(names, w[1:])), ""]
lines.append(f"Прогон {time.time()-t0:.0f} c.")
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
say("\n".join(lines))
