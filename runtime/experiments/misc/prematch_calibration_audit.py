#!/usr/bin/env python3
"""Насколько честна боевая цена: вероятность скорера против сетки и против факта.

ЗАЧЕМ. На картах серий простая перекалибровка вероятности дала больше, чем
поправка на апсет (+1.9 п.п. отдачи против +1.1). Значит вопрос шире серий: не
переоценивает ли скорер уверенность вообще. Но у прода уже есть слой калибровки —
сетка `lan_expected_wr` / `lan_min_odds` переводит индекс в обещанный винрейт и
минимальный коэффициент. Поэтому мерить надо не «вероятность против факта», а
ТРИ величины сразу: что даёт модель, что обещает сетка и что случилось.

Население — всё тестовое окно аудита, а не только карты серий: перекалибровка,
если она нужна, трогает каждый индекс.

Веса перекалибровки подбираются на первых 70% карт по времени, всё меряется на
последних 30%.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/prematch_calibration_audit.py
"""
from __future__ import annotations

import os, sys, time
from pathlib import Path

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
import prematch_scorer as ps  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
S = Path("/private/tmp/claude-501/-Users-alex-Documents-ingame/cb51a5f6-8092-496c-b4a9-fbfa39120e3b/scratchpad")
OUT = ART / "prematch_calibration_audit.md"
MIN_INDEX = float(os.getenv("PREMATCH_MIN_INDEX", "8"))
t0 = time.time(); say = lambda *a: print(*a, flush=True)

z = np.load(S / "prod_prematch_probs.npz")
zc = np.load(ART / "pro_corpus_compact.npz", allow_pickle=True)
pos = {int(x): i for i, x in enumerate(zc["mids"].tolist())}
ci = np.array([pos[int(m)] for m in z["mids"].tolist()])
P, ts, wins = z["p"], zc["ts"][ci], zc["wins"][ci].astype(int)
o = np.argsort(ts, kind="mergesort")
tr, te = o[:int(len(o) * 0.7)], o[int(len(o) * 0.7):]
say(f"карт {len(P):,}, обучение {len(tr):,}, тест {len(te):,}, AUC {roc_auc_score(wins,P):.4f}")


def conf_side(p):
    c = np.maximum(p, 1 - p)
    h = np.where(p >= 0.5, wins == 1, wins == 0).astype(float)
    return c, h


c0, h0 = conf_side(P)
ew = np.array([ps.lan_expected_wr(min(v, 0.99)) for v in c0])
mo = np.array([ps.lan_min_odds(min(v, 0.99)) for v in c0])

lines = ["# Калибровка боевой цены", "",
         f"Тестовое окно аудита, {len(P):,} вердиктов (обучение {len(tr):,} / тест "
         f"{len(te):,}). AUC {roc_auc_score(wins,P):.4f}. «Обещано сеткой» — "
         f"`lan_expected_wr`, отдача — по `lan_min_odds` на каждую ставку.", "",
         "## Что даёт модель, что обещает сетка и что вышло", "",
         "| полоса индекса | карт | уверенность модели | обещано сеткой | факт | модель − факт | сетка − факт | отдача |",
         "|---|---:|---:|---:|---:|---:|---:|---:|"]
idx = (c0 - 0.5) * 100.0
for lo, hi in ((0, 4), (4, 8), (8, 12), (12, 16), (16, 20), (20, 25), (25, 100)):
    m = np.zeros(len(P), bool); m[te] = True
    m &= (idx >= lo) & (idx < hi)
    if m.sum() < 60:
        continue
    lines.append(f"| {lo}–{hi} | {int(m.sum()):,} | {c0[m].mean():.4f} | {ew[m].mean():.4f} | "
                 f"{h0[m].mean():.4f} | {c0[m].mean()-h0[m].mean():+.4f} | "
                 f"{ew[m].mean()-h0[m].mean():+.4f} | {(h0[m]*mo[m]-1).mean():+.2%} |")
lines.append("")

# ---------- варианты перекалибровки ----------
lg = np.log(np.clip(P, 1e-4, 1 - 1e-4) / np.clip(1 - P, 1e-4, 1 - 1e-4))
X = lg.reshape(-1, 1)
mu, sd = X[tr].mean(0), X[tr].std(0) + 1e-9
p_platt = (LogisticRegression(C=1.0, max_iter=1000).fit((X[tr] - mu) / sd, wins[tr])
           .predict_proba((X - mu) / sd)[:, 1])
iso = IsotonicRegression(out_of_bounds="clip").fit(P[tr], wins[tr])
p_iso = iso.predict(P)


def book(p, name):
    c, h = conf_side(p)
    mo2 = np.array([ps.lan_min_odds(min(v, 0.99)) for v in c])
    bet = np.zeros(len(P), bool); bet[te] = True
    bet &= (c - 0.5) * 100.0 >= MIN_INDEX
    if bet.sum() < 10:
        return None
    ret = h[bet] * mo2[bet] - 1.0
    rng = np.random.default_rng(0)
    b = np.array([rng.choice(ret, len(ret), True).mean() for _ in range(4000)])
    return (name, int(bet.sum()), float(h[bet].mean()), float(ret.mean()),
            float(np.percentile(b, 2.5)), float(np.percentile(b, 97.5)),
            float(roc_auc_score(wins[te], p[te])))


lines += ["## Ставочная книга при |индексе| ≥ 8", "",
          "| вариант | ставок | винрейт | отдача | 95% ДИ | AUC |", "|---|---:|---:|---:|---|---:|"]
for p, nm in ((P, "как есть"), (p_platt, "перекалибровка логита (Platt)"),
              (p_iso, "изотоническая перекалибровка")):
    r = book(p, nm)
    if r:
        lines.append(f"| {r[0]} | {r[1]:,} | {r[2]:.4f} | {r[3]:+.2%} | {r[4]:+.2%}…{r[5]:+.2%} | {r[6]:.4f} |")
lines.append("")
lines.append(f"Прогон {time.time()-t0:.0f} c.")
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
say("\n".join(lines))
