#!/usr/bin/env python3
"""Та же поправка на апсет, но вероятности берутся у БОЕВОГО скорера.

Первый замер шёл по форвардной модели победителя карты. Прод считает иначе:
другая шкала уверенности (ставка идёт от |индекса| 8, то есть p ≈ 0.58, а не 0.80)
и другой состав признаков. Поэтому порог «сильно топила» здесь задаётся не
абсолютным числом, а ПЕРЦЕНТИЛЕМ уверенности — иначе полоса окажется либо пустой,
либо не той.

Боевой путь повторяется как в `bets_by_leading_component.py`: снимок обрезан по
`TEST_FROM`, карты прогоняются кодом скорера, сила команды берётся из живого
пакета ELO через `audit_live_path.train_columns`, драфтовый логит — из
`pro_draft_logit_full.npz`.

Запуск: venv_catboost/bin/python3 runtime/experiments/kills/series_upset_prod_scorer.py
"""
from __future__ import annotations

import os, sys, time
from collections import defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
sys.path.insert(0, str(ROOT / "base"))
import audit_live_path as A            # noqa: E402
import prematch_scorer as ps           # noqa: E402
from ideas_batch2 import COMPACT, RICH, TEST_FROM  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
S = Path("/private/tmp/claude-501/-Users-alex-Documents-ingame/cb51a5f6-8092-496c-b4a9-fbfa39120e3b/scratchpad")
CACHE = S / "prod_prematch_probs.npz"
OUT = ART / "series_upset_prod_scorer.md"
SERIES_GAP = float(os.getenv("SERIES_GAP", "10800"))
say = lambda *a: print(*a, flush=True)
t0 = time.time()


def build() -> tuple[np.ndarray, np.ndarray]:
    """(mids, probability) боевого скорера на тестовом окне."""
    if CACHE.exists():
        z = np.load(CACHE)
        say(f"вероятности из кэша: {len(z['mids']):,}")
        return z["mids"], z["p"]
    zc, zr = np.load(COMPACT), np.load(RICH)
    hyb = np.load(ART / "map_winner_hybrid_quality_forward/hybrid_features.npz",
                  allow_pickle=True)["mids"].astype(np.int64)
    cpos = {int(x): i for i, x in enumerate(zc["mids"].tolist())}
    rpos = {int(x): i for i, x in enumerate(zr["mids"].tolist())}
    ci = np.array([cpos[int(m)] for m in hyb.tolist()])
    ri = np.array([rpos[int(m)] for m in hyb.tolist()])
    ts, accounts, heroes = zc["ts"][ci], zc["accounts"][ci], zc["heroes"][ci]
    test = ts >= TEST_FROM
    lgt = np.load(ART / "pro_draft_logit_full.npz")["logit"]
    m = ps.PrematchModel(ART / f"prematch_audit_branches_cut{TEST_FROM}.npz")
    say(f"скорер загружен, тестовых карт {int(test.sum()):,} ({time.time()-t0:.0f} c)")
    _, hstr = A.train_columns(ts, accounts, zr["pstats"][ri],
                              zr["durations"][ri].astype(float) / 60.0, hyb,
                              test, m.ctx_mu, m.ctx_sd, list(m.features))
    say(f"сила команды посчитана ({time.time()-t0:.0f} c)")
    idx = np.flatnonzero(test)
    out_m, out_p = [], []
    for k, i in enumerate(idx):
        h = [int(x) for x in heroes[i]]; a = [int(x) for x in accounts[i]]
        try:
            r = m.score(radiant_accounts=a[:5], dire_accounts=a[5:],
                        radiant_heroes=h[:5], dire_heroes=h[5:],
                        radiant_team_id=0, dire_team_id=0,
                        draft_logit=float(lgt[i]), hybrid_strength=float(hstr[k]),
                        strictness="accounts", now_ts=int(ts[i]), max_age_days=0.0)
        except ps.MissingData:
            continue
        out_m.append(int(hyb[i])); out_p.append(float(r.probability))
        if (k + 1) % 5000 == 0:
            say(f"  {k+1:,}/{len(idx):,} ({time.time()-t0:.0f} c)")
    mids_o = np.array(out_m, np.int64); p_o = np.array(out_p)
    np.savez_compressed(CACHE, mids=mids_o, p=p_o)
    say(f"вердиктов: {len(mids_o):,} из {len(idx):,} ({time.time()-t0:.0f} c)")
    return mids_o, p_o


mids_p, P = build()
zc = np.load(COMPACT)
pos = {int(x): i for i, x in enumerate(zc["mids"].tolist())}
ci = np.array([pos[int(m)] for m in mids_p.tolist()])
T, ts, wins, leagues = zc["teams"][ci], zc["ts"][ci], zc["wins"][ci].astype(int), zc["leagues"][ci]
conf = np.maximum(P, 1 - P)
pick_rad = P >= 0.5
hit = np.where(pick_rad, wins == 1, wins == 0).astype(int)
from sklearn.metrics import roc_auc_score
say(f"AUC боевого скорера: {roc_auc_score(wins, P):.4f}, попадание {hit.mean():.4f}")
say("перцентили уверенности: " +
    ", ".join(f"{q}%={np.percentile(conf,q):.3f}" for q in (50, 75, 90, 95, 99)))

grp = defaultdict(list)
for i, (a, b, l) in enumerate(zip(T[:, 0], T[:, 1], leagues)):
    if a > 0 and b > 0:
        grp[(int(min(a, b)), int(max(a, b)), int(l))].append(i)
prev = np.full(len(P), -1, int)
for k, idxs in grp.items():
    idxs = sorted(idxs, key=lambda i: ts[i])
    for a, b in zip(idxs, idxs[1:]):
        if 0 < ts[b] - ts[a] <= SERIES_GAP:
            prev[b] = a
cur = np.flatnonzero(prev >= 0); pv = prev[cur]
say(f"пар карт одной серии: {len(cur):,}")
conf_prev, conf_cur = conf[pv], conf[cur]
upset = hit[pv] == 0
hit_cur, res = hit[cur], hit[cur] - conf[cur]
roi = hit_cur / conf_cur - 1.0
fav_prev = np.where(pick_rad[pv], T[pv, 0], T[pv, 1])
same_pick = np.where(pick_rad[cur], T[cur, 0], T[cur, 1]) == fav_prev


def boot(a, b, n=4000, seed=0):
    rng = np.random.default_rng(seed); xa, xb = res[a], res[b]
    d = np.array([rng.choice(xa, len(xa), True).mean() - rng.choice(xb, len(xb), True).mean()
                  for _ in range(n)])
    return float(np.percentile(d, 2.5)), float(np.percentile(d, 97.5)), float((d < 0).mean())


lines = ["# Поправка на апсет серии — на боевом скорере", "",
         f"Тестовое окно с {TEST_FROM} ({len(P):,} вердиктов), пар карт одной серии "
         f"{len(cur):,}. AUC скорера {roc_auc_score(wins,P):.4f}, попадание {hit.mean():.4f}. "
         f"Шкала другая: 95-й перцентиль уверенности {np.percentile(conf,95):.3f}, "
         f"99-й {np.percentile(conf,99):.3f} — абсолютные 0.80 здесь почти не встречаются, "
         f"поэтому «сильно топила» задаётся перцентилем.", "",
         "| порог прошлой уверенности | после апсета | карт | без апсета | карт | разница | 95% ДИ | P(<0) |",
         "|---|---:|---:|---:|---:|---:|---|---:|"]
for q in (0, 50, 75, 90, 95):
    thr = float(np.percentile(conf_prev, q))
    pm = conf_prev >= thr
    a, b = pm & upset, pm & ~upset
    if a.sum() < 25 or b.sum() < 25:
        continue
    lo, hi_, pv_ = boot(a, b)
    tag = "все" if q == 0 else f"верх {100-q}% (p ≥ {thr:.3f})"
    lines.append(f"| {tag} | {res[a].mean():+.4f} | {int(a.sum()):,} | {res[b].mean():+.4f} | "
                 f"{int(b.sum()):,} | {res[a].mean()-res[b].mean():+.4f} | {lo:+.4f}…{hi_:+.4f} | {pv_:.3f} |")
lines.append("")

lines += ["## Отдача при коэффициенте 1/вероятность", "",
          "| группа | карт | винрейт | обещано | отдача |", "|---|---:|---:|---:|---:|"]
thr90 = float(np.percentile(conf_prev, 90))
trig = (conf_prev >= thr90) & upset
for nm, m_ in (("все пары", np.ones(len(cur), bool)),
               (f"после апсета в верхних 10% уверенности (p ≥ {thr90:.3f})", trig),
               ("все остальные", ~trig)):
    if m_.sum() < 25:
        continue
    lines.append(f"| {nm} | {int(m_.sum()):,} | {hit_cur[m_].mean():.4f} | "
                 f"{conf_cur[m_].mean():.4f} | {roi[m_].mean():+.2%} |")
lines += ["", f"Модель снова ставит на ту же команду после такого апсета в "
          f"{same_pick[trig].mean():.1%} случаев." if trig.sum() else "", ""]
# ---------- боевая популяция ставок ----------
# Отдача считается по СОБСТВЕННОЙ сетке прода: min_odds на каждую ставку, а не
# 1/вероятность. Ставка идёт от |индекса| 8, то есть p >= 0.58.
MIN_INDEX = 8.0
bet = (conf_cur - 0.5) * 100.0 >= MIN_INDEX
mo = np.array([ps.lan_min_odds(min(c, 0.99)) for c in conf_cur])
ew = np.array([ps.lan_expected_wr(min(c, 0.99)) for c in conf_cur])
ret = hit_cur * mo - 1.0
days = float((ts[cur].max() - ts[cur].min()) / 86400)


def roi_ci(x, n=5000, seed=0):
    rng = np.random.default_rng(seed)
    b = np.array([rng.choice(x, len(x), True).mean() for _ in range(n)])
    return np.percentile(b, [2.5, 97.5]), float((b < 0).mean())


lines += ["## Боевая популяция: ставки при |индексе| ≥ 8", "",
          f"Окно {days:.0f} дней, ставок {int(bet.sum()):,}. Отдача — по сетке "
          f"`lan_min_odds` на каждую ставку.", "",
          "| порог прошлой уверенности | группа | ставок | в месяц | винрейт | обещано сеткой | отдача | 95% ДИ |",
          "|---|---|---:|---:|---:|---:|---:|---|"]
for nm, thr in (("верх 50% (p ≥ 0.624)", float(np.percentile(conf_prev, 50))),
                ("верх 25% (p ≥ 0.714)", float(np.percentile(conf_prev, 75))),
                ("верх 10% (p ≥ 0.814)", float(np.percentile(conf_prev, 90)))):
    tg = (conf_prev >= thr) & upset
    for tag, m_ in (("затронуты", bet & tg), ("остальные", bet & ~tg)):
        if m_.sum() < 10:
            continue
        (lo, hi_), pneg = roi_ci(ret[m_])
        lines.append(f"| {nm} | {tag} | {int(m_.sum()):,} | {m_.sum()/days*30:.0f} | "
                     f"{hit_cur[m_].mean():.4f} | {ew[m_].mean():.4f} | {ret[m_].mean():+.2%} | "
                     f"{lo:+.2%}…{hi_:+.2%} |")
lines.append("")

lines += ["### Подбор поправки D (триггер — верх 50% прошлой уверенности)", "",
          "| D | затронутых ставок остаётся | их отдача | остаток |", "|---:|---:|---:|---:|"]
tg = (conf_prev >= float(np.percentile(conf_prev, 50))) & upset
for D in (0.00, 0.03, 0.05, 0.07, 0.10):
    p2 = np.clip(conf_cur - np.where(tg, D, 0.0), 0.51, 0.99)
    keep = ((p2 - 0.5) * 100.0 >= MIN_INDEX) & tg
    if keep.sum() < 10:
        continue
    mo2 = np.array([ps.lan_min_odds(min(c, 0.99)) for c in p2])
    lines.append(f"| {D:.2f} | {int(keep.sum()):,} из {int((bet & tg).sum()):,} | "
                 f"{(hit_cur[keep]*mo2[keep]-1).mean():+.2%} | "
                 f"{hit_cur[keep].mean()-p2[keep].mean():+.4f} |")
lines.append("")

lines.append(f"Прогон {time.time()-t0:.0f} c.")
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
say("\n".join(lines))
