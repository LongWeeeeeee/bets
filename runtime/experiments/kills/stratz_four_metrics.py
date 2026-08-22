#!/usr/bin/env python3
"""Четыре метрики Stratz и прибавка каждой над словарём `all` по полосам индекса.

МЕТРИКИ. Все ориентированы на радианта.
  cpwinrate      — контрпик, сырой винрейт: среднее по 25 перекрёстным парам
                   сведённого винрейта Stratz минус 0.5.
  cprating       — контрпик, рейтинг Stratz: среднее по тем же 25 парам.
  synergywinrate — синергия, сырой винрейт: среднее по 10 внутрикомандным парам
                   радианта минус то же у дайра.
  synergyrating  — синергия, рейтинг Stratz: так же по 10 парам на сторону.

СВЕДЕНИЕ ДВУХ НАПРАВЛЕНИЙ РАЗНОЕ У СТОРОН, И ЭТО НЕ МЕЛОЧЬ. Проверено на
данных: у `vs` винрейты дополняют друг друга до единицы (сумма 0.9999), а
рейтинг антисимметричен (corr(S, −Sᵀ) = +0.986). У `with` обе стороны говорят
об одном и том же: винрейты равны (медиана расхождения 0.0020), рейтинг
симметричен (corr(S, Sᵀ) = +0.982). Поэтому `vs` сводится через инверсию и
полуразность, а `with` — через простое взвешенное среднее и полусумму. Если
применить к `with` антисимметричную формулу, от метрики останется только шум
двух подвыборок — в первом прогоне так и вышло, AUC 0.5089.

ПОЛОСЫ ИНДЕКСА. Индекс — это вердикт базовой модели в боевом масштабе,
(p − 0.5) × 100. Считается двумя способами: по самому словарю `all` (где громко
говорит именно тот блок, над которым меряем прибавку) и по боевому draft_logit
(где уверен боевой драфт). Веса всех моделей обучены на первых 70% карт по
времени, полосы нарезаются только на тестовой трети.

Запуск: DAYS=365 venv_catboost/bin/python3 runtime/experiments/kills/stratz_four_metrics.py
"""
from __future__ import annotations

import os, sys, time
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
S = Path("/private/tmp/claude-501/-Users-alex-Documents-ingame/cb51a5f6-8092-496c-b4a9-fbfa39120e3b/scratchpad")
OUT = ROOT / "runtime/artifacts/misc/stratz_four_metrics.md"
DAYS = int(os.getenv("DAYS", "365"))
BR = os.getenv("BRACKET", "all")
MIN_GAMES, MIN_STRATZ = int(os.getenv("MIN_GAMES", "30")), int(os.getenv("MIN_STRATZ", "50"))
t0 = time.time(); say = lambda *a: print(*a, flush=True)

# ---------- словарь all ----------
zd = np.load(S / "all_dict_cp1v1.npz")
Wd, Dd, Gd = zd["wins"], zd["draws"], zd["games"]
Gt, Wt, Dt = (x.transpose(2, 3, 0, 1) for x in (Gd, Wd, Dd))
G_ALL, W_ALL = Gd + Gt, Wd + (Gt - Wt - Dt)
with np.errstate(invalid="ignore", divide="ignore"):
    WR_ALL = np.where(G_ALL >= MIN_GAMES, W_ALL / np.maximum(G_ALL, 1), np.nan)

# ---------- Stratz ----------
zs = np.load(ROOT / "data/stratz_matchups/latest.npz", allow_pickle=True)
sid = zs["hero_ids"].astype(int); bi = [str(x) for x in zs["brackets"]].index(BR)
mc, wc = zs["match_count"][bi].astype(float), zs["win_count"][bi].astype(float)
syn, wav = zs["synergy"][bi].astype(float), zs["wins_average"][bi].astype(float)
smap = np.full(256, -1, int); smap[sid] = np.arange(len(sid))

m_vs, w_vs, s_vs = mc[1], wc[1], np.nan_to_num(syn[1])
tot_vs = m_vs + m_vs.T
ok_vs = tot_vs >= MIN_STRATZ
CP_WR = np.where(ok_vs, (w_vs + (m_vs.T - w_vs.T)) / np.maximum(tot_vs, 1), np.nan)
CP_RT = np.where(ok_vs, (s_vs - s_vs.T) / 2.0, np.nan)

m_w, w_w, s_w = mc[0], wc[0], np.nan_to_num(syn[0])
tot_w = m_w + m_w.T
ok_w = tot_w >= MIN_STRATZ
SY_WR = np.where(ok_w, (w_w + w_w.T) / np.maximum(tot_w, 1), np.nan)
SY_RT = np.where(ok_w, (s_w + s_w.T) / 2.0, np.nan)
say(f"Stratz {BR}: пар vs {int(ok_vs.sum())//2:,}, пар with {int(ok_w.sum())//2:,}")

# ---------- корпус ----------
zc = np.load(ROOT / "runtime/artifacts/misc/pro_corpus_compact.npz", allow_pickle=True)
H, ts, wins, mids = zc["heroes"].astype(int), zc["ts"], zc["wins"].astype(int), zc["mids"]
cut = int(ts.max()) - DAYS * 86400
sel = np.flatnonzero((ts >= cut) & (H > 0).all(1) & (smap[np.clip(H, 0, 255)] >= 0).all(1))
H, ts, y = H[sel], ts[sel], wins[sel]
rad, dire = H[:, :5], H[:, 5:]
rs, ds = smap[rad], smap[dire]
n = len(rad)
say(f"про-карт за {DAYS} дней: {n:,}")


def cross(mat, use_pos: bool) -> np.ndarray:
    acc, cnt = np.zeros(n), np.zeros(n)
    for i in range(5):
        for j in range(5):
            v = mat[rad[:, i], i, dire[:, j], j] if use_pos else mat[rs[:, i], ds[:, j]]
            ok = np.isfinite(v); acc[ok] += v[ok]; cnt[ok] += 1
    return np.where(cnt > 0, acc / np.maximum(cnt, 1), 0.0)


def within(mat) -> np.ndarray:
    out = np.zeros(n)
    for side, sgn in ((rs, 1.0), (ds, -1.0)):
        acc, cnt = np.zeros(n), np.zeros(n)
        for i in range(5):
            for j in range(i + 1, 5):
                v = mat[side[:, i], side[:, j]]
                ok = np.isfinite(v); acc[ok] += v[ok]; cnt[ok] += 1
        out += sgn * np.where(cnt > 0, acc / np.maximum(cnt, 1), 0.0)
    return out


f_all = cross(WR_ALL, True) - 0.5
METRICS = {"cpwinrate": cross(CP_WR, False) - 0.5,
           "cprating": cross(CP_RT, False),
           "synergywinrate": within(SY_WR),
           "synergyrating": within(SY_RT)}
for k, v in METRICS.items():
    say(f"  {k:>15s}: sd {v.std():.4f}  диапазон {v.min():+.3f}…{v.max():+.3f}")

lg_path = ROOT / "runtime/artifacts/misc/pro_draft_logit_full.npz"
hb_path = ROOT / "runtime/artifacts/misc/map_winner_hybrid_quality_forward/hybrid_features.npz"
f_logit, has_logit = np.zeros(n), np.zeros(n, bool)
if lg_path.exists() and hb_path.exists():
    lg = np.load(lg_path)["logit"]; hm = np.load(hb_path, allow_pickle=True)["mids"].astype(np.int64)
    by = dict(zip(hm.tolist(), lg.tolist()))
    vals = np.array([by.get(int(m), np.nan) for m in mids[sel].tolist()])
    has_logit = np.isfinite(vals); f_logit = np.nan_to_num(vals)
    say(f"draft_logit покрывает {has_logit.mean():.1%}")

order = np.argsort(ts, kind="mergesort")
tr, te = order[:int(n * 0.7)], order[int(n * 0.7):]
say(f"обучение {len(tr):,}, тест {len(te):,}")


def fit_score(cols, a, b):
    X = np.column_stack(cols)
    mu, sd = X[a].mean(0), X[a].std(0) + 1e-9
    m = LogisticRegression(C=1.0, max_iter=1000).fit((X[a] - mu) / sd, y[a])
    return m.decision_function((X[b] - mu) / sd), m.predict_proba((X[b] - mu) / sd)[:, 1]


def boot_delta(sa, sb, yy, n_boot=400, seed=0):
    rng = np.random.default_rng(seed); d = np.empty(n_boot)
    for i in range(n_boot):
        k = rng.integers(0, len(yy), len(yy))
        d[i] = (np.nan if yy[k].min() == yy[k].max()
                else roc_auc_score(yy[k], sb[k]) - roc_auc_score(yy[k], sa[k]))
    return float(np.nanpercentile(d, 2.5)), float(np.nanpercentile(d, 97.5))


lines = [f"# Четыре метрики Stratz над словарём `all`", "",
         f"Про-карты за {DAYS} дней: {n:,} (обучение {len(tr):,} / тест {len(te):,}). "
         f"Срез Stratz `{BR}`, порог пары {MIN_STRATZ}+ матчей; словарь {MIN_GAMES}+ игр.", "",
         "## Метрики поодиночке на всём тесте", "",
         "| метрика | AUC | корреляция со словарём `all` |", "|---|---:|---:|"]
sa_all, _ = fit_score([f_all], tr, te)
lines.append(f"| словарь `all` (база) | {roc_auc_score(y[te], sa_all):.4f} | — |")
for k, v in METRICS.items():
    s, _ = fit_score([v], tr, te)
    lines.append(f"| {k} | {roc_auc_score(y[te], s):.4f} | "
                 f"{np.corrcoef(f_all, v)[0,1]:+.3f} |")
lines.append("")

for tag, idx_src in (("индексу словаря `all`", "all"), ("индексу боевого draft_logit", "logit")):
    if idx_src == "all":
        _, p = fit_score([f_all], tr, te); index = (p - 0.5) * 100.0
        keep = np.ones(len(te), bool)
    else:
        if not has_logit.any():
            continue
        keep = has_logit[te]
        _, p = fit_score([f_logit], tr, te); index = (p - 0.5) * 100.0
    lines += [f"## Прибавка по полосам |индекса|, полосы по {tag}", "",
              "| полоса | карт | база `all` | " + " | ".join(METRICS) + " |",
              "|---|---:|---:|" + "---:|" * len(METRICS)]
    qs = np.quantile(np.abs(index[keep]), [0.0, 0.5, 0.75, 0.9])
    bands = [(f"все", 0.0)] + [(f"верхние {int(round((1-q)*100))}% (|индекс| ≥ {v:.1f})", v)
                               for q, v in zip((0.5, 0.75, 0.9), qs[1:])]
    for name, thr in bands:
        m = keep & (np.abs(index) >= thr)
        yy = y[te][m]
        if m.sum() < 300 or yy.min() == yy.max():
            continue
        a0 = roc_auc_score(yy, sa_all[m])
        cells = []
        for k, v in METRICS.items():
            sb, _ = fit_score([f_all, v], tr, te)
            a1 = roc_auc_score(yy, sb[m])
            lo, hi = boot_delta(sa_all[m], sb[m], yy)
            star = "" if lo <= 0 <= hi else "*"
            cells.append(f"{a1-a0:+.4f}{star}")
        lines.append(f"| {name} | {int(m.sum()):,} | {a0:.4f} | " + " | ".join(cells) + " |")
    lines.append("")
lines.append("Звёздочкой помечены прибавки, чей 95% бутстрап-интервал не накрывает ноль.")
lines.append(f"\nПрогон {time.time()-t0:.0f} c.")
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
say("\n".join(lines))
