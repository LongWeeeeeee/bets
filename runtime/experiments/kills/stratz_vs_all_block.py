#!/usr/bin/env python3
"""Даёт ли рейтинг Stratz сигнал СВЕРХ блока `all` (словарь post_lane).

НАСЕЛЕНИЕ. Про-карты за последние N дней. Про выбраны намеренно: словарь `all`
собран на паблике, поэтому на паблике он был бы в своей же выборке и сравнение
получилось бы нечестным в его пользу. На про обе таблицы — перенос, и обе
переносятся в одинаковых условиях. Это же и боевая цель: `all` работает именно
по про-матчам.

БЛОК `all`. Ключ `{герой}pos{N}_vs_{герой}pos{M}`, `wins` — победы ЛЕВОЙ стороны.
Обе ориентации хранят РАЗНЫЕ наблюдения (игры совпадают лишь у 5.8% пар,
медиана расхождения 7), то есть карта попадает в ключ той стороной, которая
оказалась левой. Поэтому направления складываются, а не усредняются:
    игры = G[a,b] + G[b,a],  победы a = W[a,b] + (G−W−D)[b,a].

STRATZ. Две стороны пары — независимые подвыборки (см. шапку
`stratz_matchups_fetch.py`), поэтому рейтинг симметризуется как
(S[a,b] − S[b,a]) / 2, а винрейт — как взвешенное среднее двух направлений.

ЧЕСТНОСТЬ БЛЕНДА. Веса логистической смеси подбираются на первых 70% карт по
времени, AUC считается на последних 30%. Иначе прибавка от второго признака
была бы просто подгонкой.

Запуск: DAYS=180 venv_catboost/bin/python3 runtime/experiments/kills/stratz_vs_all_block.py
"""
from __future__ import annotations

import os, sys, time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
S = Path("/private/tmp/claude-501/-Users-alex-Documents-ingame/cb51a5f6-8092-496c-b4a9-fbfa39120e3b/scratchpad")
OUT = ROOT / "runtime/artifacts/misc/stratz_vs_all_block.md"
DAYS = int(os.getenv("DAYS", "180"))
MIN_GAMES = int(os.getenv("MIN_GAMES", "30"))      # порог пары в словаре
MIN_STRATZ = int(os.getenv("MIN_STRATZ", "50"))    # порог пары у Stratz
t0 = time.time()
say = lambda *a: print(*a, flush=True)

# ---------- словарь all ----------
zd = np.load(S / "all_dict_cp1v1.npz")
Wd, Dd, Gd = zd["wins"], zd["draws"], zd["games"]
Gt = Gd.transpose(2, 3, 0, 1); Wt = Wd.transpose(2, 3, 0, 1); Dt = Dd.transpose(2, 3, 0, 1)
G_ALL = Gd + Gt                                    # игры пары, обе ориентации
W_ALL = Wd + (Gt - Wt - Dt)                        # победы ГЕРОЯ СЛЕВА
with np.errstate(invalid="ignore", divide="ignore"):
    WR_ALL = np.where(G_ALL >= MIN_GAMES, W_ALL / np.maximum(G_ALL, 1), np.nan)
say(f"словарь all: пар с {MIN_GAMES}+ играми {int(np.isfinite(WR_ALL).sum()):,}")

# ---------- Stratz ----------
zs = np.load(ROOT / "data/stratz_matchups/latest.npz", allow_pickle=True)
sid = zs["hero_ids"].astype(int)
BR = os.getenv("BRACKET", "all")
bi = [str(x) for x in zs["brackets"]].index(BR)
mc, wc = zs["match_count"][bi].astype(float), zs["win_count"][bi].astype(float)
syn, wav = zs["synergy"][bi].astype(float), zs["wins_average"][bi].astype(float)
NHS = len(sid)
smap = np.full(256, -1, int); smap[sid] = np.arange(NHS)


def symmetrise(side: int):
    """Рейтинг — антисимметрично, винрейт — взвешенно по числу матчей."""
    m, w, s, a = mc[side], wc[side], syn[side], wav[side]
    tot = m + m.T
    wr = np.where(tot >= MIN_STRATZ, (w + (m.T - w.T)) / np.maximum(tot, 1), np.nan)
    ss = np.where(tot >= MIN_STRATZ, (np.nan_to_num(s) - np.nan_to_num(s.T)) / 2.0, np.nan)
    return ss, wr, tot


SYN_VS, WR_VS, TOT_VS = symmetrise(1)
SYN_W, WR_W, TOT_W = symmetrise(0)
say(f"Stratz {BR}: пар vs с {MIN_STRATZ}+ матчами {int(np.isfinite(WR_VS).sum()):,}")

# ---------- про-корпус ----------
zc = np.load(ROOT / "runtime/artifacts/misc/pro_corpus_compact.npz", allow_pickle=True)
H, ts, wins, mids = zc["heroes"].astype(int), zc["ts"], zc["wins"].astype(int), zc["mids"]
cut = int(ts.max()) - DAYS * 86400
sel = np.flatnonzero((ts >= cut) & (H > 0).all(1) & (smap[np.clip(H, 0, 255)] >= 0).all(1))
say(f"про-карт за {DAYS} дней: {len(sel):,}")
H, ts, y = H[sel], ts[sel], wins[sel]
rad, dire = H[:, :5], H[:, 5:]
rs, ds = smap[rad], smap[dire]
pos = np.arange(5)


def cross(mat, use_pos: bool):
    """Среднее по 25 перекрёстным парам, ориентировано на радианта."""
    n = len(rad)
    acc = np.zeros(n); cnt = np.zeros(n)
    for i in range(5):
        for j in range(5):
            v = (mat[rad[:, i], i, dire[:, j], j] if use_pos
                 else mat[rs[:, i], ds[:, j]])
            ok = np.isfinite(v)
            acc[ok] += v[ok]; cnt[ok] += 1
    return np.where(cnt > 0, acc / np.maximum(cnt, 1), 0.0), cnt


def within(mat):
    """Разница внутрикомандных сумм: радиант минус дайр."""
    n = len(rad); out = np.zeros(n)
    for side, sgn in ((rs, 1.0), (ds, -1.0)):
        acc = np.zeros(n); cnt = np.zeros(n)
        for i in range(5):
            for j in range(i + 1, 5):
                v = mat[side[:, i], side[:, j]]
                ok = np.isfinite(v)
                acc[ok] += v[ok]; cnt[ok] += 1
        out += sgn * np.where(cnt > 0, acc / np.maximum(cnt, 1), 0.0)
    return out


f_all, c_all = cross(WR_ALL, True)
f_all -= 0.5
f_syn, c_syn = cross(SYN_VS, False)
f_swr, _ = cross(WR_VS, False); f_swr -= 0.5
f_with = within(SYN_W)
say(f"покрытие: словарь {c_all.mean():.1f}/25 пар, Stratz {c_syn.mean():.1f}/25 "
    f"({time.time()-t0:.0f} c)")

# Боевой драфтовый логит лежит отдельным массивом и выровнен не по корпусу, а по
# `hybrid_features.npz` — соединяется по mids, а не по позиции.
lgt_path = ROOT / "runtime/artifacts/misc/pro_draft_logit_full.npz"
hyb_path = ROOT / "runtime/artifacts/misc/map_winner_hybrid_quality_forward/hybrid_features.npz"
f_logit, logit_mask = None, None
if lgt_path.exists() and hyb_path.exists():
    lg = np.load(lgt_path)["logit"]
    hm = np.load(hyb_path, allow_pickle=True)["mids"].astype(np.int64)
    if len(lg) == len(hm):
        by = dict(zip(hm.tolist(), lg.tolist()))
        vals = np.array([by.get(int(m), np.nan) for m in mids[sel].tolist()])
        logit_mask = np.isfinite(vals)
        f_logit = np.nan_to_num(vals)
        say(f"боевой draft_logit подключён по mids: покрыто "
            f"{logit_mask.mean():.1%} карт выборки")

# ---------- оценка ----------
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

order = np.argsort(ts, kind="mergesort")
split = int(len(order) * 0.7)
tr, te = order[:split], order[split:]
say(f"обучение {len(tr):,} карт, тест {len(te):,}")


def score_of(cols: list[np.ndarray], tr_=None, te_=None):
    """Решение модели на тесте. Веса подбираются только на обучающей половине."""
    a, b = (tr if tr_ is None else tr_), (te if te_ is None else te_)
    X = np.column_stack(cols)
    mu, sd = X[a].mean(0), X[a].std(0) + 1e-9
    m = LogisticRegression(C=1.0, max_iter=1000).fit((X[a] - mu) / sd, y[a])
    return m.decision_function((X[b] - mu) / sd)


def auc_of(cols, tr_=None, te_=None) -> float:
    b = te if te_ is None else te_
    return float(roc_auc_score(y[b], score_of(cols, tr_, te_)))


def delta_ci(base_cols, plus_cols, n_boot: int = 400, seed: int = 0):
    """Парный бутстрап по тестовым картам: доверительный интервал на ПРИРОСТ AUC.

    Один и тот же ресэмпл применяется к обеим моделям, иначе интервал считал бы
    и общий шум AUC, который в разнице сокращается."""
    sa, sb, yy = score_of(base_cols), score_of(plus_cols), y[te]
    rng = np.random.default_rng(seed)
    d = np.empty(n_boot)
    for i in range(n_boot):
        k = rng.integers(0, len(yy), len(yy))
        if yy[k].min() == yy[k].max():
            d[i] = np.nan; continue
        d[i] = roc_auc_score(yy[k], sb[k]) - roc_auc_score(yy[k], sa[k])
    return float(np.nanpercentile(d, 2.5)), float(np.nanpercentile(d, 97.5))


base = {"словарь all (позиционный)": [f_all],
        "Stratz рейтинг vs": [f_syn],
        "Stratz винрейт vs": [f_swr],
        "Stratz синергия with": [f_with]}
lines = ["# Даёт ли Stratz сигнал сверх блока `all`", "",
         f"Про-карты за {DAYS} дней: {len(sel):,} (обучение {len(tr):,} / тест {len(te):,}). "
         f"Порог пары: словарь {MIN_GAMES}+ игр, Stratz {MIN_STRATZ}+ матчей. "
         f"Покрытие пар: словарь {c_all.mean():.1f}/25, Stratz {c_syn.mean():.1f}/25.", "",
         "## Признаки поодиночке", "", "| признак | AUC |", "|---|---:|"]
for k, v in base.items():
    lines.append(f"| {k} | {auc_of(v):.4f} |")
if f_logit is not None:
    lines.append(f"| боевой draft_logit | {auc_of([f_logit]):.4f} |")
lines.append("")

a_all = auc_of([f_all])
combos = [("словарь all + рейтинг Stratz", [f_all, f_syn]),
          ("словарь all + винрейт Stratz", [f_all, f_swr]),
          ("словарь all + рейтинг + синергия", [f_all, f_syn, f_with]),
          ("словарь all + всё от Stratz", [f_all, f_syn, f_swr, f_with])]
lines += ["## Прибавка к блоку `all`", "", "| набор | AUC | прибавка |", "|---|---:|---:|",
          f"| словарь all (база) | {a_all:.4f} | — |"]
lines[-1] = lines[-1].replace("| прибавка |", "| прибавка | 95% ДИ прироста |")
lines[-2] = lines[-2] + "---:|"
for k, v in combos:
    a = auc_of(v)
    lo, hi = delta_ci([f_all], v)
    lines.append(f"| {k} | {a:.4f} | {a-a_all:+.4f} | {lo:+.4f}…{hi:+.4f} |")
lines.append("")

if f_logit is not None:
    tr2 = np.array([i for i in tr if logit_mask[i]])
    te2 = np.array([i for i in te if logit_mask[i]])
    lines.append(f"Карт с боевым логитом: обучение {len(tr2):,}, тест {len(te2):,}.")
    lines.append("")
    a_l = auc_of([f_logit], tr2, te2); a_la = auc_of([f_logit, f_all], tr2, te2)
    lines += ["## Поверх боевого драфта", "", "| набор | AUC | прибавка |", "|---|---:|---:|",
              f"| draft_logit | {a_l:.4f} | — |",
              f"| draft_logit + словарь all | {a_la:.4f} | {a_la-a_l:+.4f} |"]
    for k, v in (("draft_logit + all + рейтинг Stratz", [f_logit, f_all, f_syn]),
                 ("draft_logit + all + всё от Stratz", [f_logit, f_all, f_syn, f_swr, f_with])):
        a = auc_of(v, tr2, te2)
        sa = score_of([f_logit, f_all], tr2, te2); sb = score_of(v, tr2, te2)
        rng = np.random.default_rng(0); yy = y[te2]; dd = np.empty(400)
        for i in range(400):
            kk = rng.integers(0, len(yy), len(yy))
            dd[i] = (np.nan if yy[kk].min() == yy[kk].max()
                     else roc_auc_score(yy[kk], sb[kk]) - roc_auc_score(yy[kk], sa[kk]))
        lo, hi = np.nanpercentile(dd, 2.5), np.nanpercentile(dd, 97.5)
        lines.append(f"| {k} | {a:.4f} | {a-a_la:+.4f} ({lo:+.4f}…{hi:+.4f}) |")
    lines.append("")

# Ниша: помогает ли Stratz там, где боевой драфт НЕ уверен. Если сигнал живёт
# только в неуверенной зоне, общий ноль ещё не приговор.
if f_logit is not None:
    sa = score_of([f_logit, f_all], tr2, te2)
    sb = score_of([f_logit, f_all, f_syn], tr2, te2)
    conf = np.abs(f_logit[te2])
    edges = np.quantile(conf, [1 / 3, 2 / 3])
    lines += ["## Ниша: где боевой драфт не уверен", "",
              "Тест поделён на трети по |draft_logit|. Веса те же, обученные на "
              "всей обучающей половине — деление только на тесте.", "",
              "| зона | карт | draft_logit + all | + рейтинг Stratz | прибавка |",
              "|---|---:|---:|---:|---:|"]
    g = np.digitize(conf, edges)
    for gi, tag in enumerate(("нижняя треть (драфт ровный)", "середина",
                              "верхняя треть (драфт решает)")):
        m = g == gi
        if m.sum() < 100 or y[te2][m].min() == y[te2][m].max():
            continue
        a1 = roc_auc_score(y[te2][m], sa[m]); a2 = roc_auc_score(y[te2][m], sb[m])
        lines.append(f"| {tag} | {int(m.sum()):,} | {a1:.4f} | {a2:.4f} | {a2-a1:+.4f} |")
    lines.append("")

names = ["словарь all", "рейтинг Stratz", "винрейт Stratz", "синергия Stratz"]
F = np.column_stack([f_all, f_syn, f_swr, f_with])
lines += ["## Корреляции признаков", "", "| | " + " | ".join(names) + " |",
          "|---" * (len(names) + 1) + "|"]
C = np.corrcoef(F.T)
for i, n in enumerate(names):
    lines.append(f"| {n} | " + " | ".join(f"{C[i,j]:+.3f}" for j in range(len(names))) + " |")
lines.append(f"\nПрогон {time.time()-t0:.0f} c.")
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
say("\n".join(lines))
