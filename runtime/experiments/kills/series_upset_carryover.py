#!/usr/bin/env python3
"""Что говорит проигрыш фаворита на первой карте о второй.

ГИПОТЕЗА alex: если ML сильно топила за кандидата (80+) и он проиграл, это должно
о многом говорить на следующей карте серии.

ДВЕ ВЕРСИИ, И ОНИ ПРОТИВОПОЛОЖНЫ. Либо модель переоценила команду, ошибка
устойчива и на второй карте повторится — тогда её вердикт надо гасить. Либо
фаворит действительно сильнее и просто отдал карту — тогда вторая карта, наоборот,
хорошее место. Замер должен различить их, а не подтвердить любую.

ВЕРОЯТНОСТЬ. `forward_predictions.npz` — форвардный прогон модели победителя
карты; выровнен к корпусу через mids `hybrid_features.npz` (сверено: ts и wins
совпадают до элемента). Конечных прогнозов 120 000 за 2024-10 … 2026-08.

СЕРИЯ. Группа — та же пара team_id в той же лиге; карты в группе сортируются по
времени, соседние считаются одной серией, если между стартами меньше SERIES_GAP.

КОНТРОЛЬ. Сравнивать «после апсета» надо не со средним по всем картам, а с
«после НЕ апсета» при ТОЙ ЖЕ уверенности на текущей карте: уверенность сама по
себе задаёт винрейт, и без выравнивания по ней сравнивались бы разные популяции.

Запуск: venv_catboost/bin/python3 runtime/experiments/kills/series_upset_carryover.py
"""
from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path

import numpy as np
from sklearn.metrics import roc_auc_score

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
OUT = ROOT / "runtime/artifacts/misc/series_upset_carryover.md"
SERIES_GAP = float(os.getenv("SERIES_GAP", "10800"))     # 3 часа между стартами карт
HI = float(os.getenv("HI", "0.80"))                      # «сильно топила»
say = lambda *a: print(*a, flush=True)

zc = np.load(ROOT / "runtime/artifacts/misc/pro_corpus_compact.npz", allow_pickle=True)
hb = np.load(ROOT / "runtime/artifacts/misc/map_winner_hybrid_quality_forward/hybrid_features.npz",
             allow_pickle=True)
fp = np.load(ROOT / "runtime/artifacts/misc/forward_predictions.npz", allow_pickle=True)
pos = {int(m): i for i, m in enumerate(zc["mids"].tolist())}
ci = np.array([pos[int(m)] for m in hb["mids"].astype(np.int64).tolist()])
P = fp["P"]
fin = np.isfinite(P)
ci, P = ci[fin], P[fin]
T, ts, wins, leagues = zc["teams"][ci], zc["ts"][ci], zc["wins"][ci].astype(int), zc["leagues"][ci]
say(f"карт с прогнозом: {len(P):,}")
conf = np.maximum(P, 1 - P)
pick_rad = P >= 0.5
hit = np.where(pick_rad, wins == 1, wins == 0).astype(int)
say(f"AUC модели: {roc_auc_score(wins, P):.4f}, попадание {hit.mean():.4f}")

# ---------- серии ----------
key = [(int(min(a, b)), int(max(a, b)), int(l)) for a, b, l in zip(T[:, 0], T[:, 1], leagues)]
grp = defaultdict(list)
for i, k in enumerate(key):
    if k[0] > 0 and k[1] > 0:
        grp[k].append(i)
prev = np.full(len(P), -1, int)
for k, idxs in grp.items():
    idxs = sorted(idxs, key=lambda i: ts[i])
    for a, b in zip(idxs, idxs[1:]):
        if 0 < ts[b] - ts[a] <= SERIES_GAP:
            prev[b] = a
has = prev >= 0
say(f"карт со связанной предыдущей: {int(has.sum()):,} ({has.mean():.1%})")

cur = np.flatnonzero(has)
pv = prev[cur]
# сторона фаворита прошлой карты в терминах team_id, чтобы сравнивать между картами
fav_prev_team = np.where(pick_rad[pv], T[pv, 0], T[pv, 1])
upset = hit[pv] == 0                                   # фаворит прошлой карты проиграл
conf_prev, conf_cur = conf[pv], conf[cur]
pick_cur_team = np.where(pick_rad[cur], T[cur, 0], T[cur, 1])
same_pick = pick_cur_team == fav_prev_team             # модель снова ставит на того же
hit_cur = hit[cur]


def wilson(k, n):
    if not n:
        return 0.0, 1.0
    p, z = k / n, 1.96
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, c - h), min(1.0, c + h)


def block(mask, title, lines):
    """Винрейт текущей карты после апсета и без него, ВЫРОВНЕННЫЙ по уверенности."""
    lines += [f"### {title}", "",
              "| полоса уверенности текущей карты | после апсета | карт | без апсета | карт | разница |",
              "|---|---:|---:|---:|---:|---:|"]
    tot_a = tot_b = na = nb = 0
    for lo, hi_ in ((0.50, 0.60), (0.60, 0.70), (0.70, 0.80), (0.80, 1.01)):
        band = mask & (conf_cur >= lo) & (conf_cur < hi_)
        a, b = band & upset, band & ~upset
        if a.sum() < 30 or b.sum() < 30:
            continue
        wa, wb = hit_cur[a].mean(), hit_cur[b].mean()
        lo_a, hi_a = wilson(int(hit_cur[a].sum()), int(a.sum()))
        lines.append(f"| {lo:.2f}–{min(hi_,1.0):.2f} | {wa:.4f} ({lo_a:.3f}–{hi_a:.3f}) | "
                     f"{int(a.sum()):,} | {wb:.4f} | {int(b.sum()):,} | {wa-wb:+.4f} |")
        tot_a += hit_cur[a].sum(); na += a.sum()
        tot_b += hit_cur[b].sum() * (a.sum() / b.sum()); nb += a.sum()
    if na:
        lines.append(f"| **сведённое по полосам** | **{tot_a/na:.4f}** | {int(na):,} | "
                     f"**{tot_b/nb:.4f}** | — | **{(tot_a-tot_b)/na:+.4f}** |")
    lines.append("")


lines = ["# Проигрыш фаворита на прошлой карте серии", "",
         f"Пары «предыдущая — текущая карта» одной серии: {int(has.sum()):,}. "
         f"Модель — форвардный прогноз победителя карты (AUC {roc_auc_score(wins,P):.4f}, "
         f"попадание {hit.mean():.4f}). Серия — та же пара команд в той же лиге с "
         f"разрывом стартов до {SERIES_GAP/3600:.0f} ч.", "",
         "Строка «сведённое по полосам» — винрейт «без апсета», пересчитанный на "
         "распределение уверенности группы «после апсета». Без такого выравнивания "
         "сравнивались бы разные популяции.", ""]

block(np.ones(len(cur), bool), "Все пары карт", lines)
block(conf_prev >= HI, f"Прошлая карта: модель топила на {HI:.0%}+", lines)
block((conf_prev >= HI) & same_pick,
      f"Прошлая {HI:.0%}+ И модель снова ставит на ту же команду", lines)
block((conf_prev >= 0.70) & (conf_prev < HI) & same_pick,
      "Прошлая 70–80% и та же команда (для сравнения)", lines)

# ---------- калибровочный остаток: контроль уверенности поштучно ----------
# Полосы уверенности внутри себя неоднородны: после апсета средняя уверенность
# текущей карты 0.857 против 0.883 без него. Остаток «попал минус обещано»
# снимает это на уровне карты, а не полосы.
res = hit_cur - conf_cur
roi = hit_cur / conf_cur - 1.0          # отдача, если кэф ровно 1/вероятность


def boot_diff(a, b, n_boot=4000, seed=0):
    rng = np.random.default_rng(seed); xa, xb = res[a], res[b]
    d = np.array([rng.choice(xa, len(xa), True).mean() - rng.choice(xb, len(xb), True).mean()
                  for _ in range(n_boot)])
    return float(np.percentile(d, 2.5)), float(np.percentile(d, 97.5)), float((d < 0).mean())


lines += ["## Калибровочный остаток по силе прошлой уверенности", "",
          "Остаток — попал минус обещано моделью. Отрицательный означает "
          "перебор уверенности.", "",
          "| прошлая карта | после апсета | карт | без апсета | карт | разница | 95% ДИ | P(разница<0) |",
          "|---|---:|---:|---:|---:|---:|---|---:|"]
for nm, pm in (("все пары", np.ones(len(cur), bool)),
               ("50–70%", (conf_prev >= 0.5) & (conf_prev < 0.7)),
               ("70–80%", (conf_prev >= 0.7) & (conf_prev < 0.8)),
               ("80%+", conf_prev >= 0.80),
               ("90%+", conf_prev >= 0.90)):
    a, b = pm & upset, pm & ~upset
    if a.sum() < 30 or b.sum() < 30:
        continue
    lo, hi_, pv_ = boot_diff(a, b)
    lines.append(f"| {nm} | {res[a].mean():+.4f} | {int(a.sum()):,} | {res[b].mean():+.4f} | "
                 f"{int(b.sum()):,} | {res[a].mean()-res[b].mean():+.4f} | "
                 f"{lo:+.4f}…{hi_:+.4f} | {pv_:.3f} |")
lines.append("")

# ---------- отдача ----------
trig = (conf_prev >= 0.80) & upset


def roi_ci(x, n_boot=4000, seed=0):
    rng = np.random.default_rng(seed)
    b = np.array([rng.choice(x, len(x), True).mean() for _ in range(n_boot)])
    return np.percentile(b, [2.5, 97.5])


lines += ["## Отдача, если коэффициент ровно 1/вероятность модели", "",
          "| группа | карт | винрейт | обещано | отдача | 95% ДИ |", "|---|---:|---:|---:|---:|---|"]
for nm, m in (("все пары карт", np.ones(len(cur), bool)),
              ("после апсета фаворита 80%+", trig),
              ("из них текущая уверенность 80%+", trig & (conf_cur >= 0.80)),
              ("после апсета фаворита 90%+", (conf_prev >= 0.90) & upset),
              ("все остальные", ~trig)):
    if m.sum() < 30:
        continue
    lo, hi_ = roi_ci(roi[m])
    lines.append(f"| {nm} | {int(m.sum()):,} | {hit_cur[m].mean():.4f} | {conf_cur[m].mean():.4f} | "
                 f"{roi[m].mean():+.2%} | {lo:+.2%}…{hi_:+.2%} |")
lines += ["", "### Размер поправки", "",
          "| вычитаем из вероятности | отдача на затронутых | \|остаток\| |", "|---:|---:|---:|"]
for D in (0.00, 0.04, 0.07, 0.10, 0.15):
    pp = np.where(trig, np.clip(conf_cur - D, 0.51, 0.99), conf_cur)
    rr = hit_cur / pp - 1.0
    lines.append(f"| {D:.2f} | {rr[trig].mean():+.2%} | {abs((hit_cur[trig]-pp[trig]).mean()):.4f} |")
lines.append("")

lines += ["## Как часто модель вообще меняет мнение после апсета", "",
          "| прошлая карта | доля «ставит на того же» | карт |", "|---|---:|---:|"]
for lo, hi_, nm in ((0.5, 0.7, "50–70%"), (0.7, 0.8, "70–80%"), (0.8, 1.01, "80%+")):
    for u, tag in ((True, "фаворит проиграл"), (False, "фаворит выиграл")):
        m = (conf_prev >= lo) & (conf_prev < hi_) & (upset == u)
        if m.sum() < 30:
            continue
        lines.append(f"| {nm}, {tag} | {same_pick[m].mean():.3f} | {int(m.sum()):,} |")
lines.append("")
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
say("\n".join(lines))
