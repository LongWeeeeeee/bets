#!/usr/bin/env python3
"""Во что обходится устаревание снимка: карты, сыгранные ПОСЛЕ его среза.

ЗАМЕЧАНИЕ alex: «всё равно мы должны учитывать винрейт игроков, винрейт героев
игроков, их форму — предыдущие карты должны учитываться».

СТРУКТУРНО ОН ПРАВ. Из 35 боевых признаков живой ровно один — `hybrid_strength`
из пакета `ELO/` (и тот не обновляется, E-224). Остальные читаются из снимка
`data/prematch_model_artifact_v3.npz`: 21 признак игроков (`form`, `games`,
`hero_games`, `imp_recent`, `gpm_ewma`, `kda_player` и прочие) по таблице
аккаунтов, шесть драфтовых по таблицам героев, очные — по истории организаций.
Снимок пересобирается раз в сутки: на 22.08.2026 в нём `snapshot_ts` = 21.08
23:28 UTC, то есть отставание 8.5 часа.

ВОПРОС ЗДЕСЬ. Если карты, сыгранные после среза, для модели невидимы, то её
уверенность должна портиться тем сильнее, чем больше их накопилось. Считается
калибровочный остаток «попал минус обещано» в разрезе числа карт, сыгранных
командой с начала суток снимка. Остаток берётся, а не винрейт: уверенность
сама по себе задаёт винрейт, и без её вычитания сравнивались бы разные группы.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/prematch_snapshot_staleness.py
"""
from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
ART = ROOT / "runtime/artifacts/misc"
S = Path("/private/tmp/claude-501/-Users-alex-Documents-ingame/cb51a5f6-8092-496c-b4a9-fbfa39120e3b/scratchpad")
OUT = ART / "prematch_snapshot_staleness.md"
#: Снимок собирается ночью; за границу суток берётся 23:30 UTC.
DAY_CUT_HOUR = 23.5
say = lambda *a: print(*a, flush=True)

z = np.load(S / "prod_prematch_probs.npz")
zc = np.load(ART / "pro_corpus_compact.npz", allow_pickle=True)
pos = {int(x): i for i, x in enumerate(zc["mids"].tolist())}
ci = np.array([pos[int(m)] for m in z["mids"].tolist()])
P, T, ts, wins = z["p"], zc["teams"][ci], zc["ts"][ci], zc["wins"][ci].astype(int)
conf = np.maximum(P, 1 - P)
hit = np.where(P >= 0.5, wins == 1, wins == 0).astype(float)
res = hit - conf
say(f"вердиктов: {len(P):,}")

# сколько карт команда уже сыграла с последней сборки снимка
day = np.floor((ts - int(DAY_CUT_HOUR * 3600)) / 86400).astype(np.int64)
seen: dict = defaultdict(int)
prior = np.zeros(len(P), int)
order = np.argsort(ts, kind="mergesort")
for i in order:
    a, b = int(T[i, 0]), int(T[i, 1])
    d = int(day[i])
    prior[i] = max(seen[(a, d)], seen[(b, d)])
    seen[(a, d)] += 1
    seen[(b, d)] += 1

lines = ["# Устаревание снимка: карты, сыгранные после его среза", "",
         f"Вердиктов {len(P):,}. «Карт до этой» — сколько карт та же команда уже "
         f"сыграла в этих сутках снимка, то есть сколько её результатов модель "
         f"НЕ видит. Остаток — попал минус обещано; минус означает перебор "
         f"уверенности.", "",
         "| карт до этой | вердиктов | доля | уверенность | винрейт | остаток |",
         "|---|---:|---:|---:|---:|---:|"]
for lo, hi, tag in ((0, 1, "0 — первая карта дня"), (1, 2, "1"), (2, 3, "2"),
                    (3, 5, "3–4"), (5, 99, "5 и больше")):
    m = (prior >= lo) & (prior < hi)
    if m.sum() < 50:
        continue
    lines.append(f"| {tag} | {int(m.sum()):,} | {m.mean():.1%} | {conf[m].mean():.4f} | "
                 f"{hit[m].mean():.4f} | {res[m].mean():+.4f} |")
lines.append("")

# то же на ставочной популяции
bet = (conf - 0.5) * 100 >= 8
lines += ["## Только ставки (|индекс| ≥ 8)", "",
          "| карт до этой | ставок | винрейт | обещано | остаток |", "|---|---:|---:|---:|---:|"]
for lo, hi, tag in ((0, 1, "0"), (1, 2, "1"), (2, 3, "2"), (3, 99, "3 и больше")):
    m = bet & (prior >= lo) & (prior < hi)
    if m.sum() < 40:
        continue
    lines.append(f"| {tag} | {int(m.sum()):,} | {hit[m].mean():.4f} | {conf[m].mean():.4f} | "
                 f"{res[m].mean():+.4f} |")
lines.append("")


def boot(a, b, n=4000, seed=0):
    rng = np.random.default_rng(seed)
    d = np.array([rng.choice(res[a], a.sum(), True).mean() -
                  rng.choice(res[b], b.sum(), True).mean() for _ in range(n)])
    return float(np.percentile(d, 2.5)), float(np.percentile(d, 97.5)), float((d < 0).mean())


first, later = prior == 0, prior >= 2
if first.sum() > 50 and later.sum() > 50:
    lo, hi, p = boot(later, first)
    lines += [f"Разница «две и более карты до этой» минус «первая карта дня»: "
              f"{res[later].mean()-res[first].mean():+.4f} "
              f"[{lo:+.4f}…{hi:+.4f}], P(хуже) = {p:.3f}.", ""]
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
say("\n".join(lines))
