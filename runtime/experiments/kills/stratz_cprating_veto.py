#!/usr/bin/env python3
"""Годится ли `cprating` как фильтр ставок — против соразмерного потока.

По `stratz_four_metrics.py` рейтинг контрпиков даёт прибавку к AUC словаря `all`,
и тем большую, чем выше индекс: +0.008 на всём тесте и до +0.039 в верхней
десятой. Но направление ставки на уверенных картах он почти не переворачивает
(знак меняется у 0.5% карт верхней десятой), значит прибавка идёт от
переупорядочивания внутри полосы, а не от разворотов.

Отсюда единственная пригодная форма — ФИЛЬТР: ставим, только когда рейтинг
Stratz согласен с направлением словаря. И проверять его надо не против «всех
карт полосы», а против СОРАЗМЕРНОГО ПОТОКА — того же числа карт, отобранного
простым поднятием порога |индекса|. В этой ветке такое сравнение уже четыре раза
переворачивало знак результата.

Запуск: DAYS=365 venv_catboost/bin/python3 runtime/experiments/kills/stratz_cprating_veto.py
"""
from __future__ import annotations

import os, sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
BASE = (ROOT / "runtime/experiments/kills/stratz_four_metrics.py").read_text()
exec(BASE.split('lines = [f"# Четыре метрики')[0])          # noqa: S102 — общая подготовка

OUT = ROOT / "runtime/artifacts/misc/stratz_cprating_veto.md"
cp = METRICS["cprating"]
sa, pa = fit_score([f_all], tr, te)
idx = (pa - 0.5) * 100.0
yy = y[te]
side = np.sign(sa)
hit = np.where(side > 0, yy == 1, yy == 0).astype(float)
agree = np.sign(cp[te]) == side

lines = ["# `cprating` как фильтр: против соразмерного потока", "",
         f"Про-карты за {DAYS} дней, тест {len(te):,}. Направление ставки задаёт "
         f"словарь `all`; фильтр пропускает карту, если знак `cprating` совпал с "
         f"направлением. Колонка «ровный порог» — столько же карт, но отобранных "
         f"просто поднятием |индекса|.", "",
         "| полоса | карт | винрейт полосы | фильтр: согласен | карт | против | ровный порог | фильтр − порог |",
         "|---|---:|---:|---:|---:|---:|---:|---:|"]
for q in (0.0, 0.5, 0.75, 0.9):
    thr = float(np.quantile(np.abs(idx), q))
    m = np.abs(idx) >= thr
    a, b = m & agree, m & ~agree
    if b.sum() < 100:
        continue
    k = int(a.sum())
    pick = np.argsort(-np.abs(idx))[:k]
    wr_thr = hit[pick].mean()
    lines.append(f"| \\|индекс\\| ≥ {thr:.1f} (верх {int(round((1-q)*100))}%) | {int(m.sum()):,} | "
                 f"{hit[m].mean():.4f} | {hit[a].mean():.4f} | {k:,} | "
                 f"{hit[b].mean():.4f} | {wr_thr:.4f} | {hit[a].mean()-wr_thr:+.4f} |")
lines += ["", "Положительная последняя колонка означает, что фильтр даёт больше, "
          "чем то же сужение потока порогом."]
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("\n".join(lines), flush=True)
