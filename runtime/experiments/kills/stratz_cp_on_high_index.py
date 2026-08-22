#!/usr/bin/env python3
"""Дают ли `cpwinrate` и `cprating` доп. информацию НА ВЫСОКИХ индексах словаря.

Прошлый замер брал метрику как непрерывный признак по всей полосе. Если сигнал
живёт только в хвостах метрики, такое усреднение его размывает. Здесь двойное
условие: полоса по |индексу| словаря `all` cp1vs1 И собственная величина метрики.

Метрика приводится к стороне ставки (умножается на знак решения словаря), так что
плюс — «Stratz за наше направление», минус — «против». Каждая метрика переведена
в тот же боевой масштаб индекса, (p − 0.5) × 100, логистикой, обученной на первых
70% карт по времени; полосы и квинтили нарезаются только на тестовой трети.

Соразмерный поток обязателен: любой отбор сравнивается с тем же числом карт,
набранным простым поднятием |индекса| словаря.

Запуск: DAYS=365 venv_catboost/bin/python3 runtime/experiments/kills/stratz_cp_on_high_index.py
"""
from __future__ import annotations

import os, sys
from pathlib import Path

import numpy as np
from sklearn.metrics import roc_auc_score

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
exec((ROOT / "runtime/experiments/kills/stratz_four_metrics.py").read_text()
     .split('lines = [f"# Четыре метрики')[0])            # noqa: S102

OUT = ROOT / "runtime/artifacts/misc/stratz_cp_on_high_index.md"
yy = y[te]
sa, pa = fit_score([f_all], tr, te)
idx_all = (pa - 0.5) * 100.0
side = np.sign(sa)
hit = np.where(side > 0, yy == 1, yy == 0).astype(float)
absidx = np.abs(idx_all)
order_idx = np.argsort(-absidx)

lines = ["# `cpwinrate` и `cprating` на высоких индексах словаря `all`", "",
         f"Про-карты за {DAYS} дней, тест {len(te):,}. Направление ставки задаёт "
         f"словарь; метрика приведена к стороне ставки. Полосы — по |индексу| "
         f"словаря, внутри полосы карты разложены по квинтилям метрики.", ""]

for mname in ("cpwinrate", "cprating"):
    v = METRICS[mname]
    _, pm = fit_score([v], tr, te)
    idx_m = (pm - 0.5) * 100.0 * side                     # + = Stratz за наше направление
    lines += [f"## {mname}", "",
              "| полоса словаря | карт | AUC метрики в полосе | Q1 (против) | Q2 | Q3 | Q4 | Q5 (за) | Q5 − Q1 |",
              "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for q, tag in ((0.0, "все"), (0.5, "верх 50%"), (0.75, "верх 25%"), (0.9, "верх 10%")):
        thr = float(np.quantile(absidx, q)); m = absidx >= thr
        if m.sum() < 500:
            continue
        auc_in = (roc_auc_score(yy[m], (idx_m * side)[m])
                  if yy[m].min() != yy[m].max() else np.nan)
        cuts = np.quantile(idx_m[m], [0.2, 0.4, 0.6, 0.8])
        g = np.digitize(idx_m[m], cuts)
        cells, hs = [], []
        for gi in range(5):
            mm = g == gi
            h = hit[m][mm].mean() if mm.sum() else np.nan
            hs.append(h); cells.append(f"{h:.4f} ({int(mm.sum()):,})")
        lines.append(f"| {tag} (\\|индекс\\| ≥ {thr:.1f}) | {int(m.sum()):,} | {auc_in:.4f} | "
                     + " | ".join(cells) + f" | {hs[4]-hs[0]:+.4f} |")
    lines.append("")

    # верхние квинтили против соразмерного потока
    lines += [f"### {mname}: отбор по метрике против ровного порога", "",
              "| полоса словаря | отбор | карт | винрейт отбора | ровный порог | разница |",
              "|---|---|---:|---:|---:|---:|"]
    for q, tag in ((0.0, "все"), (0.5, "верх 50%"), (0.75, "верх 25%"), (0.9, "верх 10%")):
        thr = float(np.quantile(absidx, q)); m = absidx >= thr
        if m.sum() < 500:
            continue
        for frac, lab in ((0.4, "верхние 40% метрики"), (0.2, "верхние 20% метрики")):
            cut = np.quantile(idx_m[m], 1 - frac)
            pick = m & (idx_m >= cut)
            k = int(pick.sum())
            if k < 150:
                continue
            base = hit[order_idx[:k]].mean()
            lines.append(f"| {tag} | {lab} | {k:,} | {hit[pick].mean():.4f} | "
                         f"{base:.4f} | {hit[pick].mean()-base:+.4f} |")
    lines.append("")

lines.append("«Ровный порог» — столько же карт, отобранных просто поднятием "
             "|индекса| словаря. Положительная разница означает, что метрика "
             "отбирает лучше, чем то же сужение потока.")
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("\n".join(lines), flush=True)
