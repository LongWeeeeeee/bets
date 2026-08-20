#!/usr/bin/env python3
"""Калибровка «уверенность → винрейт» ОТДЕЛЬНО ПО КАЖДОЙ ВЕТКЕ лестницы.

ЗАЧЕМ. `lan_winrate` переводит уверенность в ожидаемый винрейт, а из него
получается безубыточный коэффициент. Одна таблица на все ветки недопустима: 75%
от восьмиколоночной ветки и 75% от полной — разные вещи, и продавать их
одинаково значит ставить по коэффициенту, который не покрывает риск.

ПОЛНАЯ ВЕТКА НЕ ПЕРЕМЕРЯЕТСЯ. Действующая `LAN_ODDS_GRID` снята на честных
предсказаниях из шести forward-окон (E-142, 2 456 офлайн-карт) — то есть на
популяции, близкой к боевой свежести. Здешний прогон идёт по снимку, обрезанному
на `TEST_FROM`, и снимок в нём стареет на месяцы: по `audit_live_path` AUC падает
с 0.7231 при возрасте до трёх суток до 0.6883 при возрасте больше месяца.
Поэтому здешние винрейты — НИЖНЯЯ оценка, и заменять ими хорошо снятую таблицу
полной ветки нельзя. Она остаётся как есть.

ОСТАЛЬНЫЕ ВЕТКИ ПОЛУЧАЮТ СВОЮ ТАБЛИЦУ ИЛИ НЕ ПОЛУЧАЮТ НИКАКОЙ. Наследовать
таблицу полной ветки для короткой опасно в одну сторону: короткая ветка слабее,
и её настоящий винрейт при той же уверенности ниже. Поэтому полосу, где карт
меньше порога, мы НЕ заполняем — там винрейт не определён, автоматическая ставка
по такой ветке не выставляется, а панель по-прежнему показывает вероятность.
Это ровно принцип модуля: дефолтов нет.

Величина в полосе — односторонняя НИЖНЯЯ граница Уилсона 90%, как в
`venue_calibration_grid`: точечная оценка на сотнях карт гуляет, а нижняя
граница ошибается в безопасную сторону.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/branch_calibration.py
Выход:  runtime/artifacts/misc/branch_calibration.npz + branch_calibration.md
"""
from __future__ import annotations

import math
import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))

import prematch_scorer as ps  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
RAW = ART / "audit_branch_ladder_raw.npz"
OUT_NPZ = ART / "branch_calibration.npz"
OUT_MD = ART / "branch_calibration.md"

BANDS = ((50, 58), (58, 65), (65, 72), (72, 79), (79, 101))
MIN_N = 60
Z = 1.2816   # односторонняя нижняя граница 90%


def wilson_low(k: int, n: int, z: float = Z) -> float:
    """Нижняя граница Уилсона: честнее нормальной на малых выборках и у краёв."""
    if n == 0:
        return 0.0
    p = k / n
    d = 1.0 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, (c - m) / d)


def main() -> None:
    t0 = time.time()
    if not RAW.exists():
        raise SystemExit(f"нет {RAW} — сначала прогнать audit_branch_ladder.py")
    z = np.load(RAW)
    branch, y, p, lan = z["branch"], z["y"], z["p"], z["lan"]
    conf = np.maximum(p, 1.0 - p)
    hit = np.where(p > 0.5, y == 1, y == 0).astype(int)

    lines = ["# Калибровка уверенности по веткам лестницы", "",
             "Считано на БОЕВОМ пути: снимок обрезан по `TEST_FROM`, карты "
             "прогнаны кодом скорера. Величина в полосе — односторонняя нижняя "
             f"граница Уилсона 90%, полоса заполняется только при {MIN_N}+ картах.",
             "",
             "**Полная ветка не перемеряется**: её `LAN_ODDS_GRID` снята на честных "
             "предсказаниях шести forward-окон (E-142), а здешний снимок стареет на "
             "месяцы и даёт заниженные винрейты.", "",
             "**Полоса без данных не заполняется ничем.** Наследовать таблицу полной "
             "ветки опасно в одну сторону: короткая ветка слабее, её настоящий "
             "винрейт при той же уверенности ниже, и наследование продавало бы "
             "завышенный. Там, где винрейта нет, автоматическая ставка по этой ветке "
             "не выставляется.", ""]

    rows = []
    for name in sorted(set(branch.tolist())):
        if name == "full":
            lines += [f"## `{name}`", "",
                      "Оставлена действующая `LAN_ODDS_GRID` (E-142). "
                      f"На этом прогоне ветка сработала на "
                      f"{int((branch == name).sum()):,} картах.", ""]
            continue
        sel_b = branch == name
        lines += [f"## `{name}` — {int(sel_b.sum()):,} карт", "",
                  "| полоса | карт | попаданий | нижняя граница 90% | "
                  "действует у полной | решение |", "|---|---:|---:|---:|---:|---|"]
        for lo, hi in BANDS:
            sel = sel_b & (conf * 100 >= lo) & (conf * 100 < hi)
            n = int(sel.sum())
            mid = (lo + min(hi, 100)) / 2.0
            cur = ps.lan_expected_wr(mid / 100.0)
            if n < MIN_N:
                lines.append(f"| {lo}-{min(hi, 100)}% | {n} | — | — | {cur:.3f} | "
                             f"карт меньше {MIN_N}, винрейт не определён |")
                continue
            k = int(hit[sel].sum())
            low = wilson_low(k, n)
            rows.append((name, lo, min(hi, 100), low, n))
            mark = "ниже" if low < cur else "выше"
            lines.append(f"| {lo}-{min(hi, 100)}% | {n} | {k} | **{low:.3f}** | "
                         f"{cur:.3f} | своя таблица, {mark} общей "
                         f"(кэф {math.ceil(100 / max(low, 1e-9)) / 100:.2f}) |")
        lines.append("")

    if rows:
        np.savez_compressed(
            OUT_NPZ,
            cal_branch=np.array([r[0] for r in rows], dtype="<U32"),
            cal_lo=np.array([r[1] for r in rows], dtype=np.int64),
            cal_hi=np.array([r[2] for r in rows], dtype=np.int64),
            cal_wr=np.array([r[3] for r in rows], dtype=np.float64),
            cal_n=np.array([r[4] for r in rows], dtype=np.int64))
        lines.append(f"Записано {len(rows)} полос в `{OUT_NPZ.name}`.")
    else:
        lines.append("Ни одна полоса не набрала карт — таблица не записана.")
    lines.append(f"\nПрогон занял {time.time() - t0:.0f} c.")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines), flush=True)


if __name__ == "__main__":
    main()
