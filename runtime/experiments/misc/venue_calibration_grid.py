#!/usr/bin/env python3
"""Разделение калибровки «уверенность → винрейт» по ПЛОЩАДКЕ (офлайн/онлайн).

ЗАЧЕМ. `LAN_ODDS_GRID` измерена на настоящих офлайн-турнирах, но `model_bet`
отдаёт `expected_wr` и `min_odds` для ЛЮБОГО матча — проверки на офлайн в коде
нет (`audit_live_path.md` §4). Цена видна в том же отчёте: на офлайне при
уверенности ≥75% факт 84.1% против заявленных 79.7%, на онлайне 79.4% против
78.4%. То есть для онлайна таблица почти точна, а на офлайне мы недобираем около
четырёх пунктов: требуем кэф выше, чем нужно, и пропускаем годные ставки.

НА ЧЁМ СЧИТАЕТСЯ. На БОЕВЫХ значениях признаков, а не на обучающих. Массивы
берутся из `audit_live_path_raw.npz`, который пишет сквозной аудит: он прогоняет
тестовые карты через код самого скорера со снимком строго из прошлого. Считать
это на `train_columns` нельзя — корреляция «обучение против боя» у трети колонок
0.4-0.7, и на обучающем представлении та же проверка завышала разрыв втрое
(10 пунктов вместо 3.5).

ПРАВИЛО ПРАВКИ — ТОЛЬКО ВВЕРХ И ТОЛЬКО ПО НИЖНЕЙ ГРАНИЦЕ. Офлайн-выборка мала
(около 600 карт против 2 456, на которых снята действующая таблица), поэтому
точечная оценка ненадёжна. Берём одностороннюю нижнюю границу Уилсона и заменяем
значение, ТОЛЬКО если она выше нынешнего. Так правка может добавить запас, где
данные его выдерживают, и нигде не снимает нынешнюю осторожность: если выборки
не хватает, остаётся действующее число.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/venue_calibration_grid.py
Выход:  runtime/artifacts/misc/venue_calibration_grid.md + venue_grid.json
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))

ART = ROOT / "runtime/artifacts/misc"
RAW = ART / "audit_live_path_raw.npz"
OUT_MD = ART / "venue_calibration_grid.md"
OUT_JSON = ART / "venue_grid.json"

# Границы полос уверенности. Уже, чем у действующей таблицы, быть не может:
# офлайн-карт мало, и в узкой полосе останутся единицы.
BANDS = ((50, 58), (58, 65), (65, 72), (72, 79), (79, 101))
MIN_N = 60          # меньше — полосу не трогаем, оставляем действующее значение
Z = 1.2816          # односторонняя нижняя граница 90%


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
        raise SystemExit(f"нет {RAW} — сначала прогнать audit_live_path.py")
    z = np.load(RAW)
    conf, hit, is_lan = z["conf"], z["hit"], z["is_lan"]
    import prematch_scorer as ps

    lines = ["# Калибровка по площадке: офлайн против онлайна", ""]
    lines.append("Считано на БОЕВЫХ значениях признаков (`audit_live_path_raw.npz`), "
                 "не на обучающих. Замена значения — только если односторонняя нижняя "
                 f"граница Уилсона (90%) ВЫШЕ действующего и в полосе не меньше "
                 f"{MIN_N} карт. Правка может лишь добавить запас, снять осторожность "
                 "она не может.")
    lines.append("")
    lines.append(f"Карт всего: {len(conf):,}, офлайн {int((is_lan > 0).sum()):,}, "
                 f"онлайн {int((is_lan == 0).sum()):,}.")
    lines.append("")

    grid: dict[str, dict[str, float]] = {"lan": {}, "online": {}}
    for tag, name, msk in (("офлайн (LAN)", "lan", is_lan > 0),
                           ("онлайн", "online", is_lan == 0)):
        lines += [f"## {tag}", "",
                  "| полоса уверенности | карт | факт | нижняя граница 90% | "
                  "действует сейчас | решение |", "|---|---:|---:|---:|---:|---|"]
        for lo, hi in BANDS:
            sel = msk & (conf * 100 >= lo) & (conf * 100 < hi)
            n = int(sel.sum())
            mid = (lo + min(hi, 100)) / 2.0
            cur = ps.lan_expected_wr(mid / 100.0)
            if n < MIN_N:
                lines.append(f"| {lo}-{min(hi,100)}% | {n} | — | — | {cur:.3f} | "
                             f"карт меньше {MIN_N}, оставляем действующее |")
                grid[name][f"{lo}-{min(hi,100)}"] = float(cur)
                continue
            k = int(hit[sel].sum())
            fact = k / n
            low = wilson_low(k, n)
            if low > cur:
                grid[name][f"{lo}-{min(hi,100)}"] = float(low)
                verdict = f"**поднимаем до {low:.3f}** (кэф {math.ceil(100/low)/100:.2f})"
            else:
                grid[name][f"{lo}-{min(hi,100)}"] = float(cur)
                verdict = "нижняя граница не выше действующего — не трогаем"
            lines.append(f"| {lo}-{min(hi,100)}% | {n} | {fact:.3f} | {low:.3f} | "
                         f"{cur:.3f} | {verdict} |")
        lines.append("")

    OUT_JSON.write_text(json.dumps(
        {"built_ts": None, "bands": [list(b) for b in BANDS],
         "min_n": MIN_N, "z": Z, "grid": grid},
        ensure_ascii=False, indent=1), encoding="utf-8")
    lines.append(f"Сетка записана в `{OUT_JSON.name}`.")
    lines.append(f"\nПрогон занял {time.time() - t0:.0f} c.")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines), flush=True)


if __name__ == "__main__":
    main()
