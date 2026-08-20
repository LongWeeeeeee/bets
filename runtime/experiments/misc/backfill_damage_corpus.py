#!/usr/bin/env python3
"""Добор разборов OpenDota НАЗАД по времени — чтобы справочник урона можно было
построить причинно.

ЗАЧЕМ. Матчап по урону — единственная величина, добавляющая к классическому
cp1vs1 (E-206, +0.0098). Но проверить его против БОЕВЫХ 35 честно нельзя: сбор
покрывает 24.03–12.08.2026, а тестовое окно начинается 29.03. Из 40 248
разобранных карт до `TEST_FROM` лежат ровно 1 118, и при пороге 25 встреч
причинный справочник получается ПУСТОЙ — ноль ячеек (E-214 §4). Поэтому все
существующие числа против боевой базы протекают на 100%.

Список id генерировать неоткуда не надо: `mids` про-корпуса это и есть id
матчей. Берём 180 суток перед тестом — 35 281 карта, столько же, сколько в
нынешнем справочнике (33 049), но строго ДО тестового окна.

Идёт через прокси (`include_direct=False`): свой IP не занимаем, с него в
OpenDota ходит прод. Четыре адреса по 3 000 запросов в сутки = 12 000, то есть
около трёх суток. Скрипт продолжает с места остановки: `done_ids` читает уже
собранное.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/backfill_damage_corpus.py
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))

import opendota_research as OD  # noqa: E402
from ideas_batch2 import COMPACT, TEST_FROM  # noqa: E402

IDS = ROOT / "runtime/od_ids_backfill_180d.txt"
OUT_DIR = ROOT / "runtime/artifacts/misc/opendota"
DAYS = int(os.getenv("BACKFILL_DAYS", "180"))


def write_ids() -> int:
    z = np.load(COMPACT)
    lo = TEST_FROM - DAYS * 86400
    sel = (z["ts"] >= lo) & (z["ts"] < TEST_FROM)
    ids = sorted({int(m) for m in z["mids"][sel].tolist()}, reverse=True)
    IDS.write_text("\n".join(str(i) for i in ids) + "\n", encoding="utf-8")
    print(f"окно {time.strftime('%d.%m.%Y', time.localtime(lo))} — "
          f"{time.strftime('%d.%m.%Y', time.localtime(TEST_FROM))}: "
          f"{len(ids):,} карт", flush=True)
    return len(ids)


def main() -> None:
    n = write_ids()
    have = OD.done_ids(str(OUT_DIR))
    todo = n - len(set(int(x) for x in open(IDS).read().split()) &
                   set(int(x) for x in have))
    print(f"уже собрано всего {len(have):,}; из списка осталось ~{todo:,} "
          f"(~{todo / 12000:.1f} суток при четырёх прокси)", flush=True)
    OD.collect_pro_patch(str(IDS), str(OUT_DIR))


if __name__ == "__main__":
    main()
