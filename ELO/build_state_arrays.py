#!/usr/bin/env python3
"""Собрать sidecar `.npz` с массивами `model_state` — вне живого процесса.

ЗАЧЕМ. `array_model.load_state_arrays` собирает хранилища потоком из JSON через
`list.append` на миллионах элементов: на боевом состоянии (519 МБ) это 1140 МБ
RSS и ~60 c, хотя самих данных 74 МБ (замер E-253, харнесс
`runtime/experiments/cyberscore-prod/rss_attribution.py`). Sidecar переносит
эту работу из живого процесса в сборку: `load_state_arrays_cached` читает
готовые массивы memcpy'ем, а хранилища собирает ТА ЖЕ функция
`_stores_from_raw`, поэтому два пути не могут разъехаться.

КОГДА. Ночная цепочка после доставки снимка (шаг 5c в
`scripts/run/rebuild_prematch_snapshot.sh`) и вручную после любой замены
снимка/состояния. Sidecar штампуется mtime_ns и размером источника: как только
файл-источник меняется, sidecar считается устаревшим и модель собирается потоком
— то есть протухший sidecar не может отдать старые числа.

Запуск:
    venv/bin/python3 ELO/build_state_arrays.py                  # снимок
    venv/bin/python3 ELO/build_state_arrays.py --src <файл> [--out <npz>]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO import array_model  # noqa: E402
from ELO.live_team_strength import (DEFAULT_RUNTIME_MODEL_STATE_PATH,  # noqa: E402
                                    DEFAULT_SNAPSHOT_PATH)


def _rss_mb() -> float:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1048576.0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--src", type=Path, default=DEFAULT_SNAPSHOT_PATH,
                        help="файл с model_state (снимок или рантайм-состояние)")
    parser.add_argument("--runtime-state", action="store_true",
                        help="брать runtime/live_elo_model_state.json вместо снимка")
    parser.add_argument("--out", type=Path, default=None,
                        help="куда положить .npz (по умолчанию <src>.arrays.npz)")
    args = parser.parse_args(argv)

    src = DEFAULT_RUNTIME_MODEL_STATE_PATH if args.runtime_state else args.src
    if not src.exists():
        print(f"ОШИБКА: источник не найден: {src}", file=sys.stderr)
        return 1

    before = _rss_mb()
    t0 = time.time()
    out = array_model.save_state_arrays(src, "model_state.", args.out)
    elapsed = time.time() - t0
    size = out.stat().st_size / 1048576

    print(f"источник: {src} ({src.stat().st_size / 1048576:.0f} МБ)")
    print(f"sidecar:  {out} ({size:.0f} МБ) за {elapsed:.0f} c")
    print(f"RSS сборщика: {before:.0f} -> {_rss_mb():.0f} МБ")

    # Контроль: sidecar обязан читаться и давать те же хранилища, что поток.
    if not array_model._sidecar_matches(out, src, "model_state."):
        print("ОШИБКА: штамп sidecar не совпал с источником", file=sys.stderr)
        return 1
    print("штамп sidecar совпадает с источником — годен к употреблению")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
