#!/usr/bin/env python3
"""Перебазировать `runtime/live_elo_model_state.json` на свежий ELO-снимок.

ЗАЧЕМ. Ночная цепочка доставляет новый снимок и рестартует прод.
`_load_runtime_model_payload` принимает рантайм-состояние, только если его
`base_reference_timestamp` и сигнатура конфигурации совпадают со снимком
(`live_team_strength.py:590-603`). После доставки они расходятся, payload
отклоняется, и живой процесс уходит в `full_model_state()` — а это разбор всего
`model_state` из снимка, ~3 ГБ RSS, которые арены glibc уже не вернут. Замер
03.09.2026: в логе три строки «[ELO] догружаю полный model_state из снимка», по
одной на каждую доставку снимка, и RSS процесса 6.42 ГБ при slim-загрузке,
которая стоит 0.83 ГБ (E-251).

ЧТО ДЕЛАЕТ. Берёт `model_state` из снимка и пишет рантайм-файл с новыми
`base_reference_timestamp` / `base_model_config_signature`, атомарно (tmp +
os.replace). Накопленные живые обновления при этом сбрасываются — ровно то же
самое рантайм делает при смене базы сам (`:566-573`: progress.appled_maps
обнуляется), только здесь это происходит ДО старта процесса и вне его памяти.

GUARD. Если база УЖЕ совпадает со снимком, файл не трогается вовсе: иначе
перебазировка выбросила бы живые обновления рейтингов, накопленные с момента
сборки снимка, ради ничего. Поэтому инструмент идемпотентен и его можно звать
на каждом рестарте.

КОГДА ЗАПУСКАТЬ. На боевой машине между `systemctl stop` и `systemctl start`
(ночная цепочка, шаг 8): процесс разбора кратковременный и ест ~4 ГБ, при
остановленном проде это безопасно, при работающем — конкурирует с ним за память.
Отказ инструмента НЕ должен блокировать запуск прода: цепочка зовёт его через
`||`, и тогда прод просто заплатит прежние ~3 ГБ однократно.

Запуск: venv/bin/python3 ELO/rebase_runtime_model_state.py [--snapshot PATH] [--state PATH] [--force]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO import live_team_strength as lts  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot", type=Path, default=lts.DEFAULT_SNAPSHOT_PATH)
    parser.add_argument("--state", type=Path, default=lts.DEFAULT_RUNTIME_MODEL_STATE_PATH)
    parser.add_argument("--force", action="store_true",
                        help="перебазировать даже если база уже совпадает "
                             "(сбросит живые обновления рейтингов)")
    args = parser.parse_args(argv)

    if not args.snapshot.exists():
        print(f"ОШИБКА: снимок не найден: {args.snapshot}", file=sys.stderr)
        return 1

    reference = None
    signature = None
    existing = lts._load_json_dict(args.state) if args.state.exists() else None
    if isinstance(existing, dict):
        try:
            reference = int(existing.get("base_reference_timestamp") or 0)
        except (TypeError, ValueError):
            reference = None
        signature = str(existing.get("base_model_config_signature") or "")

    with args.snapshot.open("r", encoding="utf-8") as fh:
        snapshot = json.load(fh)
    want_reference = lts._snapshot_reference_timestamp(snapshot)
    want_signature = lts._snapshot_model_config_signature(snapshot)
    state = snapshot.get("model_state")
    if not isinstance(state, dict):
        print("ОШИБКА: в снимке нет model_state — перебазировать не на что",
              file=sys.stderr)
        return 1

    if not args.force and reference == want_reference and signature == want_signature:
        print(f"база уже совпадает ({want_reference}), рантайм-состояние не тронуто "
              f"— живые обновления сохранены")
        return 0

    payload = {
        "base_reference_timestamp": int(want_reference),
        "base_model_config_signature": str(want_signature),
        "updated_at": int(time.time()),
        "model_state": state,
    }
    lts._write_json_atomic(args.state, payload)
    print(f"перебазировано: {args.state}")
    print(f"  база была: {reference} / {str(signature)[:12]}…")
    print(f"  база стала: {want_reference} / {want_signature[:12]}…")
    print(f"  разделов model_state: {len(state)}, размер файла "
          f"{args.state.stat().st_size / 1048576:.0f} МБ")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
