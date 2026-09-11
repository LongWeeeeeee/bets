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

ЧТО ДЕЛАЕТ. Берёт `model_state` из снимка и перебирает сохранённый live-ledger:
результаты, которых снимок точно ещё не содержит, накладываются на новую базу;
покрытые снимком не применяются второй раз. Неоднозначное пересечение или
старый ledger без полного контекста останавливают перебазировку до записи.

GUARD. Если база УЖЕ совпадает со снимком, файл не трогается вовсе: иначе
перебазировка выбросила бы живые обновления рейтингов, накопленные с момента
сборки снимка, ради ничего. Поэтому инструмент идемпотентен и его можно звать
на каждом рестарте.

КОГДА ЗАПУСКАТЬ. На боевой машине между `systemctl stop` и `systemctl start`
(ночная цепочка, шаг 8): процесс разбора кратковременный и ест ~4 ГБ, при
остановленном проде это безопасно, при работающем — конкурирует с ним за память.
Код возврата `1` означает отказ до записи либо полный rollback — старая база
может быть запущена. Код `2` означает, что rollback не доказан: promotion
снимка и запуск сервиса надо оставить остановленными до ручной проверки.

Запуск: venv/bin/python3 ELO/rebase_runtime_model_state.py [--snapshot PATH] [--state PATH] [--progress PATH] [--force]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO import live_team_strength as lts  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot", type=Path, default=lts.DEFAULT_SNAPSHOT_PATH)
    parser.add_argument("--state", type=Path, default=lts.DEFAULT_RUNTIME_MODEL_STATE_PATH)
    parser.add_argument("--progress", type=Path, default=lts.DEFAULT_RUNTIME_PROGRESS_PATH)
    parser.add_argument("--force", action="store_true",
                        help="перебазировать даже если база уже совпадает "
                             "(пересобрать из снимка и live-ledger)")
    args = parser.parse_args(argv)

    if not args.snapshot.exists():
        print(f"ОШИБКА: снимок не найден: {args.snapshot}", file=sys.stderr)
        return 1

    with args.snapshot.open("r", encoding="utf-8") as fh:
        snapshot = json.load(fh)
    want_reference = lts._snapshot_reference_timestamp(snapshot)
    want_signature = lts._snapshot_model_config_signature(snapshot)
    try:
        changed = lts.rebase_runtime_model_state(
            snapshot=snapshot,
            runtime_model_state_path=args.state,
            progress_path=args.progress,
            force=args.force,
            create_if_absent=True,
        )
    except lts.RuntimeRebaseRollbackError as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 2
    except lts.RuntimeRebaseError as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"ОШИБКА: частичная запись runtime state/progress ({exc}); "
              "повторите перебазировку до promotion снимка", file=sys.stderr)
        return 2

    if not changed:
        print(f"база уже совпадает ({want_reference}), рантайм-состояние и progress не тронуто "
              f"— живые обновления сохранены")
        return 0

    print(f"перебазировано: {args.state}")
    print(f"  база стала: {want_reference} / {want_signature[:12]}…")
    print(f"  размер файла: {args.state.stat().st_size / 1048576:.0f} МБ")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
