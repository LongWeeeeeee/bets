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
import errno
import fcntl
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Только stdlib-модуль с путями: live_team_strength (секунды импорта) здесь НЕ импортируется
# до тех пор, пока не взяты замки, иначе `timeout` мог бы убить процесс посреди импорта,
# пока шаг 8 уже видит свободный замок и неизменные отпечатки.
from ELO import runtime_paths  # noqa: E402

REBASE_LOCK_NAME = "live_elo_rebase.lock"


def _read_lock_pid(path: Path) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return "?"
    first = text.splitlines()[0].strip() if text else ""
    return first if first.isdigit() else "?"


def _lock_dirs(args: argparse.Namespace) -> list[Path]:
    """Каталоги всего, что перебазировка может записать (state, progress, live-дельта).

    `os.replace` пишет путь КАК ЗАДАН: если выходной файл — символическая ссылка, новый
    обычный файл появляется в каталоге самой ссылки, а resolve() называет каталог цели.
    Поэтому для каждого выхода берутся ОБА: resolve().parent и абсолютный, но не
    разрешённый p.parent. Один и тот же каталог под двумя написаниями (ссылка на каталог)
    сводится к одному по (st_dev, st_ino); у ещё не созданного каталога ключ — его путь.
    Результат отсортирован: порядок один у всех писателей."""
    outputs = [Path(p) for p in (args.state, args.progress, runtime_paths.live_delta_path())]
    candidates = [p.resolve().parent for p in outputs]                     # настоящие имена — первыми
    candidates += [(p if p.is_absolute() else Path.cwd() / p).parent for p in outputs]
    found: dict[object, Path] = {}
    for directory in candidates:
        try:
            st = os.stat(directory)
            key: object = (st.st_dev, st.st_ino)
        except OSError:
            key = str(directory)
        found.setdefault(key, directory)
    return sorted(found.values(), key=str)


def _take_rebase_locks(dirs: list[Path]) -> tuple[list[int], int]:
    """Эксклюзивные замки писателя во всех каталогах вывода. ([fd...], 0) — взяты все;
    ([], rc) — отказ до любой записи (взятые к этому моменту отпущены).

    Файл открывается БЕЗ O_TRUNC: до получения замка чужой pid в нём затирать нельзя.
    pid пишется только когда взяты ВСЕ замки. fd не наследуются дочерними процессами
    (PEP 446), так что замки живут ровно столько, сколько этот процесс.
    """
    fds: list[int] = []
    seen: set[tuple[int, int]] = set()

    def release() -> None:
        for held in fds:
            os.close(held)

    for directory in dirs:
        lock_path = directory / REBASE_LOCK_NAME
        try:
            directory.mkdir(parents=True, exist_ok=True)
            fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        except OSError as exc:
            print(f"ОШИБКА: не удалось открыть замок перебазировки {lock_path} ({exc})",
                  file=sys.stderr)
            release()
            return [], 1
        try:
            st = os.fstat(fd)
            if (st.st_dev, st.st_ino) in seen:   # тот же файл по другому пути (ссылка): второй flock занял бы сам себя
                os.close(fd)
                continue
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            release()
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                print(f"ОШИБКА: перебазировка уже выполняется (замок {lock_path}, "
                      f"pid {_read_lock_pid(lock_path)})", file=sys.stderr)
                return [], 3
            print(f"ОШИБКА: не удалось взять замок перебазировки {lock_path} ({exc})",
                  file=sys.stderr)
            return [], 1
        seen.add((st.st_dev, st.st_ino))
        fds.append(fd)
    for fd in fds:
        try:
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode("ascii"))
        except OSError:
            pass  # замок взят; pid в файле нужен только для диагностики и SIGKILL-охранника
    return fds, 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--snapshot", type=Path, default=runtime_paths.DEFAULT_SNAPSHOT_PATH)
    parser.add_argument("--state", type=Path, default=runtime_paths.DEFAULT_RUNTIME_MODEL_STATE_PATH)
    parser.add_argument("--progress", type=Path, default=runtime_paths.DEFAULT_RUNTIME_PROGRESS_PATH)
    parser.add_argument("--force", action="store_true",
                        help="перебазировать даже если база уже совпадает "
                             "(пересобрать из снимка и live-ledger)")
    args = parser.parse_args(argv)

    # Замки раньше всего (и раньше импорта live_team_strength): ни чтения снимка, ни
    # записи, пока писатель не единственный во ВСЕХ каталогах вывода.
    lock_fds, lock_rc = _take_rebase_locks(_lock_dirs(args))
    if lock_rc:
        return lock_rc
    try:
        return _rebase(args)
    finally:
        for fd in lock_fds:
            os.close(fd)


def _rebase(args: argparse.Namespace) -> int:
    global lts
    from ELO import live_team_strength as lts  # noqa: PLC0415  (только под замками)

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
