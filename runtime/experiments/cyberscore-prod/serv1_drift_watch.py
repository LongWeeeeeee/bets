#!/usr/bin/env python3
"""Сторож расхождений: боевой код serv1 против репозитория.

ЧТО ДЕЛАЕТ. Раз в сутки забирает с serv1 только Python-код, кладёт его в ветку
`serv1-prod-snapshot` и сообщает, что изменилось с прошлого раза. На serv1
НИЧЕГО НЕ ПИШЕТ — ни файлов, ни рестартов.

ПОЧЕМУ НЕ АВТОСИНХРОНИЗАЦИЯ. Репозиторий и прод не отстают друг от друга, а
РАЗОШЛИСЬ форком: каждая сторона содержит то, чего нет у другой (аудит
19.08.2026). `git pull` на serv1 не упал бы, а тихо подменил гибридную
предматчевую модель (AUC ~0.72) чистым драфтом (~0.61) и затёр бы
`id_to_names.py`, который дописывает сам боевой процесс. Выравнивание здесь —
не лечение, а автоматизация аварии. Поэтому сторож только СМОТРИТ.

ЧТО ЗАКРЫВАЕТ. Две дыры сразу:
  * боевой код перестаёт существовать в единственном экземпляре на одной машине
    — он ежедневно оказывается в git;
  * новый дрейф виден назавтра, а не через месяцы, как вышло с ядром расчёта.

ЗАПУСК ВРУЧНУЮ:  venv_catboost/bin/python3 runtime/serv1_drift_watch.py
БЕЗ ЗАПИСИ:      DRIFT_DRY_RUN=1 venv_catboost/bin/python3 runtime/serv1_drift_watch.py
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
BRANCH = "serv1-prod-snapshot"
REMOTE = "serv1"
REMOTE_ROOT = "/root/main"
DIRS = ("base", "ELO")
DRY_RUN = os.getenv("DRIFT_DRY_RUN", "0") == "1"

# Имена, которые НЕЛЬЗЯ забирать ни при каких условиях. Порядок правил rsync
# имеет значение: исключения обязаны идти ПЕРЕД `--include='*.py'`, иначе
# включение срабатывает первым и секрет уезжает. На этом уже спотыкались.
SECRETS = ("keys.py", "keys.py.bak*", "keys_local.py")


def say(msg: str) -> None:
    print(msg, flush=True)


def run(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None,
                          capture_output=True, text=True)


def ensure_branch() -> None:
    """Ветка-снимок должна существовать; создаём от main, если её нет."""
    if run(["git", "rev-parse", "--verify", BRANCH], REPO).returncode == 0:
        return
    say(f"ветки {BRANCH} нет — создаю от main")
    res = run(["git", "branch", BRANCH, "main"], REPO)
    if res.returncode != 0:
        raise SystemExit(f"не удалось создать ветку: {res.stderr.strip()}")


def pull_code(dest: Path) -> None:
    """Забрать .py с serv1. Секреты исключаются ДО включений."""
    for name in DIRS:
        args = ["rsync", "-a", "--delete"]
        for pat in SECRETS:
            args += ["--exclude", pat]
        args += ["--exclude", "__pycache__/", "--exclude", "runtime/",
                 "--include", "*/", "--include", "*.py", "--exclude", "*"]
        args += [f"{REMOTE}:{REMOTE_ROOT}/{name}/", str(dest / name) + "/"]
        res = subprocess.run(args, capture_output=True, text=True)
        if res.returncode != 0:
            raise SystemExit(f"rsync {name} не отработал: {res.stderr.strip()[:300]}")


def assert_no_secrets(dest: Path) -> None:
    """Отдельная проверка после копирования, а не вместо неё.

    Правила rsync легко переставить местами и не заметить; поэтому наличие
    секрета проверяется фактом, а не доверием к аргументам.
    """
    bad = []
    for pat in ("keys.py", "keys_local.py"):
        bad += [p for p in dest.rglob(pat)]
    bad += [p for p in dest.rglob("keys.py.bak*")]
    if bad:
        for p in bad:
            p.unlink(missing_ok=True)
        raise SystemExit("СЕКРЕТЫ ПОПАЛИ В СНИМОК и были удалены: "
                         + ", ".join(str(p) for p in bad)
                         + ". Коммит не сделан — проверить порядок правил rsync.")


def main() -> int:
    ensure_branch()
    work = Path(tempfile.mkdtemp(prefix="drift-"))
    tree = work / "snap"
    try:
        res = run(["git", "worktree", "add", "--force", str(tree), BRANCH], REPO)
        if res.returncode != 0:
            raise SystemExit(f"worktree не создан: {res.stderr.strip()}")

        pull_code(tree)
        assert_no_secrets(tree)

        run(["git", "add", "-A"] + list(DIRS), tree)
        changed = run(["git", "diff", "--cached", "--name-only"], tree).stdout.split()

        if not changed:
            say(f"=== {time.strftime('%F %T')} расхождений с прошлым снимком нет ===")
            return 0

        say(f"=== {time.strftime('%F %T')} боевой код изменился с прошлого снимка ===")
        stat = run(["git", "diff", "--cached", "--numstat"], tree).stdout.strip()
        for line in stat.splitlines():
            add, rem, path = (line.split("\t") + ["", "", ""])[:3]
            say(f"  +{add:<6} -{rem:<6} {path}")
        say(f"файлов изменилось: {len(changed)}")

        if DRY_RUN:
            say("DRIFT_DRY_RUN=1 — коммит не делаю")
            return 0

        msg = (f"снимок боевого кода serv1 {time.strftime('%F')}: "
               f"{len(changed)} файлов изменилось\n\n"
               "Автоматический снимок сторожа расхождений. На serv1 ничего не\n"
               "писалось. Разбор различий и запрет на слияние — docs/SERV1_SNAPSHOT.md")
        res = run(["git", "commit", "-q", "-m", msg], tree)
        if res.returncode != 0:
            raise SystemExit(f"коммит не прошёл: {res.stderr.strip()[:300]}")
        say(f"снимок зафиксирован в ветке {BRANCH}")
        return 0
    finally:
        run(["git", "worktree", "remove", str(tree), "--force"], REPO)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
