#!/usr/bin/env python3
"""Добор свежих про-карт в корпус: переспрос сид-команд узким окном.

ЗАЧЕМ ОТДЕЛЬНЫЙ ДОБОР, ЕСЛИ ЕСТЬ `get_pros()`

`get_pros()` — снежный ком по КОМАНДАМ, и каждая команда опрашивается ровно
ОДИН раз за всё время: набор посещённых лежит в `visited_teams.json` и переживает
перезапуск. Пока в очереди есть неопрошенные команды, он растит охват. Когда
очередь исчерпана — а она исчерпана, все 200 647 команд отмечены, — вызов
печатает «опрошено 0» и корпус перестаёт пополняться. Без ошибки, без ненулевого
кода возврата, без строки в логе, которую можно было бы заметить.

Именно так корпус и застыл: свежайшая карта 12.08.2026, обнаружено 18.08 по
косвенному признаку — предупреждению ночной пересборки о возрасте снимка.
Снежный ком по устройству не добирает свежие матчи УЖЕ известных команд, а
именно они и составляют поток про-сцены.

ЧТО ДЕЛАЕТ ЭТОТ СКРИПТ

Возвращает сид-команды (tier1 + tier2) в очередь и прогоняет ОДНУ волну. После
волны набор посещённых восстанавливается целиком, поэтому охват снежного кома
не теряется и следующий обычный `get_pros()` отработает как прежде.

ПОЧЕМУ ОКНО

`start_date_time_pro = 1` (1970 год) заставляет тянуть ВСЮ историю команды —
для добора нескольких суток это десятки тысяч лишних запросов к Stratz. Здесь
окно скользящее (по умолчанию 10 суток) и правится ТОЛЬКО В ПАМЯТИ процесса:
`keys.py` не трогается, потому что от той же константы зависят другие сборы.
Запас в 10 суток покрывает несколько пропущенных прогонов подряд; дубли
отсекаются самим сборщиком по match_id.

РАСПИСАНИЕ

Ставится на 04:30 — за час до ночной пересборки снимка (05:30), чтобы снимок
собирался уже на пополненном корпусе. Порядок именно такой, а не наоборот:
пересборка приводит снимок к тому корпусу, который лежит на диске, и сама
корпус не наполняет (см. шапку `scripts/run/rebuild_prematch_snapshot.sh`).

Запуск вручную: venv_catboost/bin/python3 runtime/topup_pro_corpus.py
Окно другое:    TOPUP_DAYS=30 venv_catboost/bin/python3 runtime/topup_pro_corpus.py
"""
from __future__ import annotations

import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))

WINDOW_DAYS = int(os.getenv("TOPUP_DAYS", "10"))
LIMIT_TEAMS = int(os.getenv("TOPUP_LIMIT", "0"))      # 0 = все сиды; иначе проба
STALE_WARN_DAYS = float(os.getenv("TOPUP_STALE_WARN", "3"))
LOCK = ROOT / "runtime" / "topup_pro_corpus.lock"


def say(msg: str) -> None:
    print(msg, flush=True)


def _require_keys() -> None:
    """Явная проверка `keys.py` — молчаливое отсутствие уже стоило пяти суток.

    13.08.2026 файл пропал с машины (он в .gitignore, рядом лежат только .bak),
    и `maps_research` перестал импортироваться вовсе. Ночная пересборка при этом
    продолжала отрабатывать без ошибок — ей `keys` не нужен, — поэтому поломка
    была видна только по возрасту снимка. Здесь она обязана быть громкой.
    """
    try:
        import keys  # noqa: F401
    except ImportError as exc:
        raise SystemExit(
            "СБОР НЕВОЗМОЖЕН: не импортируется base/keys.py "
            f"({exc}). Файл закрыт .gitignore; рядом лежат keys.py.bak_* — "
            "восстановить из самого свежего и проверить пары ключ-прокси.")


def _newest_map_ts(corpus_dir: Path, scan_files: int = 6) -> int:
    """Время самой свежей карты по нескольким последним по mtime файлам.

    Полный обход корпуса стоит минуты и 2.5 ГБ памяти, а для отчёта о свежести
    хватает недавно писанных частей: сборщик дописывает свежие карты именно в них.
    """
    import orjson

    files = sorted((p for p in corpus_dir.glob("*.json")
                    if p.name != "merge_patch_summary.json"),
                   key=lambda p: p.stat().st_mtime, reverse=True)[:scan_files]
    newest = 0
    for path in files:
        try:
            data = orjson.loads(path.read_bytes())
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        for match in data.values():
            if isinstance(match, dict):
                ts = match.get("startDateTime") or 0
                if isinstance(ts, int) and ts > newest:
                    newest = ts
    return newest


def main() -> int:
    _require_keys()

    if LOCK.exists():
        age_min = (time.time() - LOCK.stat().st_mtime) / 60.0
        if age_min < 180:
            say(f"уже идёт другой добор (замок {age_min:.0f} мин назад) — выхожу")
            return 0
        say(f"замок протух ({age_min:.0f} мин) — перехватываю")
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    LOCK.write_text(str(os.getpid()), encoding="utf-8")

    try:
        window_from = int(time.time()) - WINDOW_DAYS * 86400
        import keys
        keys.start_date_time_pro = window_from        # только в памяти процесса
        import maps_research as M
        M.start_date_time = window_from

        corpus = Path(str(M.PRO_HEROES_DIR)) / "json_parts_split_from_object"
        visited_path = Path(str(M.PRO_HEROES_DIR)) / M.PRO_VISITED_TEAMS_FILE

        seeds = M._seed_team_ids()
        if LIMIT_TEAMS:
            seeds = seeds[:LIMIT_TEAMS]
        visited = M._load_visited_teams(str(visited_path))

        # Копия набора посещённых: потеря охвата снежного кома невосстановима
        # без повторного обхода 200 тыс. команд, то есть суток квоты Stratz.
        shutil.copy2(visited_path, str(visited_path) + ".before_topup")

        before_files = len(list(corpus.glob("*.json")))
        say(f"=== {time.strftime('%F %T')} добор про-корпуса ===")
        say(f"окно с {time.strftime('%F', time.gmtime(window_from))} "
            f"({WINDOW_DAYS} сут), сид-команд к переспросу {len(seeds):,}, "
            f"посещённых {len(visited):,}")

        M._save_visited_teams(str(visited_path), visited - set(seeds))
        try:
            M.get_pros(max_waves=1)
        finally:
            # Восстанавливаем ВСЕГДА: обрыв на середине волны не должен оставить
            # 625 команд «неопрошенными» — следующий обычный get_pros() тогда
            # погнал бы их заново и сжёг квоту.
            M._save_visited_teams(
                str(visited_path),
                M._load_visited_teams(str(visited_path)) | visited)

        after_files = len(list(corpus.glob("*.json")))
        newest = _newest_map_ts(corpus)
        lag_days = (time.time() - newest) / 86400.0 if newest else 999.0
        say(f"файлов в корпусе: было {before_files}, стало {after_files}")
        say(f"свежайшая карта: {time.strftime('%F %H:%M', time.gmtime(newest))}, "
            f"отставание {lag_days:.2f} суток")
        if lag_days > STALE_WARN_DAYS:
            say(f"ВНИМАНИЕ: корпус отстаёт на {lag_days:.1f} суток даже после добора — "
                "проверить ключи Stratz и то, что сид-списки tier1/tier2 не пусты; "
                "по E-177 устаревание стоит до 0.04 AUC")
        say(f"=== {time.strftime('%F %T')} готово ===")
        return 0
    finally:
        LOCK.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
