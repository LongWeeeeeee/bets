#!/usr/bin/env python3
"""Полный повторный обход пабликов: пул и окно — по факту машины, не захардкожены.

ЗАЧЕМ ТАК. Прогон переехал с Mac на serv1 (02.09.2026), а параметры в обоих
случаях одни и те же по смыслу, но разные по источнику:

1. ПУЛ. На serv1 ключи и прокси уже сведены в `keys.py` (пять пар, проверены
   замером 02.09: `probe_stratz_proxies.py` — 5/5 HTTP 200 боевым путём
   запроса). Тогда раннер ничего не переопределяет. На Mac пул в keys.py
   четырёхпарный, и пятая пара задаётся здесь индексным перебором — замер
   показал, что она работает.
2. ОКНО. `start_date_time` = дата ПОСЛЕДНЕГО СОБРАННОГО ФАЙЛА с матчами минус
   одни сутки (перекрытие, чтобы граница окна не потеряла карты). Считается из
   собственного корпуса машины, поэтому на обеих машинах выходит 1786147200
   (2026-08-08T00:00:00 UTC): последние файлы собраны 09.08.2026. Задать руками
   можно переменной START_DATE_TIME.

Курсоры сбрасывает вызывающий скрипт `scripts/run/get_pubs_full_recrawl.sh`
(откладывает, не удаляет): без `processed_ids.txt` множество уже собранных карт
пусто, а курс игроков `processed_ids_to_graph.txt` очищается — обход идёт по
ПОЛНОМУ списку (фаза 2 по E-56: обход помнит только «спрашивали или нет», поэтому
карты, сыгранные игроком ПОСЛЕ его визита, невидимы до следующего круга).

Запуск: <venv>/bin/python3 runtime/experiments/pubs-rebuild/run_full_recrawl.py
"""
from __future__ import annotations

import datetime
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "base"))

import keys  # noqa: E402
import maps_research as mr  # noqa: E402

#: Пятая пара на Mac: прокси[3] + ключ[1] (замер 02.09.2026, HTTP 200).
MAC_PAIRS = [(0, 0), (2, 2), (4, 4), (1, 3), (3, 1)]
#: Служебные json рядом с part-файлами (в т.ч. part_counters.json с 16.09.2026):
#: их mtime — не дата сбора матчей. Единый список живёт в maps_research.
SKIP_SOURCES = set(mr.PUBS_CORPUS_SIDECAR_FILES)


def newest_source_day(source_dir: Path) -> tuple[Path, datetime.datetime]:
    """Последний собранный файл с матчами и его сутки (UTC)."""
    parts = [p for p in source_dir.glob("*.json") if p.name not in SKIP_SOURCES]
    if not parts:
        raise SystemExit(f"в {source_dir} нет собранных файлов — окно считать не из чего")
    newest = max(parts, key=lambda p: p.stat().st_mtime)
    day = datetime.datetime.utcfromtimestamp(newest.stat().st_mtime).replace(
        hour=0, minute=0, second=0, microsecond=0)
    return newest, day


def _day_minus_one(day: datetime.datetime) -> int:
    return int((day - datetime.timedelta(days=1)).replace(
        tzinfo=datetime.timezone.utc).timestamp())


def _parse_iso_utc_day(value: str) -> datetime.datetime:
    ts = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if ts.tzinfo is not None:
        ts = ts.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return ts.replace(hour=0, minute=0, second=0, microsecond=0)


def start_date_time(source_dir: Path) -> int:
    """Окно = сутки последнего собранного файла минус один день (перекрытие).

    На serv1 после 16.09.2026 part-файлов корпуса больше нет (см.
    AGENTS.md/docs/SERVER_LAYOUT.md — корпус остался только на Mac), поэтому
    `source_dir` может быть пуст. Тогда окно берётся из
    `pub_player_steam_ids.json`: сначала `last_crawl_completed_utc` (сутки
    последнего ЗАВЕРШЁННОГО обхода — самый точный источник), иначе
    `source.newest_source_mtime_utc` из провенанса скана корпуса (устарел, но
    лучше, чем ничего). Без обоих источников — явный SystemExit.
    """
    override = str(os.getenv("START_DATE_TIME", "")).strip()
    if override:
        return int(override)
    try:
        newest, day = newest_source_day(source_dir)
    except SystemExit:
        loaded = mr.load_pub_player_ids()
        if loaded is None:
            raise SystemExit(
                f"в {source_dir} нет part-файлов, и {mr.PUBS_PLAYER_IDS_FILE} тоже "
                f"не найден/повреждён — окно считать не из чего; передайте "
                f"START_DATE_TIME явно"
            )
        _ids, meta = loaded
        last_crawl = meta.get("last_crawl_completed_utc")
        if last_crawl:
            day = _parse_iso_utc_day(last_crawl)
            value = _day_minus_one(day)
            print(f"part-файлов нет; окно из last_crawl_completed_utc={last_crawl}", flush=True)
            print(f"окно: сутки {day.date()} минус один день -> {value}", flush=True)
            return value
        newest_mtime = (meta.get("source") or {}).get("newest_source_mtime_utc")
        if newest_mtime:
            day = _parse_iso_utc_day(newest_mtime)
            value = _day_minus_one(day)
            print(f"part-файлов нет; окно из source.newest_source_mtime_utc={newest_mtime}",
                  flush=True)
            print(f"окно: сутки {day.date()} минус один день -> {value}", flush=True)
            return value
        raise SystemExit(
            f"в {mr.PUBS_PLAYER_IDS_FILE} нет ни last_crawl_completed_utc, ни "
            f"source.newest_source_mtime_utc — окно считать не из чего; передайте "
            f"START_DATE_TIME явно"
        )
    value = _day_minus_one(day)
    print(f"последний собранный файл: {newest.name} "
          f"({datetime.datetime.utcfromtimestamp(newest.stat().st_mtime).isoformat()} UTC)",
          flush=True)
    print(f"окно: сутки файла {day.date()} минус один день -> {value}", flush=True)
    return value


def build_pool() -> dict:
    """Пул из keys.py, если он уже свежий, иначе пятая пара добавляется здесь."""
    pool = dict(getattr(keys, "STRATZ_PROXY_MAP", {}) or {})
    fresh = pool and all("142.252." in str(p) for p in pool)
    if fresh:
        print(f"пул взят из keys.py без изменений: {len(pool)} пар", flush=True)
        return pool
    proxies = list(getattr(keys, "stratz_proxies", []) or [])
    klist = list(getattr(keys, "stratz_keys", []) or [])
    if not proxies or not klist:
        raise SystemExit("в keys.py нет ни stratz_proxies, ни рабочего пула")
    pool = {proxies[p]: klist[k] for p, k in MAC_PAIRS}
    print(f"пул собран раннером (пятая пара прокси[3]+ключ[1]): {len(pool)} пар", flush=True)
    return pool


def main() -> None:
    pool = build_pool()
    if len(pool) != 5:
        raise SystemExit(f"ожидалось 5 пар, получилось {len(pool)}")
    mr.STRATZ_PROXY_MAP = pool
    window = start_date_time(mr.PUBS_SOURCE_DIR)
    mr.start_date_time = window
    mr.start_date_time_publick = window

    day = datetime.datetime.utcfromtimestamp(window).isoformat()
    print(f"пул Stratz: {', '.join(u.split('@')[-1] for u in pool)}", flush=True)
    print(f"start_date_time: {window} ({day} UTC)", flush=True)
    print(f"каталог корпуса: {mr.ANALYSE_PUB_DIR}", flush=True)

    mr.get_pubs()


if __name__ == "__main__":
    main()
