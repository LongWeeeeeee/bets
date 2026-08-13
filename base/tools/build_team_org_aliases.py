#!/usr/bin/env python3
"""Справочник написаний команд из ЦЕПОЧЕК ПЕРЕИМЕНОВАНИЙ, а не из склейки составов.

Зачем отдельный критерий. Ростерная склейка (`add_org_identity`, пересечение
>= 4 из 5) объединяет team_id в организацию и для ELO с историей встреч это
верно: сила едет с составом. Для поиска карточки у БУКМЕКЕРА она не годится —
там имя обозначает организацию, а склейка ловит ещё и переходы игроков. Замер
13.08 на 64 организациях с двумя и более именованными тегами:

    aurora   <- talon            (обе продолжают играть)
    invictus <- g2xig            (обе продолжают играть)
    beastcoast -> m80 -> oglatam (тег сменялся, параллели нет)

Подставить букмекеру `TALON` при запросе `Aurora vs X` значит рискнуть взять
кэфы ЧУЖОГО матча, если Talon играет на той же странице. Такая ошибка стоит
денег и не видна в логе: кэфы приходят, просто не те.

Критерий разделения — активность тегов во времени. У переименования интервалы
матчей не пересекаются: старый тег заканчивается там, где начинается новый.
У перехода игроков оба тега играют параллельно. На замере 13.08 это дало
40 переименований против 24 переходов, и цепочка Tundra -> Iron Wing -> 1w
(организация 8121295) попала в переименования:

    8291895 tundra    2021-02-02..2026-05-18
    10150413          2026-05-19..2026-05-30
    10182357 1w       2026-07-07..2026-08-05

Результат: `data/team_org_aliases.json` — {ключ имени: [написания]}. Файл
маленький, его читает `team_name_aliases.alias_spellings` и отдаёт букмекерскому
парсеру наравне с ручным справочником. Ручной справочник ПРИОРИТЕТНЕЕ: он
содержит подтверждённые написания со страницы, а этот — только наши имена.

Запуск: venv_catboost/bin/python3 base/tools/build_team_org_aliases.py
"""
from __future__ import annotations

import collections
import json
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
CORPUS = Path(os.getenv(
    "PRO_CORPUS_COMPACT",
    str(ROOT / "runtime/artifacts/misc/pro_corpus_compact.npz"),
))
ARTIFACT = Path(os.getenv(
    "PREMATCH_ARTIFACT",
    str(ROOT / "runtime/artifacts/misc/prematch_model_artifact_v3.npz"),
))
OUT = Path(os.getenv("TEAM_ORG_ALIASES", str(ROOT / "data/team_org_aliases.json")))

# Допуск на доигровку: тег может закрыть начатый турнир уже после ребрендинга.
# Неделя выбрана по длине турнирной недели, а не подобрана под результат.
OVERLAP_GRACE_SECONDS = 7 * 86400


def _norm(value: str) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _name_index() -> dict[int, set[str]]:
    """team_id -> написания из справочника `id_to_names`.

    Справочник берём ПРОДОВЫЙ (`TEAM_NAMES_DIR`), а не локальный: записи вида
    `tier_two_teams['ironwing']` дописывает рантайм на serv1 при первой встрече
    команды, и локальная копия про новые теги не знает. Именно на этом
    споткнулась первая сборка — цепочка Tundra -> Iron Wing -> 1w дала только
    `tundra <-> 1w`, потому что имени `ironwing` в локальном файле нет.
    """
    sys.path.insert(0, os.getenv("TEAM_NAMES_DIR", str(ROOT / "base")))
    import id_to_names as names                      # noqa: PLC0415

    out: dict[int, set[str]] = collections.defaultdict(set)
    for source in (names.tier_one_teams, names.tier_two_teams, names.rest_teams):
        for raw, value in source.items():
            ids = value if isinstance(value, set) else {value}
            # Храним ИСХОДНОЕ написание, а не свёрнутое: букмекерский `_norm`
            # пробелы сохраняет, поэтому `spirit academy` найдётся на странице,
            # а `spiritacademy` — нет. Свёрнутая форма нужна только как ключ.
            text = str(raw or "").strip()
            if not _norm(text):
                continue
            for team_id in ids:
                if isinstance(team_id, int) and team_id > 0:
                    out[team_id].add(text)
    return out


def _activity_spans(path: Path) -> dict[int, list[int]]:
    """team_id -> [первая карта, последняя карта] по корпусу."""
    z = np.load(path)
    ts, teams = z["ts"], z["teams"]
    span: dict[int, list[int]] = {}
    for i in range(len(ts)):
        stamp = int(ts[i])
        for side in (int(teams[i, 0]), int(teams[i, 1])):
            if side <= 0:
                continue
            cur = span.get(side)
            if cur is None:
                span[side] = [stamp, stamp]
            elif stamp < cur[0]:
                cur[0] = stamp
            elif stamp > cur[1]:
                cur[1] = stamp
    return span


def build() -> dict[str, list[str]]:
    merge = np.load(ARTIFACT)["team_merge"]
    org_of = {int(a): int(b) for a, b in merge}
    org_ids: dict[int, set[int]] = collections.defaultdict(set)
    for team_id, org in org_of.items():
        org_ids[org].add(team_id)

    id_names = _name_index()
    span = _activity_spans(CORPUS)

    groups: list[list[str]] = []
    renamed = skipped = 0
    for ids in org_ids.values():
        named = [i for i in ids if id_names.get(i) and i in span]
        if len(named) < 2:
            continue
        named.sort(key=lambda i: span[i][0])
        disjoint = all(
            span[a][1] <= span[b][0] + OVERLAP_GRACE_SECONDS
            for a, b in zip(named, named[1:])
        )
        if not disjoint:
            skipped += 1
            continue
        spellings: list[str] = []
        for team_id in named:
            for name in sorted(id_names[team_id]):
                if name not in spellings:
                    spellings.append(name)
        if len(spellings) < 2:
            continue
        renamed += 1
        groups.append(spellings)

    # Ключ — свёрнутая форма (только буквы и цифры): наши имена приходят как
    # `Iron Wing`, а справочник хранит `ironwing`, и без свёртки они не сходятся.
    table: dict[str, list[str]] = {}
    for spellings in groups:
        for name in spellings:
            key = _norm(name)
            merged = table.setdefault(key, [])
            for other in spellings:
                if _norm(other) != key and other not in merged:
                    merged.append(other)
    print(f"организаций-переименований: {renamed}; отброшено переходов: {skipped}")
    print(f"имён со свидетельством ребрендинга: {len(table)}")
    return table


def main() -> None:
    table = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT.with_suffix(OUT.suffix + ".tmp")
    tmp.write_text(json.dumps(table, ensure_ascii=False, indent=1, sort_keys=True),
                   encoding="utf-8")
    tmp.replace(OUT)                                  # rebuild-then-replace
    print(f"сохранено: {OUT}")
    for name in ("tundra", "1w", "ironwing"):
        if name in table:
            print(f"   {name} -> {table[name]}")


if __name__ == "__main__":
    main()
