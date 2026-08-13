"""Единый справочник написаний названий команд для сверки с букмекерами.

Наши названия приходят из SourceTV/GC (OpenDota-нейминг), а букмекер рендерит
своё написание той же команды. Проверено на живой странице Winline 31.07.2026:
у нас `BoomBoys` — на странице `BB TEAM`; у нас `L1GA TEAM` — на странице `L1GA`
(лог монитора: `L1GA REKONIX`); у нас `Level UP esports` — на странице `LEVEL UP`
(лог монитора: `LEVEL UP NO HOODWINK`). Пока написание не совпадает, карточка
матча не находится вовсе, и кэфы не парсятся ни разу — так `BoomBoys` дал 0
успехов из 115 попыток.

Справочник пополняется руками: канон -> все встречавшиеся написания. Добавлять
можно только подтверждённые написания (видели на странице/в логе), иначе легко
склеить разные команды: `Team Spirit` и `Team Spirit Academy` — разные ростеры.

Отдельная беда — смешение алфавитов: Winline пишет `TEAM TPABOMAH` латиницей
там, где команда называется `ТРАВОМАН`. Для СОПОСТАВЛЕНИЯ (не для отображения)
кириллические буквы-двойники приводим к латинице; отображаем всегда исходное имя.
"""
from __future__ import annotations

import os
import re
from typing import Dict, List, Optional, Tuple

__all__ = [
    "TEAM_NAME_ALIASES",
    "alias_spellings",
    "canonical_team_key",
    "compact_key",
    "fold_confusables",
    "match_key",
]


# Кириллические буквы, неотличимые по начертанию от латинских. Отображение 1:1
# по символам: длина строки не меняется, поэтому позиции найденных вхождений
# остаются валидными для исходного текста. `ё` сводим к `е` — на страницах
# встречаются оба написания одного имени.
_CONFUSABLE_MAP = {
    "а": "a",
    "в": "b",
    "е": "e",
    "ё": "e",
    "к": "k",
    "м": "m",
    "н": "h",
    "о": "o",
    "р": "p",
    "с": "c",
    "т": "t",
    "у": "y",
    "х": "x",
}
_CONFUSABLE_TABLE = {ord(key): value for key, value in _CONFUSABLE_MAP.items()}


def fold_confusables(value: str) -> str:
    """Привести строку к виду, устойчивому к смешению кириллицы и латиницы."""
    return str(value or "").lower().translate(_CONFUSABLE_TABLE)


def match_key(value: str) -> str:
    """Ключ сопоставления: нижний регистр, без пунктуации, буквы-двойники сведены."""
    folded = fold_confusables(value)
    return re.sub(r"\s+", " ", re.sub(r"[^0-9a-zа-я]+", " ", folded)).strip()


def compact_key(value: str) -> str:
    """`match_key` без пробелов: `Iron Wing` и `ironwing` дают один ключ.

    Справочник `id_to_names` хранит имена свёрнутыми (`teamspiritacademy`), а из
    live-потока они приходят с пробелами. Для ПОИСКА по таблице переименований
    это одно и то же имя; для ручного справочника свёртка не применяется — там
    написания подтверждённые и сверяются как есть.
    """
    return match_key(value).replace(" ", "")


# Канон -> написания той же команды. Канон выбираем официальным именем
# организации; все написания равноправны при поиске.
TEAM_NAME_ALIASES: Dict[str, Tuple[str, ...]] = {
    # SourceTV/GC отдаёт `BoomBoys`, Winline рендерит `BB TEAM`
    # (дамп runtime/winline_name_probe_20260731_202843_live.txt: `BB TEAM TEAM LIQUID`).
    # Голый тег `BB` в справочник намеренно НЕ включён: он совпадает с турнирным
    # блоком `BB Streamers Battle` на той же странице.
    "BetBoom Team": ("BoomBoys", "BB Team", "BetBoom"),
    # Лог монитора 31.07.2026: карточка `L1GA REKONIX` — слова TEAM на сайте нет.
    "L1GA TEAM": ("L1GA",),
    # Лог монитора 31.07.2026: карточка `LEVEL UP NO HOODWINK`.
    "Level UP esports": ("Level UP", "Levelup"),
    # SourceTV отдаёт `Team Synapse`, Winline рендерит `TEAM SYNTAX` — это разные
    # СЛОВА, а не сокращение, поэтому пара не находилась вообще: дамп живой
    # страницы 05.08.2026 (`DOTA 2 | Asgard Championship TEAM SYNTAX RE.ARISE
    # 2карта ... 2К`) показал SYNAPSE = 0 вхождений в тексте и в html при
    # ARISE = 1. Матч шёл, кэфы не приходили всю карту. Канон — официальное имя
    # (Asgard Championship S1, 05.08: `Syntax vs RE Arise`).
    "Team Syntax": ("Team Synapse",),
}


def _build_groups() -> Dict[str, Tuple[str, ...]]:
    groups: Dict[str, Tuple[str, ...]] = {}
    for canonical, aliases in TEAM_NAME_ALIASES.items():
        spellings = (canonical,) + tuple(aliases)
        for spelling in spellings:
            key = match_key(spelling)
            if key:
                groups[key] = spellings
    return groups


_ALIAS_GROUPS = _build_groups()
# Таблица переименований читается лениво и один раз: её собирают ночью, а
# импортируется модуль в том числе из букмекерского подпроцесса.
_ORG_TABLE: Optional[Dict[str, Tuple[str, ...]]] = None


def _org_table() -> Dict[str, Tuple[str, ...]]:
    """Написания из цепочек ПЕРЕИМЕНОВАНИЙ (`data/team_org_aliases.json`).

    Файл собирает `base/tools/build_team_org_aliases.py` по активности тегов во
    времени: в него попадают только организации, у которых интервалы матчей
    старого и нового тега НЕ пересекаются. Переходы игроков (Talon -> Aurora,
    G2.iG -> Invictus) отброшены намеренно: обе команды продолжают играть, и
    подстановка чужого имени в поиск карточки может принести кэфы другого матча.

    Файла может не быть (не собран, не доставлен) — тогда работает только
    ручной справочник, как раньше.
    """
    global _ORG_TABLE
    if _ORG_TABLE is None:
        table: Dict[str, Tuple[str, ...]] = {}
        try:
            import json
            from pathlib import Path

            path = os.getenv(
                "TEAM_ORG_ALIASES",
                str(Path(__file__).resolve().parent.parent / "data" / "team_org_aliases.json"),
            )
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
            for key, spellings in raw.items():
                normalized = compact_key(key)
                if normalized and isinstance(spellings, list):
                    table[normalized] = tuple(str(s) for s in spellings if s)
        except Exception:                              # noqa: BLE001
            table = {}
        _ORG_TABLE = table
    return _ORG_TABLE


def alias_spellings(name: str) -> List[str]:
    """Другие известные написания той же команды (без самого `name`).

    Порядок важен: сперва РУЧНОЙ справочник — там написания, подтверждённые на
    странице букмекера (`BoomBoys` -> `BB TEAM`), и они точнее наших внутренних
    имён. Следом — цепочки переименований из корпуса: они дают старый тег той же
    организации (`Iron Wing` -> `Tundra`, `1w`), когда букмекер ещё не обновил
    название.
    """
    key = match_key(name)
    if not key:
        return []
    out: List[str] = []
    seen = {key}
    group = _ALIAS_GROUPS.get(key)
    if group:
        for spelling in group:
            spelling_key = match_key(spelling)
            if spelling_key and spelling_key not in seen:
                seen.add(spelling_key)
                out.append(spelling)
    for spelling in _org_table().get(compact_key(name), ()):
        spelling_key = match_key(spelling)
        if spelling_key and spelling_key not in seen:
            seen.add(spelling_key)
            out.append(spelling)
    return out


def canonical_team_key(name: str) -> str:
    """Ключ команды с учётом справочника: у всех написаний он одинаковый."""
    key = match_key(name)
    if not key:
        return ""
    group = _ALIAS_GROUPS.get(key)
    if not group:
        return key
    return match_key(group[0])
