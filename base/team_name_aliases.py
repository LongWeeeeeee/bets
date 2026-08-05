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

import re
from typing import Dict, List, Tuple

__all__ = [
    "TEAM_NAME_ALIASES",
    "alias_spellings",
    "canonical_team_key",
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


def alias_spellings(name: str) -> List[str]:
    """Другие известные написания той же команды (без самого `name`)."""
    key = match_key(name)
    if not key:
        return []
    group = _ALIAS_GROUPS.get(key)
    if not group:
        return []
    return [spelling for spelling in group if match_key(spelling) != key]


def canonical_team_key(name: str) -> str:
    """Ключ команды с учётом справочника: у всех написаний он одинаковый."""
    key = match_key(name)
    if not key:
        return ""
    group = _ALIAS_GROUPS.get(key)
    if not group:
        return key
    return match_key(group[0])
