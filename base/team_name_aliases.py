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
    "GENERIC_TEAM_TOKENS",
    "names_match_loosely",
    "search_forms",
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
    # 26.08.2026, квал BLAST Slam: у нас `RE.Arise`, на живой странице Winline та
    # же пара подписана `YELLOW SUBMARINE 4IKIBAMBONI` — прошлое имя того же
    # состава (подсказал alex). Замер по странице в тот момент: `arise` — 0
    # вхождений и в тексте, и в html, `4ikibamboni` — 1. Карточка не находилась,
    # кэфы по карте не шли ни разу (`promotion=no_card_scope`, `match_found=false`).
    # Строка редкая, склеить ею чужую команду нельзя.
    "RE.Arise": ("4IKIBAMBONI",),
    # Подтверждено логами Winline/SourceTV 14.09.2026: SourceTV пишет
    # `Inner Circle x Insanity`, а карточка Winline — `INNER CIRCLE`.
    "Inner Circle x Insanity": ("Inner Circle",),
    # 05.10.2026, квал PR Universe: SourceTV/GC отдаёт `Blasterbl` (team_id
    # 10291736), карточка Winline — `LEGION BLASTERBI`: строчная `l` прочитана
    # как заглавная `I`. Прод не нашёл карточку ни разу за две карты
    # (`match_found=false`, `t1_text=0 t2_text=1`); снимок страницы —
    # base/tests/fixtures/winline_overview_legion_blasterbi_20261005.json.
    "Blasterbl": ("BLASTERBI",),
    # 08.10.2026, BLAST Slam: SourceTV/GC отдаёт `1win` (team_id 9467224, ключ
    # моста `...|id:9255039|id:9467224|map2|Aurora Gaming|1win`), карточка Winline
    # — `TEAM AURORA 1W`. Прод не нашёл цену текущей карты ни разу за 17:01-18:35
    # (в листинге при этом «Победитель 2 карта 1.61 2.22»); снимок страницы —
    # base/tests/fixtures/winline_overview_snapshot_20261008_blast_duel_cards.json.
    "1win": ("1W",),
    # 08.10.2026, EPL World Series: SourceTV/GC отдаёт `ЯЧЁ123` (кириллица; у Я и Ч
    # нет латинских двойников, свёртка даёт `яче123`), карточка Winline —
    # транслит `YACHE123`. 04.10 (Cloud Dawning — ЯЧЁ123) собственные карточные
    # опросы sweep брали цену `YACHE123|CLOUD DAWNING` 40 раз, мост `ЯЧЁ123` — 0 из
    # 65; с 26.09 без цены ~20 карт EPL. Строки истории —
    # base/tests/fixtures/winline_yache123_card_keys_20261004.json.
    "ЯЧЁ123": ("YACHE123",),
    # 10.10.2026, ревью гейта присутствия: частые расхождения тега и полного имени
    # у команд тир-1 (Winline любит короткий тег: `NAVI`, `PSG.LGD`; кириллицей
    # пишут `Тим Спирит`). Ложное несовпадение держит ставку, поэтому тег и имя
    # сведены явно. `Team Spirit` и `TEAM SPIRIT ACADEMY` остаются РАЗНЫМИ.
    "Natus Vincere": ("NAVI",),
    "LGD Gaming": ("PSG.LGD",),
    "Team Spirit": ("Тим Спирит",),
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


# Слова-обёртки, которые букмекер добавляет или отбрасывает произвольно:
# `Aurora Gaming` (наше) и `TEAM AURORA` (Winline) - одна команда.
GENERIC_TEAM_TOKENS = frozenset(
    {"team", "gaming", "esports", "esport", "club", "gg", "the"})
LOOSE_RATIO_MIN = 0.85
LOOSE_RATIO_MIN_LEN = 4


def _loose_forms(name: str) -> List[str]:
    """Формы имени для ослабленного сравнения: полная и без слов-обёрток."""
    full = match_key(name)
    if not full:
        return []
    forms = [full]
    core = " ".join(t for t in full.split() if t not in GENERIC_TEAM_TOKENS)
    if core and core != full:
        forms.append(core)
    return forms


def names_match_loosely(ours: str, theirs: str) -> bool:
    """Одна ли это команда по двум написаниям (чистая функция).

    Для гейта присутствия матча в листинге Winline: ложное СОВПАДЕНИЕ там
    безвредно (ставка уходит, как до гейта), ложное НЕСОВПАДЕНИЕ держит ставку,
    поэтому правило щедрое. Проверки по порядку:
    1) равны после `match_key` (регистр, пунктуация, кириллица-двойники);
    2) общая группа справочника написаний (`alias_spellings`: ручной + переименования);
    3) равны после отбрасывания слов-обёрток (`GENERIC_TEAM_TOKENS`) или без
       пробелов (`Iron Wing` / `ironwing`);
    4) `difflib` >= 0.85 на формах длиной >= 4 (`Blasterbl` / `BLASTERBI`).
    """
    left, right = _loose_forms(ours), _loose_forms(theirs)
    if not left or not right:
        return False
    if left[0] == right[0]:
        return True
    group_left = {left[0]} | {match_key(s) for s in alias_spellings(ours)}
    group_right = {right[0]} | {match_key(s) for s in alias_spellings(theirs)}
    group_left.discard("")
    group_right.discard("")
    if group_left & group_right:
        return True
    forms_left = set(left) | group_left
    forms_right = set(right) | group_right
    if forms_left & forms_right:
        return True
    if {f.replace(" ", "") for f in forms_left} & {f.replace(" ", "") for f in forms_right}:
        return True
    import difflib

    for a in forms_left:
        if len(a) < LOOSE_RATIO_MIN_LEN:
            continue
        for b in forms_right:
            if len(b) >= LOOSE_RATIO_MIN_LEN and difflib.SequenceMatcher(
                    None, a, b).ratio() >= LOOSE_RATIO_MIN:
                return True
    return False


def search_forms(name: str, min_len: int = 2) -> List[str]:
    """Формы имени для ПОИСКА в тексте страницы (в виде `match_key`).

    Полное имя, имя без слов-обёрток, написания из справочника и слитная форма
    (`ironwing`). Формы короче `min_len` и состоящие только из слов-обёрток
    (`team`, `gaming`) не возвращаются: одно такое слово не доказывает команду.
    Порядок стабильный, без повторов.
    """
    out: List[str] = []
    seen = set()

    def _add(form: str) -> None:
        if len(form) >= min_len and form not in seen:
            seen.add(form)
            out.append(form)

    for spelling in [name] + list(alias_spellings(name)):
        for form in _loose_forms(spelling):
            if all(token in GENERIC_TEAM_TOKENS for token in form.split()):
                continue
            _add(form)
            if " " in form:
                _add(form.replace(" ", ""))
    return out
