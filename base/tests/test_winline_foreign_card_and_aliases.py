"""Winline: кэфы только из своей карточки и только своей команды.

Обе регрессии найдены 31.07-01.08.2026 замером на живых данных, а не рассуждением.

1. ЧУЖИЕ КЭФЫ. Запрос `REKONIX vs L1GA TEAM` получал кэфы карточки `ENJOY GLYPH`
   той же лиги: 11 записей из 419 с кэфами в логе монитора. Цепочка отказов:
   `L1GA TEAM` -> CamelCase-вариант `l1 ga team` -> ни одного токена >=3 символов
   не из родовых -> откат на `>=2` пропускал слово `team` -> поиск попадал в
   `TEAM VOODOOSH`; границы карточки резались только по заголовку дисциплины
   (`DOTA 2 |`), общему для нескольких матчей подряд; а рынок при недоказанной
   карточке читался по всей странице, то есть бралась ПЕРВАЯ строка `N карта a b`.

2. КОМАНДА НЕ НАХОДИТСЯ ВОВСЕ. У нас `BoomBoys` (SourceTV/GC), Winline пишет
   `BB TEAM` — 0 успехов из 115 попыток. Тот же класс: `TEAM TPABOMAH` набрано
   латиницей там, где команда называется `ТРАВОМАН`.

Тексты страниц ниже — дословные выдержки из runtime/winline_parser_monitor.log
(31.07.2026, записи 12:43:00 и 12:45:50), поэтому тест охраняет реальный дефект:
на модуле до правки первый же тест получает [1.70, 2.02] вместо [2.25, 1.57].
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
for path in (str(BASE_DIR), str(REPO_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

import bookmaker_selenium_odds as bk  # noqa: E402
from runtime.winline_current_map_odds_poller import (  # noqa: E402
    _odds_accepted,
    _teams_equivalent,
)

# Лог монитора 31.07.2026 12:43:00: две лиги, четыре матча, наша пара — вторая
# карточка первой лиги. Прод отдал [1.70, 2.02] — строку рынка ENJOY GLYPH.
TWO_LEAGUES_PAGE_TEXT = (
    "DOTA 2 | Games of the Future 2 ENJOY GLYPH 1карта 27' +38 0 0 9 14 1К Матч 1.65 2.10 2.90 "
    "- 1.5 + 1.32 1.80 2.5 1.90 1 карта 1.70 2.02 - - - - - - "
    "L1GA REKONIX 1карта +45 0 0 0 0 1К Матч 2.00 1.72 3.10 - 1.5 + 1.28 1.83 2.5 1.87 "
    "1 карта 1.57 2.25 1.83 - 7.5 + 1.87 1.85 54.5 1.85 "
    "DOTA 2 | BB Streamers Battle 2 TEAM VOODOOSH TEAM TPABOMAH 1карта +16 0 0 0 0 1К Матч 2.55 1.42 "
    "1.50 + 1.5 - 2.40 1.70 2.5 2.02 1 карта 2.30 1.55 - - - - - - "
    "TEAM STRAY TEAM NS 1карта +13 0 0 8 7 1К Матч - - - - - - - -"
)

# Лог монитора 31.07.2026 12:45:50: три карточки, первая — наш `BoomBoys` под
# именем `BB TEAM`, с открытым рынком второй карты.
THREE_CARDS_PAGE_TEXT = (
    "DOTA 2 | 1w Essence 1 BB TEAM MOUZ 2карта 08' +16 1 0 4 3 2К 33 35 1К Матч 1.42 2.65 "
    "- - - - - - - 2 карта 1.44 2.67 1.80 - 8.5 + 1.90 1.90 44.5 1.80 "
    "DOTA 2 | Games of the Future 2 ENJOY GLYPH 1карта 29' +38 0 0 9 14 1К Матч 1.70 2.02 3.00 "
    "- 1.5 + 1.30 1.80 2.5 1.90 1 карта 1.72 2.00 - - - - - - "
    "L1GA REKONIX 1карта +44 0 0 0 0 1К Матч 2.00 1.72 3.10 - 1.5 + 1.28 1.83 2.5 1.87 "
    "1 карта 1.57 2.25 1.83 - 7.5 + 1.87 1.87 54.5 1.83"
)


# Дамп живой страницы 05.08.2026, 20:41 MSK (runtime/artifacts/odds-winline/
# probe_arise_full_card_2026-08-05_2040.log): наша пара идёт live на карте 2, а
# следом в ленте стоят карточки ЛИНИИ с тем же `RE.ARISE` и рынком `1 карта` —
# на дотовской странице линия соседствует с live, и перепутать их нельзя.
ASGARD_LIVE_AND_LINE_PAGE_TEXT = (
    "DOTA 2 | Asgard Championship TEAM SYNTAX RE.ARISE 2карта +16 0 1 3 6 2К "
    "Матч 3.80 1.20 2.40 + 1.5 - 1.50 1.50 2.5 2.40 2 карта 2.40 1.50 - - - - - - "
    "DOTA 2 | European Pro League LEVEL UP RE.ARISE Завтра 12:00 +27 "
    "Матч 1.59 2.22 2.64 - 1.5 + 1.43 1.78 2.5 1.93 1 карта 1.66 2.09 - - - - - - "
    "RE.ARISE NO HOODWINK Завтра 15:00 +9 Матч 2.46 1.48 1.50 + 1.5 - 2.43 1.75"
)


def _odds(text: str, team1: str, team2: str, map_num: int):
    extract = bk._extract_winline_current_map_winner(
        text, team1, team2, forced_map_num=map_num
    )
    return list(extract.odds or []), extract


def test_neighbour_card_odds_are_never_borrowed_from_page_text():
    """Своя карточка — своя строка рынка, даже если соседняя идёт первой."""
    odds, extract = _odds(TWO_LEAGUES_PAGE_TEXT, "REKONIX", "L1GA TEAM", 1)

    assert odds == [2.25, 1.57], f"взяли не свою строку рынка: {extract.details[:200]!r}"
    assert odds != [1.70, 2.02], "это кэфы карточки ENJOY GLYPH"
    assert extract.market_kind == "current_map_winner"
    assert extract.map_num == 1
    # Роль-маркеры обязаны быть на месте: по ним пайплайн понимает, что кэфы
    # уже канонизированы к нашим team1/team2.
    assert (extract.p1_team, extract.p2_team) == ("team1", "team2")


def test_requested_order_mirrors_prices_symmetrically():
    """Смена порядка запроса разворачивает кэфы и ничего больше."""
    direct, _ = _odds(TWO_LEAGUES_PAGE_TEXT, "L1GA TEAM", "REKONIX", 1)
    reverse, _ = _odds(TWO_LEAGUES_PAGE_TEXT, "REKONIX", "L1GA TEAM", 1)

    assert direct == [1.57, 2.25]
    assert reverse == [2.25, 1.57]


def test_neighbour_match_still_gets_its_own_odds():
    """Обратная сторона инварианта: у владельца строки кэфы забирать нельзя."""
    odds, _ = _odds(TWO_LEAGUES_PAGE_TEXT, "enjoy", "GLYPH", 1)

    assert odds == [1.70, 2.02]


def test_bookmaker_spelling_of_our_team_is_found():
    """Главная жалоба: у нас `BoomBoys`, на странице `BB TEAM` — это одна команда."""
    odds, extract = _odds(THREE_CARDS_PAGE_TEXT, "BoomBoys", "MOUZ", 2)

    assert odds == [1.44, 2.67], f"карточка BB TEAM не найдена: {extract.reason!r}"
    assert extract.map_num == 2

    mirrored, _ = _odds(THREE_CARDS_PAGE_TEXT, "MOUZ", "BoomBoys", 2)
    assert mirrored == [2.67, 1.44]


def test_bookmaker_writes_our_team_as_a_different_word():
    """`Team Synapse` у нас — `TEAM SYNTAX` на сайте, и рядом линия с тем же соперником."""
    odds, extract = _odds(ASGARD_LIVE_AND_LINE_PAGE_TEXT, "Team Synapse", "RE ARISE", 2)

    assert odds == [2.40, 1.50], f"карточка TEAM SYNTAX не найдена: {extract.reason!r}"
    assert odds != [1.66, 2.09], "это рынок линии LEVEL UP — RE.ARISE"
    assert extract.map_num == 2

    mirrored, _ = _odds(ASGARD_LIVE_AND_LINE_PAGE_TEXT, "RE ARISE", "Team Synapse", 2)
    assert mirrored == [1.50, 2.40]


def test_without_the_alias_that_card_is_not_found(monkeypatch):
    """Контроль фикстуры: пару держит справочник, а не случайное совпадение слов."""
    monkeypatch.setattr(bk, "_alias_spellings", lambda _name: [])

    odds, extract = _odds(ASGARD_LIVE_AND_LINE_PAGE_TEXT, "Team Synapse", "RE ARISE", 2)

    assert odds == []
    assert extract.reason == "no_card"


def test_three_cards_in_one_page_text_are_split_by_event():
    """Заголовок турнира общий для нескольких матчей, границей события он не является."""
    boundaries = bk._winline_event_boundaries(THREE_CARDS_PAGE_TEXT)
    starts = [pos for pos in boundaries]
    segments = [
        THREE_CARDS_PAGE_TEXT[start:end]
        for start, end in zip(starts, starts[1:] + [len(THREE_CARDS_PAGE_TEXT)])
    ]
    card_segments = [seg for seg in segments if "Матч" in seg]

    assert len(card_segments) == 3, [seg[:40] for seg in segments]
    for segment in card_segments:
        # Одна строка матч-рынка = ровно одна карточка внутри куска.
        assert segment.count("Матч") == 1, segment[:120]


def test_card_not_proven_means_no_odds():
    """Рынок на странице есть, нашей пары нет — кэфы не берём (было: брали первые)."""
    odds, extract = _odds("Live esports 2К 1.52 2.45 Матч 1.30 3.15", "Winter Bear", "DOGSENT", 2)

    assert odds == []
    assert extract.reason == "no_card"
    assert "not proven" in extract.details


@pytest.mark.parametrize("name", ["L1GA TEAM", "BB Team", "Team Liquid", "Xtreme Gaming"])
def test_generic_word_is_never_a_search_form(name: str):
    """Слово `team` не ищет команду: по нему находились чужие карточки."""
    forms = list(bk._fallback_search_tokens(name))

    assert "team" not in forms, forms
    assert all(len(form) >= 3 for form in forms), forms


def test_generic_word_does_not_match_foreign_cards():
    """`L1GA TEAM` не должна находиться в шапках `TEAM VOODOOSH` / `TEAM STRAY`."""
    assert bk._find_positions_with_fallback("team voodoosh team stray team ns", "L1GA TEAM") == []
    # А своё написание с сайта (`L1GA`) — обязана.
    assert bk._find_positions_with_fallback("l1ga rekonix 1карта", "L1GA TEAM") == [0]


def test_digit_letter_name_is_not_split_into_stubs():
    """`L1GA` нельзя резать на `l1` и `ga`: обрубки ищут что попало."""
    forms = list(bk._fallback_search_tokens("L1GA TEAM"))

    assert "l1" not in forms and "ga" not in forms, forms


def test_roster_qualifier_is_never_dropped():
    """`Team Spirit Academy` без `academy` — другая команда, и её кэфы чужие."""
    forms = list(bk._fallback_search_tokens("Team Spirit Academy"))

    assert forms == ["spirit academy"], forms
    assert bk._find_positions_with_fallback("team spirit vici gaming", "Team Spirit Academy") == []


def test_short_name_is_not_matched_inside_longer_word():
    """Порядок сторон рынка не может решаться подстрокой: `1w` внутри `1WIN`."""
    assert bk._first_index_with_fallback("1win essence vici gaming", "1w") == -1
    assert bk._first_index_with_fallback("1w vici gaming 1карта", "1w") == 0


def test_latin_homoglyphs_of_cyrillic_name_are_matched():
    """Winline пишет `TEAM TPABOMAH` латиницей — команда та же."""
    odds, extract = _odds(TWO_LEAGUES_PAGE_TEXT, "Team Voodoosh", "Team Травоман", 1)

    assert odds == [2.30, 1.55], f"гомоглифы не сопоставились: {extract.reason!r}"


def test_yo_survives_normalization():
    """`ё` обязана пережить нормализацию: иначе имя рвётся на два куска."""
    assert bk._norm("Королёв Арена") == "королёв арена"
    assert bk._literal_team_positions("матч королев арена 1карта", "Королёв Арена") == [5]


def test_poller_accepts_bookmaker_spelling_of_our_team():
    """Приёмка обязана принять кэфы `BB TEAM` для нашей `BoomBoys`."""
    assert _teams_equivalent("BB TEAM", "BoomBoys")
    assert _teams_equivalent("BoomBoys", "BB Team")
    assert _teams_equivalent("BetBoom Team", "BoomBoys")

    identity = {
        "series": "dltv.org/matches/8922693443",
        "map_num": 2,
        "team1": "BoomBoys",
        "team2": "Team Liquid",
    }
    result = {
        "market_status": "open",
        "source": "winline_current_map_winner",
        "p1_odds": 1.44,
        "p2_odds": 2.67,
        "map_num": 2,
        "page_valid": True,
        "team1": "BB TEAM",
        "team2": "TEAM LIQUID",
    }
    assert _odds_accepted(result, identity=identity) is True


def test_poller_still_separates_academy_from_main_roster():
    """Справочник написаний не имеет права склеить второй состав с основным."""
    assert not _teams_equivalent("Team Spirit", "Team Spirit Academy")
    assert not _teams_equivalent("Navi", "Navi Junior")
    assert not _teams_equivalent("BoomBoys", "Team Liquid")


class _PageStub:
    """Минимальная страница: разбор идёт по body_text/html, до её методов не доходит."""

    def __init__(self, html: str, body_text: str, url: str) -> None:
        self._html = html
        self._body_text = body_text
        self.url = url

    def content(self) -> str:
        return self._html


def _parse_listing_page(monkeypatch, text: str, team1: str, team2: str, map_num: int):
    """Сквозной разбор страницы-списка (не deeplink) — путь живого пайплайна."""
    html = f"<html><body>{text}</body></html>"
    page = _PageStub(html, text, "https://winline.ru/stavki/sport/kibersport/live")
    monkeypatch.setattr(bk.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(bk, "_is_deeplink", lambda *_a, **_k: False)
    monkeypatch.setattr(
        bk,
        "_load_site_render_payload_camoufox",
        lambda *_a, **_k: ("ok", "", html, text, text),
    )
    return bk.parse_site_in_camoufox_page(
        page,
        "winline",
        page.url,
        team1,
        team2,
        mode="live",
        forced_map_num=map_num,
    )


def test_listing_page_result_carries_map_and_role_markers(monkeypatch):
    """Кэфы с листинга обязаны нести номер карты и роль-маркеры.

    Раньше при недоказанной карточке результат собирался общим добором: кэфы были,
    а `map_num` и `p1_team` приходили пустыми — приёмке нечем было проверить,
    чьи это кэфы вообще.
    """
    result = _parse_listing_page(monkeypatch, THREE_CARDS_PAGE_TEXT, "BoomBoys", "MOUZ", 2)

    assert list(result.odds or []) == [1.44, 2.67]
    assert result.map_num == 2
    assert (result.p1_team, result.p2_team) == ("team1", "team2")
    assert result.market_kind == "current_map_winner"


def test_listing_page_never_serves_neighbour_card(monkeypatch):
    """Тот же сквозной путь на паре, которой доставались чужие кэфы."""
    result = _parse_listing_page(monkeypatch, TWO_LEAGUES_PAGE_TEXT, "REKONIX", "L1GA TEAM", 1)

    assert list(result.odds or []) == [2.25, 1.57]
    assert result.map_num == 1


def test_listing_page_without_our_card_returns_no_odds(monkeypatch):
    """Нашей пары на странице нет — кэфы пустые, чужая строка рынка не годится."""
    result = _parse_listing_page(monkeypatch, TWO_LEAGUES_PAGE_TEXT, "Team Falcons", "Nigma Galaxy", 1)

    assert list(result.odds or []) == []
    assert result.market_closed is False
