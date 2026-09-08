"""Последняя карта серии: нет доступных кэфов карты — берём победителя матча.

Наблюдение с живой страницы 02.08.2026 (снимок `winline_lastmap_20260802_214358`):
у решающей карты Winline иногда не выставляет рынок карты вовсе. Карточка
`REKONIX YAKULT BROTHERS 3карта 28' +7 1 1 ... Матч 3.30 1.25 - - - - - -` —
единственная подпись рынка в ней `Матч`, и обе цены набраны классом
двухисходного рынка `coefficient-button_generic2`. На последней карте победитель
карты и победитель матча — одно событие, поэтому такие кэфы годятся.

Обратный случай из того же снимка: `VICI GAMING OG 2карта 35' ... Матч 5.81 1.10 -
... 2 карта 5.87 1.11` — это Bo2 (`series_type=3`), где «Матч» ТРЁХисходный
(`coefficient-button_generic3`, возможна ничья), а рынок карты свой есть.
Подставлять трёхисходный рынок вместо победителя карты нельзя.

Предохранители промоции (каждый закрыт тестом ниже): карта обязана быть последней
в серии, доступных кэфов запрошенной карты не должно быть ни в одной карточке пары,
рынок «Матч» обязан быть двухисходным и принимать ставку, карточка обязана сама
сообщать, что идёт именно эта карта, а порядок сторон берётся из текста карточки.
"""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import bookmaker_selenium_odds as bk  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
LAST_MAP_MATCH_ONLY = (FIXTURES / "winline_last_map_match_only_20260802.html").read_text(
    encoding="utf-8"
)
BO2_THREE_WAY_MATCH = (FIXTURES / "winline_bo2_match_three_way_20260802.html").read_text(
    encoding="utf-8"
)


def _extract(html: str, team1: str, team2: str, map_num: int, last_map: bool):
    return bk._extract_winline_current_map_winner(
        "",
        team1,
        team2,
        forced_map_num=map_num,
        html=html,
        series_last_map=last_map,
    )


def test_last_map_without_map_market_uses_match_winner():
    """Рынка карты 3 в карточке нет — берём `Матч 3.30 1.25`, канонизируя к team1/team2."""
    extract = _extract(LAST_MAP_MATCH_ONLY, "Yakult Brothers", "REKONIX", 3, True)

    # В карточке первым идёт REKONIX (3.30), значит для team1=Yakult порядок обратный.
    assert list(extract.odds or []) == [1.25, 3.30]
    assert extract.promoted_from_match is True
    assert extract.market_kind == "current_map_winner"
    assert extract.map_num == 3
    assert (extract.p1_team, extract.p2_team) == ("team1", "team2")
    assert "match winner promoted" in extract.details


def test_promotion_reports_raw_card_order_and_prices():
    """Провенанс стороны: имена в порядке карточки и НЕразвёрнутая пара цен.

    `odds` приведены к порядку запроса, поэтому по ним одним нельзя доказать,
    что сторона не уехала. С этими двумя полями доказательство помещается в один
    снимок evidence — сверка с параллельным парсером больше не нужна.
    """
    extract = _extract(LAST_MAP_MATCH_ONLY, "Yakult Brothers", "REKONIX", 3, True)

    assert extract.card_team_order == "REKONIX|Yakult Brothers"
    assert [round(x, 2) for x in (extract.card_odds or [])] == [3.30, 1.25]
    # А запрошенный порядок — обратный, и цены в нём развёрнуты.
    assert [round(x, 2) for x in (extract.odds or [])] == [1.25, 3.30]


def _decider_with_unavailable_map(map_state="blank", match_locked=False):
    """Mutate captured 2026-08-02 cards, not an invented DOM layout.

    Reproduce the 2026-09-08 report: Match is open while the deciding-map
    row is still present without usable quotes. The incident's earlier DOM
    was not saved; this is a controlled mutation of the two real captures.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(LAST_MAP_MATCH_ONLY, "html.parser")
    other = BeautifulSoup(BO2_THREE_WAY_MATCH, "html.parser")
    row = copy.deepcopy(other.select_one(".period-name").parent)
    row.select_one(".period-name").string = "3 карта"
    for button in row.select(".coefficient-button_generic2"):
        if map_state == "blank":
            button["class"].append("coefficient-button--is-blank")
            button.select_one("span").string = "-"
        elif map_state == "locked":
            button["class"].append("coefficient-button_locked")
    if match_locked:
        for button in soup.select(".coefficient-button_generic2"):
            button["class"].append("coefficient-button_locked")
    soup.select_one(".coeffs-wrapper").append(row)
    return str(soup)


@pytest.mark.parametrize("map_state", ["blank", "locked"])
def test_unavailable_deciding_map_uses_open_match(map_state):
    extract = _extract(_decider_with_unavailable_map(map_state),
                       "Yakult Brothers", "REKONIX", 3, True)
    assert extract.odds == [1.25, 3.30]
    assert extract.promoted_from_match is True
    assert extract.market_closed is False


@pytest.mark.parametrize("last_map,match_locked", [(False, False), (True, True)])
def test_unavailable_map_does_not_bypass_match_safety(last_map, match_locked):
    extract = _extract(_decider_with_unavailable_map(match_locked=match_locked),
                       "Yakult Brothers", "REKONIX", 3, last_map)
    assert not extract.odds
    assert extract.promoted_from_match is False


def test_available_deciding_map_keeps_priority_over_match():
    extract = _extract(_decider_with_unavailable_map("open"),
                       "Yakult Brothers", "REKONIX", 3, True)
    assert extract.odds == [1.11, 5.87]
    assert extract.promoted_from_match is False


def test_unavailable_deciding_map_fast_collector_accepts_match(monkeypatch):
    import cyberscore_try as cs

    monkeypatch.setattr(cs, "_winline_registry_series_last_map", lambda _: True)
    url = "https://winline.ru/stavki/sport/kibersport/dota_2"
    result = cs._winline_fast_collect_from_payload(
        {"html": _decider_with_unavailable_map(), "url": url},
        series="test-decider", map_num=3, team1="Yakult Brothers",
        team2="REKONIX", expected_url=url,
    )
    assert result is not None
    assert result["market_status"] == "open"
    assert [result["p1_odds"], result["p2_odds"]] == [1.25, 3.30]
    assert result["odds_promoted_from_match"] is True
    assert result["odds_bettable"] is True


@pytest.mark.parametrize("map_state", ["blank", "locked"])
def test_unavailable_deciding_map_full_collector_accepts_match(monkeypatch, map_state):
    from bs4 import BeautifulSoup
    from test_winline_dynamic_dom_collector import _CountingPage

    html = _decider_with_unavailable_map(map_state)
    url = "https://winline.ru/stavki/sport/kibersport/dota_2"
    page = _CountingPage(html=html, body_text=" ".join(
        BeautifulSoup(html, "html.parser").stripped_strings), url=url)
    monkeypatch.setattr(bk.time, "sleep", lambda *_: None)
    result = bk.parse_site_in_camoufox_page(
        page, "winline", url, "Yakult Brothers", "REKONIX", mode="odds",
        forced_map_num=3, series_last_map=True, acquisition_mode="dynamic_dom",
    )
    assert result.odds == [1.25, 3.30]
    assert "match winner promoted" in result.details


def test_nondeciding_map_full_collector_preserves_text_market(monkeypatch):
    from test_winline_dynamic_dom_collector import _CountingPage

    body = "TeamA TeamB 1 карта 1.55 2.40"
    url = "https://winline.ru/stavki/sport/kibersport/dota_2"
    page = _CountingPage(html=f"<html><body>{body}</body></html>",
                         body_text=body, url=url)
    monkeypatch.setattr(bk.time, "sleep", lambda *_: None)
    # Exercise the final Winline-specific fallback when generic feed parsing
    # cannot classify the row.
    monkeypatch.setattr(bk, "_extract_map_odds_from_feed_context", lambda *_a, **_kw: [])
    result = bk.parse_site_in_camoufox_page(
        page, "winline", url, "TeamA", "TeamB", mode="odds",
        forced_map_num=1, series_last_map=False, acquisition_mode="dynamic_dom",
    )
    assert result.odds == [1.55, 2.40]


def test_card_order_is_independent_of_requested_order():
    """Сырая пара всегда в порядке карточки, как бы ни был задан запрос."""
    direct = _extract(BO2_THREE_WAY_MATCH, "Vici Gaming", "OG", 2, False)
    reverse = _extract(BO2_THREE_WAY_MATCH, "OG", "Vici Gaming", 2, False)

    assert [round(x, 2) for x in (direct.odds or [])] == [5.87, 1.11]
    assert [round(x, 2) for x in (reverse.odds or [])] == [1.11, 5.87]
    for extract in (direct, reverse):
        assert extract.card_team_order == "Vici Gaming|OG"
        assert [round(x, 2) for x in (extract.card_odds or [])] == [5.87, 1.11]


def test_promotion_is_symmetric_in_requested_order():
    """Порядок запроса разворачивает цены и ничего больше."""
    direct = _extract(LAST_MAP_MATCH_ONLY, "REKONIX", "Yakult Brothers", 3, True)

    assert list(direct.odds or []) == [3.30, 1.25]
    assert direct.promoted_from_match is True


def test_no_promotion_when_map_is_not_the_last_one():
    """Не последняя карта — матчевые кэфы в поток карты не попадают никогда."""
    extract = _extract(LAST_MAP_MATCH_ONLY, "Yakult Brothers", "REKONIX", 3, False)

    assert list(extract.odds or []) == []
    assert extract.promoted_from_match is False


def test_no_promotion_for_another_map_than_the_live_one():
    """Карточка пишет `3карта`; на запрос карты 2 подставлять её рынок нельзя."""
    extract = _extract(LAST_MAP_MATCH_ONLY, "Yakult Brothers", "REKONIX", 2, True)

    assert list(extract.odds or []) == []
    assert extract.promoted_from_match is False


def test_three_way_match_market_is_never_promoted():
    """Bo2: «Матч» с ничьей (`_generic3`) — не рынок победителя карты."""
    prices = bk._winline_match_market_winner_prices(
        __import__("bs4").BeautifulSoup(BO2_THREE_WAY_MATCH, "html.parser")
    )

    assert prices is None


def test_own_map_market_still_wins_when_it_exists():
    """Свой рынок карты есть — промоция не вмешивается, даже если карта последняя."""
    extract = _extract(BO2_THREE_WAY_MATCH, "Vici Gaming", "OG", 2, True)

    assert list(extract.odds or []) == [5.87, 1.11]
    assert extract.promoted_from_match is False


def test_absent_map_market_on_non_last_card_stays_empty():
    """Карты 3 у Bo2-карточки нет, и промоция её не выдумывает."""
    extract = _extract(BO2_THREE_WAY_MATCH, "Vici Gaming", "OG", 3, True)

    assert list(extract.odds or []) == []
    assert extract.promoted_from_match is False


def test_card_header_marker_is_not_a_market_label():
    """Шапка карточки (`3карта 28'`) подписью рынка не является."""
    assert not bk._winline_map_row_present("REKONIX YAKULT BROTHERS 3карта 28' +7 1 1 3К", 3)
    assert bk._winline_map_row_present("Матч 1.50 2.40 3 карта 1.87 1.83", 3)
    # Счётчик карт (`3К 39 30`) — тоже не подпись рынка.
    assert not bk._winline_map_row_present("2К 39 30 3К 34 29 1К", 3)


def test_text_only_path_never_promotes():
    """Без DOM двухисходность рынка недоказуема, поэтому промоции нет."""
    card_text = "REKONIX YAKULT BROTHERS 3карта 28' +7 1 1 19 23 3К Матч 3.30 1.25 - - - - - -"
    extract = bk._extract_winline_current_map_winner(
        card_text, "Yakult Brothers", "REKONIX", 3, series_last_map=True
    )

    assert list(extract.odds or []) == []
    assert extract.promoted_from_match is False


# ---------------------------------------------------------------------------
# Определение «последняя карта серии» в пайплайне
# ---------------------------------------------------------------------------


@pytest.fixture()
def cyberscore(monkeypatch, tmp_path):
    import cyberscore_try as cs

    rows = [
        {
            "radiant_team_name": "Yakult Brothers",
            "dire_team_name": "REKONIX",
            "series_type": 1,  # Bo3
            "radiant_series_wins": 1,
            "dire_series_wins": 1,
            "series_game_number": 3,
        },
        {
            "radiant_team_name": "OG",
            "dire_team_name": "Vici Gaming",
            "series_type": 3,  # Bo2
            "radiant_series_wins": 0,
            "dire_series_wins": 1,
            "series_game_number": 2,
        },
        {
            "radiant_team_name": "No Format",
            "dire_team_name": "Unknown Type",
            "series_type": None,
            "series_game_number": 1,
        },
    ]
    path = tmp_path / "sourcetv_matches.json"
    path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(cs, "SOURCETV_MATCHES_PATH", str(path))
    monkeypatch.setattr(cs, "_winline_series_rows_cache", {"mtime": None, "rows": []})
    return cs


def test_bo3_third_map_is_last(cyberscore):
    assert cyberscore._winline_series_last_map(3, "Yakult Brothers", "REKONIX") is True
    assert cyberscore._winline_series_last_map(3, "REKONIX", "Yakult Brothers") is True
    assert cyberscore._winline_series_last_map(2, "Yakult Brothers", "REKONIX") is False
    assert cyberscore._winline_series_last_map(1, "Yakult Brothers", "REKONIX") is False


def test_bo2_second_map_is_last(cyberscore):
    assert cyberscore._winline_series_last_map(2, "OG", "Vici Gaming") is True
    assert cyberscore._winline_series_last_map(1, "OG", "Vici Gaming") is False


def test_unknown_format_and_unknown_match_are_not_last(cyberscore):
    """Формат неизвестен или матча в срезе нет — промоцию не разрешаем."""
    assert cyberscore._winline_series_last_map(1, "No Format", "Unknown Type") is False
    assert cyberscore._winline_series_last_map(1, "Team A", "Team B") is False
    assert cyberscore._winline_series_last_map(3, "Yakult Brothers", "Team B") is False


# ── диагноз отказа ──────────────────────────────────────────────────────────
#
# 22.08.2026 девять карт-троек остались вовсе без цены. В evidence у них
# `market_status=missing` и пустой `details`, а `series_last_map` скакал между
# попытками — понять, какой из предохранителей сработал, было нечем. Теперь
# причина отказа пишется в тот же `miss_fingerprint`, которым уже объясняется
# ненайденная пара команд.

def test_refusal_reason_is_recorded_when_the_map_is_not_the_decider():
    """Карта не решающая — подстановка запрещена по замыслу, и это видно."""
    extract = _extract(BO2_THREE_WAY_MATCH, "VICI GAMING", "OG", 1, False)
    assert extract.odds == []
    assert extract.miss_fingerprint == "promotion=not_decider"


def test_refusal_reason_names_the_three_way_match_market():
    """Bo2: «Матч» трёхисходный, подставлять его вместо победителя карты нельзя."""
    extract = _extract(BO2_THREE_WAY_MATCH, "VICI GAMING", "OG", 3, True)
    assert extract.odds == []
    assert "promotion=" in extract.miss_fingerprint
    assert extract.miss_fingerprint != "promotion=not_decider"


def test_successful_promotion_leaves_no_refusal_note():
    """Когда подстановка сработала, объяснять нечего."""
    extract = _extract(LAST_MAP_MATCH_ONLY, "REKONIX", "YAKULT BROTHERS", 3, True)
    assert extract.odds
    assert not getattr(extract, "miss_fingerprint", "")


# ---------------------------------------------------------------------------
# 07.09.2026 Team Synapse — MOUZ, карта 3: Winline убрал рынок карты и оставил
# только «Матч» с прочерками (рынок снят, ставок нет). Фикстура — вырезка живого
# DOM: pinned-бар с шапкой `3карта` + раскрытая панель `Популярные на матч /
# Победитель - -`. Захват: serv1, собственный браузер (не shared-страница),
# runtime/experiments/odds-winline/winline_decider_snapshot.py,
# страница https://winline.ru/stavki/sport/kibersport/dota_2.
# ---------------------------------------------------------------------------

DECIDER_SUSPENDED = (FIXTURES / "winline_decider_match_suspended_20260907.html").read_text(
    encoding="utf-8"
)


def test_suspended_match_on_decider_is_closed_not_missing():
    """Рынка карты нет, «Матч» снят (прочерки): вердикт closed, а не missing.

    Иначе снятый букмекером рынок неотличим в истории от слепоты парсера —
    именно так карта 3 ушла в чат одним сообщением без объяснений.
    """
    extract = _extract(DECIDER_SUSPENDED, "Team Synapse", "MOUZ", 3, True)

    assert list(extract.odds or []) == []
    assert extract.market_closed is True
    assert extract.reason == "closed"
    assert extract.promoted_from_match is False
    assert "suspended" in (extract.details or "").lower()
    assert extract.miss_fingerprint == "promotion=match_suspended"


def test_suspended_match_on_non_decider_stays_missing():
    """Снятый «Матч» на НЕрешающей карте в поток карты не попадает никак."""
    extract = _extract(DECIDER_SUSPENDED, "Team Synapse", "MOUZ", 3, False)

    assert list(extract.odds or []) == []
    assert extract.market_closed is False
    assert extract.promoted_from_match is False


def test_suspended_match_for_wrong_map_stays_missing():
    """Шапка пишет `3карта`; на запрос карты 2 снятый «Матч» не подставляем."""
    extract = _extract(DECIDER_SUSPENDED, "Team Synapse", "MOUZ", 2, True)

    assert list(extract.odds or []) == []
    assert extract.market_closed is False
    assert extract.promoted_from_match is False


def test_scan_reports_three_way_on_bo2_match():
    """Скан реальной Bo2-карточки: трёхисходный «Матч» виден, цен нет."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(BO2_THREE_WAY_MATCH, "html.parser")
    prices, three_way, suspended = bk._winline_match_market_scan(soup)

    assert prices is None
    assert three_way is True
    assert suspended is False


THREE_WAY_ONLY = """
<html><body><div class="card">
<span class="header-left__time">2карта 12'</span>
<div>TEAM A TEAM B</div>
<div class="match-row-label">Матч</div>
<div class="card__coeffs">
<span class="coefficient-button coefficient-button_generic3">5.81</span>
<span class="coefficient-button coefficient-button_generic3">1.10</span>
<span class="coefficient-button coefficient-button_generic3">-</span>
</div></div></body></html>
"""


def test_three_way_match_veto_is_recorded_not_silent():
    """Bo2: трёхисходный «Матч» ветирует промоцию именованной причиной."""
    extract = _extract(THREE_WAY_ONLY, "TEAM A", "TEAM B", 2, True)

    assert list(extract.odds or []) == []
    assert extract.promoted_from_match is False
    assert "match_market_three_way" in (extract.miss_fingerprint or "")


THREE_WAY_THEN_VALID = """
<html><body><div class="card">
<span class="header-left__time">2карта 12'</span>
<div>TEAM A TEAM B</div>
<div class="match-row-label">Матч</div>
<div class="card__coeffs">
<span class="coefficient-button coefficient-button_generic3">5.81</span>
<span class="coefficient-button coefficient-button_generic3">1.10</span>
<span class="coefficient-button coefficient-button_generic3">-</span>
</div>
<div class="match-row-label">Матч</div>
<div class="card__coeffs">
<span class="coefficient-button coefficient-button_generic2">1.90</span>
<span class="coefficient-button coefficient-button_generic2">1.80</span>
</div></div></body></html>
"""


PANEL_MATCH_OPEN = """
<html><body>
<div class="pinned">EPL Masters 3карта TEAM A TEAM B 1 1 48 34</div>
<section class="event-live-center">
<div>DOTA 2, EPL Masters TEAM A 1 : 1 50:10 TEAM B</div>
<div class="fast-bets__container"><div class="fast-bets__wrapper">
<div class="fast-bets__top"><div class="fast-bets__title">Популярные на матч</div></div>
<div class="fast-bets__bottom"><div class="bet-line bet-line--1">
<div class="bet-line__title --last"><span class="bet-line__market-name">Победитель</span>
<span class="bet-line__period"></span></div>
<div class="bet-line__coefs-wrapper bet-line__coefs-wrapper--2btn">
<div class="odd-btn">1.90</div><div class="odd-btn">1.80</div>
</div></div></div></div></div>
</section></body></html>
"""


def test_panel_match_ignored_for_wrong_map():
    """Шапка пишет `3карта`; на запрос карты 2 панельный «Матч» не трогаем."""
    extract = _extract(PANEL_MATCH_OPEN, "TEAM A", "TEAM B", 2, True)

    assert list(extract.odds or []) == []
    assert extract.promoted_from_match is False


def test_panel_match_with_prices_promotes_on_decider_map3():
    extract = _extract(PANEL_MATCH_OPEN, "TEAM A", "TEAM B", 3, True)

    assert list(extract.odds or []) == [1.90, 1.80]
    assert extract.promoted_from_match is True
    assert extract.market_kind == "current_map_winner"


def test_scan_does_not_abort_on_first_three_way_container():
    """Трёхисходный контейнер не обрывает поиск: вето выносится в конце."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(THREE_WAY_THEN_VALID, "html.parser")
    prices, three_way, _suspended = bk._winline_match_market_scan(soup)

    assert prices == [1.90, 1.80]
    assert three_way is True


def test_fingerprint_helper_is_compact_and_machine_readable():
    assert bk._winline_promotion_fingerprint([], series_last_map=True) == ""
    assert bk._winline_promotion_fingerprint(
        [], series_last_map=False) == "promotion=not_decider"
    assert bk._winline_promotion_fingerprint(
        ["card_header_silent", "team_order_unproven"],
        series_last_map=True) == "promotion=card_header_silent,team_order_unproven"
    # Длинный список режется: строка идёт в evidence по каждой попытке.
    many = bk._winline_promotion_fingerprint(
        [f"r{i}" for i in range(9)], series_last_map=True)
    assert many.count(",") == 3
