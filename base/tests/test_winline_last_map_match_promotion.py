"""Последняя карта серии: рынка карты нет — берём победителя матча.

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
в серии, подписи рынка запрошенной карты не должно быть ни в одной карточке пары,
рынок «Матч» обязан быть двухисходным и принимать ставку, карточка обязана сама
сообщать, что идёт именно эта карта, а порядок сторон берётся из текста карточки.
"""
from __future__ import annotations

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
