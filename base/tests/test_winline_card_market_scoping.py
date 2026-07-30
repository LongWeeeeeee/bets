"""Winline card scoping: рынок текущей карты берём ТОЛЬКО из своей карточки.

Регрессия на реальный дефект, найденный на живом матче Nemiga Gaming vs
Level UP esports (25.07.2026): минимальный DOM-контейнер с обеими командами —
это шапка матча (`NEMIGA GAMING LEVEL UP 1карта 30' +8`), в которой кэфов нет.
Прежняя эвристика `min(кандидаты с map-маркером)` выбирала именно её, и парсер
возвращал `market missing`, хотя рынок первой карты был открыт (3.70 / 1.21).

Обратный случай (открытая находка Reviewer'а): подниматься по DOM до рынка
нельзя бесконечно — контейнер, накрывший вторую карточку, отдал бы чужие кэфы.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bookmaker_selenium_odds as bk  # noqa: E402

TEAM1 = "Nemiga Gaming"
TEAM2 = "Level UP esports"

# Форма живой карточки Winline: команды в шапке, кэфы — в соседнем узле.
LIVE_CARD_HTML = """
<div class="tournament">
  <div class="event">
    <div class="hdr"><a href="/x">NEMIGA GAMING LEVEL UP</a><span>1карта 30' +8</span></div>
    <div class="stats">0 0 16 25</div>
    <div class="markets">1К Матч 2.55 1.42 1.55 + 1.5 - 2.30 1.72 2.5 2.00
      1 карта 3.70 1.21 - - - - - -</div>
  </div>
</div>
"""

# Целевая карточка БЕЗ рынка первой карты, соседняя — С рынком. Разделителей
# дисциплины (`DOTA 2 |`) в тексте нет: именно этот случай оставался открытым.
CROSS_CARD_HTML = """
<div class="feed">
  <div class="event">
    <div class="hdr">ZERO TENACITY ILBIRS</div>
    <div class="markets">Матч 1.38 2.80</div>
  </div>
  <div class="event">
    <div class="hdr">NEXT RADIANT NEXT DIRE</div>
    <div class="markets">1 карта 1.91 1.83</div>
  </div>
</div>
"""

# Обратный порядок команд, но рынок лежит в СВОЕЙ карточке.
REVERSE_ORDER_HTML = """
<div class="feed">
  <div class="event">
    <div class="hdr">ZERO TENACITY ILBIRS</div>
    <div class="markets">Матч 1.38 2.80 1 карта 1.62 2.15</div>
  </div>
  <div class="event">
    <div class="hdr">NEXT RADIANT NEXT DIRE</div>
    <div class="markets">1 карта 1.91 1.83</div>
  </div>
</div>
"""


def _card(html: str, team1: str, team2: str, map_num: int) -> str:
    return bk._winline_matched_card_context("", team1, team2, html=html, map_num=map_num) or ""


def test_header_only_container_does_not_hide_current_map_market():
    """Шапка матча — не доказательство отсутствия рынка: поднимаемся до карточки."""
    card = _card(LIVE_CARD_HTML, TEAM1, TEAM2, 1)
    assert "3.70" in card and "1.21" in card, card

    extract = bk._extract_winline_current_map_winner(card, TEAM1, TEAM2, 1)
    assert extract.odds == [3.70, 1.21]
    assert extract.market_kind == "current_map_winner"
    assert extract.map_num == 1


def test_cross_card_market_is_not_consumed():
    """Рынок соседней карточки не подставляется вместо отсутствующего своего."""
    team1, team2 = "Ilbirs Esports", "Zero Tenacity"
    card = _card(CROSS_CARD_HTML, team1, team2, 1)

    assert "1.91" not in card and "1.83" not in card, card

    extract = bk._extract_winline_current_map_winner(card, team1, team2, 1)
    assert extract.odds == []
    assert not extract.market_closed


def test_reverse_order_market_inside_own_card_is_accepted():
    """Обратный порядок команд не мешает, кэфы канонизируются к team1/team2."""
    team1, team2 = "Ilbirs Esports", "Zero Tenacity"
    card = _card(REVERSE_ORDER_HTML, team1, team2, 1)

    assert "1.91" not in card, card

    extract = bk._extract_winline_current_map_winner(card, team1, team2, 1)
    assert extract.odds == [2.15, 1.62]
    assert extract.p1_team == "team1"
    assert extract.p2_team == "team2"


@pytest.mark.parametrize("map_num", [2, 3, 5])
def test_absent_map_market_fails_closed(map_num: int):
    """Рынка запрошенной карты нет — матч найден, кэфы пусты, polling продолжается."""
    card = _card(LIVE_CARD_HTML, TEAM1, TEAM2, map_num)
    extract = bk._extract_winline_current_map_winner(card, TEAM1, TEAM2, map_num)
    assert extract.odds == []


def test_price_bearing_children_counts_sibling_cards():
    """Счётчик ценовых поддеревьев отличает одну карточку от контейнера с двумя."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(CROSS_CARD_HTML, "html.parser")
    feed = soup.find("div", class_="feed")
    events = soup.find_all("div", class_="event")

    assert bk._winline_price_bearing_children(feed) == 2
    assert bk._winline_price_bearing_children(events[0]) == 1
