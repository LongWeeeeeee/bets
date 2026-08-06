"""Замороженный рынок не должен отдавать кэфы ни из одной ветки разбора.

Winline помечает недоступность исхода ТОЛЬКО классом кнопки, и `_locked`
при этом СОХРАНЯЕТ видимое число — рынок неотличим от торгуемого по тексту
страницы. Ветка разбора «по компонентам» (`ww-feature-event-market-dsk` в
карточке ленты и `ww-feature-event-top-markets-dsk` в панели события) читала
числа из прямых детей контейнера, не глядя на класс кнопки, и отдавала цену
замороженного рынка. Гасил её только отдельный детектор
`_winline_map_odds_bettable`, который панель не понимает и на странице
события возвращает None (fail-open) — то есть на панели заморозку не ловил
никто.

Фикстура — живой фрагмент страницы (дамп 31.07.2026): карточка ленты и панель
выбранного события одного матча. Классы заморозки тест расставляет сам, потому
что дампа именно замороженного рынка у нас нет, а синтетика этот дефект не
воспроизводит.
"""
from __future__ import annotations

import sys
from pathlib import Path

from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
for _p in (BASE_DIR, REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import bookmaker_selenium_odds as odds  # noqa: E402

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "winline_legacy_locked_20260806.html"
TEAM1 = "PAIN GAMING"
TEAM2 = "ASTRALIS"
MAP_NUM = 2
OPEN_ODDS = [3.72, 1.28]


def _classes(node):
    raw = node.get("class") if hasattr(node, "get") else None
    if not raw:
        return []
    return list(raw) if isinstance(raw, list) else [raw]


def _soup():
    return BeautifulSoup(FIXTURE.read_text(encoding="utf-8"), "html.parser")


def _drop(soup, selector):
    for node in soup.select(selector):
        node.decompose()
    return soup


def _lock_listing_card(soup):
    """Заморозить кнопки победителя нужной карты в карточке ленты."""
    touched = 0
    for label in soup.find_all(True):
        if "period-name" not in _classes(label):
            continue
        if " ".join(label.stripped_strings).strip().lower() != f"{MAP_NUM} карта":
            continue
        for container in [c for c in (label.find_next_sibling(), label.parent) if c]:
            buttons = [
                node
                for node in container.find_all(True)
                if "coefficient-button_generic2" in _classes(node)
            ]
            if not buttons:
                continue
            for button in buttons:
                button["class"] = _classes(button) + ["coefficient-button_locked"]
                touched += 1
            break
    return touched


def _lock_event_panel(soup):
    """Заморозить кнопки строки «Победитель N карта» в панели события."""
    touched = 0
    for line in soup.select(".bet-line"):
        name = line.select_one(".bet-line__market-name")
        period = line.select_one(".bet-line__period")
        if name is None or period is None:
            continue
        if " ".join(name.stripped_strings).lower() != "победитель":
            continue
        if " ".join(period.stripped_strings).strip().lower() != f"{MAP_NUM} карта":
            continue
        for button in line.select(".bet-line__coefs-wrapper .odd-btn"):
            button["class"] = _classes(button) + ["coef-btn--locked"]
            touched += 1
    return touched


def _extract(soup):
    html = str(soup)
    return odds._extract_winline_current_map_winner("", TEAM1, TEAM2, MAP_NUM, html=html)


def test_fixture_holds_both_representations():
    """Фикстура обязана нести обе ветки — иначе тест ничего не проверяет."""
    soup = _soup()
    assert soup.select("ww-feature-event-market-dsk"), "нет карточки ленты"
    assert soup.select("ww-feature-event-top-markets-dsk"), "нет панели события"


def test_open_market_still_returns_odds():
    """Живой рынок не должен пострадать: перекрыть поток дороже, чем пропустить."""
    extract = _extract(_soup())
    assert extract.odds == OPEN_ODDS
    assert not extract.market_closed


def test_locked_listing_card_blocks_odds():
    """Карточка ленты заморожена — цены из неё брать нельзя."""
    soup = _drop(_soup(), "ww-feature-event-live-center-dsk")
    assert _lock_listing_card(soup) == 2
    extract = _extract(soup)
    assert extract.odds == []
    assert extract.market_closed or extract.reason == "closed"


def test_locked_event_panel_blocks_odds():
    """Панель события заморожена — цены из неё брать нельзя.

    Именно этот случай тёк молча: детектор доступности на странице события
    возвращает None, а ветка панели класс кнопки не проверяла.
    """
    soup = _drop(_soup(), "ww-feature-block-event-dsk")
    assert _lock_event_panel(soup) == 2
    extract = _extract(soup)
    assert extract.odds == []
    assert extract.market_closed or extract.reason == "closed"


def test_locked_both_representations_block_odds():
    """Обе ветки заморожены — рынок закрыт, кэфов нет."""
    soup = _soup()
    assert _lock_listing_card(soup) == 2
    assert _lock_event_panel(soup) == 2
    extract = _extract(soup)
    assert extract.odds == []
    assert extract.market_closed or extract.reason == "closed"


def test_locked_panel_is_invisible_to_bettable_detector():
    """Фиксируем причину, по которой правка обязана жить в парсере.

    Детектор доступности понимает только разметку карточки ленты. На странице
    события (карточки нет) он вердикта не выносит, и если бы заморозку ловил
    только он — залоченный кэф уходил бы в поток.
    """
    soup = _drop(_soup(), "ww-feature-block-event-dsk")
    _lock_event_panel(soup)
    assert odds._winline_map_odds_bettable(str(soup), TEAM1, TEAM2, MAP_NUM) is None
