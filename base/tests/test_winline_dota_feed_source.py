"""Источник live-кэфов Winline: дотовская страница + дорисовка ленты.

Контракт:
- live-кэфы берём со страницы `kibersport/dota_2`, а не с общего live-фида:
  общий фид рендерит ленту порциями во ВНУТРЕННЕМ контейнере, и матч ниже
  первой порции просто отсутствует в DOM (05.08.2026: `No Hoodwink — Lynx`,
  65 промахов поллера подряд при живой карточке на сайте);
- цена переезда — на дотовской странице соседствует линия (`Завтра 15:00`) с
  рынком `1 карта`, поэтому live определяется ТОЛЬКО по статусу игры
  (`2карта`, `2К`, таймер), а не по названию рынка и не по двоеточию;
- страховка на случай длинной дотовской ленты: прокрутка внутреннего
  контейнера дорисовывает карточки, промах перечитывает снимок.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List, Optional

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import bookmaker_selenium_odds as odds_parser  # noqa: E402


# Снимки живой страницы 05.08.2026 (лог winline_parser_monitor + дамп ленты).
LIVE_CARD = (
    "NO HOODWINK LYNX 2карта +18 1 0 5 3 2К 39 21 1К "
    "Матч 1.05 7.50 1.13 - 1.5 + 5.00 1.13 2.5 5.00 2 карта 1.13 5.00"
)
LIVE_CARD_WITH_TIMER = (
    "TEAM LIQUID 1W 1карта 18' +50 0 0 19 2 1К Матч 1.50 2.45 1 карта 1.04 9.52"
)
PREMATCH_CARD = (
    "RE.ARISE NO HOODWINK Завтра 15:00 +9 "
    "Матч 2.46 1.48 1.50 + 1.5 - 2.43 1.75 2.5 1.96 1 карта 2.23 1.58"
)
PREMATCH_CARD_TODAY = (
    "ILBIRS POWER RANGERS Сегодня 21:00 +12 Матч 1.90 1.90 1 карта 1.95 1.85"
)


def test_live_source_is_dota_page() -> None:
    assert odds_parser.BOOKMAKER_URLS["live"]["winline"] == (
        "https://winline.ru/stavki/sport/kibersport/dota_2"
    )


def test_live_card_is_not_filtered_as_future() -> None:
    assert odds_parser._looks_future_context(LIVE_CARD) is False
    assert odds_parser._looks_future_context(LIVE_CARD_WITH_TIMER) is False


def test_prematch_card_is_filtered_even_with_map_market() -> None:
    # Рынок `1 карта` и двоеточие в `15:00` больше не считаются доказательством
    # live: именно они раньше протаскивали карточку линии как живую.
    assert odds_parser._looks_future_context(PREMATCH_CARD) is True
    assert odds_parser._looks_future_context(PREMATCH_CARD_TODAY) is True


def test_live_card_survives_neighbouring_prematch_clock() -> None:
    # Окно контекста может захватить соседнюю карточку линии — статус игры
    # у живой карточки должен перевесить чужие часы старта.
    spilled = f"{LIVE_CARD} {PREMATCH_CARD}"
    assert odds_parser._looks_future_context(spilled) is False


class _SweepPage:
    """Страница с внутренней лентой: карточка появляется после прокрутки."""

    def __init__(self, *, head: str, tail: str, url: str) -> None:
        self._head = head
        self._tail = tail
        self.url = url
        self.evaluate_calls: List[str] = []
        self.scrolled = False
        self.goto_calls = 0

    def _text(self) -> str:
        return f"{self._head} {self._tail}" if self.scrolled else self._head

    def goto(self, url: str, wait_until: str = "domcontentloaded", timeout: int = 0):  # noqa: ARG002
        self.goto_calls += 1
        self.url = url

    def reload(self, wait_until: str = "domcontentloaded", timeout: int = 0):  # noqa: ARG002
        return None

    def content(self) -> str:
        return f"<html><body>{self._text()}</body></html>"

    def locator(self, selector: str):
        assert selector == "body"
        return _Locator(self._text())

    def title(self) -> str:
        return ""

    def evaluate(self, script: str, arg: Any = None):  # noqa: ARG002
        self.evaluate_calls.append(script)
        if "scrollTop" in script:
            already = self.scrolled
            self.scrolled = True
            return not already  # второй проход уже ничего не двигает
        if "document.readyState" in script:
            return "complete"
        return False


class _Locator:
    def __init__(self, text: str) -> None:
        self._text = text

    def inner_text(self, timeout: int = 0):  # noqa: ARG002
        return self._text


@pytest.fixture(autouse=True)
def _no_sleep_and_clean_state(monkeypatch):
    monkeypatch.setattr(odds_parser.time, "sleep", lambda *_a, **_k: None)
    odds_parser._feed_sweep_last_run.clear()
    yield
    odds_parser._feed_sweep_last_run.clear()


def _make_page() -> _SweepPage:
    return _SweepPage(
        head="DOTA 2 | Games of the Future PLAYTIME YAKULT BROTHERS 2карта 16' 2К",
        tail=f"DOTA 2 | Asgard Championship {LIVE_CARD}",
        url=odds_parser.BOOKMAKER_URLS["live"]["winline"],
    )


def _sweep(page, **kwargs) -> bool:
    return odds_parser._run_coroutine_blocking(odds_parser._sweep_camoufox_feed(page, **kwargs))


def test_sweep_scrolls_inner_container_and_stops_when_exhausted() -> None:
    page = _make_page()
    assert _sweep(page, force=True) is True
    assert page.scrolled is True
    # Прокрутка ограничена: как только контейнер перестал двигаться — выходим.
    assert len(page.evaluate_calls) == 2
    assert len(page.evaluate_calls) <= odds_parser.WINLINE_FEED_SWEEP_STEPS


def test_sweep_is_throttled_per_page() -> None:
    page = _make_page()
    assert _sweep(page, now=1000.0) is True
    before = len(page.evaluate_calls)
    assert _sweep(page, now=1000.0 + odds_parser.WINLINE_FEED_SWEEP_MIN_INTERVAL_SECONDS / 2) is False
    assert len(page.evaluate_calls) == before, "троттлинг не должен трогать страницу"
    # По истечении интервала прокрутка снова разрешена.
    page.scrolled = False
    assert _sweep(page, now=1000.0 + odds_parser.WINLINE_FEED_SWEEP_MIN_INTERVAL_SECONDS + 1) is True


def test_missing_card_triggers_sweep_and_rereads_snapshot(monkeypatch) -> None:
    page = _make_page()
    result = odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        page.url,
        "Team Lynx",
        "No Hoodwink",
        mode="live",
        forced_map_num=2,
        acquisition_mode="dynamic_dom",
    )
    assert page.scrolled is True, "промах обязан дорисовать ленту прокруткой"
    assert result.match_found is True
    # Кэфы отдаются в порядке ЗАПРОСА (team1=Team Lynx), а не в порядке карточки
    # (`NO HOODWINK LYNX ... 2 карта 1.13 5.00`) — так же, как в проде 05.08.2026.
    assert [round(x, 2) for x in list(result.odds)[:2]] == [5.00, 1.13]
    assert getattr(result, "market_kind", None) == "current_map_winner"
    assert getattr(result, "map_num", None) == 2


def test_present_card_does_not_scroll(monkeypatch) -> None:
    page = _make_page()
    page.scrolled = True  # карточка уже в DOM
    page.evaluate_calls.clear()
    odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        page.url,
        "Team Lynx",
        "No Hoodwink",
        mode="live",
        forced_map_num=2,
        acquisition_mode="dynamic_dom",
    )
    assert not [call for call in page.evaluate_calls if "scrollTop" in call], (
        "когда пара уже в снимке, лишняя прокрутка поллеру не нужна"
    )
