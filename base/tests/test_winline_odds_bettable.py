"""Заморозка рынка Winline: кэф видно, но поставить нельзя.

Winline помечает недоступность исхода ТОЛЬКО классом кнопки — в CSS живой
страницы (`runtime/winline_live_dump.html`) все три состояния получают
`pointer-events: none`:

    .coefficient-button_locked,
    .coefficient-button--is-blank { color: var(--wk-coef-disabled-color); pointer-events: none }
    .coefficient-button_empty    { background-color: var(--wk-coef-disabled); pointer-events: none }

`_locked` при этом СОХРАНЯЕТ видимое число, поэтому в тексте страницы такой
рынок неотличим от рабочего, и текстовый детектор (LOCK_MARKERS) его не видит.

Разметка фикстур повторяет структуру карточки из дампа:
`div.period-name` (метка карты) + соседний `div.card__coeffs` с кнопками.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
for _p in (BASE_DIR, REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import bookmaker_selenium_odds as odds  # noqa: E402

TEAM1 = "KW"
TEAM2 = "RE.ARISE"

_ACTIVE_BUTTON = (
    "coefficient-button coefficient-button_fill "
    "coefficient-button_generic2 coefficient-button_align_center"
)


def _button(extra_class: str, value: str) -> str:
    cls = f"{_ACTIVE_BUTTON} {extra_class}".strip()
    return f'<div class="{cls}"><span>{value}</span></div>'


def _card(*map_blocks: str, team1: str = TEAM1, team2: str = TEAM2) -> str:
    """Карточка события: названия команд + произвольные блоки карт."""
    return (
        '<div class="card">'
        f'<div class="card__teams">{team1} — {team2}</div>'
        + "".join(map_blocks)
        + "</div>"
    )


def _map_block(map_num: int, *buttons: str) -> str:
    return (
        '<div class="card__body card__body--second">'
        f'<div class="period-name">{map_num} карта</div>'
        '<div class="card__coeffs">'
        '<ww-feature-event-market-dsk class="card__market">'
        + "".join(buttons)
        + "</ww-feature-event-market-dsk></div></div>"
    )


# Ячейка-заполнитель под непредложенный рынок: класса рынка нет, число тоже.
# На живой странице таких кнопок большинство — именно они и «замораживали»
# рабочие рынки, пока вердикт считался по всему контейнеру.
_FILLER = '<div class="coefficient-button coefficient-button_empty coefficient-button_align_center"><span>-</span></div>'


def _market_button(market: str, extra_class: str, value: str) -> str:
    cls = (
        "coefficient-button coefficient-button_fill "
        f"coefficient-button_{market} coefficient-button_align_center {extra_class}"
    ).strip()
    return f'<div class="{cls}"><span>{value}</span></div>'


def _bettable(html: str, map_num: int):
    return odds._winline_map_odds_bettable(html, TEAM1, TEAM2, map_num)


def test_active_market_is_bettable() -> None:
    html = _card(_map_block(2, _button("", "3.00"), _button("", "1.30")))

    assert _bettable(html, 2) is True


def test_locked_market_keeps_number_but_is_not_bettable() -> None:
    """Главный случай: 1.50/2.40 видны, но приём ставок заблокирован."""
    html = _card(
        _map_block(
            1,
            _button("coefficient-button_locked", "1.50"),
            _button("coefficient-button_locked", "2.40"),
        )
    )

    assert _bettable(html, 1) is False


def test_single_locked_side_blocks_the_market() -> None:
    """Заблокирован хотя бы один исход — ставить по паре уже нельзя."""
    html = _card(
        _map_block(
            1,
            _button("coefficient-button_locked", "1.50"),
            _button("", "2.40"),
        )
    )

    assert _bettable(html, 1) is False


def test_blank_and_empty_buttons_are_not_bettable() -> None:
    blank = _card(
        _map_block(
            1,
            _button("coefficient-button--is-blank", "-"),
            _button("coefficient-button--is-blank", "-"),
        )
    )
    empty = _card(_map_block(1, _button("coefficient-button_empty", "")))

    assert _bettable(blank, 1) is False
    assert _bettable(empty, 1) is False


def test_verdict_is_scoped_to_the_requested_map() -> None:
    """Скрин пользователя: 1 карта заморожена, 2 карта торгуется."""
    html = _card(
        _map_block(
            1,
            _button("coefficient-button_locked", "1.50"),
            _button("coefficient-button_locked", "2.40"),
        ),
        _map_block(2, _button("", "3.00"), _button("", "1.30")),
    )

    assert _bettable(html, 1) is False
    assert _bettable(html, 2) is True


def test_other_match_card_is_never_read() -> None:
    """Рынок соседнего матча не должен отвечать за наши команды."""
    foreign = _card(
        _map_block(1, _button("", "1.80"), _button("", "2.10")),
        team1="Spirit",
        team2="Falcons",
    )

    assert _bettable(foreign, 1) is None


def test_unknown_state_fails_open() -> None:
    """Нет доказательства блокировки — не выдумываем его (fail-open)."""
    no_buttons = _card(
        '<div class="card__body"><div class="period-name">1 карта</div></div>'
    )

    assert _bettable("", 1) is None
    assert _bettable(no_buttons, 1) is None
    assert _bettable(_card(_map_block(2, _button("", "3.00"))), 1) is None


# --- Вердикт считается по рынку победителя, а не по всему контейнеру ---------


def test_filler_cells_do_not_freeze_a_live_market() -> None:
    """Живой рынок 1.14/4.80 рядом с пустыми ячейками соседних колонок.

    Реальная карточка Winline (`runtime/winline_live_dump.html`, EX-RUSTEC vs
    RUNE EATERS, 3 карта): победитель торгуется, а шесть ячеек форы/тотала
    отрисованы заполнителями. До правки такая карточка читалась как locked.
    """
    html = _card(
        _map_block(
            3,
            _market_button("generic2", "coefficient-button_down", "1.14"),
            _market_button("generic2", "coefficient-button_up", "4.80"),
            _FILLER * 6,
        )
    )

    assert _bettable(html, 3) is True


def test_locked_side_market_does_not_block_the_winner() -> None:
    """Заморожена фора — ставка на победителя от этого не запрещена."""
    html = _card(
        _map_block(
            1,
            _market_button("generic2", "", "2.13"),
            _market_button("generic2", "", "1.64"),
            _market_button("handicap2", "coefficient-button_locked", "1.88"),
            _market_button("handicap2", "coefficient-button--is-blank", "-"),
        )
    )

    assert _bettable(html, 1) is True


def test_pinned_bar_without_winner_market_does_not_decide() -> None:
    """Закреплённый бар — самый короткий кандидат, но рынка победителя в нём нет.

    Кандидаты перебираются от узких к широким, поэтому бар попадается первым.
    Вердикт по нему = вердикт по заполнителям; отвечать должна карточка.
    """
    pinned = (
        '<div class="ww-pinned-card">'
        f'<div class="card__teams">{TEAM1} — {TEAM2}</div>'
        '<div class="card__body"><div class="period-name">1 карта</div>'
        f'<div class="card__coeffs">{_FILLER * 2}</div></div></div>'
    )
    card = _card(
        _map_block(
            1,
            _market_button("generic2", "", "1.50"),
            _market_button("generic2", "", "2.40"),
        )
    )

    assert _bettable(f'<div class="page">{pinned}{card}</div>', 1) is True


def test_stale_locked_shadow_does_not_override_live_card() -> None:
    """Старая locked-тень не закрывает тот же рынок в живой карточке.

    Angular может оставить закреплённое DOM-представление с предыдущим
    состоянием кнопок и рядом отрисовать актуальную полную карточку. Доступность
    должна агрегироваться по всем доказанным представлениям точного рынка.
    """
    stale_shadow = (
        '<div class="ww-pinned-card shadow-card">'
        f'<div class="card__teams">{TEAM1} — {TEAM2}</div>'
        + _map_block(
            1,
            _market_button("generic2", "coefficient-button_locked", "1.50"),
            _market_button("generic2", "coefficient-button_locked", "2.40"),
        )
        + "</div>"
    )
    live_card = _card(
        _map_block(
            1,
            _market_button("generic2", "", "1.57"),
            _market_button("generic2", "", "2.25"),
        )
    )

    assert _bettable(f'<div class="page">{stale_shadow}{live_card}</div>', 1) is True


def test_open_neighbour_does_not_override_locked_target() -> None:
    """Открытая первая карта соседнего матча не делает целевой рынок live."""
    target = _card(
        _map_block(
            1,
            _market_button("generic2", "coefficient-button_locked", "1.50"),
            _market_button("generic2", "coefficient-button_locked", "2.40"),
        ),
        team1="FTS",
        team2="PuckChamp",
    )
    neighbour = _card(
        _map_block(
            1,
            _market_button("generic2", "", "1.80"),
            _market_button("generic2", "", "2.10"),
        ),
        team1="Spirit",
        team2="Falcons",
    )

    assert (
        odds._winline_map_odds_bettable(
            f'<div class="multi-event-feed">{target}{neighbour}</div>',
            "FTS",
            "PuckChamp",
            1,
        )
        is False
    )


# --- Путь ставки: замороженный кэф не должен доехать до диспетчера -----------


_LOCKED_PAGE = _card(
    _map_block(
        1,
        _button("coefficient-button_locked", "1.50"),
        _button("coefficient-button_locked", "2.40"),
    )
)


class _Page:
    """Дублёр Playwright-страницы: content() отдаёт готовое значение."""

    def __init__(self, html: str = "", raises: bool = False):
        self._html = html
        self._raises = raises

    def content(self):
        if self._raises:
            raise RuntimeError("page closed")
        return self._html


class _Driver:
    def __init__(self, html: str = "", raises: bool = False):
        self._html = html
        self._raises = raises

    @property
    def page_source(self):
        if self._raises:
            raise RuntimeError("driver dead")
        return self._html


def test_locked_extract_is_downgraded_to_closed_market() -> None:
    """Числа снимаются: ниже по конвейеру наличие odds значит «можно ставить»."""
    wl = odds._WinlineMapExtract(
        odds=[1.50, 2.40], map_num=1, market_kind="current_map_winner"
    )

    odds._winline_mark_locked_as_closed(wl)

    assert wl.odds == []
    assert wl.market_closed is True
    assert wl.reason == "closed"
    assert "locked" in wl.details


def test_page_bettability_reads_live_html() -> None:
    import asyncio

    locked = asyncio.run(
        odds._winline_page_odds_bettable(_Page(_LOCKED_PAGE), TEAM1, TEAM2, 1)
    )
    active = asyncio.run(
        odds._winline_page_odds_bettable(
            _Page(_card(_map_block(1, _button("", "1.50"), _button("", "2.40")))),
            TEAM1,
            TEAM2,
            1,
        )
    )

    assert locked is False
    assert active is True


def test_page_and_driver_failures_fail_open() -> None:
    """Сбой съёма страницы не должен выглядеть как блокировка."""
    import asyncio

    assert (
        asyncio.run(
            odds._winline_page_odds_bettable(_Page(raises=True), TEAM1, TEAM2, 1)
        )
        is None
    )
    assert _winline_driver_verdict(_Driver(raises=True)) is None


def _winline_driver_verdict(driver):
    return odds._winline_driver_odds_bettable(driver, TEAM1, TEAM2, 1)


def test_driver_bettability_reads_page_source() -> None:
    assert _winline_driver_verdict(_Driver(_LOCKED_PAGE)) is False
    assert (
        _winline_driver_verdict(
            _Driver(_card(_map_block(1, _button("", "1.50"), _button("", "2.40"))))
        )
        is True
    )
