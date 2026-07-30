"""Sync-обёртки парсера внутри работающего event loop.

Регрессия с прода 2026-07-25: рабочий поток общей Camoufox-сессии держит event
loop (его поднимает sync-API Playwright), поэтому обёртки падали с
"sync entrypoint called from a running event loop". Каждая попытка снять кэфы
Winline умирала, ошибка списывалась на прокси и браузер пересоздавался по кругу.

Обёртки работают потому, что в модуле нет ни одной настоящей точки
приостановки: все await'ы — _maybe_await поверх синхронных объектов Playwright.
Тесты закрепляют и это свойство, и внятный отказ, если оно нарушится.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import bookmaker_selenium_odds as odds_parser  # noqa: E402


async def _plain():
    return "результат"


async def _through_maybe_await():
    return await odds_parser._maybe_await("значение")


async def _through_maybe_await_awaitable():
    async def _inner():
        return "из корутины"

    return await odds_parser._maybe_await(_inner())


async def _really_suspends():
    await asyncio.sleep(0)
    return "недостижимо"


def test_works_without_running_loop():
    assert odds_parser._run_coroutine_blocking(_plain()) == "результат"


def test_works_inside_running_loop():
    """Главный случай: именно так зовёт рабочий поток общей сессии."""

    async def outer():
        return odds_parser._run_coroutine_blocking(_plain())

    assert asyncio.run(outer()) == "результат"


def test_maybe_await_chain_inside_running_loop():
    async def outer():
        return (odds_parser._run_coroutine_blocking(_through_maybe_await()),
                odds_parser._run_coroutine_blocking(_through_maybe_await_awaitable()))

    assert asyncio.run(outer()) == ("значение", "из корутины")


def test_real_suspension_fails_loudly_not_silently():
    """Если в модуле появится настоящий await — падаем с понятным текстом."""

    async def outer():
        with pytest.raises(RuntimeError, match="real suspension point"):
            odds_parser._run_coroutine_blocking(_really_suspends())

    asyncio.run(outer())


def test_module_has_no_real_suspension_points():
    """Инвариант, на котором держится ручная прокрутка корутин.

    Появление asyncio.sleep/gather/wait_for в модуле сломает sync-обёртки —
    тогда придётся переводить вызывающие стороны на *_async, а не «просто
    добавить await».
    """
    source = open(odds_parser.__file__, encoding="utf-8").read()
    # Ищем синтаксис ВЫЗОВА (со скобкой): иначе тест ловит собственные
    # упоминания этих имён в комментариях самого модуля.
    for forbidden in ("asyncio.sleep(", "asyncio.gather(", "asyncio.wait_for(",
                      "asyncio.as_completed("):
        assert forbidden not in source, (
            f"{forbidden} вводит настоящую приостановку — sync-обёртки "
            f"(parse_site_in_camoufox_page и другие) перестанут работать "
            f"внутри рабочего потока общей Camoufox-сессии"
        )
