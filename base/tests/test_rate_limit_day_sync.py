"""Синхронизация суточного счётчика с серверным остатком Stratz.

Проблема, которую это чинит (E-29). Локально сутки считаются СКОЛЬЗЯЩИМ окном
по нашему журналу запросов, у Stratz окно ФИКСИРОВАННОЕ и откатывается разом.
После отката сервер отдаёт свежие 15 000, а наш журнал помнит вчерашние
отметки — пара стоит в «локальном самотормозе» при свободной квоте. Замер
06.08: три аккаунта имели остаток 14 998 из 15 000, пока сбор писал, что все
пары упёрлись в самотормоз.

Инвариант: счётчик только ОПУСКАЕТСЯ до серверного значения. Поднимать его
нельзя — иначе потеряется наш запас (14 800 против реальных 15 000), который
существует потому, что 429-ответы наш счётчик не пишет, а API их считает.
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import maps_research  # noqa: E402


def _fill_day(tracker, count):
    now = time.time()
    log = tracker.requests_log["day"]
    log.clear()
    for i in range(count):
        log.append(now - i)


def _run(prefilled, headers, *, shared_pair=False):
    """Создаёт трекер и применяет заголовки в ОДНОМ цикле событий.

    На Python 3.9 `asyncio.Lock()` привязывается к текущему циклу в момент
    создания, а `asyncio.run()` каждый раз делает новый — трекер, созданный
    снаружи, во втором тесте упрётся в закрытый цикл. Поэтому конструируем
    внутри корутины.
    """

    async def go():
        tr = maps_research.RateLimitTracker("http://u:p@1.2.3.4:1234", "token-x")
        second = None
        if shared_pair:
            second = maps_research.RateLimitTracker(
                "http://p2", "token-x", shared_state=tr._shared
            )
        _fill_day(tr, prefilled)
        await (second or tr).apply_rate_limit_headers(headers)
        return tr

    return asyncio.run(go())


def test_window_reset_releases_the_local_throttle() -> None:
    """Главный случай: сервер откатил сутки, локальный журнал обязан очиститься."""
    tr = _run(14800, {"x-ratelimit-limit-day": "15000",
                      "x-ratelimit-remaining-day": "14998"})

    assert len(tr.requests_log["day"]) == 2, "после отката окна пара должна освободиться"


def test_partial_usage_is_synced_down() -> None:
    tr = _run(9000, {"x-ratelimit-limit-day": "15000",
                     "x-ratelimit-remaining-day": "12000"})

    assert len(tr.requests_log["day"]) == 3000


def test_counter_is_never_raised() -> None:
    """Наш счётчик намеренно строже серверного: 429 мы не пишем, а API пишет.
    Подъём стёр бы этот запас."""
    tr = _run(500, {"x-ratelimit-limit-day": "15000",
                    "x-ratelimit-remaining-day": "5000"})

    assert len(tr.requests_log["day"]) == 500, "счётчик обязан только опускаться"


def test_missing_header_changes_nothing() -> None:
    tr = _run(777, {"x-ratelimit-remaining-hour": "100"})

    assert len(tr.requests_log["day"]) == 777


def test_limit_header_absent_falls_back_to_our_constant() -> None:
    day_limit = maps_research.RATE_LIMITS["day"]
    tr = _run(14800, {"x-ratelimit-remaining-day": str(day_limit - 10)})

    assert len(tr.requests_log["day"]) == 10


def test_exhausted_day_still_blocks_the_pair() -> None:
    """Синхронизация не должна ломать блокировку по нулевому остатку."""
    tr = _run(100, {"x-ratelimit-limit-day": "15000",
                    "x-ratelimit-remaining-day": "0", "retry-after": "60"})

    assert tr.is_rate_limited is True


def test_sync_is_shared_between_tokens_of_one_account() -> None:
    """Два трекера с одним токеном делят состояние — синхронизация тоже общая."""
    tr = _run(12000, {"x-ratelimit-limit-day": "15000",
                      "x-ratelimit-remaining-day": "14999"}, shared_pair=True)

    assert len(tr.requests_log["day"]) == 1
