"""Ограничитель Stratz: блокировки строятся по заголовкам ответа, а не по таймеру.

Реальные лимиты сняты 2026-08-05 из заголовков (`x-ratelimit-limit-*`):
8/сек, 150/мин, 1500/час, 15000/сутки на ключ. Фиксированные 3 минуты блокировки
суточное окно не переживают — прогон get_pubs 05.08 полтора часа крутился в 429.

Пулы и трекеры создаются ВНУТРИ корутины: `asyncio.Lock()` на python 3.9
привязывается к текущему циклу в момент конструирования.
"""
import asyncio
import sys
import time
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from maps_research import RATE_LIMITS, ProxyAPIPool, RateLimitTracker

DAY_OUT = {
    'x-ratelimit-remaining-second': '8',
    'x-ratelimit-remaining-minute': '150',
    'x-ratelimit-remaining-hour': '1500',
    'x-ratelimit-remaining-day': '0',
    'ratelimit-reset': '15103',
    'retry-after': '15103',
}
MINUTE_OUT = dict(DAY_OUT, **{
    'x-ratelimit-remaining-minute': '0',
    'x-ratelimit-remaining-day': '150',
})
HEADROOM = dict(DAY_OUT, **{'x-ratelimit-remaining-day': '14000'})


def _pool():
    return ProxyAPIPool({
        'http://u:p@1.1.1.1:1': 'token-a',
        'http://u:p@2.2.2.2:2': 'token-b',
    })


def test_day_window_blocks_up_to_hour_cap():
    """Суточный retry-after (4 часа) режется потолком в час — чтобы перепроверять."""
    async def scenario():
        pool = _pool()
        tracker = pool.trackers[0]
        assert await tracker.can_make_request() is True
        assert await tracker.apply_rate_limit_headers(DAY_OUT) == 'day'
        assert await tracker.can_make_request() is False
        return tracker.blocked_until - time.time(), pool.trackers[1].is_rate_limited

    delay, neighbour_blocked = asyncio.run(scenario())
    assert 3590 < delay <= 3600
    assert neighbour_blocked is False


def test_minute_window_ignores_day_reset():
    """Секунда и минута не берут retry-after: он привязан к суточному окну."""
    async def scenario():
        tracker = _pool().trackers[0]
        assert await tracker.apply_rate_limit_headers(MINUTE_OUT) == 'minute'
        return tracker.blocked_until - time.time()

    assert 60 < asyncio.run(scenario()) <= 66


@pytest.mark.parametrize('headers', [None, {}, HEADROOM])
def test_no_block_while_quota_left(headers):
    async def scenario():
        tracker = _pool().trackers[0]
        scope = await tracker.apply_rate_limit_headers(headers)
        return scope, tracker.is_rate_limited

    scope, blocked = asyncio.run(scenario())
    assert scope is None
    assert blocked is False


def test_fallback_block_without_headers():
    async def scenario():
        tracker = _pool().trackers[0]
        await tracker.mark_rate_limited(scope='unknown')
        return tracker.blocked_until - time.time()

    assert 179 < asyncio.run(scenario()) <= 180


def test_sleep_until_first_free_is_capped():
    """Общий сон не длиннее cap: иначе одна кривая блокировка вешает весь сбор."""
    async def scenario():
        pool = _pool()
        for tracker in pool.trackers:
            await tracker.apply_rate_limit_headers(DAY_OUT)
        started = time.time()
        await pool._sleep_until_first_free(cap=2)
        return time.time() - started

    assert 1.5 < asyncio.run(scenario()) < 4.0


def test_local_window_still_throttles():
    """Локальный самотормоз по секундному окну работает независимо от заголовков."""
    async def scenario():
        tracker = RateLimitTracker('http://u:p@3.3.3.3:3', 'token-c')
        for _ in range(RATE_LIMITS['second']):
            await tracker.record_request()
        return await tracker.can_make_request()

    assert asyncio.run(scenario()) is False


def test_real_stratz_limits_are_not_exceeded():
    """Самотормоз должен идти ниже реального потолка ключа (429 наш счётчик не пишет)."""
    real = {'second': 8, 'minute': 150, 'hour': 1500, 'day': 15000}
    for window, cap in real.items():
        assert RATE_LIMITS[window] < cap, window
