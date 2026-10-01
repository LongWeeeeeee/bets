"""Регрессия: мёртвый STRATZ-прокси не получает каждый пятый запрос."""
import asyncio
import threading
from collections import Counter
from types import SimpleNamespace

import pytest
from curl_cffi.requests.exceptions import ConnectionError, InvalidURL, ProxyError, Timeout

import maps_research as mr


DEAD = "http://142.252.242.116:63099"
LIVE = "http://live:63099"
URL = "https://api.stratz.com/graphql"
# serv1, 2026-10-01: runtime/artifacts/pubs-rebuild/
# get_pubs_20261001_052301_2560237.log (captured by the lead).
CAPTURED_TIMEOUT = (
    "Failed to perform, curl: (28) Connection timed out after 120002 milliseconds"
)


@pytest.fixture(autouse=True)
def event_loop():
    # Python 3.9 binds Locks/Semaphores during pool construction.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)


@pytest.fixture
def clock(monkeypatch):
    state = SimpleNamespace(now=1_800_000_000.0, sleeps=[])
    real_sleep = asyncio.sleep

    async def sleep(delay):
        state.sleeps.append(delay)
        assert len(state.sleeps) <= 100, "selection busy-spins instead of waiting"
        state.now += delay
        await real_sleep(0)

    monkeypatch.setattr(mr.time, "time", lambda: state.now)
    monkeypatch.setattr(mr.asyncio, "sleep", sleep)
    return state


def install_http(monkeypatch, handler):
    monkeypatch.setattr(mr.ProxyAPIPool, "_post_json_with_requests", staticmethod(handler))


def request(pool):
    return pool.make_request(URL)


def test_one_dead_of_five_is_attempted_once_per_cooldown(monkeypatch, clock, event_loop):
    pairs = [(DEAD, "dead-token")] + [
        ("http://live%d:63099" % i, "token%d" % i) for i in range(4)
    ]
    pool = mr.ProxyAPIPool(pairs)
    calls = Counter()

    def post(url, proxy, **kwargs):
        calls[proxy] += 1
        if proxy == DEAD:
            raise Timeout(CAPTURED_TIMEOUT, code=28)
        return {"data": {}}, {}

    install_http(monkeypatch, post)

    async def run():
        for _ in range(100):
            assert await request(pool) == {"data": {}}
            clock.now += 1.1

    event_loop.run_until_complete(run())
    assert calls[DEAD] == 1
    assert sum(calls[p] for p, _ in pairs[1:]) == 100
    assert not clock.sleeps


def test_shared_token_live_proxy_still_serves(monkeypatch, clock, event_loop):
    pool = mr.ProxyAPIPool([(DEAD, "shared"), (LIVE, "shared")])
    calls = []

    def post(url, proxy, **kwargs):
        calls.append(proxy)
        if proxy == DEAD:
            raise Timeout(CAPTURED_TIMEOUT, code=28)
        return {"data": {}}, {}

    install_http(monkeypatch, post)

    async def run():
        for _ in range(10):
            assert await request(pool) == {"data": {}}
            clock.now += 1.1

    event_loop.run_until_complete(run())
    assert calls == [DEAD] + [LIVE] * 10
    assert not clock.sleeps


def test_escalation_cap_and_success_reset(monkeypatch, clock, capsys, event_loop):
    pool = mr.ProxyAPIPool([(DEAD, "dead"), (LIVE, "live")])
    calls = []
    recovered = False

    def post(url, proxy, **kwargs):
        calls.append(proxy)
        if proxy == DEAD and not recovered:
            raise Timeout(CAPTURED_TIMEOUT, code=28)
        return {"data": {}}, {}

    install_http(monkeypatch, post)

    async def run():
        nonlocal recovered
        for delay in (900, 1800, 3600, 3600):
            pool.current_index = 0
            assert await request(pool) == {"data": {}}
            assert calls[-2:] == [DEAD, LIVE]
            # Before expiry the actual outbound request must use the live pair.
            clock.now += delay - 1
            pool.current_index = 0
            assert await request(pool) == {"data": {}}
            assert calls[-1] == LIVE
            clock.now += 1
        recovered = True
        pool.current_index = 0
        assert await request(pool) == {"data": {}}
        assert calls[-1] == DEAD
        recovered = False
        pool.current_index = 0
        assert await request(pool) == {"data": {}}
        assert calls[-2:] == [DEAD, LIVE]
        clock.now += 899
        pool.current_index = 0
        await request(pool)
        assert calls[-1] == LIVE
        clock.now += 1
        pool.current_index = 0
        await request(pool)
        assert calls[-2:] == [DEAD, LIVE]

    event_loop.run_until_complete(run())
    output = capsys.readouterr().out
    pauses = [line for line in output.splitlines() if "🧊" in line]
    assert len(pauses) == 6
    assert [int(line.split("на паузе ")[1].split()[0]) for line in pauses] == [
        900, 1800, 3600, 3600, 900, 1800,
    ]
    assert output.count("✅ Прокси") == 1


def test_concurrent_failures_are_one_escalation(monkeypatch, clock, capsys, event_loop):
    pool = mr.ProxyAPIPool([(DEAD, "dead"), (LIVE, "live")])
    barrier = threading.Barrier(7)
    calls = Counter()
    started = clock.now

    def post(url, proxy, **kwargs):
        calls[proxy] += 1
        if proxy == DEAD:
            if calls[DEAD] <= 7:
                barrier.wait(timeout=5)
            raise Timeout(CAPTURED_TIMEOUT, code=28)
        return {"data": {}}, {}

    install_http(monkeypatch, post)

    async def run():
        results = await asyncio.gather(*(request(pool) for _ in range(14)))
        assert results == [{"data": {}}] * 14
        assert calls[DEAD] == 7
        # The live pair's existing per-second throttle may advance fake time.
        clock.now = started + 899
        pool.current_index = 0
        await request(pool)
        assert calls[DEAD] == 7
        clock.now += 1
        pool.current_index = 0
        await request(pool)
        assert calls[DEAD] == 8

    event_loop.run_until_complete(run())
    output = capsys.readouterr().out
    assert output.count("🧊") == 2
    assert "на паузе 1800 c" in output


@pytest.mark.parametrize("error", [
    RuntimeError("HTTP 500: upstream is unavailable"),
    Timeout("Read timed out", code=28, response=SimpleNamespace(status_code=200)),
    InvalidURL("Invalid URL", code=3),
])
def test_http_response_does_not_pause_proxy(monkeypatch, clock, capsys, error, event_loop):
    pool = mr.ProxyAPIPool([(DEAD, "token")])
    calls = []

    def post(url, proxy, **kwargs):
        calls.append(proxy)
        if len(calls) == 1:
            raise error
        return {"data": {}}, {}

    install_http(monkeypatch, post)
    assert event_loop.run_until_complete(request(pool)) == {"data": {}}
    assert calls == [DEAD, DEAD]
    assert not clock.sleeps
    assert "🧊" not in capsys.readouterr().out


@pytest.mark.parametrize("error", [
    Timeout(CAPTURED_TIMEOUT, code=28, response=SimpleNamespace(status_code=0)),
    ConnectionError("Failed to connect: Connection refused", code=7),
    ProxyError("CONNECT tunnel failed, response 502", code=56),
])
def test_transport_errors_without_stratz_response_pause(monkeypatch, clock, error, event_loop):
    pool = mr.ProxyAPIPool([(DEAD, "dead"), (LIVE, "live")])
    calls = []

    def post(url, proxy, **kwargs):
        calls.append(proxy)
        if proxy == DEAD:
            raise error
        return {"data": {}}, {}

    install_http(monkeypatch, post)

    async def run():
        await request(pool)
        pool.current_index = 0
        await request(pool)

    event_loop.run_until_complete(run())
    assert calls == [DEAD, LIVE, LIVE]
    assert not clock.sleeps


def test_all_dead_waits_in_bounded_chunks_then_raises(monkeypatch, clock, event_loop):
    pool = mr.ProxyAPIPool([(DEAD, "token")])
    attempts = []

    def post(url, proxy, **kwargs):
        attempts.append(clock.now)
        raise Timeout(CAPTURED_TIMEOUT, code=28)

    install_http(monkeypatch, post)
    with pytest.raises(Timeout):
        event_loop.run_until_complete(request(pool))
    assert len(attempts) == 2
    assert attempts[1] - attempts[0] == 900
    assert clock.sleeps == [300, 300, 300]


def test_fallback_never_returns_still_paused_proxy(monkeypatch, clock, event_loop):
    pool = mr.ProxyAPIPool([(DEAD, "token")])
    attempts = []

    def post(url, proxy, **kwargs):
        attempts.append(clock.now)
        raise Timeout(CAPTURED_TIMEOUT, code=28)

    install_http(monkeypatch, post)

    async def run():
        # Build up the 3600s cap using real failed outbound probes.
        for _ in range(2):
            with pytest.raises(Timeout):
                await request(pool)
        with pytest.raises(Timeout):
            await request(pool)

    event_loop.run_until_complete(run())
    assert [b - a for a, b in zip(attempts, attempts[1:])] == [
        900, 1800, 3600, 3600, 3600,
    ]
    assert all(delay == 300 for delay in clock.sleeps)
    assert len(clock.sleeps) == 45


def test_healthy_round_robin_unchanged(monkeypatch, clock, event_loop):
    pairs = [("http://live%d" % i, "token%d" % i) for i in range(5)]
    pool = mr.ProxyAPIPool(pairs)
    calls = []

    def post(url, proxy, **kwargs):
        calls.append(proxy)
        return {"data": {}}, {}

    install_http(monkeypatch, post)

    async def run():
        for _ in range(100):
            await request(pool)
            clock.now += 1.1

    event_loop.run_until_complete(run())
    assert calls == [p for p, _ in pairs] * 20
    assert not clock.sleeps


def test_all_five_dead_probes_only_after_earliest_expiry(monkeypatch, clock, event_loop):
    pool = mr.ProxyAPIPool([("http://dead%d" % i, "token%d" % i) for i in range(5)])
    calls = []

    def post(url, proxy, **kwargs):
        calls.append((proxy, clock.now))
        raise Timeout(CAPTURED_TIMEOUT, code=28)

    install_http(monkeypatch, post)
    with pytest.raises(Timeout):
        event_loop.run_until_complete(request(pool))
    assert len(calls) == 6  # Existing len(pool) + 1 retry budget.
    assert len({proxy for proxy, _ in calls[:5]}) == 5
    assert calls[-1][1] - calls[0][1] == 900
    assert clock.sleeps == [300, 300, 300]


def test_late_old_failure_does_not_escalate_new_generation(monkeypatch, clock, capsys, event_loop):
    pool = mr.ProxyAPIPool([(DEAD, "dead"), (LIVE, "live")])
    entered = threading.Event()
    release = threading.Event()
    dead_calls = 0

    def post(url, proxy, **kwargs):
        nonlocal dead_calls
        if proxy == DEAD:
            dead_calls += 1
            if dead_calls == 1:
                entered.set()
                assert release.wait(timeout=5)
            if dead_calls <= 3:
                raise Timeout(CAPTURED_TIMEOUT, code=28)
        return {"data": {}}, {}

    install_http(monkeypatch, post)

    async def run():
        late = asyncio.create_task(request(pool))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            pool.current_index = 0
            await request(pool)  # First pause, while the old request is in flight.
            clock.now += 900
            pool.current_index = 0
            await request(pool)  # Failed probe: 1800s pause.
            clock.now += 1800
            # Existing error handling advances this to the recovered dead pair.
            pool.current_index = 1
            release.set()
            assert await late == {"data": {}}
        finally:
            release.set()
            if not late.done():
                await late

    event_loop.run_until_complete(run())
    assert dead_calls == 4
    assert not clock.sleeps
    output = capsys.readouterr().out
    assert output.count("🧊") == 2
    assert "на паузе 3600 c" not in output
    assert output.count("✅ Прокси") == 1


def test_http_default_connect_and_total_timeout(monkeypatch):
    seen = []

    def post(url, **kwargs):
        seen.append(kwargs)
        return SimpleNamespace(json=lambda: {"data": {}}, headers={})

    monkeypatch.setattr(mr.cf_requests, "post", post)
    assert mr.ProxyAPIPool._post_json_with_requests(URL, DEAD) == ({"data": {}}, {})
    # curl_cffi 0.13.0 utils.py:505-510 adds connect + read for TIMEOUT_MS.
    assert seen[0]["timeout"] == (15, 105)


@pytest.mark.parametrize("timeout", [9, (3, 17), None])
def test_explicit_timeout_wins(monkeypatch, timeout):
    seen = []

    def post(url, **kwargs):
        seen.append(kwargs)
        return SimpleNamespace(json=lambda: {"data": {}}, headers={})

    monkeypatch.setattr(mr.cf_requests, "post", post)
    mr.ProxyAPIPool._post_json_with_requests(URL, DEAD, timeout=timeout)
    assert seen[0]["timeout"] == timeout


def test_dead_proxy_does_not_turn_local_throttle_into_long_sleeps(monkeypatch, clock, capsys, event_loop):
    # Lead regression (01.10.2026): the 2026-09-26 serv1 sweep log already had
    # 47,389 "Все пары заблокированы" lines out of 71,430. A paused proxy must not
    # send every local-throttle wait of the live pairs through that 1 s sleep path
    # (one log line per waiting request per second for the whole sweep).
    pairs = [(DEAD, "dead-token")] + [
        ("http://live%d:63099" % i, "token%d" % i) for i in range(4)
    ]
    pool = mr.ProxyAPIPool(pairs)
    dead = pool.trackers[0]
    dead.proxy_failures = 1
    dead.proxy_blocked_until = clock.now + mr.PROXY_COOLDOWN_INITIAL
    for tracker in pool.trackers[1:]:
        tracker.requests_log["second"].extend([clock.now] * mr.RATE_LIMITS["second"])

    tracker = event_loop.run_until_complete(pool.get_available_tracker())

    assert tracker is not dead
    assert clock.sleeps and set(clock.sleeps) == {0.5}
    assert "Все пары заблокированы" not in capsys.readouterr().out
