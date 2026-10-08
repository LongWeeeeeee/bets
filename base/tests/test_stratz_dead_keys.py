"""Регрессия 07.10.2026: ключи Stratz с истёкшим JWT получали долю запросов и молча
обнуляли ночной добор (3 из 5 пар, «A bearer token is required», 0 карт из 779 команд).

Ответ Stratz снят на serv1 07.10.2026 18:46 UTC командой
  runtime/probe_pairs_20261007.py (POST https://api.stratz.com/graphql через пару 88.218.187.219:64703,
  токен exp=2026-10-06 20:49:51 UTC): HTTP 403, тело ниже.
"""
import asyncio
import base64
import json
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import maps_research as mr

URL = "https://api.stratz.com/graphql"
CAPTURED_403 = {"message": "A bearer token is required for a request. View more at https://stratz.com/api"}
NOW = 1_791_400_000.0  # 2026-10-07 UTC


def jwt(exp):
    def b64(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")
    return ".".join([b64({"alg": "HS256"}), b64({"SteamId": "1", "exp": exp}), "sig"])


EXPIRED = jwt(NOW - 3600)
VALID = jwt(NOW + 86400 * 100)


@pytest.fixture(autouse=True)
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)


@pytest.fixture
def clock(monkeypatch):
    state = SimpleNamespace(now=NOW, sleeps=[])
    real_sleep = asyncio.sleep

    async def sleep(delay):
        state.sleeps.append(delay)
        assert len(state.sleeps) <= 100, "busy-spin"
        state.now += delay
        await real_sleep(0)

    monkeypatch.setattr(mr.time, "time", lambda: state.now)
    monkeypatch.setattr(mr.asyncio, "sleep", sleep)
    return state


def install_http(monkeypatch, handler):
    monkeypatch.setattr(mr.ProxyAPIPool, "_post_json_with_requests", staticmethod(handler))


def ok(data=None):
    return {"data": data or {"teams": []}}, {}


def test_expired_jwt_pairs_never_enter_the_pool(clock):
    pairs = [("http://dead%d:1" % i, EXPIRED) for i in range(3)] + [
        ("http://live%d:1" % i, VALID + str(i)) for i in range(2)]
    pool = mr.ProxyAPIPool(pairs)
    assert [t.proxy_url for t in pool.trackers] == ["http://live0:1", "http://live1:1"]


def test_all_keys_expired_is_a_loud_error(clock):
    with pytest.raises(mr.StratzAuthError):
        mr.ProxyAPIPool([("http://dead:1", EXPIRED)])


def test_non_jwt_tokens_are_kept(clock):
    pool = mr.ProxyAPIPool([("http://a:1", "opaque-token")])
    assert len(pool.trackers) == 1


def test_revoked_key_is_dropped_and_request_served_by_live_pair(monkeypatch, clock, event_loop):
    pool = mr.ProxyAPIPool([("http://revoked:1", VALID + "a"), ("http://live:1", VALID + "b")])
    calls = Counter()

    def post(url, proxy, **kwargs):
        calls[proxy] += 1
        return (CAPTURED_403, {}) if proxy == "http://revoked:1" else ok()

    install_http(monkeypatch, post)
    for _ in range(6):
        assert "data" in event_loop.run_until_complete(pool.make_request(URL))
    assert calls["http://revoked:1"] == 1, "revoked pair must be tried once, then skipped"
    assert calls["http://live:1"] == 6


def test_all_keys_revoked_raises_instead_of_empty_answer(monkeypatch, clock, event_loop):
    pool = mr.ProxyAPIPool([("http://r1:1", VALID + "a"), ("http://r2:1", VALID + "b")])
    install_http(monkeypatch, lambda url, proxy, **kw: (CAPTURED_403, {}))
    with pytest.raises(mr.StratzAuthError):
        event_loop.run_until_complete(pool.make_request(URL))
    assert not clock.sleeps


def test_retry_wrapper_does_not_loop_on_auth_error(monkeypatch, clock, event_loop):
    monkeypatch.setattr(mr, "proxy_pool", mr.ProxyAPIPool([("http://r:1", VALID)]))
    install_http(monkeypatch, lambda url, proxy, **kw: (CAPTURED_403, {}))

    async def req():
        return await mr.proxy_pool.make_request(URL)

    with pytest.raises(mr.StratzAuthError):
        event_loop.run_until_complete(mr.retry_request_with_proxy_rotation(req))
    assert not clock.sleeps, "auth errors must not sleep 300 s and retry forever"


def test_html_kong_403_is_also_an_auth_failure(monkeypatch, clock, event_loop):
    # Same 403 without "Accept: application/json": the gateway answers with an HTML page and
    # _post_json_with_requests raises RuntimeError("HTTP 403: <!doctype html> ... A bearer token is required ...").
    # Captured on serv1 07.10.2026 18:5x UTC (probe without the Accept header).
    html = ("HTTP 403: <!doctype html> <html> <head> <title>Kong Error</title> </head> <body> "
            "<h1>Kong Error</h1> <p>A bearer token is required for a request.</p>")
    pool = mr.ProxyAPIPool([("http://revoked:1", VALID + "a"), ("http://live:1", VALID + "b")])

    def post(url, proxy, **kwargs):
        if proxy == "http://revoked:1":
            raise RuntimeError(html)
        return ok()

    install_http(monkeypatch, post)
    for _ in range(4):
        assert "data" in event_loop.run_until_complete(pool.make_request(URL))
    assert pool.trackers[0].auth_dead and not pool.trackers[1].auth_dead
