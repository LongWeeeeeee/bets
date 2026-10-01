"""Winline proxy death: kef-bot alerts and a permanent halt when every proxy is dead.

Boundary under test: what reaches the Winline odds ("kef") Telegram bot, and
whether a Winline job / browser launch still happens.  Only egress is mocked:
the probe's HTTP call (``requests.get``) and ``send_winline_odds_message``.  The
shared Camoufox session is the real ``_SharedCamoufoxSession`` with a recording
fake Camoufox.
"""
import sys
import threading
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
import requests

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Never import the credential-bearing production keys module.
if "keys" not in sys.modules:
    keys = ModuleType("keys")
    keys.api_to_proxy = {}
    keys.BOOKMAKER_PROXY_URL = ""
    keys.BOOKMAKER_PROXY_POOL = []
    keys.DLTV_PROXY_POOL = []
    keys.Token = "0:STUB_MAIN_BOT"  # functions.send_message reads keys.Token directly
    sys.modules["keys"] = keys

import cyberscore_try as cs

HOSTS = ["142.252.86.%s" % n for n in range(1, 6)]
PROBE_URLS = "https://probe-a.invalid/generate_204,http://probe-b.invalid/generate_204"


class RecordingCamoufox:
    def __init__(self):
        self.options = []

    def Camoufox(self, **options):
        self.options.append(options)

        class Context:
            def __enter__(self):
                return SimpleNamespace(close=lambda: None)

            def __exit__(self, *_args):
                return False

        return Context()


class FakeEgress:
    """Stands in for ``requests.get``: decides per proxy host whether it answers."""

    def __init__(self):
        self.dead_hosts = set()
        self.fail_budget = {}  # host -> number of calls that still fail
        self.calls = []  # proxy hosts, in call order
        self.gate = None  # optional Event: every call waits for it
        self.offline = False  # the server itself has no network: every call fails
        self.status_by_host = {}  # host -> HTTP status the proxy answers with
        self._lock = threading.Lock()

    def get(self, url, proxies=None, timeout=None, **_kwargs):
        proxy = (proxies or {}).get("https") or (proxies or {}).get("http") or ""
        host = proxy.rsplit("@", 1)[-1].split(":")[0]  # "" = direct (no proxy)
        with self._lock:
            self.calls.append(host)
            if self.offline:
                raise requests.exceptions.ConnectionError("Network is unreachable")
            budget = self.fail_budget.get(host, 0)
            if budget > 0:
                self.fail_budget[host] = budget - 1
                raise requests.exceptions.ConnectTimeout("blip")
        if self.gate is not None and host:  # only proxied probes hang
            self.gate.wait(10)
        if host in self.dead_hosts:
            raise requests.exceptions.ProxyError("Cannot connect to proxy")
        return SimpleNamespace(status_code=self.status_by_host.get(host, 204))

    def calls_for(self, host):
        with self._lock:
            return sum(1 for h in self.calls if h == host)

    def proxy_calls(self):
        with self._lock:
            return [h for h in self.calls if h]


@pytest.fixture
def world(monkeypatch):
    http_pool = ["http://u:p@%s:8000" % h for h in HOSTS]
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", http_pool)
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [])
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", http_pool[0])
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_candidates", [])
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_index", 0)
    monkeypatch.setattr(cs, "_cyberscore_camoufox_proxy_kwargs", lambda: {})
    monkeypatch.setattr(cs, "_note_proxy_success", lambda *_a: None)
    monkeypatch.setattr(cs, "CAMOUFOX_AVAILABLE", True)
    # Process-wide state of the feature (raising=False: absent before the change).
    monkeypatch.setattr(cs, "_winline_dead_proxy_urls", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_parsing_halted", False, raising=False)
    monkeypatch.setattr(cs, "_winline_parsing_halted_at", None, raising=False)
    monkeypatch.setattr(cs, "_winline_parsing_halted_reason", "", raising=False)
    monkeypatch.setattr(cs, "_winline_proxy_health_inflight", False, raising=False)
    monkeypatch.setattr(cs, "_winline_proxy_health_last_started", None, raising=False)
    monkeypatch.setattr(cs, "_winline_proxy_health_thread", None, raising=False)
    monkeypatch.setattr(cs, "_winline_halt_refusal_logged_at", None, raising=False)
    monkeypatch.setattr(cs, "WINLINE_PROXY_HEALTHCHECK_MIN_INTERVAL_S", 0.0, raising=False)
    monkeypatch.setattr(cs, "WINLINE_PROXY_PROBE_RECHECK_DELAY_S", 0.0, raising=False)
    monkeypatch.setenv("WINLINE_PROXY_PROBE_URLS", PROBE_URLS)
    monkeypatch.setenv("CYBERSCORE_CAMOUFOX_BLOCK_WEBRTC", "0")
    monkeypatch.setenv("CYBERSCORE_CAMOUFOX_GEOIP", "1")
    monkeypatch.setenv("CAMOUFOX_RESET_AFTER_JOBS", "1000")
    monkeypatch.setenv("CAMOUFOX_RESET_AFTER_SECONDS", "3600")

    egress = FakeEgress()
    monkeypatch.setattr(cs.requests, "get", egress.get)

    sent = []

    def fake_send(message, **_kwargs):
        sent.append(message)
        return True

    monkeypatch.setattr(cs, "send_winline_odds_message", fake_send)

    factory = RecordingCamoufox()
    monkeypatch.setattr(cs, "camoufox", factory)
    session = cs._SharedCamoufoxSession()
    monkeypatch.setattr(cs, "_shared_camoufox_session", session)

    resets = []
    real_reset = session.request_reset

    def counting_reset():
        resets.append(time.monotonic())
        return real_reset()

    monkeypatch.setattr(session, "request_reset", counting_reset)

    ns = SimpleNamespace(session=session, factory=factory, egress=egress, sent=sent, resets=resets)

    def drain():
        thread = getattr(cs, "_winline_proxy_health_thread", None)
        if thread is not None:
            thread.join(10)
            assert not thread.is_alive(), "health check thread did not finish"

    ns.drain = drain
    yield ns
    egress.gate and egress.gate.set()
    drain()
    session.close()


def _servers(factory):
    return [o.get("proxy", {}).get("server") for o in factory.options]


def _winline_job(session, label="winline_current_map_poll:x"):
    seen = []
    session.submit(label, lambda browser: seen.append(browser), timeout=3)
    return seen


def test_one_dead_proxy_sends_one_message_and_is_never_chosen_again(world):
    w = world
    w.egress.dead_hosts = {HOSTS[0]}
    assert len(_winline_job(w.session)) == 1
    assert _servers(w.factory) == ["http://%s:8000" % HOSTS[0]]  # launched on proxy 1

    cs._bookmaker_rotate_shared_camoufox_proxy(reason="winline_acquisition_error")
    w.drain()

    assert len(w.sent) == 1, w.sent
    message = w.sent[0]
    assert message.startswith("\U0001F534 Winline: прокси ")
    assert "http://%s:8000 (US)" % HOSTS[0] in message
    assert "не отвечает (ProxyError)" in message
    assert "исключён, работаю через http://" in message
    assert "u:p" not in message and "@" not in message  # no credentials
    assert cs._winline_parsing_halted is False

    # Many more rotations/launches: the dead proxy is never selected again,
    # nothing more is announced, the dead one is not probed again.
    probes_of_dead = w.egress.calls_for(HOSTS[0])
    for _ in range(12):
        _winline_job(w.session)
        cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
        w.drain()
    assert "http://%s:8000" % HOSTS[0] not in _servers(w.factory)[1:]
    assert len(set(_servers(w.factory)[1:])) == 4  # all four live proxies get used
    assert len(w.sent) == 1
    assert w.egress.calls_for(HOSTS[0]) == probes_of_dead
    assert cs._winline_parsing_halted is False


def test_dead_proxy_at_current_index_is_skipped_at_launch(world):
    w = world
    w.egress.dead_hosts = {HOSTS[1]}
    _winline_job(w.session)
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")  # index -> proxy 2
    w.drain()
    # proxy 2 is dead; the next launch must not land on it.
    _winline_job(w.session)
    assert _servers(w.factory)[-1] != "http://%s:8000" % HOSTS[1]
    assert len(w.sent) == 1


def test_all_proxies_dead_halts_winline_and_refuses_before_any_launch(world):
    w = world
    w.egress.dead_hosts = set(HOSTS)
    _winline_job(w.session)  # browser up on proxy 1
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="winline_acquisition_error")
    w.drain()

    halt_messages = [m for m in w.sent if "все прокси мертвы" in m]
    assert len(halt_messages) == 1, w.sent
    assert halt_messages[0] == (
        "⛔ Winline: все прокси мертвы (5 из 5) — парсинг Winline остановлен. "
        "Возобновление только перезапуском службы."
    )
    assert len(w.sent) == 6  # one per dead proxy + one halt
    assert all("u:p" not in m and "@" not in m for m in w.sent)
    assert cs._winline_parsing_halted is True
    assert cs._winline_parsing_halted_at is not None

    launches_before = len(w.factory.options)
    resets_before = len(w.resets)
    probes_before = len(w.egress.calls)
    index_before = cs._bookmaker_shared_proxy_index
    callback_calls = []

    for label in ("winline_current_map_poll:x", "WINLINE-overview", "winline-shadow", "bookmaker-prefetch"):
        with pytest.raises(RuntimeError) as outer:
            cs._run_shared_camoufox_job(label, lambda b: callback_calls.append(b), timeout=3)
        assert type(outer.value).__name__ == "WinlineParsingHalted"
        # Defence in depth: the worker itself refuses too.
        with pytest.raises(RuntimeError) as inner:
            w.session.submit(label, lambda b: callback_calls.append(b), timeout=3)
        assert type(inner.value).__name__ == "WinlineParsingHalted"
    assert callback_calls == []
    assert len(w.factory.options) == launches_before  # no browser launch for Winline

    for _ in range(5):
        cs._bookmaker_rotate_shared_camoufox_proxy(reason="after_halt")
    w.drain()
    assert len(w.resets) == resets_before
    assert len(w.egress.calls) == probes_before
    assert cs._bookmaker_shared_proxy_index == index_before
    assert len(w.sent) == 6  # nothing new

    # Non-Winline work keeps running, on the fallback route (never a dead proxy).
    assert cs._run_shared_camoufox_job("dota2protracker:x", lambda b: "ok", timeout=3) == "ok"
    assert w.factory.options[-1].get("proxy") is None
    dead_servers = {"http://%s:8000" % h for h in HOSTS}
    assert not (set(_servers(w.factory)[launches_before:]) & dead_servers)


def test_blip_is_rechecked_and_does_not_mark_proxy_dead(world):
    w = world
    w.egress.fail_budget = {HOSTS[0]: 2}  # both probe targets fail once, recheck answers
    _winline_job(w.session)
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    w.drain()
    assert w.egress.calls_for(HOSTS[0]) == 3  # 2 failed targets + 1 answered recheck
    assert w.sent == []
    assert not cs._winline_dead_proxy_urls
    assert cs._winline_parsing_halted is False


def test_rotation_does_not_block_on_a_slow_probe(world):
    w = world
    w.egress.gate = threading.Event()  # every probe call hangs until released
    _winline_job(w.session)
    t0 = time.monotonic()
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    assert time.monotonic() - t0 < 1.0
    thread = cs._winline_proxy_health_thread
    assert thread is not None and thread.is_alive()
    # The shared worker keeps serving other jobs while the probe hangs.
    assert w.session.submit("dota2protracker:x", lambda b: "ok", timeout=3) == "ok"
    w.egress.gate.set()
    w.drain()
    assert w.sent == []


def test_health_check_is_single_flight(world):
    w = world
    w.egress.gate = threading.Event()
    _winline_job(w.session)
    for _ in range(4):
        cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    w.egress.gate.set()
    w.drain()
    assert len(w.egress.proxy_calls()) == len(HOSTS)  # one round, one answering target each


def test_health_check_is_rate_limited(world, monkeypatch):
    w = world
    monkeypatch.setattr(cs, "WINLINE_PROXY_HEALTHCHECK_MIN_INTERVAL_S", 60.0, raising=False)
    _winline_job(w.session)
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    w.drain()
    first_round = len(w.egress.proxy_calls())
    assert first_round == len(HOSTS)
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    w.drain()
    assert len(w.egress.proxy_calls()) == first_round


def test_halt_is_permanent_and_messages_are_not_repeated(world):
    w = world
    w.egress.dead_hosts = set(HOSTS)
    _winline_job(w.session)
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    w.drain()
    assert cs._winline_parsing_halted is True
    sent_after_halt = list(w.sent)

    # The proxies "recover": no auto resume, no re-probe, no new message.
    w.egress.dead_hosts = set()
    calls_before = len(w.egress.calls)
    cs._winline_proxy_health_check_async("manual")
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    w.drain()
    assert cs._winline_parsing_halted is True
    assert len(w.egress.calls) == calls_before
    assert w.sent == sent_after_halt
    with pytest.raises(RuntimeError) as error:
        cs._run_shared_camoufox_job("winline-overview", lambda b: "ok", timeout=3)
    assert type(error.value).__name__ == "WinlineParsingHalted"


def test_kef_bot_failure_never_breaks_the_health_check(world, monkeypatch):
    w = world
    w.egress.dead_hosts = set(HOSTS)
    attempts = []

    def broken_send(message, **_kwargs):
        attempts.append(message)
        raise RuntimeError("telegram down")

    monkeypatch.setattr(cs, "send_winline_odds_message", broken_send)
    _winline_job(w.session)
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    w.drain()
    assert attempts  # tried
    assert cs._winline_parsing_halted is True  # halt does not depend on delivery


def test_empty_pool_at_startup_is_the_existing_refusal_without_halt(world, monkeypatch):
    w = world
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", [])
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "")
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="winline_acquisition_error")
    w.drain()
    assert w.sent == []
    assert w.egress.calls == []
    assert cs._winline_parsing_halted is False
    with pytest.raises(RuntimeError) as error:
        w.session.submit("winline-overview", lambda b: "ok", timeout=3)
    assert type(error.value).__name__ == "WinlineDirectRouteRefused"


def test_server_offline_marks_nothing_and_never_halts(world):
    """serv1 loses its own network: every proxy probe would fail, but the direct
    control request fails too, so nothing is marked dead and nothing is sent."""
    w = world
    _winline_job(w.session)
    w.egress.offline = True
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="winline_acquisition_error")
    w.drain()
    assert w.sent == []
    assert not cs._winline_dead_proxy_urls
    assert cs._winline_parsing_halted is False
    assert all(h == "" for h in w.egress.calls)  # only direct controls, no proxy probed
    # Network back: Winline keeps working on a proxy.
    w.egress.offline = False
    assert len(_winline_job(w.session)) == 1


def test_outage_during_the_probe_round_is_rechecked_not_halted(world):
    """Every proxy fails the whole first probe (both targets, both rounds) while
    the direct control passes: the failures are re-probed before anything dies."""
    w = world
    w.egress.fail_budget = {h: 4 for h in HOSTS}
    _winline_job(w.session)
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="winline_acquisition_error")
    w.drain()
    assert w.sent == []
    assert not cs._winline_dead_proxy_urls
    assert cs._winline_parsing_halted is False
    assert all(w.egress.calls_for(h) == 5 for h in HOSTS)  # 4 failed + 1 answered recheck


@pytest.mark.parametrize("status", [407, 502])
def test_proxy_rejecting_auth_or_failing_upstream_is_dead(world, status):
    w = world
    w.egress.status_by_host = {HOSTS[0]: status}
    _winline_job(w.session)
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="winline_acquisition_error")
    w.drain()
    assert len(w.sent) == 1, w.sent
    assert "http://%s:8000 (US)" % HOSTS[0] in w.sent[0]
    assert "не отвечает (HTTP%d)" % status in w.sent[0]
    assert cs._winline_parsing_halted is False
