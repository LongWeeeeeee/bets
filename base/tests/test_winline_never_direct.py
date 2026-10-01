"""Winline egress contract at the shared worker's actual launch/callback boundary."""
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

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
import bookmaker_selenium_odds as odds


class RecordingCamoufox:
    def __init__(self, launch_error=None):
        self.options = []
        self.launch_error = launch_error

    def Camoufox(self, **options):
        self.options.append(options)
        factory = self

        class Context:
            def __enter__(self):
                if factory.launch_error is not None:
                    error, factory.launch_error = factory.launch_error, None
                    raise error
                return SimpleNamespace(close=lambda: None)

            def __exit__(self, *_args):
                return False

        return Context()


@pytest.fixture
def route(monkeypatch):
    http_pool = ["http://u:p@142.252.86.%s:8000" % n for n in range(1, 6)]
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", http_pool)
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [u.replace("http:", "socks5:").replace(":8000", ":9000") for u in http_pool])
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", http_pool[0])
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_candidates", [])
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_index", 0)
    monkeypatch.setattr(cs, "_cyberscore_camoufox_proxy_kwargs", lambda: {})
    monkeypatch.setattr(cs, "_note_proxy_success", lambda *_a: None)
    monkeypatch.setattr(cs, "CAMOUFOX_AVAILABLE", True)
    # Rotation spawns a background proxy liveness probe; keep it off the network
    # here (it is covered, with a mocked transport, in test_winline_proxy_death_halt.py).
    monkeypatch.setattr(cs, "_winline_proxy_health_check_async", lambda *_a, **_k: False)
    monkeypatch.setenv("CYBERSCORE_CAMOUFOX_BLOCK_WEBRTC", "0")
    monkeypatch.setenv("CYBERSCORE_CAMOUFOX_GEOIP", "1")
    monkeypatch.setenv("CAMOUFOX_RESET_AFTER_JOBS", "1000")
    monkeypatch.setenv("CAMOUFOX_RESET_AFTER_SECONDS", "3600")
    factory = RecordingCamoufox()
    monkeypatch.setattr(cs, "camoufox", factory)
    session = cs._SharedCamoufoxSession()
    monkeypatch.setattr(cs, "_shared_camoufox_session", session)
    yield session, factory
    session.close()


def test_serv1_pool_launches_and_rotates_only_http_proxies(route):
    session, factory = route
    seen = []
    for _ in range(8):  # all five candidates, then wrap around
        session.submit("winline_current_map_poll:x", lambda browser: seen.append(browser), timeout=3)
        cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    assert len(seen) == 8
    assert len(factory.options) == 8
    servers = [o["proxy"]["server"] for o in factory.options]
    assert set(servers) == {"http://142.252.86.%s:8000" % n for n in range(1, 6)}
    assert servers[5:] == servers[:3]
    assert all(o["block_webrtc"] is True for o in factory.options)


def test_valid_page_keeps_selected_proxy_without_reset(route, monkeypatch):
    session, _ = route
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_candidates", [
        {"url": "http://u:p@154.195.1.1:8000", "country": "DE"},
        {"url": "http://u:p@154.195.1.2:8000", "country": "DE"},
    ])
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_index", 1)
    index = cs._bookmaker_shared_proxy_index
    reset = Mock()
    monkeypatch.setattr(session, "request_reset", reset)
    assert cs._bookmaker_restore_shared_camoufox_direct_route(reason="valid_page") is False
    assert cs._bookmaker_shared_proxy_index == index
    reset.assert_not_called()


@pytest.mark.parametrize("pool", [[], ["http://u:p@203.0.113.1:8000"]], ids=["empty", "unknown"])
def test_unusable_pool_refuses_winline_without_reset_or_rotation(route, monkeypatch, capsys, pool):
    session, factory = route
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", pool)
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [])
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "")
    rotate, reset, callback = Mock(), Mock(), Mock()
    monkeypatch.setattr(cs, "_bookmaker_rotate_shared_camoufox_proxy", rotate)
    monkeypatch.setattr(session, "request_reset", reset)
    # Cover startup refusal and a direct browser already used by ProTracker.
    if pool:
        assert session.submit("dota2protracker:x", lambda _: "ok", timeout=3) == "ok"
    for label in ("winline_current_map_poll:x", "WINLINE-overview", "winline-shadow", "bookmaker-prefetch"):
        with pytest.raises(RuntimeError, match="Winline") as error:
            session.submit(label, callback, timeout=3)
        assert type(error.value).__name__ == "WinlineDirectRouteRefused"
    assert session.submit("dota2protracker:x", lambda _: "still ok", timeout=3) == "still ok"
    callback.assert_not_called()
    reset.assert_not_called()
    rotate.assert_not_called()
    assert len(factory.options) == 1
    assert "proxy" not in factory.options[0]
    assert capsys.readouterr().out.count("прямой выход с IP сервера запрещён") == 1


def test_rotation_without_any_winline_proxy_does_not_reset_browser(route, monkeypatch):
    """Poll recovery (error streak -> rotate) must not relaunch the ProTracker
    browser on refused Winline polls when there is no proxy to rotate to."""
    session, _ = route
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", [])
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [])
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "")
    reset = Mock()
    monkeypatch.setattr(session, "request_reset", reset)
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="winline_acquisition_error")
    reset.assert_not_called()


def test_rotation_with_proxies_still_relaunches_on_next_proxy(route, monkeypatch):
    """Control: with real candidates a rotation still resets the browser."""
    session, _ = route
    reset = Mock()
    monkeypatch.setattr(session, "request_reset", reset)
    cs._bookmaker_rotate_shared_camoufox_proxy(reason="winline_acquisition_error")
    reset.assert_called_once()
    assert cs._bookmaker_shared_proxy_index == 1


def test_empty_selector_raises_instead_of_returning_direct(route, monkeypatch):
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", [])
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [])
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "")
    with pytest.raises(RuntimeError, match="Winline") as error:
        cs._bookmaker_select_shared_camoufox_proxy_kwargs()
    assert type(error.value).__name__ == "WinlineProxyUnavailable"


def test_country_limits_order_and_dedup_at_launch(route, monkeypatch):
    session, factory = route
    de = ["http://u:p@154.195.1.%s:8000" % n for n in range(1, 8)]
    us = ["http://u:p@142.252.86.%s:8000" % n for n in range(1, 8)]
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", us + de)
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", de + us)
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", de[0])
    for _ in range(12):
        session.submit("winline-overview", lambda _: "ok", timeout=3)
        cs._bookmaker_rotate_shared_camoufox_proxy(reason="test")
    servers = [o["proxy"]["server"] for o in factory.options]
    expected = [u.replace("u:p@", "") for u in de[:5] + us[:5]]
    assert servers == expected + expected[:2]


def test_outer_job_retry_does_not_reset_on_winline_refusal(route, monkeypatch):
    session, factory = route
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", [])
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [])
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "")
    # Even a CyberScore proxy is not proof of an eligible Winline route.
    monkeypatch.setattr(cs, "_cyberscore_camoufox_proxy_kwargs", lambda: {
        "proxy": {"server": "http://203.0.113.1:8000"},
    })
    reset, rotate, callback = Mock(), Mock(), Mock()
    monkeypatch.setattr(session, "request_reset", reset)
    monkeypatch.setattr(cs, "_bookmaker_rotate_shared_camoufox_proxy", rotate)
    with pytest.raises(cs.WinlineDirectRouteRefused):
        cs._run_shared_camoufox_job("bookmaker-prefetch", callback, timeout=3)
    callback.assert_not_called()
    reset.assert_not_called()
    rotate.assert_not_called()
    assert len(factory.options) == 1
    assert session.submit("dota2protracker:x", lambda _: "ok", timeout=3) == "ok"


def test_malformed_and_socks_candidates_are_skipped_at_launch(route, monkeypatch, capsys):
    session, factory = route
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", [
        "http://142.252.86.10:8000",  # no credentials
        "http://secret:credential@142.252.86.11:bad",  # invalid port
        "socks5://u:p@142.252.86.12:9000",
        "https://u:p@154.195.1.1:8000",
    ])
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "")
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [])
    session.submit("bookmaker-prefetch", lambda _: "ok", timeout=3)
    assert factory.options[0]["proxy"]["server"] == "http://154.195.1.1:8000"
    assert "secret" not in capsys.readouterr().out


@pytest.mark.parametrize("error", [RuntimeError("geoip unavailable"), ValueError("No headers based on this input")])
def test_launch_fallback_preserves_proxy_and_webrtc_block(route, error):
    session, factory = route
    factory.launch_error = error
    assert session.submit("winline-overview", lambda _: "ok", timeout=3) == "ok"
    assert len(factory.options) == 2
    assert all(o["proxy"]["server"] == "http://142.252.86.1:8000" for o in factory.options)
    assert all(o["block_webrtc"] is True for o in factory.options)


def test_bookmaker_cli_empty_proxy_refuses_direct():
    with pytest.raises(RuntimeError):
        odds._camoufox_proxy_kwargs("")
