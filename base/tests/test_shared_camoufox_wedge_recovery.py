"""Regression cases for a blocked shared Camoufox worker."""
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Isolated verification trees exclude the ignored production credentials
# (base/keys.py); same stub as base/tests/test_winline_dom_history.py.
try:
    import keys  # noqa: F401
except ImportError:
    import types

    test_keys = types.ModuleType("keys")
    test_keys.api_to_proxy = {}
    test_keys.BOOKMAKER_PROXY_URL = None
    test_keys.BOOKMAKER_PROXY_POOL = []
    test_keys.DLTV_PROXY_POOL = []
    sys.modules["keys"] = test_keys

import cyberscore_try as cs


class _Page:
    def __init__(self):
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


class _Browser:
    def __init__(self):
        self.close_calls = 0
        self.pages = []

    def new_page(self):
        page = _Page()
        self.pages.append(page)
        return page

    def close(self):
        self.close_calls += 1


class _Camoufox:
    def __init__(self):
        self.contexts = []

    def Camoufox(self, **_kwargs):
        context = self._Context()
        self.contexts.append(context)
        return context

    class _Context:
        def __init__(self):
            self.browser = _Browser()
            self.exit_calls = 0

        def __enter__(self):
            return self.browser

        def __exit__(self, *_args):
            self.exit_calls += 1


def _session(monkeypatch):
    factory = _Camoufox()
    monkeypatch.setattr(cs, "CAMOUFOX_AVAILABLE", True)
    monkeypatch.setattr(cs, "camoufox", factory)
    monkeypatch.setattr(cs, "_bookmaker_select_shared_camoufox_proxy_kwargs", lambda: {})
    monkeypatch.setattr(cs, "_note_proxy_success", lambda *_args: None)
    monkeypatch.setattr(cs, "_bookmaker_rotate_shared_camoufox_proxy", lambda **_kwargs: None)
    return cs._SharedCamoufoxSession(), factory


def _guarded(callback, timeout=8):
    """Keep even a broken submit/close from hanging pytest's main thread."""
    outcome = []

    def run():
        try:
            outcome.append((True, callback()))
        except BaseException as exc:
            outcome.append((False, exc))

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout)
    assert not thread.is_alive(), "blocked Camoufox regression exceeded hard guard"
    success, value = outcome[0]
    if not success:
        raise value
    return value


def test_overrun_abandons_old_worker_without_closing_dead_browser(monkeypatch):
    monkeypatch.setenv("CAMOUFOX_WEDGED_JOB_SECONDS", "0")
    session, factory = _session(monkeypatch)
    release = threading.Event()
    entered = threading.Event()
    old_threads = []

    def blocked(browser):
        old_threads.append(threading.current_thread())
        session.get_or_create_page("bookmaker:winline", browser)
        entered.set()
        while not release.is_set():
            pass

    try:
        with pytest.raises(FutureTimeoutError):
            _guarded(lambda: session.submit("blocked", blocked, timeout=0.5))
        assert entered.is_set()
        old_thread = old_threads[0]
        assert _guarded(lambda: session.submit("next", lambda _browser: "ok", timeout=5)) == "ok"
        old_thread.join(timeout=5)
        assert not old_thread.is_alive()
        assert len(factory.contexts) == 2
        abandoned = factory.contexts[0]
        assert abandoned.browser.pages[0].close_calls == 0
        assert abandoned.browser.close_calls == 0
        assert abandoned.exit_calls == 0
    finally:
        release.set()
        _guarded(session.close, timeout=12)


def test_dead_driver_triggers_fast_recovery_below_overrun_ceiling(monkeypatch):
    monkeypatch.setenv("CAMOUFOX_WEDGED_JOB_SECONDS", "999")
    monkeypatch.setenv("CAMOUFOX_DEAD_DRIVER_GRACE_SECONDS", "0")
    session, factory = _session(monkeypatch)
    release = threading.Event()
    entered = threading.Event()
    child = subprocess.Popen(["sleep", "60"])
    monkeypatch.setattr(session, "_driver_pid_for_browser", lambda _browser: child.pid)

    def blocked(_browser):
        entered.set()
        while not release.is_set():
            pass

    try:
        def first_timeout():
            failures = []

            def submit_blocked():
                try:
                    session.submit("driver-died", blocked, timeout=0.5)
                except BaseException as exc:
                    failures.append(exc)

            caller = threading.Thread(target=submit_blocked, daemon=True)
            caller.start()
            assert entered.wait(timeout=3)
            child.kill()
            child.wait(timeout=3)
            caller.join(timeout=3)
            assert not caller.is_alive()
            assert len(failures) == 1 and isinstance(failures[0], FutureTimeoutError)

        _guarded(first_timeout)
        assert _guarded(lambda: session.submit("next", lambda _browser: "ok", timeout=5)) == "ok"
        assert len(factory.contexts) == 2
    finally:
        release.set()
        if child.poll() is None:
            child.kill()
            child.wait(timeout=3)
        _guarded(session.close, timeout=12)


def test_healthy_slow_job_keeps_worker_and_browser(monkeypatch):
    monkeypatch.setenv("CAMOUFOX_WEDGED_JOB_SECONDS", "300")
    session, factory = _session(monkeypatch)
    monkeypatch.setattr(session, "_driver_pid_for_browser", lambda _browser: os.getpid())
    finished = threading.Event()

    def slow(_browser):
        time.sleep(1.5)
        finished.set()

    try:
        with pytest.raises(FutureTimeoutError):
            _guarded(lambda: session.submit("healthy-slow", slow, timeout=0.5))
        worker = session._thread
        generation = session._generation
        assert _guarded(lambda: session.submit("next", lambda _browser: "ok", timeout=5)) == "ok"
        assert finished.is_set()
        assert session._thread is worker
        assert session._generation == generation
        assert len(factory.contexts) == 1
    finally:
        _guarded(session.close, timeout=12)


def _dead_child():
    child = subprocess.Popen(["sleep", "60"])
    child.kill()
    child.wait(timeout=3)
    return child.pid


def _spinning_close(browser, entered, release):
    def close():
        browser.close_calls += 1
        entered.set()
        while not release.is_set():
            pass
    browser.close = close


def test_raised_callback_dead_driver_skips_playwright_close(monkeypatch):
    monkeypatch.setenv("CAMOUFOX_DEAD_DRIVER_GRACE_SECONDS", "0")
    session, factory = _session(monkeypatch)
    dead_pid = _dead_child()
    monkeypatch.setattr(session, "_driver_pid_for_browser", lambda _browser: dead_pid)
    entered, release = threading.Event(), threading.Event()
    try:
        old_browser = _guarded(lambda: session.submit("seed", lambda browser: browser, timeout=3))
        old_thread = session._thread
        old_page = _guarded(lambda: session.submit(
            "named-page", lambda browser: session.get_or_create_page("bookmaker:winline", browser), timeout=3))
        _spinning_close(old_browser, entered, release)

        def failed(_browser):
            raise Exception("Page.content: Connection closed while reading from the driver")

        with pytest.raises(Exception, match="Connection closed while reading from the driver"):
            _guarded(lambda: session.submit("content", failed, timeout=3))
        assert _guarded(lambda: session.submit(
            "fresh", lambda browser: (browser is not old_browser, "ok"), timeout=3)) == (True, "ok")
        assert session._thread is old_thread
        assert old_thread.is_alive()
        assert not entered.is_set()
        assert old_page.close_calls == 0
        assert old_browser.close_calls == 0
        assert factory.contexts[0].exit_calls == 0
    finally:
        release.set()
        _guarded(session.close, timeout=12)


def test_raised_callback_live_driver_close_ceiling_abandons(monkeypatch):
    monkeypatch.setenv("CAMOUFOX_WEDGED_JOB_SECONDS", "0")
    session, factory = _session(monkeypatch)
    monkeypatch.setattr(session, "_driver_pid_for_browser", lambda _browser: os.getpid())
    entered, release = threading.Event(), threading.Event()
    try:
        old_browser = _guarded(lambda: session.submit("seed", lambda browser: browser, timeout=3))
        old_thread = session._thread
        _spinning_close(old_browser, entered, release)

        def failed(_browser):
            raise Exception("Page.content: Connection closed while reading from the driver")

        with pytest.raises(Exception, match="Connection closed while reading from the driver"):
            _guarded(lambda: session.submit("content", failed, timeout=3))
        assert entered.wait(timeout=2)
        with pytest.raises(FutureTimeoutError):
            _guarded(lambda: session.submit("queued", lambda _browser: None, timeout=0.2))
        assert _guarded(lambda: session.submit(
            "fresh", lambda browser: (browser is not old_browser, "ok"), timeout=3)) == (True, "ok")
        old_thread.join(timeout=3)
        assert not old_thread.is_alive()
        assert len(factory.contexts) == 2
    finally:
        release.set()
        _guarded(session.close, timeout=12)


@pytest.mark.parametrize("reset_mode", ["periodic", "requested"])
def test_successful_job_reset_skips_dead_driver_close(monkeypatch, reset_mode):
    monkeypatch.setenv("CAMOUFOX_RESET_AFTER_JOBS", "1" if reset_mode == "periodic" else "60")
    session, factory = _session(monkeypatch)
    dead_pid = _dead_child()
    monkeypatch.setattr(session, "_driver_pid_for_browser", lambda _browser: dead_pid)
    entered, release = threading.Event(), threading.Event()
    def spinning_close(browser):
        browser.close_calls += 1
        entered.set()
        while not release.is_set():
            pass
    monkeypatch.setattr(_Browser, "close", spinning_close)
    try:
        old_browser = _guarded(lambda: session.submit("seed", lambda browser: browser, timeout=3))
        if reset_mode == "requested":
            session.request_reset()
        assert _guarded(lambda: session.submit(
            "fresh", lambda browser: (browser is not old_browser, "ok"), timeout=3)) == (True, "ok")
        assert not entered.is_set()
        assert old_browser.close_calls == 0
        assert factory.contexts[0].exit_calls == 0
    finally:
        release.set()
        _guarded(session.close, timeout=12)


@pytest.mark.skipif(os.getenv("CAMOUFOX_REAL_TESTS") != "1", reason="opt-in local browser test")
def test_real_driver_death_restarts_shared_browser(monkeypatch):
    from browserforge.fingerprints import Screen

    monkeypatch.setenv("CAMOUFOX_WEDGED_JOB_SECONDS", "999")
    monkeypatch.setenv("CAMOUFOX_DEAD_DRIVER_GRACE_SECONDS", "0")
    # Avoid screeninfo's macOS native monitor probe in a headless pytest thread.
    monkeypatch.setattr(cs, "_shared_camoufox_browser_options", lambda _proxy: {
        "headless": True,
        "screen": Screen(max_width=1920, max_height=1080),
    })
    monkeypatch.setattr(cs, "_bookmaker_select_shared_camoufox_proxy_kwargs", lambda: {})
    monkeypatch.setattr(cs, "_note_proxy_success", lambda *_args: None)
    monkeypatch.setattr(cs, "_bookmaker_rotate_shared_camoufox_proxy", lambda **_kwargs: None)
    if not cs.CAMOUFOX_AVAILABLE:
        pytest.skip("local Camoufox wrapper unavailable")
    session = cs._SharedCamoufoxSession()
    def seed(browser):
        page = browser.new_page()
        page.goto("data:text/html,<p>x</p>")
        return browser, page

    try:
        try:
            first_browser, first_page = _guarded(lambda: session.submit("seed", seed, timeout=60), timeout=65)
        except Exception as exc:
            if "BrowserType.launch: Failed to launch" in str(exc):
                pytest.skip("local Camoufox launch unavailable: %s" % exc)
            raise
        pid = session._driver_pid
        assert pid and pid != os.getpid()
        os.kill(pid, 9)
        # The kernel reaps the driver asynchronously: poll instead of a single probe.
        deadline = time.monotonic() + 3.0
        while not session._driver_is_dead(pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert session._driver_is_dead(pid)
        old_thread = session._thread

        def content(browser):
            assert browser is first_browser
            return first_page.content()

        with pytest.raises(Exception, match="Connection closed while reading from the driver"):
            _guarded(lambda: session.submit("content-after-driver-kill", content, timeout=8), timeout=10)
        # Real Playwright leaves the dead driver's asyncio loop on this thread, so the
        # first job after the death may fail as "Sync API inside the asyncio loop";
        # the existing poisoned-thread branch then ends the worker and the next job
        # gets a fresh browser on a new worker thread. Anything else is a regression.
        fresh_ok = False
        for attempt in range(2):
            try:
                fresh_ok = _guarded(lambda: session.submit(
                    "fresh", lambda browser: browser is not first_browser, timeout=60), timeout=70)
                break
            except Exception as exc:
                if attempt or "asyncio loop" not in str(exc):
                    raise
        assert fresh_ok is True
        assert session._thread is not old_thread
        assert not old_thread.is_alive()
        print("real case A: page.content raised; fresh browser served by a new worker")
    finally:
        _guarded(session.close, timeout=12)
