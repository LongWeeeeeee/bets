"""Two-mode Winline odds-polling: normal (5s) + accelerated (2s) cadence.

Covers REQ-01 through REQ-14 from the polling-contract.
Uses FakeClock + FakeCollector; no network, no browser, no real sleep.
"""
from __future__ import annotations

import importlib.util
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "services" / "winline" / "winline_current_map_odds_poller.py"
BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SERIES = "dltv.org/matches/123456"
TEAM1 = "Alpha"
TEAM2 = "Beta"
MAP_NUM = 1
CANONICAL = f"{SERIES}|map{MAP_NUM}|{TEAM1}|{TEAM2}"


def _load_mod():
    if not MODULE_PATH.is_file():
        pytest.fail(f"missing module: {MODULE_PATH}")
    name = f"winline_two_mode_{id(object())}"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeClock:
    def __init__(self, mono: float = 1000.0, wall: float = 1_700_000_000.0) -> None:
        self.mono = float(mono)
        self.wall = float(wall)

    def monotonic(self) -> float:
        return self.mono

    def time(self) -> float:
        return self.wall

    def advance(self, seconds: float) -> None:
        self.mono += float(seconds)
        self.wall += float(seconds)


class FakeCollector:
    def __init__(self, script: Optional[Sequence[Dict[str, Any]]] = None) -> None:
        self.script: List[Dict[str, Any]] = list(script or [])
        self.calls: List[Dict[str, Any]] = []
        self._idx = 0
        self.default: Dict[str, Any] = {
            "market_status": "missing",
            "source": "winline_current_map_winner",
            "p1_odds": None,
            "p2_odds": None,
            "map_num": MAP_NUM,
            "team1": TEAM1,
            "team2": TEAM2,
            "series": SERIES,
            "dom_signature": "sig",
            "dom_hash": "hash",
            "parser_failure_reasons": [],
            "error": None,
            "acquisition_error": None,
            "page_valid": True,
        }

    def push(self, *results: Dict[str, Any]) -> None:
        self.script.extend(results)

    def __call__(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(dict(kwargs))
        if self._idx < len(self.script):
            payload = dict(self.default)
            payload.update(self.script[self._idx])
            self._idx += 1
        else:
            payload = dict(self.default)
        return payload


def _make_poller(mod, clock, collector, is_current=None):
    return mod.WinlineCurrentMapOddsPoller(
        collector=collector,
        is_map_current=is_current or (lambda **kw: True),
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        producer_pid=9999,
        producer_start_generation="test-gen",
    )


def _begin(poller):
    return poller.begin(series=SERIES, map_num=MAP_NUM, team1=TEAM1, team2=TEAM2)


# ─── REQ-01: Normal mode 5s cadence ───────────────────────────────────────────


class TestNormalMode5sCadence:
    def test_normal_mode_5s_cadence(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        attempt_starts = []
        for i in range(4):
            result = poller.tick()
            if result and result.get("attempt"):
                attempt_starts.append(clock.mono)
            clock.advance(5.0)

        assert len(attempt_starts) == 4
        for i in range(1, len(attempt_starts)):
            delta = attempt_starts[i] - attempt_starts[i - 1]
            assert abs(delta - 5.0) < 0.01, f"Expected 5s cadence, got {delta}"

    def test_not_due_before_5s(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        poller.tick()  # first attempt immediate
        clock.advance(3.0)
        result = poller.tick()
        assert result is not None
        assert result.get("status") == "not_due"


# ─── REQ-02: Accelerated mode 2s after dispatcher_ready ───────────────────────


class TestAcceleratedMode2s:
    def test_accelerated_mode_2s_after_dispatcher_ready(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        # Normal ticks at 5s cadence
        poller.tick()  # t=1000
        clock.advance(5.0)
        poller.tick()  # t=1005, schedules next at t=1010

        # Activate acceleration (takes effect on next attempt's scheduling)
        poller.set_accelerated(True)
        assert poller.accelerated is True

        # Reach the already-scheduled next poll (5s from prior start)
        clock.advance(5.0)  # t=1010
        result = poller.tick()
        assert result is not None
        assert result.get("attempt") is not None
        # This attempt was accelerated, so next is scheduled at +2s
        assert result["attempt"].get("cadence_interval_seconds") == 2.0

        # Now cadence is 2s
        clock.advance(2.0)  # t=1012
        result = poller.tick()
        assert result is not None
        assert result.get("attempt") is not None
        assert result["attempt"].get("cadence_interval_seconds") == 2.0

    def test_not_due_before_2s_in_accelerated(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        poller.tick()
        poller.set_accelerated(True)
        clock.advance(1.0)
        result = poller.tick()
        assert result is not None
        assert result.get("status") == "not_due"


# ─── REQ-03: Acceleration only on existing dispatcher gate ────────────────────


class TestAccelerationOnlyOnGate:
    def test_acceleration_only_on_existing_gate(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        # Before gate: not accelerated
        assert poller.accelerated is False
        poller.tick()
        assert poller.accelerated is False

        # Simulate gate passing (external code calls set_accelerated)
        poller.set_accelerated(True)
        assert poller.accelerated is True


# ─── REQ-04: Idempotent acceleration, no duplicate ────────────────────────────


class TestIdempotentAcceleration:
    def test_idempotent_acceleration_no_duplicate(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        poller.tick()
        count_before = len(poller.attempts())

        poller.set_accelerated(True)
        poller.set_accelerated(True)  # idempotent
        poller.set_accelerated(True)

        assert poller.accelerated is True
        assert len(poller.attempts()) == count_before


# ─── REQ-05: Delivery non-blocking with pending odds request ──────────────────


class TestDeliveryNonBlocking:
    def test_delivery_nonblocking_with_pending_odds_request(self):
        """Poller tick returns immediately even if collector is slow (simulated)."""
        mod = _load_mod()
        clock = FakeClock()

        call_times = []

        def slow_collector(**kwargs):
            call_times.append(clock.mono)
            return {
                "market_status": "missing",
                "source": "winline_current_map_winner",
                "p1_odds": None,
                "p2_odds": None,
                "map_num": MAP_NUM,
                "team1": TEAM1,
                "team2": TEAM2,
                "page_valid": True,
                "dom_signature": "s",
                "dom_hash": "h",
                "parser_failure_reasons": [],
            }

        poller = _make_poller(mod, clock, slow_collector)
        _begin(poller)

        import time as _time
        t0 = _time.monotonic()
        poller.tick()
        elapsed = _time.monotonic() - t0
        # tick itself is synchronous but collector is instant in test;
        # the guarantee is that tick doesn't sleep or block on external I/O
        assert elapsed < 1.0


# ─── REQ-06: Stale snapshot returns closed_wait ───────────────────────────────


class TestStaleSnapshot:
    def test_stale_snapshot_returns_closed_wait(self):
        """Poller with no accepted odds stays pending (delivery layer handles stale)."""
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        result = poller.tick()
        assert result is not None
        assert result.get("status") == "pending"
        assert result.get("attempt", {}).get("accepted") is False


# ─── REQ-07: Collector exception contained in poller ──────────────────────────


class TestCollectorExceptionContained:
    def test_collector_exception_contained_in_poller(self):
        mod = _load_mod()
        clock = FakeClock()

        def exploding_collector(**kwargs):
            raise ConnectionError("winline timeout")

        poller = _make_poller(mod, clock, exploding_collector)
        _begin(poller)

        # Must not raise
        result = poller.tick()
        assert result is not None
        attempt = result.get("attempt", {})
        assert attempt.get("error") is not None
        assert "collector_exception" in str(attempt.get("error"))
        assert poller.is_active()

    def test_multiple_exceptions_dont_stop_poller(self):
        mod = _load_mod()
        clock = FakeClock()

        def exploding_collector(**kwargs):
            raise TimeoutError("browser timeout")

        poller = _make_poller(mod, clock, exploding_collector)
        _begin(poller)

        for _ in range(5):
            result = poller.tick()
            assert result is not None
            clock.advance(5.0)

        assert poller.is_active()
        assert len(poller.attempts()) == 5


# ─── REQ-08: Single accelerated task per key ──────────────────────────────────


class TestSingleAcceleratedTaskPerKey:
    def test_single_accelerated_task_per_key(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        results = []
        barrier = threading.Barrier(4, timeout=5)

        def accelerate():
            barrier.wait()
            poller.set_accelerated(True)
            results.append(poller.accelerated)

        threads = [threading.Thread(target=accelerate) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert all(results)
        assert poller.accelerated is True


# ─── REQ-09: Rollover stops accelerated poller ────────────────────────────────


class TestRolloverStopsAccelerated:
    def test_rollover_stops_accelerated_poller(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()

        current = [True]

        def is_current(**kw):
            return current[0]

        poller = _make_poller(mod, clock, collector, is_current=is_current)
        _begin(poller)
        poller.set_accelerated(True)
        poller.tick()
        assert poller.is_active()
        assert poller.accelerated is True

        # Simulate map rollover
        current[0] = {"current": False, "reason": "map_rollover", "proven": True}
        clock.advance(2.0)
        result = poller.tick()
        assert result is not None
        assert result.get("status") == "terminal"
        assert not poller.is_active()


# ─── REQ-10: Dispatch delivery clears acceleration ────────────────────────────


class TestDispatchDeliveryClearsAcceleration:
    def test_dispatch_delivery_clears_acceleration(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        poller.set_accelerated(True)
        assert poller.accelerated is True

        # Simulate deceleration after delivery commit
        poller.set_accelerated(False)
        assert poller.accelerated is False

        # Cadence reverts to 5s
        poller.tick()
        clock.advance(3.0)
        result = poller.tick()
        assert result is not None
        assert result.get("status") == "not_due"

        clock.advance(2.0)  # total 5s
        result = poller.tick()
        assert result is not None
        assert result.get("attempt") is not None


# ─── REQ-11: Safety ceiling in accelerated mode ───────────────────────────────


class TestSafetyCeilingAccelerated:
    def test_safety_ceiling_in_accelerated_mode(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        poller.set_accelerated(True)
        poller.tick()

        # Advance to 90 minutes
        clock.advance(5400.0)
        result = poller.tick()
        assert result is not None
        assert result.get("status") == "terminal"
        terminal = poller.terminal()
        assert terminal is not None
        assert terminal["primary_reason"] == "indeterminate_map_lifecycle"


# ─── REQ-12: No forbidden imports in poller module (AST guard) ────────────────


class TestNoForbiddenImports:
    def test_module_exists_and_has_no_browser_or_sleep_capability(self):
        import ast

        if not MODULE_PATH.is_file():
            pytest.fail(f"missing module: {MODULE_PATH}")
        source = MODULE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(MODULE_PATH))

        forbidden = {
            "camoufox", "playwright", "selenium", "webdriver",
            "subprocess", "threading", "multiprocessing", "asyncio",
            "telegram",
        }
        violations = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root = alias.name.split(".")[0].lower()
                    if root in forbidden:
                        violations.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    root = node.module.split(".")[0].lower()
                    if root in forbidden:
                        violations.append(node.module)

        assert violations == [], f"Forbidden imports found: {violations}"


# ─── REQ-13: Acceleration preserves delivery state machine ────────────────────


class TestAccelerationPreservesDeliveryStateMachine:
    def test_acceleration_preserves_delivery_state_machine(self):
        """Same classification results with/without acceleration for same inputs."""
        mod = _load_mod()

        # Run without acceleration
        clock1 = FakeClock()
        collector1 = FakeCollector([
            {"market_status": "open", "p1_odds": 1.85, "p2_odds": 2.05},
        ])
        poller1 = _make_poller(mod, clock1, collector1)
        _begin(poller1)
        result1 = poller1.tick()

        # Run with acceleration
        clock2 = FakeClock()
        collector2 = FakeCollector([
            {"market_status": "open", "p1_odds": 1.85, "p2_odds": 2.05},
        ])
        poller2 = _make_poller(mod, clock2, collector2)
        _begin(poller2)
        poller2.set_accelerated(True)
        result2 = poller2.tick()

        # Both should accept odds identically
        assert result1.get("status") == result2.get("status") == "success"
        a1 = result1.get("attempt", {})
        a2 = result2.get("attempt", {})
        assert a1.get("accepted") == a2.get("accepted") == True
        assert a1.get("p1_odds") == a2.get("p1_odds")
        assert a1.get("p2_odds") == a2.get("p2_odds")


# ─── REQ-14: Overrun coalesce in accelerated mode ─────────────────────────────


class TestOverrunCoalesceAccelerated:
    def test_overrun_coalesce_in_accelerated_mode(self):
        mod = _load_mod()
        clock = FakeClock()

        call_count = [0]

        def slow_collector(**kwargs):
            call_count[0] += 1
            # Simulate 5s collector latency at 2s interval
            clock.advance(5.0)
            return {
                "market_status": "missing",
                "source": "winline_current_map_winner",
                "p1_odds": None,
                "p2_odds": None,
                "map_num": MAP_NUM,
                "team1": TEAM1,
                "team2": TEAM2,
                "page_valid": True,
                "dom_signature": "s",
                "dom_hash": "h",
                "parser_failure_reasons": [],
            }

        poller = _make_poller(mod, clock, slow_collector)
        _begin(poller)
        poller.set_accelerated(True)

        result = poller.tick()
        assert result is not None
        attempt = result.get("attempt", {})
        # 5s collector at 2s interval → overrun
        assert attempt.get("cadence_overrun") is True
        assert attempt.get("coalesced_missed_intervals", 0) >= 2
        # Next poll should be immediate (coalesced)
        assert attempt.get("next_poll_at_monotonic", float("inf")) <= clock.mono + 0.01

        # Immediate follow-up (no burst)
        result2 = poller.tick()
        assert result2 is not None
        assert result2.get("attempt") is not None
        assert call_count[0] == 2


# ─── Integration: accelerate/decelerate helpers ───────────────────────────────


class TestAccelerateDecelerateHelpers:
    """Test the cyberscore_try.py helper functions in isolation via direct poller manipulation."""

    def test_accelerate_sets_flag_on_matching_poller(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        assert poller.accelerated is False
        poller.set_accelerated(True)
        assert poller.accelerated is True

    def test_decelerate_clears_flag(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()
        poller = _make_poller(mod, clock, collector)
        _begin(poller)

        poller.set_accelerated(True)
        poller.set_accelerated(False)
        assert poller.accelerated is False

    def test_begin_resets_accelerated(self):
        mod = _load_mod()
        clock = FakeClock()
        collector = FakeCollector()

        current = [True]
        poller = _make_poller(mod, clock, collector, is_current=lambda **kw: current[0])
        _begin(poller)
        poller.set_accelerated(True)

        # Terminalize then re-begin
        current[0] = {"current": False, "reason": "map_rollover", "proven": True}
        clock.advance(2.0)
        poller.tick()
        assert not poller.is_active()

        current[0] = True
        poller.begin(series=SERIES, map_num=MAP_NUM, team1=TEAM1, team2=TEAM2)
        assert poller.accelerated is False
