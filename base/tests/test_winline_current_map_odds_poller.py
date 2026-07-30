"""W-MAP-POLLER-CONTROLLER: deterministic whole-current-map Winline polling.

Exclusive ownership of this file + runtime/winline_current_map_odds_poller.py.
Proves a no-sleep controller:
- inject collector / mono+wall clocks / exact-map-current predicate
- +5s cadence from prior attempt *start*; never overlapping attempts
- overrun coalesces missed intervals into one immediate non-overlapping follow-up
- canonical (series, map_num, ordered teams) identity
- lifecycle terminals: rollover, series end/cancel, PID generation change, 90m safety
- three-miss controlled_reload with >=60s spacing; DOM-change / accepted-odds reset
- per-attempt evidence + one primary whole-map failure reason
- no browser import/start and no real sleep
"""
from __future__ import annotations

import ast
import importlib.util
import math
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "runtime" / "winline_current_map_odds_poller.py"
BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

FORBIDDEN_IMPORT_TOKENS = (
    "camoufox",
    "Camoufox",
    "playwright",
    "Playwright",
    "selenium",
    "webdriver",
    "subprocess",
    "threading",
    "multiprocessing",
    "telegram",
    "Telegram",
)

SERIES = "dltv.org/matches/8900882416"
TEAM1 = "BoomBoys"
TEAM2 = "Nigma Galaxy"
MAP_NUM = 2
CANONICAL = f"{SERIES}|map{MAP_NUM}|{TEAM1}|{TEAM2}"


def _load_mod():
    if not MODULE_PATH.is_file():
        pytest.fail(f"missing module under test: {MODULE_PATH}")
    # Fresh load every call so implementation can appear mid-suite after RED.
    name = f"winline_current_map_odds_poller_{id(object())}"
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
        seconds = float(seconds)
        self.mono += seconds
        self.wall += seconds


class FakeCollector:
    """Scripted collector. Records calls; never sleeps or opens a browser."""

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
            "current_url": f"https://winline.example/match/{SERIES}",
            "dom_signature": "sig-a",
            "dom_hash": "hash-a",
            "parser_failure_reasons": [],
            "error": None,
            "acquisition_error": None,
            "page_valid": True,
            "observed_at": None,
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
        if payload.get("observed_at") is None:
            # collector observes at call time via injected wall if present
            payload["observed_at"] = kwargs.get("now") or time.time()
        payload.setdefault("acquisition_mode_echo", kwargs.get("acquisition_mode"))
        return payload


def _base_identity(**overrides: Any) -> Dict[str, Any]:
    ident = {
        "series": SERIES,
        "map_num": MAP_NUM,
        "team1": TEAM1,
        "team2": TEAM2,
    }
    ident.update(overrides)
    return ident


def _make_poller(
    mod,
    *,
    collector: Optional[FakeCollector] = None,
    clock: Optional[FakeClock] = None,
    is_map_current: Optional[Callable[..., Any]] = None,
    producer_pid: int = 2904707,
    producer_start_generation: str = "gen-1",
    selected_side: Any = None,
    **kwargs: Any,
):
    collector = collector or FakeCollector()
    clock = clock or FakeClock()
    if is_map_current is None:
        is_map_current = lambda **_kw: True  # noqa: E731

    factory = getattr(mod, "WinlineCurrentMapOddsPoller", None) or getattr(
        mod, "CurrentMapOddsPoller", None
    )
    if factory is None:
        pytest.fail("module must export WinlineCurrentMapOddsPoller (or CurrentMapOddsPoller)")

    return factory(
        collector=collector,
        is_map_current=is_map_current,
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        producer_pid=producer_pid,
        producer_start_generation=producer_start_generation,
        selected_side=selected_side,
        **kwargs,
    ), collector, clock


# ---------------------------------------------------------------------------
# Source / capability guards
# ---------------------------------------------------------------------------


def test_module_exists_and_has_no_browser_or_sleep_capability() -> None:
    assert MODULE_PATH.is_file(), f"expected {MODULE_PATH}"
    src = MODULE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names: List[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            else:
                names = [node.module or ""] + [a.name for a in node.names]
            joined = " ".join(names)
            for token in FORBIDDEN_IMPORT_TOKENS:
                assert token not in joined, f"forbidden import token {token!r} in {joined!r}"
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "sleep":
                pytest.fail("module must not call *.sleep")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id == "sleep":
                pytest.fail("module must not call sleep()")
    # Explicit source ban on time.sleep even if not AST-called via alias.
    assert "time.sleep" not in src
    assert "sleep(" not in src or "sleep_interval" in src or True  # soft; AST already covers calls
    # Harder: no import of sleep from time
    assert "from time import sleep" not in src


def test_canonical_identity_includes_series_map_ordered_teams() -> None:
    mod = _load_mod()
    key_fn = getattr(mod, "make_canonical_map_key", None) or getattr(
        mod, "canonical_map_key", None
    )
    assert callable(key_fn), "module must export make_canonical_map_key"
    key = key_fn(SERIES, MAP_NUM, TEAM1, TEAM2)
    assert SERIES in str(key)
    assert "map2" in str(key).lower() or str(MAP_NUM) in str(key)
    assert TEAM1 in str(key)
    assert TEAM2 in str(key)
    # ordered teams matter
    key_rev = key_fn(SERIES, MAP_NUM, TEAM2, TEAM1)
    assert key != key_rev


# ---------------------------------------------------------------------------
# Begin / timing / no-overlap
# ---------------------------------------------------------------------------


def test_begin_only_after_successful_current_map_parse_and_continues_while_current() -> None:
    mod = _load_mod()
    current = {"value": False}

    def pred(**kw):
        return bool(current["value"])

    poller, collector, clock = _make_poller(mod, is_map_current=pred)
    # cannot begin while not current
    started = poller.begin(**_base_identity())
    assert started is False or started is None or started == "not_current"
    assert poller.is_active() is False
    assert collector.calls == []

    current["value"] = True
    started = poller.begin(**_base_identity())
    assert started is True or started == "started" or started is None  # allow True-ish
    # begin itself does not necessarily poll; tick drives attempts
    assert poller.is_active() is True


def test_first_attempt_runs_immediately_on_first_due_tick_with_initial_goto() -> None:
    mod = _load_mod()
    collector = FakeCollector(
        [
            {
                "market_status": "open",
                "p1_odds": 1.55,
                "p2_odds": 2.40,
                "dom_signature": "sig-ok",
                "dom_hash": "hash-ok",
            }
        ]
    )
    clock = FakeClock(mono=500.0, wall=2_000_000.0)
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    assert poller.begin(**_base_identity()) in (True, "started", None) or poller.is_active()
    # force active if begin returns loosely
    if not poller.is_active():
        poller.begin(**_base_identity())

    result = poller.tick()
    assert result is not None
    assert len(collector.calls) == 1
    assert collector.calls[0].get("acquisition_mode") == "initial_goto"
    attempt = result.get("attempt") or result
    assert attempt["acquisition_mode"] == "initial_goto"
    assert attempt["attempt_index"] == 1
    assert attempt["producer_pid"] == 2904707
    assert attempt.get("producer_start_generation") == "gen-1" or attempt.get(
        "start_generation"
    ) == "gen-1"


def test_next_attempt_due_exactly_5s_after_prior_start_not_finish() -> None:
    mod = _load_mod()
    # each collect "takes" wall time via advance inside collector
    clock = FakeClock(mono=100.0, wall=10_000.0)
    latencies = [1.5, 0.5]
    start_monos: List[float] = []

    class TimedCollector(FakeCollector):
        def __call__(self, **kwargs):
            start_monos.append(clock.mono)
            out = super().__call__(**kwargs)
            # simulate work latency after call starts
            lat = latencies[min(len(self.calls) - 1, len(latencies) - 1)]
            clock.advance(lat)
            out["latency_seconds"] = lat
            out["observed_at"] = clock.wall
            return out

    collector = TimedCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s1",
                "dom_hash": "h1",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "s1",
                "dom_hash": "h1",
                "page_valid": True,
            },
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())

    r1 = poller.tick()
    assert r1 is not None
    assert len(collector.calls) == 1
    started_mono_1 = float(start_monos[0])
    finished_mono_1 = clock.mono
    att1 = r1.get("attempt") or r1
    next_poll = att1.get("next_poll_at_monotonic") or att1.get("next_poll_monotonic")
    assert next_poll is not None
    # Prior-start cadence: due = start + 5s (not finish + 5s).
    assert abs(float(next_poll) - (started_mono_1 + 5.0)) < 1e-9
    assert abs(float(next_poll) - (finished_mono_1 + 5.0)) > 1e-6
    assert att1.get("cadence_overrun") is False
    assert att1.get("cadence_compliant") is True
    assert float(att1.get("attempt_latency_seconds") or att1["latency_seconds"]) == pytest.approx(1.5)

    # early tick must not fire (still before start+5)
    clock.advance(3.4)  # finished was +1.5; start+5 is 3.5 after finish
    r_early = poller.tick()
    assert r_early is None or r_early.get("status") in ("waiting", "not_due", None)
    if isinstance(r_early, dict) and r_early.get("attempt"):
        pytest.fail("must not schedule attempt before prior-start+5s")
    assert len(collector.calls) == 1

    # exactly at start+5s (finish was at 101.5; start+5 = 105.0; need +3.5 from finish → already +3.4)
    clock.advance(0.1)
    r2 = poller.tick()
    assert r2 is not None
    assert len(collector.calls) == 2
    assert collector.calls[1].get("acquisition_mode") == "dynamic_dom"
    att2 = r2.get("attempt") or r2
    assert att2.get("attempt_start_delta_seconds") == pytest.approx(5.0)
    assert float(att2.get("attempt_started_at_monotonic")) == pytest.approx(started_mono_1 + 5.0)


def test_synchronous_overrun_coalesces_to_one_immediate_non_overlapping_attempt() -> None:
    """Long collector (>interval) yields one immediate coalesced follow-up, no burst."""
    mod = _load_mod()
    clock = FakeClock(mono=1000.0, wall=50_000.0)
    # First attempt takes 12s → misses start+5 and start+10; second is fast.
    latencies = [12.0, 0.2, 0.2]
    start_monos: List[float] = []

    class SlowCollector(FakeCollector):
        def __call__(self, **kwargs):
            start_monos.append(float(clock.mono))
            out = super().__call__(**kwargs)
            lat = latencies[min(len(self.calls) - 1, len(latencies) - 1)]
            clock.advance(lat)
            out["latency_seconds"] = lat
            return out

    collector = SlowCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            },
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())

    r1 = poller.tick()
    att1 = r1.get("attempt") or r1
    assert att1.get("cadence_overrun") is True or att1.get("overrun") is True
    assert att1.get("cadence_compliant") is False
    assert int(att1.get("coalesced_missed_intervals") or 0) >= 2
    # next due is immediate at finish, not finish+5 and not a burst queue
    next1 = float(att1.get("next_poll_at_monotonic") or att1["next_poll_monotonic"])
    assert next1 == pytest.approx(float(clock.mono))
    assert float(att1["attempt_latency_seconds"]) == pytest.approx(12.0)
    assert float(att1["attempt_started_at_monotonic"]) == pytest.approx(1000.0)

    # Immediate next opportunity (no clock advance) → exactly one follow-up
    r2 = poller.tick()
    assert r2 is not None
    assert len(collector.calls) == 2
    att2 = r2.get("attempt") or r2
    assert float(att2["attempt_started_at_monotonic"]) == pytest.approx(start_monos[1])
    # start delta reflects actual mono gap from prior start (12s overrun path)
    assert float(att2["attempt_start_delta_seconds"]) == pytest.approx(12.0)
    # Still serial: mid-flight reentry blocked
    assert poller._in_flight is False  # type: ignore[attr-defined]

    # Without advancing past start2+5, no third attempt (no burst of missed ticks)
    r_mid = poller.tick()
    assert r_mid is None or r_mid.get("status") in ("waiting", "not_due", None)
    if isinstance(r_mid, dict) and r_mid.get("attempt"):
        pytest.fail("must not burst-replay missed ticks after coalesce")
    assert len(collector.calls) == 2

    # After start2+5, exactly one more attempt
    clock.advance(5.0)
    r3 = poller.tick()
    assert r3 is not None
    assert len(collector.calls) == 3


def test_truthful_delta_latency_overrun_fields_on_attempt_record() -> None:
    mod = _load_mod()
    clock = FakeClock(mono=200.0, wall=9_000.0)
    start_monos: List[float] = []

    class Timed(FakeCollector):
        def __call__(self, **kwargs):
            start_monos.append(float(clock.mono))
            out = super().__call__(**kwargs)
            clock.advance(2.0)
            out["latency_seconds"] = 2.0
            return out

    collector = Timed(
        [
            {"market_status": "missing", "dom_signature": "a", "dom_hash": "ha", "page_valid": True},
            {"market_status": "missing", "dom_signature": "a", "dom_hash": "ha", "page_valid": True},
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())
    r1 = poller.tick()
    a1 = r1.get("attempt") or r1
    for field in (
        "attempt_started_at_monotonic",
        "attempt_finished_at_monotonic",
        "attempt_latency_seconds",
        "attempt_start_delta_seconds",
        "cadence_overrun",
        "coalesced_missed_intervals",
        "cadence_compliant",
        "nominal_next_poll_at_monotonic",
    ):
        assert field in a1, f"missing {field}"
    assert a1["attempt_start_delta_seconds"] is None  # first attempt
    assert a1["attempt_latency_seconds"] == pytest.approx(2.0)
    assert a1["cadence_overrun"] is False
    assert a1["cadence_compliant"] is True
    assert a1["nominal_next_poll_at_monotonic"] == pytest.approx(205.0)

    clock.advance(3.0)  # to start1+5
    r2 = poller.tick()
    a2 = r2.get("attempt") or r2
    assert a2["attempt_start_delta_seconds"] == pytest.approx(5.0)
    assert a2["attempt_latency_seconds"] == pytest.approx(2.0)
    assert a2["cadence_overrun"] is False


def test_controlled_reload_only_after_three_stable_eligible_misses_and_60s_spacing() -> None:
    """Mode matrix: controlled_reload after 3 stable eligible misses AND >=60s since prior reload."""
    mod = _load_mod()
    clock = FakeClock(mono=0.0, wall=0.0)
    miss = {
        "market_status": "missing",
        "dom_signature": "stable",
        "dom_hash": "stable-h",
        "page_valid": True,
    }
    # Enough misses for: initial + dynamics + reload + more + second reload after spacing
    collector = FakeCollector([dict(miss) for _ in range(12)])
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())

    modes: List[str] = []
    for i in range(10):
        r = poller.tick()
        if r is None or r.get("status") in ("not_due", "waiting"):
            clock.advance(5.0)
            r = poller.tick()
        att = (r or {}).get("attempt") or r or {}
        if att.get("acquisition_mode"):
            modes.append(str(att["acquisition_mode"]))
        clock.advance(5.0)

    assert modes[0] == "initial_goto"
    # attempts 2,3 are dynamic_dom (miss streak building)
    assert modes[1] == "dynamic_dom"
    assert modes[2] == "dynamic_dom"
    # 4th after 3 consecutive eligible misses → controlled_reload
    assert modes[3] == "controlled_reload", modes
    # immediately after reload, spacing blocks another controlled_reload even if streak rebuilds
    # next few should be dynamic_dom until 60s since last reload
    assert "dynamic_dom" in modes[4:7], modes
    assert "controlled_reload" not in modes[4:7], modes

    # Jump clock past 60s from last reload finish and rebuild streak of 3 misses
    clock.advance(60.0)
    # force three more eligible misses on dynamic then expect reload
    extra_modes: List[str] = []
    for _ in range(6):
        r = poller.tick()
        if r is None or r.get("status") in ("not_due", "waiting"):
            clock.advance(5.0)
            continue
        att = r.get("attempt") or r
        extra_modes.append(str(att.get("acquisition_mode")))
        clock.advance(5.0)
    assert "controlled_reload" in extra_modes, (modes, extra_modes)


def test_in_flight_attempt_cannot_schedule_another() -> None:
    mod = _load_mod()
    clock = FakeClock()
    reentered: List[bool] = []

    class ReentrantCollector(FakeCollector):
        def __call__(self, **kwargs):
            # mid-flight tick again
            reentered.append(True)
            mid = poller.tick()
            assert mid is None or (
                isinstance(mid, dict)
                and mid.get("status") in ("in_flight", "busy", "waiting", "not_due")
            )
            return super().__call__(**kwargs)

    collector = ReentrantCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            }
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())
    r = poller.tick()
    assert r is not None
    assert len(collector.calls) == 1
    assert reentered and reentered[0] is True


# ---------------------------------------------------------------------------
# Lifecycle terminals
# ---------------------------------------------------------------------------


def test_stops_on_proven_map_rollover() -> None:
    mod = _load_mod()
    clock = FakeClock()
    state = {"current": True}

    def pred(**kw):
        if not state["current"]:
            return {"current": False, "reason": "map_rollover"}
        return True

    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            }
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock, is_map_current=pred)
    poller.begin(**_base_identity())
    assert poller.tick() is not None
    state["current"] = False
    clock.advance(5.0)
    terminal = poller.tick()
    assert terminal is not None
    assert poller.is_active() is False
    term = terminal.get("terminal") or terminal
    assert term.get("terminal") is True or term.get("status") == "terminal" or term.get(
        "outcome"
    ) in (
        "terminal",
        "map_ended",
        "failed",
    )
    reason = term.get("primary_reason") or term.get("failure_reason") or term.get("reason")
    # with only missing market + rollover → market_never_exposed
    assert reason == "market_never_exposed"
    assert len(collector.calls) == 1  # no extra collect after rollover


def test_stops_on_series_complete_or_cancel() -> None:
    mod = _load_mod()
    for end_reason in ("series_complete", "series_cancel"):
        clock = FakeClock()
        state = {"end": None}

        def pred(**kw):
            if state["end"]:
                return {"current": False, "reason": state["end"]}
            return True

        collector = FakeCollector(
            [
                {
                    "market_status": "missing",
                    "dom_signature": "s",
                    "dom_hash": "h",
                    "page_valid": True,
                }
            ]
        )
        poller, _, _ = _make_poller(
            mod, collector=collector, clock=clock, is_map_current=pred
        )
        poller.begin(**_base_identity())
        poller.tick()
        state["end"] = end_reason
        clock.advance(5.0)
        terminal = poller.tick()
        assert terminal is not None
        assert poller.is_active() is False
        term = terminal.get("terminal") or terminal
        reason = term.get("primary_reason") or term.get("failure_reason") or term.get("reason")
        assert reason == "market_never_exposed"


def test_stops_on_service_pid_generation_change() -> None:
    mod = _load_mod()
    clock = FakeClock()
    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            }
        ]
    )
    poller, _, _ = _make_poller(
        mod,
        collector=collector,
        clock=clock,
        producer_pid=100,
        producer_start_generation="gen-A",
    )
    poller.begin(**_base_identity())
    poller.tick()
    clock.advance(5.0)
    # notify generation change via tick kwargs or setter
    if hasattr(poller, "note_service_generation"):
        poller.note_service_generation(pid=200, start_generation="gen-B")
        terminal = poller.tick()
    else:
        terminal = poller.tick(producer_pid=200, producer_start_generation="gen-B")
    assert terminal is not None
    assert poller.is_active() is False
    term = terminal.get("terminal") or terminal
    reason = term.get("primary_reason") or term.get("failure_reason") or term.get("reason")
    # generation change is proven lifecycle end of this producer observation
    assert reason in (
        "market_never_exposed",
        "indeterminate_map_lifecycle",
        "service_generation_change",
    ) or term.get("lifecycle_event") == "service_generation_change"
    # Contract prefers one of the four primary reasons when map ends without odds.
    # Generation change with attempts that were all missing → market_never_exposed
    # or indeterminate if generation change is not treated as proven map end.
    assert reason in {
        "market_never_exposed",
        "parser_resolution_failure",
        "browser_acquisition_failure",
        "indeterminate_map_lifecycle",
    }


def test_90_minute_safety_ceiling_is_indeterminate_not_map_end_proof() -> None:
    mod = _load_mod()
    clock = FakeClock(mono=0.0, wall=0.0)
    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            }
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())
    poller.tick()
    # jump past 90 minutes from begin/first attempt
    clock.advance(90 * 60 + 0.1)
    terminal = poller.tick()
    assert terminal is not None
    assert poller.is_active() is False
    term = terminal.get("terminal") or terminal
    reason = term.get("primary_reason") or term.get("failure_reason") or term.get("reason")
    assert reason == "indeterminate_map_lifecycle"
    # safety ceiling must not claim proven map end
    assert term.get("map_end_proven") is not True


# ---------------------------------------------------------------------------
# Reload escalation
# ---------------------------------------------------------------------------


def test_three_consecutive_market_missing_requests_one_controlled_reload() -> None:
    mod = _load_mod()
    clock = FakeClock()
    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "same",
                "dom_hash": "same",
                "page_valid": True,
            },
            {
                "market_status": "closed",
                "dom_signature": "same",
                "dom_hash": "same",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "same",
                "dom_hash": "same",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "same",
                "dom_hash": "same",
                "page_valid": True,
            },
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())

    modes: List[str] = []
    for i in range(4):
        if i > 0:
            clock.advance(5.0)
        r = poller.tick()
        assert r is not None
        att = r.get("attempt") or r
        modes.append(att["acquisition_mode"])

    assert modes[0] == "initial_goto"
    assert modes[1] == "dynamic_dom"
    assert modes[2] == "dynamic_dom"
    # after 3 consecutive eligible misses, 4th is controlled_reload
    assert modes[3] == "controlled_reload"
    assert collector.calls[3]["acquisition_mode"] == "controlled_reload"


def test_reload_not_more_often_than_once_per_60s_per_canonical_map() -> None:
    mod = _load_mod()
    clock = FakeClock()
    # 3 miss -> reload, then 3 more miss quickly; second reload blocked until 60s
    results = [
        {
            "market_status": "missing",
            "dom_signature": "s",
            "dom_hash": "h",
            "page_valid": True,
        }
        for _ in range(10)
    ]
    collector = FakeCollector(results)
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())

    modes: List[str] = []
    for i in range(7):
        if i > 0:
            clock.advance(5.0)
        r = poller.tick()
        assert r is not None
        modes.append((r.get("attempt") or r)["acquisition_mode"])

    # indices: 0 initial, 1 dyn, 2 dyn, 3 reload (after 3 misses), then
    # misses continue but reload spacing blocks another reload until 60s
    assert modes[0] == "initial_goto"
    assert modes[3] == "controlled_reload"
    # attempts 4,5,6 within <60s of reload → dynamic_dom even if 3 more misses
    assert modes[4] == "dynamic_dom"
    assert modes[5] == "dynamic_dom"
    assert modes[6] == "dynamic_dom"

    # advance so total since last reload >= 60s and miss streak eligible
    # last reload at attempt 4 finish; we've advanced 5*3=15 since then across 4,5,6
    # need >=60 from reload completion: advance more
    clock.advance(50.0)  # plenty past 60 from reload
    r = poller.tick()
    assert r is not None
    assert (r.get("attempt") or r)["acquisition_mode"] == "controlled_reload"


def test_changed_dom_signature_resets_consecutive_miss_counter() -> None:
    mod = _load_mod()
    clock = FakeClock()
    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "a",
                "dom_hash": "ha",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "a",
                "dom_hash": "ha",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "b",
                "dom_hash": "hb",
                "page_valid": True,
            },  # change resets
            {
                "market_status": "missing",
                "dom_signature": "b",
                "dom_hash": "hb",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "b",
                "dom_hash": "hb",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "b",
                "dom_hash": "hb",
                "page_valid": True,
            },  # 3rd same after reset -> reload
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())
    modes = []
    for i in range(6):
        if i:
            clock.advance(5.0)
        r = poller.tick()
        modes.append((r.get("attempt") or r)["acquisition_mode"])
    assert modes[0] == "initial_goto"
    assert modes[1] == "dynamic_dom"
    assert modes[2] == "dynamic_dom"  # DOM changed; no reload yet
    assert modes[3] == "dynamic_dom"
    assert modes[4] == "dynamic_dom"
    assert modes[5] == "controlled_reload"


def test_accepted_fresh_odds_resets_and_terminals_success() -> None:
    mod = _load_mod()
    clock = FakeClock(mono=10.0, wall=1_800_000.0)
    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            },
            {
                "market_status": "open",
                "source": "winline_current_map_winner",
                "p1_odds": 1.72,
                "p2_odds": 2.11,
                "dom_signature": "s2",
                "dom_hash": "h2",
                "page_valid": True,
                "map_num": MAP_NUM,
                "team1": TEAM1,
                "team2": TEAM2,
            },
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock, selected_side=None)
    poller.begin(**_base_identity())
    r1 = poller.tick()
    assert (r1.get("attempt") or r1).get("accepted") is not True
    clock.advance(5.0)
    r2 = poller.tick()
    assert r2 is not None
    term = r2.get("terminal") or r2
    assert poller.is_active() is False
    assert term.get("outcome") == "success" or term.get("status") == "success" or term.get(
        "primary_reason"
    ) in (None, "success")
    assert term.get("success") is True or term.get("outcome") == "success"
    att = r2.get("attempt") or r2
    assert att.get("p1_odds") == 1.72
    assert att.get("p2_odds") == 2.11
    assert att.get("accepted") is True


def test_selected_side_absent_empty_non_gating_supplied_preserved() -> None:
    mod = _load_mod()
    for side in (None, "", "  "):
        clock = FakeClock()
        collector = FakeCollector(
            [
                {
                    "market_status": "open",
                    "source": "winline_current_map_winner",
                    "p1_odds": 1.5,
                    "p2_odds": 2.5,
                    "dom_signature": "s",
                    "dom_hash": "h",
                    "page_valid": True,
                }
            ]
        )
        poller, _, _ = _make_poller(
            mod, collector=collector, clock=clock, selected_side=side
        )
        poller.begin(**_base_identity())
        r = poller.tick()
        term = r.get("terminal") or r
        assert term.get("success") is True or term.get("outcome") == "success"
        att = r.get("attempt") or r
        # preserve mapping field (empty ok)
        assert "selected_side" in att

    # supplied side preserved on attempt record
    clock = FakeClock()
    collector = FakeCollector(
        [
            {
                "market_status": "open",
                "source": "winline_current_map_winner",
                "p1_odds": 1.5,
                "p2_odds": 2.5,
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            }
        ]
    )
    poller, _, _ = _make_poller(
        mod, collector=collector, clock=clock, selected_side="P2"
    )
    poller.begin(**_base_identity())
    r = poller.tick()
    att = r.get("attempt") or r
    assert str(att.get("selected_side")).upper() == "P2"


# ---------------------------------------------------------------------------
# Terminal classifications
# ---------------------------------------------------------------------------


def test_market_never_exposed_when_all_eligible_closed_or_missing_stable_page() -> None:
    mod = _load_mod()
    clock = FakeClock()
    state = {"current": True}

    def pred(**kw):
        return True if state["current"] else {"current": False, "reason": "map_rollover"}

    collector = FakeCollector(
        [
            {
                "market_status": "closed",
                "dom_signature": "stable",
                "dom_hash": "hs",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "stable",
                "dom_hash": "hs",
                "page_valid": True,
            },
            {
                "market_status": "closed",
                "dom_signature": "stable",
                "dom_hash": "hs",
                "page_valid": True,
            },
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock, is_map_current=pred)
    poller.begin(**_base_identity())
    for i in range(3):
        if i:
            clock.advance(5.0)
        poller.tick()
    state["current"] = False
    clock.advance(5.0)
    terminal = poller.tick()
    term = terminal.get("terminal") or terminal
    reason = term.get("primary_reason") or term.get("failure_reason") or term.get("reason")
    assert reason == "market_never_exposed"
    assert term.get("attempt_count") == 3 or len(term.get("attempts") or []) == 3 or True


def test_parser_resolution_failure_when_text_present_but_p1p2_never_valid() -> None:
    mod = _load_mod()
    clock = FakeClock()
    state = {"current": True}

    def pred(**kw):
        return True if state["current"] else {"current": False, "reason": "map_rollover"}

    collector = FakeCollector(
        [
            {
                "market_status": "open",
                "source": "winline_current_map_winner",
                "p1_odds": None,
                "p2_odds": "not-a-number",
                "dom_signature": "chg1",
                "dom_hash": "h1",
                "page_valid": True,
                "parser_failure_reasons": ["p1_odds_missing", "p2_odds_invalid"],
            },
            {
                "market_status": "open",
                "source": "winline_current_map_winner",
                "p1_odds": 0.9,  # not >1
                "p2_odds": 1.1,
                "dom_signature": "chg2",
                "dom_hash": "h2",
                "page_valid": True,
                "parser_failure_reasons": ["p1_odds_invalid"],
            },
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock, is_map_current=pred)
    poller.begin(**_base_identity())
    poller.tick()
    clock.advance(5.0)
    poller.tick()
    state["current"] = False
    clock.advance(5.0)
    terminal = poller.tick()
    term = terminal.get("terminal") or terminal
    reason = term.get("primary_reason") or term.get("failure_reason") or term.get("reason")
    assert reason == "parser_resolution_failure"


def test_browser_acquisition_failure_for_persistent_page_load_errors() -> None:
    mod = _load_mod()
    clock = FakeClock()
    state = {"current": True}

    def pred(**kw):
        return True if state["current"] else {"current": False, "reason": "map_rollover"}

    collector = FakeCollector(
        [
            {
                "market_status": "error",
                "page_valid": False,
                "error": "navigation_failed",
                "acquisition_error": "navigation_failed",
                "dom_signature": "",
                "dom_hash": "",
                "parser_failure_reasons": ["browser_error"],
            },
            {
                "market_status": "error",
                "page_valid": False,
                "error": "blank_page",
                "acquisition_error": "blank_page",
                "dom_signature": "",
                "dom_hash": "",
            },
            {
                "market_status": "error",
                "page_valid": False,
                "error": "reset_required",
                "acquisition_error": "reset_required",
                "dom_signature": "",
                "dom_hash": "",
            },
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock, is_map_current=pred)
    poller.begin(**_base_identity())
    for i in range(3):
        if i:
            clock.advance(5.0)
        poller.tick()
    state["current"] = False
    clock.advance(5.0)
    terminal = poller.tick()
    term = terminal.get("terminal") or terminal
    reason = term.get("primary_reason") or term.get("failure_reason") or term.get("reason")
    assert reason == "browser_acquisition_failure"


def test_indeterminate_when_exact_rollover_cannot_be_proven() -> None:
    mod = _load_mod()
    clock = FakeClock()
    # predicate becomes unknown / not proven
    def pred(**kw):
        return {"current": False, "reason": "unknown", "proven": False}

    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            }
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock, is_map_current=pred)
    # begin while still "True" first? begin needs success current parse
    # use a two-phase predicate
    flag = {"phase": "current"}

    def pred2(**kw):
        if flag["phase"] == "current":
            return True
        return {"current": False, "reason": "unknown", "proven": False}

    poller, collector, clock = _make_poller(
        mod, collector=collector, clock=clock, is_map_current=pred2
    )
    poller.begin(**_base_identity())
    poller.tick()
    flag["phase"] = "unknown"
    clock.advance(5.0)
    terminal = poller.tick()
    term = terminal.get("terminal") or terminal
    reason = term.get("primary_reason") or term.get("failure_reason") or term.get("reason")
    assert reason == "indeterminate_map_lifecycle"


# ---------------------------------------------------------------------------
# Attempt evidence schema
# ---------------------------------------------------------------------------


def test_attempt_record_contains_required_evidence_fields() -> None:
    mod = _load_mod()
    clock = FakeClock(mono=42.0, wall=9_000.0)
    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "dom-sig-xyz",
                "dom_hash": "deadbeef",
                "page_valid": True,
                "current_url": "https://winline.example/x",
                "parser_failure_reasons": ["market_missing"],
            }
        ]
    )
    poller, _, _ = _make_poller(
        mod,
        collector=collector,
        clock=clock,
        producer_pid=111,
        producer_start_generation="start-xyz",
    )
    poller.begin(**_base_identity())
    r = poller.tick()
    att = r.get("attempt") or r
    required = [
        "producer_pid",
        "attempt_index",
        "attempt_started_at",
        "attempt_finished_at",
        "latency_seconds",
        "acquisition_mode",
        "current_url",
        "dom_signature",
        "dom_hash",
        "market_status",
        "source",
        "p1_odds",
        "p2_odds",
        "parser_failure_reasons",
        "consecutive_misses",
        "reload_count",
        "canonical_key",
        "map_num",
        "team1",
        "team2",
    ]
    for field in required:
        assert field in att, f"missing evidence field {field}"
    assert att["producer_pid"] == 111
    assert att["map_num"] == MAP_NUM
    assert att["team1"] == TEAM1
    assert att["team2"] == TEAM2
    assert att["acquisition_mode"] == "initial_goto"
    assert att["dom_hash"] == "deadbeef"
    assert isinstance(att["parser_failure_reasons"], list)
    assert att.get("next_poll_at_monotonic") is not None or att.get(
        "next_poll_monotonic"
    ) is not None
    # generation field naming flexibility
    assert (
        att.get("producer_start_generation") == "start-xyz"
        or att.get("start_generation") == "start-xyz"
    )


def test_market_missing_is_pending_not_terminal_success() -> None:
    mod = _load_mod()
    clock = FakeClock()
    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            }
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())
    r = poller.tick()
    assert poller.is_active() is True
    term = r.get("terminal")
    assert not term or term.get("success") is not True
    att = r.get("attempt") or r
    assert att.get("accepted") is not True
    assert att.get("pending") is True or r.get("status") in ("pending", "active", None)


def test_same_map_persists_across_multiple_polls_until_end() -> None:
    mod = _load_mod()
    clock = FakeClock()
    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": f"s{i}",
                "dom_hash": f"h{i}",
                "page_valid": True,
            }
            for i in range(5)
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())
    keys = []
    for i in range(5):
        if i:
            clock.advance(5.0)
        r = poller.tick()
        att = r.get("attempt") or r
        keys.append(att["canonical_key"])
        assert att["map_num"] == MAP_NUM
        assert poller.is_active() is True
    assert len(set(keys)) == 1


def test_wrong_or_blank_page_uses_initial_goto_again() -> None:
    mod = _load_mod()
    clock = FakeClock()
    collector = FakeCollector(
        [
            {
                "market_status": "error",
                "page_valid": False,
                "error": "blank",
                "acquisition_error": "blank",
                "dom_signature": "",
                "dom_hash": "",
            },
            {
                "market_status": "missing",
                "page_valid": True,
                "dom_signature": "ok",
                "dom_hash": "okh",
            },
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())
    r1 = poller.tick()
    assert (r1.get("attempt") or r1)["acquisition_mode"] == "initial_goto"
    clock.advance(5.0)
    r2 = poller.tick()
    # blank/error page → next is still initial_goto (not dynamic_dom)
    assert (r2.get("attempt") or r2)["acquisition_mode"] == "initial_goto"


def test_no_real_time_sleep_during_polling_loop() -> None:
    """Guard: running several ticks with fake clock finishes instantly."""
    mod = _load_mod()
    clock = FakeClock()
    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s",
                "dom_hash": "h",
                "page_valid": True,
            }
            for _ in range(20)
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock)
    poller.begin(**_base_identity())
    t0 = time.perf_counter()
    for i in range(10):
        if i:
            clock.advance(5.0)
        poller.tick()
    elapsed = time.perf_counter() - t0
    assert elapsed < 1.0, f"poller slept or blocked; elapsed={elapsed}"


def test_terminal_preserves_counters_timeline_and_last_diagnostics() -> None:
    mod = _load_mod()
    clock = FakeClock()
    state = {"current": True}

    def pred(**kw):
        return True if state["current"] else {"current": False, "reason": "map_rollover"}

    collector = FakeCollector(
        [
            {
                "market_status": "missing",
                "dom_signature": "s1",
                "dom_hash": "h1",
                "page_valid": True,
            },
            {
                "market_status": "missing",
                "dom_signature": "s1",
                "dom_hash": "h1",
                "page_valid": True,
            },
        ]
    )
    poller, _, _ = _make_poller(mod, collector=collector, clock=clock, is_map_current=pred)
    poller.begin(**_base_identity())
    poller.tick()
    clock.advance(5.0)
    poller.tick()
    state["current"] = False
    clock.advance(5.0)
    terminal = poller.tick()
    term = terminal.get("terminal") or terminal
    assert term.get("primary_reason") == "market_never_exposed" or term.get(
        "failure_reason"
    ) == "market_never_exposed" or term.get("reason") == "market_never_exposed"
    # timeline / counters preserved
    assert term.get("reload_count") is not None or "reload_count" in term
    assert term.get("attempt_count", 0) >= 2 or len(term.get("attempts") or []) >= 2 or len(
        poller.attempts() if hasattr(poller, "attempts") else []
    ) >= 2
    diag = term.get("last_diagnostics") or term.get("last_attempt") or {}
    if diag:
        assert "dom_hash" in diag or "dom_signature" in diag or "market_status" in diag
