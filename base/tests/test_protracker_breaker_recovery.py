"""Regression: ProTracker shared-Camoufox circuit breaker must self-heal.

The breaker previously had no recovery path: after 2 consecutive failures it
went OPEN and stayed OPEN for the whole process run, permanently disabling
ProTracker metrics (Lane_adv_protracker / Protracker_1vs1 / Protracker_duo /
d2pt) on every subsequent match. These tests pin the self-healing half-open
behaviour: OPEN -> cooldown -> one probe -> CLOSED on success / re-armed OPEN
on failure.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

cs = importlib.import_module("cyberscore_try")


def _reset_breaker(monkeypatch) -> None:
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_CAMOUFOX_BROKEN", False, raising=False)
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_BROKEN_AT", 0.0, raising=False)
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_CONSECUTIVE_FAILS", 0, raising=False)
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_FAILS_TO_BREAK", 2, raising=False)
    # request_reset must not touch the real shared session during tests.
    monkeypatch.setattr(cs._shared_camoufox_session, "request_reset", lambda: None)


def test_breaker_stays_open_within_cooldown(monkeypatch):
    """2 consecutive fails -> OPEN; an immediate 3rd call short-circuits (runner not invoked)."""
    _reset_breaker(monkeypatch)
    monkeypatch.setenv("PROTRACKER_BREAKER_COOLDOWN_SECONDS", "600")

    runner_calls: list[str] = []

    def _failing(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        runner_calls.append(label)
        raise RuntimeError("boom")

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _failing)

    # Two failures arm the breaker.
    for _ in range(2):
        with pytest.raises(RuntimeError, match="boom"):
            cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None)
    assert cs._PROTRACKER_SHARED_CAMOUFOX_BROKEN is True
    assert len(runner_calls) == 2

    # Immediate 3rd call: fail closed, runner NOT invoked.
    with pytest.raises(RuntimeError, match="marked broken"):
        cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None)
    assert len(runner_calls) == 2, "within-cooldown call must not reach the runner"


def test_breaker_half_open_probe_recovers_on_success(monkeypatch):
    """After cooldown, one probe is allowed; on success the breaker resets to CLOSED."""
    _reset_breaker(monkeypatch)
    monkeypatch.setenv("PROTRACKER_BREAKER_COOLDOWN_SECONDS", "600")

    def _failing(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        raise RuntimeError("boom")

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _failing)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="boom"):
            cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None)
    assert cs._PROTRACKER_SHARED_CAMOUFOX_BROKEN is True

    # Pretend cooldown has elapsed.
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_BROKEN_AT", 0.0)  # epoch -> ages ago

    probe_calls: list[str] = []

    def _succeeding(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        probe_calls.append(label)
        return {"matchups": {"1": []}, "synergies": {"1": []}}

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _succeeding)

    result = cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None)
    assert probe_calls == ["dota2protracker:anti-mage"]
    assert result == {"matchups": {"1": []}, "synergies": {"1": []}}
    # Breaker recovered.
    assert cs._PROTRACKER_SHARED_CAMOUFOX_BROKEN is False
    assert cs._PROTRACKER_SHARED_CONSECUTIVE_FAILS == 0
    assert cs._PROTRACKER_SHARED_BROKEN_AT == 0.0

    # Next call goes straight through (CLOSED), no short-circuit.
    result2 = cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None)
    assert result2 == result
    assert len(probe_calls) == 2


def test_breaker_half_open_probe_failure_rearms(monkeypatch):
    """A failing probe re-arms OPEN with a fresh cooldown timestamp."""
    _reset_breaker(monkeypatch)
    monkeypatch.setenv("PROTRACKER_BREAKER_COOLDOWN_SECONDS", "300")

    def _failing(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        raise RuntimeError("boom")

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _failing)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="boom"):
            cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None)
    first_broken_at = cs._PROTRACKER_SHARED_BROKEN_AT
    assert cs._PROTRACKER_SHARED_CAMOUFOX_BROKEN is True
    assert first_broken_at > 0.0

    # Push broken_at into the past -> half-open probe allowed.
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_BROKEN_AT", 0.0)

    with pytest.raises(RuntimeError, match="boom"):
        cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None)
    # Probe failed -> re-armed OPEN with a fresh (later) timestamp.
    assert cs._PROTRACKER_SHARED_CAMOUFOX_BROKEN is True
    assert cs._PROTRACKER_SHARED_BROKEN_AT > first_broken_at

    # Within cooldown again -> short-circuit, no runner call beyond the probe.
    import itertools
    runner_calls = []

    def _fail_counting(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        runner_calls.append(label)
        raise RuntimeError("boom")

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _fail_counting)
    with pytest.raises(RuntimeError, match="marked broken"):
        cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None)
    assert runner_calls == [], "immediately-after-probe call must short-circuit"


def test_no_subprocess_spawn_on_breaker_path(monkeypatch):
    """The fail-closed and probe paths must never fall through to a Camoufox subprocess."""
    _reset_breaker(monkeypatch)

    import dota2protracker as d2pt

    subprocess_calls: list = []

    def _spy_subprocess(slug, hero_id, proxy_candidate=None):
        subprocess_calls.append((slug, hero_id, proxy_candidate))
        raise AssertionError("subprocess fallback must not be reached")

    monkeypatch.setattr(d2pt, "_fetch_protracker_payload_via_subprocess", _spy_subprocess)
    monkeypatch.setattr(cs, "_protracker_subprocess_fetcher", lambda: _spy_subprocess)

    def _failing(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        raise RuntimeError("boom")

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _failing)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None)

    assert subprocess_calls == [], f"subprocess must never spawn; calls={subprocess_calls}"