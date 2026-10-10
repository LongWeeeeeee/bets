"""Quota/backoff regression at the orphan sweep's outbound HTTP boundary.

Each orphan regression module isolates its own process-global backoff state.
All HTTP is mocked, including a guard against unintended outbound requests.
"""
from __future__ import annotations

import ast
import json
import logging
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402

MATCH_ID = 9033302098
OTHER_MATCH_ID = 9017598786
NOW = datetime(2026, 10, 10, 16, 10, tzinfo=timezone.utc).timestamp()
DAILY_429 = json.loads(
    (BASE_DIR / "tests/fixtures/opendota_429_daily_limit_20261010.json").read_text()
)
BACKOFF_ENV_EXPRESSION = compile(ast.Expression(next(
    node.value for node in ast.parse((BASE_DIR / "cyberscore_try.py").read_text()).body
    if isinstance(node, ast.Assign) and any(
        isinstance(target, ast.Name) and target.id == "LIVE_ELO_ORPHAN_OD_BACKOFF"
        for target in node.targets)
)), str(BASE_DIR / "cyberscore_try.py"), "eval")


def response(status=404, payload=None, headers=None):
    return Mock(status_code=status, headers=headers or {}, json=Mock(return_value=payload))


def daily_response(headers=None):
    return response(DAILY_429["status_code"], DAILY_429["body"],
                    DAILY_429["headers"] if headers is None else headers)


@pytest.fixture(autouse=True)
def isolate_orphan_od_state(monkeypatch):
    """Isolate backoff before and after every test in this module."""
    cs._reset_opendota_orphan_backoff_state()
    monkeypatch.setattr(cs, "_OPENDOTA_ORPHAN_LOG_LAST", {})
    monkeypatch.setattr(cs, "LIVE_ELO_ORPHAN_OD_BACKOFF", True, raising=False)
    monkeypatch.setattr(cs, "LIVE_ELO_ORPHAN_OD_DAY_RESERVE", 300, raising=False)

    def unexpected_http(*args, **kwargs):
        raise AssertionError("Unexpected outbound HTTP in orphan regression")

    monkeypatch.setattr(cs.requests, "get", unexpected_http)
    yield
    cs._reset_opendota_orphan_backoff_state()


@pytest.fixture
def clock(monkeypatch):
    now = [NOW]
    monkeypatch.setattr(cs, "_opendota_orphan_now", lambda: now[0], raising=False)
    return now


@pytest.fixture
def pending_orphan(tmp_path, monkeypatch):
    progress = tmp_path / "progress.json"
    progress.write_text(json.dumps({"pending_series": {
        str(MATCH_ID): {
            "series_key": str(MATCH_ID), "series_url": f"dltv.org/matches/{MATCH_ID}",
            "last_scores": {"first": 0, "second": 0}, "updated_at": 1,
            "pending_map": {"match_id": MATCH_ID, "registered_at": 1},
        }
    }}))
    monkeypatch.setattr(cs, "ELO_LIVE_SNAPSHOT_AVAILABLE", True)
    monkeypatch.setattr(cs, "_elo_live_default_progress_path", progress)
    monkeypatch.setattr(cs, "_elo_live_finalize_series_from_scores", Mock())
    monkeypatch.setattr(cs, "_live_elo_winner_lookup", lambda *args: None)
    return progress


def test_stuck_orphan_fifty_sweeps_only_two_requests(pending_orphan, clock, monkeypatch):
    get = Mock(return_value=response())
    monkeypatch.setattr(cs.requests, "get", get)
    for cycle in range(50):
        clock[0] = NOW + cycle * 30
        assert cs._finalize_orphaned_live_elo_series(set()) == []
    assert get.call_count == 2, f"50 sweeps sent {get.call_count} outbound requests"


@pytest.mark.parametrize("reply", [response(), daily_response()])
@pytest.mark.parametrize("disabled_value", [None, "0", "false", "off", "no", "FaLsE", "OFF", " NO "])
def test_rollback_restores_one_request_per_sweep(pending_orphan, clock, monkeypatch, reply, disabled_value):
    if disabled_value is not None:
        monkeypatch.setenv("LIVE_ELO_ORPHAN_OD_BACKOFF", disabled_value)
    elif "LIVE_ELO_ORPHAN_OD_BACKOFF" not in os.environ:
        monkeypatch.setenv("LIVE_ELO_ORPHAN_OD_BACKOFF", "0")
    monkeypatch.setattr(cs, "LIVE_ELO_ORPHAN_OD_BACKOFF",
                        eval(BACKOFF_ENV_EXPRESSION, {"os": os}), raising=False)
    assert cs.LIVE_ELO_ORPHAN_OD_BACKOFF is False
    get = Mock(return_value=reply)
    monkeypatch.setattr(cs.requests, "get", get)
    for cycle in range(50):
        clock[0] = NOW + cycle * 30
        assert cs._finalize_orphaned_live_elo_series(set()) == []
    assert get.call_count == 50
    assert cs._OPENDOTA_ORPHAN_OD_RETRY == {}
    assert cs._OPENDOTA_ORPHAN_OD_DAY_PAUSE_UNTIL == 0


@pytest.mark.parametrize("enabled_value", [None, "", "1", "true", "on", "yes", "invalid", "2", "-1"])
def test_backoff_env_defaults_to_enabled(monkeypatch, enabled_value):
    if enabled_value is None:
        monkeypatch.delenv("LIVE_ELO_ORPHAN_OD_BACKOFF", raising=False)
    else:
        monkeypatch.setenv("LIVE_ELO_ORPHAN_OD_BACKOFF", enabled_value)
    assert eval(BACKOFF_ENV_EXPRESSION, {"os": os}) is True


@pytest.mark.parametrize("failure", [
    response(404), response(500), response(521), cs.requests.exceptions.ReadTimeout(),
    response(200, {"match_id": MATCH_ID}),
    response(200, {"match_id": OTHER_MATCH_ID, "radiant_win": True}),
    response(200, []),
    Mock(status_code=200, headers={}, json=Mock(side_effect=ValueError("bad JSON"))),
])
def test_all_failures_back_off_to_six_hour_cap(clock, monkeypatch, failure):
    get = Mock(side_effect=failure) if isinstance(failure, Exception) else Mock(return_value=failure)
    monkeypatch.setattr(cs.requests, "get", get)
    for attempt, delay in enumerate([600, 1800, 7200, 21600, 21600], start=1):
        assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None) is None
        assert get.call_count == attempt
        clock[0] += delay - 0.01
        out = {"unchanged": True}
        assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None, out) is None
        assert out == {"unchanged": True}
        assert get.call_count == attempt
        clock[0] += 0.01
    cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
    assert get.call_count == 6


@pytest.mark.parametrize("headers", [DAILY_429["headers"], {},
                                        {"x-rate-limit-remaining-day": "invalid"},
                                        {"X-Rate-Limit-Remaining-Day": "0"}])
def test_captured_daily_quota_blocks_all_orphans_until_utc_reset(clock, monkeypatch, headers, caplog):
    get = Mock(return_value=daily_response(headers))
    monkeypatch.setattr(cs.requests, "get", get)
    reset = (int(NOW) // 86400 + 1) * 86400 + 60
    with caplog.at_level(logging.WARNING):
        assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None) is None
        for second in [1, 60, 600, 7200, reset - NOW - 0.01]:
            clock[0] = NOW + second
            assert cs._fetch_finished_sourcetv_series_scores(OTHER_MATCH_ID, None) is None
            assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None) is None
    assert get.call_count == 1
    assert sum("daily quota paused until" in r.getMessage() for r in caplog.records) == 1
    assert any("HTTP 429" in r.getMessage() for r in caplog.records)
    clock[0] = reset
    cs._fetch_finished_sourcetv_series_scores(OTHER_MATCH_ID, None)
    assert get.call_count == 2


@pytest.mark.parametrize("remaining,blocked", [(299, True), (300, False), ("invalid", False)])
def test_success_preserves_result_and_reserves_daily_quota(clock, monkeypatch, remaining, blocked):
    payload = {"match_id": MATCH_ID, "radiant_win": True, "duration": 1981}
    get = Mock(return_value=response(200, payload, {"x-rate-limit-remaining-day": str(remaining)}))
    monkeypatch.setattr(cs.requests, "get", get)
    out = {}
    assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, {"first": 1}, out) == (2, 0)
    assert out == {"match_id": MATCH_ID, "radiant_win": True, "duration_seconds": 1981}
    cs._fetch_finished_sourcetv_series_scores(OTHER_MATCH_ID, None)
    assert get.call_count == (1 if blocked else 2)


def test_minute_quota_only_pauses_that_match_for_two_minutes(clock, monkeypatch):
    get = Mock(return_value=response(429, {"error": "rate limit exceeded"},
                                   {"x-rate-limit-remaining-day": "1500"}))
    monkeypatch.setattr(cs.requests, "get", get)
    cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
    cs._fetch_finished_sourcetv_series_scores(OTHER_MATCH_ID, None)
    assert get.call_count == 2
    clock[0] += 119.99
    cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
    assert get.call_count == 2
    clock[0] += 0.01
    cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
    assert get.call_count == 3


def test_success_clears_failure_history(clock, monkeypatch):
    get = Mock(return_value=response())
    monkeypatch.setattr(cs.requests, "get", get)
    cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
    clock[0] += 600
    get.return_value = response(200, {"match_id": MATCH_ID, "radiant_win": False})
    assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None) == (0, 1)
    assert MATCH_ID not in cs._OPENDOTA_ORPHAN_OD_RETRY
    get.return_value = response()
    cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
    assert get.call_count == 3
    clock[0] += 600
    cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
    assert get.call_count == 4


def test_retry_state_stays_bounded_without_clearing_all_backoffs(clock, monkeypatch):
    get = Mock(return_value=response())
    monkeypatch.setattr(cs.requests, "get", get)
    limit = cs._OPENDOTA_ORPHAN_LOG_MAX_KEYS
    for offset in range(limit + 10):
        cs._fetch_finished_sourcetv_series_scores(MATCH_ID + offset, None)
    assert get.call_count == limit + 10
    assert len(cs._OPENDOTA_ORPHAN_OD_RETRY) == limit
    cs._fetch_finished_sourcetv_series_scores(MATCH_ID + limit + 9, None)
    assert get.call_count == limit + 10
    assert cs._OPENDOTA_ORPHAN_OD_IN_FLIGHT == set()


def test_concurrent_sweeps_reserve_only_the_same_match(clock, monkeypatch):
    started = threading.Event()
    release = threading.Event()

    def get_response(url, **kwargs):
        if url.endswith(str(MATCH_ID)):
            started.set()
            assert release.wait(2), "mocked request was not released"
        return response()

    get = Mock(side_effect=get_response)
    monkeypatch.setattr(cs.requests, "get", get)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(cs._fetch_finished_sourcetv_series_scores, MATCH_ID, None)
        try:
            assert started.wait(2), "mocked request did not start"
            assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None) is None
            assert get.call_count == 1
            assert cs._fetch_finished_sourcetv_series_scores(OTHER_MATCH_ID, None) is None
            assert get.call_count == 2
        finally:
            release.set()
        assert future.result(timeout=2) is None
    assert cs._OPENDOTA_ORPHAN_OD_IN_FLIGHT == set()
    clock[0] += 600
    cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
    assert get.call_count == 3


def test_backoff_logs_once_per_match_pause_window(clock, monkeypatch, caplog):
    get = Mock(return_value=response())
    monkeypatch.setattr(cs.requests, "get", get)
    with caplog.at_level(logging.WARNING):
        cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
        for _ in range(50):
            cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
    assert get.call_count == 1
    assert sum("paused until" in r.getMessage() for r in caplog.records) == 1
    assert sum("failed: HTTP 404" in r.getMessage() for r in caplog.records) == 1


@pytest.mark.parametrize("reply", [
    response(200, {"match_id": MATCH_ID}, {"X-Rate-Limit-Remaining-Day": "1"}),
    response(429, None, {"x-rate-limit-remaining-day": "0"}),
])
def test_headers_protect_quota_even_without_a_parsed_body(clock, monkeypatch, reply):
    get = Mock(return_value=reply)
    monkeypatch.setattr(cs.requests, "get", get)
    assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None) is None
    assert cs._fetch_finished_sourcetv_series_scores(OTHER_MATCH_ID, None) is None
    assert get.call_count == 1


def test_rollback_ignores_an_existing_daily_pause(clock, monkeypatch):
    get = Mock(return_value=daily_response())
    monkeypatch.setattr(cs.requests, "get", get)
    cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None)
    assert cs._OPENDOTA_ORPHAN_OD_DAY_PAUSE_UNTIL > clock[0]
    monkeypatch.setattr(cs, "LIVE_ELO_ORPHAN_OD_BACKOFF", False)
    for _ in range(50):
        assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None) is None
    assert get.call_count == 51
