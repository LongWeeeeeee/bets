"""Winline overview thread must start on the first SourceTV cycle after a restart.

Incident 09.10.2026 (idea ingame-yxst, card ingame-0b1y): the pre-map Winline
price recorder runs inside the overview thread, and
`_ensure_winline_overview_refresher()` was called from the SourceTV reader
(`get_heads`) only AFTER the 'нет свежих матчей' early return. After each prod
restart (6-15 a day) no thread existed until the first live match: 0 listing
rows in 22 minutes after the 07:47 restart.

Contract: the ensure call happens on every SourceTV cycle, including an empty
bridge file, stale-only entries, a missing file and an unreadable json. The
function stays idempotent and gated by `_winline_first_active()`.

The bridge entry shape is copied from the captured-shape record used in
test_winline_first_admission.py (Mad Dogs League). Threads are never really
started with the real loop: the loop is replaced by a no-op.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


def _entry(ts: float) -> dict:
    return {
        "match_id": "mid-268",
        "series_id": "mid-268",
        "league_id": 17911,
        "league_name": "Mad Dogs League",
        "radiant_team_name": "Azure Dragons",
        "radiant_team_id": 0,
        "dire_team_name": "Stormriders",
        "dire_team_id": 0,
        "radiant_score": 0,
        "dire_score": 0,
        "radiant_series_wins": 0,
        "dire_series_wins": 0,
        "series_game_number": 1,
        "series_type": 1,
        "game_time": 100.0,
        "radiant_lead": 0,
        "timestamp": ts,
    }


@pytest.fixture
def ensure_calls(monkeypatch):
    calls: list[int] = []
    monkeypatch.setattr(runtime, "DLTV_SOURCE_MODE", "sourcetv")
    monkeypatch.setattr(
        runtime, "_ensure_winline_overview_refresher",
        lambda: calls.append(1),
    )
    return calls


def _run(monkeypatch, tmp_path, payload) -> None:
    path = tmp_path / "sourcetv_matches.json"
    if payload is not None:
        path.write_text(payload, encoding="utf-8")
    monkeypatch.setattr(runtime, "SOURCETV_MATCHES_PATH", str(path))
    heads, bodies = runtime.get_heads()
    assert heads == [] and bodies == []


def test_ensure_called_when_only_stale_entries(monkeypatch, tmp_path, ensure_calls):
    stale = {"mid-268": _entry(time.time() - 3600)}
    _run(monkeypatch, tmp_path, json.dumps(stale))
    assert ensure_calls == [1]


def test_ensure_called_when_bridge_empty(monkeypatch, tmp_path, ensure_calls):
    _run(monkeypatch, tmp_path, "{}")
    assert ensure_calls == [1]


def test_ensure_called_with_one_fresh_match(monkeypatch, tmp_path, ensure_calls):
    fresh = {"mid-268": _entry(time.time())}
    _run(monkeypatch, tmp_path, json.dumps(fresh))
    assert ensure_calls == [1]


def test_ensure_called_when_bridge_file_missing(monkeypatch, tmp_path, ensure_calls):
    _run(monkeypatch, tmp_path, None)
    assert ensure_calls == [1]


def test_ensure_called_when_bridge_json_unreadable(monkeypatch, tmp_path, ensure_calls):
    _run(monkeypatch, tmp_path, "{not json")
    assert ensure_calls == [1]


@pytest.fixture
def fake_thread_env(monkeypatch):
    started: list[int] = []

    def _noop_loop():
        started.append(1)

    monkeypatch.setattr(runtime, "_winline_overview_loop", _noop_loop)
    monkeypatch.setattr(runtime, "_winline_overview_thread", None)
    return started


def test_gate_off_creates_no_thread(monkeypatch, tmp_path, fake_thread_env):
    monkeypatch.setattr(runtime, "DLTV_SOURCE_MODE", "sourcetv")
    monkeypatch.setattr(runtime, "_winline_first_active", lambda: False)
    _run(monkeypatch, tmp_path, "{}")
    assert runtime._winline_overview_thread is None
    assert fake_thread_env == []


def test_gate_on_empty_bridge_starts_single_thread(monkeypatch, tmp_path, fake_thread_env):
    monkeypatch.setattr(runtime, "DLTV_SOURCE_MODE", "sourcetv")
    monkeypatch.setattr(runtime, "_winline_first_active", lambda: True)
    _run(monkeypatch, tmp_path, "{}")
    thread = runtime._winline_overview_thread
    assert thread is not None
    thread.join(timeout=5)
    assert fake_thread_env == [1]
