"""W-ACTIVATE: no-odds in-process Winline shadow hook (RED→GREEN).

Exclusive ownership of this file. Proves the production activation controller:
- eligible only under --no-odds (BOOKMAKER_PREFETCH_ENABLED=False)
- ordinary send/decision completes before the shadow seam is invoked
- exact context forwarding (incl. selected_side=None)
- same-key dedup / key change / in-flight suppression
- 30/60/120/240/300 capped terminal backoff with no sleep
- exception → atomic FAIL evidence + in-flight clear
- P1/P2-only PASS then one delayed candidate enrichment
- selected side maps only to ordered team1/team2
- activation path calls only run_winline_shadow_request; never Telegram /
  browser constructors / _run_shared_camoufox_job directly
"""
from __future__ import annotations

import ast
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = REPO_ROOT / "base"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402


DEFAULT_EVIDENCE = REPO_ROOT / ".hermes" / "runtime" / "winline-shadow" / "latest.json"
SERIES_URL = "dltv.org/matches/999001"
MATCH_KEY = f"{SERIES_URL}.12"
TEAM1 = "Alpha Squad"
TEAM2 = "Beta Force"


def _reset_activation_state(monkeypatch, tmp_path: Path) -> Path:
    out = tmp_path / "winline-shadow" / "latest.json"
    monkeypatch.setattr(cs, "WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH", out)
    monkeypatch.setattr(cs, "_winline_shadow_activation_state", {})
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)
    return out


def _fake_seam_factory(calls: List[Dict[str, Any]], *, rc: int = 0, raise_exc: Optional[BaseException] = None):
    def _fake(**kwargs):
        calls.append(dict(kwargs))
        if raise_exc is not None:
            raise raise_exc
        # Write a minimal seam-like record so activation can enrich/read if needed.
        path = Path(kwargs["output_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        selected = kwargs.get("selected_side")
        selected_norm = None
        if selected is not None:
            s = str(selected).strip().upper()
            if s in {"P1", "P2"}:
                selected_norm = s
            else:
                # production maps team names → P1/P2 before seam; tests may pass P1/P2
                selected_norm = s if s else None
        payload = {
            "schema_version": "winline_shadow_probe.v1",
            "match_id": "999001",
            "map_num": int(kwargs["map_num"]),
            "team1": kwargs["team1"],
            "team2": kwargs["team2"],
            "source": "Winline",
            "observed_at": float(kwargs.get("now") or time.time()) - 0.2,
            "collected_at": float(kwargs.get("now") or time.time()),
            "p1_odds": 1.85,
            "p2_odds": 2.05,
            "selected_side": selected_norm or "",
            "selected_odds": (
                1.85 if selected_norm == "P1" else (2.05 if selected_norm == "P2" else None)
            ),
            "verdict": "PASS" if rc == 0 else "FAIL",
            "failure_reasons": [] if rc == 0 else ["forced_fail"],
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        return rc

    return _fake


# ---------------------------------------------------------------------------
# Symbol / source-shape contracts
# ---------------------------------------------------------------------------


def test_activation_helpers_exist():
    assert hasattr(cs, "maybe_run_winline_shadow_activation")
    assert callable(cs.maybe_run_winline_shadow_activation)
    assert hasattr(cs, "queue_winline_shadow_activation")
    assert callable(cs.queue_winline_shadow_activation)
    assert hasattr(cs, "flush_winline_shadow_activation")
    assert callable(cs.flush_winline_shadow_activation)
    assert hasattr(cs, "WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH")


def test_activation_source_never_constructs_browser_or_sends_telegram():
    src = Path(cs.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    target_names = {
        "maybe_run_winline_shadow_activation",
        "queue_winline_shadow_activation",
        "flush_winline_shadow_activation",
        "_winline_shadow_activation_selected_side",
        "_winline_shadow_activation_write_fail",
        "_winline_shadow_activation_key",
        "_winline_shadow_activation_backoff_seconds",
    }
    found: Dict[str, ast.AST] = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in target_names:
            found[node.name] = node
    assert "maybe_run_winline_shadow_activation" in found
    assert "queue_winline_shadow_activation" in found
    assert "flush_winline_shadow_activation" in found

    forbidden_calls = {
        "send_message",
        "Camoufox",
        "subprocess",
        "Thread",
        "ThreadPoolExecutor",
        "_run_shared_camoufox_job",
        "run_sites_in_camoufox",
    }
    for name, node in found.items():
        for sub in ast.walk(node):
            if isinstance(sub, ast.Call):
                func = sub.func
                called = None
                if isinstance(func, ast.Name):
                    called = func.id
                elif isinstance(func, ast.Attribute):
                    called = func.attr
                if called in forbidden_calls:
                    pytest.fail(f"{name} must not call {called}")
            if isinstance(sub, ast.Name) and sub.id == "Camoufox":
                pytest.fail(f"{name} must not reference Camoufox")


# ---------------------------------------------------------------------------
# Eligibility / ordering / context
# ---------------------------------------------------------------------------


def test_no_odds_only_eligibility(monkeypatch, tmp_path):
    out = _reset_activation_state(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(cs, "run_winline_shadow_request", _fake_seam_factory(calls, rc=0))
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)

    rc = cs.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=True,
        now_monotonic=100.0,
    )
    assert rc is None
    assert calls == []
    assert not out.exists()

    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)
    rc2 = cs.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=True,
        now_monotonic=100.0,
    )
    assert rc2 == 0
    assert len(calls) == 1
    assert calls[0]["no_odds_active"] is True


def test_ordinary_path_must_complete_before_shadow(monkeypatch, tmp_path):
    _reset_activation_state(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(cs, "run_winline_shadow_request", _fake_seam_factory(calls, rc=0))

    rc = cs.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=2,
        team1=TEAM1,
        team2=TEAM2,
        selected_side="P1",
        ordinary_path_completed=False,
        now_monotonic=10.0,
    )
    assert rc is None
    assert calls == []

    rc2 = cs.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=2,
        team1=TEAM1,
        team2=TEAM2,
        selected_side="P1",
        ordinary_path_completed=True,
        now_monotonic=10.0,
    )
    assert rc2 == 0
    assert len(calls) == 1


def test_exact_context_forwarding_including_none_side(monkeypatch, tmp_path):
    out = _reset_activation_state(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(cs, "run_winline_shadow_request", _fake_seam_factory(calls, rc=0))

    cs.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=3,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=True,
        now_monotonic=50.0,
    )
    assert len(calls) == 1
    call = calls[0]
    assert call["match_key"] == MATCH_KEY
    assert call["map_num"] == 3
    assert call["team1"] == TEAM1
    assert call["team2"] == TEAM2
    assert call["selected_side"] is None or call["selected_side"] == ""
    assert call["no_odds_active"] is True
    assert Path(call["output_path"]) == out


def test_selected_side_maps_to_ordered_teams(monkeypatch, tmp_path):
    _reset_activation_state(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(cs, "run_winline_shadow_request", _fake_seam_factory(calls, rc=0))

    cs.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=TEAM1,
        ordinary_path_completed=True,
        now_monotonic=1.0,
    )
    assert calls[0]["selected_side"] == "P1"

    cs._winline_shadow_activation_state.clear()
    calls.clear()
    cs.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=2,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=TEAM2,
        ordinary_path_completed=True,
        now_monotonic=1.0,
    )
    assert calls[0]["selected_side"] == "P2"

    cs._winline_shadow_activation_state.clear()
    calls.clear()
    cs.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=3,
        team1=TEAM1,
        team2=TEAM2,
        selected_side="Unknown Team",
        ordinary_path_completed=True,
        now_monotonic=1.0,
    )
    assert calls[0]["selected_side"] is None or calls[0]["selected_side"] == ""


# ---------------------------------------------------------------------------
# Dedup / backoff / in-flight
# ---------------------------------------------------------------------------


def test_same_key_dedup_and_key_change_eligibility(monkeypatch, tmp_path):
    _reset_activation_state(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(cs, "run_winline_shadow_request", _fake_seam_factory(calls, rc=0))

    assert (
        cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=0.0,
        )
        == 0
    )
    assert (
        cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=100.0,
        )
        is None
    )
    assert len(calls) == 1

    # Different map → independently eligible
    assert (
        cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=2,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=100.0,
        )
        == 0
    )
    assert len(calls) == 2


def test_in_flight_suppression(monkeypatch, tmp_path):
    _reset_activation_state(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []

    def _slow(**kwargs):
        # Mark that a concurrent call should see in-flight and skip.
        calls.append(dict(kwargs))
        # Simulate concurrent re-entry while first is still running.
        nested = cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=5.0,
        )
        assert nested is None
        path = Path(kwargs["output_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "schema_version": "winline_shadow_probe.v1",
                    "verdict": "PASS",
                    "p1_odds": 1.9,
                    "p2_odds": 2.1,
                    "selected_side": "",
                    "selected_odds": None,
                    "team1": kwargs["team1"],
                    "team2": kwargs["team2"],
                    "map_num": kwargs["map_num"],
                    "source": "Winline",
                    "observed_at": 1.0,
                    "collected_at": 1.0,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(cs, "run_winline_shadow_request", _slow)
    assert (
        cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=5.0,
        )
        == 0
    )
    assert len(calls) == 1


def test_capped_terminal_backoff_no_sleep(monkeypatch, tmp_path):
    out = _reset_activation_state(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(cs, "run_winline_shadow_request", _fake_seam_factory(calls, rc=2))
    sleep_calls: List[float] = []
    monkeypatch.setattr(time, "sleep", lambda s: sleep_calls.append(s))

    # consecutive failures 1..5 → delays 30,60,120,240,300
    expected_delays = [30, 60, 120, 240, 300]
    t = 0.0
    for i, delay in enumerate(expected_delays):
        rc = cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=t,
        )
        assert rc == 2
        # immediate re-attempt must be suppressed
        assert (
            cs.maybe_run_winline_shadow_activation(
                match_key=MATCH_KEY,
                map_num=1,
                team1=TEAM1,
                team2=TEAM2,
                selected_side=None,
                ordinary_path_completed=True,
                now_monotonic=t + delay - 0.001,
            )
            is None
        )
        t = t + delay
    assert len(calls) == len(expected_delays)
    assert sleep_calls == []
    assert out.exists()


def test_exception_to_atomic_fail_and_guard_clear(monkeypatch, tmp_path):
    out = _reset_activation_state(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cs,
        "run_winline_shadow_request",
        _fake_seam_factory([], raise_exc=RuntimeError("boom")),
    )
    rc = cs.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=True,
        now_monotonic=0.0,
    )
    assert rc != 0
    assert out.exists()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"
    assert "boom" in str(data.get("failure_reasons") or data.get("acquisition_error") or "")
    # in-flight cleared → next attempt after backoff is allowed (not stuck forever)
    key = cs._winline_shadow_activation_key(MATCH_KEY, 1)
    st = cs._winline_shadow_activation_state[key]
    assert st.get("in_flight") is False


def test_p1p2_pass_then_one_delayed_candidate_enrichment(monkeypatch, tmp_path):
    _reset_activation_state(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(cs, "run_winline_shadow_request", _fake_seam_factory(calls, rc=0))

    # First: no candidate → P1/P2 PASS
    assert (
        cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=0.0,
        )
        == 0
    )
    assert len(calls) == 1
    assert calls[0]["selected_side"] in (None, "")

    # Before 30s enrichment window: still suppressed even if side appears
    assert (
        cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=TEAM1,
            ordinary_path_completed=True,
            now_monotonic=29.0,
        )
        is None
    )
    assert len(calls) == 1

    # After >=30s with a real selected side: one enrichment attempt
    assert (
        cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=TEAM1,
            ordinary_path_completed=True,
            now_monotonic=30.0,
        )
        == 0
    )
    assert len(calls) == 2
    assert calls[1]["selected_side"] == "P1"

    # Further duplicates suppressed after selected PASS
    assert (
        cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=TEAM1,
            ordinary_path_completed=True,
            now_monotonic=100.0,
        )
        is None
    )
    assert len(calls) == 2


def test_queue_then_flush_orders_after_ordinary_path(monkeypatch, tmp_path):
    """Production shape: queue during check_head, flush after ordinary path returns."""
    out = _reset_activation_state(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(cs, "run_winline_shadow_request", _fake_seam_factory(calls, rc=0))
    order: List[str] = []

    def _track(**kwargs):
        order.append("shadow")
        return _fake_seam_factory(calls, rc=0)(**kwargs)

    monkeypatch.setattr(cs, "run_winline_shadow_request", _track)

    # queue must not invoke seam yet
    cs.queue_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=TEAM2,
    )
    assert calls == []
    order.append("ordinary")
    cs.flush_winline_shadow_activation(now_monotonic=1.0)
    assert order == ["ordinary", "shadow"]
    assert len(calls) == 1
    assert calls[0]["selected_side"] == "P2"
    assert out.exists()


def test_invalid_map_num_skips(monkeypatch, tmp_path):
    _reset_activation_state(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(cs, "run_winline_shadow_request", _fake_seam_factory(calls, rc=0))
    assert (
        cs.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=None,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=0.0,
        )
        is None
    )
    assert calls == []
