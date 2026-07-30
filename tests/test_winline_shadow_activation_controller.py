"""W-CTRL: deterministic no-sleep Winline shadow activation controller.

Exclusive ownership of this file + runtime/winline_shadow_activation.py.
Proves the controller (not cyberscore_try) owns:
- stable series|mapN canonical key
- injected seam callable (no cyberscore_try import)
- same-key in-flight / terminal-success dedup
- 30/60/120/240/300 capped failure backoff without sleep
- atomic evidence enrichment + PASS/FAIL validation
- fail-closed never-raise into caller
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import runtime.winline_shadow_activation as act  # noqa: E402


SERIES_URL = "dltv.org/matches/999001"
MATCH_KEY = f"{SERIES_URL}.12"
TEAM1 = "Alpha Squad"
TEAM2 = "Beta Force"
CANONICAL_MAP1 = f"{SERIES_URL}|map1"


def _reset(monkeypatch, tmp_path: Path) -> Path:
    out = tmp_path / "winline-shadow" / "latest.json"
    monkeypatch.setattr(act, "WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH", out)
    act.reset_winline_shadow_activation_state()
    return out


def _fake_seam_factory(
    calls: List[Dict[str, Any]],
    *,
    rc: int = 0,
    raise_exc: Optional[BaseException] = None,
    mutate: Optional[Dict[str, Any]] = None,
):
    def _fake(**kwargs):
        calls.append(dict(kwargs))
        if raise_exc is not None:
            raise raise_exc
        path = Path(kwargs["output_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        selected = kwargs.get("selected_side")
        selected_norm = None
        if selected is not None and str(selected).strip():
            s = str(selected).strip().upper()
            selected_norm = s if s in {"P1", "P2"} else s
        now = float(kwargs.get("now") or time.time())
        payload = {
            "schema_version": "winline_shadow_probe.v1",
            "match_id": "999001",
            "map_num": int(kwargs["map_num"]),
            "team1": kwargs["team1"],
            "team2": kwargs["team2"],
            "p1_team": kwargs["team1"],
            "p2_team": kwargs["team2"],
            "source": "Winline",
            "observed_at": now - 0.2,
            "collected_at": now,
            "p1_odds": 1.85,
            "p2_odds": 2.05,
            "selected_side": selected_norm or "",
            "selected_odds": (
                1.85 if selected_norm == "P1" else (2.05 if selected_norm == "P2" else None)
            ),
            "verdict": "PASS" if rc == 0 else "FAIL",
            "failure_reasons": [] if rc == 0 else ["forced_fail"],
        }
        if mutate:
            payload.update(mutate)
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
# Symbol / isolation contracts
# ---------------------------------------------------------------------------


def test_controller_public_symbols_exist():
    assert callable(act.maybe_run_winline_shadow_activation)
    assert callable(act.queue_winline_shadow_activation)
    assert callable(act.flush_winline_shadow_activation)
    assert callable(act.reset_winline_shadow_activation_state)
    assert callable(act._winline_shadow_activation_key)
    assert callable(act._winline_shadow_activation_backoff_seconds)
    assert hasattr(act, "WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH")
    assert hasattr(act, "_winline_shadow_activation_state")


def test_controller_module_does_not_import_cyberscore_try():
    import ast
    import runtime.winline_shadow_activation as mod

    src = Path(mod.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imported.add(node.module.split(".")[0])
    assert "cyberscore_try" not in imported
    assert "cyberscore" not in imported
    for name in ("cyberscore_try", "cs"):
        assert name not in mod.__dict__


def test_canonical_key_stable_series_map():
    assert act._winline_shadow_activation_key(MATCH_KEY, 1) == CANONICAL_MAP1
    assert act._winline_shadow_activation_key(f"{SERIES_URL}.99", 2) == f"{SERIES_URL}|map2"
    assert act._winline_shadow_activation_key(f"{SERIES_URL}|map3", 3) == f"{SERIES_URL}|map3"
    assert act._winline_shadow_activation_key("  " + SERIES_URL + "  ", 1) == CANONICAL_MAP1
    assert act._winline_shadow_activation_key(MATCH_KEY, 0) is None
    assert act._winline_shadow_activation_key(MATCH_KEY, 6) is None
    assert act._winline_shadow_activation_key("", 1) is None
    assert act._winline_shadow_activation_key(MATCH_KEY, None) is None


def test_backoff_schedule():
    assert act._winline_shadow_activation_backoff_seconds(1) == 30
    assert act._winline_shadow_activation_backoff_seconds(2) == 60
    assert act._winline_shadow_activation_backoff_seconds(3) == 120
    assert act._winline_shadow_activation_backoff_seconds(4) == 240
    assert act._winline_shadow_activation_backoff_seconds(5) == 300
    assert act._winline_shadow_activation_backoff_seconds(99) == 300


# ---------------------------------------------------------------------------
# Eligibility / context / side mapping
# ---------------------------------------------------------------------------


def test_requires_injected_seam_and_ordinary_path(monkeypatch, tmp_path):
    out = _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    # no seam injected → skip fail-closed
    rc = act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=True,
        now_monotonic=1.0,
    )
    assert rc is None
    assert calls == []
    assert not out.exists()

    seam = _fake_seam_factory(calls, rc=0)
    rc2 = act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=False,
        now_monotonic=1.0,
        run_winline_shadow_request=seam,
    )
    assert rc2 is None
    assert calls == []


def test_exact_context_forwarding_including_none_side(monkeypatch, tmp_path):
    out = _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    seam = _fake_seam_factory(calls, rc=0)
    rc = act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=3,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=True,
        now_monotonic=50.0,
        run_winline_shadow_request=seam,
        now=1234.5,
    )
    assert rc == 0
    assert len(calls) == 1
    call = calls[0]
    assert call["match_key"] == MATCH_KEY
    assert call["map_num"] == 3
    assert call["team1"] == TEAM1
    assert call["team2"] == TEAM2
    assert call["selected_side"] in (None, "")
    assert call["no_odds_active"] is True
    assert Path(call["output_path"]) == out
    assert call["now"] == 1234.5


def test_selected_side_maps_to_ordered_teams(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    seam = _fake_seam_factory(calls, rc=0)

    act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=TEAM1,
        ordinary_path_completed=True,
        now_monotonic=1.0,
        run_winline_shadow_request=seam,
    )
    assert calls[0]["selected_side"] == "P1"

    act.reset_winline_shadow_activation_state()
    calls.clear()
    act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=2,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=TEAM2,
        ordinary_path_completed=True,
        now_monotonic=1.0,
        run_winline_shadow_request=seam,
    )
    assert calls[0]["selected_side"] == "P2"

    act.reset_winline_shadow_activation_state()
    calls.clear()
    act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=3,
        team1=TEAM1,
        team2=TEAM2,
        selected_side="Unknown Team",
        ordinary_path_completed=True,
        now_monotonic=1.0,
        run_winline_shadow_request=seam,
    )
    assert calls[0]["selected_side"] in (None, "")


# ---------------------------------------------------------------------------
# Dedup / backoff / in-flight
# ---------------------------------------------------------------------------


def test_same_key_dedup_and_key_change_eligibility(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    seam = _fake_seam_factory(calls, rc=0)

    assert (
        act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=0.0,
            run_winline_shadow_request=seam,
        )
        == 0
    )
    assert (
        act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=100.0,
            run_winline_shadow_request=seam,
        )
        is None
    )
    assert len(calls) == 1

    # Different map → independently eligible
    assert (
        act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=2,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=100.0,
            run_winline_shadow_request=seam,
        )
        == 0
    )
    assert len(calls) == 2


def test_in_flight_suppression(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []

    def _slow(**kwargs):
        calls.append(dict(kwargs))
        nested = act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=5.0,
            run_winline_shadow_request=_slow,
        )
        assert nested is None
        path = Path(kwargs["output_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "schema_version": "winline_shadow_probe.v1",
                    "match_id": "999001",
                    "map_num": 1,
                    "team1": kwargs["team1"],
                    "team2": kwargs["team2"],
                    "p1_team": kwargs["team1"],
                    "p2_team": kwargs["team2"],
                    "source": "Winline",
                    "observed_at": 1.0,
                    "collected_at": 1.0,
                    "p1_odds": 1.9,
                    "p2_odds": 2.1,
                    "selected_side": "",
                    "selected_odds": None,
                    "verdict": "PASS",
                    "failure_reasons": [],
                }
            )
            + "\n",
            encoding="utf-8",
        )
        return 0

    assert (
        act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=5.0,
            run_winline_shadow_request=_slow,
        )
        == 0
    )
    assert len(calls) == 1


def test_capped_terminal_backoff_no_sleep(monkeypatch, tmp_path):
    out = _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    seam = _fake_seam_factory(calls, rc=2)
    sleep_calls: List[float] = []
    monkeypatch.setattr(time, "sleep", lambda s: sleep_calls.append(s))

    expected_delays = [30, 60, 120, 240, 300]
    t = 0.0
    for delay in expected_delays:
        rc = act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=t,
            run_winline_shadow_request=seam,
        )
        assert rc == 2
        assert (
            act.maybe_run_winline_shadow_activation(
                match_key=MATCH_KEY,
                map_num=1,
                team1=TEAM1,
                team2=TEAM2,
                selected_side=None,
                ordinary_path_completed=True,
                now_monotonic=t + delay - 0.001,
                run_winline_shadow_request=seam,
            )
            is None
        )
        t = t + delay
    assert len(calls) == len(expected_delays)
    assert sleep_calls == []
    assert out.exists()


def test_exception_to_atomic_fail_and_guard_clear(monkeypatch, tmp_path):
    out = _reset(monkeypatch, tmp_path)
    seam = _fake_seam_factory([], raise_exc=RuntimeError("boom"))
    rc = act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=True,
        now_monotonic=0.0,
        run_winline_shadow_request=seam,
    )
    assert rc != 0
    assert out.exists()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["verdict"] == "FAIL"
    assert "boom" in str(data.get("failure_reasons") or data.get("acquisition_error") or "")
    key = act._winline_shadow_activation_key(MATCH_KEY, 1)
    st = act._winline_shadow_activation_state[key]
    assert st.get("in_flight") is False
    # never raises
    assert isinstance(rc, int)


def test_p1p2_pass_then_one_delayed_candidate_enrichment(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    seam = _fake_seam_factory(calls, rc=0)

    assert (
        act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=0.0,
            run_winline_shadow_request=seam,
        )
        == 0
    )
    assert len(calls) == 1
    assert calls[0]["selected_side"] in (None, "")

    assert (
        act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=TEAM1,
            ordinary_path_completed=True,
            now_monotonic=29.0,
            run_winline_shadow_request=seam,
        )
        is None
    )
    assert len(calls) == 1

    assert (
        act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=TEAM1,
            ordinary_path_completed=True,
            now_monotonic=30.0,
            run_winline_shadow_request=seam,
        )
        == 0
    )
    assert len(calls) == 2
    assert calls[1]["selected_side"] == "P1"

    assert (
        act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=TEAM1,
            ordinary_path_completed=True,
            now_monotonic=100.0,
            run_winline_shadow_request=seam,
        )
        is None
    )
    assert len(calls) == 2


def test_queue_then_flush_orders_after_ordinary_path(monkeypatch, tmp_path):
    out = _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    order: List[str] = []

    def _track(**kwargs):
        order.append("shadow")
        return _fake_seam_factory(calls, rc=0)(**kwargs)

    act.queue_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=TEAM2,
        run_winline_shadow_request=_track,
    )
    assert calls == []
    order.append("ordinary")
    act.flush_winline_shadow_activation(now_monotonic=1.0)
    assert order == ["ordinary", "shadow"]
    assert len(calls) == 1
    assert calls[0]["selected_side"] == "P2"
    assert out.exists()


def test_invalid_map_num_skips(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    seam = _fake_seam_factory(calls, rc=0)
    assert (
        act.maybe_run_winline_shadow_activation(
            match_key=MATCH_KEY,
            map_num=None,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            ordinary_path_completed=True,
            now_monotonic=0.0,
            run_winline_shadow_request=seam,
        )
        is None
    )
    assert calls == []


# ---------------------------------------------------------------------------
# Evidence validation / enrichment
# ---------------------------------------------------------------------------


def test_pass_enriches_terminal_evidence(monkeypatch, tmp_path):
    out = _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    seam = _fake_seam_factory(calls, rc=0)
    rc = act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side="P1",
        ordinary_path_completed=True,
        now_monotonic=10.0,
        run_winline_shadow_request=seam,
        now=5000.0,
    )
    assert rc == 0
    data = json.loads(out.read_text(encoding="utf-8"))
    # capsule schema preserved
    assert data["schema_version"] == "winline_shadow_probe.v1"
    assert data["source"] == "Winline"
    assert data["verdict"] == "PASS"
    assert data["selected_side"] == "P1"
    assert data["selected_odds"] == 1.85
    assert data["p1_odds"] > 1 and data["p2_odds"] > 1
    # controller enrichment
    assert data["canonical_key"] == CANONICAL_MAP1
    assert data["controller_outcome"] == "PASS"
    assert data["producer_pid"] == os.getpid()
    assert "attempt_started_at" in data
    assert "attempt_finished_at" in data
    assert data.get("validation_reasons") in ([], None) or data["failure_reasons"] == []


def test_mismatched_selected_odds_is_fail(monkeypatch, tmp_path):
    out = _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    # Force wrong selected_odds for P1
    seam = _fake_seam_factory(
        calls,
        rc=0,
        mutate={"selected_side": "P1", "selected_odds": 2.05, "verdict": "PASS"},
    )
    rc = act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side="P1",
        ordinary_path_completed=True,
        now_monotonic=1.0,
        run_winline_shadow_request=seam,
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["controller_outcome"] == "FAIL"
    assert data["verdict"] == "FAIL"
    reasons = data.get("controller_failure_reasons") or data.get("failure_reasons") or []
    assert any("selected_odds" in str(r) for r in reasons)


def test_nonfinite_odds_is_fail(monkeypatch, tmp_path):
    out = _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []
    seam = _fake_seam_factory(
        calls,
        rc=0,
        mutate={"p1_odds": float("nan"), "verdict": "PASS"},
    )
    rc = act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=True,
        now_monotonic=1.0,
        run_winline_shadow_request=seam,
    )
    assert rc != 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["controller_outcome"] == "FAIL"


def test_missing_evidence_after_rc0_is_fail(monkeypatch, tmp_path):
    out = _reset(monkeypatch, tmp_path)
    calls: List[Dict[str, Any]] = []

    def _no_write(**kwargs):
        calls.append(dict(kwargs))
        return 0

    rc = act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=True,
        now_monotonic=1.0,
        run_winline_shadow_request=_no_write,
    )
    assert rc != 0
    assert out.exists()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["controller_outcome"] == "FAIL"
    assert data["verdict"] == "FAIL"


def test_never_raises_on_seam_or_write_errors(monkeypatch, tmp_path):
    out = _reset(monkeypatch, tmp_path)

    def _boom(**_kwargs):
        raise ValueError("hard fail")

    rc = act.maybe_run_winline_shadow_activation(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        ordinary_path_completed=True,
        now_monotonic=1.0,
        run_winline_shadow_request=_boom,
    )
    assert isinstance(rc, int) and rc != 0
    # next eligible after backoff
    key = act._winline_shadow_activation_key(MATCH_KEY, 1)
    st = act._winline_shadow_activation_state[key]
    assert st.get("in_flight") is False
    assert st.get("next_eligible_at", 0) > 1.0
