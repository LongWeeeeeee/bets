"""W-WATCHDOG-R4: bounded any-terminal shadow event watcher (TDD).

Exclusive ownership: this file + runtime/winline_shadow_event_watchdog.py.
Covers eligibility matrix, exit codes, atomic outputs, full-byte hash identity,
PID health/rollover, transient decode retry, empty stdout.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "runtime" / "winline_shadow_event_watchdog.py"
if str(REPO_ROOT / "runtime") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "runtime"))

import winline_shadow_event_watchdog as wd  # noqa: E402


SCHEMA = "winline_shadow_probe.v1"
STATE_SCHEMA = "winline_shadow_watchdog_state.v1"
TRIGGER_SCHEMA = "winline_shadow_watchdog_trigger.v1"


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    data = (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
        "utf-8"
    )
    _atomic_write_bytes(path, data)


def _base_record(**overrides: Any) -> Dict[str, Any]:
    rec: Dict[str, Any] = {
        "schema_version": SCHEMA,
        "canonical_key": "dltv.org/matches/900001|map1",
        "map_num": 1,
        "p1_team": "Alpha",
        "p2_team": "Beta",
        "team1": "Alpha",
        "team2": "Beta",
        "producer_pid": 4242,
        "attempt_finished_at": 1_700_100_000.5,
        "attempt_started_at": 1_700_099_990.0,
        "collected_at": 1_700_100_000.0,
        "observed_at": 1_700_100_000.0,
        "p1_odds": 1.85,
        "p2_odds": 2.05,
        "selected_side": "",
        "selected_odds": None,
        "source": "Winline",
        "verdict": "PASS",
        "controller_outcome": "PASS",
        "failure_reasons": [],
        "controller_failure_reasons": [],
        "seam_rc": 0,
    }
    rec.update(overrides)
    return rec


def _identity(rec: Dict[str, Any], raw: bytes) -> Tuple[int, str, float, str]:
    return (
        int(rec["producer_pid"]),
        str(rec["canonical_key"]),
        float(rec["attempt_finished_at"]),
        _sha256_bytes(raw),
    )


def _write_artifact(path: Path, rec: Dict[str, Any]) -> Tuple[bytes, str]:
    raw = (json.dumps(rec, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    _atomic_write_bytes(path, raw)
    return raw, _sha256_bytes(raw)


def _state_payload(identity: Optional[Tuple[int, str, float, str]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "schema_version": STATE_SCHEMA,
        "updated_at": 1_700_000_000.0,
    }
    if identity is None:
        out["last_identity"] = None
    else:
        pid, key, fin, sha = identity
        out["last_identity"] = {
            "producer_pid": int(pid),
            "canonical_key": str(key),
            "attempt_finished_at": float(fin),
            "artifact_sha256": str(sha),
        }
    return out


class FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


class FakeService:
    def __init__(
        self,
        *,
        active: bool = True,
        main_pid: int = 4242,
        start_snapshot: str = "snap-1",
    ) -> None:
        self.active = active
        self.main_pid = main_pid
        self.start_snapshot = start_snapshot
        self.calls = 0

    def probe(self) -> wd.ServiceSnapshot:
        self.calls += 1
        if not self.active:
            return wd.ServiceSnapshot(active=False, main_pid=None, start_snapshot=None)
        if self.main_pid is None or int(self.main_pid) <= 0:
            return wd.ServiceSnapshot(active=True, main_pid=None, start_snapshot=None)
        return wd.ServiceSnapshot(
            active=True,
            main_pid=int(self.main_pid),
            start_snapshot=str(self.start_snapshot),
        )


def _run(
    *,
    artifact: Path,
    state: Path,
    trigger: Path,
    service: FakeService,
    clock: FakeClock,
    poll_interval: float = 0.01,
    deadline_seconds: float = 0.05,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> int:
    sleeps: List[float] = []

    def _sleep(sec: float) -> None:
        sleeps.append(sec)
        clock.advance(sec)
        if sleep_fn is not None:
            sleep_fn(sec)

    cfg = wd.WatchConfig(
        artifact_path=artifact,
        state_path=state,
        trigger_path=trigger,
        poll_interval=poll_interval,
        deadline_seconds=deadline_seconds,
        unit="cyberscore.service",
    )
    return wd.run_watch(
        cfg,
        service_probe=service.probe,
        time_fn=clock,
        sleep_fn=_sleep,
    )


# ---------------------------------------------------------------------------
# Module surface
# ---------------------------------------------------------------------------


def test_module_exports_and_defaults() -> None:
    assert SCRIPT.is_file()
    assert wd.DEFAULT_POLL_INTERVAL == 15.0
    assert wd.DEFAULT_DEADLINE_SECONDS == 1200.0
    assert "winline-shadow" in str(wd.DEFAULT_ARTIFACT_PATH)
    assert wd.DEFAULT_ARTIFACT_PATH.name == "latest.json"
    assert wd.DEFAULT_STATE_PATH.name == "watchdog_state.json"
    assert "winline-shadow-live-trigger" in str(wd.DEFAULT_TRIGGER_PATH)


# ---------------------------------------------------------------------------
# Classification (non-gating)
# ---------------------------------------------------------------------------


def test_classify_fresh_p1p2() -> None:
    assert wd.classify_observation(_base_record()) == "fresh_p1p2"


def test_classify_market_closed_or_missing() -> None:
    assert (
        wd.classify_observation(
            _base_record(
                p1_odds=None,
                p2_odds=None,
                source="winline_map_market_missing",
                market_closed=True,
                verdict="FAIL",
                controller_outcome="FAIL",
            )
        )
        == "market_closed_or_missing"
    )
    assert (
        wd.classify_observation(
            _base_record(
                p1_odds=None,
                p2_odds=None,
                source="Winline",
                failure_reasons=["p1_odds_missing_or_malformed", "p2_odds_missing_or_malformed"],
                verdict="FAIL",
            )
        )
        == "market_closed_or_missing"
    )


def test_classify_parser_error() -> None:
    assert (
        wd.classify_observation(
            _base_record(
                p1_odds=None,
                p2_odds=None,
                source="Winline",
                seam_rc=5,
                failure_reasons=["seam_rc_5", "evidence_missing"],
                acquisition_error="timeout",
                verdict="FAIL",
            )
        )
        == "parser_error"
    )


def test_selected_side_absent_does_not_block_eligibility(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    rec = _base_record(selected_side="", selected_odds=None, producer_pid=7)
    _write_artifact(art, rec)
    clock = FakeClock()
    svc = FakeService(main_pid=7)
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=svc,
        clock=clock,
        deadline_seconds=0.05,
        poll_interval=0.01,
    )
    assert rc == 0
    payload = json.loads(trig.read_text(encoding="utf-8"))
    assert payload["classification"] == "fresh_p1p2"
    assert payload["observed_identity"]["canonical_key"] == rec["canonical_key"]


# ---------------------------------------------------------------------------
# Eligible terminal rewrites
# ---------------------------------------------------------------------------


def test_eligible_fresh_p1p2_writes_trigger_and_state(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    rec = _base_record(producer_pid=99, attempt_finished_at=2_000.0)
    raw, sha = _write_artifact(art, rec)
    clock = FakeClock(10.0)
    svc = FakeService(main_pid=99, start_snapshot="S99")
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=svc,
        clock=clock,
        deadline_seconds=1.0,
        poll_interval=0.05,
    )
    assert rc == 0
    assert trig.is_file()
    assert state.is_file()
    t = json.loads(trig.read_text(encoding="utf-8"))
    s = json.loads(state.read_text(encoding="utf-8"))
    assert t["schema_version"] == TRIGGER_SCHEMA
    assert t["classification"] == "fresh_p1p2"
    assert t["main_pid"] == 99
    assert t["main_pid_start_snapshot"] == "S99"
    assert t["observed_identity"]["artifact_sha256"] == sha
    assert t["observed_identity"]["producer_pid"] == 99
    assert t["baseline_identity"] is None
    assert t["watch_started_at"] == 10.0
    assert isinstance(t["observed_at"], float)
    assert s["schema_version"] == STATE_SCHEMA
    assert s["last_identity"]["artifact_sha256"] == sha
    assert s["last_identity"]["canonical_key"] == rec["canonical_key"]
    # full-byte identity
    assert s["last_identity"]["artifact_sha256"] == _sha256_bytes(art.read_bytes())


def test_eligible_market_closed_or_missing(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    rec = _base_record(
        producer_pid=11,
        p1_odds=None,
        p2_odds=None,
        market_closed=True,
        source="winline_map_market_missing",
        verdict="FAIL",
        controller_outcome="FAIL",
        failure_reasons=["p1_odds_missing_or_malformed"],
    )
    _write_artifact(art, rec)
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=FakeService(main_pid=11),
        clock=FakeClock(),
    )
    assert rc == 0
    assert json.loads(trig.read_text())["classification"] == "market_closed_or_missing"


def test_eligible_parser_error(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    rec = _base_record(
        producer_pid=12,
        p1_odds=None,
        p2_odds=None,
        source="Winline",
        seam_rc=2,
        acquisition_error="camoufox_failed",
        failure_reasons=["seam_rc_2", "acquisition_failed"],
        verdict="FAIL",
        controller_outcome="FAIL",
    )
    _write_artifact(art, rec)
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=FakeService(main_pid=12),
        clock=FakeClock(),
    )
    assert rc == 0
    assert json.loads(trig.read_text())["classification"] == "parser_error"


# ---------------------------------------------------------------------------
# Dedupe / invalid structural cases (no trigger)
# ---------------------------------------------------------------------------


def test_duplicate_baseline_not_eligible_timeout(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    rec = _base_record(producer_pid=5, attempt_finished_at=500.0)
    raw, sha = _write_artifact(art, rec)
    _atomic_write_json(state, _state_payload(_identity(rec, raw)))
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=FakeService(main_pid=5),
        clock=FakeClock(),
        deadline_seconds=0.04,
        poll_interval=0.01,
    )
    assert rc == 2
    assert not trig.exists()


def test_stale_nonfinite_finish_not_eligible(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    for bad in (math.nan, math.inf, -math.inf, None, "nope"):
        if trig.exists():
            trig.unlink()
        rec = _base_record(producer_pid=5, attempt_finished_at=bad)
        _write_artifact(art, rec)
        rc = _run(
            artifact=art,
            state=state,
            trigger=trig,
            service=FakeService(main_pid=5),
            clock=FakeClock(),
            deadline_seconds=0.03,
            poll_interval=0.01,
        )
        assert rc == 2, bad
        assert not trig.exists()


def test_invalid_map_team_schema_not_eligible(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    cases = [
        _base_record(schema_version="wrong", producer_pid=3),
        _base_record(canonical_key="", producer_pid=3),
        _base_record(map_num=0, producer_pid=3),
        _base_record(map_num=6, producer_pid=3),
        _base_record(p1_team="", producer_pid=3),
        _base_record(p2_team="Alpha", p1_team="Alpha", producer_pid=3),  # not distinct
        _base_record(p1_team="A", p2_team="", producer_pid=3),
    ]
    for rec in cases:
        if trig.exists():
            trig.unlink()
        _write_artifact(art, rec)
        rc = _run(
            artifact=art,
            state=state,
            trigger=trig,
            service=FakeService(main_pid=3),
            clock=FakeClock(),
            deadline_seconds=0.03,
            poll_interval=0.01,
        )
        assert rc == 2
        assert not trig.exists()


def test_pid_mismatch_not_eligible(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    rec = _base_record(producer_pid=100)
    _write_artifact(art, rec)
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=FakeService(main_pid=200),
        clock=FakeClock(),
        deadline_seconds=0.03,
        poll_interval=0.01,
    )
    assert rc == 2
    assert not trig.exists()


# ---------------------------------------------------------------------------
# Health / exit codes
# ---------------------------------------------------------------------------


def test_inactive_service_exit_3(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    _write_artifact(art, _base_record(producer_pid=1))
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=FakeService(active=False, main_pid=1),
        clock=FakeClock(),
    )
    assert rc == 3
    assert not trig.exists()


def test_no_mainpid_exit_3(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    _write_artifact(art, _base_record(producer_pid=1))
    svc = FakeService(active=True, main_pid=0)
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=svc,
        clock=FakeClock(),
    )
    assert rc == 3
    assert not trig.exists()


def test_pid_rollover_during_watch_exit_3(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    # Start with missing artifact so we poll, then flip PID before eligible write.
    svc = FakeService(main_pid=50, start_snapshot="A")
    clock = FakeClock()

    def on_sleep(_sec: float) -> None:
        if svc.calls >= 2:
            svc.main_pid = 51
            svc.start_snapshot = "B"
            _write_artifact(art, _base_record(producer_pid=51, attempt_finished_at=9_999.0))

    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=svc,
        clock=clock,
        deadline_seconds=1.0,
        poll_interval=0.01,
        sleep_fn=on_sleep,
    )
    assert rc == 3
    assert not trig.exists()


def test_timeout_exit_2(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    # No artifact ever appears.
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=FakeService(main_pid=8),
        clock=FakeClock(),
        deadline_seconds=0.04,
        poll_interval=0.01,
    )
    assert rc == 2
    assert not trig.exists()


def test_invalid_args_exit_4() -> None:
    assert wd.main(["--deadline-seconds", "-1"]) == 4
    assert wd.main(["--poll-interval", "0"]) == 4
    assert wd.main(["--poll-interval", "not-a-float"]) == 4


def test_invalid_durable_state_schema_exit_4(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    _write_artifact(art, _base_record(producer_pid=4))
    _atomic_write_json(state, {"schema_version": "nope", "last_identity": {}})
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=FakeService(main_pid=4),
        clock=FakeClock(),
    )
    assert rc == 4
    assert not trig.exists()


def test_invalid_state_identity_shape_exit_4(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    _write_artifact(art, _base_record(producer_pid=4))
    _atomic_write_json(
        state,
        {
            "schema_version": STATE_SCHEMA,
            "last_identity": {"producer_pid": "x"},  # incomplete/invalid
            "updated_at": 1.0,
        },
    )
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=FakeService(main_pid=4),
        clock=FakeClock(),
    )
    assert rc == 4


# ---------------------------------------------------------------------------
# Atomic read / hash / transient retry
# ---------------------------------------------------------------------------


def test_full_byte_hash_identity_sensitive_to_bytes(tmp_path: Path) -> None:
    rec = _base_record(producer_pid=1)
    a = (json.dumps(rec, sort_keys=True) + "\n").encode()
    b = (json.dumps(rec, sort_keys=True, indent=2) + "\n").encode()
    assert json.loads(a) == json.loads(b)
    assert _sha256_bytes(a) != _sha256_bytes(b)
    # module identity uses full raw bytes
    id_a = wd.identity_from_artifact(rec, a)
    id_b = wd.identity_from_artifact(rec, b)
    assert id_a.artifact_sha256 != id_b.artifact_sha256


def test_transient_decode_retry_then_success(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    # Start with garbage (race), then rewrite valid on first sleep.
    _atomic_write_bytes(art, b"{not-json")
    rec = _base_record(producer_pid=77, attempt_finished_at=3_333.0)
    clock = FakeClock()
    svc = FakeService(main_pid=77)

    def on_sleep(_sec: float) -> None:
        if not trig.exists():
            _write_artifact(art, rec)

    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=svc,
        clock=clock,
        deadline_seconds=1.0,
        poll_interval=0.01,
        sleep_fn=on_sleep,
    )
    assert rc == 0
    assert trig.is_file()


def test_atomic_outputs_use_replace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    rec = _base_record(producer_pid=9)
    _write_artifact(art, rec)
    replaces: List[Tuple[str, str]] = []
    real_replace = os.replace

    def tracking_replace(src: str, dst: str) -> None:
        replaces.append((str(src), str(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(wd.os, "replace", tracking_replace)
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=FakeService(main_pid=9),
        clock=FakeClock(),
    )
    assert rc == 0
    dests = {Path(d).name for _, d in replaces}
    assert "trigger.json" in dests or any(Path(d) == trig for _, d in replaces)
    assert "state.json" in dests or any(Path(d) == state for _, d in replaces)
    assert all(Path(s).name.endswith(".tmp") or ".tmp" in Path(s).name for s, _ in replaces)


def test_new_identity_after_baseline_triggers(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    old = _base_record(producer_pid=2, attempt_finished_at=100.0, canonical_key="k|map1")
    raw_old, sha_old = _write_artifact(art, old)
    _atomic_write_json(state, _state_payload(_identity(old, raw_old)))
    # Newer rewrite with different finish + content
    new = _base_record(
        producer_pid=2,
        attempt_finished_at=200.0,
        canonical_key="k|map2",
        map_num=2,
        p1_odds=None,
        p2_odds=None,
        market_closed=True,
        source="winline_map_market_missing",
        verdict="FAIL",
    )
    raw_new, sha_new = _write_artifact(art, new)
    assert sha_new != sha_old
    rc = _run(
        artifact=art,
        state=state,
        trigger=trig,
        service=FakeService(main_pid=2),
        clock=FakeClock(),
    )
    assert rc == 0
    t = json.loads(trig.read_text())
    assert t["baseline_identity"]["artifact_sha256"] == sha_old
    assert t["observed_identity"]["artifact_sha256"] == sha_new
    assert t["classification"] == "market_closed_or_missing"


# ---------------------------------------------------------------------------
# CLI / stdout empty
# ---------------------------------------------------------------------------


def test_cli_stdout_empty_on_success(tmp_path: Path) -> None:
    art = tmp_path / "latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    rec = _base_record(producer_pid=4242)
    _write_artifact(art, rec)
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--artifact",
            str(art),
            "--state",
            str(state),
            "--trigger",
            str(trig),
            "--poll-interval",
            "0.01",
            "--deadline-seconds",
            "0.5",
            "--bind-main-pid",
            "4242",
            "--bind-start-snapshot",
            "cli-snap",
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert proc.stdout == ""
    assert trig.is_file()


def test_cli_stdout_empty_on_timeout(tmp_path: Path) -> None:
    art = tmp_path / "missing-latest.json"
    state = tmp_path / "state.json"
    trig = tmp_path / "trigger.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--artifact",
            str(art),
            "--state",
            str(state),
            "--trigger",
            str(trig),
            "--poll-interval",
            "0.01",
            "--deadline-seconds",
            "0.05",
            "--bind-main-pid",
            "1",
            "--bind-start-snapshot",
            "x",
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2
    assert proc.stdout == ""


def test_cli_invalid_args_stdout_empty() -> None:
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--poll-interval", "-5"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 4
    assert proc.stdout == ""


def test_production_defaults_preserved_in_parser() -> None:
    ns = wd.parse_args([])
    assert ns.poll_interval == 15.0
    assert ns.deadline_seconds == 1200.0
    assert Path(ns.artifact) == wd.DEFAULT_ARTIFACT_PATH
    assert Path(ns.state) == wd.DEFAULT_STATE_PATH
    assert Path(ns.trigger) == wd.DEFAULT_TRIGGER_PATH
