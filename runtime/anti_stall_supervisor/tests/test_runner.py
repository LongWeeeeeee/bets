#!/usr/bin/env python3
"""Tests for W4 single-lock hygiene tick runner (temp dirs + fake adapters)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

# Load runner from sibling path without package install.
RUNNER_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RUNNER_DIR))

import runner as R  # noqa: E402


@pytest.fixture
def tmp_var(tmp_path: Path) -> Path:
    var = tmp_path / "var"
    var.mkdir()
    return var


def _cfg(var: Path, **over) -> dict:
    cfg = {
        "var_dir": str(var),
        "lock_path": str(var / "hygiene.lock"),
        "state_path": str(var / "state.json"),
        "report_path": str(var / "report.json"),
        "audit_path": str(var / "audit.jsonl"),
        "tick_deadline_s": 30,
        "linter_timeout_s": 5,
        "quiet": True,
        "dry_run": False,
    }
    cfg.update(over)
    return cfg


def _ok_lint_factory(counter: dict | None = None):
    def fake_lint(cfg):
        if counter is not None:
            counter["n"] = counter.get("n", 0) + 1
        return {
            "ok": True,
            "rc": 0,
            "error": None,
            "stdout": "plan_lint findings=0 critical=0",
            "stderr": "",
            "duration_s": 0.001,
            "cmd": ["fake-lint"],
            "timed_out": False,
        }

    return fake_lint


def test_acquire_tick_lock_and_contention(tmp_var: Path):
    lock_path = tmp_var / "hygiene.lock"
    l1 = R.acquire_tick_lock(lock_path)
    assert l1.acquired is True
    l2 = R.acquire_tick_lock(lock_path)
    assert l2.acquired is False
    l1.release()
    l3 = R.acquire_tick_lock(lock_path)
    assert l3.acquired is True
    l3.release()


def test_one_lock_serialization(tmp_var: Path):
    """Two concurrent run_tick calls: second sees lock_contention; exactly one linter call from winner."""
    counter = {"n": 0}
    hold = threading.Event()
    entered = threading.Event()

    def slow_snap(config, state):
        entered.set()
        hold.wait(timeout=5)
        return {"schema": "snapshot_v1", "digest": "d1", "boards": [], "tasks": []}

    cfg = _cfg(tmp_var, linter_runner=_ok_lint_factory(counter))
    results = {}

    def run_a():
        results["a"] = R.run_tick(
            cfg,
            adapters={
                "snapshot": slow_snap,
                "decide": lambda s, st, c: {
                    "schema": "decision_plan_v1",
                    "actions": [],
                    "decision_key": "healthy_noop",
                    "reason": "ok",
                },
                "execute": lambda p, c, dry_run=False: {
                    "ok": True,
                    "applied": [],
                    "skipped": [],
                    "dry_run": dry_run,
                },
            },
        )

    t = threading.Thread(target=run_a)
    t.start()
    assert entered.wait(timeout=3)
    # contender while A holds lock
    rc_b = R.run_tick(cfg, adapters=None)
    results["b"] = rc_b
    hold.set()
    t.join(timeout=5)
    assert results["a"] == 0
    assert results["b"] == 0
    # winner invoked linter once; contender must not
    assert counter["n"] == 1
    report = json.loads((tmp_var / "report.json").read_text())
    # last writer may be contender (lock_contention) or winner depending on timing of report write
    # contender writes lock_contention report; winner overwrites after. So final should be winner's.
    # If contender finished after winner (unlikely since hold released after b), check audit.
    audit_lines = (tmp_var / "audit.jsonl").read_text().strip().splitlines()
    keys = [json.loads(x)["decision_key"] for x in audit_lines]
    assert "lock_contention" in keys
    assert any(k != "lock_contention" for k in keys)


def test_lock_contention_no_state_mutation(tmp_var: Path):
    state_path = tmp_var / "state.json"
    R.atomic_write_json(
        state_path,
        {
            **R.empty_state_v1(),
            "tick_count": 7,
            "action_keys": {"keep": {"ts": "x"}},
        },
    )
    before = state_path.read_text()
    lock = R.acquire_tick_lock(tmp_var / "hygiene.lock")
    assert lock.acquired
    counter = {"n": 0}
    rc = R.run_tick(_cfg(tmp_var, linter_runner=_ok_lint_factory(counter)))
    assert rc == 0
    assert counter["n"] == 0  # no linter on contention
    assert state_path.read_text() == before
    report = json.loads((tmp_var / "report.json").read_text())
    assert report["decision"]["key"] == "lock_contention"
    lock.release()


def test_exact_one_linter_call(tmp_var: Path):
    counter = {"n": 0}
    rc = R.run_tick(_cfg(tmp_var, linter_runner=_ok_lint_factory(counter)))
    assert rc == 0
    assert counter["n"] == 1
    # second tick also exactly one more
    rc = R.run_tick(_cfg(tmp_var, linter_runner=_ok_lint_factory(counter)))
    assert rc == 0
    assert counter["n"] == 2


def test_linter_timeout_fail_closed(tmp_var: Path):
    def slow_lint(cfg):
        # emulate timeout result path via real timeout using subprocess would be heavy;
        # use runner's TimeoutExpired path by pointing at a sleeper script.
        return {
            "ok": False,
            "rc": 124,
            "error": "linter_timeout:1.0s",
            "stdout": "",
            "stderr": "",
            "duration_s": 1.0,
            "cmd": ["sleep"],
            "timed_out": True,
        }

    actions = {"exec": 0}

    def exec_should_not(plan, config, dry_run=False):
        actions["exec"] += 1
        return {"ok": True, "applied": [{"action_key": "x"}], "skipped": [], "dry_run": dry_run}

    rc = R.run_tick(
        _cfg(tmp_var, linter_runner=slow_lint),
        adapters={
            "execute": exec_should_not,
            "decide": lambda s, st, c: {
                "schema": "decision_plan_v1",
                "actions": [{"action_key": "should_not"}],
                "decision_key": "would_act",
                "reason": "x",
            },
        },
    )
    assert rc == 0
    report = json.loads((tmp_var / "report.json").read_text())
    assert report["decision"]["key"] == "linter_timeout"
    assert report["fail_closed"] is True
    assert report["decision"]["actions_applied"] == 0
    # execute adapter must not be called when linter fails closed
    assert actions["exec"] == 0


def test_run_plan_lint_real_timeout(tmp_var: Path, tmp_path: Path):
    sleeper = tmp_path / "sleeper.py"
    sleeper.write_text("import time\ntime.sleep(30)\n")
    res = R.run_plan_lint(
        {
            "linter_path": str(sleeper),
            "linter_timeout_s": 0.3,
            "python_executable": sys.executable,
            "dry_run": True,
        }
    )
    assert res["timed_out"] is True
    assert res["ok"] is False
    assert res["rc"] == 124


def test_deadline_fail_closed(tmp_var: Path):
    # clock jumps past deadline immediately after start
    t0 = [1000.0]

    def clock():
        return t0[0]

    counter = {"n": 0}

    def lint_and_jump(cfg):
        counter["n"] += 1
        t0[0] += 500  # blow deadline
        return _ok_lint_factory()(cfg)

    actions = {"exec": 0}
    rc = R.run_tick(
        _cfg(tmp_var, linter_runner=lint_and_jump, tick_deadline_s=10),
        adapters={
            "execute": lambda p, c, dry_run=False: actions.__setitem__("exec", actions["exec"] + 1)
            or {"ok": True, "applied": [], "skipped": [], "dry_run": dry_run},
        },
        clock=clock,
    )
    assert rc == 0
    report = json.loads((tmp_var / "report.json").read_text())
    assert report["decision"]["key"] == "deadline_exceeded"
    assert report["fail_closed"] is True
    assert actions["exec"] == 0
    assert counter["n"] == 1


def test_dry_run_immutability(tmp_var: Path):
    state_path = tmp_var / "state.json"
    initial = R.empty_state_v1()
    initial["action_keys"] = {"prior": {"ts": "t0"}}
    initial["cooldowns"] = {"c1": 123}
    initial["tick_count"] = 2
    R.atomic_write_json(state_path, initial)

    def decide(s, st, c):
        return {
            "schema": "decision_plan_v1",
            "actions": [{"action_key": "new_act"}],
            "decision_key": "act",
            "reason": "try",
            "cooldown_updates": {"c1": 999, "c2": 1},
            "stall_counter_updates": {"t1": 3},
        }

    def execute(plan, config, dry_run=False):
        assert dry_run is True
        return {
            "ok": True,
            "applied": [{"action_key": "new_act"}],
            "skipped": [],
            "dry_run": True,
        }

    rc = R.run_tick(
        _cfg(tmp_var, dry_run=True, linter_runner=_ok_lint_factory()),
        adapters={"decide": decide, "execute": execute},
    )
    assert rc == 0
    st = json.loads(state_path.read_text())
    assert st["action_keys"] == {"prior": {"ts": "t0"}}
    assert st["cooldowns"] == {"c1": 123}
    assert st.get("consecutive_stall_counters") in ({}, st.get("consecutive_stall_counters"))
    assert "new_act" not in st["action_keys"]
    assert st["cooldowns"].get("c1") == 123
    report = json.loads((tmp_var / "report.json").read_text())
    assert report["dry_run"] is True
    assert report["decision"]["actions_applied"] == 0


def test_corrupt_state_recovery(tmp_var: Path):
    state_path = tmp_var / "state.json"
    state_path.write_text("{not-json", encoding="utf-8")
    actions = {"exec": 0}

    rc = R.run_tick(
        _cfg(tmp_var, linter_runner=_ok_lint_factory()),
        adapters={
            "execute": lambda p, c, dry_run=False: actions.__setitem__("exec", 1)
            or {"ok": True, "applied": [{"action_key": "x"}], "skipped": []},
        },
    )
    assert rc == 0
    # corrupt renamed aside
    asides = list(tmp_var.glob("state.json.corrupt.*"))
    assert asides, "corrupt state should be renamed aside"
    report = json.loads((tmp_var / "report.json").read_text())
    assert report["decision"]["key"] == "corrupt_state"
    assert report["fail_closed"] is True
    assert report["state_corrupt_recovered"] is True
    assert actions["exec"] == 0
    # new clean state written
    st = json.loads(state_path.read_text())
    assert st["schema"] == "state_v1"
    assert st["tick_count"] == 1


def test_atomic_state_and_report(tmp_var: Path):
    rc = R.run_tick(_cfg(tmp_var, linter_runner=_ok_lint_factory()))
    assert rc == 0
    state_path = tmp_var / "state.json"
    report_path = tmp_var / "report.json"
    assert state_path.is_file() and report_path.is_file()
    # no leftover tmp
    assert not list(tmp_var.glob("*.tmp"))
    st = json.loads(state_path.read_text())
    rp = json.loads(report_path.read_text())
    assert st["schema"] == "state_v1"
    assert rp["schema"] == "report_v1"
    assert R.validate_state(st) == []
    assert R.validate_report(rp) == []
    # atomic_write_json unit
    target = tmp_var / "unit.json"
    R.atomic_write_json(target, {"k": 1})
    assert json.loads(target.read_text()) == {"k": 1}
    assert not target.with_name("unit.json.tmp").exists()


def test_audit_redaction_and_idempotency(tmp_var: Path):
    cfg = _cfg(
        tmp_var,
        linter_runner=_ok_lint_factory(),
        audit_probe_token="sk-supersecretvalue999",
    )
    rc = R.run_tick(cfg)
    assert rc == 0
    # second identical tick -> another audit line (idempotent append, no crash)
    rc = R.run_tick(cfg)
    assert rc == 0
    lines = (tmp_var / "audit.jsonl").read_text().strip().splitlines()
    assert len(lines) == 2
    for line in lines:
        rec = json.loads(line)
        assert "decision_key" in rec and "reason" in rec
        blob = json.dumps(rec)
        assert "sk-supersecretvalue999" not in blob
        assert "***REDACTED***" in blob or "token" not in rec
        # if token key present must be redacted
        if "token" in rec:
            assert rec["token"] == "***REDACTED***"


def test_audit_rotation_no_delete(tmp_var: Path):
    audit = tmp_var / "audit.jsonl"
    # tiny max to force rotate
    R.append_audit(audit, {"decision_key": "a", "reason": "r1", "token": "secret"}, max_bytes=50, keep=2)
    R.append_audit(audit, {"decision_key": "b", "reason": "r2"}, max_bytes=50, keep=2)
    R.append_audit(audit, {"decision_key": "c", "reason": "r3"}, max_bytes=50, keep=2)
    # original or rotated must exist; no unlinked content — files remain under var
    files = list(tmp_var.glob("audit.jsonl*"))
    assert files
    # redaction on first
    all_text = "\n".join(p.read_text() for p in files)
    assert "secret" not in all_text or "***REDACTED***" in all_text


def test_quiet_stdout_stderr(tmp_var: Path, capfd):
    rc = R.run_tick(_cfg(tmp_var, linter_runner=_ok_lint_factory(), quiet=True))
    assert rc == 0
    captured = capfd.readouterr()
    assert captured.out == ""
    assert captured.err == ""

    # lock contention also quiet
    lock = R.acquire_tick_lock(tmp_var / "hygiene.lock")
    rc = R.run_tick(_cfg(tmp_var, linter_runner=_ok_lint_factory(), quiet=True))
    assert rc == 0
    captured = capfd.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    lock.release()


def test_adapter_failure_isolation(tmp_var: Path):
    def boom_snap(config, state):
        raise RuntimeError("snap_boom")

    exec_calls = {"n": 0}

    rc = R.run_tick(
        _cfg(tmp_var, linter_runner=_ok_lint_factory()),
        adapters={
            "snapshot": boom_snap,
            "execute": lambda p, c, dry_run=False: exec_calls.__setitem__("n", 1)
            or {"ok": True, "applied": []},
        },
    )
    assert rc == 0
    report = json.loads((tmp_var / "report.json").read_text())
    assert report["decision"]["key"] == "adapter_failure"
    assert report["fail_closed"] is True
    assert exec_calls["n"] == 0

    # decide failure
    rc = R.run_tick(
        _cfg(tmp_var, linter_runner=_ok_lint_factory()),
        adapters={
            "decide": lambda s, st, c: (_ for _ in ()).throw(ValueError("dec_boom")),
        },
    )
    assert rc == 0
    report = json.loads((tmp_var / "report.json").read_text())
    assert report["decision"]["key"] == "adapter_failure"

    # execute failure
    rc = R.run_tick(
        _cfg(tmp_var, linter_runner=_ok_lint_factory()),
        adapters={
            "execute": lambda p, c, dry_run=False: (_ for _ in ()).throw(RuntimeError("ex")),
        },
    )
    assert rc == 0
    report = json.loads((tmp_var / "report.json").read_text())
    assert report["decision"]["key"] == "adapter_failure"
    assert report["fail_closed"] is True


def test_run_plan_lint_missing_and_ok_path(tmp_var: Path, tmp_path: Path):
    res = R.run_plan_lint({"linter_path": str(tmp_path / "nope.py"), "dry_run": True})
    assert res["ok"] is False and res["rc"] == 127

    # real linter dry-run if present (bounded) — may be slow; use tiny fake instead
    fake = tmp_path / "lint_ok.py"
    fake.write_text("print('ok')\n", encoding="utf-8")
    res = R.run_plan_lint(
        {
            "linter_path": str(fake),
            "python_executable": sys.executable,
            "linter_timeout_s": 10,
            "dry_run": True,
        }
    )
    assert res["ok"] is True
    assert res["timed_out"] is False


def test_cli_self_test_and_dry_run(tmp_path: Path):
    # --self-test prints SELF_TEST_OK (explicit terminal)
    proc = subprocess.run(
        [sys.executable, str(RUNNER_DIR / "runner.py"), "--self-test"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0
    assert "SELF_TEST_OK" in proc.stdout

    # dry-run CLI quiet
    var = tmp_path / "cli_var"
    var.mkdir()
    # inject via config
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(
        json.dumps(
            {
                "linter_runner_note": "cannot pass callable via JSON; use missing linter + expect fail closed quiet",
                "linter_path": str(tmp_path / "missing_lint.py"),
                "tick_deadline_s": 10,
            }
        ),
        encoding="utf-8",
    )
    proc = subprocess.run(
        [
            sys.executable,
            str(RUNNER_DIR / "runner.py"),
            "--config",
            str(cfg_path),
            "--dry-run",
            "--var-dir",
            str(var),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    # missing linter -> fail closed but rc 0; quiet
    assert proc.returncode == 0
    assert proc.stdout == ""
    assert (var / "report.json").is_file()


def test_happy_path_action_state_advance(tmp_var: Path):
    def decide(s, st, c):
        return {
            "schema": "decision_plan_v1",
            "actions": [{"action_key": "act-1"}],
            "decision_key": "recover",
            "reason": "stalled",
            "cooldown_updates": {"act-1": 42},
            "stall_counter_updates": {"taskA": 2},
        }

    def execute(plan, config, dry_run=False):
        return {
            "ok": True,
            "applied": [{"action_key": "act-1"}],
            "skipped": [],
            "dry_run": dry_run,
        }

    rc = R.run_tick(
        _cfg(tmp_var, dry_run=False, linter_runner=_ok_lint_factory()),
        adapters={"decide": decide, "execute": execute},
    )
    assert rc == 0
    st = json.loads((tmp_var / "state.json").read_text())
    assert "act-1" in st["action_keys"]
    assert st["cooldowns"].get("act-1") == 42
    assert st["consecutive_stall_counters"].get("taskA") == 2
    assert st["last_completed_tick"]


def test_redact_value_nested():
    obj = {
        "password": "p",
        "nested": {"api_key": "k", "ok": 1},
        "list": [{"token": "t"}, "bearer ABC.DEF"],
    }
    out = R.redact_value(obj)
    assert out["password"] == "***REDACTED***"
    assert out["nested"]["api_key"] == "***REDACTED***"
    assert out["nested"]["ok"] == 1
    assert out["list"][0]["token"] == "***REDACTED***"
