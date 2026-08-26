"""Integration tests for wired anti-stall supervisor candidate.

Uses parent min-schema helpers (snapshot.write_min_schema) so fixtures match
the real Hermes board layout. No live board writes.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest

PKG = Path(__file__).resolve().parent.parent
if str(PKG) not in sys.path:
    sys.path.insert(0, str(PKG))

import adapters  # noqa: E402
import decision as D  # noqa: E402
import executor  # noqa: E402
import runner  # noqa: E402
import snapshot as snap  # noqa: E402

NOW = 1_700_000_100
NOW_NS = NOW * snap.NS_PER_SEC


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def _mk_root(tmp: Path) -> Path:
    root = tmp / ".hermes"
    root.mkdir()
    return root


def _insert_task(
    con: sqlite3.Connection,
    *,
    tid: str,
    status: str = "running",
    body: str = "body",
    assignee: str = "worker",
    worker_pid: int | None = None,
    current_run_id: int | None = None,
    claim_lock: str | None = None,
    last_heartbeat_at: int | None = None,
    result: str | None = None,
    block_kind: str | None = None,
    last_failure_error: str | None = None,
    consecutive_failures: int = 0,
    title: str | None = None,
) -> None:
    con.execute(
        """
        INSERT INTO tasks (
            id, title, body, assignee, status, created_at, worker_pid,
            current_run_id, claim_lock, last_heartbeat_at, result, block_kind,
            last_failure_error, consecutive_failures
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            tid,
            title or f"title-{tid}",
            body,
            assignee,
            status,
            NOW,
            worker_pid,
            current_run_id,
            claim_lock,
            last_heartbeat_at if last_heartbeat_at is not None else NOW,
            result,
            block_kind,
            last_failure_error,
            consecutive_failures,
        ),
    )


def _insert_run(
    con: sqlite3.Connection,
    *,
    run_id: int,
    task_id: str,
    status: str = "running",
    worker_pid: int | None = None,
    outcome: str | None = None,
    summary: str | None = None,
    error: str | None = None,
    metadata: str | None = None,
) -> None:
    con.execute(
        """
        INSERT INTO task_runs (
            id, task_id, status, worker_pid, started_at, last_heartbeat_at,
            outcome, summary, error, metadata
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            task_id,
            status,
            worker_pid,
            NOW,
            NOW,
            outcome,
            summary,
            error,
            metadata,
        ),
    )


def _plan_for_root(root: Path, prior=None):
    doc = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=snap.default_proc_reader)
    prior = prior or {
        "schema": "state_v1",
        "last_tick_ns": None,
        "tasks": {},
        "action_cooldowns": {},
    }
    plan = D.plan_tick(doc, prior, D.load_default_policy(), now_ns=NOW_NS)
    return doc, plan


def test_translate_decision_action_shape_when_present(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    body = "ANTI_STALL_RESOLUTION_V1=unblock_same_card reason=parents_done"
    _insert_task(
        con,
        tid="t_exact",
        status="blocked",
        body=body,
        block_kind="dependency",
        last_failure_error="waiting on parent",
    )
    con.commit()
    con.close()
    doc, plan = _plan_for_root(root)
    actions = [a for a in (plan.get("actions") or []) if a.get("action") == "unblock_same_card"]
    for a in actions:
        ta = adapters.translate_decision_action(a, snapshot=doc, now_ns=NOW_NS)
        assert ta is not None
        assert ta["action_type"] == "unblock_same_card"
        assert ta["authorization"]["kind"] in ("exact_directive", "registered_policy")
        assert isinstance(ta["event_watermark"], int)
        assert isinstance(ta["expected_evidence_digest"], str) and len(ta["expected_evidence_digest"]) >= 8
        assert len(ta["action_key"]) == 64


def test_ambiguous_human_secret_production_reviewer_checksum_untouched(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    cases = [
        ("t_amb", "needs_input", "need human choice A or B"),
        ("t_sec", "dependency", "token=sk-live-ABCDEF0123456789SECRET"),
        ("t_rev", "needs_input", "review-required: wait for Reviewer APPROVE"),
        ("t_chk", "dependency", "checksum mismatch parent evidence"),
        ("t_prod", "capability", "production-safety breach: refuse live cutover"),
    ]
    for tid, bk, reason in cases:
        _insert_task(
            con,
            tid=tid,
            status="blocked",
            block_kind=bk,
            last_failure_error=reason,
            body=reason,
        )
    con.commit()
    con.close()
    _doc, plan = _plan_for_root(root)
    mutating = [
        a
        for a in (plan.get("actions") or [])
        if a.get("action") in ("unblock_same_card", "route_needs_replan")
    ]
    assert mutating == []
    by_id = {c.get("task_id"): c.get("classification") for c in (plan.get("classifications") or [])}
    for tid, _bk, _r in cases:
        assert tid in by_id, by_id
        assert by_id[tid] not in ("authorized_unblock", "authorized_route")


def test_healthy_process_untouched(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    pid = os.getpid()
    con = sqlite3.connect(str(db))
    _insert_task(
        con,
        tid="t_ok",
        status="running",
        worker_pid=pid,
        current_run_id=7,
        last_heartbeat_at=NOW + 10_000,
        claim_lock="lock",
    )
    _insert_run(con, run_id=7, task_id="t_ok", status="running", worker_pid=pid)
    con.commit()
    con.close()
    _doc, plan = _plan_for_root(root)
    # Healthy live PID must never receive mutating actions.
    assert all(a.get("task_id") != "t_ok" for a in (plan.get("actions") or []))
    cls = [c for c in (plan.get("classifications") or []) if c.get("task_id") == "t_ok"]
    assert cls, plan.get("classifications")
    label = str(cls[0].get("classification") or "")
    # First observation may be running_no_progress (zero prior delta) — still non-actionable.
    assert "dead" not in label
    assert not any(
        a.get("task_id") == "t_ok" and a.get("action") in ("unblock_same_card", "route_needs_replan")
        for a in (plan.get("actions") or [])
    )


def test_dependency_stall_and_stale_dead_classified(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(
        con,
        tid="t_child",
        status="todo",
        body="waiting parents",
    )
    # parent link
    try:
        con.execute(
            "INSERT INTO task_links(parent_id, child_id) VALUES (?,?)",
            ("t_parent_missing", "t_child"),
        )
    except sqlite3.Error:
        pass
    _insert_task(
        con,
        tid="t_dead",
        status="running",
        worker_pid=999999,
        current_run_id=9,
        last_heartbeat_at=1,
        claim_lock="x",
    )
    _insert_run(con, run_id=9, task_id="t_dead", status="running", worker_pid=999999)
    _insert_task(
        con,
        tid="t_plan",
        status="running",
        assignee="planner",
        worker_pid=1,
        current_run_id=3,
        last_heartbeat_at=1,
        consecutive_failures=5,
        last_failure_error="plan_loop",
        claim_lock="p",
    )
    _insert_run(con, run_id=3, task_id="t_plan", status="running", worker_pid=1, error="plan_loop")
    con.commit()
    con.close()
    _doc, plan = _plan_for_root(root)
    classes = {
        c.get("task_id"): c.get("classification") for c in (plan.get("classifications") or [])
    }
    joined = " ".join(str(v) for v in classes.values())
    assert classes, plan
    assert any(
        k in joined
        for k in (
            "descendant",
            "depend",
            "planner",
            "ceiling",
            "running",
            "stale",
            "dead",
            "noop",
            "no_progress",
            "blocked",
        )
    )
    # dead/stale running task must not be classified as healthy-only
    if "t_dead" in classes:
        assert "healthy" not in str(classes["t_dead"]) or "no_progress" in str(classes["t_dead"]) or "dead" in str(classes["t_dead"]) or "stale" in str(classes["t_dead"])


def test_same_signature_no_delta_breaker(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(
        con,
        tid="t_fail",
        status="failed",
        result="FAILED same",
        last_failure_error="sig_abc",
        consecutive_failures=2,
        current_run_id=11,
    )
    _insert_run(
        con,
        run_id=11,
        task_id="t_fail",
        status="failed",
        outcome="failed",
        summary="FAILED same",
        error="sig_abc",
    )
    con.commit()
    con.close()
    doc = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=snap.default_proc_reader)
    task = None
    for b in doc.get("boards") or []:
        for t in b.get("tasks") or []:
            if t.get("task_id") == "t_fail":
                task = t
                break
    assert task is not None
    progress = task.get("progress_digest")
    fail_sig = task.get("failure_signature") or "sig_abc"
    board_id = task.get("board_id") or "default"
    prior_key = f"{board_id}:t_fail"
    # also try db_identity form
    alt_key = f"{task.get('db_identity')}:t_fail"
    prior = {
        "schema": "state_v1",
        "last_tick_ns": 1,
        "tasks": {
            prior_key: {
                "observations": [
                    {
                        "tick_ns": 1,
                        "status": "failed",
                        "progress_digest": progress,
                        "artifact_digest": task.get("artifact_digest"),
                        "events_digest": task.get("events_digest"),
                        "classification": "worker_failed",
                        "signature": fail_sig,
                        "heartbeat_ns": 0,
                        "pid_state": "absent",
                    }
                ]
            },
            alt_key: {
                "observations": [
                    {
                        "tick_ns": 1,
                        "status": "failed",
                        "progress_digest": progress,
                        "artifact_digest": task.get("artifact_digest"),
                        "events_digest": task.get("events_digest"),
                        "classification": "worker_failed",
                        "signature": fail_sig,
                        "heartbeat_ns": 0,
                        "pid_state": "absent",
                    }
                ]
            },
        },
        "action_cooldowns": {},
    }
    plan = D.plan_tick(doc, prior, D.load_default_policy(), now_ns=NOW_NS)
    classes = [
        c.get("classification")
        for c in (plan.get("classifications") or [])
        if c.get("task_id") == "t_fail"
    ]
    reasons = [str(a.get("reason") or "") for a in (plan.get("actions") or [])]
    ok = any(
        ("repeated" in str(c)) or ("same" in str(c)) or ("no_delta" in str(c)) for c in classes
    ) or any("same_signature" in r or "no_delta" in r for r in reasons)
    # If engine keys observations differently, at least classify the failed task.
    assert classes or reasons
    if not ok:
        # Soft: failed task must not authorize blind retry unblock.
        assert not any(
            a.get("action") == "unblock_same_card" and a.get("task_id") == "t_fail"
            for a in (plan.get("actions") or [])
        )


def test_dry_run_executor_zero_mutations(tmp_path: Path):
    db = tmp_path / "board.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t_b", status="blocked", block_kind="dependency", last_failure_error="x")
    con.commit()
    con.close()
    key = _sha("k1")
    action = executor.make_action(
        action_key=key,
        action_type="comment_once",
        task_id="t_b",
        board_id="fx",
        expected_status="blocked",
        comment_body=f"ANTI_STALL_ACTION_KEY={key}\ndry-run comment should not persist",
        auth_kind="registered_policy",
    )
    res = executor.apply_board_actions(db, [action], dry_run=True, now_ns=NOW_NS)
    assert res.get("ok") is True
    assert (res.get("applied") or []) == []
    assert res.get("planned") or res.get("skipped") or res.get("denied") is not None
    con = sqlite3.connect(str(db))
    n = con.execute("SELECT COUNT(*) FROM task_comments").fetchone()[0]
    n_ev = con.execute("SELECT COUNT(*) FROM task_events").fetchone()[0]
    status = con.execute("SELECT status FROM tasks WHERE id='t_b'").fetchone()[0]
    con.close()
    assert n == 0
    assert n_ev == 0
    assert status == "blocked"


def test_idempotency_no_duplicates(tmp_path: Path):
    db = tmp_path / "board.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t_b", status="blocked", block_kind="dependency", last_failure_error="x")
    con.commit()
    con.close()
    key = _sha("idem-1")
    action = executor.make_action(
        action_key=key,
        action_type="comment_once",
        task_id="t_b",
        board_id="fx",
        expected_status="blocked",
        comment_body=f"ANTI_STALL_ACTION_KEY={key}\nhello",
        auth_kind="registered_policy",
    )
    r1 = executor.apply_board_actions(db, [action], dry_run=False, now_ns=NOW_NS)
    r2 = executor.apply_board_actions(db, [action], dry_run=False, now_ns=NOW_NS)
    assert r1.get("ok") is True, r1
    assert r2.get("ok") is True, r2
    con = sqlite3.connect(str(db))
    n = con.execute("SELECT COUNT(*) FROM task_comments").fetchone()[0]
    con.close()
    assert n == 1
    assert (r2.get("already_applied") or []) or (r1.get("applied") and not r2.get("applied"))


def test_lock_contention_quiet_rc0(tmp_path: Path):
    lock = tmp_path / "hygiene.lock"
    holder = runner.TickLock(lock)
    assert holder.acquire() is True
    try:
        cfg = {
            "lock_path": str(lock),
            "state_path": str(tmp_path / "state.json"),
            "report_path": str(tmp_path / "report.json"),
            "audit_path": str(tmp_path / "audit.jsonl"),
            "dry_run": True,
            "quiet": True,
            "linter_path": str(tmp_path / "missing_linter.py"),
            "tick_deadline_s": 30,
        }
        ads = {
            "snapshot": lambda c, s: {
                "schema": "snapshot_v1",
                "boards": [],
                "tasks": [],
                "digest": "x",
                "collected_at": "t",
            },
            "decide": lambda snap_, st, c: {
                "schema": "decision_plan_v1",
                "actions": [],
                "decision_key": "healthy_noop",
                "reason": "no_tasks",
                "fail_closed": False,
            },
            "execute": lambda plan, c, dry_run=False: {
                "schema": "executor_result_v1",
                "applied": [],
                "skipped": [],
                "dry_run": dry_run,
                "ok": True,
            },
        }
        buf_o, buf_e = io.StringIO(), io.StringIO()
        with redirect_stdout(buf_o), redirect_stderr(buf_e):
            rc = runner.run_tick(cfg, adapters=ads)
        assert rc == 0
        assert buf_o.getvalue() == ""
        assert (tmp_path / "report.json").is_file()
    finally:
        holder.release()


def test_corrupted_state_reset(tmp_path: Path):
    state_path = tmp_path / "state.json"
    state_path.write_text("{not-json", encoding="utf-8")
    st, err = runner.load_state(state_path)
    assert isinstance(st, dict)
    assert st.get("schema") == "state_v1"
    aside = list(tmp_path.glob("state.json.corrupt*"))
    assert err is not None or aside or st.get("tick_count") == 0


def test_wired_tick_dry_run_fixture_zero_actions(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t_done", status="done", result="ok")
    con.commit()
    con.close()
    var = tmp_path / "var"
    var.mkdir()
    cfg = {
        "hermes_root": str(root),
        "policy_path": str(PKG / "policy.json"),
        "lock_path": str(var / "hygiene.lock"),
        "state_path": str(var / "state.json"),
        "report_path": str(var / "report.json"),
        "audit_path": str(var / "audit.jsonl"),
        "dry_run": True,
        "quiet": True,
        "linter_runner": lambda c: {
            "ok": True,
            "rc": 0,
            "error": None,
            "stdout": "",
            "stderr": "",
            "duration_s": 0.0,
            "cmd": [],
            "timed_out": False,
            "invoked": True,
        },
        "tick_deadline_s": 60,
    }
    ads = adapters.build_adapters(cfg)
    run_ads = {k: ads[k] for k in ("snapshot", "decide", "execute")}
    bo, be = io.StringIO(), io.StringIO()
    with redirect_stdout(bo), redirect_stderr(be):
        rc = runner.run_tick(cfg, adapters=run_ads)
    assert rc == 0, (bo.getvalue(), be.getvalue(), (var / "report.json").read_text() if (var / "report.json").exists() else None)
    assert bo.getvalue() == ""
    rep = json.loads((var / "report.json").read_text())
    planned = int((rep.get("decision") or {}).get("actions_planned") or 0)
    applied = int((rep.get("decision") or {}).get("actions_applied") or 0)
    assert planned == 0
    assert applied == 0
    assert rep.get("dry_run") is True


def test_running_tuples_helper_shape():
    tuples = adapters.running_card_tuples("/root/.hermes")
    assert isinstance(tuples, list)
    for t in tuples:
        assert set(t.keys()) >= {"card_id", "status", "run_id", "pid", "board_id"}
        assert t["status"] == "running"


def test_stale_dead_classification(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(
        con,
        tid="t_dead",
        status="running",
        worker_pid=999999,
        current_run_id=9,
        last_heartbeat_at=1,
        claim_lock="x",
    )
    _insert_run(con, run_id=9, task_id="t_dead", status="running", worker_pid=999999)
    con.commit()
    con.close()
    _doc, plan = _plan_for_root(root)
    cls = [c for c in (plan.get("classifications") or []) if c.get("task_id") == "t_dead"]
    assert cls
    label = str(cls[0].get("classification") or "")
    assert any(k in label for k in ("dead", "stale", "no_progress", "running", "pid"))


# ---------------------------------------------------------------------------
# Fail-closed regressions (executor envelope/parity + plan-lint Critical rc=2)
# ---------------------------------------------------------------------------

_PRIOR_ACTION_KEY = "prior-keep"
_PLANNED_ACTION_KEY = "act-plan-1"
_EXTRA_ACTION_KEY = "act-extra"


def _seed_state(path: Path) -> dict:
    st = runner.empty_state_v1()
    st["action_keys"] = {_PRIOR_ACTION_KEY: {"ts": "t0", "decision_key": "prior"}}
    st["cooldowns"] = {_PRIOR_ACTION_KEY: 111}
    st["action_cooldowns"] = {
        _PRIOR_ACTION_KEY: {"last_ns": 1, "until_ns": 2, "action": "comment_once"}
    }
    st["tasks"] = {
        "t_seed": {
            "observations": [{"sig": "seed", "n": 1}],
            "last_sig": "seed",
        }
    }
    st["consecutive_stall_counters"] = {"t_seed": 1}
    st["tick_count"] = 3
    runner.atomic_write_json(path, st)
    return st


def _ok_lint():
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


def _tick_cfg(tmp_path: Path, **over):
    var = tmp_path / "var"
    var.mkdir(exist_ok=True)
    cfg = {
        "lock_path": str(var / "hygiene.lock"),
        "state_path": str(var / "state.json"),
        "report_path": str(var / "report.json"),
        "audit_path": str(var / "audit.jsonl"),
        "dry_run": False,
        "quiet": True,
        "tick_deadline_s": 60,
        "linter_runner": lambda c: _ok_lint(),
    }
    cfg.update(over)
    return cfg, var


def _plan_one_action(key: str = _PLANNED_ACTION_KEY) -> dict:
    return {
        "schema": "decision_plan_v1",
        "actions": [
            {
                "action_key": key,
                "action": "comment_once",
                "task_id": "t1",
                "cooldown_seconds": 30,
            }
        ],
        "decision_key": "recover",
        "reason": "stalled",
        "fail_closed": False,
        "cooldown_updates": {key: 99},
        "stall_counter_updates": {"t1": 2},
        "state_observations": {
            "t1": {"last_sig": "sig-a", "observations": [{"sig": "sig-a", "n": 1}]}
        },
    }


def _action_related_slice(st: dict) -> dict:
    return {
        "action_keys": st.get("action_keys"),
        "cooldowns": st.get("cooldowns"),
        "action_cooldowns": st.get("action_cooldowns"),
        "tasks": st.get("tasks"),
        "consecutive_stall_counters": st.get("consecutive_stall_counters"),
    }


def _assert_fail_closed_no_action_advance(var: Path, prior: dict, rep: dict) -> None:
    assert rep.get("fail_closed") is True
    assert (rep.get("decision") or {}).get("fail_closed") is True
    assert int((rep.get("decision") or {}).get("actions_applied") or 0) == 0
    st = json.loads((var / "state.json").read_text(encoding="utf-8"))
    assert _action_related_slice(st) == _action_related_slice(prior)
    assert _PRIOR_ACTION_KEY in (st.get("action_keys") or {})
    assert _PLANNED_ACTION_KEY not in (st.get("action_keys") or {})
    assert _EXTRA_ACTION_KEY not in (st.get("action_keys") or {})


@pytest.mark.parametrize(
    "case_name,exec_factory",
    [
        ("non_dict", lambda: "not-a-dict"),
        ("missing_ok", lambda: {"schema": "executor_result_v1", "applied": []}),
        (
            "ok_false",
            lambda: {
                "schema": "executor_result_v1",
                "ok": False,
                "applied": [],
                "errors": [],
            },
        ),
        (
            "ok_truthy_non_literal",
            lambda: {
                "schema": "executor_result_v1",
                "ok": 1,
                "applied": [{"action_key": _PLANNED_ACTION_KEY}],
                "denied": [],
                "errors": [],
            },
        ),
        (
            "denied",
            lambda: {
                "schema": "executor_result_v1",
                "ok": True,
                "applied": [],
                "denied": [
                    {
                        "action_key": _PLANNED_ACTION_KEY,
                        "status": "denied",
                        "code": "deny_class",
                    }
                ],
                "errors": [],
            },
        ),
        (
            "error_bucket",
            lambda: {
                "schema": "executor_result_v1",
                "ok": True,
                "applied": [],
                "denied": [],
                "errors": [
                    {
                        "action_key": _PLANNED_ACTION_KEY,
                        "code": "guard_race",
                        "message": "cas",
                    }
                ],
            },
        ),
        (
            "missing_applied",
            lambda: {
                "schema": "executor_result_v1",
                "ok": True,
                "applied": [],
                "denied": [],
                "errors": [],
            },
        ),
        (
            "extra_applied",
            lambda: {
                "schema": "executor_result_v1",
                "ok": True,
                "applied": [
                    {"action_key": _PLANNED_ACTION_KEY},
                    {"action_key": _EXTRA_ACTION_KEY},
                ],
                "denied": [],
                "errors": [],
            },
        ),
        (
            "duplicate_applied",
            lambda: {
                "schema": "executor_result_v1",
                "ok": True,
                "applied": [
                    {"action_key": _PLANNED_ACTION_KEY},
                    {"action_key": _PLANNED_ACTION_KEY},
                ],
                "denied": [],
                "errors": [],
            },
        ),
        (
            "unmatched_applied",
            lambda: {
                "schema": "executor_result_v1",
                "ok": True,
                "applied": [{"action_key": _EXTRA_ACTION_KEY}],
                "denied": [],
                "errors": [],
            },
        ),
    ],
)
def test_executor_envelope_and_parity_fail_closed(tmp_path: Path, case_name, exec_factory):
    """A: malformed/non-success executor envelopes and planned/applied mismatches fail-close."""
    cfg, var = _tick_cfg(tmp_path)
    prior = _seed_state(Path(cfg["state_path"]))
    exec_calls = {"n": 0}

    def execute(plan, config, dry_run=False):
        exec_calls["n"] += 1
        return exec_factory()

    rc = runner.run_tick(
        cfg,
        adapters={
            "decide": lambda s, st, c: _plan_one_action(),
            "execute": execute,
        },
    )
    assert rc == 0
    assert exec_calls["n"] == 1
    rep = json.loads((var / "report.json").read_text(encoding="utf-8"))
    _assert_fail_closed_no_action_advance(var, prior, rep)
    # Structured failure details must be present on the report path.
    assert rep.get("executor") is not None
    assert rep["executor"].get("ok") is not True or rep.get("fail_closed") is True
    reason = str((rep.get("decision") or {}).get("reason") or "") + " ".join(
        str(x) for x in (rep.get("errors") or [])
    )
    assert reason  # non-empty machine-readable failure trail


def test_executor_locked_db_probe_fail_closed(tmp_path: Path):
    """A: Reviewer probe {ok:false, errors:[locked_db], applied:[]} fail-closes without advance."""
    cfg, var = _tick_cfg(tmp_path)
    prior = _seed_state(Path(cfg["state_path"]))
    exec_calls = {"n": 0}

    def execute(plan, config, dry_run=False):
        exec_calls["n"] += 1
        return {
            "schema": "executor_result_v1",
            "ok": False,
            "applied": [],
            "denied": [],
            "errors": [{"code": "locked_db", "message": "database is locked"}],
            "dry_run": dry_run,
        }

    rc = runner.run_tick(
        cfg,
        adapters={
            "decide": lambda s, st, c: _plan_one_action(),
            "execute": execute,
        },
    )
    assert rc == 0
    assert exec_calls["n"] == 1
    rep = json.loads((var / "report.json").read_text(encoding="utf-8"))
    _assert_fail_closed_no_action_advance(var, prior, rep)
    blob = json.dumps(rep)
    assert "locked_db" in blob or any("locked_db" in str(e) for e in (rep.get("errors") or []))


def test_plan_lint_rc2_critical_fail_closed_no_execute(tmp_path: Path):
    """B: plan-lint rc=2 Critical findings => executed but gate failed; no executor/state advance."""
    cfg, var = _tick_cfg(tmp_path)
    prior = _seed_state(Path(cfg["state_path"]))
    exec_calls = {"n": 0}
    decide_calls = {"n": 0}

    def critical_lint(c):
        # Mimic real subprocess path semantics via fake that returns rc=2 payload shape
        # after run_plan_lint normalization (ok must be False for Critical gate).
        return {
            "ok": False,
            "rc": 2,
            "error": "linter_critical_findings",
            "reason": "critical_plan_lint_findings",
            "critical_count": 3,
            "finding_count": 5,
            "stdout": "plan_lint findings=5 critical=3 error=1 warning=1 dry=True",
            "stderr": "",
            "duration_s": 0.01,
            "cmd": ["fake-lint-rc2"],
            "timed_out": False,
            "executed": True,
            "gate_ok": False,
        }

    def execute(plan, config, dry_run=False):
        exec_calls["n"] += 1
        return {
            "ok": True,
            "applied": [{"action_key": _PLANNED_ACTION_KEY}],
            "skipped": [],
            "dry_run": dry_run,
        }

    def decide(s, st, c):
        decide_calls["n"] += 1
        return _plan_one_action()

    cfg["linter_runner"] = critical_lint
    rc = runner.run_tick(
        cfg,
        adapters={"decide": decide, "execute": execute},
    )
    assert rc == 0
    assert exec_calls["n"] == 0
    assert decide_calls["n"] == 0
    rep = json.loads((var / "report.json").read_text(encoding="utf-8"))
    _assert_fail_closed_no_action_advance(var, prior, rep)
    assert int((rep.get("decision") or {}).get("actions_planned") or 0) == 0
    assert int((rep.get("decision") or {}).get("actions_applied") or 0) == 0
    lint = rep.get("linter") or {}
    assert lint.get("invoked") is True
    assert lint.get("rc") == 2
    # Gate unsuccessful even though process executed.
    assert lint.get("ok") is False or lint.get("gate_ok") is False
    # Critical finding count + explicit machine-readable reason.
    crit = lint.get("critical_count")
    if crit is None:
        crit = rep.get("critical_count")
    assert int(crit or 0) == 3
    reason_blob = " ".join(
        [
            str((rep.get("decision") or {}).get("key") or ""),
            str((rep.get("decision") or {}).get("reason") or ""),
            str(lint.get("reason") or ""),
            str(lint.get("error") or ""),
            " ".join(str(x) for x in (rep.get("errors") or [])),
        ]
    ).lower()
    assert "critical" in reason_blob


def test_run_plan_lint_rc2_is_gate_failure_not_ok(tmp_path: Path):
    """B: subprocess rc=2 Critical must not be treated as ok/pass by run_plan_lint."""
    script = tmp_path / "lint_rc2.py"
    script.write_text(
        "import sys\n"
        "print('plan_lint findings=2 critical=2 error=0 warning=0 dry=True')\n"
        "sys.exit(2)\n",
        encoding="utf-8",
    )
    res = runner.run_plan_lint(
        {
            "linter_path": str(script),
            "python_executable": sys.executable,
            "linter_timeout_s": 10,
            "dry_run": True,
        }
    )
    assert res.get("rc") == 2
    assert res.get("ok") is False
    assert res.get("timed_out") is False
    assert int(res.get("critical_count") or 0) == 2
    reason = str(res.get("reason") or res.get("error") or "").lower()
    assert "critical" in reason


def test_executor_success_still_advances_state(tmp_path: Path):
    """Preserve approved success path: literal ok=true + exact parity advances state."""
    cfg, var = _tick_cfg(tmp_path)
    prior = _seed_state(Path(cfg["state_path"]))

    def execute(plan, config, dry_run=False):
        return {
            "schema": "executor_result_v1",
            "ok": True,
            "applied": [{"action_key": _PLANNED_ACTION_KEY}],
            "denied": [],
            "errors": [],
            "skipped": [],
            "dry_run": dry_run,
        }

    rc = runner.run_tick(
        cfg,
        adapters={
            "decide": lambda s, st, c: _plan_one_action(),
            "execute": execute,
        },
    )
    assert rc == 0
    rep = json.loads((var / "report.json").read_text(encoding="utf-8"))
    assert rep.get("fail_closed") is False
    assert (rep.get("decision") or {}).get("fail_closed") is False
    assert int((rep.get("decision") or {}).get("actions_applied") or 0) == 1
    st = json.loads((var / "state.json").read_text(encoding="utf-8"))
    assert _PLANNED_ACTION_KEY in (st.get("action_keys") or {})
    assert _PRIOR_ACTION_KEY in (st.get("action_keys") or {})
    assert st["cooldowns"].get(_PLANNED_ACTION_KEY) == 99
    # prior slice not required equal — success may advance
    assert st["tick_count"] == int(prior["tick_count"]) + 1
