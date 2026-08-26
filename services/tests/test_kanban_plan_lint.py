#!/usr/bin/env python3
"""Isolated-DB tests for runtime/kanban_plan_lint.py (semantic admission v1 + legacy)."""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path("/root/main")
LINT_PATH = ROOT / "runtime" / "kanban_plan_lint.py"


def _load_lint():
    spec = importlib.util.spec_from_file_location("kanban_plan_lint_under_test", LINT_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


lint = _load_lint()


SCHEMA = """
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT,
    assignee TEXT,
    status TEXT NOT NULL,
    priority INTEGER DEFAULT 0,
    created_by TEXT,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    workspace_kind TEXT NOT NULL DEFAULT 'scratch',
    workspace_path TEXT,
    branch_name TEXT,
    project_id TEXT,
    claim_lock TEXT,
    claim_expires INTEGER,
    tenant TEXT,
    result TEXT,
    idempotency_key TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    worker_pid INTEGER,
    last_failure_error TEXT,
    max_runtime_seconds INTEGER,
    last_heartbeat_at INTEGER,
    current_run_id INTEGER,
    workflow_template_id TEXT,
    current_step_key TEXT,
    skills TEXT,
    model_override TEXT,
    max_retries INTEGER,
    goal_mode INTEGER,
    goal_max_turns INTEGER,
    session_id TEXT,
    block_kind TEXT,
    block_recurrences INTEGER
);
CREATE TABLE task_links (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    PRIMARY KEY (parent_id, child_id)
);
CREATE TABLE task_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    profile TEXT,
    step_key TEXT,
    status TEXT NOT NULL,
    claim_lock TEXT,
    claim_expires INTEGER,
    worker_pid INTEGER,
    max_runtime_seconds INTEGER,
    last_heartbeat_at INTEGER,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    outcome TEXT,
    summary TEXT,
    metadata TEXT,
    error TEXT
);
CREATE TABLE task_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    author TEXT,
    body TEXT,
    created_at INTEGER
);
CREATE TABLE task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    run_id INTEGER,
    kind TEXT,
    payload TEXT,
    created_at INTEGER
);
"""


def _connect(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    con.commit()
    return con


def _add_task(
    con: sqlite3.Connection,
    tid: str,
    *,
    title: str = "t",
    body: str = "",
    status: str = "ready",
    assignee: str = "worker",
    result: str | None = None,
    created_at: int = 1000,
    max_retries: int | None = 3,
) -> None:
    con.execute(
        """
        insert into tasks (
            id, title, body, assignee, status, created_at, result, max_retries
        ) values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (tid, title, body, assignee, status, created_at, result, max_retries),
    )


def _link(con: sqlite3.Connection, parent: str, child: str) -> None:
    con.execute(
        "insert into task_links(parent_id, child_id) values (?, ?)", (parent, child)
    )


def _run_summary(con: sqlite3.Connection, tid: str, summary: str, started_at: int = 1) -> None:
    con.execute(
        """
        insert into task_runs(task_id, status, started_at, ended_at, outcome, summary)
        values (?, 'done', ?, ?, 'completed', ?)
        """,
        (tid, started_at, started_at + 1, summary),
    )


def _kinds(findings) -> set[str]:
    return {f.kind for f in findings}


def _kinds_for(findings, tid: str) -> set[str]:
    return {f.kind for f in findings if f.task_id == tid}


def _snapshot_db(path: Path) -> bytes:
    # Logical snapshot: dump tables ordered
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    parts = []
    for table in ("tasks", "task_links", "task_runs", "task_comments", "task_events"):
        rows = con.execute(f"select * from {table} order by rowid").fetchall()
        parts.append(table.encode() + b"\n")
        for r in rows:
            parts.append(repr(tuple(r)).encode() + b"\n")
    con.close()
    return b"".join(parts)


# ---------------------------------------------------------------------------
# Parser / legacy
# ---------------------------------------------------------------------------


def test_parser_conflicting_duplicate_and_normalize():
    body = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "RESEARCH_SIGNATURE:  Foo   BAR ",
            "RESEARCH_SIGNATURE: other",
            "RESEARCH_WAVE: W1",
        ]
    )
    markers, errors = lint.parse_semantic_markers(body)
    assert "conflicting_duplicate:RESEARCH_SIGNATURE" in errors
    assert markers["SEMANTIC_ADMISSION"] == "v1"
    assert lint.normalize_signature("  Foo   BAR ") == "foo bar"
    assert lint.trim_id("  t_ABC  ") == "t_ABC"


def test_legacy_unmarked_compatibility(tmp_path: Path):
    db = tmp_path / "k.db"
    con = _connect(db)
    # Research-like body without SEMANTIC_ADMISSION: v1
    _add_task(
        con,
        "t_legacy",
        title="RESEARCH something",
        body="CARD_KIND: RESEARCH\nRESEARCH_SIGNATURE: foo\nRESEARCH_WAVE: 1\nDISCOVERY_CLOSED: true",
        status="ready",
    )
    # Fat-ish unmarked worker still subject to existing fat lint only
    fat_body = "ONE CONCRETE OUTCOME: x\n" + "\n".join(
        [
            "inventory collect list files hashes",
            "integrate assemble merge final contract write to final/",
            "systemctl status foo",
            "historical prior evidence ledger",
            "`a.json`",
            "`b.json`",
            "`c.json`",
            "`d.json`",
            "`e.json`",
            "`f.json`",
        ]
    )
    _add_task(con, "t_fat", title="IMPLEMENT mega", body=fat_body, status="ready")
    con.commit()
    findings = lint.semantic_admission_findings("test", con)
    assert findings == []
    # existing fat path still works via scan helpers
    task = dict(con.execute("select * from tasks where id='t_fat'").fetchone())
    fat = lint.lint_worker_card("test", task)
    assert any(f.kind == "fat_card_needs_fanout" for f in fat)
    con.close()


# ---------------------------------------------------------------------------
# 1) discovery / proven closure
# ---------------------------------------------------------------------------


def test_discovery_closed_and_proven_signatures(tmp_path: Path):
    db = tmp_path / "k.db"
    con = _connect(db)
    parent_body = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "CARD_KIND: IMPLEMENTATION",
            "DISCOVERY_CLOSED: true",
            "PROVEN_SIGNATURES: alpha-sig; beta other",
        ]
    )
    _add_task(con, "t_parent", title="parent", body=parent_body, status="done", created_at=1)
    # research under closed discovery
    r1 = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "CARD_KIND: RESEARCH",
            "RESEARCH_SIGNATURE: fresh-new",
            "RESEARCH_WAVE: 1",
        ]
    )
    _add_task(con, "t_r_closed", title="research closed", body=r1, status="ready", created_at=2)
    _link(con, "t_parent", "t_r_closed")
    # research of proven signature on self
    r2 = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "CARD_KIND: RESEARCH",
            "RESEARCH_SIGNATURE: Alpha-Sig",
            "RESEARCH_WAVE: 1",
            "PROVEN_SIGNATURES: alpha-sig",
        ]
    )
    _add_task(con, "t_r_proven", title="research proven", body=r2, status="ready", created_at=3)
    con.commit()
    findings = lint.semantic_admission_findings("test", con)
    assert "semantic_no_progress:discovery_closed" in _kinds_for(findings, "t_r_closed")
    assert "semantic_no_progress:discovery_closed" in _kinds_for(findings, "t_r_proven")
    # quarantine action on active
    f = next(f for f in findings if f.task_id == "t_r_closed")
    assert f.action == "block"
    con.close()


# ---------------------------------------------------------------------------
# 2) research wave same vs different
# ---------------------------------------------------------------------------


def test_research_same_wave_allowed_second_wave_blocked(tmp_path: Path):
    db = tmp_path / "k.db"
    con = _connect(db)
    base = "SEMANTIC_ADMISSION: v1\nCARD_KIND: RESEARCH\nRESEARCH_SIGNATURE: lane-x\n"
    _add_task(con, "t_w1a", title="r1a", body=base + "RESEARCH_WAVE: wave-1", status="ready", created_at=10)
    _add_task(con, "t_w1b", title="r1b", body=base + "RESEARCH_WAVE: wave-1", status="ready", created_at=11)
    _add_task(con, "t_w2", title="r2", body=base + "RESEARCH_WAVE: wave-2", status="ready", created_at=12)
    con.commit()
    findings = lint.semantic_admission_findings("test", con)
    assert "semantic_no_progress:research_wave_repeat" not in _kinds_for(findings, "t_w1a")
    assert "semantic_no_progress:research_wave_repeat" not in _kinds_for(findings, "t_w1b")
    assert "semantic_no_progress:research_wave_repeat" in _kinds_for(findings, "t_w2")
    con.close()


# ---------------------------------------------------------------------------
# 3) replan SUPERSEDES active vs quarantined
# ---------------------------------------------------------------------------


def test_replan_supersedes_active_vs_quarantined(tmp_path: Path):
    db = tmp_path / "k.db"
    con = _connect(db)
    _add_task(con, "t_old", title="old", body="x", status="ready", created_at=1)
    _add_task(con, "t_old_child", title="old child", body="x", status="todo", created_at=2)
    _link(con, "t_old", "t_old_child")
    active_body = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "CARD_KIND: IMPLEMENTATION",
            "REPLAN_TRANSACTION: txn-1",
            "SUPERSEDES: t_old",
        ]
    )
    _add_task(con, "t_new_active", title="replacement active", body=active_body, status="ready", created_at=3)

    # quarantined old graph
    _add_task(con, "t_old2", title="old2", body="x", status="blocked", created_at=4)
    _add_task(con, "t_old2c", title="old2c", body="x", status="done", created_at=5)
    _link(con, "t_old2", "t_old2c")
    ok_body = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "CARD_KIND: IMPLEMENTATION",
            "REPLAN_TRANSACTION: txn-2",
            "SUPERSEDES: t_old2",
        ]
    )
    _add_task(con, "t_new_ok", title="replacement ok", body=ok_body, status="ready", created_at=6)

    # malformed missing supersedes
    bad = "SEMANTIC_ADMISSION: v1\nREPLAN_TRANSACTION: txn-3\n"
    _add_task(con, "t_bad", title="bad", body=bad, status="ready", created_at=7)
    # missing id
    miss = "SEMANTIC_ADMISSION: v1\nREPLAN_TRANSACTION: txn-4\nSUPERSEDES: t_does_not_exist\n"
    _add_task(con, "t_miss", title="miss", body=miss, status="ready", created_at=8)
    con.commit()

    findings = lint.semantic_admission_findings("test", con)
    assert "semantic_no_progress:replan_superseded_graph_active" in _kinds_for(
        findings, "t_new_active"
    )
    assert "semantic_no_progress:replan_superseded_graph_active" not in _kinds_for(
        findings, "t_new_ok"
    )
    assert "semantic_contract_invalid" in _kinds_for(findings, "t_bad")
    assert "semantic_contract_invalid" in _kinds_for(findings, "t_miss")
    con.close()


# ---------------------------------------------------------------------------
# 4) live evidence source (result/summary; not body)
# ---------------------------------------------------------------------------


def test_live_evidence_source_result_and_parent(tmp_path: Path):
    db = tmp_path / "k.db"
    con = _connect(db)
    # done live goal without marker in result/summary (marker only in body) => missing
    body_fake = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "LIVE_GOAL: true",
            "LIVE_EVIDENCE_REQUIRED: true",
            "LIVE_EVIDENCE: PASS",
            "unit tests GREEN fake-clock GREEN",
        ]
    )
    _add_task(
        con,
        "t_live_bad",
        title="live bad",
        body=body_fake,
        status="done",
        result="all unit GREEN",
        created_at=1,
    )
    _run_summary(con, "t_live_bad", "fake-clock GREEN only")

    # done live goal with marker in result => ok
    body_ok = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "LIVE_GOAL: true",
            "LIVE_EVIDENCE_REQUIRED: true",
        ]
    )
    _add_task(
        con,
        "t_live_ok",
        title="live ok",
        body=body_ok,
        status="done",
        result="done\nLIVE_EVIDENCE: PASS\n",
        created_at=2,
    )

    # parent done without marker
    _add_task(
        con,
        "t_parent_no",
        title="parent no",
        body="SEMANTIC_ADMISSION: v1\nLIVE_GOAL: true\n",
        status="done",
        result="nope",
        created_at=3,
    )
    rev_body = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "CARD_KIND: REVIEWER",
            "LIVE_EVIDENCE_REQUIRED: true",
            "LIVE_EVIDENCE: PASS",  # child body must NOT count
        ]
    )
    _add_task(con, "t_rev_bad", title="reviewer", body=rev_body, status="ready", created_at=4)
    _link(con, "t_parent_no", "t_rev_bad")

    # parent done with marker in run summary
    _add_task(
        con,
        "t_parent_yes",
        title="parent yes",
        body="SEMANTIC_ADMISSION: v1\nLIVE_GOAL: true\n",
        status="done",
        result="finished",
        created_at=5,
    )
    _run_summary(con, "t_parent_yes", "ops check\nLIVE_EVIDENCE: PASS\n")
    _add_task(con, "t_rev_ok", title="reviewer ok", body=rev_body, status="ready", created_at=6)
    _link(con, "t_parent_yes", "t_rev_ok")
    con.commit()

    findings = lint.semantic_admission_findings("test", con)
    assert "semantic_no_progress:live_evidence_missing" in _kinds_for(findings, "t_live_bad")
    live_bad = next(f for f in findings if f.task_id == "t_live_bad")
    assert live_bad.action == "none"  # no history mutation
    assert "semantic_no_progress:live_evidence_missing" not in _kinds_for(findings, "t_live_ok")
    assert "semantic_no_progress:live_parent_evidence_missing" in _kinds_for(findings, "t_rev_bad")
    assert "semantic_no_progress:live_parent_evidence_missing" not in _kinds_for(
        findings, "t_rev_ok"
    )
    con.close()


# ---------------------------------------------------------------------------
# 5) correction first vs repeat
# ---------------------------------------------------------------------------


def test_planner_correction_first_vs_repeat(tmp_path: Path):
    db = tmp_path / "k.db"
    con = _connect(db)
    base = "SEMANTIC_ADMISSION: v1\nCARD_KIND: PLANNER_CORRECTION\nPLANNER_CORRECTION_SIGNATURE: corr-x\n"
    _add_task(
        con,
        "t_c1",
        title="corr1",
        body=base + "PLANNER_CORRECTION_ROUND: 1",
        status="ready",
        created_at=1,
    )
    _add_task(
        con,
        "t_c2",
        title="corr2",
        body=base + "PLANNER_CORRECTION_ROUND: 1",
        status="ready",
        created_at=2,
    )
    _add_task(
        con,
        "t_c3",
        title="corr3",
        body=base + "PLANNER_CORRECTION_ROUND: 2",
        status="ready",
        created_at=3,
    )
    con.commit()
    findings = lint.semantic_admission_findings("test", con)
    assert "semantic_no_progress:planner_correction_repeat" not in _kinds_for(findings, "t_c1")
    assert "semantic_no_progress:planner_correction_repeat" in _kinds_for(findings, "t_c2")
    assert "semantic_no_progress:planner_correction_repeat" in _kinds_for(findings, "t_c3")
    con.close()


# ---------------------------------------------------------------------------
# Existing-check regression + dry-run non-mutation
# ---------------------------------------------------------------------------


def test_existing_fat_protocol_max_retries_still_fire(tmp_path: Path):
    db = tmp_path / "k.db"
    con = _connect(db)
    fat_body = (
        "inventory collect hashes read-only\n"
        "integrate assemble final contract write to final/\n"
        "systemctl status x\n"
        "historical prior evidence ledger\n"
        + "\n".join(f"`art{i}.json`" for i in range(8))
    )
    _add_task(con, "t_fat", title="IMPLEMENT fat pack", body=fat_body, status="ready")
    _add_task(
        con,
        "t_proto",
        title="worker",
        body="do thing",
        status="ready",
        max_retries=5,
    )
    con.execute(
        "update tasks set consecutive_failures=2, last_failure_error=? where id='t_proto'",
        ("Worker exited rc=0 without calling kanban_complete or kanban_block",),
    )
    con.execute(
        """
        insert into task_runs(task_id, status, started_at, ended_at, outcome, error)
        values
        ('t_proto','failed',1,2,'failed','protocol violation: exited without calling kanban_complete'),
        ('t_proto','failed',3,4,'failed','protocol violation: exited without calling kanban_complete')
        """
    )
    con.commit()

    task_fat = dict(con.execute("select * from tasks where id='t_fat'").fetchone())
    task_proto = dict(con.execute("select * from tasks where id='t_proto'").fetchone())
    assert any(f.kind == "fat_card_needs_fanout" for f in lint.lint_worker_card("t", task_fat))
    assert any(
        f.kind == "protocol_streak_needs_replan"
        for f in lint.protocol_streak_findings("t", con, task_proto)
    )
    assert any(f.kind == "max_retries_too_high" for f in lint.max_retries_findings("t", task_proto))
    con.close()


def test_dry_run_reports_same_codes_without_db_mutation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db = tmp_path / "k.db"
    con = _connect(db)
    body = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "CARD_KIND: RESEARCH",
            "RESEARCH_SIGNATURE: z",
            "RESEARCH_WAVE: 1",
            "DISCOVERY_CLOSED: true",
        ]
    )
    _add_task(con, "t_x", title="research", body=body, status="ready", created_at=1)
    con.commit()
    before = _snapshot_db(db)

    findings = lint.semantic_admission_findings("test", con)
    codes = sorted(_kinds(findings))
    assert "semantic_no_progress:discovery_closed" in codes

    # apply_finding dry must not call hermes and must not write comments/events
    calls = []

    def fake_hermes(board, args, dry):
        calls.append((board, args, dry))
        return 0, "DRY"

    monkeypatch.setattr(lint, "hermes_cmd", fake_hermes)
    state: dict = {"actions": {}}
    for f in findings:
        # force block path
        f.action = "block"
        out = lint.apply_finding(f, dry=True, state=state)
        assert out.get("dry") is True

    assert calls == [] or all(c[2] is True for c in calls)
    # dry hermes_cmd returns without side effects; DB unchanged
    after = _snapshot_db(db)
    assert after == before

    # enforcing-mode decision codes identical
    findings2 = lint.semantic_admission_findings("test", con)
    assert sorted(_kinds(findings2)) == codes
    con.close()


def test_scan_board_includes_semantic(tmp_path: Path):
    db = tmp_path / "board.db"
    con = _connect(db)
    body = "\n".join(
        [
            "SEMANTIC_ADMISSION: v1",
            "CARD_KIND: RESEARCH",
            "RESEARCH_SIGNATURE: s",
            "RESEARCH_WAVE: 1",
            "DISCOVERY_CLOSED: true",
        ]
    )
    _add_task(con, "t_s", title="r", body=body, status="ready")
    con.commit()
    con.close()
    findings = lint.scan_board("iso", db)
    assert any(f.kind == "semantic_no_progress:discovery_closed" for f in findings)
