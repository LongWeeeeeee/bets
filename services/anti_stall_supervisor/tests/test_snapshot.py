#!/usr/bin/env python3
"""Unit tests for snapshot_v1 adapter — temporary Hermes roots only."""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

import snapshot as snap  # noqa: E402


NOW = 1_700_000_100
NOW_NS = NOW * snap.NS_PER_SEC


def _write_board_json(path: Path, *, slug: str, archived: bool = False, raw: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if raw is not None:
        path.write_text(raw, encoding="utf-8")
        return
    path.write_text(
        json.dumps({"slug": slug, "name": slug, "archived": archived}),
        encoding="utf-8",
    )


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
            f"title-{tid}",
            body,
            assignee,
            status,
            NOW,
            worker_pid,
            current_run_id,
            claim_lock,
            last_heartbeat_at,
            result,
            block_kind,
            last_failure_error,
            consecutive_failures,
        ),
    )


def _insert_run(
    con: sqlite3.Connection,
    *,
    task_id: str,
    status: str = "running",
    worker_pid: int | None = None,
    outcome: str | None = None,
    summary: str | None = None,
    metadata: str | None = None,
    error: str | None = None,
    claim_lock: str | None = None,
    ended: bool = False,
) -> int:
    cur = con.execute(
        """
        INSERT INTO task_runs (
            task_id, profile, status, claim_lock, worker_pid, started_at,
            ended_at, outcome, summary, metadata, error, last_heartbeat_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            task_id,
            "worker",
            status,
            claim_lock,
            worker_pid,
            NOW,
            NOW + 10 if ended else None,
            outcome,
            summary,
            metadata,
            error,
            NOW,
        ),
    )
    rid = cur.lastrowid
    assert rid is not None
    return int(rid)


def _insert_comment(con: sqlite3.Connection, *, task_id: str, body: str, author: str = "worker") -> int:
    cur = con.execute(
        "INSERT INTO task_comments (task_id, author, body, created_at) VALUES (?,?,?,?)",
        (task_id, author, body, NOW),
    )
    cid = cur.lastrowid
    assert cid is not None
    return int(cid)


def _insert_event(
    con: sqlite3.Connection,
    *,
    task_id: str,
    kind: str,
    payload: str | None = None,
    run_id: int | None = None,
) -> int:
    cur = con.execute(
        "INSERT INTO task_events (task_id, run_id, kind, payload, created_at) VALUES (?,?,?,?,?)",
        (task_id, run_id, kind, payload, NOW),
    )
    eid = cur.lastrowid
    assert eid is not None
    return int(eid)


def _proc_none(_pid: int):
    return None


def _proc_alive(start_ticks: int = 111):
    def _r(pid: int):
        return {"pid": pid, "start_ticks": start_ticks, "state": "S", "readable": True}

    return _r


def _proc_perm(_pid: int):
    raise PermissionError("EACCES")


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_discover_boards_sorts_dedupes_and_skips_archived(tmp_path: Path):
    root = _mk_root(tmp_path)
    snap.write_min_schema(root / "kanban.db")
    # active named
    a = root / "kanban" / "boards" / "zeta"
    _write_board_json(a / "board.json", slug="zeta", archived=False)
    snap.write_min_schema(a / "kanban.db")
    b = root / "kanban" / "boards" / "alpha"
    _write_board_json(b / "board.json", slug="alpha", archived=False)
    snap.write_min_schema(b / "kanban.db")
    # archived
    c = root / "kanban" / "boards" / "old"
    _write_board_json(c / "board.json", slug="old", archived=True)
    snap.write_min_schema(c / "kanban.db")
    # _archived dir
    d = root / "kanban" / "boards" / "_archived"
    d.mkdir(parents=True)
    snap.write_min_schema(d / "kanban.db")

    paths = snap.discover_boards(root)
    ids = [snap._board_id_for_db(root, p) for p in paths]
    assert ids == ["default", "alpha", "zeta"]
    # resolved paths unique
    assert len(set(str(p.resolve()) for p in paths)) == len(paths)


def test_discover_excludes_malformed_board_json(tmp_path: Path):
    root = _mk_root(tmp_path)
    bad = root / "kanban" / "boards" / "bad"
    _write_board_json(bad / "board.json", slug="bad", raw="{not json")
    snap.write_min_schema(bad / "kanban.db")
    paths = snap.discover_boards(root)
    assert paths == []
    snap_doc = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)
    codes = [d["code"] for d in snap_doc["diagnostics"]]
    assert "archive_state_unknown" in codes


# ---------------------------------------------------------------------------
# Read-only / no mutation
# ---------------------------------------------------------------------------


def test_collect_is_readonly_no_mutation(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t1", status="todo")
    con.commit()
    before = Path(db).read_bytes()
    mtime = db.stat().st_mtime_ns
    con.close()

    snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)
    after = Path(db).read_bytes()
    assert after == before
    # file content identity
    assert db.stat().st_size == len(before)


def test_read_only_uri_rejects_write(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = snap._connect_ro(db)
    with pytest.raises(sqlite3.Error):
        con.execute("INSERT INTO tasks (id,title,status,created_at) VALUES ('x','t','todo',1)")
        con.commit()
    con.close()


# ---------------------------------------------------------------------------
# Schema / corrupt / locked diagnostics
# ---------------------------------------------------------------------------


def test_unknown_schema_diagnostic(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db))
    con.execute("CREATE TABLE junk (id INTEGER)")
    con.commit()
    con.close()
    board = snap.read_board_snapshot(db, now_ns=NOW_NS, proc_reader=_proc_none, board_id="default", hermes_root=root)
    assert board["ok"] is False
    assert any(d["code"] == "unknown_schema" for d in board["diagnostics"])


def test_corrupt_db_diagnostic(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    db.write_bytes(b"not a sqlite database!!!!")
    board = snap.read_board_snapshot(db, now_ns=NOW_NS, proc_reader=_proc_none, board_id="default", hermes_root=root)
    assert board["ok"] is False
    codes = {d["code"] for d in board["diagnostics"]}
    assert codes & {"db_open_error", "db_corrupt", "db_operational_error"}


# ---------------------------------------------------------------------------
# Task fields / edges / digests
# ---------------------------------------------------------------------------


def test_parents_children_and_archived_task_excluded(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="parent", status="done", assignee="planner")
    _insert_task(con, tid="child", status="blocked", body="need parents")
    _insert_task(con, tid="arch", status="archived")
    con.execute("INSERT INTO task_links (parent_id, child_id) VALUES ('parent','child')")
    con.commit()
    con.close()

    doc = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)
    board = doc["boards"][0]
    ids = {t["task_id"] for t in board["tasks"]}
    assert "arch" not in ids
    assert "parent" in ids and "child" in ids
    child = next(t for t in board["tasks"] if t["task_id"] == "child")
    assert child["parents"] == ["parent"]
    parent = next(t for t in board["tasks"] if t["task_id"] == "parent")
    assert any(c["task_id"] == "child" for c in parent["children"])
    assert parent["planner_child_state"]["count"] == 1


def test_progress_digest_changes_on_artifact_delta(tmp_path: Path):
    root = _mk_root(tmp_path)
    owner = tmp_path / "owner" / "lane"
    owner.mkdir(parents=True)
    f1 = owner / "a.txt"
    f1.write_text("v1", encoding="utf-8")
    marker = (
        'ANTI_STALL_ARTIFACTS_V1={"task_id":"t1","owner_prefix":"%s","required_paths":["%s"],'
        '"max_no_progress_seconds":900}'
    ) % (str(owner), str(f1))

    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t1", status="running", body=marker)
    con.commit()
    con.close()

    d1 = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)
    t1 = d1["boards"][0]["tasks"][0]
    assert t1["artifacts_declared"] is True
    assert t1["artifacts"]
    pd1 = t1["progress_digest"]

    f1.write_text("v2-changed", encoding="utf-8")
    d2 = snap.collect_snapshot(root, now_ns=NOW_NS + 1, proc_reader=_proc_none)
    t2 = d2["boards"][0]["tasks"][0]
    assert t2["progress_digest"] != pd1
    assert t2["artifact_digest"] != t1["artifact_digest"]


def test_events_digest_changes_on_new_event(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t1", status="running")
    _insert_event(con, task_id="t1", kind="spawned", payload='{"x":1}')
    con.commit()
    con.close()
    d1 = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)
    e1 = d1["boards"][0]["tasks"][0]["events_digest"]

    con = sqlite3.connect(str(db))
    _insert_event(con, task_id="t1", kind="heartbeat", payload='{"hb":2}')
    con.commit()
    con.close()
    d2 = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)
    e2 = d2["boards"][0]["tasks"][0]["events_digest"]
    assert e1 != e2


# ---------------------------------------------------------------------------
# Artifacts gate
# ---------------------------------------------------------------------------


def test_missing_marker_artifacts_declared_false(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t1", body="no marker here")
    con.commit()
    con.close()
    t = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)["boards"][0]["tasks"][0]
    assert t["artifacts_declared"] is False
    assert t["artifacts"] == []


def test_rejects_path_outside_owner_prefix(tmp_path: Path):
    root = _mk_root(tmp_path)
    owner = tmp_path / "owner"
    owner.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("x", encoding="utf-8")
    marker = (
        'ANTI_STALL_ARTIFACTS_V1={"task_id":"t1","owner_prefix":"%s","required_paths":["%s"]}'
        % (str(owner), str(outside))
    )
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t1", body=marker)
    con.commit()
    con.close()
    t = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)["boards"][0]["tasks"][0]
    assert t["artifacts_declared"] is True  # marker valid
    assert t["artifacts"] == []
    assert any(d["code"] == "artifact_path_rejected" for d in t["diagnostics"])


def test_rejects_symlink_escape(tmp_path: Path):
    root = _mk_root(tmp_path)
    owner = tmp_path / "owner"
    owner.mkdir()
    secret_dir = tmp_path / "secretplace"
    secret_dir.mkdir()
    target = secret_dir / "keys.py"
    target.write_text("API=1", encoding="utf-8")
    link = owner / "escape"
    link.symlink_to(target)
    marker = (
        'ANTI_STALL_ARTIFACTS_V1={"task_id":"t1","owner_prefix":"%s","required_paths":["%s"]}'
        % (str(owner), str(link))
    )
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t1", body=marker)
    con.commit()
    con.close()
    t = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)["boards"][0]["tasks"][0]
    assert t["artifacts"] == []
    codes = {d["code"] for d in t["diagnostics"]}
    assert "artifact_path_rejected" in codes


def test_rejects_secretish_path_name(tmp_path: Path):
    root = _mk_root(tmp_path)
    owner = tmp_path / "owner"
    owner.mkdir()
    secret = owner / ".env"
    secret.write_text("TOKEN=abc", encoding="utf-8")
    marker = (
        'ANTI_STALL_ARTIFACTS_V1={"task_id":"t1","owner_prefix":"%s","required_paths":["%s"]}'
        % (str(owner), str(secret))
    )
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t1", body=marker)
    con.commit()
    con.close()
    t = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)["boards"][0]["tasks"][0]
    assert t["artifacts"] == []
    assert any(d.get("evidence", {}).get("reason") == "secret_path" for d in t["diagnostics"])


def test_accepts_valid_owned_regular_file(tmp_path: Path):
    root = _mk_root(tmp_path)
    owner = tmp_path / "owner" / "lane"
    owner.mkdir(parents=True)
    f = owner / "out.bin"
    f.write_bytes(b"hello-artifact")
    marker = (
        'ANTI_STALL_ARTIFACTS_V1={"task_id":"t1","owner_prefix":"%s","required_paths":["%s"],'
        '"max_no_progress_seconds":600}'
    ) % (str(owner), str(f))
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_comment(con, task_id="t1", body="prelude")
    # put marker in comment after task insert
    _insert_task(con, tid="t1", body="x")
    _insert_comment(con, task_id="t1", body=marker)
    con.commit()
    con.close()
    t = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)["boards"][0]["tasks"][0]
    assert t["artifacts_declared"] is True
    assert t["owner_prefix"] == str(owner)
    assert t["max_no_progress_seconds"] == 600
    assert len(t["artifacts"]) == 1
    assert t["artifacts"][0]["sha256"] == snap.sha256_file(f)
    assert t["artifacts"][0]["size"] == len(b"hello-artifact")


# ---------------------------------------------------------------------------
# PID states
# ---------------------------------------------------------------------------


def test_pid_absent_and_dead_and_alive_and_reused_and_foreign_and_permission(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))

    # absent
    _insert_task(con, tid="t_abs", status="running", worker_pid=None, current_run_id=None)

    # dead
    rid_dead = _insert_run(con, task_id="t_dead", status="running", worker_pid=424242)
    _insert_task(con, tid="t_dead", status="running", worker_pid=424242, current_run_id=rid_dead)

    # alive bound
    rid_alive = _insert_run(
        con,
        task_id="t_alive",
        status="running",
        worker_pid=7,
        claim_lock="hostA:7",
        metadata=json.dumps({"start_ticks": 99}),
    )
    _insert_task(
        con,
        tid="t_alive",
        status="running",
        worker_pid=7,
        current_run_id=rid_alive,
        claim_lock="hostA:7",
    )

    # reused ticks mismatch
    rid_re = _insert_run(
        con,
        task_id="t_re",
        status="running",
        worker_pid=8,
        metadata=json.dumps({"start_ticks": 1}),
    )
    _insert_task(con, tid="t_re", status="running", worker_pid=8, current_run_id=rid_re)

    # foreign: pid alive but run not running
    rid_f = _insert_run(con, task_id="t_f", status="done", worker_pid=9, ended=True, outcome="completed")
    _insert_task(con, tid="t_f", status="done", worker_pid=9, current_run_id=rid_f)

    # permission
    rid_p = _insert_run(con, task_id="t_p", status="running", worker_pid=10)
    _insert_task(con, tid="t_p", status="running", worker_pid=10, current_run_id=rid_p)

    con.commit()
    con.close()

    def reader(pid: int):
        if pid == 424242:
            return None
        if pid == 7:
            return {"pid": 7, "start_ticks": 99, "state": "S"}
        if pid == 8:
            return {"pid": 8, "start_ticks": 9999, "state": "S"}
        if pid == 9:
            return {"pid": 9, "start_ticks": 5, "state": "S"}
        if pid == 10:
            raise PermissionError("nope")
        return None

    doc = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=reader)
    by_id = {t["task_id"]: t for t in doc["boards"][0]["tasks"]}
    assert by_id["t_abs"]["pid_state"] == "absent"
    assert by_id["t_dead"]["pid_state"] == "dead"
    assert by_id["t_alive"]["pid_state"] == "alive"
    assert by_id["t_alive"]["pid_evidence"]["bound_to_run"] is True
    assert by_id["t_re"]["pid_state"] == "reused"
    assert by_id["t_f"]["pid_state"] == "foreign"
    assert by_id["t_p"]["pid_state"] == "permission_unknown"


# ---------------------------------------------------------------------------
# Redaction / resolution markers
# ---------------------------------------------------------------------------


def test_redacts_secrets_in_block_reason_and_comments(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    secret = "sk-abcdefghijklmnopqrstuvwxyz012345"
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t1", status="blocked", body="blocked")
    _insert_comment(con, task_id="t1", body=f"reason token={secret}")
    con.commit()
    con.close()
    t = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)["boards"][0]["tasks"][0]
    blob = json.dumps(t)
    assert secret not in blob
    assert "REDACTED" in blob


def test_resolution_markers_parsed_not_classified(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    directive = {
        "action": "unblock_same_card",
        "task_id": "t1",
        "authorization_digest": "a" * 64,
    }
    line = "ANTI_STALL_RESOLUTION_V1=" + json.dumps(directive, separators=(",", ":"))
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t1", status="blocked", body=line)
    con.commit()
    con.close()
    t = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)["boards"][0]["tasks"][0]
    assert t["resolution_markers"]
    assert t["resolution_markers"][0]["parse_ok"] is True
    # no classification fields from decision engine
    assert "stall_class" not in t
    assert "classification" not in t


# ---------------------------------------------------------------------------
# Failure / protocol evidence capture
# ---------------------------------------------------------------------------


def test_failure_signature_and_protocol_capture(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    rid = _insert_run(
        con,
        task_id="t1",
        status="failed",
        ended=True,
        outcome="failed",
        error="rc=0 without kanban complete/block",
        summary="protocol_no_complete",
        metadata=json.dumps({"protocol_no_complete": True}),
    )
    _insert_task(
        con,
        tid="t1",
        status="todo",
        current_run_id=rid,
        last_failure_error="rc=0 without kanban complete",
        consecutive_failures=2,
    )
    con.commit()
    con.close()
    t = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)["boards"][0]["tasks"][0]
    assert t["protocol_violation"] is True
    assert t["protocol_signature"]
    assert t["failure_signature"]
    assert t["attempt_count"] == 1
    assert t["consecutive_failures"] == 2


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_snapshot_digest_stable_for_identical_inputs(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    _insert_task(con, tid="t1", status="todo", body="stable")
    con.commit()
    con.close()
    a = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)
    b = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)
    assert a["snapshot_digest"] == b["snapshot_digest"]
    # board order stable
    assert [x["board_id"] for x in a["boards"]] == [x["board_id"] for x in b["boards"]]


def test_self_test_cli_emits_fixture_counts_only(tmp_path: Path, capsys):
    rc = snap.main(["--self-test"])
    assert rc == 0
    out = capsys.readouterr().out.strip()
    data = json.loads(out)
    assert data["self_test"] == "ok"
    assert data["fixture_count"] >= 1
    assert "boards" not in data or isinstance(data.get("board_count"), int)


def test_db_identity_present(tmp_path: Path):
    root = _mk_root(tmp_path)
    named = root / "kanban" / "boards" / "proj"
    _write_board_json(named / "board.json", slug="proj", archived=False)
    snap.write_min_schema(named / "kanban.db")
    con = sqlite3.connect(str(named / "kanban.db"))
    _insert_task(con, tid="t1")
    con.commit()
    con.close()
    doc = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)
    b = doc["boards"][0]
    assert b["board_id"] == "proj"
    assert b["db_identity"]
    assert b["tasks"][0]["db_identity"] == b["db_identity"]


def test_immutable_sources_include_body_comment_metadata(tmp_path: Path):
    root = _mk_root(tmp_path)
    db = root / "kanban.db"
    snap.write_min_schema(db)
    con = sqlite3.connect(str(db))
    rid = _insert_run(con, task_id="t1", metadata=json.dumps({"k": "v"}))
    _insert_task(con, tid="t1", body="BODYTEXT", current_run_id=rid)
    _insert_comment(con, task_id="t1", body="COMMENT")
    con.commit()
    con.close()
    t = snap.collect_snapshot(root, now_ns=NOW_NS, proc_reader=_proc_none)["boards"][0]["tasks"][0]
    kinds = {s["source_kind"] for s in t["immutable_sources"]}
    assert "body" in kinds
    assert "comment" in kinds
    assert "run_metadata" in kinds
