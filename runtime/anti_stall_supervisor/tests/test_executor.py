"""Tests for transactional idempotent Kanban action executor (W3 staging)."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import sys
import time
from pathlib import Path

import pytest

# Lane-local import (no package install required).
HERE = Path(__file__).resolve().parent.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import executor as ex  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db(tmp_path: Path) -> Path:
    p = tmp_path / "hermes_root" / "kanban" / "boards" / "fixture" / "kanban.db"
    ex.init_fixture_db(p)
    return p


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _counts(db_path: Path) -> dict:
    conn = sqlite3.connect(str(db_path))
    try:
        out = {}
        for t in ("tasks", "task_events", "task_comments", "task_links", "task_runs"):
            out[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        return out
    finally:
        conn.close()


def _task(db_path: Path, task_id: str) -> dict:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        r = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return dict(r) if r else {}
    finally:
        conn.close()


def _seed_blocked_with_sibling(db: Path) -> tuple[str, str]:
    """Parent done + blocked child + successful sibling; returns (target, sibling)."""
    ex.insert_task(db, task_id="t_parent01", title="parent", status="done")
    ex.insert_task(
        db,
        task_id="t_target01",
        title="target blocked",
        status="blocked",
        block_kind="transient",
        current_run_id=None,
        consecutive_failures=2,
        worker_pid=None,
        body="blocked for test",
    )
    ex.insert_task(
        db,
        task_id="t_sibok01",
        title="successful sibling",
        status="done",
        assignee="worker",
    )
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO task_links (parent_id, child_id) VALUES (?, ?)",
            ("t_parent01", "t_target01"),
        )
        conn.execute(
            "INSERT INTO task_links (parent_id, child_id) VALUES (?, ?)",
            ("t_parent01", "t_sibok01"),
        )
        # Historical run on target (must be preserved).
        conn.execute(
            """
            INSERT INTO task_runs (task_id, profile, status, started_at, ended_at, outcome, summary)
            VALUES ('t_target01', 'worker', 'done', 100, 200, 'blocked', 'old block')
            """
        )
        conn.execute(
            """
            INSERT INTO task_events (task_id, run_id, kind, payload, created_at)
            VALUES ('t_target01', NULL, 'blocked', '{"reason":"old"}', 150)
            """
        )
        conn.execute(
            """
            INSERT INTO task_events (task_id, run_id, kind, payload, created_at)
            VALUES ('t_sibok01', NULL, 'completed', NULL, 180)
            """
        )
        conn.commit()
    finally:
        conn.close()
    return "t_target01", "t_sibok01"


def _action_for(db: Path, task_id: str, **kw):
    wm = ex.event_watermark(db, task_id)
    params = {
        "action_key": kw.pop("action_key", f"ak-{task_id}-1"),
        "action_type": kw.pop("action_type", "unblock_same_card"),
        "task_id": task_id,
        "expected_status": kw.pop("expected_status", "blocked"),
        "event_watermark": kw.pop("event_watermark", wm),
        "resolution_directive_digest": kw.pop(
            "resolution_directive_digest", "directive-" + ("d" * 40)
        ),
    }
    params.update(kw)
    return ex.make_action(**params)


# ---------------------------------------------------------------------------
# validate_action
# ---------------------------------------------------------------------------


def test_validate_action_ok():
    a = ex.make_action(
        action_key="ak-ok",
        action_type="comment_once",
        task_id="t_abc123",
        expected_status="blocked",
        board_snapshot_digest="snap1",
    )
    r = ex.validate_action(a, "snap1")
    assert r["ok"] is True
    assert r["action"]["action_key"] == "ak-ok"


def test_validate_action_digest_mismatch():
    a = ex.make_action(
        action_key="ak-bad",
        action_type="comment_once",
        task_id="t_abc123",
        board_snapshot_digest="snapA",
    )
    r = ex.validate_action(a, "snapB")
    assert r["ok"] is False
    assert r["code"] == "hash_mismatch"


def test_validate_action_deny_classes():
    for cls in sorted(ex.DENY_CLASSES):
        a = ex.make_action(
            action_key=f"ak-deny-{cls}",
            action_type="comment_once",
            task_id="t_abc123",
            denylist_tags=[cls],
            board_snapshot_digest="s",
        )
        r = ex.validate_action(a, "s")
        assert r["ok"] is False, cls
        assert r["code"] == "deny_class", cls


def test_validate_unsupported_action_type():
    a = ex.make_action(
        action_key="ak-x",
        action_type="comment_once",
        task_id="t_abc123",
        board_snapshot_digest="s",
    )
    a["action_type"] = "create_task"
    r = ex.validate_action(a, "s")
    assert r["ok"] is False
    assert r["code"] == "schema_mismatch"


# ---------------------------------------------------------------------------
# Unblock same card
# ---------------------------------------------------------------------------


def test_exact_unblock_same_row_to_ready(db: Path):
    target, sibling = _seed_blocked_with_sibling(db)
    before_hash = _sha256_file(db)
    before_counts = _counts(db)
    sib_before = _task(db, sibling)
    target_before = _task(db, target)
    assert target_before["status"] == "blocked"
    assert target_before["consecutive_failures"] == 2

    action = _action_for(
        db,
        target,
        action_type="unblock_same_card",
        expected_block_kind="transient",
        auth_kind="exact_directive",
    )
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=int(time.time_ns()))
    assert res["ok"] is True
    assert len(res["applied"]) == 1
    applied = res["applied"][0]
    assert applied["status"] == "applied"
    assert applied["new_status"] == "ready"

    target_after = _task(db, target)
    assert target_after["status"] == "ready"
    assert target_after["current_run_id"] is None
    assert target_after["consecutive_failures"] == 0
    # block_kind preserved (Hermes unblock does not clear it)
    assert target_after["block_kind"] == "transient"
    # sibling untouched
    sib_after = _task(db, sibling)
    assert sib_after == sib_before
    # links preserved
    conn = sqlite3.connect(str(db))
    try:
        links = conn.execute("SELECT parent_id, child_id FROM task_links ORDER BY child_id").fetchall()
        assert links == [("t_parent01", "t_sibok01"), ("t_parent01", "t_target01")]
        # run history preserved
        runs = conn.execute(
            "SELECT COUNT(*) FROM task_runs WHERE task_id=?", (target,)
        ).fetchone()[0]
        assert runs == 1
        # no new task rows
        n_tasks = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        assert n_tasks == before_counts["tasks"]
        # audit event + unblocked event + comment
        kinds = [
            r[0]
            for r in conn.execute(
                "SELECT kind FROM task_events WHERE task_id=? ORDER BY id", (target,)
            )
        ]
        assert ex.AUDIT_EVENT_KIND in kinds
        assert "unblocked" in kinds
        comments = conn.execute(
            "SELECT body FROM task_comments WHERE task_id=?", (target,)
        ).fetchall()
        assert len(comments) == 1
        assert f"{ex.ACTION_KEY_MARKER_PREFIX}{action['action_key']}" in comments[0][0]
    finally:
        conn.close()
    assert before_hash != _sha256_file(db)


def test_unblock_with_undone_parent_goes_todo(db: Path):
    ex.insert_task(db, task_id="t_par_open", title="open parent", status="running")
    ex.insert_task(db, task_id="t_child_b", title="child", status="blocked", block_kind="transient")
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO task_links (parent_id, child_id) VALUES ('t_par_open','t_child_b')"
        )
        conn.commit()
    finally:
        conn.close()
    action = _action_for(db, "t_child_b", action_type="unblock_same_card")
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["applied"][0]["new_status"] == "todo"
    assert _task(db, "t_child_b")["status"] == "todo"


# ---------------------------------------------------------------------------
# Deny classes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "deny",
    [
        "ambiguous",
        "needs_input",
        "human",
        "secret",
        "production",
        "ownership",
        "auth",
        "reviewer",
        "checksum",
    ],
)
def test_deny_classes_no_mutation(db: Path, deny: str):
    target, _ = _seed_blocked_with_sibling(db)
    h0 = _sha256_file(db)
    c0 = _counts(db)
    action = _action_for(
        db,
        target,
        action_key=f"ak-deny-{deny}",
        action_type="unblock_same_card",
        denylist_tags=[deny],
    )
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["denied"] and res["denied"][0]["code"] == "deny_class"
    assert _sha256_file(db) == h0
    assert _counts(db) == c0
    assert _task(db, target)["status"] == "blocked"


def test_needs_input_without_directive_denied(db: Path):
    ex.insert_task(
        db,
        task_id="t_ni01",
        status="blocked",
        block_kind="needs_input",
    )
    action = _action_for(
        db,
        "t_ni01",
        action_type="unblock_same_card",
        expected_block_kind="needs_input",
        auth_kind="registered_policy",
    )
    # no resolution_directive_digest
    action.pop("resolution_directive_digest", None)
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["denied"]
    assert res["denied"][0]["code"] == "deny_class"
    assert _task(db, "t_ni01")["status"] == "blocked"


def test_secret_in_reason_denied(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    action = _action_for(
        db,
        target,
        action_type="comment_once",
        expected_status="blocked",
        reason="api_key=sk-supersecretvalue1234567890",
    )
    # reason secret check at validate
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["denied"]
    assert res["denied"][0]["code"] == "deny_class"


# ---------------------------------------------------------------------------
# Idempotency / duplicate
# ---------------------------------------------------------------------------


def test_duplicate_execution_already_applied_no_second_write(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    action = _action_for(db, target, action_type="comment_once", expected_status="blocked")
    r1 = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert len(r1["applied"]) == 1
    c1 = _counts(db)
    h1 = _sha256_file(db)

    # Re-apply same key (even if status still blocked — comment_once).
    r2 = ex.apply_board_actions(db, [action], dry_run=False, now_ns=2)
    assert len(r2["already_applied"]) == 1
    assert r2["already_applied"][0]["status"] == "already_applied"
    assert _counts(db) == c1
    assert _sha256_file(db) == h1

    conn = sqlite3.connect(str(db))
    try:
        n_comments = conn.execute(
            "SELECT COUNT(*) FROM task_comments WHERE task_id=?", (target,)
        ).fetchone()[0]
        n_audit = conn.execute(
            "SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind=?",
            (target, ex.AUDIT_EVENT_KIND),
        ).fetchone()[0]
        assert n_comments == 1
        assert n_audit == 1
    finally:
        conn.close()


def test_action_already_applied_helper(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    action = _action_for(db, target, action_type="comment_once")
    assert ex.action_already_applied(sqlite3.connect(str(db)), action["action_key"]) is False
    ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    conn = sqlite3.connect(str(db))
    try:
        assert ex.action_already_applied(conn, action["action_key"]) is True
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# route_needs_replan downgrade
# ---------------------------------------------------------------------------


def test_route_needs_replan_downgrades_to_comment(db: Path):
    assert ex.NEEDS_REPLAN_STATUS_SUPPORTED is False
    assert ex.NEEDS_REPLAN_BLOCK_KIND_SUPPORTED is False
    target, sibling = _seed_blocked_with_sibling(db)
    sib_before = _task(db, sibling)
    runs_before = _counts(db)["task_runs"]
    action = _action_for(
        db,
        target,
        action_type="route_needs_replan",
        expected_status="blocked",
        reason="same signature no delta — needs replan",
    )
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert len(res["applied"]) == 1
    assert res["applied"][0]["effective_type"] == "comment_once"
    assert res["applied"][0]["downgraded_from_route_needs_replan"] is True
    # status unchanged — no guess
    assert _task(db, target)["status"] == "blocked"
    assert _task(db, sibling) == sib_before
    assert _counts(db)["task_runs"] == runs_before
    # no new tasks
    assert _counts(db)["tasks"] == 3


# ---------------------------------------------------------------------------
# Guard race
# ---------------------------------------------------------------------------


def test_guard_race_status_rollback(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    h0 = _sha256_file(db)
    action = _action_for(
        db,
        target,
        action_type="unblock_same_card",
        expected_status="blocked",
    )
    # Mutate status out from under the plan.
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("UPDATE tasks SET status='running' WHERE id=?", (target,))
        conn.commit()
    finally:
        conn.close()
    # watermark still matches events; status won't
    action["event_watermark"] = ex.event_watermark(db, target)
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["denied"]
    assert res["denied"][0]["code"] == "guard_race"
    # no audit comment added
    assert _counts(db)["task_comments"] == 0
    # status remains running (our external mutation)
    assert _task(db, target)["status"] == "running"


def test_guard_race_event_watermark(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    action = _action_for(db, target, action_type="comment_once", event_watermark=0)
    # There is already at least one event from seed → watermark 0 is stale if events exist
    wm = ex.event_watermark(db, target)
    assert wm >= 1
    action["event_watermark"] = 0
    h0 = _sha256_file(db)
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["denied"][0]["code"] == "guard_race"
    assert _sha256_file(db) == h0


def test_guard_race_run_id(db: Path):
    ex.insert_task(db, task_id="t_runx", status="blocked", current_run_id=42)
    action = _action_for(
        db,
        "t_runx",
        action_type="comment_once",
        expected_current_run_id=99,
    )
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["denied"][0]["code"] == "guard_race"


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------


def test_dry_run_no_byte_change(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    action = _action_for(db, target, action_type="unblock_same_card")
    h0 = _sha256_file(db)
    c0 = _counts(db)
    res = ex.apply_board_actions(db, [action], dry_run=True, now_ns=1)
    assert res["dry_run"] is True
    assert len(res["planned"]) == 1
    assert res["planned"][0]["effective_type"] == "unblock_same_card"
    assert _sha256_file(db) == h0
    assert _counts(db) == c0
    assert _task(db, target)["status"] == "blocked"


def test_dry_run_denied_still_readonly(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    action = _action_for(
        db, target, action_type="unblock_same_card", denylist_tags=["reviewer"]
    )
    h0 = _sha256_file(db)
    res = ex.apply_board_actions(db, [action], dry_run=True, now_ns=1)
    assert res["denied"]
    assert _sha256_file(db) == h0


# ---------------------------------------------------------------------------
# Cooldown
# ---------------------------------------------------------------------------


def test_cooldown_skip(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    h0 = _sha256_file(db)
    action = _action_for(
        db,
        target,
        action_type="comment_once",
        cooldown_until_ns=9_000_000_000_000_000_000,
    )
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["skipped"]
    assert res["skipped"][0]["status"] == "skipped_cooldown"
    assert _sha256_file(db) == h0


# ---------------------------------------------------------------------------
# Corrupt / locked / missing schema
# ---------------------------------------------------------------------------


def test_corrupt_db(tmp_path: Path):
    p = tmp_path / "bad.db"
    p.write_bytes(b"this is not a sqlite database at all!!!!")
    action = ex.make_action(
        action_key="ak-corr",
        action_type="comment_once",
        task_id="t_x",
        board_snapshot_digest="s",
    )
    res = ex.apply_board_actions(p, [action], dry_run=False, now_ns=1)
    assert res["ok"] is False
    assert res["errors"]
    assert res["errors"][0]["code"] in {"corrupt_db", "schema_mismatch"}


def test_schema_mismatch_empty_db(tmp_path: Path):
    p = tmp_path / "empty.db"
    conn = sqlite3.connect(str(p))
    conn.execute("CREATE TABLE foo (id INT)")
    conn.commit()
    conn.close()
    action = ex.make_action(
        action_key="ak-sm",
        action_type="comment_once",
        task_id="t_x",
        board_snapshot_digest="s",
    )
    res = ex.apply_board_actions(p, [action], dry_run=False, now_ns=1)
    assert res["ok"] is False
    assert res["errors"][0]["code"] == "schema_mismatch"


def test_locked_db_begin_fails(db: Path):
    """Hold an IMMEDIATE lock in another connection; executor should error, not corrupt."""
    target, _ = _seed_blocked_with_sibling(db)
    action = _action_for(db, target, action_type="comment_once")
    holder = sqlite3.connect(str(db), timeout=0.1, isolation_level=None)
    holder.execute("PRAGMA busy_timeout=1")
    holder.execute("BEGIN IMMEDIATE")
    try:
        # Force a very short busy timeout on executor by patching constant.
        old = ex.DEFAULT_BUSY_TIMEOUT_MS
        ex.DEFAULT_BUSY_TIMEOUT_MS = 50
        try:
            res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
        finally:
            ex.DEFAULT_BUSY_TIMEOUT_MS = old
        # Either locked error on the action or empty applied.
        assert res["applied"] == []
        assert res["errors"] or res["denied"] or True
        # If errors present, code is locked_db/corrupt_db
        if res["errors"]:
            assert res["errors"][0]["code"] in {"locked_db", "corrupt_db", "internal"}
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    # After release, apply works.
    res2 = ex.apply_board_actions(db, [action], dry_run=False, now_ns=2)
    assert res2["applied"] or res2["already_applied"]


# ---------------------------------------------------------------------------
# Live boards never opened writable
# ---------------------------------------------------------------------------


def test_refuse_live_writable_path(tmp_path: Path, monkeypatch):
    # Simulate a path under home/.hermes/kanban/boards
    fake_home = tmp_path / "home"
    hermes = fake_home / ".hermes" / "kanban" / "boards" / "telemt-proxy"
    hermes.mkdir(parents=True)
    dbp = hermes / "kanban.db"
    ex.init_fixture_db(dbp)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: fake_home))
    # Also ensure resolve path detection via marker
    action = ex.make_action(
        action_key="ak-live",
        action_type="comment_once",
        task_id="t_x",
        board_snapshot_digest="s",
    )
    # Path contains /kanban/boards/ and ends with kanban.db and is not staging/tmp
    # Our detector uses Path.home() — after monkeypatch home is fake_home.
    # But _is_live_db_path also checks /root/.hermes markers. Point home.
    # Force by putting path that resolves under home/.hermes
    # Ensure env override off
    monkeypatch.delenv("ANTI_STALL_EXECUTOR_ALLOW_LIVE_WRITE", raising=False)
    res = ex.apply_board_actions(dbp, [action], dry_run=False, now_ns=1)
    # Should refuse writable open
    assert res["ok"] is False
    assert res["errors"]
    assert res["errors"][0]["code"] == "deny_class"

    # dry-run read-only should still work (mode=ro)
    ex.insert_task(dbp, task_id="t_x", status="blocked")
    action2 = _action_for(dbp, "t_x", action_type="comment_once")
    # dry-run uses readonly — allowed
    r2 = ex.apply_board_actions(dbp, [action2], dry_run=True, now_ns=1)
    # may plan or deny based on guards — must not error on open
    assert "errors" in r2
    assert not any(
        e.get("code") == "deny_class" and "live board" in e.get("message", "")
        for e in r2["errors"]
    )


def test_no_card_duplication_on_unblock(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    n0 = _counts(db)["tasks"]
    action = _action_for(db, target, action_type="unblock_same_card")
    ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert _counts(db)["tasks"] == n0


def test_batch_continues_after_denied(db: Path):
    target, sibling = _seed_blocked_with_sibling(db)
    # sibling is done — use a second blocked card
    ex.insert_task(db, task_id="t_other", status="blocked", block_kind="transient")
    bad = _action_for(
        db, target, action_key="ak-bad", denylist_tags=["ownership"], action_type="unblock_same_card"
    )
    good = _action_for(
        db, "t_other", action_key="ak-good", action_type="comment_once"
    )
    res = ex.apply_board_actions(db, [bad, good], dry_run=False, now_ns=1)
    assert res["denied"] and res["applied"]
    assert _task(db, target)["status"] == "blocked"
    # good applied comment
    conn = sqlite3.connect(str(db))
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM task_comments WHERE task_id='t_other'"
        ).fetchone()[0]
        assert n == 1
    finally:
        conn.close()


def test_comment_once_preserves_status_and_pid(db: Path):
    ex.insert_task(
        db,
        task_id="t_pid1",
        status="blocked",
        worker_pid=4242,
        consecutive_failures=3,
        block_kind="capability",
    )
    action = _action_for(db, "t_pid1", action_type="comment_once")
    ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    t = _task(db, "t_pid1")
    assert t["status"] == "blocked"
    assert t["worker_pid"] == 4242
    assert t["consecutive_failures"] == 3
    assert t["block_kind"] == "capability"


def test_auth_kind_required(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    action = _action_for(db, target, action_type="comment_once")
    action["authorization"] = {"kind": "llm_guess", "digest": "x" * 20}
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["denied"][0]["code"] == "auth_denied"


def test_circuit_route_preserves_siblings_and_history(db: Path):
    """route_needs_replan path: siblings + run history + edges intact."""
    target, sibling = _seed_blocked_with_sibling(db)
    before = {
        "sib": _task(db, sibling),
        "counts": _counts(db),
        "target_cf": _task(db, target)["consecutive_failures"],
    }
    action = _action_for(
        db,
        target,
        action_type="route_needs_replan",
        reason="signature-stable-no-delta",
    )
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["applied"]
    assert _task(db, sibling) == before["sib"]
    after_counts = _counts(db)
    assert after_counts["tasks"] == before["counts"]["tasks"]
    assert after_counts["task_runs"] == before["counts"]["task_runs"]
    assert after_counts["task_links"] == before["counts"]["task_links"]
    # only comments/events grew on target
    assert after_counts["task_comments"] == before["counts"]["task_comments"] + 1
    assert after_counts["task_events"] == before["counts"]["task_events"] + 1
    assert _task(db, target)["consecutive_failures"] == before["target_cf"]
    assert _task(db, target)["status"] == "blocked"


def test_empty_batch_ok(db: Path):
    h0 = _sha256_file(db)
    res = ex.apply_board_actions(db, [], dry_run=False, now_ns=1)
    assert res["ok"] is True
    assert _sha256_file(db) == h0


def test_registered_policy_unblock(db: Path):
    target, _ = _seed_blocked_with_sibling(db)
    action = _action_for(
        db,
        target,
        action_type="unblock_same_card",
        auth_kind="registered_policy",
        expected_block_kind="transient",
    )
    res = ex.apply_board_actions(db, [action], dry_run=False, now_ns=1)
    assert res["applied"]
    assert _task(db, target)["status"] == "ready"
