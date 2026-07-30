"""Transactional idempotent Kanban action executor (anti-stall staging W3).

Applies already-authorized decision_plan_v1 actions to a single named card
exactly once. Does not discover/classify policy and never creates cards.

Public API:
  - validate_action(action, board_snapshot_digest)
  - apply_board_actions(db_path, actions, *, dry_run, now_ns) -> dict
  - action_already_applied(connection, action_key) -> bool
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Optional

# ---------------------------------------------------------------------------
# Frozen adapter facts (mirrored in EXECUTOR_CONTRACT.json)
# ---------------------------------------------------------------------------

SUPPORTED_ACTION_TYPES = frozenset(
    {"comment_once", "unblock_same_card", "route_needs_replan"}
)
SUPPORTED_AUTH_KINDS = frozenset({"exact_directive", "registered_policy"})
DENY_CLASSES = frozenset(
    {
        "ambiguous",
        "needs_input",
        "human",
        "secret",
        "production",
        "ownership",
        "auth",
        "reviewer",
        "checksum",
    }
)

# Observed Hermes lifecycle (hermes_cli.kanban_db) — frozen at implement time.
VALID_STATUSES = frozenset(
    {
        "triage",
        "todo",
        "scheduled",
        "ready",
        "running",
        "blocked",
        "review",
        "done",
        "archived",
    }
)
VALID_BLOCK_KINDS = frozenset(
    {"dependency", "needs_input", "capability", "transient"}
)
# No native needs_replan status or block_kind exists in current lifecycle.
NEEDS_REPLAN_STATUS_SUPPORTED = False
NEEDS_REPLAN_BLOCK_KIND_SUPPORTED = False
UNBLOCK_TARGET_STATUSES = frozenset({"ready", "todo"})  # parent-gated
DEFAULT_BUSY_TIMEOUT_MS = 5_000  # bounded; full Hermes default is 120s
AUDIT_EVENT_KIND = "anti_stall_action"
AUDIT_AUTHOR = "anti_stall_executor"
ACTION_KEY_MARKER_PREFIX = "anti_stall_action_key:"

# Paths that must never be opened writable by this executor during proof.
_LIVE_DB_PATH_MARKERS = (
    "/root/.hermes/kanban.db",
    "/root/.hermes/kanban/boards/",
    "/home/",
)

_REQUIRED_ACTION_FIELDS = (
    "schema",
    "action_key",
    "action_type",
    "board_id",
    "task_id",
    "expected_status",
    "expected_evidence_digest",
    "event_watermark",
    "authorization",
    "reason",
)


# ---------------------------------------------------------------------------
# Errors / result helpers
# ---------------------------------------------------------------------------


class ExecutorError(Exception):
    """Base executor error with a stable machine code."""

    def __init__(self, code: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.extra = extra

    def as_dict(self) -> dict[str, Any]:
        d = {"code": self.code, "message": self.message}
        d.update(self.extra)
        return d


def _ns_to_sec(now_ns: int) -> int:
    if now_ns <= 0:
        return int(time.time())
    # Accept either ns or already-seconds (heuristic: seconds fit in 1e10).
    if now_ns < 10_000_000_000:
        return int(now_ns)
    return int(now_ns // 1_000_000_000)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _is_live_db_path(path: Path) -> bool:
    try:
        resolved = str(path.resolve())
    except Exception:
        resolved = str(path)
    # Allow explicit test override only.
    if os.environ.get("ANTI_STALL_EXECUTOR_ALLOW_LIVE_WRITE") == "1":
        return False
    # Staging lane artifacts are never treated as live boards.
    if "anti_stall_staging" in resolved:
        return False
    # Authoritative: anything under <home>/.hermes that looks like a board DB.
    try:
        home_hermes = str((Path.home() / ".hermes").resolve())
    except Exception:
        home_hermes = str(Path.home() / ".hermes")
    if resolved.startswith(home_hermes.rstrip("/") + os.sep) or resolved == home_hermes:
        if resolved.endswith("kanban.db") or "/kanban/boards/" in resolved:
            return True
    # Hard markers for known production roots (even if HOME differs).
    for marker in _LIVE_DB_PATH_MARKERS:
        m = marker.rstrip("/")
        if m and m in resolved and resolved.endswith("kanban.db"):
            # Fixture DBs intentionally placed under /tmp without home layout stay allowed
            # only when they are NOT under a real hermes boards tree marker with home.
            if resolved.startswith("/tmp/") and "/.hermes/" not in resolved:
                continue
            return True
    return False


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def validate_action(
    action: Mapping[str, Any], board_snapshot_digest: str
) -> dict[str, Any]:
    """Validate a single decision_plan_v1 action against frozen contract.

    Returns ``{"ok": True, "action": normalized}`` or
    ``{"ok": False, "code": ..., "message": ...}``.
    Never mutates the board.
    """
    try:
        normalized = _normalize_and_check(action, board_snapshot_digest)
        return {"ok": True, "action": normalized}
    except ExecutorError as e:
        return {"ok": False, **e.as_dict()}


def _normalize_and_check(
    action: Mapping[str, Any], board_snapshot_digest: str
) -> dict[str, Any]:
    if not isinstance(action, Mapping):
        raise ExecutorError("schema_mismatch", "action must be a mapping")

    for field in _REQUIRED_ACTION_FIELDS:
        if field not in action:
            raise ExecutorError(
                "schema_mismatch", f"missing required field: {field}", field=field
            )

    schema = action.get("schema")
    if schema not in ("decision_plan_v1", "decision_plan_v1_action"):
        raise ExecutorError(
            "schema_mismatch",
            f"unsupported action schema: {schema!r}",
            schema=schema,
        )

    action_key = action.get("action_key")
    if not isinstance(action_key, str) or not action_key.strip():
        raise ExecutorError("schema_mismatch", "action_key must be non-empty str")
    if len(action_key) > 256 or re.search(r"\s", action_key):
        raise ExecutorError(
            "schema_mismatch", "action_key must be <=256 chars without whitespace"
        )

    action_type = action.get("action_type")
    if action_type not in SUPPORTED_ACTION_TYPES:
        raise ExecutorError(
            "schema_mismatch",
            f"unsupported action_type: {action_type!r}",
            action_type=action_type,
        )

    board_id = action.get("board_id")
    if not isinstance(board_id, str) or not board_id.strip():
        raise ExecutorError("schema_mismatch", "board_id must be non-empty str")

    task_id = action.get("task_id")
    if not isinstance(task_id, str) or not re.fullmatch(r"t_[A-Za-z0-9_-]+", task_id or ""):
        raise ExecutorError("schema_mismatch", f"invalid task_id: {task_id!r}")

    expected_status = action.get("expected_status")
    if expected_status not in VALID_STATUSES:
        raise ExecutorError(
            "schema_mismatch",
            f"expected_status not in lifecycle: {expected_status!r}",
        )

    # Snapshot digest binding
    action_digest = action.get("board_snapshot_digest")
    if board_snapshot_digest is None or board_snapshot_digest == "":
        raise ExecutorError("hash_mismatch", "board_snapshot_digest required")
    if action_digest is not None and action_digest != board_snapshot_digest:
        raise ExecutorError(
            "hash_mismatch",
            "action board_snapshot_digest does not match provided snapshot digest",
            expected=board_snapshot_digest,
            got=action_digest,
        )

    evidence = action.get("expected_evidence_digest")
    if not isinstance(evidence, str) or len(evidence) < 8:
        raise ExecutorError(
            "schema_mismatch", "expected_evidence_digest must be a digest string"
        )

    wm = action.get("event_watermark")
    if not isinstance(wm, int) or wm < 0:
        raise ExecutorError(
            "schema_mismatch", "event_watermark must be non-negative int"
        )

    # expected_current_run_id may be null
    run_id = action.get("expected_current_run_id", None)
    if run_id is not None and not isinstance(run_id, int):
        raise ExecutorError(
            "schema_mismatch", "expected_current_run_id must be int or null"
        )

    auth = action.get("authorization")
    if not isinstance(auth, Mapping):
        raise ExecutorError("schema_mismatch", "authorization must be a mapping")
    auth_kind = auth.get("kind")
    if auth_kind not in SUPPORTED_AUTH_KINDS:
        raise ExecutorError(
            "auth_denied",
            f"authorization.kind not allowed: {auth_kind!r}",
            auth_kind=auth_kind,
        )
    auth_digest = auth.get("digest")
    if not isinstance(auth_digest, str) or len(auth_digest) < 8:
        raise ExecutorError(
            "auth_denied", "authorization.digest must be a non-trivial digest"
        )

    # Permanent denylist — deny classes may appear as tags or deny_class field.
    denylist = set()
    raw_deny = action.get("denylist_tags") or action.get("deny_classes") or []
    if isinstance(raw_deny, (list, tuple, set)):
        denylist |= {str(x).strip().lower() for x in raw_deny if str(x).strip()}
    single = action.get("deny_class")
    if isinstance(single, str) and single.strip():
        denylist.add(single.strip().lower())
    # Also scan reason/authorization for deny markers if policy stamped them.
    policy_stamp = str(action.get("policy_class") or "").strip().lower()
    if policy_stamp:
        denylist.add(policy_stamp)

    hit = sorted(denylist & DENY_CLASSES)
    if hit:
        raise ExecutorError(
            "deny_class",
            f"action permanently denied by class(es): {','.join(hit)}",
            deny_classes=hit,
        )

    # Unblock requires blocked + exact_directive|registered_policy (already checked).
    if action_type == "unblock_same_card":
        if expected_status != "blocked":
            raise ExecutorError(
                "guard_race",
                "unblock_same_card requires expected_status=blocked",
                expected_status=expected_status,
            )
        # needs_input block_kind is a permanent human class unless exact auth,
        # but policy layer should have stamped deny; double-check optional field.
        exp_bk = action.get("expected_block_kind")
        if exp_bk == "needs_input" and auth_kind != "exact_directive":
            # registered_policy may still authorize only if policy digest present
            # and not denied — leave to denylist. If policy_class says needs_input deny:
            pass
        if exp_bk == "needs_input" and action.get("force_needs_input_unblock") is not True:
            # Fail closed: needs_input never auto-unblocked without explicit force
            # flag AND exact_directive (both required).
            if not (
                auth_kind == "exact_directive"
                and action.get("resolution_directive_digest")
            ):
                raise ExecutorError(
                    "deny_class",
                    "needs_input blocks cannot be auto-unblocked without exact directive",
                    deny_classes=["needs_input"],
                )

    reason = action.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        raise ExecutorError("schema_mismatch", "reason must be non-empty str")
    if _looks_like_secret(reason):
        raise ExecutorError(
            "deny_class",
            "reason appears to contain secrets; refused",
            deny_classes=["secret"],
        )

    comment_body = action.get("comment_body")
    if comment_body is not None:
        if not isinstance(comment_body, str):
            raise ExecutorError("schema_mismatch", "comment_body must be str")
        if _looks_like_secret(comment_body):
            raise ExecutorError(
                "deny_class",
                "comment_body appears to contain secrets; refused",
                deny_classes=["secret"],
            )

    cooldown_until_ns = action.get("cooldown_until_ns", 0)
    if cooldown_until_ns is None:
        cooldown_until_ns = 0
    if not isinstance(cooldown_until_ns, int) or cooldown_until_ns < 0:
        raise ExecutorError(
            "schema_mismatch", "cooldown_until_ns must be non-negative int"
        )

    row_watermark = action.get("row_watermark")
    # optional: tasks.created_at or a hash of critical columns

    normalized = {
        "schema": schema,
        "action_key": action_key.strip(),
        "action_type": action_type,
        "board_id": board_id.strip(),
        "board_snapshot_digest": action_digest or board_snapshot_digest,
        "task_id": task_id,
        "expected_status": expected_status,
        "expected_current_run_id": run_id,
        "expected_evidence_digest": evidence,
        "expected_block_kind": action.get("expected_block_kind"),
        "event_watermark": int(wm),
        "row_watermark": row_watermark,
        "authorization": {
            "kind": auth_kind,
            "digest": auth_digest,
        },
        "reason": reason.strip(),
        "comment_body": (comment_body.strip() if isinstance(comment_body, str) else None),
        "cooldown_until_ns": int(cooldown_until_ns),
        "denylist_tags": sorted(denylist),
        "resolution_directive_digest": action.get("resolution_directive_digest"),
    }
    return normalized


_SECRET_PATTERNS = [
    re.compile(r"(?i)(api[_-]?key|secret|password|token|bearer\s+[a-z0-9._\\-]+)\\s*[:=]"),
    re.compile(r"(?i)BEGIN (RSA |OPENSSH |EC )?PRIVATE KEY"),
    re.compile(r"ghp_[A-Za-z0-9]{20,}"),
    re.compile(r"sk-[A-Za-z0-9]{20,}"),
]


def _looks_like_secret(text: str) -> bool:
    for pat in _SECRET_PATTERNS:
        if pat.search(text):
            return True
    return False


# ---------------------------------------------------------------------------
# Idempotency via append-only lifecycle evidence
# ---------------------------------------------------------------------------


def action_already_applied(connection: sqlite3.Connection, action_key: str) -> bool:
    """Return True if action_key is present in append-only events or comments."""
    if not action_key or not isinstance(action_key, str):
        return False
    marker = f"{ACTION_KEY_MARKER_PREFIX}{action_key}"
    # Events: payload JSON contains action_key field or marker string.
    row = connection.execute(
        """
        SELECT 1 FROM task_events
         WHERE kind = ?
           AND (
                payload LIKE ?
             OR payload LIKE ?
           )
         LIMIT 1
        """,
        (
            AUDIT_EVENT_KIND,
            f'%\"action_key\": \"{action_key}\"%',
            f"%{marker}%",
        ),
    ).fetchone()
    if row:
        return True
    # Fallback: any event payload with exact marker (legacy/diagnostic).
    row = connection.execute(
        """
        SELECT 1 FROM task_events
         WHERE payload LIKE ?
         LIMIT 1
        """,
        (f"%{marker}%",),
    ).fetchone()
    if row:
        return True
    # Comments carry the canonical marker line.
    row = connection.execute(
        """
        SELECT 1 FROM task_comments
         WHERE body LIKE ?
         LIMIT 1
        """,
        (f"%{marker}%",),
    ).fetchone()
    return bool(row)


# ---------------------------------------------------------------------------
# DB open helpers
# ---------------------------------------------------------------------------


def _open_db(db_path: Path, *, readonly: bool) -> sqlite3.Connection:
    path = Path(db_path)
    if not path.exists():
        raise ExecutorError("schema_mismatch", f"db does not exist: {path}")

    if readonly:
        uri = path.resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=DEFAULT_BUSY_TIMEOUT_MS / 1000.0)
    else:
        if _is_live_db_path(path):
            raise ExecutorError(
                "deny_class",
                f"refusing writable open of live board DB: {path}",
                deny_classes=["production"],
            )
        conn = sqlite3.connect(
            str(path),
            isolation_level=None,
            timeout=DEFAULT_BUSY_TIMEOUT_MS / 1000.0,
        )
        conn.execute(f"PRAGMA busy_timeout={DEFAULT_BUSY_TIMEOUT_MS}")
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(f"PRAGMA busy_timeout={DEFAULT_BUSY_TIMEOUT_MS}")
    except sqlite3.Error:
        pass
    _assert_min_schema(conn)
    return conn


def _assert_min_schema(conn: sqlite3.Connection) -> None:
    try:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    except sqlite3.DatabaseError as e:
        raise ExecutorError("corrupt_db", f"sqlite error reading schema: {e}") from e
    required = {"tasks", "task_events", "task_comments", "task_links", "task_runs"}
    missing = sorted(required - tables)
    if missing:
        raise ExecutorError(
            "schema_mismatch",
            f"missing required tables: {missing}",
            missing=missing,
        )
    cols = {
        r[1]
        for r in conn.execute("PRAGMA table_info(tasks)").fetchall()
    }
    need_cols = {
        "id",
        "status",
        "current_run_id",
        "block_kind",
        "block_recurrences",
        "consecutive_failures",
        "worker_pid",
    }
    miss_c = sorted(need_cols - cols)
    if miss_c:
        raise ExecutorError(
            "schema_mismatch",
            f"tasks missing columns: {miss_c}",
            missing=miss_c,
        )


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


def apply_board_actions(
    db_path: Path,
    actions: list[dict],
    *,
    dry_run: bool,
    now_ns: int,
) -> dict[str, Any]:
    """Apply a board batch of authorized actions.

    Parameters
    ----------
    db_path:
        Path to a Hermes-root kanban SQLite DB (fixture in tests).
    actions:
        list of decision_plan_v1 action dicts (same board).
    dry_run:
        When True, open read-only and return planned/skipped/denied only.
    now_ns:
        Logical clock for cooldown checks (nanoseconds or seconds).

    Returns a result dict with keys:
      ok, dry_run, planned, applied, skipped, denied, already_applied, errors
    """
    db_path = Path(db_path)
    result: dict[str, Any] = {
        "ok": True,
        "dry_run": bool(dry_run),
        "db_path": str(db_path),
        "planned": [],
        "applied": [],
        "skipped": [],
        "denied": [],
        "already_applied": [],
        "errors": [],
    }

    if not isinstance(actions, list):
        result["ok"] = False
        result["errors"].append(
            {"code": "schema_mismatch", "message": "actions must be a list"}
        )
        return result

    # Grouping assumes same board; empty batch is success.
    if not actions:
        return result

    try:
        conn = _open_db(db_path, readonly=bool(dry_run))
    except ExecutorError as e:
        result["ok"] = False
        result["errors"].append(e.as_dict())
        return result
    except sqlite3.Error as e:
        result["ok"] = False
        result["errors"].append(
            {"code": "corrupt_db", "message": f"failed to open db: {e}"}
        )
        return result

    try:
        for raw in actions:
            rec = _process_one(conn, raw, dry_run=dry_run, now_ns=now_ns)
            bucket = rec.get("bucket", "errors")
            result.setdefault(bucket, []).append(rec)
            if bucket in ("denied", "errors") and rec.get("fatal_batch"):
                result["ok"] = False
        # ok stays True if we only had denied/skipped/already_applied —
        # those are fail-closed per-action successes of the executor itself.
        # Mark ok False only when an infrastructure error occurred.
        if result["errors"]:
            # Non-fatal per-action errors keep ok True unless flagged.
            if any(e.get("fatal_batch") for e in result["errors"]):
                result["ok"] = False
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return result


def _process_one(
    conn: sqlite3.Connection,
    raw: Mapping[str, Any],
    *,
    dry_run: bool,
    now_ns: int,
) -> dict[str, Any]:
    # Snapshot digest for validate: prefer action field, else empty check fails.
    snap = ""
    if isinstance(raw, Mapping):
        snap = str(raw.get("board_snapshot_digest") or "")
    try:
        action = _normalize_and_check(raw, snap or str(raw.get("board_snapshot_digest") or ""))
    except ExecutorError as e:
        return {
            "bucket": "denied",
            "status": "denied",
            "action_key": (raw or {}).get("action_key") if isinstance(raw, Mapping) else None,
            **e.as_dict(),
        }

    action_key = action["action_key"]
    base = {
        "action_key": action_key,
        "action_type": action["action_type"],
        "task_id": action["task_id"],
        "board_id": action["board_id"],
    }

    # Cooldown
    if action["cooldown_until_ns"] and now_ns < action["cooldown_until_ns"]:
        return {
            **base,
            "bucket": "skipped",
            "status": "skipped_cooldown",
            "message": "action still in cooldown window",
            "cooldown_until_ns": action["cooldown_until_ns"],
            "now_ns": now_ns,
        }

    # Idempotency (read path works in dry-run too)
    try:
        if action_already_applied(conn, action_key):
            return {
                **base,
                "bucket": "already_applied",
                "status": "already_applied",
                "message": "action_key present in append-only lifecycle evidence",
            }
    except sqlite3.Error as e:
        return {
            **base,
            "bucket": "errors",
            "status": "error",
            "code": "corrupt_db",
            "message": f"idempotency probe failed: {e}",
        }

    # Effective type after route_needs_replan downgrade decision.
    effective_type = action["action_type"]
    downgraded = False
    if effective_type == "route_needs_replan":
        if not (NEEDS_REPLAN_STATUS_SUPPORTED or NEEDS_REPLAN_BLOCK_KIND_SUPPORTED):
            effective_type = "comment_once"
            downgraded = True

    if dry_run:
        # CAS guards checked read-only; no writes.
        try:
            _check_cas_guards(conn, action)
        except ExecutorError as e:
            return {**base, "bucket": "denied", "status": "denied", **e.as_dict()}
        planned = {
            **base,
            "bucket": "planned",
            "status": "planned",
            "effective_type": effective_type,
            "downgraded_from_route_needs_replan": downgraded,
            "message": f"would apply {effective_type}",
            "reason": action["reason"],
        }
        return planned

    # Write path: one IMMEDIATE txn per action with CAS inside.
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as e:
        code = "locked_db" if "locked" in str(e).lower() or "busy" in str(e).lower() else "corrupt_db"
        return {
            **base,
            "bucket": "errors",
            "status": "error",
            "code": code,
            "message": f"BEGIN IMMEDIATE failed: {e}",
        }

    try:
        # Re-check idempotency inside the write lock.
        if action_already_applied(conn, action_key):
            conn.execute("COMMIT")
            return {
                **base,
                "bucket": "already_applied",
                "status": "already_applied",
                "message": "action_key present (raced) — no write",
            }

        _check_cas_guards(conn, action)
        apply_meta = _mutate(conn, action, effective_type=effective_type, downgraded=downgraded, now_ns=now_ns)
        conn.execute("COMMIT")
        return {
            **base,
            "bucket": "applied",
            "status": "applied",
            "effective_type": effective_type,
            "downgraded_from_route_needs_replan": downgraded,
            **apply_meta,
        }
    except ExecutorError as e:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        return {**base, "bucket": "denied", "status": "denied", **e.as_dict()}
    except sqlite3.Error as e:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        msg = str(e).lower()
        code = "locked_db" if ("locked" in msg or "busy" in msg) else "corrupt_db"
        return {
            **base,
            "bucket": "errors",
            "status": "error",
            "code": code,
            "message": f"write failed, rolled back: {e}",
        }
    except Exception as e:  # noqa: BLE001 — isolate unexpected failures per action
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        return {
            **base,
            "bucket": "errors",
            "status": "error",
            "code": "internal",
            "message": f"unexpected error, rolled back: {e}",
        }


def _check_cas_guards(conn: sqlite3.Connection, action: Mapping[str, Any]) -> None:
    task_id = action["task_id"]
    row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        raise ExecutorError(
            "guard_race", f"task not found: {task_id}", task_id=task_id
        )

    status = row["status"]
    if status != action["expected_status"]:
        raise ExecutorError(
            "guard_race",
            f"status CAS failed: expected {action['expected_status']!r} got {status!r}",
            expected=action["expected_status"],
            got=status,
        )

    cur_run = row["current_run_id"]
    exp_run = action["expected_current_run_id"]
    # Normalize both sides: missing/NULL == None
    if cur_run is not None:
        cur_run = int(cur_run)
    if exp_run != cur_run:
        raise ExecutorError(
            "guard_race",
            f"current_run_id CAS failed: expected {exp_run!r} got {cur_run!r}",
            expected=exp_run,
            got=cur_run,
        )

    exp_bk = action.get("expected_block_kind")
    if exp_bk is not None:
        got_bk = row["block_kind"] if "block_kind" in row.keys() else None
        if got_bk != exp_bk:
            raise ExecutorError(
                "guard_race",
                f"block_kind CAS failed: expected {exp_bk!r} got {got_bk!r}",
                expected=exp_bk,
                got=got_bk,
            )

    # Event watermark: max(id) must still equal planned watermark
    # (no new events on this task since plan).
    wm_row = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS m FROM task_events WHERE task_id = ?",
        (task_id,),
    ).fetchone()
    current_wm = int(wm_row["m"] if wm_row else 0)
    if current_wm != int(action["event_watermark"]):
        raise ExecutorError(
            "guard_race",
            f"event watermark CAS failed: expected {action['event_watermark']} got {current_wm}",
            expected=action["event_watermark"],
            got=current_wm,
        )

    # Optional row watermark: hash of stable identity columns.
    row_wm = action.get("row_watermark")
    if row_wm is not None:
        current_row_wm = _row_identity_watermark(row)
        if str(row_wm) != str(current_row_wm):
            raise ExecutorError(
                "hash_mismatch",
                "row_watermark mismatch",
                expected=row_wm,
                got=current_row_wm,
            )


def _row_identity_watermark(row: sqlite3.Row) -> str:
    parts = [
        str(row["id"]),
        str(row["status"]),
        str(row["current_run_id"]),
        str(row["block_kind"]) if "block_kind" in row.keys() else "",
        str(row["worker_pid"]),
        str(row["assignee"]),
        str(row["consecutive_failures"]),
    ]
    return _sha256_text("|".join(parts))


def _mutate(
    conn: sqlite3.Connection,
    action: Mapping[str, Any],
    *,
    effective_type: str,
    downgraded: bool,
    now_ns: int,
) -> dict[str, Any]:
    task_id = action["task_id"]
    action_key = action["action_key"]
    now_sec = _ns_to_sec(now_ns)
    marker = f"{ACTION_KEY_MARKER_PREFIX}{action_key}"
    meta: dict[str, Any] = {"mutations": []}

    # Snapshot counts for evidence of no sibling touch is handled by tests;
    # here we only touch the named task_id.

    new_status = None
    if effective_type == "unblock_same_card":
        new_status = _apply_unblock(conn, task_id, now_sec)
        meta["mutations"].append({"op": "unblock", "new_status": new_status})
        meta["new_status"] = new_status
    elif effective_type == "comment_once":
        # comment only — status untouched
        pass
    elif effective_type == "route_needs_replan":
        # Should not reach here when unsupported; defensive.
        raise ExecutorError(
            "schema_mismatch",
            "route_needs_replan has no lifecycle representation",
        )
    else:
        raise ExecutorError(
            "schema_mismatch", f"effective_type not supported: {effective_type}"
        )

    # Canonical audit comment (always) — includes action key, no secrets.
    body_lines = [
        f"[anti-stall-executor] {effective_type}",
        marker,
        f"reason: {action['reason']}",
        f"auth: {action['authorization']['kind']}:{action['authorization']['digest'][:12]}",
        f"evidence: {action['expected_evidence_digest'][:16]}",
    ]
    if downgraded:
        body_lines.append(
            "note: route_needs_replan downgraded to comment_once "
            "(no native needs_replan status/block_kind in lifecycle schema)"
        )
    if action.get("comment_body"):
        body_lines.append(action["comment_body"])
    comment_body = "\n".join(body_lines)

    cur = conn.execute(
        "INSERT INTO task_comments (task_id, author, body, created_at) VALUES (?, ?, ?, ?)",
        (task_id, AUDIT_AUTHOR, comment_body, now_sec),
    )
    comment_id = int(cur.lastrowid or 0)
    meta["comment_id"] = comment_id
    meta["mutations"].append({"op": "comment", "comment_id": comment_id})

    # Audit event (append-only lifecycle evidence carrying action_key).
    payload = {
        "action_key": action_key,
        "marker": marker,
        "action_type": action["action_type"],
        "effective_type": effective_type,
        "downgraded_from_route_needs_replan": downgraded,
        "reason": action["reason"],
        "auth_kind": action["authorization"]["kind"],
        "auth_digest": action["authorization"]["digest"],
        "evidence_digest": action["expected_evidence_digest"],
        "board_id": action["board_id"],
        "board_snapshot_digest": action.get("board_snapshot_digest"),
        "new_status": new_status,
        "comment_id": comment_id,
    }
    conn.execute(
        "INSERT INTO task_events (task_id, run_id, kind, payload, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            task_id,
            None,
            AUDIT_EVENT_KIND,
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
            now_sec,
        ),
    )
    # Also emit lifecycle-native events for unblock.
    if effective_type == "unblock_same_card" and new_status is not None:
        ub_payload = {"status": new_status} if new_status != "ready" else None
        conn.execute(
            "INSERT INTO task_events (task_id, run_id, kind, payload, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                task_id,
                None,
                "unblocked",
                json.dumps(ub_payload, ensure_ascii=False) if ub_payload else None,
                now_sec,
            ),
        )
        meta["mutations"].append({"op": "event", "kind": "unblocked"})

    meta["mutations"].append({"op": "event", "kind": AUDIT_EVENT_KIND})
    return meta


def _apply_unblock(conn: sqlite3.Connection, task_id: str, now_sec: int) -> str:
    """Mirror hermes_cli.kanban_db.unblock_task semantics for the same row.

    - Only from status IN ('blocked', 'scheduled') — we require blocked via CAS.
    - Close dangling current_run_id as reclaimed if still open.
    - new_status = todo if any parent not done, else ready.
    - Does NOT touch block_recurrences / block_kind / siblings / edges.
    - Resets consecutive_failures and last_failure_error; clears current_run_id.
    """
    row = conn.execute(
        "SELECT status, current_run_id FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()
    if row is None or row["status"] != "blocked":
        raise ExecutorError(
            "guard_race",
            f"unblock requires status=blocked at write time, got {None if row is None else row['status']!r}",
        )

    # Close stale open run if pointer set.
    if row["current_run_id"]:
        conn.execute(
            """
            UPDATE task_runs
               SET status = 'reclaimed', outcome = 'reclaimed',
                   summary = COALESCE(summary, 'invariant recovery on unblock'),
                   ended_at = ?,
                   claim_lock = NULL, claim_expires = NULL, worker_pid = NULL
             WHERE id = ? AND ended_at IS NULL
            """,
            (now_sec, int(row["current_run_id"])),
        )

    undone_parents = conn.execute(
        """
        SELECT 1 FROM task_links l
        JOIN tasks p ON p.id = l.parent_id
        WHERE l.child_id = ? AND p.status != 'done'
        LIMIT 1
        """,
        (task_id,),
    ).fetchone()
    new_status = "todo" if undone_parents else "ready"

    cur = conn.execute(
        """
        UPDATE tasks
           SET status = ?,
               current_run_id = NULL,
               consecutive_failures = 0,
               last_failure_error = NULL
         WHERE id = ?
           AND status = 'blocked'
        """,
        (new_status, task_id),
    )
    if cur.rowcount != 1:
        raise ExecutorError(
            "guard_race",
            "unblock UPDATE affected 0 rows (CAS)",
        )
    return new_status


# ---------------------------------------------------------------------------
# Fixture helpers (used by tests; safe, no live I/O)
# ---------------------------------------------------------------------------

# Minimal SCHEMA matching current Hermes lifecycle columns used by executor.
FIXTURE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    id                   TEXT PRIMARY KEY,
    title                TEXT NOT NULL,
    body                 TEXT,
    assignee             TEXT,
    status               TEXT NOT NULL,
    priority             INTEGER DEFAULT 0,
    created_by           TEXT,
    created_at           INTEGER NOT NULL,
    started_at           INTEGER,
    completed_at         INTEGER,
    workspace_kind       TEXT NOT NULL DEFAULT 'scratch',
    workspace_path       TEXT,
    branch_name          TEXT,
    project_id           TEXT,
    claim_lock           TEXT,
    claim_expires        INTEGER,
    tenant               TEXT,
    result               TEXT,
    idempotency_key      TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    worker_pid           INTEGER,
    last_failure_error   TEXT,
    max_runtime_seconds  INTEGER,
    last_heartbeat_at    INTEGER,
    current_run_id       INTEGER,
    workflow_template_id TEXT,
    current_step_key     TEXT,
    skills               TEXT,
    model_override       TEXT,
    max_retries          INTEGER,
    goal_mode            INTEGER NOT NULL DEFAULT 0,
    goal_max_turns       INTEGER,
    session_id           TEXT,
    block_kind           TEXT,
    block_recurrences    INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS task_links (
    parent_id  TEXT NOT NULL,
    child_id   TEXT NOT NULL,
    PRIMARY KEY (parent_id, child_id)
);
CREATE TABLE IF NOT EXISTS task_comments (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id    TEXT NOT NULL,
    author     TEXT NOT NULL,
    body       TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS task_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id    TEXT NOT NULL,
    run_id     INTEGER,
    kind       TEXT NOT NULL,
    payload    TEXT,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS task_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id             TEXT NOT NULL,
    profile             TEXT,
    step_key            TEXT,
    status              TEXT NOT NULL,
    claim_lock          TEXT,
    claim_expires       INTEGER,
    worker_pid          INTEGER,
    max_runtime_seconds INTEGER,
    last_heartbeat_at   INTEGER,
    started_at          INTEGER NOT NULL,
    ended_at            INTEGER,
    outcome             TEXT,
    summary             TEXT,
    metadata            TEXT,
    error               TEXT
);
CREATE TABLE IF NOT EXISTS task_attachments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id      TEXT NOT NULL,
    filename     TEXT NOT NULL,
    stored_path  TEXT NOT NULL,
    content_type TEXT,
    size         INTEGER NOT NULL DEFAULT 0,
    uploaded_by  TEXT,
    created_at   INTEGER NOT NULL
);
"""


def init_fixture_db(db_path: Path) -> Path:
    """Create a minimal Hermes-lifecycle SQLite DB at db_path."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(FIXTURE_SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()
    return db_path


def insert_task(
    db_path: Path,
    *,
    task_id: str,
    title: str = "t",
    status: str = "blocked",
    block_kind: Optional[str] = None,
    current_run_id: Optional[int] = None,
    assignee: str = "worker",
    worker_pid: Optional[int] = None,
    consecutive_failures: int = 0,
    created_at: Optional[int] = None,
    body: str = "",
) -> None:
    now = created_at if created_at is not None else int(time.time())
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, body, assignee, status, created_at,
                current_run_id, block_kind, worker_pid, consecutive_failures
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                title,
                body,
                assignee,
                status,
                now,
                current_run_id,
                block_kind,
                worker_pid,
                consecutive_failures,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def event_watermark(db_path: Path, task_id: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM task_events WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def make_action(
    *,
    action_key: str,
    action_type: str,
    task_id: str,
    board_id: str = "fixture-board",
    board_snapshot_digest: str = "snap-" + ("a" * 60),
    expected_status: str = "blocked",
    expected_current_run_id: Optional[int] = None,
    expected_evidence_digest: str = "ev-" + ("b" * 60),
    event_watermark: int = 0,
    auth_kind: str = "exact_directive",
    auth_digest: str = "auth-" + ("c" * 60),
    reason: str = "exact resolution directive present",
    comment_body: Optional[str] = None,
    cooldown_until_ns: int = 0,
    denylist_tags: Optional[list] = None,
    expected_block_kind: Optional[str] = None,
    resolution_directive_digest: Optional[str] = None,
    row_watermark: Optional[str] = None,
    **extra: Any,
) -> dict[str, Any]:
    a: dict[str, Any] = {
        "schema": "decision_plan_v1",
        "action_key": action_key,
        "action_type": action_type,
        "board_id": board_id,
        "board_snapshot_digest": board_snapshot_digest,
        "task_id": task_id,
        "expected_status": expected_status,
        "expected_current_run_id": expected_current_run_id,
        "expected_evidence_digest": expected_evidence_digest,
        "event_watermark": event_watermark,
        "authorization": {"kind": auth_kind, "digest": auth_digest},
        "reason": reason,
        "comment_body": comment_body,
        "cooldown_until_ns": cooldown_until_ns,
        "denylist_tags": list(denylist_tags or []),
    }
    if expected_block_kind is not None:
        a["expected_block_kind"] = expected_block_kind
    if resolution_directive_digest is not None:
        a["resolution_directive_digest"] = resolution_directive_digest
    if row_watermark is not None:
        a["row_watermark"] = row_watermark
    a.update(extra)
    return a


__all__ = [
    "validate_action",
    "apply_board_actions",
    "action_already_applied",
    "init_fixture_db",
    "insert_task",
    "event_watermark",
    "make_action",
    "SUPPORTED_ACTION_TYPES",
    "DENY_CLASSES",
    "NEEDS_REPLAN_STATUS_SUPPORTED",
    "NEEDS_REPLAN_BLOCK_KIND_SUPPORTED",
    "AUDIT_EVENT_KIND",
    "ACTION_KEY_MARKER_PREFIX",
    "ExecutorError",
]
