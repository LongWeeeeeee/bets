"""Pure deterministic anti-stall decision engine (decision_plan_v1).

No filesystem / SQLite / process mutation. Never calls an LLM.
Converts snapshot_v1 + prior state_v1 + policy -> decision_plan_v1.
"""
from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

SCHEMA_NAME = "decision_plan_v1"
SCHEMA_VERSION = 1
POLICY_SCHEMA = "anti_stall_policy_v1"
STATE_SCHEMA = "state_v1"
SNAPSHOT_SCHEMA = "snapshot_v1"
DIRECTIVE_PREFIX = "ANTI_STALL_RESOLUTION_V1="
DIRECTIVE_ACTION = "unblock_same_card"
RULE_DEPENDENCY = "dependency_all_parents_success_v1"
HEX64 = re.compile(r"^[0-9a-f]{64}$")
NS_PER_SEC = 1_000_000_000

# Permanent deny classes — never auto-unblock even if a directive is present.
DEFAULT_DENY_CLASSES = (
    "needs_input",
    "human",
    "secret",
    "credential",
    "auth",
    "production_safety",
    "ownership",
    "reviewer_issues",
    "checksum_mismatch",
    "evidence_mismatch",
    "stale_approval",
)

_DEFAULT_POLICY_PATH = Path(__file__).resolve().parent / "policy.json"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_obj(value: Any) -> str:
    return _sha256_text(_canonical_json(value))


def policy_digest(policy: Mapping[str, Any]) -> str:
    return _sha256_obj(dict(policy))


def load_default_policy() -> dict:
    with open(_DEFAULT_POLICY_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def normalize_signature(value: Any) -> str:
    """Normalize an arbitrary failure/block signature to a stable lowercase token."""
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        text = _canonical_json(value)
    else:
        text = str(value)
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w.\-:/|=+@ ]+", "", text)
    text = text.strip()
    return text


def _is_hex64(value: Any) -> bool:
    return isinstance(value, str) and bool(HEX64.fullmatch(value))


def _as_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _diag(code: str, message: str, **kwargs: Any) -> dict:
    out = {"code": code, "message": message}
    for k in ("board_id", "task_id", "evidence"):
        if k in kwargs and kwargs[k] is not None:
            out[k] = kwargs[k]
    return out


def _empty_plan(
    *,
    now_ns: int,
    policy: Mapping[str, Any],
    ok: bool,
    fail_closed: bool,
    diagnostics: list,
    classifications: Optional[list] = None,
    actions: Optional[list] = None,
    snapshot_digest: Optional[str] = None,
    state_observations: Optional[dict] = None,
) -> dict:
    plan = {
        "schema": SCHEMA_NAME,
        "version": SCHEMA_VERSION,
        "now_ns": int(now_ns),
        "policy_version": int(policy.get("version", 0) or 0),
        "policy_digest": policy_digest(policy) if policy else ("0" * 64),
        "ok": bool(ok),
        "fail_closed": bool(fail_closed),
        "diagnostics": list(diagnostics or []),
        "classifications": list(classifications or []),
        "actions": list(actions or []),
        "state_observations": dict(state_observations or {}),
    }
    if snapshot_digest is not None:
        plan["snapshot_digest"] = snapshot_digest
    return plan


def _validate_policy(policy: Any) -> tuple[Optional[dict], list]:
    diags: list = []
    if not isinstance(policy, dict):
        return None, [_diag("policy_invalid", "policy must be an object")]
    if policy.get("schema") not in (None, POLICY_SCHEMA) and policy.get("schema") != "policy_v1":
        # Accept frozen schema name or legacy short name; unknown fails closed.
        if not str(policy.get("schema", "")).startswith("anti_stall_policy"):
            diags.append(_diag("policy_schema_unknown", f"unknown policy schema {policy.get('schema')!r}"))
            return None, diags
    version = _as_int(policy.get("version"))
    if version is None or version < 1:
        diags.append(_diag("policy_version_invalid", "policy.version must be integer >= 1"))
        return None, diags
    required_lists = ("permanent_deny_classes", "allowed_actions", "registered_rules")
    for key in required_lists:
        if key not in policy:
            # fill from defaults later is not allowed for required semantic keys when absent:
            # we allow missing and use engine defaults, but version must exist.
            pass
    # Reject unknown critical types
    for key in (
        "max_no_progress_seconds_default",
        "min_no_progress_observations",
        "min_stall_snapshots",
        "protocol_max_attempts",
        "default_action_cooldown_seconds",
        "stale_heartbeat_seconds",
        "tick_interval_seconds",
    ):
        if key in policy and _as_int(policy.get(key)) is None:
            diags.append(_diag("policy_field_invalid", f"policy.{key} must be int"))
            return None, diags
    return dict(policy), diags


def _merge_policy_defaults(policy: dict) -> dict:
    base = load_default_policy()
    merged = deepcopy(base)
    merged.update(policy)
    # preserve nested deny aliases if caller provided full map
    if "deny_class_aliases" in policy and isinstance(policy["deny_class_aliases"], dict):
        merged["deny_class_aliases"] = dict(policy["deny_class_aliases"])
    return merged


def _validate_prior_state(prior_state: Any, now_ns: int) -> tuple[Optional[dict], list]:
    diags: list = []
    if prior_state is None:
        return {
            "schema": STATE_SCHEMA,
            "last_tick_ns": None,
            "tasks": {},
            "action_cooldowns": {},
        }, []
    if not isinstance(prior_state, dict):
        return None, [_diag("prior_state_corrupt", "prior_state must be object or null")]
    schema = prior_state.get("schema")
    if schema not in (None, STATE_SCHEMA, "anti_stall_state_v1"):
        return None, [_diag("prior_state_schema_unknown", f"unknown prior_state schema {schema!r}")]
    last_tick = prior_state.get("last_tick_ns")
    if last_tick is not None:
        lt = _as_int(last_tick)
        if lt is None:
            return None, [_diag("prior_state_corrupt", "last_tick_ns not int")]
        if lt > now_ns:
            return None, [
                _diag(
                    "clock_reversal",
                    f"prior last_tick_ns {lt} > now_ns {now_ns}",
                    evidence={"last_tick_ns": lt, "now_ns": now_ns},
                )
            ]
    tasks = prior_state.get("tasks", {})
    if tasks is None:
        tasks = {}
    if not isinstance(tasks, dict):
        return None, [_diag("prior_state_corrupt", "prior_state.tasks must be object")]
    cooldowns = prior_state.get("action_cooldowns", {})
    if cooldowns is None:
        cooldowns = {}
    if not isinstance(cooldowns, dict):
        return None, [_diag("prior_state_corrupt", "action_cooldowns must be object")]
    cleaned = {
        "schema": STATE_SCHEMA,
        "last_tick_ns": _as_int(prior_state.get("last_tick_ns")),
        "policy_digest": prior_state.get("policy_digest"),
        "tasks": tasks,
        "action_cooldowns": cooldowns,
    }
    return cleaned, diags


def _validate_snapshot(snapshot: Any) -> tuple[Optional[dict], list, Optional[str]]:
    diags: list = []
    if not isinstance(snapshot, dict):
        return None, [_diag("snapshot_invalid", "snapshot must be object")], None
    schema = snapshot.get("schema")
    if schema not in (None, SNAPSHOT_SCHEMA, "anti_stall_snapshot_v1"):
        return None, [_diag("snapshot_schema_unknown", f"unknown snapshot schema {schema!r}")], None
    # unknown top-level noise is ok if boards present; reject non-list boards
    boards = snapshot.get("boards")
    if boards is None:
        # allow tasks flat form
        tasks = snapshot.get("tasks")
        if isinstance(tasks, list):
            boards = [
                {
                    "board_id": snapshot.get("board_id") or "default",
                    "db_identity": snapshot.get("db_identity")
                    or snapshot.get("db_path")
                    or snapshot.get("board_id")
                    or "default",
                    "tasks": tasks,
                    "diagnostics": snapshot.get("board_diagnostics") or [],
                }
            ]
        else:
            return None, [_diag("snapshot_invalid", "snapshot.boards missing and no tasks[]")], None
    if not isinstance(boards, list):
        return None, [_diag("snapshot_invalid", "snapshot.boards must be list")], None

    # Reject unknown required-field type corruption inside tasks early? keep soft and mark per-task.
    digest = snapshot.get("snapshot_digest")
    if digest is not None and not _is_hex64(digest):
        return None, [_diag("snapshot_digest_invalid", "snapshot_digest must be 64 lowercase hex")], None
    if digest is None:
        # compute stable digest over boards/tasks content only
        digest = _sha256_obj({"boards": boards, "schema": SNAPSHOT_SCHEMA})
    return {"schema": SNAPSHOT_SCHEMA, "boards": boards, "snapshot_digest": digest, "raw": snapshot}, diags, digest


def _task_key(board_id: str, task_id: str) -> str:
    return f"{board_id}::{task_id}"


def _iter_tasks(snapshot: dict) -> Iterable[tuple[dict, dict]]:
    for board in snapshot.get("boards") or []:
        if not isinstance(board, dict):
            continue
        board_id = str(board.get("board_id") or board.get("id") or "unknown")
        db_identity = str(
            board.get("db_identity")
            or board.get("db_path")
            or board.get("db_sha256")
            or board_id
        )
        tasks = board.get("tasks") or []
        if not isinstance(tasks, list):
            continue
        for task in tasks:
            if not isinstance(task, dict):
                continue
            t = dict(task)
            t.setdefault("board_id", board_id)
            t.setdefault("db_identity", db_identity)
            yield board, t


def _index_tasks(snapshot: dict) -> dict[str, dict]:
    idx: dict[str, dict] = {}
    for _board, task in _iter_tasks(snapshot):
        tid = str(task.get("task_id") or task.get("id") or "")
        if not tid:
            continue
        idx[_task_key(str(task["board_id"]), tid)] = task
        # also plain task_id last-write (for single-board fixtures)
        idx.setdefault(tid, task)
    return idx


def _progress_digest(task: Mapping[str, Any]) -> str:
    if task.get("progress_digest") and _is_hex64(task["progress_digest"]):
        return task["progress_digest"]
    payload = {
        "artifact_digest": task.get("artifact_digest") or _artifacts_digest(task),
        "events_digest": task.get("events_digest") or _events_digest(task),
        "tools_digest": task.get("tools_digest") or "",
        "result": task.get("result"),
        "status": task.get("status"),
        "current_run_id": task.get("current_run_id"),
    }
    return _sha256_obj(payload)


def _artifacts_digest(task: Mapping[str, Any]) -> str:
    if task.get("artifact_digest") and _is_hex64(task["artifact_digest"]):
        return task["artifact_digest"]
    arts = task.get("artifacts") or []
    if not isinstance(arts, list):
        return _sha256_obj([])
    norm = []
    for a in arts:
        if not isinstance(a, dict):
            continue
        norm.append(
            {
                "path": a.get("path"),
                "size": a.get("size"),
                "mtime_ns": a.get("mtime_ns"),
                "sha256": a.get("sha256"),
            }
        )
    norm.sort(key=lambda x: str(x.get("path") or ""))
    return _sha256_obj(norm)


def _events_digest(task: Mapping[str, Any]) -> str:
    if task.get("events_digest") and _is_hex64(task["events_digest"]):
        return task["events_digest"]
    events = task.get("events") or []
    if not isinstance(events, list):
        return _sha256_obj([])
    norm = []
    for e in events:
        if not isinstance(e, dict):
            continue
        norm.append(
            {
                "id": e.get("id") or e.get("event_id"),
                "kind": e.get("kind"),
                "ts": e.get("ts") or e.get("created_at") or e.get("created_at_ns"),
                "digest": e.get("digest") or e.get("sha256"),
            }
        )
    return _sha256_obj(norm)


def _evidence_digest(task: Mapping[str, Any]) -> str:
    if task.get("evidence_digest") and _is_hex64(task["evidence_digest"]):
        return task["evidence_digest"]
    return _sha256_obj(
        {
            "progress": _progress_digest(task),
            "block_signature": normalize_signature(
                task.get("block_signature") or task.get("block_reason") or ""
            ),
            "status": task.get("status"),
            "run_id": task.get("current_run_id"),
        }
    )


def _action_key(
    *,
    action: str,
    board_id: str,
    task_id: str,
    reason_code: str,
    auth_digest: str,
    expected_status: str,
    signature: str = "",
) -> str:
    return _sha256_obj(
        {
            "action": action,
            "board_id": board_id,
            "task_id": task_id,
            "reason_code": reason_code,
            "auth_digest": auth_digest,
            "expected_status": expected_status,
            "signature": normalize_signature(signature),
        }
    )


def _cooldown_active(prior_state: dict, action_key: str, now_ns: int, cooldown_s: int) -> bool:
    cds = prior_state.get("action_cooldowns") or {}
    entry = cds.get(action_key)
    if entry is None:
        return False
    if isinstance(entry, dict):
        until = _as_int(entry.get("until_ns") or entry.get("expires_ns"))
        last = _as_int(entry.get("last_ns") or entry.get("applied_ns"))
    else:
        until = None
        last = _as_int(entry)
    if until is not None and now_ns < until:
        return True
    if last is not None and cooldown_s > 0:
        if now_ns - last < cooldown_s * NS_PER_SEC:
            return True
    return False


def _task_observations(prior_state: dict, board_id: str, task_id: str) -> list:
    tasks = prior_state.get("tasks") or {}
    rec = tasks.get(_task_key(board_id, task_id)) or tasks.get(task_id) or {}
    if not isinstance(rec, dict):
        return []
    obs = rec.get("observations") or []
    if not isinstance(obs, list):
        return []
    return [o for o in obs if isinstance(o, dict)]


def _prior_task_rec(prior_state: dict, board_id: str, task_id: str) -> dict:
    tasks = prior_state.get("tasks") or {}
    rec = tasks.get(_task_key(board_id, task_id)) or tasks.get(task_id) or {}
    return dict(rec) if isinstance(rec, dict) else {}


def _deny_matches(text: str, policy: Mapping[str, Any]) -> Optional[str]:
    if not text:
        return None
    norm = normalize_signature(text)
    aliases = policy.get("deny_class_aliases") or {}
    classes = list(policy.get("permanent_deny_classes") or DEFAULT_DENY_CLASSES)
    for cls in classes:
        names = [cls]
        if isinstance(aliases, dict) and cls in aliases:
            names.extend(list(aliases[cls] or []))
        for name in names:
            n = normalize_signature(name)
            if not n:
                continue
            if n == norm or n in norm.split() or f" {n} " in f" {norm} " or norm.startswith(n) or n in norm:
                # word-ish containment for block kinds
                if n == norm or re.search(rf"(^|[\s:|/_\-,.]){re.escape(n)}([\s:|/_\-,.]|$)", norm):
                    return cls
    # explicit block_kind exact
    for cls in classes:
        if normalize_signature(cls) == norm:
            return cls
    return None


def _collect_deny_class(task: Mapping[str, Any], policy: Mapping[str, Any]) -> Optional[str]:
    status = normalize_signature(task.get("status") or "")
    if status in set(normalize_signature(s) for s in (policy.get("needs_input_statuses") or ["needs_input"])):
        return "needs_input"
    candidates = [
        task.get("block_kind"),
        task.get("deny_class"),
        task.get("block_reason"),
        task.get("block_signature"),
        task.get("failure_signature"),
    ]
    tags = task.get("machine_tags") or []
    if isinstance(tags, list):
        candidates.extend(tags)
    if isinstance(task.get("deny_classes"), list):
        candidates.extend(task.get("deny_classes") or [])
    for c in candidates:
        if c is None:
            continue
        hit = _deny_matches(str(c), policy)
        if hit:
            return hit
    return None


def _is_success_parent(parent_task: Optional[Mapping[str, Any]], policy: Mapping[str, Any]) -> bool:
    if not parent_task:
        return False
    status = normalize_signature(parent_task.get("status") or "")
    success_statuses = {normalize_signature(s) for s in (policy.get("success_statuses") or ["done"])}
    if status not in success_statuses:
        return False
    # optional result token
    result = parent_task.get("result")
    if result is None:
        return True
    tokens = {normalize_signature(t) for t in (policy.get("success_result_tokens") or [])}
    r = normalize_signature(result)
    if not tokens:
        return True
    if r in tokens:
        return True
    # allow result blobs that start with success
    return any(r.startswith(t) for t in tokens if t)


def _immutable_source_map(task: Mapping[str, Any]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    sources = task.get("immutable_sources") or task.get("sources") or []
    if isinstance(sources, dict):
        sources = list(sources.values())
    if not isinstance(sources, list):
        return out
    for s in sources:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("source_id") or s.get("id") or "")
        kind = str(s.get("source_kind") or s.get("kind") or "")
        sha = s.get("source_sha256") or s.get("sha256") or s.get("digest")
        if sid and _is_hex64(sha):
            out[f"{kind}:{sid}"] = {
                "source_kind": kind,
                "source_id": sid,
                "source_sha256": sha,
            }
            out[sid] = out[f"{kind}:{sid}"]
    return out


def _artifact_hash_map(task: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for a in task.get("artifacts") or []:
        if not isinstance(a, dict):
            continue
        path = a.get("path")
        sha = a.get("sha256") or a.get("digest")
        if isinstance(path, str) and _is_hex64(sha):
            out[path] = sha
    # also accept artifact_files map
    amap = task.get("artifact_hashes") or {}
    if isinstance(amap, dict):
        for p, sha in amap.items():
            if isinstance(p, str) and _is_hex64(sha):
                out[p] = sha
    return out


def _extract_directive_payloads(task: Mapping[str, Any]) -> list[dict]:
    """Return list of candidate directive dicts from structured snapshot fields only."""
    found: list[dict] = []
    # structured field
    for key in ("resolution_directive", "resolution_directives", "resolution_markers"):
        val = task.get(key)
        if val is None:
            continue
        if isinstance(val, dict):
            found.append(val)
        elif isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    # marker may wrap line
                    if "directive" in item and isinstance(item["directive"], dict):
                        found.append(item["directive"])
                    elif "payload" in item and isinstance(item["payload"], dict):
                        found.append(item["payload"])
                    else:
                        found.append(item)
                elif isinstance(item, str) and item.startswith(DIRECTIVE_PREFIX):
                    try:
                        found.append(json.loads(item[len(DIRECTIVE_PREFIX) :]))
                    except json.JSONDecodeError:
                        found.append({"_raw_line": item, "_parse_error": True})
        elif isinstance(val, str) and val.startswith(DIRECTIVE_PREFIX):
            try:
                found.append(json.loads(val[len(DIRECTIVE_PREFIX) :]))
            except json.JSONDecodeError:
                found.append({"_raw_line": val, "_parse_error": True})

    # machine comments with exact line only (snapshot must pre-parse; we accept exact lines)
    for c in task.get("machine_comments") or []:
        if isinstance(c, dict):
            body = c.get("body") or c.get("text") or ""
        else:
            body = str(c)
        for line in str(body).splitlines():
            line = line.strip()
            if line.startswith(DIRECTIVE_PREFIX):
                try:
                    found.append(json.loads(line[len(DIRECTIVE_PREFIX) :]))
                except json.JSONDecodeError:
                    found.append({"_raw_line": line, "_parse_error": True})
    return found


def _directive_body_for_hash(directive: Mapping[str, Any]) -> dict:
    body = {k: directive[k] for k in directive.keys() if k != "directive_sha256"}
    return body


def validate_resolution_directive(
    task: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict:
    """Validate exact ANTI_STALL_RESOLUTION_V1 directive for a task.

    Returns dict:
      valid: bool
      reason: str
      directive: dict|None
      authorization_digest: str
      deny_class: str|None
      diagnostics: list
    """
    _ = snapshot  # snapshot available for future board-level checks
    diags: list = []
    task_id = str(task.get("task_id") or task.get("id") or "")
    deny = _collect_deny_class(task, policy)
    if deny:
        return {
            "valid": False,
            "reason": f"permanent_deny_class:{deny}",
            "directive": None,
            "authorization_digest": _sha256_text(f"deny:{deny}"),
            "deny_class": deny,
            "diagnostics": diags,
        }

    candidates = _extract_directive_payloads(task)
    if not candidates:
        return {
            "valid": False,
            "reason": "no_directive",
            "directive": None,
            "authorization_digest": _sha256_text("no_directive"),
            "deny_class": None,
            "diagnostics": diags,
        }

    block_sig = normalize_signature(task.get("block_signature") or task.get("block_reason") or "")
    sources = _immutable_source_map(task)
    art_map = _artifact_hash_map(task)

    valid_dirs: list[tuple[dict, str]] = []
    for d in candidates:
        if not isinstance(d, dict) or d.get("_parse_error"):
            diags.append(_diag("directive_parse_error", "directive JSON parse failed", task_id=task_id))
            continue
        # required fields
        required = [
            "version",
            "task_id",
            "block_signature",
            "action",
            "chosen_value",
            "evidence_path",
            "evidence_sha256",
            "source_kind",
            "source_id",
            "source_sha256",
            "directive_sha256",
        ]
        missing = [k for k in required if k not in d]
        if missing:
            diags.append(
                _diag(
                    "directive_missing_fields",
                    f"missing fields {missing}",
                    task_id=task_id,
                    evidence={"missing": missing},
                )
            )
            continue
        if _as_int(d.get("version")) != 1:
            diags.append(_diag("directive_version", "version must be 1", task_id=task_id))
            continue
        if str(d.get("task_id")) != task_id:
            diags.append(
                _diag(
                    "directive_task_mismatch",
                    f"directive task_id {d.get('task_id')!r} != {task_id!r}",
                    task_id=task_id,
                )
            )
            continue
        if normalize_signature(d.get("block_signature")) != block_sig:
            diags.append(
                _diag(
                    "directive_signature_mismatch",
                    "block_signature does not match task",
                    task_id=task_id,
                    evidence={
                        "directive": normalize_signature(d.get("block_signature")),
                        "task": block_sig,
                    },
                )
            )
            continue
        if d.get("action") != DIRECTIVE_ACTION:
            diags.append(
                _diag(
                    "directive_action_invalid",
                    f"action must be {DIRECTIVE_ACTION}",
                    task_id=task_id,
                )
            )
            continue
        # chosen_value must be JSON-serializable structure (dict/list/scalar)
        try:
            _canonical_json(d.get("chosen_value"))
        except (TypeError, ValueError):
            diags.append(_diag("directive_chosen_value", "chosen_value not JSON-canonical", task_id=task_id))
            continue
        epath = d.get("evidence_path")
        if not isinstance(epath, str) or not epath.startswith("/") or epath.endswith("/"):
            diags.append(_diag("directive_evidence_path", "evidence_path must be absolute file path", task_id=task_id))
            continue
        esha = d.get("evidence_sha256")
        if not _is_hex64(esha):
            diags.append(_diag("directive_evidence_sha", "evidence_sha256 must be 64 lowercase hex", task_id=task_id))
            continue
        # evidence must match snapshot artifacts
        if epath not in art_map:
            diags.append(
                _diag(
                    "directive_evidence_missing",
                    "evidence_path not present in snapshot artifacts",
                    task_id=task_id,
                    evidence={"path": epath},
                )
            )
            continue
        if art_map[epath] != esha:
            diags.append(
                _diag(
                    "directive_evidence_hash_mismatch",
                    "evidence sha256 does not match snapshot artifact",
                    task_id=task_id,
                    evidence={"path": epath, "directive": esha, "snapshot": art_map[epath]},
                )
            )
            continue
        sk = str(d.get("source_kind") or "")
        sid = str(d.get("source_id") or "")
        ssha = d.get("source_sha256")
        if not sk or not sid or not _is_hex64(ssha):
            diags.append(_diag("directive_source_invalid", "source_kind/id/sha256 invalid", task_id=task_id))
            continue
        src = sources.get(f"{sk}:{sid}") or sources.get(sid)
        if not src:
            diags.append(
                _diag(
                    "directive_source_missing",
                    "immutable source not in snapshot",
                    task_id=task_id,
                    evidence={"source_kind": sk, "source_id": sid},
                )
            )
            continue
        if src.get("source_sha256") != ssha:
            diags.append(
                _diag(
                    "directive_source_hash_mismatch",
                    "source_sha256 does not match snapshot immutable source",
                    task_id=task_id,
                    evidence={"directive": ssha, "snapshot": src.get("source_sha256")},
                )
            )
            continue
        # directive_sha256 over canonical JSON excluding itself
        body = _directive_body_for_hash(d)
        expect = _sha256_obj(body)
        if d.get("directive_sha256") != expect:
            diags.append(
                _diag(
                    "directive_hash_mismatch",
                    "directive_sha256 mismatch",
                    task_id=task_id,
                    evidence={"expected": expect, "got": d.get("directive_sha256")},
                )
            )
            continue
        valid_dirs.append((dict(d), expect))

    if not valid_dirs:
        return {
            "valid": False,
            "reason": "directive_invalid",
            "directive": None,
            "authorization_digest": _sha256_text("directive_invalid"),
            "deny_class": None,
            "diagnostics": diags,
        }
    if len(valid_dirs) > 1:
        # multiple distinct valid directives => ambiguous
        digests = {h for _, h in valid_dirs}
        if len(digests) > 1:
            return {
                "valid": False,
                "reason": "directive_ambiguous",
                "directive": None,
                "authorization_digest": _sha256_text("directive_ambiguous"),
                "deny_class": None,
                "diagnostics": diags
                + [_diag("directive_ambiguous", "multiple distinct valid directives", task_id=task_id)],
            }
    directive, auth = valid_dirs[0]
    return {
        "valid": True,
        "reason": "directive_valid",
        "directive": directive,
        "authorization_digest": auth,
        "deny_class": None,
        "diagnostics": diags,
    }


def _validate_dependency_rule(
    task: Mapping[str, Any],
    task_index: Mapping[str, dict],
    policy: Mapping[str, Any],
    policy_dig: str,
) -> dict:
    """Validate dependency_all_parents_success_v1 for a blocked task."""
    task_id = str(task.get("task_id") or task.get("id") or "")
    deny = _collect_deny_class(task, policy)
    if deny:
        return {"valid": False, "reason": f"permanent_deny_class:{deny}", "authorization_digest": _sha256_text(f"deny:{deny}"), "evidence": {}}

    tags = task.get("machine_tags") or []
    block_kind = normalize_signature(task.get("block_kind") or "")
    tagged = False
    if isinstance(tags, list):
        tagged = any(normalize_signature(t) in {RULE_DEPENDENCY, "dependency", "dependency_block"} for t in tags)
    if block_kind in {normalize_signature("dependency"), RULE_DEPENDENCY, "dependency_block"}:
        tagged = True
    if task.get("dependency_rule") == RULE_DEPENDENCY:
        tagged = True
    if not tagged:
        return {
            "valid": False,
            "reason": "not_dependency_tagged",
            "authorization_digest": _sha256_text("not_dependency"),
            "evidence": {},
        }

    # policy version/digest must match if task declares expected
    expected_policy = task.get("expected_policy_digest") or task.get("policy_digest")
    if expected_policy and expected_policy != policy_dig:
        return {
            "valid": False,
            "reason": "policy_digest_mismatch",
            "authorization_digest": _sha256_text("policy_mismatch"),
            "evidence": {"expected": expected_policy, "actual": policy_dig},
        }
    expected_ver = task.get("expected_policy_version")
    if expected_ver is not None and _as_int(expected_ver) != _as_int(policy.get("version")):
        return {
            "valid": False,
            "reason": "policy_version_mismatch",
            "authorization_digest": _sha256_text("policy_version_mismatch"),
            "evidence": {},
        }

    parents = task.get("parents") or []
    if not isinstance(parents, list) or not parents:
        return {
            "valid": False,
            "reason": "no_parents",
            "authorization_digest": _sha256_text("no_parents"),
            "evidence": {},
        }

    parent_evidence = []
    board_id = str(task.get("board_id") or "")
    for p in parents:
        if isinstance(p, dict):
            pid = str(p.get("task_id") or p.get("id") or "")
            declared_digest = p.get("evidence_digest") or p.get("digest")
        else:
            pid = str(p)
            declared_digest = None
        if not pid:
            return {
                "valid": False,
                "reason": "parent_missing_id",
                "authorization_digest": _sha256_text("parent_missing"),
                "evidence": {},
            }
        parent = task_index.get(_task_key(board_id, pid)) or task_index.get(pid)
        if parent is None:
            return {
                "valid": False,
                "reason": f"parent_missing:{pid}",
                "authorization_digest": _sha256_text(f"parent_missing:{pid}"),
                "evidence": {"parent_id": pid},
            }
        if not _is_success_parent(parent, policy):
            return {
                "valid": False,
                "reason": f"parent_not_success:{pid}",
                "authorization_digest": _sha256_text(f"parent_not_success:{pid}"),
                "evidence": {
                    "parent_id": pid,
                    "status": parent.get("status"),
                    "result": parent.get("result"),
                },
            }
        ped = _evidence_digest(parent)
        # if edge declared a digest, it must match current parent evidence (unchanged)
        if declared_digest and declared_digest != ped:
            return {
                "valid": False,
                "reason": f"parent_evidence_changed:{pid}",
                "authorization_digest": _sha256_text(f"parent_evidence_changed:{pid}"),
                "evidence": {"parent_id": pid, "declared": declared_digest, "actual": ped},
            }
        # also compare against task-recorded parent_evidence_digests map
        ped_map = task.get("parent_evidence_digests") or {}
        if isinstance(ped_map, dict) and pid in ped_map and ped_map[pid] != ped:
            return {
                "valid": False,
                "reason": f"parent_evidence_changed:{pid}",
                "authorization_digest": _sha256_text(f"parent_evidence_changed:{pid}"),
                "evidence": {"parent_id": pid, "declared": ped_map[pid], "actual": ped},
            }
        parent_evidence.append({"parent_id": pid, "evidence_digest": ped, "status": parent.get("status")})

    auth = _sha256_obj(
        {
            "rule": RULE_DEPENDENCY,
            "task_id": task_id,
            "parents": parent_evidence,
            "policy_digest": policy_dig,
        }
    )
    return {
        "valid": True,
        "reason": RULE_DEPENDENCY,
        "authorization_digest": auth,
        "evidence": {"parents": parent_evidence, "rule": RULE_DEPENDENCY},
    }


def _status_in(status: Any, names: Iterable[str]) -> bool:
    s = normalize_signature(status or "")
    return s in {normalize_signature(n) for n in names}


def _heartbeat_ns(task: Mapping[str, Any]) -> Optional[int]:
    for k in ("last_heartbeat_at_ns", "heartbeat_ns", "last_heartbeat_ns"):
        v = _as_int(task.get(k))
        if v is not None:
            return v
    # seconds form
    for k in ("last_heartbeat_at", "heartbeat_at"):
        v = task.get(k)
        iv = _as_int(v)
        if iv is None:
            continue
        # heuristic: values < 10^12 are seconds
        if iv < 10**12:
            return iv * NS_PER_SEC
        return iv
    return None


def _pid_state(task: Mapping[str, Any]) -> str:
    ps = task.get("pid_state") or task.get("process_state")
    if isinstance(ps, str) and ps:
        return normalize_signature(ps)
    if task.get("pid_dead") is True:
        return "dead"
    if task.get("pid_alive") is True:
        return "alive"
    return "unknown"


def _has_artifacts_declared(task: Mapping[str, Any]) -> bool:
    if task.get("artifacts_declared") is True:
        return True
    if task.get("artifacts_declared") is False:
        return False
    # explicit owned paths list with content counts as declared only if flag true — fail closed
    return False


def _owned_paths_ok(task: Mapping[str, Any]) -> bool:
    if not _has_artifacts_declared(task):
        return False
    arts = task.get("artifacts") or []
    if not isinstance(arts, list) or not arts:
        # declared but empty required paths => not ok for status actions
        req = task.get("required_paths") or []
        return False if req else False
    # all artifacts must have path+sha
    for a in arts:
        if not isinstance(a, dict):
            return False
        if not a.get("path") or not _is_hex64(a.get("sha256") or a.get("digest")):
            return False
    return True


def _make_action(
    *,
    action: str,
    task: Mapping[str, Any],
    reason: str,
    reason_code: str,
    authorization_source: str,
    authorization_digest: str,
    policy: Mapping[str, Any],
    payload: Optional[dict] = None,
    signature: str = "",
) -> dict:
    board_id = str(task.get("board_id") or "")
    db_identity = str(task.get("db_identity") or board_id)
    task_id = str(task.get("task_id") or task.get("id") or "")
    expected_status = str(task.get("status") or "")
    expected_run_id = _as_int(task.get("current_run_id"))
    ed = _evidence_digest(task)
    cooldown = int(policy.get("default_action_cooldown_seconds") or 300)
    key = _action_key(
        action=action,
        board_id=board_id,
        task_id=task_id,
        reason_code=reason_code,
        auth_digest=authorization_digest,
        expected_status=expected_status,
        signature=signature,
    )
    out = {
        "action": action,
        "board_id": board_id,
        "db_identity": db_identity,
        "task_id": task_id,
        "expected_status": expected_status,
        "expected_run_id": expected_run_id,
        "expected_evidence_digest": ed,
        "reason": reason,
        "authorization_source": authorization_source,
        "authorization_digest": authorization_digest,
        "action_key": key,
        "cooldown_seconds": cooldown,
        "payload": payload or {},
    }
    return out


def _classify(
    *,
    classification: str,
    task: Mapping[str, Any],
    reason: str,
    evidence: Optional[dict] = None,
    severity: str = "info",
) -> dict:
    return {
        "board_id": str(task.get("board_id") or ""),
        "db_identity": str(task.get("db_identity") or task.get("board_id") or ""),
        "task_id": str(task.get("task_id") or task.get("id") or ""),
        "classification": classification,
        "reason": reason,
        "evidence": evidence or {},
        "severity": severity,
    }


def _unchanged_stall_window(
    observations: list,
    *,
    current_progress: str,
    current_class_family: str,
    min_snapshots: int,
) -> bool:
    """True if current stall signature appears unchanged across min_snapshots completed observations + current."""
    if min_snapshots < 1:
        min_snapshots = 1
    # need (min_snapshots - 1) prior observations matching + we count current separately by caller
    if len(observations) < max(0, min_snapshots - 1):
        return False
    recent = observations[-(min_snapshots - 1) :] if min_snapshots > 1 else []
    if min_snapshots == 1:
        return True
    for obs in recent:
        if obs.get("progress_digest") != current_progress:
            return False
        fam = str(obs.get("classification") or "")
        # family match: same classification or same prefix group
        if fam != current_class_family and not (
            fam.startswith(current_class_family) or current_class_family.startswith(fam.split("_")[0])
        ):
            # require exact classification match for destructive routing
            if fam != current_class_family:
                return False
    return len(recent) >= (min_snapshots - 1)


def _no_progress_observation_window(
    observations: list,
    *,
    now_ns: int,
    heartbeat_ns: Optional[int],
    current_progress: str,
    max_no_progress_s: int,
    min_observations: int,
    tick_interval_s: int,
) -> bool:
    """Fresh-heartbeat no-progress requires max_no_progress_seconds span and >= min_observations."""
    if heartbeat_ns is None:
        return False
    # collect observations with same progress digest
    same = [o for o in observations if o.get("progress_digest") == current_progress]
    # include synthetic current point
    points = list(same)
    if len(points) + 1 < min_observations:
        return False
    # use earliest same-progress obs timestamp
    times = []
    for o in points:
        t = _as_int(o.get("tick_ns") or o.get("heartbeat_ns"))
        if t is not None:
            times.append(t)
    times.append(now_ns)
    if not times:
        return False
    span_s = (max(times) - min(times)) / NS_PER_SEC
    if span_s < max_no_progress_s:
        return False
    # must span at least three 5-minute observations => min_observations points
    if len(points) + 1 < min_observations:
        return False
    # also require covering at least (min_observations-1)*tick_interval roughly
    min_span = (min_observations - 1) * tick_interval_s
    if span_s + 1e-9 < min(max_no_progress_s, min_span) and span_s < max_no_progress_s:
        return False
    return span_s >= max_no_progress_s and (len(points) + 1) >= min_observations


def _planner_has_executable_children(
    task: Mapping[str, Any],
    task_index: Mapping[str, dict],
    policy: Mapping[str, Any],
) -> tuple[bool, list]:
    children = task.get("children") or []
    if not isinstance(children, list):
        return False, []
    exec_statuses = {
        normalize_signature(s)
        for s in (policy.get("executable_child_statuses") or ["todo", "ready", "running", "done"])
    }
    board_id = str(task.get("board_id") or "")
    found = []
    for c in children:
        if isinstance(c, dict):
            cid = str(c.get("task_id") or c.get("id") or "")
            st = c.get("status")
        else:
            cid = str(c)
            st = None
        child = task_index.get(_task_key(board_id, cid)) or task_index.get(cid)
        if child is not None:
            st = child.get("status")
            cid = str(child.get("task_id") or child.get("id") or cid)
        if st is None:
            continue
        if normalize_signature(st) in exec_statuses:
            found.append({"task_id": cid, "status": st})
    return bool(found), found


def _parents_blocking(
    task: Mapping[str, Any],
    task_index: Mapping[str, dict],
    policy: Mapping[str, Any],
) -> list[dict]:
    parents = task.get("parents") or []
    if not isinstance(parents, list):
        return []
    board_id = str(task.get("board_id") or "")
    blocked_by = []
    success_statuses = {normalize_signature(s) for s in (policy.get("success_statuses") or ["done"])}
    for p in parents:
        if isinstance(p, dict):
            pid = str(p.get("task_id") or p.get("id") or "")
        else:
            pid = str(p)
        parent = task_index.get(_task_key(board_id, pid)) or task_index.get(pid)
        if parent is None:
            blocked_by.append({"parent_id": pid, "reason": "missing"})
            continue
        st = normalize_signature(parent.get("status") or "")
        if st not in success_statuses:
            blocked_by.append(
                {
                    "parent_id": pid,
                    "status": parent.get("status"),
                    "reason": "not_success",
                    "evidence_digest": _evidence_digest(parent),
                }
            )
    return blocked_by


def plan_tick(
    snapshot: dict,
    prior_state: dict,
    policy: dict,
    *,
    now_ns: int,
) -> dict:
    """Pure decision: snapshot_v1 + state_v1 + policy -> decision_plan_v1."""
    now_ns_i = _as_int(now_ns)
    if now_ns_i is None or now_ns_i < 0:
        pol = policy if isinstance(policy, dict) else {}
        return _empty_plan(
            now_ns=0,
            policy=pol,
            ok=False,
            fail_closed=True,
            diagnostics=[_diag("now_ns_invalid", "now_ns must be int >= 0")],
        )

    pol_in, pol_diags = _validate_policy(policy)
    if pol_in is None:
        return _empty_plan(
            now_ns=now_ns_i,
            policy=policy if isinstance(policy, dict) else {},
            ok=False,
            fail_closed=True,
            diagnostics=pol_diags,
        )
    pol = _merge_policy_defaults(pol_in)
    pol_dig = policy_digest(pol)

    state, state_diags = _validate_prior_state(prior_state, now_ns_i)
    if state is None:
        return _empty_plan(
            now_ns=now_ns_i,
            policy=pol,
            ok=False,
            fail_closed=True,
            diagnostics=state_diags,
        )

    snap, snap_diags, snap_dig = _validate_snapshot(snapshot)
    if snap is None:
        return _empty_plan(
            now_ns=now_ns_i,
            policy=pol,
            ok=False,
            fail_closed=True,
            diagnostics=snap_diags,
        )

    diagnostics: list = list(pol_diags) + list(state_diags) + list(snap_diags)
    # board-level diagnostics pass-through
    for board in snap.get("boards") or []:
        if isinstance(board, dict):
            for d in board.get("diagnostics") or []:
                if isinstance(d, dict):
                    diagnostics.append(
                        _diag(
                            str(d.get("code") or "board_diagnostic"),
                            str(d.get("message") or ""),
                            board_id=str(board.get("board_id") or ""),
                            evidence=d if isinstance(d, dict) else {},
                        )
                    )

    classifications: list = []
    actions: list = []
    state_observations: dict = {}
    task_index = _index_tasks(snap)

    min_stall_snapshots = int(pol.get("min_stall_snapshots") or 2)
    min_np_obs = int(pol.get("min_no_progress_observations") or 3)
    default_np = int(pol.get("max_no_progress_seconds_default") or 900)
    tick_interval = int(pol.get("tick_interval_seconds") or 300)
    protocol_max = int(pol.get("protocol_max_attempts") or 2)
    stale_hb_s = int(pol.get("stale_heartbeat_seconds") or 3600)
    planner_assignees = {normalize_signature(a) for a in (pol.get("planner_assignees") or ["planner"])}
    cooldown_s = int(pol.get("default_action_cooldown_seconds") or 300)

    def emit_action(act: dict) -> None:
        if act["action"] in (pol.get("status_actions") or ["unblock_same_card", "route_needs_replan"]):
            # status actions require artifacts_declared + owned paths
            t = task_index.get(_task_key(act["board_id"], act["task_id"])) or task_index.get(act["task_id"])
            if t is None or not _owned_paths_ok(t):
                diagnostics.append(
                    _diag(
                        "status_action_suppressed_missing_artifacts",
                        "status action suppressed: artifacts_declared/owned paths required",
                        board_id=act.get("board_id"),
                        task_id=act.get("task_id"),
                    )
                )
                # downgrade: no status mutation; optional diagnostic classification already set
                return
        if _cooldown_active(state, act["action_key"], now_ns_i, act.get("cooldown_seconds") or cooldown_s):
            diagnostics.append(
                _diag(
                    "action_cooldown_active",
                    f"action_key {act['action_key'][:12]}… cooldown active",
                    board_id=act.get("board_id"),
                    task_id=act.get("task_id"),
                    evidence={"action_key": act["action_key"]},
                )
            )
            return
        # de-dupe identical action_keys in this tick
        if any(a.get("action_key") == act["action_key"] for a in actions):
            return
        actions.append(act)

    for _board, task in _iter_tasks(snap):
        task_id = str(task.get("task_id") or task.get("id") or "")
        if not task_id:
            diagnostics.append(_diag("task_missing_id", "task without task_id skipped"))
            continue
        board_id = str(task.get("board_id") or "")
        status = str(task.get("status") or "")
        progress = _progress_digest(task)
        art_d = _artifacts_digest(task)
        ev_d = _events_digest(task)
        hb = _heartbeat_ns(task)
        pid_st = _pid_state(task)
        obs = _task_observations(state, board_id, task_id)
        prior_rec = _prior_task_rec(state, board_id, task_id)

        obs_record = {
            "tick_ns": now_ns_i,
            "status": status,
            "progress_digest": progress,
            "artifact_digest": art_d,
            "events_digest": ev_d,
            "classification": "pending",
            "signature": normalize_signature(
                task.get("failure_signature") or task.get("block_signature") or task.get("block_reason") or ""
            ),
            "heartbeat_ns": hb,
            "pid_state": pid_st,
        }

        # --- Planner ceiling ---
        assignee = normalize_signature(task.get("assignee") or "")
        if assignee in planner_assignees and _status_in(
            status, list(pol.get("running_statuses") or []) + list(pol.get("waiting_statuses") or []) + ["planning"]
        ):
            has_children, child_list = _planner_has_executable_children(task, task_index, pol)
            if has_children:
                cls = _classify(
                    classification="planner_ceiling",
                    task=task,
                    reason="planner_has_executable_children",
                    evidence={"children": child_list},
                    severity="warn",
                )
                classifications.append(cls)
                obs_record["classification"] = "planner_ceiling"
                msg = (
                    "ANTI_STALL: Planner already has executable children in "
                    "todo/ready/running/done. Complete/handoff this Planner card; "
                    "do not rewrite or complete children from supervisor."
                )
                auth = _sha256_obj({"kind": "planner_ceiling", "children": child_list})
                act = _make_action(
                    action="comment_once",
                    task=task,
                    reason="planner_ceiling_comment",
                    reason_code="planner_ceiling",
                    authorization_source="policy:planner_ceiling",
                    authorization_digest=auth,
                    policy=pol,
                    payload={"body": msg},
                    signature="planner_ceiling",
                )
                emit_action(act)
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

        # --- Descendant stalled by non-success parents ---
        if _status_in(status, list(pol.get("waiting_statuses") or []) + list(pol.get("running_statuses") or []) + list(pol.get("blocked_statuses") or [])):
            blockers = _parents_blocking(task, task_index, pol)
            # only classify waiting/todo/ready descendants primarily
            if blockers and _status_in(status, list(pol.get("waiting_statuses") or []) + ["blocked"]):
                # if already blocked for dependency, handled below; still classify
                if _status_in(status, pol.get("waiting_statuses") or []):
                    cls = _classify(
                        classification="descendant_stalled_by_parents",
                        task=task,
                        reason="parents_not_success",
                        evidence={"blockers": blockers},
                        severity="info",
                    )
                    classifications.append(cls)
                    obs_record["classification"] = "descendant_stalled_by_parents"
                    # diagnostic only — never create/reclaim
                    state_observations[_task_key(board_id, task_id)] = obs_record
                    # still may be blocked path if status blocked; fall through only if blocked
                    if not _status_in(status, pol.get("blocked_statuses") or []):
                        continue

        # --- Blocked / needs_input path ---
        if _status_in(status, list(pol.get("needs_input_statuses") or []) + list(pol.get("blocked_statuses") or [])):
            deny = _collect_deny_class(task, pol)
            if _status_in(status, pol.get("needs_input_statuses") or []) or deny == "needs_input":
                classifications.append(
                    _classify(
                        classification="blocked_needs_input",
                        task=task,
                        reason="needs_input_or_human",
                        evidence={"deny_class": deny or "needs_input"},
                        severity="warn",
                    )
                )
                obs_record["classification"] = "blocked_needs_input"
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            if deny:
                classifications.append(
                    _classify(
                        classification="blocked_deny_class",
                        task=task,
                        reason=f"permanent_deny:{deny}",
                        evidence={
                            "deny_class": deny,
                            "block_kind": task.get("block_kind"),
                            "block_signature": normalize_signature(
                                task.get("block_signature") or task.get("block_reason") or ""
                            ),
                        },
                        severity="warn",
                    )
                )
                obs_record["classification"] = "blocked_deny_class"
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            # exact directive?
            vdir = validate_resolution_directive(task, snap, pol)
            diagnostics.extend(vdir.get("diagnostics") or [])
            if vdir.get("valid"):
                classifications.append(
                    _classify(
                        classification="blocked_exact_directive",
                        task=task,
                        reason="exact_resolution_directive",
                        evidence={
                            "directive_sha256": vdir["directive"].get("directive_sha256"),
                            "authorization_digest": vdir["authorization_digest"],
                        },
                        severity="info",
                    )
                )
                obs_record["classification"] = "blocked_exact_directive"
                if _owned_paths_ok(task):
                    act = _make_action(
                        action="unblock_same_card",
                        task=task,
                        reason="exact_resolution_directive",
                        reason_code="exact_directive",
                        authorization_source="directive:ANTI_STALL_RESOLUTION_V1",
                        authorization_digest=vdir["authorization_digest"],
                        policy=pol,
                        payload={
                            "directive": vdir["directive"],
                            "chosen_value": vdir["directive"].get("chosen_value"),
                        },
                        signature=normalize_signature(
                            task.get("block_signature") or task.get("block_reason") or ""
                        ),
                    )
                    emit_action(act)
                else:
                    classifications.append(
                        _classify(
                            classification="missing_artifacts_declaration",
                            task=task,
                            reason="unblock_suppressed_no_artifacts",
                            evidence={"artifacts_declared": task.get("artifacts_declared")},
                            severity="warn",
                        )
                    )
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            # dependency rule
            vdep = _validate_dependency_rule(task, task_index, pol, pol_dig)
            if vdep.get("valid"):
                classifications.append(
                    _classify(
                        classification="blocked_dependency_rule",
                        task=task,
                        reason=RULE_DEPENDENCY,
                        evidence=vdep.get("evidence") or {},
                        severity="info",
                    )
                )
                obs_record["classification"] = "blocked_dependency_rule"
                if _owned_paths_ok(task):
                    act = _make_action(
                        action="unblock_same_card",
                        task=task,
                        reason=RULE_DEPENDENCY,
                        reason_code="dependency_rule",
                        authorization_source=f"rule:{RULE_DEPENDENCY}",
                        authorization_digest=vdep["authorization_digest"],
                        policy=pol,
                        payload={"rule": RULE_DEPENDENCY, "evidence": vdep.get("evidence")},
                        signature=RULE_DEPENDENCY,
                    )
                    emit_action(act)
                else:
                    classifications.append(
                        _classify(
                            classification="missing_artifacts_declaration",
                            task=task,
                            reason="unblock_suppressed_no_artifacts",
                            evidence={"artifacts_declared": task.get("artifacts_declared")},
                            severity="warn",
                        )
                    )
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            # same signature + no delta forbids unblock — circuit to replan if repeated
            sig = normalize_signature(task.get("block_signature") or task.get("block_reason") or "")
            prev_sig = normalize_signature(prior_rec.get("last_failure_signature") or "")
            prev_prog = None
            if obs:
                prev_prog = obs[-1].get("progress_digest")
            same_sig_no_delta = bool(sig) and sig == prev_sig and prev_prog == progress and prev_prog is not None

            if vdir.get("reason") in {"directive_ambiguous", "directive_invalid"} or not sig:
                classifications.append(
                    _classify(
                        classification="blocked_ambiguous",
                        task=task,
                        reason=vdir.get("reason") or "ambiguous_block",
                        evidence={"signature": sig, "directive_reason": vdir.get("reason")},
                        severity="warn",
                    )
                )
                obs_record["classification"] = "blocked_ambiguous"
                # diagnostic only
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            if same_sig_no_delta:
                classifications.append(
                    _classify(
                        classification="repeated_failure_signature",
                        task=task,
                        reason="same_signature_no_delta",
                        evidence={"signature": sig, "progress_digest": progress},
                        severity="error",
                    )
                )
                obs_record["classification"] = "repeated_failure_signature"
                # comment_once + route_needs_replan (at most once each via action keys)
                auth = _sha256_obj({"sig": sig, "progress": progress, "kind": "same_sig_no_delta"})
                comment = _make_action(
                    action="comment_once",
                    task=task,
                    reason="same_signature_no_delta_breaker",
                    reason_code="same_sig_no_delta_comment",
                    authorization_source="policy:same_signature_no_delta",
                    authorization_digest=auth,
                    policy=pol,
                    payload={
                        "body": (
                            f"ANTI_STALL: same normalized signature with zero artifact/event delta "
                            f"(sig={sig}). Retry/unblock forbidden; route needs_replan."
                        )
                    },
                    signature=sig,
                )
                emit_action(comment)
                if _owned_paths_ok(task):
                    route = _make_action(
                        action="route_needs_replan",
                        task=task,
                        reason="same_signature_no_delta_breaker",
                        reason_code="same_sig_no_delta_route",
                        authorization_source="policy:same_signature_no_delta",
                        authorization_digest=auth,
                        policy=pol,
                        payload={"signature": sig, "preserve_successful_siblings": True},
                        signature=sig,
                    )
                    emit_action(route)
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            # blocked without valid auto path — remain diagnostic
            classifications.append(
                _classify(
                    classification="blocked_ambiguous",
                    task=task,
                    reason="no_auto_resolution",
                    evidence={"signature": sig, "directive_reason": vdir.get("reason"), "dependency_reason": vdep.get("reason")},
                    severity="info",
                )
            )
            obs_record["classification"] = "blocked_ambiguous"
            state_observations[_task_key(board_id, task_id)] = obs_record
            continue

        # --- Running path ---
        if _status_in(status, pol.get("running_statuses") or ["running"]):
            # protocol violation
            protocol = bool(task.get("protocol_violation"))
            run_outcome = normalize_signature(task.get("run_outcome") or "")
            if task.get("protocol_rc0_without_terminal") is True:
                protocol = True
            if run_outcome in {"protocol_no_complete", "rc0_no_terminal"}:
                protocol = True
            prot_sig = normalize_signature(
                task.get("protocol_signature") or task.get("failure_signature") or "protocol_no_complete"
            )

            if protocol:
                prev_count = int(prior_rec.get("protocol_violation_count") or 0)
                prev_ps = normalize_signature(prior_rec.get("last_protocol_signature") or "")
                # count this observation as attempt N = prev identical + 1 if same sig else 1
                if prev_ps == prot_sig and prev_ps:
                    attempt_n = prev_count + 1
                else:
                    attempt_n = 1
                # also honor snapshot attempt_count if higher
                snap_attempts = _as_int(task.get("attempt_count") or task.get("protocol_attempt_count"))
                if snap_attempts is not None and snap_attempts > attempt_n:
                    attempt_n = snap_attempts

                evidence = {
                    "protocol_signature": prot_sig,
                    "attempt_n": attempt_n,
                    "protocol_max_attempts": protocol_max,
                    "progress_digest": progress,
                }
                classifications.append(
                    _classify(
                        classification="protocol_violation",
                        task=task,
                        reason="rc0_without_lifecycle_terminal",
                        evidence=evidence,
                        severity="error",
                    )
                )
                obs_record["classification"] = "protocol_violation"
                obs_record["signature"] = prot_sig
                obs_record["protocol_violation_count"] = attempt_n

                auth = _sha256_obj({"kind": "protocol", "sig": prot_sig, "attempt": attempt_n})
                if attempt_n >= protocol_max:
                    # circuit breaker — never third attempt
                    comment = _make_action(
                        action="comment_once",
                        task=task,
                        reason="protocol_circuit_breaker",
                        reason_code="protocol_breaker_comment",
                        authorization_source="policy:protocol_max_attempts",
                        authorization_digest=auth,
                        policy=pol,
                        payload={
                            "body": (
                                f"ANTI_STALL: protocol violation '{prot_sig}' reached {attempt_n}/"
                                f"{protocol_max} attempts with no lifecycle terminal. "
                                "Circuit breaker: route needs_replan; do not retry."
                            )
                        },
                        signature=prot_sig,
                    )
                    emit_action(comment)
                    if _owned_paths_ok(task):
                        route = _make_action(
                            action="route_needs_replan",
                            task=task,
                            reason="protocol_circuit_breaker",
                            reason_code="protocol_breaker_route",
                            authorization_source="policy:protocol_max_attempts",
                            authorization_digest=auth,
                            policy=pol,
                            payload={
                                "signature": prot_sig,
                                "attempt_n": attempt_n,
                                "preserve_successful_siblings": True,
                            },
                            signature=prot_sig,
                        )
                        emit_action(route)
                else:
                    # first violation: comment only (diagnostic); no auto retry/reclaim
                    comment = _make_action(
                        action="comment_once",
                        task=task,
                        reason="protocol_violation_observed",
                        reason_code="protocol_first_comment",
                        authorization_source="policy:protocol_observe",
                        authorization_digest=auth,
                        policy=pol,
                        payload={
                            "body": (
                                f"ANTI_STALL: protocol violation '{prot_sig}' attempt {attempt_n}/"
                                f"{protocol_max}. A second identical violation will circuit-break."
                            )
                        },
                        signature=prot_sig,
                    )
                    emit_action(comment)
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            # dead PID / stale heartbeat / no progress
            is_dead = pid_st in {"dead", "run_bound_dead", "not_running"}
            is_stale = False
            if task.get("heartbeat_stale") is True:
                is_stale = True
            if hb is not None and (now_ns_i - hb) > stale_hb_s * NS_PER_SEC:
                is_stale = True
            if pid_st in {"stale", "stale_heartbeat"}:
                is_stale = True

            # progress delta vs last observation
            last_prog = obs[-1].get("progress_digest") if obs else None
            last_art = obs[-1].get("artifact_digest") if obs else None
            last_ev = obs[-1].get("events_digest") if obs else None
            has_delta = False
            if last_prog is not None and last_prog != progress:
                has_delta = True
            if last_art is not None and last_art != art_d:
                has_delta = True
            if last_ev is not None and last_ev != ev_d:
                has_delta = True
            # tools digest
            tools_d = task.get("tools_digest") or ""
            if obs and tools_d and obs[-1].get("tools_digest") and obs[-1].get("tools_digest") != tools_d:
                has_delta = True

            if has_delta and not is_dead:
                classifications.append(
                    _classify(
                        classification="noop_healthy",
                        task=task,
                        reason="running_with_progress_delta",
                        evidence={
                            "progress_digest": progress,
                            "previous_progress_digest": last_prog,
                            "pid_state": pid_st,
                        },
                        severity="info",
                    )
                )
                obs_record["classification"] = "noop_healthy"
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            # no artifacts declaration => diagnostic only for any destructive route
            artifacts_ok = _owned_paths_ok(task)
            if not _has_artifacts_declared(task):
                # still can classify stalls but no status action (emit_action enforces)
                pass

            if is_dead:
                family = "running_dead_pid"
                window_ok = _unchanged_stall_window(
                    obs,
                    current_progress=progress,
                    current_class_family=family,
                    min_snapshots=min_stall_snapshots,
                )
                classifications.append(
                    _classify(
                        classification=family,
                        task=task,
                        reason="run_bound_dead_pid",
                        evidence={
                            "pid_state": pid_st,
                            "progress_digest": progress,
                            "window_ok": window_ok,
                            "min_stall_snapshots": min_stall_snapshots,
                            "observation_count": len(obs),
                        },
                        severity="error",
                    )
                )
                obs_record["classification"] = family
                if window_ok:
                    auth = _sha256_obj({"kind": "dead_pid", "progress": progress, "pid_state": pid_st})
                    comment = _make_action(
                        action="comment_once",
                        task=task,
                        reason="dead_pid_stall",
                        reason_code="dead_pid_comment",
                        authorization_source="policy:dead_pid",
                        authorization_digest=auth,
                        policy=pol,
                        payload={
                            "body": (
                                "ANTI_STALL: run-bound PID dead across required stall window; "
                                "route needs_replan (no reclaim/restart)."
                            )
                        },
                        signature=f"dead_pid:{pid_st}",
                    )
                    emit_action(comment)
                    if artifacts_ok:
                        route = _make_action(
                            action="route_needs_replan",
                            task=task,
                            reason="dead_pid_stall",
                            reason_code="dead_pid_route",
                            authorization_source="policy:dead_pid",
                            authorization_digest=auth,
                            policy=pol,
                            payload={"preserve_successful_siblings": True, "pid_state": pid_st},
                            signature=f"dead_pid:{pid_st}",
                        )
                        emit_action(route)
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            if is_stale and not has_delta:
                family = "running_stale_heartbeat"
                window_ok = _unchanged_stall_window(
                    obs,
                    current_progress=progress,
                    current_class_family=family,
                    min_snapshots=min_stall_snapshots,
                )
                classifications.append(
                    _classify(
                        classification=family,
                        task=task,
                        reason="stale_heartbeat_no_delta",
                        evidence={
                            "heartbeat_ns": hb,
                            "now_ns": now_ns_i,
                            "progress_digest": progress,
                            "window_ok": window_ok,
                        },
                        severity="error",
                    )
                )
                obs_record["classification"] = family
                if window_ok:
                    auth = _sha256_obj({"kind": "stale_hb", "progress": progress})
                    comment = _make_action(
                        action="comment_once",
                        task=task,
                        reason="stale_heartbeat_stall",
                        reason_code="stale_hb_comment",
                        authorization_source="policy:stale_heartbeat",
                        authorization_digest=auth,
                        policy=pol,
                        payload={
                            "body": (
                                "ANTI_STALL: stale heartbeat with zero progress delta across stall window; "
                                "route needs_replan."
                            )
                        },
                        signature="stale_heartbeat",
                    )
                    emit_action(comment)
                    if artifacts_ok:
                        route = _make_action(
                            action="route_needs_replan",
                            task=task,
                            reason="stale_heartbeat_stall",
                            reason_code="stale_hb_route",
                            authorization_source="policy:stale_heartbeat",
                            authorization_digest=auth,
                            policy=pol,
                            payload={"preserve_successful_siblings": True},
                            signature="stale_heartbeat",
                        )
                        emit_action(route)
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            # fresh heartbeat, zero delta
            max_np = _as_int(task.get("max_no_progress_seconds"))
            if max_np is None:
                max_np = default_np
            # consider "fresh" if heartbeat within stale threshold
            fresh = True
            if hb is not None and (now_ns_i - hb) > stale_hb_s * NS_PER_SEC:
                fresh = False
            if fresh and not has_delta:
                # need prior observations of same progress; if first tick with no prior, not enough
                family = "running_no_progress"
                window_ok = _no_progress_observation_window(
                    obs,
                    now_ns=now_ns_i,
                    heartbeat_ns=hb if hb is not None else now_ns_i,
                    current_progress=progress,
                    max_no_progress_s=int(max_np),
                    min_observations=min_np_obs,
                    tick_interval_s=tick_interval,
                )
                classifications.append(
                    _classify(
                        classification=family,
                        task=task,
                        reason="fresh_heartbeat_zero_delta",
                        evidence={
                            "progress_digest": progress,
                            "heartbeat_ns": hb,
                            "max_no_progress_seconds": int(max_np),
                            "min_observations": min_np_obs,
                            "window_ok": window_ok,
                            "observation_count": len(obs),
                            "artifacts_declared": bool(task.get("artifacts_declared")),
                        },
                        severity="warn",
                    )
                )
                obs_record["classification"] = family
                if window_ok:
                    auth = _sha256_obj(
                        {"kind": "no_progress", "progress": progress, "max_np": int(max_np)}
                    )
                    comment = _make_action(
                        action="comment_once",
                        task=task,
                        reason="no_progress_stall",
                        reason_code="no_progress_comment",
                        authorization_source="policy:no_progress",
                        authorization_digest=auth,
                        policy=pol,
                        payload={
                            "body": (
                                "ANTI_STALL: fresh heartbeat but zero required artifact/tool/event delta "
                                f"for >= {int(max_np)}s across >= {min_np_obs} observations; "
                                "route needs_replan (no reclaim)."
                            )
                        },
                        signature=f"no_progress:{progress[:16]}",
                    )
                    emit_action(comment)
                    if artifacts_ok:
                        route = _make_action(
                            action="route_needs_replan",
                            task=task,
                            reason="no_progress_stall",
                            reason_code="no_progress_route",
                            authorization_source="policy:no_progress",
                            authorization_digest=auth,
                            policy=pol,
                            payload={"preserve_successful_siblings": True, "progress_digest": progress},
                            signature=f"no_progress:{progress[:16]}",
                        )
                        emit_action(route)
                else:
                    # not enough window — diagnostic classification only (already added)
                    pass
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

            # running healthy default
            classifications.append(
                _classify(
                    classification="noop_healthy",
                    task=task,
                    reason="running_healthy_default",
                    evidence={"pid_state": pid_st, "progress_digest": progress},
                    severity="info",
                )
            )
            obs_record["classification"] = "noop_healthy"
            state_observations[_task_key(board_id, task_id)] = obs_record
            continue

        # --- repeated failure signature on non-blocked failed-like states ---
        fail_sig = normalize_signature(task.get("failure_signature") or "")
        if fail_sig and status in {"failed", "error"}:
            prev_sig = normalize_signature(prior_rec.get("last_failure_signature") or "")
            prev_prog = obs[-1].get("progress_digest") if obs else None
            if fail_sig == prev_sig and prev_prog == progress:
                classifications.append(
                    _classify(
                        classification="repeated_failure_signature",
                        task=task,
                        reason="same_signature_no_delta",
                        evidence={"signature": fail_sig, "progress_digest": progress},
                        severity="error",
                    )
                )
                obs_record["classification"] = "repeated_failure_signature"
                auth = _sha256_obj({"sig": fail_sig, "progress": progress})
                comment = _make_action(
                    action="comment_once",
                    task=task,
                    reason="same_signature_no_delta_breaker",
                    reason_code="same_sig_no_delta_comment",
                    authorization_source="policy:same_signature_no_delta",
                    authorization_digest=auth,
                    policy=pol,
                    payload={
                        "body": f"ANTI_STALL: repeated failure signature {fail_sig} with zero delta; no retry."
                    },
                    signature=fail_sig,
                )
                emit_action(comment)
                if _owned_paths_ok(task):
                    route = _make_action(
                        action="route_needs_replan",
                        task=task,
                        reason="same_signature_no_delta_breaker",
                        reason_code="same_sig_no_delta_route",
                        authorization_source="policy:same_signature_no_delta",
                        authorization_digest=auth,
                        policy=pol,
                        payload={"signature": fail_sig, "preserve_successful_siblings": True},
                        signature=fail_sig,
                    )
                    emit_action(route)
                state_observations[_task_key(board_id, task_id)] = obs_record
                continue

        # default: noop diagnostic for other statuses (done/archived/etc.)
        classifications.append(
            _classify(
                classification="noop_healthy",
                task=task,
                reason=f"status_{normalize_signature(status) or 'empty'}_noop",
                evidence={"status": status},
                severity="info",
            )
        )
        obs_record["classification"] = "noop_healthy"
        state_observations[_task_key(board_id, task_id)] = obs_record

    # deterministic sort
    classifications.sort(
        key=lambda c: (c.get("board_id", ""), c.get("task_id", ""), c.get("classification", ""), c.get("reason", ""))
    )
    actions.sort(
        key=lambda a: (
            a.get("board_id", ""),
            a.get("task_id", ""),
            a.get("action", ""),
            a.get("action_key", ""),
        )
    )

    return {
        "schema": SCHEMA_NAME,
        "version": SCHEMA_VERSION,
        "now_ns": now_ns_i,
        "policy_version": int(pol.get("version") or 1),
        "policy_digest": pol_dig,
        "snapshot_digest": snap_dig,
        "ok": True,
        "fail_closed": False,
        "diagnostics": diagnostics,
        "classifications": classifications,
        "actions": actions,
        "state_observations": state_observations,
    }


__all__ = [
    "normalize_signature",
    "validate_resolution_directive",
    "plan_tick",
    "policy_digest",
    "load_default_policy",
    "SCHEMA_NAME",
    "DIRECTIVE_PREFIX",
    "RULE_DEPENDENCY",
]
