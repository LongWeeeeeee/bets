"""Wire W1 snapshot + W2 decision + W3 executor into W4 runner adapters.

Parent modules keep their public contracts unchanged. This module only
translates shapes between them (state_v1 dual-view, action plan fields).
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Mapping, Optional

try:
    from . import decision as decision_mod
    from . import executor as executor_mod
    from . import snapshot as snapshot_mod
except ImportError:  # direct path / pytest without package context
    import decision as decision_mod  # type: ignore
    import executor as executor_mod  # type: ignore
    import snapshot as snapshot_mod  # type: ignore

PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_POLICY_PATH = PACKAGE_DIR / "policy.json"
DEFAULT_HERMES_ROOT = Path("/root/.hermes")


def _as_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _event_watermark_from_task(task: Mapping[str, Any]) -> int:
    events = task.get("events") or []
    max_id = 0
    if isinstance(events, list):
        for ev in events:
            if not isinstance(ev, Mapping):
                continue
            eid = _as_int(ev.get("id"))
            if eid is not None and eid > max_id:
                max_id = eid
    return int(max_id)


def _auth_kind(source: str) -> str:
    s = (source or "").strip().lower()
    if s.startswith("directive:") or s.startswith("exact_directive"):
        return "exact_directive"
    if s.startswith("rule:") or s.startswith("policy:") or s.startswith("registered"):
        return "registered_policy"
    # Fail closed at executor if unknown — still label as registered_policy only
    # when the decision engine stamped a known authorization_source family.
    if "directive" in s:
        return "exact_directive"
    return "registered_policy"


def _board_index(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for board in snapshot.get("boards") or []:
        if not isinstance(board, Mapping):
            continue
        bid = str(board.get("board_id") or "")
        db_id = str(board.get("db_identity") or board.get("db_path") or "")
        tasks_by_id: dict[str, Mapping[str, Any]] = {}
        for t in board.get("tasks") or []:
            if isinstance(t, Mapping) and t.get("task_id"):
                tasks_by_id[str(t["task_id"])] = t
        if bid:
            out[bid] = {
                "db_identity": db_id,
                "db_path": str(board.get("db_path") or db_id),
                "tasks": tasks_by_id,
                "board": board,
            }
        if db_id and db_id not in out:
            out[db_id] = out.get(bid) or {
                "db_identity": db_id,
                "db_path": str(board.get("db_path") or db_id),
                "tasks": tasks_by_id,
                "board": board,
            }
    return out


def translate_decision_action(
    action: Mapping[str, Any],
    *,
    snapshot: Mapping[str, Any],
    now_ns: int,
) -> Optional[dict[str, Any]]:
    """Map decision_plan_v1 action (W2) → executor action (W3).

    Returns None for pure noops (dropped, not executed).
    """
    if not isinstance(action, Mapping):
        return None
    atype = str(action.get("action") or action.get("action_type") or "")
    if atype in ("", "noop"):
        return None
    if atype not in executor_mod.SUPPORTED_ACTION_TYPES:
        # Unknown action types are not invented; skip fail-closed by omission.
        return None

    board_id = str(action.get("board_id") or "")
    task_id = str(action.get("task_id") or "")
    boards = _board_index(snapshot)
    board_rec = boards.get(board_id) or boards.get(str(action.get("db_identity") or ""))
    task = None
    if board_rec:
        task = (board_rec.get("tasks") or {}).get(task_id)
    if task is None:
        # fallback: scan all boards
        for rec in boards.values():
            task = (rec.get("tasks") or {}).get(task_id)
            if task is not None:
                board_rec = rec
                break

    snap_digest = str(
        snapshot.get("snapshot_digest")
        or snapshot.get("digest")
        or action.get("board_snapshot_digest")
        or ""
    )
    wm = _event_watermark_from_task(task) if isinstance(task, Mapping) else 0
    if isinstance(action.get("event_watermark"), int):
        wm = int(action["event_watermark"])

    auth_source = str(action.get("authorization_source") or "")
    auth_digest = str(action.get("authorization_digest") or "")
    if not auth_digest:
        auth_digest = "0" * 64

    payload = action.get("payload") if isinstance(action.get("payload"), Mapping) else {}
    comment_body = None
    if isinstance(payload, Mapping):
        body = payload.get("body") or payload.get("comment_body")
        if isinstance(body, str) and body.strip():
            comment_body = body

    cooldown_s = _as_int(action.get("cooldown_seconds")) or 0
    cooldown_until_ns = 0
    if cooldown_s > 0:
        cooldown_until_ns = int(now_ns) + int(cooldown_s) * 1_000_000_000

    expected_run = action.get("expected_run_id")
    if expected_run is None:
        expected_run = action.get("expected_current_run_id")
    expected_run_i = _as_int(expected_run)

    out: dict[str, Any] = {
        "schema": "decision_plan_v1",
        "action_key": str(action.get("action_key") or ""),
        "action_type": atype,
        "board_id": board_id,
        "board_snapshot_digest": snap_digest,
        "task_id": task_id,
        "expected_status": str(action.get("expected_status") or ""),
        "expected_current_run_id": expected_run_i,
        "expected_evidence_digest": str(action.get("expected_evidence_digest") or ""),
        "event_watermark": int(wm),
        "authorization": {
            "kind": _auth_kind(auth_source),
            "digest": auth_digest,
            "source": auth_source,
        },
        "reason": str(action.get("reason") or ""),
        "comment_body": comment_body,
        "cooldown_until_ns": int(cooldown_until_ns),
        "denylist_tags": list(action.get("denylist_tags") or action.get("deny_classes") or []),
        # preserve identity for multi-board routing
        "db_identity": str(
            action.get("db_identity")
            or (board_rec or {}).get("db_identity")
            or (board_rec or {}).get("db_path")
            or ""
        ),
        "db_path": str((board_rec or {}).get("db_path") or action.get("db_identity") or ""),
    }
    if isinstance(payload, Mapping) and payload.get("expected_block_kind"):
        out["expected_block_kind"] = payload.get("expected_block_kind")
    if isinstance(task, Mapping) and task.get("block_kind") and atype == "unblock_same_card":
        out.setdefault("expected_block_kind", task.get("block_kind"))
    # resolution digest if present on directive path
    if auth_source.startswith("directive:") and auth_digest:
        out["resolution_directive_digest"] = auth_digest
    return out


def prior_state_for_decision(runner_state: Mapping[str, Any]) -> dict[str, Any]:
    """Project runner state_v1 storage into decision prior_state view."""
    if not isinstance(runner_state, Mapping):
        return {
            "schema": "state_v1",
            "last_tick_ns": None,
            "tasks": {},
            "action_cooldowns": {},
        }
    # Prefer decision-native containers when present.
    tasks = runner_state.get("tasks")
    if not isinstance(tasks, dict):
        tasks = {}
    cooldowns = runner_state.get("action_cooldowns")
    if not isinstance(cooldowns, dict):
        # Map runner cooldowns/action_keys if that is all we have.
        cooldowns = {}
        for src_key in ("cooldowns", "action_keys"):
            src = runner_state.get(src_key) or {}
            if isinstance(src, dict):
                for k, v in src.items():
                    cooldowns[str(k)] = v
    last_tick_ns = runner_state.get("last_tick_ns")
    if last_tick_ns is None:
        # runner stores ISO timestamps; leave None rather than invent ns.
        last_tick_ns = None
    return {
        "schema": "state_v1",
        "last_tick_ns": _as_int(last_tick_ns),
        "policy_digest": runner_state.get("policy_digest"),
        "tasks": dict(tasks),
        "action_cooldowns": dict(cooldowns),
    }


def merge_plan_into_runner_state(
    runner_state: dict[str, Any],
    plan: Mapping[str, Any],
    *,
    now_ns: int,
    dry_run: bool,
) -> dict[str, Any]:
    """Attach decision observations/cooldowns onto runner state (non-dry-run)."""
    out = dict(runner_state)
    out.setdefault("tasks", {})
    out.setdefault("action_cooldowns", {})
    out["last_tick_ns"] = int(now_ns)
    if dry_run:
        return out

    obs = plan.get("state_observations") or {}
    if isinstance(obs, dict):
        tasks = dict(out.get("tasks") or {})
        for key, rec in obs.items():
            if not isinstance(rec, dict):
                continue
            prev = dict(tasks.get(str(key)) or {})
            # Keep a short rolling observations window for same-sig breaker.
            prev_obs = list(prev.get("observations") or [])
            if "observations" in rec and isinstance(rec.get("observations"), list):
                prev_obs = list(rec.get("observations") or [])
            else:
                # Append the classification record itself as an observation.
                prev_obs.append(rec)
            prev_obs = prev_obs[-8:]
            merged = dict(prev)
            merged.update(rec)
            merged["observations"] = prev_obs
            tasks[str(key)] = merged
        out["tasks"] = tasks

    # Derive cooldown entries from planned actions that were authorized.
    cds = dict(out.get("action_cooldowns") or {})
    for action in plan.get("actions") or []:
        if not isinstance(action, Mapping):
            continue
        ak = action.get("action_key")
        if not ak:
            continue
        cd_s = _as_int(action.get("cooldown_seconds")) or 0
        cds[str(ak)] = {
            "last_ns": int(now_ns),
            "until_ns": int(now_ns) + int(cd_s) * 1_000_000_000 if cd_s > 0 else int(now_ns),
            "action": action.get("action"),
            "task_id": action.get("task_id"),
        }
    out["action_cooldowns"] = cds
    return out


def load_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    path = Path(config.get("policy_path") or DEFAULT_POLICY_PATH)
    with open(path, "r", encoding="utf-8") as fh:
        import json

        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError("policy must be object")
    return data


def build_adapters(config: Optional[Mapping[str, Any]] = None) -> dict[str, Any]:
    """Return runner adapters bound to parent modules + this config."""
    cfg0 = dict(config or {})
    # Cache last snapshot for execute translation within the same tick.
    cache: dict[str, Any] = {"snapshot": None, "plan": None, "now_ns": None}

    def snapshot_adapter(config: Mapping[str, Any], state: Mapping[str, Any]) -> dict:
        hermes_root = Path(config.get("hermes_root") or cfg0.get("hermes_root") or DEFAULT_HERMES_ROOT)
        now_ns = _as_int(config.get("now_ns"))
        if now_ns is None:
            now_ns = time.time_ns()
        proc_reader = snapshot_mod.default_proc_reader
        if callable(config.get("proc_reader")):
            proc_reader = config["proc_reader"]  # type: ignore[assignment]
        doc = snapshot_mod.collect_snapshot(
            hermes_root,
            now_ns=int(now_ns),
            proc_reader=proc_reader,
        )
        # Normalize aliases runner looks for.
        if "digest" not in doc and doc.get("snapshot_digest"):
            doc["digest"] = doc["snapshot_digest"]
        cache["snapshot"] = doc
        cache["now_ns"] = int(now_ns)
        return doc

    def decide_adapter(
        snapshot: Mapping[str, Any],
        state: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> dict:
        now_ns = _as_int(config.get("now_ns")) or cache.get("now_ns") or time.time_ns()
        policy = load_policy(config if config.get("policy_path") else {**cfg0, **dict(config)})
        prior = prior_state_for_decision(state)
        plan = decision_mod.plan_tick(
            dict(snapshot),
            prior,
            policy,
            now_ns=int(now_ns),
        )
        # Runner expects decision_key/reason optionally.
        actions = plan.get("actions") or []
        if plan.get("fail_closed"):
            key = "fail_closed"
            reason = "decision_fail_closed"
            if plan.get("diagnostics"):
                d0 = plan["diagnostics"][0]
                reason = str(d0.get("code") or reason)
        elif not actions:
            key = "healthy_noop"
            reason = "no_authorized_actions"
        else:
            key = "actions_planned"
            reason = f"actions={len(actions)}"
        plan = dict(plan)
        plan["decision_key"] = key
        plan["reason"] = reason
        plan.setdefault("key", key)
        # Provide runner-shaped cooldown/stall updates (empty unless derived).
        plan.setdefault("cooldown_updates", {})
        plan.setdefault("stall_counter_updates", {})
        cache["plan"] = plan
        cache["now_ns"] = int(now_ns)
        return plan

    def execute_adapter(
        plan: Mapping[str, Any],
        config: Mapping[str, Any],
        *,
        dry_run: bool,
    ) -> dict:
        now_ns = _as_int(config.get("now_ns")) or cache.get("now_ns") or time.time_ns()
        snapshot = cache.get("snapshot") or {}
        raw_actions = list(plan.get("actions") or [])
        translated: list[dict[str, Any]] = []
        for a in raw_actions:
            ta = translate_decision_action(a, snapshot=snapshot, now_ns=int(now_ns))
            if ta is not None:
                translated.append(ta)

        # Group by board db path.
        by_db: dict[str, list[dict[str, Any]]] = {}
        for a in translated:
            dbp = str(a.get("db_path") or a.get("db_identity") or "")
            by_db.setdefault(dbp, []).append(a)

        combined = {
            "schema": "executor_result_v1",
            "ok": True,
            "dry_run": bool(dry_run),
            "planned": [],
            "applied": [],
            "skipped": [],
            "denied": [],
            "already_applied": [],
            "errors": [],
            "boards": {},
        }

        if not translated:
            combined["skipped"] = list(raw_actions)
            return combined

        for db_path, actions in by_db.items():
            if not db_path:
                combined["ok"] = False
                combined["errors"].append(
                    {"code": "missing_db_path", "message": "action missing db_path", "count": len(actions)}
                )
                continue
            # Hard safety: never allow non-dry writes unless env override (executor also guards).
            res = executor_mod.apply_board_actions(
                Path(db_path),
                actions,
                dry_run=bool(dry_run),
                now_ns=int(now_ns),
            )
            combined["boards"][db_path] = {
                "ok": res.get("ok"),
                "planned": len(res.get("planned") or []),
                "applied": len(res.get("applied") or []),
                "denied": len(res.get("denied") or []),
                "already_applied": len(res.get("already_applied") or []),
                "errors": len(res.get("errors") or []),
            }
            for k in ("planned", "applied", "skipped", "denied", "already_applied", "errors"):
                combined[k].extend(list(res.get(k) or []))
            if res.get("ok") is False:
                combined["ok"] = False

        # In dry-run, applied must stay empty for outer report semantics.
        if dry_run:
            # Move any accidental applied into planned.
            if combined["applied"]:
                combined["planned"].extend(combined["applied"])
                combined["applied"] = []
        return combined

    return {
        "snapshot": snapshot_adapter,
        "decide": decide_adapter,
        "execute": execute_adapter,
        "_cache": cache,  # test/debug only
        "merge_plan_into_runner_state": merge_plan_into_runner_state,
        "translate_decision_action": translate_decision_action,
        "prior_state_for_decision": prior_state_for_decision,
    }


def running_card_tuples(hermes_root: Path | str = DEFAULT_HERMES_ROOT) -> list[dict[str, Any]]:
    """Return sorted {card_id,status,run_id,pid,board_id} for every running card."""
    root = Path(hermes_root)
    now_ns = time.time_ns()
    snap = snapshot_mod.collect_snapshot(
        root, now_ns=now_ns, proc_reader=snapshot_mod.default_proc_reader
    )
    out: list[dict[str, Any]] = []
    for board in snap.get("boards") or []:
        if not isinstance(board, Mapping):
            continue
        for task in board.get("tasks") or []:
            if not isinstance(task, Mapping):
                continue
            if str(task.get("status") or "") != "running":
                continue
            pid = None
            pe = task.get("pid_evidence") if isinstance(task.get("pid_evidence"), Mapping) else {}
            if isinstance(pe, Mapping):
                pid = pe.get("pid") or pe.get("worker_pid")
            if pid is None:
                ps = task.get("pid_state")
                # pid_state is usually a string enum; keep pid from evidence only.
                _ = ps
            out.append(
                {
                    "card_id": str(task.get("task_id")),
                    "status": str(task.get("status")),
                    "run_id": task.get("current_run_id"),
                    "pid": pid,
                    "board_id": str(task.get("board_id") or board.get("board_id") or ""),
                }
            )
    out.sort(key=lambda r: (r.get("board_id") or "", r.get("card_id") or ""))
    return out


__all__ = [
    "build_adapters",
    "translate_decision_action",
    "prior_state_for_decision",
    "merge_plan_into_runner_state",
    "running_card_tuples",
    "load_policy",
]
