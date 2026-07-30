"""Pure fixture tests for decision_plan_v1 engine."""
from __future__ import annotations

import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest

DECISION_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(DECISION_DIR))

import decision as D  # noqa: E402

NS = 1_000_000_000
NOW = 1_700_000_000 * NS
BOARD = "telemt-proxy"
DB_ID = f"/tmp/fixture/{BOARD}/kanban.db"
OWNER = "/tmp/fixture/owner/t_demo"


def _sha(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def _policy(**over):
    p = D.load_default_policy()
    p.update(over)
    return p


def _art(path: str, content: str = "x") -> dict:
    raw = content.encode()
    return {
        "path": path,
        "size": len(raw),
        "mtime_ns": NOW - 10 * NS,
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def _source(kind: str, sid: str, body: str) -> dict:
    return {
        "source_kind": kind,
        "source_id": sid,
        "source_sha256": hashlib.sha256(body.encode()).hexdigest(),
    }


def _directive(task_id: str, block_sig: str, art: dict, source: dict, chosen=None) -> dict:
    body = {
        "version": 1,
        "task_id": task_id,
        "block_signature": block_sig,
        "action": "unblock_same_card",
        "chosen_value": chosen if chosen is not None else {"option": "A"},
        "evidence_path": art["path"],
        "evidence_sha256": art["sha256"],
        "source_kind": source["source_kind"],
        "source_id": source["source_id"],
        "source_sha256": source["source_sha256"],
    }
    body["directive_sha256"] = _sha(body)
    return body


def _task(tid: str, status: str, **kw) -> dict:
    base = {
        "task_id": tid,
        "status": status,
        "assignee": kw.pop("assignee", "worker"),
        "board_id": BOARD,
        "db_identity": DB_ID,
        "current_run_id": kw.pop("current_run_id", 10),
        "parents": kw.pop("parents", []),
        "children": kw.pop("children", []),
        "artifacts_declared": kw.pop("artifacts_declared", False),
        "artifacts": kw.pop("artifacts", []),
        "immutable_sources": kw.pop("immutable_sources", []),
        "events": kw.pop("events", []),
        "machine_tags": kw.pop("machine_tags", []),
        "machine_comments": kw.pop("machine_comments", []),
    }
    base.update(kw)
    # digests
    if "artifact_digest" not in base:
        base["artifact_digest"] = D._artifacts_digest(base)
    if "events_digest" not in base:
        base["events_digest"] = D._events_digest(base)
    if "progress_digest" not in base:
        base["progress_digest"] = D._progress_digest(base)
    if "evidence_digest" not in base:
        base["evidence_digest"] = D._evidence_digest(base)
    return base


def _snap(tasks: list, **kw) -> dict:
    return {
        "schema": "snapshot_v1",
        "boards": [
            {
                "board_id": BOARD,
                "db_identity": DB_ID,
                "tasks": tasks,
                "diagnostics": kw.get("board_diagnostics") or [],
            }
        ],
    }


def _state(tasks=None, cooldowns=None, last_tick_ns=None) -> dict:
    return {
        "schema": "state_v1",
        "last_tick_ns": last_tick_ns,
        "tasks": tasks or {},
        "action_cooldowns": cooldowns or {},
    }


def _obs(progress: str, classification: str, tick_ns: int, **kw) -> dict:
    o = {
        "tick_ns": tick_ns,
        "status": kw.get("status", "running"),
        "progress_digest": progress,
        "artifact_digest": kw.get("artifact_digest", progress),
        "events_digest": kw.get("events_digest", progress),
        "classification": classification,
        "signature": kw.get("signature", ""),
        "heartbeat_ns": kw.get("heartbeat_ns", tick_ns),
        "pid_state": kw.get("pid_state", "alive"),
    }
    if "tools_digest" in kw:
        o["tools_digest"] = kw["tools_digest"]
    return o


def _actions_of(plan, action=None):
    acts = plan["actions"]
    if action:
        acts = [a for a in acts if a["action"] == action]
    return acts


def _class_of(plan, name=None):
    cs = plan["classifications"]
    if name:
        cs = [c for c in cs if c["classification"] == name]
    return cs


# ---------------------------------------------------------------------------
# normalize_signature
# ---------------------------------------------------------------------------
def test_normalize_signature_stable():
    assert D.normalize_signature("  Foo  BAR\n") == D.normalize_signature("foo bar")
    assert D.normalize_signature({"a": 1, "b": 2}) == D.normalize_signature({"b": 2, "a": 1})
    assert D.normalize_signature(None) == ""
    assert "protocol" in D.normalize_signature("Protocol_No_Complete!")


# ---------------------------------------------------------------------------
# fail-closed gates
# ---------------------------------------------------------------------------
def test_corrupt_prior_state_fail_closed():
    plan = D.plan_tick(_snap([]), prior_state="nope", policy=_policy(), now_ns=NOW)
    assert plan["ok"] is False and plan["fail_closed"] is True
    assert plan["actions"] == []
    assert any(d["code"] == "prior_state_corrupt" for d in plan["diagnostics"])


def test_clock_reversal_fail_closed():
    st = _state(last_tick_ns=NOW + 5 * NS)
    plan = D.plan_tick(_snap([]), st, _policy(), now_ns=NOW)
    assert plan["fail_closed"] is True
    assert any(d["code"] == "clock_reversal" for d in plan["diagnostics"])
    assert plan["actions"] == []


def test_unknown_policy_schema_fail_closed():
    pol = _policy()
    pol["schema"] = "totally_unknown_v9"
    plan = D.plan_tick(_snap([]), _state(), pol, now_ns=NOW)
    assert plan["fail_closed"] is True
    assert plan["actions"] == []


def test_invalid_snapshot_fail_closed():
    plan = D.plan_tick({"schema": "snapshot_v1"}, _state(), _policy(), now_ns=NOW)
    assert plan["fail_closed"] is True
    assert plan["actions"] == []


def test_unknown_snapshot_schema_fail_closed():
    plan = D.plan_tick({"schema": "nope", "boards": []}, _state(), _policy(), now_ns=NOW)
    assert plan["fail_closed"] is True


# ---------------------------------------------------------------------------
# exact resolution directive allowed
# ---------------------------------------------------------------------------
def test_exact_resolution_allowed():
    tid = "t_block1"
    art = _art(f"{OWNER}/evidence.json", '{"ok":true}')
    src = _source("comment", "c1", "ANTI_STALL_RESOLUTION_V1 fixed body")
    sig = "waiting_lane_choice_v1"
    direc = _directive(tid, sig, art, src, chosen={"option": "B"})
    task = _task(
        tid,
        "blocked",
        block_kind="awaiting_resolution",
        block_reason=sig,
        block_signature=sig,
        artifacts_declared=True,
        artifacts=[art],
        immutable_sources=[src],
        resolution_markers=[{"directive": direc}],
        machine_tags=["awaiting_resolution"],
    )
    # remove accidental deny from block_kind=dependency — fine
    plan = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    assert plan["ok"] is True
    assert _class_of(plan, "blocked_exact_directive")
    unblocks = _actions_of(plan, "unblock_same_card")
    assert len(unblocks) == 1
    assert unblocks[0]["task_id"] == tid
    assert unblocks[0]["expected_status"] == "blocked"
    assert unblocks[0]["authorization_source"].startswith("directive:")
    assert unblocks[0]["payload"]["chosen_value"] == {"option": "B"}
    # action key stable
    plan2 = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    assert _actions_of(plan2, "unblock_same_card")[0]["action_key"] == unblocks[0]["action_key"]


def test_exact_resolution_suppressed_without_artifacts_declaration():
    tid = "t_block_noart"
    art = _art(f"{OWNER}/evidence.json", "e")
    src = _source("comment", "c2", "body")
    sig = "choose_lane"
    direc = _directive(tid, sig, art, src)
    task = _task(
        tid,
        "blocked",
        block_signature=sig,
        block_reason=sig,
        artifacts_declared=False,
        artifacts=[art],
        immutable_sources=[src],
        resolution_directive=direc,
    )
    plan = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    assert _actions_of(plan, "unblock_same_card") == []
    assert _class_of(plan, "blocked_exact_directive") or _class_of(plan, "missing_artifacts_declaration")


def test_directive_hash_mismatch_invalid():
    tid = "t_badhash"
    art = _art(f"{OWNER}/e.json", "e")
    src = _source("comment", "c3", "b")
    sig = "sigx"
    direc = _directive(tid, sig, art, src)
    direc["directive_sha256"] = "0" * 64
    task = _task(
        tid,
        "blocked",
        block_signature=sig,
        artifacts_declared=True,
        artifacts=[art],
        immutable_sources=[src],
        resolution_directive=direc,
    )
    v = D.validate_resolution_directive(task, _snap([task]), _policy())
    assert v["valid"] is False
    plan = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    assert _actions_of(plan, "unblock_same_card") == []


def test_ambiguous_remains_no_unblock():
    tid = "t_amb"
    task = _task(
        tid,
        "blocked",
        block_kind="other",
        block_reason="maybe this or that",
        block_signature="maybe this or that",
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/a.txt")],
    )
    plan = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    assert _actions_of(plan, "unblock_same_card") == []
    assert _class_of(plan, "blocked_ambiguous")


# ---------------------------------------------------------------------------
# permanent deny classes
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "deny_token",
    [
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
    ],
)
def test_permanent_deny_never_unblocks(deny_token):
    tid = f"t_deny_{deny_token}"
    art = _art(f"{OWNER}/ev_{deny_token}.json", deny_token)
    src = _source("comment", f"c_{deny_token}", "src")
    sig = f"blocked_for_{deny_token}"
    direc = _directive(tid, sig, art, src)
    # even with a perfect directive, deny wins
    task = _task(
        tid,
        "blocked",
        block_kind=deny_token,
        block_reason=f"because {deny_token}",
        block_signature=sig,
        artifacts_declared=True,
        artifacts=[art],
        immutable_sources=[src],
        resolution_directive=direc,
        machine_tags=[deny_token],
    )
    plan = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    assert _actions_of(plan, "unblock_same_card") == []
    classes = {c["classification"] for c in plan["classifications"]}
    assert "blocked_deny_class" in classes or "blocked_needs_input" in classes


def test_needs_input_status_never_unblocks():
    art = _art(f"{OWNER}/x.json", "x")
    src = _source("comment", "cni", "s")
    tid = "t_ni"
    sig = "need_human"
    direc = _directive(tid, sig, art, src)
    task = _task(
        tid,
        "needs_input",
        block_signature=sig,
        artifacts_declared=True,
        artifacts=[art],
        immutable_sources=[src],
        resolution_directive=direc,
    )
    plan = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    assert _actions_of(plan, "unblock_same_card") == []
    assert _class_of(plan, "blocked_needs_input")


# ---------------------------------------------------------------------------
# dependency rule
# ---------------------------------------------------------------------------
def test_dependency_all_parents_success_unblocks():
    parent = _task(
        "t_parent",
        "done",
        result="success",
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/p.json", "p")],
    )
    child_art = _art(f"{OWNER}/c.json", "c")
    child = _task(
        "t_child",
        "blocked",
        block_kind="dependency",
        block_reason="waiting parents",
        block_signature="waiting parents",
        machine_tags=["dependency_all_parents_success_v1"],
        parents=[{"task_id": "t_parent", "evidence_digest": parent["evidence_digest"]}],
        parent_evidence_digests={"t_parent": parent["evidence_digest"]},
        artifacts_declared=True,
        artifacts=[child_art],
        expected_policy_version=1,
    )
    pol = _policy()
    child["expected_policy_digest"] = D.policy_digest(D._merge_policy_defaults(pol))
    # recompute after setting expected — not part of evidence for child unblock auth beyond rule
    plan = D.plan_tick(_snap([parent, child]), _state(), pol, now_ns=NOW)
    assert _class_of(plan, "blocked_dependency_rule")
    unblocks = _actions_of(plan, "unblock_same_card")
    assert len(unblocks) == 1
    assert unblocks[0]["task_id"] == "t_child"
    assert unblocks[0]["authorization_source"] == "rule:dependency_all_parents_success_v1"


def test_dependency_rule_fails_if_parent_not_success():
    parent = _task("t_parent2", "running")
    child = _task(
        "t_child2",
        "blocked",
        block_kind="dependency",
        machine_tags=["dependency_all_parents_success_v1"],
        parents=["t_parent2"],
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/c2.json")],
    )
    plan = D.plan_tick(_snap([parent, child]), _state(), _policy(), now_ns=NOW)
    assert _actions_of(plan, "unblock_same_card") == []


def test_dependency_propagation_descendant_stalled():
    parent = _task("t_p3", "running")
    child = _task(
        "t_c3",
        "todo",
        parents=["t_p3"],
    )
    plan = D.plan_tick(_snap([parent, child]), _state(), _policy(), now_ns=NOW)
    assert _class_of(plan, "descendant_stalled_by_parents")
    assert _actions_of(plan, "unblock_same_card") == []


# ---------------------------------------------------------------------------
# idempotent action keys + cooldown
# ---------------------------------------------------------------------------
def test_action_key_idempotent_and_cooldown():
    tid = "t_cd"
    art = _art(f"{OWNER}/cd.json", "cd")
    src = _source("comment", "ccd", "s")
    sig = "choice"
    direc = _directive(tid, sig, art, src)
    task = _task(
        tid,
        "blocked",
        block_signature=sig,
        artifacts_declared=True,
        artifacts=[art],
        immutable_sources=[src],
        resolution_directive=direc,
    )
    plan1 = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    key = _actions_of(plan1, "unblock_same_card")[0]["action_key"]
    # cooldown active
    st = _state(cooldowns={key: {"last_ns": NOW - 10 * NS}})
    plan2 = D.plan_tick(_snap([task]), st, _policy(), now_ns=NOW)
    assert _actions_of(plan2, "unblock_same_card") == []
    assert any(d["code"] == "action_cooldown_active" for d in plan2["diagnostics"])


# ---------------------------------------------------------------------------
# same-signature / no-delta breaker
# ---------------------------------------------------------------------------
def test_same_signature_no_delta_breaker():
    tid = "t_rep"
    prog = _sha({"x": 1})
    task = _task(
        tid,
        "blocked",
        block_kind="other",
        block_signature="worker_failed_oom",
        block_reason="worker_failed_oom",
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/r.json", "r")],
        progress_digest=prog,
        artifact_digest=prog,
        events_digest=prog,
        evidence_digest=prog,
    )
    st = _state(
        tasks={
            f"{BOARD}::{tid}": {
                "last_failure_signature": D.normalize_signature("worker_failed_oom"),
                "observations": [
                    _obs(prog, "blocked_ambiguous", NOW - 600 * NS, status="blocked", signature="worker_failed_oom")
                ],
            }
        }
    )
    plan = D.plan_tick(_snap([task]), st, _policy(), now_ns=NOW)
    assert _class_of(plan, "repeated_failure_signature")
    assert _actions_of(plan, "unblock_same_card") == []
    assert len(_actions_of(plan, "comment_once")) == 1
    assert len(_actions_of(plan, "route_needs_replan")) == 1
    # preserve_successful_siblings flag
    assert _actions_of(plan, "route_needs_replan")[0]["payload"].get("preserve_successful_siblings") is True


# ---------------------------------------------------------------------------
# protocol attempt ceiling
# ---------------------------------------------------------------------------
def test_protocol_first_attempt_comment_only():
    tid = "t_prot1"
    task = _task(
        tid,
        "running",
        protocol_violation=True,
        protocol_signature="protocol_no_complete",
        pid_state="alive",
        last_heartbeat_at_ns=NOW - 30 * NS,
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/p1.json")],
    )
    plan = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    assert _class_of(plan, "protocol_violation")
    assert _actions_of(plan, "route_needs_replan") == []
    assert len(_actions_of(plan, "comment_once")) == 1


def test_protocol_second_attempt_circuit_breaker():
    tid = "t_prot2"
    prog = _sha("p2")
    task = _task(
        tid,
        "running",
        protocol_violation=True,
        protocol_signature="protocol_no_complete",
        pid_state="alive",
        last_heartbeat_at_ns=NOW - 30 * NS,
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/p2.json")],
        progress_digest=prog,
        artifact_digest=prog,
        events_digest=prog,
    )
    st = _state(
        tasks={
            f"{BOARD}::{tid}": {
                "protocol_violation_count": 1,
                "last_protocol_signature": "protocol_no_complete",
                "observations": [_obs(prog, "protocol_violation", NOW - 300 * NS)],
            }
        }
    )
    plan = D.plan_tick(_snap([task]), st, _policy(), now_ns=NOW)
    assert _class_of(plan, "protocol_violation")
    assert len(_actions_of(plan, "route_needs_replan")) == 1
    assert len(_actions_of(plan, "comment_once")) == 1
    # never emits retry/reclaim
    assert all(a["action"] in {"comment_once", "route_needs_replan"} for a in plan["actions"])


def test_protocol_third_not_emitted_still_breaker_only():
    """Even if attempt_count says 5, still only comment+route once (idempotent keys)."""
    tid = "t_prot3"
    task = _task(
        tid,
        "running",
        protocol_violation=True,
        protocol_signature="protocol_no_complete",
        attempt_count=5,
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/p3.json")],
        last_heartbeat_at_ns=NOW - 10 * NS,
        pid_state="alive",
    )
    plan = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    # breaker path
    assert len(_actions_of(plan, "route_needs_replan")) == 1
    keys = [a["action_key"] for a in plan["actions"]]
    assert len(keys) == len(set(keys))


# ---------------------------------------------------------------------------
# healthy work untouched
# ---------------------------------------------------------------------------
def test_healthy_running_with_delta_noop():
    tid = "t_ok"
    prog_old = _sha("old")
    prog_new = _sha("new")
    task = _task(
        tid,
        "running",
        pid_state="alive",
        last_heartbeat_at_ns=NOW - 20 * NS,
        progress_digest=prog_new,
        artifact_digest=prog_new,
        events_digest=prog_new,
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/ok.json", "new")],
    )
    st = _state(
        tasks={
            f"{BOARD}::{tid}": {
                "observations": [_obs(prog_old, "noop_healthy", NOW - 300 * NS)],
            }
        }
    )
    plan = D.plan_tick(_snap([task]), st, _policy(), now_ns=NOW)
    assert _class_of(plan, "noop_healthy")
    assert plan["actions"] == []


# ---------------------------------------------------------------------------
# dead / stale / no-progress windows
# ---------------------------------------------------------------------------
def test_dead_pid_requires_two_snapshots():
    tid = "t_dead"
    prog = _sha("dead")
    task = _task(
        tid,
        "running",
        pid_state="dead",
        last_heartbeat_at_ns=NOW - 1000 * NS,
        progress_digest=prog,
        artifact_digest=prog,
        events_digest=prog,
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/d.json")],
    )
    # only one observation total needed: prior matching + current => min_stall_snapshots=2
    # without prior:
    plan0 = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    assert _class_of(plan0, "running_dead_pid")
    assert _actions_of(plan0, "route_needs_replan") == []

    st = _state(
        tasks={
            f"{BOARD}::{tid}": {
                "observations": [
                    _obs(prog, "running_dead_pid", NOW - 300 * NS, pid_state="dead", status="running")
                ]
            }
        }
    )
    plan1 = D.plan_tick(_snap([task]), st, _policy(), now_ns=NOW)
    assert _class_of(plan1, "running_dead_pid")
    assert len(_actions_of(plan1, "route_needs_replan")) == 1
    assert len(_actions_of(plan1, "comment_once")) == 1


def test_stale_heartbeat_window():
    tid = "t_stale"
    prog = _sha("stale")
    task = _task(
        tid,
        "running",
        pid_state="alive",
        heartbeat_stale=True,
        last_heartbeat_at_ns=NOW - 7200 * NS,
        progress_digest=prog,
        artifact_digest=prog,
        events_digest=prog,
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/s.json")],
    )
    st = _state(
        tasks={
            f"{BOARD}::{tid}": {
                "observations": [
                    _obs(prog, "running_stale_heartbeat", NOW - 300 * NS, status="running")
                ]
            }
        }
    )
    plan = D.plan_tick(_snap([task]), st, _policy(), now_ns=NOW)
    assert _class_of(plan, "running_stale_heartbeat")
    assert _actions_of(plan, "route_needs_replan")


def test_fresh_heartbeat_no_progress_needs_three_obs_and_900s():
    tid = "t_np"
    prog = _sha("np")
    task = _task(
        tid,
        "running",
        pid_state="alive",
        last_heartbeat_at_ns=NOW - 15 * NS,
        progress_digest=prog,
        artifact_digest=prog,
        events_digest=prog,
        artifacts_declared=True,
        artifacts=[_art(f"{OWNER}/np.json")],
        max_no_progress_seconds=900,
    )
    # only 1 prior obs, span short -> no route
    st_short = _state(
        tasks={
            f"{BOARD}::{tid}": {
                "observations": [_obs(prog, "running_no_progress", NOW - 300 * NS)]
            }
        }
    )
    plan_short = D.plan_tick(_snap([task]), st_short, _policy(), now_ns=NOW)
    assert _class_of(plan_short, "running_no_progress")
    assert _actions_of(plan_short, "route_needs_replan") == []

    # 2 prior observations spanning 900s => with current = 3 obs
    st_ok = _state(
        tasks={
            f"{BOARD}::{tid}": {
                "observations": [
                    _obs(prog, "running_no_progress", NOW - 900 * NS),
                    _obs(prog, "running_no_progress", NOW - 450 * NS),
                ]
            }
        }
    )
    plan_ok = D.plan_tick(_snap([task]), st_ok, _policy(), now_ns=NOW)
    assert _class_of(plan_ok, "running_no_progress")
    assert len(_actions_of(plan_ok, "route_needs_replan")) == 1


# ---------------------------------------------------------------------------
# Planner ceiling
# ---------------------------------------------------------------------------
def test_planner_ceiling_comment_once_no_complete():
    child = _task("t_w_child", "ready", assignee="worker")
    planner = _task(
        "t_planner",
        "running",
        assignee="planner",
        children=["t_w_child"],
        last_heartbeat_at_ns=NOW - 10 * NS,
        pid_state="alive",
    )
    plan = D.plan_tick(_snap([planner, child]), _state(), _policy(), now_ns=NOW)
    assert _class_of(plan, "planner_ceiling")
    acts = _actions_of(plan, "comment_once")
    assert len(acts) == 1
    assert acts[0]["task_id"] == "t_planner"
    assert "executable children" in acts[0]["payload"]["body"]
    # never complete/unblock planner
    assert _actions_of(plan, "unblock_same_card") == []
    assert all(a["action"] == "comment_once" for a in plan["actions"] if a["task_id"] == "t_planner")


# ---------------------------------------------------------------------------
# missing artifact declarations fail closed for status actions
# ---------------------------------------------------------------------------
def test_missing_artifacts_no_status_action_on_dead_window():
    tid = "t_dead_noart"
    prog = _sha("dna")
    task = _task(
        tid,
        "running",
        pid_state="dead",
        progress_digest=prog,
        artifact_digest=prog,
        events_digest=prog,
        artifacts_declared=False,
        last_heartbeat_at_ns=NOW - 9999 * NS,
    )
    st = _state(
        tasks={
            f"{BOARD}::{tid}": {
                "observations": [_obs(prog, "running_dead_pid", NOW - 300 * NS, pid_state="dead")]
            }
        }
    )
    plan = D.plan_tick(_snap([task]), st, _policy(), now_ns=NOW)
    assert _actions_of(plan, "route_needs_replan") == []
    # comment_once is allowed (non-status)
    assert _actions_of(plan, "comment_once")


# ---------------------------------------------------------------------------
# validate_resolution_directive unit
# ---------------------------------------------------------------------------
def test_validate_resolution_directive_ok_and_source_mismatch():
    tid = "t_v"
    art = _art(f"{OWNER}/v.json", "v")
    src = _source("comment", "cv", "hello")
    sig = "sigv"
    direc = _directive(tid, sig, art, src)
    task = _task(
        tid,
        "blocked",
        block_signature=sig,
        artifacts_declared=True,
        artifacts=[art],
        immutable_sources=[src],
        resolution_markers=[f"{D.DIRECTIVE_PREFIX}{json.dumps(direc, sort_keys=True, separators=(',', ':'))}"],
    )
    # Note: string form in resolution_markers - our extractor handles ANTI_STALL prefix strings in list
    # But we passed a string in list via resolution_markers as list of str — _extract handles str items only for top-level keys as list of dict/str.
    # Fix: put as machine_comments
    task["resolution_markers"] = [direc]
    ok = D.validate_resolution_directive(task, _snap([task]), _policy())
    assert ok["valid"] is True

    bad = deepcopy(task)
    bad["immutable_sources"] = [_source("comment", "cv", "DIFFERENT")]
    bad["resolution_markers"] = [
        _directive(tid, sig, art, bad["immutable_sources"][0])  # fix hash for body but source in directive still old?
    ]
    # craft directive pointing to old source hash while snapshot has new
    d2 = _directive(tid, sig, art, src)  # old src hash inside
    bad["resolution_markers"] = [d2]
    no = D.validate_resolution_directive(bad, _snap([bad]), _policy())
    assert no["valid"] is False


def test_actions_sorted_deterministically():
    # two blocked with directives
    tasks = []
    for i, tid in enumerate(["t_z", "t_a"]):
        art = _art(f"{OWNER}/{tid}.json", tid)
        src = _source("comment", tid, tid)
        sig = f"sig_{tid}"
        direc = _directive(tid, sig, art, src)
        tasks.append(
            _task(
                tid,
                "blocked",
                block_signature=sig,
                artifacts_declared=True,
                artifacts=[art],
                immutable_sources=[src],
                resolution_directive=direc,
                current_run_id=100 + i,
            )
        )
    plan = D.plan_tick(_snap(tasks), _state(), _policy(), now_ns=NOW)
    ids = [a["task_id"] for a in plan["actions"]]
    assert ids == sorted(ids)
    # classifications sorted
    cids = [(c["task_id"], c["classification"]) for c in plan["classifications"]]
    assert cids == sorted(cids)


def test_done_task_noop():
    task = _task("t_done", "done", result="success")
    plan = D.plan_tick(_snap([task]), _state(), _policy(), now_ns=NOW)
    assert plan["actions"] == []
    assert _class_of(plan, "noop_healthy")


def test_null_prior_state_ok():
    plan = D.plan_tick(_snap([_task("t1", "done")]), None, _policy(), now_ns=NOW)
    assert plan["ok"] is True


def test_policy_digest_in_plan():
    pol = _policy()
    plan = D.plan_tick(_snap([]), _state(), pol, now_ns=NOW)
    assert plan["policy_digest"] == D.policy_digest(D._merge_policy_defaults(pol))
    assert plan["schema"] == "decision_plan_v1"


def test_no_live_db_imports():
    """decision module must not import sqlite3 or open DB paths."""
    src = Path(D.__file__).read_text(encoding="utf-8")
    assert "sqlite3" not in src
    assert "subprocess" not in src
