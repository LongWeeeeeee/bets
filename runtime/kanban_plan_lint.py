#!/usr/bin/env python3
"""Kanban plan-lint + protocol streak breaker for max parallel fan-out.

Goals
-----
1. Detect fat Worker cards that should have been split into independent staging
   cards + one INT integrator (plan-lint).
2. Detect protocol_no_complete streaks (rc=0 without kanban complete/block) and
   after 2 identical outcomes: salvage note + block needs_replan (no more burns).
3. Prefer maximum independent parallel fan-out: flag mono-cards that mix
   collection/integration, archival+live, multi-outcome, >N artifacts.
4. Semantic-admission v1 fallback (opt-in body marker SEMANTIC_ADMISSION: v1):
   discovery closure, research-wave budget, replan SUPERSEDES quarantine,
   live-evidence source, planner-correction breaker. Legacy unmarked tasks
   keep current treatment.

Does NOT change roles/models/dispatch freeze. Does NOT auto-rewrite card bodies
into subtasks (that stays Planner/Commander). Soft-enforcement: comment + optional
block of ready/todo fat cards; hard stop on protocol×2.

Usage
-----
  python kanban_plan_lint.py --dry-run
  python kanban_plan_lint.py              # apply comments/blocks
  python kanban_plan_lint.py --boards default,telemt-proxy
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from kanban_lane_paths import discover_boards as discover_lane_boards
from kanban_lane_paths import lane_home, lane_runtime_root, selected_lane

LANE = selected_lane()
HERMES_ROOT = lane_home(LANE)
RUNTIME_ROOT = lane_runtime_root(LANE)
HERMES_BIN = os.environ.get("HERMES_BIN", "/usr/local/lib/hermes-agent/venv/bin/hermes")
REPORT_PATH = RUNTIME_ROOT / "kanban_plan_lint_last.json"
STATE_PATH = RUNTIME_ROOT / "kanban_plan_lint_state.json"
LOG_PATH = RUNTIME_ROOT / "kanban_plan_lint.log"

# --- thresholds (max fan-out bias) ---
MAX_FINAL_ARTIFACTS = 5  # > this on one Worker => fat
MAX_BODY_CHARS_WORKER = 4500  # long implement bodies are smell
MAX_OUTCOME_MARKERS = 1
PROTOCOL_STREAK_LIMIT = 2  # attempts; 2nd identical => needs_replan
PROTOCOL_SIG = "protocol_no_complete"

IMPLEMENT_HINT = re.compile(
    r"\b(EXECUTE|IMPLEMENT|W\d|REPLAN\d*-?[EW]|RESEARCH|INTEGRATE|INT:|EVIDENCE)\b",
    re.I,
)
PLANNER_HINT = re.compile(r"\b(PLAN|REPLAN)\b", re.I)
REVIEW_HINT = re.compile(r"\b(REVIEW|REVIEWER)\b", re.I)

ARTIFACT_LINE = re.compile(
    r"(?im)^(?:\s*[-*]\s+|\s*\d+[.)]\s+)?(`[^`]+`|[\w./-]+\.(?:json|jsonl|md|txt|log|sha256|csv|yml|yaml|diff|png))\s*$"
)
ARTIFACT_PATH = re.compile(
    r"(?i)(?:evidence|artifact|output|write|save|create|path)[^\n]{0,80}"
    r"(/[^\s`'\"]+\.(?:json|jsonl|md|txt|log|sha256|csv|yml|yaml)|"
    r"`[^`]+\.(?:json|jsonl|md|txt|log|sha256|csv|yml|yaml)`)"
)
ARTIFACT_EXTS = r"(?:json|jsonl|md|txt|log|sha256|csv|yml|yaml|diff|png)"
# normalize-only cards are orchestration stall, not progress (skill rule 9)
NORMALIZE_ONLY = re.compile(
    r"(?i)\b(normaliz\w*|re-?emit|reformat(?:ting)?|compact(?:ion|ing)?|"
    r"rewrite\s+(?:the\s+)?(?:summary|handoff|evidence)|"
    r"field[- ]complete\s+(?:summary|handoff)|bounded\s+(?:summary|handoff))\b"
)
KNOWN_ASSIGNEES = {"worker", "planner", "reviewer", "default", ""}


def _looks_like_artifact(s: str) -> bool:
    """Accept only real file paths, not shell commands in backticks.

    False-positive guard: verify commands like `` `python -m pytest ...` `` or
    `` `git diff --check -- a.py b.py` `` used to be counted as artifacts via
    the backtick branch of ARTIFACT_LINE, inflating artifact counts and
    wrongly marking legitimate INT cards fat (score artifacts=7-9 with ~half
    being commands). A real artifact path has no whitespace, path-ish chars
    only, and ends with a known output extension.
    """
    s = s.strip().strip("`").strip()
    if not s or len(s) > 200:
        return False
    if re.search(r"\s", s):
        return False
    if not re.fullmatch(r"[~\w./:+\-]+", s):
        return False
    return bool(re.search(r"\.(?i:" + ARTIFACT_EXTS + r")$", s))
MULTI_OUTCOME = re.compile(
    r"(?i)(?:ONE CONCRETE OUTCOME|concrete outcome|acceptance|done when|success only when)"
)
COLLECTION = re.compile(
    r"(?i)\b(inventory|collect|list files|hash(?:es)?|read-only|probe|snapshot|write-ledger|"
    r"historical|safe-state|metadata|provenance)\b"
)
INTEGRATION = re.compile(
    r"(?i)\b(integrat|assemble|merge|final contract|package|pack(?:ing)?|single owner of final|"
    r"write to final|evidence contract)\b"
)
LIVE_STATE = re.compile(
    r"(?i)\b(systemctl|ss -l|port\s+\d+|iptables|sysctl|xray|telemt|meko|pid\s+\d+|listen(?:ing)?)\b"
)
ARCHIVAL = re.compile(
    r"(?i)\b(install-log|historical|prior evidence|old approval|parent evidence|ledger|snapshot from)\b"
)
FINAL_DIR = re.compile(
    r"(?i)(?:final/|evidence.contract|/final\b|required (?:ten|10) files|10 итоговых)"
)
STAGING_DIR = re.compile(r"(?i)staging/")
PARALLEL_HINT = re.compile(
    r"(?i)(?:independen|parallel|fan-?out|several workers|multiple workers|W1|W2|W3)"
)



# --- Semantic admission v1 (opt-in fallback hygiene) ---
SEMANTIC_ADMISSION_KEY = "SEMANTIC_ADMISSION"
SEMANTIC_ADMISSION_VALUE = "v1"
SEMANTIC_MARKER_KEYS = {
    "SEMANTIC_ADMISSION",
    "CARD_KIND",
    "MODE",
    "DISCOVERY_CLOSED",
    "PROVEN_SIGNATURES",
    "RESEARCH_SIGNATURE",
    "RESEARCH_WAVE",
    "REPLAN_TRANSACTION",
    "SUPERSEDES",
    "LIVE_GOAL",
    "LIVE_EVIDENCE_REQUIRED",
    "LIVE_EVIDENCE",
    "PLANNER_CORRECTION_SIGNATURE",
    "PLANNER_CORRECTION_ROUND",
}
# Keys whose values are normalized as signatures (lowercase + collapse ws)
_SEMANTIC_SIG_KEYS = {
    "RESEARCH_SIGNATURE",
    "PLANNER_CORRECTION_SIGNATURE",
}
# Keys whose list items are signature-normalized
_SEMANTIC_SIG_LIST_KEYS = {"PROVEN_SIGNATURES"}
# IDs / wave / transaction: trim only, no case fold
_SEMANTIC_TRIM_KEYS = {
    "RESEARCH_WAVE",
    "REPLAN_TRANSACTION",
    "SUPERSEDES",
    "CARD_KIND",
    "MODE",
    "SEMANTIC_ADMISSION",
    "DISCOVERY_CLOSED",
    "LIVE_GOAL",
    "LIVE_EVIDENCE_REQUIRED",
    "LIVE_EVIDENCE",
    "PLANNER_CORRECTION_ROUND",
}
LIVE_EVIDENCE_PASS_LINE = re.compile(r"(?m)^[ \t]*LIVE_EVIDENCE:[ \t]*PASS[ \t]*$")
ACTIVE_GRAPH_STATUSES = frozenset({"todo", "ready", "running"})
SEMANTIC_BLOCK_KINDS = frozenset(
    {
        "semantic_no_progress:discovery_closed",
        "semantic_no_progress:research_wave_repeat",
        "semantic_no_progress:replan_superseded_graph_active",
        "semantic_no_progress:live_evidence_missing",
        "semantic_no_progress:live_parent_evidence_missing",
        "semantic_no_progress:planner_correction_repeat",
        "semantic_contract_invalid",
    }
)


def normalize_signature(value: str) -> str:
    """lowercase + trim + collapse whitespace runs to single ASCII space."""
    s = (value or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def trim_id(value: str) -> str:
    return (value or "").strip()


def parse_semantic_markers(body: str) -> tuple[dict[str, str], list[str]]:
    """Parse line-oriented first-colon KEY: value markers.

    Returns (markers, errors). Conflicting duplicate semantic keys => error.
    Identical duplicates coalesce. Only known semantic keys are collected.
    """
    markers: dict[str, str] = {}
    errors: list[str] = []
    for raw in (body or "").splitlines():
        line = raw.strip()
        if not line or ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip()
        if not key or key != key.upper():
            # marker keys are uppercase tokens; ignore free prose
            continue
        if key not in SEMANTIC_MARKER_KEYS:
            continue
        val = val.strip()
        if key in markers:
            if markers[key] != val:
                errors.append(f"conflicting_duplicate:{key}")
            # identical duplicate coalesced
            continue
        markers[key] = val
    return markers, errors


def is_semantic_opt_in(markers: dict[str, str]) -> bool:
    return (markers.get(SEMANTIC_ADMISSION_KEY) or "").strip() == SEMANTIC_ADMISSION_VALUE


def _truthy_marker(val: Optional[str]) -> bool:
    if val is None:
        return False
    return val.strip().lower() in {"true", "1", "yes", "on"}


def research_identity(markers: dict[str, str]) -> bool:
    kind = (markers.get("CARD_KIND") or "").strip()
    mode = (markers.get("MODE") or "").strip()
    if kind == "RESEARCH":
        return True
    if mode == "EVIDENCE_TRIAGE":
        return True
    if "RESEARCH_SIGNATURE" in markers:
        return True
    return False


def is_correction(markers: dict[str, str]) -> bool:
    kind = (markers.get("CARD_KIND") or "").strip()
    if kind == "PLANNER_CORRECTION":
        return True
    if "PLANNER_CORRECTION_SIGNATURE" in markers:
        return True
    return False


def is_downstream_live_gate(markers: dict[str, str]) -> bool:
    kind = (markers.get("CARD_KIND") or "").strip()
    return kind in {"REVIEWER", "CUTOVER"} and _truthy_marker(
        markers.get("LIVE_EVIDENCE_REQUIRED")
    )


def parse_proven_signatures(raw: Optional[str]) -> list[str]:
    if not raw:
        return []
    out: list[str] = []
    for part in raw.split(";"):
        n = normalize_signature(part)
        if n and n not in out:
            out.append(n)
    return out


def parse_supersedes(raw: Optional[str]) -> list[str]:
    if raw is None:
        return []
    s = raw.strip()
    if not s:
        return []
    out: list[str] = []
    for part in s.split(","):
        tid = trim_id(part)
        if tid and tid not in out:
            out.append(tid)
    return out


def has_live_evidence_pass(*texts: Optional[str]) -> bool:
    for t in texts:
        if t and LIVE_EVIDENCE_PASS_LINE.search(t):
            return True
    return False


def load_board_graph(con: sqlite3.Connection) -> dict[str, Any]:
    """Load tasks + parent/child edges + latest run summaries for semantic lint."""
    cols = table_cols(con, "tasks")
    want = ["id", "title", "body", "assignee", "status", "result"]
    for extra in (
        "consecutive_failures",
        "last_failure_error",
        "max_retries",
        "block_kind",
        "created_at",
    ):
        if extra in cols:
            want.append(extra)
    sel = ", ".join(want)
    rows = con.execute(f"select {sel} from tasks").fetchall()
    tasks: dict[str, dict[str, Any]] = {}
    for r in rows:
        d = dict(r)
        tasks[d["id"]] = d

    parents_of: dict[str, list[str]] = {tid: [] for tid in tasks}
    children_of: dict[str, list[str]] = {tid: [] for tid in tasks}
    try:
        for pr, ch in con.execute("select parent_id, child_id from task_links"):
            if ch in parents_of:
                parents_of[ch].append(pr)
            else:
                parents_of.setdefault(ch, []).append(pr)
            if pr in children_of:
                children_of[pr].append(ch)
            else:
                children_of.setdefault(pr, []).append(ch)
    except Exception:
        pass

    # latest ended run summary per task (prefer most recent id)
    run_summary: dict[str, str] = {}
    try:
        rcols = table_cols(con, "task_runs")
        if "summary" in rcols:
            q = (
                "select task_id, summary from task_runs "
                "where summary is not null and summary != '' "
                "order by id desc"
            )
            for tid, summary in con.execute(q):
                if tid not in run_summary:
                    run_summary[tid] = summary
    except Exception:
        pass

    return {
        "tasks": tasks,
        "parents_of": parents_of,
        "children_of": children_of,
        "run_summary": run_summary,
    }


def ancestor_ids(task_id: str, parents_of: dict[str, list[str]]) -> list[str]:
    seen: set[str] = set()
    stack = list(parents_of.get(task_id) or [])
    out: list[str] = []
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        out.append(cur)
        stack.extend(parents_of.get(cur) or [])
    return out


def descendant_ids(task_id: str, children_of: dict[str, list[str]]) -> list[str]:
    seen: set[str] = set()
    stack = list(children_of.get(task_id) or [])
    out: list[str] = []
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        out.append(cur)
        stack.extend(children_of.get(cur) or [])
    return out


def _semantic_finding(
    board: str,
    task_id: str,
    kind: str,
    title: str,
    detail: str,
    *,
    status: str,
    signals: Optional[list[str]] = None,
    score: int = 8,
    terminal_evidence_only: bool = False,
) -> Finding:
    """Build a semantic finding with safe quarantine action policy."""
    if terminal_evidence_only or status in ("done", "archived"):
        action = "none"
        severity = "error"
    elif status in ACTIVE_GRAPH_STATUSES or status == "blocked":
        # blocked: still comment (avoid block churn); active: block before dispatch
        action = "block" if status in ACTIVE_GRAPH_STATUSES else "comment"
        severity = "critical" if action == "block" else "error"
    else:
        action = "comment"
        severity = "error"
    return Finding(
        board=board,
        task_id=task_id,
        kind=kind,
        severity=severity,
        title=title[:200],
        detail=detail,
        score=score,
        signals=list(signals or []),
        action=action,
    )


def semantic_admission_findings(board: str, con: sqlite3.Connection) -> list[Finding]:
    """Deterministic semantic-admission v1 fallback checks across the board graph."""
    graph = load_board_graph(con)
    tasks: dict[str, dict[str, Any]] = graph["tasks"]
    parents_of: dict[str, list[str]] = graph["parents_of"]
    children_of: dict[str, list[str]] = graph["children_of"]
    run_summary: dict[str, str] = graph["run_summary"]

    # Pre-parse markers for all tasks
    parsed: dict[str, tuple[dict[str, str], list[str]]] = {}
    for tid, task in tasks.items():
        parsed[tid] = parse_semantic_markers(task.get("body") or "")

    # Index research waves and corrections across board (all statuses)
    # research_sig -> list of (task_id, wave)
    research_by_sig: dict[str, list[tuple[str, str]]] = {}
    # correction_sig -> list of (task_id, round_int|None, created_at)
    corrections_by_sig: dict[str, list[tuple[str, Optional[int], int]]] = {}

    for tid, task in tasks.items():
        markers, errors = parsed[tid]
        if not is_semantic_opt_in(markers):
            continue
        if research_identity(markers):
            sig = normalize_signature(markers.get("RESEARCH_SIGNATURE") or "")
            wave = trim_id(markers.get("RESEARCH_WAVE") or "")
            if sig:
                research_by_sig.setdefault(sig, []).append((tid, wave))
        if is_correction(markers):
            csig = normalize_signature(markers.get("PLANNER_CORRECTION_SIGNATURE") or "")
            raw_round = markers.get("PLANNER_CORRECTION_ROUND")
            rnd: Optional[int]
            try:
                rnd = int(str(raw_round).strip()) if raw_round is not None and str(raw_round).strip() != "" else None
            except Exception:
                rnd = None
            created = int(task.get("created_at") or 0)
            if csig:
                corrections_by_sig.setdefault(csig, []).append((tid, rnd, created))

    # Stable first correction = lowest created_at then task_id
    first_correction_id: dict[str, str] = {}
    for csig, items in corrections_by_sig.items():
        items_sorted = sorted(items, key=lambda x: (x[2], x[0]))
        first_correction_id[csig] = items_sorted[0][0]

    findings: list[Finding] = []

    for tid, task in tasks.items():
        markers, errors = parsed[tid]
        if not is_semantic_opt_in(markers):
            continue  # legacy unmarked: current treatment only

        status = task.get("status") or ""
        title = (task.get("title") or "")[:80]
        body = task.get("body") or ""

        # Contract invalid on conflicting duplicates or required-field gaps
        if errors:
            findings.append(
                _semantic_finding(
                    board,
                    tid,
                    "semantic_contract_invalid",
                    f"Semantic contract invalid: {title}",
                    f"Opted-in SEMANTIC_ADMISSION v1 markers are malformed: {errors}",
                    status=status,
                    signals=errors,
                    score=9,
                )
            )
            # still continue other checks where possible

        # --- Research rules ---
        if research_identity(markers):
            sig_raw = markers.get("RESEARCH_SIGNATURE")
            wave_raw = markers.get("RESEARCH_WAVE")
            sig = normalize_signature(sig_raw or "")
            wave = trim_id(wave_raw or "")
            if not sig or not wave:
                findings.append(
                    _semantic_finding(
                        board,
                        tid,
                        "semantic_contract_invalid",
                        f"Research missing signature/wave: {title}",
                        "Research cards require non-empty RESEARCH_SIGNATURE and RESEARCH_WAVE.",
                        status=status,
                        signals=["research_requires_signature_and_wave"],
                        score=9,
                    )
                )
            else:
                # discovery closed on self or ancestors
                closed = _truthy_marker(markers.get("DISCOVERY_CLOSED"))
                proven = set(parse_proven_signatures(markers.get("PROVEN_SIGNATURES")))
                for aid in ancestor_ids(tid, parents_of):
                    am, _ae = parsed.get(aid, ({}, []))
                    if not is_semantic_opt_in(am) and not am:
                        # still honor markers if present even without opt-in on ancestor?
                        # Contract: "card/ancestor DISCOVERY_CLOSED: true" — marker presence matters
                        pass
                    if _truthy_marker(am.get("DISCOVERY_CLOSED")):
                        closed = True
                    proven.update(parse_proven_signatures(am.get("PROVEN_SIGNATURES")))
                # Also read ancestors without requiring their opt-in for closure markers
                if closed or sig in proven:
                    findings.append(
                        _semantic_finding(
                            board,
                            tid,
                            "semantic_no_progress:discovery_closed",
                            f"Discovery closed / proven research: {title}",
                            (
                                f"Opted-in research signature={sig!r} blocked: "
                                f"discovery_closed={closed} proven={sig in proven}."
                            ),
                            status=status,
                            signals=[
                                f"signature={sig}",
                                f"discovery_closed={closed}",
                                f"proven={sig in proven}",
                            ],
                            score=10,
                        )
                    )

                # wave budget: first distinct wave for signature is allowed (any parallels);
                # a different wave is research_wave_repeat. Exclude self when rechecking.
                peers = research_by_sig.get(sig) or []
                other_waves = sorted({w for pid, w in peers if pid != tid and w})
                if other_waves:
                    # allowed if our wave equals the earliest-created wave among all peers+self
                    # Contract: "first distinct RESEARCH_WAVE" — treat min wave by first appearance order
                    # Use created_at of first task that introduced a wave.
                    wave_first_ts: dict[str, tuple[int, str]] = {}
                    for pid, w in peers:
                        if not w:
                            continue
                        ts = int((tasks.get(pid) or {}).get("created_at") or 0)
                        prev = wave_first_ts.get(w)
                        if prev is None or (ts, pid) < prev:
                            wave_first_ts[w] = (ts, pid)
                    if wave_first_ts:
                        # canonical first wave = earliest (ts, tid)
                        first_wave = sorted(wave_first_ts.items(), key=lambda kv: (kv[1][0], kv[1][1], kv[0]))[0][0]
                        if wave != first_wave:
                            findings.append(
                                _semantic_finding(
                                    board,
                                    tid,
                                    "semantic_no_progress:research_wave_repeat",
                                    f"Research wave repeat: {title}",
                                    (
                                        f"signature={sig!r} wave={wave!r} but first admissible "
                                        f"wave is {first_wave!r}. Parallel same-wave OK; "
                                        f"second wave is semantic_no_progress."
                                    ),
                                    status=status,
                                    signals=[f"signature={sig}", f"wave={wave}", f"first_wave={first_wave}"],
                                    score=10,
                                )
                            )

        # --- Replan transaction / SUPERSEDES ---
        if "REPLAN_TRANSACTION" in markers:
            txn = trim_id(markers.get("REPLAN_TRANSACTION") or "")
            raw_sup = markers.get("SUPERSEDES")
            if raw_sup is None or not str(raw_sup).strip():
                findings.append(
                    _semantic_finding(
                        board,
                        tid,
                        "semantic_contract_invalid",
                        f"REPLAN_TRANSACTION missing SUPERSEDES: {title}",
                        "REPLAN_TRANSACTION requires non-empty comma-separated SUPERSEDES IDs.",
                        status=status,
                        signals=["missing_supersedes", f"txn={txn}"],
                        score=9,
                    )
                )
            else:
                supers = parse_supersedes(raw_sup)
                if not supers:
                    findings.append(
                        _semantic_finding(
                            board,
                            tid,
                            "semantic_contract_invalid",
                            f"REPLAN_TRANSACTION empty SUPERSEDES: {title}",
                            "SUPERSEDES parsed empty after trim; need concrete task IDs.",
                            status=status,
                            signals=["empty_supersedes", f"txn={txn}"],
                            score=9,
                        )
                    )
                else:
                    missing = [sid for sid in supers if sid not in tasks]
                    if missing:
                        findings.append(
                            _semantic_finding(
                                board,
                                tid,
                                "semantic_contract_invalid",
                                f"SUPERSEDES unknown IDs: {title}",
                                f"Malformed/missing superseded IDs: {missing}",
                                status=status,
                                signals=[f"missing={missing}"],
                                score=9,
                            )
                        )
                    active_old: list[str] = []
                    for sid in supers:
                        if sid not in tasks:
                            continue
                        members = [sid] + descendant_ids(sid, children_of)
                        for mid in members:
                            st = (tasks.get(mid) or {}).get("status") or ""
                            if st in ACTIVE_GRAPH_STATUSES:
                                active_old.append(f"{mid}:{st}")
                    if active_old:
                        findings.append(
                            _semantic_finding(
                                board,
                                tid,
                                "semantic_no_progress:replan_superseded_graph_active",
                                f"Superseded graph still active: {title}",
                                (
                                    f"REPLAN_TRANSACTION={txn!r} SUPERSEDES has active "
                                    f"old-graph members: {active_old[:20]}"
                                ),
                                status=status,
                                signals=active_old[:12],
                                score=10,
                            )
                        )

        # --- Live evidence ---
        live_goal = _truthy_marker(markers.get("LIVE_GOAL")) or _truthy_marker(
            markers.get("LIVE_EVIDENCE_REQUIRED")
        )
        if live_goal and status == "done":
            result = task.get("result") or ""
            summary = run_summary.get(tid) or ""
            if not has_live_evidence_pass(result, summary):
                findings.append(
                    _semantic_finding(
                        board,
                        tid,
                        "semantic_no_progress:live_evidence_missing",
                        f"Live goal done without LIVE_EVIDENCE PASS: {title}",
                        (
                            "Opted-in LIVE_GOAL/LIVE_EVIDENCE_REQUIRED task is done but "
                            "persisted result/summary lack exact standalone "
                            "'LIVE_EVIDENCE: PASS'. Body/unit GREEN does not count. "
                            "Emitting violation evidence only; not mutating terminal history."
                        ),
                        status=status,
                        signals=["live_evidence_missing", "source=result|run_summary"],
                        score=10,
                        terminal_evidence_only=True,
                    )
                )

        if is_downstream_live_gate(markers) and status in (
            "todo",
            "ready",
            "blocked",
            "running",
        ):
            # need direct done parent with marker in result/summary
            direct_parents = parents_of.get(tid) or []
            ok = False
            for pid in direct_parents:
                pt = tasks.get(pid) or {}
                if (pt.get("status") or "") != "done":
                    continue
                if has_live_evidence_pass(pt.get("result") or "", run_summary.get(pid) or ""):
                    ok = True
                    break
            if not ok:
                findings.append(
                    _semantic_finding(
                        board,
                        tid,
                        "semantic_no_progress:live_parent_evidence_missing",
                        f"Live parent evidence missing: {title}",
                        (
                            "REVIEWER/CUTOVER with LIVE_EVIDENCE_REQUIRED needs a direct "
                            "done parent whose persisted result/summary contains exact "
                            "standalone 'LIVE_EVIDENCE: PASS'. Child body marker ignored."
                        ),
                        status=status,
                        signals=[f"parents={direct_parents}"],
                        score=10,
                    )
                )

        # --- Planner correction breaker ---
        if is_correction(markers):
            csig = normalize_signature(markers.get("PLANNER_CORRECTION_SIGNATURE") or "")
            raw_round = markers.get("PLANNER_CORRECTION_ROUND")
            try:
                rnd = int(str(raw_round).strip()) if raw_round is not None and str(raw_round).strip() != "" else None
            except Exception:
                rnd = None
            if not csig or rnd is None:
                findings.append(
                    _semantic_finding(
                        board,
                        tid,
                        "semantic_contract_invalid",
                        f"Correction missing signature/round: {title}",
                        "PLANNER_CORRECTION requires PLANNER_CORRECTION_SIGNATURE and integer ROUND.",
                        status=status,
                        signals=["correction_requires_signature_and_round"],
                        score=9,
                    )
                )
            else:
                first_id = first_correction_id.get(csig)
                # Allow only first distinct task with round 1
                if tid != first_id or rnd != 1:
                    findings.append(
                        _semantic_finding(
                            board,
                            tid,
                            "semantic_no_progress:planner_correction_repeat",
                            f"Planner correction repeat: {title}",
                            (
                                f"signature={csig!r} round={rnd}; only first task "
                                f"{first_id!r} with round 1 is admissible."
                            ),
                            status=status,
                            signals=[f"signature={csig}", f"round={rnd}", f"first={first_id}"],
                            score=10,
                        )
                    )

    return findings


@dataclass
class Finding:
    board: str
    task_id: str
    kind: str
    severity: str  # warning|error|critical
    title: str
    detail: str
    score: int = 0
    signals: list[str] = field(default_factory=list)
    action: str = "comment"  # comment|block|none


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(msg: str) -> None:
    line = f"{now_iso()} {msg}"
    print(msg)
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
    tmp.replace(path)


def discover_boards() -> list[tuple[str, Path]]:
    """Return boards for the selected isolated orchestration lane."""
    return discover_lane_boards(HERMES_ROOT)


def table_cols(con: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"pragma table_info({table})")}


def extract_artifacts(body: str) -> list[str]:
    found: list[str] = []
    for m in ARTIFACT_LINE.finditer(body or ""):
        s = m.group(1).strip().strip("`")
        if _looks_like_artifact(s) and s not in found:
            found.append(s)
    for m in ARTIFACT_PATH.finditer(body or ""):
        s = m.group(1).strip().strip("`")
        if _looks_like_artifact(s) and s not in found:
            found.append(s)
    # required files bullets often like: `foo.json` — must exist
    for m in re.finditer(r"`([^`]+\.(?:json|jsonl|md|txt|log|sha256|csv|yml|yaml))`", body or "", re.I):
        s = m.group(1)
        if _looks_like_artifact(s) and s not in found:
            found.append(s)
    # drop truncated captures (ARTIFACT_PATH can grab a "/file.ext" suffix of an
    # already-listed full path) and bare/absolute duplicates of the same file
    found = [s for s in found if not any(s != o and o.endswith(s) for o in found)]
    return found


def outcome_count(body: str) -> int:
    # count distinct "outcome" / numbered success criteria blocks
    n = 0
    n += len(re.findall(r"(?im)^\s*(?:outcome|done when|success when|acceptance)\s*[:\-]", body or ""))
    n += len(re.findall(r"(?i)ONE CONCRETE OUTCOME", body or ""))
    # multiple "## " deliverable sections
    secs = re.findall(r"(?im)^\s*##\s+.+(?:evidence|artifact|deliver|output|result)", body or "")
    if len(secs) >= 2:
        n = max(n, len(secs))
    return max(n, 1 if MULTI_OUTCOME.search(body or "") else 0)


def lint_worker_card(board: str, task: dict[str, Any]) -> list[Finding]:
    """Score fat-card / missing fan-out. Only for implement-like assignees."""
    findings: list[Finding] = []
    title = task.get("title") or ""
    body = task.get("body") or ""
    status = task.get("status") or ""
    assignee = (task.get("assignee") or "").lower()
    tid = task["id"]

    if status in ("done", "archived", "running"):
        return findings
    if assignee not in ("worker", "default", ""):
        # still lint unassigned implement titles
        if not IMPLEMENT_HINT.search(title):
            return findings
    # Reviewer cards: do not fat-lint (different role); still allow protocol lint elsewhere
    if assignee == "reviewer" or (REVIEW_HINT.search(title) and assignee != "worker"):
        return findings
    # planner pure PLAN cards: skip fat fan-out lint (they emit work, not implement)
    if assignee == "planner" and PLANNER_HINT.search(title) and not re.search(
        r"(?i)\b(EXECUTE|IMPLEMENT|W\d+-|INT)\b", title
    ):
        return findings
    is_impl = bool(IMPLEMENT_HINT.search(title) or assignee == "worker")
    if not is_impl:
        return findings

    # normalize-only cards: recursive evidence reformatting is stall, not progress.
    # Producer must publish a bounded field-complete handoff in its terminal
    # lifecycle call instead of spawning normalize/normalize-again waves.
    if NORMALIZE_ONLY.search(title):
        action = "block" if status in ("todo", "ready") else "comment"
        findings.append(
            Finding(
                board=board,
                task_id=tid,
                kind="normalize_only_card",
                severity="error",
                title=f"Normalize-only card (stall pattern): {title[:80]}",
                detail=(
                    "Card exists only to normalize/reformat/compact already-collected "
                    "evidence. This is the recursive-normalization anti-pattern: the "
                    "original producer must publish a bounded, field-complete Kanban "
                    "summary in its terminal complete/block call. Do not dispatch; "
                    "replan as producer-side handoff or one exact-file Worker bridge."
                ),
                score=8,
                signals=["normalize_only_title"],
                action=action,
            )
        )
        return findings

    artifacts = extract_artifacts(body)
    signals: list[str] = []
    score = 0

    if len(artifacts) > MAX_FINAL_ARTIFACTS:
        score += 3
        signals.append(f"artifacts={len(artifacts)}>{MAX_FINAL_ARTIFACTS}")
    elif len(artifacts) >= 4:
        score += 1
        signals.append(f"artifacts={len(artifacts)}")

    if len(body) > MAX_BODY_CHARS_WORKER:
        score += 2
        signals.append(f"body_chars={len(body)}>{MAX_BODY_CHARS_WORKER}")

    has_coll = bool(COLLECTION.search(body) or COLLECTION.search(title))
    has_int = bool(INTEGRATION.search(body) or INTEGRATION.search(title))
    if has_coll and has_int:
        score += 4
        signals.append("mix_collection_and_integration")

    has_live = bool(LIVE_STATE.search(body))
    has_arch = bool(ARCHIVAL.search(body))
    if has_live and has_arch:
        score += 3
        signals.append("mix_archival_and_live_state")

    if FINAL_DIR.search(body) and not STAGING_DIR.search(body):
        score += 3
        signals.append("final_dir_without_staging")

    if FINAL_DIR.search(body) and has_coll and has_int:
        score += 2
        signals.append("single_card_owns_full_contract")

    oc = outcome_count(body)
    if oc > MAX_OUTCOME_MARKERS:
        score += 2
        signals.append(f"outcomes≈{oc}")

    # fan-out opportunity: multiple W-like concerns listed but single worker card
    w_markers = re.findall(r"\bW[1-9]\b", title + "\n" + body)
    if len(set(w_markers)) >= 2 and assignee == "worker" and "INT" not in title.upper():
        score += 3
        signals.append(f"multi_W_markers={sorted(set(w_markers))}")

    # independent list without staging split
    if PARALLEL_HINT.search(body) and has_coll and has_int and not STAGING_DIR.search(body):
        score += 2
        signals.append("parallel_hint_but_no_staging")

    # required 10 files pattern (telemt case)
    if re.search(r"(?i)\b(?:10|ten)\b.+(?:files|artifacts|items)|required evidence item", body):
        score += 3
        signals.append("large_required_evidence_pack")

    if score < 3:
        return findings

    severity = "warning" if score < 6 else ("error" if score < 9 else "critical")
    # Only auto-block ready/todo critical fat cards that are not already blocked
    action = "comment"
    if severity == "critical" and status in ("todo", "ready") and assignee in ("worker", "default", ""):
        action = "block"

    art_preview = ", ".join(artifacts[:12]) + ("…" if len(artifacts) > 12 else "")
    detail = (
        f"plan-lint score={score} signals={signals}\n"
        f"artifacts({len(artifacts)}): {art_preview}\n"
        f"REQUIRED FAN-OUT: split into independent staging Workers "
        f"(each ≤{MAX_FINAL_ARTIFACTS} artifacts, one outcome, own staging dir) "
        f"+ single INT writer to final/ + Reviewer only after INT SUCCESS.\n"
        f"Do not redispatch this mono-card. Preserve any partial paths."
    )
    findings.append(
        Finding(
            board=board,
            task_id=tid,
            kind="fat_card_needs_fanout",
            severity=severity,
            title=f"Fat card needs fan-out: {title[:80]}",
            detail=detail,
            score=score,
            signals=signals,
            action=action,
        )
    )
    return findings


def normalize_error(err: Optional[str]) -> str:
    if not err:
        return ""
    e = err.lower()
    if "without calling kanban_complete" in e or "protocol violation" in e:
        return PROTOCOL_SIG
    if "pid" in e and "not alive" in e:
        return "worker_pid_dead"
    # strip volatile bits
    e = re.sub(r"\b\d+\b", "N", e)
    e = re.sub(r"\s+", " ", e).strip()[:160]
    return e


def protocol_streak_findings(board: str, con: sqlite3.Connection, task: dict[str, Any]) -> list[Finding]:
    cols = table_cols(con, "task_runs") if "task_runs" in {
        r[0] for r in con.execute("select name from sqlite_master where type='table'")
    } else set()
    # task_runs may not exist on older boards
    try:
        runs = con.execute(
            "select status, outcome, error, started_at from task_runs where task_id=? order by id desc limit 8",
            (task["id"],),
        ).fetchall()
    except Exception:
        runs = []

    streak = 0
    last_sig = None
    for r in runs:
        if isinstance(r, sqlite3.Row):
            err = r["error"] if "error" in r.keys() else r[2]
            outcome = r["outcome"] if "outcome" in r.keys() else r[1]
        else:
            outcome, err = r[1], r[2]
        sig = normalize_error(err)
        if outcome in ("crashed", "failed", "blocked") and sig == PROTOCOL_SIG:
            if last_sig in (None, PROTOCOL_SIG):
                streak += 1
                last_sig = PROTOCOL_SIG
            else:
                break
        else:
            break

    # also consecutive_failures + last_failure_error
    last_err = task.get("last_failure_error") or ""
    if normalize_error(last_err) == PROTOCOL_SIG:
        cf = int(task.get("consecutive_failures") or 0)
        streak = max(streak, cf)

    if streak < PROTOCOL_STREAK_LIMIT:
        return []

    if task.get("status") in ("done", "archived"):
        return []

    detail = (
        f"protocol_no_complete streak={streak} (≥{PROTOCOL_STREAK_LIMIT}). "
        f"Same signature retries exhausted. Salvage partial artifacts, "
        f"block as needs_replan, force STAGING fan-out + single INT. "
        f"Do not redispatch identical body."
    )
    # Already blocked: comment only (avoid block churn). Ready/todo/running: block.
    st = task.get("status") or ""
    action = "block" if st in ("todo", "ready", "running") else "comment"
    return [
        Finding(
            board=board,
            task_id=task["id"],
            kind="protocol_streak_needs_replan",
            severity="critical",
            title=f"Protocol×{streak}: { (task.get('title') or '')[:80]}",
            detail=detail,
            score=10 + streak,
            signals=[PROTOCOL_SIG, f"streak={streak}"],
            action=action,
        )
    ]


def assignee_findings(board: str, task: dict[str, Any]) -> list[Finding]:
    """Assignee with no dispatcher profile: card can never be claimed."""
    asg = (task.get("assignee") or "").strip().lower()
    if asg in KNOWN_ASSIGNEES:
        return []
    if task.get("status") in ("done", "archived"):
        return []
    return [
        Finding(
            board=board,
            task_id=task["id"],
            kind="unknown_assignee",
            severity="error",
            title=f"Unknown assignee '{asg}' (no dispatcher): {(task.get('title') or '')[:80]}",
            detail=(
                "No Hermes profile dispatches this assignee, so the card can never "
                "be claimed if unblocked. Reassign to worker (implement) or "
                "planner/reviewer, or create a matching profile."
            ),
            score=4,
            signals=[f"assignee={asg}"],
            action="comment",
        )
    ]


def max_retries_findings(board: str, task: dict[str, Any]) -> list[Finding]:
    if "max_retries" not in task:
        return []
    mr = task.get("max_retries")
    if mr is None:
        return []
    try:
        mr_i = int(mr)
    except Exception:
        return []
    if mr_i <= 3:
        return []
    if task.get("status") in ("done", "archived"):
        return []
    return [
        Finding(
            board=board,
            task_id=task["id"],
            kind="max_retries_too_high",
            severity="warning",
            title=f"max_retries={mr_i} (>3): {(task.get('title') or '')[:80]}",
            detail="Anti-stall policy: new/active cards should use max_retries≤3. "
            "Commander should edit/recreate with --max-retries 3.",
            score=1,
            signals=[f"max_retries={mr_i}"],
            action="comment",
        )
    ]


def hermes_cmd(board: str, args: list[str], dry: bool) -> tuple[int, str]:
    cmd = [HERMES_BIN, "kanban", "--board", board] + args
    if dry:
        return 0, f"DRY {cmd}"
    env = os.environ.copy()
    env["HERMES_HOME"] = str(HERMES_ROOT)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=60)
        out = (r.stdout or "") + (r.stderr or "")
        return r.returncode, out.strip()
    except Exception as e:
        return 1, str(e)


def apply_finding(f: Finding, dry: bool, state: dict[str, Any]) -> dict[str, Any]:
    key = f"{f.board}:{f.task_id}:{f.kind}"
    prev = (state.get("actions") or {}).get(key) or {}
    # avoid comment spam: same kind within 6h unless severity critical and action block
    now = time.time()
    if prev.get("ts") and now - float(prev["ts"]) < 6 * 3600:
        if not (f.severity == "critical" and f.action == "block" and prev.get("action") != "block"):
            return {"key": key, "skipped": "recently_acted", "prev": prev}

    comment = (
        f"[plan-lint][{f.severity}] {f.kind}\n"
        f"{f.title}\n\n{f.detail}\n\n"
        f"Fan-out policy: max independent parallel Workers with exclusive staging; "
        f"one INT owns final/; Reviewer only after INT SUCCESS; "
        f"failure_limit=3; protocol×2 → replan/split."
    )
    results = {"key": key, "comment_rc": None, "block_rc": None, "dry": dry}

    # Terminal-evidence-only / action=none: report without DB mutation
    if f.action in ("none", "", None):
        results["skipped"] = "action_none_evidence_only"
        state.setdefault("actions", {})[key] = {
            "ts": now,
            "action": "none",
            "severity": f.severity,
            "kind": f.kind,
        }
        return results

    rc, out = hermes_cmd(
        f.board,
        ["comment", f.task_id, "--author", "plan-lint", comment],
        dry=dry,
    )
    results["comment_rc"] = rc
    results["comment_out"] = out[:300]

    if f.action == "block" and (
        f.kind in ("fat_card_needs_fanout", "protocol_streak_needs_replan", "normalize_only_card")
        or f.kind in SEMANTIC_BLOCK_KINDS
        or f.kind.startswith("semantic_")
    ):
        # only block if not done; for already blocked, still comment
        reason = f"plan-lint:{f.kind}: {f.title}"[:200]
        # Hermes block --kind if supported
        args = ["block", f.task_id, "--reason", reason]
        # try kind needs_replan / transient
        rc2, out2 = hermes_cmd(f.board, args + ["--kind", "needs_replan"], dry=dry)
        if rc2 != 0 and not dry:
            rc2, out2 = hermes_cmd(f.board, args, dry=dry)
        results["block_rc"] = rc2
        results["block_out"] = out2[:300]

    state.setdefault("actions", {})[key] = {
        "ts": now,
        "action": f.action,
        "severity": f.severity,
        "kind": f.kind,
    }
    return results


def scan_board(slug: str, db: Path) -> list[Finding]:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    cols = table_cols(con, "tasks")
    want = ["id", "title", "body", "assignee", "status", "consecutive_failures", "last_failure_error"]
    if "max_retries" in cols:
        want.append("max_retries")
    if "block_kind" in cols:
        want.append("block_kind")
    if "result" in cols:
        want.append("result")
    if "created_at" in cols:
        want.append("created_at")
    sel = ", ".join(want)
    try:
        rows = con.execute(
            f"select {sel} from tasks where status not in ('done','archived')"
        ).fetchall()
    except Exception as e:
        log(f"scan fail {slug}: {e}")
        return []

    findings: list[Finding] = []
    for r in rows:
        task = dict(r)
        findings.extend(lint_worker_card(slug, task))
        findings.extend(protocol_streak_findings(slug, con, task))
        findings.extend(max_retries_findings(slug, task))
        findings.extend(assignee_findings(slug, task))
    # Semantic admission v1 fallback: may also emit evidence for terminal done tasks
    try:
        findings.extend(semantic_admission_findings(slug, con))
    except Exception as e:
        log(f"semantic scan fail {slug}: {e}")
    return findings


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--boards", default="", help="comma slugs; empty=all")
    ap.add_argument("--apply-blocks", action="store_true", help="allow block actions (default on unless dry-run)")
    ap.add_argument("--no-blocks", action="store_true", help="comments only")
    args = ap.parse_args()

    boards = discover_boards()
    if args.boards:
        allow = {b.strip() for b in args.boards.split(",") if b.strip()}
        boards = [b for b in boards if b[0] in allow]

    all_findings: list[Finding] = []
    for slug, db in boards:
        fs = scan_board(slug, db)
        all_findings.extend(fs)
        log(f"board={slug} findings={len(fs)}")

    # sort critical first
    all_findings.sort(key=lambda f: ({"critical": 0, "error": 1, "warning": 2}.get(f.severity, 9), -f.score))

    state = load_json(STATE_PATH, {"actions": {}})
    applied = []
    dry = bool(args.dry_run)
    for f in all_findings:
        if args.no_blocks and f.action == "block":
            f.action = "comment"
        # default: apply blocks for critical unless dry-run
        if dry:
            applied.append(apply_finding(f, dry=True, state=state))
        else:
            applied.append(apply_finding(f, dry=False, state=state))

    if not dry:
        save_json(STATE_PATH, state)

    report = {
        "generated_at": now_iso(),
        "dry_run": dry,
        "boards": [b[0] for b in boards],
        "finding_count": len(all_findings),
        "critical": sum(1 for f in all_findings if f.severity == "critical"),
        "error": sum(1 for f in all_findings if f.severity == "error"),
        "warning": sum(1 for f in all_findings if f.severity == "warning"),
        "findings": [asdict(f) for f in all_findings[:100]],
        "applied": applied[:100],
        "policy": {
            "max_final_artifacts": MAX_FINAL_ARTIFACTS,
            "max_body_chars_worker": MAX_BODY_CHARS_WORKER,
            "protocol_streak_limit": PROTOCOL_STREAK_LIMIT,
            "fan_out": "staging per Worker + single INT + Reviewer after INT SUCCESS",
            "failure_limit": 3,
            "semantic_admission": "v1_opt_in_fallback",
        },
    }
    save_json(REPORT_PATH, report)

    # human summary
    print(
        f"plan_lint findings={len(all_findings)} "
        f"critical={report['critical']} error={report['error']} "
        f"warning={report['warning']} dry={dry}"
    )
    for f in all_findings[:30]:
        print(f"  [{f.severity}] {f.board}/{f.task_id} {f.kind} score={f.score} action={f.action}")
        print(f"    {f.title}")
        if f.signals:
            print(f"    signals: {f.signals}")
    # --dry-run is report-only hygiene: always exit 0 so cron/supervisor can
    # ingest diagnostics without treating pre-existing critical fat cards as a
    # hard runner failure. Enforcing mode still returns 2 when critical>0.
    if dry:
        return 0
    return 0 if report["critical"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
