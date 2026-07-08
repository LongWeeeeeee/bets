"""Classic multi-agent orchestration loop (Plan -> Worker -> Review -> replan).

Dependency-free encoding of the opencode-orchestrator style workflow used by the prose
workflow in ``AGENTS.md`` / ``docs/MULTI_AGENT_WORKFLOW.md``. It exists so the loop and
its exit safeguards can be verified deterministically by ``pytest`` without spinning
up real agents or touching the live Dota pipeline.

It does NOT run agents. It models the states/transitions the Commander drives:

    START -> PLANNING -> WORKING -> REVIEWING --+--> APPROVED  (review OK, done)
                                               +--> REPLANNING -> WORKING ... (review ISSUES)

On review ISSUES the Planner replans ONLY for the open problems, then the Worker
implements again and the Reviewer reviews again — classic review-after-run loop.

Exit safeguards (any one -> STOP + report to human):
  - LIMIT  : more than MAX_FIX_ITERS review->replan cycles.
  - STUCK  : the same open problem signature persists after 2 consecutive fix runs.
  - CYCLE  : the set of open problem signatures repeats a previous iteration's set.

Models (wired in opencode config, not here): Worker = GLM (opencode-go);
Planner + Reviewer + Commander = Claude Opus 4.8 (opencode / OpenCode Zen).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


MAX_FIX_ITERS = 3


class WorkflowState(Enum):
    START = "START"
    PLANNING = "PLANNING"
    WORKING = "WORKING"
    REVIEWING = "REVIEWING"
    REPLANNING = "REPLANNING"  # Planner produces a fix plan for open issues
    APPROVED = "APPROVED"      # terminal success
    FAILED = "FAILED"          # terminal (Worker gave up / safeguard tripped)

    def is_terminal(self) -> bool:
        return self in (WorkflowState.APPROVED, WorkflowState.FAILED)


class WorkflowError(RuntimeError):
    """Illegal transition."""


# ---------------------------------------------------------------------------
# Parsed agent messages
# ---------------------------------------------------------------------------
def _parse_block(raw: str) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        raise ValueError("empty message")
    cand = text
    if not cand.startswith("{"):
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if not lines:
            raise ValueError("empty message")
        cand = lines[-1].strip()
    try:
        data = json.loads(cand)
    except json.JSONDecodeError as exc:
        raise ValueError(f"message is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("message must be a JSON object")
    return data


def parse_worker_message(raw: str) -> Dict[str, Any]:
    d = _parse_block(raw)
    if d.get("status") not in ("SUCCESS", "FAILED"):
        raise ValueError("worker status must be SUCCESS or FAILED")
    return d


def parse_reviewer_message(raw: str) -> Dict[str, Any]:
    """Reviewer emits APPROVE or ISSUES.

    ISSUES payload: {"verdict":"ISSUES","findings":[{"sig":"file:type:text","severity":"Critical|Minor"}, ...]}
    APPROVE payload: {"verdict":"APPROVE"}
    A bare first line "APPROVE"/"ISSUES" is also accepted (reviewer prompt format).
    """
    text = (raw or "").strip()
    # Accept the reviewer prompt's plain format: first non-empty line is the verdict.
    first = text.splitlines()[0].strip() if text else ""
    if first in ("APPROVE", "ISSUES"):
        d: Dict[str, Any] = {"verdict": first}
        if first == "ISSUES":
            findings = _parse_findings_lines(text)
            if findings:
                d["findings"] = findings
        return d
    # Otherwise expect a JSON block.
    d = _parse_block(text)
    if d.get("verdict") not in ("APPROVE", "ISSUES"):
        raise ValueError("reviewer verdict must be APPROVE or ISSUES")
    if d["verdict"] == "ISSUES":
        d.setdefault("findings", [])
    return d


def _parse_findings_lines(text: str) -> List[Dict[str, str]]:
    """Parse lines like: 'Critical | file:type:text | what to do'."""
    out: List[Dict[str, str]] = []
    for ln in text.splitlines()[1:]:
        ln = ln.strip()
        if not ln or "|" not in ln:
            continue
        parts = [p.strip() for p in ln.split("|")]
        if len(parts) < 2:
            continue
        severity, signature = parts[0], parts[1]
        out.append({"severity": severity, "sig": signature})
    return out


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
@dataclass
class WorkflowEngine:
    """Drives one task through the classic Plan -> Worker -> Review loop."""

    task_id: str
    state: WorkflowState = WorkflowState.START
    plan: Optional[str] = None
    plans: List[str] = field(default_factory=list)            # [0] = initial plan
    iterations: int = 0                                       # review->replan cycles completed
    open_signatures: List[Set[str]] = field(default_factory=list)  # open set per iter
    worker_result: Optional[Dict[str, Any]] = None
    review_history: List[Dict[str, Any]] = field(default_factory=list)
    exit_reason: Optional[str] = None  # set when a safeguard trips
    # Parallel fan-out: when a plan decomposes into independent subtasks, the Commander
    # launches one Worker (GLM) per subtask concurrently. Each worker owns a
    # non-overlapping file scope; results are aggregated before review.
    subtasks: List[str] = field(default_factory=list)
    sub_results: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def parallel(self) -> bool:
        return bool(self.subtasks)

    # ---- lifecycle ----------------------------------------------------------
    def start(self) -> None:
        if self.state is not WorkflowState.START:
            raise WorkflowError(f"start() only valid from START, not {self.state.name}")
        self.state = WorkflowState.PLANNING

    def submit_plan(self, plan: str, subtasks: Optional[List[str]] = None) -> WorkflowState:
        """Planner produces a plan (initial or a replan). -> WORKING.

        If ``subtasks`` is given, the plan is decomposed into independent chunks and
        the Commander will fan out one Worker per subtask concurrently (parallel mode).
        Each subtask must own a non-overlapping file scope.
        """
        if self.state not in (WorkflowState.PLANNING, WorkflowState.REPLANNING):
            raise WorkflowError(f"plan only valid in PLANNING/REPLANNING, not {self.state.name}")
        self.plan = plan
        self.plans.append(plan)
        self.subtasks = list(subtasks) if subtasks else []
        self.sub_results = []
        self.state = WorkflowState.WORKING
        return self.state

    def submit_worker_message(self, raw: str) -> WorkflowState:
        """Single-worker run: implements the plan in full; emits SUCCESS or FAILED."""
        if self.state is not WorkflowState.WORKING:
            raise WorkflowError(f"worker message only valid while WORKING, not {self.state.name}")
        if self.parallel:
            raise WorkflowError("parallel plan: use submit_worker_results() with one result per subtask")
        msg = parse_worker_message(raw)
        if msg["status"] == "FAILED":
            self.worker_result = msg
            self.state = WorkflowState.FAILED
            return self.state
        # SUCCESS -> review the diff
        self.worker_result = msg
        self.state = WorkflowState.REVIEWING
        return self.state

    def submit_worker_results(self, results: List[Dict[str, Any]]) -> WorkflowState:
        """Parallel fan-out: one Worker result per subtask, aggregated before review.

        All results must be SUCCESS to reach REVIEWING; any FAILED fails the run.
        Results are accepted positionally aligned to ``subtasks``.
        """
        if self.state is not WorkflowState.WORKING:
            raise WorkflowError(f"parallel results only valid while WORKING, not {self.state.name}")
        if not self.parallel:
            raise WorkflowError("no subtasks: use submit_worker_message() for a single worker")
        if len(results) != len(self.subtasks):
            raise WorkflowError(
                f"expected {len(self.subtasks)} results, got {len(results)}"
            )
        parsed: List[Dict[str, Any]] = []
        for r in results:
            if isinstance(r, str):
                parsed.append(parse_worker_message(r))
            elif isinstance(r, dict) and r.get("status") in ("SUCCESS", "FAILED"):
                parsed.append(r)
            else:
                raise ValueError("each result must be a worker message (status SUCCESS|FAILED)")
        self.sub_results = parsed
        statuses = [r["status"] for r in parsed]
        if any(s == "FAILED" for s in statuses):
            self.worker_result = {"parallel": True, "status": "FAILED", "results": parsed}
            self.state = WorkflowState.FAILED
            return self.state
        # all SUCCESS -> review the combined diff
        self.worker_result = {"parallel": True, "status": "SUCCESS", "results": parsed}
        self.state = WorkflowState.REVIEWING
        return self.state

    def submit_reviewer_message(self, raw: str) -> WorkflowState:
        """Reviewer reviews the diff: APPROVE -> done; ISSUES -> replan (with safeguards)."""
        if self.state is not WorkflowState.REVIEWING:
            raise WorkflowError(f"review only valid while REVIEWING, not {self.state.name}")
        msg = parse_reviewer_message(raw)
        self.review_history.append(msg)

        if msg["verdict"] == "APPROVE":
            self.state = WorkflowState.APPROVED
            return self.state

        # ISSUES -> record open signatures and check safeguards before replanning.
        sigs = {f.get("sig", "") for f in msg.get("findings", []) if f.get("sig")}
        # Only Critical findings block; Minor are advisory (reviewer may still APPROVE).
        critical = {
            f["sig"] for f in msg.get("findings", []) if f.get("severity") == "Critical" and f.get("sig")
        }
        blocking = critical if critical else sigs
        self.open_signatures.append(blocking)

        # safeguard 1: iteration limit
        self.iterations += 1
        if self.iterations > MAX_FIX_ITERS:
            self.exit_reason = "limit"
            self.state = WorkflowState.FAILED
            return self.state

        # safeguard 2: stuck — same blocking signature open after 2 consecutive fix runs
        if len(self.open_signatures) >= 2 and self.open_signatures[-1] and self.open_signatures[-1] == self.open_signatures[-2]:
            self.exit_reason = "stuck"
            self.state = WorkflowState.FAILED
            return self.state

        # safeguard 3: cycle — current open set equals some earlier iteration's set
        if len(self.open_signatures) >= 2:
            for prev in self.open_signatures[:-1]:
                if prev == self.open_signatures[-1]:
                    self.exit_reason = "cycle"
                    self.state = WorkflowState.FAILED
                    return self.state

        # No safeguard tripped -> replan for the open problems.
        self.state = WorkflowState.REPLANNING
        return self.state

    # ---- convenience --------------------------------------------------------
    @property
    def fix_runs(self) -> int:
        """Number of Worker implementation runs = initial + replans."""
        return len(self.plans)

    def open_problems(self) -> Set[str]:
        return self.open_signatures[-1] if self.open_signatures else set()

    def attempt_history(self) -> List[Tuple[str, List[str]]]:
        """What changed + what the reviewer said per iteration, for the human report."""
        h: List[Tuple[str, List[str]]] = []
        for i, rev in enumerate(self.review_history):
            plan = self.plans[i] if i < len(self.plans) else ""
            sigs = [f.get("sig", "") for f in rev.get("findings", [])] if rev["verdict"] == "ISSUES" else []
            h.append((plan, sigs))
        return h
