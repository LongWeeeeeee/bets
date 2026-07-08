"""Tests for the classic orchestration loop (base/agent_workflow.py).

Scenarios:
  1. normal completion: plan -> worker -> review APPROVE -> done.
  2. replan loop: review ISSUES -> replan -> worker -> review APPROVE -> done.
  3. stuck safeguard: same blocking signature after 2 consecutive fix runs.
  4. cycle safeguard: open problem set repeats a previous iteration.
  5. limit safeguard: more than MAX_FIX_ITERS review->replan cycles.
  6. worker failure + illegal-transition / malformed-message guards.
"""

import json
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from agent_workflow import (  # noqa: E402
    MAX_FIX_ITERS,
    WorkflowEngine,
    WorkflowError,
    WorkflowState,
    parse_reviewer_message,
    parse_worker_message,
)


def _worker(status="SUCCESS", **kw) -> str:
    kw["status"] = status
    return "did the work...\n" + json.dumps(kw)


def _review(findings=None) -> str:
    if findings is None:
        return "APPROVE\nlooks good"
    lines = ["ISSUES"]
    for sev, sig, todo in findings:
        lines.append(f"{sev} | {sig} | {todo}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 1. normal completion
# ---------------------------------------------------------------------------
def test_normal_completion():
    wf = WorkflowEngine(task_id="T1")
    wf.start()
    assert wf.state is WorkflowState.PLANNING
    wf.submit_plan("plan v1")
    assert wf.state is WorkflowState.WORKING
    wf.submit_worker_message(_worker(summary="done"))
    assert wf.state is WorkflowState.REVIEWING
    wf.submit_reviewer_message(_review())
    assert wf.state is WorkflowState.APPROVED
    assert wf.state.is_terminal()
    assert wf.iterations == 0  # no replan happened


# ---------------------------------------------------------------------------
# 2. replan loop resolves on 2nd iteration
# ---------------------------------------------------------------------------
def test_replan_then_approve():
    wf = WorkflowEngine(task_id="T2")
    wf.start()
    wf.submit_plan("plan v1: implement auth")
    wf.submit_worker_message(_worker(summary="done v1"))
    # reviewer finds a Critical -> replan
    wf.submit_reviewer_message(_review([("Critical", "src/auth.py:NameError:TOKEN undefined", "declare TOKEN")]))
    assert wf.state is WorkflowState.REPLANNING
    # planner replans only for the open problem
    wf.submit_plan("plan v2: declare TOKEN in auth.py")
    assert wf.state is WorkflowState.WORKING
    assert wf.fix_runs == 2
    wf.submit_worker_message(_worker(summary="fixed"))
    wf.submit_reviewer_message(_review())  # APPROVE
    assert wf.state is WorkflowState.APPROVED
    assert wf.iterations == 1


def test_minor_only_does_not_block():
    # Reviewer with only Minor findings still blocks (it returned ISSUES not APPROVE),
    # but no critical signature; the loop still replans. Per the reviewer prompt, if
    # only Minor are open the reviewer should emit APPROVE. Here we just confirm that
    # an ISSUES verdict with no Critical still drives a replan (engine doesn't guess).
    wf = WorkflowEngine(task_id="T2b")
    wf.start()
    wf.submit_plan("plan v1")
    wf.submit_worker_message(_worker())
    wf.submit_reviewer_message(_review([("Minor", "base/x.py:fstring:missing placeholders", "fix")]))
    assert wf.state is WorkflowState.REPLANNING


# ---------------------------------------------------------------------------
# 3. stuck safeguard
# ---------------------------------------------------------------------------
def test_stuck_safeguard():
    wf = WorkflowEngine(task_id="T3")
    wf.start()
    wf.submit_plan("plan v1")
    wf.submit_worker_message(_worker())
    sig = "src/auth.py:NameError:TOKEN undefined"
    # iter 1: open
    wf.submit_reviewer_message(_review([("Critical", sig, "declare TOKEN")]))
    assert wf.state is WorkflowState.REPLANNING
    # iter 2: fix attempt, same signature persists -> stuck
    wf.submit_plan("plan v2: fix TOKEN")
    wf.submit_worker_message(_worker())
    wf.submit_reviewer_message(_review([("Critical", sig, "declare TOKEN")]))
    assert wf.state is WorkflowState.FAILED
    assert wf.exit_reason == "stuck"


# ---------------------------------------------------------------------------
# 4. cycle safeguard (A breaks B, B restores A)
# ---------------------------------------------------------------------------
def test_cycle_safeguard():
    wf = WorkflowEngine(task_id="T4")
    wf.start()
    wf.submit_plan("plan v1")
    wf.submit_worker_message(_worker())
    a = "src/a.py:NameError:A undefined"
    b = "src/b.py:NameError:B undefined"
    wf.submit_reviewer_message(_review([("Critical", a, "fix A")]))  # open {a}
    assert wf.state is WorkflowState.REPLANNING
    wf.submit_plan("plan v2: fix A")
    wf.submit_worker_message(_worker())
    wf.submit_reviewer_message(_review([("Critical", b, "fix B")]))  # open {b}
    assert wf.state is WorkflowState.REPLANNING
    wf.submit_plan("plan v3: fix B")
    wf.submit_worker_message(_worker())
    wf.submit_reviewer_message(_review([("Critical", a, "fix A")]))  # open {a} again -> cycle
    assert wf.state is WorkflowState.FAILED
    assert wf.exit_reason == "cycle"


# ---------------------------------------------------------------------------
# 5. limit safeguard
# ---------------------------------------------------------------------------
def test_limit_safeguard():
    wf = WorkflowEngine(task_id="T5")
    wf.start()
    wf.submit_plan("plan v1")
    wf.submit_worker_message(_worker())
    # churn distinct signatures each iteration so stuck/cycle don't trip, only limit does
    for i in range(MAX_FIX_ITERS + 1):
        if wf.state is WorkflowState.REPLANNING:
            wf.submit_plan(f"plan v{i+2}")
            wf.submit_worker_message(_worker())
        wf.submit_reviewer_message(_review([("Critical", f"src/x.py:logic:problem{i}", "fix")]))
        if i < MAX_FIX_ITERS:
            assert wf.state is WorkflowState.REPLANNING, f"unexpected at iter {i}: {wf.state}"
    assert wf.state is WorkflowState.FAILED
    assert wf.exit_reason == "limit"
    assert wf.iterations == MAX_FIX_ITERS + 1


# ---------------------------------------------------------------------------
# 6. failure handling
# ---------------------------------------------------------------------------
def test_worker_failed():
    wf = WorkflowEngine(task_id="T6")
    wf.start()
    wf.submit_plan("plan v1")
    wf.submit_worker_message(_worker(status="FAILED", reason="env broken"))
    assert wf.state is WorkflowState.FAILED
    assert wf.state.is_terminal()
    assert wf.worker_result["reason"] == "env broken"


def test_illegal_transitions():
    wf = WorkflowEngine(task_id="T7")
    with pytest.raises(WorkflowError):
        wf.submit_plan("plan")  # must start() first
    wf.start()
    with pytest.raises(WorkflowError):
        wf.submit_worker_message(_worker())  # need a plan first
    wf.submit_plan("plan v1")
    with pytest.raises(WorkflowError):
        wf.submit_plan("plan v2")  # already WORKING, can't plan
    with pytest.raises(WorkflowError):
        wf.submit_reviewer_message(_review())  # not REVIEWING yet
    wf.submit_worker_message(_worker())
    with pytest.raises(WorkflowError):
        wf.submit_worker_message(_worker())  # not WORKING anymore


def test_malformed_worker_message():
    with pytest.raises(ValueError):
        parse_worker_message("not json")
    with pytest.raises(ValueError):
        parse_worker_message(json.dumps({"status": "WAT"}))
    with pytest.raises(ValueError):
        parse_worker_message(json.dumps({}))


def test_reviewer_parse_plain_and_json():
    d = parse_reviewer_message("APPROVE\nok")
    assert d["verdict"] == "APPROVE"
    d = parse_reviewer_message("ISSUES\nCritical | a.py:NameError:x | fix\nMinor | b.py:fstring:y | fix")
    assert d["verdict"] == "ISSUES"
    assert d["findings"][0]["severity"] == "Critical"
    assert d["findings"][0]["sig"] == "a.py:NameError:x"
    d = parse_reviewer_message(json.dumps({"verdict": "APPROVE"}))
    assert d["verdict"] == "APPROVE"
    with pytest.raises(ValueError):
        parse_reviewer_message(json.dumps({"verdict": "MAYBE"}))


def test_attempt_history_and_open_problems():
    wf = WorkflowEngine(task_id="T8")
    wf.start()
    wf.submit_plan("plan v1")
    wf.submit_worker_message(_worker())
    wf.submit_reviewer_message(_review([("Critical", "a.py:x:x", "fix")]))
    wf.submit_plan("plan v2: fix")
    wf.submit_worker_message(_worker())
    wf.submit_reviewer_message(_review())  # APPROVE
    assert wf.state is WorkflowState.APPROVED
    assert wf.fix_runs == 2
    hist = wf.attempt_history()
    assert len(hist) == 2
    assert hist[0][1] == ["a.py:x:x"]
    assert hist[1][1] == []  # APPROVE has no findings
