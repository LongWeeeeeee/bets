# Multi-agent workflow — Plan → Worker → Review (classic opencode-orchestrator loop)

> Detailed reference for the workflow summarized in `AGENTS.md`. Classic review-after-run
> loop: Plan → Worker → Review → (ISSUES → replan → Worker → …) until APPROVE or a
> safeguard trips.

## Roles & models

Primary runtime is **opencode**. Agents live under `.opencode/agent/` and pin concrete
provider/model IDs:

| Role | Model (opencode) | Responsibility | Prompt |
|---|---|---|---|
| **Commander** | `opencode/claude-opus-4-8` (OpenCode Zen, primary/`build`) | Drives the loop: hands the plan to the Worker, on ISSUES calls the Planner to replan, enforces exit safeguards. Does not write code. | primary session |
| **Planner** | `opencode/claude-opus-4-8` (OpenCode Zen) | Plans tasks; on replan, plans **only** the open problems. Does not implement, does not review. | `.opencode/agent/planner.md` |
| **Worker** | `opencode-go/glm-5.2` (OpenCode Go) | Implements the approved plan in full; tools, edits, tests. Emits `SUCCESS`/`FAILED`. Does not plan or review. | `.opencode/agent/worker.md` |
| **Reviewer** | `opencode/claude-opus-4-8` (OpenCode Zen) | Reviews the finished diff; emits `APPROVE` or `ISSUES` with stable signatures. Read-only. | `.opencode/agent/reviewer.md` |

- **Claude Opus 4.8** (Commander, Planner, Reviewer) is served by the **`opencode`** provider ("OpenCode Zen").
- **GLM 5.2** (Worker) is served by the **`opencode-go`** provider ("OpenCode Go").
- Project `opencode.json` sets default/`build` (Commander) and `plan` to
  `opencode/claude-opus-4-8`; the Worker subagent pins `opencode-go/glm-5.2`. Restart
  opencode after config changes — config loads once at startup, not hot-reloaded.
- `.claude/agents/*` are legacy mirrors for Claude Code (whose subagent `model:`
  accepts Claude tiers only). Authoritative wiring is `.opencode/agent/*`.

## The loop

```
START -> PLANNING -> WORKING -> REVIEWING --+--> APPROVED   (reviewer APPROVE — done)
                                             +--> REPLANNING -> WORKING ... (reviewer ISSUES)
```

1. **Planner** produces a plan for the task (concrete, no decisions left to the Worker).
2. **Worker** implements the plan **in full**, without step-by-step review stops; ends
   with `SUCCESS` or `FAILED`.
3. On `SUCCESS` the Commander runs the **Reviewer** over the full diff.
4. **APPROVE** (no Critical) → task done.
5. **ISSUES** → Planner replans **only** for the open problems → new Worker run →
   Reviewer again. Finding type `needs-replan` (Critical) means an architectural decision
   escaped the plan; the Planner must fold it into the replan (not a Worker "finish it").
6. `FAILED` from the Worker → STOP (blocker).

## Exit safeguards (any one → STOP + human report)

- **stuck** — the same problem signature stays open after 2 consecutive fix runs.
- **cycle** — the open-problem set repeats a previous iteration's set (fix A breaks B,
  fix B restores A).
- **limit** — more than `MAX_FIX_ITERS = 3` replan→run→review cycles.

On trigger: do not mark done; hand the human a report — open problems + attempt
history (what changed + what the Reviewer said each iteration) + exit reason.

## Structured messages

- **Worker** ends its run with one JSON block on the last line:
  `{"status":"SUCCESS","summary":...,"tests":...,"files":[...]}` or
  `{"status":"FAILED","reason":...,"attempted":...,"files":[...]}`.
- **Planner** ends with `{"status":"PLAN","plan":...,"scope":"initial|replan","open_issues":[...]}`.
- **Reviewer** first line is the verdict: `APPROVE` or `ISSUES`; ISSUES lines are
  `<severity> | <file>:<type>:<text> | <what to do>`.

## Testable contract

`base/agent_workflow.py` encodes the states/transitions + safeguards;
`base/tests/test_agent_workflow.py` verifies: normal completion, replan-then-approve,
stuck, cycle, limit, worker failure, and illegal-transition/malformed-message guards.

```bash
venv_catboost/bin/python3 -m pytest base/tests/test_agent_workflow.py -v
```
