# microsoft/autogen vs Our Hermes Orchestration Stack

**Method:** Read actual source/docs via GitHub raw API (api.github.com / raw.githubusercontent.com), repo `microsoft/autogen` `main` branch, 2026-07-20. Key files inspected: `README.md`, `autogen_agentchat/base/{_team,_task}.py`, `conditions/_terminations.py`, `teams/_group_chat/_base_group_chat.py`, `teams/_group_chat/_magentic_one/_magentic_one_orchestrator.py` + `_prompts.py` + `_magentic_one_group_chat.py`, `teams/_group_chat/_selector_group_chat.py`, `teams/_group_chat/_swarm_group_chat.py`, `state/_states.py`, `docs/src/user-guide/core-user-guide/design-patterns/reflection.ipynb`, `autogen_ext/experimental/task_centric_memory/README.md`.

**Critical context:** AutoGen README carries a `⚠️ Maintenance Mode` notice — *"will not receive new features… community managed"… New users should start with Microsoft Agent Framework (MAF)."* All findings below are about the AutoGen codebase as it sits on `main`, not MAF.

---

## AutoGen orchestration architecture (as shipped)

- **Teams** = `Team`/`BaseGroupChat`: an in-process, asyncio, message-bus (`SingleThreadedAgentRuntime`, pub/sub topics) group of `ChatAgent`s plus a *group-chat manager* that routes the next speaker.
- **GroupChat variants:** `RoundRobinGroupChat`, `SelectorGroupChat` (LLM picks next speaker), `SwarmGroupChat` (HandoffMessage routing), `MagenticOneGroupChat` (ledger-based), `DiGraphGroupChat` (graph routing).
- **MagenticOne = the plan→worker→review→replan loop** (`_magentic_one_orchestrator.py` + `_prompts.py`): the orchestrator LLM runs an *outer loop* (gather FACTS → create PLAN → broadcast full ledger) and an *inner loop* where each iteration fills a *progress ledger* JSON:
  > `is_request_satisfied`, `is_in_loop`, `is_progress_being_made`, `next_speaker`, `instruction_or_question`  (`ORCHESTRATOR_PROGRESS_LEDGER_PROMPT`)
  - If satisfied → `_prepare_final_answer`.
  - If `not is_progress_being_made` **or** `is_in_loop` → `_n_stalls += 1`; on proper progress, `_n_stalls = max(0, _n_stalls-1)`.
  - When `_n_stalls >= max_stalls` (default **3**) → `_update_task_ledger` (replan: rewrite facts, write new plan, "explain root cause of failure") + `_reenter_outer_loop`.
  - Hard ceiling: `max_turns` (default **20**); `_max_json_retries = 10` for malformed ledger JSON.
- **Persistence/resume:** `Team.save_state()` / `load_state()` return a pydantic `Mapping[str,Any]` (`TeamState`, `MagenticOneOrchestratorState` with `task/facts/plan/n_rounds/n_stalls/message_thread`). **No on-disk store is shipped** — the caller owns serialization. `pause()`/`resume()` are cooperative asyncio flags, not process freeze.
- **Reflection/critic** = a `core-user-guide/design-patterns/reflection.ipynb` tutorial: `CoderAgent` ⇄ `ReviewerAgent` with `CodeReviewResult(approved: bool)`; coder re-runs until approved. **Tutorial pattern, not a built-in production primitive.**
- **Termination conditions** (`conditions/_terminations.py`): `MaxMessageTermination`, `TextMentionTermination`, `StopMessageTermination`, `HandoffTermination`, `TimeoutTermination`, `ExternalTermination`, and composable `|` / `&` (`all_met`/`any_met`). `TimeoutTermination` is a wall-clock *conversation-duration* cap (fires a StopMessage), not a per-worker process-kill.
- **Cancellation:** `CancellationToken` cancels the in-flight `asyncio` future cooperatively — *"Setting the cancellation token potentially put the team in an inconsistent state"* (docstring). No SIGTERM/SIGKILL, no re-spawn-to-ready.
- **Task-Centric Memory** (`autogen_ext/experimental`, flagged `EXPERIMENTAL, RESEARCH IN PROGRESS`): memo store keyed on task similarity (think "insights to avoid repeating mistakes"); in-memory/in-process, no durable cross-process board.
- **Models:** each `ChatAgent` takes its own `model_client` → heterogeneous models per agent are trivially supported *within one process*.

---

## Feature-by-feature comparison

LEGEND: **FULL** = AutoGen ships it natively · **PARTIAL** = primitive/convention present but missing core guarantees · **NONE** = absent · **EQUIV-PRIMITIVE** = a building block you'd have to assemble yourself.

| # | Our feature | AutoGen | Evidence / reference |
|---|---|---|---|
| 1 | Durable SQLite kanban (tasks/runs/links/comments/events) across restarts | **NONE** | State is a pydantic `Mapping` returned by `save_state()`; no DB/store shipped — caller owns serialization. No `sqlite3` anywhere under `autogen-agentchat`. |
| 2 | Multi-profile dispatch: planner/worker/reviewer as separate cold processes on diff models | **EQUIV-PRIMITIVE** | Each `ChatAgent` accepts its own `model_client` (hetero models within one process — trivial). gRPC worker-runtime samples (`core_grpc_worker_runtime/`) exist for *distributed runtime* but run inside one app, not cold-process profiles on a shared board. |
| 3 | plan→worker→review→replan loop w/ stuck(same-sig×2), cycle, limit(`MAX_FIX_ITERS=3`) | **PARTIAL** | MagenticOne: `is_in_loop` + `not is_progress_being_made` → `_n_stalls++`; replan at `max_stalls=3`. But it's LLM-*judged* loop detection (prompt asks "are we repeating?"), not signature/ Artifact-delta matching; no A-reverts-B cycle detector. (`_magentic_one_orchestrator.py` ~L388-402) |
| 4 | Parallel fan-out: N workers write own staging, ONE INT card assembles final/, review only after INT success | **NONE** | No staging dirs, no INT card, no assemble-then-gate. Teams are sequential single-threaded asyncio buses; "parallel" = multiple agents in one `SingleThreadedAgentRuntime`. You'd glue Teams-of-Teams yourself. |
| 5 | Per-task `max_runtime` (SIGTERM→grace→SIGKILL); timeout → ready for respawn | **NONE** | `CancellationToken` is cooperative-only; docstring: *"cancellation token potentially put the team in an inconsistent state"*. `TimeoutTermination` ends the *conversation*, doesn't kill a process or re-queue. No re-spawn-to-ready. |
| 6 | Salvage packs: SALVAGE.md + artifacts.json + session_excerpt + log_tail under runtime/task_salvage/<tid>/run_* | **NONE** | Nothing analogous. On unhandled exception the manager emits a `SerializableException` event and re-raises; no disk artifacts, no per-run抢救 directory. (`_base_group_chat.py` save_state has no crash-salvage path.) |
| 7 | PARTIAL.md reviewer checkpoint for resume across timeouts | **PARTIAL** | `save_state`/`load_state` of `MagenticOneOrchestratorState` (task/facts/plan/n_rounds/n_stalls/thread) lets *you* persist+resume — but no opinionated "PARTIAL.md" format, no reviewer-authored checkpoint; resume is caller-driven, not auto-on-timeout. |
| 8 | Plan-lint BEFORE worker dispatch (reject fat mono-cards >5, mixed coll+INT, missing ownership, >4 final artifacts) | **NONE** | Planner is an unconstrained LLM prompt (`ORCHESTRATOR_TASK_LEDGER_PLAN_PROMPT` = "short bullet-point plan"). No structural lint, no artifact-count/ownership rules, no rejection before dispatch. |
| 9 | Retry enforcer: same failure signature + zero artifact delta → block needs_replan; protocol×2 → salvage+split | **NONE** | No artifact-delta tracking, no failure-signature hashing, no "block needs_replan / salvage+split" policy. Retries are *soft*: stall counter increments on LLM-judged stall; malformed ledger retries up to `_max_json_retries=10`. |
| 10 | Hygiene guard cron */5 (assignee guard, dead-pid/stale-heartbeat→block, notify cleanup, auto-archive stale blocked) | **NONE** | Pure in-process asyncio; pid/heartbeat/notify/archive concepts don't exist. No cron, no daemon inspecting live runs. |
| 11 | Process logger: durable snapshots of live workers (pid, age, heartbeat age, anomalies) | **NONE** | No process/pid model. `core-user-guide/framework/logging.md`/`telemetry.ipynb` cover OpenTelemetry-style结构性 logging of messages, not live-worker OS snapshots. |
| 12 | git worktree / branch naming per task | **NONE** | No git integration in `autogen-agentchat`. `MagenticOneCoderAgent` works on local files only; no worktree lifecycle, no branch-per-task. |
| 13 | Evidence contract: every Worker ends with SUCCESS/FAILED + files touched + tests run + commands | **PARTIAL** | Reflection tutorial defines `CodeReviewResult(approved: bool, review: str)` and `CodeWritingResult` — a structured message protocol you'd build on, but no enforced SUCCESS/FAILED+files+tests+commands terminal schema at the framework level. |
| 14 | Staging-then-integration: nothing writes final/ except one INT card; parallel workers read-only shared immutable inputs | **NONE** | No staging/integration directory model. All participants share the *same message thread* (read-write broadcast); no last-writer-wins gating to a single integrator. |
| 15 | Telegram push only on needs_input/gave_up/true terminal finals; intermediate noise suppressed | **NONE** | No notification bus. Streaming output goes to `Console()` or an `asyncio.Queue`; no transport-gating, no human-escalation filter. |
| 16 | session_id recover-from-context-compaction + SALVAGE_RESUME guard stripping origin ids | **PARTIAL** | `SingleThreadedAgentRuntime` runs **concurrently** with you saving state mid-flight via `pause()`/`save_state()`; `MagenticOneOrchestratorState` carries task/facts/plan/n_rounds/n_stalls/thread — enough to resume after compaction *if you persist* — but no explicit compaction-recovery/origin-id-stripping sage; resume is caller-driven, not auto-on-timeout. |
| 17 | idempotency keys, goal_mode (judge loops until goal met), tenants/boards (multi-project) | **PARTIAL** | State is pydantic `Mapping`; no transactional lock. Experimental `Task-Centric Memory` (`autogen_ext/experimental/`) stores/retrieves "insights" by task similarity — adjacent to goal-mode recall but not a judge-until-goal-met mode. No idempotency keys, no tenant/board scoping. |
| 18 | max_retries caps (3), MAX_FIX_ITERS review-iteration ceiling | **FULL (diff names)** | `MagenticOneGroupChat`: `max_turns=20` (hard turn ceiling), `max_stalls=3` (replan-trigger ≈ MAX_FIX_ITERS), `_max_json_retries=10`. AssistantAgent: `max_tool_iterations` per agent. SelectorGroupChat: `max_selector_attempts=3`. Different semantics but the cap primitives exist. |

---

## What AutoGen has that we don't

- **Built-in heterogeneous speaker-selection routers** (`SelectorGroupChat`, `SwarmGroupChat`, `DiGraphGroupChat`, `RoundRobinGroupChat`) — first-class routing policies with composability; we hand-roll routing via our worker planner.
- **`Task-Centric Memory` (experimental)** — semantic memo store keyed on task similarity ("avoid repeating mistakes","recall demonstrations") cưon a cross-task knowledge layer we lack.
- **Composable termination DSL** (`conditions/_terminations.py`): `MaxMessageTermination | TextMentionTermination & TimeoutTermination` etc. — our exit conditions are imperative Python, not declaratively composable.
- **First-class streaming/`run_stream` + `pause`/`resume`** of in-flight runs (cooperative asyncio flags) — we have cold-process restart resilience but not live pause/stream of a long worker.
- **OpenTelemetry telemetry hooks** plus a structured-composable `Component` config (`ComponentBase[BaseModel]`) for declarative team serialization/registry — our stack is config-file + SQLite, not a framework-level component model.
- **Microsoft Agent Framework (MAF) successor** with A2A/MCP interoperability and multi-provider routers — the actively supported path forward; we'd want to at least track it.

## What we have that AutoGen lacks

- **Durable, cross-process, cross-LLM-model kanban** (features 1, 2): shared SQLite + separate cold processes per role — AutoGen's durable resume requires caller-owned persistence and runs inside one process.
- **Hard process kill + salvage packs + re-spawn-to-ready** (features 5, 6, 10, 11): `max_runtime` SIGTERM→grace→SIGKILL, salvage packs, process-logger snapshots, hygiene cron. AutoGen only has cooperative cancellation.
- **Staging-then-integration with a single INT writer + parallel workers on private staging dirs** (features 4, 14) — no equivalent; all AutoGen participants read-write one shared thread.
- **Mechanical exit safeguards grounded in artifacts, not prompt-judgement** (features 8, 9, 13): plan-lint counts/ownership, failure-signature+zero-delta block, evidence contract. AutoGen's stall/loop guard is an LLM answer to "are we in a loop?" — no signature/delta enforcement.
- **Operational ergonomics for the dogfooding environment** (features 12, 15, 17): git-worktree-per-task, Telegram-gated notifications, idempotency keys/tenants/boards/goal-mode — no autogen primitives, and AutoGen is now in maintenance mode so gaps won't close.

---

## Verdict

**Worth studying, not migrating.** AutoGen's MagenticOne ledger (facts/plan/progress/stall-replan) and composable termination DSL are good design references for our review-loop prompts and exit conditions — but it's *maintenance mode* with no durable store, no parallel-staging integration, no hard process control, and no operational layer; nothing we'd pull in wholesale. Borrow the MagenticOne progress-ledger JSON schema (`is_request_satisfied / is_in_loop / is_progress_being_made / next_speaker / instruction`) as a richer quality-gate signal for our reviewer prompt, and watch Microsoft Agent Framework for A2A/MCP primitives worth porting later.