# RuFlo runtime and anti-stall migration (legacy / rollback)

> С 2026-07-23 целевой Telegram developer runtime описан в
> `docs/OPENHANDS_RUNTIME.md`. Этот документ остаётся источником правды для
> работающего legacy RuFlo service и rollback до завершения controlled cutover.

> Scribe sync: 2026-07-22 against repository baseline `87049d4`. The universal
> gateway source and deployed runtime contain uncommitted changes, so that commit
> identifies the baseline, not the complete production contents.

## Scope

RuFlo is the durable coordination and learning substrate around the existing Hermes
kanban workflow. It does **not** redesign roles, model routing, assignees, Reviewer
gates, or production-safety rules in `AGENTS.md`.

Enabled project plugins:

- `ruflo-swarm@ruflo`
- `ruflo-goals@ruflo`
- `ruflo-intelligence@ruflo`
- `ruflo-autopilot@ruflo`
- `ruflo-agentdb@ruflo`

The project MCP endpoint is `/usr/local/bin/ruflo mcp start` (`.mcp.json`).

## Canonical configuration

| File | Purpose |
|---|---|
| `claude-flow.config.json` | Current CLI config API (config/memory/MCP tools); mirrored from YAML |
| `.claude-flow/config.yaml` | Canonical V3 runtime/daemon policy; kept value-equivalent to JSON |
| `.claude/settings.json` | Claude Code project plugin enablement |
| `.mcp.json` | Project-scoped RuFlo MCP server |
| `.swarm/memory.db` | Durable AgentDB/sql.js state; host runtime data, git-ignored |
| `.claude-flow/migration/anti-stall-v1.json` | Content-hash manifest for the legacy contract import |

Defaults: hierarchical-mesh, at most 15 agents, specialized coordination, AgentDB with
HNSW memory, and local-only daemon workers. Autopilot is **disabled by default** and
bounded to 3 iterations / 30 minutes when explicitly enabled.

> Ruflo 3.32.8 currently has a split config-reader issue: `ruflo config`/memory tools
> read the JSON file while the V3 runtime/daemon reads YAML. Therefore both files are
> intentionally present and must remain value-equivalent; `doctor -c config` reports a
> known collision warning until upstream unifies the readers.

## Mapping from the retired custom runtime

The custom `hermes-anti-stall-supervisor.timer` is retired.

| Legacy responsibility | Replacement |
|---|---|
| repeated no-progress evidence | AgentDB durable contract + intelligence outcomes |
| retry/circuit-breaker ceilings | `resilience.maxAttempts=3`, bounded autopilot, `AGENTS.md` signature rules |
| dependency and ownership safety | Hermes kanban + `AGENTS.md` staging/INT/Reviewer gates |
| policy and schema persistence | AgentDB namespace `ingame-orchestration` |
| periodic learning/consolidation | RuFlo local-only `consolidate` worker |
| transient tick/PID/cooldown state | deliberately not migrated; stale execution state must not resume |

Source policy/contracts remain in `runtime/anti_stall_supervisor/` as audit evidence.
Runtime reports/logs remain in `runtime/anti_stall_supervisor_var/`; neither is deleted.

## Re-run and verify migration

```bash
python3 scripts/ruflo_migrate_anti_stall.py
python3 scripts/ruflo_migrate_anti_stall.py --verify-only
ruflo mcp exec -t memory_search -p '{"query":"anti stall handoff","namespace":"ingame-orchestration","limit":10}'
ruflo mcp exec -t agentdb_health -p '{}'
```

The import is idempotent (`upsert=true`). It stores six entries: policy, four contracts,
and a concise RuFlo handoff. The manifest records source and stored content hashes.
Audit logs, live PIDs, cooldowns, and in-flight task state are intentionally excluded.

## Operations

```bash
claude plugin list
claude mcp get claude-flow
ruflo mcp tools
ruflo daemon status
ruflo autopilot status
ruflo hooks intelligence --status
ruflo mcp exec -t agentdb_health -p '{}'
```

Never enable the retired unit again:

```bash
systemctl is-enabled hermes-anti-stall-supervisor.timer  # disabled
systemctl is-active hermes-anti-stall-supervisor.timer   # inactive
```

Rollback of the RuFlo cutover means disabling autopilot and stopping the RuFlo daemon;
it does not restore the custom anti-stall timer without a separate human decision.

## Universal Telegram gateway

Production uses one `ruflo-universal-gateway.service` for all three Telegram bots.
The Telegram polling/API process is shared, but orchestration state is physically
split by bot slot:

- `tenant_key = <telegram_bot_id>:<chat_id>` scopes conversations, runs, active-run
  ownership and commands inside the bot's ledger;
- ledgers and attachments live below
  `/root/.local/state/ruflo-universal-gateway/orchestration{1,2,3}/`;
- RuFlo roots, AgentDB/memory, agent registries, compatibility `swarm-tasks.json`
  boards and MCP task stores live below
  `runtime/ruflo-orchestrators/orchestration{1,2,3}/`;
- `ruflo-orchestrator@orchestration{1,2,3}.service` runs one daemon per root;
- one tenant cannot resume, stop or inspect another tenant's active run through the
  gateway commands.

The former shared ledger/root are retained as migration/audit sources and are not
deleted. On startup each per-bot ledger idempotently imports only rows matching its
Telegram bot id.

`runtime/setup_ruflo_isolated_roots.py` creates missing AgentDB files through a
verified temporary database, materializes the complete SQLite/WAL state into a
standalone replacement, and atomically renames it into place; it never replaces an
existing valid database. `--repair-invalid` first snapshots an invalid database into
`.swarm/backups/`, then atomically replaces it. It also creates a separate official
RuFlo MCP task store at `.claude-flow/tasks/store.json` for every bot root.
`--check-only` validates config parity, `0600` board/task-store/AgentDB modes,
required AgentDB tables and distinct physical task-store/database inodes for all slots.

The workflow is `universal-fusion-v1` version 7. Before broad research, a Fable 5
resume commander examines prior terminal runs in the same bot, topic and conversation.
It may choose `fresh_research`, `resume_research`, `resume_planning`,
`resume_execution` or `resume_validation`. A valid continuation can reuse the prior
compiled research needs, evidence pack, master plan, validated DAG and explicitly
approved successful worker results. Only missing tasks and Fable-requested supplemental
research are executed; the three research planners and three solution planners are
skipped when their existing outputs remain safe. The canonical editable source is
`runtime/ruflo-universal-gateway/`; the service executes the deployment copy at
`/opt/ruflo-universal-gateway/`. Do not edit `/opt` as the source of truth.

Every run now records `conversation_id`, optional `parent_run_key` and `resume_mode` in
its per-bot ledger. Insert-only `resume-assessment` and `resume-checkpoint` artifacts
preserve the reconstruction decision and completed task IDs. Legacy ledgers are migrated
additively on startup without deleting runs, events, artifacts or model attempts.

Telegram topics have independent conversation identities. `/new` rotates the current
topic's conversation ID, so later messages cannot resume an older chain in that topic.
No-topic chats keep the same behavior using their chat conversation ID. Resume lookup is
always restricted to the current per-bot ledger, tenant, topic and conversation; it cannot
read another `orchestration1..3` bot's progress.

After a validated DAG is available, unfinished tasks are created through RuFlo's official
`task_create` MCP tool in that bot's `.claude-flow/tasks/store.json`. Worker start,
success and failure are reflected with `task_update` and `task_complete`. Task tags and
descriptions carry tenant, conversation, run and logical task IDs. Completed inherited
tasks are not recreated or rerun. The compatibility `swarm-tasks.json` file is preserved
and isolated but is not written manually because Ruflo 3.5.16's registered task tools use
the MCP task store.

### Model matrix and fallback policy

| Profile / role | Model | Allowed use |
|---|---|---|
| thinker A | `codex/gpt-5.6-sol-xhigh` / `xhigh` | independent research/solution planner |
| thinker B | `opencode-go/kimi-k3` / `max` | independent research/solution planner |
| thinker C | `fable/claude-fable-5` / `high` | independent research/solution planner |
| Resume Commander / Judge / DAG / Recovery / Final | `fable/claude-fable-5` / `high` | fallback: GPT-5.6 Sol `xhigh`, then Kimi K3 `max` |
| Reviewer / Validator | `fable/claude-fable-5` / `high` | fallback: GPT-5.6 Sol `xhigh`, then Kimi K3 `max` |
| execution / read-only verification worker | `Qwen3.8-Max-Preview` | execution fan-out and independent tool verification only |
| auxiliary | `oc/deepseek-v4-flash-free` | separate auxiliary/context-compression work only |

Thinker calls are fail-soft and have `limits.thinkerTimeoutSeconds = 2400`: an unavailable
planner contributes no plan, while the remaining planners continue. They never switch to
another thinker model. A fast connection/transport failure on the same route may be retried
up to 10 times with geometric delays
`[1, 2, 4, 8, 16, 32, 64, 128, 256, 512]` seconds. Only failures returned within
`modelRetryMaxFailureSeconds = 15` are eligible; this includes the 10-second connect timeout
plus local scheduling overhead. A model call that remains alive longer is
treated as a potentially valid reasoning response and is not duplicated. `timeout` and
`no_output` are not retried. If all
solution planners are unavailable, the controller emits a bounded deterministic fallback
plan. Commander and Reviewer roles use the explicit ordered fallback above. Qwen remains
worker-only and DeepSeek remains auxiliary-only.

Worker routing is fail-closed: Qoder is invoked with the exact
`Qwen3.8-Max-Preview` model and `--max-model-request-retries 10`. These retries apply
inside the current Qoder worker session to individual model requests, so completed tool
work and conversational context are preserved. A successful result
must report only Qwen's `qmodel_preview` usage key. Missing model-usage evidence,
another model key, an upstream error or timeout stops the run with an explicit
`worker-unavailable` Telegram message. No worker fallback to Kimi, Fable, GPT or
another Qoder model is permitted.

Qoder owns the 10 retries of an individual streaming model request. The controller also
owns process-level same-model retries for fast terminal `429`, `502`, `503`, fetch or
connection failures returned within `modelRetryMaxFailureSeconds = 15`; those use the
geometric schedule `[1, 2, 4, 8, 16, 32, 64, 128, 256, 512]`. A live Qoder agent that
reaches `workerAttemptTimeoutSeconds = 1800`, missing output, route mismatch or another
non-transport model failure is not restarted from the beginning. `workerTimeoutSeconds =
3000` bounds the full logical worker call, including retry sleeps and one long established
Qwen attempt; the total run remains bounded by `runTimeoutSeconds = 3600`.
An attempt that stays active until its limit is reported as `worker_execution_timeout`,
not as an upstream outage; the Telegram message also states whether same-model retry ran.
If Qoder exhausts all 10 request retries because an HTTP 200 SSE stream produces no first
payload, the category is `worker_first_payload_timeout` and the bot reports the exhausted
internal retry count rather than the generic `worker_model_error`.

Any non-command Telegram message in the active run's topic is accepted. If the workflow
is awaiting a question it becomes the clarification answer; otherwise it is stored as
live guidance and is injected into subsequent model calls, phase objectives, attachments
and final synthesis. The whole run is bounded by `limits.runTimeoutSeconds = 3600`,
including waits for model capacity and subprocesses.

When no run is active, a message such as `Продолжи` creates a new run in the same
conversation but does not automatically restart the full workflow. Fable receives a
compact reconstruction of recent prior run phases, errors, evidence, plans, DAG, worker
statuses and validation. It must explicitly select reusable artifact references and
successful task IDs. If the selected state is incomplete or internally inconsistent, the
controller automatically falls back to the earliest safe phase instead of blindly using it.

Commander/Reviewer role budget is `limits.modelTimeoutSeconds = 3600`; thinker role budget
is 2400 seconds. An individual established thinker/Commander/Reviewer request may run for up to
`modelAttemptTimeoutSeconds = 900`, and the local JSON proxy uses the same 900-second cap.
OmniRoute is started with `FETCH_CONNECT_TIMEOUT_MS = 10000`, separating a dead connection
from a slow model: TCP/TLS establishment fails in about 10 seconds, while an established
high-reasoning request may continue for 15 minutes. Retry sleeps and attempts remain inside
the role deadline and the total `runTimeoutSeconds = 3600` budget. The Qwen-only worker uses
`workerAttemptTimeoutSeconds = 1800` and `workerTimeoutSeconds = 3000`, preserving its
same-model-only/no-fallback contract while allowing long tool-heavy work to finish.
For each attempt the controller creates an opaque, single-use bearer token bound to
the exact original bearer credential and deadline. The proxy consumes that token,
restores only its matching credential, and rejects expired, cancelled, evicted,
unknown or replayed tokens locally. Never print, persist, paste or correlate using raw
API keys or bearer tokens.

### Observability and failure data

`model_attempts` stores one row per attempted Ruflo or Qoder model call with:
`run_key`, `phase`, `role`, `agent_id`, `model`, `attempt`, `started_at`,
`finished_at`, `duration_ms`, `timeout_ms`, `prompt_chars`, `output_chars`, `status`,
`error_category`, `error_message`, `http_status`, `correlation_id` and
`fallback_used`. `attempt` is the sequential controller subprocess/model call number within
a logical role or Qoder worker call; Qoder's internal model-request retries stay inside that
row and are reported in the terminal event/message when exhausted. A retry of the same
primary model keeps `fallback_used = 0`, while calls to GPT/Kimi fallback routes set it to 1.
Qoder rows use agent IDs such as
`qoder-research-R4`, `qoder-worker-T1` and `qoder-verifier`.
Error text is intentionally generic; prompts, provider bodies,
credentials and raw bearer tokens do not belong in this table or normal logs.
`correlation_id` contains only a safe SHA-256 fingerprint, not the provider token or
credential itself.

Useful read-only inspection commands:

```bash
systemctl status ruflo-universal-gateway.service --no-pager
journalctl -u ruflo-universal-gateway.service -n 200 --no-pager -o short-iso
sqlite3 /root/.local/state/ruflo-universal-gateway/orchestration2/ledger.sqlite3 '.tables'
sqlite3 -header -column /root/.local/state/ruflo-universal-gateway/orchestration2/ledger.sqlite3 \
  'SELECT phase,role,model,attempt,duration_ms,timeout_ms,status,error_category,http_status,correlation_id,fallback_used FROM model_attempts ORDER BY id DESC LIMIT 30;'
tail -n 200 /root/.omniroute/logs/application/app.log
find /root/.omniroute/call_logs -type f -printf '%T@ %p\n' | sort -nr | head
```

Treat raw OmniRoute call logs as confidential: inspect locally and do not copy request
headers/bodies into tickets or documentation.

Operational note for the July 22, 2026 incident: the previous 285-second local proxy cap
could disconnect an upstream model that was still reasoning. The current split timeout
allows 900 seconds for an established response but only 10 seconds for connection setup;
fast transport failures receive the 10-step geometric retry schedule. This bounds and
classifies failures but does not prove that upstream capacity is resolved.

### Atomic deployment and activation

Validate the source first, then copy each validated file through a temporary path and
atomically rename it over the deployment copy. Preserve backups; never delete the
deployment directory to rebuild it.

```bash
cd /root/main
/usr/bin/python3 runtime/setup_ruflo_isolated_roots.py
/usr/bin/python3 runtime/setup_ruflo_isolated_roots.py --check-only
src=runtime/ruflo-universal-gateway
dst=/opt/ruflo-universal-gateway
for file in core.py gateway.py universal-fusion-v1.json claude-flow.config.json test_universal_controller.py; do
  install -m 0600 "$src/$file" "$dst/$file.tmp"
  mv -f "$dst/$file.tmp" "$dst/$file"
done
install -m 0644 runtime/systemd/ruflo-orchestrator@.service /etc/systemd/system/ruflo-orchestrator@.service.tmp
mv -f /etc/systemd/system/ruflo-orchestrator@.service.tmp /etc/systemd/system/ruflo-orchestrator@.service
install -m 0644 runtime/systemd/ruflo-universal-gateway.service /etc/systemd/system/ruflo-universal-gateway.service.tmp
mv -f /etc/systemd/system/ruflo-universal-gateway.service.tmp /etc/systemd/system/ruflo-universal-gateway.service
systemctl daemon-reload
systemctl enable --now ruflo-orchestrator@orchestration1.service \
  ruflo-orchestrator@orchestration2.service ruflo-orchestrator@orchestration3.service
systemctl restart ruflo-universal-gateway.service
systemctl is-active ruflo-universal-gateway.service
journalctl -u ruflo-universal-gateway.service -n 80 --no-pager -o short-iso
```

The service is healthy only after `systemctl is-active` returns `active` **and** the
journal shows `Gateway started for ...` without a following initialization failure.
Port `20229` is the loopback compatibility proxy, not an HTTP `/health` endpoint.
The deployed preflight can be run without colliding with the live proxy by assigning
an ephemeral proxy port:

```bash
BOT_1_ENV_FILE=/root/.hermes/profiles/orchestration1/.env \
BOT_2_ENV_FILE=/root/.hermes/profiles/orchestration2/.env \
RUFLO_ORCHESTRATORS_ROOT=/root/main/runtime/ruflo-orchestrators \
RUFLO_OPENROUTER_BASE_URL=http://127.0.0.1:20229 \
RUFLO_PROXY_PORT=0 \
GATEWAY_STATE_ROOT=/root/.local/state/ruflo-universal-gateway \
QODER_BIN=/root/.local/bin/qodercli \
QODER_CONFIG_DIR=/root/.omniroute/qoder-cli \
QODER_WORKSPACE=/root/main \
CREDENTIALS_DIRECTORY=/run/credentials/ruflo-universal-gateway.service \
/usr/bin/python3 /opt/ruflo-universal-gateway/gateway.py --check
```

`--check` validates the workflow/model routes, three Telegram identities/commands,
RuFlo and Qoder versions, and OmniRoute's compatibility surface. It makes external
API calls and is a preflight snapshot, not a continuous health endpoint; always pair
it with the systemd and journal checks above.

### Worker676 effort

`@worker676_bot` is sourced from `runtime/worker676-gateway/` and deployed to
`/opt/worker676-gateway/`. `MODEL_REASONING_EFFORTS` maps Fable aliases to `high`; the
runner forwards that as `model_reasoning_effort="high"` on both new and resumed Codex
turns. The canonical unit is `runtime/systemd/worker676-gateway.service`.
