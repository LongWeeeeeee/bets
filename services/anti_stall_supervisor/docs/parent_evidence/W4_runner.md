# W4 Evidence — single-lock five-minute hygiene tick runner

Task: `t_e048eaa5`
Lane: `/root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/`
Generated (UTC): 2026-07-19T10:15:24Z
Mode: staging only — no live board writes, no `/etc` changes, no final/ deploy.

## Observed existing-linter facts (read-only)

| Fact | Value |
|------|-------|
| Path | `/root/main/runtime/kanban_plan_lint.py` |
| sha256 | `e7e87a9ed5d39e293d099540e8b4d700eb9df0b68d98cb97d1b847806c911a8c` |
| Size / mtime | 24472 bytes; Jul 18 15:46 |
| Own lock? | **No** — `rg fcntl\|flock\|lock_path\|\.lock` → no matches |
| Entrypoint | `python kanban_plan_lint.py [--dry-run] [--boards SLUGS] [--no-blocks] [--apply-blocks]` |
| `main()` rc | `0` if critical==0 else `2` (findings still produced) |
| Sibling state/report/log | `kanban_plan_lint_state.json`, `kanban_plan_lint_last.json`, `kanban_plan_lint.log` under `/root/main/runtime/` |
| Nested from hygiene | `kanban_hygiene_guard.py` subprocess-calls linter with timeout=120; **no process lock** there either |
| Cron today | `/etc/cron.d/kanban-hygiene-guard` → `*/5` hygiene_guard (not modified) |

**Frozen outer lock (this lane):** `/root/main/runtime/anti_stall_supervisor_var/hygiene.lock`
(created on demand under test var dirs only; live path not created by this Worker).

## Required symbols implemented

- `acquire_tick_lock(lock_path) -> TickLock` — nonblocking `fcntl.LOCK_EX|LOCK_NB`
- `run_plan_lint(config) -> dict` — one subprocess (or injected `linter_runner`), timeout hard-capped ≤60s, bounded stdout/stderr
- `atomic_write_json(path, value)` — `<target>.tmp` + fsync + `os.replace`
- `run_tick(config, *, adapters, clock) -> int`
- CLI: `--config`, `--dry-run`, `--self-test` (+ `--var-dir`, `--lock-path`, `--print-success`)

## Behavior contract coverage

| Requirement | Proof |
|-------------|--------|
| One nonblocking lock covers snapshot/decision/execute/state/report/audit + linter | `test_one_lock_serialization`, `test_acquire_tick_lock_and_contention` |
| Lock contention → rc=0, no state mutation, one report decision, quiet | `test_lock_contention_no_state_mutation`, `test_quiet_stdout_stderr` |
| Chain existing linter under same outer lock; exactly one call | `test_exact_one_linter_call` |
| Linter timeout/failure → fail closed, no supervisor actions | `test_linter_timeout_fail_closed`, `test_run_plan_lint_real_timeout` |
| Tick deadline ≤240s fail closed | `test_deadline_fail_closed` (injected clock) |
| Dry-run: no cooldown/action_keys advance | `test_dry_run_immutability` |
| Corrupt state renamed aside; fail closed; no actions | `test_corrupt_state_recovery` |
| Atomic state/report after complete tick | `test_atomic_state_and_report` |
| Audit JSONL redaction + idempotent append; rotate via rename (no delete) | `test_audit_redaction_and_idempotency`, `test_audit_rotation_no_delete` |
| Quiet stdout/stderr routine ticks | `test_quiet_stdout_stderr`, CLI dry-run |
| Adapter failure isolation | `test_adapter_failure_isolation` |
| Happy path advances action state when not dry-run | `test_happy_path_action_state_advance` |

## Commands + output

```text
$ /root/main/venv/bin/python -m pytest -q \
    /root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/test_runner.py
..................                                                       [100%]
18 passed in 2.48s

$ /root/main/venv/bin/python \
    /root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/runner.py --self-test
SELF_TEST_OK
```

(First pytest run in session: 18 passed in 10.61s; re-check 2.48s.)

## Lane file hashes

```text
ae28522692e7fea40a84c76360ed519d05f9f98a77c9386917a12876ed95d7e8  runner.py
723f9fb8a4afaf1ccbd251e7cc81400ce8f3705c9c16c9bffa1110cf43147687  test_runner.py
62f3014ccd3a0a1f374585025086a31233376091169fbf4aef2369ab1344a659  REPORT_SCHEMA.json
```

## Touched files (exclusive ownership only)

- `/root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/runner.py`
- `/root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/test_runner.py`
- `/root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/REPORT_SCHEMA.json`
- `/root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/EVIDENCE.md`

Not touched: live DBs, `kanban_plan_lint.py`, other staging lanes, `final/`, `/etc`, systemd, Hermes configs.

## Live side effects

- No live board write.
- No `/root/main/runtime/anti_stall_supervisor_var` created on host during tests (temp dirs only).
- No Telegram / dispatcher / daemon / LLM.

## Re-check

```bash
/root/main/venv/bin/python -m pytest -q \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/test_runner.py
# expected: 18 passed

sha256sum \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/runner.py \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/test_runner.py \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/runner/REPORT_SCHEMA.json
```

## INT wiring notes

Adapters injected via `run_tick(..., adapters={snapshot, decide, execute})`.
Default no-op adapters keep self-test/CLI dry structure safe.
Config keys: `lock_path`, `state_path`, `report_path`, `audit_path`, `linter_path`,
`linter_timeout_s` (≤60), `tick_deadline_s` (≤240), `dry_run`, `linter_runner` (tests).
