# W3 Executor Lane Evidence (`t_a1e6e05d`)

## Outcome

SUCCESS — staged transactional idempotent Kanban action executor for already-authorized `decision_plan_v1` actions. Mutates only the named existing card, exactly once, with optimistic CAS guards. Does not discover/classify policy and never creates cards.

Commit SHA: `none` (do not commit per card contract).

## Touched files (exclusive ownership)

| Path | SHA-256 | Bytes |
|---|---|---|
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/executor/executor.py` | `80131648cf3ad312cad19d0ab0ce3cf35af30661f14bd6c6e557257b1afd093d` | 41968 |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/executor/test_executor.py` | `f36fbbed325a225bbaae75006ec94b48bf857c2033aa7a4e6ad9cef3da8f7b63` | 26022 |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/executor/EXECUTOR_CONTRACT.json` | `653222bd25d77c1eb03a59fc88021770b04d5e2bba7b9328b51321af21fe1087` | 4905 |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/executor/EVIDENCE.md` | (this file) | — |

No other lanes, final runtime, `/etc`, live DBs (writable), or unrelated dirty tree paths were modified.

## Public symbols

- `validate_action(action, board_snapshot_digest) -> dict`
- `apply_board_actions(db_path: Path, actions: list[dict], *, dry_run: bool, now_ns: int) -> dict`
- `action_already_applied(connection, action_key) -> bool`

Supported mutations only: `comment_once`, `unblock_same_card`, `route_needs_replan`.

## Lifecycle schema adapter facts (frozen)

Observed from installed `hermes_cli.kanban_db` (`/usr/local/lib/hermes-agent`):

- `VALID_STATUSES` = `archived, blocked, done, ready, review, running, scheduled, todo, triage`
- `VALID_BLOCK_KINDS` = `capability, dependency, needs_input, transient`
- **No native `needs_replan` status** and **no native `needs_replan` block_kind**
  - `NEEDS_REPLAN_STATUS_SUPPORTED = false`
  - `NEEDS_REPLAN_BLOCK_KIND_SUPPORTED = false`
  - `route_needs_replan` **downgrades to `comment_once` diagnostic**; status is not guessed
- Unblock mirrors `unblock_task`:
  - source: `blocked`
  - target: `ready` if all parents `done`/absent, else `todo`
  - preserves `block_kind`, `block_recurrences`, edges, run history, siblings, assignee
  - resets `current_run_id`, `consecutive_failures`, `last_failure_error`
  - closes dangling open run as `reclaimed`
  - emits native `unblocked` event + audit `anti_stall_action` event + audit comment
- Idempotency store: append-only `task_events` / `task_comments` via marker `anti_stall_action_key:<key>`
- Txn: `BEGIN IMMEDIATE` per action, `busy_timeout_ms=5000`, per-action rollback
- Dry-run: SQLite `mode=ro`; byte-for-byte DB unchanged
- Writable open of live boards under `$HOME/.hermes/**/kanban.db` refused (`deny_class`/`production`)

Full freeze: `EXECUTOR_CONTRACT.json`.

## Checks run

```bash
/root/main/venv/bin/python -m pytest -q \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/executor/test_executor.py
```

**Result:** `37 passed in 6.27s` (repeatable; prior green `37 passed in 3.13s`).

Coverage includes:

- exact same-row unblock → `ready` (parents done) and → `todo` (open parent)
- deny classes: ambiguous/needs_input/human/secret/production/ownership/auth/reviewer/checksum (no mutation)
- needs_input without exact directive denied
- secret-shaped reason denied
- duplicate `action_key` → `already_applied`, no second comment/event
- `route_needs_replan` downgrade preserves siblings/history/edges/status
- guard-race status / event watermark / run_id → rollback, no audit write
- dry-run planned + byte-identical DB
- cooldown skip
- corrupt DB / schema mismatch
- locked DB BEGIN failure isolation
- refuse live writable path; dry-run still RO-openable
- no card duplication; batch continues after denied sibling action
- comment_once preserves status/pid/failure counters
- auth kind gate; empty batch ok; registered_policy unblock

## Fixture before/after (manual evidence probe)

Temporary Hermes-lifecycle fixture (not a live board):

| Metric | Before unblock | After unblock | After duplicate |
|---|---|---|---|
| tasks | 3 | 3 | 3 |
| task_links | 2 | 2 | 2 |
| task_runs | 1 | 1 | 1 |
| task_events | 1 | 3 | 3 |
| task_comments | 0 | 1 | 1 |
| DB sha256 | `475b29507f66927da4d91a4a465670104e7bc0b4ba2e7bf647730bf74504e367` | `53f64ba741cafffd31be452d6225bbc078f823fd51d98390dee0fad8be5768a0` | same as after (unchanged) |

- Applied: `unblock_same_card` → `new_status=ready`, mutations `[unblock, comment, unblocked, anti_stall_action]`
- Duplicate: `already_applied=1`, hash unchanged
- Subsequent dry-run on post-unblock DB: hash unchanged
- Live board `/root/.hermes/kanban/boards/telemt-proxy/kanban.db`:
  - writable apply refused (`ok=false`, `deny_class`)
  - live hash unchanged: `a7820556b1836591ae8d84824152e99ecbf98dd8cdb2249ae9f0fdf6cac302e1`

## Re-check

```bash
# unit suite
/root/main/venv/bin/python -m pytest -q \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/executor/test_executor.py
# expect: 37 passed

# lane file hashes
sha256sum \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/executor/executor.py \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/executor/test_executor.py \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/executor/EXECUTOR_CONTRACT.json

# confirm live board still not writable via executor
/root/main/venv/bin/python - <<'PY'
from pathlib import Path
import sys
sys.path.insert(0,'/root/main/runtime/anti_stall_staging/t_3c0b64e6/executor')
import executor as ex
r=ex.apply_board_actions(Path('/root/.hermes/kanban/boards/telemt-proxy/kanban.db'),
  [ex.make_action(action_key='ak-live-probe', action_type='comment_once',
    task_id='t_nope', board_snapshot_digest='x')], dry_run=False, now_ns=1)
assert r['ok'] is False and r['errors'][0]['code']=='deny_class'
print('live_refuse_ok')
PY
```

## Notes for INT

- Import surface is lane-local module `executor.py` (not installed package).
- Policy lane must emit `decision_plan_v1` actions with fields listed in `EXECUTOR_CONTRACT.json`.
- Because lifecycle has no `needs_replan`, circuit routing is comment-diagnostic only until/unless core gains representation; INT must not invent status.
