# EVIDENCE — W2 decision lane (`t_0e0ff6f2`)

**UTC:** 2026-07-19T10:18:58Z  
**Card:** `t_0e0ff6f2` — Stage deterministic fail-closed stall decision policy  
**Mode:** pure fixtures only; no live DB / SQLite / process mutation  
**Commit SHA:** none (not committed)

## Files touched (exclusive ownership)

| Path | SHA-256 |
|---|---|
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/decision/decision.py` | `5368ed6220a9101e88f2ee09bcd38e4875b19bedc460dc69f4912a4c044923f6` |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/decision/policy.json` | `69ae1284ac158d1654643d71415e5ab641f40b0be171c22fcaa995e258b1b3bf` |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/decision/test_decision.py` | `59a2cef4d90d0de7cde72b103b87e4fa8b2413b681cd054a65c16373abf5d67b` |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/decision/DECISION_SCHEMA.json` | `c8a708120bae6b3f7902a1524af0cbd8a27db8428b3326acf9b661a561f80691` |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/decision/EVIDENCE.md` | (this file) |

`git status --short` (lane only): `?? runtime/anti_stall_staging/t_3c0b64e6/decision/`

## Public symbols

- `normalize_signature(value) -> str`
- `validate_resolution_directive(task, snapshot, policy) -> dict`
- `plan_tick(snapshot, prior_state, policy, *, now_ns) -> dict` → `decision_plan_v1`

Frozen schema name: `decision_plan_v1` in `DECISION_SCHEMA.json`.  
Default policy: `anti_stall_policy_v1` version `1` in `policy.json`.

## Check command + outcome

```bash
/root/main/venv/bin/python -m pytest -q /root/main/runtime/anti_stall_staging/t_3c0b64e6/decision/test_decision.py
```

**Result (exact):**

```
..........................................                               [100%]
42 passed in 0.18s
```

Purity check: module source contains no `sqlite3` / `subprocess`; import succeeds with symbols present (`pure_module_ok`).

## Case matrix (42)

| Case | Test | Expected |
|---|---|---|
| signature normalize | `test_normalize_signature_stable` | stable lowercase token |
| corrupt prior state | `test_corrupt_prior_state_fail_closed` | fail_closed, no actions |
| clock reversal | `test_clock_reversal_fail_closed` | fail_closed |
| unknown policy schema | `test_unknown_policy_schema_fail_closed` | fail_closed |
| invalid snapshot | `test_invalid_snapshot_fail_closed` | fail_closed |
| unknown snapshot schema | `test_unknown_snapshot_schema_fail_closed` | fail_closed |
| exact directive unblock | `test_exact_resolution_allowed` | `unblock_same_card` once; stable action_key |
| missing artifacts suppress status | `test_exact_resolution_suppressed_without_artifacts_declaration` | no unblock |
| directive hash mismatch | `test_directive_hash_mismatch_invalid` | invalid / no unblock |
| ambiguous remains | `test_ambiguous_remains_no_unblock` | `blocked_ambiguous` |
| permanent deny ×11 | `test_permanent_deny_never_unblocks[...]` | never unblock |
| needs_input status | `test_needs_input_status_never_unblocks` | never unblock |
| dependency rule allow | `test_dependency_all_parents_success_unblocks` | unblock via rule |
| dependency parent not success | `test_dependency_rule_fails_if_parent_not_success` | no unblock |
| descendant stalled | `test_dependency_propagation_descendant_stalled` | classification only |
| action key + cooldown | `test_action_key_idempotent_and_cooldown` | second tick suppressed |
| same-sig no-delta breaker | `test_same_signature_no_delta_breaker` | comment_once + route_needs_replan |
| protocol attempt 1 | `test_protocol_first_attempt_comment_only` | comment only |
| protocol attempt 2 breaker | `test_protocol_second_attempt_circuit_breaker` | comment + route; no 3rd attempt class |
| protocol high attempt_count | `test_protocol_third_not_emitted_still_breaker_only` | breaker only, unique keys |
| healthy delta noop | `test_healthy_running_with_delta_noop` | noop, no actions |
| dead PID window | `test_dead_pid_requires_two_snapshots` | route only after 2 snapshots |
| stale heartbeat window | `test_stale_heartbeat_window` | route after window |
| fresh HB no-progress | `test_fresh_heartbeat_no_progress_needs_three_obs_and_900s` | needs ≥3 obs + 900s |
| planner ceiling | `test_planner_ceiling_comment_once_no_complete` | one comment; never complete planner |
| missing artifacts dead | `test_missing_artifacts_no_status_action_on_dead_window` | comment ok; no route |
| directive unit + source mismatch | `test_validate_resolution_directive_ok_and_source_mismatch` | valid / invalid |
| deterministic sort | `test_actions_sorted_deterministically` | sorted actions/classifications |
| done noop | `test_done_task_noop` | no actions |
| null prior state | `test_null_prior_state_ok` | ok |
| policy digest | `test_policy_digest_in_plan` | digest matches |
| no live DB imports | `test_no_live_db_imports` | no sqlite3/subprocess |

## Invariants enforced

- No status action without `artifacts_declared=true` and owned artifact paths+sha256.
- Permanent denylist precedes directives/rules.
- Auto-unblock only: exact `ANTI_STALL_RESOLUTION_V1` directive **or** `dependency_all_parents_success_v1`.
- Dead/stale require unchanged progress across `min_stall_snapshots` (default 2).
- Fresh-heartbeat no-progress: `max_no_progress_seconds` (default 900) and ≥3 observations.
- Protocol: max 2 attempts; second identical → circuit-breaker route; never reclaim/create cards.
- Same normalized signature + zero artifact/event delta → comment_once + route_needs_replan only.
- Planner with executable children → idempotent `comment_once` only.
- Corrupt/unknown schema / clock reversal → fail closed, empty actions.
- Never opens live Hermes DBs.

## No live DB access

- Tests use in-memory dict fixtures only.
- `decision.py` does not import `sqlite3` or touch board paths.
- No writes outside the five owned lane files.

## Re-check

```bash
/root/main/venv/bin/python -m pytest -q /root/main/runtime/anti_stall_staging/t_3c0b64e6/decision/test_decision.py
sha256sum /root/main/runtime/anti_stall_staging/t_3c0b64e6/decision/{decision.py,policy.json,test_decision.py,DECISION_SCHEMA.json}
```

Expected: `42 passed`; hashes match table above (EVIDENCE.md hash excluded from freeze table until INT re-hashes full tree).
