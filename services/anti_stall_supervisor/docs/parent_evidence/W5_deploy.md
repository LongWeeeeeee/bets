# W5 EVIDENCE — Stage hardened systemd deployment and rollback bundle

Task: `t_23457e9d`  
Lane ownership: `/root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/` only  
Mode: STAGE ONLY — no install/enable/start/stop/edit of live systemd units  
Commit SHA: `none` (do not commit)

## Touched files (exclusive ownership)

| Path | Role |
|------|------|
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/hermes-anti-stall-supervisor.service` | hardened oneshot unit |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/hermes-anti-stall-supervisor.timer` | exact 5-minute timer |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/validate_deploy.py` | fingerprint/dry-run/unit/post-enable gates + `--self-test` |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/ROLLBACK.md` | backup/deploy/rollback commands (no delete) |
| `/root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/EVIDENCE.md` | this file |

No other paths written. `/etc/systemd/system/**` untouched. Live DBs untouched by this lane (self-test uses temp fixtures only).

## Artifact SHA-256 (five lane files)

```
9a30c454695a8f967187d404eb4515691bd5af16178069c41bddc6e41b1756d5  hermes-anti-stall-supervisor.service
83450752b6b5cc605df942cd3cf66edbc701d08128f6e9a1b1e21e90011c65c3  hermes-anti-stall-supervisor.timer
e69bc892261a4ef5ba2ac9cc6d8e4991f4a5db06f8254198aef501dc05669be1  validate_deploy.py
45b95c4347701c58a59afb699e74390c50ba6e8d407d42c7069eb95cfa84ebbf  ROLLBACK.md
```

EVIDENCE.md is self-referential (hash changes if this file is edited). Recompute:

```bash
sha256sum /root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/*
# non-self hashes above must remain stable
```

Last measured EVIDENCE.md sha256 before final freeze note: `67431947cb054d854e68205e4b3a452d03f76e4d4dd5affc851ad6ff1acaafba` (will drift if this paragraph changes).
## CHECK 1 — `systemd-analyze verify` on staged units

Command:

```bash
systemd-analyze verify \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/hermes-anti-stall-supervisor.service \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/hermes-anti-stall-supervisor.timer
```

Result: **rc=0**

Recorded unrelated dependency/host warning (not hidden):

```
/etc/systemd/system/xray.service:7: Special user nobody configured, this is not safe!
```

No parse/assignment errors for the two staged units.

## CHECK 2 — `validate_deploy.py --self-test`

Command:

```bash
/root/main/venv/bin/python \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/validate_deploy.py --self-test
```

Result: **rc=0** / `SELF_TEST_OK`

Exact output:

```
validate_units: OK {"systemd_analyze_rc": 0, "warnings": []}
capture_live_fingerprint: OK {'boards': 1, 'tasks': 2, 'sha': '230376554ae4f08ee2db167c410591978cb130fb222622659cef8e4950c95f78'}
validate_dry_run_good: OK {'ok': True, 'errors': [], 'running_task_ids': ['t_run_1'], 'status_changing_count': 0, 'diagnostic_only_count': 2, 'per_running_status_changing': {'t_run_1': 0}}
validate_dry_run_bad: OK (rejected) ['status_changing_on_preexisting:t_run_1:retry', 'status_changing_on_preexisting:t_run_1:retry', 'running_card_planned_actions_nonzero:t_run_1:2', 'running_card_planned_actions_nonzero:t_run_1:1', 'mutating_planned_actions_nonzero:2']
compare_live_fingerprint_same: OK {'ok': True, 'sha': '230376554ae4f08ee2db167c410591978cb130fb222622659cef8e4950c95f78'}
compare_live_fingerprint_changed: OK (rejected) [{'task': 't_run_1', ... 'field': 'status', 'before': 'running', 'after': 'blocked'}]
validate_post_enable_gates: OK {'ok': True}
lock_held_gate: OK (rejected) ['lock_not_acquirable:lock_held']
SELF_TEST_OK
```

Self-test uses `tempfile.TemporaryDirectory` fixture roots only — no live board writes.

### Public API coverage

| Symbol | Covered |
|--------|---------|
| `capture_live_fingerprint(hermes_root)` | yes (fixture + archived exclusion + PID/start ticks) |
| `validate_dry_run(report, baseline)` | yes (good diagnostic-only on non-running; reject retry on running) |
| `compare_live_fingerprint(before, after)` | yes (same OK; status change rejected) |
| `validate_units(paths)` | yes (staged units + systemd-analyze rc=0) |
| CLI `--self-test` | yes rc=0 |

Planner correction honored: diagnostic/comment-only on non-running is allowed; running cards require `planned_actions=0` (status-changing); overall mutating planned actions must be zero pre-enable.

## CHECK 3 — Live installed-unit fingerprint before/after (inspection only)

### BEFORE (2026-07-19T10:12:14Z)

```
is-enabled=not-found
is-active-timer=inactive
is-active-service=inactive
ABSENT /etc/systemd/system/hermes-anti-stall-supervisor.service
ABSENT /etc/systemd/system/hermes-anti-stall-supervisor.timer
```

### AFTER (2026-07-19T10:15:26Z)

```
is-enabled=not-found
is-active-timer=inactive
is-active-service=inactive
ABSENT /etc/systemd/system/hermes-anti-stall-supervisor.service
ABSENT /etc/systemd/system/hermes-anti-stall-supervisor.timer
```

**Unchanged:** timer still `not-found` / inactive; no unit files under `/etc/systemd/system/hermes-anti-stall-supervisor*`; no hashes to compare (both absent before and after). Proves this lane performed **no deployment**.

## Unit security analysis (static; no install)

Service contract verified:

| Directive | Value |
|-----------|--------|
| Type | oneshot |
| User/Group | root |
| WorkingDirectory | /root/main |
| ExecStart | `/root/main/venv/bin/python -m runtime.anti_stall_supervisor --config /root/main/runtime/anti_stall_supervisor/config.json` |
| TimeoutStartSec | 240 (<240s bound) |
| Restart | no (no restart loop) |
| NoNewPrivileges | yes |
| PrivateTmp | yes |
| PrivateDevices | yes |
| ProtectSystem | strict |
| ProtectHome | read-only |
| ReadWritePaths | `/root/.hermes/kanban/boards` `/root/main/runtime` |
| RestrictAddressFamilies | AF_UNIX only (no AF_INET/AF_INET6) |
| CapabilityBoundingSet | empty |
| AmbientCapabilities | empty |
| MemoryDenyWriteExecute | yes |
| RestrictSUIDSGID / RestrictNamespaces / RestrictRealtime | yes |
| ProtectKernelTunables/Modules/ControlGroups/Clock/Hostname | yes |
| ProtectProc / ProcSubset | invisible / pid |
| LockPersonality / SystemCallArchitectures | yes / native |
| UMask | 0077 |

Timer contract verified:

| Directive | Value |
|-----------|--------|
| OnCalendar | `*:0/5` (exact 5-minute cadence) |
| RandomizedDelaySec | **absent** |
| Persistent | true |
| AccuracySec | 1s |
| Unit | hermes-anti-stall-supervisor.service |

Overlap policy: documented as runner process lock authoritative (not systemd `RefuseManualStart` serialization).

`systemd-analyze security` was **not** run against a live unit name (would require install into `/etc`); static directive audit + `systemd-analyze verify` rc=0 stand in for this staging lane.

## ROLLBACK.md summary

Documents:

1. Pre-enable timestamped backups (sibling `.bak.<TS>` + `runtime/anti_stall_supervisor_var/unit_backups/`).
2. Atomic deploy via temp + fsync + `os.replace` (INT/post-APPROVE only).
3. Scoped stop/disable of **only** these two units.
4. Restore from backup if present; else rename to `.disabled.<TS>` (**never delete**).
5. `daemon-reload` + verify inactive.
6. Preserve runtime/audit/journal; forbid git clean/reset/stash, log truncation, gateway restart.

## Forbidden actions confirmed not done

- No `systemctl enable/start/stop/mask` on these units
- No writes under `/etc/systemd/system`
- No live kanban DB mutation from this lane
- No dispatcher/gateway restart
- No role/model/assignee/toolset/profile/config changes
- No secrets in units

## Exact re-check

```bash
# 1) units still only staged
test ! -e /etc/systemd/system/hermes-anti-stall-supervisor.service
test ! -e /etc/systemd/system/hermes-anti-stall-supervisor.timer
systemctl is-enabled hermes-anti-stall-supervisor.timer 2>&1 || true   # expect not-found

# 2) verify staged units
systemd-analyze verify \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/hermes-anti-stall-supervisor.service \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/hermes-anti-stall-supervisor.timer
# expect rc=0 (unrelated xray warning may print)

# 3) self-test
/root/main/venv/bin/python \
  /root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/validate_deploy.py --self-test
# expect rc=0 and SELF_TEST_OK

# 4) hashes
sha256sum /root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/*
```

## Verdict

SUCCESS — five staging artifacts present; checks 1–3 green; zero live systemd mutation.
