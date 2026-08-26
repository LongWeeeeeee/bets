# Anti-stall supervisor (candidate)

Isolated integration of W1–W5 staging lanes into one candidate package.

**Location:** `/root/main/services/anti_stall_supervisor/`  
**State/report/audit (outside package, mutable runtime):** `/root/main/runtime/anti_stall_supervisor_var/`  
**W5 systemd units:** present under `deploy/` but **inert** (never installed by this candidate).

## Modules

| File | Parent | Role |
|------|--------|------|
| `snapshot.py` | W1 t_39059a80 | read-only multi-board snapshot |
| `decision.py` + `policy.json` | W2 t_0e0ff6f2 | fail-closed stall decisions |
| `executor.py` | W3 t_a1e6e05d | transactional idempotent actions |
| `runner.py` | W4 t_e048eaa5 | single flock hygiene tick |
| `adapters.py` | INT | shape bridge W1↔W2↔W3 + runner adapters |
| `__main__.py` | INT | CLI entry |
| `deploy/*` | W5 t_23457e9d | inert service/timer + validate/rollback |

## Commands

Isolated tests (exact):

```bash
cd /root/main && /root/main/venv/bin/python -m pytest -q \
  /root/main/services/anti_stall_supervisor/tests/test_snapshot.py \
  /root/main/services/anti_stall_supervisor/tests/test_decision.py \
  /root/main/services/anti_stall_supervisor/tests/test_executor.py \
  /root/main/services/anti_stall_supervisor/tests/test_runner.py \
  /root/main/services/anti_stall_supervisor/tests/test_integration.py
```

Live dry-run (no mutations; quiet stdout on routine success):

```bash
mkdir -p /root/main/runtime/anti_stall_supervisor_var
cd /root/main && /root/main/venv/bin/python \
  /root/main/services/anti_stall_supervisor/__main__.py \
  --config /root/main/services/anti_stall_supervisor/config.json \
  --dry-run \
  --var-dir /root/main/runtime/anti_stall_supervisor_var
# then inspect report:
python3 -c 'import json;print(json.load(open("/root/main/runtime/anti_stall_supervisor_var/report.json"))["decision"])'
```

Running-card tuples (W1 adapter):

```bash
/root/main/venv/bin/python /root/main/services/anti_stall_supervisor/__main__.py --emit-running-tuples
```

Or use the wrapper that records before/after tuples and asserts `planned_actions=0`:

```bash
/root/main/venv/bin/python /root/main/services/anti_stall_supervisor/scripts/live_dry_run_validate.py
```

## Safety

- Live Kanban DB opens are read-only in snapshot; executor refuses writable live paths.
- Dry-run forces executor `dry_run=True` and does not advance action_keys/cooldowns/observations.
- Routine success keeps stdout empty (unless `--print-report-summary` / `--emit-running-tuples`).
- No `/etc` writes, no systemctl, no Hermes config changes from this package.
