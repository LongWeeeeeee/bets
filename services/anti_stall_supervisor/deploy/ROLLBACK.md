# ROLLBACK — hermes-anti-stall-supervisor (scoped, no delete)

Scope: only these two units and their timestamped backups.
Never delete units, never truncate logs, never reset/clean/stash git, never restart
gateway/dispatcher profiles, never touch other systemd units or live kanban DBs
beyond what the supervisor itself already wrote under its own paths.

Assumptions:
- Staged sources live at:
  `/root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/hermes-anti-stall-supervisor.service`
  `/root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy/hermes-anti-stall-supervisor.timer`
- Live install targets (INT/production deploy card only):
  `/etc/systemd/system/hermes-anti-stall-supervisor.service`
  `/etc/systemd/system/hermes-anti-stall-supervisor.timer`
- Backups (if taken): same basenames with suffix `.bak.<UTC_TS>` beside the live path
  or under `/root/main/runtime/anti_stall_supervisor_var/unit_backups/`.

---

## 0. Pre-enable backup (mandatory before first install)

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
BKDIR=/root/main/runtime/anti_stall_supervisor_var/unit_backups
mkdir -p "$BKDIR"
for u in hermes-anti-stall-supervisor.service hermes-anti-stall-supervisor.timer; do
  src=/etc/systemd/system/$u
  if [ -e "$src" ]; then
    # copy + fsync; keep original in place until atomic replace below
    cp -a -- "$src" "$BKDIR/${u}.bak.${TS}"
    # also sibling backup next to unit (restore path of last resort)
    cp -a -- "$src" "${src}.bak.${TS}"
    sync -f "$BKDIR/${u}.bak.${TS}" 2>/dev/null || sync
  else
    echo "no pre-existing $src (first install)"
  fi
done
ls -la "$BKDIR" | sed -n '1,50p'
```

## 1. Deploy / enable (INT or post-APPROVE deploy card ONLY — not W5)

Atomic install via temp + fsync + rename (never write final path in-place):

```bash
STAGED=/root/main/runtime/anti_stall_staging/t_3c0b64e6/deploy
# After INT assembly the canonical live sources may instead be:
# STAGED=/root/main/services/anti_stall_supervisor/deploy
TS=$(date -u +%Y%m%dT%H%M%SZ)

install_unit () {
  local name="$1"
  local src="$STAGED/$name"
  local dst="/etc/systemd/system/$name"
  local tmp="${dst}.tmp.${TS}.$$"
  test -f "$src" || { echo "missing $src"; return 2; }
  cp -a -- "$src" "$tmp"
  # preserve mode expected for units
  chmod 644 "$tmp"
  # fsync file then replace
  python3 - <<PY
import os
p="$tmp"
fd=os.open(p, os.O_RDONLY)
os.fsync(fd); os.close(fd)
dirfd=os.open(os.path.dirname(p), os.O_RDONLY)
os.fsync(dirfd); os.close(dirfd)
os.replace(p, "$dst")
dirfd=os.open("/etc/systemd/system", os.O_RDONLY)
os.fsync(dirfd); os.close(dirfd)
print("installed", "$dst")
PY
}

install_unit hermes-anti-stall-supervisor.service
install_unit hermes-anti-stall-supervisor.timer
systemctl daemon-reload
# Dry-run / fingerprint gates MUST pass before enable (see validate_deploy.py).
# Only after gates:
systemctl enable --now hermes-anti-stall-supervisor.timer
systemctl is-enabled hermes-anti-stall-supervisor.timer
systemctl is-active hermes-anti-stall-supervisor.timer
systemctl status hermes-anti-stall-supervisor.timer --no-pager -l | sed -n '1,40p'
```

Block enable if: dry-run has any status-changing planned action; any running card has
`planned_actions != 0`; fingerprint delta; missing report; nonzero last service rc;
lock held; unsafe unit directive; absent backup on upgrade path.

---

## 2. Rollback (stop/disable ONLY these units)

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
SVC=hermes-anti-stall-supervisor.service
TMR=hermes-anti-stall-supervisor.timer

# Stop/disable timer first so no new ticks fire, then stop oneshot if active.
systemctl stop "$TMR" 2>/dev/null || true
systemctl disable "$TMR" 2>/dev/null || true
systemctl stop "$SVC" 2>/dev/null || true
# Do NOT mask; do NOT reset-failed globally; scoped only:
systemctl reset-failed "$SVC" 2>/dev/null || true
systemctl reset-failed "$TMR" 2>/dev/null || true

restore_or_disable () {
  local name="$1"
  local dst="/etc/systemd/system/$name"
  local bak=""
  # Prefer newest sibling .bak.* then var backup dir
  bak=$(ls -1t "${dst}.bak."* 2>/dev/null | head -1 || true)
  if [ -z "$bak" ]; then
    bak=$(ls -1t /root/main/runtime/anti_stall_supervisor_var/unit_backups/${name}.bak.* 2>/dev/null | head -1 || true)
  fi
  if [ -n "$bak" ] && [ -f "$bak" ]; then
    tmp="${dst}.restore.tmp.${TS}.$$"
    cp -a -- "$bak" "$tmp"
    chmod 644 "$tmp"
    python3 - <<PY
import os
p="$tmp"; d="$dst"
fd=os.open(p, os.O_RDONLY); os.fsync(fd); os.close(fd)
os.replace(p, d)
dirfd=os.open("/etc/systemd/system", os.O_RDONLY); os.fsync(dirfd); os.close(dirfd)
print("restored", d, "from", "$bak")
PY
  elif [ -e "$dst" ]; then
    # Never delete: rename aside to .disabled.<timestamp>
    disabled="${dst}.disabled.${TS}"
    mv -n -- "$dst" "$disabled"
    echo "renamed $dst -> $disabled (no backup present)"
  else
    echo "nothing to restore/disable for $name"
  fi
}

restore_or_disable "$SVC"
restore_or_disable "$TMR"
systemctl daemon-reload

# Verify inactive / not enabled
systemctl is-enabled "$TMR" 2>&1 || true
systemctl is-active "$TMR" 2>&1 || true
systemctl is-active "$SVC" 2>&1 || true
systemctl status "$TMR" --no-pager -l 2>&1 | sed -n '1,30p' || true
systemctl status "$SVC" --no-pager -l 2>&1 | sed -n '1,30p' || true
test "$(systemctl is-active "$TMR" 2>/dev/null || echo inactive)" = "inactive"
test "$(systemctl is-active "$SVC" 2>/dev/null || echo inactive)" = "inactive"
```

## 3. What MUST remain intact after rollback

- `/root/main/services/anti_stall_supervisor/**` code and config (leave for audit)
- `/root/main/runtime/anti_stall_supervisor_var/**` state/report/audit/lock/backups
- Journal logs for `hermes-anti-stall-supervisor` (no vacuum/truncate)
- All kanban board DBs and Hermes gateways/dispatchers
- Staged W5 sources under `runtime/anti_stall_staging/t_3c0b64e6/deploy/`

## 4. Forbidden during rollback

```text
rm /etc/systemd/system/hermes-anti-stall-supervisor.*
systemctl mask hermes-anti-stall-supervisor.*
git clean / git reset / git stash
truncate log.txt / journalctl --vacuum
systemctl restart hermes-gateway-*   # unless a separate explicit incident response
killall -9 python / indiscriminate pkill
```

## 5. Re-check after rollback

```bash
systemctl is-enabled hermes-anti-stall-supervisor.timer 2>&1 || true
systemctl is-active hermes-anti-stall-supervisor.timer 2>&1 || true
ls -la /etc/systemd/system/hermes-anti-stall-supervisor* 2>&1 || true
ls -la /root/main/runtime/anti_stall_supervisor_var 2>&1 | sed -n '1,40p' || true
```
