#!/bin/bash
# External oneshot: restart Hermes gateways so kanban_db salvage spawn patch loads.
# Safe to run from cron/at outside agent gateway cgroup.
set -u
LOG=/root/main/runtime/hermes_gateway_restart_salvage.log
exec >>"$LOG" 2>&1
echo "START $(date -u +%Y-%m-%dT%H:%M:%SZ) pid=$$ ppid=$PPID"

restart_one() {
  local u="$1"
  if systemctl cat "$u" >/dev/null 2>&1; then
    echo "restart $u"
    systemctl restart "$u" || systemctl start "$u" || echo "WARN fail $u"
  else
    echo "skip missing $u"
  fi
}

# Default holds kanban dispatcher lock
restart_one hermes-default-gateway-oneshot.service
sleep 2
# If transient oneshot is dead after restart, try start
if ! systemctl is-active --quiet hermes-default-gateway-oneshot.service 2>/dev/null; then
  echo "default inactive -> start"
  systemctl start hermes-default-gateway-oneshot.service || true
  sleep 2
fi

restart_one hermes-gateway-worker.service
restart_one hermes-gateway-orchestration1.service
restart_one hermes-gateway-orchestration2.service
sleep 3

echo "=== units ==="
for u in hermes-default-gateway-oneshot.service \
         hermes-gateway-worker.service \
         hermes-gateway-orchestration1.service \
         hermes-gateway-orchestration2.service; do
  st=$(systemctl show "$u" -p ActiveState -p MainPID --no-pager 2>/dev/null | tr '\n' ' ')
  echo "$u: $st"
done

echo "=== lock ==="
fuser -v /root/.hermes/kanban/.dispatcher.lock 2>&1 | head -12 || true
echo "=== gateways ==="
pgrep -af 'hermes_cli.main.*gateway run' || true

echo "=== code markers ==="
/usr/local/lib/hermes-agent/venv/bin/python - <<'PY'
import inspect
import hermes_cli.kanban_db as k
print('HAS_SALVAGE', 'HERMES_KANBAN_SALVAGE_MD' in inspect.getsource(k._default_spawn))
print('HAS_TIMEOUT', '_try_inline_salvage_on_timeout' in inspect.getsource(k.enforce_max_runtime))
PY

echo "END $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo OK > /root/main/runtime/hermes_gateway_restart_salvage.ok
