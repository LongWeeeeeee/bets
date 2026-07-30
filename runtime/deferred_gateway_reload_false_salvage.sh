#!/bin/bash
set -u
LOG=/root/main/runtime/deferred_gateway_reload_false_salvage.log
exec >>"$LOG" 2>&1
echo "START $(date -u +%Y-%m-%dT%H:%M:%SZ)"
DEADLINE=$(( $(date +%s) + 2700 ))  # 45m max wait
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  # wait until no kanban worker chat processes
  if ! pgrep -af 'hermes .*-p worker.*work kanban task' >/dev/null 2>&1; then
    echo "no kanban workers at $(date -u +%Y-%m-%dT%H:%M:%SZ) -> reload"
    break
  fi
  # if only one and it's healthy, keep waiting
  echo "wait workers: $(pgrep -af 'work kanban task' | tr '\n' '|' )"
  sleep 30
done
/bin/bash /root/main/runtime/restart_hermes_gateways_salvage.sh
# verify new gate
/usr/local/lib/hermes-agent/venv/bin/python - <<'PY'
import hermes_cli.kanban_db as k, inspect
src=inspect.getsource(k._build_salvage_spawn_context)
print('HAS_ORIGIN_GUARD', 'Origin session_id alone must not' in src or 'has_recovery' in src)
print('HAS_AUG_GATE', 'Origin ``tasks.session_id`` alone is not enough' in inspect.getsource(k._augment_spawn_prompt_with_salvage) or 'has_recovery' in inspect.getsource(k._augment_spawn_prompt_with_salvage))
PY
echo "END $(date -u +%Y-%m-%dT%H:%M:%SZ)"
