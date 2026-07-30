#!/bin/bash
set -euo pipefail
export XDG_RUNTIME_DIR=/run/user/0
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=/run/user/0/bus}"

# Apply session_reset=none configs by cycling both gateways from cron (outside agent process).
{
  echo "time=$(date -Is)"

  # default bot @asfjashjksa12bot (user unit)
  /usr/bin/systemctl --user kill -s HUP hermes-gateway.service 2>/dev/null || true
  /usr/bin/systemctl --user stop hermes-gateway.service || true
  sleep 1
  /usr/bin/systemctl --user start hermes-gateway.service || true
  sleep 2
  echo -n "user hermes-gateway: "
  /usr/bin/systemctl --user is-active hermes-gateway.service || true

  # worker bot @lasofas12bot (system unit)
  /usr/bin/systemctl stop hermes-gateway-worker.service || true
  sleep 1
  /usr/bin/systemctl start hermes-gateway-worker.service || true
  sleep 2
  echo -n "system hermes-gateway-worker: "
  /usr/bin/systemctl is-active hermes-gateway-worker.service || true

  python3 - <<'PY'
from pathlib import Path
for p in [
 Path('/root/.hermes/config.yaml'),
 Path('/root/.hermes/profiles/planner/config.yaml'),
 Path('/root/.hermes/profiles/reviewer/config.yaml'),
 Path('/root/.hermes/profiles/worker/config.yaml'),
]:
    lines=p.read_text().splitlines()
    for i,l in enumerate(lines):
        if l.startswith('session_reset:'):
            print(p, '->', lines[i+1].strip())
            break
PY
} > /tmp/session_reset_apply.status 2>&1
