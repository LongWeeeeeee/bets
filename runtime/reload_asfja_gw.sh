#!/bin/bash
set -euo pipefail
export XDG_RUNTIME_DIR=/run/user/0
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=/run/user/0/bus}"
# Apply config for @asfjashjksa12bot default gateway only (not worker).
/usr/bin/systemctl --user kill -s TERM hermes-gateway.service || true
sleep 2
/usr/bin/systemctl --user start hermes-gateway.service || true
sleep 2
{
  echo "time=$(date -Is)"
  /usr/bin/systemctl --user is-active hermes-gateway.service
  /usr/bin/systemctl --user status hermes-gateway.service --no-pager | head -n 15
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
} > /tmp/asfja_gw_reload.status 2>&1
