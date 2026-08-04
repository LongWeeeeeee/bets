#!/usr/bin/env bash
set -euo pipefail
export HOME=/root
export PATH=/usr/local/lib/hermes-agent/venv/bin:/root/.local/bin:/root/.local/share/cursor-agent/versions/2026.07.09-a3815c0:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export VIRTUAL_ENV=/usr/local/lib/hermes-agent/venv
export HERMES_HOME=/root/.hermes

CFG=/root/.hermes/config.yaml
cp -a "$CFG" "/root/.hermes/config.yaml.bak_model_$(date +%Y%m%d_%H%M%S)"

python3 - <<'PY'
from pathlib import Path
path=Path('/root/.hermes/config.yaml')
text=path.read_text()
old='  default: cx/gpt-5.6-sol-xhigh'
new='  default: cu/gpt-5.5-high-fast'
if old not in text:
    # maybe already changed
    if new.split(': ',1)[1] in text.split('default:',1)[-1][:80]:
        print('already_switched')
    else:
        raise SystemExit(f'default model line not found: {old!r}')
else:
    path.write_text(text.replace(old, new, 1))
    print('model_switched_to', new.strip())
# show head
print('\n'.join(path.read_text().splitlines()[:8]))
PY

# Bounce only DEFAULT gateway (asfj). Do not touch worker profile (not running here anyway).
echo "=== stop default gateway pid(s) ==="
for pid in $(ps -eo pid,cmd | awk '/hermes_cli.main gateway run/ && $0 !~ /--profile/ && !/awk/ {print $1}'); do
  echo "TERM $pid"
  kill -TERM "$pid" 2>/dev/null || true
done
sleep 3
for pid in $(ps -eo pid,cmd | awk '/hermes_cli.main gateway run/ && $0 !~ /--profile/ && !/awk/ {print $1}'); do
  echo "KILL $pid"
  kill -KILL "$pid" 2>/dev/null || true
done
# clear stale lock if any
rm -f /root/.hermes/gateway.lock 2>/dev/null || true

# Prefer durable user unit
export XDG_RUNTIME_DIR=/run/user/0
export DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/0/bus
systemctl start user@0.service >/dev/null 2>&1 || true
sleep 1
if systemctl --user cat hermes-gateway.service >/dev/null 2>&1; then
  systemctl --user daemon-reload || true
  systemctl --user enable hermes-gateway.service || true
  systemctl --user reset-failed hermes-gateway.service 2>/dev/null || true
  systemctl --user restart hermes-gateway.service || systemctl --user start hermes-gateway.service
  sleep 5
  systemctl --user is-active hermes-gateway.service || true
else
  systemd-run --unit=hermes-default-gateway-oneshot --collect \
    --working-directory=/root/.hermes \
    --property=Restart=always \
    --property=RestartSec=5 \
    --setenv=HOME=/root \
    --setenv=USER=root \
    --setenv=LOGNAME=root \
    --setenv=HERMES_HOME=/root/.hermes \
    --setenv=VIRTUAL_ENV=/usr/local/lib/hermes-agent/venv \
    --setenv=PATH="$PATH" \
    /usr/local/lib/hermes-agent/venv/bin/python -m hermes_cli.main gateway run || true
  sleep 5
fi

echo "=== status ==="
ps -eo pid,etime,cmd | awk '/hermes_cli.main gateway run/ && !/awk/ {print}'
ss -lptn | grep 20128 || true
systemctl is-active diary-bot || true

# wait for telegram connect
for i in 1 2 3 4 5 6 7 8 9 10; do
  if grep -q "telegram connected\|Gateway running\|polling resumed" /root/.hermes/logs/gateway.log 2>/dev/null; then
    # ensure recent
    if python3 - <<'PY'
from pathlib import Path
import time
lines=Path('/root/.hermes/logs/gateway.log').read_text(errors='ignore').splitlines()[-40:]
print('\n'.join(lines))
ok=any('telegram connected' in l or 'Gateway running' in l or 'polling resumed' in l or 'Press Ctrl+C' in l for l in lines)
raise SystemExit(0 if ok else 1)
PY
    then
      break
    fi
  fi
  sleep 2
done

echo "=== recent gateway ==="
python3 - <<'PY'
from pathlib import Path
print('\n'.join(Path('/root/.hermes/logs/gateway.log').read_text(errors='ignore').splitlines()[-30:]))
print('==== errors ====')
print('\n'.join(Path('/root/.hermes/logs/errors.log').read_text(errors='ignore').splitlines()[-15:]))
print('model.default =', [l for l in Path('/root/.hermes/config.yaml').read_text().splitlines() if 'default:' in l][0])
PY
echo DONE
