#!/usr/bin/env bash
set -euo pipefail
export HOME=/root
export XDG_RUNTIME_DIR=/run/user/0
export DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/0/bus
export PATH=/usr/local/lib/hermes-agent/venv/bin:/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export HERMES_HOME=/root/.hermes
export VIRTUAL_ENV=/usr/local/lib/hermes-agent/venv

# Ensure model is sol-xhigh
python3 - <<'PY'
from pathlib import Path
cfg=Path('/root/.hermes/config.yaml')
text=cfg.read_text()
target='  default: cx/gpt-5.6-sol-xhigh'
import re
m=re.search(r'(?m)^  default: .+$', text)
if not m:
    raise SystemExit('no default model line')
if m.group(0) != target:
    cfg.write_text(text[:m.start()] + target + text[m.end():])
    print('model_set', target.strip())
else:
    print('model_ok', target.strip())
print('\n'.join(cfg.read_text().splitlines()[:4]))
PY

# Soft-cycle default gateway only (no --profile)
mapfile -t PIDS < <(ps -eo pid,cmd | awk '/hermes_cli.main gateway run/ && $0 !~ /--profile/ && !/awk/ {print $1}')
for pid in "${PIDS[@]:-}"; do
  echo "signal_term pid=$pid"
  kill -TERM "$pid" 2>/dev/null || true
done
sleep 3
mapfile -t PIDS2 < <(ps -eo pid,cmd | awk '/hermes_cli.main gateway run/ && $0 !~ /--profile/ && !/awk/ {print $1}')
for pid in "${PIDS2[@]:-}"; do
  echo "signal_kill pid=$pid"
  kill -KILL "$pid" 2>/dev/null || true
done
rm -f /root/.hermes/gateway.lock 2>/dev/null || true

# Bring unit back up via user service if present
systemctl start user@0.service >/dev/null 2>&1 || true
sleep 1
if systemctl --user cat hermes-gateway.service >/dev/null 2>&1; then
  systemctl --user daemon-reload || true
  systemctl --user reset-failed hermes-gateway.service 2>/dev/null || true
  # start is enough if unit Restart=always and process died; also try start after failed
  if ! systemctl --user start hermes-gateway.service; then
    # unit may think still active; force via kill+start
    systemctl --user kill -s SIGKILL hermes-gateway.service 2>/dev/null || true
    sleep 1
    systemctl --user start hermes-gateway.service
  fi
  sleep 5
  systemctl --user is-active hermes-gateway.service || true
else
  systemd-run --unit=hermes-default-gateway-oneshot --collect \
    --working-directory=/root/.hermes \
    --property=Restart=always --property=RestartSec=5 \
    --setenv=HOME=/root --setenv=USER=root --setenv=LOGNAME=root \
    --setenv=HERMES_HOME=/root/.hermes \
    --setenv=VIRTUAL_ENV=/usr/local/lib/hermes-agent/venv \
    --setenv=PATH="$PATH" \
    /usr/local/lib/hermes-agent/venv/bin/python -m hermes_cli.main gateway run
  sleep 5
fi

python3 - <<'PY'
from pathlib import Path
import subprocess, time, json, urllib.request, sqlite3
print('procs:')
print(subprocess.getoutput("ps -eo pid,etime,cmd | awk '/hermes_cli.main gateway run/ && !/awk/ {print}'"))
print('model', [l for l in Path('/root/.hermes/config.yaml').read_text().splitlines() if l.strip().startswith('default:')][:1])
# wait connect
ok=False
for _ in range(20):
    lines=Path('/root/.hermes/logs/gateway.log').read_text(errors='ignore').splitlines()[-40:]
    if any('telegram connected' in l or 'Gateway running with' in l for l in lines[-15:]):
        # prefer very recent start
        if any('Starting Hermes Gateway' in l or 'telegram connected' in l for l in lines):
            print('gateway_log_tail:')
            print('\n'.join(lines[-18:]))
            ok=True
            break
    time.sleep(1)
if not ok:
    print('gateway_log_tail_fallback:')
    print('\n'.join(Path('/root/.hermes/logs/gateway.log').read_text(errors='ignore').splitlines()[-25:]))

key=Path('/root/.omniroute/local_api_key').read_text().strip()
payload=json.dumps({'model':'cx/gpt-5.6-sol-xhigh','messages':[{'role':'user','content':'Reply with exactly: pong'}],'max_tokens':8,'stream':False}).encode()
req=urllib.request.Request('http://127.0.0.1:20128/v1/chat/completions', data=payload, headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'})
with urllib.request.urlopen(req, timeout=90) as r:
    data=json.load(r)
print('chat', ((data.get('choices') or [{}])[0].get('message') or {}).get('content'))
con=sqlite3.connect('file:/root/.omniroute/storage.sqlite?mode=ro', uri=True)
for row in con.execute("SELECT email,is_active,test_status,created_at FROM provider_connections WHERE provider='codex' ORDER BY created_at"):
    print('codex', row)
print('diary', subprocess.getoutput('systemctl is-active diary-bot'))
print('DONE')
PY
