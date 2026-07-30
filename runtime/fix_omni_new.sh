#!/usr/bin/env bash
set -euo pipefail
export PATH=/root/.local/bin:/root/.local/share/cursor-agent/versions/2026.07.09-a3815c0:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export HOME=/root

echo "=== before ==="
ss -lptn | grep 20128 || true
ps -eo pid,etime,cmd | awk '/omniroute/ && !/awk/ {print}'

# stop current omniroute listeners carefully
for pid in $(ps -eo pid,cmd | awk '/omniroute serve|--no-open|--no-recovery/ && !/awk/ {print $1}'); do
  echo "term $pid"
  kill -TERM "$pid" 2>/dev/null || true
done
# also kill binary named omniroute
for pid in $(ps -eo pid,cmd | awk '$0 ~ /omniroute \(v/ && !/awk/ {print $1}'); do
  echo "term bin $pid"
  kill -TERM "$pid" 2>/dev/null || true
done
sleep 2
for pid in $(ps -eo pid,cmd | awk '/omniroute/ && !/awk/ && $0 !~ /fix_omni/ {print $1}'); do
  echo "kill $pid"
  kill -KILL "$pid" 2>/dev/null || true
done
sleep 1

# start via durable user unit if possible, else systemd-run
export XDG_RUNTIME_DIR=/run/user/0
export DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/0/bus
loginctl enable-linger root >/dev/null 2>&1 || true
systemctl start user@0.service >/dev/null 2>&1 || true
sleep 1

if systemctl --user cat omniroute.service >/dev/null 2>&1; then
  systemctl --user daemon-reload || true
  systemctl --user enable omniroute.service || true
  systemctl --user reset-failed omniroute.service 2>/dev/null || true
  # start unit; if already failed, restart
  if ! systemctl --user start omniroute.service 2>/tmp/omni_start.err; then
    cat /tmp/omni_start.err || true
    systemctl --user restart omniroute.service || true
  fi
  sleep 3
  systemctl --user is-active omniroute.service || true
else
  systemd-run --unit=omniroute-oneshot --collect \
    --working-directory=/root \
    --property=Restart=always \
    --property=RestartSec=5 \
    --property=MemoryMax=1500M \
    --property=OOMScoreAdjust=200 \
    --setenv=HOME=/root \
    --setenv=PATH="$PATH" \
    /bin/bash /root/.omniroute/omniroute-serve.sh || true
  sleep 3
fi

echo "=== after ==="
ss -lptn | grep 20128 || { echo NO_PORT; tail -n 50 /root/main/runtime/omniroute.log; exit 1; }
ps -eo pid,etime,cmd | awk '/omniroute/ && !/awk/ {print}'
# health
python3 - <<'PY'
import re, json, urllib.request
from pathlib import Path
env=Path('/root/.hermes/.env').read_text()
key=re.search(r'^OPENAI_API_KEY=(.*)$', env, re.M).group(1).strip().strip('"\'')
# models
req=urllib.request.Request('http://127.0.0.1:20128/v1/models', headers={'Authorization':'Bearer '+key})
try:
    with urllib.request.urlopen(req, timeout=20) as r:
        data=json.load(r)
    ids=[m.get('id') for m in data.get('data',[])]
    print('MODELS_OK', len(ids), ids[:12])
except Exception as e:
    body=b''
    if hasattr(e,'read'):
        try: body=e.read()
        except Exception: pass
    print('MODELS_ERR', type(e).__name__, e, body[:300])
# tiny chat
payload=json.dumps({
  'model':'cx/gpt-5.6-sol-xhigh',
  'messages':[{'role':'user','content':'Reply with exactly: pong'}],
  'max_tokens': 16,
  'stream': False,
}).encode()
req=urllib.request.Request('http://127.0.0.1:20128/v1/chat/completions', data=payload, headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'})
try:
    with urllib.request.urlopen(req, timeout=120) as r:
        data=json.load(r)
    print('CHAT_OK', json.dumps(data)[:400])
except Exception as e:
    body=b''
    if hasattr(e,'read'):
        try: body=e.read()
        except Exception: pass
    print('CHAT_ERR', type(e).__name__, e, body[:500])
PY

echo "=== diary ==="
systemctl is-active diary-bot
journalctl -u diary-bot -n 12 --no-pager

echo "=== gateway still up? ==="
ps -eo pid,etime,stat,cmd | awk '/hermes_cli.main gateway run/ && !/awk/ && !/--profile/ {print}'
# recent gateway/agent after now
python3 - <<'PY'
from pathlib import Path
import time
for name in ['gateway.log','agent.log','errors.log']:
    p=Path('/root/.hermes/logs')/name
    print(name, 'mtime', time.ctime(p.stat().st_mtime))
    lines=p.read_text(errors='ignore').splitlines()
    print('\n'.join(lines[-15:]))
    print('----')
PY
echo DONE
