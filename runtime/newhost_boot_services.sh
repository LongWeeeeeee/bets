#!/usr/bin/env bash
set -euo pipefail
export XDG_RUNTIME_DIR=/run/user/0
export DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/0/bus
export HOME=/root
export PATH=/usr/local/lib/hermes-agent/venv/bin:/usr/local/lib/hermes-agent/node_modules/.bin:/root/.local/share/cursor-agent/versions/2026.07.09-a3815c0:/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export VIRTUAL_ENV=/usr/local/lib/hermes-agent/venv

mkdir -p /run/user/0 /root/main/runtime /root/.hermes
loginctl enable-linger root >/dev/null 2>&1 || true
systemctl start user@0.service >/dev/null 2>&1 || true
sleep 1

start_bg() {
  # $1=pidfile $2=logfile then command...
  local pidfile="$1"; shift
  local logfile="$1"; shift
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    echo "already_running pid=$(cat "$pidfile") $*"
    return 0
  fi
  # Use systemd-run if available to detach cleanly without shell &
  if command -v systemd-run >/dev/null 2>&1; then
    systemd-run --unit="tmp-$(basename "$pidfile" .pid)" --collect \
      --property=Restart=no \
      --working-directory=/root \
      --setenv=HOME=/root \
      --setenv=USER=root \
      --setenv=LOGNAME=root \
      --setenv=PATH="$PATH" \
      --setenv=VIRTUAL_ENV="$VIRTUAL_ENV" \
      --setenv=HERMES_HOME="${HERMES_HOME:-/root/.hermes}" \
      --same-dir \
      /bin/bash -lc "$* >>'$logfile' 2>&1" >/tmp/systemd-run-out.txt 2>&1 || true
    sleep 2
  fi
}

# OmniRoute
if ! ss -lptn | grep -q ':20128'; then
  HERMES_HOME=/root/.hermes
  systemd-run --unit=omniroute-oneshot --collect \
    --working-directory=/root \
    --property=Restart=always \
    --property=RestartSec=5 \
    --property=MemoryMax=1500M \
    --property=OOMScoreAdjust=200 \
    --setenv=HOME=/root \
    --setenv=PATH="$PATH" \
    /bin/bash /root/.omniroute/omniroute-serve.sh \
    >/tmp/omniroute-run.txt 2>&1 || true
  # also append log via wrapper if unit doesn't capture
  sleep 3
fi
ss -lptn | grep 20128 || { echo FAIL_omniroute_port; cat /tmp/omniroute-run.txt 2>/dev/null; tail -n 40 /root/main/runtime/omniroute.log 2>/dev/null; exit 1; }
echo OMNIROUTE_OK

# Default hermes profile gateway (asfjashjksa12bot). Do NOT start worker/lasofas here.
if ! pgrep -f '/usr/local/lib/hermes-agent/venv/bin/python -m hermes_cli.main gateway run$' >/dev/null 2>&1 \
   && ! pgrep -f 'python -m hermes_cli.main gateway run' >/dev/null 2>&1; then
  systemd-run --unit=hermes-default-gateway-oneshot --collect \
    --working-directory=/root/.hermes \
    --property=Restart=always \
    --property=RestartSec=5 \
    --property=KillMode=mixed \
    --property=TimeoutStopSec=90 \
    --setenv=HOME=/root \
    --setenv=USER=root \
    --setenv=LOGNAME=root \
    --setenv=HERMES_HOME=/root/.hermes \
    --setenv=VIRTUAL_ENV=/usr/local/lib/hermes-agent/venv \
    --setenv=PATH="$PATH" \
    /usr/local/lib/hermes-agent/venv/bin/python -m hermes_cli.main gateway run \
    >/tmp/hermes-default-run.txt 2>&1 || true
  sleep 6
else
  echo DEFAULT_GATEWAY_ALREADY
fi

echo '=== processes ==='
pgrep -af 'hermes_cli.main|omniroute' || true
for pid in $(pgrep -f 'hermes_cli.main gateway run' || true); do
  echo "PID=$pid CWD=$(readlink -f /proc/$pid/cwd 2>/dev/null || true)"
  tr '\0' '\n' < /proc/$pid/environ 2>/dev/null | grep -E 'HERMES_HOME|OPENAI_BASE_URL' || true
done
echo '=== units ==='
systemctl is-active omniroute-oneshot hermes-default-gateway-oneshot 2>/dev/null || true
systemctl status omniroute-oneshot --no-pager 2>/dev/null | head -n 20 || true
systemctl status hermes-default-gateway-oneshot --no-pager 2>/dev/null | head -n 30 || true
journalctl -u hermes-default-gateway-oneshot -n 40 --no-pager 2>/dev/null || true
echo BOOT_DONE
