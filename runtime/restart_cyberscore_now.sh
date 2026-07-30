#!/bin/bash
set -euo pipefail
cd /root/main

# Single owner: systemd unit cyberscore.service.
# Never setsid/nohup a second python — that races with Restart=always and dual-PIDs.

echo "Pre-clean orphan cyberscore PIDs (non-unit)..."
while read -r pid; do
  [ -n "${pid:-}" ] || continue
  exe="$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)"
  case "$exe" in
    */python*) ;;
    *) continue ;;
  esac
  cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
  case "$cmd" in
    *base/cyberscore_try.py*) ;;
    *) continue ;;
  esac
  cg="$(tr '\0' ' ' < "/proc/$pid/cgroup" 2>/dev/null || true)"
  if echo "$cg" | grep -q 'cyberscore.service'; then
    continue
  fi
  echo "Stopping orphan pid=$pid exe=$exe"
  kill "$pid" 2>/dev/null || true
done < <(ps -eo pid=)

sleep 1
while read -r pid; do
  [ -n "${pid:-}" ] || continue
  exe="$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)"
  case "$exe" in */python*) ;; *) continue ;; esac
  cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
  case "$cmd" in *base/cyberscore_try.py*) ;; *) continue ;; esac
  cg="$(tr '\0' ' ' < "/proc/$pid/cgroup" 2>/dev/null || true)"
  if echo "$cg" | grep -q 'cyberscore.service'; then
    continue
  fi
  echo "Force-killing orphan pid=$pid"
  kill -9 "$pid" 2>/dev/null || true
done < <(ps -eo pid=)

rm -f /root/.local/state/ingame/map_id_check.txt
echo "map_id_check cleared OK"

# Env comes from unit drop-ins (stats.conf / odds.conf / oom.conf).
export STATS_LOOKUP_BACKEND="${STATS_LOOKUP_BACKEND:-sqlite}"

echo "systemctl restart cyberscore.service"
systemctl restart cyberscore.service
sleep 2

if ! systemctl is-active --quiet cyberscore.service; then
  echo "ERROR: cyberscore.service not active" >&2
  systemctl status cyberscore.service --no-pager -l | head -40 >&2 || true
  exit 1
fi

NEWPID="$(systemctl show cyberscore.service -p MainPID --value)"
echo "Started NEWPID=$NEWPID (systemd MainPID)"
if [ -n "$NEWPID" ] && [ "$NEWPID" != "0" ] && [ -d "/proc/$NEWPID" ]; then
  echo -900 > "/proc/$NEWPID/oom_score_adj" 2>/dev/null || true
  echo "oom_score_adj=$(cat /proc/$NEWPID/oom_score_adj 2>/dev/null || echo n/a) pid=$NEWPID"
fi

cmd="$(tr '\0' ' ' < "/proc/$NEWPID/cmdline" 2>/dev/null || true)"
echo "RUNNING pid=$NEWPID cmd=$cmd"
echo "unit=$(systemctl is-active cyberscore.service) log=/root/main/log.txt"

N="$(ps -eo pid=,cmd= | awk '/cyberscore_try\.py/ && /python/ && !/awk/ {c++} END{print c+0}')"
echo "cyberscore_procs=$N"
if [ "$N" != "1" ]; then
  echo "WARN: expected 1 cyberscore proc, found $N" >&2
  pgrep -af 'base/cyberscore_try.py' >&2 || true
fi

if [ -f /root/main/log.txt ]; then
  echo "log_bytes=$(wc -c < /root/main/log.txt)"
  tail -n 15 /root/main/log.txt || true
fi
