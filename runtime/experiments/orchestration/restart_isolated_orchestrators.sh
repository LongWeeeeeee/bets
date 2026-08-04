#!/bin/bash
set -uo pipefail
LOG=/root/main/runtime/restart_isolated_orchestrators.log
exec >>"$LOG" 2>&1
echo "START $(date -u +%FT%TZ) pid=$$ ppid=$PPID"
for u in hermes-default-gateway.service hermes-gateway-orchestration1.service hermes-gateway-orchestration2.service; do
  echo "restart $u"
  systemctl restart "$u" || { echo "FAILED $u"; exit 1; }
done
sleep 5
for u in hermes-default-gateway.service hermes-gateway-orchestration1.service hermes-gateway-orchestration2.service hermes-gateway-worker.service; do
  printf '%s ' "$u"
  systemctl show "$u" -p ActiveState -p MainPID -p WorkingDirectory --no-pager | tr '\n' ' '
  echo
done
echo "END $(date -u +%FT%TZ)"
touch /root/main/runtime/restart_isolated_orchestrators.ok
