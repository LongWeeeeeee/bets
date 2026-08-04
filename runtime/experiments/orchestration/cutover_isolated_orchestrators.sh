#!/bin/bash
set -uo pipefail
LOG=/root/main/runtime/isolated_orchestrators_cutover.log
exec >>"$LOG" 2>&1
echo "START $(date -u +%FT%TZ) pid=$$ ppid=$PPID"
systemctl stop hermes-default-gateway-oneshot.service || true
systemctl reset-failed hermes-default-gateway-oneshot.service || true
systemctl enable --now hermes-default-gateway.service
systemctl restart hermes-gateway-orchestration1.service hermes-gateway-orchestration2.service
sleep 5
for u in hermes-default-gateway.service hermes-gateway-orchestration1.service hermes-gateway-orchestration2.service hermes-gateway-worker.service; do
  printf '%s ' "$u"
  systemctl show "$u" -p ActiveState -p MainPID -p WorkingDirectory --no-pager | tr '\n' ' '
  echo
done
echo "END $(date -u +%FT%TZ)"
touch /root/main/runtime/isolated_orchestrators_cutover.ok
