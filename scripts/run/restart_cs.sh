#!/bin/bash
# Перезапуск в РЕЖИМЕ DLTV2 (html-источник, без прокси) вместо боевого sourcetv.
#
# Это смена режима, а не обычный перезапуск: для него есть
# restart_cyberscore.sh. Службу здесь надо именно ОСТАНОВИТЬ, иначе systemd
# (Restart=always, RestartSec=10) поднимет боевой экземпляр рядом с этим.
set -e

systemctl stop cyberscore.service 2>/dev/null || true
sleep 2
pkill -f 'python3.*cyberscore_try' 2>/dev/null || true
sleep 1

cd /root/main
bash scripts/run/run_dltv2.sh > /tmp/dltv_restart.log 2>&1 &
sleep 2
echo "экземпляров: $(ps -eo args | grep -c '[c]yberscore_try\.py')"
echo "ВЕРНУТЬ БОЕВОЙ РЕЖИМ: systemctl start cyberscore.service"
