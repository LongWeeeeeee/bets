#!/bin/bash
# Перезапуск прода — только через systemd (docs/RUNTIME_RULES.md:13).
# pkill/kill по PID дают второй экземпляр: systemd поднимает процесс сам.
set -e
cd /root/main

systemctl stop cyberscore
rm -f ~/.local/state/ingame/map_id_check.txt
systemctl start cyberscore

sleep 3
systemctl --no-pager --lines=15 status cyberscore
