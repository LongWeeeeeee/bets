#!/bin/bash
# Перезапуск боевого cyberscore.
#
# ВНИМАНИЕ. Раньше здесь было `pkill` + `nohup`, и это поднимало ДВА бота:
# служба cyberscore.service идёт под systemd с Restart=always и RestartSec=10,
# поэтому после pkill systemd через десять секунд запускал свой экземпляр —
# рядом с тем, что стартовал этот скрипт. Для системы ставок это двойные
# сигналы. Вдобавок аргументы расходились: скрипт запускал без
# --dltv-source sourcetv, то есть второй бот работал в другом режиме.
#
# Управляет процессом systemd. Единственный правильный перезапуск — через него.
set -e

# Снимок карт сбрасывается до перезапуска, как и раньше.
rm -f /root/.local/state/ingame/map_id_check.txt

systemctl restart cyberscore.service
sleep 3

echo "состояние: $(systemctl is-active cyberscore.service)"
echo "экземпляров: $(ps -eo args | grep -c '[c]yberscore_try\.py')"
systemctl status cyberscore.service --no-pager | head -4
