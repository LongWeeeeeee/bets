#!/bin/bash
# Пересборка про-корпуса: обход команд tier-1 и tier-2 через Stratz.
#
# Зачем сейчас: про-корпус отстал от паблика на целый патч — матчей 7.41e в нём
# нет вовсе, тогда как у паблика это 15 файлов из 81. Все проверки «переносится
# на про» и все калибровки порогов меряются на 7.41a-d, пока словарь наполовину
# состоит из 7.41e.
#
# Квота Stratz общая с фазой 2 паблик-сбора, но про-обход на порядки короче:
# сотни команд против 447 тысяч игроков.
set -e
cd /root/main
source venv/bin/activate
export PYTHONUNBUFFERED=1
LOG="runtime/get_pros_$(date +%Y%m%d_%H%M).log"
nohup python3 -c 'import sys
sys.path.insert(0, "base")
import maps_research as mr
mr.get_pros()' > "$LOG" 2>&1 &
PID=$!
echo "$PID" > runtime/get_pros.pid
echo "PID=$PID"
echo "лог: $LOG"
