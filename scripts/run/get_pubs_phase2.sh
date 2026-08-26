#!/bin/bash
# Фаза 2: полный повторный обход — курсор сбрасывается, чтобы КАЖДОГО игрока
# спросили заново. Обход помнит только «спрашивали или нет», поэтому матчи,
# сыгранные игроком ПОСЛЕ его визита, невидимы до следующего круга (E-56).
# Фаза 1 (только неопрошенные, 447 082 игрока) завершена 09.08 в 12:08,
# корпус вырос до 5 107 269 матчей.
#
# Курсор не удаляется, а копируется рядом: откат = вернуть бэкап на место.
set -e
cd /root/main
source venv/bin/activate
export PYTHONUNBUFFERED=1
CUR=bets_data/analise_pub_matches/processed_ids_to_graph.txt
STAMP=$(date +%Y%m%d_%H%M)
if [ -s "$CUR" ]; then
  cp -p "$CUR" "${CUR}.bak_before_phase2_${STAMP}"
  : > "$CUR"
  echo "курсор сброшен, бэкап: ${CUR}.bak_before_phase2_${STAMP}"
fi
LOG="runtime/get_pubs_${STAMP}_phase2_full_recrawl.log"
nohup python3 -c 'import sys
sys.path.insert(0, "base")
import maps_research as mr
mr.get_pubs()' > "$LOG" 2>&1 &
PID=$!
echo "$PID" > runtime/get_pubs.pid
echo "PID=$PID"
echo "лог: $LOG"
