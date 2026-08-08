#!/bin/bash
# Фаза 1: добор игроков, которых обход ещё НЕ касался.
# Курсор processed_ids_to_graph.txt НЕ трогаем — сбор сам вычтет пройденных и
# пойдёт только по новым (пул из part-файлов вырос после merge 06.08).
cd /root/main
source venv/bin/activate
export PYTHONUNBUFFERED=1
LOG="runtime/get_pubs_$(date +%Y%m%d_%H%M)_phase1_new_players.log"
nohup python3 -c 'import sys
sys.path.insert(0, "base")
import maps_research as mr
mr.get_pubs()' > "$LOG" 2>&1 &
echo "PID=$!"
echo "$!" > runtime/get_pubs.pid
echo "лог: $LOG"
