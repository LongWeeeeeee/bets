#!/usr/bin/env bash
set -euo pipefail
cd /root/main
mkdir -p runtime ~/.local/state/ingame
rm -f ~/.local/state/ingame/map_id_check.txt
LOG="runtime/cyberscore_server_$(date +%Y%m%d_%H%M%S).log"
PID="runtime/cyberscore_server.pid"
echo "$LOG" > runtime/cyberscore_server.logpath
setsid bash -lc "cd /root/main && source venv/bin/activate && export PYTHONUNBUFFERED=1 && exec python3 base/cyberscore_try.py --no-odds" > "$LOG" 2>&1 < /dev/null &
echo $! > "$PID"
echo "PID=$(cat "$PID")"
echo "LOG=$LOG"
