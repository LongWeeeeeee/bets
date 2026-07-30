#!/bin/bash
cd /root/main
pkill -f cyberscore || true
sleep 2
rm -f ~/.local/state/ingame/map_id_check.txt
source venv/bin/activate
nohup python3 base/cyberscore_try.py --no-odds > cyberscore.log 2>&1 &
echo "Started with PID $!"
