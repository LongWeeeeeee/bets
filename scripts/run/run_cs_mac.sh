#!/bin/bash
cd ~/Documents/ingame/base
source ../venv_mac/bin/activate
env LIVE_PROXY_PREFLIGHT_ENABLED=false USE_PROXY=false python3 -u cyberscore_try.py > ~/Documents/ingame/log.txt 2>&1
