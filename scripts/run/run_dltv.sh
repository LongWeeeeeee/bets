#!/bin/bash
set -e
cd /root/main/base
source ../venv/bin/activate

# убиваем старые
pkill -f cyberscore_try || true
sleep 2

env DLTV_SOURCE_MODE=html USE_PROXY=false LIVE_PROXY_PREFLIGHT_ENABLED=false \
  python3 -u cyberscore_try.py > /root/main/log_dltv.txt 2>&1
