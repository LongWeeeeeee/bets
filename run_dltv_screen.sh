#!/bin/bash
pkill -f python3.*cyberscore_try || true
sleep 2
cd /root/main/base
source ../venv/bin/activate
env DLTV_SOURCE_MODE=html \
    USE_PROXY=false \
    LIVE_PROXY_PREFLIGHT_ENABLED=false \
    CYBERSCORE_CAMOUFOX_REQUIRE_PROXY=0 \
    python3 -u cyberscore_try.py > /root/main/log_dltv.txt 2>&1
