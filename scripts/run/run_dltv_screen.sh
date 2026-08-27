#!/bin/bash
# Служба cyberscore.service идёт под systemd с Restart=always: без остановки
# он через RestartSec поднимет боевой экземпляр рядом с тем, что запускает
# этот скрипт, и два бота будут работать в разных режимах одновременно.
systemctl stop cyberscore.service 2>/dev/null || true
sleep 1

pkill -f python3.*cyberscore_try || true
sleep 2
cd /root/main/base
source ../venv/bin/activate
env DLTV_SOURCE_MODE=html \
    USE_PROXY=false \
    LIVE_PROXY_PREFLIGHT_ENABLED=false \
    CYBERSCORE_CAMOUFOX_REQUIRE_PROXY=0 \
    python3 -u cyberscore_try.py > /root/main/log_dltv.txt 2>&1
