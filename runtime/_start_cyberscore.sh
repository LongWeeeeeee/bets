#!/bin/bash
set -e
cd /root/main/base
export DLTV_SOURCE_MODE=html
export USE_PROXY=false
export LIVE_PROXY_PREFLIGHT_ENABLED=false
export CYBERSCORE_CAMOUFOX_REQUIRE_PROXY=0
export PYTHONUNBUFFERED=1
exec /root/main/venv/bin/python3 -u cyberscore_try.py
