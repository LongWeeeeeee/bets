#!/bin/bash
# Совместимость: раньше здесь был pkill + ручной nohup-запуск через run_dltv2.sh
# (html-режим вместо продового sourcetv). Прод управляется systemd —
# делегируем каноническому restart-скрипту (docs/RUNTIME_RULES.md:13).
exec bash "$(cd "$(dirname "$0")" && pwd)/restart_cyberscore.sh" "$@"
