#!/bin/bash
# Сетевой сторож для serv2 (MacBook Air M1, Wi-Fi only).
# Раз в CHECK_EVERY секунд пингует шлюз; после FAIL_LIMIT подряд неудач
# передёргивает Wi-Fi (power off/on) и обновляет DHCP-аренду.
# Ставится LaunchDaemon-ом com.ingame.netwatchdog (RunAtLoad + KeepAlive).
#
# Лог: /var/log/ingame_net_watchdog.log

set -u

IFACE="${IFACE:-en0}"
SERVICE="${SERVICE:-Wi-Fi}"
CHECK_EVERY="${CHECK_EVERY:-60}"
FAIL_LIMIT="${FAIL_LIMIT:-5}"
LOG="${LOG:-/var/log/ingame_net_watchdog.log}"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >> "$LOG"; }

gateway() { route -n get default 2>/dev/null | awk '/gateway:/ {print $2; exit}'; }

fails=0
ticks=0
HEARTBEAT_EVERY="${HEARTBEAT_EVERY:-10}"   # каждые N проверок — строка «жив»,
                                           # чтобы после пропажи было видно,
                                           # спала машина или висела сеть
log "watchdog started (iface=$IFACE service=$SERVICE every=${CHECK_EVERY}s limit=$FAIL_LIMIT)"

while true; do
    ticks=$((ticks + 1))
    if [ $((ticks % HEARTBEAT_EVERY)) -eq 0 ]; then
        log "alive ip=$(ipconfig getifaddr "$IFACE" 2>/dev/null || echo none) uptime=$(uptime | sed 's/^ *//')"
    fi
    gw="$(gateway)"
    if [ -n "$gw" ] && ping -c 2 -W 2000 -t 5 "$gw" >/dev/null 2>&1; then
        if [ "$fails" -gt 0 ]; then
            log "link ok again (gw=$gw) after $fails fail(s)"
        fi
        fails=0
    else
        fails=$((fails + 1))
        log "ping fail #$fails (gw=${gw:-none})"
        if [ "$fails" -ge "$FAIL_LIMIT" ]; then
            log "cycling $SERVICE ($IFACE)"
            networksetup -setairportpower "$IFACE" off 2>>"$LOG"
            sleep 5
            networksetup -setairportpower "$IFACE" on 2>>"$LOG"
            sleep 20
            ipconfig set "$IFACE" DHCP 2>>"$LOG"
            log "cycle done, ip=$(ipconfig getifaddr "$IFACE" 2>/dev/null || echo none)"
            fails=0
        fi
    fi
    sleep "$CHECK_EVERY"
done
