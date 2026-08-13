#!/bin/bash
# Датчик доступности serv2 со стороны клиента (локальный Mac).
# Раз в CHECK_EVERY секунд проверяет три уровня и пишет строку в лог
# ТОЛЬКО при смене состояния (плюс контрольная строка раз в KEEP_EVERY проверок):
#   ping  — жив ли IP в LAN;
#   mdns  — резолвится ли имя (поймает переезд адреса);
#   ssh   — принимает ли sshd (отделит «сеть цела, но не пускает»);
#   tun   — доступен ли serv2 через обратный туннель на serv1, то есть в обход
#           домашней сети (отделит «умерла локалка» от «умерла машина»).
#
# Ставится LaunchAgent-ом com.ingame.serv2probe.
# Лог: ~/Library/Logs/ingame_serv2_probe.log

set -u

HOST_IP="${HOST_IP:-192.168.31.96}"
HOST_MDNS="${HOST_MDNS:-alexs-MacBook-Air.local}"
SSH_TARGET="${SSH_TARGET:-serv2}"
TUN_TARGET="${TUN_TARGET:-serv2t}"
CHECK_EVERY="${CHECK_EVERY:-60}"
KEEP_EVERY="${KEEP_EVERY:-30}"
LOG="${LOG:-$HOME/Library/Logs/ingame_serv2_probe.log}"
MAX_BYTES="${MAX_BYTES:-2000000}"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >> "$LOG"; }

prev=""
ticks=0
log "probe started (ip=$HOST_IP mdns=$HOST_MDNS ssh=$SSH_TARGET tun=$TUN_TARGET every=${CHECK_EVERY}s)"

while true; do
    ticks=$((ticks + 1))

    if ping -c 2 -W 1500 -t 5 "$HOST_IP" >/dev/null 2>&1; then p=ok; else p=FAIL; fi

    mdns_ip="$(ping -c 1 -W 1500 -t 3 "$HOST_MDNS" 2>/dev/null \
                 | sed -n '1s/.*(\([0-9.]*\)).*/\1/p')"
    [ -n "$mdns_ip" ] || mdns_ip=none

    if ssh -o BatchMode=yes -o ConnectTimeout=8 "$SSH_TARGET" true >/dev/null 2>&1; then
        s=ok
    else
        s=FAIL
    fi

    # обратный туннель через serv1: жив ли serv2 в обход домашней сети
    if ssh -o BatchMode=yes -o ConnectTimeout=15 "$TUN_TARGET" true >/dev/null 2>&1; then
        t=ok
    else
        t=FAIL
    fi

    cur="ping=$p mdns=$mdns_ip ssh=$s tun=$t"
    if [ "$cur" != "$prev" ]; then
        log "СМЕНА: $cur (было: ${prev:-старт})"
        prev="$cur"
    elif [ $((ticks % KEEP_EVERY)) -eq 0 ]; then
        log "держится: $cur"
    fi

    if [ -f "$LOG" ] && [ "$(stat -f%z "$LOG" 2>/dev/null || echo 0)" -gt "$MAX_BYTES" ]; then
        tail -n 2000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
    fi

    sleep "$CHECK_EVERY"
done
