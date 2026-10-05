#!/bin/bash
# Installs the Winline team kills-total collector timer on serv1 (run ON serv1 as root).
# Owner decision 05.10.2026 (board ingame-nbpb): prematch event pages only, every 3 hours;
# Winline only through a project proxy (the collector fails closed). History:
# /root/main/runtime/winline_kills_totals_history.jsonl; log: runtime/artifacts/odds-winline/kills_totals_collector.log
# Remove: systemctl disable --now winline-kills-totals.timer
set -eu
REPO=/root/main
mkdir -p "$REPO/runtime/artifacts/odds-winline"
install -m 0644 "$REPO/scripts/ops/systemd/winline-kills-totals.service" /etc/systemd/system/winline-kills-totals.service
install -m 0644 "$REPO/scripts/ops/systemd/winline-kills-totals.timer" /etc/systemd/system/winline-kills-totals.timer
systemd-analyze verify /etc/systemd/system/winline-kills-totals.service /etc/systemd/system/winline-kills-totals.timer
systemctl daemon-reload
systemctl enable --now winline-kills-totals.timer
systemctl list-timers winline-kills-totals.timer --no-pager | head -3
