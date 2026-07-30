#!/bin/bash
# Live draft / signals tail. Usage: draft_live.sh [match_id] [N]
LOG=/root/main/base/runtime/cyberscore_sourcetv.log
N=${2:-120}
if [[ -n "$1" ]]; then
  grep -nE "Series ID: $1|драфт|Драфт|Draft|lane_adv|ProTracker|Star|Сигнал|sent_signal|ВЕРДИКТ|Early signal|All signal|Late signal|ELO WR|Tier|Lead:|Score:|early_winner|kills_window" "$LOG" | grep "$1" | tail -"$N"
else
  tail -"$N" "$LOG" | grep -nE 'Series ID|драфт|Драфт|Draft|lane_adv|ProTracker|Star|Сигнал|sent_signal|ВЕРДИКТ|Early signal|All signal|Late signal|ELO WR|Lead:|Score:|early_winner|kills_window|⛔|✅|📌'
fi
