#!/bin/bash
# Ставит ночную пересборку снимка предматчевой модели в расписание launchd.
#
# Почему это отдельный скрипт, а не «положил плист и забыл»: launchd читает
# копию из ~/Library/LaunchAgents, и плист в репозитории сам по себе ничего не
# планирует. До 14.08.2026 `rebuild_prematch_snapshot.sh` существовал, но в
# расписании не стоял и ни разу не запускался — снимок обновлялся только руками.
set -eu
REPO=/Users/alex/Documents/ingame
LABEL=com.ingame.prematch-refresh
SRC="$REPO/scripts/ops/$LABEL.plist"
DST="$HOME/Library/LaunchAgents/$LABEL.plist"

cp "$SRC" "$DST"
plutil -lint "$DST" >/dev/null
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$DST"
launchctl print "gui/$(id -u)/$LABEL" | grep -E "state|program|next fire|runs" | head -6

echo
echo "поставлено: $LABEL, ежедневно в 05:30"
echo "разовый прогон:  launchctl kickstart gui/$(id -u)/$LABEL"
echo "снять:           launchctl bootout gui/$(id -u)/$LABEL"
echo "лог прогона:     runtime/prematch_rebuild_*.log"
