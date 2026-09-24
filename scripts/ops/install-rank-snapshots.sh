#!/bin/bash
# Ставит ежедневный снимок лидербордов Valve в расписание launchd (замена автоматизации
# Codex «dota-2», которая молча встала 22.09.2026; подробности в шапке
# scripts/run/collect_rank_snapshots.sh).
#
# launchd читает копию из ~/Library/LaunchAgents: плист в репозитории сам ничего не
# планирует. /bin/bash должен иметь «Полный доступ к диску» (у остальных com.ingame.*
# он уже есть), иначе джоба молча падает с exit 126.
set -eu
REPO=/Users/alex/Documents/ingame
LABEL=com.ingame.rank-snapshots
SRC="$REPO/scripts/ops/$LABEL.plist"
DST="$HOME/Library/LaunchAgents/$LABEL.plist"
cp "$SRC" "$DST"
plutil -lint "$DST" >/dev/null
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$DST"
launchctl print "gui/$(id -u)/$LABEL" | grep -E "state|program|runs" | head -5

echo
echo "поставлено: $LABEL, ежедневно в 09:00; повтор в те же сутки UTC пропускается"
echo "разовый прогон:  launchctl kickstart gui/$(id -u)/$LABEL"
echo "снять:           launchctl bootout gui/$(id -u)/$LABEL"
echo "лог прогона:     runtime/artifacts/misc/rank_snapshots/collect_<YYYYMMDD UTC>.log"
echo "прежнюю автоматизацию Codex «dota-2» стоит выключить в приложении Codex, чтобы при"
echo "его запуске она не дублировала сбор (дубль безвреден: скрипт и heartbeat проверяют"
echo "наличие снимка за сутки UTC)."
