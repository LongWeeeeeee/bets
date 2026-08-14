#!/bin/bash
# Ставит ночную пересборку снимка предматчевой модели в расписание launchd.
#
# Почему это отдельный скрипт, а не «положил плист и забыл»: launchd читает
# копию из ~/Library/LaunchAgents, и плист в репозитории сам по себе ничего не
# планирует. До 14.08.2026 `rebuild_prematch_snapshot.sh` существовал, но в
# расписании не стоял и ни разу не запускался — снимок обновлялся только руками.
#
# ТРЕБУЕТСЯ РАЗОВО: `/bin/bash` в списке «Полный доступ к диску». Без него агент
# launchd не может даже прочитать скрипт в ~/Documents (TCC, exit 126), причём
# промпт macOS фоновым агентам не показывает — отказ молчаливый. Обойти это со
# стороны кода нельзя: копия /bin/bash вне системного пути убивается сигналом
# KILL, других шеллов на машине нет, ssh на localhost выключен.
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
echo
# Проверка доступа: сам launchd читать ~/Documents не может, пока /bin/bash не
# получит Full Disk Access. Без этого джоба падает с exit 126 и МОЛЧА не
# обновляет снимок — то есть худший из возможных исходов.
launchctl kickstart "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
sleep 5
CODE=$(launchctl print "gui/$(id -u)/$LABEL" 2>/dev/null | awk '/last exit code/{print $NF}')
if [ "${CODE:-0}" = "126" ] || [ "${CODE:-0}" = "77" ]; then
  echo "!! джоба не может прочитать репозиторий (код $CODE)."
  echo "   Системные настройки -> Конфиденциальность и безопасность ->"
  echo "   Полный доступ к диску -> добавить /bin/bash, затем запустить установщик снова."
  echo "   Проверка вручную: launchctl kickstart gui/$(id -u)/$LABEL"
else
  echo "доступ есть, пробный прогон запущен (код выхода: ${CODE:-нет данных})"
fi
