#!/bin/bash
# Ставит сторожа расхождений serv1 в расписание launchd.
#
# launchd читает копию из ~/Library/LaunchAgents — плист в репозитории сам по
# себе ничего не планирует. На этом уже обжигались: rebuild_prematch_snapshot.sh
# существовал, но в расписании не стоял и ни разу не запускался.
#
# ТРЕБУЕТСЯ РАЗОВО: `/bin/bash` в списке «Полный доступ к диску». Без него агент
# launchd не может прочитать скрипт в ~/Documents (TCC, exit 126), причём промпт
# macOS фоновым агентам не показывает — отказ молчаливый.
set -eu
REPO=/Users/alex/Documents/ingame
LABEL=com.ingame.serv1-drift
SRC="$REPO/scripts/ops/$LABEL.plist"
DST="$HOME/Library/LaunchAgents/$LABEL.plist"
cp "$SRC" "$DST"
plutil -lint "$DST" >/dev/null
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$DST"
launchctl print "gui/$(id -u)/$LABEL" | grep -E "state|program|next fire|runs" | head -6

echo
echo "поставлено: $LABEL, ежедневно в 05:00"
echo "разовый прогон:  launchctl kickstart gui/$(id -u)/$LABEL"
echo "снять:           launchctl bootout gui/$(id -u)/$LABEL"
echo "лог прогона:     runtime/serv1_drift_*.log"
echo "ветка снимков:   git log --oneline serv1-prod-snapshot"
echo
echo "ЧТО ЗНАЧИТ ВЫВОД: строки вида '+N -M файл' — боевой код изменился с"
echo "прошлого снимка. Это НЕ сигнал что-то выкатывать: машины разошлись форком,"
echo "и синхронизация в любую сторону что-то ломает. Разбирать по одному файлу,"
echo "см. docs/SERV1_SNAPSHOT.md."
echo

# Проверка доступа: launchd падает с exit 126 МОЛЧА, если /bin/bash не имеет
# Full Disk Access — джоба стоит в расписании, а работы не делает.
launchctl kickstart "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
sleep 8
CODE=$(launchctl print "gui/$(id -u)/$LABEL" 2>/dev/null | awk '/last exit code/{print $NF}')
if [ "${CODE:-0}" = "126" ] || [ "${CODE:-0}" = "77" ]; then
  echo "!! джоба не может прочитать репозиторий (код $CODE)."
  echo "   Системные настройки -> Конфиденциальность и безопасность ->"
  echo "   Полный доступ к диску -> добавить /bin/bash, затем запустить установщик снова."
else
  echo "доступ есть, пробный прогон запущен (код выхода: ${CODE:-идёт})"
fi
