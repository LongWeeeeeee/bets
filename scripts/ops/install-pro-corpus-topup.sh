#!/bin/bash
# Ставит ночной добор про-корпуса в расписание launchd.
#
# Почему это отдельный скрипт: launchd читает копию из ~/Library/LaunchAgents,
# плист в репозитории сам по себе ничего не планирует. Ровно на этом уже
# обжигались — `rebuild_prematch_snapshot.sh` существовал, но в расписании не
# стоял и ни разу не запускался.
#
# ТРЕБУЕТСЯ РАЗОВО: `/bin/bash` в списке «Полный доступ к диску». Без него агент
# launchd не может прочитать скрипт в ~/Documents (TCC, exit 126), причём промпт
# macOS фоновым агентам не показывает — отказ молчаливый.
set -eu
REPO=/Users/alex/Documents/ingame
LABEL=com.ingame.pro-corpus-topup
SRC="$REPO/scripts/ops/$LABEL.plist"
DST="$HOME/Library/LaunchAgents/$LABEL.plist"
cp "$SRC" "$DST"
plutil -lint "$DST" >/dev/null
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$DST"
launchctl print "gui/$(id -u)/$LABEL" | grep -E "state|program|next fire|runs" | head -6

echo
echo "поставлено: $LABEL, ежедневно в 04:30 (за час до пересборки снимка)"
echo "разовый прогон:  launchctl kickstart gui/$(id -u)/$LABEL"
echo "снять:           launchctl bootout gui/$(id -u)/$LABEL"
echo "лог прогона:     runtime/pro_topup_*.log"
echo
echo "ПРОВЕРКА ПОСЛЕ ПЕРВОГО ПРОГОНА: в логе должна быть строка «свежайшая карта»"
echo "с отставанием меньше суток. Строка «ВНИМАНИЕ: корпус отстаёт» означает, что"
echo "добор отработал, но карт не принёс — смотреть ключи Stratz и сид-списки."
echo

# Проверка доступа: launchd не может читать ~/Documents, пока /bin/bash не получит
# Full Disk Access, и падает с exit 126 МОЛЧА — джоба стоит в расписании, а
# корпус не пополняется. Именно молчаливость таких отказов и стоила пяти суток
# простоя корпуса в августе, поэтому проверяем сразу, а не «когда-нибудь утром».
launchctl kickstart "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
sleep 8
CODE=$(launchctl print "gui/$(id -u)/$LABEL" 2>/dev/null | awk '/last exit code/{print $NF}')
if [ "${CODE:-0}" = "126" ] || [ "${CODE:-0}" = "77" ]; then
  echo "!! джоба не может прочитать репозиторий (код $CODE)."
  echo "   Системные настройки -> Конфиденциальность и безопасность ->"
  echo "   Полный доступ к диску -> добавить /bin/bash, затем запустить установщик снова."
else
  echo "доступ есть, пробный прогон запущен (код выхода: ${CODE:-идёт})"
  echo "следить: tail -f runtime/pro_topup_*.log"
fi
