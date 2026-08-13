#!/usr/bin/env bash
# Словари (5 sqlite3, 12.9 ГБ) на serv2. Стартует ПОСЛЕ заливки корпусов, чтобы
# не делить канал: обе задачи упираются в него, а не в диск.
#
# Кладём на внешний SSD и симлинкуем из репозитория — так велит раскладка serv2
# (внутренний диск ~54 ГБ, туда уже уехали 10 ГБ про-корпуса; словари и корпуса
# живут на /Volumes/Let_me, симлинк остаётся на внутреннем APFS).
# Пути внутри репозитория при этом те же, что локально, — код не правится.
set -u
cd /Users/alex/Documents/ingame
SRC=bets_data/analise_pub_matches/_rebuild_20260809_1320
REMOTE_REPO=/Users/alexford/ingame/bets_data/analise_pub_matches
REMOTE_SSD=/Volumes/Let_me/ingame_dicts_20260809_1320
RS="rsync -a --partial --stats -e ssh"

ts() { date +%H:%M:%S; }

if [ -f runtime/artifacts/misc/sync_serv2_data.pid ]; then
  p=$(cat runtime/artifacts/misc/sync_serv2_data.pid)
  echo "[$(ts)] жду окончания заливки корпусов (PID $p)"
  while kill -0 "$p" 2>/dev/null; do sleep 30; done
fi

echo "[$(ts)] === место перед словарями ==="
ssh serv2 "df -h / /Volumes/Let_me | tail -2"

echo "[$(ts)] === заливаю 5 словарей (12.9 ГБ) на внешний SSD ==="
ssh serv2 "mkdir -p $REMOTE_SSD"
$RS "$SRC/" "serv2:$REMOTE_SSD/" || { echo "СЛОВАРИ: ОШИБКА rc=$?"; exit 1; }

echo "[$(ts)] === симлинк из репозитория ==="
# Существующий каталог не трогаем и не удаляем: если он там уже есть — сообщаем.
ssh serv2 "if [ -e $REMOTE_REPO/_rebuild_20260809_1320 ] && [ ! -L $REMOTE_REPO/_rebuild_20260809_1320 ]; then
  echo 'ВНИМАНИЕ: в репозитории уже есть НЕ-симлинк, оставляю как есть'
else
  ln -sfn $REMOTE_SSD $REMOTE_REPO/_rebuild_20260809_1320
  echo \"симлинк: \$(ls -ld $REMOTE_REPO/_rebuild_20260809_1320)\"
fi"

echo "[$(ts)] === сверка ==="
ssh serv2 "ls -la $REMOTE_SSD | awk 'NR>3 {printf \"  %.2fГБ %s\n\",\$5/1073741824,\$9}'
echo 'через симлинк видно:'; ls $REMOTE_REPO/_rebuild_20260809_1320/ 2>/dev/null | head
df -h / /Volumes/Let_me | tail -2"
echo "[$(ts)] СЛОВАРИ ЗАЛИТЫ"
