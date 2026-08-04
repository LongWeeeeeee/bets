#!/bin/bash
# Бэкап тяжёлых данных serv1, которых НЕТ в git, на локальную машину.
# Запускать С MAC (не на сервере):  bash scripts/ops/backup-heavy.sh [--dry-run]
#
# Что делает:
#   1. sqlite-словари копирует консистентно (sqlite3 .backup) во временный файл на сервере —
#      прод при этом продолжает читать оригинал, останавливать ничего не нужно;
#   2. тянет снимок в ~/Backups/serv1/<дата>/, повторяющиеся файлы жёстко линкует
#      на предыдущий снимок (--link-dest), поэтому второй бэкап занимает только дельту;
#   3. проверяет целостность скачанных sqlite (PRAGMA quick_check);
#   4. хранит последние $KEEP снимков.
#
# В git этих данных нет и не должно быть: 33 ГБ словарей паблика, датасеты, модели.
set -uo pipefail

HOST="${BACKUP_HOST:-serv1}"
DEST_ROOT="${BACKUP_DEST:-$HOME/Backups/serv1}"
KEEP="${BACKUP_KEEP:-2}"
# базы, которые НЕ бэкапим: экспериментальный no10gate-early и симлинк-дубль post_lane
# (реальные данные post-lane лежат в post_lane_dict_raw_no10gate.sqlite3 и бэкапятся)
EXCLUDE_DBS="${BACKUP_EXCLUDE:-early_dict_raw_no10gate.sqlite3 post_lane_dict_raw.sqlite3}"
DRY=""; [ "${1:-}" = "--dry-run" ] && DRY="--dry-run"

STAMP="$(date +%Y%m%d_%H%M%S)"
DEST="$DEST_ROOT/$STAMP"
[ -n "$DRY" ] && echo "(сухой прогон: каталог снимка не создаётся)"
PREV="$(ls -1d "$DEST_ROOT"/2* 2>/dev/null | tail -1)"
LINK=""; [ -n "$PREV" ] && LINK="--link-dest=$PREV"

# каталоги, которые копируются как есть
PLAIN_DIRS=(
  "/root/main/pro_heroes_data"
  "/root/main/data"
  "/root/main/ml_dataset"
  "/root/main/base/ml_dataset"
  "/root/main/output"
  "/root/main/ELO/output"
  "/root/main/bets_data/tempo_pub_experiment"
)

echo "== бэкап serv1 -> $DEST ${PREV:+(дельта к $(basename "$PREV"))}"
[ -z "$DRY" ] && mkdir -p "$DEST"

# --- 1. sqlite: консистентный снимок на сервере, затем перенос по одному файлу
# bash 3.2 (macOS) не умеет mapfile — собираем список построчно.
# readlink -f на сервере схлопывает симлинки: post_lane_dict_raw.sqlite3 указывает на
# no10gate_full_dicts/post_lane_dict_raw_no10gate.sqlite3, без дедупликации это лишние 5 ГБ.
DBS=()
while IFS= read -r line; do
  [ -n "$line" ] && DBS+=("$line")
done < <(ssh "$HOST" 'ls -1 /root/main/bets_data/analise_pub_matches/*.sqlite3 /root/main/bets_data/analise_pub_matches/*/*.sqlite3 2>/dev/null | xargs -r -I{} readlink -f {} | sort -u')
echo "-- sqlite-словарей: ${#DBS[@]} (симлинки схлопнуты)"
[ -z "$DRY" ] && mkdir -p "$DEST/bets_data"
for db in "${DBS[@]}"; do
  [ -n "$db" ] || continue
  name="$(basename "$db")"
  skip=0
  for ex in $EXCLUDE_DBS; do [ "$name" = "$ex" ] && skip=1; done
  if [ "$skip" -eq 1 ]; then echo "   пропуск $name (в списке исключений)"; continue; fi
  free_needed="$(ssh "$HOST" "stat -c %s '$db' 2>/dev/null || echo 0")"
  free_have="$(ssh "$HOST" "df --output=avail -B1 / | tail -1")"
  if [ "$free_have" -lt "$((free_needed + 2000000000))" ]; then
    echo "   ПРОПУСК $name: на сервере мало места под временный снимок" >&2; continue
  fi
  echo "   снимок $name ($((free_needed/1024/1024)) МБ)"
  if [ -z "$DRY" ]; then
    ssh "$HOST" "mkdir -p /root/backup_tmp && sqlite3 '$db' \".backup /root/backup_tmp/$name\" && touch -r '$db' /root/backup_tmp/$name" || { echo "   ошибка снимка $name" >&2; continue; }
    rsync -a --partial --stats $LINK "$HOST:/root/backup_tmp/$name" "$DEST/bets_data/" || echo "   ошибка переноса $name" >&2
    ssh "$HOST" "rm -f /root/backup_tmp/$name"
  fi
done
[ -z "$DRY" ] && ssh "$HOST" "rmdir /root/backup_tmp 2>/dev/null" || true
# карта симлинков: при восстановлении их надо воссоздать, а не копировать файл дважды
if [ -z "$DRY" ]; then
  ssh "$HOST" 'find /root/main/bets_data/analise_pub_matches -maxdepth 2 -type l -printf "%p -> %l\n"' \
    > "$DEST/bets_data/SYMLINKS.txt" 2>/dev/null
  [ -s "$DEST/bets_data/SYMLINKS.txt" ] && echo "   карта симлинков: $(wc -l < "$DEST/bets_data/SYMLINKS.txt") шт."
fi

# --- 2. обычные каталоги
for d in "${PLAIN_DIRS[@]}"; do
  echo "-- $d"
  sub="$DEST${d#/root/main}"
  [ -z "$DRY" ] && mkdir -p "$(dirname "$sub")"
  rsync -a --stats $DRY $LINK "$HOST:$d/" "$sub/" 2>&1 | grep -E "Number of files|Total transferred" | head -2 || echo "   пропущен" >&2
done

# --- 3. проверка целостности скачанных sqlite
if [ -z "$DRY" ] && command -v sqlite3 >/dev/null; then
  echo "-- проверка целостности"
  bad=0
  for f in "$DEST"/bets_data/*.sqlite3; do
    [ -f "$f" ] || continue
    r="$(sqlite3 "$f" 'PRAGMA quick_check;' 2>&1 | head -1)"
    [ "$r" = "ok" ] || { echo "   ПОВРЕЖДЁН: $(basename "$f") -> $r" >&2; bad=1; }
  done
  [ "$bad" -eq 0 ] && echo "   все sqlite целы"
fi

# --- 4. ротация
if [ -z "$DRY" ]; then
  # оставить последние $KEEP снимков (head -n -N есть только в GNU coreutils)
  ls -1d "$DEST_ROOT"/2* 2>/dev/null | sort | awk -v k="$KEEP" '{a[NR]=$0} END {for (i=1; i<=NR-k; i++) print a[i]}' | while read -r old; do
    echo "-- удаляю старый снимок $(basename "$old")"; rm -rf "$old"
  done
  echo "== готово: $(du -sh "$DEST" | cut -f1) в $DEST"
  du -sh "$DEST_ROOT" 2>/dev/null | awk '{print "   всего под бэкапами: "$1}'
fi
