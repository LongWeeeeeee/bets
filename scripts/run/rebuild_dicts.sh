#!/bin/bash
# Пересборка словарей на serv1 по новым правилам, ПО ОДНОЙ метрике за проход.
# Четыре метрики разом на serv2 упёрлись в память (RSS 4.4 ГБ при 8 ГБ, CPU 28%,
# то есть ждал диск и подкачку). Здесь 15 ГБ памяти и по одной метрике —
# RSS падает вчетверо, sqlite-сбросы реже.
set -u
cd /root/main
source venv/bin/activate
export PYTHONUNBUFFERED=1
STAMP=$(date +%Y%m%d_%H%M)
OUT=/root/main/bets_data/analise_pub_matches/_rebuild_$STAMP
mkdir -p "$OUT"
LOG=runtime/rebuild_$STAMP.log

say() { echo "[$(date +%H:%M:%S)] $*"; }
say "=== пересборка в $OUT ===" 
say "правила: early NW только маркер | early_end FF34+гейт500 | late 36 без равенства | post_lane 20"

for METRIC in early early_end late post_lane; do
  say "--- $METRIC"
  EXPLORE_METRICS=$METRIC \
  EXPLORE_STATS_DIR="$OUT" \
  EXPLORE_JSON_DIR=/root/main/bets_data/analise_pub_matches/json_parts_split_from_object \
  EXPLORE_TEST_SET_PATH=/root/main/bets_data/analise_pub_matches/_traintest_holdout_50k.json \
  EXPLORE_FLUSH_MATCHES=20000 \
  EXPLORE_FLUSH_KEYS=4000000 \
  python3 base/explore_database.py 2>&1 | tail -3
  say "    готово: $(ls -la $OUT/${METRIC}_dict_raw.sqlite3 2>/dev/null | awk '{printf "%.0f МБ", $5/1048576}')"
done

say "=== ИТОГ ==="
ls -la "$OUT"/*.sqlite3 2>/dev/null | awk '{printf "  %6.0f МБ  %s\n", $5/1048576, $9}'
df -h / | tail -1
say "=== ПЕРЕСБОРКА ЗАВЕРШЕНА ==="
