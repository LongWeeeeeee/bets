#!/bin/bash
# Ежедневный снимок четырёх региональных лидербордов Valve (europe/se_asia/americas/china)
# для исследования рангов игроков (карточка ingame-enwj, E-295).
#
# Зачем отдельная джоба launchd. До 24.09.2026 снимки собирала автоматизация Codex
# «dota-2» (heartbeat в 09:00, ~/.codex/automations/dota-2). Она молча перестала
# работать: 22.09 запрос к модели треда вернул 403 Forbidden, и ход кончился без вызова
# сборщика; с 23.09 приложение Codex закрыто, и heartbeat не срабатывает вовсе.
# Снимков за 22–23.09 нет и не будет: Valve отдаёт только текущую таблицу.
# Сбору не нужен LLM-агент — достаточно одного вызова python.
#
# DarkWake (карточка ingame-qe6y, 10.10.2026). 07, 08 и 10.10 прогон 09:00 стартовал
# в тёмном пробуждении Mac на батарее (pmset: «DarkWake from Deep Idle … Using BATT»,
# обратно в сон через ~2 с): DNS/TLS падали (gaierror, SSLEOFError), сборщик звался
# один раз, 07 и 08.10 потеряны навсегда, и их тревоги не ушли по той же мёртвой сети.
# Теперь: (1) ждём сеть до RANK_SNAPSHOT_NET_TRIES×RANK_SNAPSHOT_NET_SLEEP секунд;
# (2) до RANK_SNAPSHOT_ATTEMPTS попыток сборщика с паузой RANK_SNAPSHOT_RETRY_SLEEP;
# (3) launchd зовёт скрипт в 09/12/15/18/21 по местному времени (MSK = UTC+3,
# т.е. 06/09/12/15/18 UTC); (4) провал до 18:00 UTC — только строка в логе
# («deferred»): следующий слот тех же суток UTC повторит сбор. Тревога — только когда
# слотов в этих сутках UTC больше нет и снимок за день потерян.
#
# Повторный запуск в те же сутки UTC ничего не делает: если за текущую дату UTC уже
# есть collection.json с complete=true, сборщик не зовётся.
# Тишина ≠ успех: успех и потерянный день — строка в админ-чат (scripts/ops/notify_admin.py).
# Переменные RANK_SNAPSHOT_* — швы для теста base/tests/test_collect_rank_snapshots_runner.py.
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
OUT=${RANK_SNAPSHOT_OUT:-runtime/artifacts/misc/rank_snapshots}
COLLECT_CMD=${RANK_SNAPSHOT_COLLECT_CMD:-$PY -m base.tools.collect_rank_snapshots}
NOTIFY_CMD=${RANK_SNAPSHOT_NOTIFY_CMD:-$PY scripts/ops/notify_admin.py}
NET_CHECK_CMD=${RANK_SNAPSHOT_NET_CHECK_CMD:-curl -sS -o /dev/null -m 15 https://www.dota2.com/}
NET_TRIES=${RANK_SNAPSHOT_NET_TRIES:-20}
NET_SLEEP=${RANK_SNAPSHOT_NET_SLEEP:-30}
ATTEMPTS=${RANK_SNAPSHOT_ATTEMPTS:-3}
RETRY_SLEEP=${RANK_SNAPSHOT_RETRY_SLEEP:-120}
# Последний слот launchd — 21:00 MSK = 18:00 UTC; провал в нём или позже = день потерян.
# Допущение: часовой пояс Mac — MSK (UTC+3, без перехода на летнее время). При смене
# пояса поправить порог или слоты plist; страховка — проверка вчерашних суток в начале.
LAST_SLOT_UTC_HOUR=${RANK_SNAPSHOT_LAST_SLOT_UTC_HOUR:-18}
mkdir -p "$OUT"
LOG="$OUT/collect_$(date -u +%Y%m%d).log"

# $1 — дата UTC в формате YYYY-MM-DD (по умолчанию сегодня UTC).
have_complete_on() {
  $PY - "$OUT" "${1:-}" <<'EOF'
import json, sys, glob, datetime as dt
day = (dt.date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2]
       else dt.datetime.now(dt.timezone.utc).date())
for f in glob.glob(sys.argv[1] + "/*/collection.json"):
    try:
        j = json.load(open(f))
    except Exception:
        continue
    t = j.get("started_at")
    if j.get("complete") is True and t and dt.datetime.fromtimestamp(float(t), dt.timezone.utc).date() == day:
        print(f)
        break
EOF
}

have_complete_today() { have_complete_on "$(date -u +%F)"; }

# Отправить строку из stdin; код 0 только при подтверждённой доставке. notify_admin.py
# всегда выходит с 0 (не ломает вызывающего), поэтому доставку видно лишь по строке
# «notify_admin: ok» (astra 10.10: маркер тревоги ставился и при упавшей отправке).
notify() {
  local out
  out=$($NOTIFY_CMD 2>&1)
  echo "$out"
  echo "$out" | grep -q "notify_admin: ok"
}

# Страховка (hard-verifier 10.10, LOW a/b): если слот 21:00 проспан и launchd догнал
# его уже после 00:00 UTC, провал «сегодняшнего» прогона не скажет, что вчерашние сутки
# потеряны. Поэтому каждый прогон сначала проверяет вчерашние сутки UTC: нет полного
# снимка и тревоги о нём ещё не было -> одна строка ⚠️ и маркер .lost_alerted_<дата>.
# $1 — дата UTC старта прогона; «вчера» считается от неё, а не от часов в момент проверки.
alert_lost_yesterday() {
  local y
  y=$($PY -c 'import datetime as d, sys; print(d.date.fromisoformat(sys.argv[1]) - d.timedelta(days=1))' "$1")
  [ -e "$OUT/.lost_alerted_$y" ] && return 0
  [ -n "$(have_complete_on "$y")" ] && return 0
  # Маркер только после подтверждённой доставки: в DarkWake отправка падает, и
  # следующий запуск обязан повторить тревогу.
  echo "⚠️ снимок рангов Valve за $y UTC потерян: за сутки нет ни одного полного снимка ($(date '+%F %T'))" \
    | notify && : > "$OUT/.lost_alerted_$y"
  return 0
}

wait_network() {
  local i
  for ((i = 1; i <= NET_TRIES; i++)); do
    if $NET_CHECK_CMD; then
      [ "$i" -gt 1 ] && echo "network ready after $i checks"
      return 0
    fi
    echo "network not ready (check $i/$NET_TRIES), sleep ${NET_SLEEP}s"
    sleep "$NET_SLEEP"
  done
  return 1
}

run_collect() {
  # Час старта, а не конца: прогон, уснувший вместе с Mac и проснувшийся после 18 UTC,
  # не должен объявлять сутки потерянными, пока слот 21:00 ещё впереди.
  # Дата и час фиксируются вместе (astra 10.10): прогон, начатый в 18 UTC и уснувший до
  # 01 UTC, должен назвать потерянными сутки старта, а не следующие.
  local start_utc_hour=${RANK_SNAPSHOT_UTC_HOUR:-$(date -u +%H)}
  local start_utc_date=${RANK_SNAPSHOT_UTC_DATE:-$(date -u +%F)}
  alert_lost_yesterday "$start_utc_date"
  local have
  have=$(have_complete_on "$start_utc_date")
  if [ -n "$have" ]; then
    echo "skip: complete snapshot for today UTC already exists: $have"
    return 0
  fi
  local rc=1 attempt
  for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    if ! wait_network; then
      echo "attempt $attempt: network still down after $NET_TRIES checks"
      rc=75
    else
      rc=0
      $COLLECT_CMD --output "$OUT" || rc=$?
      echo "attempt $attempt: collect_exit=$rc"
      [ -n "$(have_complete_today)" ] && break
      [ "$rc" -eq 0 ] && rc=1
    fi
    [ "$attempt" -lt "$ATTEMPTS" ] && sleep "$RETRY_SLEEP"
  done
  local summary
  summary=$($PY - "$OUT" <<'EOF'
import json, sys, glob, os
fs = sorted(glob.glob(sys.argv[1] + "/*/collection.json"), key=os.path.getmtime)
if not fs:
    print("нет collection.json"); sys.exit()
j = json.load(open(fs[-1]))
reg = j.get("regions", {})
rows = sum(int(v.get("players") or 0) for v in reg.values())
bad = [k for k, v in reg.items() if v.get("status") != "ok" or v.get("http_status") != 200]
print(f"complete={j.get('complete')} регионов={len(reg)} строк={rows} сбойные={bad or 'нет'}")
EOF
)
  echo "$summary"
  local ok=0
  if [ "$rc" -eq 0 ] && echo "$summary" | grep -q "complete=True регионов=4" && echo "$summary" | grep -q "сбойные=нет"; then
    ok=1
  fi
  if [ "$ok" -eq 1 ]; then
    echo "✅ снимок рангов Valve (попыток $attempt): $summary" | notify
    return 0
  fi
  [ "$rc" -eq 0 ] && rc=1
  if [ "$((10#$start_utc_hour))" -lt "$LAST_SLOT_UTC_HOUR" ]; then
    echo "deferred: $ATTEMPTS attempts failed (started ${start_utc_hour}h UTC); a later launchd slot today retries, no alert"
  elif [ -e "$OUT/.lost_alerted_$start_utc_date" ]; then
    echo "lost-day alert for $start_utc_date already delivered; no duplicate"
  else
    { echo "⚠️ снимок рангов Valve за $start_utc_date UTC потерян: rc=$rc ($(date '+%F %T'))"; echo "$summary"; tail -3 "$LOG"; } \
      | notify && : > "$OUT/.lost_alerted_$start_utc_date"
  fi
  return "$rc"
}

# Под launchd работать в переднем плане: ушедший в фон скрипт launchd сочтёт завершённым
# и убьёт группу процессов (та же развилка, что в topup_pro_corpus.sh).
if [ -t 1 ]; then
  run_collect >> "$LOG" 2>&1 &
  echo "сбор запущен в фоне, PID $!, лог: $LOG"
else
  run_collect >> "$LOG" 2>&1
fi
