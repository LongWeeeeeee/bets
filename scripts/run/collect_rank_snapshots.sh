#!/bin/bash
# Ежедневный снимок четырёх региональных лидербордов Valve (europe/se_asia/americas/china)
# для исследования рангов игроков (карточка ingame-enwj, E-295).
#
# Где работает. С 10.10.2026 — только на serv1 (владелец: «убери любой сбор с мака ранги в
# том числе пусть парсятся с serv1»): systemd rank-snapshots.timer, слоты 06/09/12/15/18 UTC,
# юниты scripts/ops/systemd/rank-snapshots.{service,timer}, установка
# scripts/ops/install-rank-snapshots.sh (на serv1). Архив перенесён туда же:
# /root/main/runtime/artifacts/misc/rank_snapshots (22 снимка до 10.10 включительно).
#
# История. До 24.09.2026 снимки собирала автоматизация Codex «dota-2» (heartbeat в 09:00);
# 22.09 запрос к модели вернул 403, с 23.09 приложение Codex закрыто — снимков за 22–23.09
# нет и не будет (Valve отдаёт только текущую таблицу). 24.09–10.10 сбор шёл джобой launchd
# на Mac; 07, 08 и 10.10 её прогон 09:00 стартовал в тёмном пробуждении Mac на батарее
# (pmset: «DarkWake from Deep Idle … Using BATT», сна через ~2 с): DNS/TLS падали,
# 07 и 08.10 потеряны (карточка ingame-qe6y). Отсюда устойчивость прогона, которая
# осталась и на serv1:
# (1) ждём сеть до RANK_SNAPSHOT_NET_TRIES×RANK_SNAPSHOT_NET_SLEEP секунд;
# (2) до RANK_SNAPSHOT_ATTEMPTS попыток сборщика с паузой RANK_SNAPSHOT_RETRY_SLEEP;
# (3) провал до 18:00 UTC — только строка в логе («deferred»): следующий слот тех же суток
# UTC повторит сбор. Тревога — только когда слотов в этих сутках UTC больше нет и снимок
# за день потерян.
#
# Повторный запуск в те же сутки UTC ничего не делает: если за дату старта (UTC) уже
# есть collection.json с complete=true, сборщик не зовётся.
# Тишина ≠ успех: успех и потерянный день — строка в админ-чат (scripts/ops/notify_admin.py,
# без звука с 10.10.2026).
# Переменные RANK_SNAPSHOT_* — швы для теста base/tests/test_collect_rank_snapshots_runner.py.
set -u
# Корень репозитория — от места скрипта, а не зашитый путь Mac (на serv1 это /root/main).
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)" || exit 1
# Интерпретатор: venv_catboost на Mac, venv на serv1 (python 3.12).
if [ -n "${RANK_SNAPSHOT_PY:-}" ]; then
  PY=$RANK_SNAPSHOT_PY
elif [ -x venv_catboost/bin/python3 ]; then
  PY=venv_catboost/bin/python3
else
  PY=venv/bin/python3
fi
OUT=${RANK_SNAPSHOT_OUT:-runtime/artifacts/misc/rank_snapshots}
COLLECT_CMD=${RANK_SNAPSHOT_COLLECT_CMD:-$PY -m base.tools.collect_rank_snapshots}
NOTIFY_CMD=${RANK_SNAPSHOT_NOTIFY_CMD:-$PY scripts/ops/notify_admin.py}
NET_CHECK_CMD=${RANK_SNAPSHOT_NET_CHECK_CMD:-curl -sS -o /dev/null -m 15 https://www.dota2.com/}
NET_TRIES=${RANK_SNAPSHOT_NET_TRIES:-20}
NET_SLEEP=${RANK_SNAPSHOT_NET_SLEEP:-30}
ATTEMPTS=${RANK_SNAPSHOT_ATTEMPTS:-3}
RETRY_SLEEP=${RANK_SNAPSHOT_RETRY_SLEEP:-120}
# Потолок одной попытки сборщика (astra 10.10, MEDIUM): timeout=30 в сборщике ограничивает
# ожидание каждого чтения, а не всю загрузку; зависший ответ иначе держал бы юнит до
# TimeoutStartSec и съедал оставшиеся попытки. Обычный сбор четырёх регионов — секунды.
COLLECT_TIMEOUT=${RANK_SNAPSHOT_COLLECT_TIMEOUT:-300}
# Потолок одной отправки в админ-чат (astra 10.10, MEDIUM раунда 2): timeout=20 в
# notify_admin.py — на каждую операцию сокета, а не на весь ответ; тревога за вчера
# шлётся ДО сбора, и зависшая отправка не должна съесть слот.
NOTIFY_TIMEOUT=${RANK_SNAPSHOT_NOTIFY_TIMEOUT:-60}
# Последний слот таймера — 18:00 UTC (OnCalendar задан в UTC, от пояса serv1 не зависит);
# провал в нём или позже = день потерян. Меняешь слоты rank-snapshots.timer — поправь порог
# (тест сверяет их); страховка — проверка вчерашних суток в начале прогона.
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
        t = j.get("started_at")
        # Битый started_at в одном файле не должен ронять разбор всего архива (hard-verifier 10.10 INFO).
        d = dt.datetime.fromtimestamp(float(t), dt.timezone.utc).date() if t else None
    except Exception:
        continue
    if j.get("complete") is True and d == day:
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
  out=$(run_bounded "$NOTIFY_TIMEOUT" $NOTIFY_CMD 2>&1)
  echo "$out"
  echo "$out" | grep -q "notify_admin: ok"
}

# Страховка (hard-verifier 10.10, LOW a/b): если последний слот проспан и таймер догнал
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

# Запустить команду с потолком времени: coreutils timeout на serv1, perl alarm на macOS
# (там timeout нет). По истечении процесс получает SIGTERM/SIGALRM, код != 0.
run_bounded() {
  local secs=$1
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$secs" "$@"
  else
    perl -e '$t = shift @ARGV; alarm $t; exec @ARGV or die "exec: $!"' "$secs" "$@"
  fi
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
  # Час старта, а не конца: прогон, затянувшийся за 18 UTC, не должен объявлять сутки
  # потерянными, пока последний слот ещё впереди.
  # Дата и час фиксируются вместе и одним вызовом date (astra 10.10, hard-verifier LOW):
  # прогон, начатый в 18 UTC и закончившийся после 00 UTC, называет сутки старта.
  local now_utc_date now_utc_hour
  read -r now_utc_date now_utc_hour <<<"$(date -u '+%F %H')"
  local start_utc_hour=${RANK_SNAPSHOT_UTC_HOUR:-$now_utc_hour}
  local start_utc_date=${RANK_SNAPSHOT_UTC_DATE:-$now_utc_date}
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
      run_bounded "$COLLECT_TIMEOUT" $COLLECT_CMD --output "$OUT" || rc=$?
      echo "attempt $attempt: collect_exit=$rc"
      # Полный снимок за сутки старта (или за текущие, если прогон перешёл 00 UTC) — готово.
      # Раньше смотрели только текущие сутки: снимок, снятый до 00 UTC, а проверенный после,
      # не засчитывался, и удачный сбор объявлялся потерянным (hard-verifier 10.10 LOW).
      if [ -n "$(have_complete_on "$start_utc_date")" ] || [ -n "$(have_complete_today)" ]; then
        rc=0
        break
      fi
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
    echo "deferred: $ATTEMPTS attempts failed (started ${start_utc_hour}h UTC); a later timer slot today retries, no alert"
  elif [ -e "$OUT/.lost_alerted_$start_utc_date" ]; then
    echo "lost-day alert for $start_utc_date already delivered; no duplicate"
  else
    { echo "⚠️ снимок рангов Valve за $start_utc_date UTC потерян: rc=$rc ($(date '+%F %T'))"; echo "$summary"; tail -3 "$LOG"; } \
      | notify && : > "$OUT/.lost_alerted_$start_utc_date"
  fi
  return "$rc"
}

# Под systemd (Type=oneshot) работать в переднем плане: ушедший в фон процесс systemd убьёт
# вместе с cgroup юнита, как только главный процесс выйдет.
if [ -t 1 ]; then
  run_collect >> "$LOG" 2>&1 &
  echo "сбор запущен в фоне, PID $!, лог: $LOG"
else
  run_collect >> "$LOG" 2>&1
fi
