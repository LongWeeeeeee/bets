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
# Повторный запуск в те же сутки UTC ничего не делает: если за текущую дату UTC уже
# есть collection.json с complete=true, сборщик не зовётся.
# Тишина ≠ успех: одна строка в админ-чат в любом исходе (scripts/ops/notify_admin.py).
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
OUT=runtime/artifacts/misc/rank_snapshots
mkdir -p "$OUT"
LOG="$OUT/collect_$(date -u +%Y%m%d).log"

run_collect() {
  local have
  have=$($PY - "$OUT" <<'EOF'
import json, sys, glob, datetime as dt
today = dt.datetime.now(dt.timezone.utc).date()
for f in glob.glob(sys.argv[1] + "/*/collection.json"):
    try:
        j = json.load(open(f))
    except Exception:
        continue
    t = j.get("started_at")
    if j.get("complete") is True and t and dt.datetime.fromtimestamp(float(t), dt.timezone.utc).date() == today:
        print(f)
        break
EOF
)
  if [ -n "$have" ]; then
    echo "skip: complete snapshot for today UTC already exists: $have"
    return 0
  fi
  local rc=0
  $PY -m base.tools.collect_rank_snapshots --output "$OUT" || rc=$?
  echo "collect_exit=$rc"
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
  if [ "$rc" -ne 0 ] || ! echo "$summary" | grep -q "complete=True регионов=4" || ! echo "$summary" | grep -q "сбойные=нет"; then
    { echo "⚠️ снимок рангов Valve: rc=$rc ($(date '+%F %T'))"; echo "$summary"; tail -3 "$LOG"; } \
      | $PY scripts/ops/notify_admin.py
  else
    echo "✅ снимок рангов Valve: $summary" | $PY scripts/ops/notify_admin.py
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
