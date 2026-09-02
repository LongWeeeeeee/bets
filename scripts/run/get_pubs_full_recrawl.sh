#!/bin/bash
# Полный повторный обход пабликов на пяти новых Stratz-прокси (запуск 02.09.2026).
#
# ЧТО ДЕЛАЕТ. (1) Откладывает курсоры пройденных id — НЕ удаляет: откат это
#     возврат файлов на место. Без `processed_ids.txt` множество уже собранных
#     карт пусто, а `processed_ids_to_graph.txt` (курс игроков) и так отсутствовал,
#     поэтому обход идёт по ПОЛНОМУ списку игроков (фаза 2 по E-56: обход помнит
#     только «спрашивали или нет», и карты, сыгранные игроком ПОСЛЕ его визита,
#     невидимы до следующего круга).
# (2) Запускает `runtime/experiments/pubs-rebuild/run_full_recrawl.py`, который
#     задаёт пул из 5 пар и start_date_time В РАНТАЙМЕ — `base/keys.py` не
#     правится (AGENTS.md запрещает трогать api_to_proxy/api_to_keys).
#
# ПАРАМЕТРЫ ПРОГОНА (обоснование в раннере и в пробнике):
#   пул               5 пар: прокси[0..4] x ключи[0,2,4,3,1] — замер боевым путём
#                     запроса, все пять ответили 200 (probe_stratz_proxies.py)
#   start_date_time   1786147200 = 2026-08-08T00:00:00 UTC — последний собранный
#                     файл с матчами (09.08.2026) минус сутки на перекрытие окна
#
# ГДЕ. На Маке: новые прокси лежат в локальном keys.py (правка 01.09 23:09), на
# serv1 keys.py от 22.08 со старыми мёртвыми прокси; плюс обход не конкурирует за
# ресурсы с живым пайплайном на боевой машине.
#
# Запуск: bash scripts/run/get_pubs_full_recrawl.sh
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
export PYTHONUNBUFFERED=1
STAMP=$(date +%Y%m%d_%H%M)
PUB=bets_data/analise_pub_matches
CUR="$PUB/json_parts_split_from_object/processed_ids.txt"
GRAPH="$PUB/processed_ids_to_graph.txt"

for f in "$CUR" "$GRAPH"; do
  if [ -e "$f" ]; then
    mv "$f" "${f}.bak_full_recrawl_${STAMP}"
    echo "курсор отложен: $f -> ${f##*/}.bak_full_recrawl_${STAMP}"
  else
    echo "курсора нет (обход и так пойдёт по полному списку): $f"
  fi
done

LOG="runtime/get_pubs_${STAMP}_full_recrawl_5proxies.log"
# caffeinate -i: обход идёт сутками, сон мака оборвал бы его молча.
nohup caffeinate -i $PY runtime/experiments/pubs-rebuild/run_full_recrawl.py > "$LOG" 2>&1 &
PID=$!
echo "$PID" > runtime/get_pubs.pid
echo "PID=$PID"
echo "лог: $LOG"
echo "проверка: kill -0 $PID && tail -3 $LOG"
