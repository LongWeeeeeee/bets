#!/bin/bash
# Полный повторный обход пабликов на пяти новых Stratz-прокси (запуск 02.09.2026).
#
# ЧТО ДЕЛАЕТ. (1) Откладывает курсор ПРОЙДЕННЫХ ИГРОКОВ — НЕ удаляет: откат это
#     возврат файла на место. `processed_ids_to_graph.txt` (курс игроков)
#     очищается, поэтому обход идёт по ПОЛНОМУ списку игроков (фаза 2 по E-56:
#     обход помнит только «спрашивали или нет», и карты, сыгранные игроком
#     ПОСЛЕ его визита, невидимы до следующего круга).
#     `json_parts_split_from_object/processed_ids.txt` (курс УЖЕ СОБРАННЫХ
#     МАТЧЕЙ) больше НЕ откладывается (с 16.09.2026): на serv1 сам 36-гигабайтный
#     корпус part-файлов больше не хранится (переехал на Mac, см.
#     docs/SERVER_LAYOUT.md), поэтому merge не может пересканировать part-файлы
#     диска и восстановить дедуп-набор заново — `processed_ids.txt` это ЕДИНСТВЕННЫЙ
#     источник дедупа матчей на serv1. Убрать его — значит залить дубликаты.
# (2) Запускает `runtime/experiments/pubs-rebuild/run_full_recrawl.py`, который
#     задаёт пул из 5 пар и start_date_time В РАНТАЙМЕ — `base/keys.py` не
#     правится (AGENTS.md запрещает трогать api_to_proxy/api_to_keys).
#
# ПАРАМЕТРЫ ПРОГОНА (обоснование в раннере и в пробнике):
#   пул               5 пар: прокси[0..4] x ключи[0,2,4,3,1] — замер боевым путём
#                     запроса, все пять ответили 200 (probe_stratz_proxies.py)
#   start_date_time   1786147200 = 2026-08-08T00:00:00 UTC — последний собранный
#                     файл с матчами (09.08.2026) минус сутки на перекрытие окна;
#                     на serv1 без part-файлов раннер берёт окно из
#                     pub_player_steam_ids.json (last_crawl_completed_utc) — см.
#                     `start_date_time()` в раннере.
#   PUBS_IDS_MODE / PUBS_MAX_PLAYERS — переменные окружения `maps_research.get_pubs()`
#                     (см. docs/CODE_MAP.md), наследуются процессом раннера как
#                     обычные env; отдельного кода в этом скрипте не требуют —
#                     задавайте их перед запуском, например:
#                     `PUBS_IDS_MODE=file PUBS_MAX_PLAYERS=200 bash scripts/run/get_pubs_full_recrawl.sh`
#
# ГДЕ. Работает и на Mac, и на serv1 (03.09 прогон перенесён на serv1: там свой
# keys.py, и обход не конкурирует с локальной пересборкой снимков). Пул и окно
# раннер берёт ПО ФАКТУ машины: на serv1 пять пар уже сведены в keys.py, на Mac
# пятая пара добавляется в раннере; start_date_time считается из собственного
# корпуса (дата последнего собранного файла минус сутки) либо из
# pub_player_steam_ids.json, если part-файлов на машине нет.
#
# Запуск: bash scripts/run/get_pubs_full_recrawl.sh
set -u
if [ -d /root/main/base ]; then
  cd /root/main
  PY=venv/bin/python3
else
  cd /Users/alex/Documents/ingame
  PY=venv_catboost/bin/python3
fi
export PYTHONUNBUFFERED=1
STAMP=$(date +%Y%m%d_%H%M)
PUB=bets_data/analise_pub_matches
GRAPH="$PUB/processed_ids_to_graph.txt"

for f in "$GRAPH"; do
  if [ -e "$f" ]; then
    mv "$f" "${f}.bak_full_recrawl_${STAMP}"
    echo "курсор отложен: $f -> ${f##*/}.bak_full_recrawl_${STAMP}"
  else
    echo "курсора нет (обход и так пойдёт по полному списку): $f"
  fi
done

mkdir -p runtime
LOG="runtime/get_pubs_${STAMP}_full_recrawl_5proxies.log"
# caffeinate -i есть только на Mac: там обход идёт сутками и сон оборвал бы его
# молча. На serv1 процесс и так живёт под systemd-машиной без сна.
if command -v caffeinate >/dev/null 2>&1; then
  nohup caffeinate -i $PY runtime/experiments/pubs-rebuild/run_full_recrawl.py > "$LOG" 2>&1 &
else
  nohup $PY runtime/experiments/pubs-rebuild/run_full_recrawl.py > "$LOG" 2>&1 &
fi
PID=$!
echo "$PID" > runtime/get_pubs.pid
echo "PID=$PID"
echo "лог: $LOG"
echo "проверка: kill -0 $PID && tail -3 $LOG"
