#!/usr/bin/env bash
# Переразбор корпусов под килы v3. Параллельность 3: пять процессов с json.load
# 500-МБ файлов вставали в своп-тупик (проверено 13.08 на v2).
# Про идёт ПЕРВЫМ — на нём проверяется вся цепочка, пока разбирается паблик.
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
F=runtime/experiments/kills/kills_v3_extract.py
DIR=runtime/artifacts/kills/window_model_v2
ts() { date +%H:%M:%S; }
wait_pids() { for p in "$@"; do while kill -0 "$p" 2>/dev/null; do sleep 15; done; done; }

echo "[$(ts)] === 1/3 про (2 шарда) + паблик шард 0 ==="
$PY $F --corpus pro --shard 0 --shards 2 > $DIR/v3_extract_pro_0.log 2>&1 & p0=$!
$PY $F --corpus pro --shard 1 --shards 2 > $DIR/v3_extract_pro_1.log 2>&1 & p1=$!
$PY $F --corpus public --shard 0 --shards 5 > $DIR/v3_extract_public_0.log 2>&1 & p2=$!
wait_pids $p0 $p1 $p2

echo "[$(ts)] === 2/3 паблик шарды 1-3 ==="
for i in 1 2 3; do
  $PY $F --corpus public --shard $i --shards 5 > $DIR/v3_extract_public_$i.log 2>&1 &
  eval "q$i=\$!"
done
wait_pids $q1 $q2 $q3

echo "[$(ts)] === 3/3 паблик шард 4 ==="
$PY $F --corpus public --shard 4 --shards 5 > $DIR/v3_extract_public_4.log 2>&1
ls -la $DIR/rowsv3_*.npz | awk '{printf "  %.0fМБ %s\n",$5/1048576,$9}'
echo "[$(ts)] ПЕРЕРАЗБОР ГОТОВ"
