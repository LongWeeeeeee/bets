#!/usr/bin/env bash
# Цепочка И-31 (эмбеддинги игроков на про) и И-33 (лифт блоков над winRates).
# Оба замера независимы, поэтому идут параллельно; в конце — один сводный отчёт.
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
SUM=runtime/artifacts/misc/i31_i33_summary.md

$PY runtime/experiments/misc/state_baseline_lift.py > runtime/i33_state_lift.log 2>&1 &
P33=$!
$PY runtime/experiments/misc/pro_player_embed.py  > runtime/i31_player_embed.log 2>&1 &
P31=$!
echo "И-33 pid $P33, И-31 pid $P31"
wait $P33; R33=$?
wait $P31; R31=$?
echo "И-33 exit $R33, И-31 exit $R31"

{
  echo "# Сводка: И-31 (эмбеддинги игроков) и И-33 (лифт над состоянием)"
  echo
  echo "Прогон завершён. Коды выхода: И-33 = $R33, И-31 = $R31."
  echo
  if [ -f runtime/artifacts/misc/pro_player_embed.md ]; then
    cat runtime/artifacts/misc/pro_player_embed.md
  else
    echo "## И-31 — отчёта нет, смотри runtime/i31_player_embed.log"
    tail -20 runtime/i31_player_embed.log
  fi
  echo
  if [ -f runtime/artifacts/misc/state_baseline_lift.md ]; then
    cat runtime/artifacts/misc/state_baseline_lift.md
  else
    echo "## И-33 — отчёта нет, смотри runtime/i33_state_lift.log"
    tail -20 runtime/i33_state_lift.log
  fi
} > "$SUM"
echo "СВОДКА: $SUM"
