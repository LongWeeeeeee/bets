#!/usr/bin/env bash
# Сведение: одна таблица по всем прогонам, кривые винрейта по уверенности и
# смесь оценщиков на честной половине теста.
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
K=runtime/experiments/kills
ts() { date +%H:%M:%S; }

echo "[$(ts)] === 1/3 сводная таблица ==="
$PY $K/kills_v6_summary.py > /dev/null || true

echo "[$(ts)] === 2/3 винрейт по уверенности ==="
for T in v4gbdt v5skellam; do
  for C in public pro; do
    $PY $K/kills_v4_winrate.py --tag $T --corpus $C > /dev/null 2>&1 || true
  done
done

echo "[$(ts)] === 3/3 смесь оценщиков ==="
for C in pro public; do
  $PY $K/kills_v6_ensemble.py --corpus $C > /dev/null 2>&1 || true
done
echo "[$(ts)] V6 ГОТОВО"
