#!/usr/bin/env bash
# Килы v5: счётчики вместо знака (регрессия разницы и два пуассона + Скеллам).
# Память: объединённый вид обеих сторон удваивает строки, поэтому на паблике
# обучение режется до 1.2 млн карт (по E-165 объём там насыщен уже на 1.2 млн).
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
K=runtime/experiments/kills
ts() { date +%H:%M:%S; }

echo "[$(ts)] === 1/3 про: три оценщика на всех окнах ==="
$PY $K/kills_v5_skellam.py --corpus pro --targets w_5_15,w_10_20,w_15_25,w_20_30 || exit 1

echo "[$(ts)] === 2/3 паблик: три оценщика, обучение 1.2 млн ==="
$PY $K/kills_v5_skellam.py --corpus public --max-train 1200000 \
    --targets w_5_15,w_10_20,w_15_25,w_20_30 || exit 1

echo "[$(ts)] === 3/3 кривые винрейта по уверенности ==="
for C in pro public; do
  $PY $K/kills_v4_winrate.py --tag v5skellam --corpus $C || true
  $PY $K/kills_v4_winrate.py --tag v4gbdt --corpus $C || true
done
echo "[$(ts)] V5 ГОТОВО"
