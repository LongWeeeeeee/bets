#!/usr/bin/env bash
# Сведение: одна таблица по всем прогонам, кривые винрейта по уверенности и
# смесь оценщиков на честной половине теста.
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
K=runtime/experiments/kills
ts() { date +%H:%M:%S; }

# Первый прогон бустинга на паблике шёл с ускоряющими упрощениями (2 млн строк,
# 127 листьев, lr 0.08, половина колонок) и дал 0.6689 против 0.6698 у линейной
# связки. Вывод «нелинейность не даёт ничего» стоит слишком дорого, чтобы делать
# его по урезанному прогону: здесь тот же бустинг без единого упрощения.
echo "[$(ts)] === 0/3 честная перепроверка бустинга на полном объёме ==="
$PY $K/kills_v4_gbdt.py --corpus public --extra --draft stack --rounds 3000 \
    --leaves 255 --lr 0.05 --ff 0.75 --targets w_10_20,tot_10_20 --tag v4full || true

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
