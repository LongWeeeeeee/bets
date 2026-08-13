#!/usr/bin/env bash
# Килы v4: бустинг вместо линейной модели + пакет предматчевых признаков +
# ЗАМЕР ПОТОЛКА (сколько даёт знание карты на минуте гейта — не продукт, а
# верхняя граница, относительно которой видно, сколько ещё можно выжать).
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
K=runtime/experiments/kills
ts() { date +%H:%M:%S; }

echo "[$(ts)] === 1/5 пакет v4 и словарные колонки: про ==="
$PY $K/kills_v4_extra.py --corpus pro || exit 1
$PY $K/kills_v4_dict_features.py --corpus pro || exit 1

echo "[$(ts)] === 2/5 бустинг на про (быстрая проверка рычага) ==="
$PY $K/kills_v4_gbdt.py --corpus pro --extra --draft oof \
    --targets w_5_15,w_10_20,w_15_25,w_20_30,ge27 --tag v4gbdt || exit 1

echo "[$(ts)] === 3/5 пакет v4: паблик ==="
$PY $K/kills_v4_extra.py --corpus public || exit 1

echo "[$(ts)] === 4/5 бустинг на паблике, все окна ==="
$PY $K/kills_v4_gbdt.py --corpus public --extra --draft stack \
    --targets w_5_15,w_10_20,w_15_25,w_20_30 --tag v4gbdt || exit 1

echo "[$(ts)] === 5/5 потолок: то же плюс состояние карты на минуте гейта ==="
$PY $K/kills_v4_gbdt.py --corpus public --extra --draft stack --ingame \
    --targets w_10_20,w_20_30 --tag v4ceil || exit 1

echo "[$(ts)] V4 ГОТОВО"
