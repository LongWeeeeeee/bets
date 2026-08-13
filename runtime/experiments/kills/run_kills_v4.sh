#!/usr/bin/env bash
# Килы v4: бустинг вместо линейной модели + пакет предматчевых признаков +
# ЗАМЕР ПОТОЛКА (сколько даёт знание карты на минуте гейта — не продукт, а
# верхняя граница, относительно которой видно, сколько ещё можно выжать).
#
# Цели-тоталы добавлены не для красоты: на про линейная модель уже дала по тоталу
# карты AUC 0.719 против 0.63-0.65 по разнице. Тотал предсказуем лучше, потому
# что он про темп игры, а не про то, кто кого пересилит; и рынок «тотал килов»
# у букмекеров существует, в отличие от «кто сделает больше».
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
K=runtime/experiments/kills
ts() { date +%H:%M:%S; }

echo "[$(ts)] === 1/5 пакет v4 и словарные колонки: про ==="
$PY $K/kills_v4_extra.py --corpus pro || exit 1
$PY $K/kills_v4_dict_features.py --corpus pro || exit 1

echo "[$(ts)] === 2/5 пакет v4: паблик ==="
$PY $K/kills_v4_extra.py --corpus public || exit 1

# Паблик СНАЧАЛА: про-прогон применяет паблик-модель к про-строкам и печатает
# перенос. По v3 перенос обгоняет обучение на про, и без этой колонки вывод
# «про-модель такая-то» ничего не значит.
echo "[$(ts)] === 3/5 бустинг на паблике ==="
$PY $K/kills_v4_gbdt.py --corpus public --extra --draft stack --rounds 800 --leaves 127 --lr 0.08 --ff 0.5 --max-train 2000000 \
    --targets w_5_15,w_10_20,w_15_25,w_20_30,tot_5_15,tot_10_20,tot_15_25,tot_20_30,ge27,tot51,map \
    --tag v4gbdt || exit 1

echo "[$(ts)] === 4/5 бустинг на про: все цели + перенос паблик-модели ==="
$PY $K/kills_v4_gbdt.py --corpus pro --extra --draft oof --rounds 2000 \
    --targets w_5_15,w_10_20,w_15_25,w_20_30,tot_5_15,tot_10_20,tot_15_25,tot_20_30,ge27,tot51,map \
    --tag v4gbdt || exit 1

echo "[$(ts)] === 5/5 потолок: то же плюс состояние карты на минуте гейта ==="
$PY $K/kills_v4_gbdt.py --corpus public --extra --draft stack --ingame --rounds 800 --leaves 127 --lr 0.08 --ff 0.5 --max-train 2000000 \
    --targets w_10_20,w_20_30 --tag v4ceil || exit 1
$PY $K/kills_v4_gbdt.py --corpus pro --extra --draft stack --ingame --rounds 2000 \
    --targets w_10_20,w_20_30 --tag v4ceil || exit 1

echo "[$(ts)] V4 ГОТОВО"
