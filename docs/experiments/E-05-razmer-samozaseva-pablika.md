---
id: E-05
title: "Размер самозасева паблика"
date: "2026-08-05"
area: misc
status: full
corpus: "holdout 50k"
verdict: "измерено, см. ниже"
harness: "`runtime/experiments/misc/holdout_extract.py` (выемка карт), `runtime/experiments/misc/holdout_thresholds.py` (сетка порогов) - **Команда:** `venv_catboost/bin/python3 runtime/expe"
---

# E-05. Размер самозасева паблика
- **Дата:** 2026-08-05
- **Гипотеза:** словари собраны на тех же матчах, на которых мерим, значит
  драфт-метрики на паблике завышены. Надо узнать **насколько**.
- **Как мерили:** словари пересобраны на 1 369 345 матчах с исключением 50 000
  самых свежих; те же 50k посчитаны дважды — по train-словарям (без них) и по
  полным (с ними). Все holdout-карты попали в `7.41d_part006/007`, то есть это
  ещё и forward-тест.
- **Харнесс:** `runtime/experiments/misc/holdout_extract.py` (выемка карт),
  `runtime/experiments/misc/holdout_thresholds.py` (сетка порогов)
- **Команда:** `venv_catboost/bin/python3 runtime/experiments/misc/holdout_thresholds.py`
  (словари — `bets_data/analise_pub_matches/_traintest/`)
- **Артефакт:** `runtime/artifacts/misc/holdout_thresholds.md`
- **Результат** (блок All, порог ячейки 10, AUC train → full):
  cp1vs1 0.5970 → 0.6315 (+0.035); synergy_duo 0.5846 → 0.6169 (+0.032);
  cp1vs2 0.5866 → 0.7533 (**+0.167**); synergy_trio 0.5599 → 0.7637 (**+0.204**).
  Завышение пропорционально разреженности ячеек.
  Побочно: падение AUC при росте `min_games` на паблике — это **гасла утечка**,
  а не чистился шум. В честной колонке кривая плоская до ~100 и дальше оседает.
- **Вердикт:** измерено. Поправка для чтения старых паблик-цифр: для 1vs1/duo
  вычитать ~0.03, для 1vs2/trio — 0.17…0.20.
- **Где искать ошибку:** (1) train- и full-словари должны быть собраны ОДНИМ
  кодом — проверить, что между сборками не менялся билдер; (2) holdout-карты
  обязаны отсутствовать в train — сверить по `_traintest_holdout_50k.json` и
  строке лога «Исключено test матчей»; (3) популяция блока — Late считается
  только на `is_late_match`.
