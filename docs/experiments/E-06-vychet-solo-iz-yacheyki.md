---
id: E-06
title: "Вычет соло из ячейки"
date: "2026-08-05"
area: draft-cp
status: full
corpus: "holdout 50k"
verdict: "отвергнуто"
harness: "`runtime/experiments/misc/holdout_cell_rules.py` - **Команда:** `CR_MAX=50000 venv_catboost/bin/python3 runtime/experiments/misc/holdout_cell_rules.py` - **Артефакт:** `runtime/art"
---

# E-06. Вычет соло из ячейки
- **Дата:** 2026-08-05
- **Гипотеза:** `solo` и `cp1vs1` входят в один STAR-блок и дублируют силу
  героев; если из значения ячейки вычесть ожидание по соло-силам, останется
  чистый матчап, и совокупный сигнал станет сильнее.
- **Как мерили:** holdout 50k, AUC и точность знака, McNemar против прод-правила.
- **Харнесс:** `runtime/experiments/misc/holdout_cell_rules.py`
- **Команда:** `CR_MAX=50000 venv_catboost/bin/python3 runtime/experiments/misc/holdout_cell_rules.py`
- **Артефакт:** `runtime/artifacts/misc/holdout_cell_rules.md`
- **Результат:** хуже везде и значимо. cp1vs1 All 0.5956 → 0.5856 (**z=−4.61**),
  synergy_duo All 0.5844 → 0.5641 (**z=−5.19**), Late тот же знак.
- **Вердикт:** отвергнуто. **Предсказательная сила метрики И ЕСТЬ сила героев**,
  матчап поверх неё почти ничего не добавляет. Это же объясняет потолок драфта
  из E-13: комбайнер не бил одну cp1vs1, потому что все фичи меряют одно и то же.
- **Где искать ошибку:** формула ожидания `own/(own+(1-enemy))` — при соло-винрейтах
  около нуля знаменатель вырождается; но знак эффекта настолько устойчив, что
  ошибка в формуле объясняла бы шум, а не −0.02 AUC.
