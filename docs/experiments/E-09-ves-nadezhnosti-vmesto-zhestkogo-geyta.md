---
id: E-09
title: "Вес надёжности вместо жёсткого гейта"
date: "2026-08-05"
area: misc
status: full
corpus: "holdout + прод-отбор"
verdict: "нейтрально"
harness: "`runtime/experiments/misc/holdout_cell_rules.py`, `runtime/experiments/kills/topup_lambda_prod_selection.py` - **Команда:** `DRAFT_CELL_RELIABILITY_K=0|50|150 CP1VS2_TOPUP_FALLBACK"
---

# E-09. Вес надёжности вместо жёсткого гейта
- **Дата:** 2026-08-05
- **Гипотеза:** ячейка ниже `min_matches` сейчас выбрасывается целиком.
  `kills_window` вместо этого взвешивает по `games/(games+prior)`. Тот же приём
  на драфтовых метриках должен использовать больше данных без потери точности.
- **Как мерили:** сначала holdout 50k (скрининг), затем прод-отбор по star-блокам.
- **Харнесс:** `runtime/experiments/misc/holdout_cell_rules.py`,
  `runtime/experiments/kills/topup_lambda_prod_selection.py`
- **Команда:** `DRAFT_CELL_RELIABILITY_K=0|50|150 CP1VS2_TOPUP_FALLBACK=1
  KC_TAG=_rel0|_rel50|_rel150 python3 runtime/experiments/kills/kills_combo_extract_v2.py`,
  затем `python3 runtime/experiments/kills/topup_lambda_prod_selection.py rel0 rel50 rel150`
- **Артефакт:** `runtime/artifacts/kills/topup_lambda_prod_selection.md`
- **Результат:** скрининг прошёл — знак «+» во всех шести клетках (3 метрики ×
  2 блока), сильнее всего synergy_duo All 56.90% → 57.19% (z=+3.18 при K=150).
  **На прод-отборе рассыпался:** помогает одному блоку из четырёх
  (all_star ROI +7.6% → +8.9%) и вредит трём (ew_star +5.0% → +2.5%,
  nw_star +1.5% → +0.7%, lt_star −0.9% → −2.1%). Причём помогает там, где
  `synergy_duo` НЕ участвует, — знаки между выборками не сходятся.
- **Вердикт:** нейтрально. Флаг `DRAFT_CELL_RELIABILITY_K` выключен.
- **Где искать ошибку:** (1) вес обязан доезжать до агрегатора — при
  `GET_DIFF_WEIGHT_POWER=0` он схлопывается в единицу, режим поднимает степень
  до 1.0; (2) вес и число игр разведены по полям кортежа
  `(значение, игры, позиция_врага, вес)` — если диагностика `*_games` показывает
  дроби, развод сломан; (3) множественность: 42 варианта.
- **Коммиты:** `d30e7ed`, развод полей `4e7e810` / serv1 `60f8330`
