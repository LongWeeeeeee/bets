---
id: E-02
title: "λ у cp1vs2 top-up"
date: "2026-08-05"
area: draft-cp
status: full
corpus: "holdout + про + прод-отбор"
verdict: "отвергнуто"
harness: "`runtime/experiments/misc/holdout_cp1vs2_topup.py` (`TU_CORPUS=holdout|pro`, `TU_LAMBDAS`), `runtime/experiments/kills/topup_lambda_prod_selection.py` - **Артефакты:** `runtime/art"
---

# E-02. λ у cp1vs2 top-up
- **Дата:** 2026-08-05
- **Гипотеза:** λ=0.35 выбрана в июне на словарях ДО канонизации ключей, когда
  кросс-позиционный пул был фрагментирован. После консолидации оптимум должен
  сдвинуться вверх.
- **Как мерили:** holdout 50k (сетка λ, McNemar), про 4 680 карт, прод-отбор по
  star-блокам.
- **Харнесс:** `runtime/experiments/misc/holdout_cp1vs2_topup.py`
  (`TU_CORPUS=holdout|pro`, `TU_LAMBDAS`),
  `runtime/experiments/kills/topup_lambda_prod_selection.py`
- **Артефакты:** `runtime/artifacts/misc/holdout_cp1vs2_topup_{holdout,pro}.md`,
  `runtime/artifacts/kills/topup_lambda_prod_selection.md`
- **Результат:** по AUC λ действительно занижена — Late z растёт монотонно
  (+0.85 при 0.35, +2.42 при 0.7, +3.74 при 1.0). Про согласуется по направлению.
  **На прод-отборе победителя нет:** λ=1.0 лучше на nw/ew, хуже на all/lt, всё
  внутри CI ±4 п.п.; на отборочных уровнях 80/85 λ=0.35 лучше в 3 семействах из 4.
- **Вердикт:** отвергнуто, λ=0.35 оставлена.
- **Где искать ошибку:** λ **не переворачивает знак** уже сработавшего блока ни
  разу (310 общих карт, 0 разворотов) — весь эффект в том, какие карты попадают
  в отбор. Если в новом замере McNemar не нулевой, значит считается что-то другое.

---
