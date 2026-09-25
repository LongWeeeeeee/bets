---
id: E-341
title: "Прямые kill-цели для панели B: rad_ge30 / dire_ge30 / total_ge55 без полосы исключения"
date: "2026-09-25"
area: ml
status: full
corpus: "панельный корпус (test все 26 016 карт с 2026-03-29, без исключений); od3_full_d86400 без 30 колонок OpenDota (928 колонок панели + 122 kv3)"
verdict: "Прямые цели учатся: rad_ge30 LL 0,57995 AUC 0,71047, dire_ge30 LL 0,59541 AUC 0,71234, total_ge55 LL 0,49736 AUC 0,73038. Team AUC чуть ниже прогнозного интервала 0,72–0,74, тотал внутри; не опровергнуто (> 0,68). Карточка показывает окна + радиант ≥30 + дайр ≥30 + тотал ≥55, когда B обслужил все три цели; показ после деплоя."
harness: "runtime/experiments/kills/plain_kills/train.py; runtime/experiments/kills/plain_kills/thresholds.py; runtime/experiments/kills/plain_kills/fit_tempo.py; runtime/experiments/kills/plain_kills/fit_level.py; runtime/experiments/kills/plain_kills/promote.py"
---

# E-341. Прямые kill-цели для прематч-панели B (без полосы исключения)

## 1. Вопрос

Владелец (25.09.2026) увидел в карточке строки «🔴 радиант ≥30: ≤25 68%» и «🔴 тотал ≥55: ≤50 85%»
и сказал: «это не то! Нам нужна информация о dire >=30 килов и radiant >=30 килов, а у тебя они
показывают промежуток >=30 <=25».

Обслуживаемые B-цели `rad_30_25` / `dire_30_25` / `total_55_50` — это BAND-цели: карты с 26–29
(51–54) убийствами исключены, поэтому их p — это P(≥30 | не 26–29). Новые прямые цели:

- `rad_ge30`: убийства Radiant ≥30 против ≤29;
- `dire_ge30`: убийства Dire ≥30 против ≤29;
- `total_ge55`: тотал ≥55 против ≤54.

Рецепт — тот же боевой B (928 колонок панели + 122 колонки kv3, `od3_full_d86400` без OpenDota,
параметры и сид CatBoost как в E-340).

## 2. Данные

Источник метки: `pro_corpus_rich.npz`, `pstats[:,:,0]` — суммы убийств игроков (тот же источник,
что у band-целей). Тест — все 26 016 карт с 2026-03-29, без исключений.

Проверка дрейфа входов (замороженные боевые B-модели на сегодняшних пересобранных входах;
архивные позиционные кэши `runtime/artifacts/misc/_stale_482k_20260902_1936` удалены, спроецированы
по mid, ни одной их колонки в 928 нет):

- `rad_30_25`: mean |dp| 0,0025, p50 0,0010, p99 0,0205, 5,1% карт > 0,01;
- `total_55_50`: mean 0,0003, p99 0,0033;
- проверка на утечку: те же модели дают LL 0,52352 → 0,52339 и AUC 0,73709 → 0,73726 (rad),
  тотал совпадает до 4 знаков. Признаков будущей информации нет.

## 3. Пререгистрация (до чисел)

- Прогноз: team AUC 0,72–0,74, total 0,70–0,73 — ниже, чем у band-моделей, потому что трудные карты
  26–29 включены; опровергнуто, если AUC < 0,68.
- Новое правило порога карточки (знак 🟢) зафиксировано до взгляда на тест: валидационный минимум t
  в 0,50…0,95 с шагом 0,005 при hit ≥ max(0,75, валидационное большинство + 0,05), доля ≥ 10%,
  n ≥ 200; иначе 0,99 (`runtime/experiments/kills/plain_kills/thresholds.py`). Старое правило
  `recalibrate_panel` вырождается: 0,50 на командных целях (зелёное на каждой карте) и 0,99 на тотале.

## 4. Результаты на тесте (26 016 карт)

| цель | base rate | LL | AUC | Brier | ECE |
|---|---|---|---|---|---|
| rad_ge30 | 0,647 | 0,57995 | 0,71047 | 0,19784 | 0,0087 |
| dire_ge30 | 0,613 | 0,59541 | 0,71234 | 0,20503 | 0,0071 |
| total_ge55 | 0,748 | 0,49736 | 0,73038 | 0,16230 | 0,0104 |

Team AUC чуть ниже прогнозного интервала (0,72); тотал внутри. Не опровергнуто (> 0,68).

Пороги карточки на тесте:

- `rad_ge30` 0,63: hit 76,4% на 66,8% карт против большинства 64,7%;
- `dire_ge30` 0,635: hit 75,3% на 60,7% против 61,3%;
- правило `total_ge55` 0,54: hit 78,6% на 94,5% карт против 74,8%.

Большинство сдвинулось от валидации к тесту на +5,6…+8,3 п.п. (валидация 0,591 / 0,560 / 0,666,
тест 0,647 / 0,613 / 0,748). POST-HOC решение лида: порог `total_ge55` — 0,99 (без зелёного),
как было у band-тотала.

Темпо-коррекция серий, переподогнанная под `total_ge55` (живая коррекция по приказу владельца
переходит на показанный тотал):

- `plain_kills/fit_tempo.py` даёт a 0,16333, beta 0,008860/убийство, mu 63,111, n_cont 3880;
- out-of-fold против raw: dLL −0,00317 [−0,00559, −0,00067], dAUC +0,0032 [+0,0006, +0,0054];
  против level-only: dLL −0,00149 [−0,00328, +0,00043];
- a_level 0,14123 из `plain_kills/fit_level.py`, чей метод воспроизводит боевой 0,20196088234669715
  цели `total_55_50` точно;
- для сравнения, `total_55_50`: a 0,23966, beta 0,009956, mu 63,426, a_level 0,20196, n_cont 3555.

## 5. Вывод

Прямые цели учатся на рецепте B: командный AUC 0,71 (чуть ниже прогноза 0,72–0,74 — трудные карты
26–29 включены, как и ожидалось), тотал 0,73 внутри прогноза. Не опровергнуто.

Показ: карточка показывает окна + радиант ≥30 + дайр ≥30 + тотал ≥55 (стороны ≤29/≤54) + dur43,
когда B обслужил все три прямые цели. Иначе показывается блок E281. Band-строки — только при
`ML_PANEL_KILLS_DISPLAY=band`. Выключатели: `ML_PANEL_KV3_PLAIN=0`, `ML_PANEL_KILLS_DISPLAY=e281`.

Артефакты: `runtime/artifacts/kills/panel_plus_v3/plain/` (`<key>.metrics.json`,
`<key>.predictions.npz`, `thresholds.json`, `total_ge55.tempo.json`, `full_train.log`,
`<key>/bundle`); продвижение — `.../promote.py <bundle> ml-models/prematch_panel_kv3 --key K`.

Статус: в бою с 25.09.2026 12:17:21 MSK (коммит 1cb354f0, рестарт cyberscore на serv1). Карточка показывает «радиант ≥30», «дайр ≥30», «тотал ≥55» (стороны ≤29/≤54) вместо полосовых строк; поправка серии применяется к тоталу ≥55. На serv1 CatBoost 1.2.10 даёт те же сырые вероятности, что локальный 1.2.8 (6 карт фикстуры, max |Δ| = 0.0). Первая живая строка ещё не наблюдалась (на момент деплоя живых карт 0). Откат без деплоя: ML_PANEL_KV3_PLAIN=0 (блок E281) или ML_PANEL_KILLS_DISPLAY=band.

## 6. Харнесс и команды

```bash
# обучение (wall 1954 с)
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -u runtime/experiments/kills/plain_kills/train.py --threads 6 --out runtime/artifacts/kills/panel_plus_v3/plain
# пороги (с применением)
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -u runtime/experiments/kills/plain_kills/thresholds.py --threads 6 --apply
# темпо для total_ge55
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -u runtime/experiments/kills/plain_kills/fit_tempo.py --key total_ge55 --pred runtime/artifacts/kills/panel_plus_v3/plain/total_ge55.predictions.npz --out runtime/artifacts/kills/panel_plus_v3/plain/total_ge55.tempo.json
# уровень
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -u runtime/experiments/kills/plain_kills/fit_level.py
# продвижение бандла в ml-models/prematch_panel_kv3
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -u runtime/experiments/kills/plain_kills/promote.py <bundle> ml-models/prematch_panel_kv3 --key K
```

## 7. Где искать ошибку

- **Валидационная калибровка порогов — in-sample** (узлы изотонии подогнаны на тех же валидационных
  строках).
- **Константы темпо подогнаны на картах продолжений тест-периода**, как у E-331.
- Дрейф входов до 0,02 на p99 для Radiant.
- Метки — суммы убийств игроков, а не командный счёт.
- Проверки на свежем периоде нет.
- Порог тотала — post-hoc.
