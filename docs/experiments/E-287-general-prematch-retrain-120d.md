---
id: E-287
title: "Переобучение общей prematch ML на 120 днях причинной истории"
date: "2026-09-12"
area: model
status: full
corpus: "17 604 цели, пригодны 15 243; full fit 15 205, no_org fit 7 614; исторический test 38 карт / 28 серий"
verdict: "Кандидат переобучен и проверен: routed log loss 0.64119 против 0.68433, accuracy 26/38 против 22/38. Paired delta -0.04314 CI [-0.09441;+0.01716]: устойчивый uplift не доказан на повторно изученном test. NPZ и независимый optimizer PASS, четыре fallback сохранены. Калибровка и новая prospective проверка pending; в прод не установлен."
harness: "runtime/experiments/misc/general_ml_retrain_20260912.py; base/tools/retrain_prematch_general.py; base/prematch_history_replay.py"
---

# E-287 — переобучение общей prematch ML

Запрос пользователя: «переобучи». Продолжение E-286, задача `ingame-gr3t`.
Цель — обучить отдельный кандидат общей модели на расширенном корпусе,
сохранив формат 35 признаков и текущий компонент All. Это не активация
кандидата, не проверка прибыльности и не завершение будущего протокола E-286.

## Зафиксированный протокол

Источник — `runtime/artifacts/misc/pro_corpus_rich.npz`, последние 120 дней
от максимального end-time `1789169900`. Найдено 17 842 карты; исключены
97 пересечений с public-корпусом All и 141 карта reserved E-278.
Остаются 17 604 цели и 18 702 уникальных аккаунта до проверки покрытия.

Для каждой карты история игроков/героев и keyed ELO восстанавливаются только
по результатам с `end < start` этой карты. Используются ранее проверенные
E-286 replay и scoring-путь; отсутствующая история и конфликт позиций ведут
к исключению, без ослабления guard. Отсутствующий h2h у `no_org` представлен
нейтральным значением только в полной матрице.

Драфтовый компонент — текущий full-fit All из
`data/draft_phase_models/2026-09-05_position_pairs/all/hero_role_position_pair/model.joblib`,
SHA `b96f32d55acfb8e9edba1c61899f423b3d82b6e115c95fe5141e41350c53c340`.
Максимальный end-time публичной обучающей метки — `1788482883`.
Это фиксированное обученное представление, ретроспективно рассчитанное
для более ранних train-карт. Исторические train-прогнозы поэтому не являются
прогнозами, реально доступными тогда. Test начинается после обучения All.

Граница train/test — **05.09.2026 00:00 UTC** (`1788566400`), completion
embargo 3600 s; серии, пересекающие границу, исключаются целиком.
Все test-метки исключены из подбора весов. `C=0.1` зафиксирован из E-286;
новой сетки и подбора по test нет. Сам test уже изучался, поэтому это
описательная историческая проверка, а не независимое подтверждение.

Обучаются совместно все коэффициенты `full` (35 колонок) и `no_org` (34).
`full` использует общие пригодные строки, `no_org` — только строки, где scorer
фактически выбрал `no_org`. Метрики ветвей считаются на их фактических
test-популяциях; общий paired результат учитывает реальный выбор ветви.
Четыре fallback-ветви, порядок колонок и `ctx_mu/ctx_sd` сохраняются точно.
Выход — отдельный weights-only NPZ: top-level массивы совпадают с full branch.
Старые таблицы confidence→WR и пороги после изменения весов не валидированы.

## Харнесс и воспроизведение

```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -m pytest base/tests/test_retrain_prematch_general.py -q
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/misc/general_ml_retrain_20260912/plan.json
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/misc/general_ml_retrain_20260912/plan.json --background
```

Протокол и точные SHA — `runtime/artifacts/misc/general_ml_retrain_20260912/`.
Manifest v2 содержит 347 входов, примерно 11.414 GB; SHA плана
`b862ab3e7343974b2289f24e9bd327a93908d063c1d7fd43e32ea5ba2fa3f80d`.
Первый, не запущенный снимок сохранён; v2 исправляет выбор train-популяции
`no_org`. Read-only snapshot проверен; для macOS применён порядок
rename → chmod без изменения исходников executor. Активен только local host,
`reserve_cpu_fraction=0.5`, `respect_load=true`, один job и один поток.
Первоначальный отказ CPU не обходился; повторный preflight прошёл.

Реализован тот же L2 logistic objective с непенализируемым intercept, что
у sklearn, через явные elementwise reductions: это избегает матричных
операций, вызвавших численные warnings в E-286. Проверяются convergence,
конечность весов и градиент. Пять regression-тестов прошли; independent
sklearn optimizer даёт те же вероятности с допуском 1e-6.
Код и тесты: `a015d61`. Независимая проверка v2 не нашла блокирующих ошибок.

## Завершённый расчёт и кандидат

Кампания `run-4896dadb26047779d5475115`, job `retrain-general`, завершилась
с exit 0 за **811.62 s**. CPU user 757.78 s, system 29.14 s. Receipt подтвердил
неизменность snapshot и SHA восьми обязательных результатов. Единственный
разрешённый local host выполнил один job; unknown/failed нет.
Рабочий PID 51246 завершён, supervisor 50928 завершил сбор результатов.
Логи сохранены в `.orchestra/jobs/run-4896dadb26047779d5475115/retrain-general/`;
stderr обучения пуст. Пустой stdout во время расчёта объяснялся буферизацией
`pipe.read(65536)` в executor, а не остановкой процесса; этап ELO был проверен
по готовым файлам. Никакого второго расчёта не запускалось.

ELO: **17 604 / 17 604** целей покрыты; объединено 1 395 625 raw-карт,
обработано 1 395 623 завершения, неизвестных end-time нет; 251.58 s.
Истории: обработано 1 261 115 завершённых rich-строк, invalid 0; 549.75 s.
Пригодны **15 243 / 17 604 (86.6%)** карты: 7 615 `full` и 7 628 `no_org`.
Исключены 2 319 карт без достаточных account-признаков и 42 с конфликтом
позиций. Среди исключённых scorer выбирал `no_account_no_org` на 2 099 и
`no_account` на 220 картах; эти fallback-популяции не использованы для refit.

Полная ветка обучена на **15 205** картах, `no_org` — на **7 614**.
Остались **38 test-карт / 28 серий**: 24 карты `full`, 14 `no_org`.
Последний train label end `1788553304`, первый test start `1788753975`.
Test не стал больше от расширения train: reserved и прежняя временная
граница сохранены. Это тот же небольшой период, который уже исследовался.

| Фактическая test-популяция | Веса | n | Accuracy | AUC | Log loss ↓ | Brier ↓ |
|---|---|---:|---:|---:|---:|---:|
| full | прежние | 24 | 62.50% | 0.61806 | 0.63212 | 0.22511 |
| full | новые | 24 | 70.83% | 0.74306 | 0.57715 | 0.19822 |
| no_org | прежние | 14 | 50.00% | 0.65306 | 0.77383 | 0.27032 |
| no_org | новые | 14 | 64.29% | 0.77551 | 0.75098 | 0.25348 |
| Реальный выбор ветви | прежние | 38 | 57.89% (22/38) | 0.62881 | 0.68433 | 0.24176 |
| Реальный выбор ветви | новые | 38 | 68.42% (26/38) | 0.72299 | 0.64119 | 0.21858 |

Главное paired сравнение по 28 сериям: log-loss delta **−0.04314**, bootstrap
95% CI **[−0.09441; +0.01716]**. Интервал включает отсутствие улучшения.
Отдельные ветки и ранее изученный test не дают основания выбрать выгодный
подсрез и объявить стабильный эффект. Строка `interpretation` у paired-helper
унаследована из E-285 с его девятью рецептами; в E-287 новой сетки нет.
Baseline немного отличается от E-286: здесь используется текущий full-fit All,
там был evaluation All; сравнивать нужно строки одной таблицы на общих картах.

Проверка `runtime/experiments/misc/check_general_ml_retrain.py` прошла:

```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 runtime/experiments/misc/check_general_ml_retrain.py .orchestra/jobs/run-4896dadb26047779d5475115/retrain-general --report runtime/artifacts/misc/general_ml_retrain_20260912/independent_checks.json
```

Проверены receipt/output SHA, reserved/public SHA и нулевое пересечение,
временная граница и series-disjointness, метрики из отдельных прогнозов,
маршрутизация старых/новых ветвей, packing действующего scorer, full/top parity,
неизменность контекста и четырёх fallback-ветвей. Экспортированный NPZ точно
воспроизводит сохранённые прогнозы (максимальное отличие 0).
Независимый sklearn optimizer отличается на **3.54e−14** для full и **5.67e−7**
для no_org. У него повторились NumPy matmul warnings из E-286; они сохранены
в JSON проверки. Причина warnings не установлена, но веса, вероятности,
градиенты и независимая оптимизация проверены; сам training использовал
явные reductions и завершился без warnings.

Кандидат: `data/prematch_general_retrained_20260912/prematch_weights.npz`,
**7 333 bytes**, SHA
`361b48af9437742cc429398d2ad7550026660547035b5b614db8d3fc6ee07fdf`.
Рядом `manifest.json`, `parent_weights.npz`, `evaluation.json`, `protocol.json`
и `checks.json`; пять файлов проверены по SHA, manifest записан последним.
Это weights-only пакет, без snapshot history и без валидированной калибровки.
Новый будущий период отмечен временем выпуска в manifest; сбор/проверка
новых карт ещё не выполнены. Замороженный future-протокол E-286 не изменён.
Push, deployment, restart, включение collector/heartbeat не выполнялись.

## Где искать ошибку

1. Сверять manifest, receipt и SHA output, а не только exit code процесса.
2. Проверять keyed соответствие mid/start/end/win/accounts между rich и ELO;
   старый cache без mid здесь не применяется.
3. Проверять сортировку по завершению, strict end-time, embargo и отсутствие
   одной серии в train и test; reserved/public не должны попадать в fit.
4. Проверять фактическую ветку по mid: удаление h2h у full-карты не делает её
   представителем missing-org популяции.
5. Сверять context scaling и упаковку full/top, неизменность четырёх fallback
   ветвей и независимый пересчёт вероятностей из экспортированного NPZ.
6. Не выдавать повторно изученный маленький test, backfilled draft-признак
   или восстановленную историю за новый prospective результат. API observation
   timestamps, точная историческая serving parity и калибровка не доказаны.
