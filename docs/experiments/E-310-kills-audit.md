# E-310 — независимая проверка и исправленное обучение, 21.09.2026

## STATUS

DONE — исправлены ошибки, повторно обучены и независимо проверены все 30 вариантов.
Исходный E-310 сохранён для сравнения, новые артефакты имеют суффикс `_v2`.

## SUMMARY

Независимый пересчёт исходных 30 вариантов подтвердил сохранённые метки,
log loss, Brier, accuracy, AUC, ECE и интервалы day-bootstrap с погрешностью
менее 1e-12. Ошибка была в постановке расчёта признаков и вероятностей,
а не в арифметике отчётных метрик.

1. **Незавершённые исторические окна учитывались как полные.** Карта могла
   закончиться до конца окна, но коллектор сохранял конечный счётчик смертей
   в последующих границах. Для 5–15/10–20/15–25/20–30 это 44/337/1151/3035
   complete-карт. Временные ряды теперь допустимы только при
   `source.duration >= window.end`: иначе NaN без увеличения поддержки.
   Правило едино для deaths/gold/xp/lh/dn и командных hero kills.
2. **Оконные вероятности сторон не составляли единого распределения.** При
   взаимоисключающих метках обе стороны независимо проходили калибровку.
   На terminal одновременно имели p≥0.5: 32/20/36/39 карт по четырём окнам;
   обе p<0.5: 18/16/20/30. Теперь `qR=(rawR+1-rawD)/2`, затем калибратор
   оценивает одну Radiant-метку карты; `pD=1-pR`. Обучение CatBoost сохраняет
   две ориентации как аугментацию. side30 остаётся независимым для сторон;
   total55 по-прежнему усредняет raw-ориентации до калибровки.
3. **Схема артефактов вводила в заблуждение:** `total55_rule` присутствовало
   у всех целей. Теперь `orientation_rule` описывает конкретную цель.

Исправление истории изменило 407900 чисел на 14417 из 16523 карт.
`X_baseline`, `X_experience`, `X_dpxp`, метки, ID, времена и split совпадают
точно на уровне значений массивов с исходным датасетом.

### Что действительно использовалось и что нет

- Использовались OpenDota deaths/gold/xp/lh/dn по четырём окнам, их поддержка,
  история командных убийств героев, K/D/A/GPM/XPM из SQLite, draft hero IDs,
  число предшествующих игр игрока/героя/назначенной позиции и давность игр.
  История применялась только после `source.end < query.start`.
- Дополнительные 461632 карты rich-корпуса обогащают опыт, прежние роли и
  lagged DotaPlus. **Другие 13 pstats rich-корпуса не использовались**:
  kills, deaths, assists, goldPerMinute, experiencePerMinute, networth,
  numLastHits, numDenies, heroDamage, towerDamage, heroHealing, imp, level.
  Это ограничение охвата, а не доказательство бесполезности этих данных.
- Producer `runtime/experiments/misc/pro_corpus_rich.py` (89–97) пишет
  `float(p.get(k) or 0.0)`, теряя различие между отсутствием/null и реальным
  нулём. Presence mask в NPZ нет. Поэтому нельзя безопасно добавить все
  эти поля без восстановления пропусков из raw JSON.
- DotaPlus уже использовался в `combined_dpxp`, но его нули неоднозначны,
  а время исторического наблюдения не подтверждено. Эта ветка остаётся
  **только диагностической**, исключена из выбора. Полезность корректно
  восстановленного `dotaPlusHeroXp` этим опытом не установлена.
- Индивидуальные `kills_log`-приращения намеренно исключены: содержат события
  медведя Lone Druid. Командные hero kills считаются через смерти пяти
  противников. Финальные side30/total55 сохраняют контракт личных kills.
- Измерено использование блоков моделями: добавленные признаки имеют
  ненулевую importance. Это проверка подключения, не доказательство пользы.

### Ограничения постановки

Оконные метки исключают ничьи. Вероятности означают `P(lead | no tie)`,
не безусловную вероятность выигрыша ставки. Текущая window-policy использует
expected_diff + NW/lane-гейты и считает ничью проигрышем. E-310 не является
проверенной заменой этой policy. side30/total55 совпадают с численными целями
serving, но baseline здесь новый pro-only, а не точный production E-281.

Терминальный архив уже использован в E-308/E-309/E-310. Повторная проверка
исправлений — retrospective, не новый holdout. Гиперпараметры, даты,
series-purge и правило выбора по selection не менялись. Интервалы из 1000
пересэмплирований 17 UTC-дней не скорректированы на шесть сравнений и не
включают неопределённость выбора модели. Не измерялись ROI, live thresholds,
историческая доступность DotaPlus и полный raw missingness.

### Исправленные результаты

| Цель | Карт terminal | LL baseline → experience | Accuracy | 95% CI ΔLL |
|---|---:|---|---:|---|
| Окно 5–15 | 361 | 0.66976 → 0.65892 | 60.39% | [-0.01744; -0.00487] |
| Окно 10–20 | 384 | 0.67339 → 0.66738 | 60.16% | [-0.01275; -0.00010] |
| Окно 15–25 | 355 | 0.67780 → 0.67077 | 60.85% | [-0.01578; +0.00085] |
| Окно 20–30 | 288 | 0.68720 → 0.68806 | 53.82% | [-0.00314; +0.00473] |
| Сторона ≥30 | 404 | 0.65729 → 0.64412 | 63.61% | [-0.01971; -0.00634] |
| Карта ≥55 | 404 | 0.63219 → 0.62772 | 62.87% | [-0.00996; +0.00099] |

Все шесть выбранных на августовском selection веток — `experience`.
Добавление timelines не выиграло selection ни у одной цели в этом рецепте.
В v1 для 20–30 выбиралась `combined`; после исправления выбирается `experience`.
Для side30/total55 baseline и выбранный experience сохранили исходные значения.
У 5–15 и side30 интервалы заметно ниже нуля. У 10–20 верхняя граница
лишь −0.000099: пограничный результат без поправки на множественные сравнения.
Для остальных трёх целей интервал включает ноль; 20–30 имеет худший point estimate.
Accuracy окон теперь по одной Radiant-строке на карту; side30 — по двум
сторонам, total55 — по одной карте. Оконные метрики v1/v2 имеют разную
калибровочную постановку; их нельзя читать как чистый эффект новых данных.

## CHANGED

- `base/tools/kills_opendota_research.py`: censor исторических окон, единый
  расчёт raw-прогнозов цели перед calibration/evaluation, schema v2.
- `base/tests/test_kills_opendota_research.py`: короткие source-карты, точная
  граница окна, три вероятностных контракта, replay и schema каждого типа.
- Этот отчёт, предупреждение в исходном E-310, индекс экспериментов и
  `docs/CODE_MAP.md`: контракт offline артефактов, без изменения live policy.

## CHECKS

14 regression tests passed, в том числе на замороженной копии только
собственных исходников без чужого изменённого conftest. Полная автоматическая
проверка дерева заблокирована существующими submodules; они не изменялись. Read-only reviewer проверил исправления,
блокирующих замечаний нет. Исходные 30 метрик и SQL-метки проверены независимо;
известные series не пересекают splits, ориентации признаков согласованы.

Исправленный build: `run-ff9fc3a4489f4769522deb6c`, completed, exit0,
входной snapshot проверен до и после. Dataset SHA256:
`ac60a4849c941c02865c467d80a7e3712a205634cb86f074831f2a7129dc645a`.
Обучение: `run-1a298f7c22241b930b7b0462`, local 6/6 completed, exit0;
все snapshot_verified_after=true. Независимо проверены 102 SHA256, SQL-метки
всех шести целей, 30 наборов метрик и 30 сохранённых model/calibrator пар
с atol=1e-12. Bootstrap явно пересчитан через строки выбранных дней.
`verification.log` завершён PASS. Настройки ресурсов восстановлены побайтно
(`resource_exception.json`), согласованный режим был 1 поток, nice=10.

Воспроизведение и проверка (из корня репозитория):

```sh
venv_catboost/bin/python3 -m pytest base/tests/test_kills_opendota_research.py -q
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources status --run run-1a298f7c22241b930b7b0462
```

Точные argv, зависимости и input SHA256 закреплены в
`runtime/artifacts/kills/opendota_audit_20260921/build_campaign.json` и
`train_campaign.json`. Повторный новый эксперимент требует нового task_id
и каталога результатов; завершённые jobs не запускаются повторно.

**Где искать ошибку:** сравнить `dataset_diff.json` с исходным E-310;
`short_source_audit.json` и `_timeline_values`; `_raw_prediction_for_target`
для порядка aggregation→calibration; `schema.json` для ориентации;
`independent_metrics.json` для исходного независимого пересчёта.
`verify_corrected.py` содержит независимую реконструкцию прогнозов из CBM,
калибратора и feature matrices, SQL-метки и явное пересэмплирование строк по дням.

## POINTERS

- Аудит: `runtime/artifacts/kills/opendota_audit_20260921/`.
- Датасет: `data/kills_opendota_20260921_v2/`.
- Исправленные модели: `ml-models/kills_opendota_20260921_v2/`.
- Исходное сравнение: [E-310](E-310-kills-opendota-retrain.md).
- Доказательства вычислений: `.orchestra/jobs/<run>/<target>/receipt.json`,
  `stdout.log`, `stderr.log`, `output/`; сохранены исходные файлы.

## RISKS

Выше явно разделены ошибки, исправленные в этом ходе, и непроверенные данные.
Нельзя утверждать, что использованы все полезные признаки или что выигрыш над
новым baseline означает превосходство production. Ничьи и missingness требуют
отдельной корректной постановки перед расширением или переносом в serving.

## NEXT

Проверка и исправленный прогон завершены. Перед новым расширением признаков
нужна raw-derived presence mask; перед serving — оценка окон с ничьими и
сравнение с действующей policy на новом периоде. Это оставшиеся ограничения,
не выполненные в этом ходе эксперименты. Production/deployment/restart не менялись.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Offline only; no deploy/restart/push",
    "Preserve v1 artifacts and unrelated work",
    "Single thread nice10 exception authorized by user; original resources restored"
  ],
  "sources": [
    {
      "id": "S1",
      "path": "runtime/artifacts/kills/opendota_audit_20260921/independent_metrics.json",
      "sha256": "694ea35f9215c865298ab83c3b430012768b4dcfdb41c24e3dcb4ccbf60a6a8d"
    },
    {
      "id": "S2",
      "path": "runtime/artifacts/kills/opendota_audit_20260921/short_source_audit.json",
      "sha256": "bb38b36dae04cce3a8f83464e39f1af9247634da248a76af3d5bb0b4d97cf24d"
    },
    {
      "id": "S3",
      "path": "runtime/artifacts/kills/opendota_audit_20260921/dataset_diff.json",
      "sha256": "d954abd272d8c6b1a0110549870729b2eaf51618f43bc0be5e919a4be2f93e29"
    },
    {
      "id": "S4",
      "path": "runtime/artifacts/kills/opendota_audit_20260921/corrected_verification.json",
      "sha256": "b3b44400f94ce85c453affdd1bd424f41ba46a826e442c9a071409bf136881c7"
    },
    {
      "id": "S5",
      "path": "runtime/artifacts/kills/opendota_audit_20260921/results.json",
      "sha256": "b517b07421f6d0e010809813b80fcb1a4c069d6b4695d8f943af49a543213dba"
    },
    {
      "id": "S6",
      "path": "runtime/artifacts/kills/opendota_audit_20260921/regression.log",
      "sha256": "2081c900c2e4859301f2b088b4f077899ac07ac889b57fd28b5808eda466e97b"
    },
    {
      "id": "S7",
      "path": "runtime/artifacts/kills/opendota_audit_20260921/resource_exception.json",
      "sha256": "92acc982fe77ff3173ed341193a7c147bb43db246eaaddf93189c74a84b38a43"
    },
    {
      "id": "S8",
      "path": "base/tools/kills_opendota_research.py",
      "sha256": "f82fafe314a85932bf514ed80aa71fa262f6e68967aa96ded56d8021a117f758"
    },
    {
      "id": "S9",
      "path": "runtime/experiments/misc/pro_corpus_rich.py",
      "start_line": 89,
      "end_line": 97,
      "sha256": "c750c73255b4996a3460a69ffde51bb89886a8fcddc8fa099c72c00586409fa7"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "Original saved metrics reproduce, but source histories included incomplete windows and window side predictions were incoherent.",
      "sources": [
        "S1",
        "S2"
      ]
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Corrected censor changed 407900 timeline values on14417 maps; all other dataset arrays equal.",
      "sources": [
        "S3"
      ]
    },
    {
      "id": "F3",
      "kind": "OBSERVED",
      "claim": "Six corrected targets completed;102 hashes,30 independent saved-model replay/metric checks and SQL labels passed. All six selected experience.",
      "sources": [
        "S4",
        "S5"
      ]
    },
    {
      "id": "F4",
      "kind": "OBSERVED",
      "claim": "14 scoped frozen-source tests passed; original resource bytes restored.",
      "sources": [
        "S6",
        "S7"
      ]
    },
    {
      "id": "F5",
      "kind": "OBSERVED",
      "claim": "Rich producer conflates absent/null and zero;13 rich pstats unused and DotaPlus exploratory excluded from primary selection.",
      "sources": [
        "S8",
        "S9"
      ]
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Raw presence masks, DotaPlus historical observation time, tie-as-loss live policy performance, production baseline superiority, fresh holdout and ROI.",
      "scope": "outside completed retrospective correction"
    }
  ],
  "checks": [
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "S4"
      ],
      "observed": "Independent verification exit0:102 artifact hashes and30 replays/metrics; all label mismatches0; tolerance1e-12."
    },
    {
      "id": "T2",
      "status": "PASS",
      "sources": [
        "S6"
      ],
      "observed": "Frozen scoped pytest exit0:14 passed."
    }
  ],
  "limitations": [
    "Retrospective reopened terminal;17day bootstrap intervals unadjusted for six targets",
    "Window predictions condition on no tie; not live betting-policy probabilities",
    "Source missingness not recovered; no full rich pstats expansion",
    "Scoped tests only; no production deployment"
  ],
  "contradictions": [
    "Original blanket wording about retraining all production kills models and timeline usefulness was too broad; v1 report now marked superseded."
  ],
  "decision_required": []
}
```
