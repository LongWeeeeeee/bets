# E-310 — переобучение шести моделей убийств с OpenDota-историей

Дата: 21.09.2026. Только offline; production не менялся.

> **Исходный прогон; выводы ниже пересмотрены.** Независимая проверка обнаружила
> учёт незавершённых исторических окон и несогласованные вероятности сторон
> оконных моделей. Исправления, повторное обучение и ограничения неиспользованных
> данных описаны в [аудите E-310](E-310-kills-audit.md). Эта таблица сохранена
> как исторический результат v1, не как актуальная оценка исправленных моделей.

## STATUS

DONE — шесть целей × пять вариантов обучены, артефакты проверены; offline.

## SUMMARY

Цели: преимущество по убийствам в окнах 5–15, 10–20, 15–25, 20–30;
личные убийства стороны ≥30 и сумма личных убийств карты ≥55.
Проверяем добавочную информацию истории OpenDota и опыта игрока/героя/позиции.
Текущие данные предсказываемой карты не являются признаками.

Источники и пререгистрация: `runtime/artifacts/kills/opendota_retrain_20260921/`:
`protocol.json`, `data_audit.json`, `source_join_audit.json`,
`window_label_audit.json`, `raw_sample_audit.json`, `source_inputs.json`.


Полный train run завершён: local 6/6 jobs, exit0, все входные снимки повторно
проверены после выполнения. Обучены 30 моделей; для шести целей сохранён
выбор, сделанный только на августовском selection. Все 102 объявленных
артефакта проверены по SHA256 и собраны. Канонические model/calibrator
совпадают по хешу с выбранной веткой; все 30 пар воспроизводят сохранённые
прогнозы с atol=1e-12. Прежние resource settings восстановлены побайтно.

| Цель | Карт terminal | Выбран на selection | LL baseline → selected | Accuracy selected | 95% CI ΔLL по дням |
|---|---:|---|---|---:|---|
| Окно 5–15 | 361 | experience | 0.66984 → 0.65862 | 61.22% | [-0.01776; -0.00525] |
| Окно 10–20 | 384 | experience | 0.67282 → 0.66717 | 59.11% | [-0.01273; +0.00027] |
| Окно 15–25 | 355 | experience | 0.67155 → 0.66477 | 60.85% | [-0.01610; +0.00163] |
| Окно 20–30 | 288 | combined | 0.68452 → 0.68824 | 54.69% | [-0.00218; +0.00941] |
| Сторона ≥30 | 404 | experience | 0.65729 → 0.64412 | 63.61% | [-0.01971; -0.00634] |
| Карта ≥55 | 404 | experience | 0.63219 → 0.62772 | 62.87% | [-0.00996; +0.00099] |

ΔLL = selected minus baseline; меньше лучше. Accuracy для окон/side30 —
по двум наблюдениям сторон карты; для total55 — одна строка на карту.
Эти стороны не считаются независимыми в bootstrap: пересэмплируются дни.
Интервалы: 1000 day-cluster bootstrap draws, 17 дней, без поправки на шесть
сравнений. Они не покрывают неопределённость всей процедуры выбора модели.

По заранее заданному критерию (верхняя граница CI ΔLL<0) прирост выбранных
новых признаков подтверждён на этом архиве для 5–15 и side30. Для 10–20,
15–25 и total55 выигрыш по LL небольшой и неопределённый; для 20–30 LL хуже.
Отрицательный результат сохранён. Выбор 20–30 не заменяли задним числом
по terminal. Основным выигравшим блоком оказался experience.

Все terminal log loss по веткам (не правило повторного выбора):

| Цель | baseline | timelines | experience | combined | combined_dpxp |
|---|---:|---:|---:|---:|---:|
| Окно 5–15 | 0.66984 | 0.67197 | 0.65862 | 0.66384 | 0.65911 |
| Окно 10–20 | 0.67282 | 0.67806 | 0.66717 | 0.67344 | 0.67210 |
| Окно 15–25 | 0.67155 | 0.68428 | 0.66477 | 0.68270 | 0.68070 |
| Окно 20–30 | 0.68452 | 0.69118 | 0.68537 | 0.68824 | 0.69033 |
| Сторона ≥30 | 0.65729 | 0.66343 | 0.64412 | 0.65275 | 0.65786 |
| Карта ≥55 | 0.63219 | 0.62182 | 0.62772 | 0.62333 | 0.62508 |

Timeline-блок отдельно ухудшил LL пяти целей и улучшил total55; это
наблюдение не используется для повторного выбора. DotaPlus не дал
равномерного улучшения относительно combined и остаётся exploratory
из-за неподтверждённого времени доступности исторических значений.
Численные Brier/AUC/ECE, calibration bins, selection LL и точные прогнозы
сохранены для всех веток в `results.json` и каталогах целей.

## CHANGED

Новый харнесс `base/tools/kills_opendota_research.py`, регрессии
`base/tests/test_kills_opendota_research.py`. Исходная SQLite не изменялась.
Используется новый pro-only baseline с тем же рецептом и выборками,
а не переоценка точного production E-281 bundle с пабликовыми профилями.

Варианты: baseline; timelines; experience; combined; combined_dpxp.
Baseline: категории героев и средняя предыдущая pro-статистика игроков
(kills/deaths/assists/GPM/XPM). Timelines: исторические приросты deaths,
gold/xp/lh/dn каждого окна, сглаженная история игрока на герое, объёмы
истории и очищенные командные убийства по окнам. Gold означает total gold,
не net worth. Experience: количество предыдущих игр игрока, на герое и
на выведенной позиции, частота роли и давность предыдущих игр. Представление
содержит агрегаты стороны и разницу с соперником. Позиция текущей карты
назначается только по прошлым частотам ролей; OpenDota lane_role не является
позицией 1–5. DotaPlus: последняя предыдущая dotaPlusHeroXp для player/hero,
отдельная исследовательская ветка, исключённая из основного выбора.

Каждый источник попадает в состояние только при `source.end < query.start`.
Совпадающие карты rich/SQLite объединяются один раз; повторяющиеся rich MID
отклоняются. Стороны одной карты и известные серии не разделяются между
разделами. Неизвестные series ID считаются отдельными картами: для них
отсутствие пересечения серии не доказано.

Разделы UTC: train 01.06–01.08; early stop 01.08–10.08; calibration
10.08–20.08; selection 20.08–01.09; retrospective terminal 01.09–17.09.
Источник до июня служит прогревом истории. События после границы раздела
не используются как его обучающие исходы. CatBoost depth=5, lr=.05,
l2=12, max400, early stop40, seed20260921. Логистическая калибровка на
отдельном calibration-блоке. Выбор по selection log loss среди первых
четырёх вариантов сохраняется до оценки terminal. Для total55 две ориентации
усредняются до калибровки и оценки; оценка даёт одну строку на карту.

## CHECKS

До полного запуска: py_compile PASS; 10 регрессионных тестов PASS.
Проверяются временная граница, отсутствующие значения, текущие postgame
поля, ничьи/короткие/непарсенные окна, серии, rich-дубликаты, SQLite→NPZ,
сохранение и воспроизведение всех моделей и калибраторов с atol=1e-12.

Фактический корпус: 16552 manifest-карты, 16523 complete, 26 partial,
3 HTTP404; SQLite содержит 16549 карт / 165490 игроков. quick_check=ok.
Все 16549 карт точно совпали с rich по времени, длительности,
стороне/account/hero. Исходный rich SHA256:
`ff2234cd623bfa36456c0772f7e4136012928fcbf04e5fcea669f48bebea60b6`.
SQLite SHA256:
`dfb971f2165331dd18325bd8ebf06f04efbdd1f15557b34e84b8fc8b534c0f5f`.

## POINTERS

Воспроизводимые команды (из корня, Python только venv_catboost):

```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -m pytest base/tests/test_kills_opendota_research.py -q
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/kills/opendota_retrain_20260921/build_campaign_v2.json
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/kills/opendota_retrain_20260921/build_campaign_v2.json --background
```

Build v2 `run-81a4635bf5a3e61dad995fce` завершён: 16523 карты,
461632 дополнительных rich-источника, 17947 query accounts. Датасет:
`data/kills_opendota_20260921/dataset.npz`, SHA256
`b0add5122c2e9e2d7bd6c6317b9bd4f8f0660faf07f4da92d9bfcc43ff1b1bb2`.
Размеры блоков: baseline20, timelines128, experience12, DotaPlus2.
Train/stop/calibration/selection/terminal: 11143/1171/394/411/404 карты;
3000 карт оставлены только для истории либо исключены purge.
Terminal — 17 дней; 20 из 404 карт без известного series ID.
Медиана усреднённого по стороне player-hero history в terminal — 20.3 карты;
медиана window support player/hero — 3.4. Состав историй существенно
различается между временными разделами; подробности `history_support.json`.

Train run `run-adfcf2a7189fa20030852a92`, manifest `train_campaign.json`:

```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/kills/opendota_retrain_20260921/train_campaign.json --background
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources status --run run-adfcf2a7189fa20030852a92
```

Первый build `run-d13baae63550563c43d0409e` завершился до обработки данных
с ошибкой открытия WAL-mode SQLite в readonly snapshot. Исправлено
`mode=ro&immutable=1` для законченного снимка, добавлена WAL-регрессия.
Пользователь разрешил временный один поток nice10 без load gate;
исходные настройки сохранены в `resources_before.json`, разрешение и хеши
в `resource_exception.json`. После вычислений исходные настройки восстановлены побайтно; хеши и время
восстановления зафиксированы в `resource_exception.json`.

Полные запуски используют shared executor, замороженные хешированные входы,
отдельные выходные каталоги и receipt. Включён только host local; один job.

## RISKS

### Где искать ошибку

1. Collector считал пустой kills_log как нули: все 26 partial исключаются из
   всех query targets и timeline history. Общая когорта этого прогона —
   complete-карты; частично разобранные карты не используются для обучения.
2. Raw kills_log включает Lone Druid bear. Оконные targets строятся по сумме
   пяти противоположных exact-hero deaths. Исправление меняет знак/ничью на
   65/60/66/83 картах четырёх окон. Это credited hero kills, без neutral/suicide.
   Индивидуальные raw kill-window признаки исключены; финальные player.kills
   — другой источник и намеренно сохранённый контракт E-281.
3. Personal-sum ≥55 отличается от scoreboard ≥55 на 265 картах: это разные
   цели, результаты нельзя переносить между ними без отдельной проверки.
4. В выборке 100 complete raw maps times[index] совпал с boundary; проверка
   индексации выборочная. Не утверждается аудит каждого raw JSON.
5. Историческая доступность dotaPlusHeroXp на дату матча не подтверждена;
   сдвиг на предыдущую карту не устраняет риск ретроспективного backfill.
6. Сентябрь ранее открывался в других исследованиях. Это ретроспективный
   terminal, не новый независимый holdout. Нельзя настраивать рецепт по нему.
7. Новый baseline не равен production: прирост над ним не доказывает
   превосходство над production, калибровку live-сигналов или ROI ставок.

## NEXT

Обучение и offline-доставка завершены. Модели:
`ml-models/kills_opendota_20260921/<target>/`; внутри каждой цели —
выбранные `model.cbm`, `calibration.joblib`, `schema.json`, `selection.json`,
`metrics.json`, `predictions.npz`, `manifest.json` и все ветки `candidates/`.
Сводка и проверка артефактов: `runtime/artifacts/kills/opendota_retrain_20260921/results.json`.
Production/serving-контракт не менялся; эти модели не являются готовой
заменой E-281. Следующий независимый этап для применения — точный парный
production comparator и будущая выборка с доступными до матча признаками.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Six kills targets only; offline, no production deployment.",
    "Selection on August only; September retrospective, no terminal tuning.",
    "DotaPlus exploratory excluded from primary selection."
  ],
  "sources": [
    {
      "id": "S1",
      "path": "runtime/artifacts/kills/opendota_retrain_20260921/results.json",
      "sha256": "18b7e80d570ec453c0409e2d945c5ab3bf1ef9a27f7443149fb31a6514ee6571"
    },
    {
      "id": "S2",
      "path": "runtime/artifacts/kills/opendota_retrain_20260921/dataset_audit.json",
      "sha256": "e7a909d7fef491afc5413ff81882f89677a377aa42c8cf2ceb52cdfddff001cc"
    },
    {
      "id": "S3",
      "path": "runtime/artifacts/kills/opendota_retrain_20260921/resource_exception.json",
      "sha256": "e843ef820930849363c697f00ec7c40127c487b6f17b41ab6445b0407359ef49"
    },
    {
      "id": "S4",
      "path": ".orchestra/campaigns/run-adfcf2a7189fa20030852a92/state.json",
      "sha256": "d1a0512b1df35fb33b00ef1aef7b83c7ba17e601e419c3def1c6ed6226e05b30"
    },
    {
      "id": "S5",
      "path": "runtime/artifacts/kills/opendota_retrain_20260921/protocol.json",
      "sha256": "068756977eeefc5d7c80af1b3030105fe94cdf5232aeced5386b77112d8de356"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "All six target jobs completed with exit0; all102 artifact hashes and30 model/calibrator replay checks passed.",
      "sources": [
        "S1",
        "S4"
      ]
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Built16523 complete query maps; terminal404 maps before target-specific eligibility.",
      "sources": [
        "S2"
      ]
    },
    {
      "id": "F3",
      "kind": "OBSERVED",
      "claim": "Original resource configuration restored byte-for-byte after authorized temporary1thread exception.",
      "sources": [
        "S3"
      ]
    },
    {
      "id": "C1",
      "kind": "DERIVED",
      "claim": "Two of six selected candidates meet preregistered negative upper95CI logloss-delta criterion against new pro-only baseline.",
      "basis": [
        "F1"
      ],
      "method": "Compare results.targets[target].chosen_minus_baseline.day_cluster_95ci[1] against0; unadjusted day-bootstrap intervals."
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Exact production superiority, future validation, live feature parity and betting ROI.",
      "scope": "Not part of offline retraining."
    }
  ],
  "checks": [
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "S1",
        "S4"
      ],
      "observed": "6/6 completed jobs,102 verified artifacts,30 saved-pair replay at1e-12,canonical selected hashes match."
    }
  ],
  "limitations": [
    "Terminal archive previously exposed in other research.",
    "17day terminal; unknown series IDs on20of404 maps.",
    "No multiplicity adjustment.",
    "Baseline is newly trained pro-only, not production E281.",
    "DotaPlus original observation timestamp unverified."
  ],
  "contradictions": [
    "More timeline features did not improve all targets: selected20-30 had worse terminal logloss."
  ],
  "decision_required": []
}
```
