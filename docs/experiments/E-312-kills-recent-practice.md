# E-312 — недавняя практика игрока/героя/позиции, 22.09.2026

Решаем: добавляет ли недавняя практика информацию моделям убийств сверх
накопленного опыта и давности последней игры, без смешения с DotaPlus?

## STATUS

DONE

Offline обучение и независимый пересчёт завершены.

## SUMMARY

Уточнение пользователя: DotaPlus hero XP накапливается при подписке;
низкий тир не означает малое число игр на герое. Поэтому XP **исключён
из всех новых обучаемых вариантов**. Старый `X_dpxp` и описание его матрицы
оставлены только для воспроизведения прошлых артефактов.

В E-310 уже были число наблюдавшихся игр игрока/героя/позиции и время с
последней завершённой игры игрока и игрока на герое. Новый `X_recent`
добавляет для 7/30/90 дней по пять величин на игрока:

- число завершённых игр;
- число игр на текущем герое;
- число игр на предполагаемой текущей позиции;
- долю героя среди игр игрока;
- долю позиции среди игр игрока.

Период точно `[query.start − N*86400, query.start)` по времени **завершения**
исторического матча. Текущая карта и ещё не завершённые карты исключены.
Позиция текущего игрока назначается по предшествовавшим ролям, без текущего
исхода/статистики. Отсутствие знаменателя даёт NaN доли. Средние пяти игроков
стороны и разность own−opponent образуют 30 признаков.

Это число **наблюдённых в доступном корпусе** игр, не полная история всех
пабликов игрока. Ноль по позиции также может означать отсутствие наблюдённой
истории ролей. Значения не интерпретируются как доказательство отсутствия практики.

Гипотеза до запуска: недавняя практика добавляет информацию сверх суммарного
опыта. Опровержение: парные сравнения `recent_experience` с `experience`
и `combined_recent` с `combined` не подтверждают улучшение. Решение: какой
offline вариант сохранить выбранным; перенос в production здесь не решается.

Датасет содержит 16523 карты. Все прежние массивы из исправленного E-310
совпали по значениям: baseline/timeline/experience/DPXP, метки, ID, времена,
series и split. Добавлен только X_recent (16523×2×30), 97.46% значений конечны.

Шесть вариантов: baseline, timelines, experience, combined,
recent_experience = baseline+experience+recent,
combined_recent = baseline+timeline+experience+recent. Шесть прежних целей:
5–15, 10–20, 15–25, 20–30, сторона≥30 и карта≥55.
CatBoost400/depth5/lr.05/l2=12/seed20260921, early stop40, отдельная logistic
calibration, прежние даты и series purge. Выбор по августовскому selection.
Отдельно сохраняются парные ΔLL recent−experience и combined_recent−combined.
Прежний terminal уже открыт; повторное сравнение retrospective, не свежий holdout.

### Наблюдённый результат

36 моделей обучены, все сохранены. Добавочная польза недавней практики
**не подтверждена для всех целей**. Ниже Δlog loss: отрицательное значение лучше;
в скобках 95% интервал bootstrap по 17 UTC-дням, без поправки на множественность.

| Цель | recent_experience − experience | combined_recent − combined |
|---|---:|---:|
| 5–15 | -0.00341 [-0.00680; -0.00019] | -0.00159 [-0.00452; +0.00148] |
| 10–20 | +0.00362 [-0.00193; +0.00942] | +0.00072 [-0.00237; +0.00353] |
| 15–25 | +0.00400 [-0.00020; +0.00891] | +0.00024 [-0.00127; +0.00172] |
| 20–30 | -0.00197 [-0.00477; +0.00084] | -0.00194 [-0.00346; -0.00015] |
| Команда ≥30 | +0.00930 [+0.00320; +0.01532] | +0.00119 [-0.00426; +0.00664] |
| Карта ≥55 | +0.00055 [-0.00788; +0.00875] | +0.00218 [-0.00684; +0.01105] |

| Цель | Выбор по августовскому selection | Accuracy experience | Accuracy recent_experience |
|---|---|---:|---:|
| 5–15 | experience | 60.39% | 57.62% |
| 10–20 | experience | 60.16% | 59.38% |
| 15–25 | recent_experience | 60.85% | 58.59% |
| 20–30 | recent_experience | 53.82% | 55.56% |
| Команда ≥30 | experience | 63.61% | 60.27% |
| Карта ≥55 | experience | 62.87% | 63.37% |

Выбор сохранён по заранее указанному августовскому selection. В частности,
для 15–25 выбран recent_experience, хотя на повторно просмотренном сентябре
он хуже experience; менять победителя по этому результату нельзя без нового
правила и нового независимого периода. Для 5–15 log loss улучшился, но accuracy
при 0.5 снизилась — это разные критерии, вывод о росте WR не следует.

Для recent_experience интервал улучшения исключает ноль только у 5–15;
ухудшения — у команды ≥30. Для combined_recent улучшение видно у 20–30,
остальные интервалы включают ноль. Это локальные ретроспективные признаки,
не доказательство универсального преимущества. Хронологически выбранные
кандидаты сохранены; прежние E-310 модели также сохранены.

## CHANGED

`base/tools/kills_opendota_research.py`: история времён завершения,
`Experience.recent_games`, `History.recent_features`, отдельный X_recent,
согласованное описание матриц вариантов, исключение DotaPlus из новых fits.
Schema `kills-opendota-research-v3`; прежние вероятностные контракты сохранены.

`base/tests/test_kills_opendota_research.py`: включение нижней границы,
исключение query-time, непоследовательные timestamps, доли героя/позиции,
независимость от DotaPlus, shape/order и model replay всех трёх типов целей.

## CHECKS

16 tests passed (первый прогон 3.83s; изолированная зафиксированная копия
кода — 1.74s, exit0, `regression.log`). Независимый read-only review не нашёл блокирующих ошибок
в границах, назначении позиции, знаменателях, порядке признаков и вариантах.

Build: `run-dca607faa4124e6024a06aad`, completed, snapshot verified.
Train: `run-5db7989957debddbe6bb7698`, completed: 6 targets × 6 arms.

Независимый аудит: 114 SHA256 артефактов; 36 наборов метрик и воспроизведений
CBM/calibrator; 24 прежних контрольных прогноза неизменны. Несовпадений меток
с SQL нет. Максимальная ошибка replay 0, метрик ≤1.12e−16. Выбор варианта,
три семейства day-bootstrap и выбранные model/calibrator hashes проверены.
Ресурсный override восстановлен побайтно; SHA256 совпал с исходным.

Прямой пересчёт recent из источников на 25 картах (1500 значений):
максимальная ошибка 0. Расчёт не использует History/recent_games, однако
переиспользует загрузчики источников — это независимость вычислений, не второй
полностью независимый парсер. Первый фоновый запуск не оставил результата;
повторный запуск с ожиданием процесса завершился с exit0, лог attempt2 сохранён.

Автоматический `task ready` заблокирован существующими submodules:
`submodules need explicit supervised verification`. Задание автодоставки
отменено без изменения файлов; выполнена проверка точной копии tool/tests
и явный локальный коммит только пяти файлов задачи. Сверка хешей проверенного
кода с коммитом сохраняется в `delivery_reconciliation.json`.

Харнесс и команды:

```sh
venv_catboost/bin/python3 -m pytest base/tests/test_kills_opendota_research.py -q
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources status --run run-5db7989957debddbe6bb7698
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 venv_catboost/bin/python3 runtime/artifacts/kills/recent_practice_20260922/verify_models.py
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 venv_catboost/bin/python3 runtime/artifacts/kills/recent_practice_20260922/verify_recent_features.py
```

Точные argv, версии зависимостей и input SHA256: `build_campaign.json` и
`train_campaign.json` в `runtime/artifacts/kills/recent_practice_20260922/`.
План фиксировался до обучения (`protocol.json`). Для нового запуска нужен
новый task_id и отдельный output; завершённые jobs не дублировать.

**Где искать ошибку:** `dataset_audit.json` — сохранность прежних массивов;
`Experience.recent_games` — границы по end-time; `recent_features` — доли
до усреднения игроков и history-only роль; `ARM_BLOCKS` — соответствие матриц
именам признаков. `verify_recent_features.py` пересчитывает признаки прямо
из строк источников без History; `verify_models.py` независимо восстанавливает
прогнозы из CBM/calibrator, метки SQL, метрики и day-bootstrap.

## POINTERS

- Датасет: `data/kills_recent_practice_20260922/`.
- Модели: `ml-models/kills_recent_practice_20260922/`.
- Артефакты/проверки: `runtime/artifacts/kills/recent_practice_20260922/`.
- Receipts/logs: `.orchestra/jobs/<run>/<target>/`.
- Предыдущее сравнение: [исправленный E-310](E-310-kills-audit.md).

## RISKS

История не исчерпывающая. Новые дни выбраны заранее, но terminal уже известен;
дальнейшая настройка по нему не создаст независимое доказательство качества.
Day-bootstrap использует17 дней, без поправки на множественные сравнения.
Оконные вероятности условны на отсутствии ничьей, не равны вероятности
выигрыша ставки в действующей policy. ROI/production-превосходство не измерялись.

## NEXT

Offline пакет завершён; переноса в production нет. Новый независимый период
и полная наблюдаемая история пабликов нужны для следующей проверки гипотезы.
Это ограничения результата, а не незавершённые действия текущего эксперимента.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Kills models only; offline; one CPU thread nice10; restore resources",
    "DotaPlus XP is subscription-confounded; no low-XP implies low-practice assumption",
    "Preserve previous models; no production deployment"
  ],
  "sources": [
    {
      "id": "S1",
      "path": "runtime/artifacts/kills/recent_practice_20260922/dataset_audit.json",
      "sha256": "2cad0cfd0e48ceb6046820fb06721b735f0a0939e31a2261639047b6ee4b7fda"
    },
    {
      "id": "S2",
      "path": "runtime/artifacts/kills/recent_practice_20260922/corrected_verification.json",
      "sha256": "1f2ae45dccdcde4ce86cb45f02ea7945c03708f1231904ba029b919f8d5f29fe"
    },
    {
      "id": "S3",
      "path": "runtime/artifacts/kills/recent_practice_20260922/recent_feature_verification.json",
      "sha256": "891869aa68889e944500c6d62a27566a25078cb6fdbf4b241ab0001070164d62"
    },
    {
      "id": "S4",
      "path": "runtime/artifacts/kills/recent_practice_20260922/results.json",
      "sha256": "827b08ee6d69215d06e51ce6e1c2c2d209334f13433bd7b07c79c99c5cf3c6cf"
    },
    {
      "id": "S5",
      "path": "runtime/artifacts/kills/recent_practice_20260922/resource_exception.json",
      "sha256": "e760cc3d414379efbe8b87e66c44cb6a4a449d7cf78394b9b3fc1fcf617e2818"
    },
    {
      "id": "S6",
      "path": "runtime/artifacts/kills/recent_practice_20260922/verification.log",
      "sha256": "9a527ade476f3a6633c39e8af29713cf789dded463fd9ce981670a90b355501b"
    },
    {
      "id": "S7",
      "path": "runtime/artifacts/kills/recent_practice_20260922/regression.log",
      "sha256": "84a4b13c7c32d2b5a5f71c6611e7178d3394ff3e6d92c8a076dcb921330f757d"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "All previous dataset arrays unchanged; 30 recent features added",
      "sources": [
        "S1"
      ],
      "scope": "16523 maps compared to E310v2"
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "36 saved-model replays match; 24 old controls unchanged; SQL label mismatches zero",
      "sources": [
        "S2",
        "S6"
      ],
      "scope": "six targets and six arms"
    },
    {
      "id": "F3",
      "kind": "OBSERVED",
      "claim": "1500 recent values independently recomputed with maximum error zero",
      "sources": [
        "S3"
      ],
      "scope": "25 maps; shared source loaders, independent counts"
    },
    {
      "id": "F4",
      "kind": "OBSERVED",
      "claim": "Paired recent effects vary by target; side30 log loss worsens and 5-15 log loss improves versus experience",
      "sources": [
        "S4"
      ],
      "scope": "reused September evaluation; 17 day bootstrap, unadjusted intervals"
    },
    {
      "id": "F5",
      "kind": "OBSERVED",
      "claim": "Resources restored to original bytes",
      "sources": [
        "S5"
      ],
      "scope": "resources.json immediately after training"
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Fresh holdout advantage and production betting ROI",
      "scope": "future and production performance"
    }
  ],
  "checks": [
    {
      "id": "models",
      "status": "PASS",
      "sources": [
        "S2",
        "S6"
      ],
      "observed": "114 hashes,36 metric/replay sets,24 unchanged controls; exit0"
    },
    {
      "id": "recent",
      "status": "PASS",
      "sources": [
        "S3"
      ],
      "observed": "1500 values, max error 0; attempt2 exit0"
    },
    {
      "id": "resources",
      "status": "PASS",
      "sources": [
        "S5"
      ],
      "observed": "Restored SHA256 equals original"
    },
    {
      "id": "regression",
      "status": "PASS",
      "sources": [
        "S7"
      ],
      "observed": "16 passed in1.74s; isolated scoped source copy; exit0"
    }
  ],
  "limitations": [
    "Observed corpus does not cover every public match",
    "Window probabilities conditioned on no tie",
    "Terminal already exposed; no multiple comparison correction",
    "Accuracy and log loss can move in opposite directions",
    "Automatic whole-tree delivery blocked by pre-existing submodules; supervised scoped verification used"
  ],
  "contradictions": [],
  "decision_required": []
}
```
