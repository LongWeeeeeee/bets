# E-299. ≥43: приоры после окончания карты и последовательные окна

19.09.2026; продолжение E-298 по указанию пользователя «делай»; Beads ingame-xpi0.

## STATUS

BLOCKED. Исправление проверено, протокол зафиксирован до rolling-оценки.
Shared executor не разрешил старт из-за CPU budget; jobs не созданы.
Ожидается свободный ресурс либо явное разрешение пользователя на временный
однопоточный прогон с пониженным приоритетом и лимитом 30 минут.
Offline; serving не меняется.

## SUMMARY

Проверяем устойчивость ≥43 и полезность причинной истории длительности.
Предыдущий September test уже просмотрен. Здесь четыре исторических окна
May–Aug 2026: это временная проверка с раздельным обучением, а не новый
независимый проспективный тест и не точный replay старого 928-column CatBoost.

Код старой сборки имеет дополнительные риски: глобальное среднее до фиксированного
теста доступно не всем ранним обучающим строкам; у некоторых legacy-кэшей не
доказана позиционная привязка IDs и временная доступность. Поэтому вместо
недостоверного восстановления старых 928 колонок используется полностью
воспроизводимый baseline: unsigned hero-role + 32 статистики истории.
Нормализация или готовые фазовые прогнозы, обученные после границ fold, не используются.

## CHANGED

- `base/end_time_prior.py`: векторный offline-приор по ключу, строго end < start;
  неизвестные ключи ≤0, невалидная длительность и недоступные значения исключаются.
- `base/tests/test_end_time_prior.py`: воспроизводитель E-298, одновременные
  границы/обратный порядок окончания, неизвестные ключи и brute-force parity.
- `runtime/experiments/misc/catalog_features.py`: старый CausalPrior заменён
  общим helper, передаются окончания; реальные account IDs вместо dense0;
  неизвестный account0 исключён. Сглаживание к фиксированной опоре 36 для dur
  и 0 для остальных величин. Среднее всех будущих pre-test строк удалено.
  Полный каталог не перестраивается; другие его блоки не объявляются причинными.
- `runtime/experiments/misc/duration43_rolling.py`: отдельный frozen harness.

## CHECKS

План, заданный до получения rolling-результатов:

| Test | Train ends before | Early stopping | Calibration |
|---|---|---|---|
| Май | 01.03 | март | апрель |
| Июнь | 01.04 | апрель | май |
| Июль | 01.05 | май | июнь |
| Август | 01.06 | июнь | июль |

История начинается 01.01.2023; train с 01.01.2024. В каждом блоке исключаются
карты, не завершённые до следующей границы; между блоками нет пересечения.
История обновляется по завершениям, включая уже завершённые карты тестового
периода: это последовательный as-of replay при фиксированных весах модели.
Задержка фактического поступления исхода не моделируется.

Три CatBoost: draft-only, +causal history, +unsafe start history (только
диагностический контроль). Для всех iterations=600, depth=6, lr=.05, l2=6,
seed299, early-stop80 по Logloss; без поиска гиперпараметров. У unsafe известны
результаты сразу после начала, но tie-старты исключены: это не точная копия
всех старых ошибок. Его результат никогда не кандидат для serving.

Для causal отдельно Platt, C=1 на calibration-месяце; raw тоже сохраняется.
Опоры истории заранее фиксированы: 36 минут, P≥43=.30, P≥36=.50; shrink20.
32 признака: mean/std/min/max по десяти слотам каждой из четырёх величин
(duration, P≥43, P≥36, log count), отдельно по игрокам и героям.
Энкодер героев обучается только на train. Основные метрики Logloss/Brier,
дополнительная AUC. Day bootstrap5000: causal−draft, Platt−causal,
unsafe−causal; интервалы95/99%. Нет автоматического выбора/внедрения.

Проверки до запуска: 5 regression tests passed; векторный history совпал
с прежним end-time heap replay на 150 строках, max abs error 1.91e-6.
Интеграция исправленного helper в legacy `slot_priors` проверена: у второй
незавершённой карты prior=36; изменение будущих величин на 1e6 не меняет
ни один доступный prior. Compile helper/legacy/rolling прошёл.
Ресурсы: только включённый local, shared executor, четыре независимых fold-job.

```bash
venv_catboost/bin/python3 -m pytest base/tests/test_end_time_prior.py -q
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/misc/duration43_rolling_20260919/campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/misc/duration43_rolling_20260919/campaign.json --background
```

## POINTERS

Харнесс и helper указаны выше; manifest и итоговые файлы:
`runtime/artifacts/misc/duration43_rolling_20260919/`.

## RISKS

Где искать ошибку: секунды/минуты и 2580; end == start; таймстемпы исходов
вместо поступления; утечка глобального среднего; fitted encoders/calibrator
за границей train/calibration; пропущенные ID и одинаковые герои; зависимость
карт одной серии/дня; перемена доли длинных карт. Корпус ретроспективный,
as-of truth позиций отдельно не подтверждён. Сравнение с прежней production
моделью на идентичных входах и прибыльность не проверяются.

## NEXT

После допуска ресурса запустить четыре jobs, пересчитать метрики по сохранённым прогнозам и записать
положительный или отрицательный результат без подбора на этих test-окнах.
Текущие результаты E-298 не подменяют ещё не выполненный E-299.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Offline-only duration>=43",
    "Four retrospective monthly folds, no prospective claim",
    "Do not change CPU policy without authorization"
  ],
  "sources": [
    {
      "id": "S1",
      "path": "runtime/artifacts/misc/duration43_rolling_20260919/preflight_checks.json",
      "sha256": "4ac259dab8f4e7fe819c162471b2b56f18569686e4ddff3c72e912bc15a2bd4f"
    },
    {
      "id": "S2",
      "path": "runtime/artifacts/misc/duration43_rolling_20260919/blocked_preflight.json",
      "sha256": "2b4460452c0f57e73eac2f74b2705b65e4f91e36286a6c87829c8bb391ba67c7"
    },
    {
      "id": "S3",
      "path": "base/end_time_prior.py",
      "sha256": "d05a5c0d58d157fdd9322f3b63b3a6d9df6547f02e0c5055a78791cbfdbd07b9"
    },
    {
      "id": "S4",
      "path": "runtime/experiments/misc/catalog_features.py",
      "sha256": "2c63150aeed6a36dce90d4c679c6637158df718d988dc26af82b8bc6de1ec770"
    },
    {
      "id": "S5",
      "path": "runtime/artifacts/misc/duration43_rolling_20260919/campaign.json",
      "sha256": "43ea5b8b38c06dc4a8b8ac223a34385b9fe0578a563bbeaecc279f0af13a48ea"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "Keyed history queries strict end<start and uses fixed shrinkage references.",
      "sources": [
        "S3",
        "S4"
      ]
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Vectorized history matches heap replay; legacy integration excludes future-label effects.",
      "sources": [
        "S1"
      ]
    },
    {
      "id": "F3",
      "kind": "OBSERVED",
      "claim": "CPU preflight blocked; no jobs started.",
      "sources": [
        "S2"
      ]
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Rolling fold performance has not been measured.",
      "scope": "All four planned May-Aug fold jobs"
    }
  ],
  "checks": [
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "S1"
      ],
      "observed": "5 regression tests; heap parity max error1.91e-6; legacy integration invariant."
    },
    {
      "id": "T2",
      "status": "FAIL",
      "sources": [
        "S2"
      ],
      "observed": "no conservatively available CPU budget; no_jobs_started true"
    }
  ],
  "limitations": [
    "Code fix verified, requested rolling evaluation pending",
    "Old full928 matrix/cache causality not established",
    "No production changes"
  ],
  "contradictions": [],
  "decision_required": [
    "Wait for normal CPU capacity or explicit user exception; do not infer approval from silence"
  ]
}
```
