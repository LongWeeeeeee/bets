# E-299. ≥43: приоры после окончания карты и последовательные окна

19.09.2026; продолжение E-298 по указанию пользователя «делай»; Beads ingame-xpi0.

## STATUS

DONE

Четыре задания завершены exit0, прогнозы независимо проверены. Пользователь
разрешил однопоточный прогон nice10 до 30 минут; 4 × timeout420s. Исходная
конфигурация ресурсов восстановлена побайтно после первого dispatch.
Offline; serving не менялся.

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
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/misc/duration43_rolling_20260919/campaign_approved.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/misc/duration43_rolling_20260919/campaign_approved.json --background
```

Результаты, log loss ниже — лучше:

| Test | Карт | Draft | + causal history | + Platt | Causal AUC |
|---|---:|---:|---:|---:|---:|
| Май | 6817 | 0.644591 | 0.618393 | 0.617274 | 0.625968 |
| Июнь | 6922 | 0.640875 | 0.606500 | 0.605328 | 0.649895 |
| Июль | 4242 | 0.643533 | 0.604606 | 0.603840 | 0.649650 |
| Август | 1983 | 0.658733 | 0.617313 | 0.617932 | 0.646061 |
| Вместе | 19964 | 0.644482 | 0.611233 | 0.610343 | 0.641181 |

OBSERVED: causal−draft LL = −0.033250; paired day-bootstrap95%
[−0.037403, −0.029125], 99% [−0.038586, −0.028039], 123 UTC-дня.
Brier 0.226035 → 0.213267 → 0.212902. Доля ≥43: 0.346123;
средняя вероятность causal 0.367895, Platt 0.349946.
Platt−causal = −0.000890; 95% [−0.001546, −0.000214],
99% [−0.001715, +0.000001]; ухудшение в августе сохраняется в отчёте.
Unsafe−causal = +0.000311; 95% [−0.000425, +0.000984].
Этот контроль не показывает значимого выигрыша от раннего раскрытия исходов
и не измеряет завышение старого AUC с другими признаками и другими утечками.

Независимая проверка: 4 receipts exit0 и post-run input snapshots; 16 SHA256;
метрики пересчитаны до 1e-12; target2580, временные границы и отсутствие
пересечений test IDs подтверждены. Сохранены 12 моделей и 8 transformers.
Во всех четырёх sklearn-калибровках были RuntimeWarning в matmul.
Без повторного fitting проверены конечные коэффициенты, ручная sigmoid,
совпадение прогнозов до 1e-12 и нормализованный градиент регуляризованной
цели <1e-4 (максимум 1.31e-5). Предупреждения не скрыты; сырые CatBoost
оценки от калибровки не зависят.

```bash
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources collect --run run-2dcf60a5ff61532186ec2781
venv_catboost/bin/python3 runtime/experiments/misc/verify_duration43_rolling.py --run run-2dcf60a5ff61532186ec2781 --output-dir runtime/artifacts/misc/duration43_rolling_20260919/completed
```

## POINTERS

Харнесс и helper указаны выше; manifest и итоговые файлы:
`runtime/artifacts/misc/duration43_rolling_20260919/`.
Итог: `completed/rolling_summary.json`, `completed/pooled_predictions.npz`,
`completed/model_inventory.json`; разрешение и восстановление: `resource_exception.json`.
Run: `.orchestra/campaigns/run-2dcf60a5ff61532186ec2781/state.json`;
модели и логи: `.orchestra/jobs/run-2dcf60a5ff61532186ec2781/<fold>/`.

## RISKS

Где искать ошибку: секунды/минуты и 2580; end == start; таймстемпы исходов
вместо поступления; утечка глобального среднего; fitted encoders/calibrator
за границей train/calibration; пропущенные ID и одинаковые герои; зависимость
карт одной серии/дня; перемена доли длинных карт. Корпус ретроспективный,
as-of truth позиций отдельно не подтверждён. Сравнение с прежней production
моделью на идентичных входах и прибыльность не проверяются.

## NEXT

DERIVED: причинная история полезна относительно зафиксированного draft baseline
во всех четырёх окнах. Калибровка — небольшой и неоднородный дополнительный эффект.
До serving нужен отдельный проект сравнения с действующей моделью на общих
доступных входах и проверка поступления признаков в реальном времени.
Фазовые добавки E-298 пока не показали пользы; этот прогон их повторно не обучал.
Никакие пороги ставок, deployment или profitability здесь не выбирались.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Offline duration>=43; four retrospective windows",
    "One CPU thread nice10; four jobs timeout420s; configuration restored",
    "No serving or production changes"
  ],
  "sources": [
    {
      "id": "S1",
      "path": "runtime/artifacts/misc/duration43_rolling_20260919/completed/rolling_summary.json",
      "sha256": "ff83ed7598faff66ca5c2734cd7bb98e300f3b9f4bf34e5d7c032b50ac6af799"
    },
    {
      "id": "S2",
      "path": "runtime/artifacts/misc/duration43_rolling_20260919/resource_exception.json",
      "sha256": "aac08ebf6a38be7de190b0a03d075ec0c969180a104f8e42b89ccf764f73abd7"
    },
    {
      "id": "S3",
      "path": "runtime/artifacts/misc/duration43_rolling_20260919/completed/model_inventory.json",
      "sha256": "e434888eddd201621c6eb07ea35feadf6c16464c9f0db711751aa16b2600afec"
    },
    {
      "id": "S4",
      "path": "runtime/artifacts/misc/duration43_rolling_20260919/preflight_checks.json",
      "sha256": "4ac259dab8f4e7fe819c162471b2b56f18569686e4ddff3c72e912bc15a2bd4f"
    },
    {
      "id": "S5",
      "path": ".orchestra/campaigns/run-2dcf60a5ff61532186ec2781/state.json",
      "sha256": "6475b48e64ceda3dcaf08b9f39e92c8550333f8a5bb926ac91c99e68ebb2bf0b"
    },
    {
      "id": "S6",
      "path": "runtime/artifacts/misc/duration43_rolling_20260919/campaign_approved.json",
      "sha256": "fa1b78201740daddec35c3b299a038bddcd63ced18b188678fbf14ee57c9ec6d"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "19964 test maps; draft LL .644482, causal .611233, Platt .610343; causal improves each month.",
      "sources": [
        "S1"
      ]
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Four completed jobs; approved bounded exception and exact resource restoration recorded.",
      "sources": [
        "S2",
        "S3",
        "S5",
        "S6"
      ]
    },
    {
      "id": "F3",
      "kind": "OBSERVED",
      "claim": "All calibration numeric checks passed despite recorded runtime warnings.",
      "sources": [
        "S1"
      ]
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Superiority over current production model, prospective accuracy and profitability.",
      "scope": "Old928 replay and live serving"
    }
  ],
  "checks": [
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "S1"
      ],
      "observed": "Recomputed metrics, temporal targets, unique test IDs, 16 collected hashes, four calibration gradients and manual prediction parity."
    },
    {
      "id": "T2",
      "status": "PASS",
      "sources": [
        "S4"
      ],
      "observed": "Five regression tests, heap parity and legacy integration future-label invariance."
    }
  ],
  "limitations": [
    "Retrospective evaluation, not untouched prospective holdout",
    "Role source as-of and arrival delay not established",
    "Unsafe control is not an exact old928 leakage replay",
    "Calibration worsened August and pooled99 interval crosses zero"
  ],
  "contradictions": [],
  "decision_required": []
}
```
