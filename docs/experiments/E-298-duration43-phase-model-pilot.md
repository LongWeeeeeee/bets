# E-298. Длительность ≥43 минут: аудит и временной пилот фазовых моделей

Дата: 19.09.2026. Задача Beads: `ingame-xpi0`.

## STATUS

DONE. Аудит и предзаданный offline-пилот завершены; метрики пересчитаны из
сохранённых предсказаний. Улучшение ≥43 от фазовых моделей не подтверждено.
Serving, прод и пороги ставок не менялись.

## SUMMARY

Исходный `dur43.cbm` — CatBoost, обученный 16.08.2026: 928 колонок,
depth=6, learning_rate=0.03, l2_leaf_reg=6, 1246 оставленных деревьев.
Цель строго `duration_seconds >= 2580`. Метаданные старой матрицы:
365176 train-карт до 23.10.2024, 91294 validation до 28.03.2026,
26016 test до 11.08.2026. Доля ≥43: 23.386%, 28.389%, 34.098%.
Early stopping, изотоника и порог использовали один validation-блок.

Первоначальный отчёт: AUC 0.6554 и порог уверенности 0.610. После изменения
правила отбора порог стал 0.990: 2.1% теста, 99.3% попаданий, ровно 99.3%
у постоянного ответа в выбранной полосе. Это не доказательство преимущества
в предсказании длинных карт. Старые 928 признаков точно воспроизвести сейчас
нельзя: `panel_matrix.npy` и каталог признаков отсутствуют; rich-корпус
обновлён до 1261401 карты вместо старых 482486.

OBSERVED: старый `catalog_features.py:CausalPrior` сортирует историю по времени
начала и исключает только текущую строку, не ожидая окончания предыдущих карт.
Микропример с одинаковым ключом и стартами 100/110 секунд: изменение ещё
неизвестной длительности первой карты с 60 до 100 минут меняет prior второй
с 45 до 65 минут (shrink=1, global=30), хотя доступной истории ещё нет и prior
должен оставаться 30. В текущем rich-корпусе Jan–Aug11 2026 у 42592 из 44439
карт есть хотя бы один hero-slot с незавершённым предыдущим матчем этого героя.
Для account-slot такой случай в этом срезе не найден. Это диагностирует
генератор и текущий корпус; величина завышения старого AUC **не измерена**.
Точный прежний X отсутствует, поэтому старое утверждение «утечек нет»
недостаточно обосновано для доступности исходов по end time.

Проверяется идея пользователя: уверенность Early NW / Early Win / All / Late
и разногласия между ними могут помогать цели ≥43. Early Win предсказывает
победителя условно на длительности 20–34; это не вероятность короткой карты.
Early NW даёт условное направление и отдельную вероятность наличия маркера;
оба компонента включены без использования фактического маркера.

OBSERVED: итоговый тест 05–18.09.2026 — 466 карт, 114 (24.46%) ≥43,
14 UTC-дней. Base train — 110545, meta train — 625 карт. Все модели
оценены на одном тесте; конфигурации зафиксированы до его просмотра.

| Вариант | AUC ↑ | Log loss ↓ | Brier ↓ |
|---|---:|---:|---:|
| Постоянная частота meta-train | 0.5000 | 0.57812 | 0.19404 |
| Новая прямая ≥43 без второго уровня | 0.6050 | **0.53902** | **0.18356** |
| Прямая ≥43 + калибровка второго уровня | 0.6050 | 0.55353 | 0.18957 |
| + Early NW / Early Win / All / Late | 0.6004 | 0.55432 | 0.19013 |
| + новые winner-модели диапазонов | 0.5879 | 0.56704 | 0.19593 |
| + прямые пороги ≥32/36/40/43/47 | **0.6122** | 0.55185 | 0.18925 |
| Всё вместе | 0.5982 | 0.56768 | 0.19672 |

Парный day-bootstrap Δlogloss к калиброванной ≥43 (меньше лучше):
четыре phase +0.00078, 95% CI [−0.00873; +0.01126]; новые winner bands
+0.01351 [+0.00580; +0.02096]; соседние пороги −0.00168
[−0.00681; +0.00340]; всё вместе +0.01415 [−0.00036; +0.02915].
99% интервалы также сохранены. Это описательные интервалы по 14 дням,
не широкая проверка устойчивости по турнирам/патчам и не автоматический отбор.

DERIVED: на этом тесте преимущество phase-добавки не подтверждено;
соседние пороги дают лишь небольшой неубедительный прирост. Доля ≥43
снизилась с 34.08% в meta-train до 24.46% в test. Калибровка на недавнем
коротком блоке ухудшила raw direct43; причинный вклад сдвига отдельно не
изолирован. У raw direct43 при p≥0.5 было 7 карт и 0 попаданий, у варианта
+phase — 37 карт и 7 попаданий при среднем p=55.8%. Малые выборки, но
достоверность высоких вероятностей и готовность over-сигналов не показаны.

Дополнительная описательная оценка уже обученных моделей (без подбора):
прямые ≥32/36/40/43/47 имеют AUC 0.6347/0.6426/0.6466/0.6050/0.6090.
Новые winner-модели [20,34), [34,43), [43,+∞) на своих условных подвыборках:
N=182/164/114, AUC 0.6530/0.5879/0.6413. Это качество победителя при
**уже известном диапазоне**; при прогнозе длительность неизвестна, поэтому
такой AUC не доказывает пригодность моделей для выбора диапазона.

## CHANGED

Только offline-харнесс, его артефакты и эта запись. Старый `dur43` не заменяется.

## CHECKS

Протокол до просмотра итогового теста:

- История приоров с 01.01.2023: исход доступен строго после окончания карты.
  Текущая, одновременно начавшаяся и ещё не завершённая карта не входит.
- Пять новых прямых классификаторов ≥32/36/40/43/47 на unsigned hero-role
  и 32 сводных признаках прошлой длительности игроков и героев.
  LogisticRegression C=0.03; обучение на картах с 01.01.2025,
  завершившихся до 14.08.2026. Предзаданные параметры, без поиска.
- Три новых классификатора победителя на диапазонах [20,34), [34,43),
  [43,+∞) минут; signed hero-role-pair, C=0.01, без intercept.
  Те же границы обучения. Это дешёвый pro-пилот, не полный public-fit.
- Четыре существующие phase-модели: только `evaluation_model.joblib`,
  fit/selection cutoff до 14.08; `serving` full-fit здесь недопустим.
  Для признаков длительности направление симметризовано как
  `(p_R(original) + 1 - p_R(swapped))/2`, затем берутся уверенность и
  попарные произведения направлений. Исходные вероятности сохраняются.
- Второй уровень: LogisticRegression C=0.1, обучение 14.08–04.09,
  с исключением карт, не завершившихся к 05.09. Тест 05.09–18.09.
- Контроли: частота класса; новая прямая ≥43; её отдельная калибровка.
  Фиксированные варианты: +четыре phase; +новые winner bands;
  +прямые пороги; всё вместе. Сравнение с калиброванной новой ≥43.
- Главные метрики: log loss и Brier; AUC и доля ≥43 в верхней пятой части
  прогнозов — дополнительные. Отбор over отдельно от under.
  Парный bootstrap по UTC-дням, 5000 повторов, интервалы 95% и 99%.
  Автоматического выбора победителя или внедрения нет.

Харнесс: `runtime/experiments/misc/duration43_phase_pilot.py`.
Ресурсы: `.orchestra/resources.json` — единственный включённый host `local`.
Запуск через executor, который фиксирует SHA и изолированную копию входов:

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/misc/duration43_audit_20260919/campaign.json
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/misc/duration43_audit_20260919/campaign.json --background
```

Первый запуск `run-9cbaf9f0fcf74e41b96638e1` остановлен assert до оценки
финального теста: фазовые winner-модели имеют допустимый Radiant intercept,
поэтому исходное требование `p(original)+p(swapped)=1` было неверным.
Исправлено только преобразование в признаки длительности, а не phase-модели.
Повторный manifest имеет task_id `duration43-phase-pilot-20260919-r2`.
Завершённый run: `run-43ff9daa94a85a94449e33d7`, exit=0, input snapshot
проверен после работы; local, один поток. Повторного обучения после просмотра
теста не было. Проверены уникальные test IDs, target 2580 секунд, раздельность
meta/test, окончание всех test-карт до 19.09 и точное совпадение пересчитанных
AUC/logloss/Brier с отчётом. Встроенные проверки end-time, ties, текущего
исхода и обмена сторон прошли. NW occurrence симметричен на всех future rows.
Симметризованные phase scores — признаки, а не отдельно калиброванные
вероятности победы; probability-average не равен удалению logit-intercept.

## POINTERS

- `runtime/experiments/misc/build_panel_models.py`: исходное обучение.
- `runtime/artifacts/misc/{build_panel_models,recalibrate_panel}.md`: старые результаты.
- `ml-models/prematch_panel/{dur43.cbm,panel.json,manifest.json}`: артефакты.
- `runtime/artifacts/misc/duration43_audit_20260919/campaign.json`: frozen inputs.
- `runtime/artifacts/misc/duration43_audit_20260919/training_audit.json`: воспроизводитель старого prior и диагностический срез.
- `runtime/artifacts/misc/duration43_audit_20260919/completed/`: metrics, protocol, predictions, feature_evidence, verification, auxiliary_metrics.
- `.orchestra/jobs/run-43ff9daa94a85a94449e33d7/temporal-pilot/output/`: сохранённые модели первого и второго уровней.
- `.orchestra/input-snapshots/9c3aa85a4e143884b84b403d3b1a30086dcdfc68cdd7e309b51aed4871301cb9/`: неизменяемые входы и точный харнесс.

## RISKS

Где искать ошибку: единицы секунд/минут и включённость 43:00; отсутствие
полной исходной X-матрицы; временная доступность исхода по end time;
полный serving-fit вместо evaluation-fit; поломка порядка позиций или
знака при обмене сторон; неизвестные account IDs; маленький September test;
сдвиг доли длинных карт; зависимость карт одного дня/серии; ретроспективный
корпус вместо проспективного захвата. Этот пилот не сравнивает новую модель
с прежним CatBoost на одинаковых картах и не измеряет прибыльность.

## NEXT

Следующий обоснованный эксперимент: восстановить полноценный baseline с
приорами по окончанию карт, разнести early stopping/калибровку, проверить
несколько последовательных временных окон. Для этого нужен новый независимый
тест; September test данного пилота уже просмотрен. Phase stacking пока не
внедрять. Массовое обучение вариантов и production-переключение не выполнялись.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Primary target duration >=43 minutes",
    "Include Early NW, Early Win, All, Late and both new direct thresholds and conditional winner bands",
    "Offline only; preserve serving and unrelated files"
  ],
  "sources": [
    {
      "id": "S1",
      "path": "runtime/artifacts/misc/duration43_audit_20260919/completed/metrics.json",
      "sha256": "485e39244fa39ef25ee9dfdbf066a9f266c112ab582dee3edbe360160f3bce4d"
    },
    {
      "id": "S2",
      "path": "runtime/artifacts/misc/duration43_audit_20260919/completed/verification.json",
      "sha256": "eb8ed6f0d84ebe3e58fa9b380a3baf2005ddc7946e696f2049dcb9a86378f466"
    },
    {
      "id": "S3",
      "path": "runtime/artifacts/misc/duration43_audit_20260919/training_audit.json",
      "sha256": "c7b646013ae6a50e86ac220e644e44af96396006a36729e79c1489fd0511a1ba"
    },
    {
      "id": "S4",
      "path": "runtime/artifacts/misc/duration43_audit_20260919/completed/auxiliary_metrics.json",
      "sha256": "976a136c8bbc122434b0d93e5371ac56a3582ae0769ba6eb63be43326995c66a"
    },
    {
      "id": "S5",
      "path": ".orchestra/jobs/run-43ff9daa94a85a94449e33d7/temporal-pilot/receipt.json",
      "sha256": "f97188fa52f8f976976380c02948c322148407ba71281226fe0eb1301d938880"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "On 466 held-out maps phase stacking logloss .554315 vs calibrated baseline .553531; CI95 of paired difference includes zero.",
      "sources": [
        "S1"
      ],
      "scope": "One fixed September holdout and fixed pilot configurations"
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Old CausalPrior output responds to duration unavailable at the next start; old matrix AUC impact is unmeasured.",
      "sources": [
        "S3"
      ]
    },
    {
      "id": "F3",
      "kind": "OBSERVED",
      "claim": "Three new winner bands and five duration classifiers trained; auxiliary conditional AUCs evaluated without retuning.",
      "sources": [
        "S4",
        "S5"
      ]
    },
    {
      "id": "D1",
      "kind": "DERIVED",
      "claim": "This pilot does not justify replacing the existing duration model with phase stacking.",
      "basis": [
        "F1"
      ],
      "method": "No lower held-out logloss or paired confidence interval excluding zero in favor of phase addition."
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Same-cohort comparison to the old 928-feature CatBoost unavailable because original X is missing.",
      "scope": "Old panel model superiority, current production predictions and profitability"
    }
  ],
  "checks": [
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "S2"
      ],
      "observed": "Recomputed metrics from saved predictions; unique IDs, >=2580 target and test timing verified."
    },
    {
      "id": "T2",
      "status": "PASS",
      "sources": [
        "S5"
      ],
      "observed": "Executor exit0 and input snapshot verified after run."
    },
    {
      "id": "T3",
      "status": "PASS",
      "sources": [
        "S3"
      ],
      "observed": "Reproduced old CausalPrior unavailable-outcome dependence."
    }
  ],
  "limitations": [
    "Only 466 test maps across14 days; retrospective source corpus",
    "Calibrated baseline worse than raw direct43; no candidate promotion",
    "Phase orientation-averaged scores are features, not calibrated winner probabilities",
    "Previous failed attempt stopped on symmetry check before final evaluation; retained in run-9cbaf9f0fcf74e41b96638e1"
  ],
  "contradictions": [
    "Historic no-leakage claim does not account for concurrent unfinished hero histories"
  ],
  "decision_required": []
}
```
