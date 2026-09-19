# E-300. ≥43: сравнение с журналом действующей модели

19.09.2026; продолжение E-299 по «делай»; Beads ingame-xpi0.
Решаем: превосходит ли причинный кандидат действующую duration-модель.

## STATUS

BLOCKED

Сравнение сохранённых live-прогнозов на одинаковых ID завершено и проверено.
Строгое сравнение на одинаковых предматчевых входах заблокировано отсутствием
входных снимков: журнал не хранит heroes/accounts/roles/928-vector/model hash,
и в сопоставленной выборке нет записей до начала карты. Прод не менялся.

## SUMMARY

OBSERVED: read-only snapshot `serv1:/root/main/runtime/ml_panel.jsonl` содержит
12057 строк, 513 уникальных непустых числовых ID. 167 ID отсутствуют в пригодном
корпусе; для 346 есть карта. Берётся первая запись dur43 каждого ID;
7 записей уже после окончания исключены. Осталось 339 карт, 119 ≥43,
25 UTC-дней; 90 карт августа и 249 сентября. Ни одного прогноза до старта:
лаг min95s, median851s, max3672s. Это logged-live cohort, не prematch replay.

| Модель | Log loss ↓ | Brier ↓ | AUC ↑ |
|---|---:|---:|---:|
| Действующая, raw из журнала | 0.614229 | 0.213519 | 0.649427 |
| Действующая, calibrated из журнала | **0.614072** | **0.213394** | **0.649427** |
| Кандидат E-299 August, raw | 0.633895 | 0.221637 | 0.604813 |
| Кандидат E-299 August, Platt | 0.633808 | 0.221628 | 0.604813 |

Кандидат Platt − действующая calibrated: ΔLL +0.019736,
paired UTC-day bootstrap5000: 95% [0.001385, 0.036240],
99% [−0.005065, 0.041677]. Raw-кандидат: +0.019823,
95% [0.003312, 0.035054]. Положительная разность означает ухудшение.

| Месяц | N | Действующая calibrated | Кандидат Platt |
|---|---:|---:|---:|
| Август | 90 | 0.649139 | 0.691647 |
| Сентябрь | 249 | 0.601397 | 0.612902 |

Средняя вероятность Platt 0.351005 почти совпала с фактом 0.351032,
но ранжирование и индивидуальные вероятности хуже: совпадение средней
частоты само по себе не означает улучшение модели.

Противоречащее слишком широкому выводу наблюдение: post-hoc срез первых
5 минут (45 карт) лучше у кандидата, LL0.578006 против0.590201;
на 5–20 минутах (284) хуже:0.631837 против0.607408. После20 минут (10)
тоже хуже:0.940898 против0.910745. Это описательная диагностика после
основного результата, не основание выбрать новый временной гейт.

## CHANGED

Только offline-харнесс `runtime/experiments/misc/duration43_incumbent_compare.py`,
его локальные результаты и документация. Обучения, изменения кандидата,
замены артефактов и production-рестартов не было. Инференс одного frozen
кандидата на 339 картах, один CPU-поток; ресурсная конфигурация не менялась.

## CHECKS

До просмотра метрик зафиксировано: модель и encoder E-299 August, отдельный
сохранённый Platt; общая выборка по первой записи ID до окончания, start≥17.08.
Гипотеза — кандидат улучшает LL относительно журнала. Опровержение —
положительная парная разность; решение — есть ли основание продолжать к замене.
Ни модель, ни калибровка не переобучались. История реконструирована только по
окончаниям строго раньше старта; будущие исходы текущей карты исключены.
Кандидат обучен до01.06, early stopping в июне, калибровка в июле.
Действующая модель сохранена16.08; все сравниваемые карты позже.

Проверено независимо по сохранённым прогнозам:
- 339 уникальных ID, target duration_seconds≥2580, forecast_ts<end;
- точная первая запись и raw/p совпадают с замороженным журналом;
- LL/AUC/Brier пересчитаны до1e-12; парная разность совпала с разностью LL;
- 90 августовских прогнозов кандидата полностью совпали с E-299 при
  расширении causal-history до сентября: max abs error0;
- текущая изотоника incumbent воспроизводит журнал до4.941e-7 (p округлён
  до6 знаков). SHA256 локальных и серверных dur43/panel/feature_names совпали;
  историческая версия артефакта каждой записи всё равно не записана.

Первый запуск остановился при составлении помесячного отчёта: к y применена
маска месяца, к p — нет. Исправлена только индексация массива p; исходный лог
и протокол сохранены. Второй запуск exit0. Никакого подбора по тесту.

```bash
venv_catboost/bin/python3 runtime/experiments/misc/duration43_incumbent_compare.py
```

Харнесс отказывается перезаписывать завершённый результат. Для повторения
нужен новый каталог OUT, тот же snapshot журнала и frozen модели.

## POINTERS

- `runtime/artifacts/misc/duration43_incumbent_20260919/protocol_r2.json` — гипотеза, правила, SHA входов.
- В том же каталоге `comparison.json`, `predictions.npz`, `verification.json`,
  `serv1_ml_panel.jsonl`, `inference.log`, `inference_r2.log`.
- `.orchestra/jobs/run-2dcf60a5ff61532186ec2781/aug/output/` — замороженные модели.
- `base/ml_panel.py:247` — поля журнала; отсутствие input snapshot.
- `base/prematch_panel_scorer.py:144` — заполнение отсутствующих групп neutral.
- `runtime/experiments/misc/build_panel_models.py:162` — target dur≥43.

## RISKS

Где искать ошибку: ID карты против серии; первая запись против последней;
секунды2580; прогноз после конца; отсутствующие167карт и селекция журнала;
actual draft roles против ретроспективных corpus roles; версия модели/кэшей;
доступность признаков к моменту прогноза и зависимость карт по турнирам/дням.
Старой panel_matrix.npy нет, журнал928-vector не хранит. Состояние roster/
ratings/cache старого прогноза не восстановлено; равенство входной информации
не доказано. Срез имеет только25дней и уже просмотрен. 99%CI включает0.
Подмножество early<5min показывает обратное направление. Нельзя утверждать,
что кандидат универсально хуже, или что измерена прибыльность ставок.

## NEXT

DERIVED: на доступном logged-live сравнении причинный кандидат не подтвердил
превосходство над incumbent; оснований для замены нет. E-299 доказал выигрыш
над простым draft baseline, а не над действующей моделью.

Для строгого сравнения нужен перспективный захват общего draft/accounts/roles,
времени доступности и входных векторов вместе с hash моделей и прогнозами обеих
моделей до старта. Его deployment не выполнялся и в этот read-only этап не входит.
Верю: замена сейчас не обоснована. Опровергнет: независимый общий предматчевый
срез с устойчивым выигрышем. Следующий шаг: отдельный prospective shadow capture.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Offline only, no fitting or serving changes",
    "Exact prematch comparison requested; missing inputs explicitly reported"
  ],
  "sources": [
    {
      "id": "S1",
      "path": "runtime/artifacts/misc/duration43_incumbent_20260919/comparison.json",
      "sha256": "28a8e8f60d336ce2af846494f5b207bbd9fc5c751217ce6277fac08631b038ba"
    },
    {
      "id": "S2",
      "path": "runtime/artifacts/misc/duration43_incumbent_20260919/verification.json",
      "sha256": "41737eb770f1d857cc93d4f97c320a612e0c4accb9be4110ba862d61def73329"
    },
    {
      "id": "S3",
      "path": "runtime/artifacts/misc/duration43_incumbent_20260919/protocol_r2.json",
      "sha256": "40f67ac047f1a147853964b1ab090102054d7f6a12ebc90fa10946d7816c07cf"
    },
    {
      "id": "S4",
      "path": "base/ml_panel.py",
      "sha256": "a797e4bbbfbfd3fc1ab395c6a6770e5b8bfa9771b756724fff5f7f943396c507"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "339 shared live maps; candidate Platt LL .633808 versus recorded incumbent .614072.",
      "sources": [
        "S1"
      ]
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Zero joined prematch records; seven after-end records excluded; input vectors not journaled.",
      "sources": [
        "S1",
        "S4"
      ]
    },
    {
      "id": "F3",
      "kind": "OBSERVED",
      "claim": "Independent metrics and exact logged ID matching pass; August candidate parity90 maps exact.",
      "sources": [
        "S2"
      ]
    },
    {
      "id": "D1",
      "kind": "DERIVED",
      "claim": "Recorded live comparison does not support replacing incumbent.",
      "basis": [
        "F1",
        "F2",
        "F3"
      ],
      "method": "Compare paired saved probabilities on identical map IDs, respecting incomplete input/time equivalence."
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Equal-input prematch comparison and prospective benefit.",
      "scope": "Historical model input snapshots and prestart observations unavailable"
    }
  ],
  "checks": [
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "S2"
      ],
      "observed": "Exact ID/earliest log/target/time boundaries and metrics independently verified."
    }
  ],
  "limitations": [
    "Not equal-input prematch replay",
    "Historical per-row versions absent",
    "Retrospective role/as-of source uncertainty",
    "Small25day cohort;99percent CI crosses0"
  ],
  "contradictions": [
    "Posthoc first5min subset45maps favors candidate, unlike whole sample"
  ],
  "decision_required": [
    "Prospective common-input capture is required before exact prematch superiority can be assessed"
  ]
}
```
