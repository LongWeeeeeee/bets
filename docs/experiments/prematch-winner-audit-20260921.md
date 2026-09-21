# Проверка winner ML: E-308 / E-309, 21.09.2026

## STATUS

DONE — проверка сохранённых расчётов и исправление трёх проверок входов.
Цель «сильно лучше Elo» остаётся недостигнутой. Обучение, подбор параметров,
повторный выбор по terminal и изменения production в этом аудите не выполнялись.

## SUMMARY

**OBSERVED: ошибок в арифметике опубликованных результатов не найдено.**
Прямой расчёт по сохранённым вероятностям, без вызова `metrics`/`paired_report`,
воспроизвёл число верных прогнозов, log loss, Brier и таблицы расхождений.
Все 1 261 465 строк совпали с rich source по MID, победителю, start/end, league
и series; составы и стороны дополнительно проверены прежними overlap-проверками.

| Проверка | Результат |
|---|---|
| E-308 terminal, 2 613 карт: ML | 1 676 верных, **64.1408%**; LL 0.6220887562 |
| Те же карты: raw K24 | 1 666, **63.7581%**; LL 0.6336436061 |
| Те же карты: calibrated K24 | 1 675, **64.1026%**; LL 0.6331598983 |
| ML / calibrated K24 в расхождениях | **284 / 283** на 567 картах |
| E-309 all, 9 018 карт | 5 812 вместо прежних 5 781: **+31**, +0.3438 п.п. |
| E-309 recent, те же карты | 5 734 вместо прежних 5 772: **−38**, −0.4214 п.п. |

E-309 использует уже изученные June/early-July folds; это exploratory ablation,
а не новый независимый тест. Проверка арифметики не делает этот период holdout.
Старые доверительные интервалы не пересчитывались новым bootstrap: проверены
исходы и формула группировки по series. На terminal нет точных ничьих Elo;
на selection их 17. Их исключение не меняет отрицательный вывод о сильном lift.

**OBSERVED: три ошибки защиты входов воспроизведены и исправлены.**

1. `wins=NaN`, infinity или небинарное число могли стать поражением Radiant
   через `wins > 0.5`. Теперь builder отклоняет невалидную метку до replay.
2. Один MID мог иметь разные времена окончания в rich и Elo stream. Тогда
   K24 мог увидеть результат раньше остальных признаков. Добавлено строгое
   равенство end-time для пересекающихся MID.
3. Дубли старой карты вне query cohort могли иметь разные winner/end/identity,
   а canonical запись выбиралась по порядку файла. Теперь материальный конфликт
   отклоняется независимо от попадания карты в query. Перестановка player slots
   разрешена; XP-only различия сохраняют прежнюю deterministic policy.

**OBSERVED: текущие результаты этими дефектами не затронуты.** На полном corpus
небинарных rich labels — 0; среди 998 541 общих rich/Elo MID несовпадений start
или end — 0. Все 131 конфликтующих дубликата различаются только XP zero/positive,
а не исходом, временем или account/hero/side. Ни на selection, ни на calibration,
ни на terminal нет `series<=0`, поэтому замечание о неизвестных series ID
к этим выборкам не относится. Пересборка и повторное обучение из-за этих guards
не нужны: допустимые входы и расчёт признаков остались прежними.

**OBSERVED: проверка истории и Elo прошла независимо от incremental builder.**
На фиксированной выборке 75 карт / 750 player slots, включая все три карты
Daxak–Stariy_Bog, direct filtering прошлых событий воспроизвёл все **9 900**
новых feature values с максимальной ошибкой **0**. Проверены games/WR,
сглаживание относительно general WR, decay, дни с последней игры и prior XP.
Это выборочная проверка признаков, не полный независимый replay всей матрицы.

Отдельная реализация K24 на Python dictionaries проиграла **1 396 157** событий
и проверила все **11 631** оценённых карт. Максимальная ошибка сохранённого
`k24_diff` — **0.00002995 Elo**, ожидаемое float32 округление. Формула совпадает
с production: K=24, initial=1500, среднее пяти игроков, scale=400.
Это проверка сохранённого event stream; тождественность его состава конкретному
production state этим не доказывается.

Третья карта `9006963816`: replay diff **−29.02036** Radiant−Dire, P(Radiant)
**45.8333%**; сохранённая terminal ML давала Radiant **52.2506%**. Здесь ML
действительно ошиблась. Этот replay не заменяет реконструкцию состояния,
которое в момент production-прогноза показывало пользователю другую ELO-разницу.

**OBSERVED: не все имеющиеся данные использованы.**

| Данные | Что установлено |
|---|---|
| OpenDota histories 5/10 min | 165 490 player rows, 16 549 maps, 17 953 accounts; в этих winner inputs отсутствуют |
| Доступная прошлая история целевых игроков в OpenDota | 24 595 player rows завершились до June 1; 115 000 до July 15; это строки истории, не новые независимые карты |
| Rich postgame player fields | Использованы исторические GPM, IMP, KDA; E-309 добавил prior XP. XPM, LH/denies, damage/healing, networth и level не имеют отдельных исторических признаков в этих матрицах |
| Возраст | В feature schema отсутствует; подтверждённые DOB в данном исследовании не собраны |
| Public draft | Входы реально присутствуют в сохранённых моделях (86 final features у E-308), но доступны только для **27 479 / 1 261 465** строк, в том числе 13 692 до June 1 |
| Public source filtering | Унаследован corpus для другого исследования: duration >= 1 200 s и все позиции; manifest сообщает 53 828 rejects по duration и 9 717 по position |

53 828 duration rejects нельзя целиком назвать быстрыми играми: upstream reason
объединяет слишком короткие, отсутствующие и некорректные duration. Доли причин
в полном raw public archive в этом аудите не пересчитывались.

Вся OpenDota таблица уже входит по MID в rich corpus; новый потенциальный сигнал
здесь — отдельные исторические показатели линии, а не увеличение числа label rows.
`gold_t` — накопленное золото, не net worth. В prematch допустимы только показатели
предыдущих завершённых карт. Свои gold/xp/LH на 10-й минуте дали бы утечку.

Public examples проверены отдельно: shape (6 638 652, 10), бинарные outcomes,
уникальные MID; **225** пересечений с pro исключены. Для двух draft-моделей
проверены source hash, hashes 14 frozen snapshots, точное число admitted rows и
max end каждого cutoff. Повторная проекция 51 карты на модель совпала побитово.
На 96 raw records из восьми файлов проверены side/position/hero/label/time.
Обратного знака или перепутанного порядка сторон в этой выборке нет.

Обучение all включало 285 025 rows без league ID; после веса 0.25 и временного
затухания их суммарный вес — **0.69%**. Recent-рецепт не включал таких строк.
Поэтому большое абсолютное число non-league maps само по себе не объясняет
провал. Все evaluation rows имеют league>0 и Elo eligibility.

**DERIVED:** прежние слабые результаты реальны для проверенных моделей и
выборок. Из них нельзя заключать, что player data бесполезны или что все
доступные источники уже исчерпаны. Добавление пропущенных источников остаётся
проверяемой гипотезой; выигрыш от них пока не измерен.

## CHANGED

Code commit `ee653a58`: [causal builder](../../base/tools/prematch_causal_dataset.py),
[history builder](../../base/tools/prematch_pro_player_history.py) и их регрессии.
Нет изменений модели, весов, thresholds, production или resource policy.
Контракты описаны в [CODE_MAP](../CODE_MAP.md); предшествующий эксперимент —
[E-309](E-309-pro-player-history-recovery.md).

## CHECKS

- До исправления: **11 failed / 21 passed** в двух целевых suites; все 11 failures
  соответствуют трем выявленным дефектам.
- После исправления и добавления проверки перестановки players: **42 passed**
  в causal-dataset / pro-history / winner-research suites.
- Независимый reviewer просмотрел исходный код и полный исправляющий diff;
  оставшихся actionable замечаний к этому diff не выдал. Повторное исполнение
  тестов reviewer не выполнял.
- Прямые arithmetic/cohort join, history, K24 и public artifact checks прошли.

## POINTERS

Артефакты: `runtime/artifacts/elo/general_ml_audit_20260921/`.
Харнессы: `runtime/experiments/elo/general_ml_audit_20260921/`.
Команды из корня, только `venv_catboost/bin/python3`:

```sh
PYTHONPATH=. OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 venv_catboost/bin/python3 runtime/experiments/elo/general_ml_audit_20260921/audit_sources.py
PYTHONPATH=. OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 venv_catboost/bin/python3 runtime/experiments/elo/general_ml_audit_20260921/audit_saved.py
PYTHONPATH=. OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 venv_catboost/bin/python3 runtime/experiments/elo/general_ml_audit_20260921/audit_history.py
PYTHONPATH=. OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 venv_catboost/bin/python3 runtime/experiments/elo/general_ml_audit_20260921/audit_public.py
PYTHONPATH=. OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 venv_catboost/bin/python3 runtime/experiments/elo/general_ml_audit_20260921/audit_k24.py
venv_catboost/bin/python3 -m pytest base/tests/test_prematch_causal_dataset.py base/tests/test_prematch_pro_player_history.py base/tests/test_prematch_winner_research.py -q
```

Долгий K24 audit выполнен через nohup, PID 20883, exit 0, лог
`k24_direct_replay.v2.log`. Первый launcher PID 20181 завершился без результата
и без живого процесса; его пустой лог сохранён. Успех относится ко второму запуску.
Других обучений/длительных jobs здесь не запускалось.

Где искать ошибку: direct audit должен совпадать с сохранёнными MID/y и описанным
cohort; изменение universe или cutoff требует отдельного отчёта. Проверять raw
side semantics, completion/publication time, first-prefix vocab, artifact schema
и различие production ELO от causal offline reconstruction. Hash подтверждает
тождественность файла, но не истинность исходных сведений.

## RISKS

NOT_CHECKED: полная повторная распаковка 11 GB raw pro JSON и всего raw public
архива; историческая дата публикации каждого XP/stats; истинные DOB; реальное
покрытие всех аккаунтов за пределами имеющегося архива; fresh production replay.
End-time causal order проверен, но он не доказывает, что backfilled API field
было опубликовано в тот же момент. Полная независимая пересборка всех base71
features и повторное обучение 19 кандидатов не выполнялись.

Public training CLI доверяет side/role convention canonical input; произвольный
сторонний NPZ без подтверждённого происхождения не становится безопасным от
проверки формы. Нулевые series IDs в будущих данных потребуют отдельной политики;
в текущих evaluation cohorts их нет. Сильного превосходства или profitability
эти проверки не установили.

## NEXT

Следующий исследовательский шаг — отдельная причинная ablation исторических
lane/player-performance данных с заранее зафиксированным сравнением и новым
неоткрытым периодом для окончательной оценки. Повторно оптимизировать по уже
открытому August–September и называть его независимым доказательством нельзя.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "User requests careful check of calculations and omitted data",
    "No production change or new training",
    "Preserve opened terminal boundary"
  ],
  "sources": [
    {
      "id": "AR",
      "path": "runtime/artifacts/elo/general_ml_audit_20260921/arithmetic_and_cohorts.json",
      "sha256": "e1493fb8341190a8a98fe7a1d1b3771b6e761abf58599fc2375493b6eed34881"
    },
    {
      "id": "HI",
      "path": "runtime/artifacts/elo/general_ml_audit_20260921/history_direct_recalculation.json",
      "sha256": "2dfd8f6571c45b8c1fd9d8609366bb7b715be99e38f41a9af3f51f225601f261"
    },
    {
      "id": "EL",
      "path": "runtime/artifacts/elo/general_ml_audit_20260921/k24_direct_replay.json",
      "sha256": "e4f616a76dc4575b7888a8c25daa7f603c08fb5937b60eff58cf74522a73fe68"
    },
    {
      "id": "PO",
      "path": "runtime/artifacts/elo/general_ml_audit_20260921/reviewer_population_checks.json",
      "sha256": "3e1cb523f137b604ae48bb49f645e6985793de0f6a9f5edf724b3716efb4f397"
    },
    {
      "id": "PU",
      "path": "runtime/artifacts/elo/general_ml_audit_20260921/public_provenance.json",
      "sha256": "da0b52bf95294ab469e846003c314fd26db916834e88a49a1739a8652a7c1762"
    },
    {
      "id": "OD",
      "path": "runtime/artifacts/elo/general_ml_audit_20260921/unused_timeline.json",
      "sha256": "c68b26c81ebb069919ae6b79801d240f61666f15d6c82c2a0276b893eb9c2c7e"
    },
    {
      "id": "TE",
      "path": "runtime/artifacts/elo/general_ml_audit_20260921/regression_final.txt",
      "sha256": "0bde4883c3254c6389f3735c5666481b9f8ec298fc6147f00b02ba24f2b2dbaf"
    },
    {
      "id": "CA",
      "path": "base/tools/prematch_causal_dataset.py",
      "sha256": "6bebdff2fb37ce43df91b3438b22724ccaab7125821b5eb3022ea830b3b821ca"
    },
    {
      "id": "PR",
      "path": "base/tools/prematch_pro_player_history.py",
      "sha256": "3d70c31d00cce95aeb339caa953b04fd9c6cb0c52c29d22e5f1defe501f6fd78"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "Independent prediction arithmetic reproduces terminal ML 1676 vs calibrated K24 1675 and E309 incremental 31/-38 correct maps.",
      "sources": [
        "AR"
      ]
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Direct history audit matches 9900 sampled values exactly; direct K24 replay matches 11631 evaluated maps within 0.00003 Elo.",
      "sources": [
        "HI",
        "EL"
      ]
    },
    {
      "id": "F3",
      "kind": "OBSERVED",
      "claim": "Current artifact invalid winner / shared end / material duplicate and unknown-evaluation-series counts are zero.",
      "sources": [
        "PO",
        "HI"
      ]
    },
    {
      "id": "F4",
      "kind": "OBSERVED",
      "claim": "Historical OpenDota 165490 player rows not present in winner feature schema; public projections available for 27479 rows and source requires 1200 seconds.",
      "sources": [
        "OD",
        "PU",
        "CA",
        "PR"
      ]
    },
    {
      "id": "F5",
      "kind": "OBSERVED",
      "claim": "Three malformed-input guards added; 42 focused regressions pass.",
      "sources": [
        "CA",
        "PR",
        "TE"
      ]
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Full raw reparse, original publication-time of historical stats, independent new holdout, and exact production state identity.",
      "scope": "Beyond saved artifact audit"
    }
  ],
  "checks": [
    {
      "id": "A1",
      "status": "PASS",
      "sources": [
        "AR",
        "HI",
        "EL",
        "PO",
        "PU"
      ],
      "observed": "All audit assertions passed; explicitly bounded full row metadata joins, full evaluation K24 replay and sampled feature checks."
    },
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "TE"
      ],
      "observed": "42 passed in 0.99 seconds"
    }
  ],
  "limitations": [
    "No claim all available data used",
    "Source publication time unverified",
    "Historical negative quality result unchanged"
  ],
  "contradictions": [],
  "decision_required": []
}
```
