---
id: E-290
title: "Lane dictionary при ML47–53 и ранний выход из wait600 по командному NW"
date: "2026-09-14"
area: dispatch
status: full
corpus: "28 241 pro maps; discovery24 292 до17.07.2026, confirmation3941 17.07–04.09; 8 purged; frozen live nw_mean"
verdict: "Ретроспективно: dict>=20 при ML47–53 123/202=60.89%, но confirmation16/30=53.33%, добавочный logloss CI включает0 — не подтверждено для узкой группы. NW-кандидат: с4мин +1000 на pending-стороне, первое пересечение, confirmation440/470=93.62%, coverage33.26%, средний выход6:18; +500 даёт82.53%, +1500 98.13%. Allowlist+1000 55/58, мало. Никакого внедрения/заявления о прибыльности."
harness: "scripts/ops/research_lane_wait.py; research_lane_dictionary.py; research_lane_residual_check.py; runtime/artifacts/star-dispatch/lane_wait_20260914/direct_local/verification.json"
---

# E-290 — Lane dictionary и ранний выход по командному net worth

## STATUS

DONE

Офлайн-исследование завершено. Гейты, модели и прод не изменялись.

## SUMMARY

**Вывод:** для дальнейшей проверки досрочного выхода разумный простой кандидат —
с 4-й минуты `team_NW_lead >= 1000` **на стороне уже ожидающего win-сигнала**.
Первое пересечение выпускает сигнал; без пересечения сохраняется выход на10:00.
Это исследовательский кандидат, не доказательство будущей точности или доходности.
Порог20 у словаря в узкой группе ML47–53 пока не имеет надёжного подтверждения
на поздней части выборки.

Цель — знак общего `radiantNetworthLeads[10]`, как у E-266 Team Lane ML.
Нулевая разница считается ничьей/ошибкой направленного прогноза. Победа всей
карты — отдельный показатель; нельзя читать точность NW10 как вероятность победы.

### Словарь

На всей узкой группе ML47–53 и `abs(lane_adv_dict)>=20`: **123/202=60.89%**,
Wilson95CI54.02–67.36%. Средний Lane ML для стороны словаря —50.87%.
Dire:82/134; Radiant:41/68. Однако в discovery107/172=62.21%, а в confirmation
только **16/30=53.33%**, CI36.14–69.77%. Положительной полной выборки недостаточно
для снятия гейта на таком условии.

Контроль калибровки: на discovery обучены логистические модели
`intercept + logit(Lane ML)` и та же модель плюс `lane_adv_dict/20`.
Обе проверены на одинаковых confirmation-картах. Вероятность Lane ML приведена
к бинарной условной `pR/(pR+pD)`; реальные точные ничьи удалены из обоих вариантов.
Положительная разность logloss означает пользу словаря:

| Популяция | Confirmation n | Калиброванный ML logloss | ML + словарь | Улучшение | 95% day-block CI улучшения |
|---|---:|---:|---:|---:|---:|
| ML47–53 | 624 | 0.693680 | 0.689658 | 0.004022 | -0.008036…0.016791 |
| Оба Lane ML <60% | 1981 | 0.686540 | 0.680477 | 0.006062 | 0.001051…0.011421 |
| Все карты | 3941 | 0.650264 | 0.641426 | 0.008838 | 0.005039…0.013090 |

В широкой группе без Lane ML hit добавочный сигнал есть в ретроспективном
сравнении. В требуемой узкой группе CI включает ноль. Эта проверка использует
непрерывный словарь, а не доказывает оптимальность порога20. Снимок словаря
не имеет надёжного as-of cutoff; хронологическое обучение калибровки этого
ограничения не устраняет. Bootstrap условен на уже обученных коэффициентах.

### Что означает +500 на конкретной минуте

Карты без hit с обеих сторон Lane ML, confirmation n=1981. Здесь сторона —
та, что лидирует в данную минуту; это описательные срезы, не торговое правило.

| Минута | n при abs(NW)>=500 | Сохранила сторону NW10 | Wilson95CI |
|---|---:|---:|---:|
| 4 | 893 | 83.65% | 81.08–85.93% |
| 5 | 1048 | 85.69% | 83.44–87.68% |
| 6 | 1245 | 87.87% | 85.94–89.57% |
| 7 | 1360 | 88.38% | 86.57–89.98% |
| 8 | 1438 | 91.24% | 89.66–92.59% |
| 9 | 1458 | 95.54% | 94.36–96.49% |

На10:00 знак NW10 уже известен; 100% этого среза — тождество цели, не прогноз.

### Правило целиком: первое пересечение с4-й до9-й минуты

Популяция — реальные ветки обычного `wait_600`, воспроизведённые через
`ml_dispatch.evaluate`; сторона pending-сигнала зафиксирована до наблюдения NW.
Confirmation n=1413. Это не весь прод-отбор: доступность odds/roster/ticks не восстановлена.

| Постоянный порог | Discovery точность | Confirmation hits/n | Точность95CI | Доля раннего выхода | Средняя минута выхода | Победа всей карты |
|---|---:|---:|---:|---:|---:|---:|
| +500 | 84.38% | 562/681 | 82.53% (79.49–85.19%) | 48.20% | 5.49 | 68.87% |
| +750 | 90.18% | 502/566 | 88.69% (85.82–91.04%) | 40.06% | 5.90 | 71.38% |
| +1000 | 94.57% | 440/470 | 93.62% (91.03–95.49%) | 33.26% | 6.29 | 72.34% |
| +1250 | 97.24% | 368/383 | 96.08% (93.64–97.61%) | 27.11% | 6.68 | 72.58% |
| +1500 | 98.60% | 315/321 | 98.13% (95.98–99.14%) | 22.72% | 7.22 | 73.52% |

+1000 экономит в среднем3.71 минуты на сработавший ранний выход, или1.23 минуты
на каждый исходный ожидающий сигнал с учётом оставшихся до10:00. Его day-block
CI точности90.64–96.28%; Dire175/185, Radiant265/285. В сетке это первый постоянный
порог, у которого discovery Wilson lower bound выше90% (+750:89.30%, +1000:93.82%).
Выбор уровня90% — исследовательское допущение, пользователь не задавал допустимую
ошибку. Все пороги сохранены; результаты не являются новым неиспользованным OOS.

Для текущего league allowlist confirmation всего152 pending-карты:
+1000 даёт **55/58=94.83%**, CI85.86–98.23%, ранний выход38.16%, средняя6.66 минуты.
Этого мало для заявления «не менее90% в боевом отборе». +1500:40/41; +500:67/76.

Снижение порога по минутам накопило ошибки: расписание
`4:1000, 5:750, 6:750, 7:500, 8:500, 9:250`, выбранное по90% нижней границе
**отдельных discovery-срезов**, целиком дало только87.98% в discovery и84.70%
в confirmation. Название `discovery_lcb_0.9` относится к срезам и не гарантирует
90% для политики. Поэтому простое постоянное +1000 — более понятный кандидат.

## CHANGED

- `scripts/ops/research_lane_wait.py`: joins, replay dispatch, slices и first crossing.
- `scripts/ops/research_lane_dictionary.py`: exact source AST cascade, SQLite read-only,
  frozen environment, словарь для28241 карт; импорт приложения/ключей не нужен.
- `scripts/ops/research_lane_residual_check.py`: контроль калибровки, более поздняя
  проверка, paired day bootstrap. Явные reductions для матриц из2–3 колонок.
- `base/tests/test_research_lane_wait.py`:7 регрессионных тестов.
- Только исследовательский код, отчёт и индекс; никаких serving-изменений.

## CHECKS

- 28241 уникальных map IDs. Все NW10 и классы совпали с сохранёнными score artifacts;
  невалидных/нулевых-placeholder таймлайнов0.
- Discovery24292:24.03–16.07.2026 (115 дней); confirmation3941:17.07–04.09 (50 дней).
  Разбиение по70% уникальных UTC-дней, не по70% карт.8 карт очищены на границе
  по end time/известной series. Пересечений карт и известных series нет.
- Сетка минут4–10, NW250/500/750/1000/1250/1500/2000/2500/3000; ранний выход4–9.
  Fixed schedules и discovery-selected90/85/80% slices сохранены целиком.
- Первый crossing удерживается даже при последующей смене знака; точные ties — ошибка.
  Независимая векторная перепроверка +1000 дала те же470/440 и6.293617 минуты.
- Словарь использует live `LANE_CELL_VALUE=nw_mean`, scale1000. Scalar — среднее
  валидных lane edges без веса0.75 для mid. Единицы словаря — не gold и не вероятность.
- 8 реальных драфтов дали одинаковые результаты на serv1/serv2 (NumPy2.4.6).
- Завершённые NW/residual процессы имеют exit0. Хеши всех потреблённых frozen inputs
  совпали до/после.7 tests passed; единственный test warning — существующий urllib3/LibreSSL.
- NumPy2.0.2 macOS BLAS выдал floating-status warnings на конечных матрицах.
  Повтор с явными arithmetic reductions дал пустой stderr и те же результаты:
  max разница коэффициентов4.45e-16, loss1.2e-16. Исходный run/log сохранён.

## POINTERS

Полный artifact root: `runtime/artifacts/star-dispatch/lane_wait_20260914/`.
Авторитетные файлы:

- `.orchestra/campaigns/run-e96058ca734b7d16df93bf42/artifacts/dictionary-residual/dictionary.{json,npz}`
- `direct_local/nw_complete/{summary.json,paired.npz}`
- `direct_local/residual_arithmetic/residual_check.json`
- `direct_local/verification.json`, `input_manifest.json`, `residual_arithmetic_inputs.json`
- `prod_lane_environment.json`, `campaign_remote.json`, `extraction.json`.

Запуски: serv1 dictionary-residual completed; serv2 NW queue timeout без вычисления.
Локальная executor-попытка тоже завершилась до worker: macOS отказал в rename после
chmod0500. Пользователь явно разрешил прямой локальный запуск с nice19, хешами
и логами (`direct_local/authorization.json`). Исходный resources.json восстановлен.
Прямые shell-попытки завершались вместе с группой терминала; их частичные файлы
сохранены, а завершённые runs использовали nohup в отдельной session с exit receipt.

Для воспроизведения на подготовленной копии входов (пути frozen source/outputs
должны соответствовать manifest; запуск напрямую требует того же исключения):

```sh
venv_catboost/bin/python3 -m pytest base/tests/test_research_lane_wait.py -q
# Each worker: one process, OMP/OPENBLAS/VECLIB_MAXIMUM_THREADS=1, nice -n19.
venv_catboost/bin/python3 scripts/ops/research_lane_wait.py --output-dir OUTPUT_NW
venv_catboost/bin/python3 scripts/ops/research_lane_dictionary.py --output-dir OUTPUT_DICT
venv_catboost/bin/python3 scripts/ops/research_lane_residual_check.py --paired OUTPUT_NW/paired.npz --dictionary OUTPUT_DICT/dictionary.npz --output-dir OUTPUT_RESIDUAL
```

Модели не переобучались. Compact rich input — exact ID join из rich pro corpus
на сохранённые Lane ML IDs; extraction.json хранит source hash и поля. Final remote
manifest включает revised workers/environment; первоначальный transport tar и
старые label-mode fixtures не являются авторитетным снимком исполнения.

## RISKS

Где искать ошибку:

1. Перепутать team NW10 с per-lane target или победой карты.
2. Взять label-mode словарь вместо live nw_mean, поменять cascade/агрегацию/topology.
3. Перепутать лидера текущей минуты с фиксированной pending-стороной либо включить
   ветки `wait_1860`; здесь изучен только обычный600-секундный гейт.
4. Выдать удачные срезы минут за точность first-crossing политики; забыть повторные
   проверки, ошибки ранних выходов, несработавшие карты в coverage.
5. Назвать retrospective split независимым prospective: current dictionary не
   имеет проверенного as-of cutoff, а сохранённые ML-модели обучены позже истории.
   Нельзя исключить временную утечку/повторное исследование этого корпуса.
6. Принять условный CI фиксированных коэффициентов за неопределённость всего
   обучения. Day blocks не моделируют отдельно series, растянутые на несколько дней.
7. Считать offline evaluate реальной доставкой: нет historical odds/roster/ticks.
   Исходная минутная индексация совпала с saved target, но независимая проверка
   часов источника/тайминга live-снимков не выполнена.
8. Перенести полную выборку на35 production leagues без учёта широкого CI58 случаев.

## NEXT

Задача исследования выполнена. Для внедрения нужен отдельно утверждённый выбор
допустимой ошибки и проверка live shadow на корректном игровом времени и стороне
pending-сигнала. Узкий `ML47–53 + dict>=20` пока не использовать как доказанный
самостоятельный повод отменить ожидание. Никаких production действий в этом ходе.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "User requests research only; 500 means total team net worth.",
    "Direct local execution explicitly approved after macOS executor failure."
  ],
  "sources": [
    {
      "id": "NW",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/direct_local/nw_complete/summary.json",
      "sha256": "68faf281d22cf10cacd98d8723086507473d24945c99bc400f90268fea94b830"
    },
    {
      "id": "DICT",
      "path": ".orchestra/campaigns/run-e96058ca734b7d16df93bf42/artifacts/dictionary-residual/dictionary.json",
      "sha256": "f8410c5c8c0cbe8094ba2600ce74440b81309cb14302cfb7ba9daa10ac40901f"
    },
    {
      "id": "RES",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/direct_local/residual_arithmetic/residual_check.json",
      "sha256": "6e35604bc9f22e5fb169fc7f939e3f4b8f0b03b3780883f6279dd8fb139602d3"
    },
    {
      "id": "VERIFY",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/direct_local/verification.json",
      "sha256": "df17d9e13be79b9f7c65ba1653d1a5215c4ff92bda41f38771594632e391a2bc"
    }
  ],
  "claims": [
    {
      "id": "F1",
      "kind": "OBSERVED",
      "claim": "Confirmation pending wait600 constant1000 first crossing:440/470, coverage0.3326256, mean minute6.293617.",
      "sources": [
        "NW"
      ],
      "scope": "Frozen retrospective dispatch-rule replay."
    },
    {
      "id": "F2",
      "kind": "OBSERVED",
      "claim": "Neutral dictionary>=20 full123/202, confirmation16/30; neutral calibrated residual improvementCI includes0.",
      "sources": [
        "DICT",
        "RES"
      ],
      "scope": "Current dictionary snapshot; no as-of provenance."
    },
    {
      "id": "F3",
      "kind": "INFERRED",
      "claim": "Constant1000 from minute4 is a reasonable candidate for a later shadow evaluation.",
      "basis": [
        "F1"
      ],
      "uncertainty": "Production allowlist55/58 has wide CI; retrospective features and execution limits."
    },
    {
      "id": "U1",
      "kind": "NOT_CHECKED",
      "claim": "Prospective accuracy, live delivery, betting returns and independent source-clock alignment.",
      "scope": "Production deployment/benefits."
    }
  ],
  "checks": [
    {
      "id": "T1",
      "status": "PASS",
      "sources": [
        "VERIFY"
      ],
      "observed": "Exit0 workers; unchanged frozen hashes; unique maps; disjoint partitions and known series; independent470/440 check;7 tests passed."
    }
  ],
  "limitations": [
    "Current dictionary/model snapshots are retrospective.",
    "Confirmation-only day bootstrap is conditional on fitted coefficients.",
    "Production allowlist confirmation for1000 has58 triggers."
  ],
  "contradictions": [
    "Dictionary full neutral result60.89% weakens to53.33% on30 confirmation maps.",
    "Per-minute90% threshold selection yields only84.70% first-crossing confirmation."
  ],
  "decision_required": []
}
```
