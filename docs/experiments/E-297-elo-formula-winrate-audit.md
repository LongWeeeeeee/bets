---
id: E-297
title: Формула ELO и винрейт фаворита
area: elo
status: partial
date: 2026-09-19
corpus: E-263 63778 карт; новый replay не запущен
harness: runtime/experiments/elo/elo_formula_winrate_audit.py; orchestra resources smoke/full manifests в runtime/artifacts/elo/formula_winrate_20260919
verdict: Арифметический дефект не найден; K24 60.72% против гибрида 60.66%; свежий тест блокирован исполнителем
---

# E-297 — формула ELO и винрейт фаворита

## SUMMARY

Запрос 19.09.2026: проверить формулу ELO, используя винрейт как основной
критерий, после кейса Nemesis–1w / 9005874165. Это исследование рейтинга,
а не выбор ставки и не изменение ML-весов.

В проверенных исходниках нет ошибки знака или лишнего множителя пять.
Сохранённый причинный тест E-263 не доказывает превосходство текущего
гибрида над простым K24. Новый тест пяти заранее зафиксированных вариантов
подготовлен, но **результатов у него пока нет**: smoke остановился до запуска
worker на ошибке построения изолированной копии входов.

Источники: `ELO/config.py:45`, `ELO/models.py:1055`,
`runtime/artifacts/elo/endtime_replay_20260905/report.json`,
`runtime/artifacts/elo/formula_winrate_20260919/protocol.json`.

## OBSERVED

### Формула и обновление

Текущий prematch-контракт всегда читает TIER3:

```
S = 0.68 * mean(player_global) + 0.32 * mean(player_local[TIER3])
P(Radiant) = 1 / (1 + 10 ** ((S_Dire - S_Radiant) / 400))
error = actual_radiant_win - expected_radiant_win
R_global,player += side_sign * K_global,tier * uncertainty_global * error
R_local,player  += side_sign * K_local,tier * uncertainty_local * error
```

Обновляющая модель вычисляет ожидание в **фактическом tier матча**, с ролями
для TIER1 и learned side bias. Формула отображаемой вероятности выше исключает
side bias. Таким образом, рецепт обновления и фиксированный TIER3 readout
не идентичны. Их соответствие качеству требует эмпирического сравнения.

Глобальный K для T1/T2/T3: 28.8/12.0/7.2; локальный: 18.4/25.3/20.7.
Локальный K делится между tier- и role-рейтингом без дублирования суммы.
Одинаковое обновление всех пяти игроков меняет среднее на одну величину
`K * error`. Дополнительного деления на пять не требуется.

Множители неопределённости могут отличаться между игроками/сторонами, поэтому
сумма рейтингов не обязана сохраняться. Для нового состава множитель до 2
на первых четырёх наблюдениях; для нового player-org stint — до 2 на первых
15. Они могут перемножаться. Это запрограммированная модель, не обнаруженная
арифметическая ошибка. Роль/roster в фиксированном TIER3 readout не участвуют.

Decay выключен, inactivity keep=1. Patch reset сбрасывает TIER1 local/role,
но не глобальный рейтинг и не TIER3. Sweep bonus 0.05 есть в series evaluator,
однако штатный snapshot builder и benchmark его не применяют.

### Кейс Nemesis–1w

Точный live snapshot + 824 delta updates воспроизводят показанные числа:

| Компонент | Nemesis | 1w | Вклад в разницу Nemesis−1w |
|---|---:|---:|---:|
| Global | 1817.243 | 1980.344 | −110.909 после веса 0.68 |
| Local TIER3 | 3252.309 | 2833.097 | +134.148 после веса 0.32 |
| Итог | 2276.464 | 2253.225 | +23.239 |

Следовательно, глобальный рейтинг предпочитает 1w, а старый TIER3-компонент
переворачивает фаворита. В этом примере речь о вероятности Nemesis 53.34%,
а не о сильном фаворите. Даты последних обновлений отдельных TIER3 рейтингов
отстают на 50–80 дней; это не доказательство отсутствия реальных игр.

ML получила ту же разницу `hybrid_strength=+0.058096988`. Её отрицательный
агрегат «ELO −63%» преимущественно пришёл от отдельного `opp_elo`, а не от
этого гибридного рейтинга. Точное разложение и ограничения доставки:
`runtime/artifacts/elo/nemesis_1w_20260919/followup_report.md` и `report.md`.

### Винрейт в сохранённом причинном replay E-263

Числа заново прочитаны из локального `report.json`, не пересказаны по памяти.
ELO обновлялся после `start + duration`, до следующего доступного прогноза.

| Период UTC | Карт | Гибрид TIER3 | K24 | Разница K24, п.п. |
|---|---:|---:|---:|---:|
| Январь–февраль | 13472 | 60.273% | 60.719% | +0.445 |
| Март–май | 29645 | 60.466% | 60.310% | −0.155 |
| 1 июня–11 августа | 19885 | 61.021% | 61.056% | +0.035 |
| 12 августа–3 сентября | 776 | 65.722% | 67.655% | +1.933 |
| Все окна | 63778 | 60.662% | 60.718% | +0.056 |

Это accuracy с правилом `p >= 0.5` (равенство выбирает Radiant), а не WR
после исключения ничейных рейтингов. Общая разница соответствует 36 картам,
последнего окна — 15. В E-263 нет paired confidence interval для WR;
интервал log loss нельзя выдавать за интервал винрейта. По периодам знак
разницы меняется. Совокупный log loss: 0.654665 у гибрида, 0.653080 у K24.

### Новый фиксированный протокол

Пять методов: текущий TIER3 readout, только global, readout фактического
tier, диагностическое затухание TIER3 к 1500 с half-life 90 дней, независимый
K24. Первые четыре используют **одно** эволюционирующее гибридное состояние:
это ablations чтения, а не обучение четырёх независимых моделей.

Полный исторический warmup, оценка карт 2026 года; одинаковые карты с
ненулевой разницей у всех пяти методов. Периоды, разрезы tier, расхождение
global/TIER3, TIER3 старше 30 дней. Парный bootstrap по сериям (fallback:
пара team IDs/день), seed 297, 1000 повторов. Дополнительно одинаковый объём
верхних 50% по уверенности и пороги gap 50/100/200 с явным покрытием.
Log loss/Brier вторичны; прибыльность без кэфов не оценивается.

## DERIVED/INFERRED

1. Формула может быть математически корректной и хуже выбирать фаворитов.
   Именно эту разницу надо проверять по WR; кейс одной команды не выбирает
   оптимальные веса и не доказывает, что 1w обязана иметь больший итоговый ELO.
2. Подозрительны фиксированный TIER3 для всех матчей, отсутствие его затухания
   и повторные uncertainty boosts при смене org identity. Последнее пока
   является риском, а не доказанным дефектом конкретного аккаунта.
3. Сохранённые результаты дают основание сравнивать с K24, но не менять
   действующую формулу автоматически: общий прирост WR всего 0.056 п.п.,
   а свежего независимого подтверждения и paired WR CI пока нет.

## NOT_CHECKED

- Новый полный replay, свежий период 5–19 сентября, WR actual-tier/global/decay.
- Полная временная достоверность статических priors team tier/alias.
- Полнота внешней истории матчей и причинная история identity десяти аккаунтов.
- Независимо переобученные варианты с другими весами, K и decay.
- Production selection, ставки, коэффициенты и доходность.

## CHANGED

Только offline harness, артефакты исследования и эта запись/индекс.
Serving-код, snapshots, веса ML, resource policy и production не менялись.

## CHECKS

Харнесс: `runtime/experiments/elo/elo_formula_winrate_audit.py`.
Протокол/хеши: `runtime/artifacts/elo/formula_winrate_20260919/`.

```bash
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/elo/formula_winrate_20260919/smoke_campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/formula_winrate_20260919/smoke_campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources status --run run-518c1e40f9172aa8bfcc1564
# Только после исправления блокера и успешного smoke:
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/elo/formula_winrate_20260919/full_campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/formula_winrate_20260919/full_campaign.json --background
```

Синтаксис harness проверен. Smoke preflight сначала корректно отказал из-за
нагрузки; затем прошёл на одном CPU без изменения resource policy.
`run-518c1e40f9172aa8bfcc1564` — FAILED, worker не стартовал, артефактов
метрик нет. Точная причина воспроизведена отдельной bounded диагностикой
только этапа копирования: `remote_job.py` делает `chmod(stage, 0500)` перед
`os.replace(stage, target)`; macOS возвращает EACCES при таком rename.
Traceback сохранён в `snapshot_error.txt`. Это ошибка исследовательской
инфраструктуры, не ELO. Запрошено отдельное согласование её локальной правки.
Другой launcher не использовался, receipts не изменялись.

**Где искать ошибку:** смешение start/end времени; неизвестные durations;
конфликтующие дубликаты; несовпадение состава/позиции/tier; актуальные вместо
исторических aliases; tier-local cold starts и ничьи; разные знаменатели;
зависимость карт одной серии; применение decay только при чтении; выбор
победителя после просмотра множества диагностических разрезов. Новый harness
пока только компилировался, его эмпирическая валидность не установлена.

## POINTERS

- `ELO/models.py:1037-1119,1309-1418`: сила состава и обновления.
- `ELO/config.py:43-105`: веса, K, boosts, decay/reset.
- `ELO/tiering.py:166-218`: as-of присвоение tier.
- `ELO/live_team_strength.py:2442`: serving result-time replay без sweep bonus.
- `ELO/benchmark_replay.py:58-112`: E-263 replay.
- `ELO/benchmark_probabilities.py:65-70`: старое определение accuracy.
- `.orchestra/campaigns/run-518c1e40f9172aa8bfcc1564/state.json`: терминальный отказ.

```orchestra-evidence-v1
{"schema":"orchestra-evidence-v1","status":"PARTIAL","scope":"offline ELO formula and favorite WR audit","observed":["No arithmetic sign/averaging defect found in inspected source","Saved causal E263 accuracy: hybrid 0.6066198376, K24 0.6071842955, n=63778","New smoke failed before worker launch on snapshot directory rename EACCES"],"inferred":["Existing WR evidence does not justify automatic formula replacement"],"not_checked":["Fresh five-method replay","Paired WR intervals","Actual-tier/global/decay WR"],"next":"Resolve separately scoped executor failure; validate smoke and complete frozen replay"}
```
