---
id: E-290
title: "Lane dictionary при ML47–53 и ранний выход из wait600 по командному NW"
date: "2026-09-14"
area: dispatch
status: full
corpus: "28 241 pro maps; discovery24 292 до17.07.2026, confirmation3941 17.07–04.09; 8 purged; frozen live nw_mean"
verdict: "Цель NW10>=1500: поминутные срезы1–10; current>=2000 на6–9 даёт92–96%. Текущее4/+1000:275/470=58.51%; first-cross2000:170/186=91.40%; conditional90:168/183=91.80%. Reused confirmation, clock sensitivity; прод не менялся."
harness: "scripts/ops/research_lane_wait.py; research_lane_dictionary.py; research_lane_residual_check.py; research_lane_minute_schedule.py; runtime/artifacts/star-dispatch/lane_wait_20260914/direct_local/verification.json"
---

# E-290 — Lane dictionary и ранний выход по командному net worth

## STATUS

DONE

Офлайн-расчёты завершены. После них владелец явно поручил внедрить простой
выход с 4:00 при +1000 и перезапустить прод. Изменение932e5ef выложено наserv1 и перезапущено14.09.2026 13:32:34MSK;
приёмка ниже. Модели не менялись.

## SUMMARY

**Последняя цель владельца: командный NW на10:00 >=1500.**
Поминутная таблица1–10 и отдельные first-crossing результаты приведены ниже.
Текущее4:00/+1000 даёт275/470=58.51% по этой цели. Прод не менялся.
Предыдущие расчёты для>=1000 и>0 сохранены с явными маркерами.

**Исходный вывод для NW10>0:** для дальнейшей проверки досрочного выхода разумный простой кандидат —
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
90% для политики. Поэтому на этом этапе простое постоянное +1000 оставлено основным кандидатом.

### Дополнение: с какой минуты начинать и отдельные пороги1–10

**OBSERVED.** На той же замороженной выборке построены 20 вариантов для каждой
из двух трактовок часов: старты1/2/3/4, discovery Wilson LCB90/95%, рост NW
от минуты1 `None/0/250` (growth только для стартов3/4), постоянный+1000.
Каждый следующий порог подбирается **только на ещё не сработавших discovery-картах**.
Сетка100..3000 шаг100, минимум100 срабатываний на выбранной минуте.
Нижняя граница Wilson используется для отбора; это не одновременная гарантия
по всем просмотренным порогам. Confirmation уже использовалась ранее и не
считается свежей OOS. Пропуск минуты означает отсутствие подходящего порога
в этой сетке, а не доказательство невозможности раннего сигнала.

При основном соответствии `raw index m → минута m`, LCB90 без growth:

| Минута | Порог на pending-стороне | Confirmation первой отправки, hits/n |
|---|---:|---:|
| 1 | ждать | — |
| 2 | ждать | — |
| 3 | +1200 | 20/22 |
| 4 | +1000 | 59/64 |
| 5 | +1000 | 77/84 |
| 6 | +900 | 106/114 |
| 7 | +1100 | 55/57 |
| 8 | +700 | 102/117 |
| 9 | +700 | 70/76 |
| 10 | обычное снятие ожидания по времени | не ранний прогноз |

Подъём с900 на1100 на7-й минуте допустим: уже отправленные карты исключены,
оставшаяся популяция другая. Не нужно искусственно делать пороги монотонными.

| Политика | Hits/n | Точность (Wilson95CI) | Ранний выход | Средняя минута | Экономия минут на исходный сигнал |
|---|---:|---:|---:|---:|---:|
| +1000 с1 | 442/476 | 92.86% (90.18–94.84) | 33.69% | 6.07 | 1.323 |
| +1000 с3 | 442/475 | 93.05% (90.40–95.01) | 33.62% | 6.13 | 1.299 |
| **+1000 с4, выбран владельцем** | **440/470** | **93.62% (91.03–95.49)** | **33.26%** | **6.29** | **1.233** |
| Поминутный LCB90 с3 | 489/534 | 91.57% (88.91–93.64) | 37.79% | 6.45 | 1.341 |
| Поминутный LCB90 с4 | 487/532 | 91.54% (88.87–93.62) | 37.65% | 6.51 | 1.314 |
| Поминутный LCB95 с3/4 | 422/447 | 94.41% (91.87–96.18) | 31.63% | 7.13 | 0.907 |

LCB95 выбрал только `5:1600,6:1000,8:1100,9:900`, остальные минуты пропущены.
У поминутного LCB90 day-block95CI88.25–94.53%; allowlist58/62 (малый n).
Сравнение политик — компромисс точности/охвата на разных выбранных картах,
не парный тест превосходства. Добавление старта1 к постоянному правилу с4
даёт всего6 новых карт:2 попадания,4 ошибки; всего до4:00 пересекли+1000
49 карт (42 успешных). Это описательное сравнение на reused confirmation.

**Гипотеза про драку до крипов.** Growth>=0 или>=250 не изменил confirmation
срабатывания90% расписания: те же489/534 для старта3 и487/532 для старта4.
Для95% сgrowth250 порог на8 снизился до1000:432/457=94.53%, охват32.34%.
Это перестройка расписания, не доказанный добавочный эффект growth.
На3-й минуте при текущем NW800..1299 группа с NW1>=500 дала4/7, без такого
раннего лида55/60; группа слишком мала, чтобы считать это подтверждением
гипотезы. В диапазоне300..799 рост>=250 дал172/234 против32/38 без такого
роста: монотонной пользы роста не видно. NW growth включает убийства, башни
и другие источники; поминутных last hits/золота от крипов нет. Командный NW
не отвечает, кто выиграл конкретный мид или матчап Slark/Ember.

**Часы источника.** Rich builder (`pro_corpus_rich.py:51–62,114–117`) копирует
`radiantNetworthLeads` без вставки нулевой точки, короткий массив forward-fill.
Read-only проверка6 raw матчей из первых2 шардов: длина всегда
`ceil(durationSeconds/60)+1` (1498с→26,2071→36,2434→42,2124→37,3552→61,2305→40).
Это поддерживает baseline/index0 и основной target[10], но не заменяет
timestamp-спецификацию. В `star_dispatch_replay_rows.py:156–168` и
`networth_comeback_research.py:303–308` есть конфликтующее правило N−1.
Отдельная sensitivity смещает и наблюдения, и цель: поминутный старт3 даёт
509/575=88.52%, постоянный+1000 с4 —389/420=92.62%. Числа чувствительны к часам;
в обеих версиях поминутная схема имеет больше охват и меньше точность.
Обе версии используют одну исходную valid-популяцию: все28241 valid, finite0..10.
Формальная привязка raw к часам и отсутствие forward-fill в ранних точках
по всему корпусу не доказаны. Прод использует live `game_time` в секундах,
а не индексы исторического массива.

**DERIVED/INFERRED.** Старт4 — простой практический выбор владельца;
поминутная схема пока не доказала улучшение и не внедряется. Не делать вывод
«героям хватает ровно трёх пачек» или «рост NW — это преимущество в фарме».

### Внедрение постоянного4:00/+1000, разрешено владельцем

Только обычная ветка win/wait600: `240 <= game_time < 600`, signed team NW
в пользу целевой стороны >=1000. Lane ML hit по-прежнему может снять ожидание
раньше; 600с остаётся fallback. Missing/nonfinite NW держит ожидание. Late/All
conflict1860, kills, veto, odds и персистентный dedup сохраняются.
Настройки `ML_DISPATCH_EARLY_NW=1`, `..._START_SECONDS=240`, `..._MIN_LEAD=1000`;
rollback: `ML_DISPATCH_EARLY_NW=0` в systemd drop-in и штатный restart script.
В audit добавлены signed NW и `decisions[].reasons` с `early_nw_release`;
startup печатает фактически прочитанные значения. Проверки119 passed, включая
10 research; первоначальный wrapper test поймал отсутствие reasons в audit,
после исправления повторная проверка прошла. Старый failed log сохранён.

Приёмка: serv1 `/root/main` на `932e5ef`; штатный
`scripts/run/restart_cyberscore.sh` очистил map_id_check и перезапустил service
14.09.2026 13:32:34MSK. Один PID676356, active, NRestarts0. Startup:
`early_nw=on/240.0/1000.0`, `mode=ml`; wait600/late1860 сохранены.
Хеши обоих runtime-модулей совпали с локальными. На Python3.12 py_compile +
78 pure-dispatch tests passed; отдельный smoke с actual process env проверил
Dire при240с/-1000 → now. После живых циклов:0Tracebacks,0tick errors;3 новых audit rows
на2 картах содержат time/NW (532с/+3825,357с/+236,608с/+4430).
Ранее отправленные сигналы остановлены persistent dedup, дублей нет.
У обеих карт был Lane hit; реальная новая отправка по early-NW не наблюдалась. Это техническая приёмка, не доказательство будущей точности.
`minute_schedule/{prod_before.json,prod_acceptance.json,gate_checks.json,
deploy_preflight.log,restart.log,prod_cycle_acceptance.json,prod_audit_details.jsonl}`. Откат: EARLY_NW=0 + restart; чужой dirty
`base/id_to_names.py` сохранён, sent-ledger и log.txt не чистились.

### Дополнение: цель NW10>=1000, включительно

**OBSERVED.** Владелец заменил исследовательский исход: успех —
`radiantNetworthLeads[10] * pending_side >= 1000`. Ровно+1000 — успех;
+999,+1,0 и отрицательный NW — ошибка. Входной порог ранней отправки и итоговый
порог на10:00 независимы. Сторона — та же фиксированная pending-сторона.
Словарная часть выше не пересчитывалась: её исходным вопросом был знак Lane ML
и победитель командного NW; смена маркера здесь относится к раннему NW-выходу.

Пересчитаны прежние правила без смены trigger-масок и заново подобраны
поминутные thresholds на том же discovery. На поздней части1413 pending-карт:

| Политика | Итоговый NW>=1000, hits/n | Доля успеха (Wilson95CI) | Доля ранних выходов | Средняя минута |
|---|---:|---:|---:|---:|
| Действующее+1000 с4 | 347/470 | 73.83% (69.67–77.60) | 33.26% | 6.29 |
| Прежнее переменное расписание с3 | 371/534 | 69.48% (65.44–73.23) | 37.79% | 6.45 |
| +1500 с4 | 284/321 | 88.47% (84.52–91.52) | 22.72% | 7.22 |
| +2000 с4 | 180/186 | 96.77% (93.14–98.51) | 13.16% | 7.56 |
| +2500 с4 | 99/101 | 98.02% (93.07–99.46) | 7.15% | 7.84 |
| +3000 с4 | 53/55 | 96.36% (87.68–99.00) | 3.89% | 8.20 |
| Новое поминутное LCB90 | 268/296 | 90.54% (86.67–93.37) | 20.95% | 7.47 |
| Новое поминутное LCB95 | 224/236 | 94.92% (91.32–97.07) | 16.70% | 8.04 |

Декомпозиция470 выходов текущего правила:347 достигли>=1000,93 остались
в диапазоне(0,1000),30 закончили с NW<=0. Сумма347+93=440 восстанавливает
предыдущие93.62% по старому маркеру. На текущем allowlist:43/58=74.14%
у действующего правила; новое LCB90 —34/38, LCB95 —28/31, +2000 —20/22.
Малые выборки не доказывают заданную надёжность боевого отбора.

Новая сетка, первая отправка только по ещё не сработавшим картам:

| Минута | LCB90: порог отправки | Confirmation hits/n на этой минуте | LCB95: порог отправки |
|---|---:|---:|---:|
| 1–4 | ждать | — | ждать |
| 5 | +1500 | 39/45 | ждать |
| 6 | +1600 | 32/36 | ждать |
| 7 | +1600 | 59/63 | +1700 |
| 8 | +1800 | 35/39 | ждать |
| 9 | +1500 | 103/113 | +1700 |
| 10 | исход уже наблюдаем | не ранний прогноз | исход уже наблюдаем |

На10:00 новый исследовательский критерий проверяет факт>=1000. В production
по-прежнему обычный time fallback600; новый критерий не добавлен в live gate.
Все старты1/2/3/4 дали одинаковые обученные расписания; до5-й минуты не нашлось
порога, одновременно удовлетворяющего discovery Wilson lower>=90% и n>=100
в заданной сетке100..3000 шаг100. Это не доказательство отсутствия редких
ранних ситуаций. LCB90 discovery2184/2340=93.33%; LCB95 discovery1840/1897=97.00%.
LCB90 day-block CI86.08–94.44%; LCB95 —91.67–97.65%. Ни один из этих CI не
является гарантией будущей точности/одновременной надёжности всех минут.

Growth0/250 к NW1 не изменил новые расписания и confirmation-результаты.
При альтернативном N−1 clock mapping текущий+1000 с4 даёт305/420=72.62%;
LCB90 выбирает `6:1500,7:1600,8:1600,9:1500` и даёт210/231=90.91%; LCB95
`6:2000,8:1700,9:1900` —142/150=94.67%. Смена clock сдвигает как наблюдения,
так и итоговыйtarget; точные минуты остаются чувствительны к конвенции.

**DERIVED/INFERRED.** Для удержания>=1000 действующий entry+1000 существенно
слабее, чем для простого сохранения знака. Более высокий entry или более
поздний выход дают иной компромисс охвата/точности. Нельзя назвать найденное
расписание победителем на основании сравнения разных n или reused confirmation.
Новые пороги — только результаты исследования, не разрешение на новый деплой.

Харнесс: `scripts/ops/research_lane_minute_schedule.py --target-min-lead 1000`.
Без аргумента сохраняется прежний маркер>0. Запуск полностью сохранён в
`runtime/artifacts/star-dispatch/lane_wait_20260914/target1000/commands.json`;
один nice19 процесс, обе clock версии последовательно, exit codes[0,0].
`inputs.json` фиксирует исходники/paired.npz; `verification.json` подтверждает
все snapshot hashes, независимый scalar replay четырёх ключевых политик и
декомпозицию текущего правила.11 research tests passed, включая точную границу
1000, инверсию Dire, NaN, различение entry/outcome и сохранение старого>0.

```sh
venv_catboost/bin/python3 scripts/ops/research_lane_minute_schedule.py --paired PAIRED_NPZ --output-dir TARGET1000_OUTPUT --target-min-lead 1000 --source-index-offset 0
# Repeat with --source-index-offset -1 and a distinct output directory.
```

Где искать ошибку: перепутать>=1000 с>1000; применять abs к pending target;
подменить entry threshold итоговым исходом; прочитать старые93.62% как новый
результат; менять trigger masks при рескоринге старого правила; принимать
обычный10-минутный fallback за100% прогноз. Остальные clock/as-of/многократные
проверки и ограничения командного NW из RISKS сохраняются.

### Дополнение: поминутный NW и цель NW10>=1500

**OBSERVED.** Общий командный NW на стороне ожидающего win-сигнала.
Confirmation:1413 ordinary wait600 карт из3941 поздних карт,17.07–04.09.2026.
Порог означает не меньше указанного NW. Ячейка: частота и hits/n.
Это независимые срезы, ранее пересёкшие порог карты здесь не удаляются.

| Минута | >=500 | >=1000 | >=1500 | >=2000 | >=2500 | >=3000 |
|---|---|---|---|---|---|---|
| 1 | 37.0% (10/27)* | 25.0% (1/4)* | — (0) | — (0) | — (0) | — (0) |
| 2 | 50.0% (62/124) | 78.6% (11/14)* | 50.0% (1/2)* | — (0) | — (0) | — (0) |
| 3 | 53.2% (108/203) | 66.7% (30/45) | 80.0% (4/5)* | 100.0% (1/1)* | — (0) | — (0) |
| 4 | 56.4% (162/287) | 76.5% (62/81) | 84.2% (16/19)* | 100.0% (4/4)* | 100.0% (1/1)* | — (0) |
| 5 | 57.2% (198/346) | 71.3% (107/150) | 84.4% (38/45) | 90.9% (10/11)* | 66.7% (2/3)* | 0.0% (0/1)* |
| 6 | 55.9% (232/415) | 69.6% (156/224) | 85.1% (74/87) | 92.1% (35/38) | 100.0% (12/12)* | 100.0% (2/2)* |
| 7 | 58.4% (255/437) | 70.8% (199/281) | 82.9% (121/146) | 93.6% (73/78) | 97.1% (34/35) | 100.0% (10/10)* |
| 8 | 56.9% (269/473) | 73.5% (241/328) | 82.5% (174/211) | 94.8% (110/116) | 98.3% (57/58) | 100.0% (28/28)* |
| 9 | 57.3% (282/492) | 73.3% (264/360) | 89.3% (233/261) | 96.3% (158/164) | 97.8% (87/89) | 98.0% (50/51) |
| 10 | 54.9% (293/534) | 72.3% (293/405) | 100.0% (293/293) | 100.0% (212/212) | 100.0% (154/154) | 100.0% (92/92) |

*n<30: крайне нестабильная оценка. На10:00 это уже наблюдаемый факт;
100% при>=1500 следует из определения цели и не является прогнозом.
Полные Wilson95CI, discovery и отдельная allowlist-популяция сохранены в JSON.

**Первое пересечение, отдельно от срезов.** Подбор на остающихся discovery-картах,
минимум100, нижняя Wilson-граница>=90%/95%. Точность только ранних отправок.

| Правило | hits/n | Частота | Wilson95CI | Охват wait600 | Средняя минута |
|---|---:|---:|---|---:|---:|
| 4–9:1000 (прод) | 275/470 | 58.51% | 54.00–62.88% | 33.26% | 6.29 |
| 4–9:1500 | 247/321 | 76.95% | 72.04–81.22% | 22.72% | 7.22 |
| 4–9:2000 | 170/186 | 91.40% | 86.48–94.64% | 13.16% | 7.56 |
| 4–9:2500 | 98/101 | 97.03% | 91.63–98.98% | 7.15% | 7.84 |
| 5:1800;7:1900;8:2300;9:2000 | 168/183 | 91.80% | 86.92–94.97% | 12.95% | 7.66 |
| 7:2500;8:2400;9:2500 | 102/105 | 97.14% | 91.93–99.02% | 7.43% | 7.96 |

Пропущенная минута в fitted-расписании означает отсутствие нового разрешения
в этой дискретной проверке. Автоматическое удержание порога до следующей строки
и непрерывный live replay здесь не тестировались.

Clock sensitivity: raw index=minute−1 даёт237/420=56.43% для текущего правила;
LCB90:6/7:1900,8/9:2000 —117/130=90%; LCB95:6:2000,8:2200,9:2300 —88/91=96.70%.
Меняются и наблюдение, и target. Явных timestamp для элементов массива нет.

**DERIVED/INFERRED.** Current>=2000 на6–9 связан с частотой92–96% достижения>=1500;
первое пересечение2000 на4–9 даёт91.40%. Срез нельзя подменять целым правилом.
Большие перевесы на первых минутах слишком малочисленны для уверенного вывода.

**NOT_CHECKED.** Свежая будущая выборка, доходность, полная live odds/roster
доступность, точный source clock. Confirmation использован многократно, интервалы
не корректируют множественный подбор. Командный NW не измеряет фарм крипов
или победу конкретного героя на линии. Прод при смене цели не менялся.

Харнесс: `scripts/ops/research_lane_minute_schedule.py`; воспроизводимый запуск:

```sh
venv_catboost/bin/python3 scripts/ops/research_lane_minute_schedule.py --paired PAIRED_NPZ --output-dir TARGET1500_OUTPUT --target-min-lead 1500 --source-index-offset 0
# Repeat with --source-index-offset -1 and a distinct output directory.
```

Артефакты: `runtime/artifacts/star-dispatch/lane_wait_20260914/target1500/`:
`inputs.json`, `commands.json`, `run.log`, `exit.json`, `verification.json`,
`{primary,offset_minus1}/minute_schedule.json`. Один nice19 процесс PID91983
завершил обе версии exit[0,0]. Frozen hashes проверены после расчёта.
Независимо проверены480 наборов slice counts и192 scalar policy replays.
12 research tests passed, включая inclusive1500 и инверсию Dire. Первый
верификатор остановлен exit130 из-за повторной распаковки NPZ; после однократной
загрузки массивов проверка завершилась exit0. Основной расчёт не повторялся.

Где искать ошибку: current>=g не равно точному NW=g; одна карта встречается
в разных минутах; срез не равен первому пересечению;10-минутные100% не прогноз;
знак берётся от pending-сигнала. Нельзя приписывать результаты всех wait600
узкой группе ML47–53 или всем Dota-матчам. Fallback10 не включён в раннюю точность.

## CHANGED

- `scripts/ops/research_lane_wait.py`: joins, replay dispatch, slices и first crossing.
- `scripts/ops/research_lane_dictionary.py`: exact source AST cascade, SQLite read-only,
  frozen environment, словарь для28241 карт; импорт приложения/ключей не нужен.
- `scripts/ops/research_lane_residual_check.py`: контроль калибровки, более поздняя
  проверка, paired day bootstrap. Явные reductions для матриц из2–3 колонок.
- `scripts/ops/research_lane_minute_schedule.py`: conditional-unsent schedules, growth и clock sensitivity.
- `base/tests/test_research_lane_wait.py`:12 регрессионных тестов (цели1000 и1500).
- `base/ml_dispatch.py`, `base/cyberscore_try.py` и их тесты: разрешённый владельцем live early-NW gate.
- Исследовательский код/отчёт/индекс и отдельно разрешённое изменение обычного wait600.

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

- Дополнение: оба minute_schedule workers exit0; frozen input hashes неизменны;
  независимый replay дал489/534. `minute_schedule/verification.json`.

## POINTERS

Полный artifact root: `runtime/artifacts/star-dispatch/lane_wait_20260914/`.
Авторитетные файлы:

- `.orchestra/campaigns/run-e96058ca734b7d16df93bf42/artifacts/dictionary-residual/dictionary.{json,npz}`
- `direct_local/nw_complete/{summary.json,paired.npz}`
- `direct_local/residual_arithmetic/residual_check.json`
- `direct_local/verification.json`, `input_manifest.json`, `residual_arithmetic_inputs.json`
- `prod_lane_environment.json`, `campaign_remote.json`, `extraction.json`.
- `minute_schedule/{inputs.json,commands.json,run.log,exit.json,verification.json}`
  и `minute_schedule/{primary,offset_minus1}/minute_schedule.json`.
- `minute_schedule/gate_tests_final.log` —119 passed.

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
venv_catboost/bin/python3 scripts/ops/research_lane_minute_schedule.py --paired OUTPUT_NW/paired.npz --output-dir OUTPUT_SCHEDULE --source-index-offset 0
# Sensitivity: same command with --source-index-offset -1 and a different output dir.
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
самостоятельный повод отменить ожидание. Владелец после расчётов отдельно разрешил production4:00/+1000 и restart;
переменные пороги остаются исследовательскими. Независимая будущая выборка
нужна для нового выбора расписания.

```orchestra-evidence-v1
{
  "schema": "orchestra-evidence-v1",
  "constraints": [
    "Original research: total team net worth. User subsequently explicitly authorized live240s/1000 deployment and restart.",
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
    },
    {
      "id": "SCHEDULE",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/minute_schedule/primary/minute_schedule.json",
      "sha256": "f970a5b33410ff8649b5c1e2c10e80a253ee134b57edcee07da361df9d670d3e"
    },
    {
      "id": "CLOCK_ALT",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/minute_schedule/offset_minus1/minute_schedule.json",
      "sha256": "84bb3215516ee89a5a119f4edd515d59476f9febbcd69d7f309089c5efbd1a0c"
    },
    {
      "id": "SCHEDULE_VERIFY",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/minute_schedule/verification.json",
      "sha256": "b305b917fa80d9e22b374d9c1f2e4f77c14a579c7fe9564b3df4f20135ee052c"
    },
    {
      "id": "DEPLOY",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/minute_schedule/prod_acceptance.json",
      "sha256": "14fd7a55ea528111c3fb8d3a2c7c31b48bed28d5c814bfc087826c91e074efe1"
    },
    {
      "id": "LIVE_CYCLE",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/minute_schedule/prod_cycle_acceptance.json",
      "sha256": "29afe15b06ba9a2c59fe9e848d60894933576421ef609920833de8e1329c8b19"
    },
    {
      "id": "TARGET1000",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/target1000/primary/minute_schedule.json",
      "sha256": "371e7a3aea53b53e8f1738ba5f9447146b8de65ad54ce235cd9f151bbf3e0e22"
    },
    {
      "id": "TARGET1000_ALT",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/target1000/offset_minus1/minute_schedule.json",
      "sha256": "996e7939f86d12907be1c00c200603a9fb33b17296937b18d7b65b23080ed785"
    },
    {
      "id": "TARGET1000_VERIFY",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/target1000/verification.json",
      "sha256": "ccf93cf885d962083170e2eb67d302b19d9acd343430ae538e0d925e29821011"
    },
    {
      "id": "TARGET1500",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/target1500/primary/minute_schedule.json",
      "sha256": "2c7126a92e58e492c2e816f8c0eb2e6a5e39d7d1614be78d56db4128e5948891"
    },
    {
      "id": "TARGET1500_ALT",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/target1500/offset_minus1/minute_schedule.json",
      "sha256": "8a06f74898f1ec5c6f0d79983dd9c6c1945b2248d66dc54beb3d3cb73e267761"
    },
    {
      "id": "TARGET1500_VERIFY",
      "path": "runtime/artifacts/star-dispatch/lane_wait_20260914/target1500/verification.json",
      "sha256": "7a4963935e3f3445f6327998554e3f7f1dd283e6eeecf512e5030b8fbe582c59"
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
    },
    {
      "id": "F4",
      "kind": "OBSERVED",
      "claim": "Conditional-unsent schedule start3 LCB90:489/534; alternate source clock509/575. Growth guards0/250 do not change primary90 confirmation triggers.",
      "sources": [
        "SCHEDULE",
        "CLOCK_ALT"
      ],
      "scope": "Reused chronological confirmation; observed tradeoff, not prospective or causal efficacy."
    },
    {
      "id": "F5",
      "kind": "OBSERVED",
      "claim": "User-authorized constant240s/1000 gate deployed932e5ef, restarted13:32:34MSK; active onePID676356 NRestarts0; startup enables gate and code hashes match.",
      "sources": [
        "DEPLOY"
      ],
      "scope": "Technical startup acceptance. No qualifying live early delivery observed at initial check."
    },
    {
      "id": "F6",
      "kind": "OBSERVED",
      "claim": "Postrestart normal cycles yielded3 new audit rows on2 live maps with signedNW; no tracebacks/tick errors; existing sent signals deduplicated.",
      "sources": [
        "LIVE_CYCLE"
      ],
      "scope": "Live dataflow and dedup; neither map exercises a new early-NW delivery."
    },
    {
      "id": "F7",
      "kind": "OBSERVED",
      "claim": "With inclusive signed team NW10>=1000, current4/1000 policy347/470; newLCB90 schedule268/296; newLCB95 schedule224/236. Production unchanged during this target revision.",
      "sources": [
        "TARGET1000",
        "TARGET1000_ALT"
      ],
      "scope": "Frozen pending population, reused chronological confirmation; clock sensitivity retained."
    },
    {
      "id": "F8",
      "kind": "OBSERVED",
      "claim": "Target signed teamNW10>=1500: minute1..10 slices; current4/1000 policy275/470; conditionalLCB90 168/183; conditionalLCB95 102/105. Production unchanged.",
      "sources": [
        "TARGET1500",
        "TARGET1500_ALT"
      ],
      "scope": "Independent per-minute slices differ from first-crossing policies; reused retrospective confirmation and uncertain source clocks."
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
    },
    {
      "id": "T2",
      "status": "PASS",
      "sources": [
        "SCHEDULE_VERIFY"
      ],
      "observed": "Both schedule jobs exit0, unchanged frozen inputs, independent489/534 replay;119 research/gate/wrapper/ledger tests passed."
    },
    {
      "id": "T3",
      "status": "PASS",
      "sources": [
        "TARGET1000_VERIFY"
      ],
      "observed": "Workers exit0; frozen hashes unchanged; independent replay4policies; current outcome buckets347/93/30;11tests pass."
    },
    {
      "id": "T4",
      "status": "PASS",
      "sources": [
        "TARGET1500_VERIFY"
      ],
      "observed": "Both jobs exit0; frozen hashes unchanged; 480 independent slice counts and 192 scalar policy replays; 12 tests passed."
    }
  ],
  "limitations": [
    "Current dictionary/model snapshots are retrospective.",
    "Confirmation-only day bootstrap is conditional on fitted coefficients.",
    "Production allowlist confirmation for1000 has58 triggers.",
    "Threshold search Wilson intervals are not simultaneous guarantees.",
    "Raw clocks lack explicit timestamps; six sample lengths support primary but do not prove it.",
    "No creep-farm temporal attribution."
  ],
  "contradictions": [
    "Dictionary full neutral result60.89% weakens to53.33% on30 confirmation maps.",
    "Per-minute90% threshold selection yields only84.70% first-crossing confirmation."
  ],
  "decision_required": []
}
```
