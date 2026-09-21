# E-311 — история линий и результативности игроков в общей winner ML

21.09.2026. Протокол зафиксирован до обучения новых вариантов. Offline only.

## Протокол и граница выводов

Основа — 203 признака E-309 и прежние causal public draft projections E-308.
Добавляются две независимые семьи по 160 признаков. Performance: XPM, networth,
last hits, denies, hero/tower damage, healing и level из raw pro archive.
Все totals, кроме final level, нормируются на длительность прошлой карты.
Lane: gold_t/xp_t/lh_t/dn_t на 300/600 секунд из сохранённого OpenDota SQLite.
`gold_t` — накопленное золото, не net worth.

Источник допускается только при `end < query start`. Среднее берётся по последним
20 завершённым картам игрока; null/отсутствие/нечисло/отрицательное значение
не подменяется нулём. Отдельно считаются последние 20 карт на выбранном герое;
разность hero mean и general mean сглаживается множителем `n/(n+10)`.
При отсутствии hero observations residual=0 только при известном general mean,
с отдельными counts. Для player mean сохраняются обе команды и пять causal
ролей; hero residual агрегируется по стороне. Дополнительно counts, games и
hero games. Роли query взяты из прежней истории, не из текущего результата.

Raw source повторно читается по canonical policy E-309 с проверкой
identity/time/outcome. Отсутствующие позиции не мешают использовать игрока.
OpenDota проходит сверку MID, account, hero, side, исхода, start/end с основой.
Дубликаты игроков и несовпадения вызывают ошибку. Недоигранные окна masked.
Историческое время публикации API не подтверждено: это end-causal reconstruction.
Даты рождения не выдумываются. Public corpus не используется как личная история
целевых про-игроков.

Фиксированные arms: lane, performance, обе семьи. Во всех прежний `lgb31_all`
(450 деревьев, leaves31, half-life365), без перебора гиперпараметров. Два прежних
fold: train ends до June1/July1 минус час, series purge; evaluation June1–July1
и July1–July15 минус час. Те же карты сравниваются с E-309 lgb31_all, выбранным
ансамблем E-308 и K24. Метрики: accuracy/WR, log loss, paired series-bootstrap CI,
расхождения направлений. Сильное улучшение означает ≥5 п.п. против K24,
нижняя граница CI >0 и меньший log loss — прежний критерий не ослабляется.

Это повторно используемая exploratory selection, не независимый тест.
Август–сентябрь уже открыт в E-308, повторно для выбора не используется.
При выборе перспективного варианта фиксируются recipe/schema и сохранённые
веса; подтверждение сильного превосходства потребует новых будущих карт.
Ни выбор по этим fold, ни refit на всех метках не доказывает будущий WR.

## Харнесс, команды и где искать ошибку

`base/tools/prematch_player_performance.py --mode build --dataset <E309 dataset>
--raw-dir <raw pro> --database <OpenDota sqlite>
--output-dir <job output> --threads {threads}`.

Затем `--mode experiment --dataset <E309 dataset> --features <built projections>
--family lane|performance [--family ...] --draft <causal projection> --draft <second>
--output-dir <job output> --threads {threads}`. Полные команды и SHA входов —
в campaign manifests `runtime/artifacts/elo/general_ml_performance_20260921/`.
Запуск только через `.orchestra/runtime/orchestra.py resources preflight/run`.

Проверять строгость временной границы, сортировку equal-end по MID, отсутствие
перехода prefix sums между аккаунтами, player-hero ключи, missing counts,
расхождения сторон между API, нормировку totals и categorical indices после
вставки новых колонок. Старый rich parser обнулял missing: его pstats здесь
не используются. Canonical duplicate policy сохраняет первую запись, а не
выбирает наиболее заполненную после просмотра результата.

## Результат

Build `run-3eddc394029508736ad4c839` завершён с exit 0 и проверкой неизменности
input snapshot. 324 raw files, 1 731 690 records, 1 708 329 canonical maps,
16 036 007 player rows; OpenDota 16 549 maps / 165 490 player rows.
Оба массива по 160 колонок спроецированы на прежние 1 261 465 query maps.

`verify_features.py` независимо фильтрует завершённые карты каждого игрока,
выбирает последние 20, считает means/counts/shrinkage без вызовов builder.
Проверены 103 query maps (100 случайных selection + три Daxak), 32 960 values.
Max absolute error performance 0.00004960, lane 0.00019836: float32 rounding,
в пределах заранее заданных atol=0.0002/rtol=0.00002. Labels Daxak не используются
для нового выбора модели. Side/role направления и missing совпали.

До June1 среди 960 345 league+Elo-eligible maps хотя бы один Radiant игрок имеет
performance history на 954 505 maps, lane history — только на 2 579.
В June это 6 848/6 927 и 6 554/6 927 соответственно. Это coverage по start,
не размер embargo-filtered evaluation (там в сумме 9 018 maps).
Раннее обучение lane ограничено началом OpenDota corpus в May2026.

Raw missingness: 11 067 923 из 16 036 007 значений networth отсутствуют;
они теперь NaN. Остальные семь полей численно присутствуют во всех извлечённых
player rows, но это не подтверждает, что upstream API никогда не заполнял нули.

Регрессии: 56 passed (14 новых + 42 прежних), compile/diff checks passed.
Ревью границы времени/identity/categorical placement не нашло P0/P1.
Saved evaluation model reload проверяется на всех evaluation rows каждого fold.
Неудачный build сохраняется и требует нового output directory; cleanup не нужен.

Training campaign `run-c486ddad64b226a62607daec` завершён: local 3/3, все exit 0,
все input snapshots проверены. Шесть fitted models (два fold × три arms).
Повторная загрузка всех шести моделей воспроизводит все predictions точно.
`compare.py` независимо пересчитал accuracy, log loss, Brier и paired intervals,
проверил MID/y/series/K24, семьи/число features и отсутствие series overlap.

| Вариант | Верно / 9018 | WR | Log loss |
|---|---:|---:|---:|
| K24 | 5522 | 61.2331% | 0.644304 |
| E-309, прежний lgb31_all | 5812 | 64.4489% | 0.626703 |
| E-308, прежний выбранный ансамбль | 5834 | 64.6928% | 0.623692 |
| + только lane | 5772 | 64.0053% | 0.626671 |
| + только performance | **5844** | **64.8037%** | **0.622404** |
| + performance и lane | 5829 | 64.6374% | 0.622902 |

Лучшая новая версия по зафиксированному правилу — performance, 378 learned inputs.
Её результат против K24: +3.5706 п.п., series-bootstrap95 CI [2.5427; 4.6115].
В 2284 расхождениях ML права 1303 раза, Elo — 981 (ML 57.0490%).
Против E-309: +0.3548 п.п., CI [-0.3226; 1.0322], расхождения 508:476.
Против прежнего лучшего ансамбля: **+0.1109 п.п., CI [-0.6064; 0.8065]**,
расхождения 520:510. Значимый прирост WR к прежней лучшей ML не установлен.

Log-loss gain к E-309 0.004300, CI [0.001487; 0.007222]; к старому ансамблю
0.001288, CI [-0.001649; 0.004295]. Оценки интервалов условны на этих fitted
models, не включают вариацию seeds/подбора на уже исследованной selection.
Критерий ≥5 п.п. против Elo **не пройден ни одним arm**; новый независимый
terminal не открывался. Покрытие и дополнительные показатели не доказывают
сильное превосходство модели.

`selection.json` фиксирует performance-only, recipe и hashes до all-label refit.
Refit использует тот же frozen target corpus до 2026-09-21 minus one hour;
новые source rows не превращаются автоматически в дополнительные target labels.
Raw scores без новой независимой calibration, offline only. Production не менялся.

Refit `run-20649f0242f0dda7cf8e611e`: local 1/1, exit 0, input snapshot verified.
Обучены 1 261 465 target maps; сохранён кандидат
`data/prematch_general_performance_20260921/candidate.joblib` (378 inputs), SHA256
`57170c0605b50d4ea6f42d38e68f19eaa99bd6cd5d6681065d2af43c6cedd5c5`.
Повторное чтение после копирования точно воспроизвело 67 smoke predictions,
max absolute error 0. Эти строки входят в обучение: проверяется сериализация,
а не обобщающая способность. Полный отчёт и отдельные первичные проверки:
`runtime/artifacts/elo/general_ml_performance_20260921/final_report.md`.

Первый build run-42a98e46a12b535d9cb4dbbe корректно остановлен до обучения:
новый raw файл `7.41e_part050.json` сдвинул file_order прежнего E-309 индекса.
Теперь compact index пересобирается из того же immutable snapshot перед чтением
performance. Старый неуспешный output сохранён; source binary пишется атомарно.
