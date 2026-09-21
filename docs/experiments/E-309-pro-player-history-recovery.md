# E-309 — восстановление персональной истории из про-архива

Дата: 21.09.2026. Offline; продолжение после отрицательного E-308.

Последующая [независимая проверка E-308/E-309](prematch-winner-audit-20260921.md)
подтвердила арифметику, выявила неиспользованные источники и исправила три
проверки входных данных. В текущем corpus затронутых этими дефектами записей нет;
метрики ниже остаются прежними. 131 конфликтующий duplicate различается только XP.

## Гипотеза и ограничения

Пользователь указал на недостаточный учёт опыта игроков на героях и уточнил:
в имеющемся public corpus нет целевых pro-игроков. Public account history
не используется для восполнения их опыта. Объём пабликов, использованных
в E-308 для draft-моделей, не равен объёму персональной pro-истории.

В E-308 уже были число игр, WR игрока/героя, general recent WR и вариант
100 дополнительных признаков по ролям. Поэтому утверждение «игроки вообще
не учитывались» неверно; также не доказано, что виновато только усреднение.
Но player-hero WR был всевременным `(wins+1)/(games+2)`, без свежести
практики на этом герое и без поправки на ожидаемый результат против соперника.
Возраст игрока не входил в модель; подтверждённые даты рождения не собраны.

Сначала проверяется конкретная потеря исходных наблюдений, затем малое
добавление признаков истории, а не новый широкий перебор гиперпараметров.
Это не обещание значительного WR. Старый отрицательный terminal E-308 сохраняется.

## Наблюдения до изменения

На 9 038 строках June 1–July 15 exclusive (`league>0`, `elo_eligible`)
90 380 player slots. Это аудит входов, не новая метрика исходов:

- медиана записанной истории игрока на выбранном герое — **2 карты**;
- 27 310 slots (30.22%) без такой истории;
- 68 191 slots (75.45%) имеют меньше 10 карт;
- на 7 344 картах хотя бы один slot без истории;
- лишь 121 карта (1.34%) имеет минимум 10 прошлых карт для всех десяти slots.

В canonical K24 event stream 1 396 158 карт; **397 617** отсутствуют в rich
таблице истории ML. Из 389 963 таких карт до June 1, 72 557 содержат хотя бы
одного игрока из selection cohort. 12 037 пропусков относятся к 2025 году,
18 906 — к 2026. Это сравнение MID, не доказательство, что каждая отсутствующая
карта имеет все необходимые hero fields.

Извлечение rich требует всех десяти позиций и отбрасывает карту при отсутствии
любой позиции (`runtime/experiments/misc/pro_corpus_rich.py`). Это установленное
ограничение; доля всех пропусков, объясняемая только им, пока не измерена.
Для персонального опыта достаточно валидных account/hero/outcome/end-time.

Артефакты исходного аудита:
`runtime/artifacts/elo/general_ml_20260921/player_information_audit/coverage.json`,
`raw_history_gap.json`. Coverage построен из `role_features/dataset.npz`,
SHA256 `53c55fa159d949f94678b99344ff12821612ffd82ab41ef8637f58dfa0929bb6`.
Для каждой роли Dire count = Radiant count − diff; round до целого; затем
счётчики по десяти slots. Raw gap = `events.mid` minus `rich.mids`, строго
`events.end < 2026-06-01`, пересечение account IDs с указанным cohort.

## Протокол

Новая история только из raw pro archive; позиции не обязательны. MID
дедуплицируются детерминированно. В query допускаются только события,
завершившиеся строго до её начала. Hero XP берётся из прошлой записи игрока
на этом герое, не из результата текущей карты. Missing не заменяется
выдуманным опытом или возрастом.

Исходные 71 колонки и identity arrays E-308 должны сохраниться побитово.
Добавляются персональные объём/WR, сглаженный hero WR относительно общего,
свежесть практики, затухающий hero WR и прошлый записанный hero XP.
Выбраны два фиксированных прежних рецепта: `lgb31_all`, `lgb31_recent`.
Сравнение — только exploratory June/early-July folds, одинаковые MID и исходы.
Повторно открытый August–September не считается независимым тестом новой версии.

## Харнесс и где искать ошибку

Протокол/manifests: `runtime/artifacts/elo/general_ml_pro_history_20260921/`.
Исполнитель: `base/tools/prematch_pro_player_history.py`; регрессии:
`base/tests/test_prematch_pro_player_history.py`.

Проверять не только raw count, но account/hero identity, время окончания,
конфликтующие дубликаты, self-map leakage, XP unknown/zero, обновление decay
на момент query, неизменность старого префикса, и разное покрытие по времени.
Рост числа заполненных значений сам по себе не является улучшением модели.

## Результат

Реализован `base/tools/prematch_pro_player_history.py`: потоковое чтение
raw JSON, сохранение compact candidate binary, deterministic MID dedup,
проверка совпадающих MID с query, строгий `end < start`, 132 добавочных колонки.
Названия `_diff1..5` означают Radiant minus Dire в соответствующей роли.
`history.npz` содержит проекцию признаков, а `source_candidates.bin` — исходные
кандидатные записи. `--threads` принимается, но сборщик работает последовательно.

Регрессии покрывают пропущенные позиции, различение аккаунтов, prior XP,
дедупликацию, равную границу end/start, пустой roster и неоднозначный overlap.
Проверены UTC-границы июня/июля, сохранение базового префикса на fixture.
Совместно с предыдущим ML harness: **29 passed**; compile и diff-check прошли.

Полный recovery campaign `run-f1566653ed01385d9c37c462` завершён с exit 0;
проверка неизменности snapshot после выполнения пройдена. 323 JSON файла,
1 731 619 raw records, 1 730 200 валидных candidates, 21 942 дубликата
(131 отличаются по полям), **1 708 258 canonical карт**. Во всех 1 261 465
совпадающих с query MID прошли проверки времени, исхода и account/hero/side.

Полная проверка результата: 1 261 465 строк × 203 колонки; исходные **71 колонка
и 14 identity/contract arrays сохранены**, все новые значения конечны.
Dataset SHA256 `4efd1819b93ede6e186e8a330974d6062b9cce11a7be9fd76a11123c0a351f1a`.
`features/metadata.json` и `recovery_verification.json` — первичные отчёты.

На тех же 9 038 картах / 90 380 player slots:

| История игрока на выбранном герое | До | После |
|---|---:|---:|
| Хотя бы одна прошлая карта | 63 070 (69.78%) | 66 070 (73.10%) |
| Не меньше 10 прошлых карт | 22 189 (24.55%) | 24 249 (26.83%) |
| Медиана числа прошлых карт | 2 | 3 |

Причинно предшествующий положительный hero XP найден для 57 402 slots (63.51%).
Это доступность прошлого поля, не текущий XP и не процент угаданных карт.

Пользователь явно разрешил временное исключение из load-average ограничения:
одна задача, максимум два потока. Каждый campaign сохранил отдельный snapshot
ресурсов; общая `.orchestra/resources.json` после launch возвращена к исходному
содержимому. Recovery и два fixed ablations составляют один разрешённый эксперимент.

Первый recovery preflight выявил изменение трёх служебных metadata JSON;
actual match-file hashes совпали. `metadata_input_refresh.json` сохраняет замену
их SHA, `recovery_campaign.v2.json` — исправленный manifest. Старые файлы сохранены.
Для ablations проверен diff harness с исходным E308 snapshot: добавлены другие
неиспользуемые рецепты и schema guards, параметры двух выбранных LGBM-рецептов,
folds, веса и пороги не изменены (`training_harness_review.diff/.json`).

Оба fixed ablations завершены в `run-58c329ed5d4b3f561815df97` с exit 0,
snapshot post-check и SHA всех выходов проверены. На каждом из 9 018 матчей
MID, label, series и K24 probability **совпадают** с сохранёнными E308 predictions.
Выборка меньше coverage-аудита из-за embargo окончания у границ folds.

| Рецепт | До, WR | После, WR | Прибавка к тому же рецепту | 95% CI прибавки |
|---|---:|---:|---:|---:|
| `lgb31_all` | 64.11% (5781) | 64.45% (5812) | +0.344 п.п. | [−0.274; +1.008] |
| `lgb31_recent` | 64.01% (5772) | 63.58% (5734) | −0.421 п.п. | [−1.055; +0.221] |

K24 на этих же картах: **61.23% (5522/9018)**. После восстановления:

| Рецепт | Разница WR с K24 | 95% CI | Расхождения: ML / K24 | Log loss ML / K24 |
|---|---:|---:|---:|---:|
| `lgb31_all` | +3.216 п.п. | [2.201; 4.269] | 1283 / 993 | 0.626703 / 0.644304 |
| `lgb31_recent` | +2.351 п.п. | [1.294; 3.441] | 1330 / 1118 | 0.633166 / 0.644304 |

На расхождениях новой и прежней **той же** модели: all 446:415,
recent 418:456. Log-loss improvement против прежней модели: all +0.000902
(CI [−0.001495; +0.003306]), recent −0.001210
(CI [−0.004039; +0.001711]). Bootstrap: 4 000 выборок целых серий,
7 629 clusters; missing series использует отдельный MID. Это условные интервалы
на уже использовавшейся selection-выборке, не поправка на весь прошлый поиск.

**Вывод:** восстановление увеличило доступность истории, но существенной
добавочной точности не доказало. Порог сильного результата +5 п.п. к K24 не
пройден даже здесь. Лучший прежний ensemble E308 на selection имел 64.69%,
поэтому 64.45% также не является новым лучшим результатом всего поиска.
Независимое превосходство над Elo **не установлено**; уже открытый terminal
E308 повторно не проверялся и его отрицательный итог сохраняется.

Возраст не добавлен: в используемом наборе нет проверенных дат рождения.
Эксперимент проверяет конкретное расширение pro-истории, а не доказывает,
что все возможные персональные признаки исчерпаны. Производственная модель,
пороги, сервисы не менялись. Новые full-refit/serving weights не публиковались.

Первичный сравнительный отчёт: `ablation_comparison.json` в artifact root.
Харнессы full-prefix verification и paired comparison:
`runtime/experiments/elo/general_ml_pro_history_20260921/verify_recovery.py`,
`compare_ablations.py`.

Команды воспроизведения:

```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -m pytest base/tests/test_prematch_pro_player_history.py base/tests/test_prematch_causal_dataset.py base/tests/test_prematch_winner_research.py -q
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/elo/general_ml_pro_history_20260921/recovery_campaign.v2.json
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_pro_history_20260921/recovery_campaign.v2.json --background
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/elo/general_ml_pro_history_20260921/ablation_campaign.v2.json
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_pro_history_20260921/ablation_campaign.v2.json --background
PYTHONPATH=. /Users/alex/Documents/ingame/venv_catboost/bin/python3 runtime/experiments/elo/general_ml_pro_history_20260921/compare_ablations.py run-58c329ed5d4b3f561815df97
```

Проверка публикации данных в исторический момент отсутствует: source end
обеспечивает порядок архивных записей, но не доказывает, когда STRATZ опубликовал
поле XP. Текущая карта и будущие результаты при построении не допускаются.
