# E-309 — восстановление персональной истории из про-архива

Дата: 21.09.2026. Offline; продолжение после отрицательного E-308.

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

Полный campaign ещё не стартовал: после успешной проверки данных/зависимостей
планировщик дважды отказал из-за отсутствия консервативного CPU-бюджета.
Ограничения ресурсов не изменялись. Первый preflight также выявил изменение
трёх служебных metadata JSON архива; actual match-file hashes совпали.
Обновление только их SHA зафиксировано в `metadata_input_refresh.json`;
новый manifest — `recovery_campaign.v2.json`, старый сохранён.

Новый прирост качества и полная сохранность префикса пока **не измерены**.
Два фиксированных ablation jobs подготовлены, hash нового dataset ожидается.

Команды воспроизведения:

```sh
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -m pytest base/tests/test_prematch_pro_player_history.py base/tests/test_prematch_causal_dataset.py base/tests/test_prematch_winner_research.py -q
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/elo/general_ml_pro_history_20260921/recovery_campaign.v2.json
/Users/alex/Documents/ingame/venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/elo/general_ml_pro_history_20260921/recovery_campaign.v2.json --background
```

Проверка публикации данных в исторический момент отсутствует: source end
обеспечивает порядок архивных записей, но не доказывает, когда STRATZ опубликовал
поле XP. Текущая карта и будущие результаты при построении не допускаются.
