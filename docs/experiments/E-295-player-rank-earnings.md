# E-295 — призовые и региональный ранг игроков для общей prematch ML

Дата: 18.09.2026. Статус: **три offline-переобучения выполнены; улучшение не доказано**.
Связанные задачи: `ingame-enwj`, аудит общей модели E-292 / `ingame-gr3t`.

## STATUS

DONE для сравнения трёх фиксированных переобучений существующих 35 признаков
и реализации допущения постоянства призовых/рангов в пределах месяца/недели.
Расширенный сбор: 138 проверенных страниц, 300 игроков; призовые покрывают
40 train-карт и 22/38 test-карты, полностью — 27 и 11 соответственно.
DONE для augmented C=0.1 сравнения: кандидат ухудшил log loss и отклонён.
Serving, боевые веса, пороги и production не менялись.

## SUMMARY

### Последнее сравнение с расширенными призовыми

| Вариант | Правильных карт | Accuracy | Log loss | Brier |
|---|---:|---:|---:|---:|
| Frozen baseline | 26/38 | 68.42% | 0.641190 | 0.218578 |
| Baseline + earnings, C=0.1 | 26/38 | 68.42% | 1.457306 | 0.262364 |

Парная разница log loss **+0.816116**, series-bootstrap 95% CI
**[+0.114920; +1.655621]**, 28 серий, 5000 повторов, seed295. Это ухудшение
на ранее изученном тесте, не доказательство качества на будущих матчах.
На 11 полностью покрытых картах accuracy также не изменилась (7/11),
log loss 0.658768 → 0.956361; subset — только описательная диагностика.

В full добавились 33 train-variable earnings columns, в no_org — 29;
остальные columns константны на train и исключены без просмотра test.
Новые сведения доступны лишь на 40/15 205 тренировочных картах, но на 22/38
тестовых: выраженный сдвиг покрытия. Недостаток данных/переобучение — возможное
объяснение ухудшения, а не установленная отдельным экспериментом причина.
Этот кандидат не принят, serving не изменён; по этому тесту параметры не подбирались.

Shared executor: `run-c45a5a8b192dfdf0c32447d4`, job `augmented-refit`, local,
exit0, 14 input hashes verified before/after; 3 outputs collected. Первый
preflight ждал CPU budget; следующий разрешил 1 slot / 1 thread. Для известной
ошибки macOS rename readonly directory снимок опубликован до chmod, затем
запечатан и проверен штатным executor; runtime executor не изменялся.
Артефакты: `.orchestra/jobs/run-c45a5a8b192dfdf0c32447d4/augmented-refit/output/`
(`summary.json`, `predictions.npz`, `research_weights.npz`),
`runtime/artifacts/misc/player_metadata_expand_20260918/prepared/coverage.json`,
`verification.json`, `evaluate_plan.json`, `campaign.json` в родительском каталоге.

OBSERVED:

- В HTML [матча 427986](https://dltv.org/matches/427986/kalmychata-vs-uralan-european-pro-league-season-40)
  `series_item.series_players[].player` содержит Dota account ID (`steam_id`,
  **не** DLTV `id`), `prizes`, `rank`, `rank_updated_at`; позиция берётся из
  `series_players[].role`, не из текущего профиля игрока.
- Проверенный снимок: 10 игроков, 10 значений призовых, 9 численных рангов,
  **0 подтверждённых регионов ранга**. Ankou: rank отсутствует, это не rank=0.
- yowaai: account `234119750`, DLTV ID `42292`, призовые **$20,100**, rank **1006**,
  rank_updated_at **2026-09-10T12:00:24Z**. Значение слишком старое для выбранного
  первоначального TTL 7 суток; TTL является политикой качества данных, не
  оптимизированным параметром модели.
- [Таблица Valve](https://www.dota2.com/leaderboards/#europe) использует
  `https://www.dota2.com/webapi/ILeaderboard/GetDivisionLeaderboard/v0001?division=europe&leaderboard=0`.
  В проверенном ответе строка с ником `yowaai` имеет rank **336**; строки содержат
  `rank/name/country`, но не account ID. Это **кандидат соответствия**, не
  подтверждённое связывание с account `234119750`.
- Дополнительная проверка [OpenDota account 234119750](https://api.opendota.com/api/players/234119750)
  вернула имя профессионального игрока `yowaai`, текущий ник `run it up`,
  `leaderboard_rank=148`. Дата обновления именно ранга и его division в этом
  ответе не установлены. Три расходящихся значения нельзя молча объединять.
- Valve определяет division по матчам за последние 21 день; страна игрока
  не определяет division. Шкалы MMR разных division несопоставимы согласно FAQ.
- На странице игрока найден график `backend_data` с 45 историческими точками.
  Время публикации этих точек, их полнота и division не доказаны; график не
  принят как причинный тренировочный датасет. Текущие призовые на странице
  завершённого матча также не считаются призовыми до того матча.

DERIVED:

- Новые признаки следует проверять отдельными блоками earnings / ranks /
  role aggregates, сначала малым набором. 129 выходных колонок — каталог
  кандидатов, а не рекомендация включить все в модель на маленьком датасете.
- Пользовательский приоритет Europe > SE Asia > остальные сохраняется как
  гипотеза для ablation. Сейчас четыре региона представлены раздельно;
  коэффициенты преимущества не выдуманы, остальные регионы не удалены.

NOT_CHECKED: прирост от призовых/рангов, калибровка, реальные ставки/доходность,
покрытие всех будущих составов, production-интеграция, автообновление по расписанию.

### Переобучение по запросу «переобучи попробуй и сравни»

OBSERVED: замороженный E-287 корпус: 17 604 карты, 15 243 пригодны;
обучение full на 15 205 картах, no_org на 7 614. Общий test: 38 карт / 28 серий
(24 full, 14 no_org), временная граница 05.09.2026 UTC, embargo 1 час.
Три фиксированных C сравниваются на одних и тех же картах, без выбора победителя
для serving. Исходные веса E-287: SHA-256
`361b48af9437742cc429398d2ad7550026660547035b5b614db8d3fc6ee07fdf`.
Текущий удалённый runtime в этом прогоне не инспектировался.

| Вариант | Верных | Accuracy | Log loss ↓ | Brier ↓ |
|---|---:|---:|---:|---:|
| E-287 baseline / повтор C=0.1 | 26/38 | 68.42% | 0.641190 | 0.218578 |
| Сильнее регуляризация C=0.03 | 27/38 | 71.05% | 0.641515 | 0.218276 |
| Слабее регуляризация C=0.3 | 26/38 | 68.42% | 0.641009 | 0.218664 |

Baseline replay и повторное обучение C=0.1 совпали с сохранёнными прогнозами
точно: max absolute probability difference = 0. Для C=0.03 paired Δlogloss
`+0.000326`, series-bootstrap 95% `[-0.007275,+0.008710]`; C=0.3:
`-0.000180`, `[-0.003255,+0.002515]`. 5 000 повторов, seed 295, целые серии
выбираются с возвращением, итог взвешен по картам. Это описательные интервалы
на уже изучавшемся test, без поправки на множественные сравнения.

DERIVED: дополнительный верный исход у C=0.03 не доказывает улучшение вероятностей:
log loss ухудшился, оба интервала включают ноль. C не выбран для активации.
Внутренний временной подбор отклонён: фиксированный draft backbone обучен до
04.09 и достигает ранних train-строк. Независимой ранней validation здесь нет.

OBSERVED: последний старт карты `1789168030` раньше первого снимка
`1789682093.7371168`. Поэтому покрытие метаданных **0/17 604** независимо от
account/position join. Модель с новыми 129 признаками не обучалась: константный
нулевой блок не измеряет их пользу. Свежие значения не подставлялись в прошлое.

### Допущение пользователя: месяц для призовых, неделя для ранга

Пользователь разрешил считать значения постоянными в пределах периода.
Введён отдельный `calendar_period`: календарный месяц UTC для призовых,
ISO-неделя UTC (понедельник–воскресенье) для ранга. Это **приближённая
ретроспектива**, она разрешает использовать снимок, полученный после карты.
Строгий режим и его прежний результат 0/17 604 сохраняются по умолчанию.

OBSERVED: аудит всех 17 604 строк с новым допущением нашёл **2 train-карты**
`8979344496`, `8979484553`, по 2 игрока с известными призовыми в каждой;
полностью покрытых составов нет. Test по-прежнему **0/38**, ранги **0** из-за
неподтверждённого division. 10 аккаунтов исходного снимка вообще не пересекаются
с аккаунтами heldout. Допущение устраняет временной запрет для части строк,
но не создаёт данные об остальных игроках; новое обучение не запускалось.
Доказательства: `runtime/artifacts/misc/player_metadata_period_20260918/coverage.json`
(SHA входов), `matched_maps.jsonl`, `features_strict.jsonl`,
`features_calendar_period.jsonl`. Массив accounts берётся по map ID из E-287 rows;
producer `runtime/experiments/misc/pro_corpus_rich.py` сохраняет порядок
Radiant позиции 1..5, Dire позиции 1..5.

## CHANGED

### Расширение на игроков корпуса, 18.09.2026

Новый `base/tools/expand_player_metadata.py` объединяет bounded collection,
account/position-aligned preparation и paired evaluation. Discovery: результаты
DLTV на 05/12/18 сентября, 169 уникальных ссылок. Из них сохранены 138 полных
валидных составов; 31 страница отклонена из-за отсутствующих/дублирующихся
аккаунтов или позиций. Состав DLTV используется только для извлечения профилей;
сторона и позиция признаков всегда берутся из самой карты корпуса.

Подготовка проверяет SHA входов **до** чтения, включая `rows.npz` с аккаунтами,
и совпадение `mids/ts/y/sids` с frozen matrix. Все HTML/снимки сохранены; исправлен
ложный отказ SHA при CRLF: `load_snapshot` теперь читает исходные UTF-8 bytes
без нормализации переводов строк. Регрессионный тест воспроизводит этот случай.

Покрытие: train 40/15 205 карт хотя бы частично, 27 полностью, 384 player slots;
test 22/38 карт частично, 11 полностью, 201/380 player slots. Более старые месяцы
остаются missing. Проверенные региональные ранги всё ещё 0: Valve не возвращает
account_id, а DLTV/OpenDota не предоставили подтверждённую division. Name/country
join не применяется. Это ограничение источника, не нулевой ранг игрока.

Команды (выходы должны быть новыми; collection_v2 и неуспешные запуски сохранены):

```bash
venv_catboost/bin/python3 -m base.tools.expand_player_metadata collect \
  --dates 2026-09-05 2026-09-12 2026-09-18 --limit 200 \
  --exclude-collection runtime/artifacts/misc/player_metadata_expand_20260918/collection_v2/collection.json \
  --output runtime/artifacts/misc/player_metadata_expand_20260918/collection_v3
venv_catboost/bin/python3 -m base.tools.expand_player_metadata prepare \
  --plan runtime/artifacts/misc/player_metadata_expand_20260918/prepare_plan_v2.json \
  --snapshots runtime/artifacts/misc/player_metadata_expand_20260918/collection_v2/snapshots \
              runtime/artifacts/misc/player_metadata_expand_20260918/collection_v3/snapshots \
  --output runtime/artifacts/misc/player_metadata_expand_20260918/prepared
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight \
  --plan runtime/artifacts/misc/player_metadata_expand_20260918/campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run \
  --plan runtime/artifacts/misc/player_metadata_expand_20260918/campaign.json --background
```

Candidate: те же train/test masks, full/no_org populations, C=0.1 и исходные
35/34 признака, плюс metadata columns с ненулевой variance **на train ветки**.
Scaler также обучается только на train. Baseline воспроизводится до допуска
candidate fit. Полная выборка 38 карт — основное paired сравнение; fully-covered
subset выводится только описательно, без выбора модели по нему. Research weights
имеют отдельный формат и не заменяют serving артефакт.

### Повторяемое сравнение существующей модели

`base/tools/compare_prematch_refits.py` — offline CLI:
`python -m base.tools.compare_prematch_refits --plan PROTOCOL.json
--output-dir NEW_DIR --threads N`. Протокол содержит пути matrix/split/reference/
weights/snapshots, SHA-256 входов, C_values, baseline_C, test_cutoff, embargo_seconds.
Проверяет map/label identity, split, source HTML и replay <1e-6; использует
существующий `retrain_prematch_general.retrain`, сохраняя четыре fallback-ветви.
Выход: `weights_C_*.npz`, `predictions.npz`, `summary.json`. Не меняет serving.
Если снимки пересекают карты по времени, zero-coverage audit отказывается от
расчёта: требуется отдельный проверенный account/position join.

Запуск через неизменяемый снимок общего executor:

```bash
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources preflight --plan runtime/artifacts/misc/player_metadata_refit_20260918/campaign.json
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources run --plan runtime/artifacts/misc/player_metadata_refit_20260918/campaign.json --background
venv_catboost/bin/python3 .orchestra/runtime/orchestra.py resources status --run run-f933ff0141360c7c47d5c868
```

Единственный enabled host local: один job completed, exit 0; 14 входных файлов,
hash verification до/после расчёта. Изначальная попытка завершилась до обучения
с PermissionError (macOS rename read-only directory). Снимок подготовлен с
rename перед chmod, исходники executor не менялись. Терминальная неудачная
receipt сохранена, отсутствие supervisor проверено, тот же campaign возобновлён;
детали `runtime/artifacts/misc/player_metadata_refit_20260918/recovery.json`.

### Контракт данных и признаков

`base/player_metadata.py`:

- `parse_dltv_match(html, source_url=..., observed_at=...)` декодирует literal
  JSON без выполнения JS, проверяет две полные пятёрки, уникальные account IDs
  и позиции. Не использует результаты матча, GPM/KDA или команду-победителя.
- Снимок `prematch-player-metadata-v1`: источник/его SHA-256, время наблюдения,
  series ID и 10 игроков. `dltv_team_id` — **DLTV namespace**, не Dota team ID.
- `PlayerHistory(snapshots).features(radiant_accounts, dire_accounts, asof=...)`:
  две пятёрки **в порядке позиций 1..5 текущей карты**. Стороны передаются
  вызывающим кодом; первая команда сайта не считается Radiant автоматически.
- По умолчанию (`time_policy="strict"`) выбирается последнее наблюдение со
  строгим `observed_at < asof`. Для обучения
  `asof` обязан быть фактическим временем начала карты. Сегодняшняя загрузка
  старой страницы не получает старую дату наблюдения. Равенство cutoff также
  отклоняется. Дубли одинакового снимка допустимы; конфликт account/time — ошибка.
- Призовые доступны максимум 30 суток после наблюдения; нулевые призовые
  являются известным значением, отсутствующие — missing. Преобразование log1p
  уменьшает влияние единичных гигантских выплат, но не доказывает полезность.
- Ранг доступен максимум 7 суток после `rank_updated_at`, только при известном
  division (`europe/se_asia/americas/china`) и `rank_region_source`: объект с
  `identity_method=account_id`, совпадающими account/region/rank/rank_updated_at,
  HTTPS URL, SHA-256 и временем наблюдения, не позднее снимка. Это проверяет
  структуру и временную привязку evidence, а не подлинность стороннего источника.
  Отсутствующий timestamp, неизвестный регион и старые ранги исключаются.
  DLTV parser **не заполняет** регион: подтверждённого источника пока нет.
- Сила ранга = `-log1p(rank)` отдельно для каждой division (больше = лучше).
  Нет единой вручную взвешенной шкалы регионов.
- Группы: вся пятёрка, cores 1..3, supports 4..5, каждая из 5 позиций.
  Средний log1p призовых и покрытие; для трёх групп также min/max/std log1p.
  Для каждой группы и division — средняя сила ранга и покрытие.
- Выход: значения обеих сторон, 89 разностей Radiant−Dire и 40 joint coverage
  (минимум покрытия сторон), итого 129 колонок. Missing заполнен нулём **вместе
  с маской покрытия**. Средние по неполной группе не считаются полной группой.
  Диагностика сохраняет account, время выбранного наблюдения и причину missing.

При `time_policy="calendar_period"`:

- Призовые: один последний снимок аккаунта внутри месяца его `observed_at`
  применяется ко всему этому месяцу. В соседний месяц значение не переносится.
- Ранг: выбирается максимальная пара `(rank_updated_at, observed_at)` внутри
  недели `rank_updated_at`. Один и тот же ранг применяется ко всей неделе;
  дата получения снимка может быть позже. Без `rank_updated_at`/проверенного
  division ранг неизвестен. Месяц и неделя могут ссылаться на разные снимки.
- Прежние rolling TTL 30/7 дней относятся к `strict`; в приближённом режиме
  используются точные календарные границы. Порядок загрузки снимков не влияет
  на выбранное значение; более поздний missing не заменяется старым known.
- Выход дополнен `time_policy`, `retrospective_assumption`, `period_timezone`.
  Для каждого аккаунта сохраняются `earnings_observed_at`, `rank_observed_at`,
  `rank_updated_at`, `earnings_backfilled`, `rank_backfilled`. Прежний scalar
  `observed_at` в приближённом режиме null, чтобы не смешивать два источника.
  Имена/порядок 129 признаков не менялись, но приближённые результаты нельзя
  маркировать как causal backtest. Это разрешённый пользователем эксперимент.

`base/tools/player_metadata.py` — ручной offline CLI:

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 base/tools/player_metadata.py collect \
  --url https://dltv.org/matches/427986/kalmychata-vs-uralan-european-pro-league-season-40 \
  --output runtime/artifacts/misc/player_metadata_20260918/snapshots

/Users/alex/Documents/ingame/venv_catboost/bin/python3 base/tools/player_metadata.py export \
  --snapshots runtime/artifacts/misc/player_metadata_20260918/snapshots \
  --matches runtime/artifacts/misc/player_metadata_20260918/historical_input_v2.jsonl \
  --output runtime/artifacts/misc/player_metadata_20260918/historical_features_new.jsonl

# User-authorized calendar approximation; use a fresh output path on rerun.
/Users/alex/Documents/ingame/venv_catboost/bin/python3 base/tools/player_metadata.py export \
  --snapshots runtime/artifacts/misc/player_metadata_20260918/snapshots \
  --matches runtime/artifacts/misc/player_metadata_period_20260918/matched_maps.jsonl \
  --time-policy calendar_period \
  --output runtime/artifacts/misc/player_metadata_period_20260918/features_calendar_period_replay.jsonl
```

`collect` делает один ограниченный HTTPS-запрос без retry/redirect/browser,
сохраняет полный source HTML и snapshot в уникальном каталоге. 429/ошибка/неполный
состав не заменяют хороший снимок. Автоматический сервис не устанавливается.
Запись: fsync временного файла → атомарный hard link с отказом при существующем
назначении; новые записи каталогов также fsync-ятся. Ссылка `.tmp` сохраняется
согласно запрету удаления; это тот же inode. Power-loss simulation не выполнялась.

Вход `export`: JSONL с `match_id`, `start_ts`, `radiant_players[5]`,
`dire_players[5]`; каждый игрок — `{account_id, position}`. Порядок массива
произвольный: экспорт проверяет уникальные позиции 1..5 и сортирует по ним.
Перед экспортом проверяются SHA-256 исходного HTML и полное совпадение повторно
разобранного снимка с JSON. Ручное обогащение JSON не принимается этим DLTV
импортером; для нового account-bound источника нужен отдельный проверенный адаптер.
Выход JSONL сохраняет ID,
cutoff, признаки, обе стороны и диагностику; одинаковые ID карт отклоняются.
Экспорт — подготовка колонок для причинного тренировочного harness, **не модель**.

## CHECKS

Расширенный сбор/подготовка: **34 tests passed** с `--noconftest` для
`test_expand_player_metadata.py`, `test_player_metadata.py`,
`test_compare_prematch_refits.py`. Проверяются discovery rate-limit receipt,
лимит дат, неизменяемость output, обязательный SHA accounts, CRLF source hash,
смена сторон/позиций, train-only отбор колонок и full/no_org routing.

После добавления calendar_period: **27 tests passed** (`test_player_metadata.py`
и `test_compare_prematch_refits.py`, `--noconftest`). Дополнительно проверены
границы месяца, недели и ISO-года, источник по дате обновления ранга,
неизменность strict, независимость от порядка снимков, маркировка CLI export.
Реальный export двух совпавших карт: strict даёт только нули, calendar_period
даёт ненулевые earnings с backfill-флагом; ранги остаются неизвестными.

Переобучение: 23 tests (`test_compare_prematch_refits.py` +
`test_player_metadata.py`) и 5 существующих `test_retrain_prematch_general.py`
прошли с `--noconftest`, изолированно от чужих правок conftest. Проверяются
поздний DLTV-снимок, отказ при пересечении времён, карта/метка reference,
метрики и кластерный paired расчёт. Все шесть fits converged; max gradient <3e-8.

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -m pytest base/tests/test_player_metadata.py -q
```

Проверяются реальный сокращённый DLTV fixture, account identity, позиции,
missing/нулевые призовые, устаревший ранг, неизвестный регион, временная граница,
выбор между снимками, конфликт, симметрия сторон, защита от перезаписи и CLI export.
Отдельный live smoke: 10 игроков / 10 prizes / 9 ranks / 0 divisions.
Экспорт для завершённой карты `9003685896` с нынешним снимком даёт нулевое
покрытие: ожидаемый отказ подставлять будущее в прошлое.

## POINTERS

- Протокол и SHA входов: `runtime/artifacts/misc/player_metadata_refit_20260918/`.
- Результаты: `.orchestra/jobs/run-f933ff0141360c7c47d5c868/paired-refits/output/summary.json`,
  `predictions.npz`, три `weights_C_*.npz`; receipt на уровень выше, state и
  collected-artifacts в `.orchestra/campaigns/run-f933ff0141360c7c47d5c868/`.

- Артефакты: `runtime/artifacts/misc/player_metadata_20260918/`:
  `match427986.html`, source receipts, `valve_europe.json`,
  `opendota_yowaai.json`, `snapshots/*/snapshot.json`, `feature_smoke.json`,
  `historical_features_verified.jsonl`.
- `base/tests/fixtures/player_metadata_dltv.json`: выбранные поля реального
  источника; HTML/статистические массивы не дублируются в Git.
- E-287: существующая причинная обучающая сборка. E-292: ограничения baseline.

## RISKS — где искать ошибку

1. DLTV может исправлять identity/позицию или отдавать текущий профиль на старой
   странице. Проверять исходный HTML и source hash, account ID и состав карты.
2. Prize money отражает карьеру/размер турниров, а не только текущую силу;
   TI-выигрыш может доминировать. Нужны временные окна выплат, когда появится
   проверенная история. Нельзя делить призовой фонд команды на пять без evidence.
3. Пропуски и stale регионы могут кодировать турнир/источник вместо силы.
   Сравнивать одинаковые карты с baseline, отдельно отчитываться о coverage.
4. Ложный region join по флагу/нику/одному совпавшему рангу; публичный Valve API
   не даёт account ID. Поле `rank_region_source` — provenance, не автоматическое
  доказательство верности пользовательского mapping. Произвольная строка,
  например `country flag`, не принимается как `rank_region_source`.
5. TTL не валидирован как оптимальный. Re-fetch не омолаживает `rank_updated_at`.
6. Тесты сборщика не доказывают качество модели, историчность данных или live delivery.
7. При расхождении нового сравнения сначала проверять SHA матрицы/весов,
   массивы mids/y/sids в split/reference, replay baseline, routed populations,
   C и training-only scaler. 38 ранее изученных карт не являются новым holdout;
   не выбирать гиперпараметры по таблице и не считать accuracy калибровкой.
8. Calendar approximation может включать выигрыш турнира после целевой карты
   в том же месяце или будущий ранг внутри недели. Проверять такое смещение
   отдельно; гипотеза постоянства не доказывает отсутствие утечки. Не смешивать
   strict и calendar_period в одном отчёте без явной маркировки.

## NEXT

Расширение и первое сравнение выполнены. Следующее ограничение — история
призовых: 40 покрытых train-карт мало для 33 дополнительных признаков.
Нужны снимки других месяцев либо новая накопленная выборка; затем отдельный,
ещё не изученный тест. Регион ранга устанавливать проверяемым источником.

1. Получить account-bound источник division+rank с временем обновления, разобрать
   расхождение DLTV/Valve/OpenDota; сохранять новые снимки до начала карт.
2. Собрать проверенную историю призовых по выплатам/датам либо накопить будущую
   выборку. Текущие карьерные суммы не переносить за разрешённый месяц.
3. Зафиксировать block ablation, time/series-disjoint holdout, embargo и baseline
   на одинаковых картах; проверить log loss, Brier, калибровку и покрытие.
4. Только после этого расширять schema обученного артефакта и serving с единым
   feature order. Deployment/restart потребуют отдельного разрешённого шага.
