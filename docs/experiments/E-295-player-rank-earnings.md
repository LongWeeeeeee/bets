# E-295 — призовые и региональный ранг игроков для общей prematch ML

Дата: 18.09.2026. Статус: **сборщик и offline-признаки; улучшение модели не доказано**.
Связанные задачи: `ingame-enwj`, аудит общей модели E-292 / `ingame-gr3t`.

## STATUS

BLOCKED для переобучения/активации: нет проверенной истории призовых и
региональных рангов, доступной до целевых матчей. Локальная реализация сбора и
выгрузки признаков завершена. Serving, веса, пороги и production не менялись.

## SUMMARY

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

NOT_CHECKED: прирост log loss/Brier/калибровки, реальные ставки/доходность,
покрытие всех будущих составов, production-интеграция, автообновление по расписанию.

## CHANGED

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
- Выбирается последнее наблюдение со строгим `observed_at < asof`. Для обучения
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

`base/tools/player_metadata.py` — ручной offline CLI:

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 base/tools/player_metadata.py collect \
  --url https://dltv.org/matches/427986/kalmychata-vs-uralan-european-pro-league-season-40 \
  --output runtime/artifacts/misc/player_metadata_20260918/snapshots

/Users/alex/Documents/ingame/venv_catboost/bin/python3 base/tools/player_metadata.py export \
  --snapshots runtime/artifacts/misc/player_metadata_20260918/snapshots \
  --matches runtime/artifacts/misc/player_metadata_20260918/historical_input_v2.jsonl \
  --output runtime/artifacts/misc/player_metadata_20260918/historical_features_new.jsonl
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

## NEXT

1. Получить account-bound источник division+rank с временем обновления, разобрать
   расхождение DLTV/Valve/OpenDota; сохранять новые снимки до начала карт.
2. Собрать проверенную историю призовых по выплатам/датам либо накопить будущую
   выборку. Текущие карьерные суммы не backfill-ить в обучающие старые матчи.
3. Зафиксировать block ablation, time/series-disjoint holdout, embargo и baseline
   на одинаковых картах; проверить log loss, Brier, калибровку и покрытие.
4. Только после этого расширять schema обученного артефакта и serving с единым
   feature order. Deployment/restart потребуют отдельного разрешённого шага.
