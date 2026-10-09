# ELO состава, вариант A (new3x_dur_ac) — основной с 25.09.2026

Приказ владельца 25.09.2026 «Сразу заменить K24» после E-338
([docs/experiments/E-338-elo-robust-gain-for-prod.md](experiments/E-338-elo-robust-gain-for-prod.md)).
`get_matchup_summary` отдаёт Telegram-блоку и правилам диспетчера рейтинг состава по варианту A. K24 считается
параллельно в том же состоянии и остаётся откатом.

## Формула (на каждую завершённую карту, в порядке событий K24)
- `diff = (Σ radiant − Σ dire) / 5`, `p = 1 / (1 + 10^(−diff/400))`
- `m = clip(1981 / max(dur, 600), 0.6, 1.6)`; длительности нет или она ≤ 0 → `m = 1.0`
- `k = 24·m`; `edge = diff`, если победил Radiant, иначе `−diff`; `k *= 2.2 / (2.2 + 0.001·max(edge, −1000))`
- `d = y − p`; для игрока `k_i = k·(1 + 2/(1 + games_i/30))`; Radiant `r_i += k_i·d`, Dire `r_i −= k_i·d`;
  затем `games_i += 1`
- запрос: среднее пяти рейтингов, та же логистика /400, что у K24; неизвестный игрок = 1500.

Константа 1981 с — медиана длительности всего корпуса. Это не острый оптимум: с 1939 с (медиана до 2024 года,
без заглядывания вперёд) OOS +0,887 п.п., с 2300 с +0,946 п.п., у A +0,924 п.п. K22 и K26 дают +0,863 и +0,939.

## Откуда длительность
- Снимок: длительность карт корпуса `pro_heroes_data/json_parts_split_from_object`.
- Живое обновление: `base/stratz_map_result.py` (`_QUERY` запрашивает `durationSeconds`) →
  `cyberscore_try.py::_live_elo_winner_lookup(..., with_duration=True)` →
  `ELO/live_team_strength.py::_build_live_applied_update(..., duration_seconds=)`; длительность пишется в
  `runtime/live_elo_progress.json`.
  - Если исход виден по сдвигу счёта серии, длительность берётся ТОЛЬКО из дискового кэша Stratz
    (`score_duration_lookup`, `cache_only=True`, `series_history(..., query=<без сети>)`). Живой цикл не ждёт сеть:
    `_post` перебирает все пары ключ↔прокси по 5 с. Кэш для живых команд каждые 30 с греет
    `start_background_refresh`.
  - Если исход по счёту не определить, как и раньше спрашивается Stratz по сети (`winner_lookup`), и длительность
    приходит вместе с исходом.
  - Если длительности нет, исход Stratz расходится со счётом или сторона не опознана, `m = 1.0`. Ночная пересборка
    каждый день пересчитывает всё по корпусу, поэтому живое отклонение держится не дольше суток. Долю живых
    обновлений без длительности видно по `applied_maps[*].duration_seconds` в `runtime/live_elo_progress.json`.

## Переключатель и отказоустойчивость
- `ELO_SERVED_COMPOSITION=a` (по умолчанию) или `k24` (откат через systemd drop-in и рестарт). При любом другом
  значении отдаётся K24, а в лог один раз пишется «Invalid ELO_SERVED_COMPOSITION=…; serving K24».
- В снимке нет A-состояния (старый снимок) → отдаётся K24, в лог один раз пишется
  «A composition state is missing or unavailable; serving K24».
- Telegram: «ELO состава (A):», source=`elo_composition_a`; при K24 прежние «ELO состава (K24)» и
  `elo_composition_k24`.
- Доступность A та же, что у K24: пять уникальных положительных account ID с каждой стороны, пустые имена команд
  и общий игрок у сторон дают `None`, запрос внутри покрытия откатывает обновления с `result_timestamp ≥ timestamp`.

## Порядок выката и перебазировка
- Сначала код, потом снимок: старый код отвергает A-снимок по сверке replay-версии.
- `rebase_runtime_model_state`:
  - принимаются переходы «прогресс только с K24 → A-снимок» и «A → A»;
  - снимок без A-состояния (собранный старым локальным кодом) ставится, K24 обновляется как раньше, A остаётся
    недоступным, и отдаётся K24;
  - перебазировка отказывает, если A было доступно в базе и стало недоступным при повторном проигрывании, а также
    если изменился контракт K24, пока работа не доделана.
- Ночная цепочка `scripts/run/rebuild_prematch_snapshot.sh` не менялась: шаг 5c собирает снимок уже с A,
  удалённый блок перебазирует его и перезапускает прод.

## Стоимость
Замер на 1 396 158 событиях и 1 150 370 аккаунтах: снимок +51,9 МБ, sidecar массивов +36,8 МБ, пик RSS при загрузке
sidecar +85 МБ. На serv1 25.09 было доступно 5,6 ГБ.

## Supplement maps
Карты OpenDota, отсутствующие в Stratz-корпусе, читаются из tracked-файла
`data/elo_supplement/opendota_maps.json` (Stratz-shaped dict-of-records, `source=opendota`).
Конвертер `scripts/pro_chain/build_elo_supplement.py --input-dir <dir>` читает массивы
`listing_*.json`/`players_*.json`; `--exclude-corpus-ids <processed_ids.txt>` исключает
уже собранные ID из JSON-массива (формат корпуса); поддерживаются также ID по одному
на строку. Требуются 10 разных неанонимных аккаунтов, по пять на сторону,
boolean winner и положительная длительность. На каждой стороне обязательны
положительный `radiant_team_id`/`dire_team_id` и непустое имя
`radiant_name`/`dire_name` из OpenDota `teams`. При отсутствии любого из них карта
пропускается и учитывается в счётчике конвертера `invalid_team`; пустые/пробельные
имена и placeholders `od-*` (без учёта регистра) отклоняются, имена не синтезируются.
Loader supplement повторяет проверку ID/имён, поэтому обход конвертера не создаёт
фиктивных строк рейтинга и A rank map. Парсер основного корпуса не меняется.
OpenDota tier (`professional`, `premium`, `excluded`, null) преобразуется в
Stratz tier (`PROFESSIONAL`, `PREMIUM`, `EXCLUDED`, null). Числовой `series_type`
0/1/2/3 преобразуется соответственно в `BEST_OF_ONE`/`BEST_OF_THREE`/
`BEST_OF_FIVE`/`BEST_OF_TWO`; неизвестное значение остаётся null.
Командные строки snapshot и hybrid state совпадают с эквивалентной Stratz-записью.
Корпус всегда выигрывает по `match_id` независимо от разницы timestamp; supplement
не обновляет остальные потребители pro-корпуса. Объединённый список сортируется по
`(timestamp, match_id)` и участвует в recent membership для перебазировки live ledger.
`build_snapshot` и `_build_snapshot_dict` принимают `supplement_dir=Path(...)`;
приоритет: keyword → `ELO_SUPPLEMENT_DIR` → `<repo>/data/elo_supplement`.
Пустой/отсутствующий каталог — побайтовый no-op относительно main, включая
`model_config_signature`: supplement-поля в `meta` не добавляются.
При наличии JSON-файлов supplement `meta.load_summary` содержит `supplement_loaded`,
`supplement_skipped_in_corpus`, `supplement_invalid` (отказы существующего parser),
`supplement_duplicate_records`, `supplement_skipped_files` (нечитаемый или битый JSON-файл целиком
пропускается с `logger.warning`, ночная сборка не падает) и `supplement_team_names_aligned`
(для `team_id`, который есть в корпусе, берётся имя с ближайшей по времени карты корпуса: последней
не позже старта, иначе первой после; иначе имя-ключ организации раскололось бы на два ряда). Откат: удалить/опустошить файл либо установить
`ELO_SUPPLEMENT_DIR=/nonexistent` и пересобрать snapshot; удаление требует разрешения.

Ночное обновление (с 2b2c951a, 09.10.2026, карточка ingame-kri1): `scripts/run/pro_nightly_chain.sh` между добором и
пересборкой запускает `scripts/pro_chain/update_elo_supplement.sh` (таймаут 1800 с, rc не блокирует пересборку,
итоговая строка дополняется `, ELO-добавка rc=N`). Обёртка вызывает `od_explorer_fetch.py --cache-dir
<дерево сборки>/runtime/artifacts/elo/opendota_supplement --corpus-ids <корпус>/processed_ids.txt` (листинги лиг по
полугодиям, перечитываются только периоды в окне `--refresh-days` 45; игроки — пачками ≤400 только для карт вне корпуса и
кэша; файл пачки `players_<первый>_<последний>_<n>_<sha256>.json` никогда не заменяется другим набором; карты без строк
игроков — в `noplayers_*.json`, повторно только в окне 45 дней; rc 3 = квота/HTTP/522, кэш сохраняется), затем
конвертер в `<дерево сборки>/data/elo_supplement/opendota_maps.json` (атомарно; при rc 3 конвертирует то, что в кэше).
`series_id` ≤0 публикуется как null: OpenDota отдаёт 0 у 3 969 и null у 210 из 75 719 карт, а `ELO/series_data.py:8-12`
склеил бы карты одной пары с номером 0. Перебазировка шага 8 читает только готовый снимок — копия в `/root/main` не нужна.
Откат одним флагом: `PRO_CHAIN_ELO_SUPPLEMENT=0` в окружении юнита `pro-chain-nightly` — шаг пропускается и
экспортируется `ELO_SUPPLEMENT_DIR=/nonexistent` (снимок без добавки, даже если файл прошлой ночи лежит).
Первая ночь: 63 запроса OpenDota (6 листингов + 57 пачек), дальше ~1–2 в ночь; квота 3000/сутки общая с продом.

## Проверки
- `ELO/tests/test_variant_a.py`: формула A на реальных картах (`ELO/tests/variant_a_real_maps_20260925.json`)
  против исследовательского харнесса.
- `base/tests/test_stratz_map_result_duration.py`: длительность из настоящего ответа Stratz
  (`base/tests/fixtures/stratz_series_history_9572001_20260925.json`) доходит до A-обновления.
- Сверка всего корпуса с исследовательским реплеем (T1):
  `runtime/experiments/elo/variant_a_prod_20260925/compare_served.py` (25.09.2026: 206 152 запроса, вне допуска 0, расхождение ≤ 3,4e-15; 10 карт без оценки в обоих кодах).
