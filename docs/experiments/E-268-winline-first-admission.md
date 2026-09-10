---
id: E-268
title: "Winline-first admission: живая лента Winline разбирается раньше sourcetv-гейтов"
date: "2026-09-09"
area: collection
status: full
corpus: "захваты Winline 01.08 + 05.08.2026, 9 unit-тестов"
verdict: "внедрено: winline-hit внутри allowlist-лиг допускает матч мимо team_id-гейта, иначе прежний путь"
harness: "base/tests/test_winline_first_admission.py"
---

# E-268. Winline-first admission

- **Дата:** 2026-09-09 (запрос alex: «сначала парсим winline лиги и матчи/команды,
  потом смотрим в матчи sourcetv», кейс MOUZ vs Klim Sani4 — не разбирались ни кэфы,
  ни сам матч).
- **Вопрос:** почему матч, который Winline ведёт в лайве, не доходит даже до опроса
  кэфов, и как поменять порядок «сначала матч, потом Winline» на обратный, не сломав
  отбор.
- **Механизм дропа (подтверждён чтением кода, не гипотеза):** порядок был
  SourceTV→гейты→Winline. До букмекера стояли: league-allowlist на этапе heads
  (`cyberscore_try.py`, ветка SourceTV — пропуск при неизвестной лиге без известной
  стороны), team_id-гейт (`not radiant_team_ids or not dire_team_ids` — требует id
  ОБЕИХ сторон) и league-denylist. У стека Klim Sani4 Valve не отдаёт team entity →
  dire ids пустые → возврат «Матч пропущен (нет team_id)» до `_bookmaker_prefetch_submit`.
- **Что сделано:** раз в цикл (`WINLINE_OVERVIEW_TTL_S=45`, фоновая daemon-нить, цикл
  general() страницу никогда не ждёт) снимается общий live-снимок ленты Winline
  (`collect_winline_live_overview_in_camoufox_page`, только чтение DOM на отдельной
  вкладке `bookmaker:winline-overview`, чтобы не сбивать вкладки поллера). Join пары
  SourceTV к снимку (`_winline_first_join`) — живой карточкой через
  `_winline_matched_card_context` + `_looks_future_context` (prematch-линия с
  «Завтра» отклоняется), плейсхолдеры Radiant/Dire не джойнятся.
- **Поправка 10.09.2026 (alex): league allowlist и league-denylist — ЖЁСТКИЕ
  границы, winline-first их не обходит** (на Winline есть карты, которые мы не
  хотим разбирать вовсе). Hit допускает матч ВНУТРИ allowlist-лиг только мимо
  team_id-гейта; неизвестная сторона остаётся неизвестной (id 0, tier 3 по
  правилам tier 2, без авто-онбординга — зеркало tier-3 allowlist). Лига из карточки
  (`winline_live_card_league`) — только для логов/display.
- **Дефект, найденный поправкой:** per-card bypass был мёртв с момента внедрения —
  штамп `admitted_at` ставил только удалённый heads-хелпер `_winline_first_maybe_admit`,
  а team_id-гейт получал hit из `_winline_first_join` без штампа, и
  `_winline_first_bypass_active` всегда возвращал False. Юнит-тесты смотрели только
  heads-путь и дефект не ловили. Исправлено: штамп ставит сам join; добавлен
  регрессионный тест `test_join_hit_activates_bypass_directly` (на hit без штампа
  даёт False — то есть до фикса был бы красным).
- **Харнесс:** `base/tests/test_winline_first_admission.py` (9 тестов).
- **Запуск:** `venv_catboost/bin/python3 -m pytest base/tests/test_winline_first_admission.py -q`
  (плюс соседние winline-сюиты: 88 passed; весь `base/tests/` — зелёный кроме
  предсуществующего `test_all_only_watcher_snapshot_uses_latest_elapsed_minute`,
  падающего и на чистом HEAD).
- **Дефекты, найденные ДО выводов:** (1) первая версия брала лигу из контекста
  карточки — но контекст по конвенции парсера начинается с блока команд, лига
  осталась за левой границей (тест поймал: `''` вместо `EPL Season`); исправлено
  взятием лиги из страницы между заголовком дисциплины и парой. (2) Килл-свитч и
  изоляция: снимок читается неблокирующе, поток стартует только из прод-пути
  SourceTV (тесты через шов инъекции поток не поднимают).
- **Результат:** red→green по новым тестам (первый прогон — AttributeError на
  отсутствующих именах; после реализации 9/9). Регрессий в winline-сюитах нет.
- **Контроль:** допуск только ДОБАВЛЯЕТ матчи (fail-open): нет снимка / нет пары /
  prematch-карточка / флаг `WINLINE_FIRST_ENABLED=0` / `PURE_DLTV_MODE` — везде
  прежний путь бит-в-бит (гейты получили лишь `and not _winline_first_bypass_active(...)`,
  join считается только на drop-path, уже допущенные матчи его не тратят).
- **Где искать ошибку:** (1) ложный join при коллизии имён (защита — те же
  `_text_matches_teams`/границы карточки, что у поллера, + отказ плейсхолдерам);
  (2) протухший снимок допускает кончившийся матч — TTL допуска 180 с, дальше map_id
  и presence/odds-гейты поллера работают как раньше; (3) лига best-effort: при
  коллизии токена команды с названием лиги (`1w Essence` vs `1w`) возвращается "" —
  как пустой league_name у probe; (4) выборка боевого эффекта пока нулевая — смотреть
  прод-логи по маркеру `🧭 Winline-first`.
- **Дефект №2 (найден на деплое 10.09.2026): допуск был dormant в проде.**
  `_winline_first_active` требовал включённый `BOOKMAKER_PREFETCH_ENABLED`,
  а прод идёт с `--no-odds` (`Odds pipeline: OFF`, проверено в логе юнита).
  При этом инпроцессный опрос Winline через общую Camoufox-сессию в `--no-odds`
  жив (177 строк `Winline polling active` в `cyberscore_sourcetv.log`) — обзор
  пользуется той же сессией. Исправлено: active() зависит от Camoufox-флагов,
  а не от prefetch; в поток обновления добавлен бэкофф consecutive-промахов
  (10·2^n с, потолок 300 с), чтобы без сессии не конкурировать с поллером.
  Регрессионный тест `test_prod_no_odds_shape_stays_active`.
- **Дефект №3 (10.09.2026, прод-инцидент): в коммит утекла чужая строка.**
  Селективный stage hunks по маркерам пропустил строку пира
  (`m.get("_gated_tier12_side")`, 4-й аргумент) внутри моего heads-hunk —
  в прод уехал 4-арг вызов при 3-арг дефе (парная правка пира не задеплоена),
  главный цикл падал с `takes 3 positional arguments but 4 were given` на
  каждом не-allowlist матче (доказано логом serv1). Урок: hunks, где чужое
  изменение сидит ВНУТРИ моего контекста, маркер-классификатор не ловит —
  проверять `git diff <base> HEAD` глазами построчно перед push. Исправлено
  удалением строки; добавлены тесты `test_non_allowlisted_league_filtered_without_crash`
  (запись вида Mad Dogs League → `([], [])` через настоящий call site) и
  статический tripwire `test_known_side_filter_call_matches_def_arity`
  (все вызовы укладываются в сигнатуру, без хардкода числа).
- **Вывод:** внедрено с поправками 10.09.2026. Откат: `WINLINE_FIRST_ENABLED=0`
  (или отсутствие снимка — поведение совпадёт со старым автоматически).
