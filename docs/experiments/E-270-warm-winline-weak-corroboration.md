---
id: E-270
title: "Тёплый снимок Winline + weak-corroboration анонимных пар (PlayTime–YS)"
date: "2026-09-10"
area: collection
status: full
corpus: "прогретые захваты Winline 10.09.2026 (текст 4–6k + DOM 520k), 18 живых GC-игр, DLTv series.json, 12 unit-тестов"
verdict: "внедрено: shell-snapshot отклоняется, weak-hint 3/5 едет в мост, карточка подтверждает (end-to-end доказан на живой игре 8991598414)"
harness: "base/tests/test_winline_first_admission.py + base/tests/test_player_attribution.py"
---

# E-270. Тёплый снимок Winline + weak-corroboration

- **Дата:** 2026-09-10 (запрос alex: live PlayTime–Yellow Submarine и
  Inner Circle–kalamychata есть на Winline — разбирать кэфы и драфт).
- **Вопрос:** почему обе пары не разбираются, хотя Winline их ведёт.
- **Замер эфира (10.09.2026):**
  - GC broad (16–18 игр): `None vs PlayTime` (mid 8991598414, лига 10877,
    radiant_team null, 11 игроков); IC–kalamychata на GC НЕТ вообще.
  - Прогретый снимок Winline: `DOTA 2 | BLAST Slam, Qualifier /
    YELLOW SUBMARINE PLAYTIME 1карта / Матч 2.60 1.40` и
    `INNER CIRCLE KALMYCHATA / Матч 1.30 3.00` — обе карточки live с кэфами.
  - Составы 8991598414: radiant YS 3/5 + VP.Prodigy 1 + Rune Eaters 1;
    dire Entity 1 + LGD 2 (устаревшие!) + PlayTime 1 + Midas 1.
  - cyberscore.live: обе фикстуры с живым драфтом (`YeS ... Draft ...
    PlayTime`, `IC x Insanity ... Draft ... Kalmychata`).
  - DLTv series.json: `playtime-vs-yellow-submarine-blast-slam-9-europe-
    open-qualifier-2` (live 427961, GC-mid 8991598414) и
    `inner-circle-vs-kalmychata-...` (upcoming 427962) — пара и лига
    подтверждены третьим источником; IC–kalam просто ещё не начался.
- **Корень №1 (блокер всего Winline-first): холодный снимок — shell.**
  Холодное чтение сразу после goto отдаёт только заголовок (149 символов,
  ни кнопок, ни карточек); refresh складировал любой непустой текст и тем
  самым ЗАТИРАЛ хороший снимок. Прогретая страница (30 с) даёт 4–6k текста
  и 520k DOM. В прод-логе ноль `лига Winline` — join не срабатывал НИ РАЗУ.
  Исправлено: предикат `_winline_overview_payload_looks_like_feed`
  (классы карточек из контракта парсера + длина), shell отклоняется
  (refresh False → штатный бэкофф цикла добирает на прогретой).
- **Корень №2 (flat-матчер слеп на соседях):** YS–PT и IC–kalam лежат в
  одном текстовом интервале под общим заголовком лиги — flat-контекст
  отдаёт обе и scope отклоняет (защита от чужих строк рынка, верная).
  Исправлено: join передаёт в `card_fn` DOM снимка (параметр `html`
  существовал, но никто его не передавал) — DOM делит карточки точно;
  prematch через DOM отклоняется корректно (проверено на живых
  RECRENT/DAXAK и GAMERLEGION/KlimSani4: future=True).
- **Корень №3 (мост):** probe ронял `None vs PlayTime` (тикет 10877:
  dire id 10020555 неизвестен — канонический PlayTime 10207983 tier2;
  хинт E-269 пуст, т.к. YS 3/5 < 4). Исправлено: WEAK-хинт (топ ≥3 при
  единственном лидере) едет в мост как транспорт
  (`_player_hint_admits_game`, только гейтовый тикет); downstream его
  одного недостаточно — пару обязана подтвердить карточка.
- **Корень №4 (league-фильтр):** даже с мостом `get_heads` ронял игру
  (нет известной стороны по id). Исправлено: `_winline_card_admits_league`
  — bypass при свежем hit + GC-лига ∈ allowlist-id/гейт + лига карточки
  вне title-denylist (пустая лига карточки = отказ, fail-closed).
- **Корень №5 (confirm):** хинт несёт нормализованный ключ, а матчер
  понимает написание (`Yellow Submarine` ≠ `yellowsubmarine`).
  Исправлено: поле `display` (мажоритарное сырое написание тега) идёт в
  join; weak-хинт подтверждает только в паре с точным именем второй
  стороны из моста (weak+weak и weak+hint без якоря — отказ).
- **Что НЕ трогаем (поправка 10.09.2026 в силе):** allowlist/denylist —
  жёсткие границы; Mad Dogs мёртв на bypass (denylist) и на downstream;
  переименований по тегам нет (E-267); тиры/онбординг не меняются
  (YS tier1, PlayTime tier2 — из справочника, не из хинта).
- **End-to-end доказательство (живые данные 10.09.2026):**
  `admits_game=True`, хинт `{yellowsubmarine weak 3/5 [2576071]}`,
  `hit=YES ('Yellow Submarine','PlayTime')`, лига `BLAST Slam, Qualifier`,
  `league_bypass=True`. Дальше — штатный путь: драфт с cyberscore.live по
  GC mid, кэфы опросом по подтверждённым именам.
- **IC–kalamychata:** на GC отсутствует (upcoming 427962) — разбирать
  не из чего; при появлении на GC покрывается этой же цепочкой, если у
  анонимной стороны будет ≥3 тегов. Переименование GC-сущностей
  (`IC x Insanity`, `YeS`) в словарях нет — отдельный вопрос алиасов.
- **Харнесс:** 4 теста теплоты (shell/feed предикат, guard refresh) +
  4 weak-теста probe (3/5 weak-only, ничья 3×3, шум 2/5, scope транспорта)
  + 4 confirm/bypass-теста (якорь, отказ без якоря, denylist/foreign,
  сквозной league-фильтр). `test_threshold_three_of_five_is_not_enough`
  обновлён: E-267 запрещал допуск/переименование по составу В ОДИНОЧКУ —
  это держится; weak едет только как evidence под карточку.
- **Запуск:** `venv_catboost/bin/python3 -m pytest
  base/tests/test_winline_first_admission.py
  base/tests/test_player_attribution.py base/tests/test_sourcetv_paths.py
  -q` (64 passed).
- **Где искать ошибку:** (1) редизайн классов Winline убьёт предикат
  ленты → обзор молча встанет (fail-open, смотреть
  `WINLINE_OVERVIEW_SHELL` в логе); (2) weak 3/5 + совпавшая карточка
  чужой пары при двух одновременных анонимных играх одной именованной
  стороны — ловят TTL хита 180 с и map/presence-гейты; (3) порог weak=3
  не перемерялся на корпусе E-267 (там мерился допуск в одиночку) —
  перемерить при первом ложном confirm.
- **Вывод:** внедрено. Откат: `WINLINE_FIRST_ENABLED=0`; точечно —
  weak-транспорт игнорируется удалением `elif _player_hint_admits_game`
  (две строки в probe), shell-guard — удалением предиката из refresh.
