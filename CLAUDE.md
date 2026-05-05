# Ingame — Dota 2 Analytics Platform

Система для анализа и прогнозирования результатов Dota 2 матчей. Собирает live-данные, парсит букмекерские коэффициенты, использует ML-модели и ELO-рейтинги для предсказания исходов.

**Важно:** Hero ID героев — это OpenDota IDs. Hero ID используются в:
- `base/dota2protracker.py` — API dota2protracker.com использует те же OpenDota IDs
- `base/hero_features_processed.json` — справочник героев с ID (ключ = hero_id), единый источник правды для всех героев

Hero ID маппинг теперь динамически загружается из `base/hero_features_processed.json`:
- `get_hero_id(hero_name)` — получить ID по имени
- `get_hero_name(hero_id)` — получить имя по ID
- `get_hero_slug(hero_name)` — получить URL slug для dota2protracker.com

## Проект разработан для работы в связке с Telegram-ботом

Автор: alex | Дата: 2026-04-20

---

## Запуск на сервере

**ВАЖНО: Перед запуском процесса всегда проверять наличие уже запущенной версии и убивать предыдущий процесс:**

```bash
# Проверить запущенные процессы
ps aux | grep cyberscore
pgrep -f cyberscore

# Убить предыдущий процесс
pkill -f cyberscore
# или убить по PID
kill <PID>
```

После этого очистить map_id_check для переанализа матчей:
```bash
rm -f ~/.local/state/ingame/map_id_check.txt
```

**Важно: если пользователь не откатывает изменения, то:**
1. Фиксируем изменения в git: `git add . && git commit -m "message" && git push origin main`
2. На сервере: `cd /root/main && git pull origin main`
3. Если сервер без git — используем rsync:
   ```bash
   rsync -avz --exclude='venv*' --exclude='__pycache__' --exclude='*.log' \
     ELO/ base/ root@147.45.216.225:/root/main/
   ```
4. Очистить map_id_check: `rm -f ~/.local/state/ingame/map_id_check.txt`

**Важно для cyberscore pipeline:** после любых изменений live pipeline (`base/cyberscore_try.py`, `base/functions.py`, `base/dota2protracker.py`, букмекерский pipeline, Telegram/live dispatch) нужно запушить git-commit в `main`, подтянуть новый код на production-сервере, остановить старый `cyberscore` процесс, очистить `~/.local/state/ingame/map_id_check.txt` и перезапустить runtime уже с новым кодом.

Затем запустить:
```bash
cd /root/main
source venv/bin/activate
python3 base/cyberscore_try.py --no-odds
```

---

## Архитектура

```
├── base/                  # Основной код аналитики
│   ├── cyberscore_try.py  # ⭐ ГЛАВНЫЙ ФАЙЛ — live runtime, парсинг, API
│   ├── functions.py       # Хелперы: synergy, comeback metrics, вывод
│   ├── id_to_names.py     # Справочник команд (tier1/tier2 списки)
│   ├── maps_research.py   # Исследование карт
│   ├── check_old_maps.py  # Offline-сбор метрик по историческим картам
│   ├── metrics_winrate.py # Bucket winrate по JSON из check_old_maps.py
│   ├── analise_database.py
│   ├── explore_database.py
│   ├── bookmaker_selenium_odds.py  # Selenium-парсинг букмекеров
│   └── tests/             # Тесты интегриции
├── ELO/                   # ELO-система для прогноза победителя серии
│   ├── models.py
│   ├── config.py
│   ├── run_series_experiment.py
│   └── output/            # Артефакты экспериментов
├── data/                  # Кэш и данные
├── pro_heroes_data/       # Данные про-героев
├── bets_data/             # Логи ставок и анализа
└── runtime/              # Runtime-файлы (lock-файлы, состояние)
```

---

## Основные концепции

### Tier-классификация команд
- **tier1**: топ-команды (~60+ команд) — активный рейтинг, полный вес матчей
- **tier2**: полупро-команды — локальный рейтинг, меньший вес
- **tier3**: остальные — минимальный вес

Команды определены в `base/id_to_names.py`:
- `tier_one_teams` — dict {name: team_id}
- `tier_two_teams` — dict {name: team_id}

### Roster Lock
Сохранение roster lineage: если >=3 игроков совпадают с последним матчем org, состав продолжает ту же линию, иначе — новый segment.

### Star System
Метрики comeback-моделей с порогами `STAR_THRESHOLDS_BY_WR` (Win Rate based).

Текущий набор метрик, которые участвуют именно в STAR-сигнале:
- `counterpick_1vs1`;
- `dota2protracker_cp1vs1`;
- `counterpick_1vs2`;
- `solo`.

`synergy_duo` и `synergy_trio` могут выводиться в Telegram-блоках и пороговых таблицах, но сейчас не должны участвовать в принятии решения о STAR-сигнале.

### ML-модели
- **Ultimate Inference**: `src/live_predictor.py` — предсказание live-матчей
- **CatBoost**: используется для различных классификаций
- Данные в `base/ml_dataset/`

---

## Ключевые файлы

### `base/cyberscore_try.py` (750KB+)
Главный файл. Содержит:
- Live-парсинг DLTV (Camoufox-first, Selenium fallback)
- Краулинг букмекеров (Camoufox-first через subprocess, Selenium fallback)
- Telegram-нотификации
- ML-предиктор интеграция
- Runtime-менеджмент (lock-файлы, sharded stats)

**Важные функции:**
- `send_message()` — Telegram
- `drain_telegram_admin_commands()` — обработка команд
- `synergy_and_counterpick()` — расчёт синергии/контрпиков
- `_ensure_camoufox_browser()` — создаёт/переиспользует persistent Camoufox browser
- `_bookmaker_prefetch_fetch_camoufox_direct()` — direct Camoufox парсинг без subprocess

**Режимы запуска:**
- `--no-odds` — без букмекерского парсинга (только DLTV) — ОСНОВНОЙ РЕЖИМ
- `--odds` — с парсингом букмекерских коэффициентов
- `--bookmaker-gate-mode {odds,presence}` — режим gate для odds pipeline

**Режим minimal_odds_only:**
- `SIGNAL_MINIMAL_ODDS_ONLY_MODE=1` — отправляет только команды + счёт + кэфы
- Без dota2protracker, без словарей (cp1vs1, duo_synergy, LANING, LATE и т.д.)
- Букмекерские кэфы парсятся и добавляются в сообщение

**Переменные окружения:**
- `BOOKMAKER_PROXY_URL`, `BOOKMAKER_PROXY_POOL` — прокси для букмекеров
- `DLTV_PROXY_POOL` — прокси для DLTV
- При удалении нерабочих прокси в `base/keys.py` чистим только runtime proxy constants/pools; не удаляем и не меняем записи `api_to_proxy` / `api_to_keys` и связанные API-ключи
- `DLTV_CAMOUFOX_ENABLED=1` — включить Camoufox-first для DLTV HTML mode
- `BOOKMAKER_CAMOUFOX_ENABLED=1` — включить Camoufox для bookmaker (ОСНОВНОЙ РЕЖИМ)
- `SIGNAL_MINIMAL_ODDS_ONLY_MODE=1` — режим "только команды + счёт + кэфы"
- `DOTA2PROTRACKER_ENABLED=0` — отключить парсинг pro-tracker
- `BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS=10` — время ожидания кэфов перед отправкой

**Пример запуска локально:**
```bash
SIGNAL_MINIMAL_ODDS_ONLY_MODE=1 DOTA2PROTRACKER_ENABLED=0 \
BOOKMAKER_CAMOUFOX_ENABLED=1 BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS=10 \
/Users/alex/Documents/ingame/venv_catboost/bin/python3 \
/Users/alex/Documents/ingame/base/cyberscore_try.py --odds
```

**Пример запуска на сервере:**
```bash
SIGNAL_MINIMAL_ODDS_ONLY_MODE=1 DOTA2PROTRACKER_ENABLED=0 \
BOOKMAKER_CAMOUFOX_ENABLED=1 BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS=10 \
python3 base/cyberscore_try.py --odds
```

### `base/functions.py`
Общие хелперы. Импортируется в `cyberscore_try.py`.

### Offline-проверка draft-метрик

`base/check_old_maps.py` прогоняет исторические карты через те же draft-метрики, что используются в Telegram-сигнале:
- словари `early_dict_raw`, `late_dict_raw`, `lane_dict_raw`, `post_lane_dict_raw`;
- `dota2protracker` cp1vs1, synergy_duo и lane_advantage из локального hero cache;
- lane outcomes и early/late/post-lane outcomes из карты.

Публичные матчи по патчам лежат в `bets_data/analise_pub_matches/json_parts_split_from_object/`.
Для split-файлов можно передавать директорию и `--patch`; `--patch 7.41` автоматически использует start timestamp `1774310400`.

Пример 50k public backtest для patch 7.41:
```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 base/check_old_maps.py \
  --maps-path bets_data/analise_pub_matches/json_parts_split_from_object \
  --patch 7.41 \
  --max-matches 50000 \
  --dicts \
  --dota2protracker \
  --post-lane-max-cached-shards 127 \
  --output runtime/pub_7.41_50k_metrics.json
```

`--post-lane-max-cached-shards 127` полезен именно для offline backtest на больших public выборках: он не меняет live-логику, но убирает thrash sharded post-lane lookup.

После сборки JSON прогоняем bucket winrate:
```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 base/metrics_winrate.py \
  --input runtime/pub_7.41_50k_metrics.json \
  --bucket-mode \
  --min-matches 6 \
  > runtime/pub_7.41_50k_winrate.txt
```

Для pro-match проверки используется тот же pipeline, например `runtime/pro_maps_metrics_2025-12-15.json` -> `runtime/pro_maps_metrics_2025-12-15_winrate.txt`.

### `base/dota2protracker.py`
Парсер hero matchups и synergies с dota2protracker.com. Использует Selenium/Camoufox для обхода Cloudflare.

Hero ID в HERO_ID_MAP — это OpenDota IDs (совпадают с dota2protracker.com). Проверено по `base/hero_features_processed.json` и API `https://dota2protracker.com/api/heroes/list`.
- cp1vs1 и duo_synergy из pro-игр (от 10+ матчей)
- Кэширование в `hero_dota2protracker_data/`
- Переменная `DOTA2PROTRACKER_ENABLED=0` для отключения

**Текущее поведение интеграции:**
- `cyberscore_try.py` запускает `dota2protracker` enrichment сразу после успешного парса драфта
- при обычном режиме метрики выводятся отдельным Telegram-блоком:
  - `dota2protracker:`
  - `cp1vs1`
  - `synergy_duo`
- в `DOTA2PROTRACKER_ONLY_MODE=1` основной текст сигнала временно заменяется минимальным сообщением только с `dota2protracker`
- в `DOTA2PROTRACKER_ONLY_MODE=1` + `DOTA2PROTRACKER_BYPASS_GATES=1` сообщение отправляется на любой валидный драфт без обычных star/networth gate-веток

**Текущее поведение bookmaker Camoufox-интеграции:**
- `base/bookmaker_selenium_odds.py` умеет `presence`, `odds` и `deeplink` через Camoufox
- если `BOOKMAKER_CAMOUFOX_ENABLED=1`, CLI/subprocess-путь букмекеров не строит Selenium driver для odds/deeplink
- в `cyberscore_try.py` при `BOOKMAKER_CAMOUFOX_ENABLED=1` `BOOKMAKER_PREFETCH_USE_SUBPROCESS` по умолчанию включается автоматически
- перед фактической отправкой сообщения `cyberscore_try.py` делает повторный bookmaker subprocess-refresh, чтобы обновить кэфы ближе к dispatch, а не брать только ранний snapshot

**Важно про текущую математику `base/dota2protracker.py`:**
- `cp1vs1` и `duo_synergy` считаются только по exact position-pair данным
- старый fallback на агрегированные legacy `matchups/synergies` отключён, чтобы не подмешивать неверные позиции
- `cp1vs1_valid=True`, если у каждого из 6 core-героев (`radiant pos1/2/3`, `dire pos1/2/3`) есть минимум 2 валидных core-vs-core значения
- `synergy_duo` считается по всем 5 позициям, но валидность держится на coverage core-позиций

**Lane-specific метрики (lane_advantage):**

`calculate_lane_advantage()` считает cp1vs1 и duo_synergy для каждого лейна отдельно:

| Лейн | cp1vs1 matchups | duo synergy |
|------|-----------------|------------|
| **mid** | pos2 vs pos2 | нет |
| **top** | pos3 vs pos1, pos3 vs pos5, pos4 vs pos1, pos4 vs pos5 | radiant pos3+pos4 vs dire pos1+pos5 |
| **bot** | pos1 vs pos3, pos1 vs pos4, pos5 vs pos3, pos5 vs pos4 | radiant pos1+pos5 vs dire pos3+pos4 |

**Валидация:**
- cp1vs1: mid=1/1 matchup, top/bot=2/4 matchups (минимум 10 игр каждый)
- duo: требуется валидная синергия с обеих сторон (radiant + dire)

**Результат содержит:**
- `pro_lane_mid_cp1vs1`, `pro_lane_top_cp1vs1`, `pro_lane_bot_cp1vs1` — cp1vs1 по лейнам
- `pro_lane_top_duo`, `pro_lane_bot_duo` — duo synergy по лейнам
- `pro_lane_advantage` — усреднённое всех валидных cp1vs1 + duo значений
- Валидность: `pro_lane_mid_cp1vs1_valid`, `pro_lane_top_cp1vs1_valid`, `pro_lane_bot_cp1vs1_valid`, `pro_lane_top_duo_valid`, `pro_lane_bot_duo_valid`

**Математика matchup lookups:**
- `_get_matchup_1v1()` — берёт направление с большим количеством игр (forward или reverse)
- `_get_duo_synergy_pair()` — аналогично для duo synergy пар

### `base/id_to_names.py`
Справочник команд с динамическим onboarding (auto-added секции внизу файла).

### `ELO/run_series_experiment.py`
Запуск ELO-экспериментов:
```bash
source venv_catboost/bin/activate
python ELO/run_series_experiment.py
```

---

## Букмекеры и API

- DLTV Selenium (headless Chrome) — парсинг live-матчей
- Букмекерские сайты через Selenium (headless)
- Прокси-пулы для обхода лимитов

**Статус:** `LIVE_PREDICTOR_AVAILABLE = True` если `src/live_predictor.py` найден

---

## Правила для агента (из AGENTS.md)

- Агент не делает ничего своевольного сверх поставленной пользователем задачи или явных предписаний в проектных `.md` файлах; агент выполняет поставленную задачу и ничего больше
- Если запрос пользователя неоднозначен, агент не уверен в чём-то, или у пользователя есть логическая ошибка, агент сначала спрашивает пользователя
- Всегда используй `venv_catboost` как виртуальное окружение
- Активация: `source venv_catboost/bin/activate` из `/Users/alex/Documents/ingame`
- Не создавай и не используй другие виртуальные окружения
- **Не запускай и не перезапускай локальный cyberscore runtime** без явного запроса пользователя
- Изменения в `base/cyberscore_try.py` или логике live dispatch реализуй и тестируй без запуска локального runtime по умолчанию
- **При перезапуске процесса (локально или на сервере): очищать map_id_check** (`MAP_ID_CHECK_PATH`), чтобы матчи переанализировались заново
- **map_id_check единый для всех режимов** (--odds, --no-odds, presence и т.д.) — путь: `~/.local/state/ingame/map_id_check.txt`

---

## Тесты

```bash
pytest base/tests/ -v
```

Основные тесты:
- `test_pipeline_integrity.py`
- `test_networth_dispatch_gates.py`
- `test_tier_threshold_switch.py`
- `test_explore_database_integrity.py`

---

## Production сервер

**Адрес:** `root@147.45.216.225`

Основная площадка для запуска live runtime.

---

## Tips для агента

1. **Не читай весь cyberscore_try.py** — он 750KB+. Используй `grep` для поиска функций
2. **Букмекерский краулинг** — через Selenium, headless Chrome
3. **Proxy пулы** — настраиваются в `base/keys.py`
4. **Lock-файлы** — для предотвращения multiple instances
5. **Sharded stats** — оптимизация для больших lookup-таблиц

---

## Конфиденциальность

Этот проект содержит:
- API keys в `base/keys.py` (не коммитить!)
- `.env.example` — пример переменных окружения
- Приватные логи и данные ставок
