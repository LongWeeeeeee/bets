# Ingame — Dota 2 Analytics Platform

Система для анализа и прогнозирования результатов Dota 2 матчей. Собирает live-данные, парсит букмекерские коэффициенты, использует ML-модели и ELO-рейтинги для предсказания исходов.

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

### Деплой на сервер

**Важно: если пользователь не откатывает изменения, то:**
1. Фиксируем изменения в git: `git add . && git commit -m "message" && git push origin main`
2. На сервере: `cd /root/main && git pull origin main`
3. Если сервер без git — используем rsync:
   ```bash
   rsync -avz --exclude='venv*' --exclude='__pycache__' --exclude='*.log' \
     ELO/ base/ root@212.113.104.102:/root/main/
   ```

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

### ML-модели
- **Ultimate Inference**: `src/live_predictor.py` — предсказание live-матчей
- **CatBoost**: используется для различных классификаций
- Данные в `base/ml_dataset/`

---

## Ключевые файлы

### `base/cyberscore_try.py` (750KB+)
Главный файл. Содержит:
- Live-парсинг OpenDota API и DLTV Selenium
- Краулинг букмекеров (Selenium)
- Telegram-нотификации
- ML-предиктор интеграция
- Runtime-менеджмент (lock-файлы, sharded stats)

**Важные функции:**
- `send_message()` — Telegram
- `drain_telegram_admin_commands()` — обработка команд
- `synergy_and_counterpick()` — расчёт синергии/контрпиков
- `calculate_comeback_solo_metrics()` — метрики камбеков

**Режимы запуска:**
- `--no-odds` — без букмекерского парсинга (только DLTV Selenium) — ОСНОВНОЙ РЕЖИМ
- `--odds` — с парсингом букмекерских коэффициентов
- `--bookmaker-gate-mode {odds,presence}` — режим gate для odds pipeline

**Переменные окружения:**
- `BOOKMAKER_PROXY_URL`, `BOOKMAKER_PROXY_POOL` — прокси для букмекеров
- `DLTV_PROXY_POOL` — прокси для OpenDota/DLTV
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_ADMIN_IDS`
- `DOTA2PROTRACKER_ENABLED=0` — отключить парсинг pro-tracker
- `DOTA2PROTRACKER_MIN_GAMES=10` — минимум игр для статистики

### `base/functions.py`
Общие хелперы. Импортируется в `cyberscore_try.py`.

### `base/dota2protracker.py`
Парсер hero matchups и synergies с dota2protracker.com. Использует Selenium для обхода Cloudflare.
- `enrich_with_pro_tracker()` — обогащает synergy_and_counterpick() результаты pro-level статистикой
- cp1vs1 и duo_synergy из pro-игр (от 10+ матчей)
- Кэширование в `hero_dota2protracker_data/`
- Переменная `DOTA2PROTRACKER_ENABLED=0` для отключения

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

- OpenDota API (free tier)
- DLTV Selenium (headless Chrome) — парсинг live-матчей
- Букмекерские сайты через Selenium (headless)
- Прокси-пулы для обхода лимитов

**Статус:** `LIVE_PREDICTOR_AVAILABLE = True` если `src/live_predictor.py` найден

---

## Правила для агента (из AGENTS.md)

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

**Адрес:** `root@212.113.104.102`

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
