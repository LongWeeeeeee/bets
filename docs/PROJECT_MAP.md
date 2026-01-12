# Карта проекта (entrypoints, зависимости, IO)

## Entry points (скрипты с `__main__`)
- `src/process_pro_data.py` — основной пайплайн обогащения про-матчей (JSON → CSV).
- `src/live_predictor.py` — лайв-предсказания; содержит тестовый блок.
- `src/analyze_winrate_bet.py` — анализ логов ставок.
- `base/cyberscore_try.py`, `base/maps_research.py` — ресерч/утилиты.
- `ml-models/train_live_models_catboost.py`, `ml-models/train_extreme_classifiers.py`, `ml-models/train_winrate_classifier_v3.py`, `ml-models/train_winrate_classifier_v4.py`, `ml-models/train_winrate_ablation_search.py` — обучение моделей.
- `ml-models/tests/*.py` — тестовые/демо-скрипты моделей.
- `tools/fetch_heroes.py`, `tools/fetch_stratz_data.py`, `tools/fetch_stratz_matchups.py` — загрузка данных Stratz/героев.
- `pre_game/transitive_modules/...` (backtest, ml, utils) — скрипты транзитивного модуля (бэктесты, датасет, грид-серч).
- `pro_heroes_data/json_parts_split_from_object/merge.py`, `pro_heroes_data/pro_elo_rebuild.py` — сборка/elo.
- `bets_data/.../split_stats_databases.py` — разбиение датасетов.

## Зависимости модулей (укрупнённо)
- `src/*.py` → активно используют `pandas`, `numpy`, `tqdm`; данные из `data/` и словари; часть функций/констант из `base/*` и `src/utils/*`.
  - `process_pro_data.py` импортирует `base.id_to_names` (тиеры), кладёт пути через `Path`, читает JSON/CSV из `data/` и `pro_heroes_data/`.
  - `live_predictor.py` опирается на `src/utils/map_teams_v2` (если есть), `catboost`, `xgboost`, и загружает JSON/CSV из `data/` и модели из `ml-models/`.
  - `build_*.py` (synergy, lanes, greed, power_spikes и пр.) используют `pandas/numpy`, читают исходные CSV/JSON из `data/`/`bets_data/`, пишут в `data/`.
- `base/*.py` → вспомогательные функции/исследования; `functions.py` использует `requests`, `BeautifulSoup`, `keys.py` (API-ключи).
- `ml-models/train_*.py` → зависят от `pandas`, `sklearn`, `catboost`/`xgboost`/`lightgbm`; читают подготовленные фичи (`data/pro_matches_enriched.csv`, другие CSV), сохраняют модели/мета в `ml-models/`.
- `tools/fetch_*` → `aiohttp`/`requests`, пишут JSON/CSV в `data/`.
- `pre_game/transitive_modules/*` → свои утилиты, используют `pandas/numpy`, локальные модули в папке.

## Таблица скриптов (I/O и зависимости)
| Script | Inputs (чтение) | Outputs (запись) | Depends on |
| --- | --- | --- | --- |
| `src/process_pro_data.py` | `pro_heroes_data/json_parts_split_from_object/clean_data.json` (про-матчи), `data/hero_public_stats.csv`, словари/фичи из `data/*` (power_spikes, hero stats, lanes, synergy, cc, greed и др.) | `data/pro_matches_enriched.csv` | `base.id_to_names`, `pandas`, `numpy`, `tqdm` |
| `src/live_predictor.py` | Модели из `ml-models/*.cbm/*.txt/*.json`, фичи из `data/*` (hero_features_processed.json, power_spikes, synergy, lane_matchups и др.) | Нет явной записи (возврат предсказаний) | `catboost`/`xgboost`, `src/utils/map_teams_v2` (если доступен), `pandas`, `numpy` |
| `src/analyze_winrate_bet.py` | `logs/winrate_bets.jsonl`, `data/pro_matches_enriched.csv` | `reports/winrate_bets_report_*.json`, `reports/*.csv` | `pandas`, stdlib |
| `src/build_*.py` (blood_stats, complex_stats, draft_execution, early_late_counters, greed_index, hero_* stats, lanes, player_dna, synergy_matrix, wave_clear, etc.) | Разные сырые таблицы из `data/`, `bets_data/`, `pro_heroes_data/` | Соответствующие агрегаты/статистики в `data/` (JSON/CSV) | `pandas`, `numpy`, локальные утилиты |
| `src/debug_feature_impact.py` | Готовые фичи/модели (из `data/`, `ml-models/`) | Аналитические выводы (stdout/отчёты) | `pandas`, `sklearn` |
| `src/rebuild_pub_stats.py` | Паб-статистика из `bets_data/` | Обновлённые pub stats в `data/` | `pandas`, `numpy` |
| `base/cyberscore_try.py` | Данные матчей (вероятно API/локальные файлы) | Логи/выводы (stdout/файлы) | `base/functions.py`, `keys.py`, `requests/bs4` |
| `base/maps_research.py` | Карты/матчи из `data/` | Исследовательские выводы | `pandas`, `numpy` |
| `ml-models/train_live_models_catboost.py` | Подготовленные датасеты (`data/pro_matches_enriched.csv`, мета-файлы) | Модели `ml-models/live_cb_*.cbm`, `live_cb_meta.json`, фич-листы | `catboost`, `pandas`, `numpy` |
| `ml-models/train_extreme_classifiers.py` | Фичи/мета (`data/pro_matches_enriched.csv`, `selected_features.json`) | `ml-models/extreme_*` модели/мета | `catboost`, `pandas`, `numpy` |
| `ml-models/train_winrate_classifier_v3.py`, `v4.py` | `data/pro_matches_enriched.csv`, мета-файлы | `winrate_classifier_v*.cbm`, `winrate_classifier_v*_meta.json` | `catboost`, `pandas`, `numpy` |
| `ml-models/train_winrate_ablation_search.py` | Те же входы + вариации фич | Логи/лучшие модели в `ml-models/` | `catboost`, `pandas`, `numpy` |
| `ml-models/tests/*.py` | Модели/фичи из `ml-models/`, `data/pro_matches_enriched.csv` | Отчёты/stdout | `pandas`, `catboost` |
| `tools/fetch_heroes.py` | API Stratz/Dota (сеть) | `data/heroes.json` | `requests`/`aiohttp` (по коду), `keys.py` |
| `tools/fetch_stratz_data.py` | API Stratz (сеть) | JSON/CSV под `data/` | `aiohttp`, `orjson`, `keys.py` |
| `tools/fetch_stratz_matchups.py` | API Stratz (сеть) | Матчапы в `data/` | `aiohttp`, `pandas`, `keys.py` |
| `pre_game/transitive_modules/ml/build_transitive_ml_dataset.py` | Исторические матчи/elo (локальные файлы) | Датасет для транзитивной модели | `pandas`, локальные utils |
| `pre_game/transitive_modules/ml/train_transitive_meta_model.py`, `grid_search_weights.py` | Транзитивный датасет | Обученные веса/модели (локально) | `pandas`, `sklearn` |
| `pre_game/transitive_modules/backtest/*.py` | Elo/датасеты транзитивных матчей | Метрики/лог (stdout/файлы) | `pandas`, локальные utils |
| `pre_game/transitive_modules/utils/build_elo_ranking_snapshot.py` | История матчей | `elo_snapshot.csv` (или аналог) | `pandas`, локальные utils |
| `pro_heroes_data/json_parts_split_from_object/merge.py` | Части JSON матчей | Собранный `clean_data.json` | stdlib |
| `pro_heroes_data/pro_elo_rebuild.py` | Исторические матчи | Elo-таблицы (CSV/JSON) | `pandas`, `numpy` |
| `bets_data/.../split_stats_databases.py` | Исходные базы ставок | Разделённые базы (CSV/JSON) | `pandas`, `numpy` |

> Примечание: для отдельных build_*.py входы/выходы зависят от конкретной метрики; большинство читают исходные таблицы из `data/`/`bets_data/` и пишут агрегаты туда же.

## Схема связей (текстово)
- Загрузка сырых данных → `tools/fetch_*` → `data/` (heroes, матчапы, stratz dumps).
- Подготовка/обогащение → `src/process_pro_data.py` + `src/build_*.py` → enriched CSV/JSON в `data/`.
- Обучение моделей → `ml-models/train_*.py` берут подготовленные фичи из `data/`, пишут модели/мета в `ml-models/`.
- Инференс (live) → `src/live_predictor.py` берёт модели из `ml-models/` и фичи/словари из `data/`.
- Аналитика/отчёты → `src/analyze_winrate_bet.py`, `ml-models/tests/*.py`, `pre_game/transitive_modules/*` читают готовые данные/модели, пишут отчёты/метрики.
