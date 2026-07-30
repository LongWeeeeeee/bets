# ELO for pre-draft Dota 2 winner prediction

Этот модуль теперь нужно читать как систему для прогноза победителя серии до ее старта. 
Внутри рейтинг все еще обучается по картам, потому что так больше сигналов, но целевая метрика и итоговый прогноз строятся на уровне серии.

Основные ограничения:

- матчи считаются только по данным до драфта;
- турниры режутся по `leagueId` и составу участников;
- `tier1` турнир: если `>= 60%` уникальных команд лиги входят в tier1 список из [base/id_to_names.py](/Users/alex/Documents/ingame/base/id_to_names.py);
- `tier2` турнир: если `>= 60%` уникальных команд входят в объединение `tier1 U tier2`;
- иначе используется fallback `TIER3`;
- roster lock: если у команды хотя бы `3` игрока совпадают с последним матчем org, продолжается тот же roster lineage, иначе создается новый roster segment.

## Что внутри

- `simple_team_elo`: baseline без roster lock и без player-level разложения.
- `hybrid_player_roster_elo`: основная модель.
- `tune_hybrid.py`: небольшой grid search для подбора гиперпараметров на вашем датасете.
- `run_series_experiment.py`: основной entrypoint для задачи прогноза победителя серии.
- `tune_series_hybrid.py`: grid search уже под series-level метрику.

Гибридная модель использует три слоя:

1. `player_global_rating`
2. `player_local_rating[tier]`
3. `roster_rating[tier]`

Сила команды перед картой:

- базовая часть = средний рейтинг игроков;
- tier-часть = отдельный локальный рейтинг игроков в текущем tier;
- roster-часть = рейтинг конкретного roster segment, который растет только если состав сохраняет continuity.

Как получается прогноз серии:

- до старта серии считается вероятность победы на карте 1 по pre-match рейтингам;
- затем эта вероятность переводится в вероятность выиграть всю серию `BO1/BO3/BO5`;
- рейтинги после этого обновляются по сыгранным картам серии, чтобы сохранить плотность обучающего сигнала;
- если `BO3` заканчивается со счетом `2:0`, модель получает небольшой post-series bonus (`bo3_sweep_bonus_weight=0.05`) по pre-series ошибке, чтобы немного сильнее поощрять clean sweep;
- `BO2` с потенциальной ничьей не используются как target на winner prediction, но их карты продолжают обучать рейтинг.

Анти-гринд логика:

- матчи `TIER2/TIER3` сильно меньше двигают `global` компонент;
- низкий tier в основном двигает локальный `tier`-рейтинг, а не глобальную силу;
- roster rating тоже локален по tier, поэтому tier2 состав не переносит нагринженную синергию прямо в tier1;
- поэтому команда не должна "нагриндить" себе силу tier1 только количеством матчей против слабого пула.

## Запуск

Из корня репозитория:

```bash
source venv_catboost/bin/activate
python ELO/run_series_experiment.py
```

Свою папку для артефактов можно передать так:

```bash
source venv_catboost/bin/activate
python ELO/run_experiment.py --output-dir ELO/output/my_run
```

## Выходные файлы

- `ELO/output/.../report.json` - итоговые метрики и конфиги.
- `ELO/output/.../league_diagnostics.json` - как были классифицированы лиги по `leagueId`.
- `ELO/output/hybrid_tuning.json` - результаты поиска по гиперпараметрам из `tune_hybrid.py`.
- `ELO/output/series_hybrid_tuning.json` - результаты поиска по гиперпараметрам уже для winner-of-series.
