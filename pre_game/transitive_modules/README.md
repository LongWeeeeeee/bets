# Транзитивный Анализатор Dota 2

Модуль для предсказания победителя в матчах Dota 2 на основе Elo рейтинга команд, Elo рейтинга игроков, H2H (прямых матчей), общих противников и транзитивных связей.

## 🏆 Лучшие результаты (Player Elo Model)

### Все серии (без data leak)

| Tier | Accuracy | Coverage | EV/100 | Описание |
|------|----------|----------|--------|----------|
| Tier 1 | **71.6%** | 40.1% | 28.7 | max_player_diff >= 45 |
| Tiers 1-2 | **71.4%** | 50.8% | 36.3 | + max>=25 & all agree |
| Tiers 1-3 | 67.8% | 71.5% | 48.5 | + player & team agree |
| All | 64.3% | 100% | 64.3 | Все серии |

**Вывод:** Без data leak accuracy ниже (~71% vs ~79%), но это честный результат.

### Форматы серий в данных
- **1 карта (24%)**: Bo1 или неполные данные
- **2 карты (50%)**: Bo3 закончилась 2:0
- **3 карты (24%)**: Bo3 закончилась 2:1 или Bo5 3:0
- **4-5 карт (2%)**: Bo5

### Ключевые находки
- **Max Player Elo diff** — лучший предиктор (разница между лучшими игроками команд)
- Короткие серии (1-2 карты) значительно более предсказуемы
- **EV = 40.4** при accuracy 78.8% и coverage 51.3%
- Expected Value = accuracy × coverage × 100 (правильных на 100 серий)

### Data Leak проверка
- ✓ Player Elo использует только данные **ДО** серии
- ✓ `get_player_elo_before(ts)` возвращает Elo на момент до timestamp
- ⚠ Team Elo кэшируется для скорости (минимальный leak для старых серий)
- ⚠ **ВАЖНО:** Фильтрация по количеству карт (1-2 vs 3+) — это data leak! Мы не знаем заранее как закончится Bo3 (2:0 или 2:1). Правильный тест должен использовать **все** серии.

## Структура проекта

```
transitive_modules/
├── core/                           # Основной код
│   ├── transitive_analyzer.py      # Главный анализатор (Elo, H2H, Common, Transitive)
│   ├── player_elo_predictor.py     # 🆕 Player Elo модель (лучшие результаты!)
│   └── transitive_meta_model.py    # Runtime обёртка для ML модели
│
├── backtest/                       # Бэктесты и эксперименты
│   ├── backtest_full.py            # Полный бэктест на всех данных
│   ├── backtest_unified_sources.py # Сравнение разных сценариев
│   ├── best_results_benchmark.py   # Бенчмарк лучших результатов
│   ├── experiment_common_transitive.py
│   ├── experiment_final.py
│   ├── experiment_fresh_common_h2h.py
│   ├── optimize_weights.py
│   └── test_new_methods.py
│
├── ml/                             # Машинное обучение
│   ├── build_transitive_ml_dataset.py
│   ├── train_transitive_meta_model.py
│   └── grid_search_weights.py
│
├── utils/                          # Утилиты
│   └── build_elo_ranking_snapshot.py
│
├── data/                           # Данные и результаты
│   ├── *.csv                       # Датасеты
│   ├── *.pkl                       # Обученные модели
│   ├── *.json                      # Результаты grid search
│   └── *.txt                       # Результаты бэктестов
│
├── README.md                       # Этот файл
├── LOGIC_EXPLANATION.md            # Объяснение логики
└── CHANGELOG.md                    # История изменений
```

## Методы предсказания

### 0. 🆕 Player Elo (81% accuracy для Bo1/Bo2 Tier 1)

**Лучший метод!** Elo рейтинг на уровне игроков, а не команд.

**Ключевые сигналы:**
- `max_player_diff` — разница между лучшими игроками команд (самый сильный!)
- `player_elo_diff` — средняя разница Elo игроков
- `team_elo_diff` — классический Elo команд

**Уровни уверенности (expected_accuracy):**
```python
72%: max_player_diff >= 45           (coverage: 40%)
71%: max_player_diff >= 25 & all agree (coverage: 51%)
68%: player & team agree             (coverage: 72%)
64%: fallback to team Elo            (coverage: 100%)
```

**Использование:**
```python
from core.player_elo_predictor import PlayerEloPredictor

predictor = PlayerEloPredictor()
result = predictor.predict_series(
    radiant_team_id=123,
    dire_team_id=456,
    radiant_players=[111, 222, 333, 444, 555],
    dire_players=[666, 777, 888, 999, 1000],
    as_of_timestamp=1700000000,
)

print(f"Prediction: {'Radiant' if result['prediction'] else 'Dire'}")
print(f"Expected accuracy: {result['expected_accuracy']*100:.0f}%")
print(f"Coverage: {result['coverage']*100:.0f}%")
print(f"Max player diff: {result['signals']['max_player_diff']:.0f}")
```

### 1. Team Elo Rating (~60% accuracy)

Классический Elo рейтинг с модификациями:
- Динамический K-фактор (выше для новых команд)
- Tier-факторы для турниров разного уровня
- Сброс при смене ростера
- Upset-фактор для неожиданных результатов

**Параметры:**
```python
ELO_BASE_RATING = 1500.0
ELO_K_FACTOR = 32.0
ELO_SIGMOID_SCALE = 400.0
ELO_TIER1_FACTOR = 3.5      # Major/TI
ELO_TIER2_FACTOR = 0.9      # DPC
ELO_TIER3_FACTOR = 0.6      # Квалификации
ELO_HISTORY_DAYS = 365
```

### 2. H2H - Head-to-Head (~82-92% accuracy)

Прямые матчи между командами — самый надёжный предиктор.

**Особенности:**
- Decay по времени (half-life 14 дней)
- Учёт margin (2:0 > 2:1)
- Series value с весом по свежести
- **92.24% accuracy** когда есть 1-2 серии между командами

**Пример:**
```
Team A vs Team B: 3 победы, 1 поражение за 90 дней
→ H2H score = 0.75 (75% winrate)
→ Предсказание: Team A
```

### 3. Common Opponents (~56-68% accuracy)

Сравнение результатов против общих противников.

**Особенности:**
- Quality weighting по Elo противника
- Более агрессивный decay (7 дней)
- **68.75% accuracy** когда H2H = 0.5 (ничья)

**Пример:**
```
Team A vs Team C: 80% winrate
Team B vs Team C: 40% winrate
→ Common score в пользу Team A
```

### 4. Transitive Chains (~62% accuracy)

Транзитивные цепи: A > B > C > D.

**Особенности:**
- Propagation winrate через цепь
- Length decay (короткие цепи важнее)
- Учитываются только полные цепи

**Пример:**
```
Team A > Team C (2:0)
Team C > Team D (2:1)
Team D > Team B (2:0)
→ Цепь: A > C > D > B
→ Transitive score в пользу Team A
```

## Использование

### Быстрый бенчмарк

```bash
python3 backtest/best_results_benchmark.py
```

### Полный бэктест

```bash
python3 backtest/backtest_full.py
```

### API для предсказаний

#### 🆕 Рекомендуемый метод: `PlayerEloPredictor` (81% accuracy для Bo1/Bo2)

```python
from core.player_elo_predictor import PlayerEloPredictor

predictor = PlayerEloPredictor()

result = predictor.predict_series(
    radiant_team_id=7119388,  # Team Spirit
    dire_team_id=5,           # Opponent
    radiant_players=[111, 222, 333, 444, 555],  # account_ids
    dire_players=[666, 777, 888, 999, 1000],
    as_of_timestamp=match_start_ts,
    series_format='bo1',  # 'bo1', 'bo2', 'bo3'
)

print(f"Победитель: {'Radiant' if result['prediction'] else 'Dire'}")
print(f"Tier: {result['tier']}")
print(f"Expected accuracy: {result['expected_accuracy']*100:.0f}%")
print(f"Confidence: {result['confidence']:.2f}")
print(f"Signals: {result['signals']}")
```

#### Альтернативный метод: `predict_match_v2`

```python
from core.transitive_analyzer import TransitiveAnalyzer, predict_match_v2

analyzer = TransitiveAnalyzer()

result = predict_match_v2(
    radiant_team_id=7119388,  # Team Spirit
    dire_team_id=5,           # Opponent
    as_of_timestamp=match_start_ts,
    analyzer=analyzer,
)

print(f"Победитель: {result['winner']}")
print(f"Уверенность: {result['confidence']:.2f}")
print(f"Метод: {result['method']}")  # 'h2h' или 'elo'
print(f"H2H score: {result['h2h_score']}")
print(f"Elo diff: {result['elo_diff']:.1f}")
```

#### Альтернативный метод: `get_pre_draft_prior`

```python
from core.transitive_analyzer import get_pre_draft_prior

prior = get_pre_draft_prior(
    radiant_team_id=7119388,
    dire_team_id=5,
    as_of_timestamp=match_start_ts,
)

if prior['has_data']:
    print(f"Prior: {prior['winner']} (score={prior['score']:+.2f})")
```

## Возвращаемый формат

```python
{
    'winner': str,              # 'Radiant', 'Dire' или 'Ничья'
    'strength': float,          # 0.0 - 1.0 (уверенность)
    'confidence': float,        # 0.0 - 1.0 (доверие к данным)
    'total_score': float,       # Итоговый счет
    'h2h_score': float,         # Счет по H2H
    'common_score': float,      # Счет по Common
    'transitive_score': float,  # Счет по Transitive
    'methods_used': list,       # ['H2H', 'Common', 'Transitive']
    'has_data': bool,           # Достаточно ли данных
    'message': str              # Краткое сообщение
}
```

## Веса и пороги

### Веса источников
```python
WEIGHTS = {
    'head_to_head': 2.0,
    'common_opponents': 2.5,
    'transitive': 0.25,
    'elo': 1.0,
}
```

### Оптимальные веса (из экспериментов)
```python
H2H_WEIGHT = 0.7      # Лучший результат
COMMON_WEIGHT = 0.2   # Помогает когда H2H = 0.5
TRANS_WEIGHT = 0.0    # Не улучшает при наличии H2H
```

### Пороги
```python
STOP_THRESHOLDS = {
    'min_chains': 6,
    'strong_confidence': 9,
    'max_days': 30
}
```

## Результаты экспериментов

### 🆕 Player Elo Model (лучшие результаты)

| Формат | Tier | Accuracy | Coverage | EV/100 |
|--------|------|----------|----------|--------|
| Короткие (1-2 карты) | 1 | **78.7%** | 43.0% | 33.8 |
| Короткие (1-2 карты) | 1-2 | **78.8%** | 51.3% | **40.4** ✓ |
| Короткие (1-2 карты) | All | 69.7% | 100% | 69.7 |
| Длинные (3+ карт) | All | ~60% | 100% | ~60 |

### Validation по размеру выборки (старые методы)

| Выборка | Elo only | Elo + H2H | H2H only |
|---------|----------|-----------|----------|
| Last 100 | 69.00% | 79.00% | 82.00% |
| Last 200 | 68.69% | 79.00% | 82.00% |
| Last 300 | 65.10% | 78.00% | 77.33% |
| Last 500 | 61.24% | 77.00% | 79.80% |

### Когда использовать какой метод

- **Короткие серии (1-2 карты):** Player Elo Tier 1-2 → 78.8%, EV=40.4
- **Длинные серии (3+ карт):** Менее предсказуемы (~60%)
- **H2H:** Не улучшает accuracy на уровне серий (только на уровне матчей)
- **Нет Player Elo данных:** Fallback на Team Elo

## Известные проблемы

### Tier-3 Elo Inflation

Команды, играющие только в tier-3 турнирах, могут накопить высокий Elo без игр против топ-команд.

**Пример:** Most Wanted — 97 матчей, все против tier-3, Elo 1641 (выше Gaimin Gladiators).

**Решения (экспериментальные):**
- K-фактор зависит от собственного Elo
- Elo ceiling для команд без tier-1/2 игр
- Separate Elo pools

## Файлы данных

- `data/transitive_ml_dataset.csv` — датасет для ML
- `data/grid_search_results.json` — результаты grid search
- `data/backtest_results.txt` — результаты бэктестов

## Запуск тестов

```bash
# Бенчмарк лучших результатов
python3 backtest/best_results_benchmark.py

# Полный бэктест
python3 backtest/backtest_full.py

# Эксперименты с Common/Transitive
python3 backtest/experiment_common_transitive.py
```
