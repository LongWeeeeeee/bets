# Объяснение логики предсказаний

## Финальные результаты (30.11.2025, CLEAN DATA + SECTOR STRATEGY)

```
Sector strategy: 58.2% accuracy, 100% coverage
Baseline:        56.5% accuracy
Improvement:     +1.7%

По формату серий:
- Short (Bo1/Bo2): 61.4% accuracy
- Long (Bo3+):     54.2% accuracy (baseline 50.7%)

Фильтры для беттинга:
- Very High Conf (15% cov): 71.8% accuracy ✓
- High Conf (25% cov):      69.2% accuracy ✓
- Very High Betting (18%):  67.0% accuracy
```

## Секторная стратегия предсказания

Разбиваем 100% coverage на секторы по:
1. **Tier команд** (по Team Elo): T1vsT1, T1vsTX, T2vsT2, T3vsT3, Mixed
2. **Формат серии**: short (Bo1/Bo2), long (Bo3+)
3. **Player Elo gap**: small (<30), medium (30-60), large (>60)

T1vsTX = Tier1 команда против Tier2 или Tier3 (фаворит против андердога)

```python
from core.sector_predictor import predict_sector, is_high_confidence

pred, confidence, expected_acc = predict_sector(
    team_elo_diff, player_elo_diff, max_player_diff, min_player_diff,
    r_elo, d_elo, n_maps
)
# confidence: 'high' (65%+), 'medium' (55-65%), 'low' (<55%)
```

### High Confidence комбинации (65%+)

| Комбинация | Accuracy | Signal |
|------------|----------|--------|
| T2vsT2/short/large | 81% | team_elo |
| T1vsT1/short/large | 80% | team_elo |
| T1vsTX/short/medium | 74% | player_elo |
| Mixed/short/large | 74% | team_elo |
| T1vsTX/long/medium | 70% | max_player |
| T2vsT2/short/medium | 69% | player_elo |
| T1vsTX/short/large | 69% | team_elo |
| Mixed/short/medium | 68% | max_player |
| T1vsT1/long/small | 65% | max_player |

### Определение Tier команды

```python
def get_team_tier(elo):
    if elo >= 1600: return 'tier1'
    elif elo >= 1500: return 'tier2'
    else: return 'tier3'
```

## Базовая стратегия (fallback)

```python
if series_format in ['bo1', 'bo2']:
    # Short series: используем max_player_diff
    prediction = max_player_diff > 0
else:
    # Long series (bo3+): используем weighted
    score = 0.2*team_elo_diff/200 + 0.6*player_elo_diff/100 + 0.1*min_player_diff/100
    prediction = score > 0
```

## Иерархия методов по accuracy (без data leak)

```
Short series (max_player_diff) →  68-70%  ← ЛУЧШИЙ для Bo1/Bo2
Long series (player_elo_diff)  →  59-61%  ← ЛУЧШИЙ для Bo3+
Team Elo only                  →  59-60%
H2H (на уровне серий)          →  не улучшает
Transitive                     →  не улучшает
```

**ВАЖНО:** Data leak исправлен! Все рейтинги вычисляются строго ДО момента предсказания.

## 0. Player Elo — Лучший предиктор

### Почему Player Elo работает лучше Team Elo?

Team Elo измеряет силу "бренда" команды, но составы меняются. Player Elo измеряет силу конкретных игроков.

**Ключевые сигналы:**
- `max_player_diff` — разница между лучшими игроками команд (лучший для Bo1/Bo2)
- `player_elo_diff` — разница средних Elo игроков (лучший для Bo3+)

### Формула Player Elo

```python
# Для каждого матча обновляем Elo всех 10 игроков
r_elo = avg(player_elo[p] for p in radiant_players)
d_elo = avg(player_elo[p] for p in dire_players)

expected_r = 1 / (1 + 10 ** ((d_elo - r_elo) / 400))
delta = K * (result - expected_r)

# Обновляем каждого игрока пропорционально
for p in radiant_players:
    player_elo[p] += delta / 5
```

### NO DATA LEAK — Как это работает

```python
# Team Elo: compute_elo_ratings_up_to(ts) 
# Использует только матчи с timestamp < ts (строго меньше)
if mt >= as_of_timestamp:
    break  # Не включаем матчи с ts >= as_of_timestamp

# Player Elo: get_player_elo_before(player_id, ts)
# История хранит (match_ts, elo_before_match)
# Возвращает последнее значение где match_ts <= ts
for mts, elo in history:
    if mts > ts:  # Строго больше
        break
    last_elo = elo
```

### Использование

```python
from core.player_elo_predictor import PlayerEloPredictor
from core.transitive_analyzer import TransitiveAnalyzer, ELO_HISTORY_DAYS, DEFAULT_ELO_PARAMS

analyzer = TransitiveAnalyzer()
predictor = PlayerEloPredictor(analyzer)

# Получаем данные
ts = series_timestamp
ratings = analyzer.compute_elo_ratings_up_to(ts, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS)
team_elo_diff = ratings.get(radiant_id, 1500) - ratings.get(dire_id, 1500)

r_elos = [predictor.get_player_elo_before(p, ts) for p in radiant_players]
d_elos = [predictor.get_player_elo_before(p, ts) for p in dire_players]

max_player_diff = max(r_elos) - max(d_elos)
player_elo_diff = sum(r_elos)/len(r_elos) - sum(d_elos)/len(d_elos)

# Предсказание
if n_maps <= 2:  # Bo1/Bo2
    prediction = max_player_diff > 0  # 68-70% accuracy
else:  # Bo3+
    prediction = player_elo_diff > 0  # 59-61% accuracy
```

## 1. H2H (Head-to-Head) — Второй по силе предиктор

### Почему H2H работает хорошо?

H2H — это **прямое измерение** силы команд друг против друга. Никаких промежуточных звеньев.

**Формула:**
```python
h2h_score = wins_team1 / total_series
# 0.0 = Team1 всегда проигрывает
# 0.5 = Ничья
# 1.0 = Team1 всегда выигрывает
```

### Decay по времени

Свежие матчи важнее старых:
```python
decay = 0.5 ** (days_ago / HALF_LIFE)  # HALF_LIFE = 14 дней
# 0 дней назад: decay = 1.0
# 14 дней назад: decay = 0.5
# 28 дней назад: decay = 0.25
```

### Margin учитывается

2:0 победа ценнее чем 2:1:
```python
series_value = margin * decay
# 2:0 → margin = 2
# 2:1 → margin = 1
```

### Почему 92% при 1-2 сериях?

Когда команды играли 1-2 раза недавно:
- Данные максимально свежие
- Нет "шума" от старых матчей
- Прямое сравнение без decay

## 2. Elo Rating — Базовый предиктор

### Как работает Elo

```python
expected = 1 / (1 + 10 ** ((elo_b - elo_a) / 400))
# elo_a = 1600, elo_b = 1400 → expected = 0.76 (76% шанс победы A)

new_elo_a = elo_a + K * (actual - expected)
# Победа: actual = 1
# Поражение: actual = 0
```

### Tier-факторы

Матчи на Major важнее квалификаций:
```python
K_effective = K_BASE * TIER_FACTOR
# Tier-1 (Major/TI): K * 3.5
# Tier-2 (DPC): K * 0.9
# Tier-3 (Quals): K * 0.6
```

### Проблема Tier-3 Inflation

Команда может "накачать" Elo играя только против слабых:
```
Most Wanted: 97 матчей, все tier-3, Elo = 1641
Gaimin Gladiators: 328 матчей, tier-1/2/3, Elo = 1575
```

**Решение:** K-фактор зависит от собственного Elo:
```python
K = K_BASE * max(0.3, 1 - (elo - 1500) / 1500)
# Elo 1500 → K = 100%
# Elo 1650 → K = 90%
# Elo 1800 → K = 80%
```

## 3. Common Opponents — Вспомогательный метод

### Когда Common полезен

**Полезен (68.75%):**
- Когда H2H = 0.5 (ничья между командами)
- Когда H2H отсутствует

**Бесполезен (56%):**
- Когда есть H2H данные
- Common не добавляет информации сверх H2H

### Почему Common работает слабо?

Winrate против общих противников **не транзитивен**:
```
Team A: 80% vs Team C
Team B: 40% vs Team C
→ A лучше B? НЕ ОБЯЗАТЕЛЬНО!

Причины:
- Разные патчи/меты
- Разные составы Team C
- Стилистические особенности (A хорош против C, но плох против B)
```

### Формула Common

```python
t1_winrate = t1_wins / t1_games  # против всех общих противников
t2_winrate = t2_wins / t2_games
common_score = 0.5 + (t1_winrate - t2_winrate) / 2
```

## 4. Transitive Chains — Слабый сигнал

### Почему Transitive работает плохо (62%)?

Транзитивность в спорте **не работает**:
```
A > B > C → A > C?  НЕ ОБЯЗАТЕЛЬНО!

Примеры из реальности:
- Rock > Scissors > Paper > Rock (цикл!)
- Стилистические matchups
- Форма команды меняется
```

### Когда Transitive полезен

Только когда **нет других данных**:
- Нет H2H
- Нет Common opponents
- Transitive даёт хоть какой-то сигнал (62% лучше чем 50%)

### Полные vs Неполные цепи

**Полная цепь (учитываем):**
```
A > B > C > D
- A играл с B (и выиграл)
- B играл с C (и выиграл)
- C играл с D (и выиграл)
→ A сильнее D (транзитивный вывод)
```

**Неполная цепь (пропускаем):**
```
A > B, C > D, B > D
- A играл с B (и выиграл)
- C играл с D (и выиграл)
- B играл с D (и выиграл)
- НО: A не играл с C или D
→ Нельзя сравнить A и C
```

## 5. Комбинирование методов

### Оптимальная стратегия

```python
def predict(team1, team2, timestamp):
    # 1. Всегда считаем Elo
    elo_diff = get_elo(team1, timestamp) - get_elo(team2, timestamp)
    
    # 2. Проверяем H2H
    h2h_score, h2h_n = get_h2h(team1, team2, timestamp)
    
    if h2h_n >= 1:
        # H2H есть — используем его с весом 0.7
        score = elo_diff / 100 + 0.7 * (h2h_score - 0.5) * 100
    else:
        # H2H нет — пробуем Common
        common_score, common_n = get_common(team1, team2, timestamp)
        if common_n >= 2:
            score = elo_diff / 100 + 0.3 * (common_score - 0.5) * 100
        else:
            # Только Elo
            score = elo_diff / 100
    
    return "Team1" if score > 0 else "Team2"
```

### Почему не комбинировать всё?

Эксперименты показали:
```
Elo + H2H(0.7)                    → 79.00%
Elo + H2H(0.7) + Common(0.2)      → 79.00%  (не улучшает!)
Elo + H2H(0.7) + Trans(0.1)       → 78.50%  (ухудшает!)
```

**Причина:** H2H уже содержит всю релевантную информацию. Добавление шумных сигналов (Common, Trans) только ухудшает.

## 6. Практические рекомендации

### Финальная стратегия (65% accuracy, 100% coverage)

```python
from core.player_elo_predictor import PlayerEloPredictor
from core.transitive_analyzer import TransitiveAnalyzer, ELO_HISTORY_DAYS, DEFAULT_ELO_PARAMS

analyzer = TransitiveAnalyzer()
predictor = PlayerEloPredictor(analyzer)

def predict_series(radiant_id, dire_id, radiant_players, dire_players, ts, n_maps):
    # Team Elo (NO DATA LEAK)
    ratings = analyzer.compute_elo_ratings_up_to(ts, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS)
    team_elo_diff = ratings.get(radiant_id, 1500) - ratings.get(dire_id, 1500)
    
    # Player Elo (NO DATA LEAK)
    r_elos = [predictor.get_player_elo_before(p, ts) for p in radiant_players if p]
    d_elos = [predictor.get_player_elo_before(p, ts) for p in dire_players if p]
    
    if len(r_elos) >= 3 and len(d_elos) >= 3:
        max_player_diff = max(r_elos) - max(d_elos)
        player_elo_diff = sum(r_elos)/len(r_elos) - sum(d_elos)/len(d_elos)
    else:
        max_player_diff = None
        player_elo_diff = None
    
    # Стратегия по формату
    if n_maps <= 2:  # Bo1/Bo2: 68-70% accuracy
        if max_player_diff is not None:
            return max_player_diff > 0
        return team_elo_diff > 0
    else:  # Bo3+: 59-61% accuracy
        if player_elo_diff is not None:
            return player_elo_diff > 0
        return team_elo_diff > 0
```

### Проверка на data leak

```bash
python3 backtest/final_test.py
```

### Что важно помнить

- ✅ Team Elo: `compute_elo_ratings_up_to(ts)` — только матчи с `timestamp < ts`
- ✅ Player Elo: `get_player_elo_before(p, ts)` — только матчи с `timestamp <= ts`
- ✅ Short series (Bo1/Bo2) более предсказуемы: 68-70%
- ✅ Long series (Bo3+) менее предсказуемы: 59-61%
- ✅ Coverage: 100% серий с победителем (ничьи исключены)

---

## 7. Улучшенная модель для Bo3+ (30.11.2025)

### Результаты

```
Bo1/Bo2: 67-70% accuracy (без изменений, max_player_diff)
Bo3+:    60.3% accuracy (было 59-61%, улучшение ~1%)

Breakdown по типу серий:
- Close (2-1, 3-2):     64.1% accuracy (72% всех Bo3+)
- Dominant (2-0, 3-0):  50.9% accuracy (28% всех Bo3+)
```

### Новая стратегия для Bo3+

```python
# Weighted combination вместо pure player_elo_diff
score = (0.2 * team_elo_diff / 200 + 
         0.6 * player_elo_diff / 100 + 
         0.1 * min_player_diff / 100)
prediction = score > 0
```

**Веса:**
- `team_elo_diff`: 0.2 (нормализация /200)
- `player_elo_diff`: 0.6 (нормализация /100) — основной сигнал
- `min_player_diff`: 0.1 (нормализация /100) — слабейший игрок

### Почему player_elo_diff лучше max_player_diff для Bo3+?

| Сигнал | Bo1/Bo2 | Bo3+ |
|--------|---------|------|
| max_player_diff | 68-70% ✓ | 53.5% ✗ |
| player_elo_diff | 64% | 59.1% ✓ |
| weighted | 65% | 60.3% ✓ |

**Объяснение:**
- Bo1/Bo2: один матч, звёздный игрок может "вытащить"
- Bo3+: серия матчей, важна глубина состава, а не только топ-игрок

### Что пробовали и НЕ работает для Bo3+

| Метод | Accuracy | Комментарий |
|-------|----------|-------------|
| H2H (head-to-head) | 55.6% | Ухудшает результат |
| Transitive chains | 48.1% | Хуже рандома |
| Common opponents | 0% coverage | Нет данных |
| Form (weighted_form) | 56.7% | Слабый сигнал |
| Streak | 54.2% | Слабый сигнал |
| Activity (days since last) | 51.3% | Почти рандом |
| Core vs Support roles | 57.4% / 54.1% | Не помогает |
| ML (logistic regression) | 52.8% | Переобучение |
| top2_diff, top3_diff, median_diff | 56-58% | Хуже avg |

### Ключевая проблема: Dominant серии

```
Close серии (2-1, 3-2):     64.1% — предсказуемы
Dominant серии (2-0, 3-0):  50.9% — почти рандом
```

**Почему dominant непредсказуемы:**
1. Часто это апсеты (андердог выигрывает уверенно)
2. Данные не отражают текущую форму/мету
3. Roster changes не учтены полностью

**Oracle (теоретический максимум):** 73% — если бы знали какой сигнал правильный для каждой серии

### Что нужно для 65%+ на Bo3+

Текущие данные (team Elo, player Elo) дают потолок ~60-62%. Для 65%+ нужны:

1. **Данные о драфте** — герои, пики/баны
2. **Данные о патчах** — мета меняется
3. **Букмекерские коэффициенты** — агрегированная информация рынка
4. **Более точные roster данные** — актуальные составы

### Использование новой модели

```python
from core.player_elo_predictor import PlayerEloPredictor
from core.transitive_analyzer import TransitiveAnalyzer, ELO_HISTORY_DAYS, DEFAULT_ELO_PARAMS

analyzer = TransitiveAnalyzer()
predictor = PlayerEloPredictor(analyzer)

def predict_bo3_plus(radiant_id, dire_id, radiant_players, dire_players, ts):
    # Team Elo
    ratings = analyzer.compute_elo_ratings_up_to(ts, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS)
    team_elo_diff = ratings.get(radiant_id, 1500) - ratings.get(dire_id, 1500)
    
    # Player Elo
    r_elos = [predictor.get_player_elo_before(p, ts) for p in radiant_players if p]
    d_elos = [predictor.get_player_elo_before(p, ts) for p in dire_players if p]
    
    if len(r_elos) >= 3 and len(d_elos) >= 3:
        player_elo_diff = sum(r_elos)/len(r_elos) - sum(d_elos)/len(d_elos)
        min_player_diff = min(r_elos) - min(d_elos)
        
        # Weighted score
        score = (0.2 * team_elo_diff / 200 + 
                 0.6 * player_elo_diff / 100 + 
                 0.1 * min_player_diff / 100)
        return score > 0
    
    # Fallback
    return team_elo_diff > 0
```

### Сравнение моделей

| Модель | Bo1/Bo2 | Bo3+ | Общий |
|--------|---------|------|-------|
| Старая (max_player_diff везде) | 68-70% | 53.5% | ~62% |
| Новая (max для short, weighted для long) | 67-70% | 60.3% | ~65% |
