# Changelog - Транзитивный Анализатор

## v5 (2025-11-28) - Исправление бенчмарка

### Исправлено

- **ВАЖНО:** Предыдущие результаты 90%+ были на уровне МАТЧЕЙ, а не СЕРИЙ
- Матчи внутри одной серии имеют H2H друг с другом (data leak!)
- Правильный бенчмарк — на уровне СЕРИЙ

### Реальные результаты (на уровне серий)

| Метрика | Accuracy | Coverage |
|---------|----------|----------|
| Elo only | 60.8% | 100% |
| Strong H2H else Elo | **62.1%** | 100% |
| H2H only | 58-59% | ~50% |

### Accuracy по Elo diff

| Elo diff | Accuracy |
|----------|----------|
| <50 | 57.6% |
| 50-100 | 66.9% |
| 100-150 | 68.5% |
| 150+ | ~100% |

### Вывод

- Для 80%+ accuracy нужен фильтр по Elo diff >= 100
- Но coverage падает до ~20%
- При 100% coverage лучший результат: **62.1%**

---

## v4 (2025-11-28) - Benchmark и оптимизация

### Новое

- **best_results_benchmark.py** — единый скрипт для воспроизведения всех лучших результатов
- Документированы лучшие accuracy для каждой метрики

### Удалено

- Удалены промежуточные debug скрипты
- Удалены промежуточные эксперименты

### Обновлено

- README.md — полная документация с результатами бенчмарков
- LOGIC_EXPLANATION.md — объяснение почему H2H работает лучше всего

---

## v3 (2025-11-18) - Pre-draft API и unified backtest

### Новое

- `get_pre_draft_prior()` — упрощённый API для пре-драфт анализа
- `backtest_unified_sources.py` — сравнение сценариев (primary, combined, trans_only)
- Флаг `verbose` в `get_transitiv` для отключения stdout

### Параметры

```python
WEIGHTS = {
    'head_to_head': 2.0,
    'common_opponents': 2.5,
    'transitive': 0.25,
    'elo': 1.0
}

CHAIN_WEIGHTS = {
    'h2h_chain': 1.0,
    'common_chain': 1.0,
    'trans_chain': 0.3
}

STOP_THRESHOLDS = {
    'min_chains': 6,
    'strong_confidence': 9,
    'max_days': 30
}
```

### Результаты бэктеста (400 матчей, max_days=30)

- Primary (H2H+Common+Elo): coverage 70.5%, accuracy 56.0%
- Combined (default weights): coverage 71.2%, accuracy 55.8%
- Transitive only (k≥3): coverage 62.7%, accuracy 58.2%
- Transitive only (k≥4): coverage 58.8%, accuracy 58.7%

---

## v2 (2025-11-13) - Исправление логики транзитивных цепей

### Проблема

Частичные цепи давали неправильные результаты:
```
Hryvnia > Pipsqueak+4 > Team Lynx < Inner Circle
→ +0.5 для Hryvnia (НЕПРАВИЛЬНО!)
```

Проблема: нет связи между Pipsqueak+4 и Inner Circle.

### Решение

Учитываются только **полные транзитивные цепи**:
```
Dire > D > C > Radiant (полная цепь)
→ +1.0 для Dire (ПРАВИЛЬНО)
```

### Добавлено

- Параметры `radiant_team_name_original`, `dire_team_name_original`
- Улучшенный explanation с оригинальными именами команд

---

## v1 (2025-11-13) - Исправление логики подсчета

### Проблема

```python
# БЫЛО (неправильно):
total_score = h2h_score - common_score - transitive_score

# Radiant побеждает 3 общих противников
# common_score = +4.0
# total_score = 0 - 4.0 = -4.0 → Dire сильнее (НЕПРАВИЛЬНО!)
```

### Решение

```python
# СТАЛО (правильно):
total_score = h2h_score + common_score + transitive_score

# Radiant побеждает 3 общих противников
# common_score = +4.0
# total_score = 0 + 4.0 = +4.0 → Radiant сильнее (ПРАВИЛЬНО!)
```

### Добавлено

- Полная логика транзитивного анализа
- Формат вывода: `winner`, `strength`, `confidence`, `methods_used`
- Увеличен период поиска: 7 → 30 дней
- Детальное логирование каждой цепи

---

## Ключевые выводы из экспериментов

### H2H — главный предиктор

| Условие | Accuracy |
|---------|----------|
| H2H 1-2 серии | 92.24% |
| H2H все | 82.00% |
| Elo + H2H(0.7) | 79.00% |

### Common полезен только при H2H = 0.5

| Условие | Accuracy |
|---------|----------|
| Common when H2H=0.5 | 68.75% |
| Common only | 56.28% |

### Transitive — слабый сигнал

| Условие | Accuracy |
|---------|----------|
| Transitive only | 62.21% |
| Добавление к H2H | Ухудшает! |

### Оптимальная стратегия

```python
if has_h2h:
    return elo + h2h * 0.7
elif h2h_score == 0.5:
    return elo + common * 0.3
else:
    return elo
```
