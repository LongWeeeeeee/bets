"""
Sector-based prediction strategy.

Разбивает 100% coverage на секторы по:
1. Tier команд (T1vsT1, T1vsTX, T2vsT2, T3vsT3, Mixed)
2. Формат серии (short: Bo1/Bo2, long: Bo3+)
3. Player Elo gap (small: <30, medium: 30-60, large: >60)

Каждая комбинация имеет свою оптимальную стратегию предсказания.
"""

from typing import Dict, List, Optional, Tuple
from collections import defaultdict


# Оптимальные стратегии для каждой комбинации (sector, format, gap)
# Формат: (sector, is_short, gap_category) -> (signal, expected_accuracy)
# Оптимизировано на чистых данных (без tier3 турниров) - 30.11.2025
SECTOR_STRATEGIES = {
    # HIGH CONFIDENCE (65%+)
    ('T2vsT2', True, 'large'): ('team', 0.81),      # 81.2%
    ('T1vsT1', True, 'large'): ('team', 0.80),      # 80.0% (n=5)
    ('T1vsTX', True, 'medium'): ('player', 0.74),   # 74.1%
    ('Mixed', True, 'large'): ('team', 0.74),       # 73.5%
    ('T3vsT3', True, 'medium'): ('team', 0.71),     # 71.4% (n=7)
    ('T1vsTX', False, 'medium'): ('max', 0.70),     # 70.0%
    ('T2vsT2', True, 'medium'): ('player', 0.69),   # 69.4%
    ('T1vsTX', True, 'large'): ('team', 0.69),      # 68.8%
    ('Mixed', True, 'medium'): ('max', 0.68),       # 67.5%
    ('T1vsT1', False, 'small'): ('max', 0.65),      # 65.4%

    # MEDIUM CONFIDENCE (55-65%)
    ('Mixed', False, 'large'): ('team', 0.60),      # 60.0%
    ('T1vsTX', False, 'small'): ('max', 0.60),      # 60.0%
    ('T3vsT3', True, 'small'): ('max', 0.58),       # 58.3%
    ('T2vsT2', True, 'small'): ('player', 0.55),    # 55.1%
    ('T1vsT1', True, 'medium'): ('player', 0.59),   # 58.8%
    ('T1vsT1', True, 'small'): ('max', 0.59),       # 58.6%
    ('T1vsTX', True, 'small'): ('player', 0.56),    # 56.2%
    ('T1vsTX', False, 'large'): ('max', 0.54),      # use max (54.3%)
    ('T3vsT3', False, 'small'): ('max', 0.55),      # 55.0%

    # LOW CONFIDENCE (<55%) - optimized per combo
    ('Mixed', False, 'medium'): ('team', 0.54),     # team=53.5% (same as max)
    ('T1vsT1', False, 'medium'): ('team', 0.53),    # team=52.9%
    ('T2vsT2', False, 'small'): ('player', 0.51),   # player=51.2% > max=49.4%
    ('Mixed', True, 'small'): ('max', 0.55),        # 55.0%
    ('T3vsT3', False, 'medium'): ('max', 0.44),     # 44.4% (n=9)
    ('T2vsT2', False, 'large'): ('team', 0.40),     # team=40% > max=30%
    ('T1vsT1', False, 'large'): ('team', 0.38),     # team=38%
    ('Mixed', False, 'small'): ('max', 0.51),       # max=51.2%
    ('T2vsT2', False, 'medium'): ('team', 0.57),    # team=57.1% > max=54.8%
    ('T3vsT3', False, 'small'): ('team', 0.57),     # team=56.6% > max=49.5%
}


def get_team_tier(elo: float) -> str:
    """Определяет tier команды по её Elo."""
    if elo >= 1600:
        return 'tier1'
    elif elo >= 1500:
        return 'tier2'
    else:
        return 'tier3'


def get_sector(r_elo: float, d_elo: float) -> str:
    """Определяет сектор матча по Elo обеих команд."""
    r_tier = get_team_tier(r_elo)
    d_tier = get_team_tier(d_elo)
    
    if r_tier == 'tier1' and d_tier == 'tier1':
        return 'T1vsT1'
    elif r_tier == 'tier3' and d_tier == 'tier3':
        return 'T3vsT3'
    elif (r_tier == 'tier1' and d_tier != 'tier1') or (d_tier == 'tier1' and r_tier != 'tier1'):
        return 'T1vsTX'
    elif r_tier == 'tier2' and d_tier == 'tier2':
        return 'T2vsT2'
    else:
        return 'Mixed'


def get_gap_category(player_gap: float) -> str:
    """Определяет категорию gap по player elo."""
    if player_gap >= 60:
        return 'large'
    elif player_gap >= 30:
        return 'medium'
    else:
        return 'small'


def get_confidence_level(sector: str, is_short: bool, gap_category: str) -> str:
    """Возвращает уровень уверенности для комбинации."""
    key = (sector, is_short, gap_category)
    if key in SECTOR_STRATEGIES:
        _, acc = SECTOR_STRATEGIES[key]
        if acc >= 0.65:
            return 'high'
        elif acc >= 0.55:
            return 'medium'
    return 'low'


def predict_sector(
    team_elo_diff: float,
    player_elo_diff: float,
    max_player_diff: float,
    min_player_diff: float,
    r_elo: float,
    d_elo: float,
    n_maps: int,
) -> Tuple[bool, str, float]:
    """
    Предсказывает победителя на основе секторной стратегии.
    
    Returns:
        (prediction, confidence_level, expected_accuracy)
        - prediction: True = radiant wins, False = dire wins
        - confidence_level: 'high', 'medium', 'low'
        - expected_accuracy: ожидаемая точность для этой комбинации
    """
    sector = get_sector(r_elo, d_elo)
    is_short = n_maps <= 2
    player_gap = abs(player_elo_diff)
    gap_category = get_gap_category(player_gap)
    
    key = (sector, is_short, gap_category)
    
    if key in SECTOR_STRATEGIES:
        signal, expected_acc = SECTOR_STRATEGIES[key]
    else:
        # Fallback
        signal = 'weighted'
        expected_acc = 0.55
    
    # Применяем сигнал
    if signal == 'team':
        pred = team_elo_diff > 0
    elif signal == 'player':
        pred = player_elo_diff > 0
    elif signal == 'max':
        pred = max_player_diff > 0
    else:  # weighted
        score = (0.2 * team_elo_diff / 200 + 
                 0.6 * player_elo_diff / 100 + 
                 0.1 * min_player_diff / 100)
        pred = score > 0
    
    confidence = get_confidence_level(sector, is_short, gap_category)
    
    return pred, confidence, expected_acc


def get_sector_info(
    r_elo: float,
    d_elo: float,
    player_gap: float,
    n_maps: int,
) -> Dict:
    """
    Возвращает информацию о секторе для отладки/логирования.
    """
    sector = get_sector(r_elo, d_elo)
    is_short = n_maps <= 2
    gap_category = get_gap_category(player_gap)
    
    key = (sector, is_short, gap_category)
    signal, expected_acc = SECTOR_STRATEGIES.get(key, ('weighted', 0.55))
    confidence = get_confidence_level(sector, is_short, gap_category)
    
    return {
        'sector': sector,
        'format': 'short' if is_short else 'long',
        'gap_category': gap_category,
        'signal': signal,
        'expected_accuracy': expected_acc,
        'confidence': confidence,
    }


# High confidence combinations for filtering (65%+)
HIGH_CONFIDENCE_COMBOS = [
    ('T2vsT2', True, 'large'),   # 81%
    ('T1vsT1', True, 'large'),   # 80%
    ('T1vsTX', True, 'medium'),  # 74%
    ('Mixed', True, 'large'),    # 74%
    ('T3vsT3', True, 'medium'),  # 71%
    ('T1vsTX', False, 'medium'), # 70%
    ('T2vsT2', True, 'medium'),  # 69%
    ('T1vsTX', True, 'large'),   # 69%
    ('Mixed', True, 'medium'),   # 68%
    ('T1vsT1', False, 'small'),  # 65%
]


def is_high_confidence(
    r_elo: float,
    d_elo: float,
    player_gap: float,
    n_maps: int,
) -> bool:
    """Проверяет, попадает ли матч в высоко-уверенную комбинацию (65%+)."""
    sector = get_sector(r_elo, d_elo)
    is_short = n_maps <= 2
    gap_category = get_gap_category(player_gap)
    
    return (sector, is_short, gap_category) in HIGH_CONFIDENCE_COMBOS


def is_very_high_confidence(
    r_elo: float,
    d_elo: float,
    player_gap: float,
    max_gap: float,
    n_maps: int,
) -> bool:
    """
    Проверяет, попадает ли матч в очень высоко-уверенную комбинацию (70%+).
    
    Использует комбинацию sector + player_gap + max_gap для фильтрации.
    Coverage ~15%, accuracy ~72%.
    """
    # Базовое условие: high confidence combo + strong max_gap
    if is_high_confidence(r_elo, d_elo, player_gap, n_maps) and max_gap >= 50:
        return True
    return False


def get_betting_confidence(
    player_gap: float,
    max_gap: float,
) -> str:
    """
    Возвращает уровень уверенности для беттинга.
    
    Returns:
        'very_high': 70%+ accuracy, ~15% coverage
        'high': 65%+ accuracy, ~25% coverage  
        'medium': 55-65% accuracy
        'low': <55% accuracy
    """
    if player_gap >= 50 and max_gap >= 50:
        return 'very_high'  # 67% acc, 18% cov
    elif player_gap >= 40 and max_gap >= 30:
        return 'high'  # 65% acc, 28% cov
    elif player_gap >= 30 or max_gap >= 30:
        return 'medium'
    else:
        return 'low'
