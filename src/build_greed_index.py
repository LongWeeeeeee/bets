"""
Build Hero Greed Index based on farming patterns.

Greed Index = нормализованный GPM героя.
- Высокий GPM = фарм-зависимый герой (Spectre, AM) = поздний тайминг
- Низкий GPM = темповый герой (Support, Nuker) = ранний тайминг

Output: data/hero_greed_index.json
"""

import json
import logging
from pathlib import Path
from typing import Dict

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def build_greed_index(hero_stats_path: str = 'data/hero_public_stats.csv') -> Dict[int, Dict[str, float]]:
    """
    Строит Greed Index на основе GPM героев.
    
    Returns:
        {
            hero_id: {
                'gpm': 650.0,           # Средний GPM
                'greed_index': 0.85,    # Нормализованный (0-1)
                'timing_category': 'late'  # early/mid/late
            }
        }
    """
    df = pd.read_csv(hero_stats_path)
    
    logger.info(f"Loaded stats for {len(df)} heroes")
    
    # Нормализуем GPM к 0-1
    gpm_min = df['gpm'].min()
    gpm_max = df['gpm'].max()
    df['greed_index'] = (df['gpm'] - gpm_min) / (gpm_max - gpm_min)
    
    # Категории тайминга
    def get_timing_category(greed: float) -> str:
        if greed < 0.33:
            return 'early'  # Темповые герои, хотят драться рано
        elif greed < 0.66:
            return 'mid'    # Средний тайминг
        else:
            return 'late'   # Лейт-герои, хотят фармить
    
    df['timing_category'] = df['greed_index'].apply(get_timing_category)
    
    result: Dict[int, Dict[str, float]] = {}
    
    for _, row in df.iterrows():
        hero_id = int(row['hero_id'])
        result[hero_id] = {
            'gpm': float(row['gpm']),
            'greed_index': float(row['greed_index']),
            'timing_category': row['timing_category'],
            'aggression': float(row['aggression']),
            'pace': float(row['pace']),
        }
    
    return result


def main(
    hero_stats_path: str = 'data/hero_public_stats.csv',
    output_path: str = 'data/hero_greed_index.json'
) -> None:
    """Основной пайплайн."""
    
    greed_data = build_greed_index(hero_stats_path)
    
    logger.info(f"Built greed index for {len(greed_data)} heroes")
    
    # Статистика
    greed_values = [h['greed_index'] for h in greed_data.values()]
    timing_counts = {}
    for h in greed_data.values():
        cat = h['timing_category']
        timing_counts[cat] = timing_counts.get(cat, 0) + 1
    
    logger.info(f"Greed index range: {min(greed_values):.2f} - {max(greed_values):.2f}")
    logger.info(f"Timing distribution: {timing_counts}")
    
    # Топ-10 жадных героев
    top_greedy = sorted(greed_data.items(), key=lambda x: x[1]['greed_index'], reverse=True)[:10]
    logger.info("Top 10 greedy heroes (late game):")
    for hero_id, stats in top_greedy:
        logger.info(f"  Hero {hero_id}: greed={stats['greed_index']:.2f}, gpm={stats['gpm']:.0f}")
    
    # Топ-10 темповых героев
    top_tempo = sorted(greed_data.items(), key=lambda x: x[1]['greed_index'])[:10]
    logger.info("Top 10 tempo heroes (early game):")
    for hero_id, stats in top_tempo:
        logger.info(f"  Hero {hero_id}: greed={stats['greed_index']:.2f}, gpm={stats['gpm']:.0f}")
    
    # Сохраняем
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(greed_data, f, indent=2)
    
    logger.info(f"Saved to {output_path}")


if __name__ == '__main__':
    main()
