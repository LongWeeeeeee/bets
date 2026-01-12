"""
Player DNA Profiling: Создание "паспорта стиля" для каждого игрока.

Для каждого account_id (с >= 10 играми) вычисляем:
- avg_personal_kills: Средние киллы за игру
- avg_personal_deaths: Средние смерти за игру
- avg_personal_assists: Средние ассисты за игру
- avg_match_duration: Средняя длительность его матчей (мин)
- aggression_score: (Kills + Assists) / Duration
- versatility: Количество уникальных героев
- pace_score: Kills / Duration (киллы в минуту)
- feed_score: Deaths / Duration (смерти в минуту)

ВАЖНО: Используем только данные ДО текущего матча (no leakage).
"""

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MIN_GAMES = 10  # Минимум игр для создания DNA профиля


def build_player_dna(data_path: str = 'data/pro_matches_enriched.csv') -> Dict[str, Dict[str, float]]:
    """
    Строит DNA профили для всех игроков на основе их исторических данных.
    
    Returns:
        Dict[account_id -> DNA profile]
    """
    df = pd.read_csv(data_path)
    df = df.sort_values('match_id').reset_index(drop=True)
    
    logger.info(f"Loaded {len(df)} matches")
    
    # Собираем статистику по игрокам
    player_stats: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    
    for _, row in df.iterrows():
        match_id = row['match_id']
        duration_min = row.get('duration_min', 35)  # default 35 min
        
        # Обрабатываем всех 10 игроков
        for team in ['radiant', 'dire']:
            for pos in range(1, 6):
                account_col = f'{team}_player_{pos}_id'
                kills_col = f'{team}_player_{pos}_kills'
                deaths_col = f'{team}_player_{pos}_deaths'
                assists_col = f'{team}_player_{pos}_assists'
                hero_col = f'{team}_hero_{pos}'
                
                if account_col not in row or pd.isna(row[account_col]):
                    continue
                
                account_id = str(int(row[account_col]))
                
                # Пропускаем анонимных игроков
                if account_id == '0' or account_id == 'nan':
                    continue
                
                kills = row.get(kills_col, 0) if pd.notna(row.get(kills_col)) else 0
                deaths = row.get(deaths_col, 0) if pd.notna(row.get(deaths_col)) else 0
                assists = row.get(assists_col, 0) if pd.notna(row.get(assists_col)) else 0
                hero_id = row.get(hero_col, 0) if pd.notna(row.get(hero_col)) else 0
                
                player_stats[account_id].append({
                    'match_id': match_id,
                    'kills': kills,
                    'deaths': deaths,
                    'assists': assists,
                    'duration_min': duration_min,
                    'hero_id': hero_id,
                })
    
    logger.info(f"Found {len(player_stats)} unique players")
    
    # Строим DNA профили
    player_dna: Dict[str, Dict[str, float]] = {}
    
    for account_id, games in player_stats.items():
        if len(games) < MIN_GAMES:
            continue
        
        kills_list = [g['kills'] for g in games]
        deaths_list = [g['deaths'] for g in games]
        assists_list = [g['assists'] for g in games]
        duration_list = [g['duration_min'] for g in games]
        hero_ids = [g['hero_id'] for g in games]
        
        avg_kills = np.mean(kills_list)
        avg_deaths = np.mean(deaths_list)
        avg_assists = np.mean(assists_list)
        avg_duration = np.mean(duration_list)
        
        # Aggression: (K+A) per minute
        total_ka = sum(kills_list) + sum(assists_list)
        total_duration = sum(duration_list)
        aggression = total_ka / max(total_duration, 1)
        
        # Pace: kills per minute
        pace = sum(kills_list) / max(total_duration, 1)
        
        # Feed: deaths per minute
        feed = sum(deaths_list) / max(total_duration, 1)
        
        # Versatility: unique heroes
        versatility = len(set(hero_ids))
        
        # KDA ratio
        kda = (avg_kills + avg_assists) / max(avg_deaths, 1)
        
        # Recent aggression (last 10 games)
        recent_games = games[-10:] if len(games) >= 10 else games
        recent_ka = sum(g['kills'] + g['assists'] for g in recent_games)
        recent_dur = sum(g['duration_min'] for g in recent_games)
        recent_aggression = recent_ka / max(recent_dur, 1)
        
        # Aggression by hero type (aggressive vs passive heroes)
        # Aggressive heroes: high blood_score (Pudge, Bristle, Huskar, etc.)
        AGGRO_HEROES = {14, 99, 59, 31, 88, 67, 91, 82, 20, 114, 7, 11, 44, 9, 23}  # High blood heroes
        PASSIVE_HEROES = {76, 66, 94, 49, 34, 111, 57, 83, 12, 8, 80, 84, 101}  # Low blood heroes
        
        aggro_games = [g for g in games if g['hero_id'] in AGGRO_HEROES]
        passive_games = [g for g in games if g['hero_id'] in PASSIVE_HEROES]
        
        if aggro_games:
            aggro_ka = sum(g['kills'] + g['assists'] for g in aggro_games)
            aggro_dur = sum(g['duration_min'] for g in aggro_games)
            aggression_on_aggro = aggro_ka / max(aggro_dur, 1)
        else:
            aggression_on_aggro = aggression  # fallback to overall
        
        if passive_games:
            passive_ka = sum(g['kills'] + g['assists'] for g in passive_games)
            passive_dur = sum(g['duration_min'] for g in passive_games)
            aggression_on_passive = passive_ka / max(passive_dur, 1)
        else:
            aggression_on_passive = aggression  # fallback to overall
        
        # Aggression delta: how much more aggressive on aggro heroes
        aggression_delta = aggression_on_aggro - aggression_on_passive
        
        player_dna[account_id] = {
            'games_count': len(games),
            'avg_kills': round(avg_kills, 2),
            'avg_deaths': round(avg_deaths, 2),
            'avg_assists': round(avg_assists, 2),
            'avg_duration': round(avg_duration, 2),
            'aggression': round(aggression, 4),
            'pace': round(pace, 4),
            'feed': round(feed, 4),
            'versatility': versatility,
            'kda': round(kda, 2),
            # New extended DNA
            'recent_aggression': round(recent_aggression, 4),
            'aggression_on_aggro': round(aggression_on_aggro, 4),
            'aggression_on_passive': round(aggression_on_passive, 4),
            'aggression_delta': round(aggression_delta, 4),
            'aggro_games_count': len(aggro_games),
            'passive_games_count': len(passive_games),
        }
    
    logger.info(f"Built DNA profiles for {len(player_dna)} players (>= {MIN_GAMES} games)")
    
    return player_dna


def save_player_dna(player_dna: Dict[str, Dict[str, float]], output_path: str = 'data/player_dna.json') -> None:
    """Сохраняет DNA профили в JSON."""
    with open(output_path, 'w') as f:
        json.dump(player_dna, f, indent=2)
    logger.info(f"Saved player DNA to {output_path}")


def analyze_dna_stats(player_dna: Dict[str, Dict[str, float]]) -> None:
    """Выводит статистику по DNA профилям."""
    if not player_dna:
        print("No DNA profiles found")
        return
    
    df = pd.DataFrame(player_dna).T
    
    print("\n" + "=" * 60)
    print("PLAYER DNA STATISTICS")
    print("=" * 60)
    
    print(f"\nTotal players with DNA: {len(df)}")
    print(f"\nGames per player:")
    print(f"  Min: {df['games_count'].min():.0f}")
    print(f"  Max: {df['games_count'].max():.0f}")
    print(f"  Mean: {df['games_count'].mean():.1f}")
    
    print(f"\nAggression score:")
    print(f"  Min: {df['aggression'].min():.4f}")
    print(f"  Max: {df['aggression'].max():.4f}")
    print(f"  Mean: {df['aggression'].mean():.4f}")
    
    print(f"\nPace (kills/min):")
    print(f"  Min: {df['pace'].min():.4f}")
    print(f"  Max: {df['pace'].max():.4f}")
    print(f"  Mean: {df['pace'].mean():.4f}")
    
    print(f"\nAvg Duration:")
    print(f"  Min: {df['avg_duration'].min():.1f} min")
    print(f"  Max: {df['avg_duration'].max():.1f} min")
    print(f"  Mean: {df['avg_duration'].mean():.1f} min")
    
    print(f"\nVersatility (unique heroes):")
    print(f"  Min: {df['versatility'].min():.0f}")
    print(f"  Max: {df['versatility'].max():.0f}")
    print(f"  Mean: {df['versatility'].mean():.1f}")
    
    # Top aggressive players
    print(f"\n--- Top 10 Most Aggressive Players ---")
    top_aggro = df.nlargest(10, 'aggression')[['games_count', 'aggression', 'avg_kills', 'kda']]
    print(top_aggro.to_string())


def main() -> None:
    """Основной пайплайн."""
    player_dna = build_player_dna()
    save_player_dna(player_dna)
    analyze_dna_stats(player_dna)


if __name__ == '__main__':
    main()
