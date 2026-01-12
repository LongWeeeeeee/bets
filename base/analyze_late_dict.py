"""
Анализ late_dict_raw.json - поиск лучших/худших героев, контрпиков и синергий.
"""

import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Добавляем путь к base для импорта
sys.path.insert(0, str(Path(__file__).parent))
from functions import name_to_id

# Инвертируем словарь: id -> name
ID_TO_NAME: Dict[int, str] = {v: k for k, v in name_to_id.items()}


def get_hero_name(hero_id: int) -> str:
    """Возвращает имя героя по ID."""
    return ID_TO_NAME.get(hero_id, f'Hero_{hero_id}')


def parse_key(key: str) -> dict:
    """
    Парсит ключ словаря и определяет его тип.
    
    Форматы:
    - Solo: "86pos4" -> hero_id=86, pos=4
    - Counterpick 1vs2: "86pos4_vs_101pos4,17pos2" -> hero vs two heroes
    - Counterpick 1vs1: "86pos4_vs_101pos4" -> hero vs one hero
    - Synergy: "86pos4_with_101pos4"
    """
    result: Dict = {'type': None, 'hero_id': None, 'pos': None, 'vs': None, 'with': None}
    
    # Solo hero: "86pos4"
    solo_match = re.match(r'^(\d+)pos(\d)$', key)
    if solo_match:
        result['type'] = 'solo'
        result['hero_id'] = int(solo_match.group(1))
        result['pos'] = int(solo_match.group(2))
        return result
    
    # Counterpick 1vs2: "86pos4_vs_101pos4,17pos2"
    # Формат: heroAposX_vs_heroBposY,heroCposZ
    cp_1vs2_match = re.match(r'^(\d+)pos(\d)_vs_(\d+)pos(\d),(\d+)pos(\d)$', key)
    if cp_1vs2_match:
        result['type'] = 'counterpick_1vs2'
        result['hero_id'] = int(cp_1vs2_match.group(1))
        result['pos'] = int(cp_1vs2_match.group(2))
        result['vs'] = [
            {'hero_id': int(cp_1vs2_match.group(3)), 'pos': int(cp_1vs2_match.group(4))},
            {'hero_id': int(cp_1vs2_match.group(5)), 'pos': int(cp_1vs2_match.group(6))}
        ]
        return result
    
    # Synergy duo: "86pos4_with_101pos4"
    syn_match = re.match(r'^(\d+)pos(\d)_with_(\d+)pos(\d)$', key)
    if syn_match:
        result['type'] = 'synergy_duo'
        result['hero_id'] = int(syn_match.group(1))
        result['pos'] = int(syn_match.group(2))
        result['with'] = {'hero_id': int(syn_match.group(3)), 'pos': int(syn_match.group(4))}
        return result
    
    # Counterpick 1vs1: "86pos4_vs_101pos4"
    cp_1vs1_match = re.match(r'^(\d+)pos(\d)_vs_(\d+)pos(\d)$', key)
    if cp_1vs1_match:
        result['type'] = 'counterpick_1vs1'
        result['hero_id'] = int(cp_1vs1_match.group(1))
        result['pos'] = int(cp_1vs1_match.group(2))
        result['vs'] = [{'hero_id': int(cp_1vs1_match.group(3)), 'pos': int(cp_1vs1_match.group(4))}]
        return result
    
    return result


def analyze_late_dict(file_path: str, min_games: int = 50) -> None:
    """Анализирует late_dict и выводит топ героев/контрпиков/синергий."""
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    solo_stats: List[Tuple[str, int, int, float]] = []
    counterpick_1vs1_stats: List[Tuple[str, int, int, float]] = []
    counterpick_1vs2_stats: List[Tuple[str, int, int, float]] = []
    synergy_stats: List[Tuple[str, int, int, float]] = []
    
    for key, stats in data.items():
        wins = stats.get('wins', 0)
        games = stats.get('games', 0)
        
        if games < min_games:
            continue
        
        winrate = wins / games if games > 0 else 0
        parsed = parse_key(key)
        
        if parsed['type'] == 'solo':
            hero_name = get_hero_name(parsed['hero_id'])
            pos = parsed['pos']
            solo_stats.append((f"{hero_name} pos{pos}", wins, games, winrate))
        
        elif parsed['type'] == 'counterpick_1vs1':
            hero_name = get_hero_name(parsed['hero_id'])
            pos = parsed['pos']
            vs_hero = get_hero_name(parsed['vs'][0]['hero_id'])
            vs_pos = parsed['vs'][0]['pos']
            counterpick_1vs1_stats.append((
                f"{hero_name} pos{pos} vs {vs_hero} pos{vs_pos}",
                wins, games, winrate
            ))
        
        elif parsed['type'] == 'counterpick_1vs2':
            hero_name = get_hero_name(parsed['hero_id'])
            pos = parsed['pos']
            vs1 = get_hero_name(parsed['vs'][0]['hero_id'])
            vs1_pos = parsed['vs'][0]['pos']
            vs2 = get_hero_name(parsed['vs'][1]['hero_id'])
            vs2_pos = parsed['vs'][1]['pos']
            counterpick_1vs2_stats.append((
                f"{hero_name} pos{pos} vs ({vs1} pos{vs1_pos}, {vs2} pos{vs2_pos})",
                wins, games, winrate
            ))
        
        elif parsed['type'] == 'synergy_duo':
            hero_name = get_hero_name(parsed['hero_id'])
            pos = parsed['pos']
            with_hero = get_hero_name(parsed['with']['hero_id'])
            with_pos = parsed['with']['pos']
            synergy_stats.append((
                f"{hero_name} pos{pos} + {with_hero} pos{with_pos}",
                wins, games, winrate
            ))
    
    # Сортируем и выводим
    print("=" * 100)
    print(f"АНАЛИЗ LATE DICT (минимум {min_games} игр)")
    print("=" * 100)
    
    # Solo heroes
    if solo_stats:
        solo_stats.sort(key=lambda x: x[3], reverse=True)
        print("\n🏆 ТОП-20 ЛУЧШИХ SOLO ГЕРОЕВ В ЛЕЙТЕ:")
        print("-" * 60)
        for i, (name, wins, games, wr) in enumerate(solo_stats[:20], 1):
            print(f"  {i:2}. {name:25s} | WR: {wr:.1%} | {wins}/{games}")
        
        print("\n💀 ТОП-20 ХУДШИХ SOLO ГЕРОЕВ В ЛЕЙТЕ:")
        print("-" * 60)
        for i, (name, wins, games, wr) in enumerate(solo_stats[-20:][::-1], 1):
            print(f"  {i:2}. {name:25s} | WR: {wr:.1%} | {wins}/{games}")
    
    # Counterpick 1vs1
    if counterpick_1vs1_stats:
        counterpick_1vs1_stats.sort(key=lambda x: x[3], reverse=True)
        print("\n\n⚔️ ТОП-30 ЛУЧШИХ КОНТРПИКОВ 1vs1 В ЛЕЙТЕ:")
        print("-" * 80)
        for i, (name, wins, games, wr) in enumerate(counterpick_1vs1_stats[:30], 1):
            print(f"  {i:2}. {name:50s} | WR: {wr:.1%} | {wins}/{games}")
        
        print("\n\n💀 ТОП-30 ХУДШИХ КОНТРПИКОВ 1vs1 В ЛЕЙТЕ:")
        print("-" * 80)
        for i, (name, wins, games, wr) in enumerate(counterpick_1vs1_stats[-30:][::-1], 1):
            print(f"  {i:2}. {name:50s} | WR: {wr:.1%} | {wins}/{games}")
    
    # Counterpick 1vs2
    if counterpick_1vs2_stats:
        counterpick_1vs2_stats.sort(key=lambda x: x[3], reverse=True)
        print("\n\n⚔️ ТОП-20 ЛУЧШИХ КОНТРПИКОВ 1vs2 В ЛЕЙТЕ:")
        print("-" * 100)
        for i, (name, wins, games, wr) in enumerate(counterpick_1vs2_stats[:20], 1):
            print(f"  {i:2}. {name:70s} | WR: {wr:.1%} | {wins}/{games}")
        
        print("\n\n💀 ТОП-20 ХУДШИХ КОНТРПИКОВ 1vs2 В ЛЕЙТЕ:")
        print("-" * 100)
        for i, (name, wins, games, wr) in enumerate(counterpick_1vs2_stats[-20:][::-1], 1):
            print(f"  {i:2}. {name:70s} | WR: {wr:.1%} | {wins}/{games}")
    
    # Synergy
    if synergy_stats:
        synergy_stats.sort(key=lambda x: x[3], reverse=True)
        print("\n\n🤝 ТОП-30 ЛУЧШИХ СИНЕРГИЙ В ЛЕЙТЕ:")
        print("-" * 80)
        for i, (name, wins, games, wr) in enumerate(synergy_stats[:30], 1):
            print(f"  {i:2}. {name:50s} | WR: {wr:.1%} | {wins}/{games}")
        
        print("\n\n💀 ТОП-30 ХУДШИХ СИНЕРГИЙ В ЛЕЙТЕ:")
        print("-" * 80)
        for i, (name, wins, games, wr) in enumerate(synergy_stats[-30:][::-1], 1):
            print(f"  {i:2}. {name:50s} | WR: {wr:.1%} | {wins}/{games}")
    
    # Статистика
    print("\n\n📊 ОБЩАЯ СТАТИСТИКА:")
    print("-" * 40)
    print(f"  Solo записей (>={min_games} игр): {len(solo_stats)}")
    print(f"  Counterpick 1vs1 записей: {len(counterpick_1vs1_stats)}")
    print(f"  Counterpick 1vs2 записей: {len(counterpick_1vs2_stats)}")
    print(f"  Synergy записей: {len(synergy_stats)}")
    
    # Подсчёт всех записей без фильтра
    all_1vs2 = sum(1 for k in data.keys() if '_vs_' in k and ',' in k)
    print(f"\n  Всего 1vs2 записей в словаре (без фильтра): {all_1vs2}")


if __name__ == '__main__':
    analyze_late_dict(
        '/Users/alex/Documents/ingame/bets_data/analise_pub_matches/late_dict_raw.json',
        min_games=50
    )
