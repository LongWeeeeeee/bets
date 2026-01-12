"""
Сбор статистики по фазам игры для определения оптимальных фильтров.

Собираем:
1. Распределение длительности матчей
2. Networth leads по минутам
3. Моменты переломов (смена лидера)
4. Камбеки: когда и с какого отставания
5. Корреляция lead на каждой минуте с победой
6. Когда игра "решается" (точка невозврата)
"""

import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Any
import statistics


from typing import Optional

def analyze_match(match: dict) -> Optional[dict]:
    """Извлекает статистику из одного матча."""
    leads = match.get('radiantNetworthLeads', [])
    did_radiant_win = match.get('didRadiantWin')
    
    if did_radiant_win is None or leads is None or len(leads) < 20:
        return None
    
    duration = len(leads)
    
    # Находим переломы (смена знака lead)
    turnarounds = []
    for i in range(1, len(leads)):
        if leads[i-1] * leads[i] < 0 and abs(leads[i]) > 1000:
            turnarounds.append({
                'minute': i,
                'from_lead': leads[i-1],
                'to_lead': leads[i]
            })
    
    # Находим максимальное отставание победителя
    max_deficit = 0
    max_deficit_minute = 0
    for i, lead in enumerate(leads):
        if did_radiant_win:
            # Radiant выиграл - ищем максимальное отставание (отрицательный lead)
            if lead < max_deficit:
                max_deficit = lead
                max_deficit_minute = i
        else:
            # Dire выиграл - ищем максимальное отставание (положительный lead)
            if lead > -max_deficit:
                max_deficit = -lead
                max_deficit_minute = i
    
    # Находим когда игра "решилась" (lead >= 10k и не менялся)
    decision_point = None
    for i in range(len(leads) - 5):
        # Проверяем что lead >= 10k и держится 5 минут
        if abs(leads[i]) >= 10000:
            stable = all(
                (leads[i] > 0 and leads[j] > 5000) or 
                (leads[i] < 0 and leads[j] < -5000)
                for j in range(i, min(i + 5, len(leads)))
            )
            if stable:
                winner_at_point = 'radiant' if leads[i] > 0 else 'dire'
                actual_winner = 'radiant' if did_radiant_win else 'dire'
                decision_point = {
                    'minute': i,
                    'lead': leads[i],
                    'correct': winner_at_point == actual_winner
                }
                break
    
    # Lead на ключевых минутах
    key_minutes = {}
    for m in [10, 15, 20, 25, 30, 35, 40, 45, 50]:
        if m < len(leads):
            key_minutes[m] = leads[m]
    
    return {
        'duration': duration,
        'did_radiant_win': did_radiant_win,
        'turnarounds': turnarounds,
        'max_deficit': max_deficit,
        'max_deficit_minute': max_deficit_minute,
        'decision_point': decision_point,
        'key_minutes': key_minutes,
        'leads': leads  # Полный массив для детального анализа
    }


def collect_stats(matches_path: str, output_path: str, max_matches: Optional[int] = None):
    """Собирает статистику со всех матчей."""
    print(f"Загрузка матчей из {matches_path}...")
    
    with open(matches_path, 'r') as f:
        data = json.load(f)
    
    # Поддержка dict и list форматов
    if isinstance(data, dict):
        matches = list(data.values())
    else:
        matches = data
    
    if max_matches:
        matches = matches[:max_matches]
    
    print(f"Анализ {len(matches)} матчей...")
    
    # Собираем статистику
    stats = {
        'total_matches': 0,
        'durations': [],
        'turnarounds_by_minute': defaultdict(int),
        'comebacks': [],  # Камбеки с отставанием >= 5k
        'big_comebacks': [],  # Камбеки с отставанием >= 10k
        'decision_points': [],
        'lead_by_minute': defaultdict(list),  # lead на каждой минуте
        'win_rate_by_lead': defaultdict(lambda: {'wins': 0, 'total': 0}),  # WR по lead на минуте
    }
    
    for i, match in enumerate(matches):
        if i % 10000 == 0:
            print(f"  Обработано {i}/{len(matches)}...")
        
        result = analyze_match(match)
        if result is None:
            continue
        
        stats['total_matches'] += 1
        stats['durations'].append(result['duration'])
        
        # Переломы
        for t in result['turnarounds']:
            stats['turnarounds_by_minute'][t['minute']] += 1
        
        # Камбеки
        if abs(result['max_deficit']) >= 5000:
            stats['comebacks'].append({
                'deficit': result['max_deficit'],
                'minute': result['max_deficit_minute'],
                'duration': result['duration'],
                'won': True  # По определению - победитель отставал
            })
        
        if abs(result['max_deficit']) >= 10000:
            stats['big_comebacks'].append({
                'deficit': result['max_deficit'],
                'minute': result['max_deficit_minute'],
                'duration': result['duration']
            })
        
        # Decision points
        if result['decision_point']:
            stats['decision_points'].append(result['decision_point'])
        
        # Lead по минутам
        for minute, lead in result['key_minutes'].items():
            stats['lead_by_minute'][minute].append(lead)
            
            # WR по lead на этой минуте
            # Группируем по диапазонам: <-10k, -10k..-5k, -5k..0, 0..5k, 5k..10k, >10k
            if lead <= -10000:
                bucket = '<-10k'
            elif lead <= -5000:
                bucket = '-10k..-5k'
            elif lead <= 0:
                bucket = '-5k..0'
            elif lead <= 5000:
                bucket = '0..5k'
            elif lead <= 10000:
                bucket = '5k..10k'
            else:
                bucket = '>10k'
            
            key = f'{minute}min_{bucket}'
            stats['win_rate_by_lead'][key]['total'] += 1
            if result['did_radiant_win'] == (lead > 0):
                stats['win_rate_by_lead'][key]['wins'] += 1
    
    # Вычисляем агрегаты
    print("\nВычисление агрегатов...")
    
    aggregates = {
        'total_matches': stats['total_matches'],
        'duration': {
            'mean': statistics.mean(stats['durations']),
            'median': statistics.median(stats['durations']),
            'stdev': statistics.stdev(stats['durations']) if len(stats['durations']) > 1 else 0,
            'min': min(stats['durations']),
            'max': max(stats['durations']),
            'percentiles': {
                '10': sorted(stats['durations'])[len(stats['durations']) // 10],
                '25': sorted(stats['durations'])[len(stats['durations']) // 4],
                '50': sorted(stats['durations'])[len(stats['durations']) // 2],
                '75': sorted(stats['durations'])[3 * len(stats['durations']) // 4],
                '90': sorted(stats['durations'])[9 * len(stats['durations']) // 10],
            }
        },
        'turnarounds': dict(stats['turnarounds_by_minute']),
        'comebacks_5k': {
            'count': len(stats['comebacks']),
            'avg_deficit': statistics.mean([c['deficit'] for c in stats['comebacks']]) if stats['comebacks'] else 0,
            'avg_minute': statistics.mean([c['minute'] for c in stats['comebacks']]) if stats['comebacks'] else 0,
        },
        'comebacks_10k': {
            'count': len(stats['big_comebacks']),
            'avg_deficit': statistics.mean([c['deficit'] for c in stats['big_comebacks']]) if stats['big_comebacks'] else 0,
            'avg_minute': statistics.mean([c['minute'] for c in stats['big_comebacks']]) if stats['big_comebacks'] else 0,
        },
        'decision_points': {
            'count': len(stats['decision_points']),
            'avg_minute': statistics.mean([d['minute'] for d in stats['decision_points']]) if stats['decision_points'] else 0,
            'accuracy': sum(1 for d in stats['decision_points'] if d['correct']) / len(stats['decision_points']) if stats['decision_points'] else 0,
        },
        'lead_by_minute': {
            str(m): {
                'mean': statistics.mean(leads) if leads else 0,
                'stdev': statistics.stdev(leads) if len(leads) > 1 else 0,
                'abs_mean': statistics.mean([abs(l) for l in leads]) if leads else 0,
            }
            for m, leads in stats['lead_by_minute'].items()
        },
        'win_rate_by_lead': {
            k: {
                'wins': v['wins'],
                'total': v['total'],
                'wr': v['wins'] / v['total'] if v['total'] > 0 else 0
            }
            for k, v in stats['win_rate_by_lead'].items()
        }
    }
    
    # Детальные данные для анализа
    detailed = {
        'durations': stats['durations'],
        'comebacks': stats['comebacks'],
        'big_comebacks': stats['big_comebacks'],
        'decision_points': stats['decision_points'],
    }
    
    result = {
        'aggregates': aggregates,
        'detailed': detailed
    }
    
    print(f"\nСохранение в {output_path}...")
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)
    
    return result


def print_summary(stats: dict):
    """Выводит краткую сводку."""
    agg = stats['aggregates']
    
    print("\n" + "=" * 70)
    print("СВОДКА ПО ФАЗАМ ИГРЫ")
    print("=" * 70)
    
    print(f"\nВсего матчей: {agg['total_matches']}")
    
    print(f"\n📊 ДЛИТЕЛЬНОСТЬ:")
    d = agg['duration']
    print(f"  Среднее: {d['mean']:.1f} мин")
    print(f"  Медиана: {d['median']:.1f} мин")
    print(f"  Стд.откл: {d['stdev']:.1f} мин")
    print(f"  Диапазон: {d['min']}-{d['max']} мин")
    print(f"  Перцентили: 10%={d['percentiles']['10']}, 25%={d['percentiles']['25']}, "
          f"50%={d['percentiles']['50']}, 75%={d['percentiles']['75']}, 90%={d['percentiles']['90']}")
    
    print(f"\n🔄 КАМБЕКИ (отставание >= 5k):")
    c = agg['comebacks_5k']
    print(f"  Количество: {c['count']} ({c['count']/agg['total_matches']*100:.1f}% матчей)")
    print(f"  Среднее отставание: {c['avg_deficit']:.0f}")
    print(f"  Средняя минута: {c['avg_minute']:.1f}")
    
    print(f"\n🔄 БОЛЬШИЕ КАМБЕКИ (отставание >= 10k):")
    c = agg['comebacks_10k']
    print(f"  Количество: {c['count']} ({c['count']/agg['total_matches']*100:.1f}% матчей)")
    print(f"  Среднее отставание: {c['avg_deficit']:.0f}")
    print(f"  Средняя минута: {c['avg_minute']:.1f}")
    
    print(f"\n⚡ ТОЧКИ РЕШЕНИЯ (lead >= 10k стабильно):")
    dp = agg['decision_points']
    print(f"  Количество: {dp['count']} ({dp['count']/agg['total_matches']*100:.1f}% матчей)")
    print(f"  Средняя минута: {dp['avg_minute']:.1f}")
    print(f"  Точность предсказания: {dp['accuracy']*100:.1f}%")
    
    print(f"\n📈 LEAD ПО МИНУТАМ (абсолютное среднее):")
    for m in [10, 15, 20, 25, 30, 35, 40]:
        if str(m) in agg['lead_by_minute']:
            l = agg['lead_by_minute'][str(m)]
            print(f"  {m} мин: avg={l['mean']:+.0f}, |avg|={l['abs_mean']:.0f}, std={l['stdev']:.0f}")
    
    print(f"\n🎯 WIN RATE ПО LEAD (ключевые минуты):")
    for minute in [15, 20, 25, 30]:
        print(f"  {minute} мин:")
        for bucket in ['<-10k', '-10k..-5k', '-5k..0', '0..5k', '5k..10k', '>10k']:
            key = f'{minute}min_{bucket}'
            if key in agg['win_rate_by_lead']:
                wr = agg['win_rate_by_lead'][key]
                if wr['total'] >= 10:
                    print(f"    {bucket:>10}: {wr['wr']*100:>5.1f}% ({wr['total']} матчей)")


if __name__ == '__main__':
    # Собираем с про-матчей (более структурированные игры)
    pro_path = '/Users/alex/Documents/ingame/pro_heroes_data/json_parts_split_from_object/clean_data.json'
    pro_output = '/Users/alex/Documents/ingame/bets_data/game_phases_stats_pro.json'
    
    print("=" * 70)
    print("АНАЛИЗ ПРО-МАТЧЕЙ")
    print("=" * 70)
    
    pro_stats = collect_stats(pro_path, pro_output)
    print_summary(pro_stats)
    
    # Также собираем с паблик матчей для сравнения
    pub_path = '/Users/alex/Documents/ingame/bets_data/analise_pub_matches/extracted_100k_matches.json'
    pub_output = '/Users/alex/Documents/ingame/bets_data/game_phases_stats_pub.json'
    
    print("\n\n" + "=" * 70)
    print("АНАЛИЗ ПАБЛИК-МАТЧЕЙ")
    print("=" * 70)
    
    pub_stats = collect_stats(pub_path, pub_output, max_matches=50000)
    print_summary(pub_stats)
