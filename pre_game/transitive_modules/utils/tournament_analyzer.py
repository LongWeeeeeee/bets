"""
Анализатор турниров для построения ATP-подобного рейтинга.

Логика:
1. Группируем матчи по турнирам (leagueId)
2. Определяем тир турнира по составу команд (70%+ tier1 = tier1 турнир)
3. По формату серий определяем стадию (Bo5 = финал, Bo3 = плейофф, Bo1/Bo2 = группа)
4. Начисляем ATP поинты по местам
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional

# Импортируем tier списки
TIER_ONE_IDS: Set[int] = set()
TIER_TWO_IDS: Set[int] = set()

try:
    # Загружаем tier списки из внешнего файла
    import importlib.util
    spec = importlib.util.spec_from_file_location("id_to_names", "/Users/alex/Documents/ingame/base/id_to_names.py")
    id_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(id_module)
    
    # Извлекаем ID из словарей (значения могут быть int или set)
    for name, val in id_module.tier_one_teams.items():
        if isinstance(val, int):
            TIER_ONE_IDS.add(val)
        elif isinstance(val, set):
            TIER_ONE_IDS.update(val)
    
    for name, val in id_module.tier_two_teams.items():
        if isinstance(val, int):
            TIER_TWO_IDS.add(val)
        elif isinstance(val, set):
            TIER_TWO_IDS.update(val)
    
    print(f"Loaded {len(TIER_ONE_IDS)} tier1 teams, {len(TIER_TWO_IDS)} tier2 teams")
except Exception as e:
    print(f"Warning: Could not load tier lists: {e}")


# ATP-подобные очки по тирам турниров и местам
ATP_POINTS = {
    'tier1': {  # Major / TI
        1: 2000,
        2: 1200,
        3: 720,   # 3-4 место
        4: 720,
        5: 360,   # 5-8 место
        6: 360,
        7: 360,
        8: 360,
        9: 180,   # 9-12 место
        10: 180,
        11: 180,
        12: 180,
        13: 90,   # 13-16 место
        14: 90,
        15: 90,
        16: 90,
    },
    'tier2': {  # DPC League / Regional
        1: 500,
        2: 300,
        3: 180,
        4: 180,
        5: 90,
        6: 90,
        7: 90,
        8: 90,
        9: 45,
        10: 45,
        11: 45,
        12: 45,
    },
    'tier3': {  # Minor / Open
        1: 125,
        2: 75,
        3: 45,
        4: 45,
        5: 20,
        6: 20,
        7: 20,
        8: 20,
    },
}


class TournamentAnalyzer:
    def __init__(self, data_dir: Optional[str] = None):
        if data_dir is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            data_dir = os.path.join(base_dir, "ingame", "pro_heroes_data", "json_parts_split_from_object")
        self.data_dir = data_dir
        self.matches_data: Dict[str, Dict] = {}
        self.load_data()
    
    def load_data(self):
        """Загружает все JSON файлы"""
        import json
        for filename in os.listdir(self.data_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(self.data_dir, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.matches_data.update(data)
        print(f"Loaded {len(self.matches_data)} matches")
    
    def get_tournaments(self) -> Dict[int, Dict]:
        """Группирует матчи по турнирам (leagueId)"""
        tournaments = defaultdict(lambda: {
            'name': '',
            'matches': [],
            'teams': set(),
            'series': defaultdict(list),
            'start_ts': float('inf'),
            'end_ts': 0,
        })
        
        for match_id, match in self.matches_data.items():
            league = match.get('league') or {}
            league_id = league.get('id')
            if not league_id:
                continue
            
            t = tournaments[league_id]
            t['name'] = league.get('displayName') or league.get('name') or f'League_{league_id}'
            t['matches'].append(match)
            
            # Собираем команды
            rt = match.get('radiantTeam') or {}
            dt = match.get('direTeam') or {}
            rid, did = rt.get('id'), dt.get('id')
            if rid:
                t['teams'].add(rid)
            if did:
                t['teams'].add(did)
            
            # Группируем по сериям
            series_id = match.get('seriesId', match.get('id'))
            t['series'][series_id].append(match)
            
            # Время
            ts = match.get('startDateTime', 0)
            if ts > 0:
                t['start_ts'] = min(t['start_ts'], ts)
                t['end_ts'] = max(t['end_ts'], ts)
        
        return dict(tournaments)
    
    def classify_tournament_tier(self, teams: Set[int]) -> Tuple[str, Dict]:
        """Определяет тир турнира по составу команд"""
        if not teams:
            return 'tier3', {'tier1': 0, 'tier2': 0, 'other': 0}
        
        t1_count = sum(1 for tid in teams if tid in TIER_ONE_IDS)
        t2_count = sum(1 for tid in teams if tid in TIER_TWO_IDS)
        other_count = len(teams) - t1_count - t2_count
        
        stats = {
            'tier1': t1_count,
            'tier2': t2_count,
            'other': other_count,
            'total': len(teams),
            't1_pct': t1_count / len(teams) * 100,
            't2_pct': t2_count / len(teams) * 100,
        }
        
        # 70%+ tier1 команд = tier1 турнир
        if t1_count / len(teams) >= 0.7:
            return 'tier1', stats
        # 70%+ tier2 команд = tier2 турнир
        elif t2_count / len(teams) >= 0.7:
            return 'tier2', stats
        # 70%+ других = tier3 турнир
        elif other_count / len(teams) >= 0.7:
            return 'tier3', stats
        else:
            return 'mixed', stats
    
    def analyze_tournament_structure(self, tournament: Dict) -> Dict:
        """Анализирует структуру турнира по формату серий"""
        series = tournament['series']
        
        structure = {
            'bo5_series': [],  # Финалы
            'bo3_series': [],  # Плейофф
            'bo2_series': [],  # Группа
            'bo1_series': [],  # Группа / квалы
            'total_series': len(series),
            'total_maps': len(tournament['matches']),
        }
        
        for series_id, matches in series.items():
            n_maps = len(matches)
            
            # Определяем формат серии
            if n_maps >= 4:  # Bo5 (3-5 карт)
                structure['bo5_series'].append((series_id, matches))
            elif n_maps == 3:  # Bo3 (2-3 карты)
                structure['bo3_series'].append((series_id, matches))
            elif n_maps == 2:  # Bo2 или Bo3 (2:0)
                structure['bo2_series'].append((series_id, matches))
            else:  # Bo1
                structure['bo1_series'].append((series_id, matches))
        
        return structure
    
    def determine_placements(self, tournament: Dict) -> List[Tuple[int, int, str]]:
        """
        Определяет места команд в турнире через анализ elimination path.
        
        Логика для double elimination:
        - Строим граф: кто кого выбил
        - Финалист GF = 2 место
        - Проигравший LB Final = 3 место  
        - Проигравший LB Semi = 4 место (или 3-4 если нет LB Final)
        - И т.д. по нижней сетке
        
        Returns:
            List of (team_id, place, stage_name)
        """
        structure = self.analyze_tournament_structure(tournament)
        placements = []
        placed_teams = set()
        
        # Собираем все серии с результатами
        all_series = []
        for series_id, matches in (structure['bo5_series'] + structure['bo3_series']):
            winner, loser = self._get_series_result(matches)
            if winner and loser:
                ts = max(m.get('startDateTime', 0) for m in matches)
                n_maps = len(matches)
                all_series.append({
                    'series_id': series_id,
                    'winner': winner,
                    'loser': loser,
                    'ts': ts,
                    'n_maps': n_maps,
                    'is_bo5': n_maps >= 4,
                })
        
        # Сортируем по времени (новые первые)
        all_series.sort(key=lambda x: x['ts'], reverse=True)
        
        if not all_series:
            # Нет плейофф серий - все из группы
            for team_id in tournament['teams']:
                placements.append((team_id, 1, 'Group stage'))
            return placements
        
        # 1. Grand Final (Bo5 или последняя Bo3)
        gf = all_series[0]
        placements.append((gf['winner'], 1, 'Grand Final Winner'))
        placements.append((gf['loser'], 2, 'Grand Final Loser'))
        placed_teams.add(gf['winner'])
        placed_teams.add(gf['loser'])
        
        # 2. Анализируем остальные серии
        # Строим elimination order: кто когда вылетел
        elimination_order = []  # (team_id, ts, eliminated_by)
        
        for s in all_series[1:]:  # Пропускаем GF
            loser = s['loser']
            if loser not in placed_teams:
                elimination_order.append((loser, s['ts'], s['winner']))
        
        # Сортируем по времени вылета (позже = выше место)
        elimination_order.sort(key=lambda x: x[1], reverse=True)
        
        # Назначаем места
        # Double elim: 3, 4, 5-6, 7-8, 9-12, 13-16
        place_brackets = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        place_idx = 0
        
        for team_id, ts, eliminated_by in elimination_order:
            if team_id in placed_teams:
                continue
            if place_idx < len(place_brackets):
                place = place_brackets[place_idx]
                placements.append((team_id, place, f'Eliminated by {self.get_team_name(eliminated_by)}'))
                placed_teams.add(team_id)
                place_idx += 1
        
        # 3. Команды из групповой стадии без плейоффа
        remaining_teams = tournament['teams'] - placed_teams
        if remaining_teams:
            group_place = max(p[1] for p in placements) + 1 if placements else 1
            for team_id in remaining_teams:
                placements.append((team_id, group_place, 'Group stage / Did not qualify'))
        
        return sorted(placements, key=lambda x: x[1])
    
    def _get_series_result(self, matches: List[Dict]) -> Tuple[Optional[int], Optional[int]]:
        """Определяет победителя и проигравшего серии"""
        if not matches:
            return None, None
        
        team_wins = defaultdict(int)
        teams = set()
        
        for match in matches:
            rt = match.get('radiantTeam') or {}
            dt = match.get('direTeam') or {}
            rid, did = rt.get('id'), dt.get('id')
            rw = match.get('didRadiantWin')
            
            if rid:
                teams.add(rid)
            if did:
                teams.add(did)
            
            if rw is True and rid:
                team_wins[rid] += 1
            elif rw is False and did:
                team_wins[did] += 1
        
        if len(teams) != 2:
            return None, None
        
        teams_list = list(teams)
        t1, t2 = teams_list[0], teams_list[1]
        
        if team_wins[t1] > team_wins[t2]:
            return t1, t2
        elif team_wins[t2] > team_wins[t1]:
            return t2, t1
        else:
            return None, None  # Ничья (не завершена)
    
    def calculate_atp_points(self, tournament: Dict, tier: str) -> Dict[int, int]:
        """Начисляет ATP очки командам по местам"""
        placements = self.determine_placements(tournament)
        points_table = ATP_POINTS.get(tier, ATP_POINTS['tier3'])
        
        team_points = {}
        for team_id, place, stage in placements:
            points = points_table.get(place, 0)
            team_points[team_id] = points
        
        return team_points
    
    def get_team_name(self, team_id: int) -> str:
        """Получает имя команды из матчей"""
        for match in self.matches_data.values():
            rt = match.get('radiantTeam') or {}
            dt = match.get('direTeam') or {}
            if rt.get('id') == team_id:
                return rt.get('name', f'Team_{team_id}')
            if dt.get('id') == team_id:
                return dt.get('name', f'Team_{team_id}')
        return f'Team_{team_id}'


def analyze_single_tournament(analyzer: TournamentAnalyzer, league_id: int):
    """Анализирует один турнир"""
    tournaments = analyzer.get_tournaments()
    
    if league_id not in tournaments:
        print(f"Tournament {league_id} not found")
        return
    
    t = tournaments[league_id]
    tier, stats = analyzer.classify_tournament_tier(t['teams'])
    structure = analyzer.analyze_tournament_structure(t)
    
    print(f"\n{'='*60}")
    print(f"Tournament: {t['name']}")
    print(f"League ID: {league_id}")
    print(f"{'='*60}")
    
    # Даты
    start = datetime.fromtimestamp(t['start_ts']).strftime('%Y-%m-%d') if t['start_ts'] < float('inf') else 'N/A'
    end = datetime.fromtimestamp(t['end_ts']).strftime('%Y-%m-%d') if t['end_ts'] > 0 else 'N/A'
    print(f"Period: {start} - {end}")
    
    # Тир
    print(f"\nTier: {tier.upper()}")
    print(f"  Tier1 teams: {stats['tier1']}/{stats['total']} ({stats['t1_pct']:.1f}%)")
    print(f"  Tier2 teams: {stats['tier2']}/{stats['total']} ({stats['t2_pct']:.1f}%)")
    print(f"  Other teams: {stats['other']}/{stats['total']}")
    
    # Структура
    print(f"\nStructure:")
    print(f"  Total series: {structure['total_series']}")
    print(f"  Total maps: {structure['total_maps']}")
    print(f"  Bo5 series: {len(structure['bo5_series'])} (finals)")
    print(f"  Bo3 series: {len(structure['bo3_series'])} (playoff)")
    print(f"  Bo2 series: {len(structure['bo2_series'])} (groups)")
    print(f"  Bo1 series: {len(structure['bo1_series'])} (groups/quals)")
    
    # Места и очки
    if tier != 'mixed':
        placements = analyzer.determine_placements(t)
        points = analyzer.calculate_atp_points(t, tier)
        
        print(f"\nPlacements & ATP Points:")
        for team_id, place, stage in placements[:16]:
            name = analyzer.get_team_name(team_id)
            pts = points.get(team_id, 0)
            print(f"  {place:2d}. {name:<25} {pts:4d} pts  ({stage})")
    else:
        print(f"\nMixed tier tournament - skipping placements")
        print("Teams:")
        for tid in list(t['teams'])[:10]:
            name = analyzer.get_team_name(tid)
            in_t1 = tid in TIER_ONE_IDS
            in_t2 = tid in TIER_TWO_IDS
            tier_str = "T1" if in_t1 else ("T2" if in_t2 else "T3")
            print(f"  - {name} [{tier_str}]")


def list_recent_tournaments(analyzer: TournamentAnalyzer, n: int = 20):
    """Показывает последние N турниров"""
    tournaments = analyzer.get_tournaments()
    
    # Сортируем по дате окончания
    sorted_t = sorted(tournaments.items(), key=lambda x: x[1]['end_ts'], reverse=True)
    
    print(f"\n{'='*80}")
    print(f"Recent {n} tournaments:")
    print(f"{'='*80}")
    print(f"{'ID':<10} {'Name':<35} {'Teams':<6} {'Maps':<6} {'Tier':<8} {'End Date':<12}")
    print("-" * 80)
    
    for league_id, t in sorted_t[:n]:
        tier, stats = analyzer.classify_tournament_tier(t['teams'])
        end = datetime.fromtimestamp(t['end_ts']).strftime('%Y-%m-%d') if t['end_ts'] > 0 else 'N/A'
        name = t['name'][:33] + '..' if len(t['name']) > 35 else t['name']
        print(f"{league_id:<10} {name:<35} {len(t['teams']):<6} {len(t['matches']):<6} {tier:<8} {end:<12}")


def build_atp_ranking(
    analyzer: TournamentAnalyzer, 
    days: int = 365, 
    include_mixed: bool = True,
    decay_half_life: int = 90,  # Очки уменьшаются вдвое каждые N дней
) -> Dict[int, Dict]:
    """
    Строит ATP рейтинг по всем турнирам за последние N дней.
    
    Args:
        days: Период в днях
        include_mixed: Если True, mixed турниры считаются как tier2
        decay_half_life: Период полураспада очков в днях (0 = без decay)
    
    Returns:
        Dict[team_id] -> {
            'name': str,
            'total_points': int,
            'tournaments': List[{league_id, name, place, points, date}]
        }
    """
    import time
    import math
    
    now_ts = time.time()
    cutoff_ts = now_ts - days * 86400
    
    tournaments = analyzer.get_tournaments()
    team_rankings = defaultdict(lambda: {'name': '', 'total_points': 0, 'raw_points': 0, 'tournaments': []})
    
    for league_id, t in tournaments.items():
        # Пропускаем старые турниры
        if t['end_ts'] < cutoff_ts:
            continue
        
        # Пропускаем слишком маленькие (< 4 команд)
        if len(t['teams']) < 4:
            continue
        
        tier, stats = analyzer.classify_tournament_tier(t['teams'])
        
        # Mixed турниры - как tier2 если include_mixed
        if tier == 'mixed':
            if include_mixed:
                tier = 'tier2'
            else:
                continue
        
        # Получаем очки
        points = analyzer.calculate_atp_points(t, tier)
        
        end_date = datetime.fromtimestamp(t['end_ts']).strftime('%Y-%m-%d')
        
        # Вычисляем decay factor
        days_ago = (now_ts - t['end_ts']) / 86400
        if decay_half_life > 0:
            decay_factor = math.pow(0.5, days_ago / decay_half_life)
        else:
            decay_factor = 1.0
        
        for team_id, pts in points.items():
            if pts > 0:
                name = analyzer.get_team_name(team_id)
                decayed_pts = int(pts * decay_factor)
                
                team_rankings[team_id]['name'] = name
                team_rankings[team_id]['total_points'] += decayed_pts
                team_rankings[team_id]['raw_points'] += pts
                team_rankings[team_id]['tournaments'].append({
                    'league_id': league_id,
                    'name': t['name'],
                    'tier': tier,
                    'points': pts,
                    'decayed_points': decayed_pts,
                    'date': end_date,
                    'days_ago': int(days_ago),
                })
    
    return dict(team_rankings)


def save_atp_ranking(analyzer: TournamentAnalyzer, output_path: str, days: int = 365, decay_half_life: int = 90):
    """Сохраняет ATP рейтинг в JSON файл"""
    import json
    
    rankings = build_atp_ranking(analyzer, days=days, include_mixed=True, decay_half_life=decay_half_life)
    
    # Сортируем и добавляем rank
    sorted_teams = sorted(rankings.items(), key=lambda x: x[1]['total_points'], reverse=True)
    
    output = {
        'generated': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'period_days': days,
        'decay_half_life': decay_half_life,
        'rankings': []
    }
    
    for rank, (team_id, data) in enumerate(sorted_teams, 1):
        output['rankings'].append({
            'rank': rank,
            'team_id': team_id,
            'name': data['name'],
            'points': data['total_points'],
            'raw_points': data.get('raw_points', data['total_points']),
            'tournaments_count': len(data['tournaments']),
            'tournaments': data['tournaments'],
        })
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"Saved ATP ranking to {output_path}")
    return output


def get_team_atp_rank(analyzer: TournamentAnalyzer, team_id: int, days: int = 365, decay_half_life: int = 90) -> Tuple[int, int]:
    """
    Возвращает ATP ранг и очки команды.
    
    Returns:
        (rank, points) или (None, 0) если команда не найдена
    """
    rankings = build_atp_ranking(analyzer, days=days, decay_half_life=decay_half_life)
    sorted_teams = sorted(rankings.items(), key=lambda x: x[1]['total_points'], reverse=True)
    
    for rank, (tid, data) in enumerate(sorted_teams, 1):
        if tid == team_id:
            return rank, data['total_points']
    
    return None, 0


def print_atp_ranking(analyzer: TournamentAnalyzer, top_n: int = 30, days: int = 365):
    """Выводит ATP рейтинг"""
    rankings = build_atp_ranking(analyzer, days=days)
    
    # Сортируем по очкам
    sorted_teams = sorted(rankings.items(), key=lambda x: x[1]['total_points'], reverse=True)
    
    print(f"\n{'='*70}")
    print(f"ATP RANKING (last {days} days)")
    print(f"{'='*70}")
    print(f"{'Rank':<5} {'Team':<25} {'Points':<8} {'Tournaments':<10}")
    print("-" * 70)
    
    for i, (team_id, data) in enumerate(sorted_teams[:top_n], 1):
        n_tournaments = len(data['tournaments'])
        print(f"{i:<5} {data['name']:<25} {data['total_points']:<8} {n_tournaments:<10}")
    
    return sorted_teams


if __name__ == '__main__':
    analyzer = TournamentAnalyzer()
    
    import sys
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == 'ranking':
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 365
            print_atp_ranking(analyzer, top_n=40, days=days)
        elif arg == 'save':
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 365
            output_path = 'data/atp_ranking.json'
            save_atp_ranking(analyzer, output_path, days=days)
        elif arg == 'list':
            list_recent_tournaments(analyzer, n=30)
        else:
            league_id = int(arg)
            analyze_single_tournament(analyzer, league_id)
    else:
        # По умолчанию показываем рейтинг
        print_atp_ranking(analyzer, top_n=40, days=365)
        print("\n\nUsage:")
        print("  python tournament_analyzer.py              # ATP ranking")
        print("  python tournament_analyzer.py ranking 180  # Ranking for last 180 days")
        print("  python tournament_analyzer.py save 365     # Save ranking to JSON")
        print("  python tournament_analyzer.py list         # List tournaments")
        print("  python tournament_analyzer.py <league_id>  # Analyze specific tournament")
