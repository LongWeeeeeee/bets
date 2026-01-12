import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict
import math
import contextlib

# Импортируем списки tier-1 и tier-2 команд по ID
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
# ВРЕМЕННО ОТКЛЮЧЕНО - pair_factor с tier списками ухудшает accuracy
# TODO: оптимизировать pair_factor или использовать tier списки по-другому
TIER_ONE_IDS: Set[int] = set()
TIER_TWO_IDS: Set[int] = set()

DECAY_HALF_LIFE_DAYS = 14.0
COMMON_DECAY_HALF_LIFE_DAYS = 7.0  # Более агрессивный decay для common opponents

# Настройки Elo по умолчанию (оптимизированы через grid search)
# K=32, Scale=400, no uncertainty — оптимальные параметры
ELO_BASE_RATING = 1500.0
ELO_K_FACTOR = 32.0
ELO_SIGMOID_SCALE = 400.0
ELO_ROSTER_RESET_FACTOR = 0.35
ELO_TIER1_FACTOR = 3.5
ELO_TIER2_FACTOR = 0.9
ELO_TIER3_FACTOR = 0.6
ELO_PAIR_TIER2_FACTOR = 0.6
ELO_PAIR_TIER3_FACTOR = 0.35
ELO_K_UNCERTAINTY_FACTOR = 1.0  # Отключено - не улучшает accuracy
ELO_UNCERTAINTY_GAMES = 20
ELO_K_UPSET_FACTOR = 1.8
ELO_HISTORY_DAYS = 365

DEFAULT_ELO_PARAMS = {
    'base_rating': ELO_BASE_RATING,
    'k_factor': ELO_K_FACTOR,
    'roster_reset_factor': ELO_ROSTER_RESET_FACTOR,
    'tier1_factor': ELO_TIER1_FACTOR,
    'tier2_factor': ELO_TIER2_FACTOR,
    'tier3_factor': ELO_TIER3_FACTOR,
    'pair_tier2_factor': ELO_PAIR_TIER2_FACTOR,
    'pair_tier3_factor': ELO_PAIR_TIER3_FACTOR,
    'elo_sigmoid_scale': ELO_SIGMOID_SCALE,
    'k_uncertainty_factor': ELO_K_UNCERTAINTY_FACTOR,
    'uncertainty_games': ELO_UNCERTAINTY_GAMES,
    'k_upset_factor': ELO_K_UPSET_FACTOR,
}

class TransitiveAnalyzer:
    def __init__(self, data_dir: Optional[str] = None):
        """Инициализатор анализатора.

        data_dir:
            Путь к каталогу с JSON-файлами про-матчей. Если не указан,
            используется путь по умолчанию относительно корня проекта:
            ../pro_heroes_data/json_parts_split_from_object
        """
        if data_dir is None:
            # core/ -> transitive_modules/ -> pre_game/ -> Documents/ -> ingame/pro_heroes_data
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            data_dir = os.path.join(base_dir, "ingame", "pro_heroes_data", "json_parts_split_from_object")
        self.data_dir = data_dir
        self.matches_data: Dict[str, Dict] = {}
        self.team_matches: Dict[int, List[Dict]] = defaultdict(list)
        self.team_names: Dict[int, str] = {}  # Кэш для имен команд
        # Индекс серий по паре команд: pair_series[(min_id, max_id)][series_id] = [matches]
        self.pair_series: Dict[Tuple[int, int], Dict] = defaultdict(lambda: defaultdict(list))
        # Список всех матчей, отсортированных по времени (для рейтингов и глобального анализа)
        self.matches_sorted: List[Dict] = []
        # Кэш tier турниров по составу команд: leagueId -> 'tier1' | 'tier2' | 'tier3'
        self.league_team_tier: Dict[int, str] = {}
        self.load_all_data()
        # После загрузки матчей можем вычислить tier турниров по командам
        # ВРЕМЕННО ОТКЛЮЧЕНО для проверки accuracy
        # self._init_league_team_tiers()

    def load_all_data(self):
        """Загружает все JSON файлы из папки данных"""
        for filename in os.listdir(self.data_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(self.data_dir, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.matches_data.update(data)
        
        # Индексируем матчи по командам, собираем имена и серии по парам команд
        for match_id, match in self.matches_data.items():
            radiant_team = match.get('radiantTeam') or {}
            dire_team = match.get('direTeam') or {}
            radiant_id = radiant_team.get('id')
            dire_id = dire_team.get('id')
            
            if radiant_id:
                radiant_name = radiant_team.get('name', f'Team_{radiant_id}')
                self.team_matches[radiant_id].append(match)
                if radiant_id not in self.team_names:
                    self.team_names[radiant_id] = radiant_name

            if dire_id:
                dire_name = dire_team.get('name', f'Team_{dire_id}')
                self.team_matches[dire_id].append(match)
                if dire_id not in self.team_names:
                    self.team_names[dire_id] = dire_name

            # Индекс по паре команд для ускорения поиска H2H и транзитивных связей
            if radiant_id and dire_id:
                series_id = match.get('seriesId', match.get('id'))
                key = tuple(sorted((radiant_id, dire_id)))
                self.pair_series[key][series_id].append(match)

        # Строим список всех матчей, отсортированных по времени (для рейтинговых вычислений)
        self.matches_sorted = sorted(
            [m for m in self.matches_data.values() if m.get('startDateTime', 0) > 0],
            key=lambda x: x.get('startDateTime', 0)
        )

    def _init_league_team_tiers(self) -> None:
        """Определяет tier турниров (leagueId) по составу команд.

        Логика:
        - tier1: >=60% команд из списка Tier 1 (TIER_ONE_IDS)
        - tier3: >=60% команд, которых нет ни в Tier1, ни в Tier2
        - иначе: tier2 (включая смешанные турниры и те, где доля Tier2 >=60%)
        """
        if not self.matches_data:
            return

        league_to_teams: Dict[int, Set[int]] = defaultdict(set)
        for match in self.matches_data.values():
            league = match.get('league') or {}
            league_id = league.get('id')
            if not league_id:
                continue
            radiant_team = match.get('radiantTeam') or {}
            dire_team = match.get('direTeam') or {}
            rid = radiant_team.get('id')
            did = dire_team.get('id')
            if isinstance(rid, int):
                league_to_teams[league_id].add(rid)
            if isinstance(did, int):
                league_to_teams[league_id].add(did)

        for league_id, team_ids in league_to_teams.items():
            if not team_ids:
                continue
            t1 = sum(1 for tid in team_ids if tid in TIER_ONE_IDS)
            t2 = sum(1 for tid in team_ids if tid in TIER_TWO_IDS)
            t3 = sum(1 for tid in team_ids if tid not in TIER_ONE_IDS and tid not in TIER_TWO_IDS)
            total = len(team_ids)
            if total == 0:
                continue
            t1_share = t1 / total
            t2_share = t2 / total
            t3_share = t3 / total

            if t1_share >= 0.60:
                tier_label = 'tier1'
            elif t2_share >= 0.60:
                tier_label = 'tier2'
            elif t3_share >= 0.60:
                tier_label = 'tier3'
            else:
                # Смешанный турнир без явного доминирования → считаем tier2
                tier_label = 'tier2'

            self.league_team_tier[league_id] = tier_label
    
    def get_team_name(self, team_id: int) -> str:
        """Получает имя команды по ID"""
        return self.team_names.get(team_id, f'Team_{team_id}')

    def compute_elo_ratings_up_to(
        self,
        as_of_timestamp: int,
        max_days: Optional[int] = None,
        base_rating: float = ELO_BASE_RATING,
        k_factor: float = ELO_K_FACTOR,
        # Configurable hyperparameters
        roster_reset_factor: float = ELO_ROSTER_RESET_FACTOR,
        tier1_factor: float = ELO_TIER1_FACTOR,
        tier2_factor: float = ELO_TIER2_FACTOR,
        tier3_factor: float = ELO_TIER3_FACTOR,
        pair_tier2_factor: float = ELO_PAIR_TIER2_FACTOR,
        pair_tier3_factor: float = ELO_PAIR_TIER3_FACTOR,
        min_core_players: int = 4,
        # New formula params
        elo_sigmoid_scale: float = ELO_SIGMOID_SCALE,
        k_uncertainty_factor: float = ELO_K_UNCERTAINTY_FACTOR,  # Multiplier for new teams
        uncertainty_games: int = ELO_UNCERTAINTY_GAMES,          # Number of games to consider "new"
        k_upset_factor: float = ELO_K_UPSET_FACTOR,              # Multiplier for upsets
    ) -> Dict[int, float]:
        """Вычисляет Elo-рейтинги команд на момент as_of_timestamp.

        Особенности:
        - Используется классическая формула ожидания Elo (логистическая кривая).
        - Матчи берутся только из окна [as_of_timestamp - max_days; as_of_timestamp).
        - K-фактор динамический:
          * новички (мало матчей) и команды после долгого простоја получают больший K;
          * команды с большим числом игр в окне получают меньший K;
          * победы/поражения против сильных соперников автоматически дают больше/меньше Elo
            за счёт ожидания (expected score).
        - Обновление идёт по картам (каждый match = одна карта), поэтому 2:0 и 2:1
          дают разное суммарное изменение рейтинга.
        """
        ratings: Dict[int, float] = defaultdict(lambda: base_rating)
        if not self.matches_sorted:
            return ratings

        start_cutoff = None
        if max_days is not None:
            start_cutoff = as_of_timestamp - int(max_days) * 86400

        # Статистика по командам для динамического K
        games_played: Dict[int, int] = defaultdict(int)
        last_match_time: Dict[int, int] = {}
        # Последний состав команды (по игрокам) для отслеживания "эр" ростера
        last_lineup: Dict[int, Set[int]] = {}
        MIN_CORE_PLAYERS = min_core_players

        def dynamic_k(team_id: int, current_ts: int) -> float:
            """ИСТОРИЧЕСКИЙ динамический K-фактор (больше не используется).

            Оставлен для совместимости, но основная формула Elo ниже теперь
            использует симметричный K для обеих команд в матче.
            """
            base_k = float(k_factor)
            return base_k

        def get_team_category(team_id: int) -> str:
            """Возвращает категорию команды для Elo: 'tier1' | 'tier2' | 'tier3'.

            Используется для мягкого подавления матчей, где нет tier-1 команд.
            """
            if team_id in TIER_ONE_IDS:
                return 'tier1'
            if team_id in TIER_TWO_IDS:
                return 'tier2'
            return 'tier3'

        def get_team_players(match: Dict, is_radiant: bool) -> Set[int]:
            """Возвращает множество player_id для команды в этом матче."""
            players = match.get('players') or []
            result: Set[int] = set()
            for p in players:
                is_r = bool(p.get('isRadiant'))
                if is_r != is_radiant:
                    continue
                acc = p.get('steamAccount') or {}
                pid = acc.get('id')
                if isinstance(pid, int) and pid > 0:
                    result.add(pid)
            return result

        def get_tier_factor(match: Dict) -> float:
            """Вес матча по уровню турнира / серии.

            Приоритет:
            1) league_team_tier на основе состава команд (tier1/tier2/tier3);
            2) fallback на league.tier (MAJOR/PROFESSIONAL/AMATEUR/QUALIFIER), если нет leagueId.
            """
            league = match.get('league') or {}
            league_id = league.get('id')
            base: float

            # Сначала пробуем наш классификатор по составу команд
            tier_label = None
            if isinstance(league_id, int) and league_id in self.league_team_tier:
                tier_label = self.league_team_tier[league_id]

            if tier_label == 'tier1':
                base = tier1_factor
            elif tier_label == 'tier2':
                base = tier2_factor
            elif tier_label == 'tier3':
                base = tier3_factor
            else:
                # Fallback на league.tier из API, если классификатор не сработал
                league = match.get('league') or {}
                tier = (league.get('tier') or '').upper()
                if tier == 'MAJOR':
                    base = tier1_factor
                elif tier == 'PROFESSIONAL':
                    base = tier2_factor
                elif tier in ('AMATEUR', 'QUALIFIER'):
                    base = tier3_factor
                else:
                    base = tier2_factor

            # Поправка за формат серии (финалы Bo5 немного важнее)
            series = match.get('series') or {}
            series_type = (series.get('type') or '').upper()
            if series_type in ('BEST_OF_FIVE', 'BO5'):
                bo_factor = 1.15
            elif series_type in ('BEST_OF_THREE', 'BO3'):
                bo_factor = 1.05
            else:
                bo_factor = 1.0

            return base * bo_factor

        def maybe_reset_roster(team_id: int, new_players: Set[int]) -> None:
            """Сбрасывает рейтинг при смене кора (менее MIN_CORE_PLAYERS общих игроков).

            Идея: если в новом матче у команды осталось <4 игроков из прошлого матча,
            считаем, что это новый ростер/эра.
            """
            prev_players = last_lineup.get(team_id)
            if not prev_players or not new_players:
                return
            overlap = len(prev_players & new_players)
            if overlap < MIN_CORE_PLAYERS:
                if roster_reset_factor > 0:
                    old_rating = ratings[team_id]
                    # Линейная интерполяция между старым рейтингом и базовым
                    ratings[team_id] = old_rating * (1.0 - roster_reset_factor) + base_rating * roster_reset_factor
                    
                # Если полный сброс, сбрасываем и счетчик игр (как "новая команда")
                if roster_reset_factor >= 0.99:
                    games_played[team_id] = 0
                    if team_id in last_match_time:
                        del last_match_time[team_id]

        for match in self.matches_sorted:
            mt = match.get('startDateTime', 0)
            if mt <= 0:
                continue
            # Используем только матчи СТРОГО раньше as_of_timestamp, чтобы не подсматривать в серию,
            # которую мы сейчас пытаемся предсказать.
            if mt >= as_of_timestamp:
                break
            if start_cutoff is not None and mt < start_cutoff:
                continue

            radiant_team = match.get('radiantTeam') or {}
            dire_team = match.get('direTeam') or {}
            rid = radiant_team.get('id')
            did = dire_team.get('id')
            rw = match.get('didRadiantWin', None)
            if not rid or not did or not isinstance(rw, bool):
                continue

            # Определяем составы команд в этом матче и при необходимости
            # сбрасываем рейтинг при смене кора (4+ общих игрока = тот же ростер)
            r_players = get_team_players(match, is_radiant=True)
            d_players = get_team_players(match, is_radiant=False)
            maybe_reset_roster(rid, r_players)
            maybe_reset_roster(did, d_players)

            ra = ratings[rid]
            rb = ratings[did]
            # Ожидаемый результат по классической формуле Elo
            expected_r = 1.0 / (1.0 + 10 ** ((rb - ra) / float(elo_sigmoid_scale)))
            expected_d = 1.0 - expected_r
            score_r = 1.0 if rw else 0.0
            score_d = 1.0 - score_r

            # Симметричный K для матча с учётом уровня турнира и категории пар команд
            tier_factor = get_tier_factor(match)

            # Дополнительный мягкий коэффициент по парам уровней команд:
            r_cat = get_team_category(rid)
            d_cat = get_team_category(did)
            if r_cat == 'tier1' or d_cat == 'tier1':
                pair_factor = 1.0
            elif r_cat == 'tier2' and d_cat == 'tier2':
                pair_factor = pair_tier2_factor
            else:
                pair_factor = pair_tier3_factor

            full_factor = tier_factor * pair_factor

            # Базовый K
            k_current = float(k_factor) * full_factor
            
            # Учет неопределенности (мало игр)
            gp_r = games_played[rid]
            gp_d = games_played[did]
            
            k_r = k_current
            k_d = k_current
            
            if k_uncertainty_factor != 1.0:
                if gp_r < uncertainty_games:
                    k_r *= k_uncertainty_factor
                if gp_d < uncertainty_games:
                    k_d *= k_uncertainty_factor
            
            # Учет апсетов (победа с низким шансом)
            # Если победитель имел шанс < 0.3, то это апсет
            if k_upset_factor != 1.0:
                # Для Radiant
                if rw and expected_r < 0.3:
                    k_r *= k_upset_factor
                elif not rw and expected_d < 0.3:
                    k_d *= k_upset_factor

            ratings[rid] = ra + k_r * (score_r - expected_r)
            ratings[did] = rb + k_d * (score_d - expected_d)

            # Обновляем статистику
            games_played[rid] += 1
            games_played[did] += 1
            last_match_time[rid] = mt
            last_match_time[did] = mt
            last_lineup[rid] = r_players
            last_lineup[did] = d_players

        return ratings

    # =========================================================================
    # НОВЫЕ МЕТОДЫ: Form, Momentum, Consistency
    # =========================================================================

    def _team_won_match(self, match: Dict, team_id: int) -> bool:
        """Проверяет, выиграла ли команда данный матч."""
        radiant_team = match.get('radiantTeam') or {}
        dire_team = match.get('direTeam') or {}
        radiant_id = radiant_team.get('id')
        dire_id = dire_team.get('id')
        radiant_won = match.get('didRadiantWin', False)
        
        if radiant_id == team_id:
            return radiant_won
        elif dire_id == team_id:
            return not radiant_won
        return False

    def compute_team_form(
        self,
        team_id: int,
        as_of_timestamp: int,
        n_games: int = 10,
        n_series: Optional[int] = None,
    ) -> Dict:
        """Вычисляет форму команды за последние N игр/серий.
        
        Returns:
            {
                'win_rate': float,          # Винрейт за последние N игр (0.0-1.0)
                'wins': int,                # Число побед
                'losses': int,              # Число поражений
                'games_played': int,        # Всего игр в выборке
                'series_win_rate': float,   # Винрейт по сериям
                'series_wins': int,
                'series_losses': int,
                'series_played': int,
                'weighted_form': float,     # Взвешенная форма (свежие игры важнее)
            }
        """
        team_matches = self.team_matches.get(team_id, [])
        if not team_matches:
            return {
                'win_rate': 0.5, 'wins': 0, 'losses': 0, 'games_played': 0,
                'series_win_rate': 0.5, 'series_wins': 0, 'series_losses': 0, 'series_played': 0,
                'weighted_form': 0.5,
            }
        
        # Фильтруем матчи до указанного времени
        valid_matches = [m for m in team_matches if 0 < m.get('startDateTime', 0) < as_of_timestamp]
        if not valid_matches:
            return {
                'win_rate': 0.5, 'wins': 0, 'losses': 0, 'games_played': 0,
                'series_win_rate': 0.5, 'series_wins': 0, 'series_losses': 0, 'series_played': 0,
                'weighted_form': 0.5,
            }
        
        # Сортируем по времени (новые первые)
        valid_matches.sort(key=lambda m: m.get('startDateTime', 0), reverse=True)
        recent_matches = valid_matches[:n_games]
        
        # Считаем победы по картам
        wins = 0
        weighted_wins = 0.0
        weighted_total = 0.0
        
        for i, match in enumerate(recent_matches):
            won = self._team_won_match(match, team_id)
            if won:
                wins += 1
            # Экспоненциальный вес: более свежие игры важнее
            weight = 0.9 ** i  # 1.0, 0.9, 0.81, 0.729, ...
            weighted_wins += weight if won else 0
            weighted_total += weight
        
        games_played = len(recent_matches)
        win_rate = wins / games_played if games_played > 0 else 0.5
        weighted_form = weighted_wins / weighted_total if weighted_total > 0 else 0.5
        
        # Группируем по сериям для подсчёта винрейта по сериям
        series_results: Dict[int, Dict] = {}  # series_id -> {wins, losses}
        for match in recent_matches:
            series_id = match.get('seriesId', match.get('id'))
            if series_id not in series_results:
                series_results[series_id] = {'wins': 0, 'losses': 0}
            won = self._team_won_match(match, team_id)
            if won:
                series_results[series_id]['wins'] += 1
            else:
                series_results[series_id]['losses'] += 1
        
        # Определяем победителей серий
        series_wins = 0
        series_losses = 0
        for sid, res in series_results.items():
            if res['wins'] > res['losses']:
                series_wins += 1
            elif res['losses'] > res['wins']:
                series_losses += 1
            # Ничьи (равное число карт) не считаем
        
        series_played = series_wins + series_losses
        series_win_rate = series_wins / series_played if series_played > 0 else 0.5
        
        return {
            'win_rate': win_rate,
            'wins': wins,
            'losses': games_played - wins,
            'games_played': games_played,
            'series_win_rate': series_win_rate,
            'series_wins': series_wins,
            'series_losses': series_losses,
            'series_played': series_played,
            'weighted_form': weighted_form,
        }

    def compute_win_streak(self, team_id: int, as_of_timestamp: int) -> Dict:
        """Вычисляет текущую серию побед/поражений.
        
        Returns:
            {
                'streak': int,              # Положительный = серия побед, отрицательный = поражений
                'streak_type': str,         # 'winning', 'losing', 'none'
                'streak_length': int,       # Абсолютная длина серии
                'last_5_pattern': str,      # Паттерн последних 5 игр: 'WWLWL'
            }
        """
        team_matches = self.team_matches.get(team_id, [])
        if not team_matches:
            return {'streak': 0, 'streak_type': 'none', 'streak_length': 0, 'last_5_pattern': ''}
        
        valid_matches = [m for m in team_matches if 0 < m.get('startDateTime', 0) < as_of_timestamp]
        if not valid_matches:
            return {'streak': 0, 'streak_type': 'none', 'streak_length': 0, 'last_5_pattern': ''}
        
        valid_matches.sort(key=lambda m: m.get('startDateTime', 0), reverse=True)
        
        # Паттерн последних 5 игр
        last_5 = valid_matches[:5]
        pattern = ''.join('W' if self._team_won_match(m, team_id) else 'L' for m in last_5)
        
        # Считаем серию
        streak = 0
        if not valid_matches:
            return {'streak': 0, 'streak_type': 'none', 'streak_length': 0, 'last_5_pattern': pattern}
        
        first_result = self._team_won_match(valid_matches[0], team_id)
        streak = 1 if first_result else -1
        
        for match in valid_matches[1:]:
            won = self._team_won_match(match, team_id)
            if won == first_result:
                streak += 1 if first_result else -1
            else:
                break
        
        streak_type = 'winning' if streak > 0 else ('losing' if streak < 0 else 'none')
        
        return {
            'streak': streak,
            'streak_type': streak_type,
            'streak_length': abs(streak),
            'last_5_pattern': pattern,
        }

    def compute_elo_momentum(
        self,
        team_id: int,
        as_of_timestamp: int,
        lookback_days: int = 14,
    ) -> Dict:
        """Вычисляет изменение Elo рейтинга за период.
        
        Returns:
            {
                'elo_now': float,           # Текущий Elo
                'elo_before': float,        # Elo N дней назад
                'elo_change': float,        # Изменение (положительный = рост)
                'elo_change_pct': float,    # Изменение в % от базового (1500)
                'momentum': str,            # 'rising', 'falling', 'stable'
            }
        """
        # Elo сейчас
        elo_now_ratings = self.compute_elo_ratings_up_to(
            as_of_timestamp,
            max_days=ELO_HISTORY_DAYS,
            **DEFAULT_ELO_PARAMS,
        )
        elo_now = elo_now_ratings.get(team_id, ELO_BASE_RATING)
        
        # Elo N дней назад
        before_timestamp = as_of_timestamp - lookback_days * 86400
        elo_before_ratings = self.compute_elo_ratings_up_to(
            before_timestamp,
            max_days=ELO_HISTORY_DAYS,
            **DEFAULT_ELO_PARAMS,
        )
        elo_before = elo_before_ratings.get(team_id, ELO_BASE_RATING)
        
        elo_change = elo_now - elo_before
        elo_change_pct = (elo_change / ELO_BASE_RATING) * 100  # % от базы
        
        # Определяем тренд
        if elo_change > 30:
            momentum = 'rising'
        elif elo_change < -30:
            momentum = 'falling'
        else:
            momentum = 'stable'
        
        return {
            'elo_now': elo_now,
            'elo_before': elo_before,
            'elo_change': elo_change,
            'elo_change_pct': elo_change_pct,
            'momentum': momentum,
        }

    def compute_days_since_last_match(self, team_id: int, as_of_timestamp: int) -> Dict:
        """Вычисляет время с последнего матча команды.
        
        Returns:
            {
                'days_since_last': float,   # Дней с последнего матча
                'hours_since_last': float,  # Часов с последнего матча
                'is_cold': bool,            # True если >14 дней без игр
                'is_active': bool,          # True если <3 дней с последней игры
                'last_match_ts': int,       # Timestamp последнего матча
            }
        """
        team_matches = self.team_matches.get(team_id, [])
        if not team_matches:
            return {
                'days_since_last': 999.0,
                'hours_since_last': 999.0 * 24,
                'is_cold': True,
                'is_active': False,
                'last_match_ts': 0,
            }
        
        valid_matches = [m for m in team_matches if 0 < m.get('startDateTime', 0) < as_of_timestamp]
        if not valid_matches:
            return {
                'days_since_last': 999.0,
                'hours_since_last': 999.0 * 24,
                'is_cold': True,
                'is_active': False,
                'last_match_ts': 0,
            }
        
        last_match = max(valid_matches, key=lambda m: m.get('startDateTime', 0))
        last_ts = last_match.get('startDateTime', 0)
        
        seconds_since = as_of_timestamp - last_ts
        days_since = seconds_since / 86400.0
        hours_since = seconds_since / 3600.0
        
        return {
            'days_since_last': days_since,
            'hours_since_last': hours_since,
            'is_cold': days_since > 14,
            'is_active': days_since < 3,
            'last_match_ts': last_ts,
        }

    def compute_map_margin_stats(self, team_id: int, as_of_timestamp: int, n_series: int = 10) -> Dict:
        """Вычисляет статистику по margin картам (2:0 vs 2:1 и т.д.).
        
        Returns:
            {
                'avg_margin': float,            # Средний margin в победных сериях
                'clean_sweep_rate': float,      # Доля чистых побед (2:0, 3:0)
                'close_series_rate': float,     # Доля близких серий (2:1, 3:2)
                'consistency': float,           # 0-1, насколько стабильны результаты
                'dominant_wins': int,           # Число чистых побед
                'close_wins': int,              # Число близких побед
                'close_losses': int,            # Число близких поражений
                'dominant_losses': int,         # Число разгромных поражений
            }
        """
        team_matches = self.team_matches.get(team_id, [])
        if not team_matches:
            return {
                'avg_margin': 0.0, 'clean_sweep_rate': 0.0, 'close_series_rate': 0.0,
                'consistency': 0.5, 'dominant_wins': 0, 'close_wins': 0,
                'close_losses': 0, 'dominant_losses': 0,
            }
        
        valid_matches = [m for m in team_matches if 0 < m.get('startDateTime', 0) < as_of_timestamp]
        if not valid_matches:
            return {
                'avg_margin': 0.0, 'clean_sweep_rate': 0.0, 'close_series_rate': 0.0,
                'consistency': 0.5, 'dominant_wins': 0, 'close_wins': 0,
                'close_losses': 0, 'dominant_losses': 0,
            }
        
        # Группируем по сериям
        series_data: Dict[int, Dict] = {}
        for match in valid_matches:
            series_id = match.get('seriesId', match.get('id'))
            if series_id not in series_data:
                series_data[series_id] = {'wins': 0, 'losses': 0, 'ts': match.get('startDateTime', 0)}
            won = self._team_won_match(match, team_id)
            if won:
                series_data[series_id]['wins'] += 1
            else:
                series_data[series_id]['losses'] += 1
        
        # Сортируем по времени и берём последние N серий
        sorted_series = sorted(series_data.items(), key=lambda x: x[1]['ts'], reverse=True)[:n_series]
        
        if not sorted_series:
            return {
                'avg_margin': 0.0, 'clean_sweep_rate': 0.0, 'close_series_rate': 0.0,
                'consistency': 0.5, 'dominant_wins': 0, 'close_wins': 0,
                'close_losses': 0, 'dominant_losses': 0,
            }
        
        dominant_wins = 0      # 2:0, 3:0
        close_wins = 0         # 2:1, 3:2
        close_losses = 0       # 1:2, 2:3
        dominant_losses = 0    # 0:2, 0:3
        margins = []
        
        for series_id, data in sorted_series:
            w, l = data['wins'], data['losses']
            total = w + l
            if total == 0:
                continue
            
            margin = w - l
            margins.append(margin)
            
            if w > l:  # Победа
                if l == 0:  # Чистая победа
                    dominant_wins += 1
                else:
                    close_wins += 1
            elif l > w:  # Поражение
                if w == 0:  # Разгром
                    dominant_losses += 1
                else:
                    close_losses += 1
        
        total_series = dominant_wins + close_wins + close_losses + dominant_losses
        total_wins = dominant_wins + close_wins
        
        avg_margin = sum(margins) / len(margins) if margins else 0.0
        clean_sweep_rate = dominant_wins / total_wins if total_wins > 0 else 0.0
        close_series_rate = (close_wins + close_losses) / total_series if total_series > 0 else 0.0
        
        # Консистентность: насколько стабильны результаты (меньше variance = больше consistency)
        if len(margins) > 1:
            mean_margin = sum(margins) / len(margins)
            variance = sum((m - mean_margin) ** 2 for m in margins) / len(margins)
            # Нормализуем: variance 0 -> consistency 1, variance 4+ -> consistency ~0.2
            consistency = 1.0 / (1.0 + variance / 2.0)
        else:
            consistency = 0.5
        
        return {
            'avg_margin': avg_margin,
            'clean_sweep_rate': clean_sweep_rate,
            'close_series_rate': close_series_rate,
            'consistency': consistency,
            'dominant_wins': dominant_wins,
            'close_wins': close_wins,
            'close_losses': close_losses,
            'dominant_losses': dominant_losses,
        }

    def compute_side_stats(self, team_id: int, as_of_timestamp: int, n_games: int = 20) -> Dict:
        """Вычисляет статистику по сторонам (Radiant vs Dire).
        
        Returns:
            {
                'radiant_wr': float,        # Винрейт на Radiant
                'dire_wr': float,           # Винрейт на Dire
                'radiant_games': int,
                'dire_games': int,
                'side_preference': str,     # 'radiant', 'dire', 'neutral'
                'side_diff': float,         # radiant_wr - dire_wr
            }
        """
        team_matches = self.team_matches.get(team_id, [])
        if not team_matches:
            return {
                'radiant_wr': 0.5, 'dire_wr': 0.5, 'radiant_games': 0, 'dire_games': 0,
                'side_preference': 'neutral', 'side_diff': 0.0,
            }
        
        valid_matches = [m for m in team_matches if 0 < m.get('startDateTime', 0) < as_of_timestamp]
        valid_matches.sort(key=lambda m: m.get('startDateTime', 0), reverse=True)
        recent = valid_matches[:n_games]
        
        radiant_wins = 0
        radiant_total = 0
        dire_wins = 0
        dire_total = 0
        
        for match in recent:
            radiant_team = match.get('radiantTeam') or {}
            dire_team = match.get('direTeam') or {}
            radiant_id = radiant_team.get('id')
            radiant_won = match.get('didRadiantWin', False)
            
            if radiant_id == team_id:
                radiant_total += 1
                if radiant_won:
                    radiant_wins += 1
            else:
                dire_total += 1
                if not radiant_won:
                    dire_wins += 1
        
        radiant_wr = radiant_wins / radiant_total if radiant_total > 0 else 0.5
        dire_wr = dire_wins / dire_total if dire_total > 0 else 0.5
        side_diff = radiant_wr - dire_wr
        
        if side_diff > 0.15:
            side_preference = 'radiant'
        elif side_diff < -0.15:
            side_preference = 'dire'
        else:
            side_preference = 'neutral'
        
        return {
            'radiant_wr': radiant_wr,
            'dire_wr': dire_wr,
            'radiant_games': radiant_total,
            'dire_games': dire_total,
            'side_preference': side_preference,
            'side_diff': side_diff,
        }

    def compute_opponent_tier_stats(self, team_id: int, as_of_timestamp: int, n_games: int = 20) -> Dict:
        """Вычисляет статистику против разных tier команд.
        
        Returns:
            {
                'vs_tier1_wr': float,       # Винрейт против tier-1
                'vs_tier2_wr': float,       # Винрейт против tier-2
                'vs_tier3_wr': float,       # Винрейт против tier-3
                'vs_tier1_games': int,
                'vs_tier2_games': int,
                'vs_tier3_games': int,
                'strength_of_schedule': float,  # Средний tier оппонентов (1=сильные, 3=слабые)
            }
        """
        team_matches = self.team_matches.get(team_id, [])
        if not team_matches:
            return {
                'vs_tier1_wr': 0.5, 'vs_tier2_wr': 0.5, 'vs_tier3_wr': 0.5,
                'vs_tier1_games': 0, 'vs_tier2_games': 0, 'vs_tier3_games': 0,
                'strength_of_schedule': 2.0,
            }
        
        valid_matches = [m for m in team_matches if 0 < m.get('startDateTime', 0) < as_of_timestamp]
        valid_matches.sort(key=lambda m: m.get('startDateTime', 0), reverse=True)
        recent = valid_matches[:n_games]
        
        tier1_wins, tier1_total = 0, 0
        tier2_wins, tier2_total = 0, 0
        tier3_wins, tier3_total = 0, 0
        tier_sum = 0.0
        
        for match in recent:
            radiant_team = match.get('radiantTeam') or {}
            dire_team = match.get('direTeam') or {}
            radiant_id = radiant_team.get('id')
            dire_id = dire_team.get('id')
            
            opponent_id = dire_id if radiant_id == team_id else radiant_id
            if not opponent_id:
                continue
            
            won = self._team_won_match(match, team_id)
            
            if opponent_id in TIER_ONE_IDS:
                tier1_total += 1
                tier1_wins += 1 if won else 0
                tier_sum += 1
            elif opponent_id in TIER_TWO_IDS:
                tier2_total += 1
                tier2_wins += 1 if won else 0
                tier_sum += 2
            else:
                tier3_total += 1
                tier3_wins += 1 if won else 0
                tier_sum += 3
        
        total_games = tier1_total + tier2_total + tier3_total
        
        return {
            'vs_tier1_wr': tier1_wins / tier1_total if tier1_total > 0 else 0.5,
            'vs_tier2_wr': tier2_wins / tier2_total if tier2_total > 0 else 0.5,
            'vs_tier3_wr': tier3_wins / tier3_total if tier3_total > 0 else 0.5,
            'vs_tier1_games': tier1_total,
            'vs_tier2_games': tier2_total,
            'vs_tier3_games': tier3_total,
            'strength_of_schedule': tier_sum / total_games if total_games > 0 else 2.0,
        }

    # =========================================================================
    # КОНЕЦ НОВЫХ МЕТОДОВ
    # =========================================================================
    
    def get_latest_match(self, team_id: int, before_time: int) -> Optional[Dict]:
        """Находит последний матч команды до указанного времени"""
        team_matches = self.team_matches.get(team_id, [])
        if not team_matches:
            return None
        
        # Фильтруем матчи до указанного времени
        valid_matches = [m for m in team_matches if m.get('startDateTime', 0) < before_time]
        if not valid_matches:
            return None
        
        # Сортируем по времени и берем самый последний
        latest_match = max(valid_matches, key=lambda x: x.get('startDateTime', 0))
        return latest_match
    
    def find_head_to_head(self, team1_id: int, team2_id: int, start_time: int, end_time: int) -> List[Dict]:
        """Находит прямые матчи между командами, группируя по seriesId и определяя победителей серий.
        
        ВАЖНО: end_time используется как СТРОГАЯ верхняя граница (< end_time, не <=).
        Это предотвращает data leak при предсказании серии.
        """
        # Сначала находим все серии которые попадают в диапазон
        series_in_range = set()
        for match in self.matches_data.values():
            # СТРОГОЕ сравнение: < end_time (не <=) чтобы избежать data leak
            if start_time <= match.get('startDateTime', 0) < end_time:
                radiant_team = match.get('radiantTeam', {})
                dire_team = match.get('direTeam', {})
                
                if (radiant_team.get('id') == team1_id and dire_team.get('id') == team2_id) or \
                   (radiant_team.get('id') == team2_id and dire_team.get('id') == team1_id):
                    series_id = match.get('seriesId')
                    if series_id:
                        series_in_range.add(series_id)
        
        # Собираем все матчи из найденных серий и группируем по seriesId
        series_matches = defaultdict(list)
        for match in self.matches_data.values():
            series_id = match.get('seriesId')
            match_time = match.get('startDateTime', 0)
            # СТРОГОЕ сравнение: < end_time
            in_range = (start_time <= match_time < end_time) or (series_id and series_id in series_in_range)
            
            if in_range and match_time < end_time:
                radiant_team = match.get('radiantTeam', {})
                dire_team = match.get('direTeam', {})
                
                if (radiant_team.get('id') == team1_id and dire_team.get('id') == team2_id) or \
                   (radiant_team.get('id') == team2_id and dire_team.get('id') == team1_id):
                    if series_id:
                        series_matches[series_id].append(match)
                    else:
                        # Если нет seriesId, считаем каждый матч отдельной серией
                        series_matches[match.get('id')].append(match)
        
        # Возвращаем список серий (каждая серия представлена списком матчей)
        # Сортируем по времени первого матча в серии
        result = []
        for series_id, matches in series_matches.items():
            sorted_matches = sorted(matches, key=lambda x: x.get('startDateTime', 0))
            result.append({
                'series_id': series_id,
                'matches': sorted_matches,
                'start_time': sorted_matches[0].get('startDateTime', 0)
            })
        
        return sorted(result, key=lambda x: x['start_time'], reverse=True)
    
    def find_common_opponents(self, team1_id: int, team2_id: int, start_time: int, end_time: int) -> List[Dict]:
        """Находит матчи против общих противников, группируя по seriesId"""
        team1_opponents = set()
        team2_opponents = set()
        team1_series = defaultdict(lambda: defaultdict(list))  # team1_series[opponent][series_id] = [matches]
        team2_series = defaultdict(lambda: defaultdict(list))
        
        # Сначала находим все серии которые попадают в диапазон
        series_in_range = set()
        for match in self.matches_data.values():
            if start_time <= match.get('startDateTime', 0) <= end_time:
                series_id = match.get('seriesId')
                if series_id:
                    series_in_range.add(series_id)
        
        # Собираем оппонентов для каждой команды, группируя по seriesId
        for match in self.matches_data.values():
            series_id = match.get('seriesId', match.get('id'))  # Если нет seriesId, используем match id
            # Включаем матч если он в диапазоне ИЛИ его серия в диапазоне
            match_time = match.get('startDateTime', 0)
            in_range = (start_time <= match_time <= end_time) or (series_id and series_id in series_in_range)
            
            if in_range and match_time <= end_time:
                radiant_team = match.get('radiantTeam', {})
                dire_team = match.get('direTeam', {})
                radiant_id = radiant_team.get('id')
                dire_id = dire_team.get('id')
                
                if radiant_id == team1_id:
                    opponent = dire_id
                    if opponent:
                        team1_opponents.add(opponent)
                        team1_series[opponent][series_id].append(match)
                elif dire_id == team1_id:
                    opponent = radiant_id
                    if opponent:
                        team1_opponents.add(opponent)
                        team1_series[opponent][series_id].append(match)
                
                if radiant_id == team2_id:
                    opponent = dire_id
                    if opponent:
                        team2_opponents.add(opponent)
                        team2_series[opponent][series_id].append(match)
                elif dire_id == team2_id:
                    opponent = radiant_id
                    if opponent:
                        team2_opponents.add(opponent)
                        team2_series[opponent][series_id].append(match)
        
        # Находим общих противников
        common_opponents = team1_opponents.intersection(team2_opponents)
        results = []
        
        for opponent in common_opponents:
            # Исключаем случай когда "общий противник" - это одна из анализируемых команд
            if opponent == team1_id or opponent == team2_id:
                continue
                
            if opponent in team1_series and opponent in team2_series:
                # Возвращаем серии для каждой команды
                results.append({
                    'opponent': opponent,
                    'team1_series': team1_series[opponent],  # dict: series_id -> [matches]
                    'team2_series': team2_series[opponent]   # dict: series_id -> [matches]
                })
        
        return results
    
    def find_transitive_connections(self, team1_id: int, team2_id: int, start_time: int, end_time: int, max_depth: int = 4) -> List[Dict]:
        """Находит транзитивные цепи между командами с максимум max_depth рёбер.

        Базовая цепь: A > C > D > B (3 ребра). Теперь поддерживаем A > ... > B
        с произвольным числом промежуточных команд до указанного предела (например A>B>C>D>F).
        """
        connections: List[Dict] = []
        MIN_EDGE_MATCHES = 2
        MIN_CHAIN_EDGES = 3  # минимум три ребра (A > C > D > B)
        max_depth = max(MIN_CHAIN_EDGES, max_depth)
        MIN_CHAIN_NODES = MIN_CHAIN_EDGES + 1
        MAX_CONNECTIONS = 256

        neighbors_cache: Dict[int, Set[int]] = {}
        edge_series_cache: Dict[Tuple[int, int], Dict] = {}

        def get_series_for_pair(team_a: int, team_b: int) -> Tuple[Dict, int]:
            """Возвращает словарь серий между team_a и team_b и число матчей в окне."""
            key = tuple(sorted((team_a, team_b)))
            if key in edge_series_cache:
                series_dict = edge_series_cache[key]
            else:
                series_dict = defaultdict(list)
                pair_series_dict = self.pair_series.get(key, {})
                for series_id, matches in pair_series_dict.items():
                    for match in matches:
                        mt = match.get('startDateTime', 0)
                        if not (start_time <= mt <= end_time):
                            continue
                        series_dict[series_id].append(match)
                edge_series_cache[key] = series_dict
            match_count = sum(len(ms) for ms in series_dict.values())
            return series_dict, match_count

        def build_neighbors(team_id: int) -> Set[int]:
            if team_id in neighbors_cache:
                return neighbors_cache[team_id]
            opponents_series = defaultdict(lambda: defaultdict(list))
            for match in self.team_matches.get(team_id, []):
                mt = match.get('startDateTime', 0)
                if mt <= 0 or mt > end_time or mt < start_time:
                    continue
                radiant_team = match.get('radiantTeam', {})
                dire_team = match.get('direTeam', {})
                radiant_id = radiant_team.get('id')
                dire_id = dire_team.get('id')
                opponent = None
                if radiant_id == team_id:
                    opponent = dire_id
                elif dire_id == team_id:
                    opponent = radiant_id
                if not opponent:
                    continue
                series_id = match.get('seriesId', match.get('id'))
                opponents_series[opponent][series_id].append(match)
            valid_neighbors: Set[int] = set()
            for opponent, series_dict in opponents_series.items():
                match_count = sum(len(ms) for ms in series_dict.values())
                if match_count < MIN_EDGE_MATCHES:
                    continue
                valid_neighbors.add(opponent)
                key = tuple(sorted((team_id, opponent)))
                edge_series_cache[key] = series_dict
            neighbors_cache[team_id] = valid_neighbors
            return valid_neighbors

        seen_paths: Set[Tuple[int, ...]] = set()

        def dfs(current_path: List[int], edge_series_acc: List[Dict]):
            if len(connections) >= MAX_CONNECTIONS:
                return

            current_team = current_path[-1]
            edges_used = len(current_path) - 1
            if edges_used >= max_depth:
                return

            for neighbor in build_neighbors(current_team):
                if neighbor in current_path:
                    continue

                series_dict, match_count = get_series_for_pair(current_team, neighbor)
                if match_count < MIN_EDGE_MATCHES:
                    continue

                new_path = current_path + [neighbor]
                new_edge_series = edge_series_acc + [{
                    'teams': (current_team, neighbor),
                    'series': series_dict,
                }]

                if neighbor == team2_id:
                    if len(new_path) >= MIN_CHAIN_NODES:
                        path_key = tuple(new_path)
                        if path_key in seen_paths:
                            continue
                        seen_paths.add(path_key)
                        connections.append({
                            'path': new_path,
                            'edge_series': new_edge_series,
                        })
                    continue

                dfs(new_path, new_edge_series)

        dfs([team1_id], [])
        return connections
    
    def analyze_match_results(self, matches: List[Dict]) -> Dict:
        """Анализирует результаты матчей"""
        if not matches:
            return {'total': 0, 'wins': 0, 'losses': 0, 'win_rate': 0.0}
        
        total = len(matches)
        wins = 0
        
        for match in matches:
            # Здесь нужно определить, какая команда считается "нашей"
            # Это зависит от контекста вызова функции
            if match.get('didRadiantWin', False):
                wins += 1
        
        losses = total - wins
        win_rate = wins / total if total > 0 else 0.0
        
        return {
            'total': total,
            'wins': wins,
            'losses': losses,
            'win_rate': win_rate
        }

    # =========================================================================
    # УЛУЧШЕННЫЕ МЕТОДЫ: Quality-Weighted Common и Transitive
    # =========================================================================

    def compute_common_opponents_v2(
        self,
        team1_id: int,
        team2_id: int,
        as_of_timestamp: int,
        max_days: int = 21,
        elo_ratings: Optional[Dict[int, float]] = None,
    ) -> Tuple[float, int]:
        """Улучшенный Common Opponents с quality weighting.
        
        Особенности:
        - Quality weighting: противники с высоким Elo важнее
        - Более агрессивный decay (7 дней half-life)
        - Нормализация по весам
        
        Returns:
            (score, n_opponents): score положительный = team1 лучше
        """
        start_time = as_of_timestamp - max_days * 86400
        
        if elo_ratings is None:
            elo_ratings = self.compute_elo_ratings_up_to(
                as_of_timestamp, max_days=365, **DEFAULT_ELO_PARAMS
            )
        
        avg_elo = sum(elo_ratings.values()) / len(elo_ratings) if elo_ratings else 1500
        
        # Собираем данные по противникам для каждой команды
        t1_data: Dict[int, List[Tuple[int, float]]] = defaultdict(list)  # opp -> [(margin, decay)]
        t2_data: Dict[int, List[Tuple[int, float]]] = defaultdict(list)
        
        for team_id, data_dict in [(team1_id, t1_data), (team2_id, t2_data)]:
            for m in self.team_matches.get(team_id, []):
                mt = m.get('startDateTime', 0)
                if not (start_time <= mt < as_of_timestamp):
                    continue
                
                rad = m.get('radiantTeam') or {}
                dire = m.get('direTeam') or {}
                r_id, d_id = rad.get('id'), dire.get('id')
                
                opp_id = d_id if r_id == team_id else r_id
                if not opp_id or opp_id in (team1_id, team2_id):
                    continue
                
                rw = m.get('didRadiantWin')
                if r_id == team_id:
                    margin = 1 if rw else -1
                else:
                    margin = -1 if rw else 1
                
                age_days = (as_of_timestamp - mt) / 86400
                decay = 0.5 ** (age_days / COMMON_DECAY_HALF_LIFE_DAYS)
                
                data_dict[opp_id].append((margin, decay))
        
        # Общие противники
        common = set(t1_data.keys()) & set(t2_data.keys())
        if not common:
            return 0.0, 0
        
        score = 0.0
        total_weight = 0.0
        n_opps = 0
        
        for opp_id in common:
            opp_elo = elo_ratings.get(opp_id, 1500)
            quality = opp_elo / avg_elo  # >1 для сильных противников
            
            # Weighted performance для team1
            t1_perf = sum(m * d for m, d in t1_data[opp_id])
            t1_weight = sum(d for m, d in t1_data[opp_id])
            
            # Weighted performance для team2
            t2_perf = sum(m * d for m, d in t2_data[opp_id])
            t2_weight = sum(d for m, d in t2_data[opp_id])
            
            if t1_weight > 0 and t2_weight > 0:
                t1_norm = t1_perf / t1_weight
                t2_norm = t2_perf / t2_weight
                diff = (t1_norm - t2_norm) * quality
                
                opp_weight = min(t1_weight, t2_weight) * quality
                score += diff * opp_weight
                total_weight += opp_weight
                n_opps += 1
        
        if total_weight > 0:
            score = score / total_weight
        
        return score, n_opps

    def compute_transitive_chains_v2(
        self,
        team1_id: int,
        team2_id: int,
        as_of_timestamp: int,
        max_days: int = 21,
        elo_ratings: Optional[Dict[int, float]] = None,
        max_depth: int = 3,
        max_paths: int = 50,
    ) -> Tuple[float, int]:
        """Улучшенные Transitive chains с propagation и quality.
        
        Особенности:
        - Propagation: winrate перемножается по цепи
        - Quality weighting промежуточных команд
        - Length decay: короткие цепи важнее
        
        Returns:
            (score, n_chains): score положительный = team1 лучше
        """
        start_time = as_of_timestamp - max_days * 86400
        
        if elo_ratings is None:
            elo_ratings = self.compute_elo_ratings_up_to(
                as_of_timestamp, max_days=365, **DEFAULT_ELO_PARAMS
            )
        
        avg_elo = sum(elo_ratings.values()) / len(elo_ratings) if elo_ratings else 1500
        
        # Строим граф
        edges: Dict[Tuple[int, int], Dict[str, int]] = defaultdict(lambda: {'r_w': 0, 'd_w': 0})
        neighbors: Dict[int, Set[int]] = defaultdict(set)
        
        for m in self.matches_sorted:
            mt = m.get('startDateTime', 0)
            if mt >= as_of_timestamp or mt < start_time:
                continue
            
            rad = m.get('radiantTeam') or {}
            dire = m.get('direTeam') or {}
            r_id, d_id = rad.get('id'), dire.get('id')
            
            if not r_id or not d_id:
                continue
            
            rw = m.get('didRadiantWin')
            key = (r_id, d_id)
            if rw:
                edges[key]['r_w'] += 1
            else:
                edges[key]['d_w'] += 1
            
            neighbors[r_id].add(d_id)
            neighbors[d_id].add(r_id)
        
        # BFS для поиска путей
        paths: List[List[int]] = []
        queue: List[Tuple[int, List[int]]] = [(team1_id, [team1_id])]
        
        while queue and len(paths) < max_paths:
            cur, path = queue.pop(0)
            if len(path) > max_depth + 1:
                continue
            
            for nb in neighbors[cur]:
                if nb in path:
                    continue
                
                new_path = path + [nb]
                if nb == team2_id and len(new_path) >= 3:
                    paths.append(new_path)
                elif len(new_path) <= max_depth:
                    queue.append((nb, new_path))
        
        if not paths:
            return 0.0, 0
        
        score = 0.0
        n_chains = 0
        
        for path in paths:
            chain_wr = 1.0
            valid = True
            
            for i in range(len(path) - 1):
                a, b = path[i], path[i + 1]
                k1, k2 = (a, b), (b, a)
                
                wins = edges[k1]['r_w'] + edges[k2]['d_w']
                losses = edges[k1]['d_w'] + edges[k2]['r_w']
                total = wins + losses
                
                if total < 2:  # Минимум 2 матча
                    valid = False
                    break
                
                wr = wins / total
                quality = elo_ratings.get(b, 1500) / avg_elo
                chain_wr *= (0.5 + (wr - 0.5) * quality)
            
            if valid:
                length_decay = 1.0 / (len(path) - 1)
                score += (chain_wr - 0.5) * 2 * length_decay
                n_chains += 1
        
        if n_chains > 0:
            score = score / n_chains
        
        return score, n_chains
    
    def predict_winner(self, team1_stats: Dict, team2_stats: Dict) -> str:
        """Делает предсказание на основе статистики"""
        if team1_stats['total'] == 0 and team2_stats['total'] == 0:
            return "Недостаточно данных для предсказания"
        
        if team1_stats['total'] == 0:
            return f"Команда 2 (побед в {team2_stats['total']} матчах)"
        
        if team2_stats['total'] == 0:
            return f"Команда 1 (побед в {team1_stats['total']} матчах)"
        
        # Сравниваем винрейты
        if team1_stats['win_rate'] > team2_stats['win_rate']:
            return f"Команда 1 (винрейт: {team1_stats['win_rate']:.2%} vs {team2_stats['win_rate']:.2%})"
        elif team2_stats['win_rate'] > team1_stats['win_rate']:
            return f"Команда 2 (винрейт: {team2_stats['win_rate']:.2%} vs {team1_stats['win_rate']:.2%})"
        else:
            return "Ничья по статистике (равные винрейты)"

def get_transitiv(
    radiant_team_id: int,
    dire_team_id: int,
    radiant_team_name_original: str = None,
    dire_team_name_original: str = None,
    as_of_timestamp: Optional[int] = None,
    analyzer: 'TransitiveAnalyzer' = None,
    max_days: Optional[int] = None,
    weights: Optional[Dict[str, float]] = None,
    use_transitive: bool = True,
    use_elo_filter: bool = False,
    chain_weights: Optional[Dict[str, float]] = None,
    min_transitive_chains: Optional[int] = None,
    min_primary_chains: Optional[int] = None,
    verbose: bool = True,
) -> Dict:
    """
    Главная функция для получения предсказания на основе транзитивного анализа
    
    Args:
        radiant_team_id: ID команды Radiant
        dire_team_id: ID команды Dire
        radiant_team_name_original: Оригинальное имя команды Radiant (опционально)
        dire_team_name_original: Оригинальное имя команды Dire (опционально)
    
    Returns:
        Dict с полным объяснением прогноза:
        {
            'prediction': str,  # 'Radiant', 'Dire' или 'Ничья'
            'confidence': str,  # 'высокая', 'средняя' или 'низкая'
            'total_score': float,  # Итоговый счет (+ = Radiant, - = Dire)
            'h2h_score': float,  # Счет по прямым матчам
            'common_score': float,  # Счет по общим противникам
            'transitive_score': float,  # Счет по транзитивным связям
            'total_series': int,  # Всего серий найдено
            'h2h_series': int,  # Прямых матчей
            'common_series': int,  # Общих противников
            'transitive_series': int,  # Транзитивных связей
            'days_analyzed': int,  # Дней проанализировано
            'explanation': str,  # Текстовое объяснение
            'has_data': bool  # Есть ли достаточно данных
        }
    """
    analyzer = analyzer or TransitiveAnalyzer()
    current_time = datetime.fromtimestamp(as_of_timestamp) if as_of_timestamp else datetime.now()
    as_of_ts_int = int(current_time.timestamp())
    
    # Получаем имена команд для использования в цепях
    # Используем оригинальные имена если переданы, иначе берем из базы
    radiant_name = radiant_team_name_original if radiant_team_name_original else analyzer.get_team_name(radiant_team_id)
    dire_name = dire_team_name_original if dire_team_name_original else analyzer.get_team_name(dire_team_id)
    
    # Веса для разных типов связей
    if weights is None:
        # Оптимизированные веса: Elo доминирует, H2H/common сильно подавлены
        # Тесты показали что H2H добавляет шум и снижает accuracy
        WEIGHTS = {
            'head_to_head': 0.3,
            'common_opponents': 0.3,
            'transitive': 0.2,
            # Elo — основной сигнал (вес 3.0 даёт лучший результат)
            'elo': 3.0,
        }
    else:
        # Безопасно подставляем значения, если каких-то ключей нет, берём дефолт
        WEIGHTS = {
            'head_to_head': float(weights.get('head_to_head', 3.5)),
            'common_opponents': float(weights.get('common_opponents', 2.0)),
            'transitive': float(weights.get('transitive', 0.75)),
            'elo': float(weights.get('elo', 1.0)),
        }

    # Коэффициенты влияния счёта серии (2:0 ценится выше, чем 2:1)
    H2H_MARGIN_SCALE = 1.0
    COMMON_MARGIN_SCALE = 1.0
    TRANSITIVE_MARGIN_SCALE = 0.75

    # Веса информативности цепей (для base_confidence в strength)
    # По умолчанию считаем одну H2H/common-серию базовой единицей информации,
    # транзитивную цепь делаем слабее (будем подбирать коэффициент по данным).
    if chain_weights is None:
        CHAIN_WEIGHTS = {
            'h2h_chain': 1.0,
            'common_chain': 1.0,
            'trans_chain': 0.3,
        }
    else:
        CHAIN_WEIGHTS = {
            'h2h_chain': float(chain_weights.get('h2h_chain', 1.0)),
            'common_chain': float(chain_weights.get('common_chain', 1.0)),
            'trans_chain': float(chain_weights.get('trans_chain', 0.3)),
        }
    
    # Пороги для остановки поиска
    STOP_THRESHOLDS = {
        # Минимальное количество цепей (серий/связей), чтобы вообще принимать решение по H2H+common
        # Снижено с 6 до 2 после анализа — большинство матчей имеют мало H2H/common данных
        'min_chains': int(min_primary_chains) if min_primary_chains is not None else 2,
        # Число цепей, при котором считаем уверенность высокой
        'strong_confidence': 6,
        # Максимальная ширина окна поиска по времени (в днях)
        'max_days': int(max_days) if max_days else 30,
    }

    # Для транзитивных цепей требуем немного более высокий порог, чем для H2H+common,
    # так как это более шумный тип информации. По умолчанию берём min_chains + 2 (обычно 8),
    # но допускаем явное переопределение через параметр min_transitive_chains.
    if min_transitive_chains is not None:
        MIN_TRANSITIVE_CHAINS = int(min_transitive_chains)
    else:
        MIN_TRANSITIVE_CHAINS = max(STOP_THRESHOLDS['min_chains'] + 2, 8)

    # Динамические коэффициенты влияния источников
    H2H_PRIORITY_SERIES = 2
    H2H_WEAK_SCALE = 0.35
    COMMON_PRIORITY_SERIES = 3
    COMMON_WEAK_SCALE = 0.5
    TRANSITIVE_ASSIST_SERIES = 4
    TRANSITIVE_ASSIST_WEIGHT = 0.6

    # Порог сильного преимущества по Elo, при котором не хотим идти жёстко против рейтинга
    ELO_STRONG_EDGE = 150.0
    # Нормировочный масштаб для перевода разницы Elo в счёт модели:
    # разница ELO_STRONG_EDGE примерно соответствует ~1 "очковому" юниту до умножения на вес.
    ELO_SCALE = float(ELO_STRONG_EDGE)
    
    if verbose:
        print("=== ВЗВЕШЕННЫЙ АНАЛИЗ СВЯЗЕЙ ===")
        print(f"Команды: {radiant_name} (ID: {radiant_team_id}) vs {dire_name} (ID: {dire_team_id})")
        print(f"Веса: H2H={WEIGHTS['head_to_head']}, Общие={WEIGHTS['common_opponents']}, Транзитивные={WEIGHTS['transitive']}")
        print(f"Пороги: минимум {STOP_THRESHOLDS['min_chains']} цепей, высокая уверенность при {STOP_THRESHOLDS['strong_confidence']}+")
        print()
    
    # Переменные для накопления данных по всем дням
    all_h2h_details = []
    all_common_details = []
    all_transitive_details = []

    # Elo-рейтинги на момент as_of_timestamp.
    # Используем более длинный горизонт, чем для H2H/Common/Transitive, но с обрезкой по году:
    # max_days=365 → учитываем примерно последний год выступлений текущих ростеров.
    ELO_MAX_DAYS = ELO_HISTORY_DAYS
    elo_ratings = analyzer.compute_elo_ratings_up_to(
        as_of_ts_int,
        max_days=ELO_MAX_DAYS,
        **DEFAULT_ELO_PARAMS,
    )
    elo_radiant = elo_ratings.get(radiant_team_id, 1500.0)
    elo_dire = elo_ratings.get(dire_team_id, 1500.0)

    # Нормализованный Elo-скор: положительный = преимущество Radiant, отрицательный = Dire
    elo_diff = elo_radiant - elo_dire
    if ELO_SCALE > 0:
        elo_norm_raw = elo_diff / ELO_SCALE
    else:
        elo_norm_raw = 0.0
    # Ограничиваем влияние Elo, чтобы он не доминировал полностью
    elo_norm_clamped = max(min(elo_norm_raw, 3.0), -3.0)
    elo_score_norm = elo_norm_clamped * WEIGHTS['elo']

    # =========================================================================
    # НОВЫЕ ФИЧИ: Form, Momentum, Streak, Consistency, Side Stats
    # =========================================================================

    # Form (последние 10 игр)
    radiant_form = analyzer.compute_team_form(radiant_team_id, as_of_ts_int, n_games=10)
    dire_form = analyzer.compute_team_form(dire_team_id, as_of_ts_int, n_games=10)
    
    # Разница в форме (положительный = Radiant в лучшей форме)
    form_diff = radiant_form['weighted_form'] - dire_form['weighted_form']
    form_diff_raw = radiant_form['win_rate'] - dire_form['win_rate']
    
    # Streak (серия побед/поражений)
    radiant_streak = analyzer.compute_win_streak(radiant_team_id, as_of_ts_int)
    dire_streak = analyzer.compute_win_streak(dire_team_id, as_of_ts_int)
    
    # Разница в streak (положительный = Radiant на серии побед или Dire на серии поражений)
    streak_diff = radiant_streak['streak'] - dire_streak['streak']
    
    # Elo Momentum (изменение рейтинга за 14 дней)
    radiant_momentum = analyzer.compute_elo_momentum(radiant_team_id, as_of_ts_int, lookback_days=14)
    dire_momentum = analyzer.compute_elo_momentum(dire_team_id, as_of_ts_int, lookback_days=14)
    
    momentum_diff = radiant_momentum['elo_change'] - dire_momentum['elo_change']
    
    # Days since last match
    radiant_activity = analyzer.compute_days_since_last_match(radiant_team_id, as_of_ts_int)
    dire_activity = analyzer.compute_days_since_last_match(dire_team_id, as_of_ts_int)
    
    # Map margin stats (consistency)
    radiant_margin = analyzer.compute_map_margin_stats(radiant_team_id, as_of_ts_int, n_series=10)
    dire_margin = analyzer.compute_map_margin_stats(dire_team_id, as_of_ts_int, n_series=10)
    
    # Side stats (Radiant/Dire WR)
    radiant_side = analyzer.compute_side_stats(radiant_team_id, as_of_ts_int, n_games=20)
    dire_side = analyzer.compute_side_stats(dire_team_id, as_of_ts_int, n_games=20)
    
    # Opponent tier stats
    radiant_tier_stats = analyzer.compute_opponent_tier_stats(radiant_team_id, as_of_ts_int, n_games=20)
    dire_tier_stats = analyzer.compute_opponent_tier_stats(dire_team_id, as_of_ts_int, n_games=20)
    
    # =========================================================================
    # ПРОИЗВОДНЫЕ ФИЧИ: Signal Agreement, Combined Scores
    # =========================================================================
    
    # Нормализованные производные фичи для модели
    # Form score: разница в форме, нормализованная
    form_score = form_diff * 2.0  # масштабируем чтобы было в диапазоне примерно -1..+1
    
    # Momentum score: разница в momentum, нормализованная по ELO_SCALE
    momentum_score = momentum_diff / ELO_SCALE if ELO_SCALE > 0 else 0.0
    
    # Streak score: нормализованный streak (делим на 5 — типичная длинная серия)
    streak_score = streak_diff / 5.0
    
    # Consistency score: разница в consistency
    consistency_diff = radiant_margin['consistency'] - dire_margin['consistency']
    
    # Activity score: штраф за "холодную" команду
    radiant_cold_penalty = 0.1 if radiant_activity['is_cold'] else 0.0
    dire_cold_penalty = 0.1 if dire_activity['is_cold'] else 0.0
    activity_score = dire_cold_penalty - radiant_cold_penalty  # положительный если Dire "холоднее"
    
    # Side advantage: Radiant играет на Radiant, у него есть side_wr
    # Dire играет на Dire, у него есть свой side_wr
    # Преимущество Radiant если он хорошо играет за Radiant и/или Dire плохо играет за Dire
    side_advantage = (radiant_side['radiant_wr'] - 0.5) - (dire_side['dire_wr'] - 0.5)
    
    # Strength of schedule: если Radiant играл против более сильных команд, это хороший знак
    # (меньше = сильнее schedule)
    sos_diff = dire_tier_stats['strength_of_schedule'] - radiant_tier_stats['strength_of_schedule']
    
    if verbose:
        print(f"📊 НОВЫЕ ФИЧИ:")
        print(f"  Form: {radiant_name}={radiant_form['weighted_form']:.2f}, {dire_name}={dire_form['weighted_form']:.2f}, diff={form_diff:+.2f}")
        print(f"  Streak: {radiant_name}={radiant_streak['streak']:+d} ({radiant_streak['last_5_pattern']}), {dire_name}={dire_streak['streak']:+d} ({dire_streak['last_5_pattern']})")
        print(f"  Momentum (14d): {radiant_name}={radiant_momentum['elo_change']:+.1f}, {dire_name}={dire_momentum['elo_change']:+.1f}, diff={momentum_diff:+.1f}")
        print(f"  Activity: {radiant_name}={radiant_activity['days_since_last']:.1f}d, {dire_name}={dire_activity['days_since_last']:.1f}d")
        print(f"  Consistency: {radiant_name}={radiant_margin['consistency']:.2f}, {dire_name}={dire_margin['consistency']:.2f}")
        print()

    # Упакуем новые фичи в словарь для удобства
    new_features = {
        # Form
        'radiant_form': radiant_form['weighted_form'],
        'dire_form': dire_form['weighted_form'],
        'form_diff': form_diff,
        'radiant_form_raw': radiant_form['win_rate'],
        'dire_form_raw': dire_form['win_rate'],
        'form_diff_raw': form_diff_raw,
        'radiant_form_games': radiant_form['games_played'],
        'dire_form_games': dire_form['games_played'],
        
        # Streak
        'radiant_streak': radiant_streak['streak'],
        'dire_streak': dire_streak['streak'],
        'streak_diff': streak_diff,
        'radiant_streak_length': radiant_streak['streak_length'],
        'dire_streak_length': dire_streak['streak_length'],
        
        # Momentum
        'radiant_momentum': radiant_momentum['elo_change'],
        'dire_momentum': dire_momentum['elo_change'],
        'momentum_diff': momentum_diff,
        
        # Activity
        'radiant_days_since_last': radiant_activity['days_since_last'],
        'dire_days_since_last': dire_activity['days_since_last'],
        'radiant_is_cold': int(radiant_activity['is_cold']),
        'dire_is_cold': int(dire_activity['is_cold']),
        
        # Map margin / Consistency
        'radiant_consistency': radiant_margin['consistency'],
        'dire_consistency': dire_margin['consistency'],
        'consistency_diff': consistency_diff,
        'radiant_avg_margin': radiant_margin['avg_margin'],
        'dire_avg_margin': dire_margin['avg_margin'],
        'radiant_clean_sweep_rate': radiant_margin['clean_sweep_rate'],
        'dire_clean_sweep_rate': dire_margin['clean_sweep_rate'],
        
        # Side stats
        'radiant_radiant_wr': radiant_side['radiant_wr'],
        'radiant_dire_wr': radiant_side['dire_wr'],
        'dire_radiant_wr': dire_side['radiant_wr'],
        'dire_dire_wr': dire_side['dire_wr'],
        'side_advantage': side_advantage,
        
        # Tier stats
        'radiant_vs_tier1_wr': radiant_tier_stats['vs_tier1_wr'],
        'radiant_vs_tier2_wr': radiant_tier_stats['vs_tier2_wr'],
        'dire_vs_tier1_wr': dire_tier_stats['vs_tier1_wr'],
        'dire_vs_tier2_wr': dire_tier_stats['vs_tier2_wr'],
        'radiant_sos': radiant_tier_stats['strength_of_schedule'],
        'dire_sos': dire_tier_stats['strength_of_schedule'],
        'sos_diff': sos_diff,
        
        # Normalized scores
        'form_score': form_score,
        'momentum_score': momentum_score,
        'streak_score': streak_score,
        'activity_score': activity_score,
    }

    # =========================================================================
    # КОНЕЦ НОВЫХ ФИЧЕЙ
    # =========================================================================
    
    # Накопительные счетчики за все дни
    total_h2h_score = 0
    total_common_score = 0
    total_transitive_score = 0
    total_h2h_series = 0
    total_common_series = 0
    total_transitive_series = 0
    
    # Лучший найденный результат по мере расширения окна
    best_result = None
    best_strength = -1.0
    
        # Функция для определения победителя серий (вынесена за пределы цикла)
    def get_series_winner_from_dict(series_dict, team_id):
        """Определяет победителя на основе словаря серий {series_id: [matches]}
        Возвращает True если команда выиграла больше серий, False иначе
        """
        series_wins = 0
        series_losses = 0
        total_match_wins = 0
        total_match_losses = 0
        
        team_name = analyzer.get_team_name(team_id)
        
        for series_id, matches in series_dict.items():
            match_wins = 0
            match_losses = 0
            
            for match in matches:
                radiant_id = match.get('radiantTeam', {}).get('id')
                dire_id = match.get('direTeam', {}).get('id')
                radiant_won = match.get('didRadiantWin', False)
                
                team_won = (radiant_id == team_id and radiant_won) or (dire_id == team_id and not radiant_won)
                
                if team_won:
                    match_wins += 1
                    total_match_wins += 1
                else:
                    match_losses += 1
                    total_match_losses += 1
            
            # Определяем победителя серии
            if match_wins > match_losses:
                series_wins += 1
            elif match_losses > match_wins:
                series_losses += 1
            # Если match_wins == match_losses, серия не засчитывается ни в wins ни в losses
        
        # Возвращаем True если команда выиграла больше серий
        # ВАЖНО: если серии равны (1:1, 2:2), возвращаем False (нет четкого преимущества)
        return series_wins > series_losses

    def compute_margin_strength_for_team(series_dict, team_id, margin_scale: float = 1.0) -> float:
        """Суммарная "сила" серий team_id с учётом счёта по картам.

        Используется в транзитивных цепях: 2:0 даёт больший вклад, чем 2:1.
        """
        strength = 0.0
        for series_id, matches in series_dict.items():
            wins = 0
            losses = 0
            for match in matches:
                radiant_id = match.get('radiantTeam', {}).get('id')
                dire_id = match.get('direTeam', {}).get('id')
                radiant_won = match.get('didRadiantWin', False)
                team_won = (radiant_id == team_id and radiant_won) or (dire_id == team_id and not radiant_won)
                if team_won:
                    wins += 1
                else:
                    losses += 1
            total = wins + losses
            if wins > losses and total > 0:
                margin = wins - losses
                margin_frac = margin / total
                strength += 1.0 + margin_scale * margin_frac
        return strength

    def compute_average_decay(series_dict) -> float:
        """Взвешенная свежесть матча для ребра транзитивной цепи."""
        total = 0.0
        count = 0
        for matches in series_dict.values():
            for match in matches:
                mt = datetime.fromtimestamp(match.get('startDateTime', 0))
                age_days = (current_time - mt).total_seconds() / 86400.0
                total += 0.5 ** (age_days / DECAY_HALF_LIFE_DAYS)
                count += 1
        return (total / count) if count > 0 else 0.0
    
    # Ищем данные по фиксированным окнам: 1, 2, 3, 5, 7, 14 и max_days
    # Это даёт приоритет свежей информации, но позволяет использовать больше истории как fallback.
    candidate_days = [1, 2, 3, 5, 7, 14, STOP_THRESHOLDS['max_days']]
    days_list = sorted({d for d in candidate_days if d <= STOP_THRESHOLDS['max_days']})
    for days in days_list:
        if verbose:
            print(f"=== АНАЛИЗ ЗА {days} ДНЕЙ ===")
        
        # Создаем диапазон за ВСЕ дни до текущего (накопительно)
        start_time = current_time - timedelta(days=days)
        end_time = current_time
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp()) - 1
        
        if verbose:
            print(f"Период: {start_time.strftime('%Y-%m-%d')} - {end_time.strftime('%Y-%m-%d')} ({days} дней)")
        
        # 1. Ищем прямые матчи за весь период (группируем по сериям)
        h2h_series_list = analyzer.find_head_to_head(radiant_team_id, dire_team_id, start_timestamp, end_timestamp)
        h2h_score = 0
        h2h_series = len(h2h_series_list)
        h2h_total_radiant_maps = 0
        h2h_total_dire_maps = 0
        
        if h2h_series_list:
            if verbose:
                print(f"Найдено {h2h_series} прямых серий!")
                print("=== ПРЯМЫЕ МАТЧИ ===")
            
            radiant_series_wins = 0
            dire_series_wins = 0
            # Дополнительные взвешенные значения серий, чтобы 2:0 ценилось выше чем 2:1
            radiant_series_value = 0.0
            dire_series_value = 0.0
            total_radiant_map_wins = 0
            total_dire_map_wins = 0
            total_radiant_map_wins_w = 0.0
            total_dire_map_wins_w = 0.0
            
            # Очищаем старые детали перед добавлением новых (чтобы избежать дублей)
            all_h2h_details.clear()
            
            for series_data in h2h_series_list:
                series_id = series_data['series_id']
                matches = series_data['matches']
                
                if verbose:
                    print(f"  Серия ID: {series_id}, матчей: {len(matches)}")
                
                # Считаем победы в матчах серии и среднюю свежесть карт
                radiant_match_wins = 0
                dire_match_wins = 0
                series_decay_sum = 0.0
                series_decay_count = 0
                
                for match in matches:
                    match_time = datetime.fromtimestamp(match.get('startDateTime', 0))
                    radiant_team = match.get('radiantTeam', {})
                    dire_team = match.get('direTeam', {})
                    
                    # Определяем кто выиграл матч
                    is_radiant_win = (radiant_team.get('id') == radiant_team_id and match.get('didRadiantWin', False)) or \
                                    (dire_team.get('id') == radiant_team_id and not match.get('didRadiantWin', False))
                    
                    # Вес свежести матча (экспоненциальный спад)
                    age_days = (current_time - match_time).total_seconds() / 86400.0
                    decay = 0.5 ** (age_days / DECAY_HALF_LIFE_DAYS)
                    series_decay_sum += decay
                    series_decay_count += 1
                    if is_radiant_win:
                        radiant_match_wins += 1
                        total_radiant_map_wins += 1
                        total_radiant_map_wins_w += decay
                    else:
                        dire_match_wins += 1
                        total_dire_map_wins += 1
                        total_dire_map_wins_w += decay
                    
                    if verbose:
                        winner_name = radiant_name if is_radiant_win else dire_name
                        print(f"    Матч {match_time.strftime('%Y-%m-%d %H:%M')}: победа {winner_name}")
                    else:
                        winner_name = radiant_name if is_radiant_win else dire_name
                
                # Средняя свежесть серии (если по какой-то причине матчей нет, считаем 1.0)
                avg_decay = (series_decay_sum / series_decay_count) if series_decay_count > 0 else 1.0
                
                # Определяем победителя серии и учитываем разницу по картам
                total_maps_in_series = radiant_match_wins + dire_match_wins
                if radiant_match_wins > dire_match_wins:
                    radiant_series_wins += 1
                    margin = radiant_match_wins - dire_match_wins
                    margin_frac = (margin / total_maps_in_series) if total_maps_in_series > 0 else 0.0
                    # 2:0 (или 3:0) даёт заметно больший вклад, чем 2:1 (или 3:2)
                    value_contrib = (1.0 + H2H_MARGIN_SCALE * margin_frac) * avg_decay
                    radiant_series_value += value_contrib
                    series_winner = radiant_name
                    series_score = f"{radiant_match_wins}:{dire_match_wins}"
                    if verbose:
                        print(f"  → Серию выиграл {radiant_name} ({series_score}) [margin {margin}, value contrib {value_contrib:.2f}, decay {avg_decay:.2f}]")
                elif dire_match_wins > radiant_match_wins:
                    dire_series_wins += 1
                    margin = dire_match_wins - radiant_match_wins
                    margin_frac = (margin / total_maps_in_series) if total_maps_in_series > 0 else 0.0
                    value_contrib = (1.0 + H2H_MARGIN_SCALE * margin_frac) * avg_decay
                    dire_series_value += value_contrib
                    series_winner = dire_name
                    series_score = f"{dire_match_wins}:{radiant_match_wins}"
                    if verbose:
                        print(f"  → Серию выиграл {dire_name} ({series_score}) [margin {margin}, value contrib {value_contrib:.2f}, decay {avg_decay:.2f}]")
                else:
                    series_winner = "Ничья"
                    series_score = f"{radiant_match_wins}:{dire_match_wins}"
                    if verbose:
                        print(f"  → Ничья в серии ({series_score})")
                
                # Сохраняем детали для объяснения (ОДИН РАЗ на серию)
                all_h2h_details.append({
                    'series_id': series_id,
                    'score': series_score,
                    'winner': series_winner,
                    'radiant_maps': radiant_match_wins,
                    'dire_maps': dire_match_wins
                })
            
            h2h_total_radiant_maps = total_radiant_map_wins
            h2h_total_dire_maps = total_dire_map_wins
            
            # Вычисляем взвешенный счет для прямых матчей (по сериям, с учетом разницы по картам)
            # Положительный = Radiant сильнее, отрицательный = Dire сильнее
            series_value_diff = radiant_series_value - dire_series_value
            if abs(series_value_diff) > 1e-9:
                h2h_score = series_value_diff * WEIGHTS['head_to_head']
                if verbose:
                    if series_value_diff > 0:
                        print(f"  Преимущество {radiant_name}: +{h2h_score:.1f} очков (серий {radiant_series_wins}:{dire_series_wins}, value diff {series_value_diff:+.2f})")
                    else:
                        print(f"  Преимущество {dire_name}: {h2h_score:.1f} очков (серий {dire_series_wins}:{radiant_series_wins}, value diff {series_value_diff:+.2f})")
            else:
                h2h_score = 0
                if verbose:
                    print(f"  Ничья по прямым матчам: 0 очков (серий {radiant_series_wins}:{dire_series_wins})")
        
        # 2. Ищем общих противников за весь период (группируем по сериям)
        common_opponents = analyzer.find_common_opponents(radiant_team_id, dire_team_id, start_timestamp, end_timestamp)
        common_score = 0
        # common_series_counts будет считать именно СЕРИИ против общих соперников, а не количество команд
        common_series = 0
        # Инициализируем счётчик серий по общим соперникам, даже если их нет
        total_common_series_count = 0
        
        if common_opponents:
            if verbose:
                print(f"Найдено {len(common_opponents)} общих противников за {days} дней!")
                print("=== ОБЩИЕ ПРОТИВНИКИ ===")
            
            # Очищаем старые детали
            all_common_details.clear()
            
            radiant_opponent_wins = 0
            dire_opponent_wins = 0
            common_contrib_sum = 0
            total_common_series_count = 0
            
            for opponent_data in common_opponents:
                opponent = opponent_data['opponent']
                opponent_name = analyzer.get_team_name(opponent)
                team1_series_dict = opponent_data['team1_series']  # dict: series_id -> [matches]
                team2_series_dict = opponent_data['team2_series']
                
                if verbose:
                    print(f"  Общий противник: {opponent_name} (ID: {opponent})")
                
                # Считаем победы в сериях для команды 1 (radiant)
                team1_series_wins = 0
                team1_series_total = len(team1_series_dict)
                # Учитываем фактическое число серий против этого соперника
                total_common_series_count += team1_series_total
                team1_total_match_wins = 0
                team1_total_match_losses = 0
                team1_total_match_wins_w = 0.0
                team1_total_match_losses_w = 0.0
                team1_series_scores = []
                team1_series_value = 0.0
                
                for series_id, matches in team1_series_dict.items():
                    radiant_match_wins = 0
                    opponent_match_wins = 0
                    radiant_match_wins_w = 0.0
                    opponent_match_wins_w = 0.0
                    series_decay_sum = 0.0
                    series_decay_count = 0
                    
                    for match in matches:
                        match_radiant_id = match.get('radiantTeam', {}).get('id')
                        match_dire_id = match.get('direTeam', {}).get('id')
                        match_radiant_won = match.get('didRadiantWin', False)
                        match_time = datetime.fromtimestamp(match.get('startDateTime', 0))
                        age_days = (current_time - match_time).total_seconds() / 86400.0
                        decay = 0.5 ** (age_days / DECAY_HALF_LIFE_DAYS)
                        series_decay_sum += decay
                        series_decay_count += 1
                        
                        won = (match_radiant_id == radiant_team_id and match_radiant_won) or \
                              (match_dire_id == radiant_team_id and not match_radiant_won)
                        
                        if won:
                            radiant_match_wins += 1
                            radiant_match_wins_w += decay
                            team1_total_match_wins += 1
                            team1_total_match_wins_w += decay
                        else:
                            opponent_match_wins += 1
                            opponent_match_wins_w += decay
                            team1_total_match_losses += 1
                            team1_total_match_losses_w += decay
                    
                    team1_series_scores.append((radiant_match_wins, opponent_match_wins))
                    
                    # 2:0 по общему оппоненту ценится выше, чем 2:1, и более свежие серии важнее старых
                    total_w = radiant_match_wins_w + opponent_match_wins_w
                    avg_decay = (series_decay_sum / series_decay_count) if series_decay_count > 0 else 1.0
                    if radiant_match_wins_w > opponent_match_wins_w and total_w > 0:
                        margin_w = radiant_match_wins_w - opponent_match_wins_w
                        margin_frac = margin_w / total_w
                        value_contrib = (1.0 + COMMON_MARGIN_SCALE * margin_frac) * avg_decay
                        team1_series_value += value_contrib
                    
                    if radiant_match_wins > opponent_match_wins:
                        team1_series_wins += 1
                        if verbose:
                            print(f"    {radiant_name} vs {opponent_name} (серия {series_id}): {radiant_match_wins}:{opponent_match_wins} ✓")
                    else:
                        if verbose:
                            print(f"    {radiant_name} vs {opponent_name} (серия {series_id}): {radiant_match_wins}:{opponent_match_wins} ✗")
                
                # Считаем победы в сериях для команды 2 (dire)
                team2_series_wins = 0
                team2_series_total = len(team2_series_dict)
                total_common_series_count += team2_series_total
                team2_total_match_wins = 0
                team2_total_match_losses = 0
                team2_total_match_wins_w = 0.0
                team2_total_match_losses_w = 0.0
                team2_series_scores = []
                team2_series_value = 0.0
                
                for series_id, matches in team2_series_dict.items():
                    dire_match_wins = 0
                    opponent_match_wins = 0
                    dire_match_wins_w = 0.0
                    opponent_match_wins_w = 0.0
                    series_decay_sum = 0.0
                    series_decay_count = 0
                    
                    for match in matches:
                        match_radiant_id = match.get('radiantTeam', {}).get('id')
                        match_dire_id = match.get('direTeam', {}).get('id')
                        match_radiant_won = match.get('didRadiantWin', False)
                        match_time = datetime.fromtimestamp(match.get('startDateTime', 0))
                        age_days = (current_time - match_time).total_seconds() / 86400.0
                        decay = 0.5 ** (age_days / DECAY_HALF_LIFE_DAYS)
                        series_decay_sum += decay
                        series_decay_count += 1
                        
                        won = (match_radiant_id == dire_team_id and match_radiant_won) or \
                              (match_dire_id == dire_team_id and not match_radiant_won)
                        
                        if won:
                            dire_match_wins += 1
                            dire_match_wins_w += decay
                            team2_total_match_wins += 1
                            team2_total_match_wins_w += decay
                        else:
                            opponent_match_wins += 1
                            opponent_match_wins_w += decay
                            team2_total_match_losses += 1
                            team2_total_match_losses_w += decay
                    
                    team2_series_scores.append((dire_match_wins, opponent_match_wins))
                    
                    # 2:0 по общему оппоненту ценится выше, чем 2:1, и более свежие серии важнее старых
                    total_w = dire_match_wins_w + opponent_match_wins_w
                    avg_decay = (series_decay_sum / series_decay_count) if series_decay_count > 0 else 1.0
                    if dire_match_wins_w > opponent_match_wins_w and total_w > 0:
                        margin_w = dire_match_wins_w - opponent_match_wins_w
                        margin_frac = margin_w / total_w
                        value_contrib = (1.0 + COMMON_MARGIN_SCALE * margin_frac) * avg_decay
                        team2_series_value += value_contrib
                    
                    if dire_match_wins > opponent_match_wins:
                        team2_series_wins += 1
                        if verbose:
                            print(f"    {dire_name} vs {opponent_name} (серия {series_id}): {dire_match_wins}:{opponent_match_wins} ✓")
                    else:
                        if verbose:
                            print(f"    {dire_name} vs {opponent_name} (серия {series_id}): {dire_match_wins}:{opponent_match_wins} ✗")
                
                # Определяем кто сильнее против этого противника
                # 1. Сначала сравниваем количество выигранных серий
                # 2. Если равно - сравниваем общий счет матчей (учитываем качество побед)
                
                if team1_series_wins > team2_series_wins:
                    radiant_won = True
                    dire_won = False
                    radiant_opponent_wins += 1
                    if verbose:
                        print(f"    → {radiant_name} сильнее (выиграл {team1_series_wins}/{team1_series_total} серий vs {team2_series_wins}/{team2_series_total})")
                elif team2_series_wins > team1_series_wins:
                    radiant_won = False
                    dire_won = True
                    dire_opponent_wins += 1
                    if verbose:
                        print(f"    → {dire_name} сильнее (выиграл {team2_series_wins}/{team2_series_total} серий vs {team1_series_wins}/{team1_series_total})")
                else:
                    # Равное количество выигранных серий - смотрим на общий счет матчей
                    if verbose:
                        print(f"    → Серий поровну ({team1_series_wins}:{team2_series_wins}), смотрим на матчи: {team1_total_match_wins}:{team1_total_match_losses} vs {team2_total_match_wins}:{team2_total_match_losses}")
                    # Сравниваем разность побед-поражений (качество игры) с учетом давности
                    team1_diff = team1_total_match_wins_w - team1_total_match_losses_w
                    team2_diff = team2_total_match_wins_w - team2_total_match_losses_w
                    if team1_diff > team2_diff:
                        radiant_won = True
                        dire_won = False
                        radiant_opponent_wins += 1
                        if verbose:
                            print(f"    → {radiant_name} сильнее (лучшая разность: {team1_diff:+.1f} vs {team2_diff:+.1f})")
                    elif team2_diff > team1_diff:
                        radiant_won = False
                        dire_won = True
                        dire_opponent_wins += 1
                        if verbose:
                            print(f"    → {dire_name} сильнее (лучшая разность: {team2_diff:+.1f} vs {team1_diff:+.1f})")
                    else:
                        radiant_won = False
                        dire_won = False
                        if verbose:
                            print(f"    → Полная ничья (серий {team1_series_wins}:{team2_series_wins}, разность {team1_diff:+.1f} vs {team2_diff:+.1f})")
                # Вклад соперника в общий счет с учётом качества серий (2:0 ценится выше, чем 2:1)
                opponent_contrib = team1_series_value - team2_series_value
                # Симметрично учитываем и победы, и поражения: сильные 0-2 дают отрицательный вклад.
                # Ограничиваем вклад одного соперника, чтобы он не доминировал полностью.
                if opponent_contrib > 3:
                    opponent_contrib = 3
                elif opponent_contrib < -3:
                    opponent_contrib = -3
                common_contrib_sum += opponent_contrib
                
                # Сохраняем детали
                all_common_details.append({
                    'opponent': opponent,
                    'opponent_name': opponent_name,
                    'radiant_won': radiant_won,
                    'dire_won': dire_won,
                    'radiant_series': team1_series_wins,
                    'dire_series': team2_series_wins,
                    'radiant_matches': team1_total_match_wins,
                    'dire_matches': team2_total_match_wins,
                    'radiant_match_losses': team1_total_match_losses,
                    'dire_match_losses': team2_total_match_losses
                })
            
            # Вычисляем взвешенный счет для общих противников.
            # Нормализуем по КОЛИЧЕСТВУ СЕРИЙ: средний вклад на серию * вес метода.
            if total_common_series_count > 0 and abs(common_contrib_sum) > 1e-9:
                avg_common_contrib = common_contrib_sum / float(total_common_series_count)
                common_score = avg_common_contrib * WEIGHTS['common_opponents']
                if verbose:
                    if common_score > 0:
                        print(f"  Преимущество {radiant_name} по общим противникам: +{common_score:.1f} очков")
                    else:
                        print(f"  Преимущество {dire_name} по общим противникам: {common_score:.1f} очков")
            else:
                common_score = 0
                if verbose:
                    print(f"  Ничья по общим противникам: 0 очков")
        
        # 3. Ищем транзитивные связи за весь период (если включены)
        transitive_score = 0
        transitive_series = 0  # Будем считать только валидные цепи
        
        transitive_connections = []
        if use_transitive:
            transitive_connections = analyzer.find_transitive_connections(
                radiant_team_id,
                dire_team_id,
                start_timestamp,
                end_timestamp,
                max_depth=4,
            )
        
        # H2H + общие противники считаются основным источником решения,
        # транзитивные связи используются только как fallback, когда primary-информации мало.
        
        if transitive_connections and verbose:
            print(f"Найдено {len(transitive_connections)} потенциальных транзитивных связей за {days} дней!")
            print("=== ТРАНЗИТИВНЫЕ СВЯЗИ ===")
            
            # Очищаем старые детали
            all_transitive_details.clear()
            
            radiant_transitive_points = 0.0
            dire_transitive_points = 0.0

            def evaluate_chain_direction(nodes: List[int], edge_lookup: Dict[frozenset, Dict]) -> Optional[Dict]:
                if len(nodes) < 4:
                    return None
                margins: List[float] = []
                decays: List[float] = []
                for idx in range(len(nodes) - 1):
                    a = nodes[idx]
                    b = nodes[idx + 1]
                    edge = edge_lookup.get(frozenset((a, b)))
                    if not edge:
                        return None
                    if not edge['wins'].get(a):
                        return None
                    margin = edge['margins'].get(a, 0.0)
                    if margin <= 0:
                        return None
                    margins.append(margin)
                    decays.append(edge['decay'])
                if not margins or not decays:
                    return None
                base_strength = min(margins)
                freshness = min(decays)
                if base_strength <= 0 or freshness <= 0:
                    return None
                chain_desc = " > ".join(analyzer.get_team_name(t) for t in nodes)
                return {
                    'points': base_strength * freshness,
                    'chain_desc': chain_desc,
                    'winner_id': nodes[0],
                }
            
            for conn in transitive_connections:
                path = conn.get('path') or []
                if not path or path[0] != radiant_team_id or path[-1] != dire_team_id:
                    continue
                if len(path) < 4:
                    continue
                if len(set(path)) < len(path):
                    chain_names = " > ".join(analyzer.get_team_name(t) for t in path)
                    if verbose:
                        print(f"  ❌ ПРОПУСКАЕМ цепь {chain_names}: повтор команды в цепи")
                    continue

                edge_series_list = conn.get('edge_series', [])
                if len(edge_series_list) != len(path) - 1:
                    continue

                chain_names = [analyzer.get_team_name(t) for t in path]
                if verbose:
                    print(f"  Транзитивная цепь ({len(path) - 2} промежуточных): {' > '.join(chain_names)}")
                
                edge_lookup: Dict[frozenset, Dict] = {}
                edge_data_valid = True
                for edge in edge_series_list:
                    teams = edge.get('teams')
                    series_dict = edge.get('series', {})
                    if not teams or not series_dict:
                        edge_data_valid = False
                        break
                    decay_value = compute_average_decay(series_dict)
                    edge_lookup[frozenset(teams)] = {
                        'teams': teams,
                        'series': series_dict,
                        'decay': decay_value,
                        'wins': {
                            teams[0]: get_series_winner_from_dict(series_dict, teams[0]),
                            teams[1]: get_series_winner_from_dict(series_dict, teams[1]),
                        },
                        'margins': {
                            teams[0]: compute_margin_strength_for_team(series_dict, teams[0], TRANSITIVE_MARGIN_SCALE),
                            teams[1]: compute_margin_strength_for_team(series_dict, teams[1], TRANSITIVE_MARGIN_SCALE),
                        },
                    }
                if not edge_data_valid:
                    if verbose:
                        print("    → ⚠️  Недостаточно данных на одном из рёбер (пропускаем)")
                    continue

                reverse_result = evaluate_chain_direction(list(reversed(path)), edge_lookup)
                forward_result = evaluate_chain_direction(path, edge_lookup)

                chain_result = None
                winner_side = None
                if reverse_result:
                    chain_result = reverse_result
                    winner_side = 'dire'
                elif forward_result:
                    chain_result = forward_result
                    winner_side = 'radiant'

                if not chain_result or not winner_side:
                    if verbose:
                        print("    → ⚠️  НЕПОЛНАЯ ЦЕПЬ (пропускаем)")
                    continue

                points_awarded = chain_result['points']
                chain_desc = chain_result['chain_desc']
                if winner_side == 'dire':
                    dire_transitive_points += points_awarded
                    winner_name = dire_name
                else:
                    radiant_transitive_points += points_awarded
                    winner_name = radiant_name

                if verbose:
                    print(f"    → ✅ ПОЛНАЯ ЦЕПЬ: {chain_desc}: +{points_awarded:.2f} для {winner_name}")
                
                transitive_series += 1
                all_transitive_details.append({
                    'chain': chain_desc,
                    'points': points_awarded,
                    'winner': winner_name
                })
            
            # Вычисляем итоговый счет
            if radiant_transitive_points > dire_transitive_points:
                transitive_score = (radiant_transitive_points - dire_transitive_points) * WEIGHTS['transitive']
                if verbose:
                    print(f"  Преимущество Radiant по транзитивным связям: +{transitive_score:.1f} очков")
            elif dire_transitive_points > radiant_transitive_points:
                transitive_score = -(dire_transitive_points - radiant_transitive_points) * WEIGHTS['transitive']
                if verbose:
                    print(f"  Преимущество Dire по транзитивным связям: {transitive_score:.1f} очков")
            else:
                transitive_score = 0
                if verbose:
                    print(f"  Ничья по транзитивным связям: 0 очков")
        
        # 4. Обновляем накопительные счетчики
        # Здесь total_common_series теперь интерпретируем как ОБЩЕЕ число серий против общих соперников
        total_h2h_series = h2h_series
        total_common_series = total_common_series_count
        total_transitive_series = transitive_series
        total_h2h_score = h2h_score
        total_common_score = common_score
        total_transitive_score = transitive_score
        
        # Нормализованные скоры: средний вклад на одну серию/цепь
        h2h_score_norm = (total_h2h_score / total_h2h_series) if total_h2h_series > 0 else 0.0
        # Для общих соперников common_score уже приведён к "среднему на серию" внутри блока выше
        common_score_norm = total_common_score
        transitive_score_norm = (total_transitive_score / total_transitive_series) if total_transitive_series > 0 else 0.0
        
        # Считаем общее количество цепей (каждый тип считается как отдельная цепь)
        total_chains = total_h2h_series + total_common_series + total_transitive_series
        
        # Разделяем "первичную" информацию (H2H+общие) и транзитивную
        # Для primary_series берём реальное количество СЕРИЙ: H2H-серии + серий против общих соперников
        primary_series = total_h2h_series + total_common_series
        
        # Балансируем вклад H2H и общих соперников по объёму информации:
        # компонента с 1–2 сериями не должна полностью переезжать компонент с десятками серий.
        h2h_info = float(total_h2h_series)
        common_info = float(total_common_series)
        total_primary_info = h2h_info + common_info
        if total_primary_info > 0:
            # Доля информации компонента среди всех primary-серий
            h2h_frac = h2h_info / total_primary_info
            common_frac = common_info / total_primary_info
            # Используем корень, чтобы влияние объёма было ощутимым, но не жёстким (diminishing returns)
            h2h_volume_factor = h2h_frac ** 0.5 if h2h_frac > 0 else 0.0
            common_volume_factor = common_frac ** 0.5 if common_frac > 0 else 0.0
        else:
            h2h_volume_factor = 0.0
            common_volume_factor = 0.0

        # Усиливаем H2H когда серий достаточно и подавляем шум, если серия одна
        if total_h2h_series >= H2H_PRIORITY_SERIES:
            h2h_conf_scale = 1.0
        elif total_h2h_series > 0:
            h2h_conf_scale = H2H_WEAK_SCALE
        else:
            h2h_conf_scale = 0.0

        if total_common_series >= COMMON_PRIORITY_SERIES:
            common_conf_scale = 1.0
        elif total_common_series > 0:
            common_conf_scale = COMMON_WEAK_SCALE
        else:
            common_conf_scale = 0.0

        h2h_score_effective = h2h_score_norm * h2h_volume_factor * h2h_conf_scale
        common_score_effective = common_score_norm * common_volume_factor * common_conf_scale

        transitive_assist = 0.0
        primary_transitive_used = False
        if (
            total_transitive_series >= TRANSITIVE_ASSIST_SERIES
            and primary_series < STOP_THRESHOLDS['min_chains']
        ):
            transitive_assist = transitive_score_norm * TRANSITIVE_ASSIST_WEIGHT
            primary_transitive_used = True

        # primary_score считаем как сумму эффективных скоров двух компонентов + Elo + Activity
        # Activity weight найден через grid search (0.6)
        ACTIVITY_WEIGHT = 0.6
        primary_score = h2h_score_effective + common_score_effective + transitive_assist + elo_score_norm + activity_score * ACTIVITY_WEIGHT
        
        # Суммарное количество всех цепей (для информации / объяснения)
        total_chains = total_h2h_series + total_common_series + total_transitive_series

        if verbose:
            print(f"=== ИТОГИ ДНЯ {days} ===")
            print(f"  Прямые матчи: {total_h2h_series} серий, счет: {total_h2h_score:+.1f}")
            print(f"  Общие противники: {total_common_series} серий против общих соперников, счет: {total_common_score:+.1f}")
            print(f"  Транзитивные связи: {total_transitive_series} цепей, счет: {total_transitive_score:+.1f}")
            print(f"  Всего цепей: {total_chains}")
            primary_components_desc = "H2H+общие"
            if primary_transitive_used:
                primary_components_desc += "+Trans"
            primary_components_desc += "+Elo"
            print(f"  Primary-скор ({primary_components_desc}): {primary_score:+.1f}")
            if use_transitive and total_transitive_series > 0:
                trans_decision_score = transitive_score_norm + elo_score_norm
                print(f"  Transitive-скор (транзитивка+Elo): {trans_decision_score:+.1f}")
            print(f"  Elo: {elo_radiant:.1f} vs {elo_dire:.1f} (diff={elo_diff:+.1f}, вклад Elo={elo_score_norm:+.2f})")

        # В этом окне формируем до двух кандидатов: primary и transitive.
        # Затем по всем окнам выберем кандидата с максимальным strength.

        def build_candidate(
            decision_mode: str,
            info_units: float,
            raw_chains: int,
            final_score: float,
            prediction: str,
            extra_methods: Optional[List[str]] = None,
        ):
            """Строит кандидата (primary или transitive) с расчётом strength и текстовым объяснением.

            info_units — взвешенное количество информации (с учётом CHAIN_WEIGHTS),
            raw_chains — фактическое число серий/цепей данного типа.

            Возвращает dict или None, если кандидат отфильтрован (например, по Elo).
            """
            nonlocal best_result, best_strength, primary_transitive_used

            # Опциональный Elo-фильтр: если модель сильно противоречит Elo при большом отрыве, можно не давать сигнал
            if use_elo_filter and decision_mode in ("primary", "transitive") and prediction != "Ничья":
                elo_sign_local = 1 if elo_diff > 0 else -1 if elo_diff < 0 else 0
                score_sign = 1 if final_score > 0 else -1 if final_score < 0 else 0
                if elo_sign_local != 0 and score_sign != 0 and elo_sign_local != score_sign and abs(elo_diff) >= ELO_STRONG_EDGE:
                    if verbose:
                        print(f"  ⚠️ Конфликт с Elo (elo_diff={elo_diff:+.1f}) для режима {decision_mode}, фильтруем сигнал")
                    return None

            # Базовая уверенность по объёму информации (в информационных единицах, а не в сырых цепях)
            # Для простоты используем общий порог STRONG_UNITS для всех режимов,
            # различие информативности задаётся через CHAIN_WEIGHTS.
            STRONG_UNITS = STOP_THRESHOLDS['strong_confidence']
            base_confidence = min(info_units / STRONG_UNITS, 1.0) if STRONG_UNITS > 0 else 0.0
            # Масштаб для нормализованных скоров: считаем, что |final_score| ~ 1.5 уже очень сильный сигнал
            score_confidence = min(abs(final_score) / 1.5, 1.0)
            strength = (base_confidence * 0.6 + score_confidence * 0.4)

            # Переводим strength в дискретную метку уверенности для внешнего использования
            if strength >= 0.75:
                confidence_label = "высокая"
            elif strength >= 0.5:
                confidence_label = "средняя"
            else:
                confidence_label = "низкая"

            if verbose:
                print(f"  РЕШЕНИЕ ({decision_mode}): {prediction} (уверенность: {confidence_label}, strength={strength:.2f}, цепей={raw_chains}, units={info_units:.2f})")
                print()

            # Формируем детальное объяснение
            explanation = f"🎯 ПРОГНОЗ: {prediction}\n"
            explanation += f"📊 Уверенность: {confidence_label}\n"
            explanation += f"⚖️ Итоговый счет: {final_score:+.1f}\n"
            explanation += f"   (положительный = {radiant_name}, отрицательный = {dire_name})\n"
            explanation += f"   Elo: {elo_radiant:.1f} vs {elo_dire:.1f} (diff={elo_diff:+.1f}, вклад Elo={elo_score_norm:+.2f})\n\n"

            explanation += f"📈 ДЕТАЛИ АНАЛИЗА ({days} дней):\n"

            # Прямые матчи
            if total_h2h_series > 0:
                explanation += f"\n🥊 ПРЯМЫЕ МАТЧИ ({total_h2h_series} серий, вес x{WEIGHTS['head_to_head']}):\n"
                explanation += f"   Счет: {total_h2h_score:+.1f}\n"
                explanation += f"   Итого по картам: {radiant_name} {h2h_total_radiant_maps}-{h2h_total_dire_maps}\n"
                for detail in all_h2h_details:
                    explanation += f"   • Серия {detail['series_id']}: {detail['score']} - победа {detail['winner']}\n"

            # Общие противники
            if total_common_series > 0:
                explanation += f"\n🤝 ОБЩИЕ ПРОТИВНИКИ ({total_common_series} серий против общих соперников, вес x{WEIGHTS['common_opponents']}):\\n"
                explanation += f"   Счет: {total_common_score:+.1f}\n"
                radiant_common_wins = sum(1 for d in all_common_details if d['radiant_won'])
                dire_common_wins = sum(1 for d in all_common_details if d['dire_won'])
                explanation += f"   {radiant_name} побед: {radiant_common_wins}, {dire_name} побед: {dire_common_wins}\n"
                total_r_w = sum(d.get('radiant_matches', 0) for d in all_common_details)
                total_r_l = sum(d.get('radiant_match_losses', 0) for d in all_common_details)
                total_d_w = sum(d.get('dire_matches', 0) for d in all_common_details)
                total_d_l = sum(d.get('dire_match_losses', 0) for d in all_common_details)
                explanation += f"   Итого по картам: {radiant_name} {total_r_w}-{total_r_l}, {dire_name} {total_d_w}-{total_d_l}\n"
                for detail in all_common_details[:5]:  # Показываем первые 5
                    r_w = detail.get('radiant_matches', 0)
                    r_l = detail.get('radiant_match_losses', 0)
                    d_w = detail.get('dire_matches', 0)
                    d_l = detail.get('dire_match_losses', 0)
                    explanation += f"   • {detail['opponent_name']}: "
                    explanation += f"{radiant_name} {r_w}-{r_l} по картам, "
                    explanation += f"{dire_name} {d_w}-{d_l} по картам"
                    if detail['radiant_won']:
                        explanation += f" → {radiant_name} сильнее\n"
                    elif detail['dire_won']:
                        explanation += f" → {dire_name} сильнее\n"
                    else:
                        explanation += f" → ничья\n"
                if len(all_common_details) > 5:
                    explanation += f"   ... и еще {len(all_common_details) - 5}\n"

            # Транзитивные связи
            if total_transitive_series > 0 and all_transitive_details:
                explanation += f"\n🔗 ТРАНЗИТИВНЫЕ СВЯЗИ ({len(all_transitive_details)} цепей, вес x{WEIGHTS['transitive']}):\n"
                explanation += f"   Счет: {total_transitive_score:+.1f}\n"
                for detail in all_transitive_details[:5]:  # Показываем первые 5
                    explanation += f"   • {detail['chain']}: +{detail['points']} для {detail['winner']}\n"
                if len(all_transitive_details) > 5:
                    explanation += f"   ... и еще {len(all_transitive_details) - 5}\n"

            explanation += f"\n💡 ИТОГО: {total_chains} цепей проанализировано за {days} дней"

            # Определяем методы которые использовались
            methods_used = []
            if total_h2h_series > 0:
                methods_used.append('H2H')
            if total_common_series > 0:
                methods_used.append('Common')
            if total_transitive_series > 0 and (
                decision_mode == "transitive" or (decision_mode == "primary" and primary_transitive_used)
            ):
                methods_used.append('Transitive')
            if extra_methods:
                for method in extra_methods:
                    if method not in methods_used:
                        methods_used.append(method)

            # Signal Agreement: проверяем согласованность сигналов
            h2h_sign = 1 if h2h_score_norm > 0.1 else (-1 if h2h_score_norm < -0.1 else 0)
            common_sign = 1 if common_score_norm > 0.1 else (-1 if common_score_norm < -0.1 else 0)
            trans_sign_local = 1 if transitive_score_norm > 0.1 else (-1 if transitive_score_norm < -0.1 else 0)
            elo_sign_local = 1 if elo_diff > 30 else (-1 if elo_diff < -30 else 0)
            form_sign = 1 if form_diff > 0.1 else (-1 if form_diff < -0.1 else 0)
            
            # Подсчёт согласия между ненулевыми сигналами
            signals = [s for s in [h2h_sign, common_sign, elo_sign_local, form_sign] if s != 0]
            if len(signals) >= 2:
                # Все согласны если все знаки одинаковые
                all_same = all(s == signals[0] for s in signals)
                signals_agree = 1 if all_same else 0
                # Подсчёт конфликтов
                pos_count = sum(1 for s in signals if s > 0)
                neg_count = sum(1 for s in signals if s < 0)
                signal_conflict = 1 if (pos_count > 0 and neg_count > 0) else 0
            else:
                signals_agree = 0
                signal_conflict = 0
            
            # H2H vs Elo conflict (важный индикатор)
            h2h_elo_conflict = 1 if (h2h_sign != 0 and elo_sign_local != 0 and h2h_sign != elo_sign_local) else 0
            
            candidate = {
                'prediction': prediction,
                'winner': prediction,
                'strength': strength,
                'confidence': base_confidence,
                'confidence_label': confidence_label,
                'period_days': days,
                'days_analyzed': days,
                'methods_used': methods_used,
                'total_score': final_score,
                'h2h_score': h2h_score_norm,
                'common_score': common_score_norm,
                'transitive_score': transitive_score_norm,
                'h2h_score_raw': total_h2h_score,
                'common_score_raw': total_common_score,
                'transitive_score_raw': total_transitive_score,
                'total_series': raw_chains,
                'info_units': info_units,
                'h2h_series': total_h2h_series,
                'common_series': total_common_series,
                'transitive_series': total_transitive_series,
                'elo_radiant': elo_radiant,
                'elo_dire': elo_dire,
                'elo_diff': elo_diff,
                'elo_score': elo_score_norm,
                'explanation': explanation,
                'has_data': True,
                'message': f"Найдено {total_chains} цепей за {days} дней",
                'decision_mode': decision_mode,
                # Signal Agreement
                'signals_agree': signals_agree,
                'signal_conflict': signal_conflict,
                'h2h_elo_conflict': h2h_elo_conflict,
                # Новые фичи
                **new_features,
            }

            if strength > best_strength or (
                best_result is None
                or (abs(strength - best_strength) < 1e-9 and abs(final_score) > abs(best_result['total_score']))
            ):
                best_result = candidate
                best_strength = strength

            return candidate

        # 1) Кандидат PRIMARY (H2H + общие + Elo)
        if primary_series >= STOP_THRESHOLDS['min_chains']:
            primary_final_score = primary_score
            primary_prediction = (
                radiant_name if primary_final_score > 0 else dire_name if primary_final_score < 0 else "Ничья"
            )
            primary_info_units = (
                CHAIN_WEIGHTS['h2h_chain'] * float(total_h2h_series)
                + CHAIN_WEIGHTS['common_chain'] * float(total_common_series)
            )
            build_candidate(
                decision_mode="primary",
                info_units=primary_info_units,
                raw_chains=primary_series,
                final_score=primary_final_score,
                prediction=primary_prediction,
            )

        # 2) Кандидат TRANSITIVE (транзитивные связи + Elo)
        if use_transitive and total_transitive_series >= MIN_TRANSITIVE_CHAINS:
            trans_final_score = transitive_score_norm + elo_score_norm + activity_score * ACTIVITY_WEIGHT
            trans_prediction = (
                radiant_name if trans_final_score > 0 else dire_name if trans_final_score < 0 else "Ничья"
            )

            # Дополнительная защита: транзитивное решение не должно идти жёстко против Elo,
            # когда Elo показывает сильный перекос. В таком случае не используем транзитивного кандидата.
            trans_sign = 1 if transitive_score_norm > 0 else -1 if transitive_score_norm < 0 else 0
            elo_sign = 1 if elo_diff > 0 else -1 if elo_diff < 0 else 0
            if (
                elo_sign != 0
                and trans_sign != 0
                and elo_sign != trans_sign
                and abs(elo_diff) >= ELO_STRONG_EDGE
            ):
                if verbose:
                    print(
                        f"  ⚠️ Транзитивный режим противоречит сильному сигналу Elo (diff={elo_diff:+.1f}), "
                        "кандидат transitive отброшен"
                    )
            else:
                trans_info_units = CHAIN_WEIGHTS['trans_chain'] * float(total_transitive_series)
                build_candidate(
                    decision_mode="transitive",
                    info_units=trans_info_units,
                    raw_chains=total_transitive_series,
                    final_score=trans_final_score,
                    prediction=trans_prediction,
                )

        # Если в этом окне не создано ни одного кандидата, просто двигаемся дальше
        if (
            primary_series < STOP_THRESHOLDS['min_chains']
            and (not use_transitive or total_transitive_series < MIN_TRANSITIVE_CHAINS)
        ):
            if verbose:
                print(
                    f"  Недостаточно данных для решения в этом окне (primary серий={primary_series}, "
                    f"transitive цепей={total_transitive_series})"
                )
                print()
    
    # Возвращаем лучший найденный результат, если есть
    if best_result is not None:
        return best_result

    # Fallback: если вообще нет H2H/Common/Transitive цепей, но Elo даёт ненулевую разницу,
    # возвращаем Elo-only кандидат с низкой/средней уверенность вместо "нет данных".
    if abs(elo_diff) > 0:
        # Базовая уверенность по величине Elo-разницы
        elo_conf = min(abs(elo_diff) / ELO_STRONG_EDGE, 1.0) if ELO_STRONG_EDGE > 0 else 0.0
        strength = elo_conf  # здесь strength ~ доверие только к Elo
        if strength >= 0.75:
            confidence_label = "высокая"
        elif strength >= 0.5:
            confidence_label = "средняя"
        else:
            confidence_label = "низкая"

        if elo_diff > 0:
            winner_name = radiant_name
        elif elo_diff < 0:
            winner_name = dire_name
        else:
            winner_name = "Ничья"

        explanation = (
            "🎯 ПРОГНОЗ (только Elo): " f"{winner_name}\n"
            f"📊 Уверенность: {confidence_label} (strength={strength:.2f})\n"
            f"⚖️ Итоговый счет (Elo): {elo_score_norm:+.2f}\n"
            f"   Radiant Elo: {elo_radiant:.1f}, Dire Elo: {elo_dire:.1f}, diff={elo_diff:+.1f}\n\n"
            f"💡 Источники: только глобальный Elo-рейтинг за последние {STOP_THRESHOLDS['max_days']} дней; "
            "H2H/Common/Transitive цепей не найдено."
        )

        # Signal agreement для elo_only (только form vs elo)
        elo_sign_fb = 1 if elo_diff > 30 else (-1 if elo_diff < -30 else 0)
        form_sign_fb = 1 if form_diff > 0.1 else (-1 if form_diff < -0.1 else 0)
        signals_agree_fb = 1 if (elo_sign_fb != 0 and form_sign_fb != 0 and elo_sign_fb == form_sign_fb) else 0

        return {
            'prediction': winner_name,
            'winner': winner_name,
            'strength': strength,
            'confidence': elo_conf,
            'confidence_label': confidence_label,
            'period_days': STOP_THRESHOLDS['max_days'],
            'days_analyzed': STOP_THRESHOLDS['max_days'],
            'methods_used': ['Elo'],
            'total_score': elo_score_norm,
            'h2h_score': 0.0,
            'common_score': 0.0,
            'transitive_score': 0.0,
            'total_series': 0,
            'info_units': 0.0,
            'h2h_series': 0,
            'common_series': 0,
            'transitive_series': 0,
            'elo_radiant': elo_radiant,
            'elo_dire': elo_dire,
            'elo_diff': elo_diff,
            'elo_score': elo_score_norm,
            'explanation': explanation,
            'has_data': True,
            'message': (
                "Использован только Elo рейтинг; H2H/Common/Transitive данных "
                f"нет за {STOP_THRESHOLDS['max_days']} дней"
            ),
            'decision_mode': 'elo_only',
            # Signal Agreement
            'signals_agree': signals_agree_fb,
            'signal_conflict': 0,
            'h2h_elo_conflict': 0,
            # Новые фичи
            **new_features,
        }

    # Если не нашли достаточно данных вообще (даже Elo не отличает команды)
    return {
        'prediction': 'Unknown',
        'winner': 'Unknown',
        'strength': 0.0,
        'confidence': 0.0,
        'confidence_label': 'низкая',
        'period_days': STOP_THRESHOLDS['max_days'],
        'days_analyzed': STOP_THRESHOLDS['max_days'],
        'methods_used': [],
        'total_score': 0.0,
        'h2h_score': 0.0,
        'common_score': 0.0,
        'transitive_score': 0.0,
        'total_series': 0,
        'info_units': 0.0,
        'h2h_series': 0,
        'common_series': 0,
        'transitive_series': 0,
        'elo_radiant': elo_radiant,
        'elo_dire': elo_dire,
        'elo_diff': elo_diff,
        'elo_score': elo_score_norm,
        'explanation': (
            "❌ Недостаточно данных для предсказания\n" 
            f"Найдено менее {STOP_THRESHOLDS['min_chains']} цепей за "
            f"{STOP_THRESHOLDS['max_days']} дней и Elo не даёт различий"
        ),
        'has_data': False,
        'message': (
            "Недостаточно данных (< " f"{STOP_THRESHOLDS['min_chains']} "
            f"цепей за {STOP_THRESHOLDS['max_days']} дней и Elo≈равный)"
        ),
        # Signal Agreement
        'signals_agree': 0,
        'signal_conflict': 0,
        'h2h_elo_conflict': 0,
        # Новые фичи
        **new_features,
    }

def get_pre_draft_prior(
    radiant_team_id: int,
    dire_team_id: int,
    as_of_timestamp: Optional[int] = None,
    analyzer: Optional[TransitiveAnalyzer] = None,
    max_days: Optional[int] = 30,
    use_transitive: bool = True,
    radiant_team_name_original: Optional[str] = None,
    dire_team_name_original: Optional[str] = None,
) -> Dict:
    """Упрощённый API для пре-драфт анализа силы команд.

    Возвращает компактный prior по карте без подробного консольного вывода.
    """
    res = get_transitiv(
        radiant_team_id=radiant_team_id,
        dire_team_id=dire_team_id,
        radiant_team_name_original=radiant_team_name_original,
        dire_team_name_original=dire_team_name_original,
        as_of_timestamp=as_of_timestamp,
        analyzer=analyzer,
        max_days=max_days,
        use_transitive=use_transitive,
        verbose=False,
    )
    score = float(res.get("total_score", 0.0) or 0.0)
    strength = float(res.get("strength", 0.0) or 0.0)
    methods_used = res.get("methods_used") or []
    winner = res.get("winner") or res.get("prediction")
    return {
        "score": score,
        "strength": strength,
        "winner": winner,
        "methods_used": methods_used,
        "has_data": bool(res.get("has_data")),
        "message": res.get("message", ""),
        "explanation": res.get("explanation", ""),
    }


# Тестовая функция
def test_transitive_analysis():
    """Тестирование функции анализа"""
    # Пример с реальными ID команд из данных
    result = get_transitiv(5, 7119388)  # G2 x iG vs Team Spirit
    print(result)

def backtest_last_n_matches(
    n: int = 100,
    max_days: int = 7,
    min_strength: float = 0.0,
    require_non_low_confidence: bool = False,
    use_transitive: bool = True,
    chain_weights: Optional[Dict[str, float]] = None,
    min_transitive_chains: Optional[int] = None,
) -> Dict:
    """Бэктест по последним N матчам.

    Args:
        n: сколько последних матчей использовать.
        max_days: максимальное окно по времени (прокидывается в get_transitiv).
        min_strength: минимальная допустимая strength (0.0-1.0); слабее считаем как skip.
        require_non_low_confidence: если True, пропускаем прогнозы с меткой "низкая".
    """
    analyzer = TransitiveAnalyzer()
    matches = [m for m in analyzer.matches_data.values() if m.get('startDateTime', 0) > 0]
    matches.sort(key=lambda m: m.get('startDateTime', 0), reverse=True)
    matches = matches[:n]
    used = 0
    hits = 0
    skipped = 0
    ties = 0
    for m in matches:
        rad = m.get('radiantTeam') or {}
        dire = m.get('direTeam') or {}
        rid = rad.get('id')
        did = dire.get('id')
        rname = rad.get('name', f'Team_{rid}')
        dname = dire.get('name', f'Team_{did}')
        t = m.get('startDateTime', 0)
        rw = m.get('didRadiantWin', None)
        if not rid or not did or not isinstance(rw, bool):
            skipped += 1
            continue
        # подавляем подробный вывод для ускорения бэктеста
        with contextlib.redirect_stdout(open(os.devnull, 'w')):
            res = get_transitiv(
                rid,
                did,
                rname,
                dname,
                as_of_timestamp=t,
                analyzer=analyzer,
                max_days=max_days,
                use_transitive=use_transitive,
                chain_weights=chain_weights,
                min_transitive_chains=min_transitive_chains,
            )
        if not res.get('has_data'):
            skipped += 1
            continue
        strength = float(res.get('strength', 0.0) or 0.0)
        conf_label = res.get('confidence_label') or ''
        if strength < min_strength:
            skipped += 1
            continue
        if require_non_low_confidence and conf_label == 'низкая':
            skipped += 1
            continue
        pred_name = res.get('prediction') or res.get('winner')
        if pred_name == 'Ничья' or pred_name is None:
            ties += 1
            continue
        if pred_name == rname:
            pid = rid
        elif pred_name == dname:
            pid = did
        else:
            skipped += 1
            continue
        aid = rid if rw else did
        used += 1
        if pid == aid:
            hits += 1
    acc = (hits / used) if used else 0.0
    cov = (used / n) if n else 0.0
    print(f"Backtest (matches): N={n}, used={used}, skipped={skipped}, ties={ties}, max_days={max_days}, min_strength={min_strength}, require_non_low_confidence={require_non_low_confidence}, use_transitive={use_transitive}")
    print(f"Accuracy: {acc:.3f}, Coverage: {cov:.3f}")
    return {
        'n': n,
        'used': used,
        'skipped': skipped,
        'ties': ties,
        'accuracy': acc,
        'coverage': cov,
        'max_days': max_days,
        'min_strength': min_strength,
'require_non_low_confidence': require_non_low_confidence,
        'unit': 'matches',
        'use_transitive': use_transitive,
    }


def backtest_last_n_series(
    n: int = 100,
    max_days: int = 7,
    min_strength: float = 0.0,
    require_non_low_confidence: bool = False,
    use_transitive: bool = True,
    chain_weights: Optional[Dict[str, float]] = None,
    min_transitive_chains: Optional[int] = None,
) -> Dict:
    """Бэктест по последним N сериям (BO3/BO5 и т.п.).

    Для каждой серии сначала определяем фактического победителя по картам,
    затем сравниваем его с прогнозом `get_transitiv`, посчитанным до начала серии.
    """
    analyzer = TransitiveAnalyzer()

    # Собираем список серий: (team1_id, team2_id, series_id, matches, start_ts)
    series_entries = []
    for (tid1, tid2), series_dict in analyzer.pair_series.items():
        for series_id, matches in series_dict.items():
            times = [m.get('startDateTime', 0) for m in matches if m.get('startDateTime', 0) > 0]
            if not times:
                continue
            start_ts = min(times)
            series_entries.append({
                'team1_id': tid1,
                'team2_id': tid2,
                'series_id': series_id,
                'matches': matches,
                'start_ts': start_ts,
            })

    # Берём последние N серий по времени
    series_entries.sort(key=lambda e: e['start_ts'], reverse=True)
    series_entries = series_entries[:n]

    used = 0
    hits = 0
    skipped = 0
    ties = 0

    for entry in series_entries:
        tid1 = entry['team1_id']
        tid2 = entry['team2_id']
        series_id = entry['series_id']
        matches = entry['matches']
        start_ts = entry['start_ts']

        # Определяем фактического победителя серии по картам
        maps1 = 0
        maps2 = 0
        for m in matches:
            rad = m.get('radiantTeam') or {}
            dire = m.get('direTeam') or {}
            rid = rad.get('id')
            did = dire.get('id')
            rw = m.get('didRadiantWin', None)
            if not rid or not did or not isinstance(rw, bool):
                continue
            winner_id = rid if rw else did
            if winner_id == tid1:
                maps1 += 1
            elif winner_id == tid2:
                maps2 += 1
        if maps1 == maps2:
            # Ничья / неполная серия — не считаем
            ties += 1
            continue
        actual_winner_id = tid1 if maps1 > maps2 else tid2

        rname = analyzer.get_team_name(tid1)
        dname = analyzer.get_team_name(tid2)

        # подавляем подробный вывод для ускорения бэктеста
        with contextlib.redirect_stdout(open(os.devnull, 'w')):
            res = get_transitiv(
                tid1,
                tid2,
                rname,
                dname,
                as_of_timestamp=start_ts,
                analyzer=analyzer,
                max_days=max_days,
                use_transitive=use_transitive,
                chain_weights=chain_weights,
                min_transitive_chains=min_transitive_chains,
            )

        if not res.get('has_data'):
            skipped += 1
            continue

        strength = float(res.get('strength', 0.0) or 0.0)
        conf_label = res.get('confidence_label') or ''
        if strength < min_strength:
            skipped += 1
            continue
        if require_non_low_confidence and conf_label == 'низкая':
            skipped += 1
            continue

        pred_name = res.get('prediction') or res.get('winner')
        if pred_name == 'Ничья' or pred_name is None:
            ties += 1
            continue
        if pred_name == rname:
            pred_id = tid1
        elif pred_name == dname:
            pred_id = tid2
        else:
            skipped += 1
            continue

        used += 1
        if pred_id == actual_winner_id:
            hits += 1

    acc = (hits / used) if used else 0.0
    cov = (used / n) if n else 0.0
    print(f"Backtest (series): N={n}, used={used}, skipped={skipped}, ties={ties}, max_days={max_days}, min_strength={min_strength}, require_non_low_confidence={require_non_low_confidence}, use_transitive={use_transitive}")
    print(f"Accuracy: {acc:.3f}, Coverage: {cov:.3f}")
    return {
        'n': n,
        'used': used,
        'skipped': skipped,
        'ties': ties,
        'accuracy': acc,
        'coverage': cov,
        'max_days': max_days,
        'min_strength': min_strength,
'require_non_low_confidence': require_non_low_confidence,
        'unit': 'series',
        'use_transitive': use_transitive,
    }


def predict_match_v2(
    radiant_team_id: int,
    dire_team_id: int,
    as_of_timestamp: Optional[int] = None,
    analyzer: Optional[TransitiveAnalyzer] = None,
    h2h_days: int = 90,
    h2h_half_life: float = 14.0,
    current_series_id: Optional[int] = None,
) -> Dict:
    """
    Оптимизированная функция предсказания v2.
    
    ВАЖНО: На уровне серий accuracy ~62%, не 90%!
    90% было на уровне матчей (data leak).
    
    Лучшая стратегия: Strong H2H (>0.7 или <0.3) else Elo
    
    Args:
        radiant_team_id: ID команды Radiant
        dire_team_id: ID команды Dire
        as_of_timestamp: Timestamp матча (для исключения будущих данных)
        analyzer: Экземпляр TransitiveAnalyzer (создаётся если не передан)
        h2h_days: Окно для поиска H2H (по умолчанию 90 дней)
        h2h_half_life: Half-life для decay (по умолчанию 14 дней)
        current_series_id: ID текущей серии (исключается из H2H)
    
    Returns:
        {
            'winner': 'Radiant' | 'Dire',
            'confidence': float (0-1),
            'h2h_score': float | None,
            'elo_diff': float,
            'method': 'h2h' | 'elo',
        }
    """
    import time
    
    if analyzer is None:
        analyzer = TransitiveAnalyzer()
    
    if as_of_timestamp is None:
        as_of_timestamp = int(time.time())
    
    # Elo
    ratings = analyzer.compute_elo_ratings_up_to(
        as_of_timestamp,
        max_days=ELO_HISTORY_DAYS,
        **DEFAULT_ELO_PARAMS
    )
    elo_r = ratings.get(radiant_team_id, ELO_BASE_RATING)
    elo_d = ratings.get(dire_team_id, ELO_BASE_RATING)
    elo_diff = elo_r - elo_d
    
    # H2H с decay
    start_time = as_of_timestamp - h2h_days * 86400
    h2h_series = analyzer.find_head_to_head(radiant_team_id, dire_team_id, start_time, as_of_timestamp)
    
    # Исключаем текущую серию если указана
    if current_series_id is not None:
        h2h_series = [s for s in h2h_series if s.get('series_id') != current_series_id]
    
    h2h_score = None
    method = 'elo'
    
    if h2h_series:
        w_radiant = 0.0
        w_dire = 0.0
        
        for series in h2h_series:
            matches = series['matches']
            r_maps, d_maps, series_ts = 0, 0, 0
            
            for m in matches:
                series_ts = max(series_ts, m.get('startDateTime', 0))
                rad = m.get('radiantTeam', {}).get('id')
                rw = m.get('didRadiantWin')
                
                if rad == radiant_team_id:
                    r_maps += 1 if rw else 0
                    d_maps += 0 if rw else 1
                else:
                    d_maps += 1 if rw else 0
                    r_maps += 0 if rw else 1
            
            # Decay по времени
            days_ago = (as_of_timestamp - series_ts) / 86400
            decay = 0.5 ** (days_ago / h2h_half_life)
            
            # Margin bonus
            margin = 1.0 + abs(r_maps - d_maps) * 0.2
            
            weight = decay * margin
            
            if r_maps > d_maps:
                w_radiant += weight
            else:
                w_dire += weight
        
        total_weight = w_radiant + w_dire
        if total_weight > 0:
            h2h_score = w_radiant / total_weight
            method = 'h2h'
    
    # Предсказание
    if h2h_score is not None:
        # Сильный H2H сигнал
        if h2h_score >= 0.7 or h2h_score <= 0.3:
            winner = 'Radiant' if h2h_score > 0.5 else 'Dire'
            confidence = abs(h2h_score - 0.5) * 2
        else:
            # Слабый H2H - комбинируем с Elo
            h2h_signal = (h2h_score - 0.5) * 100
            elo_signal = elo_diff / 3
            combined = h2h_signal + elo_signal
            winner = 'Radiant' if combined > 0 else 'Dire'
            confidence = min(abs(combined) / 50, 1.0)
    else:
        # Нет H2H - только Elo
        winner = 'Radiant' if elo_diff > 0 else 'Dire'
        confidence = min(abs(elo_diff) / 200, 1.0)
    
    return {
        'winner': winner,
        'confidence': confidence,
        'h2h_score': h2h_score,
        'elo_diff': elo_diff,
        'method': method,
        'radiant_elo': elo_r,
        'dire_elo': elo_d,
    }


def predict_match_v3(
    radiant_team_id: int,
    dire_team_id: int,
    as_of_timestamp: Optional[int] = None,
    analyzer: Optional[TransitiveAnalyzer] = None,
    current_series_id: Optional[int] = None,
) -> Dict:
    """
    Высокоточная функция предсказания v3.
    
    Достигает 77.4% accuracy с 5.8% coverage.
    Использует комбинацию:
    - Elo >= 120 (всегда)
    - Elo >= 117 + обе команды >= 20 матчей (опытные команды)
    
    Returns:
        {
            'winner': 'Radiant' | 'Dire' | None,
            'confidence': float (0-1),
            'tier': 'elo_120' | 'elo_117_exp' | 'no_pred',
            'accuracy_expected': float,
            'elo_diff': float,
        }
    """
    import time
    
    if analyzer is None:
        analyzer = TransitiveAnalyzer()
    
    if as_of_timestamp is None:
        as_of_timestamp = int(time.time())
    
    # Elo
    ratings = analyzer.compute_elo_ratings_up_to(
        as_of_timestamp, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS
    )
    elo_r = ratings.get(radiant_team_id, ELO_BASE_RATING)
    elo_d = ratings.get(dire_team_id, ELO_BASE_RATING)
    elo_diff = elo_r - elo_d
    
    # Tier 1: Elo >= 120 (76.6%)
    if abs(elo_diff) >= 120:
        return {
            'winner': 'Radiant' if elo_diff > 0 else 'Dire',
            'confidence': min(abs(elo_diff) / 200, 1.0),
            'tier': 'elo_120',
            'accuracy_expected': 0.766,
            'elo_diff': elo_diff,
        }
    
    # Tier 2: Elo >= 117 + опытные команды (79.4%)
    if abs(elo_diff) >= 117:
        r_matches = len([m for m in analyzer.team_matches.get(radiant_team_id, []) 
                        if m.get('startDateTime', 0) < as_of_timestamp])
        d_matches = len([m for m in analyzer.team_matches.get(dire_team_id, []) 
                        if m.get('startDateTime', 0) < as_of_timestamp])
        
        if r_matches >= 20 and d_matches >= 20:
            return {
                'winner': 'Radiant' if elo_diff > 0 else 'Dire',
                'confidence': min(abs(elo_diff) / 200, 1.0),
                'tier': 'elo_117_exp',
                'accuracy_expected': 0.794,
                'elo_diff': elo_diff,
            }
    
    return {
        'winner': None,
        'confidence': 0.0,
        'tier': 'no_pred',
        'accuracy_expected': 0.0,
        'elo_diff': elo_diff,
    }


def predict_match_v4(
    radiant_team_id: int,
    dire_team_id: int,
    as_of_timestamp: Optional[int] = None,
    analyzer: Optional[TransitiveAnalyzer] = None,
    elo_threshold: int = 100,
) -> Dict:
    """
    Функция предсказания v4 с настраиваемым порогом Elo.
    
    Рекомендуемые пороги:
    - elo_threshold=120: 76.6% accuracy, 5.3% coverage (high precision)
    - elo_threshold=100: 69.9% accuracy, 10.0% coverage (balanced)
    - elo_threshold=80: 66.3% accuracy, 16.9% coverage (high coverage)
    
    Returns:
        {
            'winner': 'Radiant' | 'Dire' | None,
            'confidence': float (0-1),
            'elo_diff': float,
            'threshold_used': int,
        }
    """
    import time
    
    if analyzer is None:
        analyzer = TransitiveAnalyzer()
    
    if as_of_timestamp is None:
        as_of_timestamp = int(time.time())
    
    # Elo
    ratings = analyzer.compute_elo_ratings_up_to(
        as_of_timestamp, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS
    )
    elo_r = ratings.get(radiant_team_id, ELO_BASE_RATING)
    elo_d = ratings.get(dire_team_id, ELO_BASE_RATING)
    elo_diff = elo_r - elo_d
    
    if abs(elo_diff) >= elo_threshold:
        return {
            'winner': 'Radiant' if elo_diff > 0 else 'Dire',
            'confidence': min(abs(elo_diff) / 200, 1.0),
            'elo_diff': elo_diff,
            'threshold_used': elo_threshold,
        }
    
    return {
        'winner': None,
        'confidence': 0.0,
        'elo_diff': elo_diff,
        'threshold_used': elo_threshold,
    }


def predict_match_v5(
    radiant_team_id: int,
    dire_team_id: int,
    as_of_timestamp: Optional[int] = None,
    analyzer: Optional[TransitiveAnalyzer] = None,
    current_series_id: Optional[int] = None,
    h2h_days: int = 90,
    h2h_half_life: float = 14.0,
) -> Dict:
    """
    Функция предсказания v5 с 100% coverage.
    
    Максимальная достижимая accuracy: ~63% при 100% coverage.
    
    Использует взвешенную комбинацию:
    - Elo (вес 1.5)
    - H2H (вес 0.8)
    - Form (вес 0.3)
    - Streak (вес 0.1)
    
    ВАЖНО: 76% accuracy с 100% coverage НЕ достижима с текущими данными.
    Причины:
    - 51% серий имеют |Elo diff| < 30 (практически равные команды)
    - 39% серий - апсеты (фаворит проигрывает)
    - Детекция апсетов имеет precision ~50% (не лучше случайного)
    
    Returns:
        {
            'winner': 'Radiant' | 'Dire',
            'confidence': float (0-1),
            'score': float,
            'elo_diff': float,
            'h2h_score': float | None,
            'form_diff': float,
            'streak_diff': int,
        }
    """
    import time
    
    if analyzer is None:
        analyzer = TransitiveAnalyzer()
    
    if as_of_timestamp is None:
        as_of_timestamp = int(time.time())
    
    # === Elo ===
    ratings = analyzer.compute_elo_ratings_up_to(
        as_of_timestamp, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS
    )
    elo_r = ratings.get(radiant_team_id, ELO_BASE_RATING)
    elo_d = ratings.get(dire_team_id, ELO_BASE_RATING)
    elo_diff = elo_r - elo_d
    
    # === H2H ===
    start_time = as_of_timestamp - h2h_days * 86400
    h2h_series = analyzer.find_head_to_head(radiant_team_id, dire_team_id, start_time, as_of_timestamp)
    
    if current_series_id is not None:
        h2h_series = [s for s in h2h_series if s.get('series_id') != current_series_id]
    
    h2h_score = None
    if h2h_series:
        w_radiant, w_dire = 0.0, 0.0
        for series in h2h_series:
            matches = series['matches']
            r_maps, d_maps, series_ts = 0, 0, 0
            for m in matches:
                series_ts = max(series_ts, m.get('startDateTime', 0))
                rad = m.get('radiantTeam', {}).get('id')
                rw = m.get('didRadiantWin')
                if rad == radiant_team_id:
                    r_maps += 1 if rw else 0
                    d_maps += 0 if rw else 1
                else:
                    d_maps += 1 if rw else 0
                    r_maps += 0 if rw else 1
            days_ago = (as_of_timestamp - series_ts) / 86400
            decay = 0.5 ** (days_ago / h2h_half_life)
            margin = 1.0 + abs(r_maps - d_maps) * 0.2
            weight = decay * margin
            if r_maps > d_maps:
                w_radiant += weight
            else:
                w_dire += weight
        total_weight = w_radiant + w_dire
        if total_weight > 0:
            h2h_score = w_radiant / total_weight
    
    # === Form ===
    def get_form(tid, n=10):
        matches = analyzer.team_matches.get(tid, [])
        valid = sorted([m for m in matches if 0 < m.get('startDateTime', 0) < as_of_timestamp],
                       key=lambda m: m.get('startDateTime', 0), reverse=True)[:n]
        if not valid:
            return 0.5
        wins, total = 0.0, 0.0
        for i, m in enumerate(valid):
            rad = m.get('radiantTeam', {}).get('id')
            rw = m.get('didRadiantWin')
            won = (rad == tid and rw) or (rad != tid and not rw)
            w = 0.85 ** i
            total += w
            wins += w if won else 0
        return wins / total if total > 0 else 0.5
    
    form_r = get_form(radiant_team_id)
    form_d = get_form(dire_team_id)
    form_diff = form_r - form_d
    
    # === Streak ===
    def get_streak(tid):
        matches = analyzer.team_matches.get(tid, [])
        valid = sorted([m for m in matches if 0 < m.get('startDateTime', 0) < as_of_timestamp],
                       key=lambda m: m.get('startDateTime', 0), reverse=True)[:20]
        if not valid:
            return 0
        streak, first_result = 0, None
        for m in valid:
            rad = m.get('radiantTeam', {}).get('id')
            rw = m.get('didRadiantWin')
            won = (rad == tid and rw) or (rad != tid and not rw)
            if first_result is None:
                first_result = won
                streak = 1 if won else -1
            elif won == first_result:
                streak += 1 if won else -1
            else:
                break
        return streak
    
    streak_r = get_streak(radiant_team_id)
    streak_d = get_streak(dire_team_id)
    streak_diff = streak_r - streak_d
    
    # === Weighted Score ===
    # Оптимизированные веса: elo=1.5, h2h=0.8, form=0.3, streak=0.1
    score = (elo_diff / 100) * 1.5
    if h2h_score is not None:
        score += (h2h_score - 0.5) * 2 * 0.8
    score += form_diff * 2 * 0.3
    score += (streak_diff / 5) * 0.1
    
    winner = 'Radiant' if score > 0 else 'Dire'
    confidence = min(abs(score) / 3, 1.0)
    
    return {
        'winner': winner,
        'confidence': confidence,
        'score': score,
        'elo_diff': elo_diff,
        'h2h_score': h2h_score,
        'form_diff': form_diff,
        'streak_diff': streak_diff,
    }


if __name__ == "__main__":
    test_transitive_analysis()
