"""
Функция для анализа матчей и записи данных в словари для статистики.

⚠️ ВАЖНО ДЛЯ ИЗБЕЖАНИЯ DATA LEAKAGE:
1. При построении статистики для обучения ML моделей, словари должны строиться 
   с TEMPORAL SPLIT - используйте только матчи ДО текущего матча
2. Никогда не включайте текущий матч в статистику при его предсказании
3. Используйте exclude_match_ids для фильтрации матчей
4. Фильтруйте про-матчи если работаете только с публичными данными
"""

import json
import os
from functools import lru_cache
from itertools import combinations
from pathlib import Path


ALCHEMIST_HERO_ID = 73
EARLY_DOMINATOR_THRESHOLDS_PATH = Path(os.getenv(
    "EARLY_DOMINATOR_THRESHOLDS_PATH",
    Path(__file__).with_name("early_networth_dominator_20pct_thresholds_7_41.json"),
))
LATE_WR60_THRESHOLDS_PATH = Path(os.getenv(
    "LATE_WR60_THRESHOLDS_PATH",
    Path(__file__).with_name("is_late_wr60_70pct_thresholds.json"),
))
EARLY_DOMINATOR_FALLBACK_THRESHOLDS = {
    "alchemist_leading": {
        20: 6000, 21: 6000, 22: 6500, 23: 7000, 24: 8000,
        25: 8000, 26: 8000, 27: 9500, 28: 7500, 29: 9500,
        30: 9500, 31: 10500, 32: 12000, 33: 11000, 34: 12000,
    },
    "alchemist_trailing": {
        20: 6000, 21: 6000, 22: 6000, 23: 5500, 24: 6500,
        25: 6500, 26: 6500, 27: 7000, 28: 8500, 29: 7500,
        30: 6000, 31: 7500, 32: 7500, 33: 6500, 34: 7000,
    },
    "no_alchemist": {
        20: 6000, 21: 6500, 22: 6500, 23: 7000, 24: 7000,
        25: 7000, 26: 7500, 27: 8000, 28: 8500, 29: 8000,
        30: 8500, 31: 9000, 32: 9000, 33: 9000, 34: 9500,
    },
}


def _append_to_dict(target_dict, key, value, is_defaultdict=None):
    """
    Вспомогательная функция для добавления значения в словарь.
    Оптимизирует повторяющийся код.
    
    Теперь работает с агрегированными счетчиками вместо списков для ускорения.
    Параметр is_defaultdict оставлен для обратной совместимости, но не используется.
    
    value может быть:
    - 1: победа
    - 0: поражение
    - 0.5: ничья (draw/tie)
    """
    if key not in target_dict:
        target_dict[key] = {'wins': 0, 'draws': 0, 'games': 0}
    target_dict[key]['games'] += 1
    if value == 1:
        target_dict[key]['wins'] += 1
    elif value == 0.5:
        target_dict[key]['draws'] += 1


def extract_heroes_by_position(match):
    """
    Извлекает героев и позиции из матча.
    
    Returns:
        tuple: (r_by_pos, d_by_pos) или (None, None) если недостаточно данных
    """
    r_by_pos = {}
    d_by_pos = {}
    
    for p in match.get('players', []):
        # Поддержка двух форматов: hero.id и heroId
        hero = p.get('hero', {})
        hero_id = hero.get('id') if hero else p.get('heroId')
        if hero_id is None:
            continue
        
        position = p.get('position')
        if position:
            if isinstance(position, str) and 'POSITION_' in position:
                pos_num = int(position.split('_')[1])
            elif isinstance(position, int):
                pos_num = position
            else:
                continue
        else:
            continue
        
        if p.get('isRadiant', False):
            r_by_pos[pos_num] = hero_id
        else:
            d_by_pos[pos_num] = hero_id
    
    # Нужны все 5 позиций
    if len(r_by_pos) != 5 or len(d_by_pos) != 5:
        return None, None
    
    return r_by_pos, d_by_pos


def _player_hero_id(player):
    hero = player.get('hero', {}) if isinstance(player, dict) else {}
    hero_id = hero.get('id') if hero else player.get('heroId') if isinstance(player, dict) else None
    try:
        return int(hero_id)
    except (TypeError, ValueError):
        return None


def _hero_side_flags(match, hero_id):
    radiant_has = False
    dire_has = False
    for player in match.get('players', []):
        if not isinstance(player, dict):
            continue
        if _player_hero_id(player) != int(hero_id):
            continue
        if bool(player.get('isRadiant')):
            radiant_has = True
        else:
            dire_has = True
    return radiant_has, dire_has


def lanes(match, lane_dict):
    """
    Обрабатывает данные по лайнам и записывает в lane_dict.
    
    Lane dict intentionally has no draft/tempo gates: if the match has enough
    lane outcome and position data, it contributes to lane statistics.
    
    Формирует:
    - Соло героя с позицией
    - Контрипики 2x2, 1x2, 2x1, 1x1 для каждого лайна
    - Синергию 1+1 для каждого лайна
    - Все ключи включают позиции героев
    
    Args:
        match: словарь с данными матча
        lane_dict: словарь для записи статистики по лайнам
    """
    # Извлекаем героев и позиции
    r_by_pos, d_by_pos = extract_heroes_by_position(match)
    if r_by_pos is None:
        return
    
    # Определяем исходы лайнов
    top_outcome = match.get('topLaneOutcome', '')
    mid_outcome = match.get('midLaneOutcome', '')
    bot_outcome = match.get('bottomLaneOutcome', '')
    
    def get_lane_value(outcome, key_starts_with_radiant):
        """
        Определяет значение для записи в lane_dict на основе исхода лайна.
        
        Args:
            outcome: исход лайна (topLaneOutcome, midLaneOutcome, bottomLaneOutcome)
            key_starts_with_radiant: True если ключ начинается с radiant героев
        
        Returns:
            1 если radiant выиграл и ключ начинается с radiant, или если dire выиграл и ключ начинается с dire
            0 если противоположная команда выиграла
            0.5 если TIE/DRAW (ничья)
            None если исход отсутствует
        """
        if not outcome:
            return None
        
        radiant_won = 'RADIANT' in outcome.upper()
        dire_won = 'DIRE' in outcome.upper()
        tie = 'TIE' in outcome.upper() or 'DRAW' in outcome.upper()
        
        if radiant_won:
            return 1 if key_starts_with_radiant else 0
        elif dire_won:
            return 0 if key_starts_with_radiant else 1
        elif tie:
            # TIE/DRAW - ничья для обеих команд
            return 0.5
        else:
            # Неизвестный исход
            return None
    
    def add_lane_data(r_heroes, d_heroes, outcome):
        """
        Вспомогательная функция для добавления данных лайна.
        
        Args:
            r_heroes: список кортежей (hero_id, position) для Radiant
            d_heroes: список кортежей (hero_id, position) для Dire
            outcome: исход лайна
        """
        if not outcome:
            return
        
        value_r = get_lane_value(outcome, True)
        value_d = get_lane_value(outcome, False)
        
        if value_r is None:
            return
        
        # Соло герои Radiant
        for hero_id, pos in r_heroes:
            _append_to_dict(lane_dict, f'{hero_id}pos{pos}', value_r)
        
        # Соло герои Dire
        for hero_id, pos in d_heroes:
            _append_to_dict(lane_dict, f'{hero_id}pos{pos}', value_d)
        
        # Если это парный лайн (2v2)
        if len(r_heroes) == 2 and len(d_heroes) == 2:
            r_h1, r_p1 = r_heroes[0]
            r_h2, r_p2 = r_heroes[1]
            d_h1, d_p1 = d_heroes[0]
            d_h2, d_p2 = d_heroes[1]
            
            # Контрипики 2x2
            key = f'{r_h1}pos{r_p1},{r_h2}pos{r_p2}_vs_{d_h1}pos{d_p1},{d_h2}pos{d_p2}'
            _append_to_dict(lane_dict, key, value_r)
            
            # Контрипики 2x1 (Radiant 2 vs Dire 1)
            _append_to_dict(lane_dict, f'{r_h1}pos{r_p1},{r_h2}pos{r_p2}_vs_{d_h1}pos{d_p1}', value_r)
            _append_to_dict(lane_dict, f'{r_h1}pos{r_p1},{r_h2}pos{r_p2}_vs_{d_h2}pos{d_p2}', value_r)
            
            # Контрипики 1x2 (Radiant 1 vs Dire 2)
            _append_to_dict(lane_dict, f'{r_h1}pos{r_p1}_vs_{d_h1}pos{d_p1},{d_h2}pos{d_p2}', value_r)
            _append_to_dict(lane_dict, f'{r_h2}pos{r_p2}_vs_{d_h1}pos{d_p1},{d_h2}pos{d_p2}', value_r)
            
            # Контрипики 1x1 (все комбинации)
            for r_hero, r_pos in r_heroes:
                for d_hero, d_pos in d_heroes:
                    _append_to_dict(lane_dict, f'{r_hero}pos{r_pos}_vs_{d_hero}pos{d_pos}', value_r)
            
            # Синергия 1+1 для Radiant
            _append_to_dict(lane_dict, f'{r_h1}pos{r_p1}_with_{r_h2}pos{r_p2}', value_r)
            
            # Синергия 1+1 для Dire
            _append_to_dict(lane_dict, f'{d_h1}pos{d_p1}_with_{d_h2}pos{d_p2}', value_d)
        
        # Если это 1v1
        elif len(r_heroes) == 1 and len(d_heroes) == 1:
            r_h, r_p = r_heroes[0]
            d_h, d_p = d_heroes[0]
            
            # Контрипик 1x1
            _append_to_dict(lane_dict, f'{r_h}pos{r_p}_vs_{d_h}pos{d_p}', value_r)
    
    # TOP LANE: Radiant (pos3+pos4) vs Dire (pos1+pos5)
    if 3 in r_by_pos and 4 in r_by_pos and 1 in d_by_pos and 5 in d_by_pos:
        add_lane_data(
            [(r_by_pos[3], 3), (r_by_pos[4], 4)],
            [(d_by_pos[1], 1), (d_by_pos[5], 5)],
            top_outcome
        )
    
    # MID LANE: Radiant (pos2) vs Dire (pos2) - 1x1
    if 2 in r_by_pos and 2 in d_by_pos:
        add_lane_data(
            [(r_by_pos[2], 2)],
            [(d_by_pos[2], 2)],
            mid_outcome
        )
    
    # BOT LANE: Radiant (pos1+pos5) vs Dire (pos3+pos4)
    if 1 in r_by_pos and 5 in r_by_pos and 3 in d_by_pos and 4 in d_by_pos:
        add_lane_data(
            [(r_by_pos[1], 1), (r_by_pos[5], 5)],
            [(d_by_pos[3], 3), (d_by_pos[4], 4)],
            bot_outcome
        )



# ============================================================================
# НАСТРОЙКИ ФИЛЬТРОВ EARLY/LATE (подбираются экспериментально)
# ============================================================================
# Early: требуем близкий networth на gate-точке и ищем ранний перевес.
EARLY_GATE_INDEX = 10                # фильтр на leads[10]
EARLY_GATE_MAX_ABS_LEAD = 2000       # игра не должна разъехаться до early-gate
EARLY_LEAD_WINDOW = (20, 28)         # реальные минуты достижения 20% comeback threshold
EARLY_FAST_FINISH_MAX_MINUTES = 34   # быстрые карты считаем early по победителю

# Late: длинная игра, где networth gap не разъехался сильнее WR60 ladder.
LATE_MIN_DURATION = 34
LATE_MAX_DURATION = None  # None если не нужен верхний предел
LATE_EARLY_WINDOW = (15, 25)         # окно для оценки раннего snowball
LATE_EARLY_STOMP_MAX = 12000         # max |lead| для раннего snowball
LATE_COMEBACK_AVG_DEFICIT = 4000     # средний deficit победителя в 15-25
LATE_CLOSE_WINDOW = (20, 30)         # окно "близкой" игры
LATE_CLOSE_MAX_LEAD = 5000           # max |lead| в close-окне
LATE_MODE = 'comeback'               # 'either' | 'comeback' | 'close'
LATE_REQUIRE_EARLY_LOSS = True      # late = победитель не был early-доминатором
LATE_WR60_START_MINUTE = 28
LATE_WR60_FALLBACK_THRESHOLDS = {
    20: 2498.74,
    21: 2666.64,
    22: 2890.64,
    23: 3151.62,
    24: 3363.39,
    25: 3603.93,
    26: 3846.09,
    27: 4104.51,
    28: 4380.31,
    29: 4674.63,
    30: 4988.72,
    31: 5121.12,
    32: 5257.03,
    33: 5396.56,
    34: 5539.79,
    35: 5686.81,
    36: 5837.74,
    37: 5992.67,
    38: 6151.72,
    39: 6314.99,
    40: 6482.59,
    41: 6945.47,
    42: 7441.40,
    43: 7972.74,
    44: 8542.02,
    45: 9151.95,
    46: 9805.44,
    47: 10505.58,
    48: 11255.71,
    49: 12059.41,
    50: 12920.50,
    51: 13843.07,
    52: 14831.51,
    53: 15890.53,
    54: 17025.17,
    55: 18240.82,
    56: 19543.29,
    57: 20938.74,
}

# Post-lane: нейтральная выборка после лейнинга.
# Гейты только на 10-й минуте и минимальную длину; дальше любая длительность.
POST_LANE_GATE_MINUTE = 10
POST_LANE_MAX_ABS_LEAD_AT_GATE = 2000
POST_LANE_MIN_DURATION = 20


def _max_abs_in_window(leads, start, end):
    if len(leads) <= start:
        return None
    end = min(end, len(leads) - 1)
    return max(abs(leads[i]) for i in range(start, end + 1))


def _avg_in_window(leads, start, end):
    if len(leads) <= start:
        return None
    end = min(end, len(leads) - 1)
    window = leads[start:end + 1]
    return sum(window) / len(window) if window else None


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_thresholds(raw):
    normalized = {}
    for group, values in (raw or {}).items():
        if not isinstance(values, dict):
            continue
        group_values = {}
        for minute, threshold in values.items():
            try:
                group_values[int(minute)] = int(threshold)
            except (TypeError, ValueError):
                continue
        if group_values:
            normalized[str(group)] = group_values
    return normalized


def _normalize_minute_thresholds(raw):
    normalized = {}
    for minute, threshold in (raw or {}).items():
        try:
            normalized[int(minute)] = abs(float(threshold))
        except (TypeError, ValueError):
            continue
    return normalized


@lru_cache(maxsize=1)
def _load_early_dominator_thresholds():
    try:
        payload = json.loads(EARLY_DOMINATOR_THRESHOLDS_PATH.read_text(encoding='utf-8'))
        raw_thresholds = payload.get('thresholds_by_group') if isinstance(payload, dict) else None
        thresholds = _normalize_thresholds(raw_thresholds)
        if thresholds:
            return thresholds
    except (OSError, json.JSONDecodeError):
        pass
    return EARLY_DOMINATOR_FALLBACK_THRESHOLDS


@lru_cache(maxsize=1)
def _load_late_wr60_thresholds():
    try:
        payload = json.loads(LATE_WR60_THRESHOLDS_PATH.read_text(encoding='utf-8'))
        raw_thresholds = payload.get('thresholds_by_minute') if isinstance(payload, dict) else None
        thresholds = _normalize_minute_thresholds(raw_thresholds)
        if thresholds:
            return thresholds
    except (OSError, json.JSONDecodeError):
        pass
    return LATE_WR60_FALLBACK_THRESHOLDS


def _early_threshold_group(match, dominator):
    radiant_has_alchemist, dire_has_alchemist = _hero_side_flags(match, ALCHEMIST_HERO_ID)
    leading_has_alchemist = radiant_has_alchemist if dominator == 'radiant' else dire_has_alchemist
    trailing_has_alchemist = dire_has_alchemist if dominator == 'radiant' else radiant_has_alchemist
    if leading_has_alchemist:
        return 'alchemist_leading'
    if trailing_has_alchemist:
        return 'alchemist_trailing'
    return 'no_alchemist'


def _early_threshold_for(match, dominator, minute):
    thresholds = _load_early_dominator_thresholds()
    group = _early_threshold_group(match, dominator)
    group_thresholds = thresholds.get(group) or thresholds.get('no_alchemist') or {}
    threshold = group_thresholds.get(int(minute))
    if threshold is not None:
        return threshold

    earlier_minutes = [item for item in group_thresholds if item <= int(minute)]
    if earlier_minutes:
        return group_thresholds[max(earlier_minutes)]
    later_minutes = [item for item in group_thresholds if item >= int(minute)]
    if later_minutes:
        return group_thresholds[min(later_minutes)]
    return None


def _first_dynamic_threshold_reach(match, leads, start_minute, end_minute):
    for minute in range(int(start_minute), int(end_minute) + 1):
        idx = minute - 1
        if idx < 0 or len(leads) <= idx:
            continue
        lead = _as_float(leads[idx])
        if lead is None or lead == 0:
            continue
        dominator = 'radiant' if lead > 0 else 'dire'
        threshold = _early_threshold_for(match, dominator, minute)
        if threshold is not None and abs(lead) >= threshold:
            return dominator, minute
    return None, None


def _late_wr60_gap_hit(leads, start_minute=LATE_WR60_START_MINUTE):
    thresholds = _load_late_wr60_thresholds()
    for minute in sorted(thresholds):
        if minute < int(start_minute):
            continue
        idx = minute - 1
        if idx < 0 or len(leads) <= idx:
            continue
        lead = _as_float(leads[idx])
        threshold = thresholds[minute]
        if lead is not None and abs(lead) <= threshold:
            return True
    return False


def is_early_match(match, n: int = 3000):
    """
    Проверяет, подходит ли матч для early словаря.
    
    ЛОГИКА EARLY:
    - Быстрые карты duration <= 34 минут считаются early; dominator = winner
    - Для длинных карт на gate-точке leads[10] игра не должна быть уже слишком разъехавшейся
    - Early dominator = кто первым достиг 20% comeback networth threshold
      в окне 20-28 минут
    - Победитель матча для early не важен
    
    Args:
        match: словарь с данными матча
        n: параметр сохранен для совместимости (не используется)
    
    Returns:
        tuple: (bool, dominator)
            dominator: 'radiant' | 'dire' | None
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)

    if duration <= EARLY_FAST_FINISH_MAX_MINUTES:
        did_radiant_win = match.get('didRadiantWin')
        if did_radiant_win is None:
            win_rates = match.get('winRates', [])
            did_radiant_win = win_rates[-1] > 0.5 if win_rates else None
        if did_radiant_win is not None:
            return True, 'radiant' if did_radiant_win else 'dire'
        final_lead = _as_float(leads[-1]) if leads else None
        if final_lead is not None and final_lead != 0:
            return True, 'radiant' if final_lead > 0 else 'dire'
        return False, None

    if duration <= EARLY_GATE_INDEX:
        return False, None

    gate_lead = _as_float(leads[EARLY_GATE_INDEX])
    if gate_lead is None or abs(gate_lead) > EARLY_GATE_MAX_ABS_LEAD:
        return False, None

    if duration < EARLY_LEAD_WINDOW[0]:
        return False, None

    early_thresholds_by_group = _load_early_dominator_thresholds()
    for minute in range(EARLY_LEAD_WINDOW[0], EARLY_LEAD_WINDOW[1] + 1):
        idx = minute - 1
        if idx < 0 or len(leads) <= idx:
            continue
        lead = _as_float(leads[idx])
        if lead is None or lead == 0:
            continue

        dominator = 'radiant' if lead > 0 else 'dire'
        threshold_group = _early_threshold_group(match, dominator)
        thresholds_by_minute = (
            early_thresholds_by_group.get(threshold_group)
            or early_thresholds_by_group.get('no_alchemist')
            or {}
        )
        threshold = thresholds_by_minute.get(int(minute))
        if threshold is None:
            earlier_minutes = [item for item in thresholds_by_minute if item <= int(minute)]
            if earlier_minutes:
                threshold = thresholds_by_minute[max(earlier_minutes)]
            else:
                later_minutes = [item for item in thresholds_by_minute if item >= int(minute)]
                if later_minutes:
                    threshold = thresholds_by_minute[min(later_minutes)]

        if threshold is not None and abs(lead) >= threshold:
            return True, dominator

    return False, None


def is_late_match(match, dominator=None, if_check: bool = False, n: int = 7000):
    """
    Проверяет, подходит ли матч для late словаря.
    
    ЛОГИКА LATE:
    - Матч должен длиться >= 34 минут.
    - Берем WR60 networth ladder с LATE_WR60_START_MINUTE.
    - Если абсолютный networth gap хотя бы на одной минуте не больше
      WR60-порога этой минуты, матч идет в late sample.
    
    Args:
        match: словарь с данными матча
        dominator: не используется (для обратной совместимости)
        if_check: не используется (для обратной совместимости)
        n: параметр сохранен для совместимости (не используется)
    
    Returns:
        bool | tuple: подходит ли матч для late словаря
            При if_check=True возвращает (bool, winner)
    """
    leads = match.get('radiantNetworthLeads', [])
    did_radiant_win = match.get('didRadiantWin')
    duration = len(leads)
    
    if did_radiant_win is None:
        win_rates = match.get('winRates', [])
        did_radiant_win = win_rates[-1] > 0.5 if win_rates else None

    if did_radiant_win is None or duration < LATE_MIN_DURATION:
        return (False, None) if if_check else False

    if LATE_MAX_DURATION is not None and duration > LATE_MAX_DURATION:
        return (False, None) if if_check else False

    winner = 'radiant' if did_radiant_win else 'dire'
    late_thresholds_by_minute = _load_late_wr60_thresholds()
    for minute, threshold in sorted(late_thresholds_by_minute.items()):
        if minute < int(LATE_WR60_START_MINUTE):
            continue
        idx = minute - 1
        if idx < 0 or len(leads) <= idx:
            continue
        lead = _as_float(leads[idx])
        if lead is not None and abs(lead) <= threshold:
            return (True, winner) if if_check else True

    return (False, None) if if_check else False


def is_post_lane_match(match, if_check: bool = False):
    """
    Проверяет, подходит ли матч для post-lane словаря.

    Логика:
    - матч должен иметь победителя;
    - длина матча >= POST_LANE_MIN_DURATION;
    - на 10-й минуте игра не должна быть уже слишком разъехавшейся;
    - верхнего ограничения по длительности нет.

    Returns:
        bool | tuple: подходит ли матч, и победитель при if_check=True.
    """
    leads = match.get('radiantNetworthLeads', [])
    did_radiant_win = match.get('didRadiantWin')
    duration = len(leads)

    if did_radiant_win is None:
        win_rates = match.get('winRates', [])
        did_radiant_win = win_rates[-1] > 0.5 if win_rates else None

    if did_radiant_win is None or duration < POST_LANE_MIN_DURATION:
        return (False, None) if if_check else False

    gate_index = POST_LANE_GATE_MINUTE - 1
    if len(leads) <= gate_index:
        return (False, None) if if_check else False

    try:
        gate_lead = float(leads[gate_index])
    except (TypeError, ValueError):
        return (False, None) if if_check else False

    if abs(gate_lead) > POST_LANE_MAX_ABS_LEAD_AT_GATE:
        return (False, None) if if_check else False

    winner = 'radiant' if did_radiant_win else 'dire'
    return (True, winner) if if_check else True


def _add_combinations_to_dict(r_by_pos, d_by_pos, target_dict, r_value, d_value=None):
    """
    Добавляет все комбинации героев в словарь.
    Оптимизированная версия для уменьшения дублирования кода.
    
    Args:
        r_by_pos: словарь позиций героев Radiant
        d_by_pos: словарь позиций героев Dire
        target_dict: целевой словарь (обычный dict, будет содержать счетчики {'wins': N, 'games': M})
        r_value: значение для Radiant героев и комбинаций начинающихся с Radiant
        d_value: значение для Dire героев и комбинаций Dire (если None, используется r_value)
    """
    if d_value is None:
        d_value = r_value
    
    r_items = list(r_by_pos.items())
    d_items = list(d_by_pos.items())
    
    # Одиночные герои
    for pos_num, hero_id in r_items:
        _append_to_dict(target_dict, f'{hero_id}pos{pos_num}', r_value)
    
    for pos_num, hero_id in d_items:
        _append_to_dict(target_dict, f'{hero_id}pos{pos_num}', d_value)
    
    # Контрипики 1x2
    for r_pos, r_hero in r_items:
        for d_pos1, d_hero1 in d_items:
            for d_pos2, d_hero2 in d_items:
                if d_hero1 == d_hero2:
                    continue
                key = f'{r_hero}pos{r_pos}_vs_{d_hero1}pos{d_pos1},{d_hero2}pos{d_pos2}'
                _append_to_dict(target_dict, key, r_value)
    
    # Контрипики 2x1
    for r_pos1, r_hero1 in r_items:
        for r_pos2, r_hero2 in r_items:
            if r_hero1 == r_hero2:
                continue
            for d_pos, d_hero in d_items:
                key = f'{r_hero1}pos{r_pos1},{r_hero2}pos{r_pos2}_vs_{d_hero}pos{d_pos}'
                _append_to_dict(target_dict, key, r_value)
    
    # Контрипики 1x1
    for r_pos, r_hero in r_items:
        for d_pos, d_hero in d_items:
            key = f'{r_hero}pos{r_pos}_vs_{d_hero}pos{d_pos}'
            _append_to_dict(target_dict, key, r_value)
    
    # Синергия 1+1 (Radiant)
    for r_pos1, r_hero1 in r_items:
        for r_pos2, r_hero2 in r_items:
            if r_hero1 == r_hero2:
                continue
            key = f'{r_hero1}pos{r_pos1}_with_{r_hero2}pos{r_pos2}'
            _append_to_dict(target_dict, key, r_value)
    
    # Синергия 1+1 (Dire)
    for d_pos1, d_hero1 in d_items:
        for d_pos2, d_hero2 in d_items:
            if d_hero1 == d_hero2:
                continue
            key = f'{d_hero1}pos{d_pos1}_with_{d_hero2}pos{d_pos2}'
            _append_to_dict(target_dict, key, d_value)
    
    # Трио синергия (Radiant)
    for i in range(len(r_items)):
        for j in range(i + 1, len(r_items)):
            for k in range(j + 1, len(r_items)):
                r_pos1, r_hero1 = r_items[i]
                r_pos2, r_hero2 = r_items[j]
                r_pos3, r_hero3 = r_items[k]
                key = f'{r_hero1}pos{r_pos1},{r_hero2}pos{r_pos2},{r_hero3}pos{r_pos3}'
                _append_to_dict(target_dict, key, r_value)
    
    # Трио синергия (Dire)
    for i in range(len(d_items)):
        for j in range(i + 1, len(d_items)):
            for k in range(j + 1, len(d_items)):
                d_pos1, d_hero1 = d_items[i]
                d_pos2, d_hero2 = d_items[j]
                d_pos3, d_hero3 = d_items[k]
                key = f'{d_hero1}pos{d_pos1},{d_hero2}pos{d_pos2},{d_hero3}pos{d_pos3}'
                _append_to_dict(target_dict, key, d_value)


def is_pro_match(match):
    """
    Определяет является ли матч про-матчем.
    
    Про-матчи определяются по наличию:
    - leagueId (турнирные матчи)
    - radiantTeam.id и direTeam.id (командные матчи)
    
    Returns:
        True если это про-матч, False если паблик
    """
    # Проверяем наличие турнирной лиги
    if match.get('leagueId'):
        return True
    
    # Проверяем наличие команд (не просто стаков пабликов)
    r_team = match.get('radiantTeam', {})
    d_team = match.get('direTeam', {})
    
    if r_team and d_team and r_team.get('id') and d_team.get('id'):
        return True
    
    return False


def analise_database(match, lane_dict, early_dict, late_dict, *,
                     exclude_match_ids=None, exclude_pro_matches=True, dominator=None,
                     post_lane_dict=None):
    """
    Основная функция анализа матча.
    
    Args:
        match: словарь с данными матча
        lane_dict: словарь для записи статистики по лайнам
        early_dict: словарь для записи статистики по early фазе
        late_dict: словарь для записи статистики по late фазе
        post_lane_dict: словарь после лейнинга с gate на 10-й минуте и min duration
        exclude_match_ids: set или list ID матчей которые нужно исключить (для избежания data leakage)
        exclude_pro_matches: если True, пропускает про-матчи (default: True)
    
    ⚠️ ВАЖНО: Для избежания data leakage при обучении ML моделей:
    - Всегда передавайте exclude_match_ids содержащий текущий матч
    - Используйте temporal split: обрабатывайте матчи в хронологическом порядке
    - Для каждого матча используйте только статистику из предыдущих матчей
    """
    # Фильтр про-матчей
    if exclude_pro_matches and is_pro_match(match):
        return False  # Матч пропущен
    
    # Фильтр исключаемых матчей
    match_id = match.get('id')
    if exclude_match_ids and match_id and match_id in exclude_match_ids:
        return False  # Матч пропущен
    # 1. Обработка лайнов
    lanes(match, lane_dict)
    
    # 2. Извлекаем героев и позиции для early/late/post-lane
    r_by_pos, d_by_pos = extract_heroes_by_position(match)
    if r_by_pos is None:
        return
    
    # Определяем победителя один раз (используется в late и post-lane)
    did_radiant_win = match.get('didRadiantWin')
    if did_radiant_win is None:
        # Используем последний элемент winRates
        win_rates = match.get('winRates', [])
        did_radiant_win = win_rates[-1] > 0.5 if win_rates else False

    # 3. Обработка EARLY словаря
    # Используем новый фильтр is_early_match()
    early_result, dominator = is_early_match(match)
    if early_result:
        # Для early_dict значение зависит от того, кто доминировал
        r_val = 1 if dominator == 'radiant' else 0
        d_val = 1 if dominator == 'dire' else 0
        _add_combinations_to_dict(r_by_pos, d_by_pos, early_dict, r_val, d_val)
    
    # Проверяем условия для late_dict
    # Используем улучшенный фильтр is_late_match()
    if is_late_match(match, dominator):
        # Записываем кто выиграл матч
        r_val = 1 if did_radiant_win else 0
        d_val = 0 if did_radiant_win else 1
        _add_combinations_to_dict(r_by_pos, d_by_pos, late_dict, r_val, d_val)

    if post_lane_dict is not None and is_post_lane_match(match):
        # После post-lane gate записываем фактического победителя матча.
        r_val = 1 if did_radiant_win else 0
        d_val = 0 if did_radiant_win else 1
        _add_combinations_to_dict(r_by_pos, d_by_pos, post_lane_dict, r_val, d_val)

    return True  # Матч успешно обработан
