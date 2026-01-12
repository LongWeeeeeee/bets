"""
Функция для анализа матчей и записи данных в словари для статистики.

⚠️ ВАЖНО ДЛЯ ИЗБЕЖАНИЯ DATA LEAKAGE:
1. При построении статистики для обучения ML моделей, словари должны строиться 
   с TEMPORAL SPLIT - используйте только матчи ДО текущего матча
2. Никогда не включайте текущий матч в статистику при его предсказании
3. Используйте exclude_match_ids для фильтрации матчей
4. Фильтруйте про-матчи если работаете только с публичными данными
"""

from itertools import combinations


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


def _has_extreme_imp(match, threshold=30):
    """
    Проверяет есть ли игрок с экстремальным imp в матче.
    Используется для фильтрации лейнинга - экстремальный imp
    скорее показывает скилл игрока, а не силу драфта.
    
    Accuracy на тесте: 63.2% с threshold=30 (vs 61.8% без фильтра)
    """
    for p in match.get('players', []):
        imp = p.get('imp')
        if imp is not None and abs(imp) > threshold:
            return True
    return False


def lanes(match, lane_dict):
    """
    Обрабатывает данные по лайнам и записывает в lane_dict.
    
    ФИЛЬТР: исключает матчи с экстремальным imp (>30) для минимизации
    влияния скилла игроков на статистику лейнинга.
    
    Формирует:
    - Соло героя с позицией
    - Контрипики 2x2, 1x2, 2x1, 1x1 для каждого лайна
    - Синергию 1+1 для каждого лайна
    - Все ключи включают позиции героев
    
    Args:
        match: словарь с данными матча
        lane_dict: словарь для записи статистики по лайнам
    """
    # Фильтр: исключаем матчи с экстремальным imp
    if _has_extreme_imp(match, threshold=30):
        return
    
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



def check_comeback(match, lead_threshold=12000, min_duration=3):
    """
    Проверяет, был ли РЕАЛЬНЫЙ камбек в матче.
    
    Камбек: было >= lead_threshold преимущества за одной командой до 40 минуты,
    и это преимущество ДЕРЖАЛОСЬ минимум min_duration минут (не просто пик),
    но выиграла другая команда.
    
    ОПТИМИЗИРОВАНО экспериментально:
    - lead_12k_3min: 76.9% accuracy (147 матчей) - лучший результат
    - lead_10k_5min: 76.5% accuracy (170 матчей)
    - lead_10k_3min: 74.3% accuracy (748 матчей)
    
    Args:
        match: словарь с данными матча
        lead_threshold: минимальное преимущество в золоте (default: 12000)
        min_duration: минимальная длительность преимущества в минутах (default: 3)
    
    Returns:
        True если был камбек, False если нет
    """
    networth_leads = match.get('radiantNetworthLeads', [])
    
    # Проверяем наличие данных до 40 минуты
    if len(networth_leads) > 50 or len(networth_leads) < 30:
        return False
    
    # Определяем победителя
    did_radiant_win = match.get('didRadiantWin')
    if did_radiant_win is None:
        # Используем последний элемент winRates
        win_rates = match.get('winRates', [])
        if not win_rates:
            return False
        did_radiant_win = win_rates[-1] > 0.5
    
    # Проверяем СТАБИЛЬНОЕ преимущество (держалось минимум min_duration минут подряд)
    leads_to_check = networth_leads[:40]
    
    # Находим периоды стабильного преимущества >= threshold
    def has_stable_lead(leads, threshold, duration):
        """Проверяет был ли период где lead держался >= duration минут"""
        consecutive_count = 0
        for lead in leads:
            if (threshold > 0 and lead >= threshold) or (threshold < 0 and lead <= threshold):
                consecutive_count += 1
                if consecutive_count >= duration:
                    return True
            else:
                consecutive_count = 0
        return False
    
    had_stable_radiant_lead = has_stable_lead(leads_to_check, lead_threshold, min_duration)
    had_stable_dire_lead = has_stable_lead(leads_to_check, -lead_threshold, min_duration)
    
    # Камбек: если Radiant выиграл но было СТАБИЛЬНОЕ преимущество Dire >= threshold, 
    # или Dire выиграл но было СТАБИЛЬНОЕ преимущество Radiant >= threshold
    return (did_radiant_win and had_stable_dire_lead) or (not did_radiant_win and had_stable_radiant_lead)


def _dominant_lane_stomp(match, dominator, threshold=2):
    """
    Ограничивает влияние лейнинга на early: отбрасываем матчи,
    где доминант уже выиграл большинство лайнов.
    """
    outcomes = [
        match.get('topLaneOutcome'),
        match.get('midLaneOutcome'),
        match.get('bottomLaneOutcome')
    ]
    radiant_wins = 0
    dire_wins = 0
    for outcome in outcomes:
        if not outcome or not isinstance(outcome, str):
            continue
        up = outcome.upper()
        if 'RADIANT' in up:
            radiant_wins += 1
        elif 'DIRE' in up:
            dire_wins += 1
    if dominator == 'radiant' and radiant_wins >= threshold:
        return True
    if dominator == 'dire' and dire_wins >= threshold:
        return True
    return False


def _has_lane_stomp(match):
    """
    Проверяет, был ли stomp на каком-либо лайне (по строке outcome).
    Такие матчи исключаем из early, чтобы лейнинг не решал исход.
    """
    outcomes = [
        match.get('topLaneOutcome'),
        match.get('midLaneOutcome'),
        match.get('bottomLaneOutcome')
    ]
    for outcome in outcomes:
        if isinstance(outcome, str) and 'STOMP' in outcome.upper():
            return True
    return False


# ============================================================================
# НАСТРОЙКИ ФИЛЬТРОВ EARLY/LATE (подбираются экспериментально)
# ============================================================================
# Early: фильтруем лейнинг и ищем ранний перевес (draft strength в early).
EARLY_LANE_WINDOW = (8, 12)          # минута лейнинга (включительно)
EARLY_LANE_MAX_LEAD = 2000           # max |lead| в окне лейнинга
EARLY_MAX_LEAD_BEFORE = 3000         # max |lead| до конца лейнинга (0..12)
EARLY_LEAD_THRESHOLD = 7000          # порог раннего доминирования
EARLY_LEAD_WINDOW = (20, 27)         # окно для достижения порога
EARLY_STABLE_MINUTES = 2             # минимум минут подряд с lead >= threshold
EARLY_WIN_MAX_MINUTES = 34           # победа до этой минуты = early победа
EARLY_REQUIRE_THRESHOLD_FOR_FAST_WIN = True  # быстрые победы учитываем только если был threshold
EARLY_AVG_WINDOW = (20, 28)          # окно для средней доминации
EARLY_AVG_MIN_LEAD = 5000            # минимум среднего lead в early-окне
EARLY_LEAD_AT_15_MAX = 2500          # ограничение |lead| на 15-й минуте
EARLY_REQUIRE_MATCH_WINNER = True    # учитываем только матчи, где early-доминатор победил
EARLY_EXCLUDE_LANE_STOMP = True      # исключать stomp по лайнам
EARLY_EXCLUDE_DOMINANT_LANES = True  # исключать матчи с доминированием на 2+ лайнах
EARLY_DOMINANT_LANES_THRESHOLD = 2
EARLY_EXCLUDE_EXTREME_IMP = True    # исключать матчи с экстремальным imp
EARLY_EXTREME_IMP_THRESHOLD = 30

# Late: длинная игра без раннего snowball или с камбеком.
LATE_MIN_DURATION = 40
LATE_MAX_DURATION = 70  # None если не нужен верхний предел
LATE_EARLY_WINDOW = (15, 25)         # окно для оценки раннего snowball
LATE_EARLY_STOMP_MAX = 12000         # max |lead| для раннего snowball
LATE_COMEBACK_AVG_DEFICIT = 4000     # средний deficit победителя в 15-25
LATE_CLOSE_WINDOW = (20, 30)         # окно "близкой" игры
LATE_CLOSE_MAX_LEAD = 5000           # max |lead| в close-окне
LATE_MODE = 'comeback'               # 'either' | 'comeback' | 'close'
LATE_LEAD_AT_20_MAX = 8000           # ограничение |lead| на 20-й минуте для late
LATE_REQUIRE_EARLY_LOSS = True      # late = победитель не был early-доминатором
LATE_EXCLUDE_EXTREME_IMP = True      # исключать матчи с экстремальным imp
LATE_EXTREME_IMP_THRESHOLD = 30


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


def _first_stable_reach(leads, start, end, threshold, stable_minutes, sign):
    if len(leads) <= start:
        return None
    end = min(end, len(leads) - 1)
    consecutive = 0
    for i in range(start, end + 1):
        lead = leads[i]
        hit = lead >= threshold if sign > 0 else lead <= -threshold
        if hit:
            consecutive += 1
            if consecutive >= stable_minutes:
                return i - stable_minutes + 1
        else:
            consecutive = 0
    return None


def is_early_match(match, n: int = 3000):
    """
    Проверяет, подходит ли матч для early словаря.
    
    ЛОГИКА EARLY:
    - Лейнинг должен быть ровным (ограничиваем влияние лейнов)
    - Early победитель = кто первым стабильно достиг порога в окне 20-27,
      ИЛИ кто выиграл матч до EARLY_WIN_MAX_MINUTES
    
    Args:
        match: словарь с данными матча
        n: параметр сохранен для совместимости (не используется)
    
    Returns:
        tuple: (bool, winner) - (подходит ли матч, кто сделал камбек)
            winner: 'radiant' | 'dire' | None
    """
    leads = match.get('radiantNetworthLeads', [])
    did_radiant_win = match.get('didRadiantWin')
    duration = len(leads)
    
    if did_radiant_win is None:
        win_rates = match.get('winRates', [])
        did_radiant_win = win_rates[-1] > 0.5 if win_rates else None

    if did_radiant_win is None or duration <= EARLY_LANE_WINDOW[1]:
        return False, None

    if EARLY_EXCLUDE_EXTREME_IMP and _has_extreme_imp(match, threshold=EARLY_EXTREME_IMP_THRESHOLD):
        return False, None

    # Ровный лейнинг: фильтруем ранние snowball/стомпы
    lane_max_abs = _max_abs_in_window(leads, EARLY_LANE_WINDOW[0], EARLY_LANE_WINDOW[1])
    if lane_max_abs is None or lane_max_abs > EARLY_LANE_MAX_LEAD:
        return False, None
    pre_lane_max_abs = _max_abs_in_window(leads, 0, EARLY_LANE_WINDOW[1])
    if pre_lane_max_abs is None or pre_lane_max_abs > EARLY_MAX_LEAD_BEFORE:
        return False, None
    if len(leads) > 15 and abs(leads[15]) > EARLY_LEAD_AT_15_MAX:
        return False, None
    if EARLY_EXCLUDE_LANE_STOMP and _has_lane_stomp(match):
        return False, None

    # Ищем первую стабильную достижение порога в окне 20-27
    r_min = _first_stable_reach(
        leads, EARLY_LEAD_WINDOW[0], EARLY_LEAD_WINDOW[1],
        EARLY_LEAD_THRESHOLD, EARLY_STABLE_MINUTES, sign=1
    )
    d_min = _first_stable_reach(
        leads, EARLY_LEAD_WINDOW[0], EARLY_LEAD_WINDOW[1],
        EARLY_LEAD_THRESHOLD, EARLY_STABLE_MINUTES, sign=-1
    )

    # Быстрая победа: учитываем только если был threshold (если требуется)
    if duration <= EARLY_WIN_MAX_MINUTES and not EARLY_REQUIRE_THRESHOLD_FOR_FAST_WIN:
        dominator = 'radiant' if did_radiant_win else 'dire'
    else:
        if r_min is None and d_min is None:
            return False, None
        if r_min is not None and d_min is not None and r_min == d_min:
            return False, None
        if d_min is None or (r_min is not None and r_min < d_min):
            dominator = 'radiant'
        else:
            dominator = 'dire'

    # Среднее доминирование в early-окне
    avg_early = _avg_in_window(leads, EARLY_AVG_WINDOW[0], EARLY_AVG_WINDOW[1])
    if avg_early is None or abs(avg_early) < EARLY_AVG_MIN_LEAD:
        return False, None

    if EARLY_EXCLUDE_DOMINANT_LANES and _dominant_lane_stomp(match, dominator, threshold=EARLY_DOMINANT_LANES_THRESHOLD):
        return False, None

    if EARLY_REQUIRE_MATCH_WINNER:
        winner = 'radiant' if did_radiant_win else 'dire'
        if dominator != winner:
            return False, None

    return True, dominator


def is_late_match(match, dominator=None, if_check: bool = False, n: int = 7000):
    """
    Проверяет, подходит ли матч для late словаря.
    
    ЛОГИКА LATE:
    - Игра должна быть длинной (>= LATE_MIN_DURATION)
    - Исключаем ранний snowball, если он не перешел в камбек
    - Включаем камбеки и/или "ровные" игры, где late должен решать
    
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

    if LATE_EXCLUDE_EXTREME_IMP and _has_extreme_imp(match, threshold=LATE_EXTREME_IMP_THRESHOLD):
        return (False, None) if if_check else False

    if len(leads) > 20 and abs(leads[20]) > LATE_LEAD_AT_20_MAX:
        return (False, None) if if_check else False

    winner = 'radiant' if did_radiant_win else 'dire'
    
    # Камбек: победитель был в минусе на 15-25 (среднее значение)
    avg_early = _avg_in_window(leads, LATE_EARLY_WINDOW[0], LATE_EARLY_WINDOW[1])
    comeback = False
    if avg_early is not None:
        if winner == 'radiant' and avg_early <= -LATE_COMEBACK_AVG_DEFICIT:
            comeback = True
        elif winner == 'dire' and avg_early >= LATE_COMEBACK_AVG_DEFICIT:
            comeback = True

    # Ровная игра в mid/late-окне
    close_max_abs = _max_abs_in_window(leads, LATE_CLOSE_WINDOW[0], LATE_CLOSE_WINDOW[1])
    close_early = close_max_abs is not None and close_max_abs <= LATE_CLOSE_MAX_LEAD

    # Ранний snowball без камбека = не late
    early_max_abs = _max_abs_in_window(leads, LATE_EARLY_WINDOW[0], LATE_EARLY_WINDOW[1])
    early_stomp = early_max_abs is not None and early_max_abs >= LATE_EARLY_STOMP_MAX

    if dominator in ('radiant', 'dire') and dominator == winner and not comeback and early_stomp:
        return (False, None) if if_check else False

    if LATE_REQUIRE_EARLY_LOSS and dominator in ('radiant', 'dire') and dominator == winner:
        return (False, None) if if_check else False

    if LATE_MODE == 'comeback':
        if comeback:
            return (True, winner) if if_check else True
        return (False, None) if if_check else False
    if LATE_MODE == 'close':
        if close_early:
            return (True, winner) if if_check else True
        return (False, None) if if_check else False
    if comeback or close_early:
        return (True, winner) if if_check else True

    return (False, None) if if_check else False


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


def analise_database(match, lane_dict, early_dict, late_dict, comeback_dict=None, 
                     exclude_match_ids=None, exclude_pro_matches=True,dominator = None):
    """
    Основная функция анализа матча.
    
    Args:
        match: словарь с данными матча
        lane_dict: словарь для записи статистики по лайнам
        early_dict: словарь для записи статистики по early фазе
        late_dict: словарь для записи статистики по late фазе
        comeback_dict: словарь для записи статистики по камбекам (опционально)
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
    
    # 2. Извлекаем героев и позиции для early/late/comeback
    r_by_pos, d_by_pos = extract_heroes_by_position(match)
    if r_by_pos is None:
        return
    
    networth_leads = match.get('radiantNetworthLeads', [])
    match_duration = len(networth_leads)
    
    # Определяем победителя один раз (используется в late и comeback)
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
    
    # 5. Обработка COMEBACK словаря
    if comeback_dict is not None and check_comeback(match):
        # Записываем комбинации для команды которая сделала камбек
        r_val = 1 if did_radiant_win else 0
        d_val = 0 if did_radiant_win else 1
        _add_combinations_to_dict(r_by_pos, d_by_pos, comeback_dict, r_val, d_val)
    
    return True  # Матч успешно обработан
