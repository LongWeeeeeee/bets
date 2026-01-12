"""
Скрипт для тестирования разных фильтров early/late/lane.
Быстрый цикл: изменить фильтр -> построить словари -> проверить метрики.
"""

import json
import sys
from pathlib import Path
from typing import Tuple, Optional, Dict, Any

# Добавляем путь для импорта
sys.path.insert(0, str(Path(__file__).parent))

from itertools import combinations


# ============================================================================
# НАСТРОЙКИ ТЕСТА
# ============================================================================
# Путь к тестовым матчам (небольшой кусок для быстрого тестирования)
TRAIN_FILE = '/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object/combined1.json'
TEST_FILE = '/Users/alex/Documents/ingame/bets_data/analise_pub_matches/extracted_100k_matches.json'

# Лимит матчей для быстрого тестирования
TRAIN_LIMIT = 50000  # Сколько матчей брать для построения словарей
TEST_LIMIT = 30000   # Сколько матчей брать для проверки

# Минимальная дата (можно None)
from keys import start_date_time_739 as START_DATE_TIME


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================
def _append_to_dict(target_dict: Dict, key: str, value: int) -> None:
    """Добавляет значение в словарь со счётчиками."""
    if key not in target_dict:
        target_dict[key] = {'wins': 0, 'draws': 0, 'games': 0}
    target_dict[key]['games'] += 1
    if value == 1:
        target_dict[key]['wins'] += 1
    elif value == 0.5:
        target_dict[key]['draws'] += 1


def extract_heroes_by_position(match: Dict) -> Tuple[Optional[Dict], Optional[Dict]]:
    """Извлекает героев и позиции из матча."""
    r_by_pos = {}
    d_by_pos = {}
    
    for p in match.get('players', []):
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
    
    if len(r_by_pos) != 5 or len(d_by_pos) != 5:
        return None, None
    
    return r_by_pos, d_by_pos


def add_combinations_to_dict(r_by_pos: Dict, d_by_pos: Dict, target_dict: Dict, 
                              r_value: int, d_value: Optional[int] = None) -> None:
    """Добавляет все комбинации героев в словарь."""
    if d_value is None:
        d_value = r_value
    
    r_items = list(r_by_pos.items())
    d_items = list(d_by_pos.items())
    
    # Одиночные герои
    for pos_num, hero_id in r_items:
        _append_to_dict(target_dict, f'{hero_id}pos{pos_num}', r_value)
    for pos_num, hero_id in d_items:
        _append_to_dict(target_dict, f'{hero_id}pos{pos_num}', d_value)
    
    # Контрипики 1x1
    for r_pos, r_hero in r_items:
        for d_pos, d_hero in d_items:
            key = f'{r_hero}pos{r_pos}_vs_{d_hero}pos{d_pos}'
            _append_to_dict(target_dict, key, r_value)
    
    # Контрипики 1x2
    for r_pos, r_hero in r_items:
        for d_pos1, d_hero1 in d_items:
            for d_pos2, d_hero2 in d_items:
                if d_hero1 == d_hero2:
                    continue
                key = f'{r_hero}pos{r_pos}_vs_{d_hero1}pos{d_pos1},{d_hero2}pos{d_pos2}'
                _append_to_dict(target_dict, key, r_value)
    
    # Синергия duo
    for r_pos1, r_hero1 in r_items:
        for r_pos2, r_hero2 in r_items:
            if r_hero1 == r_hero2:
                continue
            key = f'{r_hero1}pos{r_pos1}_with_{r_hero2}pos{r_pos2}'
            _append_to_dict(target_dict, key, r_value)
    
    for d_pos1, d_hero1 in d_items:
        for d_pos2, d_hero2 in d_items:
            if d_hero1 == d_hero2:
                continue
            key = f'{d_hero1}pos{d_pos1}_with_{d_hero2}pos{d_pos2}'
            _append_to_dict(target_dict, key, d_value)
    
    # Трио синергия
    for i in range(len(r_items)):
        for j in range(i + 1, len(r_items)):
            for k in range(j + 1, len(r_items)):
                r_pos1, r_hero1 = r_items[i]
                r_pos2, r_hero2 = r_items[j]
                r_pos3, r_hero3 = r_items[k]
                key = f'{r_hero1}pos{r_pos1},{r_hero2}pos{r_pos2},{r_hero3}pos{r_pos3}'
                _append_to_dict(target_dict, key, r_value)
    
    for i in range(len(d_items)):
        for j in range(i + 1, len(d_items)):
            for k in range(j + 1, len(d_items)):
                d_pos1, d_hero1 = d_items[i]
                d_pos2, d_hero2 = d_items[j]
                d_pos3, d_hero3 = d_items[k]
                key = f'{d_hero1}pos{d_pos1},{d_hero2}pos{d_pos2},{d_hero3}pos{d_pos3}'
                _append_to_dict(target_dict, key, d_value)


# ============================================================================
# ФИЛЬТРЫ ДЛЯ ТЕСТИРОВАНИЯ
# ============================================================================

def is_early_match_v1(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 1: Простой фильтр по lead на 20-25 минутах.
    Доминант = кто имел lead >= 5k на 20-25 мин.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 40:
        return False, None
    
    # Проверяем lead на 20-25 минутах
    for i in range(20, min(26, duration)):
        if leads[i] >= 5000:
            return True, 'radiant'
        elif leads[i] <= -5000:
            return True, 'dire'
    
    return False, None


def is_early_match_v2(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 2: Фильтр по среднему lead на 15-25 минутах.
    Доминант = кто имел средний lead >= 4k.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    # Средний lead на 15-25 минутах
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 4000:
        return True, 'radiant'
    elif avg_lead <= -4000:
        return True, 'dire'
    
    return False, None


def is_early_match_v3(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 3: Строгий фильтр - равенство на лейнинге + доминирование после.
    Расширен диапазон до 50 мин.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 50:
        return False, None
    
    # Равенство на 8-12 минутах (лейнинг не решил)
    if any(abs(leads[i]) >= 3000 for i in range(8, min(13, duration))):
        return False, None
    
    # Доминирование на 18-28 минутах (расширен диапазон)
    for i in range(18, min(29, duration)):
        if leads[i] >= 6000:
            return True, 'radiant'
        elif leads[i] <= -6000:
            return True, 'dire'
    
    return False, None


def is_late_match_v1(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 1: Простой фильтр - матч 35+ минут.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    return duration >= 35 and duration <= 60


def is_late_match_v2(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 2: Матч 35+ минут где не было раннего доминирования.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 35 or duration > 60:
        return False
    
    # Не было раннего доминирования (lead < 8k на 15-25 мин)
    if duration >= 25:
        max_early_lead = max(abs(leads[i]) for i in range(15, 25))
        if max_early_lead >= 8000:
            return False
    
    return True


def is_late_match_v3(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 3: Камбек - победитель проигрывал на 15-25 минутах.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    # Средний lead на 15-25 минутах
    avg_lead = sum(leads[15:25]) / 10
    
    # Камбек: победитель проигрывал
    if did_radiant_win and avg_lead <= -3000:
        return True
    if not did_radiant_win and avg_lead >= 3000:
        return True
    
    return False


# ============================================================================
# ОСНОВНОЙ ТЕСТ
# ============================================================================

def build_dicts(matches: Dict, early_filter, late_filter) -> Tuple[Dict, Dict]:
    """Строит early и late словари с заданными фильтрами."""
    early_dict = {}
    late_dict = {}
    
    early_count = 0
    late_count = 0
    
    for match_id, match in matches.items():
        if not isinstance(match, dict):
            continue
        if 'players' not in match or len(match.get('players', [])) != 10:
            continue
        if START_DATE_TIME and match.get('startDateTime', 0) < int(START_DATE_TIME):
            continue
        
        r_by_pos, d_by_pos = extract_heroes_by_position(match)
        if r_by_pos is None:
            continue
        
        did_radiant_win = match.get('didRadiantWin', False)
        
        # Early
        early_result, dominator = early_filter(match)
        if early_result and dominator:
            r_val = 1 if dominator == 'radiant' else 0
            d_val = 1 if dominator == 'dire' else 0
            add_combinations_to_dict(r_by_pos, d_by_pos, early_dict, r_val, d_val)
            early_count += 1
        
        # Late
        if late_filter(match, dominator):
            r_val = 1 if did_radiant_win else 0
            d_val = 0 if did_radiant_win else 1
            add_combinations_to_dict(r_by_pos, d_by_pos, late_dict, r_val, d_val)
            late_count += 1
    
    print(f"  Early матчей: {early_count}, Late матчей: {late_count}")
    return early_dict, late_dict


def test_filter_combination(name: str, early_filter_train, late_filter_train,
                           early_filter_test=None, late_filter_test=None):
    """
    Тестирует комбинацию фильтров.
    train фильтры - строгие, для построения словарей
    test фильтры - мягкие, как человек смотрит на ставку
    """
    print(f"\n{'='*60}")
    print(f"ТЕСТ: {name}")
    print('='*60)
    
    # Если test фильтры не заданы, используем train фильтры
    if early_filter_test is None:
        early_filter_test = early_filter_train
    if late_filter_test is None:
        late_filter_test = late_filter_train
    
    # Загружаем train данные
    print(f"Загрузка train данных из {TRAIN_FILE}...")
    with open(TRAIN_FILE, 'r') as f:
        train_matches = json.load(f)
    
    # Ограничиваем количество
    if TRAIN_LIMIT and len(train_matches) > TRAIN_LIMIT:
        train_matches = dict(list(train_matches.items())[:TRAIN_LIMIT])
    print(f"  Загружено: {len(train_matches)} матчей")
    
    # Строим словари
    print("Построение словарей...")
    early_dict, late_dict = build_dicts(train_matches, early_filter_train, late_filter_train)
    print(f"  Early dict: {len(early_dict)} ключей")
    print(f"  Late dict: {len(late_dict)} ключей")
    
    # Сохраняем временно
    stats_dir = Path('/Users/alex/Documents/ingame/bets_data/analise_pub_matches')
    with open(stats_dir / 'early_dict_test.json', 'w') as f:
        json.dump(early_dict, f)
    with open(stats_dir / 'late_dict_test.json', 'w') as f:
        json.dump(late_dict, f)
    
    # Загружаем test данные
    print(f"\nЗагрузка test данных из {TEST_FILE}...")
    with open(TEST_FILE, 'r') as f:
        test_matches = json.load(f)
    
    if TEST_LIMIT and len(test_matches) > TEST_LIMIT:
        test_matches = dict(list(test_matches.items())[:TEST_LIMIT])
    print(f"  Загружено: {len(test_matches)} матчей")
    
    # Проверяем метрики
    print("\nПроверка метрик...")
    from functions import synergy_and_counterpick, check_bad_map
    
    early_correct = 0
    early_total = 0
    late_correct = 0
    late_total = 0
    
    # Счётчики для разных метрик
    early_syn_correct = 0
    early_syn_total = 0
    late_solo_correct = 0
    late_solo_total = 0
    late_syn_correct = 0
    late_syn_total = 0
    late_cp_correct = 0
    late_cp_total = 0
    
    for match_id, match in test_matches.items():
        result = check_bad_map(match=match, maps_data=test_matches)
        if result is None:
            continue
        
        radiant_heroes_and_pos, dire_heroes_and_pos = result
        did_radiant_win = match.get('didRadiantWin', False)
        
        # Применяем test фильтры
        early_result, dominator = early_filter_test(match)
        late_result = late_filter_test(match, dominator)
        
        # Early метрики - synergy_duo
        if early_result and dominator:
            s = synergy_and_counterpick(
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos,
                early_dict=early_dict,
                mid_dict={},
            ) or {}
            
            early_output = s.get('early_output', {})
            
            # counterpick_1vs1
            cp_1vs1 = early_output.get('counterpick_1vs1')
            if cp_1vs1 is not None and abs(cp_1vs1) >= 10:
                early_total += 1
                if (cp_1vs1 > 0 and dominator == 'radiant') or (cp_1vs1 < 0 and dominator == 'dire'):
                    early_correct += 1
            
            # synergy_duo
            syn_duo = early_output.get('synergy_duo')
            if syn_duo is not None and abs(syn_duo) >= 10:
                early_syn_total += 1
                if (syn_duo > 0 and dominator == 'radiant') or (syn_duo < 0 and dominator == 'dire'):
                    early_syn_correct += 1
        
        # Late метрики - solo winrate + synergy_duo + counterpick
        if late_result:
            s = synergy_and_counterpick(
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos,
                early_dict={},
                mid_dict=late_dict,
            ) or {}
            
            late_output = s.get('mid_output', {})
            
            # solo
            late_solo = late_output.get('solo')
            if late_solo is not None and abs(late_solo) >= 10:
                late_solo_total += 1
                if (late_solo > 0 and did_radiant_win) or (late_solo < 0 and not did_radiant_win):
                    late_solo_correct += 1
            
            # synergy_duo
            late_syn = late_output.get('synergy_duo')
            if late_syn is not None and abs(late_syn) >= 10:
                late_syn_total += 1
                if (late_syn > 0 and did_radiant_win) or (late_syn < 0 and not did_radiant_win):
                    late_syn_correct += 1
            
            # counterpick_1vs1
            late_cp = late_output.get('counterpick_1vs1')
            if late_cp is not None and abs(late_cp) >= 10:
                late_cp_total += 1
                if (late_cp > 0 and did_radiant_win) or (late_cp < 0 and not did_radiant_win):
                    late_cp_correct += 1
    
    # Результаты
    print(f"\n📊 РЕЗУЛЬТАТЫ (MIN_INDEX >= 10):")
    if early_total > 0:
        print(f"  Early cp_1vs1:    {early_correct}/{early_total} = {early_correct/early_total:.1%}")
    else:
        print(f"  Early cp_1vs1: нет данных")
    
    if early_syn_total > 0:
        print(f"  Early syn_duo:    {early_syn_correct}/{early_syn_total} = {early_syn_correct/early_syn_total:.1%}")
    else:
        print(f"  Early syn_duo: нет данных")
    
    if late_solo_total > 0:
        print(f"  Late solo:        {late_solo_correct}/{late_solo_total} = {late_solo_correct/late_solo_total:.1%}")
    else:
        print(f"  Late solo: нет данных")
    
    if late_syn_total > 0:
        print(f"  Late syn_duo:     {late_syn_correct}/{late_syn_total} = {late_syn_correct/late_syn_total:.1%}")
    else:
        print(f"  Late syn_duo: нет данных")
    
    if late_cp_total > 0:
        print(f"  Late cp_1vs1:     {late_cp_correct}/{late_cp_total} = {late_cp_correct/late_cp_total:.1%}")
    else:
        print(f"  Late cp_1vs1: нет данных")
    
    return {
        'name': name,
        'early_cp_accuracy': early_correct/early_total if early_total > 0 else 0,
        'early_cp_total': early_total,
        'early_syn_accuracy': early_syn_correct/early_syn_total if early_syn_total > 0 else 0,
        'early_syn_total': early_syn_total,
        'late_solo_accuracy': late_solo_correct/late_solo_total if late_solo_total > 0 else 0,
        'late_solo_total': late_solo_total,
        'late_cp_accuracy': late_cp_correct/late_cp_total if late_cp_total > 0 else 0,
        'late_cp_total': late_cp_total,
        'late_syn_accuracy': late_syn_correct/late_syn_total if late_syn_total > 0 else 0,
        'late_syn_total': late_syn_total,
    }


def is_early_match_v4(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 4: Средний lead 5k на 15-25 минутах (строже чем V2).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 5000:
        return True, 'radiant'
    elif avg_lead <= -5000:
        return True, 'dire'
    
    return False, None


def is_early_match_v5(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 5: Средний lead 3k на 18-28 минутах (мягче, позже).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 28 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[18:28]) / 10
    
    if avg_lead >= 3000:
        return True, 'radiant'
    elif avg_lead <= -3000:
        return True, 'dire'
    
    return False, None


def is_early_match_v6(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 6: Стабильный lead >= 4k минимум 5 минут подряд на 15-30.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    # Ищем 5 минут подряд с lead >= 4k
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 4000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 5:
                return True, 'radiant'
        elif leads[i] <= -4000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 5:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_late_match_v4(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 4: Матч 40+ минут (строже).
    """
    leads = match.get('radiantNetworthLeads', [])
    return len(leads) >= 40 and len(leads) <= 60


def is_late_match_v5(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 5: Матч 35+ минут где на 30 мин lead < 10k.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 35 or duration > 60:
        return False
    
    if duration >= 30 and abs(leads[29]) >= 10000:
        return False
    
    return True


def is_early_match_v7(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 7: Средний lead 6k на 15-25 минутах (ещё строже).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 6000:
        return True, 'radiant'
    elif avg_lead <= -6000:
        return True, 'dire'
    
    return False, None


def is_early_match_v8(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 8: Средний lead 4k на 12-22 минутах (раньше).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 22 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[12:22]) / 10
    
    if avg_lead >= 4000:
        return True, 'radiant'
    elif avg_lead <= -4000:
        return True, 'dire'
    
    return False, None


def is_late_match_v6(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 6: Матч 38+ минут где avg lead на 15-25 < 6k.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 38 or duration > 60:
        return False
    
    if duration >= 25:
        avg_early = sum(abs(leads[i]) for i in range(15, 25)) / 10
        if avg_early >= 6000:
            return False
    
    return True


def is_late_match_v7(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 7: Камбек строже - победитель проигрывал avg 5k на 15-25.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -5000:
        return True
    if not did_radiant_win and avg_lead >= 5000:
        return True
    
    return False


def is_early_match_v9(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 9: Средний lead 3k на 15-25 минутах (мягче чем V2).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 3000:
        return True, 'radiant'
    elif avg_lead <= -3000:
        return True, 'dire'
    
    return False, None


def is_early_match_v10(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 10: Пик lead >= 7k на 15-30 минутах (не средний, а максимум).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    max_lead = max(leads[15:30])
    min_lead = min(leads[15:30])
    
    if max_lead >= 7000:
        return True, 'radiant'
    elif min_lead <= -7000:
        return True, 'dire'
    
    return False, None


def is_early_match_v11(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 11: Средний lead 4k на 20-30 минутах (позже).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    avg_lead = sum(leads[20:30]) / 10
    
    if avg_lead >= 4000:
        return True, 'radiant'
    elif avg_lead <= -4000:
        return True, 'dire'
    
    return False, None


def is_late_match_v8(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 8: Матч 35+ минут где на 25 мин lead < 5k (близкая игра).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 35 or duration > 60:
        return False
    
    if duration >= 25 and abs(leads[24]) >= 5000:
        return False
    
    return True


def is_late_match_v9(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 9: Матч 40+ минут где avg lead на 20-30 < 5k.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 40 or duration > 65:
        return False
    
    if duration >= 30:
        avg_mid = sum(abs(leads[i]) for i in range(20, 30)) / 10
        if avg_mid >= 5000:
            return False
    
    return True


def is_late_match_v10(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 10: Матч 35+ минут, не было раннего доминирования (max lead < 6k на 15-25).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 35 or duration > 60:
        return False
    
    if duration >= 25:
        max_early_lead = max(abs(leads[i]) for i in range(15, 25))
        if max_early_lead >= 6000:
            return False
    
    return True


def is_late_match_v11(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 11: Очень длинный матч 45+ минут.
    """
    leads = match.get('radiantNetworthLeads', [])
    return len(leads) >= 45 and len(leads) <= 70


def is_late_match_v12(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 12: Матч 35+ где lead менялся (был и + и - на 20-35).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 35 or duration > 60:
        return False
    
    # Проверяем что lead менялся
    had_radiant_lead = any(leads[i] >= 3000 for i in range(20, min(35, duration)))
    had_dire_lead = any(leads[i] <= -3000 for i in range(20, min(35, duration)))
    
    return had_radiant_lead and had_dire_lead


def is_late_match_v13(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 13: Матч 40+ где на 30 мин было близко (lead < 3k).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 40 or duration > 65:
        return False
    
    if duration >= 30 and abs(leads[29]) >= 3000:
        return False
    
    return True


def is_late_match_v14(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 14: Камбек мягче - победитель проигрывал avg 2k на 15-25.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -2000:
        return True
    if not did_radiant_win and avg_lead >= 2000:
        return True
    
    return False


def is_late_match_v15(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 15: Камбек строже - победитель проигрывал avg 4k на 15-25.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -4000:
        return True
    if not did_radiant_win and avg_lead >= 4000:
        return True
    
    return False


def is_late_match_v16(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 16: Камбек ещё мягче - победитель проигрывал avg 1k на 15-25.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -1000:
        return True
    if not did_radiant_win and avg_lead >= 1000:
        return True
    
    return False


def is_late_match_v17(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 17: Камбек на 20-30 минутах вместо 15-25.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[20:30]) / 10
    
    if did_radiant_win and avg_lead <= -2000:
        return True
    if not did_radiant_win and avg_lead >= 2000:
        return True
    
    return False


def is_late_match_v18(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 18: Камбек 3k на 15-25.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -3000:
        return True
    if not did_radiant_win and avg_lead >= 3000:
        return True
    
    return False


def is_late_match_v19(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 19: Камбек 5k на 15-25.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -5000:
        return True
    if not did_radiant_win and avg_lead >= 5000:
        return True
    
    return False


def is_late_match_v20(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 20: Камбек 6k на 15-25.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -6000:
        return True
    if not did_radiant_win and avg_lead >= 6000:
        return True
    
    return False


def is_late_match_v21(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 21: Камбек 4k на 15-25, матч 40+ минут.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 40 or duration > 65:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -4000:
        return True
    if not did_radiant_win and avg_lead >= 4000:
        return True
    
    return False


def is_late_match_v22(match: Dict, dominator: Optional[str]) -> bool:
    """
    ВАРИАНТ 22: Камбек 4k на 18-28.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[18:28]) / 10
    
    if did_radiant_win and avg_lead <= -4000:
        return True
    if not did_radiant_win and avg_lead >= 4000:
        return True
    
    return False


def is_early_match_v12(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 12: V3 но мягче - равенство на лейнинге (< 2k) + доминирование 5k после.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    # Равенство на 8-12 минутах (лейнинг не решил)
    if any(abs(leads[i]) >= 2000 for i in range(8, min(13, duration))):
        return False, None
    
    # Доминирование на 18-25 минутах
    for i in range(18, min(26, duration)):
        if leads[i] >= 5000:
            return True, 'radiant'
        elif leads[i] <= -5000:
            return True, 'dire'
    
    return False, None


def is_early_match_v13(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 13: Строже V3 - равенство на лейнинге (< 2k) + доминирование 8k после.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    # Равенство на 8-12 минутах
    if any(abs(leads[i]) >= 2000 for i in range(8, min(13, duration))):
        return False, None
    
    # Сильное доминирование на 18-28 минутах
    for i in range(18, min(29, duration)):
        if leads[i] >= 8000:
            return True, 'radiant'
        elif leads[i] <= -8000:
            return True, 'dire'
    
    return False, None


def is_early_match_v14(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 14: Avg lead 5k на 18-28 минутах (позже и строже).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 28 or duration > 50:
        return False, None
    
    avg_lead = sum(leads[18:28]) / 10
    
    if avg_lead >= 5000:
        return True, 'radiant'
    elif avg_lead <= -5000:
        return True, 'dire'
    
    return False, None


def is_early_match_v15(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 15: Стабильный lead >= 5k минимум 7 минут подряд на 15-30.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 5000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 7:
                return True, 'radiant'
        elif leads[i] <= -5000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 7:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_v3a(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 3a: V3 но мягче - равенство < 4k на лейнинге + доминирование 5k после.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 50:
        return False, None
    
    # Равенство на 8-12 минутах (мягче)
    if any(abs(leads[i]) >= 4000 for i in range(8, min(13, duration))):
        return False, None
    
    # Доминирование на 18-28 минутах (мягче)
    for i in range(18, min(29, duration)):
        if leads[i] >= 5000:
            return True, 'radiant'
        elif leads[i] <= -5000:
            return True, 'dire'
    
    return False, None


def is_early_match_v3b(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 3b: V3 но строже - равенство < 2k на лейнинге + доминирование 7k после.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 50:
        return False, None
    
    # Равенство на 8-12 минутах (строже)
    if any(abs(leads[i]) >= 2000 for i in range(8, min(13, duration))):
        return False, None
    
    # Доминирование на 18-28 минутах (строже)
    for i in range(18, min(29, duration)):
        if leads[i] >= 7000:
            return True, 'radiant'
        elif leads[i] <= -7000:
            return True, 'dire'
    
    return False, None


def is_early_match_v3c(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 3c: V3 но avg lead вместо пика - равенство < 3k + avg lead 5k на 18-28.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 28 or duration > 50:
        return False, None
    
    # Равенство на 8-12 минутах
    if any(abs(leads[i]) >= 3000 for i in range(8, min(13, duration))):
        return False, None
    
    # Средний lead на 18-28 минутах
    avg_lead = sum(leads[18:28]) / 10
    
    if avg_lead >= 5000:
        return True, 'radiant'
    elif avg_lead <= -5000:
        return True, 'dire'
    
    return False, None


def is_early_match_v16(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 16: Avg lead 5k на 15-25 (строже V2).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 5000:
        return True, 'radiant'
    elif avg_lead <= -5000:
        return True, 'dire'
    
    return False, None


def is_early_match_v17(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 17: Avg lead 6k на 15-25 (ещё строже).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 6000:
        return True, 'radiant'
    elif avg_lead <= -6000:
        return True, 'dire'
    
    return False, None


def is_early_match_v18(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 18: Avg lead 4.5k на 15-25.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 4500:
        return True, 'radiant'
    elif avg_lead <= -4500:
        return True, 'dire'
    
    return False, None


def is_early_match_v19(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    ВАРИАНТ 19: Avg lead 4k на 15-25 + матч < 35 мин (быстрая победа).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 35:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 4000:
        return True, 'radiant'
    elif avg_lead <= -4000:
        return True, 'dire'
    
    return False, None


def is_early_match_comeback(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    Comeback для early: проигрывал на 8-15 мин, но доминировал на 20-28.
    Доминатор = кто сделал камбек.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 28 or duration > 50:
        return False, None
    
    # Средний lead на 8-15 (лейнинг)
    avg_early = sum(leads[8:15]) / 7
    # Средний lead на 20-28 (мид гейм)
    avg_mid = sum(leads[20:28]) / 8
    
    # Radiant камбек: проигрывал лейнинг, выиграл мид
    if avg_early <= -2000 and avg_mid >= 3000:
        return True, 'radiant'
    # Dire камбек
    if avg_early >= 2000 and avg_mid <= -3000:
        return True, 'dire'
    
    return False, None


def is_early_match_comeback_v2(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    Comeback для early v2: проигрывал на 10-18 мин, но доминировал на 22-30.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    avg_early = sum(leads[10:18]) / 8
    avg_mid = sum(leads[22:30]) / 8
    
    if avg_early <= -2000 and avg_mid >= 4000:
        return True, 'radiant'
    if avg_early >= 2000 and avg_mid <= -4000:
        return True, 'dire'
    
    return False, None


def is_early_match_no_filter(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    Без фильтра - все матчи. Доминатор = победитель.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 20 or duration > 60:
        return False, None
    
    did_radiant_win = match.get('didRadiantWin', False)
    return True, 'radiant' if did_radiant_win else 'dire'


def is_late_match_no_filter(match: Dict, dominator: Optional[str]) -> bool:
    """
    Без фильтра - все матчи 25+ минут.
    """
    leads = match.get('radiantNetworthLeads', [])
    return len(leads) >= 25 and len(leads) <= 60


# ============================================================================
# МЯГКИЕ ФИЛЬТРЫ ДЛЯ ТЕСТИРОВАНИЯ (как человек смотрит на ставку)
# ============================================================================

def is_early_test_soft(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    Мягкий фильтр для early test: после лейнинга нет сноуболла.
    Lead < 5k на 15 мин - игра ещё не решена.
    Доминатор = кто имеет avg lead на 20-28.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 28 or duration > 50:
        return False, None
    
    # Нет сноуболла после лейнинга
    if abs(leads[14]) >= 5000:
        return False, None
    
    # Доминатор по avg lead на 20-28
    avg_lead = sum(leads[20:28]) / 8
    
    if avg_lead >= 2000:
        return True, 'radiant'
    elif avg_lead <= -2000:
        return True, 'dire'
    
    return False, None


def is_late_test_soft(match: Dict, dominator: Optional[str]) -> bool:
    """
    Мягкий фильтр для late test: игра была равная к концу early.
    Lead < 7k на 28 мин - можно поверить в камбек.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 35 or duration > 60:
        return False
    
    # Игра была равная к концу early
    if duration >= 28 and abs(leads[27]) >= 7000:
        return False
    
    return True


def is_late_test_soft_v2(match: Dict, dominator: Optional[str]) -> bool:
    """
    Мягкий фильтр v2: lead < 6k на 28 мин.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 35 or duration > 60:
        return False
    
    if duration >= 28 and abs(leads[27]) >= 6000:
        return False
    
    return True


def is_late_test_soft_v3(match: Dict, dominator: Optional[str]) -> bool:
    """
    Мягкий фильтр v3: avg lead < 5k на 20-28 мин.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 35 or duration > 60:
        return False
    
    if duration >= 28:
        avg_lead = sum(abs(leads[i]) for i in range(20, 28)) / 8
        if avg_lead >= 5000:
            return False
    
    return True


def is_late_match_v2_10k(match: Dict, dominator: Optional[str]) -> bool:
    """
    35+ min, no early dom 10k (мягче).
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 35 or duration > 60:
        return False
    
    if duration >= 25:
        max_early_lead = max(abs(leads[i]) for i in range(15, 25))
        if max_early_lead >= 10000:
            return False
    
    return True


def is_late_match_v2_40min(match: Dict, dominator: Optional[str]) -> bool:
    """
    40+ min, no early dom 8k.
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 40 or duration > 65:
        return False
    
    if duration >= 25:
        max_early_lead = max(abs(leads[i]) for i in range(15, 25))
        if max_early_lead >= 8000:
            return False
    
    return True


def is_early_match_stable3k_5min(match: Dict) -> Tuple[bool, Optional[str]]:
    """Stable lead >= 3k минимум 5 минут подряд на 15-30."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 3000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 5:
                return True, 'radiant'
        elif leads[i] <= -3000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 5:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_stable5k_5min(match: Dict) -> Tuple[bool, Optional[str]]:
    """Stable lead >= 5k минимум 5 минут подряд на 15-30."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 5000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 5:
                return True, 'radiant'
        elif leads[i] <= -5000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 5:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_stable4k_4min(match: Dict) -> Tuple[bool, Optional[str]]:
    """Stable lead >= 4k минимум 4 минуты подряд на 15-30."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 4000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 4:
                return True, 'radiant'
        elif leads[i] <= -4000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 4:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_stable4k_6min(match: Dict) -> Tuple[bool, Optional[str]]:
    """Stable lead >= 4k минимум 6 минут подряд на 15-30."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 4000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 6:
                return True, 'radiant'
        elif leads[i] <= -4000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 6:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_stable3k_4min(match: Dict) -> Tuple[bool, Optional[str]]:
    """Stable lead >= 3k минимум 4 минуты подряд на 15-30."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 3000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 4:
                return True, 'radiant'
        elif leads[i] <= -3000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 4:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_stable4k_3min(match: Dict) -> Tuple[bool, Optional[str]]:
    """Stable lead >= 4k минимум 3 минуты подряд на 15-30."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 4000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 3:
                return True, 'radiant'
        elif leads[i] <= -4000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 3:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_stable35k_4min(match: Dict) -> Tuple[bool, Optional[str]]:
    """Stable lead >= 3.5k минимум 4 минуты подряд на 15-30."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 3500:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 4:
                return True, 'radiant'
        elif leads[i] <= -3500:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 4:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_current(match: Dict) -> Tuple[bool, Optional[str]]:
    """
    Текущий фильтр из analise_database.py:
    - Равенство на 8-12 мин (lead < 2k)
    - Доминирование 6k на 20-28 мин
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25:
        return False, None
    
    # Равенство на 8-12 минутах
    if any(abs(leads[i]) >= 2000 for i in range(8, min(12, duration))):
        return False, None
    
    # Доминирование на 20-28 минутах
    for i in range(20, min(28, duration)):
        if leads[i] >= 6000:
            return True, 'radiant'
        elif leads[i] <= -6000:
            return True, 'dire'
    
    return False, None


def is_late_match_current(match: Dict, dominator: Optional[str]) -> bool:
    """
    Текущий фильтр из analise_database.py:
    - Если dominator=None и duration >= 32 → True
    - Если dominator != None и duration >= 39 → True
    """
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration > 60:
        return False
    
    if dominator is None and duration >= 32:
        return True
    
    if dominator is not None and duration >= 39:
        return True
    
    return False


def is_early_match_avg7k(match: Dict) -> Tuple[bool, Optional[str]]:
    """Avg lead >= 7k на 15-25 минутах."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 7000:
        return True, 'radiant'
    elif avg_lead <= -7000:
        return True, 'dire'
    
    return False, None


def is_early_match_avg8k(match: Dict) -> Tuple[bool, Optional[str]]:
    """Avg lead >= 8k на 15-25 минутах."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 8000:
        return True, 'radiant'
    elif avg_lead <= -8000:
        return True, 'dire'
    
    return False, None


def is_early_match_v3_8k(match: Dict) -> Tuple[bool, Optional[str]]:
    """V3 но строже: равенство < 3k на лейнинге + dom 8k после."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 50:
        return False, None
    
    # Равенство на 8-12 минутах
    if any(abs(leads[i]) >= 3000 for i in range(8, min(13, duration))):
        return False, None
    
    # Доминирование 8k на 18-28 минутах
    for i in range(18, min(29, duration)):
        if leads[i] >= 8000:
            return True, 'radiant'
        elif leads[i] <= -8000:
            return True, 'dire'
    
    return False, None


def is_early_match_avg9k(match: Dict) -> Tuple[bool, Optional[str]]:
    """Avg lead >= 9k на 15-25 минутах."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 9000:
        return True, 'radiant'
    elif avg_lead <= -9000:
        return True, 'dire'
    
    return False, None


def is_early_match_avg10k(match: Dict) -> Tuple[bool, Optional[str]]:
    """Avg lead >= 10k на 15-25 минутах."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 10000:
        return True, 'radiant'
    elif avg_lead <= -10000:
        return True, 'dire'
    
    return False, None


def is_early_match_v3_10k(match: Dict) -> Tuple[bool, Optional[str]]:
    """V3 но очень строго: равенство < 3k на лейнинге + dom 10k после."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 50:
        return False, None
    
    # Равенство на 8-12 минутах
    if any(abs(leads[i]) >= 3000 for i in range(8, min(13, duration))):
        return False, None
    
    # Доминирование 10k на 18-28 минутах
    for i in range(18, min(29, duration)):
        if leads[i] >= 10000:
            return True, 'radiant'
        elif leads[i] <= -10000:
            return True, 'dire'
    
    return False, None


def is_early_match_avg12k(match: Dict) -> Tuple[bool, Optional[str]]:
    """Avg lead >= 12k на 15-25 минутах."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 12000:
        return True, 'radiant'
    elif avg_lead <= -12000:
        return True, 'dire'
    
    return False, None


def is_early_match_avg15k(match: Dict) -> Tuple[bool, Optional[str]]:
    """Avg lead >= 15k на 15-25 минутах."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 45:
        return False, None
    
    avg_lead = sum(leads[15:25]) / 10
    
    if avg_lead >= 15000:
        return True, 'radiant'
    elif avg_lead <= -15000:
        return True, 'dire'
    
    return False, None


def is_early_match_stable6k_5min(match: Dict) -> Tuple[bool, Optional[str]]:
    """Stable lead >= 6k минимум 5 минут подряд на 15-30."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 6000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 5:
                return True, 'radiant'
        elif leads[i] <= -6000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 5:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_stable7k_5min(match: Dict) -> Tuple[bool, Optional[str]]:
    """Stable lead >= 7k минимум 5 минут подряд на 15-30."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 7000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 5:
                return True, 'radiant'
        elif leads[i] <= -7000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 5:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_stable8k_4min(match: Dict) -> Tuple[bool, Optional[str]]:
    """Stable lead >= 8k минимум 4 минуты подряд на 15-30."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    consecutive_r = 0
    consecutive_d = 0
    
    for i in range(15, min(30, duration)):
        if leads[i] >= 8000:
            consecutive_r += 1
            consecutive_d = 0
            if consecutive_r >= 4:
                return True, 'radiant'
        elif leads[i] <= -8000:
            consecutive_d += 1
            consecutive_r = 0
            if consecutive_d >= 4:
                return True, 'dire'
        else:
            consecutive_r = 0
            consecutive_d = 0
    
    return False, None


def is_early_match_v3_12k(match: Dict) -> Tuple[bool, Optional[str]]:
    """V3 очень строго: равенство < 3k на лейнинге + dom 12k после."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 50:
        return False, None
    
    # Равенство на 8-12 минутах
    if any(abs(leads[i]) >= 3000 for i in range(8, min(13, duration))):
        return False, None
    
    # Доминирование 12k на 18-28 минутах
    for i in range(18, min(29, duration)):
        if leads[i] >= 12000:
            return True, 'radiant'
        elif leads[i] <= -12000:
            return True, 'dire'
    
    return False, None


def is_early_match_v3_15k(match: Dict) -> Tuple[bool, Optional[str]]:
    """V3 экстремально строго: равенство < 3k на лейнинге + dom 15k после."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 25 or duration > 50:
        return False, None
    
    # Равенство на 8-12 минутах
    if any(abs(leads[i]) >= 3000 for i in range(8, min(13, duration))):
        return False, None
    
    # Доминирование 15k на 18-28 минутах
    for i in range(18, min(29, duration)):
        if leads[i] >= 15000:
            return True, 'radiant'
        elif leads[i] <= -15000:
            return True, 'dire'
    
    return False, None


def is_late_match_comeback_4k(match: Dict, dominator: Optional[str]) -> bool:
    """Камбек 4k на 15-25 (победитель проигрывал)."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -4000:
        return True
    if not did_radiant_win and avg_lead >= 4000:
        return True
    
    return False


def is_late_match_comeback_6k(match: Dict, dominator: Optional[str]) -> bool:
    """Камбек 6k на 15-25 (победитель проигрывал)."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -6000:
        return True
    if not did_radiant_win and avg_lead >= 6000:
        return True
    
    return False


def is_late_match_comeback_8k(match: Dict, dominator: Optional[str]) -> bool:
    """Камбек 8k на 15-25 (победитель проигрывал)."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    did_radiant_win = match.get('didRadiantWin', False)
    
    if duration < 35 or duration > 60:
        return False
    
    avg_lead = sum(leads[15:25]) / 10
    
    if did_radiant_win and avg_lead <= -8000:
        return True
    if not did_radiant_win and avg_lead >= 8000:
        return True
    
    return False


def is_late_match_45min_close(match: Dict, dominator: Optional[str]) -> bool:
    """45+ min, lead < 5k на 30 мин."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 45 or duration > 70:
        return False
    
    if duration >= 30 and abs(leads[29]) >= 5000:
        return False
    
    return True


def is_late_match_50min(match: Dict, dominator: Optional[str]) -> bool:
    """50+ min."""
    leads = match.get('radiantNetworthLeads', [])
    return len(leads) >= 50 and len(leads) <= 75


def is_early_match_avg8k_2030(match: Dict) -> Tuple[bool, Optional[str]]:
    """Avg lead >= 8k на 20-30 минутах (позже)."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    avg_lead = sum(leads[20:30]) / 10
    
    if avg_lead >= 8000:
        return True, 'radiant'
    elif avg_lead <= -8000:
        return True, 'dire'
    
    return False, None


def is_early_match_avg10k_2030(match: Dict) -> Tuple[bool, Optional[str]]:
    """Avg lead >= 10k на 20-30 минутах."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    avg_lead = sum(leads[20:30]) / 10
    
    if avg_lead >= 10000:
        return True, 'radiant'
    elif avg_lead <= -10000:
        return True, 'dire'
    
    return False, None


def is_early_match_avg12k_2030(match: Dict) -> Tuple[bool, Optional[str]]:
    """Avg lead >= 12k на 20-30 минутах."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    avg_lead = sum(leads[20:30]) / 10
    
    if avg_lead >= 12000:
        return True, 'radiant'
    elif avg_lead <= -12000:
        return True, 'dire'
    
    return False, None


def is_early_match_peak10k_2030(match: Dict) -> Tuple[bool, Optional[str]]:
    """Peak lead >= 10k на 20-30 минутах."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    max_lead = max(leads[20:30])
    min_lead = min(leads[20:30])
    
    if max_lead >= 10000:
        return True, 'radiant'
    elif min_lead <= -10000:
        return True, 'dire'
    
    return False, None


def is_early_match_peak12k_2030(match: Dict) -> Tuple[bool, Optional[str]]:
    """Peak lead >= 12k на 20-30 минутах."""
    leads = match.get('radiantNetworthLeads', [])
    duration = len(leads)
    
    if duration < 30 or duration > 50:
        return False, None
    
    max_lead = max(leads[20:30])
    min_lead = min(leads[20:30])
    
    if max_lead >= 12000:
        return True, 'radiant'
    elif min_lead <= -12000:
        return True, 'dire'
    
    return False, None


if __name__ == '__main__':
    results = []
    
    # ============================================================
    # Тест Late CP с разными фильтрами
    # ============================================================
    
    print("="*80)
    print("ТЕСТ LATE CP С РАЗНЫМИ ФИЛЬТРАМИ")
    print("="*80)
    
    # Comeback 4k (лучший для solo)
    results.append(test_filter_combination(
        "Comeback 4k",
        is_early_match_avg8k,
        is_late_match_comeback_4k,
    ))
    
    # V2 (no dom 8k) - больше матчей
    results.append(test_filter_combination(
        "V2 (no dom 8k)",
        is_early_match_avg8k,
        is_late_match_v2,
    ))
    
    # 45+ min
    results.append(test_filter_combination(
        "45+ min",
        is_early_match_avg8k,
        is_late_match_v11,
    ))
    
    # Итоговая таблица
    print("\n" + "="*140)
    print("ИТОГОВАЯ ТАБЛИЦА (MIN_INDEX >= 10)")
    print("="*140)
    print(f"{'Вариант':<25} {'Early CP':<16} {'Early Syn':<16} {'Late Solo':<16} {'Late Syn':<16} {'Late CP':<16}")
    print("-"*140)
    for r in results:
        early_cp = f"{r['early_cp_accuracy']:.1%} ({r['early_cp_total']})" if r['early_cp_total'] > 0 else "N/A"
        early_syn = f"{r['early_syn_accuracy']:.1%} ({r['early_syn_total']})" if r['early_syn_total'] > 0 else "N/A"
        late_solo = f"{r['late_solo_accuracy']:.1%} ({r['late_solo_total']})" if r['late_solo_total'] > 0 else "N/A"
        late_syn = f"{r['late_syn_accuracy']:.1%} ({r['late_syn_total']})" if r['late_syn_total'] > 0 else "N/A"
        late_cp = f"{r['late_cp_accuracy']:.1%} ({r['late_cp_total']})" if r.get('late_cp_total', 0) > 0 else "N/A"
        print(f"{r['name']:<25} {early_cp:<16} {early_syn:<16} {late_solo:<16} {late_syn:<16} {late_cp:<16}")
