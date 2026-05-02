"""
Скрипт для анализа винрейта метрик из pro_output.txt.

Мягкие фильтры для проверки метрик:
- EARLY: игра не решена после лейнинга (lead на 15 мин < 5k)
- LATE: игра дожила до 30+ минут и не была решена в early (lead на 15 мин < 8k)

Обрабатываемые метрики:
- counterpick_* , synergy_* , solo (early/late)
- Лейны: lane_top/bot/mid (формат "win 55%", "loose 45%", "draw 30%")

Дополнительно добавлены настраиваемые пороги минимального индекса:
- EARLY_MIN_INDEX / LATE_MIN_INDEX: с какого индекса выводить метрики
- LANE_MIN_CONFIDENCE: минимальный % уверенности для отображения
"""

import argparse
import json
import math
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

# Подключаем базу для использования фильтров из analise_database
sys.path.insert(0, str(Path(__file__).parent))
from analise_database import (
    analise_database,
    is_early_match as is_early_match_strict,
    is_late_match as is_late_match_strict,
    is_post_lane_match,
)


def is_early_match_soft(match):
    """
    Мягкий фильтр для early метрик.
    
    Критерии:
    - Игра не решена после лейнинга: abs(lead) на 15 минуте < 5000
    - Игра закончилась (есть победитель)
    - Длительность 15-45 минут
    
    Returns:
        tuple: (bool, dominator) - (подходит ли матч, кто выиграл)
    """
    leads = match.get('radiantNetworthLeads', [])
    did_radiant_win = match.get('didRadiantWin')
    
    if did_radiant_win is None:
        return False, None
    
    # Нужны данные минимум до 15 минуты
    if len(leads) < 15:
        return False, None
    
    # Игра 15-45 минут
    duration = len(leads)
    if duration < 15 or duration > 45:
        return False, None
    
    # Проверяем что игра не была решена после лейнинга
    lead_at_15 = leads[14] if len(leads) > 14 else leads[-1]
    if abs(lead_at_15) >= 5000:
        return False, None
    
    dominator = 'radiant' if did_radiant_win else 'dire'
    return True, dominator


def is_late_match_soft(match):
    """
    Мягкий фильтр для late метрик.
    
    Критерии:
    - Игра дожила до 30+ минут
    - На 15 минуте не было огромного преимущества (< 8k)
    
    Returns:
        bool: подходит ли матч
    """
    leads = match.get('radiantNetworthLeads', [])
    did_radiant_win = match.get('didRadiantWin')
    
    if did_radiant_win is None:
        return False
    
    # Игра должна быть 30+ минут
    duration = len(leads)
    if duration < 30:
        return False
    
    # На 15 минуте не должно быть огромного преимущества
    if len(leads) > 14:
        lead_at_15 = leads[14]
        if abs(lead_at_15) >= 8000:
            return False
    
    return True


def is_early_5050(match):
    """
    Early 50/50 фильтр - матч был "ровным" на 10-й минуте.
    
    Опорная точка: 10-я минута (старт ранней игры после лайнинга).
    
    Условия:
    1. Есть данные на 10-й минуте (len > 10)
    2. |lead[10]| <= 500 (очень близко к нулю)
    3. Стабильность [8..12]: max(|lead|) <= 1500 (реально ровный матч)
    4. max(|lead|) <= 3000 на [0..10] (не было ранних стомпов)
    
    Returns:
        tuple: (bool, winner) - (подходит ли матч, кто выиграл)
    """
    leads = match.get('radiantNetworthLeads', [])
    did_radiant_win = match.get('didRadiantWin')
    
    if did_radiant_win is None:
        return False, None
    
    # Условие 1: данные минимум до 10-й минуты
    if len(leads) <= 10:
        return False, None
    
    # Условие 2: |lead[10]| <= 500
    lead_at_10 = leads[10]
    if abs(lead_at_10) > 500:
        return False, None
    
    # Условие 3: стабильность [8..12] - max(|lead|) <= 1500
    window_8_12 = leads[8:13]  # индексы 8, 9, 10, 11, 12
    if max(abs(l) for l in window_8_12) > 1500:
        return False, None
    
    # Условие 4: max(|lead|) <= 3000 на [0..10]
    window_0_10 = leads[0:11]
    if max(abs(l) for l in window_0_10) > 3000:
        return False, None
    
    winner = 'radiant' if did_radiant_win else 'dire'
    return True, winner


def is_late_5050(match):
    """
    Late 50/50 фильтр - матч был "ровным" на 35-й минуте.
    
    Опорная точка: 35-я минута (late game начинается).
    
    Условия:
    1. Длительность >= 40 минут
    2. Есть данные на 35-й минуте (len > 35)
    3. |lead[35]| <= 4000
    4. Стабильность [30..35]: max(|lead|) <= 10000
    
    Returns:
        tuple: (bool, winner) - (подходит ли матч, кто выиграл)
    """
    leads = match.get('radiantNetworthLeads', [])
    did_radiant_win = match.get('didRadiantWin')
    
    if did_radiant_win is None:
        return False, None
    
    # Условие 1: длительность >= 40 минут
    duration = len(leads)
    if duration < 40:
        return False, None
    
    # Условие 2: данные на 35-й минуте
    if len(leads) <= 35:
        return False, None
    
    # Условие 3: |lead[35]| <= 4000
    lead_at_35 = leads[35]
    if abs(lead_at_35) > 4000:
        return False, None

    # Условие 4: стабильность [30..35]: max(|lead|) <= 10000
    window_30_35 = leads[30:36]
    if max(abs(l) for l in window_30_35) > 10000:
        return False, None

    winner = 'radiant' if did_radiant_win else 'dire'
    return True, winner


def _first_reach_threshold(leads: list, start: int, end: int, threshold: int) -> Tuple[Optional[str], Optional[int]]:
    """
    Находит кто первым достиг порога networth в окне [start, end].
    
    Returns:
        tuple: (dominator, minute) - ('radiant'/'dire'/None, минута достижения/None)
    """
    for i in range(start, min(end + 1, len(leads))):
        if leads[i] >= threshold:
            return 'radiant', i
        elif leads[i] <= -threshold:
            return 'dire', i
    return None, None


def is_early_match_custom(match: dict) -> Tuple[bool, Optional[str]]:
    """
    Custom фильтр для early метрик.
    
    Логика:
    - Матч до 34 минут: победитель = actual winner
    - Матч 34+ минут: смотрим кто первый набрал 7000 networth в окне (20, 27)
    
    Returns:
        tuple: (bool, dominator) - (подходит ли матч, кто доминировал в early)
    """
    leads = match.get('radiantNetworthLeads', [])
    did_radiant_win = match.get('didRadiantWin')
    
    if did_radiant_win is None:
        return False, None
    
    duration = len(leads)
    
    # Матч до 34 минут - early победитель = actual winner
    if duration < 34:
        if duration < 20:  # слишком короткий матч
            return False, None
        winner = 'radiant' if did_radiant_win else 'dire'
        return True, winner
    
    # Матч 34+ минут - смотрим кто первый набрал 7000 в окне (20, 27)
    dominator, _ = _first_reach_threshold(leads, 20, 27, 7000)
    if dominator is None:
        return False, None
    
    return True, dominator


def is_late_match_custom(match: dict, early_dominator: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    Custom фильтр для late метрик.
    
    Логика:
    - Игра 40+ минут
    - early_dominator = None (никто не доминировал в early)
    
    Returns:
        tuple: (bool, winner) - (подходит ли матч, кто выиграл)
    """
    leads = match.get('radiantNetworthLeads', [])
    did_radiant_win = match.get('didRadiantWin')
    
    if did_radiant_win is None:
        return False, None
    
    duration = len(leads)
    
    # Игра должна быть 40+ минут
    if duration < 40:
        return False, None
    
    # early_dominator должен быть None
    if early_dominator is not None:
        return False, None
    
    winner = 'radiant' if did_radiant_win else 'dire'
    return True, winner


# --- Настройки источника данных ---
# 'precomputed' -> используем early_output/late_output внутри матчей
# 'on_the_fly'  -> строим словари на train и считаем метрики на лету
DATA_MODE = ''
PRECOMPUTED_FILE = Path(os.getenv(
    'METRICS_PRECOMPUTED_FILE',
    '/Users/alex/Documents/ingame/runtime/pro_maps_metrics_2025-12-15.json',
))
TRAIN_DIR = Path('/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object')
TRAIN_MAX_FILES = 5          # None чтобы использовать все файлы
TRAIN_LIMIT_PER_FILE = None  # None чтобы не ограничивать
TEST_FILE = Path('/Users/alex/Documents/ingame/bets_data/analise_pub_matches/extracted_100k_matches.json')
TEST_LIMIT = 20000           # None чтобы использовать весь тест
EXCLUDE_TEST_FROM_TRAIN = False
MIN_START_DATE = 0           # 0 чтобы не фильтровать по дате

# --- Настройки выводимого диапазона индексов ---
EARLY_MIN_INDEX = 18  # например 8, чтобы смотреть только сильные ранние сигналы
LATE_MIN_INDEX = 18   # например 6 или 10
POST_LANE_MIN_INDEX = 18
LANE_MIN_CONFIDENCE = 52  # минимальный % уверенности (1-100)

LANE_MAX_CONFIDENCE = 100
METRIC_MAX_INDEX = 100

# Считать винрейт накопительно: индекс N включает все случаи с |value| >= N
USE_CUMULATIVE_INDICES = True
# Выбор фильтра для оценки винрейта:
# - 'draft': фильтры из analise_database (приближены к логике ранней/поздней силы драфта)
# - 'soft' : мягкие фильтры is_early_match_soft/is_late_match_soft
# - '5050' : строгие 50/50 фильтры is_early_5050/is_late_5050
# - 'custom': legacy режим; draft является каноничным для текущих early/late словарей
FILTER_MODE = os.getenv('METRICS_FILTER_MODE', 'draft')
BUCKET_MODE = os.getenv('METRICS_BUCKET_MODE', '1').strip().lower() not in ('0', 'false', 'off', 'no')
BUCKET_MAX_INDEX = int(os.getenv('METRICS_BUCKET_MAX_INDEX', '100'))


def load_matches(filename: str) -> list[dict]:
    """Загружает матчи из JSON файла. Поддерживает и список, и словарь."""
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    # Если словарь (match_id -> match), конвертируем в список
    if isinstance(data, dict):
        return list(data.values())
    return data


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value is not None


def _is_internal_metric_key(metric_name: object) -> bool:
    metric = str(metric_name)
    return metric != 'synergy_duo' and metric.startswith('synergy_duo_')


def _winner_from_generated_fields(match: dict, phase: str) -> tuple[bool, Optional[str]]:
    if phase == 'early':
        if 'is_early' in match or 'early_win' in match:
            winner = match.get('early_win')
            if match.get('is_early') and winner in ('radiant', 'dire'):
                return True, winner
            return False, None
        winner = match.get('early_win')
        if winner in ('radiant', 'dire'):
            return True, winner
        if FILTER_MODE == 'draft':
            return is_early_match_strict(match)
        did_radiant_win = match.get('didRadiantWin')
        if did_radiant_win is None:
            return False, None
        if FILTER_MODE == 'soft':
            return is_early_match_soft(match)
        if FILTER_MODE == '5050':
            return is_early_5050(match)
        return is_early_match_custom(match)

    if phase == 'late':
        if 'is_late' in match or 'late_win' in match:
            winner = match.get('late_win')
            if match.get('is_late') and winner in ('radiant', 'dire'):
                return True, winner
            return False, None
        winner = match.get('late_win')
        if match.get('is_late') and winner in ('radiant', 'dire'):
            return True, winner
        if FILTER_MODE == 'draft':
            early_ok, early_winner = is_early_match_strict(match)
            return is_late_match_strict(match, early_winner if early_ok else None, if_check=True)
        if FILTER_MODE == 'soft':
            ok = is_late_match_soft(match)
            did_radiant_win = match.get('didRadiantWin')
            return ok, ('radiant' if did_radiant_win else 'dire') if ok and did_radiant_win is not None else None
        if FILTER_MODE == '5050':
            return is_late_5050(match)
        early_ok, early_winner = is_early_match_custom(match)
        return is_late_match_custom(match, early_winner if early_ok else None)

    if phase == 'post_lane':
        if 'is_post_lane' in match or 'post_lane_win' in match:
            winner = match.get('post_lane_win')
            if match.get('is_post_lane') and winner in ('radiant', 'dire'):
                return True, winner
            return False, None
        winner = match.get('post_lane_win')
        if match.get('is_post_lane') and winner in ('radiant', 'dire'):
            return True, winner
        return is_post_lane_match(match, if_check=True)

    return False, None


def _bucket_for_value(value: float, max_index: int = BUCKET_MAX_INDEX) -> Optional[int]:
    if not _is_number(value):
        return None
    abs_value = abs(float(value))
    if not math.isfinite(abs_value):
        return None
    bucket = int(math.floor(abs_value))
    if bucket > max_index:
        bucket = max_index
    return bucket


def _record_signed_metric(results: dict, metric_name: str, value: float, winner: Optional[str]) -> None:
    if winner not in ('radiant', 'dire') or not _is_number(value) or float(value) == 0:
        return
    bucket = _bucket_for_value(float(value))
    if bucket is None:
        return
    predicted = 'radiant' if float(value) > 0 else 'dire'
    stats = results.setdefault(metric_name, {}).setdefault(bucket, {'wins': 0, 'looses': 0})
    if predicted == winner:
        stats['wins'] += 1
    else:
        stats['looses'] += 1


def _lane_winner_from_outcome(outcome: Optional[str]) -> Optional[str]:
    if not outcome:
        return None
    upper = str(outcome).upper()
    if 'TIE' in upper or 'DRAW' in upper:
        return None
    if 'RADIANT' in upper:
        return 'radiant'
    if 'DIRE' in upper:
        return 'dire'
    return None


def _lane_metric_value(prediction: Optional[str]) -> Optional[float]:
    if not prediction:
        return None
    cleaned = str(prediction).strip()
    if ':' in cleaned:
        cleaned = cleaned.split(':', 1)[1].strip()
    parts = cleaned.split()
    if len(parts) != 2:
        return None
    side, raw_conf = parts
    if side == 'draw':
        return None
    try:
        confidence = float(raw_conf.rstrip('%'))
    except ValueError:
        return None
    if side == 'win':
        return confidence
    if side in ('lose', 'loose'):
        return -confidence
    return None


def process_metrics_winrate_buckets(matches: list[dict]) -> dict:
    """
    Считает объединённый signed winrate по непересекающимся bucket-интервалам:
    0=[0,1), 1=[1,2), 2=[2,3), ...

    Плюс и минус объединяются: знак метрики задаёт predicted side, а win/lose
    считается относительно winner для нужной фазы.
    """
    results: dict = {}
    counters = {
        'early': 0,
        'late': 0,
        'post_lane': 0,
        'dota2protracker': 0,
        'lane': 0,
    }

    if MIN_START_DATE:
        filtered_matches = [m for m in matches if m.get('startDateTime', 0) >= MIN_START_DATE]
    else:
        filtered_matches = matches
    print(f"Отфильтровано по дате (>= {MIN_START_DATE}): {len(filtered_matches)} из {len(matches)} матчей")

    for idx, match in enumerate(filtered_matches, 1):
        if idx % 500 == 0 or idx == len(filtered_matches):
            print(f"  [{idx:>5}/{len(filtered_matches)}] bucket winrate", end='\r')

        phase_specs = (
            ('early', 'early_output'),
            ('late', 'late_output'),
            ('post_lane', 'post_lane_output'),
        )
        for phase, bucket_name in phase_specs:
            ok, winner = _winner_from_generated_fields(match, phase)
            if not ok or winner not in ('radiant', 'dire'):
                continue
            counters[phase] += 1
            for metric_name, metric_value in (match.get(bucket_name) or {}).items():
                if str(metric_name).endswith('_games'):
                    continue
                if _is_internal_metric_key(metric_name):
                    continue
                if _is_number(metric_value):
                    _record_signed_metric(results, f'{phase}_{metric_name}', metric_value, winner)

        post_ok, post_winner = _winner_from_generated_fields(match, 'post_lane')
        pro = match.get('dota2protracker') or match.get('protracker_output') or {}
        if post_ok and post_winner in ('radiant', 'dire') and isinstance(pro, dict):
            counters['dota2protracker'] += 1
            pro_metric_map = {
                'pro_cp1vs1_late': 'dota2protracker_cp1vs1',
                'pro_duo_synergy_late': 'dota2protracker_duo_synergy',
            }
            for source_key, metric_name in pro_metric_map.items():
                valid_key = source_key.replace('_late', '_valid')
                if valid_key in pro and not bool(pro.get(valid_key)):
                    continue
                metric_value = pro.get(source_key)
                if _is_number(metric_value):
                    _record_signed_metric(results, metric_name, metric_value, post_winner)

        lane_specs = (
            ('top', 'topLaneOutcome', 'top_source'),
            ('mid', 'midLaneOutcome', 'mid_source'),
            ('bot', 'bottomLaneOutcome', 'bot_source'),
        )
        for lane_name, outcome_key, source_key in lane_specs:
            lane_winner = _lane_winner_from_outcome(match.get(outcome_key))
            if lane_winner is None:
                continue
            lane_value = _lane_metric_value(match.get(lane_name))
            if lane_value is not None:
                counters['lane'] += 1
                _record_signed_metric(results, f'lane_{lane_name}', lane_value, lane_winner)
                lane_source = str(match.get(source_key) or '').strip()
                if lane_source:
                    _record_signed_metric(results, f'lane_source_{lane_source}', lane_value, lane_winner)

            if isinstance(pro, dict):
                for source_key in (f'pro_lane_{lane_name}_cp1vs1', f'pro_lane_{lane_name}_duo'):
                    if source_key not in pro or not _is_number(pro.get(source_key)):
                        continue
                    valid_key = f'{source_key}_valid'
                    if valid_key in pro and not bool(pro.get(valid_key)):
                        continue
                    _record_signed_metric(results, f'dota2protracker_{source_key}', pro[source_key], lane_winner)

    print()
    print("Фазовые фильтры:")
    for key, value in counters.items():
        print(f"  {key:18s}: {value}")
    return results


def print_bucket_results(results: dict, *, min_matches: int = 6) -> None:
    print("РЕЗУЛЬТАТЫ BUCKET WINRATE")
    print("=" * 120)
    print("Bucket N означает интервал [N,N+1), например 1=[1,2). Плюс/минус объединены по predicted side.")
    for metric_name in sorted(results):
        parts = []
        for bucket in sorted(results[metric_name]):
            stats = results[metric_name][bucket]
            total = stats['wins'] + stats['looses']
            if total < min_matches:
                continue
            wr = stats['wins'] / total
            parts.append(f"{bucket}-{bucket + 1}: {wr:.1%} (n={total})")
        if parts:
            print(f"{metric_name:42s} {' '.join(parts)}")


def build_train_dicts(train_files: list[Path], limit_per_file=None, exclude_ids=None, include_post_lane: bool = False):
    """Строит early/late словари на train выборке, опционально post-lane."""
    lane_dict = {}
    early_dict = {}
    late_dict = {}
    post_lane_dict = {} if include_post_lane else None
    total = 0

    for idx, file in enumerate(train_files, 1):
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        matches = list(data.values()) if isinstance(data, dict) else data
        if limit_per_file:
            matches = matches[:limit_per_file]
        for match in matches:
            if not isinstance(match, dict):
                continue
            if exclude_ids and match.get('id') in exclude_ids:
                continue
            if 'players' not in match or len(match.get('players', [])) != 10:
                continue
            analise_database(
                match,
                lane_dict,
                early_dict,
                late_dict,
                post_lane_dict=post_lane_dict,
            )
            total += 1
        print(f"  train[{idx}/{len(train_files)}]: обработано {total:,} матчей", end='\r')
    print()
    if include_post_lane:
        return early_dict, late_dict, post_lane_dict
    return early_dict, late_dict


def process_metrics_winrate(matches, early_dict=None, late_dict=None, post_lane_dict=None, use_train_dicts: bool = False):
    """
    Обрабатывает матчи и вычисляет винрейт для метрик.
    
    Для каждой метрики отдельно проверяет индексы от 1 до 50 (с положительным и отрицательным знаком).
    Разделяет метрики: early_*, late_* и post_lane_*.
    
    Args:
        matches: список матчей для анализа

    Returns:
        dict: словарь с результатами для каждой метрики и каждого индекса
            {phase_metric_name: {index: {'positive': {...}, 'negative': {...}}}}
    """
    if isinstance(post_lane_dict, bool) and not use_train_dicts:
        use_train_dicts = post_lane_dict
        post_lane_dict = None

    results = {}  # {phase_metric_name: {index: {'positive': {...}, 'negative': {...}}}}
    unique_matches_per_metric = {}  # {metric_name: set of match_ids}
    
    # Счетчики для дебага
    early_count = 0
    late_count = 0
    post_lane_count = 0
    early_with_metrics = 0
    late_with_metrics = 0
    post_lane_with_metrics = 0
    early_no_filter = 0
    late_no_filter = 0
    post_lane_no_filter = 0
    
    early_and_stats = {
        'cp_strong': {'matches': 0, 'wins': 0},
        'syn_strong': {'matches': 0, 'wins': 0},
        'both_strong_same_sign': {'matches': 0, 'wins': 0},
    }
    
    # Фильтруем матчи по дате (если задано)
    if MIN_START_DATE:
        filtered_matches = [m for m in matches if m.get('startDateTime', 0) >= MIN_START_DATE]
    else:
        filtered_matches = matches
    print(f"Отфильтровано по дате (>= {MIN_START_DATE}): {len(filtered_matches)} из {len(matches)} матчей")
    
    total_matches = len(filtered_matches)
    print(f"Обработка {total_matches:,} матчей...")
    
    for idx, match in enumerate(filtered_matches, 1):
        # Показываем прогресс каждые 500 матчей
        if idx % 500 == 0 or idx == total_matches:
            percent = (idx / total_matches) * 100
            print(f"  [{idx:>5}/{total_matches}] ({percent:>5.1f}%) - обработано", end='\r')
        # Получаем необходимые данные
        did_radiant_win = match.get('didRadiantWin')
        
        if did_radiant_win is None:
            continue
        # Применяем выбранные фильтры
        if FILTER_MODE == 'draft':
            match_is_early, early_dominator = is_early_match_strict(match)
            match_is_late, late_dominator = is_late_match_strict(match, early_dominator, if_check=True)
        elif FILTER_MODE == 'soft':
            match_is_early, early_dominator = is_early_match_soft(match)
            match_is_late = is_late_match_soft(match)
            late_dominator = 'radiant' if did_radiant_win else 'dire'
        elif FILTER_MODE == '5050':
            match_is_early, early_dominator = is_early_5050(match)
            match_is_late, late_dominator = is_late_5050(match)
        elif FILTER_MODE == 'custom':
            match_is_early, early_dominator = is_early_match_custom(match)
            match_is_late, late_dominator = is_late_match_custom(match, early_dominator)
        else:
            raise ValueError(f'Unknown FILTER_MODE: {FILTER_MODE}')
        match_is_post_lane = is_post_lane_match(match)
        post_lane_dominator = 'radiant' if did_radiant_win else 'dire'
        
        # Получаем метрики из early_output и late_output
        if use_train_dicts:
            from functions import synergy_and_counterpick, check_bad_map
            result = check_bad_map(match)
            if result is None:
                continue
            r_by_pos, d_by_pos = result
            s = synergy_and_counterpick(
                radiant_heroes_and_pos=r_by_pos,
                dire_heroes_and_pos=d_by_pos,
                early_dict=early_dict or {},
                mid_dict=late_dict or {},
                post_lane_dict=post_lane_dict or {},
            ) or {}
            early_output = s.get('early_output', {})
            late_output = s.get('mid_output', {})
            post_lane_output = s.get('post_lane_output', {})
        else:
            early_output = match.get('early_output', {})
            late_output = match.get('late_output', {})
            post_lane_output = match.get('post_lane_output', {})
        
        # Диагностика: считаем метрики с/без фильтров
        has_early_metrics = any(v is not None and isinstance(v, (int, float)) for v in early_output.values())
        has_late_metrics = any(v is not None and isinstance(v, (int, float)) for v in late_output.values())
        has_post_lane_metrics = any(v is not None and isinstance(v, (int, float)) for v in post_lane_output.values())
        
        if has_early_metrics:
            if match_is_early:
                early_with_metrics += 1
            else:
                early_no_filter += 1
        
        if has_late_metrics:
            if match_is_late:
                late_with_metrics += 1
            else:
                late_no_filter += 1
        if has_post_lane_metrics:
            if match_is_post_lane:
                post_lane_with_metrics += 1
            else:
                post_lane_no_filter += 1
        
        # Счетчики для фильтров
        if match_is_early:
            early_count += 1
        if match_is_late:
            late_count += 1
        if match_is_post_lane:
            post_lane_count += 1
        
        # Обрабатываем метрики из early_output как early_*
        # ВАЖНО: обрабатываем ТОЛЬКО если матч прошел early фильтр!
        # Сравниваем ПРЕДСКАЗАНИЕ метрики (знак) с early dominator.
        # Победитель матча для early-фильтра намеренно не важен.
        if match_is_early and early_dominator is not None:
            actual_winner = early_dominator
            match_id = match.get('id', idx)
            
            for metric_name, metric_value in early_output.items():
                # Пропускаем None значения
                if not isinstance(metric_value, (int, float)) or metric_value is None:
                    continue
                if _is_internal_metric_key(metric_name):
                    continue
                
                full_metric_name = f'early_{metric_name}'
                
                # Отслеживаем уникальные матчи для этой метрики
                if full_metric_name not in unique_matches_per_metric:
                    unique_matches_per_metric[full_metric_name] = set()
                unique_matches_per_metric[full_metric_name].add(match_id)
                
                # Инициализируем структуру для метрики если нужно
                if full_metric_name not in results:
                    results[full_metric_name] = {}
                    for idx in range(1, 51):
                        results[full_metric_name][idx] = {
                            'positive': {'wins': 0, 'looses': 0},
                            'negative': {'wins': 0, 'looses': 0}
                        }

                # Проверяем индексы (точно или накопительно)
                abs_val = abs(int(metric_value))
                if abs_val == 0:
                    continue
                max_idx = min(50, abs_val) if USE_CUMULATIVE_INDICES else 50
                for index in range(1, max_idx + 1):
                    if not USE_CUMULATIVE_INDICES and abs_val != index:
                        continue
                    # metric_value > 0 означает метрика ПРЕДСКАЗЫВАЕТ победу Radiant
                    # metric_value < 0 означает метрика ПРЕДСКАЗЫВАЕТ победу Dire
                    # Сравниваем предсказание с early dominator
                    if metric_value > 0:
                        # Метрика предсказала Radiant
                        if actual_winner == 'radiant':
                            results[full_metric_name][index]['positive']['wins'] += 1
                        else:
                            results[full_metric_name][index]['positive']['looses'] += 1
                    elif metric_value < 0:
                        # Метрика предсказала Dire
                        if actual_winner == 'dire':
                            results[full_metric_name][index]['negative']['wins'] += 1
                        else:
                            results[full_metric_name][index]['negative']['looses'] += 1

            cp_val = early_output.get('counterpick_1vs1')
            syn_val = early_output.get('synergy_duo')

            if isinstance(cp_val, (int, float)) and cp_val is not None and abs(cp_val) >= EARLY_MIN_INDEX:
                early_and_stats['cp_strong']['matches'] += 1
                if (cp_val > 0 and actual_winner == 'radiant') or (cp_val < 0 and actual_winner == 'dire'):
                    early_and_stats['cp_strong']['wins'] += 1

            if isinstance(syn_val, (int, float)) and syn_val is not None and abs(syn_val) >= EARLY_MIN_INDEX:
                early_and_stats['syn_strong']['matches'] += 1
                if (syn_val > 0 and actual_winner == 'radiant') or (syn_val < 0 and actual_winner == 'dire'):
                    early_and_stats['syn_strong']['wins'] += 1

            if (
                isinstance(cp_val, (int, float))
                and isinstance(syn_val, (int, float))
                and cp_val is not None
                and syn_val is not None
                and abs(cp_val) >= EARLY_MIN_INDEX
                and abs(syn_val) >= EARLY_MIN_INDEX
            ):
                if (cp_val > 0 and syn_val > 0) or (cp_val < 0 and syn_val < 0):
                    early_and_stats['both_strong_same_sign']['matches'] += 1
                    if (cp_val > 0 and actual_winner == 'radiant') or (cp_val < 0 and actual_winner == 'dire'):
                        early_and_stats['both_strong_same_sign']['wins'] += 1

        # Обрабатываем метрики из late_output как late_*
        # ВАЖНО: обрабатываем ТОЛЬКО если матч прошел late фильтр!
        # late_dominator здесь - это winner матча, но для расчёта винрейта метрики
        # нужно сравнивать ПРЕДСКАЗАНИЕ метрики (знак) с РЕАЛЬНЫМ победителем
        if match_is_late and late_dominator is not None:
            # late_dominator = winner матча (кто реально выиграл)
            actual_winner = late_dominator
            match_id = match.get('id', idx)
            
            for metric_name, metric_value in late_output.items():
                # Пропускаем None значения
                if not isinstance(metric_value, (int, float)) or metric_value is None:
                    continue
                if _is_internal_metric_key(metric_name):
                    continue

                full_metric_name = f'late_{metric_name}'
                
                # Отслеживаем уникальные матчи для этой метрики
                if full_metric_name not in unique_matches_per_metric:
                    unique_matches_per_metric[full_metric_name] = set()
                unique_matches_per_metric[full_metric_name].add(match_id)

                # Инициализируем структуру для метрики если нужно
                if full_metric_name not in results:
                    results[full_metric_name] = {}
                    for idx in range(1, 51):
                        results[full_metric_name][idx] = {
                            'positive': {'wins': 0, 'looses': 0},
                            'negative': {'wins': 0, 'looses': 0}
                        }
                
                # Проверяем индексы (точно или накопительно)
                abs_val = abs(int(metric_value))
                if abs_val == 0:
                    continue
                max_idx = min(50, abs_val) if USE_CUMULATIVE_INDICES else 50
                for index in range(1, max_idx + 1):
                    if not USE_CUMULATIVE_INDICES and abs_val != index:
                        continue
                    # metric_value > 0 означает метрика ПРЕДСКАЗЫВАЕТ победу Radiant
                    # metric_value < 0 означает метрика ПРЕДСКАЗЫВАЕТ победу Dire
                    # Сравниваем предсказание с actual_winner
                    if metric_value > 0:
                        # Метрика предсказала Radiant
                        if actual_winner == 'radiant':
                            results[full_metric_name][index]['positive']['wins'] += 1
                        else:
                            results[full_metric_name][index]['positive']['looses'] += 1
                    elif metric_value < 0:
                        # Метрика предсказала Dire
                        if actual_winner == 'dire':
                            results[full_metric_name][index]['negative']['wins'] += 1
                        else:
                            results[full_metric_name][index]['negative']['looses'] += 1

        # Post-lane словарь обучается на матчах после 10m gate + min duration и дальше берёт winner.
        if match_is_post_lane and post_lane_dominator is not None:
            actual_winner = post_lane_dominator
            match_id = match.get('id', idx)

            for metric_name, metric_value in post_lane_output.items():
                if not isinstance(metric_value, (int, float)) or metric_value is None:
                    continue
                if _is_internal_metric_key(metric_name):
                    continue

                full_metric_name = f'post_lane_{metric_name}'
                if full_metric_name not in unique_matches_per_metric:
                    unique_matches_per_metric[full_metric_name] = set()
                unique_matches_per_metric[full_metric_name].add(match_id)

                if full_metric_name not in results:
                    results[full_metric_name] = {}
                    for idx in range(1, 51):
                        results[full_metric_name][idx] = {
                            'positive': {'wins': 0, 'looses': 0},
                            'negative': {'wins': 0, 'looses': 0}
                        }

                abs_val = abs(int(metric_value))
                if abs_val == 0:
                    continue
                max_idx = min(50, abs_val) if USE_CUMULATIVE_INDICES else 50
                for index in range(1, max_idx + 1):
                    if not USE_CUMULATIVE_INDICES and abs_val != index:
                        continue
                    if metric_value > 0:
                        if actual_winner == 'radiant':
                            results[full_metric_name][index]['positive']['wins'] += 1
                        else:
                            results[full_metric_name][index]['positive']['looses'] += 1
                    elif metric_value < 0:
                        if actual_winner == 'dire':
                            results[full_metric_name][index]['negative']['wins'] += 1
                        else:
                            results[full_metric_name][index]['negative']['looses'] += 1
        
        # Обрабатываем COMEBACK метрики (опционально)
        # Обрабатываем лейн метрики (top, bot, mid)
        # Сравниваем ПРЕДСКАЗАНИЕ лейна с РЕАЛЬНЫМ ИСХОДОМ лейна
        lane_mapping = {
            'top': 'topLaneOutcome',
            'bot': 'bottomLaneOutcome',
            'mid': 'midLaneOutcome'
        }
        
        for lane_name, outcome_field in lane_mapping.items():
            lane_prediction = match.get(lane_name)  # Предсказание: "win 55%"
            lane_actual_outcome = match.get(outcome_field)  # Реальный исход: "RADIANT_VICTORY"
            
            # Пропускаем если нет предсказания или реального исхода
            if not lane_prediction or lane_prediction == 'None' or not lane_actual_outcome:
                continue
            
            # Парсим предсказание формата "Top: win 55%\n" или "win 55%"
            try:
                # Убираем префикс "Top: ", "Bot: ", "Mid: " если есть
                cleaned = lane_prediction.strip()
                if ':' in cleaned:
                    # Формат "Top: win 55%"
                    cleaned = cleaned.split(':', 1)[1].strip()
                
                parts = cleaned.split()
                if len(parts) != 2:
                    continue
                
                predicted_outcome = parts[0]  # win, loose, draw
                confidence = int(parts[1].rstrip('%'))  # 55
                
                # Пропускаем draw - не учитываем ничьи в предсказаниях
                if predicted_outcome == 'draw':
                    continue
                
                # Определяем реальный исход лейна
                actual_outcome_upper = lane_actual_outcome.upper()
                radiant_won_lane = 'RADIANT' in actual_outcome_upper
                dire_won_lane = 'DIRE' in actual_outcome_upper
                tie_lane = 'TIE' in actual_outcome_upper or 'DRAW' in actual_outcome_upper
                
                # Пропускаем TIE в реальном исходе - не учитываем
                if tie_lane or (not radiant_won_lane and not dire_won_lane):
                    continue
                
                # Определяем имя метрики для лейна
                full_lane_name = f'lane_{lane_name}'
                
                # Инициализируем структуру если нужно
                if full_lane_name not in results:
                    results[full_lane_name] = {}
                    for idx in range(1, 101):  # Лейны идут в процентах 1-100
                        results[full_lane_name][idx] = {
                            'positive': {'wins': 0, 'looses': 0},
                            'negative': {'wins': 0, 'looses': 0}
                        }
                
                # Проверяем правильность предсказания
                # predicted_outcome="win" означает мы предсказали победу Radiant на лейне
                # predicted_outcome="loose" означает мы предсказали победу Dire на лейне
                if confidence < LANE_MIN_CONFIDENCE or confidence > LANE_MAX_CONFIDENCE:
                    continue
                if predicted_outcome == 'win':
                    # Предсказали что Radiant выиграет лейн
                    if confidence <= 100:
                        if radiant_won_lane:
                            # Предсказание сбылось!
                            results[full_lane_name][confidence]['positive']['wins'] += 1
                        elif dire_won_lane:
                            # Предсказание не сбылось
                            results[full_lane_name][confidence]['positive']['looses'] += 1
                
                elif predicted_outcome == 'loose':
                    # Предсказали что Dire выиграет лейн (Radiant проиграет)
                    if confidence <= 100:
                        if dire_won_lane:
                            # Предсказание сбылось!
                            results[full_lane_name][confidence]['negative']['wins'] += 1
                        elif radiant_won_lane:
                            # Предсказание не сбылось
                            results[full_lane_name][confidence]['negative']['looses'] += 1
            
            except (ValueError, IndexError):
                # Если не удалось распарсить, пропускаем
                continue
    
    print()  # Новая строка после прогресса
    print(f"\nФильтрация:")
    print(f"  Early матчей (прошли фильтр): {early_count}")
    print(f"  Late матчей (прошли фильтр): {late_count}")
    print(f"  Post-lane матчей (прошли фильтр): {post_lane_count}")
    print(f"\nДиагностика метрик:")
    print(f"  Early метрики:")
    print(f"    Прошли фильтр: {early_with_metrics}")
    print(f"    Отсеяно фильтром: {early_no_filter}")
    print(f"  Late метрики:")
    print(f"    Прошли фильтр: {late_with_metrics}")
    print(f"    Отсеяно фильтром: {late_no_filter}")
    print(f"  Post-lane метрики:")
    print(f"    Прошли фильтр: {post_lane_with_metrics}")
    print(f"    Отсеяно фильтром: {post_lane_no_filter}")
    
    cp_matches = early_and_stats['cp_strong']['matches']
    syn_matches = early_and_stats['syn_strong']['matches']
    both_matches = early_and_stats['both_strong_same_sign']['matches']
    if cp_matches > 0 or syn_matches > 0 or both_matches > 0:
        print("\nЭкспериментальный расчёт условного винрейта для сильного counterpick_1vs1, сильного synergy_duo и их одновременного наличия с совпадающим знаком (AND):")
        if cp_matches > 0:
            cp_wr = early_and_stats['cp_strong']['wins'] / cp_matches
            print(f"  A: сильный counterpick_1vs1        | матчей: {cp_matches:5d}, винрейт: {cp_wr:.2%}")
        if syn_matches > 0:
            syn_wr = early_and_stats['syn_strong']['wins'] / syn_matches
            print(f"  B: сильный synergy_duo             | матчей: {syn_matches:5d}, винрейт: {syn_wr:.2%}")
        if both_matches > 0:
            both_wr = early_and_stats['both_strong_same_sign']['wins'] / both_matches
            print(f"  A∧B: одновременное наличие сильного counterpick_1vs1 и сильного synergy_duo с совпадающим знаком      | матчей: {both_matches:5d}, винрейт: {both_wr:.2%}")
    
    return results, unique_matches_per_metric


def _min_index_for(metric_name: str) -> int:
    if metric_name.startswith('lane_'):
        return max(1, LANE_MIN_CONFIDENCE)
    if metric_name.startswith('early_'):
        return max(1, EARLY_MIN_INDEX)
    if metric_name.startswith('late_'):
        return max(1, LATE_MIN_INDEX)
    if metric_name.startswith('post_lane_'):
        return max(1, POST_LANE_MIN_INDEX)
    return 1


def _max_index_for(metric_name: str) -> int:
    return LANE_MAX_CONFIDENCE if metric_name.startswith('lane_') else METRIC_MAX_INDEX


def print_results(results, unique_matches_per_metric: Optional[dict] = None):
    """Выводит результаты - только процент винрейта для каждой метрики."""
    print("РЕЗУЛЬТАТЫ АНАЛИЗА МЕТРИК ВИНРЕЙТА")
    print("=" * 120)
    
    if unique_matches_per_metric is None:
        unique_matches_per_metric = {}
    
    avg_winrates = {}
    avg_match_counts = {}
    
    # Минимум матчей для учёта в среднем винрейте
    MIN_MATCHES_FOR_AVG = 10
    
    print("\n📊 МЕТРИКИ (early/late/post-lane + лейны):")
    print("=" * 120)
    for metric_name in sorted(results.keys()):
        metric_data = results[metric_name]
        
        # Собираем строки для всех индексов
        lines = []
        
        # Для расчета среднего винрейта
        total_weighted_wins = 0
        total_matches_all = 0
        
        max_index = _max_index_for(metric_name) + 1
        min_index = _min_index_for(metric_name)
        
        for index in range(min_index, max_index):
            # Проверяем что индекс есть в данных
            if index not in metric_data:
                continue
                
            pos = metric_data[index]['positive']
            neg = metric_data[index]['negative']
            
            pos_total = pos['wins'] + pos['looses']
            neg_total = neg['wins'] + neg['looses']
            total_matches = pos_total + neg_total
            total_wins = pos['wins'] + neg['wins']
            
            # Выводим только если общее количество матчей >= 6
            if total_matches >= 6:
                overall_wr = total_wins / total_matches if total_matches > 0 else 0
                lines.append(f"{index}: {overall_wr:.1%}")
            
            # Для расчета среднего винрейта учитываем все индексы с данными
            if total_matches > 0:
                total_weighted_wins += total_wins
                total_matches_all += total_matches
        
        # Выводим только если есть данные для этой метрики
        if lines:
            print(f"{metric_name:30s} {' '.join(lines)}")
        
        # Сохраняем средний винрейт только если достаточно матчей
        if total_matches_all >= MIN_MATCHES_FOR_AVG:
            avg_winrates[metric_name] = total_weighted_wins / total_matches_all
            avg_match_counts[metric_name] = total_matches_all
        else:
            avg_winrates[metric_name] = 0
            avg_match_counts[metric_name] = total_matches_all
    
    # Выводим средние винрейты
    print("\n" + "=" * 120)
    print(f"СРЕДНИЙ ВИНРЕЙТ ПО МЕТРИКАМ (минимум {MIN_MATCHES_FOR_AVG} матчей):")
    print("=" * 120)
    
    print("\n📊 Метрики:")
    for metric_name in sorted(results.keys()):
        avg_wr = avg_winrates.get(metric_name, 0)
        match_count = avg_match_counts.get(metric_name, 0)
        unique_count = len(unique_matches_per_metric.get(metric_name, set()))
        # Пропускаем метрики с 100% или 0% винрейтом - они подозрительные
        if avg_wr > 0 and avg_wr < 1.0:
            print(f"  {metric_name:30s} {avg_wr:.2%} (n={match_count}, unique={unique_count})")
        elif avg_wr >= 1.0 or avg_wr == 0:
            # Показываем подозрительные метрики отдельно
            pass
    
    # Показываем подозрительные метрики
    suspicious = [(m, avg_winrates[m], avg_match_counts[m], len(unique_matches_per_metric.get(m, set()))) 
                  for m in results.keys() 
                  if avg_winrates.get(m, 0) >= 1.0 or (avg_winrates.get(m, 0) == 0 and avg_match_counts.get(m, 0) > 0)]
    if suspicious:
        print("\n⚠️ Подозрительные метрики (100% или 0% винрейт):")
        for m, wr, cnt, unique in sorted(suspicious):
            print(f"  {m:30s} {wr:.2%} (n={cnt}, unique={unique}) - мало данных")
            print(f"  {metric_name:30s} {avg_wr:.2%} (n={match_count})")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Winrate analysis for precomputed draft metrics.")
    parser.add_argument("--input", default=str(PRECOMPUTED_FILE), help="Precomputed JSON from base/check_old_maps.py")
    parser.add_argument("--bucket-mode", dest="bucket_mode", action="store_true", default=BUCKET_MODE)
    parser.add_argument("--old-mode", dest="bucket_mode", action="store_false")
    parser.add_argument("--min-matches", type=int, default=6)
    return parser


if __name__ == '__main__':
    args = _build_arg_parser().parse_args()
    if DATA_MODE == 'on_the_fly' and not args.bucket_mode:
        train_files = sorted(TRAIN_DIR.glob('combined*.json'))
        if TRAIN_MAX_FILES:
            train_files = train_files[:TRAIN_MAX_FILES]
        if not train_files:
            raise FileNotFoundError(f'Не найдены train файлы в {TRAIN_DIR}')

        test_matches = load_matches(str(TEST_FILE))
        if TEST_LIMIT:
            test_matches = test_matches[:TEST_LIMIT]

        exclude_ids = None
        if EXCLUDE_TEST_FROM_TRAIN:
            exclude_ids = {m.get('id') for m in test_matches if isinstance(m, dict) and m.get('id')}

        print(f"TRAIN файлов: {len(train_files)}")
        print("Построение словарей train...")
        early_dict, late_dict, post_lane_dict = build_train_dicts(
            train_files,
            TRAIN_LIMIT_PER_FILE,
            exclude_ids,
            include_post_lane=True,
        )

        print(f"\nЗагружено test матчей: {len(test_matches)}")
        print("\nОбработка метрик...")
        results, unique_matches = process_metrics_winrate(
            test_matches,
            early_dict,
            late_dict,
            post_lane_dict,
            use_train_dicts=True,
        )
        print_results(results, unique_matches)
    else:
        matches = load_matches(str(args.input))
        print(f"Загружено матчей: {len(matches)}")
        if args.bucket_mode:
            print("\nОбработка bucket winrate...")
            bucket_results = process_metrics_winrate_buckets(matches)
            print_bucket_results(bucket_results, min_matches=args.min_matches)
        else:
            print("\nОбработка метрик...")
            results, unique_matches = process_metrics_winrate(matches)
            print_results(results, unique_matches)
