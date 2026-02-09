import datetime
import html
import json
import os
import re
import time
from pathlib import Path
from itertools import chain, permutations
from typing import ClassVar

import pytz
import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

import keys


# Заглушка для устаревшей функции get_team_positions
class _IdToName:
    translate: ClassVar[dict] = {}

id_to_name = _IdToName()

# ИМПОРТ УЛУЧШЕННЫХ ФУНКЦИЙ
# Заменяет старые функции с проверкой статистической значимости
# === Переключатель варианта get_diff для экспериментов ===
GET_DIFF_VARIANT = 'winsorized'  # варианты: 'mean' | 'median' | 'trimmed' | 'winsorized'
# Параметры агрегации (для median/trimmed/winsorized)
GET_DIFF_TRIM_ALPHA = 0.1  # доля отсечения/винзоризации по хвостам
# Преобразование веса матчапа (влияние количества игр)
GET_DIFF_WEIGHT_POWER = 0.0  # 1.0=linear, 0.5=sqrt, 0.0=uniform
GET_DIFF_WEIGHT_CAP = None  # например 300 или None чтобы не ограничивать
# Дискретизация индекса (1 = без бина)
GET_DIFF_INDEX_BIN = 1
# Дополнительный масштаб индекса (1.0 = без масштабирования)
GET_DIFF_INDEX_SCALE = 1.0

# Позволяем переопределять параметры через env (для экспериментов)
_env_variant = os.getenv('GET_DIFF_VARIANT')
if _env_variant:
    GET_DIFF_VARIANT = _env_variant.strip()
_env_trim = os.getenv('GET_DIFF_TRIM_ALPHA')
if _env_trim:
    try:
        GET_DIFF_TRIM_ALPHA = float(_env_trim)
    except ValueError:
        pass
_env_power = os.getenv('GET_DIFF_WEIGHT_POWER')
if _env_power:
    try:
        GET_DIFF_WEIGHT_POWER = float(_env_power)
    except ValueError:
        pass
_env_cap = os.getenv('GET_DIFF_WEIGHT_CAP')
if _env_cap:
    try:
        GET_DIFF_WEIGHT_CAP = float(_env_cap)
    except ValueError:
        GET_DIFF_WEIGHT_CAP = None
_env_bin = os.getenv('GET_DIFF_INDEX_BIN')
if _env_bin:
    try:
        GET_DIFF_INDEX_BIN = int(float(_env_bin))
    except ValueError:
        pass
_env_scale = os.getenv('GET_DIFF_INDEX_SCALE')
if _env_scale:
    try:
        GET_DIFF_INDEX_SCALE = float(_env_scale)
    except ValueError:
        pass
# Переопределение весов позиций через env (JSON dict)
_ENV_POS_WEIGHTS = None
_env_pos_weights = os.getenv('GET_DIFF_POS_WEIGHTS')
if _env_pos_weights:
    try:
        _parsed = json.loads(_env_pos_weights)
        if isinstance(_parsed, dict):
            _ENV_POS_WEIGHTS = {
                k: float(v) for k, v in _parsed.items() if k.startswith('pos')
            }
    except (ValueError, TypeError):
        _ENV_POS_WEIGHTS = None
_ENV_POS_WEIGHTS_EARLY = None
_env_pos_weights_early = os.getenv('GET_DIFF_POS_WEIGHTS_EARLY')
if _env_pos_weights_early:
    try:
        _parsed = json.loads(_env_pos_weights_early)
        if isinstance(_parsed, dict):
            _ENV_POS_WEIGHTS_EARLY = {
                k: float(v) for k, v in _parsed.items() if k.startswith('pos')
            }
    except (ValueError, TypeError):
        _ENV_POS_WEIGHTS_EARLY = None
_ENV_POS_WEIGHTS_LATE = None
_env_pos_weights_late = os.getenv('GET_DIFF_POS_WEIGHTS_LATE')
if _env_pos_weights_late:
    try:
        _parsed = json.loads(_env_pos_weights_late)
        if isinstance(_parsed, dict):
            _ENV_POS_WEIGHTS_LATE = {
                k: float(v) for k, v in _parsed.items() if k.startswith('pos')
            }
    except (ValueError, TypeError):
        _ENV_POS_WEIGHTS_LATE = None
# Пороговые параметры get_diff (можно тюнить для стабильности метрик)
GET_DIFF_MIN_MATCHES = 0  # Порог матчей отключен (используем пороги при сборе)
GET_DIFF_MIN_FINAL_DEVIATION = 0.0  # Порог отключен
GET_DIFF_MIN_WR_GAP = 0.0  # Порог отключен
# Минимум core-позиций (pos1-3) с данными для расчета counterpick_1vs1
COUNTERPICK_1VS1_MIN_CORE_POSITIONS = 0
# Минимальный абсолютный индекс counterpick_1vs1 для сохранения (отсекаем слабый шум)
COUNTERPICK_1VS1_MIN_ABS = 0
# Core позиции и полное покрытие по вариантам
CORE_POSITIONS = ('pos1', 'pos2', 'pos3')
COUNTERPICK_1VS1_CORES_REQUIRED = len(CORE_POSITIONS) * len(CORE_POSITIONS)  # 3x3 = 9
SYNERGY_DUO_CORES_REQUIRED = (len(CORE_POSITIONS) * (len(CORE_POSITIONS) - 1)) // 2  # C(3,2) = 3
# Пороги по количеству матчей (единственные действующие пороги)
SOLO_MIN_MATCHES = 50
SYNERGY_DUO_MIN_MATCHES = 50
COUNTERPICK_1VS1_MIN_MATCHES = 50
COUNTERPICK_1VS2_MIN_MATCHES = 30
SYNERGY_TRIO_MIN_MATCHES = 30
SYNERGY_DUO_REQUIRE_CP_ALIGN = False

# Позиционные веса по умолчанию (разные для early/late)
EARLY_POSITION_WEIGHTS = {
    'pos1': 1.2,
    'pos2': 1.2,
    'pos3': 1.0,
    'pos4': 0.9,
    'pos5': 0.8,
}
LATE_POSITION_WEIGHTS = {
    'pos1': 3.0,
    'pos2': 2.2,
    'pos3': 1.6,
    'pos4': 0.9,
    'pos5': 0.7,
}


def structure_lane_dict(flat_lane_dict):
    """
    Преобразует плоский lane_dict в структурированный формат для calculate_lanes.
    
    Входной формат (плоский):
        {
            '1pos1': {'wins': N, 'draws': M, 'games': K},
            '1pos1_vs_2pos2': {'wins': N, 'draws': M, 'games': K},
            ...
        }
    
    Выходной формат (структурированный):
        {
            '2v2_lanes': {...},
            '2v1_lanes': {...},
            '1v1_lanes': {...},
            '1_with_1_lanes': {...}
        }
    """
    structured = {
        '2v2_lanes': {},
        '2v1_lanes': {},
        '1v1_lanes': {},
        '1_with_1_lanes': {},
        'solo_lanes': {},
    }
    
    for key, value in flat_lane_dict.items():
        if '_vs_' in key:
            # Это контрпик
            parts = key.split('_vs_')
            left_heroes = parts[0].split(',')
            right_heroes = parts[1].split(',')
            
            if len(left_heroes) == 2 and len(right_heroes) == 2:
                # 2v2
                structured['2v2_lanes'][key] = value
            elif len(left_heroes) == 2 and len(right_heroes) == 1:
                # 2v1
                structured['2v1_lanes'][key] = value
            elif len(left_heroes) == 1 and len(right_heroes) == 2:
                # 1v2
                structured['2v1_lanes'][key] = value
            elif len(left_heroes) == 1 and len(right_heroes) == 1:
                # 1v1
                structured['1v1_lanes'][key] = value
        elif '_with_' in key:
            # Это синергия
            structured['1_with_1_lanes'][key] = value
        else:
            structured['solo_lanes'][key] = value
        # Соло герои не нужны для этой структуры
    
    return structured


def get_diff(radiant, dire, _1vs2=False, min_confidence=0.95, skip_significance_check=False,
             custom_position_weights=None, use_max_for_synergy=False):
    """
    ИСПРАВЛЕННАЯ ВЕРСИЯ v4 - ПРЯМОЕ СРАВНЕНИЕ RADIANT VS DIRE

    КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ:
    Для counterpick данные из ОДНИХ И ТЕХ ЖЕ матчей!
    Если Radiant hero_A винит 55% против Dire hero_B,
    то Dire hero_B винит 45% против Radiant hero_A (зеркало).

    Сравнение с baseline НЕ работает - обе команды могут показывать > 50%
    против разных оппонентов из разных матчей!

    РЕШЕНИЕ: Сравниваем Radiant НАПРЯМУЮ с Dire в ЭТОМ матче.

    Args:
        radiant: для synergy - список (wr, count), для counterpick - dict {pos: [(wr, count), ...]}
        dire: аналогично
        _1vs2: True для counterpick (с весами позиций), False для synergy
        min_confidence: минимальная уверенность для возврата результата (не используется)
        skip_significance_check: пропустить проверку значимости
        custom_position_weights: dict с весами позиций

    Returns:
        int: разница в процентах или None если нет данных
    """
    if radiant is None or dire is None:
        return None

    import math

    MIN_FINAL_DEVIATION = GET_DIFF_MIN_FINAL_DEVIATION  # Минимальное отклонение ИТОГОВОГО результата от 0
    MIN_MATCHES_PER_MATCHUP = GET_DIFF_MIN_MATCHES  # Минимум матчей для учета отдельного матчапа
    BAYESIAN_PRIOR_STRENGTH = 0  # ОТКЛЮЧЕНО! Не применяем сглаживание
    BASELINE = 0.50  # Используется только для байесовского сглаживания (если включено)

    def winrate_to_logodds(wr):
        """
        Преобразует винрейт в log-odds (logit) для корректной математики.
        Log-odds учитывает, что разница 45%→50% ≠ разница 50%→55%

        Примеры:
        - 60% → 0.405
        - 55% → 0.201
        - 50% → 0.000
        - 45% → -0.201
        - 40% → -0.405
        """
        # Защита от 0 и 1 (которые дают inf/-inf)
        wr = max(0.001, min(0.999, wr))
        return math.log(wr / (1 - wr))

    def logodds_to_winrate(lo):
        """Обратная трансформация: log-odds → winrate"""
        # Защита от переполнения
        lo = max(-10, min(10, lo))
        return 1 / (1 + math.exp(-lo))

    def apply_bayesian_smoothing(winrate, count):
        """
        Байесовское сглаживание: малые сэмплы "притягиваются" к baseline.

        Примеры:
        - 70% на 5 матчах  → ~53% (сильное сглаживание)
        - 70% на 100 матчах → ~68% (слабое сглаживание)
        - 52% на 50 матчах → ~51.4% (минимальное сглаживание)
        """
        return (winrate * count + BASELINE * BAYESIAN_PRIOR_STRENGTH) / (count + BAYESIAN_PRIOR_STRENGTH)

    def confidence_margin(winrate, count, confidence=0.95):
        """
        Вычисляет margin of error для винрейта (упрощенная нормальная аппроксимация).
        Используется для оценки надежности данных.

        Возвращает: половину ширины доверительного интервала
        Пример: winrate=0.52, margin=0.05 → реальный винрейт скорее всего в [0.47, 0.57]
        """
        if count < 1:
            return 0.5  # Максимальная неопределенность

        # Z-score для 95% confidence
        z = 1.96 if confidence >= 0.95 else 1.645

        # Нормальная аппроксимация биномиального распределения
        # Стандартная ошибка: sqrt(p*(1-p)/n)
        std_error = math.sqrt(winrate * (1 - winrate) / count)
        return z * std_error

    def _transform_weight(w):
        if w is None:
            return 0.0
        try:
            w = float(w)
        except (TypeError, ValueError):
            return 0.0
        if GET_DIFF_WEIGHT_CAP is not None:
            w = min(w, float(GET_DIFF_WEIGHT_CAP))
        if GET_DIFF_WEIGHT_POWER != 1.0:
            if w <= 0:
                return 0.0
            w = w ** float(GET_DIFF_WEIGHT_POWER)
        return w

    def _normalize_items(items):
        normalized = []
        for it in items:
            if isinstance(it, (tuple, list)) and len(it) >= 1:
                val = float(it[0])
                weight = float(it[1]) if len(it) >= 2 else 1.0
            else:
                try:
                    val = float(it)
                    weight = 1.0
                except (TypeError, ValueError):
                    continue

            if weight < MIN_MATCHES_PER_MATCHUP:
                continue

            if BAYESIAN_PRIOR_STRENGTH > 0:
                val = apply_bayesian_smoothing(val, weight)

            weight = _transform_weight(weight)
            if weight <= 0:
                continue
            normalized.append((val, weight))
        return normalized

    def _weighted_mean(items):
        total_w = sum(w for _, w in items)
        if total_w <= 0:
            return None, 0.0
        return (sum(v * w for v, w in items) / total_w), total_w

    def _weighted_quantile(items, q):
        if not items:
            return None
        total_w = sum(w for _, w in items)
        if total_w <= 0:
            return None
        target = q * total_w
        acc = 0.0
        for v, w in sorted(items, key=lambda x: x[0]):
            acc += w
            if acc >= target:
                return v
        return sorted(items, key=lambda x: x[0])[-1][0]

    def _weighted_trimmed_mean(items, alpha):
        if not items:
            return None, 0.0
        if alpha <= 0:
            return _weighted_mean(items)
        total_w = sum(w for _, w in items)
        if total_w <= 0:
            return None, 0.0
        low = alpha * total_w
        high = (1 - alpha) * total_w
        acc = 0.0
        kept = []
        for v, w in sorted(items, key=lambda x: x[0]):
            next_acc = acc + w
            take = max(0.0, min(next_acc, high) - max(acc, low))
            if take > 0:
                kept.append((v, take))
            acc = next_acc
            if acc >= high:
                break
        return _weighted_mean(kept)

    def _aggregate_items(items):
        normalized = _normalize_items(items)
        if not normalized:
            return None, 0.0
        if GET_DIFF_VARIANT == 'median':
            val = _weighted_quantile(normalized, 0.5)
            total_w = sum(w for _, w in normalized)
            return val, total_w
        if GET_DIFF_VARIANT == 'trimmed':
            return _weighted_trimmed_mean(normalized, GET_DIFF_TRIM_ALPHA)
        if GET_DIFF_VARIANT == 'winsorized':
            low = _weighted_quantile(normalized, GET_DIFF_TRIM_ALPHA)
            high = _weighted_quantile(normalized, 1 - GET_DIFF_TRIM_ALPHA)
            if low is None or high is None:
                return None, 0.0
            clamped = [(min(max(v, low), high), w) for v, w in normalized]
            return _weighted_mean(clamped)
        return _weighted_mean(normalized)

    def _finalize_index(diff_value):
        if diff_value is None:
            return None
        try:
            val = float(diff_value) * 100.0 * float(GET_DIFF_INDEX_SCALE)
        except (TypeError, ValueError):
            return None
        if GET_DIFF_INDEX_BIN and GET_DIFF_INDEX_BIN > 1:
            val = round(val / GET_DIFF_INDEX_BIN) * GET_DIFF_INDEX_BIN
        return int(round(val))

    if not _1vs2:
        # ===================================================================
        # ДЛЯ SYNERGY_DUO и SYNERGY_TRIO (без весов позиций)
        # ===================================================================
        if not radiant or not dire:
            return None

        if use_max_for_synergy:
            def max_value(items):
                best = None
                for it in items:
                    if isinstance(it, (tuple, list)) and len(it) >= 2:
                        val = float(it[0])
                        weight = float(it[1])
                    else:
                        continue
                    if weight < MIN_MATCHES_PER_MATCHUP:
                        continue
                    if abs(val - 0.5) < GET_DIFF_MIN_WR_GAP:
                        continue
                    best = val if best is None or val > best else best
                return best

            r_max = max_value(radiant)
            d_max = max_value(dire)
            if r_max is None or d_max is None:
                return None
            diff = r_max - d_max
            if not skip_significance_check and abs(diff) < MIN_FINAL_DEVIATION:
                return None
            return _finalize_index(diff)

        radiant_avg, _ = _aggregate_items(radiant)
        dire_avg, _ = _aggregate_items(dire)

        if radiant_avg is None or dire_avg is None:
            return None

        # ПРЯМОЕ сравнение: Radiant synergy - Dire synergy
        diff = radiant_avg - dire_avg

        # Фильтруем только если разница слишком мала
        if not skip_significance_check and abs(diff) < MIN_FINAL_DEVIATION:
            return None

        return _finalize_index(diff)

    # ===================================================================
    # ДЛЯ COUNTERPICK 1vs1 и 1vs2 (С ВЕСАМИ ПОЗИЦИЙ)
    # ===================================================================

    # Улучшенные веса позиций (адаптивные через параметр функции)
    if custom_position_weights:
        weights = custom_position_weights
    elif _ENV_POS_WEIGHTS:
        weights = _ENV_POS_WEIGHTS
    else:
        weights = {
            'pos1': 3.0,   # carry - самый важный
            'pos2': 2.0,   # mid
            'pos3': 1.5,   # offlane
            'pos4': 0.9,   # soft support
            'pos5': 0.7,   # hard support
        }

    def weighted_average_by_position(side):
        """
            Вычисляет взвешенный средний винрейт с учетом весов позиций.
            ПРЯМОЕ значение винрейта, без сравнения с baseline!
            """
        num, den = 0.0, 0.0

        for pos, pos_weight in weights.items():
            matchups = side.get(pos, [])
            if not matchups:
                continue

            pos_avg, total_weight = _aggregate_items(matchups)
            if pos_avg is None or total_weight <= 0:
                continue

            num += pos_avg * pos_weight * total_weight
            den += pos_weight * total_weight

        if den == 0:
            return None
        return num / den

    radiant_avg = weighted_average_by_position(radiant)
    dire_avg = weighted_average_by_position(dire)

    if radiant_avg is None or dire_avg is None:
        return None

    # ПРЯМОЕ сравнение: Radiant counterpick - Dire counterpick
    diff = radiant_avg - dire_avg

    # Фильтруем только если разница слишком мала
    if not skip_significance_check and abs(diff) < MIN_FINAL_DEVIATION:
        return None

    return _finalize_index(diff)


def set_get_diff_variant(variant):
    global GET_DIFF_VARIANT
    if variant in ('mean', 'median', 'trimmed', 'winsorized', 'baseline'):
        GET_DIFF_VARIANT = variant
    else:
        raise ValueError(f"Unknown get_diff variant: {variant}")

def send_message(message):
    bot_token = f'{keys.Token}'
    chat_id = f'{keys.Chat_id}'
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': message,
    }
    requests.post(url, json=payload)
name_to_id = {'abaddon': 102, 'alchemist': 73, 'ancient apparition': 68, 'anti-mage': 1, 'arc warden': 113, 'axe': 2, 'bane': 3, 'batrider': 65, 'beastmaster': 38, 'bloodseeker': 4, 'bounty hunter': 62, 'brewmaster': 78, 'bristleback': 99, 'broodmother': 61, 'centaur warrunner': 96, 'chaos knight': 81, 'chen': 66, 'clinkz': 56, 'clockwerk': 51, 'crystal maiden': 5, 'dark seer': 55, 'dark willow': 119, 'dawnbreaker': 135, 'dazzle': 50, 'death prophet': 43, 'disruptor': 87, 'doom': 69, 'dragon knight': 49, 'drow ranger': 6, 'earth spirit': 107, 'earthshaker': 7, 'elder titan': 103, 'ember spirit': 106, 'enchantress': 58, 'enigma': 33, 'faceless void': 41, 'grimstroke': 121, 'gyrocopter': 72, 'hoodwink': 123, 'huskar': 59, 'invoker': 74, 'io': 91, 'jakiro': 64, 'juggernaut': 8, 'keeper of the light': 90, 'kez': 145, 'kunkka': 23, 'legion commander': 104, 'leshrac': 52, 'lich': 31, 'lifestealer': 54, 'lina': 25, 'lion': 26, 'lone druid': 80, 'luna': 48, 'lycan': 77, 'magnus': 97, 'marci': 136, 'mars': 129, 'medusa': 94, 'meepo': 82, 'mirana': 9, 'monkey king': 114, 'morphling': 10, 'muerta': 138, 'naga siren': 89, "nature's prophet": 53, 'necrophos': 36, 'night stalker': 60, 'nyx assassin': 88, 'ogre magi': 84, 'omniknight': 57, 'oracle': 111, 'outworld destroyer': 76, 'pangolier': 120, 'phantom assassin': 44, 'phantom lancer': 12, 'phoenix': 110, 'primal beast': 137, 'puck': 13, 'pudge': 14, 'pugna': 45, 'queen of pain': 39, 'razor': 15, 'riki': 32, 'ring master': 131, 'ringmaster': 131, 'rubick': 86, 'sand king': 16, 'shadow demon': 79, 'shadow fiend': 11, 'shadow shaman': 27, 'silencer': 75, 'skywrath mage': 101, 'slardar': 28, 'slark': 93, 'snapfire': 128, 'sniper': 35, 'spectre': 67, 'spirit breaker': 71, 'storm spirit': 17, 'sven': 18, 'techies': 105, 'templar assassin': 46, 'terrorblade': 109, 'tidehunter': 29, 'timbersaw': 98, 'tinker': 34, 'tiny': 19, 'treant protector': 83, 'troll warlord': 95, 'tusk': 100, 'underlord': 108, 'undying': 85, 'ursa': 70, 'vengeful spirit': 20, 'venomancer': 40, 'viper': 47, 'visage': 92, 'void spirit': 126, 'warlock': 37, 'weaver': 63, 'windranger': 21, 'winter wyvern': 112, 'witch doctor': 30, 'wraith king': 42, 'zeus': 22}

def get_team_names(soup):
    tags_block = soup.find('div', class_='plus__stats-details desktop-none')
    tags = tags_block.find_all('span', class_='title')
    scores = soup.find('div', class_='score__scores live').find_all('span')
    score = [i.text.strip() for i in scores]
    radiant_team_name, dire_team_name = None, None
    for tag in tags:
        team_info = tag.text.strip().split('')
        if team_info[1].replace(' ', '').lower() == 'radiant':
            radiant_team_name = team_info[0].lower().replace(' ', '')
        else:
            dire_team_name = team_info[0].lower().replace(' ', '')
    return radiant_team_name, dire_team_name, score


def get_player_names_and_heroes(soup):
    radiant_players, dire_players = {}, {}
    radiant_block = soup.find('div', class_='picks__new-picks__picks radiant')
    dire_block = soup.find('div', class_='picks__new-picks__picks dire')
    if radiant_block is not None and dire_block is not None:
        radiant_heroes_block = radiant_block.find_all('div', class_='pick player')
        dire_heroes_block = dire_block.find_all('div', class_='pick player')
        for hero in radiant_heroes_block[0:5]:
            hero_name = hero.get('data-tippy-content').replace('Outworld Devourer', 'Outworld Destroyer')
            player_name = hero.find('span', class_='pick__player-title').text.lower()
            player_name = re.sub(r'[^\w\s\u4e00-\u9fff]+', '', player_name)
            radiant_players[player_name] = {'hero': hero_name}
        for hero in dire_heroes_block:
            hero_name = hero.get('data-tippy-content').replace('Outworld Devourer', 'Outworld Destroyer')
            player_name = hero.find('span', class_='pick__player-title').text.lower()
            player_name = re.sub(r'[^\w\s\u4e00-\u9fff]+', '', player_name)
            dire_players[player_name] = {'hero': hero_name}
        if len(radiant_players) == 5 and len(dire_players) == 5:
            return radiant_players, dire_players
    return None


def get_team_positions(url):
    response = requests.get(url)
    if response.status_code == 200:
        response_html = html.unescape(response.text)
        soup = BeautifulSoup(response_html, 'lxml')
        picks_item = soup.find_all('div', class_='picks-item with-match-players-tooltip')
        # picks_item = soup.find('div', class_='match-statistics--teams-players')

        heroes = []
        for hero_block in picks_item:
            for hero in list(id_to_name.translate.values()):
                if f'({hero})' in hero_block.text:
                    heroes.append(hero)
        radiant_heroes_and_pos = {}
        dire_heroes_and_pos = {}
        for i in range(5):
            for translate_hero_id in id_to_name.translate:
                if id_to_name.translate[translate_hero_id] == heroes[i]:
                    hero_id = translate_hero_id
                    radiant_heroes_and_pos[f'pos{i + 1}'] = {'hero_id': hero_id, 'hero_name': heroes[i]}
        c = 0
        for i in range(5, 10):
            for translate_hero_id in id_to_name.translate:
                if id_to_name.translate[translate_hero_id] == heroes[i]:
                    hero_id = translate_hero_id
                    dire_heroes_and_pos[f'pos{c + 1}'] = {'hero_id': hero_id, 'hero_name': heroes[i]}
                    c += 1

        return radiant_heroes_and_pos, dire_heroes_and_pos
    print('РЅРµС‚Сѓ live РјР°С‚С‡РµР№')
    return None



def levenshtein_distance(s1, s2):
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def similarity_percentage(s1, s2):
    distance = levenshtein_distance(s1, s2)
    max_length = max(len(s1), len(s2))
    return (1 - distance / max_length) * 100


def are_similar(s1, s2, threshold=70):
    return similarity_percentage(s1, s2) >= threshold


def get_map_id(match):
    if match['team_dire'] is not None and match['team_radiant'] is not None \
            and 'Kobold' not in match['tournament']['name']:
        radiant_team_name = match['team_radiant']['name'].lower()
        dire_team_name = match['team_dire']['name'].lower()
        score = match['best_of_score']
        dic = {
            'fissure': 1,
            'riyadh': 1,
            'international': 1,
            'pgl': 1,
            'bb': 1,
            'epl': 2,
        }
        match_name = match['tournament']['name'].lower()
        tier = match['tournament']['tier']

        # РџСЂРѕРІРµСЂРєР° РЅР°Р»РёС‡РёСЏ РёРјРµРЅРё РІ СЃР»РѕРІР°СЂРµ Рё РѕР±РЅРѕРІР»РµРЅРёРµ Р·РЅР°С‡РµРЅРёСЏ tier
        for name, tier_val in dic.items():
            if name in match_name:
                tier = tier_val
        if tier in [1, 2, 3, 4, 5]:
            for karta in match['related_matches']:
                if karta['status'] == 'online':
                    map_id = karta['id']
                    url = f'https://cyberscore.live/en/matches/{map_id}/'
                    result = if_unique(url, score)
                    if result is not None:
                        return url, radiant_team_name, dire_team_name, score, tier
    return None


def if_unique(url, score):
    check_uniq_url = str(url) + '.' + str(int(score[0]) + int(score[1]))
    with open('count_synergy_10th_2000/map_id_check.txt', 'r+') as f:
        data = json.load(f)
        if check_uniq_url not in data:
            # data.append(url)
            # f.truncate()
            # f.seek(0)
            # json.dump(data, f)
            return True
    return None


def add_url(url):
    with open('count_synergy_10th_2000/map_id_check.txt', 'r+') as f:
        data = json.load(f)
        data.append(url)
        f.truncate()
        f.seek(0)
        json.dump(data, f)


def find_in_radiant(radiant_players, nick_name, translate, position, radiant_pick, radiant_lst):
    for radiant_player_name in radiant_players:
        if are_similar(radiant_player_name, nick_name, threshold=70):
            radiant_pick[translate[position]] = radiant_players[radiant_player_name]['hero']
            if position in radiant_lst:
                radiant_lst.remove(position)
                return radiant_lst, radiant_pick
    return None


def find_in_dire(dire_players, nick_name, translate, position, dire_pick, dire_lst):
    for dire_player_name in dire_players:
        if are_similar(dire_player_name, nick_name, threshold=70):
            dire_pick[translate[position]] = dire_players[dire_player_name]['hero']
            if position in dire_lst:
                dire_lst.remove(position)
                return dire_lst, dire_pick
    return None


def if_picks_are_done(soup):
    dire_block = soup.find('div', class_='picks__new-picks__picks dire')
    radiant_block = soup.find('div', class_='picks__new-picks__picks radiant')
    if radiant_block is not None and dire_block is not None:
        items_radiant = radiant_block.find('div', class_='items').find_all('div', class_='pick')
        items_dire = dire_block.find('div', class_='items').find_all('div', class_='pick')
        if len(items_dire) == 5 and len(items_radiant) == 5:
            return True
    return None


def clean_up(inp, length=0):
    if len(inp) >= length:
        copy = inp.copy()
        for i in inp:
            if 0.52 >= i >= 0.48:
                copy.remove(i)
        if len(copy) <= length:
            return inp
        return copy
    return inp











def process_synergy_data(position, synergies, team_positions):
    wr_list = []
    for synergy in synergies:
        tracker_position = synergy['position'].replace('pos ', 'pos')
        data_pos = synergy['other_pos'].replace('pos ', 'pos')
        data_hero = synergy['other_hero']
        data_wr = synergy['win_rate']
        if synergy['num_matches'] >= 15 and data_pos in team_positions and team_positions[data_pos][
                'hero_name'] == data_hero:
            if tracker_position == position:
                wr_list.append(data_wr)
    return wr_list


def process_matchup_data(position, matchups, opposing_team_positions):
    wr_list = []
    for matchup in matchups:
        tracker_position = matchup['position'].replace('pos ', 'pos')
        data_pos = matchup['other_pos'].replace('pos ', 'pos')
        data_hero = matchup['other_hero']
        data_wr = matchup['win_rate']
        if matchup['num_matches'] >= 15 and data_pos in opposing_team_positions and \
                opposing_team_positions[data_pos]['hero_name'] == data_hero:
            if tracker_position == position:
                wr_list.append(data_wr)
    return wr_list





STAR_THRESHOLDS_PATH = Path(
    os.getenv('STAR_THRESHOLDS_PATH', '/Users/alex/Documents/ingame/data/star_thresholds_by_wr.json')
)

# Fallback, если JSON не найден
_STAR_THRESHOLDS_FALLBACK = {
    60: {
        'early_output': [
            ('counterpick_1vs1', 4),
            ('counterpick_1vs2', 7),
            ('synergy_duo', 7),
            ('solo', 3),
            ('synergy_trio', 7),
        ],
        'mid_output': [
            ('counterpick_1vs1', 5),
            ('counterpick_1vs2', 8),
            ('synergy_duo', 8),
            ('synergy_trio', 6),
            ('solo', 3),
        ],
    },
}


def _load_star_thresholds() -> dict:
    if STAR_THRESHOLDS_PATH.exists():
        try:
            data = json.loads(STAR_THRESHOLDS_PATH.read_text(encoding='utf-8'))
            parsed = {}
            if isinstance(data, dict):
                for k, v in data.items():
                    try:
                        key = int(k)
                    except Exception:
                        continue
                    if not isinstance(v, dict):
                        continue
                    block = {}
                    for section in ('early_output', 'mid_output'):
                        items = v.get(section) or []
                        block[section] = [(m, int(t)) for m, t in items]
                    parsed[key] = block
            if parsed:
                return parsed
        except Exception:
            pass
    return _STAR_THRESHOLDS_FALLBACK.copy()


STAR_THRESHOLDS_BY_WR = _load_star_thresholds()


def format_output_dict(output_dict, flag=False, none_trashold=None):
    def mark_if_exceeds(data, key, threshold):
        val = data.get(key)
        if val is None or isinstance(val, str):
            return False, None
        if abs(val) >= threshold:
            data[key] = f"{val}*"
            sign = 1 if val > 0 else (-1 if val < 0 else None)
            return True, sign
        return False, None
    # Пороговые наборы (подбирались по pro_new_holdout_200kfiles.txt).
    # Выбор набора: STAR_THRESHOLD_WR (проценты, например 70). По умолчанию 65.
    try:
        target_wr = int(os.getenv('STAR_THRESHOLD_WR', '60'))
    except ValueError:
        target_wr = 60
    thresholds = STAR_THRESHOLDS_BY_WR.get(target_wr, STAR_THRESHOLDS_BY_WR[60])

    any_valid_block = False
    for section, metrics in thresholds.items():
        data = output_dict.get(section, {})
        block_star_count = 0
        block_sign = None
        block_conflict = False
        for key, threshold in metrics:
            hit, sign = mark_if_exceeds(data, key, threshold)
            if not hit:
                continue
            block_star_count += 1
            if sign is None:
                continue
            if block_sign is None:
                block_sign = sign
            elif block_sign != sign:
                block_conflict = True
        if block_star_count > 0 and not block_conflict:
            any_valid_block = True
    return any_valid_block



def get_map_players(data, match, soup, name_to_pos):
    radiant_pick = match.find('div', class_='picks__new-picks__picks radiant').find('div',
                                                                                    class_='items').find_all(
        'div', class_='pick player')
    dire_pick = match.find('div', class_='picks__new-picks__picks dire').find('div',
                                                                              class_='items').find_all(
        'div', class_='pick player')
    if not radiant_pick:
        return None
    for player in radiant_pick:
        data_hero_id = player['data-hero-id']
        data_tippy_content = player['data-tippy-content']
        player_title = player.find('span', class_='pick__player-title').text.lower()
        data.setdefault('radiant', []).append(
            {'hero_id': data_hero_id, 'hero_name': data_tippy_content, 'player_name': player_title})
    if len(data['radiant']) != 5:
        return None
    for player in dire_pick:
        data_hero_id = player['data-hero-id']
        data_tippy_content = player['data-tippy-content']
        player_title = player.find('span', class_='pick__player-title').text.lower()
        data.setdefault('dire', []).append(
            {'hero_id': data_hero_id, 'hero_name': data_tippy_content, 'player_name': player_title})
    if len(data['dire']) != 5:
        return None
    teams = soup.find_all('div', class_='lineups__team-players')
    for team in teams:
        players = team.find_all('div', class_='player')
        for player in players:
            role_data = player.find('div', class_='player__role')
            if not role_data:
                return None
            role = role_data.find('span').text
            role = name_to_pos[role]
            name = player.find('div', class_='player__name').find('div',
                                                                  class_='player__name-name').text.lower()
            for side in [data['radiant'], data['dire']]:
                for i in range(len(side)):
                    if side[i]['player_name'] == name:
                        side[i]['role'] = role
    roles = ['pos1', 'pos2', 'pos3', 'pos4', 'pos5']
    for player in data['radiant']:
        if 'role' in player:
            if player['role'] not in roles:
                return None
            roles.remove(player['role'])
    if len(roles) == 1:
        for player in data['radiant']:
            if 'role' not in player:
                player['role'] = roles[0]
    roles = ['pos1', 'pos2', 'pos3', 'pos4', 'pos5']
    for player in data['dire']:
        if 'role' in player:
            if player['role'] not in roles:
                return None
            roles.remove(player['role'])
    if len(roles) == 1:
        for player in data['dire']:
            if 'role' not in player:
                player['role'] = roles[0]

    radiant_heroes_and_pos = {
        player['role']: {'hero_name': player['hero_name'], 'hero_id': player['hero_id']} for player in
        data['radiant']}
    dire_heroes_and_pos = {
        player['role']: {'hero_name': player['hero_name'], 'hero_id': player['hero_id']} for
        player in data['dire']}

    if len(radiant_heroes_and_pos) != 5 or len(dire_heroes_and_pos) != 5:
        return None
    radiant_team_name = data['teams']['radiant'].lower()
    dire_team_name = data['teams']['dire'].lower()
    return radiant_team_name, dire_team_name, radiant_heroes_and_pos, dire_heroes_and_pos


def some_func():
    with open('teams_stat_dict.txt') as f:
        data = json.load(f)
        data_copy = data.copy()
        for team in data_copy:
            odd = data[team]['kills'] / data[team]['time']
            data.setdefault(team, {}).setdefault('odd', odd)
        sorted_data = dict(sorted(data.items(), key=lambda item: item[1]["odd"]))
    with open('teams_stat_dict.txt', 'w') as f:
        json.dump(sorted_data, f, indent=4)


# def get_pro_players_ids(counter=0):
#     bottle, pro_ids = set(), set()
#     for name in pro_teams:
#         counter += 1
#         print(f'{counter}/{len(pro_teams)}')
#         bottle.add(pro_teams[name]['id'])
#         if len(bottle) == 5 or counter == len(pro_teams):
#             query = '''
#                     {teams(teamIds: %s){
#                         members{
#                             lastMatchDateTime
#                         steamAccount{
#                           id
#                           name
#
#                         }
#                         team {
#                           id
#                           name
#                         }
#                       }
#                     }}''' % list(bottle)
#             headers = {
#                 "Content-Type": "application/json",
#                 "Accept": "application/json",
#                 "Accept-Encoding": "gzip, deflate, br, zstd",
#                 "Origin": "https://api.stratz.com",
#                 "Referer": "https://api.stratz.com/graphiql",
#                 "User-Agent": "STRATZ_API",
#                 "Authorization": f"Bearer {api_token_5}"
#             }
#             response = requests.post('https://api.stratz.com/graphql', json={"query": query}, headers=headers)
#             teams = json.loads(response.text)['data']['teams']
#             for team in teams:
#                 last_date = 0
#                 for member in team['members']:
#                     if last_date < member['lastMatchDateTime']:
#                         last_date = member['lastMatchDateTime']
#                 for member in team['members']:
#                     if member['lastMatchDateTime'] == last_date:
#                         pro_ids.add(member['steamAccount']['id'])
#             bottle = set()
#     return pro_ids


def merge_dicts(dict1, dict2):
    """
    Р¤СѓРЅРєС†РёСЏ РґР»СЏ РѕР±СЉРµРґРёРЅРµРЅРёСЏ РґРІСѓС… СЃР»РѕРІР°СЂРµР№. Р•СЃР»Рё РєР»СЋС‡Рё РїРµСЂРµСЃРµРєР°СЋС‚СЃСЏ, Р·РЅР°С‡РµРЅРёСЏ РѕР±СЉРµРґРёРЅСЏСЋС‚СЃСЏ.
    Р•СЃР»Рё РєР»СЋС‡ СѓРЅРёРєР°Р»РµРЅ, РѕРЅ РїСЂРѕСЃС‚Рѕ РґРѕР±Р°РІР»СЏРµС‚СЃСЏ.
    """
    for key, value in dict2.items():
        if key in dict1:
            if isinstance(value, dict) and isinstance(dict1[key], dict):
                dict1[key] = merge_dicts(dict1[key], value)
            elif isinstance(value, list) and isinstance(dict1[key], list):
                dict1[key].extend(value)
            else:
                dict1[key] += value
        else:
            dict1[key] = value
    return dict1


def calculate_average(values):
    return sum(values) / len(values) if len(values) else None


def synergy_team(heroes_and_pos, output, mkdir, data, min_matches_trio=20):
    """
    Анализирует синергию героев в команде

    Args:
        heroes_and_pos: словарь героев и позиций
        output: выходной словарь
        mkdir: префикс для ключей (radiant_synergy/dire_synergy)
        data: данные статистики
        min_matches_trio: минимальное количество матчей для trio (по умолчанию 20)
    """
    # Проверка валидности входных данных
    if not isinstance(heroes_and_pos, dict):
        print(f"ОШИБКА в synergy_team: heroes_and_pos должен быть словарем, получен {type(heroes_and_pos)} = {heroes_and_pos}")
        return
    
    if not heroes_and_pos:
        print(f"ПРЕДУПРЕЖДЕНИЕ в synergy_team: heroes_and_pos пустой словарь для {mkdir}")
        return
    
    unique_combinations = set()
    items = list(heroes_and_pos.items())

    for i in range(len(items)):
        pos, hero_data = items[i]
        hero_id = str(hero_data['hero_id'])
        hero_key = f"{hero_id}{pos}"

        for j in range(i + 1, len(items)):
            second_pos, second_data = items[j]
            second_hero_id = str(second_data['hero_id'])
            if hero_id == second_hero_id:
                continue
            second_key = f"{second_hero_id}{second_pos}"

            pair = sorted([hero_key, second_key])
            key = f"{pair[0]}_with_{pair[1]}"
            foo = data.get(key, {})

            games = foo.get('games', 0)
            if games >= SYNERGY_DUO_MIN_MATCHES:
                # Учитываем позиции, чтобы не смешивать разные конфигурации дуо
                combo = tuple(sorted([f"{hero_id}{pos}", f"{second_hero_id}{second_pos}"]))
                if combo not in unique_combinations:
                    unique_combinations.add(combo)
                    wins = foo['wins']
                    value = wins / games
                    # Сохраняем (winrate, count) для взвешивания в get_diff
                    output.setdefault(f'{mkdir}_duo', []).append((value, games))

                    # Support duo (pos4+pos5)
                    if all(p in ['pos4', 'pos5'] for p in (pos, second_pos)):
                        output.setdefault(f'{mkdir}_support_duo', []).append((value, games))
                    # Cores duo (оба в pos1-3)
                    if all(p in ['pos1', 'pos2', 'pos3'] for p in (pos, second_pos)):
                        output.setdefault(f'{mkdir}_cores_duo', []).append((value, games))

            # Анализ трио
        for j in range(i + 1, len(items)):
            second_pos, second_data = items[j]
            second_hero_id = str(second_data['hero_id'])
            if second_hero_id == hero_id:
                continue
            second_key = f"{second_hero_id}{second_pos}"

            for k in range(j + 1, len(items)):
                third_pos, third_data = items[k]
                third_hero_id = str(third_data['hero_id'])
                if third_hero_id in [second_hero_id, hero_id]:
                    continue
                third_key = f"{third_hero_id}{third_pos}"

                parts = sorted([hero_key, second_key, third_key])
                key = ",".join(parts)
                foo = data.get(key, {})

                games = foo.get('games', 0)
                if games >= min_matches_trio:
                    combo = tuple(parts)
                    if combo not in unique_combinations:
                        unique_combinations.add(combo)
                        wins = foo['wins']
                        value = wins / games

                        # Фильтруем trio: минимум 2 кора (pos1-3)
                        trio_positions = {pos, second_pos, third_pos}
                        cores_positions = trio_positions & {'pos1', 'pos2', 'pos3'}

                        # Сохраняем (winrate, count) для взвешивания в get_diff
                        if len(cores_positions) >= 2:
                            output.setdefault(f'{mkdir}_trio_2cores', []).append((value, games))
                        if all(i in trio_positions for i in ('pos1', 'pos2', 'pos3')):
                            output.setdefault(f'{mkdir}_trio_all_cores', []).append((value, games))
                        output.setdefault(f'{mkdir}_trio', []).append((value, games))



def counterpick_team(heroes_and_pos, heroes_and_pos_opposite, output, mkdir, data, pos1_matchup=None, check_solo=False):
    """
    Анализирует контрпики против вражеской команды
    ИЗМЕНЕНО: теперь сохраняет (winrate, num_matches) вместо просто winrate
    """
    unique_combinations = set()
    def _canon_vs(left, right):
        if left <= right:
            return f"{left}_vs_{right}", True
        return f"{right}_vs_{left}", False

    opp_items = list(heroes_and_pos_opposite.items())
    for pos in heroes_and_pos:
        hero_id = str(heroes_and_pos[pos]['hero_id'])
        hero_key = f"{hero_id}{pos}"
        if check_solo:
            foo = data.get(hero_key, {})
            games = foo.get('games', 0)
            if games >= SOLO_MIN_MATCHES:
                wins = foo['wins']
                value = wins / games
                # Сохраняем (winrate, count) для взвешивания в get_diff
                output.setdefault(f'{mkdir}_solo', {}).setdefault(pos, []).append((value, games))
        # 1vs1 matchups
        for enemy_pos, enemy_data in opp_items:
            enemy_hero_id = str(enemy_data['hero_id'])
            enemy_key = f"{enemy_hero_id}{enemy_pos}"
            key, hero_left = _canon_vs(hero_key, enemy_key)
            foo = data.get(key, {})

            games = foo.get('games', 0)
            if games >= COUNTERPICK_1VS1_MIN_MATCHES:
                wins = foo['wins']
                value = wins / games
                if not hero_left:
                    value = 1 - value

                # Сохраняем (winrate, count) для взвешивания в get_diff
                output.setdefault(f'{mkdir}_1vs1', {}).setdefault(pos, []).append((value, games))

                # Core vs Core matchups (pos1-3 vs pos1-3)
                if pos in CORE_POSITIONS and enemy_pos in CORE_POSITIONS:
                    output.setdefault(f'{mkdir}_1vs1_cores', {}).setdefault(pos, []).append((value, games))

            # 1vs2 matchups
        for i in range(len(opp_items)):
            enemy_pos, enemy_data = opp_items[i]
            enemy_hero_id = str(enemy_data['hero_id'])
            enemy_key = f"{enemy_hero_id}{enemy_pos}"
            for j in range(i + 1, len(opp_items)):
                second_enemy_pos, second_enemy_data = opp_items[j]
                second_enemy_id = str(second_enemy_data['hero_id'])
                if enemy_hero_id == second_enemy_id:
                    continue

                duo_parts = sorted([enemy_key, f"{second_enemy_id}{second_enemy_pos}"])
                duo_key = ",".join(duo_parts)
                key, hero_left = _canon_vs(hero_key, duo_key)
                foo = data.get(key, {})

                games = foo.get('games', 0)
                if games >= COUNTERPICK_1VS2_MIN_MATCHES:
                    # Учитываем позиции, чтобы не смешивать разные конфигурации
                    combo = (
                        f"{hero_id}{pos}",
                        *tuple(sorted([
                            f"{enemy_hero_id}{enemy_pos}",
                            f"{second_enemy_id}{second_enemy_pos}"
                        ]))
                    )
                    if combo not in unique_combinations:
                        unique_combinations.add(combo)
                        wins = foo['wins']
                        value = wins / games
                        if not hero_left:
                            value = 1 - value
                        # Сохраняем (winrate, count) для взвешивания в get_diff
                        if pos in {'pos1', 'pos2', 'pos3'} and any(i in {'pos1', 'pos2', 'pos3'} for i in [second_enemy_pos, enemy_pos]):
                            output.setdefault(f'{mkdir}_1vs2_two_cores', {}).setdefault(pos, []).append((value, games))
                        if pos in {'pos1', 'pos2', 'pos3'}:
                            output.setdefault(f'{mkdir}_1vs2_one_core', {}).setdefault(pos, []).append((value, games))
                        if pos in {'pos1', 'pos2', 'pos3'} and all(i in {'pos1', 'pos2', 'pos3'} for i in [second_enemy_pos, enemy_pos]):
                            output.setdefault(f'{mkdir}_1vs2_all_cores', {}).setdefault(pos, []).append((value, games))
                        # Сохраняем все 1vs2
                        output.setdefault(f'{mkdir}_1vs2', {}).setdefault(pos, []).append((value, games))


# functions.py
def get_diff_another(radiant, dire, weight_check=False, custom_weights=None, min_len=2):
    if radiant is None or dire is None:
        return None

    # === Вариант на основе baseline из functions_improved ===
    if GET_DIFF_VARIANT == 'baseline':
        try:
            return get_diff(radiant, dire, _1vs2=bool(weight_check))
        except Exception:
            return None

    # Подготовка входа для synergy (списки) и 1vs2 (dict по позициям)
    if not weight_check:
        if isinstance(dire, dict):
            dire = list(chain(*dire.values()))
            radiant = list(chain(*radiant.values()))
        if len(radiant) < min_len or len(dire) < min_len:
            return None

        # Извлекаем значения из кортежей (wr, count) если нужно
        def extract_values(items):
            """Извлекает значения из списка, поддерживая формат (wr, count) или просто wr"""
            values = []
            for it in items:
                if isinstance(it, (tuple, list)) and len(it) >= 1:
                    values.append(float(it[0]))
                else:
                    try:
                        values.append(float(it))
                    except (TypeError, ValueError):
                        continue
            return values

        vals_r = extract_values(radiant)
        vals_d = extract_values(dire)

        if len(vals_r) < min_len or len(vals_d) < min_len:
            return None

        if GET_DIFF_VARIANT == 'median':
            try:
                from statistics import median
                r = median(vals_r)
                d = median(vals_d)
            except Exception:
                r = sum(vals_r) / len(vals_r) if vals_r else None
                d = sum(vals_d) / len(vals_d) if vals_d else None
            if r is None or d is None:
                return None
            return round((r - d) * 100)
        if GET_DIFF_VARIANT == 'trimmed':
            # 20% trimmed mean
            vals_r_sorted = sorted(vals_r)
            vals_d_sorted = sorted(vals_d)
            k_r = max(0, int(len(vals_r_sorted) * 0.2))
            k_d = max(0, int(len(vals_d_sorted) * 0.2))
            trimmed_r = vals_r_sorted[k_r:len(vals_r_sorted)-k_r] if len(vals_r_sorted) - 2*k_r > 0 else vals_r_sorted
            trimmed_d = vals_d_sorted[k_d:len(vals_d_sorted)-k_d] if len(vals_d_sorted) - 2*k_d > 0 else vals_d_sorted
            r = sum(trimmed_r) / len(trimmed_r) if trimmed_r else None
            d = sum(trimmed_d) / len(trimmed_d) if trimmed_d else None
            if r is None or d is None:
                return None
            return round((r - d) * 100)
        # mean (исходный)
        r = sum(vals_r) / len(vals_r) if vals_r else None
        d = sum(vals_d) / len(vals_d) if vals_d else None
        if r is None or d is None:
            return None
        return round((r - d) * 100)

    # === 1vs2 и подобные (dict позиций) ===
    if custom_weights is not None:
        weights = custom_weights
    else:
        weights = {'pos1': 2.0, 'pos2': 2.0, 'pos3': 1.4, 'pos4': 1.0, 'pos5': 1.0}

    def wmean(side):
        if not isinstance(side, dict):
            return None
        weighted_sum = 0.0
        total_weight = 0.0
        for pos, w in weights.items():
            vals = side.get(pos, [])
            if not vals:
                continue

            # Извлекаем значения и веса из кортежей (wr, count) если нужно
            def extract_weighted_values(items):
                """Извлекает значения и веса из списка кортежей (wr, count) или просто wr"""
                values = []
                weights_list = []
                for it in items:
                    if isinstance(it, (tuple, list)) and len(it) >= 1:
                        values.append(float(it[0]))
                        weights_list.append(float(it[1]) if len(it) >= 2 else 1.0)
                    else:
                        try:
                            values.append(float(it))
                            weights_list.append(1.0)
                        except (TypeError, ValueError):
                            continue
                return values, weights_list

            values, item_weights = extract_weighted_values(vals)
            if not values:
                continue

            # Вычисляем среднее с учетом весов элементов
            if GET_DIFF_VARIANT == 'median':
                try:
                    from statistics import median
                    m = median(values)
                    n = sum(item_weights)  # Используем сумму весов как количество
                except Exception:
                    # Взвешенное среднее как fallback
                    weighted_val = sum(v * w for v, w in zip(values, item_weights, strict=False))
                    total_w = sum(item_weights)
                    m = weighted_val / total_w if total_w > 0 else None
                    n = total_w
                    if m is None:
                        continue
            elif GET_DIFF_VARIANT == 'trimmed':
                # Сортируем по значениям, сохраняя веса
                sorted_pairs = sorted(zip(values, item_weights, strict=False))
                k = max(0, int(len(sorted_pairs) * 0.2))
                trimmed_pairs = sorted_pairs[k:len(sorted_pairs)-k] if len(sorted_pairs) - 2*k > 0 else sorted_pairs
                if not trimmed_pairs:
                    continue
                trimmed_values, trimmed_weights = zip(*trimmed_pairs, strict=False)
                weighted_val = sum(v * w for v, w in zip(trimmed_values, trimmed_weights, strict=False))
                total_w = sum(trimmed_weights)
                m = weighted_val / total_w if total_w > 0 else None
                n = total_w
                if m is None:
                    continue
            else:
                # Взвешенное среднее для mean варианта
                weighted_val = sum(v * w for v, w in zip(values, item_weights, strict=False))
                total_w = sum(item_weights)
                m = weighted_val / total_w if total_w > 0 else None
                n = total_w
                if m is None:
                    continue

            weighted_sum += m * w * n
            total_weight += w * n
        return (weighted_sum / total_weight) if total_weight > 0 else None

    r = wmean(radiant)
    d = wmean(dire)
    if r is not None and d is not None:
        return round((r - d) * 100)
    return None




def get_multiplied_results(radiant, dire, radiant_new=1, dire_new =1):
    if all(foo is not None and len(foo)>0 for foo in (radiant, dire)):
        for i in radiant:
            radiant_new *= i
        for i in dire:
            dire_new *= i
        total = (radiant_new + dire_new)
        if total == 0:
            return None
        return round(radiant_new / total * 100 - 50)
    return None
def get_ordinar_results(radiant, dire):
    if all(foo is not None and len(foo) > 2 for foo in (radiant, dire)):
        return round((sum(radiant)/len(radiant) - sum(dire)/len(dire))*100)
    return None

# def calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos, data, over40_1vs2=None, over40_duo_synergy=None,
#                      over40_duo_counterpick=None, over40_solo=None, over40_trio=None, over40_pos1_matchup=None):
#     output = {}
#     over40_counter(radiant_heroes_and_pos, dire_heroes_and_pos, data, output, mkdir='radiant')
#     over40_counter(dire_heroes_and_pos, radiant_heroes_and_pos, data, output, mkdir='dire')
#     synergy_over40(radiant_heroes_and_pos, data, output, mkdir='radiant')
#     synergy_over40(dire_heroes_and_pos, data, output, mkdir='dire')
#     if 'radiant_pos1_matchup' in output:
#         over40_pos1_matchup = round((output['radiant_pos1_matchup'] - 0.50)*100)
#     if all(i in output for i in ['dire_winrate1vs1', 'radiant_winrate1vs1']) and all(len(output['radiant_winrate1vs1'].get(p, [])) >= 1 for p in ['pos1', 'pos2', 'pos3']) and \
#             all(len(output['dire_winrate1vs1'].get(p, [])) >= 1 for p in ['pos1', 'pos2', 'pos3']):
#         over40_duo_counterpick = get_diff(output['radiant_winrate1vs1'],
#                                             output['dire_winrate1vs1'], _1vs2=True)
#     if all(i in output for i in ['radiant_winrate1vs2', 'dire_winrate1vs2']) and all(len(output['radiant_winrate1vs2'].get(p, [])) >= 1 for p in ['pos1', 'pos2', 'pos3']) and \
#             all(len(output['dire_winrate1vs2'].get(p, [])) >= 1 for p in ['pos1', 'pos2', 'pos3']):
#         over40_1vs2 = get_diff(output['radiant_winrate1vs2'], output['dire_winrate1vs2'], _1vs2=True)
#     if all(i in output for i in ['radiant_over40_solo', 'dire_over40_solo']) and all(len(output['radiant_over40_solo'].get(p, [])) >= 1 for p in ['pos1', 'pos2', 'pos3']) and \
#             all(len(output['dire_over40_solo'].get(p, [])) >= 1 for p in ['pos1', 'pos2', 'pos3']):
#         over40_solo = get_diff(output['radiant_over40_solo'], output['dire_over40_solo'], _1vs2=True)
#     if all(i in output for i in ['radiant_over40_trio', 'dire_over40_trio']):
#         over40_trio = get_diff(output['radiant_over40_trio'], output['dire_over40_trio'])
#     if all(i in output for i in ['radiant_over40_duo_synergy', 'dire_over40_duo_synergy']) and all(len(output['radiant_over40_duo_synergy'].get(p, [])) >= 1 for p in ['pos1', 'pos2', 'pos3']) and \
#             all(len(output['dire_over40_duo_synergy'].get(p, [])) >= 1 for p in ['pos1', 'pos2', 'pos3']):
#         over40_duo_synergy = get_diff(output['radiant_over40_duo_synergy'], output['dire_over40_duo_synergy'], _1vs2=True)
#     return over40_duo_synergy, over40_duo_counterpick, over40_1vs2, over40_solo, over40_duo_synergy, over40_trio, over40_pos1_matchup


# def over40_counter(heroes_and_pos, heroes_and_pos_opposite, data, output, mkdir):
#     unique_combinations = set()
#     winrate_1vs1, winrate1vs2_cores, winrate1vs2_sups, winrate1vs2 = {}, {}, {}, {}
#     for pos in heroes_and_pos:
#         # if pos in ['pos4', 'pos5']: continue
#         hero_id = str(heroes_and_pos[pos]['hero_id'])
#         for enemy_pos in heroes_and_pos_opposite:
#             enemy_hero_id = str(heroes_and_pos_opposite[enemy_pos]['hero_id'])
#             key = f"{hero_id}{pos}_vs_{enemy_hero_id}{enemy_pos}"
#             foo = data.get(key, {})
#             if len(foo) >= 15:
#                 value = foo.count(1) / (foo.count(1) + foo.count(0))
#                 if pos == 'pos1' and enemy_pos == 'pos1':
#                     output.setdefault(f'{mkdir}_pos1_matchup', value)
#                 output.setdefault(f'{mkdir}_winrate1vs1', {}).setdefault(pos, []).append(value)
#             for second_enemy_pos in heroes_and_pos_opposite:
#                 second_enemy_id = str(heroes_and_pos_opposite[second_enemy_pos]['hero_id'])
#                 if enemy_hero_id == second_enemy_id:
#                     continue
#
#                 key = f"{hero_id}{pos}_vs_{enemy_hero_id}{enemy_pos},{second_enemy_id}{second_enemy_pos}"
#                 foo = data.get(key, {})
#
#                 if len(foo) >= 10:
#                     combo = (hero_id,) + tuple(sorted([enemy_hero_id, second_enemy_id]))
#                     if combo not in unique_combinations:
#                         unique_combinations.add(combo)
#                         value = foo.count(1) / (foo.count(1) + foo.count(0))
#                         output.setdefault(f'{mkdir}_winrate1vs2', {}).setdefault(pos, []).append(value)


def check_bad_map(match, maps_data=None, break_flag=False, start_date_time=None):
    # Проверка валидности входных данных
    if not isinstance(match, dict):
        print(f"ОШИБКА в check_bad_map: match должен быть словарем, получен {type(match)} = {match}")
        return None
    
    if 'startDateTime' not in match:
        print(f"ОШИБКА в check_bad_map: у match нет ключа 'startDateTime'")
        return None
    
    if start_date_time is not None and match['startDateTime'] < int(start_date_time):
        return None
    
    dire_heroes_and_pos = {}
    radiant_heroes_and_pos = {}
    
    if 'players' not in match:
        print(f"ОШИБКА в check_bad_map: у match нет ключа 'players'")
        return None
    
    players = match['players']
    for player in players:
        if not isinstance(player, dict):
            print(f"ОШИБКА в check_bad_map: player должен быть словарем, получен {type(player)}")
            return None
        
        hero_id = player.get('heroId')
        position = player.get('position')
        if position is None:
            return None
        
        # Проверка, что position это строка или число, из которого можно извлечь последний символ
        if not isinstance(position, (str, int)):
            print(f"ОШИБКА в check_bad_map: position должен быть строкой или числом, получен {type(position)} = {position}")
            return None
        
        # Преобразуем в строку для безопасного извлечения последнего символа
        position_str = str(position)
        if not position_str:
            print(f"ОШИБКА в check_bad_map: position пустой")
            return None
        
        position_key = f'pos{position_str[-1]}'
        if player.get('isRadiant'):
            radiant_heroes_and_pos.setdefault(position_key, {}).setdefault('hero_id', hero_id)
        else:
            dire_heroes_and_pos.setdefault(position_key, {}).setdefault('hero_id', hero_id)
    r_keys = sorted(radiant_heroes_and_pos.keys())
    d_keys = sorted(dire_heroes_and_pos.keys())
    if not all(i == ['pos1', 'pos2', 'pos3', 'pos4', 'pos5'] for i in
               [r_keys, d_keys]) or break_flag:
        return None
    return radiant_heroes_and_pos, dire_heroes_and_pos


def synergy_and_counterpick(radiant_heroes_and_pos, dire_heroes_and_pos, early_dict, mid_dict, match=None, custom_weights=None,
                              early_trio_threshold=SYNERGY_TRIO_MIN_MATCHES, mid_trio_threshold=SYNERGY_TRIO_MIN_MATCHES,
                              comeback_dict=None, comeback_trio_threshold=SYNERGY_TRIO_MIN_MATCHES,
                              synergy_duo_use_max=False, early_position_weights=None, late_position_weights=None):
    """
    Основная функция анализа синергии и контрпиков

    Args:
        radiant_heroes_and_pos: герои и позиции радианта
        dire_heroes_and_pos: герои и позиции дира
        early_dict: данные для early фазы
        mid_dict: данные для mid фазы
        match: данные матча (опционально)
        custom_weights: кастомные веса позиций (опционально)
        early_trio_threshold: минимум матчей для early trio (по умолчанию 20)
        mid_trio_threshold: минимум матчей для mid trio (по умолчанию 20)
        comeback_dict: данные для comeback фазы (опционально)
        comeback_trio_threshold: минимум матчей для comeback trio (по умолчанию 20)
        synergy_duo_use_max: если True, берёт лучший duo по winrate (без учёта количества матчей);
                             если False, использует взвешенное среднее по матчам (по умолчанию)
    """
    return_dict = {}
    early_output, mid_output, comeback_output = {}, {}, {}
    early_weights = early_position_weights or custom_weights or _ENV_POS_WEIGHTS_EARLY or _ENV_POS_WEIGHTS or EARLY_POSITION_WEIGHTS
    late_weights = late_position_weights or custom_weights or _ENV_POS_WEIGHTS_LATE or _ENV_POS_WEIGHTS or LATE_POSITION_WEIGHTS
    def _all_heroes_known(radiant, dire):
        for side in (radiant, dire):
            if not isinstance(side, dict):
                return False
            for pos in ('pos1', 'pos2', 'pos3', 'pos4', 'pos5'):
                hero_id = side.get(pos, {}).get('hero_id')
                try:
                    hero_id = int(hero_id)
                except (TypeError, ValueError):
                    return False
                if hero_id <= 0:
                    return False
        return True

    def _is_valid_hero_id(hero_id):
        try:
            return int(hero_id) > 0
        except (TypeError, ValueError):
            return False

    def _team_list(side):
        return [(pos, side[pos].get('hero_id')) for pos in ('pos1', 'pos2', 'pos3', 'pos4', 'pos5') if pos in side]

    def _covers_solo(data, team):
        if not isinstance(data, dict):
            return False
        for pos, hero_id in team:
            if not _is_valid_hero_id(hero_id):
                return False
            key = f"{int(hero_id)}{pos}"
            games = data.get(key, {}).get('games', 0)
            if games < SOLO_MIN_MATCHES:
                return False
        return True

    def _covers_duo(data, team):
        if not isinstance(data, dict):
            return False
        for i, (pos_i, hero_i) in enumerate(team):
            if not _is_valid_hero_id(hero_i):
                return False
            found = False
            for j, (pos_j, hero_j) in enumerate(team):
                if i == j:
                    continue
                if not _is_valid_hero_id(hero_j):
                    continue
                parts = sorted([f"{int(hero_i)}{pos_i}", f"{int(hero_j)}{pos_j}"])
                key = f"{parts[0]}_with_{parts[1]}"
                games = data.get(key, {}).get('games', 0)
                if games >= SYNERGY_DUO_MIN_MATCHES:
                    found = True
                    break
            if not found:
                return False
        return True

    def _covers_trio(data, team, min_matches_trio):
        if not isinstance(data, dict):
            return False
        covered = set()
        n = len(team)
        for i in range(n):
            pos_i, hero_i = team[i]
            if not _is_valid_hero_id(hero_i):
                return False
            for j in range(i + 1, n):
                pos_j, hero_j = team[j]
                if not _is_valid_hero_id(hero_j):
                    return False
                for k in range(j + 1, n):
                    pos_k, hero_k = team[k]
                    if not _is_valid_hero_id(hero_k):
                        return False
                    parts = [
                        f"{int(hero_i)}{pos_i}",
                        f"{int(hero_j)}{pos_j}",
                        f"{int(hero_k)}{pos_k}",
                    ]
                    key = ",".join(sorted(parts))
                    games = data.get(key, {}).get('games', 0)
                    has_games = games >= min_matches_trio
                    if has_games:
                        covered.update([i, j, k])
        return len(covered) == n

    def _covers_1vs1(data, team, opp):
        if not isinstance(data, dict):
            return False
        for pos_i, hero_i in team:
            if not _is_valid_hero_id(hero_i):
                return False
            found = False
            for pos_j, hero_j in opp:
                if not _is_valid_hero_id(hero_j):
                    continue
                left = f"{int(hero_i)}{pos_i}"
                right = f"{int(hero_j)}{pos_j}"
                key = f"{left}_vs_{right}" if left <= right else f"{right}_vs_{left}"
                games = data.get(key, {}).get('games', 0)
                if games >= COUNTERPICK_1VS1_MIN_MATCHES:
                    found = True
                    break
            if not found:
                return False
        return True

    def _covers_1vs2(data, team, opp):
        if not isinstance(data, dict):
            return False
        for pos_i, hero_i in team:
            if not _is_valid_hero_id(hero_i):
                return False
            found = False
            for a in range(len(opp)):
                pos_a, hero_a = opp[a]
                if not _is_valid_hero_id(hero_a):
                    continue
                for b in range(a + 1, len(opp)):
                    pos_b, hero_b = opp[b]
                    if not _is_valid_hero_id(hero_b) or hero_a == hero_b:
                        continue
                    solo = f"{int(hero_i)}{pos_i}"
                    duo_parts = sorted([f"{int(hero_a)}{pos_a}", f"{int(hero_b)}{pos_b}"])
                    duo = ",".join(duo_parts)
                    key = f"{solo}_vs_{duo}" if solo <= duo else f"{duo}_vs_{solo}"
                    games = data.get(key, {}).get('games', 0)
                    if games >= COUNTERPICK_1VS2_MIN_MATCHES:
                        found = True
                        break
                if found:
                    break
            if not found:
                return False
        return True

    all_heroes_known = _all_heroes_known(radiant_heroes_and_pos, dire_heroes_and_pos)
    radiant_team = _team_list(radiant_heroes_and_pos)
    dire_team = _team_list(dire_heroes_and_pos)

    synergy_team(radiant_heroes_and_pos, early_output, 'radiant_synergy', early_dict, min_matches_trio=early_trio_threshold)
    synergy_team(dire_heroes_and_pos, early_output, 'dire_synergy', early_dict, min_matches_trio=early_trio_threshold)
    synergy_team(radiant_heroes_and_pos, mid_output, 'radiant_synergy', mid_dict, min_matches_trio=mid_trio_threshold)
    synergy_team(dire_heroes_and_pos, mid_output, 'dire_synergy', mid_dict, min_matches_trio=mid_trio_threshold)
    
    # Обработка comeback словаря
    if comeback_dict is not None:
        synergy_team(radiant_heroes_and_pos, comeback_output, 'radiant_synergy', comeback_dict, min_matches_trio=comeback_trio_threshold)
        synergy_team(dire_heroes_and_pos, comeback_output, 'dire_synergy', comeback_dict, min_matches_trio=comeback_trio_threshold)
    
    # Анализ контрпиков
    counterpick_team(radiant_heroes_and_pos, dire_heroes_and_pos, early_output, 'radiant_counterpick', early_dict, check_solo=True)
    counterpick_team(dire_heroes_and_pos, radiant_heroes_and_pos, early_output, 'dire_counterpick', early_dict, check_solo=True)
    counterpick_team(radiant_heroes_and_pos, dire_heroes_and_pos, mid_output, 'radiant_counterpick', mid_dict, check_solo=True)
    counterpick_team(dire_heroes_and_pos, radiant_heroes_and_pos, mid_output, 'dire_counterpick', mid_dict, check_solo=True)
    
    # Обработка comeback контрпиков
    if comeback_dict is not None:
        counterpick_team(radiant_heroes_and_pos, dire_heroes_and_pos, comeback_output, 'radiant_counterpick', comeback_dict, check_solo=True)
        counterpick_team(dire_heroes_and_pos, radiant_heroes_and_pos, comeback_output, 'dire_counterpick', comeback_dict, check_solo=True)
    # # Вычисление разниц с проверкой значимости
    outputs_to_process = [
        (early_output, 'early_output', early_dict, early_trio_threshold),
        (mid_output, 'mid_output', mid_dict, mid_trio_threshold),
    ]
    if comeback_dict is not None:
        outputs_to_process.append((comeback_output, 'comeback_output', comeback_dict, comeback_trio_threshold))
    
    for output, name, data_dict, trio_threshold in outputs_to_process:
        phase_bucket = return_dict.setdefault(name, {})
        phase_weights = early_weights if name == 'early_output' else late_weights
        # Требуем, чтобы у всех 10 героев были известны данные для соответствующей метрики
        has_all_solo = all_heroes_known and _covers_solo(data_dict, radiant_team) and _covers_solo(data_dict, dire_team)
        has_all_duo = all_heroes_known and _covers_duo(data_dict, radiant_team) and _covers_duo(data_dict, dire_team)
        has_all_trio = all_heroes_known and _covers_trio(data_dict, radiant_team, trio_threshold) and _covers_trio(data_dict, dire_team, trio_threshold)
        has_all_1vs1 = all_heroes_known and _covers_1vs1(data_dict, radiant_team, dire_team) and _covers_1vs1(data_dict, dire_team, radiant_team)
        has_all_1vs2 = all_heroes_known and _covers_1vs2(data_dict, radiant_team, dire_team) and _covers_1vs2(data_dict, dire_team, radiant_team)
        def _sum_games_list(items):
            total = 0
            if not items:
                return 0
            for it in items:
                if isinstance(it, (tuple, list)) and len(it) >= 2:
                    try:
                        total += int(it[1])
                    except Exception:
                        continue
            return total

        def _sum_games_dict(pos_dict):
            total = 0
            if not isinstance(pos_dict, dict):
                return 0
            for lst in pos_dict.values():
                total += _sum_games_list(lst)
            return total

        if has_all_1vs2 and all(f'{side}_counterpick_1vs2' in output for side in ['radiant', 'dire']):
            if (all(len(output['radiant_counterpick_1vs2'].get(p, [])) >= 1 for p in ['pos1', 'pos2', 'pos3']) and
                    all(len(output['dire_counterpick_1vs2'].get(p, [])) >= 1 for p in ['pos1', 'pos2', 'pos3'])):
                phase_bucket['counterpick_1vs2'] = get_diff(
                    output['radiant_counterpick_1vs2'],
                    output['dire_counterpick_1vs2'],
                    _1vs2=True,  # КРИТИЧНО: counterpick требует взвешивания по позициям!
                    custom_position_weights=phase_weights,  # веса позиций по фазе
                )
                # games
                r_games = _sum_games_dict(output.get('radiant_counterpick_1vs2'))
                d_games = _sum_games_dict(output.get('dire_counterpick_1vs2'))
                if r_games and d_games:
                    phase_bucket['counterpick_1vs2_games'] = min(r_games, d_games)
        def _has_min_core_positions(counterpick_dict, min_positions):
            if not isinstance(counterpick_dict, dict):
                return False
            core_positions = ('pos1', 'pos2', 'pos3')
            available = sum(1 for p in core_positions if counterpick_dict.get(p))
            return available >= min_positions

        def _has_full_core_1vs1(counterpick_dict):
            if not isinstance(counterpick_dict, dict):
                return False
            # Требуем полный набор 3x3 матчапов по корам
            for pos in CORE_POSITIONS:
                if len(counterpick_dict.get(pos, [])) < len(CORE_POSITIONS):
                    return False
            return True

        def _has_full_core_duo(synergy_list):
            return isinstance(synergy_list, list) and len(synergy_list) >= SYNERGY_DUO_CORES_REQUIRED

        if has_all_1vs1 and all(f'{side}_counterpick_1vs1' in output for side in ['radiant', 'dire']):
            if (
                _has_min_core_positions(output['radiant_counterpick_1vs1'], COUNTERPICK_1VS1_MIN_CORE_POSITIONS)
                and _has_min_core_positions(output['dire_counterpick_1vs1'], COUNTERPICK_1VS1_MIN_CORE_POSITIONS)
            ):
                cp_1vs1 = get_diff(
                    output['radiant_counterpick_1vs1'],
                    output['dire_counterpick_1vs1'],
                    _1vs2=True,
                    custom_position_weights=phase_weights,
                )
                if cp_1vs1 is not None and abs(cp_1vs1) >= COUNTERPICK_1VS1_MIN_ABS:
                    phase_bucket['counterpick_1vs1'] = cp_1vs1
                r_games = _sum_games_dict(output.get('radiant_counterpick_1vs1'))
                d_games = _sum_games_dict(output.get('dire_counterpick_1vs1'))
                if r_games and d_games:
                    phase_bucket['counterpick_1vs1_games'] = min(r_games, d_games)
        if has_all_solo and all(f'{side}_counterpick_solo' in output for side in ['radiant', 'dire']):
            # Для solo НЕ проверяем значимость (слишком мало данных)
            # ВНИМАНИЕ: solo теперь хранится по позициям и использует веса позиций
            phase_bucket['solo'] = get_diff(
                output['radiant_counterpick_solo'],
                output['dire_counterpick_solo'],
                _1vs2=True,
                custom_position_weights=phase_weights,
            )
            r_games = _sum_games_dict(output.get('radiant_counterpick_solo'))
            d_games = _sum_games_dict(output.get('dire_counterpick_solo'))
            if r_games and d_games:
                phase_bucket['solo_games'] = min(r_games, d_games)
        if has_all_trio and all(f'{side}_synergy_trio' in output for side in ['radiant', 'dire']):
            phase_bucket['synergy_trio'] = get_diff(
                output['radiant_synergy_trio'],
                output['dire_synergy_trio'],
            )
            r_games = _sum_games_list(output.get('radiant_synergy_trio'))
            d_games = _sum_games_list(output.get('dire_synergy_trio'))
            if r_games and d_games:
                phase_bucket['synergy_trio_games'] = min(r_games, d_games)


        synergy_duo_val = None
        r_games = d_games = 0
        if has_all_duo:
            cores_diff = None
            support_diff = None
            if all(f'{side}_synergy_cores_duo' in output for side in ['radiant', 'dire']):
                cores_diff = get_diff(
                    output['radiant_synergy_cores_duo'],
                    output['dire_synergy_cores_duo'],
                    use_max_for_synergy=synergy_duo_use_max,
                )
            if all(f'{side}_synergy_support_duo' in output for side in ['radiant', 'dire']):
                support_diff = get_diff(
                    output['radiant_synergy_support_duo'],
                    output['dire_synergy_support_duo'],
                    use_max_for_synergy=synergy_duo_use_max,
                )

            if cores_diff is not None or support_diff is not None:
                # Ставим упор на коры: поддержка часто шумит для предикта силы драфта
                synergy_duo_val = cores_diff if cores_diff is not None else support_diff
                if cores_diff is not None:
                    r_games = _sum_games_list(output.get('radiant_synergy_cores_duo'))
                    d_games = _sum_games_list(output.get('dire_synergy_cores_duo'))
                else:
                    r_games = _sum_games_list(output.get('radiant_synergy_support_duo'))
                    d_games = _sum_games_list(output.get('dire_synergy_support_duo'))
            elif all(f'{side}_synergy_duo' in output for side in ['radiant', 'dire']):
                synergy_duo_val = get_diff(
                    output['radiant_synergy_duo'],
                    output['dire_synergy_duo'],
                    use_max_for_synergy=synergy_duo_use_max,
                )
                r_games = _sum_games_list(output.get('radiant_synergy_duo'))
                d_games = _sum_games_list(output.get('dire_synergy_duo'))

        if not SYNERGY_DUO_REQUIRE_CP_ALIGN and synergy_duo_val is not None:
            phase_bucket['synergy_duo'] = synergy_duo_val
            if r_games and d_games:
                phase_bucket['synergy_duo_games'] = min(r_games, d_games)

        # Комбинированные сигналы:
        # 1) duo + 1vs1
        # 2) trio + 1vs2
        def _combine_if_aligned(a, b):
            """
            Возвращает среднее модулей с общим знаком, только если оба сигнала есть и одного знака.
            Иначе None – не смешиваем шумный/конфликтный сигнал.
            """
            if a is None or b is None:
                return None
            if a == 0 or b == 0:
                return None
            if (a > 0 and b > 0) or (a < 0 and b < 0):
                magnitude = (abs(a) + abs(b)) / 2
                sign = 1 if a > 0 else -1
                return sign * magnitude
            return None

        synergy_duo_val = phase_bucket.get('synergy_duo')
        pair_one = _combine_if_aligned(synergy_duo_val, phase_bucket.get('counterpick_1vs1'))
        if pair_one is not None and SYNERGY_DUO_REQUIRE_CP_ALIGN:
            # Усиливаем synergy_duo, когда она подтверждена counterpick_1vs1
            phase_bucket['synergy_duo'] = round(pair_one)
    return return_dict


# functions.py



# def proceed_map(radiant_heroes_and_pos, dire_heroes_and_pos, over40_data, synergy_data, lane_data,
#                 data_1vs2, data_1vs1, data_1vs3, synergy4, radiant_team_name=None, dire_team_name=None,
#                 url=None):
#     output_dict = {'kills_mediana': None, 'time_mediana': None, 'kills_average': None, 'time_average': None,
#                    'over40_duo': (calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos, over40_data))[0],
#                    'over40_duo_counterpick':
#                        (calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos, over40_data))[1],
#                    'over40_1vs2': (calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos, over40_data))[2],
#                    'over40_solo': (calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos, over40_data))[3],
#                    'over40_duo_synergy': (calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos, over40_data))[4],
#                    'over40_trio': (calculate_over40(radiant_heroes_and_pos, dire_heroes_and_pos, over40_data))[5],
#                    'top_message': (calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, lane_data))[0],
#                    'bot_message': (calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, lane_data))[1],
#                    'mid_message': (calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, lane_data))[2],
#                    'synergy_duo': (synergy_and_counterpick_new(radiant_heroes_and_pos=radiant_heroes_and_pos,
#                                                                dire_heroes_and_pos=dire_heroes_and_pos,
#                                                                synergy_data=synergy_data, data_1vs2=data_1vs2,
#                                                                data_1vs1=data_1vs1, data_1vs3=data_1vs3))[0],
#                    'radiant_synergy_trio': (synergy_and_counterpick_new(radiant_heroes_and_pos=radiant_heroes_and_pos,
#                                                                         dire_heroes_and_pos=dire_heroes_and_pos,
#                                                                         synergy_data=synergy_data, data_1vs2=data_1vs2,
#                                                                         data_1vs1=data_1vs1, data_1vs3=data_1vs3))[1],
#                    'duo_diff': (synergy_and_counterpick_new(radiant_heroes_and_pos=radiant_heroes_and_pos,
#                                                             dire_heroes_and_pos=dire_heroes_and_pos,
#                                                             synergy_data=synergy_data, data_1vs2=data_1vs2,
#                                                             data_1vs1=data_1vs1, data_1vs3=data_1vs3))[2],
#                    'radiant_counterpick_1vs2':
#                        (synergy_and_counterpick_new(radiant_heroes_and_pos=radiant_heroes_and_pos,
#                                                     dire_heroes_and_pos=dire_heroes_and_pos,
#                                                     synergy_data=synergy_data, data_1vs2=data_1vs2,
#                                                     data_1vs1=data_1vs1, data_1vs3=data_1vs3))[3],
#                    'pos1_matchup': (synergy_and_counterpick_new(radiant_heroes_and_pos=radiant_heroes_and_pos,
#                                                                 dire_heroes_and_pos=dire_heroes_and_pos,
#                                                                 synergy_data=synergy_data, data_1vs2=data_1vs2,
#                                                                 data_1vs1=data_1vs1, data_1vs3=data_1vs3))[4],
#                    'support_dif': (synergy_and_counterpick_new(radiant_heroes_and_pos=radiant_heroes_and_pos,
#                                                                dire_heroes_and_pos=dire_heroes_and_pos,
#                                                                synergy_data=synergy_data, data_1vs2=data_1vs2,
#                                                                data_1vs1=data_1vs1, data_1vs3=data_1vs3))[5]}
#     # if radiant_team_name is not None:
#     #     answer = \
#     #         tm_kills_teams(radiant_heroes_and_pos=radiant_heroes_and_pos,
#     #                            dire_heroes_and_pos=dire_heroes_and_pos,
#     #                            radiant_team_name=radiant_team_name,
#     #                            dire_team_name=dire_team_name, min_len=2)
#     #     if answer is not None:
#     #         output_dict['kills_mediana'], output_dict['time_mediana'], output_dict['kills_average'],\
#     #             output_dict['time_average'] = answer
#     #     else:
#     #         output_dict['kills_mediana'], output_dict['time_mediana'], output_dict['kills_average'],\
#     #             output_dict['time_average'] = None, None, None, None
#
#     return output_dict

def check_barracks_status(match):
    """
    Проверяет состояние критических структур (T3 башни) на трёх стадиях.

    Стадии:
    - early: на 27 минуте (индекс [26] в radiantNetworthLeads)
    - snowball_check: на 32-34 минутах (для проверки сноубола)
    - mid: на 50 минуте (индекс [49]), либо на последней доступной минуте в диапазоне 32-50

    Returns: dict с доминацией для каждой стадии
        {
            'early': {'radiant_domination': bool, 'dire_domination': bool, 'radiant_mega': bool, 'dire_mega': bool} или None,
            'snowball_check': {...} или None,
            'mid': {...} или None
        }
    """
    # T3 башни (3 штуки на сторону)
    RADIANT_T3 = {22, 23, 24}  # top, mid, bot
    DIRE_T3 = {32, 33, 34}  # top, mid, bot

    # Получаем длительность игры
    radiant_networth = match.get('radiantNetworthLeads', [])
    game_duration_minutes = len(radiant_networth)

    td = match.get('towerDeaths') or []

    # Результаты для каждой стадии
    results = {
        'early': None,
        'snowball_check': None,
        'mid': None,
    }

    # Функция для подсчета башен до определенного времени (в секундах)
    def count_towers_until(max_time_seconds):
        radiant_t3_destroyed = set()
        dire_t3_destroyed = set()

        for ev in td:
            npc_id = ev.get('npcId')
            is_radiant = ev.get('isRadiant')
            time_seconds = ev.get('time')

            # Учитываем только события ДО max_time_seconds
            if time_seconds is None or time_seconds >= max_time_seconds:
                continue

            if is_radiant is True and npc_id in RADIANT_T3:
                radiant_t3_destroyed.add(npc_id)
            elif is_radiant is False and npc_id in DIRE_T3:
                dire_t3_destroyed.add(npc_id)

        radiant_t3_lost = len(radiant_t3_destroyed)
        dire_t3_lost = len(dire_t3_destroyed)

        return {
            'radiant_domination': dire_t3_lost >= 2,
            'dire_domination': radiant_t3_lost >= 2,
            'dire_made_megas_to_radiant': radiant_t3_lost == 3,
            'radiant_made_megas_to_dire': dire_t3_lost == 3,
        }

    # Early стадия: на 27 минуте (если есть)
    EARLY_MINUTE = 29
    if game_duration_minutes >= EARLY_MINUTE:
        results['early'] = count_towers_until(EARLY_MINUTE * 60)

    return results


def determine_game_dominance(match):
    """
    Определяет доминирующую команду на early и mid стадиях игры.

    Args:
        match: словарь с данными матча, должен содержать:
            - radiantNetworthLeads: список преимущества по нетворту
            - towerDeaths: данные о разрушенных башнях (опционально)

    Returns:
        dict: {
            'first_dominator': 'radiant'/'dire'/None - доминатор early фазы,
            'mid_dominator': 'radiant'/'dire'/None - доминатор mid фазы
        }
    """
    networth_leads = match.get('radiantNetworthLeads', [])

    # Проверяем статус бараков
    barracks_result = check_barracks_status(match)

    early_radiant_domination = None
    early_dire_domination = None
    first_dominator = None
    if barracks_result['early'] is not None:
        if barracks_result['early']['radiant_domination']:
            first_dominator = 'radiant'
        if barracks_result['early']['dire_domination']:
            first_dominator = 'dire'

    # Определяем early dominator

    if len(networth_leads) >= 25:
        threshold_early = 10000  # Оптимизировано через эксперименты (см. EXPERIMENT_REPORT.md)
        for idx in range(19, min(29, len(networth_leads))):
            lead = networth_leads[idx]
            if lead >= threshold_early:
                first_dominator = 'radiant'
                break
            if lead <= -threshold_early:
                first_dominator = 'dire'
                break

        if first_dominator is None and barracks_result['early'] is not None:
            if early_radiant_domination:
                first_dominator = 'radiant'
            elif early_dire_domination:
                first_dominator = 'dire'


    return {
        'first_dominator': first_dominator,
    }


def one_match(radiant_heroes_and_pos, dire_heroes_and_pos, lane_data, early_dict, late_dict,
              radiant_team_name=None, dire_team_name=None, match=None, comeback_dict=None):

    for key in dire_heroes_and_pos:
        hero_name = dire_heroes_and_pos[key]['hero_name'].lower()
        if hero_name in name_to_id:
            dire_heroes_and_pos[key]['hero_id'] = name_to_id[hero_name]
        else:
            send_message(f'Error handling name {hero_name}')
            return
    for key in radiant_heroes_and_pos:
        hero_name = radiant_heroes_and_pos[key]['hero_name'].lower()
        if hero_name in name_to_id:
            radiant_heroes_and_pos[key]['hero_id'] = name_to_id[hero_name]
        else:
            send_message(f'Error handling name {hero_name}')
            return
    if match is not None:
        with open('one_match.json', encoding='utf-8') as f:
            one_match = json.load(f)
        for map_id, _ in one_match.items():
            for player in one_match[map_id]['players']:
                hero_id = player.get('hero', {}).get('id')
                position = player.get('position')
                is_radiant = player.get('isRadiant')
                position_key = f'pos{position[-1]}'  # POSITION_1 -> pos1
                if is_radiant:
                    radiant_heroes_and_pos.setdefault(position_key, {}).setdefault('hero_id', hero_id)
                else:
                    dire_heroes_and_pos.setdefault(position_key, {}).setdefault('hero_id', hero_id)
    # Подготовка lane_data (структурируем один раз)
    structured_lane_data = lane_data
    if isinstance(lane_data, dict) and '2v2_lanes' not in lane_data:
        structured_lane_data = structure_lane_dict(lane_data)

    s = synergy_and_counterpick(
        radiant_heroes_and_pos=radiant_heroes_and_pos,
        dire_heroes_and_pos=dire_heroes_and_pos,
        early_dict=early_dict, mid_dict=late_dict, comeback_dict=comeback_dict)
    base_top, base_bot, base_mid = calculate_lanes(
        radiant_heroes_and_pos, dire_heroes_and_pos, structured_lane_data
    )

    # ML корректировка (Rule B) если есть данные игроков
    lane_corrector_enabled = True
    env_lc = os.getenv("LANE_CORRECTOR")
    if env_lc is not None:
        lane_corrector_enabled = env_lc.strip().lower() not in ("0", "false", "off", "no")
    lane_corrector_enabled = lane_corrector_enabled and bool(_lc_load_models().get("models"))

    match_obj = None
    if isinstance(match, dict) and match.get("players"):
        match_obj = match
    elif match is not None:
        try:
            with open('one_match.json', encoding='utf-8') as f:
                one_match_data = json.load(f)
            if isinstance(one_match_data, dict):
                for _, m in one_match_data.items():
                    if isinstance(m, dict) and m.get("players"):
                        match_obj = m
                        break
        except Exception:
            match_obj = None

    if lane_corrector_enabled and match_obj:
        baseline_msgs = {"top": base_top, "bot": base_bot, "mid": base_mid}
        lc_res = _lc_predict_lanes_for_match(
            match=match_obj,
            radiant_heroes_and_pos=radiant_heroes_and_pos,
            dire_heroes_and_pos=dire_heroes_and_pos,
            heroes_data=structured_lane_data,
            baseline_messages=baseline_msgs,
            player_stats={},
            pair_stats={},
            pair_hero_stats={},
            team_lane_history={},
        )
        if lc_res:
            corrected, _ = lc_res
            base_top = _lc_format_lane_message("top", *corrected.get("top", (None, None)))
            base_mid = _lc_format_lane_message("mid", *corrected.get("mid", (None, None)))
            base_bot = _lc_format_lane_message("bot", *corrected.get("bot", (None, None)))

    s['top'], s['bot'], s['mid'] = base_top, base_bot, base_mid

    # if format_output_dict(s):
    if True:
        def _format_metrics(title, data, metrics):
            lines = [title]
            for key, label in metrics:
                lines.append(f"{label}: {data.get(key)}")
            return "\n".join(lines) + "\n"

        def _has_any_metric(data):
            return any(value is not None for value in data.values()) if isinstance(data, dict) else False

        metric_list = [
            ('counterpick_1vs1', 'Counterpick_1vs1'),
            ('counterpick_1vs2', 'Counterpick_1vs2'),
            ('solo', 'Solo'),
            ('synergy_duo', 'Synergy_duo'),
            ('synergy_trio', 'Synergy_trio'),
        ]

        early_output = s.get('early_output', {})
        mid_output = s.get('mid_output', {})
        comeback_output = s.get('comeback_output', {})

        early_block = _format_metrics("10-28 Minute:", early_output, metric_list)
        mid_block = _format_metrics("Mid (25-50 min):", mid_output, metric_list)
        comeback_block = ""
        if _has_any_metric(comeback_output):
            comeback_block = _format_metrics("Comeback:", comeback_output, metric_list)

        # Формирование сообщения
        send_message(
            f'ПОМНИ: КОМАНДА ВАЖНЕЕ ПИКА\n'
            f"{radiant_team_name} VS {dire_team_name}\n"
            f"Lanes:\n{s.get('top')}{s.get('mid')}{s.get('bot')}"
            f"{early_block}"
            f"{mid_block}"
            f"{comeback_block}"
            f'ПОМНИ: КОМАНДА ВАЖНЕЕ ПИКА')


def normalize_weights(weights_dict):
    """
    Нормализует веса, деля на минимальный вес.
    Это позволяет выявить эквивалентные комбинации.

    Например: {pos1:2.0, pos2:2.0, pos3:1.8, pos4:1.2, pos5:1.2}
           -> {pos1:1.67, pos2:1.67, pos3:1.5, pos4:1.0, pos5:1.0}
    """
    min_weight = min(weights_dict.values())
    if min_weight == 0:
        min_weight = 0.1  # Защита от деления на 0

    return {pos: round(w / min_weight, 2) for pos, w in weights_dict.items()}


def remove_duplicate_combinations(combinations, positions):
    """
    Удаляет эквивалентные комбинации весов.
    Две комбинации эквивалентны, если их нормализованные веса одинаковы.

    Args:
        combinations: список кортежей весов
        positions: список названий позиций ['pos1', 'pos2', ...]

    Returns:
        unique_combinations: список уникальных комбинаций
        original_count: исходное количество
    """
    original_count = len(combinations)

    seen_normalized = set()
    unique_combinations = []

    for weights_tuple in combinations:
        weights_dict = dict(zip(positions, weights_tuple, strict=False))

        # Нормализуем веса
        normalized = normalize_weights(weights_dict)

        # Создаем хеш из нормализованных весов
        normalized_tuple = tuple(normalized[pos] for pos in positions)

        if normalized_tuple not in seen_normalized:
            seen_normalized.add(normalized_tuple)
            unique_combinations.append(weights_tuple)

    return unique_combinations, original_count


def evaluate_winrate_check_old_maps(matches):
    """
    Оценивает винрейт метрик counterpick для early и mid фаз.
    Аналогично evaluate_winrate из optimize_weights_simple.py
    """
    if isinstance(matches, dict):
        matches = list(matches.values())
    winrates_by_index = {}

    for index in range(10, 26):
        metrics_stats = {
            'early_counterpick_1vs2': {'win': 0, 'lose': 0},
            'early_counterpick_1vs1': {'win': 0, 'lose': 0},
            'mid_counterpick_1vs2': {'win': 0, 'lose': 0},
            'mid_counterpick_1vs1': {'win': 0, 'lose': 0},
        }

        for match in matches:
            result = check_barracks_status(match)
            radiant_networth = match.get('radiantNetworthLeads', [])

            # Определяем early dominator (аналогично metrics_winrate.py)
            first_dominator = None
            if len(radiant_networth) >= 29:
                threshold_early = 5500
                for idx in range(19, min(29, len(radiant_networth))):
                    lead = radiant_networth[idx]
                    if lead >= threshold_early:
                        first_dominator = 'radiant'
                        break
                    if lead <= -threshold_early:
                        first_dominator = 'dire'
                        break

                if first_dominator is None and result['early'] is not None:
                    if result['early']['radiant_domination']:
                        first_dominator = 'radiant'
                    elif result['early']['dire_domination']:
                        first_dominator = 'dire'

            # Early phase
            if first_dominator is not None:
                early_output = match.get('early_output', {})

                if first_dominator == 'dire':
                    for metric in ['counterpick_1vs2', 'counterpick_1vs1']:
                        val = early_output.get(metric)
                        if val == -index:
                            metrics_stats[f'early_{metric}']['win'] += 1
                        elif val == index:
                            metrics_stats[f'early_{metric}']['lose'] += 1

                elif first_dominator == 'radiant':
                    for metric in ['counterpick_1vs2', 'counterpick_1vs1']:
                        val = early_output.get(metric)
                        if val == index:
                            metrics_stats[f'early_{metric}']['win'] += 1
                        elif val == -index:
                            metrics_stats[f'early_{metric}']['lose'] += 1

            # Определяем mid dominator (аналогично metrics_winrate.py)
            mid_dominator = None
            if len(radiant_networth) >= 32:
                threshold_mid = 5000

                # Проверка сноубола
                is_snowball = False
                snowball_radiant_domination = result.get('snowball_check', {}).get('radiant_domination') if result.get('snowball_check') else None
                snowball_dire_domination = result.get('snowball_check', {}).get('dire_domination') if result.get('snowball_check') else None

                if first_dominator is not None and len(radiant_networth) >= 33:
                    for check_idx in range(28, min(33, len(radiant_networth))):
                        lead_at_check = radiant_networth[check_idx]
                        if (first_dominator == 'radiant' and lead_at_check >= 10000) or (first_dominator == 'dire' and lead_at_check <= -10000):
                            is_snowball = True
                            break

                if not is_snowball and first_dominator is not None and snowball_radiant_domination is not None:
                    if (first_dominator == 'radiant' and snowball_radiant_domination) or (first_dominator == 'dire' and snowball_dire_domination):
                        is_snowball = True

                # Если не сноубол, определяем mid_dominator
                if not is_snowball:
                    mid_range_end = min(51, len(radiant_networth))
                    if mid_range_end > 24:
                        final_minute_value = radiant_networth[mid_range_end - 1]
                        mid_radiant_domination = result.get('mid', {}).get('radiant_domination') if result.get('mid') else None
                        mid_dire_domination = result.get('mid', {}).get('dire_domination') if result.get('mid') else None
                        mid_dire_megas = result.get('mid', {}).get('dire_mega') if result.get('mid') else None
                        mid_radiant_megas = result.get('mid', {}).get('radiant_mega') if result.get('mid') else None

                        if final_minute_value >= threshold_mid or (mid_radiant_domination and final_minute_value >= 0) or mid_dire_megas:
                            mid_dominator = 'radiant'
                        elif final_minute_value <= -threshold_mid or (mid_dire_domination and final_minute_value <= 0) or mid_radiant_megas:
                            mid_dominator = 'dire'

            # Mid phase
            if mid_dominator is not None and len(radiant_networth) >= 41:
                mid_output = match.get('mid_output', {})

                if mid_dominator == 'dire':
                    for metric in ['counterpick_1vs2', 'counterpick_1vs1']:
                        val = mid_output.get(metric)
                        if val == -index:
                            metrics_stats[f'mid_{metric}']['win'] += 1
                        elif val == index:
                            metrics_stats[f'mid_{metric}']['lose'] += 1

                elif mid_dominator == 'radiant':
                    for metric in ['counterpick_1vs2', 'counterpick_1vs1']:
                        val = mid_output.get(metric)
                        if val == index:
                            metrics_stats[f'mid_{metric}']['win'] += 1
                        elif val == -index:
                            metrics_stats[f'mid_{metric}']['lose'] += 1

        # Сохраняем винрейты для этого индекса
        for metric, stats in metrics_stats.items():
            total = stats['win'] + stats['lose']
            if total > 6:
                wr = stats['win'] / total * 100
                winrates_by_index.setdefault(metric, {})[index] = wr

    # Считаем средний винрейт
    all_wr = []
    for metric_wrs in winrates_by_index.values():
        all_wr.extend(metric_wrs.values())

    avg_wr = sum(all_wr) / len(all_wr) if all_wr else 50.0
    return avg_wr, winrates_by_index


def check_old_maps(early_dict, late_dict, lane_data, outfile_name, custom_weights=None, write_to_file=True, start_date_time=1747872000, comeback_dict=None, maps_path=None, output_path=None, merge_side_lanes: bool = False, disable_lanes: bool = False, max_matches: int = None, autoload_dicts: bool = True, use_lane_corrector: bool = True, lane_corrector_dir: str = None):
    import sys
    import time
    start_time = time.time()
    print("\n" + "="*80, flush=True)
    print("CHECK_OLD_MAPS: Начало обработки", flush=True)
    print("="*80, flush=True)
    if maps_path is None:
        maps_path = '/Users/alex/Documents/ingame/pro_heroes_data/json_parts_split_from_object/combined1.json'
    with open(maps_path) as f:
        maps_data = json.load(f)
    
    total_matches = len(maps_data)
    print(f"Загружено матчей: {total_matches:,}", flush=True)
    print(f"Путь к файлу: {maps_path}", flush=True)
    if start_date_time:
        print(f"Фильтр по дате: >= {start_date_time} (22 мая 2025)", flush=True)
    print(flush=True)
    
    # Если словари не переданы, грузим дефолтные из stats
    if autoload_dicts:
        try:
            from pathlib import Path
            stats_dir = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches")
            if (not early_dict) and (stats_dir / "early_dict_raw.json").exists():
                with open(stats_dir / "early_dict_raw.json", "r") as f:
                    early_dict = json.load(f)
                print("  ✓ Загружен early_dict по умолчанию")
            if (not late_dict) and (stats_dir / "late_dict_raw.json").exists():
                with open(stats_dir / "late_dict_raw.json", "r") as f:
                    late_dict = json.load(f)
                print("  ✓ Загружен late_dict по умолчанию")
            if comeback_dict is None and (stats_dir / "comeback_dict_raw.json").exists():
                with open(stats_dir / "comeback_dict_raw.json", "r") as f:
                    comeback_dict = json.load(f)
                print("  ✓ Загружен comeback_dict по умолчанию")
            if (not lane_data) and (stats_dir / "lane_dict_raw.json").exists():
                with open(stats_dir / "lane_dict_raw.json", "r") as f:
                    lane_data = json.load(f)
                print("  ✓ Загружен lane_dict по умолчанию")
        except Exception as e:
            print(f"⚠️ Не удалось автозагрузить словари: {e}")
    
    # Подготовка lane_data: структуру строим один раз, чтобы не тратить время в каждом матче
    structured_lane_data = lane_data
    if isinstance(lane_data, dict) and '2v2_lanes' not in lane_data:
        structured_lane_data = structure_lane_dict(lane_data)

    output = {}
    processed = 0
    skipped = 0

    lane_corrector_enabled = bool(use_lane_corrector) and not disable_lanes
    env_lc = os.getenv("LANE_CORRECTOR")
    if env_lc is not None:
        lane_corrector_enabled = env_lc.strip().lower() not in ("0", "false", "off", "no")
    lane_corrector_enabled = lane_corrector_enabled and bool(_lc_load_models(models_dir=lane_corrector_dir).get("models"))
    player_stats = {} if lane_corrector_enabled else None
    pair_stats = {} if lane_corrector_enabled else None
    pair_hero_stats = {} if lane_corrector_enabled else None
    team_lane_history = {} if lane_corrector_enabled else None

    items = list(maps_data.items())
    if lane_corrector_enabled:
        items.sort(key=lambda kv: (_lc_coerce_int(kv[1].get("startDateTime")), _lc_coerce_int(kv[1].get("id"))))
        warmup_path = (os.getenv("LANE_CORRECTOR_WARMUP_MAPS_PATH") or "").strip()
        if warmup_path:
            try:
                with open(warmup_path, "r") as wf:
                    warm_raw = json.load(wf)
                if isinstance(warm_raw, dict):
                    warm_items = list(warm_raw.items())
                elif isinstance(warm_raw, list):
                    warm_items = [(str(_lc_coerce_int(m.get("id"))), m) for m in warm_raw if isinstance(m, dict)]
                else:
                    warm_items = []
                first_main_ts = 0
                if items:
                    first_main_ts = min(_lc_coerce_int(v.get("startDateTime")) for _, v in items)
                warm_items.sort(key=lambda kv: (_lc_coerce_int(kv[1].get("startDateTime")), _lc_coerce_int(kv[1].get("id"))))
                warmed = 0
                for _, warm_match in warm_items:
                    warm_ts = _lc_coerce_int(warm_match.get("startDateTime"))
                    if first_main_ts > 0 and warm_ts >= first_main_ts:
                        continue
                    warm_bad = check_bad_map(match=warm_match, start_date_time=0)
                    if warm_bad is None:
                        continue
                    warm_radiant, warm_dire = warm_bad
                    warm_top, warm_bot, warm_mid = calculate_lanes(
                        radiant_heroes_and_pos=warm_radiant,
                        dire_heroes_and_pos=warm_dire,
                        heroes_data=structured_lane_data,
                        merge_side_lanes=merge_side_lanes,
                    )
                    warm_base_preds = {
                        "top": _lc_parse_lane_prediction(warm_top),
                        "mid": _lc_parse_lane_prediction(warm_mid),
                        "bot": _lc_parse_lane_prediction(warm_bot),
                    }
                    _lc_update_stats_after_match(
                        warm_match,
                        warm_base_preds,
                        player_stats,
                        pair_stats,
                        pair_hero_stats,
                        team_lane_history,
                    )
                    warmed += 1
                print(f"  ✓ Lane corrector warmup: {warmed} матчей из {len(warm_items)}", flush=True)
            except Exception as warm_exc:
                print(f"  ⚠️ Lane corrector warmup skipped: {warm_exc}", flush=True)
    total_matches = len(items)
    
    for idx, (match_id, match) in enumerate(items, 1):
        if max_matches is not None and idx > int(max_matches):
            break
        # Показываем прогресс каждые 1000 матчей или на важных этапах
        if idx % 1000 == 0 or idx == 1 or idx == total_matches:
            elapsed = time.time() - start_time
            rate = idx / elapsed if elapsed > 0 else 0
            eta = (total_matches - idx) / rate if rate > 0 else 0
            percent = (idx / total_matches) * 100
            print(f"  [{idx:>6}/{total_matches}] ({percent:>5.1f}%) | Обработано: {processed:>5} | Пропущено: {skipped:>5} | {rate:.1f} м/с | ETA: {eta/60:.1f} мин", flush=True)
        
        result = check_bad_map(match=match, maps_data=maps_data, start_date_time=start_date_time)
        if result is None:
            skipped += 1
            continue

        # Проверка валидности результата
        if not isinstance(result, tuple) or len(result) != 2:
            print(f"ОШИБКА: check_bad_map вернул неожиданный результат: {type(result)} = {result}")
            skipped += 1
            continue

        radiant_heroes_and_pos, dire_heroes_and_pos = result
        
        # Дополнительная проверка валидности данных
        if not isinstance(radiant_heroes_and_pos, dict) or not isinstance(dire_heroes_and_pos, dict):
            print(f"ОШИБКА: heroes_and_pos не является словарем: radiant={type(radiant_heroes_and_pos)}, dire={type(dire_heroes_and_pos)}")
            skipped += 1
            continue
        s = synergy_and_counterpick(
            radiant_heroes_and_pos=radiant_heroes_and_pos,
            dire_heroes_and_pos=dire_heroes_and_pos,
            early_dict=early_dict,
            mid_dict=late_dict,
            comeback_dict=comeback_dict,
            custom_weights=custom_weights,
        ) or {}
        # Совместимость: старые пайплайны ожидают late_output
        if 'mid_output' in s and 'late_output' not in s:
            s['late_output'] = s['mid_output']
        # Удаляем метрики *_games для check_old_maps
        def _strip_games_metrics(bucket):
            if not isinstance(bucket, dict):
                return
            for k in list(bucket.keys()):
                if k.endswith('_games'):
                    bucket.pop(k, None)
        _strip_games_metrics(s.get('early_output'))
        _strip_games_metrics(s.get('mid_output'))
        _strip_games_metrics(s.get('late_output'))
        _strip_games_metrics(s.get('comeback_output'))
        if not disable_lanes:
            base_top, base_bot, base_mid = calculate_lanes(
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos,
                heroes_data=structured_lane_data,
                merge_side_lanes=merge_side_lanes,
            )
            if lane_corrector_enabled:
                baseline_msgs = {"top": base_top, "bot": base_bot, "mid": base_mid}
                lc_res = _lc_predict_lanes_for_match(
                    match=match,
                    radiant_heroes_and_pos=radiant_heroes_and_pos,
                    dire_heroes_and_pos=dire_heroes_and_pos,
                    heroes_data=structured_lane_data,
                    baseline_messages=baseline_msgs,
                    player_stats=player_stats,
                    pair_stats=pair_stats,
                    pair_hero_stats=pair_hero_stats,
                    team_lane_history=team_lane_history,
                    models_dir=lane_corrector_dir,
                )
                if lc_res:
                    corrected, base_preds = lc_res
                    base_top = _lc_format_lane_message("top", *corrected.get("top", (None, None)))
                    base_mid = _lc_format_lane_message("mid", *corrected.get("mid", (None, None)))
                    base_bot = _lc_format_lane_message("bot", *corrected.get("bot", (None, None)))
                    _lc_update_stats_after_match(
                        match,
                        base_preds,
                        player_stats,
                        pair_stats,
                        pair_hero_stats,
                        team_lane_history,
                    )
            s['top'], s['bot'], s['mid'] = base_top, base_bot, base_mid
        s['radiantTeam'] = match.get('radiantTeam')
        s['direTeam'] = match.get('direTeam')
        s['didRadiantWin'] = maps_data[match_id]['didRadiantWin']
        s['radiantNetworthLeads'] = maps_data[match_id]['radiantNetworthLeads']
        s['winRates'] = maps_data[match_id].get('winRates', [])
        s['startDateTime'] = maps_data[match_id].get('startDateTime')
        s['bottomLaneOutcome'] = maps_data[match_id].get('bottomLaneOutcome')
        s['topLaneOutcome'] = maps_data[match_id].get('topLaneOutcome')
        s['midLaneOutcome'] = maps_data[match_id].get('midLaneOutcome')
        s['towerdeaths'] = maps_data[match_id].get('towerDeaths')
        s['players'] = maps_data[match_id].get('players')
        output[int(match_id)] = s
        processed += 1
    
    print(flush=True)  # Новая строка после прогресса
    print("\n" + "="*80, flush=True)
    print("РЕЗУЛЬТАТЫ:", flush=True)
    print("="*80, flush=True)
    total_time = time.time() - start_time
    print(f"Всего матчей:      {total_matches:>6,}", flush=True)
    print(f"Обработано:        {processed:>6,} ({processed/total_matches*100:.1f}%)", flush=True)
    print(f"Пропущено:         {skipped:>6,} ({skipped/total_matches*100:.1f}%)", flush=True)
    print(f"Время выполнения:  {total_time/60:.1f} мин ({total_time:.0f} сек)", flush=True)
    print(f"Скорость:          {total_matches/total_time:.1f} матчей/сек", flush=True)
    print("="*80 + "\n", flush=True)
    
    if write_to_file:
        out_path = output_path or os.getenv('PRO_NEW_OUTPUT') or f'/Users/alex/Documents/ingame/pro_heroes_data/{outfile_name}.txt'
        print(f"Сохранение результатов: {out_path}")
        with open(out_path, 'w') as f:
            json.dump(output, f)
        print('✅ old_maps успешно завершен\n')
    
    return output




def check_old_maps_weights(early_dict, mid_dict, lane_data, custom_weights=None, comeback_dict=None):
    """
    Оптимизация весов позиций для метрик counterpick_1vs2 и counterpick_1vs1.
    Перебирает комбинации весов и находит лучшую по винрейту.
    """
    import itertools

    print("\n" + "="*70)
    print("ОПТИМИЗАЦИЯ ВЕСОВ ДЛЯ check_old_maps")
    print("="*70 + "\n")

    weight_ranges = {
        'pos1': [2.0, 1.8, 1.6, 1.4, 1.2, 1.0],
        'pos2': [2.0, 1.8, 1.6, 1.4, 1.2, 1.0],
        'pos3': [1.8, 1.6, 1.4, 1.2, 1.0],
        'pos4': [1.2, 1.0],
        'pos5': [1.0, 1.2],
    }

    positions = ['pos1', 'pos2', 'pos3', 'pos4', 'pos5']
    all_combinations = list(itertools.product(*[weight_ranges[pos] for pos in positions]))

    print(f"📊 Сгенерировано комбинаций: {len(all_combinations):,}")

    # Удаляем дубликаты
    print("🔍 Удаление эквивалентных комбинаций...")
    combinations, original_count = remove_duplicate_combinations(all_combinations, positions)

    removed = original_count - len(combinations)
    percent_removed = (removed / original_count * 100) if original_count > 0 else 0

    print(f"✅ Удалено дубликатов: {removed:,} ({percent_removed:.1f}%)")
    print(f"✅ Уникальных комбинаций для тестирования: {len(combinations):,}\n")

    # Загружаем данные один раз
    with open('count_synergy_10th_2000/json_parts_split_from_object/pro_output.json') as f:
        maps_data = json.load(f)

    results = []
    best_wr = 0
    best_weights = None

    start_time = time.time()

    for idx, weights_tuple in enumerate(combinations, 1):
        current_weights = dict(zip(positions, weights_tuple, strict=False))

        print(f"\n{'='*70}")
        print(f"Тест {idx}/{len(combinations)}: {current_weights}")
        print(f"{'='*70}")

        output = []

        # Обрабатываем все матчи с текущими весами
        for counter, match_id in enumerate(maps_data):
            if counter % 50 == 0:
                print(f"   {counter}/{len(maps_data)} ({counter*100//len(maps_data)}%)", end='\r')

            result = check_bad_map(match=match_id, maps_data=maps_data)
            if result is None:
                continue

            radiant_heroes_and_pos, dire_heroes_and_pos = result
            s = synergy_and_counterpick(
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos,
                early_dict=early_dict,
                mid_dict=mid_dict,
                custom_weights=current_weights,
                comeback_dict=comeback_dict,
            )

            s['didRadiantWin'] = maps_data[match_id]['didRadiantWin']
            s['radiantNetworthLeads'] = maps_data[match_id]['radiantNetworthLeads']
            s['id'] = int(match_id)
            s['bottomLaneOutcome'] = maps_data[match_id].get('bottomLaneOutcome')
            s['topLaneOutcome'] = maps_data[match_id].get('topLaneOutcome')
            s['midLaneOutcome'] = maps_data[match_id].get('midLaneOutcome')
            output.append(s)

        print(f"\n   ✓ Обработано {len(output)} матчей")

        # Оцениваем винрейт
        avg_wr, detailed = evaluate_winrate_check_old_maps(output)

        results.append({
            'weights': current_weights.copy(),
            'avg_winrate': avg_wr,
            'detailed': detailed,
        })

        print(f"   📊 Средний винрейт: {avg_wr:.2f}%")

        if avg_wr > best_wr:
            best_wr = avg_wr
            best_weights = current_weights.copy()
            print("   🎉 НОВЫЙ ЛУЧШИЙ РЕЗУЛЬТАТ!")

        elapsed = time.time() - start_time
        eta = (elapsed / idx) * (len(combinations) - idx) / 60
        print(f"   ⏱️  ETA: {eta:.1f} мин")

    # Финальные результаты
    print(f"\n{'='*70}")
    print("✅ ОПТИМИЗАЦИЯ ЗАВЕРШЕНА")
    print(f"{'='*70}")
    print(f"🏆 Лучшие веса: {best_weights}")
    print(f"📈 Лучший винрейт: {best_wr:.2f}%")
    print(f"⏱️  Общее время: {(time.time()-start_time)/60:.1f} мин\n")

    # Сохраняем результаты
    output_file = 'check_old_maps_optimized_weights.json'
    with open(output_file, 'w') as f:
        json.dump({
            'best_weights': best_weights,
            'best_winrate': best_wr,
            'all_results': results,
        }, f, indent=2)

    print(f"💾 Результаты сохранены в {output_file}\n")

    # Топ-5
    sorted_results = sorted(results, key=lambda x: x['avg_winrate'], reverse=True)
    print(f"{'='*70}")
    print("🏆 ТОП-5 ЛУЧШИХ КОМБИНАЦИЙ")
    print(f"{'='*70}")
    for i, r in enumerate(sorted_results[:5], 1):
        print(f"\n{i}. Винрейт: {r['avg_winrate']:.2f}%")
        print(f"   Веса: {r['weights']}")

    return best_weights, best_wr


def synergy_over40(heroes_and_positions, data, output, mkdir):
    unique_combinations = set()
    for pos in heroes_and_positions:
        hero_id = str(heroes_and_positions[pos]['hero_id'])
        key = f"{hero_id + pos}"
        foo = data.get(key, {})
        if len(foo) >= 15:
            value = foo.count(1) / (foo.count(1) + foo.count(0))
            output.setdefault(f'{mkdir}_over40_solo', {}).setdefault(pos, []).append(value)
        for second_pos in heroes_and_positions:
            second_hero_id = str(heroes_and_positions[second_pos]['hero_id'])
            if hero_id == second_hero_id:
                continue
            key = f"{hero_id + pos}_with_{second_hero_id + second_pos}"
            foo = data.get(key, {})
            if len(foo) >= 15:
                value = foo.count(1) / (foo.count(1) + foo.count(0))
                output.setdefault(f'{mkdir}_over40_duo_synergy', {}).setdefault(pos, []).append(value)
            for third_pos in heroes_and_positions:
                third_hero_id = str(heroes_and_positions[third_pos]['hero_id'])
                if third_hero_id in [second_hero_id, hero_id]:
                    continue
                third_hero_id = str(heroes_and_positions[third_pos]['hero_id'])
                key = f"{hero_id + pos},{second_hero_id + second_pos},{third_hero_id + third_pos}"
                foo = data.get(key, {})
                if len(foo) >= 10:
                    combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                    if combo not in unique_combinations:
                        unique_combinations.add(combo)
                        value = foo.count(1) / (foo.count(1) + foo.count(0))
                        output.setdefault(f'{mkdir}_over40_trio', []).append(value)



def find_biggest_param(data, mid=False):
    win_val = data.get('win', 0)
    draw_val = data.get('draw', 0)
    lose_val = data.get('lose')
    if lose_val is None:
        lose_val = data.get('loose', 0)
    data = {'draw': draw_val, 'lose': lose_val, 'win': win_val}
    if abs(win_val - lose_val) < 5:
        return 'draw', int(round(draw_val))
    sorted_keys = sorted(data, key=lambda k: data[k], reverse=True)
    second_max_key = sorted_keys[1]
    second_max_value = int(round(data[second_max_key]))
    key = sorted_keys[0]
    first_key_max_value = int(round(data[key]))
    if first_key_max_value == second_max_value:
        if all(i in ['win', 'lose'] for i in (key, second_max_key)) or 'draw' in [key, second_max_key]:
            key = 'draw'
    return key, first_key_max_value


def _canon_vs(left, right):
    if left <= right:
        return f"{left}_vs_{right}", True
    return f"{right}_vs_{left}", False


def _split_vs_key(key):
    parts = key.split('_vs_')
    if len(parts) != 2:
        return None
    left_raw, right_raw = parts
    left_parts = left_raw.split(',')
    right_parts = right_raw.split(',')
    left_sorted = ",".join(sorted(left_parts))
    right_sorted = ",".join(sorted(right_parts))
    return left_raw, right_raw, left_parts, right_parts, left_sorted, right_sorted


def _get_lane_stats_for_key(key, heroes_data):
    split = _split_vs_key(key)
    if split is None:
        return heroes_data.get(key, {}), False, None, None
    left_raw, right_raw, left_parts, right_parts, left_sorted, right_sorted = split

    canon_key, left_is_canon = _canon_vs(left_sorted, right_sorted)
    if canon_key in heroes_data:
        return heroes_data[canon_key], not left_is_canon, left_parts, right_parts
    if key in heroes_data:
        return heroes_data[key], False, left_parts, right_parts
    rev_key = f"{right_raw}_vs_{left_raw}"
    if rev_key in heroes_data:
        return heroes_data[rev_key], True, left_parts, right_parts
    sorted_direct = f"{left_sorted}_vs_{right_sorted}"
    if sorted_direct in heroes_data:
        return heroes_data[sorted_direct], False, left_parts, right_parts
    sorted_rev = f"{right_sorted}_vs_{left_sorted}"
    if sorted_rev in heroes_data:
        return heroes_data[sorted_rev], True, left_parts, right_parts
    return {}, False, left_parts, right_parts


def _apply_stomp_weighted_counts(wins, draws, losses, stats, invert=False):
    try:
        weight = float(os.getenv("LANE_STOMP_COUNT_WEIGHT", "0"))
    except (TypeError, ValueError):
        weight = 0.0
    if weight <= 0 or not isinstance(stats, dict):
        return wins, draws, losses
    sw = int(stats.get("stomp_win", 0) or 0)
    sl = int(stats.get("stomp_lose", 0) or 0)
    if invert:
        sw, sl = sl, sw
    if sw == 0 and sl == 0:
        return wins, draws, losses
    return (float(wins) + weight * sw, float(draws), float(losses) + weight * sl)


def lane_2vs2(radiant, dire, heroes_data, output):
    data_2vs2 = heroes_data['2v2_lanes']

    bot_lane = f'{radiant["pos1"]["hero_id"]}pos1,{radiant["pos5"]["hero_id"]}pos5_vs_' \
               f'{dire["pos3"]["hero_id"]}pos3,{dire["pos4"]["hero_id"]}pos4'
    top_lane = f'{radiant["pos3"]["hero_id"]}pos3,{radiant["pos4"]["hero_id"]}pos4_vs_' \
               f'{dire["pos1"]["hero_id"]}pos1,{dire["pos5"]["hero_id"]}pos5'
    for lane, key in [[top_lane, 'top'], [bot_lane, 'bot']]:
        parts = lane.split('_vs_')
        left_raw, right_raw = parts if len(parts) == 2 else (None, None)
        left_parts = left_raw.split(',') if left_raw else []
        right_parts = right_raw.split(',') if right_raw else []
        left_sorted = ",".join(sorted(left_parts)) if left_parts else None
        right_sorted = ",".join(sorted(right_parts)) if right_parts else None

        wins = draws = losses = games = 0

        def _add_counts(s, invert=False):
            nonlocal wins, draws, losses, games
            if isinstance(s, dict) and 'games' in s:
                g = int(s.get('games', 0) or 0)
                if g <= 0:
                    return
                w = int(s.get('wins', 0) or 0)
                d = int(s.get('draws', 0) or 0)
                l = max(0, g - w - d)
            else:
                value = s.get('value', []) if isinstance(s, dict) else []
                if not value:
                    return
                g = len(value)
                w = value.count(1)
                d = value.count(0)
                l = value.count(-1)
            if invert:
                w, l = l, w
            w, d, l = _apply_stomp_weighted_counts(w, d, l, s, invert=invert)
            wins += w
            draws += d
            losses += l
            games += g

        candidates = []
        if left_raw and right_raw:
            candidates.extend([
                (f'{left_raw}_vs_{right_raw}', False),
                (f'{right_raw}_vs_{left_raw}', True),
            ])
        if left_sorted and right_sorted:
            candidates.extend([
                (f'{left_sorted}_vs_{right_sorted}', False),
                (f'{right_sorted}_vs_{left_sorted}', True),
            ])

        seen = set()
        for key_candidate, invert in candidates:
            if key_candidate in seen:
                continue
            seen.add(key_candidate)
            stats = data_2vs2.get(key_candidate, {})
            _add_counts(stats, invert=invert)

        if games >= 6:
            alpha = 1.0
            denom = games + 3.0 * alpha
            win = (wins + alpha) / denom if denom > 0 else 0
            draw = (draws + alpha) / denom if denom > 0 else 0
            lose = (losses + alpha) / denom if denom > 0 else 0

            total = lose + win + draw
            if total > 0:
                lose = lose / total * 100
                draw = draw / total * 100
                win = win / total * 100

                output.setdefault(key, {}).setdefault('lose', lose)
                output.setdefault(key, {}).setdefault('draw', draw)
                output.setdefault(key, {}).setdefault('win', win)


def multiply_list(lst, result=1):
    """Взвешенное среднее вместо перемножения - иначе занижаем винрейт в разы"""
    if lst:
        total = 0.0
        total_w = 0.0
        for it in lst:
            if isinstance(it, (tuple, list)) and len(it) >= 2:
                try:
                    val = float(it[0])
                    w = float(it[1])
                except (TypeError, ValueError):
                    continue
            else:
                try:
                    val = float(it)
                    w = 1.0
                except (TypeError, ValueError):
                    continue
            if w <= 0:
                continue
            total += val * w
            total_w += w
        if total_w > 0:
            return total / total_w
        return result
    return result




def get_values(lane_side, key, heroes_data, output):
    # Новая структура: {'wins': N, 'draws': M, 'games': K}
    stats, invert, left_parts, right_parts = _get_lane_stats_for_key(key, heroes_data)
    
    if isinstance(stats, dict) and 'games' in stats:
        games = stats.get('games', 0)
        if games >= 20:  # Поднято для очистки шумных 2v1 матчапов
            wins = stats.get('wins', 0)
            draws = stats.get('draws', 0)
            losses = max(0, games - wins - draws)
            if invert:
                wins, losses = losses, wins
            wins, draws, losses = _apply_stomp_weighted_counts(wins, draws, losses, stats, invert=invert)

            alpha = 1.0
            denom = games + 3.0 * alpha
            win = (wins + alpha) / denom if denom > 0 else 0
            draw = (draws + alpha) / denom if denom > 0 else 0
            lose = (losses + alpha) / denom if denom > 0 else 0

            output.setdefault(lane_side, {}).setdefault('lose', []).append((lose, games))
            output.setdefault(lane_side, {}).setdefault('draw', []).append((draw, games))
            output.setdefault(lane_side, {}).setdefault('win', []).append((win, games))
        else:
            solo = None
            if left_parts is not None and right_parts is not None:
                if len(left_parts) == 1:
                    solo = left_parts[0]
                elif len(right_parts) == 1:
                    solo = right_parts[0]
            if solo is None:
                foo = key.split('_vs_')
                to_be_appended = [i for i in foo if len(i.split(',')) == 1]
                if to_be_appended:
                    solo = to_be_appended[0]
            if solo is not None:
                output.setdefault(lane_side, {}).setdefault('not_used_hero_pos', []).append(solo)
    else:
        # Старая структура для обратной совместимости
        value = stats.get('value', [])
        if len(value) >= 10:
            games = len(value)
            wins = value.count(1)
            draws = value.count(0)
            losses = value.count(-1)
            if invert:
                wins, losses = losses, wins
            wins, draws, losses = _apply_stomp_weighted_counts(wins, draws, losses, stats, invert=invert)

            alpha = 1.0
            denom = games + 3.0 * alpha
            win = (wins + alpha) / denom if denom > 0 else 0
            draw = (draws + alpha) / denom if denom > 0 else 0
            lose = (losses + alpha) / denom if denom > 0 else 0

            output.setdefault(lane_side, {}).setdefault('lose', []).append((lose, games))
            output.setdefault(lane_side, {}).setdefault('draw', []).append((draw, games))
            output.setdefault(lane_side, {}).setdefault('win', []).append((win, games))
        else:
            solo = None
            if left_parts is not None and right_parts is not None:
                if len(left_parts) == 1:
                    solo = left_parts[0]
                elif len(right_parts) == 1:
                    solo = right_parts[0]
            if solo is None:
                foo = key.split('_vs_')
                to_be_appended = [i for i in foo if len(i.split(',')) == 1]
                if to_be_appended:
                    solo = to_be_appended[0]
            if solo is not None:
                output.setdefault(lane_side, {}).setdefault('not_used_hero_pos', []).append(solo)


def lane_2vs1(radiant, dire, heroes_data, lane):
    # Для mid матчей подходящие ключи лежат в 1v1_lanes, для боковых лайнов – в 2v1_lanes
    if lane == 'mid' and isinstance(heroes_data, dict) and '1v1_lanes' in heroes_data:
        heroes_data = heroes_data['1v1_lanes']
    else:
        heroes_data = heroes_data['2v1_lanes']
    output = {}
    if lane == 'bot':
        for key in [
                f'{radiant["pos1"]["hero_id"]}pos1,{radiant["pos5"]["hero_id"]}pos5_vs_{dire["pos3"]["hero_id"]}pos3',
                f'{radiant["pos1"]["hero_id"]}pos1,{radiant["pos5"]["hero_id"]}pos5_vs_'
                f'{dire["pos4"]["hero_id"]}pos4']:
            get_values('bot_radiant', key, heroes_data, output)
        for key in [
                f'{radiant["pos1"]["hero_id"]}pos1_vs_{dire["pos3"]["hero_id"]}pos3,{dire["pos4"]["hero_id"]}pos4',
                f'{radiant["pos5"]["hero_id"]}pos5_vs_{dire["pos3"]["hero_id"]}pos3,{dire["pos4"]["hero_id"]}pos4']:
            get_values('bot_dire', key, heroes_data, output)
    elif lane == 'top':
        for key in [
                f'{radiant["pos3"]["hero_id"]}pos3,{radiant["pos4"]["hero_id"]}pos4_vs_{dire["pos1"]["hero_id"]}pos1',
                f'{radiant["pos3"]["hero_id"]}pos3,{radiant["pos4"]["hero_id"]}pos4_vs_'
                f'{dire["pos5"]["hero_id"]}pos5']:
            get_values('top_radiant', key, heroes_data, output)
        for key in [
                f'{radiant["pos3"]["hero_id"]}pos3_vs_{dire["pos1"]["hero_id"]}pos1,{dire["pos5"]["hero_id"]}pos5',
                f'{radiant["pos4"]["hero_id"]}pos4_vs_{dire["pos1"]["hero_id"]}pos1,{dire["pos5"]["hero_id"]}pos5']:
            get_values('top_dire', key, heroes_data, output)
    elif lane == 'mid':
        key = f'{radiant["pos2"]["hero_id"]}pos2_vs_{dire["pos2"]["hero_id"]}pos2'
        stats, invert, _, _ = _get_lane_stats_for_key(key, heroes_data)
        
        # Новая структура: {'wins': N, 'draws': M, 'games': K}
        if isinstance(stats, dict) and 'games' in stats:
            games = stats.get('games', 0)
            if games >= 20:  # Поднято, чтобы убрать шум mid 1v1
                wins = stats.get('wins', 0)
                draws = stats.get('draws', 0)
                losses = max(0, games - wins - draws)
                if invert:
                    wins, losses = losses, wins
                wins, draws, losses = _apply_stomp_weighted_counts(wins, draws, losses, stats, invert=invert)

                alpha = 1.0
                denom = games + 3.0 * alpha
                win = ((wins + alpha) / denom) * 100 if denom > 0 else 0
                draw = ((draws + alpha) / denom) * 100 if denom > 0 else 0
                lose = ((losses + alpha) / denom) * 100 if denom > 0 else 0

                output.setdefault('mid_radiant', {}).setdefault('lose', lose)
                output.setdefault('mid_radiant', {}).setdefault('draw', draw)
                output.setdefault('mid_radiant', {}).setdefault('win', win)
    return output


def both_found(lane, data, output):
    data[f'{lane}_dire']['draw'] = multiply_list(data[f'{lane}_dire']['draw'])
    data[f'{lane}_dire']['win'] = multiply_list(data[f'{lane}_dire']['win'])
    data[f'{lane}_dire']['lose'] = multiply_list(data[f'{lane}_dire']['lose'])

    data[f'{lane}_radiant']['draw'] = multiply_list(data[f'{lane}_radiant']['draw'])
    data[f'{lane}_radiant']['win'] = multiply_list(data[f'{lane}_radiant']['win'])
    data[f'{lane}_radiant']['lose'] = multiply_list(data[f'{lane}_radiant']['lose'])

    radiant_draw = (data[f'{lane}_radiant']['draw'] + data[f'{lane}_dire']['draw'])/2
    radiant_win = (data[f'{lane}_radiant']['win'] + data[f'{lane}_dire']['win'])/2
    radiant_lose = (data[f'{lane}_radiant']['lose'] + data[f'{lane}_dire']['lose'])/2

    total = radiant_lose + radiant_draw + radiant_win
    if total not in [0, 0.0]:
        output.setdefault(f'{lane}', {}).setdefault('win', round(radiant_win / total * 100))
        output.setdefault(f'{lane}', {}).setdefault('lose', round(radiant_lose / total * 100))
        output.setdefault(f'{lane}', {}).setdefault('draw', round(radiant_draw / total * 100))

        bot_key, bot_key_value = find_biggest_param(output[f'{lane}'])
        return bot_key, bot_key_value
    return None


def counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane):
    """Анализ индивидуальных 1v1 матчапов на лайне (контрпики)"""
    heroes_data_1v1 = heroes_data.get('1v1_lanes', {})

    def _aggregate_matchups(matchups):
        buckets = []
        for matchup_key in matchups:
            stats, invert, _, _ = _get_lane_stats_for_key(matchup_key, heroes_data_1v1)

            if isinstance(stats, dict) and 'games' in stats:
                games = stats.get('games', 0)
                if games >= 50:
                    wins = stats.get('wins', 0)
                    draws = stats.get('draws', 0)
                    losses = max(0, games - wins - draws)
                    if invert:
                        wins, losses = losses, wins
                    wins, draws, losses = _apply_stomp_weighted_counts(wins, draws, losses, stats, invert=invert)

                    alpha = 1.0
                    denom = games + 3.0 * alpha
                    win = (wins + alpha) / denom if denom > 0 else 0
                    draw = (draws + alpha) / denom if denom > 0 else 0
                    lose = (losses + alpha) / denom if denom > 0 else 0
                    buckets.append((win, draw, lose, games))
                continue

            value = stats.get('value', [])
            if len(value) >= 50:
                games = len(value)
                wins = value.count(1)
                draws = value.count(0)
                losses = value.count(-1)
                if invert:
                    wins, losses = losses, wins
                wins, draws, losses = _apply_stomp_weighted_counts(wins, draws, losses, stats, invert=invert)

                alpha = 1.0
                denom = games + 3.0 * alpha
                win = (wins + alpha) / denom if denom > 0 else 0
                draw = (draws + alpha) / denom if denom > 0 else 0
                lose = (losses + alpha) / denom if denom > 0 else 0
                buckets.append((win, draw, lose, games))

        if len(buckets) < 2:
            return None

        total_w = sum(float(b[3]) for b in buckets)
        if total_w <= 0:
            return None

        win_avg = sum(float(b[0]) * float(b[3]) for b in buckets) / total_w
        draw_avg = sum(float(b[1]) * float(b[3]) for b in buckets) / total_w
        lose_avg = sum(float(b[2]) * float(b[3]) for b in buckets) / total_w

        total = win_avg + draw_avg + lose_avg
        if total <= 0:
            return None

        win = win_avg / total * 100
        draw = draw_avg / total * 100
        lose = lose_avg / total * 100
        return find_biggest_param({'win': win, 'draw': draw, 'lose': lose})

    if lane == 'bot':
        # Все возможные 1v1 матчапы на бот лайне
        matchups = [
            f"{radiant_heroes_and_pos['pos1']['hero_id']}pos1_vs_{dire_heroes_and_pos['pos3']['hero_id']}pos3",
            f"{radiant_heroes_and_pos['pos1']['hero_id']}pos1_vs_{dire_heroes_and_pos['pos4']['hero_id']}pos4",
            f"{radiant_heroes_and_pos['pos5']['hero_id']}pos5_vs_{dire_heroes_and_pos['pos3']['hero_id']}pos3",
            f"{radiant_heroes_and_pos['pos5']['hero_id']}pos5_vs_{dire_heroes_and_pos['pos4']['hero_id']}pos4",
        ]
        res = _aggregate_matchups(matchups)
        if res is not None:
            return res

    elif lane == 'top':
        # Все возможные 1v1 матчапы на топ лайне
        matchups = [
            f"{radiant_heroes_and_pos['pos3']['hero_id']}pos3_vs_{dire_heroes_and_pos['pos1']['hero_id']}pos1",
            f"{radiant_heroes_and_pos['pos3']['hero_id']}pos3_vs_{dire_heroes_and_pos['pos5']['hero_id']}pos5",
            f"{radiant_heroes_and_pos['pos4']['hero_id']}pos4_vs_{dire_heroes_and_pos['pos1']['hero_id']}pos1",
            f"{radiant_heroes_and_pos['pos4']['hero_id']}pos4_vs_{dire_heroes_and_pos['pos5']['hero_id']}pos5",
        ]
        res = _aggregate_matchups(matchups)
        if res is not None:
            return res

    return None


def synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane):
    heroes_data = heroes_data['1_with_1_lanes']
    if lane == 'bot':
        radiant_pair = sorted([
            f"{radiant_heroes_and_pos['pos1']['hero_id']}pos1",
            f"{radiant_heroes_and_pos['pos5']['hero_id']}pos5",
        ])
        dire_pair = sorted([
            f"{dire_heroes_and_pos['pos3']['hero_id']}pos3",
            f"{dire_heroes_and_pos['pos4']['hero_id']}pos4",
        ])
        radiant_key = f"{radiant_pair[0]}_with_{radiant_pair[1]}"
        dire_key = f"{dire_pair[0]}_with_{dire_pair[1]}"
        
        radiant_stats = heroes_data.get(radiant_key, {})
        dire_stats = heroes_data.get(dire_key, {})
        
        # Новая структура: {'wins': N, 'draws': M, 'games': K}
        if isinstance(radiant_stats, dict) and 'games' in radiant_stats and isinstance(dire_stats, dict) and 'games' in dire_stats:
            radiant_games = radiant_stats.get('games', 0)
            dire_games = dire_stats.get('games', 0)
            
            if radiant_games >= 30 and dire_games >= 30:
                radiant_wins = radiant_stats.get('wins', 0)
                radiant_draws = radiant_stats.get('draws', 0)
                radiant_losses = max(0, radiant_games - radiant_wins - radiant_draws)
                
                dire_wins = dire_stats.get('wins', 0)
                dire_draws = dire_stats.get('draws', 0)
                dire_losses = max(0, dire_games - dire_wins - dire_draws)
                radiant_wins, radiant_draws, radiant_losses = _apply_stomp_weighted_counts(
                    radiant_wins, radiant_draws, radiant_losses, radiant_stats, invert=False
                )
                dire_wins, dire_draws, dire_losses = _apply_stomp_weighted_counts(
                    dire_wins, dire_draws, dire_losses, dire_stats, invert=False
                )

                alpha = 1.0
                r_denom = radiant_games + 3.0 * alpha
                d_denom = dire_games + 3.0 * alpha

                radiant_win = (radiant_wins + alpha) / r_denom if r_denom > 0 else 0
                radiant_lose = (radiant_losses + alpha) / r_denom if r_denom > 0 else 0
                radiant_tie = (radiant_draws + alpha) / r_denom if r_denom > 0 else 0

                dire_win = (dire_wins + alpha) / d_denom if d_denom > 0 else 0
                dire_lose = (dire_losses + alpha) / d_denom if d_denom > 0 else 0
                dire_tie = (dire_draws + alpha) / d_denom if d_denom > 0 else 0
                
                win = radiant_win * dire_lose
                draw = radiant_tie * dire_tie
                lose = radiant_lose * dire_win
                total = sum([win, draw, lose])
                if total > 0:
                    win = win / total * 100
                    draw = draw / total * 100
                    lose = lose / total * 100
                    key, first_key_max_value = find_biggest_param({'win': win, 'draw': draw, 'lose': lose})
                    return key, first_key_max_value
        else:
            # Старая структура для обратной совместимости
            radiant_value = radiant_stats.get('value', [])
            dire_value = dire_stats.get('value', [])
            if len(radiant_value) >= 30 and len(dire_value) >= 30:
                radiant_win = radiant_value.count(1) / len(radiant_value)
                radiant_lose = radiant_value.count(-1) / len(radiant_value)
                radiant_tie = radiant_value.count(0) / len(radiant_value)
                dire_win = dire_value.count(1) / len(dire_value)
                dire_lose = dire_value.count(-1) / len(dire_value)
                dire_tie = dire_value.count(0) / len(dire_value)
                win = radiant_win * dire_lose
                draw = radiant_tie * dire_tie
                lose = radiant_lose * dire_win
                total = sum([win, draw, lose])
                win = win / total * 100
                draw = draw / total * 100
                lose = lose / total * 100
                key, first_key_max_value = find_biggest_param({'win': win, 'draw': draw, 'lose': lose})
                return key, first_key_max_value

    elif lane == 'top':
        radiant_pair = sorted([
            f"{radiant_heroes_and_pos['pos3']['hero_id']}pos3",
            f"{radiant_heroes_and_pos['pos4']['hero_id']}pos4",
        ])
        dire_pair = sorted([
            f"{dire_heroes_and_pos['pos1']['hero_id']}pos1",
            f"{dire_heroes_and_pos['pos5']['hero_id']}pos5",
        ])
        radiant_key = f"{radiant_pair[0]}_with_{radiant_pair[1]}"
        dire_key = f"{dire_pair[0]}_with_{dire_pair[1]}"
        
        radiant_stats = heroes_data.get(radiant_key, {})
        dire_stats = heroes_data.get(dire_key, {})
        
        # Новая структура: {'wins': N, 'draws': M, 'games': K}
        if isinstance(radiant_stats, dict) and 'games' in radiant_stats and isinstance(dire_stats, dict) and 'games' in dire_stats:
            radiant_games = radiant_stats.get('games', 0)
            dire_games = dire_stats.get('games', 0)
            
            if radiant_games >= 30 and dire_games >= 30:
                radiant_wins = radiant_stats.get('wins', 0)
                radiant_draws = radiant_stats.get('draws', 0)
                radiant_losses = max(0, radiant_games - radiant_wins - radiant_draws)
                
                dire_wins = dire_stats.get('wins', 0)
                dire_draws = dire_stats.get('draws', 0)
                dire_losses = max(0, dire_games - dire_wins - dire_draws)
                radiant_wins, radiant_draws, radiant_losses = _apply_stomp_weighted_counts(
                    radiant_wins, radiant_draws, radiant_losses, radiant_stats, invert=False
                )
                dire_wins, dire_draws, dire_losses = _apply_stomp_weighted_counts(
                    dire_wins, dire_draws, dire_losses, dire_stats, invert=False
                )

                alpha = 1.0
                r_denom = radiant_games + 3.0 * alpha
                d_denom = dire_games + 3.0 * alpha

                radiant_win = (radiant_wins + alpha) / r_denom if r_denom > 0 else 0
                radiant_lose = (radiant_losses + alpha) / r_denom if r_denom > 0 else 0
                radiant_tie = (radiant_draws + alpha) / r_denom if r_denom > 0 else 0

                dire_win = (dire_wins + alpha) / d_denom if d_denom > 0 else 0
                dire_lose = (dire_losses + alpha) / d_denom if d_denom > 0 else 0
                dire_tie = (dire_draws + alpha) / d_denom if d_denom > 0 else 0
                
                win = radiant_win * dire_lose
                draw = radiant_tie * dire_tie
                lose = radiant_lose * dire_win
                total = sum([win, draw, lose])
                if total > 0:
                    win = win / total * 100
                    draw = draw / total * 100
                    lose = lose / total * 100
                    key, first_key_max_value = find_biggest_param({'win': win, 'draw': draw, 'lose': lose})
                    return key, first_key_max_value
        else:
            # Старая структура для обратной совместимости
            radiant_value = radiant_stats.get('value', [])
            dire_value = dire_stats.get('value', [])
            if len(radiant_value) >= 30 and len(dire_value) >= 30:
                radiant_win = radiant_value.count(1)/len(radiant_value)
                radiant_lose = radiant_value.count(-1)/len(radiant_value)
                radiant_tie = radiant_value.count(0)/len(radiant_value)
                dire_win = dire_value.count(1)/len(dire_value)
                dire_lose = dire_value.count(-1)/len(dire_value)
                dire_tie = dire_value.count(0)/len(dire_value)
                win = radiant_win * dire_lose
                draw = radiant_tie * dire_tie
                lose = radiant_lose * dire_win
                total = sum([win, draw, lose])
                win = win/total*100
                draw = draw/total*100
                lose = lose/total*100
                key, first_key_max_value = find_biggest_param({'win': win, 'draw': draw, 'lose': lose})
                return key, first_key_max_value
    return None


def _merge_lane_predictions(counterpick_res, synergy_res, counterpick_weight=0.55,
                            draw_floor=52, draw_cap=62, strong_gap=6, hard_take=60, soft_block=55):
    """
    Смешивает сигнал контрпика лайна (1v1/1v2) с синергией дуо на лайне.
    coverage не приоритет: если есть явный сильный сигнал — берём его даже при расхождении,
    иначе уходим в draw, чтобы не плодить ложнопозитивы.
    """
    def _normalize(res):
        if not res or not isinstance(res, (tuple, list)) or len(res) != 2:
            return None
        key, val = res
        if key not in ('win', 'lose', 'draw'):
            return None
        try:
            return key, float(val)
        except (TypeError, ValueError):
            return None

    cp = _normalize(counterpick_res)
    sy = _normalize(synergy_res)

    if not cp and not sy:
        return None, None
    if cp and not sy:
        return cp
    if sy and not cp:
        return sy

    cp_key, cp_val = cp
    sy_key, sy_val = sy

    if cp_key == sy_key:
        blended = cp_val * counterpick_weight + sy_val * (1 - counterpick_weight)
        return cp_key, round(blended)

    # Источники расходятся: если один сильно уверен, берём его, иначе draw
    if abs(cp_val - sy_val) >= strong_gap:
        chosen = cp if cp_val > sy_val else sy
        return chosen[0], round(chosen[1])
    if (cp_val >= hard_take and sy_val <= soft_block) or (sy_val >= hard_take and cp_val <= soft_block):
        chosen = cp if cp_val > sy_val else sy
        return chosen[0], round(chosen[1])

    confidence = (cp_val + sy_val) / 2
    confidence = max(draw_floor, min(draw_cap, confidence))
    return 'draw', round(confidence)


def _single_side_2v1_prediction(lane_data, lane_name):
    """
    Делает предсказание по одному найденному боксу 2v1 (radiant/dire),
    если второй отсутствует. Возвращает (key, value) в процентах или None.
    """
    predictions = []
    for side in (f'{lane_name}_radiant', f'{lane_name}_dire'):
        side_data = lane_data.get(side, {})
        if not side_data:
            continue
        agg = {}
        for k in ('win', 'draw', 'lose'):
            vals = side_data.get(k, [])
            if vals:
                agg[k] = multiply_list(vals) * 100
        if not agg:
            continue
        key, val = find_biggest_param(agg)
        # Инвертируем перспективу, если данные со стороны Dire
        if side.endswith('_dire'):
            if key == 'win':
                key = 'lose'
            elif key == 'lose':
                key = 'win'
        predictions.append((key, val))

    if not predictions:
        return None
    # Берём самый уверенный
    return max(predictions, key=lambda x: x[1])


# === Lane corrector (ML) helpers ===
_LC_LANES = ("top", "mid", "bot")
_LC_CAT_COLS = (
    "lane",
    "patch_bucket",
    "pred_outcome",
    "base_out",
    "raw2v1_status",
    "raw2v2_outcome",
    "raw2v1_outcome",
    "rawcp_outcome",
    "rawsy_outcome",
)
_LC_MODEL_CACHE = {"loaded": False, "models": {}, "error": None}
_LC_DEC_MODEL_CACHE = {"loaded": False, "models": {}, "error": None}
_LC_PATCH_TS_736 = 1716498000
_LC_PATCH_TS_738 = 1740096000
_LC_PATCH_TS_739 = 1747872000
_LC_DEC_SWITCH_DEFAULT = {
    "top": 0.00,
    "mid": 0.04,
    "bot": 0.08,
}


class _LCHeroStats:
    __slots__ = ("games", "wins", "lane_games", "lane_wins")

    def __init__(self):
        self.games = 0
        self.wins = 0
        self.lane_games = {l: 0 for l in _LC_LANES}
        self.lane_wins = {l: 0.0 for l in _LC_LANES}


class _LCPredStats:
    __slots__ = ("games", "correct", "expected_sum", "actual_sum", "conf_sum")

    def __init__(self):
        self.games = 0
        self.correct = 0
        self.expected_sum = 0.0
        self.actual_sum = 0.0
        self.conf_sum = 0.0


class _LCPlayerStats:
    __slots__ = ("games", "wins", "lane_games", "lane_wins", "heroes", "pred")

    def __init__(self):
        self.games = 0
        self.wins = 0
        self.lane_games = {l: 0 for l in _LC_LANES}
        self.lane_wins = {l: 0.0 for l in _LC_LANES}
        self.heroes = {}
        self.pred = {l: _LCPredStats() for l in _LC_LANES}


class _LCH2HStats:
    __slots__ = ("games", "score_sum")

    def __init__(self):
        self.games = 0
        self.score_sum = 0.0


class _LCTeamLaneEntry:
    __slots__ = ("roster", "lane_scores")

    def __init__(self, roster, lane_scores):
        self.roster = roster
        self.lane_scores = lane_scores


def _lc_coerce_int(v):
    try:
        if v is None:
            return 0
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return 0
        return int(float(s))
    except Exception:
        return 0


def _lc_parse_pos(position):
    if position is None:
        return None
    s = str(position).strip().upper()
    if not s:
        return None
    if s.startswith("POSITION_"):
        try:
            n = int(s.replace("POSITION_", ""))
        except Exception:
            return None
        return n if 1 <= n <= 5 else None
    try:
        n = int(s)
        return n if 1 <= n <= 5 else None
    except Exception:
        return None


def _lc_patch_bucket(start_ts):
    ts = _lc_coerce_int(start_ts)
    if ts >= _LC_PATCH_TS_739:
        return "p739_plus"
    if ts >= _LC_PATCH_TS_738:
        return "p738"
    if ts >= _LC_PATCH_TS_736:
        return "p736"
    return "pre736"


def _lc_smooth_rate(num, den, prior=0.5, prior_weight=10.0):
    return (num + prior * prior_weight) / (den + prior_weight) if den >= 0 else prior


def _lc_invert_outcome(outcome):
    if outcome == "win":
        return "lose"
    if outcome == "lose":
        return "win"
    return outcome


def _lc_parse_lane_prediction(raw):
    if not raw:
        return None, None
    cleaned = str(raw).strip()
    if not cleaned or cleaned.lower() == "none":
        return None, None
    if ":" in cleaned:
        cleaned = cleaned.split(":", 1)[1].strip()
    parts = cleaned.split()
    if len(parts) != 2:
        return None, None
    outcome = parts[0].lower()
    if outcome == "loose":
        outcome = "lose"
    try:
        conf = int(float(parts[1].rstrip("%")))
    except Exception:
        conf = None
    if outcome not in ("win", "lose", "draw"):
        return None, None
    return outcome, conf


def _lc_pred_probs(outcome, conf):
    if outcome is None or conf is None:
        return 1 / 3, 1 / 3, 1 / 3
    p = max(0.0, min(1.0, conf / 100.0))
    if outcome == "win":
        return p, (1 - p) / 2, (1 - p) / 2
    if outcome == "lose":
        return (1 - p) / 2, (1 - p) / 2, p
    return (1 - p) / 2, p, (1 - p) / 2


def _lc_expected_score(p_win, p_lose):
    return float(p_win - p_lose)


def _lc_actual_score(outcome):
    if outcome == "win":
        return 1.0
    if outcome == "lose":
        return -1.0
    if outcome == "draw":
        return 0.0
    return None


def _lc_lane_from_player(is_radiant, pos):
    if pos == 2:
        return "mid"
    if is_radiant:
        if pos in (3, 4):
            return "top"
        if pos in (1, 5):
            return "bot"
    else:
        if pos in (3, 4):
            return "bot"
        if pos in (1, 5):
            return "top"
    return None


def _lc_get_player(stats, pid):
    if pid not in stats:
        stats[pid] = _LCPlayerStats()
    return stats[pid]


def _lc_get_hero(pstats, hero_id):
    if hero_id not in pstats.heroes:
        pstats.heroes[hero_id] = _LCHeroStats()
    return pstats.heroes[hero_id]


def _lc_player_features(pstats, hero_id, lane, dota_plus_xp):
    import math

    g = pstats.games
    w = pstats.wins
    lane_g = pstats.lane_games.get(lane, 0)
    lane_w = pstats.lane_wins.get(lane, 0.0)

    hstats = pstats.heroes.get(hero_id, _LCHeroStats())
    hg = hstats.games
    hw = hstats.wins
    hlg = hstats.lane_games.get(lane, 0)
    hlw = hstats.lane_wins.get(lane, 0.0)

    pred = pstats.pred.get(lane, _LCPredStats())
    pred_acc = _lc_smooth_rate(float(pred.correct), float(pred.games), prior=0.5, prior_weight=5.0)
    pred_bias = (pred.actual_sum - pred.expected_sum) / float(pred.games + 5)
    pred_conf = pred.conf_sum / float(pred.games) if pred.games > 0 else 50.0

    dpxp = float(dota_plus_xp)
    return {
        "games": float(g),
        "wr": float(_lc_smooth_rate(float(w), float(g), prior=0.5, prior_weight=10.0)),
        "lane_games": float(lane_g),
        "lane_wr": float(_lc_smooth_rate(float(lane_w), float(lane_g), prior=0.5, prior_weight=10.0)),
        "lane_share": float(lane_g / max(1.0, float(g))),
        "hero_games": float(hg),
        "hero_wr": float(_lc_smooth_rate(float(hw), float(hg), prior=0.5, prior_weight=10.0)),
        "hero_lane_games": float(hlg),
        "hero_lane_wr": float(_lc_smooth_rate(float(hlw), float(hlg), prior=0.5, prior_weight=10.0)),
        "pred_games": float(pred.games),
        "pred_acc": float(pred_acc),
        "pred_bias": float(pred_bias),
        "pred_conf": float(pred_conf),
        "dpxp": dpxp,
        "dpxp_log": float(math.log1p(max(0.0, dpxp))),
    }


def _lc_aggregate_players(players):
    if not players:
        return {}
    keys = list(players[0].keys())
    out = {}
    for k in keys:
        vals = [p.get(k, 0.0) for p in players]
        out[f"{k}_mean"] = float(sum(vals) / len(vals))
        out[f"{k}_min"] = float(min(vals))
        out[f"{k}_max"] = float(max(vals))
    return out


def _lc_collect_lane_players(side_pos, is_radiant, lane):
    if lane == "mid":
        pos_list = [2]
    elif lane == "top":
        pos_list = [3, 4] if is_radiant else [1, 5]
    else:
        pos_list = [1, 5] if is_radiant else [3, 4]
    out = []
    for pos in pos_list:
        if pos in side_pos:
            out.append(side_pos[pos])
    return out


def _lc_team_id(raw):
    if isinstance(raw, dict):
        return _lc_coerce_int(raw.get("id"))
    return _lc_coerce_int(raw)


def _lc_lane_score(outcome):
    if outcome == "win":
        return 1.0
    if outcome == "lose":
        return 0.0
    if outcome == "draw":
        return 0.5
    return None


def _lc_h2h_feature_pack(prefix, wr_values, games_values, known_pairs, total_pairs):
    if not wr_values:
        return {
            f"{prefix}_wr_mean": 0.5,
            f"{prefix}_wr_min": 0.5,
            f"{prefix}_wr_max": 0.5,
            f"{prefix}_wr_weighted": 0.5,
            f"{prefix}_games_mean": 0.0,
            f"{prefix}_games_min": 0.0,
            f"{prefix}_games_max": 0.0,
            f"{prefix}_coverage": 0.0,
        }
    weighted_pairs = list(zip(wr_values, games_values))
    return {
        f"{prefix}_wr_mean": float(sum(wr_values) / len(wr_values)),
        f"{prefix}_wr_min": float(min(wr_values)),
        f"{prefix}_wr_max": float(max(wr_values)),
        f"{prefix}_wr_weighted": float(_lc_weighted_mean([(v, g) for v, g in weighted_pairs], 0.5)),
        f"{prefix}_games_mean": float(sum(games_values) / len(games_values)),
        f"{prefix}_games_min": float(min(games_values)),
        f"{prefix}_games_max": float(max(games_values)),
        f"{prefix}_coverage": float(known_pairs / max(1, total_pairs)),
    }


def _lc_lane_h2h_features(lane, rad_players, dire_players, pair_stats, pair_hero_stats):
    total_pairs = len(rad_players) * len(dire_players)
    if total_pairs <= 0:
        out = _lc_h2h_feature_pack("h2h_pvp", [], [], 0, 1)
        out.update(_lc_h2h_feature_pack("h2h_hero", [], [], 0, 1))
        return out

    pvp_wr = []
    pvp_games = []
    pvp_known = 0
    hero_wr = []
    hero_games = []
    hero_known = 0

    for rp in rad_players:
        rpid = _lc_coerce_int(rp.get("account_id"))
        rhid = _lc_coerce_int(rp.get("hero_id"))
        for dp in dire_players:
            dpid = _lc_coerce_int(dp.get("account_id"))
            dhid = _lc_coerce_int(dp.get("hero_id"))

            pvp_st = pair_stats.get((lane, rpid, dpid))
            pvp_g = int(pvp_st.games) if pvp_st else 0
            pvp_s = float(pvp_st.score_sum) if pvp_st else 0.0
            if pvp_g > 0:
                pvp_known += 1
            pvp_wr.append(float(_lc_smooth_rate(pvp_s, float(pvp_g), prior=0.5, prior_weight=5.0)))
            pvp_games.append(float(pvp_g))

            hero_st = pair_hero_stats.get((lane, rpid, dpid, rhid, dhid))
            hero_g = int(hero_st.games) if hero_st else 0
            hero_s = float(hero_st.score_sum) if hero_st else 0.0
            if hero_g > 0:
                hero_known += 1
            hero_wr.append(float(_lc_smooth_rate(hero_s, float(hero_g), prior=0.5, prior_weight=3.0)))
            hero_games.append(float(hero_g))

    out = _lc_h2h_feature_pack("h2h_pvp", pvp_wr, pvp_games, pvp_known, total_pairs)
    out.update(_lc_h2h_feature_pack("h2h_hero", hero_wr, hero_games, hero_known, total_pairs))
    return out


def _lc_team_lane_overlap_features(team_history, team_id, lane, roster, min_overlap=4, max_scan=80, max_hits=20):
    out = {
        "team_o4_games": 0.0,
        "team_o4_score": 0.5,
        "team_o4_win_rate": 0.5,
        "team_o4_draw_rate": 0.0,
        "team_o4_decisive_rate": 0.0,
    }
    if team_id <= 0 or not roster:
        return out
    hist = team_history.get(team_id)
    if not hist:
        return out

    scores = []
    scanned = 0
    for entry in reversed(hist):
        scanned += 1
        if scanned > max_scan or len(scores) >= max_hits:
            break
        if len(roster.intersection(entry.roster)) < min_overlap:
            continue
        score = entry.lane_scores.get(lane)
        if score is None:
            continue
        scores.append(float(score))

    games = len(scores)
    if games <= 0:
        return out
    wins = sum(1 for s in scores if s > 0.75)
    draws = sum(1 for s in scores if 0.25 < s < 0.75)
    score_sum = float(sum(scores))

    out["team_o4_games"] = float(games)
    out["team_o4_score"] = float(_lc_smooth_rate(score_sum, float(games), prior=0.5, prior_weight=3.0))
    out["team_o4_win_rate"] = float(_lc_smooth_rate(float(wins), float(games), prior=0.5, prior_weight=3.0))
    out["team_o4_draw_rate"] = float(draws / float(games))
    out["team_o4_decisive_rate"] = float((games - draws) / float(games))
    return out


def _lc_update_matchup_stats(table, key, score):
    item = table.get(key)
    if item is None:
        item = _LCH2HStats()
        table[key] = item
    item.games += 1
    item.score_sum += float(score)


def _lc_entry_counts(entry, invert=False):
    if not entry or not isinstance(entry, dict) or "games" not in entry:
        return None
    g = int(entry.get("games", 0) or 0)
    if g <= 0:
        return None
    w = int(entry.get("wins", 0) or 0)
    d = int(entry.get("draws", 0) or 0)
    l = max(0, g - w - d)
    stomp_w = int(entry.get("stomp_win", 0) or 0)
    stomp_l = int(entry.get("stomp_lose", 0) or 0)
    if invert:
        w, l = l, w
        stomp_w, stomp_l = stomp_l, stomp_w
    return w, d, l, g, stomp_w, stomp_l


def _lc_entry_stats(entry, invert=False, alpha=1.0):
    counts = _lc_entry_counts(entry, invert=invert)
    if counts is None:
        return None
    w, d, l, g, stomp_w, stomp_l = counts
    denom = g + 3.0 * alpha
    win = (w + alpha) / denom if denom > 0 else 0.0
    draw = (d + alpha) / denom if denom > 0 else 0.0
    lose = (l + alpha) / denom if denom > 0 else 0.0
    stomp_total = stomp_w + stomp_l
    stomp_rate = (stomp_total / g) if g > 0 else 0.0
    stomp_balance = ((stomp_w - stomp_l) / g) if g > 0 else 0.0
    return {
        "win": float(win),
        "draw": float(draw),
        "lose": float(lose),
        "games": float(g),
        "stomp_rate": float(stomp_rate),
        "stomp_balance": float(stomp_balance),
    }


def _lc_weighted_mean(pairs, default=0.0):
    if not pairs:
        return default
    num = 0.0
    den = 0.0
    for v, w in pairs:
        if w <= 0:
            continue
        num += v * w
        den += w
    return num / den if den > 0 else default


def _lc_duo_key(hero_pos_list):
    parts = [f"{hid}pos{pos}" for hid, pos in hero_pos_list]
    return ",".join(sorted(parts))


def _lc_solo_stats_for_side(solo_data, heroes):
    stats = []
    stomp_rates = []
    stomp_balances = []
    games_list = []
    for hero_id, pos in heroes:
        key = f"{hero_id}pos{pos}"
        entry = solo_data.get(key)
        st = _lc_entry_stats(entry, invert=False)
        if st is None:
            continue
        stats.append((st["win"], st["games"]))
        stomp_rates.append((st["stomp_rate"], st["games"]))
        stomp_balances.append((st["stomp_balance"], st["games"]))
        games_list.append(st["games"])
    if not stats:
        return {
            "wr_mean": 0.0,
            "games_mean": 0.0,
            "games_min": 0.0,
            "games_max": 0.0,
            "stomp_rate_mean": 0.0,
            "stomp_balance_mean": 0.0,
        }
    return {
        "wr_mean": _lc_weighted_mean(stats, 0.0),
        "games_mean": float(sum(games_list) / len(games_list)) if games_list else 0.0,
        "games_min": float(min(games_list)) if games_list else 0.0,
        "games_max": float(max(games_list)) if games_list else 0.0,
        "stomp_rate_mean": _lc_weighted_mean(stomp_rates, 0.0),
        "stomp_balance_mean": _lc_weighted_mean(stomp_balances, 0.0),
    }


def _lc_raw_2v2_signal(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane):
    if lane == "mid":
        return None, None
    bucket = {}
    lane_2vs2(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, bucket)
    if lane in bucket and bucket[lane]:
        return find_biggest_param(bucket[lane])
    return None, None


def _lc_raw_2v1_signal(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane):
    layer = lane_2vs1(
        radiant=radiant_heroes_and_pos,
        dire=dire_heroes_and_pos,
        heroes_data=heroes_data,
        lane=lane,
    )
    if lane == "mid":
        mid_stats = layer.get("mid_radiant")
        if mid_stats:
            out, conf = find_biggest_param(mid_stats)
            return out, conf, "mid"
        return None, None, "none"

    has_both = all(
        len(line) == 2
        for line in [
            layer.get(f"{lane}_radiant", {}).get("win", {}),
            layer.get(f"{lane}_dire", {}).get("win", {}),
        ]
    )
    if has_both:
        tmp = {}
        res = both_found(lane=lane, data=layer, output=tmp)
        if res:
            return res[0], res[1], "full"
        return None, None, "full"

    single = _single_side_2v1_prediction(layer, lane)
    if single:
        return single[0], single[1], "single"
    return None, None, "none"


def _lc_add_pred_features(row, prefix, outcome, conf):
    row[f"{prefix}_outcome"] = outcome or "none"
    row[f"{prefix}_conf"] = float(conf) if conf is not None else 0.0
    p_win, p_draw, p_lose = _lc_pred_probs(outcome, conf)
    row[f"{prefix}_p_win"] = float(p_win)
    row[f"{prefix}_p_draw"] = float(p_draw)
    row[f"{prefix}_p_lose"] = float(p_lose)
    row[f"{prefix}_score"] = float(_lc_expected_score(p_win, p_lose))


def _lc_lane_label_from_outcome(raw):
    if not raw:
        return None
    s = str(raw).upper()
    if "RADIANT" in s:
        return "win"
    if "DIRE" in s:
        return "lose"
    if "TIE" in s or "DRAW" in s:
        return "draw"
    return None


def _lc_build_player_maps(players):
    if not isinstance(players, list) or len(players) != 10:
        return None, None
    rad_pos = {}
    dire_pos = {}
    for p in players:
        pos = _lc_parse_pos(p.get("position"))
        if pos is None:
            return None, None
        sa = p.get("steamAccount") or {}
        account_id = _lc_coerce_int(sa.get("id"))
        hero_id = _lc_coerce_int(p.get("heroId"))
        if account_id <= 0 or hero_id <= 0:
            return None, None
        is_radiant = bool(p.get("isRadiant"))
        entry = {
            "account_id": account_id,
            "hero_id": hero_id,
            "is_radiant": is_radiant,
            "pos": pos,
            "dota_plus_xp": _lc_coerce_int(p.get("dotaPlusHeroXp")),
        }
        if is_radiant:
            if pos in rad_pos:
                return None, None
            rad_pos[pos] = entry
        else:
            if pos in dire_pos:
                return None, None
            dire_pos[pos] = entry
    if len(rad_pos) != 5 or len(dire_pos) != 5:
        return None, None
    return rad_pos, dire_pos


def _lc_load_models(models_dir=None):
    if _LC_MODEL_CACHE.get("loaded"):
        return _LC_MODEL_CACHE
    models_dir = models_dir or "/Users/alex/Documents/ingame/ml-models"
    try:
        from catboost import CatBoostClassifier  # type: ignore
    except Exception as exc:
        _LC_MODEL_CACHE["loaded"] = True
        _LC_MODEL_CACHE["error"] = f"catboost import failed: {exc}"
        return _LC_MODEL_CACHE

    models = {}
    for lane in _LC_LANES:
        model_path = os.path.join(models_dir, f"lane_corrector_{lane}.cbm")
        meta_path = os.path.join(models_dir, f"lane_corrector_{lane}_meta.json")
        if not os.path.exists(model_path) or not os.path.exists(meta_path):
            continue
        try:
            model = CatBoostClassifier()
            model.load_model(model_path)
            with open(meta_path, "r") as f:
                meta = json.load(f)
            feature_cols = meta.get("feature_cols") or []
            raw_label_map = meta.get("label_map") or {0: "lose", 1: "draw", 2: "win"}
            label_map = {}
            if isinstance(raw_label_map, dict):
                for k, v in raw_label_map.items():
                    try:
                        idx = int(k)
                    except Exception:
                        continue
                    if v in ("lose", "draw", "win"):
                        label_map[idx] = v
            for idx, label in ((0, "lose"), (1, "draw"), (2, "win")):
                if idx not in label_map:
                    label_map[idx] = label
            cat_idx = [feature_cols.index(c) for c in _LC_CAT_COLS if c in feature_cols]
            models[lane] = {
                "model": model,
                "feature_cols": feature_cols,
                "cat_idx": cat_idx,
                "label_map": label_map,
            }
        except Exception as exc:
            _LC_MODEL_CACHE["error"] = f"lane_corrector load failed: {exc}"
            continue
    _LC_MODEL_CACHE["models"] = models
    _LC_MODEL_CACHE["loaded"] = True
    return _LC_MODEL_CACHE


def _lc_load_decisive_models(models_dir=None):
    if _LC_DEC_MODEL_CACHE.get("loaded"):
        return _LC_DEC_MODEL_CACHE
    models_dir = models_dir or "/Users/alex/Documents/ingame/ml-models"
    try:
        from catboost import CatBoostClassifier  # type: ignore
    except Exception as exc:
        _LC_DEC_MODEL_CACHE["loaded"] = True
        _LC_DEC_MODEL_CACHE["error"] = f"catboost import failed: {exc}"
        return _LC_DEC_MODEL_CACHE

    models = {}
    for lane in _LC_LANES:
        model_path = os.path.join(models_dir, f"lane_corrector_decisive_{lane}.cbm")
        meta_path = os.path.join(models_dir, f"lane_corrector_decisive_{lane}_meta.json")
        if not os.path.exists(model_path) or not os.path.exists(meta_path):
            continue
        try:
            model = CatBoostClassifier()
            model.load_model(model_path)
            with open(meta_path, "r") as f:
                meta = json.load(f)
            feature_cols = meta.get("feature_cols") or []
            raw_label_map = meta.get("label_map") or {0: "lose", 1: "win"}
            label_map = {}
            if isinstance(raw_label_map, dict):
                for k, v in raw_label_map.items():
                    try:
                        idx = int(k)
                    except Exception:
                        continue
                    if v in ("lose", "win"):
                        label_map[idx] = v
            if 0 not in label_map:
                label_map[0] = "lose"
            if 1 not in label_map:
                label_map[1] = "win"
            raw_cat_idx = meta.get("cat_idx")
            if isinstance(raw_cat_idx, list) and raw_cat_idx:
                cat_idx = []
                for idx in raw_cat_idx:
                    try:
                        cat_idx.append(int(idx))
                    except Exception:
                        continue
            else:
                cat_idx = [feature_cols.index(c) for c in _LC_CAT_COLS if c in feature_cols]
            models[lane] = {
                "model": model,
                "feature_cols": feature_cols,
                "cat_idx": cat_idx,
                "label_map": label_map,
            }
        except Exception as exc:
            _LC_DEC_MODEL_CACHE["error"] = f"lane_corrector_decisive load failed: {exc}"
            continue
    _LC_DEC_MODEL_CACHE["models"] = models
    _LC_DEC_MODEL_CACHE["loaded"] = True
    return _LC_DEC_MODEL_CACHE


def _lc_predict_from_row(lane, row, model_cache):
    info = (model_cache or {}).get("models", {}).get(lane)
    if not info:
        return None, None
    try:
        from catboost import Pool  # type: ignore
    except Exception:
        return None, None

    cols = info.get("feature_cols") or []
    if not cols:
        return None, None
    values = []
    for c in cols:
        v = row.get(c)
        if v is None:
            v = "none" if c in _LC_CAT_COLS else 0.0
        values.append(v)
    pool = Pool([values], cat_features=info.get("cat_idx") or [])
    try:
        probs = info["model"].predict_proba(pool)
    except Exception:
        return None, None
    if probs is None:
        return None, None
    try:
        if len(probs) == 0:
            return None, None
    except Exception:
        return None, None
    p = probs[0]
    try:
        if p is None or len(p) == 0:
            return None, None
    except Exception:
        return None, None
    best_idx = max(range(len(p)), key=lambda i: p[i])
    conf = int(round(float(p[best_idx]) * 100))
    label_map = info.get("label_map") or {0: "lose", 1: "draw", 2: "win"}
    outcome = label_map.get(best_idx)
    return outcome, conf


def _lc_predict_decisive_from_row(lane, row, dec_model_cache):
    info = (dec_model_cache or {}).get("models", {}).get(lane)
    if not info:
        return None, None, None
    try:
        from catboost import Pool  # type: ignore
    except Exception:
        return None, None, None

    cols = info.get("feature_cols") or []
    if not cols:
        return None, None, None
    values = []
    for c in cols:
        v = row.get(c)
        if v is None:
            v = "none" if c in _LC_CAT_COLS else 0.0
        values.append(v)
    pool = Pool([values], cat_features=info.get("cat_idx") or [])
    try:
        probs = info["model"].predict_proba(pool)
    except Exception:
        return None, None, None
    if probs is None:
        return None, None, None
    try:
        if len(probs) == 0:
            return None, None, None
    except Exception:
        return None, None, None
    p = probs[0]
    try:
        if p is None or len(p) == 0:
            return None, None, None
    except Exception:
        return None, None, None
    label_map = info.get("label_map") or {0: "lose", 1: "win"}
    win_idx = None
    lose_idx = None
    for idx, label in label_map.items():
        if label == "win":
            win_idx = idx
        elif label == "lose":
            lose_idx = idx
    if win_idx is None:
        win_idx = 1 if len(p) > 1 else 0
    if lose_idx is None:
        lose_idx = 0
    try:
        p_win = float(p[win_idx])
        p_lose = float(p[lose_idx])
    except Exception:
        return None, None, None
    if p_win >= p_lose:
        outcome = "win"
        prob = p_win
    else:
        outcome = "lose"
        prob = p_lose
    conf = int(round(prob * 100))
    delta = abs(float(p_win) - 0.5)
    return outcome, conf, delta


def _lc_build_lane_row(
    lane,
    radiant_heroes_and_pos,
    dire_heroes_and_pos,
    heroes_data,
    match_start_time,
    baseline_outcome,
    baseline_conf,
    rad_pos,
    dire_pos,
    player_stats,
    pair_stats,
    pair_hero_stats,
    team_lane_history,
    rad_team_id,
    dire_team_id,
    rad_roster,
    dire_roster,
):
    solo_data = heroes_data.get("solo_lanes", {}) if isinstance(heroes_data, dict) else {}
    v1_data = heroes_data.get("1v1_lanes", {}) if isinstance(heroes_data, dict) else {}
    v2_data = heroes_data.get("2v2_lanes", {}) if isinstance(heroes_data, dict) else {}
    sy_data = heroes_data.get("1_with_1_lanes", {}) if isinstance(heroes_data, dict) else {}

    p_win, p_draw, p_lose = _lc_pred_probs(baseline_outcome, baseline_conf)
    pred_score = _lc_expected_score(p_win, p_lose)

    rad_players = _lc_collect_lane_players(rad_pos, True, lane)
    dire_players = _lc_collect_lane_players(dire_pos, False, lane)
    if not rad_players or not dire_players:
        return None

    rad_heroes = [(p["hero_id"], p["pos"]) for p in rad_players]
    dire_heroes = [(p["hero_id"], p["pos"]) for p in dire_players]

    rad_solo = _lc_solo_stats_for_side(solo_data, rad_heroes)
    dire_solo = _lc_solo_stats_for_side(solo_data, dire_heroes)

    rad_feats = []
    for rp in rad_players:
        ps = _lc_get_player(player_stats, rp["account_id"])
        rad_feats.append(_lc_player_features(ps, rp["hero_id"], lane, rp["dota_plus_xp"]))

    dire_feats = []
    for dp in dire_players:
        ps = _lc_get_player(player_stats, dp["account_id"])
        dire_feats.append(_lc_player_features(ps, dp["hero_id"], lane, dp["dota_plus_xp"]))

    rad_agg = _lc_aggregate_players(rad_feats)
    dire_agg = _lc_aggregate_players(dire_feats)
    h2h_feats = _lc_lane_h2h_features(
        lane=lane,
        rad_players=rad_players,
        dire_players=dire_players,
        pair_stats=pair_stats,
        pair_hero_stats=pair_hero_stats,
    )
    rad_team_feats = _lc_team_lane_overlap_features(
        team_history=team_lane_history,
        team_id=rad_team_id,
        lane=lane,
        roster=rad_roster,
    )
    dire_team_feats = _lc_team_lane_overlap_features(
        team_history=team_lane_history,
        team_id=dire_team_id,
        lane=lane,
        roster=dire_roster,
    )

    row = {
        "lane": lane,
        "patch_bucket": _lc_patch_bucket(match_start_time),
        "pred_outcome": baseline_outcome or "none",
        "base_out": baseline_outcome or "none",
        "pred_conf": float(baseline_conf) if baseline_conf is not None else 0.0,
        "pred_p_win": float(p_win),
        "pred_p_draw": float(p_draw),
        "pred_p_lose": float(p_lose),
        "pred_score": float(pred_score),
    }

    r2v2_out, r2v2_conf = _lc_raw_2v2_signal(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane)
    r2v1_out, r2v1_conf, r2v1_status = _lc_raw_2v1_signal(
        radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane
    )
    cp_out, cp_conf = counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane) or (None, None)
    sy_out, sy_conf = synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane) or (None, None)

    _lc_add_pred_features(row, "raw2v2", r2v2_out, r2v2_conf)
    _lc_add_pred_features(row, "raw2v1", r2v1_out, r2v1_conf)
    _lc_add_pred_features(row, "rawcp", cp_out, cp_conf)
    _lc_add_pred_features(row, "rawsy", sy_out, sy_conf)
    row["raw2v1_status"] = r2v1_status or "none"

    for k, v in rad_solo.items():
        row[f"rad_solo_{k}"] = v
    for k, v in dire_solo.items():
        row[f"dire_solo_{k}"] = v
    for k in rad_solo:
        if k in dire_solo:
            row[f"diff_solo_{k}"] = float(rad_solo[k] - dire_solo[k])

    # Stomp priors
    if lane != "mid":
        r_duo = _lc_duo_key(rad_heroes)
        d_duo = _lc_duo_key(dire_heroes)
        canon_key, left_is_left = _canon_vs(r_duo, d_duo)
        entry = v2_data.get(canon_key)
        st = _lc_entry_stats(entry, invert=not left_is_left)
        if st:
            row["raw2v2_stomp_rate"] = st["stomp_rate"]
            row["raw2v2_stomp_balance"] = st["stomp_balance"]
        else:
            row["raw2v2_stomp_rate"] = 0.0
            row["raw2v2_stomp_balance"] = 0.0

        r_sy_key = f"{_lc_duo_key(rad_heroes).replace(',', '_with_', 1)}"
        d_sy_key = f"{_lc_duo_key(dire_heroes).replace(',', '_with_', 1)}"
        r_sy = _lc_entry_stats(sy_data.get(r_sy_key), invert=False)
        d_sy = _lc_entry_stats(sy_data.get(d_sy_key), invert=False)
        row["rawsy_stomp_rate"] = (r_sy["stomp_rate"] if r_sy else 0.0)
        row["rawsy_stomp_balance"] = (r_sy["stomp_balance"] if r_sy else 0.0)
        row["diff_sy_stomp_rate"] = (r_sy["stomp_rate"] if r_sy else 0.0) - (d_sy["stomp_rate"] if d_sy else 0.0)
        row["diff_sy_stomp_balance"] = (r_sy["stomp_balance"] if r_sy else 0.0) - (d_sy["stomp_balance"] if d_sy else 0.0)

        if lane == "top":
            matchups = [
                (f"{rad_heroes[0][0]}pos3", f"{dire_heroes[0][0]}pos1"),
                (f"{rad_heroes[0][0]}pos3", f"{dire_heroes[1][0]}pos5"),
                (f"{rad_heroes[1][0]}pos4", f"{dire_heroes[0][0]}pos1"),
                (f"{rad_heroes[1][0]}pos4", f"{dire_heroes[1][0]}pos5"),
            ]
        else:
            matchups = [
                (f"{rad_heroes[0][0]}pos1", f"{dire_heroes[0][0]}pos3"),
                (f"{rad_heroes[0][0]}pos1", f"{dire_heroes[1][0]}pos4"),
                (f"{rad_heroes[1][0]}pos5", f"{dire_heroes[0][0]}pos3"),
                (f"{rad_heroes[1][0]}pos5", f"{dire_heroes[1][0]}pos4"),
            ]
        stomp_rates = []
        stomp_balances = []
        for left, right in matchups:
            canon, left_is_left = _canon_vs(left, right)
            ent = v1_data.get(canon)
            st = _lc_entry_stats(ent, invert=not left_is_left)
            if st:
                stomp_rates.append((st["stomp_rate"], st["games"]))
                stomp_balances.append((st["stomp_balance"], st["games"]))
        row["rawcp_stomp_rate"] = _lc_weighted_mean(stomp_rates, 0.0)
        row["rawcp_stomp_balance"] = _lc_weighted_mean(stomp_balances, 0.0)
    else:
        left = f"{rad_heroes[0][0]}pos2"
        right = f"{dire_heroes[0][0]}pos2"
        canon, left_is_left = _canon_vs(left, right)
        ent = v1_data.get(canon)
        st = _lc_entry_stats(ent, invert=not left_is_left)
        row["rawcp_stomp_rate"] = st["stomp_rate"] if st else 0.0
        row["rawcp_stomp_balance"] = st["stomp_balance"] if st else 0.0
        row["raw2v2_stomp_rate"] = 0.0
        row["raw2v2_stomp_balance"] = 0.0
        row["rawsy_stomp_rate"] = 0.0
        row["rawsy_stomp_balance"] = 0.0
        row["diff_sy_stomp_rate"] = 0.0
        row["diff_sy_stomp_balance"] = 0.0

    for k, v in rad_agg.items():
        row[f"rad_{k}"] = v
    for k, v in dire_agg.items():
        row[f"dire_{k}"] = v
    for k in rad_agg:
        if k.endswith("_mean") and k in dire_agg:
            row[f"diff_{k}"] = float(rad_agg[k] - dire_agg[k])
    for k, v in h2h_feats.items():
        row[k] = float(v)
    for k, v in rad_team_feats.items():
        row[f"rad_{k}"] = float(v)
    for k, v in dire_team_feats.items():
        row[f"dire_{k}"] = float(v)
    for k in rad_team_feats:
        if k in dire_team_feats:
            row[f"diff_{k}"] = float(rad_team_feats[k] - dire_team_feats[k])

    return row


def _lc_env_bool(name, default):
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in ("0", "false", "off", "no")


def _lc_env_int(name, default):
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(float(raw))
    except Exception:
        return int(default)


def _lc_env_float(name, default):
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _lc_gate_param_int(base_name, lane, default):
    lane_key = f"{base_name}_{str(lane).upper()}"
    if os.getenv(lane_key) is not None:
        return _lc_env_int(lane_key, default)
    return _lc_env_int(base_name, default)


def _lc_dec_switch_delta(lane):
    lane = str(lane or "").lower()
    lane_default = _LC_DEC_SWITCH_DEFAULT.get(lane, 0.08)
    lane_key = f"LANE_CORRECTOR_DECISIVE_SWITCH_DELTA_{lane.upper()}"
    if os.getenv(lane_key) is not None:
        return _lc_env_float(lane_key, lane_default)
    if os.getenv("LANE_CORRECTOR_DECISIVE_SWITCH_DELTA") is not None:
        return _lc_env_float("LANE_CORRECTOR_DECISIVE_SWITCH_DELTA", lane_default)
    return lane_default


def _lc_apply_rule_b(lane, base_outcome, base_conf, model_outcome, model_conf):
    if model_outcome is None or model_conf is None:
        return base_outcome, base_conf
    if base_outcome is None or base_conf is None:
        return model_outcome, model_conf
    if base_outcome == model_outcome:
        return base_outcome, base_conf

    keep_base_conf = _lc_env_bool("LANE_CORRECTOR_KEEP_BASE_CONF", True)
    allow_to_draw = _lc_env_bool("LANE_CORRECTOR_ALLOW_TO_DRAW", False)
    default_min_model_conf = 55 if lane == "bot" else 0
    min_model_conf = _lc_gate_param_int("LANE_CORRECTOR_MIN_MODEL_CONF", lane, default_min_model_conf)
    min_margin = _lc_gate_param_int("LANE_CORRECTOR_MIN_MARGIN", lane, 0)
    draw_switch_conf = _lc_gate_param_int("LANE_CORRECTOR_DRAW_SWITCH_CONF", lane, 58)

    if model_conf < min_model_conf:
        return base_outcome, base_conf

    # Draws are intentionally treated conservatively: switching away from draw
    # needs a stronger model signal; switching to draw is disabled by default.
    if base_outcome == "draw" and model_outcome in ("win", "lose"):
        if model_conf >= max(draw_switch_conf, min_model_conf):
            return model_outcome, base_conf if keep_base_conf else model_conf
        return base_outcome, base_conf

    if model_outcome == "draw" and not allow_to_draw:
        return base_outcome, base_conf

    if model_conf >= (base_conf + min_margin):
        return model_outcome, base_conf if keep_base_conf else model_conf
    return base_outcome, base_conf


def _lc_format_lane_message(lane_name, outcome, conf):
    if conf is None or outcome is None:
        if lane_name == "top":
            return "Top: None\n"
        if lane_name == "mid":
            return "Mid: None\n"
        return "Bot: None\n\n"
    if lane_name == "top":
        return f"Top: {outcome} {conf}%\n"
    if lane_name == "mid":
        return f"Mid: {outcome} {conf}%\n"
    return f"Bot: {outcome} {conf}%\n\n"


def _lc_update_stats_after_match(
    match,
    baseline_preds,
    player_stats,
    pair_stats,
    pair_hero_stats,
    team_lane_history,
):
    players = match.get("players") or []
    lane_outcomes = {
        "top": _lc_lane_label_from_outcome(match.get("topLaneOutcome")),
        "mid": _lc_lane_label_from_outcome(match.get("midLaneOutcome")),
        "bot": _lc_lane_label_from_outcome(match.get("bottomLaneOutcome")),
    }
    radiant_win = 1 if bool(match.get("didRadiantWin")) else 0

    for p in players:
        pos = _lc_parse_pos(p.get("position"))
        if pos is None:
            continue
        is_radiant = bool(p.get("isRadiant"))
        sa = p.get("steamAccount") or {}
        account_id = _lc_coerce_int(sa.get("id"))
        hero_id = _lc_coerce_int(p.get("heroId"))
        if account_id <= 0 or hero_id <= 0:
            continue

        lane = _lc_lane_from_player(is_radiant, pos)
        if lane not in _LC_LANES:
            continue

        pstats = _lc_get_player(player_stats, account_id)
        team_win = radiant_win if is_radiant else 1 - radiant_win
        pstats.games += 1
        pstats.wins += int(team_win)

        hstats = _lc_get_hero(pstats, hero_id)
        hstats.games += 1
        hstats.wins += int(team_win)

        lane_actual = lane_outcomes.get(lane)
        if lane_actual:
            player_lane_actual = lane_actual if is_radiant else _lc_invert_outcome(lane_actual)
            pstats.lane_games[lane] += 1
            hstats.lane_games[lane] += 1
            if player_lane_actual == "win":
                pstats.lane_wins[lane] += 1.0
                hstats.lane_wins[lane] += 1.0
            elif player_lane_actual == "draw":
                pstats.lane_wins[lane] += 0.5
                hstats.lane_wins[lane] += 0.5

        pred_outcome, pred_conf = baseline_preds.get(lane, (None, None))
        if pred_outcome is not None and pred_conf is not None and lane_actual:
            player_pred = pred_outcome if is_radiant else _lc_invert_outcome(pred_outcome)
            player_lane_actual = lane_actual if is_radiant else _lc_invert_outcome(lane_actual)
            p_win, _, p_lose = _lc_pred_probs(player_pred, pred_conf)
            exp_score = _lc_expected_score(p_win, p_lose)
            act_score = _lc_actual_score(player_lane_actual)
            if act_score is not None:
                pst = pstats.pred[lane]
                pst.games += 1
                if player_pred == player_lane_actual:
                    pst.correct += 1
                pst.expected_sum += float(exp_score)
                pst.actual_sum += float(act_score)
                pst.conf_sum += float(pred_conf)

    rad_pos, dire_pos = _lc_build_player_maps(players)
    if rad_pos is None or dire_pos is None:
        return

    # Update player-vs-player and hero matchup history after current match is processed.
    for lane in _LC_LANES:
        lane_actual = lane_outcomes.get(lane)
        score_rad = _lc_lane_score(lane_actual)
        if score_rad is None:
            continue
        score_dire = _lc_lane_score(_lc_invert_outcome(lane_actual))
        if score_dire is None:
            continue
        rad_lane_players = _lc_collect_lane_players(rad_pos, True, lane)
        dire_lane_players = _lc_collect_lane_players(dire_pos, False, lane)
        for rp in rad_lane_players:
            rpid = _lc_coerce_int(rp.get("account_id"))
            rhid = _lc_coerce_int(rp.get("hero_id"))
            if rpid <= 0 or rhid <= 0:
                continue
            for dp in dire_lane_players:
                dpid = _lc_coerce_int(dp.get("account_id"))
                dhid = _lc_coerce_int(dp.get("hero_id"))
                if dpid <= 0 or dhid <= 0:
                    continue
                _lc_update_matchup_stats(pair_stats, (lane, rpid, dpid), score_rad)
                _lc_update_matchup_stats(pair_stats, (lane, dpid, rpid), score_dire)
                _lc_update_matchup_stats(pair_hero_stats, (lane, rpid, dpid, rhid, dhid), score_rad)
                _lc_update_matchup_stats(pair_hero_stats, (lane, dpid, rpid, dhid, rhid), score_dire)

    # Update team history snapshots for overlap>=4 features.
    rad_team_id = _lc_team_id(match.get("radiantTeam"))
    dire_team_id = _lc_team_id(match.get("direTeam"))
    rad_roster = frozenset(
        _lc_coerce_int(v.get("account_id")) for v in rad_pos.values() if _lc_coerce_int(v.get("account_id")) > 0
    )
    dire_roster = frozenset(
        _lc_coerce_int(v.get("account_id")) for v in dire_pos.values() if _lc_coerce_int(v.get("account_id")) > 0
    )
    if rad_team_id > 0 and rad_roster:
        lane_scores = {
            lane: score
            for lane, score in (
                (ln, _lc_lane_score(outcome))
                for ln, outcome in lane_outcomes.items()
            )
            if score is not None
        }
        if lane_scores:
            hist = team_lane_history.setdefault(rad_team_id, [])
            hist.append(_LCTeamLaneEntry(rad_roster, lane_scores))
            if len(hist) > 400:
                del hist[:-400]
    if dire_team_id > 0 and dire_roster:
        lane_scores = {
            lane: score
            for lane, score in (
                (ln, _lc_lane_score(_lc_invert_outcome(outcome)))
                for ln, outcome in lane_outcomes.items()
            )
            if score is not None
        }
        if lane_scores:
            hist = team_lane_history.setdefault(dire_team_id, [])
            hist.append(_LCTeamLaneEntry(dire_roster, lane_scores))
            if len(hist) > 400:
                del hist[:-400]


def _lc_predict_lanes_for_match(
    match,
    radiant_heroes_and_pos,
    dire_heroes_and_pos,
    heroes_data,
    baseline_messages,
    player_stats,
    pair_stats,
    pair_hero_stats,
    team_lane_history,
    models_dir=None,
):
    model_cache = _lc_load_models(models_dir=models_dir)
    if not model_cache.get("models"):
        return None
    dec_model_cache = _lc_load_decisive_models(models_dir=models_dir)
    use_decisive = _lc_env_bool("LANE_CORRECTOR_USE_DECISIVE", False)

    rad_pos, dire_pos = _lc_build_player_maps(match.get("players"))
    if rad_pos is None or dire_pos is None:
        return None
    rad_team_id = _lc_team_id(match.get("radiantTeam"))
    dire_team_id = _lc_team_id(match.get("direTeam"))
    rad_roster = frozenset(
        _lc_coerce_int(v.get("account_id")) for v in rad_pos.values() if _lc_coerce_int(v.get("account_id")) > 0
    )
    dire_roster = frozenset(
        _lc_coerce_int(v.get("account_id")) for v in dire_pos.values() if _lc_coerce_int(v.get("account_id")) > 0
    )

    base_preds = {
        "top": _lc_parse_lane_prediction(baseline_messages.get("top")),
        "mid": _lc_parse_lane_prediction(baseline_messages.get("mid")),
        "bot": _lc_parse_lane_prediction(baseline_messages.get("bot")),
    }

    corrected = {}
    match_start_time = _lc_coerce_int(match.get("startDateTime"))
    for lane in _LC_LANES:
        base_out, base_conf = base_preds.get(lane, (None, None))
        row = _lc_build_lane_row(
            lane=lane,
            radiant_heroes_and_pos=radiant_heroes_and_pos,
            dire_heroes_and_pos=dire_heroes_and_pos,
            heroes_data=heroes_data,
            match_start_time=match_start_time,
            baseline_outcome=base_out,
            baseline_conf=base_conf,
            rad_pos=rad_pos,
            dire_pos=dire_pos,
            player_stats=player_stats,
            pair_stats=pair_stats,
            pair_hero_stats=pair_hero_stats,
            team_lane_history=team_lane_history,
            rad_team_id=rad_team_id,
            dire_team_id=dire_team_id,
            rad_roster=rad_roster,
            dire_roster=dire_roster,
        )
        if row is None:
            corrected[lane] = (base_out, base_conf)
            continue
        if use_decisive and base_out in ("win", "lose"):
            dec_out, _dec_conf, dec_delta = _lc_predict_decisive_from_row(lane, row, dec_model_cache)
            if dec_out in ("win", "lose") and dec_delta is not None:
                if dec_delta >= _lc_dec_switch_delta(lane):
                    corrected[lane] = (dec_out, base_conf)
                else:
                    corrected[lane] = (base_out, base_conf)
                continue
        model_out, model_conf = _lc_predict_from_row(lane, row, model_cache)
        final_out, final_conf = _lc_apply_rule_b(lane, base_out, base_conf, model_out, model_conf)
        corrected[lane] = (final_out, final_conf)

    return corrected, base_preds

def calculate_lanes_old(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data):

    # Приводим плоский lane_dict к структурированному виду, если нужно
    if heroes_data is not None and isinstance(heroes_data, dict) and '2v2_lanes' not in heroes_data:
        heroes_data = structure_lane_dict(heroes_data)

    output, bot_key, bot_key_value, top_key, top_key_value, mid_key, mid_key_value = {}, None, None, None, None, None, None
    cp_full_2v1_top = None
    cp_full_2v1_bot = None

    # === TOP lane: 2v2 -> 1v2 (с синергией) -> 1v1 (с синергией) ===
    top_output_2v2 = {}
    lane_2vs2(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, top_output_2v2)
    if top_output_2v2.get('top'):
        top_key, top_key_value = find_biggest_param(top_output_2v2['top'])

    if top_key_value is None:
        top2vs1 = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                            heroes_data=heroes_data, lane='top')
        cp_res_top = None
        cp_full_2v1_top = False
        if all(len(line) == 2 for line in
               [top2vs1.get('top_radiant', {}).get('win', {}), top2vs1.get('top_dire', {}).get('win', {})]):
            tmp = {}
            cp_res_top = both_found(lane='top', data=top2vs1, output=tmp)
            cp_full_2v1_top = True

        synergy_top = synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'top')
        if cp_res_top is None:
            cp_res_top = _single_side_2v1_prediction(top2vs1, 'top')
            cp_full_2v1_top = False

        top_base = None
        if cp_res_top is not None:
            if cp_full_2v1_top:
                top_base = cp_res_top  # без синергии
            else:
                merged_top = _merge_lane_predictions(cp_res_top, synergy_top)
                top_base = merged_top if merged_top[1] is not None else cp_res_top
        if top_base is not None and top_base[1] is not None:
            top_key, top_key_value = top_base

    if top_key_value is None:
        counterpick_top = counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'top')
        synergy_top = synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'top')
        top_key, top_key_value = _merge_lane_predictions(counterpick_top, synergy_top)
    elif cp_full_2v1_top is not None and not cp_full_2v1_top:
        counterpick_top = counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'top')
        merged = _merge_lane_predictions((top_key, top_key_value), counterpick_top)
        if merged[1] is not None:
            top_key, top_key_value = merged
    elif cp_full_2v1_top is not None and not cp_full_2v1_top:
        # Был только один бокс 2v1: усиливаем 1v1
        counterpick_top = counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'top')
        top_key, top_key_value = _merge_lane_predictions((top_key, top_key_value), counterpick_top)

    # === BOT lane: 2v2 -> 1v2 (с синергией) -> 1v1 (с синергией) ===
    bot_output_2v2 = {}
    lane_2vs2(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, bot_output_2v2)
    if bot_output_2v2.get('bot'):
        bot_key, bot_key_value = find_biggest_param(bot_output_2v2['bot'])

    if bot_key_value is None:
        bot2vs1 = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                            heroes_data=heroes_data, lane='bot')
        cp_res_bot = None
        cp_full_2v1_bot = False
        if all(len(line) == 2 for line in [bot2vs1.get('bot_radiant', {}).get('win', {}), bot2vs1.get('bot_dire', {}).get('win', {})]):
            tmp = {}
            cp_res_bot = both_found(lane='bot', data=bot2vs1, output=tmp)
            cp_full_2v1_bot = True

        synergy_bot = synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'bot')
        if cp_res_bot is None:
            cp_res_bot = _single_side_2v1_prediction(bot2vs1, 'bot')
            cp_full_2v1_bot = False

        bot_base = None
        if cp_res_bot is not None:
            if cp_full_2v1_bot:
                bot_base = cp_res_bot
            else:
                merged_bot = _merge_lane_predictions(cp_res_bot, synergy_bot)
                bot_base = merged_bot if merged_bot[1] is not None else cp_res_bot
        if bot_base is not None and bot_base[1] is not None:
            bot_key, bot_key_value = bot_base

    if bot_key_value is None:
        counterpick_bot = counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'bot')
        synergy_bot = synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'bot')
        bot_key, bot_key_value = _merge_lane_predictions(counterpick_bot, synergy_bot)
    elif cp_full_2v1_bot is not None and not cp_full_2v1_bot:
        counterpick_bot = counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'bot')
        merged = _merge_lane_predictions((bot_key, bot_key_value), counterpick_bot)
        if merged[1] is not None:
            bot_key, bot_key_value = merged
    elif cp_full_2v1_bot is not None and not cp_full_2v1_bot:
        # Был только один бокс 2v1: усиливаем 1v1
        counterpick_bot = counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, 'bot')
        bot_key, bot_key_value = _merge_lane_predictions((bot_key, bot_key_value), counterpick_bot)



    mid_output = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                           heroes_data=heroes_data, lane='mid')
    if mid_output:
        mid_key, mid_key_value = find_biggest_param(
            mid_output['mid_radiant'], mid=True)



    if top_key_value is None:
        top_message = 'Top: None\n'
    else:
        top_message = f'Top: {top_key} {top_key_value}%\n'
    if bot_key_value is None:
        bot_message = 'Bot: None\n\n'
    else:
        bot_message = f'Bot: {bot_key} {bot_key_value}%\n\n'
    if mid_key_value is None:
        mid_message = 'Mid: None\n'
    else:
        mid_message = f'Mid: {mid_key} {mid_key_value}%\n'
    return top_message, bot_message, mid_message


def calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, merge_side_lanes: bool = False):
    """
    Человекочитаемый пайплайн лейнов:
    1) Пробуем 2v2 матчап.
    2) 2v1: если оба бокса есть — берём чистый контрпик; если один — мешаем с дуо-синергией и усиливаем 1v1.
    3) Фолбэк: 1v1 + синергия.
    
    Args:
        merge_side_lanes: если True, при отсутствии данных для конкретного лайна
                         пробуем найти те же hero_id с позициями другого бокового лайна
    """

    # Приводим плоский lane_dict к структурированному виду, если нужно
    if heroes_data is not None and isinstance(heroes_data, dict) and '2v2_lanes' not in heroes_data:
        heroes_data = structure_lane_dict(heroes_data)

    # --- STOMP integration (optional via env) ---
    _stomp_mode = (os.getenv("LANE_STOMP_MODE") or "off").strip().lower()
    if _stomp_mode in ("0", "false", "off", "no"):
        _stomp_mode = "off"
    _stomp_effective = _stomp_mode
    if _stomp_mode.startswith("post_"):
        _stomp_effective = _stomp_mode.split("post_", 1)[1] or "boost"
    _stomp_min_games = int(os.getenv("LANE_STOMP_MIN_GAMES", "12"))
    _stomp_min_stomps = int(os.getenv("LANE_STOMP_MIN_STOMPS", "3"))
    _stomp_balance_threshold = float(os.getenv("LANE_STOMP_BALANCE", "0.12"))
    _stomp_max_shift = float(os.getenv("LANE_STOMP_MAX_SHIFT", "8"))
    _stomp_boost_k = float(os.getenv("LANE_STOMP_BOOST_K", "0.5"))
    _stomp_tie_conf = int(os.getenv("LANE_STOMP_TIE_CONF", "55"))
    _stomp_flip_threshold = float(os.getenv("LANE_STOMP_FLIP", "0.22"))
    _stomp_win_weight = float(os.getenv("LANE_STOMP_WIN_WEIGHT", "1.0"))
    _stomp_min_conf = int(os.getenv("LANE_STOMP_MIN_CONF", os.getenv("LANE_MIN_CONFIDENCE", "1")))

    def _stomp_stats(entry, invert=False):
        if not entry or not isinstance(entry, dict):
            return None
        g = int(entry.get("games", 0) or 0)
        if g < _stomp_min_games:
            return None
        sw = int(entry.get("stomp_win", 0) or 0)
        sl = int(entry.get("stomp_lose", 0) or 0)
        if invert:
            sw, sl = sl, sw
        stomp_total = sw + sl
        if stomp_total < _stomp_min_stomps:
            return None
        balance = ((sw - sl) * _stomp_win_weight) / g if g > 0 else 0.0
        rate = stomp_total / g if g > 0 else 0.0
        return balance, rate, g

    def _balance_2v2(lane_name):
        if lane_name == "mid":
            return None
        data_2v2 = heroes_data.get("2v2_lanes", {}) if isinstance(heroes_data, dict) else {}
        if not data_2v2:
            return None
        if lane_name == "top":
            left_parts = [
                f"{radiant_heroes_and_pos['pos3']['hero_id']}pos3",
                f"{radiant_heroes_and_pos['pos4']['hero_id']}pos4",
            ]
            right_parts = [
                f"{dire_heroes_and_pos['pos1']['hero_id']}pos1",
                f"{dire_heroes_and_pos['pos5']['hero_id']}pos5",
            ]
        else:  # bot
            left_parts = [
                f"{radiant_heroes_and_pos['pos1']['hero_id']}pos1",
                f"{radiant_heroes_and_pos['pos5']['hero_id']}pos5",
            ]
            right_parts = [
                f"{dire_heroes_and_pos['pos3']['hero_id']}pos3",
                f"{dire_heroes_and_pos['pos4']['hero_id']}pos4",
            ]
        left_sorted = ",".join(sorted(left_parts))
        right_sorted = ",".join(sorted(right_parts))
        canon_key, left_is_canon = _canon_vs(left_sorted, right_sorted)
        stats = data_2v2.get(canon_key)
        st = _stomp_stats(stats, invert=not left_is_canon)
        if not st:
            return None
        return st[0]

    def _balance_1v1(lane_name):
        data_1v1 = heroes_data.get("1v1_lanes", {}) if isinstance(heroes_data, dict) else {}
        if not data_1v1:
            return None
        pairs = []
        if lane_name == "mid":
            left = f"{radiant_heroes_and_pos['pos2']['hero_id']}pos2"
            right = f"{dire_heroes_and_pos['pos2']['hero_id']}pos2"
            canon_key, left_is_canon = _canon_vs(left, right)
            st = _stomp_stats(data_1v1.get(canon_key), invert=not left_is_canon)
            if st:
                return st[0]
            return None
        if lane_name == "top":
            matchups = [
                (f"{radiant_heroes_and_pos['pos3']['hero_id']}pos3", f"{dire_heroes_and_pos['pos1']['hero_id']}pos1"),
                (f"{radiant_heroes_and_pos['pos3']['hero_id']}pos3", f"{dire_heroes_and_pos['pos5']['hero_id']}pos5"),
                (f"{radiant_heroes_and_pos['pos4']['hero_id']}pos4", f"{dire_heroes_and_pos['pos1']['hero_id']}pos1"),
                (f"{radiant_heroes_and_pos['pos4']['hero_id']}pos4", f"{dire_heroes_and_pos['pos5']['hero_id']}pos5"),
            ]
        else:  # bot
            matchups = [
                (f"{radiant_heroes_and_pos['pos1']['hero_id']}pos1", f"{dire_heroes_and_pos['pos3']['hero_id']}pos3"),
                (f"{radiant_heroes_and_pos['pos1']['hero_id']}pos1", f"{dire_heroes_and_pos['pos4']['hero_id']}pos4"),
                (f"{radiant_heroes_and_pos['pos5']['hero_id']}pos5", f"{dire_heroes_and_pos['pos3']['hero_id']}pos3"),
                (f"{radiant_heroes_and_pos['pos5']['hero_id']}pos5", f"{dire_heroes_and_pos['pos4']['hero_id']}pos4"),
            ]
        vals = []
        for left, right in matchups:
            canon_key, left_is_canon = _canon_vs(left, right)
            st = _stomp_stats(data_1v1.get(canon_key), invert=not left_is_canon)
            if st:
                balance, _rate, games = st
                vals.append((balance, games))
        if not vals:
            return None
        num = sum(v * w for v, w in vals)
        den = sum(w for _, w in vals)
        return num / den if den > 0 else None

    def _balance_synergy(lane_name):
        data_sy = heroes_data.get("1_with_1_lanes", {}) if isinstance(heroes_data, dict) else {}
        if not data_sy or lane_name == "mid":
            return None
        if lane_name == "top":
            r_key = ",".join(sorted([
                f"{radiant_heroes_and_pos['pos3']['hero_id']}pos3",
                f"{radiant_heroes_and_pos['pos4']['hero_id']}pos4",
            ]))
            d_key = ",".join(sorted([
                f"{dire_heroes_and_pos['pos1']['hero_id']}pos1",
                f"{dire_heroes_and_pos['pos5']['hero_id']}pos5",
            ]))
        else:  # bot
            r_key = ",".join(sorted([
                f"{radiant_heroes_and_pos['pos1']['hero_id']}pos1",
                f"{radiant_heroes_and_pos['pos5']['hero_id']}pos5",
            ]))
            d_key = ",".join(sorted([
                f"{dire_heroes_and_pos['pos3']['hero_id']}pos3",
                f"{dire_heroes_and_pos['pos4']['hero_id']}pos4",
            ]))
        r_key = r_key.replace(",", "_with_", 1)
        d_key = d_key.replace(",", "_with_", 1)
        r_st = _stomp_stats(data_sy.get(r_key), invert=False)
        d_st = _stomp_stats(data_sy.get(d_key), invert=False)
        if not r_st or not d_st:
            return None
        return (r_st[0] - d_st[0])

    def _lane_balance(lane_name, source):
        if _stomp_mode == "off":
            return None
        if source in ("2v2", "2v2m"):
            return _balance_2v2(lane_name)
        # fallback: 1v1 first, then synergy
        b1 = _balance_1v1(lane_name)
        if b1 is not None:
            return b1
        return _balance_synergy(lane_name)

    def _apply_stomp(lane_name, outcome, conf, source):
        if _stomp_mode == "off":
            return outcome, conf
        if outcome is None or conf is None:
            return outcome, conf
        if _stomp_mode.startswith("post") and source != "final":
            return outcome, conf
        balance = _lane_balance(lane_name, source)
        if balance is None:
            return outcome, conf

        if _stomp_effective == "boost":
            if outcome in ("win", "lose"):
                if conf < _stomp_min_conf:
                    return outcome, conf
                adj = max(-_stomp_max_shift, min(_stomp_max_shift, balance * 100.0 * _stomp_boost_k))
                new_conf = conf + (adj if outcome == "win" else -adj)
                if new_conf < _stomp_min_conf:
                    return outcome, conf
                new_conf = int(max(1, min(100, round(new_conf))))
                return outcome, new_conf
            return outcome, conf

        if _stomp_effective == "draw":
            if outcome == "draw" and abs(balance) >= _stomp_balance_threshold:
                new_out = "win" if balance > 0 else "lose"
                new_conf = int(max(conf, round(50 + abs(balance) * 100.0 * _stomp_boost_k)))
                new_conf = int(max(1, min(100, new_conf)))
                return new_out, new_conf
            return outcome, conf

        if _stomp_effective == "tiebreak":
            if outcome == "draw" or conf <= _stomp_tie_conf:
                if abs(balance) >= _stomp_balance_threshold:
                    new_out = "win" if balance > 0 else "lose"
                    new_conf = int(max(conf, round(50 + abs(balance) * 100.0 * _stomp_boost_k)))
                    new_conf = int(max(1, min(100, new_conf)))
                    return new_out, new_conf
            return outcome, conf

        if _stomp_effective == "block":
            if outcome in ("win", "lose") and abs(balance) >= _stomp_balance_threshold:
                conflict = (outcome == "win" and balance < 0) or (outcome == "lose" and balance > 0)
                if conflict:
                    if abs(balance) >= _stomp_flip_threshold:
                        return ("lose" if outcome == "win" else "win"), conf
                    return "draw", int(max(50, min(60, conf)))
            return outcome, conf

        return outcome, conf

    def from_2v2(lane_name):
        """Возвращает (outcome, confidence) из 2v2 словаря, либо (None, None)."""
        bucket = {}
        lane_2vs2(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, bucket)
        if lane_name in bucket and bucket[lane_name]:
            key, val = find_biggest_param(bucket[lane_name])
            return _apply_stomp(lane_name, key, val, "2v2")
        return None, None

    def from_2v2_merged(lane_name):
        """
        Fallback для 2v2: ищем те же hero_id но с ЛЮБЫМИ перестановками позиций внутри дуо.
        Например: Sven(pos1)+Lich(pos5) и Sven(pos5)+Lich(pos1) считаются одним матчапом.
        """
        if lane_name == 'mid':
            return None, None
        
        data_2vs2 = heroes_data.get('2v2_lanes', {})
        if not data_2vs2:
            return None, None
        
        # Получаем hero_id для текущего лайна
        if lane_name == 'top':
            r_h1, r_h2 = radiant_heroes_and_pos['pos3']['hero_id'], radiant_heroes_and_pos['pos4']['hero_id']
            d_h1, d_h2 = dire_heroes_and_pos['pos1']['hero_id'], dire_heroes_and_pos['pos5']['hero_id']
        else:  # bot
            r_h1, r_h2 = radiant_heroes_and_pos['pos1']['hero_id'], radiant_heroes_and_pos['pos5']['hero_id']
            d_h1, d_h2 = dire_heroes_and_pos['pos3']['hero_id'], dire_heroes_and_pos['pos4']['hero_id']
        
        wins = draws = losses = games = 0
        stomp_w = stomp_l = 0
        seen_keys = set()
        
        # Форматы позиций для каждого лайна
        lane_formats = [
            (('pos3', 'pos4'), ('pos1', 'pos5')),  # TOP
            (('pos1', 'pos5'), ('pos3', 'pos4')),  # BOT
        ]
        
        # Все перестановки героев внутри каждого дуо
        # radiant: (r_h1, r_h2) и (r_h2, r_h1)
        # dire: (d_h1, d_h2) и (d_h2, d_h1)
        radiant_hero_perms = [(r_h1, r_h2), (r_h2, r_h1)]
        dire_hero_perms = [(d_h1, d_h2), (d_h2, d_h1)]
        
        for (r_p1, r_p2), (d_p1, d_p2) in lane_formats:
            for r_ha, r_hb in radiant_hero_perms:
                for d_ha, d_hb in dire_hero_perms:
                    left_parts = [f'{r_ha}{r_p1}', f'{r_hb}{r_p2}']
                    right_parts = [f'{d_ha}{d_p1}', f'{d_hb}{d_p2}']
                    left_sorted = ",".join(sorted(left_parts))
                    right_sorted = ",".join(sorted(right_parts))
                    canon_key, left_is_canon = _canon_vs(left_sorted, right_sorted)

                    if canon_key in seen_keys:
                        continue
                    seen_keys.add(canon_key)

                    stats = data_2vs2.get(canon_key, {})
                    invert = not left_is_canon
                    if not stats:
                        key = f'{left_parts[0]},{left_parts[1]}_vs_{right_parts[0]},{right_parts[1]}'
                        stats = data_2vs2.get(key, {})
                        invert = False
                        if not stats:
                            rev_key = f'{right_parts[0]},{right_parts[1]}_vs_{left_parts[0]},{left_parts[1]}'
                            stats = data_2vs2.get(rev_key, {})
                            invert = True

                    if isinstance(stats, dict) and stats.get('games', 0) > 0:
                        g = int(stats.get('games', 0))
                        w = int(stats.get('wins', 0))
                        d = int(stats.get('draws', 0))
                        l = max(0, g - w - d)
                        sw = int(stats.get('stomp_win', 0) or 0)
                        sl = int(stats.get('stomp_lose', 0) or 0)
                        if invert:
                            w, l = l, w
                            sw, sl = sl, sw
                        w, d, l = _apply_stomp_weighted_counts(w, d, l, stats, invert=invert)
                        wins += w
                        draws += d
                        losses += l
                        stomp_w += sw
                        stomp_l += sl
                        games += g
                    
                    # Обратный ключ: dire vs radiant (инвертируем результат)
                    rev_key = f'{d_ha}{r_p1},{d_hb}{r_p2}_vs_{r_ha}{d_p1},{r_hb}{d_p2}'
                    if rev_key not in seen_keys:
                        seen_keys.add(rev_key)
                        rev_stats = data_2vs2.get(rev_key, {})
                        if isinstance(rev_stats, dict) and rev_stats.get('games', 0) > 0:
                            g = int(rev_stats.get('games', 0))
                            w = int(rev_stats.get('wins', 0))
                            d = int(rev_stats.get('draws', 0))
                            l = max(0, g - w - d)
                            sw = int(rev_stats.get('stomp_win', 0) or 0)
                            sl = int(rev_stats.get('stomp_lose', 0) or 0)
                            w, d, l = _apply_stomp_weighted_counts(w, d, l, rev_stats, invert=False)
                            wins += l
                            draws += d
                            losses += w
                            stomp_w += sl
                            stomp_l += sw
                            games += g
        
        if games < 6:
            return None, None
        
        alpha = 1.0
        denom = games + 3.0 * alpha
        win = (wins + alpha) / denom if denom > 0 else 0
        draw = (draws + alpha) / denom if denom > 0 else 0
        lose = (losses + alpha) / denom if denom > 0 else 0
        
        total = lose + win + draw
        if total <= 0:
            return None, None
        
        key, val = find_biggest_param({
            'win': win / total * 100,
            'draw': draw / total * 100,
            'lose': lose / total * 100,
        })
        # merged balance (optional)
        if _stomp_mode != "off" and games >= _stomp_min_games:
            stomp_total = stomp_w + stomp_l
            if stomp_total >= _stomp_min_stomps:
                balance = ((stomp_w - stomp_l) * _stomp_win_weight) / games if games > 0 else 0.0
                key, val = _apply_stomp(lane_name, key, val, "2v2m")
        return key, val

    def from_2v1(lane_name):
        """
        Возвращает ((outcome, confidence), status):
        - status 'full' если есть оба бокса 2v1,
        - 'single' если найден только один бокс,
        - 'mid' для mid 1v1 bucket,
        - None если данных нет.
        """
        layer = lane_2vs1(radiant=radiant_heroes_and_pos, dire=dire_heroes_and_pos,
                          heroes_data=heroes_data, lane=lane_name)
        if lane_name == 'mid':
            mid_stats = layer.get('mid_radiant')
            if mid_stats:
                key, val = find_biggest_param(mid_stats, mid=True)
                key, val = _apply_stomp(lane_name, key, val, "2v1")
                return (key, val), 'mid'
            return (None, None), None

        has_both_boxes = all(len(line) == 2 for line in [
            layer.get(f'{lane_name}_radiant', {}).get('win', {}),
            layer.get(f'{lane_name}_dire', {}).get('win', {})
        ])
        if has_both_boxes:
            tmp = {}
            return both_found(lane=lane_name, data=layer, output=tmp), 'full'

        single_prediction = _single_side_2v1_prediction(layer, lane_name)
        return single_prediction if single_prediction is not None else (None, None), ('single' if single_prediction else None)

    def from_solo(lane_name):
        solo_data = heroes_data.get('solo_lanes', {}) if isinstance(heroes_data, dict) else {}
        if not solo_data:
            return None, None

        alpha = 1.0

        def _side_probs(keys):
            win_vals = []
            draw_vals = []
            lose_vals = []

            for k in keys:
                stats = solo_data.get(k, {})

                if isinstance(stats, dict) and 'games' in stats:
                    games = int(stats.get('games', 0) or 0)
                    if games < 10:
                        continue
                    wins = int(stats.get('wins', 0) or 0)
                    draws = int(stats.get('draws', 0) or 0)
                    losses = max(0, games - wins - draws)
                else:
                    value = stats.get('value', []) if isinstance(stats, dict) else []
                    if len(value) < 10:
                        continue
                    games = len(value)
                    wins = value.count(1)
                    draws = value.count(0)
                    losses = value.count(-1)

                denom = games + 3.0 * alpha
                if denom <= 0:
                    continue

                win = (wins + alpha) / denom
                draw = (draws + alpha) / denom
                lose = (losses + alpha) / denom

                win_vals.append((win, games))
                draw_vals.append((draw, games))
                lose_vals.append((lose, games))

            if not win_vals:
                return None

            win = multiply_list(win_vals, result=0.0)
            draw = multiply_list(draw_vals, result=0.0)
            lose = multiply_list(lose_vals, result=0.0)
            total = win + draw + lose
            if total <= 0:
                return None
            return win / total, draw / total, lose / total

        if lane_name == 'top':
            r_keys = [
                f"{radiant_heroes_and_pos['pos3']['hero_id']}pos3",
                f"{radiant_heroes_and_pos['pos4']['hero_id']}pos4",
            ]
            d_keys = [
                f"{dire_heroes_and_pos['pos1']['hero_id']}pos1",
                f"{dire_heroes_and_pos['pos5']['hero_id']}pos5",
            ]
        elif lane_name == 'bot':
            r_keys = [
                f"{radiant_heroes_and_pos['pos1']['hero_id']}pos1",
                f"{radiant_heroes_and_pos['pos5']['hero_id']}pos5",
            ]
            d_keys = [
                f"{dire_heroes_and_pos['pos3']['hero_id']}pos3",
                f"{dire_heroes_and_pos['pos4']['hero_id']}pos4",
            ]
        elif lane_name == 'mid':
            r_keys = [f"{radiant_heroes_and_pos['pos2']['hero_id']}pos2"]
            d_keys = [f"{dire_heroes_and_pos['pos2']['hero_id']}pos2"]
        else:
            return None, None

        r = _side_probs(r_keys)
        d = _side_probs(d_keys)
        if r is None or d is None:
            return None, None

        win = r[0] * d[2]
        draw = r[1] * d[1]
        lose = r[2] * d[0]
        total = win + draw + lose
        if total <= 0:
            return None, None

        return find_biggest_param({
            'win': win / total * 100,
            'draw': draw / total * 100,
            'lose': lose / total * 100,
        })

    def process_lane(lane_name):
        CONSENSUS_MIN_CONF = 55

        def _consensus_gate(primary):
            """Если 2v2/2v1 расходится с 1v1+synergy, уходим в draw при достаточной уверенности."""
            def _strip(res):
                if not res or not isinstance(res, (tuple, list)) or len(res) < 2:
                    return res
                return res[0], res[1]
            if not primary or primary[1] is None:
                return _strip(primary)
            primary_outcome, primary_conf = primary
            if primary_outcome not in ('win', 'lose'):
                return _strip(primary)
            lane_1v1 = counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane_name)
            duo_synergy = synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane_name)
            fallback = _merge_lane_predictions(lane_1v1, duo_synergy)
            if not fallback or fallback[1] is None:
                return _strip(primary)
            fb_outcome, fb_conf = fallback
            if fb_outcome not in ('win', 'lose'):
                return _strip(primary)
            if primary_outcome != fb_outcome and min(primary_conf, fb_conf) >= CONSENSUS_MIN_CONF:
                return 'draw', int(round((primary_conf + fb_conf) / 2))
            return _strip(primary)

        def _finalize(res):
            if not res or res[1] is None:
                return res
            if _stomp_mode.startswith("post"):
                key, val = _apply_stomp(lane_name, res[0], res[1], "final")
                return key, val
            return res

        # 1) 2v2 обычный
        two_by_two_outcome, two_by_two_conf = from_2v2(lane_name)
        if two_by_two_conf is not None:
            return _finalize(_consensus_gate((two_by_two_outcome, two_by_two_conf)))
        
        # 1.5) 2v2 merged (перестановки позиций внутри дуо) — только для боковых лайнов
        if merge_side_lanes and lane_name != 'mid':
            merged_2v2_outcome, merged_2v2_conf = from_2v2_merged(lane_name)
            if merged_2v2_conf is not None:
                return _finalize(_consensus_gate((merged_2v2_outcome, merged_2v2_conf)))

        # 2) 2v1
        counterpick_res, status = from_2v1(lane_name)
        counterpick_outcome = counterpick_res[0] if isinstance(counterpick_res, (tuple, list)) and len(counterpick_res) >= 1 else None
        counterpick_conf = counterpick_res[1] if isinstance(counterpick_res, (tuple, list)) and len(counterpick_res) >= 2 else None

        if status == 'mid' and counterpick_conf is not None:
            return _finalize((counterpick_outcome, counterpick_conf))
        if status == 'full' and counterpick_conf is not None:
            return _finalize(_consensus_gate(counterpick_res))

        if status == 'single' and counterpick_conf is not None:
            duo_synergy = synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane_name)
            merged = _merge_lane_predictions(counterpick_res, duo_synergy)
            base = merged if merged[1] is not None else (counterpick_outcome, counterpick_conf)
            lane_1v1 = counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane_name)
            boosted = _merge_lane_predictions(base, lane_1v1)
            res = boosted if boosted[1] is not None else base
            return _finalize(res)

        # 3) Фолбэк 1v1 + синергия
        lane_1v1 = counterpick_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane_name)
        duo_synergy = synergy_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, heroes_data, lane_name)
        merged = _merge_lane_predictions(lane_1v1, duo_synergy)
        if merged[1] is not None:
            return _finalize(merged)

        solo = from_solo(lane_name)
        if solo[1] is not None:
            return _finalize(solo)
        return (None, None)

    top_key, top_val = process_lane('top')
    bot_key, bot_val = process_lane('bot')
    mid_key, mid_val = process_lane('mid')

    top_message = f'Top: {top_key} {top_val}%\n' if top_val is not None else 'Top: None\n'
    bot_message = f'Bot: {bot_key} {bot_val}%\n\n' if bot_val is not None else 'Bot: None\n\n'
    mid_message = f'Mid: {mid_key} {mid_val}%\n' if mid_val is not None else 'Mid: None\n'

    return top_message, bot_message, mid_message


def is_moscow_night():
    moscow_tz = pytz.timezone("Europe/Moscow")
    now = datetime.datetime.now(moscow_tz)
    return 2 <= now.hour < 6


def sleep_until_morning():
    moscow_tz = pytz.timezone("Europe/Moscow")

    while True:
        now = datetime.datetime.now(moscow_tz)
        # Р¤РѕСЂРјРёСЂСѓРµРј РІСЂРµРјСЏ 07:00 С‚РµРєСѓС‰РµРіРѕ РґРЅСЏ
        morning = now.replace(hour=6, minute=0, second=0, microsecond=0)

        # Р•СЃР»Рё С‚РµРєСѓС‰РµРµ РІСЂРµРјСЏ СѓР¶Рµ 07:00 РёР»Рё РїРѕР·Р¶Рµ, РІС‹С…РѕРґРёРј РёР· С†РёРєР»Р°
        if now >= morning:
            print("РќР°СЃС‚СѓРїРёР»Рѕ СѓС‚СЂРѕ!")
            break

        # Р’С‹С‡РёСЃР»СЏРµРј РѕСЃС‚Р°РІС€РёРµСЃСЏ СЃРµРєСѓРЅРґС‹ РґРѕ 07:00
        remaining_seconds = (morning - now).total_seconds()
        # Р‘СѓРґРµРј СЃРїР°С‚СЊ РЅРµ Р±РѕР»СЊС€Рµ 60 СЃРµРєСѓРЅРґ Р·Р° СЂР°Р·, С‡С‚РѕР±С‹ С‡Р°СЃС‚Рѕ РїСЂРѕРІРµСЂСЏС‚СЊ РІСЂРµРјСЏ
        sleep_interval = min(remaining_seconds, 60)

        print(
            f"РЎРµР№С‡Р°СЃ {now.strftime('%H:%M:%S')} РїРѕ РњРѕСЃРєРІРµ. Р”Рѕ 06:00 РѕСЃС‚Р°Р»РѕСЃСЊ {int(remaining_seconds)} СЃРµРєСѓРЅРґ. Р—Р°СЃС‹РїР°РµРј РЅР° {int(sleep_interval)} СЃРµРєСѓРЅРґ.")
        time.sleep(sleep_interval)



def tm_kills(radiant_heroes_and_positions, dire_heroes_and_positions):
    output_data = {'dire_kills_duo': [], 'dire_kills_trio': [], 'dire_time_duo': [], 'dire_time_trio': [],
                   'radiant_kills_duo': [], 'radiant_kills_trio': [], 'radiant_time_duo': [], 'radiant_time_trio': []}
    # print('tm_kills')
    positions = ['1', '2', '3', '4', '5']
    radiant_time_unique_combinations, radiant_kills_unique_combinations, dire_kills_unique_combinations, \
        dire_time_unique_combinations = set(), set(), set(), set()
    with open('/Users/alex/Documents/bets_data/pro_heroes_data/total_time_kills_dict.txt') as f:
        data = json.load(f)['value']
    for pos in positions:
        # radiant_synergy
        hero_id = str(radiant_heroes_and_positions['pos' + pos]['hero_id'])
        time_data = data.get(hero_id, {}).get('pos' + pos, {}).get('total_time_duo', {})
        kills_data = data.get(hero_id, {}).get('pos' + pos, {}).get('total_kills_duo', {})
        for hero_data in [time_data, kills_data]:
            for pos2, item2 in radiant_heroes_and_positions.items():
                second_hero_id = str(item2['hero_id'])
                if second_hero_id == hero_id:
                    continue
                duo_data = hero_data.get(second_hero_id, {}).get(pos2, {})
                if len(duo_data.get('value', {})) >= 10:  # Увеличен порог с 2 до 10 для duo статистики
                    combo = tuple(sorted([hero_id, second_hero_id]))
                    if hero_data == time_data:
                        if combo not in radiant_time_unique_combinations:
                            radiant_time_unique_combinations.add(combo)
                            value = (sum(duo_data['value']) / len(duo_data['value'])) / 60
                            output_data['radiant_time_duo'].append(value)
                    elif hero_data == kills_data:
                        if combo not in radiant_kills_unique_combinations:
                            radiant_kills_unique_combinations.add(combo)
                            value = sum(duo_data['value']) / len(duo_data['value'])
                            output_data['radiant_kills_duo'].append(value)
                    # РўСЂРµС‚РёР№ РіРµСЂРѕР№
                    for pos3, item3 in radiant_heroes_and_positions.items():
                        third_hero_id = str(item3['hero_id'])
                        if third_hero_id not in [second_hero_id, hero_id]:
                            # РЎРѕР·РґР°С‘Рј РѕС‚СЃРѕСЂС‚РёСЂРѕРІР°РЅРЅС‹Р№ РєРѕСЂС‚РµР¶ РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂРѕРІ РіРµСЂРѕРµРІ РґР»СЏ СѓРЅРёРєР°Р»СЊРЅРѕСЃС‚Рё
                            combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                            if hero_data == time_data:
                                if combo not in radiant_time_unique_combinations:
                                    radiant_time_unique_combinations.add(combo)
                                    trio_data = duo_data.get('total_time_trio', {}).\
                                        get(third_hero_id, {}).get(pos3, {}).get('value', {})
                                    if len(trio_data):
                                        value = (sum(trio_data) / len(trio_data)) / 60
                                        output_data['radiant_time_trio'].append(value)
                            elif hero_data == kills_data:
                                if combo not in radiant_kills_unique_combinations:
                                    radiant_kills_unique_combinations.add(combo)
                                    trio_data = duo_data.get('total_kills_trio', {}).\
                                        get(third_hero_id, {}).get(pos3, 'value', {})
                                    if len(trio_data):
                                        value = sum(trio_data) / len(trio_data)
                                        output_data['radiant_kills_trio'].append(value)
        # dire_synergy
        hero_id = str(dire_heroes_and_positions['pos' + pos]['hero_id'])
        time_data = data.get(hero_id, {}).get('pos' + pos, {}).get('total_time_duo', {})
        kills_data = data.get(hero_id, {}).get('pos' + pos, {}).get('total_kills_duo', {})
        for hero_data in [time_data, kills_data]:
            for pos2, item2 in dire_heroes_and_positions.items():
                second_hero_id = str(item2['hero_id'])
                if second_hero_id == hero_id:
                    continue
                duo_data = hero_data.get(second_hero_id, {}).get(pos2, {})
                if len(duo_data.get('value', {})) >= 10:  # Увеличен порог с 2 до 10 для duo статистики
                    combo = tuple(sorted([hero_id, second_hero_id]))
                    if hero_data == time_data:
                        if combo not in dire_time_unique_combinations:
                            dire_time_unique_combinations.add(combo)
                            value = (sum(duo_data['value']) / len(duo_data['value'])) / 60
                            output_data['dire_time_duo'].append(value)
                    elif hero_data == kills_data:
                        if combo not in dire_kills_unique_combinations:
                            dire_kills_unique_combinations.add(combo)
                            value = sum(duo_data['value']) / len(duo_data['value'])
                            output_data['dire_kills_duo'].append(value)
                    # third_hero
                    for pos3, item3 in dire_heroes_and_positions.items():
                        third_hero_id = str(item3['hero_id'])
                        if third_hero_id not in [second_hero_id, hero_id]:
                            combo = tuple(sorted([hero_id, second_hero_id, third_hero_id]))
                            if hero_data == time_data:
                                if combo not in dire_time_unique_combinations:
                                    dire_time_unique_combinations.add(combo)
                                    trio_data = duo_data.get('total_time_trio', {}).get(third_hero_id, {}).get(pos3,
                                                                                                               {}).get(
                                        'value', {})
                                    if len(trio_data):
                                        value = (sum(trio_data) / len(trio_data)) / 60
                                        output_data['dire_time_trio'].append(value)
                            elif hero_data == kills_data:
                                if combo not in dire_kills_unique_combinations:
                                    dire_kills_unique_combinations.add(combo)
                                    trio_data = duo_data.get('total_kills_trio', {}).get(third_hero_id, {}).get(pos3,
                                                                                                                {}).get(
                                        'value', {})
                                    if len(trio_data):
                                        value = sum(trio_data) / len(trio_data)
                                        output_data['dire_kills_trio'].append(value)

    avg_time_trio = calculate_average(output_data['radiant_time_trio'] + output_data['dire_time_trio'])
    avg_kills_trio = calculate_average(output_data['radiant_kills_trio'] + output_data['dire_kills_trio'])
    avg_time_duo = calculate_average(output_data['radiant_time_duo'] + output_data['dire_time_duo'])
    avg_kills_duo = calculate_average(output_data['radiant_kills_duo'] + output_data['dire_kills_duo'])

    avg_kills = (avg_kills_trio + avg_kills_duo) / 2 if avg_kills_trio and avg_kills_duo else avg_kills_duo
    avg_time = (avg_time_duo + avg_time_trio) / 2 if avg_time_trio and avg_time_duo else avg_time_duo

    return round(avg_kills, 2), round(avg_time, 2)


def find_lowest(lst):
    if len(lst) > 0:
        c = lst[0]
        for foo in lst:
            if foo < c:
                c = foo
        return c
    return None


def sum_if_none(n1, n2):
    if all(i is None for i in [n1, n2]):
        return None
    if any(i is None for i in [n1, n2]):
        c = 0
        for i in [n1, n2]:
            if i is not None:
                c += i
        return c
    return (n1 + n2) / 2


def tm_kills_teams(radiant_heroes_and_pos, dire_heroes_and_pos, radiant_team_name, dire_team_name, min_len):
    # print('tm_kills')
    output_data, positions = {}, ['1', '2', '3', '4', '5']
    trslt = {
        'aurora': 'aurora gaming',
        'team waska': 'waska',
        'fusion': 'fusion esports',
        '1win team': '1win',
        'talon esports': 'talon',
        'passion ua': 'team hryvnia',
    }
    radiant_team_name = trslt[radiant_team_name] if radiant_team_name in trslt else radiant_team_name.lower()
    dire_team_name = trslt[dire_team_name] if dire_team_name in trslt else dire_team_name.lower()
    with open('./pro_heroes_data/total_time_kills_dict_teams.txt') as f:
        file_data = json.load(f)['teams']
    if not all(team in file_data for team in [radiant_team_name, dire_team_name]):
        if radiant_team_name not in file_data:
            print(f'{radiant_team_name} not in team list')
        if dire_team_name not in file_data:
            print(f'{dire_team_name} not in team list')
        return None
    for side_name, heroes_and_pos, team_name in [['radiant', radiant_heroes_and_pos, radiant_team_name], ['dire', dire_heroes_and_pos, dire_team_name]]:
        time_unique_combinations, kills_unique_combinations = set(), set()
        work_data = file_data[team_name]
        for pos in positions:
            hero_id = str(heroes_and_pos['pos' + pos]['hero_id'])
            data = work_data.get(hero_id, {}).get('pos' + pos, {})
            if not data:
                continue
            solo_time = data.get('solo_time', {}).get('value', {})
            if solo_time:
                output_data.setdefault(side_name, {}).setdefault('solo_time', [])
                output_data[side_name]['solo_time'] += solo_time
            solo_kills = data.get('solo_kills', {}).get('value', {})
            if solo_kills:
                output_data.setdefault(side_name, {}).setdefault('solo_kills', [])
                # output_data[side_name]['solo_kills'] += [sum(solo_kills)/len(solo_kills)]
                output_data[side_name]['solo_kills'] += solo_kills
            time_data = data.get('time_duo', {})
            kills_data = data.get('kills_duo', {})
            for hero_data in [time_data, kills_data]:
                for pos2, item in heroes_and_pos.items():
                    second_hero_id = str(item['hero_id'])
                    if second_hero_id == hero_id:
                        continue
                    duo_data = hero_data.get(second_hero_id, {}).get(pos2, {})
                    if len(duo_data.get('value', {})) > 0:
                        combo = tuple(sorted([hero_id, second_hero_id]))
                        if hero_data == time_data:
                            if combo not in time_unique_combinations:
                                time_unique_combinations.add(combo)
                                value = duo_data['value']
                                output_data.setdefault(side_name, {}).setdefault('time_duo', [])
                                output_data[side_name]['time_duo'] += value
                        elif hero_data == kills_data:
                            if combo not in kills_unique_combinations:
                                kills_unique_combinations.add(combo)
                                value = duo_data['value']
                                output_data.setdefault(side_name, {}).setdefault('kills_duo', [])
                                # output_data[side_name]['kills_duo'] += [sum(value)/len(value)]
                                output_data[side_name]['kills_duo'] += value
    r_solo_t = output_data.get('radiant', {}).get('solo_time', [])
    d_solo_t = output_data.get('dire', {}).get('solo_time', [])
    r_solo_k = output_data.get('radiant', {}).get('solo_kills', [])
    d_solo_k = output_data.get('dire', {}).get('solo_kills', [])
    r_duo_t = output_data.get('radiant', {}).get('time_duo', [])
    d_duo_t = output_data.get('dire', {}).get('time_duo', [])
    r_duo_k = output_data.get('radiant', {}).get('kills_duo', [])
    d_duo_k = output_data.get('dire', {}).get('kills_duo', [])
    def find_mediana(lst):
        lst = sorted(lst)
        lenght = len(lst)
        if len(lst) == 0:
            return None
        if lenght == 1:

            return lst[0]
        if lenght % 2 != 0:
            return lst[(lenght//2)+1]
        if lenght %2 == 0:
            return (lst[lenght//2] + lst[lenght//2-1])/2
        return None

    kills_mediana = find_mediana(r_solo_k+ d_solo_k + r_duo_k + d_duo_k)
    time_mediana = find_mediana(r_solo_t + d_solo_t + r_duo_t + d_duo_t)
    kills_average = sum(r_solo_k+ d_solo_k + r_duo_k + d_duo_k)/len(r_solo_k+ d_solo_k + r_duo_k + d_duo_k)
    time_average = sum(r_solo_t + d_solo_t + r_duo_t + d_duo_t)/len(r_solo_t + d_solo_t + r_duo_t + d_duo_t)

    if time_mediana is not None:
        time_mediana = time_mediana/60

    return kills_mediana, time_mediana, kills_average, time_average


if __name__ == '__main__':
    a = ['batrider', 'beastmaster', 'clockwerk', 'dawnbreaker', 'enigma', 'faceless void', 'magnus', 'puck', 'pudge', 'slardar', 'spirit breaker', 'tusk', 'vengeful spirit', 'warlock', 'winter wyvern']
    ids = []
    for name, hero_id in name_to_id.items():
        if name in a:
            ids.append(hero_id)
