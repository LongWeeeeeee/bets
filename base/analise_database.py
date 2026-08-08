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
import math
import os
from functools import lru_cache
from itertools import combinations
from pathlib import Path


def _env_bool(name, default=True):
    default_value = "1" if default else "0"
    raw = str(os.getenv(name, default_value)).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


# Early retains the neutral minute-10 sample. All/Post-lane includes every
# qualifying 20+ minute map by default; the environment variable remains an
# explicit opt-in for historical gated rebuilds.
ANALISE_EARLY_MINUTE10_GATE_ENABLED = _env_bool(
    "ANALISE_EARLY_MINUTE10_GATE_ENABLED", True
)
ANALISE_POST_LANE_MINUTE10_GATE_ENABLED = _env_bool(
    "ANALISE_POST_LANE_MINUTE10_GATE_ENABLED", False
)


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


def _canonical_group(*tokens):
    """Ключ неупорядоченной группы героев: ровно один порядок на набор.

    Раньше билдер писал каждую пару в обоих порядках (`A,B` и `B,A`) с
    одинаковыми инкрементами — 42% ключей словаря были побайтовыми копиями.
    Трио писались в порядке `r_items`, то есть в порядке игроков из JSON, и
    один и тот же набор растекался по перестановкам (82% трио имели близнецов,
    ~80% их выборки лежало в чужих ключах).

    Читатели (`_lookup_vs_winrate`, `_lookup_with_winrate`,
    `_lookup_unordered_combo_winrate`) перебирают перестановки и суммируют
    найденное, поэтому канонический порядок находится и в новых словарях, и в
    старых. Единственное исключение — `calculate_kills_window_advantage`,
    который собирает ключ напрямую; он канонизирован тем же `sorted`.
    """
    return ",".join(sorted(tokens))


def _draft_entries(by_pos):
    """[(токен, hero_id)] — токен собирается один раз на матч, а не в каждом цикле."""
    return [(f'{hero_id}pos{pos_num}', hero_id) for pos_num, hero_id in by_pos.items()]


# Старшие слои kills_window (1v2/2v1) прод-политика `core_1v1_with` не читает:
# она берёт 1v1 и with, а до fallback-цепочки доходит лишь когда оба пусты — на
# 3000 про-картах x 4 окна таких случаев 0. Слои стоят 100 записей на матч, но
# держим их собранными для политик first_hit/blend_all/best_abs и слоевых A/B.
# KILLS_WINDOW_BUILD_HIGH_ORDER=0 при ребилде убирает их (словарь ужимается
# c ~2x до ~150x, но слоевые эксперименты на таком словаре не поставить).
KILLS_WINDOW_BUILD_HIGH_ORDER = str(
    os.getenv("KILLS_WINDOW_BUILD_HIGH_ORDER", "1") or "1"
).strip().lower() not in ("", "0", "false", "no", "off")


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


def _append_lane_entry(target_dict, key, value, kills10_diff=None):
    """Append the lane outcome and, when available, the team kills@10 result.

    ``kills10_diff`` is always expressed from the side at the start of ``key``.
    Keeping the kill counters beside the existing lane counters avoids storing
    millions of duplicate draft keys.
    """
    _append_to_dict(target_dict, key, value)
    if kills10_diff is None:
        return
    stats = target_dict[key]
    stats.setdefault('kills10_leads', 0)
    stats.setdefault('kills10_draws', 0)
    stats.setdefault('kills10_games', 0)
    stats.setdefault('kills10_diff_sum', 0.0)
    stats.setdefault('kills10_diff_sq_sum', 0.0)
    stats['kills10_games'] += 1
    stats['kills10_diff_sum'] += kills10_diff
    stats['kills10_diff_sq_sum'] += kills10_diff * kills10_diff
    if kills10_diff > 0:
        stats['kills10_leads'] += 1
    elif kills10_diff == 0:
        stats['kills10_draws'] += 1


def _normalize_comparable_id(value):
    """Normalize match/map IDs so str/int forms compare equal.

    Returns None for empty/invalid values so they never match a real ID.
    Numeric strings become int; other non-empty values become stable strings.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):  # NaN / +/-inf
            return None
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit() or (stripped.startswith('-') and stripped[1:].isdigit()):
            try:
                return int(stripped)
            except Exception:
                return stripped
        return stripped
    try:
        as_int = int(value)
        return as_int
    except Exception:
        text = str(value).strip()
        return text or None


def _match_in_exclude_set(match, exclude_match_ids, match_id_hint=None):
    """Return True if the match is covered by exclude_match_ids.

    Candidates: match['id'], match['match_id'], match['_map_id'], then match_id_hint.
    Exclude values are normalized on every call (no stale cache over mutable sets).
    """
    if not exclude_match_ids:
        return False
    if not isinstance(match, dict):
        match = {}

    normalized_exclude = set()
    for raw in exclude_match_ids:
        norm = _normalize_comparable_id(raw)
        if norm is not None:
            normalized_exclude.add(norm)
    if not normalized_exclude:
        return False

    candidates = (
        match.get('id'),
        match.get('match_id'),
        match.get('_map_id'),
        match_id_hint,
    )
    for raw in candidates:
        norm = _normalize_comparable_id(raw)
        if norm is not None and norm in normalized_exclude:
            return True
    return False


# Half-open minute-bucket windows for team kill-advantage dictionaries.
# Matches live pipeline convention: [:10] = minutes 0..9 (bucket 10 excluded).
KILLS_WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))
KILLS_WINDOW_LABELS = tuple(f"{start}_{end}" for start, end in KILLS_WINDOWS)


def _kills_window_diff(match, start, end):
    """Return Radiant minus Dire kills in half-open minute buckets [start:end].

    ``radiantKills`` / ``direKills`` are per-minute kill counts (not cumulative).
    Invalid / too-short series return None so the caller can skip that window
    without dropping other valid windows for the same match.
    """
    start = int(start)
    end = int(end)
    if end <= start or start < 0:
        return None
    radiant = match.get('radiantKills')
    dire = match.get('direKills')
    if not isinstance(radiant, list) or not isinstance(dire, list):
        return None
    if len(radiant) < end or len(dire) < end:
        return None
    radiant_slice = radiant[start:end]
    dire_slice = dire[start:end]
    values = radiant_slice + dire_slice
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
        for value in values
    ):
        return None
    return float(sum(radiant_slice) - sum(dire_slice))


def _kills10_diff(match):
    """Return Radiant minus Dire kills in the first ten minute buckets.

    Bucket index 10 is deliberately excluded: the live pipeline already uses
    ``[:10]`` for its kills-at-10 marker, so the training dictionary must use
    exactly the same boundary.
    """
    return _kills_window_diff(match, 0, 10)


def _empty_kills_window_stats():
    """Compact multi-window kill counters for one draft key.

    Layout per window (len(KILLS_WINDOWS) blocks of 5):
      leads, draws, games, diff_sum, diff_sq_sum
    """
    return [0, 0, 0, 0.0, 0.0] * len(KILLS_WINDOWS)


def _append_kills_window_entry(target_dict, key, window_diffs, invert=False):
    """Accumulate multi-window kill diffs for a draft key.

    ``window_diffs`` is a sequence aligned with ``KILLS_WINDOWS``; each item is
    Radiant-minus-Dire (or None when that window is unavailable). When
    ``invert`` is True the key is Dire-oriented and the sign is flipped.
    """
    if not window_diffs:
        return
    stats = target_dict.get(key)
    if stats is None:
        stats = _empty_kills_window_stats()
        target_dict[key] = stats
    for index, diff in enumerate(window_diffs):
        if diff is None:
            continue
        # Diffs are already numeric from the kills timeline; avoid per-cell float().
        value = -diff if invert else diff
        base = index * 5
        stats[base + 2] += 1
        stats[base + 3] += value
        stats[base + 4] += value * value
        if value > 0:
            stats[base] += 1
        elif value == 0:
            stats[base + 1] += 1


def _add_kills_window_combinations(r_by_pos, d_by_pos, target_dict, window_diffs):
    """Write full-draft solo/cp/synergy keys for kill-window targets.

    Same key grammar as early/late/post_lane, trios included: they used to be
    skipped because ordered duplicates made the dict explode, but after
    canonicalisation (`_canonical_group`) a trio costs 20 records per match.
    Radiant-leading keys store Radiant-minus-Dire; Dire-leading keys invert.
    """
    r_entries = _draft_entries(r_by_pos)
    d_entries = _draft_entries(d_by_pos)

    for token, _hero in r_entries:
        _append_kills_window_entry(target_dict, token, window_diffs, invert=False)
    for token, _hero in d_entries:
        _append_kills_window_entry(target_dict, token, window_diffs, invert=True)

    if KILLS_WINDOW_BUILD_HIGH_ORDER:
        for r_token, _r_hero in r_entries:
            for (d_token1, d_hero1), (d_token2, d_hero2) in combinations(d_entries, 2):
                if d_hero1 == d_hero2:
                    continue
                key = f'{r_token}_vs_{_canonical_group(d_token1, d_token2)}'
                _append_kills_window_entry(target_dict, key, window_diffs, invert=False)

        for (r_token1, r_hero1), (r_token2, r_hero2) in combinations(r_entries, 2):
            if r_hero1 == r_hero2:
                continue
            left = _canonical_group(r_token1, r_token2)
            for d_token, _d_hero in d_entries:
                _append_kills_window_entry(target_dict, f'{left}_vs_{d_token}', window_diffs, invert=False)

    for r_token, _r_hero in r_entries:
        for d_token, _d_hero in d_entries:
            _append_kills_window_entry(target_dict, f'{r_token}_vs_{d_token}', window_diffs, invert=False)

    for (token1, hero1), (token2, hero2) in combinations(r_entries, 2):
        if hero1 == hero2:
            continue
        left, right = sorted((token1, token2))
        _append_kills_window_entry(target_dict, f'{left}_with_{right}', window_diffs, invert=False)

    for (token1, hero1), (token2, hero2) in combinations(d_entries, 2):
        if hero1 == hero2:
            continue
        left, right = sorted((token1, token2))
        _append_kills_window_entry(target_dict, f'{left}_with_{right}', window_diffs, invert=True)

    for trio in combinations(r_entries, 3):
        key = _canonical_group(*(token for token, _ in trio))
        _append_kills_window_entry(target_dict, key, window_diffs, invert=False)

    for trio in combinations(d_entries, 3):
        key = _canonical_group(*(token for token, _ in trio))
        _append_kills_window_entry(target_dict, key, window_diffs, invert=True)


def kills_windows(match, kills_window_dict):
    """Record multi-window team kill advantage for one match into the dict.

    No early/late/lane-outcome gates: any match with full positions and at
    least one valid kill window contributes. Returns True if anything written.
    """
    if kills_window_dict is None:
        return False
    r_by_pos, d_by_pos = extract_heroes_by_position(match)
    if r_by_pos is None:
        return False
    window_diffs = [_kills_window_diff(match, start, end) for start, end in KILLS_WINDOWS]
    if all(diff is None for diff in window_diffs):
        return False
    _add_kills_window_combinations(r_by_pos, d_by_pos, kills_window_dict, window_diffs)
    return True


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

    Returns:
        True if at least one lane key was written, otherwise False.
    """
    # Извлекаем героев и позиции
    r_by_pos, d_by_pos = extract_heroes_by_position(match)
    if r_by_pos is None:
        return False

    updated = False

    # Определяем исходы лайнов
    top_outcome = match.get('topLaneOutcome', '')
    mid_outcome = match.get('midLaneOutcome', '')
    bot_outcome = match.get('bottomLaneOutcome', '')
    radiant_kills10_diff = _kills10_diff(match)
    
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

        Returns:
            True if at least one lane key was written for this lane.
        """
        nonlocal updated
        if not outcome:
            return False
        
        value_r = get_lane_value(outcome, True)
        value_d = get_lane_value(outcome, False)
        
        if value_r is None:
            return False

        wrote = False

        # Соло герои Radiant
        for hero_id, pos in r_heroes:
            _append_lane_entry(lane_dict, f'{hero_id}pos{pos}', value_r, radiant_kills10_diff)
            wrote = True
        
        # Соло герои Dire
        for hero_id, pos in d_heroes:
            _append_lane_entry(lane_dict, f'{hero_id}pos{pos}', value_d, -radiant_kills10_diff if radiant_kills10_diff is not None else None)
            wrote = True
        
        # Если это парный лайн (2v2)
        if len(r_heroes) == 2 and len(d_heroes) == 2:
            r_h1, r_p1 = r_heroes[0]
            r_h2, r_p2 = r_heroes[1]
            d_h1, d_p1 = d_heroes[0]
            d_h2, d_p2 = d_heroes[1]
            
            # Контрипики 2x2
            key = f'{r_h1}pos{r_p1},{r_h2}pos{r_p2}_vs_{d_h1}pos{d_p1},{d_h2}pos{d_p2}'
            _append_lane_entry(lane_dict, key, value_r, radiant_kills10_diff)
            
            # Контрипики 2x1 (Radiant 2 vs Dire 1)
            _append_lane_entry(lane_dict, f'{r_h1}pos{r_p1},{r_h2}pos{r_p2}_vs_{d_h1}pos{d_p1}', value_r, radiant_kills10_diff)
            _append_lane_entry(lane_dict, f'{r_h1}pos{r_p1},{r_h2}pos{r_p2}_vs_{d_h2}pos{d_p2}', value_r, radiant_kills10_diff)
            
            # Контрипики 1x2 (Radiant 1 vs Dire 2)
            _append_lane_entry(lane_dict, f'{r_h1}pos{r_p1}_vs_{d_h1}pos{d_p1},{d_h2}pos{d_p2}', value_r, radiant_kills10_diff)
            _append_lane_entry(lane_dict, f'{r_h2}pos{r_p2}_vs_{d_h1}pos{d_p1},{d_h2}pos{d_p2}', value_r, radiant_kills10_diff)
            
            # Контрипики 1x1 (все комбинации)
            for r_hero, r_pos in r_heroes:
                for d_hero, d_pos in d_heroes:
                    _append_lane_entry(lane_dict, f'{r_hero}pos{r_pos}_vs_{d_hero}pos{d_pos}', value_r, radiant_kills10_diff)
            
            # Синергия 1+1 для Radiant
            _append_lane_entry(lane_dict, f'{r_h1}pos{r_p1}_with_{r_h2}pos{r_p2}', value_r, radiant_kills10_diff)
            
            # Синергия 1+1 для Dire
            _append_lane_entry(lane_dict, f'{d_h1}pos{d_p1}_with_{d_h2}pos{d_p2}', value_d, -radiant_kills10_diff if radiant_kills10_diff is not None else None)
            wrote = True
        
        # Если это 1v1
        elif len(r_heroes) == 1 and len(d_heroes) == 1:
            r_h, r_p = r_heroes[0]
            d_h, d_p = d_heroes[0]
            
            # Контрипик 1x1
            _append_lane_entry(lane_dict, f'{r_h}pos{r_p}_vs_{d_h}pos{d_p}', value_r, radiant_kills10_diff)
            wrote = True

        if wrote:
            updated = True
        return wrote
    
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

    return updated



# ============================================================================
# НАСТРОЙКИ ФИЛЬТРОВ EARLY/LATE (подбираются экспериментально)
# ============================================================================
# Early: требуем близкий networth на gate-точке и ищем ранний перевес.
EARLY_GATE_INDEX = 9                 # фильтр на leads[9] (minute 10)
EARLY_GATE_MAX_ABS_LEAD = 2000       # игра не должна разъехаться до early-gate
EARLY_LEAD_WINDOW = (20, 28)         # реальные минуты достижения 20% comeback threshold
EARLY_FAST_FINISH_MAX_MINUTES = 34   # быстрые карты считаем early по победителю

# Late: длинная игра, где networth gap не разъехался сильнее WR60 ladder.
# Все четыре параметра правила сбора вынесены в env для A/B-пересборок словаря;
# дефолты равны историческим значениям, поведение прода без env не меняется.
LATE_MIN_DURATION = int(os.getenv("ANALISE_LATE_MIN_DURATION", "36"))
LATE_MAX_DURATION = (
    int(os.getenv("ANALISE_LATE_MAX_DURATION"))
    if os.getenv("ANALISE_LATE_MAX_DURATION") else None
)  # верхний предел длины карты; нужен, чтобы собрать «короткую» половину для метрики скейлинга
LATE_EARLY_WINDOW = (15, 25)         # окно для оценки раннего snowball
LATE_EARLY_STOMP_MAX = 12000         # max |lead| для раннего snowball
LATE_COMEBACK_AVG_DEFICIT = 4000     # средний deficit победителя в 15-25
LATE_CLOSE_WINDOW = (20, 30)         # окно "близкой" игры
LATE_CLOSE_MAX_LEAD = 5000           # max |lead| в close-окне
LATE_MODE = 'comeback'               # 'either' | 'comeback' | 'close'
LATE_REQUIRE_EARLY_LOSS = True      # late = победитель не был early-доминатором
LATE_WR60_START_MINUTE = int(os.getenv("ANALISE_LATE_WR60_START_MINUTE", "28"))
# Множитель WR60-лестницы: <1 = более строгое требование «равной» игры.
LATE_EQUAL_GATE_K = float(os.getenv("ANALISE_LATE_EQUAL_GATE_K", "1.0"))
# 0 = брать любую игру нужной длины, не требуя равного момента вообще.
LATE_REQUIRE_EQUAL_MOMENT = _env_bool("ANALISE_LATE_REQUIRE_EQUAL_MOMENT", False)
# 'any' — равный момент на ЛЮБОЙ минуте начиная со START (историческое правило);
# 'at'  — равенство ИМЕННО на минуте START (совпадает с тем, что видно в live).
LATE_EQUAL_MODE = os.getenv("ANALISE_LATE_EQUAL_MODE", "any").strip().lower()
# Какое правило допуска использовать:
#   'equal'        — длинная игра с равным поздним счётом (текущий прод);
#   'comeback_avg' — историческое правило до 28.04.2026: победитель в СРЕДНЕМ
#                    отставал >= DEFICIT в окне COMEBACK_WINDOW;
#   'comeback_max' — победитель отыграл дефицит >= DEFICIT (минимум его lead за
#                    игру <= -DEFICIT), то есть «камбек с N тысяч».
#   ВНИМАНИЕ: оба comeback-режима отбирают карту ПО ИСХОДУ (условие сформулировано
#   про победителя), поэтому игры, где отстающий не вытащил, в выборку не попадают,
#   и винрейты лейтовых героев в таком словаре завышены. Для честного «кто вытаскивает
#   из отставания» есть режим ниже, где условие только про состояние игры.
#   'deficit_state'   — на минуте DEFICIT_MINUTE разрыв >= DEFICIT_MIN, КТО БЫ ни выиграл;
#   'equal_or_deficit'— объединение: равный поздний счёт ИЛИ позиция отставания.
LATE_RULE = os.getenv("ANALISE_LATE_RULE", "equal").strip().lower()
LATE_DEFICIT_MIN = float(os.getenv("ANALISE_LATE_DEFICIT_MIN", "8000"))
LATE_DEFICIT_MINUTE = int(os.getenv("ANALISE_LATE_DEFICIT_MINUTE", "30"))
#   'tower_gap' — на минуте TOWER_MINUTE структурный перекос: разница «очков»
#                 (потерянные T3 + снесённые линии бараков * 3) >= TOWER_MIN_GAP.
#                 Условие про состояние карты, не про исход (E-43).
LATE_TOWER_MINUTE = int(os.getenv("ANALISE_LATE_TOWER_MINUTE", "32"))
LATE_TOWER_MIN_GAP = int(os.getenv("ANALISE_LATE_TOWER_MIN_GAP", "2"))
# Вторая, БОЛЕЕ РАННЯЯ минута: требуем, чтобы структура тогда была ещё ровной.
# Зачем. `_tower_structure_score(match, M)` считает все падения башен со временем
# меньше M и НЕ требует, чтобы матч дожил до M. Поэтому одиночное условие на
# поздней минуте вырождается: у законченной игры перекос почти всегда есть, и
# правило пропускает 88% корпуса вместо 26% (замер 08.08). Пара условий
# «ровно на EVEN_MINUTE и разошлось к TOWER_MINUTE» отбирает именно игры, которые
# решились ПОЗДНО, — а это и есть популяция late-блока.
# 0 = выключено, поведение прежнее.
LATE_TOWER_EVEN_MINUTE = int(os.getenv("ANALISE_LATE_TOWER_EVEN_MINUTE", "0"))
LATE_TOWER_EVEN_MAX_GAP = int(os.getenv("ANALISE_LATE_TOWER_EVEN_MAX_GAP", "1"))
# Чему учится словарь (МАРКЕР, а не правило допуска):
#   'winner'     — победитель карты (все наши словари исторически такие);
#   'tower_lead' — сторона, у которой к минуте TOWER_MINUTE башни целее.
# Второй режим даёт метрику «этот драфт держит/сносит строения», а не «побеждает».
# Расхождение между такой метрикой и фактическим состоянием карты — и есть
# кандидат в сигнал на камбек (идея alex).
LATE_MARKER = os.getenv("ANALISE_LATE_MARKER", "winner").strip().lower()
# Ограничить solo-записи late последним version-патчем, как у post_lane (E-46).
LATE_SOLO_LATEST_PATCH_ONLY = _env_bool("ANALISE_LATE_SOLO_LATEST_PATCH_ONLY", False)
# --- Структурное условие для EARLY-словарей (идея alex, E-48) ---
# Карта идёт в early-словарь только если у стороны-метки строения целы:
#   'off'              — как сейчас, без условия;
#   'target_intact'    — у стороны-метки все башни тира целы;
#   'intact_vs_lost'   — у стороны-метки все целы И у оппонента пала хотя бы одна.
EARLY_TOWER_RULE = os.getenv("ANALISE_EARLY_TOWER_RULE", "off").strip().lower()
EARLY_TOWER_TIER = os.getenv("ANALISE_EARLY_TOWER_TIER", "t3").strip().lower()
EARLY_TOWER_MINUTE = int(os.getenv("ANALISE_EARLY_TOWER_MINUTE", "28"))
_TIER_IDS = {
    "t1": {"radiant": {16, 17, 18}, "dire": {26, 27, 28}},
    "t2": {"radiant": {19, 20, 21}, "dire": {29, 30, 31}},
    "t3": {"radiant": {22, 23, 24}, "dire": {32, 33, 34}},
}
# npcId башен и бараков по сторонам (E-43, восстановлены по медиане времени падения)
_T3_IDS = {"radiant": {22, 23, 24}, "dire": {32, 33, 34}}
_RAX_IDS = {"radiant": set(range(38, 44)), "dire": set(range(44, 50))}
_RAX_PER_LANE = 2
LATE_COMEBACK_DEFICIT = float(os.getenv("ANALISE_LATE_COMEBACK_DEFICIT", "10000"))
LATE_COMEBACK_WINDOW = tuple(
    int(part) for part in os.getenv("ANALISE_LATE_COMEBACK_WINDOW", "15,25").split(",")[:2]
)
# только для comeback_avg: отбрасывать карты, где победитель был ранним доминатором
LATE_COMEBACK_REQUIRE_EARLY_LOSS = _env_bool("ANALISE_LATE_COMEBACK_REQUIRE_EARLY_LOSS", True)
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
POST_LANE_MIN_DURATION = int(os.getenv("ANALISE_POST_LANE_MIN_DURATION", "28"))
# 28 вместо 20 (E-52, решение alex): поминутная развёртка дала на 28-й отчётливый
# пик (64.4/64.5/64.5 против 64.2/64.0/63.0 у прежних 20), соседи 27 и 29 ниже.
# Это ровно граница EARLY_LEAD_WINDOW: до неё карта «ранняя», после — другая фаза.


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
    - Для длинных карт на gate-точке leads[9] (minute 10) игра не должна быть уже слишком разъехавшейся
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
    leads = match.get('radiantNetworthLeads') or []
    duration = len(leads)

    if not duration:
        # Карта без networth-полосы (в про-дампах таких ~14%) раньше проходила
        # как «быстрая» и получала метку по одному лишь победителю. Данных для
        # ранней фазы у неё нет — в early-выборку она не идёт.
        return False, None

    if duration <= EARLY_FAST_FINISH_MAX_MINUTES:
        did_radiant_win = match.get('didRadiantWin')
        if did_radiant_win is None:
            win_rates = match.get('winRates') or []
            did_radiant_win = win_rates[-1] > 0.5 if win_rates else None
        if did_radiant_win is not None:
            return True, 'radiant' if did_radiant_win else 'dire'
        final_lead = _as_float(leads[-1]) if leads else None
        if final_lead is not None and final_lead != 0:
            return True, 'radiant' if final_lead > 0 else 'dire'
        return False, None

    if ANALISE_EARLY_MINUTE10_GATE_ENABLED:
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


def _early_tower_ok(match, side):
    """Структурное условие early: у стороны `side` строения целы (E-48).

    side — 'radiant'/'dire', сторона, чей исход записывается меткой. Карты без
    данных о башнях не проходят условие: проверить его нечем.
    """
    if EARLY_TOWER_RULE == "off":
        return True
    if side not in ("radiant", "dire"):
        return False
    deaths = match.get("towerDeaths")
    if not deaths:
        return False
    ids = _TIER_IDS.get(EARLY_TOWER_TIER) or _TIER_IDS["t3"]
    cutoff = int(EARLY_TOWER_MINUTE) * 60
    lost = {"radiant": 0, "dire": 0}
    for event in deaths:
        npc_id = event.get("npcId")
        is_radiant = event.get("isRadiant")
        time_seconds = event.get("time")
        if npc_id is None or time_seconds is None or time_seconds >= cutoff:
            continue
        owner = "radiant" if is_radiant is True else ("dire" if is_radiant is False else None)
        if owner is None:
            continue
        if npc_id in ids[owner]:
            lost[owner] += 1
    opponent = "dire" if side == "radiant" else "radiant"
    if lost[side] != 0:
        return False
    if EARLY_TOWER_RULE == "intact_vs_lost":
        return lost[opponent] > 0
    return True


def _tower_structure_score(match, minute):
    """«Очки» структурных потерь каждой стороны к указанной минуте.

    Потерянная T3 = 1 очко, снесённая линия бараков (два барака) = 3 очка.
    Возвращает (radiant_score, dire_score) или None, если данных о башнях нет.
    """
    deaths = match.get('towerDeaths')
    if not deaths:
        return None
    cutoff = int(minute) * 60
    lost = {'radiant': {'t3': 0, 'rax': 0}, 'dire': {'t3': 0, 'rax': 0}}
    for event in deaths:
        npc_id = event.get('npcId')
        is_radiant = event.get('isRadiant')
        time_seconds = event.get('time')
        if npc_id is None or time_seconds is None or time_seconds >= cutoff:
            continue
        side = 'radiant' if is_radiant is True else ('dire' if is_radiant is False else None)
        if side is None:
            continue
        if npc_id in _T3_IDS[side]:
            lost[side]['t3'] += 1
        elif npc_id in _RAX_IDS[side]:
            lost[side]['rax'] += 1
    return tuple(
        lost[side]['t3'] + (lost[side]['rax'] // _RAX_PER_LANE) * 3
        for side in ('radiant', 'dire')
    )


def _tower_gap_hit(match, minute, min_gap) -> bool:
    scores = _tower_structure_score(match, minute)
    if scores is None:
        return False
    return abs(scores[0] - scores[1]) >= int(min_gap)


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
    leads = match.get('radiantNetworthLeads') or []
    did_radiant_win = match.get('didRadiantWin')
    duration = len(leads)
    
    if did_radiant_win is None:
        win_rates = match.get('winRates') or []
        did_radiant_win = win_rates[-1] > 0.5 if win_rates else None

    if did_radiant_win is None or duration < LATE_MIN_DURATION:
        return (False, None) if if_check else False

    if LATE_MAX_DURATION is not None and duration > LATE_MAX_DURATION:
        return (False, None) if if_check else False

    winner = 'radiant' if did_radiant_win else 'dire'

    def _deficit_state_hit() -> bool:
        """Позиция отставания на заданной минуте — условие НЕ зависит от исхода."""
        idx = int(LATE_DEFICIT_MINUTE) - 1
        if idx < 0 or len(leads) <= idx:
            return False
        value = _as_float(leads[idx])
        return value is not None and abs(value) >= LATE_DEFICIT_MIN

    if LATE_RULE == 'tower_gap':
        ok = _tower_gap_hit(match, LATE_TOWER_MINUTE, LATE_TOWER_MIN_GAP)
        if ok and LATE_TOWER_EVEN_MINUTE > 0:
            # Игра должна была быть ещё СТРУКТУРНО РОВНОЙ на ранней минуте:
            # иначе в выборку попадают партии, решённые задолго до TOWER_MINUTE.
            early = _tower_structure_score(match, LATE_TOWER_EVEN_MINUTE)
            ok = early is not None and abs(early[0] - early[1]) <= LATE_TOWER_EVEN_MAX_GAP
        return (ok, winner if ok else None) if if_check else ok

    if LATE_RULE == 'deficit_state':
        ok = _deficit_state_hit()
        return (ok, winner if ok else None) if if_check else ok

    if LATE_RULE == 'equal_or_deficit':
        if _deficit_state_hit():
            return (True, winner) if if_check else True
        # иначе падаем в обычную проверку равного позднего счёта ниже

    if LATE_RULE in ('comeback_avg', 'comeback_max'):
        # Знак lead всегда «в пользу radiant», разворачиваем под победителя.
        sign = 1.0 if winner == 'radiant' else -1.0
        if LATE_RULE == 'comeback_max':
            # Победитель отыграл дефицит: его минимальный lead за игру <= -DEFICIT.
            worst = None
            for raw in leads:
                value = _as_float(raw)
                if value is None:
                    continue
                own = sign * value
                if worst is None or own < worst:
                    worst = own
            ok = worst is not None and worst <= -LATE_COMEBACK_DEFICIT
            return (ok, winner if ok else None) if if_check else ok

        # comeback_avg: средний дефицит победителя в окне
        avg = _avg_in_window(leads, LATE_COMEBACK_WINDOW[0], LATE_COMEBACK_WINDOW[1])
        if avg is None:
            return (False, None) if if_check else False
        if LATE_COMEBACK_REQUIRE_EARLY_LOSS and dominator in ('radiant', 'dire') and dominator == winner:
            return (False, None) if if_check else False
        ok = (sign * avg) <= -LATE_COMEBACK_DEFICIT
        return (ok, winner if ok else None) if if_check else ok

    if not LATE_REQUIRE_EQUAL_MOMENT:
        # Правило «просто длинная игра»: условие равенства выключено.
        return (True, winner) if if_check else True

    late_thresholds_by_minute = _load_late_wr60_thresholds()
    for minute, threshold in sorted(late_thresholds_by_minute.items()):
        if minute < int(LATE_WR60_START_MINUTE):
            continue
        if LATE_EQUAL_MODE == 'at' and minute != int(LATE_WR60_START_MINUTE):
            break
        idx = minute - 1
        if idx < 0 or len(leads) <= idx:
            continue
        lead = _as_float(leads[idx])
        if lead is not None and abs(lead) <= threshold * LATE_EQUAL_GATE_K:
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
    leads = match.get('radiantNetworthLeads') or []
    did_radiant_win = match.get('didRadiantWin')
    duration = len(leads)

    if did_radiant_win is None:
        win_rates = match.get('winRates') or []
        did_radiant_win = win_rates[-1] > 0.5 if win_rates else None

    if did_radiant_win is None or duration < POST_LANE_MIN_DURATION:
        return (False, None) if if_check else False

    if ANALISE_POST_LANE_MINUTE10_GATE_ENABLED:
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


# Старт последнего version-патча (для пер-патч скоупа solo в post_lane).
# Динамически = самый свежий version-патч из keys (сейчас 7.41d=1780531200), авто-обновится.
try:
    from keys import DOTA_VERSION_PATCH_EVENTS as _DOTA_VER_EVENTS
    LATEST_PATCH_START_TS = max(int(e["start_ts"]) for e in _DOTA_VER_EVENTS)
except Exception:
    LATEST_PATCH_START_TS = 1780531200  # 7.41d fallback (2026-06-04 UTC)

# Начало окна, с которого пишутся post_lane-solo записи. По умолчанию — последний
# патч (прежнее поведение), но explore_database перед сборкой сдвигает границу
# вглубь, пока окно не наберёт POST_LANE_SOLO_MIN_MATCHES матчей: свежий патч
# сам по себе не должен обнулять покрытие solo.
#
# Сколько нужно матчей: на 479 токенах «герой+позиция» текущего словаря
# (502 507 матчей в скоупе) порог n=140 берут 95% токенов при ~335 000 матчей,
# 99% при ~1.02M, все — при ~1.41M (упирается в экзотику вроде саппорта на pos1,
# её всё равно режет SOLO_MIN_MATCHES=50 на чтении). Отсюда дефолт 350 000.
POST_LANE_SOLO_MIN_MATCHES = max(0, int(
    os.getenv("POST_LANE_SOLO_MIN_MATCHES", "350000") or "350000"
))
POST_LANE_SOLO_SCOPE_START_TS = LATEST_PATCH_START_TS


def _add_combinations_to_dict(r_by_pos, d_by_pos, target_dict, r_value, d_value=None, write_solo=True):
    """
    Добавляет все комбинации героев в словарь.
    Оптимизированная версия для уменьшения дублирования кода.

    Args:
        r_by_pos: словарь позиций героев Radiant
        d_by_pos: словарь позиций героев Dire
        target_dict: целевой словарь (обычный dict, будет содержать счетчики {'wins': N, 'games': M})
        r_value: значение для Radiant героев и комбинаций начинающихся с Radiant
        d_value: значение для Dire героев и комбинаций Dire (если None, используется r_value)
        write_solo: писать ли solo-записи ({hero}pos{n}). Для post_lane передаём False на
                    старых матчах, чтобы post_lane-solo собирался ТОЛЬКО на последнем патче,
                    а cp/synergy post_lane оставались на широком окне.
    """
    if d_value is None:
        d_value = r_value

    r_entries = _draft_entries(r_by_pos)
    d_entries = _draft_entries(d_by_pos)

    # Одиночные герои (solo) — опционально (пер-патч скоуп для post_lane)
    if write_solo:
        for token, _hero in r_entries:
            _append_to_dict(target_dict, token, r_value)

        for token, _hero in d_entries:
            _append_to_dict(target_dict, token, d_value)

    # Контрипики 1x2 (пара врагов — неупорядоченная группа)
    for r_token, _r_hero in r_entries:
        for (d_token1, d_hero1), (d_token2, d_hero2) in combinations(d_entries, 2):
            if d_hero1 == d_hero2:
                continue
            key = f'{r_token}_vs_{_canonical_group(d_token1, d_token2)}'
            _append_to_dict(target_dict, key, r_value)

    # Контрипики 2x1 (пара своих — неупорядоченная группа)
    for (r_token1, r_hero1), (r_token2, r_hero2) in combinations(r_entries, 2):
        if r_hero1 == r_hero2:
            continue
        left = _canonical_group(r_token1, r_token2)
        for d_token, _d_hero in d_entries:
            _append_to_dict(target_dict, f'{left}_vs_{d_token}', r_value)

    # Контрипики 1x1
    for r_token, _r_hero in r_entries:
        for d_token, _d_hero in d_entries:
            _append_to_dict(target_dict, f'{r_token}_vs_{d_token}', r_value)

    # Синергия 1+1 (Radiant)
    for (token1, hero1), (token2, hero2) in combinations(r_entries, 2):
        if hero1 == hero2:
            continue
        left, right = sorted((token1, token2))
        _append_to_dict(target_dict, f'{left}_with_{right}', r_value)

    # Синергия 1+1 (Dire)
    for (token1, hero1), (token2, hero2) in combinations(d_entries, 2):
        if hero1 == hero2:
            continue
        left, right = sorted((token1, token2))
        _append_to_dict(target_dict, f'{left}_with_{right}', d_value)

    # Трио синергия (Radiant)
    for trio in combinations(r_entries, 3):
        _append_to_dict(target_dict, _canonical_group(*(token for token, _ in trio)), r_value)

    # Трио синергия (Dire)
    for trio in combinations(d_entries, 3):
        _append_to_dict(target_dict, _canonical_group(*(token for token, _ in trio)), d_value)


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
                     post_lane_dict=None, kills_window_dict=None, match_id_hint=None,
                     early_end_dict=None):
    """
    Основная функция анализа матча.
    
    Args:
        match: словарь с данными матча
        lane_dict: словарь для записи статистики по лайнам
        early_dict: early stats with NW-dominator label (current default)
        late_dict: словарь для записи статистики по late фазе
        post_lane_dict: словарь после лейнинга с gate на 10-й минуте и min duration
        kills_window_dict: multi-window team kill advantage (5-15/10-20/15-25/20-30)
        early_end_dict: early stats with map-winner label (same early gates as early_dict)
        exclude_match_ids: set или list ID матчей которые нужно исключить (для избежания data leakage)
        exclude_pro_matches: если True, пропускает про-матчи (default: True)
        match_id_hint: optional external map key when match payload has no id
    
    ⚠️ ВАЖНО: Для избежания data leakage при обучении ML моделей:
    - Всегда передавайте exclude_match_ids содержащий текущий матч
    - Используйте temporal split: обрабатывайте матчи в хронологическом порядке
    - Для каждого матча используйте только статистику из предыдущих матчей

    Returns:
        True if at least one target dict was actually updated; False otherwise.
    """
    # Фильтр про-матчей
    if exclude_pro_matches and is_pro_match(match):
        return False  # Матч пропущен
    
    # Фильтр исключаемых матчей (str/int-normalized; supports id/match_id/_map_id/hint)
    if _match_in_exclude_set(match, exclude_match_ids, match_id_hint=match_id_hint):
        return False  # Матч пропущен

    updated = False

    # 1. Обработка лайнов
    if lane_dict is not None:
        if lanes(match, lane_dict):
            updated = True

    # 1b. Multi-window kill advantage (independent of lane/early/late gates)
    if kills_window_dict is not None:
        if kills_windows(match, kills_window_dict):
            updated = True
    
    # 2. Извлекаем героев и позиции для early/late/post-lane
    r_by_pos, d_by_pos = extract_heroes_by_position(match)
    if r_by_pos is None:
        return updated
    
    # Определяем победителя один раз (используется в late и post-lane)
    did_radiant_win = match.get('didRadiantWin')
    if did_radiant_win is None:
        # Используем последний элемент winRates
        win_rates = match.get('winRates') or []
        did_radiant_win = win_rates[-1] > 0.5 if win_rates else False

    # В скоупе post_lane-solo? Раньше это был жёстко последний патч, из-за чего
    # свежий патч обнулял покрытие solo, пока не наберёт объём. Теперь скоуп —
    # окно, набранное от свежего патча вглубь до POST_LANE_SOLO_MIN_MATCHES
    # (см. explore_database._resolve_post_lane_solo_scope).
    try:
        is_latest_patch = int(match.get('startDateTime', 0)) >= POST_LANE_SOLO_SCOPE_START_TS
    except (TypeError, ValueError):
        is_latest_patch = False

    # 3. Обработка EARLY словарей
    # Используем новый фильтр is_early_match()
    # early_dict: true = NW dominator (current production semantics)
    # early_end_dict: true = map winner (same early sample gates)
    early_result, dominator = is_early_match(match)
    if early_result:
        if early_dict is not None and _early_tower_ok(match, dominator):
            r_val = 1 if dominator == 'radiant' else 0
            d_val = 1 if dominator == 'dire' else 0
            _add_combinations_to_dict(r_by_pos, d_by_pos, early_dict, r_val, d_val)
            updated = True
        if early_end_dict is not None:
            winner_side = 'radiant' if did_radiant_win else 'dire'
            if _early_tower_ok(match, winner_side):
                r_val = 1 if did_radiant_win else 0
                d_val = 0 if did_radiant_win else 1
                _add_combinations_to_dict(r_by_pos, d_by_pos, early_end_dict, r_val, d_val)
                updated = True
    
    # Проверяем условия для late_dict
    # Используем улучшенный фильтр is_late_match()
    if late_dict is not None and is_late_match(match, dominator):
        late_marker = None
        if LATE_MARKER == 'tower_lead':
            # Маркер — у кого целее строения к заданной минуте. Карты без данных о
            # башнях и с равной структурой в словарь не пишутся: метки нет.
            scores = _tower_structure_score(match, LATE_TOWER_MINUTE)
            if scores is not None and scores[0] != scores[1]:
                late_marker = scores[0] < scores[1]  # True = строения целее у Radiant
        else:
            late_marker = bool(did_radiant_win)
        if late_marker is not None:
            r_val = 1 if late_marker else 0
            d_val = 0 if late_marker else 1
            # solo-записи late исторически копятся по ВСЕМ патчам, тогда как у
            # post_lane они ограничены последним (Option C). Флаг позволяет
            # собрать late-solo в том же скоупе и померить, решает ли свежесть.
            _add_combinations_to_dict(
                r_by_pos, d_by_pos, late_dict, r_val, d_val,
                write_solo=(is_latest_patch if LATE_SOLO_LATEST_PATCH_ONLY else True),
            )
            updated = True

    if post_lane_dict is not None and is_post_lane_match(match):
        # После post-lane gate записываем фактического победителя матча.
        # cp/synergy post_lane — широкое окно; solo — ТОЛЬКО последний патч (Option C).
        r_val = 1 if did_radiant_win else 0
        d_val = 0 if did_radiant_win else 1
        _add_combinations_to_dict(r_by_pos, d_by_pos, post_lane_dict, r_val, d_val,
                                  write_solo=is_latest_patch)
        updated = True

    return updated
