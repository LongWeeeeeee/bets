"""
Шаг 2: Обработка про-матчей и обогащение метриками героев + командными фичами.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - fallback for environments without tqdm
    def tqdm(iterable=None, **kwargs):
        return iterable if iterable is not None else []

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from base.id_to_names import tier_one_teams, tier_two_teams


def _build_tier_lookup() -> dict[int, int]:
    """Builds team_id -> tier lookup."""
    lookup: dict[int, int] = {}
    for team_ids in tier_one_teams.values():
        if isinstance(team_ids, set):
            for tid in team_ids:
                lookup[tid] = 1
        else:
            lookup[team_ids] = 1
    for team_ids in tier_two_teams.values():
        if isinstance(team_ids, set):
            for tid in team_ids:
                if tid not in lookup:
                    lookup[tid] = 2
        else:
            if team_ids not in lookup:
                lookup[team_ids] = 2
    return lookup


_TIER_LOOKUP = _build_tier_lookup()


def get_team_tier(team_id: int) -> int:
    """Returns team tier (1, 2, or 3)."""
    return _TIER_LOOKUP.get(team_id, 3)


# Patch schedule (UTC dates) for patch-aware features
_PATCH_SCHEDULE = [
    ("2025-02-19", "7.38"),
    ("2025-03-05", "7.38b"),
    ("2025-03-19", "7.38b"),
    ("2025-03-27", "7.38c"),
    ("2025-05-21", "7.39"),
    ("2025-05-29", "7.39b"),
    ("2025-06-24", "7.39c"),
    ("2025-08-05", "7.39d"),
    ("2025-08-08", "7.39d"),
    ("2025-08-22", "7.39d"),
    ("2025-10-02", "7.39e"),
    ("2025-10-09", "7.39e"),
    ("2025-11-10", "7.39e"),
    ("2025-12-12", "7.39e"),
    ("2025-12-15", "7.40"),
    ("2025-12-23", "7.40b"),
]


def _build_patch_schedule() -> list[dict[str, Any]]:
    from datetime import datetime, timezone

    schedule: list[dict[str, Any]] = []
    for idx, (date_str, label) in enumerate(_PATCH_SCHEDULE):
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            ts = int(dt.timestamp())
        except Exception:
            ts = 0
        if ts <= 0:
            continue
        schedule.append({"patch_id": idx, "label": label, "ts": ts})
    schedule.sort(key=lambda s: s["ts"])
    return schedule


_PATCH_SCHEDULE_INFO = _build_patch_schedule()


def get_patch_label(ts: int) -> str:
    if ts <= 0 or not _PATCH_SCHEDULE_INFO:
        return "UNKNOWN"
    idx = -1
    for i, patch in enumerate(_PATCH_SCHEDULE_INFO):
        if ts >= patch["ts"]:
            idx = i
        else:
            break
    if idx < 0:
        idx = 0
    return str(_PATCH_SCHEDULE_INFO[idx]["label"])


def get_patch_id(ts: int) -> int:
    if ts <= 0 or not _PATCH_SCHEDULE_INFO:
        return -1
    idx = -1
    for i, patch in enumerate(_PATCH_SCHEDULE_INFO):
        if ts >= patch["ts"]:
            idx = i
        else:
            break
    if idx < 0:
        idx = 0
    return int(_PATCH_SCHEDULE_INFO[idx]["patch_id"])


def get_major_patch(label: str) -> str:
    if not label or label == "UNKNOWN":
        return "UNKNOWN"
    base = label
    while base and base[-1].isalpha():
        base = base[:-1]
    return base or label

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_pro_match(match_id: str, match_data: dict) -> Optional[dict]:
    """Извлекает данные из про-матча."""
    players = match_data.get('players', [])
    if len(players) != 10:
        return None
    
    radiant = [p for p in players if p.get('isRadiant')]
    dire = [p for p in players if not p.get('isRadiant')]
    
    if len(radiant) != 5 or len(dire) != 5:
        return None
    
    def sort_key(p: dict) -> int:
        pos = p.get('position') or 'POSITION_5'
        return int(pos.replace('POSITION_', ''))
    
    radiant.sort(key=sort_key)
    dire.sort(key=sort_key)
    
    radiant_kills = sum(p.get('kills', 0) for p in radiant)
    dire_kills = sum(p.get('kills', 0) for p in dire)
    
    dire_kills_arr = match_data.get('direKills') or []
    duration_min = len(dire_kills_arr)
    if duration_min < 10:
        return None
    
    # Team IDs
    radiant_team = match_data.get('radiantTeam') or {}
    dire_team = match_data.get('direTeam') or {}
    league = match_data.get('league') or {}
    
    result = {
        'match_id': int(match_id),
        'total_kills': radiant_kills + dire_kills,
        'radiant_score': radiant_kills,
        'dire_score': dire_kills,
        'duration_min': duration_min,
        'radiant_win': match_data.get('didRadiantWin', False),
        'start_time': match_data.get('startDateTime', 0),
        'radiant_team_id': radiant_team.get('id', 0),
        'dire_team_id': dire_team.get('id', 0),
        'league_id': league.get('id', 0),
    }
    
    for i, p in enumerate(radiant, 1):
        result[f'radiant_hero_{i}'] = p.get('heroId', 0)
    for i, p in enumerate(dire, 1):
        result[f'dire_hero_{i}'] = p.get('heroId', 0)
    
    return result


def load_pro_matches(json_path: str) -> pd.DataFrame:
    """Загружает про-матчи из JSON."""
    logger.info(f"Loading pro matches from {json_path}...")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    records = []
    for match_id, match_data in tqdm(data.items(), desc="Parsing pro matches"):
        record = extract_pro_match(match_id, match_data)
        if record:
            records.append(record)
    
    df = pd.DataFrame(records)
    df = df.sort_values('start_time').reset_index(drop=True)
    logger.info(f"Loaded {len(df)} pro matches")
    return df


def enrich_with_hero_stats(df: pd.DataFrame, hero_stats_path: str) -> pd.DataFrame:
    """Обогащает матчи метриками героев."""
    hero_stats = pd.read_csv(hero_stats_path)
    hero_stats = hero_stats.set_index('hero_id')
    
    logger.info(f"Loaded hero stats for {len(hero_stats)} heroes")
    
    aggression_map = hero_stats['aggression'].to_dict()
    feed_map = hero_stats['feed'].to_dict()
    pace_map = hero_stats['pace'].to_dict()
    gpm_map = hero_stats['gpm'].to_dict()
    
    default_aggression = hero_stats['aggression'].mean()
    default_feed = hero_stats['feed'].mean()
    default_pace = hero_stats['pace'].mean()
    default_gpm = hero_stats['gpm'].mean()
    
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_aggression'] = df[col].map(lambda x: aggression_map.get(x, default_aggression))
            df[f'{col}_feed'] = df[col].map(lambda x: feed_map.get(x, default_feed))
            df[f'{col}_pace'] = df[col].map(lambda x: pace_map.get(x, default_pace))
            df[f'{col}_gpm'] = df[col].map(lambda x: gpm_map.get(x, default_gpm))
    
    for team in ['radiant', 'dire']:
        aggr_cols = [f'{team}_hero_{i}_aggression' for i in range(1, 6)]
        feed_cols = [f'{team}_hero_{i}_feed' for i in range(1, 6)]
        pace_cols = [f'{team}_hero_{i}_pace' for i in range(1, 6)]
        gpm_cols = [f'{team}_hero_{i}_gpm' for i in range(1, 6)]
        
        df[f'{team}_avg_aggression'] = df[aggr_cols].mean(axis=1)
        df[f'{team}_avg_feed'] = df[feed_cols].mean(axis=1)
        df[f'{team}_avg_pace'] = df[pace_cols].mean(axis=1)
        df[f'{team}_avg_gpm'] = df[gpm_cols].mean(axis=1)
        df[f'{team}_total_aggression'] = df[aggr_cols].sum(axis=1)
        df[f'{team}_total_feed'] = df[feed_cols].sum(axis=1)
    
    df['aggression_diff'] = df['radiant_total_aggression'] - df['dire_total_aggression']
    df['feed_diff'] = df['radiant_total_feed'] - df['dire_total_feed']
    df['combined_aggression'] = df['radiant_total_aggression'] + df['dire_total_aggression']
    df['combined_feed'] = df['radiant_total_feed'] + df['dire_total_feed']
    
    return df


def enrich_with_power_spikes(
    df: pd.DataFrame,
    power_spikes_path: str = 'data/hero_power_spikes.json'
) -> pd.DataFrame:
    """
    Обогащает матчи фичами Power Spikes героев.
    
    Features:
    - radiant/dire_early_power: сумма early_power 5 героев
    - radiant/dire_late_power: сумма late_power 5 героев
    - power_curve_diff: разница power_curve между командами
    - early_vs_late: одна команда early, другая late
    - both_early: обе команды early-game oriented
    - both_late: обе команды late-game oriented
    """
    try:
        with open(power_spikes_path, 'r', encoding='utf-8') as f:
            power_spikes = json.load(f)
        # Convert string keys to int
        power_spikes = {int(k): v for k, v in power_spikes.items()}
        logger.info(f"Loaded power spikes for {len(power_spikes)} heroes")
    except FileNotFoundError:
        logger.warning(f"Power spikes file not found: {power_spikes_path}")
        return df
    
    # Create lookup maps
    early_map = {h: s['early_power'] for h, s in power_spikes.items()}
    late_map = {h: s['late_power'] for h, s in power_spikes.items()}
    curve_map = {h: s['power_curve'] for h, s in power_spikes.items()}
    
    # Defaults (neutral values)
    default_early = np.mean(list(early_map.values()))
    default_late = np.mean(list(late_map.values()))
    default_curve = 0.0
    
    # Per-hero power stats
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_early_power'] = df[col].map(
                lambda x: early_map.get(x, default_early)
            )
            df[f'{col}_late_power'] = df[col].map(
                lambda x: late_map.get(x, default_late)
            )
            df[f'{col}_power_curve'] = df[col].map(
                lambda x: curve_map.get(x, default_curve)
            )
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        early_cols = [f'{team}_hero_{i}_early_power' for i in range(1, 6)]
        late_cols = [f'{team}_hero_{i}_late_power' for i in range(1, 6)]
        curve_cols = [f'{team}_hero_{i}_power_curve' for i in range(1, 6)]
        
        df[f'{team}_early_power'] = df[early_cols].sum(axis=1)
        df[f'{team}_late_power'] = df[late_cols].sum(axis=1)
        df[f'{team}_power_curve'] = df[curve_cols].sum(axis=1)
    
    # Combined features
    df['combined_early_power'] = df['radiant_early_power'] + df['dire_early_power']
    df['combined_late_power'] = df['radiant_late_power'] + df['dire_late_power']
    df['early_power_diff'] = df['radiant_early_power'] - df['dire_early_power']
    df['late_power_diff'] = df['radiant_late_power'] - df['dire_late_power']
    df['power_curve_diff'] = df['radiant_power_curve'] - df['dire_power_curve']
    
    # Timing mismatch features
    # Threshold: power_curve > 0.3 = late team, < -0.3 = early team
    CURVE_THRESHOLD = 0.3
    df['radiant_is_early'] = (df['radiant_power_curve'] < -CURVE_THRESHOLD).astype(int)
    df['radiant_is_late'] = (df['radiant_power_curve'] > CURVE_THRESHOLD).astype(int)
    df['dire_is_early'] = (df['dire_power_curve'] < -CURVE_THRESHOLD).astype(int)
    df['dire_is_late'] = (df['dire_power_curve'] > CURVE_THRESHOLD).astype(int)
    
    # Matchup types
    df['early_vs_late'] = (
        (df['radiant_is_early'] & df['dire_is_late']) |
        (df['radiant_is_late'] & df['dire_is_early'])
    ).astype(int)
    df['both_early_teams'] = (df['radiant_is_early'] & df['dire_is_early']).astype(int)
    df['both_late_teams'] = (df['radiant_is_late'] & df['dire_is_late']).astype(int)
    
    # Power curve cross point (simplified: absolute difference in curves)
    df['power_curve_clash'] = abs(df['radiant_power_curve'] - df['dire_power_curve'])
    
    return df


def enrich_with_save_sustain(
    df: pd.DataFrame,
    healing_stats_path: str = 'data/hero_healing_stats.json'
) -> pd.DataFrame:
    """
    Обогащает матчи фичами Save/Sustain героев.
    
    Гипотеза:
    - Много сейва/хила -> Труднее сделать килл -> Тотал Меньше
    - 0 сейва -> Любой стан = Смерть -> Тотал Больше
    
    Features:
    - radiant/dire_save_count: количество save-героев
    - radiant/dire_heal_score: суммарный healing score
    - combined_save_count: общее количество save-героев
    - no_save_match: обе команды без save-героев
    """
    try:
        with open(healing_stats_path, 'r', encoding='utf-8') as f:
            healing_stats = json.load(f)
        healing_stats = {int(k): v for k, v in healing_stats.items()}
        logger.info(f"Loaded healing stats for {len(healing_stats)} heroes")
    except FileNotFoundError:
        logger.warning(f"Healing stats file not found: {healing_stats_path}")
        return df
    
    # Create lookup maps
    heal_score_map = {h: s['healing_score'] for h, s in healing_stats.items()}
    save_hero_map = {h: 1 if s['is_save_hero'] else 0 for h, s in healing_stats.items()}
    
    default_heal = 1.0  # Neutral
    default_save = 0
    
    # Per-hero stats
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_heal_score'] = df[col].map(
                lambda x: heal_score_map.get(x, default_heal)
            )
            df[f'{col}_is_save'] = df[col].map(
                lambda x: save_hero_map.get(x, default_save)
            )
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        heal_cols = [f'{team}_hero_{i}_heal_score' for i in range(1, 6)]
        save_cols = [f'{team}_hero_{i}_is_save' for i in range(1, 6)]
        
        df[f'{team}_heal_score'] = df[heal_cols].sum(axis=1)
        df[f'{team}_save_count'] = df[save_cols].sum(axis=1)
    
    # Combined features
    df['combined_heal_score'] = df['radiant_heal_score'] + df['dire_heal_score']
    df['combined_save_count'] = df['radiant_save_count'] + df['dire_save_count']
    df['heal_score_diff'] = df['radiant_heal_score'] - df['dire_heal_score']
    df['save_count_diff'] = df['radiant_save_count'] - df['dire_save_count']
    
    # Special flags
    df['no_save_match'] = ((df['radiant_save_count'] == 0) & (df['dire_save_count'] == 0)).astype(int)
    df['high_save_match'] = (df['combined_save_count'] >= 3).astype(int)
    df['save_advantage'] = (abs(df['save_count_diff']) >= 2).astype(int)
    
    return df


def enrich_with_push_defense(
    df: pd.DataFrame,
    push_stats_path: str = 'data/hero_push_stats.json'
) -> pd.DataFrame:
    """
    Обогащает матчи фичами Push/Defense.
    
    Гипотеза:
    - High Push vs Low Defense -> Быстрый снос -> Тотал Меньше
    - Low Push vs High Defense -> Бесконечная осада -> Тотал Больше
    
    Features:
    - radiant/dire_push_score: суммарный push potential
    - radiant/dire_defense_count: количество defense-героев
    - push_vs_defense: high push vs high defense matchup
    """
    try:
        with open(push_stats_path, 'r', encoding='utf-8') as f:
            push_stats = json.load(f)
        push_stats = {int(k): v for k, v in push_stats.items()}
        logger.info(f"Loaded push stats for {len(push_stats)} heroes")
    except FileNotFoundError:
        logger.warning(f"Push stats file not found: {push_stats_path}")
        return df
    
    # Create lookup maps
    push_score_map = {h: s['push_score'] for h, s in push_stats.items()}
    defense_map = {h: 1 if s['is_defense_hero'] else 0 for h, s in push_stats.items()}
    
    default_push = 1.0  # Neutral
    default_defense = 0
    
    # Per-hero stats
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_push_score'] = df[col].map(
                lambda x: push_score_map.get(x, default_push)
            )
            df[f'{col}_is_defense'] = df[col].map(
                lambda x: defense_map.get(x, default_defense)
            )
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        push_cols = [f'{team}_hero_{i}_push_score' for i in range(1, 6)]
        defense_cols = [f'{team}_hero_{i}_is_defense' for i in range(1, 6)]
        
        df[f'{team}_push_score'] = df[push_cols].sum(axis=1)
        df[f'{team}_defense_count'] = df[defense_cols].sum(axis=1)
    
    # Combined features
    df['combined_push_score'] = df['radiant_push_score'] + df['dire_push_score']
    df['combined_defense_count'] = df['radiant_defense_count'] + df['dire_defense_count']
    df['push_score_diff'] = df['radiant_push_score'] - df['dire_push_score']
    df['defense_count_diff'] = df['radiant_defense_count'] - df['dire_defense_count']
    
    # Matchup features
    # High push (>6) vs low defense (0) = fast game
    df['radiant_high_push'] = (df['radiant_push_score'] > 6).astype(int)
    df['dire_high_push'] = (df['dire_push_score'] > 6).astype(int)
    df['radiant_high_defense'] = (df['radiant_defense_count'] >= 2).astype(int)
    df['dire_high_defense'] = (df['dire_defense_count'] >= 2).astype(int)
    
    # Push vs Defense matchups
    df['push_vs_defense'] = (
        (df['radiant_high_push'] & df['dire_high_defense']) |
        (df['dire_high_push'] & df['radiant_high_defense'])
    ).astype(int)
    df['both_high_push'] = (df['radiant_high_push'] & df['dire_high_push']).astype(int)
    df['no_defense_match'] = ((df['radiant_defense_count'] == 0) & (df['dire_defense_count'] == 0)).astype(int)
    
    # Push advantage (one team has much higher push)
    df['push_advantage'] = (abs(df['push_score_diff']) > 2).astype(int)
    
    return df


# Hero IDs for tactical feed features
# Reincarnation / Second Life heroes
REINCARNATION_HEROES: set[int] = {
    42,   # Wraith King (Reincarnation)
    85,   # Undying (Flesh Golem sustain, Aghs zombie)
    110,  # Phoenix (Supernova)
    102,  # Abaddon (Borrowed Time)
    93,   # Meepo (multiple lives effectively)
}

# Buyback & Return (global/high mobility heroes)
BUYBACK_RETURN_HEROES: set[int] = {
    53,   # Nature's Prophet (Teleportation)
    67,   # Spectre (Haunt)
    106,  # Ember Spirit (Remnants)
    63,   # Weaver (Time Lapse)
    13,   # Puck (Phase Shift, Orb)
    17,   # Storm Spirit (Ball Lightning)
    34,   # Tinker (Boots of Travel)
    90,   # Keeper of the Light (Recall)
    74,   # Invoker (Sunstrike global, Ghostwalk escape)
    10,   # Morphling (Replicate)
    44,   # Phantom Assassin (Blur escape)
}

# Death Benefit heroes (useful after death or want to die)
DEATH_BENEFIT_HEROES: set[int] = {
    20,   # Vengeful Spirit (Vengeance Illusion)
    80,   # Techies (Blast Off, mines persist)
    37,   # Warlock (Golem persists)
    45,   # Pugna (Nether Ward persists)
    77,   # Shadow Shaman (Serpent Wards persist)
    85,   # Undying (Tombstone persists)
    66,   # Chen (Holy Persuasion creeps persist)
    95,   # Arc Warden (Tempest Double)
}


def enrich_with_tactical_feed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Tactical Feed.
    
    Гипотеза:
    - Reincarnation heroes: смерти менее критичны -> игра затягивается
    - Buyback Return heroes: могут вернуться в бой -> больше киллов
    - Death Benefit heroes: полезны после смерти -> затягивают игру
    
    Features:
    - radiant/dire_reincarnation_score
    - radiant/dire_buyback_return_potential
    - radiant/dire_death_benefit
    - combined_tactical_feed: общий "инфляционный" потенциал
    """
    logger.info("Computing tactical feed features...")
    
    # Per-hero flags
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_reincarnation'] = df[col].isin(REINCARNATION_HEROES).astype(int)
            df[f'{col}_buyback_return'] = df[col].isin(BUYBACK_RETURN_HEROES).astype(int)
            df[f'{col}_death_benefit'] = df[col].isin(DEATH_BENEFIT_HEROES).astype(int)
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        reincarn_cols = [f'{team}_hero_{i}_reincarnation' for i in range(1, 6)]
        buyback_cols = [f'{team}_hero_{i}_buyback_return' for i in range(1, 6)]
        death_cols = [f'{team}_hero_{i}_death_benefit' for i in range(1, 6)]
        
        df[f'{team}_reincarnation_score'] = df[reincarn_cols].sum(axis=1)
        df[f'{team}_buyback_return_potential'] = df[buyback_cols].sum(axis=1)
        df[f'{team}_death_benefit'] = df[death_cols].sum(axis=1)
        
        # Combined tactical feed score
        df[f'{team}_tactical_feed'] = (
            df[f'{team}_reincarnation_score'] +
            df[f'{team}_buyback_return_potential'] +
            df[f'{team}_death_benefit']
        )
    
    # Combined features
    df['combined_reincarnation'] = df['radiant_reincarnation_score'] + df['dire_reincarnation_score']
    df['combined_buyback_return'] = df['radiant_buyback_return_potential'] + df['dire_buyback_return_potential']
    df['combined_death_benefit'] = df['radiant_death_benefit'] + df['dire_death_benefit']
    df['combined_tactical_feed'] = df['radiant_tactical_feed'] + df['dire_tactical_feed']
    
    # Diff features
    df['reincarnation_diff'] = df['radiant_reincarnation_score'] - df['dire_reincarnation_score']
    df['buyback_return_diff'] = df['radiant_buyback_return_potential'] - df['dire_buyback_return_potential']
    df['death_benefit_diff'] = df['radiant_death_benefit'] - df['dire_death_benefit']
    df['tactical_feed_diff'] = df['radiant_tactical_feed'] - df['dire_tactical_feed']
    
    # Special flags
    df['high_tactical_feed'] = (df['combined_tactical_feed'] >= 4).astype(int)
    df['both_have_reincarnation'] = (
        (df['radiant_reincarnation_score'] > 0) & (df['dire_reincarnation_score'] > 0)
    ).astype(int)
    df['high_buyback_match'] = (df['combined_buyback_return'] >= 3).astype(int)
    
    return df


def enrich_with_cc_initiation(
    df: pd.DataFrame,
    cc_stats_path: str = 'data/hero_cc_stats.json'
) -> pd.DataFrame:
    """
    Обогащает матчи фичами Crowd Control & Initiation.
    
    Гипотеза:
    - Много контроля -> Легкие киллы -> Тотал Больше
    - Нет контроля -> Враги убегают -> Тотал Меньше
    
    Features:
    - radiant/dire_stun_duration: сумма секунд контроля
    - radiant/dire_initiation_score: количество инициаторов
    - catch_potential: initiation - escape (можно ли поймать врага)
    """
    try:
        with open(cc_stats_path, 'r', encoding='utf-8') as f:
            cc_stats = json.load(f)
        cc_stats = {int(k): v for k, v in cc_stats.items()}
        logger.info(f"Loaded CC stats for {len(cc_stats)} heroes")
    except FileNotFoundError:
        logger.warning(f"CC stats file not found: {cc_stats_path}")
        return df
    
    # Create lookup maps
    stun_map = {h: s['stun_duration'] for h, s in cc_stats.items()}
    init_map = {h: 1 if s['is_initiator'] else 0 for h, s in cc_stats.items()}
    cc_score_map = {h: s['cc_score'] for h, s in cc_stats.items()}
    
    default_stun = 1.0
    default_init = 0
    default_cc = 1.0
    
    # Per-hero stats
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_stun_dur'] = df[col].map(
                lambda x: stun_map.get(x, default_stun)
            )
            df[f'{col}_is_initiator'] = df[col].map(
                lambda x: init_map.get(x, default_init)
            )
            df[f'{col}_cc_score'] = df[col].map(
                lambda x: cc_score_map.get(x, default_cc)
            )
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        stun_cols = [f'{team}_hero_{i}_stun_dur' for i in range(1, 6)]
        init_cols = [f'{team}_hero_{i}_is_initiator' for i in range(1, 6)]
        cc_cols = [f'{team}_hero_{i}_cc_score' for i in range(1, 6)]
        
        df[f'{team}_stun_duration'] = df[stun_cols].sum(axis=1)
        df[f'{team}_initiation_score'] = df[init_cols].sum(axis=1)
        df[f'{team}_cc_score'] = df[cc_cols].sum(axis=1)
    
    # Combined features
    df['combined_stun_duration'] = df['radiant_stun_duration'] + df['dire_stun_duration']
    df['combined_initiation'] = df['radiant_initiation_score'] + df['dire_initiation_score']
    df['combined_cc_score'] = df['radiant_cc_score'] + df['dire_cc_score']
    
    df['stun_duration_diff'] = df['radiant_stun_duration'] - df['dire_stun_duration']
    df['initiation_diff'] = df['radiant_initiation_score'] - df['dire_initiation_score']
    df['cc_score_diff'] = df['radiant_cc_score'] - df['dire_cc_score']
    
    # Catch potential: initiation vs escape
    # Use escape count from role composition if available
    if 'radiant_total_escapes' in df.columns:
        df['radiant_catch_potential'] = df['radiant_initiation_score'] - df['dire_total_escapes']
        df['dire_catch_potential'] = df['dire_initiation_score'] - df['radiant_total_escapes']
        df['catch_potential_diff'] = df['radiant_catch_potential'] - df['dire_catch_potential']
    
    # Special flags
    df['high_cc_match'] = (df['combined_cc_score'] > 20).astype(int)
    df['low_cc_match'] = (df['combined_cc_score'] < 12).astype(int)
    df['both_have_initiators'] = (
        (df['radiant_initiation_score'] > 0) & (df['dire_initiation_score'] > 0)
    ).astype(int)
    df['initiation_advantage'] = (abs(df['initiation_diff']) >= 2).astype(int)
    
    return df


# Greedy carry heroes (need lots of farm, AFK farmers)
GREEDY_CARRY_HEROES: set[int] = {
    1,    # Anti-Mage
    10,   # Morphling
    41,   # Faceless Void
    46,   # Templar Assassin
    48,   # Luna
    67,   # Spectre
    70,   # Ursa
    81,   # Chaos Knight
    82,   # Meepo
    94,   # Medusa
    109,  # Terrorblade
    113,  # Arc Warden
    44,   # Phantom Assassin
    89,   # Naga Siren
    93,   # Slark
}

# Active/fighting heroes (want to fight early, not farm)
ACTIVE_HEROES: set[int] = {
    2,    # Axe
    7,    # Earthshaker
    11,   # Sven
    14,   # Pudge
    19,   # Tidehunter
    59,   # Huskar
    71,   # Spirit Breaker
    85,   # Undying
    96,   # Centaur
    100,  # Tusk
    104,  # Legion Commander
    114,  # Monkey King
    129,  # Mars
    137,  # Primal Beast
}


def enrich_with_economy_greed(
    df: pd.DataFrame,
    hero_stats_path: str = 'data/hero_public_stats.csv'
) -> pd.DataFrame:
    """
    Обогащает матчи фичами Economy Greed.
    
    Гипотеза:
    - Два жадных пика (Medusa vs Spectre) -> AFK фарм -> Тотал Меньше
    - Жадный vs Активный -> Активный давит -> Тотал Больше
    
    Features:
    - radiant/dire_gpm_potential: средний GPM героев
    - radiant/dire_greedy_count: количество жадных коров
    - radiant/dire_active_count: количество активных героев
    - both_greedy_carries: обе команды с жадными керри
    """
    try:
        hero_stats = pd.read_csv(hero_stats_path)
        hero_stats = hero_stats.set_index('hero_id')
        gpm_map = hero_stats['gpm'].to_dict()
        default_gpm = hero_stats['gpm'].mean()
        logger.info(f"Loaded GPM stats for {len(gpm_map)} heroes")
    except FileNotFoundError:
        logger.warning(f"Hero stats file not found: {hero_stats_path}")
        return df
    
    # Per-hero stats
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_gpm_potential'] = df[col].map(
                lambda x: gpm_map.get(x, default_gpm)
            )
            df[f'{col}_is_greedy'] = df[col].isin(GREEDY_CARRY_HEROES).astype(int)
            df[f'{col}_is_active'] = df[col].isin(ACTIVE_HEROES).astype(int)
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        gpm_cols = [f'{team}_hero_{i}_gpm_potential' for i in range(1, 6)]
        greedy_cols = [f'{team}_hero_{i}_is_greedy' for i in range(1, 6)]
        active_cols = [f'{team}_hero_{i}_is_active' for i in range(1, 6)]
        
        df[f'{team}_gpm_potential'] = df[gpm_cols].mean(axis=1)
        df[f'{team}_total_gpm_potential'] = df[gpm_cols].sum(axis=1)
        df[f'{team}_greedy_count'] = df[greedy_cols].sum(axis=1)
        df[f'{team}_active_count'] = df[active_cols].sum(axis=1)
    
    # Combined features
    df['combined_gpm_potential'] = df['radiant_gpm_potential'] + df['dire_gpm_potential']
    df['gpm_potential_diff'] = df['radiant_gpm_potential'] - df['dire_gpm_potential']
    df['combined_greedy'] = df['radiant_greedy_count'] + df['dire_greedy_count']
    df['combined_active'] = df['radiant_active_count'] + df['dire_active_count']
    df['greedy_diff'] = df['radiant_greedy_count'] - df['dire_greedy_count']
    df['active_diff'] = df['radiant_active_count'] - df['dire_active_count']
    
    # Map control potential (GPM advantage)
    df['map_control_potential'] = df['radiant_total_gpm_potential'] - df['dire_total_gpm_potential']
    
    # Special flags
    df['both_greedy_carries'] = (
        (df['radiant_greedy_count'] >= 1) & (df['dire_greedy_count'] >= 1)
    ).astype(int)
    df['greedy_vs_active'] = (
        ((df['radiant_greedy_count'] >= 2) & (df['dire_active_count'] >= 2)) |
        ((df['dire_greedy_count'] >= 2) & (df['radiant_active_count'] >= 2))
    ).astype(int)
    df['both_active_teams'] = (
        (df['radiant_active_count'] >= 2) & (df['dire_active_count'] >= 2)
    ).astype(int)
    df['high_greed_match'] = (df['combined_greedy'] >= 3).astype(int)
    
    return df


# BKB-Piercing Stuns/Disables (can interrupt TP through BKB)
# Only TRUE BKB-piercing: Stuns, Taunts, Displacement, Mute
BKB_PIERCE_HEROES: set[int] = {
    # Stuns/Taunts that pierce BKB
    2,    # Axe (Berserker's Call - taunt)
    3,    # Bane (Fiend's Grip - pierces BKB)
    14,   # Pudge (Dismember - pierces BKB)
    33,   # Enigma (Black Hole - pierces BKB)
    38,   # Beastmaster (Primal Roar - pierces BKB)
    41,   # Faceless Void (Chronosphere - pierces BKB)
    51,   # Clockwerk (Hookshot - pierces BKB)
    65,   # Batrider (Flaming Lasso - pierces BKB)
    71,   # Spirit Breaker (Nether Strike - pierces BKB)
    97,   # Magnus (Reverse Polarity - pierces BKB)
    100,  # Tusk (Walrus Punch - pierces BKB)
    104,  # Legion Commander (Duel - pierces BKB)
    112,  # Winter Wyvern (Winter's Curse - pierces BKB)
    137,  # Primal Beast (Pulverize - pierces BKB)
    
    # Displacement/Special
    20,   # Vengeful Spirit (Nether Swap - pierces BKB)
    69,   # Doom (Doom - mute pierces BKB)
    86,   # Rubick (can steal BKB-piercing spells)
}


def enrich_with_bkb_pierce(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами BKB-Piercing Stuns.
    
    Гипотеза:
    - Если нет BKB-pierce -> враги TP-out в лейте -> Тотал Меньше
    - Много BKB-pierce -> можно убить даже с BKB -> Тотал Больше
    
    Features:
    - radiant/dire_bkb_pierce_count: количество героев с BKB-pierce
    - combined_bkb_pierce: общее количество
    - no_bkb_pierce_match: ни у кого нет BKB-pierce
    """
    logger.info("Computing BKB-pierce features...")
    
    # Per-hero flags
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_bkb_pierce'] = df[col].isin(BKB_PIERCE_HEROES).astype(int)
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        bkb_cols = [f'{team}_hero_{i}_bkb_pierce' for i in range(1, 6)]
        df[f'{team}_bkb_pierce_count'] = df[bkb_cols].sum(axis=1)
    
    # Combined features
    df['combined_bkb_pierce'] = df['radiant_bkb_pierce_count'] + df['dire_bkb_pierce_count']
    df['bkb_pierce_diff'] = df['radiant_bkb_pierce_count'] - df['dire_bkb_pierce_count']
    
    # Special flags
    df['no_bkb_pierce_match'] = (df['combined_bkb_pierce'] == 0).astype(int)
    df['high_bkb_pierce_match'] = (df['combined_bkb_pierce'] >= 3).astype(int)
    df['bkb_pierce_advantage'] = (abs(df['bkb_pierce_diff']) >= 2).astype(int)
    
    return df


def compute_rolling_player_dna(df: pd.DataFrame, min_games: int = 5) -> pd.DataFrame:
    """
    Вычисляет Rolling Player DNA - для каждого матча используем только ПРОШЛЫЕ данные.
    
    NO DATA LEAKAGE: DNA игрока вычисляется только на основе матчей ДО текущего.
    
    Для каждой команды вычисляем агрегированные DNA метрики:
    - avg_dna_kills: Средняя кровожадность игроков
    - avg_dna_deaths: Средний фид игроков
    - avg_dna_aggression: Средняя агрессия команды
    - avg_dna_pace: Средний привычный темп
    - avg_dna_duration: Средняя привычная длительность игр
    - dna_versatility: Средняя универсальность
    
    Args:
        df: DataFrame отсортированный по времени (match_id)
        min_games: Минимум игр для расчёта DNA (default: 5)
    """
    logger.info(f"Computing rolling player DNA (min_games={min_games})...")
    
    # DNA метрики
    dna_metrics = ['avg_kills', 'avg_deaths', 'aggression', 'pace', 'feed', 'avg_duration', 'versatility', 'kda']
    
    # Инициализируем колонки
    for team in ['radiant', 'dire']:
        for metric in dna_metrics:
            df[f'{team}_dna_{metric}'] = np.nan
        df[f'{team}_dna_coverage'] = 0.0
    
    # История игроков: player_id -> list of (kills, deaths, assists, duration, hero_id)
    player_history: Dict[str, List[Dict[str, Any]]] = {}
    
    def compute_player_dna(history: List[Dict[str, Any]]) -> Dict[str, float]:
        """Вычисляет DNA из истории игрока."""
        if len(history) < min_games:
            return {}
        
        kills_list = [g['kills'] for g in history]
        deaths_list = [g['deaths'] for g in history]
        assists_list = [g['assists'] for g in history]
        duration_list = [g['duration'] for g in history]
        hero_ids = [g['hero_id'] for g in history]
        
        avg_kills = np.mean(kills_list)
        avg_deaths = np.mean(deaths_list)
        avg_duration = np.mean(duration_list)
        
        total_ka = sum(kills_list) + sum(assists_list)
        total_duration = sum(duration_list)
        
        aggression = total_ka / max(total_duration, 1)
        pace = sum(kills_list) / max(total_duration, 1)
        feed = sum(deaths_list) / max(total_duration, 1)
        versatility = len(set(hero_ids))
        kda = (avg_kills + np.mean(assists_list)) / max(avg_deaths, 1)
        
        return {
            'avg_kills': avg_kills,
            'avg_deaths': avg_deaths,
            'aggression': aggression,
            'pace': pace,
            'feed': feed,
            'avg_duration': avg_duration,
            'versatility': versatility,
            'kda': kda,
        }
    
    def get_team_dna(player_ids: List[str]) -> Dict[str, float]:
        """Агрегирует DNA для команды."""
        values: Dict[str, List[float]] = {metric: [] for metric in dna_metrics}
        found_count = 0
        
        for pid in player_ids:
            if pid in player_history:
                dna = compute_player_dna(player_history[pid])
                if dna:
                    for metric in dna_metrics:
                        if metric in dna:
                            values[metric].append(dna[metric])
                    found_count += 1
        
        result: Dict[str, float] = {}
        for metric in dna_metrics:
            if values[metric]:
                result[metric] = np.mean(values[metric])
            else:
                result[metric] = np.nan
        
        result['coverage'] = found_count / 5.0
        return result
    
    # Итерируем по матчам (должны быть отсортированы по времени!)
    for idx in tqdm(range(len(df)), desc="Rolling player DNA"):
        row = df.iloc[idx]
        
        # Собираем player IDs для обеих команд
        for team in ['radiant', 'dire']:
            player_ids: List[str] = []
            for pos in range(1, 6):
                col = f'{team}_player_{pos}_id'
                if col in row.index and pd.notna(row[col]) and row[col] != 0:
                    player_ids.append(str(int(row[col])))
            
            # Вычисляем DNA на основе ПРОШЛЫХ матчей (до текущего)
            team_dna = get_team_dna(player_ids)
            for metric in dna_metrics:
                df.at[idx, f'{team}_dna_{metric}'] = team_dna.get(metric, np.nan)
            df.at[idx, f'{team}_dna_coverage'] = team_dna['coverage']
        
        # ПОСЛЕ расчёта обновляем историю игроков (no leakage!)
        duration_min = row.get('duration_min', 35)
        for team in ['radiant', 'dire']:
            for pos in range(1, 6):
                player_col = f'{team}_player_{pos}_id'
                kills_col = f'{team}_player_{pos}_kills'
                deaths_col = f'{team}_player_{pos}_deaths'
                assists_col = f'{team}_player_{pos}_assists'
                hero_col = f'{team}_hero_{pos}'
                
                if player_col not in row.index or pd.isna(row[player_col]) or row[player_col] == 0:
                    continue
                
                pid = str(int(row[player_col]))
                
                # Получаем статистику игрока в этом матче
                kills = row.get(kills_col, 0) if pd.notna(row.get(kills_col)) else 0
                deaths = row.get(deaths_col, 0) if pd.notna(row.get(deaths_col)) else 0
                assists = row.get(assists_col, 0) if pd.notna(row.get(assists_col)) else 0
                hero_id = row.get(hero_col, 0) if pd.notna(row.get(hero_col)) else 0
                
                if pid not in player_history:
                    player_history[pid] = []
                
                player_history[pid].append({
                    'kills': kills,
                    'deaths': deaths,
                    'assists': assists,
                    'duration': duration_min,
                    'hero_id': hero_id,
                })
    
    # Комбинированные фичи
    df['combined_dna_kills'] = df['radiant_dna_avg_kills'].fillna(0) + df['dire_dna_avg_kills'].fillna(0)
    df['combined_dna_deaths'] = df['radiant_dna_avg_deaths'].fillna(0) + df['dire_dna_avg_deaths'].fillna(0)
    df['combined_dna_aggression'] = df['radiant_dna_aggression'].fillna(0) + df['dire_dna_aggression'].fillna(0)
    df['combined_dna_pace'] = df['radiant_dna_pace'].fillna(0) + df['dire_dna_pace'].fillna(0)
    
    df['dna_kills_diff'] = df['radiant_dna_avg_kills'].fillna(0) - df['dire_dna_avg_kills'].fillna(0)
    df['dna_aggression_diff'] = df['radiant_dna_aggression'].fillna(0) - df['dire_dna_aggression'].fillna(0)
    df['dna_pace_diff'] = df['radiant_dna_pace'].fillna(0) - df['dire_dna_pace'].fillna(0)
    df['dna_duration_diff'] = df['radiant_dna_avg_duration'].fillna(0) - df['dire_dna_avg_duration'].fillna(0)
    
    # Pace clash
    df['dna_pace_clash'] = abs(df['dna_pace_diff'])
    
    # High/low aggression flags
    valid_aggro = df['combined_dna_aggression'][df['combined_dna_aggression'] > 0]
    if len(valid_aggro) > 0:
        median_aggro = valid_aggro.median()
        df['high_dna_aggression'] = (df['combined_dna_aggression'] > median_aggro * 1.1).astype(int)
        df['low_dna_aggression'] = (df['combined_dna_aggression'] < median_aggro * 0.9).astype(int)
    else:
        df['high_dna_aggression'] = 0
        df['low_dna_aggression'] = 0
    
    # DNA coverage quality
    df['combined_dna_coverage'] = (df['radiant_dna_coverage'] + df['dire_dna_coverage']) / 2
    df['high_dna_coverage'] = (df['combined_dna_coverage'] >= 0.6).astype(int)
    
    # Статистика
    coverage_pct = (df['combined_dna_coverage'] > 0).mean() * 100
    logger.info(f"Rolling DNA computed. Coverage: {coverage_pct:.1f}% of matches have DNA data")
    logger.info(f"Added {len([c for c in df.columns if 'dna' in c.lower()])} Player DNA features")
    
    return df


def enrich_with_player_dna(df: pd.DataFrame, dna_path: str = 'data/player_dna.json') -> pd.DataFrame:
    """
    DEPRECATED: Use compute_rolling_player_dna() instead to avoid data leakage.
    
    This function uses static DNA file which contains future data.
    Kept for backward compatibility with live predictions.
    """
    logger.warning("enrich_with_player_dna() uses static DNA - consider compute_rolling_player_dna() for training")
    
    try:
        with open(dna_path, 'r') as f:
            player_dna = json.load(f)
        logger.info(f"Loaded DNA for {len(player_dna)} players")
    except FileNotFoundError:
        logger.warning(f"Player DNA file not found: {dna_path}")
        return df
    
    # DNA метрики для извлечения
    dna_metrics = ['avg_kills', 'avg_deaths', 'aggression', 'pace', 'feed', 'avg_duration', 'versatility', 'kda']
    
    # Инициализируем колонки
    for team in ['radiant', 'dire']:
        for metric in dna_metrics:
            df[f'{team}_dna_{metric}'] = 0.0
        df[f'{team}_dna_coverage'] = 0.0
    
    def get_team_dna(row: pd.Series, team: str) -> Dict[str, float]:
        """Получает агрегированные DNA метрики для команды."""
        values: Dict[str, List[float]] = {metric: [] for metric in dna_metrics}
        found_count = 0
        
        for pos in range(1, 6):
            player_id = row.get(f'{team}_player_{pos}_id')
            if pd.isna(player_id) or player_id == 0:
                continue
            
            player_id_str = str(int(player_id))
            if player_id_str in player_dna:
                dna = player_dna[player_id_str]
                for metric in dna_metrics:
                    if metric in dna:
                        values[metric].append(dna[metric])
                found_count += 1
        
        result: Dict[str, float] = {}
        for metric in dna_metrics:
            if values[metric]:
                result[metric] = np.mean(values[metric])
            else:
                result[metric] = 0.0
        
        result['coverage'] = found_count / 5.0
        return result
    
    # Применяем к каждой строке
    for idx, row in df.iterrows():
        for team in ['radiant', 'dire']:
            team_dna = get_team_dna(row, team)
            for metric in dna_metrics:
                df.at[idx, f'{team}_dna_{metric}'] = team_dna[metric]
            df.at[idx, f'{team}_dna_coverage'] = team_dna['coverage']
    
    # Комбинированные фичи
    df['combined_dna_kills'] = df['radiant_dna_avg_kills'] + df['dire_dna_avg_kills']
    df['combined_dna_deaths'] = df['radiant_dna_avg_deaths'] + df['dire_dna_avg_deaths']
    df['combined_dna_aggression'] = df['radiant_dna_aggression'] + df['dire_dna_aggression']
    df['combined_dna_pace'] = df['radiant_dna_pace'] + df['dire_dna_pace']
    
    df['dna_kills_diff'] = df['radiant_dna_avg_kills'] - df['dire_dna_avg_kills']
    df['dna_aggression_diff'] = df['radiant_dna_aggression'] - df['dire_dna_aggression']
    df['dna_pace_diff'] = df['radiant_dna_pace'] - df['dire_dna_pace']
    df['dna_duration_diff'] = df['radiant_dna_avg_duration'] - df['dire_dna_avg_duration']
    
    # Pace clash
    df['dna_pace_clash'] = abs(df['dna_pace_diff'])
    
    # High aggression match
    median_aggro = df['combined_dna_aggression'].median()
    df['high_dna_aggression'] = (df['combined_dna_aggression'] > median_aggro * 1.1).astype(int)
    df['low_dna_aggression'] = (df['combined_dna_aggression'] < median_aggro * 0.9).astype(int)
    
    # DNA coverage quality
    df['combined_dna_coverage'] = (df['radiant_dna_coverage'] + df['dire_dna_coverage']) / 2
    df['high_dna_coverage'] = (df['combined_dna_coverage'] >= 0.6).astype(int)
    
    logger.info(f"Added {len([c for c in df.columns if 'dna' in c.lower()])} Player DNA features")
    
    return df


def enrich_with_wave_clear(df: pd.DataFrame, stats_path: str = 'data/hero_wave_clear.json') -> pd.DataFrame:
    """
    Обогащает матчи фичами Wave Clear (способность убивать волны крипов).
    
    Features:
    - radiant_wave_clear, dire_wave_clear: Сумма wave clear скоров команды
    - push_defense_ratio: dire_push / radiant_wave_clear (и наоборот)
    """
    try:
        with open(stats_path, 'r') as f:
            wave_clear = json.load(f)
        logger.info(f"Loaded wave clear stats for {len(wave_clear)} heroes")
    except FileNotFoundError:
        logger.warning(f"Wave clear file not found: {stats_path}")
        return df
    
    def get_team_wave_clear(row: pd.Series, team: str) -> float:
        """Получает сумму wave clear для команды."""
        total = 0.0
        for i in range(1, 6):
            hero_id = row.get(f'{team}_hero_{i}', 0)
            if pd.notna(hero_id) and hero_id != 0:
                hero_str = str(int(hero_id))
                if hero_str in wave_clear:
                    total += wave_clear[hero_str].get('wave_clear_score', 0.5)
                else:
                    total += 0.5  # default
        return total
    
    # Вычисляем wave clear для каждой команды
    df['radiant_wave_clear'] = df.apply(lambda r: get_team_wave_clear(r, 'radiant'), axis=1)
    df['dire_wave_clear'] = df.apply(lambda r: get_team_wave_clear(r, 'dire'), axis=1)
    
    df['combined_wave_clear'] = df['radiant_wave_clear'] + df['dire_wave_clear']
    df['wave_clear_diff'] = df['radiant_wave_clear'] - df['dire_wave_clear']
    
    # Push vs Defense ratio
    # Высокий ratio = много пуша, мало дефа -> быстрый снос -> ТМ
    # Низкий ratio = мало пуша, много дефа -> вечная осада -> ТБ
    
    # Используем push_score из существующих фичей
    if 'dire_push_score' in df.columns and 'radiant_push_score' in df.columns:
        # Radiant defense vs Dire push
        df['radiant_defense_ratio'] = df['radiant_wave_clear'] / (df['dire_push_score'] + 0.1)
        # Dire defense vs Radiant push
        df['dire_defense_ratio'] = df['dire_wave_clear'] / (df['radiant_push_score'] + 0.1)
        
        # Combined push-defense interaction
        df['push_wave_clear_ratio'] = (df['radiant_push_score'] + df['dire_push_score']) / (df['combined_wave_clear'] + 0.1)
        
        # High push, low wave clear -> fast game -> TM
        df['fast_game_potential'] = (df['push_wave_clear_ratio'] > 1.5).astype(int)
        # Low push, high wave clear -> slow game -> TB
        df['slow_game_potential'] = (df['push_wave_clear_ratio'] < 0.8).astype(int)
    
    logger.info(f"Added wave clear features")
    
    return df


def enrich_with_hg_defense(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами High Ground Defense (экспертные тиры).
    
    Гипотеза:
    - Высокий HG defense -> Осада затягивается -> Драки на базе -> Байбеки -> ТБ
    - Низкий HG defense -> Снесли за 20 секунд -> ТМ
    
    Features:
    - radiant_hg_defense, dire_hg_defense: Сумма весов HG defense героев
    - siege_difficulty_radiant: dire_push / (radiant_hg_defense + 1)
    - siege_difficulty_dire: radiant_push / (dire_hg_defense + 1)
    - combined_hg_defense: Общий HG defense обеих команд
    """
    from src.utils.hero_tiers import get_hg_defense_score
    
    logger.info("Computing High Ground Defense features...")
    
    # Per-hero HG defense scores
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_hg_defense'] = df[col].apply(get_hg_defense_score)
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        hg_cols = [f'{team}_hero_{i}_hg_defense' for i in range(1, 6)]
        df[f'{team}_hg_defense'] = df[hg_cols].sum(axis=1)
    
    # Combined features
    df['combined_hg_defense'] = df['radiant_hg_defense'] + df['dire_hg_defense']
    df['hg_defense_diff'] = df['radiant_hg_defense'] - df['dire_hg_defense']
    
    # Siege difficulty: push_score / (hg_defense + 1)
    # Высокий = легко снести (много пуша, мало дефа) -> ТМ
    # Низкий = сложно снести (мало пуша, много дефа) -> ТБ
    if 'dire_push_score' in df.columns and 'radiant_push_score' in df.columns:
        df['siege_difficulty_radiant'] = df['dire_push_score'] / (df['radiant_hg_defense'] + 1)
        df['siege_difficulty_dire'] = df['radiant_push_score'] / (df['dire_hg_defense'] + 1)
        df['combined_siege_difficulty'] = df['siege_difficulty_radiant'] + df['siege_difficulty_dire']
        df['siege_difficulty_diff'] = df['siege_difficulty_radiant'] - df['siege_difficulty_dire']
        
        # Interaction: push vs HG defense
        # High siege difficulty = easy to end = TM
        df['easy_siege_match'] = (df['combined_siege_difficulty'] > 4.0).astype(int)
        # Low siege difficulty = hard to end = TB (more fights)
        df['hard_siege_match'] = (df['combined_siege_difficulty'] < 2.0).astype(int)
    
    # Special flags
    df['high_hg_defense_match'] = (df['combined_hg_defense'] >= 6.0).astype(int)
    df['low_hg_defense_match'] = (df['combined_hg_defense'] <= 2.0).astype(int)
    df['hg_defense_advantage'] = (abs(df['hg_defense_diff']) >= 3.0).astype(int)
    
    # Sniper/Tinker/Techies special: Tier S heroes
    TIER_S_HEROES = {35, 34, 80, 113, 112, 33, 97}  # Sniper, Tinker, Techies, Arc, WW, Enigma, Magnus
    for team in ['radiant', 'dire']:
        tier_s_count = 0
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            tier_s_count += df[col].isin(TIER_S_HEROES).astype(int)
        df[f'{team}_tier_s_defenders'] = tier_s_count
    
    df['combined_tier_s_defenders'] = df['radiant_tier_s_defenders'] + df['dire_tier_s_defenders']
    df['has_tier_s_defender'] = (df['combined_tier_s_defenders'] > 0).astype(int)
    
    logger.info(f"Added HG defense features")
    
    return df


def enrich_with_burst_damage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Burst vs Sustain Damage.
    
    Гипотеза:
    - High Burst -> Смерти без сейва -> ТБ
    - High Sustain (Slow) vs High Heal -> Никто не умирает -> ТМ
    
    Features:
    - radiant_burst_score, dire_burst_score: Сумма burst весов
    - burst_vs_heal: burst_score - heal_score (burst против хила)
    - high_burst_match: обе команды с высоким burst
    """
    from src.utils.hero_tiers import get_burst_score
    
    logger.info("Computing Burst Damage features...")
    
    # Per-hero burst scores
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_burst'] = df[col].apply(get_burst_score)
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        burst_cols = [f'{team}_hero_{i}_burst' for i in range(1, 6)]
        df[f'{team}_burst_score'] = df[burst_cols].sum(axis=1)
    
    # Combined features
    df['combined_burst_score'] = df['radiant_burst_score'] + df['dire_burst_score']
    df['burst_score_diff'] = df['radiant_burst_score'] - df['dire_burst_score']
    
    # Burst vs Heal interaction
    # High burst vs high heal = kills happen (burst wins)
    # Low burst vs high heal = no one dies (heal wins)
    if 'radiant_heal_score' in df.columns and 'dire_heal_score' in df.columns:
        # Radiant burst vs Dire heal
        df['radiant_burst_vs_heal'] = df['radiant_burst_score'] - df['dire_heal_score']
        # Dire burst vs Radiant heal
        df['dire_burst_vs_heal'] = df['dire_burst_score'] - df['radiant_heal_score']
        # Combined: total burst vs total heal
        df['burst_vs_heal_total'] = df['combined_burst_score'] - (df['radiant_heal_score'] + df['dire_heal_score'])
        
        # High burst advantage = more kills
        df['burst_advantage'] = (df['burst_vs_heal_total'] > 5).astype(int)
        # Heal advantage = fewer kills
        df['heal_advantage'] = (df['burst_vs_heal_total'] < -3).astype(int)
    
    # Special flags
    df['high_burst_match'] = (df['combined_burst_score'] >= 12).astype(int)
    df['low_burst_match'] = (df['combined_burst_score'] <= 6).astype(int)
    df['burst_diff_advantage'] = (abs(df['burst_score_diff']) >= 4).astype(int)
    
    # Insta-kill heroes (Tier S)
    TIER_S_BURST = {25, 26, 44, 10, 55, 19, 80}  # Lina, Lion, PA, Morph, Nyx, Tiny, Techies
    for team in ['radiant', 'dire']:
        tier_s_count = 0
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            tier_s_count += df[col].isin(TIER_S_BURST).astype(int)
        df[f'{team}_instakill_heroes'] = tier_s_count
    
    df['combined_instakill_heroes'] = df['radiant_instakill_heroes'] + df['dire_instakill_heroes']
    df['has_instakill_hero'] = (df['combined_instakill_heroes'] > 0).astype(int)
    
    logger.info(f"Added Burst Damage features")
    
    return df


def enrich_with_save_disengage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Save & Disengage.
    
    Гипотеза:
    - High Save vs High Burst -> Сейвы ломают инициацию -> ТМ
    - Low Save vs High Burst -> Все умирают мгновенно -> ТБ
    
    Features:
    - radiant_save_score, dire_save_score: Сумма save весов
    - save_burst_ratio: save_score / (burst_score + 1)
    - high_save_match: обе команды с высоким save
    """
    from src.utils.hero_tiers import get_save_score
    
    logger.info("Computing Save & Disengage features...")
    
    # Per-hero save scores
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_save'] = df[col].apply(get_save_score)
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        save_cols = [f'{team}_hero_{i}_save' for i in range(1, 6)]
        df[f'{team}_save_score'] = df[save_cols].sum(axis=1)
    
    # Combined features
    df['combined_save_score'] = df['radiant_save_score'] + df['dire_save_score']
    df['save_score_diff'] = df['radiant_save_score'] - df['dire_save_score']
    
    # Save vs Burst interaction
    # High save vs high burst = saves win, fewer kills
    # Low save vs high burst = burst wins, more kills
    if 'radiant_burst_score' in df.columns and 'dire_burst_score' in df.columns:
        # Radiant save vs Dire burst
        df['radiant_save_vs_burst'] = df['radiant_save_score'] / (df['dire_burst_score'] + 1)
        # Dire save vs Radiant burst
        df['dire_save_vs_burst'] = df['dire_save_score'] / (df['radiant_burst_score'] + 1)
        # Combined ratio
        df['save_burst_ratio'] = df['combined_save_score'] / (
            df['radiant_burst_score'] + df['dire_burst_score'] + 1
        )
        
        # High save ratio = fewer kills (saves dominate)
        df['save_dominates'] = (df['save_burst_ratio'] > 0.8).astype(int)
        # Low save ratio = more kills (burst dominates)
        df['burst_dominates'] = (df['save_burst_ratio'] < 0.3).astype(int)
    
    # Special flags
    df['high_disengage_match'] = (df['combined_save_score'] >= 5).astype(int)
    df['no_disengage_match'] = (df['combined_save_score'] == 0).astype(int)
    df['save_diff_advantage'] = (abs(df['save_score_diff']) >= 3).astype(int)
    
    # Hard save heroes (Tier S)
    TIER_S_SAVE = {79, 76, 100, 54, 110, 145, 20}  # SD, OD, Tusk, LS, Phoenix, Ringmaster, VS
    for team in ['radiant', 'dire']:
        tier_s_count = 0
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            tier_s_count += df[col].isin(TIER_S_SAVE).astype(int)
        df[f'{team}_hard_save_heroes'] = tier_s_count
    
    df['combined_hard_save'] = df['radiant_hard_save_heroes'] + df['dire_hard_save_heroes']
    df['has_hard_save'] = (df['combined_hard_save'] > 0).astype(int)
    
    logger.info(f"Added Save & Disengage features")
    
    return df


def enrich_with_big_ults(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Big Ultimate (долгие CD, game-changing).
    
    Гипотеза:
    - Много героев с долгими ультами -> Ждут КД -> Пассивность -> ТМ
    
    Features:
    - radiant_big_ult_count, dire_big_ult_count: количество героев с big ults
    - combined_big_ult_count: общее количество
    - high_big_ult_match: много героев зависят от ультов
    """
    from src.utils.hero_tiers import get_big_ult_count
    
    logger.info("Computing Big Ultimate features...")
    
    # Per-hero big ult flags
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_big_ult'] = df[col].apply(get_big_ult_count)
    
    # Team aggregates
    for team in ['radiant', 'dire']:
        big_ult_cols = [f'{team}_hero_{i}_big_ult' for i in range(1, 6)]
        df[f'{team}_big_ult_count'] = df[big_ult_cols].sum(axis=1)
    
    # Combined features
    df['combined_big_ult_count'] = df['radiant_big_ult_count'] + df['dire_big_ult_count']
    df['big_ult_diff'] = df['radiant_big_ult_count'] - df['dire_big_ult_count']
    
    # Special flags
    df['high_big_ult_match'] = (df['combined_big_ult_count'] >= 4).astype(int)
    df['low_big_ult_match'] = (df['combined_big_ult_count'] == 0).astype(int)
    df['big_ult_advantage'] = (abs(df['big_ult_diff']) >= 2).astype(int)
    
    logger.info(f"Added Big Ultimate features")
    
    return df


# ============ VISION CONTROL HEROES ============
VISION_HEROES: dict[int, float] = {
    22: 5.0, 60: 5.0, 62: 4.5, 56: 4.5, 54: 4.0,
    38: 4.0, 66: 4.0, 45: 3.5, 77: 3.5, 85: 3.5,
    15: 3.0, 98: 3.0, 119: 3.0, 9: 2.5, 53: 2.5, 67: 2.5,
}

# ============ SMOKE GANK HEROES ============
SMOKE_GANK_HEROES: dict[int, float] = {
    71: 5.0, 14: 5.0, 17: 5.0, 3: 4.5, 65: 4.5,
    7: 4.5, 100: 4.5, 26: 4.0, 27: 4.0, 25: 4.0,
    106: 4.0, 126: 4.0, 129: 4.0, 62: 3.5, 56: 3.5,
    73: 3.5, 32: 3.5, 93: 3.5,
}

# ============ HIGH GROUND DEFENSE HEROES ============
HG_DEFENSE_HEROES: dict[int, float] = {
    34: 5.0, 52: 5.0, 94: 5.0, 33: 4.5, 29: 4.5,
    97: 4.5, 110: 4.5, 112: 4.0, 89: 4.0, 87: 4.0,
    48: 3.5, 46: 3.5, 64: 3.5, 68: 3.5, 50: 3.0,
    102: 3.0, 111: 3.0,
}

# ============ HIGH GROUND SIEGE HEROES ============
HG_SIEGE_HEROES: dict[int, float] = {
    53: 5.0, 89: 5.0, 109: 5.0, 12: 4.5, 35: 4.5,
    52: 4.5, 64: 4.5, 57: 4.0, 50: 4.0, 91: 4.0,
    48: 3.5, 81: 3.5, 54: 3.5, 70: 3.5, 77: 3.0,
    37: 3.0, 45: 3.0,
}

# ============ AURA HEROES ============
AURA_HEROES: dict[int, float] = {
    38: 5.0, 77: 4.5, 54: 4.5, 96: 4.0, 85: 4.0,
    36: 4.0, 5: 3.5, 84: 3.5, 31: 3.5, 57: 3.0,
    91: 3.0, 98: 3.0, 20: 2.5, 66: 2.5,
}

# ============ DISPEL HEROES ============
DISPEL_HEROES: dict[int, float] = {
    102: 5.0, 111: 5.0, 54: 4.5, 93: 4.5, 79: 4.0,
    57: 4.0, 26: 4.0, 11: 3.5, 63: 3.5, 31: 3.0,
    50: 3.0, 5: 2.5, 37: 2.5,
}

# ============ SHARD TIMING HEROES ============
STRONG_SHARD: dict[int, float] = {
    14: 5.0, 7: 5.0, 17: 5.0, 97: 4.5, 29: 4.5,
    100: 4.5, 71: 4.0, 2: 4.0, 96: 4.0, 104: 4.0,
    25: 3.5, 27: 3.5, 52: 3.5,
}

STRONG_AGHS: dict[int, float] = {
    33: 5.0, 41: 5.0, 63: 5.0, 91: 4.5, 110: 4.5,
    86: 4.5, 22: 4.0, 75: 4.0, 53: 4.0,
}

# ============ MANA DEPENDENCY HEROES ============
MANA_HUNGRY: dict[int, float] = {
    17: 5.0, 52: 5.0, 34: 5.0, 74: 4.5, 22: 4.5,
    25: 4.5, 5: 4.0, 87: 4.0, 64: 4.0, 68: 4.0,
    36: 3.5, 45: 3.5,
}

MANA_INDEPENDENT: dict[int, float] = {
    59: 5.0, 70: 5.0, 81: 4.5, 44: 4.5, 1: 4.0,
    54: 4.0, 11: 4.0, 93: 3.5, 48: 3.5,
}

# ============ TEMPO CONTROL HEROES ============
TEMPO_HEROES: dict[int, float] = {
    71: 5.0, 17: 5.0, 106: 5.0, 126: 4.5, 59: 4.5,
    104: 4.5, 14: 4.0, 65: 4.0, 100: 4.0, 129: 4.0,
    53: 3.5, 48: 3.5, 52: 3.5, 77: 3.5,
}

# ============ OBJECTIVE FOCUS HEROES ============
TOWER_HEROES: dict[int, float] = {
    53: 5.0, 77: 5.0, 52: 5.0, 109: 4.5, 89: 4.5,
    48: 4.5, 64: 4.0, 35: 4.0, 81: 4.0, 70: 3.5, 54: 3.5,
}

ROSH_HEROES: dict[int, float] = {
    70: 5.0, 81: 5.0, 54: 4.5, 109: 4.5, 77: 4.0,
    48: 4.0, 89: 4.0, 67: 3.5, 1: 3.5,
}


def enrich_with_vision_control(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Vision Control.
    
    Гипотеза:
    - Хороший вижн = больше пикоффов = больше киллов
    """
    logger.info("Computing Vision Control features...")
    
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_vision'] = df[col].map(lambda x: VISION_HEROES.get(x, 0.0))
    
    for team in ['radiant', 'dire']:
        vision_cols = [f'{team}_hero_{i}_vision' for i in range(1, 6)]
        df[f'{team}_vision_control'] = df[vision_cols].sum(axis=1)
    
    df['combined_vision_control'] = df['radiant_vision_control'] + df['dire_vision_control']
    df['vision_control_diff'] = df['radiant_vision_control'] - df['dire_vision_control']
    
    return df


def enrich_with_smoke_gank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Smoke Gank Potential.
    
    Гипотеза:
    - Хорошие смок-ганкеры = больше ротаций = больше киллов
    """
    logger.info("Computing Smoke Gank features...")
    
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_smoke_gank'] = df[col].map(lambda x: SMOKE_GANK_HEROES.get(x, 0.0))
    
    for team in ['radiant', 'dire']:
        smoke_cols = [f'{team}_hero_{i}_smoke_gank' for i in range(1, 6)]
        df[f'{team}_smoke_gank'] = df[smoke_cols].sum(axis=1)
    
    df['combined_smoke_gank'] = df['radiant_smoke_gank'] + df['dire_smoke_gank']
    df['smoke_gank_diff'] = df['radiant_smoke_gank'] - df['dire_smoke_gank']
    
    return df


def enrich_with_highground(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами High Ground Defense/Siege.
    
    Гипотеза:
    - Хорошая HG defense = дольше игры = больше киллов
    - Хороший HG siege = быстрее закрытие = меньше киллов
    """
    logger.info("Computing High Ground features...")
    
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_hg_def'] = df[col].map(lambda x: HG_DEFENSE_HEROES.get(x, 0.0))
            df[f'{col}_hg_siege'] = df[col].map(lambda x: HG_SIEGE_HEROES.get(x, 0.0))
    
    for team in ['radiant', 'dire']:
        def_cols = [f'{team}_hero_{i}_hg_def' for i in range(1, 6)]
        siege_cols = [f'{team}_hero_{i}_hg_siege' for i in range(1, 6)]
        df[f'{team}_hg_defense'] = df[def_cols].sum(axis=1)
        df[f'{team}_hg_siege'] = df[siege_cols].sum(axis=1)
    
    df['combined_hg_defense'] = df['radiant_hg_defense'] + df['dire_hg_defense']
    df['hg_defense_diff'] = df['radiant_hg_defense'] - df['dire_hg_defense']
    df['combined_hg_siege'] = df['radiant_hg_siege'] + df['dire_hg_siege']
    df['hg_siege_diff'] = df['radiant_hg_siege'] - df['dire_hg_siege']
    
    # Siege vs Defense matchup
    df['radiant_siege_vs_def'] = df['radiant_hg_siege'] - df['dire_hg_defense']
    df['dire_siege_vs_def'] = df['dire_hg_siege'] - df['radiant_hg_defense']
    df['siege_vs_def_clash'] = abs(df['radiant_siege_vs_def']) + abs(df['dire_siege_vs_def'])
    df['high_hg_defense_match'] = (df['combined_hg_defense'] > 15).astype(int)
    
    return df


def enrich_with_aura_stacking(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Aura Stacking.
    
    Гипотеза:
    - Много аур = сильнее deathball = больше файтов = больше киллов
    """
    logger.info("Computing Aura Stacking features...")
    
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_aura'] = df[col].map(lambda x: AURA_HEROES.get(x, 0.0))
    
    for team in ['radiant', 'dire']:
        aura_cols = [f'{team}_hero_{i}_aura' for i in range(1, 6)]
        df[f'{team}_aura_score'] = df[aura_cols].sum(axis=1)
    
    df['combined_aura_score'] = df['radiant_aura_score'] + df['dire_aura_score']
    df['aura_score_diff'] = df['radiant_aura_score'] - df['dire_aura_score']
    
    return df


def enrich_with_dispel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Dispel Availability.
    
    Гипотеза:
    - Много диспелов = выживаемость = меньше киллов
    """
    logger.info("Computing Dispel features...")
    
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_dispel'] = df[col].map(lambda x: DISPEL_HEROES.get(x, 0.0))
    
    for team in ['radiant', 'dire']:
        dispel_cols = [f'{team}_hero_{i}_dispel' for i in range(1, 6)]
        df[f'{team}_dispel_score'] = df[dispel_cols].sum(axis=1)
    
    df['combined_dispel_score'] = df['radiant_dispel_score'] + df['dire_dispel_score']
    df['dispel_score_diff'] = df['radiant_dispel_score'] - df['dire_dispel_score']
    
    return df


def enrich_with_shard_timing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Shard/Aghs Timing.
    
    Гипотеза:
    - Сильные шарды на 15 мин = power spike = больше активности
    """
    logger.info("Computing Shard/Aghs Timing features...")
    
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_shard'] = df[col].map(lambda x: STRONG_SHARD.get(x, 0.0))
            df[f'{col}_aghs'] = df[col].map(lambda x: STRONG_AGHS.get(x, 0.0))
    
    for team in ['radiant', 'dire']:
        shard_cols = [f'{team}_hero_{i}_shard' for i in range(1, 6)]
        aghs_cols = [f'{team}_hero_{i}_aghs' for i in range(1, 6)]
        df[f'{team}_shard_score'] = df[shard_cols].sum(axis=1)
        df[f'{team}_aghs_score'] = df[aghs_cols].sum(axis=1)
        df[f'{team}_item_timing'] = df[f'{team}_shard_score'] * 0.6 + df[f'{team}_aghs_score'] * 0.4
    
    df['combined_shard_score'] = df['radiant_shard_score'] + df['dire_shard_score']
    df['shard_score_diff'] = df['radiant_shard_score'] - df['dire_shard_score']
    df['combined_aghs_score'] = df['radiant_aghs_score'] + df['dire_aghs_score']
    df['aghs_score_diff'] = df['radiant_aghs_score'] - df['dire_aghs_score']
    df['item_timing_diff'] = df['radiant_item_timing'] - df['dire_item_timing']
    
    return df


def enrich_with_mana_dependency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Mana Dependency.
    
    Гипотеза:
    - Mana-hungry герои уязвимы к mana burn
    - Mana-independent герои могут драться дольше
    """
    logger.info("Computing Mana Dependency features...")
    
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_mana_hungry'] = df[col].map(lambda x: MANA_HUNGRY.get(x, 0.0))
            df[f'{col}_mana_indep'] = df[col].map(lambda x: MANA_INDEPENDENT.get(x, 0.0))
    
    for team in ['radiant', 'dire']:
        hungry_cols = [f'{team}_hero_{i}_mana_hungry' for i in range(1, 6)]
        indep_cols = [f'{team}_hero_{i}_mana_indep' for i in range(1, 6)]
        df[f'{team}_mana_hungry'] = df[hungry_cols].sum(axis=1)
        df[f'{team}_mana_independent'] = df[indep_cols].sum(axis=1)
        df[f'{team}_mana_balance'] = df[f'{team}_mana_independent'] - df[f'{team}_mana_hungry']
    
    df['combined_mana_hungry'] = df['radiant_mana_hungry'] + df['dire_mana_hungry']
    df['mana_hungry_diff'] = df['radiant_mana_hungry'] - df['dire_mana_hungry']
    df['mana_independent_diff'] = df['radiant_mana_independent'] - df['dire_mana_independent']
    
    return df


def enrich_with_tempo_control(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Tempo Control.
    
    Гипотеза:
    - High tempo герои диктуют когда драться = больше активности
    """
    logger.info("Computing Tempo Control features...")
    
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_tempo'] = df[col].map(lambda x: TEMPO_HEROES.get(x, 0.0))
    
    for team in ['radiant', 'dire']:
        tempo_cols = [f'{team}_hero_{i}_tempo' for i in range(1, 6)]
        df[f'{team}_tempo'] = df[tempo_cols].sum(axis=1)
    
    df['combined_tempo'] = df['radiant_tempo'] + df['dire_tempo']
    df['tempo_diff'] = df['radiant_tempo'] - df['dire_tempo']
    df['high_tempo_match'] = (df['combined_tempo'] > 15).astype(int)
    
    return df


def enrich_with_objective_focus(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает матчи фичами Objective Focus.
    
    Гипотеза:
    - High objective focus = быстрее игры = меньше киллов
    """
    logger.info("Computing Objective Focus features...")
    
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            col = f'{team}_hero_{i}'
            df[f'{col}_tower'] = df[col].map(lambda x: TOWER_HEROES.get(x, 0.0))
            df[f'{col}_rosh'] = df[col].map(lambda x: ROSH_HEROES.get(x, 0.0))
    
    for team in ['radiant', 'dire']:
        tower_cols = [f'{team}_hero_{i}_tower' for i in range(1, 6)]
        rosh_cols = [f'{team}_hero_{i}_rosh' for i in range(1, 6)]
        df[f'{team}_tower_score'] = df[tower_cols].sum(axis=1)
        df[f'{team}_rosh_score'] = df[rosh_cols].sum(axis=1)
        df[f'{team}_objective'] = df[f'{team}_tower_score'] * 0.6 + df[f'{team}_rosh_score'] * 0.4
    
    df['combined_objective'] = df['radiant_objective'] + df['dire_objective']
    df['objective_diff'] = df['radiant_objective'] - df['dire_objective']
    df['tower_score_diff'] = df['radiant_tower_score'] - df['dire_tower_score']
    df['rosh_score_diff'] = df['radiant_rosh_score'] - df['dire_rosh_score']
    df['high_objective_match'] = (df['combined_objective'] > 20).astype(int)
    
    return df


def enrich_with_blood_stats(df: pd.DataFrame, stats_path: str = 'data/blood_stats.json') -> pd.DataFrame:
    """
    Обогащает матчи фичами Blood Stats (кровавость героев и связок).
    
    Гипотеза:
    - High blood_score -> Больше киллов -> ТБ
    - High blood_synergy -> Союзники создают больше киллов вместе -> ТБ
    - High blood_clash -> Враги создают больше киллов друг против друга -> ТБ
    
    Features (absolute and per-minute versions):
    - radiant_blood_score, dire_blood_score: Сумма личных blood скоров
    - radiant_blood_synergy, dire_blood_synergy: Сумма синергий пар союзников
    - match_blood_clash: Сумма clash скоров пар врагов
    - *_pm versions: Per-minute normalized versions of all above
    """
    try:
        with open(stats_path, 'r', encoding='utf-8') as f:
            blood_data = json.load(f)
        logger.info(f"Loaded blood stats from {stats_path}")
    except FileNotFoundError:
        logger.warning(f"Blood stats file not found: {stats_path}")
        return df
    
    hero_blood = blood_data.get('hero_blood', {})
    duo_blood = blood_data.get('duo_blood', {})
    vs_blood = blood_data.get('vs_blood', {})
    
    def make_pair_key(h1: int, h2: int) -> str:
        return f"{min(h1, h2)}_{max(h1, h2)}"
    
    def get_team_blood_score(row: pd.Series, team: str, per_minute: bool = False) -> float:
        """Сумма личных blood скоров команды."""
        total = 0.0
        key_name = 'blood_score_pm' if per_minute else 'blood_score'
        for i in range(1, 6):
            hero_id = row.get(f'{team}_hero_{i}', 0)
            if pd.notna(hero_id) and hero_id != 0:
                hero_str = str(int(hero_id))
                if hero_str in hero_blood:
                    total += hero_blood[hero_str].get(key_name, 0)
        return total
    
    def get_team_blood_synergy(row: pd.Series, team: str, per_minute: bool = False) -> float:
        """Сумма синергий пар союзников."""
        total = 0.0
        key_name = 'synergy_pm' if per_minute else 'synergy'
        heroes = []
        for i in range(1, 6):
            hero_id = row.get(f'{team}_hero_{i}', 0)
            if pd.notna(hero_id) and hero_id != 0:
                heroes.append(int(hero_id))
        
        # All pairs within team
        for i, h1 in enumerate(heroes):
            for h2 in heroes[i+1:]:
                key = make_pair_key(h1, h2)
                if key in duo_blood:
                    total += duo_blood[key].get(key_name, 0)
        return total
    
    def get_blood_clash(row: pd.Series, per_minute: bool = False) -> float:
        """Сумма clash скоров пар врагов."""
        total = 0.0
        key_name = 'clash_pm' if per_minute else 'clash'
        radiant_heroes = []
        dire_heroes = []
        
        for i in range(1, 6):
            r_hero = row.get(f'radiant_hero_{i}', 0)
            d_hero = row.get(f'dire_hero_{i}', 0)
            if pd.notna(r_hero) and r_hero != 0:
                radiant_heroes.append(int(r_hero))
            if pd.notna(d_hero) and d_hero != 0:
                dire_heroes.append(int(d_hero))
        
        # All cross-team pairs
        for h1 in radiant_heroes:
            for h2 in dire_heroes:
                key = make_pair_key(h1, h2)
                if key in vs_blood:
                    total += vs_blood[key].get(key_name, 0)
        return total
    
    logger.info("Computing Blood Stats features...")
    
    # ============ ABSOLUTE BLOOD STATS ============
    # Team blood scores
    df['radiant_blood_score'] = df.apply(lambda r: get_team_blood_score(r, 'radiant'), axis=1)
    df['dire_blood_score'] = df.apply(lambda r: get_team_blood_score(r, 'dire'), axis=1)
    df['combined_blood_score'] = df['radiant_blood_score'] + df['dire_blood_score']
    df['blood_score_diff'] = df['radiant_blood_score'] - df['dire_blood_score']
    
    # Team blood synergy (allies)
    df['radiant_blood_synergy'] = df.apply(lambda r: get_team_blood_synergy(r, 'radiant'), axis=1)
    df['dire_blood_synergy'] = df.apply(lambda r: get_team_blood_synergy(r, 'dire'), axis=1)
    df['combined_blood_synergy'] = df['radiant_blood_synergy'] + df['dire_blood_synergy']
    df['blood_synergy_diff'] = df['radiant_blood_synergy'] - df['dire_blood_synergy']
    
    # Blood clash (enemies)
    df['match_blood_clash'] = df.apply(get_blood_clash, axis=1)
    
    # Combined blood potential
    df['total_blood_potential'] = df['combined_blood_score'] + df['combined_blood_synergy'] + df['match_blood_clash']
    
    # ============ PER-MINUTE BLOOD STATS ============
    # Team blood scores (per minute)
    df['radiant_blood_score_pm'] = df.apply(lambda r: get_team_blood_score(r, 'radiant', per_minute=True), axis=1)
    df['dire_blood_score_pm'] = df.apply(lambda r: get_team_blood_score(r, 'dire', per_minute=True), axis=1)
    df['combined_blood_score_pm'] = df['radiant_blood_score_pm'] + df['dire_blood_score_pm']
    df['blood_score_diff_pm'] = df['radiant_blood_score_pm'] - df['dire_blood_score_pm']
    
    # Team blood synergy (per minute)
    df['radiant_blood_synergy_pm'] = df.apply(lambda r: get_team_blood_synergy(r, 'radiant', per_minute=True), axis=1)
    df['dire_blood_synergy_pm'] = df.apply(lambda r: get_team_blood_synergy(r, 'dire', per_minute=True), axis=1)
    df['combined_blood_synergy_pm'] = df['radiant_blood_synergy_pm'] + df['dire_blood_synergy_pm']
    df['blood_synergy_diff_pm'] = df['radiant_blood_synergy_pm'] - df['dire_blood_synergy_pm']
    
    # Blood clash (per minute)
    df['match_blood_clash_pm'] = df.apply(lambda r: get_blood_clash(r, per_minute=True), axis=1)
    
    # Combined blood potential (per minute)
    df['total_blood_potential_pm'] = (df['combined_blood_score_pm'] + 
                                       df['combined_blood_synergy_pm'] + 
                                       df['match_blood_clash_pm'])
    
    # ============ SPECIAL FLAGS ============
    df['high_blood_match'] = (df['total_blood_potential'] > 1.0).astype(int)
    df['low_blood_match'] = (df['total_blood_potential'] < -1.0).astype(int)
    
    logger.info(f"Added Blood Stats features (absolute + per-minute)")
    
    return df


def enrich_with_early_late_counters(
    df: pd.DataFrame,
    counters_path: str = 'data/early_late_counters.json'
) -> pd.DataFrame:
    """
    Обогащает матчи фичами Early/Late Counters из публичных матчей.
    
    Features:
    - counter_1v1: контрпики для early (<34 min) и late (>35 min) игр
    - synergy_2: синергия пар (1+1) для early и late
    - synergy_3: синергия трио (1+1+1) для early и late
    - draft_adv: комбинированное преимущество драфта
    """
    try:
        with open(counters_path, 'r', encoding='utf-8') as f:
            counters = json.load(f)
        logger.info(f"Loaded early/late counters from {counters_path}")
    except FileNotFoundError:
        logger.warning(f"Early/late counters file not found: {counters_path}")
        return df
    
    early_data = counters.get('early', {})
    late_data = counters.get('late', {})
    
    def compute_counter_score(
        team_heroes: List[int], enemy_heroes: List[int], counter_dict: Dict[str, float]
    ) -> float:
        """Compute 1v1 counter score."""
        total = 0.0
        count = 0
        for t_hero in team_heroes:
            for e_hero in enemy_heroes:
                key = f"{t_hero}_vs_{e_hero}"
                wr = counter_dict.get(key)
                if wr is not None:
                    total += (wr - 0.5) * 100  # Convert to advantage %
                    count += 1
        return total / max(count, 1)
    
    def compute_synergy_score(heroes: List[int], syn_dict: Dict[str, float]) -> float:
        """Compute pair synergy score (1+1)."""
        total = 0.0
        count = 0
        for i in range(len(heroes)):
            for j in range(i + 1, len(heroes)):
                key = f"{min(heroes[i], heroes[j])}_{max(heroes[i], heroes[j])}"
                wr = syn_dict.get(key)
                if wr is not None:
                    total += (wr - 0.5) * 100
                    count += 1
        return total / max(count, 1)
    
    def compute_trio_synergy(heroes: List[int], syn_dict: Dict[str, float]) -> float:
        """Compute trio synergy score (1+1+1)."""
        from itertools import combinations
        total = 0.0
        count = 0
        for h1, h2, h3 in combinations(sorted(heroes), 3):
            key = f"{h1}_{h2}_{h3}"
            wr = syn_dict.get(key)
            if wr is not None:
                total += (wr - 0.5) * 100
                count += 1
        return total / max(count, 1)
    
    def compute_counter_2v1(
        team_heroes: List[int], enemy_heroes: List[int], counter_dict: Dict[str, float]
    ) -> float:
        """Compute 2v1 counter score - how well pairs counter single enemies."""
        total = 0.0
        count = 0
        for i in range(len(team_heroes)):
            for j in range(i + 1, len(team_heroes)):
                h1, h2 = int(min(team_heroes[i], team_heroes[j])), int(max(team_heroes[i], team_heroes[j]))
                for enemy in enemy_heroes:
                    key = f"{h1}_{h2}_vs_{int(enemy)}"
                    wr = counter_dict.get(key)
                    if wr is not None:
                        total += (wr - 0.5) * 100
                        count += 1
        return total / max(count, 1)
    
    def compute_counter_1v2(
        team_heroes: List[int], enemy_heroes: List[int], counter_dict: Dict[str, float]
    ) -> float:
        """Compute 1v2 counter score - how well single heroes handle enemy pairs."""
        total = 0.0
        count = 0
        for i in range(len(enemy_heroes)):
            for j in range(i + 1, len(enemy_heroes)):
                e1, e2 = int(min(enemy_heroes[i], enemy_heroes[j])), int(max(enemy_heroes[i], enemy_heroes[j]))
                for hero in team_heroes:
                    key = f"{e1}_{e2}_vs_{int(hero)}"
                    wr = counter_dict.get(key)
                    if wr is not None:
                        # Inverse: enemy pair winning = bad for us
                        total += (0.5 - wr) * 100
                        count += 1
        return total / max(count, 1)
    
    # Get counter/synergy dicts
    early_counter_1v1 = early_data.get('counter_1v1', {})
    late_counter_1v1 = late_data.get('counter_1v1', {})
    early_counter_2v1 = early_data.get('counter_2v1', {})
    late_counter_2v1 = late_data.get('counter_2v1', {})
    early_synergy_2 = early_data.get('synergy_2', {})
    late_synergy_2 = late_data.get('synergy_2', {})
    early_synergy_3 = early_data.get('synergy_3', {})
    late_synergy_3 = late_data.get('synergy_3', {})
    early_mid_1v1 = early_data.get('mid_1v1', {})
    late_mid_1v1 = late_data.get('mid_1v1', {})
    
    logger.info(f"Counter data: early_1v1={len(early_counter_1v1)}, late_1v1={len(late_counter_1v1)}")
    logger.info(f"Counter 2v1: early={len(early_counter_2v1)}, late={len(late_counter_2v1)}")
    logger.info(f"Synergy data: early_2={len(early_synergy_2)}, late_2={len(late_synergy_2)}")
    logger.info(f"Trio data: early_3={len(early_synergy_3)}, late_3={len(late_synergy_3)}")
    logger.info(f"Mid 1v1 data: early={len(early_mid_1v1)}, late={len(late_mid_1v1)}")
    
    def compute_mid_matchup(radiant_mid: int, dire_mid: int, mid_dict: Dict[str, float]) -> float:
        """Compute mid lane 1v1 matchup advantage."""
        key = f"{int(radiant_mid)}_vs_{int(dire_mid)}"
        wr = mid_dict.get(key)
        if wr is not None:
            return (wr - 0.5) * 100  # Radiant advantage
        return 0.0
    
    # Compute features for each row
    results: List[Dict[str, float]] = []
    
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Computing early/late counters"):
        radiant_heroes = [row[f'radiant_hero_{i}'] for i in range(1, 6)]
        dire_heroes = [row[f'dire_hero_{i}'] for i in range(1, 6)]
        
        # Early counter scores
        r_early_counter = compute_counter_score(radiant_heroes, dire_heroes, early_counter_1v1)
        d_early_counter = compute_counter_score(dire_heroes, radiant_heroes, early_counter_1v1)
        
        # Late counter scores
        r_late_counter = compute_counter_score(radiant_heroes, dire_heroes, late_counter_1v1)
        d_late_counter = compute_counter_score(dire_heroes, radiant_heroes, late_counter_1v1)
        
        # Early synergy (pairs)
        r_early_syn = compute_synergy_score(radiant_heroes, early_synergy_2)
        d_early_syn = compute_synergy_score(dire_heroes, early_synergy_2)
        
        # Late synergy (pairs)
        r_late_syn = compute_synergy_score(radiant_heroes, late_synergy_2)
        d_late_syn = compute_synergy_score(dire_heroes, late_synergy_2)
        
        # Trio synergy
        r_trio_early = compute_trio_synergy(radiant_heroes, early_synergy_3)
        d_trio_early = compute_trio_synergy(dire_heroes, early_synergy_3)
        r_trio_late = compute_trio_synergy(radiant_heroes, late_synergy_3)
        d_trio_late = compute_trio_synergy(dire_heroes, late_synergy_3)
        
        # 2v1 counter (pair vs single)
        r_2v1_early = compute_counter_2v1(radiant_heroes, dire_heroes, early_counter_2v1)
        d_2v1_early = compute_counter_2v1(dire_heroes, radiant_heroes, early_counter_2v1)
        r_2v1_late = compute_counter_2v1(radiant_heroes, dire_heroes, late_counter_2v1)
        d_2v1_late = compute_counter_2v1(dire_heroes, radiant_heroes, late_counter_2v1)
        
        # 1v2 counter (single vs pair - inverse)
        r_1v2_early = compute_counter_1v2(radiant_heroes, dire_heroes, early_counter_2v1)
        d_1v2_early = compute_counter_1v2(dire_heroes, radiant_heroes, early_counter_2v1)
        r_1v2_late = compute_counter_1v2(radiant_heroes, dire_heroes, late_counter_2v1)
        d_1v2_late = compute_counter_1v2(dire_heroes, radiant_heroes, late_counter_2v1)
        
        # Mid 1v1 matchup (position 2 is typically index 1)
        radiant_mid = radiant_heroes[1] if len(radiant_heroes) > 1 else radiant_heroes[0]
        dire_mid = dire_heroes[1] if len(dire_heroes) > 1 else dire_heroes[0]
        mid_early = compute_mid_matchup(radiant_mid, dire_mid, early_mid_1v1)
        mid_late = compute_mid_matchup(radiant_mid, dire_mid, late_mid_1v1)
        
        results.append({
            'radiant_early_counter_pub': r_early_counter,
            'dire_early_counter_pub': d_early_counter,
            'early_counter_diff_pub': r_early_counter - d_early_counter,
            'radiant_late_counter_pub': r_late_counter,
            'dire_late_counter_pub': d_late_counter,
            'late_counter_diff_pub': r_late_counter - d_late_counter,
            'radiant_early_synergy_pub': r_early_syn,
            'dire_early_synergy_pub': d_early_syn,
            'combined_early_synergy_pub': r_early_syn + d_early_syn,
            'early_synergy_diff_pub': r_early_syn - d_early_syn,
            'radiant_late_synergy_pub': r_late_syn,
            'dire_late_synergy_pub': d_late_syn,
            'combined_late_synergy_pub': r_late_syn + d_late_syn,
            'late_synergy_diff_pub': r_late_syn - d_late_syn,
            'radiant_trio_synergy_early': r_trio_early,
            'dire_trio_synergy_early': d_trio_early,
            'combined_trio_synergy_early': r_trio_early + d_trio_early,
            'trio_synergy_diff_early': r_trio_early - d_trio_early,
            'radiant_trio_synergy_late': r_trio_late,
            'dire_trio_synergy_late': d_trio_late,
            'combined_trio_synergy_late': r_trio_late + d_trio_late,
            'trio_synergy_diff_late': r_trio_late - d_trio_late,
            # 2v1 counter features
            'radiant_2v1_early': r_2v1_early,
            'dire_2v1_early': d_2v1_early,
            'counter_2v1_diff_early': r_2v1_early - d_2v1_early,
            'radiant_2v1_late': r_2v1_late,
            'dire_2v1_late': d_2v1_late,
            'counter_2v1_diff_late': r_2v1_late - d_2v1_late,
            # 1v2 counter features
            'radiant_1v2_early': r_1v2_early,
            'dire_1v2_early': d_1v2_early,
            'counter_1v2_diff_early': r_1v2_early - d_1v2_early,
            'radiant_1v2_late': r_1v2_late,
            'dire_1v2_late': d_1v2_late,
            'counter_1v2_diff_late': r_1v2_late - d_1v2_late,
            # Combined pair counter
            'radiant_pair_counter_early': r_2v1_early + r_1v2_early,
            'dire_pair_counter_early': d_2v1_early + d_1v2_early,
            'pair_counter_diff_early': (r_2v1_early + r_1v2_early) - (d_2v1_early + d_1v2_early),
            'radiant_pair_counter_late': r_2v1_late + r_1v2_late,
            'dire_pair_counter_late': d_2v1_late + d_1v2_late,
            'pair_counter_diff_late': (r_2v1_late + r_1v2_late) - (d_2v1_late + d_1v2_late),
            # Mid 1v1 matchup
            'mid_matchup_early': mid_early,
            'mid_matchup_late': mid_late,
            'mid_matchup_avg': (mid_early + mid_late) / 2,
            # Combined draft advantage (now includes pair counter + mid)
            'radiant_draft_adv_early': r_early_counter + r_early_syn + r_trio_early + r_2v1_early + mid_early * 0.5,
            'dire_draft_adv_early': d_early_counter + d_early_syn + d_trio_early + d_2v1_early - mid_early * 0.5,
            'draft_adv_diff_early': (r_early_counter + r_early_syn + r_trio_early + r_2v1_early + mid_early * 0.5) - (d_early_counter + d_early_syn + d_trio_early + d_2v1_early - mid_early * 0.5),
            'radiant_draft_adv_late': r_late_counter + r_late_syn + r_trio_late + r_2v1_late + mid_late * 0.5,
            'dire_draft_adv_late': d_late_counter + d_late_syn + d_trio_late + d_2v1_late - mid_late * 0.5,
            'draft_adv_diff_late': (r_late_counter + r_late_syn + r_trio_late + r_2v1_late + mid_late * 0.5) - (d_late_counter + d_late_syn + d_trio_late + d_2v1_late - mid_late * 0.5),
        })
    
    # Add results to dataframe
    results_df = pd.DataFrame(results)
    for col in results_df.columns:
        df[col] = results_df[col].values
    
    logger.info(f"Added {len(results_df.columns)} early/late counter features")
    
    return df


def enrich_with_stratz_draft_features(
    df: pd.DataFrame,
    hero_features_path: str = 'data/hero_features_processed.json'
) -> pd.DataFrame:
    """
    Обогащает матчи фичами из Stratz hero data.
    
    Features:
    - stratz_matchup: контрпики из Stratz API
    - stratz_synergy: синергия из Stratz API
    - role_composition: баланс ролей в команде
    - disable_chain: длительность станов и chain potential
    - damage_balance: баланс физ/маг урона
    """
    try:
        with open(hero_features_path, 'r', encoding='utf-8') as f:
            hero_features = json.load(f)
        logger.info(f"Loaded hero features from {hero_features_path}")
    except FileNotFoundError:
        logger.warning(f"Hero features file not found: {hero_features_path}")
        return df
    
    def compute_stratz_matchup(
        team_heroes: List[int], enemy_heroes: List[int]
    ) -> float:
        """Compute matchup advantage using Stratz data."""
        total = 0.0
        count = 0
        for hero_id in team_heroes:
            hero_data = hero_features.get(str(int(hero_id)), {})
            matchups = hero_data.get('matchups', {})
            for enemy_id in enemy_heroes:
                adv = matchups.get(str(int(enemy_id)), 0.0)
                if adv != 0.0:
                    total += adv
                    count += 1
        return total / max(count, 1)
    
    def compute_stratz_synergy(heroes: List[int]) -> float:
        """Compute synergy using Stratz data."""
        if len(heroes) < 2:
            return 0.0
        total = 0.0
        count = 0
        for i, hero_id in enumerate(heroes):
            hero_data = hero_features.get(str(int(hero_id)), {})
            synergies = hero_data.get('synergies', {})
            for j in range(i + 1, len(heroes)):
                ally_id = heroes[j]
                syn = synergies.get(str(int(ally_id)), 0.0)
                if syn != 0.0:
                    total += syn
                    count += 1
        return total / max(count, 1)
    
    def compute_role_composition(heroes: List[int]) -> Dict[str, float]:
        """Compute role composition for a team."""
        roles = {
            'carry': 0, 'support': 0, 'nuker': 0, 'disabler': 0,
            'initiator': 0, 'durable': 0, 'pusher': 0, 'escape': 0
        }
        for hero_id in heroes:
            hero_data = hero_features.get(str(int(hero_id)), {})
            if hero_data.get('is_carry'):
                roles['carry'] += 1
            if hero_data.get('is_support'):
                roles['support'] += 1
            if hero_data.get('is_nuker'):
                roles['nuker'] += 1
            if hero_data.get('is_disabler'):
                roles['disabler'] += 1
            if hero_data.get('is_initiator'):
                roles['initiator'] += 1
            if hero_data.get('is_durable'):
                roles['durable'] += 1
            if hero_data.get('is_pusher'):
                roles['pusher'] += 1
            if hero_data.get('has_escape'):
                roles['escape'] += 1
        
        roles['teamfight_score'] = roles['disabler'] + roles['initiator'] + roles['nuker']
        roles['splitpush_score'] = roles['pusher'] + roles['escape']
        return roles
    
    def compute_disable_chain(heroes: List[int]) -> Dict[str, float]:
        """Compute disable chain potential."""
        total_stun = 0.0
        stun_count = 0
        bkb_pierce_count = 0
        for hero_id in heroes:
            hero_data = hero_features.get(str(int(hero_id)), {})
            stun_dur = hero_data.get('stun_duration', 0.0)
            if stun_dur > 0:
                total_stun += stun_dur
                stun_count += 1
            if hero_data.get('has_bkb_pierce'):
                bkb_pierce_count += 1
        chain_potential = total_stun * (1 + stun_count * 0.1)
        return {
            'total_stun': total_stun,
            'stun_count': stun_count,
            'bkb_pierce': bkb_pierce_count,
            'chain_potential': chain_potential,
        }
    
    def compute_damage_balance(heroes: List[int]) -> Dict[str, float]:
        """Compute physical vs magical damage balance."""
        physical = 0.0
        magical = 0.0
        for hero_id in heroes:
            hero_data = hero_features.get(str(int(hero_id)), {})
            primary = hero_data.get('primary_attribute', 'str')
            is_nuker = hero_data.get('is_nuker', False)
            if is_nuker:
                magical += 1
            if primary == 'agi':
                physical += 1
            elif primary == 'str':
                physical += 0.5
                magical += 0.5
        balance = 1.0 - abs(physical - magical) / 5.0
        return {
            'physical': physical,
            'magical': magical,
            'balance': max(0.0, balance),
        }
    
    results: List[Dict[str, float]] = []
    
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Computing Stratz draft features"):
        radiant_heroes = [row[f'radiant_hero_{i}'] for i in range(1, 6)]
        dire_heroes = [row[f'dire_hero_{i}'] for i in range(1, 6)]
        
        # Stratz matchup/synergy
        r_matchup = compute_stratz_matchup(radiant_heroes, dire_heroes)
        d_matchup = compute_stratz_matchup(dire_heroes, radiant_heroes)
        r_synergy = compute_stratz_synergy(radiant_heroes)
        d_synergy = compute_stratz_synergy(dire_heroes)
        
        # Role composition
        r_roles = compute_role_composition(radiant_heroes)
        d_roles = compute_role_composition(dire_heroes)
        
        # Disable chain
        r_disable = compute_disable_chain(radiant_heroes)
        d_disable = compute_disable_chain(dire_heroes)
        
        # Damage balance
        r_damage = compute_damage_balance(radiant_heroes)
        d_damage = compute_damage_balance(dire_heroes)
        
        results.append({
            # Stratz matchup/synergy
            'radiant_stratz_matchup': r_matchup,
            'dire_stratz_matchup': d_matchup,
            'stratz_matchup_diff': r_matchup - d_matchup,
            'radiant_stratz_synergy': r_synergy,
            'dire_stratz_synergy': d_synergy,
            'stratz_synergy_diff': r_synergy - d_synergy,
            'radiant_stratz_draft': r_matchup + r_synergy,
            'dire_stratz_draft': d_matchup + d_synergy,
            'stratz_draft_diff': (r_matchup + r_synergy) - (d_matchup + d_synergy),
            # Role composition
            'radiant_carry_count': r_roles['carry'],
            'dire_carry_count': d_roles['carry'],
            'radiant_support_count': r_roles['support'],
            'dire_support_count': d_roles['support'],
            'radiant_disabler_count': r_roles['disabler'],
            'dire_disabler_count': d_roles['disabler'],
            'radiant_initiator_count': r_roles['initiator'],
            'dire_initiator_count': d_roles['initiator'],
            'radiant_durable_count': r_roles['durable'],
            'dire_durable_count': d_roles['durable'],
            'radiant_teamfight_score': r_roles['teamfight_score'],
            'dire_teamfight_score': d_roles['teamfight_score'],
            'teamfight_score_diff': r_roles['teamfight_score'] - d_roles['teamfight_score'],
            'radiant_splitpush_score': r_roles['splitpush_score'],
            'dire_splitpush_score': d_roles['splitpush_score'],
            'splitpush_score_diff': r_roles['splitpush_score'] - d_roles['splitpush_score'],
            # Disable chain
            'radiant_total_stun': r_disable['total_stun'],
            'dire_total_stun': d_disable['total_stun'],
            'total_stun_diff': r_disable['total_stun'] - d_disable['total_stun'],
            'radiant_chain_potential': r_disable['chain_potential'],
            'dire_chain_potential': d_disable['chain_potential'],
            'chain_potential_diff': r_disable['chain_potential'] - d_disable['chain_potential'],
            'radiant_bkb_pierce': r_disable['bkb_pierce'],
            'dire_bkb_pierce': d_disable['bkb_pierce'],
            'bkb_pierce_diff': r_disable['bkb_pierce'] - d_disable['bkb_pierce'],
            # Damage balance
            'radiant_physical_dmg': r_damage['physical'],
            'dire_physical_dmg': d_damage['physical'],
            'radiant_magical_dmg': r_damage['magical'],
            'dire_magical_dmg': d_damage['magical'],
            'radiant_damage_balance': r_damage['balance'],
            'dire_damage_balance': d_damage['balance'],
            'damage_balance_diff': r_damage['balance'] - d_damage['balance'],
        })
    
    results_df = pd.DataFrame(results)
    for col in results_df.columns:
        df[col] = results_df[col].values
    
    logger.info(f"Added {len(results_df.columns)} Stratz draft features")
    
    return df


def enrich_with_advanced_draft_features(
    df: pd.DataFrame,
    hero_features_path: str = 'data/hero_features_processed.json'
) -> pd.DataFrame:
    """
    Обогащает матчи продвинутыми драфт-фичами.
    
    Features:
    - catch_vs_escape: catch score vs enemy escape
    - scaling: late game scaling potential
    - global_presence: heroes with global abilities
    - lane_presence: ranged/melee, armor
    - big_ult_synergy: wombo combo potential
    """
    try:
        with open(hero_features_path, 'r', encoding='utf-8') as f:
            hero_features = json.load(f)
        logger.info(f"Loaded hero features for advanced draft")
    except FileNotFoundError:
        logger.warning(f"Hero features file not found: {hero_features_path}")
        return df
    
    def compute_catch_vs_escape(
        team_heroes: List[int], enemy_heroes: List[int]
    ) -> Dict[str, float]:
        team_catch = 0.0
        enemy_escape = 0.0
        for hero_id in team_heroes:
            hero_data = hero_features.get(str(int(hero_id)), {})
            if hero_data.get('is_disabler'):
                team_catch += 1.5
            if hero_data.get('has_stun'):
                team_catch += 1.0
            if hero_data.get('has_root'):
                team_catch += 0.5
            if hero_data.get('has_silence'):
                team_catch += 0.5
            team_catch += hero_data.get('stun_duration', 0.0) * 0.3
        for hero_id in enemy_heroes:
            hero_data = hero_features.get(str(int(hero_id)), {})
            if hero_data.get('has_escape'):
                enemy_escape += 1.0
            enemy_escape += hero_data.get('evasiveness_rating', 0) * 0.2
        return {
            'catch_score': team_catch,
            'catch_vs_enemy_escape': team_catch - enemy_escape,
        }
    
    def compute_scaling(heroes: List[int]) -> Dict[str, float]:
        total_agi = 0.0
        total_str = 0.0
        carry_scaling = 0.0
        for hero_id in heroes:
            hero_data = hero_features.get(str(int(hero_id)), {})
            agi_gain = hero_data.get('agi_gain', 2.0)
            str_gain = hero_data.get('str_gain', 2.0)
            int_gain = hero_data.get('int_gain', 2.0)
            total_agi += agi_gain
            total_str += str_gain
            if hero_data.get('is_carry') and agi_gain > 2.5:
                carry_scaling += agi_gain
        return {
            'late_scaling': total_agi * 0.4 + total_str * 0.3,
            'carry_scaling': carry_scaling,
            'agi_gain': total_agi,
        }
    
    def compute_global(heroes: List[int]) -> float:
        score = 0.0
        for hero_id in heroes:
            hero_data = hero_features.get(str(int(hero_id)), {})
            if hero_data.get('has_global'):
                score += 2.0
            if int(hero_id) in {53, 9, 22, 75, 91, 114}:
                score += 1.0
        return score
    
    def compute_lane(heroes: List[int]) -> Dict[str, float]:
        ranged = 0
        total_armor = 0.0
        total_range = 0.0
        for hero_id in heroes:
            hero_data = hero_features.get(str(int(hero_id)), {})
            if not hero_data.get('is_melee'):
                ranged += 1
            total_armor += hero_data.get('starting_armor', 0.0)
            total_range += hero_data.get('attack_range', 150)
        return {
            'ranged_count': ranged,
            'total_armor': total_armor,
            'avg_range': total_range / max(len(heroes), 1),
        }
    
    def compute_big_ult(heroes: List[int]) -> float:
        count = 0
        for hero_id in heroes:
            hero_data = hero_features.get(str(int(hero_id)), {})
            if hero_data.get('has_big_ult'):
                count += 1
            if int(hero_id) in {33, 97, 110, 29, 41, 89}:
                count += 1
        if count >= 3:
            return count * 1.5
        elif count >= 2:
            return count * 1.2
        return float(count)
    
    def compute_damage_vs_defense(
        team_heroes: List[int], enemy_heroes: List[int]
    ) -> Dict[str, float]:
        """Compute damage type advantage vs enemy defenses."""
        # Team damage types
        phys = sum(1 for h in team_heroes if hero_features.get(str(int(h)), {}).get('primary_attribute') == 'agi')
        magic = sum(1 for h in team_heroes if hero_features.get(str(int(h)), {}).get('is_nuker', False))
        
        # Enemy defenses
        enemy_armor = sum(hero_features.get(str(int(h)), {}).get('starting_armor', 0) for h in enemy_heroes)
        enemy_magic_res = sum(hero_features.get(str(int(h)), {}).get('starting_magic_armor', 25) for h in enemy_heroes)
        
        phys_adv = phys * 2 - enemy_armor / 10
        magic_adv = magic * 2 - (enemy_magic_res - 125) / 10
        
        return {
            'phys_vs_armor': phys_adv,
            'magic_vs_resist': magic_adv,
            'damage_adv': phys_adv + magic_adv,
        }
    
    def compute_initiation(heroes: List[int]) -> float:
        """Compute initiation score."""
        INITIATORS = {33, 97, 29, 41, 3, 17, 110, 14, 28, 100}
        count = 0
        for h in heroes:
            if int(h) in INITIATORS:
                count += 1
            elif hero_features.get(str(int(h)), {}).get('is_initiator', False):
                count += 1
        return float(count)
    
    def compute_save_score(heroes: List[int]) -> float:
        """Compute save hero score."""
        SAVE_HEROES = {
            50: 5.0, 102: 5.0, 111: 5.0, 112: 4.5, 91: 4.5, 79: 4.0, 76: 4.0, 57: 4.0,
            100: 3.5, 110: 3.5, 20: 3.5, 14: 3.0, 129: 3.0, 3: 3.0, 145: 3.0, 37: 2.5,
            31: 2.5, 85: 2.0, 75: 2.0, 54: 2.0, 41: 2.0, 126: 2.0, 63: 2.0, 12: 1.5,
            89: 1.5, 35: 1.5, 77: 1.5,
        }
        return sum(SAVE_HEROES.get(int(h), 0.0) for h in heroes)
    
    def compute_splitpush(heroes: List[int]) -> float:
        """Compute splitpush threat."""
        SPLITPUSH = {
            53: 5.0, 1: 4.5, 80: 4.5, 46: 4.0, 67: 4.0, 89: 4.0, 12: 3.5, 74: 3.5,
            47: 3.5, 77: 3.5, 93: 3.5, 109: 3.0, 94: 3.0, 81: 3.0, 44: 3.0, 101: 2.5, 63: 2.5,
        }
        return sum(SPLITPUSH.get(int(h), 0.0) for h in heroes)
    
    def compute_roshan(heroes: List[int]) -> float:
        """Compute Roshan potential."""
        ROSHAN = {
            114: 5.0, 44: 4.5, 81: 4.5, 11: 4.5, 8: 4.0, 64: 4.0, 77: 4.0, 49: 4.0,
            109: 4.0, 54: 3.5, 98: 3.5, 6: 3.5, 46: 3.5, 50: 3.0, 20: 3.0, 31: 2.5, 102: 2.5,
        }
        return sum(ROSHAN.get(int(h), 0.0) for h in heroes)
    
    def compute_pickoff(heroes: List[int]) -> float:
        """Compute pickoff potential."""
        PICKOFF = {
            62: 5.0, 56: 5.0, 93: 5.0, 32: 4.5, 73: 4.5, 17: 4.5, 39: 4.0, 13: 4.0,
            106: 4.0, 126: 4.0, 3: 4.0, 65: 3.5, 14: 3.5, 26: 3.5, 45: 3.5, 22: 3.0, 75: 3.0,
        }
        return sum(PICKOFF.get(int(h), 0.0) for h in heroes)
    
    def compute_teamfight(heroes: List[int]) -> float:
        """Compute teamfight potential."""
        TEAMFIGHT = {
            33: 5.0, 97: 5.0, 29: 5.0, 110: 4.5, 41: 4.5, 89: 4.5, 86: 4.5,
            25: 4.0, 27: 4.0, 52: 4.0, 87: 4.0, 112: 4.0, 5: 4.0, 64: 3.5, 68: 3.5,
            30: 3.5, 36: 3.5, 84: 3.5,
        }
        return sum(TEAMFIGHT.get(int(h), 0.0) for h in heroes)
    
    def compute_counter_init(heroes: List[int]) -> float:
        """Compute counter-initiation potential."""
        COUNTER_INIT = {
            86: 5.0, 79: 5.0, 111: 5.0, 102: 4.5, 50: 4.5, 112: 4.5, 75: 4.0, 76: 4.0,
            57: 4.0, 91: 4.0, 20: 3.5, 3: 3.5, 100: 3.5, 37: 3.0, 31: 3.0,
        }
        return sum(COUNTER_INIT.get(int(h), 0.0) for h in heroes)
    
    def compute_late_insurance(heroes: List[int]) -> float:
        """Compute late game insurance."""
        LATE_INS = {
            93: 5.0, 54: 5.0, 98: 4.5, 94: 4.5, 67: 4.5, 1: 4.0, 81: 4.0, 109: 4.0,
            46: 3.5, 44: 3.5, 6: 3.5, 41: 3.5, 89: 3.5, 12: 3.0, 74: 3.0, 80: 3.0,
        }
        return sum(LATE_INS.get(int(h), 0.0) for h in heroes)
    
    def compute_early_dominance(heroes: List[int]) -> float:
        """Compute early game dominance."""
        EARLY_DOM = {
            98: 5.0, 8: 4.5, 93: 4.5, 49: 4.5, 47: 4.0, 40: 4.0, 36: 4.0,
            2: 4.0, 28: 4.0, 17: 3.5, 39: 3.5, 106: 3.5, 56: 3.5, 62: 3.5,
        }
        return sum(EARLY_DOM.get(int(h), 0.0) for h in heroes)
    
    def compute_comeback(heroes: List[int]) -> float:
        """Compute comeback potential."""
        COMEBACK = {
            33: 5.0, 97: 5.0, 29: 4.5, 41: 4.5, 110: 4.0, 89: 4.0, 67: 4.0, 94: 4.0,
            1: 3.5, 109: 3.5, 102: 3.5, 50: 3.5, 111: 3.5,
        }
        return sum(COMEBACK.get(int(h), 0.0) for h in heroes)
    
    results: List[Dict[str, float]] = []
    
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Computing advanced draft features"):
        radiant_heroes = [row[f'radiant_hero_{i}'] for i in range(1, 6)]
        dire_heroes = [row[f'dire_hero_{i}'] for i in range(1, 6)]
        
        r_catch = compute_catch_vs_escape(radiant_heroes, dire_heroes)
        d_catch = compute_catch_vs_escape(dire_heroes, radiant_heroes)
        r_scaling = compute_scaling(radiant_heroes)
        d_scaling = compute_scaling(dire_heroes)
        r_global = compute_global(radiant_heroes)
        d_global = compute_global(dire_heroes)
        r_lane = compute_lane(radiant_heroes)
        d_lane = compute_lane(dire_heroes)
        r_big_ult = compute_big_ult(radiant_heroes)
        d_big_ult = compute_big_ult(dire_heroes)
        
        # New features
        r_dmg = compute_damage_vs_defense(radiant_heroes, dire_heroes)
        d_dmg = compute_damage_vs_defense(dire_heroes, radiant_heroes)
        r_init = compute_initiation(radiant_heroes)
        d_init = compute_initiation(dire_heroes)
        r_save = compute_save_score(radiant_heroes)
        d_save = compute_save_score(dire_heroes)
        
        # Additional draft features
        r_splitpush = compute_splitpush(radiant_heroes)
        d_splitpush = compute_splitpush(dire_heroes)
        r_roshan = compute_roshan(radiant_heroes)
        d_roshan = compute_roshan(dire_heroes)
        r_pickoff = compute_pickoff(radiant_heroes)
        d_pickoff = compute_pickoff(dire_heroes)
        r_teamfight = compute_teamfight(radiant_heroes)
        d_teamfight = compute_teamfight(dire_heroes)
        r_counter_init = compute_counter_init(radiant_heroes)
        d_counter_init = compute_counter_init(dire_heroes)
        
        results.append({
            'radiant_catch_score': r_catch['catch_score'],
            'dire_catch_score': d_catch['catch_score'],
            'catch_score_diff': r_catch['catch_score'] - d_catch['catch_score'],
            'radiant_catch_vs_escape': r_catch['catch_vs_enemy_escape'],
            'dire_catch_vs_escape': d_catch['catch_vs_enemy_escape'],
            'catch_vs_escape_diff': r_catch['catch_vs_enemy_escape'] - d_catch['catch_vs_enemy_escape'],
            'radiant_late_scaling': r_scaling['late_scaling'],
            'dire_late_scaling': d_scaling['late_scaling'],
            'late_scaling_diff': r_scaling['late_scaling'] - d_scaling['late_scaling'],
            'radiant_carry_scaling': r_scaling['carry_scaling'],
            'dire_carry_scaling': d_scaling['carry_scaling'],
            'carry_scaling_diff': r_scaling['carry_scaling'] - d_scaling['carry_scaling'],
            'radiant_agi_gain': r_scaling['agi_gain'],
            'dire_agi_gain': d_scaling['agi_gain'],
            'agi_gain_diff': r_scaling['agi_gain'] - d_scaling['agi_gain'],
            'radiant_global_presence': r_global,
            'dire_global_presence': d_global,
            'global_presence_diff': r_global - d_global,
            'radiant_ranged_count': r_lane['ranged_count'],
            'dire_ranged_count': d_lane['ranged_count'],
            'ranged_diff': r_lane['ranged_count'] - d_lane['ranged_count'],
            'radiant_total_armor': r_lane['total_armor'],
            'dire_total_armor': d_lane['total_armor'],
            'armor_diff': r_lane['total_armor'] - d_lane['total_armor'],
            'radiant_avg_range': r_lane['avg_range'],
            'dire_avg_range': d_lane['avg_range'],
            'range_diff': r_lane['avg_range'] - d_lane['avg_range'],
            'radiant_big_ult_synergy': r_big_ult,
            'dire_big_ult_synergy': d_big_ult,
            'big_ult_synergy_diff': r_big_ult - d_big_ult,
            # Damage vs defense
            'radiant_phys_vs_armor': r_dmg['phys_vs_armor'],
            'dire_phys_vs_armor': d_dmg['phys_vs_armor'],
            'phys_vs_armor_diff': r_dmg['phys_vs_armor'] - d_dmg['phys_vs_armor'],
            'radiant_magic_vs_resist': r_dmg['magic_vs_resist'],
            'dire_magic_vs_resist': d_dmg['magic_vs_resist'],
            'magic_vs_resist_diff': r_dmg['magic_vs_resist'] - d_dmg['magic_vs_resist'],
            'radiant_damage_adv': r_dmg['damage_adv'],
            'dire_damage_adv': d_dmg['damage_adv'],
            'damage_adv_diff': r_dmg['damage_adv'] - d_dmg['damage_adv'],
            # Initiation
            'radiant_initiation': r_init,
            'dire_initiation': d_init,
            'initiation_diff': r_init - d_init,
            # Save score
            'radiant_save_score': r_save,
            'dire_save_score': d_save,
            'save_score_diff': r_save - d_save,
            # Initiation vs save
            'radiant_init_vs_save': r_init - d_save / 3,
            'dire_init_vs_save': d_init - r_save / 3,
            'init_vs_save_diff': (r_init - d_save / 3) - (d_init - r_save / 3),
            # Splitpush
            'radiant_splitpush_threat': r_splitpush,
            'dire_splitpush_threat': d_splitpush,
            'splitpush_threat_diff': r_splitpush - d_splitpush,
            # Roshan
            'radiant_roshan_potential': r_roshan,
            'dire_roshan_potential': d_roshan,
            'roshan_potential_diff': r_roshan - d_roshan,
            # Pickoff
            'radiant_pickoff_potential': r_pickoff,
            'dire_pickoff_potential': d_pickoff,
            'pickoff_potential_diff': r_pickoff - d_pickoff,
            # Teamfight
            'radiant_teamfight_potential': r_teamfight,
            'dire_teamfight_potential': d_teamfight,
            'teamfight_potential_diff': r_teamfight - d_teamfight,
            # Counter-initiation
            'radiant_counter_init': r_counter_init,
            'dire_counter_init': d_counter_init,
            'counter_init_diff': r_counter_init - d_counter_init,
            # Playstyle
            'radiant_tf_vs_pickoff': r_teamfight - r_pickoff,
            'dire_tf_vs_pickoff': d_teamfight - d_pickoff,
            'playstyle_clash': abs((r_teamfight - r_pickoff) - (d_teamfight - d_pickoff)),
        })
    
    results_df = pd.DataFrame(results)
    for col in results_df.columns:
        df[col] = results_df[col].values
    
    logger.info(f"Added {len(results_df.columns)} advanced draft features")
    
    return df


def enrich_with_complex_stats(df: pd.DataFrame, stats_path: str = 'data/complex_hero_stats.json') -> pd.DataFrame:
    """
    Обогащает матчи фичами Complex Stats: Laning, Phase Synergy, Core Trio.
    
    Features:
    - Laning: safe_lane_strength, mid_lane_strength, off_lane_strength, laning_domination
    - Phase: early_synergy, late_synergy, phase_clash
    - Trio: core_trio_score
    """
    try:
        with open(stats_path, 'r') as f:
            stats = json.load(f)
        logger.info(f"Loaded complex stats from {stats_path}")
    except FileNotFoundError:
        logger.warning(f"Complex stats file not found: {stats_path}")
        return df
    
    laning = stats.get('laning_matrix', {})
    phase = stats.get('phase_synergy', {})
    trio = stats.get('core_trio', {})
    pair_wr = stats.get('pair_winrates', {})
    
    def make_pair_key(h1: int, h2: int) -> str:
        return f"{min(h1, h2)}_{max(h1, h2)}"
    
    def make_trio_key(h1: int, h2: int, h3: int) -> str:
        heroes = sorted([h1, h2, h3])
        return f"{heroes[0]}_{heroes[1]}_{heroes[2]}"
    
    def get_lane_strength(heroes: List[int], lane_data: Dict, default: float = 0.5) -> float:
        """Получает среднюю силу линии для героев."""
        if not heroes or not lane_data:
            return default
        
        strengths = []
        for h in heroes:
            # Ищем все пары с этим героем
            for pair_key, data in lane_data.items():
                h1, h2 = map(int, pair_key.split('_'))
                if h == h1 or h == h2:
                    strengths.append(data.get('win_rate', 0.5))
        
        return np.mean(strengths) if strengths else default
    
    def get_phase_synergy(heroes: List[int], phase_type: str) -> float:
        """Получает сумму phase synergy для всех пар."""
        if len(heroes) < 2:
            return 0.0
        
        total = 0.0
        count = 0
        
        for i, h1 in enumerate(heroes):
            for h2 in heroes[i+1:]:
                pair_key = make_pair_key(h1, h2)
                if pair_key in phase:
                    wr = phase[pair_key].get(f'{phase_type}_winrate')
                    if wr is not None:
                        total += wr
                        count += 1
        
        return total / max(count, 1)
    
    def get_trio_score(h1: int, h2: int, h3: int) -> float:
        """Получает winrate тройки или fallback к парам."""
        trio_key = make_trio_key(h1, h2, h3)
        
        if trio_key in trio:
            return trio[trio_key].get('winrate', 0.5)
        
        # Fallback: среднее пар
        pairs = [
            make_pair_key(h1, h2),
            make_pair_key(h1, h3),
            make_pair_key(h2, h3),
        ]
        
        wrs = [pair_wr.get(p, 0.5) for p in pairs if p in pair_wr]
        return np.mean(wrs) if wrs else 0.5
    
    # Инициализируем колонки
    for team in ['radiant', 'dire']:
        df[f'{team}_safe_lane_strength'] = 0.5
        df[f'{team}_mid_lane_strength'] = 0.5
        df[f'{team}_off_lane_strength'] = 0.5
        df[f'{team}_early_synergy'] = 0.5
        df[f'{team}_late_synergy'] = 0.5
        df[f'{team}_core_trio_score'] = 0.5
    
    # Применяем к каждой строке
    for idx, row in df.iterrows():
        for team in ['radiant', 'dire']:
            # Get heroes by position
            heroes = [int(row.get(f'{team}_hero_{i}', 0)) for i in range(1, 6)]
            
            # Pos 1+5 = safe, Pos 2 = mid, Pos 3+4 = off
            safe_heroes = [heroes[0], heroes[4]] if len(heroes) >= 5 else []
            mid_heroes = [heroes[1]] if len(heroes) >= 2 else []
            off_heroes = [heroes[2], heroes[3]] if len(heroes) >= 4 else []
            core_heroes = heroes[:3] if len(heroes) >= 3 else []
            
            # Laning strength
            lane_key = f'{team}_safe' if team == 'radiant' else f'{team}_off'
            df.at[idx, f'{team}_safe_lane_strength'] = get_lane_strength(
                safe_heroes, laning.get('radiant_safe', {})
            )
            df.at[idx, f'{team}_mid_lane_strength'] = get_lane_strength(
                mid_heroes, laning.get('mid', {})
            )
            df.at[idx, f'{team}_off_lane_strength'] = get_lane_strength(
                off_heroes, laning.get('radiant_off', {})
            )
            
            # Phase synergy
            df.at[idx, f'{team}_early_synergy'] = get_phase_synergy(heroes, 'early')
            df.at[idx, f'{team}_late_synergy'] = get_phase_synergy(heroes, 'late')
            
            # Core trio
            if len(core_heroes) == 3:
                df.at[idx, f'{team}_core_trio_score'] = get_trio_score(*core_heroes)
    
    # Combined features
    df['combined_safe_lane'] = df['radiant_safe_lane_strength'] + df['dire_safe_lane_strength']
    df['combined_mid_lane'] = df['radiant_mid_lane_strength'] + df['dire_mid_lane_strength']
    df['combined_off_lane'] = df['radiant_off_lane_strength'] + df['dire_off_lane_strength']
    
    df['laning_domination_radiant'] = (
        df['radiant_safe_lane_strength'] + 
        df['radiant_mid_lane_strength'] + 
        df['radiant_off_lane_strength']
    )
    df['laning_domination_dire'] = (
        df['dire_safe_lane_strength'] + 
        df['dire_mid_lane_strength'] + 
        df['dire_off_lane_strength']
    )
    df['laning_domination_diff'] = df['laning_domination_radiant'] - df['laning_domination_dire']
    
    df['combined_early_synergy'] = df['radiant_early_synergy'] + df['dire_early_synergy']
    df['combined_late_synergy'] = df['radiant_late_synergy'] + df['dire_late_synergy']
    df['early_synergy_diff'] = df['radiant_early_synergy'] - df['dire_early_synergy']
    df['late_synergy_diff'] = df['radiant_late_synergy'] - df['dire_late_synergy']
    df['phase_clash'] = df['early_synergy_diff']  # Кто сильнее в начале
    
    df['combined_trio_score'] = df['radiant_core_trio_score'] + df['dire_core_trio_score']
    df['trio_score_diff'] = df['radiant_core_trio_score'] - df['dire_core_trio_score']
    
    # Snowball potential: high laning + high early synergy
    df['radiant_snowball_potential'] = df['laning_domination_radiant'] * df['radiant_early_synergy']
    df['dire_snowball_potential'] = df['laning_domination_dire'] * df['dire_early_synergy']
    df['snowball_diff'] = df['radiant_snowball_potential'] - df['dire_snowball_potential']
    
    logger.info(f"Added {len([c for c in df.columns if 'lane_strength' in c or 'synergy' in c or 'trio' in c or 'snowball' in c])} Complex Stats features")
    
    return df


def extract_pro_match_with_players(match_id: str, match_data: dict) -> Optional[dict]:
    """Извлекает данные из про-матча включая account_id игроков."""
    players = match_data.get('players', [])
    if len(players) != 10:
        return None
    
    radiant = [p for p in players if p.get('isRadiant')]
    dire = [p for p in players if not p.get('isRadiant')]
    
    if len(radiant) != 5 or len(dire) != 5:
        return None
    
    def sort_key(p: dict) -> int:
        pos = p.get('position') or 'POSITION_5'
        return int(pos.replace('POSITION_', ''))
    
    radiant.sort(key=sort_key)
    dire.sort(key=sort_key)
    
    radiant_kills = sum(p.get('kills', 0) for p in radiant)
    dire_kills = sum(p.get('kills', 0) for p in dire)
    
    dire_kills_arr = match_data.get('direKills') or []
    duration_min = len(dire_kills_arr)
    if duration_min < 10:
        return None
    
    radiant_team = match_data.get('radiantTeam') or {}
    dire_team = match_data.get('direTeam') or {}
    league = match_data.get('league') or {}
    
    # Tournament tier mapping (importance level)
    tier_str = league.get('tier', 'PROFESSIONAL')
    tier_map = {
        'INTERNATIONAL': 2,  # TI, Major finals - highest importance
        'PREMIUM': 2,        # Premium LANs (OpenDota / Stratz)
        'PROFESSIONAL': 1,   # DPC, regular pro matches
        'AMATEUR': 0,        # Lower tier
    }
    tournament_tier = tier_map.get(tier_str, 1)
    
    # Series info for fatigue/decider detection
    series = match_data.get('series') or {}
    series_id = series.get('id', 0) or match_data.get('seriesId', 0)
    series_type = series.get('type', 'BEST_OF_ONE')  # BEST_OF_ONE, BEST_OF_THREE, BEST_OF_FIVE
    
    patch_label = get_patch_label(int(match_data.get('startDateTime', 0)))
    patch_major_label = get_major_patch(patch_label)
    patch_id = get_patch_id(int(match_data.get('startDateTime', 0)))

    result = {
        'match_id': int(match_id),
        'total_kills': radiant_kills + dire_kills,
        'radiant_score': radiant_kills,
        'dire_score': dire_kills,
        'duration_min': duration_min,
        'radiant_win': match_data.get('didRadiantWin', False),
        'start_time': match_data.get('startDateTime', 0),
        'radiant_team_id': radiant_team.get('id', 0),
        'dire_team_id': dire_team.get('id', 0),
        'league_id': league.get('id', 0),
        'region_id': match_data.get('regionId', 0),
        'tournament_tier': tournament_tier,
        'series_id': series_id,
        'series_type': series_type,
        'patch_label': patch_label,
        'patch_major_label': patch_major_label,
        'patch_id': patch_id,
    }
    
    # Hero IDs
    for i, p in enumerate(radiant, 1):
        result[f'radiant_hero_{i}'] = p.get('heroId', 0)
    for i, p in enumerate(dire, 1):
        result[f'dire_hero_{i}'] = p.get('heroId', 0)
    
    # Player account IDs и их статистика в этом матче
    for i, p in enumerate(radiant, 1):
        steam_acc = p.get('steamAccount') or {}
        result[f'radiant_player_{i}_id'] = steam_acc.get('id', 0)
        result[f'radiant_player_{i}_kills'] = p.get('kills', 0)
        result[f'radiant_player_{i}_deaths'] = p.get('deaths', 0)
        result[f'radiant_player_{i}_gpm'] = p.get('goldPerMinute', 0)
        result[f'radiant_player_{i}_assists'] = p.get('assists', 0)
        result[f'radiant_player_{i}_hero_damage'] = p.get('heroDamage', 0)
        result[f'radiant_player_{i}_tower_damage'] = p.get('towerDamage', 0)
        result[f'radiant_player_{i}_dota_plus_xp'] = p.get('dotaPlusHeroXp', 0) or 0
    
    for i, p in enumerate(dire, 1):
        steam_acc = p.get('steamAccount') or {}
        result[f'dire_player_{i}_id'] = steam_acc.get('id', 0)
        result[f'dire_player_{i}_kills'] = p.get('kills', 0)
        result[f'dire_player_{i}_deaths'] = p.get('deaths', 0)
        result[f'dire_player_{i}_gpm'] = p.get('goldPerMinute', 0)
        result[f'dire_player_{i}_assists'] = p.get('assists', 0)
        result[f'dire_player_{i}_hero_damage'] = p.get('heroDamage', 0)
        result[f'dire_player_{i}_tower_damage'] = p.get('towerDamage', 0)
        result[f'dire_player_{i}_dota_plus_xp'] = p.get('dotaPlusHeroXp', 0) or 0
    
    # Team totals for damage
    result['radiant_total_hero_damage'] = sum(p.get('heroDamage', 0) for p in radiant)
    result['radiant_total_tower_damage'] = sum(p.get('towerDamage', 0) for p in radiant)
    result['dire_total_hero_damage'] = sum(p.get('heroDamage', 0) for p in dire)
    result['dire_total_tower_damage'] = sum(p.get('towerDamage', 0) for p in dire)
    
    # Team totals for healing and utility
    result['radiant_total_healing'] = sum(p.get('heroHealing', 0) for p in radiant)
    result['dire_total_healing'] = sum(p.get('heroHealing', 0) for p in dire)
    result['radiant_total_invis'] = sum(p.get('invisibleSeconds', 0) for p in radiant)
    result['dire_total_invis'] = sum(p.get('invisibleSeconds', 0) for p in dire)
    
    return result


def load_pro_matches_with_players(json_path: str) -> pd.DataFrame:
    """Загружает про-матчи из JSON с данными игроков."""
    logger.info(f"Loading pro matches with player data from {json_path}...")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    records = []
    for match_id, match_data in tqdm(data.items(), desc="Parsing pro matches"):
        record = extract_pro_match_with_players(match_id, match_data)
        if record:
            records.append(record)
    
    df = pd.DataFrame(records)
    df = df.sort_values('start_time').reset_index(drop=True)
    logger.info(f"Loaded {len(df)} pro matches with player data")
    return df


def compute_player_rolling_stats(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Вычисляет скользящие средние для каждого игрока.
    ВАЖНО: Использует только прошлые матчи (no data leakage).
    
    Включает ultra-short window (last 3 games) для hot/cold streaks.
    """
    logger.info(f"Computing player rolling stats (window={window})...")
    
    # История игроков: account_id -> list of (kills, deaths, gpm, assists, won)
    player_history: dict[int, list[tuple[int, int, int, int, bool]]] = {}
    
    # Инициализируем колонки для агрегированных фичей
    for team in ['radiant', 'dire']:
        df[f'{team}_players_avg_kills'] = np.nan
        df[f'{team}_players_avg_deaths'] = np.nan
        df[f'{team}_players_avg_gpm'] = np.nan
        df[f'{team}_players_avg_kda'] = np.nan
        # Позиционные фичи (carry=pos1, mid=pos2)
        df[f'{team}_carry_avg_kills'] = np.nan
        df[f'{team}_carry_avg_deaths'] = np.nan
        df[f'{team}_mid_avg_kills'] = np.nan
        df[f'{team}_mid_avg_deaths'] = np.nan
        df[f'{team}_mid_avg_gpm'] = np.nan
        # Ultra-short window: last 3 games
        df[f'{team}_carry_last3_kda'] = np.nan
        df[f'{team}_mid_last3_kda'] = np.nan
        df[f'{team}_carry_hot_streak'] = np.nan
        df[f'{team}_carry_cold_streak'] = np.nan
        df[f'{team}_mid_hot_streak'] = np.nan
        df[f'{team}_mid_cold_streak'] = np.nan
    
    for idx in tqdm(range(len(df)), desc="Player rolling stats"):
        row = df.iloc[idx]
        
        radiant_won = row.get('radiant_win', False)
        
        for team in ['radiant', 'dire']:
            team_kills = []
            team_deaths = []
            team_gpm = []
            team_kda = []
            
            # Определяем, выиграла ли эта команда
            team_won = radiant_won if team == 'radiant' else not radiant_won
            
            for pos in range(1, 6):
                player_id = row.get(f'{team}_player_{pos}_id', 0)
                
                if player_id and player_id in player_history:
                    hist = player_history[player_id][-window:]
                    if len(hist) >= 3:
                        avg_kills = np.mean([h[0] for h in hist])
                        avg_deaths = np.mean([h[1] for h in hist])
                        avg_gpm = np.mean([h[2] for h in hist])
                        avg_assists = np.mean([h[3] for h in hist])
                        kda = (avg_kills + avg_assists) / max(avg_deaths, 1)
                        
                        team_kills.append(avg_kills)
                        team_deaths.append(avg_deaths)
                        team_gpm.append(avg_gpm)
                        team_kda.append(kda)
                        
                        # Ultra-short window: last 3 games
                        last3 = player_history[player_id][-3:]
                        if len(last3) >= 3:
                            last3_kills = np.mean([h[0] for h in last3])
                            last3_deaths = np.mean([h[1] for h in last3])
                            last3_assists = np.mean([h[3] for h in last3])
                            last3_kda = (last3_kills + last3_assists) / max(last3_deaths, 1)
                            last3_wins = sum(1 for h in last3 if h[4])
                            last3_winrate = last3_wins / 3
                            
                            # Hot streak: winrate > 66%
                            hot_streak = 1 if last3_winrate > 0.66 else 0
                            # Cold streak: winrate < 33%
                            cold_streak = 1 if last3_winrate < 0.33 else 0
                            
                            # Позиционные фичи с hot/cold streaks
                            if pos == 1:  # Carry
                                df.at[idx, f'{team}_carry_last3_kda'] = last3_kda
                                df.at[idx, f'{team}_carry_hot_streak'] = hot_streak
                                df.at[idx, f'{team}_carry_cold_streak'] = cold_streak
                            elif pos == 2:  # Mid
                                df.at[idx, f'{team}_mid_last3_kda'] = last3_kda
                                df.at[idx, f'{team}_mid_hot_streak'] = hot_streak
                                df.at[idx, f'{team}_mid_cold_streak'] = cold_streak
                        
                        # Позиционные фичи (long window)
                        if pos == 1:  # Carry
                            df.at[idx, f'{team}_carry_avg_kills'] = avg_kills
                            df.at[idx, f'{team}_carry_avg_deaths'] = avg_deaths
                        elif pos == 2:  # Mid
                            df.at[idx, f'{team}_mid_avg_kills'] = avg_kills
                            df.at[idx, f'{team}_mid_avg_deaths'] = avg_deaths
                            df.at[idx, f'{team}_mid_avg_gpm'] = avg_gpm
            
            # Агрегируем по команде
            if len(team_kills) >= 3:
                df.at[idx, f'{team}_players_avg_kills'] = np.mean(team_kills)
                df.at[idx, f'{team}_players_avg_deaths'] = np.mean(team_deaths)
                df.at[idx, f'{team}_players_avg_gpm'] = np.mean(team_gpm)
                df.at[idx, f'{team}_players_avg_kda'] = np.mean(team_kda)
        
        # Обновляем историю ПОСЛЕ расчета (no leakage)
        for team in ['radiant', 'dire']:
            team_won = radiant_won if team == 'radiant' else not radiant_won
            for pos in range(1, 6):
                player_id = row.get(f'{team}_player_{pos}_id', 0)
                if player_id:
                    if player_id not in player_history:
                        player_history[player_id] = []
                    player_history[player_id].append((
                        row.get(f'{team}_player_{pos}_kills', 0),
                        row.get(f'{team}_player_{pos}_deaths', 0),
                        row.get(f'{team}_player_{pos}_gpm', 0),
                        row.get(f'{team}_player_{pos}_assists', 0),
                        team_won  # Добавляем результат матча
                    ))
    
    # Комбинированные фичи
    df['combined_players_avg_kills'] = (
        df['radiant_players_avg_kills'].fillna(0) + df['dire_players_avg_kills'].fillna(0)
    )
    df['combined_players_avg_deaths'] = (
        df['radiant_players_avg_deaths'].fillna(0) + df['dire_players_avg_deaths'].fillna(0)
    )
    df['combined_mid_aggression'] = (
        df['radiant_mid_avg_kills'].fillna(0) + df['dire_mid_avg_kills'].fillna(0)
    )
    df['players_kda_diff'] = (
        df['radiant_players_avg_kda'].fillna(0) - df['dire_players_avg_kda'].fillna(0)
    )
    
    # Hot/Cold streak комбинированные фичи
    df['radiant_hot_streaks'] = (
        df['radiant_carry_hot_streak'].fillna(0) + df['radiant_mid_hot_streak'].fillna(0)
    )
    df['dire_hot_streaks'] = (
        df['dire_carry_hot_streak'].fillna(0) + df['dire_mid_hot_streak'].fillna(0)
    )
    df['radiant_cold_streaks'] = (
        df['radiant_carry_cold_streak'].fillna(0) + df['radiant_mid_cold_streak'].fillna(0)
    )
    df['dire_cold_streaks'] = (
        df['dire_carry_cold_streak'].fillna(0) + df['dire_mid_cold_streak'].fillna(0)
    )
    df['hot_streak_diff'] = df['radiant_hot_streaks'] - df['dire_hot_streaks']
    df['cold_streak_diff'] = df['radiant_cold_streaks'] - df['dire_cold_streaks']
    df['momentum_score'] = (
        df['radiant_hot_streaks'] - df['radiant_cold_streaks'] -
        df['dire_hot_streaks'] + df['dire_cold_streaks']
    )
    
    # Last3 KDA diff
    df['carry_last3_kda_diff'] = (
        df['radiant_carry_last3_kda'].fillna(0) - df['dire_carry_last3_kda'].fillna(0)
    )
    df['mid_last3_kda_diff'] = (
        df['radiant_mid_last3_kda'].fillna(0) - df['dire_mid_last3_kda'].fillna(0)
    )
    
    return df


def compute_roster_confidence(df: pd.DataFrame, min_games: int = 10) -> pd.DataFrame:
    """
    Вычисляет roster_confidence: % игроков с достаточной историей.
    
    Защита от ноунеймов: если в матче много неизвестных игроков,
    модель не должна делать уверенные ставки.
    
    Features:
    - roster_confidence: (known_players / 10) * 100
    - radiant_known_players: кол-во игроков Radiant с историей >= min_games
    - dire_known_players: кол-во игроков Dire с историей >= min_games
    """
    logger.info(f"Computing roster confidence (min_games={min_games})...")
    
    # История игроков: player_id -> games_count
    player_games_count: dict[int, int] = {}
    
    # Инициализируем колонки
    df['radiant_known_players'] = 0
    df['dire_known_players'] = 0
    df['roster_confidence'] = 0.0
    
    for idx in tqdm(range(len(df)), desc="Roster confidence"):
        row = df.iloc[idx]
        
        radiant_known = 0
        dire_known = 0
        
        # Проверяем Radiant игроков
        for pos in range(1, 6):
            player_id = row.get(f'radiant_player_{pos}_id', 0)
            if player_id and player_games_count.get(player_id, 0) >= min_games:
                radiant_known += 1
        
        # Проверяем Dire игроков
        for pos in range(1, 6):
            player_id = row.get(f'dire_player_{pos}_id', 0)
            if player_id and player_games_count.get(player_id, 0) >= min_games:
                dire_known += 1
        
        total_known = radiant_known + dire_known
        confidence = (total_known / 10) * 100
        
        df.at[idx, 'radiant_known_players'] = radiant_known
        df.at[idx, 'dire_known_players'] = dire_known
        df.at[idx, 'roster_confidence'] = confidence
        
        # Обновляем счётчики ПОСЛЕ расчёта (no leakage)
        for team in ['radiant', 'dire']:
            for pos in range(1, 6):
                player_id = row.get(f'{team}_player_{pos}_id', 0)
                if player_id:
                    player_games_count[player_id] = player_games_count.get(player_id, 0) + 1
    
    # Дополнительные фичи
    df['known_players_diff'] = df['radiant_known_players'] - df['dire_known_players']
    df['low_confidence_match'] = (df['roster_confidence'] < 50).astype(int)
    df['high_confidence_match'] = (df['roster_confidence'] >= 80).astype(int)
    
    return df


def compute_roster_stability(df: pd.DataFrame, min_shared: int = 3) -> pd.DataFrame:
    """
    Computes roster stability based on shared players vs the previous match for the same team.
    If shared players < min_shared, it's treated as a new roster.
    """
    logger.info(f"Computing roster stability (min_shared={min_shared})...")

    for team in ['radiant', 'dire']:
        df[f'{team}_roster_shared_prev'] = np.nan
        df[f'{team}_roster_changed_prev'] = np.nan
        df[f'{team}_roster_stable_prev'] = 0
        df[f'{team}_roster_new_team'] = 0
        df[f'{team}_roster_group_id'] = -1
        df[f'{team}_roster_group_matches'] = 0
        df[f'{team}_roster_player_count'] = 0

    team_state: dict[int, dict[str, Any]] = {}

    for idx in tqdm(range(len(df)), desc="Roster stability"):
        row = df.iloc[idx]

        for side in ['radiant', 'dire']:
            team_id = int(row.get(f'{side}_team_id', 0) or 0)
            if team_id <= 0:
                continue

            roster = {
                int(row.get(f'{side}_player_{pos}_id', 0) or 0)
                for pos in range(1, 6)
                if int(row.get(f'{side}_player_{pos}_id', 0) or 0) > 0
            }
            roster_count = len(roster)
            df.at[idx, f'{side}_roster_player_count'] = roster_count

            prev = team_state.get(team_id)
            if prev is None or roster_count < min_shared:
                df.at[idx, f'{side}_roster_new_team'] = 1
                df.at[idx, f'{side}_roster_group_id'] = 0 if prev is None else prev['group_id'] + 1
                df.at[idx, f'{side}_roster_group_matches'] = 1
                team_state[team_id] = {
                    "roster": roster,
                    "group_id": int(df.at[idx, f'{side}_roster_group_id']),
                    "group_matches": 1,
                }
                continue

            prev_roster = prev.get("roster") or set()
            shared = len(roster & prev_roster)
            df.at[idx, f'{side}_roster_shared_prev'] = shared

            if roster_count == 5 and len(prev_roster) == 5:
                changed = 5 - shared
            else:
                changed = roster_count - shared
            df.at[idx, f'{side}_roster_changed_prev'] = changed

            if shared >= min_shared:
                df.at[idx, f'{side}_roster_stable_prev'] = 1
                df.at[idx, f'{side}_roster_new_team'] = 0
                group_id = prev['group_id']
                group_matches = prev['group_matches'] + 1
            else:
                df.at[idx, f'{side}_roster_stable_prev'] = 0
                df.at[idx, f'{side}_roster_new_team'] = 1
                group_id = prev['group_id'] + 1
                group_matches = 1

            df.at[idx, f'{side}_roster_group_id'] = group_id
            df.at[idx, f'{side}_roster_group_matches'] = group_matches
            team_state[team_id] = {
                "roster": roster,
                "group_id": group_id,
                "group_matches": group_matches,
            }

    return df


def compute_roster_stability_recent(df: pd.DataFrame, min_shared: int = 3) -> pd.DataFrame:
    """
    Computes roster stability starting from the most recent matches.
    This is for analysis only (uses future info), not for training.
    """
    logger.info(f"Computing roster stability from most recent (min_shared={min_shared})...")

    for team in ['radiant', 'dire']:
        df[f'{team}_roster_shared_recent'] = np.nan
        df[f'{team}_roster_changed_recent'] = np.nan
        df[f'{team}_roster_stable_recent'] = 0
        df[f'{team}_roster_new_team_recent'] = 0
        df[f'{team}_roster_group_recent_id'] = -1
        df[f'{team}_roster_group_recent_matches'] = 0

    team_state: dict[int, dict[str, Any]] = {}

    for idx in tqdm(range(len(df) - 1, -1, -1), desc="Roster stability (recent)"):
        row = df.iloc[idx]

        for side in ['radiant', 'dire']:
            team_id = int(row.get(f'{side}_team_id', 0) or 0)
            if team_id <= 0:
                continue

            roster = {
                int(row.get(f'{side}_player_{pos}_id', 0) or 0)
                for pos in range(1, 6)
                if int(row.get(f'{side}_player_{pos}_id', 0) or 0) > 0
            }
            roster_count = len(roster)

            prev = team_state.get(team_id)
            if prev is None or roster_count < min_shared:
                df.at[idx, f'{side}_roster_new_team_recent'] = 1
                df.at[idx, f'{side}_roster_group_recent_id'] = 0 if prev is None else prev['group_id'] + 1
                df.at[idx, f'{side}_roster_group_recent_matches'] = 1
                team_state[team_id] = {
                    "roster": roster,
                    "group_id": int(df.at[idx, f'{side}_roster_group_recent_id']),
                    "group_matches": 1,
                }
                continue

            prev_roster = prev.get("roster") or set()
            shared = len(roster & prev_roster)
            df.at[idx, f'{side}_roster_shared_recent'] = shared

            if roster_count == 5 and len(prev_roster) == 5:
                changed = 5 - shared
            else:
                changed = roster_count - shared
            df.at[idx, f'{side}_roster_changed_recent'] = changed

            if shared >= min_shared:
                df.at[idx, f'{side}_roster_stable_recent'] = 1
                df.at[idx, f'{side}_roster_new_team_recent'] = 0
                group_id = prev['group_id']
                group_matches = prev['group_matches'] + 1
            else:
                df.at[idx, f'{side}_roster_stable_recent'] = 0
                df.at[idx, f'{side}_roster_new_team_recent'] = 1
                group_id = prev['group_id'] + 1
                group_matches = 1

            df.at[idx, f'{side}_roster_group_recent_id'] = group_id
            df.at[idx, f'{side}_roster_group_recent_matches'] = group_matches
            team_state[team_id] = {
                "roster": roster,
                "group_id": group_id,
                "group_matches": group_matches,
            }

    return df


def compute_rolling_team_stats(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Вычисляет скользящие средние для команд.
    ВАЖНО: Использует только прошлые матчи (no data leakage).
    """
    logger.info(f"Computing rolling team stats (window={window})...")
    
    # Инициализируем колонки
    df['radiant_team_avg_kills'] = np.nan
    df['radiant_team_avg_deaths'] = np.nan
    df['radiant_team_aggression_trend'] = np.nan
    df['dire_team_avg_kills'] = np.nan
    df['dire_team_avg_deaths'] = np.nan
    df['dire_team_aggression_trend'] = np.nan
    
    # Собираем историю команд: team_id -> list of (kills, deaths, duration)
    team_history: dict[int, list[tuple[int, int, int]]] = {}
    
    for idx in tqdm(range(len(df)), desc="Rolling team stats"):
        row = df.iloc[idx]
        radiant_tid = row['radiant_team_id']
        dire_tid = row['dire_team_id']
        
        # Считаем статистику для Radiant team
        if radiant_tid and radiant_tid in team_history:
            hist = team_history[radiant_tid][-window:]
            if len(hist) >= 3:
                avg_kills = np.mean([h[0] for h in hist])
                avg_deaths = np.mean([h[1] for h in hist])
                avg_duration = np.mean([h[2] for h in hist])
                aggr_trend = avg_kills / avg_duration if avg_duration > 0 else 0
                
                df.at[idx, 'radiant_team_avg_kills'] = avg_kills
                df.at[idx, 'radiant_team_avg_deaths'] = avg_deaths
                df.at[idx, 'radiant_team_aggression_trend'] = aggr_trend
        
        # Считаем статистику для Dire team
        if dire_tid and dire_tid in team_history:
            hist = team_history[dire_tid][-window:]
            if len(hist) >= 3:
                avg_kills = np.mean([h[0] for h in hist])
                avg_deaths = np.mean([h[1] for h in hist])
                avg_duration = np.mean([h[2] for h in hist])
                aggr_trend = avg_kills / avg_duration if avg_duration > 0 else 0
                
                df.at[idx, 'dire_team_avg_kills'] = avg_kills
                df.at[idx, 'dire_team_avg_deaths'] = avg_deaths
                df.at[idx, 'dire_team_aggression_trend'] = aggr_trend
        
        # Обновляем историю ПОСЛЕ расчета (no leakage)
        if radiant_tid:
            if radiant_tid not in team_history:
                team_history[radiant_tid] = []
            team_history[radiant_tid].append((
                row['radiant_score'],
                row['dire_score'],  # deaths = opponent kills
                row['duration_min']
            ))
        
        if dire_tid:
            if dire_tid not in team_history:
                team_history[dire_tid] = []
            team_history[dire_tid].append((
                row['dire_score'],
                row['radiant_score'],
                row['duration_min']
            ))
    
    # Комбинированные фичи
    df['combined_team_avg_kills'] = df['radiant_team_avg_kills'].fillna(0) + df['dire_team_avg_kills'].fillna(0)
    df['combined_team_aggression'] = df['radiant_team_aggression_trend'].fillna(0) + df['dire_team_aggression_trend'].fillna(0)
    
    return df


def _get_patch_key(row: pd.Series) -> str:
    patch_major = row.get("patch_major_label")
    if isinstance(patch_major, str) and patch_major:
        return patch_major
    patch_label = row.get("patch_label")
    if isinstance(patch_label, str) and patch_label:
        return get_major_patch(patch_label)
    match_time = int(row.get("start_time", 0) or 0)
    return get_major_patch(get_patch_label(match_time))


def compute_patch_rolling_team_stats(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Patch-aware rolling team stats (major patch key).
    Uses only past matches from the same major patch (no leakage).
    """
    logger.info(f"Computing patch-aware rolling team stats (window={window})...")

    for team in ['radiant', 'dire']:
        df[f'{team}_patch_team_avg_kills'] = np.nan
        df[f'{team}_patch_team_avg_deaths'] = np.nan
        df[f'{team}_patch_team_aggression'] = np.nan

    team_history: dict[tuple[int, str], list[tuple[int, int, int]]] = {}

    for idx in tqdm(range(len(df)), desc="Patch rolling team stats"):
        row = df.iloc[idx]
        patch_key = _get_patch_key(row)
        radiant_tid = row['radiant_team_id']
        dire_tid = row['dire_team_id']

        if radiant_tid:
            key = (radiant_tid, patch_key)
            if key in team_history:
                hist = team_history[key][-window:]
                if len(hist) >= 3:
                    avg_kills = np.mean([h[0] for h in hist])
                    avg_deaths = np.mean([h[1] for h in hist])
                    avg_duration = np.mean([h[2] for h in hist])
                    aggr = avg_kills / avg_duration if avg_duration > 0 else 0.0
                    df.at[idx, 'radiant_patch_team_avg_kills'] = avg_kills
                    df.at[idx, 'radiant_patch_team_avg_deaths'] = avg_deaths
                    df.at[idx, 'radiant_patch_team_aggression'] = aggr

        if dire_tid:
            key = (dire_tid, patch_key)
            if key in team_history:
                hist = team_history[key][-window:]
                if len(hist) >= 3:
                    avg_kills = np.mean([h[0] for h in hist])
                    avg_deaths = np.mean([h[1] for h in hist])
                    avg_duration = np.mean([h[2] for h in hist])
                    aggr = avg_kills / avg_duration if avg_duration > 0 else 0.0
                    df.at[idx, 'dire_patch_team_avg_kills'] = avg_kills
                    df.at[idx, 'dire_patch_team_avg_deaths'] = avg_deaths
                    df.at[idx, 'dire_patch_team_aggression'] = aggr

        # Update history AFTER computation (no leakage)
        if radiant_tid:
            key = (radiant_tid, patch_key)
            team_history.setdefault(key, []).append(
                (row['radiant_score'], row['dire_score'], row['duration_min'])
            )
        if dire_tid:
            key = (dire_tid, patch_key)
            team_history.setdefault(key, []).append(
                (row['dire_score'], row['radiant_score'], row['duration_min'])
            )

    df['combined_patch_team_avg_kills'] = (
        df['radiant_patch_team_avg_kills'].fillna(0) + df['dire_patch_team_avg_kills'].fillna(0)
    )
    df['combined_patch_team_aggression'] = (
        df['radiant_patch_team_aggression'].fillna(0) + df['dire_patch_team_aggression'].fillna(0)
    )

    return df


def compute_synthetic_team_stats(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Roster-Aware Team Stats: агрегирует статистику по ИГРОКАМ, а не по team_id.
    
    Patch-Aware Weighted Averaging:
    - Игры текущего патча: вес 1.0
    - Игры старых патчей: вес 0.7
    - Если игр в текущем патче < 5: используем смешанную статистику
    
    Features:
    - radiant_synthetic_avg_kills: сумма avg_kills всех 5 игроков
    - radiant_synthetic_avg_deaths: сумма avg_deaths всех 5 игроков
    - radiant_synthetic_pace: среднее avg_duration всех 5 игроков
    - radiant_synthetic_aggression: kills / duration
    """
    logger.info(f"Computing synthetic (roster-aware) team stats (window={window})...")
    
    # История игроков: player_id -> list of (kills, deaths, assists, duration, total_game_kills, start_time)
    player_game_history: dict[int, list[tuple[int, int, int, int, int, int]]] = {}
    
    # Tier-based defaults для новых игроков
    tier_defaults: dict[int, dict[str, list[float]]] = {
        0: {'kills': [], 'deaths': [], 'duration': []},
        1: {'kills': [], 'deaths': [], 'duration': []},
        2: {'kills': [], 'deaths': [], 'duration': []},
    }
    
    # Инициализируем колонки
    for team in ['radiant', 'dire']:
        df[f'{team}_synthetic_avg_kills'] = np.nan
        df[f'{team}_synthetic_avg_deaths'] = np.nan
        df[f'{team}_synthetic_pace'] = np.nan
        df[f'{team}_synthetic_aggression'] = np.nan
        df[f'{team}_roster_coverage'] = 0.0  # % игроков с историей
    
    for idx in tqdm(range(len(df)), desc="Synthetic team stats"):
        row = df.iloc[idx]
        tournament_tier = row.get('tournament_tier', 1)
        total_game_kills = row['total_kills']
        duration = row['duration_min']
        match_time = row.get('start_time', 0)
        current_patch = get_patch_label(int(match_time))
        current_major = get_major_patch(current_patch)
        
        for team in ['radiant', 'dire']:
            team_kills: list[float] = []
            team_deaths: list[float] = []
            team_durations: list[float] = []
            players_with_history = 0
            
            for pos in range(1, 6):
                player_id = row.get(f'{team}_player_{pos}_id', 0)
                
                if player_id and player_id in player_game_history:
                    hist = player_game_history[player_id][-window:]
                    if len(hist) >= 3:
                        # Patch-aware weighted averaging
                        weights: list[float] = []
                        kills_vals: list[float] = []
                        deaths_vals: list[float] = []
                        duration_vals: list[float] = []
                        current_patch_games = 0
                        
                        for h in hist:
                            game_time = h[5]
                            game_patch = get_patch_label(int(game_time))
                            game_major = get_major_patch(game_patch)
                            
                            # Вес: 1.0 для текущего патча, 0.7 для старых
                            weight = 1.0 if game_major == current_major else 0.7
                            weights.append(weight)
                            kills_vals.append(h[0])
                            deaths_vals.append(h[1])
                            duration_vals.append(h[3])
                            
                            if game_major == current_major:
                                current_patch_games += 1
                        
                        # Weighted average
                        total_weight = sum(weights)
                        avg_kills = sum(k * w for k, w in zip(kills_vals, weights)) / total_weight
                        avg_deaths = sum(d * w for d, w in zip(deaths_vals, weights)) / total_weight
                        avg_duration = sum(d * w for d, w in zip(duration_vals, weights)) / total_weight
                        
                        team_kills.append(avg_kills)
                        team_deaths.append(avg_deaths)
                        team_durations.append(avg_duration)
                        players_with_history += 1
                
                # Fallback: tier-based default для новых игроков
                elif tournament_tier in tier_defaults:
                    tier_data = tier_defaults[tournament_tier]
                    if len(tier_data['kills']) >= 50:
                        # Используем медиану по tier
                        team_kills.append(np.median(tier_data['kills'][-500:]))
                        team_deaths.append(np.median(tier_data['deaths'][-500:]))
                        team_durations.append(np.median(tier_data['duration'][-500:]))
            
            # Записываем synthetic stats если есть хотя бы 3 игрока с данными
            if len(team_kills) >= 3:
                synthetic_kills = sum(team_kills)
                synthetic_deaths = sum(team_deaths)
                avg_pace = np.mean(team_durations) if team_durations else 35
                synthetic_aggression = synthetic_kills / max(avg_pace, 1)
                
                df.at[idx, f'{team}_synthetic_avg_kills'] = synthetic_kills
                df.at[idx, f'{team}_synthetic_avg_deaths'] = synthetic_deaths
                df.at[idx, f'{team}_synthetic_pace'] = avg_pace
                df.at[idx, f'{team}_synthetic_aggression'] = synthetic_aggression
                df.at[idx, f'{team}_roster_coverage'] = players_with_history / 5
        
        # === Обновляем историю ПОСЛЕ расчёта (no leakage) ===
        for team in ['radiant', 'dire']:
            for pos in range(1, 6):
                player_id = row.get(f'{team}_player_{pos}_id', 0)
                player_kills = row.get(f'{team}_player_{pos}_kills', 0)
                player_deaths = row.get(f'{team}_player_{pos}_deaths', 0)
                player_assists = row.get(f'{team}_player_{pos}_assists', 0)
                
                if player_id:
                    if player_id not in player_game_history:
                        player_game_history[player_id] = []
                    player_game_history[player_id].append((
                        player_kills,
                        player_deaths,
                        player_assists,
                        duration,
                        total_game_kills,
                        match_time  # Добавляем timestamp для patch-aware weighting
                    ))
        
        # Обновляем tier defaults
        if tournament_tier in tier_defaults:
            # Средние kills/deaths на игрока в этом матче
            avg_player_kills = total_game_kills / 10
            avg_player_deaths = total_game_kills / 10  # примерно равно
            tier_defaults[tournament_tier]['kills'].append(avg_player_kills)
            tier_defaults[tournament_tier]['deaths'].append(avg_player_deaths)
            tier_defaults[tournament_tier]['duration'].append(duration)
    
    # Комбинированные фичи
    df['combined_synthetic_kills'] = (
        df['radiant_synthetic_avg_kills'].fillna(0) + df['dire_synthetic_avg_kills'].fillna(0)
    )
    df['combined_synthetic_deaths'] = (
        df['radiant_synthetic_avg_deaths'].fillna(0) + df['dire_synthetic_avg_deaths'].fillna(0)
    )
    df['combined_synthetic_aggression'] = (
        df['radiant_synthetic_aggression'].fillna(0) + df['dire_synthetic_aggression'].fillna(0)
    )
    df['synthetic_kills_diff'] = (
        df['radiant_synthetic_avg_kills'].fillna(0) - df['dire_synthetic_avg_kills'].fillna(0)
    )
    df['synthetic_pace_diff'] = (
        df['radiant_synthetic_pace'].fillna(35) - df['dire_synthetic_pace'].fillna(35)
    )
    
    # Roster coverage (насколько полные данные)
    df['combined_roster_coverage'] = (
        df['radiant_roster_coverage'] + df['dire_roster_coverage']
    ) / 2
    
    # Флаг: обе команды с полным покрытием
    df['full_roster_data'] = (
        (df['radiant_roster_coverage'] >= 0.8) & (df['dire_roster_coverage'] >= 0.8)
    ).astype(int)
    
    return df


def compute_short_term_form(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    """
    Короткий window для текущей формы команды (последние 5 матчей).
    """
    logger.info(f"Computing short-term form (window={window})...")
    
    df['radiant_team_form_kills'] = np.nan
    df['dire_team_form_kills'] = np.nan
    
    team_history: dict[int, list[int]] = {}
    
    for idx in tqdm(range(len(df)), desc="Short-term form"):
        row = df.iloc[idx]
        radiant_tid = row['radiant_team_id']
        dire_tid = row['dire_team_id']
        
        if radiant_tid and radiant_tid in team_history:
            hist = team_history[radiant_tid][-window:]
            if len(hist) >= 2:
                df.at[idx, 'radiant_team_form_kills'] = np.mean(hist)
        
        if dire_tid and dire_tid in team_history:
            hist = team_history[dire_tid][-window:]
            if len(hist) >= 2:
                df.at[idx, 'dire_team_form_kills'] = np.mean(hist)
        
        # Update after
        if radiant_tid:
            if radiant_tid not in team_history:
                team_history[radiant_tid] = []
            team_history[radiant_tid].append(row['radiant_score'])
        
        if dire_tid:
            if dire_tid not in team_history:
                team_history[dire_tid] = []
            team_history[dire_tid].append(row['dire_score'])
    
    df['combined_form_kills'] = df['radiant_team_form_kills'].fillna(0) + df['dire_team_form_kills'].fillna(0)
    
    return df


def compute_patch_short_term_form(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    """
    Patch-aware short-term form (major patch key).
    """
    logger.info(f"Computing patch-aware short-term form (window={window})...")

    df['radiant_patch_team_form_kills'] = np.nan
    df['dire_patch_team_form_kills'] = np.nan

    team_history: dict[tuple[int, str], list[int]] = {}

    for idx in tqdm(range(len(df)), desc="Patch short-term form"):
        row = df.iloc[idx]
        patch_key = _get_patch_key(row)
        radiant_tid = row['radiant_team_id']
        dire_tid = row['dire_team_id']

        if radiant_tid:
            key = (radiant_tid, patch_key)
            if key in team_history:
                hist = team_history[key][-window:]
                if len(hist) >= 2:
                    df.at[idx, 'radiant_patch_team_form_kills'] = np.mean(hist)

        if dire_tid:
            key = (dire_tid, patch_key)
            if key in team_history:
                hist = team_history[key][-window:]
                if len(hist) >= 2:
                    df.at[idx, 'dire_patch_team_form_kills'] = np.mean(hist)

        if radiant_tid:
            key = (radiant_tid, patch_key)
            team_history.setdefault(key, []).append(row['radiant_score'])

        if dire_tid:
            key = (dire_tid, patch_key)
            team_history.setdefault(key, []).append(row['dire_score'])

    df['combined_patch_form_kills'] = (
        df['radiant_patch_team_form_kills'].fillna(0) + df['dire_patch_team_form_kills'].fillna(0)
    )

    return df


def compute_h2h_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Вычисляет статистику личных встреч (Head-to-Head).
    """
    logger.info("Computing H2H stats...")
    
    df['h2h_avg_total'] = np.nan
    df['h2h_matches_count'] = 0
    
    # h2h_key -> list of total_kills
    h2h_history: dict[tuple[int, int], list[int]] = {}
    
    for idx in tqdm(range(len(df)), desc="H2H stats"):
        row = df.iloc[idx]
        t1, t2 = row['radiant_team_id'], row['dire_team_id']
        
        if not t1 or not t2:
            continue
        
        # Нормализуем ключ (меньший ID первый)
        h2h_key = (min(t1, t2), max(t1, t2))
        
        # Считаем статистику из прошлых встреч
        if h2h_key in h2h_history and len(h2h_history[h2h_key]) >= 1:
            df.at[idx, 'h2h_avg_total'] = np.mean(h2h_history[h2h_key])
            df.at[idx, 'h2h_matches_count'] = len(h2h_history[h2h_key])
        
        # Обновляем историю ПОСЛЕ
        if h2h_key not in h2h_history:
            h2h_history[h2h_key] = []
        h2h_history[h2h_key].append(row['total_kills'])
    
    return df


def compute_team_hero_pool(df: pd.DataFrame, window: int = 50) -> pd.DataFrame:
    """
    Вычисляет гибкость hero pool для каждой команды.
    
    - unique_heroes: сколько уникальных героев использовали за последние N игр
    - hero_flexibility: unique_heroes / window (0-1, где 1 = максимальная гибкость)
    - hero_concentration: топ-10 героев покрывают какой % пиков
    """
    logger.info(f"Computing team hero pool stats (window={window})...")
    
    # team_id -> list of [hero_ids from each match]
    team_hero_history: dict[int, list[list[int]]] = {}
    
    # Инициализируем колонки
    df['radiant_hero_flexibility'] = np.nan
    df['dire_hero_flexibility'] = np.nan
    df['radiant_unique_heroes'] = np.nan
    df['dire_unique_heroes'] = np.nan
    
    for idx in tqdm(range(len(df)), desc="Team hero pool"):
        row = df.iloc[idx]
        radiant_tid = row['radiant_team_id']
        dire_tid = row['dire_team_id']
        
        # Radiant team hero pool
        if radiant_tid and radiant_tid in team_hero_history:
            # Берём последние N матчей
            recent_matches = team_hero_history[radiant_tid][-window:]
            if len(recent_matches) >= 10:
                # Собираем все hero_ids
                all_heroes = [h for match_heroes in recent_matches for h in match_heroes]
                unique_heroes = len(set(all_heroes))
                # Flexibility = unique / (matches * 5), нормализуем к 0-1
                flexibility = unique_heroes / (len(recent_matches) * 5)
                flexibility = min(flexibility, 1.0)  # cap at 1.0
                
                df.at[idx, 'radiant_unique_heroes'] = unique_heroes
                df.at[idx, 'radiant_hero_flexibility'] = flexibility
        
        # Dire team hero pool
        if dire_tid and dire_tid in team_hero_history:
            recent_matches = team_hero_history[dire_tid][-window:]
            if len(recent_matches) >= 10:
                all_heroes = [h for match_heroes in recent_matches for h in match_heroes]
                unique_heroes = len(set(all_heroes))
                flexibility = unique_heroes / (len(recent_matches) * 5)
                flexibility = min(flexibility, 1.0)
                
                df.at[idx, 'dire_unique_heroes'] = unique_heroes
                df.at[idx, 'dire_hero_flexibility'] = flexibility
        
        # Обновляем историю ПОСЛЕ расчёта (no leakage)
        radiant_heroes = [row[f'radiant_hero_{i}'] for i in range(1, 6)]
        dire_heroes = [row[f'dire_hero_{i}'] for i in range(1, 6)]
        
        if radiant_tid:
            if radiant_tid not in team_hero_history:
                team_hero_history[radiant_tid] = []
            team_hero_history[radiant_tid].append(radiant_heroes)
        
        if dire_tid:
            if dire_tid not in team_hero_history:
                team_hero_history[dire_tid] = []
            team_hero_history[dire_tid].append(dire_heroes)
    
    # Комбинированные фичи
    df['hero_flexibility_diff'] = (
        df['radiant_hero_flexibility'].fillna(0.5) - df['dire_hero_flexibility'].fillna(0.5)
    )
    df['combined_hero_flexibility'] = (
        df['radiant_hero_flexibility'].fillna(0.5) + df['dire_hero_flexibility'].fillna(0.5)
    )
    
    return df


def compute_draft_synergy_features(df: pd.DataFrame, synergy_path: str = 'data/hero_synergy.json') -> pd.DataFrame:
    """
    Вычисляет фичи синергии и контр-пиков на основе драфта.
    """
    logger.info(f"Computing draft synergy features from {synergy_path}...")
    
    # Загружаем матрицы
    try:
        with open(synergy_path, 'r') as f:
            synergy_data = json.load(f)
        synergy_matrix = synergy_data.get('synergy', {})
        counter_matrix = synergy_data.get('counter', {})
        logger.info(f"Loaded {len(synergy_matrix)} synergy pairs, {len(counter_matrix)} counter matchups")
    except FileNotFoundError:
        logger.warning(f"Synergy file not found: {synergy_path}. Skipping draft features.")
        df['radiant_draft_synergy'] = 0.0
        df['dire_draft_synergy'] = 0.0
        df['radiant_counter_advantage'] = 0.0
        df['draft_synergy_diff'] = 0.0
        df['total_draft_synergy'] = 0.0
        return df
    
    # Инициализируем колонки
    df['radiant_draft_synergy'] = 0.0
    df['dire_draft_synergy'] = 0.0
    df['radiant_counter_advantage'] = 0.0
    df['dire_counter_advantage'] = 0.0
    df['radiant_hard_counters'] = 0  # Кол-во жёстких контр-пиков (delta > 0.05)
    df['dire_hard_counters'] = 0
    df['radiant_max_counter'] = 0.0  # Максимальный контр-пик
    df['dire_max_counter'] = 0.0
    
    radiant_cols = [f'radiant_hero_{i}' for i in range(1, 6)]
    dire_cols = [f'dire_hero_{i}' for i in range(1, 6)]
    
    # Порог для "жёсткого" контр-пика (winrate delta > 5%)
    HARD_COUNTER_THRESHOLD = 0.05
    
    for idx in tqdm(range(len(df)), desc="Draft synergy features"):
        row = df.iloc[idx]
        
        radiant_heroes = [int(row[col]) for col in radiant_cols]
        dire_heroes = [int(row[col]) for col in dire_cols]
        
        # Synergy внутри Radiant (все пары)
        radiant_synergy = 0.0
        for i in range(5):
            for j in range(i + 1, 5):
                h1, h2 = sorted([radiant_heroes[i], radiant_heroes[j]])
                key = f"{h1}_{h2}"
                radiant_synergy += synergy_matrix.get(key, 0.0)
        
        # Synergy внутри Dire
        dire_synergy = 0.0
        for i in range(5):
            for j in range(i + 1, 5):
                h1, h2 = sorted([dire_heroes[i], dire_heroes[j]])
                key = f"{h1}_{h2}"
                dire_synergy += synergy_matrix.get(key, 0.0)
        
        # Counter advantage: Radiant heroes vs Dire heroes
        radiant_counter = 0.0
        radiant_hard_count = 0
        radiant_max = 0.0
        for r_hero in radiant_heroes:
            for d_hero in dire_heroes:
                key = f"{r_hero}_vs_{d_hero}"
                score = counter_matrix.get(key, 0.0)
                radiant_counter += score
                if score > HARD_COUNTER_THRESHOLD:
                    radiant_hard_count += 1
                radiant_max = max(radiant_max, score)
        
        # Counter advantage: Dire heroes vs Radiant heroes
        dire_counter = 0.0
        dire_hard_count = 0
        dire_max = 0.0
        for d_hero in dire_heroes:
            for r_hero in radiant_heroes:
                key = f"{d_hero}_vs_{r_hero}"
                score = counter_matrix.get(key, 0.0)
                dire_counter += score
                if score > HARD_COUNTER_THRESHOLD:
                    dire_hard_count += 1
                dire_max = max(dire_max, score)
        
        df.at[idx, 'radiant_draft_synergy'] = radiant_synergy
        df.at[idx, 'dire_draft_synergy'] = dire_synergy
        df.at[idx, 'radiant_counter_advantage'] = radiant_counter
        df.at[idx, 'dire_counter_advantage'] = dire_counter
        df.at[idx, 'radiant_hard_counters'] = radiant_hard_count
        df.at[idx, 'dire_hard_counters'] = dire_hard_count
        df.at[idx, 'radiant_max_counter'] = radiant_max
        df.at[idx, 'dire_max_counter'] = dire_max
    
    # Комбинированные фичи
    df['draft_synergy_diff'] = df['radiant_draft_synergy'] - df['dire_draft_synergy']
    df['total_draft_synergy'] = df['radiant_draft_synergy'] + df['dire_draft_synergy']
    df['counter_advantage_abs'] = df['radiant_counter_advantage'].abs()
    
    # Counter Intelligence фичи
    df['counter_diff'] = df['radiant_counter_advantage'] - df['dire_counter_advantage']
    df['total_counter_advantage'] = df['radiant_counter_advantage'] + df['dire_counter_advantage']
    df['hard_counter_diff'] = df['radiant_hard_counters'] - df['dire_hard_counters']
    df['total_hard_counters'] = df['radiant_hard_counters'] + df['dire_hard_counters']
    df['max_counter_diff'] = df['radiant_max_counter'] - df['dire_max_counter']
    df['combined_max_counter'] = df['radiant_max_counter'] + df['dire_max_counter']
    
    # Флаги экстремальных случаев
    df['one_sided_counters'] = (df['hard_counter_diff'].abs() >= 3).astype(int)  # Одна сторона жёстко законтрена
    df['counter_war'] = (df['total_hard_counters'] >= 6).astype(int)  # Обе стороны контрят друг друга
    
    return df


def compute_role_composition_features(
    df: pd.DataFrame,
    roles_path: str = 'data/hero_roles.json'
) -> pd.DataFrame:
    """
    Вычисляет фичи role composition на основе драфта.
    
    Features:
    - radiant_total_nukers, radiant_total_disablers, etc.
    - nuker_diff, disabler_diff, etc.
    - glass_cannon_score: nukers - durables (стеклянные пушки)
    - lockdown_score: disablers - escapes (контроль vs мобильность)
    """
    logger.info(f"Computing role composition features from {roles_path}...")
    
    try:
        with open(roles_path, 'r') as f:
            role_data = json.load(f)
        hero_roles = role_data.get('heroes', {})
        all_roles = role_data.get('roles', [])
        logger.info(f"Loaded role data for {len(hero_roles)} heroes, {len(all_roles)} roles")
    except FileNotFoundError:
        logger.warning(f"Role data file not found: {roles_path}. Skipping.")
        return df
    
    # Инициализируем колонки для каждой роли
    for team in ['radiant', 'dire']:
        for role in all_roles:
            df[f'{team}_total_{role.lower()}s'] = 0
    
    radiant_cols = [f'radiant_hero_{i}' for i in range(1, 6)]
    dire_cols = [f'dire_hero_{i}' for i in range(1, 6)]
    
    for idx in tqdm(range(len(df)), desc="Role composition features"):
        row = df.iloc[idx]
        
        # Radiant role counts
        radiant_role_counts = {role: 0 for role in all_roles}
        for col in radiant_cols:
            hero_id = str(int(row[col]))
            hero_role_data = hero_roles.get(hero_id, {})
            for role in all_roles:
                radiant_role_counts[role] += hero_role_data.get(role, 0)
        
        # Dire role counts
        dire_role_counts = {role: 0 for role in all_roles}
        for col in dire_cols:
            hero_id = str(int(row[col]))
            hero_role_data = hero_roles.get(hero_id, {})
            for role in all_roles:
                dire_role_counts[role] += hero_role_data.get(role, 0)
        
        # Записываем
        for role in all_roles:
            df.at[idx, f'radiant_total_{role.lower()}s'] = radiant_role_counts[role]
            df.at[idx, f'dire_total_{role.lower()}s'] = dire_role_counts[role]
    
    # Комбинированные фичи (diff)
    for role in all_roles:
        role_lower = role.lower()
        df[f'{role_lower}_diff'] = (
            df[f'radiant_total_{role_lower}s'] - df[f'dire_total_{role_lower}s']
        )
        df[f'combined_{role_lower}s'] = (
            df[f'radiant_total_{role_lower}s'] + df[f'dire_total_{role_lower}s']
        )
    
    # Специальные композитные фичи
    # Glass cannon score: много nukers, мало durables = стеклянные пушки
    df['radiant_glass_cannon'] = df['radiant_total_nukers'] - df['radiant_total_durables']
    df['dire_glass_cannon'] = df['dire_total_nukers'] - df['dire_total_durables']
    df['combined_glass_cannon'] = df['radiant_glass_cannon'] + df['dire_glass_cannon']
    
    # Lockdown score: много disablers, мало escapes = хороший контроль
    df['radiant_lockdown'] = df['radiant_total_disablers'] - df['radiant_total_escapes']
    df['dire_lockdown'] = df['dire_total_disablers'] - df['dire_total_escapes']
    df['combined_lockdown'] = df['radiant_lockdown'] + df['dire_lockdown']
    
    # Fight potential: nukers + disablers (способность к файтам)
    df['radiant_fight_potential'] = df['radiant_total_nukers'] + df['radiant_total_disablers']
    df['dire_fight_potential'] = df['dire_total_nukers'] + df['dire_total_disablers']
    df['combined_fight_potential'] = df['radiant_fight_potential'] + df['dire_fight_potential']
    
    return df


def compute_comeback_features(
    df: pd.DataFrame,
    comeback_path: str = 'data/hero_comeback_stats.json'
) -> pd.DataFrame:
    """
    Вычисляет фичи comeback potential на основе драфта.
    
    Features:
    - radiant_comeback_potential: средний comeback_rate героев Radiant
    - dire_comeback_potential: средний comeback_rate героев Dire
    - combined_comeback_potential: сумма (высокая = качели, долгая игра)
    - comeback_diff: разница (кто лучше играет из-за спины)
    """
    logger.info(f"Computing comeback features from {comeback_path}...")
    
    try:
        with open(comeback_path, 'r') as f:
            comeback_stats = json.load(f)
        logger.info(f"Loaded comeback stats for {len(comeback_stats)} heroes")
    except FileNotFoundError:
        logger.warning(f"Comeback stats file not found: {comeback_path}. Skipping.")
        for col in ['radiant_comeback_potential', 'dire_comeback_potential',
                    'combined_comeback_potential', 'comeback_diff',
                    'radiant_tempo_score', 'dire_tempo_score']:
            df[col] = 0.5
        return df
    
    # Default comeback rate (средний по всем героям)
    all_rates = [s['comeback_rate'] for s in comeback_stats.values()]
    default_rate = sum(all_rates) / len(all_rates) if all_rates else 0.13
    
    # Инициализируем колонки
    df['radiant_comeback_potential'] = 0.0
    df['dire_comeback_potential'] = 0.0
    
    radiant_cols = [f'radiant_hero_{i}' for i in range(1, 6)]
    dire_cols = [f'dire_hero_{i}' for i in range(1, 6)]
    
    for idx in tqdm(range(len(df)), desc="Comeback features"):
        row = df.iloc[idx]
        
        # Radiant comeback potential
        radiant_rates = []
        for col in radiant_cols:
            hero_id = str(int(row[col]))
            stats = comeback_stats.get(hero_id, {})
            radiant_rates.append(stats.get('comeback_rate', default_rate))
        
        # Dire comeback potential
        dire_rates = []
        for col in dire_cols:
            hero_id = str(int(row[col]))
            stats = comeback_stats.get(hero_id, {})
            dire_rates.append(stats.get('comeback_rate', default_rate))
        
        df.at[idx, 'radiant_comeback_potential'] = sum(radiant_rates) / 5
        df.at[idx, 'dire_comeback_potential'] = sum(dire_rates) / 5
    
    # Комбинированные фичи
    df['combined_comeback_potential'] = (
        df['radiant_comeback_potential'] + df['dire_comeback_potential']
    )
    df['comeback_diff'] = (
        df['radiant_comeback_potential'] - df['dire_comeback_potential']
    )
    # Tempo score = inverse of comeback (низкий comeback = темповый драфт)
    df['radiant_tempo_score'] = 1 - df['radiant_comeback_potential']
    df['dire_tempo_score'] = 1 - df['dire_comeback_potential']
    df['combined_tempo_score'] = df['radiant_tempo_score'] + df['dire_tempo_score']
    
    return df


def compute_greed_features(
    df: pd.DataFrame,
    greed_path: str = 'data/hero_greed_index.json'
) -> pd.DataFrame:
    """
    Вычисляет фичи Greed Index (жадность драфта).
    
    Features:
    - radiant_greed_index: средняя жадность драфта Radiant
    - dire_greed_index: средняя жадность драфта Dire
    - timing_mismatch: разница в таймингах (одна команда хочет рано, другая поздно)
    - combined_greed: общая жадность (высокая = обе команды хотят фармить)
    """
    logger.info(f"Computing greed features from {greed_path}...")
    
    try:
        with open(greed_path, 'r') as f:
            greed_data = json.load(f)
        logger.info(f"Loaded greed index for {len(greed_data)} heroes")
    except FileNotFoundError:
        logger.warning(f"Greed index file not found: {greed_path}. Skipping.")
        for col in ['radiant_greed_index', 'dire_greed_index', 'combined_greed',
                    'timing_mismatch', 'greed_diff', 'both_greedy', 'both_tempo']:
            df[col] = 0.5
        return df
    
    # Default greed (средний)
    all_greed = [h['greed_index'] for h in greed_data.values()]
    default_greed = sum(all_greed) / len(all_greed) if all_greed else 0.5
    
    # Инициализируем колонки
    df['radiant_greed_index'] = 0.0
    df['dire_greed_index'] = 0.0
    
    radiant_cols = [f'radiant_hero_{i}' for i in range(1, 6)]
    dire_cols = [f'dire_hero_{i}' for i in range(1, 6)]
    
    for idx in tqdm(range(len(df)), desc="Greed features"):
        row = df.iloc[idx]
        
        # Radiant greed
        radiant_greed = []
        for col in radiant_cols:
            hero_id = str(int(row[col]))
            hero_data = greed_data.get(hero_id, {})
            radiant_greed.append(hero_data.get('greed_index', default_greed))
        
        # Dire greed
        dire_greed = []
        for col in dire_cols:
            hero_id = str(int(row[col]))
            hero_data = greed_data.get(hero_id, {})
            dire_greed.append(hero_data.get('greed_index', default_greed))
        
        df.at[idx, 'radiant_greed_index'] = sum(radiant_greed) / 5
        df.at[idx, 'dire_greed_index'] = sum(dire_greed) / 5
    
    # Комбинированные фичи
    df['combined_greed'] = df['radiant_greed_index'] + df['dire_greed_index']
    df['greed_diff'] = df['radiant_greed_index'] - df['dire_greed_index']
    
    # Timing mismatch: абсолютная разница (одна команда хочет рано, другая поздно)
    df['timing_mismatch'] = df['greed_diff'].abs()
    
    # Both greedy: обе команды хотят фармить (combined > 1.0)
    df['both_greedy'] = (df['combined_greed'] > 1.0).astype(int)
    
    # Both tempo: обе команды хотят драться рано (combined < 0.6)
    df['both_tempo'] = (df['combined_greed'] < 0.6).astype(int)
    
    return df


def compute_lane_matchup_features(
    df: pd.DataFrame,
    lane_path: str = 'data/hero_lane_matchups.json'
) -> pd.DataFrame:
    """
    Вычисляет фичи lane matchups на основе позиций героев.
    
    Позиции в про-матчах:
    - Position 1 (Carry) vs Position 3 (Offlaner) - safe/off lane
    - Position 2 (Mid) vs Position 2 (Mid) - mid lane
    - Position 3 (Offlaner) vs Position 1 (Carry) - off/safe lane
    
    Features:
    - mid_lane_advantage: gold diff для Radiant Mid vs Dire Mid
    - safe_lane_advantage: gold diff для Radiant Carry vs Dire Offlaner
    - off_lane_advantage: gold diff для Radiant Offlaner vs Dire Carry
    - total_laning_score: сумма всех lane advantages
    - laning_variance: разброс по линиям (если одна линия сильно выигрывает)
    """
    logger.info(f"Computing lane matchup features from {lane_path}...")
    
    try:
        with open(lane_path, 'r') as f:
            lane_matrix = json.load(f)
        logger.info(f"Loaded {len(lane_matrix)} lane matchups")
    except FileNotFoundError:
        logger.warning(f"Lane matchup file not found: {lane_path}. Skipping lane features.")
        for col in ['mid_lane_advantage', 'safe_lane_advantage', 'off_lane_advantage',
                    'total_laning_score', 'laning_variance', 'mid_lane_winrate',
                    'safe_lane_winrate', 'off_lane_winrate', 'avg_lane_winrate']:
            df[col] = 0.0
        return df
    
    # Инициализируем колонки
    df['mid_lane_advantage'] = 0.0
    df['safe_lane_advantage'] = 0.0
    df['off_lane_advantage'] = 0.0
    df['mid_lane_winrate'] = 0.5
    df['safe_lane_winrate'] = 0.5
    df['off_lane_winrate'] = 0.5
    
    for idx in tqdm(range(len(df)), desc="Lane matchup features"):
        row = df.iloc[idx]
        
        # Hero IDs по позициям
        r_carry = int(row['radiant_hero_1'])   # Position 1
        r_mid = int(row['radiant_hero_2'])     # Position 2
        r_off = int(row['radiant_hero_3'])     # Position 3
        
        d_carry = int(row['dire_hero_1'])      # Position 1
        d_mid = int(row['dire_hero_2'])        # Position 2
        d_off = int(row['dire_hero_3'])        # Position 3
        
        # Mid lane: Radiant Mid vs Dire Mid
        mid_key = f"{r_mid}_{d_mid}"
        mid_data = lane_matrix.get(mid_key, {})
        mid_gold = mid_data.get('gold_diff', 0.0)
        mid_wr = mid_data.get('winrate', 0.5)
        
        # Safe lane: Radiant Carry vs Dire Offlaner
        safe_key = f"{r_carry}_{d_off}"
        safe_data = lane_matrix.get(safe_key, {})
        safe_gold = safe_data.get('gold_diff', 0.0)
        safe_wr = safe_data.get('winrate', 0.5)
        
        # Off lane: Radiant Offlaner vs Dire Carry
        off_key = f"{r_off}_{d_carry}"
        off_data = lane_matrix.get(off_key, {})
        off_gold = off_data.get('gold_diff', 0.0)
        off_wr = off_data.get('winrate', 0.5)
        
        df.at[idx, 'mid_lane_advantage'] = mid_gold
        df.at[idx, 'safe_lane_advantage'] = safe_gold
        df.at[idx, 'off_lane_advantage'] = off_gold
        df.at[idx, 'mid_lane_winrate'] = mid_wr
        df.at[idx, 'safe_lane_winrate'] = safe_wr
        df.at[idx, 'off_lane_winrate'] = off_wr
    
    # Комбинированные фичи
    df['total_laning_score'] = (
        df['mid_lane_advantage'] + df['safe_lane_advantage'] + df['off_lane_advantage']
    )
    df['laning_variance'] = df[['mid_lane_advantage', 'safe_lane_advantage', 'off_lane_advantage']].std(axis=1)
    df['avg_lane_winrate'] = (
        df['mid_lane_winrate'] + df['safe_lane_winrate'] + df['off_lane_winrate']
    ) / 3
    
    # Абсолютные значения для модели
    df['total_laning_abs'] = df['total_laning_score'].abs()
    df['mid_dominance'] = (df['mid_lane_winrate'] - 0.5).abs()  # Насколько mid неравный
    
    return df


def compute_fatigue_and_series_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Вычисляет фичи усталости и позиции в серии.
    
    Features:
    - radiant_fatigue: кол-во игр команды за последние 12 часов
    - dire_fatigue: кол-во игр команды за последние 12 часов
    - series_game_num: номер игры в серии (1, 2, 3...)
    - is_decider_game: 1 если это решающая карта (счёт 1-1 в BO3, 2-2 в BO5)
    - combined_fatigue: сумма усталости обеих команд
    """
    logger.info("Computing fatigue and series features...")
    
    # 12 часов в секундах
    FATIGUE_WINDOW = 12 * 60 * 60
    
    # Инициализируем колонки
    df['radiant_fatigue'] = 0
    df['dire_fatigue'] = 0
    df['series_game_num'] = 1
    df['is_decider_game'] = 0
    
    # История матчей команд: team_id -> list of (start_time, series_id, won)
    team_match_history: dict[int, list[tuple[int, int, bool]]] = {}
    
    # История серий: series_id -> list of (radiant_team_id, dire_team_id, radiant_won)
    series_history: dict[int, list[tuple[int, int, bool]]] = {}
    
    for idx in tqdm(range(len(df)), desc="Fatigue & series features"):
        row = df.iloc[idx]
        
        radiant_tid = row['radiant_team_id']
        dire_tid = row['dire_team_id']
        start_time = row['start_time']
        series_id = row.get('series_id', 0)
        series_type = row.get('series_type', 'BEST_OF_ONE')
        radiant_won = row.get('radiant_win', False)
        
        # === FATIGUE: считаем игры за последние 12 часов ===
        if radiant_tid and radiant_tid in team_match_history:
            recent_games = [
                t for t, _, _ in team_match_history[radiant_tid]
                if start_time - t < FATIGUE_WINDOW and start_time > t
            ]
            df.at[idx, 'radiant_fatigue'] = len(recent_games)
        
        if dire_tid and dire_tid in team_match_history:
            recent_games = [
                t for t, _, _ in team_match_history[dire_tid]
                if start_time - t < FATIGUE_WINDOW and start_time > t
            ]
            df.at[idx, 'dire_fatigue'] = len(recent_games)
        
        # === SERIES: определяем номер игры и decider ===
        if series_id and series_id in series_history:
            prev_games = series_history[series_id]
            game_num = len(prev_games) + 1
            df.at[idx, 'series_game_num'] = game_num
            
            # Считаем счёт серии
            # Нужно определить кто radiant/dire в текущей игре vs предыдущих
            radiant_series_wins = 0
            dire_series_wins = 0
            
            for prev_r_tid, prev_d_tid, prev_r_won in prev_games:
                # Определяем победителя предыдущей игры
                if prev_r_won:
                    winner_tid = prev_r_tid
                else:
                    winner_tid = prev_d_tid
                
                # Сопоставляем с текущими командами
                if winner_tid == radiant_tid:
                    radiant_series_wins += 1
                elif winner_tid == dire_tid:
                    dire_series_wins += 1
            
            # Decider game detection
            if series_type == 'BEST_OF_THREE' and radiant_series_wins == 1 and dire_series_wins == 1:
                df.at[idx, 'is_decider_game'] = 1
            elif series_type == 'BEST_OF_FIVE' and radiant_series_wins == 2 and dire_series_wins == 2:
                df.at[idx, 'is_decider_game'] = 1
        
        # === Обновляем историю ПОСЛЕ расчёта (no leakage) ===
        if radiant_tid:
            if radiant_tid not in team_match_history:
                team_match_history[radiant_tid] = []
            team_match_history[radiant_tid].append((start_time, series_id, radiant_won))
        
        if dire_tid:
            if dire_tid not in team_match_history:
                team_match_history[dire_tid] = []
            team_match_history[dire_tid].append((start_time, series_id, not radiant_won))
        
        if series_id:
            if series_id not in series_history:
                series_history[series_id] = []
            series_history[series_id].append((radiant_tid, dire_tid, radiant_won))
    
    # Комбинированные фичи
    df['combined_fatigue'] = df['radiant_fatigue'] + df['dire_fatigue']
    df['fatigue_diff'] = df['radiant_fatigue'] - df['dire_fatigue']
    df['high_fatigue'] = (df['combined_fatigue'] >= 4).astype(int)  # 4+ игры = высокая усталость
    df['late_series_game'] = (df['series_game_num'] >= 3).astype(int)  # 3+ игра в серии
    
    return df


def compute_team_aggro_style(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Вычисляет стиль игры команды: aggro vs objective focused.
    
    Features:
    - radiant_avg_hero_damage: средний hero damage команды за последние N игр
    - radiant_avg_tower_damage: средний tower damage команды
    - radiant_aggro_ratio: hero_damage / tower_damage (высокий = ищут драки)
    - combined_aggro_ratio: сумма aggro_ratio обеих команд
    """
    logger.info(f"Computing team aggro style (window={window})...")
    
    # Проверяем наличие колонок damage
    if 'radiant_total_hero_damage' not in df.columns:
        logger.warning("Hero/tower damage columns not found. Skipping aggro style.")
        for col in ['radiant_avg_hero_damage', 'radiant_avg_tower_damage', 'radiant_aggro_ratio',
                    'dire_avg_hero_damage', 'dire_avg_tower_damage', 'dire_aggro_ratio',
                    'combined_aggro_ratio', 'aggro_ratio_diff', 'both_aggro', 'both_objective']:
            df[col] = np.nan
        return df
    
    # Инициализируем колонки
    df['radiant_avg_hero_damage'] = np.nan
    df['radiant_avg_tower_damage'] = np.nan
    df['radiant_aggro_ratio'] = np.nan
    df['dire_avg_hero_damage'] = np.nan
    df['dire_avg_tower_damage'] = np.nan
    df['dire_aggro_ratio'] = np.nan
    
    # team_id -> list of (hero_damage, tower_damage, duration)
    team_damage_history: dict[int, list[tuple[int, int, int]]] = {}
    
    for idx in tqdm(range(len(df)), desc="Team aggro style"):
        row = df.iloc[idx]
        radiant_tid = row['radiant_team_id']
        dire_tid = row['dire_team_id']
        duration = row.get('duration_min', 30)
        
        # Radiant team aggro style
        if radiant_tid and radiant_tid in team_damage_history:
            hist = team_damage_history[radiant_tid][-window:]
            if len(hist) >= 3:
                avg_hero_dmg = np.mean([h / max(d, 1) for h, t, d in hist])  # per minute
                avg_tower_dmg = np.mean([t / max(d, 1) for h, t, d in hist])
                aggro_ratio = avg_hero_dmg / max(avg_tower_dmg, 1)
                
                df.at[idx, 'radiant_avg_hero_damage'] = avg_hero_dmg
                df.at[idx, 'radiant_avg_tower_damage'] = avg_tower_dmg
                df.at[idx, 'radiant_aggro_ratio'] = aggro_ratio
        
        # Dire team aggro style
        if dire_tid and dire_tid in team_damage_history:
            hist = team_damage_history[dire_tid][-window:]
            if len(hist) >= 3:
                avg_hero_dmg = np.mean([h / max(d, 1) for h, t, d in hist])
                avg_tower_dmg = np.mean([t / max(d, 1) for h, t, d in hist])
                aggro_ratio = avg_hero_dmg / max(avg_tower_dmg, 1)
                
                df.at[idx, 'dire_avg_hero_damage'] = avg_hero_dmg
                df.at[idx, 'dire_avg_tower_damage'] = avg_tower_dmg
                df.at[idx, 'dire_aggro_ratio'] = aggro_ratio
        
        # Обновляем историю ПОСЛЕ расчёта (no leakage)
        if radiant_tid:
            if radiant_tid not in team_damage_history:
                team_damage_history[radiant_tid] = []
            team_damage_history[radiant_tid].append((
                row.get('radiant_total_hero_damage', 0),
                row.get('radiant_total_tower_damage', 0),
                duration
            ))
        
        if dire_tid:
            if dire_tid not in team_damage_history:
                team_damage_history[dire_tid] = []
            team_damage_history[dire_tid].append((
                row.get('dire_total_hero_damage', 0),
                row.get('dire_total_tower_damage', 0),
                duration
            ))
    
    # Комбинированные фичи
    df['combined_aggro_ratio'] = (
        df['radiant_aggro_ratio'].fillna(50) + df['dire_aggro_ratio'].fillna(50)
    )
    df['aggro_ratio_diff'] = (
        df['radiant_aggro_ratio'].fillna(50) - df['dire_aggro_ratio'].fillna(50)
    )
    
    # Флаги экстремальных случаев
    median_aggro = df['radiant_aggro_ratio'].median()
    df['both_aggro'] = (
        (df['radiant_aggro_ratio'].fillna(median_aggro) > median_aggro * 1.2) &
        (df['dire_aggro_ratio'].fillna(median_aggro) > median_aggro * 1.2)
    ).astype(int)
    df['both_objective'] = (
        (df['radiant_aggro_ratio'].fillna(median_aggro) < median_aggro * 0.8) &
        (df['dire_aggro_ratio'].fillna(median_aggro) < median_aggro * 0.8)
    ).astype(int)
    
    return df


def compute_team_utility_style(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Вычисляет стиль игры команды через healing и stealth (прокси для vision control).
    
    Features:
    - radiant_avg_healing: средний hero healing команды (sustain style)
    - radiant_avg_invis_time: среднее время в невидимости (smoke/invis usage)
    - radiant_sustain_ratio: healing / deaths (выживаемость)
    """
    logger.info(f"Computing team utility style (window={window})...")
    
    # Сначала нужно добавить healing и invis в extract, проверим наличие
    # Если нет - используем существующие данные
    
    # Инициализируем колонки
    df['radiant_avg_healing'] = np.nan
    df['dire_avg_healing'] = np.nan
    df['radiant_sustain_ratio'] = np.nan
    df['dire_sustain_ratio'] = np.nan
    
    # team_id -> list of (total_healing, total_deaths, duration)
    team_utility_history: dict[int, list[tuple[float, int, int]]] = {}
    
    for idx in tqdm(range(len(df)), desc="Team utility style"):
        row = df.iloc[idx]
        radiant_tid = row['radiant_team_id']
        dire_tid = row['dire_team_id']
        duration = row.get('duration_min', 30)
        
        # Используем deaths как прокси (уже есть в данных)
        radiant_deaths = row.get('dire_score', 0)  # radiant deaths = dire kills
        dire_deaths = row.get('radiant_score', 0)
        
        # Radiant team utility style
        if radiant_tid and radiant_tid in team_utility_history:
            hist = team_utility_history[radiant_tid][-window:]
            if len(hist) >= 3:
                avg_healing = np.mean([h / max(d, 1) for h, deaths, d in hist])
                avg_deaths = np.mean([deaths / max(d, 1) for h, deaths, d in hist])
                sustain = avg_healing / max(avg_deaths, 0.1)
                
                df.at[idx, 'radiant_avg_healing'] = avg_healing
                df.at[idx, 'radiant_sustain_ratio'] = sustain
        
        # Dire team utility style
        if dire_tid and dire_tid in team_utility_history:
            hist = team_utility_history[dire_tid][-window:]
            if len(hist) >= 3:
                avg_healing = np.mean([h / max(d, 1) for h, deaths, d in hist])
                avg_deaths = np.mean([deaths / max(d, 1) for h, deaths, d in hist])
                sustain = avg_healing / max(avg_deaths, 0.1)
                
                df.at[idx, 'dire_avg_healing'] = avg_healing
                df.at[idx, 'dire_sustain_ratio'] = sustain
        
        # Обновляем историю ПОСЛЕ расчёта (no leakage)
        radiant_healing = row.get('radiant_total_healing', 0)
        dire_healing = row.get('dire_total_healing', 0)
        
        if radiant_tid:
            if radiant_tid not in team_utility_history:
                team_utility_history[radiant_tid] = []
            team_utility_history[radiant_tid].append((radiant_healing, radiant_deaths, duration))
        
        if dire_tid:
            if dire_tid not in team_utility_history:
                team_utility_history[dire_tid] = []
            team_utility_history[dire_tid].append((dire_healing, dire_deaths, duration))
    
    # Комбинированные фичи
    df['combined_sustain'] = (
        df['radiant_sustain_ratio'].fillna(0) + df['dire_sustain_ratio'].fillna(0)
    )
    df['sustain_diff'] = (
        df['radiant_sustain_ratio'].fillna(0) - df['dire_sustain_ratio'].fillna(0)
    )
    
    return df


def compute_side_specific_stats(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Вычисляет side-specific performance (Radiant vs Dire winrates).
    
    Features:
    - radiant_team_radiant_winrate: винрейт команды Radiant когда она за Radiant
    - dire_team_dire_winrate: винрейт команды Dire когда она за Dire
    - side_winrate_delta: разница (большая = односторонний матч)
    - radiant_side_preference: насколько команда лучше играет за Radiant vs Dire
    """
    logger.info(f"Computing side-specific stats (window={window})...")
    
    # Инициализируем колонки
    df['radiant_team_radiant_winrate'] = np.nan
    df['radiant_team_dire_winrate'] = np.nan
    df['dire_team_radiant_winrate'] = np.nan
    df['dire_team_dire_winrate'] = np.nan
    
    # team_id -> {'radiant_wins': 0, 'radiant_games': 0, 'dire_wins': 0, 'dire_games': 0}
    team_side_history: dict[int, dict[str, list[int]]] = {}
    
    for idx in tqdm(range(len(df)), desc="Side-specific stats"):
        row = df.iloc[idx]
        radiant_tid = row['radiant_team_id']
        dire_tid = row['dire_team_id']
        radiant_won = row.get('radiant_win', False)
        
        # Radiant team side stats
        if radiant_tid and radiant_tid in team_side_history:
            hist = team_side_history[radiant_tid]
            
            # Radiant winrate когда за Radiant
            radiant_games = hist['radiant_results'][-window:]
            if len(radiant_games) >= 3:
                df.at[idx, 'radiant_team_radiant_winrate'] = sum(radiant_games) / len(radiant_games)
            
            # Radiant winrate когда за Dire (для side preference)
            dire_games = hist['dire_results'][-window:]
            if len(dire_games) >= 3:
                df.at[idx, 'radiant_team_dire_winrate'] = sum(dire_games) / len(dire_games)
        
        # Dire team side stats
        if dire_tid and dire_tid in team_side_history:
            hist = team_side_history[dire_tid]
            
            # Dire winrate когда за Dire
            dire_games = hist['dire_results'][-window:]
            if len(dire_games) >= 3:
                df.at[idx, 'dire_team_dire_winrate'] = sum(dire_games) / len(dire_games)
            
            # Dire winrate когда за Radiant (для side preference)
            radiant_games = hist['radiant_results'][-window:]
            if len(radiant_games) >= 3:
                df.at[idx, 'dire_team_radiant_winrate'] = sum(radiant_games) / len(radiant_games)
        
        # Обновляем историю ПОСЛЕ расчёта (no leakage)
        if radiant_tid:
            if radiant_tid not in team_side_history:
                team_side_history[radiant_tid] = {'radiant_results': [], 'dire_results': []}
            team_side_history[radiant_tid]['radiant_results'].append(1 if radiant_won else 0)
        
        if dire_tid:
            if dire_tid not in team_side_history:
                team_side_history[dire_tid] = {'radiant_results': [], 'dire_results': []}
            team_side_history[dire_tid]['dire_results'].append(1 if not radiant_won else 0)
    
    # Комбинированные фичи
    # Side winrate delta: разница между radiant team's radiant WR и dire team's dire WR
    df['side_winrate_delta'] = (
        df['radiant_team_radiant_winrate'].fillna(0.5) - df['dire_team_dire_winrate'].fillna(0.5)
    )
    
    # Side preference: насколько команда лучше играет за свою сторону
    df['radiant_side_preference'] = (
        df['radiant_team_radiant_winrate'].fillna(0.5) - df['radiant_team_dire_winrate'].fillna(0.5)
    )
    df['dire_side_preference'] = (
        df['dire_team_dire_winrate'].fillna(0.5) - df['dire_team_radiant_winrate'].fillna(0.5)
    )
    
    # Combined side advantage
    df['combined_side_advantage'] = (
        df['radiant_team_radiant_winrate'].fillna(0.5) + df['dire_team_dire_winrate'].fillna(0.5)
    )
    
    # Mismatch indicator: большая разница = односторонний матч
    df['side_mismatch'] = df['side_winrate_delta'].abs()
    
    return df


def compute_hero_mastery_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Вычисляет Composite Hero Mastery Score.
    
    Логика:
    1. Dota Plus XP: нормализуем в 0-100 (74000 XP = 100)
    2. Historical Games: кол-во игр на герое в про-матчах (50+ = 100)
    3. Composite: Dota Plus если есть, иначе Historical, иначе team avg
    
    Features:
    - radiant_avg_mastery: средний mastery score команды
    - dire_avg_mastery: средний mastery score команды
    - mastery_gap: абсолютная разница (большая = одна команда на комфортных героях)
    - combined_mastery: сумма (высокая = обе команды на сигнатурах)
    - mastery_variance: разброс внутри команды (низкий = все на комфорте)
    """
    logger.info("Computing hero mastery features...")
    
    # Константы нормализации
    DOTA_PLUS_MAX_XP = 74000  # Grandmaster level
    HISTORICAL_MAX_GAMES = 50  # 50+ про-игр на герое = мастер
    
    # История игр: (player_id, hero_id) -> count
    player_hero_history: dict[tuple[int, int], int] = {}
    
    # Инициализируем колонки для individual mastery scores
    for team in ['radiant', 'dire']:
        for i in range(1, 6):
            df[f'{team}_player_{i}_mastery'] = np.nan
    
    df['radiant_avg_mastery'] = np.nan
    df['dire_avg_mastery'] = np.nan
    
    for idx in tqdm(range(len(df)), desc="Hero mastery features"):
        row = df.iloc[idx]
        
        for team in ['radiant', 'dire']:
            team_mastery_scores: list[float] = []
            
            for pos in range(1, 6):
                player_id = row.get(f'{team}_player_{pos}_id', 0)
                hero_id = row.get(f'{team}_hero_{pos}', 0)
                dota_plus_xp = row.get(f'{team}_player_{pos}_dota_plus_xp', 0) or 0
                
                mastery_score: float = np.nan
                
                # Source 1: Dota Plus XP (если > 0)
                if dota_plus_xp > 0:
                    dota_plus_score = min(dota_plus_xp / DOTA_PLUS_MAX_XP * 100, 100)
                    mastery_score = dota_plus_score
                
                # Source 2: Historical games (fallback)
                elif player_id and hero_id:
                    key = (player_id, hero_id)
                    if key in player_hero_history:
                        games_played = player_hero_history[key]
                        historical_score = min(games_played / HISTORICAL_MAX_GAMES * 100, 100)
                        mastery_score = historical_score
                
                # Записываем individual score
                if not np.isnan(mastery_score):
                    df.at[idx, f'{team}_player_{pos}_mastery'] = mastery_score
                    team_mastery_scores.append(mastery_score)
            
            # Team average (если есть хотя бы 3 игрока с данными)
            if len(team_mastery_scores) >= 3:
                team_avg = np.mean(team_mastery_scores)
                df.at[idx, f'{team}_avg_mastery'] = team_avg
                
                # Imputation: заполняем пропуски средним по команде
                for pos in range(1, 6):
                    if np.isnan(df.at[idx, f'{team}_player_{pos}_mastery']):
                        df.at[idx, f'{team}_player_{pos}_mastery'] = team_avg
        
        # === Обновляем историю ПОСЛЕ расчёта (no leakage) ===
        for team in ['radiant', 'dire']:
            for pos in range(1, 6):
                player_id = row.get(f'{team}_player_{pos}_id', 0)
                hero_id = row.get(f'{team}_hero_{pos}', 0)
                
                if player_id and hero_id:
                    key = (player_id, hero_id)
                    player_hero_history[key] = player_hero_history.get(key, 0) + 1
    
    # Комбинированные фичи
    df['mastery_gap'] = (
        df['radiant_avg_mastery'].fillna(50) - df['dire_avg_mastery'].fillna(50)
    ).abs()
    
    df['combined_mastery'] = (
        df['radiant_avg_mastery'].fillna(50) + df['dire_avg_mastery'].fillna(50)
    )
    
    df['mastery_diff'] = (
        df['radiant_avg_mastery'].fillna(50) - df['dire_avg_mastery'].fillna(50)
    )
    
    # Variance внутри команды (низкий = все на комфорте)
    radiant_mastery_cols = [f'radiant_player_{i}_mastery' for i in range(1, 6)]
    dire_mastery_cols = [f'dire_player_{i}_mastery' for i in range(1, 6)]
    
    df['radiant_mastery_variance'] = df[radiant_mastery_cols].std(axis=1).fillna(0)
    df['dire_mastery_variance'] = df[dire_mastery_cols].std(axis=1).fillna(0)
    df['combined_mastery_variance'] = df['radiant_mastery_variance'] + df['dire_mastery_variance']
    
    # Флаги экстремальных случаев
    df['both_high_mastery'] = (
        (df['radiant_avg_mastery'].fillna(50) > 60) &
        (df['dire_avg_mastery'].fillna(50) > 60)
    ).astype(int)
    
    df['mastery_mismatch'] = (df['mastery_gap'] > 20).astype(int)
    
    return df


def compute_league_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Вычисляет статистику по турниру (tournament meta adaptation).
    
    Features:
    - league_avg_kills: средний тотал на турнире
    - league_kills_std: разброс тоталов (волатильность турнира)
    - league_meta_diff: разница между турнирной и глобальной метой
    - radiant_draft_tournament_popularity: насколько популярен драфт на этом турнире
    """
    logger.info("Computing tournament meta stats...")
    
    # Инициализируем колонки
    df['league_avg_kills'] = np.nan
    df['league_kills_std'] = np.nan
    df['league_meta_diff'] = np.nan
    df['league_games_played'] = 0
    df['radiant_draft_tournament_popularity'] = np.nan
    df['dire_draft_tournament_popularity'] = np.nan
    
    # league_id -> list of total_kills
    league_history: dict[int, list[int]] = {}
    # league_id -> dict of hero_id -> pick_count
    league_hero_picks: dict[int, dict[int, int]] = {}
    global_kills: list[int] = []
    
    # Колонки героев
    radiant_hero_cols = [f'radiant_hero_{i}' for i in range(1, 6)]
    dire_hero_cols = [f'dire_hero_{i}' for i in range(1, 6)]
    
    for idx in tqdm(range(len(df)), desc="Tournament meta stats"):
        row = df.iloc[idx]
        league_id = row['league_id']
        
        # === Tournament kills stats ===
        if league_id and league_id in league_history and len(league_history[league_id]) >= 5:
            league_kills = league_history[league_id]
            league_avg = np.mean(league_kills)
            league_std = np.std(league_kills) if len(league_kills) > 1 else 0
            
            df.at[idx, 'league_avg_kills'] = league_avg
            df.at[idx, 'league_kills_std'] = league_std
            df.at[idx, 'league_games_played'] = len(league_kills)
            
            # Meta diff: насколько турнир отличается от глобального среднего
            if len(global_kills) >= 50:
                global_avg = np.mean(global_kills[-500:])
                df.at[idx, 'league_meta_diff'] = league_avg - global_avg
        elif len(global_kills) >= 10:
            df.at[idx, 'league_avg_kills'] = np.mean(global_kills[-500:])
            df.at[idx, 'league_kills_std'] = np.std(global_kills[-500:]) if len(global_kills) > 1 else 0
        
        # === Hero tournament popularity ===
        if league_id and league_id in league_hero_picks:
            hero_picks = league_hero_picks[league_id]
            total_picks = sum(hero_picks.values())
            
            if total_picks >= 50:  # Минимум 50 пиков на турнире
                # Radiant draft popularity
                radiant_popularity = 0
                for col in radiant_hero_cols:
                    hero_id = int(row[col])
                    radiant_popularity += hero_picks.get(hero_id, 0) / total_picks
                df.at[idx, 'radiant_draft_tournament_popularity'] = radiant_popularity / 5
                
                # Dire draft popularity
                dire_popularity = 0
                for col in dire_hero_cols:
                    hero_id = int(row[col])
                    dire_popularity += hero_picks.get(hero_id, 0) / total_picks
                df.at[idx, 'dire_draft_tournament_popularity'] = dire_popularity / 5
        
        # === Обновляем историю ПОСЛЕ расчёта (no leakage) ===
        if league_id:
            # Kills history
            if league_id not in league_history:
                league_history[league_id] = []
            league_history[league_id].append(row['total_kills'])
            
            # Hero picks history
            if league_id not in league_hero_picks:
                league_hero_picks[league_id] = {}
            
            for col in radiant_hero_cols + dire_hero_cols:
                hero_id = int(row[col])
                if hero_id:
                    league_hero_picks[league_id][hero_id] = league_hero_picks[league_id].get(hero_id, 0) + 1
        
        global_kills.append(row['total_kills'])
    
    # Комбинированные фичи
    df['combined_draft_tournament_popularity'] = (
        df['radiant_draft_tournament_popularity'].fillna(0) + 
        df['dire_draft_tournament_popularity'].fillna(0)
    )
    df['draft_popularity_diff'] = (
        df['radiant_draft_tournament_popularity'].fillna(0) - 
        df['dire_draft_tournament_popularity'].fillna(0)
    )
    
    return df


def compute_per_minute_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Нормализует абсолютные значения на среднюю длительность (per minute).
    
    Важно: Делим НЕ на длительность текущего матча (это таргет, лик!),
    а на avg_pace (среднюю длительность матчей героя/игрока).
    
    Features:
    - *_per_min: нормализованные версии damage, healing, kills, deaths
    """
    logger.info("Computing per-minute normalized features...")
    
    # Используем radiant_avg_pace и dire_avg_pace как нормализатор
    # Это средняя длительность матчей героев в драфте (из hero_public_stats)
    
    # Fallback: если pace не доступен, используем глобальное среднее
    global_avg_pace = 35.0  # ~35 минут средняя игра
    
    # Нормализуем hero damage
    if 'radiant_avg_hero_damage' in df.columns and 'radiant_avg_pace' in df.columns:
        pace_r = df['radiant_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        pace_d = df['dire_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        
        df['radiant_hero_damage_per_min'] = df['radiant_avg_hero_damage'].fillna(0) / pace_r
        df['dire_hero_damage_per_min'] = df['dire_avg_hero_damage'].fillna(0) / pace_d
        df['combined_hero_damage_per_min'] = df['radiant_hero_damage_per_min'] + df['dire_hero_damage_per_min']
        df['hero_damage_per_min_diff'] = df['radiant_hero_damage_per_min'] - df['dire_hero_damage_per_min']
    
    # Нормализуем tower damage
    if 'radiant_avg_tower_damage' in df.columns and 'radiant_avg_pace' in df.columns:
        pace_r = df['radiant_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        pace_d = df['dire_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        
        df['radiant_tower_damage_per_min'] = df['radiant_avg_tower_damage'].fillna(0) / pace_r
        df['dire_tower_damage_per_min'] = df['dire_avg_tower_damage'].fillna(0) / pace_d
        df['combined_tower_damage_per_min'] = df['radiant_tower_damage_per_min'] + df['dire_tower_damage_per_min']
        df['tower_damage_per_min_diff'] = df['radiant_tower_damage_per_min'] - df['dire_tower_damage_per_min']
    
    # Нормализуем healing
    if 'radiant_avg_healing' in df.columns and 'radiant_avg_pace' in df.columns:
        pace_r = df['radiant_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        pace_d = df['dire_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        
        df['radiant_healing_per_min'] = df['radiant_avg_healing'].fillna(0) / pace_r
        df['dire_healing_per_min'] = df['dire_avg_healing'].fillna(0) / pace_d
        df['combined_healing_per_min'] = df['radiant_healing_per_min'] + df['dire_healing_per_min']
        df['healing_per_min_diff'] = df['radiant_healing_per_min'] - df['dire_healing_per_min']
    
    # Нормализуем heal_score (из hero stats)
    if 'radiant_heal_score' in df.columns and 'radiant_avg_pace' in df.columns:
        pace_r = df['radiant_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        pace_d = df['dire_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        
        df['radiant_heal_score_per_min'] = df['radiant_heal_score'].fillna(0) / pace_r
        df['dire_heal_score_per_min'] = df['dire_heal_score'].fillna(0) / pace_d
        df['combined_heal_score_per_min'] = df['radiant_heal_score_per_min'] + df['dire_heal_score_per_min']
    
    # Нормализуем player kills/deaths
    if 'radiant_players_avg_kills' in df.columns and 'radiant_avg_pace' in df.columns:
        pace_r = df['radiant_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        pace_d = df['dire_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        
        df['radiant_kills_per_min'] = df['radiant_players_avg_kills'].fillna(0) / pace_r
        df['dire_kills_per_min'] = df['dire_players_avg_kills'].fillna(0) / pace_d
        df['combined_kills_per_min'] = df['radiant_kills_per_min'] + df['dire_kills_per_min']
        df['kills_per_min_diff'] = df['radiant_kills_per_min'] - df['dire_kills_per_min']
        
        df['radiant_deaths_per_min'] = df['radiant_players_avg_deaths'].fillna(0) / pace_r
        df['dire_deaths_per_min'] = df['dire_players_avg_deaths'].fillna(0) / pace_d
        df['combined_deaths_per_min'] = df['radiant_deaths_per_min'] + df['dire_deaths_per_min']
    
    # Нормализуем aggression
    if 'radiant_total_aggression' in df.columns and 'radiant_avg_pace' in df.columns:
        pace_r = df['radiant_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        pace_d = df['dire_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        
        df['radiant_aggression_per_min'] = df['radiant_total_aggression'].fillna(0) / pace_r
        df['dire_aggression_per_min'] = df['dire_total_aggression'].fillna(0) / pace_d
        df['combined_aggression_per_min'] = df['radiant_aggression_per_min'] + df['dire_aggression_per_min']
    
    # Нормализуем burst score
    if 'radiant_burst_score' in df.columns and 'radiant_avg_pace' in df.columns:
        pace_r = df['radiant_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        pace_d = df['dire_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        
        df['radiant_burst_per_min'] = df['radiant_burst_score'].fillna(0) / pace_r
        df['dire_burst_per_min'] = df['dire_burst_score'].fillna(0) / pace_d
        df['combined_burst_per_min'] = df['radiant_burst_per_min'] + df['dire_burst_per_min']
    
    # Нормализуем save score
    if 'radiant_save_score' in df.columns and 'radiant_avg_pace' in df.columns:
        pace_r = df['radiant_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        pace_d = df['dire_avg_pace'].fillna(global_avg_pace).clip(lower=20)
        
        df['radiant_save_per_min'] = df['radiant_save_score'].fillna(0) / pace_r
        df['dire_save_per_min'] = df['dire_save_score'].fillna(0) / pace_d
        df['combined_save_per_min'] = df['radiant_save_per_min'] + df['dire_save_per_min']
    
    logger.info(f"Added per-minute normalized features")
    
    return df


def main(
    pro_json_path: str,
    hero_stats_path: str,
    output_path: str
) -> None:
    """Основной пайплайн."""
    # Используем новый загрузчик с данными игроков
    df = load_pro_matches_with_players(pro_json_path)
    df = enrich_with_hero_stats(df, hero_stats_path)
    df = enrich_with_power_spikes(df)
    df = enrich_with_save_sustain(df)
    df = enrich_with_push_defense(df)
    df = enrich_with_tactical_feed(df)
    df = enrich_with_cc_initiation(df)
    df = enrich_with_economy_greed(df)
    df = enrich_with_bkb_pierce(df)
    df = compute_rolling_player_dna(df)  # Rolling DNA - no leakage
    df = enrich_with_complex_stats(df)
    df = enrich_with_wave_clear(df)
    df = enrich_with_hg_defense(df)
    df = enrich_with_burst_damage(df)
    df = enrich_with_save_disengage(df)
    df = enrich_with_big_ults(df)
    df = enrich_with_vision_control(df)
    df = enrich_with_smoke_gank(df)
    df = enrich_with_highground(df)
    df = enrich_with_aura_stacking(df)
    df = enrich_with_dispel(df)
    df = enrich_with_shard_timing(df)
    df = enrich_with_mana_dependency(df)
    df = enrich_with_tempo_control(df)
    df = enrich_with_objective_focus(df)
    df = enrich_with_blood_stats(df)
    df = enrich_with_early_late_counters(df)
    df = enrich_with_stratz_draft_features(df)  # Stratz matchup/synergy/roles
    df = enrich_with_advanced_draft_features(df)  # catch/escape, scaling, global, big ult
    
    # Player-level stats (NEW!)
    df = compute_player_rolling_stats(df, window=20)
    
    # Roster confidence (protection against unknowns)
    df = compute_roster_confidence(df, min_games=10)
    df = compute_roster_stability(df, min_shared=3)
    df = compute_roster_stability_recent(df, min_shared=3)
    
    # Team-level stats (legacy, team_id based)
    df = compute_rolling_team_stats(df, window=20)
    df = compute_patch_rolling_team_stats(df, window=20)
    
    # Synthetic team stats (roster-aware, player-based) - решает проблему решафлов
    df = compute_synthetic_team_stats(df, window=20)
    
    df = compute_short_term_form(df, window=5)
    df = compute_patch_short_term_form(df, window=5)
    df = compute_h2h_stats(df)
    df = compute_league_stats(df)
    
    # Team hero pool flexibility
    df = compute_team_hero_pool(df, window=50)
    
    # Draft synergy/counter features
    df = compute_draft_synergy_features(df)
    
    # Role composition features (NEW!)
    df = compute_role_composition_features(df)
    
    # Comeback potential features
    df = compute_comeback_features(df)
    
    # Greed index features (NEW!)
    df = compute_greed_features(df)
    
    # Lane matchup features
    df = compute_lane_matchup_features(df)
    
    # Fatigue and series features
    df = compute_fatigue_and_series_features(df)
    
    # Team aggro style
    df = compute_team_aggro_style(df, window=20)
    
    # Team stealth/healing style
    df = compute_team_utility_style(df, window=20)
    
    # Side-specific performance (NEW!)
    df = compute_side_specific_stats(df, window=20)
    
    # Hero mastery features (Composite: Dota Plus + Historical)
    df = compute_hero_mastery_features(df)
    
    # Per-minute normalized features (NEW!)
    df = compute_per_minute_features(df)
    
    # Calendar features (weekend/day of week)
    df['datetime'] = pd.to_datetime(df['start_time'], unit='s')
    df['day_of_week'] = df['datetime'].dt.dayofweek  # 0=Monday, 6=Sunday
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)  # Sat/Sun
    df['hour_of_day'] = df['datetime'].dt.hour
    df['is_prime_time'] = ((df['hour_of_day'] >= 18) | (df['hour_of_day'] <= 2)).astype(int)  # Evening/night
    df = df.drop(columns=['datetime'])  # Убираем временную колонку
    
    # Team tier features (based on curated tier lists)
    logger.info("Computing team tier features...")
    df['radiant_tier'] = df['radiant_team_id'].apply(get_team_tier)
    df['dire_tier'] = df['dire_team_id'].apply(get_team_tier)
    df['tier_diff'] = (df['radiant_tier'] - df['dire_tier']).abs()
    df['avg_tier'] = (df['radiant_tier'] + df['dire_tier']) / 2
    df['tier_mismatch'] = df['tier_diff']
    df['match_tier_score'] = df['avg_tier']
    df['both_tier1'] = ((df['radiant_tier'] == 1) & (df['dire_tier'] == 1)).astype(int)
    df['tier1_vs_other'] = ((df['radiant_tier'] == 1) ^ (df['dire_tier'] == 1)).astype(int)
    df['both_tier2_plus'] = ((df['radiant_tier'] >= 2) & (df['dire_tier'] >= 2)).astype(int)
    df['is_elite_match'] = (df['match_tier_score'] == 1.0).astype(int)  # Both Tier 1
    df['is_tier2_match'] = (df['match_tier_score'] == 2.0).astype(int)  # Both Tier 2
    df['is_mismatch_match'] = (df['tier_mismatch'] >= 1).astype(int)  # Different tiers
    df['tier_mismatch_known'] = (
        (df['radiant_tier'] <= 2) & (df['dire_tier'] <= 2) & (df['radiant_tier'] != df['dire_tier'])
    ).astype(int)
    df['tier1_vs_tier2'] = (
        ((df['radiant_tier'] == 1) & (df['dire_tier'] == 2))
        | ((df['radiant_tier'] == 2) & (df['dire_tier'] == 1))
    ).astype(int)
    
    # CRITICAL: Заполняем NaN expanding mean (только прошлые данные, NO LEAKAGE)
    # Для каждой строки используем среднее total_kills ТОЛЬКО из предыдущих матчей
    fill_cols = [
        'radiant_team_avg_kills', 'radiant_team_avg_deaths', 'radiant_team_aggression_trend',
        'dire_team_avg_kills', 'dire_team_avg_deaths', 'dire_team_aggression_trend',
        'radiant_patch_team_avg_kills', 'radiant_patch_team_avg_deaths', 'radiant_patch_team_aggression',
        'dire_patch_team_avg_kills', 'dire_patch_team_avg_deaths', 'dire_patch_team_aggression',
        'radiant_patch_team_form_kills', 'dire_patch_team_form_kills',
        'combined_patch_form_kills', 'combined_patch_team_avg_kills', 'combined_patch_team_aggression',
        'h2h_avg_total', 'league_avg_kills',
        # Player stats
        'radiant_players_avg_kills', 'radiant_players_avg_deaths', 'radiant_players_avg_gpm',
        'dire_players_avg_kills', 'dire_players_avg_deaths', 'dire_players_avg_gpm',
        'radiant_carry_avg_kills', 'radiant_mid_avg_kills', 'radiant_mid_avg_gpm',
        'dire_carry_avg_kills', 'dire_mid_avg_kills', 'dire_mid_avg_gpm',
    ]
    
    # Expanding mean: для каждой строки i, среднее total_kills[:i] (только прошлое)
    expanding_mean = df['total_kills'].expanding(min_periods=1).mean().shift(1)
    # Первый матч не имеет истории — используем глобальную медиану как fallback
    global_median = df['total_kills'].median()
    expanding_mean = expanding_mean.fillna(global_median)
    
    for col in fill_cols:
        if col in df.columns:
            # Заполняем NaN expanding mean (no future leakage)
            df[col] = df[col].fillna(expanding_mean)
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    
    logger.info(f"Saved enriched pro matches to {output_path}")
    logger.info(f"Shape: {df.shape}")
    
    # Статистика покрытия (проверяем оригинальные NaN до fillna)
    # Используем expanding_mean как маркер заполненных значений
    team_coverage = (df['radiant_team_avg_kills'] != expanding_mean).mean()
    h2h_coverage = (df['h2h_matches_count'] > 0).mean()
    player_coverage = (df['radiant_players_avg_kills'] != expanding_mean).mean()
    logger.info(f"Team stats coverage: {team_coverage:.1%}")
    logger.info(f"H2H coverage: {h2h_coverage:.1%}")
    logger.info(f"Player stats coverage: {player_coverage:.1%}")
    
    # Mastery coverage
    mastery_coverage = df['radiant_avg_mastery'].notna().mean()
    logger.info(f"Mastery stats coverage: {mastery_coverage:.1%}")
    
    # Synthetic (roster-aware) coverage
    synthetic_coverage = df['radiant_synthetic_avg_kills'].notna().mean()
    avg_roster_coverage = df['combined_roster_coverage'].mean()
    logger.info(f"Synthetic stats coverage: {synthetic_coverage:.1%}")
    logger.info(f"Average roster coverage: {avg_roster_coverage:.1%}")
    
    print("\nFeatures correlation with total_kills:")
    corr_cols = [
        'combined_team_avg_kills', 'combined_team_aggression', 'h2h_avg_total', 'league_avg_kills',
        'combined_players_avg_kills', 'combined_mid_aggression', 'players_kda_diff',
        'total_draft_synergy', 'draft_synergy_diff', 'counter_advantage_abs',
        'combined_mastery', 'mastery_gap', 'both_high_mastery',
        'combined_synthetic_kills', 'combined_synthetic_aggression', 'full_roster_data',
        'total_hard_counters', 'one_sided_counters', 'counter_war',
    ]
    for col in corr_cols:
        if col in df.columns:
            corr = df[col].corr(df['total_kills'])
            print(f"  {col}: {corr:.4f}")


if __name__ == '__main__':
    main(
        pro_json_path='/Users/alex/Documents/ingame/pro_heroes_data/json_parts_split_from_object/clean_data.json',
        hero_stats_path='data/hero_public_stats.csv',
        output_path='data/pro_matches_enriched.csv'
    )
