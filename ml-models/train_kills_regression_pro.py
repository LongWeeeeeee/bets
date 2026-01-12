#!/usr/bin/env python3
"""
Train a pro-match total kills regressor using only pregame + <=10 minute in-game stats.

Constraints:
- NO winRates
- NO networth-based in-game stats
- Use last 100 pro matches (by startDateTime) from clean_data.json as test
"""

from __future__ import annotations

import argparse
from collections import deque
import json
import logging
import math
import sys
from datetime import datetime, timezone
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor, Pool
from sklearn.metrics import mean_absolute_error


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("kills_reg")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = PROJECT_ROOT / "ml-models"

DEFAULT_CLEAN_PATH = PROJECT_ROOT / "pro_heroes_data/json_parts_split_from_object/clean_data.json"
PUB_PLAYERS_DIR = PROJECT_ROOT / "data/pub_timeaware_full/players"
PUB_PRIORS_PATH = MODELS_DIR / "pub_hero_priors.json"
SELECTED_FEATURES_PATH = MODELS_DIR / "selected_features.json"

sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "base"))
from live_predictor import LivePredictor

# Patch schedule used for group training (UTC dates from patch notes).
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


def _build_patch_schedule() -> List[Dict[str, Any]]:
    schedule: List[Dict[str, Any]] = []
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


def get_patch_major_label(ts: int) -> str:
    if ts <= 0 or not _PATCH_SCHEDULE_INFO:
        return "UNKNOWN"
    idx = -1
    for patch in _PATCH_SCHEDULE_INFO:
        if ts >= patch["ts"]:
            idx += 1
        else:
            break
    if idx < 0:
        idx = 0
    label = str(_PATCH_SCHEDULE_INFO[idx]["label"])
    if not label or label == "UNKNOWN":
        return "UNKNOWN"
    base = label
    while base and base[-1].isalpha():
        base = base[:-1]
    return base or label


def get_patch_id(ts: int) -> int:
    if ts <= 0 or not _PATCH_SCHEDULE_INFO:
        return -1
    idx = -1
    for patch in _PATCH_SCHEDULE_INFO:
        if ts >= patch["ts"]:
            idx += 1
        else:
            break
    if idx < 0:
        idx = 0
    return int(_PATCH_SCHEDULE_INFO[idx]["patch_id"])


def patch_label_to_slug(label: str) -> str:
    return (label or "UNKNOWN").replace(".", "_")


@dataclass
class SplitConfig:
    test_size: int = 100
    val_size: int = 300


def _allow_draft_feature(name: str) -> bool:
    n = name.lower()
    if n in {"radiant_team_id", "dire_team_id"}:
        return False
    if n.startswith("radiant_hero_") or n.startswith("dire_hero_"):
        parts = n.split("_")
        if len(parts) == 3 and parts[2].isdigit():
            return False
    if "glicko" in n:
        return False
    if "winrate" in n:
        return False
    return True


def _draft_features(
    predictor: LivePredictor,
    radiant_ids: List[int],
    dire_ids: List[int],
    radiant_team_id: int,
    dire_team_id: int,
    match_start_time: Optional[int] = None,
    league_id: Optional[int] = None,
    series_type: Optional[str] = None,
    region_id: Optional[int] = None,
    tournament_tier: Optional[int] = None,
    radiant_roster_shared_prev: Optional[float] = None,
    dire_roster_shared_prev: Optional[float] = None,
    radiant_roster_changed_prev: Optional[float] = None,
    dire_roster_changed_prev: Optional[float] = None,
    radiant_roster_stable_prev: Optional[float] = None,
    dire_roster_stable_prev: Optional[float] = None,
    radiant_roster_new_team: Optional[float] = None,
    dire_roster_new_team: Optional[float] = None,
    radiant_roster_group_matches: Optional[float] = None,
    dire_roster_group_matches: Optional[float] = None,
    radiant_roster_player_count: Optional[float] = None,
    dire_roster_player_count: Optional[float] = None,
) -> Dict[str, Any]:
    raw = predictor.build_features(
        radiant_ids=radiant_ids,
        dire_ids=dire_ids,
        radiant_account_ids=None,
        dire_account_ids=None,
        radiant_team_id=radiant_team_id if radiant_team_id > 0 else None,
        dire_team_id=dire_team_id if dire_team_id > 0 else None,
        match_start_time=match_start_time,
        league_id=league_id,
        series_type=series_type,
        region_id=region_id,
        tournament_tier=tournament_tier,
        radiant_roster_shared_prev=radiant_roster_shared_prev,
        dire_roster_shared_prev=dire_roster_shared_prev,
        radiant_roster_changed_prev=radiant_roster_changed_prev,
        dire_roster_changed_prev=dire_roster_changed_prev,
        radiant_roster_stable_prev=radiant_roster_stable_prev,
        dire_roster_stable_prev=dire_roster_stable_prev,
        radiant_roster_new_team=radiant_roster_new_team,
        dire_roster_new_team=dire_roster_new_team,
        radiant_roster_group_matches=radiant_roster_group_matches,
        dire_roster_group_matches=dire_roster_group_matches,
        radiant_roster_player_count=radiant_roster_player_count,
        dire_roster_player_count=dire_roster_player_count,
    )
    return {k: v for k, v in raw.items() if _allow_draft_feature(k) and v is not None}


def _load_selected_features(path: Path) -> set:
    if not path.exists():
        return set()
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return set(data.get("selected_features", []))
        if isinstance(data, list):
            return set(data)
    except Exception as e:
        logger.warning("Failed to load selected features: %s", e)
    return set()


def _draft_feature_names() -> set:
    try:
        predictor = LivePredictor()
        feats = _draft_features(
            predictor,
            radiant_ids=[1, 2, 3, 4, 5],
            dire_ids=[6, 7, 8, 9, 10],
            radiant_team_id=0,
            dire_team_id=0,
        )
        return set(feats.keys())
    except Exception as e:
        logger.warning("Draft feature discovery failed: %s", e)
        return set()


def select_feature_cols(
    feature_cols: List[str],
    use_selected: bool,
    selected_path: Path = SELECTED_FEATURES_PATH,
) -> List[str]:
    if not use_selected:
        return feature_cols
    selected = _load_selected_features(selected_path)
    if not selected:
        logger.warning("Selected features list is empty; keeping full feature set.")
        return feature_cols
    draft_names = _draft_feature_names()
    if not draft_names:
        logger.warning("Draft feature names unavailable; keeping full feature set.")
        return feature_cols
    filtered = [c for c in feature_cols if c not in draft_names or c in selected]
    logger.info(
        "Selected draft features: kept=%d dropped=%d",
        len(filtered),
        len(feature_cols) - len(filtered),
    )
    return filtered


def drop_networth_features(cols: List[str]) -> List[str]:
    dropped = []
    kept = []
    for c in cols:
        lc = c.lower()
        if "networth" in lc or "net_worth" in lc or "_nw" in lc or lc.startswith("nw"):
            dropped.append(c)
            continue
        kept.append(c)
    return kept


def _coerce_int(v: Any) -> int:
    try:
        if v is None:
            return 0
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, np.integer)):
            return int(v)
        if isinstance(v, (float, np.floating)):
            return int(v)
        s = str(v).strip()
        if not s:
            return 0
        return int(float(s))
    except Exception:
        return 0


def _league_tier_to_numeric(league: Optional[Dict[str, Any]]) -> int:
    tier = (league or {}).get("tier")
    if tier is None:
        return 1
    tier_str = str(tier).upper()
    if tier_str in {"INTERNATIONAL", "PREMIUM"}:
        return 2
    if tier_str == "AMATEUR":
        return 0
    return 1


def _get_team_tier(team_id: int) -> int:
    """
    Get team tier from id_to_names definitions.
    Returns 1 (Tier 1), 2 (Tier 2), or 3 (Unknown/Rest).
    """
    if team_id <= 0:
        return 3
    try:
        from id_to_names import tier_one_teams, tier_two_teams
    except Exception:
        return 3

    for ids in tier_one_teams.values():
        if isinstance(ids, set):
            if team_id in ids:
                return 1
        elif ids == team_id:
            return 1

    for ids in tier_two_teams.values():
        if isinstance(ids, set):
            if team_id in ids:
                return 2
        elif ids == team_id:
            return 2

    return 3


def _determine_match_tier(
    radiant_team_id: int,
    dire_team_id: int,
    default_unknown: int = 1,
) -> int:
    """
    Determine tournament tier for a match based on team tiers.
    If both teams are Tier 1 → Tier 1 match
    If both teams are Tier 2 → Tier 2 match
    If mixed → use higher tier (Tier 1)
    """
    r_tier = _get_team_tier(radiant_team_id)
    d_tier = _get_team_tier(dire_team_id)

    if r_tier == 1 and d_tier == 1:
        return 1
    if r_tier == 2 and d_tier == 2:
        return 2
    if r_tier <= 2 and d_tier <= 2:
        return min(r_tier, d_tier)

    if r_tier <= 2:
        return r_tier
    if d_tier <= 2:
        return d_tier
    return default_unknown


def _coerce_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, bool):
            return float(v)
        if isinstance(v, (int, float, np.integer, np.floating)):
            return float(v)
        s = str(v).strip()
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def _parse_pos(position: Any) -> Optional[int]:
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
        if 1 <= n <= 5:
            return n
    return None


def load_clean_data(path: Path) -> List[Tuple[int, str, Dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    matches: List[Tuple[int, str, Dict[str, Any]]] = []
    for match_id, match in data.items():
        start_time = _coerce_int(match.get("startDateTime"))
        if start_time <= 0:
            continue
        matches.append((start_time, match_id, match))

    matches.sort(key=lambda x: (x[0], x[1]))
    return matches


def _zscore_map(values: Dict[int, float]) -> Dict[int, float]:
    vals = list(values.values())
    if not vals:
        return {}
    mean = float(np.mean(vals))
    std = float(np.std(vals))
    if std <= 1e-6:
        return {k: 0.0 for k in values}
    return {k: (v - mean) / std for k, v in values.items()}


def build_pub_hero_priors(players_dir: Path, cache_path: Path) -> Dict[int, Dict[str, float]]:
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return {int(k): v for k, v in data.items()}

    if not players_dir.exists():
        logger.warning("pub players dir not found: %s", players_dir)
        return {}

    logger.info("Building public hero priors from %s ...", players_dir)

    sums: Dict[int, Dict[str, float]] = {}
    counts: Dict[int, int] = {}

    parts = sorted(players_dir.rglob("part.parquet"))
    for part in parts:
        df = pd.read_parquet(
            part,
            columns=["hero_id", "kills", "deaths", "assists", "duration_min"],
        )
        grp = df.groupby("hero_id").agg(
            kills=("kills", "sum"),
            deaths=("deaths", "sum"),
            assists=("assists", "sum"),
            duration=("duration_min", "sum"),
            count=("hero_id", "size"),
        )

        for hero_id, row in grp.iterrows():
            hid = int(hero_id)
            s = sums.get(hid)
            if s is None:
                s = {"kills": 0.0, "deaths": 0.0, "assists": 0.0, "duration": 0.0}
                sums[hid] = s
                counts[hid] = 0
            s["kills"] += float(row["kills"])
            s["deaths"] += float(row["deaths"])
            s["assists"] += float(row["assists"])
            s["duration"] += float(row["duration"])
            counts[hid] += int(row["count"])

    if not sums:
        return {}

    kills_avg = {hid: sums[hid]["kills"] / counts[hid] for hid in sums}
    deaths_avg = {hid: sums[hid]["deaths"] / counts[hid] for hid in sums}
    assists_avg = {hid: sums[hid]["assists"] / counts[hid] for hid in sums}
    dur_avg = {hid: sums[hid]["duration"] / counts[hid] for hid in sums}
    kpm_avg = {
        hid: (sums[hid]["kills"] / sums[hid]["duration"]) if sums[hid]["duration"] > 0 else 0.0
        for hid in sums
    }
    dpm_avg = {
        hid: (sums[hid]["deaths"] / sums[hid]["duration"]) if sums[hid]["duration"] > 0 else 0.0
        for hid in sums
    }
    apm_avg = {
        hid: (sums[hid]["assists"] / sums[hid]["duration"]) if sums[hid]["duration"] > 0 else 0.0
        for hid in sums
    }
    kapm_avg = {
        hid: ((sums[hid]["kills"] + sums[hid]["assists"]) / sums[hid]["duration"])
        if sums[hid]["duration"] > 0
        else 0.0
        for hid in sums
    }
    kda_avg = {
        hid: ((sums[hid]["kills"] + sums[hid]["assists"]) / max(1.0, sums[hid]["deaths"]))
        for hid in sums
    }

    kills_z = _zscore_map(kills_avg)
    deaths_z = _zscore_map(deaths_avg)
    assists_z = _zscore_map(assists_avg)
    kpm_z = _zscore_map(kpm_avg)
    dpm_z = _zscore_map(dpm_avg)
    apm_z = _zscore_map(apm_avg)
    kapm_z = _zscore_map(kapm_avg)
    kda_z = _zscore_map(kda_avg)
    dur_z = _zscore_map(dur_avg)

    priors: Dict[int, Dict[str, float]] = {}
    for hid in sums:
        priors[hid] = {
            "kills_z": kills_z.get(hid, 0.0),
            "deaths_z": deaths_z.get(hid, 0.0),
            "assists_z": assists_z.get(hid, 0.0),
            "kpm_z": kpm_z.get(hid, 0.0),
            "dpm_z": dpm_z.get(hid, 0.0),
            "apm_z": apm_z.get(hid, 0.0),
            "kapm_z": kapm_z.get(hid, 0.0),
            "kda_z": kda_z.get(hid, 0.0),
            "dur_z": dur_z.get(hid, 0.0),
        }

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump({str(k): v for k, v in priors.items()}, f)

    logger.info("Saved public hero priors: %s", cache_path)
    return priors


def _linear_slope(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    x = np.arange(len(values), dtype=np.float64)
    y = np.array(values, dtype=np.float64)
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    denom = float(((x - x_mean) ** 2).sum())
    if denom <= 1e-9:
        return 0.0
    return float(((x - x_mean) * (y - y_mean)).sum() / denom)


def build_dataset(
    matches: List[Tuple[int, str, Dict[str, Any]]],
    pub_priors: Dict[int, Dict[str, float]],
) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []

    try:
        draft_predictor = LivePredictor()
    except Exception as e:
        logger.warning("Draft features disabled: %s", e)
        draft_predictor = None

    # Time-aware stats
    player_stats: Dict[int, Dict[str, float]] = {}
    team_stats: Dict[int, Dict[str, float]] = {}
    roster_group_stats: Dict[Tuple[int, int], Dict[str, float]] = {}
    hero_stats: Dict[int, Dict[str, float]] = {}
    hero_pair_stats: Dict[Tuple[int, int], Dict[str, float]] = {}
    hero_vs_stats: Dict[Tuple[int, int], Dict[str, float]] = {}
    player_hero_stats: Dict[Tuple[int, int], Dict[str, float]] = {}
    player_pair_stats: Dict[Tuple[int, int], Dict[str, float]] = {}
    team_vs_stats: Dict[Tuple[int, int], Dict[str, float]] = {}
    team_early_stats: Dict[int, Dict[str, float]] = {}
    hero_early_stats: Dict[int, Dict[str, float]] = {}
    player_early_stats: Dict[int, Dict[str, float]] = {}
    team_vs_early_stats: Dict[Tuple[int, int], Dict[str, float]] = {}
    league_stats: Dict[int, Dict[str, float]] = {}
    version_stats: Dict[int, Dict[str, float]] = {}
    team_roster_state: Dict[int, Dict[str, Any]] = {}

    def compute_roster_state(
        team_id: int,
        roster_ids: List[int],
        min_shared: int = 3,
    ) -> Dict[str, Any]:
        if team_id <= 0:
            return {
                "roster_shared_prev": None,
                "roster_changed_prev": None,
                "roster_stable_prev": 0,
                "roster_new_team": 0,
                "roster_group_id": -1,
                "roster_group_matches": 0,
                "roster_player_count": 0,
            }

        roster = {int(pid) for pid in roster_ids if int(pid) > 0}
        roster_count = len(roster)
        prev = team_roster_state.get(team_id)
        shared = None
        changed = None
        stable = 0
        new_team = 0
        group_id = 0
        group_matches = 1

        if prev is None or roster_count < min_shared:
            new_team = 1
            group_id = 0 if prev is None else prev["group_id"] + 1
            group_matches = 1
        else:
            prev_roster = prev.get("roster") or set()
            shared = len(roster & prev_roster)
            if roster_count == 5 and len(prev_roster) == 5:
                changed = 5 - shared
            else:
                changed = roster_count - shared
            if shared >= min_shared:
                stable = 1
                new_team = 0
                group_id = prev["group_id"]
                group_matches = prev["group_matches"] + 1
            else:
                stable = 0
                new_team = 1
                group_id = prev["group_id"] + 1
                group_matches = 1

        team_roster_state[team_id] = {
            "roster": roster,
            "group_id": group_id,
            "group_matches": group_matches,
        }

        return {
            "roster_shared_prev": shared,
            "roster_changed_prev": changed,
            "roster_stable_prev": stable,
            "roster_new_team": new_team,
            "roster_group_id": group_id,
            "roster_group_matches": group_matches,
            "roster_player_count": roster_count,
        }

    global_player = {
        "count": 0,
        "kills": 0.0,
        "deaths": 0.0,
        "assists": 0.0,
        "duration": 0.0,
        "gpm": 0.0,
        "xpm": 0.0,
        "hero_damage": 0.0,
        "tower_damage": 0.0,
        "imp": 0.0,
        "lhpm": 0.0,
        "denypm": 0.0,
        "healpm": 0.0,
        "invispm": 0.0,
        "level": 0.0,
    }
    global_team = {
        "count": 0,
        "kills_for": 0.0,
        "kills_against": 0.0,
        "total_kills": 0.0,
        "kpm": 0.0,
        "duration": 0.0,
        "over50": 0.0,
        "under40": 0.0,
    }
    global_hero = {
        "count": 0,
        "total_kills": 0.0,
        "kpm": 0.0,
        "duration": 0.0,
        "over50": 0.0,
        "under40": 0.0,
    }
    global_pair = {"count": 0, "total_kills": 0.0, "kpm": 0.0, "duration": 0.0}
    global_vs = {"count": 0, "total_kills": 0.0, "kpm": 0.0}
    global_player_hero = {"count": 0, "total_kills": 0.0, "kpm": 0.0}
    global_player_pair = {"count": 0, "total_kills": 0.0, "kpm": 0.0, "duration": 0.0}
    global_team_vs = {"count": 0, "total_kills": 0.0, "kpm": 0.0, "over50": 0.0, "under40": 0.0}
    global_team_early = {
        "count": 0,
        "for10": 0.0,
        "against10": 0.0,
        "total10": 0.0,
        "share10": 0.0,
        "diff10": 0.0,
    }
    global_hero_early = {
        "count": 0,
        "for10": 0.0,
        "against10": 0.0,
        "total10": 0.0,
        "share10": 0.0,
    }
    global_player_early = {
        "count": 0,
        "for10": 0.0,
        "against10": 0.0,
        "total10": 0.0,
        "share10": 0.0,
    }
    global_team_vs_early = {
        "count": 0,
        "total10": 0.0,
        "kpm10": 0.0,
        "abs_diff10": 0.0,
    }
    global_league = {
        "count": 0,
        "total_kills": 0.0,
        "kpm": 0.0,
        "duration": 0.0,
        "over50": 0.0,
        "under40": 0.0,
    }
    global_version = {
        "count": 0,
        "total_kills": 0.0,
        "kpm": 0.0,
        "duration": 0.0,
        "over50": 0.0,
        "under40": 0.0,
    }
    recent_window = 8
    hero_recent_window = 20
    player_recent_window = 20
    team_recent: Dict[int, deque] = {}
    hero_recent: Dict[int, deque] = {}
    player_recent: Dict[int, deque] = {}
    team_elo: Dict[int, float] = {}
    team_games: Dict[int, int] = {}
    player_hero_set: Dict[int, set] = {}

    def elo_expected(r_a: float, r_b: float) -> float:
        return 1.0 / (1.0 + 10 ** ((r_b - r_a) / 400.0))

    def elo_k(games_played: int) -> float:
        return max(10.0, 64.0 / math.sqrt(1.0 + games_played))

    def recent_stats(team_id: int) -> Tuple[float, float, float, float, float, float, int]:
        hist = team_recent.get(team_id)
        if not hist:
            return (float("nan"), float("nan"), float("nan"), float("nan"), float("nan"), float("nan"), 0)
        totals = [t for t, _, _ in hist]
        kpms = [k for _, k, _ in hist]
        durs = [d for _, _, d in hist]
        count = len(totals)
        avg_total = sum(totals) / count
        avg_kpm = sum(kpms) / count
        avg_dur = sum(durs) / count
        over50 = sum(1 for t in totals if t > 50) / count
        under40 = sum(1 for t in totals if t < 40) / count
        std_total = math.sqrt(sum((t - avg_total) ** 2 for t in totals) / count) if count else float("nan")
        return avg_total, avg_kpm, avg_dur, over50, under40, std_total, count

    def avg_stat(stats: Optional[Dict[str, float]], key: str, global_stats: Dict[str, float], global_key: str) -> float:
        if stats and stats["count"] > 0:
            return stats[key] / stats["count"]
        if global_stats["count"] > 0:
            return global_stats[global_key] / global_stats["count"]
        return 0.0

    def avg_pair(stats: Optional[Dict[str, float]], key: str, global_stats: Dict[str, float]) -> Tuple[float, int]:
        if stats and stats["count"] > 0:
            return stats[key] / stats["count"], int(stats["count"])
        if global_stats["count"] > 0:
            return global_stats[key] / global_stats["count"], int(global_stats["count"])
        return 0.0, 0

    def early_avg(stats: Optional[Dict[str, float]], key: str, global_stats: Dict[str, float]) -> float:
        if stats and stats["count"] > 0:
            return stats.get(key, 0.0) / stats["count"]
        if global_stats["count"] > 0:
            return global_stats.get(key, 0.0) / global_stats["count"]
        return 0.0

    for start_time, match_id, match in matches:
        players = match.get("players") or []
        if len(players) != 10:
            continue

        radiant = [p for p in players if p.get("isRadiant")]
        dire_players = [p for p in players if not p.get("isRadiant")]
        if len(radiant) != 5 or len(dire_players) != 5:
            continue

        def sort_key(p: Dict[str, Any]) -> int:
            pos = p.get("position") or "POSITION_5"
            return _coerce_int(str(pos).replace("POSITION_", ""))

        radiant.sort(key=sort_key)
        dire_players.sort(key=sort_key)

        rad = match.get("radiantKills")
        dire = match.get("direKills")
        xp = match.get("radiantExperienceLeads")
        nw = match.get("radiantNetworthLeads")

        rad_list = rad if isinstance(rad, list) else []
        dire_list = dire if isinstance(dire, list) else []
        xp_list = xp if isinstance(xp, list) else []
        nw_list = nw if isinstance(nw, list) else []

        rad_len = len(rad_list)
        dire_len = len(dire_list)
        xp_len = len(xp_list)
        nw_len = len(nw_list)

        def minute_val(arr: List[Any], idx: int) -> float:
            if idx >= len(arr):
                return float("nan")
            val = arr[idx]
            try:
                return float(val)
            except Exception:
                return float("nan")

        rad_vals = [minute_val(rad_list, i) for i in range(10)]
        dire_vals = [minute_val(dire_list, i) for i in range(10)]
        xp_vals = [minute_val(xp_list, i) for i in range(10)]
        nw_vals = [minute_val(nw_list, i) for i in range(10)]

        total_per_min: List[float] = []
        for rv, dv in zip(rad_vals, dire_vals):
            if math.isnan(rv) or math.isnan(dv):
                total_per_min.append(float("nan"))
            else:
                total_per_min.append(rv + dv)
        lead_vals = [
            (rv - dv) if not math.isnan(rv) and not math.isnan(dv) else float("nan")
            for rv, dv in zip(rad_vals, dire_vals)
        ]

        kill_minutes_available = min(10, rad_len, dire_len) if rad_len and dire_len else 0
        xp_minutes_available = min(10, xp_len) if xp_len else 0
        nw_minutes_available = min(10, nw_len) if nw_len else 0
        has_kill_series = 1 if kill_minutes_available > 0 else 0
        has_xp_series = 1 if xp_minutes_available > 0 else 0
        has_nw_series = 1 if nw_minutes_available > 0 else 0
        has_full_early = (
            1
            if kill_minutes_available >= 10 and xp_minutes_available >= 10 and nw_minutes_available >= 10
            else 0
        )

        rad_valid = [v for v in rad_vals if not math.isnan(v)]
        dire_valid = [v for v in dire_vals if not math.isnan(v)]
        total_valid = [v for v in total_per_min if not math.isnan(v)]
        lead_valid = [v for v in lead_vals if not math.isnan(v)]

        rad10 = float(np.sum(rad_valid)) if rad_valid else float("nan")
        dire10 = float(np.sum(dire_valid)) if dire_valid else float("nan")
        total10 = float(np.sum(total_valid)) if total_valid else float("nan")
        kpm10 = (total10 / 10.0) if not math.isnan(total10) else float("nan")
        diff10 = (rad10 - dire10) if not math.isnan(rad10) and not math.isnan(dire10) else float("nan")
        lead10 = float(np.sum(lead_valid)) if lead_valid else float("nan")
        lead_abs10 = float(np.sum([abs(v) for v in lead_valid])) if lead_valid else float("nan")
        if lead_valid:
            lead_mean = float(np.mean(lead_valid))
            lead_std = float(np.sqrt(np.mean([(v - lead_mean) ** 2 for v in lead_valid])))
        else:
            lead_std = float("nan")

        accel = float("nan")
        if kill_minutes_available >= 10 and not any(math.isnan(v) for v in total_per_min):
            accel = sum(total_per_min[-3:]) - sum(total_per_min[:7])

        kill_std = float(np.nanstd(total_per_min)) if total_valid else float("nan")
        kill_zero = sum(1 for v in total_valid if v == 0) if total_valid else float("nan")
        kill_max = float(np.nanmax(total_per_min)) if total_valid else float("nan")
        kill_slope = _linear_slope(total_valid) if len(total_valid) >= 2 else float("nan")

        first5 = float("nan")
        last5 = float("nan")
        if kill_minutes_available >= 10 and not any(math.isnan(v) for v in total_per_min):
            first5 = sum(total_per_min[:5])
            last5 = sum(total_per_min[5:10])

        xp10 = float("nan")
        if xp_len >= 10:
            xp10 = _coerce_float(xp_list[9])
        elif xp_len > 0:
            xp10 = _coerce_float(xp_list[-1])

        xp5 = float("nan")
        if xp_len >= 5:
            xp5 = _coerce_float(xp_list[4])

        xp_valid = [v for v in xp_vals if not math.isnan(v)]
        xp_mean = float(np.nanmean(xp_vals)) if xp_valid else float("nan")
        xp_std = float(np.nanstd(xp_vals)) if xp_valid else float("nan")
        xp_slope = _linear_slope(xp_valid) if len(xp_valid) >= 2 else float("nan")

        def safe_mean(vals: List[float]) -> float:
            if not vals or any(math.isnan(v) for v in vals):
                return float("nan")
            return float(sum(vals) / len(vals))

        xp_first5 = safe_mean(xp_vals[:5])
        xp_last5 = safe_mean(xp_vals[5:10])
        xp_change_5_10 = (
            (xp_last5 - xp_first5) if not math.isnan(xp_first5) and not math.isnan(xp_last5) else float("nan")
        )
        xp_abs_mean = float(np.nanmean(np.abs(xp_vals))) if xp_valid else float("nan")
        xp_abs_max = float(np.nanmax(np.abs(xp_vals))) if xp_valid else float("nan")
        xp_pos_frac = float(np.sum([1 for v in xp_valid if v > 0]) / len(xp_valid)) if xp_valid else float("nan")
        xp_neg_frac = float(np.sum([1 for v in xp_valid if v < 0]) / len(xp_valid)) if xp_valid else float("nan")

        def sign_changes(vals: List[float]) -> float:
            signs = []
            for v in vals:
                if v > 0:
                    signs.append(1)
                elif v < 0:
                    signs.append(-1)
            if len(signs) < 2:
                return 0.0
            return float(sum(1 for i in range(1, len(signs)) if signs[i] != signs[i - 1]))

        xp_sign_changes = sign_changes(xp_valid) if xp_valid else float("nan")

        nw10 = float("nan")
        if nw_len >= 10:
            nw10 = _coerce_float(nw_list[9])
        elif nw_len > 0:
            nw10 = _coerce_float(nw_list[-1])

        nw5 = float("nan")
        if nw_len >= 5:
            nw5 = _coerce_float(nw_list[4])

        nw_valid = [v for v in nw_vals if not math.isnan(v)]
        nw_mean = float(np.nanmean(nw_vals)) if nw_valid else float("nan")
        nw_std = float(np.nanstd(nw_vals)) if nw_valid else float("nan")
        nw_slope = _linear_slope(nw_valid) if len(nw_valid) >= 2 else float("nan")
        nw_first5 = safe_mean(nw_vals[:5])
        nw_last5 = safe_mean(nw_vals[5:10])
        nw_change_5_10 = (
            (nw_last5 - nw_first5) if not math.isnan(nw_first5) and not math.isnan(nw_last5) else float("nan")
        )
        nw_abs_mean = float(np.nanmean(np.abs(nw_vals))) if nw_valid else float("nan")
        nw_abs_max = float(np.nanmax(np.abs(nw_vals))) if nw_valid else float("nan")
        nw_pos_frac = float(np.sum([1 for v in nw_valid if v > 0]) / len(nw_valid)) if nw_valid else float("nan")
        nw_neg_frac = float(np.sum([1 for v in nw_valid if v < 0]) / len(nw_valid)) if nw_valid else float("nan")
        nw_sign_changes = sign_changes(nw_valid) if nw_valid else float("nan")

        nw_per_kill10 = float("nan")
        if not math.isnan(nw10) and not math.isnan(total10) and total10 > 0:
            nw_per_kill10 = nw10 / total10
        xp_per_kill10 = float("nan")
        if not math.isnan(xp10) and not math.isnan(total10) and total10 > 0:
            xp_per_kill10 = xp10 / total10

        fb_time = _coerce_int(match.get("firstBloodTime"))
        if fb_time and fb_time <= 600:
            fb_happened = 1
            fb_time_10 = fb_time
        else:
            fb_happened = 0
            fb_time_10 = 600

        # Lineup IDs
        rad_ids = [int(p.get("heroId") or 0) for p in radiant]
        dire_ids = [int(p.get("heroId") or 0) for p in dire_players]
        rad_pids = [int((p.get("steamAccount") or {}).get("id") or 0) for p in radiant]
        dire_pids = [int((p.get("steamAccount") or {}).get("id") or 0) for p in dire_players]

        # Public priors (relative, z-scored)
        def pub_vals(hero_ids: Iterable[int], key: str) -> List[float]:
            vals = []
            for hid in hero_ids:
                pri = pub_priors.get(hid)
                vals.append(pri.get(key, 0.0) if pri else 0.0)
            return vals

        rad_pub_kills = pub_vals(rad_ids, "kills_z")
        dire_pub_kills = pub_vals(dire_ids, "kills_z")
        rad_pub_deaths = pub_vals(rad_ids, "deaths_z")
        dire_pub_deaths = pub_vals(dire_ids, "deaths_z")
        rad_pub_assists = pub_vals(rad_ids, "assists_z")
        dire_pub_assists = pub_vals(dire_ids, "assists_z")
        rad_pub_kpm = pub_vals(rad_ids, "kpm_z")
        dire_pub_kpm = pub_vals(dire_ids, "kpm_z")
        rad_pub_dpm = pub_vals(rad_ids, "dpm_z")
        dire_pub_dpm = pub_vals(dire_ids, "dpm_z")
        rad_pub_apm = pub_vals(rad_ids, "apm_z")
        dire_pub_apm = pub_vals(dire_ids, "apm_z")
        rad_pub_kapm = pub_vals(rad_ids, "kapm_z")
        dire_pub_kapm = pub_vals(dire_ids, "kapm_z")
        rad_pub_kda = pub_vals(rad_ids, "kda_z")
        dire_pub_kda = pub_vals(dire_ids, "kda_z")
        rad_pub_dur = pub_vals(rad_ids, "dur_z")
        dire_pub_dur = pub_vals(dire_ids, "dur_z")

        # Time-aware hero stats (pro history only)
        def hero_avg(hid: int, key: str, global_key: str) -> float:
            if hid <= 0:
                return avg_stat(None, key, global_hero, global_key)
            return avg_stat(hero_stats.get(hid), key, global_hero, global_key)

        def hero_recent_avg(hid: int) -> Tuple[float, float, float, int]:
            hist = hero_recent.get(hid)
            if hist:
                totals = [t for t, _, _ in hist]
                kpms = [k for _, k, _ in hist]
                durs = [d for _, _, d in hist]
                count = len(totals)
                return (
                    float(sum(totals) / count),
                    float(sum(kpms) / count),
                    float(sum(durs) / count),
                    count,
                )
            return (
                hero_avg(hid, "total_kills", "total_kills"),
                hero_avg(hid, "kpm", "kpm"),
                hero_avg(hid, "duration", "duration"),
                0,
            )

        rad_hero_avg_kills = sum(hero_avg(h, "total_kills", "total_kills") for h in rad_ids)
        dire_hero_avg_kills = sum(hero_avg(h, "total_kills", "total_kills") for h in dire_ids)
        rad_hero_avg_kpm = sum(hero_avg(h, "kpm", "kpm") for h in rad_ids)
        dire_hero_avg_kpm = sum(hero_avg(h, "kpm", "kpm") for h in dire_ids)
        rad_hero_avg_dur = sum(hero_avg(h, "duration", "duration") for h in rad_ids)
        dire_hero_avg_dur = sum(hero_avg(h, "duration", "duration") for h in dire_ids)
        rad_hero_recent_kills = sum(hero_recent_avg(h)[0] for h in rad_ids)
        dire_hero_recent_kills = sum(hero_recent_avg(h)[0] for h in dire_ids)
        rad_hero_recent_kpm = sum(hero_recent_avg(h)[1] for h in rad_ids)
        dire_hero_recent_kpm = sum(hero_recent_avg(h)[1] for h in dire_ids)
        rad_hero_recent_dur = sum(hero_recent_avg(h)[2] for h in rad_ids)
        dire_hero_recent_dur = sum(hero_recent_avg(h)[2] for h in dire_ids)
        rad_hero_recent_count = float(np.mean([hero_recent_avg(h)[3] for h in rad_ids])) if rad_ids else 0.0
        dire_hero_recent_count = float(np.mean([hero_recent_avg(h)[3] for h in dire_ids])) if dire_ids else 0.0
        rad_hero_over50 = sum(hero_avg(h, "over50", "over50") for h in rad_ids)
        dire_hero_over50 = sum(hero_avg(h, "over50", "over50") for h in dire_ids)
        rad_hero_under40 = sum(hero_avg(h, "under40", "under40") for h in rad_ids)
        dire_hero_under40 = sum(hero_avg(h, "under40", "under40") for h in dire_ids)

        # Time-aware team stats (pro history only)
        radiant_team = (match.get("radiantTeam") or {}).get("id", 0) or 0
        dire_team = (match.get("direTeam") or {}).get("id", 0) or 0
        roster_r = compute_roster_state(int(radiant_team), rad_pids)
        roster_d = compute_roster_state(int(dire_team), dire_pids)
        r_team_tier = _get_team_tier(int(radiant_team))
        d_team_tier = _get_team_tier(int(dire_team))
        match_tier = _determine_match_tier(int(radiant_team), int(dire_team))
        match_tier_known = 1 if (r_team_tier <= 2 and d_team_tier <= 2) else 0
        patch_major_label = get_patch_major_label(start_time)
        patch_id = get_patch_id(start_time)
        def team_avg(tid: int, key: str, global_key: str) -> float:
            if tid <= 0:
                return avg_stat(None, key, global_team, global_key)
            return avg_stat(team_stats.get(tid), key, global_team, global_key)

        def roster_group_avg(tid: int, gid: int, key: str, global_key: str) -> float:
            if tid <= 0 or gid < 0:
                return avg_stat(None, key, global_team, global_key)
            return avg_stat(roster_group_stats.get((tid, gid)), key, global_team, global_key)

        r_team_kills = team_avg(radiant_team, "kills_for", "kills_for")
        d_team_kills = team_avg(dire_team, "kills_for", "kills_for")
        r_team_against = team_avg(radiant_team, "kills_against", "kills_against")
        d_team_against = team_avg(dire_team, "kills_against", "kills_against")
        r_team_total = team_avg(radiant_team, "total_kills", "total_kills")
        d_team_total = team_avg(dire_team, "total_kills", "total_kills")
        r_team_kpm = team_avg(radiant_team, "kpm", "kpm")
        d_team_kpm = team_avg(dire_team, "kpm", "kpm")
        r_team_dur = team_avg(radiant_team, "duration", "duration")
        d_team_dur = team_avg(dire_team, "duration", "duration")
        r_team_over50 = team_avg(radiant_team, "over50", "over50")
        d_team_over50 = team_avg(dire_team, "over50", "over50")
        r_team_under40 = team_avg(radiant_team, "under40", "under40")
        d_team_under40 = team_avg(dire_team, "under40", "under40")
        r_team_hist = team_stats.get(radiant_team, {}).get("count", 0) if radiant_team > 0 else 0
        d_team_hist = team_stats.get(dire_team, {}).get("count", 0) if dire_team > 0 else 0

        r_group_id = int(roster_r.get("roster_group_id", -1) or -1)
        d_group_id = int(roster_d.get("roster_group_id", -1) or -1)

        r_roster_kills = roster_group_avg(radiant_team, r_group_id, "kills_for", "kills_for")
        d_roster_kills = roster_group_avg(dire_team, d_group_id, "kills_for", "kills_for")
        r_roster_against = roster_group_avg(radiant_team, r_group_id, "kills_against", "kills_against")
        d_roster_against = roster_group_avg(dire_team, d_group_id, "kills_against", "kills_against")
        r_roster_total = roster_group_avg(radiant_team, r_group_id, "total_kills", "total_kills")
        d_roster_total = roster_group_avg(dire_team, d_group_id, "total_kills", "total_kills")
        r_roster_kpm = roster_group_avg(radiant_team, r_group_id, "kpm", "kpm")
        d_roster_kpm = roster_group_avg(dire_team, d_group_id, "kpm", "kpm")
        r_roster_dur = roster_group_avg(radiant_team, r_group_id, "duration", "duration")
        d_roster_dur = roster_group_avg(dire_team, d_group_id, "duration", "duration")
        r_roster_over50 = roster_group_avg(radiant_team, r_group_id, "over50", "over50")
        d_roster_over50 = roster_group_avg(dire_team, d_group_id, "over50", "over50")
        r_roster_under40 = roster_group_avg(radiant_team, r_group_id, "under40", "under40")
        d_roster_under40 = roster_group_avg(dire_team, d_group_id, "under40", "under40")
        r_roster_hist = roster_group_stats.get((radiant_team, r_group_id), {}).get("count", 0) if r_group_id >= 0 else 0
        d_roster_hist = roster_group_stats.get((dire_team, d_group_id), {}).get("count", 0) if d_group_id >= 0 else 0

        def team_kill_share(tid: int) -> float:
            st = team_stats.get(tid)
            if st and st["total_kills"] > 0:
                return st["kills_for"] / st["total_kills"]
            if global_team["total_kills"] > 0:
                return global_team["kills_for"] / global_team["total_kills"]
            return 0.0

        def team_kill_ratio(tid: int) -> float:
            st = team_stats.get(tid)
            if st and st["kills_against"] > 0:
                return st["kills_for"] / max(1.0, st["kills_against"])
            if global_team["kills_against"] > 0:
                return global_team["kills_for"] / max(1.0, global_team["kills_against"])
            return 1.0

        r_team_kill_share = team_kill_share(radiant_team)
        d_team_kill_share = team_kill_share(dire_team)
        r_team_kill_ratio = team_kill_ratio(radiant_team)
        d_team_kill_ratio = team_kill_ratio(dire_team)
        r_team_elo = team_elo.get(radiant_team, 1500.0)
        d_team_elo = team_elo.get(dire_team, 1500.0)
        r_team_elo_games = team_games.get(radiant_team, 0)
        d_team_elo_games = team_games.get(dire_team, 0)
        team_elo_diff = r_team_elo - d_team_elo
        team_elo_win_prob = elo_expected(r_team_elo, d_team_elo)
        (
            r_team_recent_total,
            r_team_recent_kpm,
            r_team_recent_dur,
            r_team_recent_over50,
            r_team_recent_under40,
            r_team_recent_std,
            r_team_recent_count,
        ) = recent_stats(radiant_team)
        (
            d_team_recent_total,
            d_team_recent_kpm,
            d_team_recent_dur,
            d_team_recent_over50,
            d_team_recent_under40,
            d_team_recent_std,
            d_team_recent_count,
        ) = recent_stats(dire_team)

        # Time-aware player stats (pro history only)
        def player_avg(pid: int, key: str, global_key: str) -> float:
            if pid <= 0:
                return avg_stat(None, key, global_player, global_key)
            return avg_stat(player_stats.get(pid), key, global_player, global_key)

        def player_kpm(pid: int) -> float:
            p = player_stats.get(pid)
            if p and p["duration"] > 0:
                return p["kills"] / p["duration"]
            if global_player["duration"] > 0:
                return global_player["kills"] / global_player["duration"]
            return 0.0

        def player_aggression(pid: int) -> float:
            p = player_stats.get(pid)
            if p and p["duration"] > 0:
                return (p["kills"] + p["assists"]) / p["duration"]
            if global_player["duration"] > 0:
                return (global_player["kills"] + global_player["assists"]) / global_player["duration"]
            return 0.0

        def player_feed_pm(pid: int) -> float:
            p = player_stats.get(pid)
            if p and p["duration"] > 0:
                return p["deaths"] / p["duration"]
            if global_player["duration"] > 0:
                return global_player["deaths"] / global_player["duration"]
            return 0.0

        def player_hero_share(pid: int, hero_id: int) -> float:
            if pid <= 0 or hero_id <= 0:
                return 0.0
            total = player_stats.get(pid, {}).get("count", 0)
            if total <= 0:
                return 0.0
            ph = player_hero_stats.get((pid, hero_id))
            if not ph:
                return 0.0
            return ph["count"] / total

        def player_unique_count(pid: int) -> int:
            return len(player_hero_set.get(pid, set()))

        def player_recent_avg(pid: int) -> Tuple[float, float, float, float, int]:
            hist = player_recent.get(pid)
            if hist:
                kills = [k for k, _, _, _ in hist]
                deaths = [d for _, d, _, _ in hist]
                assists = [a for _, _, a, _ in hist]
                kpms = [kpm for _, _, _, kpm in hist]
                count = len(kills)
                return (
                    float(sum(kills) / count),
                    float(sum(deaths) / count),
                    float(sum(assists) / count),
                    float(sum(kpms) / count),
                    count,
                )
            return (
                player_avg(pid, "kills", "kills"),
                player_avg(pid, "deaths", "deaths"),
                player_avg(pid, "assists", "assists"),
                player_kpm(pid),
                0,
            )

        rad_player_stats = []
        dire_player_stats = []
        rad_player_recent_stats = []
        dire_player_recent_stats = []
        rad_player_aggr = []
        dire_player_aggr = []
        rad_player_feed = []
        dire_player_feed = []
        rad_player_unique = []
        dire_player_unique = []
        rad_player_hero_share = []
        dire_player_hero_share = []
        for p in radiant:
            sa = p.get("steamAccount") or {}
            pid = _coerce_int(sa.get("id"))
            hero_id = _coerce_int(p.get("heroId"))
            rad_player_stats.append(
                (
                    player_avg(pid, "kills", "kills"),
                    player_avg(pid, "deaths", "deaths"),
                    player_avg(pid, "assists", "assists"),
                    player_kpm(pid),
                    player_avg(pid, "gpm", "gpm"),
                    player_avg(pid, "xpm", "xpm"),
                    player_avg(pid, "hero_damage", "hero_damage"),
                    player_avg(pid, "tower_damage", "tower_damage"),
                    player_avg(pid, "imp", "imp"),
                    player_stats.get(pid, {}).get("count", 0),
                    player_avg(pid, "lhpm", "lhpm"),
                    player_avg(pid, "denypm", "denypm"),
                    player_avg(pid, "healpm", "healpm"),
                    player_avg(pid, "invispm", "invispm"),
                    player_avg(pid, "level", "level"),
                )
            )
            rad_player_recent_stats.append(player_recent_avg(pid))
            rad_player_aggr.append(player_aggression(pid))
            rad_player_feed.append(player_feed_pm(pid))
            rad_player_unique.append(player_unique_count(pid))
            rad_player_hero_share.append(player_hero_share(pid, hero_id))
        for p in dire_players:
            sa = p.get("steamAccount") or {}
            pid = _coerce_int(sa.get("id"))
            hero_id = _coerce_int(p.get("heroId"))
            dire_player_stats.append(
                (
                    player_avg(pid, "kills", "kills"),
                    player_avg(pid, "deaths", "deaths"),
                    player_avg(pid, "assists", "assists"),
                    player_kpm(pid),
                    player_avg(pid, "gpm", "gpm"),
                    player_avg(pid, "xpm", "xpm"),
                    player_avg(pid, "hero_damage", "hero_damage"),
                    player_avg(pid, "tower_damage", "tower_damage"),
                    player_avg(pid, "imp", "imp"),
                    player_stats.get(pid, {}).get("count", 0),
                    player_avg(pid, "lhpm", "lhpm"),
                    player_avg(pid, "denypm", "denypm"),
                    player_avg(pid, "healpm", "healpm"),
                    player_avg(pid, "invispm", "invispm"),
                    player_avg(pid, "level", "level"),
                )
            )
            dire_player_recent_stats.append(player_recent_avg(pid))
            dire_player_aggr.append(player_aggression(pid))
            dire_player_feed.append(player_feed_pm(pid))
            dire_player_unique.append(player_unique_count(pid))
            dire_player_hero_share.append(player_hero_share(pid, hero_id))

        def stats_mean(stats: List[Tuple[float, ...]], idx: int) -> float:
            vals = [s[idx] for s in stats]
            return float(np.mean(vals)) if vals else 0.0

        def stats_std(stats: List[Tuple[float, ...]], idx: int) -> float:
            vals = [s[idx] for s in stats]
            if len(vals) < 2:
                return 0.0
            return float(np.std(vals))

        def stats_kda(stats: List[Tuple[float, ...]]) -> float:
            if not stats:
                return 0.0
            k = stats_mean(stats, 0)
            d = stats_mean(stats, 1)
            a = stats_mean(stats, 2)
            return float((k + a) / max(1.0, d))

        def list_mean(vals: List[float]) -> float:
            return float(np.mean(vals)) if vals else 0.0

        def list_std(vals: List[float]) -> float:
            if len(vals) < 2:
                return 0.0
            return float(np.std(vals))

        def list_min(vals: List[float]) -> float:
            return float(np.min(vals)) if vals else 0.0

        def list_max(vals: List[float]) -> float:
            return float(np.max(vals)) if vals else 0.0

        def team_pair_features(hero_ids: List[int]) -> Tuple[float, float, float, float]:
            vals_kills: List[float] = []
            vals_kpm: List[float] = []
            vals_dur: List[float] = []
            vals_cnt: List[int] = []
            for h1, h2 in combinations(hero_ids, 2):
                if h1 <= 0 or h2 <= 0:
                    continue
                key = (h1, h2) if h1 < h2 else (h2, h1)
                stat = hero_pair_stats.get(key)
                avg_k, cnt = avg_pair(stat, "total_kills", global_pair)
                avg_kpm, _ = avg_pair(stat, "kpm", global_pair)
                avg_dur, _ = avg_pair(stat, "duration", global_pair)
                vals_kills.append(avg_k)
                vals_kpm.append(avg_kpm)
                vals_dur.append(avg_dur)
                vals_cnt.append(cnt)
            if not vals_kills:
                return 0.0, 0.0, 0.0, 0.0
            return (
                float(np.mean(vals_kills)),
                float(np.mean(vals_kpm)),
                float(np.mean(vals_dur)),
                float(np.mean(vals_cnt)) if vals_cnt else 0.0,
            )

        def hero_vs_features(rad_ids: List[int], dire_ids: List[int]) -> Tuple[float, float, float]:
            vals_kills: List[float] = []
            vals_kpm: List[float] = []
            vals_cnt: List[int] = []
            for rh in rad_ids:
                for dh in dire_ids:
                    if rh <= 0 or dh <= 0:
                        continue
                    key = (rh, dh) if rh < dh else (dh, rh)
                    stat = hero_vs_stats.get(key)
                    avg_k, cnt = avg_pair(stat, "total_kills", global_vs)
                    avg_kpm, _ = avg_pair(stat, "kpm", global_vs)
                    vals_kills.append(avg_k)
                    vals_kpm.append(avg_kpm)
                    vals_cnt.append(cnt)
            if not vals_kills:
                return 0.0, 0.0, 0.0
            return (
                float(np.mean(vals_kills)),
                float(np.mean(vals_kpm)),
                float(np.mean(vals_cnt)) if vals_cnt else 0.0,
            )

        def player_hero_features(pids: List[int], hids: List[int]) -> Tuple[float, float, float]:
            vals_kills: List[float] = []
            vals_kpm: List[float] = []
            vals_cnt: List[int] = []
            for pid, hid in zip(pids, hids):
                if pid <= 0 or hid <= 0:
                    continue
                key = (pid, hid)
                stat = player_hero_stats.get(key)
                avg_k, cnt = avg_pair(stat, "total_kills", global_player_hero)
                avg_kpm, _ = avg_pair(stat, "kpm", global_player_hero)
                vals_kills.append(avg_k)
                vals_kpm.append(avg_kpm)
                vals_cnt.append(cnt)
            if not vals_kills:
                return 0.0, 0.0, 0.0
            return (
                float(np.mean(vals_kills)),
                float(np.mean(vals_kpm)),
                float(np.mean(vals_cnt)) if vals_cnt else 0.0,
            )

        def player_pair_features(pids: List[int]) -> Tuple[float, float, float, float]:
            vals_kills: List[float] = []
            vals_kpm: List[float] = []
            vals_dur: List[float] = []
            vals_cnt: List[int] = []
            for p1, p2 in combinations(pids, 2):
                if p1 <= 0 or p2 <= 0:
                    continue
                key = (p1, p2) if p1 < p2 else (p2, p1)
                stat = player_pair_stats.get(key)
                avg_k, cnt = avg_pair(stat, "total_kills", global_player_pair)
                avg_kpm, _ = avg_pair(stat, "kpm", global_player_pair)
                avg_dur, _ = avg_pair(stat, "duration", global_player_pair)
                vals_kills.append(avg_k)
                vals_kpm.append(avg_kpm)
                vals_dur.append(avg_dur)
                vals_cnt.append(cnt)
            if not vals_kills:
                return 0.0, 0.0, 0.0, 0.0
            return (
                float(np.mean(vals_kills)),
                float(np.mean(vals_kpm)),
                float(np.mean(vals_dur)),
                float(np.mean(vals_cnt)) if vals_cnt else 0.0,
            )

        # Target
        total_kills = sum(p.get("kills", 0) for p in players)

        team_vs_avg_kills = 0.0
        team_vs_avg_kpm = 0.0
        team_vs_hist = 0.0
        team_vs_over50_rate = float("nan")
        team_vs_under40_rate = float("nan")
        if radiant_team > 0 and dire_team > 0:
            team_key = (radiant_team, dire_team) if radiant_team < dire_team else (dire_team, radiant_team)
            stat = team_vs_stats.get(team_key)
            team_vs_avg_kills, team_vs_hist = avg_pair(stat, "total_kills", global_team_vs)
            team_vs_avg_kpm, _ = avg_pair(stat, "kpm", global_team_vs)
            if stat and stat.get("count", 0) > 0:
                team_vs_over50_rate = stat.get("over50", 0.0) / stat["count"]
                team_vs_under40_rate = stat.get("under40", 0.0) / stat["count"]
            elif global_team_vs.get("count", 0) > 0:
                team_vs_over50_rate = global_team_vs.get("over50", 0.0) / global_team_vs["count"]
                team_vs_under40_rate = global_team_vs.get("under40", 0.0) / global_team_vs["count"]

        league = match.get("league") or {}
        league_id = _coerce_int(league.get("id"))
        league_avg_kills = avg_stat(league_stats.get(league_id), "total_kills", global_league, "total_kills")
        league_avg_kpm = avg_stat(league_stats.get(league_id), "kpm", global_league, "kpm")
        league_avg_dur = avg_stat(league_stats.get(league_id), "duration", global_league, "duration")
        league_over50 = avg_stat(league_stats.get(league_id), "over50", global_league, "over50")
        league_under40 = avg_stat(league_stats.get(league_id), "under40", global_league, "under40")
        league_hist = league_stats.get(league_id, {}).get("count", 0) if league_id > 0 else 0

        version_id = _coerce_int(match.get("gameVersionId"))
        version_avg_kills = avg_stat(version_stats.get(version_id), "total_kills", global_version, "total_kills")
        version_avg_kpm = avg_stat(version_stats.get(version_id), "kpm", global_version, "kpm")
        version_avg_dur = avg_stat(version_stats.get(version_id), "duration", global_version, "duration")
        version_over50 = avg_stat(version_stats.get(version_id), "over50", global_version, "over50")
        version_under40 = avg_stat(version_stats.get(version_id), "under40", global_version, "under40")
        version_hist = version_stats.get(version_id, {}).get("count", 0) if version_id > 0 else 0

        team_recent_kpm_sum = float("nan")
        if not math.isnan(r_team_recent_kpm) and not math.isnan(d_team_recent_kpm):
            team_recent_kpm_sum = r_team_recent_kpm + d_team_recent_kpm
        hero_recent_kpm_sum = rad_hero_recent_kpm + dire_hero_recent_kpm

        expected_total10_team = (
            team_recent_kpm_sum * 10.0 if not math.isnan(team_recent_kpm_sum) else float("nan")
        )
        expected_total10_hero = (
            hero_recent_kpm_sum * 10.0 if not math.isnan(hero_recent_kpm_sum) else float("nan")
        )

        early_kpm_diff_team = (
            kpm10 - team_recent_kpm_sum
            if not math.isnan(kpm10) and not math.isnan(team_recent_kpm_sum)
            else float("nan")
        )
        early_kpm_diff_hero = (
            kpm10 - hero_recent_kpm_sum
            if not math.isnan(kpm10) and not math.isnan(hero_recent_kpm_sum)
            else float("nan")
        )
        early_total10_ratio_team = (
            (total10 / expected_total10_team)
            if not math.isnan(total10)
            and not math.isnan(expected_total10_team)
            and expected_total10_team > 0
            else float("nan")
        )
        early_total10_ratio_hero = (
            (total10 / expected_total10_hero)
            if not math.isnan(total10)
            and not math.isnan(expected_total10_hero)
            and expected_total10_hero > 0
            else float("nan")
        )
        early_total10_delta_team = (
            total10 - expected_total10_team
            if not math.isnan(total10) and not math.isnan(expected_total10_team)
            else float("nan")
        )
        early_total10_delta_hero = (
            total10 - expected_total10_hero
            if not math.isnan(total10) and not math.isnan(expected_total10_hero)
            else float("nan")
        )

        def team_early_avg(tid: int, key: str) -> float:
            if tid <= 0:
                return early_avg(None, key, global_team_early)
            return early_avg(team_early_stats.get(tid), key, global_team_early)

        def hero_early_avg(hid: int, key: str) -> float:
            if hid <= 0:
                return early_avg(None, key, global_hero_early)
            return early_avg(hero_early_stats.get(hid), key, global_hero_early)

        def player_early_avg(pid: int, key: str) -> float:
            if pid <= 0:
                return early_avg(None, key, global_player_early)
            return early_avg(player_early_stats.get(pid), key, global_player_early)

        def team_vs_early_avg(t1: int, t2: int, key: str) -> float:
            if t1 <= 0 or t2 <= 0:
                return early_avg(None, key, global_team_vs_early)
            pair = (t1, t2) if t1 < t2 else (t2, t1)
            return early_avg(team_vs_early_stats.get(pair), key, global_team_vs_early)

        r_team_early_for10 = team_early_avg(radiant_team, "for10")
        d_team_early_for10 = team_early_avg(dire_team, "for10")
        r_team_early_against10 = team_early_avg(radiant_team, "against10")
        d_team_early_against10 = team_early_avg(dire_team, "against10")
        r_team_early_total10 = team_early_avg(radiant_team, "total10")
        d_team_early_total10 = team_early_avg(dire_team, "total10")
        r_team_early_share10 = team_early_avg(radiant_team, "share10")
        d_team_early_share10 = team_early_avg(dire_team, "share10")
        r_team_early_count = team_early_stats.get(radiant_team, {}).get("count", 0) if radiant_team > 0 else 0
        d_team_early_count = team_early_stats.get(dire_team, {}).get("count", 0) if dire_team > 0 else 0

        team_early_total10_mean = float("nan")
        if not math.isnan(r_team_early_total10) and not math.isnan(d_team_early_total10):
            team_early_total10_mean = (r_team_early_total10 + d_team_early_total10) / 2.0
        early_total10_delta_team_early = (
            total10 - team_early_total10_mean
            if not math.isnan(total10) and not math.isnan(team_early_total10_mean)
            else float("nan")
        )

        rad_hero_early_total = [hero_early_avg(h, "total10") for h in rad_ids]
        dire_hero_early_total = [hero_early_avg(h, "total10") for h in dire_ids]
        rad_hero_early_for = [hero_early_avg(h, "for10") for h in rad_ids]
        dire_hero_early_for = [hero_early_avg(h, "for10") for h in dire_ids]
        rad_hero_early_share = [hero_early_avg(h, "share10") for h in rad_ids]
        dire_hero_early_share = [hero_early_avg(h, "share10") for h in dire_ids]
        rad_hero_early_count = [
            hero_early_stats.get(h, {}).get("count", 0) if h > 0 else 0 for h in rad_ids
        ]
        dire_hero_early_count = [
            hero_early_stats.get(h, {}).get("count", 0) if h > 0 else 0 for h in dire_ids
        ]

        r_player_early_total = [player_early_avg(p, "total10") for p in rad_pids]
        d_player_early_total = [player_early_avg(p, "total10") for p in dire_pids]
        r_player_early_for = [player_early_avg(p, "for10") for p in rad_pids]
        d_player_early_for = [player_early_avg(p, "for10") for p in dire_pids]
        r_player_early_share = [player_early_avg(p, "share10") for p in rad_pids]
        d_player_early_share = [player_early_avg(p, "share10") for p in dire_pids]
        r_player_early_count = [
            player_early_stats.get(p, {}).get("count", 0) if p > 0 else 0 for p in rad_pids
        ]
        d_player_early_count = [
            player_early_stats.get(p, {}).get("count", 0) if p > 0 else 0 for p in dire_pids
        ]

        team_vs_early_total10 = team_vs_early_avg(radiant_team, dire_team, "total10")
        team_vs_early_kpm10 = team_vs_early_avg(radiant_team, dire_team, "kpm10")
        team_vs_early_abs_diff10 = team_vs_early_avg(radiant_team, dire_team, "abs_diff10")
        if radiant_team > 0 and dire_team > 0:
            team_vs_pair = (radiant_team, dire_team) if radiant_team < dire_team else (dire_team, radiant_team)
            team_vs_early_count = team_vs_early_stats.get(team_vs_pair, {}).get("count", 0)
        else:
            team_vs_early_count = 0

        r_pair_kills, r_pair_kpm, r_pair_dur, r_pair_cnt = team_pair_features(rad_ids)
        d_pair_kills, d_pair_kpm, d_pair_dur, d_pair_cnt = team_pair_features(dire_ids)

        hero_vs_kills, hero_vs_kpm, hero_vs_cnt = hero_vs_features(rad_ids, dire_ids)

        r_player_hero_kills, r_player_hero_kpm, r_player_hero_cnt = player_hero_features(rad_pids, rad_ids)
        d_player_hero_kills, d_player_hero_kpm, d_player_hero_cnt = player_hero_features(dire_pids, dire_ids)
        r_player_pair_kills, r_player_pair_kpm, r_player_pair_dur, r_player_pair_cnt = player_pair_features(
            rad_pids
        )
        d_player_pair_kills, d_player_pair_kpm, d_player_pair_dur, d_player_pair_cnt = player_pair_features(
            dire_pids
        )

        dt = datetime.fromtimestamp(start_time, tz=timezone.utc)

        rec: Dict[str, Any] = {
            "start_time": start_time,
            "patch_id": patch_id,
            "patch_major_label": patch_major_label,
            "match_tier": match_tier,
            "match_tier_known": match_tier_known,
            "total_kills": total_kills,
            "kill_minutes_available": kill_minutes_available,
            "xp_minutes_available": xp_minutes_available,
            "nw_minutes_available": nw_minutes_available,
            "has_kill_series": has_kill_series,
            "has_xp_series": has_xp_series,
            "has_nw_series": has_nw_series,
            "has_full_early": has_full_early,
            "radiant_roster_shared_prev": roster_r["roster_shared_prev"],
            "dire_roster_shared_prev": roster_d["roster_shared_prev"],
            "radiant_roster_changed_prev": roster_r["roster_changed_prev"],
            "dire_roster_changed_prev": roster_d["roster_changed_prev"],
            "radiant_roster_stable_prev": roster_r["roster_stable_prev"],
            "dire_roster_stable_prev": roster_d["roster_stable_prev"],
            "radiant_roster_new_team": roster_r["roster_new_team"],
            "dire_roster_new_team": roster_d["roster_new_team"],
            "radiant_roster_group_matches": roster_r["roster_group_matches"],
            "dire_roster_group_matches": roster_d["roster_group_matches"],
            "radiant_roster_player_count": roster_r["roster_player_count"],
            "dire_roster_player_count": roster_d["roster_player_count"],
            "rad10": rad10,
            "dire10": dire10,
            "total10": total10,
            "kpm10": kpm10,
            "diff10": diff10,
            "lead10": lead10,
            "lead_abs10": lead_abs10,
            "lead_std10": lead_std,
            "accel10": accel,
            "kill_std10": kill_std,
            "kill_zero10": kill_zero,
            "kill_max10": kill_max,
            "kill_slope10": kill_slope,
            "first5_kills": first5,
            "last5_kills": last5,
            "first5_kpm": (first5 / 5.0) if not math.isnan(first5) else float("nan"),
            "last5_kpm": (last5 / 5.0) if not math.isnan(last5) else float("nan"),
            "kpm_change_5_10": ((last5 - first5) / 5.0) if not math.isnan(first5) and not math.isnan(last5) else float("nan"),
            "first5_share": (first5 / total10)
            if not math.isnan(first5) and not math.isnan(total10) and total10 > 0
            else float("nan"),
            "last5_share": (last5 / total10)
            if not math.isnan(last5) and not math.isnan(total10) and total10 > 0
            else float("nan"),
            "frontload_kills": (first5 - last5)
            if not math.isnan(first5) and not math.isnan(last5)
            else float("nan"),
            "lead_ratio10": (diff10 / total10)
            if not math.isnan(diff10) and not math.isnan(total10) and total10 > 0
            else float("nan"),
            "lead_abs_ratio10": (lead_abs10 / total10)
            if not math.isnan(lead_abs10) and not math.isnan(total10) and total10 > 0
            else float("nan"),
            "xp10": xp10,
            "xp10_abs": abs(xp10),
            "xp5": xp5,
            "xp_mean10": xp_mean,
            "xp_std10": xp_std,
            "xp_slope10": xp_slope,
            "xp_first5": xp_first5,
            "xp_last5": xp_last5,
            "xp_change_5_10": xp_change_5_10,
            "xp_abs_mean10": xp_abs_mean,
            "xp_abs_max10": xp_abs_max,
            "xp_pos_frac10": xp_pos_frac,
            "xp_neg_frac10": xp_neg_frac,
            "xp_sign_changes10": xp_sign_changes,
            "nw10": nw10,
            "nw10_abs": abs(nw10),
            "nw5": nw5,
            "nw_mean10": nw_mean,
            "nw_std10": nw_std,
            "nw_slope10": nw_slope,
            "nw_first5": nw_first5,
            "nw_last5": nw_last5,
            "nw_change_5_10": nw_change_5_10,
            "nw_abs_mean10": nw_abs_mean,
            "nw_abs_max10": nw_abs_max,
            "nw_pos_frac10": nw_pos_frac,
            "nw_neg_frac10": nw_neg_frac,
            "nw_sign_changes10": nw_sign_changes,
            "nw_per_kill10": nw_per_kill10,
            "xp_per_kill10": xp_per_kill10,
            "fb_time_10": fb_time_10,
            "fb_happened_10": fb_happened,
            "rad_pub_kills_sum": float(sum(rad_pub_kills)),
            "dire_pub_kills_sum": float(sum(dire_pub_kills)),
            "pub_kills_diff": float(sum(rad_pub_kills) - sum(dire_pub_kills)),
            "rad_pub_deaths_sum": float(sum(rad_pub_deaths)),
            "dire_pub_deaths_sum": float(sum(dire_pub_deaths)),
            "pub_deaths_diff": float(sum(rad_pub_deaths) - sum(dire_pub_deaths)),
            "rad_pub_assists_sum": float(sum(rad_pub_assists)),
            "dire_pub_assists_sum": float(sum(dire_pub_assists)),
            "pub_assists_diff": float(sum(rad_pub_assists) - sum(dire_pub_assists)),
            "rad_pub_kpm_sum": float(sum(rad_pub_kpm)),
            "dire_pub_kpm_sum": float(sum(dire_pub_kpm)),
            "pub_kpm_diff": float(sum(rad_pub_kpm) - sum(dire_pub_kpm)),
            "rad_pub_dpm_sum": float(sum(rad_pub_dpm)),
            "dire_pub_dpm_sum": float(sum(dire_pub_dpm)),
            "pub_dpm_diff": float(sum(rad_pub_dpm) - sum(dire_pub_dpm)),
            "rad_pub_apm_sum": float(sum(rad_pub_apm)),
            "dire_pub_apm_sum": float(sum(dire_pub_apm)),
            "pub_apm_diff": float(sum(rad_pub_apm) - sum(dire_pub_apm)),
            "rad_pub_kapm_sum": float(sum(rad_pub_kapm)),
            "dire_pub_kapm_sum": float(sum(dire_pub_kapm)),
            "pub_kapm_diff": float(sum(rad_pub_kapm) - sum(dire_pub_kapm)),
            "rad_pub_kda_sum": float(sum(rad_pub_kda)),
            "dire_pub_kda_sum": float(sum(dire_pub_kda)),
            "pub_kda_diff": float(sum(rad_pub_kda) - sum(dire_pub_kda)),
            "rad_pub_dur_sum": float(sum(rad_pub_dur)),
            "dire_pub_dur_sum": float(sum(dire_pub_dur)),
            "pub_dur_diff": float(sum(rad_pub_dur) - sum(dire_pub_dur)),
            "rad_pub_kills_max": list_max(rad_pub_kills),
            "dire_pub_kills_max": list_max(dire_pub_kills),
            "pub_kills_max_diff": (
                list_max(rad_pub_kills) - list_max(dire_pub_kills)
                if rad_pub_kills and dire_pub_kills
                else 0.0
            ),
            "rad_pub_kills_min": list_min(rad_pub_kills),
            "dire_pub_kills_min": list_min(dire_pub_kills),
            "pub_kills_min_diff": (
                list_min(rad_pub_kills) - list_min(dire_pub_kills)
                if rad_pub_kills and dire_pub_kills
                else 0.0
            ),
            "rad_pub_kpm_max": list_max(rad_pub_kpm),
            "dire_pub_kpm_max": list_max(dire_pub_kpm),
            "pub_kpm_max_diff": (
                list_max(rad_pub_kpm) - list_max(dire_pub_kpm)
                if rad_pub_kpm and dire_pub_kpm
                else 0.0
            ),
            "rad_pub_kpm_min": list_min(rad_pub_kpm),
            "dire_pub_kpm_min": list_min(dire_pub_kpm),
            "pub_kpm_min_diff": (
                list_min(rad_pub_kpm) - list_min(dire_pub_kpm)
                if rad_pub_kpm and dire_pub_kpm
                else 0.0
            ),
            "rad_hero_avg_kills": rad_hero_avg_kills,
            "dire_hero_avg_kills": dire_hero_avg_kills,
            "hero_avg_kills_diff": rad_hero_avg_kills - dire_hero_avg_kills,
            "rad_hero_avg_kpm": rad_hero_avg_kpm,
            "dire_hero_avg_kpm": dire_hero_avg_kpm,
            "hero_avg_kpm_diff": rad_hero_avg_kpm - dire_hero_avg_kpm,
            "rad_hero_avg_dur": rad_hero_avg_dur,
            "dire_hero_avg_dur": dire_hero_avg_dur,
            "hero_avg_dur_diff": rad_hero_avg_dur - dire_hero_avg_dur,
            "rad_hero_recent_kills": rad_hero_recent_kills,
            "dire_hero_recent_kills": dire_hero_recent_kills,
            "hero_recent_kills_diff": rad_hero_recent_kills - dire_hero_recent_kills,
            "rad_hero_recent_kpm": rad_hero_recent_kpm,
            "dire_hero_recent_kpm": dire_hero_recent_kpm,
            "hero_recent_kpm_diff": rad_hero_recent_kpm - dire_hero_recent_kpm,
            "rad_hero_recent_dur": rad_hero_recent_dur,
            "dire_hero_recent_dur": dire_hero_recent_dur,
            "hero_recent_dur_diff": rad_hero_recent_dur - dire_hero_recent_dur,
            "rad_hero_recent_count": rad_hero_recent_count,
            "dire_hero_recent_count": dire_hero_recent_count,
            "rad_hero_over50": rad_hero_over50,
            "dire_hero_over50": dire_hero_over50,
            "hero_over50_diff": rad_hero_over50 - dire_hero_over50,
            "rad_hero_under40": rad_hero_under40,
            "dire_hero_under40": dire_hero_under40,
            "hero_under40_diff": rad_hero_under40 - dire_hero_under40,
            "r_team_avg_kills": r_team_kills,
            "d_team_avg_kills": d_team_kills,
            "team_kills_diff": r_team_kills - d_team_kills,
            "r_team_avg_against": r_team_against,
            "d_team_avg_against": d_team_against,
            "team_against_diff": r_team_against - d_team_against,
            "r_team_avg_total": r_team_total,
            "d_team_avg_total": d_team_total,
            "team_total_diff": r_team_total - d_team_total,
            "r_team_avg_kpm": r_team_kpm,
            "d_team_avg_kpm": d_team_kpm,
            "team_kpm_diff": r_team_kpm - d_team_kpm,
            "r_team_avg_dur": r_team_dur,
            "d_team_avg_dur": d_team_dur,
            "team_dur_diff": r_team_dur - d_team_dur,
            "r_team_over50_rate": r_team_over50,
            "d_team_over50_rate": d_team_over50,
            "team_over50_diff": r_team_over50 - d_team_over50,
            "r_team_under40_rate": r_team_under40,
            "d_team_under40_rate": d_team_under40,
            "team_under40_diff": r_team_under40 - d_team_under40,
            "r_roster_group_kills": r_roster_kills,
            "d_roster_group_kills": d_roster_kills,
            "roster_group_kills_diff": r_roster_kills - d_roster_kills,
            "r_roster_group_against": r_roster_against,
            "d_roster_group_against": d_roster_against,
            "roster_group_against_diff": r_roster_against - d_roster_against,
            "r_roster_group_total": r_roster_total,
            "d_roster_group_total": d_roster_total,
            "roster_group_total_diff": r_roster_total - d_roster_total,
            "r_roster_group_kpm": r_roster_kpm,
            "d_roster_group_kpm": d_roster_kpm,
            "roster_group_kpm_diff": r_roster_kpm - d_roster_kpm,
            "r_roster_group_dur": r_roster_dur,
            "d_roster_group_dur": d_roster_dur,
            "roster_group_dur_diff": r_roster_dur - d_roster_dur,
            "r_roster_group_over50": r_roster_over50,
            "d_roster_group_over50": d_roster_over50,
            "roster_group_over50_diff": r_roster_over50 - d_roster_over50,
            "r_roster_group_under40": r_roster_under40,
            "d_roster_group_under40": d_roster_under40,
            "roster_group_under40_diff": r_roster_under40 - d_roster_under40,
            "r_roster_group_hist": r_roster_hist,
            "d_roster_group_hist": d_roster_hist,
            "roster_group_hist_diff": r_roster_hist - d_roster_hist,
            "r_team_kill_share": r_team_kill_share,
            "d_team_kill_share": d_team_kill_share,
            "team_kill_share_diff": r_team_kill_share - d_team_kill_share,
            "r_team_kill_ratio": r_team_kill_ratio,
            "d_team_kill_ratio": d_team_kill_ratio,
            "team_kill_ratio_diff": r_team_kill_ratio - d_team_kill_ratio,
            "r_team_elo": r_team_elo,
            "d_team_elo": d_team_elo,
            "team_elo_diff": team_elo_diff,
            "team_elo_win_prob": team_elo_win_prob,
            "r_team_elo_games": r_team_elo_games,
            "d_team_elo_games": d_team_elo_games,
            "team_elo_games_diff": r_team_elo_games - d_team_elo_games,
            "r_team_hist_count": r_team_hist,
            "d_team_hist_count": d_team_hist,
            "r_team_recent_total": r_team_recent_total,
            "d_team_recent_total": d_team_recent_total,
            "team_recent_total_diff": r_team_recent_total - d_team_recent_total
            if not math.isnan(r_team_recent_total) and not math.isnan(d_team_recent_total)
            else float("nan"),
            "r_team_recent_kpm": r_team_recent_kpm,
            "d_team_recent_kpm": d_team_recent_kpm,
            "team_recent_kpm_diff": r_team_recent_kpm - d_team_recent_kpm
            if not math.isnan(r_team_recent_kpm) and not math.isnan(d_team_recent_kpm)
            else float("nan"),
            "r_team_recent_dur": r_team_recent_dur,
            "d_team_recent_dur": d_team_recent_dur,
            "team_recent_dur_diff": r_team_recent_dur - d_team_recent_dur
            if not math.isnan(r_team_recent_dur) and not math.isnan(d_team_recent_dur)
            else float("nan"),
            "r_team_recent_over50": r_team_recent_over50,
            "d_team_recent_over50": d_team_recent_over50,
            "team_recent_over50_diff": r_team_recent_over50 - d_team_recent_over50
            if not math.isnan(r_team_recent_over50) and not math.isnan(d_team_recent_over50)
            else float("nan"),
            "r_team_recent_under40": r_team_recent_under40,
            "d_team_recent_under40": d_team_recent_under40,
            "team_recent_under40_diff": r_team_recent_under40 - d_team_recent_under40
            if not math.isnan(r_team_recent_under40) and not math.isnan(d_team_recent_under40)
            else float("nan"),
            "r_team_recent_std": r_team_recent_std,
            "d_team_recent_std": d_team_recent_std,
            "team_recent_std_diff": r_team_recent_std - d_team_recent_std
            if not math.isnan(r_team_recent_std) and not math.isnan(d_team_recent_std)
            else float("nan"),
            "r_team_recent_count": r_team_recent_count,
            "d_team_recent_count": d_team_recent_count,
            "team_recent_kpm_sum": team_recent_kpm_sum,
            "hero_recent_kpm_sum": hero_recent_kpm_sum,
            "expected_total10_team": expected_total10_team,
            "expected_total10_hero": expected_total10_hero,
            "early_kpm_diff_team": early_kpm_diff_team,
            "early_kpm_diff_hero": early_kpm_diff_hero,
            "early_total10_ratio_team": early_total10_ratio_team,
            "early_total10_ratio_hero": early_total10_ratio_hero,
            "early_total10_delta_team": early_total10_delta_team,
            "early_total10_delta_hero": early_total10_delta_hero,
            "r_team_early_for10": r_team_early_for10,
            "d_team_early_for10": d_team_early_for10,
            "team_early_for10_diff": r_team_early_for10 - d_team_early_for10,
            "r_team_early_against10": r_team_early_against10,
            "d_team_early_against10": d_team_early_against10,
            "team_early_against10_diff": r_team_early_against10 - d_team_early_against10,
            "r_team_early_total10": r_team_early_total10,
            "d_team_early_total10": d_team_early_total10,
            "team_early_total10_diff": r_team_early_total10 - d_team_early_total10,
            "r_team_early_share10": r_team_early_share10,
            "d_team_early_share10": d_team_early_share10,
            "team_early_share10_diff": r_team_early_share10 - d_team_early_share10,
            "r_team_early_count": r_team_early_count,
            "d_team_early_count": d_team_early_count,
            "team_early_total10_mean": team_early_total10_mean,
            "early_total10_delta_team_early": early_total10_delta_team_early,
            "rad_hero_early_total10_sum": float(sum(rad_hero_early_total)),
            "dire_hero_early_total10_sum": float(sum(dire_hero_early_total)),
            "hero_early_total10_diff": float(sum(rad_hero_early_total) - sum(dire_hero_early_total)),
            "rad_hero_early_for10_sum": float(sum(rad_hero_early_for)),
            "dire_hero_early_for10_sum": float(sum(dire_hero_early_for)),
            "hero_early_for10_diff": float(sum(rad_hero_early_for) - sum(dire_hero_early_for)),
            "rad_hero_early_share10_mean": list_mean(rad_hero_early_share),
            "dire_hero_early_share10_mean": list_mean(dire_hero_early_share),
            "hero_early_share10_diff": list_mean(rad_hero_early_share) - list_mean(dire_hero_early_share),
            "rad_hero_early_count_mean": list_mean(rad_hero_early_count),
            "dire_hero_early_count_mean": list_mean(dire_hero_early_count),
            "hero_early_count_diff": list_mean(rad_hero_early_count) - list_mean(dire_hero_early_count),
            "r_player_early_total10_mean": list_mean(r_player_early_total),
            "d_player_early_total10_mean": list_mean(d_player_early_total),
            "player_early_total10_diff": list_mean(r_player_early_total) - list_mean(d_player_early_total),
            "r_player_early_for10_mean": list_mean(r_player_early_for),
            "d_player_early_for10_mean": list_mean(d_player_early_for),
            "player_early_for10_diff": list_mean(r_player_early_for) - list_mean(d_player_early_for),
            "r_player_early_total10_max": list_max(r_player_early_total),
            "d_player_early_total10_max": list_max(d_player_early_total),
            "player_early_total10_max_diff": list_max(r_player_early_total) - list_max(d_player_early_total),
            "r_player_early_for10_max": list_max(r_player_early_for),
            "d_player_early_for10_max": list_max(d_player_early_for),
            "player_early_for10_max_diff": list_max(r_player_early_for) - list_max(d_player_early_for),
            "r_player_early_share10_mean": list_mean(r_player_early_share),
            "d_player_early_share10_mean": list_mean(d_player_early_share),
            "player_early_share10_diff": list_mean(r_player_early_share) - list_mean(d_player_early_share),
            "r_player_early_count_mean": list_mean(r_player_early_count),
            "d_player_early_count_mean": list_mean(d_player_early_count),
            "player_early_count_diff": list_mean(r_player_early_count) - list_mean(d_player_early_count),
            "team_vs_early_total10": team_vs_early_total10,
            "team_vs_early_kpm10": team_vs_early_kpm10,
            "team_vs_early_abs_diff10": team_vs_early_abs_diff10,
            "team_vs_early_count": team_vs_early_count,
            "r_player_avg_kills": stats_mean(rad_player_stats, 0),
            "d_player_avg_kills": stats_mean(dire_player_stats, 0),
            "player_kills_diff": stats_mean(rad_player_stats, 0) - stats_mean(dire_player_stats, 0),
            "r_player_avg_deaths": stats_mean(rad_player_stats, 1),
            "d_player_avg_deaths": stats_mean(dire_player_stats, 1),
            "player_deaths_diff": stats_mean(rad_player_stats, 1) - stats_mean(dire_player_stats, 1),
            "r_player_avg_assists": stats_mean(rad_player_stats, 2),
            "d_player_avg_assists": stats_mean(dire_player_stats, 2),
            "player_assists_diff": stats_mean(rad_player_stats, 2) - stats_mean(dire_player_stats, 2),
            "r_player_avg_kpm": stats_mean(rad_player_stats, 3),
            "d_player_avg_kpm": stats_mean(dire_player_stats, 3),
            "player_kpm_diff": stats_mean(rad_player_stats, 3) - stats_mean(dire_player_stats, 3),
            "r_player_avg_gpm": stats_mean(rad_player_stats, 4),
            "d_player_avg_gpm": stats_mean(dire_player_stats, 4),
            "player_gpm_diff": stats_mean(rad_player_stats, 4) - stats_mean(dire_player_stats, 4),
            "r_player_avg_xpm": stats_mean(rad_player_stats, 5),
            "d_player_avg_xpm": stats_mean(dire_player_stats, 5),
            "player_xpm_diff": stats_mean(rad_player_stats, 5) - stats_mean(dire_player_stats, 5),
            "r_player_avg_hero_dmg": stats_mean(rad_player_stats, 6),
            "d_player_avg_hero_dmg": stats_mean(dire_player_stats, 6),
            "player_hero_dmg_diff": stats_mean(rad_player_stats, 6) - stats_mean(dire_player_stats, 6),
            "r_player_avg_tower_dmg": stats_mean(rad_player_stats, 7),
            "d_player_avg_tower_dmg": stats_mean(dire_player_stats, 7),
            "player_tower_dmg_diff": stats_mean(rad_player_stats, 7) - stats_mean(dire_player_stats, 7),
            "r_player_avg_imp": stats_mean(rad_player_stats, 8),
            "d_player_avg_imp": stats_mean(dire_player_stats, 8),
            "player_imp_diff": stats_mean(rad_player_stats, 8) - stats_mean(dire_player_stats, 8),
            "r_player_avg_kda": stats_kda(rad_player_stats),
            "d_player_avg_kda": stats_kda(dire_player_stats),
            "player_kda_diff": stats_kda(rad_player_stats) - stats_kda(dire_player_stats),
            "r_player_hist_count": stats_mean(rad_player_stats, 9),
            "d_player_hist_count": stats_mean(dire_player_stats, 9),
            "r_player_avg_lhpm": stats_mean(rad_player_stats, 10),
            "d_player_avg_lhpm": stats_mean(dire_player_stats, 10),
            "player_lhpm_diff": stats_mean(rad_player_stats, 10) - stats_mean(dire_player_stats, 10),
            "r_player_avg_denypm": stats_mean(rad_player_stats, 11),
            "d_player_avg_denypm": stats_mean(dire_player_stats, 11),
            "player_denypm_diff": stats_mean(rad_player_stats, 11) - stats_mean(dire_player_stats, 11),
            "r_player_avg_healpm": stats_mean(rad_player_stats, 12),
            "d_player_avg_healpm": stats_mean(dire_player_stats, 12),
            "player_healpm_diff": stats_mean(rad_player_stats, 12) - stats_mean(dire_player_stats, 12),
            "r_player_avg_invispm": stats_mean(rad_player_stats, 13),
            "d_player_avg_invispm": stats_mean(dire_player_stats, 13),
            "player_invispm_diff": stats_mean(rad_player_stats, 13) - stats_mean(dire_player_stats, 13),
            "r_player_avg_level": stats_mean(rad_player_stats, 14),
            "d_player_avg_level": stats_mean(dire_player_stats, 14),
            "player_level_diff": stats_mean(rad_player_stats, 14) - stats_mean(dire_player_stats, 14),
            "r_player_recent_kills": stats_mean(rad_player_recent_stats, 0),
            "d_player_recent_kills": stats_mean(dire_player_recent_stats, 0),
            "player_recent_kills_diff": stats_mean(rad_player_recent_stats, 0) - stats_mean(dire_player_recent_stats, 0),
            "r_player_recent_deaths": stats_mean(rad_player_recent_stats, 1),
            "d_player_recent_deaths": stats_mean(dire_player_recent_stats, 1),
            "player_recent_deaths_diff": stats_mean(rad_player_recent_stats, 1) - stats_mean(dire_player_recent_stats, 1),
            "r_player_recent_assists": stats_mean(rad_player_recent_stats, 2),
            "d_player_recent_assists": stats_mean(dire_player_recent_stats, 2),
            "player_recent_assists_diff": stats_mean(rad_player_recent_stats, 2) - stats_mean(dire_player_recent_stats, 2),
            "r_player_recent_kpm": stats_mean(rad_player_recent_stats, 3),
            "d_player_recent_kpm": stats_mean(dire_player_recent_stats, 3),
            "player_recent_kpm_diff": stats_mean(rad_player_recent_stats, 3) - stats_mean(dire_player_recent_stats, 3),
            "r_player_recent_count": stats_mean(rad_player_recent_stats, 4),
            "d_player_recent_count": stats_mean(dire_player_recent_stats, 4),
            "r_player_recent_kills_std": stats_std(rad_player_recent_stats, 0),
            "d_player_recent_kills_std": stats_std(dire_player_recent_stats, 0),
            "player_recent_kills_std_diff": stats_std(rad_player_recent_stats, 0)
            - stats_std(dire_player_recent_stats, 0),
            "r_player_recent_kpm_std": stats_std(rad_player_recent_stats, 3),
            "d_player_recent_kpm_std": stats_std(dire_player_recent_stats, 3),
            "player_recent_kpm_std_diff": stats_std(rad_player_recent_stats, 3)
            - stats_std(dire_player_recent_stats, 3),
            "r_player_aggr_avg": float(np.mean(rad_player_aggr)) if rad_player_aggr else 0.0,
            "d_player_aggr_avg": float(np.mean(dire_player_aggr)) if dire_player_aggr else 0.0,
            "player_aggr_diff": (
                float(np.mean(rad_player_aggr)) - float(np.mean(dire_player_aggr))
                if rad_player_aggr and dire_player_aggr
                else 0.0
            ),
            "r_player_aggr_max": list_max(rad_player_aggr),
            "d_player_aggr_max": list_max(dire_player_aggr),
            "player_aggr_max_diff": list_max(rad_player_aggr) - list_max(dire_player_aggr)
            if rad_player_aggr and dire_player_aggr
            else 0.0,
            "r_player_aggr_min": list_min(rad_player_aggr),
            "d_player_aggr_min": list_min(dire_player_aggr),
            "player_aggr_min_diff": list_min(rad_player_aggr) - list_min(dire_player_aggr)
            if rad_player_aggr and dire_player_aggr
            else 0.0,
            "r_player_aggr_std": list_std(rad_player_aggr),
            "d_player_aggr_std": list_std(dire_player_aggr),
            "player_aggr_std_diff": list_std(rad_player_aggr) - list_std(dire_player_aggr)
            if rad_player_aggr and dire_player_aggr
            else 0.0,
            "r_player_feed_avg": float(np.mean(rad_player_feed)) if rad_player_feed else 0.0,
            "d_player_feed_avg": float(np.mean(dire_player_feed)) if dire_player_feed else 0.0,
            "player_feed_diff": (
                float(np.mean(rad_player_feed)) - float(np.mean(dire_player_feed))
                if rad_player_feed and dire_player_feed
                else 0.0
            ),
            "r_player_feed_max": list_max(rad_player_feed),
            "d_player_feed_max": list_max(dire_player_feed),
            "player_feed_max_diff": list_max(rad_player_feed) - list_max(dire_player_feed)
            if rad_player_feed and dire_player_feed
            else 0.0,
            "r_player_feed_min": list_min(rad_player_feed),
            "d_player_feed_min": list_min(dire_player_feed),
            "player_feed_min_diff": list_min(rad_player_feed) - list_min(dire_player_feed)
            if rad_player_feed and dire_player_feed
            else 0.0,
            "r_player_feed_std": list_std(rad_player_feed),
            "d_player_feed_std": list_std(dire_player_feed),
            "player_feed_std_diff": list_std(rad_player_feed) - list_std(dire_player_feed)
            if rad_player_feed and dire_player_feed
            else 0.0,
            "r_player_unique_avg": float(np.mean(rad_player_unique)) if rad_player_unique else 0.0,
            "d_player_unique_avg": float(np.mean(dire_player_unique)) if dire_player_unique else 0.0,
            "player_unique_diff": (
                float(np.mean(rad_player_unique)) - float(np.mean(dire_player_unique))
                if rad_player_unique and dire_player_unique
                else 0.0
            ),
            "r_player_unique_min": list_min(rad_player_unique),
            "d_player_unique_min": list_min(dire_player_unique),
            "player_unique_min_diff": list_min(rad_player_unique) - list_min(dire_player_unique)
            if rad_player_unique and dire_player_unique
            else 0.0,
            "r_player_unique_max": list_max(rad_player_unique),
            "d_player_unique_max": list_max(dire_player_unique),
            "player_unique_max_diff": list_max(rad_player_unique) - list_max(dire_player_unique)
            if rad_player_unique and dire_player_unique
            else 0.0,
            "r_player_unique_std": list_std(rad_player_unique),
            "d_player_unique_std": list_std(dire_player_unique),
            "player_unique_std_diff": list_std(rad_player_unique) - list_std(dire_player_unique)
            if rad_player_unique and dire_player_unique
            else 0.0,
            "r_player_hero_share_avg": float(np.mean(rad_player_hero_share)) if rad_player_hero_share else 0.0,
            "d_player_hero_share_avg": float(np.mean(dire_player_hero_share)) if dire_player_hero_share else 0.0,
            "player_hero_share_diff": (
                float(np.mean(rad_player_hero_share)) - float(np.mean(dire_player_hero_share))
                if rad_player_hero_share and dire_player_hero_share
                else 0.0
            ),
            "r_player_hero_share_min": float(np.min(rad_player_hero_share)) if rad_player_hero_share else 0.0,
            "d_player_hero_share_min": float(np.min(dire_player_hero_share)) if dire_player_hero_share else 0.0,
            "player_hero_share_min_diff": (
                float(np.min(rad_player_hero_share)) - float(np.min(dire_player_hero_share))
                if rad_player_hero_share and dire_player_hero_share
                else 0.0
            ),
            "r_player_hero_share_max": list_max(rad_player_hero_share),
            "d_player_hero_share_max": list_max(dire_player_hero_share),
            "player_hero_share_max_diff": list_max(rad_player_hero_share) - list_max(dire_player_hero_share)
            if rad_player_hero_share and dire_player_hero_share
            else 0.0,
            "r_player_hero_share_std": list_std(rad_player_hero_share),
            "d_player_hero_share_std": list_std(dire_player_hero_share),
            "player_hero_share_std_diff": list_std(rad_player_hero_share) - list_std(dire_player_hero_share)
            if rad_player_hero_share and dire_player_hero_share
            else 0.0,
            "r_pair_avg_kills": r_pair_kills,
            "d_pair_avg_kills": d_pair_kills,
            "pair_avg_kills_diff": r_pair_kills - d_pair_kills,
            "r_pair_avg_kpm": r_pair_kpm,
            "d_pair_avg_kpm": d_pair_kpm,
            "pair_avg_kpm_diff": r_pair_kpm - d_pair_kpm,
            "r_pair_avg_dur": r_pair_dur,
            "d_pair_avg_dur": d_pair_dur,
            "pair_avg_dur_diff": r_pair_dur - d_pair_dur,
            "r_pair_hist_count": r_pair_cnt,
            "d_pair_hist_count": d_pair_cnt,
            "hero_vs_avg_kills": hero_vs_kills,
            "hero_vs_avg_kpm": hero_vs_kpm,
            "hero_vs_hist_count": hero_vs_cnt,
            "r_player_hero_avg_kills": r_player_hero_kills,
            "d_player_hero_avg_kills": d_player_hero_kills,
            "player_hero_kills_diff": r_player_hero_kills - d_player_hero_kills,
            "r_player_hero_avg_kpm": r_player_hero_kpm,
            "d_player_hero_avg_kpm": d_player_hero_kpm,
            "player_hero_kpm_diff": r_player_hero_kpm - d_player_hero_kpm,
            "r_player_hero_hist_count": r_player_hero_cnt,
            "d_player_hero_hist_count": d_player_hero_cnt,
            "r_player_pair_avg_kills": r_player_pair_kills,
            "d_player_pair_avg_kills": d_player_pair_kills,
            "player_pair_kills_diff": r_player_pair_kills - d_player_pair_kills,
            "r_player_pair_avg_kpm": r_player_pair_kpm,
            "d_player_pair_avg_kpm": d_player_pair_kpm,
            "player_pair_kpm_diff": r_player_pair_kpm - d_player_pair_kpm,
            "r_player_pair_avg_dur": r_player_pair_dur,
            "d_player_pair_avg_dur": d_player_pair_dur,
            "player_pair_dur_diff": r_player_pair_dur - d_player_pair_dur,
            "r_player_pair_avg_cnt": r_player_pair_cnt,
            "d_player_pair_avg_cnt": d_player_pair_cnt,
            "team_vs_avg_kills": team_vs_avg_kills,
            "team_vs_avg_kpm": team_vs_avg_kpm,
            "team_vs_hist_count": team_vs_hist,
            "team_vs_over50_rate": team_vs_over50_rate,
            "team_vs_under40_rate": team_vs_under40_rate,
            "league_avg_kills": league_avg_kills,
            "league_avg_kpm": league_avg_kpm,
            "league_avg_dur": league_avg_dur,
            "league_over50_rate": league_over50,
            "league_under40_rate": league_under40,
            "league_hist_count": league_hist,
            "version_avg_kills": version_avg_kills,
            "version_avg_kpm": version_avg_kpm,
            "version_avg_dur": version_avg_dur,
            "version_over50_rate": version_over50,
            "version_under40_rate": version_under40,
            "version_hist_count": version_hist,
            "start_year": dt.year,
            "start_month": dt.month,
            "start_weekday": dt.weekday(),
            "start_hour": dt.hour,
        }

        # Per-minute features (kills and XP)
        for i in range(10):
            rec[f"rad_kills_m{i+1}"] = rad_vals[i]
            rec[f"dire_kills_m{i+1}"] = dire_vals[i]
            rec[f"total_kills_m{i+1}"] = total_per_min[i]
            rec[f"xp_lead_m{i+1}"] = xp_vals[i]
            rec[f"nw_lead_m{i+1}"] = nw_vals[i]

        # Categorical IDs
        for i, hid in enumerate(rad_ids, 1):
            rec[f"radiant_hero_{i}"] = hid
        for i, hid in enumerate(dire_ids, 1):
            rec[f"dire_hero_{i}"] = hid
        for i, pid in enumerate(rad_pids, 1):
            rec[f"radiant_player_{i}_id"] = pid
        for i, pid in enumerate(dire_pids, 1):
            rec[f"dire_player_{i}_id"] = pid

        rec["radiant_team_id"] = int(radiant_team)
        rec["dire_team_id"] = int(dire_team)
        rec["game_version_id"] = version_id
        rec["league_id"] = league_id

        series = match.get("series") or {}
        rec["series_type"] = (series.get("type") or "UNKNOWN")
        rec["series_game"] = _coerce_int(series.get("game"))

        rec["tournament_round"] = (match.get("tournamentRound") or "UNKNOWN")
        rec["lobby_type"] = (match.get("lobbyType") or "UNKNOWN")
        rec["region_id"] = _coerce_int(match.get("regionId"))
        rec["rank"] = _coerce_int(match.get("rank"))
        rec["bracket"] = _coerce_int(match.get("bracket"))
        rec["bottom_lane_outcome"] = match.get("bottomLaneOutcome") or "UNKNOWN"
        rec["mid_lane_outcome"] = match.get("midLaneOutcome") or "UNKNOWN"
        rec["top_lane_outcome"] = match.get("topLaneOutcome") or "UNKNOWN"

        if draft_predictor is not None:
            match_start_time = _coerce_int(match.get("startDateTime"))
            league_info = match.get("league") or {}
            tournament_tier = _league_tier_to_numeric(league_info)
            draft_feats = _draft_features(
                draft_predictor,
                rad_ids,
                dire_ids,
                int(radiant_team),
                int(dire_team),
                match_start_time=match_start_time,
                league_id=league_id,
                series_type=rec.get("series_type"),
                region_id=rec.get("region_id"),
                tournament_tier=tournament_tier,
                radiant_roster_shared_prev=roster_r["roster_shared_prev"],
                dire_roster_shared_prev=roster_d["roster_shared_prev"],
                radiant_roster_changed_prev=roster_r["roster_changed_prev"],
                dire_roster_changed_prev=roster_d["roster_changed_prev"],
                radiant_roster_stable_prev=roster_r["roster_stable_prev"],
                dire_roster_stable_prev=roster_d["roster_stable_prev"],
                radiant_roster_new_team=roster_r["roster_new_team"],
                dire_roster_new_team=roster_d["roster_new_team"],
                radiant_roster_group_matches=roster_r["roster_group_matches"],
                dire_roster_group_matches=roster_d["roster_group_matches"],
                radiant_roster_player_count=roster_r["roster_player_count"],
                dire_roster_player_count=roster_d["roster_player_count"],
            )
            for key, val in draft_feats.items():
                if key not in rec:
                    rec[key] = val

        records.append(rec)

        # Update stats AFTER generating features (no leakage)
        duration_seconds = _coerce_float(match.get("durationSeconds"))
        duration_min = duration_seconds / 60.0 if duration_seconds > 0 else float(max(rad_len, dire_len))
        if duration_min <= 0:
            duration_min = 0.0

        for p in players:
            sa = p.get("steamAccount") or {}
            pid = _coerce_int(sa.get("id"))
            if pid <= 0:
                continue
            p_kills = float(p.get("kills", 0))
            p_deaths = float(p.get("deaths", 0))
            p_assists = float(p.get("assists", 0))
            p_kpm = (p_kills / duration_min) if duration_min > 0 else 0.0
            st = player_stats.get(pid)
            if st is None:
                st = {
                    "count": 0,
                    "kills": 0.0,
                    "deaths": 0.0,
                    "assists": 0.0,
                    "duration": 0.0,
                    "gpm": 0.0,
                    "xpm": 0.0,
                    "hero_damage": 0.0,
                    "tower_damage": 0.0,
                    "imp": 0.0,
                    "lhpm": 0.0,
                    "denypm": 0.0,
                    "healpm": 0.0,
                    "invispm": 0.0,
                    "level": 0.0,
                }
                player_stats[pid] = st
            st["count"] += 1
            st["kills"] += p_kills
            st["deaths"] += p_deaths
            st["assists"] += p_assists
            st["duration"] += float(duration_min)
            st["gpm"] += _coerce_float(p.get("goldPerMinute"))
            st["xpm"] += _coerce_float(p.get("experiencePerMinute"))
            st["hero_damage"] += _coerce_float(p.get("heroDamage"))
            st["tower_damage"] += _coerce_float(p.get("towerDamage"))
            st["imp"] += _coerce_float(p.get("imp"))
            lh = _coerce_float(p.get("numLastHits"))
            denies = _coerce_float(p.get("numDenies"))
            heal = _coerce_float(p.get("heroHealing"))
            invis = _coerce_float(p.get("invisibleSeconds"))
            level = _coerce_float(p.get("level"))
            if duration_min > 0:
                st["lhpm"] += lh / duration_min
                st["denypm"] += denies / duration_min
                st["healpm"] += heal / duration_min
                st["invispm"] += invis / duration_min
            st["level"] += level

            global_player["count"] += 1
            global_player["kills"] += p_kills
            global_player["deaths"] += p_deaths
            global_player["assists"] += p_assists
            global_player["duration"] += float(duration_min)
            global_player["gpm"] += _coerce_float(p.get("goldPerMinute"))
            global_player["xpm"] += _coerce_float(p.get("experiencePerMinute"))
            global_player["hero_damage"] += _coerce_float(p.get("heroDamage"))
            global_player["tower_damage"] += _coerce_float(p.get("towerDamage"))
            global_player["imp"] += _coerce_float(p.get("imp"))
            if duration_min > 0:
                global_player["lhpm"] += lh / duration_min
                global_player["denypm"] += denies / duration_min
                global_player["healpm"] += heal / duration_min
                global_player["invispm"] += invis / duration_min
            global_player["level"] += level

            hist = player_recent.get(pid)
            if hist is None:
                hist = deque(maxlen=player_recent_window)
                player_recent[pid] = hist
            hist.append((p_kills, p_deaths, p_assists, p_kpm))

            hero_id = _coerce_int(p.get("heroId"))
            if hero_id > 0:
                ph_key = (pid, hero_id)
                ph = player_hero_stats.get(ph_key)
                if ph is None:
                    ph = {"count": 0, "total_kills": 0.0, "kpm": 0.0}
                    player_hero_stats[ph_key] = ph
                ph["count"] += 1
                ph["total_kills"] += float(total_kills)
                ph["kpm"] += float(total_kills / duration_min) if duration_min > 0 else 0.0

                hero_set = player_hero_set.get(pid)
                if hero_set is None:
                    hero_set = set()
                    player_hero_set[pid] = hero_set
                hero_set.add(hero_id)

                global_player_hero["count"] += 1
                global_player_hero["total_kills"] += float(total_kills)
                global_player_hero["kpm"] += float(total_kills / duration_min) if duration_min > 0 else 0.0

        radiant_kills = sum(p.get("kills", 0) for p in radiant)
        dire_kills = sum(p.get("kills", 0) for p in dire_players)
        total_match_kills = radiant_kills + dire_kills
        kpm = total_match_kills / duration_min if duration_min > 0 else 0.0

        if (
            kill_minutes_available >= 10
            and not math.isnan(total10)
            and not math.isnan(rad10)
            and not math.isnan(dire10)
        ):
            r_share10 = (rad10 / total10) if total10 > 0 else 0.0
            d_share10 = (dire10 / total10) if total10 > 0 else 0.0
            for team_id, for10, against10, share10 in (
                (radiant_team, rad10, dire10, r_share10),
                (dire_team, dire10, rad10, d_share10),
            ):
                if team_id <= 0:
                    continue
                st = team_early_stats.get(team_id)
                if st is None:
                    st = {
                        "count": 0,
                        "for10": 0.0,
                        "against10": 0.0,
                        "total10": 0.0,
                        "share10": 0.0,
                        "diff10": 0.0,
                    }
                    team_early_stats[team_id] = st
                st["count"] += 1
                st["for10"] += float(for10)
                st["against10"] += float(against10)
                st["total10"] += float(total10)
                st["share10"] += float(share10)
                st["diff10"] += float(for10 - against10)

                global_team_early["count"] += 1
                global_team_early["for10"] += float(for10)
                global_team_early["against10"] += float(against10)
                global_team_early["total10"] += float(total10)
                global_team_early["share10"] += float(share10)
                global_team_early["diff10"] += float(for10 - against10)

            if radiant_team > 0 and dire_team > 0:
                pair = (radiant_team, dire_team) if radiant_team < dire_team else (dire_team, radiant_team)
                tv = team_vs_early_stats.get(pair)
                if tv is None:
                    tv = {"count": 0, "total10": 0.0, "kpm10": 0.0, "abs_diff10": 0.0}
                    team_vs_early_stats[pair] = tv
                tv["count"] += 1
                tv["total10"] += float(total10)
                tv["kpm10"] += float(total10 / 10.0)
                tv["abs_diff10"] += float(abs(rad10 - dire10))

                global_team_vs_early["count"] += 1
                global_team_vs_early["total10"] += float(total10)
                global_team_vs_early["kpm10"] += float(total10 / 10.0)
                global_team_vs_early["abs_diff10"] += float(abs(rad10 - dire10))

            for hid in rad_ids:
                if hid <= 0:
                    continue
                hs = hero_early_stats.get(hid)
                if hs is None:
                    hs = {"count": 0, "for10": 0.0, "against10": 0.0, "total10": 0.0, "share10": 0.0}
                    hero_early_stats[hid] = hs
                hs["count"] += 1
                hs["for10"] += float(rad10)
                hs["against10"] += float(dire10)
                hs["total10"] += float(total10)
                hs["share10"] += float(r_share10)

                global_hero_early["count"] += 1
                global_hero_early["for10"] += float(rad10)
                global_hero_early["against10"] += float(dire10)
                global_hero_early["total10"] += float(total10)
                global_hero_early["share10"] += float(r_share10)

            for hid in dire_ids:
                if hid <= 0:
                    continue
                hs = hero_early_stats.get(hid)
                if hs is None:
                    hs = {"count": 0, "for10": 0.0, "against10": 0.0, "total10": 0.0, "share10": 0.0}
                    hero_early_stats[hid] = hs
                hs["count"] += 1
                hs["for10"] += float(dire10)
                hs["against10"] += float(rad10)
                hs["total10"] += float(total10)
                hs["share10"] += float(d_share10)

                global_hero_early["count"] += 1
                global_hero_early["for10"] += float(dire10)
                global_hero_early["against10"] += float(rad10)
                global_hero_early["total10"] += float(total10)
                global_hero_early["share10"] += float(d_share10)

            for pid in rad_pids:
                if pid <= 0:
                    continue
                ps = player_early_stats.get(pid)
                if ps is None:
                    ps = {"count": 0, "for10": 0.0, "against10": 0.0, "total10": 0.0, "share10": 0.0}
                    player_early_stats[pid] = ps
                ps["count"] += 1
                ps["for10"] += float(rad10)
                ps["against10"] += float(dire10)
                ps["total10"] += float(total10)
                ps["share10"] += float(r_share10)

                global_player_early["count"] += 1
                global_player_early["for10"] += float(rad10)
                global_player_early["against10"] += float(dire10)
                global_player_early["total10"] += float(total10)
                global_player_early["share10"] += float(r_share10)

            for pid in dire_pids:
                if pid <= 0:
                    continue
                ps = player_early_stats.get(pid)
                if ps is None:
                    ps = {"count": 0, "for10": 0.0, "against10": 0.0, "total10": 0.0, "share10": 0.0}
                    player_early_stats[pid] = ps
                ps["count"] += 1
                ps["for10"] += float(dire10)
                ps["against10"] += float(rad10)
                ps["total10"] += float(total10)
                ps["share10"] += float(d_share10)

                global_player_early["count"] += 1
                global_player_early["for10"] += float(dire10)
                global_player_early["against10"] += float(rad10)
                global_player_early["total10"] += float(total10)
                global_player_early["share10"] += float(d_share10)

        for team_pids in (rad_pids, dire_pids):
            for p1, p2 in combinations(team_pids, 2):
                if p1 <= 0 or p2 <= 0:
                    continue
                key = (p1, p2) if p1 < p2 else (p2, p1)
                pp = player_pair_stats.get(key)
                if pp is None:
                    pp = {"count": 0, "total_kills": 0.0, "kpm": 0.0, "duration": 0.0}
                    player_pair_stats[key] = pp
                pp["count"] += 1
                pp["total_kills"] += float(total_match_kills)
                pp["kpm"] += float(kpm)
                pp["duration"] += float(duration_min)

                global_player_pair["count"] += 1
                global_player_pair["total_kills"] += float(total_match_kills)
                global_player_pair["kpm"] += float(kpm)
                global_player_pair["duration"] += float(duration_min)

        for team_id, kills_for, kills_against in (
            (radiant_team, radiant_kills, dire_kills),
            (dire_team, dire_kills, radiant_kills),
        ):
            t = team_stats.get(team_id)
            if t is None:
                t = {
                    "count": 0,
                    "kills_for": 0.0,
                    "kills_against": 0.0,
                    "total_kills": 0.0,
                    "kpm": 0.0,
                    "duration": 0.0,
                    "over50": 0.0,
                    "under40": 0.0,
                }
                team_stats[team_id] = t
            t["count"] += 1
            t["kills_for"] += float(kills_for)
            t["kills_against"] += float(kills_against)
            t["total_kills"] += float(total_match_kills)
            t["kpm"] += float(kpm)
            t["duration"] += float(duration_min)
            t["over50"] += 1.0 if total_match_kills > 50 else 0.0
            t["under40"] += 1.0 if total_match_kills < 40 else 0.0

            global_team["count"] += 1
            global_team["kills_for"] += float(kills_for)
            global_team["kills_against"] += float(kills_against)
            global_team["total_kills"] += float(total_match_kills)
            global_team["kpm"] += float(kpm)
            global_team["duration"] += float(duration_min)
            global_team["over50"] += 1.0 if total_match_kills > 50 else 0.0
            global_team["under40"] += 1.0 if total_match_kills < 40 else 0.0

        for team_id, kills_for, kills_against, group_id in (
            (radiant_team, radiant_kills, dire_kills, r_group_id),
            (dire_team, dire_kills, radiant_kills, d_group_id),
        ):
            if team_id <= 0 or group_id < 0:
                continue
            key = (team_id, group_id)
            rg = roster_group_stats.get(key)
            if rg is None:
                rg = {
                    "count": 0,
                    "kills_for": 0.0,
                    "kills_against": 0.0,
                    "total_kills": 0.0,
                    "kpm": 0.0,
                    "duration": 0.0,
                    "over50": 0.0,
                    "under40": 0.0,
                }
                roster_group_stats[key] = rg
            rg["count"] += 1
            rg["kills_for"] += float(kills_for)
            rg["kills_against"] += float(kills_against)
            rg["total_kills"] += float(total_match_kills)
            rg["kpm"] += float(kpm)
            rg["duration"] += float(duration_min)
            rg["over50"] += 1.0 if total_match_kills > 50 else 0.0
            rg["under40"] += 1.0 if total_match_kills < 40 else 0.0

        if radiant_team > 0 and dire_team > 0:
            team_key = (radiant_team, dire_team) if radiant_team < dire_team else (dire_team, radiant_team)
            tv = team_vs_stats.get(team_key)
            if tv is None:
                tv = {"count": 0, "total_kills": 0.0, "kpm": 0.0, "over50": 0.0, "under40": 0.0}
                team_vs_stats[team_key] = tv
            tv["count"] += 1
            tv["total_kills"] += float(total_match_kills)
            tv["kpm"] += float(kpm)
            tv["over50"] += 1.0 if total_match_kills > 50 else 0.0
            tv["under40"] += 1.0 if total_match_kills < 40 else 0.0

            global_team_vs["count"] += 1
            global_team_vs["total_kills"] += float(total_match_kills)
            global_team_vs["kpm"] += float(kpm)
            global_team_vs["over50"] += 1.0 if total_match_kills > 50 else 0.0
            global_team_vs["under40"] += 1.0 if total_match_kills < 40 else 0.0

        for team_id in (radiant_team, dire_team):
            if team_id <= 0:
                continue
            hist = team_recent.get(team_id)
            if hist is None:
                hist = deque(maxlen=recent_window)
                team_recent[team_id] = hist
            hist.append((float(total_match_kills), float(kpm), float(duration_min)))

        for hid in rad_ids + dire_ids:
            if hid <= 0:
                continue
            hist = hero_recent.get(hid)
            if hist is None:
                hist = deque(maxlen=hero_recent_window)
                hero_recent[hid] = hist
            hist.append((float(total_match_kills), float(kpm), float(duration_min)))

        if league_id > 0:
            ls = league_stats.get(league_id)
            if ls is None:
                ls = {
                    "count": 0,
                    "total_kills": 0.0,
                    "kpm": 0.0,
                    "duration": 0.0,
                    "over50": 0.0,
                    "under40": 0.0,
                }
                league_stats[league_id] = ls
            ls["count"] += 1
            ls["total_kills"] += float(total_match_kills)
            ls["kpm"] += float(kpm)
            ls["duration"] += float(duration_min)
            ls["over50"] += 1.0 if total_match_kills > 50 else 0.0
            ls["under40"] += 1.0 if total_match_kills < 40 else 0.0

            global_league["count"] += 1
            global_league["total_kills"] += float(total_match_kills)
            global_league["kpm"] += float(kpm)
            global_league["duration"] += float(duration_min)
            global_league["over50"] += 1.0 if total_match_kills > 50 else 0.0
            global_league["under40"] += 1.0 if total_match_kills < 40 else 0.0

        if version_id > 0:
            vs = version_stats.get(version_id)
            if vs is None:
                vs = {
                    "count": 0,
                    "total_kills": 0.0,
                    "kpm": 0.0,
                    "duration": 0.0,
                    "over50": 0.0,
                    "under40": 0.0,
                }
                version_stats[version_id] = vs
            vs["count"] += 1
            vs["total_kills"] += float(total_match_kills)
            vs["kpm"] += float(kpm)
            vs["duration"] += float(duration_min)
            vs["over50"] += 1.0 if total_match_kills > 50 else 0.0
            vs["under40"] += 1.0 if total_match_kills < 40 else 0.0

            global_version["count"] += 1
            global_version["total_kills"] += float(total_match_kills)
            global_version["kpm"] += float(kpm)
            global_version["duration"] += float(duration_min)
            global_version["over50"] += 1.0 if total_match_kills > 50 else 0.0
            global_version["under40"] += 1.0 if total_match_kills < 40 else 0.0

        for hid in rad_ids + dire_ids:
            h = hero_stats.get(hid)
            if h is None:
                h = {
                    "count": 0,
                    "total_kills": 0.0,
                    "kpm": 0.0,
                    "duration": 0.0,
                    "over50": 0.0,
                    "under40": 0.0,
                }
                hero_stats[hid] = h
            h["count"] += 1
            h["total_kills"] += float(total_match_kills)
            h["kpm"] += float(kpm)
            h["duration"] += float(duration_min)
            h["over50"] += 1.0 if total_match_kills > 50 else 0.0
            h["under40"] += 1.0 if total_match_kills < 40 else 0.0

            global_hero["count"] += 1
            global_hero["total_kills"] += float(total_match_kills)
            global_hero["kpm"] += float(kpm)
            global_hero["duration"] += float(duration_min)
            global_hero["over50"] += 1.0 if total_match_kills > 50 else 0.0
            global_hero["under40"] += 1.0 if total_match_kills < 40 else 0.0

        for team_ids in (rad_ids, dire_ids):
            for h1, h2 in combinations(team_ids, 2):
                if h1 <= 0 or h2 <= 0:
                    continue
                key = (h1, h2) if h1 < h2 else (h2, h1)
                hp = hero_pair_stats.get(key)
                if hp is None:
                    hp = {"count": 0, "total_kills": 0.0, "kpm": 0.0, "duration": 0.0}
                    hero_pair_stats[key] = hp
                hp["count"] += 1
                hp["total_kills"] += float(total_match_kills)
                hp["kpm"] += float(kpm)
                hp["duration"] += float(duration_min)

                global_pair["count"] += 1
                global_pair["total_kills"] += float(total_match_kills)
                global_pair["kpm"] += float(kpm)
                global_pair["duration"] += float(duration_min)

        for rh in rad_ids:
            for dh in dire_ids:
                if rh <= 0 or dh <= 0:
                    continue
                key = (rh, dh) if rh < dh else (dh, rh)
                hv = hero_vs_stats.get(key)
                if hv is None:
                    hv = {"count": 0, "total_kills": 0.0, "kpm": 0.0}
                    hero_vs_stats[key] = hv
                hv["count"] += 1
                hv["total_kills"] += float(total_match_kills)
                hv["kpm"] += float(kpm)

                global_vs["count"] += 1
                global_vs["total_kills"] += float(total_match_kills)
                global_vs["kpm"] += float(kpm)

        radiant_win = match.get("didRadiantWin")
        if radiant_team > 0 and dire_team > 0 and radiant_win is not None:
            r_rating = team_elo.get(radiant_team, 1500.0)
            d_rating = team_elo.get(dire_team, 1500.0)
            r_games = team_games.get(radiant_team, 0)
            d_games = team_games.get(dire_team, 0)
            exp_r = elo_expected(r_rating, d_rating)
            score_r = 1.0 if radiant_win else 0.0
            score_d = 1.0 - score_r
            team_elo[radiant_team] = r_rating + elo_k(r_games) * (score_r - exp_r)
            team_elo[dire_team] = d_rating + elo_k(d_games) * (score_d - (1.0 - exp_r))
            team_games[radiant_team] = r_games + 1
            team_games[dire_team] = d_games + 1

    df = pd.DataFrame(records)
    df = df.sort_values("start_time").reset_index(drop=True)
    return df


def split_time(df: pd.DataFrame, cfg: SplitConfig) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if len(df) <= cfg.test_size + cfg.val_size:
        raise ValueError("dataset too small for requested splits")
    train = df.iloc[: -(cfg.test_size + cfg.val_size)].copy()
    val = df.iloc[-(cfg.test_size + cfg.val_size) : -cfg.test_size].copy()
    test = df.iloc[-cfg.test_size :].copy()
    return train, val, test


def prepare_xy(
    df: pd.DataFrame,
    feature_cols: List[str],
    cat_cols: List[str],
) -> Tuple[pd.DataFrame, pd.Series, List[int]]:
    X = df[feature_cols].copy()
    y = df["total_kills"].copy()

    for c in cat_cols:
        if c in X.columns:
            X[c] = X[c].fillna("UNKNOWN").astype(str)

    cat_indices = [X.columns.get_loc(c) for c in cat_cols if c in X.columns]
    return X, y, cat_indices


def train_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    cat_indices: List[int],
    config: Dict[str, Any],
    sample_weight: Optional[np.ndarray] = None,
) -> CatBoostRegressor:
    model = CatBoostRegressor(**config)
    train_pool = Pool(X_train, y_train, cat_features=cat_indices, weight=sample_weight)
    val_pool = Pool(X_val, y_val, cat_features=cat_indices)
    model.fit(train_pool, eval_set=val_pool, use_best_model=True)
    return model


def evaluate(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, Any]:
    mae = mean_absolute_error(y_true, y_pred)
    mask_low = y_true < 40
    mask_high = y_true > 50
    out: Dict[str, Any] = {
        "mae": mae,
        "low_count": int(mask_low.sum()),
        "high_count": int(mask_high.sum()),
        "mae_low": mean_absolute_error(y_true[mask_low], y_pred[mask_low]) if mask_low.sum() else None,
        "mae_high": mean_absolute_error(y_true[mask_high], y_pred[mask_high]) if mask_high.sum() else None,
    }
    return out


def _group_split_sizes(
    n_rows: int,
    base_val: int,
    base_test: int,
    min_train: int = 80,
    min_val: int = 20,
    min_test: int = 20,
) -> Optional[Tuple[int, int]]:
    if n_rows <= min_train + min_val + min_test:
        return None
    val_size = min(base_val, max(min_val, int(n_rows * 0.20)))
    test_size = min(base_test, max(min_test, int(n_rows * 0.15)))
    if n_rows - (val_size + test_size) < min_train:
        max_holdout = n_rows - min_train
        if max_holdout <= 0:
            return None
        val_size = max(min_val, int(max_holdout * 0.6))
        test_size = max(min_test, max_holdout - val_size)
    if n_rows <= val_size + test_size:
        return None
    return val_size, test_size


def _build_patch_major_order() -> Dict[str, int]:
    majors: List[str] = []
    for _, label in _PATCH_SCHEDULE:
        base = label
        while base and base[-1].isalpha():
            base = base[:-1]
        if base and base not in majors:
            majors.append(base)
    return {label: idx for idx, label in enumerate(majors)}


_PATCH_MAJOR_ORDER = _build_patch_major_order()


def _patch_weight(
    label: str,
    focus_label: str,
    decay: float,
    min_weight: float,
    unknown_weight: float,
) -> float:
    if not focus_label:
        return 1.0
    if not label or label == "UNKNOWN":
        return unknown_weight
    order = _PATCH_MAJOR_ORDER.get(label)
    focus_order = _PATCH_MAJOR_ORDER.get(focus_label)
    if order is None or focus_order is None:
        return unknown_weight
    dist = abs(order - focus_order)
    return max(min_weight, decay**dist)


def compute_patch_weights(
    df: pd.DataFrame,
    focus_label: Optional[str],
    decay: float,
    min_weight: float,
    unknown_weight: float,
) -> np.ndarray:
    if not focus_label:
        return np.ones(len(df), dtype=np.float64)
    labels = df.get("patch_major_label")
    if labels is None:
        return np.ones(len(df), dtype=np.float64)
    return np.array(
        [
            _patch_weight(str(label), focus_label, decay, min_weight, unknown_weight)
            for label in labels
        ],
        dtype=np.float64,
    )


def compute_roster_lock_weights(df: pd.DataFrame, unstable_weight: float) -> np.ndarray:
    if unstable_weight >= 0.999:
        return np.ones(len(df), dtype=np.float64)
    if unstable_weight <= 0.0:
        unstable_weight = 0.5

    def _is_stable(val: Any) -> int:
        try:
            v = float(val)
            if math.isnan(v):
                return 0
            return 1 if v >= 1.0 else 0
        except Exception:
            return 0

    r_vals = df.get("radiant_roster_stable_prev")
    d_vals = df.get("dire_roster_stable_prev")
    if r_vals is None or d_vals is None:
        return np.ones(len(df), dtype=np.float64)
    weights = np.ones(len(df), dtype=np.float64)
    for idx, (r, d) in enumerate(zip(r_vals, d_vals)):
        stable_count = _is_stable(r) + _is_stable(d)
        if stable_count >= 2:
            weights[idx] = 1.0
        elif stable_count == 1:
            weights[idx] = (1.0 + unstable_weight) / 2.0
        else:
            weights[idx] = unstable_weight
    return weights


def split_focus_patch(
    df: pd.DataFrame,
    focus_label: str,
    base_val: int,
    base_test: int,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    focus_df = df[df["patch_major_label"] == focus_label].copy()
    if focus_df.empty:
        raise ValueError(f"focus patch {focus_label} has no rows")
    focus_df = focus_df.sort_values("start_time")
    split_sizes = _group_split_sizes(len(focus_df), base_val, base_test)
    if split_sizes is None:
        raise ValueError("focus patch too small for split")
    val_size, test_size = split_sizes
    _, val_focus, test_focus = split_time(
        focus_df, SplitConfig(test_size=test_size, val_size=val_size)
    )
    holdout_idx = set(val_focus.index).union(set(test_focus.index))
    train_df = df.drop(index=holdout_idx, errors="ignore").copy()
    return train_df, val_focus, test_focus


def _is_invalid_group_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, (int, np.integer)) and int(value) <= 0:
        return True
    if isinstance(value, str):
        v = value.strip()
        if not v or v.upper() == "UNKNOWN":
            return True
    return False


def train_group_models(
    df: pd.DataFrame,
    group_col: str,
    group_label: str,
    feature_cols: List[str],
    cat_cols: List[str],
    best_cfg: Dict[str, Any],
    segment: str,
    test_size: int,
    val_size: int,
    save_models: bool,
    models_dir: Path,
) -> None:
    if group_col not in df.columns:
        logger.warning("Group column '%s' missing; skipping %s models", group_col, group_label)
        return

    values = sorted(df[group_col].dropna().unique().tolist())
    for value in values:
        if _is_invalid_group_value(value):
            continue
        group_df = df[df[group_col] == value].copy()
        split_sizes = _group_split_sizes(len(group_df), val_size, test_size)
        if split_sizes is None:
            logger.info("Skip %s=%s: insufficient rows=%d", group_label, value, len(group_df))
            continue
        group_val_size, group_test_size = split_sizes
        group_df = group_df.sort_values("start_time").reset_index(drop=True)
        try:
            train_df, val_df, test_df = split_time(
                group_df, SplitConfig(test_size=group_test_size, val_size=group_val_size)
            )
        except ValueError:
            logger.info("Skip %s=%s: insufficient rows for split", group_label, value)
            continue

        X_train, y_train, cat_indices = prepare_xy(train_df, feature_cols, cat_cols)
        X_val, y_val, _ = prepare_xy(val_df, feature_cols, cat_cols)
        X_test, y_test, _ = prepare_xy(test_df, feature_cols, cat_cols)

        train_mask = np.ones(len(y_train), dtype=bool)
        val_mask = np.ones(len(y_val), dtype=bool)
        test_mask = np.ones(len(y_test), dtype=bool)
        if segment == "low":
            train_mask = y_train < 40
            val_mask = y_val < 40
            test_mask = y_test < 40
        elif segment == "high":
            train_mask = y_train > 50
            val_mask = y_val > 50
            test_mask = y_test > 50

        X_train_seg = X_train[train_mask]
        y_train_seg = y_train[train_mask]
        X_val_seg = X_val[val_mask]
        y_val_seg = y_val[val_mask]
        X_test_seg = X_test[test_mask]
        y_test_seg = y_test[test_mask]

        if len(X_train_seg) < 50 or len(X_val_seg) < 10 or len(X_test_seg) < 10:
            logger.info(
                "Skip %s=%s: segment=%s too small (train=%d val=%d test=%d)",
                group_label,
                value,
                segment,
                len(X_train_seg),
                len(X_val_seg),
                len(X_test_seg),
            )
            continue

        weights = None
        if segment == "all":
            weights = np.ones(len(y_train_seg), dtype=np.float64)
            weights[(y_train_seg < 40) | (y_train_seg > 50)] = 1.8

        model = train_model(
            X_train_seg,
            y_train_seg,
            X_val_seg,
            y_val_seg,
            cat_indices,
            best_cfg,
            sample_weight=weights,
        )
        test_pred = model.predict(X_test_seg)
        metrics = evaluate(y_test_seg.values, test_pred)
        logger.info(
            "Group %s=%s segment=%s | test_mae=%.3f (low=%s high=%s)",
            group_label,
            value,
            segment,
            metrics["mae"],
            metrics["mae_low"],
            metrics["mae_high"],
        )

        if not save_models:
            continue
        if group_label == "patch":
            suffix = f"patch_{patch_label_to_slug(str(value))}"
        else:
            suffix = f"tier_{int(value)}"
        models_dir.mkdir(parents=True, exist_ok=True)
        seg_suffix = "" if segment == "all" else f"_{segment}"
        model_path = models_dir / f"live_cb_kills_reg_{suffix}{seg_suffix}.cbm"
        model.save_model(str(model_path))
        logger.info("Saved group model: %s", model_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean-path", type=str, default=str(DEFAULT_CLEAN_PATH))
    parser.add_argument("--test-size", type=int, default=100)
    parser.add_argument("--val-size", type=int, default=300)
    parser.add_argument("--no-pub-priors", action="store_true")
    parser.add_argument("--segment", choices=["all", "low", "high"], default="all")
    parser.add_argument("--save-model", action="store_true")
    parser.add_argument("--train-quantiles", action="store_true")
    parser.add_argument("--use-selected", action="store_true")
    parser.add_argument("--selected-features-path", type=str, default=str(SELECTED_FEATURES_PATH))
    parser.add_argument("--drop-networth", action="store_true")
    parser.add_argument("--fast", action="store_true", help="Use a smaller, faster config grid")
    parser.add_argument("--by-patch", action="store_true", help="Train per-patch major models")
    parser.add_argument("--by-tier", action="store_true", help="Train per-tier models")
    parser.add_argument("--focus-patch", type=str, default=None)
    parser.add_argument("--patch-weight-decay", type=float, default=0.6)
    parser.add_argument("--patch-weight-min", type=float, default=0.25)
    parser.add_argument("--patch-weight-unknown", type=float, default=0.2)
    parser.add_argument("--roster-lock-weight", type=float, default=0.7)
    parser.add_argument("--save-focus-as-patch", action="store_true")
    args = parser.parse_args()

    clean_path = Path(args.clean_path)
    logger.info("Loading matches: %s", clean_path)
    matches = load_clean_data(clean_path)

    pub_priors = {}
    if not args.no_pub_priors:
        pub_priors = build_pub_hero_priors(PUB_PLAYERS_DIR, PUB_PRIORS_PATH)

    logger.info("Building dataset (time-aware features)...")
    df = build_dataset(matches, pub_priors)
    logger.info("Dataset rows: %d", len(df))

    if args.focus_patch:
        try:
            train_df, val_df, test_df = split_focus_patch(
                df, args.focus_patch, args.val_size, args.test_size
            )
            logger.info(
                "Focus patch=%s split sizes: train=%d val=%d test=%d",
                args.focus_patch,
                len(train_df),
                len(val_df),
                len(test_df),
            )
        except Exception as e:
            logger.warning("Focus patch split failed (%s); falling back to standard split", e)
            train_df, val_df, test_df = split_time(
                df, SplitConfig(test_size=args.test_size, val_size=args.val_size)
            )
            logger.info(
                "Split sizes: train=%d val=%d test=%d", len(train_df), len(val_df), len(test_df)
            )
    else:
        train_df, val_df, test_df = split_time(
            df, SplitConfig(test_size=args.test_size, val_size=args.val_size)
        )
        logger.info("Split sizes: train=%d val=%d test=%d", len(train_df), len(val_df), len(test_df))

    feature_cols = [c for c in df.columns if c not in ("total_kills", "start_time")]
    feature_cols = select_feature_cols(feature_cols, args.use_selected, Path(args.selected_features_path))
    if args.drop_networth:
        feature_cols = drop_networth_features(feature_cols)

    cat_cols = [
        c
        for c in feature_cols
        if c.startswith("radiant_hero_")
        or c.startswith("dire_hero_")
        or c.startswith("radiant_player_")
        or c.startswith("dire_player_")
        or c.endswith("_team_id")
        or c in (
            "league_id",
            "patch_id",
            "patch_major_label",
            "game_version_id",
            "series_type",
            "tournament_round",
            "lobby_type",
            "region_id",
            "rank",
            "bracket",
            "bottom_lane_outcome",
            "mid_lane_outcome",
            "top_lane_outcome",
        )
    ]

    X_train, y_train, cat_indices = prepare_xy(train_df, feature_cols, cat_cols)
    X_val, y_val, _ = prepare_xy(val_df, feature_cols, cat_cols)
    X_test, y_test, _ = prepare_xy(test_df, feature_cols, cat_cols)

    train_mask = np.ones(len(y_train), dtype=bool)
    val_mask = np.ones(len(y_val), dtype=bool)
    test_mask = np.ones(len(y_test), dtype=bool)
    if args.segment == "low":
        train_mask = y_train < 40
        val_mask = y_val < 40
        test_mask = y_test < 40
    elif args.segment == "high":
        train_mask = y_train > 50
        val_mask = y_val > 50
        test_mask = y_test > 50

    X_train_seg = X_train[train_mask]
    y_train_seg = y_train[train_mask]
    X_val_seg = X_val[val_mask]
    y_val_seg = y_val[val_mask]

    weights = None
    if args.focus_patch or args.segment == "all":
        base_weights = np.ones(len(y_train), dtype=np.float64)
        if args.focus_patch:
            base_weights *= compute_patch_weights(
                train_df,
                args.focus_patch,
                args.patch_weight_decay,
                args.patch_weight_min,
                args.patch_weight_unknown,
            )
            base_weights *= compute_roster_lock_weights(train_df, args.roster_lock_weight)
        if args.segment == "all":
            base_weights[(y_train < 40) | (y_train > 50)] *= 1.8
        weights = base_weights[train_mask]

    if args.fast:
        configs = [
            dict(
                iterations=1200,
                depth=7,
                learning_rate=0.06,
                loss_function="MAE",
                eval_metric="MAE",
                random_seed=42,
                early_stopping_rounds=150,
                verbose=False,
            )
        ]
    else:
        configs = [
            dict(
                iterations=3000,
                depth=8,
                learning_rate=0.04,
                loss_function="MAE",
                eval_metric="MAE",
                random_seed=42,
                early_stopping_rounds=200,
                verbose=False,
            ),
            dict(
                iterations=3500,
                depth=9,
                learning_rate=0.035,
                loss_function="MAE",
                eval_metric="MAE",
                random_seed=42,
                early_stopping_rounds=200,
                verbose=False,
            ),
            dict(
                iterations=2500,
                depth=7,
                learning_rate=0.05,
                loss_function="MAE",
                eval_metric="MAE",
                random_seed=42,
                early_stopping_rounds=200,
                verbose=False,
            ),
        ]

    best_model = None
    best_val = math.inf
    best_cfg = None

    for cfg in configs:
        model = train_model(
            X_train_seg, y_train_seg, X_val_seg, y_val_seg, cat_indices, cfg, sample_weight=weights
        )
        val_pred = model.predict(X_val_seg)
        val_mae = mean_absolute_error(y_val_seg, val_pred)
        logger.info("Val MAE: %.3f | cfg=%s", val_mae, cfg)
        if val_mae < best_val:
            best_val = val_mae
            best_model = model
            best_cfg = cfg

    if best_model is None:
        raise RuntimeError("no model trained")

    test_pred = best_model.predict(X_test)
    metrics = evaluate(y_test.values, test_pred)
    logger.info("Test MAE: %.3f (low=%s, high=%s)", metrics["mae"], metrics["mae_low"], metrics["mae_high"])
    logger.info("Low/High counts: low=%d high=%d", metrics["low_count"], metrics["high_count"])

    if args.segment != "all":
        if test_mask.sum():
            seg_mae = mean_absolute_error(y_test[test_mask], test_pred[test_mask])
            logger.info("Segment=%s MAE: %.3f (count=%d)", args.segment, seg_mae, int(test_mask.sum()))
        else:
            logger.info("Segment=%s has no test samples", args.segment)

    train_quantiles = args.train_quantiles or args.save_model
    quantile_models = {}
    if train_quantiles and args.segment == "all":
        for alpha, name in [(0.1, "q10"), (0.9, "q90")]:
            q_cfg = dict(best_cfg)
            q_cfg["loss_function"] = f"Quantile:alpha={alpha}"
            q_cfg["eval_metric"] = f"Quantile:alpha={alpha}"
            q_model = train_model(
                X_train, y_train, X_val, y_val, cat_indices, q_cfg, sample_weight=weights
            )
            quantile_models[name] = q_model
            q_pred = q_model.predict(X_val)
            q_mae = mean_absolute_error(y_val, q_pred)
            logger.info("Quantile %s Val MAE: %.3f", name, q_mae)

    if args.save_model:
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        suffix = "" if args.segment == "all" else f"_{args.segment}"
        model_path = MODELS_DIR / f"live_cb_kills_reg{suffix}.cbm"
        meta_path = MODELS_DIR / f"live_cb_kills_reg{suffix}_meta.json"
        best_model.save_model(str(model_path))

        if quantile_models:
            if "q10" in quantile_models:
                quantile_models["q10"].save_model(str(MODELS_DIR / "live_cb_kills_reg_q10.cbm"))
            if "q90" in quantile_models:
                quantile_models["q90"].save_model(str(MODELS_DIR / "live_cb_kills_reg_q90.cbm"))

        meta = {
            "feature_cols": feature_cols,
            "cat_features": cat_cols,
            "cat_indices": cat_indices,
            "segment": args.segment,
            "train_size": len(train_df),
            "val_size": len(val_df),
            "test_size": len(test_df),
            "test_start_time_min": int(test_df["start_time"].min()),
            "test_start_time_max": int(test_df["start_time"].max()),
            "val_best_mae": float(best_val),
            "config": best_cfg,
            "use_selected": bool(args.use_selected),
            "focus_patch": args.focus_patch,
            "patch_weight_decay": args.patch_weight_decay if args.focus_patch else None,
            "patch_weight_min": args.patch_weight_min if args.focus_patch else None,
            "patch_weight_unknown": args.patch_weight_unknown if args.focus_patch else None,
            "roster_lock_weight": args.roster_lock_weight if args.focus_patch else None,
        }
        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        logger.info("Saved model: %s", model_path)
        logger.info("Saved meta: %s", meta_path)

        if args.focus_patch and args.save_focus_as_patch:
            focus_suffix = patch_label_to_slug(args.focus_patch)
            focus_path = MODELS_DIR / f"live_cb_kills_reg_patch_{focus_suffix}{suffix}.cbm"
            best_model.save_model(str(focus_path))
            logger.info("Saved focus patch model: %s", focus_path)

    if args.by_patch or args.by_tier:
        if args.by_patch:
            train_group_models(
                df,
                "patch_major_label",
                "patch",
                feature_cols,
                cat_cols,
                best_cfg,
                args.segment,
                args.test_size,
                args.val_size,
                args.save_model,
                MODELS_DIR,
            )
        if args.by_tier:
            tier_df = df
            if "match_tier_known" in df.columns:
                tier_df = df[df["match_tier_known"] == 1].copy()
            train_group_models(
                tier_df,
                "match_tier",
                "tier",
                feature_cols,
                cat_cols,
                best_cfg,
                args.segment,
                args.test_size,
                args.val_size,
                args.save_model,
                MODELS_DIR,
            )


if __name__ == "__main__":
    main()
