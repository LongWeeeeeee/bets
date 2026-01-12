"""
Live Predictor - модуль для предсказаний в реальном времени.

Интегрируется с cyberscore_try.py для предсказания:
- Total Kills Over/Under
- Winner (Radiant/Dire)
- Duration (Over/Under Time)

Использует обученные модели из ultimate_inference.
"""

import json
import logging
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Import team mapping utility
try:
    from utils.map_teams_v2 import (
        TEAM_NAME_TO_ID,
        get_match_tier_info,
        get_team_id,
        get_team_tier,
        is_tier_two_team,
    )

    TEAM_MAPPING_AVAILABLE = True
except ImportError:
    TEAM_MAPPING_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Путь к данным
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
CONFIG_DIR = BASE_DIR / "ml-models"
MODELS_DIR = BASE_DIR / "ml-models"

# CatBoost availability flag
CATBOOST_AVAILABLE = False
try:
    from catboost import CatBoostClassifier, CatBoostRegressor, Pool

    CATBOOST_AVAILABLE = True
except ImportError:
    pass

# XGBoost availability flag
XGBOOST_AVAILABLE = False
try:
    import xgboost as xgb

    XGBOOST_AVAILABLE = True
except ImportError:
    pass

# Patch schedule used for time-aware features (UTC dates from patch notes)
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


def _parse_patch_label(label: str) -> Tuple[int, int, int]:
    if not label:
        return 0, 0, 0
    parts = label.strip().split(".", 1)
    if len(parts) != 2:
        return 0, 0, 0
    try:
        major = int(parts[0])
    except ValueError:
        major = 0
    minor_digits = ""
    suffix = ""
    for ch in parts[1]:
        if ch.isdigit():
            minor_digits += ch
        else:
            suffix += ch
    minor = int(minor_digits) if minor_digits else 0
    sub = 0
    if suffix:
        letter = suffix[0].lower()
        if "a" <= letter <= "z":
            sub = ord(letter) - ord("a") + 1
    return major, minor, sub


def _build_patch_schedule() -> Tuple[List[Dict[str, Any]], List[int]]:
    schedule: List[Dict[str, Any]] = []
    for idx, (date_str, label) in enumerate(_PATCH_SCHEDULE):
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            ts = int(dt.timestamp())
        except Exception:
            ts = 0
        major, minor, sub = _parse_patch_label(label)
        schedule.append(
            {
                "patch_id": idx,
                "label": label,
                "ts": ts,
                "major": major,
                "minor": minor,
                "sub": sub,
            }
        )
    schedule = [s for s in schedule if s["ts"] > 0]
    schedule.sort(key=lambda s: s["ts"])
    return schedule, [s["ts"] for s in schedule]


_PATCH_SCHEDULE_INFO, _PATCH_SCHEDULE_TS = _build_patch_schedule()

# ============ ENHANCED EVASIVENESS/CATCH SCORES ============
HARD_MOBILITY_HEROES: Dict[int, float] = {
    1: 3.0,
    39: 3.0,
    13: 3.0,
    17: 3.0,
    106: 3.0,
    126: 3.0,
    10: 2.5,
    41: 2.5,
    120: 2.5,
    93: 2.0,
    32: 2.0,
}
RESET_HEROES: Dict[int, float] = {
    89: 5.0,
    63: 5.0,
    102: 4.0,
    111: 4.0,
    50: 4.0,
    112: 3.5,
    57: 3.0,
    79: 3.0,
    76: 3.0,
    34: 3.0,
}
ILLUSION_HEROES: Dict[int, float] = {
    89: 2.0,
    12: 2.0,
    109: 2.0,
    94: 1.5,
    74: 1.5,
    43: 1.5,
}
INVIS_HEROES: Dict[int, float] = {
    32: 1.5,
    62: 1.5,
    63: 1.5,
    56: 1.5,
    73: 1.5,
    9: 1.0,
    119: 1.0,
}
INSTANT_HEX_HEROES: Dict[int, float] = {26: 3.0, 45: 3.0}
AOE_LOCKDOWN_HEROES: Dict[int, float] = {
    33: 4.0,
    41: 4.0,
    97: 4.0,
    29: 3.5,
    110: 3.5,
    112: 3.5,
    86: 3.0,
    87: 3.0,
    25: 3.0,
}
SINGLE_DISABLE_HEROES: Dict[int, float] = {
    3: 2.5,
    38: 2.5,
    65: 2.5,
    14: 2.0,
    69: 2.0,
    2: 2.0,
    137: 2.0,
}
SILENCE_HEROES: Dict[int, float] = {
    14: 2.0,
    86: 1.5,
    69: 1.5,
    75: 1.5,
    101: 1.5,
    73: 1.5,
}

# ============ SAVE HEROES (can prevent ally deaths) ============
SAVE_HEROES: Dict[int, float] = {
    # Strong saves (can completely prevent death)
    50: 5.0,  # Dazzle - Shallow Grave
    102: 5.0,  # Abaddon - Borrowed Time, Aphotic Shield
    111: 5.0,  # Oracle - False Promise, Fate's Edict
    112: 4.5,  # Winter Wyvern - Cold Embrace
    91: 4.5,  # Io/Wisp - Relocate save
    79: 4.0,  # Shadow Demon - Disruption
    76: 4.0,  # Outworld Destroyer - Astral Imprisonment
    57: 4.0,  # Omniknight - Guardian Angel, Heavenly Grace
    # Medium saves
    100: 3.5,  # Tusk - Snowball
    110: 3.5,  # Phoenix - Supernova
    20: 3.5,  # Vengeful Spirit - Swap save
    14: 3.0,  # Pudge - Hook save
    129: 3.0,  # Marci - Rebound save
    3: 3.0,  # Bane - Nightmare save
    145: 3.0,  # Ringmaster - Escape Act
    37: 2.5,  # Necrophos - Ghost Shroud (self)
    # Soft saves (displacement/protection)
    31: 2.5,  # Lich - Sinister Gaze pull, Frost Shield
    85: 2.0,  # Undying - Tombstone zone, Decay sustain
    75: 2.0,  # Silencer - Global Silence interrupt
    54: 2.0,  # Lifestealer - Infest (self)
    41: 2.0,  # Faceless Void - Chrono save
    126: 2.0,  # Void Spirit - Dissimilate (self)
    63: 2.0,  # Weaver - Time Lapse (self), Aghs ally
    12: 1.5,  # Phantom Lancer - Doppelganger (self)
    89: 1.5,  # Naga Siren - Song of the Siren
    35: 1.5,  # Sniper - Concussive Grenade knockback
    77: 1.5,  # Lycan - Howl MS boost
}


class LivePredictor:
    """
    Предсказатель для live матчей.

    Загружает обученные модели и делает предсказания на основе драфта.
    """

    def __init__(self) -> None:
        self.models_loaded = False

        # Static data
        self.heroes: Dict = {}
        self.hero_roles: Dict = {}
        self.blood_stats: Dict = {}
        self.hero_synergy: Dict = {}
        self.hero_cc_stats: Dict = {}
        self.hero_power_spikes: Dict = {}
        self.hero_greed_index: Dict = {}
        self.hero_push_stats: Dict = {}
        self.hero_healing_stats: Dict = {}
        self.hero_wave_clear: Dict = {}
        self.complex_hero_stats: Dict = {}
        self.player_dna: Dict = {}
        self.team_ratings: Dict = {}
        self.team_name_map: Dict[str, int] = {}
        self.hero_features: Dict = {}  # From Stratz API (hero_features_processed.json)
        self.hero_lane_matchups: Dict = {}  # Lane matchup data
        self.hero_comeback_stats: Dict = {}  # Comeback/stomp stats
        self.early_late_counters: Dict = {}  # Early/late counters from pubs
        self.draft_execution_stats: Dict = {}  # Draft execution by roster

        # Selected features
        self.selected_features: List[str] = []

        # BK Line (median from training)
        self.bk_line: float = 47.0

        # Models (LightGBM/XGBoost - legacy)
        self.lgbm_model = None
        self.xgb_model = None

        # CatBoost models (new)
        self.cb_kills: Optional[Any] = None
        self.cb_winner: Optional[Any] = None
        self.cb_duration: Optional[Any] = None
        self.cb_kpm: Optional[Any] = None
        self.cb_feature_cols: List[str] = []
        self.cb_cat_features: List[str] = []
        self.cb_cat_indices: List[int] = []
        self.use_catboost: bool = False

        # CatBoost regression model for Total Kills (MAE optimized)
        self.cb_kills_reg: Optional[Any] = None
        self.cb_kills_reg_feature_cols: List[str] = []
        self.cb_kills_reg_cat_features: List[str] = []
        self.cb_kills_reg_cat_indices: List[int] = []
        self.use_kills_regression: bool = False

        # Meta model for kills probability (stacking with duration/kpm)
        self.kills_meta_model: Optional[Any] = None
        self.kills_meta_feature_cols: List[str] = []
        self.use_kills_meta: bool = False

        # Kills probability calibrator
        self.kills_calibrator: Optional[Any] = None
        self.kills_cal_feature_cols: List[str] = []
        self.use_kills_calibrator: bool = False

        # Quantile regression for kills distribution
        self.cb_kills_q10: Optional[Any] = None
        self.cb_kills_q90: Optional[Any] = None
        self.use_kills_quantiles: bool = False

        # XGBoost models (ensemble)
        self.xgb_kills: Optional[Any] = None
        self.xgb_winner: Optional[Any] = None
        self.xgb_feature_cols: List[str] = []
        self.use_xgboost: bool = False

        # Extreme classifiers (LOW <38, HIGH >52) - SEPARATE feature sets
        self.extreme_low_model: Optional[Any] = None
        self.extreme_high_model: Optional[Any] = None
        # LOW model features
        self.extreme_low_feature_cols: List[str] = []
        self.extreme_low_cat_features: List[str] = []
        self.extreme_low_cat_indices: List[int] = []
        # HIGH model features
        self.extreme_high_feature_cols: List[str] = []
        self.extreme_high_cat_features: List[str] = []
        self.extreme_high_cat_indices: List[int] = []
        # Legacy (for backward compatibility)
        self.extreme_feature_cols: List[str] = []
        self.extreme_cat_features: List[str] = []
        self.extreme_cat_indices: List[int] = []
        # Thresholds
        self.extreme_low_threshold: int = 38
        self.extreme_high_threshold: int = 52
        self.use_extreme_classifier: bool = False
        self.use_separate_extreme_features: bool = False

        # In-game models (use live data from first N minutes)
        self.ingame_models: Dict[
            int, Dict[str, Any]
        ] = {}  # minute -> {low_model, high_model, meta}
        self.use_ingame_model: bool = False

        # Winrate classifier (radiant_win prediction)
        self.winrate_model: Optional[Any] = None
        self.winrate_feature_cols: List[str] = []
        self.winrate_cat_features: List[str] = []
        self.winrate_cat_indices: List[int] = []
        self.use_winrate_classifier: bool = False
        self.winrate_include_team_ids: bool = True
        self.winrate_include_rolling_dna: bool = False
        self.winrate_default_threshold: float = 0.60
        self.winrate_min_threshold: float = 0.55
        self.winrate_model_tag: str = ""

        self._load_static_data()
        self._load_models()

    def _load_static_data(self) -> None:
        """Загружает статические данные."""
        try:
            # Heroes mapping
            heroes_path = DATA_DIR / "heroes.json"
            if heroes_path.exists():
                with open(heroes_path, "r") as f:
                    self.heroes = json.load(f)

            # Hero roles
            hero_roles_path = DATA_DIR / "hero_roles.json"
            if hero_roles_path.exists():
                with open(hero_roles_path, "r") as f:
                    self.hero_roles = json.load(f)

            # Blood stats
            blood_stats_path = DATA_DIR / "blood_stats.json"
            if blood_stats_path.exists():
                with open(blood_stats_path, "r") as f:
                    self.blood_stats = json.load(f)

            # Hero synergy
            synergy_path = DATA_DIR / "hero_synergy.json"
            if synergy_path.exists():
                with open(synergy_path, "r") as f:
                    self.hero_synergy = json.load(f)

            # Hero CC stats
            cc_path = DATA_DIR / "hero_cc_stats.json"
            if cc_path.exists():
                with open(cc_path, "r") as f:
                    self.hero_cc_stats = json.load(f)

            # Hero power spikes
            spikes_path = DATA_DIR / "hero_power_spikes.json"
            if spikes_path.exists():
                with open(spikes_path, "r") as f:
                    self.hero_power_spikes = json.load(f)

            # Hero greed index
            greed_path = DATA_DIR / "hero_greed_index.json"
            if greed_path.exists():
                with open(greed_path, "r") as f:
                    self.hero_greed_index = json.load(f)

            # Hero push stats
            push_path = DATA_DIR / "hero_push_stats.json"
            if push_path.exists():
                with open(push_path, "r") as f:
                    self.hero_push_stats = json.load(f)

            # Hero healing stats
            heal_path = DATA_DIR / "hero_healing_stats.json"
            if heal_path.exists():
                with open(heal_path, "r") as f:
                    self.hero_healing_stats = json.load(f)

            # Hero wave clear
            wave_path = DATA_DIR / "hero_wave_clear.json"
            if wave_path.exists():
                with open(wave_path, "r") as f:
                    self.hero_wave_clear = json.load(f)

            # Complex hero stats
            complex_path = DATA_DIR / "complex_hero_stats.json"
            if complex_path.exists():
                with open(complex_path, "r") as f:
                    self.complex_hero_stats = json.load(f)

            # Player DNA
            dna_path = DATA_DIR / "player_dna.json"
            if dna_path.exists():
                with open(dna_path, "r") as f:
                    self.player_dna = json.load(f)

            # Team ratings (Glicko-2)
            ratings_path = DATA_DIR / "team_ratings.json"
            if ratings_path.exists():
                with open(ratings_path, "r") as f:
                    self.team_ratings = json.load(f)

            # Team name to ID mapping
            team_map_path = DATA_DIR / "team_name_map.json"
            if team_map_path.exists():
                with open(team_map_path, "r") as f:
                    self.team_name_map = json.load(f)

            # Hero features from Stratz API (roles, abilities, matchups)
            hero_features_path = DATA_DIR / "hero_features_processed.json"
            if hero_features_path.exists():
                with open(hero_features_path, "r") as f:
                    self.hero_features = json.load(f)
                logger.info(
                    f"Loaded hero_features_processed: {len(self.hero_features)} heroes"
                )

            # Lane matchups (hero vs hero lane performance)
            lane_matchups_path = DATA_DIR / "hero_lane_matchups.json"
            if lane_matchups_path.exists():
                with open(lane_matchups_path, "r") as f:
                    self.hero_lane_matchups = json.load(f)
                logger.info(
                    f"Loaded hero_lane_matchups: {len(self.hero_lane_matchups)} matchups"
                )

            # Comeback stats (hero comeback/stomp potential)
            comeback_path = DATA_DIR / "hero_comeback_stats.json"
            if comeback_path.exists():
                with open(comeback_path, "r") as f:
                    self.hero_comeback_stats = json.load(f)
                logger.info(
                    f"Loaded hero_comeback_stats: {len(self.hero_comeback_stats)} heroes"
                )

            # Early/late counters from public matches
            el_counters_path = DATA_DIR / "early_late_counters.json"
            if el_counters_path.exists():
                with open(el_counters_path, "r") as f:
                    self.early_late_counters = json.load(f)
                early_1v1 = len(
                    self.early_late_counters.get("early", {}).get("counter_1v1", {})
                )
                late_1v1 = len(
                    self.early_late_counters.get("late", {}).get("counter_1v1", {})
                )
                logger.info(
                    f"Loaded early_late_counters: early_1v1={early_1v1}, late_1v1={late_1v1}"
                )

            # Draft execution stats (how rosters perform with draft adv/disadv)
            exec_path = DATA_DIR / "draft_execution_stats.json"
            if exec_path.exists():
                with open(exec_path, "r") as f:
                    self.draft_execution_stats = json.load(f)
                logger.info(
                    f"Loaded draft_execution_stats: {len(self.draft_execution_stats)} rosters"
                )

            # Selected features
            features_path = CONFIG_DIR / "selected_features.json"
            if features_path.exists():
                with open(features_path, "r") as f:
                    data = json.load(f)
                    self.selected_features = data.get("selected_features", [])

            hero_blood_count = len(self.blood_stats.get("hero_blood", {}))
            logger.info(
                f"Loaded static data: {len(self.heroes)} heroes, "
                f"{hero_blood_count} hero blood stats, "
                f"{len(self.selected_features)} features"
            )

        except Exception as e:
            logger.error(f"Error loading static data: {e}")

    def _load_models(self) -> None:
        """Загружает обученные модели для live предсказаний."""
        # Try CatBoost first (preferred) for the main live (kills/winner/duration/kpm) stack.
        if self._load_catboost_models():
            self.use_catboost = True
            self.models_loaded = True
            logger.info("Using CatBoost models for predictions")

            # Also try to load XGBoost for ensemble
            self._load_xgboost_ensemble()
        else:
            # Fallback to LightGBM/XGBoost for the main stack
            self._load_legacy_models()

        # IMPORTANT:
        # Winrate/extreme/ingame models should be available regardless of whether the main stack
        # runs on CatBoost or legacy boosters. This avoids offline/online skew where these
        # models silently aren't loaded just because CatBoost live stack isn't active.
        #
        # These loaders are safe to call multiple times; they no-op if models are unavailable.
        try:
            self._load_winrate_classifier()
        except Exception as e:
            logger.warning(f"Winrate classifier load failed (non-fatal): {e}")

        try:
            self._load_extreme_classifiers()
        except Exception as e:
            logger.warning(f"Extreme classifiers load failed (non-fatal): {e}")

        try:
            self._load_ingame_models()
        except Exception as e:
            logger.warning(f"In-game models load failed (non-fatal): {e}")

    def _load_xgboost_ensemble(self) -> bool:
        """Load XGBoost models for ensemble with CatBoost."""
        if not XGBOOST_AVAILABLE:
            logger.info("XGBoost not available for ensemble")
            return False

        try:
            # Load metadata
            meta_path = MODELS_DIR / "live_xgb_meta.json"
            if not meta_path.exists():
                logger.info("XGBoost ensemble metadata not found")
                return False

            with open(meta_path, "r") as f:
                meta = json.load(f)
                self.xgb_feature_cols = meta.get("feature_cols", [])

            # Load kills classifier
            kills_path = MODELS_DIR / "live_xgb_kills.json"
            if kills_path.exists():
                self.xgb_kills = xgb.XGBClassifier()
                self.xgb_kills.load_model(str(kills_path))
                logger.info(
                    f"Loaded XGBoost kills model for ensemble ({len(self.xgb_feature_cols)} features)"
                )
                self.use_xgboost = True

            # Load winner classifier
            winner_path = MODELS_DIR / "live_xgb_winner.json"
            if winner_path.exists():
                self.xgb_winner = xgb.XGBClassifier()
                self.xgb_winner.load_model(str(winner_path))
                logger.info("Loaded XGBoost winner model for ensemble")

            return self.use_xgboost

        except Exception as e:
            logger.error(f"Error loading XGBoost ensemble: {e}")
            return False

    def _load_catboost_models(self) -> bool:
        """Загружает CatBoost модели. Returns True if successful."""
        if not CATBOOST_AVAILABLE:
            logger.info("CatBoost not available, skipping CatBoost models")
            return False

        try:
            # Load metadata
            meta_path = MODELS_DIR / "live_cb_meta.json"
            if not meta_path.exists():
                logger.info("CatBoost metadata not found")
                return False

            with open(meta_path, "r") as f:
                meta = json.load(f)
                self.cb_feature_cols = meta.get("feature_cols", [])
                self.cb_cat_features = meta.get("cat_features", [])
                self.cb_cat_indices = meta.get("cat_indices", [])
                self.bk_line = meta.get("bk_line", 47.0)

            logger.info(
                f"CatBoost metadata: {len(self.cb_feature_cols)} features, "
                f"{len(self.cb_cat_features)} categorical, BK line={self.bk_line}"
            )

            # Load kills classifier
            kills_path = MODELS_DIR / "live_cb_kills.cbm"
            if kills_path.exists():
                self.cb_kills = CatBoostClassifier()
                self.cb_kills.load_model(str(kills_path))
                logger.info("Loaded CatBoost kills model")
            else:
                return False

            # Load winner classifier
            winner_path = MODELS_DIR / "live_cb_winner.cbm"
            if winner_path.exists():
                self.cb_winner = CatBoostClassifier()
                self.cb_winner.load_model(str(winner_path))
                logger.info("Loaded CatBoost winner model")

            # Load duration regressor
            duration_path = MODELS_DIR / "live_cb_duration.cbm"
            if duration_path.exists():
                self.cb_duration = CatBoostRegressor()
                self.cb_duration.load_model(str(duration_path))
                logger.info("Loaded CatBoost duration model")

            # Load KPM regressor
            kpm_path = MODELS_DIR / "live_cb_kpm.cbm"
            if kpm_path.exists():
                self.cb_kpm = CatBoostRegressor()
                self.cb_kpm.load_model(str(kpm_path))
                logger.info("Loaded CatBoost KPM model")

            # Load kills regression model (MAE optimized)
            self._load_kills_regression_model()
            # Load kills meta model (stacking with duration/kpm)
            self._load_kills_meta_model()
            # Load kills probability calibrator
            self._load_kills_calibrator()

            # Load extreme classifiers
            self._load_extreme_classifiers()

            # Load in-game models
            self._load_ingame_models()

            # Load winrate classifier
            self._load_winrate_classifier()

            return True

        except Exception as e:
            logger.error(f"Error loading CatBoost models: {e}")
            return False

    def _load_kills_regression_model(self) -> bool:
        """Load CatBoost regression model for Total Kills (MAE optimized)."""
        try:
            meta_path = MODELS_DIR / "live_cb_kills_reg_meta.json"
            if not meta_path.exists():
                logger.info("Kills regression metadata not found")
                return False

            with open(meta_path, "r") as f:
                meta = json.load(f)
                self.cb_kills_reg_feature_cols = meta.get("feature_cols", [])
                self.cb_kills_reg_cat_features = meta.get("cat_features", [])
                self.cb_kills_reg_cat_indices = meta.get("cat_indices", [])

            loaded = False
            model_path = MODELS_DIR / "live_cb_kills_reg.cbm"
            if model_path.exists():
                self.cb_kills_reg = CatBoostRegressor()
                self.cb_kills_reg.load_model(str(model_path))
                self.use_kills_regression = True
                loaded = True
                logger.info(
                    f"Loaded CatBoost kills regression model ({len(self.cb_kills_reg_feature_cols)} features)"
                )

            # Load quantile models if available
            q10_path = MODELS_DIR / "live_cb_kills_reg_q10.cbm"
            q90_path = MODELS_DIR / "live_cb_kills_reg_q90.cbm"
            if q10_path.exists() and q90_path.exists():
                self.cb_kills_q10 = CatBoostRegressor()
                self.cb_kills_q90 = CatBoostRegressor()
                self.cb_kills_q10.load_model(str(q10_path))
                self.cb_kills_q90.load_model(str(q90_path))
                self.use_kills_quantiles = True
                logger.info("Loaded kills quantile models (q10/q90)")

            return loaded
        except Exception as e:
            logger.error(f"Error loading kills regression model: {e}")
            return False

    def _load_kills_meta_model(self) -> bool:
        """Load meta model to calibrate kills probability."""
        meta_path = MODELS_DIR / "live_kills_meta.json"
        model_path = MODELS_DIR / "live_kills_meta.pkl"
        if not meta_path.exists() or not model_path.exists():
            logger.info("Kills meta model not found")
            return False
        try:
            with open(meta_path, "r") as f:
                meta = json.load(f)
                self.kills_meta_feature_cols = meta.get("feature_cols", [])
            with open(model_path, "rb") as f:
                self.kills_meta_model = pickle.load(f)
            self.use_kills_meta = True
            logger.info(
                f"Loaded kills meta model ({len(self.kills_meta_feature_cols)} features)"
            )
            return True
        except Exception as e:
            logger.error(f"Error loading kills meta model: {e}")
            return False

    def _load_kills_calibrator(self) -> bool:
        """Load calibrator for kills probability."""
        meta_path = MODELS_DIR / "live_kills_calibrator.json"
        model_path = MODELS_DIR / "live_kills_calibrator.pkl"
        if not meta_path.exists() or not model_path.exists():
            logger.info("Kills calibrator not found")
            return False
        try:
            with open(meta_path, "r") as f:
                meta = json.load(f)
                self.kills_cal_feature_cols = meta.get("feature_cols", [])
            with open(model_path, "rb") as f:
                self.kills_calibrator = pickle.load(f)
            self.use_kills_calibrator = True
            logger.info(
                f"Loaded kills calibrator ({len(self.kills_cal_feature_cols)} features)"
            )
            return True
        except Exception as e:
            logger.error(f"Error loading kills calibrator: {e}")
            return False

    def _load_extreme_classifiers(self) -> bool:
        """Load extreme kills classifiers (LOW <38, HIGH >52) with separate feature sets."""
        if not CATBOOST_AVAILABLE:
            return False

        try:
            meta_path = MODELS_DIR / "extreme_classifier_meta_v2.json"
            if not meta_path.exists():
                logger.info("Extreme classifier v2 metadata not found")
                return False

            with open(meta_path, "r") as f:
                meta = json.load(f)

                # Check for new format (separate feature sets)
                if "low_feature_cols" in meta and "high_feature_cols" in meta:
                    # New format: separate features for each model
                    self.extreme_low_feature_cols = meta.get("low_feature_cols", [])
                    self.extreme_low_cat_features = meta.get("low_cat_features", [])
                    self.extreme_low_cat_indices = meta.get("low_cat_indices", [])

                    self.extreme_high_feature_cols = meta.get("high_feature_cols", [])
                    self.extreme_high_cat_features = meta.get("high_cat_features", [])
                    self.extreme_high_cat_indices = meta.get("high_cat_indices", [])

                    self.use_separate_extreme_features = True
                    logger.info(
                        f"Using separate feature sets: LOW={len(self.extreme_low_feature_cols)}, HIGH={len(self.extreme_high_feature_cols)}"
                    )
                else:
                    # Legacy format: shared features
                    self.extreme_feature_cols = meta.get("feature_cols", [])
                    self.extreme_cat_features = meta.get("cat_features", [])
                    self.extreme_cat_indices = meta.get("cat_indices", [])
                    self.use_separate_extreme_features = False

                self.extreme_low_threshold = meta.get("low_threshold", 38)
                self.extreme_high_threshold = meta.get("high_threshold", 52)

            low_path = MODELS_DIR / "extreme_low_classifier_v2.cbm"
            high_path = MODELS_DIR / "extreme_high_classifier_v2.cbm"

            if low_path.exists() and high_path.exists():
                self.extreme_low_model = CatBoostClassifier()
                self.extreme_low_model.load_model(str(low_path))

                self.extreme_high_model = CatBoostClassifier()
                self.extreme_high_model.load_model(str(high_path))

                self.use_extreme_classifier = True
                logger.info(
                    f"Loaded extreme classifiers (LOW<{self.extreme_low_threshold}, HIGH>{self.extreme_high_threshold})"
                )
                return True

            return False
        except Exception as e:
            logger.error(f"Error loading extreme classifiers: {e}")
            return False

    def _load_ingame_models(self) -> bool:
        """Load in-game models for different time points (5, 7, 10 min)."""
        if not CATBOOST_AVAILABLE:
            return False

        loaded_any = False
        for minute in [5, 7, 10]:
            try:
                meta_path = MODELS_DIR / f"ingame_meta_min{minute}.json"
                low_path = MODELS_DIR / f"ingame_low_min{minute}.cbm"
                high_path = MODELS_DIR / f"ingame_high_min{minute}.cbm"

                if not (
                    meta_path.exists() and low_path.exists() and high_path.exists()
                ):
                    continue

                with open(meta_path, "r") as f:
                    meta = json.load(f)

                low_model = CatBoostClassifier()
                low_model.load_model(str(low_path))

                high_model = CatBoostClassifier()
                high_model.load_model(str(high_path))

                self.ingame_models[minute] = {
                    "low_model": low_model,
                    "high_model": high_model,
                    "feature_cols": meta.get("feature_cols", []),
                    "cat_features": meta.get("cat_features", []),
                    "cat_indices": meta.get("cat_indices", []),
                }
                loaded_any = True
                logger.info(
                    f"Loaded in-game models for minute={minute} (AUC: LOW={meta.get('low_auc', 0):.3f}, HIGH={meta.get('high_auc', 0):.3f})"
                )

            except Exception as e:
                logger.warning(f"Error loading in-game model for minute={minute}: {e}")

        self.use_ingame_model = loaded_any
        if loaded_any:
            logger.info(
                f"In-game models available for minutes: {list(self.ingame_models.keys())}"
            )
        return loaded_any

    def _load_winrate_classifier(self) -> bool:
        """Load winrate classifier (radiant_win prediction). Prefer v4, then v3, then v2 as fallback."""
        if not CATBOOST_AVAILABLE:
            return False

        try:
            # Prefer newest artifacts first.
            candidates = [
                (
                    "winrate_classifier_v4_roll_dna_meta.json",
                    "winrate_classifier_v4_roll_dna.cbm",
                    "v4_roll_dna",
                ),
                (
                    "winrate_classifier_v4_draft_meta.json",
                    "winrate_classifier_v4_draft.cbm",
                    "v4_draft",
                ),
                ("winrate_classifier_v4_meta.json", "winrate_classifier_v4.cbm", "v4"),
                ("winrate_classifier_v3_meta.json", "winrate_classifier_v3.cbm", "v3"),
                ("winrate_classifier_v2_meta.json", "winrate_classifier_v2.cbm", "v2"),
            ]

            meta_path = None
            model_path = None
            selected_tag = None

            for meta_name, model_name, tag in candidates:
                mp = MODELS_DIR / meta_name
                mdlp = MODELS_DIR / model_name
                if mp.exists() and mdlp.exists():
                    meta_path = mp
                    model_path = mdlp
                    selected_tag = tag
                    break

            if meta_path is None or model_path is None:
                logger.info("Winrate classifier not found")
                return False

            with open(meta_path, "r") as f:
                meta = json.load(f)
                self.winrate_feature_cols = meta.get("feature_cols", [])
                self.winrate_cat_features = meta.get("cat_features", [])
                self.winrate_cat_indices = meta.get("cat_indices", [])
                training = meta.get("training") or {}
                training_params = training.get("params") or {}
                self.winrate_include_team_ids = bool(training.get("include_team_ids", True))
                self.winrate_include_rolling_dna = bool(
                    training_params.get("include_rolling_dna", False)
                )
                self.winrate_model_tag = str(selected_tag or "")
                if not self.winrate_include_team_ids:
                    self.winrate_default_threshold = 0.525
                    self.winrate_min_threshold = 0.50
                else:
                    if self.winrate_include_rolling_dna:
                        self.winrate_default_threshold = 0.68
                        self.winrate_min_threshold = 0.60
                    else:
                        self.winrate_default_threshold = 0.60
                        self.winrate_min_threshold = 0.55

            self.winrate_model = CatBoostClassifier()
            self.winrate_model.load_model(str(model_path))
            self.use_winrate_classifier = True

            logger.info(
                f"Loaded winrate classifier {selected_tag} ({len(self.winrate_feature_cols)} features, AUC~{meta.get('auc_estimate', 0):.3f})"
            )
            return True

        except Exception as e:
            logger.error(f"Error loading winrate classifier: {e}")
            return False

    def _predict_ingame(
        self, features: Dict[str, Any], game_time_min: float
    ) -> Optional[Dict[str, float]]:
        """
        Predict using in-game model for the closest available minute.

        Args:
            features: Dict with both pre-game and in-game features
            game_time_min: Current game time in minutes

        Returns:
            Dict with low_prob, high_prob or None if no suitable model
        """
        if not self.ingame_models:
            return None

        # Check required in-game features are present
        required_ingame = ["ingame_total_kills", "ingame_kpm", "ingame_nw_lead"]
        missing_required = [
            feat for feat in required_ingame if features.get(feat) is None
        ]
        if missing_required:
            # This is a hard requirement for in-game models (avoid silently predicting with junk defaults)
            logger.info(
                f"In-game model unavailable (minute={game_time_min:.2f}): missing required features: {missing_required}"
            )
            return None

        # Find closest available minute (not exceeding current time)
        available_minutes = sorted(self.ingame_models.keys())
        selected_minute = None
        for m in available_minutes:
            if m <= game_time_min:
                selected_minute = m

        if selected_minute is None:
            return None

        model_data = self.ingame_models[selected_minute]
        feature_cols = model_data["feature_cols"]
        cat_features = model_data["cat_features"]

        try:
            # Build feature vector (+ track missing)
            missing_cols = [
                col
                for col in feature_cols
                if col not in features or features.get(col) is None
            ]
            missing_count = len(missing_cols)
            if missing_count:
                logger.warning(
                    f"In-game features missing: {missing_count}/{len(feature_cols)} "
                    f"(minute_used={selected_minute}, sample: {missing_cols[:20]})"
                )

            feature_values = []
            for col in feature_cols:
                val = features.get(col, 0.0)
                if val is None:
                    val = 0.0
                feature_values.append(val)

            X = pd.DataFrame([feature_values], columns=feature_cols)

            # Handle numeric features
            for col in X.columns:
                if col not in cat_features:
                    X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0.0)

            # Handle categorical features (safer coercion + count invalids)
            cat_invalid_count = 0
            invalid_cols = []
            for col in cat_features:
                if col in X.columns:
                    raw = X[col].iloc[0]
                    coerced = (
                        pd.to_numeric(pd.Series([raw]), errors="coerce")
                        .fillna(-1)
                        .astype(int)
                        .iloc[0]
                    )
                    if coerced <= 0:
                        cat_invalid_count += 1
                        invalid_cols.append(col)
                    X[col] = pd.Series([coerced]).astype(str)

            if cat_invalid_count:
                logger.warning(
                    f"In-game categorical invalid (<=0) count: {cat_invalid_count}/{len(cat_features)} "
                    f"(minute_used={selected_minute}, cols: {invalid_cols[:20]})"
                )

            # Additional targeted observability for team IDs (common source of -1)
            team_id_cols = [
                c
                for c in ["radiant_team_id", "dire_team_id"]
                if c in cat_features and c in X.columns
            ]
            if team_id_cols:
                team_ids = {c: X[c].iloc[0] for c in team_id_cols}
                if any(v in ("-1", "0") for v in team_ids.values()):
                    logger.info(
                        f"In-game team_id missing/invalid (minute_used={selected_minute}): {team_ids}"
                    )

            # Predict
            low_prob = float(model_data["low_model"].predict_proba(X)[0, 1])
            high_prob = float(model_data["high_model"].predict_proba(X)[0, 1])

            return {
                "low_prob": low_prob,
                "high_prob": high_prob,
                "minute_used": selected_minute,
                "missing_count": missing_count,
                "missing_cols_sample": missing_cols[:25],
                "cat_invalid_count": cat_invalid_count,
            }

        except Exception as e:
            logger.warning(f"In-game prediction failed: {e}")
            return None

    def _load_legacy_models(self) -> None:
        """Загружает LightGBM/XGBoost модели (legacy)."""
        try:
            import lightgbm as lgb
            import xgboost as xgb

            # Load metadata
            meta_path = MODELS_DIR / "live_models_meta.json"
            if meta_path.exists():
                with open(meta_path, "r") as f:
                    meta = json.load(f)
                    self.model_features = meta.get("feature_cols", [])
                    self.bk_line = meta.get("bk_line", 47.0)
                    logger.info(
                        f"Loaded model metadata: {len(self.model_features)} features, BK line={self.bk_line}"
                    )
            else:
                self.model_features = self.selected_features

            # Load LightGBM kills model
            lgbm_path = MODELS_DIR / "live_lgbm_kills.txt"
            if lgbm_path.exists():
                self.lgbm_kills = lgb.Booster(model_file=str(lgbm_path))
                logger.info("Loaded LightGBM kills model")
            else:
                self.lgbm_kills = None

            # Load XGBoost kills model
            xgb_kills_path = MODELS_DIR / "live_xgb_kills.json"
            if xgb_kills_path.exists():
                self.xgb_kills = xgb.Booster()
                self.xgb_kills.load_model(str(xgb_kills_path))
                logger.info("Loaded XGBoost kills model")
            else:
                self.xgb_kills = None

            # Load Winner model
            winner_path = MODELS_DIR / "live_winner.txt"
            if winner_path.exists():
                self.lgbm_winner = lgb.Booster(model_file=str(winner_path))
                logger.info("Loaded Winner model")
            else:
                self.lgbm_winner = None

            # Load Duration model
            duration_path = MODELS_DIR / "live_duration.json"
            if duration_path.exists():
                self.xgb_duration = xgb.Booster()
                self.xgb_duration.load_model(str(duration_path))
                logger.info("Loaded Duration model")
            else:
                self.xgb_duration = None

            # Load KPM model
            kpm_path = MODELS_DIR / "live_xgb_kpm.json"
            if kpm_path.exists():
                self.xgb_kpm = xgb.Booster()
                self.xgb_kpm.load_model(str(kpm_path))
                logger.info("Loaded KPM model")
            else:
                self.xgb_kpm = None

            self.models_loaded = (
                self.lgbm_kills is not None or self.xgb_kills is not None
            )

            if self.models_loaded:
                logger.info("Legacy models loaded successfully!")
            else:
                logger.warning("No models found, using heuristic predictions")

        except Exception as e:
            logger.error(f"Error loading legacy models: {e}")
            self.models_loaded = False

    def get_hero_name(self, hero_id: int) -> str:
        """Получает имя героя по ID."""
        hero_data = self.heroes.get(str(hero_id), {})
        if isinstance(hero_data, dict):
            return hero_data.get("localized_name", f"Hero_{hero_id}")
        return str(hero_data) if hero_data else f"Hero_{hero_id}"

    def extract_heroes_from_draft(
        self, radiant_heroes_and_pos: Dict, dire_heroes_and_pos: Dict
    ) -> Tuple[List[int], List[int]]:
        """Извлекает ID героев из драфта."""
        radiant_ids = []
        dire_ids = []

        for pos in ["pos1", "pos2", "pos3", "pos4", "pos5"]:
            # Radiant
            if pos in radiant_heroes_and_pos:
                hero_data = radiant_heroes_and_pos[pos]
                hero_id = None

                if "hero_id" in hero_data and hero_data["hero_id"] is not None:
                    hero_id = int(hero_data["hero_id"])
                elif "hero_name" in hero_data and hero_data["hero_name"]:
                    hero_id = self._get_hero_id_by_name(hero_data["hero_name"])

                if hero_id is not None and hero_id > 0:
                    radiant_ids.append(hero_id)

            # Dire
            if pos in dire_heroes_and_pos:
                hero_data = dire_heroes_and_pos[pos]
                hero_id = None

                if "hero_id" in hero_data and hero_data["hero_id"] is not None:
                    hero_id = int(hero_data["hero_id"])
                elif "hero_name" in hero_data and hero_data["hero_name"]:
                    hero_id = self._get_hero_id_by_name(hero_data["hero_name"])

                if hero_id is not None and hero_id > 0:
                    dire_ids.append(hero_id)

        return radiant_ids, dire_ids

    def _get_hero_id_by_name(self, hero_name: str) -> Optional[int]:
        """Получает ID героя по имени."""
        if not hero_name:
            return None

        hero_name_lower = hero_name.lower().strip()

        for hero_id, hero_data in self.heroes.items():
            if isinstance(hero_data, dict):
                name = hero_data.get("localized_name", "").lower()
            else:
                name = str(hero_data).lower()

            if name == hero_name_lower or hero_name_lower in name:
                return int(hero_id)

        return None

    def _get_hero_stat(
        self, stats_dict: Dict, hero_id: int, key: str, default: float = 0.0
    ) -> float:
        """Получает статистику героя из словаря."""
        hero_data = stats_dict.get(str(hero_id), {})
        if isinstance(hero_data, dict):
            return float(hero_data.get(key, default))
        return default

    def _compute_blood_score(self, hero_ids: List[int]) -> float:
        """Вычисляет blood score для команды (абсолютный)."""
        if not self.blood_stats or not hero_ids:
            return 0.0

        hero_scores = self.blood_stats.get("hero_blood", {})
        total = 0.0
        for hero_id in hero_ids:
            hero_data = hero_scores.get(str(hero_id), {})
            if isinstance(hero_data, dict):
                total += hero_data.get("blood_score", 0.0)
            else:
                total += float(hero_data) if hero_data else 0.0

        return total

    def _compute_blood_score_pm(self, hero_ids: List[int]) -> float:
        """Вычисляет blood score per minute для команды."""
        if not self.blood_stats or not hero_ids:
            return 0.0

        hero_scores = self.blood_stats.get("hero_blood", {})
        total = 0.0
        for hero_id in hero_ids:
            hero_data = hero_scores.get(str(hero_id), {})
            if isinstance(hero_data, dict):
                total += hero_data.get("blood_score_pm", 0.0)

        return total

    def _compute_blood_synergy(self, hero_ids: List[int]) -> float:
        """Вычисляет blood synergy для пар героев (абсолютный)."""
        if not self.blood_stats or len(hero_ids) < 2:
            return 0.0

        duo_scores = self.blood_stats.get("duo_blood", {})
        total = 0.0

        for i in range(len(hero_ids)):
            for j in range(i + 1, len(hero_ids)):
                key = f"{min(hero_ids[i], hero_ids[j])}_{max(hero_ids[i], hero_ids[j])}"
                duo_data = duo_scores.get(key, {})
                if isinstance(duo_data, dict):
                    total += duo_data.get("synergy", 0.0)

        return total

    def _compute_blood_synergy_pm(self, hero_ids: List[int]) -> float:
        """Вычисляет blood synergy per minute для пар героев."""
        if not self.blood_stats or len(hero_ids) < 2:
            return 0.0

        duo_scores = self.blood_stats.get("duo_blood", {})
        total = 0.0

        for i in range(len(hero_ids)):
            for j in range(i + 1, len(hero_ids)):
                key = f"{min(hero_ids[i], hero_ids[j])}_{max(hero_ids[i], hero_ids[j])}"
                duo_data = duo_scores.get(key, {})
                if isinstance(duo_data, dict):
                    total += duo_data.get("synergy_pm", 0.0)

        return total

    def _compute_match_blood_clash(
        self, radiant_ids: List[int], dire_ids: List[int]
    ) -> float:
        """Вычисляет blood clash между командами (абсолютный)."""
        if not self.blood_stats:
            return 0.0

        vs_scores = self.blood_stats.get("vs_blood", {})
        total = 0.0

        for r_id in radiant_ids:
            for d_id in dire_ids:
                key = f"{min(r_id, d_id)}_{max(r_id, d_id)}"
                vs_data = vs_scores.get(key, {})
                if isinstance(vs_data, dict):
                    total += vs_data.get("clash", 0.0)

        return total

    def _compute_match_blood_clash_pm(
        self, radiant_ids: List[int], dire_ids: List[int]
    ) -> float:
        """Вычисляет blood clash per minute между командами."""
        if not self.blood_stats:
            return 0.0

        vs_scores = self.blood_stats.get("vs_blood", {})
        total = 0.0

        for r_id in radiant_ids:
            for d_id in dire_ids:
                key = f"{min(r_id, d_id)}_{max(r_id, d_id)}"
                vs_data = vs_scores.get(key, {})
                if isinstance(vs_data, dict):
                    total += vs_data.get("clash_pm", 0.0)

        return total

    def _compute_team_cc_score(self, hero_ids: List[int]) -> float:
        """Вычисляет CC score для команды."""
        total = 0.0
        for hero_id in hero_ids:
            total += self._get_hero_stat(self.hero_cc_stats, hero_id, "cc_score", 0.0)
        return total

    def _compute_team_greed(self, hero_ids: List[int]) -> float:
        """Вычисляет greed index для команды."""
        total = 0.0
        for hero_id in hero_ids:
            total += self._get_hero_stat(
                self.hero_greed_index, hero_id, "greed_index", 0.5
            )
        return total / max(len(hero_ids), 1)

    def _compute_team_push_score(self, hero_ids: List[int]) -> float:
        """Вычисляет push score для команды."""
        total = 0.0
        for hero_id in hero_ids:
            total += self._get_hero_stat(
                self.hero_push_stats, hero_id, "push_score", 0.0
            )
        return total

    def _compute_team_heal_score(self, hero_ids: List[int]) -> float:
        """Вычисляет heal score для команды."""
        total = 0.0
        for hero_id in hero_ids:
            total += self._get_hero_stat(
                self.hero_healing_stats, hero_id, "healing_score", 0.0
            )
        return total

    def _compute_team_wave_clear(self, hero_ids: List[int]) -> float:
        """Вычисляет wave clear для команды."""
        total = 0.0
        for hero_id in hero_ids:
            total += self._get_hero_stat(
                self.hero_wave_clear, hero_id, "wave_clear_score", 0.0
            )
        return total

    def _compute_team_early_power(self, hero_ids: List[int]) -> float:
        """Вычисляет early game power для команды."""
        total = 0.0
        for hero_id in hero_ids:
            total += self._get_hero_stat(
                self.hero_power_spikes, hero_id, "early_power", 0.5
            )
        return total / max(len(hero_ids), 1)

    def _compute_team_late_power(self, hero_ids: List[int]) -> float:
        """Вычисляет late game power для команды."""
        total = 0.0
        for hero_id in hero_ids:
            total += self._get_hero_stat(
                self.hero_power_spikes, hero_id, "late_power", 0.5
            )
        return total / max(len(hero_ids), 1)

    def _get_player_dna(self, account_id: int) -> Dict[str, float]:
        """Получает DNA игрока."""
        dna = self.player_dna.get(str(account_id), {})
        return {
            "avg_kills": dna.get("avg_kills", 5.0),
            "avg_deaths": dna.get("avg_deaths", 5.0),
            "aggression": dna.get("aggression", 1.0),
            "pace": dna.get("pace", 1.0),
        }

    def _compute_team_dna(
        self, account_ids: Optional[List[int]], require_all: bool = False
    ) -> Optional[Dict[str, float]]:
        """
        Compute aggregated DNA stats for a team.

        Args:
            account_ids: List of player account IDs
            require_all: If True, returns None if any player is missing DNA data

        Returns:
            Dict with DNA stats or None if insufficient data
        """
        MIN_PLAYERS_WITH_DNA = 3  # At least 3 out of 5 players must have DNA

        if not account_ids or not self.player_dna:
            return None

        # Filter out zero/invalid account IDs
        valid_ids = [acc_id for acc_id in account_ids if acc_id and acc_id > 0]
        if len(valid_ids) < MIN_PLAYERS_WITH_DNA:
            return None

        kills_sum, deaths_sum, agg_sum, pace_sum = 0.0, 0.0, 0.0, 0.0
        feed_sum, dur_sum, kda_sum = 0.0, 0.0, 0.0
        recent_agg_sum, agg_on_aggro_sum, agg_on_passive_sum, agg_delta_sum = (
            0.0,
            0.0,
            0.0,
            0.0,
        )
        count = 0
        missing_players = []

        for acc_id in valid_ids:
            dna = self.player_dna.get(str(acc_id), {})
            if dna and "aggression" in dna:  # Check for actual DNA data, not empty dict
                kills_sum += dna.get("avg_kills", 5.0)
                deaths_sum += dna.get("avg_deaths", 5.0)
                agg_sum += dna.get("aggression", 0.35)
                pace_sum += dna.get("pace", 0.12)
                feed_sum += dna.get("feed", 0.1)
                dur_sum += dna.get("avg_duration", 40.0)
                kda_sum += dna.get("kda", 3.0)
                # Extended DNA
                recent_agg_sum += dna.get(
                    "recent_aggression", dna.get("aggression", 0.35)
                )
                agg_on_aggro_sum += dna.get(
                    "aggression_on_aggro", dna.get("aggression", 0.35)
                )
                agg_on_passive_sum += dna.get(
                    "aggression_on_passive", dna.get("aggression", 0.35)
                )
                agg_delta_sum += dna.get("aggression_delta", 0.0)
                count += 1
            else:
                missing_players.append(acc_id)

        # Strict mode: require all players
        if require_all and missing_players:
            logger.warning(f"Missing DNA for players: {missing_players}")
            return None

        # Need at least MIN_PLAYERS_WITH_DNA players with DNA data
        if count < MIN_PLAYERS_WITH_DNA:
            logger.warning(
                f"Only {count}/{len(valid_ids)} players have DNA data (need {MIN_PLAYERS_WITH_DNA})"
            )
            return None

        return {
            "avg_kills": kills_sum / count,
            "avg_deaths": deaths_sum / count,
            "aggression": agg_sum / count,
            "pace": pace_sum / count,
            "feed": feed_sum / count,
            "avg_duration": dur_sum / count,
            "kda": kda_sum / count,
            "coverage": count,
            # Extended DNA
            "recent_aggression": recent_agg_sum / count,
            "aggression_on_aggro": agg_on_aggro_sum / count,
            "aggression_on_passive": agg_on_passive_sum / count,
            "aggression_delta": agg_delta_sum / count,
        }

    def _get_team_rating(self, team_id: Optional[int]) -> Tuple[float, float, str]:
        """
        Получает рейтинг команды из Glicko-2.

        Returns:
            (rating, rd, team_name) - defaults to (1500, 350, 'Unknown') if not found
        """
        if team_id is None:
            return 1500.0, 350.0, "Unknown"

        teams = self.team_ratings.get("teams", {})
        team_data = teams.get(str(team_id), {})

        return (
            team_data.get("rating", 1500.0),
            team_data.get("rd", 350.0),
            team_data.get("name", f"Team_{team_id}"),
        )

    def _calculate_rating_win_prob(
        self,
        rating1: float,
        rd1: float,
        rating2: float,
        rd2: float,
    ) -> float:
        """
        Вычисляет вероятность победы team1 над team2 по Glicko.
        """
        import math

        combined_rd = math.sqrt(rd1**2 + rd2**2)
        q = math.log(10) / 400
        g_rd = 1.0 / math.sqrt(1 + 3 * (q**2) * (combined_rd**2) / (math.pi**2))
        exp_score = 1.0 / (1.0 + 10 ** (-g_rd * (rating1 - rating2) / 400))
        return exp_score

    def _get_hero_is_greedy(self, hero_id: int) -> float:
        """Проверяет, является ли герой greedy (greed_index > 0.6)."""
        greed = self._get_hero_stat(self.hero_greed_index, hero_id, "greed_index", 0.5)
        return 1.0 if greed > 0.6 else 0.0

    def _get_team_tier_numeric(self, team_id: Optional[int]) -> int:
        """
        Get team tier as numeric value.

        Returns:
            1 = Tier 1, 2 = Tier 2, 3 = Tier 3/Unknown
        """
        if team_id is None:
            return 3
        if TEAM_MAPPING_AVAILABLE:
            try:
                return get_team_tier(team_id)
            except Exception:
                pass

        # Tier 1 teams (top pro teams)
        tier1_ids = {
            7119388,
            8255888,
            8724984,
            2586976,
            2163,
            8291895,
            8599101,
            9338413,
            15,
            726228,
            8574561,
            9351740,
            8597976,
            7732977,
            8728920,
            8254400,
            9247354,
            8894818,
            9303484,
            8605863,
            9255039,
            7422789,
        }

        # Tier 2 teams
        tier2_ids = {
            1838315,
            111474,
            8214850,
            8254145,
            8668460,
            9467430,
            5014799,
            8697185,
            8375259,
            9470838,
            8376696,
            7917893,
            2576071,
            8106674,
        }

        if team_id in tier1_ids:
            return 1
        elif team_id in tier2_ids:
            return 2
        else:
            return 3

    def _normalize_match_start_time(self, match_start_time: Optional[int]) -> Optional[int]:
        if match_start_time is None:
            return None
        try:
            ts = int(match_start_time)
        except Exception:
            return None
        if ts <= 0:
            return None
        if ts > 10_000_000_000:
            ts = int(ts / 1000)
        return ts

    def _get_time_features(self, match_start_time: Optional[int]) -> Dict[str, Any]:
        ts = self._normalize_match_start_time(match_start_time)
        if ts is None:
            dt = datetime.now(timezone.utc)
            defaulted = 1.0
        else:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            defaulted = 0.0
        day_of_week = dt.weekday()
        hour_of_day = dt.hour
        is_weekend = 1.0 if day_of_week >= 5 else 0.0
        is_prime_time = 1.0 if 18 <= hour_of_day <= 23 else 0.0
        return {
            "is_weekend": is_weekend,
            "is_prime_time": is_prime_time,
            "day_of_week": day_of_week,
            "hour_of_day": hour_of_day,
            "start_time_defaulted": defaulted,
        }

    def _get_patch_features(self, match_start_time: Optional[int]) -> Dict[str, Any]:
        ts = self._normalize_match_start_time(match_start_time)
        if not _PATCH_SCHEDULE_INFO or ts is None:
            return {
                "patch_id": -1,
                "patch_major": 0,
                "patch_minor": 0,
                "patch_sub": 0,
                "days_since_patch": 0.0,
                "is_new_patch": 0.0,
                "patch_known": 0.0,
                "patch_pre_schedule": 1.0,
            }

        patch_known = 1.0
        pre_schedule = 0.0
        idx = -1
        for i, patch in enumerate(_PATCH_SCHEDULE_INFO):
            if ts >= patch["ts"]:
                idx = i
            else:
                break
        if idx < 0:
            idx = 0
            pre_schedule = 1.0
        patch = _PATCH_SCHEDULE_INFO[idx]
        days_since_patch = max(0.0, (ts - patch["ts"]) / 86400.0)
        is_new_patch = 1.0 if days_since_patch <= 14.0 else 0.0
        return {
            "patch_id": patch["patch_id"],
            "patch_major": patch["major"],
            "patch_minor": patch["minor"],
            "patch_sub": patch["sub"],
            "days_since_patch": days_since_patch,
            "is_new_patch": is_new_patch,
            "patch_known": patch_known,
            "patch_pre_schedule": pre_schedule,
        }

    def _normal_cdf(self, x: float, mu: float, sigma: float) -> float:
        import math

        if sigma <= 1e-6:
            return 0.5
        z = (x - mu) / (sigma * math.sqrt(2.0))
        return 0.5 * (1.0 + math.erf(z))

    @staticmethod
    def _cat_to_str(val: Any) -> str:
        if val is None:
            return "-1"
        if isinstance(val, str):
            s = val.strip()
            return s if s else "UNKNOWN"
        try:
            if isinstance(val, (int, np.integer)):
                return str(int(val))
            if isinstance(val, (float, np.floating)):
                if np.isnan(val):
                    return "-1"
                return str(int(val))
        except Exception:
            pass
        return str(val)

    def _compute_synergy_score(self, hero_ids: List[int]) -> float:
        """Вычисляет synergy score для команды из hero_synergy.json."""
        if not self.hero_synergy or len(hero_ids) < 2:
            return 0.0

        synergy_data = self.hero_synergy.get("synergy", {})
        total = 0.0
        for i in range(len(hero_ids)):
            for j in range(i + 1, len(hero_ids)):
                key = f"{min(hero_ids[i], hero_ids[j])}_{max(hero_ids[i], hero_ids[j])}"
                syn_val = synergy_data.get(key, 0.0)
                if isinstance(syn_val, (int, float)):
                    total += float(syn_val)
        return total

    def _compute_early_synergy(self, hero_ids: List[int]) -> float:
        """Вычисляет early game synergy (uses general synergy as proxy)."""
        if not self.hero_synergy or len(hero_ids) < 2:
            return 0.0

        synergy_data = self.hero_synergy.get("synergy", {})
        total = 0.0
        for i in range(len(hero_ids)):
            for j in range(i + 1, len(hero_ids)):
                key = f"{min(hero_ids[i], hero_ids[j])}_{max(hero_ids[i], hero_ids[j])}"
                # Synergy value is a float, not a dict
                syn_val = synergy_data.get(key, 0.0)
                if isinstance(syn_val, (int, float)):
                    total += syn_val
        return total

    def _compute_late_synergy(self, hero_ids: List[int]) -> float:
        """Вычисляет late game synergy (uses general synergy as proxy)."""
        if not self.hero_synergy or len(hero_ids) < 2:
            return 0.0

        synergy_data = self.hero_synergy.get("synergy", {})
        total = 0.0
        for i in range(len(hero_ids)):
            for j in range(i + 1, len(hero_ids)):
                key = f"{min(hero_ids[i], hero_ids[j])}_{max(hero_ids[i], hero_ids[j])}"
                # Synergy value is a float, not a dict
                syn_val = synergy_data.get(key, 0.0)
                if isinstance(syn_val, (int, float)):
                    total += syn_val
        return total

    def _compute_early_counter_score(
        self, team_ids: List[int], enemy_ids: List[int]
    ) -> float:
        """Compute early game counter score (1v1) from public matches."""
        if not self.early_late_counters:
            return 0.0

        counter_data = self.early_late_counters.get("early", {}).get("counter_1v1", {})
        total = 0.0
        count = 0

        for t_hero in team_ids:
            for e_hero in enemy_ids:
                key = f"{t_hero}_vs_{e_hero}"
                wr = counter_data.get(key)
                if wr is not None:
                    total += (wr - 0.5) * 100  # Convert to advantage %
                    count += 1

        return total / max(count, 1)

    def _compute_late_counter_score(
        self, team_ids: List[int], enemy_ids: List[int]
    ) -> float:
        """Compute late game counter score (1v1) from public matches."""
        if not self.early_late_counters:
            return 0.0

        counter_data = self.early_late_counters.get("late", {}).get("counter_1v1", {})
        total = 0.0
        count = 0

        for t_hero in team_ids:
            for e_hero in enemy_ids:
                key = f"{t_hero}_vs_{e_hero}"
                wr = counter_data.get(key)
                if wr is not None:
                    total += (wr - 0.5) * 100
                    count += 1

        return total / max(count, 1)

    def _compute_early_synergy_pub(self, hero_ids: List[int]) -> float:
        """Compute early game synergy (1+1) from public matches."""
        if not self.early_late_counters or len(hero_ids) < 2:
            return 0.0

        syn_data = self.early_late_counters.get("early", {}).get("synergy_2", {})
        total = 0.0
        count = 0

        for i in range(len(hero_ids)):
            for j in range(i + 1, len(hero_ids)):
                key = f"{min(hero_ids[i], hero_ids[j])}_{max(hero_ids[i], hero_ids[j])}"
                wr = syn_data.get(key)
                if wr is not None:
                    total += (wr - 0.5) * 100
                    count += 1

        return total / max(count, 1)

    def _compute_late_synergy_pub(self, hero_ids: List[int]) -> float:
        """Compute late game synergy (1+1) from public matches."""
        if not self.early_late_counters or len(hero_ids) < 2:
            return 0.0

        syn_data = self.early_late_counters.get("late", {}).get("synergy_2", {})
        total = 0.0
        count = 0

        for i in range(len(hero_ids)):
            for j in range(i + 1, len(hero_ids)):
                key = f"{min(hero_ids[i], hero_ids[j])}_{max(hero_ids[i], hero_ids[j])}"
                wr = syn_data.get(key)
                if wr is not None:
                    total += (wr - 0.5) * 100
                    count += 1

        return total / max(count, 1)

    def _compute_trio_synergy(self, hero_ids: List[int], phase: str = "early") -> float:
        """Compute trio synergy (1+1+1) from public matches."""
        if not self.early_late_counters or len(hero_ids) < 3:
            return 0.0

        from itertools import combinations

        syn_data = self.early_late_counters.get(phase, {}).get("synergy_3", {})
        total = 0.0
        count = 0

        for h1, h2, h3 in combinations(sorted(hero_ids), 3):
            key = f"{h1}_{h2}_{h3}"
            wr = syn_data.get(key)
            if wr is not None:
                total += (wr - 0.5) * 100
                count += 1

        return total / max(count, 1)

    def _compute_mid_matchup(
        self, radiant_mid: int, dire_mid: int, phase: str = "early"
    ) -> float:
        """Compute mid lane 1v1 matchup advantage."""
        if not self.early_late_counters:
            return 0.0

        mid_data = self.early_late_counters.get(phase, {}).get("mid_1v1", {})
        key = f"{radiant_mid}_vs_{dire_mid}"
        wr = mid_data.get(key)

        if wr is not None:
            return (wr - 0.5) * 100  # Radiant advantage
        return 0.0

    def _compute_splitpush_threat(self, hero_ids: List[int]) -> float:
        """
        Compute splitpush threat score.
        Heroes that can pressure multiple lanes simultaneously.
        """
        # Strong splitpushers
        SPLITPUSH_HEROES: Dict[int, float] = {
            53: 5.0,  # Nature's Prophet - global TP, treants
            1: 4.5,  # Anti-Mage - blink, fast farm
            80: 4.5,  # Tinker - BoT, global presence
            46: 4.0,  # Phantom Assassin - fast, elusive
            67: 4.0,  # Spectre - Haunt
            89: 4.0,  # Naga Siren - illusions
            12: 3.5,  # Phantom Lancer - illusions
            74: 3.5,  # Invoker - can push waves globally
            47: 3.5,  # Viper - tower damage
            77: 3.5,  # Lycan - wolves, fast push
            93: 3.5,  # Slark - elusive
            109: 3.0,  # Terrorblade - illusions, tower damage
            94: 3.0,  # Medusa - split shot clear
            81: 3.0,  # Wraith King - skeletons
            44: 3.0,  # Phantom Assassin
            101: 2.5,  # Skywrath - wave clear
            63: 2.5,  # Weaver - elusive
        }
        return sum(SPLITPUSH_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_roshan_potential(self, hero_ids: List[int]) -> float:
        """
        Compute Roshan kill potential.
        Heroes that can take Roshan early/safely.
        """
        ROSHAN_HEROES: Dict[int, float] = {
            # Strong Roshan heroes
            114: 5.0,  # Monkey King - Jingu stacks
            44: 4.5,  # Phantom Assassin - crit
            81: 4.5,  # Wraith King - lifesteal, crit
            11: 4.5,  # Shadow Fiend - damage
            8: 4.0,  # Juggernaut - healing ward
            64: 4.0,  # Jakiro - liquid fire
            77: 4.0,  # Lycan - wolves tank
            49: 4.0,  # Dragon Knight - tanky
            109: 4.0,  # Terrorblade - metamorphosis
            54: 3.5,  # Lifestealer - feast
            98: 3.5,  # Ursa - fury swipes (best Roshan)
            6: 3.5,  # Drow Ranger - damage aura
            46: 3.5,  # Templar Assassin - meld
            # Support Roshan
            50: 3.0,  # Dazzle - minus armor
            20: 3.0,  # Vengeful Spirit - minus armor
            31: 2.5,  # Lich - frost shield
            102: 2.5,  # Abaddon - sustain
        }
        return sum(ROSHAN_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_pickoff_potential(self, hero_ids: List[int]) -> float:
        """
        Compute pickoff potential - ability to catch and kill solo heroes.
        """
        PICKOFF_HEROES: Dict[int, float] = {
            # Strong pickoff
            62: 5.0,  # Bounty Hunter - track, invis
            56: 5.0,  # Clinkz - invis, burst
            93: 5.0,  # Slark - pounce, shadow dance
            32: 4.5,  # Riki - invis, smoke
            73: 4.5,  # Nyx Assassin - stun, invis
            17: 4.5,  # Storm Spirit - zip, catch
            39: 4.0,  # Queen of Pain - blink, burst
            13: 4.0,  # Puck - silence, catch
            106: 4.0,  # Ember Spirit - chains, mobility
            126: 4.0,  # Void Spirit - catch, burst
            3: 4.0,  # Bane - grip
            65: 3.5,  # Batrider - lasso
            14: 3.5,  # Pudge - hook
            26: 3.5,  # Lion - hex, finger
            45: 3.5,  # Shadow Shaman - shackles
            22: 3.0,  # Zeus - vision, damage
            75: 3.0,  # Silencer - global silence
        }
        return sum(PICKOFF_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_teamfight_score(self, hero_ids: List[int]) -> float:
        """
        Compute teamfight potential - ability to win 5v5 fights.
        """
        TEAMFIGHT_HEROES: Dict[int, float] = {
            # AOE ultimates
            33: 5.0,  # Enigma - Black Hole
            97: 5.0,  # Magnus - RP
            29: 5.0,  # Tidehunter - Ravage
            110: 4.5,  # Phoenix - Supernova
            41: 4.5,  # Faceless Void - Chrono
            89: 4.5,  # Naga Siren - Song
            86: 4.5,  # Rubick - steal ults
            # Strong teamfight
            25: 4.0,  # Lina - AOE stun, damage
            27: 4.0,  # Shadow Fiend - requiem
            52: 4.0,  # Leshrac - AOE damage
            87: 4.0,  # Disruptor - static storm
            112: 4.0,  # Winter Wyvern - curse
            5: 4.0,  # Crystal Maiden - freezing field
            64: 3.5,  # Jakiro - macropyre
            68: 3.5,  # Ancient Apparition - ice blast
            30: 3.5,  # Witch Doctor - death ward
            36: 3.5,  # Necrophos - reaper
            84: 3.5,  # Ogre Magi - bloodlust
        }
        return sum(TEAMFIGHT_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_counter_initiation(self, hero_ids: List[int]) -> float:
        """
        Compute counter-initiation potential - ability to respond to enemy initiation.
        """
        COUNTER_INIT_HEROES: Dict[int, float] = {
            86: 5.0,  # Rubick - spell steal
            79: 5.0,  # Shadow Demon - disruption, purge
            111: 5.0,  # Oracle - false promise, disarm
            102: 4.5,  # Abaddon - aphotic shield, borrowed time
            50: 4.5,  # Dazzle - shallow grave
            112: 4.5,  # Winter Wyvern - cold embrace, curse
            75: 4.0,  # Silencer - global silence
            76: 4.0,  # Outworld Destroyer - astral
            57: 4.0,  # Omniknight - guardian angel
            91: 4.0,  # Io - relocate save
            20: 3.5,  # Vengeful Spirit - swap
            3: 3.5,  # Bane - nightmare
            100: 3.5,  # Tusk - snowball
            37: 3.0,  # Necrophos - ghost shroud
            31: 3.0,  # Lich - frost shield
        }
        return sum(COUNTER_INIT_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_vision_control(self, hero_ids: List[int]) -> float:
        """
        Compute vision control potential.

        Heroes that provide superior vision = better map control = more pickoffs.
        High vision teams can find kills more easily.
        """
        VISION_HEROES: Dict[int, float] = {
            # Global/semi-global vision
            22: 5.0,  # Zeus - Lightning Bolt true sight, Nimbus
            60: 5.0,  # Nightstalker - Hunter in the Night vision
            62: 4.5,  # Bounty Hunter - Track
            56: 4.5,  # Clinkz - Skeleton Walk (invis detection with talent)
            54: 4.0,  # Lifestealer - Infest (scout)
            # Ward-based vision
            38: 4.0,  # Beastmaster - Hawk
            66: 4.0,  # Chen - creeps
            45: 3.5,  # Pugna - Nether Ward
            77: 3.5,  # Shadow Shaman - Serpent Wards
            85: 3.5,  # Undying - Tombstone
            # Invis detection
            15: 3.0,  # Razor - Static Link reveals
            98: 3.0,  # Treant Protector - Eyes in the Forest
            119: 3.0,  # Dark Willow - Bramble Maze
            # Scouting abilities
            9: 2.5,  # Mirana - Leap (scout)
            53: 2.5,  # Nature's Prophet - Sprout, Teleport
            67: 2.5,  # Spectre - Haunt (global vision)
        }
        return sum(VISION_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_smoke_gank_potential(self, hero_ids: List[int]) -> float:
        """
        Compute smoke gank potential.

        Heroes that excel at smoke ganks = more kills from rotations.
        Good smoke heroes have: instant disable, burst, mobility.
        """
        SMOKE_GANK_HEROES: Dict[int, float] = {
            # Instant lockdown
            71: 5.0,  # Spirit Breaker - Charge + Bash
            14: 5.0,  # Pudge - Hook
            17: 5.0,  # Storm Spirit - Ball Lightning + Vortex
            3: 4.5,  # Bane - Nightmare + Grip
            65: 4.5,  # Batrider - Lasso
            # High burst
            7: 4.5,  # Earthshaker - Fissure
            100: 4.5,  # Tusk - Snowball + Walrus Punch
            26: 4.0,  # Lion - Hex + Finger
            27: 4.0,  # Shadow Fiend - Requiem
            25: 4.0,  # Lina - Stun + Laguna
            # Mobility + disable
            106: 4.0,  # Ember Spirit - Chains
            126: 4.0,  # Void Spirit - Aether Remnant
            129: 4.0,  # Mars - Arena + Spear
            # Support gankers
            62: 3.5,  # Bounty Hunter - Track
            56: 3.5,  # Clinkz - Strafe burst
            73: 3.5,  # Nyx Assassin - Impale + Vendetta
            32: 3.5,  # Riki - Smoke Screen
            93: 3.5,  # Slark - Pounce
        }
        return sum(SMOKE_GANK_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_highground_defense(self, hero_ids: List[int]) -> float:
        """
        Compute high ground defense potential.

        Heroes that excel at defending high ground = longer games, more kills.
        Good HG defense = wave clear, AOE, counter-initiation.
        """
        HG_DEFENSE_HEROES: Dict[int, float] = {
            # Wave clear + AOE
            34: 5.0,  # Tinker - March, Laser
            52: 5.0,  # Leshrac - Diabolic Edict, Pulse Nova
            94: 5.0,  # Medusa - Split Shot, Stone Gaze
            # Strong AOE ultimates
            33: 4.5,  # Enigma - Black Hole
            29: 4.5,  # Tidehunter - Ravage
            97: 4.5,  # Magnus - RP
            110: 4.5,  # Phoenix - Supernova
            # Counter-initiation
            112: 4.0,  # Winter Wyvern - Curse
            89: 4.0,  # Naga Siren - Song
            87: 4.0,  # Disruptor - Static Storm
            # Wave clear
            48: 3.5,  # Luna - Eclipse, Glaives
            46: 3.5,  # Templar Assassin - Psi Blades
            64: 3.5,  # Jakiro - Macropyre
            68: 3.5,  # Ancient Apparition - Ice Blast
            # Defensive abilities
            50: 3.0,  # Dazzle - Shallow Grave
            102: 3.0,  # Abaddon - Borrowed Time
            111: 3.0,  # Oracle - False Promise
        }
        return sum(HG_DEFENSE_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_highground_siege(self, hero_ids: List[int]) -> float:
        """
        Compute high ground siege potential.

        Heroes that excel at taking high ground = faster games.
        Good HG siege = summons, long range, sustain.
        """
        HG_SIEGE_HEROES: Dict[int, float] = {
            # Summons/Illusions
            53: 5.0,  # Nature's Prophet - Treants
            89: 5.0,  # Naga Siren - Illusions
            109: 5.0,  # Terrorblade - Illusions
            12: 4.5,  # Phantom Lancer - Illusions
            # Long range damage
            35: 4.5,  # Sniper - Take Aim
            52: 4.5,  # Leshrac - Edict
            64: 4.5,  # Jakiro - Liquid Fire
            # Sustain/Heal
            57: 4.0,  # Omniknight - Heal, Guardian Angel
            50: 4.0,  # Dazzle - Heal, Grave
            91: 4.0,  # Io - Tether heal
            # Tower damage
            48: 3.5,  # Luna - Glaives
            81: 3.5,  # Chaos Knight - Phantasm
            54: 3.5,  # Lifestealer - Feast
            70: 3.5,  # Ursa - Fury Swipes
            # Auras
            77: 3.0,  # Shadow Shaman - Serpent Wards
            37: 3.0,  # Warlock - Golem
            45: 3.0,  # Pugna - Nether Ward
        }
        return sum(HG_SIEGE_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_aura_stacking(self, hero_ids: List[int]) -> float:
        """
        Compute aura stacking potential.

        Teams with multiple aura carriers = stronger deathball.
        Auras amplify team damage/survivability in fights.
        """
        AURA_HEROES: Dict[int, float] = {
            # Strong auras
            38: 5.0,  # Beastmaster - Inner Beast, Drums
            77: 4.5,  # Lycan - Howl, Feral Impulse
            54: 4.5,  # Lifestealer - Open Wounds (slow aura)
            # Armor/Attack auras
            96: 4.0,  # Centaur - Stampede
            85: 4.0,  # Undying - Flesh Golem
            36: 4.0,  # Necrophos - Heartstopper Aura
            # Support auras
            5: 3.5,  # Crystal Maiden - Arcane Aura
            84: 3.5,  # Ogre Magi - Bloodlust
            31: 3.5,  # Lich - Frost Shield
            # Item-based aura carriers
            57: 3.0,  # Omniknight - usually builds auras
            91: 3.0,  # Io - Tether (pseudo-aura)
            98: 3.0,  # Treant Protector - Living Armor
            20: 2.5,  # Vengeful Spirit - Vengeance Aura
            66: 2.5,  # Chen - Penitence, creep auras
        }
        return sum(AURA_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_dispel_availability(self, hero_ids: List[int]) -> float:
        """
        Compute dispel availability.

        Teams with dispels can remove key debuffs = survive more.
        Strong dispels counter stuns, silences, slows.
        """
        DISPEL_HEROES: Dict[int, float] = {
            # Strong dispels (can remove stuns)
            102: 5.0,  # Abaddon - Aphotic Shield (strong dispel)
            111: 5.0,  # Oracle - Fortune's End (strong dispel)
            54: 4.5,  # Lifestealer - Rage (self strong dispel)
            93: 4.5,  # Slark - Dark Pact (self strong dispel)
            # Basic dispels
            79: 4.0,  # Shadow Demon - Demonic Purge
            57: 4.0,  # Omniknight - Heavenly Grace
            26: 4.0,  # Lion - Mana Drain (dispel talent)
            # Self dispels
            11: 3.5,  # Juggernaut - Blade Fury
            54: 3.5,  # Lifestealer - Rage
            63: 3.5,  # Weaver - Time Lapse
            # Soft dispels
            31: 3.0,  # Lich - Frost Shield
            50: 3.0,  # Dazzle - Shadow Wave
            5: 2.5,  # Crystal Maiden - can build items
            37: 2.5,  # Necrophos - Ghost Shroud
        }
        return sum(DISPEL_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_shard_timing(self, hero_ids: List[int]) -> Dict[str, float]:
        """
        Compute shard/aghs timing impact.

        Some heroes have game-changing shards at 15 min.
        Strong shard = power spike at 15 min = more action.
        """
        # Heroes with strong 15-min shard spikes
        STRONG_SHARD: Dict[int, float] = {
            # Game-changing shards
            14: 5.0,  # Pudge - Dismember heal
            7: 5.0,  # Earthshaker - Aftershock on Enchant
            17: 5.0,  # Storm Spirit - Electric Vortex pull
            97: 4.5,  # Magnus - Skewer AoE
            29: 4.5,  # Tidehunter - Gush AoE
            100: 4.5,  # Tusk - Frozen Sigil
            # Strong utility shards
            71: 4.0,  # Spirit Breaker - Bulldoze
            2: 4.0,  # Axe - Counter Helix on attack
            96: 4.0,  # Centaur - Cart
            104: 4.0,  # Legion Commander - Press the Attack AoE
            # Damage shards
            25: 3.5,  # Lina - Fiery Soul stacks
            27: 3.5,  # Shadow Fiend - Shadowraze heal
            52: 3.5,  # Leshrac - Nihilism
        }

        # Heroes with strong Aghs (20-25 min spike)
        STRONG_AGHS: Dict[int, float] = {
            # Game-winning Aghs
            33: 5.0,  # Enigma - Midnight Pulse in Black Hole
            41: 5.0,  # Faceless Void - Time Walk allies
            63: 5.0,  # Weaver - Time Lapse ally
            # Strong Aghs
            91: 4.5,  # Io - Tether no break
            110: 4.5,  # Phoenix - Supernova ally
            86: 4.5,  # Rubick - Spell Steal upgrade
            # Utility Aghs
            22: 4.0,  # Zeus - Nimbus
            75: 4.0,  # Silencer - Global Silence pierce
            53: 4.0,  # Nature's Prophet - Greater Treants
        }

        shard_score = sum(STRONG_SHARD.get(h, 0.0) for h in hero_ids)
        aghs_score = sum(STRONG_AGHS.get(h, 0.0) for h in hero_ids)

        return {
            "shard_score": shard_score,
            "aghs_score": aghs_score,
            "item_timing_score": shard_score * 0.6 + aghs_score * 0.4,
        }

    def _compute_mana_dependency(self, hero_ids: List[int]) -> Dict[str, float]:
        """
        Compute mana dependency score.

        Mana-hungry heroes = vulnerable to mana burn/drain.
        Low mana heroes = can fight longer.
        """
        # High mana dependency (need mana to function)
        MANA_HUNGRY: Dict[int, float] = {
            17: 5.0,  # Storm Spirit - Ball Lightning
            52: 5.0,  # Leshrac - Pulse Nova
            34: 5.0,  # Tinker - Rearm
            74: 4.5,  # Invoker - spell combos
            22: 4.5,  # Zeus - spam spells
            25: 4.5,  # Lina - spell combos
            5: 4.0,  # Crystal Maiden - low pool
            87: 4.0,  # Disruptor - combo
            64: 4.0,  # Jakiro - combo
            68: 4.0,  # Ancient Apparition - combo
            36: 3.5,  # Necrophos - sustain
            45: 3.5,  # Pugna - Nether Blast spam
        }

        # Low mana dependency (can fight without mana)
        MANA_INDEPENDENT: Dict[int, float] = {
            59: 5.0,  # Huskar - Burning Spear
            70: 5.0,  # Ursa - Fury Swipes
            81: 4.5,  # Chaos Knight - right click
            44: 4.5,  # Phantom Assassin - crit
            1: 4.0,  # Anti-Mage - mana break
            54: 4.0,  # Lifestealer - Feast
            11: 4.0,  # Juggernaut - right click
            93: 3.5,  # Slark - Essence Shift
            48: 3.5,  # Luna - Glaives
        }

        mana_hungry = sum(MANA_HUNGRY.get(h, 0.0) for h in hero_ids)
        mana_independent = sum(MANA_INDEPENDENT.get(h, 0.0) for h in hero_ids)

        return {
            "mana_hungry": mana_hungry,
            "mana_independent": mana_independent,
            "mana_balance": mana_independent - mana_hungry,
        }

    def _compute_tempo_control(self, hero_ids: List[int]) -> float:
        """
        Compute tempo control score.

        Tempo heroes dictate when fights happen.
        High tempo = more action = more kills.
        """
        TEMPO_HEROES: Dict[int, float] = {
            # High tempo (force fights)
            71: 5.0,  # Spirit Breaker - Charge
            17: 5.0,  # Storm Spirit - Ball Lightning
            106: 5.0,  # Ember Spirit - Remnants
            126: 4.5,  # Void Spirit - mobility
            59: 4.5,  # Huskar - early aggression
            104: 4.5,  # Legion Commander - Duel
            # Medium tempo
            14: 4.0,  # Pudge - Hook
            65: 4.0,  # Batrider - Lasso
            100: 4.0,  # Tusk - Snowball
            129: 4.0,  # Mars - Arena
            # Objective tempo
            53: 3.5,  # Nature's Prophet - split push
            48: 3.5,  # Luna - push
            52: 3.5,  # Leshrac - push
            77: 3.5,  # Lycan - push
        }
        return sum(TEMPO_HEROES.get(h, 0.0) for h in hero_ids)

    def _compute_objective_focus(self, hero_ids: List[int]) -> Dict[str, float]:
        """
        Compute objective focus score.

        Heroes good at taking objectives (towers, rosh).
        High objective = faster game = fewer kills.
        """
        # Tower damage heroes
        TOWER_HEROES: Dict[int, float] = {
            53: 5.0,  # Nature's Prophet - Treants
            77: 5.0,  # Lycan - Howl, wolves
            52: 5.0,  # Leshrac - Edict
            109: 4.5,  # Terrorblade - Metamorphosis
            89: 4.5,  # Naga Siren - Illusions
            48: 4.5,  # Luna - Glaives
            64: 4.0,  # Jakiro - Liquid Fire
            35: 4.0,  # Sniper - range
            81: 4.0,  # Chaos Knight - Phantasm
            70: 3.5,  # Ursa - damage
            54: 3.5,  # Lifestealer - Feast
        }

        # Roshan heroes
        ROSH_HEROES: Dict[int, float] = {
            70: 5.0,  # Ursa - Fury Swipes
            81: 5.0,  # Wraith King - sustain
            54: 4.5,  # Lifestealer - Feast
            109: 4.5,  # Terrorblade - damage
            77: 4.0,  # Lycan - wolves tank
            48: 4.0,  # Luna - damage
            89: 4.0,  # Naga Siren - illusions tank
            67: 3.5,  # Spectre - Desolate
            1: 3.5,  # Anti-Mage - damage
        }

        tower_score = sum(TOWER_HEROES.get(h, 0.0) for h in hero_ids)
        rosh_score = sum(ROSH_HEROES.get(h, 0.0) for h in hero_ids)

        return {
            "tower_score": tower_score,
            "rosh_score": rosh_score,
            "objective_score": tower_score * 0.6 + rosh_score * 0.4,
        }

    def _compute_lane_domination(
        self, team_ids: List[int], enemy_ids: List[int], team_is_radiant: bool = True
    ) -> Dict[str, float]:
        """
        Compute lane domination potential by position.
        Assumes standard positions: [0]=pos1, [1]=pos2, [2]=pos3, [3]=pos4, [4]=pos5
        """
        if len(team_ids) < 5 or len(enemy_ids) < 5:
            return {"safe": 0.0, "mid": 0.0, "off": 0.0, "total": 0.0}

        safe_score = 0.0
        mid_score = self._compute_mid_matchup(team_ids[1], enemy_ids[1], "early")
        off_score = 0.0

        if not self.early_late_counters:
            return {
                "safe": safe_score,
                "mid": mid_score,
                "off": off_score,
                "total": safe_score + mid_score + off_score,
            }

        phase = "early"
        phase_data = self.early_late_counters.get(phase, {})
        counter_1v1 = phase_data.get("counter_1v1", {})
        counter_2v1 = phase_data.get("counter_2v1", {})
        synergy_2 = phase_data.get("synergy_2", {})
        safe_2v2 = phase_data.get("safe_2v2", {})
        off_2v2 = phase_data.get("off_2v2", {})

        def _pair_sorted(a: int, b: int) -> Tuple[int, int]:
            return (a, b) if a <= b else (b, a)

        def _pair_adv_syn(pair: Tuple[int, int]) -> float:
            a, b = _pair_sorted(pair[0], pair[1])
            if a <= 0 or b <= 0:
                return 0.0
            wr = synergy_2.get(f"{a}_{b}")
            if wr is None:
                return 0.0
            return (float(wr) - 0.5) * 100.0

        def _avg_adv_2v1(pair: Tuple[int, int], enemies: Tuple[int, int]) -> Tuple[float, int]:
            a, b = _pair_sorted(pair[0], pair[1])
            if a <= 0 or b <= 0:
                return 0.0, 0
            total = 0.0
            cnt = 0
            for e in enemies:
                if e <= 0:
                    continue
                wr = counter_2v1.get(f"{a}_{b}_vs_{e}")
                if wr is None:
                    continue
                total += (float(wr) - 0.5) * 100.0
                cnt += 1
            if cnt == 0:
                return 0.0, 0
            return total / cnt, cnt

        def _avg_adv_1v1(ours: Tuple[int, int], enemies: Tuple[int, int]) -> Tuple[float, int]:
            total = 0.0
            cnt = 0
            for h in ours:
                if h <= 0:
                    continue
                for e in enemies:
                    if e <= 0:
                        continue
                    wr = counter_1v1.get(f"{h}_vs_{e}")
                    if wr is None:
                        continue
                    total += (float(wr) - 0.5) * 100.0
                    cnt += 1
            if cnt == 0:
                return 0.0, 0
            return total / cnt, cnt

        def _approx_2v2(ours: Tuple[int, int], enemies: Tuple[int, int]) -> float:
            our_2v1, c1 = _avg_adv_2v1(ours, enemies)
            enemy_2v1, c2 = _avg_adv_2v1(enemies, ours)
            syn_adv = _pair_adv_syn(ours) - _pair_adv_syn(enemies)
            if c1 == 0 and c2 == 0:
                adv_1v1, _ = _avg_adv_1v1(ours, enemies)
                return adv_1v1 + syn_adv * 0.5
            return (our_2v1 - enemy_2v1) + syn_adv * 0.5

        def _direct_2v2_score_safe(our_pair: Tuple[int, int], enemy_pair: Tuple[int, int]) -> Optional[float]:
            a, b = _pair_sorted(our_pair[0], our_pair[1])
            c, d = _pair_sorted(enemy_pair[0], enemy_pair[1])
            if a <= 0 or b <= 0 or c <= 0 or d <= 0:
                return None
            if team_is_radiant:
                wr = safe_2v2.get(f"{a}_{b}_vs_{c}_{d}")
                return (float(wr) - 0.5) * 100.0 if wr is not None else None
            wr = off_2v2.get(f"{c}_{d}_vs_{a}_{b}")
            return (0.5 - float(wr)) * 100.0 if wr is not None else None

        def _direct_2v2_score_off(our_pair: Tuple[int, int], enemy_pair: Tuple[int, int]) -> Optional[float]:
            a, b = _pair_sorted(our_pair[0], our_pair[1])
            c, d = _pair_sorted(enemy_pair[0], enemy_pair[1])
            if a <= 0 or b <= 0 or c <= 0 or d <= 0:
                return None
            if team_is_radiant:
                wr = off_2v2.get(f"{a}_{b}_vs_{c}_{d}")
                return (float(wr) - 0.5) * 100.0 if wr is not None else None
            wr = safe_2v2.get(f"{c}_{d}_vs_{a}_{b}")
            return (0.5 - float(wr)) * 100.0 if wr is not None else None

        safe_pair = (team_ids[0], team_ids[4])
        enemy_off_pair = (enemy_ids[2], enemy_ids[3])
        off_pair = (team_ids[2], team_ids[3])
        enemy_safe_pair = (enemy_ids[0], enemy_ids[4])

        safe_direct = _direct_2v2_score_safe(safe_pair, enemy_off_pair)
        safe_approx = _approx_2v2(safe_pair, enemy_off_pair)
        safe_score = safe_approx if safe_direct is None else safe_direct * 0.7 + safe_approx * 0.3

        off_direct = _direct_2v2_score_off(off_pair, enemy_safe_pair)
        off_approx = _approx_2v2(off_pair, enemy_safe_pair)
        off_score = off_approx if off_direct is None else off_direct * 0.7 + off_approx * 0.3

        return {
            "safe": safe_score,
            "mid": mid_score,
            "off": off_score,
            "total": safe_score + mid_score + off_score,
        }

    def _get_draft_execution(
        self, account_ids: Optional[List[int]], min_overlap: int = 3
    ) -> Dict[str, float]:
        """
        Get draft execution stats for a roster.
        Matches rosters with at least min_overlap players in common.
        """
        result = {
            "execution": 0.0,
            "resilience": 0.0,
            "coverage": 0,
        }

        if not self.draft_execution_stats or not account_ids:
            return result

        # Get valid account IDs
        valid_ids = set(int(a) for a in account_ids if a and a > 0)
        if len(valid_ids) < min_overlap:
            return result

        best_match = None
        best_overlap = 0

        for roster_key, stats in self.draft_execution_stats.items():
            roster_players = set(stats.get("players", []))
            overlap = len(valid_ids & roster_players)

            if overlap >= min_overlap and overlap > best_overlap:
                best_overlap = overlap
                best_match = stats

        if best_match:
            result["execution"] = best_match.get("execution_score", 0.0)
            result["resilience"] = best_match.get("resilience_score", 0.0)
            result["coverage"] = best_match.get("games_with_adv", 0) + best_match.get(
                "games_with_disadv", 0
            )

        return result

    def _compute_position_matchup(
        self, radiant_ids: List[int], dire_ids: List[int]
    ) -> Dict[str, float]:
        """
        Compute position-specific matchups.
        Assumes: [0]=pos1(carry), [1]=pos2(mid), [2]=pos3(off), [3]=pos4, [4]=pos5
        """
        if len(radiant_ids) < 5 or len(dire_ids) < 5:
            return {"carry_matchup": 0.0, "mid_matchup": 0.0, "off_matchup": 0.0}

        counter_1v1 = self.early_late_counters.get("early", {}).get("counter_1v1", {})

        # Carry vs Carry (pos1 vs pos1)
        key = f"{radiant_ids[0]}_vs_{dire_ids[0]}"
        carry_wr = counter_1v1.get(key)
        carry_matchup = (carry_wr - 0.5) * 100 if carry_wr else 0.0

        # Mid vs Mid (pos2 vs pos2) - use mid_1v1 data
        mid_matchup = self._compute_mid_matchup(radiant_ids[1], dire_ids[1], "early")

        # Offlane vs Offlane (pos3 vs pos3)
        key = f"{radiant_ids[2]}_vs_{dire_ids[2]}"
        off_wr = counter_1v1.get(key)
        off_matchup = (off_wr - 0.5) * 100 if off_wr else 0.0

        return {
            "carry_matchup": carry_matchup,
            "mid_matchup": mid_matchup,
            "off_matchup": off_matchup,
            "core_matchup_total": carry_matchup + mid_matchup + off_matchup,
        }

    def _compute_late_game_insurance(self, hero_ids: List[int]) -> float:
        """
        Compute late game insurance - heroes that guarantee late game relevance.
        These heroes scale infinitely or have game-winning abilities.
        """
        LATE_INSURANCE: Dict[int, float] = {
            # Infinite scalers
            93: 5.0,  # Slark - essence shift stacks
            54: 5.0,  # Lifestealer - feast %hp
            98: 4.5,  # Ursa - fury swipes
            94: 4.5,  # Medusa - split shot, mana shield
            67: 4.5,  # Spectre - dispersion, desolate
            1: 4.0,  # Anti-Mage - mana break, blink
            81: 4.0,  # Wraith King - reincarnation
            109: 4.0,  # Terrorblade - metamorphosis, sunder
            # Strong late game
            46: 3.5,  # Templar Assassin - refraction
            44: 3.5,  # Phantom Assassin - crit
            6: 3.5,  # Drow Ranger - marksmanship
            41: 3.5,  # Faceless Void - chrono
            89: 3.5,  # Naga Siren - illusions
            12: 3.0,  # Phantom Lancer - illusions
            74: 3.0,  # Invoker - 10 spells
            80: 3.0,  # Tinker - rearm
        }
        return sum(LATE_INSURANCE.get(h, 0.0) for h in hero_ids)

    def _compute_early_game_dominance(self, hero_ids: List[int]) -> float:
        """
        Compute early game dominance - heroes that win lanes and snowball.
        """
        EARLY_DOMINANCE: Dict[int, float] = {
            # Lane dominators
            98: 5.0,  # Ursa - fury swipes
            8: 4.5,  # Juggernaut - spin, healing ward
            93: 4.5,  # Slark - pounce, dark pact
            49: 4.5,  # Dragon Knight - regen, stun
            47: 4.0,  # Viper - poison attack
            40: 4.0,  # Venomancer - poison
            36: 4.0,  # Necrophos - heartstopper
            # Strong early fighters
            2: 4.0,  # Axe - call, helix
            28: 4.0,  # Sand King - burrowstrike
            17: 3.5,  # Storm Spirit - remnant
            39: 3.5,  # Queen of Pain - dagger
            106: 3.5,  # Ember Spirit - chains
            56: 3.5,  # Clinkz - searing arrows
            62: 3.5,  # Bounty Hunter - jinada
        }
        return sum(EARLY_DOMINANCE.get(h, 0.0) for h in hero_ids)

    def _compute_comeback_potential(self, hero_ids: List[int]) -> float:
        """
        Compute comeback potential - heroes that can turn losing games.
        """
        COMEBACK: Dict[int, float] = {
            # Big teamfight ults
            33: 5.0,  # Enigma - Black Hole
            97: 5.0,  # Magnus - RP
            29: 4.5,  # Tidehunter - Ravage
            41: 4.5,  # Faceless Void - Chrono
            110: 4.0,  # Phoenix - Supernova
            89: 4.0,  # Naga Siren - Song reset
            # Late game carries
            67: 4.0,  # Spectre - always relevant
            94: 4.0,  # Medusa - stone gaze
            1: 3.5,  # Anti-Mage - mana void
            109: 3.5,  # Terrorblade - sunder
            # Defensive heroes
            102: 3.5,  # Abaddon - borrowed time
            50: 3.5,  # Dazzle - shallow grave
            111: 3.5,  # Oracle - false promise
        }
        return sum(COMEBACK.get(h, 0.0) for h in hero_ids)

    def _compute_counter_2v1(
        self, team_ids: List[int], enemy_ids: List[int], phase: str = "early"
    ) -> float:
        """
        Compute 2v1 counter score - how well pairs counter single enemies.
        Key format: hero1_hero2_vs_enemy = winrate
        """
        if not self.early_late_counters or len(team_ids) < 2:
            return 0.0

        counter_data = self.early_late_counters.get(phase, {}).get("counter_2v1", {})
        total = 0.0
        count = 0

        # All pairs from team vs each enemy
        for i in range(len(team_ids)):
            for j in range(i + 1, len(team_ids)):
                h1, h2 = min(team_ids[i], team_ids[j]), max(team_ids[i], team_ids[j])
                for enemy in enemy_ids:
                    key = f"{h1}_{h2}_vs_{enemy}"
                    wr = counter_data.get(key)
                    if wr is not None:
                        total += (wr - 0.5) * 100
                        count += 1

        return total / max(count, 1)

    def _compute_counter_1v2(
        self, team_ids: List[int], enemy_ids: List[int], phase: str = "early"
    ) -> float:
        """
        Compute 1v2 counter score - how well single heroes handle enemy pairs.
        This is the inverse of 2v1 - we check enemy pairs vs our heroes.
        """
        if not self.early_late_counters or len(enemy_ids) < 2:
            return 0.0

        counter_data = self.early_late_counters.get(phase, {}).get("counter_2v1", {})
        total = 0.0
        count = 0

        # All enemy pairs vs each of our heroes
        for i in range(len(enemy_ids)):
            for j in range(i + 1, len(enemy_ids)):
                e1, e2 = (
                    min(enemy_ids[i], enemy_ids[j]),
                    max(enemy_ids[i], enemy_ids[j]),
                )
                for hero in team_ids:
                    key = f"{e1}_{e2}_vs_{hero}"
                    wr = counter_data.get(key)
                    if wr is not None:
                        # Inverse: enemy pair winning = bad for us
                        total += (0.5 - wr) * 100
                        count += 1

        return total / max(count, 1)

    # ============ NEW STRATZ-BASED FEATURES ============

    def _get_hero_feature(self, hero_id: int, key: str, default: Any = 0.0) -> Any:
        """Get feature from hero_features_processed.json."""
        hero_data = self.hero_features.get(str(hero_id), {})
        return hero_data.get(key, default)

    def _compute_team_heal_save(self, hero_ids: List[int]) -> float:
        """
        Compute Team Heal/Save score.
        Only counts heroes that are BOTH healers AND supports.
        Huskar (Carry + Heal) = self-sustain, ignored.
        Dazzle (Support + Heal) = team heal, counted.
        """
        total = 0.0
        for hero_id in hero_ids:
            has_heal = self._get_hero_feature(hero_id, "has_heal", False)
            is_support = self._get_hero_feature(hero_id, "is_support", False)

            if has_heal and is_support:
                # Full heal value for support healers
                total += 1.0
            elif has_heal:
                # Partial value for self-sustain (Huskar, Necro, etc.)
                total += 0.3

        return total

    def _compute_team_stun_control(self, hero_ids: List[int]) -> float:
        """
        Compute Team Stun/Control score.
        Counts all heroes with stun, bonus for Disabler role.
        """
        total = 0.0
        for hero_id in hero_ids:
            has_stun = self._get_hero_feature(hero_id, "has_stun", False)
            is_disabler = self._get_hero_feature(hero_id, "is_disabler", False)
            role_levels = self._get_hero_feature(hero_id, "role_levels", {})
            disabler_level = role_levels.get("Disabler", 0)

            if has_stun:
                base_score = 1.0
                # Bonus for strong disablers (level > 1)
                if is_disabler and disabler_level > 1:
                    base_score += 0.5
                total += base_score

        return total

    def _compute_matchup_advantage(
        self, team_ids: List[int], enemy_ids: List[int]
    ) -> float:
        """
        Compute matchup advantage for team vs enemy.
        Sums advantage scores from hero_features matchups.
        Positive = team counters enemy, Negative = enemy counters team.
        """
        total = 0.0
        for hero_id in team_ids:
            matchups = self._get_hero_feature(hero_id, "matchups", {})
            for enemy_id in enemy_ids:
                # matchups[enemy_id] = advantage when hero plays vs enemy
                adv = matchups.get(str(enemy_id), 0.0)
                total += adv
        return total

    def _compute_team_tankiness(self, hero_ids: List[int]) -> float:
        """
        Compute Team Tankiness score.
        Formula: sum(starting_armor + str_gain * 2)
        Higher = harder to kill team.
        """
        total = 0.0
        for hero_id in hero_ids:
            armor = self._get_hero_feature(hero_id, "starting_armor", 0.0)
            str_gain = self._get_hero_feature(hero_id, "str_gain", 0.0)
            total += armor + (str_gain * 2)
        return total

    def _compute_team_burst_rating(self, hero_ids: List[int]) -> float:
        """
        Compute total burst damage rating for team.
        Uses role_levels['Nuker'] >= 2 to filter real nukers (Lion, Lina, Zeus)
        from weak nukers (IO, BB, AM which have level 1).
        Score scales by nuker level: level 2 = 1.0, level 3 = 1.5
        """
        total = 0.0
        for hero_id in hero_ids:
            # First try burst_damage_rating from abilities
            burst = self._get_hero_feature(hero_id, "burst_damage_rating", 0.0)
            if burst > 0:
                total += burst
            else:
                # Fallback: use Nuker role level (>= 2 = real nuker)
                role_levels = self._get_hero_feature(hero_id, "role_levels", {})
                nuker_level = role_levels.get("Nuker", 0)
                if nuker_level >= 2:
                    # Scale by level: 2 -> 1.0, 3 -> 1.5
                    total += nuker_level * 0.5
        return total

    def _compute_lane_matchup_score(
        self, radiant_ids: List[int], dire_ids: List[int]
    ) -> Dict[str, float]:
        """
        Compute lane matchup advantages based on hero_lane_matchups.json.

        Returns aggregated lane dominance scores which predict early kills.
        """
        if not self.hero_lane_matchups:
            return {
                "radiant_lane_adv": 0.0,
                "dire_lane_adv": 0.0,
                "total_lane_volatility": 0.0,
                "stomp_potential": 0.0,
            }

        radiant_adv = 0.0
        dire_adv = 0.0
        stomp_sum = 0.0
        matchup_count = 0

        for r_id in radiant_ids:
            for d_id in dire_ids:
                key = f"{r_id}_{d_id}"
                matchup = self.hero_lane_matchups.get(key, {})
                if matchup and matchup.get("matches", 0) >= 10:
                    # Radiant hero vs Dire hero
                    wr = matchup.get("winrate", 0.5)
                    gold_diff = matchup.get("gold_diff", 0)
                    stomp = matchup.get("stomp_rate", 0)

                    # Positive = radiant advantage
                    radiant_adv += (wr - 0.5) + (gold_diff / 500)
                    stomp_sum += stomp
                    matchup_count += 1

                # Check reverse matchup for dire advantage
                key_rev = f"{d_id}_{r_id}"
                matchup_rev = self.hero_lane_matchups.get(key_rev, {})
                if matchup_rev and matchup_rev.get("matches", 0) >= 10:
                    wr = matchup_rev.get("winrate", 0.5)
                    gold_diff = matchup_rev.get("gold_diff", 0)
                    stomp = matchup_rev.get("stomp_rate", 0)

                    dire_adv += (wr - 0.5) + (gold_diff / 500)
                    stomp_sum += stomp

        return {
            "radiant_lane_adv": radiant_adv,
            "dire_lane_adv": dire_adv,
            "lane_adv_diff": radiant_adv - dire_adv,
            "total_lane_volatility": abs(radiant_adv) + abs(dire_adv),
            "stomp_potential": stomp_sum / max(matchup_count, 1),
        }

    def _compute_team_comeback_stats(self, hero_ids: List[int]) -> Dict[str, float]:
        """
        Compute comeback/stomp potential for a team.

        High comeback = team can play from behind = fewer early kills (careful play)
        High stomp = team snowballs hard = more kills when ahead
        """
        if not self.hero_comeback_stats:
            return {
                "comeback_rate": 0.13,
                "stomp_rate": 0.86,
                "comeback_factor": 0.15,
            }

        comeback_sum = 0.0
        stomp_sum = 0.0
        factor_sum = 0.0
        count = 0

        for hero_id in hero_ids:
            stats = self.hero_comeback_stats.get(str(hero_id), {})
            if stats:
                comeback_sum += stats.get("comeback_rate", 0.13)
                stomp_sum += stats.get("stomp_rate", 0.86)
                factor_sum += stats.get("comeback_factor", 0.15)
                count += 1

        if count == 0:
            return {"comeback_rate": 0.13, "stomp_rate": 0.86, "comeback_factor": 0.15}

        return {
            "comeback_rate": comeback_sum / count,
            "stomp_rate": stomp_sum / count,
            "comeback_factor": factor_sum / count,
        }

    def _compute_timing_clash(
        self, radiant_ids: List[int], dire_ids: List[int]
    ) -> Dict[str, float]:
        """
        Compute timing clash between teams based on power spikes.

        Both early = lots of early fights = more kills
        Early vs Late = one team forces, other avoids = variable
        Both late = farm fest = fewer kills early
        """
        if not self.hero_power_spikes:
            return {
                "radiant_early_count": 0,
                "dire_early_count": 0,
                "timing_clash_score": 0.0,
                "both_early": 0,
                "both_late": 0,
            }

        r_early = 0
        r_late = 0
        d_early = 0
        d_late = 0

        for hero_id in radiant_ids:
            spike = self.hero_power_spikes.get(str(hero_id), {})
            timing = spike.get("spike_timing", "mid")
            if timing == "early":
                r_early += 1
            elif timing == "late":
                r_late += 1

        for hero_id in dire_ids:
            spike = self.hero_power_spikes.get(str(hero_id), {})
            timing = spike.get("spike_timing", "mid")
            if timing == "early":
                d_early += 1
            elif timing == "late":
                d_late += 1

        # Timing clash: both early = high, both late = low, mixed = medium
        both_early = min(r_early, d_early)
        both_late = min(r_late, d_late)

        # Score: positive = more early action expected
        timing_clash = (r_early + d_early) - (r_late + d_late) * 0.5

        return {
            "radiant_early_count": r_early,
            "radiant_late_count": r_late,
            "dire_early_count": d_early,
            "dire_late_count": d_late,
            "timing_clash_score": timing_clash,
            "both_early_heroes": both_early,
            "both_late_heroes": both_late,
            "early_vs_late_mismatch": abs((r_early - r_late) - (d_early - d_late)),
        }

    def _compute_team_evasiveness_v2(self, hero_ids: List[int]) -> Dict[str, float]:
        """Compute detailed evasiveness scores for a team."""
        mobility = sum(HARD_MOBILITY_HEROES.get(h, 0) for h in hero_ids)
        reset = sum(RESET_HEROES.get(h, 0) for h in hero_ids)
        illusion = sum(ILLUSION_HEROES.get(h, 0) for h in hero_ids)
        invis = sum(INVIS_HEROES.get(h, 0) for h in hero_ids)
        return {
            "mobility": mobility,
            "reset": reset,
            "illusion": illusion,
            "invis": invis,
            "total": mobility + reset + illusion + invis,
        }

    def _compute_team_catch(self, hero_ids: List[int]) -> Dict[str, float]:
        """Compute catch/lockdown scores for a team."""
        hex_score = sum(INSTANT_HEX_HEROES.get(h, 0) for h in hero_ids)
        aoe = sum(AOE_LOCKDOWN_HEROES.get(h, 0) for h in hero_ids)
        single = sum(SINGLE_DISABLE_HEROES.get(h, 0) for h in hero_ids)
        silence = sum(SILENCE_HEROES.get(h, 0) for h in hero_ids)
        return {
            "hex": hex_score,
            "aoe_lockdown": aoe,
            "single_disable": single,
            "silence": silence,
            "total": hex_score + aoe + single + silence,
        }

    # ============ NEW DRAFT FEATURES FOR WINRATE ============

    def _compute_stratz_matchup_score(
        self, team_ids: List[int], enemy_ids: List[int]
    ) -> float:
        """
        Compute matchup advantage using Stratz hero matchups data.
        Positive = team counters enemy, Negative = enemy counters team.
        """
        if not self.hero_features:
            return 0.0

        total = 0.0
        count = 0
        for hero_id in team_ids:
            hero_data = self.hero_features.get(str(hero_id), {})
            matchups = hero_data.get("matchups", {})
            for enemy_id in enemy_ids:
                adv = matchups.get(str(enemy_id), 0.0)
                if adv != 0.0:
                    total += adv
                    count += 1

        return total / max(count, 1)

    def _compute_stratz_synergy_score(self, hero_ids: List[int]) -> float:
        """
        Compute synergy score using Stratz hero synergies data.
        Higher = better hero combinations.
        """
        if not self.hero_features or len(hero_ids) < 2:
            return 0.0

        total = 0.0
        count = 0
        for i, hero_id in enumerate(hero_ids):
            hero_data = self.hero_features.get(str(hero_id), {})
            synergies = hero_data.get("synergies", {})
            for j in range(i + 1, len(hero_ids)):
                ally_id = hero_ids[j]
                syn = synergies.get(str(ally_id), 0.0)
                if syn != 0.0:
                    total += syn
                    count += 1

        return total / max(count, 1)

    def _compute_role_composition(self, hero_ids: List[int]) -> Dict[str, float]:
        """
        Compute role composition balance for a team.
        Returns counts and balance scores for key roles.
        """
        roles = {
            "carry": 0,
            "support": 0,
            "nuker": 0,
            "disabler": 0,
            "initiator": 0,
            "durable": 0,
            "pusher": 0,
            "escape": 0,
        }

        for hero_id in hero_ids:
            hero_data = self.hero_features.get(str(hero_id), {})
            if hero_data.get("is_carry"):
                roles["carry"] += 1
            if hero_data.get("is_support"):
                roles["support"] += 1
            if hero_data.get("is_nuker"):
                roles["nuker"] += 1
            if hero_data.get("is_disabler"):
                roles["disabler"] += 1
            if hero_data.get("is_initiator"):
                roles["initiator"] += 1
            if hero_data.get("is_durable"):
                roles["durable"] += 1
            if hero_data.get("is_pusher"):
                roles["pusher"] += 1
            if hero_data.get("has_escape"):
                roles["escape"] += 1

        # Balance scores
        # Good team: 1-2 carries, 2 supports, 1+ disabler, 1+ initiator
        carry_balance = 1.0 if 1 <= roles["carry"] <= 2 else 0.5
        support_balance = 1.0 if roles["support"] >= 2 else 0.5
        disable_balance = 1.0 if roles["disabler"] >= 2 else 0.5

        return {
            **roles,
            "carry_balance": carry_balance,
            "support_balance": support_balance,
            "disable_balance": disable_balance,
            "teamfight_score": roles["disabler"] + roles["initiator"] + roles["nuker"],
            "splitpush_score": roles["pusher"] + roles["escape"],
        }

    def _compute_disable_chain(self, hero_ids: List[int]) -> Dict[str, float]:
        """
        Compute total disable duration and chain potential.
        More stuns = longer disable chains = more kills.
        """
        total_stun = 0.0
        stun_count = 0
        bkb_pierce_count = 0

        for hero_id in hero_ids:
            hero_data = self.hero_features.get(str(hero_id), {})
            stun_dur = hero_data.get("stun_duration", 0.0)
            if stun_dur > 0:
                total_stun += stun_dur
                stun_count += 1
            if hero_data.get("has_bkb_pierce"):
                bkb_pierce_count += 1

        # Chain potential: more stunners = longer chains
        chain_potential = total_stun * (1 + stun_count * 0.1)

        return {
            "total_stun_duration": total_stun,
            "stun_count": stun_count,
            "bkb_pierce_count": bkb_pierce_count,
            "chain_potential": chain_potential,
        }

    def _compute_damage_type_balance(self, hero_ids: List[int]) -> Dict[str, float]:
        """
        Compute physical vs magical damage balance.
        Mixed damage is harder to itemize against.
        """
        physical = 0
        magical = 0

        for hero_id in hero_ids:
            hero_data = self.hero_features.get(str(hero_id), {})
            primary = hero_data.get("primary_attribute", "str")
            is_nuker = hero_data.get("is_nuker", False)

            # Nukers deal magical, AGI carries deal physical
            if is_nuker:
                magical += 1
            if primary == "agi":
                physical += 1
            elif primary == "str":
                physical += 0.5
                magical += 0.5

        # Balance score: 2-3 of each is ideal
        balance = 1.0 - abs(physical - magical) / 5.0

        return {
            "physical_damage": physical,
            "magical_damage": magical,
            "damage_balance": max(0.0, balance),
        }

    def _compute_catch_vs_escape(
        self, team_ids: List[int], enemy_ids: List[int]
    ) -> Dict[str, float]:
        """
        Compute catch vs escape balance.
        Team with more catch vs enemy escape = advantage.
        """
        team_catch = 0.0
        team_escape = 0.0
        enemy_escape = 0.0

        for hero_id in team_ids:
            hero_data = self.hero_features.get(str(hero_id), {})
            if hero_data.get("is_disabler"):
                team_catch += 1.5
            if hero_data.get("has_stun"):
                team_catch += 1.0
            if hero_data.get("has_root"):
                team_catch += 0.5
            if hero_data.get("has_silence"):
                team_catch += 0.5
            if hero_data.get("has_escape"):
                team_escape += 1.0
            team_catch += hero_data.get("stun_duration", 0.0) * 0.3

        for hero_id in enemy_ids:
            hero_data = self.hero_features.get(str(hero_id), {})
            if hero_data.get("has_escape"):
                enemy_escape += 1.0
            enemy_escape += hero_data.get("evasiveness_rating", 0) * 0.2

        return {
            "catch_score": team_catch,
            "escape_score": team_escape,
            "catch_vs_enemy_escape": team_catch - enemy_escape,
        }

    def _compute_scaling_potential(self, hero_ids: List[int]) -> Dict[str, float]:
        """
        Compute late game scaling potential based on stat gains.
        Higher AGI/STR gain = better scaling.
        """
        total_agi_gain = 0.0
        total_str_gain = 0.0
        total_int_gain = 0.0
        carry_scaling = 0.0

        for hero_id in hero_ids:
            hero_data = self.hero_features.get(str(hero_id), {})
            agi_gain = hero_data.get("agi_gain", 2.0)
            str_gain = hero_data.get("str_gain", 2.0)
            int_gain = hero_data.get("int_gain", 2.0)

            total_agi_gain += agi_gain
            total_str_gain += str_gain
            total_int_gain += int_gain

            # Carries with high AGI gain scale better
            if hero_data.get("is_carry") and agi_gain > 2.5:
                carry_scaling += agi_gain

        return {
            "total_agi_gain": total_agi_gain,
            "total_str_gain": total_str_gain,
            "total_int_gain": total_int_gain,
            "carry_scaling": carry_scaling,
            "late_game_scaling": total_agi_gain * 0.4
            + total_str_gain * 0.3
            + total_int_gain * 0.3,
        }

    def _compute_global_presence(self, hero_ids: List[int]) -> float:
        """
        Compute global presence score.
        Heroes with global abilities can influence all lanes.
        """
        global_score = 0.0
        for hero_id in hero_ids:
            hero_data = self.hero_features.get(str(hero_id), {})
            if hero_data.get("has_global"):
                global_score += 2.0
            # Semi-globals (long range abilities)
            if hero_id in {
                53,
                9,
                22,
                75,
                91,
                114,
            }:  # NP, Mirana, Zeus, Silencer, IO, MK
                global_score += 1.0
        return global_score

    def _compute_lane_presence(self, hero_ids: List[int]) -> Dict[str, float]:
        """
        Compute lane presence based on hero attributes.
        Ranged vs melee, armor, attack speed.
        """
        ranged_count = 0
        melee_count = 0
        total_armor = 0.0
        total_attack_range = 0.0

        for hero_id in hero_ids:
            hero_data = self.hero_features.get(str(hero_id), {})
            if hero_data.get("is_melee"):
                melee_count += 1
            else:
                ranged_count += 1
            total_armor += hero_data.get("starting_armor", 0.0)
            total_attack_range += hero_data.get("attack_range", 150)

        return {
            "ranged_count": ranged_count,
            "melee_count": melee_count,
            "total_armor": total_armor,
            "avg_attack_range": total_attack_range / max(len(hero_ids), 1),
            "ranged_advantage": ranged_count - melee_count,
        }

    def _compute_big_ult_synergy(self, hero_ids: List[int]) -> float:
        """
        Compute big ultimate synergy.
        Multiple big ults = wombo combo potential.
        """
        big_ult_count = 0
        for hero_id in hero_ids:
            hero_data = self.hero_features.get(str(hero_id), {})
            if hero_data.get("has_big_ult"):
                big_ult_count += 1
            # Additional big ult heroes not in data
            if hero_id in {
                33,
                97,
                110,
                29,
                41,
                89,
            }:  # Enigma, Magnus, Phoenix, Tide, Void, Naga
                big_ult_count += 1

        # Synergy bonus for multiple big ults
        if big_ult_count >= 3:
            return big_ult_count * 1.5
        elif big_ult_count >= 2:
            return big_ult_count * 1.2
        return float(big_ult_count)

    def build_winrate_features(
        self,
        radiant_ids: List[int],
        dire_ids: List[int],
        radiant_team_id: Optional[int] = None,
        dire_team_id: Optional[int] = None,
        radiant_account_ids: Optional[List[int]] = None,
        dire_account_ids: Optional[List[int]] = None,
        # Optional pre-game side context (only if you have it in the calling pipeline).
        # If unknown, keep None and the downstream predictor will default/ignore.
        radiant_is_radiant_side: Optional[int] = None,
        first_pick_team: Optional[str] = None,  # "RADIANT" | "DIRE" | None
    ) -> Dict[str, Any]:
        """
        Build a minimal, stable pre-game feature set for winrate prediction.

        Design goals:
        - Avoid sparse/optional context that is frequently missing in production (league/h2h/series).
        - Avoid player/DNA dependencies (low coverage).
        - Keep only features that can be computed deterministically from the draft + optional team ids/ratings.

        NOTE:
        This method returns a feature dict suitable for feeding into predict_winrate(), which will
        select and order columns based on the loaded model's meta feature list.
        """
        # Start from core draft features using existing implementation to avoid divergence.
        # We intentionally pass no accounts and no extra context fields.
        features = self.build_features(
            radiant_ids=radiant_ids,
            dire_ids=dire_ids,
            radiant_account_ids=radiant_account_ids,
            dire_account_ids=dire_account_ids,
            radiant_team_id=radiant_team_id,
            dire_team_id=dire_team_id,
            h2h_avg_total=None,
            h2h_matches_count=None,
            league_avg_kills=None,
            league_kills_std=None,
            league_meta_diff=None,
            series_game_num=None,
            is_decider_game=None,
            combined_form_kills=None,
            combined_team_avg_kills=None,
            combined_team_aggression=None,
            combined_synthetic_kills=None,
        )

        # Add stable team rating context (pre-game, if team_id present in team_ratings.json)
        r_rating, r_rd, _ = self._get_team_rating(radiant_team_id)
        d_rating, d_rd, _ = self._get_team_rating(dire_team_id)
        features["radiant_glicko_rating"] = r_rating
        features["dire_glicko_rating"] = d_rating
        features["glicko_rating_diff"] = r_rating - d_rating
        features["radiant_glicko_rd"] = r_rd
        features["dire_glicko_rd"] = d_rd
        features["glicko_rating_win_prob"] = self._calculate_rating_win_prob(
            r_rating, r_rd, d_rating, d_rd
        )
        features["both_teams_reliable"] = 1.0 if r_rd < 150 and d_rd < 150 else 0.0

        # Add stable tier signals (pre-game)
        r_tier = self._get_team_tier_numeric(radiant_team_id)
        d_tier = self._get_team_tier_numeric(dire_team_id)
        features["radiant_tier"] = r_tier
        features["dire_tier"] = d_tier
        features["avg_tier"] = (r_tier + d_tier) / 2
        features["tier_diff"] = abs(r_tier - d_tier)
        features["both_tier1"] = 1.0 if r_tier == 1 and d_tier == 1 else 0.0
        features["tier1_vs_other"] = 1.0 if (r_tier == 1) != (d_tier == 1) else 0.0
        features["both_tier2_plus"] = 1.0 if r_tier >= 2 and d_tier >= 2 else 0.0
        # Match-tier aggregates (align with process_pro_data)
        features["tier_mismatch"] = abs(r_tier - d_tier)
        features["match_tier_score"] = (r_tier + d_tier) / 2
        features["is_elite_match"] = 1.0 if r_tier == 1 and d_tier == 1 else 0.0
        features["is_tier2_match"] = 1.0 if r_tier == 2 and d_tier == 2 else 0.0
        features["is_mismatch_match"] = 1.0 if abs(r_tier - d_tier) >= 1 else 0.0
        features["tier_mismatch_known"] = (
            1.0 if (r_tier <= 2 and d_tier <= 2 and r_tier != d_tier) else 0.0
        )
        features["tier1_vs_tier2"] = (
            1.0 if (r_tier == 1 and d_tier == 2) or (r_tier == 2 and d_tier == 1) else 0.0
        )

        # ===== Side / first-pick context (optional, stable) =====
        # These are intentionally simple binary flags. If the caller can't provide them,
        # we keep them as None and downstream code will default them to 0.0.
        if radiant_is_radiant_side is None:
            # In Dota, "radiant_ids" are already radiant side in match representation;
            # but some upstream systems may swap for "team A/B". Allow explicit override.
            features["radiant_is_radiant_side"] = 1
        else:
            features["radiant_is_radiant_side"] = (
                1 if int(radiant_is_radiant_side) else 0
            )

        fp = str(first_pick_team).upper() if first_pick_team is not None else None
        features["first_pick_radiant"] = 1 if fp == "RADIANT" else 0
        features["first_pick_dire"] = 1 if fp == "DIRE" else 0
        features["first_pick_known"] = 1 if fp in ("RADIANT", "DIRE") else 0

        # ===== Simple team strength priors (stable) =====
        # Provide bounded, smooth transforms that generalize better than raw diffs.
        # These can help coverage by giving the model a reliable baseline when draft features are ambiguous.
        features["glicko_rating_diff_abs"] = abs(
            features.get("glicko_rating_diff", 0.0) or 0.0
        )
        features["glicko_rd_sum"] = (
            features.get("radiant_glicko_rd", 350.0) or 350.0
        ) + (features.get("dire_glicko_rd", 350.0) or 350.0)
        features["glicko_reliability_score"] = 1.0 / max(
            1.0, features["glicko_rd_sum"] / 100.0
        )

        # Tier advantage signals (coarse, stable)
        # Note: smaller tier number is stronger (1 is best).
        features["tier_advantage_radiant"] = 1.0 if r_tier < d_tier else 0.0
        features["tier_advantage_dire"] = 1.0 if d_tier < r_tier else 0.0
        features["tier_equal"] = 1.0 if r_tier == d_tier else 0.0
        features["tier_gap_signed"] = float(r_tier - d_tier)
        features["tier_mismatch_known"] = (
            1.0 if r_tier <= 2 and d_tier <= 2 and r_tier != d_tier else 0.0
        )
        features["tier1_vs_tier2"] = (
            1.0 if {r_tier, d_tier} == {1, 2} else 0.0
        )

        return features

    def build_features(
        self,
        radiant_ids: List[int],
        dire_ids: List[int],
        radiant_account_ids: Optional[List[int]] = None,
        dire_account_ids: Optional[List[int]] = None,
        radiant_team_id: Optional[int] = None,
        dire_team_id: Optional[int] = None,
        # Extra context features (for extreme classifier)
        h2h_avg_total: Optional[float] = None,
        h2h_matches_count: Optional[int] = None,
        league_avg_kills: Optional[float] = None,
        league_kills_std: Optional[float] = None,
        league_meta_diff: Optional[float] = None,
        series_game_num: Optional[int] = None,
        is_decider_game: Optional[int] = None,
        combined_form_kills: Optional[float] = None,
        combined_team_avg_kills: Optional[float] = None,
        combined_team_aggression: Optional[float] = None,
        combined_synthetic_kills: Optional[float] = None,
        match_start_time: Optional[int] = None,
        league_id: Optional[int] = None,
        series_type: Optional[str] = None,
        region_id: Optional[int] = None,
        tournament_tier: Optional[int] = None,
        combined_patch_form_kills: Optional[float] = None,
        combined_patch_team_avg_kills: Optional[float] = None,
        combined_patch_team_aggression: Optional[float] = None,
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
        """
        Строит фичи для предсказания на основе драфта.
        Включает все 124 draft-only фичи для совместимости с моделью v2.
        Для CatBoost добавляет hero IDs и team IDs как категориальные фичи.
        """
        features: Dict[str, Any] = {}

        # ============ HERO IDs (for CatBoost categorical features) ============
        for i, hero_id in enumerate(radiant_ids[:5]):
            features[f"radiant_hero_{i + 1}"] = hero_id
        for i, hero_id in enumerate(dire_ids[:5]):
            features[f"dire_hero_{i + 1}"] = hero_id

        # Pad with -1 if less than 5 heroes
        for i in range(len(radiant_ids), 5):
            features[f"radiant_hero_{i + 1}"] = -1
        for i in range(len(dire_ids), 5):
            features[f"dire_hero_{i + 1}"] = -1

        # ============ TEAM IDs (for CatBoost categorical features) ============
        features["radiant_team_id"] = radiant_team_id if radiant_team_id else -1
        features["dire_team_id"] = dire_team_id if dire_team_id else -1

        # ============ MATCH META (league/series/region) ============
        features["league_id"] = league_id if league_id is not None else -1
        features["series_type"] = series_type if series_type is not None else "UNKNOWN"
        features["region_id"] = region_id if region_id is not None else -1

        # ============ ROSTER STABILITY (prev match, no-leak) ============
        r_shared = 0.0 if radiant_roster_shared_prev is None else float(radiant_roster_shared_prev)
        d_shared = 0.0 if dire_roster_shared_prev is None else float(dire_roster_shared_prev)
        r_changed = 0.0 if radiant_roster_changed_prev is None else float(radiant_roster_changed_prev)
        d_changed = 0.0 if dire_roster_changed_prev is None else float(dire_roster_changed_prev)
        r_stable = 0.0 if radiant_roster_stable_prev is None else float(radiant_roster_stable_prev)
        d_stable = 0.0 if dire_roster_stable_prev is None else float(dire_roster_stable_prev)
        r_new = 0.0 if radiant_roster_new_team is None else float(radiant_roster_new_team)
        d_new = 0.0 if dire_roster_new_team is None else float(dire_roster_new_team)
        r_group = 0.0 if radiant_roster_group_matches is None else float(radiant_roster_group_matches)
        d_group = 0.0 if dire_roster_group_matches is None else float(dire_roster_group_matches)
        r_count = 0.0 if radiant_roster_player_count is None else float(radiant_roster_player_count)
        d_count = 0.0 if dire_roster_player_count is None else float(dire_roster_player_count)

        features["radiant_roster_shared_prev"] = r_shared
        features["dire_roster_shared_prev"] = d_shared
        features["radiant_roster_changed_prev"] = r_changed
        features["dire_roster_changed_prev"] = d_changed
        features["radiant_roster_stable_prev"] = r_stable
        features["dire_roster_stable_prev"] = d_stable
        features["radiant_roster_new_team"] = r_new
        features["dire_roster_new_team"] = d_new
        features["radiant_roster_group_matches"] = r_group
        features["dire_roster_group_matches"] = d_group
        features["radiant_roster_player_count"] = r_count
        features["dire_roster_player_count"] = d_count
        features["combined_roster_shared_prev"] = r_shared + d_shared
        features["roster_shared_diff"] = r_shared - d_shared
        features["combined_roster_changed_prev"] = r_changed + d_changed
        features["roster_changed_diff"] = r_changed - d_changed
        features["both_roster_stable_prev"] = 1.0 if r_stable >= 1 and d_stable >= 1 else 0.0
        features["any_roster_new_team"] = 1.0 if r_new >= 1 or d_new >= 1 else 0.0
        features["both_roster_new_team"] = 1.0 if r_new >= 1 and d_new >= 1 else 0.0
        features["roster_stable_diff"] = r_stable - d_stable
        features["combined_roster_group_matches"] = r_group + d_group
        features["avg_roster_group_matches"] = (r_group + d_group) / 2.0
        features["roster_group_matches_diff"] = r_group - d_group
        features["combined_roster_player_count"] = r_count + d_count
        features["roster_player_count_diff"] = r_count - d_count

        # ============ TEAM GLICKO RATINGS ============
        r_rating, r_rd, _ = self._get_team_rating(radiant_team_id)
        d_rating, d_rd, _ = self._get_team_rating(dire_team_id)
        features["radiant_glicko_rating"] = r_rating
        features["dire_glicko_rating"] = d_rating
        features["glicko_rating_diff"] = r_rating - d_rating
        features["radiant_glicko_rd"] = r_rd
        features["dire_glicko_rd"] = d_rd
        features["glicko_rating_win_prob"] = self._calculate_rating_win_prob(
            r_rating, r_rd, d_rating, d_rd
        )
        features["both_teams_reliable"] = 1.0 if r_rd < 150 and d_rd < 150 else 0.0

        # ============ MATCH TIER & GLICKO TIER ============
        avg_glicko = (r_rating + d_rating) / 2
        features["avg_glicko_rating"] = avg_glicko

        # Glicko tier: High (>1700), Mid (1550-1700), Low (<1550)
        if avg_glicko >= 1700:
            features["glicko_tier"] = 2  # High
        elif avg_glicko >= 1550:
            features["glicko_tier"] = 1  # Mid
        else:
            features["glicko_tier"] = 0  # Low

        # Match tier based on team tiers
        r_tier = self._get_team_tier_numeric(radiant_team_id)
        d_tier = self._get_team_tier_numeric(dire_team_id)
        features["radiant_tier"] = r_tier
        features["dire_tier"] = d_tier
        features["avg_tier"] = (r_tier + d_tier) / 2
        features["tier_diff"] = abs(r_tier - d_tier)
        features["both_tier1"] = 1.0 if r_tier == 1 and d_tier == 1 else 0.0
        features["tier1_vs_other"] = 1.0 if (r_tier == 1) != (d_tier == 1) else 0.0
        features["both_tier2_plus"] = 1.0 if r_tier >= 2 and d_tier >= 2 else 0.0

        # ============ BLOOD STATS (absolute) ============
        radiant_blood = self._compute_blood_score(radiant_ids)
        dire_blood = self._compute_blood_score(dire_ids)
        radiant_blood_syn = self._compute_blood_synergy(radiant_ids)
        dire_blood_syn = self._compute_blood_synergy(dire_ids)
        match_clash = self._compute_match_blood_clash(radiant_ids, dire_ids)

        features["radiant_blood_score"] = radiant_blood
        features["dire_blood_score"] = dire_blood
        features["combined_blood_score"] = radiant_blood + dire_blood
        features["blood_score_diff"] = radiant_blood - dire_blood
        features["radiant_blood_synergy"] = radiant_blood_syn
        features["dire_blood_synergy"] = dire_blood_syn
        features["combined_blood_synergy"] = radiant_blood_syn + dire_blood_syn
        features["blood_synergy_diff"] = radiant_blood_syn - dire_blood_syn
        features["match_blood_clash"] = match_clash
        features["total_blood_potential"] = (
            radiant_blood
            + dire_blood
            + radiant_blood_syn
            + dire_blood_syn
            + match_clash
        )

        # ============ BLOOD STATS (per minute) ============
        radiant_blood_pm = self._compute_blood_score_pm(radiant_ids)
        dire_blood_pm = self._compute_blood_score_pm(dire_ids)
        radiant_blood_syn_pm = self._compute_blood_synergy_pm(radiant_ids)
        dire_blood_syn_pm = self._compute_blood_synergy_pm(dire_ids)
        match_clash_pm = self._compute_match_blood_clash_pm(radiant_ids, dire_ids)

        features["radiant_blood_score_pm"] = radiant_blood_pm
        features["dire_blood_score_pm"] = dire_blood_pm
        features["combined_blood_score_pm"] = radiant_blood_pm + dire_blood_pm
        features["blood_score_diff_pm"] = radiant_blood_pm - dire_blood_pm
        features["radiant_blood_synergy_pm"] = radiant_blood_syn_pm
        features["dire_blood_synergy_pm"] = dire_blood_syn_pm
        features["combined_blood_synergy_pm"] = radiant_blood_syn_pm + dire_blood_syn_pm
        features["blood_synergy_diff_pm"] = radiant_blood_syn_pm - dire_blood_syn_pm
        features["match_blood_clash_pm"] = match_clash_pm
        features["total_blood_potential_pm"] = (
            radiant_blood_pm
            + dire_blood_pm
            + radiant_blood_syn_pm
            + dire_blood_syn_pm
            + match_clash_pm
        )

        # Blood flags
        features["high_blood_match"] = (
            1.0 if (radiant_blood + dire_blood) > 0.5 else 0.0
        )
        features["low_blood_match"] = (
            1.0 if (radiant_blood + dire_blood) < -0.5 else 0.0
        )

        # ============ HERO AGGRESSION/FEED AGGREGATES ============
        # Sum of per-hero aggression and feed stats
        r_agg_sum = sum(
            self._get_hero_stat(self.complex_hero_stats, h, "aggression", 0.35)
            for h in radiant_ids
        )
        d_agg_sum = sum(
            self._get_hero_stat(self.complex_hero_stats, h, "aggression", 0.35)
            for h in dire_ids
        )
        r_feed_sum = sum(
            self._get_hero_stat(self.complex_hero_stats, h, "feed", 0.1)
            for h in radiant_ids
        )
        d_feed_sum = sum(
            self._get_hero_stat(self.complex_hero_stats, h, "feed", 0.1)
            for h in dire_ids
        )

        features["radiant_total_aggression"] = r_agg_sum
        features["dire_total_aggression"] = d_agg_sum
        features["combined_aggression"] = r_agg_sum + d_agg_sum
        features["radiant_avg_aggression"] = r_agg_sum / max(len(radiant_ids), 1)
        features["dire_avg_aggression"] = d_agg_sum / max(len(dire_ids), 1)

        features["radiant_total_feed"] = r_feed_sum
        features["dire_total_feed"] = d_feed_sum
        features["combined_feed"] = r_feed_sum + d_feed_sum
        features["radiant_avg_feed"] = r_feed_sum / max(len(radiant_ids), 1)
        features["dire_avg_feed"] = d_feed_sum / max(len(dire_ids), 1)

        # ============ BURST VS HEAL ============
        r_burst = self._compute_team_burst_rating(radiant_ids)
        d_burst = self._compute_team_burst_rating(dire_ids)
        features["radiant_burst_rating"] = r_burst
        features["dire_burst_rating"] = d_burst
        features["combined_burst_rating"] = r_burst + d_burst

        # ============ CC STATS ============
        radiant_cc = self._compute_team_cc_score(radiant_ids)
        dire_cc = self._compute_team_cc_score(dire_ids)
        features["radiant_cc_score"] = radiant_cc
        features["dire_cc_score"] = dire_cc
        features["combined_cc_score"] = radiant_cc + dire_cc
        features["cc_score_diff"] = radiant_cc - dire_cc

        # ============ GREED INDEX ============
        radiant_greed = self._compute_team_greed(radiant_ids)
        dire_greed = self._compute_team_greed(dire_ids)
        features["radiant_greed_index"] = radiant_greed
        features["dire_greed_index"] = dire_greed
        features["combined_greed"] = radiant_greed + dire_greed
        features["greed_diff"] = radiant_greed - dire_greed

        # Greedy counts
        radiant_greedy = sum(self._get_hero_is_greedy(h) for h in radiant_ids)
        dire_greedy = sum(self._get_hero_is_greedy(h) for h in dire_ids)
        features["radiant_greedy_count"] = radiant_greedy
        features["dire_greedy_count"] = dire_greedy
        features["combined_greedy"] = radiant_greedy + dire_greedy
        features["greedy_diff"] = radiant_greedy - dire_greedy
        features["both_greedy"] = (
            1.0 if radiant_greedy >= 2 and dire_greedy >= 2 else 0.0
        )
        features["both_greedy_carries"] = (
            1.0 if radiant_greedy >= 1 and dire_greedy >= 1 else 0.0
        )
        features["high_greed_match"] = (
            1.0 if (radiant_greed + dire_greed) > 1.2 else 0.0
        )
        features["greedy_vs_active"] = radiant_greedy - dire_greedy

        # ============ PUSH STATS ============
        radiant_push = self._compute_team_push_score(radiant_ids)
        dire_push = self._compute_team_push_score(dire_ids)
        features["radiant_push_score"] = radiant_push
        features["dire_push_score"] = dire_push
        features["combined_push_score"] = radiant_push + dire_push
        features["push_score_diff"] = radiant_push - dire_push

        # Pusher counts
        radiant_pushers = sum(
            1
            for h in radiant_ids
            if self._get_hero_stat(self.hero_push_stats, h, "push_score", 0) > 1.0
        )
        dire_pushers = sum(
            1
            for h in dire_ids
            if self._get_hero_stat(self.hero_push_stats, h, "push_score", 0) > 1.0
        )
        features["combined_pushers"] = radiant_pushers + dire_pushers

        # ============ HEAL STATS ============
        radiant_heal = self._compute_team_heal_score(radiant_ids)
        dire_heal = self._compute_team_heal_score(dire_ids)
        features["radiant_heal_score"] = radiant_heal
        features["dire_heal_score"] = dire_heal
        features["combined_heal_score"] = radiant_heal + dire_heal
        features["heal_score_diff"] = radiant_heal - dire_heal

        # Heal per min - removed hardcoded 38 min, use raw heal scores instead
        # (model will learn the relationship)

        # Burst vs Heal ratio (now that we have both)
        features["burst_vs_heal_total"] = features["combined_burst_rating"] - (
            radiant_heal + dire_heal
        )

        # ============ WAVE CLEAR ============
        radiant_wave = self._compute_team_wave_clear(radiant_ids)
        dire_wave = self._compute_team_wave_clear(dire_ids)
        features["radiant_wave_clear"] = radiant_wave
        features["dire_wave_clear"] = dire_wave
        features["combined_wave_clear"] = radiant_wave + dire_wave
        features["wave_clear_diff"] = radiant_wave - dire_wave

        # Push/wave ratio
        features["push_wave_clear_ratio"] = (radiant_push + dire_push) / max(
            radiant_wave + dire_wave, 0.1
        )

        # ============ POWER SPIKES ============
        radiant_early = self._compute_team_early_power(radiant_ids)
        radiant_late = self._compute_team_late_power(radiant_ids)
        dire_early = self._compute_team_early_power(dire_ids)
        dire_late = self._compute_team_late_power(dire_ids)

        features["radiant_early_power"] = radiant_early
        features["radiant_late_power"] = radiant_late
        features["dire_early_power"] = dire_early
        features["dire_late_power"] = dire_late
        features["combined_early_power"] = radiant_early + dire_early
        features["combined_late_power"] = radiant_late + dire_late
        features["early_power_diff"] = radiant_early - dire_early
        features["late_power_diff"] = radiant_late - dire_late

        # ============ SYNERGY (from hero_synergy.json) ============
        radiant_syn = self._compute_synergy_score(radiant_ids)
        dire_syn = self._compute_synergy_score(dire_ids)
        radiant_early_syn = self._compute_early_synergy(radiant_ids)
        dire_early_syn = self._compute_early_synergy(dire_ids)
        radiant_late_syn = self._compute_late_synergy(radiant_ids)
        dire_late_syn = self._compute_late_synergy(dire_ids)

        features["radiant_draft_synergy"] = radiant_syn
        features["dire_draft_synergy"] = dire_syn
        features["total_draft_synergy"] = radiant_syn + dire_syn
        features["draft_synergy_diff"] = radiant_syn - dire_syn
        features["radiant_early_synergy"] = radiant_early_syn
        features["dire_early_synergy"] = dire_early_syn
        features["combined_early_synergy"] = radiant_early_syn + dire_early_syn
        features["early_synergy_diff"] = radiant_early_syn - dire_early_syn
        features["radiant_late_synergy"] = radiant_late_syn
        features["dire_late_synergy"] = dire_late_syn
        features["combined_late_synergy"] = radiant_late_syn + dire_late_syn
        features["late_synergy_diff"] = radiant_late_syn - dire_late_syn

        # ============ STRATZ-BASED FEATURES (from hero_features_processed.json) ============

        # Team Heal/Save (only support healers count fully)
        radiant_team_heal = self._compute_team_heal_save(radiant_ids)
        dire_team_heal = self._compute_team_heal_save(dire_ids)
        features["radiant_team_heal_save"] = radiant_team_heal
        features["dire_team_heal_save"] = dire_team_heal
        features["combined_team_heal_save"] = radiant_team_heal + dire_team_heal
        features["team_heal_save_diff"] = radiant_team_heal - dire_team_heal

        # Team Stun/Control (all stunners, bonus for disablers)
        radiant_stun_control = self._compute_team_stun_control(radiant_ids)
        dire_stun_control = self._compute_team_stun_control(dire_ids)
        features["radiant_stun_control"] = radiant_stun_control
        features["dire_stun_control"] = dire_stun_control
        features["combined_stun_control"] = radiant_stun_control + dire_stun_control
        features["stun_control_diff"] = radiant_stun_control - dire_stun_control

        # Matchup Advantage (counter-pick score)
        radiant_matchup_adv = self._compute_matchup_advantage(radiant_ids, dire_ids)
        dire_matchup_adv = self._compute_matchup_advantage(dire_ids, radiant_ids)
        features["radiant_matchup_advantage"] = radiant_matchup_adv
        features["dire_matchup_advantage"] = dire_matchup_adv
        features["matchup_advantage_diff"] = radiant_matchup_adv - dire_matchup_adv
        features["total_matchup_volatility"] = abs(radiant_matchup_adv) + abs(
            dire_matchup_adv
        )

        # Team Tankiness (armor + str_gain)
        radiant_tankiness = self._compute_team_tankiness(radiant_ids)
        dire_tankiness = self._compute_team_tankiness(dire_ids)
        features["radiant_tankiness"] = radiant_tankiness
        features["dire_tankiness"] = dire_tankiness
        features["combined_tankiness"] = radiant_tankiness + dire_tankiness
        features["tankiness_diff"] = radiant_tankiness - dire_tankiness

        # Team Burst Rating (ability damage / cooldown)
        radiant_burst = self._compute_team_burst_rating(radiant_ids)
        dire_burst = self._compute_team_burst_rating(dire_ids)
        features["radiant_burst_rating"] = radiant_burst
        features["dire_burst_rating"] = dire_burst
        features["combined_burst_rating"] = radiant_burst + dire_burst
        features["burst_rating_diff"] = radiant_burst - dire_burst

        # Burst vs Tankiness ratio (high burst vs low tank = more kills)
        features["burst_vs_tankiness_ratio"] = (radiant_burst + dire_burst) / max(
            radiant_tankiness + dire_tankiness, 1.0
        )

        # ============ SUPER FEATURES: MELEE & ESCAPE ============
        # Total melee heroes (hypothesis: 8+ melee = guaranteed TB over)
        radiant_melee = sum(
            1 for hid in radiant_ids if self._get_hero_feature(hid, "is_melee", False)
        )
        dire_melee = sum(
            1 for hid in dire_ids if self._get_hero_feature(hid, "is_melee", False)
        )
        features["radiant_melee_count"] = radiant_melee
        features["dire_melee_count"] = dire_melee
        features["total_melee_heroes"] = radiant_melee + dire_melee
        features["melee_diff"] = radiant_melee - dire_melee

        # Escape score (heroes that can disengage = fewer kills)
        radiant_escape = sum(
            1 for hid in radiant_ids if self._get_hero_feature(hid, "has_escape", False)
        )
        dire_escape = sum(
            1 for hid in dire_ids if self._get_hero_feature(hid, "has_escape", False)
        )
        features["radiant_escape_score"] = radiant_escape
        features["dire_escape_score"] = dire_escape
        features["combined_escape_score"] = radiant_escape + dire_escape
        features["escape_diff"] = radiant_escape - dire_escape

        # Save heroes (can prevent kills) - using SAVE_HEROES scores
        radiant_save_score = sum(SAVE_HEROES.get(hid, 0.0) for hid in radiant_ids)
        dire_save_score = sum(SAVE_HEROES.get(hid, 0.0) for hid in dire_ids)
        features["radiant_save_score"] = radiant_save_score
        features["dire_save_score"] = dire_save_score
        features["combined_save_score"] = radiant_save_score + dire_save_score
        features["save_score_diff"] = radiant_save_score - dire_save_score

        # Also keep count for backward compatibility
        radiant_save = sum(1 for hid in radiant_ids if SAVE_HEROES.get(hid, 0) > 0)
        dire_save = sum(1 for hid in dire_ids if SAVE_HEROES.get(hid, 0) > 0)
        features["radiant_save_heroes"] = radiant_save
        features["dire_save_heroes"] = dire_save
        features["combined_save_heroes"] = radiant_save + dire_save

        # ============ EVASIVENESS RATING (0-3 scale) ============
        # Level 3 = impossible to catch (AM, Puck, Weaver, Storm, Ember, Tinker, Slark, Riki, Morph, Void Spirit)
        # Level 2 = hard to catch (Naga, PL, Mirana, QoP, WR, DS, BH, Clinkz, Nyx, Hoodwink, etc.)
        # Level 1 = can survive (Jugg, LS, Oracle, Dazzle, WW, Abaddon, Omni, etc.)
        # Level 0 = sitting ducks (default)
        radiant_evasiveness = sum(
            self._get_hero_feature(hid, "evasiveness_rating", 0) for hid in radiant_ids
        )
        dire_evasiveness = sum(
            self._get_hero_feature(hid, "evasiveness_rating", 0) for hid in dire_ids
        )
        features["radiant_evasiveness"] = radiant_evasiveness
        features["dire_evasiveness"] = dire_evasiveness
        features["total_match_evasiveness"] = radiant_evasiveness + dire_evasiveness
        features["evasiveness_diff"] = radiant_evasiveness - dire_evasiveness

        # High evasiveness = fewer kills (hard to catch heroes)
        features["high_evasiveness_match"] = (
            1.0 if (radiant_evasiveness + dire_evasiveness) >= 8 else 0.0
        )

        # ============ ENHANCED EVASIVENESS V2 (for regression model) ============
        r_evas = self._compute_team_evasiveness_v2(radiant_ids)
        d_evas = self._compute_team_evasiveness_v2(dire_ids)
        r_catch = self._compute_team_catch(radiant_ids)
        d_catch = self._compute_team_catch(dire_ids)

        features["radiant_mobility"] = r_evas["mobility"]
        features["dire_mobility"] = d_evas["mobility"]
        features["radiant_reset"] = r_evas["reset"]
        features["dire_reset"] = d_evas["reset"]
        features["radiant_illusion"] = r_evas["illusion"]
        features["dire_illusion"] = d_evas["illusion"]
        features["radiant_invis"] = r_evas["invis"]
        features["dire_invis"] = d_evas["invis"]
        features["radiant_evasiveness_v2"] = r_evas["total"]
        features["dire_evasiveness_v2"] = d_evas["total"]
        features["total_evasiveness_v2"] = r_evas["total"] + d_evas["total"]

        features["radiant_hex"] = r_catch["hex"]
        features["dire_hex"] = d_catch["hex"]
        features["radiant_aoe_lockdown"] = r_catch["aoe_lockdown"]
        features["dire_aoe_lockdown"] = d_catch["aoe_lockdown"]
        features["radiant_single_disable"] = r_catch["single_disable"]
        features["dire_single_disable"] = d_catch["single_disable"]
        features["radiant_silence"] = r_catch["silence"]
        features["dire_silence"] = d_catch["silence"]
        features["radiant_catch"] = r_catch["total"]
        features["dire_catch"] = d_catch["total"]
        features["total_catch"] = r_catch["total"] + d_catch["total"]

        # Catch vs Evasiveness ratios
        features["radiant_catch_ratio"] = r_catch["total"] / max(d_evas["total"], 0.1)
        features["dire_catch_ratio"] = d_catch["total"] / max(r_evas["total"], 0.1)
        features["total_catch_ratio"] = (
            features["radiant_catch_ratio"] + features["dire_catch_ratio"]
        ) / 2
        features["radiant_kill_potential"] = r_catch["total"] - d_evas["total"]
        features["dire_kill_potential"] = d_catch["total"] - r_evas["total"]
        features["total_kill_potential"] = (
            features["radiant_kill_potential"] + features["dire_kill_potential"]
        )
        features["total_reset"] = r_evas["reset"] + d_evas["reset"]
        features["high_reset_match"] = 1.0 if features["total_reset"] >= 8 else 0.0

        # ============ PLAYER DNA FEATURES ============
        r_dna = self._compute_team_dna(radiant_account_ids)
        d_dna = self._compute_team_dna(dire_account_ids)

        # Check if DNA data is available for both teams
        features["_dna_complete"] = r_dna is not None and d_dna is not None

        # Use empty defaults only for feature structure (will be flagged as incomplete)
        if r_dna is None:
            r_dna = {
                "avg_kills": 0.0,
                "avg_deaths": 0.0,
                "aggression": 0.0,
                "pace": 0.0,
                "feed": 0.0,
                "avg_duration": 0.0,
                "kda": 0.0,
                "coverage": 0,
                "recent_aggression": 0.0,
                "aggression_on_aggro": 0.0,
                "aggression_on_passive": 0.0,
                "aggression_delta": 0.0,
            }
        if d_dna is None:
            d_dna = {
                "avg_kills": 0.0,
                "avg_deaths": 0.0,
                "aggression": 0.0,
                "pace": 0.0,
                "feed": 0.0,
                "avg_duration": 0.0,
                "kda": 0.0,
                "coverage": 0,
                "recent_aggression": 0.0,
                "aggression_on_aggro": 0.0,
                "aggression_on_passive": 0.0,
                "aggression_delta": 0.0,
            }

        features["radiant_dna_avg_kills"] = r_dna["avg_kills"]
        features["radiant_dna_avg_deaths"] = r_dna["avg_deaths"]
        features["radiant_dna_aggression"] = r_dna["aggression"]
        features["radiant_dna_pace"] = r_dna["pace"]
        features["radiant_dna_feed"] = r_dna["feed"]
        features["radiant_dna_avg_duration"] = r_dna["avg_duration"]
        features["radiant_dna_kda"] = r_dna["kda"]
        features["radiant_dna_coverage"] = r_dna["coverage"]

        features["dire_dna_avg_kills"] = d_dna["avg_kills"]
        features["dire_dna_avg_deaths"] = d_dna["avg_deaths"]
        features["dire_dna_aggression"] = d_dna["aggression"]
        features["dire_dna_pace"] = d_dna["pace"]
        features["dire_dna_feed"] = d_dna["feed"]
        features["dire_dna_avg_duration"] = d_dna["avg_duration"]
        features["dire_dna_kda"] = d_dna["kda"]
        features["dire_dna_coverage"] = d_dna["coverage"]

        # Log DNA coverage for debugging
        logger.debug(
            f"DNA coverage: radiant={r_dna['coverage']}, dire={d_dna['coverage']}, complete={features['_dna_complete']}"
        )

        # Combined DNA features
        features["combined_dna_kills"] = r_dna["avg_kills"] + d_dna["avg_kills"]
        features["combined_dna_deaths"] = r_dna["avg_deaths"] + d_dna["avg_deaths"]
        features["combined_dna_aggression"] = r_dna["aggression"] + d_dna["aggression"]
        features["combined_dna_pace"] = r_dna["pace"] + d_dna["pace"]
        features["dna_kills_diff"] = r_dna["avg_kills"] - d_dna["avg_kills"]
        features["dna_aggression_diff"] = r_dna["aggression"] - d_dna["aggression"]
        features["dna_pace_diff"] = r_dna["pace"] - d_dna["pace"]
        features["dna_duration_diff"] = r_dna["avg_duration"] - d_dna["avg_duration"]
        features["dna_pace_clash"] = abs(r_dna["pace"] - d_dna["pace"])
        features["high_dna_aggression"] = (
            1.0 if (r_dna["aggression"] + d_dna["aggression"]) > 0.8 else 0.0
        )
        features["low_dna_aggression"] = (
            1.0 if (r_dna["aggression"] + d_dna["aggression"]) < 0.6 else 0.0
        )
        features["combined_dna_coverage"] = r_dna["coverage"] + d_dna["coverage"]
        features["high_dna_coverage"] = (
            1.0 if (r_dna["coverage"] + d_dna["coverage"]) >= 8 else 0.0
        )

        # Extended DNA features (recent aggression, aggression by hero type)
        features["radiant_dna_recent_aggression"] = r_dna["recent_aggression"]
        features["dire_dna_recent_aggression"] = d_dna["recent_aggression"]
        features["combined_dna_recent_aggression"] = (
            r_dna["recent_aggression"] + d_dna["recent_aggression"]
        )

        features["radiant_dna_aggression_on_aggro"] = r_dna["aggression_on_aggro"]
        features["dire_dna_aggression_on_aggro"] = d_dna["aggression_on_aggro"]
        features["combined_dna_aggression_on_aggro"] = (
            r_dna["aggression_on_aggro"] + d_dna["aggression_on_aggro"]
        )

        features["radiant_dna_aggression_on_passive"] = r_dna["aggression_on_passive"]
        features["dire_dna_aggression_on_passive"] = d_dna["aggression_on_passive"]
        features["combined_dna_aggression_on_passive"] = (
            r_dna["aggression_on_passive"] + d_dna["aggression_on_passive"]
        )

        features["radiant_dna_aggression_delta"] = r_dna["aggression_delta"]
        features["dire_dna_aggression_delta"] = d_dna["aggression_delta"]
        features["combined_dna_aggression_delta"] = (
            r_dna["aggression_delta"] + d_dna["aggression_delta"]
        )

        # ============ CROSS-FEATURE COMBINATIONS (based on top features) ============
        # These combine top-performing features to capture interaction effects

        # 1. Blood × DNA Aggression: blood potential amplified by player aggression
        # High blood heroes + aggressive players = more kills
        features["blood_x_dna_aggression"] = (
            features["combined_blood_synergy"] * features["combined_dna_aggression"]
        )
        features["radiant_blood_x_dna_agg"] = (
            features["radiant_blood_synergy"] * r_dna["aggression"]
        )
        features["dire_blood_x_dna_agg"] = (
            features["dire_blood_synergy"] * d_dna["aggression"]
        )

        # 2. Blood Clash × (1 - Heal Ratio): blood clash when no healing to save
        # High clash + low heal = more confirmed kills
        heal_ratio = features["combined_heal_score"] / max(
            features["combined_blood_score"] + 1, 1
        )
        features["blood_clash_no_heal"] = features["match_blood_clash"] * max(
            1 - heal_ratio, 0.1
        )
        features["blood_potential_no_heal"] = features["total_blood_potential"] * max(
            1 - heal_ratio, 0.1
        )

        # 2b. Blood × Save: high save score reduces confirmed kills
        # Save heroes can prevent deaths even when blood is high
        save_factor = 1.0 / max(1.0 + features["combined_save_score"] * 0.1, 1.0)
        features["blood_minus_save"] = features["total_blood_potential"] * save_factor
        features["blood_clash_minus_save"] = features["match_blood_clash"] * save_factor
        features["save_vs_blood_ratio"] = features["combined_save_score"] / max(
            features["combined_blood_score"], 1.0
        )

        # 3. DNA Kills × Hero Blood: player kill tendency on high-blood heroes
        # Players who get many kills on heroes that enable kills = synergy
        features["dna_kills_x_blood"] = (
            features["combined_dna_kills"] * features["combined_blood_score"]
        )
        features["dna_deaths_x_blood"] = (
            features["combined_dna_deaths"] * features["combined_blood_score"]
        )

        # 4. Hero-Player Aggression Match: does player aggression match hero blood?
        # If aggressive player on aggressive hero = amplified effect
        r_hero_agg = features.get("radiant_avg_aggression", 0.35)
        d_hero_agg = features.get("dire_avg_aggression", 0.35)
        features["radiant_player_hero_agg_match"] = r_dna["aggression"] * r_hero_agg
        features["dire_player_hero_agg_match"] = d_dna["aggression"] * d_hero_agg
        features["combined_player_hero_agg_match"] = (
            features["radiant_player_hero_agg_match"]
            + features["dire_player_hero_agg_match"]
        )

        # 5. Blood per Catch: blood potential relative to catch ability
        # High blood + high catch = kills happen; high blood + low catch = escapes
        total_catch = features.get("total_catch", 1.0)
        features["blood_per_catch"] = features["total_blood_potential"] / max(
            total_catch, 0.1
        )
        features["blood_synergy_per_catch"] = features["combined_blood_synergy"] / max(
            total_catch, 0.1
        )

        # 6. Aggression Amplified Blood: blood × (dna_aggression + hero_aggression)
        # Total aggression from both players and heroes amplifying blood
        total_aggression = (
            features["combined_dna_aggression"] + features["combined_aggression"]
        )
        features["aggression_amplified_blood"] = (
            features["total_blood_potential"] * total_aggression
        )
        features["aggression_amplified_synergy"] = (
            features["combined_blood_synergy"] * total_aggression
        )

        # 7. Blood × Push: aggressive teams that push = more tower fights = more kills
        features["blood_x_push"] = (
            features["total_blood_potential"] * features["combined_push_score"]
        )

        # 8. DNA Aggression on Aggro Heroes × Blood: players aggressive on aggro heroes + blood
        features["aggro_hero_dna_x_blood"] = (
            features["combined_dna_aggression_on_aggro"]
            * features["combined_blood_score"]
        )

        # 9. Blood Clash × CC: high clash + high CC = confirmed kills
        features["blood_clash_x_cc"] = (
            features["match_blood_clash"] * features["combined_cc_score"]
        )

        # 10. Burst × Blood - Heal: burst damage + blood potential - healing
        features["burst_blood_minus_heal"] = (
            features["combined_burst_rating"]
            + features["total_blood_potential"]
            - features["combined_heal_score"]
        )

        # 11. Kill Potential Amplified: catch - evasiveness + blood
        features["kill_potential_amplified"] = (
            features.get("total_kill_potential", 0) + features["total_blood_potential"]
        )

        # 12. DNA Recent vs Overall Aggression × Blood
        # If recent aggression higher than overall = team on fire
        recent_vs_overall = (
            features["combined_dna_recent_aggression"]
            - features["combined_dna_aggression"]
        )
        features["recent_aggression_trend_x_blood"] = (
            recent_vs_overall * features["total_blood_potential"]
        )

        # ============ LANE MATCHUP FEATURES ============
        # Lane dominance predicts early kills and snowball potential
        lane_stats = self._compute_lane_matchup_score(radiant_ids, dire_ids)
        features["radiant_lane_advantage"] = lane_stats["radiant_lane_adv"]
        features["dire_lane_advantage"] = lane_stats["dire_lane_adv"]
        features["lane_advantage_diff"] = lane_stats["lane_adv_diff"]
        features["total_lane_volatility"] = lane_stats["total_lane_volatility"]
        features["lane_stomp_potential"] = lane_stats["stomp_potential"]

        # Lane advantage × blood = lane dominance leads to kills
        features["lane_adv_x_blood"] = (
            lane_stats["total_lane_volatility"] * features["total_blood_potential"]
        )
        features["lane_stomp_x_aggression"] = (
            lane_stats["stomp_potential"] * features["combined_dna_aggression"]
        )

        # ============ COMEBACK/STOMP FEATURES ============
        # Teams with high comeback = play careful when behind = fewer kills
        # Teams with high stomp = snowball hard = more kills when ahead
        r_comeback = self._compute_team_comeback_stats(radiant_ids)
        d_comeback = self._compute_team_comeback_stats(dire_ids)

        features["radiant_comeback_rate"] = r_comeback["comeback_rate"]
        features["dire_comeback_rate"] = d_comeback["comeback_rate"]
        features["combined_comeback_rate"] = (
            r_comeback["comeback_rate"] + d_comeback["comeback_rate"]
        )

        features["radiant_stomp_rate"] = r_comeback["stomp_rate"]
        features["dire_stomp_rate"] = d_comeback["stomp_rate"]
        features["combined_stomp_rate"] = (
            r_comeback["stomp_rate"] + d_comeback["stomp_rate"]
        )

        features["radiant_comeback_factor"] = r_comeback["comeback_factor"]
        features["dire_comeback_factor"] = d_comeback["comeback_factor"]
        features["combined_comeback_factor"] = (
            r_comeback["comeback_factor"] + d_comeback["comeback_factor"]
        )

        # Stomp × blood = snowball teams on blood heroes
        features["stomp_x_blood"] = (
            r_comeback["stomp_rate"] + d_comeback["stomp_rate"]
        ) * features["total_blood_potential"]
        # Comeback factor inverse = low comeback = more decisive kills
        features["decisive_kills_potential"] = (
            2.0 - r_comeback["comeback_factor"] - d_comeback["comeback_factor"]
        ) * features["combined_blood_synergy"]

        # ============ TIMING CLASH FEATURES ============
        # Both early teams = lots of early fights = more kills
        timing = self._compute_timing_clash(radiant_ids, dire_ids)
        features["radiant_early_heroes"] = timing["radiant_early_count"]
        features["radiant_late_heroes"] = timing["radiant_late_count"]
        features["dire_early_heroes"] = timing["dire_early_count"]
        features["dire_late_heroes"] = timing["dire_late_count"]
        features["timing_clash_score"] = timing["timing_clash_score"]
        features["both_early_heroes"] = timing["both_early_heroes"]
        features["both_late_heroes"] = timing["both_late_heroes"]
        features["timing_mismatch"] = timing["early_vs_late_mismatch"]

        # Timing × blood = early teams with blood = early kills
        features["early_timing_x_blood"] = (
            timing["radiant_early_count"] + timing["dire_early_count"]
        ) * features["total_blood_potential"]
        # Both early = guaranteed action
        features["both_early_x_aggression"] = (
            timing["both_early_heroes"] * features["combined_dna_aggression"]
        )
        # Late heroes = farm fest potential
        features["late_heroes_total"] = (
            timing["radiant_late_count"] + timing["dire_late_count"]
        )
        features["farm_fest_potential"] = features["late_heroes_total"] * (
            1.0 - features["combined_dna_aggression"]
        )

        # ============ EARLY/LATE COUNTER FEATURES (from public matches) ============
        # 1v1 counter scores for early and late game
        r_early_counter = self._compute_early_counter_score(radiant_ids, dire_ids)
        d_early_counter = self._compute_early_counter_score(dire_ids, radiant_ids)
        r_late_counter = self._compute_late_counter_score(radiant_ids, dire_ids)
        d_late_counter = self._compute_late_counter_score(dire_ids, radiant_ids)

        features["radiant_early_counter_pub"] = r_early_counter
        features["dire_early_counter_pub"] = d_early_counter
        features["early_counter_diff_pub"] = r_early_counter - d_early_counter
        features["radiant_late_counter_pub"] = r_late_counter
        features["dire_late_counter_pub"] = d_late_counter
        features["late_counter_diff_pub"] = r_late_counter - d_late_counter

        # Synergy scores (1+1) for early and late game
        r_early_syn_pub = self._compute_early_synergy_pub(radiant_ids)
        d_early_syn_pub = self._compute_early_synergy_pub(dire_ids)
        r_late_syn_pub = self._compute_late_synergy_pub(radiant_ids)
        d_late_syn_pub = self._compute_late_synergy_pub(dire_ids)

        features["radiant_early_synergy_pub"] = r_early_syn_pub
        features["dire_early_synergy_pub"] = d_early_syn_pub
        features["combined_early_synergy_pub"] = r_early_syn_pub + d_early_syn_pub
        features["early_synergy_diff_pub"] = r_early_syn_pub - d_early_syn_pub
        features["radiant_late_synergy_pub"] = r_late_syn_pub
        features["dire_late_synergy_pub"] = d_late_syn_pub
        features["combined_late_synergy_pub"] = r_late_syn_pub + d_late_syn_pub
        features["late_synergy_diff_pub"] = r_late_syn_pub - d_late_syn_pub

        # Trio synergy (1+1+1) for early and late game
        r_trio_early = self._compute_trio_synergy(radiant_ids, "early")
        d_trio_early = self._compute_trio_synergy(dire_ids, "early")
        r_trio_late = self._compute_trio_synergy(radiant_ids, "late")
        d_trio_late = self._compute_trio_synergy(dire_ids, "late")

        features["radiant_trio_synergy_early"] = r_trio_early
        features["dire_trio_synergy_early"] = d_trio_early
        features["combined_trio_synergy_early"] = r_trio_early + d_trio_early
        features["trio_synergy_diff_early"] = r_trio_early - d_trio_early
        features["radiant_trio_synergy_late"] = r_trio_late
        features["dire_trio_synergy_late"] = d_trio_late
        features["combined_trio_synergy_late"] = r_trio_late + d_trio_late
        features["trio_synergy_diff_late"] = r_trio_late - d_trio_late

        # ============ 2v1 / 1v2 COUNTER FEATURES ============
        # How well pairs counter single enemies (2v1) and vice versa (1v2)
        r_2v1_early = self._compute_counter_2v1(radiant_ids, dire_ids, "early")
        d_2v1_early = self._compute_counter_2v1(dire_ids, radiant_ids, "early")
        r_2v1_late = self._compute_counter_2v1(radiant_ids, dire_ids, "late")
        d_2v1_late = self._compute_counter_2v1(dire_ids, radiant_ids, "late")

        r_1v2_early = self._compute_counter_1v2(radiant_ids, dire_ids, "early")
        d_1v2_early = self._compute_counter_1v2(dire_ids, radiant_ids, "early")
        r_1v2_late = self._compute_counter_1v2(radiant_ids, dire_ids, "late")
        d_1v2_late = self._compute_counter_1v2(dire_ids, radiant_ids, "late")

        features["radiant_2v1_early"] = r_2v1_early
        features["dire_2v1_early"] = d_2v1_early
        features["counter_2v1_diff_early"] = r_2v1_early - d_2v1_early
        features["radiant_2v1_late"] = r_2v1_late
        features["dire_2v1_late"] = d_2v1_late
        features["counter_2v1_diff_late"] = r_2v1_late - d_2v1_late

        features["radiant_1v2_early"] = r_1v2_early
        features["dire_1v2_early"] = d_1v2_early
        features["counter_1v2_diff_early"] = r_1v2_early - d_1v2_early
        features["radiant_1v2_late"] = r_1v2_late
        features["dire_1v2_late"] = d_1v2_late
        features["counter_1v2_diff_late"] = r_1v2_late - d_1v2_late

        # Combined pair counter advantage (2v1 + 1v2)
        features["radiant_pair_counter_early"] = r_2v1_early + r_1v2_early
        features["dire_pair_counter_early"] = d_2v1_early + d_1v2_early
        features["pair_counter_diff_early"] = (
            features["radiant_pair_counter_early"] - features["dire_pair_counter_early"]
        )
        features["radiant_pair_counter_late"] = r_2v1_late + r_1v2_late
        features["dire_pair_counter_late"] = d_2v1_late + d_1v2_late
        features["pair_counter_diff_late"] = (
            features["radiant_pair_counter_late"] - features["dire_pair_counter_late"]
        )

        # ============ MID LANE 1v1 MATCHUP ============
        # Position 2 (mid) is typically hero index 1 (0-indexed) in pro matches
        # But we use hero_2 which is the second hero in the draft
        radiant_mid = radiant_ids[1] if len(radiant_ids) > 1 else radiant_ids[0]
        dire_mid = dire_ids[1] if len(dire_ids) > 1 else dire_ids[0]

        mid_early = self._compute_mid_matchup(radiant_mid, dire_mid, "early")
        mid_late = self._compute_mid_matchup(radiant_mid, dire_mid, "late")

        features["mid_matchup_early"] = mid_early
        features["mid_matchup_late"] = mid_late
        features["mid_matchup_avg"] = (mid_early + mid_late) / 2

        # ============ DAMAGE TYPE VS DEFENSE ============
        # Physical damage vs enemy armor, magical vs magic resist
        r_phys = sum(
            1
            for h in radiant_ids
            if self._get_hero_feature(h, "primary_attribute") == "agi"
        )
        d_phys = sum(
            1
            for h in dire_ids
            if self._get_hero_feature(h, "primary_attribute") == "agi"
        )
        r_magic = sum(
            1 for h in radiant_ids if self._get_hero_feature(h, "is_nuker", False)
        )
        d_magic = sum(
            1 for h in dire_ids if self._get_hero_feature(h, "is_nuker", False)
        )

        r_armor = sum(
            self._get_hero_feature(h, "starting_armor", 0) for h in radiant_ids
        )
        d_armor = sum(self._get_hero_feature(h, "starting_armor", 0) for h in dire_ids)
        r_magic_res = sum(
            self._get_hero_feature(h, "starting_magic_armor", 25) for h in radiant_ids
        )
        d_magic_res = sum(
            self._get_hero_feature(h, "starting_magic_armor", 25) for h in dire_ids
        )

        # Radiant physical vs Dire armor (higher = radiant advantage)
        features["radiant_phys_vs_armor"] = r_phys * 2 - d_armor / 10
        features["dire_phys_vs_armor"] = d_phys * 2 - r_armor / 10
        features["phys_vs_armor_diff"] = (
            features["radiant_phys_vs_armor"] - features["dire_phys_vs_armor"]
        )

        # Radiant magic vs Dire magic resist
        features["radiant_magic_vs_resist"] = r_magic * 2 - (d_magic_res - 125) / 10
        features["dire_magic_vs_resist"] = d_magic * 2 - (r_magic_res - 125) / 10
        features["magic_vs_resist_diff"] = (
            features["radiant_magic_vs_resist"] - features["dire_magic_vs_resist"]
        )

        # Combined damage advantage
        features["radiant_damage_adv"] = (
            features["radiant_phys_vs_armor"] + features["radiant_magic_vs_resist"]
        )
        features["dire_damage_adv"] = (
            features["dire_phys_vs_armor"] + features["dire_magic_vs_resist"]
        )
        features["damage_adv_diff"] = (
            features["radiant_damage_adv"] - features["dire_damage_adv"]
        )

        # ============ INITIATION ADVANTAGE ============
        # Heroes with strong initiation tools
        INITIATORS = {
            33,
            97,
            29,
            41,
            3,
            17,
            110,
            14,
            28,
            100,
        }  # Enigma, Magnus, Tide, Void, Bane, Storm, Phoenix, Pudge, SK, Tusk
        r_init = sum(
            1
            for h in radiant_ids
            if h in INITIATORS or self._get_hero_feature(h, "is_initiator", False)
        )
        d_init = sum(
            1
            for h in dire_ids
            if h in INITIATORS or self._get_hero_feature(h, "is_initiator", False)
        )

        features["radiant_initiation"] = r_init
        features["dire_initiation"] = d_init
        features["initiation_diff"] = r_init - d_init

        # Initiation vs counter-initiation (save heroes)
        features["radiant_init_vs_save"] = r_init - dire_save_score / 3
        features["dire_init_vs_save"] = d_init - radiant_save_score / 3
        features["init_vs_save_diff"] = (
            features["radiant_init_vs_save"] - features["dire_init_vs_save"]
        )

        # Combined draft advantage (counter + synergy + pair counter + mid)
        features["radiant_draft_adv_early"] = (
            r_early_counter
            + r_early_syn_pub
            + r_trio_early
            + r_2v1_early
            + mid_early * 0.5
        )
        features["dire_draft_adv_early"] = (
            d_early_counter
            + d_early_syn_pub
            + d_trio_early
            + d_2v1_early
            - mid_early * 0.5
        )
        features["draft_adv_diff_early"] = (
            features["radiant_draft_adv_early"] - features["dire_draft_adv_early"]
        )
        features["radiant_draft_adv_late"] = (
            r_late_counter + r_late_syn_pub + r_trio_late + r_2v1_late + mid_late * 0.5
        )
        features["dire_draft_adv_late"] = (
            d_late_counter + d_late_syn_pub + d_trio_late + d_2v1_late - mid_late * 0.5
        )
        features["draft_adv_diff_late"] = (
            features["radiant_draft_adv_late"] - features["dire_draft_adv_late"]
        )

        # ============ STRATZ MATCHUP/SYNERGY FEATURES ============
        # These use high-quality Stratz data for hero matchups and synergies
        r_stratz_matchup = self._compute_stratz_matchup_score(radiant_ids, dire_ids)
        d_stratz_matchup = self._compute_stratz_matchup_score(dire_ids, radiant_ids)
        r_stratz_synergy = self._compute_stratz_synergy_score(radiant_ids)
        d_stratz_synergy = self._compute_stratz_synergy_score(dire_ids)

        features["radiant_stratz_matchup"] = r_stratz_matchup
        features["dire_stratz_matchup"] = d_stratz_matchup
        features["stratz_matchup_diff"] = r_stratz_matchup - d_stratz_matchup
        features["radiant_stratz_synergy"] = r_stratz_synergy
        features["dire_stratz_synergy"] = d_stratz_synergy
        features["stratz_synergy_diff"] = r_stratz_synergy - d_stratz_synergy

        # Combined Stratz draft score
        features["radiant_stratz_draft"] = r_stratz_matchup + r_stratz_synergy
        features["dire_stratz_draft"] = d_stratz_matchup + d_stratz_synergy
        features["stratz_draft_diff"] = (
            features["radiant_stratz_draft"] - features["dire_stratz_draft"]
        )

        # ============ ROLE COMPOSITION FEATURES ============
        r_roles = self._compute_role_composition(radiant_ids)
        d_roles = self._compute_role_composition(dire_ids)

        features["radiant_carry_count"] = r_roles["carry"]
        features["dire_carry_count"] = d_roles["carry"]
        features["radiant_support_count"] = r_roles["support"]
        features["dire_support_count"] = d_roles["support"]
        features["radiant_disabler_count"] = r_roles["disabler"]
        features["dire_disabler_count"] = d_roles["disabler"]
        features["radiant_initiator_count"] = r_roles["initiator"]
        features["dire_initiator_count"] = d_roles["initiator"]
        features["radiant_durable_count"] = r_roles["durable"]
        features["dire_durable_count"] = d_roles["durable"]

        features["radiant_teamfight_score"] = r_roles["teamfight_score"]
        features["dire_teamfight_score"] = d_roles["teamfight_score"]
        features["teamfight_score_diff"] = (
            r_roles["teamfight_score"] - d_roles["teamfight_score"]
        )
        features["radiant_splitpush_score"] = r_roles["splitpush_score"]
        features["dire_splitpush_score"] = d_roles["splitpush_score"]
        features["splitpush_score_diff"] = (
            r_roles["splitpush_score"] - d_roles["splitpush_score"]
        )

        # ============ DISABLE CHAIN FEATURES ============
        r_disable = self._compute_disable_chain(radiant_ids)
        d_disable = self._compute_disable_chain(dire_ids)

        features["radiant_total_stun"] = r_disable["total_stun_duration"]
        features["dire_total_stun"] = d_disable["total_stun_duration"]
        features["total_stun_diff"] = (
            r_disable["total_stun_duration"] - d_disable["total_stun_duration"]
        )
        features["radiant_chain_potential"] = r_disable["chain_potential"]
        features["dire_chain_potential"] = d_disable["chain_potential"]
        features["chain_potential_diff"] = (
            r_disable["chain_potential"] - d_disable["chain_potential"]
        )
        features["radiant_bkb_pierce"] = r_disable["bkb_pierce_count"]
        features["dire_bkb_pierce"] = d_disable["bkb_pierce_count"]
        features["bkb_pierce_diff"] = (
            r_disable["bkb_pierce_count"] - d_disable["bkb_pierce_count"]
        )

        # ============ DAMAGE TYPE BALANCE ============
        r_damage = self._compute_damage_type_balance(radiant_ids)
        d_damage = self._compute_damage_type_balance(dire_ids)

        features["radiant_physical_dmg"] = r_damage["physical_damage"]
        features["dire_physical_dmg"] = d_damage["physical_damage"]
        features["radiant_magical_dmg"] = r_damage["magical_damage"]
        features["dire_magical_dmg"] = d_damage["magical_damage"]
        features["radiant_damage_balance"] = r_damage["damage_balance"]
        features["dire_damage_balance"] = d_damage["damage_balance"]
        features["damage_balance_diff"] = (
            r_damage["damage_balance"] - d_damage["damage_balance"]
        )

        # ============ CATCH VS ESCAPE ============
        r_catch = self._compute_catch_vs_escape(radiant_ids, dire_ids)
        d_catch = self._compute_catch_vs_escape(dire_ids, radiant_ids)

        features["radiant_catch_score"] = r_catch["catch_score"]
        features["dire_catch_score"] = d_catch["catch_score"]
        features["catch_score_diff"] = r_catch["catch_score"] - d_catch["catch_score"]
        features["radiant_catch_vs_escape"] = r_catch["catch_vs_enemy_escape"]
        features["dire_catch_vs_escape"] = d_catch["catch_vs_enemy_escape"]
        features["catch_vs_escape_diff"] = (
            r_catch["catch_vs_enemy_escape"] - d_catch["catch_vs_enemy_escape"]
        )

        # ============ SCALING POTENTIAL ============
        r_scaling = self._compute_scaling_potential(radiant_ids)
        d_scaling = self._compute_scaling_potential(dire_ids)

        features["radiant_late_scaling"] = r_scaling["late_game_scaling"]
        features["dire_late_scaling"] = d_scaling["late_game_scaling"]
        features["late_scaling_diff"] = (
            r_scaling["late_game_scaling"] - d_scaling["late_game_scaling"]
        )
        features["radiant_carry_scaling"] = r_scaling["carry_scaling"]
        features["dire_carry_scaling"] = d_scaling["carry_scaling"]
        features["carry_scaling_diff"] = (
            r_scaling["carry_scaling"] - d_scaling["carry_scaling"]
        )
        features["radiant_agi_gain"] = r_scaling["total_agi_gain"]
        features["dire_agi_gain"] = d_scaling["total_agi_gain"]
        features["agi_gain_diff"] = (
            r_scaling["total_agi_gain"] - d_scaling["total_agi_gain"]
        )

        # ============ GLOBAL PRESENCE ============
        r_global = self._compute_global_presence(radiant_ids)
        d_global = self._compute_global_presence(dire_ids)

        features["radiant_global_presence"] = r_global
        features["dire_global_presence"] = d_global
        features["global_presence_diff"] = r_global - d_global

        # ============ LANE PRESENCE ============
        r_lane = self._compute_lane_presence(radiant_ids)
        d_lane = self._compute_lane_presence(dire_ids)

        features["radiant_ranged_count"] = r_lane["ranged_count"]
        features["dire_ranged_count"] = d_lane["ranged_count"]
        features["ranged_diff"] = r_lane["ranged_count"] - d_lane["ranged_count"]
        features["radiant_total_armor"] = r_lane["total_armor"]
        features["dire_total_armor"] = d_lane["total_armor"]
        features["armor_diff"] = r_lane["total_armor"] - d_lane["total_armor"]
        features["radiant_avg_range"] = r_lane["avg_attack_range"]
        features["dire_avg_range"] = d_lane["avg_attack_range"]
        features["range_diff"] = r_lane["avg_attack_range"] - d_lane["avg_attack_range"]

        # ============ BIG ULT SYNERGY ============
        r_big_ult = self._compute_big_ult_synergy(radiant_ids)
        d_big_ult = self._compute_big_ult_synergy(dire_ids)

        features["radiant_big_ult_synergy"] = r_big_ult
        features["dire_big_ult_synergy"] = d_big_ult
        features["big_ult_synergy_diff"] = r_big_ult - d_big_ult

        # ============ COMBINED DRAFT SCORE ============
        # Ultimate draft advantage combining all factors
        r_total_draft = (
            features["radiant_draft_adv_early"] * 0.3
            + features["radiant_draft_adv_late"] * 0.3
            + features["radiant_stratz_draft"] * 0.2
            + r_catch["catch_vs_enemy_escape"] * 0.1
            + r_big_ult * 0.1
        )
        d_total_draft = (
            features["dire_draft_adv_early"] * 0.3
            + features["dire_draft_adv_late"] * 0.3
            + features["dire_stratz_draft"] * 0.2
            + d_catch["catch_vs_enemy_escape"] * 0.1
            + d_big_ult * 0.1
        )
        features["radiant_total_draft_score"] = r_total_draft
        features["dire_total_draft_score"] = d_total_draft
        features["total_draft_score_diff"] = r_total_draft - d_total_draft

        # ============ SPLITPUSH / ROSHAN / PICKOFF / TEAMFIGHT ============
        r_splitpush = self._compute_splitpush_threat(radiant_ids)
        d_splitpush = self._compute_splitpush_threat(dire_ids)
        features["radiant_splitpush_threat"] = r_splitpush
        features["dire_splitpush_threat"] = d_splitpush
        features["splitpush_threat_diff"] = r_splitpush - d_splitpush

        r_roshan = self._compute_roshan_potential(radiant_ids)
        d_roshan = self._compute_roshan_potential(dire_ids)
        features["radiant_roshan_potential"] = r_roshan
        features["dire_roshan_potential"] = d_roshan
        features["roshan_potential_diff"] = r_roshan - d_roshan

        r_pickoff = self._compute_pickoff_potential(radiant_ids)
        d_pickoff = self._compute_pickoff_potential(dire_ids)
        features["radiant_pickoff_potential"] = r_pickoff
        features["dire_pickoff_potential"] = d_pickoff
        features["pickoff_potential_diff"] = r_pickoff - d_pickoff

        r_teamfight = self._compute_teamfight_score(radiant_ids)
        d_teamfight = self._compute_teamfight_score(dire_ids)
        features["radiant_teamfight_potential"] = r_teamfight
        features["dire_teamfight_potential"] = d_teamfight
        features["teamfight_potential_diff"] = r_teamfight - d_teamfight

        r_counter_init = self._compute_counter_initiation(radiant_ids)
        d_counter_init = self._compute_counter_initiation(dire_ids)
        features["radiant_counter_init"] = r_counter_init
        features["dire_counter_init"] = d_counter_init
        features["counter_init_diff"] = r_counter_init - d_counter_init

        # Teamfight vs Pickoff style
        features["radiant_tf_vs_pickoff"] = r_teamfight - r_pickoff
        features["dire_tf_vs_pickoff"] = d_teamfight - d_pickoff
        features["playstyle_clash"] = abs(
            (r_teamfight - r_pickoff) - (d_teamfight - d_pickoff)
        )

        # ============ VISION CONTROL ============
        r_vision = self._compute_vision_control(radiant_ids)
        d_vision = self._compute_vision_control(dire_ids)
        features["radiant_vision_control"] = r_vision
        features["dire_vision_control"] = d_vision
        features["vision_control_diff"] = r_vision - d_vision
        features["combined_vision_control"] = r_vision + d_vision

        # Vision × Pickoff: good vision enables pickoffs
        features["vision_x_pickoff"] = (
            (r_vision + d_vision) * (r_pickoff + d_pickoff) / 10
        )

        # ============ SMOKE GANK POTENTIAL ============
        r_smoke = self._compute_smoke_gank_potential(radiant_ids)
        d_smoke = self._compute_smoke_gank_potential(dire_ids)
        features["radiant_smoke_gank"] = r_smoke
        features["dire_smoke_gank"] = d_smoke
        features["smoke_gank_diff"] = r_smoke - d_smoke
        features["combined_smoke_gank"] = r_smoke + d_smoke

        # Smoke gank × DNA aggression: aggressive teams use smokes more
        features["smoke_x_aggression"] = (r_smoke + d_smoke) * features.get(
            "combined_dna_aggression", 0.7
        )

        # ============ HIGH GROUND DEFENSE/SIEGE ============
        r_hg_def = self._compute_highground_defense(radiant_ids)
        d_hg_def = self._compute_highground_defense(dire_ids)
        r_hg_siege = self._compute_highground_siege(radiant_ids)
        d_hg_siege = self._compute_highground_siege(dire_ids)

        features["radiant_hg_defense"] = r_hg_def
        features["dire_hg_defense"] = d_hg_def
        features["hg_defense_diff"] = r_hg_def - d_hg_def
        features["radiant_hg_siege"] = r_hg_siege
        features["dire_hg_siege"] = d_hg_siege
        features["hg_siege_diff"] = r_hg_siege - d_hg_siege

        # Siege vs Defense matchup
        features["radiant_siege_vs_def"] = r_hg_siege - d_hg_def
        features["dire_siege_vs_def"] = d_hg_siege - r_hg_def
        features["siege_vs_def_clash"] = abs(features["radiant_siege_vs_def"]) + abs(
            features["dire_siege_vs_def"]
        )

        # High defense = longer games = more kills potential
        features["combined_hg_defense"] = r_hg_def + d_hg_def
        features["high_hg_defense_match"] = 1.0 if (r_hg_def + d_hg_def) > 15 else 0.0

        # ============ AURA STACKING ============
        r_aura = self._compute_aura_stacking(radiant_ids)
        d_aura = self._compute_aura_stacking(dire_ids)
        features["radiant_aura_score"] = r_aura
        features["dire_aura_score"] = d_aura
        features["aura_score_diff"] = r_aura - d_aura
        features["combined_aura_score"] = r_aura + d_aura

        # Aura × Teamfight: auras amplify teamfight
        features["aura_x_teamfight"] = (
            (r_aura + d_aura) * (r_teamfight + d_teamfight) / 20
        )

        # ============ DISPEL AVAILABILITY ============
        r_dispel = self._compute_dispel_availability(radiant_ids)
        d_dispel = self._compute_dispel_availability(dire_ids)
        features["radiant_dispel_score"] = r_dispel
        features["dire_dispel_score"] = d_dispel
        features["dispel_score_diff"] = r_dispel - d_dispel
        features["combined_dispel_score"] = r_dispel + d_dispel

        # Dispel vs CC: dispels counter enemy CC
        features["radiant_dispel_vs_cc"] = r_dispel - dire_cc
        features["dire_dispel_vs_cc"] = d_dispel - radiant_cc

        # ============ SHARD/AGHS TIMING ============
        r_timing = self._compute_shard_timing(radiant_ids)
        d_timing = self._compute_shard_timing(dire_ids)
        features["radiant_shard_score"] = r_timing["shard_score"]
        features["dire_shard_score"] = d_timing["shard_score"]
        features["shard_score_diff"] = r_timing["shard_score"] - d_timing["shard_score"]
        features["radiant_aghs_score"] = r_timing["aghs_score"]
        features["dire_aghs_score"] = d_timing["aghs_score"]
        features["aghs_score_diff"] = r_timing["aghs_score"] - d_timing["aghs_score"]
        features["radiant_item_timing"] = r_timing["item_timing_score"]
        features["dire_item_timing"] = d_timing["item_timing_score"]
        features["item_timing_diff"] = (
            r_timing["item_timing_score"] - d_timing["item_timing_score"]
        )

        # Combined timing advantage
        features["combined_shard_score"] = (
            r_timing["shard_score"] + d_timing["shard_score"]
        )
        features["combined_aghs_score"] = (
            r_timing["aghs_score"] + d_timing["aghs_score"]
        )

        # ============ MANA DEPENDENCY ============
        r_mana = self._compute_mana_dependency(radiant_ids)
        d_mana = self._compute_mana_dependency(dire_ids)
        features["radiant_mana_hungry"] = r_mana["mana_hungry"]
        features["dire_mana_hungry"] = d_mana["mana_hungry"]
        features["mana_hungry_diff"] = r_mana["mana_hungry"] - d_mana["mana_hungry"]
        features["radiant_mana_independent"] = r_mana["mana_independent"]
        features["dire_mana_independent"] = d_mana["mana_independent"]
        features["mana_independent_diff"] = (
            r_mana["mana_independent"] - d_mana["mana_independent"]
        )
        features["radiant_mana_balance"] = r_mana["mana_balance"]
        features["dire_mana_balance"] = d_mana["mana_balance"]
        features["combined_mana_hungry"] = r_mana["mana_hungry"] + d_mana["mana_hungry"]

        # ============ TEMPO CONTROL ============
        r_tempo = self._compute_tempo_control(radiant_ids)
        d_tempo = self._compute_tempo_control(dire_ids)
        features["radiant_tempo"] = r_tempo
        features["dire_tempo"] = d_tempo
        features["tempo_diff"] = r_tempo - d_tempo
        features["combined_tempo"] = r_tempo + d_tempo

        # High tempo = more action
        features["high_tempo_match"] = 1.0 if (r_tempo + d_tempo) > 15 else 0.0
        features["tempo_x_aggression"] = (r_tempo + d_tempo) * features.get(
            "combined_dna_aggression", 0.7
        )

        # ============ OBJECTIVE FOCUS ============
        r_obj = self._compute_objective_focus(radiant_ids)
        d_obj = self._compute_objective_focus(dire_ids)
        features["radiant_tower_score"] = r_obj["tower_score"]
        features["dire_tower_score"] = d_obj["tower_score"]
        features["tower_score_diff"] = r_obj["tower_score"] - d_obj["tower_score"]
        features["radiant_rosh_score"] = r_obj["rosh_score"]
        features["dire_rosh_score"] = d_obj["rosh_score"]
        features["rosh_score_diff"] = r_obj["rosh_score"] - d_obj["rosh_score"]
        features["radiant_objective"] = r_obj["objective_score"]
        features["dire_objective"] = d_obj["objective_score"]
        features["objective_diff"] = r_obj["objective_score"] - d_obj["objective_score"]
        features["combined_objective"] = (
            r_obj["objective_score"] + d_obj["objective_score"]
        )

        # High objective focus = faster games = fewer kills
        features["high_objective_match"] = (
            1.0 if (r_obj["objective_score"] + d_obj["objective_score"]) > 20 else 0.0
        )

        # ============ LANE DOMINATION BY POSITION ============
        r_lanes = self._compute_lane_domination(
            radiant_ids, dire_ids, team_is_radiant=True
        )
        d_lanes = self._compute_lane_domination(dire_ids, radiant_ids, team_is_radiant=False)
        features["radiant_safe_lane_adv"] = r_lanes["safe"]
        features["dire_safe_lane_adv"] = d_lanes["safe"]
        features["radiant_off_lane_adv"] = r_lanes["off"]
        features["dire_off_lane_adv"] = d_lanes["off"]
        features["radiant_lane_total"] = r_lanes["total"]
        features["dire_lane_total"] = d_lanes["total"]
        features["lane_total_diff"] = r_lanes["total"] - d_lanes["total"]

        # ============ POSITION MATCHUPS ============
        pos_matchup = self._compute_position_matchup(radiant_ids, dire_ids)
        features["carry_vs_carry"] = pos_matchup["carry_matchup"]
        features["mid_vs_mid"] = pos_matchup["mid_matchup"]
        features["off_vs_off"] = pos_matchup["off_matchup"]
        features["core_matchup_total"] = pos_matchup["core_matchup_total"]

        # ============ LATE GAME INSURANCE ============
        r_late_ins = self._compute_late_game_insurance(radiant_ids)
        d_late_ins = self._compute_late_game_insurance(dire_ids)
        features["radiant_late_insurance"] = r_late_ins
        features["dire_late_insurance"] = d_late_ins
        features["late_insurance_diff"] = r_late_ins - d_late_ins

        # ============ EARLY GAME DOMINANCE ============
        r_early_dom = self._compute_early_game_dominance(radiant_ids)
        d_early_dom = self._compute_early_game_dominance(dire_ids)
        features["radiant_early_dominance"] = r_early_dom
        features["dire_early_dominance"] = d_early_dom
        features["early_dominance_diff"] = r_early_dom - d_early_dom

        # ============ COMEBACK POTENTIAL ============
        r_comeback = self._compute_comeback_potential(radiant_ids)
        d_comeback = self._compute_comeback_potential(dire_ids)
        features["radiant_comeback_potential"] = r_comeback
        features["dire_comeback_potential"] = d_comeback
        features["comeback_potential_diff"] = r_comeback - d_comeback

        # ============ TIMING ADVANTAGE ============
        # Early dominance vs late insurance
        features["radiant_timing_adv"] = r_early_dom - d_late_ins
        features["dire_timing_adv"] = d_early_dom - r_late_ins
        features["timing_adv_diff"] = (
            features["radiant_timing_adv"] - features["dire_timing_adv"]
        )

        # ============ DRAFT EXECUTION (roster-based) ============
        r_exec = self._get_draft_execution(radiant_account_ids)
        d_exec = self._get_draft_execution(dire_account_ids)

        features["radiant_draft_execution"] = r_exec["execution"]
        features["dire_draft_execution"] = d_exec["execution"]
        features["draft_execution_diff"] = r_exec["execution"] - d_exec["execution"]
        features["radiant_draft_resilience"] = r_exec["resilience"]
        features["dire_draft_resilience"] = d_exec["resilience"]
        features["draft_resilience_diff"] = r_exec["resilience"] - d_exec["resilience"]
        features["radiant_exec_coverage"] = r_exec["coverage"]
        features["dire_exec_coverage"] = d_exec["coverage"]

        # ============ PER-HERO FEATURES ============
        # These are critical for the model - stats for each hero position
        for i, hero_id in enumerate(radiant_ids[:5]):
            pos = i + 1
            prefix = f"radiant_hero_{pos}"
            self._add_hero_features(features, prefix, hero_id)

        for i, hero_id in enumerate(dire_ids[:5]):
            pos = i + 1
            prefix = f"dire_hero_{pos}"
            self._add_hero_features(features, prefix, hero_id)

        # ============ EXTRA CONTEXT FEATURES (for extreme classifier) ============
        # These are REQUIRED for accurate predictions - no defaults!
        features["h2h_avg_total"] = h2h_avg_total
        features["h2h_matches_count"] = h2h_matches_count
        features["league_avg_kills"] = league_avg_kills
        features["league_kills_std"] = league_kills_std
        features["league_meta_diff"] = league_meta_diff
        features["series_game_num"] = series_game_num
        features["is_decider_game"] = is_decider_game
        features["combined_form_kills"] = combined_form_kills
        features["combined_team_avg_kills"] = combined_team_avg_kills
        features["combined_team_aggression"] = combined_team_aggression
        features["combined_synthetic_kills"] = combined_synthetic_kills
        features["combined_patch_form_kills"] = combined_patch_form_kills
        features["combined_patch_team_avg_kills"] = combined_patch_team_avg_kills
        features["combined_patch_team_aggression"] = combined_patch_team_aggression

        # Flag to check if all context features are provided
        required_context = [
            h2h_avg_total,
            league_avg_kills,
            series_game_num,
            combined_team_avg_kills,
            combined_team_aggression,
        ]
        features["_context_complete"] = all(v is not None for v in required_context)

        # ============ DERIVED FEATURES FOR EXTREME CLASSIFIER ============
        # These are computed from existing features to match training data

        # Blood synergy flags
        cbs = features.get("combined_blood_synergy", 0) or 0
        features["blood_synergy_negative"] = 1.0 if cbs < 0 else 0.0
        features["blood_synergy_strong"] = 1.0 if cbs > 1 else 0.0
        features["blood_synergy_very_negative"] = 1.0 if cbs < -1 else 0.0

        # Blood potential flags
        tbp = features.get("total_blood_potential", 0) or 0
        features["low_blood_potential"] = 1.0 if tbp < 0 else 0.0
        features["high_blood_potential"] = 1.0 if tbp > 2 else 0.0

        # H2H vs league
        h2h = h2h_avg_total if h2h_avg_total is not None else 48.0
        league = league_avg_kills if league_avg_kills is not None else 48.0
        features["h2h_vs_league"] = h2h - league
        features["h2h_low"] = 1.0 if h2h < 42 else 0.0
        features["h2h_high"] = 1.0 if h2h > 55 else 0.0

        # Team avg flags
        team_avg = (
            combined_team_avg_kills if combined_team_avg_kills is not None else 48.0
        )
        features["team_avg_low"] = 1.0 if team_avg < 45 else 0.0
        features["team_avg_high"] = 1.0 if team_avg > 50 else 0.0

        # Blood match diff
        hbm = features.get("high_blood_match", 0) or 0
        lbm = features.get("low_blood_match", 0) or 0
        features["blood_match_diff"] = hbm - lbm

        # Burst features
        r_burst = features.get("radiant_burst_score", 0) or 0
        d_burst = features.get("dire_burst_score", 0) or 0
        features["burst_score_diff"] = r_burst - d_burst
        features["burst_advantage"] = 1.0 if r_burst > d_burst else 0.0
        features["burst_diff_advantage"] = 1.0 if abs(r_burst - d_burst) > 5 else 0.0
        features["burst_dominates"] = (
            1.0 if r_burst > d_burst + 10 or d_burst > r_burst + 10 else 0.0
        )

        # Push vs blood
        r_push = features.get("radiant_push_score", 0) or 0
        d_push = features.get("dire_push_score", 0) or 0
        combined_push = r_push + d_push
        combined_blood = features.get("combined_blood_score", 0) or 0
        features["push_vs_blood"] = combined_push - combined_blood

        # Waveclear vs aggro
        r_wc = features.get("radiant_wave_clear", 0) or 0
        d_wc = features.get("dire_wave_clear", 0) or 0
        combined_wc = r_wc + d_wc
        combined_aggr = features.get("combined_aggression", 0) or 0.1
        features["waveclear_vs_aggro"] = combined_wc / max(combined_aggr, 0.1)

        # Heal vs burst
        r_heal = features.get("radiant_heal_score", 0) or 0
        d_heal = features.get("dire_heal_score", 0) or 0
        combined_heal = r_heal + d_heal
        combined_burst = r_burst + d_burst
        features["heal_vs_burst"] = combined_heal - combined_burst

        # Aggression per min (using avg_pace as proxy for game length)
        r_pace = features.get("radiant_avg_pace", 40) or 40
        d_pace = features.get("dire_avg_pace", 40) or 40
        r_aggr = features.get("radiant_total_aggression", 0) or 0
        d_aggr = features.get("dire_total_aggression", 0) or 0
        features["radiant_aggression_per_min"] = r_aggr / max(r_pace, 20)
        features["dire_aggression_per_min"] = d_aggr / max(d_pace, 20)
        features["combined_aggression_per_min"] = (
            features["radiant_aggression_per_min"] + features["dire_aggression_per_min"]
        )

        # Save features
        r_save = features.get("radiant_save_score", 0) or 0
        d_save = features.get("dire_save_score", 0) or 0
        features["save_count_diff"] = r_save - d_save
        features["save_advantage"] = 1.0 if r_save > d_save else 0.0
        features["save_diff_advantage"] = 1.0 if abs(r_save - d_save) > 3 else 0.0
        features["save_dominates"] = (
            1.0 if r_save > d_save + 5 or d_save > r_save + 5 else 0.0
        )
        features["save_burst_ratio"] = (r_save + d_save) / max(r_burst + d_burst, 1)

        # Heal advantage
        features["heal_advantage"] = 1.0 if r_heal > d_heal else 0.0

        # Aggro ratio
        r_aggro_ratio = features.get("radiant_aggro_ratio", 0) or 0
        d_aggro_ratio = features.get("dire_aggro_ratio", 0) or 0
        features["aggro_ratio_diff"] = r_aggro_ratio - d_aggro_ratio

        # Active/passive features
        r_active = features.get("radiant_active_count", 0) or 0
        d_active = features.get("dire_active_count", 0) or 0
        features["active_diff"] = r_active - d_active
        features["combined_active"] = r_active + d_active
        features["both_active_teams"] = 1.0 if r_active >= 3 and d_active >= 3 else 0.0
        features["both_aggro"] = (
            1.0 if r_aggro_ratio > 0.5 and d_aggro_ratio > 0.5 else 0.0
        )

        # Aggression diff
        features["aggression_diff"] = (features.get("radiant_aggression", 0) or 0) - (
            features.get("dire_aggression", 0) or 0
        )

        # Combined features
        features["combined_aggro_ratio"] = r_aggro_ratio + d_aggro_ratio

        # ============ MORE DERIVED FEATURES ============

        # Big ult features
        r_big_ult = features.get("radiant_big_ult_count", 0) or 0
        d_big_ult = features.get("dire_big_ult_count", 0) or 0
        features["combined_big_ult_count"] = r_big_ult + d_big_ult
        features["big_ult_diff"] = r_big_ult - d_big_ult
        features["big_ult_advantage"] = 1.0 if r_big_ult > d_big_ult else 0.0
        features["high_big_ult_match"] = 1.0 if (r_big_ult + d_big_ult) >= 4 else 0.0
        features["low_big_ult_match"] = 1.0 if (r_big_ult + d_big_ult) <= 1 else 0.0

        # BKB pierce features
        r_bkb = features.get("radiant_bkb_pierce", 0) or 0
        d_bkb = features.get("dire_bkb_pierce", 0) or 0
        features["combined_bkb_pierce"] = r_bkb + d_bkb
        features["bkb_pierce_advantage"] = 1.0 if r_bkb > d_bkb else 0.0
        features["high_bkb_pierce_match"] = 1.0 if (r_bkb + d_bkb) >= 3 else 0.0
        features["no_bkb_pierce_match"] = 1.0 if (r_bkb + d_bkb) == 0 else 0.0

        # Burst per min
        features["radiant_burst_per_min"] = r_burst / max(r_pace, 20)
        features["dire_burst_per_min"] = d_burst / max(d_pace, 20)
        features["combined_burst_per_min"] = (
            features["radiant_burst_per_min"] + features["dire_burst_per_min"]
        )
        features["high_burst_match"] = 1.0 if combined_burst > 30 else 0.0
        features["low_burst_match"] = 1.0 if combined_burst < 10 else 0.0

        # Combined burst score
        features["combined_burst_score"] = combined_burst

        # CC features
        r_cc = features.get("radiant_cc_score", 0) or 0
        d_cc = features.get("dire_cc_score", 0) or 0
        features["combined_cc_score"] = r_cc + d_cc
        features["high_cc_match"] = 1.0 if (r_cc + d_cc) > 20 else 0.0
        features["low_cc_match"] = 1.0 if (r_cc + d_cc) < 8 else 0.0

        # Initiator features
        r_init = features.get("radiant_initiators", 0) or 0
        d_init = features.get("dire_initiators", 0) or 0
        features["combined_initiators"] = r_init + d_init
        features["both_have_initiators"] = 1.0 if r_init >= 1 and d_init >= 1 else 0.0
        features["initiator_diff"] = r_init - d_init
        features["initiation_advantage"] = 1.0 if r_init > d_init else 0.0

        # Defense features
        r_def = features.get("radiant_defense_count", 0) or 0
        d_def = features.get("dire_defense_count", 0) or 0
        features["combined_defense_count"] = r_def + d_def
        features["defense_count_diff"] = r_def - d_def
        features["no_defense_match"] = 1.0 if (r_def + d_def) == 0 else 0.0

        # Disabler features
        r_dis = features.get("radiant_disablers", 0) or 0
        d_dis = features.get("dire_disablers", 0) or 0
        features["combined_disablers"] = r_dis + d_dis
        features["disabler_diff"] = r_dis - d_dis

        # Nuker features
        r_nuk = features.get("radiant_nukers", 0) or 0
        d_nuk = features.get("dire_nukers", 0) or 0
        features["combined_nukers"] = r_nuk + d_nuk
        features["nuker_diff"] = r_nuk - d_nuk

        # Durable features
        r_dur = features.get("radiant_durables", 0) or 0
        d_dur = features.get("dire_durables", 0) or 0
        features["combined_durables"] = r_dur + d_dur
        features["durable_diff"] = r_dur - d_dur

        # Carry features
        r_carry = features.get("radiant_carrys", 0) or 0
        d_carry = features.get("dire_carrys", 0) or 0
        features["combined_carrys"] = r_carry + d_carry
        features["carry_diff"] = r_carry - d_carry

        # Support features
        r_sup = features.get("radiant_supports", 0) or 0
        d_sup = features.get("dire_supports", 0) or 0
        features["combined_supports"] = r_sup + d_sup
        features["support_diff"] = r_sup - d_sup

        # Escape features
        r_esc = features.get("radiant_escapes", 0) or 0
        d_esc = features.get("dire_escapes", 0) or 0
        features["combined_escapes"] = r_esc + d_esc
        features["no_disengage_match"] = 1.0 if (r_esc + d_esc) == 0 else 0.0

        # Push features
        features["combined_push_score"] = combined_push
        features["push_advantage"] = 1.0 if r_push > d_push else 0.0
        features["both_high_push"] = 1.0 if r_push > 10 and d_push > 10 else 0.0

        # Siege difficulty
        r_siege = features.get("radiant_siege_difficulty", 0) or 0
        d_siege = features.get("dire_siege_difficulty", 0) or 0
        features["combined_siege_difficulty"] = r_siege + d_siege
        features["siege_difficulty_radiant"] = r_siege
        features["siege_difficulty_dire"] = d_siege
        features["siege_difficulty_diff"] = r_siege - d_siege

        # Greed features
        r_greed = features.get("radiant_greed", 0) or 0
        d_greed = features.get("dire_greed", 0) or 0
        features["combined_greed"] = r_greed + d_greed
        features["both_greedy"] = 1.0 if r_greed > 2 and d_greed > 2 else 0.0

        # GPM potential
        r_gpm = features.get("radiant_gpm_potential", 0) or 0
        d_gpm = features.get("dire_gpm_potential", 0) or 0
        features["combined_gpm_potential"] = r_gpm + d_gpm
        features["gpm_potential_diff"] = r_gpm - d_gpm

        # Tempo features
        r_tempo = features.get("radiant_tempo_score", 0) or 0
        d_tempo = features.get("dire_tempo_score", 0) or 0
        features["combined_tempo_score"] = r_tempo + d_tempo
        features["both_tempo"] = 1.0 if r_tempo > 5 and d_tempo > 5 else 0.0

        # Early/Late team flags
        r_early = features.get("radiant_early_power", 0) or 0
        d_early = features.get("dire_early_power", 0) or 0
        r_late = features.get("radiant_late_power", 0) or 0
        d_late = features.get("dire_late_power", 0) or 0
        features["both_early_teams"] = 1.0 if r_early > 0.6 and d_early > 0.6 else 0.0
        features["both_late_teams"] = 1.0 if r_late > 0.6 and d_late > 0.6 else 0.0
        features["early_vs_late"] = (r_early + d_early) - (r_late + d_late)

        # Objective focus
        r_obj = features.get("radiant_objective_focus", 0) or 0
        d_obj = features.get("dire_objective_focus", 0) or 0
        features["both_objective"] = 1.0 if r_obj > 0.5 and d_obj > 0.5 else 0.0

        # Mastery features
        r_mastery = features.get("radiant_mastery", 0) or 0
        d_mastery = features.get("dire_mastery", 0) or 0
        features["combined_mastery"] = r_mastery + d_mastery
        features["mastery_diff"] = r_mastery - d_mastery
        features["mastery_gap"] = abs(r_mastery - d_mastery)
        features["mastery_mismatch"] = 1.0 if abs(r_mastery - d_mastery) > 2 else 0.0
        features["both_high_mastery"] = 1.0 if r_mastery > 3 and d_mastery > 3 else 0.0

        # Reincarnation (Aegis/WK)
        r_reinc = features.get("radiant_reincarnation", 0) or 0
        d_reinc = features.get("dire_reincarnation", 0) or 0
        features["both_have_reincarnation"] = (
            1.0 if r_reinc > 0 and d_reinc > 0 else 0.0
        )
        features["reincarnation_diff"] = r_reinc - d_reinc

        # Buyback return
        r_buyback = features.get("radiant_buyback_return", 0) or 0
        d_buyback = features.get("dire_buyback_return", 0) or 0
        features["buyback_return_diff"] = r_buyback - d_buyback
        features["high_buyback_match"] = 1.0 if (r_buyback + d_buyback) > 4 else 0.0

        # Death benefit
        r_death = features.get("radiant_death_benefit", 0) or 0
        d_death = features.get("dire_death_benefit", 0) or 0
        features["combined_death_benefit"] = r_death + d_death
        features["death_benefit_diff"] = r_death - d_death

        # Feed features
        r_feed = features.get("radiant_feed", 0) or 0
        d_feed = features.get("dire_feed", 0) or 0
        features["combined_feed"] = r_feed + d_feed
        features["feed_diff"] = r_feed - d_feed
        features["tactical_feed_diff"] = features.get(
            "radiant_tactical_feed", 0
        ) - features.get("dire_tactical_feed", 0)

        # Sustain
        r_sustain = features.get("radiant_sustain", 0) or 0
        d_sustain = features.get("dire_sustain", 0) or 0
        features["combined_sustain"] = r_sustain + d_sustain
        features["sustain_diff"] = r_sustain - d_sustain

        # Snowball
        r_snow = features.get("radiant_snowball", 0) or 0
        d_snow = features.get("dire_snowball", 0) or 0
        features["snowball_diff"] = r_snow - d_snow

        # Comeback
        r_comeback = features.get("radiant_comeback_potential", 0) or 0
        d_comeback = features.get("dire_comeback_potential", 0) or 0
        features["combined_comeback_potential"] = r_comeback + d_comeback
        features["comeback_diff"] = r_comeback - d_comeback

        # Lockdown
        r_lock = features.get("radiant_lockdown", 0) or 0
        d_lock = features.get("dire_lockdown", 0) or 0
        features["combined_lockdown"] = r_lock + d_lock

        # Fight potential
        r_fight = features.get("radiant_fight_potential", 0) or 0
        d_fight = features.get("dire_fight_potential", 0) or 0
        features["combined_fight_potential"] = r_fight + d_fight

        # Stomp rate
        r_stomp = features.get("radiant_stomp_rate", 0) or 0
        d_stomp = features.get("dire_stomp_rate", 0) or 0
        features["combined_stomp_rate"] = r_stomp + d_stomp

        # Fatigue
        r_fatigue = features.get("radiant_fatigue", 0) or 0
        d_fatigue = features.get("dire_fatigue", 0) or 0
        features["combined_fatigue"] = r_fatigue + d_fatigue
        features["fatigue_diff"] = r_fatigue - d_fatigue

        # Hot/cold streak
        r_hot = features.get("radiant_hot_streak", 0) or 0
        d_hot = features.get("dire_hot_streak", 0) or 0
        r_cold = features.get("radiant_cold_streak", 0) or 0
        d_cold = features.get("dire_cold_streak", 0) or 0
        features["hot_streak_diff"] = r_hot - d_hot
        features["cold_streak_diff"] = r_cold - d_cold

        # Tier flags
        features["is_elite_match"] = 1.0 if features.get("both_tier1", 0) == 1 else 0.0
        features["is_tier2_match"] = 1.0 if features.get("avg_tier", 3) == 2 else 0.0
        features["is_mismatch_match"] = (
            1.0 if features.get("tier_diff", 0) >= 1 else 0.0
        )

        # Time + patch features (match start time when available)
        features.update(self._get_time_features(match_start_time))
        features.update(self._get_patch_features(match_start_time))

        # Tournament tier (league-based if provided)
        if tournament_tier is not None:
            features["tournament_tier"] = tournament_tier
        else:
            features["tournament_tier"] = features.get("avg_tier", 2)

        # Laning features
        r_lane = features.get("radiant_laning_score", 0) or 0
        d_lane = features.get("dire_laning_score", 0) or 0
        features["total_laning_score"] = r_lane + d_lane
        features["total_laning_abs"] = abs(r_lane) + abs(d_lane)
        features["laning_domination_radiant"] = 1.0 if r_lane > 5 else 0.0
        features["laning_domination_dire"] = 1.0 if d_lane > 5 else 0.0
        features["laning_domination_diff"] = (
            features["laning_domination_radiant"] - features["laning_domination_dire"]
        )
        features["laning_variance"] = abs(r_lane - d_lane)

        # Lane winrates (defaults)
        features["avg_lane_winrate"] = 0.5
        features["safe_lane_winrate"] = 0.5
        features["mid_lane_winrate"] = 0.5
        features["off_lane_winrate"] = 0.5
        features["safe_lane_advantage"] = 0.0
        features["mid_lane_advantage"] = 0.0
        features["off_lane_advantage"] = 0.0
        features["mid_dominance"] = 0.0

        # Counter features
        r_counter = features.get("radiant_counter_score", 0) or 0
        d_counter = features.get("dire_counter_score", 0) or 0
        features["counter_diff"] = r_counter - d_counter
        features["counter_advantage_abs"] = abs(r_counter - d_counter)
        features["counter_war"] = 1.0 if r_counter > 3 and d_counter > 3 else 0.0
        features["total_counter_advantage"] = r_counter + d_counter
        features["total_hard_counters"] = features.get(
            "radiant_hard_counters", 0
        ) + features.get("dire_hard_counters", 0)
        features["hard_counter_diff"] = features.get(
            "radiant_hard_counters", 0
        ) - features.get("dire_hard_counters", 0)
        features["one_sided_counters"] = 1.0 if abs(r_counter - d_counter) > 5 else 0.0
        features["max_counter_diff"] = max(r_counter, d_counter) - min(
            r_counter, d_counter
        )

        # Has features
        features["has_hard_save"] = 1.0 if (r_save + d_save) > 3 else 0.0
        features["has_instakill_hero"] = (
            1.0 if features.get("combined_instakill", 0) > 0 else 0.0
        )
        features["has_tier_s_defender"] = 0.0  # Would need specific hero check

        # No features
        features["no_save_match"] = 1.0 if (r_save + d_save) == 0 else 0.0

        # Siege features
        features["easy_siege_match"] = 1.0 if (r_siege + d_siege) < 3 else 0.0
        features["hard_siege_match"] = 1.0 if (r_siege + d_siege) > 8 else 0.0

        # Game pace potential
        features["fast_game_potential"] = (
            1.0 if combined_push > 20 and (r_early + d_early) > 1.2 else 0.0
        )
        features["slow_game_potential"] = (
            1.0 if combined_push < 10 and (r_late + d_late) > 1.2 else 0.0
        )

        # Map control
        features["map_control_potential"] = (r_push + d_push + r_wc + d_wc) / 4

        # Power curve
        features["power_curve_diff"] = features.get(
            "radiant_power_curve", 0
        ) - features.get("dire_power_curve", 0)
        features["power_curve_clash"] = abs(features["power_curve_diff"])

        # Phase clash
        features["phase_clash"] = (
            1.0
            if (r_early > 0.6 and d_late > 0.6) or (d_early > 0.6 and r_late > 0.6)
            else 0.0
        )

        # Hero flexibility
        r_flex = features.get("radiant_hero_flexibility", 0) or 0
        d_flex = features.get("dire_hero_flexibility", 0) or 0
        features["combined_hero_flexibility"] = r_flex + d_flex
        features["hero_flexibility_diff"] = r_flex - d_flex

        # Draft popularity
        r_pop = features.get("radiant_draft_popularity", 0) or 0
        d_pop = features.get("dire_draft_popularity", 0) or 0
        features["draft_popularity_diff"] = r_pop - d_pop

        # Confidence features
        features["roster_confidence"] = features.get("_dna_complete", 0)
        features["full_roster_data"] = (
            1.0 if features.get("_dna_complete", False) else 0.0
        )
        features["low_confidence_match"] = (
            1.0 if not features.get("_dna_complete", False) else 0.0
        )
        features["known_players_diff"] = features.get(
            "radiant_dna_coverage", 0
        ) - features.get("dire_dna_coverage", 0)

        # Momentum
        features["momentum_score"] = (r_hot - r_cold) - (d_hot - d_cold)

        # Trio score
        r_trio = features.get("radiant_trio_score", 0) or 0
        d_trio = features.get("dire_trio_score", 0) or 0
        features["combined_trio_score"] = r_trio + d_trio
        features["trio_score_diff"] = r_trio - d_trio

        # Greedy carries
        features["both_greedy_carries"] = 1.0 if r_greed > 3 and d_greed > 3 else 0.0

        # Pusher diff
        r_pusher = features.get("radiant_pushers", 0) or 0
        d_pusher = features.get("dire_pushers", 0) or 0
        features["pusher_diff"] = r_pusher - d_pusher

        # Push vs defense
        features["push_vs_defense"] = combined_push - (r_def + d_def)

        # Push wave clear ratio
        features["push_wave_clear_ratio"] = combined_push / max(combined_wc, 1)

        # League games played (default)
        features["league_games_played"] = 0

        # Late series game
        features["late_series_game"] = (
            is_decider_game if is_decider_game is not None else 0
        )

        # Stun duration
        r_stun = features.get("radiant_stun_duration", 0) or 0
        d_stun = features.get("dire_stun_duration", 0) or 0
        features["combined_stun_duration"] = r_stun + d_stun
        features["stun_duration_diff"] = r_stun - d_stun

        return features

    def _add_hero_features(
        self, features: Dict[str, Any], prefix: str, hero_id: int
    ) -> None:
        """Добавляет все фичи для одного героя."""
        # CC stats
        features[f"{prefix}_cc_score"] = self._get_hero_stat(
            self.hero_cc_stats, hero_id, "cc_score", 0.0
        )
        features[f"{prefix}_stun_dur"] = self._get_hero_stat(
            self.hero_cc_stats, hero_id, "stun_duration", 0.0
        )
        features[f"{prefix}_is_initiator"] = self._get_hero_stat(
            self.hero_cc_stats, hero_id, "is_initiator", 0.0
        )

        # Power spikes
        features[f"{prefix}_early_power"] = self._get_hero_stat(
            self.hero_power_spikes, hero_id, "early_power", 0.5
        )
        features[f"{prefix}_late_power"] = self._get_hero_stat(
            self.hero_power_spikes, hero_id, "late_power", 0.5
        )
        features[f"{prefix}_power_curve"] = self._get_hero_stat(
            self.hero_power_spikes, hero_id, "power_curve", 0.0
        )

        # Heal stats
        features[f"{prefix}_heal_score"] = self._get_hero_stat(
            self.hero_healing_stats, hero_id, "healing_score", 0.0
        )
        features[f"{prefix}_is_save"] = self._get_hero_stat(
            self.hero_healing_stats, hero_id, "is_save_hero", 0.0
        )

        # Push stats
        features[f"{prefix}_push_score"] = self._get_hero_stat(
            self.hero_push_stats, hero_id, "push_score", 0.0
        )
        features[f"{prefix}_is_defense"] = self._get_hero_stat(
            self.hero_push_stats, hero_id, "is_defense_hero", 0.0
        )

        # Greed stats
        features[f"{prefix}_is_greedy"] = self._get_hero_is_greedy(hero_id)
        features[f"{prefix}_gpm"] = self._get_hero_stat(
            self.hero_greed_index, hero_id, "gpm", 400.0
        )
        features[f"{prefix}_gpm_potential"] = self._get_hero_stat(
            self.hero_greed_index, hero_id, "greed_index", 0.5
        )
        features[f"{prefix}_aggression"] = self._get_hero_stat(
            self.hero_greed_index, hero_id, "aggression", 1.0
        )
        features[f"{prefix}_pace"] = self._get_hero_stat(
            self.hero_greed_index, hero_id, "pace", 0.0
        )

        # Wave clear
        features[f"{prefix}_wave_clear"] = self._get_hero_stat(
            self.hero_wave_clear, hero_id, "wave_clear_score", 0.0
        )

        # Blood stats (per hero)
        hero_blood = self.blood_stats.get("hero_blood", {}).get(str(hero_id), {})
        features[f"{prefix}_blood"] = (
            hero_blood.get("blood_score", 0.0) if isinstance(hero_blood, dict) else 0.0
        )

        # Additional flags from complex stats
        features[f"{prefix}_burst"] = self._get_hero_burst(hero_id)
        features[f"{prefix}_save"] = self._get_hero_stat(
            self.hero_healing_stats, hero_id, "healing_score", 0.0
        )
        features[f"{prefix}_feed"] = self._get_hero_feed(hero_id)
        features[f"{prefix}_buyback_return"] = self._get_hero_buyback_return(hero_id)
        features[f"{prefix}_death_benefit"] = self._get_hero_death_benefit(hero_id)
        features[f"{prefix}_hg_defense"] = self._get_hero_stat(
            self.hero_push_stats, hero_id, "is_defense_hero", 0.0
        )
        features[f"{prefix}_reincarnation"] = self._get_hero_reincarnation(hero_id)
        features[f"{prefix}_big_ult"] = self._get_hero_big_ult(hero_id)
        features[f"{prefix}_bkb_pierce"] = self._get_hero_bkb_pierce(hero_id)
        features[f"{prefix}_is_active"] = (
            1.0
            if self._get_hero_stat(self.hero_greed_index, hero_id, "aggression", 1.0)
            > 1.2
            else 0.0
        )

    def _get_hero_burst(self, hero_id: int) -> float:
        """Получает burst score героя (high early power = burst)."""
        # Use early_power from hero_power_spikes - high early power = burst heroes
        return self._get_hero_stat(self.hero_power_spikes, hero_id, "early_power", 0.5)

    def _get_hero_feed(self, hero_id: int) -> float:
        """Получает feed potential героя (high aggression = more deaths)."""
        # Use aggression from hero_greed_index - high aggression heroes tend to die more
        return self._get_hero_stat(self.hero_greed_index, hero_id, "aggression", 0.5)

    def _get_hero_buyback_return(self, hero_id: int) -> float:
        """Buyback return value - heroes that benefit from buyback."""
        # High impact heroes benefit more from buyback
        high_buyback_heroes = {
            33,
            97,
            41,
            29,
            110,
        }  # Enigma, Magnus, Void, Tide, Phoenix
        return 1.0 if hero_id in high_buyback_heroes else 0.0

    def _get_hero_death_benefit(self, hero_id: int) -> float:
        """Death benefit - heroes that gain from dying (Bloodstone, etc.)."""
        death_benefit_heroes = {52, 75}  # Leshrac, Death Prophet (Bloodstone carriers)
        return 1.0 if hero_id in death_benefit_heroes else 0.0

    def _get_hero_reincarnation(self, hero_id: int) -> float:
        """Heroes with reincarnation or pseudo-reincarnation."""
        reincarnation_heroes = {81, 102}  # Wraith King, Abaddon
        return 1.0 if hero_id in reincarnation_heroes else 0.0

    def _get_hero_big_ult(self, hero_id: int) -> float:
        """
        Big Teamfight Ultimate - герои с мощными командными ультами.

        Ульты, которые могут выиграть файт в одиночку. Команды с такими героями
        чаще ищут 5v5 файты, что увеличивает количество смертей.
        Влияет на: частота файтов, количество смертей за файт.

        Heroes:
            33  - Enigma (Black Hole - 4 sec AoE disable)
            97  - Magnus (Reverse Polarity - AoE stun + reposition)
            110 - Phoenix (Supernova - AoE stun + full heal)
        """
        big_ult_heroes = {33, 97, 110, 29}
        return 1.0 if hero_id in big_ult_heroes else 0.0

    def _get_hero_bkb_pierce(self, hero_id: int) -> float:
        """
        BKB-Piercing Abilities - герои со способностями, пробивающими BKB.

        Могут контролировать врагов даже под BKB, что делает файты более смертельными.
        Команда с BKB-pierce может убивать керри под BKB = больше смертей.
        Влияет на: эффективность файтов, количество смертей керри.

        Heroes:
            2   - Axe (Berserker's Call - taunt through BKB)
            33  - Enigma (Black Hole - disable through BKB)
            97  - Magnus (Reverse Polarity - stun through BKB)
            3   - Bane (Fiend's Grip - channel through BKB)

        """
        bkb_pierce_heroes = {2, 33, 97, 3}
        return 1.0 if hero_id in bkb_pierce_heroes else 0.0

    def predict_with_models(
        self, features: Dict[str, Any]
    ) -> Tuple[float, float, float, float, float]:
        """
        Делает предсказание с помощью обученных моделей.

        Returns:
            (kills_prob, predicted_kills, winner_prob, duration_pred, kpm_pred)
        """
        if not self.models_loaded:
            # Fallback to heuristic
            return self._predict_heuristic(features)

        # Use CatBoost if available
        if self.use_catboost and self.cb_kills is not None:
            return self._predict_catboost(features)

        # Legacy LightGBM/XGBoost prediction
        return self._predict_legacy(features)

    def _predict_catboost(
        self, features: Dict[str, Any]
    ) -> Tuple[float, float, float, float, float]:
        """Предсказание с помощью CatBoost моделей + XGBoost ensemble."""
        # Prepare feature vector in correct order for CatBoost
        feature_values = []
        for col in self.cb_feature_cols:
            val = features.get(col, 0)
            # Convert categorical features to string
            if col in self.cb_cat_features:
                val = self._cat_to_str(val)
            else:
                val = float(val) if val is not None else 0.0
            feature_values.append(val)

        # Create DataFrame for CatBoost
        X_cb = pd.DataFrame([feature_values], columns=self.cb_feature_cols)

        # Prepare XGBoost features (numeric only)
        X_xgb = None
        if self.use_xgboost and self.xgb_feature_cols:
            xgb_values = [float(features.get(col, 0)) for col in self.xgb_feature_cols]
            X_xgb = pd.DataFrame([xgb_values], columns=self.xgb_feature_cols)

        # ============ KILLS PREDICTION (ENSEMBLE) ============
        cb_kills_prob = 0.5
        if self.cb_kills is not None:
            try:
                proba = self.cb_kills.predict_proba(X_cb)[0]
                cb_kills_prob = float(proba[1])
            except Exception as e:
                logger.warning(f"CatBoost kills prediction failed: {e}")

        xgb_kills_prob = cb_kills_prob  # Default to CatBoost
        if self.use_xgboost and self.xgb_kills is not None and X_xgb is not None:
            try:
                proba = self.xgb_kills.predict_proba(X_xgb)[0]
                xgb_kills_prob = float(proba[1])
            except Exception as e:
                logger.warning(f"XGBoost kills prediction failed: {e}")

        # Ensemble: 60% CatBoost + 40% XGBoost
        if self.use_xgboost:
            kills_prob = 0.6 * cb_kills_prob + 0.4 * xgb_kills_prob
        else:
            kills_prob = cb_kills_prob

        # Optional calibration for kills probability
        if self.use_kills_calibrator and self.kills_calibrator is not None:
            try:
                cal_cols = self.kills_cal_feature_cols or ["kills_prob"]
                cal_features = {"kills_prob": kills_prob}
                cal_values = [float(cal_features.get(col, 0.0)) for col in cal_cols]
                X_cal = pd.DataFrame([cal_values], columns=cal_cols)
                if hasattr(self.kills_calibrator, "predict_proba"):
                    kills_prob = float(self.kills_calibrator.predict_proba(X_cal)[0][1])
                else:
                    kills_prob = float(self.kills_calibrator.predict(X_cal)[0])
            except Exception as e:
                logger.warning(f"Kills calibration failed: {e}")

        # ============ WINNER PREDICTION (ENSEMBLE) ============
        cb_winner_prob = 0.5
        if self.cb_winner is not None:
            try:
                proba = self.cb_winner.predict_proba(X_cb)[0]
                cb_winner_prob = float(proba[1])
            except Exception as e:
                logger.warning(f"CatBoost winner prediction failed: {e}")

        xgb_winner_prob = cb_winner_prob  # Default to CatBoost
        if self.use_xgboost and self.xgb_winner is not None and X_xgb is not None:
            try:
                proba = self.xgb_winner.predict_proba(X_xgb)[0]
                xgb_winner_prob = float(proba[1])
            except Exception as e:
                logger.warning(f"XGBoost winner prediction failed: {e}")

        # Ensemble: 60% CatBoost + 40% XGBoost
        if self.use_xgboost:
            winner_prob = 0.6 * cb_winner_prob + 0.4 * xgb_winner_prob
        else:
            winner_prob = cb_winner_prob

        # Duration prediction
        duration_pred = 37.0
        if self.cb_duration is not None:
            try:
                duration_pred = float(self.cb_duration.predict(X_cb)[0])
            except Exception as e:
                logger.warning(f"CatBoost duration prediction failed: {e}")

        # KPM prediction
        kpm_pred = 1.2
        if self.cb_kpm is not None:
            try:
                kpm_pred = float(self.cb_kpm.predict(X_cb)[0])
            except Exception as e:
                logger.warning(f"CatBoost KPM prediction failed: {e}")

        # Meta stacking for kills probability (optional)
        meta_prob = None
        if self.use_kills_meta and self.kills_meta_model is not None:
            try:
                meta_features = {
                    "kills_prob": kills_prob,
                    "duration_pred": duration_pred,
                    "kpm_pred": kpm_pred,
                    "kills_formula": duration_pred * kpm_pred,
                }
                meta_values = [
                    float(meta_features.get(col, 0.0))
                    for col in self.kills_meta_feature_cols
                ]
                X_meta = pd.DataFrame([meta_values], columns=self.kills_meta_feature_cols)
                if hasattr(self.kills_meta_model, "predict_proba"):
                    meta_prob = float(self.kills_meta_model.predict_proba(X_meta)[0][1])
                else:
                    meta_prob = float(self.kills_meta_model.predict(X_meta)[0])
            except Exception as e:
                logger.warning(f"Kills meta prediction failed: {e}")

        # ============ KILLS REGRESSION (MAE optimized) ============
        predicted_kills_reg = None
        X_reg = None
        if self.use_kills_regression and self.cb_kills_reg is not None:
            try:
                # Add aux predictions as features (stacking)
                features["predicted_duration"] = duration_pred
                features["predicted_kpm"] = kpm_pred
                features["predicted_kills_formula"] = duration_pred * kpm_pred

                # Prepare features for regression model
                reg_values = []
                for col in self.cb_kills_reg_feature_cols:
                    val = features.get(col, 0)
                    if col in self.cb_kills_reg_cat_features:
                        val = self._cat_to_str(val)
                    else:
                        val = float(val) if val is not None else 0.0
                    reg_values.append(val)
                X_reg = pd.DataFrame([reg_values], columns=self.cb_kills_reg_feature_cols)
                predicted_kills_reg = float(self.cb_kills_reg.predict(X_reg)[0])
            except Exception as e:
                logger.warning(f"Kills regression prediction failed: {e}")

        q10_pred = None
        q90_pred = None
        dist_prob = None
        if (
            self.use_kills_quantiles
            and self.cb_kills_q10 is not None
            and self.cb_kills_q90 is not None
            and X_reg is not None
        ):
            try:
                q10_pred = float(self.cb_kills_q10.predict(X_reg)[0])
                q90_pred = float(self.cb_kills_q90.predict(X_reg)[0])
                if q90_pred < q10_pred:
                    q10_pred, q90_pred = q90_pred, q10_pred
                mu = (
                    predicted_kills_reg
                    if predicted_kills_reg is not None
                    else (q10_pred + q90_pred) / 2.0
                )
                sigma = max(1.0, (q90_pred - q10_pred) / (2.0 * 1.28155))
                dist_prob = 1.0 - self._normal_cdf(self.bk_line, mu, sigma)
                dist_prob = max(0.05, min(0.95, dist_prob))
            except Exception as e:
                logger.warning(f"Kills quantile prediction failed: {e}")

        # Use regression prediction if available, otherwise estimate from classifier
        reg_prob = None
        predicted_kills = None
        if predicted_kills_reg is not None:
            predicted_kills = predicted_kills_reg
            reg_prob = 0.5 + (predicted_kills - self.bk_line) / 20
            reg_prob = max(0.1, min(0.9, reg_prob))

        if meta_prob is not None:
            meta_blend = 0.35
            kills_prob = (1.0 - meta_blend) * kills_prob + meta_blend * meta_prob

        if reg_prob is not None:
            kills_prob = 0.5 * reg_prob + 0.5 * kills_prob

        if dist_prob is not None:
            dist_blend = 0.35
            kills_prob = (1.0 - dist_blend) * kills_prob + dist_blend * dist_prob

        if predicted_kills is None:
            predicted_kills = self.bk_line + (kills_prob - 0.5) * 20

        # Live blend: adjust with in-game pace when available
        game_time_min = features.get("game_time_min_live")
        live_kills = features.get("ingame_total_kills")
        live_kpm = features.get("ingame_kpm")
        try:
            if (
                game_time_min is not None
                and live_kills is not None
                and live_kpm is not None
                and float(game_time_min) > 0
                and float(live_kpm) > 0
            ):
                game_time_min = float(game_time_min)
                live_kills = float(live_kills)
                live_kpm = float(live_kpm)
                remaining = max(duration_pred - game_time_min, 0.0)
                live_projected = live_kills + live_kpm * remaining
                live_weight = min(0.75, max(0.0, (game_time_min - 5.0) / 15.0))
                predicted_kills = (1.0 - live_weight) * predicted_kills + live_weight * live_projected
                kills_prob = 0.5 + (predicted_kills - self.bk_line) / 20
                kills_prob = max(0.1, min(0.9, kills_prob))
        except Exception as e:
            logger.warning(f"Live blend failed: {e}")

        return kills_prob, predicted_kills, winner_prob, duration_pred, kpm_pred

    def _predict_legacy(
        self, features: Dict[str, Any]
    ) -> Tuple[float, float, float, float, float]:
        """Предсказание с помощью LightGBM/XGBoost моделей (legacy)."""
        import xgboost as xgb

        # Prepare feature vector using model_features order
        feature_names = getattr(self, "model_features", self.selected_features)
        feature_values = [float(features.get(f, 0.0)) for f in feature_names]

        # Create DataFrame for prediction
        X = pd.DataFrame([feature_values], columns=feature_names)
        X = X.fillna(0.0)

        probs = []

        # LightGBM kills prediction
        if hasattr(self, "lgbm_kills") and self.lgbm_kills is not None:
            try:
                lgbm_prob = self.lgbm_kills.predict(X)[0]
                probs.append(lgbm_prob)
            except Exception as e:
                logger.warning(f"LightGBM prediction failed: {e}")

        # XGBoost kills prediction
        if hasattr(self, "xgb_kills") and self.xgb_kills is not None:
            try:
                dmatrix = xgb.DMatrix(X)
                xgb_prob = self.xgb_kills.predict(dmatrix)[0]
                probs.append(xgb_prob)
            except Exception as e:
                logger.warning(f"XGBoost prediction failed: {e}")

        # Ensemble kills probability
        if probs:
            kills_prob = float(np.mean(probs))
        else:
            kills_prob = 0.5

        # Winner prediction
        winner_prob = 0.5
        if hasattr(self, "lgbm_winner") and self.lgbm_winner is not None:
            try:
                winner_prob = float(self.lgbm_winner.predict(X)[0])
            except Exception as e:
                logger.warning(f"Winner prediction failed: {e}")

        # Duration prediction
        duration_pred = 37.0
        if hasattr(self, "xgb_duration") and self.xgb_duration is not None:
            try:
                dmatrix = xgb.DMatrix(X)
                duration_pred = float(self.xgb_duration.predict(dmatrix)[0])
            except Exception as e:
                logger.warning(f"Duration prediction failed: {e}")

        # KPM prediction
        kpm_pred = 1.2  # Default ~45 kills / 37 min
        if hasattr(self, "xgb_kpm") and self.xgb_kpm is not None:
            try:
                dmatrix = xgb.DMatrix(X)
                kpm_pred = float(self.xgb_kpm.predict(dmatrix)[0])
            except Exception as e:
                logger.warning(f"KPM prediction failed: {e}")

        # Estimate kills based on probability
        predicted_kills = self.bk_line + (kills_prob - 0.5) * 20

        return kills_prob, predicted_kills, winner_prob, duration_pred, kpm_pred

    def _predict_heuristic(
        self, features: Dict[str, float]
    ) -> Tuple[float, float, float, float, float]:
        """Fallback эвристика когда модели не загружены."""
        # Blood stats - STRONGEST signal
        blood_score = features.get("combined_blood_score", 0)
        blood_potential = features.get("total_blood_potential", 0)
        blood_signal = (blood_score * 0.6 + blood_potential * 0.4) / 5.0

        # Other signals
        cc_score = features.get("combined_cc_score", 0)
        greed = features.get("combined_greed", 1.0)
        push_score = features.get("combined_push_score", 0)

        total_signal = blood_signal * 0.5 + cc_score / 30.0 - push_score / 50.0

        kills_prob = 0.5 + total_signal * 0.15
        kills_prob = max(0.35, min(0.65, kills_prob))

        predicted_kills = self.bk_line + total_signal * 10

        # Winner heuristic
        blood_diff = features.get("blood_score_diff", 0)
        winner_prob = 0.5 + blood_diff * 0.05
        winner_prob = max(0.35, min(0.65, winner_prob))

        # Duration heuristic
        duration_pred = 37.0 + (greed - 1.0) * 5 - push_score / 10
        duration_pred = max(28, min(50, duration_pred))

        # KPM heuristic
        kpm_pred = 1.2 + blood_signal * 0.3
        kpm_pred = max(0.8, min(1.6, kpm_pred))

        return kills_prob, predicted_kills, winner_prob, duration_pred, kpm_pred

    def _resolve_team_id(self, team_name: str, team_id: Optional[int]) -> Optional[int]:
        """Resolves team ID from name if not provided."""
        if team_id is not None and int(team_id) not in (-1, 0):
            return team_id

        # Try new mapping first (more comprehensive)
        if team_name and TEAM_MAPPING_AVAILABLE:
            resolved_id = get_team_id(team_name)
            if resolved_id:
                return resolved_id

        # Fallback to old mapping
        if team_name and self.team_name_map:
            return self.team_name_map.get(team_name)
        return None

    def predict_extreme(
        self,
        features: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Predict extreme kills games using binary classifiers.

        Returns:
            Dict with:
            - low_prob: probability of LOW game (<38 kills)
            - high_prob: probability of HIGH game (>52 kills)
            - prediction: 'LOW', 'HIGH', or 'NORMAL'
            - confidence: confidence level (0-1)
            - should_bet: True if confidence is high enough
        """
        if not self.use_extreme_classifier:
            return {
                "low_prob": 0.0,
                "high_prob": 0.0,
                "prediction": "NORMAL",
                "confidence": 0.0,
                "should_bet": False,
                "reason": "Extreme classifier not loaded",
            }

        # Check if all required context features are provided
        missing = []

        # Context features (from team history)
        if features.get("h2h_avg_total") is None:
            missing.append("h2h_avg_total")
        if features.get("league_avg_kills") is None:
            missing.append("league_avg_kills")
        if features.get("series_game_num") is None:
            missing.append("series_game_num")
        if features.get("combined_team_avg_kills") is None:
            missing.append("combined_team_avg_kills")
        if features.get("combined_team_aggression") is None:
            missing.append("combined_team_aggression")

        # DNA features (from player history)
        if not features.get("_dna_complete", False):
            missing.append("player_dna")

        # Context complete flag
        if not features.get("_context_complete", False):
            missing.append("team_context")

        if missing:
            logger.info(
                f"Extreme prediction skipped due to missing prerequisites: {missing}"
            )
            return {
                "low_prob": 0.0,
                "high_prob": 0.0,
                "prediction": "SKIP",
                "confidence": 0.0,
                "should_bet": False,
                "reason": f"Missing data: {', '.join(missing)}",
                "missing_prerequisites": missing,
            }

        try:
            # Prepare features - use separate feature sets if available
            if self.use_separate_extreme_features:
                # LOW model features
                low_values = []
                for col in self.extreme_low_feature_cols:
                    val = features.get(col, 0.0)
                    if val is None:
                        val = 0.0
                    low_values.append(val)
                X_low = pd.DataFrame(
                    [low_values], columns=self.extreme_low_feature_cols
                )

                # Numeric coercion (avoid silent string/object leakage into the model)
                for col in X_low.columns:
                    if col not in self.extreme_low_cat_features:
                        X_low[col] = pd.to_numeric(X_low[col], errors="coerce").fillna(
                            0.0
                        )

                # Safer categorical coercion + invalid counter
                low_cat_invalid_count = 0
                low_invalid_cols = []
                for col in self.extreme_low_cat_features:
                    if col in X_low.columns:
                        raw = X_low[col].iloc[0]
                        coerced = (
                            pd.to_numeric(pd.Series([raw]), errors="coerce")
                            .fillna(-1)
                            .astype(int)
                            .iloc[0]
                        )
                        if coerced <= 0:
                            low_cat_invalid_count += 1
                            low_invalid_cols.append(col)
                        X_low[col] = pd.Series([coerced]).astype(str)

                if low_cat_invalid_count:
                    logger.warning(
                        f"Extreme LOW categorical invalid (<=0) count: {low_cat_invalid_count}/{len(self.extreme_low_cat_features)} "
                        f"(cols: {low_invalid_cols[:20]})"
                    )

                # HIGH model features
                high_values = []
                for col in self.extreme_high_feature_cols:
                    val = features.get(col, 0.0)
                    if val is None:
                        val = 0.0
                    high_values.append(val)
                X_high = pd.DataFrame(
                    [high_values], columns=self.extreme_high_feature_cols
                )

                # Numeric coercion (avoid silent string/object leakage into the model)
                for col in X_high.columns:
                    if col not in self.extreme_high_cat_features:
                        X_high[col] = pd.to_numeric(
                            X_high[col], errors="coerce"
                        ).fillna(0.0)

                # Safer categorical coercion + invalid counter
                high_cat_invalid_count = 0
                high_invalid_cols = []
                for col in self.extreme_high_cat_features:
                    if col in X_high.columns:
                        raw = X_high[col].iloc[0]
                        coerced = (
                            pd.to_numeric(pd.Series([raw]), errors="coerce")
                            .fillna(-1)
                            .astype(int)
                            .iloc[0]
                        )
                        if coerced <= 0:
                            high_cat_invalid_count += 1
                            high_invalid_cols.append(col)
                        X_high[col] = pd.Series([coerced]).astype(str)

                if high_cat_invalid_count:
                    logger.warning(
                        f"Extreme HIGH categorical invalid (<=0) count: {high_cat_invalid_count}/{len(self.extreme_high_cat_features)} "
                        f"(cols: {high_invalid_cols[:20]})"
                    )

                # Predict with separate feature sets
                low_prob = float(self.extreme_low_model.predict_proba(X_low)[0, 1])
                high_prob = float(self.extreme_high_model.predict_proba(X_high)[0, 1])
            else:
                # Shared feature set (legacy)
                missing_cols = [
                    col
                    for col in self.extreme_feature_cols
                    if col not in features or features.get(col) is None
                ]
                if missing_cols:
                    logger.warning(
                        f"Extreme (shared) features missing: {len(missing_cols)}/{len(self.extreme_feature_cols)} "
                        f"(sample: {missing_cols[:25]})"
                    )

                feature_values = []
                for col in self.extreme_feature_cols:
                    val = features.get(col, 0.0)
                    if val is None:
                        val = 0.0
                    feature_values.append(val)

                X = pd.DataFrame([feature_values], columns=self.extreme_feature_cols)

                # Numeric coercion
                for col in X.columns:
                    if col not in self.extreme_cat_features:
                        X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0.0)

                # Safer categorical coercion + invalid counter
                shared_cat_invalid_count = 0
                shared_invalid_cols = []
                for col in self.extreme_cat_features:
                    if col in X.columns:
                        raw = X[col].iloc[0]
                        coerced = (
                            pd.to_numeric(pd.Series([raw]), errors="coerce")
                            .fillna(-1)
                            .astype(int)
                            .iloc[0]
                        )
                        if coerced <= 0:
                            shared_cat_invalid_count += 1
                            shared_invalid_cols.append(col)
                        X[col] = pd.Series([coerced]).astype(str)

                if shared_cat_invalid_count:
                    logger.warning(
                        f"Extreme (shared) categorical invalid (<=0) count: {shared_cat_invalid_count}/{len(self.extreme_cat_features)} "
                        f"(cols: {shared_invalid_cols[:20]})"
                    )

                low_prob = float(self.extreme_low_model.predict_proba(X)[0, 1])
                high_prob = float(self.extreme_high_model.predict_proba(X)[0, 1])

            # Determine prediction
            prediction = "NORMAL"
            confidence = 0.0
            should_bet = False
            reason = ""

            # ===== IN-GAME MODEL (if available and game started) =====
            game_time_min = features.get("game_time_min_live", 0)
            ingame_used = False

            if game_time_min > 0 and self.use_ingame_model:
                # Use in-game model instead of pre-game model
                ingame_result = self._predict_ingame(features, game_time_min)
                if ingame_result is not None:
                    low_prob = ingame_result["low_prob"]
                    high_prob = ingame_result["high_prob"]
                    ingame_used = True
                    reason = f"[InGame min={int(game_time_min)}] "

                    # Surface observability fields if present
                    if isinstance(ingame_result, dict):
                        if "missing_count" in ingame_result:
                            reason += (
                                f"(missing={ingame_result.get('missing_count', 0)}) "
                            )
                        if "cat_invalid_count" in ingame_result:
                            reason += f"(cat_invalid={ingame_result.get('cat_invalid_count', 0)}) "

            if low_prob >= 0.5 and low_prob > high_prob:
                prediction = "LOW"
                confidence = low_prob
                if low_prob >= 0.6:
                    should_bet = True
                    reason = f"HIGH confidence LOW (<{self.extreme_low_threshold})"
                else:
                    reason = f"Medium confidence LOW"
            elif high_prob >= 0.5 and high_prob > low_prob:
                prediction = "HIGH"
                confidence = high_prob
                if high_prob >= 0.6:
                    should_bet = True
                    reason = f"HIGH confidence HIGH (>{self.extreme_high_threshold})"
                else:
                    reason = f"Medium confidence HIGH"
            else:
                prediction = "NORMAL"
                confidence = 1.0 - max(low_prob, high_prob)
                reason = "Normal game expected"

            return {
                "low_prob": low_prob,
                "high_prob": high_prob,
                "prediction": prediction,
                "confidence": confidence,
                "should_bet": should_bet,
                "reason": reason,
                "low_threshold": self.extreme_low_threshold,
                "high_threshold": self.extreme_high_threshold,
                "ingame_used": ingame_used,
            }

        except Exception as e:
            logger.error(f"Error in predict_extreme: {e}")
            return {
                "low_prob": 0.0,
                "high_prob": 0.0,
                "prediction": "NORMAL",
                "confidence": 0.0,
                "should_bet": False,
                "reason": f"Error: {e}",
            }

    def predict_winrate(
        self,
        features: Dict[str, Any],
        threshold: Optional[float] = None,
        match_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Predict match winner (radiant_win) using winrate classifier.

        Args:
            features: Dict with pre-game features
            threshold: Confidence threshold for betting (default 0.65)
            match_id: Optional match identifier (for logging/analysis)

        Returns:
            Dict with:
            - radiant_prob: probability of radiant win
            - dire_prob: probability of dire win (1 - radiant_prob)
            - prediction: 'RADIANT', 'DIRE', or 'SKIP'
            - confidence: confidence level (0-1)
            - should_bet: True if confidence >= threshold
            - reason: explanation
        """
        if not self.use_winrate_classifier:
            return {
                "radiant_prob": 0.5,
                "dire_prob": 0.5,
                "prediction": "SKIP",
                "confidence": 0.0,
                "should_bet": False,
                "reason": "Winrate classifier not loaded",
                "missing_count": 0,
                "missing_cols_sample": [],
                "cat_invalid_count": 0,
                "match_id": match_id,
            }

        try:
            # ===== Anti-leakage sanitization (pre-game) =====
            #
            # Goal: prevent offline/online skew where post-match features accidentally leak into pre-game prediction.
            # We do NOT try to be clever here: we hard-zero suspicious keys if they show up, and we log once per call.
            #
            # Note: the model was trained with a specific feature list; we keep the same schema, but sanitize inputs.
            LEAKY_SUBSTRINGS = (
                "_hero_damage",
                "_tower_damage",
                "_dota_plus",
                "radiant_score",
                "dire_score",
                "total_hero_damage",
                "total_tower_damage",
                "total_healing",
            )
            LEAKY_SUFFIXES = (
                "_kills",
                "_deaths",
                "_assists",
                "_gpm",
                "_xpm",
                "_lh",
                "_dn",
            )

            sanitized_features = dict(features)
            leaky_cols_hit = []
            for col in self.winrate_feature_cols:
                if col in self.winrate_cat_features:
                    continue
                is_player_stat = "player_" in col
                is_leaky_match_stat = any(s in col for s in LEAKY_SUBSTRINGS) or any(
                    col.endswith(s) for s in LEAKY_SUFFIXES
                )
                is_leaky_non_player = col in (
                    "radiant_score",
                    "dire_score",
                    "total_kills",
                    "duration_min",
                    "duration",
                )
                if (is_player_stat and is_leaky_match_stat) or is_leaky_non_player:
                    if sanitized_features.get(col) not in (None, 0, 0.0):
                        leaky_cols_hit.append(col)
                    sanitized_features[col] = 0.0

            if leaky_cols_hit:
                logger.warning(
                    f"Winrate anti-leakage: zeroed {len(leaky_cols_hit)} suspicious feature(s) "
                    f"(sample: {leaky_cols_hit[:25]})"
                )

            # Build feature vector (+ track missing)
            missing_cols = [
                col
                for col in self.winrate_feature_cols
                if col not in sanitized_features or sanitized_features.get(col) is None
            ]
            missing_count = len(missing_cols)
            if missing_count:
                logger.warning(
                    f"Winrate features missing: {missing_count}/{len(self.winrate_feature_cols)} "
                    f"(sample: {missing_cols[:15]})"
                )

            # ===== Dynamic thresholding (data quality aware) =====
            #
            # If key context features are missing, we should be more conservative with bets.
            # This improves realized winrate by avoiding low-information situations.
            if threshold is None:
                base_threshold = float(getattr(self, "winrate_default_threshold", 0.60))
            else:
                base_threshold = float(threshold)

            # These are "key context" signals frequently absent in live; when missing,
            # pre-game winrate becomes less reliable.
            key_context_cols = []

            key_missing = [
                c
                for c in key_context_cols
                if (c in self.winrate_feature_cols)
                and (sanitized_features.get(c) is None)
            ]

            # Another strong reliability proxy: team IDs/rating availability
            team_ids_missing = sanitized_features.get("radiant_team_id") in (
                None,
                -1,
                0,
            ) or sanitized_features.get("dire_team_id") in (None, -1, 0)

            tier_diff = sanitized_features.get("tier_diff")
            if tier_diff is None:
                tier_diff = 0.0
            try:
                tier_diff_f = float(tier_diff)
            except Exception:
                tier_diff_f = 0.0

            gdiff = sanitized_features.get("glicko_rating_diff")
            if gdiff is None:
                gdiff = 0.0
            try:
                gdiff_abs = abs(float(gdiff))
            except Exception:
                gdiff_abs = 0.0

            if threshold is None and getattr(self, "winrate_include_rolling_dna", False):
                if tier_diff_f >= 1.0 and gdiff_abs >= 25.0:
                    base_threshold = 0.61
                else:
                    base_threshold = max(base_threshold, 0.68)

            threshold_bump = 0.0

            if getattr(self, "winrate_include_team_ids", True):
                both_teams_reliable = sanitized_features.get("both_teams_reliable")
                if both_teams_reliable is None:
                    both_teams_reliable = 0.0
                if (not getattr(self, "winrate_include_rolling_dna", False)) and (
                    float(both_teams_reliable) < 0.5
                ):
                    threshold_bump += 0.02

                if not getattr(self, "winrate_include_rolling_dna", False):
                    if tier_diff_f >= 1.0:
                        threshold_bump -= 0.01

            min_threshold = float(getattr(self, "winrate_min_threshold", 0.55))
            effective_threshold = min(
                0.80, max(min_threshold, base_threshold + threshold_bump)
            )
            if effective_threshold != base_threshold:
                logger.info(
                    f"Winrate dynamic threshold: base={base_threshold:.2f} -> effective={effective_threshold:.2f} "
                    f"(key_missing={len(key_missing)}, team_ids_missing={team_ids_missing})"
                )

            feature_values = []
            for col in self.winrate_feature_cols:
                val = sanitized_features.get(col, 0.0)
                if val is None:
                    val = 0.0
                feature_values.append(val)

            X = pd.DataFrame([feature_values], columns=self.winrate_feature_cols)

            # Handle numeric features
            for col in X.columns:
                if col not in self.winrate_cat_features:
                    X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0.0)

            # Handle categorical features (safer coercion + count invalids)
            cat_invalid_count = 0
            for col in self.winrate_cat_features:
                if col in X.columns:
                    raw = X[col].iloc[0]
                    coerced = (
                        pd.to_numeric(pd.Series([raw]), errors="coerce")
                        .fillna(-1)
                        .astype(int)
                        .iloc[0]
                    )
                    if coerced <= 0:
                        cat_invalid_count += 1
                    X[col] = pd.Series([coerced]).astype(str)
            if cat_invalid_count:
                logger.warning(
                    f"Winrate categorical invalid (<=0) count: {cat_invalid_count}/{len(self.winrate_cat_features)}"
                )

            # Predict
            radiant_prob = float(self.winrate_model.predict_proba(X)[0, 1])
            dire_prob = 1.0 - radiant_prob

            # Determine prediction (use effective_threshold)
            if radiant_prob >= effective_threshold:
                prediction = "RADIANT"
                confidence = radiant_prob
                should_bet = True
                reason = f"Radiant favored ({radiant_prob:.0%})"
            elif dire_prob >= effective_threshold:
                prediction = "DIRE"
                confidence = dire_prob
                should_bet = True
                reason = f"Dire favored ({dire_prob:.0%})"
            else:
                prediction = "SKIP"
                confidence = max(radiant_prob, dire_prob)
                should_bet = False
                reason = (
                    f"Low confidence ({confidence:.0%} < {effective_threshold:.0%})"
                )

            if (
                (prediction == "SKIP")
                and (not getattr(self, "winrate_include_team_ids", True))
                and (confidence >= max(min_threshold, effective_threshold - 0.01))
            ):
                side_sign = 1.0 if radiant_prob >= dire_prob else -1.0
                blood_diff = sanitized_features.get("blood_score_diff")
                armor_diff = sanitized_features.get("armor_diff")
                ult_diff = sanitized_features.get("big_ult_synergy_diff")
                try:
                    blood_signed = float(blood_diff or 0.0) * side_sign
                except Exception:
                    blood_signed = 0.0
                try:
                    armor_signed = float(armor_diff or 0.0) * side_sign
                except Exception:
                    armor_signed = 0.0
                try:
                    ult_signed = float(ult_diff or 0.0) * side_sign
                except Exception:
                    ult_signed = 0.0

                if (blood_signed >= 6.74) or (armor_signed >= 6.83) or (ult_signed >= 4.5):
                    prediction = "RADIANT" if side_sign > 0 else "DIRE"
                    should_bet = True
                    reason = f"Draft diff boost ({confidence:.0%})"

            # ===== Persist winrate prediction logs for later analysis =====
            #
            # We log ALL predictions (not only bet signals) to enable:
            # - calibration plots
            # - threshold optimization
            # - error analysis on "near-miss" games
            #
            # This is intentionally JSONL so it can be appended safely and analyzed later.
            try:
                import time
                from pathlib import Path as _Path

                log_dir = _Path(BASE_DIR) / "logs"
                log_dir.mkdir(parents=True, exist_ok=True)
                log_path = log_dir / "winrate_bets.jsonl"

                # Enrich logs with draft/team context if available in the features dict.
                # These keys are produced by build_features() for other models and are safe to log.
                bet_record = {
                    "ts": int(time.time()),
                    "match_id": match_id,
                    "threshold": effective_threshold,
                    "prediction": prediction,
                    "confidence": confidence,
                    "radiant_prob": radiant_prob,
                    "dire_prob": dire_prob,
                    "should_bet": should_bet,
                    "missing_count": missing_count,
                    "missing_cols_sample": missing_cols[:25],
                    "cat_invalid_count": cat_invalid_count,
                    "anti_leakage_zeroed_count": len(leaky_cols_hit),
                    "anti_leakage_zeroed_sample": leaky_cols_hit[:25],
                    "radiant_team_id": features.get("radiant_team_id"),
                    "dire_team_id": features.get("dire_team_id"),
                    "radiant_tier": features.get("radiant_tier"),
                    "dire_tier": features.get("dire_tier"),
                    "glicko_rating_diff": features.get("glicko_rating_diff"),
                    "glicko_rating_win_prob": features.get("glicko_rating_win_prob"),
                    "series_game_num": features.get("series_game_num"),
                    "is_decider_game": features.get("is_decider_game"),
                    "league_avg_kills": features.get("league_avg_kills"),
                    "league_kills_std": features.get("league_kills_std"),
                    "league_meta_diff": features.get("league_meta_diff"),
                    "radiant_heroes": [
                        features.get("radiant_hero_1"),
                        features.get("radiant_hero_2"),
                        features.get("radiant_hero_3"),
                        features.get("radiant_hero_4"),
                        features.get("radiant_hero_5"),
                    ],
                    "dire_heroes": [
                        features.get("dire_hero_1"),
                        features.get("dire_hero_2"),
                        features.get("dire_hero_3"),
                        features.get("dire_hero_4"),
                        features.get("dire_hero_5"),
                    ],
                }
                with open(log_path, "a") as f:
                    f.write(json.dumps(bet_record, ensure_ascii=False) + "\n")
            except Exception as _e:
                logger.warning(f"Failed to write winrate bet log: {_e}")

            return {
                "radiant_prob": radiant_prob,
                "dire_prob": dire_prob,
                "prediction": prediction,
                "confidence": confidence,
                "should_bet": should_bet,
                "reason": reason,
                "threshold": effective_threshold,
                "base_threshold": threshold,
                "threshold_bump": threshold_bump,
                "key_context_missing_count": len(key_missing),
                "key_context_missing_sample": key_missing[:25],
                "team_ids_missing": team_ids_missing,
                "missing_count": missing_count,
                "missing_cols_sample": missing_cols[:25],
                "cat_invalid_count": cat_invalid_count,
                "anti_leakage_zeroed_count": len(leaky_cols_hit),
                "anti_leakage_zeroed_sample": leaky_cols_hit[:25],
                "match_id": match_id,
            }

        except Exception as e:
            logger.error(f"Error in predict_winrate: {e}")
            return {
                "radiant_prob": 0.5,
                "dire_prob": 0.5,
                "prediction": "SKIP",
                "confidence": 0.0,
                "should_bet": False,
                "reason": f"Error: {e}",
                "match_id": match_id,
            }

    def predict_match(
        self,
        radiant_heroes_and_pos: Dict,
        dire_heroes_and_pos: Dict,
        radiant_team_name: str = "",
        dire_team_name: str = "",
        radiant_team_id: Optional[int] = None,
        dire_team_id: Optional[int] = None,
        match_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Делает предсказание для матча на основе драфта.
        """
        # Resolve team IDs from names if not provided
        radiant_team_id = self._resolve_team_id(radiant_team_name, radiant_team_id)
        dire_team_id = self._resolve_team_id(dire_team_name, dire_team_id)

        # Извлекаем hero IDs и account IDs
        radiant_ids, dire_ids = self.extract_heroes_from_draft(
            radiant_heroes_and_pos, dire_heroes_and_pos
        )

        if len(radiant_ids) < 5 or len(dire_ids) < 5:
            return {
                "error": f"Incomplete draft: Radiant {len(radiant_ids)}/5, Dire {len(dire_ids)}/5"
            }

        # Extract account IDs if available
        radiant_account_ids = []
        dire_account_ids = []
        for pos in ["pos1", "pos2", "pos3", "pos4", "pos5"]:
            if pos in radiant_heroes_and_pos:
                acc_id = radiant_heroes_and_pos[pos].get("account_id", 0)
                radiant_account_ids.append(acc_id if acc_id else 0)
            if pos in dire_heroes_and_pos:
                acc_id = dire_heroes_and_pos[pos].get("account_id", 0)
                dire_account_ids.append(acc_id if acc_id else 0)

        # Build features (full set used by the other models)
        features = self.build_features(
            radiant_ids,
            dire_ids,
            radiant_account_ids if any(radiant_account_ids) else None,
            dire_account_ids if any(dire_account_ids) else None,
            radiant_team_id=radiant_team_id,
            dire_team_id=dire_team_id,
        )

        # Winrate prediction should use a stable, pre-game-specific feature set to reduce missingness
        # and improve coverage/robustness.
        winrate_radiant_team_id = (
            radiant_team_id if getattr(self, "winrate_include_team_ids", True) else None
        )
        winrate_dire_team_id = (
            dire_team_id if getattr(self, "winrate_include_team_ids", True) else None
        )
        winrate_features = self.build_winrate_features(
            radiant_ids=radiant_ids,
            dire_ids=dire_ids,
            radiant_team_id=winrate_radiant_team_id,
            dire_team_id=winrate_dire_team_id,
            radiant_account_ids=radiant_account_ids if any(radiant_account_ids) else None,
            dire_account_ids=dire_account_ids if any(dire_account_ids) else None,
        )
        winrate_pred = self.predict_winrate(
            features=winrate_features,
            match_id=match_id,
        )

        # Get model predictions (all 5 values)
        kills_prob, predicted_kills, winner_prob, duration_pred, kpm_pred = (
            self.predict_with_models(features)
        )

        # ============ THE BOOSTER LOGIC V3 ============
        # Thresholds
        TIER1_THRESHOLD = 0.56  # High confidence
        WEAK_SIGNAL_MIN = 0.51  # Weak signal zone start
        WEAK_SIGNAL_MAX = 0.56  # Weak signal zone end

        # Booster thresholds
        BOOST_OVER_DURATION = 38.0  # Long game threshold
        BOOST_OVER_KPM = 1.1  # High KPM threshold
        BOOST_UNDER_DURATION = 32.0  # Short game threshold
        BOOST_UNDER_KPM = 0.9  # Low KPM threshold

        # Stomp protection threshold
        STOMP_WINNER_CONF = 0.75  # Clear favorite

        # Determine kills signal
        kills_prediction = None
        kills_confidence = 0.5
        signal_reason = "No Signal"
        should_bet = False
        is_blocked = False

        # TIER 1: Strong Signal (conf > 56%)
        if kills_prob > TIER1_THRESHOLD:
            kills_prediction = "OVER"
            kills_confidence = kills_prob
            signal_reason = "Tier 1 Strong (Over)"
            should_bet = True
        elif kills_prob < (1 - TIER1_THRESHOLD):
            kills_prediction = "UNDER"
            kills_confidence = 1 - kills_prob
            signal_reason = "Tier 1 Strong (Under)"
            should_bet = True

        # TIER 2: Weak Signal Rescue (THE BOOSTER)
        elif WEAK_SIGNAL_MIN < kills_prob < WEAK_SIGNAL_MAX:
            # Weak Over signal - check if physics supports it
            if duration_pred > BOOST_OVER_DURATION and kpm_pred > BOOST_OVER_KPM:
                kills_prediction = "OVER"
                kills_confidence = kills_prob + 0.05  # Boost confidence
                signal_reason = "Boosted by Physics (Over: long game + high KPM)"
                should_bet = True

        elif (1 - WEAK_SIGNAL_MAX) < kills_prob < (1 - WEAK_SIGNAL_MIN):
            # Weak Under signal - check if physics supports it
            if duration_pred < BOOST_UNDER_DURATION and kpm_pred < BOOST_UNDER_KPM:
                kills_prediction = "UNDER"
                kills_confidence = (1 - kills_prob) + 0.05  # Boost confidence
                signal_reason = "Boosted by Physics (Under: short game + low KPM)"
                should_bet = True

        # Default if no signal
        if kills_prediction is None:
            kills_prediction = "OVER" if kills_prob > 0.5 else "UNDER"
            kills_confidence = max(kills_prob, 1 - kills_prob)
            signal_reason = "No Strong Signal"
            should_bet = False

        # SAFETY NET: Stomp Protection
        # If clear favorite (>75% winner conf) -> block OVER bets (risk of 20 min stomp)
        winner_confidence_raw = max(winner_prob, 1 - winner_prob)
        if (
            winner_confidence_raw > STOMP_WINNER_CONF
            and kills_prediction == "OVER"
            and should_bet
        ):
            is_blocked = True
            signal_reason += " [BLOCKED: Stomp Risk]"
            should_bet = False

        # ============ TEAM RATINGS BOOST ============
        # Get team ratings and calculate rating-based win probability
        radiant_rating, radiant_rd, radiant_rating_name = self._get_team_rating(
            radiant_team_id
        )
        dire_rating, dire_rd, dire_rating_name = self._get_team_rating(dire_team_id)

        rating_diff = radiant_rating - dire_rating
        rating_win_prob = self._calculate_rating_win_prob(
            radiant_rating, radiant_rd, dire_rating, dire_rd
        )

        # Combine model prediction with rating prediction
        # Weight: 60% model, 40% rating (if ratings are reliable, i.e., low RD)
        rating_weight = 0.0
        if radiant_rd < 150 and dire_rd < 150:
            rating_weight = 0.4  # Both teams have reliable ratings
        elif radiant_rd < 200 and dire_rd < 200:
            rating_weight = 0.25  # Moderate confidence in ratings
        elif radiant_rd < 300 and dire_rd < 300:
            rating_weight = 0.1  # Low confidence

        # Blend model and rating predictions
        combined_winner_prob = (
            winner_prob * (1 - rating_weight) + rating_win_prob * rating_weight
        )

        # Winner prediction with rating boost
        if combined_winner_prob > 0.55:
            winner_prediction = "RADIANT"
            winner_confidence = combined_winner_prob
        elif combined_winner_prob < 0.45:
            winner_prediction = "DIRE"
            winner_confidence = 1 - combined_winner_prob
        else:
            winner_prediction = "RADIANT" if combined_winner_prob > 0.5 else "DIRE"
            winner_confidence = max(combined_winner_prob, 1 - combined_winner_prob)

        # RATING BOOST: If rating diff > 200, boost confidence significantly
        if abs(rating_diff) > 200 and rating_weight > 0:
            if rating_diff > 200:  # Radiant much stronger
                winner_prediction = "RADIANT"
                winner_confidence = max(
                    winner_confidence, 0.70 + min(rating_diff - 200, 200) / 1000
                )
            elif rating_diff < -200:  # Dire much stronger
                winner_prediction = "DIRE"
                winner_confidence = max(
                    winner_confidence, 0.70 + min(abs(rating_diff) - 200, 200) / 1000
                )

        # Update stomp protection with rating info
        winner_confidence_raw = winner_confidence
        if (
            winner_confidence_raw > STOMP_WINNER_CONF
            and kills_prediction == "OVER"
            and should_bet
        ):
            is_blocked = True
            signal_reason += " [BLOCKED: Stomp Risk]"
            should_bet = False

        # Duration prediction
        if duration_pred > 40:
            duration_prediction = "OVER_TIME"
            duration_confidence = 0.55 + (duration_pred - 40) * 0.02
        elif duration_pred < 34:
            duration_prediction = "UNDER_TIME"
            duration_confidence = 0.55 + (34 - duration_pred) * 0.02
        else:
            duration_prediction = "OVER_TIME" if duration_pred > 37 else "UNDER_TIME"
            duration_confidence = 0.52
        duration_confidence = min(0.70, duration_confidence)

        # Best Bet Selection
        kills_edge = abs(kills_confidence - 0.5) if should_bet else 0.0
        winner_edge = abs(winner_confidence - 0.5)
        duration_edge = abs(duration_confidence - 0.5)

        edges = [
            ("TOTAL", kills_prediction, kills_edge, kills_confidence),
            ("WINNER", winner_prediction, winner_edge, winner_confidence),
            ("DURATION", duration_prediction, duration_edge, duration_confidence),
        ]

        # ============ TIER-2 MATCH ADJUSTMENT ============
        # Tier-2 matches tend to have more kills (less disciplined play)
        is_tier_one_match = False
        is_tier_two_match = False
        tier_adjustment_applied = False

        if TEAM_MAPPING_AVAILABLE and radiant_team_id and dire_team_id:
            is_tier_one, is_tier_two, _ = get_match_tier_info(
                radiant_team_id, dire_team_id
            )
            is_tier_one_match = is_tier_one
            is_tier_two_match = is_tier_two

            # Tier-2 correction: +15% boost to OVER probability
            if is_tier_two:
                tier_adjustment = 0.15
                adjusted_kills_prob = min(0.85, kills_prob + tier_adjustment)

                # Recalculate kills prediction with adjustment
                if adjusted_kills_prob > TIER1_THRESHOLD:
                    kills_prediction = "OVER"
                    kills_confidence = adjusted_kills_prob
                    signal_reason = (
                        f"Tier 2 Boosted (Over) [+{tier_adjustment * 100:.0f}%]"
                    )
                    should_bet = True
                    tier_adjustment_applied = True

                    # Update edges with new values
                    kills_edge = abs(kills_confidence - 0.5)
                    edges[0] = ("TOTAL", kills_prediction, kills_edge, kills_confidence)

        best = max(edges, key=lambda x: x[2])

        return {
            "total_kills": {
                "prediction": kills_prediction,
                "confidence": kills_confidence,
                "predicted_kills": predicted_kills,
                "line": self.bk_line,
                "model_prob": kills_prob,
                "should_bet": should_bet,
                "signal_reason": signal_reason,
                "is_blocked": is_blocked,
            },
            "winner": {
                "prediction": winner_prediction,
                "confidence": winner_confidence,
                "model_prob": winner_prob,
                "rating_prob": rating_win_prob,
                "rating_diff": rating_diff,
                "radiant_rating": radiant_rating,
                "dire_rating": dire_rating,
            },
            "winrate": winrate_pred,
            "duration": {
                "prediction": duration_prediction,
                "confidence": duration_confidence,
                "predicted_minutes": duration_pred,
            },
            "kpm": {
                "predicted": kpm_pred,
            },
            "best_bet": {
                "market": best[0],
                "side": best[1],
                "edge": best[2],
                "confidence": best[3],
            },
            "booster": {
                "should_bet": should_bet,
                "signal_reason": signal_reason,
                "is_blocked": is_blocked,
                "duration_pred": duration_pred,
                "kpm_pred": kpm_pred,
            },
            "features": {
                "combined_blood_score": features.get("combined_blood_score", 0),
                "total_blood_potential": features.get("total_blood_potential", 0),
                "combined_greed": features.get("combined_greed", 0),
                "combined_push_score": features.get("combined_push_score", 0),
            },
            "teams": {
                "radiant": radiant_team_name,
                "dire": dire_team_name,
                "radiant_id": radiant_team_id,
                "dire_id": dire_team_id,
                "is_tier_one_match": is_tier_one_match,
                "is_tier_two_match": is_tier_two_match,
            },
            "heroes": {
                "radiant": radiant_ids,
                "dire": dire_ids,
            },
        }

    def format_prediction_message(self, prediction: Dict) -> str:
        """Форматирует предсказание в читаемое сообщение."""
        if "error" in prediction:
            return f"❌ Ошибка: {prediction['error']}"

        teams = prediction.get("teams", {})
        radiant = teams.get("radiant", "Radiant")
        dire = teams.get("dire", "Dire")

        total = prediction["total_kills"]
        winner = prediction["winner"]
        duration = prediction["duration"]
        best = prediction["best_bet"]
        feats = prediction.get("features", {})

        # Rating info
        radiant_rating = winner.get("radiant_rating", 1500)
        dire_rating = winner.get("dire_rating", 1500)
        rating_diff = winner.get("rating_diff", 0)

        lines = [
            f"🎮 {radiant} vs {dire}",
            f"📊 Ratings: {radiant_rating:.0f} vs {dire_rating:.0f} (diff: {rating_diff:+.0f})",
            "",
            "📊 ПРЕДСКАЗАНИЯ:",
            f"  💀 Total Kills: {total['prediction']} {total['line']:.0f} ({total['confidence'] * 100:.0f}%)",
            f"     Прогноз: ~{total['predicted_kills']:.0f} киллов (model: {total.get('model_prob', 0.5) * 100:.0f}%)",
            f"  🏆 Winner: {winner['prediction']} ({winner['confidence'] * 100:.0f}%)",
            f"     Model: {winner.get('model_prob', 0.5) * 100:.0f}%, Rating: {winner.get('rating_prob', 0.5) * 100:.0f}%",
            f"  ⏱️ Duration: {duration['prediction']} ({duration['confidence'] * 100:.0f}%)",
            f"     Прогноз: ~{duration['predicted_minutes']:.0f} мин",
        ]

        # Booster info
        booster = prediction.get("booster", {})
        kpm = prediction.get("kpm", {})

        if booster.get("should_bet"):
            lines.append(f"🚀 THE BOOSTER: ✅ BET SIGNAL")
            lines.append(f"   Reason: {booster.get('signal_reason', 'Unknown')}")
        elif booster.get("is_blocked"):
            lines.append(f"🚀 THE BOOSTER: ⛔ BLOCKED")
            lines.append(f"   Reason: {booster.get('signal_reason', 'Unknown')}")
        else:
            lines.append(f"🚀 THE BOOSTER: ❌ No Signal")
            lines.append(
                f"   Reason: {booster.get('signal_reason', 'Weak confidence')}"
            )

        lines.append(
            f"   Duration: {booster.get('duration_pred', 37):.1f} min, KPM: {kpm.get('predicted', 1.2):.2f}"
        )
        lines.append("")

        lines.append(f"🎯 ЛУЧШАЯ СТАВКА: {best['market']} - {best['side']}")
        lines.append(
            f"   Edge: {best['edge'] * 100:.1f}%, Conf: {best['confidence'] * 100:.0f}%"
        )
        lines.append("")
        lines.append("📈 KEY FEATURES:")
        lines.append(f"   Blood: {feats.get('combined_blood_score', 0):+.1f}")
        lines.append(
            f"   Blood Potential: {feats.get('total_blood_potential', 0):+.1f}"
        )
        lines.append(f"   Greed: {feats.get('combined_greed', 0):.2f}")
        lines.append(f"   Push: {feats.get('combined_push_score', 0):+.1f}")

        return "\n".join(lines)


# Глобальный экземпляр предсказателя
_predictor: Optional[LivePredictor] = None


def get_predictor() -> LivePredictor:
    """Возвращает глобальный экземпляр предсказателя."""
    global _predictor
    if _predictor is None:
        _predictor = LivePredictor()
    return _predictor


def predict_live_match(
    radiant_heroes_and_pos: Dict,
    dire_heroes_and_pos: Dict,
    radiant_team_name: str = "",
    dire_team_name: str = "",
    radiant_team_id: Optional[int] = None,
    dire_team_id: Optional[int] = None,
) -> Tuple[Dict, str]:
    """
    Главная функция для предсказания live матча.

    Returns:
        (prediction_dict, formatted_message)
    """
    predictor = get_predictor()
    prediction = predictor.predict_match(
        radiant_heroes_and_pos=radiant_heroes_and_pos,
        dire_heroes_and_pos=dire_heroes_and_pos,
        radiant_team_name=radiant_team_name,
        dire_team_name=dire_team_name,
        radiant_team_id=radiant_team_id,
        dire_team_id=dire_team_id,
    )
    message = predictor.format_prediction_message(prediction)
    return prediction, message


if __name__ == "__main__":
    # Тест
    radiant = {
        "pos1": {"hero_id": 136},  # Shadow Fiend
        "pos2": {"hero_id": 13},  # Puck
        "pos3": {"hero_id": 137},  # Pangolier
        "pos4": {"hero_id": 100},  # Hoodwink
        "pos5": {"hero_id": 128},  # Snapfire
    }
    dire = {
        "pos1": {"hero_id": 95},  # Invoker
        "pos2": {"hero_id": 49},  # Dragon Knight
        "pos3": {"hero_id": 129},  # Mars
        "pos4": {"hero_id": 101},  # Skywrath Mage
        "pos5": {"hero_id": 84},  # Ogre Magi
    }

    prediction, message = predict_live_match(
        radiant, dire, radiant_team_name="Nemiga", dire_team_name="Kalmychata"
    )

    print(message)
