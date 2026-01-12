"""
Train Live Models with CatBoost: использует категориальные фичи для героев и команд.

CatBoost автоматически выучит "эмбеддинги" героев и команд.

Использует LivePredictor.build_features() для генерации фичей из hero IDs,
включая новые Stratz фичи (matchup_advantage, tankiness, burst_rating и т.д.)

Сохраняет:
- ml-models/live_cb_kills.cbm - CatBoost classifier для Total Kills
- ml-models/live_cb_winner.cbm - CatBoost classifier для Winner
- ml-models/live_cb_duration.cbm - CatBoost regressor для Duration
- ml-models/live_cb_kpm.cbm - CatBoost regressor для KPM
- ml-models/live_cb_meta.json - metadata с feature_cols и cat_features
"""

import json
import logging
import pickle
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, CatBoostRegressor, Pool
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, mean_absolute_error, roc_auc_score
from tqdm import tqdm

# Add src to path for LivePredictor import
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
from live_predictor import LivePredictor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MODELS_DIR = Path('ml-models')
CONFIG_DIR = Path('config')
META_FEATURE_COLS = ["kills_prob", "duration_pred", "kpm_pred", "kills_formula"]


def get_feature_columns(df: pd.DataFrame) -> Tuple[List[str], List[str], List[int]]:
    """
    Возвращает списки фичей и индексы категориальных фичей.
    Использует только фичи, которые можно вычислить в build_features().
    
    Returns:
        (all_features, cat_feature_names, cat_feature_indices)
    """
    # ============ CATEGORICAL COLUMNS ============
    # Hero ID columns
    hero_cols = []
    for team in ['radiant', 'dire']:
        for pos in range(1, 6):
            col = f'{team}_hero_{pos}'
            if col in df.columns:
                hero_cols.append(col)
    
    # Team ID columns
    team_cols = []
    for col in ['radiant_team_id', 'dire_team_id']:
        if col in df.columns:
            team_cols.append(col)
    
    meta_cols = []
    for col in ["league_id", "series_type", "region_id", "patch_id"]:
        if col in df.columns:
            meta_cols.append(col)

    cat_features = hero_cols + team_cols + meta_cols
    
    # ============ NUMERIC FEATURES (only those computable in build_features) ============
    # These are the features that LivePredictor.build_features() can compute
    ALLOWED_PREFIXES = [
        # Blood stats
        'blood', 'radiant_blood', 'dire_blood', 'combined_blood', 'match_blood',
        # CC stats
        'cc_score', 'radiant_cc', 'dire_cc', 'combined_cc',
        # Greed
        'greed', 'radiant_greed', 'dire_greed', 'combined_greed', 'greedy',
        # Push
        'push_score', 'radiant_push', 'dire_push', 'combined_push',
        # Heal
        'heal_score', 'radiant_heal', 'dire_heal', 'combined_heal',
        # Wave clear
        'wave_clear', 'radiant_wave', 'dire_wave', 'combined_wave',
        # Power spikes
        'early_power', 'late_power', 'radiant_early', 'radiant_late',
        'dire_early', 'dire_late', 'combined_early', 'combined_late',
        # Synergy
        'synergy', 'radiant_draft', 'dire_draft', 'total_draft', 'draft_synergy',
        # Per-hero stats
        'radiant_hero_', 'dire_hero_',
        # Roster stability
        'roster',
    ]
    
    ALLOWED_EXACT = [
        'push_wave_clear_ratio',
        'high_blood_match', 'low_blood_match',
        'both_greedy', 'both_greedy_carries', 'high_greed_match',
    ]
    
    numeric_features = []
    for col in df.columns:
        # Skip categorical
        if col in cat_features:
            continue
        
        # Check if column matches allowed patterns
        col_lower = col.lower()
        
        # Check exact matches
        if col in ALLOWED_EXACT:
            numeric_features.append(col)
            continue
        
        # Check prefix matches
        for prefix in ALLOWED_PREFIXES:
            if col_lower.startswith(prefix) or prefix in col_lower:
                # Exclude post-match stats
                if 'per_min' in col_lower and 'heal_score_per_min' not in col_lower:
                    continue
                numeric_features.append(col)
                break
    
    # Sort for consistency
    numeric_features = sorted(list(set(numeric_features)))
    
    # ============ COMBINE ============
    all_features = cat_features + numeric_features
    
    # Filter to only existing columns
    all_features = [f for f in all_features if f in df.columns]
    cat_features = [f for f in cat_features if f in df.columns]
    
    # Get indices of categorical features
    cat_indices = [all_features.index(f) for f in cat_features if f in all_features]
    
    return all_features, cat_features, cat_indices


def prepare_data(
    df: pd.DataFrame,
    feature_cols: List[str],
    cat_features: List[str],
) -> pd.DataFrame:
    """Подготавливает данные для CatBoost."""
    X = df[feature_cols].copy()
    
    # Fill NaN for numeric columns
    for col in X.columns:
        if col not in cat_features:
            X[col] = X[col].fillna(X[col].median())
    
    # Convert categorical to string (CatBoost requirement)
    for col in cat_features:
        if col in X.columns:
            if X[col].dtype.kind in "biufc":
                X[col] = pd.to_numeric(X[col], errors="coerce").fillna(-1).astype(int).astype(str)
            else:
                X[col] = X[col].fillna("UNKNOWN").astype(str)
    
    return X


def _row_value(row: pd.Series, col: str) -> Optional[float]:
    if col not in row.index:
        return None
    val = row[col]
    if pd.isna(val):
        return None
    return val


def _compute_time_decay_weights(
    start_times: pd.Series,
    half_life_days: float = 180.0,
    min_weight: float = 0.2,
) -> np.ndarray:
    times = pd.to_numeric(start_times, errors="coerce").fillna(0).astype(float)
    latest = float(times.max()) if len(times) > 0 else 0.0
    if latest <= 0:
        return np.ones(len(times), dtype=np.float64)
    age_days = (latest - times).clip(lower=0) / 86400.0
    weights = np.power(0.5, age_days / half_life_days)
    if min_weight is not None:
        weights = np.clip(weights, min_weight, 1.0)
    return weights.astype(np.float64)


def _build_meta_features_df(
    kills_prob: np.ndarray,
    duration_pred: np.ndarray,
    kpm_pred: np.ndarray,
) -> pd.DataFrame:
    kills_formula = duration_pred * kpm_pred
    df = pd.DataFrame(
        {
            "kills_prob": kills_prob,
            "duration_pred": duration_pred,
            "kpm_pred": kpm_pred,
            "kills_formula": kills_formula,
        }
    )
    df = df.replace([np.inf, -np.inf], np.nan)
    return df.fillna(df.median())


def train_kills_classifier(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    cat_indices: List[int],
    sample_weight: Optional[np.ndarray] = None,
) -> CatBoostClassifier:
    """Обучает CatBoost классификатор для Total Kills Over/Under."""
    logger.info("Training Kills Classifier (CatBoost)...")
    logger.info(f"  Input features: {X_train.shape[1]}")
    
    model = CatBoostClassifier(
        iterations=2000,
        learning_rate=0.02,
        depth=7,
        loss_function='Logloss',
        eval_metric='AUC',
        cat_features=cat_indices,
        random_seed=42,
        verbose=200,
        early_stopping_rounds=150,
        l2_leaf_reg=5,
        bagging_temperature=0.3,
        rsm=0.8,  # Random subspace method - use 80% features per tree
    )
    
    train_pool = Pool(X_train, y_train, cat_features=cat_indices, weight=sample_weight)
    test_pool = Pool(X_test, y_test, cat_features=cat_indices)
    
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)
    
    # Evaluate
    proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, proba)
    acc = accuracy_score(y_test, (proba > 0.5).astype(int))
    
    logger.info(f"Kills Classifier: AUC={auc:.4f}, Accuracy={acc*100:.2f}%")
    
    return model


def train_winner_classifier(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    cat_indices: List[int],
    sample_weight: Optional[np.ndarray] = None,
) -> CatBoostClassifier:
    """Обучает CatBoost классификатор для Winner."""
    logger.info("Training Winner Classifier (CatBoost)...")
    logger.info(f"  Input features: {X_train.shape[1]}")
    
    model = CatBoostClassifier(
        iterations=2000,
        learning_rate=0.02,
        depth=7,
        loss_function='Logloss',
        eval_metric='AUC',
        cat_features=cat_indices,
        random_seed=42,
        verbose=200,
        early_stopping_rounds=150,
        l2_leaf_reg=5,
        bagging_temperature=0.3,
        rsm=0.8,  # Random subspace method - use 80% features per tree
    )
    
    train_pool = Pool(X_train, y_train, cat_features=cat_indices, weight=sample_weight)
    test_pool = Pool(X_test, y_test, cat_features=cat_indices)
    
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)
    
    proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, proba)
    acc = accuracy_score(y_test, (proba > 0.5).astype(int))
    
    logger.info(f"Winner Classifier: AUC={auc:.4f}, Accuracy={acc*100:.2f}%")
    
    return model


def train_duration_regressor(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    cat_indices: List[int],
    sample_weight: Optional[np.ndarray] = None,
) -> CatBoostRegressor:
    """Обучает CatBoost регрессор для Duration."""
    logger.info("Training Duration Regressor (CatBoost)...")
    logger.info(f"  Input features: {X_train.shape[1]}")
    
    model = CatBoostRegressor(
        iterations=2000,
        learning_rate=0.02,
        depth=7,
        loss_function='RMSE',
        eval_metric='MAE',
        cat_features=cat_indices,
        random_seed=42,
        verbose=200,
        early_stopping_rounds=150,
        l2_leaf_reg=5,
        bagging_temperature=0.3,
        rsm=0.8,  # Random subspace method - use 80% features per tree
    )
    
    train_pool = Pool(X_train, y_train, cat_features=cat_indices, weight=sample_weight)
    test_pool = Pool(X_test, y_test, cat_features=cat_indices)
    
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)
    
    pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    
    logger.info(f"Duration Regressor: MAE={mae:.2f} minutes")
    
    return model


def train_kpm_regressor(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    cat_indices: List[int],
    sample_weight: Optional[np.ndarray] = None,
) -> CatBoostRegressor:
    """Обучает CatBoost регрессор для KPM."""
    logger.info("Training KPM Regressor (CatBoost)...")
    logger.info(f"  Input features: {X_train.shape[1]}")
    
    model = CatBoostRegressor(
        iterations=2000,
        learning_rate=0.02,
        depth=7,
        loss_function='RMSE',
        eval_metric='MAE',
        cat_features=cat_indices,
        random_seed=42,
        verbose=200,
        early_stopping_rounds=150,
        l2_leaf_reg=5,
        bagging_temperature=0.3,
        rsm=0.8,  # Random subspace method - use 80% features per tree
    )
    
    train_pool = Pool(X_train, y_train, cat_features=cat_indices, weight=sample_weight)
    test_pool = Pool(X_test, y_test, cat_features=cat_indices)
    
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)
    
    pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    
    logger.info(f"KPM Regressor: MAE={mae:.3f} kills/min")
    
    return model


def save_models(
    cb_kills: CatBoostClassifier,
    cb_winner: CatBoostClassifier,
    cb_duration: CatBoostRegressor,
    cb_kpm: CatBoostRegressor,
    feature_cols: List[str],
    cat_features: List[str],
    cat_indices: List[int],
    bk_line: float,
) -> None:
    """Сохраняет модели и метаданные."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    
    cb_kills.save_model(str(MODELS_DIR / 'live_cb_kills.cbm'))
    logger.info("Saved: ml-models/live_cb_kills.cbm")
    
    cb_winner.save_model(str(MODELS_DIR / 'live_cb_winner.cbm'))
    logger.info("Saved: ml-models/live_cb_winner.cbm")
    
    cb_duration.save_model(str(MODELS_DIR / 'live_cb_duration.cbm'))
    logger.info("Saved: ml-models/live_cb_duration.cbm")
    
    cb_kpm.save_model(str(MODELS_DIR / 'live_cb_kpm.cbm'))
    logger.info("Saved: ml-models/live_cb_kpm.cbm")
    
    # Save metadata
    metadata = {
        'feature_cols': feature_cols,
        'cat_features': cat_features,
        'cat_indices': cat_indices,
        'bk_line': bk_line,
        'n_features': len(feature_cols),
        'n_cat_features': len(cat_features),
        'version': 'catboost_v1',
    }
    with open(MODELS_DIR / 'live_cb_meta.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    logger.info("Saved: ml-models/live_cb_meta.json")


def generate_features_from_predictor(
    df: pd.DataFrame,
    predictor: LivePredictor,
    include_dna: bool = True,
) -> pd.DataFrame:
    """
    Generate features using LivePredictor.build_features() for each match.
    This ensures we use the latest Stratz features (matchup, tankiness, burst, etc.)
    
    Args:
        df: DataFrame with match data
        predictor: LivePredictor instance
        include_dna: Whether to include player DNA features
    """
    logger.info(f"Generating features (DNA={include_dna})...")
    
    features_list: List[Dict[str, Any]] = []
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Building features"):
        # Extract hero IDs
        radiant_ids = [
            int(row[f'radiant_hero_{i}']) 
            for i in range(1, 6) 
            if pd.notna(row.get(f'radiant_hero_{i}', None))
        ]
        dire_ids = [
            int(row[f'dire_hero_{i}']) 
            for i in range(1, 6) 
            if pd.notna(row.get(f'dire_hero_{i}', None))
        ]
        
        # Get team IDs if available
        radiant_team_id = int(row['radiant_team_id']) if pd.notna(row.get('radiant_team_id')) else None
        dire_team_id = int(row['dire_team_id']) if pd.notna(row.get('dire_team_id')) else None
        
        # Get player IDs for DNA features
        radiant_account_ids: List[int] = []
        dire_account_ids: List[int] = []
        
        if include_dna:
            for i in range(1, 6):
                r_col = f'radiant_player_{i}_id'
                d_col = f'dire_player_{i}_id'
                if r_col in row.index and pd.notna(row[r_col]):
                    radiant_account_ids.append(int(row[r_col]))
                if d_col in row.index and pd.notna(row[d_col]):
                    dire_account_ids.append(int(row[d_col]))

        match_start_time = _row_value(row, "start_time")
        if match_start_time is not None:
            match_start_time = int(match_start_time)

        h2h_avg_total = _row_value(row, "h2h_avg_total")
        h2h_matches_count = _row_value(row, "h2h_matches_count")
        league_avg_kills = _row_value(row, "league_avg_kills")
        league_kills_std = _row_value(row, "league_kills_std")
        league_meta_diff = _row_value(row, "league_meta_diff")
        series_game_num = _row_value(row, "series_game_num")
        is_decider_game = _row_value(row, "is_decider_game")
        combined_form_kills = _row_value(row, "combined_form_kills")
        combined_team_avg_kills = _row_value(row, "combined_team_avg_kills")
        combined_team_aggression = _row_value(row, "combined_team_aggression")
        combined_synthetic_kills = _row_value(row, "combined_synthetic_kills")
        combined_patch_form_kills = _row_value(row, "combined_patch_form_kills")
        combined_patch_team_avg_kills = _row_value(row, "combined_patch_team_avg_kills")
        combined_patch_team_aggression = _row_value(row, "combined_patch_team_aggression")
        radiant_roster_shared_prev = _row_value(row, "radiant_roster_shared_prev")
        dire_roster_shared_prev = _row_value(row, "dire_roster_shared_prev")
        radiant_roster_changed_prev = _row_value(row, "radiant_roster_changed_prev")
        dire_roster_changed_prev = _row_value(row, "dire_roster_changed_prev")
        radiant_roster_stable_prev = _row_value(row, "radiant_roster_stable_prev")
        dire_roster_stable_prev = _row_value(row, "dire_roster_stable_prev")
        radiant_roster_new_team = _row_value(row, "radiant_roster_new_team")
        dire_roster_new_team = _row_value(row, "dire_roster_new_team")
        radiant_roster_group_matches = _row_value(row, "radiant_roster_group_matches")
        dire_roster_group_matches = _row_value(row, "dire_roster_group_matches")
        radiant_roster_player_count = _row_value(row, "radiant_roster_player_count")
        dire_roster_player_count = _row_value(row, "dire_roster_player_count")

        league_id = _row_value(row, "league_id")
        series_type = row.get("series_type") if "series_type" in row.index else None
        if series_type is not None and pd.isna(series_type):
            series_type = None
        region_id = _row_value(row, "region_id")
        tournament_tier = _row_value(row, "tournament_tier")

        if series_game_num is not None:
            series_game_num = int(series_game_num)
        if is_decider_game is not None:
            is_decider_game = int(is_decider_game)
        if league_id is not None:
            league_id = int(league_id)
        if region_id is not None:
            region_id = int(region_id)
        if tournament_tier is not None:
            tournament_tier = int(tournament_tier)

        # Build features using LivePredictor
        features = predictor.build_features(
            radiant_ids=radiant_ids,
            dire_ids=dire_ids,
            radiant_account_ids=radiant_account_ids if include_dna else None,
            dire_account_ids=dire_account_ids if include_dna else None,
            radiant_team_id=radiant_team_id,
            dire_team_id=dire_team_id,
            h2h_avg_total=h2h_avg_total,
            h2h_matches_count=h2h_matches_count,
            league_avg_kills=league_avg_kills,
            league_kills_std=league_kills_std,
            league_meta_diff=league_meta_diff,
            series_game_num=series_game_num,
            is_decider_game=is_decider_game,
            combined_form_kills=combined_form_kills,
            combined_team_avg_kills=combined_team_avg_kills,
            combined_team_aggression=combined_team_aggression,
            combined_synthetic_kills=combined_synthetic_kills,
            match_start_time=match_start_time,
            league_id=league_id,
            series_type=series_type,
            region_id=region_id,
            tournament_tier=tournament_tier,
            combined_patch_form_kills=combined_patch_form_kills,
            combined_patch_team_avg_kills=combined_patch_team_avg_kills,
            combined_patch_team_aggression=combined_patch_team_aggression,
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
        
        features_list.append(features)
    
    features_df = pd.DataFrame(features_list)
    logger.info(f"Generated {len(features_df.columns)} features for {len(features_df)} matches")
    
    return features_df


def main() -> None:
    """Основной пайплайн обучения с LivePredictor features."""
    
    # 1. Load data
    df = pd.read_csv('data/pro_matches_enriched.csv')
    if 'start_time' in df.columns:
        df = df.sort_values('start_time').reset_index(drop=True)
    else:
        df = df.sort_values('match_id').reset_index(drop=True)
    logger.info(f"Loaded {len(df)} matches")
    
    # 2. Time-based split FIRST (before feature generation)
    test_size = 500
    train_idx = len(df) - test_size
    df_train = df.iloc[:train_idx].copy()
    df_test = df.iloc[train_idx:].copy()
    logger.info(f"Train: {len(df_train)}, Test: {len(df_test)}")
    
    # 3. BK Line from train set
    bk_line = df_train['total_kills'].median()
    logger.info(f"BK Line (median from train): {bk_line}")

    # 3b. Time decay weights (favor recent matches)
    train_weights = None
    if 'start_time' in df_train.columns:
        train_weights = _compute_time_decay_weights(df_train['start_time'])
        logger.info(
            "Time decay weights: min=%.3f max=%.3f",
            float(np.min(train_weights)),
            float(np.max(train_weights)),
        )
    
    # 4. Initialize LivePredictor (without loading models - we're training new ones)
    # Suppress model loading logs
    logging.getLogger('live_predictor').setLevel(logging.WARNING)
    predictor = LivePredictor()
    logging.getLogger('live_predictor').setLevel(logging.INFO)
    
    # 5. Generate features using LivePredictor.build_features() WITH DNA
    X_train_df = generate_features_from_predictor(df_train, predictor, include_dna=True)
    X_test_df = generate_features_from_predictor(df_test, predictor, include_dna=True)
    
    # 6. Get feature columns from generated data
    feature_cols = list(X_train_df.columns)
    
    # Identify categorical features (hero IDs and team IDs)
    cat_features = [c for c in feature_cols if 'hero_' in c and c.endswith(('_1', '_2', '_3', '_4', '_5'))]
    cat_features += [c for c in feature_cols if c in ['radiant_team_id', 'dire_team_id']]
    cat_features += [c for c in feature_cols if c in ['league_id', 'series_type', 'region_id', 'patch_id']]
    cat_features = [c for c in cat_features if c in feature_cols]
    
    cat_indices = [feature_cols.index(c) for c in cat_features]
    
    logger.info(f"Total features: {len(feature_cols)}")
    logger.info(f"Categorical features: {len(cat_features)}")
    logger.info(f"Numeric features: {len(feature_cols) - len(cat_features)}")
    
    # Show new Stratz features
    stratz_features = [c for c in feature_cols if any(x in c.lower() for x in 
        ['matchup', 'tankiness', 'burst_rating', 'heal_save', 'stun_control'])]
    logger.info(f"New Stratz features: {stratz_features}")
    
    # 7. Prepare data for CatBoost
    X_train = prepare_data(X_train_df, feature_cols, cat_features)
    X_test = prepare_data(X_test_df, feature_cols, cat_features)
    
    logger.info(f"Feature matrix shape: {X_train.shape}")
    
    # 8. Prepare targets
    y_train_kills = (df_train['total_kills'] > bk_line).astype(int)
    y_test_kills = (df_test['total_kills'] > bk_line).astype(int)
    
    y_train_winner = df_train['radiant_win'].astype(int)
    y_test_winner = df_test['radiant_win'].astype(int)
    
    y_train_duration = df_train['duration_min']
    y_test_duration = df_test['duration_min']
    
    y_train_kpm = df_train['total_kills'] / df_train['duration_min']
    y_test_kpm = df_test['total_kills'] / df_test['duration_min']
    
    # 7. Train models
    cb_kills = train_kills_classifier(
        X_train, y_train_kills, X_test, y_test_kills, cat_indices, sample_weight=train_weights
    )
    
    cb_winner = train_winner_classifier(
        X_train, y_train_winner, X_test, y_test_winner, cat_indices, sample_weight=train_weights
    )
    
    cb_duration = train_duration_regressor(
        X_train, y_train_duration, X_test, y_test_duration, cat_indices, sample_weight=train_weights
    )
    
    cb_kpm = train_kpm_regressor(
        X_train, y_train_kpm, X_test, y_test_kpm, cat_indices, sample_weight=train_weights
    )

    # 7a. Train kills calibrator on recent holdout
    cal_size = min(1000, max(200, len(df_train) // 5))
    if len(df_train) - cal_size >= 500:
        X_cal = X_train.iloc[-cal_size:]
        y_cal = y_train_kills.iloc[-cal_size:]
        cal_probs = cb_kills.predict_proba(X_cal)[:, 1]
        cal_df = pd.DataFrame({"kills_prob": cal_probs})
        cal_model = LogisticRegression(max_iter=1000)
        cal_weights = train_weights[-cal_size:] if train_weights is not None else None
        cal_model.fit(cal_df, y_cal, sample_weight=cal_weights)

        test_probs = cb_kills.predict_proba(X_test)[:, 1]
        cal_test = pd.DataFrame({"kills_prob": test_probs})
        cal_probs_test = cal_model.predict_proba(cal_test)[:, 1]
        cal_auc = roc_auc_score(y_test_kills, cal_probs_test)
        cal_acc = accuracy_score(y_test_kills, (cal_probs_test > 0.5).astype(int))
        logger.info(
            "Kills calibrator: AUC=%.4f, Accuracy=%.2f%% (cal_size=%d)",
            cal_auc,
            cal_acc * 100,
            cal_size,
        )

        with open(MODELS_DIR / "live_kills_calibrator.pkl", "wb") as f:
            pickle.dump(cal_model, f)
        with open(MODELS_DIR / "live_kills_calibrator.json", "w") as f:
            json.dump({"feature_cols": ["kills_prob"], "cal_size": cal_size}, f, indent=2)
        logger.info("Saved: ml-models/live_kills_calibrator.pkl / live_kills_calibrator.json")
    else:
        logger.info("Skipping calibrator (insufficient data for holdout)")

    # 7b. Train kills meta model (stacking with duration/kpm)
    meta_size = min(1000, max(200, len(df_train) // 5))
    if len(df_train) - meta_size >= 500:
        base_train_df = df_train.iloc[:-meta_size]
        meta_df = df_train.iloc[-meta_size:]
        X_base_df = X_train_df.iloc[:-meta_size]
        X_meta_df = X_train_df.iloc[-meta_size:]

        y_base_kills = y_train_kills.iloc[:-meta_size]
        y_meta_kills = y_train_kills.iloc[-meta_size:]
        y_base_duration = y_train_duration.iloc[:-meta_size]
        y_meta_duration = y_train_duration.iloc[-meta_size:]
        y_base_kpm = y_train_kpm.iloc[:-meta_size]
        y_meta_kpm = y_train_kpm.iloc[-meta_size:]

        base_weights = None
        if 'start_time' in base_train_df.columns:
            base_weights = _compute_time_decay_weights(base_train_df['start_time'])

        X_base = prepare_data(X_base_df, feature_cols, cat_features)
        X_meta = prepare_data(X_meta_df, feature_cols, cat_features)

        logger.info("Training meta base models...")
        base_kills = train_kills_classifier(
            X_base, y_base_kills, X_meta, y_meta_kills, cat_indices, sample_weight=base_weights
        )
        base_duration = train_duration_regressor(
            X_base, y_base_duration, X_meta, y_meta_duration, cat_indices, sample_weight=base_weights
        )
        base_kpm = train_kpm_regressor(
            X_base, y_base_kpm, X_meta, y_meta_kpm, cat_indices, sample_weight=base_weights
        )

        meta_kills_prob = base_kills.predict_proba(X_meta)[:, 1]
        meta_duration_pred = base_duration.predict(X_meta)
        meta_kpm_pred = base_kpm.predict(X_meta)

        meta_features_train = _build_meta_features_df(
            meta_kills_prob, meta_duration_pred, meta_kpm_pred
        )[META_FEATURE_COLS]

        meta_model = LogisticRegression(max_iter=1000)
        meta_model.fit(meta_features_train, y_meta_kills)

        test_kills_prob = base_kills.predict_proba(X_test)[:, 1]
        test_duration_pred = base_duration.predict(X_test)
        test_kpm_pred = base_kpm.predict(X_test)
        meta_features_test = _build_meta_features_df(
            test_kills_prob, test_duration_pred, test_kpm_pred
        )[META_FEATURE_COLS]
        meta_probs = meta_model.predict_proba(meta_features_test)[:, 1]
        meta_auc = roc_auc_score(y_test_kills, meta_probs)
        meta_acc = accuracy_score(y_test_kills, (meta_probs > 0.5).astype(int))
        logger.info(
            "Kills meta model: AUC=%.4f, Accuracy=%.2f%% (meta_size=%d)",
            meta_auc,
            meta_acc * 100,
            meta_size,
        )

        with open(MODELS_DIR / 'live_kills_meta.pkl', 'wb') as f:
            pickle.dump(meta_model, f)
        meta_info = {
            "feature_cols": META_FEATURE_COLS,
            "meta_size": meta_size,
            "base_train_size": len(base_train_df),
            "test_size": len(df_test),
        }
        with open(MODELS_DIR / 'live_kills_meta.json', 'w') as f:
            json.dump(meta_info, f, indent=2)
        logger.info("Saved: ml-models/live_kills_meta.pkl / live_kills_meta.json")
    else:
        logger.info("Skipping meta model (insufficient data for holdout)")
    
    # 8. Save models
    save_models(
        cb_kills, cb_winner, cb_duration, cb_kpm,
        feature_cols, cat_features, cat_indices, bk_line
    )
    
    # 9. Feature importance
    print("\n" + "=" * 60)
    print("TOP 20 FEATURES BY IMPORTANCE (CatBoost Kills)")
    print("=" * 60)
    
    importance = cb_kills.get_feature_importance()
    feat_imp = list(zip(feature_cols, importance))
    feat_imp.sort(key=lambda x: x[1], reverse=True)
    
    for name, imp in feat_imp[:20]:
        cat_marker = " [CAT]" if name in cat_features else ""
        print(f"  {name}{cat_marker}: {imp:.2f}")
    
    # 10. Backtest
    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    
    proba = cb_kills.predict_proba(X_test)[:, 1]
    y_true = df_test['total_kills'].values
    
    for threshold in [0.54, 0.56, 0.58, 0.60]:
        bets = 0
        wins = 0
        for i in range(len(y_true)):
            prob = proba[i]
            actual = y_true[i]
            
            if prob > threshold:
                bets += 1
                if actual > bk_line:
                    wins += 1
            elif prob < (1 - threshold):
                bets += 1
                if actual < bk_line:
                    wins += 1
        
        if bets > 0:
            wr = wins / bets * 100
            roi = (wins * 1.9 - bets) / bets * 100
            print(f"Threshold {threshold*100:.0f}%: {bets} bets, {wr:.1f}% WR, {roi:+.1f}% ROI")
    
    print(f"\nModels saved to: {MODELS_DIR}/")
    print("  - live_cb_kills.cbm")
    print("  - live_cb_winner.cbm")
    print("  - live_cb_duration.cbm")
    print("  - live_cb_kpm.cbm")
    print("  - live_cb_meta.json")


if __name__ == '__main__':
    main()
