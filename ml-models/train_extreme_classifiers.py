"""
Train Extreme Classifiers: LOW (<38) and HIGH (>52) total kills.

Uses ONLY features from LivePredictor.build_features() - no CSV-only features.
This ensures the model works in production where only build_features() is available.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, roc_auc_score
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
from live_predictor import LivePredictor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MODELS_DIR = Path('ml-models')
LOW_THRESHOLD = 38
HIGH_THRESHOLD = 52


def generate_features(df: pd.DataFrame, predictor: LivePredictor) -> pd.DataFrame:
    """Generate features using ONLY LivePredictor.build_features()."""
    logger.info(f"Generating features for {len(df)} matches...")
    features_list: List[Dict[str, Any]] = []
    
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Building features"):
        radiant_ids = [int(row[f'radiant_hero_{i}']) for i in range(1, 6) if pd.notna(row.get(f'radiant_hero_{i}'))]
        dire_ids = [int(row[f'dire_hero_{i}']) for i in range(1, 6) if pd.notna(row.get(f'dire_hero_{i}'))]
        
        radiant_team_id = int(row['radiant_team_id']) if pd.notna(row.get('radiant_team_id')) else None
        dire_team_id = int(row['dire_team_id']) if pd.notna(row.get('dire_team_id')) else None
        
        radiant_account_ids = [int(row[f'radiant_player_{i}_id']) for i in range(1, 6) 
                               if f'radiant_player_{i}_id' in row.index and pd.notna(row[f'radiant_player_{i}_id'])]
        dire_account_ids = [int(row[f'dire_player_{i}_id']) for i in range(1, 6)
                           if f'dire_player_{i}_id' in row.index and pd.notna(row[f'dire_player_{i}_id'])]
        
        # Context from CSV (these ARE available in production via get_team_context)
        ctx_kwargs: Dict[str, Any] = {}
        
        # H2H context
        if pd.notna(row.get('h2h_avg_total')):
            ctx_kwargs['h2h_avg_total'] = float(row['h2h_avg_total'])
        if pd.notna(row.get('h2h_matches_count')):
            ctx_kwargs['h2h_matches_count'] = int(row['h2h_matches_count'])
        
        # League context
        if pd.notna(row.get('league_avg_kills')):
            ctx_kwargs['league_avg_kills'] = float(row['league_avg_kills'])
        if pd.notna(row.get('league_kills_std')):
            ctx_kwargs['league_kills_std'] = float(row['league_kills_std'])
        if pd.notna(row.get('league_meta_diff')):
            ctx_kwargs['league_meta_diff'] = float(row['league_meta_diff'])
        
        # Series context
        if pd.notna(row.get('series_game_num')):
            ctx_kwargs['series_game_num'] = int(row['series_game_num'])
        if pd.notna(row.get('late_series_game')):
            ctx_kwargs['is_decider_game'] = int(row['late_series_game'])
        
        # Team history context
        if pd.notna(row.get('combined_form_kills')):
            ctx_kwargs['combined_form_kills'] = float(row['combined_form_kills'])
        if pd.notna(row.get('combined_team_avg_kills')):
            ctx_kwargs['combined_team_avg_kills'] = float(row['combined_team_avg_kills'])
        if pd.notna(row.get('combined_team_aggression')):
            ctx_kwargs['combined_team_aggression'] = float(row['combined_team_aggression'])
        if pd.notna(row.get('combined_synthetic_kills')):
            ctx_kwargs['combined_synthetic_kills'] = float(row['combined_synthetic_kills'])
        if pd.notna(row.get('radiant_roster_shared_prev')):
            ctx_kwargs['radiant_roster_shared_prev'] = float(row['radiant_roster_shared_prev'])
        if pd.notna(row.get('dire_roster_shared_prev')):
            ctx_kwargs['dire_roster_shared_prev'] = float(row['dire_roster_shared_prev'])
        if pd.notna(row.get('radiant_roster_changed_prev')):
            ctx_kwargs['radiant_roster_changed_prev'] = float(row['radiant_roster_changed_prev'])
        if pd.notna(row.get('dire_roster_changed_prev')):
            ctx_kwargs['dire_roster_changed_prev'] = float(row['dire_roster_changed_prev'])
        if pd.notna(row.get('radiant_roster_stable_prev')):
            ctx_kwargs['radiant_roster_stable_prev'] = float(row['radiant_roster_stable_prev'])
        if pd.notna(row.get('dire_roster_stable_prev')):
            ctx_kwargs['dire_roster_stable_prev'] = float(row['dire_roster_stable_prev'])
        if pd.notna(row.get('radiant_roster_new_team')):
            ctx_kwargs['radiant_roster_new_team'] = float(row['radiant_roster_new_team'])
        if pd.notna(row.get('dire_roster_new_team')):
            ctx_kwargs['dire_roster_new_team'] = float(row['dire_roster_new_team'])
        if pd.notna(row.get('radiant_roster_group_matches')):
            ctx_kwargs['radiant_roster_group_matches'] = float(row['radiant_roster_group_matches'])
        if pd.notna(row.get('dire_roster_group_matches')):
            ctx_kwargs['dire_roster_group_matches'] = float(row['dire_roster_group_matches'])
        if pd.notna(row.get('radiant_roster_player_count')):
            ctx_kwargs['radiant_roster_player_count'] = float(row['radiant_roster_player_count'])
        if pd.notna(row.get('dire_roster_player_count')):
            ctx_kwargs['dire_roster_player_count'] = float(row['dire_roster_player_count'])
        
        features = predictor.build_features(
            radiant_ids=radiant_ids,
            dire_ids=dire_ids,
            radiant_account_ids=radiant_account_ids or None,
            dire_account_ids=dire_account_ids or None,
            radiant_team_id=radiant_team_id,
            dire_team_id=dire_team_id,
            **ctx_kwargs
        )
        
        features_list.append(features)
    
    return pd.DataFrame(features_list)


def prepare_data(X_df: pd.DataFrame, cat_features: List[str]) -> pd.DataFrame:
    """Prepare data for CatBoost."""
    X = X_df.copy()
    for col in X.columns:
        if col not in cat_features:
            X[col] = pd.to_numeric(X[col], errors='coerce')
            median_val = X[col].median()
            X[col] = X[col].fillna(median_val if pd.notna(median_val) else 0.0)
    for col in cat_features:
        if col in X.columns:
            X[col] = X[col].fillna(-1).astype(int).astype(str)
    return X


def train_extreme_classifier(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    cat_indices: List[int],
    name: str,
    is_low: bool = False
) -> CatBoostClassifier:
    """Train extreme classifier."""
    logger.info(f"Training {name} Classifier...")
    logger.info(f"  Positive samples: train={y_train.sum()}, test={y_test.sum()}")
    
    pos_weight = (len(y_train) - y_train.sum()) / max(y_train.sum(), 1)
    
    model = CatBoostClassifier(
        iterations=2000,
        learning_rate=0.015,
        depth=6,
        loss_function='Logloss',
        eval_metric='AUC',
        cat_features=cat_indices,
        random_seed=42,
        verbose=200,
        early_stopping_rounds=250,
        l2_leaf_reg=5,
        bagging_temperature=0.3,
        rsm=0.7,
        scale_pos_weight=pos_weight,
        min_data_in_leaf=25,
    )
    
    train_pool = Pool(X_train, y_train, cat_features=cat_indices)
    test_pool = Pool(X_test, y_test, cat_features=cat_indices)
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)
    
    proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, proba) if y_test.sum() > 0 else 0
    acc = accuracy_score(y_test, (proba > 0.5).astype(int))
    logger.info(f"{name}: AUC={auc:.4f}, Accuracy={acc*100:.2f}%")
    
    return model


def main() -> None:
    df = pd.read_csv('data/pro_matches_enriched.csv')
    df = df.sort_values('match_id').reset_index(drop=True)
    logger.info(f"Loaded {len(df)} matches")
    
    test_size = 500
    df_train = df.iloc[:-test_size].copy()
    df_test = df.iloc[-test_size:].copy()
    logger.info(f"Train: {len(df_train)}, Test: {len(df_test)}")
    
    # Generate features using ONLY build_features()
    logging.getLogger('live_predictor').setLevel(logging.WARNING)
    predictor = LivePredictor()
    
    X_train_df = generate_features(df_train, predictor)
    X_test_df = generate_features(df_test, predictor)
    
    # Ensure same columns
    feature_cols = sorted(list(X_train_df.columns))
    for col in feature_cols:
        if col not in X_test_df.columns:
            X_test_df[col] = 0.0
    X_test_df = X_test_df[feature_cols]
    X_train_df = X_train_df[feature_cols]
    
    # Categorical features
    cat_features = [c for c in feature_cols if 'hero_' in c and c.endswith(('_1', '_2', '_3', '_4', '_5'))]
    cat_features += [c for c in feature_cols if c in ['radiant_team_id', 'dire_team_id']]
    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]
    
    logger.info(f"Features: {len(feature_cols)}, Categorical: {len(cat_features)}")
    
    X_train = prepare_data(X_train_df, cat_features)
    X_test = prepare_data(X_test_df, cat_features)
    
    # Targets
    y_train_low = (df_train['total_kills'] < LOW_THRESHOLD).astype(int)
    y_test_low = (df_test['total_kills'] < LOW_THRESHOLD).astype(int)
    y_train_high = (df_train['total_kills'] > HIGH_THRESHOLD).astype(int)
    y_test_high = (df_test['total_kills'] > HIGH_THRESHOLD).astype(int)
    
    # Train
    low_model = train_extreme_classifier(
        X_train, y_train_low, X_test, y_test_low, cat_indices, "LOW (<38)", is_low=True
    )
    high_model = train_extreme_classifier(
        X_train, y_train_high, X_test, y_test_high, cat_indices, "HIGH (>52)", is_low=False
    )
    
    # Save
    MODELS_DIR.mkdir(exist_ok=True)
    low_model.save_model(str(MODELS_DIR / 'extreme_low_classifier_v2.cbm'))
    high_model.save_model(str(MODELS_DIR / 'extreme_high_classifier_v2.cbm'))
    
    meta = {
        'feature_cols': feature_cols,
        'cat_features': cat_features,
        'cat_indices': cat_indices,
        'low_threshold': LOW_THRESHOLD,
        'high_threshold': HIGH_THRESHOLD,
        'n_features': len(feature_cols),
    }
    with open(MODELS_DIR / 'extreme_classifier_meta_v2.json', 'w') as f:
        json.dump(meta, f, indent=2)
    
    logger.info(f"Saved extreme classifiers with {len(feature_cols)} features")
    
    # Backtest
    print("\n" + "="*60)
    print("BACKTEST - EXTREME PREDICTIONS")
    print("="*60)
    
    low_proba = low_model.predict_proba(X_test)[:, 1]
    high_proba = high_model.predict_proba(X_test)[:, 1]
    y_true = df_test['total_kills'].values
    
    for thresh in [0.55, 0.60, 0.65, 0.70, 0.75]:
        low_bets = low_wins = high_bets = high_wins = 0
        for i in range(len(y_true)):
            if low_proba[i] > thresh:
                low_bets += 1
                if y_true[i] < LOW_THRESHOLD:
                    low_wins += 1
            if high_proba[i] > thresh:
                high_bets += 1
                if y_true[i] > HIGH_THRESHOLD:
                    high_wins += 1
        
        low_wr = low_wins / low_bets * 100 if low_bets else 0
        high_wr = high_wins / high_bets * 100 if high_bets else 0
        print(f"Thresh {thresh*100:.0f}%: LOW {low_bets} bets {low_wr:.1f}% WR | HIGH {high_bets} bets {high_wr:.1f}% WR")


if __name__ == '__main__':
    main()
