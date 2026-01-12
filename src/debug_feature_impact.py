#!/usr/bin/env python3
"""
Debug: Why DNA features don't improve MAE when aux predictions are present.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor, Pool
from sklearn.metrics import mean_absolute_error
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent))
from live_predictor import LivePredictor
from train_kills_regression import compute_evasiveness_catch_features

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def generate_features(
    df: pd.DataFrame,
    predictor: LivePredictor,
    include_aux: bool = True,
    include_dna: bool = True,
) -> pd.DataFrame:
    """Generate features with options."""
    features_list: List[Dict[str, Any]] = []
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Features"):
        radiant_ids = [int(row[f'radiant_hero_{i}']) for i in range(1, 6) if pd.notna(row.get(f'radiant_hero_{i}'))]
        dire_ids = [int(row[f'dire_hero_{i}']) for i in range(1, 6) if pd.notna(row.get(f'dire_hero_{i}'))]
        
        radiant_team_id = int(row['radiant_team_id']) if pd.notna(row.get('radiant_team_id')) else None
        dire_team_id = int(row['dire_team_id']) if pd.notna(row.get('dire_team_id')) else None
        
        radiant_players, dire_players = [], []
        if include_dna:
            for i in range(1, 6):
                if f'radiant_player_{i}_id' in row and pd.notna(row[f'radiant_player_{i}_id']):
                    radiant_players.append(int(row[f'radiant_player_{i}_id']))
                if f'dire_player_{i}_id' in row and pd.notna(row[f'dire_player_{i}_id']):
                    dire_players.append(int(row[f'dire_player_{i}_id']))
        
        features = predictor.build_features(
            radiant_ids=radiant_ids,
            dire_ids=dire_ids,
            radiant_account_ids=radiant_players if include_dna else None,
            dire_account_ids=dire_players if include_dna else None,
            radiant_team_id=radiant_team_id,
            dire_team_id=dire_team_id,
        )
        
        evas_catch = compute_evasiveness_catch_features(radiant_ids, dire_ids)
        features.update(evas_catch)
        
        # Add aux predictions
        if include_aux and predictor.cb_duration is not None and predictor.cb_kpm is not None:
            try:
                cb_values = []
                for col in predictor.cb_feature_cols:
                    val = features.get(col, 0)
                    if col in predictor.cb_cat_features:
                        val = str(int(val)) if val is not None else "-1"
                    else:
                        val = float(val) if val is not None else 0.0
                    cb_values.append(val)
                X_cb = pd.DataFrame([cb_values], columns=predictor.cb_feature_cols)
                
                features['predicted_duration'] = float(predictor.cb_duration.predict(X_cb)[0])
                features['predicted_kpm'] = float(predictor.cb_kpm.predict(X_cb)[0])
                features['predicted_kills_formula'] = features['predicted_duration'] * features['predicted_kpm']
            except Exception:
                features['predicted_duration'] = 37.0
                features['predicted_kpm'] = 1.2
                features['predicted_kills_formula'] = 44.4
        
        features_list.append(features)
    
    return pd.DataFrame(features_list)


def prepare_data(df: pd.DataFrame, feature_cols: List[str], cat_features: List[str]) -> pd.DataFrame:
    X = df[feature_cols].copy()
    for col in X.columns:
        if col not in cat_features:
            X[col] = X[col].fillna(X[col].median())
    for col in cat_features:
        if col in X.columns:
            X[col] = X[col].fillna(-1).astype(int).astype(str)
    return X


def train_model(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    X_test: pd.DataFrame,
    y_test: np.ndarray,
    cat_indices: List[int],
) -> tuple:
    model = CatBoostRegressor(
        iterations=1500, learning_rate=0.03, depth=6,
        loss_function='MAE', eval_metric='MAE',
        cat_features=cat_indices, random_seed=42,
        verbose=0, early_stopping_rounds=150,
    )
    
    train_pool = Pool(X_train, y_train, cat_features=cat_indices)
    test_pool = Pool(X_test, y_test, cat_features=cat_indices)
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)
    
    pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    
    importance = model.get_feature_importance()
    feat_imp = sorted(zip(X_train.columns.tolist(), importance), key=lambda x: -x[1])
    
    return mae, feat_imp, model


def main() -> None:
    df = pd.read_csv('data/pro_matches_enriched.csv')
    df = df.sort_values('match_id').reset_index(drop=True)
    
    test_size = 500
    train_idx = len(df) - test_size
    df_train = df.iloc[:train_idx].copy()
    df_test = df.iloc[train_idx:].copy()
    
    y_train = df_train['total_kills'].values
    y_test = df_test['total_kills'].values
    
    logging.getLogger('live_predictor').setLevel(logging.WARNING)
    predictor = LivePredictor()
    
    print("\n" + "=" * 70)
    print("EXPERIMENT: Aux Predictions + DNA Features")
    print("=" * 70)
    
    # 1. With Aux, WITHOUT DNA
    print("\n[1] Aux predictions, NO DNA...")
    X_train_1 = generate_features(df_train, predictor, include_aux=True, include_dna=False)
    X_test_1 = generate_features(df_test, predictor, include_aux=True, include_dna=False)
    
    feature_cols = list(X_train_1.columns)
    cat_features = [c for c in feature_cols if 'hero_' in c and c.endswith(('_1', '_2', '_3', '_4', '_5'))]
    cat_features += [c for c in ['radiant_team_id', 'dire_team_id'] if c in feature_cols]
    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]
    
    X_train_prep = prepare_data(X_train_1, feature_cols, cat_features)
    X_test_prep = prepare_data(X_test_1, feature_cols, cat_features)
    
    mae1, imp1, _ = train_model(X_train_prep, y_train, X_test_prep, y_test, cat_indices)
    
    print(f"\nMAE = {mae1:.2f}")
    print("Top 15 features:")
    for name, imp in imp1[:15]:
        print(f"  {name}: {imp:.2f}")
    
    # 2. With Aux, WITH DNA
    print("\n[2] Aux predictions + DNA...")
    X_train_2 = generate_features(df_train, predictor, include_aux=True, include_dna=True)
    X_test_2 = generate_features(df_test, predictor, include_aux=True, include_dna=True)
    
    feature_cols = list(X_train_2.columns)
    cat_features = [c for c in feature_cols if 'hero_' in c and c.endswith(('_1', '_2', '_3', '_4', '_5'))]
    cat_features += [c for c in ['radiant_team_id', 'dire_team_id'] if c in feature_cols]
    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]
    
    X_train_prep = prepare_data(X_train_2, feature_cols, cat_features)
    X_test_prep = prepare_data(X_test_2, feature_cols, cat_features)
    
    mae2, imp2, _ = train_model(X_train_prep, y_train, X_test_prep, y_test, cat_indices)
    
    print(f"\nMAE = {mae2:.2f}")
    print("Top 15 features:")
    for name, imp in imp2[:15]:
        print(f"  {name}: {imp:.2f}")
    
    # DNA features importance
    dna_features = [f for f, i in imp2 if 'dna' in f.lower()]
    print(f"\nDNA features importance:")
    for name, imp in imp2:
        if 'dna' in name.lower():
            print(f"  {name}: {imp:.2f}")
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Aux only:      MAE = {mae1:.2f} ({len(X_train_1.columns)} features)")
    print(f"Aux + DNA:     MAE = {mae2:.2f} ({len(X_train_2.columns)} features)")
    print(f"Difference:    {mae2 - mae1:+.2f}")
    
    # Check correlation between predicted_kills_formula and DNA
    print("\n" + "=" * 70)
    print("CORRELATION: predicted_kills_formula vs DNA features")
    print("=" * 70)
    
    dna_cols = [c for c in X_train_2.columns if 'dna' in c.lower() and X_train_2[c].dtype in ['float64', 'int64']]
    if 'predicted_kills_formula' in X_train_2.columns:
        for col in dna_cols[:10]:
            corr = X_train_2['predicted_kills_formula'].corr(X_train_2[col])
            print(f"  {col}: r = {corr:.3f}")


if __name__ == '__main__':
    main()
