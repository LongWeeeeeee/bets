"""
Forensic Analysis: Explain CatBoost prediction using SHAP values.

Shows which features push prediction UP (toward OVER) and DOWN (toward UNDER).
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from catboost import CatBoostClassifier, Pool
from live_predictor import LivePredictor


def get_shap_explanation(
    model: CatBoostClassifier,
    features: Dict[str, Any],
    feature_cols: List[str],
    cat_features: List[str],
) -> Tuple[float, List[Tuple[str, float]]]:
    """
    Get SHAP values for a prediction.
    
    Returns:
        Tuple of (base_value, list of (feature_name, shap_value))
    """
    # Build feature vector
    feature_values = []
    for col in feature_cols:
        val = features.get(col, 0)
        if col in cat_features:
            val = str(int(val)) if val is not None else "-1"
        else:
            val = float(val) if val is not None else 0.0
        feature_values.append(val)
    
    # Create DataFrame
    X = pd.DataFrame([feature_values], columns=feature_cols)
    
    # Create Pool with cat features
    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]
    pool = Pool(X, cat_features=cat_indices)
    
    # Get SHAP values
    shap_values = model.get_feature_importance(pool, type='ShapValues')
    
    # Last column is the base value (expected value)
    base_value = shap_values[0, -1]
    feature_shaps = shap_values[0, :-1]
    
    # Pair features with their SHAP values
    feature_shap_pairs = list(zip(feature_cols, feature_shaps))
    
    return base_value, feature_shap_pairs


def explain_match(
    predictor: LivePredictor,
    radiant_heroes: List[int],
    dire_heroes: List[int],
    radiant_team_name: str,
    dire_team_name: str,
    radiant_team_id: int = None,
    dire_team_id: int = None,
) -> None:
    """Explain prediction for a match."""
    
    # Build features
    features = predictor.build_features(
        radiant_heroes, dire_heroes,
        radiant_team_id=radiant_team_id,
        dire_team_id=dire_team_id,
    )
    
    # Get raw prediction
    kills_prob, _, _, _, _ = predictor._predict_catboost(features)
    
    print(f"\n{'='*70}")
    print(f"🔍 FORENSIC ANALYSIS: {radiant_team_name} vs {dire_team_name}")
    print(f"{'='*70}")
    print(f"\nRadiant Heroes: {radiant_heroes}")
    print(f"Dire Heroes: {dire_heroes}")
    print(f"\n📊 RAW MODEL PREDICTION: {kills_prob*100:.1f}% OVER")
    
    # Get SHAP explanation
    model = predictor.cb_kills
    base_value, feature_shaps = get_shap_explanation(
        model, features, 
        predictor.cb_feature_cols, 
        predictor.cb_cat_features
    )
    
    print(f"\n📈 BASE VALUE (avg prediction): {base_value:.4f}")
    
    # Sort by absolute SHAP value
    sorted_shaps = sorted(feature_shaps, key=lambda x: abs(x[1]), reverse=True)
    
    # Separate positive (push UP) and negative (push DOWN)
    positive_shaps = [(f, s) for f, s in sorted_shaps if s > 0.001]
    negative_shaps = [(f, s) for f, s in sorted_shaps if s < -0.001]
    
    # Sort each by magnitude
    positive_shaps.sort(key=lambda x: x[1], reverse=True)
    negative_shaps.sort(key=lambda x: x[1])
    
    print(f"\n{'─'*70}")
    print("🔺 TOP 15 FEATURES PUSHING TOWARD OVER (positive SHAP)")
    print(f"{'─'*70}")
    for i, (feat, shap) in enumerate(positive_shaps[:15], 1):
        feat_val = features.get(feat, 'N/A')
        if isinstance(feat_val, float):
            feat_val = f"{feat_val:.3f}"
        print(f"  {i:2}. {feat:40} SHAP: {shap:+.4f}  (val: {feat_val})")
    
    print(f"\n{'─'*70}")
    print("🔻 TOP 15 FEATURES PUSHING TOWARD UNDER (negative SHAP)")
    print(f"{'─'*70}")
    for i, (feat, shap) in enumerate(negative_shaps[:15], 1):
        feat_val = features.get(feat, 'N/A')
        if isinstance(feat_val, float):
            feat_val = f"{feat_val:.3f}"
        print(f"  {i:2}. {feat:40} SHAP: {shap:+.4f}  (val: {feat_val})")
    
    # Key features summary
    print(f"\n{'─'*70}")
    print("📋 KEY FEATURES VALUES")
    print(f"{'─'*70}")
    key_features = [
        'combined_blood_score', 'radiant_blood_score', 'dire_blood_score',
        'total_blood_potential', 'combined_push_score', 'combined_cc_score',
        'combined_greed', 'combined_aggression', 'radiant_team_id', 'dire_team_id',
    ]
    for feat in key_features:
        val = features.get(feat, 'N/A')
        if isinstance(val, float):
            val = f"{val:.3f}"
        # Find SHAP for this feature
        shap_val = next((s for f, s in feature_shaps if f == feat), 0)
        print(f"  {feat:35} = {val:>10}  (SHAP: {shap_val:+.4f})")
    
    # Sum of SHAP values
    total_positive = sum(s for _, s in positive_shaps)
    total_negative = sum(s for _, s in negative_shaps)
    
    print(f"\n{'─'*70}")
    print("📊 SHAP SUMMARY")
    print(f"{'─'*70}")
    print(f"  Total positive SHAP (toward OVER):  {total_positive:+.4f}")
    print(f"  Total negative SHAP (toward UNDER): {total_negative:+.4f}")
    print(f"  Net effect:                         {total_positive + total_negative:+.4f}")
    print(f"  Final logit:                        {base_value + total_positive + total_negative:.4f}")


def main() -> None:
    """Run forensic analysis on VP.P vs YeS match."""
    
    predictor = LivePredictor()
    
    # VP.P vs YeS - Tier 2 teams
    # Using typical aggressive draft for analysis
    # VP Prodigy ID: 9872558, YeS (assuming tier 2)
    
    # Example draft - adjust based on actual match
    # Aggressive draft to test why model gives low confidence
    radiant_heroes = [137, 52, 99, 114, 28]  # Primal, Lesh, BB, MK, Slardar
    dire_heroes = [59, 91, 21, 30, 86]       # Huskar, IO, WR, WD, Rubick
    
    explain_match(
        predictor,
        radiant_heroes=radiant_heroes,
        dire_heroes=dire_heroes,
        radiant_team_name="VP.Prodigy",
        dire_team_name="YeS",
        radiant_team_id=9872558,  # VP Prodigy
        dire_team_id=None,        # Unknown team
    )
    
    print("\n" + "="*70)
    print("🔬 ADDITIONAL TEST: Pure Aggression Draft")
    print("="*70)
    
    # Pure aggression for comparison
    explain_match(
        predictor,
        radiant_heroes=[137, 52, 99, 53, 63],  # Primal, Lesh, BB, NP, Weaver
        dire_heroes=[59, 91, 28, 21, 30],      # Huskar, IO, Slardar, WR, WD
        radiant_team_name="Team Liquid",
        dire_team_name="GG",
        radiant_team_id=2163,     # Liquid
        dire_team_id=8599101,     # GG
    )


if __name__ == '__main__':
    main()
