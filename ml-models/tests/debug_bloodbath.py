#!/usr/bin/env python3
"""
Diagnostic: KPM vs Total Kills

Hypothesis: Model underestimates Total Kills for Bloodbath because it predicts short game.
If Calculated Kills (Duration * KPM) >> Predicted Kills, then we should use KPM * Duration formula.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from live_predictor import LivePredictor


def main() -> None:
    predictor = LivePredictor()
    
    # BLOODBATH scenario
    radiant = [137, 52, 99, 14, 71]  # Primal Beast, Leshrac, BB, Pudge, SB
    dire = [70, 93, 2, 59, 63]       # Ursa, Slark, Axe, Huskar, Weaver
    
    # Build features
    features = predictor.build_features(radiant, dire)
    
    # Get raw model predictions
    kills_prob, predicted_kills, winner_prob, duration_pred, kpm_pred = predictor.predict_with_models(features)
    
    # Calculate kills from KPM * Duration
    calculated_kills = duration_pred * kpm_pred
    
    print("=" * 60)
    print("BLOODBATH DIAGNOSTIC")
    print("=" * 60)
    print(f"\nRadiant: Primal Beast, Leshrac, Bristleback, Pudge, Spirit Breaker")
    print(f"Dire: Ursa, Slark, Axe, Huskar, Weaver")
    print()
    
    print("=" * 60)
    print("RAW MODEL OUTPUTS")
    print("=" * 60)
    print(f"  Kills Over Probability: {kills_prob*100:.1f}%")
    print(f"  Predicted Kills (classifier): ~{predicted_kills:.0f}")
    print(f"  Predicted Duration: {duration_pred:.1f} min")
    print(f"  Predicted KPM: {kpm_pred:.2f}")
    print()
    
    print("=" * 60)
    print("CALCULATED vs PREDICTED")
    print("=" * 60)
    print(f"  Calculated Kills (Duration * KPM): {calculated_kills:.1f}")
    print(f"  Predicted Kills (classifier): {predicted_kills:.0f}")
    print(f"  Difference: {calculated_kills - predicted_kills:+.1f}")
    print()
    
    bk_line = predictor.bk_line
    print(f"  BK Line: {bk_line}")
    print(f"  Calculated vs Line: {calculated_kills:.1f} vs {bk_line} = {'OVER' if calculated_kills > bk_line else 'UNDER'}")
    print(f"  Predicted vs Line: {predicted_kills:.0f} vs {bk_line} = {'OVER' if predicted_kills > bk_line else 'UNDER'}")
    print()
    
    # Analysis
    print("=" * 60)
    print("ANALYSIS")
    print("=" * 60)
    if calculated_kills > predicted_kills + 5:
        print("  ⚠️  Calculated Kills >> Predicted Kills")
        print("  → The classifier is underestimating kills")
        print("  → Consider using KPM * Duration formula instead")
        print(f"  → With KPM*Duration: {calculated_kills:.0f} kills = {(calculated_kills/bk_line - 1)*100:+.0f}% vs line")
    elif calculated_kills < predicted_kills - 5:
        print("  ⚠️  Calculated Kills << Predicted Kills")
        print("  → The classifier is overestimating kills")
    else:
        print("  ✅ Calculated and Predicted kills are consistent")
    
    # Test SNOOZEFEST too
    print("\n" + "=" * 60)
    print("SNOOZEFEST DIAGNOSTIC (for comparison)")
    print("=" * 60)
    
    radiant2 = [89, 94, 34, 90, 83]  # Naga, Medusa, Tinker, KotL, Treant
    dire2 = [1, 67, 109, 105, 3]     # AM, Spectre, TB, Techies, Bane
    
    features2 = predictor.build_features(radiant2, dire2)
    kills_prob2, predicted_kills2, _, duration_pred2, kpm_pred2 = predictor.predict_with_models(features2)
    calculated_kills2 = duration_pred2 * kpm_pred2
    
    print(f"  Kills Over Probability: {kills_prob2*100:.1f}%")
    print(f"  Predicted Kills (classifier): ~{predicted_kills2:.0f}")
    print(f"  Predicted Duration: {duration_pred2:.1f} min")
    print(f"  Predicted KPM: {kpm_pred2:.2f}")
    print(f"  Calculated Kills (Duration * KPM): {calculated_kills2:.1f}")
    print(f"  Difference: {calculated_kills2 - predicted_kills2:+.1f}")


if __name__ == "__main__":
    main()
