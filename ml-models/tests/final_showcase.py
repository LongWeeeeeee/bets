#!/usr/bin/env python3
"""
Final Model Showcase: The Trinity Tests

Tests the model's reaction to extreme conditions:
1. BLOODBATH - High kill potential teams
2. SNOOZEFEST - Low kill potential teams  
3. THE PERFECT COUNTER - Immortal vs squishy lineup
"""

import sys
from pathlib import Path
from typing import Dict, List, Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from live_predictor import LivePredictor


def format_team(predictor: LivePredictor, hero_ids: List[int]) -> str:
    """Format team as hero names."""
    names = []
    for hid in hero_ids:
        name = predictor._get_hero_feature(hid, 'name', str(hid))
        names.append(name)
    return ", ".join(names)


def get_key_features(features: Dict[str, Any]) -> Dict[str, float]:
    """Extract key features for display."""
    keys = [
        'combined_blood_synergy', 'match_blood_clash', 'total_blood_potential',
        'radiant_team_heal_save', 'dire_team_heal_save', 'team_heal_save_diff',
        'matchup_advantage_diff', 'radiant_matchup_advantage', 'dire_matchup_advantage',
        'radiant_tankiness', 'dire_tankiness', 'tankiness_diff',
        'radiant_burst_rating', 'dire_burst_rating', 'burst_rating_diff',
        'radiant_stun_control', 'dire_stun_control', 'stun_control_diff',
    ]
    return {k: features.get(k, 0) for k in keys if k in features}


def ids_to_draft(hero_ids: List[int]) -> Dict[str, Dict[str, int]]:
    """Convert hero IDs list to draft format expected by predict_match."""
    positions = ['pos1', 'pos2', 'pos3', 'pos4', 'pos5']
    return {pos: {'hero_id': hid} for pos, hid in zip(positions, hero_ids)}


def predict_simple(
    predictor: LivePredictor,
    radiant_ids: List[int],
    dire_ids: List[int],
) -> Dict[str, Any]:
    """Simple prediction using hero IDs directly."""
    radiant_draft = ids_to_draft(radiant_ids)
    dire_draft = ids_to_draft(dire_ids)
    return predictor.predict_match(radiant_draft, dire_draft)


def run_scenario(
    predictor: LivePredictor,
    name: str,
    radiant_ids: List[int],
    dire_ids: List[int],
    expected: str,
) -> None:
    """Run a single test scenario."""
    print("\n" + "=" * 70)
    print(f"🎮 {name}")
    print("=" * 70)
    
    print(f"\n🟢 Radiant: {format_team(predictor, radiant_ids)}")
    print(f"🔴 Dire: {format_team(predictor, dire_ids)}")
    
    # Get prediction
    result = predict_simple(predictor, radiant_ids, dire_ids)
    
    if 'error' in result:
        print(f"❌ Error: {result['error']}")
        return
    
    total = result['total_kills']
    winner = result['winner']
    duration = result['duration']
    booster = result.get('booster', {})
    
    print(f"\n📊 PREDICTIONS:")
    print(f"   Kills Over Prob: {total['confidence']*100:.1f}% ({total['prediction']})")
    print(f"   Predicted Kills: ~{total['predicted_kills']:.0f} (line: {total['line']})")
    print(f"   Radiant Win Prob: {winner['confidence']*100:.1f}% ({winner['prediction']})")
    print(f"   Expected Duration: {duration['predicted_minutes']:.1f} min")
    print(f"   Expected KPM: {booster.get('kpm_pred', 0):.2f}")
    
    print(f"\n🎯 Expected: {expected}")
    
    # Get features for analysis
    features = predictor.build_features(radiant_ids, dire_ids)
    key_feats = get_key_features(features)
    
    print(f"\n📈 KEY FEATURES:")
    for feat, val in sorted(key_feats.items(), key=lambda x: abs(x[1]), reverse=True)[:10]:
        print(f"   {feat}: {val:.2f}")


def main() -> None:
    """Run all showcase scenarios."""
    print("=" * 70)
    print("🏆 FINAL MODEL SHOWCASE: THE TRINITY TESTS")
    print("=" * 70)
    
    predictor = LivePredictor()
    
    # =========================================================================
    # TEST 1: BLOODBATH (Total Over)
    # =========================================================================
    run_scenario(
        predictor,
        "TEST 1: BLOODBATH 🩸",
        radiant_ids=[137, 52, 99, 14, 71],  # Primal Beast, Leshrac, BB, Pudge, SB
        dire_ids=[70, 93, 2, 59, 63],        # Ursa, Slark, Axe, Huskar, Weaver
        expected="Kills Prob > 65% (aggressive heroes = more kills)"
    )
    
    # =========================================================================
    # TEST 2: SNOOZEFEST (Total Under)
    # =========================================================================
    run_scenario(
        predictor,
        "TEST 2: SNOOZEFEST 😴",
        radiant_ids=[89, 94, 34, 90, 83],   # Naga, Medusa, Tinker, KotL, Treant
        dire_ids=[1, 67, 109, 105, 3],       # AM, Spectre, TB, Techies, Bane
        expected="Kills Prob < 45% (farming heroes = fewer kills)"
    )
    
    # =========================================================================
    # TEST 3: THE PERFECT COUNTER (Winner Prediction)
    # =========================================================================
    run_scenario(
        predictor,
        "TEST 3: THE PERFECT COUNTER ⚔️",
        radiant_ids=[59, 50, 111, 57, 102],  # Huskar, Dazzle, Oracle, Omni, Abaddon
        dire_ids=[22, 67, 44, 32, 56],       # Zeus, Spectre, PA, Riki, Clinkz
        expected="Radiant Win > 60% (immortal lineup vs squishy heroes)"
    )
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 70)
    print("📋 SUMMARY")
    print("=" * 70)
    
    # Re-run for summary
    r1 = predict_simple(predictor, [137, 52, 99, 14, 71], [70, 93, 2, 59, 63])
    r2 = predict_simple(predictor, [89, 94, 34, 90, 83], [1, 67, 109, 105, 3])
    r3 = predict_simple(predictor, [59, 50, 111, 57, 102], [22, 67, 44, 32, 56])
    
    # Extract probabilities
    r1_kills = r1['total_kills']['confidence'] if r1['total_kills']['prediction'] == 'OVER' else 1 - r1['total_kills']['confidence']
    r2_kills = r2['total_kills']['confidence'] if r2['total_kills']['prediction'] == 'OVER' else 1 - r2['total_kills']['confidence']
    r3_winner = r3['winner']['confidence'] if r3['winner']['prediction'] == 'RADIANT' else 1 - r3['winner']['confidence']
    
    print(f"\n   BLOODBATH:       Kills Over = {r1_kills*100:.1f}%  {'✅' if r1_kills > 0.65 else '❌'} (target: >65%)")
    print(f"   SNOOZEFEST:      Kills Over = {r2_kills*100:.1f}%  {'✅' if r2_kills < 0.45 else '❌'} (target: <45%)")
    print(f"   PERFECT COUNTER: Radiant Win = {r3_winner*100:.1f}%  {'✅' if r3_winner > 0.60 else '❌'} (target: >60%)")
    
    passed = sum([
        r1_kills > 0.65,
        r2_kills < 0.45,
        r3_winner > 0.60,
    ])
    
    print(f"\n   Tests Passed: {passed}/3")
    print("=" * 70)


if __name__ == "__main__":
    main()
