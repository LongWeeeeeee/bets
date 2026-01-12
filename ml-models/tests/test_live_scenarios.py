"""
Test Live Scenarios: проверка системы на экстремальных драфтах.

Сценарии:
1. BLOODBATH - максимально кровавый драфт -> ожидаем OVER
2. SNOOZEFEST - максимально пассивный драфт -> ожидаем UNDER
3. STOMP - явный фаворит -> ожидаем высокий Winner confidence
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from live_predictor import predict_live_match


def create_draft(
    radiant_heroes: List[int],
    dire_heroes: List[int],
    radiant_accounts: Optional[List[int]] = None,
    dire_accounts: Optional[List[int]] = None,
) -> Tuple[Dict, Dict]:
    """Создаёт драфт из списков hero_id и опционально account_id."""
    radiant_accounts = radiant_accounts or [0] * 5
    dire_accounts = dire_accounts or [0] * 5
    
    radiant = {
        f'pos{i+1}': {'hero_id': hero_id, 'account_id': radiant_accounts[i]}
        for i, hero_id in enumerate(radiant_heroes)
    }
    dire = {
        f'pos{i+1}': {'hero_id': hero_id, 'account_id': dire_accounts[i]}
        for i, hero_id in enumerate(dire_heroes)
    }
    return radiant, dire


def test_scenario(
    name: str,
    radiant_heroes: List[int],
    dire_heroes: List[int],
    radiant_name: str,
    dire_name: str,
    expected: str,
    radiant_team_id: Optional[int] = None,
    dire_team_id: Optional[int] = None,
    radiant_accounts: Optional[List[int]] = None,
    dire_accounts: Optional[List[int]] = None,
) -> Dict:
    """Тестирует сценарий и выводит результаты."""
    print(f"\n{'='*70}")
    print(f"🎮 SCENARIO: {name}")
    print(f"{'='*70}")
    print(f"Expected: {expected}")
    print(f"\nRadiant ({radiant_name}): {radiant_heroes}")
    print(f"Dire ({dire_name}): {dire_heroes}")
    if radiant_team_id or dire_team_id:
        print(f"Team IDs: {radiant_team_id} vs {dire_team_id}")
    if radiant_accounts or dire_accounts:
        print(f"Player IDs: {radiant_accounts} vs {dire_accounts}")
    
    radiant, dire = create_draft(radiant_heroes, dire_heroes, radiant_accounts, dire_accounts)
    
    prediction, message = predict_live_match(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        radiant_team_name=radiant_name,
        dire_team_name=dire_name,
        radiant_team_id=radiant_team_id,
        dire_team_id=dire_team_id,
    )
    
    print(f"\n{message}")
    
    # Extract key metrics
    total = prediction['total_kills']
    winner = prediction['winner']
    duration = prediction['duration']
    booster = prediction['booster']
    kpm = prediction.get('kpm', {})
    best = prediction['best_bet']
    
    print(f"\n{'─'*50}")
    print("📊 KEY METRICS:")
    print(f"  Kills Prob (Over): {total['model_prob']*100:.1f}%")
    print(f"  Kills Confidence: {total['confidence']*100:.1f}%")
    print(f"  Winner Confidence: {winner['confidence']*100:.1f}%")
    print(f"  Duration Pred: {duration['predicted_minutes']:.1f} min")
    print(f"  KPM Pred: {kpm.get('predicted', 0):.2f}")
    print(f"\n  Booster Active: {'✅ YES' if booster['should_bet'] else '❌ NO'}")
    print(f"  Booster Reason: {booster['signal_reason']}")
    print(f"  Is Blocked: {'⛔ YES' if booster['is_blocked'] else '✅ NO'}")
    print(f"\n  Best Bet: {best['market']} - {best['side']}")
    print(f"  Edge: {best['edge']*100:.1f}%")
    
    return prediction


def main() -> None:
    """Запускает все тестовые сценарии."""
    print("\n" + "="*70)
    print("🧪 LIVE PREDICTOR STRESS TEST")
    print("="*70)
    print("Testing extreme scenarios to verify model behavior...")
    
    # ============ SCENARIO 1: BLOODBATH ============
    # Максимально кровавые герои
    # Pudge, Primal Beast, Leshrac, Undying, Bristleback
    # VS Techies, Axe, Witch Doctor, Slark, Invoker
    bloodbath_radiant = [14, 137, 52, 85, 99]   # Pudge, Primal, Leshrac, Undying, Bristle
    bloodbath_dire = [105, 2, 30, 93, 74]       # Techies, Axe, WD, Slark, Invoker
    
    test_scenario(
        name="BLOODBATH (Мясо)",
        radiant_heroes=bloodbath_radiant,
        dire_heroes=bloodbath_dire,
        radiant_name="Team Fight",
        dire_name="Team Aggro",
        expected="Total Kills OVER, High KPM"
    )
    
    # ============ SCENARIO 2: SNOOZEFEST ============
    # Максимально пассивные герои (фарм, лейт)
    # Naga, Medusa, Sniper, KotL, Treant
    # VS Anti-Mage, Terrorblade, Disruptor, Bane, Enigma
    snoozefest_radiant = [89, 94, 35, 90, 83]   # Naga, Medusa, Sniper, KotL, Treant
    snoozefest_dire = [1, 109, 87, 3, 33]       # AM, TB, Disruptor, Bane, Enigma
    
    test_scenario(
        name="SNOOZEFEST (Сон)",
        radiant_heroes=snoozefest_radiant,
        dire_heroes=snoozefest_dire,
        radiant_name="Team Farm",
        dire_name="Team Late",
        expected="Total Kills UNDER, Duration > 40 min"
    )
    
    # ============ SCENARIO 3: STOMP ============
    # Сильные мета-герои против слабых
    # Radiant: Strong meta picks
    # Dire: Weak/off-meta picks
    stomp_radiant = [11, 8, 98, 86, 50]         # SF, Jugg, Underlord, Rubick, Dazzle
    stomp_dire = [66, 80, 82, 77, 91]           # Chen, Techies, Meepo, Lycan, Io
    
    test_scenario(
        name="STOMP (Разгром)",
        radiant_heroes=stomp_radiant,
        dire_heroes=stomp_dire,
        radiant_name="Team Meta",
        dire_name="Team Meme",
        expected="Winner confidence > 60%"
    )
    
    # ============ SCENARIO 4: BALANCED ============
    # Сбалансированный драфт - может сработать Booster если physics поддерживает
    balanced_radiant = [1, 13, 97, 86, 5]       # AM, Puck, Magnus, Rubick, CM
    balanced_dire = [67, 8, 98, 26, 50]         # Spectre, Jugg, Underlord, Lion, Dazzle
    
    test_scenario(
        name="BALANCED (Баланс)",
        radiant_heroes=balanced_radiant,
        dire_heroes=balanced_dire,
        radiant_name="Team A",
        dire_name="Team B",
        expected="Moderate edge, Booster may activate"
    )
    
    # ============ SCENARIO 5: RATING MISMATCH ============
    # Top team vs bottom team - should have high winner confidence
    # Team 9247354 (Rating ~1894) vs Team 9530783 (Rating ~906)
    rating_radiant = [1, 13, 97, 86, 5]       # Standard draft
    rating_dire = [67, 8, 98, 26, 50]
    
    test_scenario(
        name="RATING MISMATCH (Гранд vs Новичок)",
        radiant_heroes=rating_radiant,
        dire_heroes=rating_dire,
        radiant_name="Top Team",
        dire_name="Bottom Team",
        expected="Winner confidence > 70% (rating diff ~1000)",
        radiant_team_id=9247354,  # Top rated team
        dire_team_id=9530783,     # Bottom rated team
    )
    
    # ============ SCENARIO 6: EQUAL RATINGS ============
    # Two similar teams - should rely more on draft
    equal_radiant = [14, 137, 52, 85, 99]     # Bloodbath draft
    equal_dire = [105, 2, 30, 93, 74]
    
    test_scenario(
        name="EQUAL RATINGS (Равные команды)",
        radiant_heroes=equal_radiant,
        dire_heroes=equal_dire,
        radiant_name="Team A",
        dire_name="Team B",
        expected="Winner ~50%, focus on draft features",
        radiant_team_id=9247354,  # Similar rating
        dire_team_id=9572001,     # Similar rating
    )
    
    # ============ SCENARIO 7: GG vs FALCONS (Real Pro Teams) ============
    # GG roster: Dyrachyo, Quinn, Ace, tOfu, Seleri
    # Falcons roster: skiter, malr1ne, ATF, Cr1t, Sneyking
    gg_accounts = [302214028, 221666230, 168028741, 256156323, 113457795]  # GG
    falcons_accounts = [100058342, 321580662, 145550466, 25907144, 20585508]  # Falcons
    
    test_scenario(
        name="GG vs FALCONS (Pro Match)",
        radiant_heroes=[14, 137, 52, 85, 99],   # Bloody draft for GG
        dire_heroes=[105, 2, 30, 93, 74],       # Aggressive draft for Falcons
        radiant_name="Gaimin Gladiators",
        dire_name="Team Falcons",
        expected="OVER with Tier 1 Signal (aggressive teams + bloody draft)",
        radiant_accounts=gg_accounts,
        dire_accounts=falcons_accounts,
    )
    
    # ============ SCENARIO 8: BB vs LIQUID (Farm Style) ============
    # BetBoom roster: Nightfall, gpk, Pure, Noticed, SoNNeikO
    # Liquid roster: Micke, Nisha, Boxi, zai, Insania
    bb_accounts = [321580662, 132851371, 321580662, 178915298, 117421467]  # BB (approx)
    liquid_accounts = [113331514, 201358612, 94054712, 73562326, 54580962]  # Liquid
    
    test_scenario(
        name="BB vs LIQUID (Farm Style)",
        radiant_heroes=[89, 94, 35, 90, 83],   # Naga, Medusa, Sniper, KotL, Treant
        dire_heroes=[1, 109, 87, 3, 33],       # AM, TB, Disruptor, Bane, Enigma
        radiant_name="BetBoom Team",
        dire_name="Team Liquid",
        expected="UNDER with Tier 1 Signal (farm teams + passive draft)",
        radiant_accounts=bb_accounts,
        dire_accounts=liquid_accounts,
    )
    
    # ============ SCENARIO 9: FULL CONTEXT (Rating + Draft + Players) ============
    # Top team with aggressive players vs bottom team with passive players
    test_scenario(
        name="FULL CONTEXT (Rating + Draft + Players)",
        radiant_heroes=[14, 137, 52, 85, 99],   # Bloody draft
        dire_heroes=[89, 94, 35, 90, 83],       # Passive draft
        radiant_name="Top Aggressive",
        dire_name="Bottom Passive",
        expected="High Winner conf + blood diff signal",
        radiant_team_id=9247354,  # Top rated
        dire_team_id=9530783,     # Bottom rated
        radiant_accounts=gg_accounts,
        dire_accounts=liquid_accounts,
    )
    
    # ============ SCENARIO 10: MOUZ vs SPIRIT (Aggro vs Control) ============
    # MOUZ: Aggressive midgame team, wants to end fast
    # Spirit: Defensive late-game team, will stall and outscale
    mouz_accounts = [126238768, 152043743, 96803083, 292921272, 164532328]  # Ulnit, Supream, Force, NARMAN, Bengan
    spirit_accounts = [321580662, 398183925, 87565780, 256156323, 132851371]  # Yatoro, Larl, Collapse, Mira, Miposhka
    
    # MOUZ draft: Aggro/Push (Mars, MK, Hoodwink, Chen, TA)
    mouz_heroes = [129, 114, 123, 66, 46]
    # Spirit draft: Def/Late (Magnus, Rubick, Tidehunter, TB, Bane)
    spirit_heroes = [97, 86, 29, 109, 3]
    
    test_scenario(
        name="MOUZ vs SPIRIT (Aggro vs Control)",
        radiant_heroes=mouz_heroes,
        dire_heroes=spirit_heroes,
        radiant_name="MOUZ",
        dire_name="Team Spirit",
        expected="Spirit favored (70-80%), Long duration, Kills ???",
        radiant_accounts=mouz_accounts,
        dire_accounts=spirit_accounts,
    )
    
    # ============ SCENARIO 11: REAL MATCH - MOUZ vs SPIRIT (86 kills) ============
    # Actual match that had 86 kills - testing if model sees OVER signal
    # MOUZ (Radiant): Tiny, Dazzle, Huskar, MK, Slardar
    # Spirit (Dire): Abaddon, Snapfire, Pangolier, TB, Bane
    real_mouz_heroes = [19, 50, 59, 114, 28]  # Tiny, Dazzle, Huskar, MK, Slardar
    real_spirit_heroes = [102, 128, 120, 109, 3]  # Abaddon, Snapfire, Pango, TB, Bane
    
    test_scenario(
        name="REAL MATCH: MOUZ vs SPIRIT (86 kills)",
        radiant_heroes=real_mouz_heroes,
        dire_heroes=real_spirit_heroes,
        radiant_name="MOUZ",
        dire_name="Team Spirit",
        expected="Spirit Winner (High Conf), Kills OVER (Huskar/MK aggro)",
        radiant_accounts=mouz_accounts,
        dire_accounts=spirit_accounts,
    )
    
    # ============ SCENARIO 12: PURE AGGRESSION (Calibration Test) ============
    # Maximum aggro draft - model MUST predict OVER with high confidence
    # Liquid: Primal Beast, Leshrac, Bristleback, NP, Weaver
    # GG: Huskar, IO, Slardar, Windranger, WD
    pure_aggro_radiant = [137, 52, 99, 53, 63]  # Primal, Lesh, BB, NP, Weaver
    pure_aggro_dire = [59, 91, 28, 21, 30]  # Huskar, IO, Slardar, WR, WD
    
    test_scenario(
        name="PURE AGGRESSION (Calibration Test)",
        radiant_heroes=pure_aggro_radiant,
        dire_heroes=pure_aggro_dire,
        radiant_name="Team Liquid",
        dire_name="Gaimin Gladiators",
        expected="Kills OVER 47 (>70% conf), KPM > 1.5 - CALIBRATION CHECK",
        radiant_accounts=liquid_accounts,
        dire_accounts=gg_accounts,
    )
    
    print("\n" + "="*70)
    print("✅ STRESS TEST COMPLETE")
    print("="*70)


if __name__ == '__main__':
    main()
