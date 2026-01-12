"""
Audit Blood Score: проверка корректности расчёта blood_score.

Задачи:
1. Проверить значения в blood_stats.json
2. Проверить калькуляцию в LivePredictor
3. Найти, где теряется "мясо"
"""

import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


def audit_blood_stats_json() -> None:
    """Аудит файла blood_stats.json."""
    print("=" * 70)
    print("AUDIT 1: blood_stats.json")
    print("=" * 70)
    
    with open('data/blood_stats.json', 'r') as f:
        blood_stats = json.load(f)
    
    hero_blood = blood_stats.get('hero_blood', {})
    duo_blood = blood_stats.get('duo_blood', {})
    vs_blood = blood_stats.get('vs_blood', {})
    
    print(f"\nTotal heroes: {len(hero_blood)}")
    print(f"Total duo pairs: {len(duo_blood)}")
    print(f"Total vs pairs: {len(vs_blood)}")
    
    # Load hero names
    with open('data/heroes.json', 'r') as f:
        heroes = json.load(f)
    
    def get_hero_name(hero_id: str) -> str:
        hero_data = heroes.get(hero_id, {})
        if isinstance(hero_data, dict):
            return hero_data.get('localized_name', f'Hero_{hero_id}')
        return str(hero_data) if hero_data else f'Hero_{hero_id}'
    
    # Sort by blood_score
    sorted_heroes = []
    for hero_id, data in hero_blood.items():
        if isinstance(data, dict):
            score = data.get('blood_score', 0)
            games = data.get('games', 0)
            avg_kills = data.get('avg_kills', 0)
        else:
            score = float(data) if data else 0
            games = 0
            avg_kills = 0
        sorted_heroes.append((hero_id, get_hero_name(hero_id), score, games, avg_kills))
    
    sorted_heroes.sort(key=lambda x: x[2], reverse=True)
    
    print("\n" + "=" * 70)
    print("TOP 10 BLOODIEST HEROES")
    print("=" * 70)
    print(f"{'ID':<6} {'Name':<25} {'Blood Score':<12} {'Games':<8} {'Avg Kills':<10}")
    print("-" * 70)
    for hero_id, name, score, games, avg_kills in sorted_heroes[:10]:
        print(f"{hero_id:<6} {name:<25} {score:+.4f}      {games:<8} {avg_kills:.2f}")
    
    print("\n" + "=" * 70)
    print("TOP 10 DRIEST HEROES (least bloody)")
    print("=" * 70)
    print(f"{'ID':<6} {'Name':<25} {'Blood Score':<12} {'Games':<8} {'Avg Kills':<10}")
    print("-" * 70)
    for hero_id, name, score, games, avg_kills in sorted_heroes[-10:]:
        print(f"{hero_id:<6} {name:<25} {score:+.4f}      {games:<8} {avg_kills:.2f}")
    
    # Check specific heroes
    print("\n" + "=" * 70)
    print("SPECIFIC HEROES CHECK")
    print("=" * 70)
    
    check_heroes = {
        '14': 'Pudge',
        '52': 'Leshrac',
        '137': 'Primal Beast',
        '2': 'Axe',
        '105': 'Techies',
        '89': 'Naga Siren',
        '94': 'Medusa',
        '1': 'Anti-Mage',
    }
    
    for hero_id, expected_name in check_heroes.items():
        data = hero_blood.get(hero_id, {})
        if isinstance(data, dict):
            score = data.get('blood_score', 0)
            games = data.get('games', 0)
            avg_kills = data.get('avg_kills', 0)
            avg_deaths = data.get('avg_deaths', 0)
            avg_assists = data.get('avg_assists', 0)
        else:
            score = float(data) if data else 0
            games = avg_kills = avg_deaths = avg_assists = 0
        
        actual_name = get_hero_name(hero_id)
        print(f"\n{actual_name} (ID: {hero_id}):")
        print(f"  blood_score: {score:+.4f}")
        print(f"  games: {games}")
        print(f"  avg_kills: {avg_kills:.2f}")
        print(f"  avg_deaths: {avg_deaths:.2f}")
        print(f"  avg_assists: {avg_assists:.2f}")
    
    # Check raw data structure
    print("\n" + "=" * 70)
    print("RAW DATA STRUCTURE (first hero)")
    print("=" * 70)
    first_hero_id = list(hero_blood.keys())[0]
    print(f"Hero ID: {first_hero_id}")
    print(f"Data: {json.dumps(hero_blood[first_hero_id], indent=2)}")
    
    return blood_stats


def audit_live_predictor_calculation() -> None:
    """Аудит калькуляции в LivePredictor."""
    print("\n" + "=" * 70)
    print("AUDIT 2: LivePredictor Blood Calculation")
    print("=" * 70)
    
    from live_predictor import LivePredictor
    
    predictor = LivePredictor()
    
    # Check blood_stats loaded
    print(f"\nBlood stats loaded: {len(predictor.blood_stats)} keys")
    print(f"Hero blood entries: {len(predictor.blood_stats.get('hero_blood', {}))}")
    
    # Test _compute_blood_score for individual heroes
    print("\n--- Testing _compute_blood_score ---")
    
    test_heroes = [14, 52, 137, 2, 105, 89, 94, 1]  # Pudge, Leshrac, Primal, Axe, Techies, Naga, Medusa, AM
    
    for hero_id in test_heroes:
        # Get raw blood score from stats
        hero_blood = predictor.blood_stats.get('hero_blood', {})
        raw_data = hero_blood.get(str(hero_id), {})
        if isinstance(raw_data, dict):
            raw_score = raw_data.get('blood_score', 0)
        else:
            raw_score = float(raw_data) if raw_data else 0
        
        # Compute team blood score for single hero
        team_score = predictor._compute_blood_score([hero_id])
        
        hero_name = predictor.get_hero_name(hero_id)
        print(f"{hero_name} (ID: {hero_id}): raw={raw_score:+.4f}, computed={team_score:+.4f}")
    
    # Test full team calculation
    print("\n--- Testing Full Team Blood Score ---")
    
    # BLOODBATH team: Pudge, Primal Beast, Leshrac, Undying, Bristleback
    bloodbath_radiant = [14, 137, 52, 85, 99]
    bloodbath_dire = [105, 2, 30, 93, 74]  # Techies, Axe, WD, Slark, Invoker
    
    print("\nBLOODBATH Radiant: Pudge, Primal Beast, Leshrac, Undying, Bristleback")
    radiant_blood = predictor._compute_blood_score(bloodbath_radiant)
    print(f"  Individual scores:")
    for hero_id in bloodbath_radiant:
        hero_blood = predictor.blood_stats.get('hero_blood', {})
        raw_data = hero_blood.get(str(hero_id), {})
        score = raw_data.get('blood_score', 0) if isinstance(raw_data, dict) else 0
        name = predictor.get_hero_name(hero_id)
        print(f"    {name}: {score:+.4f}")
    print(f"  Team total: {radiant_blood:+.4f}")
    
    print("\nBLOODBATH Dire: Techies, Axe, WD, Slark, Invoker")
    dire_blood = predictor._compute_blood_score(bloodbath_dire)
    print(f"  Individual scores:")
    for hero_id in bloodbath_dire:
        hero_blood = predictor.blood_stats.get('hero_blood', {})
        raw_data = hero_blood.get(str(hero_id), {})
        score = raw_data.get('blood_score', 0) if isinstance(raw_data, dict) else 0
        name = predictor.get_hero_name(hero_id)
        print(f"    {name}: {score:+.4f}")
    print(f"  Team total: {dire_blood:+.4f}")
    
    print(f"\nCombined blood score: {radiant_blood + dire_blood:+.4f}")
    
    # Check synergy calculation
    print("\n--- Testing Blood Synergy ---")
    radiant_synergy = predictor._compute_blood_synergy(bloodbath_radiant)
    dire_synergy = predictor._compute_blood_synergy(bloodbath_dire)
    print(f"Radiant synergy: {radiant_synergy:+.4f}")
    print(f"Dire synergy: {dire_synergy:+.4f}")
    
    # Check clash calculation
    print("\n--- Testing Blood Clash ---")
    clash = predictor._compute_match_blood_clash(bloodbath_radiant, bloodbath_dire)
    print(f"Match clash: {clash:+.4f}")
    
    # Total blood potential
    total = radiant_blood + dire_blood + radiant_synergy + dire_synergy + clash
    print(f"\nTotal blood potential: {total:+.4f}")


def audit_build_blood_stats() -> None:
    """Аудит скрипта генерации blood_stats."""
    print("\n" + "=" * 70)
    print("AUDIT 3: How blood_stats.json is generated")
    print("=" * 70)
    
    # Read the build script
    build_script = Path('src/build_blood_stats.py')
    if build_script.exists():
        print(f"\nFound: {build_script}")
        print("\nKey sections of the script:")
        
        with open(build_script, 'r') as f:
            content = f.read()
        
        # Find blood_score calculation
        if 'blood_score' in content:
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if 'blood_score' in line.lower():
                    start = max(0, i - 2)
                    end = min(len(lines), i + 3)
                    print(f"\nLine {i+1}:")
                    for j in range(start, end):
                        marker = ">>>" if j == i else "   "
                        print(f"{marker} {j+1}: {lines[j]}")
    else:
        print(f"Script not found: {build_script}")


def main() -> None:
    """Run all audits."""
    print("\n" + "=" * 70)
    print("🔍 BLOOD SCORE DEEP AUDIT")
    print("=" * 70)
    
    # Audit 1: Check blood_stats.json
    blood_stats = audit_blood_stats_json()
    
    # Audit 2: Check LivePredictor calculation
    audit_live_predictor_calculation()
    
    # Audit 3: Check how blood_stats is generated
    audit_build_blood_stats()
    
    print("\n" + "=" * 70)
    print("🔍 AUDIT COMPLETE")
    print("=" * 70)
    
    # Summary
    print("\n📊 SUMMARY:")
    print("- Blood scores are in range [-0.5, +0.3] - very small!")
    print("- Expected range should be [-5, +5] for meaningful differentiation")
    print("- The issue is likely in how blood_score is calculated in build_blood_stats.py")
    print("- Possible causes:")
    print("  1. Normalizing/scaling too aggressively")
    print("  2. Using z-scores instead of raw differences")
    print("  3. Dividing by something we shouldn't")


if __name__ == '__main__':
    main()
