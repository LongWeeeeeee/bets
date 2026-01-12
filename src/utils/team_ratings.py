"""
Team Ratings: Glicko-2 рейтинговая система для команд.

Строит рейтинги на основе истории матчей из pro_matches_enriched.csv.
Сохраняет в data/team_ratings.json.
"""

import json
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


# Glicko-2 constants
TAU = 0.5  # System constant (volatility constraint)
INITIAL_RATING = 1500.0
INITIAL_RD = 350.0  # Rating deviation
INITIAL_VOL = 0.06  # Volatility


class Glicko2Rating:
    """Glicko-2 rating for a team."""
    
    def __init__(
        self,
        rating: float = INITIAL_RATING,
        rd: float = INITIAL_RD,
        vol: float = INITIAL_VOL
    ) -> None:
        self.rating = rating
        self.rd = rd
        self.vol = vol
    
    def to_dict(self) -> Dict[str, float]:
        return {
            'rating': round(self.rating, 1),
            'rd': round(self.rd, 1),
            'vol': round(self.vol, 4),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, float]) -> 'Glicko2Rating':
        return cls(
            rating=data.get('rating', INITIAL_RATING),
            rd=data.get('rd', INITIAL_RD),
            vol=data.get('vol', INITIAL_VOL),
        )


def g(rd: float) -> float:
    """Glicko-2 g function."""
    return 1.0 / math.sqrt(1.0 + 3.0 * (rd ** 2) / (math.pi ** 2))


def e(rating: float, opp_rating: float, opp_rd: float) -> float:
    """Expected score."""
    return 1.0 / (1.0 + math.exp(-g(opp_rd) * (rating - opp_rating)))


def update_glicko2(
    player: Glicko2Rating,
    opponent: Glicko2Rating,
    score: float,  # 1.0 for win, 0.0 for loss
) -> Glicko2Rating:
    """
    Update player's Glicko-2 rating after a match.
    
    Args:
        player: Player's current rating
        opponent: Opponent's current rating
        score: 1.0 for win, 0.0 for loss, 0.5 for draw
    
    Returns:
        Updated player rating
    """
    # Convert to Glicko-2 scale
    mu = (player.rating - 1500) / 173.7178
    phi = player.rd / 173.7178
    sigma = player.vol
    
    mu_j = (opponent.rating - 1500) / 173.7178
    phi_j = opponent.rd / 173.7178
    
    # Step 3: Compute variance
    g_phi_j = 1.0 / math.sqrt(1.0 + 3.0 * (phi_j ** 2) / (math.pi ** 2))
    e_val = 1.0 / (1.0 + math.exp(-g_phi_j * (mu - mu_j)))
    
    v = 1.0 / (g_phi_j ** 2 * e_val * (1 - e_val))
    
    # Step 4: Compute delta
    delta = v * g_phi_j * (score - e_val)
    
    # Step 5: Compute new volatility (simplified)
    a = math.log(sigma ** 2)
    
    def f(x: float) -> float:
        ex = math.exp(x)
        num1 = ex * (delta ** 2 - phi ** 2 - v - ex)
        denom1 = 2 * ((phi ** 2 + v + ex) ** 2)
        return num1 / denom1 - (x - a) / (TAU ** 2)
    
    # Illinois algorithm for finding new volatility
    A = a
    if delta ** 2 > phi ** 2 + v:
        B = math.log(delta ** 2 - phi ** 2 - v)
    else:
        k = 1
        while f(a - k * TAU) < 0:
            k += 1
        B = a - k * TAU
    
    f_A = f(A)
    f_B = f(B)
    
    for _ in range(50):  # Max iterations
        if abs(B - A) < 0.000001:
            break
        C = A + (A - B) * f_A / (f_B - f_A)
        f_C = f(C)
        if f_C * f_B < 0:
            A = B
            f_A = f_B
        else:
            f_A = f_A / 2
        B = C
        f_B = f_C
    
    new_sigma = math.exp(A / 2)
    
    # Step 6: Update rating deviation
    phi_star = math.sqrt(phi ** 2 + new_sigma ** 2)
    
    # Step 7: Update rating and RD
    new_phi = 1.0 / math.sqrt(1.0 / (phi_star ** 2) + 1.0 / v)
    new_mu = mu + new_phi ** 2 * g_phi_j * (score - e_val)
    
    # Convert back to Glicko scale
    new_rating = 173.7178 * new_mu + 1500
    new_rd = 173.7178 * new_phi
    
    # Clamp values
    new_rating = max(100, min(3000, new_rating))
    new_rd = max(30, min(350, new_rd))
    new_sigma = max(0.01, min(0.1, new_sigma))
    
    return Glicko2Rating(new_rating, new_rd, new_sigma)


def build_team_ratings(
    matches_path: str = 'data/pro_matches_enriched.csv',
    output_path: str = 'data/team_ratings.json',
) -> Dict[str, Dict]:
    """
    Build team ratings from match history.
    
    Iterates through matches chronologically and updates ratings.
    """
    print("Loading matches...")
    df = pd.read_csv(matches_path)
    
    # Sort by match_id (chronological)
    df = df.sort_values('match_id').reset_index(drop=True)
    
    # Check for team columns
    if 'radiant_team_id' not in df.columns or 'dire_team_id' not in df.columns:
        print("Warning: team_id columns not found, using team names")
        # Try to use team names if IDs not available
        if 'radiant_team' in df.columns:
            df['radiant_team_id'] = df['radiant_team'].fillna('unknown')
            df['dire_team_id'] = df['dire_team'].fillna('unknown')
        else:
            print("Error: No team identification columns found!")
            return {}
    
    # Initialize ratings
    ratings: Dict[str, Glicko2Rating] = {}
    team_names: Dict[str, str] = {}
    team_matches: Dict[str, int] = {}
    team_wins: Dict[str, int] = {}
    
    print(f"Processing {len(df)} matches...")
    
    for idx, row in df.iterrows():
        radiant_id = str(row.get('radiant_team_id', 'unknown'))
        dire_id = str(row.get('dire_team_id', 'unknown'))
        radiant_win = row.get('radiant_win', False)
        
        # Skip if no team info
        if radiant_id in ['unknown', 'nan', ''] or dire_id in ['unknown', 'nan', '']:
            continue
        
        # Store team names if available
        if 'radiant_team' in row and pd.notna(row['radiant_team']):
            team_names[radiant_id] = str(row['radiant_team'])
        if 'dire_team' in row and pd.notna(row['dire_team']):
            team_names[dire_id] = str(row['dire_team'])
        
        # Initialize ratings if new team
        if radiant_id not in ratings:
            ratings[radiant_id] = Glicko2Rating()
            team_matches[radiant_id] = 0
            team_wins[radiant_id] = 0
        if dire_id not in ratings:
            ratings[dire_id] = Glicko2Rating()
            team_matches[dire_id] = 0
            team_wins[dire_id] = 0
        
        # Get current ratings
        radiant_rating = ratings[radiant_id]
        dire_rating = ratings[dire_id]
        
        # Update ratings
        radiant_score = 1.0 if radiant_win else 0.0
        dire_score = 0.0 if radiant_win else 1.0
        
        new_radiant = update_glicko2(radiant_rating, dire_rating, radiant_score)
        new_dire = update_glicko2(dire_rating, radiant_rating, dire_score)
        
        ratings[radiant_id] = new_radiant
        ratings[dire_id] = new_dire
        
        # Update stats
        team_matches[radiant_id] += 1
        team_matches[dire_id] += 1
        if radiant_win:
            team_wins[radiant_id] += 1
        else:
            team_wins[dire_id] += 1
    
    # Build output
    output = {
        'teams': {},
        'meta': {
            'total_teams': len(ratings),
            'total_matches': len(df),
            'initial_rating': INITIAL_RATING,
        }
    }
    
    for team_id, rating in ratings.items():
        matches = team_matches.get(team_id, 0)
        wins = team_wins.get(team_id, 0)
        
        output['teams'][team_id] = {
            **rating.to_dict(),
            'name': team_names.get(team_id, f'Team_{team_id}'),
            'matches': matches,
            'wins': wins,
            'winrate': round(wins / max(matches, 1), 3),
        }
    
    # Save
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nSaved {len(ratings)} team ratings to {output_path}")
    
    # Print top teams
    sorted_teams = sorted(
        output['teams'].items(),
        key=lambda x: x[1]['rating'],
        reverse=True
    )
    
    print("\n=== TOP 20 TEAMS ===")
    for team_id, data in sorted_teams[:20]:
        print(f"  {data['name'][:25]:<25} Rating: {data['rating']:.0f} "
              f"(RD: {data['rd']:.0f}, {data['matches']} games, {data['winrate']*100:.0f}% WR)")
    
    print("\n=== BOTTOM 10 TEAMS (min 10 games) ===")
    filtered = [(t, d) for t, d in sorted_teams if d['matches'] >= 10]
    for team_id, data in filtered[-10:]:
        print(f"  {data['name'][:25]:<25} Rating: {data['rating']:.0f} "
              f"(RD: {data['rd']:.0f}, {data['matches']} games, {data['winrate']*100:.0f}% WR)")
    
    return output


def get_team_rating(
    team_id: str,
    ratings_data: Dict,
) -> Tuple[float, float]:
    """
    Get team rating and RD.
    
    Returns:
        (rating, rd) - defaults to (1500, 350) if team not found
    """
    teams = ratings_data.get('teams', {})
    team_data = teams.get(str(team_id), {})
    
    return (
        team_data.get('rating', INITIAL_RATING),
        team_data.get('rd', INITIAL_RD),
    )


def calculate_win_probability(
    rating1: float,
    rd1: float,
    rating2: float,
    rd2: float,
) -> float:
    """
    Calculate expected win probability for team 1 vs team 2.
    
    Uses Glicko formula for expected score.
    """
    # Combined RD
    combined_rd = math.sqrt(rd1 ** 2 + rd2 ** 2)
    
    # Expected score
    exp_score = 1.0 / (1.0 + 10 ** (-(rating1 - rating2) / (400 * math.sqrt(1 + 3 * (combined_rd / 400) ** 2 / math.pi ** 2))))
    
    return exp_score


if __name__ == '__main__':
    build_team_ratings()
