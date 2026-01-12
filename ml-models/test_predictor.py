#!/usr/bin/env python3
"""Quick test of draft predictor on sample data."""

import json
import sys
sys.path.insert(0, "ml-models")

from draft_predictor_final import DraftPredictor
import numpy as np

print("Loading predictor...")
predictor = DraftPredictor("ml-models/draft_v7_stats.json")

print("Loading sample matches (first 10k from 100k file)...")
# Read file in chunks to avoid memory issues
matches = []
with open("bets_data/analise_pub_matches/extracted_100k_matches.json") as f:
    content = f.read()

data = json.loads(content)
del content  # Free memory

count = 0
for match_id, m in data.items():
    if count >= 10000:
        break
    
    players = m.get("players", [])
    if len(players) != 10:
        continue
    
    r_by_pos, d_by_pos = {}, {}
    for p in players:
        hero_id = p.get("heroId")
        pos_str = p.get("position", "")
        is_rad = p.get("isRadiant", False)
        
        if not hero_id or "POSITION_" not in pos_str:
            continue
        
        pos = int(pos_str.replace("POSITION_", ""))
        if is_rad:
            r_by_pos[pos] = hero_id
        else:
            d_by_pos[pos] = hero_id
    
    if len(r_by_pos) != 5 or len(d_by_pos) != 5:
        continue
    
    matches.append({
        "radiant_win": 1 if m.get("didRadiantWin") else 0,
        "r_pos": r_by_pos,
        "d_pos": d_by_pos,
    })
    count += 1

del data  # Free memory
print(f"Loaded {len(matches)} matches")

# Calculate scores
print("\nCalculating draft scores...")
scores = []
actuals = []

for m in matches:
    r_heroes = [m["r_pos"][p] for p in range(1, 6)]
    d_heroes = [m["d_pos"][p] for p in range(1, 6)]
    
    score, _ = predictor.calculate_draft_score(
        r_heroes, d_heroes, m["r_pos"], m["d_pos"]
    )
    scores.append(score)
    actuals.append(m["radiant_win"])

scores = np.array(scores)
actuals = np.array(actuals)

print(f"\nScore distribution:")
print(f"  Mean: {np.mean(scores):.4f}")
print(f"  Std: {np.std(scores):.4f}")
print(f"  Min: {np.min(scores):.4f}")
print(f"  Max: {np.max(scores):.4f}")

print("\n=== Threshold Analysis ===")
for th in [0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20, 0.25, 0.30]:
    mask = np.abs(scores) >= th
    if mask.sum() < 50:
        print(f"  |score|>={th:.2f}: Too few samples ({mask.sum()})")
        continue
    
    preds = (scores >= 0).astype(int)
    wr = (preds[mask] == actuals[mask]).mean()
    cov = mask.mean()
    
    print(f"  |score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()} matches)")
