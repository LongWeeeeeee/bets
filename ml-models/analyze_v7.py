#!/usr/bin/env python3
"""Analyze v7 model predictions distribution."""

import json
import numpy as np
from catboost import CatBoostClassifier

# Load model
print("Loading model...")
model = CatBoostClassifier()
model.load_model("ml-models/draft_v7.cbm")

# Load meta
with open("ml-models/draft_v7_meta.json") as f:
    meta = json.load(f)

feature_cols = meta["feature_cols"]
cat_features = meta["cat_features"]

print(f"Features: {len(feature_cols)}")
print(f"Cat features: {len(cat_features)}")

# Load stats
with open("ml-models/draft_v7_stats.json") as f:
    stats = json.load(f)

hero_wr = {int(k): v for k, v in stats["hero_wr"].items()}
hero_pos_wr = {}
for k, v in stats["hero_pos_wr"].items():
    h, p = k.rsplit("_", 1)
    hero_pos_wr[(int(h), int(p))] = v
synergy = stats["synergy"]
counter = stats["counter"]

print(f"Stats loaded: {len(hero_wr)} heroes, {len(counter)} counters")

# Generate some test predictions
import pandas as pd

def build_features(r_pos, d_pos):
    f = {}
    for pos in range(1, 6):
        f[f"r_pos{pos}"] = r_pos.get(pos, -1)
        f[f"d_pos{pos}"] = d_pos.get(pos, -1)
    
    for pos in range(1, 6):
        rh = r_pos.get(pos, -1)
        dh = d_pos.get(pos, -1)
        f[f"matchup_pos{pos}"] = rh * 1000 + dh if rh > 0 and dh > 0 else -1
    
    r_wr = sum(hero_wr.get(r_pos[p], 0.5) for p in range(1, 6))
    d_wr = sum(hero_wr.get(d_pos[p], 0.5) for p in range(1, 6))
    f["wr_diff"] = r_wr - d_wr
    
    r_pos_wr = []
    d_pos_wr = []
    for pos in range(1, 6):
        rpw = hero_pos_wr.get((r_pos[pos], pos), 0.5)
        dpw = hero_pos_wr.get((d_pos[pos], pos), 0.5)
        r_pos_wr.append(rpw)
        d_pos_wr.append(dpw)
        f[f"pos{pos}_diff"] = rpw - dpw
    
    f["pos_wr_diff"] = sum(r_pos_wr) - sum(d_pos_wr)
    f["core_wr_diff"] = sum(r_pos_wr[:3]) - sum(d_pos_wr[:3])
    f["supp_wr_diff"] = sum(r_pos_wr[3:]) - sum(d_pos_wr[3:])
    
    r_heroes = sorted([r_pos[p] for p in range(1, 6)])
    d_heroes = sorted([d_pos[p] for p in range(1, 6)])
    
    r_syn = sum(synergy.get(f"{r_heroes[i]}_{r_heroes[j]}", 0.0) for i in range(5) for j in range(i+1, 5))
    d_syn = sum(synergy.get(f"{d_heroes[i]}_{d_heroes[j]}", 0.0) for i in range(5) for j in range(i+1, 5))
    f["synergy_diff"] = r_syn - d_syn
    
    r_cnt = sum(counter.get(f"{rh}_vs_{dh}", 0.0) for rh in r_heroes for dh in d_heroes)
    d_cnt = sum(counter.get(f"{dh}_vs_{rh}", 0.0) for rh in r_heroes for dh in d_heroes)
    f["counter_diff"] = r_cnt - d_cnt
    
    f["draft_diff"] = f["wr_diff"] + f["pos_wr_diff"] + f["synergy_diff"] * 2 + f["counter_diff"] * 2
    
    return f

# Test with some random drafts
print("\nTesting predictions...")

# Example: Strong radiant draft (Meepo counter)
r_pos = {1: 82, 2: 8, 3: 2, 4: 86, 5: 26}  # Meepo pos1
d_pos = {1: 1, 2: 39, 3: 7, 4: 14, 5: 75}  # Anti-Mage pos1

f = build_features(r_pos, d_pos)
df = pd.DataFrame([f])

for col in feature_cols:
    if col in cat_features:
        df[col] = df[col].fillna(-1).astype(int).astype(str)
    else:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

proba = model.predict_proba(df[feature_cols])[:, 1]
print(f"Meepo draft: proba={proba[0]:.3f}, draft_diff={f['draft_diff']:.3f}")

# Balanced draft
r_pos = {1: 1, 2: 8, 3: 2, 4: 86, 5: 26}
d_pos = {1: 70, 2: 39, 3: 7, 4: 14, 5: 75}

f = build_features(r_pos, d_pos)
df = pd.DataFrame([f])
for col in feature_cols:
    if col in cat_features:
        df[col] = df[col].fillna(-1).astype(int).astype(str)
    else:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

proba = model.predict_proba(df[feature_cols])[:, 1]
print(f"Balanced draft: proba={proba[0]:.3f}, draft_diff={f['draft_diff']:.3f}")
