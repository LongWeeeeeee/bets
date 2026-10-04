#!/usr/bin/env python3
"""Добавляет в артефакт опознание организации ПО СОСТАВУ.

Зачем именно так. Склейка team_id по составу (замер выше: 64 710 id -> 57 608
организаций, покрытие h2h 40.4% -> 42.4%) чинит переименования только для тех
id, что уже были в корпусе. У Iron Wing id новый и в корпусе его нет — карта
склейки не поможет. Поэтому в артефакт кладётся ещё и СОСТАВ каждой организации:
скорер, получив десять аккаунтов, находит организацию по пересечению >= 4 из 5
и достаёт её историю личных встреч, даже если тег видит впервые.
"""
from __future__ import annotations
import os, sys
from collections import defaultdict
from pathlib import Path
import numpy as np
ROOT = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ideas_batch2 import COMPACT
SRC = ROOT / "runtime/artifacts/misc/prematch_model_artifact_v2.npz"
OUT = ROOT / "runtime/artifacts/misc/prematch_model_artifact_v3.npz"
K = 24.0

zc = np.load(COMPACT)
ts, acc, tm, wins = zc["ts"], zc["accounts"], zc["teams"], zc["wins"].astype(int)
merged: dict[int, int] = {}
org_roster: dict[int, set] = {}
h2h = defaultdict(lambda: [0.0, 0]); rat = {}

def org_of(tid: int, members: set) -> int:
    if tid <= 0 or len(members) < 5:
        return -1
    if tid in merged:
        cur = merged[tid]; org_roster[cur] = members; return cur
    for other, ros in org_roster.items():
        if len(ros & members) >= 4:
            merged[tid] = other; org_roster[other] = members; return other
    merged[tid] = tid; org_roster[tid] = members; return tid

for i in range(len(ts)):
    r = {int(x) for x in acc[i, :5] if x > 0}
    d = {int(x) for x in acc[i, 5:] if x > 0}
    mr, md = org_of(int(tm[i, 0]), r), org_of(int(tm[i, 1]), d)
    if mr > 0 and md > 0 and mr != md:
        key = (min(mr, md), max(mr, md)); sgn = 1 if mr < md else -1
        er = 1/(1+10**((rat.get(md,1500.)-rat.get(mr,1500.))/400))
        won = bool(wins[i])
        h2h[key][0] += sgn*((1.0 if won else 0.0)-er); h2h[key][1] += 1
        rat[mr] = rat.get(mr,1500.)+K*((1.0 if won else 0.0)-er)
        rat[md] = rat.get(md,1500.)+K*((0.0 if won else 1.0)-(1-er))
    if (i+1) % 100_000 == 0: print(f"  {i+1:,}", flush=True)

z = dict(np.load(SRC))
z["team_merge"] = np.array([[k, v] for k, v in merged.items()], dtype=np.int64)
z["org_roster"] = np.array([[o] + sorted(r) for o, r in org_roster.items() if len(r) == 5],
                           dtype=np.int64)
z["h2h_org"] = np.array([[k[0], k[1], v[0]/(v[1]+3.0)] for k, v in h2h.items()], dtype=np.float64)
np.savez_compressed(OUT, **z)
print(f"\nорганизаций: {len(set(merged.values())):,} из {len(merged):,} team_id")
print(f"составов организаций: {len(z['org_roster']):,}; пар с историей: {len(z['h2h_org']):,}")
print("сохранено:", OUT)
