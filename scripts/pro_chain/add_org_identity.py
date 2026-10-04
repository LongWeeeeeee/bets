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
import os
from collections import defaultdict
from pathlib import Path
import numpy as np
ROOT = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
COMPACT = ROOT / "runtime/artifacts/misc/pro_corpus_compact.npz"
SRC = ROOT / "runtime/artifacts/misc/prematch_model_artifact_v2.npz"
OUT = ROOT / "runtime/artifacts/misc/prematch_model_artifact_v3.npz"
K = 24.0

zc = np.load(COMPACT)
ts, acc, tm, wins = zc["ts"], zc["accounts"], zc["teams"], zc["wins"].astype(int)
merged: dict[int, int] = {}
org_roster: dict[int, set] = {}
org_order: dict[int, int] = {}
account_orgs: dict[int, set] = defaultdict(set)
h2h = defaultdict(lambda: [0.0, 0]); rat = {}

def replace_roster(org: int, members: set) -> None:
    previous = org_roster.get(org, set())
    # Only changed memberships need work, even on repeated known-team calls.
    for account in previous - members:
        account_orgs[account].remove(org)
        if not account_orgs[account]:
            del account_orgs[account]
    for account in members - previous:
        account_orgs[account].add(org)
    org_roster[org] = members

def org_of(tid: int, members: set) -> int:
    if tid <= 0 or len(members) < 5:
        return -1
    if tid in merged:
        cur = merged[tid]; replace_roster(cur, members); return cur
    overlaps = {}
    for account in members:
        for org in account_orgs.get(account, ()):
            overlaps[org] = overlaps.get(org, 0) + 1
    # Posting-set order is arbitrary; the legacy scan chose the first dict key.
    candidates = [org for org, count in overlaps.items() if count >= 4]
    if candidates:
        cur = min(candidates, key=org_order.__getitem__)
    else:
        cur = tid
        org_order[cur] = len(org_order)
    merged[tid] = cur; replace_roster(cur, members); return cur

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
