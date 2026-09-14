#!/usr/bin/env python3
"""Retrospective minute schedules fitted only on still-unsent discovery maps."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from research_lane_wait import summary, unique_index, wilson
from research_lane_residual_check import day_interval


def eligible_at(nw, minute, threshold, growth):
    eligible = np.isfinite(nw[:, minute]) & (nw[:, minute] >= threshold)
    if growth is not None:
        if minute <= 1:
            raise ValueError("Growth since minute1 requires a later observation")
        eligible &= np.isfinite(nw[:, 1]) & (nw[:, minute] - nw[:, 1] >= growth)
    return eligible


def fit_schedule(nw, hit, train, start, reliability, growth=None,
                 thresholds=range(100, 3001, 100), minimum=100):
    remaining = train.copy()
    schedule, cohorts = {}, {}
    for minute in range(start, 10):
        for threshold in thresholds:
            take = remaining & eligible_at(nw, minute, threshold, growth)
            n, k = int(take.sum()), int(hit[take].sum())
            if n >= minimum and wilson(k, n)[0] >= reliability:
                schedule[minute] = threshold
                cohorts[minute] = dict(n=n, hits=k, rate=k / n, ci95=wilson(k, n))
                remaining[take] = False
                break
    return schedule, cohorts


def replay(nw, population, schedule, growth=None):
    take = np.zeros(len(nw), dtype=bool)
    when = np.full(len(nw), 10, dtype=np.int16)
    for minute, threshold in sorted(schedule.items()):
        selected = population & ~take & eligible_at(nw, minute, threshold, growth)
        when[selected] = minute
        take |= selected
    return take, when


def target_hit(d, target_min_lead=None):
    signed = d["nw10"] * d["dispatch_target"]
    if target_min_lead is not None and (not np.isfinite(target_min_lead) or target_min_lead <= 0):
        raise ValueError("Target minimum must be finite and positive")
    return signed > 0 if target_min_lead is None else signed >= target_min_lead


def minute_slices(d, nw, population, hit):
    """Independent descriptive slices; earlier crossings are NOT removed."""
    rows = []
    for minute in range(1, 11):
        for threshold in (500, 1000, 1500, 2000, 2500, 3000):
            row = {"minute": minute, "entry_min_lead": threshold,
                   "is_forecast": minute < 10}
            for split in ("discover", "confirm"):
                universe = population & d[split]
                selected = universe & eligible_at(nw, minute, threshold, None)
                row[split] = summary(hit, selected, universe)
            rows.append(row)
    return rows


def evaluate(d, nw, population, schedule, growth, target_min_lead=None):
    take, when = replay(nw, population, schedule, growth)
    hit = target_hit(d, target_min_lead)
    win = np.where(d["dispatch_target"] > 0, d["wins"] == 1, d["wins"] == 0)
    out = {}
    for split in ("discover", "confirm"):
        universe = population & d[split]
        selected = take & universe
        r = summary(hit, selected, universe, when, win)
        r["day_ci95"] = day_interval(hit[selected], d["ts"][selected] // 86400)
        r["eligible"] = int(universe.sum())
        r["first_minute"] = {
            str(m): summary(hit, selected & (when == m), universe)
            for m in schedule}
        r["by_side"] = {str(side): summary(hit, selected & (d["dispatch_target"] == side),
                                           universe & (d["dispatch_target"] == side))
                        for side in (-1, 1)}
        out[split] = r
    return out


def analyze(d, target_min_lead=None):
    unique_index(d["mid"])
    assert not np.any(d["discover"] & d["confirm"])
    base = (d["dispatch_target"] != 0) & d["valid"]
    nw = d["nw"].astype(float) * d["dispatch_target"][:, None]
    hit = target_hit(d, target_min_lead)
    out = {"policies": {}, "minute1_spike": {},
           "target": {"operator": ">" if target_min_lead is None else ">=",
                      "signed_team_nw_at10": 0 if target_min_lead is None else target_min_lead},
           "method": "Minimum100 per conditional unsent discovery cohort; ascending100..3000 gold grid; LCB90/95; first crossing only; unconditional release10",
           "limitation": "Retrospective reused confirmation, not fresh OOS. NW growth is not creep-farm attribution."}
    for start in (1, 2, 3, 4):
        for growth in ([None] if start < 3 else [None, 0, 250]):
            for reliability in (.90, .95):
                schedule, cohorts = fit_schedule(nw, hit, base & d["discover"], start, reliability, growth)
                name = f"start{start}_lcb{reliability}_growth{growth}"
                out["policies"][name] = {
                    "start": start, "growth_since_minute1": growth, "requested_lcb": reliability,
                    "schedule": schedule, "conditional_discovery_cohorts": cohorts,
                    "all_wait": evaluate(d, nw, base, schedule, growth, target_min_lead),
                    # Same schedule, never fit a second one using the smaller allowlist.
                    "allowlist_wait": evaluate(d, nw, base & d["allowlist"], schedule, growth, target_min_lead)}
        for threshold in ([1000] if start != 4 else [1000, 1500, 2000, 2500, 3000]):
            schedule = dict.fromkeys(range(start, 10), threshold)
            out["policies"][f"constant{threshold}_start{start}"] = {
                "start": start, "growth_since_minute1": None, "schedule": schedule,
                "all_wait": evaluate(d, nw, base, schedule, None, target_min_lead),
                "allowlist_wait": evaluate(d, nw, base & d["allowlist"], schedule, None, target_min_lead)}
    # Fixed policy from the prior >0 study; rescore without refitting it.
    old = {3: 1200, 4: 1000, 5: 1000, 6: 900, 7: 1100, 8: 700, 9: 700}
    out["previous_variable_schedule"] = evaluate(d, nw, base, old, None, target_min_lead)
    out["minute_slices"] = {
        "method": "P(target | signed NW at minute >= threshold); independent slices, not a stopping policy; minute10 is contemporaneous",
        "all_wait": minute_slices(d, nw, base, hit),
        "allowlist_wait": minute_slices(d, nw, base & d["allowlist"], hit)}
    # Match on current NW bands to avoid comparing big current leads to small
    # ones when describing whether an early lead has been maintained.
    for minute in (3, 4):
        rows = []
        for lo, hi in ((300, 800), (800, 1300), (1300, 2000), (2000, 100000)):
            universe = base & d["confirm"] & (nw[:, minute] >= lo) & (nw[:, minute] < hi)
            for label, condition in {
                "minute1_lead_at_least500": nw[:, 1] >= 500,
                "minute1_lead_below500": nw[:, 1] < 500,
                "grew_since1": nw[:, minute] - nw[:, 1] >= 250,
                "did_not_grow250": nw[:, minute] - nw[:, 1] < 250,
            }.items():
                rows.append(dict(current_nw_band=[lo, hi], condition=label,
                                 **summary(hit, universe & condition, universe)))
        out["minute1_spike"][str(minute)] = rows
    return out


def clock_view(data, offset):
    """Keep legacy raw-index mapping primary; allow explicit N-1 sensitivity."""
    if offset not in (0, -1):
        raise ValueError("Only observed local clock conventions are supported")
    d = dict(data)
    nw = np.full((len(d["nw"]), 11), np.nan)
    for minute in range(1, 11):
        nw[:, minute] = d["nw"][:, minute + offset]
    d["nw"] = nw
    d["nw10"] = nw[:, 10]
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--paired", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--source-index-offset", type=int, choices=(0, -1), default=0)
    ap.add_argument("--target-min-lead", type=float,
                    help="Inclusive positive signed NW@10 target; omitted keeps legacy >0")
    args = ap.parse_args()
    result = analyze(clock_view(np.load(args.paired), args.source_index_offset), args.target_min_lead)
    result["source_index_offset"] = args.source_index_offset
    args.output_dir.mkdir(parents=True, exist_ok=True)
    tmp = args.output_dir / "minute_schedule.json.tmp"
    tmp.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    tmp.replace(args.output_dir / "minute_schedule.json")


if __name__ == "__main__":
    main()
