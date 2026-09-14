#!/usr/bin/env python3
"""Offline team-NW10 early stopping study; never sends or changes runtime gates.

Uses frozen historical scores. A chronological confirmation split is secondary
retrospective evidence, not a prospective backtest of these fitted models.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "base"))
from ml_dispatch import Config, Ctx, ModelVerdict, evaluate

RICH = "runtime/artifacts/star-dispatch/lane_wait_20260914/rich_paired.npz"
LANE = "runtime/artifacts/star-dispatch/laning_pro_2026-09-12/scores.npz"
TABLE = "runtime/artifacts/star-dispatch/dispatch_rules_backtest_2026-09-12/dispatch_rules_backtest_table.npz"
ALLOW = "runtime/artifacts/star-dispatch/prod_allowlist_league_ids.json"
THRESHOLDS = (250, 500, 750, 1000, 1250, 1500, 2000, 2500, 3000)


def wilson(k, n):
    if not n:
        return [None, None]
    p, z = k / n, 1.959963984540054
    den = 1 + z * z / n
    c = (p + z * z / (2 * n)) / den
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return [c - h, c + h]


def summary(hit, selected, universe, minute=None, win=None):
    n = int(selected.sum())
    k = int(hit[selected].sum())
    out = dict(n=n, hits=k, rate=k / n if n else None, ci95=wilson(k, n),
               coverage=n / max(int(universe.sum()), 1))
    if minute is not None:
        out["mean_minute"] = float(minute[selected].mean()) if n else None
        out["saved_minutes_per_eligible_map"] = float((10 - minute[selected]).sum() / max(universe.sum(), 1))
    if win is not None:
        out["map_win_rate"] = float(win[selected].mean()) if n else None
    return out


def first_crossing(signed_nw, schedule, valid):
    """First observed minute wins; never select using later NW or final label."""
    when = np.full(len(signed_nw), 10, dtype=np.int16)
    triggered = np.zeros(len(signed_nw), dtype=bool)
    for minute, threshold in sorted(schedule.items()):
        take = valid & ~triggered & np.isfinite(signed_nw[:, minute]) & (signed_nw[:, minute] >= threshold)
        when[take] = minute
        triggered[take] = True
    return triggered, when


def unique_index(ids):
    ids = np.asarray(ids)
    if len(np.unique(ids)) != len(ids):
        raise ValueError("Duplicate match IDs must be resolved before analysis")
    return {int(mid): i for i, mid in enumerate(ids)}


def valid_timeline(nw, durations):
    early = nw[:, :11]
    return (durations >= 600) & np.all(np.isfinite(early), axis=1) & np.any(early != 0, axis=1)


def verdict(p):
    if not np.isfinite(p):
        return None
    return ModelVerdict("Radiant" if p >= .5 else "Dire", float(max(p, 1 - p)))


def load_data():
    rich = np.load(ROOT / RICH)
    lane = np.load(ROOT / LANE)
    table = np.load(ROOT / TABLE)
    ri, ti = unique_index(rich["mids"]), unique_index(table["mid"])
    unique_index(lane["mid"])
    li = np.array([i for i, mid in enumerate(lane["mid"]) if int(mid) in ri], dtype=int)
    ix = np.array([ri[int(mid)] for mid in lane["mid"][li]], dtype=int)
    d = {k: lane[k][li] for k in lane.files}
    for key in ("nw", "wins", "leagues", "sids", "durations"):
        d[key] = rich[key][ix]
    if not np.array_equal(d["nw"][:, 10], d["nw10"]):
        raise ValueError("NW10 target mismatch between score artifact and rich corpus")
    expected = np.where(d["nw10"] > 0, 2, np.where(d["nw10"] < 0, 0, 1))
    if not np.array_equal(expected, d["truth_class"]):
        raise ValueError("Score class encoding mismatch")
    d["valid"] = valid_timeline(d["nw"], d["durations"])
    cfg = Config()
    cols = {k: table[k] for k in table.files}
    targets = np.zeros(len(li), dtype=np.int8)
    for i, mid in enumerate(d["mid"]):
        j = ti.get(int(mid))
        if j is None:
            continue
        ctx = Ctx(int(mid), "offline:" + str(mid), 1, 0, "Radiant", "Dire", None,
                  1500 + float(cols["elo_diff_pts"][j]), 1500,
                  early_nw=verdict(cols["p_early_nw"][j]), early_win=verdict(cols["p_early_win"][j]),
                  late=verdict(cols["p_late"][j]), all=verdict(cols["p_all"][j]),
                  lane=ModelVerdict("Radiant", float(d["p_radiant"][i])) if d["p_radiant"][i] >= d["p_dire"][i]
                  else ModelVerdict("Dire", float(d["p_dire"][i])))
        dec = [x for x in evaluate(ctx, cfg).decisions if x.market == "win" and x.timing == "wait_600"]
        if len(dec) > 1:
            raise ValueError("More than one pending win target")
        if dec:
            targets[i] = 1 if dec[0].target_side == "Radiant" else -1
    d["dispatch_target"] = targets
    a = json.loads((ROOT / ALLOW).read_text())
    if isinstance(a, dict):
        a = a.get("league_ids", a.get("ids", []))
    d["allowlist"] = np.isin(d["leagues"], list(map(int, a)))
    # Split at UTC day; purge any series spanning the boundary from discovery.
    days = np.unique(d["ts"] // 86400)
    cutoff = int(days[int(len(days) * .70)] * 86400)
    d["confirm"] = d["ts"] >= cutoff
    later_series = np.unique(d["sids"][d["confirm"] & (d["sids"] > 0)])
    d["discover"] = (d["ts"] + d["durations"] < cutoff) & ~np.isin(d["sids"], later_series)
    return d, cutoff


def analyze(d, cutoff):
    # For unselected maps evaluate the side ahead at each minute. Dispatch
    # analyses keep the original target fixed even if the other side leads.
    neutral = (d["p_radiant"] >= .47) & (d["p_radiant"] <= .53) & (d["p_dire"] >= .47) & (d["p_dire"] <= .53)
    nohit = (d["p_radiant"] < .60) & (d["p_dire"] < .60)
    groups = {"no_lane_hit": nohit, "neutral_47_53": neutral,
              "dispatch_wait": d["dispatch_target"] != 0,
              "dispatch_wait_allowlist": (d["dispatch_target"] != 0) & d["allowlist"]}
    out = {"cutoff_ts": cutoff, "n": len(d["mid"]), "valid": int(d["valid"].sum()),
           "excluded_missing_timeline": int((~d["valid"]).sum()), "groups": {}}
    for name, pop in groups.items():
        fixed = name.startswith("dispatch")
        side = d["dispatch_target"] if fixed else np.ones(len(pop), dtype=int)
        nw = d["nw"].astype(float) * side[:, None]
        hit = d["nw10"] * side > 0
        win = np.where(side > 0, d["wins"] == 1, d["wins"] == 0)
        base = pop & d["valid"]
        report = {"eligible": int(base.sum()), "fixed_dispatch_target": fixed, "slices": {}}
        for split in ("discover", "confirm"):
            eligible = base & d[split]
            rows = []
            for minute in range(4, 11):
                for threshold in THRESHOLDS:
                    current_side = side if fixed else np.sign(d["nw"][:, minute])
                    select = eligible & (nw[:, minute] >= threshold if fixed else np.abs(nw[:, minute]) >= threshold)
                    h = d["nw10"] * current_side > 0
                    w = np.where(current_side > 0, d["wins"] == 1, d["wins"] == 0)
                    rows.append(dict(minute=minute, threshold=threshold,
                                     **summary(h, select, eligible, win=w)))
            report["slices"][split] = rows
        if fixed:
            report["policies"] = {}
            # Early release is 4..9 only. At 10:00 the existing gate releases
            # unconditionally; NW10 sign would be the target itself, not a forecast.
            # Descriptive minute-10 slices above are retained as a sanity check.
            schedules = {"constant_" + str(t): dict.fromkeys(range(4, 10), t) for t in THRESHOLDS}
            # Choose a per-minute threshold only in discovery. 80/85/90 are
            # research reliability levels, not user-authorized production gates.
            for reliability in (.80, .85, .90):
                schedule = {}
                for minute in range(4, 10):
                    candidates = [r for r in report["slices"]["discover"] if r["minute"] == minute
                                  and r["n"] >= 100 and r["ci95"][0] >= reliability]
                    if candidates:
                        schedule[minute] = min(r["threshold"] for r in candidates)
                schedules["discovery_lcb_" + str(reliability)] = schedule
            for key, schedule in schedules.items():
                triggered, when = first_crossing(nw, schedule, base)
                item = {"schedule": schedule}
                for split in ("discover", "confirm"):
                    eligible = base & d[split]
                    take = triggered & eligible
                    item[split] = summary(hit, take, eligible, when, win)
                    item[split]["by_side"] = {str(s): summary(hit, take & (side == s), eligible & (side == s)) for s in (-1, 1)}
                    # Day-block bootstrap keeps all observations of a day together.
                    unique_days = np.unique(d["ts"][take] // 86400)
                    counts = np.array([[(take & (d["ts"] // 86400 == day)).sum(),
                                        (hit & take & (d["ts"] // 86400 == day)).sum()] for day in unique_days])
                    if len(counts) > 1:
                        rng = np.random.default_rng(20260914)
                        sampled = counts[rng.integers(len(counts), size=(2000, len(counts)))].sum(axis=1)
                        item[split]["day_bootstrap_ci95"] = np.quantile(sampled[:, 1] / sampled[:, 0], [.025, .975]).tolist()
                report["policies"][key] = item
        out["groups"][name] = report
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, required=True)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    d, cutoff = load_data()
    np.savez_compressed(args.output_dir / "paired.npz", **d)
    result = analyze(d, cutoff)
    tmp = args.output_dir / "summary.json.tmp"
    tmp.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    tmp.replace(args.output_dir / "summary.json")
    print(json.dumps({k: v for k, v in result.items() if k != "groups"}))


if __name__ == "__main__":
    main()
