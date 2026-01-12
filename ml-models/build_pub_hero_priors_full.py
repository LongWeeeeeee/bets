#!/usr/bin/env python3
"""
Build public hero priors (z-scored) from full public matches dataset.

Uses public matches from:
  /Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object

Outputs:
  /Users/alex/Documents/ingame/ml-models/pub_hero_priors.json
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Dict

import numpy as np


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("pub_priors")


def _zscore_map(values: Dict[int, float]) -> Dict[int, float]:
    vals = list(values.values())
    if not vals:
        return {}
    mean = float(np.mean(vals))
    std = float(np.std(vals))
    if std <= 1e-6:
        return {k: 0.0 for k in values}
    return {k: (v - mean) / std for k, v in values.items()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pub-dir",
        type=str,
        default="/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="/Users/alex/Documents/ingame/ml-models/pub_hero_priors.json",
    )
    args = parser.parse_args()

    pub_dir = Path(args.pub_dir)
    out_path = Path(args.out)

    files = sorted(pub_dir.glob("combined*.json"))
    if not files:
        raise SystemExit(f"No pub files found in {pub_dir}")

    sums: Dict[int, Dict[str, float]] = {}
    counts: Dict[int, int] = {}
    total_matches = 0

    for path in files:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        total_matches += len(data)

        for match in data.values():
            players = match.get("players") or []
            if not players:
                continue

            dire_kills = match.get("direKills") or []
            duration_min = float(len(dire_kills)) if isinstance(dire_kills, list) else 0.0
            if duration_min <= 0:
                continue

            for p in players:
                hero_id = int(p.get("heroId") or 0)
                if hero_id <= 0:
                    continue
                s = sums.get(hero_id)
                if s is None:
                    s = {"kills": 0.0, "deaths": 0.0, "assists": 0.0, "duration": 0.0}
                    sums[hero_id] = s
                    counts[hero_id] = 0
                s["kills"] += float(p.get("kills", 0))
                s["deaths"] += float(p.get("deaths", 0))
                s["assists"] += float(p.get("assists", 0))
                s["duration"] += duration_min
                counts[hero_id] += 1

    if not sums:
        raise SystemExit("No hero stats built from public matches")

    kills_avg = {hid: sums[hid]["kills"] / counts[hid] for hid in sums}
    deaths_avg = {hid: sums[hid]["deaths"] / counts[hid] for hid in sums}
    assists_avg = {hid: sums[hid]["assists"] / counts[hid] for hid in sums}
    dur_avg = {hid: sums[hid]["duration"] / counts[hid] for hid in sums}
    kpm_avg = {
        hid: (sums[hid]["kills"] / sums[hid]["duration"]) if sums[hid]["duration"] > 0 else 0.0
        for hid in sums
    }
    dpm_avg = {
        hid: (sums[hid]["deaths"] / sums[hid]["duration"]) if sums[hid]["duration"] > 0 else 0.0
        for hid in sums
    }
    apm_avg = {
        hid: (sums[hid]["assists"] / sums[hid]["duration"]) if sums[hid]["duration"] > 0 else 0.0
        for hid in sums
    }
    kapm_avg = {
        hid: ((sums[hid]["kills"] + sums[hid]["assists"]) / sums[hid]["duration"])
        if sums[hid]["duration"] > 0
        else 0.0
        for hid in sums
    }
    kda_avg = {
        hid: ((sums[hid]["kills"] + sums[hid]["assists"]) / max(1.0, sums[hid]["deaths"]))
        for hid in sums
    }

    kills_z = _zscore_map(kills_avg)
    deaths_z = _zscore_map(deaths_avg)
    assists_z = _zscore_map(assists_avg)
    kpm_z = _zscore_map(kpm_avg)
    dpm_z = _zscore_map(dpm_avg)
    apm_z = _zscore_map(apm_avg)
    kapm_z = _zscore_map(kapm_avg)
    kda_z = _zscore_map(kda_avg)
    dur_z = _zscore_map(dur_avg)

    priors: Dict[int, Dict[str, float]] = {}
    for hid in sums:
        priors[hid] = {
            "kills_z": kills_z.get(hid, 0.0),
            "deaths_z": deaths_z.get(hid, 0.0),
            "assists_z": assists_z.get(hid, 0.0),
            "kpm_z": kpm_z.get(hid, 0.0),
            "dpm_z": dpm_z.get(hid, 0.0),
            "apm_z": apm_z.get(hid, 0.0),
            "kapm_z": kapm_z.get(hid, 0.0),
            "kda_z": kda_z.get(hid, 0.0),
            "dur_z": dur_z.get(hid, 0.0),
        }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({str(k): v for k, v in priors.items()}, f)

    logger.info("Saved priors: %s (heroes=%d, matches=%d)", out_path, len(priors), total_matches)


if __name__ == "__main__":
    main()
