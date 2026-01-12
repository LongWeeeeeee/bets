#!/usr/bin/env python3
"""
Unified backtest harness to compare signal sources (primary, transitive, combined).

Usage:
    python3 backtest_unified_sources.py 400 30 --min-strength 0.0

Outputs accuracy/coverage for each scenario so we can see
which blend maximizes winrate vs coverage on identical match sets.
"""

from __future__ import annotations

import argparse
import contextlib
import io
from collections import defaultdict
from typing import Dict, List, Any

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from core.transitive_analyzer import TransitiveAnalyzer, get_transitiv


SCENARIOS: List[Dict[str, Any]] = [
    {
        "name": "hybrid_best",
        "label": "Hybrid (Transitive > Elo)",
        "params": {
            "weights": {
                "head_to_head": 0.0,
                "common_opponents": 0.0,
                "transitive": 5.0,  # High weight to override Elo
                "elo": 1.0,         # Fallback
            },
            "use_transitive": True,
        },
    },
    {
        "name": "elo_only",
        "label": "Pure Elo (V2)",
        "params": {
            "weights": {
                "head_to_head": 0.0,
                "common_opponents": 0.0,
                "transitive": 0.0,
                "elo": 1.0,
            },
            "use_transitive": False,
        },
    },
    {
        "name": "primary_only",
        "label": "Primary (H2H+Common+Elo)",
        "params": {
            "use_transitive": False,
        },
    },
    {
        "name": "elo_plus_transitive",
        "label": "Elo + Transitive (No H2H/Common)",
        "params": {
            "weights": {
                "head_to_head": 0.0,
                "common_opponents": 0.0,
                "transitive": 1.5,
                "elo": 1.0,
            },
            "use_transitive": True,
        },
    },
    {
        "name": "combined_default",
        "label": "Combined (default weights)",
        "params": {},
    },
    {
        "name": "combined_trans_boost",
        "label": "Combined (trans weight x2)",
        "params": {
            "weights": {
                "head_to_head": 2.0,
                "common_opponents": 2.5,
                "transitive": 0.5,
                "elo": 1.0,
            },
        },
    },
    {
        "name": "trans_only_k3",
        "label": "Transitive only (k>=3)",
        "params": {
            "weights": {
                "head_to_head": 0.0,
                "common_opponents": 0.0,
                "transitive": 1.0,
                "elo": 0.0,
            },
            "chain_weights": {
                "h2h_chain": 0.0,
                "common_chain": 0.0,
                "trans_chain": 1.0,
            },
            "use_transitive": True,
            "use_elo_filter": False,
            "min_transitive_chains": 3,
        },
        "require_transitive_chains": 3,
        "require_decision_mode": "transitive",
    },
    {
        "name": "trans_only_k4",
        "label": "Transitive only (k>=4)",
        "params": {
            "weights": {
                "head_to_head": 0.0,
                "common_opponents": 0.0,
                "transitive": 1.0,
                "elo": 0.0,
            },
            "chain_weights": {
                "h2h_chain": 0.0,
                "common_chain": 0.0,
                "trans_chain": 1.0,
            },
            "use_transitive": True,
            "use_elo_filter": False,
            "min_transitive_chains": 4,
        },
        "require_transitive_chains": 4,
        "require_decision_mode": "transitive",
    },
]


def load_matches(analyzer: TransitiveAnalyzer) -> List[Dict]:
    matches = [m for m in analyzer.matches_data.values() if m.get("startDateTime", 0) > 0]
    matches.sort(key=lambda x: x.get("startDateTime", 0), reverse=True)
    return matches


def evaluate_scenarios(n_matches: int, max_days: int, min_strength: float, scenario_names: List[str] | None = None) -> None:
    analyzer = TransitiveAnalyzer()
    matches = load_matches(analyzer)[:n_matches]

    active_scenarios = [s for s in SCENARIOS if not scenario_names or s["name"] in scenario_names]
    scenario_stats = {}
    for scenario in active_scenarios:
        scenario_stats[scenario["name"]] = {
            "label": scenario["label"],
            "used": 0,
            "hits": 0,
            "skipped_no_data": 0,
            "skipped_strength": 0,
            "skipped_filters": 0,
            "ties": 0,
            "total_strength": 0.0,
            "decision_counts": defaultdict(int),
        }

    for match in matches:
        radiant = match.get("radiantTeam") or {}
        dire = match.get("direTeam") or {}
        radiant_id = radiant.get("id")
        dire_id = dire.get("id")
        as_of_ts = match.get("startDateTime", 0)
        actual_radiant_win = match.get("didRadiantWin")

        if not radiant_id or not dire_id or actual_radiant_win is None or as_of_ts <= 0:
            continue

        for scenario in active_scenarios:
            stats = scenario_stats[scenario["name"]]
            params = dict(scenario.get("params", {}))
            params.setdefault("use_transitive", True)

            try:
                # Suppress verbose logs coming from get_transitiv
                with contextlib.redirect_stdout(io.StringIO()):
                    result = get_transitiv(
                        radiant_team_id=radiant_id,
                        dire_team_id=dire_id,
                        radiant_team_name_original=radiant.get("name"),
                        dire_team_name_original=dire.get("name"),
                        analyzer=analyzer,
                        as_of_timestamp=as_of_ts,
                        max_days=max_days,
                        min_transitive_chains=params.pop("min_transitive_chains", None),
                        **params,
                    )
            except Exception as exc:
                print(f"[WARN] Scenario {scenario['name']} threw exception on match {match.get('id')}: {exc}")
                continue

            if not result.get("has_data"):
                stats["skipped_no_data"] += 1
                continue

            if result.get("winner") == "Ничья":
                stats["ties"] += 1
                continue

            strength = result.get("strength", 0.0)
            if strength < max(min_strength, scenario.get("min_strength", 0.0)):
                stats["skipped_strength"] += 1
                continue

            if scenario.get("require_transitive_chains"):
                if result.get("transitive_series", 0) < scenario["require_transitive_chains"]:
                    stats["skipped_filters"] += 1
                    continue

            if scenario.get("require_decision_mode"):
                if result.get("decision_mode") != scenario["require_decision_mode"]:
                    stats["skipped_filters"] += 1
                    continue

            stats["used"] += 1
            stats["total_strength"] += strength
            decision_mode = result.get("decision_mode", "unknown")
            stats["decision_counts"][decision_mode] += 1

            predicted_raw = result.get("prediction") or result.get("winner")
            radiant_name = (match.get("radiantTeam") or {}).get("name")
            dire_name = (match.get("direTeam") or {}).get("name")
            if predicted_raw == "Radiant" or predicted_raw == radiant_name:
                predicted_winner = "Radiant"
            elif predicted_raw == "Dire" or predicted_raw == dire_name:
                predicted_winner = "Dire"
            else:
                # If we cannot map name back to side, skip accuracy but keep coverage.
                predicted_winner = None

            if predicted_winner is None:
                stats["skipped_filters"] += 1
                continue

            correct = (predicted_winner == "Radiant" and actual_radiant_win) or (
                predicted_winner == "Dire" and not actual_radiant_win
            )
            if correct:
                stats["hits"] += 1

    print("=" * 80)
    print("UNIFIED BACKTEST RESULTS")
    print("=" * 80)
    print(f"Matches evaluated: {len(matches)} (max_days={max_days}, min_strength={min_strength})\n")

    header = f"{'Scenario':<28} {'Used':<6} {'Coverage':<10} {'Accuracy':<10} {'AvgStrength':<12} {'Skipped':<8}"
    print(header)
    print("-" * len(header))

    for scenario in active_scenarios:
        stats = scenario_stats[scenario["name"]]
        used = stats["used"]
        coverage = used / float(len(matches)) if matches else 0.0
        accuracy = (stats["hits"] / used) if used else 0.0
        avg_strength = (stats["total_strength"] / used) if used else 0.0
        skipped = stats["skipped_no_data"] + stats["skipped_strength"] + stats["skipped_filters"]
        print(
            f"{stats['label']:<28} "
            f"{used:<6d} "
            f"{coverage*100:>8.1f}% "
            f"{accuracy*100:>8.1f}% "
            f"{avg_strength:>10.2f} "
            f"{skipped:<8d}"
        )

    print("\nNotes:")
    print(" - Coverage = used / total matches fed to the harness.")
    print(" - Accuracy computed only on used matches.")
    print(" - Skipped counts include no-data, low-strength, and filter-based skips.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified backtest for deterministic signals.")
    parser.add_argument("n_matches", type=int, help="Number of most recent matches to evaluate.")
    parser.add_argument("max_days", type=int, help="Window size (days) for data gathering.")
    parser.add_argument("--min-strength", type=float, default=0.0, help="Global strength cutoff.")
    parser.add_argument(
        "--scenarios",
        type=str,
        default="",
        help="Comma-separated scenario names to run (default: all). Names: "
             + ", ".join(s["name"] for s in SCENARIOS),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    scenario_names = [x.strip() for x in args.scenarios.split(",") if x.strip()]
    evaluate_scenarios(
        n_matches=args.n_matches,
        max_days=args.max_days,
        min_strength=args.min_strength,
        scenario_names=scenario_names or None,
    )
