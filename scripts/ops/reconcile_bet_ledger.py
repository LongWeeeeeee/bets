#!/usr/bin/env python3
"""Map-winner win-rate reconciliation for base/runtime/bet_dispatch_ledger.jsonl.

Joins the sent-bet ledger (written by `_record_bet_dispatch_ledger` at the
single delivery point `_deliver_and_persist_signal`, base/cyberscore_try.py)
against real outcomes from `runtime/live_elo_progress.json`
(`applied_maps[*].match_id`/`.radiant_win`); win-rate with a Wilson 95% CI,
grouped by path/market/tier pair/side/month. Kills markets are not scored;
ml_dispatch rows without a market require --include-legacy-ml-dispatch.
Unmatched map-winner rows count as unknown, never dropped from "sent".
Usage: reconcile_bet_ledger.py [--ledger P] [--progress P]
[--include-legacy-ml-dispatch]
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LEDGER = REPO_ROOT / "base" / "runtime" / "bet_dispatch_ledger.jsonl"
DEFAULT_PROGRESS = REPO_ROOT / "runtime" / "live_elo_progress.json"

def _load_ledger(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows

def _load_outcomes(path: Path) -> Dict[int, bool]:
    """match_id -> radiant_win from applied_maps[*]."""
    try:
        applied = json.loads(path.read_text(encoding="utf-8")).get("applied_maps")
    except Exception:
        return {}
    outcomes: Dict[int, bool] = {}
    if isinstance(applied, dict):
        for state in applied.values():
            if not isinstance(state, dict) or "radiant_win" not in state:
                continue
            try:
                mid = int(state.get("match_id") or 0)
            except (TypeError, ValueError):
                continue
            if mid > 0:
                outcomes[mid] = bool(state["radiant_win"])
    return outcomes

def _row_result(row: dict, outcomes: Dict[int, bool]) -> Optional[bool]:
    """Bet's side won -> True, lost -> False, outcome unknown -> None."""
    side = str(row.get("side") or "").strip().lower()
    if side not in ("radiant", "dire"):
        return None
    try:
        mid = int(row.get("match_id") or 0)
    except (TypeError, ValueError):
        return None
    if mid <= 0 or mid not in outcomes:
        return None
    return outcomes[mid] if side == "radiant" else not outcomes[mid]

def _wilson_ci(wins: int, n: int, z: float = 1.96) -> tuple[float, float, float]:
    if n == 0:
        return (0.0, 0.0, 0.0)
    p, denom = wins / n, 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (p, max(0.0, center - margin), min(1.0, center + margin))

def _print_group(title: str, rows: Iterable[dict], outcomes: Dict[int, bool], keyfn) -> None:
    buckets: Dict[Any, list] = defaultdict(list)
    for row in rows:
        buckets[keyfn(row)].append(_row_result(row, outcomes))
    print(f"\n=== {title} ===")
    for key in sorted(buckets, key=str):
        results = buckets[key]
        known = [r for r in results if r is not None]
        wins = sum(1 for r in known if r)
        p, lo, hi = _wilson_ci(wins, len(known))
        print(
            f"  {str(key):<24} n={len(known):>4} wins={wins:>4} "
            f"wr={p * 100:5.1f}% CI=[{lo * 100:5.1f}%,{hi * 100:5.1f}%] "
            f"unknown={len(results) - len(known)}"
        )

def _tier_pair_key(row: dict) -> str:
    return "T{}-T{}".format(*sorted((int(row.get("radiant_tier") or 3), int(row.get("dire_tier") or 3))))

def _month_key(row: dict) -> str:
    try:
        return datetime.fromtimestamp(int(row.get("ts") or 0), tz=timezone.utc).strftime("%Y-%m")
    except (TypeError, ValueError, OSError):
        return "unknown"

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--progress", type=Path, default=DEFAULT_PROGRESS)
    parser.add_argument(
        "--include-legacy-ml-dispatch", action="store_true",
        help="Score ml_dispatch rows without a market as map-winner bets (may include kills bets)",
    )
    args = parser.parse_args()
    rows = _load_ledger(args.ledger)
    outcomes = _load_outcomes(args.progress)
    print(f"Ledger rows: {len(rows)} ({args.ledger}); known outcomes: {len(outcomes)} ({args.progress})")
    map_winner_rows = []
    not_map_winner = legacy_ml_dispatch = 0
    for row in rows:
        market = row.get("market")
        if market is None and row.get("reason") == "ml_dispatch":
            legacy_ml_dispatch += 1
            if not args.include_legacy_ml_dispatch:
                continue
        elif market is not None and market != "win":
            not_map_winner += 1
            continue
        elif market is None and any(tag in str(row.get("reason") or "") for tag in ("kills", "tempo_over")):
            # Pre-ml kills / game-total paths (early_winner_kills_window_sent,
            # star_signal_sent_now_kills_window_policy, star_signal_sent_now_kills_dual,
            # tempo_over_fallback_sent in base/cyberscore_try.py) write no market;
            # 0 such prod rows on 08.10.2026.
            not_map_winner += 1
            continue
        map_winner_rows.append(row)
    print(f"not map-winner: {not_map_winner}")
    print(f"ml_dispatch rows without market (before 08.10.2026, may be kills bets): {legacy_ml_dispatch}")
    rows = map_winner_rows
    if not rows:
        return
    _print_group("By path", rows, outcomes, lambda r: str(r.get("reason") or "unknown"))
    _print_group("By market", rows, outcomes, lambda r: str(r.get("market") or "legacy"))
    _print_group("By tier pair", rows, outcomes, _tier_pair_key)
    _print_group("By side", rows, outcomes, lambda r: str(r.get("side") or "unknown"))
    _print_group("By month", rows, outcomes, _month_key)

if __name__ == "__main__":
    main()
