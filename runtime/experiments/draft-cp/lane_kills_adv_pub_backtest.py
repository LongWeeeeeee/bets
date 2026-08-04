#!/usr/bin/env python3
"""Backtest lane_kills_adv_dict on newest public matches (memory-safe).

Predict which team gets more kills in minutes 0-10 via calculate_lane_kills_advantage.
Actual: sum(radiantKills[:10]) - sum(direKills[:10]).
Tie (actual_diff == 0) counts as LOSE. Zero expected_diff also LOSE.

Reports overall WR and WR by abs(expected_diff) buckets.
"""
from __future__ import annotations

import argparse
import gc
import heapq
import json
import math
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

ROOT = Path("/root/main")
BASE = ROOT / "base"
sys.path = [str(BASE)] + [p for p in sys.path if Path(p).resolve() != ROOT]

import orjson  # noqa: E402
from analise_database import _kills10_diff, extract_heroes_by_position  # noqa: E402
from functions import calculate_lane_kills_advantage  # noqa: E402

PUB_DIR = ROOT / "bets_data" / "analise_pub_matches" / "json_parts_split_from_object"
LANE_DB = ROOT / "bets_data" / "analise_pub_matches" / "lane_dict_raw.sqlite3"
OUT_JSON = ROOT / "runtime" / "lane_kills_adv_pub_backtest_n20000.json"


class _BucketView:
    """Lazy lane-bucket view over a flat key→stats dict (no copy)."""

    __slots__ = ("_flat", "_pred")

    def __init__(self, flat: dict, pred: Callable[[str], bool]):
        self._flat = flat
        self._pred = pred

    def get(self, key: str, default=None):
        if key in self._flat and self._pred(key):
            return self._flat[key]
        return default


def _lazy_structured_lane(flat: dict) -> dict:
    def parts_vs(key: str) -> Optional[tuple[list[str], list[str]]]:
        if "_vs_" not in key:
            return None
        left, right = key.split("_vs_", 1)
        return left.split(","), right.split(",")

    def is_2v2(key: str) -> bool:
        p = parts_vs(key)
        return bool(p) and len(p[0]) == 2 and len(p[1]) == 2

    def is_2v1(key: str) -> bool:
        p = parts_vs(key)
        return bool(p) and (
            (len(p[0]) == 2 and len(p[1]) == 1) or (len(p[0]) == 1 and len(p[1]) == 2)
        )

    def is_1v1(key: str) -> bool:
        p = parts_vs(key)
        return bool(p) and len(p[0]) == 1 and len(p[1]) == 1

    def is_with(key: str) -> bool:
        return "_with_" in key

    def is_solo(key: str) -> bool:
        return "_vs_" not in key and "_with_" not in key

    return {
        "2v2_lanes": _BucketView(flat, is_2v2),
        "2v1_lanes": _BucketView(flat, is_2v1),
        "1v1_lanes": _BucketView(flat, is_1v1),
        "1_with_1_lanes": _BucketView(flat, is_with),
        "solo_lanes": _BucketView(flat, is_solo),
    }


def _load_lane_dict_from_sqlite(db_path: Path) -> dict:
    uri = f"{db_path.resolve().as_uri()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as conn:
        return {
            str(row[0]): {
                "wins": row[1],
                "draws": row[2],
                "games": row[3],
                "kills10_leads": row[4],
                "kills10_draws": row[5],
                "kills10_games": row[6],
                "kills10_diff_sum": row[7],
                "kills10_diff_sq_sum": row[8],
            }
            for row in conn.execute(
                "SELECT key, wins, draws, games, kills10_leads, kills10_draws, "
                "kills10_games, kills10_diff_sum, kills10_diff_sq_sum FROM stats"
            )
        }


def _draft_from_pos_tuple(pos_tuple: tuple[int, int, int, int, int]) -> dict[str, dict[str, int]]:
    return {f"pos{i}": {"hero_id": int(pos_tuple[i - 1])} for i in range(1, 6)}


def _abs_bucket(abs_diff: float, step: float) -> float:
    if abs_diff <= 0:
        return 0.0
    return float(math.ceil(abs_diff / step) * step)


def _bucket_range(label: float, step: float) -> str:
    if label <= 0:
        return "0"
    low = label - step
    if low <= 0:
        return f"(0-{label:g}]"
    return f"({low:g}-{label:g}]"


def _pos_tuple(by_pos: dict[int, int]) -> tuple[int, int, int, int, int]:
    return tuple(int(by_pos[i]) for i in range(1, 6))  # type: ignore[return-value]


def _iter_newest_candidates(limit: int) -> list[dict[str, Any]]:
    """Newest `limit` valid matches; scan newest part files, keep heap of size limit."""
    files = sorted(PUB_DIR.glob("7.41*.json"), reverse=True)
    # heap entries: (start_time, steam_id, actual_diff, r_pos, d_pos, source)
    heap: list[tuple] = []
    scanned = 0
    for path in files:
        print(f"  scanning {path.name} ...", flush=True)
        data = orjson.loads(path.read_bytes())
        if not isinstance(data, dict):
            del data
            gc.collect()
            continue
        for mid, match in data.items():
            scanned += 1
            if not isinstance(match, dict):
                continue
            try:
                start = int(match.get("startDateTime") or 0)
                steam_id = int(mid)
            except (TypeError, ValueError):
                continue
            if start <= 0:
                continue
            diff = _kills10_diff(match)
            if diff is None:
                continue
            r_pos, d_pos = extract_heroes_by_position(match)
            if r_pos is None or d_pos is None:
                continue
            item = (
                start,
                steam_id,
                float(diff),
                _pos_tuple(r_pos),
                _pos_tuple(d_pos),
                path.name,
            )
            if len(heap) < limit:
                heapq.heappush(heap, item)
            elif start > heap[0][0]:
                heapq.heapreplace(heap, item)
        del data
        gc.collect()
        print(f"    heap={len(heap)} scanned={scanned}", flush=True)
        # Newest parts first: once heap full and we left 7.41d*, older files
        # cannot beat the heap minimum start_time much — still need older if
        # heap not full. If full after 7.41d, stop.
        if len(heap) >= limit and not path.name.startswith("7.41d"):
            break
        if len(heap) >= limit and path.name == "7.41d_part001.json":
            # finished all d-series with a full heap of freshest
            break
    rows = [
        {
            "steam_id": steam_id,
            "start_time": start,
            "actual_diff": actual,
            "radiant_draft": _draft_from_pos_tuple(r_pos),
            "dire_draft": _draft_from_pos_tuple(d_pos),
            "source_file": source,
        }
        for start, steam_id, actual, r_pos, d_pos, source in sorted(heap)
    ]
    print(f"  scanned={scanned} kept={len(rows)}", flush=True)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20000)
    ap.add_argument("--bucket-step", type=float, default=1.0)
    ap.add_argument("--out", type=Path, default=OUT_JSON)
    args = ap.parse_args()

    started = datetime.now(timezone.utc).isoformat()
    t0 = time.time()
    print(f"loading lane sqlite: {LANE_DB}", flush=True)
    flat = _load_lane_dict_from_sqlite(LANE_DB)
    print(f"  flat_keys={len(flat):,} in {time.time()-t0:.1f}s", flush=True)
    lane_data = _lazy_structured_lane(flat)
    print(f"  lazy-structured in {time.time()-t0:.1f}s", flush=True)

    print(f"collecting newest {args.limit} pub matches...", flush=True)
    matches = _iter_newest_candidates(args.limit)
    if not matches:
        print("no matches", flush=True)
        return 1

    buckets: dict[float, dict[str, int]] = defaultdict(lambda: {"n": 0, "hits": 0, "ties_actual": 0})
    scored = 0
    skipped_no_pred = 0
    hits = 0
    ties_actual = 0
    samples: list[dict[str, Any]] = []

    for i, match in enumerate(matches, 1):
        pred = calculate_lane_kills_advantage(
            match["radiant_draft"],
            match["dire_draft"],
            lane_data,
        )
        if not isinstance(pred, dict) or pred.get("expected_diff") is None:
            skipped_no_pred += 1
            continue
        expected = float(pred["expected_diff"])
        actual = float(match["actual_diff"])
        abs_exp = abs(expected)
        if expected > 0:
            pick_radiant: Optional[bool] = True
        elif expected < 0:
            pick_radiant = False
        else:
            pick_radiant = None

        if actual == 0:
            hit = False
            ties_actual += 1
            is_tie = True
        elif pick_radiant is None:
            hit = False
            is_tie = False
        else:
            hit = (actual > 0) == pick_radiant
            is_tie = False

        scored += 1
        hits += int(hit)
        label = _abs_bucket(abs_exp, args.bucket_step)
        buckets[label]["n"] += 1
        buckets[label]["hits"] += int(hit)
        buckets[label]["ties_actual"] += int(is_tie)

        if len(samples) < 30:
            samples.append(
                {
                    "steam_id": match["steam_id"],
                    "start_time": match["start_time"],
                    "expected_diff": round(expected, 3),
                    "actual_diff": actual,
                    "abs_expected": round(abs_exp, 3),
                    "bucket": label,
                    "hit": hit,
                    "coverage": pred.get("coverage"),
                    "lead_probability": round(float(pred.get("lead_probability") or 0), 3),
                }
            )
        if i % 2000 == 0:
            wr = 100.0 * hits / scored if scored else 0
            print(f"  [{i}/{len(matches)}] scored={scored} wr={wr:.1f}%", flush=True)

    summary_buckets = []
    for label in sorted(buckets):
        n = buckets[label]["n"]
        h = buckets[label]["hits"]
        summary_buckets.append(
            {
                "abs_bucket": label,
                "range": _bucket_range(label, args.bucket_step),
                "n": n,
                "hits": h,
                "ties_actual": buckets[label]["ties_actual"],
                "wr": round(100.0 * h / n, 1) if n else None,
            }
        )

    payload = {
        "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - t0, 1),
        "limit": args.limit,
        "bucket_step": args.bucket_step,
        "lane_db": str(LANE_DB),
        "matched": len(matches),
        "scored": scored,
        "skipped_no_pred": skipped_no_pred,
        "hits": hits,
        "ties_actual": ties_actual,
        "overall_wr": round(100.0 * hits / scored, 1) if scored else None,
        "method": {
            "metric": "calculate_lane_kills_advantage → expected_diff",
            "window": "kills minutes 0-10 (sum radiantKills[:10]-direKills[:10])",
            "pick": "sign(expected_diff); expected_diff==0 → miss",
            "tie_rule": "actual_diff==0 counts as LOSE",
            "sample": "newest N pub matches by startDateTime from 7.41* parts",
            "note": "in-sample vs lane_dict train (optimistic bias possible)",
        },
        "buckets": summary_buckets,
        "samples": samples,
        "time_range": {
            "min_start": matches[0]["start_time"] if matches else None,
            "max_start": matches[-1]["start_time"] if matches else None,
        },
    }
    args.out.write_text(json.dumps(payload, indent=2, ensure_ascii=False))

    print("\n=== OVERALL ===", flush=True)
    print(
        f"scored={scored} hits={hits} wr={payload['overall_wr']}% "
        f"ties_actual={ties_actual} skipped_no_pred={skipped_no_pred}",
        flush=True,
    )
    print("=== ABS(expected_diff) WR ===", flush=True)
    for b in summary_buckets:
        print(
            f"  {b['range']:>12}: n={b['n']:>5} hits={b['hits']:>5} "
            f"wr={b['wr']}% ties={b['ties_actual']}",
            flush=True,
        )
    print(f"saved {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
