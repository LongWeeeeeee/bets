#!/usr/bin/env python3
"""Backtest calculate_kills_window_advantage on newest public matches.

For each window (5-15 / 10-20 / 15-25 / 20-30):
  actual_diff = sum(radiantKills[s:e]) - sum(direKills[s:e])
  predict sign(expected_diff); actual_diff==0 or expected_diff==0 → LOSE.

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
from typing import Any, Optional

ROOT = Path("/root/main")
BASE = ROOT / "base"
sys.path = [str(BASE)] + [p for p in sys.path if Path(p).resolve() != ROOT]

import orjson  # noqa: E402
from analise_database import (  # noqa: E402
    KILLS_WINDOWS,
    _kills_window_diff,
    extract_heroes_by_position,
)
from functions import calculate_kills_window_advantage  # noqa: E402

PUB_DIR = ROOT / "bets_data" / "analise_pub_matches" / "json_parts_split_from_object"
KW_DB = ROOT / "bets_data" / "analise_pub_matches" / "kills_window_dict_raw.sqlite3"
OUT_JSON = ROOT / "runtime" / "kills_window_adv_pub_backtest_n20000.json"


def _tokens(by_pos: dict) -> list[str]:
    return [f"{hero}pos{pos}" for pos, hero in sorted(by_pos.items())]


def _load_kills_window_dict(path: Path) -> dict[str, dict[str, float]]:
    uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
    out: dict[str, dict[str, float]] = {}
    with sqlite3.connect(uri, uri=True) as conn:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(stats)").fetchall()]
        value_cols = [c for c in cols if c != "key"]
        select = "key, " + ", ".join(value_cols)
        for row in conn.execute(f"SELECT {select} FROM stats"):
            key = row[0]
            out[key] = {name: row[i + 1] for i, name in enumerate(value_cols)}
    return out


def _bucket(abs_diff: float) -> str:
    if abs_diff < 0.5:
        return "0-0.5"
    if abs_diff < 1.0:
        return "0.5-1"
    if abs_diff < 2.0:
        return "1-2"
    if abs_diff < 3.0:
        return "2-3"
    return "3+"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=20000)
    parser.add_argument("--db", type=Path, default=KW_DB)
    parser.add_argument("--out", type=Path, default=OUT_JSON)
    args = parser.parse_args()

    if not args.db.exists():
        print(f"missing dict: {args.db}", file=sys.stderr)
        return 1

    print(f"loading {args.db} ...", flush=True)
    t0 = time.monotonic()
    heroes_data = _load_kills_window_dict(args.db)
    print(f"  keys={len(heroes_data):,} in {time.monotonic() - t0:.1f}s", flush=True)

    # newest N matches by startDateTime across parts
    heap: list[tuple[int, str, Path]] = []  # (ts, mid, file) min-heap size N
    for part in sorted(PUB_DIR.glob("*.json")):
        try:
            data = orjson.loads(part.read_bytes())
        except Exception as exc:
            print(f"  skip {part.name}: {exc}")
            continue
        if not isinstance(data, dict):
            continue
        for mid, match in data.items():
            if not isinstance(match, dict):
                continue
            try:
                ts = int(match.get("startDateTime") or 0)
            except (TypeError, ValueError):
                ts = 0
            item = (ts, str(mid), part)
            if len(heap) < args.n:
                heapq.heappush(heap, item)
            elif ts > heap[0][0]:
                heapq.heapreplace(heap, item)
        del data
        gc.collect()

    selected = sorted(heap, key=lambda x: x[0], reverse=True)
    print(f"selected {len(selected)} newest matches", flush=True)

    # group by file for one re-read
    by_file: dict[Path, list[str]] = defaultdict(list)
    for _ts, mid, part in selected:
        by_file[part].append(mid)

    stats: dict[str, dict[str, Any]] = {
        f"{s}_{e}": {
            "n": 0,
            "correct": 0,
            "skip_no_actual": 0,
            "skip_no_pred": 0,
            "buckets": defaultdict(lambda: {"n": 0, "correct": 0}),
        }
        for s, e in KILLS_WINDOWS
    }

    processed = 0
    for part, mids in by_file.items():
        want = set(mids)
        data = orjson.loads(part.read_bytes())
        for mid in want:
            match = data.get(mid)
            if not isinstance(match, dict):
                continue
            r_by_pos, d_by_pos = extract_heroes_by_position(match)
            if r_by_pos is None:
                continue
            radiant = _tokens(r_by_pos)
            dire = _tokens(d_by_pos)
            preds = calculate_kills_window_advantage(
                radiant, dire, heroes_data, window=None
            )
            if not isinstance(preds, dict):
                continue
            for start, end in KILLS_WINDOWS:
                label = f"{start}_{end}"
                bucket = stats[label]
                actual = _kills_window_diff(match, start, end)
                if actual is None:
                    bucket["skip_no_actual"] += 1
                    continue
                pred = preds.get(label)
                if not isinstance(pred, dict):
                    bucket["skip_no_pred"] += 1
                    continue
                expected = float(pred.get("expected_diff") or 0.0)
                if expected == 0.0 or actual == 0.0:
                    # tie / zero-pred = LOSE (same contract as lane@10 backtest)
                    hit = False
                else:
                    hit = (expected > 0) == (actual > 0)
                bucket["n"] += 1
                bucket["correct"] += int(hit)
                b = _bucket(abs(expected))
                bucket["buckets"][b]["n"] += 1
                bucket["buckets"][b]["correct"] += int(hit)
            processed += 1
        del data
        gc.collect()

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_selected": len(selected),
        "n_processed": processed,
        "db": str(args.db),
        "windows": {},
    }
    for label, bucket in stats.items():
        n = bucket["n"]
        wr = (bucket["correct"] / n) if n else None
        buckets_out = {}
        for name, val in sorted(bucket["buckets"].items()):
            bn = val["n"]
            buckets_out[name] = {
                "n": bn,
                "correct": val["correct"],
                "wr": (val["correct"] / bn) if bn else None,
            }
        report["windows"][label] = {
            "n": n,
            "correct": bucket["correct"],
            "wr": wr,
            "skip_no_actual": bucket["skip_no_actual"],
            "skip_no_pred": bucket["skip_no_pred"],
            "buckets": buckets_out,
        }
        print(
            f"{label}: n={n} wr={wr:.4f}" if wr is not None else f"{label}: n=0",
            flush=True,
        )
        for name, val in buckets_out.items():
            if val["wr"] is None:
                continue
            print(f"  |diff| {name}: n={val['n']} wr={val['wr']:.4f}", flush=True)

    args.out.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    print(f"wrote {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
