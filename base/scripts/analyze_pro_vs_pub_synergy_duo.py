#!/usr/bin/env python3
"""Compare Dota2ProTracker pro duo synergy with public-match synergy blocks.

Read-only artifact builder. It evaluates the same drafts from
pro_matches_with_metrics.json against the current public early/late/post-lane
statistics, then reports coverage, sign overlap, correlations and joint outcome
quality. It does not alter STAR configuration.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from pathlib import Path
from statistics import NormalDist
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "base"
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

import orjson  # noqa: E402
import functions  # noqa: E402
from dota2protracker import get_hero_id  # noqa: E402

POSITIONS = ("pos1", "pos2", "pos3", "pos4", "pos5")
PHASES = {
    "early": "early_output",
    "late": "mid_output",
    "post_lane": "post_lane_output",
}


class SqliteLookup(dict):
    def __init__(self, path: Path):
        super().__init__()
        uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
        self.conn = sqlite3.connect(uri, uri=True)

    def __bool__(self):
        # post_lane processing is guarded by ``if post_lane_dict`` in the live
        # metric function.  A lazy lookup is intentionally non-empty even though
        # its inherited in-memory dict storage has no rows.
        return True

    def get(self, key: Any, default=None):
        row = self.conn.execute("SELECT value FROM kv WHERE key = ?", (str(key),)).fetchone()
        if row is None:
            return default
        return orjson.loads(row[0])

    def close(self):
        self.conn.close()


def _draft_side(raw: dict) -> dict:
    out = {}
    for pos in POSITIONS:
        name = str((raw or {}).get(pos) or "").strip()
        hero_id = int(get_hero_id(name) or 0)
        if hero_id <= 0:
            return {}
        out[pos] = {"hero_id": hero_id, "hero_name": name}
    return out


def _finite(value: Any):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _sign(value: float) -> int:
    return 1 if value > 0 else -1 if value < 0 else 0


def _pearson(xs: list[float], ys: list[float]):
    if len(xs) < 3:
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(vx * vy)


def _rank(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=values.__getitem__)
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i + 1
        while j < len(order) and values[order[j]] == values[order[i]]:
            j += 1
        rank = (i + j - 1) / 2.0 + 1.0
        for k in order[i:j]:
            ranks[k] = rank
        i = j
    return ranks


def _wilson(wins: int, total: int, z: float = 1.959963984540054):
    if total <= 0:
        return None
    p = wins / total
    den = 1 + z * z / total
    center = (p + z * z / (2 * total)) / den
    half = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / den
    return [center - half, center + half]


def _metric_stats(rows: list[dict], phase: str, pro_thr: float, pub_thr: float) -> dict:
    covered = [r for r in rows if r[phase] is not None]
    xs = [r["pro"] for r in covered]
    ys = [r[phase] for r in covered]
    sign_same = sum(_sign(x) == _sign(y) for x, y in zip(xs, ys) if x and y)
    sign_den = sum(bool(x) and bool(y) for x, y in zip(xs, ys))

    def outcome(pred_key: str, predicate):
        chosen = [r for r in covered if predicate(r)]
        wins = sum(_sign(r[pred_key]) == r["winner_sign"] for r in chosen)
        return {
            "n": len(chosen),
            "wins": wins,
            "wr": wins / len(chosen) if chosen else None,
            "wilson95": _wilson(wins, len(chosen)),
        }

    pro_hit = lambda r: abs(r["pro"]) >= pro_thr
    pub_hit = lambda r: abs(r[phase]) >= pub_thr
    return {
        "coverage": {"n": len(covered), "of": len(rows), "rate": len(covered) / len(rows) if rows else 0},
        "pearson": _pearson(xs, ys),
        "spearman": _pearson(_rank(xs), _rank(ys)) if len(xs) >= 3 else None,
        "sign_agreement": {"same": sign_same, "n": sign_den, "rate": sign_same / sign_den if sign_den else None},
        "all_covered": outcome(phase, lambda r: bool(r[phase])),
        "pro_abs_ge": {"threshold": pro_thr, **outcome("pro", pro_hit)},
        "pub_abs_ge": {"threshold": pub_thr, **outcome(phase, pub_hit)},
        "both_hit_same_sign": outcome(
            phase,
            lambda r: pro_hit(r) and pub_hit(r) and _sign(r["pro"]) == _sign(r[phase]),
        ),
        "both_hit_opposite_sign": outcome(
            phase,
            lambda r: pro_hit(r) and pub_hit(r) and _sign(r["pro"]) != _sign(r[phase]),
        ),
        "pro_hit_pub_miss": outcome("pro", lambda r: pro_hit(r) and not pub_hit(r)),
        "pub_hit_pro_miss": outcome(phase, lambda r: pub_hit(r) and not pro_hit(r)),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=ROOT / "pro_matches_with_metrics.json")
    ap.add_argument("--stats-dir", type=Path, default=ROOT / "bets_data/analise_pub_matches")
    ap.add_argument("--output", type=Path, default=ROOT / "base/ml_dataset/pro_vs_pub_synergy_duo_overlap.json")
    ap.add_argument("--pro-threshold", type=float, default=5.0)
    ap.add_argument("--pub-threshold", type=float, default=5.0)
    args = ap.parse_args()

    source = json.loads(args.input.read_text(encoding="utf-8"))
    lookups = {
        "early": SqliteLookup(args.stats_dir / "early_dict_raw.sqlite3"),
        "late": SqliteLookup(args.stats_dir / "late_dict_raw.sqlite3"),
        "post_lane": SqliteLookup(args.stats_dir / "post_lane_dict_raw.sqlite3"),
    }
    rows = []
    invalid_draft = 0
    try:
        for item in source:
            if not bool(item.get("synergy_duo_valid")):
                continue
            pro = _finite(item.get("synergy_duo"))
            if pro is None or pro == 0:
                continue
            draft = item.get("draft") or {}
            radiant = _draft_side(draft.get("radiant") or {})
            dire = _draft_side(draft.get("dire") or {})
            if len(radiant) != 5 or len(dire) != 5:
                invalid_draft += 1
                continue
            result = functions.synergy_and_counterpick(
                radiant_heroes_and_pos=radiant,
                dire_heroes_and_pos=dire,
                early_dict=lookups["early"],
                mid_dict=lookups["late"],
                post_lane_dict=lookups["post_lane"],
            ) or {}
            row = {
                "map_id": str(item.get("map_id")),
                "winner_sign": 1 if item.get("winner") == "radiant" else -1,
                "pro": pro,
            }
            for phase, bucket_name in PHASES.items():
                bucket = result.get(bucket_name) or {}
                row[phase] = _finite(bucket.get("synergy_duo"))
            rows.append(row)
    finally:
        for lookup in lookups.values():
            lookup.close()

    report = {
        "sources": {
            "pro_metric": str(args.input),
            "public_stats_dir": str(args.stats_dir),
            "pro_semantics": "Dota2ProTracker duo synergy; same value exposed as early and late",
            "public_semantics": "public-match early/mid/post_lane synergy_duo from synergy_and_counterpick",
        },
        "input": {
            "rows": len(source),
            "valid_pro_duo": sum(bool(x.get("synergy_duo_valid")) for x in source),
            "compared_rows": len(rows),
            "invalid_draft_rows": invalid_draft,
        },
        "thresholds": {"pro_abs": args.pro_threshold, "pub_abs": args.pub_threshold},
        "phases": {
            phase: _metric_stats(rows, phase, args.pro_threshold, args.pub_threshold)
            for phase in PHASES
        },
        "rows": rows,
        "limitations": [
            "The 307-map pro artifact is dated and small; this is not a current walk-forward calibration.",
            "Pro and public scores have different source populations and aggregation contracts.",
            "Outcome WR is final-map winner for comparability, not the early-dominator label.",
            "No STAR production change is justified without a time-frozen policy replay on a larger pro holdout.",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "input": report["input"], "phases": report["phases"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
