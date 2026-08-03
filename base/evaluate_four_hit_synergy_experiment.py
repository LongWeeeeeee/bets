#!/usr/bin/env python3
"""Measure role-pooled synergy_trio only as a fourth STAR confirmation.

Base hits are the WR60 hits from counterpick_1vs1, solo and counterpick_1vs2.
The experiment compares exact two-base-hit consensus, three-base-hit consensus,
and the nested four-hit subset where role-pooled synergy_trio confirms the same
side.  Trio thresholds are swept independently because a metric can add useful
conditional evidence even when it has no reliable standalone WR threshold.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "base", ROOT / "runtime"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from base.evaluate_role_pool_experiment import (  # noqa: E402
    PHASES,
    RoleStatsLookup,
    iter_record_chunks,
    number,
    trio_role_keys,
    trio_score,
    wilson_lower,
)


BASE_METRICS = ("counterpick_1vs1", "solo", "counterpick_1vs2")
TARGET_WR = "60"
ALL_SOLO_WR60_THRESHOLD = 4.0


def wr60_thresholds(payload: dict[str, Any], section: str) -> dict[str, float]:
    return {
        str(metric): float(threshold)
        for metric, threshold in (payload.get(TARGET_WR, {}).get(section) or [])
        if metric in BASE_METRICS and number(threshold) not in (None, 0)
    }


def base_hit_signs(
    output: dict[str, Any],
    thresholds: dict[str, float],
) -> dict[str, int]:
    hits: dict[str, int] = {}
    for metric in BASE_METRICS:
        value = number(output.get(metric))
        threshold = thresholds.get(metric)
        if value in (None, 0) or threshold is None or abs(float(value)) < threshold:
            continue
        hits[metric] = 1 if float(value) > 0 else -1
    return hits


def consensus_sign(hits: dict[str, int], exact_count: int) -> int | None:
    if len(hits) != exact_count or len(set(hits.values())) != 1:
        return None
    return next(iter(hits.values()))


def exact_two_variant(hits: dict[str, int]) -> str:
    return "hits2_" + "+".join(metric.replace("counterpick_", "cp") for metric in BASE_METRICS if metric in hits)


def add(
    stats: dict[tuple[str, str, str, int], list[int]],
    segments: tuple[str, str],
    phase: str,
    variant: str,
    threshold: int,
    won: bool,
) -> None:
    for segment in segments:
        bucket = stats[(segment, phase, variant, threshold)]
        bucket[0] += 1
        bucket[1] += int(won)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", required=True)
    parser.add_argument("--role-stats-dir", required=True, type=Path)
    parser.add_argument("--thresholds", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--split-index", type=int, default=52_711)
    parser.add_argument("--trio-min-index", type=int, default=1)
    parser.add_argument("--trio-max-index", type=int, default=50)
    parser.add_argument("--chunk-size", type=int, default=250)
    args = parser.parse_args()

    threshold_payload = json.loads(args.thresholds.read_text(encoding="utf-8"))
    base_thresholds = {
        phase: wr60_thresholds(threshold_payload, threshold_section)
        for phase, (_output_name, threshold_section) in PHASES.items()
    }
    # User-calibrated All solo ladder starts at |solo|>=4 for WR60.  Solo is
    # intentionally absent from the shared JSON's All section, so make this
    # experiment's already-established family rule explicit.
    base_thresholds["all"]["solo"] = ALL_SOLO_WR60_THRESHOLD
    lookups = {
        phase: RoleStatsLookup(args.role_stats_dir / f"{phase}.sqlite3")
        for phase in PHASES
    }
    stats: dict[tuple[str, str, str, int], list[int]] = defaultdict(lambda: [0, 0])
    trio_thresholds = range(
        max(1, args.trio_min_index),
        max(args.trio_min_index, args.trio_max_index) + 1,
    )

    processed = 0
    for chunk in iter_record_chunks(args.metrics, max(1, args.chunk_size)):
        keys_by_row = []
        for row in chunk:
            radiant = row.get("radiant_draft") or {}
            dire = row.get("dire_draft") or {}
            keys_by_row.append(trio_role_keys(radiant, dire))
        all_keys = set().union(*keys_by_row) if keys_by_row else set()
        phase_data = {
            phase: lookup.get_many("synergy_trio", all_keys)
            for phase, lookup in lookups.items()
        }

        for row in chunk:
            segment = "discovery" if processed < args.split_index else "validation"
            processed += 1
            segments = (segment, "full")
            winner = 1 if bool(row["didRadiantWin"]) else -1
            radiant = row.get("radiant_draft") or {}
            dire = row.get("dire_draft") or {}

            for phase, (output_name, _threshold_section) in PHASES.items():
                output = row.get(output_name) or {}
                if not isinstance(output, dict):
                    continue
                hits = base_hit_signs(output, base_thresholds[phase])
                hits2_sign = consensus_sign(hits, 2)
                if hits2_sign is not None:
                    won = hits2_sign == winner
                    add(stats, segments, phase, "hits2_any", 0, won)
                    add(stats, segments, phase, exact_two_variant(hits), 0, won)

                hits3_sign = consensus_sign(hits, 3)
                if hits3_sign is None:
                    continue
                base_won = hits3_sign == winner
                add(stats, segments, phase, "hits3_base", 0, base_won)

                role_score = trio_score(radiant, dire, phase_data[phase], 25)
                for threshold in trio_thresholds:
                    if role_score in (None, 0) or abs(float(role_score)) < threshold:
                        variant = "hits3_trio_below_or_missing"
                        won = base_won
                    elif (1 if float(role_score) > 0 else -1) == hits3_sign:
                        variant = "hits4_trio_same"
                        won = base_won
                    else:
                        variant = "hits3_trio_conflict"
                        won = base_won
                    add(stats, segments, phase, variant, threshold, won)

        if processed % 2_000 < len(chunk):
            print(f"processed={processed:,}", flush=True)

    for lookup in lookups.values():
        lookup.close()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    temp = args.output.with_suffix(args.output.suffix + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "segment", "phase", "variant", "trio_abs_threshold",
            "n", "wins", "wr_pct", "wilson95_lower_pct",
        ])
        for key, (n, wins) in sorted(stats.items()):
            writer.writerow([
                *key,
                n,
                wins,
                round(100 * wins / n, 4),
                round(100 * wilson_lower(wins, n), 4),
            ])
    temp.replace(args.output)
    print(f"completed processed={processed:,} output={args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
