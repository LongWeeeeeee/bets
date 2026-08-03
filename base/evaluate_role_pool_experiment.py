#!/usr/bin/env python3
"""Frozen-OOS evaluation of core/support pooled cp1vs2 and synergy_trio.

The candidate cp1vs2 gate is N>=25.  The candidate synergy_trio gates are
N>=25/50/75/100.  Existing exact-position values stored in each metrics row are
the baseline.  This script only reads frozen metrics/statistics artifacts.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import sqlite3
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime"))

import functions  # noqa: E402
from base.cp1vs2_role_pool import make_role_key  # noqa: E402
from base.synergy_trio_role_pool import make_trio_role_key  # noqa: E402
from star_old_vs_family_eval import custom_all_maps  # noqa: E402
from star_two_vs_three_hit_eval import LEVELS, metric_level, threshold_maps  # noqa: E402


POSITIONS = ("pos1", "pos2", "pos3", "pos4", "pos5")
PHASES = {
    "early": ("early_output", "early_output"),
    "late": ("late_output", "mid_output"),
    "all": ("post_lane_output", "all_output"),
}
CP_GATES = (25,)
TRIO_GATES = (25, 50, 75, 100)


class RoleStatsLookup:
    def __init__(self, path: Path):
        uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
        self.conn = sqlite3.connect(uri, uri=True)
        self.conn.execute("PRAGMA query_only=ON")
        self.conn.execute("PRAGMA mmap_size=1073741824")

    def get_many(self, table: str, keys: Iterable[str]) -> dict[str, tuple[float, int]]:
        result: dict[str, tuple[float, int]] = {}
        values = sorted(set(keys))
        for start in range(0, len(values), 800):
            chunk = values[start:start + 800]
            if not chunk:
                continue
            placeholders = ",".join("?" for _ in chunk)
            query = f"SELECT key, score, games FROM {table} WHERE key IN ({placeholders})"
            for key, score, games in self.conn.execute(query, chunk):
                result[str(key)] = (float(score), int(games))
        return result

    def close(self) -> None:
        self.conn.close()


def hero_token(draft: dict[str, Any], position: str) -> str | None:
    try:
        hero_id = int((draft.get(position) or {}).get("hero_id"))
    except (TypeError, ValueError):
        return None
    return f"{hero_id}{position}" if hero_id > 0 else None


def draft_tokens(draft: dict[str, Any]) -> list[str] | None:
    values = [hero_token(draft, position) for position in POSITIONS]
    return [str(value) for value in values] if all(values) else None


def cp_role_keys(radiant: dict[str, Any], dire: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for team, enemy in ((radiant, dire), (dire, radiant)):
        own_tokens = draft_tokens(team)
        enemy_tokens = draft_tokens(enemy)
        if own_tokens is None or enemy_tokens is None:
            continue
        for own in own_tokens:
            for duo in combinations(enemy_tokens, 2):
                key = make_role_key(own, duo)
                if key:
                    keys.add(key)
    return keys


def trio_role_keys(radiant: dict[str, Any], dire: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for team in (radiant, dire):
        tokens = draft_tokens(team)
        if tokens is None:
            continue
        for trio in combinations(tokens, 3):
            key = make_trio_role_key(trio)
            if key:
                keys.add(key)
    return keys


def _entry(data: dict[str, tuple[float, int]], key: str | None, gate: int):
    if key is None or key not in data:
        return None
    score, games = data[key]
    if games < gate or games <= 0:
        return None
    return score / games, games


def cp_score(radiant: dict[str, Any], dire: dict[str, Any],
             data: dict[str, tuple[float, int]], gate: int, phase: str) -> int | None:
    def side(team: dict[str, Any], enemy: dict[str, Any]):
        own_tokens = draft_tokens(team)
        enemy_tokens = draft_tokens(enemy)
        if own_tokens is None or enemy_tokens is None:
            return None
        values: dict[str, list[tuple[float, int]]] = {}
        for index, own in enumerate(own_tokens):
            for duo in combinations(enemy_tokens, 2):
                found = _entry(data, make_role_key(own, duo), gate)
                if found is not None:
                    values.setdefault(POSITIONS[index], []).append(found)
        return values

    radiant_values = side(radiant, dire)
    dire_values = side(dire, radiant)
    if radiant_values is None or dire_values is None:
        return None
    # Match production's _covers_1vs2: all five heroes need at least one entry.
    if not all(radiant_values.get(pos) and dire_values.get(pos) for pos in POSITIONS):
        return None
    weights = functions.EARLY_POSITION_WEIGHTS if phase == "early" else functions.LATE_POSITION_WEIGHTS
    return functions.get_diff(
        radiant_values, dire_values, _1vs2=True, custom_position_weights=weights
    )


def trio_score(radiant: dict[str, Any], dire: dict[str, Any],
               data: dict[str, tuple[float, int]], gate: int) -> int | None:
    def side(team: dict[str, Any]):
        tokens = draft_tokens(team)
        if tokens is None:
            return None
        values: list[tuple[float, int]] = []
        covered: set[int] = set()
        for indexes in combinations(range(len(tokens)), 3):
            trio = [tokens[index] for index in indexes]
            found = _entry(data, make_trio_role_key(trio), gate)
            if found is not None:
                values.append(found)
                covered.update(indexes)
        return values if len(covered) == len(POSITIONS) else None

    radiant_values = side(radiant)
    dire_values = side(dire)
    if not radiant_values or not dire_values:
        return None
    return functions.get_diff(radiant_values, dire_values)


def number(value: Any) -> float | None:
    try:
        parsed = float(str(value).rstrip("*"))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def iter_records(patterns: str):
    paths = sorted({
        Path(path)
        for token in patterns.split(",")
        for path in glob.glob(token.strip())
        if Path(path).is_file()
    })
    if not paths:
        raise SystemExit(f"no metrics matched: {patterns}")
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        values = payload.values() if isinstance(payload, dict) else payload
        rows = [row for row in values if isinstance(row, dict) and row.get("didRadiantWin") is not None]
        rows.sort(key=lambda row: int(row.get("startDateTime") or 0))
        print(f"{path.name}: {len(rows):,}", flush=True)
        yield from rows


def iter_record_chunks(patterns: str, chunk_size: int):
    chunk = []
    for row in iter_records(patterns):
        chunk.append(row)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def wilson_lower(wins: int, n: int) -> float:
    if n <= 0:
        return 0.0
    z = 1.96
    p = wins / n
    denominator = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (centre - margin) / denominator


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", required=True)
    parser.add_argument("--role-stats-dir", type=Path, required=True)
    parser.add_argument("--thresholds", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--paired-output", type=Path, required=True)
    parser.add_argument("--split-index", type=int, default=52_711)
    parser.add_argument("--chunk-size", type=int, default=250)
    args = parser.parse_args()

    threshold_payload = json.loads(args.thresholds.read_text(encoding="utf-8"))
    star_maps = {
        "early": threshold_maps(threshold_payload, "early_output"),
        "late": threshold_maps(threshold_payload, "mid_output"),
        "all": custom_all_maps(threshold_payload),
    }
    lookups = {
        phase: RoleStatsLookup(args.role_stats_dir / f"{phase}.sqlite3")
        for phase in PHASES
    }
    stats: dict[tuple[str, str, str, str, int], list[int]] = defaultdict(lambda: [0, 0])
    paired: dict[tuple[str, str, str, int, int, str], list[int]] = defaultdict(lambda: [0, 0, 0])
    coverage: dict[tuple[str, str, str, str], list[int]] = defaultdict(lambda: [0, 0])

    processed = 0
    for chunk in iter_record_chunks(args.metrics, max(1, args.chunk_size)):
        row_keys = []
        for row in chunk:
            radiant = row.get("radiant_draft") or {}
            dire = row.get("dire_draft") or {}
            row_keys.append((cp_role_keys(radiant, dire), trio_role_keys(radiant, dire)))
        all_cp_keys = set().union(*(item[0] for item in row_keys)) if row_keys else set()
        all_trio_keys = set().union(*(item[1] for item in row_keys)) if row_keys else set()
        phase_data = {
            phase: (
                lookup.get_many("cp1vs2", all_cp_keys),
                lookup.get_many("synergy_trio", all_trio_keys),
            )
            for phase, lookup in lookups.items()
        }

        for row, _keys in zip(chunk, row_keys, strict=True):
            segment = "discovery" if processed < args.split_index else "validation"
            processed += 1
            segments = (segment, "full")
            winner = 1 if bool(row["didRadiantWin"]) else -1
            radiant = row.get("radiant_draft") or {}
            dire = row.get("dire_draft") or {}

            for phase, (output_name, _threshold_name) in PHASES.items():
                output = row.get(output_name) or {}
                cp_data, trio_data = phase_data[phase]
                candidates = {
                    ("counterpick_1vs2", 25): cp_score(radiant, dire, cp_data, 25, phase),
                    **{
                        ("synergy_trio", gate): trio_score(radiant, dire, trio_data, gate)
                        for gate in TRIO_GATES
                    },
                }
                for (metric, gate), candidate_score in candidates.items():
                    baseline_score = number(output.get(metric))
                    baseline_variant = f"exact_ref_n{gate}"
                    candidate_variant = f"role_n{gate}"
                    for seg in segments:
                        coverage[(seg, phase, metric, baseline_variant)][0] += 1
                        coverage[(seg, phase, metric, baseline_variant)][1] += int(baseline_score is not None)
                        coverage[(seg, phase, metric, candidate_variant)][0] += 1
                        coverage[(seg, phase, metric, candidate_variant)][1] += int(candidate_score is not None)
                    predictions = {}
                    for variant, score in ((baseline_variant, baseline_score), (candidate_variant, candidate_score)):
                        level = metric_level(metric, score, star_maps[phase])
                        sign = 1 if score is not None and score > 0 else -1
                        predictions[variant] = (score, level, sign)
                        if score in (None, 0) or level is None:
                            continue
                        for target_level in LEVELS:
                            if level >= target_level:
                                for seg in segments:
                                    bucket = stats[(seg, phase, metric, variant, target_level)]
                                    bucket[0] += 1
                                    bucket[1] += int(sign == winner)
                    baseline = predictions[baseline_variant]
                    candidate = predictions[candidate_variant]
                    for target_level in LEVELS:
                        base_hit = baseline[1] is not None and baseline[1] >= target_level
                        cand_hit = candidate[1] is not None and candidate[1] >= target_level
                        if base_hit and cand_hit:
                            category = "both_same" if baseline[2] == candidate[2] else "both_flip"
                        elif cand_hit:
                            category = "candidate_only"
                        elif base_hit:
                            category = "baseline_only"
                        else:
                            continue
                        for seg in segments:
                            bucket = paired[(seg, phase, metric, gate, target_level, category)]
                            bucket[0] += 1
                            bucket[1] += int(base_hit and baseline[2] == winner)
                            bucket[2] += int(cand_hit and candidate[2] == winner)
        if processed % 2_000 < len(chunk):
            print(f"processed={processed:,}", flush=True)

    for lookup in lookups.values():
        lookup.close()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    temp = args.output.with_suffix(args.output.suffix + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["row_type", "segment", "phase", "metric", "variant", "star_level", "n", "wins_or_covered", "wr_or_coverage_pct", "wilson95_lower_pct"])
        for key, (n, wins) in sorted(stats.items()):
            writer.writerow(["star", *key, n, wins, round(100 * wins / n, 4), round(100 * wilson_lower(wins, n), 4)])
        for key, (total, covered) in sorted(coverage.items()):
            writer.writerow(["coverage", *key, "", total, covered, round(100 * covered / total, 4), ""])
    temp.replace(args.output)

    paired_temp = args.paired_output.with_suffix(args.paired_output.suffix + ".tmp")
    with paired_temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["segment", "phase", "metric", "gate", "star_level", "category", "n", "baseline_wins", "candidate_wins"])
        for key, values in sorted(paired.items()):
            writer.writerow([*key, *values])
    paired_temp.replace(args.paired_output)
    print(f"completed processed={processed:,} output={args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
