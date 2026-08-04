#!/usr/bin/env python3
"""Frozen 7.41d experiment for Early and Late solo-table filters."""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import math
import os
import sys
from array import array
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
PROJECT_BASE_DIR = SCRIPT_DIR if SCRIPT_DIR.name == "base" else PROJECT_DIR / "base"
# A server research run may copy this script into runtime/. Keep that directory
# available for a helper copied beside it, and add the canonical project/base
# directory for analise_database and the tracked helper.
for import_dir in (PROJECT_BASE_DIR, SCRIPT_DIR):
    import_path = str(import_dir)
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

try:  # Package import in tests/local modules.
    from . import analise_database as production_stats
    from .evaluate_post_lane_solo_gate import (
        CorpusScanError,
        Prediction,
        _atomic_json,
        _atomic_text,
        chronological_group_split,
        is_pro_match,
        iter_json_object,
        paired_statistics,
        raw_buckets,
        rounded_buckets,
        wilson_interval,
    )
except ImportError:  # Direct ``python base/evaluate_solo_phase_filters.py`` / copied server file.
    import analise_database as production_stats  # type: ignore
    from evaluate_post_lane_solo_gate import (  # type: ignore
        CorpusScanError,
        Prediction,
        _atomic_json,
        _atomic_text,
        chronological_group_split,
        is_pro_match,
        iter_json_object,
        paired_statistics,
        raw_buckets,
        rounded_buckets,
        wilson_interval,
    )

ROOT_DIR = PROJECT_DIR
DEFAULT_INPUT_DIR = ROOT_DIR / "bets_data" / "analise_pub_matches" / "json_parts_split_from_object"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "runtime" / "solo_phase_filters"
PATCH = "7.41d"
PATCH_START = 1_780_531_200
MIN_CELL_GAMES = 50
EARLY_WEIGHTS = {1: 1.4, 2: 1.6, 3: 1.4, 4: 1.2, 5: 0.8}
LATE_WEIGHTS = {1: 2.4, 2: 2.2, 3: 1.4, 4: 1.2, 5: 0.6}


@dataclass(frozen=True)
class PhaseRow:
    map_id: int
    start_time: int
    radiant_win: bool
    hero_ids: tuple[int, ...]  # Radiant pos1..5, then Dire pos1..5.
    duration: int
    # Float32 is exact for realistic integer net-worth leads and keeps the full
    # public corpus substantially smaller than tuples of Python integers.
    leads: array


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def canonicalize_match(raw: Any, object_key: Any = None) -> tuple[PhaseRow | None, str | None]:
    if not isinstance(raw, Mapping):
        return None, "not_object"
    raw_id = raw.get("id", object_key)
    if isinstance(raw_id, bool):
        return None, "invalid_map_id"
    try:
        map_id = int(raw_id)
    except (TypeError, ValueError, OverflowError):
        return None, "invalid_map_id"
    if map_id <= 0:
        return None, "invalid_map_id"
    raw_time = raw.get("startDateTime")
    if isinstance(raw_time, bool):
        return None, "invalid_start_time"
    try:
        start_time = int(raw_time)
    except (TypeError, ValueError, OverflowError):
        return None, "invalid_start_time"
    if start_time < PATCH_START:
        return None, "outside_7_41d"
    if is_pro_match(raw):
        return None, "pro_match"
    radiant_win = raw.get("didRadiantWin")
    if not isinstance(radiant_win, bool):
        return None, "invalid_outcome"

    players = raw.get("players")
    if not isinstance(players, list) or len(players) != 10:
        return None, "invalid_player_count"
    sides: dict[bool, dict[int, int]] = {True: {}, False: {}}
    heroes: set[int] = set()
    for player in players:
        if not isinstance(player, Mapping):
            return None, "invalid_player"
        side = player.get("isRadiant")
        if not isinstance(side, bool):
            return None, "invalid_side"
        position_raw = player.get("position")
        if not isinstance(position_raw, str) or not position_raw.startswith("POSITION_"):
            return None, "invalid_position"
        try:
            position = int(position_raw[len("POSITION_") :])
        except ValueError:
            return None, "invalid_position"
        if position not in EARLY_WEIGHTS or position in sides[side]:
            return None, "duplicate_position" if position in sides[side] else "invalid_position"
        hero_raw = player.get("heroId")
        if isinstance(hero_raw, bool):
            return None, "invalid_hero_id"
        try:
            hero_id = int(hero_raw)
        except (TypeError, ValueError, OverflowError):
            return None, "invalid_hero_id"
        if hero_id <= 0:
            return None, "invalid_hero_id"
        if hero_id in heroes:
            return None, "duplicate_hero"
        heroes.add(hero_id)
        sides[side][position] = hero_id
    positions = set(EARLY_WEIGHTS)
    if set(sides[True]) != positions or set(sides[False]) != positions:
        return None, "incomplete_positions"

    raw_leads = raw.get("radiantNetworthLeads")
    if not isinstance(raw_leads, list):
        raw_leads = []
    compact_leads = array("f")
    for value in raw_leads:
        number = _finite(value)
        compact_leads.append(number if number is not None else math.nan)
    hero_ids = tuple(sides[side][position] for side in (True, False) for position in EARLY_WEIGHTS)
    return PhaseRow(map_id, start_time, radiant_win, hero_ids, len(raw_leads), compact_leads), None


def scan_corpus(paths: Iterable[Path]) -> tuple[list[PhaseRow], dict[str, Any]]:
    rows: list[PhaseRow] = []
    seen: set[int] = set()
    rejected: Counter[str] = Counter()
    files: list[dict[str, Any]] = []
    scanned = 0
    for path in sorted(Path(value) for value in paths):
        shard_scanned = shard_accepted = 0
        shard_rejected: Counter[str] = Counter()
        try:
            for object_key, raw in iter_json_object(path):
                scanned += 1
                shard_scanned += 1
                row, reason = canonicalize_match(raw, object_key)
                if row is None:
                    reason = reason or "unknown_invalid"
                    rejected[reason] += 1
                    shard_rejected[reason] += 1
                    continue
                if row.map_id in seen:
                    rejected["duplicate_map_id"] += 1
                    shard_rejected["duplicate_map_id"] += 1
                    continue
                seen.add(row.map_id)
                rows.append(row)
                shard_accepted += 1
        except Exception as exc:
            raise CorpusScanError(path, shard_scanned, exc) from exc
        files.append({
            "path": str(path), "scanned": shard_scanned, "accepted": shard_accepted,
            "rejected": dict(sorted(shard_rejected.items())),
        })
    return rows, {
        "files": files, "files_count": len(files), "scanned": scanned, "accepted": len(rows),
        "rejected": dict(sorted(rejected.items())), "dedupe_key": "map_id", "patch": PATCH,
        "patch_rule": f"startDateTime >= {PATCH_START}",
    }


def _lead(row: PhaseRow, index: int) -> float | None:
    if index < 0 or index >= len(row.leads):
        return None
    value = float(row.leads[index])
    return value if math.isfinite(value) else None


def _alchemist_group(row: PhaseRow, dominator_radiant: bool) -> str:
    hero_id = int(production_stats.ALCHEMIST_HERO_ID)
    radiant_has = hero_id in row.hero_ids[:5]
    dire_has = hero_id in row.hero_ids[5:]
    leading_has = radiant_has if dominator_radiant else dire_has
    trailing_has = dire_has if dominator_radiant else radiant_has
    if leading_has:
        return "alchemist_leading"
    if trailing_has:
        return "alchemist_trailing"
    return "no_alchemist"


def early_label(row: PhaseRow, remove_gate: bool = False) -> bool | None:
    """Radiant-side Early target, exactly preserving production except lead[9]."""
    if row.duration <= int(production_stats.EARLY_FAST_FINISH_MAX_MINUTES):
        return row.radiant_win
    if not remove_gate:
        gate = _lead(row, int(production_stats.EARLY_GATE_INDEX))
        if gate is None or abs(gate) > float(production_stats.EARLY_GATE_MAX_ABS_LEAD):
            return None
    if row.duration < int(production_stats.EARLY_LEAD_WINDOW[0]):
        return None
    thresholds = production_stats._load_early_dominator_thresholds()
    start, end = production_stats.EARLY_LEAD_WINDOW
    for minute in range(int(start), int(end) + 1):
        lead = _lead(row, minute - 1)
        if lead is None or lead == 0:
            continue
        radiant_dominator = lead > 0
        group = _alchemist_group(row, radiant_dominator)
        by_minute = thresholds.get(group) or thresholds.get("no_alchemist") or {}
        threshold = by_minute.get(int(minute))
        if threshold is None:
            earlier = [value for value in by_minute if value <= minute]
            later = [value for value in by_minute if value >= minute]
            if earlier:
                threshold = by_minute[max(earlier)]
            elif later:
                threshold = by_minute[min(later)]
        if threshold is not None and abs(lead) >= float(threshold):
            return radiant_dominator
    return None


def late_label(row: PhaseRow, all_duration_34: bool = False) -> bool | None:
    if row.duration < int(production_stats.LATE_MIN_DURATION):
        return None
    if all_duration_34:
        return row.radiant_win
    max_duration = production_stats.LATE_MAX_DURATION
    if max_duration is not None and row.duration > int(max_duration):
        return None
    thresholds = production_stats._load_late_wr60_thresholds()
    for minute, threshold in sorted(thresholds.items()):
        if int(minute) < int(production_stats.LATE_WR60_START_MINUTE):
            continue
        lead = _lead(row, int(minute) - 1)
        if lead is not None and abs(lead) <= float(threshold):
            return row.radiant_win
    return None


def build_table(rows: Iterable[PhaseRow], labeler) -> dict[tuple[int, int], tuple[int, int]]:
    counts: dict[tuple[int, int], list[int]] = defaultdict(lambda: [0, 0])
    for row in rows:
        label = labeler(row)
        if label is None:
            continue
        for offset, hero_id in enumerate(row.hero_ids):
            position = offset % 5 + 1
            side_won = label if offset < 5 else not label
            cell = counts[(hero_id, position)]
            cell[0] += int(side_won)
            cell[1] += 1
    return {key: (value[0], value[1]) for key, value in counts.items()}


def score(
    row: PhaseRow,
    target_radiant: bool,
    table: Mapping[tuple[int, int], tuple[int, int]],
    weights: Mapping[int, float],
) -> Prediction:
    rates: list[float] = []
    for offset, hero_id in enumerate(row.hero_ids):
        wins, games = table.get((hero_id, offset % 5 + 1), (0, 0))
        if games < MIN_CELL_GAMES:
            return Prediction(False, None, None, None, None)
        rates.append(wins / games)
    denominator = sum(weights.values())
    radiant = sum(weights[position] * rates[position - 1] for position in weights) / denominator
    dire = sum(weights[position] * rates[5 + position - 1] for position in weights) / denominator
    raw = (radiant - dire) * 100.0
    index = int(round(raw))
    if index == 0:
        return Prediction(True, raw, index, None, None)
    selected_radiant = index > 0
    selected_win = target_radiant if selected_radiant else not target_radiant
    return Prediction(True, raw, index, selected_radiant, selected_win)


def _summary(predictions: Sequence[Prediction], total: int) -> dict[str, Any]:
    covered = [value for value in predictions if value.covered]
    selected = [value for value in covered if value.index != 0]
    wins = sum(bool(value.selected_win) for value in selected)
    low, high = wilson_interval(wins, len(selected))
    radiant = sum(value.selected_radiant is True for value in selected)
    return {
        "pool_n": total, "coverage_n": len(covered),
        "coverage": len(covered) / total if total else 0.0,
        "zero_index_abstain_n": sum(value.index == 0 for value in covered),
        "nonzero_n": len(selected), "wins": wins, "losses": len(selected) - wins,
        "win_rate": wins / len(selected) if selected else None,
        "wilson95": {"low": low, "high": high},
        "selected_radiant_n": radiant, "selected_dire_n": len(selected) - radiant,
    }


def _variant_report(predictions: Sequence[Prediction], total: int) -> dict[str, Any]:
    return {
        "summary": _summary(predictions, total),
        "raw_abs_1pp_buckets": raw_buckets(predictions),
        "rounded_abs_index_buckets": rounded_buckets(predictions),
    }


def _phase_paired_statistics(current: Sequence[Prediction], alternative: Sequence[Prediction]) -> dict[str, Any]:
    """Rename the generic helper's no-gate fields for either phase alternative."""
    result = paired_statistics(current, alternative)
    result["alternative_wr"] = result.pop("no_gate_wr")
    result["delta_wr_alternative_minus_current"] = result.pop("delta_wr_no_gate_minus_current")
    discordant = result["discordant"]
    discordant["current_win_alternative_loss"] = discordant.pop("current_win_no_gate_loss")
    discordant["current_loss_alternative_win"] = discordant.pop("current_loss_no_gate_win")
    result["abs_index_changes"]["mean_alternative_minus_current"] = result["abs_index_changes"].pop(
        "mean_no_gate_minus_current"
    )
    return result


def _phase_evaluation(
    test: Sequence[PhaseRow],
    tables: Mapping[str, Mapping[tuple[int, int], tuple[int, int]]],
    variants: tuple[str, str],
    primary_labeler,
    current_labeler,
    weights: Mapping[int, float],
) -> tuple[dict[str, Any], dict[int, dict[str, Prediction]]]:
    pool = [(index, row, primary_labeler(row)) for index, row in enumerate(test)]
    pool = [(index, row, label) for index, row, label in pool if label is not None]
    predictions: dict[str, list[Prediction]] = {variant: [] for variant in variants}
    by_test_index: dict[int, dict[str, Prediction]] = {}
    for index, row, label in pool:
        by_test_index[index] = {}
        for variant in variants:
            prediction = score(row, bool(label), tables[variant], weights)
            predictions[variant].append(prediction)
            by_test_index[index][variant] = prediction
    current_indices = [offset for offset, (_, row, _) in enumerate(pool) if current_labeler(row) is not None]
    new_indices = [offset for offset, (_, row, _) in enumerate(pool) if current_labeler(row) is None]
    diagnostics = {}
    for name, indices in (("current_eligible", current_indices), ("newly_added", new_indices)):
        diagnostics[name] = {
            "n": len(indices),
            "variants": {
                variant: _summary([predictions[variant][index] for index in indices], len(indices))
                for variant in variants
            },
        }
    # Explicitly descriptive map-winner universe across every held-out draft.
    diagnostics["all_draft_mapwinner"] = {
        "n": len(test),
        "variants": {
            variant: _summary([score(row, row.radiant_win, tables[variant], weights) for row in test], len(test))
            for variant in variants
        },
        "selection_basis": False,
    }
    return {
        "primary_pool_n": len(pool),
        "variants": {variant: _variant_report(predictions[variant], len(pool)) for variant in variants},
        "paired_common_nonzero": _phase_paired_statistics(predictions[variants[0]], predictions[variants[1]]),
        "diagnostics": diagnostics,
    }, by_test_index


def _write_predictions(path: Path, rows: Sequence[PhaseRow], phase_predictions: Mapping[str, Mapping[int, Mapping[str, Prediction]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    variants = ("early_current", "early_no10", "late_current", "late_all34")
    with temporary.open("wb") as raw_handle:
        with gzip.GzipFile(filename="", fileobj=raw_handle, mode="wb", mtime=0) as gzip_handle:
            with io.TextIOWrapper(gzip_handle, encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                header = ["map_id", "startDateTime", "didRadiantWin", "duration"]
                for variant in variants:
                    header.extend((f"{variant}_covered", f"{variant}_raw", f"{variant}_index", f"{variant}_selected_win"))
                writer.writerow(header)
                for index, row in enumerate(rows):
                    output: list[Any] = [row.map_id, row.start_time, int(row.radiant_win), row.duration]
                    for variant in variants:
                        phase = "early" if variant.startswith("early") else "late"
                        prediction = phase_predictions.get(phase, {}).get(index, {}).get(variant)
                        output.extend((
                            int(prediction.covered) if prediction else None,
                            prediction.raw_diff_pp if prediction else None,
                            prediction.index if prediction else None,
                            prediction.selected_win if prediction else None,
                        ))
                    writer.writerow(output)
    os.replace(temporary, path)


def _markdown(report: Mapping[str, Any]) -> str:
    lines = ["# 7.41d Solo phase-filter experiment", ""]
    for phase in ("early", "late"):
        lines.extend([f"## {phase}", ""])
        for variant, result in report["phases"][phase]["variants"].items():
            summary = result["summary"]
            wr = f"{summary['win_rate']:.2%}" if summary["win_rate"] is not None else "n/a"
            lines.append(
                f"- {variant}: coverage {summary['coverage_n']:,}/{summary['pool_n']:,} "
                f"({summary['coverage']:.2%}), nonzero {summary['nonzero_n']:,}, WR {wr}."
            )
        paired = report["phases"][phase]["paired_common_nonzero"]
        delta = paired["delta_wr_alternative_minus_current"]
        delta_text = f"{delta:+.2%}" if delta is not None else "n/a"
        lines.extend(["", f"Paired N {paired['n']:,}; alternative-current delta {delta_text}; "
                      f"McNemar p {paired['mcnemar_exact_binom_p']:.6g}.", ""])
    lines.append("Diagnostics are descriptive only and were not used for selection.\n")
    return "\n".join(lines)


def run_analysis(paths: Sequence[Path], output_dir: Path) -> dict[str, Any]:
    rows, audit = scan_corpus(paths)  # Fail closed before any output mutation.
    train, test = chronological_group_split(rows)
    labelers = {
        "early_current": lambda row: early_label(row, False),
        "early_no10": lambda row: early_label(row, True),
        "late_current": lambda row: late_label(row, False),
        "late_all34": lambda row: late_label(row, True),
    }
    tables = {variant: build_table(train, labeler) for variant, labeler in labelers.items()}
    early, early_predictions = _phase_evaluation(
        test, tables, ("early_current", "early_no10"), labelers["early_no10"],
        labelers["early_current"], EARLY_WEIGHTS,
    )
    late, late_predictions = _phase_evaluation(
        test, tables, ("late_current", "late_all34"), labelers["late_all34"],
        labelers["late_current"], LATE_WEIGHTS,
    )
    report = {
        "schema": "solo_phase_filters.v1",
        "protocol": {
            "patch": PATCH, "patch_rule": f"startDateTime >= {PATCH_START}",
            "split": "group-aware chronological nearest 80/20",
            "min_cell_games": MIN_CELL_GAMES,
            "early_primary_pool": "heldout rows labelable by early_no10 mixed target",
            "late_primary_pool": "heldout rows duration>=34 with map-winner target",
            "rounding": "int(round(raw_diff_pp))",
            "early_weights": {f"pos{key}": value for key, value in EARLY_WEIGHTS.items()},
            "late_weights": {f"pos{key}": value for key, value in LATE_WEIGHTS.items()},
        },
        "corpus": {key: value for key, value in audit.items() if key != "files"},
        "split": {"train_n": len(train), "test_n": len(test),
                  "train_time_max": train[-1].start_time, "test_time_min": test[0].start_time},
        "training": {
            variant: {
                "included_matches": sum(labelers[variant](row) is not None for row in train),
                "cells": len(table),
                "eligible_cells_ge_50": sum(games >= MIN_CELL_GAMES for _, games in table.values()),
            }
            for variant, table in tables.items()
        },
        "phases": {"early": early, "late": late},
    }
    _atomic_json(output_dir / "corpus_audit.json", audit)
    _write_predictions(output_dir / "predictions.csv.gz", test, {"early": early_predictions, "late": late_predictions})
    _atomic_json(output_dir / "report.json", report)
    _atomic_text(output_dir / "report.md", _markdown(report))
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--glob", default="7.41d_part*.json")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    paths = sorted(args.input_dir.glob(args.glob))
    if not paths:
        raise SystemExit(f"No shards matched {args.input_dir / args.glob}")
    report = run_analysis(paths, args.output_dir.resolve())
    print(json.dumps({"status": "ok", "accepted": report["corpus"]["accepted"],
                      "report": str(args.output_dir.resolve() / "report.json")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
