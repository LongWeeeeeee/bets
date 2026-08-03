#!/usr/bin/env python3
"""Evaluate the 7.41d post-lane solo minute-10 training gate.

Two hero x exact-position lookup tables are trained on the same chronological
80% public-match discovery window. ``current`` uses the production post-lane
gate (at least 20 net-worth samples and finite ``lead[9]`` within +/-2000).
``no_gate`` keeps the same duration requirement but does not inspect lead[9].
Both frozen tables score the same structurally valid final 20% pregame drafts.

This offline command does not import or modify the live runtime.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import math
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import numpy as np
from scipy.stats import binomtest

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_DIR = ROOT_DIR / "bets_data" / "analise_pub_matches" / "json_parts_split_from_object"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "runtime" / "post_lane_solo_gate"
PATCH = "7.41d"
PATCH_START = 1_780_531_200
MIN_CELL_GAMES = 50
GATE_ABS_LEAD = 2_000.0
POSITION_WEIGHTS = {1: 2.4, 2: 2.2, 3: 1.4, 4: 1.2, 5: 0.6}
WILSON_Z = 1.959963984540054


class CorpusScanError(RuntimeError):
    def __init__(self, path: Path, scanned_in_shard: int, cause: BaseException):
        self.path = Path(path)
        self.scanned_in_shard = int(scanned_in_shard)
        self.cause = cause
        super().__init__(
            f"Failed closed while reading {self.path} after {self.scanned_in_shard} rows: "
            f"{type(cause).__name__}: {cause}"
        )


@dataclass(frozen=True)
class MatchRow:
    map_id: int
    start_time: int
    radiant_win: bool
    # Radiant pos1..5, then Dire pos1..5.
    hero_ids: tuple[int, ...]
    lead_count: int
    lead_at_10: Any


@dataclass(frozen=True)
class Prediction:
    covered: bool
    raw_diff_pp: float | None
    index: int | None
    selected_radiant: bool | None
    selected_win: bool | None


def is_pro_match(raw: Mapping[str, Any]) -> bool:
    """Mirror production: leagueId, or both nested team ids, means pro."""
    if raw.get("leagueId"):
        return True
    radiant_team = raw.get("radiantTeam")
    dire_team = raw.get("direTeam")
    return bool(
        isinstance(radiant_team, Mapping)
        and isinstance(dire_team, Mapping)
        and radiant_team.get("id")
        and dire_team.get("id")
    )


def canonicalize_match(raw: Any, object_key: Any = None) -> tuple[MatchRow | None, str | None]:
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
    heroes_seen: set[int] = set()
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
        if position not in POSITION_WEIGHTS:
            return None, "invalid_position"
        if position in sides[side]:
            return None, "duplicate_position"
        hero_raw = player.get("heroId")
        if isinstance(hero_raw, bool):
            return None, "invalid_hero_id"
        try:
            hero_id = int(hero_raw)
        except (TypeError, ValueError, OverflowError):
            return None, "invalid_hero_id"
        if hero_id <= 0:
            return None, "invalid_hero_id"
        if hero_id in heroes_seen:
            return None, "duplicate_hero"
        heroes_seen.add(hero_id)
        sides[side][position] = hero_id
    expected = set(POSITION_WEIGHTS)
    if set(sides[True]) != expected or set(sides[False]) != expected:
        return None, "incomplete_positions"

    raw_leads = raw.get("radiantNetworthLeads")
    lead_count = len(raw_leads) if isinstance(raw_leads, list) else 0
    lead_at_10 = raw_leads[9] if isinstance(raw_leads, list) and len(raw_leads) > 9 else None
    hero_ids = tuple(sides[side][position] for side in (True, False) for position in POSITION_WEIGHTS)
    return MatchRow(map_id, start_time, radiant_win, hero_ids, lead_count, lead_at_10), None


def iter_json_object(path: Path) -> Iterator[tuple[str, Any]]:
    try:
        import ijson  # type: ignore
    except ImportError:
        ijson = None
    if ijson is not None:
        with path.open("rb") as handle:
            yield from ijson.kvitems(handle, "")
        return
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected top-level object in {path}")
    yield from payload.items()


def scan_corpus(paths: Iterable[Path]) -> tuple[list[MatchRow], dict[str, Any]]:
    """Scan all shards fail-closed; partial corpora are never returned."""
    rows: list[MatchRow] = []
    seen_ids: set[int] = set()
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
                if row.map_id in seen_ids:
                    rejected["duplicate_map_id"] += 1
                    shard_rejected["duplicate_map_id"] += 1
                    continue
                seen_ids.add(row.map_id)
                rows.append(row)
                shard_accepted += 1
        except Exception as exc:
            raise CorpusScanError(path, shard_scanned, exc) from exc
        files.append({
            "path": str(path), "scanned": shard_scanned, "accepted": shard_accepted,
            "rejected": dict(sorted(shard_rejected.items())),
        })
    audit = {
        "files": files,
        "files_count": len(files),
        "scanned": scanned,
        "accepted": len(rows),
        "rejected": dict(sorted(rejected.items())),
        "dedupe_key": "map_id",
        "patch": PATCH,
        "patch_rule": f"startDateTime >= {PATCH_START}",
        "pro_rule": "truthy leagueId OR both radiantTeam.id and direTeam.id",
    }
    return rows, audit


def chronological_group_split(rows: Sequence[MatchRow], train_fraction: float = 0.80) -> tuple[list[MatchRow], list[MatchRow]]:
    ordered = sorted(rows, key=lambda row: (row.start_time, row.map_id))
    unique_times = sorted({row.start_time for row in ordered})
    if len(unique_times) < 2:
        raise ValueError("Need at least two unique timestamp groups for chronological 80/20 split")
    counts = Counter(row.start_time for row in ordered)
    cumulative = 0
    candidates: list[tuple[int, int]] = []
    for timestamp in unique_times[:-1]:
        cumulative += counts[timestamp]
        candidates.append((cumulative, timestamp))
    target = len(ordered) * train_fraction
    cut, boundary_time = min(candidates, key=lambda item: (abs(item[0] - target), item[0]))
    train = ordered[:cut]
    test = ordered[cut:]
    if not train or not test or train[-1].start_time >= test[0].start_time:
        raise AssertionError(f"Invalid group split at timestamp {boundary_time}")
    return train, test


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def diagnostic_stratum(row: MatchRow) -> str:
    if row.lead_count < 20:
        return "short_or_missing"
    gate_lead = _finite_number(row.lead_at_10)
    if gate_lead is None:
        return "short_or_missing"
    return "close10" if abs(gate_lead) <= GATE_ABS_LEAD else "blowout10"


def included_in_training(row: MatchRow, variant: str) -> bool:
    if row.lead_count < 20:
        return False
    if variant == "no_gate":
        return True
    if variant != "current":
        raise ValueError(f"Unknown variant: {variant}")
    gate_lead = _finite_number(row.lead_at_10)
    return gate_lead is not None and abs(gate_lead) <= GATE_ABS_LEAD


def build_solo_table(rows: Iterable[MatchRow], variant: str) -> dict[tuple[int, int], tuple[int, int]]:
    counts: dict[tuple[int, int], list[int]] = defaultdict(lambda: [0, 0])
    for row in rows:
        if not included_in_training(row, variant):
            continue
        for offset, hero_id in enumerate(row.hero_ids):
            position = offset % 5 + 1
            side_radiant = offset < 5
            won = row.radiant_win if side_radiant else not row.radiant_win
            cell = counts[(hero_id, position)]
            cell[0] += int(won)
            cell[1] += 1
    return {key: (value[0], value[1]) for key, value in counts.items()}


def score_draft(
    row: MatchRow,
    table: Mapping[tuple[int, int], tuple[int, int]],
    min_games: int = MIN_CELL_GAMES,
) -> Prediction:
    win_rates: list[float] = []
    for offset, hero_id in enumerate(row.hero_ids):
        position = offset % 5 + 1
        wins, games = table.get((hero_id, position), (0, 0))
        if games < min_games:
            return Prediction(False, None, None, None, None)
        win_rates.append(wins / games)
    denominator = sum(POSITION_WEIGHTS.values())
    radiant = sum(POSITION_WEIGHTS[position] * win_rates[position - 1] for position in POSITION_WEIGHTS) / denominator
    dire = sum(POSITION_WEIGHTS[position] * win_rates[5 + position - 1] for position in POSITION_WEIGHTS) / denominator
    raw_diff_pp = (radiant - dire) * 100.0
    index = int(round(raw_diff_pp))
    if index == 0:
        return Prediction(True, raw_diff_pp, index, None, None)
    selected_radiant = index > 0
    selected_win = row.radiant_win if selected_radiant else not row.radiant_win
    return Prediction(True, raw_diff_pp, index, selected_radiant, selected_win)


def raw_abs_bucket(value: float) -> int:
    value = max(0.0, abs(float(value)))
    return max(1, int(math.ceil(value - 1e-12)))


def wilson_interval(wins: int, n: int) -> tuple[float, float]:
    if n <= 0:
        return 0.0, 1.0
    p = wins / n
    denominator = 1.0 + WILSON_Z**2 / n
    center = (p + WILSON_Z**2 / (2.0 * n)) / denominator
    half = WILSON_Z * math.sqrt(p * (1.0 - p) / n + WILSON_Z**2 / (4.0 * n * n)) / denominator
    return max(0.0, center - half), min(1.0, center + half)


def _outcome_summary(predictions: Sequence[Prediction], total_drafts: int) -> dict[str, Any]:
    covered = [prediction for prediction in predictions if prediction.covered]
    selected = [prediction for prediction in covered if prediction.index != 0]
    wins = sum(bool(prediction.selected_win) for prediction in selected)
    n = len(selected)
    low, high = wilson_interval(wins, n)
    radiant_n = sum(prediction.selected_radiant is True for prediction in selected)
    return {
        "drafts_n": total_drafts,
        "coverage_n": len(covered),
        "coverage": len(covered) / total_drafts if total_drafts else 0.0,
        "zero_index_abstain_n": sum(prediction.index == 0 for prediction in covered),
        "nonzero_n": n,
        "wins": wins,
        "losses": n - wins,
        "win_rate": wins / n if n else None,
        "wilson95": {"low": low, "high": high},
        "selected_radiant_n": radiant_n,
        "selected_dire_n": n - radiant_n,
        "selected_radiant_share": radiant_n / n if n else None,
    }


def _bucket_summary(items: Sequence[Prediction], label: str) -> dict[str, Any]:
    selected = [prediction for prediction in items if prediction.selected_win is not None]
    wins = sum(bool(prediction.selected_win) for prediction in selected)
    low, high = wilson_interval(wins, len(selected))
    radiant_n = sum(prediction.selected_radiant is True for prediction in selected)
    return {
        "bucket": label,
        "n": len(items),
        "abstain_n": len(items) - len(selected),
        "selected_n": len(selected),
        "wins": wins,
        "losses": len(selected) - wins,
        "win_rate": wins / len(selected) if selected else None,
        "wilson95": {"low": low, "high": high},
        "selected_radiant_n": radiant_n,
        "selected_dire_n": len(selected) - radiant_n,
    }


def raw_buckets(predictions: Sequence[Prediction]) -> list[dict[str, Any]]:
    grouped: dict[int, list[Prediction]] = defaultdict(list)
    for prediction in predictions:
        if prediction.covered and prediction.raw_diff_pp is not None:
            grouped[raw_abs_bucket(prediction.raw_diff_pp)].append(prediction)
    output = []
    for bucket in sorted(grouped):
        label = "0.00-1.00" if bucket == 1 else f"{bucket - 1}.01-{bucket}.00"
        row = _bucket_summary(grouped[bucket], label)
        row["bounds"] = {"lower_exclusive": None if bucket == 1 else bucket - 1, "upper_inclusive": bucket}
        output.append(row)
    return output


def rounded_buckets(predictions: Sequence[Prediction]) -> list[dict[str, Any]]:
    grouped: dict[int, list[Prediction]] = defaultdict(list)
    for prediction in predictions:
        if prediction.covered and prediction.index is not None:
            grouped[abs(prediction.index)].append(prediction)
    output = []
    for bucket in sorted(grouped):
        row = _bucket_summary(grouped[bucket], str(bucket))
        row["abs_index"] = bucket
        output.append(row)
    return output


def evaluate_variant(rows: Sequence[MatchRow], predictions: Sequence[Prediction]) -> dict[str, Any]:
    return {
        "summary": _outcome_summary(predictions, len(rows)),
        "raw_abs_1pp_buckets": raw_buckets(predictions),
        "rounded_abs_index_buckets": rounded_buckets(predictions),
    }


def paired_statistics(current: Sequence[Prediction], no_gate: Sequence[Prediction]) -> dict[str, Any]:
    paired = [
        (left, right)
        for left, right in zip(current, no_gate)
        if left.covered and right.covered and left.index != 0 and right.index != 0
    ]
    n = len(paired)
    current_wins = np.asarray([int(bool(left.selected_win)) for left, _ in paired], dtype=np.float64)
    no_gate_wins = np.asarray([int(bool(right.selected_win)) for _, right in paired], dtype=np.float64)
    differences = no_gate_wins - current_wins
    delta = float(differences.mean()) if n else None
    if n > 1:
        standard_error = float(differences.std(ddof=1) / math.sqrt(n))
        ci = (max(-1.0, delta - 1.96 * standard_error), min(1.0, delta + 1.96 * standard_error))
    elif n == 1:
        ci = (delta, delta)
    else:
        ci = (None, None)
    current_win_no_gate_loss = sum(left.selected_win is True and right.selected_win is False for left, right in paired)
    current_loss_no_gate_win = sum(left.selected_win is False and right.selected_win is True for left, right in paired)
    discordant = current_win_no_gate_loss + current_loss_no_gate_win
    p_value = float(
        binomtest(current_loss_no_gate_win, discordant, p=0.5, alternative="two-sided").pvalue
    ) if discordant else 1.0
    abs_changes = [abs(int(right.index)) - abs(int(left.index)) for left, right in paired]
    return {
        "n": n,
        "current_wr": float(current_wins.mean()) if n else None,
        "no_gate_wr": float(no_gate_wins.mean()) if n else None,
        "delta_wr_no_gate_minus_current": delta,
        "paired_normal_95ci": {"low": ci[0], "high": ci[1]},
        "discordant": {
            "current_win_no_gate_loss": current_win_no_gate_loss,
            "current_loss_no_gate_win": current_loss_no_gate_win,
            "total": discordant,
        },
        "mcnemar_exact_binom_p": p_value,
        "sign_changes_n": sum((left.index > 0) != (right.index > 0) for left, right in paired),
        "abs_index_changes": {
            "changed_n": sum(change != 0 for change in abs_changes),
            "mean_no_gate_minus_current": float(np.mean(abs_changes)) if abs_changes else None,
            "mean_absolute_change": float(np.mean(np.abs(abs_changes))) if abs_changes else None,
        },
    }


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _write_predictions(
    path: Path,
    rows: Sequence[MatchRow],
    current: Sequence[Prediction],
    no_gate: Sequence[Prediction],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as raw_handle:
        with gzip.GzipFile(filename="", fileobj=raw_handle, mode="wb", mtime=0) as gzip_handle:
            with io.TextIOWrapper(gzip_handle, encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow((
                    "map_id", "startDateTime", "didRadiantWin", "diagnostic_stratum",
                    "current_covered", "current_raw_diff_pp", "current_index", "current_selected_win",
                    "no_gate_covered", "no_gate_raw_diff_pp", "no_gate_index", "no_gate_selected_win",
                ))
                for row, left, right in zip(rows, current, no_gate):
                    writer.writerow((
                        row.map_id, row.start_time, int(row.radiant_win), diagnostic_stratum(row),
                        int(left.covered), left.raw_diff_pp, left.index, left.selected_win,
                        int(right.covered), right.raw_diff_pp, right.index, right.selected_win,
                    ))
    os.replace(temporary, path)


def _markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# 7.41d post-lane solo gate experiment", "",
        "Frozen chronological 80/20 evaluation. Both variants score the same held-out pregame drafts.", "",
    ]
    for variant in ("current", "no_gate"):
        summary = report["variants"][variant]["summary"]
        lines.extend([
            f"## {variant}", "",
            f"Coverage: {summary['coverage_n']:,}/{summary['drafts_n']:,} ({summary['coverage']:.2%}); "
            f"nonzero: {summary['nonzero_n']:,}; WR: "
            f"{summary['win_rate']:.2%}." if summary["win_rate"] is not None else "WR: n/a.", "",
            "| raw abs index | N | selected | wins | WR |", "|---:|---:|---:|---:|---:|",
        ])
        for row in report["variants"][variant]["raw_abs_1pp_buckets"]:
            wr = f"{row['win_rate']:.2%}" if row["win_rate"] is not None else "n/a"
            lines.append(f"| {row['bucket']} | {row['n']:,} | {row['selected_n']:,} | {row['wins']:,} | {wr} |")
        lines.append("")
    paired = report["paired_common_nonzero"]
    delta = paired["delta_wr_no_gate_minus_current"]
    lines.extend([
        "## Paired common nonzero", "",
        f"N: {paired['n']:,}; delta WR (no_gate-current): {delta:+.2%}; "
        f"exact McNemar p: {paired['mcnemar_exact_binom_p']:.6g}." if delta is not None else "N: 0.", "",
        "Diagnostic strata are descriptive only and were not used for selection.", "",
    ])
    return "\n".join(lines)


def run_analysis(paths: Sequence[Path], output_dir: Path) -> dict[str, Any]:
    # Fail-closed scan happens before output_dir is created or any output changes.
    rows, audit = scan_corpus(paths)
    train, test = chronological_group_split(rows)
    tables = {variant: build_solo_table(train, variant) for variant in ("current", "no_gate")}
    predictions = {
        variant: [score_draft(row, tables[variant]) for row in test]
        for variant in ("current", "no_gate")
    }
    variants = {variant: evaluate_variant(test, predictions[variant]) for variant in predictions}
    diagnostics: dict[str, Any] = {}
    for stratum in ("close10", "blowout10", "short_or_missing"):
        indices = [index for index, row in enumerate(test) if diagnostic_stratum(row) == stratum]
        diagnostics[stratum] = {
            "n": len(indices),
            "variants": {
                variant: _outcome_summary([predictions[variant][index] for index in indices], len(indices))
                for variant in predictions
            },
        }
    report = {
        "schema": "post_lane_solo_gate_experiment.v1",
        "protocol": {
            "patch": PATCH,
            "patch_rule": f"startDateTime >= {PATCH_START}",
            "split": "group-aware chronological nearest 80/20; equal timestamps never split",
            "train_current": "len(radiantNetworthLeads)>=20 and finite abs(lead[9])<=2000",
            "train_no_gate": "len(radiantNetworthLeads)>=20; lead[9] ignored",
            "test_universe": "all structurally valid held-out public drafts",
            "min_cell_games": MIN_CELL_GAMES,
            "position_weights": {f"pos{key}": value for key, value in POSITION_WEIGHTS.items()},
            "index": "int(round((weighted_radiant_wr-weighted_dire_wr)*100))",
        },
        "corpus": {key: value for key, value in audit.items() if key != "files"},
        "split": {
            "train_n": len(train), "test_n": len(test),
            "train_time_max": train[-1].start_time, "test_time_min": test[0].start_time,
        },
        "training": {
            variant: {
                "included_matches": sum(included_in_training(row, variant) for row in train),
                "cells": len(tables[variant]),
                "eligible_cells_ge_50": sum(games >= MIN_CELL_GAMES for _, games in tables[variant].values()),
            }
            for variant in tables
        },
        "variants": variants,
        "paired_common_nonzero": paired_statistics(predictions["current"], predictions["no_gate"]),
        "diagnostic_strata": diagnostics,
    }
    _atomic_json(output_dir / "corpus_audit.json", audit)
    _write_predictions(output_dir / "predictions.csv.gz", test, predictions["current"], predictions["no_gate"])
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
    print(json.dumps({
        "status": "ok", "accepted": report["corpus"]["accepted"],
        "report": str(args.output_dir.resolve() / "report.json"),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
