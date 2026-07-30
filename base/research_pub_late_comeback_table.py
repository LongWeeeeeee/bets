from __future__ import annotations

import argparse
import copy
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = REPO_ROOT / "base"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from functions import format_output_dict  # noqa: E402
from cyberscore_try import _recommend_odds_for_block  # noqa: E402


DEFAULT_INPUT = REPO_ROOT / "pro_heroes_data" / "pub.json"
DEFAULT_OUTPUT_JSON = REPO_ROOT / "base" / "pub_late_star_comeback_table.json"
DEFAULT_OUTPUT_CSV = REPO_ROOT / "base" / "pub_late_star_comeback_table.csv"


def _iter_matches_from_payload(payload: Any) -> Iterator[Tuple[str, dict]]:
    if isinstance(payload, dict):
        for match_id, match in payload.items():
            if isinstance(match, dict):
                yield str(match_id), match
        return
    if isinstance(payload, list):
        for index, match in enumerate(payload):
            if not isinstance(match, dict):
                continue
            match_id = match.get("id") or match.get("match_id") or index
            yield str(match_id), match


def _target_diff_from_radiant_lead(lead_value: Any, target_side: str) -> Optional[float]:
    try:
        lead = float(lead_value)
    except (TypeError, ValueError):
        return None
    if target_side == "radiant":
        return lead
    if target_side == "dire":
        return -lead
    return None


def _star_block_sign(block: dict) -> Optional[int]:
    signs = set()
    for raw_value in block.values():
        if not isinstance(raw_value, str) or not raw_value.strip().endswith("*"):
            continue
        value_text = raw_value.strip()[:-1]
        try:
            value = float(value_text)
        except ValueError:
            continue
        if value == 0:
            continue
        signs.add(1 if value > 0 else -1)
    if len(signs) != 1:
        return None
    return next(iter(signs))


def _collect_table_rows(
    matches_payload: Any,
    min_count: int,
) -> Tuple[dict, list[dict], list[dict]]:
    bucket_values: dict[int, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    detailed_matches: list[dict] = []

    total_matches = 0
    late_signal_matches = 0
    late_signal_winner_matches = 0
    late_signal_comeback_winner_matches = 0

    for match_id, match in _iter_matches_from_payload(matches_payload):
        total_matches += 1

        late_raw = copy.deepcopy(match.get("late_output") or match.get("mid_output") or {})
        if not isinstance(late_raw, dict) or not late_raw:
            continue

        payload = {"early_output": {}, "mid_output": late_raw}
        has_star_signal = format_output_dict(payload)
        if not has_star_signal:
            continue

        formatted_late = payload["mid_output"]
        late_rec = _recommend_odds_for_block(formatted_late, "late")
        late_sign = _star_block_sign(formatted_late)
        if late_rec is None or late_sign is None:
            continue

        late_signal_matches += 1
        target_side = "radiant" if late_sign > 0 else "dire"

        did_radiant_win = match.get("didRadiantWin")
        if did_radiant_win is None:
            continue
        target_won = bool(did_radiant_win) if target_side == "radiant" else (not bool(did_radiant_win))
        if not target_won:
            continue

        late_signal_winner_matches += 1
        leads = match.get("radiantNetworthLeads") or []
        if not isinstance(leads, list) or len(leads) <= 20:
            continue

        trailing_minutes: list[dict] = []
        for minute in range(20, len(leads)):
            target_diff = _target_diff_from_radiant_lead(leads[minute], target_side)
            if target_diff is None:
                continue
            if target_diff < 0:
                trailing_minutes.append(
                    {
                        "minute": minute,
                        "target_networth_diff": float(target_diff),
                    }
                )

        if not trailing_minutes:
            continue

        late_signal_comeback_winner_matches += 1
        wr_level = int(late_rec["level"])
        for row in trailing_minutes:
            bucket_values[wr_level][row["minute"]].append(row["target_networth_diff"])

        detailed_matches.append(
            {
                "match_id": match_id,
                "didRadiantWin": bool(did_radiant_win),
                "target_side": target_side,
                "wr_level": wr_level,
                "late_wr_pct": float(late_rec["wr_pct"]),
                "late_block": formatted_late,
                "trailing_minutes": trailing_minutes,
            }
        )

    table_rows: list[dict] = []
    for wr_level in sorted(bucket_values):
        for minute in sorted(bucket_values[wr_level]):
            values = bucket_values[wr_level][minute]
            if len(values) < int(min_count):
                continue
            table_rows.append(
                {
                    "wr_level": wr_level,
                    "minute": minute,
                    "count": len(values),
                    "avg_target_networth_diff": round(sum(values) / len(values), 2),
                    "median_target_networth_diff": round(statistics.median(values), 2),
                    "min_target_networth_diff": round(min(values), 2),
                    "max_target_networth_diff": round(max(values), 2),
                }
            )

    summary = {
        "total_matches": total_matches,
        "late_signal_matches": late_signal_matches,
        "late_signal_winner_matches": late_signal_winner_matches,
        "late_signal_comeback_winner_matches": late_signal_comeback_winner_matches,
        "min_count": int(min_count),
        "table_rows": len(table_rows),
    }
    return summary, table_rows, detailed_matches


def _normalize_table_rows(table_rows: list[dict]) -> list[dict]:
    normalized_rows: list[dict] = []
    rows_by_wr: dict[int, list[dict]] = defaultdict(list)
    for row in table_rows:
        rows_by_wr[int(row["wr_level"])].append(dict(row))

    for wr_level in sorted(rows_by_wr):
        wr_rows = sorted(rows_by_wr[wr_level], key=lambda row: int(row["minute"]))
        running_avg_min: Optional[float] = None
        running_median_min: Optional[float] = None
        for row in wr_rows:
            raw_avg = float(row["avg_target_networth_diff"])
            raw_median = float(row["median_target_networth_diff"])
            row["avg_target_networth_diff_raw"] = raw_avg
            row["median_target_networth_diff_raw"] = raw_median

            if running_avg_min is None:
                running_avg_min = raw_avg
            else:
                running_avg_min = min(running_avg_min, raw_avg)
            if running_median_min is None:
                running_median_min = raw_median
            else:
                running_median_min = min(running_median_min, raw_median)

            row["avg_target_networth_diff"] = round(running_avg_min, 2)
            row["median_target_networth_diff"] = round(running_median_min, 2)
            normalized_rows.append(row)

    normalized_rows.sort(key=lambda row: (int(row["wr_level"]), int(row["minute"])))
    return normalized_rows


def _write_csv(path: Path, rows: Iterable[dict]) -> None:
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "wr_level",
        "minute",
        "count",
        "avg_target_networth_diff",
        "avg_target_networth_diff_raw",
        "median_target_networth_diff",
        "median_target_networth_diff_raw",
        "min_target_networth_diff",
        "max_target_networth_diff",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build late-star comeback deficit tables from pub.json using current runtime star logic."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--min-count", type=int, default=6)
    args = parser.parse_args()

    with args.input.open("r", encoding="utf-8") as f:
        matches_payload = json.load(f)

    summary, table_rows, detailed_matches = _collect_table_rows(
        matches_payload=matches_payload,
        min_count=args.min_count,
    )
    table_rows = _normalize_table_rows(table_rows)
    summary["normalization"] = (
        "avg_target_networth_diff and median_target_networth_diff are monotonic normalized by WR level "
        "using running minimum across minutes; *_raw fields preserve original values"
    )

    payload = {
        "source": str(args.input),
        "summary": summary,
        "table_rows": table_rows,
        "detailed_matches": detailed_matches,
    }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(args.output_csv, table_rows)

    print(args.output_json)
    print(args.output_csv)
    print(json.dumps(summary, ensure_ascii=False))
    for row in table_rows[:20]:
        print(json.dumps(row, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
