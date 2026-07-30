from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_JSON = REPO_ROOT / "base" / "pub_late_star_comeback_table.json"
DEFAULT_OUTPUT_JSON = REPO_ROOT / "base" / "pub_late_star_comeback_table_piecewise.json"
DEFAULT_OUTPUT_CSV = REPO_ROOT / "base" / "pub_late_star_comeback_table_piecewise.csv"
DEFAULT_SEGMENTS = ((20, 30), (30, 40), (40, None))


def _segment_label(start: int, end: Optional[int]) -> str:
    if end is None:
        return f"{start}+"
    return f"{start}-{end - 1}"


def _load_rows(path: Path) -> Tuple[dict, List[dict]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    summary = dict(payload.get("summary") or {})
    rows = [dict(row) for row in payload.get("table_rows") or [] if isinstance(row, dict)]
    return summary, rows


def _weighted_geometric_mean(ratios: List[Tuple[float, int]]) -> float:
    total_weight = sum(weight for _, weight in ratios)
    if total_weight <= 0:
        return 1.0
    numerator = sum(math.log(ratio) * weight for ratio, weight in ratios if ratio > 0)
    return math.exp(numerator / total_weight)


def _compute_global_segment_coefficients(
    rows_by_wr: Dict[int, List[dict]],
    value_key: str,
    segments: Tuple[Tuple[int, Optional[int]], ...],
) -> Dict[str, float]:
    coefficients: Dict[str, float] = {}
    for start, end in segments:
        ratios: List[Tuple[float, int]] = []
        for wr_rows in rows_by_wr.values():
            for prev_row, curr_row in zip(wr_rows, wr_rows[1:]):
                prev_minute = int(prev_row["minute"])
                curr_minute = int(curr_row["minute"])
                if curr_minute != prev_minute + 1:
                    continue
                if not (start <= prev_minute and (end is None or prev_minute < end)):
                    continue
                prev_mag = abs(float(prev_row[value_key]))
                curr_mag = abs(float(curr_row[value_key]))
                if prev_mag <= 0 or curr_mag <= prev_mag:
                    continue
                ratios.append((curr_mag / prev_mag, min(int(prev_row["count"]), int(curr_row["count"]))))
        label = _segment_label(start, end)
        coefficients[label] = _weighted_geometric_mean(ratios) if ratios else 1.0
    return coefficients


def _compute_piecewise_coefficients(
    wr_rows: List[dict],
    value_key: str,
    segments: Tuple[Tuple[int, Optional[int]], ...],
    global_coefficients: Dict[str, float],
) -> Dict[str, float]:
    coefficients: Dict[str, float] = {}
    for start, end in segments:
        ratios: List[Tuple[float, int]] = []
        for prev_row, curr_row in zip(wr_rows, wr_rows[1:]):
            prev_minute = int(prev_row["minute"])
            curr_minute = int(curr_row["minute"])
            if curr_minute != prev_minute + 1:
                continue
            if not (start <= prev_minute and (end is None or prev_minute < end)):
                continue
            prev_mag = abs(float(prev_row[value_key]))
            curr_mag = abs(float(curr_row[value_key]))
            if prev_mag <= 0 or curr_mag <= prev_mag:
                continue
            ratios.append((curr_mag / prev_mag, min(int(prev_row["count"]), int(curr_row["count"]))))
        label = _segment_label(start, end)
        coefficient = _weighted_geometric_mean(ratios) if ratios else global_coefficients.get(label, 1.0)
        if coefficient <= 1.0:
            coefficient = max(global_coefficients.get(label, 1.0), 1.0005)
        coefficients[label] = coefficient
    return coefficients


def _compute_cross_wr_coefficients(
    rows: List[dict],
    value_key: str,
    *,
    damping: float = 0.35,
    min_ratio: float = 1.01,
    max_ratio: float = 1.06,
) -> Dict[str, float]:
    rows_by_minute: Dict[int, Dict[int, float]] = defaultdict(dict)
    wr_levels = sorted({int(row["wr_level"]) for row in rows})
    for row in rows:
        rows_by_minute[int(row["minute"])][int(row["wr_level"])] = abs(float(row[value_key]))

    coefficients: Dict[str, float] = {}
    for prev_wr, curr_wr in zip(wr_levels, wr_levels[1:]):
        positive_ratios: List[float] = []
        for minute in sorted(rows_by_minute):
            minute_values = rows_by_minute[minute]
            if prev_wr not in minute_values or curr_wr not in minute_values:
                continue
            prev_value = float(minute_values[prev_wr])
            curr_value = float(minute_values[curr_wr])
            if prev_value <= 0.0 or curr_value <= prev_value:
                continue
            positive_ratios.append(curr_value / prev_value)
        if positive_ratios:
            geometric_mean = math.exp(sum(math.log(value) for value in positive_ratios) / len(positive_ratios))
            ratio = 1.0 + (geometric_mean - 1.0) * damping
        else:
            ratio = min_ratio
        ratio = min(max(ratio, min_ratio), max_ratio)
        coefficients[f"{prev_wr}->{curr_wr}"] = round(ratio, 6)
    return coefficients


def _coefficient_for_minute(
    minute: int,
    coefficients: Dict[str, float],
    segments: Tuple[Tuple[int, Optional[int]], ...],
) -> float:
    for start, end in segments:
        if minute >= start and (end is None or minute < end):
            return float(coefficients[_segment_label(start, end)])
    return 1.0005


def _smooth_series(
    wr_rows: List[dict],
    base_key: str,
    output_key: str,
    coefficients: Dict[str, float],
    segments: Tuple[Tuple[int, Optional[int]], ...],
) -> None:
    previous_value: Optional[float] = None
    for row in wr_rows:
        observed_value = float(row[base_key])
        minute = int(row["minute"])
        if previous_value is None:
            smoothed_value = observed_value
        else:
            coefficient = _coefficient_for_minute(minute - 1, coefficients, segments)
            predicted_value = previous_value * coefficient
            smoothed_value = min(observed_value, predicted_value)
            if round(smoothed_value, 2) >= round(previous_value, 2):
                smoothed_value = previous_value - 1.0
        row[output_key] = round(smoothed_value, 2)
        previous_value = row[output_key]


def _build_piecewise_rows(
    input_rows: List[dict],
    segments: Tuple[Tuple[int, Optional[int]], ...],
) -> Tuple[List[dict], Dict[str, Any]]:
    rows_by_wr: Dict[int, List[dict]] = defaultdict(list)
    for row in input_rows:
        wr_level = int(row["wr_level"])
        rows_by_wr[wr_level].append(dict(row))
    for wr_rows in rows_by_wr.values():
        wr_rows.sort(key=lambda row: int(row["minute"]))

    global_avg_coefficients = _compute_global_segment_coefficients(rows_by_wr, "avg_target_networth_diff", segments)
    global_median_coefficients = _compute_global_segment_coefficients(rows_by_wr, "median_target_networth_diff", segments)

    piecewise_meta: Dict[str, Any] = {
        "segments": [
            {
                "label": _segment_label(start, end),
                "start_minute": start,
                "end_minute_exclusive": end,
            }
            for start, end in segments
        ],
        "global_avg_coefficients": {key: round(value, 6) for key, value in global_avg_coefficients.items()},
        "global_median_coefficients": {key: round(value, 6) for key, value in global_median_coefficients.items()},
        "by_wr": {},
    }

    output_rows: List[dict] = []
    for wr_level in sorted(rows_by_wr):
        wr_rows = rows_by_wr[wr_level]
        avg_coefficients = _compute_piecewise_coefficients(
            wr_rows,
            "avg_target_networth_diff",
            segments,
            global_avg_coefficients,
        )
        median_coefficients = _compute_piecewise_coefficients(
            wr_rows,
            "median_target_networth_diff",
            segments,
            global_median_coefficients,
        )
        piecewise_meta["by_wr"][str(wr_level)] = {
            "avg_coefficients": {key: round(value, 6) for key, value in avg_coefficients.items()},
            "median_coefficients": {key: round(value, 6) for key, value in median_coefficients.items()},
        }
        _smooth_series(
            wr_rows,
            base_key="avg_target_networth_diff",
            output_key="avg_target_networth_diff_piecewise",
            coefficients=avg_coefficients,
            segments=segments,
        )
        _smooth_series(
            wr_rows,
            base_key="median_target_networth_diff",
            output_key="median_target_networth_diff_piecewise",
            coefficients=median_coefficients,
            segments=segments,
        )
        for row in wr_rows:
            row["avg_target_networth_diff_base_monotonic"] = float(row["avg_target_networth_diff"])
            row["median_target_networth_diff_base_monotonic"] = float(row["median_target_networth_diff"])
            row["avg_target_networth_diff_piecewise_time_only"] = float(row["avg_target_networth_diff_piecewise"])
            row["median_target_networth_diff_piecewise_time_only"] = float(row["median_target_networth_diff_piecewise"])
            row["avg_target_networth_diff"] = float(row["avg_target_networth_diff_piecewise"])
            row["median_target_networth_diff"] = float(row["median_target_networth_diff_piecewise"])
            row.pop("avg_target_networth_diff_piecewise", None)
            row.pop("median_target_networth_diff_piecewise", None)
            output_rows.append(row)

    cross_wr_avg_coefficients = _compute_cross_wr_coefficients(
        output_rows,
        "avg_target_networth_diff_piecewise_time_only",
    )
    cross_wr_median_coefficients = _compute_cross_wr_coefficients(
        output_rows,
        "median_target_networth_diff_piecewise_time_only",
    )
    piecewise_meta["cross_wr_avg_coefficients"] = cross_wr_avg_coefficients
    piecewise_meta["cross_wr_median_coefficients"] = cross_wr_median_coefficients

    for _ in range(8):
        changed = False

        rows_by_minute: Dict[int, List[dict]] = defaultdict(list)
        for row in output_rows:
            rows_by_minute[int(row["minute"])].append(row)
        for minute in sorted(rows_by_minute):
            minute_rows = sorted(rows_by_minute[minute], key=lambda row: int(row["wr_level"]))
            previous_avg_abs: Optional[float] = None
            previous_median_abs: Optional[float] = None
            previous_wr: Optional[int] = None
            for row in minute_rows:
                wr_level = int(row["wr_level"])
                current_avg = float(row["avg_target_networth_diff"])
                current_median = float(row["median_target_networth_diff"])
                next_avg = current_avg
                next_median = current_median
                current_avg_abs = abs(current_avg)
                current_median_abs = abs(current_median)
                if previous_avg_abs is not None and previous_wr is not None:
                    avg_ratio = float(cross_wr_avg_coefficients.get(f"{previous_wr}->{wr_level}", 1.01))
                    required_avg_abs = previous_avg_abs * avg_ratio
                    next_avg = -round(max(current_avg_abs, required_avg_abs), 2)
                    previous_avg_abs = abs(next_avg)
                else:
                    previous_avg_abs = current_avg_abs
                if previous_median_abs is not None and previous_wr is not None:
                    median_ratio = float(cross_wr_median_coefficients.get(f"{previous_wr}->{wr_level}", 1.01))
                    required_median_abs = previous_median_abs * median_ratio
                    next_median = -round(max(current_median_abs, required_median_abs), 2)
                    previous_median_abs = abs(next_median)
                else:
                    previous_median_abs = current_median_abs
                next_avg = round(next_avg, 2)
                next_median = round(next_median, 2)
                if next_avg != row["avg_target_networth_diff"]:
                    row["avg_target_networth_diff"] = next_avg
                    changed = True
                if next_median != row["median_target_networth_diff"]:
                    row["median_target_networth_diff"] = next_median
                    changed = True
                previous_wr = wr_level

        rows_by_wr_iter: Dict[int, List[dict]] = defaultdict(list)
        for row in output_rows:
            rows_by_wr_iter[int(row["wr_level"])].append(row)
        for wr_level in sorted(rows_by_wr_iter):
            wr_rows = sorted(rows_by_wr_iter[wr_level], key=lambda row: int(row["minute"]))
            prev_avg: Optional[float] = None
            prev_median: Optional[float] = None
            for row in wr_rows:
                current_avg = float(row["avg_target_networth_diff"])
                current_median = float(row["median_target_networth_diff"])
                next_avg = current_avg
                next_median = current_median
                if prev_avg is not None and current_avg >= prev_avg:
                    next_avg = round(prev_avg - 1.0, 2)
                if prev_median is not None and current_median >= prev_median:
                    next_median = round(prev_median - 1.0, 2)
                if next_avg != row["avg_target_networth_diff"]:
                    row["avg_target_networth_diff"] = next_avg
                    changed = True
                if next_median != row["median_target_networth_diff"]:
                    row["median_target_networth_diff"] = next_median
                    changed = True
                prev_avg = float(row["avg_target_networth_diff"])
                prev_median = float(row["median_target_networth_diff"])

        if not changed:
            break

    output_rows.sort(key=lambda row: (int(row["wr_level"]), int(row["minute"])))
    return output_rows, piecewise_meta


def _write_csv(path: Path, rows: Iterable[dict]) -> None:
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "wr_level",
        "minute",
        "count",
        "avg_target_networth_diff",
        "avg_target_networth_diff_piecewise_time_only",
        "avg_target_networth_diff_base_monotonic",
        "avg_target_networth_diff_raw",
        "median_target_networth_diff",
        "median_target_networth_diff_piecewise_time_only",
        "median_target_networth_diff_base_monotonic",
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
        description="Build a piecewise-coefficient late comeback table without flat minute plateaus."
    )
    parser.add_argument("--input-json", type=Path, default=DEFAULT_INPUT_JSON)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    args = parser.parse_args()

    summary, input_rows = _load_rows(args.input_json)
    output_rows, piecewise_meta = _build_piecewise_rows(input_rows, DEFAULT_SEGMENTS)
    summary = dict(summary)
    summary["piecewise_normalization"] = (
        "avg_target_networth_diff and median_target_networth_diff are re-smoothed with WR-specific "
        "piecewise multiplicative coefficients over segments 20-29, 30-39, 40+ to remove plateaus."
    )

    payload = {
        "source_table": str(args.input_json),
        "summary": summary,
        "piecewise_meta": piecewise_meta,
        "table_rows": output_rows,
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(args.output_csv, output_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
