#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
BASE_DIR = ROOT_DIR / "base"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    import orjson
except Exception:
    orjson = None

try:
    import ijson
except Exception:
    ijson = None

from analise_database import (
    is_late_match,
    _has_t3_alive_at_minute,
    _resolve_did_radiant_win,
)
from explore_database import DEFAULT_BASE_DIR, DEFAULT_JSON_SUBDIR, PATCH_739_RELEASE_TS, _iter_matches
from functions import LATE_POSITION_WEIGHTS, SOLO_MIN_MATCHES, check_bad_map, counterpick_team, get_diff
from metrics_winrate import METRIC_MAX_INDEX, USE_CUMULATIVE_INDICES, _coerce_metric_value

_SOLO_KEY_RE = re.compile(r"^\d+pos[1-5]$")
_LATE_MATCH_THRESHOLDS = [
    (30, 5504), (31, 5669), (32, 5827), (33, 5981), (34, 6167), (35, 6314), (36, 6431),
    (37, 6525), (38, 6678), (39, 6799), (40, 6894), (41, 6986), (42, 7135), (43, 7215),
    (44, 7287), (45, 7350), (46, 7476), (47, 7621), (48, 7711), (49, 7827), (50, 7999),
    (51, 8122), (52, 8210), (53, 8335), (54, 8581), (55, 8782),
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare baseline vs experimental late_solo winrate using metrics_winrate index logic."
    )
    parser.add_argument(
        "--json-dir",
        default=str(DEFAULT_BASE_DIR / DEFAULT_JSON_SUBDIR),
        help="Directory with combined*.json files.",
    )
    parser.add_argument(
        "--baseline-late-dict",
        default=str(DEFAULT_BASE_DIR / "late_dict_raw.json"),
        help="Baseline late dictionary.",
    )
    parser.add_argument(
        "--experiment-late-dict",
        default=str(ROOT_DIR / "bets_data" / "analise_pub_matches_late35_80_test" / "late_dict_experiment_raw.json"),
        help="Experimental late dictionary.",
    )
    parser.add_argument(
        "--output",
        default=str(ROOT_DIR / "runtime" / "late_solo_experiment_report.json"),
        help="JSON report path.",
    )
    parser.add_argument(
        "--min-start-ts",
        type=int,
        default=PATCH_739_RELEASE_TS,
        help="Minimum startDateTime filter for raw matches.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=50000,
        help="Progress print interval by scanned matches.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=0,
        help="Optional file limit for smoke/debug runs.",
    )
    parser.add_argument(
        "--limit-matches",
        type=int,
        default=0,
        help="Optional global match limit for smoke/debug runs.",
    )
    return parser.parse_args()


def _iter_dict_items(path: Path):
    if ijson is not None:
        with open(path, "rb") as fh:
            yield from ijson.kvitems(fh, "")
        return
    if orjson is not None:
        data = orjson.loads(path.read_bytes())
    else:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    for key, value in data.items():
        yield key, value


def _load_solo_dict(path: Path):
    out = {}
    loaded = 0
    for key, value in _iter_dict_items(path):
        if not isinstance(key, str) or not _SOLO_KEY_RE.match(key):
            continue
        if not isinstance(value, dict):
            continue
        try:
            games = int(value.get("games", 0))
            wins = float(value.get("wins", 0))
            draws = float(value.get("draws", 0))
        except Exception:
            continue
        out[key] = {"games": games, "wins": wins, "draws": draws}
        loaded += 1
    print(f"loaded solo entries from {path.name}: {loaded:,}", flush=True)
    return out


def _late_experiment_tail_growth_coeff() -> float:
    tail = _LATE_MATCH_THRESHOLDS[-6:]
    ratios = []
    for (_, prev_thr), (_, next_thr) in zip(tail, tail[1:]):
        if prev_thr > 0 and next_thr > prev_thr:
            ratios.append(next_thr / prev_thr)
    if not ratios:
        return 1.016
    return sum(ratios) / len(ratios)


def _build_late_match_thresholds(
    min_minute: int = 35,
    max_minute: int = 80,
    tail_growth_coeff: float | None = None,
):
    min_minute = max(0, int(min_minute))
    max_minute = max(min_minute, int(max_minute))
    schedule = [(minute, max_abs) for minute, max_abs in _LATE_MATCH_THRESHOLDS if min_minute <= minute <= max_minute]
    if max_minute <= _LATE_MATCH_THRESHOLDS[-1][0]:
        return schedule

    coeff = float(tail_growth_coeff or _late_experiment_tail_growth_coeff())
    last_minute, last_threshold = _LATE_MATCH_THRESHOLDS[-1]
    current_threshold = float(last_threshold)
    for minute in range(last_minute + 1, max_minute + 1):
        current_threshold = max(current_threshold + 1.0, current_threshold * coeff)
        if minute >= min_minute:
            schedule.append((minute, int(round(current_threshold))))
    return schedule


def _is_late_match_experiment(
    match,
    *,
    min_minute: int = 35,
    max_minute: int = 80,
    min_extra_minutes: int = 3,
    require_t3_both: bool = True,
    domination_threshold: int = 24000,
    tail_growth_coeff: float | None = None,
):
    leads = match.get("radiantNetworthLeads", [])
    did_radiant_win = _resolve_did_radiant_win(match)
    if did_radiant_win is None:
        return False, None
    duration = len(leads)
    threshold_schedule = _build_late_match_thresholds(
        min_minute=min_minute,
        max_minute=max_minute,
        tail_growth_coeff=tail_growth_coeff,
    )
    dominator = None
    for minute, max_abs in threshold_schedule:
        if duration <= minute:
            continue
        if duration < (minute + min_extra_minutes):
            continue
        if require_t3_both and not _has_t3_alive_at_minute(match, minute, require_both_sides=True):
            continue
        if leads[minute] > max_abs and did_radiant_win:
            continue
        if leads[minute] < -max_abs and not did_radiant_win:
            continue
        for lead in leads[minute:]:
            if lead >= domination_threshold:
                dominator = "radiant"
                break
            if lead <= -domination_threshold:
                dominator = "dire"
                break
        if dominator is None:
            dominator = "radiant" if did_radiant_win else "dire"
        return True, dominator
    return False, None


def _new_stats():
    return {
        "late_matches": 0,
        "solo_available_matches": 0,
        "bets": 0,
        "wins": 0,
        "losses": 0,
        "indices": {},
    }


def _metric_bucket(metric_num: float) -> int:
    abs_val = int(round(abs(metric_num)))
    if abs_val < 1:
        abs_val = 1
    if abs_val > METRIC_MAX_INDEX:
        abs_val = METRIC_MAX_INDEX
    return abs_val


def _record_metric(stats: dict, metric_num: float, actual_winner: str) -> None:
    metric_num = _coerce_metric_value(metric_num)
    if metric_num is None or metric_num == 0:
        return
    if USE_CUMULATIVE_INDICES:
        raise RuntimeError("compare_late_solo_experiment expects exact index logic, but USE_CUMULATIVE_INDICES=True")
    index = _metric_bucket(metric_num)
    bucket = stats["indices"].setdefault(
        str(index),
        {"bets": 0, "wins": 0, "losses": 0},
    )
    stats["bets"] += 1
    stats["solo_available_matches"] += 1
    bucket["bets"] += 1
    predicted_winner = "radiant" if metric_num > 0 else "dire"
    if predicted_winner == actual_winner:
        stats["wins"] += 1
        bucket["wins"] += 1
    else:
        stats["losses"] += 1
        bucket["losses"] += 1


def _coverage_ready(late_dict: dict, team: dict) -> bool:
    for pos in ("pos1", "pos2", "pos3", "pos4", "pos5"):
        hero_id = team.get(pos, {}).get("hero_id")
        try:
            hero_id = int(hero_id)
        except (TypeError, ValueError):
            return False
        key = f"{hero_id}{pos}"
        if late_dict.get(key, {}).get("games", 0) < SOLO_MIN_MATCHES:
            return False
    return True


def _compute_late_solo(match: dict, late_dict: dict):
    checked = check_bad_map(match=match, start_date_time=None)
    if checked is None:
        return None
    radiant_heroes_and_pos, dire_heroes_and_pos = checked
    if not _coverage_ready(late_dict, radiant_heroes_and_pos):
        return None
    if not _coverage_ready(late_dict, dire_heroes_and_pos):
        return None
    output = {}
    counterpick_team(radiant_heroes_and_pos, dire_heroes_and_pos, output, "radiant_counterpick", late_dict, check_solo=True)
    counterpick_team(dire_heroes_and_pos, radiant_heroes_and_pos, output, "dire_counterpick", late_dict, check_solo=True)
    if "radiant_counterpick_solo" not in output or "dire_counterpick_solo" not in output:
        return None
    return get_diff(
        output["radiant_counterpick_solo"],
        output["dire_counterpick_solo"],
        _1vs2=True,
        custom_position_weights=LATE_POSITION_WEIGHTS,
    )


def _finalize(stats: dict) -> dict:
    indices = {}
    for key in sorted(stats["indices"], key=lambda value: int(value)):
        bucket = stats["indices"][key]
        bets = bucket["bets"]
        wins = bucket["wins"]
        wr = (wins / bets * 100.0) if bets else None
        indices[key] = {
            "bets": bets,
            "wins": wins,
            "losses": bucket["losses"],
            "wr": wr,
        }
    bets_6_plus = wins_6_plus = losses_6_plus = 0
    for key, bucket in indices.items():
        if int(key) >= 6:
            bets_6_plus += bucket["bets"]
            wins_6_plus += bucket["wins"]
            losses_6_plus += bucket["losses"]
    return {
        "late_matches": stats["late_matches"],
        "solo_available_matches": stats["solo_available_matches"],
        "coverage_vs_late_matches": (
            stats["solo_available_matches"] / stats["late_matches"] * 100.0 if stats["late_matches"] else None
        ),
        "bets": stats["bets"],
        "wins": stats["wins"],
        "losses": stats["losses"],
        "wr": (stats["wins"] / stats["bets"] * 100.0) if stats["bets"] else None,
        "indices": indices,
        "index_6_plus": {
            "bets": bets_6_plus,
            "wins": wins_6_plus,
            "losses": losses_6_plus,
            "wr": (wins_6_plus / bets_6_plus * 100.0) if bets_6_plus else None,
        },
    }


def _print_summary(report: dict) -> None:
    baseline = report["baseline"]
    experiment = report["experiment"]
    print("late_solo summary", flush=True)
    print(
        f"baseline: late_matches={baseline['late_matches']:,} "
        f"solo={baseline['solo_available_matches']:,} "
        f"coverage={baseline['coverage_vs_late_matches']:.2f}% "
        f"wr={baseline['wr']:.2f}% "
        f"wr_6plus={baseline['index_6_plus']['wr']:.2f}% "
        f"bets_6plus={baseline['index_6_plus']['bets']:,}"
    , flush=True)
    print(
        f"experiment: late_matches={experiment['late_matches']:,} "
        f"solo={experiment['solo_available_matches']:,} "
        f"coverage={experiment['coverage_vs_late_matches']:.2f}% "
        f"wr={experiment['wr']:.2f}% "
        f"wr_6plus={experiment['index_6_plus']['wr']:.2f}% "
        f"bets_6plus={experiment['index_6_plus']['bets']:,}"
    , flush=True)
    all_indices = sorted(
        {int(k) for k in baseline["indices"].keys()} | {int(k) for k in experiment["indices"].keys()}
    )
    print("\nidx | baseline bets/wr | experiment bets/wr | delta wr", flush=True)
    for index in all_indices:
        b = baseline["indices"].get(str(index), {})
        e = experiment["indices"].get(str(index), {})
        b_wr = b.get("wr")
        e_wr = e.get("wr")
        delta = None if b_wr is None or e_wr is None else e_wr - b_wr
        print(
            f"{index:>3} | "
            f"{b.get('bets', 0):>6} / {('-' if b_wr is None else f'{b_wr:.2f}%'):>8} | "
            f"{e.get('bets', 0):>6} / {('-' if e_wr is None else f'{e_wr:.2f}%'):>8} | "
            f"{('-' if delta is None else f'{delta:+.2f}'):>7}"
        , flush=True)


def main() -> None:
    args = _parse_args()
    json_dir = Path(args.json_dir)
    baseline_path = Path(args.baseline_late_dict)
    experiment_path = Path(args.experiment_late_dict)
    output_path = Path(args.output)

    baseline_dict = _load_solo_dict(baseline_path)
    experiment_dict = _load_solo_dict(experiment_path)

    files = sorted(json_dir.glob("combined*.json"))
    if args.limit_files > 0:
        files = files[: args.limit_files]
    if not files:
        raise RuntimeError(f"No combined*.json files found in {json_dir}")

    baseline_stats = _new_stats()
    experiment_stats = _new_stats()
    scanned = 0
    started = time.time()

    for file_index, file_path in enumerate(files, 1):
        print(f"[{file_index}/{len(files)}] {file_path.name}", flush=True)
        for _, match in _iter_matches(file_path):
            scanned += 1
            if args.limit_matches > 0 and scanned > args.limit_matches:
                break
            if not isinstance(match, dict):
                continue
            start_ts = match.get("startDateTime")
            try:
                start_ts = int(start_ts)
            except Exception:
                continue
            if start_ts < int(args.min_start_ts):
                continue
            if len(match.get("players", [])) != 10:
                continue

            match_is_late, late_dominator = is_late_match(match=match, dominator=None)
            if match_is_late and late_dominator is not None:
                baseline_stats["late_matches"] += 1
                metric_value = _compute_late_solo(match, baseline_dict)
                if metric_value is not None:
                    _record_metric(baseline_stats, metric_value, late_dominator)

            match_is_late_exp, late_dominator_exp = _is_late_match_experiment(match)
            if match_is_late_exp and late_dominator_exp is not None:
                experiment_stats["late_matches"] += 1
                metric_value_exp = _compute_late_solo(match, experiment_dict)
                if metric_value_exp is not None:
                    _record_metric(experiment_stats, metric_value_exp, late_dominator_exp)

            if args.progress_every > 0 and scanned % args.progress_every == 0:
                elapsed = time.time() - started
                print(
                    f"  scanned={scanned:,} "
                    f"baseline_late={baseline_stats['late_matches']:,} "
                    f"experiment_late={experiment_stats['late_matches']:,} "
                    f"elapsed={elapsed/60.0:.1f}m"
                , flush=True)
        if args.limit_matches > 0 and scanned >= args.limit_matches:
            break

    coeff = _late_experiment_tail_growth_coeff()
    report = {
        "meta": {
            "json_dir": str(json_dir),
            "baseline_late_dict": str(baseline_path),
            "experiment_late_dict": str(experiment_path),
            "min_start_ts": args.min_start_ts,
            "scanned_matches": scanned,
            "metric_max_index": METRIC_MAX_INDEX,
            "use_cumulative_indices": USE_CUMULATIVE_INDICES,
            "late_experiment_schedule": [
                {"minute": minute, "max_abs": max_abs}
                for minute, max_abs in _build_late_match_thresholds(35, 80, coeff)
            ],
            "late_experiment_tail_growth_coeff": coeff,
        },
        "baseline": _finalize(baseline_stats),
        "experiment": _finalize(experiment_stats),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nreport saved to {output_path}", flush=True)
    _print_summary(report)


if __name__ == "__main__":
    main()
