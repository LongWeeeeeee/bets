#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
BASE_DIR = ROOT_DIR / "base"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import analise_database as ad
import explore_database
from explore_database import DEFAULT_JSON_SUBDIR, DEFAULT_BASE_DIR, PATCH_739_RELEASE_TS

_LATE_MATCH_THRESHOLDS = [
    (30, 5504), (31, 5669), (32, 5827), (33, 5981), (34, 6167), (35, 6314), (36, 6431),
    (37, 6525), (38, 6678), (39, 6799), (40, 6894), (41, 6986), (42, 7135), (43, 7215),
    (44, 7287), (45, 7350), (46, 7476), (47, 7621), (48, 7711), (49, 7827), (50, 7999),
    (51, 8122), (52, 8210), (53, 8335), (54, 8581), (55, 8782),
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build experimental late-only dictionary with min minute 35 and max minute 80."
    )
    parser.add_argument(
        "--base-dir",
        default=str(ROOT_DIR / "bets_data" / "analise_pub_matches_late35_80_test"),
        help="Output directory for experimental artifacts.",
    )
    parser.add_argument(
        "--json-dir",
        default=str(DEFAULT_BASE_DIR / DEFAULT_JSON_SUBDIR),
        help="Directory with combined*.json public match shards.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=10000,
        help="Progress print interval inside a file.",
    )
    parser.add_argument(
        "--min-start-ts",
        type=int,
        default=PATCH_739_RELEASE_TS,
        help="Minimum startDateTime filter.",
    )
    parser.add_argument(
        "--min-minute",
        type=int,
        default=35,
        help="Experimental late minimum minute.",
    )
    parser.add_argument(
        "--max-minute",
        type=int,
        default=80,
        help="Experimental late maximum minute.",
    )
    parser.add_argument(
        "--tail-growth-coeff",
        type=float,
        default=0.0,
        help="Optional explicit multiplicative growth coefficient after minute 55.",
    )
    parser.add_argument(
        "--min-extra-minutes",
        type=int,
        default=3,
        help="Minimum extra duration after qualifying minute.",
    )
    parser.add_argument(
        "--require-t3-both",
        type=int,
        default=1,
        choices=[0, 1],
        help="Require both T3 towers alive at qualifying minute.",
    )
    parser.add_argument(
        "--domination-threshold",
        type=int,
        default=24000,
        help="Late experiment domination threshold.",
    )
    return parser.parse_args()


def _set_env(name: str, value: int | float | str) -> None:
    os.environ[name] = str(value)


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
    min_minute: int,
    max_minute: int,
    min_extra_minutes: int,
    require_t3_both: bool,
    domination_threshold: int,
    tail_growth_coeff: float,
):
    leads = match.get("radiantNetworthLeads", [])
    did_radiant_win = ad._resolve_did_radiant_win(match)
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
        if require_t3_both and not ad._has_t3_alive_at_minute(match, minute, require_both_sides=True):
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


def _make_experimental_analyzer(
    *,
    min_minute: int,
    max_minute: int,
    min_extra_minutes: int,
    require_t3_both: bool,
    domination_threshold: int,
    tail_growth_coeff: float,
):
    def _analise_database_experiment(
        match,
        lane_dict,
        early_dict,
        late_dict,
        exclude_match_ids=None,
        exclude_pro_matches=True,
        dominator=None,
        late_experiment_dict=None,
        match_id_hint=None,
    ):
        if match is None or not isinstance(match, dict):
            return False
        if exclude_pro_matches and ad.is_pro_match(match):
            return False
        if ad._match_in_exclude_set(match, exclude_match_ids, match_id_hint=match_id_hint):
            return False

        updated = False
        if lane_dict is not None:
            updated = ad.lanes(match, lane_dict) or updated

        radiant_by_pos, dire_by_pos = ad.extract_heroes_by_position(match)
        if radiant_by_pos is None:
            return updated

        did_radiant_win = ad._resolve_did_radiant_win(match)
        if did_radiant_win is None:
            return updated

        early_ok, dominator = ad.is_early_match(match)
        if early_ok and early_dict is not None:
            radiant_value = 1 if dominator == "radiant" else 0
            dire_value = 1 if dominator == "dire" else 0
            ad._add_combinations_to_dict(radiant_by_pos, dire_by_pos, early_dict, radiant_value, dire_value)
            updated = True

        status, dominator = ad.is_late_match(match=match, dominator=dominator)
        if status and late_dict is not None:
            radiant_value = 1 if did_radiant_win else 0
            dire_value = 0 if did_radiant_win else 1
            ad._add_combinations_to_dict(radiant_by_pos, dire_by_pos, late_dict, radiant_value, dire_value)
            updated = True

        if late_experiment_dict is not None:
            status_exp, _ = _is_late_match_experiment(
                match,
                min_minute=min_minute,
                max_minute=max_minute,
                min_extra_minutes=min_extra_minutes,
                require_t3_both=require_t3_both,
                domination_threshold=domination_threshold,
                tail_growth_coeff=tail_growth_coeff,
            )
            if status_exp:
                radiant_value = 1 if did_radiant_win else 0
                dire_value = 0 if did_radiant_win else 1
                ad._add_combinations_to_dict(
                    radiant_by_pos,
                    dire_by_pos,
                    late_experiment_dict,
                    radiant_value,
                    dire_value,
                )
                updated = True

        return updated

    return _analise_database_experiment


def main() -> None:
    args = _parse_args()
    base_dir = Path(args.base_dir)
    json_dir = Path(args.json_dir)
    coeff = args.tail_growth_coeff if args.tail_growth_coeff > 1.0 else _late_experiment_tail_growth_coeff()
    schedule = _build_late_match_thresholds(
        min_minute=args.min_minute,
        max_minute=args.max_minute,
        tail_growth_coeff=coeff,
    )

    _set_env("EXPLORE_EXPERIMENTAL_LATE_ONLY", 1)
    _set_env("EXPLORE_DISABLE_TEST_EXCLUSION", 1)
    _set_env("EXPLORE_EXCLUDE_PATCH_738", 1)
    _set_env("EXPLORE_PROGRESS_EVERY", args.progress_every)
    _set_env("EXPLORE_BASE_DIR", str(base_dir))
    _set_env("EXPLORE_JSON_DIR", str(json_dir))
    _set_env("LATE_EXPERIMENT_MIN_MINUTE", args.min_minute)
    _set_env("LATE_EXPERIMENT_MAX_MINUTE", args.max_minute)
    _set_env("LATE_EXPERIMENT_MIN_EXTRA_MINUTES", args.min_extra_minutes)
    _set_env("LATE_EXPERIMENT_REQUIRE_T3_BOTH", args.require_t3_both)
    _set_env("LATE_EXPERIMENT_DOMINATION_THRESHOLD", args.domination_threshold)
    _set_env("LATE_EXPERIMENT_TAIL_GROWTH_COEFF", coeff)
    _set_env("EXPLORE_MIN_START_TS", args.min_start_ts)

    base_dir.mkdir(parents=True, exist_ok=True)
    config = {
        "base_dir": str(base_dir),
        "json_dir": str(json_dir),
        "min_start_ts": args.min_start_ts,
        "min_minute": args.min_minute,
        "max_minute": args.max_minute,
        "min_extra_minutes": args.min_extra_minutes,
        "require_t3_both": bool(args.require_t3_both),
        "domination_threshold": args.domination_threshold,
        "tail_growth_coeff": coeff,
        "threshold_schedule": [{"minute": minute, "max_abs": max_abs} for minute, max_abs in schedule],
    }
    (base_dir / "late_experiment_config.json").write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("late experiment config:")
    print(json.dumps(config, ensure_ascii=False, indent=2))
    explore_database.analise_database = _make_experimental_analyzer(
        min_minute=args.min_minute,
        max_minute=args.max_minute,
        min_extra_minutes=args.min_extra_minutes,
        require_t3_both=bool(args.require_t3_both),
        domination_threshold=args.domination_threshold,
        tail_growth_coeff=coeff,
    )
    explore_database.run_explore_database(base_dir=base_dir, json_dir=json_dir)


if __name__ == "__main__":
    main()
