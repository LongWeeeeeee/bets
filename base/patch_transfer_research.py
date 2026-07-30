#!/usr/bin/env python3
import argparse
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Dict, Iterable, Iterator, Optional, Tuple

import orjson

try:
    import ijson
except Exception:
    ijson = None

from analise_database import analise_database
from functions import STAR_THRESHOLDS_BY_WR, check_bad_map, synergy_and_counterpick


@dataclass
class EvalStats:
    wins: int = 0
    total: int = 0

    def add(self, is_win: bool) -> None:
        self.total += 1
        if is_win:
            self.wins += 1

    @property
    def wr(self) -> float:
        return (self.wins / self.total) if self.total > 0 else 0.0


def _iter_match_items(path: Path) -> Iterator[Tuple[str, dict]]:
    if ijson is not None:
        with path.open("rb") as f:
            for key, value in ijson.kvitems(f, "", use_float=True):
                if isinstance(value, dict):
                    yield str(key), value
        return
    with path.open("rb") as f:
        payload = orjson.loads(f.read())
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, dict):
                yield str(key), value


def _collect_start_timestamps(path: Path) -> list:
    out = []
    for _, match in _iter_match_items(path):
        ts = match.get("startDateTime")
        try:
            out.append(int(ts))
        except Exception:
            continue
    return out


def _determine_side_win(predicted_sign: int, did_radiant_win: bool) -> bool:
    if predicted_sign > 0:
        return bool(did_radiant_win)
    return not bool(did_radiant_win)


def _coerce_float(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return float(value)
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        if v.endswith("*"):
            v = v[:-1]
        try:
            return float(v)
        except ValueError:
            return None
    return None


def _is_match_allowed_by_ts(match: dict, ts_min: Optional[int], ts_max: Optional[int]) -> bool:
    try:
        ts = int(match.get("startDateTime"))
    except Exception:
        return False
    if ts_min is not None and ts < ts_min:
        return False
    if ts_max is not None and ts > ts_max:
        return False
    return True


def build_phase_dicts(
    train_paths: list,
    limit_per_path: int,
    ts_min: Optional[int] = None,
    ts_max: Optional[int] = None,
) -> tuple:
    early_dict = {}
    late_dict = {}
    total_seen = 0
    total_added = 0
    t0 = time.time()

    for path in train_paths:
        path_seen = 0
        path_added = 0
        print(f"\n[TRAIN] {path}")
        for _, match in _iter_match_items(path):
            if not _is_match_allowed_by_ts(match, ts_min, ts_max):
                continue
            total_seen += 1
            path_seen += 1
            ok = analise_database(
                match=match,
                lane_dict=None,
                early_dict=early_dict,
                late_dict=late_dict,
                exclude_pro_matches=True,
            )
            if ok:
                total_added += 1
                path_added += 1
            if path_seen % 10000 == 0:
                print(
                    f"  seen={path_seen:,}, added={path_added:,}, "
                    f"early_keys={len(early_dict):,}, late_keys={len(late_dict):,}"
                )
            if path_seen >= limit_per_path:
                break
        print(
            f"  done: seen={path_seen:,}, added={path_added:,}, "
            f"early_keys={len(early_dict):,}, late_keys={len(late_dict):,}"
        )

    print(
        f"\n[TRAIN SUMMARY] seen={total_seen:,}, added={total_added:,}, "
        f"early_keys={len(early_dict):,}, late_keys={len(late_dict):,}, "
        f"time_sec={time.time()-t0:.1f}"
    )
    return early_dict, late_dict


def evaluate_on_target(
    target_path: Path,
    early_dict: dict,
    late_dict: dict,
    eval_limit: int,
    ts_min: Optional[int] = None,
    ts_max: Optional[int] = None,
) -> dict:
    thresholds = STAR_THRESHOLDS_BY_WR.get(60) or {}

    def _normalize_threshold_block(block):
        if isinstance(block, dict):
            return {str(k): float(v) for k, v in block.items()}
        if isinstance(block, list):
            out = {}
            for item in block:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    key = str(item[0])
                    try:
                        out[key] = float(item[1])
                    except Exception:
                        continue
            return out
        return {}

    early_thr = _normalize_threshold_block(thresholds.get("early_output"))
    late_thr = _normalize_threshold_block(thresholds.get("mid_output"))
    early_metrics = ["counterpick_1vs1", "counterpick_1vs2", "solo", "synergy_duo", "synergy_trio"]
    late_metrics = ["counterpick_1vs1", "counterpick_1vs2", "solo", "synergy_duo", "synergy_trio"]

    per_metric = {}
    for m in early_metrics:
        per_metric[f"early_{m}"] = EvalStats()
    for m in late_metrics:
        per_metric[f"late_{m}"] = EvalStats()

    early_strict = EvalStats()
    late_strict = EvalStats()
    match_same_sign = EvalStats()
    early_conflicts = 0
    late_conflicts = 0
    processed = 0
    valid = 0
    t0 = time.time()

    print(f"\n[EVAL] {target_path}")
    for _, match in _iter_match_items(target_path):
        if not _is_match_allowed_by_ts(match, ts_min, ts_max):
            continue
        processed += 1

        res = check_bad_map(match=match, maps_data=None, start_date_time=None)
        if res is None:
            continue
        radiant_heroes_and_pos, dire_heroes_and_pos = res
        s = synergy_and_counterpick(
            radiant_heroes_and_pos=radiant_heroes_and_pos,
            dire_heroes_and_pos=dire_heroes_and_pos,
            early_dict=early_dict,
            mid_dict=late_dict,
        ) or {}

        did_radiant_win = bool(match.get("didRadiantWin"))
        valid += 1

        early_output = s.get("early_output", {}) if isinstance(s.get("early_output"), dict) else {}
        late_output = s.get("mid_output", {}) if isinstance(s.get("mid_output"), dict) else {}

        early_hits = []
        late_hits = []

        for m in early_metrics:
            v = _coerce_float(early_output.get(m))
            thr = float(early_thr.get(m, 10**9))
            if v is None:
                continue
            if abs(v) >= thr:
                sign = 1 if v > 0 else -1
                per_metric[f"early_{m}"].add(_determine_side_win(sign, did_radiant_win))
                early_hits.append(sign)

        for m in late_metrics:
            v = _coerce_float(late_output.get(m))
            thr = float(late_thr.get(m, 10**9))
            if v is None:
                continue
            if abs(v) >= thr:
                sign = 1 if v > 0 else -1
                per_metric[f"late_{m}"].add(_determine_side_win(sign, did_radiant_win))
                late_hits.append(sign)

        early_sign = None
        if early_hits:
            uniq = set(early_hits)
            if len(uniq) == 1:
                early_sign = next(iter(uniq))
                early_strict.add(_determine_side_win(early_sign, did_radiant_win))
            else:
                early_conflicts += 1

        late_sign = None
        if late_hits:
            uniq = set(late_hits)
            if len(uniq) == 1:
                late_sign = next(iter(uniq))
                late_strict.add(_determine_side_win(late_sign, did_radiant_win))
            else:
                late_conflicts += 1

        if early_sign is not None and late_sign is not None and early_sign == late_sign:
            match_same_sign.add(_determine_side_win(early_sign, did_radiant_win))

        if processed % 10000 == 0:
            print(
                f"  processed={processed:,}, valid={valid:,}, "
                f"early_strict_cov={early_strict.total:,}, late_strict_cov={late_strict.total:,}"
            )
        if processed >= eval_limit:
            break

    out = {
        "processed": processed,
        "valid_after_check_bad_map": valid,
        "time_sec": round(time.time() - t0, 2),
        "early_conflicts": early_conflicts,
        "late_conflicts": late_conflicts,
        "early_strict": {
            "wins": early_strict.wins,
            "total": early_strict.total,
            "wr": early_strict.wr,
        },
        "late_strict": {
            "wins": late_strict.wins,
            "total": late_strict.total,
            "wr": late_strict.wr,
        },
        "match_same_sign": {
            "wins": match_same_sign.wins,
            "total": match_same_sign.total,
            "wr": match_same_sign.wr,
        },
        "per_metric": {
            name: {"wins": st.wins, "total": st.total, "wr": st.wr}
            for name, st in sorted(per_metric.items())
        },
    }
    return out


def run_research(
    numeric_root: Path,
    output_json: Path,
    train_limit_per_patch: int,
    eval_limit: int,
) -> None:
    p740 = numeric_root / "7.40" / "matches.json"
    p739 = numeric_root / "7.39" / "matches.json"
    p738 = numeric_root / "7.38" / "matches.json"

    for p in (p740, p739, p738):
        if not p.exists():
            raise FileNotFoundError(f"Missing required file: {p}")

    print("[SETUP] collecting timestamps for 7.40 split...")
    ts = _collect_start_timestamps(p740)
    if not ts:
        raise RuntimeError("No startDateTime values in 7.40 matches.json")
    split_ts = int(median(ts))
    print(f"[SETUP] 7.40 median startDateTime split = {split_ts}")

    configs = [
        {
            "name": "prev_7.39_only",
            "train_paths": [p739],
            "train_ts_min": None,
            "train_ts_max": None,
        },
        {
            "name": "prev_7.39_plus_7.38",
            "train_paths": [p739, p738],
            "train_ts_min": None,
            "train_ts_max": None,
        },
        {
            "name": "inpatch_7.40_first_half_reference",
            "train_paths": [p740],
            "train_ts_min": None,
            "train_ts_max": split_ts,
        },
    ]

    report = {
        "numeric_root": str(numeric_root),
        "target_patch": "7.40",
        "target_eval_ts_min": split_ts + 1,
        "train_limit_per_patch": int(train_limit_per_patch),
        "eval_limit": int(eval_limit),
        "configs": [],
    }

    for cfg in configs:
        print("\n" + "=" * 90)
        print(f"CONFIG: {cfg['name']}")
        print("=" * 90)
        early_dict, late_dict = build_phase_dicts(
            train_paths=cfg["train_paths"],
            limit_per_path=int(train_limit_per_patch),
            ts_min=cfg["train_ts_min"],
            ts_max=cfg["train_ts_max"],
        )
        eval_res = evaluate_on_target(
            target_path=p740,
            early_dict=early_dict,
            late_dict=late_dict,
            eval_limit=int(eval_limit),
            ts_min=split_ts + 1,
            ts_max=None,
        )
        cfg_report = {
            "name": cfg["name"],
            "train_paths": [str(p) for p in cfg["train_paths"]],
            "train_ts_min": cfg["train_ts_min"],
            "train_ts_max": cfg["train_ts_max"],
            "train_early_keys": len(early_dict),
            "train_late_keys": len(late_dict),
            "eval": eval_res,
        }
        report["configs"].append(cfg_report)

        print(
            f"[RESULT] {cfg['name']}: "
            f"early_strict={eval_res['early_strict']['wr']:.2%} (cov={eval_res['early_strict']['total']:,}), "
            f"late_strict={eval_res['late_strict']['wr']:.2%} (cov={eval_res['late_strict']['total']:,}), "
            f"same_sign={eval_res['match_same_sign']['wr']:.2%} (cov={eval_res['match_same_sign']['total']:,})"
        )

    output_json.parent.mkdir(parents=True, exist_ok=True)
    with output_json.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nSaved report: {output_json}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Patch-transfer research for numeric patches")
    parser.add_argument(
        "--numeric-root",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/sorted_by_patch_numeric"),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/base/ml_dataset/patch_transfer_numeric_report.json"),
    )
    parser.add_argument("--train-limit-per-patch", type=int, default=120000)
    parser.add_argument("--eval-limit", type=int, default=100000)
    args = parser.parse_args()

    run_research(
        numeric_root=args.numeric_root,
        output_json=args.output_json,
        train_limit_per_patch=args.train_limit_per_patch,
        eval_limit=args.eval_limit,
    )


if __name__ == "__main__":
    main()
