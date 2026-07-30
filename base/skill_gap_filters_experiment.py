#!/usr/bin/env python3
import argparse
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Dict, Iterator, Optional, Tuple

import orjson

try:
    import ijson
except Exception:
    ijson = None

from analise_database import analise_database
from functions import STAR_THRESHOLDS_BY_WR, check_bad_map, evaluate_winrate_check_old_maps, synergy_and_counterpick
from maps_research import check_match_quality


EARLY_METRICS = ("counterpick_1vs1", "counterpick_1vs2", "solo", "synergy_duo", "synergy_trio")
LATE_METRICS = ("counterpick_1vs1", "counterpick_1vs2", "solo", "synergy_duo", "synergy_trio")


@dataclass
class WinStat:
    wins: int = 0
    total: int = 0

    def add(self, is_win: bool) -> None:
        self.total += 1
        if is_win:
            self.wins += 1

    @property
    def wr(self) -> float:
        return (self.wins / self.total) if self.total else 0.0


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


def _normalize_threshold_block(block):
    if isinstance(block, dict):
        out = {}
        for k, v in block.items():
            try:
                out[str(k)] = float(v)
            except Exception:
                continue
        return out
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


def _strict_sign(output: dict, metrics: tuple, threshold_map: dict) -> tuple:
    hits = []
    for m in metrics:
        v = _coerce_float(output.get(m))
        if v is None:
            continue
        thr = float(threshold_map.get(m, 10**9))
        if abs(v) >= thr:
            hits.append(1 if v > 0 else -1)
    if not hits:
        return None, False
    uniq = set(hits)
    if len(uniq) == 1:
        return next(iter(uniq)), False
    return None, True


def _side_win(sign: int, did_radiant_win: bool) -> bool:
    if sign > 0:
        return bool(did_radiant_win)
    return not bool(did_radiant_win)


def _collect_timestamps(path: Path) -> list:
    ts = []
    for _, match in _iter_match_items(path):
        try:
            ts.append(int(match.get("startDateTime")))
        except Exception:
            continue
    return ts


def _is_ts_in_range(match: dict, ts_min: Optional[int], ts_max: Optional[int]) -> bool:
    try:
        ts = int(match.get("startDateTime"))
    except Exception:
        return False
    if ts_min is not None and ts < ts_min:
        return False
    if ts_max is not None and ts > ts_max:
        return False
    return True


def _build_dicts(
    matches_path: Path,
    train_limit: int,
    train_ts_max: int,
    enable_skill_gap_filters: bool,
    enable_death_anomaly_filter: bool,
) -> tuple:
    early_dict = {}
    late_dict = {}
    seen = 0
    quality_ok = 0
    quality_bad = 0
    added = 0
    quality_reason_counts: Dict[str, int] = {}
    t0 = time.time()
    print(
        f"\n[TRAIN] enable_skill_gap_filters={enable_skill_gap_filters} "
        f"enable_death_anomaly_filter={enable_death_anomaly_filter} "
        f"limit={train_limit:,} ts_max={train_ts_max}"
    )
    for _, match in _iter_match_items(matches_path):
        if not _is_ts_in_range(match, ts_min=None, ts_max=train_ts_max):
            continue
        seen += 1

        ok_quality, reason = check_match_quality(
            match,
            check_old_maps=False,
            enable_skill_gap_filters=enable_skill_gap_filters,
            enable_death_anomaly_filter=enable_death_anomaly_filter,
        )
        if not ok_quality:
            quality_bad += 1
            key = str(reason or "unknown")
            quality_reason_counts[key] = quality_reason_counts.get(key, 0) + 1
            if seen >= train_limit:
                break
            continue

        quality_ok += 1
        ok = analise_database(
            match=match,
            lane_dict=None,
            early_dict=early_dict,
            late_dict=late_dict,
            exclude_pro_matches=True,
        )
        if ok:
            added += 1

        if seen % 10000 == 0:
            print(
                f"  seen={seen:,} quality_ok={quality_ok:,} quality_bad={quality_bad:,} added={added:,} "
                f"early={len(early_dict):,} late={len(late_dict):,}"
            )
        if seen >= train_limit:
            break

    reason_top = sorted(quality_reason_counts.items(), key=lambda kv: kv[1], reverse=True)[:12]
    print(
        f"[TRAIN DONE] seen={seen:,} quality_ok={quality_ok:,} quality_bad={quality_bad:,} "
        f"added={added:,} early={len(early_dict):,} late={len(late_dict):,} time_sec={time.time()-t0:.1f}"
    )
    return early_dict, late_dict, {
        "seen": seen,
        "quality_ok": quality_ok,
        "quality_bad": quality_bad,
        "added": added,
        "quality_bad_reasons_top12": reason_top,
        "early_keys": len(early_dict),
        "late_keys": len(late_dict),
    }


def _evaluate(
    matches_path: Path,
    early_dict: dict,
    late_dict: dict,
    eval_ts_min: int,
    eval_limit: int,
) -> dict:
    thresholds = STAR_THRESHOLDS_BY_WR.get(60) or {}
    early_thr = _normalize_threshold_block(thresholds.get("early_output"))
    late_thr = _normalize_threshold_block(thresholds.get("mid_output"))

    early_stat = WinStat()
    late_stat = WinStat()
    same_stat = WinStat()
    early_conflicts = 0
    late_conflicts = 0
    processed = 0
    valid = 0
    output_rows = []
    t0 = time.time()

    print(f"\n[EVAL] limit={eval_limit:,} ts_min={eval_ts_min}")
    for match_id, match in _iter_match_items(matches_path):
        if not _is_ts_in_range(match, ts_min=eval_ts_min, ts_max=None):
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

        early_output = s.get("early_output")
        if not isinstance(early_output, dict):
            early_output = {}
        late_output = s.get("mid_output")
        if not isinstance(late_output, dict):
            late_output = {}

        did_radiant_win = bool(match.get("didRadiantWin"))
        valid += 1

        early_sign, ec = _strict_sign(early_output, EARLY_METRICS, early_thr)
        late_sign, lc = _strict_sign(late_output, LATE_METRICS, late_thr)
        if ec:
            early_conflicts += 1
        if lc:
            late_conflicts += 1

        if early_sign is not None:
            early_stat.add(_side_win(early_sign, did_radiant_win))
        if late_sign is not None:
            late_stat.add(_side_win(late_sign, did_radiant_win))
        if early_sign is not None and late_sign is not None and early_sign == late_sign:
            same_stat.add(_side_win(early_sign, did_radiant_win))

        output_rows.append(
            {
                "id": int(match_id) if str(match_id).isdigit() else match_id,
                "didRadiantWin": did_radiant_win,
                "radiantNetworthLeads": match.get("radiantNetworthLeads") or [],
                "early_output": dict(early_output),
                "mid_output": dict(late_output),
            }
        )

        if processed % 10000 == 0:
            print(
                f"  processed={processed:,} valid={valid:,} "
                f"early_cov={early_stat.total:,} late_cov={late_stat.total:,} same_cov={same_stat.total:,}"
            )
        if processed >= eval_limit:
            break

    avg_wr, details = evaluate_winrate_check_old_maps(output_rows)
    return {
        "processed": processed,
        "valid_after_check_bad_map": valid,
        "time_sec": round(time.time() - t0, 2),
        "early_conflicts": early_conflicts,
        "late_conflicts": late_conflicts,
        "check_old_maps_avg_wr": avg_wr,
        "check_old_maps_details": details,
        "star_metrics": {
            "early_strict": {"wins": early_stat.wins, "total": early_stat.total, "wr": early_stat.wr},
            "late_strict": {"wins": late_stat.wins, "total": late_stat.total, "wr": late_stat.wr},
            "match_same_sign": {"wins": same_stat.wins, "total": same_stat.total, "wr": same_stat.wr},
        },
    }


def run_experiment(
    matches_path: Path,
    output_json: Path,
    train_limit: int,
    eval_limit: int,
) -> None:
    if not matches_path.exists():
        raise FileNotFoundError(matches_path)
    ts = _collect_timestamps(matches_path)
    if not ts:
        raise RuntimeError("No startDateTime in matches file")
    split_ts = int(median(ts))
    eval_ts_min = split_ts + 1

    report = {
        "matches_path": str(matches_path),
        "split_ts": split_ts,
        "eval_ts_min": eval_ts_min,
        "train_limit": int(train_limit),
        "eval_limit": int(eval_limit),
        "configs": [],
    }

    configs = [
        ("quality_baseline", False, False),
        ("quality_skill_gap_only", True, False),
        ("quality_skill_gap_plus_death_anomaly", True, True),
    ]
    for name, enable_skill, enable_death_anomaly in configs:
        print("\n" + "=" * 90)
        print(f"CONFIG: {name}")
        print("=" * 90)
        early_dict, late_dict, train_info = _build_dicts(
            matches_path=matches_path,
            train_limit=int(train_limit),
            train_ts_max=split_ts,
            enable_skill_gap_filters=enable_skill,
            enable_death_anomaly_filter=enable_death_anomaly,
        )
        eval_res = _evaluate(
            matches_path=matches_path,
            early_dict=early_dict,
            late_dict=late_dict,
            eval_ts_min=eval_ts_min,
            eval_limit=int(eval_limit),
        )
        report["configs"].append(
            {
                "name": name,
                "enable_skill_gap_filters": enable_skill,
                "enable_death_anomaly_filter": enable_death_anomaly,
                "train": train_info,
                "eval": eval_res,
            }
        )
        print(
            f"[RESULT] {name}: avg_wr={eval_res['check_old_maps_avg_wr']:.2f}% | "
            f"early={eval_res['star_metrics']['early_strict']['wr']:.2%} "
            f"(cov={eval_res['star_metrics']['early_strict']['total']:,}) | "
            f"late={eval_res['star_metrics']['late_strict']['wr']:.2%} "
            f"(cov={eval_res['star_metrics']['late_strict']['total']:,}) | "
            f"same={eval_res['star_metrics']['match_same_sign']['wr']:.2%} "
            f"(cov={eval_res['star_metrics']['match_same_sign']['total']:,})"
        )

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved report: {output_json}")


def main() -> None:
    parser = argparse.ArgumentParser(description="A/B test for skill-gap quality filters")
    parser.add_argument(
        "--matches-path",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/sorted_by_patch_numeric/7.40/matches.json"),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/base/ml_dataset/skill_gap_filters_experiment.json"),
    )
    parser.add_argument("--train-limit", type=int, default=100000)
    parser.add_argument("--eval-limit", type=int, default=100000)
    args = parser.parse_args()

    run_experiment(
        matches_path=args.matches_path,
        output_json=args.output_json,
        train_limit=int(args.train_limit),
        eval_limit=int(args.eval_limit),
    )


if __name__ == "__main__":
    main()
