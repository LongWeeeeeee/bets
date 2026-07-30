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
from functions import STAR_THRESHOLDS_BY_WR, check_bad_map, synergy_and_counterpick


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


@dataclass
class ModePairStat:
    a: WinStat
    b: WinStat
    overlap_a: WinStat
    overlap_b: WinStat
    overlap_total: int = 0
    a_only: int = 0
    b_only: int = 0
    n01_a_wrong_b_right: int = 0
    n10_a_right_b_wrong: int = 0


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


def _determine_side_win(predicted_sign: int, did_radiant_win: bool) -> bool:
    if predicted_sign > 0:
        return bool(did_radiant_win)
    return not bool(did_radiant_win)


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


def build_phase_dicts(
    train_path: Path,
    limit: int,
    ts_min: Optional[int] = None,
    ts_max: Optional[int] = None,
) -> tuple:
    early_dict = {}
    late_dict = {}
    seen = 0
    added = 0
    t0 = time.time()
    print(f"\n[TRAIN] {train_path.name} ts_min={ts_min} ts_max={ts_max} limit={limit:,}")

    for _, match in _iter_match_items(train_path):
        if not _is_ts_in_range(match, ts_min, ts_max):
            continue
        seen += 1
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
                f"  seen={seen:,} added={added:,} "
                f"early_keys={len(early_dict):,} late_keys={len(late_dict):,}"
            )
        if seen >= limit:
            break

    print(
        f"[TRAIN DONE] seen={seen:,} added={added:,} early_keys={len(early_dict):,} "
        f"late_keys={len(late_dict):,} time_sec={time.time() - t0:.1f}"
    )
    return early_dict, late_dict


def _extract_model_signals(
    match: dict,
    radiant_heroes_and_pos: dict,
    dire_heroes_and_pos: dict,
    early_dict: dict,
    late_dict: dict,
    early_thr: dict,
    late_thr: dict,
) -> tuple:
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
        late_output = s.get("late_output")
        if not isinstance(late_output, dict):
            late_output = {}

    early_sign, early_conflict = _strict_sign(early_output, EARLY_METRICS, early_thr)
    late_sign, late_conflict = _strict_sign(late_output, LATE_METRICS, late_thr)
    same_sign = early_sign if (early_sign is not None and late_sign is not None and early_sign == late_sign) else None

    return early_sign, late_sign, same_sign, early_conflict, late_conflict


def _mcnemar_p_value(n01: int, n10: int) -> float:
    n = n01 + n10
    if n == 0:
        return 1.0
    chi2 = ((abs(n01 - n10) - 1.0) ** 2) / n
    return math.erfc(math.sqrt(max(chi2, 0.0) / 2.0))


def _mode_report(stat: ModePairStat) -> dict:
    overlap_delta_pp = (stat.overlap_b.wr - stat.overlap_a.wr) * 100.0 if stat.overlap_total else 0.0
    return {
        "model_a": {
            "wins": stat.a.wins,
            "total": stat.a.total,
            "wr": stat.a.wr,
        },
        "model_b": {
            "wins": stat.b.wins,
            "total": stat.b.total,
            "wr": stat.b.wr,
        },
        "overlap": {
            "total": stat.overlap_total,
            "a_only": stat.a_only,
            "b_only": stat.b_only,
            "model_a_wins": stat.overlap_a.wins,
            "model_a_wr": stat.overlap_a.wr,
            "model_b_wins": stat.overlap_b.wins,
            "model_b_wr": stat.overlap_b.wr,
            "delta_b_minus_a_pp": overlap_delta_pp,
            "n01_a_wrong_b_right": stat.n01_a_wrong_b_right,
            "n10_a_right_b_wrong": stat.n10_a_right_b_wrong,
            "mcnemar_p_value": _mcnemar_p_value(stat.n01_a_wrong_b_right, stat.n10_a_right_b_wrong),
        },
    }


def evaluate_paired_on_target(
    target_path: Path,
    model_a_early: dict,
    model_a_late: dict,
    model_b_early: dict,
    model_b_late: dict,
    eval_limit: int,
    ts_min: Optional[int],
    ts_max: Optional[int],
) -> dict:
    thresholds = STAR_THRESHOLDS_BY_WR.get(60) or {}
    early_thr = _normalize_threshold_block(thresholds.get("early_output"))
    late_thr = _normalize_threshold_block(thresholds.get("mid_output"))

    modes: Dict[str, ModePairStat] = {
        "early_strict": ModePairStat(a=WinStat(), b=WinStat(), overlap_a=WinStat(), overlap_b=WinStat()),
        "late_strict": ModePairStat(a=WinStat(), b=WinStat(), overlap_a=WinStat(), overlap_b=WinStat()),
        "match_same_sign": ModePairStat(a=WinStat(), b=WinStat(), overlap_a=WinStat(), overlap_b=WinStat()),
    }

    processed = 0
    valid = 0
    a_early_conflicts = 0
    a_late_conflicts = 0
    b_early_conflicts = 0
    b_late_conflicts = 0
    t0 = time.time()

    print(f"\n[EVAL] {target_path.name} ts_min={ts_min} ts_max={ts_max} limit={eval_limit:,}")
    for _, match in _iter_match_items(target_path):
        if not _is_ts_in_range(match, ts_min, ts_max):
            continue
        processed += 1

        res = check_bad_map(match=match, maps_data=None, start_date_time=None)
        if res is None:
            continue
        radiant_heroes_and_pos, dire_heroes_and_pos = res
        did_radiant_win = bool(match.get("didRadiantWin"))
        valid += 1

        a_early, a_late, a_same, a_ec, a_lc = _extract_model_signals(
            match,
            radiant_heroes_and_pos,
            dire_heroes_and_pos,
            model_a_early,
            model_a_late,
            early_thr,
            late_thr,
        )
        b_early, b_late, b_same, b_ec, b_lc = _extract_model_signals(
            match,
            radiant_heroes_and_pos,
            dire_heroes_and_pos,
            model_b_early,
            model_b_late,
            early_thr,
            late_thr,
        )

        if a_ec:
            a_early_conflicts += 1
        if a_lc:
            a_late_conflicts += 1
        if b_ec:
            b_early_conflicts += 1
        if b_lc:
            b_late_conflicts += 1

        pairs = {
            "early_strict": (a_early, b_early),
            "late_strict": (a_late, b_late),
            "match_same_sign": (a_same, b_same),
        }
        for mode, (a_sign, b_sign) in pairs.items():
            st = modes[mode]
            a_ok = None
            b_ok = None

            if a_sign is not None:
                a_ok = _determine_side_win(a_sign, did_radiant_win)
                st.a.add(a_ok)
            if b_sign is not None:
                b_ok = _determine_side_win(b_sign, did_radiant_win)
                st.b.add(b_ok)

            if a_sign is not None and b_sign is not None:
                st.overlap_total += 1
                st.overlap_a.add(bool(a_ok))
                st.overlap_b.add(bool(b_ok))
                if bool(a_ok) != bool(b_ok):
                    if (not bool(a_ok)) and bool(b_ok):
                        st.n01_a_wrong_b_right += 1
                    else:
                        st.n10_a_right_b_wrong += 1
            elif a_sign is not None:
                st.a_only += 1
            elif b_sign is not None:
                st.b_only += 1

        if processed % 10000 == 0:
            print(
                f"  processed={processed:,} valid={valid:,} "
                f"early_overlap={modes['early_strict'].overlap_total:,} "
                f"late_overlap={modes['late_strict'].overlap_total:,} "
                f"same_overlap={modes['match_same_sign'].overlap_total:,}"
            )
        if processed >= eval_limit:
            break

    return {
        "processed": processed,
        "valid_after_check_bad_map": valid,
        "time_sec": round(time.time() - t0, 2),
        "conflicts": {
            "model_a_early": a_early_conflicts,
            "model_a_late": a_late_conflicts,
            "model_b_early": b_early_conflicts,
            "model_b_late": b_late_conflicts,
        },
        "modes": {mode: _mode_report(st) for mode, st in modes.items()},
    }


def _merge_mode_totals(acc: ModePairStat, cur: ModePairStat) -> None:
    acc.a.wins += cur.a.wins
    acc.a.total += cur.a.total
    acc.b.wins += cur.b.wins
    acc.b.total += cur.b.total
    acc.overlap_a.wins += cur.overlap_a.wins
    acc.overlap_a.total += cur.overlap_a.total
    acc.overlap_b.wins += cur.overlap_b.wins
    acc.overlap_b.total += cur.overlap_b.total
    acc.overlap_total += cur.overlap_total
    acc.a_only += cur.a_only
    acc.b_only += cur.b_only
    acc.n01_a_wrong_b_right += cur.n01_a_wrong_b_right
    acc.n10_a_right_b_wrong += cur.n10_a_right_b_wrong


def _mode_pair_from_report(mode_report: dict) -> ModePairStat:
    st = ModePairStat(a=WinStat(), b=WinStat(), overlap_a=WinStat(), overlap_b=WinStat())
    st.a.wins = int(mode_report["model_a"]["wins"])
    st.a.total = int(mode_report["model_a"]["total"])
    st.b.wins = int(mode_report["model_b"]["wins"])
    st.b.total = int(mode_report["model_b"]["total"])
    st.overlap_total = int(mode_report["overlap"]["total"])
    st.a_only = int(mode_report["overlap"]["a_only"])
    st.b_only = int(mode_report["overlap"]["b_only"])
    st.overlap_a.wins = int(mode_report["overlap"]["model_a_wins"])
    st.overlap_a.total = int(mode_report["overlap"]["total"])
    st.overlap_b.wins = int(mode_report["overlap"]["model_b_wins"])
    st.overlap_b.total = int(mode_report["overlap"]["total"])
    st.n01_a_wrong_b_right = int(mode_report["overlap"]["n01_a_wrong_b_right"])
    st.n10_a_right_b_wrong = int(mode_report["overlap"]["n10_a_right_b_wrong"])
    return st


def _ordered_non_empty_patches(summary: dict, version_root: Path, min_count: int) -> list:
    counts = summary.get("counts_by_patch") or {}
    boundaries = summary.get("patch_boundaries_utc") or {}
    rows = []
    for patch, meta in boundaries.items():
        count = int(counts.get(patch, 0) or 0)
        if count < min_count:
            continue
        p = version_root / patch / "matches.json"
        if not p.exists():
            continue
        ts = meta.get("release_ts")
        if ts is None:
            continue
        rows.append((int(ts), patch, count, p))
    rows.sort(key=lambda x: x[0])
    return rows


def run_walkforward(
    version_root: Path,
    summary_json: Path,
    output_json: Path,
    train_limit_prev: int,
    train_limit_inpatch: int,
    eval_limit: int,
    min_patch_count: int,
) -> None:
    summary = json.loads(summary_json.read_text(encoding="utf-8"))
    ordered = _ordered_non_empty_patches(summary, version_root, min_patch_count)
    if len(ordered) < 2:
        raise RuntimeError("Not enough non-empty patch files for walk-forward")

    print("\n[PATCH ORDER]")
    for _, patch, count, _ in ordered:
        print(f"  {patch}: {count:,}")

    transitions = []

    for i in range(1, len(ordered)):
        _, prev_patch, _, prev_path = ordered[i - 1]
        _, target_patch, _, target_path = ordered[i]

        print("\n" + "=" * 100)
        print(f"TRANSITION {i}/{len(ordered)-1}: {prev_patch} -> {target_patch}")
        print("=" * 100)

        target_ts = _collect_timestamps(target_path)
        if not target_ts:
            print(f"  skip {target_patch}: no startDateTime")
            continue
        split_ts = int(median(target_ts))
        eval_ts_min = split_ts + 1
        print(
            f"  target split by median ts: split_ts={split_ts} "
            f"(eval from {eval_ts_min})"
        )

        prev_early, prev_late = build_phase_dicts(
            train_path=prev_path,
            limit=int(train_limit_prev),
            ts_min=None,
            ts_max=None,
        )
        in_early, in_late = build_phase_dicts(
            train_path=target_path,
            limit=int(train_limit_inpatch),
            ts_min=None,
            ts_max=split_ts,
        )

        eval_report = evaluate_paired_on_target(
            target_path=target_path,
            model_a_early=prev_early,
            model_a_late=prev_late,
            model_b_early=in_early,
            model_b_late=in_late,
            eval_limit=int(eval_limit),
            ts_min=eval_ts_min,
            ts_max=None,
        )

        transitions.append(
            {
                "prev_patch_model_a": prev_patch,
                "target_patch_model_b": target_patch,
                "target_eval_ts_min": eval_ts_min,
                "train_a_limit": int(train_limit_prev),
                "train_b_limit": int(train_limit_inpatch),
                "eval_limit": int(eval_limit),
                "train_a_keys": {"early": len(prev_early), "late": len(prev_late)},
                "train_b_keys": {"early": len(in_early), "late": len(in_late)},
                "eval": eval_report,
            }
        )

        for mode in ("early_strict", "late_strict", "match_same_sign"):
            m = eval_report["modes"][mode]
            print(
                f"  {mode}: "
                f"A={m['model_a']['wr']:.2%} (cov={m['model_a']['total']:,}) | "
                f"B={m['model_b']['wr']:.2%} (cov={m['model_b']['total']:,}) | "
                f"overlap={m['overlap']['total']:,} "
                f"Δ(B-A)={m['overlap']['delta_b_minus_a_pp']:.2f}pp "
                f"p={m['overlap']['mcnemar_p_value']:.4f}"
            )

    agg = {
        "early_strict": ModePairStat(a=WinStat(), b=WinStat(), overlap_a=WinStat(), overlap_b=WinStat()),
        "late_strict": ModePairStat(a=WinStat(), b=WinStat(), overlap_a=WinStat(), overlap_b=WinStat()),
        "match_same_sign": ModePairStat(a=WinStat(), b=WinStat(), overlap_a=WinStat(), overlap_b=WinStat()),
    }
    for tr in transitions:
        for mode, acc in agg.items():
            cur = _mode_pair_from_report(tr["eval"]["modes"][mode])
            _merge_mode_totals(acc, cur)

    aggregate_report = {mode: _mode_report(st) for mode, st in agg.items()}

    report = {
        "version_root": str(version_root),
        "summary_json": str(summary_json),
        "generated_at_epoch": int(time.time()),
        "train_limit_prev": int(train_limit_prev),
        "train_limit_inpatch": int(train_limit_inpatch),
        "eval_limit": int(eval_limit),
        "min_patch_count": int(min_patch_count),
        "patch_order": [patch for _, patch, _, _ in ordered],
        "transitions": transitions,
        "aggregate": aggregate_report,
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved report: {output_json}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Walk-forward patch transfer paired research")
    parser.add_argument(
        "--version-root",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/sorted_by_patch_version"),
    )
    parser.add_argument(
        "--summary-json",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/sorted_by_patch_version/summary.json"),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/base/ml_dataset/patch_transfer_walkforward_report.json"),
    )
    parser.add_argument("--train-limit-prev", type=int, default=60000)
    parser.add_argument("--train-limit-inpatch", type=int, default=60000)
    parser.add_argument("--eval-limit", type=int, default=60000)
    parser.add_argument("--min-patch-count", type=int, default=50000)
    args = parser.parse_args()

    run_walkforward(
        version_root=args.version_root,
        summary_json=args.summary_json,
        output_json=args.output_json,
        train_limit_prev=int(args.train_limit_prev),
        train_limit_inpatch=int(args.train_limit_inpatch),
        eval_limit=int(args.eval_limit),
        min_patch_count=int(args.min_patch_count),
    )


if __name__ == "__main__":
    main()
