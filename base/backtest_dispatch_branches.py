#!/usr/bin/env python3
"""
Backtest dispatch-веток + метрик + networth (pub matches).

Собирает:
  1) Ветки E/L/A (max WR-tier + sign), same-sign / opposite, single-block min65
  2) Per-metric WR по блокам: e_cp1vs1, l_solo, a_pos1_vs_pos1, ...
     с порогами abs(index) >= T
  3) Networth lead на минутах 6/10/12/15/20/27 (для коррекции dispatch-гейтов)
  4) Rejected / missed-edge пулы (opposite, single@60, strong metric без star)
  5) Stake-tier proxy (x0.5/x1/x2/x3) по live-правилам (late hits >=2 assumed)

Использование:
    python3 backtest_dispatch_branches.py --max-matches 40000 --patch 7.41 \\
        --output ../data/backtest_branches_v2.json
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from collections import defaultdict
from typing import Any, Optional

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
for _p in (str(BASE_DIR), str(ROOT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
sys.path.insert(0, str(BASE_DIR))

from check_old_maps import (
    _resolve_maps_paths,
    _load_stats_dicts,
    _draft_stats_lookup_keys,
    _draft_scoped_stats_lookup,
    _iter_json_object_items,
    check_bad_map,
    _team_payload,
    _match_outcomes,
    _dump_json,
    _rss_mb,
    SqliteStatsLookup,
    DEFAULT_STATS_DIR,
    DEC_15_2025_UTC,
)
from functions import synergy_and_counterpick
from cyberscore_try import (
    _star_block_diagnostics,
    _build_all_star_output,
    _coerce_metric_value,
    STAR_THRESHOLDS_BY_WR,
)

# Live gate may be absent on older server checkouts; keep backtest self-contained.
try:
    from cyberscore_try import SINGLE_BLOCK_STAR_MIN_WR
except ImportError:
    SINGLE_BLOCK_STAR_MIN_WR = 65.0

WR_LEVELS = sorted(int(k) for k in STAR_THRESHOLDS_BY_WR.keys())
BLOCKS = (("early_output", "E"), ("mid_output", "L"), ("all_output", "A"))
BLOCK_LABELS = {"early_output": "E", "mid_output": "L", "all_output": "A"}
BLOCK_PREFIX = {"early_output": "e", "mid_output": "l", "all_output": "a"}
DISPATCH_PRIORITY = ("mid_output", "early_output", "all_output")

# Metrics to score individually (user-requested + cp1vs2 as sibling of cp1vs1).
METRIC_KEYS = (
    ("counterpick_1vs1", "cp1vs1"),
    ("counterpick_1vs2", "cp1vs2"),
    ("solo", "solo"),
    ("synergy_duo", "synergy_duo"),
    ("synergy_trio", "synergy_trio"),
    ("pos1_vs_pos1", "pos1_vs_pos1"),
)
ABS_THRESHOLDS = (1, 3, 5, 7, 10, 12, 15, 20, 25, 30)
NETWORTH_MINUTES = (6, 10, 12, 15, 20, 27)
# target-side lead buckets at a given minute
NW_BUCKETS = (
    ("lead_ge_3000", 3000, None),
    ("lead_ge_1500", 1500, 3000),
    ("lead_ge_800", 800, 1500),
    ("lead_ge_0", 0, 800),
    ("behind_0_800", -800, 0),
    ("behind_800_1500", -1500, -800),
    ("behind_ge_1500", None, -1500),
)


def _iter_records(maps_paths, start_date_time: int, max_matches: Optional[int]):
    scanned = 0
    selected = 0
    skipped = 0
    for path in maps_paths:
        print(f"Reading maps: {path}", flush=True)
        for match_id, match in _iter_json_object_items(path):
            scanned += 1
            if not isinstance(match, dict):
                skipped += 1
                continue
            if int(match.get("startDateTime") or 0) < int(start_date_time):
                continue
            parsed = check_bad_map(match, start_date_time=start_date_time)
            if parsed is None:
                skipped += 1
                continue
            radiant_draft, dire_draft = parsed
            leads = match.get("radiantNetworthLeads") or match.get("radiant_networth_leads") or []
            if not isinstance(leads, list):
                leads = []
            selected += 1
            yield {
                "id": int(match.get("id") or match_id or 0),
                "radiant_draft": _team_payload(radiant_draft),
                "dire_draft": _team_payload(dire_draft),
                "radiant_networth_leads": leads,
                **_match_outcomes(match),
            }
            if scanned % 5000 == 0:
                print(f"  scanned={scanned:,} selected={selected:,} skipped={skipped:,}", flush=True)
            if max_matches is not None and selected >= int(max_matches):
                print(f"Reached max_matches={max_matches} (scanned={scanned:,}, skipped={skipped:,})")
                return
    print(f"Selected matches: {selected:,} (scanned={scanned:,}, skipped={skipped:,})")


def _shrink_sqlite_caches(*lookups) -> None:
    for lk in lookups:
        if isinstance(lk, SqliteStatsLookup):
            try:
                lk.max_cached_keys = 5000
                conn = lk._connect()
                conn.execute("PRAGMA cache_size=-32000")
                conn.execute("PRAGMA mmap_size=0")
            except Exception as exc:  # noqa: BLE001
                print(f"  ! cache shrink failed for {getattr(lk, 'label', '?')}: {exc}")


def _new_stat() -> dict:
    return {"matches": 0, "wins": 0, "losses": 0}


def _add(stat: dict, won: int) -> None:
    stat["matches"] += 1
    stat["wins"] += won
    stat["losses"] += 1 - won


def _wr(stat: dict) -> float:
    m = stat.get("matches", 0)
    return (stat.get("wins", 0) / m * 100.0) if m else 0.0


def _sign_char(sign: Optional[int]) -> str:
    return "r" if sign == 1 else "d" if sign == -1 else "?"


def _sign_to_side(sign: Optional[int]) -> Optional[str]:
    if sign == 1:
        return "radiant"
    if sign == -1:
        return "dire"
    return None


def _block_label(tier: Optional[int], sign: Optional[int]) -> str:
    if tier is None:
        return "no"
    return f"{tier}{_sign_char(sign)}"


def _block_max_tier(metrics_block: Any, section: str) -> tuple[Optional[int], Optional[int], int]:
    """Return (max_tier, sign, hit_count_at_max_tier)."""
    raw_block = metrics_block if isinstance(metrics_block, dict) else {}
    best_tier: Optional[int] = None
    best_sign: Optional[int] = None
    best_hits = 0
    for wr in WR_LEVELS:
        diag = _star_block_diagnostics(raw_block, wr, section)
        if diag.get("valid") and diag.get("sign") in (1, -1):
            best_tier = wr
            best_sign = diag.get("sign")
            try:
                best_hits = int(diag.get("hit_count") or len(diag.get("hit_metrics") or []))
            except (TypeError, ValueError):
                best_hits = len(diag.get("hit_metrics") or [])
    return best_tier, best_sign, best_hits


def _lead_at_minute(leads: list, minute: int) -> Optional[float]:
    """radiantNetworthLeads[minute-1] == radiant lead at that minute."""
    if not leads or minute < 1:
        return None
    idx = minute - 1
    if idx >= len(leads):
        return None
    try:
        return float(leads[idx])
    except (TypeError, ValueError):
        return None


def _nw_bucket(target_lead: float) -> str:
    for name, lo, hi in NW_BUCKETS:
        lo_ok = True if lo is None else target_lead >= float(lo)
        hi_ok = True if hi is None else target_lead < float(hi)
        if lo_ok and hi_ok:
            return name
    return "other"


def _stake_proxy(
    *,
    has_l: bool,
    late_tier: Optional[int],
    late_hits: int,
) -> tuple[float, str]:
    """Approximate live stake multiplier (assumes known late hit count)."""
    if not has_l:
        return 0.5, "no_late_star"
    if late_hits < 2:
        return 0.5, "late_hits_lt_2"
    if late_tier == 60:
        return 0.5, "late_wr60"
    if late_tier is not None and late_tier >= 85:
        return 3.0, "late_wr>=85"
    if late_tier is not None and late_tier >= 75:
        return 2.0, "late_wr>=75"
    return 1.0, "late_wr_65_74"


def _decorate(d: dict) -> dict:
    return {
        k: {**v, "wr_pct": round(_wr(v), 2)}
        for k, v in sorted(d.items(), key=lambda kv: -kv[1]["matches"])
    }


def run_backtest(
    maps_path: str,
    stats_dir: Path,
    start_date_time: int,
    max_matches: Optional[int],
    output_path: Path,
    min_report: int = 50,
    patch: Optional[str] = None,
) -> dict:
    started_at = time.monotonic()
    maps_paths = _resolve_maps_paths(maps_path, patch=patch)
    print("=" * 80)
    print("BACKTEST v2: branches + metrics(|idx|) + networth + missed-edge")
    print(f"maps_path: {maps_path} -> {len(maps_paths)} file(s)")
    print(f"stats_dir: {stats_dir}")
    print(f"max_matches: {max_matches}")
    print(f"metric abs thresholds: {ABS_THRESHOLDS}")
    print(f"networth minutes: {NETWORTH_MINUTES}")
    print("=" * 80)

    early_dict, late_dict, _lane, post_lane_dict = _load_stats_dicts(
        stats_dir,
        include_dicts=True,
        include_lanes=False,
        post_lane_max_cached_shards=48,
    )
    _shrink_sqlite_caches(early_dict, late_dict, post_lane_dict)
    print(f"Loaded dicts (caches shrunk), RSS≈{_rss_mb():.0f}MB")

    all_branches: dict[str, dict] = defaultdict(_new_stat)
    dispatch_kept: dict[str, dict] = defaultdict(_new_stat)  # after same-sign + single>=65
    dispatch_kept_by_tier: dict[str, dict] = defaultdict(_new_stat)
    rejected_opposite: dict[str, dict] = defaultdict(_new_stat)
    rejected_single60: dict[str, dict] = defaultdict(_new_stat)
    block_exact_tier: dict[str, dict] = defaultdict(_new_stat)
    block_cumulative_tier: dict[str, dict] = defaultdict(_new_stat)

    # metric: key like "e_cp1vs1|abs>=7"
    metric_cumul: dict[str, dict] = defaultdict(_new_stat)
    # strong metric but no star dispatch at all
    missed_strong_metric: dict[str, dict] = defaultdict(_new_stat)

    # networth at minute × (kept dispatch / rejected)
    nw_kept: dict[str, dict] = defaultdict(_new_stat)       # "m10|lead_ge_800"
    nw_rejected_opp: dict[str, dict] = defaultdict(_new_stat)
    nw_by_branch: dict[str, dict] = defaultdict(_new_stat)  # "E+L+A|m10|lead_ge_800"

    stake_stats: dict[str, dict] = defaultdict(_new_stat)
    stake_by_branch: dict[str, dict] = defaultdict(_new_stat)

    # counterfactual: bet All side when L+A opposite (we currently drop)
    counterfactual_opp_bet_late: dict = _new_stat()
    counterfactual_opp_bet_all: dict = _new_stat()
    counterfactual_opp_bet_early: dict = _new_stat()

    total = 0
    no_draft_metrics = 0
    no_dispatch = 0

    for idx, record in enumerate(_iter_records(maps_paths, start_date_time, max_matches), 1):
        total = idx
        draft_keys = _draft_stats_lookup_keys(record["radiant_draft"], record["dire_draft"])
        early_lookup = _draft_scoped_stats_lookup(early_dict, draft_keys)
        late_lookup = _draft_scoped_stats_lookup(late_dict, draft_keys)
        post_lane_lookup = _draft_scoped_stats_lookup(post_lane_dict, draft_keys)
        metrics = synergy_and_counterpick(
            radiant_heroes_and_pos=record["radiant_draft"],
            dire_heroes_and_pos=record["dire_draft"],
            early_dict=early_lookup,
            mid_dict=late_lookup,
            post_lane_dict=post_lane_lookup,
        ) or {}
        if not metrics:
            no_draft_metrics += 1

        blocks_raw = {
            "early_output": metrics.get("early_output") or {},
            "mid_output": metrics.get("mid_output") or {},
            "all_output": _build_all_star_output(metrics.get("post_lane_output") or {}, None),
        }

        actual_winner = record.get("actual_winner")
        leads = record.get("radiant_networth_leads") or []

        tiers: dict[str, Optional[int]] = {}
        signs: dict[str, Optional[int]] = {}
        hits: dict[str, int] = {}
        for section, _lbl in BLOCKS:
            t, s, h = _block_max_tier(blocks_raw[section], section)
            tiers[section] = t
            signs[section] = s
            hits[section] = h

        present = {sec: tiers[sec] is not None for sec, _ in BLOCKS}
        present_signs = [signs[sec] for sec, _ in BLOCKS if present.get(sec) and signs[sec] in (1, -1)]
        agreement = "same_sign" if present_signs and len(set(present_signs)) == 1 else (
            "opposite" if len(set(present_signs)) > 1 else "none"
        )

        # --- per-metric cumulative abs thresholds ---
        any_strong_metric = False
        for section, _lbl in BLOCKS:
            prefix = BLOCK_PREFIX[section]
            block = blocks_raw[section]
            for metric_key, short in METRIC_KEYS:
                val = _coerce_metric_value(block.get(metric_key))
                if val is None or val == 0.0:
                    continue
                side = "radiant" if val > 0 else "dire"
                won = 1 if side == actual_winner else 0
                abs_v = abs(val)
                if abs_v >= 15:
                    any_strong_metric = True
                for thr in ABS_THRESHOLDS:
                    if abs_v >= thr:
                        _add(metric_cumul[f"{prefix}_{short}|abs>={thr}"], won)

        # --- block marginal ---
        for section, lbl in BLOCKS:
            t = tiers[section]
            s = signs[section]
            if t is None or s not in (1, -1):
                continue
            side = _sign_to_side(s)
            won = 1 if side == actual_winner else 0
            _add(block_exact_tier[f"{lbl}@wr{t}"], won)
            for wr in WR_LEVELS:
                if t >= wr:
                    _add(block_cumulative_tier[f"{lbl}>=wr{wr}"], won)

        # --- dispatch decision ---
        has_e = present.get("early_output", False)
        has_l = present.get("mid_output", False)
        has_a = present.get("all_output", False)
        branch_id = "  ".join(
            f"{lbl}:{_block_label(tiers[sec], signs[sec])}" for sec, lbl in BLOCKS
        )

        target_side: Optional[str] = None
        deciding_tier: Optional[int] = None
        deciding_section: Optional[str] = None
        for section in DISPATCH_PRIORITY:
            if present.get(section) and signs[section] in (1, -1):
                target_side = _sign_to_side(signs[section])
                deciding_tier = tiers[section]
                deciding_section = section
                break

        if target_side is None:
            no_dispatch += 1
            # missed: strong metric, no star block — bet strongest |metric| sign
            if any_strong_metric:
                best_side = None
                best_abs = 0.0
                for section, _lbl in BLOCKS:
                    for metric_key, _short in METRIC_KEYS:
                        val = _coerce_metric_value(blocks_raw[section].get(metric_key))
                        if val is None:
                            continue
                        if abs(val) > best_abs:
                            best_abs = abs(val)
                            best_side = "radiant" if val > 0 else "dire"
                if best_side is not None:
                    _add(
                        missed_strong_metric["strong_metric_no_star_block"],
                        1 if best_side == actual_winner else 0,
                    )
            if idx % 2000 == 0:
                print(f"  [{idx:>6}] RSS≈{_rss_mb():.0f}MB branches={len(all_branches)}", flush=True)
            continue

        won = 1 if target_side == actual_winner else 0
        _add(all_branches[branch_id], won)

        n_blocks = sum([has_e, has_l, has_a])
        single_below_65 = bool(n_blocks == 1 and deciding_tier is not None and deciding_tier < SINGLE_BLOCK_STAR_MIN_WR)

        # mode label
        if has_e and has_l and has_a:
            mode = "E+L+A"
        elif has_e and has_l:
            mode = "E+L"
        elif has_l and has_a:
            mode = "L+A"
        elif has_e and has_a:
            mode = "E+A"
        elif has_l:
            mode = "L_only"
        elif has_e:
            mode = "E_only"
        else:
            mode = "A_only"

        # networth samples (radiant lead → target lead)
        nw_at: dict[int, Optional[float]] = {}
        for minute in NETWORTH_MINUTES:
            r_lead = _lead_at_minute(leads, minute)
            if r_lead is None:
                nw_at[minute] = None
                continue
            t_lead = r_lead if target_side == "radiant" else -r_lead
            nw_at[minute] = t_lead

        def _record_nw(bucket_map: dict, prefix: str = "") -> None:
            for minute, t_lead in nw_at.items():
                if t_lead is None:
                    continue
                b = _nw_bucket(t_lead)
                key = f"{prefix}m{minute}|{b}" if prefix else f"m{minute}|{b}"
                _add(bucket_map[key], won)

        # pools
        if agreement == "opposite":
            _add(rejected_opposite[mode], won)
            _record_nw(nw_rejected_opp)
            # counterfactuals
            if has_l and signs["mid_output"] in (1, -1):
                _add(
                    counterfactual_opp_bet_late,
                    1 if _sign_to_side(signs["mid_output"]) == actual_winner else 0,
                )
            if has_a and signs["all_output"] in (1, -1):
                _add(
                    counterfactual_opp_bet_all,
                    1 if _sign_to_side(signs["all_output"]) == actual_winner else 0,
                )
            if has_e and signs["early_output"] in (1, -1):
                _add(
                    counterfactual_opp_bet_early,
                    1 if _sign_to_side(signs["early_output"]) == actual_winner else 0,
                )
        elif single_below_65:
            _add(rejected_single60[mode], won)
        elif agreement == "same_sign" or n_blocks == 1:
            # kept under current policy
            _add(dispatch_kept[mode], won)
            _add(dispatch_kept_by_tier[f"{mode}@wr{deciding_tier}"], won)
            _record_nw(nw_kept)
            _record_nw(nw_by_branch, prefix=f"{mode}|")

            mult, reason = _stake_proxy(
                has_l=has_l,
                late_tier=tiers.get("mid_output"),
                late_hits=hits.get("mid_output", 0),
            )
            label = {0.5: "x0.5", 1.0: "x1", 2.0: "x2", 3.0: "x3"}[mult]
            _add(stake_stats[f"{label}|{reason}"], won)
            _add(stake_by_branch[f"{label}|{mode}"], won)

        if idx % 2000 == 0:
            print(f"  [{idx:>6}] RSS≈{_rss_mb():.0f}MB branches={len(all_branches)}", flush=True)

    elapsed = time.monotonic() - started_at
    kept_n = sum(v["matches"] for v in dispatch_kept.values())
    result = {
        "summary": {
            "total_matches": total,
            "dispatched_raw": total - no_dispatch,
            "no_dispatch": no_dispatch,
            "no_draft_metrics": no_draft_metrics,
            "kept_same_sign_single65": kept_n,
            "rejected_opposite_n": sum(v["matches"] for v in rejected_opposite.values()),
            "rejected_single60_n": sum(v["matches"] for v in rejected_single60.values()),
            "unique_branches": len(all_branches),
            "elapsed_seconds": round(elapsed, 1),
            "rss_mb": round(_rss_mb(), 0),
            "filters": {
                "same_sign_required": True,
                "single_block_min_wr": SINGLE_BLOCK_STAR_MIN_WR,
            },
            "abs_thresholds": list(ABS_THRESHOLDS),
            "networth_minutes": list(NETWORTH_MINUTES),
        },
        "dispatch_kept": _decorate(dispatch_kept),
        "dispatch_kept_by_tier": _decorate(dispatch_kept_by_tier),
        "rejected_opposite": _decorate(rejected_opposite),
        "rejected_single60": _decorate(rejected_single60),
        "counterfactual_opposite": {
            "bet_late": {**counterfactual_opp_bet_late, "wr_pct": round(_wr(counterfactual_opp_bet_late), 2)},
            "bet_all": {**counterfactual_opp_bet_all, "wr_pct": round(_wr(counterfactual_opp_bet_all), 2)},
            "bet_early": {**counterfactual_opp_bet_early, "wr_pct": round(_wr(counterfactual_opp_bet_early), 2)},
        },
        "stake_stats": _decorate(stake_stats),
        "stake_by_branch": _decorate(stake_by_branch),
        "block_exact_tier": {
            k: {**v, "wr_pct": round(_wr(v), 2)}
            for k, v in sorted(block_exact_tier.items(), key=lambda kv: (kv[0][0], kv[0]))
        },
        "block_cumulative_tier": {
            k: {**v, "wr_pct": round(_wr(v), 2)}
            for k, v in sorted(block_cumulative_tier.items(), key=lambda kv: (kv[0][0], kv[0]))
        },
        "metric_abs_cumulative": {
            k: {**v, "wr_pct": round(_wr(v), 2)}
            for k, v in sorted(metric_cumul.items(), key=lambda kv: kv[0])
        },
        "missed_strong_metric": _decorate(missed_strong_metric),
        "networth_kept": _decorate(nw_kept),
        "networth_rejected_opposite": _decorate(nw_rejected_opp),
        "networth_by_branch": _decorate(nw_by_branch),
        "all_branches": _decorate(all_branches),
    }
    _dump_json(output_path, result)
    print(f"\nSaved: {output_path}")

    # ---- console highlights ----
    def _pt(title: str, rows, key_w: int = 28) -> None:
        print("\n" + "=" * 72)
        print(title)
        print("=" * 72)
        print(f"{'key':<{key_w}} {'N':>7} {'win%':>7}")
        print("-" * (key_w + 16))
        for k, st in rows:
            print(f"{k:<{key_w}} {st['matches']:>7} {_wr(st):>6.1f}%")

    _pt("KEPT DISPATCH (same-sign + single>=65)", sorted(dispatch_kept.items(), key=lambda kv: -kv[1]["matches"]), 12)
    _pt("REJECTED OPPOSITE", sorted(rejected_opposite.items(), key=lambda kv: -kv[1]["matches"]), 12)
    _pt(
        "COUNTERFACTUAL opposite (bet which side?)",
        [
            ("bet_late", counterfactual_opp_bet_late),
            ("bet_all", counterfactual_opp_bet_all),
            ("bet_early", counterfactual_opp_bet_early),
        ],
        12,
    )
    _pt("STAKE PROXY", sorted(stake_stats.items(), key=lambda kv: -kv[1]["matches"]), 28)

    # metric highlights at abs>=7
    metric_rows = [
        (k, v) for k, v in metric_cumul.items()
        if k.endswith("|abs>=7") and v["matches"] >= min_report
    ]
    metric_rows.sort(key=lambda kv: -_wr(kv[1]))
    _pt(f"METRICS abs>=7 (N>={min_report}), by win%", metric_rows[:40], 28)

    # networth at m10 for kept
    nw10 = [(k, v) for k, v in nw_kept.items() if k.startswith("m10|") and v["matches"] >= 30]
    nw10.sort(key=lambda kv: kv[0])
    _pt("NETWORTH @ min10 (kept dispatch)", nw10, 28)

    print(f"\nDone in {elapsed / 60:.1f} min, RSS≈{_rss_mb():.0f}MB")
    return result


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backtest v2: branches + metrics + networth")
    p.add_argument(
        "--maps-path",
        default=str(ROOT_DIR / "bets_data" / "analise_pub_matches" / "json_parts_split_from_object"),
    )
    p.add_argument("--stats-dir", default=str(DEFAULT_STATS_DIR))
    p.add_argument("--output", default=str(ROOT_DIR / "data" / "backtest_branches_v2.json"))
    p.add_argument("--patch", default=None)
    p.add_argument("--start-date-time", type=int, default=DEC_15_2025_UTC)
    p.add_argument("--max-matches", type=int, default=40000)
    p.add_argument("--min-report", type=int, default=50)
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    run_backtest(
        maps_path=args.maps_path,
        stats_dir=Path(args.stats_dir),
        start_date_time=args.start_date_time,
        max_matches=args.max_matches,
        output_path=Path(args.output),
        min_report=args.min_report,
        patch=args.patch,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
