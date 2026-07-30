#!/usr/bin/env python3
"""
Pure aggregation / calibration engine for instrumented STAR dispatch rows.

Consumes plain row dictionaries (planner schema). No I/O, no corpus scan,
no dependency on unfinished worker modules.
"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from typing import Any, Iterable, Mapping, Optional, Sequence

WILSON_Z = 1.959963984540054
PROBABILITY_CLIP = 1e-15

BLOCK_ORDER = ("E", "L", "A")
# Cumulative STAR WR thresholds commonly used in live/backtest code.
CUMULATIVE_TIER_THRESHOLDS = (60, 65, 70, 75, 80, 85, 90)

# Dispatch-minute buckets (closed-open style labels).
_MINUTE_BUCKETS = (
    ("m0_9", 0, 10),
    ("m10_14", 10, 15),
    ("m15_19", 15, 20),
    ("m20_27", 20, 28),
    ("m28_33", 28, 34),
    ("m34_plus", 34, None),
)

# Selected-side networth buckets aligned with planner contract.
_NW_BUCKETS = (
    ("le_neg_3000", None, -3000),  # (-inf, -3000]
    ("neg_3000_neg_1500", -3000, -1500),  # (-3000, -1500]
    ("neg_1500_neg_800", -1500, -800),
    ("neg_800_pos_800", -800, 800),  # (-800, 800)
    ("pos_800_1500", 800, 1500),
    ("pos_1500_3000", 1500, 3000),
    ("ge_3000", 3000, None),
)

_HIT_BANDS = (
    ("hits_0", 0, 1),
    ("hits_1", 1, 2),
    ("hits_2", 2, 3),
    ("hits_3_4", 3, 5),
    ("hits_ge_5", 5, None),
)


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------


def wilson_interval(
    wins: int,
    total: int,
    z: float = WILSON_Z,
) -> Optional[list[float]]:
    """Wilson score interval (two-sided) for binomial proportion; None if N=0."""
    if total is None or int(total) <= 0:
        return None
    n = int(total)
    w = int(wins)
    if w < 0:
        w = 0
    if w > n:
        w = n
    p = w / n
    zz = float(z) * float(z)
    den = 1.0 + zz / n
    center = (p + zz / (2.0 * n)) / den
    half = (
        float(z)
        * math.sqrt(p * (1.0 - p) / n + zz / (4.0 * n * n))
        / den
    )
    lo = center - half
    hi = center + half
    # Numerical clamp to [0, 1]
    if lo < 0.0:
        lo = 0.0
    if hi > 1.0:
        hi = 1.0
    return [lo, hi]


def empty_table(unit: str) -> dict[str, Any]:
    return {
        "unit": unit,
        "N": 0,
        "unique_map_n": 0,
        "W": 0,
        "L": 0,
        "WR": None,
        "wilson95": None,
        "missing_n": 0,
        "coverage_denominator": 0,
        "absent_n": 0,
    }


def _finalize_table(
    unit: str,
    wins: int,
    losses: int,
    map_ids: Iterable[Any],
    *,
    missing_n: int = 0,
    coverage_denominator: int = 0,
    absent_n: int = 0,
) -> dict[str, Any]:
    n = int(wins) + int(losses)
    unique = len({m for m in map_ids if m is not None})
    wr = (wins / n) if n > 0 else None
    return {
        "unit": unit,
        "N": n,
        "unique_map_n": unique,
        "W": int(wins),
        "L": int(losses),
        "WR": wr,
        "wilson95": wilson_interval(wins, n),
        "missing_n": int(missing_n),
        "coverage_denominator": int(coverage_denominator),
        "absent_n": int(absent_n),
    }


def summarize_outcomes(
    *,
    unit: str,
    outcomes: Sequence[Any],
    map_ids: Sequence[Any],
    missing_n: int = 0,
    coverage_denominator: int = 0,
    absent_n: int = 0,
) -> dict[str, Any]:
    """outcomes: sequence of bool/0/1 win flags (only decided rows)."""
    wins = 0
    losses = 0
    kept_maps: list[Any] = []
    for i, o in enumerate(outcomes):
        if o is True or o == 1:
            wins += 1
            kept_maps.append(map_ids[i] if i < len(map_ids) else None)
        elif o is False or o == 0:
            losses += 1
            kept_maps.append(map_ids[i] if i < len(map_ids) else None)
    return _finalize_table(
        unit,
        wins,
        losses,
        kept_maps,
        missing_n=missing_n,
        coverage_denominator=coverage_denominator,
        absent_n=absent_n,
    )


def dumps_deterministic(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# ---------------------------------------------------------------------------
# Row field accessors (tolerant of planner schema variants)
# ---------------------------------------------------------------------------


def _as_bool(v: Any) -> Optional[bool]:
    if v is True or v is False:
        return v
    if v is None:
        return None
    if v == 1 or v == "1" or v == "true" or v == "True":
        return True
    if v == 0 or v == "0" or v == "false" or v == "False":
        return False
    return None


def _get_blocks(row: Mapping[str, Any]) -> dict[str, dict]:
    raw = row.get("blocks")
    if not isinstance(raw, dict):
        raw = {}
    out: dict[str, dict] = {}
    for b in BLOCK_ORDER:
        # accept E/L/A or early/late/all aliases
        blk = raw.get(b)
        if blk is None:
            aliases = {
                "E": ("early", "early_output", "e"),
                "L": ("late", "mid_output", "l"),
                "A": ("all", "all_output", "a"),
            }[b]
            for a in aliases:
                if a in raw:
                    blk = raw[a]
                    break
        if not isinstance(blk, dict):
            blk = {"present": False}
        present = bool(blk.get("present"))
        if not present and blk.get("tier") is not None and blk.get("sign") in (1, -1):
            # tolerate implicit presence
            present = True
        out[b] = {
            "present": present,
            "side": blk.get("side"),
            "sign": blk.get("sign"),
            "tier": blk.get("tier"),
            "hit_count": blk.get("hit_count") if blk.get("hit_count") is not None else blk.get("support_count"),
            "won": _as_bool(blk.get("won")),
            "metric_strength": blk.get("metric_strength"),
        }
    return out


def block_presence_combo(blocks: Mapping[str, Mapping[str, Any]]) -> str:
    parts = [b for b in BLOCK_ORDER if bool((blocks.get(b) or {}).get("present"))]
    return "+".join(parts) if parts else "none"


def sign_agreement_pattern(blocks: Mapping[str, Mapping[str, Any]]) -> str:
    signs: list[int] = []
    for b in BLOCK_ORDER:
        blk = blocks.get(b) or {}
        if not blk.get("present"):
            continue
        s = blk.get("sign")
        if s in (1, -1):
            signs.append(int(s))
        elif blk.get("side") in ("radiant", "r", "R"):
            signs.append(1)
        elif blk.get("side") in ("dire", "d", "D"):
            signs.append(-1)
    if not signs:
        return "none"
    if len(signs) == 1:
        return "single"
    uniq = set(signs)
    if len(uniq) == 1:
        return "same_sign"
    return "opposite"


def dispatch_minute_bucket(minute: Any) -> str:
    if minute is None:
        return "missing"
    try:
        m = int(minute)
    except (TypeError, ValueError):
        return "invalid"
    for name, lo, hi in _MINUTE_BUCKETS:
        if m < lo:
            continue
        if hi is None or m < hi:
            return name
    return "m34_plus"


def selected_side_networth_bucket(lead: Any, explicit: Any = None) -> str:
    if explicit is not None and explicit != "":
        return str(explicit)
    if lead is None:
        return "missing"
    try:
        v = float(lead)
    except (TypeError, ValueError):
        return "invalid"
    # Planner: (-inf,-3000], (-3000,-1500], (-1500,-800], (-800,800), [800,1500), [1500,3000), [3000,inf)
    if v <= -3000:
        return "le_neg_3000"
    if v <= -1500:
        return "neg_3000_neg_1500"
    if v <= -800:
        return "neg_1500_neg_800"
    if v < 800:
        return "neg_800_pos_800"
    if v < 1500:
        return "pos_800_1500"
    if v < 3000:
        return "pos_1500_3000"
    return "ge_3000"


def support_hit_count_band(hit_count: Any) -> str:
    if hit_count is None:
        return "missing"
    try:
        h = int(hit_count)
    except (TypeError, ValueError):
        return "invalid"
    for name, lo, hi in _HIT_BANDS:
        if h < lo:
            continue
        if hi is None or h < hi:
            return name
    return "hits_ge_5"


def _patch_key(patch: Any) -> str:
    if patch is None or patch == "":
        return "<null>"
    return str(patch)


def _time_slice_key(start: Any, *, width: int = 86400 * 7) -> str:
    """Coarse chronological time slice (week buckets by default)."""
    try:
        s = int(start)
    except (TypeError, ValueError):
        return "invalid"
    if width <= 0:
        return str(s)
    bucket = s // width
    return f"t{bucket}"


def _tier_int(tier: Any) -> Optional[int]:
    if tier is None:
        return None
    try:
        return int(tier)
    except (TypeError, ValueError):
        return None


def _nominal_p_from_tier(tier: Any) -> Optional[float]:
    """Map STAR tier representation to nominal probability when valid.

    Existing representation is an integer WR percent (60, 65, …). Nominal p = tier/100.
    Returns None when mapping is unavailable (never invent).
    """
    t = _tier_int(tier)
    if t is None:
        return None
    if t <= 0 or t >= 100:
        # 0 or 100+ not a valid open probability from this representation
        if t == 100:
            return None  # degenerate; refuse to invent clip
        if t <= 0:
            return None
    return float(t) / 100.0


# ---------------------------------------------------------------------------
# Accumulators
# ---------------------------------------------------------------------------


class _Acc:
    __slots__ = ("wins", "losses", "maps", "missing", "absent")

    def __init__(self) -> None:
        self.wins = 0
        self.losses = 0
        self.maps: set[Any] = set()
        self.missing = 0
        self.absent = 0

    def add_outcome(self, won: Optional[bool], map_id: Any) -> None:
        if won is True:
            self.wins += 1
            if map_id is not None:
                self.maps.add(map_id)
        elif won is False:
            self.losses += 1
            if map_id is not None:
                self.maps.add(map_id)
        else:
            self.missing += 1

    def add_absent(self) -> None:
        self.absent += 1

    def table(self, unit: str, coverage_denominator: int) -> dict[str, Any]:
        return _finalize_table(
            unit,
            self.wins,
            self.losses,
            self.maps,
            missing_n=self.missing,
            coverage_denominator=coverage_denominator,
            absent_n=self.absent,
        )


def _sorted_table_map(d: Mapping[str, dict], unit: str, coverage: int) -> dict[str, dict]:
    return {k: v.table(unit, coverage) for k, v in sorted(d.items(), key=lambda kv: str(kv[0]))}


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------


def _clip_p(p: float, eps: float = PROBABILITY_CLIP) -> float:
    if p < eps:
        return eps
    if p > 1.0 - eps:
        return 1.0 - eps
    return p


def calibration_for_block(
    rows: Sequence[Mapping[str, Any]],
    block: str,
    *,
    probability_clip: float = PROBABILITY_CLIP,
) -> dict[str, Any]:
    """Empirical WR vs nominal tier probability for one block across rows."""
    ys: list[float] = []
    ps: list[float] = []
    unavailable_reasons: list[str] = []
    for row in rows:
        blocks = _get_blocks(row)
        blk = blocks.get(block) or {}
        if not blk.get("present"):
            continue
        won = blk.get("won")
        if won is None:
            continue
        p = _nominal_p_from_tier(blk.get("tier"))
        if p is None:
            unavailable_reasons.append("no_valid_nominal_tier_probability")
            continue
        ys.append(1.0 if won else 0.0)
        ps.append(p)

    meta = {
        "probability_clip": probability_clip,
        "nominal_mapping": "tier_pct_over_100",
        "wilson_z": WILSON_Z,
    }

    if not ys:
        reason = (
            unavailable_reasons[0]
            if unavailable_reasons
            else "no_present_block_with_outcome_and_valid_nominal"
        )
        return {
            "available": False,
            "metrics": None,
            "unavailable_reason": reason,
            "metadata": meta,
        }

    n = len(ys)
    emp = sum(ys) / n
    mean_p = sum(ps) / n
    gap = emp - mean_p
    brier = sum((p - y) ** 2 for p, y in zip(ps, ys)) / n
    log_loss = 0.0
    for p, y in zip(ps, ys):
        pc = _clip_p(p, probability_clip)
        log_loss -= y * math.log(pc) + (1.0 - y) * math.log(1.0 - pc)
    log_loss /= n

    return {
        "available": True,
        "metrics": {
            "N": n,
            "empirical_WR": emp,
            "mean_nominal_p": mean_p,
            "calibration_gap": gap,
            "brier": brier,
            "log_loss": log_loss,
        },
        "unavailable_reason": None,
        "metadata": meta,
    }


# ---------------------------------------------------------------------------
# Temporal splits
# ---------------------------------------------------------------------------


def _sort_key(row: Mapping[str, Any]) -> tuple:
    try:
        start = int(row.get("startDateTime") or 0)
    except (TypeError, ValueError):
        start = 0
    mid = row.get("map_id")
    try:
        mid_k: Any = int(mid) if mid is not None else 0
    except (TypeError, ValueError):
        mid_k = str(mid)
    eid = str(row.get("event_id") or "")
    return (start, mid_k, eid)


def assign_temporal_splits(
    rows: Sequence[Mapping[str, Any]],
    *,
    train_frac: float = 0.6,
    cal_frac: float = 0.2,
    test_frac: float = 0.2,
    frozen_dicts_overlap_corpus: bool = True,
) -> dict[str, Any]:
    """Predeclared 60/20/20 chronological splits without using outcomes.

    Boundaries computed on sorted (startDateTime, map_id). When frozen
    cumulative dictionaries overlap the corpus, all splits are labelled
    diagnostic/in-sample (never OOS).
    """
    total_frac = train_frac + cal_frac + test_frac
    if abs(total_frac - 1.0) > 1e-9:
        # normalize defensively
        train_frac, cal_frac, test_frac = (
            train_frac / total_frac,
            cal_frac / total_frac,
            test_frac / total_frac,
        )

    ordered = sorted(rows, key=_sort_key)
    n = len(ordered)
    n_train = int(n * train_frac)
    n_cal = int(n * cal_frac)
    # remainder goes to final_test to keep sum == n
    n_test = n - n_train - n_cal
    # edge: ensure non-empty middle/test when n large enough
    if n >= 5 and n_cal == 0 and cal_frac > 0:
        n_cal = 1
        if n_train + n_cal > n:
            n_train = max(0, n - n_cal - max(n_test, 0))
        n_test = n - n_train - n_cal
    if n >= 5 and n_test == 0 and test_frac > 0:
        n_test = 1
        if n_train + n_cal + n_test > n:
            n_cal = max(0, n - n_train - n_test)
        n_test = n - n_train - n_cal

    train_rows = ordered[:n_train]
    cal_rows = ordered[n_train : n_train + n_cal]
    test_rows = ordered[n_train + n_cal :]

    def _start(r: Mapping[str, Any]) -> Optional[int]:
        try:
            return int(r.get("startDateTime"))
        except (TypeError, ValueError):
            return None

    cutoffs = {
        "train_end_startDateTime": _start(train_rows[-1]) if train_rows else None,
        "calibration_end_startDateTime": _start(cal_rows[-1]) if cal_rows else None,
        "final_test_start_startDateTime": _start(test_rows[0]) if test_rows else None,
    }

    row_split: dict[str, str] = {}
    for r in train_rows:
        row_split[str(r.get("event_id"))] = "train"
    for r in cal_rows:
        row_split[str(r.get("event_id"))] = "calibration"
    for r in test_rows:
        row_split[str(r.get("event_id"))] = "final_test"

    if frozen_dicts_overlap_corpus:
        label = "diagnostic_in_sample"
        oos = False
        gap = {
            "reason": (
                "Frozen cumulative STAR dictionaries overlap the evaluated corpus; "
                "chronological splits remain diagnostic/in-sample and must not be "
                "labelled out-of-sample edge."
            ),
            "walk_forward_requirement": (
                "Build time-frozen dictionaries whose training cutoff strictly precedes "
                "each evaluation row's startDateTime; re-run walk-forward with "
                "no dictionary leakage across the same cutoffs recorded here."
            ),
            "recorded_cutoffs": dict(cutoffs),
        }
    else:
        label = "temporal_holdout_candidate"
        oos = True
        gap = {
            "reason": "Caller asserted dictionaries do not overlap; still verify dict build cutoffs.",
            "walk_forward_requirement": (
                "Confirm each dictionary snapshot is frozen at or before the split cutoff "
                "before claiming OOS."
            ),
            "recorded_cutoffs": dict(cutoffs),
        }

    return {
        "ordering": ["startDateTime", "map_id"],
        "fractions": {
            "train": train_frac,
            "calibration": cal_frac,
            "final_test": test_frac,
        },
        "counts": {
            "train": len(train_rows),
            "calibration": len(cal_rows),
            "final_test": len(test_rows),
            "total": n,
        },
        "cutoffs": cutoffs,
        "label": label,
        "oos": oos,
        "gap": gap,
        "row_split_by_event_id": row_split,
    }


# ---------------------------------------------------------------------------
# Main aggregation
# ---------------------------------------------------------------------------


def aggregate_dispatch_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    frozen_dicts_overlap_corpus: bool = True,
) -> dict[str, Any]:
    """Aggregate map/event and per-present-E/L/A branch metrics.

    Units are always explicit. Absent blocks are excluded from that block's
    denominator and counted as absent, never as losses. All observed groups
    are emitted (not only high-WR). Sort order is deterministic.
    """
    rows_seen = len(rows)
    invalid_n = 0
    map_ended_n = 0
    silent_drop = 0

    # event / map level
    event_outcomes: list[Optional[bool]] = []
    event_maps: list[Any] = []
    map_first_outcome: dict[Any, Optional[bool]] = {}
    events_per_map: dict[Any, int] = defaultdict(int)

    # per-block overall
    block_acc: dict[str, _Acc] = {b: _Acc() for b in BLOCK_ORDER}
    block_exact: dict[str, dict[str, _Acc]] = {b: defaultdict(_Acc) for b in BLOCK_ORDER}
    block_cumul: dict[str, dict[str, _Acc]] = {b: defaultdict(_Acc) for b in BLOCK_ORDER}

    # dimensions (event-level, selected/dispatch perspective)
    dim_keys = (
        "block_presence_combo",
        "sign_agreement",
        "deciding_block",
        "dispatch_minute_bucket",
        "selected_side_networth_bucket",
        "patch",
        "time_slice",
        "support_hit_count_band",
        "metric_strength_band",
        "exact_tier",
        "cumulative_tier",
    )
    dim_acc: dict[str, dict[str, _Acc]] = {k: defaultdict(_Acc) for k in dim_keys}

    # also per-block dimension breakdowns for exact/cumulative already above;
    # selected-side outcome for dimension tables
    coverage = rows_seen

    for row in rows:
        if row.get("invalid"):
            invalid_n += 1
        if row.get("map_ended"):
            map_ended_n += 1

        map_id = row.get("map_id")
        events_per_map[map_id] += 1

        blocks = _get_blocks(row)
        combo = block_presence_combo(blocks)
        agreement = sign_agreement_pattern(blocks)
        deciding = str(row.get("deciding_block") or row.get("selected_block") or "unknown")
        minute_b = dispatch_minute_bucket(row.get("dispatch_minute"))
        nw_b = selected_side_networth_bucket(
            row.get("selected_side_lead"),
            row.get("selected_side_networth_bucket"),
        )
        patch_k = _patch_key(row.get("patch"))
        tslice = _time_slice_key(row.get("startDateTime"))
        msb = row.get("metric_strength_band")

        # selected-side outcome for event-level tables
        selected_side = row.get("selected_side")
        final_winner = row.get("final_winner")
        selected_won: Optional[bool]
        if selected_side in (None, "") or final_winner in (None, ""):
            selected_won = None
        else:
            selected_won = str(selected_side) == str(final_winner)

        # if deciding block has explicit won, prefer that for event outcome
        dec_blk = blocks.get(deciding) if deciding in blocks else None
        if dec_blk and dec_blk.get("present") and dec_blk.get("won") is not None:
            event_won = dec_blk.get("won")
        else:
            event_won = selected_won

        event_outcomes.append(event_won)
        event_maps.append(map_id)
        if map_id not in map_first_outcome:
            map_first_outcome[map_id] = event_won

        # support/hit from deciding block if present else max hit among present
        hit_src = None
        if dec_blk and dec_blk.get("present"):
            hit_src = dec_blk.get("hit_count")
        else:
            for b in BLOCK_ORDER:
                if blocks[b]["present"] and blocks[b].get("hit_count") is not None:
                    hit_src = blocks[b]["hit_count"]
                    break
        hit_band = support_hit_count_band(hit_src)

        # dimension accumulators (event unit)
        dim_acc["block_presence_combo"][combo].add_outcome(event_won, map_id)
        dim_acc["sign_agreement"][agreement].add_outcome(event_won, map_id)
        dim_acc["deciding_block"][deciding].add_outcome(event_won, map_id)
        dim_acc["dispatch_minute_bucket"][minute_b].add_outcome(event_won, map_id)
        dim_acc["selected_side_networth_bucket"][nw_b].add_outcome(event_won, map_id)
        dim_acc["patch"][patch_k].add_outcome(event_won, map_id)
        dim_acc["time_slice"][tslice].add_outcome(event_won, map_id)
        dim_acc["support_hit_count_band"][hit_band].add_outcome(event_won, map_id)
        if msb is not None and msb != "":
            dim_acc["metric_strength_band"][str(msb)].add_outcome(event_won, map_id)

        # exact/cumulative tier on deciding block when available
        if dec_blk and dec_blk.get("present"):
            t = _tier_int(dec_blk.get("tier"))
            if t is not None:
                dim_acc["exact_tier"][str(t)].add_outcome(event_won, map_id)
                for thr in CUMULATIVE_TIER_THRESHOLDS:
                    if t >= thr:
                        dim_acc["cumulative_tier"][f">={thr}"].add_outcome(event_won, map_id)

        # per-block independent outcomes
        for b in BLOCK_ORDER:
            blk = blocks[b]
            if not blk["present"]:
                block_acc[b].add_absent()
                continue
            won = blk.get("won")
            block_acc[b].add_outcome(won, map_id)
            t = _tier_int(blk.get("tier"))
            if t is not None and won is not None:
                block_exact[b][str(t)].add_outcome(won, map_id)
                for thr in CUMULATIVE_TIER_THRESHOLDS:
                    if t >= thr:
                        block_cumul[b][f">={thr}"].add_outcome(won, map_id)
            elif t is not None and won is None:
                block_exact[b][str(t)].missing += 1
            elif t is None and won is not None:
                # present without tier — still counted in overall; exact key "unknown"
                block_exact[b]["unknown"].add_outcome(won, map_id)

    # event summary tables
    decided = [(o, m) for o, m in zip(event_outcomes, event_maps) if o is True or o is False]
    missing_events = sum(1 for o in event_outcomes if o is not True and o is not False)
    all_events = summarize_outcomes(
        unit="dispatch_event",
        outcomes=[o for o, _ in decided],
        map_ids=[m for _, m in decided],
        missing_n=missing_events,
        coverage_denominator=coverage,
    )

    multi_maps = {m for m, c in events_per_map.items() if c >= 2}
    multi_event_rows = [
        (o, m)
        for o, m in zip(event_outcomes, event_maps)
        if m in multi_maps and (o is True or o is False)
    ]
    multi_table = summarize_outcomes(
        unit="dispatch_event",
        outcomes=[o for o, _ in multi_event_rows],
        map_ids=[m for _, m in multi_event_rows],
        missing_n=sum(1 for o, m in zip(event_outcomes, event_maps) if m in multi_maps and o is not True and o is not False),
        coverage_denominator=sum(1 for m in event_maps if m in multi_maps),
    )
    multi_table["unique_map_n"] = len(multi_maps)

    map_outcomes = list(map_first_outcome.values())
    map_ids_u = list(map_first_outcome.keys())
    map_decided = [(o, m) for o, m in zip(map_outcomes, map_ids_u) if o is True or o is False]
    map_table = summarize_outcomes(
        unit="unique_map",
        outcomes=[o for o, _ in map_decided],
        map_ids=[m for _, m in map_decided],
        missing_n=sum(1 for o in map_outcomes if o is not True and o is not False),
        coverage_denominator=len(map_ids_u),
    )

    by_block: dict[str, Any] = {}
    for b in BLOCK_ORDER:
        overall = block_acc[b].table("dispatch_event", coverage)
        # ensure cumulative keys for thresholds that appeared via any block tier
        # include zero-N thresholds only when some higher/lower observed? Spec:
        # include all observed groups. So only keys that received add_outcome or missing.
        by_block[b] = {
            "overall": overall,
            "by_exact_tier": _sorted_table_map(block_exact[b], "dispatch_event", coverage),
            "by_cumulative_tier": _sorted_table_map(block_cumul[b], "dispatch_event", coverage),
        }
        # attach absent_n on overall explicitly (already in table)
        by_block[b]["overall"]["absent_n"] = block_acc[b].absent

    dimensions = {
        k: _sorted_table_map(v, "dispatch_event", coverage)
        for k, v in dim_acc.items()
        if k != "metric_strength_band" or v  # only when supplied
    }
    # metric_strength_band may be empty — omit if nothing supplied
    if not dim_acc["metric_strength_band"]:
        dimensions["metric_strength_band"] = {}

    calibration = {
        "by_block": {b: calibration_for_block(rows, b) for b in BLOCK_ORDER},
        "metadata": {
            "probability_clip": PROBABILITY_CLIP,
            "nominal_mapping": "tier_pct_over_100",
            "wilson_z": WILSON_Z,
        },
    }

    splits = assign_temporal_splits(
        rows,
        train_frac=0.6,
        cal_frac=0.2,
        test_frac=0.2,
        frozen_dicts_overlap_corpus=frozen_dicts_overlap_corpus,
    )

    result = {
        "meta": {
            "wilson_z": WILSON_Z,
            "probability_clip": PROBABILITY_CLIP,
            "block_order": list(BLOCK_ORDER),
            "cumulative_tier_thresholds": list(CUMULATIVE_TIER_THRESHOLDS),
            "units": {
                "dispatch_event": "one row / dispatch opportunity",
                "unique_map": "one row per distinct map_id (first event outcome)",
            },
        },
        "field_quality": {
            "rows_seen": rows_seen,
            "invalid_n": invalid_n,
            "map_ended_n": map_ended_n,
            "rows_dropped_silent": silent_drop,
            "missing_selected_outcome_n": missing_events,
        },
        "event_summaries": {
            "all_dispatch_events": all_events,
            "multi_event_maps": multi_table,
        },
        "map_summaries": {
            "unique_maps_with_dispatch": map_table,
        },
        "by_block": by_block,
        "dimensions": dimensions,
        "calibration": calibration,
        "temporal_splits": splits,
    }
    return result


def write_fixture_sample(
    rows: Sequence[Mapping[str, Any]],
    staging_dir: str | Path,
) -> dict[str, Any]:
    """Write deterministic fixture aggregate under staging/metrics (optional helper)."""
    from pathlib import Path as _P

    out_dir = _P(staging_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    agg = aggregate_dispatch_metrics(rows)
    path = out_dir / "fixture_aggregate.json"
    tmp = out_dir / "fixture_aggregate.json.tmp"
    payload = dumps_deterministic(agg)
    tmp.write_text(payload + "\n", encoding="utf-8")
    tmp.replace(path)
    return {"path": str(path), "aggregate": agg}


__all__ = [
    "WILSON_Z",
    "PROBABILITY_CLIP",
    "aggregate_dispatch_metrics",
    "assign_temporal_splits",
    "block_presence_combo",
    "calibration_for_block",
    "dispatch_minute_bucket",
    "dumps_deterministic",
    "empty_table",
    "selected_side_networth_bucket",
    "sign_agreement_pattern",
    "summarize_outcomes",
    "support_hit_count_band",
    "wilson_interval",
    "write_fixture_sample",
]
