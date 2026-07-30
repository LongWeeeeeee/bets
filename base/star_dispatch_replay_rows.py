#!/usr/bin/env python3
"""Deterministic unique-map STAR dispatch row extraction (pure layer).

Reuses backtest block/priority semantics without mutating source maps or
dictionaries. No production calls, no live probes, no full corpus run.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, MutableMapping, Optional, Sequence

# Mirror base/backtest_dispatch_branches.py contracts (read-only reuse of names).
BLOCKS: tuple[tuple[str, str], ...] = (
    ("early_output", "E"),
    ("mid_output", "L"),
    ("all_output", "A"),
)
BLOCK_LABELS: dict[str, str] = {
    "early_output": "E",
    "mid_output": "L",
    "all_output": "A",
}
LABEL_TO_SECTION: dict[str, str] = {lbl: sec for sec, lbl in BLOCKS}
DISPATCH_PRIORITY: tuple[str, ...] = ("mid_output", "early_output", "all_output")
CHECKPOINT_MINUTES: tuple[int, ...] = (6, 10, 12, 15, 20, 27, 34, 35, 40)
M34_ELIGIBLE_SECONDS: int = 34 * 60  # 2040
DEFAULT_MAX_UNIQUE: int = 40_000
TIME_KIND_CHECKPOINT: str = "checkpoint"
TREND_INTERVAL: tuple[int, int] = (10, 20)

# Non-overlapping lead buckets (selected-side or radiant perspective).
# (-inf,-3000], (-3000,-1500], (-1500,-800], (-800,800), [800,1500), [1500,3000), [3000,inf)
LEAD_BUCKETS: tuple[tuple[str, Optional[float], Optional[float], str, str], ...] = (
    # name, lo, hi, lo_op ('<'|'<='), hi_op ('<'|'<=')
    ("(-inf,-3000]", None, -3000.0, "open", "<="),
    ("(-3000,-1500]", -3000.0, -1500.0, "<", "<="),
    ("(-1500,-800]", -1500.0, -800.0, "<", "<="),
    ("(-800,800)", -800.0, 800.0, "<", "<"),
    ("[800,1500)", 800.0, 1500.0, "<=", "<"),
    ("[1500,3000)", 1500.0, 3000.0, "<=", "<"),
    ("[3000,inf)", 3000.0, None, "<=", "open"),
)

ROOT_DIR = Path(__file__).resolve().parent.parent
STAGING_ROWS_DIR = ROOT_DIR / "runtime" / "star_dispatch_replay" / "staging" / "rows"

MAP_ROW_KEYS = (
    "map_id",
    "startDateTime",
    "source_shard",
    "patch",
    "patch_unavailable_reason",
    "durationSeconds",
    "final_winner",
    "checkpoints",
    "trend",
    "m34",
)

DISPATCH_ROW_KEYS = (
    "event_id",
    "map_id",
    "startDateTime",
    "source_shard",
    "patch",
    "final_winner",
    "durationSeconds",
    "time_kind",
    "actual_send_time",
    "dispatch_minute",
    "dispatch_seconds",
    "selected_side",
    "policy_reason",
    "deciding_block",
    "dispatch_exists",
    "blocks",
    "dispatch_checkpoint",
    "checkpoints",
)


def empty_iteration_counts() -> dict[str, int]:
    return {
        "raw_seen": 0,
        "duplicate_skipped": 0,
        "invalid_id": 0,
        "unique_accepted": 0,
        "quarantined": 0,
    }


def canonicalize_map_id(value: Any) -> Optional[int]:
    """Stable scalar map id. Accepts int-like values only."""
    if value is None or isinstance(value, (dict, list, set, tuple, bool)):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        if value.is_integer() and value >= 0:
            return int(value)
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s or not re.fullmatch(r"[0-9]+", s):
            return None
        try:
            return int(s)
        except ValueError:
            return None
    return None


def _cmp_lo(value: float, lo: Optional[float], op: str) -> bool:
    if lo is None or op == "open":
        return True
    if op == "<":
        return value > lo
    if op == "<=":
        return value >= lo
    raise ValueError(op)


def _cmp_hi(value: float, hi: Optional[float], op: str) -> bool:
    if hi is None or op == "open":
        return True
    if op == "<":
        return value < hi
    if op == "<=":
        return value <= hi
    raise ValueError(op)


def lead_bucket(lead: float) -> str:
    """Non-overlapping bucket label for a numeric lead."""
    x = float(lead)
    for name, lo, hi, lo_op, hi_op in LEAD_BUCKETS:
        if _cmp_lo(x, lo, lo_op) and _cmp_hi(x, hi, hi_op):
            return name
    return "other"


def convert_selected_side_lead(
    radiant_lead: Optional[float],
    selected_side: Optional[str],
) -> Optional[float]:
    if radiant_lead is None or selected_side not in ("radiant", "dire"):
        return None
    r = float(radiant_lead)
    return r if selected_side == "radiant" else -r


def _lead_at_minute(leads: Sequence[Any], minute: int) -> Optional[float]:
    """radiantNetworthLeads[minute-1] == radiant lead at that minute."""
    if not leads or minute < 1:
        return None
    idx = minute - 1
    if idx >= len(leads):
        return None
    raw = leads[idx]
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def checkpoint_observability(
    leads: Sequence[Any],
    *,
    duration_seconds: Optional[int],
    minute: int,
) -> dict[str, Any]:
    """Return state observed|map_ended|missing for one checkpoint minute."""
    minute = int(minute)
    dur = None if duration_seconds is None else int(duration_seconds)
    threshold = minute * 60
    if dur is not None and dur < threshold:
        return {"state": "map_ended", "radiant_lead": None, "minute": minute}
    lead = _lead_at_minute(leads, minute)
    if lead is None:
        return {"state": "missing", "radiant_lead": None, "minute": minute}
    return {"state": "observed", "radiant_lead": float(lead), "minute": minute}


def m34_eligibility(
    *,
    duration_seconds: Optional[int],
    m34_state: str,
) -> dict[str, Any]:
    """Eligibility uses only duration + m34 observability (no final outcome)."""
    dur = 0 if duration_seconds is None else int(duration_seconds)
    died = dur < M34_ELIGIBLE_SECONDS
    state = str(m34_state or "")
    missing = (not died) and state != "observed"
    eligible = (not died) and state == "observed"
    return {
        "eligible": bool(eligible),
        "died_before_34": bool(died),
        "missing_m34": bool(missing),
        "durationSeconds": dur,
        "m34_state": state,
        "threshold_seconds": M34_ELIGIBLE_SECONDS,
    }


def _sign_to_side(sign: Optional[int]) -> Optional[str]:
    if sign == 1:
        return "radiant"
    if sign == -1:
        return "dire"
    return None


def _side_to_sign(side: Optional[str]) -> Optional[int]:
    if side == "radiant":
        return 1
    if side == "dire":
        return -1
    return None


def _final_winner_from_match(match: Mapping[str, Any]) -> Optional[str]:
    if "final_winner" in match and match["final_winner"] in ("radiant", "dire"):
        return str(match["final_winner"])
    if "actual_winner" in match and match["actual_winner"] in ("radiant", "dire"):
        return str(match["actual_winner"])
    if "didRadiantWin" in match:
        val = match.get("didRadiantWin")
        if val is None:
            return None
        return "radiant" if bool(val) else "dire"
    return None


def _extract_patch(
    match: Mapping[str, Any],
    *,
    source_shard: Optional[str],
    patch_hint: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    for key in ("patch", "patchVersion", "game_patch"):
        val = match.get(key)
        if val is not None and str(val).strip():
            return str(val).strip(), None
    if patch_hint is not None and str(patch_hint).strip():
        return str(patch_hint).strip(), None
    if source_shard:
        name = Path(str(source_shard)).name
        m = re.match(r"([0-9]+\.[0-9]+[a-zA-Z]?)", name)
        if m:
            return m.group(1), "inferred_from_shard_name"
    return None, "missing_on_source"


def _normalize_block_entry(raw: Any, label: str) -> dict[str, Any]:
    """Normalize a single E/L/A block description to present/side/sign/tier/hit_count."""
    absent = {
        "present": False,
        "side": None,
        "sign": None,
        "tier": None,
        "hit_count": None,
        "won": None,
        "label": label,
    }
    if not isinstance(raw, Mapping):
        return absent
    present = raw.get("present")
    sign = raw.get("sign")
    side = raw.get("side")
    tier = raw.get("tier", raw.get("exact_tier"))
    hits = raw.get("hit_count", raw.get("support"))

    if present is False:
        return absent

    if side in ("radiant", "dire") and sign not in (1, -1):
        sign = _side_to_sign(side)
    if sign in (1, -1) and side not in ("radiant", "dire"):
        side = _sign_to_side(int(sign))

    # Present only with a decisive sign.
    if present is True or (sign in (1, -1) and side in ("radiant", "dire")):
        if sign not in (1, -1) or side not in ("radiant", "dire"):
            return absent
        try:
            tier_i = int(tier) if tier is not None else None
        except (TypeError, ValueError):
            tier_i = None
        try:
            hits_i = int(hits) if hits is not None else None
        except (TypeError, ValueError):
            hits_i = None
        return {
            "present": True,
            "side": side,
            "sign": int(sign),
            "tier": tier_i,
            "hit_count": hits_i,
            "won": None,  # filled later
            "label": label,
        }
    return absent


def normalize_blocks(blocks: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Return E/L/A dicts; absent blocks have null value fields."""
    src = blocks if isinstance(blocks, Mapping) else {}
    out: dict[str, dict[str, Any]] = {}
    for _section, label in BLOCKS:
        # Accept either label keys (E/L/A) or section keys.
        raw = src.get(label)
        if raw is None:
            raw = src.get(_section)
        out[label] = _normalize_block_entry(raw, label)
    return out


def select_dispatch_target(
    blocks: Mapping[str, Mapping[str, Any]],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (selected_side, deciding_block_label, policy_reason) via DISPATCH_PRIORITY."""
    for section in DISPATCH_PRIORITY:
        label = BLOCK_LABELS[section]
        b = blocks.get(label) or {}
        if b.get("present") and b.get("side") in ("radiant", "dire"):
            return str(b["side"]), label, f"priority:{label}"
    return None, None, "no_star_block"


def event_id_for_dispatch(
    *,
    map_id: int,
    dispatch_minute: Optional[int],
    selected_side: Optional[str],
    deciding_block: Optional[str],
) -> str:
    """Deterministic event id (not a live send id)."""
    payload = (
        f"{int(map_id)}|"
        f"{'' if dispatch_minute is None else int(dispatch_minute)}|"
        f"{selected_side or ''}|"
        f"{deciding_block or ''}"
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"evt_{int(map_id)}_{digest}"


def _independent_won(side: Optional[str], final_winner: Optional[str]) -> Optional[bool]:
    if side not in ("radiant", "dire") or final_winner not in ("radiant", "dire"):
        return None
    return side == final_winner


def _trend_from_checkpoints(checkpoints: Mapping[Any, Mapping[str, Any]]) -> dict[str, Any]:
    start_m, end_m = TREND_INTERVAL
    a = checkpoints.get(start_m) or checkpoints.get(str(start_m)) or {}
    b = checkpoints.get(end_m) or checkpoints.get(str(end_m)) or {}
    base = {
        "interval": [start_m, end_m],
        "delta_radiant": None,
        "state": "unavailable",
        "reason": None,
    }
    sa = a.get("state")
    sb = b.get("state")
    if sa == "map_ended" or sb == "map_ended":
        base["state"] = "map_ended"
        base["reason"] = "map_ended_before_interval_end"
        return base
    if sa != "observed" or sb != "observed":
        base["state"] = "missing"
        base["reason"] = f"endpoint_not_observed:{sa}/{sb}"
        return base
    ra = a.get("radiant_lead")
    rb = b.get("radiant_lead")
    if ra is None or rb is None:
        base["state"] = "missing"
        base["reason"] = "null_lead_despite_observed"
        return base
    base["state"] = "observed"
    base["delta_radiant"] = float(rb) - float(ra)
    base["reason"] = None
    return base


def build_map_row(
    match: Mapping[str, Any],
    *,
    source_shard: str,
    patch_hint: Optional[str] = None,
) -> dict[str, Any]:
    """Construct a pure map row; never mutates ``match``."""
    mid = canonicalize_map_id(match.get("id") if match.get("id") is not None else match.get("map_id"))
    duration = match.get("durationSeconds")
    try:
        duration_i = int(duration) if duration is not None else None
    except (TypeError, ValueError):
        duration_i = None
    leads = match.get("radiantNetworthLeads") or match.get("radiant_networth_leads") or []
    if not isinstance(leads, list):
        leads = []
    # Defensive copy of leads list only for local reads (source stays intact).
    leads_view = list(leads)

    patch, patch_reason = _extract_patch(match, source_shard=source_shard, patch_hint=patch_hint)
    # If patch was inferred, keep it but record reason; if explicit, reason is None.
    if patch is not None and patch_reason == "inferred_from_shard_name":
        pass
    elif patch is None and patch_reason is None:
        patch_reason = "missing_on_source"

    checkpoints: dict[int, dict[str, Any]] = {}
    for minute in CHECKPOINT_MINUTES:
        obs = checkpoint_observability(leads_view, duration_seconds=duration_i, minute=minute)
        checkpoints[minute] = {
            "state": obs["state"],
            "radiant_lead": obs["radiant_lead"],
            "minute": minute,
        }

    m34 = checkpoints.get(34) or {"state": "missing", "radiant_lead": None, "minute": 34}
    m34_info = m34_eligibility(duration_seconds=duration_i, m34_state=str(m34["state"]))

    row = {
        "map_id": mid,
        "startDateTime": match.get("startDateTime"),
        "source_shard": source_shard,
        "patch": patch,
        "patch_unavailable_reason": None if patch is not None and patch_reason is None else patch_reason,
        "durationSeconds": duration_i,
        "final_winner": _final_winner_from_match(match),
        "checkpoints": checkpoints,
        "trend": _trend_from_checkpoints(checkpoints),
        "m34": m34_info,
    }
    # If patch present via inference, still surface reason for auditability.
    if patch is not None and patch_reason == "inferred_from_shard_name":
        row["patch_unavailable_reason"] = "inferred_from_shard_name"
    elif patch is not None:
        row["patch_unavailable_reason"] = None
    return row


def build_dispatch_row(
    *,
    map_id: int,
    start_date_time: Any,
    source_shard: str,
    patch: Optional[str],
    final_winner: Optional[str],
    duration_seconds: Optional[int],
    blocks: Mapping[str, Any],
    dispatch_minute: Optional[int],
    radiant_lead_at_dispatch: Optional[float],
    policy_reason: Optional[str] = None,
    map_checkpoints: Optional[Mapping[Any, Mapping[str, Any]]] = None,
    quarantine: bool = False,
) -> Any:
    """Build a dispatch row. If quarantine=True, return (row|None, err|None)."""

    def _fail(reason: str):
        err = {"reason": reason, "map_id": map_id, "source_shard": source_shard}
        if quarantine:
            return None, err
        raise ValueError(reason)

    mid = canonicalize_map_id(map_id)
    if mid is None:
        return _fail("invalid_map_id")
    if final_winner not in ("radiant", "dire"):
        return _fail("invalid_final_winner")

    norm = normalize_blocks(blocks)
    selected_side, deciding_block, auto_reason = select_dispatch_target(norm)
    reason = policy_reason if policy_reason is not None else auto_reason
    dispatch_exists = selected_side in ("radiant", "dire")

    # Independent won for present blocks only.
    for label, b in norm.items():
        if b["present"]:
            b["won"] = _independent_won(b["side"], final_winner)
        else:
            # Explicit nulls for all value fields.
            b["side"] = None
            b["sign"] = None
            b["tier"] = None
            b["hit_count"] = None
            b["won"] = None

    try:
        minute_i = int(dispatch_minute) if dispatch_minute is not None else None
    except (TypeError, ValueError):
        minute_i = None
    dispatch_seconds = None if minute_i is None else minute_i * 60

    sel_lead = convert_selected_side_lead(radiant_lead_at_dispatch, selected_side)
    r_lead = None if radiant_lead_at_dispatch is None else float(radiant_lead_at_dispatch)
    dispatch_cp = {
        "minute": minute_i,
        "seconds": dispatch_seconds,
        "state": "observed" if r_lead is not None else "missing",
        "radiant_lead": r_lead,
        "selected_side_lead": sel_lead,
        "radiant_bucket": None if r_lead is None else lead_bucket(r_lead),
        "selected_side_bucket": None if sel_lead is None else lead_bucket(sel_lead),
    }

    # Attach selected-side leads on provided map checkpoints (copy, no mutate).
    cp_out: dict[int, dict[str, Any]] = {}
    if isinstance(map_checkpoints, Mapping):
        for key, val in map_checkpoints.items():
            try:
                m = int(key)
            except (TypeError, ValueError):
                continue
            if not isinstance(val, Mapping):
                continue
            r = val.get("radiant_lead")
            state = val.get("state")
            entry = {
                "minute": m,
                "state": state,
                "radiant_lead": r,
                "selected_side_lead": convert_selected_side_lead(r, selected_side)
                if state == "observed"
                else None,
                "radiant_bucket": lead_bucket(float(r)) if state == "observed" and r is not None else None,
                "selected_side_bucket": None,
            }
            if entry["selected_side_lead"] is not None:
                entry["selected_side_bucket"] = lead_bucket(entry["selected_side_lead"])
            cp_out[m] = entry

    event_id = event_id_for_dispatch(
        map_id=mid,
        dispatch_minute=minute_i,
        selected_side=selected_side,
        deciding_block=deciding_block,
    )

    row = {
        "event_id": event_id,
        "map_id": mid,
        "startDateTime": start_date_time,
        "source_shard": source_shard,
        "patch": patch,
        "final_winner": final_winner,
        "durationSeconds": duration_seconds,
        "time_kind": TIME_KIND_CHECKPOINT,
        "actual_send_time": None,  # never fabricate
        "dispatch_minute": minute_i,
        "dispatch_seconds": dispatch_seconds,
        "selected_side": selected_side,
        "policy_reason": reason,
        "deciding_block": deciding_block,
        "dispatch_exists": dispatch_exists,
        "blocks": {
            "E": dict(norm["E"]),
            "L": dict(norm["L"]),
            "A": dict(norm["A"]),
        },
        "dispatch_checkpoint": dispatch_cp,
        "checkpoints": cp_out,
    }
    if quarantine:
        return row, None
    return row


def iterate_unique_maps(
    records: Iterable[Mapping[str, Any]],
    *,
    max_unique: int = DEFAULT_MAX_UNIQUE,
    counts: Optional[MutableMapping[str, int]] = None,
) -> Iterator[dict[str, Any]]:
    """Canonical iteration with global map_id dedup; preserve first occurrence.

    Each input record: {match_id?, match: dict, source_shard: str}.
    Yields: {map_id, match, source_shard, match_id} without mutating match.
    """
    if counts is None:
        counts = empty_iteration_counts()
    seen: set[int] = set()
    for rec in records:
        counts["raw_seen"] = int(counts.get("raw_seen", 0)) + 1
        match = rec.get("match") if isinstance(rec, Mapping) else None
        source_shard = str(rec.get("source_shard") or "") if isinstance(rec, Mapping) else ""
        if not isinstance(match, Mapping):
            counts["invalid_id"] = int(counts.get("invalid_id", 0)) + 1
            continue
        raw_id = match.get("id")
        if raw_id is None:
            raw_id = rec.get("match_id") if isinstance(rec, Mapping) else None
        mid = canonicalize_map_id(raw_id)
        if mid is None:
            counts["invalid_id"] = int(counts.get("invalid_id", 0)) + 1
            continue
        if mid in seen:
            counts["duplicate_skipped"] = int(counts.get("duplicate_skipped", 0)) + 1
            continue
        seen.add(mid)
        counts["unique_accepted"] = int(counts.get("unique_accepted", 0)) + 1
        # Shallow wrapper only; do not mutate source match.
        yield {
            "map_id": mid,
            "match": match,
            "source_shard": source_shard,
            "match_id": rec.get("match_id") if isinstance(rec, Mapping) else None,
        }
        if int(counts["unique_accepted"]) >= int(max_unique):
            return


def build_rows_from_precomputed(
    *,
    match: Mapping[str, Any],
    source_shard: str,
    blocks: Mapping[str, Any],
    dispatch_minute: int = 12,
    patch_hint: Optional[str] = None,
) -> tuple[dict[str, Any], Any]:
    """Convenience: map row + dispatch row from a match + precomputed blocks."""
    map_row = build_map_row(match, source_shard=source_shard, patch_hint=patch_hint)
    minute = int(dispatch_minute)
    cp = map_row["checkpoints"].get(minute) or map_row["checkpoints"].get(str(minute)) or {}
    radiant_lead = cp.get("radiant_lead") if cp.get("state") == "observed" else None
    disp = build_dispatch_row(
        map_id=map_row["map_id"],
        start_date_time=map_row["startDateTime"],
        source_shard=source_shard,
        patch=map_row["patch"],
        final_winner=map_row["final_winner"],
        duration_seconds=map_row["durationSeconds"],
        blocks=blocks,
        dispatch_minute=minute,
        radiant_lead_at_dispatch=radiant_lead,
        map_checkpoints=map_row["checkpoints"],
        quarantine=True,
    )
    return map_row, disp


def write_rows_staging_sample(
    staging_dir: Optional[Path] = None,
) -> dict[str, Any]:
    """Tiny fixture-only sample under staging/rows (no full replay)."""
    out_dir = Path(staging_dir) if staging_dir is not None else STAGING_ROWS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    def _leads(vals: dict[int, float], length: int = 45) -> list:
        arr: list[Any] = [None] * length
        for m, v in vals.items():
            arr[m - 1] = v
        return arr

    fixtures = [
        {
            "match": {
                "id": 1001,
                "startDateTime": 1_800_000_100,
                "durationSeconds": 2500,
                "didRadiantWin": True,
                "radiantNetworthLeads": _leads(
                    {6: 100, 10: 500, 12: 900, 15: 1200, 20: 2000, 27: 2500, 34: 3000, 35: 3100, 40: 4000}
                ),
            },
            "source_shard": "7.41a_part001.json",
            "blocks": {
                "E": {"present": True, "sign": 1, "tier": 70, "hit_count": 2},
                "L": {"present": True, "sign": 1, "tier": 75, "hit_count": 3},
                "A": {"present": False},
            },
        },
        {
            "match": {
                "id": 1001,  # duplicate across shards
                "startDateTime": 1_800_000_100,
                "durationSeconds": 1111,
                "didRadiantWin": False,
                "radiantNetworthLeads": _leads({6: 1}),
            },
            "source_shard": "7.41a_part002.json",
            "blocks": {
                "E": {"present": True, "sign": -1, "tier": 65, "hit_count": 1},
            },
        },
        {
            "match": {
                "id": 1002,
                "startDateTime": 1_800_000_200,
                "durationSeconds": 900,  # dies before 34
                "didRadiantWin": False,
                "radiantNetworthLeads": _leads({6: -500, 10: -900, 12: -1000}, length=15),
            },
            "source_shard": "7.41a_part001.json",
            "blocks": {
                "E": {"present": True, "sign": 1, "tier": 65, "hit_count": 1},
                "L": {"present": True, "sign": -1, "tier": 80, "hit_count": 2},
                "A": {"present": True, "sign": 1, "tier": 70, "hit_count": 1},
            },
        },
        {
            "match": {
                "id": "not-valid",
                "startDateTime": 1,
                "durationSeconds": 100,
                "didRadiantWin": True,
                "radiantNetworthLeads": [],
            },
            "source_shard": "7.41a_part001.json",
            "blocks": {},
        },
    ]

    counts = empty_iteration_counts()
    map_rows: list[dict[str, Any]] = []
    dispatch_rows: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []

    # Index fixtures by map for block lookup after dedup.
    blocks_by_first: dict[int, Mapping[str, Any]] = {}
    records = []
    for fx in fixtures:
        records.append(
            {
                "match_id": fx["match"].get("id"),
                "match": fx["match"],
                "source_shard": fx["source_shard"],
            }
        )
        mid = canonicalize_map_id(fx["match"].get("id"))
        if mid is not None and mid not in blocks_by_first:
            blocks_by_first[mid] = fx["blocks"]

    for item in iterate_unique_maps(records, max_unique=DEFAULT_MAX_UNIQUE, counts=counts):
        mid = item["map_id"]
        map_row = build_map_row(item["match"], source_shard=item["source_shard"])
        map_rows.append(map_row)
        blocks = blocks_by_first.get(mid, {})
        drow, err = build_dispatch_row(
            map_id=mid,
            start_date_time=map_row["startDateTime"],
            source_shard=item["source_shard"],
            patch=map_row["patch"],
            final_winner=map_row["final_winner"],
            duration_seconds=map_row["durationSeconds"],
            blocks=blocks,
            dispatch_minute=12,
            radiant_lead_at_dispatch=(
                (map_row["checkpoints"].get(12) or {}).get("radiant_lead")
                if (map_row["checkpoints"].get(12) or {}).get("state") == "observed"
                else None
            ),
            map_checkpoints=map_row["checkpoints"],
            quarantine=True,
        )
        if err is not None:
            counts["quarantined"] = int(counts.get("quarantined", 0)) + 1
            quarantined.append(err)
        else:
            dispatch_rows.append(drow)

    sample = {
        "schema_version": 1,
        "lane": "rows",
        "counts": dict(counts),
        "map_rows": map_rows,
        "dispatch_rows": dispatch_rows,
        "quarantined": quarantined,
        "constants": {
            "BLOCKS": [list(x) for x in BLOCKS],
            "DISPATCH_PRIORITY": list(DISPATCH_PRIORITY),
            "CHECKPOINT_MINUTES": list(CHECKPOINT_MINUTES),
            "LEAD_BUCKETS": [b[0] for b in LEAD_BUCKETS],
            "M34_ELIGIBLE_SECONDS": M34_ELIGIBLE_SECONDS,
            "TREND_INTERVAL": list(TREND_INTERVAL),
            "TIME_KIND": TIME_KIND_CHECKPOINT,
        },
    }
    schema = {
        "map_row_keys": list(MAP_ROW_KEYS),
        "dispatch_row_keys": list(DISPATCH_ROW_KEYS),
        "checkpoint_states": ["observed", "map_ended", "missing"],
        "block_value_fields": ["present", "side", "sign", "tier", "hit_count", "won"],
        "absent_block_rule": "all value fields including won are null; absence is never a loss",
        "independent_won_rule": "present block won is evaluated vs final_winner regardless of selected target",
        "dedup_rule": "canonicalize map_id; preserve first occurrence; global before count/split",
        "m34_rule": "eligible iff durationSeconds>=2040 AND m34 state=observed; no final outcome",
        "time_kind": "checkpoint when only checkpoint known; actual_send_time stays null",
    }

    sample_path = out_dir / "fixture_sample.json"
    schema_path = out_dir / "schema_summary.json"
    # rebuild-then-replace
    tmp_sample = out_dir / "fixture_sample.json.tmp"
    tmp_schema = out_dir / "schema_summary.json.tmp"
    tmp_sample.write_text(json.dumps(sample, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_schema.write_text(json.dumps(schema, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_sample.replace(sample_path)
    tmp_schema.replace(schema_path)

    def _sha(path: Path) -> str:
        h = hashlib.sha256()
        h.update(path.read_bytes())
        return h.hexdigest()

    return {
        "sample_path": str(sample_path),
        "schema_path": str(schema_path),
        "sample_sha256": _sha(sample_path),
        "schema_sha256": _sha(schema_path),
        "counts": dict(counts),
    }


__all__ = [
    "BLOCKS",
    "BLOCK_LABELS",
    "CHECKPOINT_MINUTES",
    "DEFAULT_MAX_UNIQUE",
    "DISPATCH_PRIORITY",
    "LEAD_BUCKETS",
    "M34_ELIGIBLE_SECONDS",
    "STAGING_ROWS_DIR",
    "TIME_KIND_CHECKPOINT",
    "TREND_INTERVAL",
    "build_dispatch_row",
    "build_map_row",
    "build_rows_from_precomputed",
    "canonicalize_map_id",
    "checkpoint_observability",
    "convert_selected_side_lead",
    "empty_iteration_counts",
    "event_id_for_dispatch",
    "iterate_unique_maps",
    "lead_bucket",
    "m34_eligibility",
    "normalize_blocks",
    "select_dispatch_target",
    "write_rows_staging_sample",
]
