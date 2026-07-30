#!/usr/bin/env python3
"""TDD for deterministic unique-map STAR dispatch row extraction.

Pure functions only: no production, no live probes, no full 40k replay.
"""
from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
ROOT_DIR = BASE_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from star_dispatch_replay_rows import (  # noqa: E402
    BLOCKS,
    CHECKPOINT_MINUTES,
    DISPATCH_PRIORITY,
    LEAD_BUCKETS,
    M34_ELIGIBLE_SECONDS,
    STAGING_ROWS_DIR,
    TIME_KIND_CHECKPOINT,
    build_dispatch_row,
    build_map_row,
    canonicalize_map_id,
    checkpoint_observability,
    convert_selected_side_lead,
    empty_iteration_counts,
    event_id_for_dispatch,
    iterate_unique_maps,
    lead_bucket,
    m34_eligibility,
    write_rows_staging_sample,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _leads(values_by_minute: dict[int, float], length: int = 45) -> list:
    arr = [None] * length
    for minute, val in values_by_minute.items():
        if 1 <= minute <= length:
            arr[minute - 1] = val
    return arr


def _raw_map(
    map_id,
    *,
    start: int = 1_800_000_000,
    duration: int = 2500,
    winner_radiant: bool = True,
    leads=None,
    patch=None,
    source_shard: str = "7.41a_part001.json",
    extra=None,
) -> dict:
    m = {
        "id": map_id,
        "startDateTime": start,
        "durationSeconds": duration,
        "didRadiantWin": winner_radiant,
        "radiantNetworthLeads": leads if leads is not None else _leads({6: 100, 10: 500, 12: 900, 15: 1200, 20: 2000, 27: 2500, 34: 3000, 35: 3100, 40: 4000}),
        "patch": patch,
        "_source_shard": source_shard,
    }
    if extra:
        m.update(extra)
    return m


def _block_metrics(
    *,
    e=None,
    l=None,
    a=None,
) -> dict:
    """Precomputed block diagnostics: present blocks as (sign, tier, hit_count)."""
    def pack(spec):
        if spec is None:
            return {"present": False, "sign": None, "side": None, "tier": None, "hit_count": None}
        sign, tier, hits = spec
        side = "radiant" if sign == 1 else "dire" if sign == -1 else None
        return {
            "present": True,
            "sign": sign,
            "side": side,
            "tier": tier,
            "hit_count": hits,
        }

    return {"E": pack(e), "L": pack(l), "A": pack(a)}


# ---------------------------------------------------------------------------
# Constants / contracts
# ---------------------------------------------------------------------------


def test_blocks_and_priority_match_backtest():
    assert BLOCKS == (("early_output", "E"), ("mid_output", "L"), ("all_output", "A"))
    assert DISPATCH_PRIORITY == ("mid_output", "early_output", "all_output")
    assert CHECKPOINT_MINUTES == (6, 10, 12, 15, 20, 27, 34, 35, 40)
    assert M34_ELIGIBLE_SECONDS == 2040


def test_lead_buckets_exact_boundaries_non_overlapping():
    # (-inf, -3000], (-3000, -1500], (-1500, -800], (-800, 800),
    # [800, 1500), [1500, 3000), [3000, inf)
    cases = [
        (-3000.0, "(-inf,-3000]"),
        (-3000.1, "(-inf,-3000]"),
        (-2999.9, "(-3000,-1500]"),
        (-1500.0, "(-3000,-1500]"),
        (-1499.9, "(-1500,-800]"),
        (-800.0, "(-1500,-800]"),
        (-799.9, "(-800,800)"),
        (0.0, "(-800,800)"),
        (799.9, "(-800,800)"),
        (800.0, "[800,1500)"),
        (1499.9, "[800,1500)"),
        (1500.0, "[1500,3000)"),
        (2999.9, "[1500,3000)"),
        (3000.0, "[3000,inf)"),
        (99999.0, "[3000,inf)"),
    ]
    for lead, expected in cases:
        assert lead_bucket(lead) == expected, (lead, expected)
    assert [b[0] for b in LEAD_BUCKETS] == [
        "(-inf,-3000]",
        "(-3000,-1500]",
        "(-1500,-800]",
        "(-800,800)",
        "[800,1500)",
        "[1500,3000)",
        "[3000,inf)",
    ]


# ---------------------------------------------------------------------------
# Canonical id + unique iteration / dedup
# ---------------------------------------------------------------------------


def test_canonicalize_map_id_stable_scalar():
    assert canonicalize_map_id(8761246899) == 8761246899
    assert canonicalize_map_id("8761246899") == 8761246899
    assert canonicalize_map_id({"id": "42"}) is None
    assert canonicalize_map_id(None) is None
    assert canonicalize_map_id("") is None
    assert canonicalize_map_id("not-a-number") is None


def test_cross_shard_duplicate_preserves_first_occurrence_order():
    shard_a = "shard_a.json"
    shard_b = "shard_b.json"
    records = [
        {"match_id": "100", "match": _raw_map(100, start=10, source_shard=shard_a), "source_shard": shard_a},
        {"match_id": "200", "match": _raw_map(200, start=20, source_shard=shard_a), "source_shard": shard_a},
        # cross-shard duplicate of 100 — must be skipped, first wins
        {"match_id": "100", "match": _raw_map(100, start=99, source_shard=shard_b, duration=1111), "source_shard": shard_b},
        {"match_id": "300", "match": _raw_map(300, start=30, source_shard=shard_b), "source_shard": shard_b},
        {"match_id": "bad", "match": _raw_map("xx", start=1), "source_shard": shard_b},  # invalid id in payload
        {"match_id": None, "match": {"startDateTime": 1}, "source_shard": shard_b},
    ]
    # inject invalid id via match without numeric id
    records[4]["match"]["id"] = "xx"

    accepted = []
    counts = empty_iteration_counts()
    for item in iterate_unique_maps(records, max_unique=40_000, counts=counts):
        accepted.append(item)

    assert [a["map_id"] for a in accepted] == [100, 200, 300]
    assert accepted[0]["source_shard"] == shard_a
    assert accepted[0]["match"]["durationSeconds"] == 2500  # first occurrence body
    assert counts["raw_seen"] == 6
    assert counts["duplicate_skipped"] == 1
    assert counts["invalid_id"] == 2
    assert counts["unique_accepted"] == 3


def test_stop_after_max_unique_accepted():
    records = [
        {"match_id": i, "match": _raw_map(i, start=i), "source_shard": "s.json"}
        for i in range(1, 20)
    ]
    counts = empty_iteration_counts()
    accepted = list(iterate_unique_maps(records, max_unique=5, counts=counts))
    assert len(accepted) == 5
    assert counts["unique_accepted"] == 5
    assert counts["raw_seen"] == 5  # stops once unique quota filled


def test_iteration_does_not_mutate_source_maps():
    m = _raw_map(7, leads=_leads({10: 1}))
    original = copy.deepcopy(m)
    records = [{"match_id": 7, "match": m, "source_shard": "s.json"}]
    list(iterate_unique_maps(records, max_unique=10, counts=empty_iteration_counts()))
    assert m == original


# ---------------------------------------------------------------------------
# Checkpoint observability: observed | map_ended | missing
# ---------------------------------------------------------------------------


def test_checkpoint_map_ended_vs_missing_vs_observed():
    # duration 19*60=1140 => minutes >=20 are map_ended; short timeline => missing for mid minutes that survived
    duration = 19 * 60  # 1140
    leads = _leads({6: 100.0, 10: 200.0}, length=12)  # only up to minute 12 index-wise length 12 => m12 index 11 exists if len=12
    # len=12 means indices 0..11 => minutes 1..12 available if values set
    leads = [None] * 12
    leads[5] = 100.0   # m6
    leads[9] = 200.0   # m10
    # m12 index 11 is None -> missing (map survived past 12 min: 1140 >= 720)
    # m15 index would be 14 but duration 1140 < 15*60=900? 1140>=900 so survived m15 but no array slot -> missing
    # m20: duration 1140 < 1200 -> map_ended

    obs6 = checkpoint_observability(leads, duration_seconds=duration, minute=6)
    obs10 = checkpoint_observability(leads, duration_seconds=duration, minute=10)
    obs12 = checkpoint_observability(leads, duration_seconds=duration, minute=12)
    obs15 = checkpoint_observability(leads, duration_seconds=duration, minute=15)
    obs20 = checkpoint_observability(leads, duration_seconds=duration, minute=20)
    obs34 = checkpoint_observability(leads, duration_seconds=duration, minute=34)

    assert obs6 == {"state": "observed", "radiant_lead": 100.0, "minute": 6}
    assert obs10["state"] == "observed" and obs10["radiant_lead"] == 200.0
    assert obs12["state"] == "missing" and obs12["radiant_lead"] is None
    assert obs15["state"] == "missing" and obs15["radiant_lead"] is None
    assert obs20["state"] == "map_ended" and obs20["radiant_lead"] is None
    assert obs34["state"] == "map_ended"


def test_selected_side_lead_conversion():
    assert convert_selected_side_lead(500.0, "radiant") == 500.0
    assert convert_selected_side_lead(500.0, "dire") == -500.0
    assert convert_selected_side_lead(None, "radiant") is None
    assert convert_selected_side_lead(100.0, None) is None


# ---------------------------------------------------------------------------
# Map row schema
# ---------------------------------------------------------------------------


def test_build_map_row_schema_and_checkpoints():
    match = _raw_map(
        55,
        duration=2100,
        leads=_leads({6: -500, 10: -900, 12: -1000, 15: 0, 20: 800, 27: 1500, 34: 3000, 35: 3100, 40: 4000}),
        patch=None,
        source_shard="7.41_part002.json",
    )
    row = build_map_row(match, source_shard="7.41_part002.json", patch_hint="7.41")
    assert row["map_id"] == 55
    assert row["startDateTime"] == match["startDateTime"]
    assert row["source_shard"] == "7.41_part002.json"
    assert row["durationSeconds"] == 2100
    assert row["final_winner"] == "radiant"
    # patch_hint fills when match has no patch
    assert row["patch"] == "7.41"
    assert row["patch_unavailable_reason"] is None
    # without hint / match field, reason is explicit
    row_no_patch = build_map_row(match, source_shard="custom_shard.json", patch_hint=None)
    # shard name custom_shard.json does not encode a patch → missing
    assert row_no_patch["patch"] is None
    assert row_no_patch["patch_unavailable_reason"] == "missing_on_source"

    cps = row["checkpoints"]
    for minute in CHECKPOINT_MINUTES:
        assert str(minute) in cps or minute in cps
    # normalize access
    def cp(m):
        return cps.get(m) or cps.get(str(m))

    assert cp(6)["state"] == "observed"
    assert cp(6)["radiant_lead"] == -500.0
    assert cp(34)["state"] == "observed"
    assert cp(34)["radiant_lead"] == 3000.0
    # never silently drop map_ended / missing
    short = build_map_row(
        _raw_map(56, duration=500, leads=_leads({6: 10}, length=6)),
        source_shard="s.json",
    )
    scp = short["checkpoints"]
    def sc(m):
        return scp.get(m) or scp.get(str(m))
    assert sc(6)["state"] == "observed"
    assert sc(10)["state"] == "map_ended"
    assert sc(40)["state"] == "map_ended"


def test_m34_eligibility_ignores_final_outcome():
    # duration >= 2040 and m34 observed
    elig = m34_eligibility(duration_seconds=2040, m34_state="observed")
    assert elig["eligible"] is True
    assert elig["died_before_34"] is False
    assert elig["missing_m34"] is False

    died = m34_eligibility(duration_seconds=2000, m34_state="map_ended")
    assert died["eligible"] is False
    assert died["died_before_34"] is True
    assert died["missing_m34"] is False

    missing = m34_eligibility(duration_seconds=2500, m34_state="missing")
    assert missing["eligible"] is False
    assert missing["died_before_34"] is False
    assert missing["missing_m34"] is True

    # eligibility must not take winner; function signature has no winner arg
    import inspect
    sig = inspect.signature(m34_eligibility)
    assert "winner" not in sig.parameters
    assert "final" not in "".join(sig.parameters)


# ---------------------------------------------------------------------------
# Dispatch row: absent null, independent wins, event_id, time_kind
# ---------------------------------------------------------------------------


def test_absent_block_null_semantics_not_loss():
    blocks = _block_metrics(e=(1, 70, 2), l=None, a=(-1, 65, 1))
    row = build_dispatch_row(
        map_id=9,
        start_date_time=123,
        source_shard="s.json",
        patch=None,
        final_winner="radiant",
        duration_seconds=2400,
        blocks=blocks,
        dispatch_minute=12,
        radiant_lead_at_dispatch=900.0,
        policy_reason=None,
    )
    assert row["blocks"]["L"]["present"] is False
    assert row["blocks"]["L"]["side"] is None
    assert row["blocks"]["L"]["sign"] is None
    assert row["blocks"]["L"]["tier"] is None
    assert row["blocks"]["L"]["hit_count"] is None
    assert row["blocks"]["L"]["won"] is None  # absence is never a loss

    assert row["blocks"]["E"]["present"] is True
    assert row["blocks"]["E"]["won"] is True  # E radiant vs radiant winner
    assert row["blocks"]["A"]["present"] is True
    assert row["blocks"]["A"]["won"] is False  # A dire vs radiant winner


def test_independent_ela_wins_regardless_of_selected_target():
    # L selects dire (priority), but E radiant wins independently, A dire wins independently
    blocks = _block_metrics(e=(1, 75, 3), l=(-1, 80, 2), a=(-1, 70, 1))
    row = build_dispatch_row(
        map_id=11,
        start_date_time=50,
        source_shard="s.json",
        patch="7.41",
        final_winner="radiant",
        duration_seconds=2600,
        blocks=blocks,
        dispatch_minute=15,
        radiant_lead_at_dispatch=-200.0,
    )
    assert row["selected_side"] == "dire"  # L priority
    assert row["deciding_block"] == "L"
    assert row["blocks"]["E"]["won"] is True   # radiant side correct
    assert row["blocks"]["L"]["won"] is False  # dire side wrong
    assert row["blocks"]["A"]["won"] is False  # dire side wrong
    # selected-side lead at dispatch = -radiant_lead when selected=dire
    assert row["dispatch_checkpoint"]["radiant_lead"] == -200.0
    assert row["dispatch_checkpoint"]["selected_side_lead"] == 200.0
    assert row["dispatch_checkpoint"]["selected_side_bucket"] == lead_bucket(200.0)


def test_deterministic_event_id_stable():
    blocks = _block_metrics(l=(1, 65, 1))
    kwargs = dict(
        map_id=42,
        start_date_time=999,
        source_shard="shard.json",
        patch="7.41a",
        final_winner="dire",
        duration_seconds=3000,
        blocks=blocks,
        dispatch_minute=12,
        radiant_lead_at_dispatch=100.0,
    )
    r1 = build_dispatch_row(**kwargs)
    r2 = build_dispatch_row(**kwargs)
    assert r1["event_id"] == r2["event_id"]
    assert r1["event_id"] == event_id_for_dispatch(
        map_id=42,
        dispatch_minute=12,
        selected_side="radiant",
        deciding_block="L",
    )
    # changing inputs changes id
    r3 = build_dispatch_row(**{**kwargs, "dispatch_minute": 20})
    assert r3["event_id"] != r1["event_id"]


def test_time_kind_checkpoint_no_fabricated_send_time():
    blocks = _block_metrics(e=(1, 65, 1))
    row = build_dispatch_row(
        map_id=1,
        start_date_time=10,
        source_shard="s.json",
        patch=None,
        final_winner="radiant",
        duration_seconds=2000,
        blocks=blocks,
        dispatch_minute=10,
        radiant_lead_at_dispatch=50.0,
    )
    assert row["time_kind"] == TIME_KIND_CHECKPOINT
    assert row["actual_send_time"] is None
    assert row["dispatch_minute"] == 10
    assert row["dispatch_seconds"] == 10 * 60


def test_quarantine_invalid_rows_do_not_crash():
    # invalid blocks payload / missing final winner should quarantine
    row, err = build_dispatch_row(
        map_id=1,
        start_date_time=10,
        source_shard="s.json",
        patch=None,
        final_winner=None,  # invalid
        duration_seconds=2000,
        blocks=_block_metrics(e=(1, 65, 1)),
        dispatch_minute=10,
        radiant_lead_at_dispatch=50.0,
        quarantine=True,
    )
    assert row is None
    assert err is not None
    assert "final_winner" in err["reason"] or "invalid" in err["reason"]


def test_trend_delta_predeclared_interval_or_explicit_reason():
    # default predeclared interval 10->20
    match = _raw_map(3, leads=_leads({10: 100, 20: 500}))
    row = build_map_row(match, source_shard="s.json")
    trend = row["trend"]
    assert trend["interval"] == (10, 20) or trend["interval"] == [10, 20]
    assert trend["state"] == "observed"
    assert trend["delta_radiant"] == 400.0

    match2 = _raw_map(4, duration=500, leads=_leads({6: 10}, length=6))
    row2 = build_map_row(match2, source_shard="s.json")
    assert row2["trend"]["state"] in ("map_ended", "missing", "unavailable")
    assert row2["trend"]["delta_radiant"] is None
    assert row2["trend"]["reason"]


def test_no_dispatch_when_all_blocks_absent():
    blocks = _block_metrics()
    row = build_dispatch_row(
        map_id=8,
        start_date_time=1,
        source_shard="s.json",
        patch=None,
        final_winner="dire",
        duration_seconds=2500,
        blocks=blocks,
        dispatch_minute=12,
        radiant_lead_at_dispatch=0.0,
    )
    assert row["selected_side"] is None
    assert row["deciding_block"] is None
    assert row["dispatch_exists"] is False
    for lbl in ("E", "L", "A"):
        assert row["blocks"][lbl]["present"] is False
        assert row["blocks"][lbl]["won"] is None


def test_staging_sample_write_under_rows_only(tmp_path, monkeypatch):
    staging = tmp_path / "rows"
    monkeypatch.setattr(
        "star_dispatch_replay_rows.STAGING_ROWS_DIR",
        staging,
    )
    # rebuild path constant used inside write
    from star_dispatch_replay_rows import write_rows_staging_sample as _write
    # call with explicit dir
    out = write_rows_staging_sample(staging_dir=staging)
    assert staging in Path(out["sample_path"]).parents or Path(out["sample_path"]).parent == staging
    assert Path(out["sample_path"]).exists()
    assert Path(out["schema_path"]).exists()
    sample = json.loads(Path(out["sample_path"]).read_text(encoding="utf-8"))
    assert "map_rows" in sample and "dispatch_rows" in sample
    assert "counts" in sample
    assert sample["counts"]["unique_accepted"] >= 1
    # ensure no final/ write
    assert "final" not in str(out["sample_path"])


def test_map_row_does_not_mutate_source():
    m = _raw_map(77)
    original = copy.deepcopy(m)
    build_map_row(m, source_shard="s.json")
    assert m == original
