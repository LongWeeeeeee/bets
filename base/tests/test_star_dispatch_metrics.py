#!/usr/bin/env python3
"""TDD for pure STAR dispatch aggregation/calibration engine."""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from star_dispatch_metrics import (  # noqa: E402
    WILSON_Z,
    assign_temporal_splits,
    aggregate_dispatch_metrics,
    block_presence_combo,
    calibration_for_block,
    dumps_deterministic,
    empty_table,
    sign_agreement_pattern,
    summarize_outcomes,
    wilson_interval,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _block(
    present: bool,
    *,
    side: str | None = None,
    sign: int | None = None,
    tier: int | None = None,
    hit_count: int | None = None,
    won: bool | None = None,
    metric_strength: float | None = None,
) -> dict:
    if not present:
        return {
            "present": False,
            "side": None,
            "sign": None,
            "tier": None,
            "hit_count": None,
            "won": None,
            "metric_strength": None,
        }
    return {
        "present": True,
        "side": side,
        "sign": sign,
        "tier": tier,
        "hit_count": hit_count,
        "won": won,
        "metric_strength": metric_strength,
    }


def _event(
    event_id: str,
    map_id: int,
    start: int,
    *,
    e=None,
    l=None,
    a=None,
    selected_side: str = "radiant",
    deciding_block: str = "L",
    dispatch_minute: int = 12,
    selected_side_lead: float | None = 1000.0,
    selected_side_networth_bucket: str | None = None,
    patch: str | None = "7.41",
    metric_strength_band: str | None = None,
    final_winner: str = "radiant",
    invalid: bool = False,
    map_ended_flag: bool = False,
) -> dict:
    blocks = {
        "E": e if e is not None else _block(False),
        "L": l if l is not None else _block(False),
        "A": a if a is not None else _block(False),
    }
    row = {
        "event_id": event_id,
        "map_id": map_id,
        "startDateTime": start,
        "patch": patch,
        "final_winner": final_winner,
        "dispatch_minute": dispatch_minute,
        "selected_side": selected_side,
        "deciding_block": deciding_block,
        "selected_side_lead": selected_side_lead,
        "selected_side_networth_bucket": selected_side_networth_bucket,
        "metric_strength_band": metric_strength_band,
        "blocks": blocks,
        "invalid": invalid,
        "map_ended": map_ended_flag,
    }
    return row


# ---------------------------------------------------------------------------
# Wilson
# ---------------------------------------------------------------------------


def test_wilson_known_cases_including_n0():
    assert wilson_interval(0, 0) is None
    assert wilson_interval(5, 0) is None

    lo, hi = wilson_interval(5, 10)
    assert abs(lo - 0.236593090512564) < 1e-12
    assert abs(hi - 0.7634069094874361) < 1e-12

    lo0, hi0 = wilson_interval(0, 10)
    assert lo0 == 0.0
    assert abs(hi0 - 0.2775327998628892) < 1e-12

    lo1, hi1 = wilson_interval(10, 10)
    assert abs(lo1 - 0.7224672001371107) < 1e-12
    assert hi1 == pytest.approx(1.0)

    assert WILSON_Z == pytest.approx(1.959963984540054)


def test_summarize_outcomes_n0_and_missing():
    table = summarize_outcomes(
        unit="dispatch_event",
        outcomes=[],  # empty
        map_ids=[],
        missing_n=3,
        coverage_denominator=10,
    )
    assert table["unit"] == "dispatch_event"
    assert table["N"] == 0
    assert table["unique_map_n"] == 0
    assert table["W"] == 0
    assert table["L"] == 0
    assert table["WR"] is None
    assert table["wilson95"] is None
    assert table["missing_n"] == 3
    assert table["coverage_denominator"] == 10
    assert set(empty_table("x").keys()) >= {
        "unit",
        "N",
        "unique_map_n",
        "W",
        "L",
        "WR",
        "missing_n",
        "coverage_denominator",
        "wilson95",
    }


# ---------------------------------------------------------------------------
# Absent-block denominators / independent E/L/A
# ---------------------------------------------------------------------------


def test_absent_block_excluded_from_denominator_never_loss():
    """E present win, L present loss, A absent → A not in N and not a loss."""
    rows = [
        _event(
            "e1",
            1,
            100,
            e=_block(True, side="radiant", sign=1, tier=65, hit_count=2, won=True),
            l=_block(True, side="dire", sign=-1, tier=70, hit_count=1, won=False),
            a=_block(False),
            deciding_block="E",
            selected_side="radiant",
        )
    ]
    out = aggregate_dispatch_metrics(rows)
    e = out["by_block"]["E"]["overall"]
    l = out["by_block"]["L"]["overall"]
    a = out["by_block"]["A"]["overall"]
    assert e["N"] == 1 and e["W"] == 1 and e["L"] == 0 and e["WR"] == 1.0
    assert l["N"] == 1 and l["W"] == 0 and l["L"] == 1 and l["WR"] == 0.0
    assert a["N"] == 0 and a["W"] == 0 and a["L"] == 0 and a["WR"] is None
    assert a["absent_n"] == 1
    assert e["absent_n"] == 0
    assert l["wilson95"] is not None
    assert a["wilson95"] is None


def test_two_events_same_map_separate_event_and_map_units():
    """Two dispatch events on one map: event N=2, unique_map_n=1."""
    rows = [
        _event(
            "e1",
            42,
            1000,
            e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=True),
            l=_block(True, side="radiant", sign=1, tier=65, hit_count=2, won=True),
            deciding_block="L",
            dispatch_minute=10,
        ),
        _event(
            "e2",
            42,
            1000,
            e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=False),
            l=_block(True, side="radiant", sign=1, tier=70, hit_count=3, won=False),
            deciding_block="L",
            dispatch_minute=28,
        ),
    ]
    out = aggregate_dispatch_metrics(rows)
    ev = out["event_summaries"]["all_dispatch_events"]
    mp = out["map_summaries"]["unique_maps_with_dispatch"]
    assert ev["N"] == 2
    assert ev["unique_map_n"] == 1
    assert mp["N"] == 1
    assert mp["unique_map_n"] == 1
    # descriptive same-map multi-event count
    assert out["event_summaries"]["multi_event_maps"]["unique_map_n"] == 1
    assert out["event_summaries"]["multi_event_maps"]["N"] == 2  # events on multi-event maps
    # E block event-level N=2 with unique_map_n=1
    assert out["by_block"]["E"]["overall"]["N"] == 2
    assert out["by_block"]["E"]["overall"]["unique_map_n"] == 1
    assert out["by_block"]["E"]["overall"]["W"] == 1
    assert out["by_block"]["E"]["overall"]["L"] == 1


# ---------------------------------------------------------------------------
# Cumulative tiers + grouping dimensions
# ---------------------------------------------------------------------------


def test_exact_and_cumulative_tier_grouping():
    rows = [
        _event(
            "a",
            1,
            10,
            e=_block(True, side="radiant", sign=1, tier=70, hit_count=2, won=True),
            deciding_block="E",
        ),
        _event(
            "b",
            2,
            20,
            e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=False),
            deciding_block="E",
        ),
        _event(
            "c",
            3,
            30,
            e=_block(True, side="dire", sign=-1, tier=70, hit_count=3, won=True),
            deciding_block="E",
            selected_side="dire",
            final_winner="dire",
        ),
    ]
    out = aggregate_dispatch_metrics(rows)
    exact = out["by_block"]["E"]["by_exact_tier"]
    cumul = out["by_block"]["E"]["by_cumulative_tier"]
    assert exact["70"]["N"] == 2 and exact["70"]["W"] == 2
    assert exact["60"]["N"] == 1 and exact["60"]["W"] == 0
    # cumulative: tier 70 also counts in >=60 and >=65 and >=70
    assert cumul[">=60"]["N"] == 3
    assert cumul[">=70"]["N"] == 2
    # only observed cumulative keys (tier never reached 75 → key absent or N=0)
    assert cumul.get(">=75", {}).get("N", 0) == 0


def test_grouping_dimensions_include_all_observed():
    rows = [
        _event(
            "e1",
            1,
            100,
            e=_block(True, side="radiant", sign=1, tier=65, hit_count=1, won=True),
            l=_block(True, side="radiant", sign=1, tier=65, hit_count=2, won=True),
            deciding_block="L",
            dispatch_minute=12,
            selected_side_lead=900,
            patch="7.41",
            metric_strength_band="strong",
        ),
        _event(
            "e2",
            2,
            200,
            e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=False),
            l=_block(True, side="dire", sign=-1, tier=70, hit_count=4, won=True),
            deciding_block="L",
            selected_side="dire",
            final_winner="dire",
            dispatch_minute=30,
            selected_side_lead=-2000,
            patch="7.40",
            metric_strength_band=None,  # optional — only when supplied
        ),
        _event(
            "e3",
            3,
            300,
            a=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=True),
            deciding_block="A",
            dispatch_minute=5,
            selected_side_lead=50,
            patch=None,
        ),
    ]
    out = aggregate_dispatch_metrics(rows)

    assert block_presence_combo(rows[0]["blocks"]) == "E+L"
    assert block_presence_combo(rows[1]["blocks"]) == "E+L"
    assert block_presence_combo(rows[2]["blocks"]) == "A"
    assert sign_agreement_pattern(rows[0]["blocks"]) == "same_sign"
    assert sign_agreement_pattern(rows[1]["blocks"]) == "opposite"
    assert sign_agreement_pattern(rows[2]["blocks"]) == "single"

    dims = out["dimensions"]
    # all observed combos present, not only high-WR
    assert "E+L" in dims["block_presence_combo"]
    assert "A" in dims["block_presence_combo"]
    assert "same_sign" in dims["sign_agreement"]
    assert "opposite" in dims["sign_agreement"]
    assert "single" in dims["sign_agreement"]
    assert "L" in dims["deciding_block"]
    assert "A" in dims["deciding_block"]
    assert any(k.startswith("m") for k in dims["dispatch_minute_bucket"])
    assert dims["patch"]  # includes observed patches; null patch surfaced
    assert "null" in dims["patch"] or None in dims["patch"] or "None" in dims["patch"] or "<null>" in dims["patch"]
    # support/hit band observed
    assert dims["support_hit_count_band"]
    # metric strength only when supplied
    assert "strong" in dims["metric_strength_band"]
    # low-WR groups still included
    low = dims["block_presence_combo"]["E+L"]
    assert low["N"] >= 1


# ---------------------------------------------------------------------------
# Temporal splits
# ---------------------------------------------------------------------------


def test_deterministic_split_boundaries_without_outcomes():
    rows = [
        _event(f"e{i}", i, start=1000 + i * 10, e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=bool(i % 2)))
        for i in range(10)
    ]
    # scramble order — splits must sort by startDateTime, map_id first
    scrambled = [rows[i] for i in (7, 1, 9, 0, 3, 5, 2, 8, 4, 6)]
    splits = assign_temporal_splits(scrambled, train_frac=0.6, cal_frac=0.2, test_frac=0.2)
    assert splits["ordering"] == ["startDateTime", "map_id"]
    assert splits["fractions"] == {"train": 0.6, "calibration": 0.2, "final_test": 0.2}
    assert splits["counts"]["train"] == 6
    assert splits["counts"]["calibration"] == 2
    assert splits["counts"]["final_test"] == 2
    # cutoffs are exact timestamps of first row of each subsequent split
    assert splits["cutoffs"]["train_end_startDateTime"] == rows[5]["startDateTime"]
    assert splits["cutoffs"]["calibration_end_startDateTime"] == rows[7]["startDateTime"]
    assert splits["cutoffs"]["final_test_start_startDateTime"] == rows[8]["startDateTime"]
    # labels diagnostic / in-sample when frozen dicts overlap
    assert splits["label"] == "diagnostic_in_sample"
    assert splits["oos"] is False
    assert "walk_forward_requirement" in splits["gap"]
    wf = splits["gap"]["walk_forward_requirement"].lower()
    assert "time-frozen" in wf or "time_frozen" in wf or "walk-forward" in wf

    # deterministic assignment map
    labels = [splits["row_split_by_event_id"][r["event_id"]] for r in rows]
    assert labels == ["train"] * 6 + ["calibration"] * 2 + ["final_test"] * 2

    # re-run is identical
    splits2 = assign_temporal_splits(scrambled, train_frac=0.6, cal_frac=0.2, test_frac=0.2)
    assert dumps_deterministic(splits) == dumps_deterministic(splits2)


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------


def test_calibration_null_when_no_valid_nominal():
    # tier missing / non-numeric → unavailable
    rows = [
        _event(
            "e1",
            1,
            10,
            e=_block(True, side="radiant", sign=1, tier=None, hit_count=1, won=True),
            deciding_block="E",
        )
    ]
    out = aggregate_dispatch_metrics(rows)
    cal = out["calibration"]["by_block"]["E"]
    assert cal["available"] is False
    assert cal["metrics"] is None
    assert cal["unavailable_reason"]


def test_calibration_gap_brier_logloss_with_valid_tier():
    # tier 60 → nominal p=0.6; 2 wins / 2 losses → emp WR=0.5
    rows = [
        _event("a", 1, 10, e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=True), deciding_block="E"),
        _event("b", 2, 20, e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=True), deciding_block="E"),
        _event("c", 3, 30, e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=False), deciding_block="E"),
        _event("d", 4, 40, e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=False), deciding_block="E"),
    ]
    cal = calibration_for_block(rows, "E")
    assert cal["available"] is True
    m = cal["metrics"]
    assert m["N"] == 4
    assert m["empirical_WR"] == 0.5
    assert m["mean_nominal_p"] == pytest.approx(0.6)
    assert m["calibration_gap"] == pytest.approx(0.5 - 0.6)
    # Brier = mean((p-y)^2) = 2*(0.6-1)^2 + 2*(0.6-0)^2 / 4 = (0.16+0.16+0.36+0.36)/4 = 0.26
    assert m["brier"] == pytest.approx(0.26)
    assert m["log_loss"] is not None and m["log_loss"] > 0
    assert "probability_clip" in cal["metadata"]
    assert cal["metadata"]["nominal_mapping"] == "tier_pct_over_100"


# ---------------------------------------------------------------------------
# Missing / invalid / stable ordering
# ---------------------------------------------------------------------------


def test_missing_map_ended_invalid_surfaced_no_silent_drop():
    rows = [
        _event("ok", 1, 10, e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=True), deciding_block="E"),
        _event("bad", 2, 20, e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=None), deciding_block="E", invalid=True),
        _event(
            "ended",
            3,
            30,
            e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=True),
            deciding_block="E",
            map_ended_flag=True,
        ),
        # present block with missing won counts as missing outcome, not silent drop
        _event(
            "miss_won",
            4,
            40,
            e=_block(True, side="radiant", sign=1, tier=65, hit_count=1, won=None),
            deciding_block="E",
        ),
    ]
    out = aggregate_dispatch_metrics(rows)
    q = out["field_quality"]
    assert q["invalid_n"] >= 1
    assert q["map_ended_n"] >= 1
    assert q["rows_seen"] == 4
    assert q["rows_dropped_silent"] == 0
    # E overall: only rows with present + boolean won enter N
    e = out["by_block"]["E"]["overall"]
    assert e["N"] == 2  # ok + ended
    assert e["missing_n"] >= 1  # miss_won (+ maybe invalid)


def test_stable_ordering_and_deterministic_json():
    rows = [
        _event("z", 9, 50, e=_block(True, side="radiant", sign=1, tier=70, hit_count=1, won=True), deciding_block="E", patch="7.41"),
        _event("a", 1, 10, e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=False), deciding_block="E", patch="7.40"),
        _event("m", 5, 30, l=_block(True, side="dire", sign=-1, tier=65, hit_count=2, won=True), deciding_block="L", selected_side="dire", final_winner="dire", patch="7.41"),
    ]
    out1 = aggregate_dispatch_metrics(list(reversed(rows)))
    out2 = aggregate_dispatch_metrics(rows)
    s1 = dumps_deterministic(out1)
    s2 = dumps_deterministic(out2)
    assert s1 == s2
    # dimension keys sorted
    assert list(out1["dimensions"]["patch"].keys()) == sorted(out1["dimensions"]["patch"].keys())
    assert list(out1["by_block"]["E"]["by_exact_tier"].keys()) == sorted(
        out1["by_block"]["E"]["by_exact_tier"].keys(), key=lambda x: int(x) if str(x).lstrip("-").isdigit() else str(x)
    )


def test_event_table_declares_coverage_denominator():
    rows = [
        _event("e1", 1, 10, e=_block(True, side="radiant", sign=1, tier=60, hit_count=1, won=True), deciding_block="E"),
        _event("e2", 2, 20, a=_block(False), deciding_block="A"),  # no present blocks
    ]
    out = aggregate_dispatch_metrics(rows)
    for table in out["event_summaries"].values():
        assert "coverage_denominator" in table
        assert "missing_n" in table
        assert "unique_map_n" in table
        assert "unit" in table
