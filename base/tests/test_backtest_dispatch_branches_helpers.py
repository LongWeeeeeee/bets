#!/usr/bin/env python3
"""Regression tests for the pure helpers used by the dispatch backtest."""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from backtest_dispatch_branches import (  # noqa: E402
    _add,
    _block_label,
    _decorate,
    _lead_at_minute,
    _new_stat,
    _nw_bucket,
    _sign_char,
    _sign_to_side,
    _stake_proxy,
    _wr,
)


def test_stat_helpers_count_wins_and_losses() -> None:
    stat = _new_stat()

    _add(stat, 1)
    _add(stat, 0)
    _add(stat, 1)

    assert stat == {"matches": 3, "wins": 2, "losses": 1}
    assert _wr(stat) == pytest.approx(200 / 3)
    assert _wr(_new_stat()) == 0.0


@pytest.mark.parametrize(
    ("sign", "char", "side"),
    [
        (1, "r", "radiant"),
        (-1, "d", "dire"),
        (None, "?", None),
        (0, "?", None),
    ],
)
def test_sign_helpers(sign, char, side) -> None:
    assert _sign_char(sign) == char
    assert _sign_to_side(sign) == side


def test_block_label_includes_tier_and_side() -> None:
    assert _block_label(None, None) == "no"
    assert _block_label(65, 1) == "65r"
    assert _block_label(70, -1) == "70d"


def test_lead_at_minute_is_one_based_and_fail_closed() -> None:
    leads = [100, -200, 350]

    assert _lead_at_minute(leads, 1) == 100.0
    assert _lead_at_minute(leads, 3) == 350.0
    assert _lead_at_minute(leads, 0) is None
    assert _lead_at_minute(leads, 4) is None
    assert _lead_at_minute(["bad"], 1) is None


@pytest.mark.parametrize(
    ("lead", "expected"),
    [
        (3000, "lead_ge_3000"),
        (2999, "lead_ge_1500"),
        (1499, "lead_ge_800"),
        (799, "lead_ge_0"),
        (-1, "behind_0_800"),
        (-800, "behind_0_800"),
        (-801, "behind_800_1500"),
        (-1500, "behind_800_1500"),
        (-1501, "behind_ge_1500"),
    ],
)
def test_networth_bucket_boundaries(lead, expected) -> None:
    assert _nw_bucket(lead) == expected


@pytest.mark.parametrize(
    ("has_l", "late_tier", "late_hits", "expected"),
    [
        (False, None, 0, (0.5, "no_late_star")),
        (True, 80, 1, (0.5, "late_hits_lt_2")),
        (True, 60, 2, (0.5, "late_wr60")),
        (True, 70, 2, (1.0, "late_wr_65_74")),
        (True, 75, 2, (2.0, "late_wr>=75")),
        (True, 85, 2, (3.0, "late_wr>=85")),
    ],
)
def test_stake_proxy_bands(has_l, late_tier, late_hits, expected) -> None:
    assert _stake_proxy(
        has_l=has_l,
        late_tier=late_tier,
        late_hits=late_hits,
    ) == expected


def test_decorate_adds_wr_and_orders_by_sample_size() -> None:
    stats = defaultdict(_new_stat)
    _add(stats["small"], 1)
    _add(stats["large"], 1)
    _add(stats["large"], 0)

    decorated = _decorate(stats)

    assert list(decorated) == ["large", "small"]
    assert decorated["large"]["wr_pct"] == 50.0
    assert decorated["small"]["wr_pct"] == 100.0
