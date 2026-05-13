"""Unit tests for Late→same-sign-all promote (pt2) and kills gate (pt3).

These tests exercise the pure helpers (``_evaluate_late_all_same_sign_promote``
and ``_evaluate_kills_gate``) without running the full dispatch pipeline, so
the STAR diagnostics stay in their real (non-mocked) form and exercise the
WR60 thresholds directly.
"""

import importlib
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

runtime = importlib.import_module("cyberscore_try")


# -----------------------------
# pt2: Late promote via same-sign All
# -----------------------------


def test_late_promote_fires_when_all_same_sign_and_late_has_same_sign_hit() -> None:
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        raw_mid_output={
            "counterpick_1vs1": 4,  # WR60 threshold for late cp1v1 == 4
            "counterpick_1vs2": 0,
            "solo": 0,
        },
    )
    assert result["active"] is True
    assert result["valid"] is True
    assert "counterpick_1vs1" in result["same_sign_hits"]
    assert result["opposite_sign_hits"] == []
    assert result["opposite_sign_nonzero"] == []


def test_late_promote_rejects_carstensz_case_opposite_sign_hit() -> None:
    # Carstensz: early=+, all=+, late has cp1vs2=-6 (opposite sign hit).
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        raw_mid_output={
            "counterpick_1vs1": -1,  # opposite sign, below threshold
            "counterpick_1vs2": -6,
            "solo": 0,
        },
    )
    assert result["active"] is True
    assert result["valid"] is False
    assert "counterpick_1vs2" in result["opposite_sign_hits"]


def test_late_promote_inactive_when_all_opposite_sign() -> None:
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=-1,
        raw_mid_output={"counterpick_1vs1": 4},
    )
    assert result["active"] is False
    assert result["valid"] is False


def test_late_promote_inactive_when_late_already_valid() -> None:
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=True,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        raw_mid_output={"counterpick_1vs1": 4},
    )
    assert result["active"] is False
    assert result["valid"] is False


def test_late_promote_inactive_when_no_all_star() -> None:
    # No All star => kills path may apply, but promote must not fire.
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=False,
        has_all_star=False,
        early_sign=1,
        all_sign=None,
        raw_mid_output={"counterpick_1vs1": 4},
    )
    assert result["active"] is False
    assert result["valid"] is False


def test_late_promote_rejects_conflicting_nonzero_opposite_metric() -> None:
    # No opposite-sign hit, but a nonzero opposite-sign metric below threshold
    # must still block the promote.
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        raw_mid_output={
            "counterpick_1vs1": 4,
            "counterpick_1vs2": -1,
            "solo": 0,
        },
    )
    assert result["active"] is True
    assert result["valid"] is False
    assert "counterpick_1vs2" in result["opposite_sign_nonzero"]


# -----------------------------
# pt3: kills gate activation + WR gate
# -----------------------------


def test_kills_gate_activates_when_early_only_and_late_has_any_hit() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=False,
        early_sign=1,
        all_sign=None,
        early_wr_pct=70.0,
        early_hit_count=2,
        raw_mid_output={"counterpick_1vs2": -6},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is True
    assert result["valid"] is True
    assert result["wr_gate"]["passes_wr70"] is True


def test_kills_gate_activates_on_carstensz_case_with_all_same_sign() -> None:
    # Carstensz: early=+, all=+, late has opposite-sign hit cp1vs2=-6, pt2
    # promote failed. Kills gate must fire.
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        early_wr_pct=65.0,
        early_hit_count=2,
        raw_mid_output={
            "counterpick_1vs1": -1,
            "counterpick_1vs2": -6,
            "solo": 0,
        },
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is True
    assert result["valid"] is True


def test_kills_gate_blocks_when_early_wr_below_65() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=False,
        early_sign=1,
        all_sign=None,
        early_wr_pct=60.0,
        early_hit_count=3,
        raw_mid_output={"counterpick_1vs2": -6},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is True
    assert result["valid"] is False
    assert result["active_but_blocked"] is True


def test_kills_gate_blocks_when_early_wr_65_but_only_one_hit() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=False,
        early_sign=1,
        all_sign=None,
        early_wr_pct=65.0,
        early_hit_count=1,
        raw_mid_output={"counterpick_1vs2": -6},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is True
    assert result["valid"] is False
    assert result["active_but_blocked"] is True


def test_kills_gate_inactive_when_late_has_no_star_hits() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        early_wr_pct=70.0,
        early_hit_count=3,
        raw_mid_output={"counterpick_1vs2": 0, "solo": 0},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is False
    assert result["valid"] is False


def test_kills_gate_inactive_when_late_already_valid() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=True,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        early_wr_pct=70.0,
        early_hit_count=3,
        raw_mid_output={"counterpick_1vs2": -6},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is False


def test_kills_gate_inactive_when_promote_succeeds() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        early_wr_pct=70.0,
        early_hit_count=3,
        raw_mid_output={"counterpick_1vs1": 4},
        late_all_same_sign_promote_valid=True,
    )
    assert result["active"] is False


def test_early_star_meets_kills_wr_gate_edge_cases() -> None:
    # 70.0 passes regardless of hit_count
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=70.0, early_hit_count=1
    )["valid"] is True
    # 65 passes only with >=2 hits
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=65.0, early_hit_count=2
    )["valid"] is True
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=65.0, early_hit_count=1
    )["valid"] is False
    # 64.99 never passes
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=64.0, early_hit_count=5
    )["valid"] is False
    # None WR → blocked
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=None, early_hit_count=5
    )["valid"] is False
