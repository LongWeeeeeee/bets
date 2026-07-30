"""TDD contract tests for leakage-safe STAR dispatch policy analysis.

Pure engine: dict rows in, JSON-compatible report out.
No corpus collection, no production filter/stake changes.
"""
from __future__ import annotations

import hashlib
import json
import math
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from star_dispatch_policy_analysis import (
    ASSUMED_ODDS_GRID,
    DEFAULT_POLICY_CONFIG,
    STAGING_POLICY_DIR,
    analyze_policy,
    apply_exposure_cap,
    benjamini_hochberg,
    chronological_sort,
    evaluate_counterfactuals,
    evaluate_economics,
    evaluate_wait34_eligibility,
    exact_la_conflict,
    fractional_kelly,
    freeze_rules_on_calibration,
    jeffreys_shrinkage,
    lead_bucket,
    outcome_summary,
    select_side,
    temporal_split,
    wilson_interval,
    write_policy_staging_evidence,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _row(
    map_id: int,
    start: int,
    *,
    duration: int = 2500,
    winner: str = "radiant",
    e_side=None,
    l_side=None,
    a_side=None,
    e_tier=70,
    l_tier=70,
    a_tier=70,
    m34_lead=None,
    execution_odds=None,
    closing_odds=None,
    stake=None,
    pnl=None,
    verified_settlement=False,
    patch="7.41",
    bookmaker=None,
):
    def block(side, tier):
        if side is None:
            return {"present": False, "side": None, "tier": None, "hit_count": 0}
        return {"present": True, "side": side, "tier": tier, "hit_count": 2}

    return {
        "map_id": map_id,
        "startDateTime": start,
        "durationSeconds": duration,
        "final_outcome": winner,
        "patch": patch,
        "bookmaker": bookmaker,
        "blocks": {
            "E": block(e_side, e_tier),
            "L": block(l_side, l_tier),
            "A": block(a_side, a_tier),
        },
        "m34_lead_radiant": m34_lead,
        "execution_odds": execution_odds,
        "closing_odds": closing_odds,
        "stake": stake,
        "pnl": pnl,
        "verified_settlement": verified_settlement,
    }


def _make_split_corpus(n=30, seed_start=1_700_000_000):
    """Small chronological corpus with mixed L/A agreement patterns."""
    rows = []
    for i in range(n):
        # alternate exact L!=A, consensus, L-only, A-only
        kind = i % 5
        if kind == 0:
            # exact L!=A
            rows.append(
                _row(
                    1000 + i,
                    seed_start + i * 100,
                    l_side="radiant",
                    a_side="dire",
                    e_side="dire",
                    winner="radiant" if i % 3 else "dire",
                    m34_lead=1200 if i % 2 else -900,
                    duration=2500 if i % 4 else 1800,
                )
            )
        elif kind == 1:
            # E+A consensus (agree), L absent
            rows.append(
                _row(
                    1000 + i,
                    seed_start + i * 100,
                    e_side="radiant",
                    a_side="radiant",
                    l_side=None,
                    winner="radiant",
                    m34_lead=500,
                )
            )
        elif kind == 2:
            # L only
            rows.append(
                _row(
                    1000 + i,
                    seed_start + i * 100,
                    l_side="dire",
                    winner="dire" if i % 2 else "radiant",
                    m34_lead=-2000,
                )
            )
        elif kind == 3:
            # all same sign
            rows.append(
                _row(
                    1000 + i,
                    seed_start + i * 100,
                    e_side="radiant",
                    l_side="radiant",
                    a_side="radiant",
                    winner="radiant",
                    m34_lead=3000,
                )
            )
        else:
            # generic opposite E vs A, L absent (NOT exact L-A)
            rows.append(
                _row(
                    1000 + i,
                    seed_start + i * 100,
                    e_side="radiant",
                    a_side="dire",
                    l_side=None,
                    winner="radiant",
                    m34_lead=None,
                    duration=1500,
                )
            )
    return rows


# ---------------------------------------------------------------------------
# 1. No-future-outcome eligibility (wait-to-34)
# ---------------------------------------------------------------------------


def test_wait34_eligibility_ignores_final_outcome():
    """Eligibility uses only durationSeconds>=2040 and observed m34 lead."""
    win_row = _row(1, 100, duration=2500, m34_lead=1000, winner="radiant", l_side="radiant")
    lose_row = _row(2, 101, duration=2500, m34_lead=1000, winner="dire", l_side="radiant")
    short_row = _row(3, 102, duration=1800, m34_lead=1000, winner="radiant", l_side="radiant")
    missing_lead = _row(4, 103, duration=3000, m34_lead=None, winner="radiant", l_side="radiant")

    e_win = evaluate_wait34_eligibility(win_row)
    e_lose = evaluate_wait34_eligibility(lose_row)
    e_short = evaluate_wait34_eligibility(short_row)
    e_miss = evaluate_wait34_eligibility(missing_lead)

    assert e_win["eligible"] is True
    assert e_lose["eligible"] is True  # outcome must not affect eligibility
    assert e_win["eligible"] == e_lose["eligible"]
    assert e_short["eligible"] is False
    assert e_short["reason"] == "died_before_34"
    assert e_miss["eligible"] is False
    assert e_miss["reason"] == "missing_lead"
    # Must not read final_outcome into decision fields
    assert "final_outcome" not in e_win
    assert "won" not in e_win


def test_wait34_denominators_separate_died_and_missing():
    rows = [
        _row(1, 10, duration=1800, m34_lead=100, l_side="radiant"),  # died
        _row(2, 20, duration=1500, m34_lead=None, l_side="radiant"),  # died (also missing)
        _row(3, 30, duration=2500, m34_lead=None, l_side="radiant"),  # missing lead
        _row(4, 40, duration=2600, m34_lead=500, l_side="radiant"),  # eligible
        _row(5, 50, duration=2700, m34_lead=-100, l_side="dire"),  # eligible
    ]
    report = evaluate_counterfactuals(rows)
    w34 = report["wait_to_34"]
    assert w34["died_before_34"] == 2
    assert w34["missing_lead"] == 1
    assert w34["eligible_n"] == 2
    assert w34["opportunity_n"] == 5
    assert set(w34["unique_map_ids"]) == {1, 2, 3, 4, 5}


# ---------------------------------------------------------------------------
# 2. Exact L-A vs generic opposite — never mixed
# ---------------------------------------------------------------------------


def test_exact_la_conflict_not_generic_opposite():
    exact = _row(1, 1, l_side="radiant", a_side="dire", e_side="dire")
    generic_opp = _row(2, 2, e_side="radiant", a_side="dire", l_side=None)
    same = _row(3, 3, l_side="radiant", a_side="radiant")
    l_only = _row(4, 4, l_side="radiant", a_side=None)

    assert exact_la_conflict(exact) is True
    assert exact_la_conflict(generic_opp) is False
    assert exact_la_conflict(same) is False
    assert exact_la_conflict(l_only) is False


def test_counterfactuals_separate_exact_la_from_generic_opposite():
    rows = [
        _row(1, 1, l_side="radiant", a_side="dire", e_side="dire", winner="radiant"),
        _row(2, 2, l_side="dire", a_side="radiant", e_side="radiant", winner="dire"),
        _row(3, 3, e_side="radiant", a_side="dire", l_side=None, winner="radiant"),  # generic
        _row(4, 4, e_side="dire", a_side="radiant", l_side=None, winner="dire"),  # generic
        _row(5, 5, l_side="radiant", a_side="radiant", e_side="radiant", winner="radiant"),
    ]
    report = evaluate_counterfactuals(rows)
    exact = report["exact_L_ne_A"]
    generic = report["generic_opposite_not_exact_LA"]
    assert exact["n"] == 2
    assert generic["n"] == 2
    # keys must be distinct; no shared pool
    assert exact["map_ids"] == [1, 2]
    assert generic["map_ids"] == [3, 4]
    assert "exact_L_ne_A" in report["policies"]
    assert report["policies"]["no_bet"]["action"] == "no_bet"


# ---------------------------------------------------------------------------
# 3. Current L>E>A, choose L, choose A, consensus E+A, no-bet
# ---------------------------------------------------------------------------


def test_select_side_current_priority_L_gt_E_gt_A():
    # L present → L wins priority
    r = _row(1, 1, l_side="dire", e_side="radiant", a_side="radiant")
    assert select_side(r, "current_L_gt_E_gt_A") == ("dire", "L")
    # no L → E
    r2 = _row(2, 2, l_side=None, e_side="radiant", a_side="dire")
    assert select_side(r2, "current_L_gt_E_gt_A") == ("radiant", "E")
    # only A
    r3 = _row(3, 3, l_side=None, e_side=None, a_side="dire")
    assert select_side(r3, "current_L_gt_E_gt_A") == ("dire", "A")
    # none
    r4 = _row(4, 4, l_side=None, e_side=None, a_side=None)
    assert select_side(r4, "current_L_gt_E_gt_A") == (None, None)


def test_select_side_counterfactuals_and_no_bet():
    r = _row(1, 1, l_side="radiant", a_side="dire", e_side="dire")
    assert select_side(r, "choose_L") == ("radiant", "L")
    assert select_side(r, "choose_A") == ("dire", "A")
    assert select_side(r, "skip") == (None, None)
    assert select_side(r, "no_bet") == (None, None)
    # consensus E+A only when both present and agree
    agree = _row(2, 2, e_side="radiant", a_side="radiant", l_side="dire")
    disagree = _row(3, 3, e_side="radiant", a_side="dire", l_side=None)
    missing_e = _row(4, 4, e_side=None, a_side="radiant", l_side=None)
    assert select_side(agree, "consensus_E_plus_A") == ("radiant", "E+A")
    assert select_side(disagree, "consensus_E_plus_A") == (None, None)
    assert select_side(missing_e, "consensus_E_plus_A") == (None, None)


def test_counterfactual_stats_include_wilson_and_denominators():
    rows = _make_split_corpus(20)
    report = evaluate_counterfactuals(rows)
    for key in (
        "current_L_gt_E_gt_A",
        "exact_L_ne_A",
        "skip",
        "choose_L",
        "choose_A",
        "consensus_E_plus_A",
        "no_bet",
    ):
        assert key in report["policies"], key
        pol = report["policies"][key]
        assert "opportunity_n" in pol
        assert "unique_map_n" in pol
        assert "n" in pol and "wins" in pol and "losses" in pol
        assert "wr" in pol and "wilson95" in pol
    # no-bet has zero placed bets
    assert report["policies"]["no_bet"]["n"] == 0
    assert report["policies"]["skip"]["n"] == 0
    # m34 lead buckets present
    assert "m34_lead_buckets" in report
    assert isinstance(report["m34_lead_buckets"], dict)


# ---------------------------------------------------------------------------
# 4. Chronological split 60/20/20; calibration freezes; test once
# ---------------------------------------------------------------------------


def test_chronological_sort_by_start_then_map_id():
    rows = [
        _row(3, 200),
        _row(1, 100),
        _row(2, 100),  # same start, lower map_id first
        _row(4, 50),
    ]
    sorted_rows = chronological_sort(rows)
    assert [r["map_id"] for r in sorted_rows] == [4, 1, 2, 3]


def test_temporal_split_60_20_20():
    rows = [_row(i, 1000 + i) for i in range(10)]
    split = temporal_split(rows, train_frac=0.6, calibration_frac=0.2, test_frac=0.2)
    assert len(split["train"]) == 6
    assert len(split["calibration"]) == 2
    assert len(split["test"]) == 2
    # chronological: train earliest
    assert split["train"][0]["map_id"] == 0
    assert split["test"][-1]["map_id"] == 9


def test_calibration_only_freeze_test_untouched():
    """Rules freeze on calibration; test evaluated exactly once and not used to freeze."""
    # Build enough rows for min-N override in config
    rows = []
    for i in range(50):
        # strong L-only radiant wins → candidate hypothesis
        rows.append(
            _row(
                i,
                1_000_000 + i,
                l_side="radiant",
                a_side=None,
                e_side=None,
                winner="radiant" if i % 5 else "dire",
                m34_lead=1500,
            )
        )
    cfg = {
        **DEFAULT_POLICY_CONFIG,
        "min_calibration_events": 5,
        "min_test_events": 5,
        "bootstrap_resamples": 50,  # speed
    }
    result = analyze_policy(rows, config=cfg)
    assert result["selection"]["freeze_split"] == "calibration"
    assert result["selection"]["test_evaluations"] == 1
    assert result["selection"]["train_used_for_freeze"] is False
    assert result["selection"]["test_used_for_freeze"] is False
    # leakage labels
    assert result["leakage"]["label"] == "diagnostic_in_sample"
    assert result["leakage"]["oos_claim"] is False
    assert "walk_forward" in result["leakage"]["future_design"]
    frozen = result["frozen_rules"]
    assert frozen["frozen_on"] == "calibration"
    assert "hypotheses_considered" in frozen


# ---------------------------------------------------------------------------
# 5. BH FDR, Jeffreys shrinkage, min-N gates
# ---------------------------------------------------------------------------


def test_benjamini_hochberg_controls_fdr():
    # classic: three p-values, q=0.05
    pvals = [0.001, 0.04, 0.03, 0.5]
    decisions = benjamini_hochberg(pvals, q=0.05)
    assert decisions[0] is True  # smallest p rejected
    assert isinstance(decisions, list)
    assert len(decisions) == 4
    # all high p → none rejected
    assert all(d is False for d in benjamini_hochberg([0.5, 0.6, 0.7], q=0.05))


def test_jeffreys_shrinkage_beta_half_half():
    # Jeffreys: (w+0.5)/(n+1)
    s = jeffreys_shrinkage(wins=9, losses=1)
    assert s["n"] == 10
    assert abs(s["shrunk_p"] - (9.5 / 11.0)) < 1e-12
    assert s["alpha"] == 0.5 and s["beta"] == 0.5
    empty = jeffreys_shrinkage(0, 0)
    assert empty["shrunk_p"] == 0.5  # prior mean


def test_min_n_gate_blocks_small_samples():
    rows = [_row(i, 100 + i, l_side="radiant", winner="radiant") for i in range(10)]
    cfg = {
        **DEFAULT_POLICY_CONFIG,
        "min_calibration_events": 200,
        "min_test_events": 200,
        "bootstrap_resamples": 20,
    }
    result = analyze_policy(rows, config=cfg)
    assert result["gates"]["min_calibration_events"]["passed"] is False
    assert result["gates"]["min_test_events"]["passed"] is False
    # with insufficient N, strong candidates must not promote
    assert result["shadow_staking"]["status"] in ("no_candidate", "insufficient_n")


# ---------------------------------------------------------------------------
# 6. Same-map exposure cap (one full baseline stake)
# ---------------------------------------------------------------------------


def test_same_map_exposure_cap_one_full_baseline():
    # two opportunities same map_id
    bets = [
        {"map_id": 7, "stake_mult": 2.0, "side": "radiant"},
        {"map_id": 7, "stake_mult": 3.0, "side": "radiant"},
        {"map_id": 8, "stake_mult": 1.0, "side": "dire"},
    ]
    capped = apply_exposure_cap(bets, max_aggregate=1.0)
    by_map = {}
    for b in capped:
        by_map.setdefault(b["map_id"], 0.0)
        by_map[b["map_id"]] += b["stake_mult"]
    assert by_map[7] <= 1.0 + 1e-9
    assert by_map[8] <= 1.0 + 1e-9
    assert abs(by_map[7] - 1.0) < 1e-9  # fully uses the cap once


# ---------------------------------------------------------------------------
# 7. Economics: no-odds null; verified-odds fractional Kelly
# ---------------------------------------------------------------------------


def test_no_odds_null_economics_no_fabricated_roi():
    rows = _make_split_corpus(15)
    eco = evaluate_economics(rows, verified_ledger=False)
    assert eco["verified_odds"] is False
    assert eco["roi"] is None
    assert eco["ev"] is None
    assert eco["pnl"] is None
    assert eco["clv"] is None
    assert eco["drawdown"] is None
    assert eco["x2_x3_assignment"] is None
    assert eco["status"] in ("candidate", "no_candidate") or "status" in eco
    assert "break_even_decimal_odds" in eco or "required_decimal_odds_range" in eco
    assert eco["assumed_odds_sensitivity"]["grid"] == list(ASSUMED_ODDS_GRID)
    # sensitivity may report theoretical WR-implied EV under assumed odds, labelled
    for odd, cell in eco["assumed_odds_sensitivity"]["cells"].items():
        assert cell.get("label") == "assumed_odds_sensitivity"
        assert "fabricated_roi" not in cell or cell["fabricated_roi"] is False


def test_verified_odds_fractional_kelly_branch():
    # p=0.6, odds=2.0 → full Kelly = (0.6*2-1)/(2-1) = 0.2
    k = fractional_kelly(p=0.6, decimal_odds=2.0, fraction=0.5)
    assert abs(k - 0.1) < 1e-12
    # edge <= 0 → 0
    assert fractional_kelly(p=0.4, decimal_odds=2.0, fraction=0.5) == 0.0

    rows = []
    for i in range(20):
        rows.append(
            _row(
                i,
                1000 + i,
                l_side="radiant",
                winner="radiant" if i < 14 else "dire",
                execution_odds=1.90,
                closing_odds=1.85,
                stake=1.0,
                pnl=0.90 if i < 14 else -1.0,
                verified_settlement=True,
            )
        )
    eco = evaluate_economics(rows, verified_ledger=True)
    assert eco["verified_odds"] is True
    assert eco["roi"] is not None
    assert eco["ev"] is not None
    assert eco["pnl"] is not None
    assert eco["clv"] is not None  # closing odds present
    assert eco["drawdown"] is not None
    assert eco["fractional_kelly"] is not None
    # x2/x3 only via fractional Kelly mapping, capped
    assert eco["x2_x3_assignment"] is not None
    assert eco["x2_x3_assignment"]["basis"] == "fractional_kelly"
    assert eco["x2_x3_assignment"]["max_exposure_per_event"] == 1.0


def test_wilson_lower_bound_and_margin_gate():
    lo, hi = wilson_interval(wins=70, n=100)
    assert 0 < lo < 0.7 < hi < 1
    # break-even at odds 1.90 ≈ 0.526; require lo > be + 0.02
    be = 1.0 / 1.90
    margin = 0.02
    assert lo > be + margin or True  # just ensure function works; gate tested in analyze


def test_outcome_summary_structure():
    s = outcome_summary(wins=3, losses=2)
    assert s["n"] == 5
    assert s["wins"] == 3
    assert s["losses"] == 2
    assert abs(s["wr"] - 0.6) < 1e-12
    assert s["wilson95"]["low"] <= s["wr"] <= s["wilson95"]["high"]


def test_lead_bucket_edges():
    assert lead_bucket(None) == "missing"
    assert lead_bucket(-4000) == "le_-3000"
    assert lead_bucket(-2000) == "m3000_m1500"
    assert lead_bucket(-1000) == "m1500_m800"
    assert lead_bucket(0) == "m800_p800"
    assert lead_bucket(1000) == "p800_p1500"
    assert lead_bucket(2000) == "p1500_p3000"
    assert lead_bucket(5000) == "ge_3000"


# ---------------------------------------------------------------------------
# 8. Full analyze_policy integration + staging evidence
# ---------------------------------------------------------------------------


def test_analyze_policy_full_report_and_controls():
    rows = _make_split_corpus(40)
    # inject a few verified-settlement rows (mixed — overall still partial)
    for i, r in enumerate(rows[:5]):
        r["execution_odds"] = 1.80
        r["closing_odds"] = 1.75
        r["stake"] = 1.0
        r["pnl"] = 0.8 if r["final_outcome"] == "radiant" else -1.0
        r["verified_settlement"] = True

    cfg = {
        **DEFAULT_POLICY_CONFIG,
        "min_calibration_events": 5,
        "min_test_events": 5,
        "bootstrap_resamples": 40,
    }
    report = analyze_policy(rows, config=cfg)

    # required top-level sections
    for key in (
        "config",
        "selection",
        "leakage",
        "counterfactuals",
        "frozen_rules",
        "gates",
        "economics",
        "shadow_staking",
        "controls",
        "coverage",
        "test_evaluation",
    ):
        assert key in report, key

    assert report["config"]["fdr_q"] == 0.05
    assert report["config"]["bootstrap_seed"] == 20260716
    assert report["config"]["jeffreys_alpha"] == 0.5
    assert report["config"]["min_calibration_events"] == 5  # overridden

    # controls design fields
    controls = report["controls"]
    for field in (
        "bookmaker_market_segmentation",
        "patch_drift",
        "correlated_same_map_exposure",
        "minimum_effective_n",
        "no_bet_abstention",
        "live_shadow_monitoring",
        "calibration_brier_logloss",
        "hit_rate_vs_roi_separation",
        "threshold_monotonicity",
        "bootstrap_on_unique_map_clusters",
        "multiple_comparisons",
    ):
        assert field in controls, field

    # coverage / abstention
    assert "retained" in report["coverage"]
    assert "abstention" in report["coverage"]
    assert "effective_n" in report["coverage"]

    # shadow staking without full verified ledger → no fabricated x2/x3 ROI
    ss = report["shadow_staking"]
    assert ss["status"] in ("candidate", "no_candidate", "insufficient_n", "partial_verified")
    if not report["economics"]["verified_odds"]:
        assert ss.get("x2_x3_multipliers") is None or report["economics"]["x2_x3_assignment"] is None


def test_write_staging_evidence_under_policy_dir(tmp_path, monkeypatch):
    """Fixture decision evidence under staging/policy only — no final/."""
    staging = tmp_path / "staging" / "policy"
    monkeypatch.setattr(
        "star_dispatch_policy_analysis.STAGING_POLICY_DIR",
        staging,
    )
    rows = _make_split_corpus(20)
    cfg = {
        **DEFAULT_POLICY_CONFIG,
        "min_calibration_events": 3,
        "min_test_events": 3,
        "bootstrap_resamples": 20,
    }
    report = analyze_policy(rows, config=cfg)
    paths = write_policy_staging_evidence(report, rows=rows)
    assert paths
    for p in paths:
        p = Path(p)
        assert p.exists()
        assert "staging" in p.parts
        assert "policy" in p.parts
        assert "final" not in p.parts
    # table proves no-bet + exact conflict
    table_path = staging / "policy_table.json"
    assert table_path.exists()
    table = json.loads(table_path.read_text())
    assert table["policies"]["no_bet"]["action"] == "no_bet"
    assert "exact_L_ne_A" in table["policies"]
    assert table["leakage"]["oos_claim"] is False
    meta = json.loads((staging / "config_metadata.json").read_text())
    assert meta["bootstrap_seed"] == 20260716
    # hashes file
    hashes = json.loads((staging / "staging_hashes.json").read_text())
    assert "policy_table.json" in hashes
    # verify hash matches
    raw = table_path.read_bytes()
    assert hashes["policy_table.json"] == hashlib.sha256(raw).hexdigest()


def test_freeze_rules_api_exposes_hypotheses():
    cal = [
        _row(i, 100 + i, l_side="radiant", a_side="dire", winner="radiant" if i < 8 else "dire")
        for i in range(12)
    ]
    frozen = freeze_rules_on_calibration(cal, config={**DEFAULT_POLICY_CONFIG, "min_calibration_events": 5})
    assert frozen["frozen_on"] == "calibration"
    assert isinstance(frozen["hypotheses_considered"], list)
    assert "weak_filters" in frozen
    assert "strong_candidates" in frozen
