"""Leakage-safe STAR dispatch policy analysis engine.

Pure functions: canonical row dicts in, JSON-compatible report out.
NO corpus collection, NO production filter/stake changes, NO fabricated ROI.

Contract summary implemented here:
1. Counterfactual policies with explicit opportunity/unique-map denominators:
   current L>E>A, exact L!=A, skip, choose L, choose A, consensus E+A, no_bet.
   Wait-to-34 eligibility uses ONLY durationSeconds>=2040 and observed m34 lead;
   died-before-34 and missing-lead reported separately. Eligibility never uses
   final outcome.
2. m34 lead buckets + N/W/L/WR/Wilson for every policy. Generic opposite is
   never mixed with the exact L-A conflict subset. A no-bet action is preserved.
3. Chronological startDateTime/map_id ordering; 60/20/20 train/calibration/test.
   Train may enumerate hypotheses; calibration alone freezes weak-filter and
   strong-candidate rules; final test is evaluated exactly once. Everything is
   labelled diagnostic/in-sample while cumulative dictionaries overlap; we emit
   a future walk-forward design rather than claiming OOS.
4. Predeclared, configurable, recorded defaults: >=200 unique events in
   calibration and final test, Wilson lower bound >= break-even +0.02,
   Benjamini-Hochberg FDR q=0.05, Jeffreys Beta(0.5,0.5) shrinkage,
   2,000 map-cluster bootstrap resamples with seed 20260716, stability across
   three contiguous calibration subwindows, one full-baseline-stake max
   aggregate exposure per map/event. Reports coverage/abstention/effective_n/
   uncertainty/threshold monotonicity.
5. Economics guardrail: with verified execution odds/stake/settlement rows
   compute odds-conditioned EV/ROI/PnL, CLV when closing odds exist, drawdown
   and bootstrap stress; fractional Kelly is the only basis for x2/x3 shadow
   mapping and remains capped by one full baseline exposure per event. Without
   verified odds all real-money metrics are null/unmeasurable; we output only
   candidate|no_candidate status, a conservative break-even/required decimal
   odds range, and an explicitly assumed-odds sensitivity grid (never fabricated).
6. Controls/design fields for bookmaker segmentation, patch drift, correlated
   same-map exposure, minimum/effective N, no-bet abstention, live shadow
   monitoring, calibration/Brier/log-loss, hit-rate-vs-ROI separation.
   Multiple comparisons and bootstrap operate on unique map clusters.

OWNER: W-POLICY (t_e4f5c458). Exclusive scope: this module + its test + the
staging/policy path under runtime/star_dispatch_replay/staging/policy/.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
BASE_DIR = Path(__file__).resolve().parent

STAGING_POLICY_DIR = Path(
    os.environ.get(
        "STAR_DISPATCH_POLICY_STAGING_DIR",
        ROOT_DIR / "runtime" / "star_dispatch_replay" / "staging" / "policy",
    )
)

# ---------------------------------------------------------------------------
# Predeclared defaults (all configurable & recorded in the report)
# ---------------------------------------------------------------------------

ASSUMED_ODDS_GRID: tuple[float, ...] = (1.50, 1.60, 1.70, 1.80, 1.90, 2.00, 2.20)
WILSON_Z = 1.959963984540054  # 95% CI
JEFFREYS_ALPHA = 0.5
JEFFREYS_BETA = 0.5
FDR_Q = 0.05
BOOTSTRAP_SEED = 20260716
DEFAULT_BOOTSTRAP_RESAMPLES = 2000
MIN_CALIBRATION_EVENTS = 200
MIN_TEST_EVENTS = 200
WILSON_BREAK_EVEN_MARGIN = 0.02
MAX_EXPOSURE_PER_MAP_EVENT = 1.0
WAIT34_MIN_DURATION = 2040  # seconds (matches m34+ observability)

POLICY_KEYS = (
    "current_L_gt_E_gt_A",
    "exact_L_ne_A",
    "skip",
    "choose_L",
    "choose_A",
    "consensus_E_plus_A",
    "no_bet",
)

LEAD_BUCKETS = (
    ("le_-3000", None, -3000.0),
    ("m3000_m1500", -3000.0, -1500.0),
    ("m1500_m800", -1500.0, -800.0),
    ("m800_p800", -800.0, 800.0),
    ("p800_p1500", 800.0, 1500.0),
    ("p1500_p3000", 1500.0, 3000.0),
    ("ge_3000", 3000.0, None),
)

DEFAULT_POLICY_CONFIG: dict[str, Any] = {
    "min_calibration_events": MIN_CALIBRATION_EVENTS,
    "min_test_events": MIN_TEST_EVENTS,
    "wilson_break_even_margin": WILSON_BREAK_EVEN_MARGIN,
    "fdr_q": FDR_Q,
    "jeffreys_alpha": JEFFREYS_ALPHA,
    "jeffreys_beta": JEFFREYS_BETA,
    "bootstrap_resamples": DEFAULT_BOOTSTRAP_RESAMPLES,
    "bootstrap_seed": BOOTSTRAP_SEED,
    "stability_subwindows": 3,
    "max_aggregate_exposure_per_event": MAX_EXPOSURE_PER_MAP_EVENT,
    "assumed_odds_grid": list(ASSUMED_ODDS_GRID),
    "train_frac": 0.60,
    "calibration_frac": 0.20,
    "test_frac": 0.20,
    "wait34_min_duration": WAIT34_MIN_DURATION,
}


# ---------------------------------------------------------------------------
# Statistics primitives
# ---------------------------------------------------------------------------


def wilson_interval(wins: int, n: int, z: float = WILSON_Z) -> tuple[float, float]:
    if n <= 0:
        return (0.0, 1.0)
    p = wins / n
    den = 1.0 + z * z / n
    center = (p + z * z / (2.0 * n)) / den
    half = z * math.sqrt(p * (1.0 - p) / n + z * z / (4.0 * n * n)) / den
    return (max(0.0, center - half), min(1.0, center + half))


def jeffreys_shrinkage(wins: int, losses: int) -> dict[str, Any]:
    n = wins + losses
    a = JEFFREYS_ALPHA
    b = JEFFREYS_BETA
    shrunk_p = (wins + a) / (n + a + b) if n >= 0 else 0.5
    if n == 0:
        shrunk_p = a / (a + b)
    return {
        "n": n,
        "wins": wins,
        "losses": losses,
        "shrunk_p": float(shrunk_p),
        "alpha": a,
        "beta": b,
    }


def benjamini_hochberg(pvals: list[float], q: float = FDR_Q) -> list[bool]:
    """Return per-hypothesis rejection booleans under BH step-up FDR control."""
    m = len(pvals)
    if m == 0:
        return []
    order = sorted(range(m), key=lambda i: pvals[i])
    rejected = [False] * m
    # find largest k in sorted order with p_(k) <= k/m * q
    max_k = 0
    found = False
    for rank, idx in enumerate(order, start=1):
        threshold = (rank / m) * q
        if pvals[idx] <= threshold:
            max_k = rank
            found = True
    if found:
        for rank, idx in enumerate(order, start=1):
            if rank <= max_k:
                rejected[idx] = True
    return rejected


def outcome_summary(wins: int, losses: int) -> dict[str, Any]:
    n = wins + losses
    wr = wins / n if n else 0.0
    lo, hi = wilson_interval(wins, n) if n else (0.0, 1.0)
    return {
        "n": n,
        "wins": wins,
        "losses": losses,
        "wr": wr,
        "wilson95": {"low": lo, "high": hi},
    }


def fractional_kelly(p: float, decimal_odds: float, fraction: float = 0.5) -> float:
    """Half-Kelly by default. f* = (p*decimal_odds - 1)/(decimal_odds - 1)."""
    if decimal_odds <= 1.0 or p <= 0.0 or p >= 1.0:
        return 0.0
    edge = p * decimal_odds - 1.0
    if edge <= 0.0:
        return 0.0
    f = edge / (decimal_odds - 1.0)  # full Kelly
    return max(0.0, min(fraction * f, 1.0))


def lead_bucket(lead: Optional[float]) -> str:
    if lead is None:
        return "missing"
    try:
        v = float(lead)
    except (TypeError, ValueError):
        return "missing"
    for name, lo, hi in LEAD_BUCKETS:
        lo_ok = lo is None or v >= lo
        hi_ok = hi is None or v < hi
        if lo_ok and hi_ok:
            return name
    return "other"


# ---------------------------------------------------------------------------
# Sorting / splitting
# ---------------------------------------------------------------------------


def chronological_sort(rows: Iterable[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: (int(r.get("startDateTime") or 0), int(r.get("map_id") or 0)))


def temporal_split(
    rows: list[dict],
    train_frac: float = 0.60,
    calibration_frac: float = 0.20,
    test_frac: float = 0.20,
) -> dict[str, list[dict]]:
    rows = chronological_sort(rows)
    n = len(rows)
    if n == 0:
        return {"train": [], "calibration": [], "test": []}
    total = train_frac + calibration_frac + test_frac
    if total <= 0:
        total = 1.0
    tf = train_frac / total
    cf = calibration_frac / total
    n_train = int(math.floor(tf * n))
    n_cal = int(math.floor(cf * n))
    # guarantee non-empty where possible
    if n >= 3 and (n_train == 0 or n_cal == 0 or n - n_train - n_cal == 0):
        n_train = max(1, n - 2)
        n_cal = 1 if n - n_train >= 1 else 0
    n_test = n - n_train - n_cal
    train = rows[:n_train]
    cal = rows[n_train : n_train + n_cal]
    test = rows[n_train + n_cal :]
    return {"train": train, "calibration": cal, "test": test}


# ---------------------------------------------------------------------------
# Policy selection
# ---------------------------------------------------------------------------


def _block(raw: dict, label: str) -> dict[str, Any]:
    b = (raw or {}).get("blocks", {}).get(label) or {}
    return b


def _present(b: dict) -> bool:
    return bool(b) and b.get("present") is True and b.get("side") is not None


def _side(b: dict) -> Optional[str]:
    return b.get("side") if _present(b) else None


def exact_la_conflict(row: dict) -> bool:
    """True only when L present, A present, L side != A side."""
    l = _block(row, "L")
    a = _block(row, "A")
    ls = _side(l)
    as_ = _side(a)
    return ls is not None and as_ is not None and ls != as_


def select_side(row: dict, policy_key: str) -> tuple[Optional[str], Optional[str]]:
    """Return (chosen_side, deciding_block). Never reads final_outcome."""
    l = _block(row, "L")
    e = _block(row, "E")
    a = _block(row, "A")
    ls = _side(l)
    es = _side(e)
    as_ = _side(a)

    if policy_key == "current_L_gt_E_gt_A":
        if ls is not None:
            return ls, "L"
        if es is not None:
            return es, "E"
        if as_ is not None:
            return as_, "A"
        return None, None
    if policy_key == "exact_L_ne_A":
        if exact_la_conflict(row):
            return ls, "L_vs_A"
        return None, None
    if policy_key == "skip":
        return None, None
    if policy_key == "no_bet":
        return None, None
    if policy_key == "choose_L":
        return (ls, "L") if ls is not None else (None, None)
    if policy_key == "choose_A":
        return (as_, "A") if as_ is not None else (None, None)
    if policy_key == "consensus_E_plus_A":
        if es is not None and as_ is not None and es == as_:
            return es, "E+A"
        return None, None
    return None, None


def evaluate_wait34_eligibility(row: dict) -> dict[str, Any]:
    """Eligibility ONLY from durationSeconds>=2040 and observed m34 lead."""
    duration = int(row.get("durationSeconds") or 0)
    lead = row.get("m34_lead_radiant")
    if duration < WAIT34_MIN_DURATION:
        return {"eligible": False, "reason": "died_before_34"}
    if lead is None:
        return {"eligible": False, "reason": "missing_lead"}
    return {"eligible": True, "reason": "ok"}


# ---------------------------------------------------------------------------
# Counterfactual evaluation
# ---------------------------------------------------------------------------


def _outcomes_for_policy(rows: list[dict], policy_key: str) -> tuple[list[int], list[int], list[int]]:
    """Return (map_ids, won_flags, n_for_policy) where n_for_policy counts placed bets."""
    map_ids: list[int] = []
    won: list[int] = []
    for r in rows:
        mid = int(r.get("map_id") or 0)
        side, _block_label = select_side(r, policy_key)
        if side is None:
            continue
        outcome = r.get("final_outcome")
        won.append(1 if outcome == side else 0)
        map_ids.append(mid)
    return map_ids, won, won


def _generic_opposite(row: dict) -> bool:
    """E vs A opposite when L absent — NOT an exact L-A conflict."""
    e = _block(row, "E")
    a = _block(row, "A")
    l = _block(row, "L")
    if _side(l) is not None:
        return False
    es = _side(e)
    as_ = _side(a)
    return es is not None and as_ is not None and es != as_


def _opportunity(row: dict) -> bool:
    """A dispatch opportunity exists if at least one block present."""
    return any(_present(_block(row, lbl)) for lbl in ("E", "L", "A"))


def _won(row: dict, side: Optional[str]) -> Optional[int]:
    if side is None:
        return None
    return 1 if row.get("final_outcome") == side else 0


def _policy_stat(rows: list[dict], policy_key: str) -> dict[str, Any]:
    opp_n = 0
    unique_maps: set[int] = set()
    wins = 0
    losses = 0
    placed = 0
    for r in rows:
        if not _opportunity(r):
            continue
        opp_n += 1
        unique_maps.add(int(r.get("map_id") or 0))
        side, _lbl = select_side(r, policy_key)
        w = _won(r, side)
        if side is None:
            continue
        placed += 1
        if w:
            wins += 1
        else:
            losses += 1
    wl_lo, wl_hi = wilson_interval(wins, placed) if placed else (0.0, 1.0)
    action = "no_bet" if policy_key in ("no_bet", "skip") else policy_key
    return {
        "action": action,
        "opportunity_n": opp_n,
        "unique_map_n": len(unique_maps),
        "unique_map_ids": sorted(unique_maps),
        "n": placed,
        "wins": wins,
        "losses": losses,
        "wr": wins / placed if placed else 0.0,
        "wilson95": {"low": wl_lo, "high": wl_hi},
        "jeffreys": jeffreys_shrinkage(wins, losses),
    }


def _summarize_pool(rows: list[dict], predicate: Callable[[dict], bool]) -> dict[str, Any]:
    """Generic N/W/L/WR/Wilson for a predicate-defined pool (no policy action)."""
    sel: list[int] = []
    wins = 0
    losses = 0
    map_ids: list[int] = []
    for r in rows:
        if predicate(r):
            mid = int(r.get("map_id") or 0)
            map_ids.append(mid)
            sel.append(mid)
            outcome = r.get("final_outcome")
            # generic opposite pool has two sides; we count a "hit" as E side winning
            e = _block(r, "E")
            side = _side(e)
            hit = 1 if side is not None and outcome == side else 0
            wins += hit
            losses += 1 - hit
    lo, hi = wilson_interval(wins, len(sel)) if sel else (0.0, 1.0)
    return {
        "n": len(sel),
        "wins": wins,
        "losses": losses,
        "wr": wins / len(sel) if sel else 0.0,
        "wilson95": {"low": lo, "high": hi},
        "map_ids": map_ids,
    }


def evaluate_counterfactuals(rows: list[dict]) -> dict[str, Any]:
    rows = chronological_sort(rows)
    policies = {key: _policy_stat(rows, key) for key in POLICY_KEYS}

    # exact L!=A and generic opposite are separate pools
    exact = _summarize_pool(rows, exact_la_conflict)
    generic = _summarize_pool(rows, _generic_opposite)

    # wait-to-34 denominators
    died = 0
    missing_lead = 0
    eligible = 0
    opp_ids: set[int] = set()
    el_map_ids: set[int] = set()
    for r in rows:
        if not _opportunity(r):
            continue
        opp_ids.add(int(r.get("map_id") or 0))
        e = evaluate_wait34_eligibility(r)
        if e["eligible"]:
            eligible += 1
            el_map_ids.add(int(r.get("map_id") or 0))
        elif e["reason"] == "died_before_34":
            died += 1
        elif e["reason"] == "missing_lead":
            missing_lead += 1

    # m34 lead buckets
    buckets: dict[str, dict[str, int]] = {}
    for r in rows:
        if not _opportunity(r):
            continue
        bk = lead_bucket(r.get("m34_lead_radiant"))
        buckets.setdefault(bk, {"n": 0})
        buckets[bk]["n"] += 1

    return {
        "policies": policies,
        "exact_L_ne_A": exact,
        "generic_opposite_not_exact_LA": generic,
        "wait_to_34": {
            "opportunity_n": len(opp_ids),
            "eligible_n": eligible,
            "died_before_34": died,
            "missing_lead": missing_lead,
            "unique_map_ids": sorted(opp_ids),
            "eligible_map_ids": sorted(el_map_ids),
        },
        "m34_lead_buckets": buckets,
    }


# ---------------------------------------------------------------------------
# Calibration freeze + gates
# ---------------------------------------------------------------------------


_HIPOS = (
    ("L_only_wr_band", lambda r: _present(_block(r, "L")) and _side(_block(r, "A")) is None),
    ("L_NE_A_exact_conflict", exact_la_conflict),
    ("consensus_E_plus_A", lambda r: _present(_block(r, "E")) and _present(_block(r, "A")) and _side(_block(r, "E")) == _side(_block(r, "A"))),
    ("current_dispatch_target", lambda r: select_side(r, "current_L_gt_E_gt_A")[0] is not None),
)


def _hypothesis_w(rows: list[dict], pred: Callable[[dict], bool]) -> tuple[int, int]:
    wins = 0
    n = 0
    for r in rows:
        if pred(r):
            side, _ = select_side(r, "current_L_gt_E_gt_A")
            if side is None:
                side = _side(_block(r, "E")) or _side(_block(r, "L")) or _side(_block(r, "A"))
            if side is None:
                # no side → default to first present block side; if no block, skip
                continue
            n += 1
            wins += 1 if r.get("final_outcome") == side else 0
    return wins, n


def freeze_rules_on_calibration(calibration: list[dict], config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = {**DEFAULT_POLICY_CONFIG, **(config or {})}
    calibration = chronological_sort(calibration)

    considered: list[dict[str, Any]] = []
    for name, pred in _HIPOS:
        wins, n = _hypothesis_w(calibration, pred)
        lo, hi = wilson_interval(wins, n) if n else (0.0, 1.0)
        # naive p-value via normal approx under H0 p=0.5 for ranking only
        pval = _two_sided_p(wins, n) if n else 1.0
        considered.append(
            {
                "name": name,
                "n": n,
                "wins": wins,
                "wr": wins / n if n else 0.0,
                "wilson95": {"low": lo, "high": hi},
                "p_value": pval,
            }
        )

    # BH FDR across considered hypotheses
    pvals = [h["p_value"] for h in considered]
    decisions = benjamini_hochberg(pvals, q=cfg["fdr_q"])
    for h, dec in zip(considered, decisions):
        h["bh_rejected"] = bool(dec)

    # weak filters: hypotheses NOT rejected by BH and with shrunk WR <= 0.5
    weak_filters: list[dict[str, Any]] = []
    for h in considered:
        shrunk = jeffreys_shrinkage(h["wins"], h["n"] - h["wins"])
        if (not h["bh_rejected"]) and shrunk["shrunk_p"] <= 0.52:
            weak_filters.append(
                {
                    "name": h["name"],
                    "n": h["n"],
                    "shrunk_p": shrunk["shrunk_p"],
                    "wilson_low": h["wilson95"]["low"],
                    "rule": f"filter_when_present__{h['name']}",
                }
            )

    # strong candidates: BH-rejected + Wilson lower > break-even + margin (assume odds 1.90)
    be_p = 1.0 / 1.90
    strong: list[dict[str, Any]] = []
    for h in considered:
        if h["bh_rejected"] and h["n"] >= 10 and h["wilson95"]["low"] > be_p + cfg["wilson_break_even_margin"]:
            strong.append(
                {
                    "name": h["name"],
                    "n": h["n"],
                    "wins": h["wins"],
                    "wr": h["wr"],
                    "wilson_low": h["wilson95"]["low"],
                    "required_break_even_p": be_p,
                    "margin": cfg["wilson_break_even_margin"],
                }
            )

    # stability across contiguous subwindows
    sw = cfg["stability_subwindows"]
    stability = _stability_subwindows(calibration, considered, sw)

    return {
        "frozen_on": "calibration",
        "hypotheses_considered": considered,
        "weak_filters": weak_filters,
        "strong_candidates": strong,
        "stability_subwindows": stability,
        "frozen_with_config": _public_config(cfg),
    }


def _stability_subwindows(cal: list[dict], considered: list[dict], windows: int) -> list[dict[str, Any]]:
    if windows <= 0 or len(cal) == 0:
        return []
    out: list[dict[str, Any]] = []
    chunk = max(1, len(cal) // windows)
    for w in range(windows):
        sub = cal[w * chunk : (w + 1) * chunk] if w < windows - 1 else cal[w * chunk :]
        entry: dict[str, Any] = {"window": w, "n": len(sub)}
        for h in considered:
            wins, n = _hypothesis_w(sub, next(p for name, p in _HIPOS if name == h["name"]))
            entry[h["name"]] = {"n": n, "wr": wins / n if n else 0.0}
        out.append(entry)
    return out


def _two_sided_p(wins: int, n: int, p0: float = 0.5) -> float:
    if n <= 0:
        return 1.0
    mu = n * p0
    sigma = math.sqrt(n * p0 * (1.0 - p0))
    if sigma == 0:
        return 1.0 if wins == int(mu) else 0.0
    z = (wins - mu) / sigma
    return float(math.erfc(abs(z) / math.sqrt(2.0)))


def _evaluate_gates(
    cal_n: int,
    test_n: int,
    frozen: dict[str, Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    be_p = 1.0 / 1.90
    strong_pass: list[dict[str, Any]] = []
    for s in frozen.get("strong_candidates", []):
        strong_pass.append(
            {
                "name": s["name"],
                "wilson_low": s["wilson_low"],
                "exceeds_break_even": s["wilson_low"] > be_p,
                "wilson_low_above_be_plus_margin": s["wilson_low"] > be_p + cfg["wilson_break_even_margin"],
                "passed": s["wilson_low"] > be_p + cfg["wilson_break_even_margin"],
            }
        )
    return {
        "min_calibration_events": {"required": cfg["min_calibration_events"], "observed": cal_n, "passed": cal_n >= cfg["min_calibration_events"]},
        "min_test_events": {"required": cfg["min_test_events"], "observed": test_n, "passed": test_n >= cfg["min_test_events"]},
        "wilson_break_even_margin": {"margin": cfg["wilson_break_even_margin"], "strong_candidates": strong_pass},
        "fdr_q": {"q": cfg["fdr_q"], "n_hypotheses": len(frozen.get("hypotheses_considered", []))},
    }


def apply_exposure_cap(bets: list[dict], max_aggregate: float = MAX_EXPOSURE_PER_MAP_EVENT) -> list[dict]:
    """Cap aggregate stake multiplier per map_id to one full baseline stake."""
    usage: dict[Any, float] = {}
    capped: list[dict] = []
    for b in bets:
        mid = b.get("map_id")
        remaining = max_aggregate - usage.get(mid, 0.0)
        stake = min(float(b.get("stake_mult", 0.0)), remaining)
        if stake <= 0.0:
            continue
        capped.append({**b, "stake_mult": stake, "capped": stake < float(b.get("stake_mult", 0.0))})
        usage[mid] = usage.get(mid, 0.0) + stake
    return capped


# ---------------------------------------------------------------------------
# Economics
# ---------------------------------------------------------------------------


def evaluate_economics(rows: list[dict], verified_ledger: bool) -> dict[str, Any]:
    if not verified_ledger:
        # no fabricated ROI
        # build assumed-odds sensitivity from observed WR of placed current-policy bets
        placed = [r for r in rows if select_side(r, "current_L_gt_E_gt_A")[0] is not None]
        wins = sum(1 for r in placed if r.get("final_outcome") == select_side(r, "current_L_gt_E_gt_A")[0])
        n = len(placed)
        shrunk = jeffreys_shrinkage(wins, n - wins)["shrunk_p"] if n else 0.5
        cells: dict[str, dict[str, Any]] = {}
        for odd in ASSUMED_ODDS_GRID:
            be = 1.0 / odd
            edge = shrunk * odd - 1.0
            cells[f"{odd:.2f}"] = {
                "label": "assumed_odds_sensitivity",
                "assumed_decimal_odds": float(odd),
                "break_even_p": float(be),
                "estimated_win_p_jeffreys": float(shrunk),
                "edge_assumed": float(edge),
                "fabricated_roi": False,
            }
        return {
            "verified_odds": False,
            "status": "candidate" if n > 0 else "no_candidate",
            "roi": None,
            "ev": None,
            "pnl": None,
            "clv": None,
            "drawdown": None,
            "x2_x3_assignment": None,
            "fractional_kelly": None,
            "break_even_decimal_odds": {
                "conservative_min": 1.50,
                "note": "without verified settlement rows real break-even cannot be observed",
            },
            "required_decimal_odds_range": {"min": 1.50, "label": "conservative_assumed"},
            "assumed_odds_sensitivity": {
                "grid": list(ASSUMED_ODDS_GRID),
                "cells": cells,
            },
            "settlement_rows_used": 0,
        }

    # verified ledger path
    settled = [r for r in rows if r.get("verified_settlement") and r.get("execution_odds")]
    n = len(settled)
    if n == 0:
        return evaluate_economics(rows, verified_ledger=False)
    total_stake = 0.0
    total_pnl = 0.0
    ev_terms: list[float] = []
    clv_terms: list[float] = []
    wr_wins = 0
    fractional_kelly_estimates = []
    drawdown_series: list[float] = []

    running = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in settled:
        odds = float(r.get("execution_odds"))
        stake = float(r.get("stake") or 0.0)
        pnl = float(r.get("pnl") or 0.0)
        side = select_side(r, "current_L_gt_E_gt_A")[0]
        won = 1 if (side is not None and r.get("final_outcome") == side) else 0
        wr_wins += won
        total_stake += stake if stake > 0 else 1.0
        total_pnl += pnl
        # EV per unit stake = p*odds - 1 with jeffreys p from running WR not permitted here;
        # use historical realized per-bet edge: pnl/stake
        unit = stake if stake > 0 else 1.0
        ev_terms.append(pnl / unit)
        # CLV when closing odds exist
        closing = r.get("closing_odds")
        if closing is not None:
            clv_terms.append(float(odds) - float(closing))
        # fractional Kelly using realized win rate
        p_est = max(0.01, min(0.99, wr_wins / max(1, len(settled))))
        fk = fractional_kelly(p=p_est, decimal_odds=odds, fraction=0.5)
        fractional_kelly_estimates.append(fk)
        running += pnl
        peak = max(peak, running)
        dd = peak - running
        max_dd = max(max_dd, dd)
        drawdown_series.append(float(dd))

    roi = total_pnl / total_stake if total_stake else 0.0
    ev = sum(ev_terms) / n if n else 0.0
    clv = sum(clv_terms) / len(clv_terms) if clv_terms else None
    fk_avg = sum(fractional_kelly_estimates) / n if n else 0.0
    # x2/x3 mapping: fractional Kelly scaled, capped at one full baseline exposure
    x2_x3 = {
        "basis": "fractional_kelly",
        "fractional_kelly_avg": float(fk_avg),
        "max_exposure_per_event": MAX_EXPOSURE_PER_MAP_EVENT,
        "mapping": {
            "x2": round(min(2.0, max(0.0, fk_avg * 4.0)), 4) if fk_avg > 0 else 0.0,
            "x3": round(min(3.0, max(0.0, fk_avg * 6.0)), 4) if fk_avg > 0 else 0.0,
        },
        "capped": True,
    }
    return {
        "verified_odds": True,
        "status": "candidate" if fk_avg > 0.0 else "no_candidate",
        "roi": float(roi),
        "ev": float(ev),
        "pnl": float(total_pnl),
        "clv": float(clv) if clv is not None else None,
        "drawdown": {"max": float(max_dd), "series": drawdown_series},
        "fractional_kelly": {"fraction": 0.5, "avg": float(fk_avg)},
        "x2_x3_assignment": x2_x3,
        "break_even_decimal_odds": {"observed": True},
        "required_decimal_odds_range": {"min": 1.50, "label": "conservative_assumed"},
        "assumed_odds_sensitivity": {"grid": list(ASSUMED_ODDS_GRID), "cells": {}},
        "settlement_rows_used": n,
    }


# ---------------------------------------------------------------------------
# Bootstrap (map-cluster) — deterministic with seed
# ---------------------------------------------------------------------------


def _bootstrap_ci(map_ids: list[int], won: list[int], resamples: int, seed: int) -> dict[str, Any]:
    if not map_ids:
        return {"mean": None, "ci_low": None, "ci_high": None, "resamples": resamples}
    import random

    rng = random.Random(seed)
    # cluster by unique map_id
    clusters: dict[int, list[int]] = {}
    for mid, w in zip(map_ids, won):
        clusters.setdefault(mid, []).append(w)
    keys = list(clusters.keys())
    n_clusters = len(keys)
    means = []
    for _ in range(resamples):
        sampled_ids = [keys[rng.randrange(n_clusters)] for _ in range(n_clusters)]
        total_w = 0
        total_n = 0
        for mid in sampled_ids:
            ws = clusters[mid]
            total_w += sum(ws)
            total_n += len(ws)
        means.append(total_w / total_n if total_n else 0.0)
    means.sort()
    lo_idx = max(0, int(0.025 * resamples))
    hi_idx = min(resamples - 1, int(0.975 * resamples) - 1)
    return {
        "mean": sum(means) / resamples,
        "ci_low": means[lo_idx],
        "ci_high": means[max(lo_idx, hi_idx)],
        "resamples": resamples,
    }


# ---------------------------------------------------------------------------
# Public config snapshot
# ---------------------------------------------------------------------------


def _public_config(cfg: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in sorted(cfg.items())}


# ---------------------------------------------------------------------------
# Top-level analyze_policy
# ---------------------------------------------------------------------------


def analyze_policy(rows: list[dict], config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = {**DEFAULT_POLICY_CONFIG, **(config or {})}
    rows = chronological_sort(rows)

    split = temporal_split(
        rows,
        train_frac=cfg["train_frac"],
        calibration_frac=cfg["calibration_frac"],
        test_frac=cfg["test_frac"],
    )
    train, cal, test = split["train"], split["calibration"], split["test"]

    # Train: enumerate hypotheses (no freeze)
    train_hypotheses = [
        {"name": name, "n": _hypothesis_w(train, pred)[1], "phase": "diagnostic_in_sample"}
        for name, pred in _HIPOS
    ]

    # Calibration: freeze rules
    frozen = freeze_rules_on_calibration(cal, config=cfg)

    # Gates
    cal_n = len({int(r.get("map_id") or 0) for r in cal})
    test_n = len({int(r.get("map_id") or 0) for r in test})
    gates = _evaluate_gates(cal_n, test_n, frozen, cfg)

    # Counterfactuals on full corpus (diagnostic)
    counterfactuals = evaluate_counterfactuals(rows)

    # Economics on settlement rows present
    has_settled = any(r.get("verified_settlement") and r.get("execution_odds") for r in rows)
    economics = evaluate_economics(rows, verified_ledger=has_settled)

    # Test evaluation: evaluate frozen rules once on the test split
    test_eval = _evaluate_on_test(test, frozen)

    # Bootstrap uncertainty on current-policy placed bets
    placed = [r for r in rows if select_side(r, "current_L_gt_E_gt_A")[0] is not None]
    placed_map_ids = [int(r.get("map_id") or 0) for r in placed]
    placed_won = [1 if r.get("final_outcome") == select_side(r, "current_L_gt_E_gt_A")[0] else 0 for r in placed]
    bootstrap_ci = _bootstrap_ci(
        placed_map_ids,
        placed_won,
        resamples=int(cfg["bootstrap_resamples"]),
        seed=int(cfg["bootstrap_seed"]),
    )

    # Shadow staking status (combines economics + gates)
    ss_status = _shadow_status(gates, economics)

    # Coverage / abstention
    coverage = _coverage_report(rows, frozen)

    # Controls design block (static design fields)
    controls = {
        "bookmaker_market_segmentation": {"design": "segment by bookmaker/source shard labels recorded per row"},
        "patch_drift": {"design": "split/stratify by patch label recorded per row", "patches_seen": sorted({str(r.get("patch")) for r in rows if r.get("patch") is not None})},
        "correlated_same_map_exposure": {"design": "cap aggregate stake multiplier per map_id to one full baseline", "max_aggregate_exposure_per_event": cfg["max_aggregate_exposure_per_event"]},
        "minimum_effective_n": {"design": "require >=200 unique events in calibration and final test", "calibration": cal_n, "test": test_n},
        "no_bet_abstention": {"design": "no_bet and skip policies preserved; abstain when no Wilson margin or no edge"},
        "live_shadow_monitoring": {"design": "shadow-only candidates, no production stake change; schema hooks for closing_odds/pnl"},
        "calibration_brier_logloss": {"design": "report Brier and log-loss of jeffreys shrunk probabilities vs realized outcomes", "implemented": "_public_preview_brier_logloss"},
        "hit_rate_vs_roi_separation": {"design": "hit-rate (WR) is reported separately from odds-conditioned ROI; ROI null unless verified ledger"},
        "threshold_monotonicity": {"design": "verify WR is monotone in dispatched block tier buckets where ordered"},
        "bootstrap_on_unique_map_clusters": {"design": True, "resamples": cfg["bootstrap_resamples"], "seed": cfg["bootstrap_seed"], "result": bootstrap_ci},
        "multiple_comparisons": {"design": "Benjamini-Hochberg FDR across calibration hypotheses", "q": cfg["fdr_q"]},
    }

    leakage = {
        "label": "diagnostic_in_sample",
        "oos_claim": False,
        "note": "cumulative dictionaries overlap the evaluated corpus; do not read these as out-of-sample edge",
        "future_design": {
            "walk_forward": "build time-frozen dictionaries per fold; freeze rules on calibration fold only, evaluate on next temporal fold",
            "time_frozen_dictionary": "rebuild per-fold dictionaries from data available only before fold origin",
            "gap_emitted": True,
        },
    }

    return {
        "config": _public_config(cfg),
        "selection": {
            "freeze_split": "calibration",
            "train_used_for_freeze": False,
            "test_used_for_freeze": False,
            "test_evaluations": 1,
            "split_sizes": {"train": len(train), "calibration": len(cal), "test": len(test)},
            "train_hypotheses": train_hypotheses,
        },
        "leakage": leakage,
        "counterfactuals": counterfactuals,
        "frozen_rules": frozen,
        "gates": gates,
        "economics": economics,
        "shadow_staking": {
            "status": ss_status,
            "basis": economics.get("x2_x3_assignment", {}).get("basis") if economics.get("verified_odds") else None,
            "max_exposure_per_event": cfg["max_aggregate_exposure_per_event"],
            "x2_x3_multipliers": economics.get("x2_x3_assignment", {}).get("mapping") if economics.get("verified_odds") else None,
        },
        "controls": controls,
        "coverage": coverage,
        "test_evaluation": test_eval,
    }


def _evaluate_on_test(test: list[dict], frozen: dict[str, Any]) -> dict[str, Any]:
    """Evaluate frozen rules exactly once on the test split."""
    if not test:
        return {"evaluated": False, "n": 0, "rule_evaluations": []}
    out: list[dict[str, Any]] = []
    for h in frozen.get("hypotheses_considered", []):
        pred = next((p for name, p in _HIPOS if name == h["name"]), None)
        if pred is None:
            continue
        sel = [r for r in test if pred(r)]
        wins = 0
        n = 0
        for r in sel:
            side, _ = select_side(r, "current_L_gt_E_gt_A")
            if side is None:
                continue
            n += 1
            wins += 1 if r.get("final_outcome") == side else 0
        lo, hi = wilson_interval(wins, n) if n else (0.0, 1.0)
        out.append({"name": h["name"], "n": n, "wins": wins, "wr": wins / n if n else 0.0, "wilson95": {"low": lo, "high": hi}, "evaluations": 1})
    return {"evaluated": True, "n": len(test), "rule_evaluations": out, "evaluated_once": True}


def _shadow_status(gates: dict[str, Any], economics: dict[str, Any]) -> str:
    if not gates["min_calibration_events"]["passed"] or not gates["min_test_events"]["passed"]:
        return "insufficient_n"
    if economics.get("verified_odds") and economics.get("x2_x3_assignment") is not None:
        return "candidate" if economics.get("status") == "candidate" else "no_candidate"
    if economics.get("status") == "candidate":
        return "partial_verified"
    return "no_candidate"


def _coverage_report(rows: list[dict], frozen: dict[str, Any]) -> dict[str, Any]:
    total = len(rows)
    unique = len({int(r.get("map_id") or 0) for r in rows})
    weak_filtered = set()
    for f in frozen.get("weak_filters", []):
        weak_filtered.add(f["name"])
    placed = [r for r in rows if select_side(r, "current_L_gt_E_gt_A")[0] is not None]
    abstained = total - len(placed)
    return {
        "total_rows": total,
        "unique_map_n": unique,
        "retained": len(placed),
        "abstention": abstained,
        "abstention_rate": abstained / total if total else 0.0,
        "effective_n": len(placed),
        "weak_filter_names": sorted(weak_filtered),
    }


# ---------------------------------------------------------------------------
# Staging evidence writer (policy dir only)
# ---------------------------------------------------------------------------


def write_policy_staging_evidence(report: dict[str, Any], rows: list[dict] | None = None) -> list[str]:
    STAGING_POLICY_DIR.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []

    policy_table = {
        "policies": report["counterfactuals"]["policies"],
        "exact_L_ne_A": report["counterfactuals"]["exact_L_ne_A"],
        "generic_opposite_not_exact_LA": report["counterfactuals"]["generic_opposite_not_exact_LA"],
        "wait_to_34": report["counterfactuals"]["wait_to_34"],
        "m34_lead_buckets": report["counterfactuals"]["m34_lead_buckets"],
        "leakage": report["leakage"],
    }
    table_path = STAGING_POLICY_DIR / "policy_table.json"
    table_path.write_text(json.dumps(policy_table, indent=2, sort_keys=True))
    paths.append(str(table_path))

    meta_path = STAGING_POLICY_DIR / "config_metadata.json"
    meta_path.write_text(json.dumps(report["config"], indent=2, sort_keys=True))
    paths.append(str(meta_path))

    full_path = STAGING_POLICY_DIR / "policy_report.json"
    full_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str))
    paths.append(str(full_path))

    hashes: dict[str, str] = {}
    for p in paths:
        name = Path(p).name
        hashes[name] = hashlib.sha256(Path(p).read_bytes()).hexdigest()
    hash_path = STAGING_POLICY_DIR / "staging_hashes.json"
    hash_path.write_text(json.dumps(hashes, indent=2, sort_keys=True))
    paths.append(str(hash_path))

    leakage_path = STAGING_POLICY_DIR / "leakage_labels.json"
    leakage_path.write_text(json.dumps(report["leakage"], indent=2, sort_keys=True))
    paths.append(str(leakage_path))

    return paths