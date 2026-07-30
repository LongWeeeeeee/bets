#!/usr/bin/env python3
"""Chronological ELO model lab. Throwaway research: does not modify production config/state."""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ELO.config import HybridEloConfig, SimpleTeamEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier, SeriesBundle
from ELO.models import HybridPlayerRosterEloModel, SimpleTeamEloModel
from ELO.roster import RosterLineageTracker
from ELO.series_data import build_series_bundles
from ELO.series_evaluation import probability_to_win_series
from ELO.team_identity import resolve_org_key
from ELO.tiering import attach_league_tiers, classify_leagues

EPS = 1e-12
SECONDS_DAY = 86400.0
ELO_SCALE = 400.0


def clip(p: float) -> float:
    return min(max(float(p), EPS), 1.0 - EPS)


def logit(p: float) -> float:
    p = clip(p)
    return math.log(p / (1.0 - p))


def logistic(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def elo_to_logit(points: float) -> float:
    return float(points) * math.log(10.0) / ELO_SCALE


def iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def make_hybrid(config: HybridEloConfig, min_shared_players: int = 3) -> HybridPlayerRosterEloModel:
    model = HybridPlayerRosterEloModel(config)
    model.roster_tracker = RosterLineageTracker(min_shared_players=min_shared_players)
    return model


class ExactLineupResidualModel(HybridPlayerRosterEloModel):
    """Experimental exact-lineup residual blended over the current player model."""

    def __init__(self, config: HybridEloConfig, *, max_weight: float, full_weight_matches: int = 8):
        super().__init__(config)
        self.max_weight = float(max_weight)
        self.full_weight_matches = int(full_weight_matches)
        self.exact_ratings: dict[LeagueTier, dict[str, float]] = {tier: {} for tier in LeagueTier}
        self.exact_counts: dict[LeagueTier, defaultdict[str, int]] = {
            tier: defaultdict(int) for tier in LeagueTier
        }

    def _build_team_context(
        self,
        team_id,
        team_name,
        player_ids,
        player_positions,
        tier,
        timestamp,
        *,
        mutate,
    ):
        context = super()._build_team_context(
            team_id=team_id,
            team_name=team_name,
            player_ids=player_ids,
            player_positions=player_positions,
            tier=tier,
            timestamp=timestamp,
            mutate=mutate,
        )
        key = self._lineup_key(resolve_org_key(team_id, team_name), player_ids)
        count = self.exact_counts[tier][key]
        rating = self.exact_ratings[tier].get(key, context.prior_blended_strength)
        confidence = min(1.0, count / max(1, self.full_weight_matches))
        weight = self.max_weight * confidence
        context.team_strength = (1.0 - weight) * context.team_strength + weight * rating
        return context

    def process_match(self, match):
        tier = match.derived_league_tier
        rad_key = self._lineup_key(resolve_org_key(match.radiant_team_id, match.radiant_team_name), match.radiant_player_ids)
        dire_key = self._lineup_key(resolve_org_key(match.dire_team_id, match.dire_team_name), match.dire_player_ids)
        rad_base = self.exact_ratings[tier].get(rad_key)
        dire_base = self.exact_ratings[tier].get(dire_key)
        step = super().process_match(match)
        error = (1.0 if match.radiant_win else 0.0) - step.p_radiant
        if rad_base is None:
            rad_base = float(step.metadata.get("radiant_prior_blended_strength", 1500.0))
        if dire_base is None:
            dire_base = float(step.metadata.get("dire_prior_blended_strength", 1500.0))
        k = float(self.config.k_roster_by_tier[tier])
        self.exact_ratings[tier][rad_key] = rad_base + k * error
        self.exact_ratings[tier][dire_key] = dire_base - k * error
        self.exact_counts[tier][rad_key] += 1
        self.exact_counts[tier][dire_key] += 1
        return step


def collect_rows(model: Any, bundles: list[SeriesBundle], label: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    eligible_idx = 0
    for bundle in bundles:
        series = bundle.series
        first_map = bundle.deciding_maps[0] if bundle.deciding_maps else None
        pre_map_prob: float | None = None
        pre_series_prob: float | None = None
        pre_step = None
        if series.eligible_for_winner_target and first_map is not None:
            pre_step = model.predict_match(first_map)
            pre_map_prob = clip(pre_step.p_radiant)
            pre_series_prob = clip(probability_to_win_series(pre_map_prob, series.best_of))
            rows.append(
                {
                    "idx": eligible_idx,
                    "series_id": int(series.series_id),
                    "timestamp": int(series.start_timestamp),
                    "week": int(series.start_timestamp // (7 * 86400)),
                    "league_tier": series.derived_league_tier.value,
                    "best_of": int(series.best_of),
                    "team_a_name": series.team_a_name,
                    "team_b_name": series.team_b_name,
                    "team_a_org": resolve_org_key(series.team_a_id, series.team_a_name),
                    "team_b_org": resolve_org_key(series.team_b_id, series.team_b_name),
                    "team_a_players": list(series.team_a_player_ids),
                    "team_b_players": list(series.team_b_player_ids),
                    "p_map": pre_map_prob,
                    "p": pre_series_prob,
                    "actual": 1.0 if series.team_a_won else 0.0,
                    "metadata": dict(pre_step.metadata or {}),
                }
            )
            eligible_idx += 1

        for match in bundle.all_maps:
            model.process_match(match)

        apply_bonus = getattr(model, "apply_bo3_sweep_bonus", None)
        if (
            callable(apply_bonus)
            and series.eligible_for_winner_target
            and first_map is not None
            and pre_map_prob is not None
            and pre_series_prob is not None
            and series.best_of == 3
            and ((series.team_a_map_wins == 2 and series.team_b_map_wins == 0) or
                 (series.team_a_map_wins == 0 and series.team_b_map_wins == 2))
        ):
            apply_bonus(
                first_map=first_map,
                actual=1.0 if series.team_a_won else 0.0,
                pre_map_prob=pre_map_prob,
                pre_series_prob=pre_series_prob,
            )
    print(f"{label}: rows={len(rows)}", flush=True)
    return rows


def aligned(*variants: list[dict[str, Any]]) -> None:
    if not variants:
        return
    base = [(r["idx"], r["series_id"], r["timestamp"]) for r in variants[0]]
    for rows in variants[1:]:
        check = [(r["idx"], r["series_id"], r["timestamp"]) for r in rows]
        if check != base:
            raise RuntimeError("Variant prediction rows are not aligned")


def transform_temperature(rows: list[dict[str, Any]], temperature: float) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        p_map = logistic(logit(row["p_map"]) / float(temperature))
        nr = dict(row)
        nr["p_map"] = p_map
        nr["p"] = clip(probability_to_win_series(p_map, row["best_of"]))
        out.append(nr)
    return out


def blend_map_logits(
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    right_weight: float,
) -> list[dict[str, Any]]:
    aligned(left_rows, right_rows)
    out = []
    for left, right in zip(left_rows, right_rows):
        mixed_logit = (1.0 - right_weight) * logit(left["p_map"]) + right_weight * logit(right["p_map"])
        p_map = logistic(mixed_logit)
        nr = dict(left)
        nr["p_map"] = p_map
        nr["p"] = clip(probability_to_win_series(p_map, nr["best_of"]))
        out.append(nr)
    return out


def _decay_value(value: float, elapsed: int, half_life_days: float) -> float:
    if not value or elapsed <= 0 or half_life_days <= 0:
        return value
    return value * math.pow(0.5, elapsed / (half_life_days * SECONDS_DAY))


def dynamic_adjust(
    rows: list[dict[str, Any]],
    *,
    form_k: float = 0.0,
    form_half_life_days: float = 30.0,
    pair_k: float = 0.0,
    pair_prior: float = 8.0,
    pair_half_life_days: float = 180.0,
    pair_identity: str = "org",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    form_state: dict[str, tuple[float, int]] = {}
    pair_state: dict[tuple[str, str], tuple[float, int, int]] = {}
    out: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []

    def get_form(key: str, ts: int) -> float:
        value, last_ts = form_state.get(key, (0.0, ts))
        return _decay_value(value, ts - last_ts, form_half_life_days)

    for row in rows:
        ts = int(row["timestamp"])
        org_a = str(row["team_a_org"])
        org_b = str(row["team_b_org"])
        form_a = get_form(org_a, ts) if form_k > 0 else 0.0
        form_b = get_form(org_b, ts) if form_k > 0 else 0.0
        form_adjust = form_a - form_b

        if pair_identity == "roster":
            meta = row.get("metadata") or {}
            key_a = str(meta.get("radiant_roster_key") or org_a)
            key_b = str(meta.get("dire_roster_key") or org_b)
        else:
            key_a, key_b = org_a, org_b
        if key_a <= key_b:
            pair_key = (key_a, key_b)
            sign = 1.0
        else:
            pair_key = (key_b, key_a)
            sign = -1.0
        pair_rating, pair_n, pair_last_ts = pair_state.get(pair_key, (0.0, 0, ts))
        pair_rating = _decay_value(pair_rating, ts - pair_last_ts, pair_half_life_days)
        shrink = pair_n / (pair_n + pair_prior) if pair_prior > 0 else 1.0
        pair_adjust = sign * pair_rating * shrink if pair_k > 0 else 0.0

        adjusted_map_logit = logit(row["p_map"]) + elo_to_logit(form_adjust + pair_adjust)
        p_map = logistic(adjusted_map_logit)
        p_series = clip(probability_to_win_series(p_map, row["best_of"]))
        actual = float(row["actual"])
        error = actual - p_series

        nr = dict(row)
        nr["p_map"] = p_map
        nr["p"] = p_series
        out.append(nr)
        diagnostics.append(
            {
                "idx": row["idx"],
                "timestamp": ts,
                "pair": pair_key,
                "team_a_name": row["team_a_name"],
                "team_b_name": row["team_b_name"],
                "pair_n_pre": pair_n,
                "pair_adjust_elo": pair_adjust,
                "form_adjust_elo": form_adjust,
                "base_p": row["p"],
                "adjusted_p": p_series,
                "actual": actual,
            }
        )

        if form_k > 0:
            form_state[org_a] = (form_a + form_k * error, ts)
            form_state[org_b] = (form_b - form_k * error, ts)
        if pair_k > 0:
            pair_state[pair_key] = (pair_rating + sign * pair_k * error, pair_n + 1, ts)
    return out, diagnostics


def metric(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"n": 0, "accuracy": None, "log_loss": None, "brier": None, "ece": None}
    p = np.asarray([clip(r["p"]) for r in rows], dtype=float)
    y = np.asarray([float(r["actual"]) for r in rows], dtype=float)
    ll = float(np.mean(-(y * np.log(p) + (1.0 - y) * np.log(1.0 - p))))
    brier = float(np.mean((p - y) ** 2))
    acc = float(np.mean((p >= 0.5) == (y >= 0.5)))
    ece = 0.0
    for lo in np.linspace(0.0, 0.9, 10):
        mask = (p >= lo) & (p < lo + 0.1 if lo < 0.9 else p <= 1.0)
        if mask.any():
            ece += float(mask.mean()) * abs(float(p[mask].mean()) - float(y[mask].mean()))
    return {
        "n": len(rows),
        "accuracy": acc,
        "log_loss": ll,
        "brier": brier,
        "ece": ece,
        "avg_p": float(p.mean()),
        "actual_rate": float(y.mean()),
    }


def slice_rows(rows: list[dict[str, Any]], start_frac: float, end_frac: float = 1.0) -> list[dict[str, Any]]:
    n = len(rows)
    return rows[int(n * start_frac): int(n * end_frac)]


def by_tier(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {tier.value: metric([r for r in rows if r["league_tier"] == tier.value]) for tier in LeagueTier}


def paired_block_ci(
    baseline_rows: list[dict[str, Any]],
    trial_rows: list[dict[str, Any]],
    seed: int = 20260719,
    draws: int = 1500,
) -> dict[str, Any]:
    aligned(baseline_rows, trial_rows)
    base_loss = np.asarray([
        -(r["actual"] * math.log(clip(r["p"])) + (1.0-r["actual"]) * math.log(clip(1.0-r["p"])))
        for r in baseline_rows
    ])
    trial_loss = np.asarray([
        -(r["actual"] * math.log(clip(r["p"])) + (1.0-r["actual"]) * math.log(clip(1.0-r["p"])))
        for r in trial_rows
    ])
    diff = trial_loss - base_loss
    weeks = np.asarray([r["week"] for r in baseline_rows])
    unique_weeks = np.unique(weeks)
    by_week = [diff[weeks == week] for week in unique_weeks]
    rng = np.random.default_rng(seed)
    boot = np.empty(draws, dtype=float)
    for idx in range(draws):
        chosen = rng.integers(0, len(by_week), size=len(by_week))
        values = np.concatenate([by_week[i] for i in chosen])
        boot[idx] = float(values.mean())
    return {
        "delta_log_loss_trial_minus_baseline": float(diff.mean()),
        "block_bootstrap_ci95": [float(np.quantile(boot, 0.025)), float(np.quantile(boot, 0.975))],
        "probability_trial_better": float(np.mean(boot < 0.0)),
        "weeks": int(len(unique_weeks)),
    }


def top_pair_diagnostics(
    diagnostics: list[dict[str, Any]],
    test_start_idx: int,
    limit: int = 15,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in diagnostics:
        if int(row["idx"]) >= test_start_idx and int(row["pair_n_pre"]) >= 3:
            groups[tuple(row["pair"])].append(row)
    out = []
    for pair, pair_rows in groups.items():
        base_losses = []
        adjusted_losses = []
        for r in pair_rows:
            y = float(r["actual"])
            bp = clip(r["base_p"])
            ap = clip(r["adjusted_p"])
            base_losses.append(-(y*math.log(bp)+(1-y)*math.log(1-bp)))
            adjusted_losses.append(-(y*math.log(ap)+(1-y)*math.log(1-ap)))
        out.append({
            "pair": list(pair),
            "test_series": len(pair_rows),
            "max_prior_series": max(int(r["pair_n_pre"]) for r in pair_rows),
            "avg_abs_pair_adjust_elo": statistics.mean(abs(float(r["pair_adjust_elo"])) for r in pair_rows),
            "delta_log_loss": statistics.mean(adjusted_losses) - statistics.mean(base_losses),
            "example_names": [pair_rows[-1]["team_a_name"], pair_rows[-1]["team_b_name"]],
        })
    out.sort(key=lambda x: (-x["avg_abs_pair_adjust_elo"], -x["test_series"]))
    return out[:limit]


def roster_diagnostics(rows: list[dict[str, Any]], start_idx: int) -> dict[str, Any]:
    overlap_counts: dict[str, int] = defaultdict(int)
    sides = 0
    role_complete = 0
    lineup_new_sides = 0
    for row in rows:
        if int(row["idx"]) < start_idx:
            continue
        meta = row.get("metadata") or {}
        for prefix in ("radiant", "dire"):
            overlap = meta.get(f"{prefix}_overlap")
            if overlap is not None:
                overlap_counts[str(int(overlap))] += 1
                sides += 1
            if int(meta.get(f"{prefix}_lineup_matches") or 0) == 0:
                lineup_new_sides += 1
        # Role coverage is derived from the actual first-series lineups.
        # The exact positions are not kept in the row metadata, so infer from nonzero role effect metadata availability elsewhere is unsafe.
    return {
        "evaluated_sides": sides,
        "overlap_counts": dict(sorted(overlap_counts.items(), key=lambda kv: int(kv[0]))),
        "new_exact_lineup_sides": lineup_new_sides,
        "new_exact_lineup_share": lineup_new_sides / sides if sides else None,
    }


def json_ready(value: Any) -> Any:
    if isinstance(value, LeagueTier):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(v) for v in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    matches, load_summary = load_matches(args.data_dir)
    league_info, league_summary = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    bundles, series_summary = build_series_bundles(matches)
    current_cfg = HybridEloConfig()

    model_rows: dict[str, list[dict[str, Any]]] = {}
    diagnostics: dict[str, Any] = {}

    # Team-only Elo: K and inactivity decay sweep.
    team_trials = [
        ("team_k20", 20.0, 0.0),
        ("team_k28", 28.0, 0.0),
        ("team_k36", 36.0, 0.0),
        ("team_k28_decay90", 28.0, 90.0),
        ("team_k28_decay180", 28.0, 180.0),
        ("team_k36_decay180", 36.0, 180.0),
    ]
    for name, k, decay in team_trials:
        model_rows[name] = collect_rows(
            SimpleTeamEloModel(SimpleTeamEloConfig(base_k=k, team_decay_half_life_days=decay)), bundles, name
        )

    # Isolated player Elo variants plus production ablations.
    pure_base = replace(
        current_cfg,
        bo3_sweep_bonus_weight=0.0,
        player_role_weight=0.0,
        max_roster_weight=0.0,
        lineup_uncertainty_boost_max=0.0,
        player_org_uncertainty_boost_max=0.0,
        patch_local_reset_mode="none",
    )
    hybrid_trials: list[tuple[str, HybridEloConfig, int]] = [
        ("player_global_only", replace(pure_base, player_global_weight=1.0, player_tier_weight=0.0), 3),
        ("player_tier_only", replace(pure_base, player_global_weight=0.0, player_tier_weight=1.0), 3),
        ("player_global_local", replace(pure_base, player_global_weight=0.68, player_tier_weight=0.32), 3),
        ("current_prod", current_cfg, 3),
        ("current_no_role", replace(current_cfg, player_role_weight=0.0), 3),
        ("current_no_patch_reset", replace(current_cfg, patch_local_reset_mode="none"), 3),
        ("current_no_lineup_uncertainty", replace(current_cfg, lineup_uncertainty_boost_max=0.0), 3),
        ("current_no_player_org_uncertainty", replace(current_cfg, player_org_uncertainty_boost_max=0.0), 3),
        ("current_player_decay", replace(current_cfg, player_global_decay_half_life_days=365.0, player_local_decay_half_life_days=120.0), 3),
    ]
    for threshold in (3, 4, 5):
        for weight in (0.10, 0.20, 0.30):
            hybrid_trials.append((
                f"roster_t{threshold}_w{int(weight*100):02d}",
                replace(current_cfg, max_roster_weight=weight),
                threshold,
            ))
    for name, cfg, threshold in hybrid_trials:
        model_rows[name] = collect_rows(make_hybrid(cfg, threshold), bundles, name)

    baseline_rows = model_rows["current_prod"]
    eligible_n = len(baseline_rows)
    val_start = int(eligible_n * 0.70)
    test_start = int(eligible_n * 0.85)

    # Dynamic team/player blends in map-logit space.
    for weight in (0.25, 0.50, 0.75):
        name = f"blend_team{int((1-weight)*100):02d}_player{int(weight*100):02d}"
        model_rows[name] = blend_map_logits(model_rows["team_k28"], baseline_rows, weight)

    # Probability calibration. Temperature is selected only by validation later.
    for temperature in (1.05, 1.10, 1.15, 1.20, 1.30, 1.40):
        model_rows[f"temperature_{temperature:.2f}"] = transform_temperature(baseline_rows, temperature)

    # Opponent-adjusted rolling form and kryptonite residuals.
    dynamic_specs = [
        ("form_k16_hl30", dict(form_k=16.0, form_half_life_days=30.0)),
        ("form_k32_hl30", dict(form_k=32.0, form_half_life_days=30.0)),
        ("form_k32_hl60", dict(form_k=32.0, form_half_life_days=60.0)),
        ("form_k64_hl30", dict(form_k=64.0, form_half_life_days=30.0)),
        ("h2h_org_k32_p4", dict(pair_k=32.0, pair_prior=4.0, pair_half_life_days=180.0, pair_identity="org")),
        ("h2h_org_k64_p4", dict(pair_k=64.0, pair_prior=4.0, pair_half_life_days=180.0, pair_identity="org")),
        ("h2h_org_k64_p8", dict(pair_k=64.0, pair_prior=8.0, pair_half_life_days=180.0, pair_identity="org")),
        ("h2h_org_k64_p8_hl365", dict(pair_k=64.0, pair_prior=8.0, pair_half_life_days=365.0, pair_identity="org")),
        ("h2h_roster_k64_p4", dict(pair_k=64.0, pair_prior=4.0, pair_half_life_days=180.0, pair_identity="roster")),
        ("form32_h2h64", dict(form_k=32.0, form_half_life_days=30.0, pair_k=64.0, pair_prior=8.0, pair_half_life_days=180.0, pair_identity="org")),
    ]
    for name, kwargs in dynamic_specs:
        rows, diag = dynamic_adjust(baseline_rows, **kwargs)
        model_rows[name] = rows
        diagnostics[name] = diag
        print(f"{name}: rows={len(rows)}", flush=True)

    # Ensure everything compares exactly the same chronological series.
    aligned(*model_rows.values())

    windows = {
        "holdout30": (0.70, 1.0),
        "validation15": (0.70, 0.85),
        "test15": (0.85, 1.0),
        "recent10": (0.90, 1.0),
    }
    trial_reports: dict[str, Any] = {}
    baseline_test = slice_rows(baseline_rows, 0.85, 1.0)
    for name, rows in model_rows.items():
        report: dict[str, Any] = {}
        for window, (start, end) in windows.items():
            part = slice_rows(rows, start, end)
            report[window] = metric(part)
            report[f"{window}_by_tier"] = by_tier(part)
        report["test_vs_current"] = paired_block_ci(baseline_test, slice_rows(rows, 0.85, 1.0))
        trial_reports[name] = report

    # Select candidates on validation only; report untouched final test ranking.
    validation_ranking = sorted(
        model_rows,
        key=lambda name: trial_reports[name]["validation15"]["log_loss"],
    )
    test_ranking = sorted(
        model_rows,
        key=lambda name: trial_reports[name]["test15"]["log_loss"],
    )
    selected_on_validation = validation_ranking[0]

    top_pair = {}
    for name in [spec[0] for spec in dynamic_specs if "h2h" in spec[0]]:
        top_pair[name] = top_pair_diagnostics(diagnostics[name], test_start)

    # H2H prevalence: how often test rows have enough previous pair history for a meaningful effect.
    h2h_diag = diagnostics["h2h_org_k64_p8"]
    test_h2h = [d for d in h2h_diag if int(d["idx"]) >= test_start]
    h2h_support = {
        f"prior_at_least_{n}": sum(int(d["pair_n_pre"]) >= n for d in test_h2h) / len(test_h2h)
        for n in (1, 2, 3, 5, 8, 10)
    }

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "throwaway chronological ELO model comparison; production config was not modified",
        "data_dir": str(args.data_dir),
        "dataset": {
            **load_summary,
            "first_match_utc": iso(matches[0].timestamp),
            "last_match_utc": iso(matches[-1].timestamp),
            "series_summary": series_summary,
            "league_summary": league_summary,
            "eligible_series": eligible_n,
        },
        "split": {
            "validation": {"start_idx": val_start, "end_idx": test_start, "n": test_start-val_start,
                           "first_utc": iso(baseline_rows[val_start]["timestamp"]), "last_utc": iso(baseline_rows[test_start-1]["timestamp"])},
            "test": {"start_idx": test_start, "end_idx": eligible_n, "n": eligible_n-test_start,
                     "first_utc": iso(baseline_rows[test_start]["timestamp"]), "last_utc": iso(baseline_rows[-1]["timestamp"])},
        },
        "current_config": json_ready(asdict(current_cfg)),
        "trials": trial_reports,
        "validation_ranking": validation_ranking,
        "test_ranking": test_ranking,
        "selected_on_validation": selected_on_validation,
        "selected_test_result": trial_reports[selected_on_validation]["test15"],
        "roster_diagnostics_test": roster_diagnostics(baseline_rows, test_start),
        "h2h_test_support": h2h_support,
        "top_pair_diagnostics": top_pair,
    }

    json_path = args.output_dir / "report.json"
    json_path.write_text(json.dumps(json_ready(report), ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# ELO model lab — chronological experiments",
        "",
        f"Data: {load_summary['loaded_matches']} maps, {eligible_n} eligible series, {iso(matches[0].timestamp)} → {iso(matches[-1].timestamp)}",
        f"Validation: {test_start-val_start} series; final test: {eligible_n-test_start} series.",
        "",
        "## Top by validation log loss",
        "",
        "| # | Variant | Val LL | Test LL | Test acc | ΔLL vs current | 95% week-block CI |",
        "|---:|---|---:|---:|---:|---:|---|",
    ]
    for rank, name in enumerate(validation_ranking[:20], 1):
        val = trial_reports[name]["validation15"]
        test = trial_reports[name]["test15"]
        comp = trial_reports[name]["test_vs_current"]
        ci = comp["block_bootstrap_ci95"]
        lines.append(
            f"| {rank} | {name} | {val['log_loss']:.6f} | {test['log_loss']:.6f} | {test['accuracy']:.4f} | "
            f"{comp['delta_log_loss_trial_minus_baseline']:+.6f} | [{ci[0]:+.6f}, {ci[1]:+.6f}] |"
        )
    lines += [
        "",
        "## Top by untouched final test (diagnostic only; do not select from this table)",
        "",
        "| # | Variant | Test LL | Brier | Acc | ECE |",
        "|---:|---|---:|---:|---:|---:|",
    ]
    for rank, name in enumerate(test_ranking[:20], 1):
        test = trial_reports[name]["test15"]
        lines.append(f"| {rank} | {name} | {test['log_loss']:.6f} | {test['brier']:.6f} | {test['accuracy']:.4f} | {test['ece']:.4f} |")
    lines += [
        "",
        f"Selected strictly on validation: **{selected_on_validation}**.",
        "",
        "## H2H support in final test",
        "",
        json.dumps(h2h_support, ensure_ascii=False),
        "",
        "## Roster diagnostics in final test",
        "",
        json.dumps(report["roster_diagnostics_test"], ensure_ascii=False),
        "",
    ]
    md_path = args.output_dir / "report.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"saved {json_path}", flush=True)
    print(f"saved {md_path}", flush=True)
    print(f"selected_on_validation={selected_on_validation}", flush=True)


if __name__ == "__main__":
    main()
