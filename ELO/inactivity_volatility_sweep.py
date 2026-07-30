from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.config import EvaluationConfig, HybridEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier, MatchRecord
from ELO.models import HybridPlayerRosterEloModel, _team_key
from ELO.series_data import build_series_bundles
from ELO.series_evaluation import run_series_online_evaluation
from ELO.tiering import attach_league_tiers, classify_leagues

SECONDS_PER_DAY = 24 * 60 * 60


def _to_json_ready(value: Any) -> Any:
    if isinstance(value, LeagueTier):
        return value.value
    if is_dataclass(value):
        return _to_json_ready(asdict(value))
    if isinstance(value, dict):
        return {str(key.value if isinstance(key, LeagueTier) else key): _to_json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_ready(item) for item in value]
    return value


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "pro_heroes_data" / "json_parts_split_prod"


def _default_output_path() -> Path:
    return Path(__file__).resolve().parent / "output" / "inactivity_volatility_sweep_prod.json"


@dataclass(frozen=True)
class TrialCfg:
    name: str
    boost_max: float
    stabilize_matches: int
    local: bool
    roster: bool
    tier1_only: bool = True


class InactivityVolatilityModel(HybridPlayerRosterEloModel):
    def __init__(
        self,
        config: HybridEloConfig,
        *,
        boost_max: float,
        stabilize_matches: int,
        apply_local: bool,
        apply_roster: bool,
        tier1_only: bool,
    ) -> None:
        super().__init__(config)
        self.boost_max = max(0.0, float(boost_max))
        self.stabilize_matches = max(0, int(stabilize_matches))
        self.apply_local = bool(apply_local)
        self.apply_roster = bool(apply_roster)
        self.tier1_only = bool(tier1_only)
        self.player_return_counts: defaultdict[tuple[LeagueTier, int], int] = defaultdict(int)
        self.active_player_returns: set[tuple[LeagueTier, int]] = set()
        self.roster_return_counts: defaultdict[tuple[LeagueTier, str], int] = defaultdict(int)
        self.active_roster_returns: set[tuple[LeagueTier, str]] = set()

    def _enabled_for_tier(self, tier: LeagueTier) -> bool:
        return not self.tier1_only or tier == LeagueTier.TIER1

    def _gap_seconds(self) -> int:
        gap_days = int(getattr(self.config, "inactivity_penalty_gap_days", 0) or 0)
        return gap_days * SECONDS_PER_DAY

    def _maybe_start_player_return(
        self,
        *,
        player_id: int,
        tier: LeagueTier,
        previous_last_seen_ts: int | None,
        timestamp: int,
    ) -> None:
        if not self.apply_local or not self._enabled_for_tier(tier):
            return
        gap_seconds = self._gap_seconds()
        if gap_seconds <= 0 or previous_last_seen_ts is None:
            return
        if timestamp - previous_last_seen_ts >= gap_seconds:
            key = (tier, player_id)
            self.player_return_counts[key] = 0
            self.active_player_returns.add(key)

    def _maybe_start_roster_return(
        self,
        *,
        roster_key: str,
        tier: LeagueTier,
        previous_last_seen_ts: int | None,
        timestamp: int,
    ) -> None:
        if not self.apply_roster or not self._enabled_for_tier(tier):
            return
        gap_seconds = self._gap_seconds()
        if gap_seconds <= 0 or previous_last_seen_ts is None:
            return
        if timestamp - previous_last_seen_ts >= gap_seconds:
            key = (tier, roster_key)
            self.roster_return_counts[key] = 0
            self.active_roster_returns.add(key)

    def _boost_multiplier(self, current_count: int) -> float:
        if self.boost_max <= 0.0 or self.stabilize_matches <= 0:
            return 1.0
        freshness = max(0.0, 1.0 - (float(current_count) / float(max(1, self.stabilize_matches))))
        return 1.0 + self.boost_max * freshness

    def _player_return_multiplier(self, *, player_id: int, tier: LeagueTier) -> float:
        key = (tier, player_id)
        if key not in self.active_player_returns:
            return 1.0
        return self._boost_multiplier(self.player_return_counts[key])

    def _roster_return_multiplier(self, *, roster_key: str, tier: LeagueTier) -> float:
        key = (tier, roster_key)
        if key not in self.active_roster_returns:
            return 1.0
        return self._boost_multiplier(self.roster_return_counts[key])

    def _commit_player_return(self, *, player_id: int, tier: LeagueTier) -> None:
        key = (tier, player_id)
        if key not in self.active_player_returns:
            return
        self.player_return_counts[key] += 1
        if self.player_return_counts[key] >= self.stabilize_matches:
            self.active_player_returns.discard(key)
            self.player_return_counts.pop(key, None)

    def _commit_roster_return(self, *, roster_key: str, tier: LeagueTier) -> None:
        key = (tier, roster_key)
        if key not in self.active_roster_returns:
            return
        self.roster_return_counts[key] += 1
        if self.roster_return_counts[key] >= self.stabilize_matches:
            self.active_roster_returns.discard(key)
            self.roster_return_counts.pop(key, None)

    def process_match(self, match: MatchRecord):
        self._maybe_apply_patch_local_reset(match.timestamp)

        preview_step, radiant_context, dire_context, tier, side_bias = self._preview_match(match, mutate=False)

        pre_global_last_seen = {
            player_id: self.player_global_last_seen_ts.get(player_id)
            for player_id in (*match.radiant_player_ids, *match.dire_player_ids)
        }
        pre_local_last_seen = {
            player_id: self.player_local_last_seen_ts[tier].get(player_id)
            for player_id in (*match.radiant_player_ids, *match.dire_player_ids)
        }
        pre_roster_last_seen = {
            radiant_context.roster_key: self.roster_last_seen_ts[tier].get(radiant_context.roster_key),
            dire_context.roster_key: self.roster_last_seen_ts[tier].get(dire_context.roster_key),
        }

        step, radiant_context, dire_context, tier, side_bias = self._preview_match(match, mutate=True)
        actual = 1.0 if match.radiant_win else 0.0
        error = actual - preview_step.p_radiant

        for player_id in match.radiant_player_ids:
            self._maybe_start_player_return(
                player_id=player_id,
                tier=tier,
                previous_last_seen_ts=pre_local_last_seen.get(player_id),
                timestamp=match.timestamp,
            )
        for player_id in match.dire_player_ids:
            self._maybe_start_player_return(
                player_id=player_id,
                tier=tier,
                previous_last_seen_ts=pre_local_last_seen.get(player_id),
                timestamp=match.timestamp,
            )
        self._maybe_start_roster_return(
            roster_key=radiant_context.roster_key,
            tier=tier,
            previous_last_seen_ts=pre_roster_last_seen.get(radiant_context.roster_key),
            timestamp=match.timestamp,
        )
        self._maybe_start_roster_return(
            roster_key=dire_context.roster_key,
            tier=tier,
            previous_last_seen_ts=pre_roster_last_seen.get(dire_context.roster_key),
            timestamp=match.timestamp,
        )

        k_global = self.config.k_global_by_tier[tier]
        k_local = self.config.k_local_by_tier[tier]
        k_roster = self.config.k_roster_by_tier[tier]
        radiant_positions = getattr(match, "radiant_player_positions", ())
        dire_positions = getattr(match, "dire_player_positions", ())
        rad_role_share = self._role_local_k_share(tier, radiant_positions)
        dire_role_share = self._role_local_k_share(tier, dire_positions)
        rad_role_local_k = k_local * rad_role_share
        dire_role_local_k = k_local * dire_role_share
        rad_tier_local_k = k_local * (1.0 - rad_role_share)
        dire_tier_local_k = k_local * (1.0 - dire_role_share)
        rad_mult = radiant_context.lineup_k_multiplier
        dire_mult = dire_context.lineup_k_multiplier
        rad_org = _team_key(match.radiant_team_id, match.radiant_team_name)
        dire_org = _team_key(match.dire_team_id, match.dire_team_name)

        for player_id, position in zip(match.radiant_player_ids, radiant_positions):
            player_mult = self._player_org_k_multiplier(player_id, rad_org, tier)
            global_mult = rad_mult if self.config.lineup_uncertainty_boost_global else 1.0
            local_mult = rad_mult if self.config.lineup_uncertainty_boost_local else 1.0
            if self.config.player_org_uncertainty_boost_global:
                global_mult *= player_mult
            if self.config.player_org_uncertainty_boost_local:
                local_mult *= player_mult
            local_mult *= self._player_return_multiplier(player_id=player_id, tier=tier)
            self.player_global[player_id] += k_global * global_mult * error
            self.player_local[tier][player_id] += rad_tier_local_k * local_mult * error
            if position and rad_role_local_k > 0.0:
                role_key = (player_id, position)
                self.player_role_local[tier][role_key] = self.player_role_local[tier].get(
                    role_key,
                    self.config.initial_rating,
                ) + (rad_role_local_k * local_mult * error)
                self.player_role_local_last_seen_ts[tier][role_key] = match.timestamp
            self.player_global_last_seen_ts[player_id] = match.timestamp
            self.player_local_last_seen_ts[tier][player_id] = match.timestamp
            self._commit_player_org(player_id, rad_org)
            self._commit_player_return(player_id=player_id, tier=tier)

        for player_id, position in zip(match.dire_player_ids, dire_positions):
            player_mult = self._player_org_k_multiplier(player_id, dire_org, tier)
            global_mult = dire_mult if self.config.lineup_uncertainty_boost_global else 1.0
            local_mult = dire_mult if self.config.lineup_uncertainty_boost_local else 1.0
            if self.config.player_org_uncertainty_boost_global:
                global_mult *= player_mult
            if self.config.player_org_uncertainty_boost_local:
                local_mult *= player_mult
            local_mult *= self._player_return_multiplier(player_id=player_id, tier=tier)
            self.player_global[player_id] -= k_global * global_mult * error
            self.player_local[tier][player_id] -= dire_tier_local_k * local_mult * error
            if position and dire_role_local_k > 0.0:
                role_key = (player_id, position)
                self.player_role_local[tier][role_key] = self.player_role_local[tier].get(
                    role_key,
                    self.config.initial_rating,
                ) - (dire_role_local_k * local_mult * error)
                self.player_role_local_last_seen_ts[tier][role_key] = match.timestamp
            self.player_global_last_seen_ts[player_id] = match.timestamp
            self.player_local_last_seen_ts[tier][player_id] = match.timestamp
            self._commit_player_org(player_id, dire_org)
            self._commit_player_return(player_id=player_id, tier=tier)

        rad_roster_mult = (rad_mult if self.config.lineup_uncertainty_boost_roster else 1.0) * self._roster_return_multiplier(
            roster_key=radiant_context.roster_key,
            tier=tier,
        )
        dire_roster_mult = (dire_mult if self.config.lineup_uncertainty_boost_roster else 1.0) * self._roster_return_multiplier(
            roster_key=dire_context.roster_key,
            tier=tier,
        )
        self.roster_ratings[tier][radiant_context.roster_key] = radiant_context.roster_rating + (
            k_roster * rad_roster_mult * error
        )
        self.roster_ratings[tier][dire_context.roster_key] = dire_context.roster_rating - (
            k_roster * dire_roster_mult * error
        )
        self.roster_last_seen_ts[tier][radiant_context.roster_key] = match.timestamp
        self.roster_last_seen_ts[tier][dire_context.roster_key] = match.timestamp
        self._commit_roster_return(roster_key=radiant_context.roster_key, tier=tier)
        self._commit_roster_return(roster_key=dire_context.roster_key, tier=tier)

        self.roster_match_counts[tier][radiant_context.roster_key] = radiant_context.roster_matches + 1
        self.roster_match_counts[tier][dire_context.roster_key] = dire_context.roster_matches + 1
        self.lineup_match_counts[radiant_context.lineup_key] = radiant_context.lineup_matches + 1
        self.lineup_match_counts[dire_context.lineup_key] = dire_context.lineup_matches + 1
        self.side_bias[tier] = side_bias + self.config.side_bias_k * error

        step.metadata["k_global"] = k_global
        step.metadata["k_local"] = k_local
        step.metadata["k_roster"] = k_roster
        step.metadata["radiant_lineup_k_multiplier"] = rad_mult
        step.metadata["dire_lineup_k_multiplier"] = dire_mult
        step.metadata["side_bias"] = self.side_bias[tier]
        return step


def _summarize_trial(trial: TrialCfg, report: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "trial": trial.name,
        "trial_cfg": _to_json_ready(trial),
        "accuracy": float(report["accuracy"]),
        "log_loss": float(report["log_loss"]),
        "brier": float(report["brier"]),
        "by_tier": _to_json_ready(report.get("by_tier", {})),
        "delta_vs_baseline": {
            "accuracy": float(report["accuracy"]) - float(baseline["accuracy"]),
            "log_loss": float(report["log_loss"]) - float(baseline["log_loss"]),
            "brier": float(report["brier"]) - float(baseline["brier"]),
            "tier1_accuracy": float(report["by_tier"]["TIER1"]["accuracy"])
            - float(baseline["by_tier"]["TIER1"]["accuracy"]),
            "tier1_log_loss": float(report["by_tier"]["TIER1"]["log_loss"])
            - float(baseline["by_tier"]["TIER1"]["log_loss"]),
            "tier1_brier": float(report["by_tier"]["TIER1"]["brier"])
            - float(baseline["by_tier"]["TIER1"]["brier"]),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sweep post-inactivity volatility variants for the hybrid ELO model.")
    parser.add_argument("--data-dir", type=Path, default=_default_data_dir())
    parser.add_argument("--output-path", type=Path, default=_default_output_path())
    args = parser.parse_args()

    data_dir = args.data_dir
    output_path = args.output_path

    matches, load_summary = load_matches(data_dir)
    if not matches:
        raise SystemExit("No valid matches were loaded.")

    league_info, league_summary = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)

    eval_cfg = EvaluationConfig()
    base_cfg = HybridEloConfig()
    baseline_report = run_series_online_evaluation(
        model=HybridPlayerRosterEloModel(config=base_cfg),
        series_bundles=series_bundles,
        config=eval_cfg,
    )

    trials = [
        TrialCfg("baseline", 0.0, 0, False, False, True),
        TrialCfg("boost1.0_10_local_roster_t1", 1.0, 10, True, True, True),
        TrialCfg("boost1.0_12_local_roster_t1", 1.0, 12, True, True, True),
        TrialCfg("boost1.0_15_local_roster_t1", 1.0, 15, True, True, True),
        TrialCfg("boost1.5_10_local_roster_t1", 1.5, 10, True, True, True),
        TrialCfg("boost1.5_15_local_roster_t1", 1.5, 15, True, True, True),
        TrialCfg("boost1.0_12_local_t1", 1.0, 12, True, False, True),
        TrialCfg("boost1.0_15_local_t1", 1.0, 15, True, False, True),
    ]

    results: list[dict[str, Any]] = []
    for trial in trials:
        if trial.name == "baseline":
            report = baseline_report
        else:
            report = run_series_online_evaluation(
                model=InactivityVolatilityModel(
                    config=base_cfg,
                    boost_max=trial.boost_max,
                    stabilize_matches=trial.stabilize_matches,
                    apply_local=trial.local,
                    apply_roster=trial.roster,
                    tier1_only=trial.tier1_only,
                ),
                series_bundles=series_bundles,
                config=eval_cfg,
            )
        results.append(_summarize_trial(trial, report, baseline_report))

    best_by_tier1_accuracy = max(
        results,
        key=lambda row: (
            row["by_tier"]["TIER1"]["accuracy"],
            -row["by_tier"]["TIER1"]["log_loss"],
        ),
    )
    best_by_tier1_log_loss = min(
        results,
        key=lambda row: (
            row["by_tier"]["TIER1"]["log_loss"],
            -row["by_tier"]["TIER1"]["accuracy"],
        ),
    )
    best_by_overall_log_loss = min(results, key=lambda row: (row["log_loss"], row["brier"], -row["accuracy"]))

    output = {
        "data_dir": str(data_dir),
        "dataset_summary": load_summary,
        "league_summary": league_summary,
        "series_summary": series_summary,
        "evaluation": _to_json_ready(eval_cfg),
        "baseline": results[0],
        "best_by_tier1_accuracy": best_by_tier1_accuracy,
        "best_by_tier1_log_loss": best_by_tier1_log_loss,
        "best_by_overall_log_loss": best_by_overall_log_loss,
        "results": results,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(_to_json_ready(output), fh, ensure_ascii=False, indent=2)

    print(f"Saved inactivity-volatility sweep to {output_path}")
    print(
        "Best TIER1 accuracy: "
        f"{best_by_tier1_accuracy['trial']} "
        f"acc={best_by_tier1_accuracy['by_tier']['TIER1']['accuracy']:.4f} "
        f"ll={best_by_tier1_accuracy['by_tier']['TIER1']['log_loss']:.4f}"
    )
    print(
        "Best TIER1 log_loss: "
        f"{best_by_tier1_log_loss['trial']} "
        f"acc={best_by_tier1_log_loss['by_tier']['TIER1']['accuracy']:.4f} "
        f"ll={best_by_tier1_log_loss['by_tier']['TIER1']['log_loss']:.4f}"
    )
    print(
        "Best overall log_loss: "
        f"{best_by_overall_log_loss['trial']} "
        f"acc={best_by_overall_log_loss['accuracy']:.4f} "
        f"ll={best_by_overall_log_loss['log_loss']:.4f}"
    )


if __name__ == "__main__":
    main()
