from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.config import EvaluationConfig, HybridEloConfig
from ELO.domain import LeagueTier, MatchRecord, StepResult
from ELO.models import (
    HybridPlayerRosterEloModel,
    _HybridTeamContext,
    _decay_towards_target,
    _elo_probability,
    _mean,
    _team_key,
)
from ELO.series_data import build_series_bundles
from ELO.series_evaluation import run_series_online_evaluation
from ELO.tiering import attach_league_tiers, classify_leagues


@dataclass
class PositionMatchRecord(MatchRecord):
    radiant_player_positions: tuple[str | None, ...] = field(default_factory=tuple)
    dire_player_positions: tuple[str | None, ...] = field(default_factory=tuple)


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
    return Path(__file__).resolve().parents[1] / "pro_heroes_data" / "json_parts_split_from_object"


def _default_output_path() -> Path:
    return Path(__file__).resolve().parent / "output" / "role_local_sweep.json"


def _extract_player_slots(players: list[dict], is_radiant: bool) -> tuple[tuple[int, ...], tuple[str | None, ...]]:
    slots: dict[int, str | None] = {}
    for player in players:
        if bool(player.get("isRadiant")) != is_radiant:
            continue
        steam_account = player.get("steamAccount") or {}
        account_id = steam_account.get("id")
        if not isinstance(account_id, int):
            continue
        position = player.get("position")
        slots[account_id] = str(position) if position is not None else None
    ordered = sorted(slots.items())
    return tuple(account_id for account_id, _ in ordered), tuple(position for _, position in ordered)


def _parse_match(raw_match: dict) -> PositionMatchRecord | None:
    match_id = raw_match.get("id")
    timestamp = raw_match.get("startDateTime")
    if not isinstance(match_id, int) or not isinstance(timestamp, int):
        return None

    players = raw_match.get("players") or []
    if len(players) != 10:
        return None

    radiant_player_ids, radiant_positions = _extract_player_slots(players, is_radiant=True)
    dire_player_ids, dire_positions = _extract_player_slots(players, is_radiant=False)
    if len(radiant_player_ids) != 5 or len(dire_player_ids) != 5:
        return None

    radiant_team = raw_match.get("radiantTeam") or {}
    dire_team = raw_match.get("direTeam") or {}
    radiant_team_id = radiant_team.get("id")
    dire_team_id = dire_team.get("id")
    radiant_team_name = str(radiant_team.get("name") or "")
    dire_team_name = str(dire_team.get("name") or "")
    if radiant_team_id is None and not radiant_team_name:
        return None
    if dire_team_id is None and not dire_team_name:
        return None

    league = raw_match.get("league") or {}
    series = raw_match.get("series") or {}
    radiant_win = raw_match.get("didRadiantWin")
    if not isinstance(radiant_win, bool):
        return None

    return PositionMatchRecord(
        match_id=match_id,
        timestamp=timestamp,
        radiant_win=radiant_win,
        radiant_team_id=radiant_team_id if isinstance(radiant_team_id, int) else None,
        radiant_team_name=radiant_team_name,
        dire_team_id=dire_team_id if isinstance(dire_team_id, int) else None,
        dire_team_name=dire_team_name,
        radiant_player_ids=radiant_player_ids,
        dire_player_ids=dire_player_ids,
        radiant_player_positions=radiant_positions,
        dire_player_positions=dire_positions,
        league_id=raw_match.get("leagueId") if isinstance(raw_match.get("leagueId"), int) else None,
        league_name=str(league.get("name") or ""),
        source_league_tier=str(league.get("tier")) if league.get("tier") is not None else None,
        series_id=series.get("id") if isinstance(series.get("id"), int) else None,
        series_type=str(series.get("type")) if series.get("type") is not None else None,
    )


def load_matches_with_positions(data_dir: Path) -> tuple[list[PositionMatchRecord], dict[str, int]]:
    summary: Counter[str] = Counter()
    matches: list[PositionMatchRecord] = []
    for json_path in sorted(data_dir.glob("*.json")):
        summary["files"] += 1
        with json_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict):
            continue
        summary["raw_matches"] += len(payload)
        for raw_match in payload.values():
            summary["seen_matches"] += 1
            if not isinstance(raw_match, dict):
                summary["skipped_non_dict"] += 1
                continue
            match = _parse_match(raw_match)
            if match is None:
                summary["skipped_invalid"] += 1
                continue
            matches.append(match)
            summary["loaded_matches"] += 1
    matches.sort(key=lambda match: (match.timestamp, match.match_id))
    return matches, dict(summary)


@dataclass(frozen=True)
class TrialCfg:
    name: str
    role_weight: float
    tier1_only: bool


class RoleAwareLocalModel(HybridPlayerRosterEloModel):
    def __init__(self, config: HybridEloConfig, *, role_weight: float, tier1_only: bool) -> None:
        super().__init__(config)
        self.role_weight = max(0.0, min(float(role_weight), float(config.player_tier_weight)))
        self.tier1_only = bool(tier1_only)
        self.player_role_local: dict[LeagueTier, dict[tuple[int, str], float]] = {
            LeagueTier.TIER1: {},
            LeagueTier.TIER2: {},
            LeagueTier.TIER3: {},
        }
        self.player_role_local_last_seen_ts: dict[LeagueTier, dict[tuple[int, str], int]] = {
            LeagueTier.TIER1: {},
            LeagueTier.TIER2: {},
            LeagueTier.TIER3: {},
        }

    def _effective_role_weight(
        self,
        tier: LeagueTier,
        player_positions: tuple[str | None, ...] | None = None,
    ) -> float:
        if self.role_weight <= 0.0:
            return 0.0
        if self.tier1_only and tier != LeagueTier.TIER1:
            return 0.0
        if player_positions and any(position is None for position in player_positions):
            return 0.0
        return self.role_weight

    def _role_local_k_share(
        self,
        tier: LeagueTier,
        player_positions: tuple[str | None, ...] | None = None,
    ) -> float:
        effective_role_weight = self._effective_role_weight(tier, player_positions)
        if effective_role_weight <= 0.0 or self.config.player_tier_weight <= 0.0:
            return 0.0
        return effective_role_weight / self.config.player_tier_weight

    def _get_player_role_local_rating(
        self,
        player_id: int,
        position: str | None,
        tier: LeagueTier,
        timestamp: int,
        *,
        mutate: bool,
    ) -> float:
        if not position:
            return self.config.initial_rating
        store = self.player_role_local[tier]
        last_seen_store = self.player_role_local_last_seen_ts[tier]
        key = (player_id, position)
        rating = store.get(key, self.config.initial_rating)
        last_seen_ts = last_seen_store.get(key)
        if last_seen_ts is not None:
            rating = _decay_towards_target(
                rating=rating,
                target=self.config.initial_rating,
                elapsed_seconds=timestamp - last_seen_ts,
                half_life_days=self.config.player_local_decay_half_life_days,
            )
        if mutate:
            store[key] = rating
            last_seen_store[key] = timestamp
        return rating

    def _build_team_context_with_positions(
        self,
        *,
        team_id: int | None,
        team_name: str,
        player_ids: tuple[int, ...],
        player_positions: tuple[str | None, ...],
        tier: LeagueTier,
        timestamp: int,
        mutate: bool,
    ) -> _HybridTeamContext:
        player_global_avg = _mean(
            [self._get_player_global_rating(player_id, timestamp, mutate=mutate) for player_id in player_ids]
        )
        player_local_avg = _mean(
            [self._get_player_local_rating(player_id, tier, timestamp, mutate=mutate) for player_id in player_ids]
        )
        role_local_avg = _mean(
            [
                self._get_player_role_local_rating(player_id, position, tier, timestamp, mutate=mutate)
                for player_id, position in zip(player_ids, player_positions)
            ]
        )
        effective_role_weight = self._effective_role_weight(tier)
        base_local_weight = max(0.0, self.config.player_tier_weight - effective_role_weight)
        player_strength = (
            self.config.player_global_weight * player_global_avg
            + base_local_weight * player_local_avg
            + effective_role_weight * role_local_avg
        )
        org_prior_rating = self._org_prior_rating(team_id)

        org_key = _team_key(team_id, team_name)
        lineup_key = self._lineup_key(org_key, player_ids)
        lineup_matches = int(self.lineup_match_counts[lineup_key])
        lineup_k_multiplier = self._lineup_k_multiplier(lineup_matches, tier)
        roster_resolution = (
            self.roster_tracker.resolve(org_key, player_ids)
            if mutate
            else self.roster_tracker.preview(org_key, player_ids)
        )
        roster_key = roster_resolution.roster_key
        roster_matches = self.roster_match_counts[tier][roster_key]
        prior_fade = min(1.0, roster_matches / max(1, self.config.org_prior_fade_matches))
        prior_weight = self.config.cold_start_org_prior_weight * (1.0 - prior_fade)
        prior_blended_strength = (1.0 - prior_weight) * player_strength + prior_weight * org_prior_rating
        roster_rating = self._get_roster_rating(
            roster_key=roster_key,
            tier=tier,
            timestamp=timestamp,
            target_strength=prior_blended_strength,
            mutate=mutate,
        )
        if roster_resolution.continuity:
            continuity_share = roster_resolution.overlap_count / max(1, len(player_ids))
        else:
            continuity_share = 0.0
        roster_confidence = min(1.0, roster_matches / max(1, self.config.roster_full_weight_matches))
        roster_weight = self.config.max_roster_weight * roster_confidence * continuity_share
        team_strength = (1.0 - roster_weight) * prior_blended_strength + roster_weight * roster_rating

        context = _HybridTeamContext(
            team_strength=team_strength,
            player_strength=player_strength,
            prior_blended_strength=prior_blended_strength,
            player_global_avg=player_global_avg,
            player_local_avg=player_local_avg,
            lineup_key=lineup_key,
            lineup_matches=lineup_matches,
            lineup_k_multiplier=lineup_k_multiplier,
            roster_key=roster_key,
            roster_rating=roster_rating,
            roster_matches=roster_matches,
            roster_weight=roster_weight,
            overlap_count=roster_resolution.overlap_count,
            continuity=roster_resolution.continuity,
        )
        return context

    def _preview_match(self, match: PositionMatchRecord, *, mutate: bool):
        tier = match.derived_league_tier
        radiant_context = self._build_team_context_with_positions(
            team_id=match.radiant_team_id,
            team_name=match.radiant_team_name,
            player_ids=match.radiant_player_ids,
            player_positions=match.radiant_player_positions,
            tier=tier,
            timestamp=match.timestamp,
            mutate=mutate,
        )
        dire_context = self._build_team_context_with_positions(
            team_id=match.dire_team_id,
            team_name=match.dire_team_name,
            player_ids=match.dire_player_ids,
            player_positions=match.dire_player_positions,
            tier=tier,
            timestamp=match.timestamp,
            mutate=mutate,
        )
        side_bias = self.side_bias[tier]
        rating_diff = radiant_context.team_strength + side_bias - dire_context.team_strength
        p_radiant = _elo_probability(rating_diff, self.config.elo_scale)
        step = StepResult(
            p_radiant=p_radiant,
            radiant_strength=radiant_context.team_strength,
            dire_strength=dire_context.team_strength,
            metadata={
                "tier": tier.value,
                "radiant_player_strength": radiant_context.player_strength,
                "dire_player_strength": dire_context.player_strength,
                "radiant_player_global_avg": radiant_context.player_global_avg,
                "dire_player_global_avg": dire_context.player_global_avg,
                "radiant_player_local_avg": radiant_context.player_local_avg,
                "dire_player_local_avg": dire_context.player_local_avg,
                "radiant_lineup_key": radiant_context.lineup_key,
                "dire_lineup_key": dire_context.lineup_key,
                "radiant_lineup_matches": radiant_context.lineup_matches,
                "dire_lineup_matches": dire_context.lineup_matches,
                "radiant_lineup_k_multiplier": radiant_context.lineup_k_multiplier,
                "dire_lineup_k_multiplier": dire_context.lineup_k_multiplier,
                "radiant_roster_key": radiant_context.roster_key,
                "dire_roster_key": dire_context.roster_key,
                "radiant_roster_weight": radiant_context.roster_weight,
                "dire_roster_weight": dire_context.roster_weight,
                "side_bias": side_bias,
                "role_weight": self._effective_role_weight(tier),
            },
        )
        return step, radiant_context, dire_context, tier, side_bias

    def predict_match(self, match: PositionMatchRecord) -> StepResult:
        step, _, _, _, _ = self._preview_match(match, mutate=False)
        return step

    def process_match(self, match: PositionMatchRecord) -> StepResult:
        self._maybe_apply_patch_local_reset(match.timestamp)
        step, radiant_context, dire_context, tier, side_bias = self._preview_match(match, mutate=True)
        actual = 1.0 if match.radiant_win else 0.0
        error = actual - step.p_radiant

        k_global = self.config.k_global_by_tier[tier]
        k_local = self.config.k_local_by_tier[tier]
        k_roster = self.config.k_roster_by_tier[tier]
        role_local_share = self._role_local_k_share(tier)
        role_local_k = k_local * role_local_share
        tier_local_k = k_local * (1.0 - role_local_share)
        rad_mult = radiant_context.lineup_k_multiplier
        dire_mult = dire_context.lineup_k_multiplier
        rad_org = _team_key(match.radiant_team_id, match.radiant_team_name)
        dire_org = _team_key(match.dire_team_id, match.dire_team_name)

        for player_id, position in zip(match.radiant_player_ids, match.radiant_player_positions):
            player_mult = self._player_org_k_multiplier(player_id, rad_org, tier)
            global_mult = rad_mult if self.config.lineup_uncertainty_boost_global else 1.0
            local_mult = rad_mult if self.config.lineup_uncertainty_boost_local else 1.0
            if self.config.player_org_uncertainty_boost_global:
                global_mult *= player_mult
            if self.config.player_org_uncertainty_boost_local:
                local_mult *= player_mult
            self.player_global[player_id] += k_global * global_mult * error
            self.player_local[tier][player_id] += tier_local_k * local_mult * error
            if position:
                key = (player_id, position)
                self.player_role_local[tier][key] = self.player_role_local[tier].get(
                    key, self.config.initial_rating
                ) + role_local_k * local_mult * error
                self.player_role_local_last_seen_ts[tier][key] = match.timestamp
            self._commit_player_org(player_id, rad_org)
        for player_id, position in zip(match.dire_player_ids, match.dire_player_positions):
            player_mult = self._player_org_k_multiplier(player_id, dire_org, tier)
            global_mult = dire_mult if self.config.lineup_uncertainty_boost_global else 1.0
            local_mult = dire_mult if self.config.lineup_uncertainty_boost_local else 1.0
            if self.config.player_org_uncertainty_boost_global:
                global_mult *= player_mult
            if self.config.player_org_uncertainty_boost_local:
                local_mult *= player_mult
            self.player_global[player_id] -= k_global * global_mult * error
            self.player_local[tier][player_id] -= tier_local_k * local_mult * error
            if position:
                key = (player_id, position)
                self.player_role_local[tier][key] = self.player_role_local[tier].get(
                    key, self.config.initial_rating
                ) - role_local_k * local_mult * error
                self.player_role_local_last_seen_ts[tier][key] = match.timestamp
            self._commit_player_org(player_id, dire_org)

        self.roster_ratings[tier][radiant_context.roster_key] = radiant_context.roster_rating + (
            k_roster * (rad_mult if self.config.lineup_uncertainty_boost_roster else 1.0) * error
        )
        self.roster_ratings[tier][dire_context.roster_key] = dire_context.roster_rating - (
            k_roster * (dire_mult if self.config.lineup_uncertainty_boost_roster else 1.0) * error
        )
        self.roster_match_counts[tier][radiant_context.roster_key] = radiant_context.roster_matches + 1
        self.roster_match_counts[tier][dire_context.roster_key] = dire_context.roster_matches + 1
        self.lineup_match_counts[radiant_context.lineup_key] = radiant_context.lineup_matches + 1
        self.lineup_match_counts[dire_context.lineup_key] = dire_context.lineup_matches + 1
        self.side_bias[tier] = side_bias + self.config.side_bias_k * error

        step.metadata["k_global"] = k_global
        step.metadata["k_local"] = k_local
        step.metadata["k_roster"] = k_roster
        step.metadata["role_local_k_share"] = role_local_share
        step.metadata["side_bias"] = self.side_bias[tier]
        return step


def _summarize_trial(
    trial: TrialCfg,
    report: dict[str, Any],
    baseline: dict[str, Any],
) -> dict[str, Any]:
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
    data_dir = _default_data_dir()
    output_path = _default_output_path()

    matches, load_summary = load_matches_with_positions(data_dir)
    if not matches:
        raise SystemExit("No valid matches were loaded.")

    league_info, league_summary = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)

    eval_cfg = EvaluationConfig()
    baseline_cfg = HybridEloConfig()
    baseline_report = run_series_online_evaluation(
        model=HybridPlayerRosterEloModel(config=baseline_cfg),
        series_bundles=series_bundles,
        config=eval_cfg,
    )

    trials = [
        TrialCfg("baseline", 0.0, False),
        TrialCfg("role_local_0.08_t1only", 0.08, True),
        TrialCfg("role_local_0.12_t1only", 0.12, True),
        TrialCfg("role_local_0.16_t1only", 0.16, True),
        TrialCfg("role_local_0.08_all", 0.08, False),
        TrialCfg("role_local_0.16_all", 0.16, False),
        TrialCfg("role_local_full_0.32_t1only", 0.32, True),
    ]

    results: list[dict[str, Any]] = []
    for trial in trials:
        if trial.name == "baseline":
            report = baseline_report
        else:
            report = run_series_online_evaluation(
                model=RoleAwareLocalModel(
                    config=baseline_cfg,
                    role_weight=trial.role_weight,
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
    best_by_overall_log_loss = min(
        results,
        key=lambda row: (row["log_loss"], row["brier"], -row["accuracy"]),
    )

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

    print(f"Saved role-local sweep to {output_path}")
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
