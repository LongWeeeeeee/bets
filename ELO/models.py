from __future__ import annotations

import math
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from ELO.config import HybridEloConfig, SimpleTeamEloConfig
from ELO.domain import LeagueTier, MatchRecord, StepResult
from ELO.roster import RosterLineageTracker
from ELO.team_identity import resolve_org_key
from ELO.tiering import get_known_team_tier

_SECONDS_PER_DAY = 24 * 60 * 60
_PATCH_RELEASES_RAW: tuple[tuple[str, str], ...] = (
    ("7.40c", "2026-01-21"),
    ("7.40b", "2025-12-23"),
    ("7.40", "2025-12-15"),
    ("7.39e", "2025-10-02"),
    ("7.39d", "2025-08-05"),
    ("7.39c", "2025-06-24"),
    ("7.39b", "2025-05-29"),
    ("7.39", "2025-05-21"),
    ("7.38c", "2025-03-27"),
    ("7.38b", "2025-03-05"),
    ("7.38", "2025-02-19"),
    ("7.37e", "2024-11-19"),
    ("7.37d", "2024-10-01"),
    ("7.37c", "2024-08-28"),
    ("7.37b", "2024-08-14"),
    ("7.37", "2024-07-31"),
    ("7.36c", "2024-06-24"),
    ("7.36b", "2024-06-05"),
    ("7.36a", "2024-05-26"),
    ("7.36", "2024-05-22"),
    ("7.35d", "2024-03-21"),
    ("7.35c", "2024-02-21"),
)


@dataclass(frozen=True)
class _PatchRelease:
    label: str
    release_ts: int


def _patch_release_ts(date_str: str) -> int:
    from datetime import datetime, timezone

    return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


_PATCH_RELEASES: tuple[_PatchRelease, ...] = tuple(
    _PatchRelease(label=label, release_ts=_patch_release_ts(date_str))
    for label, date_str in _PATCH_RELEASES_RAW
)


def _team_key(team_id: int | None, team_name: str) -> str:
    return resolve_org_key(team_id, team_name)


def _aligned_positions(
    player_ids: tuple[int, ...],
    player_positions: tuple[str | None, ...] | None,
) -> tuple[str | None, ...]:
    if player_positions and len(player_positions) == len(player_ids):
        return tuple(player_positions)
    return tuple(None for _ in player_ids)


def _elo_probability(rating_diff: float, scale: float) -> float:
    return 1.0 / (1.0 + math.pow(10.0, -rating_diff / scale))


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _build_simple_step(
    p_radiant: float,
    radiant_strength: float,
    dire_strength: float,
    metadata: dict[str, float | str],
) -> StepResult:
    return StepResult(
        p_radiant=p_radiant,
        radiant_strength=radiant_strength,
        dire_strength=dire_strength,
        metadata=metadata,
    )


def _patch_label_for_timestamp(timestamp: int) -> str:
    if timestamp <= 0:
        return "unknown"
    for patch in _PATCH_RELEASES:
        if timestamp >= patch.release_ts:
            return patch.label
    return "pre_7.35c"


def _patch_key_for_timestamp(timestamp: int, mode: str) -> str:
    label = _patch_label_for_timestamp(timestamp)
    if str(mode).strip().lower() == "major":
        match = re.match(r"^(\d+\.\d+)", label)
        return match.group(1) if match else label
    return label


def _decay_towards_target(
    rating: float,
    target: float,
    elapsed_seconds: int,
    half_life_days: float,
) -> float:
    if elapsed_seconds <= 0 or half_life_days <= 0:
        return rating
    half_life_seconds = half_life_days * _SECONDS_PER_DAY
    if half_life_seconds <= 0:
        return rating
    keep_factor = math.pow(0.5, elapsed_seconds / half_life_seconds)
    return target + (rating - target) * keep_factor


@dataclass
class _HybridTeamContext:
    team_strength: float
    player_strength: float
    prior_blended_strength: float
    player_global_avg: float
    player_local_avg: float
    lineup_key: str
    lineup_matches: int
    lineup_k_multiplier: float
    roster_key: str
    roster_rating: float
    roster_matches: int
    roster_weight: float
    overlap_count: int
    continuity: bool


class SimpleTeamEloModel:
    name = "simple_team_elo"

    def __init__(self, config: SimpleTeamEloConfig) -> None:
        self.config = config
        self.team_ratings: dict[str, float] = {}
        self.team_last_seen_ts: dict[str, int] = {}
        self.side_bias: dict[LeagueTier, float] = {tier: 0.0 for tier in LeagueTier}

    def _team_prior_rating(self, team_id: int | None) -> float:
        known_tier = get_known_team_tier(team_id)
        if known_tier is None:
            return self.config.initial_rating
        return self.config.initial_rating + self.config.team_prior_bonus_by_tier[known_tier]

    def _get_team_rating(
        self,
        team_key: str,
        team_id: int | None,
        timestamp: int,
        *,
        mutate: bool,
    ) -> float:
        rating = self.team_ratings.get(team_key, self._team_prior_rating(team_id))
        last_seen_ts = self.team_last_seen_ts.get(team_key)
        if last_seen_ts is not None:
            rating = _decay_towards_target(
                rating=rating,
                target=self._team_prior_rating(team_id),
                elapsed_seconds=timestamp - last_seen_ts,
                half_life_days=self.config.team_decay_half_life_days,
            )
        if mutate:
            self.team_ratings[team_key] = rating
            self.team_last_seen_ts[team_key] = timestamp
        return rating

    def _preview_match(self, match: MatchRecord, *, mutate: bool) -> tuple[StepResult, dict[str, str], float]:
        radiant_key = _team_key(match.radiant_team_id, match.radiant_team_name)
        dire_key = _team_key(match.dire_team_id, match.dire_team_name)
        radiant_rating = self._get_team_rating(
            radiant_key,
            match.radiant_team_id,
            match.timestamp,
            mutate=mutate,
        )
        dire_rating = self._get_team_rating(
            dire_key,
            match.dire_team_id,
            match.timestamp,
            mutate=mutate,
        )
        side_bias = self.side_bias[match.derived_league_tier]
        rating_diff = radiant_rating + side_bias - dire_rating
        p_radiant = _elo_probability(rating_diff, self.config.elo_scale)
        step = _build_simple_step(
            p_radiant=p_radiant,
            radiant_strength=radiant_rating,
            dire_strength=dire_rating,
            metadata={
                "radiant_team_key": radiant_key,
                "dire_team_key": dire_key,
                "side_bias": side_bias,
            },
        )
        return step, {"radiant_key": radiant_key, "dire_key": dire_key}, side_bias

    def predict_match(self, match: MatchRecord) -> StepResult:
        step, _, _ = self._preview_match(match, mutate=False)
        return step

    def process_match(self, match: MatchRecord) -> StepResult:
        step, team_keys, side_bias = self._preview_match(match, mutate=True)

        actual = 1.0 if match.radiant_win else 0.0
        error = actual - step.p_radiant
        k_value = self.config.base_k * self.config.tier_k_multiplier[match.derived_league_tier]
        delta = k_value * error

        self.team_ratings[team_keys["radiant_key"]] = step.radiant_strength + delta
        self.team_ratings[team_keys["dire_key"]] = step.dire_strength - delta
        self.side_bias[match.derived_league_tier] = side_bias + self.config.side_bias_k * error
        step.metadata["k_value"] = k_value
        step.metadata["side_bias"] = self.side_bias[match.derived_league_tier]
        return step


class HybridPlayerRosterEloModel:
    name = "hybrid_player_roster_elo"

    def __init__(self, config: HybridEloConfig) -> None:
        self.config = config
        self.player_global: dict[int, float] = {}
        self.player_global_last_seen_ts: dict[int, int] = {}
        self.player_local: dict[LeagueTier, dict[int, float]] = {
            LeagueTier.TIER1: {},
            LeagueTier.TIER2: {},
            LeagueTier.TIER3: {},
        }
        self.player_local_last_seen_ts: dict[LeagueTier, dict[int, int]] = {
            LeagueTier.TIER1: {},
            LeagueTier.TIER2: {},
            LeagueTier.TIER3: {},
        }
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
        self.roster_ratings: dict[LeagueTier, dict[str, float]] = {
            LeagueTier.TIER1: {},
            LeagueTier.TIER2: {},
            LeagueTier.TIER3: {},
        }
        self.roster_last_seen_ts: dict[LeagueTier, dict[str, int]] = {
            LeagueTier.TIER1: {},
            LeagueTier.TIER2: {},
            LeagueTier.TIER3: {},
        }
        self.roster_match_counts: dict[LeagueTier, defaultdict[str, int]] = {
            LeagueTier.TIER1: defaultdict(int),
            LeagueTier.TIER2: defaultdict(int),
            LeagueTier.TIER3: defaultdict(int),
        }
        self.lineup_match_counts: defaultdict[str, int] = defaultdict(int)
        self.player_current_org: dict[int, str] = {}
        self.player_current_org_matches: defaultdict[tuple[int, str], int] = defaultdict(int)
        self.current_patch_key: str | None = None
        self.roster_tracker = RosterLineageTracker(min_shared_players=3)
        self.side_bias: dict[LeagueTier, float] = {tier: 0.0 for tier in LeagueTier}

    def export_state(self) -> dict[str, Any]:
        return {
            "config": {
                "initial_rating": float(self.config.initial_rating),
                "elo_scale": float(self.config.elo_scale),
                "bo3_sweep_bonus_weight": float(self.config.bo3_sweep_bonus_weight),
                "bo3_sweep_bonus_error_basis": str(self.config.bo3_sweep_bonus_error_basis),
                "player_global_weight": float(self.config.player_global_weight),
                "player_tier_weight": float(self.config.player_tier_weight),
                "player_role_weight": float(self.config.player_role_weight),
                "player_role_tier1_only": bool(self.config.player_role_tier1_only),
                "max_roster_weight": float(self.config.max_roster_weight),
                "roster_full_weight_matches": int(self.config.roster_full_weight_matches),
                "lineup_uncertainty_boost_max": float(self.config.lineup_uncertainty_boost_max),
                "lineup_uncertainty_boost_matches": int(self.config.lineup_uncertainty_boost_matches),
                "lineup_uncertainty_boost_global": bool(self.config.lineup_uncertainty_boost_global),
                "lineup_uncertainty_boost_local": bool(self.config.lineup_uncertainty_boost_local),
                "lineup_uncertainty_boost_roster": bool(self.config.lineup_uncertainty_boost_roster),
                "lineup_uncertainty_tier1_enabled": bool(self.config.lineup_uncertainty_tier1_enabled),
                "player_org_uncertainty_boost_max": float(self.config.player_org_uncertainty_boost_max),
                "player_org_uncertainty_boost_matches": int(self.config.player_org_uncertainty_boost_matches),
                "player_org_uncertainty_boost_global": bool(self.config.player_org_uncertainty_boost_global),
                "player_org_uncertainty_boost_local": bool(self.config.player_org_uncertainty_boost_local),
                "player_org_uncertainty_tier1_enabled": bool(self.config.player_org_uncertainty_tier1_enabled),
                "patch_local_reset_mode": str(self.config.patch_local_reset_mode),
                "patch_local_reset_player_local_keep": float(self.config.patch_local_reset_player_local_keep),
                "patch_local_reset_roster_keep": float(self.config.patch_local_reset_roster_keep),
                "patch_local_reset_tier1_only": bool(self.config.patch_local_reset_tier1_only),
                "inactivity_penalty_gap_days": int(self.config.inactivity_penalty_gap_days),
                "inactivity_penalty_keep": float(self.config.inactivity_penalty_keep),
                "inactivity_penalty_local": bool(self.config.inactivity_penalty_local),
                "inactivity_penalty_roster": bool(self.config.inactivity_penalty_roster),
                "inactivity_penalty_global": bool(self.config.inactivity_penalty_global),
                "inactivity_penalty_tier1_only": bool(self.config.inactivity_penalty_tier1_only),
                "side_bias_k": float(self.config.side_bias_k),
                "player_global_decay_half_life_days": float(self.config.player_global_decay_half_life_days),
                "player_local_decay_half_life_days": float(self.config.player_local_decay_half_life_days),
                "roster_decay_half_life_days": float(self.config.roster_decay_half_life_days),
                "cold_start_org_prior_weight": float(self.config.cold_start_org_prior_weight),
                "org_prior_fade_matches": int(self.config.org_prior_fade_matches),
                "org_prior_rating_by_tier": {
                    tier.value: float(value) for tier, value in self.config.org_prior_rating_by_tier.items()
                },
                "k_global_by_tier": {
                    tier.value: float(value) for tier, value in self.config.k_global_by_tier.items()
                },
                "k_local_by_tier": {
                    tier.value: float(value) for tier, value in self.config.k_local_by_tier.items()
                },
                "k_roster_by_tier": {
                    tier.value: float(value) for tier, value in self.config.k_roster_by_tier.items()
                },
            },
            "player_global": {str(player_id): float(rating) for player_id, rating in self.player_global.items()},
            "player_global_last_seen_ts": {
                str(player_id): int(timestamp) for player_id, timestamp in self.player_global_last_seen_ts.items()
            },
            "player_local": {
                tier.value: {str(player_id): float(rating) for player_id, rating in store.items()}
                for tier, store in self.player_local.items()
            },
            "player_local_last_seen_ts": {
                tier.value: {str(player_id): int(timestamp) for player_id, timestamp in store.items()}
                for tier, store in self.player_local_last_seen_ts.items()
            },
            "player_role_local": {
                tier.value: {
                    f"{player_id}|{position}": float(rating)
                    for (player_id, position), rating in store.items()
                }
                for tier, store in self.player_role_local.items()
            },
            "player_role_local_last_seen_ts": {
                tier.value: {
                    f"{player_id}|{position}": int(timestamp)
                    for (player_id, position), timestamp in store.items()
                }
                for tier, store in self.player_role_local_last_seen_ts.items()
            },
            "roster_ratings": {
                tier.value: {str(roster_key): float(rating) for roster_key, rating in store.items()}
                for tier, store in self.roster_ratings.items()
            },
            "roster_last_seen_ts": {
                tier.value: {str(roster_key): int(timestamp) for roster_key, timestamp in store.items()}
                for tier, store in self.roster_last_seen_ts.items()
            },
            "roster_match_counts": {
                tier.value: {str(roster_key): int(count) for roster_key, count in store.items()}
                for tier, store in self.roster_match_counts.items()
            },
            "lineup_match_counts": {
                str(lineup_key): int(count) for lineup_key, count in self.lineup_match_counts.items()
            },
            "player_current_org": {
                str(player_id): str(org_key) for player_id, org_key in self.player_current_org.items()
            },
            "player_current_org_matches": {
                str(player_id): {
                    str(org_key): int(count)
                    for org_key, count in org_counts.items()
                }
                for player_id, org_counts in self._player_current_org_matches_nested().items()
            },
            "current_patch_key": str(self.current_patch_key) if self.current_patch_key else None,
            "side_bias": {tier.value: float(value) for tier, value in self.side_bias.items()},
            "roster_tracker": self.roster_tracker.export_state(),
        }

    @classmethod
    def from_state(cls, raw_state: dict[str, Any] | None) -> "HybridPlayerRosterEloModel":
        state = raw_state if isinstance(raw_state, dict) else {}
        raw_config = state.get("config") if isinstance(state.get("config"), dict) else {}
        default_config = HybridEloConfig()

        def _tier_map(
            raw_value: Any,
            fallback: dict[LeagueTier, float],
            cast_type: type[float] | type[int],
        ) -> dict[LeagueTier, float] | dict[LeagueTier, int]:
            raw_map = raw_value if isinstance(raw_value, dict) else {}
            out: dict[LeagueTier, float] | dict[LeagueTier, int] = {}
            for tier in LeagueTier:
                candidate = raw_map.get(tier.value, fallback[tier])
                try:
                    out[tier] = cast_type(candidate)
                except (TypeError, ValueError):
                    out[tier] = cast_type(fallback[tier])
            return out

        config = HybridEloConfig(
            initial_rating=float(raw_config.get("initial_rating", default_config.initial_rating)),
            elo_scale=float(raw_config.get("elo_scale", default_config.elo_scale)),
            bo3_sweep_bonus_weight=float(
                raw_config.get("bo3_sweep_bonus_weight", default_config.bo3_sweep_bonus_weight)
            ),
            bo3_sweep_bonus_error_basis=str(
                raw_config.get("bo3_sweep_bonus_error_basis", default_config.bo3_sweep_bonus_error_basis)
            ),
            player_global_weight=float(raw_config.get("player_global_weight", default_config.player_global_weight)),
            player_tier_weight=float(raw_config.get("player_tier_weight", default_config.player_tier_weight)),
            player_role_weight=float(raw_config.get("player_role_weight", default_config.player_role_weight)),
            player_role_tier1_only=bool(raw_config.get("player_role_tier1_only", default_config.player_role_tier1_only)),
            max_roster_weight=float(raw_config.get("max_roster_weight", default_config.max_roster_weight)),
            roster_full_weight_matches=int(
                raw_config.get("roster_full_weight_matches", default_config.roster_full_weight_matches)
            ),
            lineup_uncertainty_boost_max=float(
                raw_config.get("lineup_uncertainty_boost_max", default_config.lineup_uncertainty_boost_max)
            ),
            lineup_uncertainty_boost_matches=int(
                raw_config.get("lineup_uncertainty_boost_matches", default_config.lineup_uncertainty_boost_matches)
            ),
            lineup_uncertainty_boost_global=bool(
                raw_config.get("lineup_uncertainty_boost_global", default_config.lineup_uncertainty_boost_global)
            ),
            lineup_uncertainty_boost_local=bool(
                raw_config.get("lineup_uncertainty_boost_local", default_config.lineup_uncertainty_boost_local)
            ),
            lineup_uncertainty_boost_roster=bool(
                raw_config.get("lineup_uncertainty_boost_roster", default_config.lineup_uncertainty_boost_roster)
            ),
            lineup_uncertainty_tier1_enabled=bool(
                raw_config.get("lineup_uncertainty_tier1_enabled", default_config.lineup_uncertainty_tier1_enabled)
            ),
            player_org_uncertainty_boost_max=float(
                raw_config.get("player_org_uncertainty_boost_max", default_config.player_org_uncertainty_boost_max)
            ),
            player_org_uncertainty_boost_matches=int(
                raw_config.get(
                    "player_org_uncertainty_boost_matches",
                    default_config.player_org_uncertainty_boost_matches,
                )
            ),
            player_org_uncertainty_boost_global=bool(
                raw_config.get(
                    "player_org_uncertainty_boost_global",
                    default_config.player_org_uncertainty_boost_global,
                )
            ),
            player_org_uncertainty_boost_local=bool(
                raw_config.get(
                    "player_org_uncertainty_boost_local",
                    default_config.player_org_uncertainty_boost_local,
                )
            ),
            player_org_uncertainty_tier1_enabled=bool(
                raw_config.get(
                    "player_org_uncertainty_tier1_enabled",
                    default_config.player_org_uncertainty_tier1_enabled,
                )
            ),
            patch_local_reset_mode=str(
                raw_config.get("patch_local_reset_mode", default_config.patch_local_reset_mode)
            ),
            patch_local_reset_player_local_keep=float(
                raw_config.get(
                    "patch_local_reset_player_local_keep",
                    default_config.patch_local_reset_player_local_keep,
                )
            ),
            patch_local_reset_roster_keep=float(
                raw_config.get(
                    "patch_local_reset_roster_keep",
                    default_config.patch_local_reset_roster_keep,
                )
            ),
            patch_local_reset_tier1_only=bool(
                raw_config.get(
                    "patch_local_reset_tier1_only",
                    default_config.patch_local_reset_tier1_only,
                )
            ),
            inactivity_penalty_gap_days=int(
                raw_config.get(
                    "inactivity_penalty_gap_days",
                    default_config.inactivity_penalty_gap_days,
                )
            ),
            inactivity_penalty_keep=float(
                raw_config.get(
                    "inactivity_penalty_keep",
                    default_config.inactivity_penalty_keep,
                )
            ),
            inactivity_penalty_local=bool(
                raw_config.get(
                    "inactivity_penalty_local",
                    default_config.inactivity_penalty_local,
                )
            ),
            inactivity_penalty_roster=bool(
                raw_config.get(
                    "inactivity_penalty_roster",
                    default_config.inactivity_penalty_roster,
                )
            ),
            inactivity_penalty_global=bool(
                raw_config.get(
                    "inactivity_penalty_global",
                    default_config.inactivity_penalty_global,
                )
            ),
            inactivity_penalty_tier1_only=bool(
                raw_config.get(
                    "inactivity_penalty_tier1_only",
                    default_config.inactivity_penalty_tier1_only,
                )
            ),
            side_bias_k=float(raw_config.get("side_bias_k", default_config.side_bias_k)),
            player_global_decay_half_life_days=float(
                raw_config.get(
                    "player_global_decay_half_life_days",
                    default_config.player_global_decay_half_life_days,
                )
            ),
            player_local_decay_half_life_days=float(
                raw_config.get(
                    "player_local_decay_half_life_days",
                    default_config.player_local_decay_half_life_days,
                )
            ),
            roster_decay_half_life_days=float(
                raw_config.get("roster_decay_half_life_days", default_config.roster_decay_half_life_days)
            ),
            org_prior_rating_by_tier=_tier_map(
                raw_config.get("org_prior_rating_by_tier"),
                default_config.org_prior_rating_by_tier,
                float,
            ),
            cold_start_org_prior_weight=float(
                raw_config.get("cold_start_org_prior_weight", default_config.cold_start_org_prior_weight)
            ),
            org_prior_fade_matches=int(raw_config.get("org_prior_fade_matches", default_config.org_prior_fade_matches)),
            k_global_by_tier=_tier_map(raw_config.get("k_global_by_tier"), default_config.k_global_by_tier, float),
            k_local_by_tier=_tier_map(raw_config.get("k_local_by_tier"), default_config.k_local_by_tier, float),
            k_roster_by_tier=_tier_map(raw_config.get("k_roster_by_tier"), default_config.k_roster_by_tier, float),
        )
        model = cls(config)

        def _load_player_map(raw_map: Any) -> dict[int, float]:
            if not isinstance(raw_map, dict):
                return {}
            out: dict[int, float] = {}
            for key, value in raw_map.items():
                try:
                    out[int(key)] = float(value)
                except (TypeError, ValueError):
                    continue
            return out

        def _load_player_ts_map(raw_map: Any) -> dict[int, int]:
            if not isinstance(raw_map, dict):
                return {}
            out: dict[int, int] = {}
            for key, value in raw_map.items():
                try:
                    out[int(key)] = int(value)
                except (TypeError, ValueError):
                    continue
            return out

        def _load_tiered_player_map(raw_map: Any) -> dict[LeagueTier, dict[int, float]]:
            out: dict[LeagueTier, dict[int, float]] = {tier: {} for tier in LeagueTier}
            if not isinstance(raw_map, dict):
                return out
            for tier in LeagueTier:
                out[tier] = _load_player_map(raw_map.get(tier.value))
            return out

        def _load_tiered_player_ts_map(raw_map: Any) -> dict[LeagueTier, dict[int, int]]:
            out: dict[LeagueTier, dict[int, int]] = {tier: {} for tier in LeagueTier}
            if not isinstance(raw_map, dict):
                return out
            for tier in LeagueTier:
                out[tier] = _load_player_ts_map(raw_map.get(tier.value))
            return out

        def _load_tiered_role_map(raw_map: Any, cast_type: type[float] | type[int]) -> dict[LeagueTier, dict[tuple[int, str], float]] | dict[LeagueTier, dict[tuple[int, str], int]]:
            out: dict[LeagueTier, dict[tuple[int, str], float]] | dict[LeagueTier, dict[tuple[int, str], int]] = {
                tier: {} for tier in LeagueTier
            }
            if not isinstance(raw_map, dict):
                return out
            for tier in LeagueTier:
                tier_payload = raw_map.get(tier.value)
                tier_store: dict[tuple[int, str], float] | dict[tuple[int, str], int] = {}
                if isinstance(tier_payload, dict):
                    for raw_key, raw_value in tier_payload.items():
                        try:
                            player_str, position = str(raw_key).split("|", 1)
                            tier_store[(int(player_str), str(position))] = cast_type(raw_value)
                        except (TypeError, ValueError):
                            continue
                out[tier] = tier_store
            return out

        def _load_tiered_roster_map(raw_map: Any, cast_type: type[float] | type[int]) -> dict[LeagueTier, dict[str, float]] | dict[LeagueTier, dict[str, int]]:
            out: dict[LeagueTier, dict[str, float]] | dict[LeagueTier, dict[str, int]] = {tier: {} for tier in LeagueTier}
            if not isinstance(raw_map, dict):
                return out
            for tier in LeagueTier:
                tier_payload = raw_map.get(tier.value)
                tier_store: dict[str, float] | dict[str, int] = {}
                if isinstance(tier_payload, dict):
                    for key, value in tier_payload.items():
                        try:
                            tier_store[str(key)] = cast_type(value)
                        except (TypeError, ValueError):
                            continue
                out[tier] = tier_store
            return out

        model.player_global = _load_player_map(state.get("player_global"))
        model.player_global_last_seen_ts = _load_player_ts_map(state.get("player_global_last_seen_ts"))
        model.player_local = _load_tiered_player_map(state.get("player_local"))
        model.player_local_last_seen_ts = _load_tiered_player_ts_map(state.get("player_local_last_seen_ts"))
        model.player_role_local = _load_tiered_role_map(state.get("player_role_local"), float)
        model.player_role_local_last_seen_ts = _load_tiered_role_map(
            state.get("player_role_local_last_seen_ts"),
            int,
        )
        model.roster_ratings = _load_tiered_roster_map(state.get("roster_ratings"), float)
        model.roster_last_seen_ts = _load_tiered_roster_map(state.get("roster_last_seen_ts"), int)
        raw_roster_match_counts = _load_tiered_roster_map(state.get("roster_match_counts"), int)
        model.roster_match_counts = {
            tier: defaultdict(int, raw_roster_match_counts.get(tier, {}))
            for tier in LeagueTier
        }
        raw_lineup_match_counts = state.get("lineup_match_counts")
        if isinstance(raw_lineup_match_counts, dict):
            model.lineup_match_counts = defaultdict(
                int,
                {
                    str(lineup_key): int(count)
                    for lineup_key, count in raw_lineup_match_counts.items()
                    if isinstance(count, (int, float)) or (isinstance(count, str) and str(count).isdigit())
                },
            )
        raw_player_current_org = state.get("player_current_org")
        if isinstance(raw_player_current_org, dict):
            model.player_current_org = {
                int(player_id): str(org_key)
                for player_id, org_key in raw_player_current_org.items()
                if str(player_id).isdigit() and org_key is not None
            }
        raw_player_current_org_matches = state.get("player_current_org_matches")
        if isinstance(raw_player_current_org_matches, dict):
            restored_counts: dict[tuple[int, str], int] = {}
            for raw_player_id, raw_org_counts in raw_player_current_org_matches.items():
                try:
                    player_id = int(raw_player_id)
                except (TypeError, ValueError):
                    continue
                if not isinstance(raw_org_counts, dict):
                    continue
                for org_key, count in raw_org_counts.items():
                    try:
                        restored_counts[(player_id, str(org_key))] = int(count)
                    except (TypeError, ValueError):
                        continue
            model.player_current_org_matches = defaultdict(int, restored_counts)
        raw_current_patch_key = state.get("current_patch_key")
        if isinstance(raw_current_patch_key, str) and raw_current_patch_key:
            model.current_patch_key = raw_current_patch_key
        raw_side_bias = state.get("side_bias") if isinstance(state.get("side_bias"), dict) else {}
        model.side_bias = {
            tier: float(raw_side_bias.get(tier.value, 0.0) or 0.0) for tier in LeagueTier
        }
        model.roster_tracker = RosterLineageTracker.from_state(state.get("roster_tracker"))
        return model

    def _player_current_org_matches_nested(self) -> dict[int, dict[str, int]]:
        out: dict[int, dict[str, int]] = {}
        for (player_id, org_key), count in self.player_current_org_matches.items():
            out.setdefault(int(player_id), {})[str(org_key)] = int(count)
        return out

    def _org_prior_rating(self, team_id: int | None) -> float:
        known_tier = get_known_team_tier(team_id)
        if known_tier is None:
            return self.config.initial_rating
        return self.config.org_prior_rating_by_tier[known_tier]

    def _get_player_global_rating(self, player_id: int, timestamp: int, *, mutate: bool) -> float:
        rating = self.player_global.get(player_id, self.config.initial_rating)
        last_seen_ts = self.player_global_last_seen_ts.get(player_id)
        if last_seen_ts is not None:
            rating = _decay_towards_target(
                rating=rating,
                target=self.config.initial_rating,
                elapsed_seconds=timestamp - last_seen_ts,
                half_life_days=self.config.player_global_decay_half_life_days,
            )
        rating = self._apply_inactivity_penalty(
            rating=rating,
            target=self.config.initial_rating,
            last_seen_ts=last_seen_ts,
            timestamp=timestamp,
            enabled=bool(getattr(self.config, "inactivity_penalty_global", False)),
        )
        if mutate:
            self.player_global[player_id] = rating
            self.player_global_last_seen_ts[player_id] = timestamp
        return rating

    def _inactivity_penalty_enabled_for_tier(self, tier: LeagueTier) -> bool:
        if bool(getattr(self.config, "inactivity_penalty_tier1_only", True)):
            return tier == LeagueTier.TIER1
        return True

    def _apply_inactivity_penalty(
        self,
        *,
        rating: float,
        target: float,
        last_seen_ts: int | None,
        timestamp: int,
        enabled: bool,
        tier: LeagueTier | None = None,
    ) -> float:
        if not enabled:
            return rating
        if tier is not None and not self._inactivity_penalty_enabled_for_tier(tier):
            return rating
        if last_seen_ts is None:
            return rating
        gap_days = int(getattr(self.config, "inactivity_penalty_gap_days", 0) or 0)
        if gap_days <= 0:
            return rating
        if timestamp - last_seen_ts < gap_days * _SECONDS_PER_DAY:
            return rating
        keep = min(max(float(getattr(self.config, "inactivity_penalty_keep", 1.0)), 0.0), 1.0)
        return target + (rating - target) * keep

    def _get_player_local_rating(
        self,
        player_id: int,
        tier: LeagueTier,
        timestamp: int,
        *,
        mutate: bool,
    ) -> float:
        store = self.player_local[tier]
        last_seen_store = self.player_local_last_seen_ts[tier]
        rating = store.get(player_id, self.config.initial_rating)
        last_seen_ts = last_seen_store.get(player_id)
        if last_seen_ts is not None:
            rating = _decay_towards_target(
                rating=rating,
                target=self.config.initial_rating,
                elapsed_seconds=timestamp - last_seen_ts,
                half_life_days=self.config.player_local_decay_half_life_days,
            )
        rating = self._apply_inactivity_penalty(
            rating=rating,
            target=self.config.initial_rating,
            last_seen_ts=last_seen_ts,
            timestamp=timestamp,
            enabled=bool(getattr(self.config, "inactivity_penalty_local", False)),
            tier=tier,
        )
        if mutate:
            store[player_id] = rating
            last_seen_store[player_id] = timestamp
        return rating

    def _effective_role_weight(self, tier: LeagueTier, player_positions: tuple[str | None, ...] | None) -> float:
        if float(getattr(self.config, "player_role_weight", 0.0) or 0.0) <= 0.0:
            return 0.0
        if bool(getattr(self.config, "player_role_tier1_only", True)) and tier != LeagueTier.TIER1:
            return 0.0
        if not player_positions or any(position is None for position in player_positions):
            return 0.0
        return min(float(self.config.player_role_weight), float(self.config.player_tier_weight))

    def _role_local_k_share(self, tier: LeagueTier, player_positions: tuple[str | None, ...] | None) -> float:
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
        rating = self._apply_inactivity_penalty(
            rating=rating,
            target=self.config.initial_rating,
            last_seen_ts=last_seen_ts,
            timestamp=timestamp,
            enabled=bool(getattr(self.config, "inactivity_penalty_local", False)),
            tier=tier,
        )
        if mutate:
            store[key] = rating
            last_seen_store[key] = timestamp
        return rating

    def _get_roster_rating(
        self,
        roster_key: str,
        tier: LeagueTier,
        timestamp: int,
        target_strength: float,
        *,
        mutate: bool,
    ) -> float:
        store = self.roster_ratings[tier]
        last_seen_store = self.roster_last_seen_ts[tier]
        rating = store.get(roster_key, target_strength)
        last_seen_ts = last_seen_store.get(roster_key)
        if last_seen_ts is not None:
            rating = _decay_towards_target(
                rating=rating,
                target=target_strength,
                elapsed_seconds=timestamp - last_seen_ts,
                half_life_days=self.config.roster_decay_half_life_days,
            )
        rating = self._apply_inactivity_penalty(
            rating=rating,
            target=target_strength,
            last_seen_ts=last_seen_ts,
            timestamp=timestamp,
            enabled=bool(getattr(self.config, "inactivity_penalty_roster", False)),
            tier=tier,
        )
        if mutate:
            store[roster_key] = rating
            last_seen_store[roster_key] = timestamp
        return rating

    @staticmethod
    def _lineup_key(org_key: str, player_ids: tuple[int, ...]) -> str:
        ordered_ids = ",".join(str(player_id) for player_id in sorted(player_ids))
        return f"{org_key}::lineup:{ordered_ids}"

    def _lineup_k_multiplier(self, lineup_matches: int, tier: LeagueTier) -> float:
        boost_max = float(getattr(self.config, "lineup_uncertainty_boost_max", 0.0) or 0.0)
        stabilize_matches = int(getattr(self.config, "lineup_uncertainty_boost_matches", 0) or 0)
        if boost_max <= 0.0 or stabilize_matches <= 0:
            return 1.0
        if tier == LeagueTier.TIER1 and not bool(
            getattr(self.config, "lineup_uncertainty_tier1_enabled", False)
        ):
            return 1.0
        freshness = max(0.0, 1.0 - (float(lineup_matches) / float(max(1, stabilize_matches))))
        return 1.0 + boost_max * freshness

    def _player_org_k_multiplier(self, player_id: int, org_key: str, tier: LeagueTier) -> float:
        boost_max = float(getattr(self.config, "player_org_uncertainty_boost_max", 0.0) or 0.0)
        stabilize_matches = int(getattr(self.config, "player_org_uncertainty_boost_matches", 0) or 0)
        if boost_max <= 0.0 or stabilize_matches <= 0:
            return 1.0
        if tier == LeagueTier.TIER1 and not bool(
            getattr(self.config, "player_org_uncertainty_tier1_enabled", True)
        ):
            return 1.0
        current_org = self.player_current_org.get(player_id)
        if current_org != org_key:
            stint_matches = 0
        else:
            stint_matches = int(self.player_current_org_matches[(player_id, org_key)])
        freshness = max(0.0, 1.0 - (float(stint_matches) / float(max(1, stabilize_matches))))
        return 1.0 + boost_max * freshness

    def _commit_player_org(self, player_id: int, org_key: str) -> None:
        if self.player_current_org.get(player_id) != org_key:
            self.player_current_org[player_id] = org_key
            self.player_current_org_matches[(player_id, org_key)] = 1
            return
        self.player_current_org_matches[(player_id, org_key)] += 1

    def _maybe_apply_patch_local_reset(self, timestamp: int) -> None:
        mode = str(getattr(self.config, "patch_local_reset_mode", "none") or "none").strip().lower()
        if mode in {"", "none", "off", "disabled"}:
            return
        next_patch_key = _patch_key_for_timestamp(timestamp, mode)
        if not next_patch_key:
            return
        if self.current_patch_key is None:
            self.current_patch_key = next_patch_key
            return
        if next_patch_key == self.current_patch_key:
            return

        local_keep = min(
            max(float(getattr(self.config, "patch_local_reset_player_local_keep", 1.0)), 0.0),
            1.0,
        )
        roster_keep = min(
            max(float(getattr(self.config, "patch_local_reset_roster_keep", 1.0)), 0.0),
            1.0,
        )
        tiers = [LeagueTier.TIER1] if bool(getattr(self.config, "patch_local_reset_tier1_only", False)) else list(LeagueTier)
        for tier in tiers:
            for player_id, rating in list(self.player_local[tier].items()):
                self.player_local[tier][player_id] = (
                    self.config.initial_rating + (float(rating) - self.config.initial_rating) * local_keep
                )
            for player_role_key, rating in list(self.player_role_local[tier].items()):
                self.player_role_local[tier][player_role_key] = (
                    self.config.initial_rating + (float(rating) - self.config.initial_rating) * local_keep
                )
            for roster_key, rating in list(self.roster_ratings[tier].items()):
                self.roster_ratings[tier][roster_key] = (
                    self.config.initial_rating + (float(rating) - self.config.initial_rating) * roster_keep
                )
        self.current_patch_key = next_patch_key

    def _build_team_context(
        self,
        team_id: int | None,
        team_name: str,
        player_ids: tuple[int, ...],
        player_positions: tuple[str | None, ...] | None,
        tier: LeagueTier,
        timestamp: int,
        *,
        mutate: bool,
    ) -> _HybridTeamContext:
        aligned_positions = _aligned_positions(player_ids, player_positions)
        player_global_avg = _mean(
            [self._get_player_global_rating(player_id, timestamp, mutate=mutate) for player_id in player_ids]
        )
        player_local_avg = _mean(
            [self._get_player_local_rating(player_id, tier, timestamp, mutate=mutate) for player_id in player_ids]
        )
        effective_role_weight = self._effective_role_weight(tier, aligned_positions)
        base_local_weight = max(0.0, self.config.player_tier_weight - effective_role_weight)
        player_role_local_avg = 0.0
        if effective_role_weight > 0.0:
            player_role_local_avg = _mean(
                [
                    self._get_player_role_local_rating(player_id, position, tier, timestamp, mutate=mutate)
                    for player_id, position in zip(player_ids, aligned_positions)
                ]
            )
        player_strength = (
            self.config.player_global_weight * player_global_avg
            + base_local_weight * player_local_avg
            + effective_role_weight * player_role_local_avg
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
        roster_confidence = min(
            1.0,
            roster_matches / max(1, self.config.roster_full_weight_matches),
        )
        roster_weight = self.config.max_roster_weight * roster_confidence * continuity_share
        team_strength = (1.0 - roster_weight) * prior_blended_strength + roster_weight * roster_rating

        return _HybridTeamContext(
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

    def _preview_match(
        self,
        match: MatchRecord,
        *,
        mutate: bool,
    ) -> tuple[StepResult, _HybridTeamContext, _HybridTeamContext, LeagueTier, float]:
        tier = match.derived_league_tier
        radiant_context = self._build_team_context(
            team_id=match.radiant_team_id,
            team_name=match.radiant_team_name,
            player_ids=match.radiant_player_ids,
            player_positions=getattr(match, "radiant_player_positions", ()),
            tier=tier,
            timestamp=match.timestamp,
            mutate=mutate,
        )
        dire_context = self._build_team_context(
            team_id=match.dire_team_id,
            team_name=match.dire_team_name,
            player_ids=match.dire_player_ids,
            player_positions=getattr(match, "dire_player_positions", ()),
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
                "radiant_prior_blended_strength": radiant_context.prior_blended_strength,
                "dire_prior_blended_strength": dire_context.prior_blended_strength,
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
                "radiant_overlap": radiant_context.overlap_count,
                "dire_overlap": dire_context.overlap_count,
                "side_bias": side_bias,
            },
        )
        return step, radiant_context, dire_context, tier, side_bias

    def predict_match(self, match: MatchRecord) -> StepResult:
        step, _, _, _, _ = self._preview_match(match, mutate=False)
        return step

    def preview_team_strength(
        self,
        *,
        team_id: int | None,
        team_name: str,
        player_ids: tuple[int, ...],
        player_positions: tuple[str | None, ...] | None = None,
        tier: LeagueTier,
        timestamp: int,
    ) -> dict[str, float | str | int | bool]:
        context = self._build_team_context(
            team_id=team_id,
            team_name=team_name,
            player_ids=player_ids,
            player_positions=player_positions,
            tier=tier,
            timestamp=timestamp,
            mutate=False,
        )
        return {
            "team_strength": context.team_strength,
            "player_strength": context.player_strength,
            "prior_blended_strength": context.prior_blended_strength,
            "player_global_avg": context.player_global_avg,
            "player_local_avg": context.player_local_avg,
            "lineup_key": context.lineup_key,
            "lineup_matches": context.lineup_matches,
            "lineup_k_multiplier": context.lineup_k_multiplier,
            "roster_key": context.roster_key,
            "roster_rating": context.roster_rating,
            "roster_matches": context.roster_matches,
            "roster_weight": context.roster_weight,
            "overlap_count": context.overlap_count,
            "continuity": context.continuity,
        }

    def apply_bo3_sweep_bonus(
        self,
        *,
        first_map: MatchRecord,
        actual: float,
        pre_map_prob: float,
        pre_series_prob: float,
    ) -> bool:
        weight = float(getattr(self.config, "bo3_sweep_bonus_weight", 0.0) or 0.0)
        if weight <= 0.0:
            return False
        error_basis = str(getattr(self.config, "bo3_sweep_bonus_error_basis", "series") or "series").casefold()
        expected = pre_series_prob if error_basis == "series" else pre_map_prob
        error = actual - expected
        if error == 0.0:
            return False

        tier = first_map.derived_league_tier
        radiant_context = self._build_team_context(
            team_id=first_map.radiant_team_id,
            team_name=first_map.radiant_team_name,
            player_ids=first_map.radiant_player_ids,
            player_positions=getattr(first_map, "radiant_player_positions", ()),
            tier=tier,
            timestamp=first_map.timestamp,
            mutate=False,
        )
        dire_context = self._build_team_context(
            team_id=first_map.dire_team_id,
            team_name=first_map.dire_team_name,
            player_ids=first_map.dire_player_ids,
            player_positions=getattr(first_map, "dire_player_positions", ()),
            tier=tier,
            timestamp=first_map.timestamp,
            mutate=False,
        )

        k_global = self.config.k_global_by_tier[tier] * weight
        k_local = self.config.k_local_by_tier[tier] * weight
        k_roster = self.config.k_roster_by_tier[tier] * weight
        radiant_positions = _aligned_positions(
            first_map.radiant_player_ids,
            getattr(first_map, "radiant_player_positions", ()),
        )
        dire_positions = _aligned_positions(
            first_map.dire_player_ids,
            getattr(first_map, "dire_player_positions", ()),
        )
        rad_role_share = self._role_local_k_share(tier, radiant_positions)
        dire_role_share = self._role_local_k_share(tier, dire_positions)
        rad_role_local_k = k_local * rad_role_share
        dire_role_local_k = k_local * dire_role_share
        rad_tier_local_k = k_local * (1.0 - rad_role_share)
        dire_tier_local_k = k_local * (1.0 - dire_role_share)
        rad_mult = radiant_context.lineup_k_multiplier
        dire_mult = dire_context.lineup_k_multiplier

        for player_id, position in zip(first_map.radiant_player_ids, radiant_positions):
            self.player_global[player_id] = self.player_global.get(player_id, self.config.initial_rating) + (
                k_global * (rad_mult if self.config.lineup_uncertainty_boost_global else 1.0) * error
            )
            self.player_local[tier][player_id] = self.player_local[tier].get(player_id, self.config.initial_rating) + (
                rad_tier_local_k * (rad_mult if self.config.lineup_uncertainty_boost_local else 1.0) * error
            )
            if position and rad_role_local_k > 0.0:
                role_key = (player_id, position)
                self.player_role_local[tier][role_key] = self.player_role_local[tier].get(
                    role_key,
                    self.config.initial_rating,
                ) + (rad_role_local_k * (rad_mult if self.config.lineup_uncertainty_boost_local else 1.0) * error)
        for player_id, position in zip(first_map.dire_player_ids, dire_positions):
            self.player_global[player_id] = self.player_global.get(player_id, self.config.initial_rating) - (
                k_global * (dire_mult if self.config.lineup_uncertainty_boost_global else 1.0) * error
            )
            self.player_local[tier][player_id] = self.player_local[tier].get(player_id, self.config.initial_rating) - (
                dire_tier_local_k * (dire_mult if self.config.lineup_uncertainty_boost_local else 1.0) * error
            )
            if position and dire_role_local_k > 0.0:
                role_key = (player_id, position)
                self.player_role_local[tier][role_key] = self.player_role_local[tier].get(
                    role_key,
                    self.config.initial_rating,
                ) - (dire_role_local_k * (dire_mult if self.config.lineup_uncertainty_boost_local else 1.0) * error)

        self.roster_ratings[tier][radiant_context.roster_key] = radiant_context.roster_rating + (
            k_roster * (rad_mult if self.config.lineup_uncertainty_boost_roster else 1.0) * error
        )
        self.roster_ratings[tier][dire_context.roster_key] = dire_context.roster_rating - (
            k_roster * (dire_mult if self.config.lineup_uncertainty_boost_roster else 1.0) * error
        )
        return True

    def process_match(self, match: MatchRecord) -> StepResult:
        self._maybe_apply_patch_local_reset(match.timestamp)
        step, radiant_context, dire_context, tier, side_bias = self._preview_match(match, mutate=True)
        actual = 1.0 if match.radiant_win else 0.0
        error = actual - step.p_radiant

        k_global = self.config.k_global_by_tier[tier]
        k_local = self.config.k_local_by_tier[tier]
        k_roster = self.config.k_roster_by_tier[tier]
        radiant_positions = _aligned_positions(
            match.radiant_player_ids,
            getattr(match, "radiant_player_positions", ()),
        )
        dire_positions = _aligned_positions(
            match.dire_player_ids,
            getattr(match, "dire_player_positions", ()),
        )
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
        rad_player_org_mults: list[float] = []
        dire_player_org_mults: list[float] = []
        rad_global_mults: list[float] = []
        dire_global_mults: list[float] = []
        rad_local_mults: list[float] = []
        dire_local_mults: list[float] = []

        for player_id, position in zip(match.radiant_player_ids, radiant_positions):
            player_mult = self._player_org_k_multiplier(player_id, rad_org, tier)
            global_mult = rad_mult if self.config.lineup_uncertainty_boost_global else 1.0
            local_mult = rad_mult if self.config.lineup_uncertainty_boost_local else 1.0
            if self.config.player_org_uncertainty_boost_global:
                global_mult *= player_mult
            if self.config.player_org_uncertainty_boost_local:
                local_mult *= player_mult
            rad_player_org_mults.append(player_mult)
            rad_global_mults.append(global_mult)
            rad_local_mults.append(local_mult)
            self.player_global[player_id] += (
                k_global * global_mult * error
            )
            self.player_local[tier][player_id] += (
                rad_tier_local_k * local_mult * error
            )
            if position and rad_role_local_k > 0.0:
                role_key = (player_id, position)
                self.player_role_local[tier][role_key] = self.player_role_local[tier].get(
                    role_key,
                    self.config.initial_rating,
                ) + (rad_role_local_k * local_mult * error)
                self.player_role_local_last_seen_ts[tier][role_key] = match.timestamp
            self._commit_player_org(player_id, rad_org)
        for player_id, position in zip(match.dire_player_ids, dire_positions):
            player_mult = self._player_org_k_multiplier(player_id, dire_org, tier)
            global_mult = dire_mult if self.config.lineup_uncertainty_boost_global else 1.0
            local_mult = dire_mult if self.config.lineup_uncertainty_boost_local else 1.0
            if self.config.player_org_uncertainty_boost_global:
                global_mult *= player_mult
            if self.config.player_org_uncertainty_boost_local:
                local_mult *= player_mult
            dire_player_org_mults.append(player_mult)
            dire_global_mults.append(global_mult)
            dire_local_mults.append(local_mult)
            self.player_global[player_id] -= (
                k_global * global_mult * error
            )
            self.player_local[tier][player_id] -= (
                dire_tier_local_k * local_mult * error
            )
            if position and dire_role_local_k > 0.0:
                role_key = (player_id, position)
                self.player_role_local[tier][role_key] = self.player_role_local[tier].get(
                    role_key,
                    self.config.initial_rating,
                ) - (dire_role_local_k * local_mult * error)
                self.player_role_local_last_seen_ts[tier][role_key] = match.timestamp
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
        step.metadata["radiant_lineup_k_multiplier"] = rad_mult
        step.metadata["dire_lineup_k_multiplier"] = dire_mult
        step.metadata["radiant_player_org_k_multiplier_avg"] = _mean(rad_player_org_mults)
        step.metadata["dire_player_org_k_multiplier_avg"] = _mean(dire_player_org_mults)
        step.metadata["radiant_effective_global_k_multiplier_avg"] = _mean(rad_global_mults)
        step.metadata["dire_effective_global_k_multiplier_avg"] = _mean(dire_global_mults)
        step.metadata["radiant_effective_local_k_multiplier_avg"] = _mean(rad_local_mults)
        step.metadata["dire_effective_local_k_multiplier_avg"] = _mean(dire_local_mults)
        step.metadata["side_bias"] = self.side_bias[tier]
        return step
