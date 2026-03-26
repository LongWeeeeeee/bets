from __future__ import annotations

from dataclasses import dataclass, field

from ELO.domain import LeagueTier


@dataclass(frozen=True)
class EvaluationConfig:
    evaluation_fraction: float = 0.30
    min_train_matches: int = 2000
    min_train_series: int = 1000
    calibration_buckets: int = 10


@dataclass(frozen=True)
class SimpleTeamEloConfig:
    initial_rating: float = 1500.0
    elo_scale: float = 400.0
    base_k: float = 28.0
    side_bias_k: float = 3.0
    team_decay_half_life_days: float = 0.0
    team_prior_bonus_by_tier: dict[LeagueTier, float] = field(
        default_factory=lambda: {
            LeagueTier.TIER1: 0.0,
            LeagueTier.TIER2: 0.0,
            LeagueTier.TIER3: 0.0,
        }
    )
    tier_k_multiplier: dict[LeagueTier, float] = field(
        default_factory=lambda: {
            LeagueTier.TIER1: 1.00,
            LeagueTier.TIER2: 0.72,
            LeagueTier.TIER3: 0.50,
        }
    )


@dataclass(frozen=True)
class HybridEloConfig:
    initial_rating: float = 1500.0
    elo_scale: float = 400.0
    bo3_sweep_bonus_weight: float = 0.05
    bo3_sweep_bonus_error_basis: str = "series"
    player_global_weight: float = 0.68
    player_tier_weight: float = 0.32
    player_role_weight: float = 0.12
    player_role_tier1_only: bool = True
    max_roster_weight: float = 0.00
    roster_full_weight_matches: int = 16
    lineup_uncertainty_boost_max: float = 1.0
    lineup_uncertainty_boost_matches: int = 4
    lineup_uncertainty_boost_global: bool = True
    lineup_uncertainty_boost_local: bool = True
    lineup_uncertainty_boost_roster: bool = True
    lineup_uncertainty_tier1_enabled: bool = False
    player_org_uncertainty_boost_max: float = 1.0
    player_org_uncertainty_boost_matches: int = 15
    player_org_uncertainty_boost_global: bool = False
    player_org_uncertainty_boost_local: bool = True
    player_org_uncertainty_tier1_enabled: bool = True
    patch_local_reset_mode: str = "exact"
    patch_local_reset_player_local_keep: float = 0.0
    patch_local_reset_roster_keep: float = 1.0
    patch_local_reset_tier1_only: bool = True
    inactivity_penalty_gap_days: int = 60
    inactivity_penalty_keep: float = 1.0
    inactivity_penalty_local: bool = False
    inactivity_penalty_roster: bool = False
    inactivity_penalty_global: bool = False
    inactivity_penalty_tier1_only: bool = True
    side_bias_k: float = 3.0
    player_global_decay_half_life_days: float = 0.0
    player_local_decay_half_life_days: float = 0.0
    roster_decay_half_life_days: float = 0.0
    org_prior_rating_by_tier: dict[LeagueTier, float] = field(
        default_factory=lambda: {
            LeagueTier.TIER1: 1540.0,
            LeagueTier.TIER2: 1515.0,
            LeagueTier.TIER3: 1500.0,
        }
    )
    cold_start_org_prior_weight: float = 0.0
    org_prior_fade_matches: int = 6
    k_global_by_tier: dict[LeagueTier, float] = field(
        default_factory=lambda: {
            LeagueTier.TIER1: 28.8,
            LeagueTier.TIER2: 12.0,
            LeagueTier.TIER3: 7.2,
        }
    )
    k_local_by_tier: dict[LeagueTier, float] = field(
        default_factory=lambda: {
            LeagueTier.TIER1: 18.4,
            LeagueTier.TIER2: 25.3,
            LeagueTier.TIER3: 20.7,
        }
    )
    k_roster_by_tier: dict[LeagueTier, float] = field(
        default_factory=lambda: {
            LeagueTier.TIER1: 18.0,
            LeagueTier.TIER2: 8.0,
            LeagueTier.TIER3: 5.0,
        }
    )
