from datetime import datetime, timezone

import pytest

from ELO.config import EvaluationConfig, HybridEloConfig
from ELO.domain import LeagueTier, MatchRecord
from ELO.models import HybridPlayerRosterEloModel
from ELO.series_data import build_series_bundles
from ELO.series_evaluation import probability_to_win_series, run_series_online_evaluation


def test_probability_to_win_series_for_best_of_three() -> None:
    probability = probability_to_win_series(0.6, 3)
    assert round(probability, 6) == round(0.6 * 0.6 + 2 * 0.6 * 0.6 * 0.4, 6)


def test_build_series_bundles_uses_first_map_as_team_order_and_filters_incomplete() -> None:
    first = MatchRecord(
        match_id=1,
        timestamp=1,
        radiant_win=True,
        radiant_team_id=1,
        radiant_team_name="A",
        dire_team_id=2,
        dire_team_name="B",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=1,
        league_name="L",
        source_league_tier="PREMIUM",
        series_id=11,
        series_type="BEST_OF_THREE",
        derived_league_tier=LeagueTier.TIER1,
    )
    second = MatchRecord(
        match_id=2,
        timestamp=2,
        radiant_win=False,
        radiant_team_id=2,
        radiant_team_name="B",
        dire_team_id=1,
        dire_team_name="A",
        radiant_player_ids=(6, 7, 8, 9, 10),
        dire_player_ids=(1, 2, 3, 4, 5),
        league_id=1,
        league_name="L",
        source_league_tier="PREMIUM",
        series_id=11,
        series_type="BEST_OF_THREE",
        derived_league_tier=LeagueTier.TIER1,
    )
    incomplete = MatchRecord(
        match_id=3,
        timestamp=3,
        radiant_win=True,
        radiant_team_id=3,
        radiant_team_name="C",
        dire_team_id=4,
        dire_team_name="D",
        radiant_player_ids=(11, 12, 13, 14, 15),
        dire_player_ids=(16, 17, 18, 19, 20),
        league_id=1,
        league_name="L",
        source_league_tier="PREMIUM",
        series_id=22,
        series_type="BEST_OF_THREE",
        derived_league_tier=LeagueTier.TIER1,
    )

    bundles, summary = build_series_bundles([first, second, incomplete])

    eligible = [bundle for bundle in bundles if bundle.series.eligible_for_winner_target]
    assert len(eligible) == 1
    assert eligible[0].series.team_a_name == "A"
    assert eligible[0].series.team_b_name == "B"
    assert eligible[0].series.team_a_won is True
    assert summary["skipped_no_decisive_winner"] == 1


def test_run_series_online_evaluation_predicts_before_processing_maps() -> None:
    zero_k = {
        LeagueTier.TIER1: 0.0,
        LeagueTier.TIER2: 0.0,
        LeagueTier.TIER3: 0.0,
    }
    model = HybridPlayerRosterEloModel(
        HybridEloConfig(
            side_bias_k=0.0,
            k_global_by_tier=zero_k,
            k_local_by_tier=zero_k,
            k_roster_by_tier=zero_k,
        )
    )
    match = MatchRecord(
        match_id=1,
        timestamp=1,
        radiant_win=True,
        radiant_team_id=1,
        radiant_team_name="A",
        dire_team_id=2,
        dire_team_name="B",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=1,
        league_name="L",
        source_league_tier="PREMIUM",
        series_id=101,
        series_type="BEST_OF_ONE",
        derived_league_tier=LeagueTier.TIER1,
    )
    bundles, _ = build_series_bundles([match])
    report = run_series_online_evaluation(
        model=model,
        series_bundles=bundles,
        config=EvaluationConfig(evaluation_fraction=1.0, min_train_series=0),
    )

    assert report["matches"] == 1
    assert report["sample_predictions"][0]["p_radiant"] == 0.5


def test_bo3_sweep_bonus_improves_next_series_prediction_for_sweep_winner() -> None:
    zero_k = {
        LeagueTier.TIER1: 0.0,
        LeagueTier.TIER2: 0.0,
        LeagueTier.TIER3: 0.0,
    }
    global_k = {
        LeagueTier.TIER1: 40.0,
        LeagueTier.TIER2: 40.0,
        LeagueTier.TIER3: 40.0,
    }

    def _build_model(*, sweep_bonus_weight: float) -> HybridPlayerRosterEloModel:
        return HybridPlayerRosterEloModel(
            HybridEloConfig(
                player_global_weight=1.0,
                player_tier_weight=0.0,
                max_roster_weight=0.0,
                side_bias_k=0.0,
                k_global_by_tier=global_k,
                k_local_by_tier=zero_k,
                k_roster_by_tier=zero_k,
                bo3_sweep_bonus_weight=sweep_bonus_weight,
                bo3_sweep_bonus_error_basis="series",
            )
        )

    first_map = MatchRecord(
        match_id=1,
        timestamp=1,
        radiant_win=True,
        radiant_team_id=1,
        radiant_team_name="A",
        dire_team_id=2,
        dire_team_name="B",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=1,
        league_name="L",
        source_league_tier="PREMIUM",
        series_id=11,
        series_type="BEST_OF_THREE",
        derived_league_tier=LeagueTier.TIER1,
    )
    second_map = MatchRecord(
        match_id=2,
        timestamp=2,
        radiant_win=False,
        radiant_team_id=2,
        radiant_team_name="B",
        dire_team_id=1,
        dire_team_name="A",
        radiant_player_ids=(6, 7, 8, 9, 10),
        dire_player_ids=(1, 2, 3, 4, 5),
        league_id=1,
        league_name="L",
        source_league_tier="PREMIUM",
        series_id=11,
        series_type="BEST_OF_THREE",
        derived_league_tier=LeagueTier.TIER1,
    )
    next_series = MatchRecord(
        match_id=3,
        timestamp=3,
        radiant_win=True,
        radiant_team_id=1,
        radiant_team_name="A",
        dire_team_id=2,
        dire_team_name="B",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=1,
        league_name="L",
        source_league_tier="PREMIUM",
        series_id=12,
        series_type="BEST_OF_ONE",
        derived_league_tier=LeagueTier.TIER1,
    )
    bundles, _ = build_series_bundles([first_map, second_map, next_series])

    no_bonus_report = run_series_online_evaluation(
        model=_build_model(sweep_bonus_weight=0.0),
        series_bundles=bundles,
        config=EvaluationConfig(evaluation_fraction=1.0, min_train_series=0),
    )
    bonus_report = run_series_online_evaluation(
        model=_build_model(sweep_bonus_weight=1.0),
        series_bundles=bundles,
        config=EvaluationConfig(evaluation_fraction=1.0, min_train_series=0),
    )

    assert no_bonus_report["applied_bo3_sweep_bonus_count"] == 0
    assert bonus_report["applied_bo3_sweep_bonus_count"] == 1
    assert bonus_report["sample_predictions"][1]["p_radiant"] > no_bonus_report["sample_predictions"][1]["p_radiant"]


def test_lineup_uncertainty_boost_is_high_for_new_lineup_and_decays() -> None:
    zero_k = {
        LeagueTier.TIER1: 0.0,
        LeagueTier.TIER2: 0.0,
        LeagueTier.TIER3: 0.0,
    }
    global_k = {
        LeagueTier.TIER1: 20.0,
        LeagueTier.TIER2: 20.0,
        LeagueTier.TIER3: 20.0,
    }
    model = HybridPlayerRosterEloModel(
        HybridEloConfig(
            player_global_weight=1.0,
            player_tier_weight=0.0,
            max_roster_weight=0.0,
            side_bias_k=0.0,
            k_global_by_tier=global_k,
            k_local_by_tier=zero_k,
            k_roster_by_tier=zero_k,
            lineup_uncertainty_boost_max=1.0,
            lineup_uncertainty_boost_matches=4,
            lineup_uncertainty_boost_global=True,
            lineup_uncertainty_boost_local=False,
            lineup_uncertainty_boost_roster=False,
            lineup_uncertainty_tier1_enabled=True,
        )
    )

    def _match(match_id: int, timestamp: int, radiant_win: bool) -> MatchRecord:
        return MatchRecord(
            match_id=match_id,
            timestamp=timestamp,
            radiant_win=radiant_win,
            radiant_team_id=1,
            radiant_team_name="A",
            dire_team_id=2,
            dire_team_name="B",
            radiant_player_ids=(1, 2, 3, 4, 5),
            dire_player_ids=(6, 7, 8, 9, 10),
            league_id=1,
            league_name="L",
            source_league_tier="PREMIUM",
            series_id=100 + match_id,
            series_type="BEST_OF_ONE",
            derived_league_tier=LeagueTier.TIER1,
        )

    first_step = model.process_match(_match(1, 1, True))
    assert first_step.metadata["radiant_lineup_k_multiplier"] == 2.0
    assert model.player_global[1] == 1520.0

    model.process_match(_match(2, 2, True))
    model.process_match(_match(3, 3, True))
    model.process_match(_match(4, 4, True))
    stable_step = model.process_match(_match(5, 5, True))
    assert stable_step.metadata["radiant_lineup_k_multiplier"] == 1.0


def test_player_org_local_uncertainty_boost_is_high_for_new_stint_and_decays() -> None:
    zero_k = {
        LeagueTier.TIER1: 0.0,
        LeagueTier.TIER2: 0.0,
        LeagueTier.TIER3: 0.0,
    }
    local_k = {
        LeagueTier.TIER1: 20.0,
        LeagueTier.TIER2: 20.0,
        LeagueTier.TIER3: 20.0,
    }
    model = HybridPlayerRosterEloModel(
        HybridEloConfig(
            player_global_weight=0.0,
            player_tier_weight=1.0,
            max_roster_weight=0.0,
            side_bias_k=0.0,
            k_global_by_tier=zero_k,
            k_local_by_tier=local_k,
            k_roster_by_tier=zero_k,
            lineup_uncertainty_boost_max=0.0,
            player_org_uncertainty_boost_max=1.0,
            player_org_uncertainty_boost_matches=4,
            player_org_uncertainty_boost_global=False,
            player_org_uncertainty_boost_local=True,
            player_org_uncertainty_tier1_enabled=True,
        )
    )

    def _match(match_id: int, timestamp: int, team_id: int, team_name: str) -> MatchRecord:
        return MatchRecord(
            match_id=match_id,
            timestamp=timestamp,
            radiant_win=True,
            radiant_team_id=team_id,
            radiant_team_name=team_name,
            dire_team_id=2,
            dire_team_name="B",
            radiant_player_ids=(1, 2, 3, 4, 5),
            dire_player_ids=(6, 7, 8, 9, 10),
            league_id=1,
            league_name="L",
            source_league_tier="PREMIUM",
            series_id=200 + match_id,
            series_type="BEST_OF_ONE",
            derived_league_tier=LeagueTier.TIER1,
        )

    model.process_match(_match(1, 1, 1, "A"))
    assert model.player_local[LeagueTier.TIER1][1] == 1520.0

    model.process_match(_match(2, 2, 1, "A"))
    model.process_match(_match(3, 3, 1, "A"))
    model.process_match(_match(4, 4, 1, "A"))
    org_a = model.player_current_org[1]
    stable_multiplier = model._player_org_k_multiplier(1, org_a, LeagueTier.TIER1)
    model.process_match(_match(5, 5, 1, "A"))
    assert stable_multiplier == 1.0

    pre_switch_multiplier = model._player_org_k_multiplier(1, "C", LeagueTier.TIER1)
    model.process_match(_match(6, 6, 3, "C"))
    assert pre_switch_multiplier == 2.0


def test_player_org_local_uncertainty_state_roundtrip_preserves_stint_counts() -> None:
    zero_k = {
        LeagueTier.TIER1: 0.0,
        LeagueTier.TIER2: 0.0,
        LeagueTier.TIER3: 0.0,
    }
    local_k = {
        LeagueTier.TIER1: 20.0,
        LeagueTier.TIER2: 20.0,
        LeagueTier.TIER3: 20.0,
    }
    config = HybridEloConfig(
        player_global_weight=0.0,
        player_tier_weight=1.0,
        max_roster_weight=0.0,
        side_bias_k=0.0,
        k_global_by_tier=zero_k,
        k_local_by_tier=local_k,
        k_roster_by_tier=zero_k,
        lineup_uncertainty_boost_max=0.0,
        player_org_uncertainty_boost_max=1.0,
        player_org_uncertainty_boost_matches=4,
        player_org_uncertainty_boost_global=False,
        player_org_uncertainty_boost_local=True,
        player_org_uncertainty_tier1_enabled=True,
    )
    model = HybridPlayerRosterEloModel(config)

    def _match(match_id: int, timestamp: int) -> MatchRecord:
        return MatchRecord(
            match_id=match_id,
            timestamp=timestamp,
            radiant_win=True,
            radiant_team_id=1,
            radiant_team_name="A",
            dire_team_id=2,
            dire_team_name="B",
            radiant_player_ids=(1, 2, 3, 4, 5),
            dire_player_ids=(6, 7, 8, 9, 10),
            league_id=1,
            league_name="L",
            source_league_tier="PREMIUM",
            series_id=300 + match_id,
            series_type="BEST_OF_ONE",
            derived_league_tier=LeagueTier.TIER1,
        )

    model.process_match(_match(1, 1))
    restored = HybridPlayerRosterEloModel.from_state(model.export_state())
    assert restored._player_org_k_multiplier(1, restored.player_current_org[1], LeagueTier.TIER1) == 1.75
    restored.process_match(_match(2, 2))

    assert restored.player_current_org_matches[(1, restored.player_current_org[1])] == 2
    assert restored.player_local[LeagueTier.TIER1][1] == pytest.approx(1535.4940928183198)


def test_patch_local_reset_exact_tier1_only_resets_local_before_new_patch() -> None:
    zero_k = {
        LeagueTier.TIER1: 0.0,
        LeagueTier.TIER2: 0.0,
        LeagueTier.TIER3: 0.0,
    }
    local_k = {
        LeagueTier.TIER1: 20.0,
        LeagueTier.TIER2: 20.0,
        LeagueTier.TIER3: 20.0,
    }
    model = HybridPlayerRosterEloModel(
        HybridEloConfig(
            player_global_weight=0.0,
            player_tier_weight=1.0,
            max_roster_weight=0.0,
            side_bias_k=0.0,
            k_global_by_tier=zero_k,
            k_local_by_tier=local_k,
            k_roster_by_tier=zero_k,
            lineup_uncertainty_boost_max=0.0,
            player_org_uncertainty_boost_max=0.0,
            patch_local_reset_mode="exact",
            patch_local_reset_player_local_keep=0.0,
            patch_local_reset_roster_keep=1.0,
            patch_local_reset_tier1_only=True,
        )
    )

    def _ts(date_str: str) -> int:
        return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    def _match(match_id: int, timestamp: int) -> MatchRecord:
        return MatchRecord(
            match_id=match_id,
            timestamp=timestamp,
            radiant_win=True,
            radiant_team_id=1,
            radiant_team_name="A",
            dire_team_id=2,
            dire_team_name="B",
            radiant_player_ids=(1, 2, 3, 4, 5),
            dire_player_ids=(6, 7, 8, 9, 10),
            league_id=1,
            league_name="L",
            source_league_tier="PREMIUM",
            series_id=400 + match_id,
            series_type="BEST_OF_ONE",
            derived_league_tier=LeagueTier.TIER1,
        )

    first_step = model.process_match(_match(1, _ts("2025-05-28")))
    assert first_step.p_radiant == 0.5
    assert model.player_local[LeagueTier.TIER1][1] == 1510.0
    model.player_local[LeagueTier.TIER2][1] = 1600.0

    second_step = model.process_match(_match(2, _ts("2025-05-29")))
    assert second_step.p_radiant == 0.5
    assert model.player_local[LeagueTier.TIER1][1] == 1510.0
    assert model.player_local[LeagueTier.TIER2][1] == 1600.0
    assert model.current_patch_key == "7.39b"


def test_patch_local_reset_state_roundtrip_preserves_current_patch_key() -> None:
    zero_k = {
        LeagueTier.TIER1: 0.0,
        LeagueTier.TIER2: 0.0,
        LeagueTier.TIER3: 0.0,
    }
    local_k = {
        LeagueTier.TIER1: 20.0,
        LeagueTier.TIER2: 20.0,
        LeagueTier.TIER3: 20.0,
    }
    config = HybridEloConfig(
        player_global_weight=0.0,
        player_tier_weight=1.0,
        max_roster_weight=0.0,
        side_bias_k=0.0,
        k_global_by_tier=zero_k,
        k_local_by_tier=local_k,
        k_roster_by_tier=zero_k,
        lineup_uncertainty_boost_max=0.0,
        player_org_uncertainty_boost_max=0.0,
        patch_local_reset_mode="exact",
        patch_local_reset_player_local_keep=0.0,
        patch_local_reset_roster_keep=1.0,
        patch_local_reset_tier1_only=True,
    )
    model = HybridPlayerRosterEloModel(config)

    def _ts(date_str: str) -> int:
        return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    def _match(match_id: int, timestamp: int) -> MatchRecord:
        return MatchRecord(
            match_id=match_id,
            timestamp=timestamp,
            radiant_win=True,
            radiant_team_id=1,
            radiant_team_name="A",
            dire_team_id=2,
            dire_team_name="B",
            radiant_player_ids=(1, 2, 3, 4, 5),
            dire_player_ids=(6, 7, 8, 9, 10),
            league_id=1,
            league_name="L",
            source_league_tier="PREMIUM",
            series_id=500 + match_id,
            series_type="BEST_OF_ONE",
            derived_league_tier=LeagueTier.TIER1,
        )

    model.process_match(_match(1, _ts("2025-05-28")))
    restored = HybridPlayerRosterEloModel.from_state(model.export_state())
    assert restored.current_patch_key == "7.39"

    second_step = restored.process_match(_match(2, _ts("2025-05-29")))
    assert second_step.p_radiant == 0.5
    assert restored.current_patch_key == "7.39b"
    assert restored.player_local[LeagueTier.TIER1][1] == 1510.0


def test_inactivity_penalty_shrinks_tier1_local_and_roster_after_gap() -> None:
    model = HybridPlayerRosterEloModel(
        HybridEloConfig(
            inactivity_penalty_gap_days=60,
            inactivity_penalty_keep=0.5,
            inactivity_penalty_local=True,
            inactivity_penalty_roster=True,
            inactivity_penalty_global=False,
            inactivity_penalty_tier1_only=True,
        )
    )

    old_ts = int(datetime.strptime("2025-01-01", "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    new_ts = int(datetime.strptime("2025-03-15", "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    model.player_local[LeagueTier.TIER1][1] = 1520.0
    model.player_local_last_seen_ts[LeagueTier.TIER1][1] = old_ts
    model.roster_ratings[LeagueTier.TIER1]["org:test::roster:1"] = 1600.0
    model.roster_last_seen_ts[LeagueTier.TIER1]["org:test::roster:1"] = old_ts

    local_rating = model._get_player_local_rating(1, LeagueTier.TIER1, new_ts, mutate=False)
    roster_rating = model._get_roster_rating(
        "org:test::roster:1",
        LeagueTier.TIER1,
        new_ts,
        target_strength=1500.0,
        mutate=False,
    )

    assert local_rating == pytest.approx(1510.0)
    assert roster_rating == pytest.approx(1550.0)
