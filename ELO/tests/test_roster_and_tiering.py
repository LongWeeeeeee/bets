from ELO.config import HybridEloConfig
from ELO.domain import LeagueTier, MatchRecord
from ELO.models import HybridPlayerRosterEloModel
from ELO.roster import RosterLineageTracker
from ELO.team_identity import resolve_org_key
from ELO.tiering import attach_league_tiers_time_aware, classify_leagues


def _match(
    *,
    match_id: int,
    league_id: int,
    radiant_team_id: int,
    dire_team_id: int,
) -> MatchRecord:
    return MatchRecord(
        match_id=match_id,
        timestamp=match_id,
        radiant_win=True,
        radiant_team_id=radiant_team_id,
        radiant_team_name=f"team_{radiant_team_id}",
        dire_team_id=dire_team_id,
        dire_team_name=f"team_{dire_team_id}",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=league_id,
        league_name=f"league_{league_id}",
        source_league_tier="PROFESSIONAL",
        series_id=None,
        series_type="BEST_OF_THREE",
    )


def test_roster_lineage_tracker_respects_three_player_lock() -> None:
    tracker = RosterLineageTracker(min_shared_players=3)

    first = tracker.resolve("id:1", (1, 2, 3, 4, 5))
    second = tracker.resolve("id:1", (1, 2, 3, 4, 6))
    third = tracker.resolve("id:1", (1, 2, 7, 8, 9))

    assert first.roster_key == "id:1::roster:1"
    assert first.continuity is False
    assert second.roster_key == "id:1::roster:1"
    assert second.continuity is True
    assert second.overlap_count == 4
    assert third.roster_key == "id:1::roster:2"
    assert third.continuity is False
    assert third.overlap_count == 2


def test_default_roster_lineage_requires_four_shared_players() -> None:
    tracker = RosterLineageTracker()

    first = tracker.resolve("org:42", (1, 2, 3, 4, 5))
    three_shared = tracker.resolve("org:42", (1, 2, 3, 6, 7))
    four_shared = tracker.resolve("org:42", (1, 2, 3, 6, 8))

    assert first.roster_key == "org:42::roster:1"
    assert three_shared.continuity is False
    assert three_shared.roster_key == "org:42::roster:2"
    assert four_shared.continuity is True
    assert four_shared.roster_key == "org:42::roster:2"


def test_roster_lineage_tracker_keeps_segment_through_short_standin_break() -> None:
    tracker = RosterLineageTracker(min_shared_players=3)

    first = tracker.resolve("id:1", (1, 2, 3, 4, 5))
    standin = tracker.resolve("id:1", (1, 2, 3, 6, 7))
    back_to_core = tracker.resolve("id:1", (1, 2, 4, 5, 8))

    assert first.roster_key == "id:1::roster:1"
    assert standin.roster_key == "id:1::roster:1"
    assert standin.continuity is True
    assert back_to_core.roster_key == "id:1::roster:1"
    assert back_to_core.continuity is True
    assert back_to_core.overlap_count == 4


def test_classify_leagues_uses_tier_shares() -> None:
    matches = [
        _match(match_id=1, league_id=100, radiant_team_id=7119388, dire_team_id=2163),
        _match(match_id=2, league_id=100, radiant_team_id=8255888, dire_team_id=8599101),
        _match(match_id=3, league_id=100, radiant_team_id=9247354, dire_team_id=8291895),
        _match(match_id=4, league_id=200, radiant_team_id=9005019, dire_team_id=8588969),
        _match(match_id=5, league_id=200, radiant_team_id=8376696, dire_team_id=1234567),
        _match(match_id=6, league_id=200, radiant_team_id=7654321, dire_team_id=9876543),
    ]

    league_info, _ = classify_leagues(matches)

    assert league_info[100].derived_tier == LeagueTier.TIER1
    assert league_info[200].derived_tier == LeagueTier.TIER2


def test_time_aware_tiering_does_not_use_future_league_teams() -> None:
    matches = [
        _match(match_id=1, league_id=300, radiant_team_id=1234567, dire_team_id=2345678),
        _match(match_id=2, league_id=300, radiant_team_id=7119388, dire_team_id=2163),
        _match(match_id=3, league_id=300, radiant_team_id=8255888, dire_team_id=8599101),
    ]

    league_info, _ = classify_leagues(matches)
    assert league_info[300].derived_tier == LeagueTier.TIER1

    attach_league_tiers_time_aware(matches)

    assert matches[0].derived_league_tier == LeagueTier.TIER2
    assert matches[1].derived_league_tier == LeagueTier.TIER2
    assert matches[2].derived_league_tier == LeagueTier.TIER1


def test_hybrid_roster_rating_is_local_to_tier() -> None:
    model = HybridPlayerRosterEloModel(
        HybridEloConfig(
            max_roster_weight=0.30,
            roster_full_weight_matches=1,
            cold_start_org_prior_weight=0.0,
        )
    )
    player_ids_radiant = (1, 2, 3, 4, 5)
    player_ids_dire = (6, 7, 8, 9, 10)

    first_tier2 = MatchRecord(
        match_id=1,
        timestamp=1,
        radiant_win=True,
        radiant_team_id=8376696,
        radiant_team_name="One Move",
        dire_team_id=123456,
        dire_team_name="Unknown",
        radiant_player_ids=player_ids_radiant,
        dire_player_ids=player_ids_dire,
        league_id=1,
        league_name="tier2",
        source_league_tier="PROFESSIONAL",
        series_id=None,
        series_type="BEST_OF_THREE",
        derived_league_tier=LeagueTier.TIER2,
    )
    second_tier2 = MatchRecord(
        match_id=2,
        timestamp=2,
        radiant_win=True,
        radiant_team_id=8376696,
        radiant_team_name="One Move",
        dire_team_id=123456,
        dire_team_name="Unknown",
        radiant_player_ids=player_ids_radiant,
        dire_player_ids=player_ids_dire,
        league_id=1,
        league_name="tier2",
        source_league_tier="PROFESSIONAL",
        series_id=None,
        series_type="BEST_OF_THREE",
        derived_league_tier=LeagueTier.TIER2,
    )
    first_tier1 = MatchRecord(
        match_id=3,
        timestamp=3,
        radiant_win=True,
        radiant_team_id=8376696,
        radiant_team_name="One Move",
        dire_team_id=2163,
        dire_team_name="Team Liquid",
        radiant_player_ids=player_ids_radiant,
        dire_player_ids=(11, 12, 13, 14, 15),
        league_id=2,
        league_name="tier1",
        source_league_tier="PREMIUM",
        series_id=None,
        series_type="BEST_OF_THREE",
        derived_league_tier=LeagueTier.TIER1,
    )

    model.process_match(first_tier2)
    step_tier2 = model.process_match(second_tier2)
    step_tier1 = model.process_match(first_tier1)

    assert step_tier2.metadata["radiant_roster_weight"] > 0.0
    assert step_tier1.metadata["radiant_roster_weight"] == 0.0


def test_team_identity_merges_ids_from_same_set_org() -> None:
    org_key_a = resolve_org_key(36, "Natus Vincere")
    org_key_b = resolve_org_key(9828954, "NAVI")

    assert org_key_a == org_key_b


def test_hybrid_roster_continuity_survives_team_id_change_inside_same_org_set() -> None:
    model = HybridPlayerRosterEloModel(
        HybridEloConfig(
            max_roster_weight=0.30,
            roster_full_weight_matches=1,
            cold_start_org_prior_weight=0.0,
        )
    )
    common_players = (1, 2, 3, 4, 5)

    first = MatchRecord(
        match_id=1,
        timestamp=1,
        radiant_win=True,
        radiant_team_id=36,
        radiant_team_name="Natus Vincere",
        dire_team_id=2163,
        dire_team_name="Team Liquid",
        radiant_player_ids=common_players,
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=1,
        league_name="tier1",
        source_league_tier="PREMIUM",
        series_id=None,
        series_type="BEST_OF_THREE",
        derived_league_tier=LeagueTier.TIER1,
    )
    second = MatchRecord(
        match_id=2,
        timestamp=2,
        radiant_win=True,
        radiant_team_id=9828954,
        radiant_team_name="NAVI",
        dire_team_id=2163,
        dire_team_name="Team Liquid",
        radiant_player_ids=common_players,
        dire_player_ids=(11, 12, 13, 14, 15),
        league_id=1,
        league_name="tier1",
        source_league_tier="PREMIUM",
        series_id=None,
        series_type="BEST_OF_THREE",
        derived_league_tier=LeagueTier.TIER1,
    )

    model.process_match(first)
    step = model.process_match(second)

    assert step.metadata["radiant_overlap"] == 5
    assert step.metadata["radiant_roster_weight"] > 0.0
