from __future__ import annotations

import json

import pytest

import ELO.live_team_strength as live_team_strength_module
from ELO.config import HybridEloConfig
from ELO.domain import LeagueTier, MatchRecord
from ELO.live_team_strength import (
    build_matchup_summary_from_snapshot,
    finalize_live_series_from_scores,
    get_matchup_summary,
    register_live_map_context,
)
from ELO.models import HybridPlayerRosterEloModel


def _reset_live_team_strength_caches() -> None:
    live_team_strength_module._SNAPSHOT_CACHE = None
    live_team_strength_module._MODEL_FROM_SNAPSHOT_CACHE["snapshot_id"] = None
    live_team_strength_module._MODEL_FROM_SNAPSHOT_CACHE["model"] = None
    live_team_strength_module._RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = None
    live_team_strength_module._RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = None
    live_team_strength_module._RUNTIME_SNAPSHOT_CACHE["snapshot"] = None
    live_team_strength_module._LIVE_PROBABILITY_POLICY_CACHE["path"] = None
    live_team_strength_module._LIVE_PROBABILITY_POLICY_CACHE["signature"] = None
    live_team_strength_module._LIVE_PROBABILITY_POLICY_CACHE["policy"] = None


def test_build_matchup_summary_from_snapshot_uses_current_strengths() -> None:
    snapshot = {
        "meta": {"reference_timestamp": 1771153251},
        "teams_by_org_key": {
            "org:lynx": {
                "team_id": 9928636,
                "team_name": "Team Lynx",
                "current_strength": 1610.0,
                "tier": "TIER2",
                "last_seen_utc": "2026-02-01T00:00:00+00:00",
            },
            "org:1win": {
                "team_id": 9255039,
                "team_name": "1win",
                "current_strength": 1690.0,
                "tier": "TIER1",
                "last_seen_utc": "2026-02-01T00:00:00+00:00",
            },
        },
    }

    summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=9928636,
        dire_team_id=9255039,
        radiant_team_name="Team Lynx",
        dire_team_name="1win",
    )

    assert summary is not None
    assert summary["radiant"]["org_key"] == "org:lynx"
    assert summary["dire"]["org_key"] == "org:1win"
    assert summary["radiant"]["rating"] == 1610.0
    assert summary["dire"]["rating"] == 1690.0
    assert summary["radiant_win_prob"] < 0.5
    assert summary["elo_diff"] == -80.0


def test_build_matchup_summary_from_snapshot_uses_baseline_for_missing_team() -> None:
    snapshot = {
        "meta": {"reference_timestamp": 1771153251},
        "teams_by_org_key": {
            "org:1win": {
                "team_id": 9255039,
                "team_name": "1win",
                "current_strength": 1690.0,
                "tier": "TIER1",
                "last_seen_utc": "2026-02-01T00:00:00+00:00",
            }
        },
    }

    summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=None,
        dire_team_id=9255039,
        radiant_team_name="Unknown Team",
        dire_team_name="1win",
    )

    assert summary is not None
    assert summary["radiant"]["matched"] is False
    assert summary["radiant"]["rating"] == 1500.0
    assert summary["dire"]["matched"] is True
    assert summary["dire"]["rating"] == 1690.0


def test_build_matchup_summary_from_snapshot_applies_cross_tier_bonus() -> None:
    snapshot = {
        "meta": {
            "reference_timestamp": 1771153251,
            "tier_matchup_elo_bonus": {
                "TIER1_vs_TIER2": {
                    "series_count": 1122,
                    "strong_winrate": 0.72,
                    "elo_bonus": 164.0,
                }
            },
        },
        "teams_by_org_key": {
            "org:lynx": {
                "team_id": 9928636,
                "team_name": "Team Lynx",
                "current_strength": 1596.0,
                "tier": "TIER2",
                "last_seen_utc": "2026-03-13T00:00:00+00:00",
            },
            "org:1win": {
                "team_id": 9255039,
                "team_name": "1win",
                "current_strength": 1511.0,
                "tier": "TIER1",
                "last_seen_utc": "2026-02-10T00:00:00+00:00",
            },
        },
    }

    summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=9928636,
        dire_team_id=9255039,
        radiant_team_name="Team Lynx",
        dire_team_name="1win",
    )

    assert summary is not None
    assert summary["tier_gap_key"] == "TIER1_vs_TIER2"
    assert summary["tier_gap_bonus"] == pytest.approx(-164.0)
    assert summary["radiant"]["base_rating"] == 1596.0
    assert summary["dire"]["base_rating"] == 1511.0
    assert summary["dire"]["rating"] > summary["radiant"]["rating"]
    assert summary["dire_win_prob"] > 0.5


def test_build_matchup_summary_from_snapshot_applies_cross_tier_bonus_with_names_only() -> None:
    snapshot = {
        "meta": {
            "reference_timestamp": 1771153251,
            "tier_matchup_elo_bonus": {
                "TIER1_vs_TIER2": {
                    "series_count": 1122,
                    "strong_winrate": 0.72,
                    "elo_bonus": 164.0,
                }
            },
        },
        "teams_by_org_key": {
            "org:lynx": {
                "team_id": 9928636,
                "team_name": "Team Lynx",
                "current_strength": 1596.0,
                "tier": "TIER2",
                "last_seen_utc": "2026-03-13T00:00:00+00:00",
            },
            "org:1win": {
                "team_id": 9255039,
                "team_name": "1win",
                "current_strength": 1511.0,
                "tier": "TIER1",
                "last_seen_utc": "2026-02-10T00:00:00+00:00",
            },
        },
    }

    summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=None,
        dire_team_id=None,
        radiant_team_name="Team Lynx",
        dire_team_name="1win",
    )

    assert summary is not None
    assert summary["tier_gap_key"] == "TIER1_vs_TIER2"
    assert summary["tier_gap_bonus"] == pytest.approx(-164.0)
    assert summary["dire"]["rating"] > summary["radiant"]["rating"]
    assert summary["dire_win_prob"] > 0.5


def test_build_matchup_summary_from_snapshot_uses_lineup_player_state_for_unseen_team() -> None:
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    for player_id in range(1, 6):
        model.player_global[player_id] = 1600.0
        model.player_local[LeagueTier.TIER2][player_id] = 1600.0
    for player_id in range(6, 11):
        model.player_global[player_id] = 1400.0
        model.player_local[LeagueTier.TIER2][player_id] = 1400.0

    snapshot = {
        "meta": {"reference_timestamp": 1771153251},
        "teams_by_org_key": {},
        "model_state": model.export_state(),
    }

    summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=None,
        dire_team_id=None,
        radiant_team_name="Astini+5",
        dire_team_name="Unknown Stack",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
        match_tier=2,
    )

    assert summary is not None
    assert summary["source"] == "elo_live_lineup_snapshot"
    assert summary["radiant"]["lineup_used"] is True
    assert summary["dire"]["lineup_used"] is True
    assert summary["radiant"]["base_rating"] == pytest.approx(1600.0)
    assert summary["dire"]["base_rating"] == pytest.approx(1400.0)
    assert summary["radiant_win_prob"] > 0.5


def test_build_matchup_summary_from_snapshot_prefers_lineup_over_snapshot_current_strength() -> None:
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    for player_id in range(1, 6):
        model.player_global[player_id] = 1600.0
        model.player_local[LeagueTier.TIER2][player_id] = 1600.0

    snapshot = {
        "meta": {"reference_timestamp": 1771153251},
        "teams_by_org_key": {
            "org:l1ga": {
                "team_id": 9303383,
                "team_name": "L1GA TEAM",
                "current_strength": 1511.0,
                "tier": "TIER2",
                "last_seen_utc": "2026-03-13T00:00:00+00:00",
            }
        },
        "model_state": model.export_state(),
    }

    summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=9303383,
        dire_team_id=None,
        radiant_team_name="L1GA TEAM",
        dire_team_name="Unknown Team",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[],
        match_tier="TIER2",
    )

    assert summary is not None
    assert summary["radiant"]["matched"] is True
    assert summary["radiant"]["lineup_used"] is True
    assert summary["radiant"]["base_rating"] == pytest.approx(1600.0)
    assert summary["radiant"]["base_rating"] > 1511.0


def test_build_matchup_summary_from_snapshot_uses_player_strength_for_cold_roster() -> None:
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    player_ids = (1, 2, 3, 4, 5)
    for player_id in player_ids:
        model.player_global[player_id] = 1600.0
        model.player_local[LeagueTier.TIER2][player_id] = 1600.0

    roster_resolution = model.roster_tracker.resolve("org:l1ga", player_ids)
    model.roster_match_counts[LeagueTier.TIER2][roster_resolution.roster_key] = 2
    model.roster_ratings[LeagueTier.TIER2][roster_resolution.roster_key] = 2200.0

    snapshot = {
        "meta": {"reference_timestamp": 1771153251},
        "teams_by_org_key": {
            "org:l1ga": {
                "team_id": 9303383,
                "team_name": "L1GA TEAM",
                "current_strength": 1511.0,
                "tier": "TIER2",
                "last_seen_utc": "2026-03-13T00:00:00+00:00",
            }
        },
        "model_state": model.export_state(),
    }

    summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=9303383,
        dire_team_id=None,
        radiant_team_name="L1GA TEAM",
        dire_team_name="Unknown Team",
        radiant_account_ids=list(player_ids),
        dire_account_ids=[],
        match_tier="TIER2",
    )

    assert summary is not None
    assert summary["radiant"]["lineup_used"] is True
    assert summary["radiant"]["player_strength"] == pytest.approx(1600.0)
    assert summary["radiant"]["team_strength"] == pytest.approx(1600.0)
    assert summary["radiant"]["roster_matches"] == 2
    assert summary["radiant"]["rating_source"] == "lineup_player_strength_cold_roster"
    assert summary["radiant"]["base_rating"] == pytest.approx(1600.0)


def test_build_matchup_summary_from_snapshot_applies_segment_probability_grid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = {
        "meta": {"reference_timestamp": 1771153251},
        "teams_by_org_key": {
            "org:parivision": {
                "team_id": 987654,
                "team_name": "PARIVISION",
                "current_strength": 1737.0,
                "tier": "TIER1",
                "last_seen_utc": "2026-03-27T10:07:01+00:00",
            },
            "org:aurora": {
                "team_id": 987655,
                "team_name": "Aurora",
                "current_strength": 1873.0,
                "tier": "TIER1",
                "last_seen_utc": "2026-03-27T10:07:01+00:00",
            },
        },
    }

    monkeypatch.setattr(
        live_team_strength_module,
        "_load_live_probability_segment_policy",
        lambda *_args, **_kwargs: {
            "tier1_only": {
                "mode": "grid",
                "variant": "blend_fav_k30",
                "edges": [120.0, 200.0],
                "bucket_probs": [0.54, 0.62, 0.78],
            }
        },
    )
    monkeypatch.setattr(
        live_team_strength_module,
        "get_known_team_tier",
        lambda _team_id, _team_name: LeagueTier.TIER1,
    )

    summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=987654,
        dire_team_id=987655,
        radiant_team_name="PARIVISION",
        dire_team_name="Aurora",
    )

    assert summary is not None
    assert summary["probability_segment"] == "tier1_only"
    assert summary["probability_policy_segment"] == "tier1_only"
    assert summary["probability_mode"] == "grid"
    assert summary["probability_variant"] == "blend_fav_k30"
    assert summary["radiant_win_prob"] == pytest.approx(0.22)
    assert summary["dire_win_prob"] == pytest.approx(0.78)
    assert summary["direct_radiant_win_prob"] == pytest.approx(
        1.0 / (1.0 + 10 ** ((1873.0 - 1737.0) / 400.0))
    )


def test_build_matchup_summary_from_snapshot_keeps_direct_for_tier1_vs_tier2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = {
        "meta": {"reference_timestamp": 1771153251},
        "teams_by_org_key": {
            "name:tierone": {
                "team_id": 1001,
                "team_name": "Tier One",
                "current_strength": 1700.0,
                "tier": "TIER1",
                "last_seen_utc": "2026-03-27T10:07:01+00:00",
            },
            "name:tiertwo": {
                "team_id": 1002,
                "team_name": "Tier Two",
                "current_strength": 1600.0,
                "tier": "TIER2",
                "last_seen_utc": "2026-03-27T10:07:01+00:00",
            },
        },
    }

    monkeypatch.setattr(
        live_team_strength_module,
        "_load_live_probability_segment_policy",
        lambda *_args, **_kwargs: {
            "tier1_vs_tier2": {
                "mode": "direct_series_prob",
            }
        },
    )

    def _fake_known_tier(team_id: int | None, _team_name: str) -> LeagueTier | None:
        if team_id == 1001:
            return LeagueTier.TIER1
        if team_id == 1002:
            return LeagueTier.TIER2
        return None

    monkeypatch.setattr(live_team_strength_module, "get_known_team_tier", _fake_known_tier)

    summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=1001,
        dire_team_id=1002,
        radiant_team_name="Tier One",
        dire_team_name="Tier Two",
    )

    assert summary is not None
    assert summary["probability_segment"] == "tier1_vs_tier2"
    assert summary["probability_mode"] == "direct_series_prob"
    assert summary["radiant_win_prob"] == pytest.approx(
        1.0 / (1.0 + 10 ** ((1600.0 - 1700.0) / 400.0))
    )


def test_register_live_map_context_applies_previous_map_once_and_updates_runtime_snapshot(tmp_path) -> None:
    _reset_live_team_strength_caches()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    snapshot_path = tmp_path / "live_snapshot.json"
    progress_path = tmp_path / "live_progress.json"
    runtime_model_state_path = tmp_path / "live_model_state.json"
    runtime_lock_path = tmp_path / "live_state.lock"

    model = HybridPlayerRosterEloModel(HybridEloConfig())
    snapshot = {
        "meta": {"reference_timestamp": 1771153251},
        "teams_by_org_key": {},
        "model_state": model.export_state(),
    }
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

    map1 = MatchRecord(
        match_id=101,
        timestamp=1771153200,
        radiant_win=False,
        radiant_team_id=1,
        radiant_team_name="Elegia",
        dire_team_id=2,
        dire_team_name="Team Mariachi",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11,
        league_name="Test League",
        source_league_tier="TIER2",
        series_id=425663,
        series_type="3",
        derived_league_tier=LeagueTier.TIER2,
    )
    result_map1 = register_live_map_context(
        series_key="425663",
        series_url="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1",
        map_key="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1.0",
        first_team_score=0,
        second_team_score=0,
        first_team_is_radiant=True,
        match_record=map1,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        progress_path=progress_path,
        runtime_model_state_path=runtime_model_state_path,
        runtime_lock_path=runtime_lock_path,
    )

    assert result_map1 is not None
    assert result_map1["applied_update"] is None
    assert runtime_model_state_path.exists() is False

    summary_before = get_matchup_summary(
        radiant_team_id=1,
        dire_team_id=2,
        radiant_team_name="Elegia",
        dire_team_name="Team Mariachi",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
        match_tier=LeagueTier.TIER2,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        runtime_model_state_path=runtime_model_state_path,
    )

    assert summary_before is not None
    assert summary_before["radiant_win_prob"] == pytest.approx(0.5)

    map2 = MatchRecord(
        match_id=102,
        timestamp=1771153800,
        radiant_win=False,
        radiant_team_id=1,
        radiant_team_name="Elegia",
        dire_team_id=2,
        dire_team_name="Team Mariachi",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11,
        league_name="Test League",
        source_league_tier="TIER2",
        series_id=425663,
        series_type="3",
        derived_league_tier=LeagueTier.TIER2,
    )
    result_map2 = register_live_map_context(
        series_key="425663",
        series_url="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1",
        map_key="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1.1",
        first_team_score=1,
        second_team_score=0,
        first_team_is_radiant=True,
        match_record=map2,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        progress_path=progress_path,
        runtime_model_state_path=runtime_model_state_path,
        runtime_lock_path=runtime_lock_path,
    )

    assert result_map2 is not None
    assert result_map2["applied_update"] is not None
    assert result_map2["applied_update"]["map_key"].endswith(".0")
    assert result_map2["applied_update"]["winner_slot"] == "first"
    assert result_map2["applied_update"]["radiant_win"] is True
    assert result_map2["applied_update"]["first_team_name"] == "Elegia"
    assert result_map2["applied_update"]["second_team_name"] == "Team Mariachi"
    assert result_map2["applied_update"]["winner_team_name"] == "Elegia"
    assert result_map2["applied_update"]["series_score_before"] == {"first": 0, "second": 0}
    assert result_map2["applied_update"]["series_score_after"] == {"first": 1, "second": 0}
    assert result_map2["applied_update"]["radiant"]["delta"] > 0.0
    assert result_map2["applied_update"]["dire"]["delta"] < 0.0
    assert "lineup_k_multiplier" in result_map2["applied_update"]["radiant"]
    assert "player_org_k_multiplier_avg" in result_map2["applied_update"]["radiant"]
    assert "effective_local_k_multiplier_avg" in result_map2["applied_update"]["radiant"]
    assert "rating_delta_sum" in result_map2["applied_update"]
    assert "base_delta_sum" in result_map2["applied_update"]
    assert result_map2["applied_update"]["radiant_win_prob_after"] > result_map2["applied_update"]["radiant_win_prob_before"]
    assert runtime_model_state_path.exists() is True

    summary_after = get_matchup_summary(
        radiant_team_id=1,
        dire_team_id=2,
        radiant_team_name="Elegia",
        dire_team_name="Team Mariachi",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
        match_tier=LeagueTier.TIER2,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        runtime_model_state_path=runtime_model_state_path,
    )

    assert summary_after is not None
    assert summary_after["radiant_win_prob"] > 0.5
    assert summary_after["radiant"]["snapshot_base_rating"] == pytest.approx(summary_before["radiant"]["base_rating"])
    assert summary_after["dire"]["snapshot_base_rating"] == pytest.approx(summary_before["dire"]["base_rating"])
    assert summary_after["radiant"]["live_base_delta"] > 0.0
    assert summary_after["dire"]["live_base_delta"] < 0.0
    assert summary_after["has_live_delta"] is True

    result_map2_repeat = register_live_map_context(
        series_key="425663",
        series_url="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1",
        map_key="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1.1",
        first_team_score=1,
        second_team_score=0,
        first_team_is_radiant=True,
        match_record=map2,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        progress_path=progress_path,
        runtime_model_state_path=runtime_model_state_path,
        runtime_lock_path=runtime_lock_path,
    )

    assert result_map2_repeat is not None
    assert result_map2_repeat["applied_update"] is None

    summary_after_repeat = get_matchup_summary(
        radiant_team_id=1,
        dire_team_id=2,
        radiant_team_name="Elegia",
        dire_team_name="Team Mariachi",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
        match_tier=LeagueTier.TIER2,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        runtime_model_state_path=runtime_model_state_path,
    )

    assert summary_after_repeat is not None
    assert summary_after_repeat["radiant_win_prob"] == pytest.approx(summary_after["radiant_win_prob"])

    progress_payload = json.loads(progress_path.read_text(encoding="utf-8"))
    assert "425663" in progress_payload["pending_series"]
    assert "dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1.0" in progress_payload["applied_maps"]

    _reset_live_team_strength_caches()


def test_finalize_live_series_from_scores_applies_pending_final_map_once(tmp_path) -> None:
    _reset_live_team_strength_caches()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    snapshot_path = tmp_path / "live_snapshot.json"
    progress_path = tmp_path / "live_progress.json"
    runtime_model_state_path = tmp_path / "live_model_state.json"
    runtime_lock_path = tmp_path / "live_state.lock"

    model = HybridPlayerRosterEloModel(HybridEloConfig())
    snapshot = {
        "meta": {"reference_timestamp": 1771153251},
        "teams_by_org_key": {},
        "model_state": model.export_state(),
    }
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

    map1 = MatchRecord(
        match_id=201,
        timestamp=1771153200,
        radiant_win=False,
        radiant_team_id=1,
        radiant_team_name="Elegia",
        dire_team_id=2,
        dire_team_name="Team Mariachi",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11,
        league_name="Test League",
        source_league_tier="TIER2",
        series_id=425663,
        series_type="3",
        derived_league_tier=LeagueTier.TIER2,
    )
    register_live_map_context(
        series_key="425663",
        series_url="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1",
        map_key="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1.0",
        first_team_score=0,
        second_team_score=0,
        first_team_is_radiant=True,
        match_record=map1,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        progress_path=progress_path,
        runtime_model_state_path=runtime_model_state_path,
        runtime_lock_path=runtime_lock_path,
    )

    map2 = MatchRecord(
        match_id=202,
        timestamp=1771153800,
        radiant_win=False,
        radiant_team_id=1,
        radiant_team_name="Elegia",
        dire_team_id=2,
        dire_team_name="Team Mariachi",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11,
        league_name="Test League",
        source_league_tier="TIER2",
        series_id=425663,
        series_type="3",
        derived_league_tier=LeagueTier.TIER2,
    )
    register_live_map_context(
        series_key="425663",
        series_url="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1",
        map_key="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1.1",
        first_team_score=1,
        second_team_score=0,
        first_team_is_radiant=True,
        match_record=map2,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        progress_path=progress_path,
        runtime_model_state_path=runtime_model_state_path,
        runtime_lock_path=runtime_lock_path,
    )

    summary_after_map1 = get_matchup_summary(
        radiant_team_id=1,
        dire_team_id=2,
        radiant_team_name="Elegia",
        dire_team_name="Team Mariachi",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
        match_tier=LeagueTier.TIER2,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        runtime_model_state_path=runtime_model_state_path,
    )
    assert summary_after_map1 is not None

    finalize_result = finalize_live_series_from_scores(
        series_key="425663",
        series_url="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1",
        first_team_score=2,
        second_team_score=0,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        progress_path=progress_path,
        runtime_model_state_path=runtime_model_state_path,
        runtime_lock_path=runtime_lock_path,
    )

    assert finalize_result is not None
    assert finalize_result["applied_update"] is not None
    assert finalize_result["applied_update"]["map_key"].endswith(".1")
    assert finalize_result["applied_update"]["winner_slot"] == "first"
    assert finalize_result["applied_update"]["radiant_win"] is True
    assert finalize_result["applied_update"]["series_score_before"] == {"first": 1, "second": 0}
    assert finalize_result["applied_update"]["series_score_after"] == {"first": 2, "second": 0}
    assert finalize_result["applied_update"]["winner_team_name"] == "Elegia"
    assert finalize_result["applied_update"]["radiant"]["delta"] > 0.0
    assert finalize_result["applied_update"]["dire"]["delta"] < 0.0
    assert "lineup_k_multiplier" in finalize_result["applied_update"]["dire"]
    assert "player_org_k_multiplier_avg" in finalize_result["applied_update"]["dire"]

    summary_after_map2 = get_matchup_summary(
        radiant_team_id=1,
        dire_team_id=2,
        radiant_team_name="Elegia",
        dire_team_name="Team Mariachi",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
        match_tier=LeagueTier.TIER2,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        runtime_model_state_path=runtime_model_state_path,
    )
    assert summary_after_map2 is not None
    assert summary_after_map2["radiant_win_prob"] >= summary_after_map1["radiant_win_prob"]

    finalize_repeat = finalize_live_series_from_scores(
        series_key="425663",
        series_url="dltv.org/matches/425663/elegia-vs-team-mariachi-1win-streamers-league-1",
        first_team_score=2,
        second_team_score=0,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        progress_path=progress_path,
        runtime_model_state_path=runtime_model_state_path,
        runtime_lock_path=runtime_lock_path,
    )
    assert finalize_repeat is not None
    assert finalize_repeat["applied_update"] is None

    progress_payload = json.loads(progress_path.read_text(encoding="utf-8"))
    assert "425663" not in progress_payload["pending_series"]

    _reset_live_team_strength_caches()


def test_live_runtime_applies_roster_change_and_uncertainty_boosts(tmp_path) -> None:
    _reset_live_team_strength_caches()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    snapshot_path = tmp_path / "live_snapshot.json"
    progress_path = tmp_path / "live_progress.json"
    runtime_model_state_path = tmp_path / "live_model_state.json"
    runtime_lock_path = tmp_path / "live_state.lock"

    config = HybridEloConfig(
        lineup_uncertainty_boost_max=1.0,
        lineup_uncertainty_boost_matches=4,
        lineup_uncertainty_boost_global=True,
        lineup_uncertainty_boost_local=True,
        lineup_uncertainty_boost_roster=True,
        lineup_uncertainty_tier1_enabled=True,
        player_org_uncertainty_boost_max=1.0,
        player_org_uncertainty_boost_matches=4,
        player_org_uncertainty_boost_global=False,
        player_org_uncertainty_boost_local=True,
        player_org_uncertainty_tier1_enabled=True,
    )
    model = HybridPlayerRosterEloModel(config)

    seed_match = MatchRecord(
        match_id=301,
        timestamp=1771152600,
        radiant_win=True,
        radiant_team_id=10,
        radiant_team_name="Old Org",
        dire_team_id=20,
        dire_team_name="Stable Opponent",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11,
        league_name="Test League",
        source_league_tier="TIER1",
        series_id=500001,
        series_type="3",
        derived_league_tier=LeagueTier.TIER1,
    )
    model.process_match(seed_match)

    snapshot = {
        "meta": {"reference_timestamp": 1771153251},
        "teams_by_org_key": {},
        "model_state": model.export_state(),
    }
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

    preview_before = get_matchup_summary(
        radiant_team_id=11,
        dire_team_id=20,
        radiant_team_name="New Org",
        dire_team_name="Stable Opponent",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
        match_tier=LeagueTier.TIER1,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        runtime_model_state_path=runtime_model_state_path,
    )

    assert preview_before is not None
    assert preview_before["source"] == "elo_live_lineup_snapshot"
    assert preview_before["radiant"]["lineup_used"] is True
    assert preview_before["radiant"]["rating_source"] == "lineup_player_strength_cold_roster"
    assert preview_before["radiant"]["roster_matches"] == 0

    live_map1 = MatchRecord(
        match_id=302,
        timestamp=1771153800,
        radiant_win=True,
        radiant_team_id=11,
        radiant_team_name="New Org",
        dire_team_id=20,
        dire_team_name="Stable Opponent",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11,
        league_name="Test League",
        source_league_tier="TIER1",
        series_id=500002,
        series_type="3",
        derived_league_tier=LeagueTier.TIER1,
    )
    register_live_map_context(
        series_key="500002",
        series_url="dltv.org/matches/500002/new-org-vs-stable-opponent.0",
        map_key="dltv.org/matches/500002/new-org-vs-stable-opponent.0",
        first_team_score=0,
        second_team_score=0,
        first_team_is_radiant=True,
        match_record=live_map1,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        progress_path=progress_path,
        runtime_model_state_path=runtime_model_state_path,
        runtime_lock_path=runtime_lock_path,
    )

    live_map2 = MatchRecord(
        match_id=303,
        timestamp=1771154400,
        radiant_win=True,
        radiant_team_id=11,
        radiant_team_name="New Org",
        dire_team_id=20,
        dire_team_name="Stable Opponent",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11,
        league_name="Test League",
        source_league_tier="TIER1",
        series_id=500002,
        series_type="3",
        derived_league_tier=LeagueTier.TIER1,
    )
    result_map2 = register_live_map_context(
        series_key="500002",
        series_url="dltv.org/matches/500002/new-org-vs-stable-opponent.1",
        map_key="dltv.org/matches/500002/new-org-vs-stable-opponent.1",
        first_team_score=1,
        second_team_score=0,
        first_team_is_radiant=True,
        match_record=live_map2,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        progress_path=progress_path,
        runtime_model_state_path=runtime_model_state_path,
        runtime_lock_path=runtime_lock_path,
    )

    assert result_map2 is not None
    applied_update = result_map2["applied_update"]
    assert applied_update is not None
    assert applied_update["radiant"]["lineup_k_multiplier"] > 1.0
    assert applied_update["radiant"]["player_org_k_multiplier_avg"] > 1.0
    assert applied_update["radiant"]["effective_local_k_multiplier_avg"] > 1.0
    assert applied_update["radiant"]["after_roster_matches"] == 1

    preview_after = get_matchup_summary(
        radiant_team_id=11,
        dire_team_id=20,
        radiant_team_name="New Org",
        dire_team_name="Stable Opponent",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
        match_tier=LeagueTier.TIER1,
        snapshot_path=snapshot_path,
        data_dir=data_dir,
        rebuild_if_missing=False,
        runtime_model_state_path=runtime_model_state_path,
    )

    assert preview_after is not None
    assert preview_after["radiant"]["lineup_used"] is True
    assert preview_after["radiant"]["roster_key"] == preview_before["radiant"]["roster_key"]
    assert preview_after["radiant"]["roster_matches"] == 1
    assert preview_after["radiant"]["live_base_delta"] != pytest.approx(0.0)

    _reset_live_team_strength_caches()
