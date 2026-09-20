from __future__ import annotations

import math
import json

import numpy as np
import pytest

from base.tools.prematch_causal_dataset import (
    BASE_FEATURE_NAMES, FEATURE_NAMES, ROLE_FEATURE_NAMES, build_dataset, build_from_arrays, export_elo_events,
)


def _rows(starts, durations, wins, *, pstats=None):
    n = len(starts)
    accounts = np.array([[row * 100 + slot + 1 for slot in range(10)] for row in range(n)], dtype=np.int64)
    heroes = np.array([[row * 20 + slot + 1 for slot in range(10)] for row in range(n)], dtype=np.int32)
    if pstats is None:
        pstats = np.zeros((n, 10, 14), dtype=np.float32)
        pstats[:, :, 0] = 2; pstats[:, :, 1] = 1; pstats[:, :, 2] = 3
        pstats[:, :, 3] = 400; pstats[:, :, 11] = 20
    return {"mids": np.arange(1, n + 1, dtype=np.int64), "ts": np.asarray(starts, dtype=np.int64),
            "durations": np.asarray(durations, dtype=np.int32), "wins": np.asarray(wins, dtype=np.int8),
            "accounts": accounts, "heroes": heroes, "teams": np.arange(1, 2 * n + 1, dtype=np.int64).reshape(n, 2),
            "sids": np.arange(100, 100 + n, dtype=np.int64), "leagues": np.zeros(n, dtype=np.int64),
            "pstats": pstats,
            "pstat_names": np.array(["kills", "deaths", "assists", "goldPerMinute", "experiencePerMinute", "networth", "numLastHits", "numDenies", "heroDamage", "towerDamage", "heroHealing", "imp", "level", "dotaPlusHeroXp"])}


def _row(data, mid):
    return int(np.flatnonzero(data["mid"] == mid)[0])


def test_end_equal_start_is_not_visible_but_next_second_is():
    rows = _rows([10, 100, 101], [90, 10, 10], [1, 0, 0])
    rows["accounts"][2] = rows["accounts"][0]
    data, _ = build_from_arrays(rows)
    games = FEATURE_NAMES.index("player_games_mean")
    assert data["X"][_row(data, 2), games] == 0.0  # map 1 ends exactly at map 2 start
    assert data["X"][_row(data, 3), games] == 1.0


def test_overlapping_result_is_unavailable_until_its_end():
    # Map 1 starts first but ends last.  Map 2 is the only completed result at map 3's query.
    rows = _rows([10, 100, 160], [190, 50, 10], [1, 0, 1])
    rows["heroes"][2] = rows["heroes"][1]
    data, _ = build_from_arrays(rows)
    games = FEATURE_NAMES.index("player_games_mean")
    assert data["X"][_row(data, 2), games] == 0.0
    assert data["X"][_row(data, 3), games] == 0.0  # accounts are distinct, so its players have no history
    # Hero aggregate coverage proves map 2, but not overlapping map 1, was visible.
    coverage = FEATURE_NAMES.index("hero_games_coverage")
    assert data["X"][_row(data, 3), coverage] == 10.0


def test_future_result_and_stats_cannot_change_earlier_features():
    rows = _rows([10, 110, 210], [50, 50, 10], [1, 0, 1])
    first, _ = build_from_arrays(rows)
    changed = {name: value.copy() for name, value in rows.items()}
    changed["wins"][1] = 1
    changed["pstats"][1, :, 3] = 99999
    second, _ = build_from_arrays(changed)
    assert np.array_equal(first["X"][_row(first, 2)], second["X"][_row(second, 2)])


def test_k24_matches_sequential_five_account_math():
    rows = _rows([10, 100, 200], [10, 10, 10], [1, 0, 1])
    # Reuse the first roster in every map so the third query sees two sequential updates.
    rows["accounts"][:] = rows["accounts"][0]
    data, _ = build_from_arrays(rows)
    player_delta1 = 12.0
    expected_probability = 1.0 / (1.0 + 10.0 ** (-24.0 / 400.0))
    delta2 = 24.0 * (0.0 - expected_probability)
    expected_diff = 2.0 * (player_delta1 + delta2)
    assert data["k24_diff"][_row(data, 3)] == pytest.approx(expected_diff)
    assert data["X"][_row(data, 3), FEATURE_NAMES.index("k24_prob")] == pytest.approx(
        1.0 / (1.0 + math.pow(10.0, -expected_diff / 400.0)))


def test_external_elo_events_define_k24_population_and_eligibility():
    rows = _rows([10, 100], [10, 10], [0, 0])
    event_accounts = rows["accounts"][1].copy()
    events = {"mid": np.array([999]), "ts": np.array([1]), "end": np.array([50]),
              "accounts": event_accounts[None, :], "y": np.array([1], dtype=np.int8)}
    data, metadata = build_from_arrays(rows, elo_events=events)
    assert data["k24_diff"][_row(data, 2)] == pytest.approx(24.0)
    assert data["elo_eligible"].tolist() == [False, False]
    assert data["X"][_row(data, 2), FEATURE_NAMES.index("k24_rating_coverage")] == 10.0
    assert data["X"][_row(data, 2), FEATURE_NAMES.index("k24_games_mean")] == 1.0
    assert metadata["elo_events"] == 1 and metadata["elo_eligible_emitted"] == 0


def test_shared_elo_event_rejects_side_orientation_mismatch():
    rows = _rows([10], [10], [1])
    reversed_sides = np.r_[rows["accounts"][0, 5:], rows["accounts"][0, :5]]
    events = {"mid": np.array([1]), "ts": np.array([10]), "end": np.array([20]),
              "accounts": reversed_sides[None, :], "y": np.array([1], dtype=np.int8)}
    with pytest.raises(ValueError, match="orientation mismatch"):
        build_from_arrays(rows, elo_events=events)


def test_opponent_quality_uses_the_other_side_rating_at_each_historical_start():
    rows = _rows([10, 30, 50], [10, 10, 10], [1, 1, 0])
    rows["accounts"][:] = rows["accounts"][0]
    data, _ = build_from_arrays(rows)
    # At map 2's start, map 1 made Radiant 1512 and Dire 1488.  Radiant's
    # historical opponent mean is therefore (1500 + 1488) / 2, never 1506.
    assert data["X"][_row(data, 3), FEATURE_NAMES.index("opponent_k24_mean")] == pytest.approx(1494.0)


def test_player_hero_and_team_h2h_priors_respect_strict_end_boundary():
    rows = _rows([10, 20, 21], [10, 10, 10], [1, 0, 0])
    rows["accounts"][:] = rows["accounts"][0]
    rows["heroes"][:] = rows["heroes"][0]
    rows["teams"][:] = rows["teams"][0]
    data, _ = build_from_arrays(rows)
    ph_games = FEATURE_NAMES.index("player_hero_games_mean")
    ph_wr = FEATURE_NAMES.index("player_hero_winrate_mean")
    h2h_games = FEATURE_NAMES.index("team_h2h_games")
    h2h_wr = FEATURE_NAMES.index("team_h2h_winrate")
    assert data["X"][_row(data, 2), ph_games] == 0.0  # map 1 ends at this exact start
    assert data["X"][_row(data, 3), ph_games] == 1.0
    assert data["X"][_row(data, 3), ph_wr] == 2.0 / 3.0
    assert data["X"][_row(data, 3), h2h_games] == 1.0
    assert data["X"][_row(data, 3), h2h_wr] == 2.0 / 3.0


def test_query_slot_order_is_estimated_from_past_not_current_raw_order():
    rows = _rows([10, 30], [10, 10], [1, 0])
    rows["accounts"][1] = rows["accounts"][0]
    rows["heroes"][1] = rows["heroes"][0]
    baseline, _ = build_from_arrays(rows)
    perm = np.array([1, 0, 2, 3, 4, 6, 5, 7, 8, 9])
    swapped = {name: value.copy() for name, value in rows.items()}
    for name in ("accounts", "heroes", "pstats"):
        swapped[name][1] = swapped[name][1, perm]
    changed, _ = build_from_arrays(swapped)
    left, right = _row(baseline, 2), _row(changed, 2)
    assert np.array_equal(baseline["X"][left], changed["X"][right])
    assert np.array_equal(baseline["accounts"][left], changed["accounts"][right])
    assert np.array_equal(baseline["heroes"][left], changed["heroes"][right])


def test_role_features_append_after_the_unchanged_base_prefix():
    assert len(BASE_FEATURE_NAMES) == 71
    assert FEATURE_NAMES[:len(BASE_FEATURE_NAMES)] == BASE_FEATURE_NAMES
    assert FEATURE_NAMES[len(BASE_FEATURE_NAMES):] == ROLE_FEATURE_NAMES
    rows = _rows([10, 30], [10, 10], [1, 0])
    rows["accounts"][1] = rows["accounts"][0]
    rows["heroes"][1] = rows["heroes"][0]
    data, _ = build_from_arrays(rows)
    row = _row(data, 2)
    assert data["X"][row, FEATURE_NAMES.index("role1_k24_rating_radiant")] == pytest.approx(1512.0)
    assert data["X"][row, FEATURE_NAMES.index("role1_k24_rating_diff")] == pytest.approx(24.0)
    assert data["X"][row, FEATURE_NAMES.index("role1_role_confidence_radiant")] == pytest.approx(1.0 / 3.0)


def test_invalid_roster_and_duration_are_neither_emitted_nor_history():
    rows = _rows([10, 30, 50], [10, 0, 10], [1, 1, 0])
    rows["accounts"][1, 1] = rows["accounts"][1, 0]
    data, metadata = build_from_arrays(rows)
    assert metadata["valid_rows"] == 2 and set(data["mid"]) == {1, 3}


def test_saved_dataset_has_training_contract_members(tmp_path):
    rows = _rows([10, 30], [10, 10], [1, 0])
    source = tmp_path / "source.npz"
    np.savez_compressed(source, **rows)
    metadata = build_dataset(source, tmp_path / "out", emit_from=20)
    with np.load(tmp_path / "out" / "dataset.npz", allow_pickle=False) as data:
        assert set(data.files) == {"X", "feature_names", "mid", "ts", "end", "y", "series", "league", "heroes", "accounts", "k24_diff", "elo_eligible", "label_side", "hero_slots", "k24_direction", "role_assignment"}
        assert data["X"].dtype == np.float32 and data["X"].shape == (1, len(FEATURE_NAMES))
        assert data["mid"].tolist() == [2]
        assert data["label_side"].item() == "radiant"
        assert data["hero_slots"].item() == "radiant_1_5,dire_1_5"
        assert data["k24_direction"].item() == "radiant_minus_dire"
        assert data["role_assignment"].item() == "past_completed_player_roles"
    assert metadata["sourcehash"] and metadata["emitted_rows"] == 1


def test_elo_export_uses_loader_validation_and_first_chronological_duplicate(tmp_path):
    def raw(mid, start, won):
        return {"id": mid, "startDateTime": start, "durationSeconds": 60, "didRadiantWin": won,
                "radiantTeam": {"id": 1, "name": "R"}, "direTeam": {"id": 2, "name": "D"},
                "players": [{"isRadiant": slot < 5, "steamAccount": {"id": slot + 1}}
                            for slot in range(10)]}
    (tmp_path / "a.json").write_text(json.dumps({"early": raw(7, 10, True), "later": raw(8, 30, False)}))
    (tmp_path / "b.json").write_text(json.dumps({"copy": raw(7, 10, False)}))
    report = export_elo_events(tmp_path, tmp_path / "events.npz")
    with np.load(tmp_path / "events.npz", allow_pickle=False) as events:
        assert events["mid"].tolist() == [7, 8]
        assert events["end"].tolist() == [70, 90]
        assert events["y"].tolist() == [1, 0]
    assert report["duplicate_records"] == 1 and report["completed_events"] == 2
