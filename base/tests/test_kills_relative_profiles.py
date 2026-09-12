import numpy as np

from base.kills_relative_profiles import METRIC_NAMES, aggregate_profiles, parse_match, relative_metrics, relative_metrics_batch


def _match(match_id=1, start=100, duration=1000):
    players = []
    for radiant in (True, False):
        for position in range(1, 6):
            n = (position if radiant else position + 5)
            players.append({
                "isRadiant": radiant, "position": f"POSITION_{position}", "heroId": 1000 + n,
                "accountId": 10000 + n, "kills": n, "deaths": 1, "assists": 2,
                "networth": 100 * n, "experiencePerMinute": 200 * n, "goldPerMinute": 300 + n,
            })
    return {"id": match_id, "startDateTime": start, "endDateTime": start + duration,
            "players": players, "radiantKills": [0, 1], "direKills": [0, 2]}


def test_shuffled_slots_and_sparse_hero_ids_are_canonical():
    raw = _match()
    raw["players"] = list(reversed(raw["players"]))
    raw["players"][0]["heroId"] = 10000
    record = parse_match(raw)
    assert record["heroes"] == (1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 10000)
    assert relative_metrics(record).shape == (10, len(METRIC_NAMES))


def test_missing_stat_is_nan_but_real_zero_is_zero():
    raw = _match()
    raw["players"][0].pop("assists")
    raw["players"][1]["assists"] = 0
    record = parse_match(raw)
    metrics = relative_metrics(record)
    assert np.isnan(record["stats"]["assists"][0])
    assert np.isnan(metrics[0, 2])
    assert record["stats"]["assists"][1] == 0
    # The side denominator is unknown when one teammate's value is missing.
    assert np.isnan(metrics[1, 2])
    complete = _match()
    for player in complete["players"]:
        player["assists"] = 0
    assert np.isnan(relative_metrics(parse_match(complete))[:, 2]).all()


def test_team_deaths_are_teammate_deaths_and_not_enemy_kills():
    raw = _match()
    for player in raw["players"][:5]:
        player["deaths"] = 10
    for player in raw["players"][5:]:
        player["deaths"] = 1
    record = parse_match(raw)
    metrics = relative_metrics(record)
    # radiant team death share is 1/2; it is not derived from dire's kills.
    assert np.allclose(metrics[:5, 1], 0.2)
    assert np.allclose(metrics[:5, 10], 50 / ((50 + 5) / 2))


def test_cutoff_is_strict_and_duplicate_matches_are_skipped():
    first = parse_match(_match(match_id=7, start=1_000, duration=100))
    at_cutoff = parse_match(_match(match_id=8, start=2_000, duration=100))
    result = aggregate_profiles([first, at_cutoff, first], before_timestamp=2_100)
    assert result[1001]["count"] == 1
    result = aggregate_profiles([first, at_cutoff], before_timestamp=2_200)
    assert result[1001]["count"] == 2
    assert aggregate_profiles([first], before_timestamp=1_100) == {}


def test_invalid_positions_and_nonpositive_duration_are_rejected():
    raw = _match(duration=0)
    assert parse_match(raw) is None
    raw = _match()
    raw["players"][0]["position"] = "POSITION_6"
    assert parse_match(raw) is None


def test_relative_stats_do_not_import_public_kill_scale_and_batch_matches_scalar():
    first, scaled = _match(), _match(2)
    for player in scaled["players"]:
        for key in ("kills", "deaths", "assists", "networth", "experiencePerMinute", "goldPerMinute"):
            player[key] *= 3
    a, b = parse_match(first), parse_match(scaled)
    np.testing.assert_allclose(relative_metrics(a), relative_metrics(b))
    batch = np.stack([np.stack(list(row["stats"].values()), axis=1) for row in (a, b)])
    np.testing.assert_allclose(relative_metrics_batch(batch, np.array([a["duration"], b["duration"]])),
                               np.stack([relative_metrics(a), relative_metrics(b)]))


def test_nested_identities_and_invalid_time_or_identity():
    raw = _match()
    raw["radiantTeam"] = {"id": 8}
    raw["players"][0]["steamAccount"] = {"id": 1234}
    parsed = parse_match(raw)
    assert parsed["team_ids"]["radiant"] == 8
    assert parsed["account_ids"][0] == 1234
    raw["durationSeconds"] = 1
    assert parse_match(raw) is None
    raw = _match()
    raw["players"][0]["heroId"] = 1.9
    assert parse_match(raw) is None
    raw = _match()
    raw["id"] = None
    assert parse_match(raw) is None
