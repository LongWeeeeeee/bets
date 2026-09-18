import copy
import json
import math
from pathlib import Path

import pytest

from base.player_metadata import DAY, PlayerHistory, parse_dltv_match, timestamp
from base.tools.player_metadata import export, write_new, position_accounts, load_snapshot


FIXTURE = Path(__file__).parent / "fixtures" / "player_metadata_dltv.json"


def sample():
    data = json.loads(FIXTURE.read_text())
    return parse_dltv_match("<script>series_item = " + json.dumps(data["series_item"]) + ";</script>",
                            source_url=data["source_url"], observed_at=data["observed_at"])


def lineups(snapshot):
    teams = sorted({p["dltv_team_id"] for p in snapshot["players"]})
    return [[p["account_id"] for p in sorted(snapshot["players"], key=lambda p: p["position"])
             if p["dltv_team_id"] == team] for team in teams]


def test_real_dltv_example_uses_account_id_earnings_and_unknown_region():
    snapshot = sample()
    row = next(p for p in snapshot["players"] if p["slug"] == "yowaai")
    assert row["account_id"] == 234119750
    assert row["dltv_player_id"] == 42292
    assert row["rank"] == 1006
    assert row["earnings_usd"] == 20100
    assert row["position"] == 1
    assert row["rank_region"] is None
    assert row["observed_at"] - row["rank_updated_at"] > 7 * DAY
    assert len(snapshot["players"]) == 10


def test_current_snapshot_does_not_backfill_historical_or_equal_time_match():
    snapshot = sample()
    history = PlayerHistory([snapshot])
    for cutoff in (snapshot["observed_at"] - DAY, snapshot["observed_at"]):
        result = history.features(*lineups(snapshot), asof=cutoff)
        assert not any(result["features"].values())
        assert all(p["rank_status"] == "no_prior_observation" for side in result["diagnostics"] for p in side)


def test_rank_unknown_stale_and_missing_are_not_zero_rank():
    snapshot = sample()
    now = snapshot["observed_at"]
    row = snapshot["players"][0]
    row.update(rank=100, rank_updated_at=now, rank_region=None)
    result = PlayerHistory([snapshot]).features(*lineups(snapshot), asof=now + 1)
    assert any(p["rank_status"] == "rank_region_unknown" for side in result["diagnostics"] for p in side)
    assert all(value == 0 for key, value in result["features"].items() if "_rank_" in key)


def test_verified_regional_rank_keeps_regions_separate_and_expires():
    snapshot = sample()
    now = snapshot["observed_at"]
    for p in snapshot["players"]:
        p.update(rank=10, rank_updated_at=now, rank_region="europe", rank_region_source={
            "url": "https://example.test/verified-account", "sha256": "0" * 64,
            "account_id": p["account_id"], "identity_method": "account_id", "observed_at": now,
            "region": "europe", "rank": 10, "rank_updated_at": now})
    radiant, dire = lineups(snapshot)
    player = next(p for p in snapshot["players"] if p["account_id"] == radiant[0])
    player["rank_region"] = "se_asia"
    player["rank_region_source"]["region"] = "se_asia"
    history = PlayerHistory([snapshot])
    result = history.features(radiant, dire, asof=now + 1)
    assert result["radiant"]["pos1_rank_se_asia_strength"] == pytest.approx(-math.log1p(10))
    assert result["radiant"]["pos1_rank_europe_coverage"] == 0
    assert result["features"]["pos1_rank_europe_coverage_joint"] == 0
    equal = history.features(radiant, dire, asof=now, time_policy="calendar_period")
    assert equal["radiant"]["pos1_rank_se_asia_coverage"] == 1
    assert all(not p["earnings_backfilled"] and not p["rank_backfilled"]
               for side in equal["diagnostics"] for p in side)
    assert not any(history.features(radiant, dire, asof=now)["features"].values())
    stale = history.features(radiant, dire, asof=now + 7 * DAY + 1)
    assert not any(v for k, v in stale["features"].items() if "_rank_" in k)
    assert all(p["rank_status"] == "rank_stale" for side in stale["diagnostics"] for p in side)


def test_position_order_and_side_swapping():
    snapshot = sample()
    radiant, dire = lineups(snapshot)
    history = PlayerHistory([snapshot])
    result = history.features(radiant, dire, asof=snapshot["observed_at"] + 1)
    swapped = history.features(dire, radiant, asof=snapshot["observed_at"] + 1)
    for key, value in result["features"].items():
        assert swapped["features"][key] == pytest.approx(value if key.endswith("_joint") else -value)
    first = next(p for p in snapshot["players"] if p["account_id"] == radiant[0])
    assert result["radiant"]["pos1_earnings_log_mean"] == pytest.approx(math.log1p(first["earnings_usd"]))


def test_zero_prizes_are_known_missing_are_not():
    snapshot = sample()
    radiant, dire = lineups(snapshot)
    for p in snapshot["players"]:
        p["earnings_usd"] = 0 if p["account_id"] in radiant else None
    result = PlayerHistory([snapshot]).features(radiant, dire, asof=snapshot["observed_at"] + 1)
    assert result["radiant"]["team_earnings_coverage"] == 1
    assert result["dire"]["team_earnings_coverage"] == 0
    assert result["features"]["team_earnings_log_mean_diff"] == 0
    assert result["features"]["team_earnings_coverage_diff"] == 1


def test_multiple_snapshots_select_past_observation_and_expire_earnings():
    old = sample()
    new = copy.deepcopy(old)
    new["observed_at"] += DAY
    for p in new["players"]:
        p["observed_at"] = new["observed_at"]
        p["earnings_usd"] = 1000000
    history = PlayerHistory([new, old, old])
    aid = old["players"][0]["account_id"]
    assert history.at(aid, new["observed_at"])["earnings_usd"] == old["players"][0]["earnings_usd"]
    assert history.at(aid, new["observed_at"] + 1)["earnings_usd"] == 1000000
    result = history.features(*lineups(old), asof=new["observed_at"] + 31 * DAY)
    assert not any(v for k, v in result["features"].items() if "_earnings_" in k)


@pytest.mark.parametrize("mutation", ["duplicate", "steam64", "partial", "role"])
def test_bad_dltv_identity_fails_closed(mutation):
    data = json.loads(FIXTURE.read_text())
    players = data["series_item"]["series_players"]
    if mutation == "duplicate":
        players[1]["player"]["steam_id"] = players[0]["player"]["steam_id"]
    elif mutation == "steam64":
        players[0]["player"]["steam_id"] = 76561197960265728
    elif mutation == "partial":
        players.pop()
    else:
        players[0]["role"] = 7
    with pytest.raises(ValueError):
        parse_dltv_match("series_item = " + json.dumps(data["series_item"]), source_url=data["source_url"], observed_at=data["observed_at"])


@pytest.mark.parametrize("field,value", [("earnings_usd", -1), ("rank", float("nan")), ("account_id", True), ("rank_region", "europe")])
def test_invalid_normalized_observation_rejected(field, value):
    snapshot = sample()
    snapshot["players"][0][field] = value
    with pytest.raises(ValueError):
        PlayerHistory([snapshot])


def test_conflicting_same_time_and_future_rank_rejected():
    snapshot = sample()
    changed = copy.deepcopy(snapshot)
    changed["players"][0]["earnings_usd"] = 999
    with pytest.raises(ValueError, match="conflicting"):
        PlayerHistory([snapshot, changed])
    snapshot["players"][0]["rank_updated_at"] = snapshot["observed_at"] + 10
    with pytest.raises(ValueError, match="after observation"):
        PlayerHistory([snapshot])


def test_export_joins_real_lineups_by_id_and_preserves_output(tmp_path):
    snapshot = sample()
    directory = tmp_path / "snapshots" / "one"
    directory.mkdir(parents=True)
    (directory / "snapshot.json").write_text(json.dumps(snapshot))
    fixture = json.loads(FIXTURE.read_text())
    (directory / "source.html").write_text("<script>series_item = " + json.dumps(fixture["series_item"]) + ";</script>")
    radiant, dire = lineups(snapshot)
    matches = tmp_path / "matches.jsonl"
    matches.write_text(json.dumps({"match_id": "example", "start_ts": snapshot["observed_at"] - 1,
                                  "radiant_players": [{"account_id": a, "position": i + 1} for i, a in enumerate(radiant)],
                                  "dire_players": [{"account_id": a, "position": i + 1} for i, a in enumerate(dire)]}) + "\n")
    output = tmp_path / "features.jsonl"
    assert export(directory.parent, matches, output)["rows"] == 1
    assert not any(json.loads(output.read_text())["features"].values())
    approximate = tmp_path / "approximate.jsonl"
    export(directory.parent, matches, approximate, time_policy="calendar_period")
    result = json.loads(approximate.read_text())
    assert result["time_policy"] == "calendar_period"
    assert result["retrospective_assumption"] is True
    assert result["features"]["team_earnings_coverage_joint"] == 1
    before = output.read_bytes()
    with pytest.raises(FileExistsError):
        write_new(output, "overwrite")
    assert output.read_bytes() == before
    snapshot["players"][0]["earnings_usd"] += 1000
    (directory / "snapshot.json").write_text(json.dumps(snapshot))
    with pytest.raises(ValueError, match="differ from retained source"):
        load_snapshot(directory / "snapshot.json")


def test_explicit_positions_normalize_unordered_lineup_and_reject_duplicates():
    players = [{"account_id": 100 + i, "position": i} for i in range(5, 0, -1)]
    assert position_accounts(players) == [101, 102, 103, 104, 105]
    players[0]["position"] = 1
    with pytest.raises(ValueError, match="unique map positions"):
        position_accounts(players)


def test_country_string_cannot_authorize_region():
    snapshot = sample()
    snapshot["players"][0].update(rank_region="europe", rank_region_source="country flag")
    with pytest.raises(ValueError, match="account-bound"):
        PlayerHistory([snapshot])


def test_shell_or_javascript_is_never_executed():
    with pytest.raises(ValueError, match="missing JSON"):
        parse_dltv_match("series_item = fetch('/secret')", source_url="https://dltv.org", observed_at=100)


def test_monthly_assumption_is_opt_in_and_never_crosses_utc_month():
    snapshot = sample()
    history = PlayerHistory([snapshot])
    lineup = lineups(snapshot)
    def at(date, policy="calendar_period"):
        return history.features(*lineup, asof=timestamp(date), time_policy=policy)
    first = at("2026-09-01T00:00:00Z")
    assert first["features"]["team_earnings_coverage_joint"] == 1
    assert first["retrospective_assumption"] is True
    assert all(p["earnings_backfilled"] for side in first["diagnostics"] for p in side)
    for boundary in ("2026-08-31T23:59:59Z", "2026-10-01T00:00:00Z"):
        assert at(boundary)["features"]["team_earnings_coverage_joint"] == 0
    assert not any(at("2026-09-01T00:00:00Z", "strict")["features"].values())
    late = at("2026-09-30T23:59:59Z")
    assert {k: v for k, v in first["features"].items() if "earnings" in k} == {
        k: v for k, v in late["features"].items() if "earnings" in k}
    assert not any(v for k, v in first["features"].items() if "rank" in k)


def test_weekly_assumption_uses_update_week_but_requires_verified_region():
    snapshot = sample()
    radiant, dire = lineups(snapshot)
    row = next(p for p in snapshot["players"] if p["account_id"] == radiant[0])
    updated = timestamp("2026-09-10T12:00:00Z")
    row.update(rank=100, rank_updated_at=updated, rank_region="europe", rank_region_source={
        "url": "https://example.test/account", "sha256": "0" * 64,
        "account_id": row["account_id"], "identity_method": "account_id",
        "observed_at": snapshot["observed_at"], "region": "europe", "rank": 100,
        "rank_updated_at": updated})
    history = PlayerHistory([snapshot])
    for date in ("2026-09-07T00:00:00Z", "2026-09-13T23:59:59Z"):
        result = history.features(radiant, dire, asof=timestamp(date), time_policy="calendar_period")
        assert result["radiant"]["pos1_rank_europe_strength"] == pytest.approx(-math.log1p(100))
        assert result["diagnostics"][0][0]["rank_backfilled"] is True
        assert result["diagnostics"][0][0]["rank_updated_at"] == updated
    for date in ("2026-09-06T23:59:59Z", "2026-09-14T00:00:00Z"):
        result = history.features(radiant, dire, asof=timestamp(date), time_policy="calendar_period")
        assert result["radiant"]["pos1_rank_europe_coverage"] == 0


def test_period_representative_is_latest_and_order_independent():
    old = sample()
    new = copy.deepcopy(old)
    new["observed_at"] += DAY
    for p in new["players"]:
        p["observed_at"] = new["observed_at"]
        p["earnings_usd"] = 1000000
    cutoff = timestamp("2026-09-05T00:00:00Z")
    left = PlayerHistory([old, new]).features(*lineups(old), asof=cutoff, time_policy="calendar_period")
    right = PlayerHistory([new, old]).features(*lineups(old), asof=cutoff, time_policy="calendar_period")
    assert left == right
    assert left["radiant"]["team_earnings_log_mean"] == pytest.approx(math.log1p(1000000))
    with pytest.raises(ValueError, match="time policy"):
        PlayerHistory([old]).features(*lineups(old), asof=cutoff, time_policy="relaxed")


def test_iso_week_key_crosses_calendar_year():
    from base.player_metadata import calendar_keys
    _, december = calendar_keys(timestamp("2026-12-31T23:00:00Z"))
    _, january = calendar_keys(timestamp("2027-01-01T01:00:00Z"))
    _, monday = calendar_keys(timestamp("2027-01-04T00:00:00Z"))
    assert december == january == (2026, 53)
    assert monday == (2027, 1)
