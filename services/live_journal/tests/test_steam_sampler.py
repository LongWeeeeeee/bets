"""Offline entry-point regressions against captured Steam responses."""
import configparser
import gzip
import json
import time
from datetime import datetime, timedelta, timezone
from http.client import IncompleteRead
from pathlib import Path
from urllib.error import URLError

import pytest

from services.live_journal import steam_sampler as sampler


FIXTURES = Path(__file__).parent / "fixtures"
STAMP = "20261010T175305Z"
CEB = 88271237
PRIVATE = 1673586483
OTHER = 303615577
OFFSET = 76561197960265728
TOP = "IDOTA2Match_570/GetTopLiveGame/v1/"
SUMMARIES = "ISteamUser/GetPlayerSummaries/v2/"
RECENT = "IPlayerService/GetRecentlyPlayedGames/v1/"


def body(name):
    return gzip.decompress((FIXTURES / (name + "_" + STAMP + ".json.gz")).read_bytes())


def rows(directory, mode):
    return [json.loads(line) for path in sorted(directory.glob(mode + "_*.jsonl"))
            for line in path.read_text().splitlines()]


def write_lines(path, values):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(value) + "\n" for value in values))


@pytest.fixture
def journal(tmp_path, monkeypatch):
    output = tmp_path / "journal"
    output.mkdir()
    monkeypatch.setenv("LIVE_JOURNAL_DIR", str(output))
    return output


def sources(journal, accounts):
    path = journal / "positions.jsonl"
    write_lines(path, [{"ts": time.time(), "resolved": {str(a): 1 for a in accounts}}])
    return path


def playtime_args(path, *extra):
    return ["playtime", "--pos-resolution", str(path), "--sleep-s", "0", *extra]


def captured_http(calls):
    def get(endpoint, params):
        calls.append((endpoint, params))
        if endpoint == TOP:
            partner = 1 if params["partner"] == 1 else 0
            return 200, body("getTopLiveGame_partner" + str(partner))
        if endpoint == SUMMARIES:
            return 200, body("getPlayerSummaries")
        assert endpoint == RECENT
        account = int(params["steamid"]) - OFFSET
        name = ("getRecentlyPlayedGames_visible_88271237" if account == CEB else
                "getRecentlyPlayedGames_private_1673586483")
        return 200, body(name)
    return get


def test_toplive_captured_dedup_raw_fields_and_append(journal, monkeypatch):
    calls = []
    monkeypatch.setattr(sampler, "http_get", captured_http(calls))
    assert sampler.main(["toplive"]) == 0
    result = rows(journal, "toplive")
    games = [g for p in (0, 1) for g in json.loads(body("getTopLiveGame_partner" + str(p)))["game_list"]]
    first = {}
    partners = {}
    for p in range(4):
        for game in json.loads(body("getTopLiveGame_partner" + str(1 if p == 1 else 0)))["game_list"]:
            key = game.get("lobby_id") or game["match_id"]
            first.setdefault(key, game)
            partners.setdefault(key, []).append(p)
    assert len(result) == len(first) == 20
    assert len(games) == 20
    assert [params["partner"] for _, params in calls] == [0, 1, 2, 3]
    for row in result:
        key = row.get("lobby_id") or row["match_id"]
        assert row["schema"] == "live_toplive.v1"
        assert isinstance(row["wall"], (int, float))
        assert row["partners"] == partners[key]
        assert {k: v for k, v in row.items() if k not in ("schema", "wall", "partners")} == {
            k: v for k, v in first[key].items() if k not in ("team_logo_radiant", "team_logo_dire")}
    original = next(journal.glob("toplive_*.jsonl")).read_bytes()
    assert sampler.main(["toplive"]) == 0
    assert len(rows(journal, "toplive")) == 40
    assert next(journal.glob("toplive_*.jsonl")).read_bytes().startswith(original)


def test_playtime_captured_union_privacy_and_append(journal, monkeypatch):
    now = datetime.now(timezone.utc)
    ticks = journal / ("ticks_" + now.strftime("%Y%m%d") + ".jsonl")
    write_lines(ticks, [{"schema": "live_ticks.v1", "picks": {
        "radiant": {"pos1": {"hero_id": 1, "account_id": CEB},
                    "pos2": {"account_id": 0}},
        "dire": {"pos1": {"account_id": 4294967295}}}},
        {"schema": "other", "picks": {"radiant": {"pos1": {"account_id": 123}}}},
        {"schema": "live_ticks.v1"}, {"schema": "live_ticks.v1", "picks": []}])
    with ticks.open("a") as stream:
        stream.write("bad JSON\n")
    old = journal / ("ticks_" + (now - timedelta(days=61)).strftime("%Y%m%d") + ".jsonl")
    write_lines(old, [{"schema": "live_ticks.v1", "picks": {"radiant": {"pos1": {"account_id": 456}}}}])
    path = sources(journal, [PRIVATE, CEB, 0, 4294967295])
    with path.open("a") as stream:
        stream.write("truncated {\n")
        stream.write(json.dumps({"ts": time.time() - 61 * 86400, "resolved": {"789": 1}}) + "\n")
        stream.write(json.dumps({"ts": "bad", "resolved": {"790": 1}}) + "\n")
    calls = []
    monkeypatch.setattr(sampler, "http_get", captured_http(calls))
    assert sampler.main(playtime_args(path)) == 0
    result = {row["account_id"]: row for row in rows(journal, "playtime")}
    assert set(result) == {CEB, PRIVATE}
    ceb = result[CEB]
    captured = json.loads(body("getRecentlyPlayedGames_visible_88271237"))["response"]["games"][0]
    assert ceb["dota_playtime_2weeks_min"] == captured["playtime_2weeks"] == 9608
    assert ceb["dota_playtime_forever_min"] == captured["playtime_forever"]
    assert ceb["recent_private"] is False
    assert ceb["recent_games_count"] == 1
    assert ceb["steamid64"] == str(OFFSET + CEB)
    summary = next(p for p in json.loads(body("getPlayerSummaries"))["response"]["players"]
                   if int(p["steamid"]) == OFFSET + CEB)
    for key in ("communityvisibilitystate", "profilestate", "personastate", "timecreated", "loccountrycode"):
        assert ceb[key] == summary.get(key)
    assert ceb["in_game_appid"] == summary.get("gameid")
    assert result[PRIVATE]["recent_private"] is True
    assert result[PRIVATE]["dota_playtime_2weeks_min"] is None
    assert result[PRIVATE]["recent_games_count"] is None
    allowed = {"schema", "wall", "account_id", "steamid64", "communityvisibilitystate", "profilestate",
               "personastate", "in_game_appid", "timecreated", "loccountrycode", "recent_private",
               "dota_playtime_2weeks_min", "dota_playtime_forever_min", "recent_games_count"}
    assert all(set(row) == allowed for row in result.values())
    assert len(calls) == 3
    output = next(journal.glob("playtime_*.jsonl"))
    original = output.read_bytes()
    assert sampler.main(playtime_args(path)) == 0
    assert output.read_bytes().startswith(original)
    assert len(rows(journal, "playtime")) == 4


@pytest.mark.parametrize("status", [429, 403, 500, 503, 599])
@pytest.mark.parametrize("k", [1, 2, 3])
def test_protective_status_stops_exactly_at_k_recent_call(journal, monkeypatch, capsys, status, k):
    path = sources(journal, [CEB, PRIVATE, OTHER])
    calls = []
    captured = captured_http(calls)
    recent_calls = []
    def get(endpoint, params):
        if endpoint == RECENT:
            recent_calls.append(params)
            if len(recent_calls) == k:
                calls.append((endpoint, params))
                return status, b"secret-key-in-error-body"
        return captured(endpoint, params)
    monkeypatch.setattr(sampler, "http_get", get)
    assert sampler.main(playtime_args(path)) == 0
    assert len(rows(journal, "playtime")) == k - 1
    assert len(calls) == k + 1
    log = capsys.readouterr().out
    assert "http_" + str(status) in log
    assert "secret-key" not in log
    assert len(log.splitlines()) == 1


def test_empty_sources_no_http_or_output(journal, monkeypatch, capsys):
    def forbidden(*args):
        pytest.fail("Empty sources must not call Steam")
    monkeypatch.setattr(sampler, "http_get", forbidden)
    assert sampler.main(playtime_args(journal / "missing.jsonl")) == 0
    assert rows(journal, "playtime") == []
    assert not list(journal.glob("playtime_*.jsonl"))
    log = capsys.readouterr().out
    assert "accounts=0 calls=0 rows=0" in log


def test_pos_resolution_alone_and_account_cap(journal, monkeypatch):
    path = sources(journal, [CEB, PRIVATE, OTHER])
    calls = []
    monkeypatch.setattr(sampler, "http_get", captured_http(calls))
    assert sampler.main(playtime_args(path, "--max-accounts", "2")) == 0
    assert [r["account_id"] for r in rows(journal, "playtime")] == sorted([CEB, PRIVATE, OTHER])[:2]
    assert len(calls) == 3


@pytest.mark.parametrize("limit, expected_rows", [(0, 0), (1, 0), (2, 1), (3, 2)])
def test_max_calls_is_hard_cap(journal, monkeypatch, capsys, limit, expected_rows):
    path = sources(journal, [CEB, PRIVATE, OTHER])
    calls = []
    monkeypatch.setattr(sampler, "http_get", captured_http(calls))
    assert sampler.main(playtime_args(path, "--max-calls", str(limit))) == 0
    assert len(calls) == limit
    assert len(rows(journal, "playtime")) == expected_rows
    assert "stop=max_calls" in capsys.readouterr().out


def test_summary_batches_at_most_100(journal, monkeypatch):
    path = sources(journal, list(range(1, 102)))
    calls = []
    monkeypatch.setattr(sampler, "http_get", captured_http(calls))
    assert sampler.main(playtime_args(path)) == 0
    batches = [params["steamids"].split(",") for endpoint, params in calls if endpoint == SUMMARIES]
    assert [len(batch) for batch in batches] == [100, 1]
    assert len(rows(journal, "playtime")) == 101
    assert len(calls) == 103


@pytest.mark.parametrize("error", [URLError("https://example.invalid/?key=never-print-me"),
                                 IncompleteRead(b"never-print-me", 100)])
def test_three_consecutive_network_errors_redacted(journal, monkeypatch, capsys, error):
    path = sources(journal, [1, 2, 3, 4])
    calls = []
    captured = captured_http(calls)
    def get(endpoint, params):
        if endpoint == RECENT:
            calls.append((endpoint, params))
            raise error
        return captured(endpoint, params)
    monkeypatch.setattr(sampler, "http_get", get)
    assert sampler.main(playtime_args(path)) == 0
    assert len(calls) == 4
    assert rows(journal, "playtime") == []
    log = capsys.readouterr().out
    assert "stop=network_errors_3" in log
    assert "never-print-me" not in log


def test_success_resets_network_error_count(journal, monkeypatch, capsys):
    path = sources(journal, [1, 2, 3, 4, 5])
    calls = []
    captured = captured_http(calls)
    attempts = []
    def get(endpoint, params):
        if endpoint == RECENT:
            attempts.append(params)
            if len(attempts) in (1, 3, 4):
                calls.append((endpoint, params))
                raise URLError("redact-this")
        return captured(endpoint, params)
    monkeypatch.setattr(sampler, "http_get", get)
    assert sampler.main(playtime_args(path)) == 0
    assert len(attempts) == 5
    assert len(rows(journal, "playtime")) == 2
    assert "stop=complete" in capsys.readouterr().out


def test_toplive_stops_on_shared_key_status(journal, monkeypatch, capsys):
    calls = []
    def get(endpoint, params):
        calls.append(params)
        if params["partner"] == 1:
            return 429, b"redact-this"
        return 200, body("getTopLiveGame_partner0")
    monkeypatch.setattr(sampler, "http_get", get)
    assert sampler.main(["toplive"]) == 0
    assert len(calls) == 2
    assert len(rows(journal, "toplive")) == 10
    assert all(r["partners"] == [0] for r in rows(journal, "toplive"))
    assert "stop=http_429" in capsys.readouterr().out


def test_source_boundaries_and_invalid_ids(journal, monkeypatch):
    now = datetime.now(timezone.utc)
    cutoff_date = now - timedelta(days=60)
    write_lines(journal / ("ticks_" + cutoff_date.strftime("%Y%m%d") + ".jsonl"), [{
        "schema": "live_ticks.v1", "picks": {"dire": {
            "pos1": {"account_id": CEB}, "pos2": {"account_id": True},
            "pos3": {"account_id": -1}, "pos4": {"account_id": "bad"},
            "pos5": {"account_id": 4294967296}}}}])
    future = journal / ("ticks_" + (now + timedelta(days=1)).strftime("%Y%m%d") + ".jsonl")
    write_lines(future, [{"schema": "live_ticks.v1", "picks": {"dire": {"pos1": {"account_id": 321}}}}])
    path = sources(journal, [PRIVATE])
    with path.open("a") as stream:
        for ts in (time.time() + 86400, float("nan"), float("inf")):
            stream.write(json.dumps({"ts": ts, "resolved": {"123": 1}}) + "\n")
        stream.write(json.dumps({"ts": time.time(), "resolved": []}) + "\n")
    calls = []
    monkeypatch.setattr(sampler, "http_get", captured_http(calls))
    assert sampler.main(playtime_args(path)) == 0
    assert {r["account_id"] for r in rows(journal, "playtime")} == {CEB, PRIVATE}


def test_invalid_json_never_becomes_private_row(journal, monkeypatch, capsys):
    path = sources(journal, [CEB])
    calls = []
    captured = captured_http(calls)
    def get(endpoint, params):
        if endpoint == RECENT:
            calls.append((endpoint, params))
            return 200, b"never-log-this-secret"
        return captured(endpoint, params)
    monkeypatch.setattr(sampler, "http_get", get)
    assert sampler.main(playtime_args(path)) == 0
    assert rows(journal, "playtime") == []
    log = capsys.readouterr().out
    assert "stop=invalid_json" in log
    assert "never-log" not in log


@pytest.mark.parametrize("mode, timeout, calendar", [
    ("toplive", "120", "*:0/10"), ("playtime", "3600", "*-*-* 01:40:00 UTC")])
def test_units_match_resource_limits_and_schedules(mode, timeout, calendar):
    directory = Path(__file__).resolve().parents[2] / "systemd"
    service = configparser.ConfigParser(interpolation=None)
    service.read(directory / ("ingame-live-" + mode + ".service"))
    expected = {
        "Type": "oneshot", "WorkingDirectory": "/root/main",
        "Environment": "HOME=/root PYTHONDONTWRITEBYTECODE=1",
        "ExecStart": "/root/main/venv/bin/python3 -m services.live_journal.steam_sampler " + mode,
        "Nice": "19", "CPUWeight": "10", "IOSchedulingClass": "idle",
        "MemoryHigh": "128M", "MemoryMax": "256M", "MemorySwapMax": "0",
        "OOMScoreAdjust": "1000", "TimeoutStartSec": timeout,
        "StandardOutput": "append:/root/main/runtime/artifacts/misc/live_journal_steam.log",
        "StandardError": "append:/root/main/runtime/artifacts/misc/live_journal_steam.log"}
    assert dict(service["Service"]) == {k.lower(): v for k, v in expected.items()}
    timer = configparser.ConfigParser(interpolation=None)
    timer.read(directory / ("ingame-live-" + mode + ".timer"))
    assert timer["Timer"]["OnCalendar"] == calendar
    assert timer["Timer"]["Persistent"] == "false"
    assert timer["Timer"]["RandomizedDelaySec"] == "60"
    assert timer["Timer"]["Unit"] == "ingame-live-" + mode + ".service"
    assert timer["Install"]["WantedBy"] == "timers.target"
