"""Captured missing map reaches the served snapshot, with corpus precedence."""
from __future__ import annotations

import ast
import copy
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from ELO.config import HybridEloConfig
from ELO import live_team_strength as live


PRE_SUPPLEMENT_COMMIT = "630979142f1b1cd1247f5535fd9cf2df0a1cef38"  # last main commit before the supplement (7398ee0e); full sha so a short prefix can never become ambiguous
FIXTURE = Path(__file__).parent / "fixtures/elo_supplement_20261006"
CONVERTER = Path(__file__).resolve().parents[2] / "scripts/pro_chain/build_elo_supplement.py"


def test_fixture_readme_pins_capture_command_and_dates():
    readme = (FIXTURE / "README.md").read_text()
    for date in ("2026-10-05", "2026-10-06", "2026-10-07T18:37:52.958Z"):
        assert date in readme
    marker = "<!-- capture-command:start -->\n```sh\n"
    assert marker in readme
    command = readme.split(marker, 1)[1].split("\n```\n<!-- capture-command:end -->", 1)[0]
    assert hashlib.sha256(command.encode()).hexdigest() == "f6aae33659461adfab2e2e128cde907d346dd70f754f37b63faf7a930860fa12"
    assert "not captured explorer name fields" in readme
    assert "2026-10-07T19:03:17.212Z" in readme


def _convert(input_dir, output, *extra):
    result = subprocess.run([sys.executable, str(CONVERTER), "--input-dir", str(input_dir),
                             "--output", str(output), *map(str, extra)],
                            check=True, capture_output=True, text=True)
    return json.loads(output.read_text()), json.loads(result.stdout)


def _build(corpus, supplement_dir=None):
    return live._build_snapshot_dict(data_dir=corpus, active_cutoff_days=180,
                                     display_decay_half_life_days=120, config=HybridEloConfig(),
                                     supplement_dir=supplement_dir)


def test_captured_missing_map_reaches_snapshot_and_corpus_wins(tmp_path, monkeypatch):
    corpus, supplement = tmp_path / "corpus", tmp_path / "supplement"
    corpus.mkdir()
    raw_corpus = json.loads((FIXTURE / "corpus_captured.json").read_text())
    (corpus / "7.41e_part091.json").write_text(json.dumps(raw_corpus))
    maps, counts = _convert(FIXTURE, supplement / "opendota_maps.json")
    assert counts["written"] == 1
    missing = maps["9021470896"]
    assert missing["durationSeconds"] == 2785
    assert missing["didRadiantWin"] is False
    collision = copy.deepcopy(next(iter(raw_corpus.values())))
    collision["startDateTime"] = missing["startDateTime"] - 86400
    collision["didRadiantWin"] = not collision["didRadiantWin"]
    altered_accounts = []
    for i, player in enumerate(collision["players"]):
        player["steamAccount"]["id"] = 4000000000 + i
        altered_accounts.append(str(4000000000 + i))
    maps[str(collision["id"])] = collision
    maps["invalid"] = dict(missing, players=[])
    (supplement / "opendota_maps.json").write_text(json.dumps(maps))
    monkeypatch.setenv("ELO_SUPPLEMENT_DIR", str(tmp_path / "absent"))
    corpus_only = _build(corpus)
    snapshot = _build(corpus, supplement)
    state = snapshot["model_state"]
    for player in missing["players"]:
        account = str(player["steamAccount"]["id"])
        assert state["player_a_games"].get(account) == 1
        rating = state["player_a"][account]
        assert rating < 1500 if player["isRadiant"] else rating > 1500
    assert missing["id"] in snapshot["meta"]["recent_completed_match_ids"]
    assert [missing["id"], missing["radiantTeam"]["id"], missing["direTeam"]["id"],
            missing["startDateTime"]] in snapshot["meta"]["recent_completed_match_keys"]
    assert all(account not in state["player_a_games"] for account in altered_accounts)
    for account, games in corpus_only["model_state"]["player_a_games"].items():
        assert state["player_a_games"][account] == games
        assert state["player_a"][account] == corpus_only["model_state"]["player_a"][account]
    summary = snapshot["meta"]["load_summary"]
    assert summary["supplement_loaded"] == 1
    assert summary["supplement_skipped_in_corpus"] == 1
    assert summary["supplement_invalid"] == 1
    assert snapshot["meta"]["loaded_matches"] == 3


def test_keyword_overrides_env_and_public_builder_writes_snapshot(tmp_path, monkeypatch):
    corpus, supplement = tmp_path / "corpus", tmp_path / "supplement"
    corpus.mkdir()
    maps, _ = _convert(FIXTURE, supplement / "opendota_maps.json")
    monkeypatch.setenv("ELO_SUPPLEMENT_DIR", str(tmp_path / "absent"))
    output = tmp_path / "snapshot.json"
    snapshot = live.build_snapshot(data_dir=corpus, snapshot_path=output,
                                   supplement_dir=supplement)
    assert json.loads(output.read_text()) == snapshot
    assert snapshot["meta"]["recent_completed_match_ids"] == [9021470896]
    assert snapshot["meta"]["load_summary"]["supplement_loaded"] == len(maps) == 1
    monkeypatch.delenv("ELO_SUPPLEMENT_DIR")
    monkeypatch.setattr(live, "DEFAULT_SUPPLEMENT_DIR", supplement)
    assert _build(corpus)["meta"]["load_summary"]["supplement_loaded"] == 1


def test_missing_supplement_and_empty_corpus_are_noop(tmp_path, monkeypatch):
    from ELO.data_loader import load_supplement_matches

    monkeypatch.setenv("ELO_SUPPLEMENT_DIR", str(tmp_path / "absent"))
    assert load_supplement_matches(tmp_path / "absent") == ([], {})
    snapshot = _build(tmp_path)
    assert snapshot["model_state"] is None
    assert snapshot["meta"]["loaded_matches"] == 0
    assert "load_summary" not in snapshot["meta"]
    assert "supplement_dir" not in snapshot["meta"]


def test_supplement_repeated_files_are_one_rating_update(tmp_path, monkeypatch):
    supplement = tmp_path / "supplement"
    maps, _ = _convert(FIXTURE, supplement / "first.json")
    (supplement / "second.json").write_text(json.dumps(maps))
    snapshot = _build(tmp_path, supplement)
    assert snapshot["meta"]["load_summary"]["supplement_duplicate_records"] == 1
    assert snapshot["meta"]["loaded_matches"] == 1
    assert set(snapshot["model_state"]["player_a_games"].values()) == {1}


@pytest.mark.parametrize("ids_text", ["[9021470896, 123]\n", "9021470896\n\n123\n"])
def test_converter_excludes_corpus_ids_in_both_formats(tmp_path, ids_text):
    ids = tmp_path / "processed_ids.txt"
    ids.write_text(ids_text)
    output = tmp_path / "opendota_maps.json"
    maps, counts = _convert(FIXTURE, output, "--exclude-corpus-ids", ids)
    assert maps == {}
    assert counts["excluded"] == 1
    assert counts["written"] == 0
    assert not list(tmp_path.glob("opendota_maps.json.*.tmp"))


@pytest.mark.parametrize("side", ["radiant", "dire"])
@pytest.mark.parametrize("field,value", [("team_id", None), ("team_id", 0),
                                        ("team_id", -1), ("team_id", True),
                                        ("name", None), ("name", ""), ("name", "  "),
                                        ("name", "od-2000000001"),
                                        ("name", " OD-2000000001 ")])
def test_converter_skips_missing_team_identity(tmp_path, side, field, value):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    listing = json.loads((FIXTURE / "listing_captured.json").read_text())
    listing[0][f"{side}_{field}"] = value
    (inputs / "listing_maps.json").write_text(json.dumps(listing))
    (inputs / "players_maps.json").write_text((FIXTURE / "players_captured.json").read_text())
    output = tmp_path / "opendota_maps.json"
    maps, counts = _convert(inputs, output)
    assert maps == {}
    assert counts["invalid_team"] == 1
    assert counts["written"] == 0
    assert not list(tmp_path.glob("opendota_maps.json.*.tmp"))


@pytest.mark.parametrize("side", ["radiantTeam", "direTeam"])
@pytest.mark.parametrize("name", ["", "  ", "od-2000000001", " OD-2000000001 "])
def test_invalid_supplement_identity_never_enters_a_rank_map(tmp_path, side, name):
    corpus, supplement = tmp_path / "corpus", tmp_path / "supplement"
    corpus.mkdir()
    maps, _ = _convert(FIXTURE, supplement / "maps.json")
    (corpus / "maps.json").write_text((FIXTURE / "corpus_captured.json").read_text())
    baseline = _build(corpus)
    maps["9021470896"][side] = {"id": 2000000001, "name": name}
    (supplement / "maps.json").write_text(json.dumps(maps))
    actual = _build(corpus, supplement)
    assert actual["model_state"] == baseline["model_state"]
    assert actual["teams_by_org_key"] == baseline["teams_by_org_key"]
    assert live._a_leaderboard_rank_map(actual) == live._a_leaderboard_rank_map(baseline)
    assert actual["meta"]["load_summary"]["supplement_invalid"] == 1
    assert actual["meta"]["load_summary"]["supplement_loaded"] == 0


def test_autouse_supplement_isolation_guards_default_snapshot(tmp_path, monkeypatch):
    import os

    isolated = Path(os.environ.get("ELO_SUPPLEMENT_DIR", ""))
    assert isolated.name.startswith("empty_elo_supplement")
    assert isolated.is_dir() and list(isolated.iterdir()) == []
    # Item 1 pin: the fixture must not add anything inside the test's own tmp_path
    # (test_rebase_lock asserts no stray file next to its state files).
    assert tmp_path not in isolated.parents
    assert list(tmp_path.iterdir()) == []
    # A populated fallback must never be read, even when repo data exists.
    fallback = tmp_path / "populated_default"
    _convert(FIXTURE, fallback / "maps.json")
    monkeypatch.setattr(live, "DEFAULT_SUPPLEMENT_DIR", fallback)
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    assert _build(corpus)["model_state"] is None


@pytest.mark.parametrize("with_corpus", [False, True])
@pytest.mark.parametrize("exists", [False, True])
def test_empty_supplement_leaves_no_trace(tmp_path, with_corpus, exists):
    """Builder-change-proof inertness: an empty or missing supplement dir adds no
    supplement key, path or counter anywhere in the snapshot."""
    corpus, supplement = tmp_path / "corpus", tmp_path / "supplement"
    corpus.mkdir()
    if exists:
        supplement.mkdir()
        (supplement / "notes.txt").write_text("not a supplement file")
    if with_corpus:
        (corpus / "maps.json").write_text((FIXTURE / "corpus_captured.json").read_text())
    snapshot = _build(corpus, supplement)

    def keys_and_strings(node):
        if isinstance(node, dict):
            for key, value in node.items():
                yield str(key)
                yield from keys_and_strings(value)
        elif isinstance(node, (list, tuple)):
            for value in node:
                yield from keys_and_strings(value)
        elif isinstance(node, str):
            yield node

    # tmp_path itself contains this test's name: blank it out, then no key or string may
    # mention the supplement (a key such as "meta/supplement" is caught too).
    found = [s.replace(str(tmp_path), "<tmp>") for s in keys_and_strings(snapshot)]
    assert not [s for s in found if "supplement" in s.casefold()]


@pytest.mark.parametrize("with_corpus", [False, True])
@pytest.mark.parametrize("exists", [False, True])
def test_empty_supplement_snapshot_is_byte_identical_to_main(tmp_path, with_corpus, exists):
    corpus, supplement = tmp_path / "corpus", tmp_path / "supplement"
    corpus.mkdir()
    if exists:
        supplement.mkdir()
    if with_corpus:
        (corpus / "maps.json").write_text((FIXTURE / "corpus_captured.json").read_text())
    # Execute only the PRE-SUPPLEMENT snapshot function, against these captured inputs.
    # Pinned to the last main commit before the supplement (7398ee0e): comparing against
    # HEAD became a comparison with the supplement builder itself after that commit and
    # failed every uncommitted builder change. safe.directory: serv1 runs the suite under
    # systemd-run without HOME, where git refuses the repo as "dubious ownership".
    # If _build_snapshot_dict itself is changed on purpose later, this pin no longer
    # describes main. Do not just retire it: test_empty_supplement_leaves_no_trace only
    # catches leaks that carry the word "supplement" (a neutral counter slips through), so
    # replace this with a comparison against the new builder run on a supplement-free tree.
    shown = subprocess.run(["git", "-c", "safe.directory=*", "show",
                            f"{PRE_SUPPLEMENT_COMMIT}:ELO/live_team_strength.py"],
                           cwd=CONVERTER.parents[2], capture_output=True, text=True)
    if shown.returncode != 0:
        pytest.skip(f"pre-supplement commit {PRE_SUPPLEMENT_COMMIT} unavailable: "
                    f"{shown.stderr.strip()[:200]}")
    function = next(node for node in ast.parse(shown.stdout).body
                    if isinstance(node, ast.FunctionDef) and node.name == "_build_snapshot_dict")
    namespace = dict(vars(live))
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 f"{PRE_SUPPLEMENT_COMMIT}:snapshot", "exec"), namespace)
    expected = namespace["_build_snapshot_dict"](
        data_dir=corpus, active_cutoff_days=180,
        display_decay_half_life_days=120, config=HybridEloConfig())
    actual = _build(corpus, supplement)
    assert json.dumps(actual, sort_keys=True) == json.dumps(expected, sort_keys=True)
    assert actual["meta"]["model_config_signature"] == expected["meta"]["model_config_signature"]


@pytest.mark.parametrize("tier,stratz_tier", [("professional", "PROFESSIONAL"),
                                            ("premium", "PREMIUM"),
                                            ("excluded", "EXCLUDED"), (None, None)])
@pytest.mark.parametrize("series_type,stratz_type", [(0, "BEST_OF_ONE"),
                                                   (1, "BEST_OF_THREE"),
                                                   (2, "BEST_OF_FIVE"),
                                                   (3, "BEST_OF_TWO")])
@pytest.mark.parametrize("known_teams", [True, False])
def test_supplement_matches_equivalent_stratz_snapshot(
        tmp_path, tier, stratz_tier, series_type, stratz_type, known_teams):
    from ELO.team_identity import resolve_org_key

    inputs, corpus, supplement, empty = (
        tmp_path / name for name in ("inputs", "corpus", "supplement", "empty"))
    inputs.mkdir()
    corpus.mkdir()
    empty.mkdir()
    row = json.loads((FIXTURE / "listing_captured.json").read_text())[0]
    # Unmapped IDs exercise name-based identity and the source-tier fallback.
    row.update(tier=tier, series_type=series_type)
    if not known_teams:
        row.update(radiant_team_id=2000000001, dire_team_id=2000000002,
                   radiant_name="Unmapped Radiant", dire_name="Unmapped Dire")
    (inputs / "listing_maps.json").write_text(json.dumps([row]))
    players = json.loads((FIXTURE / "players_captured.json").read_text())
    (inputs / "players_maps.json").write_text(json.dumps(players))
    maps, _ = _convert(inputs, supplement / "opendota_maps.json")
    equivalent = {
        "id": row["match_id"], "startDateTime": row["start_time"],
        "durationSeconds": row["duration"], "didRadiantWin": row["radiant_win"],
        "leagueId": row["leagueid"],
        "league": {"id": row["leagueid"], "tier": stratz_tier, "name": row["league_name"]},
        "series": {"id": row["series_id"], "type": stratz_type},
        "radiantTeam": {"id": row["radiant_team_id"], "name": row["radiant_name"]},
        "direTeam": {"id": row["dire_team_id"], "name": row["dire_name"]},
        "players": [{"isRadiant": p["player_slot"] < 128,
                     "steamAccount": {"id": p["account_id"]}, "position": None} for p in players],
    }
    (corpus / "maps.json").write_text(json.dumps({str(row["match_id"]): equivalent}))
    actual = _build(empty, supplement)
    expected = _build(corpus, empty)
    assert actual["model_state"] == expected["model_state"]
    assert actual["teams_by_org_key"] == expected["teams_by_org_key"]
    assert len(actual["teams_by_org_key"]) == 2
    for side in ("radiant", "dire"):
        key = resolve_org_key(row[f"{side}_team_id"], row[f"{side}_name"])
        assert actual["teams_by_org_key"][key]["team_name"] == row[f"{side}_name"]
    assert not any(key == "name:unknown" or key.startswith("name:od")
                   for key in actual["teams_by_org_key"])
    for key in ("series_groups", "eligible_series", "tier_matchup_elo_bonus"):
        assert actual["meta"][key] == expected["meta"][key]
    assert maps[str(row["match_id"])]["league"]["tier"] == stratz_tier
    assert maps[str(row["match_id"])]["series"]["type"] == stratz_type


@pytest.mark.parametrize("bad", ["anonymous", "null_account", "duplicate_account",
                                 "winner", "duration", "side_count"])
def test_converter_rejects_ineligible_captured_map(tmp_path, bad):
    listing = json.loads((FIXTURE / "listing_captured.json").read_text())
    players = json.loads((FIXTURE / "players_captured.json").read_text())
    if bad == "anonymous":
        players[0]["account_id"] = 4294967295
    elif bad == "null_account":
        players[0]["account_id"] = None
    elif bad == "duplicate_account":
        players[0]["account_id"] = players[1]["account_id"]
    elif bad == "winner":
        listing[0]["radiant_win"] = 0
    elif bad == "duration":
        listing[0]["duration"] = 0
    elif bad == "side_count":
        players[0]["player_slot"] = 133
    (tmp_path / "listing_maps.json").write_text(json.dumps(listing))
    (tmp_path / "players_maps.json").write_text(json.dumps(players))
    maps, counts = _convert(tmp_path, tmp_path / "opendota_maps.json")
    assert maps == {}
    assert counts["invalid"] == 1


def test_converter_bad_input_preserves_existing_output(tmp_path):
    output = tmp_path / "opendota_maps.json"
    _convert(FIXTURE, output)
    previous = output.read_bytes()
    (tmp_path / "listing_maps.json").write_text("{broken")
    with pytest.raises(subprocess.CalledProcessError):
        _convert(tmp_path, output)
    assert output.read_bytes() == previous


def test_converter_dump_failure_cleans_tmp_and_preserves_output(tmp_path, monkeypatch):
    output = tmp_path / "opendota_maps.json"
    _convert(FIXTURE, output)
    previous = output.read_bytes()
    spec = importlib.util.spec_from_file_location("elo_supplement_converter", CONVERTER)
    converter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(converter)
    monkeypatch.setattr(sys, "argv", [str(CONVERTER), "--input-dir", str(FIXTURE),
                                    "--output", str(output)])

    def fail_dump(value, fh, **kwargs):
        fh.write("{partial")
        raise RuntimeError("injected dump failure")

    monkeypatch.setattr(converter.json, "dump", fail_dump)
    with pytest.raises(RuntimeError, match="injected dump failure"):
        converter.main()
    assert output.read_bytes() == previous
    assert not list(tmp_path.glob("opendota_maps.json.*.tmp"))


# --- C1 round 3: items 2-4 -------------------------------------------------------

def _drift_inputs(tmp_path, corpus_name="Zq Unmapped Five Old", supplement_name="Zq Unmapped Five"):
    """The verifier's demo: one unmapped team_id, two spellings, same five players."""
    corpus, supplement = tmp_path / "corpus", tmp_path / "supplement"
    corpus.mkdir()
    raw = json.loads((FIXTURE / "corpus_captured.json").read_text())
    corpus_map = raw["9030292632"]
    corpus_map["radiantTeam"] = {"id": 2000000001, "name": corpus_name}
    (corpus / "maps.json").write_text(json.dumps({"9030292632": corpus_map}))
    maps, _ = _convert(FIXTURE, supplement / "maps.json")
    sup = maps["9021470896"]
    sup["radiantTeam"] = {"id": 2000000001, "name": supplement_name}
    corpus_radiant = [p["steamAccount"]["id"] for p in corpus_map["players"] if p["isRadiant"]]
    radiant = [p for p in sup["players"] if p["isRadiant"]]
    assert len(corpus_radiant) == len(radiant) == 5
    for player, account in zip(radiant, corpus_radiant):
        player["steamAccount"]["id"] = account
    (supplement / "maps.json").write_text(json.dumps(maps))
    return corpus, supplement


def test_supplement_name_drift_for_unmapped_team_id_is_one_org_and_one_lineup(tmp_path):
    corpus, supplement = _drift_inputs(tmp_path)
    snapshot = _build(corpus, supplement)
    assert snapshot["meta"]["load_summary"]["supplement_loaded"] == 1
    zq_orgs = [key for key in snapshot["teams_by_org_key"] if "zq" in key]
    assert zq_orgs == ["name:zqunmappedfiveold"]
    assert snapshot["teams_by_org_key"][zq_orgs[0]]["team_name"] == "Zq Unmapped Five Old"
    zq_lineups = [key for key in snapshot["model_state"]["lineup_match_counts"] if "zq" in key]
    assert len(zq_lineups) == 1
    assert snapshot["model_state"]["lineup_match_counts"][zq_lineups[0]] == 2
    assert snapshot["meta"]["load_summary"]["supplement_team_names_aligned"] == 1


def test_supplement_team_absent_from_corpus_keeps_its_own_name(tmp_path):
    corpus, supplement = _drift_inputs(tmp_path)
    maps = json.loads((supplement / "maps.json").read_text())
    maps["9021470896"]["radiantTeam"]["id"] = 2000000002
    (supplement / "maps.json").write_text(json.dumps(maps))
    snapshot = _build(corpus, supplement)
    assert "name:zqunmappedfive" in snapshot["teams_by_org_key"]
    assert snapshot["meta"]["load_summary"]["supplement_team_names_aligned"] == 0


def _rec(match_id, ts, radiant, dire=(10, "Other")):
    from ELO.domain import MatchRecord
    return MatchRecord(match_id=match_id, timestamp=ts, radiant_win=True,
                       radiant_team_id=radiant[0], radiant_team_name=radiant[1],
                       dire_team_id=dire[0], dire_team_name=dire[1],
                       radiant_player_ids=(1, 2, 3, 4, 5), dire_player_ids=(6, 7, 8, 9, 10),
                       league_id=1, league_name="L", source_league_tier="PROFESSIONAL",
                       series_id=None, series_type=None)


def test_supplement_name_comes_from_nearest_corpus_map_in_time():
    team = 2000000001
    corpus = [_rec(1, 100, (team, "First")), _rec(2, 200, (team, "Second")),
              _rec(3, 300, (10, "Other"), dire=(team, "Third"))]
    names = lambda ts, name="Drift": [  # noqa: E731
        (m.radiant_team_name, m.dire_team_name)
        for m in live._align_supplement_team_names(
            corpus, [_rec(9, ts, (team, name))])[0]][0]
    assert names(50) == ("First", "Other")        # before every corpus map -> earliest after
    assert names(100) == ("First", "Other")       # at start -> that map
    assert names(150) == ("First", "Other")       # latest at or before
    assert names(250) == ("Second", "Other")
    assert names(999) == ("Third", "Other")       # dire side of the corpus map counts too
    aligned, count = live._align_supplement_team_names(corpus, [_rec(9, 250, (team, "Drift"))])
    assert count == 1 and aligned[0].dire_team_name == "Other"
    unknown = _rec(8, 250, (777, "Unseen"))
    assert live._align_supplement_team_names(corpus, [unknown]) == ([unknown], 0)


def test_malformed_supplement_file_is_skipped_with_warning_and_counted(tmp_path, caplog):
    supplement = tmp_path / "supplement"
    _convert(FIXTURE, supplement / "good.json")
    (supplement / "bad_truncated.json").write_text('{"9021470897": {"id": 9021470897, ')
    (supplement / "bad_garbage.json").write_bytes(b"\xff\xfenot json")
    (supplement / "bad_binary.json").write_bytes(b"\x00\x01\x02")
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    with caplog.at_level("WARNING"):
        snapshot = _build(corpus, supplement)
    summary = snapshot["meta"]["load_summary"]
    assert summary["supplement_loaded"] == 1
    assert summary["supplement_skipped_files"] == 3
    assert snapshot["meta"]["recent_completed_match_ids"] == [9021470896]
    warned = " ".join(r.getMessage() for r in caplog.records if r.levelname == "WARNING")
    for name in ("bad_truncated.json", "bad_garbage.json", "bad_binary.json"):
        assert name in warned
    assert "good.json" not in warned


def test_malformed_supplement_file_keeps_no_partial_maps(tmp_path):
    supplement = tmp_path / "supplement"
    maps, _ = _convert(FIXTURE, supplement / "seed.json")
    (supplement / "seed.json").unlink()
    text = json.dumps(maps)
    # The first map parses completely, then a second entry is cut off (verifier input:
    # text[:-3] cut inside the only map, so a kept partial map was never exercised).
    (supplement / "truncated.json").write_text(text[:-1] + ', "second": {')
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    snapshot = _build(corpus, supplement)
    assert snapshot["meta"]["load_summary"]["supplement_loaded"] == 0
    assert snapshot["meta"]["load_summary"]["supplement_skipped_files"] == 1
    assert snapshot["model_state"] is None


def test_only_malformed_supplement_file_leaves_corpus_snapshot_intact(tmp_path):
    corpus, supplement = tmp_path / "corpus", tmp_path / "supplement"
    corpus.mkdir()
    supplement.mkdir()
    (corpus / "maps.json").write_text((FIXTURE / "corpus_captured.json").read_text())
    (supplement / "bad.json").write_text("{")
    baseline = _build(corpus, tmp_path / "absent")
    actual = _build(corpus, supplement)
    assert actual["model_state"] == baseline["model_state"]
    assert actual["teams_by_org_key"] == baseline["teams_by_org_key"]
    assert actual["meta"]["load_summary"]["supplement_skipped_files"] == 1


def test_corpus_loader_still_raises_on_malformed_file(tmp_path):
    from ELO.data_loader import load_matches

    (tmp_path / "bad.json").write_text("{")
    with pytest.raises(Exception):
        load_matches(tmp_path)


def test_converter_single_id_exclude_file_excludes_that_id(tmp_path):
    ids = tmp_path / "processed_ids.txt"
    ids.write_text("9021470896\n")
    maps, counts = _convert(FIXTURE, tmp_path / "opendota_maps.json", "--exclude-corpus-ids", ids)
    assert maps == {}
    assert counts["excluded"] == 1 and counts["written"] == 0
