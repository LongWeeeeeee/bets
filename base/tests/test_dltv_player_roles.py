"""DLTV player roles: incident replay + module contract tests.

Incident 2026-09-24, match 9013821098 (BetBoom Streamers Battle), Team Daxak
(dire, team_id 10271241): without player-level data the permutation step
resolves DK=1/AA=2/Io=5; the DLTV soft vote (w=0.2) restores Io=1/DK=2/AA=5.
"""
import json
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(BASE_DIR.parent) not in sys.path:
    sys.path.insert(0, str(BASE_DIR.parent))

import dltv_player_roles as roles
import sourcetv_probe as probe

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "dltv_search_20260924"

# (account_id, hero_id): DK=49, AA=68, BB=99, DW=119, Io=91
DAXAK = [(216733371, 49), (212037295, 68), (177411785, 99),
         (154921394, 119), (970130166, 91)]
DAXAK_EXPECTED = {970130166: 1, 216733371: 2, 177411785: 3,
                  154921394: 4, 212037295: 5}
DAXAK_INCIDENT_OLD = {216733371: 1, 212037295: 2, 177411785: 3,
                      154921394: 4, 970130166: 5}


def _fixture_fetch(counter=None):
    def fetch(account_id):
        if counter is not None:
            counter.append(account_id)
        path = FIXTURES / f"search_{account_id}.json"
        return json.loads(path.read_text(encoding="utf-8"))
    return fetch


def _deny_network(*args, **kwargs):
    raise AssertionError("real network is disabled in tests (dltv.org)")


@pytest.fixture(autouse=True)
def _isolate_probe_and_cache(tmp_path, monkeypatch):
    # No test may ever reach dltv.org: block urlopen on every copy of
    # the module (`dltv_player_roles` and `base.dltv_player_roles` share
    # the urllib package object, but patch via each handle for safety).
    for _mod_name in ("dltv_player_roles", "base.dltv_player_roles"):
        try:
            __import__(_mod_name)
        except ImportError:
            pass
    for _name, _mod in list(sys.modules.items()):
        if _name.rsplit(".", 1)[-1] == "dltv_player_roles":
            monkeypatch.setattr(_mod.urllib.request, "urlopen", _deny_network)
    monkeypatch.setenv("DLTV_ROLE_CACHE_PATH", str(tmp_path / "dltv_cache.json"))
    monkeypatch.setattr(probe, "POSRES_JSONL", str(tmp_path / "posres.jsonl"))
    monkeypatch.delenv("DLTV_ROLE_LOOKUP", raising=False)
    monkeypatch.delenv("DLTV_ROLE_WEIGHT", raising=False)
    monkeypatch.delenv("DLTV_ROLE_LOOKUP_DEADLINE_S", raising=False)
    for _name, _mod in list(sys.modules.items()):
        if _name.rsplit(".", 1)[-1] == "dltv_player_roles" and hasattr(_mod, "_MEM_CACHE"):
            _mod._MEM_CACHE.clear()
    probe._POSRES_SEEN.clear()
    saved = (probe._PRO_POSITIONS_INDEX, probe._HERO_POS_COUNTS,
             probe._POS_OVERRIDES, probe._LAST_POS_RESOLUTION)
    probe._HERO_POS_COUNTS = {}
    probe._POS_OVERRIDES = {}
    probe._LAST_POS_RESOLUTION = {}
    try:
        yield
    finally:
        (probe._PRO_POSITIONS_INDEX, probe._HERO_POS_COUNTS,
         probe._POS_OVERRIDES, probe._LAST_POS_RESOLUTION) = saved
        for _name, _mod in list(sys.modules.items()):
            if _name.rsplit(".", 1)[-1] == "dltv_player_roles" and hasattr(_mod, "_MEM_CACHE"):
                _mod._MEM_CACHE.clear()


def _setup_incident_history():
    # BB 10x pos 3, DW 5x pos 4 on another team -> all-teams fallback,
    # exactly as the journal "история игрока"; others have no history.
    probe._PRO_POSITIONS_INDEX = {
        154921394: [(1000 - i, 4, 999) for i in range(5)],
        177411785: [(1000 - i, 3, 999) for i in range(10)],
    }
    stats = json.loads((BASE_DIR / "hero_position_stats.json").read_text(encoding="utf-8"))
    probe._HERO_POS_COUNTS = {
        int(h): {int(k): v.get("games", 0) for k, v in e.get("positions", {}).items()}
        for h, e in stats.items()
    }


def _pos_prob(hero_id, pos):
    counts = probe._HERO_POS_COUNTS[hero_id]
    return counts[pos] / sum(counts.values())


def _patch_default_fetch(monkeypatch, fetch):
    # The probe imports base.dltv_player_roles while this file imports the
    # top-level dltv_player_roles: two module objects when both sys.path
    # entries exist. Patch every loaded copy so the probe really sees it.
    for probe_name in ("base.dltv_player_roles", "dltv_player_roles"):
        try:
            __import__(probe_name)
        except ImportError:
            pass
    patched = set()
    for name, mod in list(sys.modules.items()):
        if name.rsplit(".", 1)[-1] == "dltv_player_roles" and hasattr(mod, "_default_fetch"):
            monkeypatch.setattr(mod, "_default_fetch", fetch)
            patched.add(name)
    assert patched, "no dltv_player_roles module loaded to patch"


def test_incident_replay_dltv_soft_vote_fixes_positions(monkeypatch):
    _setup_incident_history()
    _patch_default_fetch(monkeypatch, _fixture_fetch())
    result = probe.load_player_positions(
        [a for a, _ in DAXAK], [h for _, h in DAXAK],
        team_id=10271241, match_id=9013821098, side="dire")
    assert result == DAXAK_EXPECTED
    last = probe._LAST_POS_RESOLUTION
    assert last["method"] == "permutation"
    assert last["dltv_matched"] == 5
    assert last["raw_matched"] == 2
    assert last["raw_known"] == 2
    expected_conf = round((1 + 1 + _pos_prob(91, 1) + _pos_prob(49, 2)
                           + _pos_prob(68, 5)) / 5, 3)
    assert last["conf"] == expected_conf
    # stats_conf stays hero-stat only: the vote part is subtracted back out.
    expected_stats = round((_pos_prob(49, 2) + _pos_prob(68, 5) + _pos_prob(99, 3)
                            + _pos_prob(119, 4) + _pos_prob(91, 1)) / 5, 3)
    assert last["stats_conf"] == expected_stats


def test_kill_switch_reproduces_old_result(monkeypatch):
    _setup_incident_history()

    def _boom(account_id):
        raise AssertionError("network must not be touched with DLTV_ROLE_LOOKUP=0")

    monkeypatch.setenv("DLTV_ROLE_LOOKUP", "0")
    _patch_default_fetch(monkeypatch, _boom)
    result = probe.load_player_positions(
        [a for a, _ in DAXAK], [h for _, h in DAXAK],
        team_id=10271241, match_id=9013821098, side="dire")
    assert result == DAXAK_INCIDENT_OLD


def test_raw_path_never_touches_dltv(monkeypatch):
    aids = [101, 102, 103, 104, 105]
    probe._PRO_POSITIONS_INDEX = {
        aid: [(1000 - i, pos, 0) for i in range(4)]
        for aid, pos in zip(aids, [1, 2, 3, 4, 5])
    }
    calls = []
    _patch_default_fetch(monkeypatch, _fixture_fetch(calls))
    result = probe.load_player_positions(aids, [None] * 5, team_id=0,
                                         match_id=1, side="radiant")
    assert result == {aid: pos for aid, pos in zip(aids, [1, 2, 3, 4, 5])}
    assert probe._LAST_POS_RESOLUTION["method"] == "raw"
    assert calls == []


def _clear_role_caches():
    import os
    for _name, _mod in list(sys.modules.items()):
        if _name.rsplit(".", 1)[-1] == "dltv_player_roles" and hasattr(_mod, "_MEM_CACHE"):
            _mod._MEM_CACHE.clear()
    try:
        os.unlink(os.environ["DLTV_ROLE_CACHE_PATH"])
    except (KeyError, OSError):
        pass


def test_single_dltv_vote_does_not_override_agreeing_history(monkeypatch):
    _setup_incident_history()
    aids = [a for a, _ in DAXAK]
    hids = [h for _, h in DAXAK]

    # Stub fetch BEFORE the baseline call: unknown for everyone, so the
    # baseline resolves purely from player history / hero stats.
    def empty_fetch(account_id, timeout=None):
        return {"teams": [], "players": []}

    _patch_default_fetch(monkeypatch, empty_fetch)
    baseline = probe.load_player_positions(
        aids, hids, team_id=10271241, match_id=2, side="dire")
    assert baseline[177411785] == 3
    assert probe._LAST_POS_RESOLUTION["dltv_known"] == 0

    # Fresh caches between the two calls so the vote below is really fetched.
    _clear_role_caches()

    calls = []

    def fetch(account_id, timeout=None):
        # Only BB gets a (wrong) DLTV role; everyone else unknown.
        calls.append(account_id)
        if account_id == 177411785:
            return {"teams": [], "players": [{"steam_id": 177411785, "role": 5}]}
        return {"teams": [], "players": []}

    _patch_default_fetch(monkeypatch, fetch)
    probe._POSRES_SEEN.clear()
    voted = probe.load_player_positions(
        aids, hids, team_id=10271241, match_id=2, side="dire")
    assert voted == baseline
    assert voted[177411785] == 3
    # The vote was really exercised: all 5 ids fetched, exactly BB known.
    assert sorted(calls) == sorted(aids)
    assert probe._LAST_POS_RESOLUTION["dltv_known"] == 1


def test_not_found_and_error_ttl_and_deadline(monkeypatch, tmp_path):
    notfound = json.loads((FIXTURES / "search_111111111_notfound.json").read_text())
    assert roles.parse_search_role(notfound, 111111111) is None
    assert roles.lookup_roles([111111111], fetch=lambda aid: notfound) == {}

    # Fetch raising -> no role, and no re-fetch within the 10-min error TTL.
    calls = []
    clock = [1000.0]

    def boom(aid):
        calls.append(aid)
        raise ConnectionError("dltv down")

    assert roles.lookup_roles([555], fetch=boom, now=lambda: clock[0]) == {}
    assert roles.lookup_roles([555], fetch=boom, now=lambda: clock[0]) == {}
    assert len(calls) == 1
    clock[0] += 10 * 60 + 1
    assert roles.lookup_roles([555], fetch=boom, now=lambda: clock[0]) == {}
    assert len(calls) == 2

    # Hit TTL: a found role is served from cache without re-fetch.
    payload = {"teams": [], "players": [{"steam_id": 777, "role": 2}]}
    hit_calls = []
    fetch = lambda aid: (hit_calls.append(aid), payload)[1]
    assert roles.lookup_roles([777], fetch=fetch, now=lambda: clock[0]) == {777: 2}
    assert roles.lookup_roles([777], fetch=fetch, now=lambda: clock[0]) == {777: 2}
    assert len(hit_calls) == 1

    # Deadline: a slow fetch clock stops the batch after the first id.
    slow_clock = [0.0]

    def slow_fetch(aid):
        slow_clock[0] += 10.0
        return {"teams": [], "players": [{"steam_id": aid, "role": 1}]}

    roles._MEM_CACHE.clear()
    got = roles.lookup_roles([901, 902, 903], fetch=slow_fetch,
                             now=lambda: slow_clock[0], deadline_s=5)
    assert got == {901: 1}


def test_deadline_skips_cached_as_errors_and_timeout_bounded():
    clock = [1000.0]
    seen = []

    def fetch(aid, timeout=None):
        seen.append((aid, timeout, clock[0]))
        clock[0] += 5.0  # each fetch costs 5 s of fake time
        return {"teams": [], "players": [{"steam_id": aid, "role": 2}]}

    got = roles.lookup_roles([901, 902, 903], fetch=fetch,
                             now=lambda: clock[0], deadline_s=6)
    # 1st id: remaining 6 -> timeout 4.0; 2nd: remaining 1 -> timeout 1.0;
    # 3rd never reached -> cached as error, not fetched.
    assert got == {901: 2, 902: 2}
    assert [s[0] for s in seen] == [901, 902]
    assert seen[0][1] == 4.0
    assert seen[1][1] == 1.0
    for _aid, _timeout, _at in seen:
        assert _timeout <= max(0.5, 6 - (_at - 1000.0)) + 1e-9

    # 2nd call within the error TTL: 901/902 are hits, 903 is a cached
    # error -> zero fetches (an outage stalls only once).
    seen.clear()
    got2 = roles.lookup_roles([901, 902, 903], fetch=fetch,
                              now=lambda: clock[0])
    assert got2 == {901: 2, 902: 2}
    assert seen == []


def test_role_weight_rejects_nonfinite_and_negative(monkeypatch):
    assert roles.role_weight() == 0.2
    for bad in ("nan", "NaN", "inf", "-inf", "Infinity", "-1", "-0.5",
                "nope", ""):
        monkeypatch.setenv("DLTV_ROLE_WEIGHT", bad)
        assert roles.role_weight() == 0.2, bad
    for good, want in (("0", 0.0), ("0.5", 0.5), ("2", 2.0)):
        monkeypatch.setenv("DLTV_ROLE_WEIGHT", good)
        assert roles.role_weight() == want, good


def test_nan_weight_keeps_default_vote(monkeypatch):
    # nan/inf must not turn the resolver into a fallback: with the
    # default weight the incident replay still resolves via the vote.
    _setup_incident_history()
    monkeypatch.setenv("DLTV_ROLE_WEIGHT", "nan")
    _patch_default_fetch(monkeypatch, _fixture_fetch())
    result = probe.load_player_positions(
        [a for a, _ in DAXAK], [h for _, h in DAXAK],
        team_id=10271241, match_id=9013821098, side="dire")
    assert result == DAXAK_EXPECTED


def test_parse_search_role_ignores_fuzzy_and_bad_roles():
    payload = {"teams": [], "players": [
        {"steam_id": 999999999, "role": 1},
        {"steam_id": 212037295, "role": 9},
        {"steam_id": "212037295", "role": "5"},
    ]}
    assert roles.parse_search_role(payload, 212037295) == 5
    assert roles.parse_search_role(payload, 999999999) == 1
    assert roles.parse_search_role({"teams": [], "players": []}, 1) is None
    assert roles.parse_search_role({}, 1) is None
    assert roles.parse_search_role(None, 1) is None
    assert roles.parse_search_role(
        {"players": [{"steam_id": 1, "role": 0}]}, 1) is None
    assert roles.parse_search_role(
        {"players": [{"steam_id": 1, "role": 6}]}, 1) is None
