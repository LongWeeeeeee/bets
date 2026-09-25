"""Team-pair+time fallback membership proof for the live-ledger rebase.

An old live ledger entry can carry a DLTV series id or a fixed placeholder in
`match_id` instead of the Valve match id (E-293-adjacent finding, 16.09.2026:
replaying serv1's ledger against a fresh snapshot classified 65/159 applied
entries as unprovable `error`, 55 of them keyed `dltv.org/matches/<series_id>.0`
with `match_id` == the series id, one with a fixed placeholder `1700000000`).
Exact-id snapshot membership then always misses even when the match is
present.  `ELO.live_team_strength` now also emits `meta.recent_completed_match_keys`
(unordered team pair + start timestamp) so the rebase guard can prove or
disprove membership by team pair within a bounded tolerance before giving up.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import ELO.live_team_strength as lts  # noqa: E402
from ELO.rebase_runtime_model_state import main as rebase_main  # noqa: E402
from ELO.config import HybridEloConfig  # noqa: E402
from ELO.domain import LeagueTier, MatchRecord  # noqa: E402
from ELO.models import HybridPlayerRosterEloModel  # noqa: E402


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _live_record(match_id: int, *, start: int, duration: int | None = 60,
                  radiant_team_id: int | None = 101, dire_team_id: int | None = 202) -> MatchRecord:
    return MatchRecord(
        match_id=match_id, timestamp=start, radiant_win=True,
        radiant_team_id=radiant_team_id, radiant_team_name="Radiant",
        dire_team_id=dire_team_id, dire_team_name="Dire",
        radiant_player_ids=(1, 2, 3, 4, 5), dire_player_ids=(6, 7, 8, 9, 10),
        league_id=1, league_name="Test League", source_league_tier="Tier 2",
        series_id=77, series_type="bo3",
        radiant_player_positions=("1", "2", "3", "4", "5"),
        dire_player_positions=("1", "2", "3", "4", "5"),
        radiant_kills=30, dire_kills=20, source_patch="7.41",
        derived_league_tier=LeagueTier.TIER2, duration_seconds=duration,
    )


def _applied_entry(record: MatchRecord, *, observed_at: int) -> dict:
    return {
        "match_id": record.match_id, "radiant_win": True,
        "applied_at": observed_at, "result_timestamp": observed_at,
        "match_record": lts._serialize_match_record(record),
    }


def _ledger_snapshot(tmp_path: Path, *, reference: int = 1_000, coverage_since: int | None = None,
                      completed_ids: list[int] | None = None,
                      keys: list[list[int]] | None = None,
                      keys_tolerance: int | None = None) -> tuple[Path, dict]:
    state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    meta = {
        "reference_timestamp": reference,
        "model_config_signature": "same-history-signature",
        "recent_completed_match_ids_coverage_since": (
            coverage_since if coverage_since is not None else reference - 7 * 86400
        ),
        "recent_completed_match_ids": completed_ids or [],
    }
    if keys is not None:
        meta["recent_completed_match_keys"] = keys
    if keys_tolerance is not None:
        meta["recent_completed_match_keys_tolerance_seconds"] = keys_tolerance
    payload = {"meta": meta, "model_state": state}
    path = tmp_path / "ledger_snapshot.json"
    _write(path, payload)
    return path, payload


def test_placeholder_id_entry_is_covered_by_team_pair_and_time(tmp_path) -> None:
    # E.g. serv1 entry with match_id 1700000000: duration unknown (the common
    # score-observation path), so exact-id and the existing duration-based
    # window fallback both miss; only the team-pair+time proof can cover it.
    record = _live_record(1_700_000_000, start=900, duration=None)
    snapshot_path, snapshot = _ledger_snapshot(
        tmp_path, completed_ids=[], keys=[[9_999_000_001, 101, 202, 900]], keys_tolerance=10_800,
    )
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": HybridPlayerRosterEloModel(HybridEloConfig()).export_state()})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {},
                           "applied_maps": {"placeholder": _applied_entry(record, observed_at=950)}})

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    assert json.loads(state_path.read_text(encoding="utf-8"))["model_state"] == snapshot["model_state"]
    covered = json.loads(progress_path.read_text(encoding="utf-8"))["applied_maps"]
    assert covered["placeholder"] == {
        "match_id": 1_700_000_000,
        "snapshot_covered": True,
        "snapshot_covered_reference": 1_000,
        "snapshot_covered_match_id": 9_999_000_001,
    }


def test_pair_outside_tolerance_but_inside_window_attempts_unsafe_replay(
        tmp_path, capsys, monkeypatch) -> None:
    # Same team pair as the snapshot key, but 200s away while the snapshot's
    # tolerance is only 60s: the pair proof must not fire.  The entry's own
    # result_timestamp (750) then lies inside [coverage_since, snapshot_reference]
    # (window exactness: not listed => not in snapshot), so the guard routes
    # this into the same replay path as an exact-id absence proof instead of
    # the generic "unprovable membership" raise.  Replaying an event whose
    # own processing timestamp predates the new baseline would still corrupt
    # ELO event order under the strict (opt-in) order guard, so this test
    # forces strict mode to keep asserting the refusal -- default behaviour
    # is now lenient late replay (LIVE_ELO_REBASE_STRICT_ORDER).
    monkeypatch.setattr(lts, "LIVE_ELO_REBASE_STRICT_ORDER", True)
    record = _live_record(1_700_000_002, start=700, duration=None)
    snapshot_path, _snapshot = _ledger_snapshot(
        tmp_path, coverage_since=0, completed_ids=[],
        keys=[[9_999_000_002, 101, 202, 500]], keys_tolerance=60,
    )
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": {"legacy": True}})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {},
                           "applied_maps": {"map-1": _applied_entry(record, observed_at=750)}})
    before = state_path.read_bytes(), progress_path.read_bytes()

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    err = capsys.readouterr().err
    assert "без изменения порядка ELO-событий" in err
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before


def test_keys_absent_raises_with_original_message_prefix(tmp_path, capsys) -> None:
    # Old snapshot built before this change: no `recent_completed_match_keys`
    # at all.  `_snapshot_recent_completed_keys` must return None (not an
    # empty map), and the guard must keep raising with the original prefix so
    # older logs stay comparable.
    record = _live_record(1_700_000_003, start=700, duration=None)
    snapshot_path, _snapshot = _ledger_snapshot(tmp_path, completed_ids=[])
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": {"legacy": True}})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {},
                           "applied_maps": {"map-1": _applied_entry(record, observed_at=750)}})
    before = state_path.read_bytes(), progress_path.read_bytes()

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    err = capsys.readouterr().err
    assert "а снимок не доказывает её membership; перебазировка отменена" in err
    assert "recent_completed_match_keys" in err
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before


def test_missing_team_ids_skip_pair_proof_and_still_raise(tmp_path, capsys) -> None:
    # Keys are present, but this entry has no team ids at all (some legacy
    # rows never recorded them), so the pair proof cannot even be attempted.
    # A tight coverage_since also keeps the window-replay fallback from
    # firing, so this must fall through to the original raise, unaided by
    # team ids it does not have.
    record = _live_record(1_700_000_004, start=800, duration=None,
                          radiant_team_id=None, dire_team_id=None)
    snapshot_path, _snapshot = _ledger_snapshot(
        tmp_path, coverage_since=990, completed_ids=[],
        keys=[[9_999_000_004, 101, 202, 800]], keys_tolerance=10_800,
    )
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": {"legacy": True}})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {},
                           "applied_maps": {"map-1": _applied_entry(record, observed_at=800)}})
    before = state_path.read_bytes(), progress_path.read_bytes()

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    err = capsys.readouterr().err
    assert "а снимок не доказывает её membership; перебазировка отменена" in err
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before


def test_builder_emits_keys_aligned_one_to_one_with_ids(tmp_path) -> None:
    matches = [
        MatchRecord(
            match_id=1, timestamp=100, radiant_win=True,
            radiant_team_id=11, radiant_team_name="A", dire_team_id=22, dire_team_name="B",
            radiant_player_ids=(), dire_player_ids=(), league_id=1, league_name="L",
            source_league_tier="TIER2", series_id=1, series_type="1",
            derived_league_tier=LeagueTier.TIER2, duration_seconds=30,
        ),
        MatchRecord(
            match_id=2, timestamp=200, radiant_win=True,
            radiant_team_id=33, radiant_team_name="C", dire_team_id=44, dire_team_name="D",
            radiant_player_ids=(), dire_player_ids=(), league_id=1, league_name="L",
            source_league_tier="TIER2", series_id=2, series_type="1",
            derived_league_tier=LeagueTier.TIER2, duration_seconds=30,
        ),
        # Outside the coverage window: must be absent from both ids and keys.
        MatchRecord(
            match_id=3, timestamp=1, radiant_win=True,
            radiant_team_id=55, radiant_team_name="E", dire_team_id=66, dire_team_name="F",
            radiant_player_ids=(), dire_player_ids=(), league_id=1, league_name="L",
            source_league_tier="TIER2", series_id=3, series_type="1",
            derived_league_tier=LeagueTier.TIER2, duration_seconds=30,
        ),
    ]
    meta = lts._recent_completed_match_meta(matches, coverage_since=100, reference_timestamp=300)

    assert meta["recent_completed_match_ids"] == [1, 2]
    assert meta["recent_completed_match_keys"] == sorted([[1, 11, 22, 100], [2, 33, 44, 200]])
    assert len(meta["recent_completed_match_keys"]) == len(meta["recent_completed_match_ids"])
    assert {row[0] for row in meta["recent_completed_match_keys"]} == set(meta["recent_completed_match_ids"])
    assert meta["recent_completed_match_keys_tolerance_seconds"] == (
        lts.SNAPSHOT_RECENT_COMPLETED_MATCH_KEYS_TOLERANCE_SECONDS
    )


# --------------------------------------------------------------------------- #
# known_team_ids: third proof — neither team has any base trace (17.09.2026,
# amateur/pub leagues like "WINLINE Star Series" whose team_id never appears
# in the base at all: id proof and team-pair+time proof both miss forever,
# not just outside tolerance, because the base never saw these teams).
# --------------------------------------------------------------------------- #

def test_both_teams_unknown_to_base_replays_regardless_of_cutoff() -> None:
    # Neither team has any base rating/roster trace, so there is no base
    # event of theirs to interleave with: the order-safety gate is moot, and
    # this must replay even though result_timestamp (600) is well before the
    # cutoff (1000) -- which would raise without `known_team_ids`.
    record = _live_record(9_500_001, start=500, duration=None,
                          radiant_team_id=999_001, dire_team_id=999_002)
    raw = _applied_entry(record, observed_at=600)

    action, match, result_timestamp = lts._applied_entry_for_rebase(
        raw, snapshot_reference=1_000,
        recent_completed_ids=None, recent_completed_keys=None,
        known_team_ids={12_345},
    )

    assert action == "replay_unknown_teams"
    assert match is not None and match.match_id == 9_500_001
    assert result_timestamp == 600


def test_one_team_known_and_unproven_still_raises() -> None:
    # Radiant is unknown to the base, but dire IS known: the new rule needs
    # BOTH sides absent, so this must still fall through to the original
    # "membership not proven" raise instead of replaying unconditionally.
    record = _live_record(9_500_002, start=500, duration=None,
                          radiant_team_id=999_001, dire_team_id=12_345)
    raw = _applied_entry(record, observed_at=600)

    with pytest.raises(lts.RuntimeRebaseError, match="не доказывает её membership"):
        lts._applied_entry_for_rebase(
            raw, snapshot_reference=1_000,
            recent_completed_ids=None, recent_completed_keys=None,
            known_team_ids={12_345},
        )


def test_known_team_ids_none_keeps_old_behaviour() -> None:
    # `known_team_ids=None` (the default) means the new rule is inactive: an
    # otherwise identical pre-cutoff, unprovable entry must still raise
    # exactly as it did before this change.
    record = _live_record(9_500_003, start=500, duration=None,
                          radiant_team_id=999_001, dire_team_id=999_002)
    raw = _applied_entry(record, observed_at=600)

    with pytest.raises(lts.RuntimeRebaseError, match="не доказывает её membership"):
        lts._applied_entry_for_rebase(
            raw, snapshot_reference=1_000,
            recent_completed_ids=None, recent_completed_keys=None,
        )


# --------------------------------------------------------------------------- #
# Independent review (E-294, 17.09.2026): proof ORDER (id -> pair -> unknown
# -teams -> window-gated replay/raise), a registry-incomplete veto, and
# one-to-one key consumption so a fixed tolerance stays safe for two maps of
# one series close together.
# --------------------------------------------------------------------------- #

def test_pending_unknown_teams_but_id_covered_returns_covered() -> None:
    # FIX 1 + FIX 4(a): exact-id proof must win even when both teams would
    # otherwise qualify for the "unknown to base" rule.
    record = _live_record(9_500_030, start=500, duration=None,
                          radiant_team_id=999_001, dire_team_id=999_002)
    pending_map = {
        "map_key": "dltv.org/matches/9500030.0",
        "match_record": lts._serialize_match_record(record),
    }

    action, match = lts._pending_entry_for_rebase(
        pending_map, snapshot_reference=1_000,
        recent_completed_ids=(0, {9_500_030}),
        recent_completed_keys=None,
        known_team_ids={12_345},
    )

    assert action == "covered"
    assert match is not None and match.match_id == 9_500_030


def test_id_proof_wins_over_unknown_teams_rule() -> None:
    # FIX 1 + FIX 4(c): same as above for the applied-map guard.
    record = _live_record(9_500_010, start=500, duration=None,
                          radiant_team_id=999_001, dire_team_id=999_002)
    raw = _applied_entry(record, observed_at=600)

    action, match, result_timestamp = lts._applied_entry_for_rebase(
        raw, snapshot_reference=1_000,
        recent_completed_ids=(0, {9_500_010}),
        recent_completed_keys=None,
        known_team_ids={12_345},
    )

    assert action == "covered"


def test_two_maps_of_one_pair_forty_minutes_apart_consume_key_once(monkeypatch) -> None:
    # FIX 3 + FIX 4(b): the snapshot lists only the FIRST real map's key for
    # pair (101, 202); the second real map (2400s = 40 min later, well
    # inside the 10800s tolerance) has no key of its own -- e.g. it hadn't
    # finished when the snapshot was built.  Without one-to-one consumption
    # the second entry would ALSO match the first map's key by proximity
    # (2400s <= 10800s), wrongly marking it "covered".  Sharing one
    # `consumed` set across both calls (as `rebase_runtime_model_state`
    # does) is what makes a single fixed tolerance safe here. Strict mode is
    # forced so the second, still-unprovable entry keeps raising instead of
    # the new default lenient late replay (LIVE_ELO_REBASE_STRICT_ORDER).
    monkeypatch.setattr(lts, "LIVE_ELO_REBASE_STRICT_ORDER", True)
    recent_completed_keys = (10_800, {9_999_000_010: (101, 202, 1_000)})
    recent_completed_ids = (0, set())
    consumed: set[int] = set()

    first = _live_record(1_700_000_010, start=1_000, duration=None,
                         radiant_team_id=101, dire_team_id=202)
    action1, match1, _rt1 = lts._applied_entry_for_rebase(
        _applied_entry(first, observed_at=1_200), snapshot_reference=100_000,
        recent_completed_ids=recent_completed_ids,
        recent_completed_keys=recent_completed_keys,
        consumed=consumed,
    )
    assert action1 == "covered"
    assert consumed == {9_999_000_010}

    second = _live_record(1_700_000_011, start=1_000 + 2_400, duration=None,
                          radiant_team_id=101, dire_team_id=202)
    with pytest.raises(lts.RuntimeRebaseError, match="без изменения порядка"):
        lts._applied_entry_for_rebase(
            _applied_entry(second, observed_at=1_200 + 2_400), snapshot_reference=100_000,
            recent_completed_ids=recent_completed_ids,
            recent_completed_keys=recent_completed_keys,
            consumed=consumed,
        )


def test_team_present_in_keys_but_absent_from_registry_counts_as_known() -> None:
    # FIX 2 + FIX 4(d): team 555555 is not in the static TEAM_ID_TO_ORG_KEY
    # registry (unit-level: no `model_state` needed here at all), but it DOES
    # have a row in `recent_completed_match_keys` -- real corpus history --
    # so it must veto the "both unknown" rule exactly like a registry hit.
    recent_completed_keys = (10_800, {9_999_000_020: (555_555, 202, 1_000)})
    known = lts._known_team_ids_from_model_state({}, recent_completed_keys)
    assert 555_555 in known and 202 in known

    # End to end: radiant (555555) is known via this union, dire (999999) is
    # not -- "at least one known" must keep the old id/pair/raise pipeline,
    # never falling into "replay_unknown_teams".
    record = _live_record(9_500_020, start=5_000, duration=None,
                          radiant_team_id=555_555, dire_team_id=999_999)
    raw = _applied_entry(record, observed_at=6_000)

    with pytest.raises(lts.RuntimeRebaseError, match="не доказывает её membership"):
        lts._applied_entry_for_rebase(
            raw, snapshot_reference=5_050,
            recent_completed_ids=(0, set()),
            recent_completed_keys=recent_completed_keys,
            known_team_ids=known,
        )


# --------------------------------------------------------------------------- #
# Config-hash guard (E-294 follow-up, 17.09.2026): `meta.model_config_signature`
# is `_rating_history_signature` (hash of ALL matches) and changes on every
# nightly rebuild by design, so comparing it against outstanding live work
# blocked every rebase 12-16.09.2026.  The guard now compares
# `_model_config_signature(model_state.config)` of the old runtime state vs
# the new snapshot instead, and only raises when the MODEL config (not the
# match history) actually changed.
# --------------------------------------------------------------------------- #

def test_config_hash_guard_allows_rebase_when_only_history_signature_changed(tmp_path) -> None:
    old_state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    new_state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    completed = _live_record(9_600_001, start=500)

    snapshot_path = tmp_path / "snapshot.json"
    _write(snapshot_path, {
        "meta": {"reference_timestamp": 2_000, "model_config_signature": "new-history-signature",
                 "recent_completed_match_ids_coverage_since": 0,
                 "recent_completed_match_ids": [9_600_001]},
        "model_state": new_state,
    })
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "old-history-signature",
                        "model_state": old_state})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "old-history-signature",
                           "pending_series": {},
                           "applied_maps": {"map-1": _applied_entry(completed, observed_at=1_500)}})

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    new_progress = json.loads(progress_path.read_text(encoding="utf-8"))
    assert new_progress["base_reference_timestamp"] == 2_000
    assert new_progress["base_model_config_signature"] == "new-history-signature"


def test_config_hash_guard_blocks_rebase_when_model_config_changed(tmp_path, capsys) -> None:
    old_state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    new_state = HybridPlayerRosterEloModel(HybridEloConfig(elo_scale=999.0)).export_state()
    completed = _live_record(9_600_002, start=500)

    snapshot_path = tmp_path / "snapshot.json"
    _write(snapshot_path, {
        "meta": {"reference_timestamp": 2_000, "model_config_signature": "new-history-signature-2",
                 "recent_completed_match_ids_coverage_since": 0,
                 "recent_completed_match_ids": [9_600_002]},
        "model_state": new_state,
    })
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "old-history-signature-2",
                        "model_state": old_state})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "old-history-signature-2",
                           "pending_series": {},
                           "applied_maps": {"map-1": _applied_entry(completed, observed_at=1_500)}})
    before = state_path.read_bytes(), progress_path.read_bytes()

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    err = capsys.readouterr().err
    assert "конфиг модели изменился" in err
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before


@pytest.mark.parametrize("old_has_a", [False, True])
def test_a_snapshot_rebases_old_and_new_progress_without_config_signature_change(tmp_path, old_has_a):
    old_state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    if not old_has_a:
        for key in list(old_state):
            if key.startswith("a_") or key.startswith("player_a"):
                del old_state[key]
        del old_state["k24_contract"]
    new_state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    assert lts._model_config_signature(old_state) == lts._model_config_signature(new_state)
    completed = _live_record(9_600_101, start=500)
    snapshot_path = tmp_path / "snapshot.json"
    _write(snapshot_path, {
        "meta": {"reference_timestamp": 2_000, "model_config_signature": "new-history-a",
                 "recent_completed_match_ids_coverage_since": 0,
                 "recent_completed_match_ids": [completed.match_id]},
        "model_state": new_state,
    })
    state_path, progress_path = tmp_path / "state.json", tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "old-history-k24",
                        "model_state": old_state})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "old-history-k24",
                           "pending_series": {},
                           "applied_maps": {"map-1": _applied_entry(completed, observed_at=1_500)}})
    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    rebased = json.loads(state_path.read_text())
    assert rebased["model_state"]["a_schema_version"] == 1
    assert rebased["base_model_config_signature"] == "new-history-a"
    assert json.loads(progress_path.read_text())["applied_maps"]["map-1"]["snapshot_covered"]


def test_rebase_rejects_changed_k24_contract_with_outstanding_work(tmp_path, capsys):
    old_state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    new_state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    new_state["k24_contract"]["k"] = 30.0
    assert lts._model_config_signature(old_state) != lts._model_config_signature(new_state)
    completed = _live_record(9_600_102, start=500)
    snapshot_path, state_path, progress_path = [tmp_path / name for name in
                                                 ("snapshot.json", "state.json", "progress.json")]
    _write(snapshot_path, {"meta": {"reference_timestamp": 2_000,
                                    "model_config_signature": "changed-k24-history"},
                           "model_state": new_state})
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "old-k24-history", "model_state": old_state})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "old-k24-history",
                           "pending_series": {},
                           "applied_maps": {"map-1": _applied_entry(completed, observed_at=1_500)}})
    before = state_path.read_bytes(), progress_path.read_bytes()
    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    assert "конфиг модели изменился" in capsys.readouterr().err
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before
