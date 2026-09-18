"""Память живого ELO-пути: кэш разбора рантайм-состояния и его перебазировка.

Оба механизма — продолжение E-251 (slim-загрузка снимка). Замер 03.09.2026 на
проде: RSS процесса 6.42 ГБ и пик 10.74 ГБ при том, что slim-загрузка снимка
стоит 0.83 ГБ, а json.load всего снимка — 3.69 ГБ. Разницу дали две вещи,
которые эти тесты закрепляют:

* `_load_json_dict` парсил `live_elo_model_state.json` (519 МБ) ЗАНОВО на каждом
  вызове, а `_load_runtime_model_payload` зовётся до трёх раз на завершённую
  карту (`:627`, `:1722`, `:1891`) — два-три разбора по ~2.5-3 ГБ временных
  словарей на карту, и RSS не возвращается (арены glibc фрагментированы);
* после ночной доставки снимка `base_reference_timestamp` расходился, payload
  отклонялся (`:590-603`), и процесс уходил в `full_model_state()` — разбор
  всего model_state из снимка. В логе прода три строки «[ELO] догружаю полный
  model_state», по одной на каждую доставку.

Кэш безопасен, потому что `from_state` КОПИРУЕТ словари состояния
(`models.py:571-579, 657, 662`), а обратная запись строит НОВЫЙ payload через
`export_state()` (`:1918-1922`) — закэшированный dict никто не мутирует.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import ELO.live_team_strength as lts  # noqa: E402
from ELO.rebase_runtime_model_state import main as rebase_main  # noqa: E402
from ELO.domain import LeagueTier, MatchRecord  # noqa: E402
from ELO.config import HybridEloConfig  # noqa: E402
from ELO.models import HybridPlayerRosterEloModel  # noqa: E402
from ELO.replay import result_record  # noqa: E402
from ELO import state_overlay  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_json_cache():
    lts._JSON_DICT_CACHE.clear()
    yield
    lts._JSON_DICT_CACHE.clear()


def _write(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _snapshot(tmp_path: Path, reference: int = 1788387568,
              signature: str = "sig-ca41da66", *, with_state: bool = True) -> Path:
    snap = tmp_path / "snapshot.json"
    payload = {
        "meta": {
            "reference_timestamp": reference,
            "model_config_signature": signature,
            "reference_utc": "2026-09-02T22:19:28+00:00",
        },
        "teams_by_org_key": {"org:tundra": {"team_id": 8291895, "tier": "TIER1"}},
        "team_kills_history_by_team_id": {"8291895": [{"match_id": 1, "kills": 30}]},
    }
    if with_state:
        payload["model_state"] = {
            "config": {"k_global": 24.0},
            "player_global": {"11": 1612.5, "22": 1488.0},
        }
    _write(snap, payload)
    return snap


# --------------------------------------------------------------------------- #
# кэш разбора
# --------------------------------------------------------------------------- #

def test_load_json_dict_reuses_parse_until_file_changes(tmp_path) -> None:
    path = tmp_path / "state.json"
    _write(path, {"base_reference_timestamp": 1, "model_state": {"a": 1}})

    first = lts._load_json_dict(path)
    second = lts._load_json_dict(path)
    assert first is second, "повторный вызов обязан отдать кэш, а не парсить заново"

    _write(path, {"base_reference_timestamp": 2, "model_state": {"a": 2, "b": 3}})
    third = lts._load_json_dict(path)
    assert third is not first
    assert third["base_reference_timestamp"] == 2


def test_load_json_dict_survives_same_size_content_change(tmp_path) -> None:
    """Кэш ключуется mtime_ns И размером: равный размер не должен означать «то же»."""
    path = tmp_path / "state.json"
    _write(path, {"v": 1})
    first = lts._load_json_dict(path)

    _write(path, {"v": 2})                      # размер тот же, содержимое другое
    os.utime(path, ns=(1_800_000_000_000_000_000, 1_800_000_000_000_000_000))
    second = lts._load_json_dict(path)

    assert second == {"v": 2}
    assert second is not first


def test_load_json_dict_missing_and_non_dict(tmp_path) -> None:
    assert lts._load_json_dict(tmp_path / "нет.json") is None
    bad = tmp_path / "list.json"
    _write(bad, [1, 2, 3])
    assert lts._load_json_dict(bad) is None
    broken = tmp_path / "broken.json"
    broken.write_text("{не json", encoding="utf-8")
    assert lts._load_json_dict(broken) is None


# --------------------------------------------------------------------------- #
# перебазировка рантайм-состояния
# --------------------------------------------------------------------------- #

def test_rebase_creates_state_when_absent(tmp_path, capsys) -> None:
    snap = _snapshot(tmp_path)
    state = tmp_path / "live_elo_model_state.json"
    progress = tmp_path / "live_elo_progress.json"

    assert rebase_main(["--snapshot", str(snap), "--state", str(state),
                        "--progress", str(progress)]) == 0

    payload = json.loads(state.read_text(encoding="utf-8"))
    assert payload["base_reference_timestamp"] == 1788387568
    assert payload["base_model_config_signature"] == "sig-ca41da66"
    assert payload["model_state"]["player_global"] == {"11": 1612.5, "22": 1488.0}
    assert "перебазировано" in capsys.readouterr().out


def test_rebase_is_noop_when_base_already_matches(tmp_path, capsys) -> None:
    """Главный guard: иначе перебазировка выбросила бы живые обновления рейтингов."""
    snap = _snapshot(tmp_path)
    state = tmp_path / "live_elo_model_state.json"
    progress = tmp_path / "live_elo_progress.json"
    live = {
        "base_reference_timestamp": 1788387568,
        "base_model_config_signature": "sig-ca41da66",
        "updated_at": 1,
        "model_state": {"config": {"k_global": 24.0},
                        "player_global": {"11": 1700.0}},  # живой апдейт рейтинга
    }
    _write(state, live)
    before = state.read_bytes()

    assert rebase_main(["--snapshot", str(snap), "--state", str(state),
                        "--progress", str(progress)]) == 0

    assert state.read_bytes() == before, "файл не должен быть тронут"
    assert json.loads(state.read_text())["model_state"]["player_global"]["11"] == 1700.0
    assert "не тронуто" in capsys.readouterr().out


def test_rebase_rewrites_when_base_diverged(tmp_path) -> None:
    snap = _snapshot(tmp_path, reference=1788387568, signature="sig-new")
    state = tmp_path / "live_elo_model_state.json"
    progress = tmp_path / "live_elo_progress.json"
    _write(state, {
        "base_reference_timestamp": 1788295911,       # старый срез
        "base_model_config_signature": "sig-old",
        "updated_at": 1,
        "model_state": {"config": {}, "player_global": {"11": 1500.0}},
    })

    assert rebase_main(["--snapshot", str(snap), "--state", str(state),
                        "--progress", str(progress)]) == 0

    payload = json.loads(state.read_text(encoding="utf-8"))
    assert payload["base_reference_timestamp"] == 1788387568
    assert payload["base_model_config_signature"] == "sig-new"
    assert payload["model_state"]["player_global"]["11"] == 1612.5


def test_rebase_force_overrides_matching_base(tmp_path) -> None:
    snap = _snapshot(tmp_path)
    state = tmp_path / "live_elo_model_state.json"
    progress = tmp_path / "live_elo_progress.json"
    _write(state, {
        "base_reference_timestamp": 1788387568,
        "base_model_config_signature": "sig-ca41da66",
        "updated_at": 1,
        "model_state": {"player_global": {"11": 1700.0}},
    })

    assert rebase_main(["--snapshot", str(snap), "--state", str(state),
                        "--progress", str(progress), "--force"]) == 0
    assert json.loads(state.read_text())["model_state"]["player_global"]["11"] == 1612.5


def test_rebase_fails_loudly_without_model_state(tmp_path, capsys) -> None:
    snap = _snapshot(tmp_path, with_state=False)
    state = tmp_path / "live_elo_model_state.json"
    progress = tmp_path / "live_elo_progress.json"

    assert rebase_main(["--snapshot", str(snap), "--state", str(state),
                        "--progress", str(progress)]) == 1
    assert "нет model_state" in capsys.readouterr().err
    assert not state.exists()


# --------------------------------------------------------------------------- #
# live-ledger rebase
# --------------------------------------------------------------------------- #

def _live_record(match_id: int, *, start: int, duration: int | None = 60) -> MatchRecord:
    return MatchRecord(
        match_id=match_id,
        timestamp=start,
        radiant_win=True,
        radiant_team_id=101,
        radiant_team_name="Radiant",
        dire_team_id=202,
        dire_team_name="Dire",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=1,
        league_name="Test League",
        source_league_tier="Tier 2",
        series_id=77,
        series_type="bo3",
        radiant_player_positions=("1", "2", "3", "4", "5"),
        dire_player_positions=("1", "2", "3", "4", "5"),
        radiant_kills=30,
        dire_kills=20,
        source_patch="7.41",
        derived_league_tier=LeagueTier.TIER2,
        duration_seconds=duration,
    )


def _ledger_snapshot(tmp_path: Path, *, reference: int = 1_000,
                     completed_ids: list[int] | None = None,
                     keys: list[list[int]] | None = None,
                     keys_tolerance: int | None = None) -> tuple[Path, dict]:
    state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    meta = {
        "reference_timestamp": reference,
        "model_config_signature": "same-history-signature",
        "recent_completed_match_ids_coverage_since": reference - 7 * 86400,
        "recent_completed_match_ids": completed_ids or [],
    }
    # `keys`/`keys_tolerance` mirror the team-pair+time proof structure used by
    # ELO/tests/test_rebase_membership_keys.py (`recent_completed_match_keys`):
    # [[match_id, radiant_team_id, dire_team_id, timestamp], ...]. Optional and
    # unused by default so every pre-existing caller keeps its old (ids-only)
    # snapshot shape.
    if keys is not None:
        meta["recent_completed_match_keys"] = keys
    if keys_tolerance is not None:
        meta["recent_completed_match_keys_tolerance_seconds"] = keys_tolerance
    payload = {"meta": meta, "model_state": state}
    path = tmp_path / "ledger_snapshot.json"
    _write(path, payload)
    return path, payload


def _applied_entry(record: MatchRecord, *, observed_at: int) -> dict:
    return {
        "match_id": record.match_id,
        "radiant_win": True,
        "applied_at": observed_at,
        "result_timestamp": observed_at,
        "match_record": lts._serialize_match_record(record),
    }


def test_pending_result_uses_one_timestamp_for_progress_and_model(monkeypatch) -> None:
    """Clock rollover must not make the replay ledger one second stale (E-255)."""
    record = _live_record(9050, start=1_010, duration=None)
    pending_map = {
        "map_key": "dltv.org/matches/9050.0",
        "match_record": lts._serialize_match_record(record),
        "first_team_is_radiant": True,
    }
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    applied_maps: dict = {}
    ticks = iter((2_000, 2_001))
    monkeypatch.setattr(lts.time, "time", lambda: next(ticks))

    update = lts._apply_one_pending_map(
        pending_map=pending_map,
        winner_slot="first",
        applied_maps=applied_maps,
        snapshot={"meta": {}},
        model_getter=lambda: model,
        previous_scores={"first": 0, "second": 0},
        current_scores={"first": 1, "second": 0},
        normalized_series_key="9050",
        series_url="dltv.org/matches/9050",
    )

    assert update is not None
    entry = applied_maps["dltv.org/matches/9050.0"]
    assert entry["applied_at"] == entry["result_timestamp"] == 2_000
    assert {model.player_global_last_seen_ts[player_id] for player_id in range(1, 11)} == {2_000}
    assert set(model.roster_last_seen_ts[LeagueTier.TIER2].values()) == {2_000}


def test_rebase_replays_post_snapshot_live_result_once_and_keeps_pending(tmp_path) -> None:
    snapshot_path, snapshot = _ledger_snapshot(tmp_path)
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    completed = _live_record(9001, start=1_010)
    pending = _live_record(9002, start=1_020)
    _write(state_path, {
        "base_reference_timestamp": 900,
        "base_model_config_signature": "same-history-signature",
        "model_state": HybridPlayerRosterEloModel(HybridEloConfig()).export_state(),
    })
    _write(progress_path, {
        "base_reference_timestamp": 900,
        "base_model_config_signature": "same-history-signature",
        "applied_maps": {"map-1": _applied_entry(completed, observed_at=1_080)},
        "pending_series": {"series-77": {
            "pending_maps": [{"map_key": "map-2", "match_record": lts._serialize_match_record(pending)}],
            "pending_map": {"map_key": "map-2", "match_record": lts._serialize_match_record(pending)},
            "last_scores": {"first": 1, "second": 0},
        }},
    })

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    expected = HybridPlayerRosterEloModel.from_state(snapshot["model_state"])
    expected.process_match(result_record(completed, 1_080))
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["model_state"] == expected.export_state()
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    assert progress["applied_maps"]["map-1"]["result_timestamp"] == 1_080
    assert progress["applied_maps"]["map-1"]["match_record"]["radiant_player_positions"] == ["1", "2", "3", "4", "5"]
    assert progress["pending_series"]["series-77"]["pending_maps"][0]["map_key"] == "map-2"

    before = state_path.read_bytes(), progress_path.read_bytes()
    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before


def test_rebase_deduplicates_snapshot_covered_durationless_live_result(tmp_path) -> None:
    # The normal score-observation path has no duration.  Exact positive
    # snapshot membership still proves coverage; observed_at alone does not.
    record = _live_record(9010, start=920, duration=None)
    snapshot_path, snapshot = _ledger_snapshot(tmp_path, completed_ids=[record.match_id])
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": HybridPlayerRosterEloModel(HybridEloConfig()).export_state()})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {},
                           "applied_maps": {"covered": _applied_entry(record, observed_at=1_050)}})

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    assert json.loads(state_path.read_text(encoding="utf-8"))["model_state"] == snapshot["model_state"]
    covered = json.loads(progress_path.read_text(encoding="utf-8"))["applied_maps"]
    assert covered["covered"] == {
        "match_id": record.match_id,
        "snapshot_covered": True,
        "snapshot_covered_reference": 1_000,
    }


def test_rebase_keeps_covered_pending_tombstone_across_next_advance(tmp_path) -> None:
    record = _live_record(9011, start=920, duration=None)
    snapshot_path, snapshot = _ledger_snapshot(tmp_path, completed_ids=[record.match_id])
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": HybridPlayerRosterEloModel(HybridEloConfig()).export_state()})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {"s": {"pending_maps": [
                               {"map_key": "pending-covered", "match_record": lts._serialize_match_record(record)}]}},
                           "applied_maps": {}})

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    first = json.loads(progress_path.read_text(encoding="utf-8"))
    assert first["applied_maps"]["pending-covered"]["snapshot_covered"] is True

    snapshot["meta"]["reference_timestamp"] = 1_001
    _write(snapshot_path, snapshot)
    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    second = json.loads(progress_path.read_text(encoding="utf-8"))
    assert second["applied_maps"]["pending-covered"]["snapshot_covered"] is True
    assert second["applied_maps"]["pending-covered"]["snapshot_covered_reference"] == 1_000


def test_rebase_tombstone_requires_current_proof_then_expires(tmp_path, capsys) -> None:
    """E-294 follow-up: an unconfirmed tombstone is now RETAINED, not fatal.

    A tombstone carries no match_record, so refusing to rebase past it cannot
    restore anything -- it only freezes prod on a stale base forever, because
    the ledger's own `match_id` (here a placeholder never proven by exact id)
    can never appear in a later snapshot's recent ids. The tombstone still
    expires on its own after SNAPSHOT_RECENT_RESULT_COVERAGE_SECONDS (the
    second half of this test, unchanged).
    """
    snapshot_path, snapshot = _ledger_snapshot(tmp_path, completed_ids=[])
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 1_000,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": snapshot["model_state"]})
    tombstone = {"match_id": 9012, "snapshot_covered": True,
                 "snapshot_covered_reference": 1_000}
    _write(progress_path, {"base_reference_timestamp": 1_000,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {}, "applied_maps": {"covered": dict(tombstone)}})
    snapshot["meta"]["reference_timestamp"] = 1_001
    _write(snapshot_path, snapshot)

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    assert "не подтверждён" in capsys.readouterr().err
    unconfirmed = json.loads(progress_path.read_text(encoding="utf-8"))["applied_maps"]
    assert unconfirmed["covered"] == tombstone

    snapshot["meta"]["reference_timestamp"] = 1_000 + 7 * 86400 + 1
    _write(snapshot_path, snapshot)
    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    assert json.loads(progress_path.read_text(encoding="utf-8"))["applied_maps"] == {}


def test_rebase_pair_proven_tombstone_records_proof_and_reconfirms(tmp_path, capsys) -> None:
    """E-294: a team-pair-proven tombstone must store the CONSUMED snapshot key,
    not just the ledger's own (never-provable-again) placeholder match_id, so a
    LATER rebase can re-confirm it -- see `_snapshot_covered_tombstone`'s
    `proof_match_id` and the tombstone re-check in `_applied_entry_for_rebase`.
    """
    record = _live_record(1_700_000_555, start=900, duration=None)
    snapshot_path, snapshot = _ledger_snapshot(
        tmp_path, completed_ids=[], keys=[[9_999_000_555, 101, 202, 900]], keys_tolerance=10_800,
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
    covered = json.loads(progress_path.read_text(encoding="utf-8"))["applied_maps"]
    assert covered["placeholder"] == {
        "match_id": 1_700_000_555,
        "snapshot_covered": True,
        "snapshot_covered_reference": 1_000,
        "snapshot_covered_match_id": 9_999_000_555,
    }
    capsys.readouterr()

    # Next rebase: same snapshot key still present, later cutoff. Must confirm
    # via the stored proof id, with no "не подтверждён" warning.
    snapshot["meta"]["reference_timestamp"] = 1_001
    _write(snapshot_path, snapshot)
    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    assert "не подтверждён" not in capsys.readouterr().err
    second = json.loads(progress_path.read_text(encoding="utf-8"))["applied_maps"]
    assert second["placeholder"]["snapshot_covered_match_id"] == 9_999_000_555
    assert second["placeholder"]["snapshot_covered_reference"] == 1_000


def test_rebase_refuses_legacy_post_cutoff_result_without_replay_context(tmp_path, capsys) -> None:
    snapshot_path, _snapshot_payload = _ledger_snapshot(tmp_path)
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": {"legacy": True}})
    legacy = {"base_reference_timestamp": 900, "base_model_config_signature": "same-history-signature",
              "pending_series": {}, "applied_maps": {"lost": {"match_id": 9999, "applied_at": 1_050}}}
    _write(progress_path, legacy)
    before = state_path.read_bytes(), progress_path.read_bytes()

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    assert "полного match_record" in capsys.readouterr().err
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before


def test_rebase_refuses_legacy_entry_without_exact_snapshot_membership(tmp_path, capsys) -> None:
    snapshot_path, _snapshot = _ledger_snapshot(tmp_path)
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": {"legacy": True}})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {},
                           "applied_maps": {"old": {"match_id": 9998, "applied_at": 999}}})

    before = state_path.read_bytes(), progress_path.read_bytes()
    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    assert "полного match_record" in capsys.readouterr().err
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before


def test_rebase_deduplicates_legacy_entry_by_exact_snapshot_id(tmp_path) -> None:
    snapshot_path, snapshot = _ledger_snapshot(tmp_path, completed_ids=[9998])
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": {"legacy": True}})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {},
                           "applied_maps": {"old": {"match_id": 9998, "applied_at": 999}}})

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    assert json.loads(state_path.read_text(encoding="utf-8"))["model_state"] == snapshot["model_state"]
    covered = json.loads(progress_path.read_text(encoding="utf-8"))["applied_maps"]
    assert covered["old"] == {
        "match_id": 9998,
        "snapshot_covered": True,
        "snapshot_covered_reference": 1_000,
    }


def test_rebase_refuses_absent_delayed_row_with_pre_cutoff_live_timestamp_when_strict(
        tmp_path, capsys, monkeypatch) -> None:
    # Exact recent coverage proves this source row is absent, but moving its
    # ELO event backwards behind the rebuilt baseline is not valid replay
    # under the strict (opt-in) order guard. Default behaviour is now lenient
    # late replay -- see
    # test_rebase_replays_absent_delayed_row_with_pre_cutoff_live_timestamp_by_default.
    monkeypatch.setattr(lts, "LIVE_ELO_REBASE_STRICT_ORDER", True)
    record = _live_record(9997, start=920, duration=60)
    snapshot_path, _snapshot = _ledger_snapshot(tmp_path, completed_ids=[])
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": {"legacy": True}})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {},
                           "applied_maps": {"delayed": _applied_entry(record, observed_at=980)}})
    before = state_path.read_bytes(), progress_path.read_bytes()

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    assert "без изменения порядка" in capsys.readouterr().err
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before


def test_rebase_replays_absent_delayed_row_with_pre_cutoff_live_timestamp_by_default(
        tmp_path, capsys) -> None:
    # Default (lenient) mode: the same absent, pre-cutoff row is replayed
    # with a stderr warning instead of blocking every nightly rebase (E-18
    # follow-up: 7 tier-2 maps the corpus top-up never ingests used to freeze
    # prod on a stale base here).
    record = _live_record(9997, start=920, duration=60)
    snapshot_path, snapshot = _ledger_snapshot(tmp_path, completed_ids=[])
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": {"legacy": True}})
    _write(progress_path, {"base_reference_timestamp": 900,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {},
                           "applied_maps": {"delayed": _applied_entry(record, observed_at=980)}})

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    err = capsys.readouterr().err
    assert "replay после среза" in err

    rebased_state = json.loads(state_path.read_text(encoding="utf-8"))["model_state"]
    assert rebased_state != snapshot["model_state"]

    rebased_progress = json.loads(progress_path.read_text(encoding="utf-8"))
    applied = rebased_progress["applied_maps"]["delayed"]
    assert applied["match_id"] == 9997
    assert applied["result_timestamp"] == 980


def test_rebase_rolls_back_both_runtime_files_when_progress_replace_fails(tmp_path, monkeypatch, capsys) -> None:
    snapshot_path, _snapshot = _ledger_snapshot(tmp_path)
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    old_state = {"base_reference_timestamp": 900, "base_model_config_signature": "same-history-signature",
                 "model_state": {"old": 1}}
    old_progress = {"base_reference_timestamp": 900, "base_model_config_signature": "same-history-signature",
                    "pending_series": {}, "applied_maps": {}}
    _write(state_path, old_state)
    _write(progress_path, old_progress)
    original_write = lts._write_json_atomic
    failed = False

    def fail_progress_once(path: Path, payload: dict) -> None:
        nonlocal failed
        if path == progress_path and not failed:
            failed = True
            raise OSError("injected progress replace failure")
        original_write(path, payload)

    monkeypatch.setattr(lts, "_write_json_atomic", fail_progress_once)
    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    assert "прежние runtime state/progress восстановлены" in capsys.readouterr().err
    assert json.loads(state_path.read_text(encoding="utf-8")) == old_state
    assert json.loads(progress_path.read_text(encoding="utf-8")) == old_progress


def test_rebase_recovers_overlay_marker_and_restores_delta_on_write_failure(tmp_path, monkeypatch) -> None:
    record = _live_record(9040, start=1_010, duration=60)
    snapshot_path, snapshot = _ledger_snapshot(tmp_path)
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    delta_path = tmp_path / "delta.json"
    monkeypatch.setenv("LIVE_ELO_DELTA", str(delta_path))
    entry = _applied_entry(record, observed_at=1_080)
    state = {"base_reference_timestamp": 1_000,
             "base_model_config_signature": "same-history-signature",
             "model_state": snapshot["model_state"]}
    progress = {"base_reference_timestamp": 1_000,
                "base_model_config_signature": "same-history-signature",
                "pending_series": {}, "applied_maps": {"map-9040": entry},
                "pending_overlay_commit": {"entries": {"map-9040": dict(entry)}}}
    _write(state_path, state)
    _write(progress_path, progress)
    state_overlay.save_delta(delta_path, base_reference_timestamp=1_000,
                             base_model_config_signature="same-history-signature",
                             changes={}, resets={}, small_parts={}, updated_at=1)
    before = state_path.read_bytes(), progress_path.read_bytes(), delta_path.read_bytes()
    original_write = lts._write_json_atomic

    def fail_progress(path: Path, payload: dict) -> None:
        if path == progress_path:
            raise OSError("injected progress replace failure")
        original_write(path, payload)

    monkeypatch.setattr(lts, "_write_json_atomic", fail_progress)
    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    assert (state_path.read_bytes(), progress_path.read_bytes(), delta_path.read_bytes()) == before


def test_rebase_rollback_preserves_malformed_state_bytes(tmp_path, monkeypatch) -> None:
    record = _live_record(9042, start=1_010, duration=60)
    snapshot_path, _snapshot = _ledger_snapshot(tmp_path)
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    entry = _applied_entry(record, observed_at=1_080)
    state_path.write_bytes(b"{malformed runtime state\n")
    _write(progress_path, {"base_reference_timestamp": 1_000,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {}, "applied_maps": {"map-9042": entry},
                           "pending_overlay_commit": {"entries": {"map-9042": dict(entry)}}})
    before = state_path.read_bytes(), progress_path.read_bytes()
    original_write = lts._write_json_atomic

    def fail_progress(path: Path, payload: dict) -> None:
        if path == progress_path:
            raise OSError("injected progress replace failure")
        original_write(path, payload)

    monkeypatch.setattr(lts, "_write_json_atomic", fail_progress)
    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before


def test_rebase_recovers_overlay_marker_once_and_invalidates_compatible_delta(tmp_path, monkeypatch) -> None:
    record = _live_record(9041, start=1_010, duration=60)
    snapshot_path, snapshot = _ledger_snapshot(tmp_path)
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    delta_path = tmp_path / "delta.json"
    monkeypatch.setenv("LIVE_ELO_DELTA", str(delta_path))
    entry = _applied_entry(record, observed_at=1_080)
    _write(state_path, {"base_reference_timestamp": 1_000,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": snapshot["model_state"]})
    _write(progress_path, {"base_reference_timestamp": 1_000,
                           "base_model_config_signature": "same-history-signature",
                           "pending_series": {}, "applied_maps": {"map-9041": entry},
                           "pending_overlay_commit": {"entries": {"map-9041": dict(entry)}}})
    state_overlay.save_delta(delta_path, base_reference_timestamp=1_000,
                             base_model_config_signature="same-history-signature",
                             changes={}, resets={}, small_parts={}, updated_at=1)

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0
    expected = HybridPlayerRosterEloModel.from_state(snapshot["model_state"])
    expected.process_match(result_record(record, 1_080))
    assert json.loads(state_path.read_text(encoding="utf-8"))["model_state"] == expected.export_state()
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    assert "pending_overlay_commit" not in progress
    assert state_overlay.load_delta(delta_path, base_reference_timestamp=1_000,
                                    base_model_config_signature="same-history-signature") is None


def test_rebase_refuses_mixed_covered_and_retained_pending_queue(tmp_path, capsys) -> None:
    covered = _live_record(9995, start=920, duration=None)
    retained = _live_record(9996, start=1_020, duration=None)
    snapshot_path, _snapshot = _ledger_snapshot(tmp_path, completed_ids=[covered.match_id])
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": "same-history-signature",
                        "model_state": {"old": 1}})
    pending = {"base_reference_timestamp": 900, "base_model_config_signature": "same-history-signature",
               "applied_maps": {}, "pending_series": {"s": {"last_scores": {"first": 0, "second": 0},
               "pending_maps": [{"map_key": "covered", "match_record": lts._serialize_match_record(covered)},
                                {"map_key": "retained", "match_record": lts._serialize_match_record(retained)}]}}}
    _write(progress_path, pending)
    before = state_path.read_bytes(), progress_path.read_bytes()

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 1
    assert "covered и retained" in capsys.readouterr().err
    assert (state_path.read_bytes(), progress_path.read_bytes()) == before


def test_current_live_registration_does_not_parse_full_state_for_rebase(tmp_path, monkeypatch) -> None:
    """A current small progress ledger must keep the E-251/255 lazy path."""
    model_state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    snapshot_path = tmp_path / "snapshot.json"
    snapshot = {
        "meta": {**lts._rating_replay_meta(), "reference_timestamp": 1_000},
        "model_state": model_state,
    }
    _write(snapshot_path, snapshot)
    signature = lts._snapshot_model_config_signature(snapshot)
    progress_path = tmp_path / "progress.json"
    runtime_state_path = tmp_path / "large-state.json"
    _write(progress_path, {"base_reference_timestamp": 1_000,
                           "base_model_config_signature": signature,
                           "pending_series": {}, "applied_maps": {}})
    _write(runtime_state_path, {"pretend": "large"})
    original_load = lts._load_json_dict

    def guarded_load(path: Path):
        if path == runtime_state_path:
            raise AssertionError("steady live rebase must not parse model_state")
        return original_load(path)

    monkeypatch.setattr(lts, "_load_json_dict", guarded_load)
    result = lts.register_live_map_context(
        series_key="77", series_url="test", map_key="map-new",
        first_team_score=0, second_team_score=0, first_team_is_radiant=True,
        match_record=_live_record(9020, start=1_020, duration=None),
        snapshot_path=snapshot_path, data_dir=data_dir, rebuild_if_missing=False,
        progress_path=progress_path, runtime_model_state_path=runtime_state_path,
        runtime_lock_path=tmp_path / "lock",
    )
    assert result is not None


def test_live_registration_rebases_advanced_reference_before_loading_progress(tmp_path) -> None:
    """The normal runtime path must replay instead of clearing an old ledger."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    base_model = HybridPlayerRosterEloModel(HybridEloConfig())
    snapshot = {
        "meta": {**lts._rating_replay_meta(), "reference_timestamp": 1_000},
        "model_state": base_model.export_state(),
    }
    snapshot_path = tmp_path / "snapshot.json"
    _write(snapshot_path, snapshot)
    signature = lts._snapshot_model_config_signature(snapshot)
    completed = _live_record(9030, start=1_010, duration=None)
    progress_path = tmp_path / "progress.json"
    state_path = tmp_path / "state.json"
    _write(progress_path, {
        "base_reference_timestamp": 900,
        "base_model_config_signature": signature,
        "pending_series": {},
        "applied_maps": {"previous": _applied_entry(completed, observed_at=1_080)},
    })
    _write(state_path, {"base_reference_timestamp": 900,
                        "base_model_config_signature": signature,
                        "model_state": base_model.export_state()})

    result = lts.register_live_map_context(
        series_key="77", series_url="test", map_key="next",
        first_team_score=0, second_team_score=0, first_team_is_radiant=True,
        match_record=_live_record(9031, start=1_090, duration=None),
        snapshot_path=snapshot_path, data_dir=data_dir, rebuild_if_missing=False,
        progress_path=progress_path, runtime_model_state_path=state_path,
        runtime_lock_path=tmp_path / "lock",
    )
    assert result is not None
    expected = HybridPlayerRosterEloModel.from_state(snapshot["model_state"])
    expected.process_match(result_record(completed, 1_080))
    assert json.loads(state_path.read_text(encoding="utf-8"))["model_state"] == expected.export_state()
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    assert "previous" in progress["applied_maps"]
    assert progress["base_reference_timestamp"] == 1_000


def test_rebase_collapses_alias_twin_applied_within_window(tmp_path, capsys) -> None:
    """Two sourcetv alias ids for ONE physical map must be applied once (E-294 follow-up).

    A now-fixed enqueue bug (dafbec37) produced ledger twins such as
    `…9003417257.0` and `…9003417257.60`: same `match_id`, same teams,
    `result_timestamp` a few minutes apart. Replaying both double-counts the
    map's result in the rebased model state.
    """
    snapshot_path, snapshot = _ledger_snapshot(tmp_path)
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    twin_a = _live_record(9001, start=1_010)
    twin_b = _live_record(9001, start=1_010)
    _write(state_path, {
        "base_reference_timestamp": 900,
        "base_model_config_signature": "same-history-signature",
        "model_state": HybridPlayerRosterEloModel(HybridEloConfig()).export_state(),
    })
    _write(progress_path, {
        "base_reference_timestamp": 900,
        "base_model_config_signature": "same-history-signature",
        "pending_series": {},
        "applied_maps": {
            "map-1": _applied_entry(twin_a, observed_at=1_080),
            "map-2": _applied_entry(twin_b, observed_at=1_080 + 200),
        },
    })

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0

    expected = HybridPlayerRosterEloModel.from_state(snapshot["model_state"])
    expected.process_match(result_record(twin_a, 1_080))
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["model_state"] == expected.export_state()

    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    # The kept twin is applied normally; the skipped one still stays in the
    # ledger (unchanged rebase semantics for a later run), just without a
    # second `process_match` call behind it.
    assert progress["applied_maps"]["map-1"]["result_timestamp"] == 1_080
    assert "map-2" in progress["applied_maps"]
    assert "дубликат" in capsys.readouterr().out


def test_rebase_keeps_far_apart_same_match_id_maps_as_two_applications(tmp_path) -> None:
    """A legacy series that reuses an alias match_id across two REAL maps must not collapse."""
    snapshot_path, snapshot = _ledger_snapshot(tmp_path)
    state_path = tmp_path / "state.json"
    progress_path = tmp_path / "progress.json"
    map_a = _live_record(9002, start=1_010)
    map_b = _live_record(9002, start=1_010)
    _write(state_path, {
        "base_reference_timestamp": 900,
        "base_model_config_signature": "same-history-signature",
        "model_state": HybridPlayerRosterEloModel(HybridEloConfig()).export_state(),
    })
    _write(progress_path, {
        "base_reference_timestamp": 900,
        "base_model_config_signature": "same-history-signature",
        "pending_series": {},
        "applied_maps": {
            "map-1": _applied_entry(map_a, observed_at=1_080),
            "map-2": _applied_entry(map_b, observed_at=1_080 + 1_200),
        },
    })

    assert rebase_main(["--snapshot", str(snapshot_path), "--state", str(state_path),
                        "--progress", str(progress_path)]) == 0

    expected = HybridPlayerRosterEloModel.from_state(snapshot["model_state"])
    expected.process_match(result_record(map_a, 1_080))
    expected.process_match(result_record(map_b, 1_080 + 1_200))
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["model_state"] == expected.export_state()

    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    assert progress["applied_maps"]["map-1"]["result_timestamp"] == 1_080
    assert progress["applied_maps"]["map-2"]["result_timestamp"] == 1_080 + 1_200
