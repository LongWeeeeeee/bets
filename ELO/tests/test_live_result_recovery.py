"""Regression guards for the offline lost-live-result recovery tool."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ELO.config import HybridEloConfig
from ELO import array_model
from ELO.convert_state_to_delta import main as convert_delta_main
from ELO.domain import LeagueTier, MatchRecord
from ELO.live_team_strength import _serialize_match_record
from ELO.models import HybridPlayerRosterEloModel
from ELO.recover_live_results import RecoveryInputError, recover
from ELO.replay import result_record
from ELO import state_overlay


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _record(match_id: int, *, start: int, radiant_win: bool) -> MatchRecord:
    offset = (match_id - 9_000) * 20
    return MatchRecord(
        match_id=match_id, timestamp=start, radiant_win=radiant_win,
        radiant_team_id=101 + offset, radiant_team_name=f"Radiant {match_id}",
        dire_team_id=201 + offset, dire_team_name=f"Dire {match_id}",
        radiant_player_ids=tuple(range(10_001 + offset, 10_006 + offset)),
        dire_player_ids=tuple(range(20_001 + offset, 20_006 + offset)),
        league_id=7, league_name="Test League", source_league_tier="TIER2",
        series_id=match_id, series_type="1", source_patch=None,
        derived_league_tier=LeagueTier.TIER2, duration_seconds=None,
    )


def _event(record: MatchRecord, *, observed_at: int, repair_existing: bool = False) -> dict:
    return {
        "map_key": f"dltv.org/matches/{record.match_id}.0",
        "match_record": _serialize_match_record(record),
        "radiant_win": record.radiant_win,
        "result_timestamp": observed_at,
        "provenance": "test",
        **({"repair_existing": True} if repair_existing else {}),
    }


def _inputs(tmp_path: Path) -> tuple[Path, Path, Path, dict]:
    base = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    snapshot = tmp_path / "snapshot.json"
    progress = tmp_path / "progress.json"
    delta = tmp_path / "delta.json"
    common = {"base_reference_timestamp": 1_000, "base_model_config_signature": "test-signature"}
    _write(snapshot, {"meta": {"reference_timestamp": 1_000,
                                 "model_config_signature": "test-signature"}, "model_state": base})
    pending_record = _record(9_009, start=1_120, radiant_win=False)
    pending_map = {"map_key": "dltv.org/matches/9009.0",
                   "match_record": _serialize_match_record(pending_record)}
    _write(progress, {**common, "pending_series": {"future": {
        "pending_maps": [pending_map], "pending_map": pending_map,
    }}, "applied_maps": {}})
    _write(delta, {**common, "model_state": base})
    return snapshot, progress, delta, base


def test_recovery_repairs_current_delta_then_adds_results_in_outcome_order(tmp_path: Path) -> None:
    snapshot, progress, delta, base = _inputs(tmp_path)
    current = _record(9_001, start=1_010, radiant_win=False)
    recovered = _record(9_002, start=1_020, radiant_win=True)
    current_at, recovered_at = 1_080, 1_090
    model = HybridPlayerRosterEloModel.from_state(base)
    model.process_match(result_record(current, current_at))
    common = {"base_reference_timestamp": 1_000, "base_model_config_signature": "test-signature"}
    _write(delta, {**common, "model_state": model.export_state()})
    pending_record = _record(9_009, start=1_120, radiant_win=False)
    pending_map = {"map_key": "dltv.org/matches/9009.0",
                   "match_record": _serialize_match_record(pending_record)}
    expected_pending = {"future": {"pending_maps": [pending_map], "pending_map": pending_map}}
    _write(progress, {**common, "pending_series": expected_pending, "applied_maps": {
        "dltv.org/matches/9001.0": {"match_id": 9_001, "radiant_win": False,
                                      "applied_at": current_at},
    }})
    events = tmp_path / "events.json"
    _write(events, {"events": [_event(current, observed_at=current_at, repair_existing=True),
                                _event(recovered, observed_at=recovered_at)]})
    state_out, progress_out, report_out = (tmp_path / "state.out", tmp_path / "progress.out", tmp_path / "report.out")

    report = recover(snapshot_path=snapshot, progress_path=progress, delta_path=delta, events_path=events,
                     output_state=state_out, output_progress=progress_out, output_report=report_out,
                     verify_current_delta=True)

    model.process_match(result_record(recovered, recovered_at))
    assert json.loads(state_out.read_text()) ["model_state"] == model.export_state()
    staged_progress = json.loads(progress_out.read_text())
    assert staged_progress["pending_series"] == expected_pending
    assert staged_progress["applied_maps"]["dltv.org/matches/9001.0"]["match_record"]["match_id"] == 9_001
    assert staged_progress["applied_maps"]["dltv.org/matches/9002.0"]["radiant_win"] is True
    assert report["current_delta_verified"] is True
    assert report["recovered_event_count"] == 1


def test_recovery_refuses_duplicate_match_id_before_creating_outputs(tmp_path: Path) -> None:
    snapshot, progress, delta, _ = _inputs(tmp_path)
    record = _record(9_003, start=1_010, radiant_win=True)
    events = tmp_path / "events.json"
    duplicate = _event(record, observed_at=1_080)
    duplicate["map_key"] = "dltv.org/matches/9004.0"
    _write(events, {"events": [_event(record, observed_at=1_080), duplicate]})
    state_out, progress_out, report_out = (tmp_path / "state.out", tmp_path / "progress.out", tmp_path / "report.out")

    with pytest.raises(RecoveryInputError, match="duplicate recovered match_id"):
        recover(snapshot_path=snapshot, progress_path=progress, delta_path=delta, events_path=events,
                output_state=state_out, output_progress=progress_out, output_report=report_out)
    assert not state_out.exists()
    assert not progress_out.exists()
    assert not report_out.exists()


def test_recovery_refuses_unresolved_overlay_commit(tmp_path: Path) -> None:
    snapshot, progress, delta, _ = _inputs(tmp_path)
    payload = json.loads(progress.read_text())
    payload["pending_overlay_commit"] = {"entries": {}}
    _write(progress, payload)
    events = tmp_path / "events.json"
    _write(events, {"events": [_event(_record(9_003, start=1_010, radiant_win=True), observed_at=1_080)]})

    with pytest.raises(RecoveryInputError, match="pending_overlay_commit"):
        recover(snapshot_path=snapshot, progress_path=progress, delta_path=delta, events_path=events,
                output_state=tmp_path / "state.out", output_progress=tmp_path / "progress.out",
                output_report=tmp_path / "report.out")


def test_recovery_refuses_pending_mirror_for_a_different_map(tmp_path: Path) -> None:
    snapshot, progress, delta, _ = _inputs(tmp_path)
    payload = json.loads(progress.read_text())
    payload["pending_series"]["future"]["pending_map"] = {
        "map_key": "dltv.org/matches/9010.0",
        "match_record": _serialize_match_record(_record(9_010, start=1_120, radiant_win=False)),
    }
    _write(progress, payload)
    events = tmp_path / "events.json"
    _write(events, {"events": [_event(_record(9_003, start=1_010, radiant_win=True), observed_at=1_080)]})
    with pytest.raises(RecoveryInputError, match="pending mirror"):
        recover(snapshot_path=snapshot, progress_path=progress, delta_path=delta, events_path=events,
                output_state=tmp_path / "state.out", output_progress=tmp_path / "progress.out",
                output_report=tmp_path / "report.out")
    assert not (tmp_path / "state.out").exists()


def test_recovery_refuses_recovered_map_that_overlaps_pending(tmp_path: Path) -> None:
    snapshot, progress, delta, _ = _inputs(tmp_path)
    pending = json.loads(progress.read_text())["pending_series"]["future"]["pending_map"]
    record = _record(9_009, start=1_120, radiant_win=True)
    events = tmp_path / "events.json"
    _write(events, {"events": [{**_event(record, observed_at=1_180), "map_key": pending["map_key"]}]})

    with pytest.raises(RecoveryInputError, match="пересекается"):
        recover(snapshot_path=snapshot, progress_path=progress, delta_path=delta, events_path=events,
                output_state=tmp_path / "state.out", output_progress=tmp_path / "progress.out",
                output_report=tmp_path / "report.out")


def test_overlay_control_stages_full_state_convertible_to_delta(tmp_path: Path) -> None:
    snapshot, progress, delta, base = _inputs(tmp_path)
    current = _record(9_001, start=1_010, radiant_win=False)
    recovered = _record(9_002, start=1_020, radiant_win=True)
    current_at, recovered_at = 1_080, 1_090
    live = array_model.build_overlay_model(snapshot, None)
    live.process_match(result_record(current, current_at))
    parts = {
        "changes": state_overlay.collect_changes(live._overlay_wrappers),
        "resets": state_overlay.collect_resets(live._overlay_wrappers),
        "small_parts": state_overlay.collect_small_parts(live),
    }
    state_overlay.save_delta(delta, base_reference_timestamp=1_000,
                             base_model_config_signature="test-signature", updated_at=current_at, **parts)
    array_model._OVERLAY_CACHE.clear()
    array_model._READ_CACHE.clear()
    _write(progress, {"base_reference_timestamp": 1_000, "base_model_config_signature": "test-signature",
                      "pending_series": {}, "applied_maps": {
                          "dltv.org/matches/9001.0": {"match_id": 9_001, "radiant_win": False,
                                                        "applied_at": current_at},
                      }})
    events = tmp_path / "events.json"
    _write(events, {"events": [_event(current, observed_at=current_at, repair_existing=True),
                                _event(recovered, observed_at=recovered_at)]})
    state_out, progress_out, report_out = (tmp_path / "state.out", tmp_path / "progress.out", tmp_path / "report.out")

    report = recover(snapshot_path=snapshot, progress_path=progress, delta_path=delta, events_path=events,
                     output_state=state_out, output_progress=progress_out, output_report=report_out,
                     verify_current_delta=True)

    expected = HybridPlayerRosterEloModel.from_state(base)
    expected.process_match(result_record(current, current_at))
    expected.process_match(result_record(recovered, recovered_at))
    assert json.loads(state_out.read_text())["model_state"] == expected.export_state()
    assert report["delta_format"] == "overlay"
    converted = tmp_path / "converted_delta.json"
    assert convert_delta_main(["--snapshot", str(snapshot), "--state", str(state_out), "--delta", str(converted)]) == 0
    assert state_overlay.load_delta(converted, base_reference_timestamp=1_000,
                                    base_model_config_signature="test-signature") is not None


def test_overlay_delta_writer_round_trips_the_staged_contract(tmp_path: Path) -> None:
    """The production recovery path must emit the canonical, loadable delta format."""
    path = tmp_path / "staged_delta.json"
    state_overlay.save_delta(
        path,
        base_reference_timestamp=1_000,
        base_model_config_signature="test-signature",
        changes={"player_global": [[123, 1510.25]]},
        resets={},
        small_parts={"current_patch_key": "7.41", "side_bias": {}, "roster_tracker": {}},
        updated_at=1_100,
    )
    loaded = state_overlay.load_delta(path, base_reference_timestamp=1_000,
                                      base_model_config_signature="test-signature")
    assert loaded is not None
    assert loaded["changes"] == {"player_global": [[123, 1510.25]]}
