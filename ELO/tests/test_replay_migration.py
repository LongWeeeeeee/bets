import json

from ELO import live_team_strength as lts, state_overlay


def test_pinned_legacy_snapshot_is_declined_without_implicit_rebuild(tmp_path, monkeypatch):
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps({"meta": {"reference_timestamp": 100}, "model_state": {},
                                "teams_by_org_key": {}, "team_kills_history_by_team_id": {}}))
    monkeypatch.setenv(lts.SNAPSHOT_PIN_ENV, "1")
    def unexpected(**kwargs):
        raise AssertionError("migration must be an explicit offline rebuild")
    monkeypatch.setattr(lts, "build_snapshot", unexpected)
    assert lts.ensure_snapshot(snapshot_path=path, data_dir=tmp_path) is None
    assert lts.load_live_snapshot(path) is None


def test_same_last_time_but_changed_history_rejects_old_state_progress_and_delta(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    raw = {"id": 1, "startDateTime": 1786000000, "durationSeconds": 2000,
           "didRadiantWin": True, "radiantTeam": {"id": 10, "name": "A"},
           "direTeam": {"id": 20, "name": "B"},
           "players": [{"isRadiant": i < 5, "steamAccount": {"id": i + 1},
                        "position": f"POSITION_{i % 5 + 1}"} for i in range(10)]}
    source_path = source / "7.41e_part1.json"
    source_path.write_text(json.dumps({"1": raw}))
    before = lts.build_snapshot(data_dir=source, snapshot_path=tmp_path / "before.json")
    ref = lts._snapshot_reference_timestamp(before)
    signature = lts._snapshot_model_config_signature(before)
    state_path, progress_path, delta_path = [tmp_path / f"{name}.json" for name in ("state", "progress", "delta")]
    payload = {"base_reference_timestamp": ref, "base_model_config_signature": signature,
               "model_state": before["model_state"], "pending_series": {"x": {}}, "applied_maps": {"1": {}}}
    state_path.write_text(json.dumps(payload))
    progress_path.write_text(json.dumps(payload))
    state_overlay.save_delta(delta_path, base_reference_timestamp=ref,
                             base_model_config_signature=signature, changes={}, small_parts={}, updated_at=ref)
    assert lts._delta_is_usable(before, delta_path)
    raw["didRadiantWin"] = False
    source_path.write_text(json.dumps({"1": raw}))
    after = lts.build_snapshot(data_dir=source, snapshot_path=tmp_path / "after.json")
    assert lts._snapshot_reference_timestamp(after) == ref
    new_signature = lts._snapshot_model_config_signature(after)
    assert new_signature != signature
    assert lts._load_runtime_model_payload(snapshot=after, runtime_model_state_path=state_path) is None
    assert not lts._delta_is_usable(after, delta_path)
    progress = lts._load_runtime_progress(base_reference_timestamp=ref, model_config_signature=new_signature,
                                          progress_path=progress_path)
    assert progress["pending_series"] == progress["applied_maps"] == {}


def test_calendar_change_requires_rebuild_even_with_same_replay_version(monkeypatch):
    snapshot = {"meta": lts._rating_replay_meta()}
    assert lts._snapshot_replay_is_current(snapshot)
    monkeypatch.setattr(lts, "PATCH_RELEASES", lts.PATCH_RELEASES[1:])
    assert not lts._snapshot_replay_is_current(snapshot)
