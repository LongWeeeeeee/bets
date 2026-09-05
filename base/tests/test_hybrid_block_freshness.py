"""Регрессии обновления модели hybrid-блока без боевых ELO-артефактов."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import hybrid_block
from ELO import live_team_strength as lts


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch):
    monkeypatch.setattr(
        hybrid_block,
        "_state",
        {"loaded": False, "model": None, "names": {}, "error": None,
         "built_ts": 0, "signature": None},
    )


def _replace_json(path: Path, payload: dict) -> None:
    replacement = path.with_suffix(".next")
    replacement.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(replacement, path)


def test_custom_snapshot_is_reloaded_after_atomic_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "custom.json"
    _replace_json(path, {"meta": {**lts._rating_replay_meta(), "reference_timestamp": 1},
                         "teams_by_org_key": {}})
    models = []
    monkeypatch.setattr(hybrid_block, "SNAPSHOT", path)
    monkeypatch.setattr(lts, "_restore_model_from_snapshot",
                        lambda snapshot: models.append(snapshot["meta"]["reference_timestamp"]) or object())

    first = hybrid_block._load()
    assert models == [1]
    _replace_json(path, {"meta": {**lts._rating_replay_meta(), "reference_timestamp": 2},
                         "teams_by_org_key": {}})

    refreshed = hybrid_block._load()
    assert refreshed is first
    assert models == [1, 2]
    assert refreshed["built_ts"] == 2


def test_missing_custom_snapshot_recovers_when_it_appears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "late.json"
    monkeypatch.setattr(hybrid_block, "SNAPSHOT", path)
    monkeypatch.setattr(lts, "_restore_model_from_snapshot", lambda _snapshot: object())

    assert hybrid_block._load()["model"] is None
    _replace_json(path, {"meta": {**lts._rating_replay_meta(), "reference_timestamp": 7},
                         "teams_by_org_key": {}})
    assert hybrid_block._load()["model"] is not None


def test_default_path_reloads_when_live_delta_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = tmp_path / "snapshot.json"
    runtime = tmp_path / "runtime.json"
    delta = tmp_path / "delta.json"
    for path in (snapshot, runtime, delta):
        _replace_json(path, {"version": 1, "meta": lts._rating_replay_meta()})
    calls = []
    monkeypatch.setattr(hybrid_block, "SNAPSHOT", snapshot)
    monkeypatch.setattr(lts, "DEFAULT_SNAPSHOT_PATH", snapshot)
    monkeypatch.setattr(lts, "DEFAULT_RUNTIME_MODEL_STATE_PATH", runtime)
    monkeypatch.setattr(lts, "DEFAULT_LIVE_DELTA_PATH", delta)
    import ELO.array_model as array_model

    monkeypatch.setattr(array_model, "load_read_model",
                        lambda *_args, **_kwargs: calls.append("load") or object())
    monkeypatch.setattr(array_model, "load_team_names", lambda _path: {})

    assert hybrid_block._load()["model"] is not None
    _replace_json(delta, {"version": 2})
    assert hybrid_block._load()["model"] is not None
    assert calls == ["load", "load"]


def test_legacy_snapshot_does_not_reach_fast_array_model(tmp_path, monkeypatch):
    path = tmp_path / "legacy.json"
    _replace_json(path, {"meta": {"reference_timestamp": 1}})
    monkeypatch.setattr(hybrid_block, "SNAPSHOT", path)
    monkeypatch.setattr(lts, "DEFAULT_SNAPSHOT_PATH", path)
    import ELO.array_model as arrays
    def unexpected(*args, **kwargs):
        raise AssertionError("legacy arrays must not be served")
    monkeypatch.setattr(arrays, "load_read_model", unexpected)
    assert hybrid_block._load()["model"] is None
    assert "rebuild" in hybrid_block._load()["error"]
