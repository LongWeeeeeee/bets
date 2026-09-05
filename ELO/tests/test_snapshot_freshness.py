"""Регрессии свежести кэша базового ELO-снимка."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from ELO import live_team_strength as lts


@pytest.fixture(autouse=True)
def _clear_snapshot_cache(monkeypatch):
    monkeypatch.setattr(lts, "_SNAPSHOT_CACHE", None)
    monkeypatch.setattr(lts, "_SNAPSHOT_CACHE_SIGNATURE", None)


def _replace_json(path: Path, payload: dict) -> None:
    replacement = path.with_suffix(".next")
    replacement.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(replacement, path)


def test_load_snapshot_reuses_unchanged_file_and_refreshes_after_atomic_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "snapshot.json"
    _replace_json(path, {"version": 1})
    calls = []

    def _small_loader(source: Path):
        calls.append(source)
        return json.loads(source.read_text(encoding="utf-8"))

    monkeypatch.setattr(lts, "_load_snapshot_streaming", _small_loader)
    first = lts.load_snapshot(path)
    assert lts.load_snapshot(path) is first
    assert len(calls) == 1

    _replace_json(path, {"version": 2})
    refreshed = lts.load_snapshot(path)
    assert refreshed == {"version": 2}
    assert refreshed is not first
    assert len(calls) == 2


def test_load_snapshot_cache_is_keyed_by_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    _replace_json(first_path, {"source": "first"})
    _replace_json(second_path, {"source": "second"})
    monkeypatch.setattr(
        lts, "_load_snapshot_streaming",
        lambda source: json.loads(source.read_text(encoding="utf-8")),
    )

    assert lts.load_snapshot(first_path) == {"source": "first"}
    assert lts.load_snapshot(second_path) == {"source": "second"}


def test_load_snapshot_recovers_when_file_appears_after_initial_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "late.json"
    monkeypatch.setattr(
        lts, "_load_snapshot_streaming",
        lambda source: json.loads(source.read_text(encoding="utf-8")),
    )

    assert lts.load_snapshot(path) is None
    _replace_json(path, {"ready": True})
    assert lts.load_snapshot(path) == {"ready": True}


def test_build_snapshot_registers_the_written_file_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "built.json"
    built = {"meta": {"source": "test"}}
    monkeypatch.setattr(lts, "_build_snapshot_dict", lambda **_kwargs: built)
    monkeypatch.setattr(
        lts, "_load_snapshot_streaming",
        lambda _source: pytest.fail("built snapshot was reparsed"),
    )

    assert lts.build_snapshot(snapshot_path=path) is built
    assert lts.load_snapshot(path) is built


def test_all_model_cache_stamps_notice_preserved_mtime_atomic_replace(tmp_path, monkeypatch):
    from ELO import array_model

    path = tmp_path / "state.json"
    _replace_json(path, {"version": 1})
    before = path.stat()
    monkeypatch.setattr(lts, "_JSON_DICT_CACHE", {})
    monkeypatch.setattr(array_model, "_READ_CACHE", [])
    monkeypatch.setattr(array_model, "build_read_model",
                        lambda source, *_args: json.loads(source.read_text()))
    first_model = array_model.load_read_model(path)
    first_payload = lts._load_json_dict(path)
    first_runtime_signature = lts._runtime_file_signature(path)
    _replace_json(path, {"version": 2})
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert path.stat().st_size == before.st_size
    assert lts._runtime_file_signature(path) != first_runtime_signature
    assert lts._load_json_dict(path) == {"version": 2}
    assert lts._load_json_dict(path) is not first_payload
    assert array_model.load_read_model(path) == {"version": 2}
    assert array_model.load_read_model(path) is not first_model
