"""Регрессии кеша валидированной дельты для read-only ELO-модели."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ELO import array_model, state_overlay  # noqa: E402
from ELO.config import HybridEloConfig  # noqa: E402
from ELO.models import HybridPlayerRosterEloModel  # noqa: E402

REFERENCE = 1_788_387_568
SIGNATURE = "delta-cache-test"


@pytest.fixture(autouse=True)
def _clear_caches():
    array_model._READ_CACHE.clear()
    array_model._OVERLAY_CACHE.clear()
    array_model._META_CACHE.clear()
    yield
    array_model._READ_CACHE.clear()
    array_model._OVERLAY_CACHE.clear()
    array_model._META_CACHE.clear()


def _snapshot(path: Path) -> Path:
    state = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    path.write_text(json.dumps({
        "meta": {"reference_timestamp": REFERENCE,
                 "model_config_signature": SIGNATURE},
        "teams_by_org_key": {},
        "model_state": state,
    }), encoding="utf-8")
    return path


def _delta(path: Path, rating: float, *, reference: int = REFERENCE,
           signature: str = SIGNATURE) -> None:
    state_overlay.save_delta(
        path, base_reference_timestamp=reference,
        base_model_config_signature=signature,
        changes={"player_global": [[11, rating]]}, resets={}, small_parts={}, updated_at=1,
    )


def test_read_model_parses_valid_delta_once_cold_and_not_on_warm_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path / "snapshot.json")
    delta = tmp_path / "live_elo_delta.json"
    _delta(delta, 1600.0)
    original = state_overlay.load_delta
    calls = []

    def counted(*args, **kwargs):
        calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(state_overlay, "load_delta", counted)

    cold = array_model.load_read_model(snapshot, None, delta)
    warm = array_model.load_read_model(snapshot, None, delta)

    assert float(cold.player_global[11]) == pytest.approx(1600.0)
    assert warm is cold
    assert len(calls) == 1


def test_read_model_refreshes_after_atomic_delta_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path / "snapshot.json")
    delta = tmp_path / "live_elo_delta.json"
    _delta(delta, 1600.0)
    original = state_overlay.load_delta
    calls = []

    def counted(*args, **kwargs):
        calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(state_overlay, "load_delta", counted)
    first = array_model.load_read_model(snapshot, None, delta)
    _delta(delta, 1700.0)
    refreshed = array_model.load_read_model(snapshot, None, delta)

    assert float(first.player_global[11]) == pytest.approx(1600.0)
    assert float(refreshed.player_global[11]) == pytest.approx(1700.0)
    assert refreshed is not first
    assert len(calls) == 2


def test_rekeyed_valid_overlay_is_reused_without_reparsing_delta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path / "snapshot.json")
    delta = tmp_path / "live_elo_delta.json"
    _delta(delta, 1600.0)
    original = state_overlay.load_delta
    calls = []

    def counted(*args, **kwargs):
        calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(state_overlay, "load_delta", counted)
    live = array_model.load_read_model(snapshot, None, delta)
    _delta(delta, 1600.0)
    array_model.rekey_overlay_cache(live, snapshot, delta)

    assert array_model.load_read_model(snapshot, None, delta) is live
    assert len(calls) == 1


def test_snapshot_replace_during_overlay_build_falls_back_without_caching_delta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path / "snapshot.json")
    delta = tmp_path / "live_elo_delta.json"
    runtime = tmp_path / "runtime.json"
    _delta(delta, 1600.0)
    calls = []
    replaced = False
    fallback = object()

    def build(source: Path, runtime_path: Path | None):
        nonlocal replaced
        calls.append((source, runtime_path))
        if runtime_path is None and not replaced:
            replacement = _snapshot(tmp_path / "snapshot.next")
            os.replace(replacement, snapshot)
            replaced = True
            return object()
        return fallback

    monkeypatch.setattr(array_model, "build_read_model", build)

    assert array_model.load_read_model(snapshot, runtime, delta) is fallback
    assert calls == [(snapshot, None), (snapshot, runtime)]
    assert array_model._OVERLAY_CACHE == []


def test_direct_overlay_refuses_snapshot_replace_during_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(tmp_path / "snapshot.json")
    delta = tmp_path / "live_elo_delta.json"
    _delta(delta, 1600.0)

    def replace_snapshot(source: Path, _runtime_path: Path | None):
        replacement = _snapshot(tmp_path / "snapshot.next")
        os.replace(replacement, snapshot)
        return object()

    monkeypatch.setattr(array_model, "load_read_model", replace_snapshot)

    with pytest.raises(RuntimeError, match="снимок сменился"):
        array_model.build_overlay_model(snapshot, delta)


def test_rekey_refuses_model_from_replaced_snapshot(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path / "snapshot.json")
    delta = tmp_path / "live_elo_delta.json"
    _delta(delta, 1600.0)
    live = array_model.load_read_model(snapshot, None, delta)
    replacement = _snapshot(tmp_path / "snapshot.next")
    os.replace(replacement, snapshot)

    array_model.rekey_overlay_cache(live, snapshot, delta)

    current_key = array_model._overlay_key(snapshot, delta)
    assert all(key != current_key for key, _model in array_model._OVERLAY_CACHE)


@pytest.mark.parametrize("kind", ("corrupt", "mismatch", "missing"))
def test_invalid_or_missing_delta_uses_runtime_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str,
) -> None:
    snapshot = _snapshot(tmp_path / "snapshot.json")
    delta = tmp_path / "live_elo_delta.json"
    if kind == "corrupt":
        delta.write_text("{not json", encoding="utf-8")
    elif kind == "mismatch":
        _delta(delta, 1600.0, reference=REFERENCE + 1)
    runtime = tmp_path / "runtime.json"
    fallback = object()
    calls = []

    def build(source: Path, runtime_path: Path | None):
        calls.append((source, runtime_path))
        return fallback

    monkeypatch.setattr(array_model, "build_read_model", build)

    assert array_model.load_read_model(snapshot, runtime, delta) is fallback
    assert calls == [(snapshot, runtime)]


def test_invalid_direct_overlay_cannot_be_promoted_by_rekey(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path / "snapshot.json")
    delta = tmp_path / "live_elo_delta.json"
    _delta(delta, 1600.0, signature="foreign")

    invalid = array_model.build_overlay_model(snapshot, delta)
    array_model.rekey_overlay_cache(invalid, snapshot, delta)

    assert array_model._OVERLAY_CACHE == []
