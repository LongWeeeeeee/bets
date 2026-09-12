"""Кеш валидности живой ELO-дельты не должен удерживать её JSON."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ELO import live_team_strength as lts, state_overlay  # noqa: E402

SNAPSHOT = {"meta": {"reference_timestamp": 1_000,
                     "model_config_signature": "cache-test"}}


@pytest.fixture(autouse=True)
def _clear_delta_usability_cache():
    lts._DELTA_USABILITY_CACHE.update({"key": None, "usable": False})
    yield
    lts._DELTA_USABILITY_CACHE.update({"key": None, "usable": False})


def _save(path: Path, rating: float, *, reference: int = 1_000,
          signature: str = "cache-test") -> None:
    state_overlay.save_delta(
        path, base_reference_timestamp=reference,
        base_model_config_signature=signature,
        changes={"player_global": [[11, rating]]}, resets={}, small_parts={}, updated_at=1,
    )


def test_usable_delta_is_parsed_once_until_file_identity_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    delta = tmp_path / "live_elo_delta.json"
    _save(delta, 1600.0)
    original = state_overlay.load_delta
    calls = []

    def counted(*args, **kwargs):
        calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(state_overlay, "load_delta", counted)

    assert lts._delta_is_usable(SNAPSHOT, delta)
    assert lts._delta_is_usable(SNAPSHOT, delta)
    _save(delta, 1700.0)
    assert lts._delta_is_usable(SNAPSHOT, delta)
    assert len(calls) == 2


@pytest.mark.parametrize("kind", ("corrupt", "mismatch", "missing"))
def test_unusable_delta_is_not_negative_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str,
) -> None:
    delta = tmp_path / "live_elo_delta.json"
    if kind == "corrupt":
        delta.write_text("{not json", encoding="utf-8")
    elif kind == "mismatch":
        _save(delta, 1600.0, signature="foreign")
    original = state_overlay.load_delta
    calls = []

    def counted(*args, **kwargs):
        calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(state_overlay, "load_delta", counted)

    assert not lts._delta_is_usable(SNAPSHOT, delta)
    assert not lts._delta_is_usable(SNAPSHOT, delta)
    assert len(calls) == (2 if kind != "missing" else 0)


def test_delta_replaced_while_validating_is_not_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    delta = tmp_path / "live_elo_delta.json"
    _save(delta, 1600.0)
    original = state_overlay.load_delta
    calls = []
    replaced = False

    def replace_after_read(*args, **kwargs):
        nonlocal replaced
        calls.append((args, kwargs))
        payload = original(*args, **kwargs)
        if not replaced:
            replacement = tmp_path / "replacement.json"
            _save(replacement, 1700.0)
            os.replace(replacement, delta)
            replaced = True
        return payload

    monkeypatch.setattr(state_overlay, "load_delta", replace_after_read)

    assert not lts._delta_is_usable(SNAPSHOT, delta)
    assert lts._delta_is_usable(SNAPSHOT, delta)
    assert len(calls) == 2
