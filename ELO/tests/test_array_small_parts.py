"""Selective streaming regression tests for :func:`array_model._small_parts`."""
from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import ijson

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ELO import array_model  # noqa: E402


def _legacy_small_parts(path: Path) -> dict:
    out = {}
    with path.open("rb") as fh:
        for key, value in ijson.kvitems(fh, "model_state"):
            if key in array_model._KEEP_AS_IS:
                out[key] = value
    return out


def test_small_parts_matches_legacy_parser_for_nested_values(tmp_path):
    state = {
        "config": {"k_global": 24.123456789012345, "nested": {"x": [1, 2, {"z": "ok"}]}},
        "current_patch_key": "7.41e",
        "side_bias": {"TIER1": -0.000000000123456789, "TIER2": 3},
        "roster_tracker": {"teams": [{"id": 11, "active": True}], "none": None},
        "player_global": {str(i): i for i in range(100)},
    }
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps({"model_state": state}), encoding="utf-8")

    assert array_model._small_parts(path) == _legacy_small_parts(path)
    assert isinstance(array_model._small_parts(path)["config"]["k_global"], Decimal)


def test_small_parts_does_not_materialize_ignored_subtree(tmp_path, monkeypatch):
    ignored = {"player": {str(i): {"v": i} for i in range(2000)}}
    state = {
        "config": {"k_global": 24.0},
        "current_patch_key": None,
        "side_bias": [],
        "roster_tracker": {},
        "player_global": ignored,
    }
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps({"model_state": state}), encoding="utf-8")

    seen = []
    original = ijson.common.ObjectBuilder

    class TrackingBuilder(original):
        def event(self, event, value):
            seen.append((event, value))
            return super().event(event, value)

    monkeypatch.setattr(ijson.common, "ObjectBuilder", TrackingBuilder)
    assert array_model._small_parts(path) == {
        "config": {"k_global": 24},
        "current_patch_key": None,
        "side_bias": [],
        "roster_tracker": {},
    }
    assert seen == [
        ("start_map", None),
        ("map_key", "k_global"),
        ("number", 24),
        ("end_map", None),
        ("null", None),
        ("start_array", None),
        ("end_array", None),
        ("start_map", None),
        ("end_map", None),
    ]
