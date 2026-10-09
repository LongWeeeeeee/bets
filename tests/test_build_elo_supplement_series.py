"""OpenDota series_id 0/null must not group unrelated supplement maps (card ingame-kri1).

OpenDota explorer answers series_id 0 for 3,969 and null for 210 of all 75,719
league listing rows captured 08.10.2026 03:00 MSK; among the 53,249 maps shared
with the STRATZ corpus, these counts are 1,968 and 45 respectively
(runtime/artifacts/elo/prod_review_20261006/od_supp, e.g. match 7526549985:
corpus series 101702386, OpenDota 0). Every positive id equals the STRATZ corpus
id on all 51,236 shared maps with positive series_id (K2 check, scratchpad k2).
ELO/series_data.py:8-12 groups maps by (series_id, team pair) and falls back to
-match_id only for None, so two unrelated maps of the same pair with series_id 0
would form one series. The converter must therefore publish 0 as None.

Input: the captured fixture row 7515635423 and its 10 captured player rows
(tests/fixtures/od_explorer_20261008); the series_id value is set to the shapes
OpenDota returns (0, None) and a positive id must pass through unchanged.
"""
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures/od_explorer_20261008"
MATCH_ID = 7515635423


def _converter():
    spec = importlib.util.spec_from_file_location(
        "build_elo_supplement_under_test", ROOT / "scripts/pro_chain/build_elo_supplement.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _captured_row():
    for path in sorted(FIXTURES.glob("listing_*.json")):
        for row in json.loads(path.read_text(encoding="utf-8")):
            if row["match_id"] == MATCH_ID:
                return row
    raise AssertionError("captured fixture row missing")


@pytest.mark.parametrize("series_id, expected", [(0, None), (None, None), (838318, 838318)])
def test_nonpositive_opendota_series_id_is_published_as_none(tmp_path, series_id, expected):
    row = dict(_captured_row(), series_id=series_id)
    players = [p for p in json.loads((FIXTURES / "players_7515635423.json").read_text(encoding="utf-8"))
               if p["match_id"] == MATCH_ID]
    assert len(players) == 10
    (tmp_path / "listing_1.json").write_text(json.dumps([row]), encoding="utf-8")
    (tmp_path / "players_1.json").write_text(json.dumps(players), encoding="utf-8")
    maps, _counts = _converter().convert(tmp_path, set())
    converted = maps[str(MATCH_ID)] if isinstance(maps, dict) else maps[0]
    assert converted["series"]["id"] == expected
