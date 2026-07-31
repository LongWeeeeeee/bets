from __future__ import annotations

import json
import math
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import team_kills25_shadow as shadow  # noqa: E402


def _metrics() -> dict:
    return {
        "early_output": {"solo": 5, "counterpick_1vs1": 4},
        "early_end_output": {"solo": 3, "counterpick_1vs1": -2},
        "mid_output": {"solo": -6, "synergy_duo": 7},
        "all_output": {"solo": 2, "counterpick_1vs1": 8},
    }


def _artifact() -> dict:
    size = len(shadow.FEATURE_NAMES)
    coefficients = [0.0] * size
    coefficients[shadow.FEATURE_NAMES.index("elo_target_win_prob")] = 1.0
    return {
        "schema_version": shadow.SCHEMA_VERSION,
        "feature_schema_hash": shadow.FEATURE_SCHEMA_HASH,
        "feature_names": list(shadow.FEATURE_NAMES),
        "bet_threshold": 0.6,
        "created_at_utc": "2026-07-31T00:00:00+00:00",
        "model": {
            "medians": [0.0] * size,
            "means": [0.0] * size,
            "scales": [1.0] * size,
            "coefficients": coefficients,
            "intercept": 0.0,
        },
    }


def test_build_features_aligns_every_block_to_target_side():
    radiant = shadow.build_features(
        metrics_payload=_metrics(),
        team_elo_meta={"raw_radiant_wr": 60, "raw_dire_wr": 40, "raw_diff": 80},
        target_side="radiant",
        nw_hit_count=2,
        nw_max_wr=65,
    )
    dire = shadow.build_features(
        metrics_payload=_metrics(),
        team_elo_meta={"raw_radiant_wr": 60, "raw_dire_wr": 40, "raw_diff": 80},
        target_side="dire",
        nw_hit_count=2,
        nw_max_wr=65,
    )
    assert radiant["early_nw_solo"] == 5
    assert radiant["nw_max_wr"] == 65
    assert dire["early_nw_solo"] == -5
    assert radiant["early_win_counterpick_1vs1"] == -2
    assert dire["early_win_counterpick_1vs1"] == 2
    assert radiant["late_solo"] == -6
    assert dire["late_solo"] == 6
    assert radiant["all_counterpick_1vs1"] == 8
    assert dire["all_counterpick_1vs1"] == -8
    assert radiant["elo_target_win_prob"] == 0.6
    assert dire["elo_target_win_prob"] == 0.4
    assert radiant["elo_target_diff"] == 80
    assert dire["elo_target_diff"] == -80


def test_predict_probability_uses_frozen_numeric_artifact():
    features = {name: 0.0 for name in shadow.FEATURE_NAMES}
    features["elo_target_win_prob"] = 1.0
    probability = shadow.predict_probability(features, _artifact())
    assert math.isclose(probability, 1.0 / (1.0 + math.exp(-1.0)))


def test_shadow_record_is_disabled_by_default(monkeypatch, tmp_path):
    monkeypatch.delenv("TEAM_KILLS25_SHADOW_ENABLED", raising=False)
    shadow.reset_shadow_state_for_tests()
    output = tmp_path / "shadow.jsonl"
    result = shadow.record_shadow_observation(
        match_key="map-1",
        match_id=1,
        observed_at=1,
        target_side="radiant",
        target_team_id=10,
        target_team_name="A",
        opponent_team_id=20,
        opponent_team_name="B",
        tier_segment="T1-T2",
        nw_hit_count=2,
        nw_max_wr=65,
        nw_hit_metrics=["solo", "counterpick_1vs1"],
        metrics_payload=_metrics(),
        team_elo_meta={"raw_radiant_wr": 60, "raw_dire_wr": 40, "raw_diff": 80},
        artifact=_artifact(),
        log_path=output,
    )
    assert result is None
    assert not output.exists()


def test_shadow_record_writes_once_and_never_contains_outcome(monkeypatch, tmp_path):
    monkeypatch.setenv("TEAM_KILLS25_SHADOW_ENABLED", "1")
    shadow.reset_shadow_state_for_tests()
    output = tmp_path / "shadow.jsonl"
    kwargs = {
        "match_key": "map-2",
        "match_id": 2,
        "observed_at": 2,
        "target_side": "radiant",
        "target_team_id": 10,
        "target_team_name": "A",
        "opponent_team_id": 20,
        "opponent_team_name": "B",
        "tier_segment": "T1-T2",
        "nw_hit_count": 2,
        "nw_max_wr": 65,
        "nw_hit_metrics": ["solo", "counterpick_1vs1"],
        "metrics_payload": _metrics(),
        "team_elo_meta": {"raw_radiant_wr": 60, "raw_dire_wr": 40, "raw_diff": 80},
        "artifact": _artifact(),
        "log_path": output,
    }
    first = shadow.record_shadow_observation(**kwargs)
    second = shadow.record_shadow_observation(**kwargs)
    assert first is not None
    assert second is None
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert rows[0]["elo_gate_eligible"] is True
    assert rows[0]["ml_probability"] is not None
    assert "target_ge25" not in rows[0]
    assert "final_kills" not in rows[0]
