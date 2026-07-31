from __future__ import annotations

import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


def test_nw60_hits2_candidate_is_forwarded_to_non_sending_shadow(monkeypatch):
    thresholds = runtime._star_thresholds_for_wr(60, "early_output")
    block = {metric: float(threshold) + 1.0 for metric, threshold in thresholds.items()}
    captured = {}

    def fake_record(**kwargs):
        captured.update(kwargs)
        return {"recorded": True}

    monkeypatch.setattr(runtime, "TEAM_KILLS25_SHADOW_AVAILABLE", True)
    monkeypatch.setattr(runtime, "_record_kills25_shadow", fake_record)
    result = runtime._record_team_kills25_shadow_candidate(
        match_key="map-1",
        match_id=1,
        observed_at=2,
        metrics_payload={"early_output": block},
        team_elo_meta={"raw_radiant_wr": 60, "raw_dire_wr": 40, "raw_diff": 80},
        radiant_team_id=1,
        dire_team_id=2,
        radiant_team_name="Radiant",
        dire_team_name="Dire",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
    )
    assert result == {"recorded": True}
    assert captured["target_side"] == "radiant"
    assert captured["nw_hit_count"] >= 2
    assert captured["nw_max_wr"] >= 60
    assert len(captured["nw_hit_metrics"]) >= 2
    assert captured["target_account_ids"] == [1, 2, 3, 4, 5]


def test_single_nw60_hit_is_not_recorded(monkeypatch):
    thresholds = runtime._star_thresholds_for_wr(60, "early_output")
    metric, threshold = next(iter(thresholds.items()))
    called = []
    monkeypatch.setattr(runtime, "TEAM_KILLS25_SHADOW_AVAILABLE", True)
    monkeypatch.setattr(runtime, "_record_kills25_shadow", lambda **kwargs: called.append(kwargs))
    result = runtime._record_team_kills25_shadow_candidate(
        match_key="map-2",
        match_id=2,
        observed_at=3,
        metrics_payload={"early_output": {metric: float(threshold) + 1.0}},
        team_elo_meta=None,
        radiant_team_id=1,
        dire_team_id=2,
        radiant_team_name="Radiant",
        dire_team_name="Dire",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
    )
    assert result is None
    assert called == []
