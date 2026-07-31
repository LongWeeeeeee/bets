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


def _qualifying_history(team_id: int = 10) -> tuple[list[int], dict]:
    player_ids = [1, 2, 3, 4, 5]
    return player_ids, {
        "team_kills_history_by_team_id": {
            str(team_id): [
                {
                    "match_id": 100 + index,
                    "timestamp": 1,
                    "player_ids": player_ids,
                    "kills": 25,
                }
                for index in range(6)
            ]
        }
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


def test_roster_history_requires_four_players_and_excludes_current_future_and_duplicates():
    current_players = [1, 2, 3, 4, 5]
    rows = [
        {
            "match_id": index,
            "timestamp": 10 + index,
            "player_ids": [1, 2, 3, 4, 100 + index],
            "kills": kills,
        }
        for index, kills in enumerate([27, 9, 15, 27, 12, 6], start=1)
    ]
    rows.extend(
        [
            {**rows[0], "kills": 99},  # duplicate match_id must not add a map
            {
                "match_id": 50,
                "timestamp": 50,
                "player_ids": [1, 2, 3, 90, 91],
                "kills": 40,
            },  # only three current players
            {
                "match_id": 999,
                "timestamp": 90,
                "player_ids": current_players,
                "kills": 50,
            },  # current map
            {
                "match_id": 1000,
                "timestamp": 101,
                "player_ids": current_players,
                "kills": 50,
            },  # future map in replay/backtest
        ]
    )
    context = shadow.build_roster_kills_context(
        team_id=10,
        current_account_ids=current_players,
        observed_at=100,
        current_match_id=999,
        history_snapshot={"team_kills_history_by_team_id": {"10": rows}},
    )
    assert context["matches"] == 6
    assert context["mean_kills"] == 16.0
    assert context["median_kills"] == 13.5
    assert context["ge25_hits"] == 2
    assert context["ge25_rate"] == 2 / 6


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


def test_qualified_telegram_bet_is_sent_once_across_process_restart(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("TEAM_KILLS25_SHADOW_ENABLED", "1")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_ENABLED", "1")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_BOT_TOKEN", "secret-test-token")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_CHAT_ID", "123")
    sent_path = tmp_path / "telegram-sent.jsonl"
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_SENT_PATH", str(sent_path))
    calls = []

    def fake_post(**kwargs):
        calls.append(kwargs)
        return True, None

    monkeypatch.setattr(shadow, "_post_telegram_message", fake_post)
    target_account_ids, roster_history_snapshot = _qualifying_history()
    kwargs = {
        "match_key": "dltv.org/matches/12345.26",
        "match_id": 12345,
        "observed_at": 2,
        "target_side": "radiant",
        "target_team_id": 10,
        "target_team_name": "Target Team",
        "opponent_team_id": 20,
        "opponent_team_name": "Opponent Team",
        "tier_segment": "T1-T2",
        "nw_hit_count": 2,
        "nw_max_wr": 65,
        "nw_hit_metrics": ["solo", "counterpick_1vs1"],
        "metrics_payload": _metrics(),
        "team_elo_meta": {
            "raw_radiant_wr": 60,
            "raw_dire_wr": 40,
            "raw_diff": 80,
        },
        "target_account_ids": target_account_ids,
        "roster_history_snapshot": roster_history_snapshot,
        "artifact": _artifact(),
        "log_path": tmp_path / "shadow.jsonl",
    }

    shadow.reset_shadow_state_for_tests()
    first = shadow.record_shadow_observation(**kwargs)
    assert first is not None
    assert first["telegram"]["sent"] is True
    assert len(calls) == 1
    assert "Target Team: 25+ убийств" in calls[0]["text"]
    assert "secret-test-token" not in json.dumps(first)

    # Simulate a new process: in-memory state is empty, persistent sent log remains.
    shadow.reset_shadow_state_for_tests()
    second = shadow.record_shadow_observation(
        **{**kwargs, "match_key": "dltv.org/matches/12345.30"}
    )
    assert second is not None
    assert second["telegram"]["reason"] == "already_sent"
    assert len(calls) == 1
    assert len(sent_path.read_text(encoding="utf-8").splitlines()) == 1


def test_telegram_dedupe_reads_legacy_volatile_match_key(monkeypatch, tmp_path):
    monkeypatch.setenv("TEAM_KILLS25_SHADOW_ENABLED", "1")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_ENABLED", "1")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_BOT_TOKEN", "secret-test-token")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_CHAT_ID", "123")
    sent_path = tmp_path / "telegram-sent.jsonl"
    sent_path.write_text(
        json.dumps(
            {
                "match_key": "dltv.org/matches/8922211678.26",
                "sent_at": 1785496788,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_SENT_PATH", str(sent_path))
    calls = []
    monkeypatch.setattr(
        shadow,
        "_post_telegram_message",
        lambda **kwargs: calls.append(kwargs) or (True, None),
    )
    shadow.reset_shadow_state_for_tests()
    target_account_ids, roster_history_snapshot = _qualifying_history()
    result = shadow.record_shadow_observation(
        match_key="dltv.org/matches/8922211678.38",
        match_id=8922211678,
        observed_at=2,
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
        target_account_ids=target_account_ids,
        roster_history_snapshot=roster_history_snapshot,
        artifact=_artifact(),
        log_path=tmp_path / "shadow.jsonl",
    )
    assert result is not None
    assert result["telegram"]["reason"] == "already_sent"
    assert calls == []


def test_telegram_claim_prevents_retry_after_ambiguous_failure(monkeypatch, tmp_path):
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_ENABLED", "1")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_BOT_TOKEN", "secret-test-token")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_CHAT_ID", "123")
    sent_path = tmp_path / "telegram-sent.jsonl"
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_SENT_PATH", str(sent_path))
    calls = []
    monkeypatch.setattr(
        shadow,
        "_post_telegram_message",
        lambda **kwargs: calls.append(kwargs) or (False, "request_Timeout"),
    )
    record = {
        "match_key": "dltv.org/matches/7777777777.1",
        "match_id": 7777777777,
        "ml_probability": 0.8,
        "ml_threshold": 0.6,
        "nw_max_wr": 65,
        "roster_kills": {
            "available": True,
            "matches": 6,
            "mean_kills": 25.0,
            "median_kills": 25.0,
            "ge25_hits": 6,
            "ge25_rate": 1.0,
        },
    }
    shadow.reset_shadow_state_for_tests()
    first = shadow._maybe_send_telegram_bet(record)
    second = shadow._maybe_send_telegram_bet(
        {**record, "match_key": "dltv.org/matches/7777777777.2"}
    )
    assert first["sent"] is False
    assert first["reason"] == "request_Timeout"
    assert second["reason"] == "already_sent"
    assert len(calls) == 1


def test_glyph_recent_roster_average_blocks_the_bet(monkeypatch):
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_ENABLED", "1")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_BOT_TOKEN", "secret-test-token")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_CHAT_ID", "123")
    calls = []
    monkeypatch.setattr(
        shadow,
        "_post_telegram_message",
        lambda **kwargs: calls.append(kwargs) or (True, None),
    )
    # Four stable GLYPH players are enough to keep the lineage when the mid changes.
    current_players = [392565237, 202217968, 147767183, 152859296, 343084576]
    stable_four = [392565237, 147767183, 152859296, 343084576]
    rows = [
        {
            "match_id": 800 + index,
            "timestamp": 10 + index,
            "player_ids": stable_four + [392169957],
            "kills": kills,
        }
        for index, kills in enumerate([27, 9, 15, 27, 12, 6])
    ]
    roster = shadow.build_roster_kills_context(
        team_id=10081680,
        current_account_ids=current_players,
        observed_at=100,
        current_match_id=8922300474,
        history_snapshot={"team_kills_history_by_team_id": {"10081680": rows}},
    )
    result = shadow._maybe_send_telegram_bet(
        {
            "match_key": "dltv.org/matches/8922300474.1",
            "match_id": 8922300474,
            "ml_probability": 0.608,
            "ml_threshold": 0.45,
            "nw_max_wr": 65,
            "roster_kills": roster,
        }
    )
    assert roster["matches"] == 6
    assert roster["mean_kills"] == 16.0
    assert result["eligible"] is False
    assert result["reason"] == "roster_avg_kills_below_threshold"
    assert calls == []


def test_telegram_gate_does_not_send_below_artifact_threshold(monkeypatch, tmp_path):
    monkeypatch.setenv("TEAM_KILLS25_SHADOW_ENABLED", "1")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_ENABLED", "1")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_BOT_TOKEN", "secret-test-token")
    monkeypatch.setenv("TEAM_KILLS25_TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv(
        "TEAM_KILLS25_TELEGRAM_SENT_PATH", str(tmp_path / "telegram-sent.jsonl")
    )
    calls = []
    monkeypatch.setattr(
        shadow,
        "_post_telegram_message",
        lambda **kwargs: calls.append(kwargs) or (True, None),
    )
    artifact = _artifact()
    artifact["bet_threshold"] = 0.90
    shadow.reset_shadow_state_for_tests()
    result = shadow.record_shadow_observation(
        match_key="map-telegram-low",
        match_id=2,
        observed_at=2,
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
        artifact=artifact,
        log_path=tmp_path / "shadow.jsonl",
    )
    assert result is not None
    assert result["telegram"]["eligible"] is False
    assert calls == []
