"""The egxrdemxn restriction is enforced at the final send boundary."""

import ast
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Isolated verification trees exclude the ignored production credentials
# (base/keys.py); same ImportError-only stub as
# base/tests/test_ml_dispatch_lane_kills.py. A real keys.py wins.
try:
    import keys  # noqa: F401,E402
except ImportError:
    import types

    _test_keys = types.ModuleType("keys")
    _test_keys.api_to_proxy = {}
    _test_keys.BOOKMAKER_PROXY_URL = None
    _test_keys.BOOKMAKER_PROXY_POOL = []
    _test_keys.DLTV_PROXY_POOL = []
    sys.modules["keys"] = _test_keys

import cyberscore_try as runtime  # noqa: E402


@pytest.fixture
def delivery(monkeypatch):
    sent = Mock()
    blocked = Mock()
    persisted = Mock()
    monkeypatch.setattr(runtime, "send_message", sent)
    monkeypatch.setattr(runtime, "_record_delivery_gate_block", blocked)
    monkeypatch.setattr(runtime, "add_url", persisted)
    monkeypatch.setattr(runtime, "_record_bet_dispatch_ledger", Mock())
    monkeypatch.setattr(runtime, "_signal_fingerprint_try_reserve", lambda *_: (True, "test"))
    monkeypatch.setattr(runtime, "_signal_fingerprint_mark_sent", Mock())
    monkeypatch.setattr(runtime, "decelerate_winline_current_map_polling", Mock())
    # Isolate unrelated policy and bookmaker state; only the outbound network
    # send is mocked at the delivery boundary.
    for gate in (
        "_dispatch_mode_reject_for_delivery",
        "_half_stake_elo_underdog_reject_for_delivery",
        "_win_model_reject_for_delivery",
        "_late_win_model_reject_for_delivery",
        "_ml_dispatch_min_odds_reject_for_delivery",
    ):
        monkeypatch.setattr(runtime, gate, lambda *args, **kwargs: None)
    monkeypatch.setattr(runtime, "_PLAYER_DENYLIST_BY_MAP", {}, raising=False)
    monkeypatch.setattr(runtime, "_PLAYER_DENYLIST_TEAM_NAMES", {}, raising=False)
    monkeypatch.setattr(runtime, "_PLAYER_DENYLIST_LOGGED_BLOCKS", set(), raising=False)
    return sent, blocked, persisted


def _deliver(message, *, side=None, ids=None, context_side=None):
    runtime._PLAYER_DENYLIST_BY_MAP[("dltv.org/matches/123", 2)] = {
        "radiant": ids if ids is not None else [390015464],
        "dire": [0, "0"],
        "radiant_team": "Mentality Monsters",
        "dire_team": "Other Team",
    }
    context = {"target_side": context_side} if context_side else None
    return runtime._deliver_and_persist_signal(
        "dltv.org/matches/123.40",
        message,
        add_url_reason="test",
        skip_bookmaker_prepare=True,
        map_num=2,
        selected_side=side,
        stake_multiplier_context=context,
    )


def test_target_team_bet_blocked_at_send(delivery):
    sent, blocked, persisted = delivery
    assert _deliver("СТАВКА НА Mentality Monsters x1\nКарта 2", side="radiant") is False
    sent.assert_not_called()
    persisted.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"


def test_opponent_bet_allowed(delivery):
    sent, blocked, persisted = delivery
    assert _deliver("СТАВКА НА Other Team x1\nКарта 2", side="dire") is True
    sent.assert_called_once()
    blocked.assert_not_called()
    persisted.assert_called_once()


def test_header_side_wins_over_stale_context(delivery):
    sent, blocked, _ = delivery
    assert _deliver(
        "СТАВКА НА Other Team x1", side="radiant", context_side="radiant"
    ) is True
    sent.assert_called_once()
    blocked.assert_not_called()


def test_steam64_target_and_zero_lineup(delivery):
    sent, blocked, _ = delivery
    assert _deliver(
        "СТАВКА НА Ранние килы 10-20 Mentality Monsters\nКарта 2",
        side="radiant",
        ids=[76561198350281192, 0],
    ) is False
    sent.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"
    sent.reset_mock()
    blocked.reset_mock()
    assert _deliver("СТАВКА НА Mentality Monsters x1", side="radiant", ids=[0, "0"]) is True
    sent.assert_called_once()
    blocked.assert_not_called()


def test_unknown_side_fails_closed_when_match_contains_player(delivery):
    sent, blocked, _ = delivery
    assert _deliver("СТАВКА НА НЕИЗВЕСТНАЯ КОМАНДА x1") is False
    sent.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"


def test_team_kills_total_blocks_player_team_but_allows_opponent(delivery):
    sent, blocked, _ = delivery
    assert _deliver("СТАВКА НА Тотал килов Mentality Monsters БОЛЬШЕ") is False
    sent.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"
    blocked.reset_mock()
    assert _deliver("СТАВКА НА Тотал килов Other Team БОЛЬШЕ") is True
    sent.assert_called_once()
    blocked.assert_not_called()


def test_map_number_mismatch_uses_series_lineup(delivery):
    sent, blocked, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/789.40", 3, [390015464], [0],
        "Mentality Monsters", "Other Team",
    )
    assert runtime._deliver_and_persist_signal(
        "dltv.org/matches/789.1", "СТАВКА НА Mentality Monsters x1",
        add_url_reason="ml_dispatch", add_url_details={"target_side": "radiant"},
        skip_bookmaker_prepare=True, map_num=1, selected_side="radiant",
    ) is False
    sent.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"


def test_team_name_backstop_with_unknown_match_key(delivery):
    sent, blocked, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/789.40", 3, [390015464], [0],
        "Mentality Monsters", "Other Team",
    )
    assert runtime._deliver_and_persist_signal(
        "dltv.org/matches/999.1", "СТАВКА НА Mentality Monsters x1",
        add_url_reason="test", skip_bookmaker_prepare=True,
    ) is False
    sent.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"


def test_display_normalized_header_resolves_player_side(delivery):
    sent, blocked, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/321.40", 2, [390015464], [0],
        "team mentality monsters", "Other Team",
    )
    assert runtime._deliver_and_persist_signal(
        "dltv.org/matches/321.40", "СТАВКА НА Mentality Monsters x1",
        add_url_reason="test", skip_bookmaker_prepare=True,
        map_num=2, selected_side="dire",
        stake_multiplier_context={"target_side": "dire"},
    ) is False
    sent.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"


def test_score_key_without_map_matches_explicit_delivery_map(delivery):
    sent, blocked, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/654.40", None, [390015464], [0],
        "Mentality Monsters", "Other Team",
    )
    assert runtime._deliver_and_persist_signal(
        "dltv.org/matches/654.1", "СТАВКА НА Mentality Monsters x1",
        add_url_reason="test", skip_bookmaker_prepare=True, map_num=3,
    ) is False
    sent.assert_not_called()
    blocked.assert_called_once()


def test_expired_team_name_backstop_allows_unknown_match(delivery):
    sent, blocked, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/789.40", 3, [390015464], [0],
        "Mentality Monsters", "Other Team",
    )
    for entry in runtime._PLAYER_DENYLIST_TEAM_NAMES.values():
        entry["ts"] -= runtime._PLAYER_DENYLIST_TEAM_NAME_TTL_SECONDS + 1
    assert runtime._deliver_and_persist_signal(
        "dltv.org/matches/999.1", "СТАВКА НА Mentality Monsters x1",
        add_url_reason="test", skip_bookmaker_prepare=True,
    ) is True
    sent.assert_called_once()
    blocked.assert_not_called()


def test_repeated_block_logs_once_but_never_sends(delivery):
    sent, blocked, persisted = delivery
    for _ in range(2):
        assert _deliver("СТАВКА НА Mentality Monsters x1", side="radiant") is False
    sent.assert_not_called()
    persisted.assert_not_called()
    blocked.assert_called_once()


def test_map_total_without_team_is_not_team_bet(delivery):
    sent, blocked, _ = delivery
    assert _deliver("СТАВКА НА Тотал килов БОЛЬШЕ") is True
    sent.assert_called_once()
    blocked.assert_not_called()


def test_series_backstop_survives_missing_ids_on_later_map(delivery):
    sent, blocked, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/123.40", 2, [390015464], [0], "Mentality Monsters", "Other Team"
    )
    assert runtime._player_denylist_map_key("dltv.org/matches/123.45", 2) in runtime._PLAYER_DENYLIST_BY_MAP
    assert runtime._player_denylist_map_key("dltv.org/matches/123.45", 3) not in runtime._PLAYER_DENYLIST_BY_MAP
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/123.1", 3, [0], [0], "Mentality Monsters", "Other Team"
    )
    assert runtime._deliver_and_persist_signal(
        "dltv.org/matches/123.1",
        "СТАВКА НА Mentality Monsters x1",
        add_url_reason="test",
        skip_bookmaker_prepare=True,
        map_num=3,
        selected_side="radiant",
    ) is False
    sent.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"


def test_delayed_payload_lineup_blocks_after_cache_loss(delivery):
    sent, blocked, _ = delivery
    assert runtime._deliver_and_persist_signal(
        "dltv.org/matches/456.10",
        "СТАВКА НА Mentality Monsters x1",
        add_url_reason="test",
        add_url_details={"player_denylist_lineups": {
            "radiant": [390015464], "dire": [0],
            "radiant_team": "Mentality Monsters", "dire_team": "Other Team",
        }},
        skip_bookmaker_prepare=True,
        map_num=1,
        selected_side="radiant",
    ) is False
    sent.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"


def test_delayed_payload_lineup_survives_incomplete_new_snapshot(delivery):
    sent, blocked, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/456.10", 1, [0], [0],
        "Mentality Monsters", "Other Team",
    )
    assert runtime._deliver_and_persist_signal(
        "dltv.org/matches/456.10", "СТАВКА НА Mentality Monsters x1",
        add_url_reason="test", skip_bookmaker_prepare=True, map_num=1,
        add_url_details={"player_denylist_lineups": {
            "radiant": [390015464], "dire": [0],
            "radiant_team": "Mentality Monsters", "dire_team": "Other Team",
        }},
    ) is False
    sent.assert_not_called()
    blocked.assert_called_once()


def test_find_skipped_player_ids_normalizes_steam64_and_ignores_zero():
    assert runtime._find_skipped_player_account_ids(
        [390015464, "76561198350281192", 0], [1250582363, "0"]
    ) == {"radiant": [390015464], "dire": [1250582363]}


@pytest.mark.parametrize("builder_index", range(4))
def test_every_persisted_delayed_builder_carries_lineups(builder_index):
    tree = ast.parse(Path(runtime.__file__).read_text())
    builders = sorted(
        (node for node in ast.walk(tree)
         if isinstance(node, ast.Assign)
         and any(isinstance(target, ast.Name) and target.id == "delayed_payload"
                 for target in node.targets)
         and isinstance(node.value, ast.Dict)
         and "add_url_reason" in [key.value for key in node.value.keys
                                  if isinstance(key, ast.Constant)]),
        key=lambda node: node.lineno,
    )
    assert len(builders) == 4
    payload = builders[builder_index].value
    fields = {key.value: value for key, value in zip(payload.keys, payload.values)
              if isinstance(key, ast.Constant)}
    lineups = fields["player_denylist_lineups"]
    assert isinstance(lineups, ast.Dict)
    assert {key.value for key in lineups.keys} == {
        "radiant", "dire", "radiant_team", "dire_team"
    }
    assert {key.value: value.id for key, value in zip(lineups.keys, lineups.values)
            if isinstance(key, ast.Constant) and isinstance(value, ast.Name)} == {
        "radiant": "radiant_account_ids",
        "dire": "dire_account_ids",
        "radiant_team": "radiant_team_name_original",
        "dire_team": "dire_team_name_original",
    }


@pytest.mark.parametrize("reason", [
    "late_star_comeback_ceiling_monitor",
    "post_target_comeback_ceiling_monitor",
])
@pytest.mark.parametrize("target_side", ["radiant", "dire"])
def test_persisted_comeback_payload_gate_and_memory_after_restart(
    tmp_path, monkeypatch, delivery, reason, target_side,
):
    sent, blocked, _ = delivery
    match_key = "dltv.org/matches/456.10"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(tmp_path / "delayed.json"))
    monkeypatch.setattr(runtime, "_is_url_processed", lambda *_: False)
    monkeypatch.setattr(runtime, "_fetch_delayed_match_state", lambda *_: {
        "game_time": 1200.0, "radiant_lead": -1000.0,
    })
    monkeypatch.setattr(runtime, "_late_comeback_monitor_check", lambda **_: {"ready": True})
    monkeypatch.setattr(runtime, "_evaluate_late27_dispatch_guard", lambda *_, **__: {"blocked": False})
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_: None)
    monkeypatch.setattr(runtime, "_refresh_stake_multiplier_message", lambda message, **_: message)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_: None)
    monkeypatch.setattr(runtime, "_record_map_verdict", lambda *_, **__: None)
    monkeypatch.setattr(runtime, "_update_delayed_match", lambda *_, **__: True)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_, **__: None)
    monkeypatch.setattr(runtime, "_bookmaker_prepare_message_for_delivery", lambda _key, message, **_: (message, True, None, None))
    monkeypatch.setattr(runtime, "_bookmaker_release_match_tabs", lambda *_: None)
    monkeypatch.setattr(runtime, "_maybe_strip_early_kills_header_late", lambda *_, **__: None)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_, **__: False)
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    payload = {
        "message": "СТАВКА НА " + ("Mentality Monsters" if target_side == "radiant" else "Other Team") + " x1",
        "stake_multiplier_context": {"target_side": target_side},
        "reason": reason,
        "json_url": "https://dltv.org/live/456.json",
        "target_game_time": 1100.0,
        "queued_at": runtime.time.time() - 10,
        "queued_game_time": 1000.0,
        "last_game_time": 1000.0,
        "last_progress_at": runtime.time.time() - 10,
        "add_url_reason": "star_signal_sent_delayed",
        "add_url_details": {"target_side": target_side},
        "late_comeback_monitor_active": True,
        "late_comeback_monitor_deadline_game_time": 1300.0,
        "networth_target_side": target_side,
        "send_on_target_game_time": False,
        "allow_live_recheck": False,
        "player_denylist_lineups": {
            "radiant": [390015464], "dire": [0],
            "radiant_team": "Mentality Monsters", "dire_team": "Other Team",
        },
    }
    runtime._set_delayed_match(match_key, payload)
    runtime._replace_monitored_matches_from_snapshot(runtime._load_delayed_queue_state(recover=False))
    assert runtime._PLAYER_DENYLIST_BY_MAP == {}
    assert runtime._PLAYER_DENYLIST_TEAM_NAMES == {}
    runtime._drain_due_delayed_signals_once(only_match_key=match_key)
    if target_side == "radiant":
        sent.assert_not_called()
        assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"
    else:
        sent.assert_called_once()
        blocked.assert_not_called()
    assert "Mentality Monsters" in runtime._PLAYER_DENYLIST_TEAM_NAMES
    sent.reset_mock()
    blocked.reset_mock()
    assert runtime._deliver_and_persist_signal(
        "dltv.org/matches/999.1", "СТАВКА НА Mentality Monsters x1",
        add_url_reason="test", skip_bookmaker_prepare=True,
    ) is False
    sent.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
