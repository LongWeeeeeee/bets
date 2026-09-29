"""Yangon Galacticos bets stop at the final delivery boundary."""

import ast
import inspect
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    import keys  # noqa: F401,E402
except ImportError:
    import types

    test_keys = types.ModuleType("keys")
    test_keys.api_to_proxy = {}
    test_keys.BOOKMAKER_PROXY_URL = None
    test_keys.BOOKMAKER_PROXY_POOL = []
    test_keys.DLTV_PROXY_POOL = []
    sys.modules["keys"] = test_keys

import cyberscore_try as runtime  # noqa: E402


@pytest.fixture
def delivery(monkeypatch):
    sent = Mock()
    blocked = Mock()
    persisted = Mock()
    dropped = Mock()
    monkeypatch.setattr(runtime, "send_message", sent)
    monkeypatch.setattr(runtime, "_record_delivery_gate_block", blocked)
    monkeypatch.setattr(runtime, "add_url", persisted)
    monkeypatch.setattr(runtime, "_drop_delayed_match", dropped)
    monkeypatch.setattr(runtime, "_record_bet_dispatch_ledger", Mock())
    monkeypatch.setattr(runtime, "_signal_fingerprint_try_reserve", lambda *_: (True, "test"))
    monkeypatch.setattr(runtime, "_signal_fingerprint_mark_sent", Mock())
    monkeypatch.setattr(runtime, "decelerate_winline_current_map_polling", Mock())
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
    monkeypatch.setattr(runtime, "_TEAM_DENYLIST_LOGGED_BLOCKS", set(), raising=False)
    monkeypatch.setattr(runtime, "_TEAM_DENYLIST_LEARNED_NAMES", set(), raising=False)
    monkeypatch.setattr(runtime, "_TEAM_DENYLIST_ID_ALIASES", None, raising=False)
    monkeypatch.setattr(runtime, "_TEAM_DENYLIST_ID_ALIASES_TS", None, raising=False)
    return sent, blocked, persisted, dropped


def _deliver(message, *, side="radiant", details=None, context=None,
             match_key="dltv.org/matches/yangon.1", map_num=1):
    return runtime._deliver_and_persist_signal(
        match_key,
        message,
        add_url_reason="test",
        add_url_details=details,
        skip_bookmaker_prepare=True,
        map_num=map_num,
        selected_side=side,
        stake_multiplier_context=context,
    )


@pytest.mark.parametrize("header", [
    "СТАВКА НА Yangon Galacticos x1",
    "СТАВКА НА Yangon Galacticos x0.5",
    "СТАВКА НА Тотал килов Yangon Galacticos БОЛЬШЕ",
    "СТАВКА НА Ранние килы 5-15 Yangon Galacticos",
    "СТАВКА НА килы от Yangon Galacticos",
    "СТАВКА НА   YANGON   GALACTICOS   x1",
    "СТАВКА НА Yangon Galacticos x1\nКарта 0",
    "СТАВКА НА Yangon Galacticos Esports x1",
    "СТАВКА НА Yangon x1",
    "СТАВКА НА Yangon Galácticos x1",
])
def test_yangon_bet_is_blocked_before_send(delivery, header):
    sent, blocked, persisted, _ = delivery
    assert _deliver(header) is False
    sent.assert_not_called()
    persisted.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_team_denylist"


@pytest.mark.parametrize("header", [
    "СТАВКА НА Xipto Esports x1",
    "СТАВКА НА Тотал килов БОЛЬШЕ",
    "СТАВКА НА Galacticos x1",
    "СТАВКА НА Super Galacticos x1",
    "СТАВКА НА Yangon Warriors x1",
    "СТАВКА НА Team Yangon x1",
    "СТАВКА НА PIPELINE CHECK Yangon Galacticos x1",
])
def test_other_bets_still_send(delivery, header):
    sent, blocked, persisted, _ = delivery
    assert _deliver(header, side="dire") is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


def test_side_bound_observation_learns_alias_but_not_opponent(delivery):
    sent, blocked, persisted, _ = delivery
    details = {
        "radiant_team": "YG", "radiant_team_id": 9546449,
        "dire_team": "Xipto Esports", "dire_team_id": 10242397,
    }
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/yangon.1", 1, [], [],
        "YG", "Xipto Esports", [9546449], [10242397],
    )
    assert _deliver("СТАВКА НА YG x1", details=details) is False
    sent.assert_not_called()
    persisted.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_team_denylist"
    blocked.reset_mock()
    assert _deliver("СТАВКА НА Xipto Esports x1", side="radiant", details=details) is True
    sent.assert_called_once()
    blocked.assert_not_called()


def test_unknown_tag_without_id_observation_is_sent(delivery):
    sent, blocked, persisted, _ = delivery
    assert _deliver("СТАВКА НА YG x1") is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


@pytest.mark.parametrize("team", [
    "Yangon Galacticos Esports", "Yangon", "Yangon Galácticos",
])
def test_production_name_only_context_blocks_yangon_alias(delivery, team):
    sent, blocked, persisted, _ = delivery
    context = {
        "target_side": "radiant",
        "radiant_team_name": "Yangon Galacticos",
        "dire_team_name": "Xipto Esports",
    }
    assert _deliver(f"СТАВКА НА {team} x1", context=context) is False
    sent.assert_not_called()
    persisted.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_team_denylist"


def test_name_only_lineup_snapshot_does_not_infer_unknown_tag(delivery):
    sent, blocked, persisted, _ = delivery
    match_key = "dltv.org/matches/yangon-snapshot.1"
    runtime._remember_player_denylist_lineup(
        match_key, 1, [], [], "Yangon Galacticos", "Xipto Esports",
    )
    assert _deliver("СТАВКА НА YG x1", match_key=match_key) is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


def test_leading_newline_yangon_header_is_blocked(delivery):
    sent, blocked, persisted, _ = delivery
    assert _deliver("\nСТАВКА НА Yangon Galacticos x1") is False
    sent.assert_not_called()
    persisted.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_team_denylist"


@pytest.mark.parametrize("opponent", ["Xipto Esports", "Super Galacticos"])
def test_opponent_header_wins_over_stale_target_side(delivery, opponent):
    sent, blocked, persisted, _ = delivery
    context = {
        "target_side": "radiant",
        "radiant_team_name": "Yangon Galacticos",
        "dire_team_name": opponent,
    }
    assert _deliver(f"СТАВКА НА {opponent} x1", context=context) is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


def test_ids_only_snapshot_does_not_infer_unknown_tag(delivery):
    sent, blocked, persisted, _ = delivery
    match_key = "dltv.org/matches/yangon-id.1"
    runtime._remember_player_denylist_lineup(
        match_key, 1, [], [], "", "Xipto Esports", [9546449], [10242397],
    )
    assert _deliver("СТАВКА НА YG x1", match_key=match_key) is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


@pytest.mark.parametrize("header,snapshot_names,map_num,context", [
    ("Xipto Esports", ("", ""), 1, None),
    ("Xipto Esports", ("YG", "Xipto"), 1, None),
    ("Xipto Esports", ("", ""), 2, None),
    ("Xipto", ("YG", "Xipto Esports"), 2, {"target_side": "radiant"}),
])
def test_stale_swapped_snapshot_never_blocks_opponent(
    delivery, header, snapshot_names, map_num, context,
):
    sent, blocked, persisted, _ = delivery
    match_key = "dltv.org/matches/stale-swapped.1"
    runtime._remember_player_denylist_lineup(
        match_key, 1, [], [], *snapshot_names, [9546449], [10242397],
    )
    assert _deliver(
        f"СТАВКА НА {header} x1", match_key=match_key,
        map_num=map_num, context=context,
    ) is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


def test_static_id_alias_blocks_header(delivery, monkeypatch):
    import id_to_names

    sent, blocked, persisted, _ = delivery
    monkeypatch.setitem(id_to_names.tier_two_teams, "The YG Squad", {9546449})
    assert _deliver("СТАВКА НА The YG Squad x1") is False
    sent.assert_not_called()
    persisted.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_team_denylist"


def test_sourcetv_side_bound_alias_learning(delivery):
    sent, blocked, persisted, _ = delivery
    runtime._winline_sourcetv_series_key({
        "league_id": 123,
        "radiant_team_name": "YG", "radiant_team_id": 9546449,
        "dire_team_name": "Xipto Esports", "dire_team_id": 10242397,
    })
    assert _deliver("СТАВКА НА YG x1") is False
    sent.assert_not_called()
    persisted.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_team_denylist"
    blocked.reset_mock()
    assert _deliver("СТАВКА НА Xipto Esports x1") is True
    sent.assert_called_once()
    blocked.assert_not_called()


def test_ambiguous_same_name_is_not_learned(delivery):
    sent, blocked, persisted, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/ambiguous.1", 1, [], [],
        "YG", "YG", [9546449], [10242397],
    )
    assert _deliver("СТАВКА НА YG x1") is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


@pytest.mark.parametrize("radiant_ids", [[9546449], [10242397, 9546449]])
def test_swapped_opponent_name_never_becomes_global_alias(delivery, radiant_ids):
    sent, blocked, persisted, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/swapped.1", 1, [], [],
        "Xipto Esports", "Yangon Galacticos", radiant_ids, [10242397],
    )
    assert "xiptoesports" not in runtime._TEAM_DENYLIST_LEARNED_NAMES
    assert _deliver("СТАВКА НА Xipto Esports x1", match_key="dltv.org/matches/other.1") is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


def test_mixed_ids_never_learn_opponent_name(delivery):
    sent, blocked, persisted, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/mixed.1", 1, [], [],
        "Xipto Esports", "Neutral Team", [10242397, 9546449], [12345678],
    )
    assert "xiptoesports" not in runtime._TEAM_DENYLIST_LEARNED_NAMES
    assert _deliver("СТАВКА НА Xipto Esports x1", match_key="dltv.org/matches/other.2") is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


def test_banned_id_on_both_sides_cannot_learn_short_name(delivery):
    sent, blocked, persisted, _ = delivery
    runtime._remember_player_denylist_lineup(
        "dltv.org/matches/ambiguous-ids.1", 1, [], [],
        "YG", "Xipto Esports", [9546449], [8944230],
    )
    assert "yg" not in runtime._TEAM_DENYLIST_LEARNED_NAMES
    assert "xiptoesports" not in runtime._TEAM_DENYLIST_LEARNED_NAMES
    assert _deliver("СТАВКА НА Xipto Esports x1", match_key="dltv.org/matches/other.3") is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


def test_non_sourcetv_check_head_does_not_pass_team_ids_to_lineup():
    tree = ast.parse(inspect.getsource(runtime.check_head))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name)
             and node.func.id == "_remember_player_denylist_lineup"]
    assert len(calls) == 1
    for side_ids in calls[0].args[6:8]:
        assert isinstance(side_ids, ast.IfExp)
        assert isinstance(side_ids.test, ast.Name)
        assert side_ids.test.id == "is_sourcetv_card"
        assert isinstance(side_ids.orelse, ast.Constant)
        assert side_ids.orelse.value is None


def test_id_alias_cache_refreshes_after_ttl(delivery, monkeypatch):
    import id_to_names

    sent, blocked, persisted, _ = delivery
    clock = Mock(return_value=1000.0)
    monkeypatch.setattr(runtime.time, "time", clock)
    monkeypatch.setattr(runtime, "_ensure_dynamic_tier2_overlay", lambda: None)
    assert _deliver("СТАВКА НА Nova Seven x1") is True
    monkeypatch.setitem(id_to_names.tier_two_teams, "Nova Seven", {8944230})
    assert _deliver("СТАВКА НА Nova Seven x1", match_key="dltv.org/matches/other.4") is True
    clock.return_value += runtime._TEAM_DENYLIST_ID_ALIASES_TTL_SECONDS + 1
    assert _deliver("СТАВКА НА Nova Seven x1", match_key="dltv.org/matches/other.5") is False
    assert sent.call_count == 2
    assert persisted.call_count == 2
    assert blocked.call_args.kwargs["reason"] == "skip_team_denylist"


def test_alias_refresh_error_prevents_delivery(delivery, monkeypatch):
    sent, _, persisted, _ = delivery
    clock = Mock(return_value=1000.0)
    monkeypatch.setattr(runtime.time, "time", clock)
    monkeypatch.setattr(runtime, "_ensure_dynamic_tier2_overlay", lambda: None)
    assert _deliver("СТАВКА НА Nova Seven x1") is True
    clock.return_value += runtime._TEAM_DENYLIST_ID_ALIASES_TTL_SECONDS + 1
    monkeypatch.setattr(runtime, "_ensure_dynamic_tier2_overlay",
                        Mock(side_effect=RuntimeError("alias refresh failed")))
    with pytest.raises(RuntimeError, match="alias refresh failed"):
        _deliver("СТАВКА НА Nova Seven x1", match_key="dltv.org/matches/other.6")
    sent.assert_called_once()
    persisted.assert_called_once()


def test_blocked_direct_bet_does_not_drop_opponent_queue(delivery):
    sent, blocked, persisted, dropped = delivery
    assert _deliver("СТАВКА НА Yangon Galacticos x1") is False
    sent.assert_not_called()
    persisted.assert_not_called()
    dropped.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_team_denylist"


def test_delayed_delivery_drops_yangon_queue_without_send(delivery, monkeypatch):
    sent, blocked, persisted, dropped = delivery
    match_key = "dltv.org/matches/delayed.1"
    monkeypatch.setattr(runtime, "_is_url_processed", lambda *_: False)
    monkeypatch.setattr(runtime, "_fetch_delayed_match_state", Mock(side_effect=AssertionError("must not fetch")))
    with runtime.monitored_matches_lock:
        runtime.monitored_matches[match_key] = {
            "message": "СТАВКА НА Yangon Galacticos x1",
            "stake_multiplier_context": {"target_side": "radiant"},
            "add_url_details": {"target_side": "radiant"},
        }
    try:
        runtime._drain_due_delayed_signals_once(only_match_key=match_key)
    finally:
        with runtime.monitored_matches_lock:
            runtime.monitored_matches.pop(match_key, None)
    sent.assert_not_called()
    persisted.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_team_denylist"
    dropped.assert_called_once_with(match_key, reason="skip_team_denylist")


def test_delayed_name_only_alias_drops_before_fetch(delivery, monkeypatch):
    sent, blocked, persisted, dropped = delivery
    match_key = "dltv.org/matches/delayed-alias.1"
    runtime._remember_player_denylist_lineup(
        match_key, 1, [], [], "YG", "Xipto Esports", [9546449], [10242397],
    )
    monkeypatch.setattr(runtime, "_is_url_processed", lambda *_: False)
    monkeypatch.setattr(runtime, "_fetch_delayed_match_state", Mock(side_effect=AssertionError("must not fetch")))
    with runtime.monitored_matches_lock:
        runtime.monitored_matches[match_key] = {
            "message": "СТАВКА НА YG x1",
            "stake_multiplier_context": {
                "target_side": "radiant", "radiant_team_name": "Yangon Galacticos",
                "dire_team_name": "Xipto Esports",
            },
        }
    try:
        runtime._drain_due_delayed_signals_once(only_match_key=match_key)
    finally:
        with runtime.monitored_matches_lock:
            runtime.monitored_matches.pop(match_key, None)
    sent.assert_not_called()
    persisted.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_team_denylist"
    dropped.assert_called_once_with(match_key, reason="skip_team_denylist")
