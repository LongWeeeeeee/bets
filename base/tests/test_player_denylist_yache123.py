"""YACHE123 player ban (user request 02.10.2026) at the final send boundary.

Every bet ON a team that fields Fortunes, Krish, Seimei, Tsukimoto or Ace12
is blocked; bets on the opponent still go. Lineups are the real ledger rows
captured from serv1 (base/tests/fixtures/yache123_lineup_20260928.json).
"""

import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Isolated verification trees exclude the ignored production credentials
# (base/keys.py); same ImportError-only stub as test_player_denylist_egxrdemxn.
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

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "yache123_lineup_20260928.json"
LEDGER = json.loads(FIXTURE.read_text(encoding="utf-8"))["applied_maps"]
FULL_ROSTER = LEDGER["dltv.org/matches/9020148294.0"]["match_record"]
STANDIN_ROSTER = LEDGER["dltv.org/matches/9016509881.0"]["match_record"]

YACHE123_PLAYERS = {
    457637739: "Fortunes",
    349495318: "Krish",
    242835570: "Seimei",
    285319482: "Tsukimoto",
    274078636: "Ace12",
}


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
    # Only the outbound send is mocked; unrelated policy gates are isolated.
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


def _deliver(record, message, *, side, dire_ids=None):
    series = f"dltv.org/matches/{record['match_id']}"
    runtime._PLAYER_DENYLIST_BY_MAP[(series, 1)] = {
        "radiant": list(record["radiant_player_ids"]),
        "dire": list(record["dire_player_ids"] if dire_ids is None else dire_ids),
        "radiant_team": record["radiant_team_name"],
        "dire_team": record["dire_team_name"],
    }
    return runtime._deliver_and_persist_signal(
        f"{series}.40",
        message,
        add_url_reason="test",
        skip_bookmaker_prepare=True,
        map_num=1,
        selected_side=side,
    )


def test_full_roster_bet_blocked_with_player_names(delivery):
    sent, blocked, persisted = delivery
    assert FULL_ROSTER["dire_team_name"] == "ЯЧЁ123"
    assert _deliver(FULL_ROSTER, "СТАВКА НА ЯЧЁ123 x1\nКарта 1", side="dire") is False
    sent.assert_not_called()
    persisted.assert_not_called()
    kwargs = blocked.call_args.kwargs
    assert kwargs["reason"] == "skip_player_denylist"
    assert sorted(blocked.call_args.args[2]["blocked_player_account_ids"]) == sorted(YACHE123_PLAYERS)
    for name in YACHE123_PLAYERS.values():
        assert name in kwargs["verdict"]
    assert "egxrdemxn" not in kwargs["verdict"]


def test_opponent_of_yache123_still_gets_the_bet(delivery):
    sent, blocked, persisted = delivery
    message = "СТАВКА НА InterActive Philippines x1\nКарта 1"
    assert _deliver(FULL_ROSTER, message, side="radiant") is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


@pytest.mark.parametrize("account_id", sorted(YACHE123_PLAYERS))
def test_each_player_alone_blocks_his_team(delivery, account_id):
    """One banned player among unknown slots (stand-in team) is enough."""
    sent, blocked, _ = delivery
    assert _deliver(
        FULL_ROSTER, "СТАВКА НА ЯЧЁ123 x1", side="dire", dire_ids=[account_id, 0, 0, 0, 0]
    ) is False
    sent.assert_not_called()
    assert blocked.call_args.args[2]["blocked_player_account_ids"] == [account_id]
    assert YACHE123_PLAYERS[account_id] in blocked.call_args.kwargs["verdict"]


def test_steam64_form_of_a_player_is_blocked(delivery):
    sent, blocked, _ = delivery
    steam64 = str(349495318 + 76561197960265728)
    assert _deliver(
        FULL_ROSTER, "СТАВКА НА ЯЧЁ123 x1", side="dire", dire_ids=[steam64, 0, 0, 0, 0]
    ) is False
    sent.assert_not_called()
    assert "Krish" in blocked.call_args.kwargs["verdict"]


def test_standin_lineup_of_25_09_is_blocked(delivery):
    sent, blocked, _ = delivery
    assert _deliver(STANDIN_ROSTER, "СТАВКА НА ЯЧЁ123 x1", side="dire") is False
    sent.assert_not_called()
    assert sorted(blocked.call_args.args[2]["blocked_player_account_ids"]) == [
        242835570, 274078636, 457637739,
    ]


def test_egxrdemxn_block_keeps_its_label(delivery):
    sent, blocked, _ = delivery
    assert _deliver(
        FULL_ROSTER, "СТАВКА НА ЯЧЁ123 x1", side="dire", dire_ids=[1250582363, 0, 0, 0, 0]
    ) is False
    sent.assert_not_called()
    assert "(egxrdemxn)" in blocked.call_args.kwargs["verdict"]


def test_denylist_keeps_egxrdemxn_and_adds_all_five():
    assert {390015464, 1250582363} <= runtime.SKIPPED_PLAYER_ACCOUNT_IDS
    assert set(YACHE123_PLAYERS) <= runtime.SKIPPED_PLAYER_ACCOUNT_IDS
