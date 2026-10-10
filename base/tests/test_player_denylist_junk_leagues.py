"""Junk-league player ban (owner request 10.10.2026, card ingame-h886).

Players of Destiny League, Mad Dogs League, AD2L, RD2L, IDL and League of Lads
join the player denylist from data/player_denylist_junk_leagues.json; serious
pros (>=30 maps since 7.39 in allowlisted leagues) are spared. Checked at the
final send boundary on real lineups of bet maps captured from STRATZ
(base/tests/fixtures/junk_league_lineups_20261010.json).
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
# (base/keys.py); same ImportError-only stub as test_player_denylist_yache123.
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

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "junk_league_lineups_20261010.json"
MAPS = json.loads(FIXTURE.read_text(encoding="utf-8"))["maps"]
CDUB_MAP = MAPS["9037351109"]      # CDUB fields menace 871439294 (AD2L/RD2L regular)
AMARU_MAP = MAPS["9037218370"]     # Amaru fields AMINN 184131721 (RD2L)
OG_MAP = MAPS["9022834804"]        # OG fields Topson 94054712 (AD2L 15 maps, spared pro)
DATA_FILE = BASE_DIR.parent / "data" / "player_denylist_junk_leagues.json"
MENACE, AMINN, TOPSON = 871439294, 184131721, 94054712


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


def _deliver(record, message, *, side):
    series = f"dltv.org/matches/{record['match_id']}"
    runtime._PLAYER_DENYLIST_BY_MAP[(series, 1)] = {
        "radiant": list(record["radiant_player_ids"]),
        "dire": list(record["dire_player_ids"]),
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


def test_bet_on_team_with_ad2l_rd2l_regular_is_blocked(delivery):
    sent, blocked, persisted = delivery
    assert CDUB_MAP["dire_team_name"] == "CDUB" and MENACE in CDUB_MAP["dire_player_ids"]
    assert _deliver(CDUB_MAP, "СТАВКА НА CDUB x1\nКарта 1", side="dire") is False
    sent.assert_not_called()
    persisted.assert_not_called()
    assert blocked.call_args.kwargs["reason"] == "skip_player_denylist"
    assert blocked.call_args.args[2]["blocked_player_account_ids"] == [MENACE]
    assert "RD2L" in blocked.call_args.kwargs["verdict"] or "AD2L" in blocked.call_args.kwargs["verdict"]


def test_opponent_of_junk_regular_still_gets_the_bet(delivery):
    sent, blocked, persisted = delivery
    assert _deliver(CDUB_MAP, "СТАВКА НА NEW GROWTH x1\nКарта 1", side="radiant") is True
    sent.assert_called_once()
    persisted.assert_called_once()
    blocked.assert_not_called()


def test_single_rd2l_player_blocks_his_team(delivery):
    sent, blocked, _ = delivery
    assert _deliver(AMARU_MAP, "СТАВКА НА Amaru x1\nКарта 1", side="dire") is False
    sent.assert_not_called()
    assert blocked.call_args.args[2]["blocked_player_account_ids"] == [AMINN]


def test_serious_pro_from_ad2l_is_spared(delivery):
    """Topson played 15 AD2L maps but has 45 allowlisted pro maps: no ban."""
    sent, blocked, _ = delivery
    assert TOPSON in OG_MAP["radiant_player_ids"]
    assert _deliver(OG_MAP, "СТАВКА НА OG x1\nКарта 1", side="radiant") is True
    sent.assert_called_once()
    blocked.assert_not_called()


def test_data_file_bans_junk_regulars_and_spares_serious_pros():
    payload = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    banned = {int(a) for ids in payload["families"].values() for a in ids}
    spared = {int(a) for a in payload["spared_serious"]}
    assert {"Destiny League/Cup", "Mad Dogs League", "AD2L", "RD2L"} <= set(payload["families"])
    assert len(banned) > 19000
    assert not banned & spared
    assert {TOPSON, 103735745, 177203952} <= spared  # Topson, Saksa, Yuma
    assert {MENACE, AMINN} <= banned
    assert banned <= runtime.SKIPPED_PLAYER_ACCOUNT_IDS
    assert not spared & runtime.SKIPPED_PLAYER_ACCOUNT_IDS


def test_hand_picked_labels_survive_the_merge():
    assert runtime.SKIPPED_PLAYER_NAMES[390015464] == "egxrdemxn"
    assert runtime.SKIPPED_PLAYER_NAMES[457637739] == "Fortunes"


def test_loader_spares_listed_pro_even_if_a_family_lists_him(tmp_path):
    path = tmp_path / "denylist.json"
    path.write_text(json.dumps({
        "families": {"AD2L": [TOPSON, MENACE]},
        "spared_serious": {str(TOPSON): {"name": "Topson"}},
    }), encoding="utf-8")
    labels = runtime._load_junk_league_player_denylist(path, True)
    assert labels == {MENACE: f"{MENACE} (AD2L)"}


def test_loader_off_and_missing_file_load_nothing(tmp_path, capsys):
    assert runtime._load_junk_league_player_denylist(DATA_FILE, False) == {}
    assert runtime._load_junk_league_player_denylist(tmp_path / "absent.json", True) == {}
    assert "NOT loaded" in capsys.readouterr().out


@pytest.mark.parametrize("payload", [
    "[1, 2]",                                                # not an object
    "{not json",                                             # invalid JSON
    json.dumps({"spared_serious": {}}),                      # families missing
    json.dumps({"families": [MENACE]}),                      # families is a list
    json.dumps({"families": "AD2L"}),                        # families is a string
    json.dumps({"families": {"AD2L": MENACE}}),              # family value is an int
    json.dumps({"families": {"AD2L": [MENACE]}, "spared_serious": 5}),  # spared is an int
])
def test_malformed_file_never_breaks_import_and_loads_nothing(tmp_path, capsys, payload):
    """A broken data edit disables only the junk addition; it never raises."""
    path = tmp_path / "denylist.json"
    path.write_text(payload, encoding="utf-8")
    assert runtime._load_junk_league_player_denylist(path, True) == {}
    assert "NOT loaded" in capsys.readouterr().out
