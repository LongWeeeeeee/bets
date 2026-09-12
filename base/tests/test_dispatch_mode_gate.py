"""Тесты гейта DISPATCH_MODE (star_dispatch_disabled), этап 2 плана
swirling-giggling-kurzweil.md.

В режиме `ml` старые словарные STAR-пути не удаляются — их сообщения режутся
единой точкой доставки `_deliver_and_persist_signal`, ПЕРВЫМ гейтом, до
подготовки кэфов и dedup-резерва. Определяется ставка по префиксу
«СТАВКА НА » (kills-хедеры множителя не несут и под старый
`_STAKE_HEADER_MULTIPLIER_RE` не попадают, но под этот префикс — попадают).
Пропускает только `stake_multiplier_context["origin"] == "ml_dispatch"`.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
import cyberscore_try as C  # noqa: E402
from base import laning_serving as _laning_serving_module  # noqa: E402


STAR_WIN_MESSAGE = "СТАВКА НА Team Synapse x2\nSynapse VS Nemiga"
STAR_KILLS_WINDOW_MESSAGE = "СТАВКА НА Ранние килы 10-20 Team Synapse\nSynapse VS Nemiga"
STAR_KILLS_TOTAL_MESSAGE = "СТАВКА НА Тотал килов Team Synapse БОЛЬШЕ\nSynapse VS Nemiga"
PANEL_ONLY_MESSAGE = "Synapse VS Nemiga\n\n\U0001F916 ML-модель: Radiant 63.5%"

ML_ORIGIN_CTX = {"origin": "ml_dispatch", "target_side": "radiant", "stake_team_name": "Team Synapse"}


def test_dispatch_mode_unknown_value_falls_back_to_star(monkeypatch) -> None:
    monkeypatch.setenv("DISPATCH_MODE", "bogus")
    assert C.dispatch_mode() == "star"


def test_dispatch_mode_reads_env_values(monkeypatch) -> None:
    for value in ("star", "shadow", "ml"):
        monkeypatch.setenv("DISPATCH_MODE", value)
        assert C.dispatch_mode() == value


def test_dispatch_mode_default_is_star(monkeypatch) -> None:
    monkeypatch.delenv("DISPATCH_MODE", raising=False)
    assert C.dispatch_mode() == "star"


def test_star_message_blocked_in_ml_mode_without_origin(monkeypatch) -> None:
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    decision = C._dispatch_mode_reject_for_delivery(STAR_WIN_MESSAGE, None)
    assert decision is not None
    assert decision["reason"] == "star_dispatch_disabled"


def test_kills_window_message_blocked_in_ml_mode(monkeypatch) -> None:
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    decision = C._dispatch_mode_reject_for_delivery(STAR_KILLS_WINDOW_MESSAGE, None)
    assert decision is not None
    assert decision["reason"] == "star_dispatch_disabled"


def test_kills_total_message_blocked_in_ml_mode(monkeypatch) -> None:
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    decision = C._dispatch_mode_reject_for_delivery(STAR_KILLS_TOTAL_MESSAGE, None)
    assert decision is not None
    assert decision["reason"] == "star_dispatch_disabled"


def test_panel_without_stake_header_passes_in_ml_mode(monkeypatch) -> None:
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    assert C._dispatch_mode_reject_for_delivery(PANEL_ONLY_MESSAGE, None) is None


def test_ml_dispatch_origin_passes_in_ml_mode(monkeypatch) -> None:
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    assert C._dispatch_mode_reject_for_delivery(STAR_WIN_MESSAGE, ML_ORIGIN_CTX) is None


def test_star_mode_never_blocks(monkeypatch) -> None:
    monkeypatch.setenv("DISPATCH_MODE", "star")
    assert C._dispatch_mode_reject_for_delivery(STAR_WIN_MESSAGE, None) is None
    assert C._dispatch_mode_reject_for_delivery(STAR_KILLS_WINDOW_MESSAGE, None) is None


def test_shadow_mode_never_blocks(monkeypatch) -> None:
    monkeypatch.setenv("DISPATCH_MODE", "shadow")
    assert C._dispatch_mode_reject_for_delivery(STAR_WIN_MESSAGE, None) is None


def test_ml_dispatch_origin_not_re_blocked_by_win_or_late_gates(monkeypatch) -> None:
    monkeypatch.setattr(C, "BET_REQUIRE_WIN_MODEL", True, raising=False)
    monkeypatch.setattr(C, "BET_REQUIRE_LATE_WIN_MODEL", True, raising=False)
    # Панель без строк ML/Late-моделей вовсе -- обычные гейты выдали бы
    # model_missing/late_model_missing, но origin=ml_dispatch их обходит
    # (вето 0.60 уже применено в ml_dispatch.evaluate).
    text = "СТАВКА НА Team Synapse x1\nSynapse VS Nemiga"
    assert C._win_model_reject_for_delivery(text, ML_ORIGIN_CTX) is None
    assert C._late_win_model_reject_for_delivery(text, ML_ORIGIN_CTX) is None


def test_gate_is_wired_first_into_delivery(monkeypatch) -> None:
    """Гейт режима обязан стоять ПЕРВЫМ: до остальных гейтов, кэфов и dedup."""
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    called: list[str] = []
    monkeypatch.setattr(
        C, "_half_stake_elo_underdog_reject_for_delivery",
        lambda *a, **k: called.append("half_stake") or None,
    )
    monkeypatch.setattr(
        C, "_win_model_reject_for_delivery",
        lambda *a, **k: called.append("win_model") or None,
    )
    monkeypatch.setattr(
        C, "_late_win_model_reject_for_delivery",
        lambda *a, **k: called.append("late_win_model") or None,
    )
    monkeypatch.setattr(
        C, "_bookmaker_prepare_message_for_delivery",
        lambda *a, **k: called.append("prepare") or ("", False, "", None),
    )
    monkeypatch.setattr(
        C, "_signal_fingerprint_try_reserve",
        lambda *a, **k: called.append("reserve") or (False, ""),
    )
    delivered = C._deliver_and_persist_signal(
        "dltv.org/matches/dispatch-mode-gate.0",
        STAR_WIN_MESSAGE,
        add_url_reason="test",
        stake_multiplier_context=None,
    )
    assert delivered is False
    assert called == [], f"гейт режима пропустил STAR-сигнал дальше: {called}"


def test_terminal_reject_drops_delayed_queue_entry(monkeypatch) -> None:
    """star_dispatch_disabled помечает delayed-сигнал обработанным, не ретраит."""
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    dropped: list[tuple] = []
    monkeypatch.setattr(
        C, "_drop_delayed_match",
        lambda match_key, reason="": dropped.append((match_key, reason)),
    )
    delivered = C._deliver_and_persist_signal(
        "dltv.org/matches/dispatch-mode-drop.0",
        STAR_WIN_MESSAGE,
        add_url_reason="test",
        stake_multiplier_context=None,
    )
    assert delivered is False
    assert dropped == [("dltv.org/matches/dispatch-mode-drop.0", "star_dispatch_disabled")]


def test_late_star_line_still_matches_panel_regex() -> None:
    """★-суффикс на строке Late ML-модель не должен ломать `_LATE_WIN_MODEL_PANEL_RE`."""
    text = "\U0001F551 Late ML-модель: Radiant 65.0% ★"
    match = C._LATE_WIN_MODEL_PANEL_RE.search(text)
    assert match is not None
    assert match.group("side") == "Radiant"


# --- _ml_dispatch_tick: shadow logs only, ml delivers once with dedup -------

class _FakeLedger:
    def __init__(self):
        self._keys: set = set()

    def as_set(self):
        return set(self._keys)

    def add(self, key):
        self._keys.add(tuple(key))

    def save(self):
        pass


def _patch_ml_dispatch_tick_deps(monkeypatch, *, delivered_calls, logged, ledger):
    # Late confirms Radiant at 0.70 (>= 0.60 default threshold); no All/lane
    # verdict at all, so nothing vetoes Radiant.
    monkeypatch.setattr(
        C, "_ml_dispatch_extract_index_details",
        lambda *blocks: (5.0, {"late": {"side": "Radiant", "confidence": 0.70}}),
    )
    monkeypatch.setattr(C.win_model_veto, "_heroes_vector", lambda r, d: tuple(range(10)))
    monkeypatch.setattr(_laning_serving_module, "verdicts", lambda *a, **k: {"all": None, "lane": None})
    monkeypatch.setattr(
        C, "_team_elo_base_rating_for_side",
        lambda meta, side: 1500.0 if side == "radiant" else 1400.0,
    )
    monkeypatch.setattr(C, "_ml_dispatch_sent_ledger", lambda: ledger)
    monkeypatch.setattr(
        C, "_ml_dispatch_record_decisions",
        lambda record, *, dedup_view: logged.append(record),
    )
    monkeypatch.setattr(
        C, "_deliver_and_persist_signal",
        lambda *a, **k: delivered_calls.append((a, k)) or True,
    )


def _call_ml_dispatch_tick(match_key: str = "dltv.org/matches/ml-dispatch-tick.0") -> None:
    C._ml_dispatch_tick(
        match_key=match_key,
        radiant_team_name="Team A",
        dire_team_name="Team B",
        live_league={},
        top="", mid="", bot="",
        protracker_payload=None,
        team_elo_block="",
        team_elo_meta={"radiant_base_rating": 1500.0, "dire_base_rating": 1400.0},
        game_time_seconds=650.0,
        radiant_lead=0,
        full_message_text="СТАВКА НА Team A x1\nTeam A VS Team B",
    )


def test_ml_dispatch_tick_shadow_mode_logs_without_delivering(monkeypatch) -> None:
    monkeypatch.setenv("DISPATCH_MODE", "shadow")
    delivered_calls: list = []
    logged: list = []
    _patch_ml_dispatch_tick_deps(monkeypatch, delivered_calls=delivered_calls, logged=logged, ledger=_FakeLedger())

    _call_ml_dispatch_tick()

    assert delivered_calls == []
    assert len(logged) == 1
    assert logged[0]["mode"] == "shadow"
    win_decisions = [d for d in logged[0]["decisions"] if d["market"] == "win"]
    assert len(win_decisions) == 1
    assert win_decisions[0]["target_side"] == "Radiant"


def test_ml_dispatch_tick_ml_mode_delivers_once_then_dedups(monkeypatch) -> None:
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    delivered_calls: list = []
    logged: list = []
    ledger = _FakeLedger()
    _patch_ml_dispatch_tick_deps(monkeypatch, delivered_calls=delivered_calls, logged=logged, ledger=ledger)

    _call_ml_dispatch_tick()
    _call_ml_dispatch_tick()  # same match/map -- ledger now carries the dedup key

    assert len(delivered_calls) == 1
    call_args, call_kwargs = delivered_calls[0]
    assert call_kwargs["stake_multiplier_context"]["origin"] == "ml_dispatch"
    assert call_kwargs["stake_multiplier_context"]["calibration"]["expected_wr"] == 0.70
    assert len(logged) == 2
    assert logged[0]["delivered"][0]["status"] == "delivered"
    assert logged[1]["decisions"] == []  # second tick: dedup skip, no repeat decision


# --- Defect 1: once-per-cycle guard around _ml_dispatch_tick -----------------

def test_ml_dispatch_tick_once_per_cycle_skips_same_game_time(monkeypatch) -> None:
    calls: list = []
    monkeypatch.setattr(C, "_ml_dispatch_tick", lambda **kw: calls.append(kw))
    monkeypatch.setattr(C, "_ml_dispatch_tick_last_cycle_key", {})
    common = dict(
        radiant_team_name="A", dire_team_name="B", top="", mid="", bot="",
        protracker_payload=None, team_elo_block="", team_elo_meta=None, radiant_lead=0,
    )
    C._ml_dispatch_tick_once_per_cycle(match_key="m1", live_league={}, game_time_seconds=650.0, **common)
    C._ml_dispatch_tick_once_per_cycle(match_key="m1", live_league={}, game_time_seconds=650.0, **common)
    assert len(calls) == 1  # same (match_key, map_num, game_time) -- second call is a no-op

    C._ml_dispatch_tick_once_per_cycle(match_key="m1", live_league={}, game_time_seconds=700.0, **common)
    assert len(calls) == 2  # game_time moved on -- real call again


# --- Defect 2: _ml_dispatch_open_kills_windows uses real window specs -------

def test_kills_windows_open_at_game_time_zero_all_four_nearest_first() -> None:
    assert C._ml_dispatch_open_kills_windows(0.0) == ["5_15", "10_20", "15_25", "20_30"]


def test_kills_windows_open_at_200_nearest_is_10_20() -> None:
    windows = C._ml_dispatch_open_kills_windows(200.0)
    assert windows[0] == "10_20"
    assert "5_15" not in windows


def test_kills_windows_open_at_500_nearest_is_15_25() -> None:
    windows = C._ml_dispatch_open_kills_windows(500.0)
    assert windows[0] == "15_25"
    assert "10_20" not in windows


def test_kills_windows_open_at_1100_none_open() -> None:
    assert C._ml_dispatch_open_kills_windows(1100.0) == []


# --- Defect 3: min-odds floor gate for ML win bets --------------------------

ML_WIN_CTX = {
    "origin": "ml_dispatch",
    "ml_market": "win",
    "calibration": {"expected_wr": 0.60, "min_odds": 1.67},
}
ML_KILLS_CTX = {
    "origin": "ml_dispatch",
    "ml_market": "kills_window",
    "calibration": {"expected_wr": 0.60, "min_odds": 1.67},
}


def test_ml_min_odds_reject_when_price_below_floor() -> None:
    text = "СТАВКА НА Team Synapse x1\nКэф Winline: 1.55\n"
    decision = C._ml_dispatch_min_odds_reject_for_delivery(text, ML_WIN_CTX)
    assert decision is not None
    assert decision["reason"] == "ml_min_odds_below_floor"


def test_ml_min_odds_passes_when_price_above_floor() -> None:
    text = "СТАВКА НА Team Synapse x1\nКэф Winline: 1.80\n"
    assert C._ml_dispatch_min_odds_reject_for_delivery(text, ML_WIN_CTX) is None


def test_ml_min_odds_not_applied_to_kills_market() -> None:
    text = "СТАВКА НА Ранние килы 10-20 Team Synapse\nКэф Winline: 1.10\n"
    assert C._ml_dispatch_min_odds_reject_for_delivery(text, ML_KILLS_CTX) is None


def test_ml_min_odds_unknown_price_follows_bookmaker_block_without_odds(monkeypatch) -> None:
    text = "СТАВКА НА Team Synapse x1\n"  # no "Кэф Winline:" line at all
    monkeypatch.setattr(C, "BOOKMAKER_BLOCK_WITHOUT_ODDS", True)
    decision = C._ml_dispatch_min_odds_reject_for_delivery(text, ML_WIN_CTX)
    assert decision is not None
    assert decision["reason"] == "ml_min_odds_below_floor"
    assert decision["price"] is None

    monkeypatch.setattr(C, "BOOKMAKER_BLOCK_WITHOUT_ODDS", False)
    assert C._ml_dispatch_min_odds_reject_for_delivery(text, ML_WIN_CTX) is None
