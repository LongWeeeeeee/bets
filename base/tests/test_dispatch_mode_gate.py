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

import ast
import sys

import pytest
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
    """★-суффикс и подпись фильтра в скобках не должны ломать `_LATE_WIN_MODEL_PANEL_RE`."""
    text = "\U0001F551 Late ML-модель (карта ≥36 мин): Radiant 65.0% ★"
    match = C._LATE_WIN_MODEL_PANEL_RE.search(text)
    assert match is not None
    assert match.group("side") == "Radiant"
    # Старый формат без подписи (delayed-записи, собранные до деплоя) читается по-прежнему.
    legacy = C._LATE_WIN_MODEL_PANEL_RE.search("\U0001F551 Late ML-модель: Dire 56.0%")
    assert legacy is not None and legacy.group("side") == "Dire"


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
    # E-281 adds probabilities, not a side/confidence model verdict. Keep
    # this populated so the complete tick exercises audit fingerprinting.
    monkeypatch.setattr(
        C.win_model_veto, "last_kills30",
        lambda index: {"radiant": 0.395, "dire": 0.407, "total": 0.396},
    )
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


def _call_ml_dispatch_tick(match_key: str = "dltv.org/matches/ml-dispatch-tick.0", *,
                           game_time=650.0, radiant_lead=0) -> None:
    C._ml_dispatch_tick(
        match_key=match_key,
        radiant_team_name="Team A",
        dire_team_name="Team B",
        live_league={},
        top="", mid="", bot="",
        protracker_payload=None,
        team_elo_block="",
        team_elo_meta={"radiant_base_rating": 1500.0, "dire_base_rating": 1400.0},
        game_time_seconds=game_time,
        radiant_lead=radiant_lead,
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
    assert logged[0]["verdicts"]["kills30"] == {
        "radiant": 0.395, "dire": 0.407, "total": 0.396,
    }
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


def _patch_ml_dispatch_tick_deps_no_index_mock(monkeypatch, *, logged, ledger):
    # Same as `_patch_ml_dispatch_tick_deps` but leaves the real
    # `_ml_dispatch_extract_index_details`/`_ml_dispatch_prematch_source` in
    # place, so an `early_output` block's own INDEX_KEY/SOURCE_KEY drive
    # `ctx.prematch` for real (owner decision 15.09.2026).
    monkeypatch.setattr(C.win_model_veto, "_heroes_vector", lambda r, d: tuple(range(10)))
    monkeypatch.setattr(C.win_model_veto, "last_kills30", lambda index: None)
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
    monkeypatch.setattr(C, "_deliver_and_persist_signal", lambda *a, **k: True)


def _call_ml_dispatch_tick_with_early_output(match_key: str, early_output: dict) -> None:
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
        early_output=early_output,
        full_message_text="СТАВКА НА Team A x1\nTeam A VS Team B",
    )


def test_ml_dispatch_tick_ctx_prematch_verdict_matches_panel_line(monkeypatch) -> None:
    # Ctx.prematch (owner decision 15.09.2026) must carry the SAME
    # side/confidence the "🤖 ML-модель: ... (оценка)" panel line prints
    # (`_format_win_model_line`, cyberscore_try.py:7979-7994): index=-17.1,
    # SOURCE_PREMATCH -> "Dire 67.1%" -> ModelVerdict("Dire", 0.671).
    monkeypatch.setenv("DISPATCH_MODE", "shadow")
    logged: list = []
    _patch_ml_dispatch_tick_deps_no_index_mock(monkeypatch, logged=logged, ledger=_FakeLedger())
    index = -17.1
    early_output = {
        C.win_model_veto.INDEX_KEY: index,
        C.win_model_veto.SOURCE_KEY: C.win_model_veto.SOURCE_PREMATCH,
        C.win_model_veto.DETAILS_KEY: {
            "index": index, "early_nw": None, "early_win": None, "late": None,
        },
    }
    _call_ml_dispatch_tick_with_early_output(
        "dltv.org/matches/ml-dispatch-tick-prematch.0", early_output,
    )
    assert len(logged) == 1
    assert logged[0]["verdicts"]["prematch"] == {"side": "Dire", "confidence": 0.671}


def test_ml_dispatch_tick_ctx_prematch_absent_when_source_is_not_prematch(monkeypatch) -> None:
    # A draft-only source (model refused/not the 35-feature prematch model
    # this tick) -- ctx.prematch stays None, same as the panel's "(оценка)"
    # suffix not appearing.
    monkeypatch.setenv("DISPATCH_MODE", "shadow")
    logged: list = []
    _patch_ml_dispatch_tick_deps_no_index_mock(monkeypatch, logged=logged, ledger=_FakeLedger())
    index = -17.1
    early_output = {
        C.win_model_veto.INDEX_KEY: index,
        C.win_model_veto.SOURCE_KEY: C.win_model_veto.SOURCE_DRAFT,
        C.win_model_veto.DETAILS_KEY: {
            "index": index, "early_nw": None, "early_win": None, "late": None,
        },
    }
    _call_ml_dispatch_tick_with_early_output(
        "dltv.org/matches/ml-dispatch-tick-prematch-refused.0", early_output,
    )
    assert len(logged) == 1
    assert logged[0]["verdicts"]["prematch"] is None


def test_ml_dispatch_tick_early_nw_wait_transition_and_delivery_dedup(monkeypatch):
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    monkeypatch.setenv("ML_DISPATCH_EARLY_NW", "1")
    delivered_calls, logged = [], []
    _patch_ml_dispatch_tick_deps(monkeypatch, delivered_calls=delivered_calls,
                                 logged=logged, ledger=_FakeLedger())
    for game_time, lead in [(239, 1000), (240, None), (240, -1000), (240, 999)]:
        _call_ml_dispatch_tick(game_time=game_time, radiant_lead=lead)
        assert logged[-1]["decisions"][0]["timing"] == "wait_600"
    assert delivered_calls == []
    _call_ml_dispatch_tick(game_time=240, radiant_lead=1000)
    assert len(delivered_calls) == 1
    assert logged[-1]["radiant_networth_lead"] == 1000
    assert any("early_nw_release" in r for r in logged[-1]["decisions"][0]["reasons"])
    _call_ml_dispatch_tick(game_time=245, radiant_lead=1100)
    assert len(delivered_calls) == 1


# --- _ml_dispatch_record_decisions: dedup keyed by base_url, not match_key --

def test_ml_dispatch_record_decisions_dedups_by_base_url_across_match_key_suffixes(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(C, "_ml_dispatch_decisions_log_last_fingerprint", {})
    log_path = tmp_path / "ml_dispatch_decisions.jsonl"
    monkeypatch.setattr(C, "_ml_dispatch_decisions_log_path", lambda: log_path)

    dedup_view = {"decisions": [("win", "Radiant", "now")]}
    base_record = {
        "match_key": "dltv.org/matches/8994944191.50",
        "base_url": "dltv.org/matches/8994944191",
        "map_num": 1,
    }
    C._ml_dispatch_record_decisions(dict(base_record), dedup_view=dedup_view)
    # Same base_url/map_num/verdicts, but match_key suffix moved on (next tick) --
    # must NOT write a second line.
    C._ml_dispatch_record_decisions(
        dict(base_record, match_key="dltv.org/matches/8994944191.53"),
        dedup_view=dedup_view,
    )
    lines = log_path.read_text().splitlines()
    assert len(lines) == 1

    # Verdicts/decisions actually changed -- must write again.
    C._ml_dispatch_record_decisions(
        dict(base_record, match_key="dltv.org/matches/8994944191.61"),
        dedup_view={"decisions": [("win", "Dire", "now")]},
    )
    lines = log_path.read_text().splitlines()
    assert len(lines) == 2


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


def test_ml_dispatch_tick_once_per_cycle_keys_by_base_url_not_raw_match_key(monkeypatch) -> None:
    # sourcetv match_key grows a numeric suffix every tick; the cycle guard
    # must key on base_url so it still catches the repeat.
    calls: list = []
    monkeypatch.setattr(C, "_ml_dispatch_tick", lambda **kw: calls.append(kw))
    monkeypatch.setattr(C, "_ml_dispatch_tick_last_cycle_key", {})
    common = dict(
        radiant_team_name="A", dire_team_name="B", top="", mid="", bot="",
        protracker_payload=None, team_elo_block="", team_elo_meta=None, radiant_lead=0,
    )
    C._ml_dispatch_tick_once_per_cycle(
        match_key="dltv.org/matches/8994944191.50", live_league={}, game_time_seconds=650.0, **common)
    C._ml_dispatch_tick_once_per_cycle(
        match_key="dltv.org/matches/8994944191.53", live_league={}, game_time_seconds=650.0, **common)
    assert len(calls) == 1  # same base_url/map_num/game_time despite differing match_key suffix


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
    # Odds gate ACTIVE (pipeline on, gate mode "odds"): unknown price mirrors
    # BOOKMAKER_BLOCK_WITHOUT_ODDS exactly like the general delivery path.
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(C, "BOOKMAKER_BLOCK_WITHOUT_ODDS", True)
    decision = C._ml_dispatch_min_odds_reject_for_delivery(text, ML_WIN_CTX)
    assert decision is not None
    assert decision["reason"] == "ml_min_odds_below_floor"
    assert decision["price"] is None

    monkeypatch.setattr(C, "BOOKMAKER_BLOCK_WITHOUT_ODDS", False)
    assert C._ml_dispatch_min_odds_reject_for_delivery(text, ML_WIN_CTX) is None


def test_ml_min_odds_unknown_price_passes_when_odds_pipeline_off(monkeypatch, capsys) -> None:
    # Prod 12.09.2026: cyberscore_try.py runs with --no-odds, so
    # _bookmaker_prepare_message_for_delivery returns ready=True/"disabled"
    # and the general path sends WITHOUT "Кэф Winline". With no matching
    # fresh in-process quote, unknown price still passes; this avoids the
    # 45 false blocks from 12.09.2026 when --no-odds was active.
    text = "СТАВКА НА Dawn Bulls x1\n"
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(C, "BOOKMAKER_BLOCK_WITHOUT_ODDS", True)
    assert C._ml_dispatch_min_odds_reject_for_delivery(
        text, ML_WIN_CTX, match_key="dltv.org/matches/8995259364.5",
    ) is None
    out = capsys.readouterr().out
    assert "ML-пол по кэфу не применён" in out and "8995259364.5" in out

    # Gate mode other than "odds" also means no reserved price ever appears.
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_GATE_MODE", "off")
    assert C._ml_dispatch_min_odds_reject_for_delivery(text, ML_WIN_CTX) is None

    # A KNOWN price below the floor is still rejected regardless of pipeline state.
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_ENABLED", False)
    below = "СТАВКА НА Dawn Bulls x1\nКэф Winline: 1.40\n"
    decision = C._ml_dispatch_min_odds_reject_for_delivery(below, ML_WIN_CTX)
    assert decision is not None and decision["price"] == 1.40


@pytest.mark.parametrize("rule", ["underdog", "late_conflict"])
@pytest.mark.parametrize("team_ids,require_tier1,allowed", [
    ((111, 222), True, False),  # neither team qualifies
    ((101, 222), True, True),  # only Radiant is Tier-1
    ((111, 101), True, True),  # only Dire is Tier-1
    ((0, None), True, False),  # missing IDs fail closed
    ((111, 222), False, True),  # existing explicit opt-out
])
def test_ml_early_kills_tier_gate_before_delivery(
    monkeypatch, rule, team_ids, require_tier1, allowed,
):
    import id_to_names

    monkeypatch.setenv("DISPATCH_MODE", "ml")
    monkeypatch.setattr(C, "KILLS_REQUIRE_TIER1_TEAM", require_tier1)
    monkeypatch.setattr(C, "_ensure_dynamic_tier2_overlay", lambda: None)
    monkeypatch.setattr(C, "_auto_added_tier2_ids", set())
    monkeypatch.setattr(id_to_names, "tier_one_teams", {"Tier1": {101}})
    monkeypatch.setattr(id_to_names, "tier_two_teams", {"DIREBORN": 111, "Team Nemesis": 222})
    delivered, logged, ledger = [], [], _FakeLedger()
    _patch_ml_dispatch_tick_deps(
        monkeypatch, delivered_calls=delivered, logged=logged, ledger=ledger,
    )
    details = {"early_win": {"side": "Radiant", "confidence": 0.65}}
    if rule == "late_conflict":
        details["late"] = {"side": "Dire", "confidence": 0.70}
    monkeypatch.setattr(C, "_ml_dispatch_extract_index_details", lambda *a: (5.0, details))
    monkeypatch.setattr(
        _laning_serving_module, "verdicts",
        lambda *a, **k: {"all": {"side": "Radiant", "confidence": 0.65}, "lane": None},
    )
    monkeypatch.setattr(
        C, "_team_elo_base_rating_for_side",
        lambda meta, side: 1400.0 if rule == "late_conflict" or side == "radiant" else 1600.0,
    )
    monkeypatch.setattr(C.win_model_veto, "last_kills30", lambda i: {"radiant": 0.99})
    C._ml_dispatch_tick(
        match_key="dltv.org/matches/direborn-tier-regression.0",
        radiant_team_name="DIREBORN", dire_team_name="Team Nemesis",
        radiant_team_id=team_ids[0], dire_team_id=team_ids[1],
        live_league={}, top="", mid="", bot="", protracker_payload=None,
        team_elo_block="", team_elo_meta={}, game_time_seconds=0, radiant_lead=0,
        radiant_heroes_and_pos={}, dire_heroes_and_pos={},
        full_message_text=("DIREBORN VS Team Nemesis\n"
                           "🔴 окно 5-15: Dire 64%\nKills_window: 5_15: +0.25"),
    )
    assert len(logged) == 1
    windows = [d for d in logged[0]["decisions"] if d["market"] == "kills_window"]
    assert bool(windows) is allowed
    sent_markets = [kwargs["stake_multiplier_context"]["ml_market"] for _, kwargs in delivered]
    assert ("kills_window" in sent_markets) is allowed
    assert "kills_total" in sent_markets  # unchanged market, same ML evidence
    blocked = [s for s in logged[0]["skipped"] if s["reason"] == "kills_requires_tier1_team"]
    assert bool(blocked) is (not allowed)
    assert any(k[2] == "kills_window" for k in ledger.as_set()) is allowed
    if allowed:
        assert windows[0]["target_side"] == "Radiant"
        assert windows[0]["reasons"][-1] == "window=5_15"


def test_every_ml_dispatch_callsite_passes_resolved_team_ids():
    tree = ast.parse(Path(C.__file__).read_text())
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name)
             and node.func.id == "_ml_dispatch_tick_once_per_cycle"]
    assert len(calls) == 4
    for call in calls:
        args = {kw.arg: kw.value for kw in call.keywords}
        for name in ("radiant_team_id", "dire_team_id"):
            assert isinstance(args.get(name), ast.Name)
            assert args[name].id == name


@pytest.mark.parametrize("nemesis_tier,allowed", [(1, True), (2, False)])
def test_direborn_map4_incident_replay_respects_team_tier(monkeypatch, nemesis_tier, allowed):
    # serv1 ml_dispatch_decisions.jsonl:478, 2026-09-16T10:09:04Z.
    # The sent rule used Early Win, despite the panel window favoring Dire.
    from base import ml_dispatch as md

    monkeypatch.setenv("DISPATCH_MODE", "ml")
    monkeypatch.setattr(C, "KILLS_REQUIRE_TIER1_TEAM", True)
    monkeypatch.setattr(C, "_get_team_tier", lambda team_id: nemesis_tier if team_id == 9691969 else 2)
    delivered, logged, ledger = [], [], _FakeLedger()
    _patch_ml_dispatch_tick_deps(
        monkeypatch, delivered_calls=delivered, logged=logged, ledger=ledger,
    )
    details = {
        "early_nw": {"side": "Dire", "confidence": 0.5364},
        "early_win": {"side": "Radiant", "confidence": 0.625102},
        "late": {"side": "Radiant", "confidence": 0.5302},
    }
    monkeypatch.setattr(C, "_ml_dispatch_extract_index_details", lambda *a: (-22.99, details))
    monkeypatch.setattr(C, "_ml_dispatch_prematch_source", lambda *a: C.win_model_veto.SOURCE_PREMATCH)
    monkeypatch.setattr(
        _laning_serving_module, "verdicts", lambda *a, **k: {
            "all": {"side": "Radiant", "confidence": 0.5784},
            "lane": {"side": "Radiant", "confidence": 0.6672},
        },
    )
    monkeypatch.setattr(
        C, "_team_elo_base_rating_for_side",
        lambda meta, side: 2037.802445754411 if side == "radiant" else 2172.694518987726,
    )
    monkeypatch.setattr(C.win_model_veto, "last_kills30", lambda i: {
        "radiant": 0.5383362133942335, "dire": 0.3798063135879572,
        "total": 0.4155635242327326,
    })
    original_evaluate = md.evaluate
    raw_decisions = []

    def capture_raw_decisions(ctx, cfg):
        result = original_evaluate(ctx, cfg)
        raw_decisions.extend(result.decisions)
        return result

    monkeypatch.setattr(md, "evaluate", capture_raw_decisions)
    C._ml_dispatch_tick(
        match_key="dltv.org/matches/9001373364.0",
        radiant_team_id=10150434, dire_team_id=9691969,
        radiant_team_name="DIREBORN", dire_team_name="Team Nemesis",
        live_league={}, top="", mid="", bot="", protracker_payload=None,
        team_elo_block="", team_elo_meta={}, game_time_seconds=-79, radiant_lead=0,
        radiant_heroes_and_pos={}, dire_heroes_and_pos={},
        full_message_text="DIREBORN VS Team Nemesis\n1-2",
    )
    assert [(d.market, d.target_side, d.rule) for d in raw_decisions] == [
        ("kills_window", "Radiant", "kills_underdog_early_window"),
    ]
    assert raw_decisions[0].models_for == ["early_win"]
    assert raw_decisions[0].reasons[-1] == "window=5_15"
    assert len(logged) == 1
    assert bool(logged[0]["decisions"]) is allowed
    assert any(s["reason"] == "kills_requires_tier1_team" for s in logged[0]["skipped"]) is (not allowed)
    assert bool(delivered) is allowed
    assert bool(ledger.as_set()) is allowed


def test_team_nemesis_is_tier2_and_direborn_pair_cannot_send_early_kills(monkeypatch):
    # User correction 2026-09-16: real IDs, real source lists, no tier mock.
    monkeypatch.setattr(C, "KILLS_REQUIRE_TIER1_TEAM", True)
    monkeypatch.setattr(C, "_ensure_dynamic_tier2_overlay", lambda: None)
    monkeypatch.setattr(C, "_auto_added_tier2_ids", set())
    assert C._get_team_tier(9691969) == 2
    assert C._get_team_tier(10150434) != 1
    assert C._match_has_tier1_team(10150434, 9691969) is False
    assert C._match_has_tier1_team(9691969, 10150434) is False


@pytest.fixture
def position_dispatch(monkeypatch):
    from types import SimpleNamespace
    from test_prematch_refusal_fallback import POSITION_MISMATCH_SLOTS, POSITION_MISMATCH_REASON

    original_extract = C._ml_dispatch_extract_index_details
    original_heroes = C.win_model_veto._heroes_vector
    delivered, logged, fingerprints, ledger = [], [], [], _FakeLedger()
    _patch_ml_dispatch_tick_deps(monkeypatch, delivered_calls=delivered, logged=logged, ledger=ledger)
    monkeypatch.setattr(C, '_ml_dispatch_extract_index_details', original_extract)
    monkeypatch.setattr(C.win_model_veto, '_heroes_vector', original_heroes)
    monkeypatch.setattr(C, '_match_has_tier1_team', lambda *a: True)
    monkeypatch.setattr(_laning_serving_module, 'verdicts', lambda *a, **k: {
        'all': {'side': 'Radiant', 'confidence': .6222},
        'lane': {'side': 'Radiant', 'confidence': .5955}})
    monkeypatch.setattr(C, '_ml_dispatch_record_decisions',
                        lambda record, *, dedup_view: (logged.append(record), fingerprints.append(dedup_view)))
    heroes = [102, 126, 43, 40, 131, 46, 63, 2, 27, 87]
    radiant = {f'pos{i+1}': {'hero_id': h, 'account_id': 100+i} for i, h in enumerate(heroes[:5])}
    dire = {f'pos{i+1}': {'hero_id': h, 'account_id': 200+i} for i, h in enumerate(heroes[5:])}
    radiant['pos4']['account_id'], radiant['pos5']['account_id'] = 118325938, 91535476
    dire['pos3']['account_id'] = 161839895
    resolution = {'radiant': {'method': 'permutation', 'conf': .2, 'raw_known': 2},
                  'dire': {'method': 'raw', 'conf': 1., 'private_payload': 'must not log'}}

    def call(conflicts=POSITION_MISMATCH_SLOTS, match_key='dltv.org/matches/8995525359.11'):
        details = {'refusal_reason': POSITION_MISMATCH_REASON if conflicts else 'no_account_no_org_blocked',
                   'position_mismatch': conflicts}
        C._ml_dispatch_tick(
            match_key=match_key, radiant_team_name='VooDooSh Club', dire_team_name='Stariy_Bog Club',
            live_league={}, top='', mid='', bot='', protracker_payload=None,
            team_elo_block='', team_elo_meta={}, game_time_seconds=650., radiant_lead=0,
            early_output={C.win_model_veto.DETAILS_KEY: details},
            radiant_heroes_and_pos=radiant, dire_heroes_and_pos=dire,
            position_source='sourcetv', position_resolution=resolution,
            full_message_text='VooDooSh Club VS Stariy_Bog Club')
    return SimpleNamespace(call=call, delivered=delivered, logged=logged, ledger=ledger,
                           fingerprints=fingerprints, radiant=radiant, resolution=resolution,
                           conflicts=POSITION_MISMATCH_SLOTS)


@pytest.mark.parametrize('mode', ['ml', 'shadow'])
def test_real_position_refusal_incident_blocks_all_win_and_preserves_verdict(monkeypatch, position_dispatch, mode):
    monkeypatch.setenv('DISPATCH_MODE', mode)
    p = position_dispatch
    p.call()
    assert p.delivered == [] and p.ledger.as_set() == set()
    row = p.logged[0]
    assert row['decisions'] == []
    assert any(s['market'] == 'win' and s['reason'] == 'position_mismatch' for s in row['skipped'])
    assert row['verdicts']['all'] == {'side': 'Radiant', 'confidence': .6222}
    assert row['draft_input']['position_mismatch'] == [list(x) for x in p.conflicts]
    assert row['draft_input']['slots'][3] == {'side': 'radiant', 'pos': 4, 'hero_id': 40, 'account_id': 118325938}


@pytest.mark.parametrize('market', ['win', 'kills_window', 'kills_total'])
@pytest.mark.parametrize('timing', ['now', 'wait_600'])
def test_position_guard_covers_every_candidate_market(monkeypatch, position_dispatch, market, timing):
    from base import ml_dispatch as md
    monkeypatch.setenv('DISPATCH_MODE', 'ml')
    candidate = md.Decision(market, 'Radiant', 'Team', 'test', ['all'], [], timing, .7, 1.43, [])
    monkeypatch.setattr(md, 'evaluate', lambda *a: md.EvalResult([candidate], [], 'Dire', 100., 'ml'))
    p = position_dispatch
    p.call()
    assert not p.delivered and not p.ledger.as_set()
    assert p.logged[0]['decisions'] == []
    assert p.logged[0]['skipped'] == [{'market': market, 'side': 'Radiant',
                                     'reason': 'position_mismatch',
                                     'detail': 'hard position conflict in attached prematch snapshot'}]


def test_account_org_refusal_on_next_map_ignores_stale_global_conflict(monkeypatch, position_dispatch):
    monkeypatch.setenv('DISPATCH_MODE', 'ml')
    p = position_dispatch
    p.call()
    monkeypatch.setattr(C.win_model_veto, '_LAST_REFUSAL', {'position_mismatch': p.conflicts})
    p.call(None, 'dltv.org/matches/9005071263.0')
    assert len(p.delivered) == 1 and len(p.ledger.as_set()) == 1
    row = p.logged[-1]
    assert row['draft_input']['position_mismatch'] == []
    assert row['delivered'][0]['status'] == 'delivered'
    assert row['delivered'][0]['attempt_started_at'] <= row['delivered'][0]['attempt_finished_at'] <= row['ts']
    # Low parser confidence is evidence, not a hard-refusal policy.
    assert row['draft_input']['resolution']['radiant']['conf'] == .2
    assert 'private_payload' not in row['draft_input']['resolution']['dire']


def test_position_evidence_changes_dedup_but_capture_clock_does_not(monkeypatch, position_dispatch):
    monkeypatch.setenv('DISPATCH_MODE', 'shadow')
    p = position_dispatch
    p.call(None)
    p.call(None)
    assert p.fingerprints[0] == p.fingerprints[1]
    assert 'captured_at' not in p.fingerprints[0]['draft_input']
    old_slot = dict(p.logged[0]['draft_input']['slots'][0])
    p.radiant['pos1'], p.radiant['pos2'] = p.radiant['pos2'], p.radiant['pos1']
    p.call(None)
    assert p.fingerprints[-1] != p.fingerprints[0]
    assert p.logged[0]['draft_input']['slots'][0] == old_slot
    assert p.logged[-1]['draft_input']['slots'][0]['hero_id'] == 126
    assert p.logged[-1]['draft_input']['source_observed_at'] is None


def test_every_ml_dispatch_callsite_passes_current_position_source():
    tree = ast.parse(Path(C.__file__).read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == '_ml_dispatch_tick_once_per_cycle']
    assert len(calls) == 4
    for call in calls:
        kwargs = {k.arg: ast.unparse(k.value) for k in call.keywords}
        assert kwargs['position_source'] == "listing_context.get('source')"
        assert kwargs['position_resolution'] == "data.get('_pos_resolution')"
