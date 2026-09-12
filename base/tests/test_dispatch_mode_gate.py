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
