"""Обычная ставка не уходит без согласия ML-модели победителя карты.

28.08.2026 ставка на Team Synapse ушла в чат, хотя предматчевая модель
отказалась оценивать матч («разметка позиций противоречит истории у 3
слотов»): строки `🤖 ML-модель:` в панели не было, и отсутствие мнения
оказалось неотличимо от согласия. Гейт закрывает три случая — мнения нет,
мнение против таргета, блок All против таргета — и НЕ трогает kills-ставки:
модель предсказывает победителя карты, а не килы, и ML-строки у них не бывает.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
import cyberscore_try as C  # noqa: E402

MODEL_FOR_RADIANT = "🤖 ML-модель: Radiant 63.5% | ML от кэфа: 1.54"
MODEL_FOR_DIRE = "🤖 ML-модель: Dire 63.5% | ML от кэфа: 1.54"
RADIANT_CTX = {"target_side": "radiant", "stake_team_name": "Team Synapse"}


def _panel(*, model_line: str = MODEL_FOR_RADIANT, all_team: str = "Team Synapse",
           header: str = "СТАВКА НА Team Synapse x0.5") -> str:
    """Панель обычной ставки в боевом виде: заголовок, ML-строка, Оценка WR."""
    lines = [header, "Synapse VS Nemiga", ""]
    if model_line:
        lines.append(model_line)
    lines += [
        "Оценка WR:",
        "Early Winner: Team Synapse WR≈65.0% от кэфа 1.54",
        f"All: {all_team} WR≈65.0% от кэфа 1.54",
        "⭐ Star hits (WR60+):",
        "  All: Counterpick_1vs1 +8 (WR70)",
        "All:",
        "Counterpick_1vs1: 8*",
    ]
    return "\n".join(lines)


def test_panel_with_model_on_target_side_passes():
    assert C._win_model_reject_for_delivery(_panel(), RADIANT_CTX) is None


def test_missing_model_line_blocks():
    decision = C._win_model_reject_for_delivery(_panel(model_line=""), RADIANT_CTX)
    assert decision is not None and decision["reason"] == "model_missing"


def test_model_on_opposite_side_blocks():
    decision = C._win_model_reject_for_delivery(
        _panel(model_line=MODEL_FOR_DIRE), RADIANT_CTX
    )
    assert decision is not None and decision["reason"] == "model_against"
    assert decision["model_side"] == "dire" and decision["target_side"] == "radiant"


def test_all_block_on_other_team_blocks():
    decision = C._win_model_reject_for_delivery(
        _panel(all_team="Nemiga Gaming"), RADIANT_CTX
    )
    assert decision is not None and decision["reason"] == "all_against"
    assert decision["all_team"] == "Nemiga Gaming"


def test_kills_bet_is_not_gated():
    """Kills-хедер множителя не несёт, ML-строки у него не бывает — пропускаем."""
    kills = _panel(model_line="", header="СТАВКА НА Ранние килы 10-20 Team Synapse")
    assert C._win_model_reject_for_delivery(kills, RADIANT_CTX) is None
    kills_from = _panel(model_line="", header="СТАВКА НА килы от Team Synapse")
    assert C._win_model_reject_for_delivery(kills_from, RADIANT_CTX) is None


def test_missing_model_blocks_even_without_stake_context():
    """Мнения нет — блок не зависит от контекста: решение целиком в тексте."""
    decision = C._win_model_reject_for_delivery(_panel(model_line=""), None)
    assert decision is not None and decision["reason"] == "model_missing"


def test_side_check_is_skipped_without_context():
    """Стороны (radiant/dire) в тексте нет; без контекста сверять не с чем."""
    assert C._win_model_reject_for_delivery(_panel(model_line=MODEL_FOR_DIRE), None) is None


def test_all_check_works_without_context():
    """Имена в панели есть всегда — этот запрет контекста не требует."""
    decision = C._win_model_reject_for_delivery(_panel(all_team="Nemiga Gaming"), None)
    assert decision is not None and decision["reason"] == "all_against"


def test_disabled_flag_lets_everything_through(monkeypatch):
    monkeypatch.setattr(C, "BET_REQUIRE_WIN_MODEL", False)
    assert C._win_model_reject_for_delivery(_panel(model_line=""), RADIANT_CTX) is None


def test_metrics_section_header_is_not_read_as_all_verdict():
    """«All:» без WR — заголовок секции метрик, вердиктом его считать нельзя."""
    text = "\n".join([
        "СТАВКА НА Team Synapse x0.5",
        MODEL_FOR_RADIANT,
        "All:",
        "Counterpick_1vs1: 8*",
    ])
    assert C._win_model_reject_for_delivery(text, RADIANT_CTX) is None


def test_gate_is_actually_wired_into_delivery(monkeypatch):
    """Гейт обязан стоять В ПУТИ отправки, а не просто существовать.

    Проверка функции в отрыве ничего не говорит о том, вызывается ли она:
    сигнал уходит десятком путей, и все они сходятся в
    `_deliver_and_persist_signal`. Здесь доставка вызывается по-настоящему и
    обязана отказать ДО любой тяжёлой работы — до подготовки кэфов и до
    резервирования dedup-ключа, которые в тесте недоступны и при обращении
    к ним свалили бы его.
    """
    called: list[str] = []
    monkeypatch.setattr(
        C, "_bookmaker_prepare_message_for_delivery",
        lambda *a, **k: called.append("prepare") or ("", False, "", None),
    )
    monkeypatch.setattr(
        C, "_signal_fingerprint_try_reserve",
        lambda *a, **k: called.append("reserve") or (False, ""),
    )
    delivered = C._deliver_and_persist_signal(
        "dltv.org/matches/1.0",
        _panel(model_line=""),
        add_url_reason="test",
        stake_multiplier_context={"target_side": "radiant",
                                  "stake_team_name": "Team Synapse"},
    )
    assert delivered is False
    assert called == [], f"гейт пропустил сигнал дальше: {called}"
