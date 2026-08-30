"""Late-ставка не уходит против late-модели.

30.08.2026, просьба владельца: «ставка на late не работает если ml late
показывала не на таргет». Late-модель (`base/late_win_model.py`, E-240) обучена
на картах >= 36 минут и печатается в панель отдельной строкой; до этой правки
она была чисто информационной и на отправку не влияла.

Отличие от предматчевого гейта: молчание late-модели ставку НЕ блокирует. Её
отказ молчаливый по построению (нет артефакта, незнакомый герой, неполный
драфт), и трактовать его как «против» значило бы гасить весь late-поток при
пропавшем артефакте.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
import cyberscore_try as C  # noqa: E402

# Формат строки задаёт `_build_win_model_line` (base/cyberscore_try.py, ~5846):
# "\n\U0001F551 Late ML-модель: {side} {confidence}%". Часы U+1F551 обязательны —
# `late_win_model.panel_line()` их не ставит, панель добавляет сама.
LATE_FOR_RADIANT = "🕑 Late ML-модель: Radiant 56.0%"
LATE_FOR_DIRE = "🕑 Late ML-модель: Dire 56.0%"

# Сторону задаёт late-звезда, early того же знака её не подтверждает.
LATE_DRIVEN_RADIANT_CTX = {
    "target_side": "radiant",
    "stake_team_name": "Team Synapse",
    "has_selected_late_star": True,
    "selected_late_sign": 1,
    "has_selected_early_star": False,
    "selected_early_sign": None,
    "all_star_hits": [],
}


def _panel(*, late_line: str = LATE_FOR_RADIANT,
           header: str = "СТАВКА НА Team Synapse x0.5") -> str:
    lines = [header, "Synapse VS Nemiga", "",
             "🤖 ML-модель: Radiant 63.5% | ML от кэфа: 1.54"]
    if late_line:
        lines.append(late_line)
    lines += [
        "Оценка WR:",
        "Late: Team Synapse WR≈70.0% от кэфа 1.43",
        "⭐ Star hits (WR60+):",
        "  Late: Counterpick_1vs1 +8 (WR70)",
    ]
    return "\n".join(lines)


def test_panel_line_format_is_recognised() -> None:
    """Регексп читает ровно ту строку, которую печатает панель."""
    match = C._LATE_WIN_MODEL_PANEL_RE.search(_panel(late_line=LATE_FOR_DIRE))
    assert match is not None
    assert match.group("side") == "Dire"


def test_late_model_on_target_side_passes() -> None:
    assert C._late_win_model_reject_for_delivery(
        _panel(), LATE_DRIVEN_RADIANT_CTX
    ) is None


def test_late_model_against_target_blocks() -> None:
    decision = C._late_win_model_reject_for_delivery(
        _panel(late_line=LATE_FOR_DIRE), LATE_DRIVEN_RADIANT_CTX
    )
    assert decision is not None
    assert decision["reason"] == "late_model_against"
    assert decision["late_model_side"] == "dire"
    assert decision["target_side"] == "radiant"


def test_silent_late_model_does_not_block() -> None:
    """Строки нет = модель отказала молча. Это НЕ «против», ставка идёт."""
    assert C._late_win_model_reject_for_delivery(
        _panel(late_line=""), LATE_DRIVEN_RADIANT_CTX
    ) is None


def test_non_late_driven_bet_is_untouched() -> None:
    """Early того же знака подтверждает сторону — это уже не ставка на late."""
    ctx = dict(LATE_DRIVEN_RADIANT_CTX,
               has_selected_early_star=True, selected_early_sign=1)
    assert C._late_win_model_reject_for_delivery(
        _panel(late_line=LATE_FOR_DIRE), ctx
    ) is None


def test_bet_without_late_star_is_untouched() -> None:
    ctx = dict(LATE_DRIVEN_RADIANT_CTX, has_selected_late_star=False)
    assert C._late_win_model_reject_for_delivery(
        _panel(late_line=LATE_FOR_DIRE), ctx
    ) is None


def test_target_side_opposite_to_late_sign_is_untouched() -> None:
    """Сторона ставки не совпадает со стороной late-звезды — не наша популяция."""
    ctx = dict(LATE_DRIVEN_RADIANT_CTX, target_side="dire")
    assert C._late_win_model_reject_for_delivery(
        _panel(late_line=LATE_FOR_RADIANT), ctx
    ) is None


def test_kills_message_without_stake_header_is_untouched() -> None:
    text = "Ранние килы\nSynapse VS Nemiga\n" + LATE_FOR_DIRE
    assert C._late_win_model_reject_for_delivery(text, LATE_DRIVEN_RADIANT_CTX) is None


def test_kill_switch_disables_gate(monkeypatch) -> None:
    monkeypatch.setattr(C, "BET_REQUIRE_LATE_WIN_MODEL", False)
    assert C._late_win_model_reject_for_delivery(
        _panel(late_line=LATE_FOR_DIRE), LATE_DRIVEN_RADIANT_CTX
    ) is None


def test_missing_context_does_not_block() -> None:
    """Без контекста сторону ставки не определить — гейт молчит, а не рубит."""
    assert C._late_win_model_reject_for_delivery(
        _panel(late_line=LATE_FOR_DIRE), None
    ) is None


def test_gate_is_actually_wired_into_delivery(monkeypatch) -> None:
    """Гейт обязан стоять В ПУТИ отправки, а не просто существовать.

    Late-ставка уходит несколькими путями (immediate, delayed watcher,
    спекулятив), и сходятся они только в `_deliver_and_persist_signal`. Здесь
    доставка вызывается по-настоящему и обязана отказать ДО подготовки кэфов и
    до резервирования dedup-ключа.
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
        "dltv.org/matches/late-gate.0",
        _panel(late_line=LATE_FOR_DIRE),
        add_url_reason="test",
        stake_multiplier_context=LATE_DRIVEN_RADIANT_CTX,
    )
    assert delivered is False
    assert called == [], f"гейт пропустил late-сигнал дальше: {called}"


def test_agreeing_late_model_does_not_block_delivery(monkeypatch) -> None:
    """Контроль к предыдущему: при согласии модели доставка идёт дальше гейта."""
    called: list[str] = []
    monkeypatch.setattr(
        C, "_bookmaker_prepare_message_for_delivery",
        lambda *a, **k: called.append("prepare") or ("", False, "", None),
    )
    monkeypatch.setattr(
        C, "_signal_fingerprint_try_reserve",
        lambda *a, **k: called.append("reserve") or (False, ""),
    )
    C._deliver_and_persist_signal(
        "dltv.org/matches/late-gate-ok.0",
        _panel(late_line=LATE_FOR_RADIANT),
        add_url_reason="test",
        stake_multiplier_context=LATE_DRIVEN_RADIANT_CTX,
    )
    assert called, "гейт заблокировал сигнал, с которым модель согласна"
