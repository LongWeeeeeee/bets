"""Карта ставки прячет Lanes/Early/Late/All/Mix/Kills_window по умолчанию.

Владелец 20.09.2026: подробные драфт-блоки читались как шум и убраны из
карты ставки; вместо них каждая ML-строка несёт приписку свежести данных
(`BET_SHOW_DRAFT_BLOCKS`, `model_data_asof.py`). Восстановление старой карты —
`BET_SHOW_DRAFT_BLOCKS=1`.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
ROOT = BASE_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cyberscore_try as cs  # noqa: E402

SOURCE = BASE_DIR / "cyberscore_try.py"


def test_compose_for_bet_hidden_by_default(monkeypatch):
    monkeypatch.delenv("BET_SHOW_DRAFT_BLOCKS", raising=False)
    assert cs._compose_star_metric_blocks_for_bet("E\n", "L\n", "A\n", "M\n") == ""


def test_lane_block_for_bet_hidden_by_default(monkeypatch):
    monkeypatch.delenv("BET_SHOW_DRAFT_BLOCKS", raising=False)
    assert cs._build_lane_block_for_bet(
        "Top: win 60%", "Mid: win 50%", "Bot: win 40%", ml_laning_line=""
    ) == ""


def test_compose_for_bet_matches_pure_builder_when_enabled(monkeypatch):
    monkeypatch.setenv("BET_SHOW_DRAFT_BLOCKS", "1")
    got = cs._compose_star_metric_blocks_for_bet("E\n", "L\n", "A\n", "M\n")
    expected = cs._compose_star_metric_blocks_for_message("E\n", "L\n", "A\n", "M\n")
    assert got == expected and got != ""


def test_lane_block_for_bet_matches_pure_builder_when_enabled(monkeypatch):
    monkeypatch.setenv("BET_SHOW_DRAFT_BLOCKS", "1")
    args = ("Top: win 60%", "Mid: win 50%", "Bot: win 40%")
    got = cs._build_lane_block_for_bet(*args, ml_laning_line="")
    expected = cs._build_lane_block(*args, ml_laning_line="")
    assert got == expected and got != ""


def test_kills_window_hidden_by_default(monkeypatch):
    monkeypatch.delenv("BET_SHOW_DRAFT_BLOCKS", raising=False)
    body = cs._format_live_message_state_block(
        game_time_seconds=1200,
        radiant_lead=500,
        radiant_team_name="R",
        dire_team_name="D",
        show_kills_time_blocks=False,
    )
    assert "Time:" in body
    assert "Networth:" in body
    assert "Kills_window:" not in body


def test_only_the_wrapper_calls_the_pure_composers():
    source = SOURCE.read_text(encoding="utf-8")
    assert source.count("_compose_star_metric_blocks_for_message(") == 2
    assert source.count("_build_lane_block(") == 2


def test_format_win_model_line_appends_freshness_note_and_keeps_regexes(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_MIN_CONF", "0.60")
    details = {
        "early_nw": {"side": "Dire", "confidence": .61, "freshness_note": "данные до 01.09 (19 дн.)"},
        "early_win": {"side": "Radiant", "confidence": .72, "freshness_note": "данные до 05.09 (15 дн.)"},
        "late": {"side": "Dire", "confidence": .655, "freshness_note": "данные до 29.08 (22 дн.)"},
        "refusal_reason": "x",
        "refusal_warning_line": "",
    }
    early_output = {cs.win_model_veto.DETAILS_KEY: details}
    text = cs._format_win_model_line(early_output, {}, {})
    lines = [line for line in text.splitlines() if line.strip()]
    assert any(line.endswith("| данные до 01.09 (19 дн.)") for line in lines)
    assert any(line.endswith("| данные до 05.09 (15 дн.)") for line in lines)
    assert any(line.endswith("| данные до 29.08 (22 дн.)") for line in lines)
    late_line = next(line for line in lines if "Late ML-модель" in line)
    assert cs._LATE_WIN_MODEL_PANEL_RE.search(late_line) is not None
    assert cs._WIN_MODEL_PANEL_RE.search(late_line) is None
