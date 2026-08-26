"""Ставка «на 00» не идёт, когда поздний star-блок против модели.

26.08.2026, карта Synapse — Nemiga Gaming (квал BLAST Slam). Поздние звёзды
показывали на Synapse и были сняты вето модели
(`late=win_model_veto(side=radiant)`), early-блок показывал на Nemiga, но не
дотянул до порога early-only. Ставку в ту же секунду отправила ПРЕДМАТЧЕВАЯ
модель — на Nemiga, на 2:40 и без нетворса. Два источника разошлись, ставить в
этот момент не на что.
"""

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402
import win_model_veto  # noqa: E402


def test_conflict_label_covers_both_veto_kinds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Оба вето позднего блока считаются расхождением с моделью."""
    seen = {}

    def _fake_diag(*, raw_block, target_wr, section):
        seen["target_wr"] = target_wr
        seen["section"] = section
        return {"status": raw_block.get("status_for_test"), "valid": False}

    monkeypatch.setattr(runtime, "_star_block_diagnostics", _fake_diag)
    assert runtime._prematch_late_block_conflict(
        {"status_for_test": "win_model_veto"}, 60) == "win_model_veto"
    assert seen == {"target_wr": 60, "section": "mid_output"}
    assert runtime._prematch_late_block_conflict(
        {"status_for_test": "draft_against_side"}, 65) == "draft_against_side"
    # Прочие исходы блока ставку не трогают.
    assert runtime._prematch_late_block_conflict({"status_for_test": "ok"}, 60) == ""
    assert runtime._prematch_late_block_conflict({"status_for_test": "no_hits"}, 60) == ""


def test_missing_block_and_broken_diag_leave_the_bet_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Нет позднего блока или диагностика упала — правило молчит (fail-open)."""
    assert runtime._prematch_late_block_conflict(None, 60) == ""
    assert runtime._prematch_late_block_conflict("не словарь", 60) == ""

    def _boom(**_kwargs):
        raise RuntimeError("диагностика упала")

    monkeypatch.setattr(runtime, "_star_block_diagnostics", _boom)
    assert runtime._prematch_late_block_conflict({"any": 1}, 60) == ""


def test_default_threshold_when_star_wr_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = {}

    def _fake_diag(*, raw_block, target_wr, section):
        seen["target_wr"] = target_wr
        return {"status": "ok"}

    monkeypatch.setattr(runtime, "_star_block_diagnostics", _fake_diag)
    runtime._prematch_late_block_conflict({"any": 1}, None)
    assert seen["target_wr"] == runtime.TIER_SIGNAL_MIN_THRESHOLD_TIER2


def test_bet_is_cancelled_when_late_block_opposes_the_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Боевой сценарий: модель дала ставку, поздний блок против — отправки нет."""
    monkeypatch.setattr(runtime, "PREMATCH_MODEL_BET_ENABLED", True)
    monkeypatch.setattr(
        win_model_veto, "model_bet",
        lambda *_blocks: {"side": "dire", "index": -32.35, "confidence": 0.823, "min_odds": 1.23},
    )
    monkeypatch.setattr(
        runtime, "_prematch_late_block_conflict", lambda *_a, **_kw: "win_model_veto"
    )

    def _must_not_run(*_a, **_kw):
        raise AssertionError("ставка не должна доходить до отправки")

    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", _must_not_run)

    assert runtime._try_dispatch_prematch_model_bet(
        match_key="dltv.org/matches/8966549098.0",
        status="live",
        radiant_team_name="Synapse",
        dire_team_name="Nemiga Gaming",
        live_league=None,
        top=None,
        mid=None,
        bot=None,
        protracker_payload=None,
        team_elo_block="",
        game_time_seconds=160,
        radiant_lead=0,
        mid_output={"any": 1},
        selected_star_wr=60,
    ) is False


def test_bet_survives_when_late_block_does_not_oppose(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Без расхождения правило не вмешивается: дальше идёт прежний путь.

    Дальше стоит дедуп по обработанным url — на нём и останавливаемся, важно
    лишь то, что отказ случился НЕ из-за нового правила.
    """
    monkeypatch.setattr(runtime, "PREMATCH_MODEL_BET_ENABLED", True)
    monkeypatch.setattr(
        win_model_veto, "model_bet",
        lambda *_blocks: {"side": "dire", "index": -32.35, "confidence": 0.823, "min_odds": 1.23},
    )
    monkeypatch.setattr(runtime, "_prematch_late_block_conflict", lambda *_a, **_kw: "")
    reached = {"dedup": False}

    def _fake_skip(*_a, **_kw):
        reached["dedup"] = True
        return True

    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", _fake_skip)

    assert runtime._try_dispatch_prematch_model_bet(
        match_key="dltv.org/matches/8966549098.0",
        status="live",
        radiant_team_name="Synapse",
        dire_team_name="Nemiga Gaming",
        live_league=None,
        top=None,
        mid=None,
        bot=None,
        protracker_payload=None,
        team_elo_block="",
        game_time_seconds=160,
        radiant_lead=0,
        mid_output={"any": 1},
        selected_star_wr=60,
    ) is False
    assert reached["dedup"] is True
