"""Ранний релиз при early-звезде у оппонента (замер 01.08.2026).

Раньше ветки late-таргета с early-звездой у оппонента жёстко блокировались до
24:00. Теперь в окне [OPPOSITE_EARLY_RELEASE_FROM_MINUTE, 24:00) dispatch
допустим по устойчивому большому лиду:
  * порог OPPOSITE_EARLY_RELEASE_THRESHOLD (по умолчанию 1600),
  * лид должен держаться OPPOSITE_EARLY_RELEASE_HOLD_SECONDS игровых секунд,
  * окно закрыто, если all-блок стоит на стороне early (против таргета).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


FROM_MINUTE = int(runtime.OPPOSITE_EARLY_RELEASE_FROM_SECONDS // 60)
THRESHOLD = float(runtime.OPPOSITE_EARLY_RELEASE_THRESHOLD)
HOLD_SECONDS = float(runtime.OPPOSITE_EARLY_RELEASE_HOLD_SECONDS)

# Watcher-сетка в рантайме подтягивается из pub_late_pre27_watcher_thresholds.json
# уже после импорта модуля; для тестов подставляем минимальный валидный грид.
WATCHER_GRID = {
    "late_all": {70: {10: 1500.0, 20: 900.0}},
    "late_only": {70: {10: 1500.0, 20: 900.0}},
}


@pytest.fixture(autouse=True)
def _watcher_thresholds(monkeypatch):
    monkeypatch.setattr(
        runtime, "late_pre27_watcher_thresholds_by_group_wr", WATCHER_GRID, raising=False
    )


def _watcher_config(
    *,
    has_opposite_early_star: bool = True,
    all_opposes_target: Optional[bool] = False,
    signal_group: str = "late_all",
) -> Dict[str, Any]:
    config = runtime._late_pre27_watcher_monitor_config(
        signal_group=signal_group,
        target_sign=1,
        late_wr_pct=70.0,
        all_wr_pct=70.0,
        selected_star_wr=70,
        has_opposite_early_star=has_opposite_early_star,
        all_opposes_target=all_opposes_target,
    )
    assert isinstance(config, dict) and config.get("enabled"), "watcher config must be enabled"
    return config


def _dominance_config(all_opposes_target: Optional[bool] = False) -> Dict[str, Any]:
    config = runtime._late_pre27_dominance_monitor_config(
        has_selected_early_star=True,
        selected_early_sign=-1,
        has_selected_late_star=True,
        selected_late_sign=1,
        early_rec={"wr_pct": 70.0},
        late_rec={"wr_pct": 70.0},
        early_wr_pct=70.0,
        late_wr_pct=70.0,
        all_opposes_target=all_opposes_target,
    )
    assert isinstance(config, dict) and config.get("enabled"), "dominance config must be enabled"
    return config


# --- all-блок против таргета: поведение не меняется (жёсткий блок до 24:00) ---


def test_watcher_all_against_target_still_blocked_before_24() -> None:
    config = _watcher_config(all_opposes_target=True)
    for minute in (FROM_MINUTE, 18, 23):
        snapshot = runtime._late_pre27_watcher_snapshot(config, minute * 60)
        assert snapshot.get("threshold") is None, f"minute {minute} must stay blocked"


def test_dominance_all_against_target_still_blocked_before_24() -> None:
    config = _dominance_config(all_opposes_target=True)
    for minute in (FROM_MINUTE, 18, 23):
        snapshot = runtime._late_pre27_dominance_snapshot(config, minute * 60)
        assert snapshot.get("threshold") is None, f"minute {minute} must stay blocked"


def test_unknown_all_side_is_treated_as_blocked() -> None:
    """None (сторона all неизвестна) — консервативно блокируем, как раньше."""
    config = _watcher_config(all_opposes_target=None)
    snapshot = runtime._late_pre27_watcher_snapshot(config, 18 * 60)
    assert snapshot.get("threshold") is None


# --- all не против таргета: открывается окно раннего релиза ---


def test_watcher_early_release_opens_from_configured_minute() -> None:
    config = _watcher_config(all_opposes_target=False)

    before = runtime._late_pre27_watcher_snapshot(config, (FROM_MINUTE - 1) * 60)
    assert before.get("threshold") is None, "до окна релиза порога быть не должно"

    inside = runtime._late_pre27_watcher_snapshot(config, FROM_MINUTE * 60)
    assert inside.get("threshold") == THRESHOLD
    assert inside.get("hold_seconds") == HOLD_SECONDS


def test_dominance_early_release_opens_from_configured_minute() -> None:
    config = _dominance_config(all_opposes_target=False)

    before = runtime._late_pre27_dominance_snapshot(config, (FROM_MINUTE - 1) * 60)
    assert before.get("threshold") is None

    inside = runtime._late_pre27_dominance_snapshot(config, (FROM_MINUTE + 3) * 60)
    assert inside.get("threshold") == THRESHOLD
    assert inside.get("hold_seconds") == HOLD_SECONDS


def test_no_opposite_early_star_keeps_existing_thresholds() -> None:
    """Без early у оппонента ветка работает по старым флэт-порогам (1000/800)."""
    config = _watcher_config(has_opposite_early_star=False)

    phase1 = runtime._late_pre27_watcher_snapshot(config, 15 * 60)
    assert phase1.get("threshold") == 1000.0
    assert phase1.get("hold_seconds") is None

    phase2 = runtime._late_pre27_watcher_snapshot(config, 21 * 60)
    assert phase2.get("threshold") == 800.0


def test_after_24_min_thresholds_unchanged() -> None:
    """После 24:00 ранний релиз не участвует: остаются прежние пороги."""
    watcher = runtime._late_pre27_watcher_snapshot(_watcher_config(), 25 * 60)
    assert watcher.get("threshold") == 800.0
    assert watcher.get("hold_seconds") is None

    dominance = runtime._late_pre27_dominance_snapshot(_dominance_config(), 25 * 60)
    assert dominance.get("threshold") == 800.0
    assert dominance.get("hold_seconds") is None


def test_release_disabled_by_env_flag(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "OPPOSITE_EARLY_RELEASE_ENABLED", False)
    config = _watcher_config()
    snapshot = runtime._late_pre27_watcher_snapshot(config, 18 * 60)
    assert snapshot.get("threshold") is None, "с выключенным флагом — старый блок"


# --- hold: касание порога недостаточно, лид надо держать ---


def test_hold_requires_sustained_lead() -> None:
    touch = runtime._networth_monitor_hold_check(
        current_game_time=float(FROM_MINUTE * 60),
        target_networth_diff=THRESHOLD + 100.0,
        monitor_threshold=THRESHOLD,
        hold_started_game_time=None,
        hold_seconds=HOLD_SECONDS,
    )
    assert touch.get("enabled") is True
    assert touch.get("ready") is False, "мгновенное касание порога не должно отправлять"

    held = runtime._networth_monitor_hold_check(
        current_game_time=float(FROM_MINUTE * 60) + HOLD_SECONDS,
        target_networth_diff=THRESHOLD + 100.0,
        monitor_threshold=THRESHOLD,
        hold_started_game_time=float(FROM_MINUTE * 60),
        hold_seconds=HOLD_SECONDS,
    )
    assert held.get("ready") is True, "лид, продержавшийся hold-окно, должен отправлять"

    dropped = runtime._networth_monitor_hold_check(
        current_game_time=float(FROM_MINUTE * 60) + HOLD_SECONDS,
        target_networth_diff=THRESHOLD - 300.0,
        monitor_threshold=THRESHOLD,
        hold_started_game_time=float(FROM_MINUTE * 60),
        hold_seconds=HOLD_SECONDS,
    )
    assert dropped.get("ready") is False, "просадка лида должна сбрасывать hold"
    assert dropped.get("hold_started_game_time") is None


def test_lead_below_threshold_does_not_release() -> None:
    config = _watcher_config()
    snapshot = runtime._late_pre27_watcher_snapshot(config, 18 * 60)
    check = runtime._networth_monitor_hold_check(
        current_game_time=18 * 60,
        target_networth_diff=THRESHOLD - 1.0,
        monitor_threshold=float(snapshot["threshold"]),
        hold_started_game_time=None,
        hold_seconds=float(snapshot["hold_seconds"]),
    )
    assert check.get("threshold_met") is False
    assert check.get("ready") is False


# --- хелпер определения стороны all ---


def test_all_block_opposes_target_helper() -> None:
    assert runtime._all_block_opposes_target(
        has_selected_all_star=True, selected_all_sign=-1, target_sign=1
    ) is True
    assert runtime._all_block_opposes_target(
        has_selected_all_star=True, selected_all_sign=1, target_sign=1
    ) is False
    # Нет all-звезды — это не "против": ранний релиз разрешён.
    assert runtime._all_block_opposes_target(
        has_selected_all_star=False, selected_all_sign=None, target_sign=1
    ) is False
