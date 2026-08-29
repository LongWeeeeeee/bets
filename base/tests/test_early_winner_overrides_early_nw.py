"""Early Winner важнее Early NW, когда early-блоки смотрят в разные стороны.

Референс-кейс (прод, 31.07): RE ARISE vs BALU TEAM.
Early NW → BALU TEAM (WR≈60), Early Winner → RE ARISE (WR≈75),
Late → RE ARISE (WR≈80), All → RE ARISE (WR≈75).

До правки диспатч смотрел на early-сторону ТОЛЬКО через ``early_output``:
знаки early/late расходились → ``delay_late_only_opposite_signs`` →
pre-27 watcher, и ставка ушла на 24:54. Ожидаемое поведение: при разных
знаках авторитетным early-блоком становится тот, у которого WR выше
(здесь Early Winner, 75 > 60) → обычный networth-гейт 0-10 / после 10 минут.
"""

from __future__ import annotations

import sys
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from test_networth_dispatch_gates import (  # noqa: E402
    BranchScenario,
    _run_branch_scenario,
    runtime,
)


ALIGNED_LANE_OUTPUT = ("Top: win 70%", "Bot: win 70%", "Mid: win 70%")

REARISE_EARLY_NW = {"solo": -3}
REARISE_EARLY_WINNER = {"counterpick_1vs1": 14, "solo": 8}
REARISE_LATE = {"counterpick_1vs1": 14, "counterpick_1vs2": 21, "solo": 8}
REARISE_ALL = {"counterpick_1vs1": 8, "counterpick_1vs2": 15, "protracker_1vs1": 4.3}


def _patch_block_wr(monkeypatch, *, early, early_end, late, all_level) -> None:
    """WR по блокам как в карточке RE ARISE: 60 / 75 / 80 / 75."""

    levels = {
        "early": early,
        "early_end": early_end,
        "late": late,
        "mid": late,
        "all": all_level,
    }

    def _recommend(_data, phase):
        level = levels.get(str(phase))
        if level is None:
            return None
        return {
            "level": int(level),
            "wr_pct": float(level),
            "min_odds": round(100.0 / float(level), 2),
        }

    monkeypatch.setattr(runtime, "_recommend_odds_for_block", _recommend)


def _rearise_case(**overrides) -> BranchScenario:
    params = dict(
        name="rearise_early_winner_overrides_early_nw",
        game_time_seconds=8 * 60,
        target_side="radiant",
        target_networth_diff=1200,
        has_early_star=True,
        early_sign=-1,
        has_early_end_star=True,
        early_end_sign=1,
        has_late_star=True,
        late_sign=1,
        has_all_star=True,
        all_sign=1,
        expected_send_calls=1,
        raw_early_output=dict(REARISE_EARLY_NW),
        raw_early_end_output=dict(REARISE_EARLY_WINNER),
        raw_mid_output=dict(REARISE_LATE),
        raw_post_lane_output=dict(REARISE_ALL),
    )
    params.update(overrides)
    return BranchScenario(**params)


def _patch_pre27_grid(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "late_pre27_watcher_thresholds_by_group_wr",
        {
            "late_all": {
                level: {10: -900.0, 11: -1200.0, 12: -1500.0}
                for level in (65, 70, 75, 80)
            }
        },
        raising=False,
    )


# ── чистый предикат ────────────────────────────────────────────────────────

_EARLY_NW_DIAG = {"valid": True, "sign": -1}
_EARLY_WINNER_DIAG = {"valid": True, "sign": 1}


def test_helper_prefers_early_winner_when_wr_is_higher() -> None:
    assert runtime._early_winner_overrides_early_nw(
        early_diag=_EARLY_NW_DIAG,
        early_wr_pct=60.0,
        early_end_diag=_EARLY_WINNER_DIAG,
        early_end_wr_pct=75.0,
    ) is True


def test_helper_keeps_early_nw_when_its_wr_is_not_lower() -> None:
    for winner_wr in (60.0, 55.0):
        assert runtime._early_winner_overrides_early_nw(
            early_diag=_EARLY_NW_DIAG,
            early_wr_pct=60.0,
            early_end_diag=_EARLY_WINNER_DIAG,
            early_end_wr_pct=winner_wr,
        ) is False


def test_helper_requires_opposite_signs() -> None:
    assert runtime._early_winner_overrides_early_nw(
        early_diag={"valid": True, "sign": 1},
        early_wr_pct=60.0,
        early_end_diag=_EARLY_WINNER_DIAG,
        early_end_wr_pct=75.0,
    ) is False


def test_helper_requires_both_blocks_valid() -> None:
    assert runtime._early_winner_overrides_early_nw(
        early_diag={"valid": False, "sign": None},
        early_wr_pct=60.0,
        early_end_diag=_EARLY_WINNER_DIAG,
        early_end_wr_pct=75.0,
    ) is False
    assert runtime._early_winner_overrides_early_nw(
        early_diag=_EARLY_NW_DIAG,
        early_wr_pct=60.0,
        early_end_diag={"valid": False, "sign": None},
        early_end_wr_pct=75.0,
    ) is False


def test_helper_requires_numeric_wr() -> None:
    assert runtime._early_winner_overrides_early_nw(
        early_diag=_EARLY_NW_DIAG,
        early_wr_pct=None,
        early_end_diag=_EARLY_WINNER_DIAG,
        early_end_wr_pct=75.0,
    ) is False


def test_helper_disabled_by_flag(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "EARLY_WINNER_OVERRIDES_EARLY_NW", False, raising=False)
    assert runtime._early_winner_overrides_early_nw(
        early_diag=_EARLY_NW_DIAG,
        early_wr_pct=60.0,
        early_end_diag=_EARLY_WINNER_DIAG,
        early_end_wr_pct=75.0,
    ) is False


# ── сквозной диспатч ───────────────────────────────────────────────


def test_rearise_map_dispatches_immediately_instead_of_pre27_watcher(monkeypatch) -> None:
    """Early Winner подменяет Early NW → early/late/all одного знака → отправка."""

    _patch_block_wr(monkeypatch, early=60, early_end=75, late=80, all_level=75)
    _patch_pre27_grid(monkeypatch)

    result = _run_branch_scenario(
        monkeypatch,
        _rearise_case(),
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    details = result.add_url_calls[-1]["details"]
    # Для диспатча early-сторона теперь radiant (Early Winner), а не dire (Early NW).
    assert details["selected_early_star"] is True
    assert details["selected_early_sign"] == 1
    assert details["selected_late_sign"] == 1
    assert details["selected_all_sign"] == 1
    assert details["dispatch_mode"] == "immediate_star_rule"


def test_rearise_card_still_prints_both_early_blocks(monkeypatch) -> None:
    """Подмена — только для диспатча: в карточке Early NW остаётся своим."""

    _patch_block_wr(monkeypatch, early=60, early_end=75, late=80, all_level=75)
    _patch_pre27_grid(monkeypatch)

    result = _run_branch_scenario(
        monkeypatch,
        _rearise_case(),
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    message = result.sent_messages[0]
    # Early NW печатается своей стороной и своим WR, несмотря на подмену.
    assert "Early NW: Dire Team WR≈60.0%" in message
    assert "Early Winner: WR≈75.0%" in message
    assert "Late: Radiant Team WR≈80.0%" in message


def test_rearise_map_dispatches_after_minute_10(monkeypatch) -> None:
    """После 10 минуты карта тоже уходит, а не ждёт watcher до 27:00."""

    _patch_block_wr(monkeypatch, early=60, early_end=75, late=80, all_level=75)
    _patch_pre27_grid(monkeypatch)

    result = _run_branch_scenario(
        monkeypatch,
        _rearise_case(game_time_seconds=11 * 60, target_networth_diff=1200),
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None


def test_flag_off_restores_pre27_watcher_delay(monkeypatch) -> None:
    """С EARLY_WINNER_OVERRIDES_EARLY_NW=0 возвращается старое поведение.

    Это и есть воспроизведение бага: early NW dire против late/all radiant →
    opposite signs → задержка до 27:00 вместо отправки в 0-10.
    """

    monkeypatch.setattr(runtime, "EARLY_WINNER_OVERRIDES_EARLY_NW", False, raising=False)
    _patch_block_wr(monkeypatch, early=60, early_end=75, late=80, all_level=75)
    _patch_pre27_grid(monkeypatch)

    result = _run_branch_scenario(
        monkeypatch,
        _rearise_case(expected_send_calls=0),
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["dynamic_monitor_profile"] == runtime.LATE_PRE27_WATCHER_PROFILE
    assert int(result.queued_payload["target_game_time"]) == int(
        runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS
    )
