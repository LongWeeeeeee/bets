from __future__ import annotations

import sys
from pathlib import Path

import pytest


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


def _header(team: str, multiplier: float) -> str:
    return runtime._format_signal_header(stake_team_name=team, stake_multiplier=multiplier)


def test_stake_multiplier_parsed_from_header():
    assert runtime._stake_multiplier_from_message(_header("Team A", 0.5)) == 0.5
    assert runtime._stake_multiplier_from_message(_header("Team A", 1.0)) == 1.0
    assert runtime._stake_multiplier_from_message(_header("Team A", 3.0)) == 3.0
    assert runtime._stake_multiplier_from_message(f"{_header('Team A', 0.5)}\nTeam A VS Team B") == 0.5


def test_kills_headers_carry_no_multiplier():
    kills_header = runtime._format_signal_header(
        stake_team_name="Team A",
        stake_multiplier=0.5,
        special_header_mode="early_kills",
    )
    from_header = runtime._format_signal_header(
        stake_team_name="Team A",
        stake_multiplier=0.5,
        special_header_mode="kills_from",
    )
    assert runtime._stake_multiplier_from_message(kills_header) is None
    assert runtime._stake_multiplier_from_message(from_header) is None
    assert runtime._stake_multiplier_from_message("") is None


@pytest.mark.parametrize(
    "target_rating, opposite_rating, expected_block",
    [
        (1500.0, 1550.0, True),   # таргет слабее ровно на 50 → блок
        (1500.0, 1600.0, True),   # слабее сильнее порога → блок
        (1500.0, 1549.0, False),  # слабее, но меньше 50 → пропуск
        (1600.0, 1500.0, False),  # таргет фаворит → пропуск
        (1500.0, 1500.0, False),  # равные → пропуск
    ],
)
def test_half_stake_blocked_only_for_elo_underdog(target_rating, opposite_rating, expected_block):
    decision = runtime._half_stake_elo_underdog_reject(
        stake_multiplier=0.5,
        target_rating=target_rating,
        opposite_rating=opposite_rating,
    )
    assert (decision is not None) is expected_block
    if expected_block:
        assert decision["elo_diff"] == pytest.approx(opposite_rating - target_rating)
        assert decision["min_diff"] == pytest.approx(runtime.HALF_STAKE_ELO_UNDERDOG_MIN_DIFF)


@pytest.mark.parametrize("multiplier", [1.0, 2.0, 3.0])
def test_non_half_stakes_never_blocked(multiplier):
    assert (
        runtime._half_stake_elo_underdog_reject(
            stake_multiplier=multiplier,
            target_rating=1400.0,
            opposite_rating=1900.0,
        )
        is None
    )


def test_missing_ratings_do_not_block():
    assert (
        runtime._half_stake_elo_underdog_reject(
            stake_multiplier=0.5,
            target_rating=None,
            opposite_rating=1900.0,
        )
        is None
    )
    assert (
        runtime._half_stake_elo_underdog_reject(
            stake_multiplier=0.5,
            target_rating=1400.0,
            opposite_rating=None,
        )
        is None
    )


def test_reject_for_delivery_uses_message_header_and_context():
    context = {
        "stake_team_name": "Team A",
        "target_side": "radiant",
        "target_rating": 1500.0,
        "opposite_rating": 1600.0,
    }
    blocked = runtime._half_stake_elo_underdog_reject_for_delivery(
        f"{_header('Team A', 0.5)}\nTeam A VS Team B",
        context,
    )
    assert blocked is not None
    assert blocked["target_side"] == "radiant"
    assert blocked["stake_team_name"] == "Team A"

    assert (
        runtime._half_stake_elo_underdog_reject_for_delivery(
            f"{_header('Team A', 1.0)}\nTeam A VS Team B",
            context,
        )
        is None
    )
    # Без контекста ставки правило не применяется.
    assert runtime._half_stake_elo_underdog_reject_for_delivery(_header("Team A", 0.5), None) is None


def test_delivery_is_blocked_before_send(monkeypatch):
    sent: list[str] = []
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda *args, **kwargs: sent.append(str(args[0] if args else "")),
    )
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda *args, **kwargs: pytest.fail("add_url must not be called for blocked x0.5"),
    )
    context = {
        "stake_team_name": "Team A",
        "target_side": "radiant",
        "target_rating": 1500.0,
        "opposite_rating": 1600.0,
    }
    delivered = runtime._deliver_and_persist_signal(
        "dltv.org/matches/1.1",
        f"{_header('Team A', 0.5)}\nTeam A VS Team B",
        add_url_reason="star_signal_sent_now",
        skip_bookmaker_prepare=True,
        stake_multiplier_context=context,
    )
    assert delivered is False
    assert sent == []


def test_delivery_not_blocked_without_context(monkeypatch):
    sent: list[str] = []
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda *args, **kwargs: sent.append(str(args[0] if args else "")),
    )
    monkeypatch.setattr(runtime, "add_url", lambda *args, **kwargs: None)
    monkeypatch.setattr(runtime, "_signal_fingerprint_try_reserve", lambda *args, **kwargs: (True, "k"))
    monkeypatch.setattr(runtime, "_signal_fingerprint_mark_sent", lambda *args, **kwargs: None)

    delivered = runtime._deliver_and_persist_signal(
        "dltv.org/matches/2.1",
        f"{_header('Team A', 0.5)}\nTeam A VS Team B",
        add_url_reason="star_signal_sent_now",
        skip_bookmaker_prepare=True,
    )
    assert delivered is True
    assert len(sent) == 1


# ── Интеграция: тот же гейт через реальный dispatch-путь ──────────────────

TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from test_networth_dispatch_gates import (  # noqa: E402
    BranchScenario,
    _patch_team_elo_summary,
    _run_branch_scenario,
)
from test_same_sign_lane_adv_dispatch import _patch_early_late_wr  # noqa: E402


def _all_only_half_stake_case() -> BranchScenario:
    # All-only блок всегда даёт x0.5 (см. _stake_multiplier_for_signal).
    return BranchScenario(
        name="all_only_half_stake",
        game_time_seconds=5 * 60,
        target_side="radiant",
        target_networth_diff=900,
        has_early_star=False,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        has_all_star=True,
        all_sign=1,
        expected_send_calls=0,
        raw_early_output={"solo": 0},
        raw_mid_output={"solo": 0},
        raw_post_lane_output={"synergy_duo": 5},
    )


def _prepare_all_only(monkeypatch) -> None:
    _patch_early_late_wr(monkeypatch, early_level=60, late_level=65, all_level=70)
    monkeypatch.setattr(
        runtime,
        "all_only_watcher_thresholds_by_wr",
        {70: {10: -1000.0, 11: -1200.0, 12: -1500.0}},
        raising=False,
    )


def test_dispatch_sends_half_stake_when_teams_are_even(monkeypatch) -> None:
    _prepare_all_only(monkeypatch)
    _patch_team_elo_summary(monkeypatch, radiant_wr=50.0, dire_wr=50.0)

    result = _run_branch_scenario(monkeypatch, _all_only_half_stake_case())

    assert len(result.sent_messages) == 1
    assert runtime._stake_multiplier_from_message(result.sent_messages[0]) == 0.5


def test_dispatch_blocks_half_stake_when_target_is_elo_underdog(monkeypatch) -> None:
    _prepare_all_only(monkeypatch)
    # radiant 1470 vs dire 1530 → таргет слабее на 60 пунктов (>= 50).
    _patch_team_elo_summary(monkeypatch, radiant_wr=42.5, dire_wr=57.5)

    result = _run_branch_scenario(monkeypatch, _all_only_half_stake_case())

    assert result.sent_messages == []


def test_dispatch_sends_half_stake_when_block_disabled(monkeypatch) -> None:
    _prepare_all_only(monkeypatch)
    _patch_team_elo_summary(monkeypatch, radiant_wr=42.5, dire_wr=57.5)
    monkeypatch.setattr(runtime, "HALF_STAKE_ELO_UNDERDOG_BLOCK_ENABLED", False, raising=False)

    result = _run_branch_scenario(monkeypatch, _all_only_half_stake_case())

    assert len(result.sent_messages) == 1
    assert runtime._stake_multiplier_from_message(result.sent_messages[0]) == 0.5
