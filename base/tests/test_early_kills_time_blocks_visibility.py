"""Visibility of early-kills time blocks (catalog + active index) in TG live state."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest

BASE = Path(__file__).resolve().parents[1]
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

os.environ.setdefault("LANE_ADV_STANDALONE_KILLS_ENABLED", "0")
os.environ.setdefault("EARLY_WINNER_KILLS_WINDOW_ENABLED", "0")

cs = importlib.import_module("cyberscore_try")


@pytest.mark.parametrize(
    "game_time,expected_index",
    [
        (0, 0),
        (179, 0),
        (180, 1),
        (359, 1),
        (360, 2),
        (599, 2),
        (600, 3),
        (644, 3),
        (779, 3),
        (780, 3),
        (900, 3),
        (959, 3),
        (960, 4),
    ],
)
def test_early_kills_time_block_index_from_game_time(game_time, expected_index):
    assert cs._early_kills_time_block_index_from_game_time(game_time) == expected_index


def test_status_labels_map_to_block_indices():
    assert (
        cs._early_kills_time_block_index_from_status(
            cs.NETWORTH_STATUS_TIER1_EARLY_KILLS_LANE_ADV_DICT_IMMEDIATE_SEND
        )
        == 0
    )
    assert (
        cs._early_kills_time_block_index_from_status(
            cs.NETWORTH_STATUS_TIER1_EARLY_KILLS_3_6_LEAD_SEND
        )
        == 1
    )
    assert (
        cs._early_kills_time_block_index_from_status(
            cs.NETWORTH_STATUS_TIER1_EARLY_KILLS_6_10_TARGET_NONNEG_SEND
        )
        == 2
    )
    assert (
        cs._early_kills_time_block_index_from_status(
            cs.NETWORTH_STATUS_TIER1_EARLY_KILLS_4_12_SEND_500
        )
        == 3
    )
    assert (
        cs._early_kills_time_block_index_from_status(
            cs.NETWORTH_STATUS_TIER1_EARLY_KILLS_WINDOW_CLOSED
        )
        == 4
    )


def test_live_state_includes_catalog_and_active_gate_for_10_44_fallback():
    # Dispatch-кап поднят до 16:00 (связка 20-30), header strip тоже 16:00 —
    # каталог в live-state блоке отражает актуальные границы.
    text = cs._format_live_message_state_block(
        game_time_seconds=644,
        radiant_lead=1720,
        radiant_team_name="Team Falcons",
        dire_team_name="Vici Gaming",
        show_kills_time_blocks=True,
        kills_release_status=cs.NETWORTH_STATUS_TIER1_EARLY_KILLS_4_12_SEND_500,
    )
    assert "Time: 10:44" in text
    assert "Networth: Team Falcons +1720" in text
    assert "Kills time blocks:" in text
    assert "[0]" in text and "[3]" in text and "[4]" in text
    assert "Kills gate: [3]" in text
    assert "10-13m fallback" in text
    assert "hard max 16:00" in text
    assert "header strip 16:00" in text
    assert "status=tier1_early_kills_4_12_send_500" in text


def test_kills_window_mode_shows_window_catalog_and_index():
    text = cs._format_live_message_state_block(
        game_time_seconds=120,
        radiant_lead=0,
        radiant_team_name="A",
        dire_team_name="B",
        show_kills_time_blocks=True,
        kills_window_label="10_20",
    )
    assert "Kills window blocks: [0]5_15 | [1]10_20 | [2]15_25 | [3]20_30" in text
    assert "Kills gate: [1] 10_20" in text


def test_without_flag_no_kills_block_lines():
    text = cs._format_live_message_state_block(
        game_time_seconds=644,
        radiant_lead=100,
        radiant_team_name="R",
        dire_team_name="D",
        show_kills_time_blocks=False,
    )
    assert "Time:" in text
    assert "Kills time blocks:" not in text
    assert "Kills gate:" not in text


def test_refresh_keeps_protracker_duo_under_1vs1_and_drops_kills_blocks():
    # Килы идут строго по 4 связкам policy: мусорный каталог "Kills time
    # blocks" из тела ставки удалён — refresh рендерит state-блок без него.
    msg = (
        "СТАВКА НА Ранние килы X\n"
        "A VS B\n"
        "All:\n"
        "Counterpick_1vs1: -1\n"
        "Solo: 0\n"
        "Protracker_1vs1: +0.75\n"
        "Protracker_duo: -3.21\n"
        "Time: 01:00\n"
        "Networth: 0\n"
    )
    smc = {
        "special_header_mode": "early_kills",
        "stake_team_name": "X",
        "radiant_team_name": "Team Falcons",
        "dire_team_name": "Vici",
        "selected_early_sign": 1,
        "selected_late_sign": None,
        "has_selected_early_star": True,
        "has_selected_late_star": False,
        "early_wr_pct": 75.0,
        "late_wr_pct": None,
        "kills_release_status": cs.NETWORTH_STATUS_TIER1_EARLY_KILLS_4_12_SEND_500,
    }
    out = cs._refresh_stake_multiplier_message(
        msg,
        stake_multiplier_context=smc,
        game_time_seconds=644,
        radiant_lead=1720,
    )
    lines = out.splitlines()
    i1 = next(i for i, line in enumerate(lines) if line.startswith("Protracker_1vs1:"))
    assert lines[i1 + 1].startswith("Protracker_duo:")
    assert out.find("Protracker_duo:") < out.find("Time:")
    assert "Kills time blocks:" not in out
    assert "Kills gate:" not in out
