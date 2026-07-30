"""Main late stake (x1+) must cancel speculative comeback x0.5 watcher."""

from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime


def test_speculative_header_detection_and_main_stake_lookup(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(
        "SENT_SIGNAL_FINGERPRINT_PATH",
        str(tmp_path / "sent_signal_fingerprints.json"),
    )
    runtime._SIGNAL_DEDUP_FINGERPRINTS.clear()
    runtime._SENT_SIGNAL_DEDUP_KEYS.clear()

    match_key = "dltv.org/matches/8891742793.44"
    runtime._signal_fingerprint_register(
        match_key,
        "Nemiga Gaming",
        "Team Spirit Academy",
        0,
        0,
    )
    assert runtime._is_speculative_stake_dedup_header("ставканаnemigagamingx05")
    assert not runtime._is_speculative_stake_dedup_header("ставканаnemigagamingx1")

    assert (
        runtime._main_late_stake_already_sent_for_map(
            match_key,
            stake_team_name="Nemiga Gaming",
        )
        is None
    )

    runtime._SENT_SIGNAL_DEDUP_KEYS.add(
        "nemigagaming|teamspiritacademy|map1|ставканаnemigagamingx1"
    )
    found = runtime._main_late_stake_already_sent_for_map(
        match_key,
        stake_team_name="Nemiga Gaming",
    )
    assert found == "nemigagaming|teamspiritacademy|map1|ставканаnemigagamingx1"

    # Speculative half alone must NOT count as main stake.
    runtime._SENT_SIGNAL_DEDUP_KEYS.clear()
    runtime._SENT_SIGNAL_DEDUP_KEYS.add(
        "nemigagaming|teamspiritacademy|map1|ставканаnemigagamingx05"
    )
    assert (
        runtime._main_late_stake_already_sent_for_map(
            match_key,
            stake_team_name="Nemiga Gaming",
        )
        is None
    )


def test_set_delayed_match_skips_queue_when_main_stake_already_sent(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "SENT_SIGNAL_FINGERPRINT_PATH",
        str(tmp_path / "sent_signal_fingerprints.json"),
    )
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(tmp_path / "delayed.json"))
    runtime._SIGNAL_DEDUP_FINGERPRINTS.clear()
    runtime._SENT_SIGNAL_DEDUP_KEYS.clear()
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()

    match_key = "dltv.org/matches/8891742793.50"
    runtime._signal_fingerprint_register(
        match_key,
        "Nemiga Gaming",
        "Team Spirit Academy",
        0,
        0,
    )
    runtime._SENT_SIGNAL_DEDUP_KEYS.add(
        "nemigagaming|teamspiritacademy|map1|ставканаnemigagamingx1"
    )

    runtime._set_delayed_match(
        match_key,
        {
            "message": "СТАВКА НА Nemiga Gaming x0.5\nNemiga VS Spiritacademy\n0-0\n",
            "reason": "late_all_same_weak_early_pre27_watcher",
            "stake_multiplier_context": {
                "stake_team_name": "Nemiga Gaming",
                "radiant_team_name": "Team Spirit Academy",
                "dire_team_name": "Nemiga Gaming",
            },
            "target_game_time": 1620.0,
        },
    )

    with runtime.monitored_matches_lock:
        assert match_key not in runtime.monitored_matches


def test_drop_delayed_matches_for_registry_clears_sibling_suffixes(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "SENT_SIGNAL_FINGERPRINT_PATH",
        str(tmp_path / "sent_signal_fingerprints.json"),
    )
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(tmp_path / "delayed.json"))
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
        runtime.monitored_matches["dltv.org/matches/8891742793.11"] = {"reason": "a"}
        runtime.monitored_matches["dltv.org/matches/8891742793.44"] = {"reason": "b"}
        runtime.monitored_matches["dltv.org/matches/9999999999.1"] = {"reason": "other"}

    dropped = runtime._drop_delayed_matches_for_registry(
        "dltv.org/matches/8891742793.26",
        reason="main_late_pub_table_sent_cancels_watcher",
    )
    assert dropped == 2
    with runtime.monitored_matches_lock:
        assert "dltv.org/matches/8891742793.11" not in runtime.monitored_matches
        assert "dltv.org/matches/8891742793.44" not in runtime.monitored_matches
        assert "dltv.org/matches/9999999999.1" in runtime.monitored_matches
