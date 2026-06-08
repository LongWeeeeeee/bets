import sys
from pathlib import Path
import importlib

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

runtime = importlib.import_module("cyberscore_try")


def test_tempo_over_fallback_diagnostics_without_elo():
    # Diagnostics should fallback to default base threshold (0.9965) when no Elo is provided
    runtime.TEMPO_OVER_FALLBACK_ENABLED = True
    runtime.TEMPO_OVER_SCORE_THRESHOLD = 0.9965

    diag = runtime._compute_tempo_over_fallback_diagnostics(
        radiant_heroes_and_pos={},
        dire_heroes_and_pos={},
        match_tier=1,
        elo_summary=None
    )
    assert isinstance(diag, dict)
    assert diag["threshold"] == 0.9965
    assert diag["elo_delta"] is None
    assert diag["elo_reason"] == "standard"

def test_tempo_over_fallback_diagnostics_with_close_elo():
    # When ratings difference < 50, it should relax threshold to 0.8500
    elo_summary = {
        "radiant": {"base_rating": 1800.0},
        "dire": {"base_rating": 1780.0} # delta = 20
    }
    diag = runtime._compute_tempo_over_fallback_diagnostics(
        radiant_heroes_and_pos={},
        dire_heroes_and_pos={},
        match_tier=1,
        elo_summary=elo_summary
    )
    assert isinstance(diag, dict)
    assert diag["threshold"] == 0.8500
    assert diag["elo_delta"] == 20.0
    assert diag["elo_reason"] == "elo_equals_relaxed"

def test_tempo_over_fallback_diagnostics_with_stomp_elo():
    # When ratings difference > 150, it should restrict threshold to 1.2500
    elo_summary = {
        "radiant": {"base_rating": 1900.0},
        "dire": {"base_rating": 1700.0} # delta = 200
    }
    diag = runtime._compute_tempo_over_fallback_diagnostics(
        radiant_heroes_and_pos={},
        dire_heroes_and_pos={},
        match_tier=1,
        elo_summary=elo_summary
    )
    assert isinstance(diag, dict)
    assert diag["threshold"] == 1.2500
    assert diag["elo_delta"] == 200.0
    assert diag["elo_reason"] == "elo_stomp_strict"

def test_tempo_over_fallback_diagnostics_blocked_by_push_hero():
    # Under any circumstances, having a push hero like Chen (66) should trigger a block
    r_draft = {"pos1": 1, "pos2": 2, "pos3": 3, "pos4": 4, "pos5": 66} # 66 = Chen
    diag = runtime._compute_tempo_over_fallback_diagnostics(
        radiant_heroes_and_pos=r_draft,
        dire_heroes_and_pos={},
        match_tier=1,
    )
    assert isinstance(diag, dict)
    assert diag["payload"] is None
    assert diag["reason"] == "push_hero_blocked"

def test_tempo_over_fallback_diagnostics_relaxed_by_bloody_hero():
    # Spectre (67) is a bloody hero, should relax base threshold to 0.7000
    r_draft = {"pos1": 67, "pos2": 2, "pos3": 3, "pos4": 4, "pos5": 5} # 67 = Spectre
    diag = runtime._compute_tempo_over_fallback_diagnostics(
        radiant_heroes_and_pos=r_draft,
        dire_heroes_and_pos={},
        match_tier=1,
    )
    assert isinstance(diag, dict)
    assert diag["threshold"] == 0.7000

def test_tempo_over_fallback_diagnostics_active_mids_relax():
    # Having active midlaners (Ember 106 vs Puck 13 on pos2) relaxes base threshold to 0.7000
    r_draft = {"pos1": 1, "pos2": 106, "pos3": 3, "pos4": 4, "pos5": 5} # pos2 = Ember
    d_draft = {"pos1": 12, "pos2": 13, "pos3": 14, "pos4": 15, "pos5": 16} # pos2 = Puck
    diag = runtime._compute_tempo_over_fallback_diagnostics(
        radiant_heroes_and_pos=r_draft,
        dire_heroes_and_pos=d_draft,
        match_tier=1,
    )
    assert isinstance(diag, dict)
    assert diag["threshold"] == 0.7000
