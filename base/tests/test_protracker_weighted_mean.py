"""ProTracker cp1vs1/duo 'scores' already carry diff*weight per pair; the
consumers averaged them with sum(scores)/len(scores) — a plain mean of
already-weighted values, not the real weighted mean Σ(diff·w)/Σw.

Worked example from the review: pairs (diff +20, weight 3.0) and (diff 0,
weight 1.6). Old formula: (20*3.0 + 0*1.6) / 2 = 30.0. Correct weighted mean:
(20*3.0 + 0*1.6) / (3.0 + 1.6) = 60.0 / 4.6 = 13.043...

These exact weights (3.0, 1.6) are not contrived — they are the real
production values of PRO_CP1VS1_PAIR_WEIGHTS[('pos1','pos1')] and
[('pos1','pos3')] (dota2protracker.py ~729), so the boundary test below
drives the real `_calculate_cp1vs1_all_positions` builder with those exact
positions instead of a hand-rolled scores/weights pair.

Fix is behind env flag PRO_CP_WEIGHTED_MEAN (default "0" = old behaviour,
byte-identical) so deploy is safe; thresholds/star gates calibrated against
the old (biased-toward-count) behaviour need a separate re-derivation before
flipping the flag in production.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import dota2protracker as protracker  # noqa: E402


def _add_precise_matchup(
    hero_data: dict,
    radiant_hero: str,
    dire_hero: str,
    radiant_pos: str,
    dire_pos: str,
    wr: float,
    games: int = 20,
) -> None:
    radiant_key = protracker._hero_norm_key(radiant_hero)
    dire_key = protracker._hero_norm_key(dire_hero)
    radiant_pos_num = protracker.POSITION_MAP[radiant_pos]
    dire_pos_num = protracker.POSITION_MAP[dire_pos]
    entry = hero_data.setdefault(radiant_key, {"_matchups_by_hero_pos": {}})
    entry.setdefault("_matchups_by_hero_pos", {}).setdefault(dire_key, {}).setdefault(
        dire_pos_num, {}
    )[radiant_pos_num] = {"wr": wr, "games": games, "diff": wr - 50.0, "metric": "match_wr"}


def test_protracker_weighted_mean_unit_matches_worked_example(monkeypatch) -> None:
    """Direct unit check of the formula: flag off 30.0, flag on 13.043."""
    scores = [20.0 * 3.0, 0.0 * 1.6]
    weights = [3.0, 1.6]

    monkeypatch.setattr(protracker, "PRO_CP_WEIGHTED_MEAN", False)
    assert protracker._protracker_weighted_mean(scores, weights) == 30.0

    monkeypatch.setattr(protracker, "PRO_CP_WEIGHTED_MEAN", True)
    assert protracker._protracker_weighted_mean(scores, weights) == pytest.approx(13.043, abs=1e-3)


def test_cp1vs1_all_positions_weighted_mean_boundary(monkeypatch) -> None:
    """Boundary: drive the real builder with real PRO_CP1VS1_PAIR_WEIGHTS
    entries (pos1,pos1)=3.0 and (pos1,pos3)=1.6 — not hand-rolled numbers —
    then feed its 'scores'/'weights' through the same helper the production
    code path uses."""
    hero_data: dict = {}
    # (pos1, pos1): weight 3.0, wr=70 -> diff=+20
    _add_precise_matchup(hero_data, "Hero A", "Hero B", "pos1", "pos1", wr=70.0)
    # (pos1, pos3): weight 1.6, wr=50 -> diff=0
    _add_precise_matchup(hero_data, "Hero A", "Hero C", "pos1", "pos3", wr=50.0)

    radiant_positions = [("pos1", "Hero A")]
    dire_positions = [("pos1", "Hero B"), ("pos3", "Hero C")]

    _valid, data = protracker._calculate_cp1vs1_all_positions(
        radiant_positions=radiant_positions,
        dire_positions=dire_positions,
        hero_data=hero_data,
        min_games=10,
    )

    assert data["scores"] == [20.0 * 3.0, 0.0 * 1.6]
    assert data["weights"] == [3.0, 1.6]

    monkeypatch.setattr(protracker, "PRO_CP_WEIGHTED_MEAN", False)
    assert protracker._protracker_weighted_mean(data["scores"], data["weights"]) == 30.0

    monkeypatch.setattr(protracker, "PRO_CP_WEIGHTED_MEAN", True)
    assert protracker._protracker_weighted_mean(
        data["scores"], data["weights"]
    ) == pytest.approx(13.043, abs=1e-3)


def test_weighted_mean_falls_back_to_plain_mean_when_no_weights(monkeypatch) -> None:
    """No weights recorded (old-format 'weights' missing/empty) -> falls back
    to the old sum/len path even with the flag on, instead of dividing by
    zero or crashing."""
    monkeypatch.setattr(protracker, "PRO_CP_WEIGHTED_MEAN", True)
    assert protracker._protracker_weighted_mean([10.0, 20.0], []) == 15.0
    assert protracker._protracker_weighted_mean([10.0, 20.0], None) == 15.0
