from dataclasses import replace

import pytest

from ELO.domain import LeagueTier, MatchRecord
from ELO.replay import replay_events, result_record
from ELO.evaluation import run_online_evaluation
from ELO.config import EvaluationConfig, SimpleTeamEloConfig
from ELO.models import SimpleTeamEloModel
from ELO.tiering import attach_league_tiers_asof


def match(mid, start, duration):
    return MatchRecord(mid, start, True, 10, "A", 20, "B", (1, 2, 3, 4, 5),
                       (6, 7, 8, 9, 10), 1, "League", None, 1, "BEST_OF_THREE",
                       duration_seconds=duration)


def test_results_wait_until_finish_and_equal_timestamp_starts_come_first():
    matches = [match(1, 10, 100), match(2, 20, 5), match(3, 25, 10), match(4, 25, None)]
    events = [(kind, ts, m.match_id) for kind, ts, m in replay_events(matches)]
    assert events == [("start", 10, 1), ("start", 20, 2), ("start", 25, 3),
                      ("start", 25, 4), ("result", 25, 2), ("result", 35, 3), ("result", 110, 1)]


@pytest.mark.parametrize("duration", [None, 0, -1, True, 4.5, "900"])
def test_unknown_or_invalid_duration_never_learns_at_start(duration):
    m = match(1, 10, duration)
    assert m.result_timestamp is None
    assert list(replay_events([m])) == [("start", 10, m)]
    with pytest.raises(ValueError, match="observation"):
        result_record(m)


def test_map_evaluation_does_not_use_outcome_of_still_running_match():
    matches = [match(1, 10, 100), match(2, 20, 5), match(3, 30, 5)]
    config = EvaluationConfig(evaluation_fraction=1, min_train_matches=0)
    report = run_online_evaluation(SimpleTeamEloModel(SimpleTeamEloConfig()), matches, config)
    probs = [r["p_radiant"] for r in report["sample_predictions"]]
    assert probs[:2] == [0.5, 0.5]
    assert probs[2] > .5
    changed = [replace(matches[0], radiant_win=False), *matches[1:]]
    other = run_online_evaluation(SimpleTeamEloModel(SimpleTeamEloConfig()), changed, config)
    assert probs == [r["p_radiant"] for r in other["sample_predictions"]]


def test_asof_league_tier_does_not_use_future_participants(monkeypatch):
    import ELO.tiering as tiers
    monkeypatch.setattr(tiers, "KNOWN_TIER1_IDS", {30, 40, 50, 60})
    monkeypatch.setattr(tiers, "KNOWN_TIER2_IDS", set())
    first = match(1, 10, 5)
    future = [replace(match(2, 20, 5), radiant_team_id=30, dire_team_id=40),
              replace(match(3, 30, 5), radiant_team_id=50, dire_team_id=60)]
    attach_league_tiers_asof([first, *future])
    assert first.derived_league_tier == LeagueTier.TIER3
    assert future[-1].derived_league_tier == LeagueTier.TIER1
