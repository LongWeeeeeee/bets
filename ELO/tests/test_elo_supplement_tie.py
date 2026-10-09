"""An exact start-time tie uses that corpus spelling, across distinct Valve maps."""
from dataclasses import replace

import pytest

from ELO.domain import MatchRecord
from ELO import live_team_strength as live


@pytest.mark.parametrize('side', ['radiant', 'dire'])
def test_equal_timestamp_prefers_corpus_spelling(side):
    team = 2000000001
    same_second = 1791424800
    corpus_at_tie = MatchRecord(
        match_id=9021470896, timestamp=same_second, radiant_win=True,
        radiant_team_id=team, radiant_team_name='Corpus at tie',
        dire_team_id=10, dire_team_name='Other',
        radiant_player_ids=(1, 2, 3, 4, 5), dire_player_ids=(6, 7, 8, 9, 10),
        league_id=1, league_name='L', source_league_tier='PROFESSIONAL',
        series_id=None, series_type=None,
    )
    if side == 'dire':
        corpus_at_tie = replace(corpus_at_tie, radiant_team_id=10,
                                radiant_team_name='Other', dire_team_id=team,
                                dire_team_name='Corpus at tie')
    name_field = side + '_team_name'
    earlier = replace(corpus_at_tie, match_id=9021470895, timestamp=same_second - 60,
                      **{name_field: 'Earlier spelling'})
    later = replace(corpus_at_tie, match_id=9021470898, timestamp=same_second + 60,
                    **{name_field: 'Later spelling'})
    supplement = replace(corpus_at_tie, match_id=9021470897,
                         **{name_field: 'OpenDota spelling'})

    aligned, changed = live._align_supplement_team_names(
        [earlier, corpus_at_tie, later], [supplement])

    assert changed == 1
    assert aligned == [replace(supplement, **{name_field: 'Corpus at tie'})]
    assert getattr(supplement, name_field) == 'OpenDota spelling'
