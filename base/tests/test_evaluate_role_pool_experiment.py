from itertools import combinations

from base.cp1vs2_role_pool import make_role_key
from base.evaluate_role_pool_experiment import (
    POSITIONS,
    cp_score,
    draft_tokens,
    trio_score,
)
from base.synergy_trio_role_pool import make_trio_role_key


def _draft(first_hero_id):
    return {
        position: {"hero_id": first_hero_id + index}
        for index, position in enumerate(POSITIONS)
    }


def test_role_pooled_cp1vs2_requires_coverage_for_all_five_heroes():
    radiant = _draft(1)
    dire = _draft(6)
    data = {}
    for team, enemy, winrate in ((radiant, dire, 0.60), (dire, radiant, 0.40)):
        own = draft_tokens(team)
        opponents = draft_tokens(enemy)
        assert own and opponents
        for token in own:
            for duo in combinations(opponents, 2):
                data[make_role_key(token, duo)] = (winrate * 100, 100)

    assert cp_score(radiant, dire, data, 25, "early") > 0

    radiant_pos5 = draft_tokens(radiant)[-1]
    for duo in combinations(draft_tokens(dire), 2):
        data.pop(make_role_key(radiant_pos5, duo))
    assert cp_score(radiant, dire, data, 25, "early") is None


def test_role_pooled_trio_higher_n_changes_coverage_without_recounting():
    radiant = _draft(1)
    dire = _draft(6)
    data = {}
    for team, winrate in ((radiant, 0.60), (dire, 0.40)):
        tokens = draft_tokens(team)
        assert tokens
        for trio in combinations(tokens, 3):
            data[make_trio_role_key(trio)] = (winrate * 75, 75)

    assert trio_score(radiant, dire, data, 25) > 0
    assert trio_score(radiant, dire, data, 75) > 0
    assert trio_score(radiant, dire, data, 100) is None
