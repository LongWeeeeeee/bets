from base.evaluate_four_hit_synergy_experiment import (
    ALL_SOLO_WR60_THRESHOLD,
    base_hit_signs,
    consensus_sign,
    exact_two_variant,
    wr60_thresholds,
)


def test_wr60_thresholds_select_only_base_metrics():
    payload = {
        "60": {
            "early_output": [
                ["counterpick_1vs1", 4],
                ["solo", 3],
                ["counterpick_1vs2", 4],
                ["synergy_trio", 25],
            ]
        }
    }
    assert wr60_thresholds(payload, "early_output") == {
        "counterpick_1vs1": 4.0,
        "solo": 3.0,
        "counterpick_1vs2": 4.0,
    }


def test_all_solo_wr60_family_threshold_is_four():
    assert ALL_SOLO_WR60_THRESHOLD == 4.0


def test_base_hits_require_the_current_wr60_threshold():
    hits = base_hit_signs(
        {"counterpick_1vs1": 4, "solo": -2, "counterpick_1vs2": 6},
        {"counterpick_1vs1": 4, "solo": 3, "counterpick_1vs2": 4},
    )
    assert hits == {"counterpick_1vs1": 1, "counterpick_1vs2": 1}
    assert consensus_sign(hits, 2) == 1
    assert exact_two_variant(hits) == "hits2_cp1vs1+cp1vs2"


def test_three_hits_must_have_one_consensus_sign():
    assert consensus_sign(
        {"counterpick_1vs1": 1, "solo": 1, "counterpick_1vs2": 1},
        3,
    ) == 1
    assert consensus_sign(
        {"counterpick_1vs1": 1, "solo": -1, "counterpick_1vs2": 1},
        3,
    ) is None
