from __future__ import annotations

import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import analise_database as stats  # noqa: E402


def _player(hero_id: int, position: int, is_radiant: bool, *, imp: int = 0) -> dict:
    return {
        "heroId": hero_id,
        "position": f"POSITION_{position}",
        "isRadiant": is_radiant,
        "intentionalFeeding": False,
        "imp": imp,
    }


def _match(
    *,
    match_id: str = "101",
    duration: int = 25,
    minute_10_lead: int = 0,
    radiant_win: bool = True,
    imp: int = 0,
) -> dict:
    leads = [0 for _ in range(duration)]
    if duration >= stats.POST_LANE_GATE_MINUTE:
        leads[stats.POST_LANE_GATE_MINUTE - 1] = minute_10_lead
    return {
        "id": match_id,
        "didRadiantWin": radiant_win,
        "radiantNetworthLeads": leads,
        "winRates": [0.6 if radiant_win else 0.4],
        "topLaneOutcome": "RADIANT_WIN",
        "midLaneOutcome": "TIE",
        "bottomLaneOutcome": "DIRE_WIN",
        "players": [
            _player(1, 1, True, imp=imp),
            _player(2, 2, True, imp=imp),
            _player(3, 3, True, imp=imp),
            _player(4, 4, True, imp=imp),
            _player(5, 5, True, imp=imp),
            _player(6, 1, False, imp=imp),
            _player(7, 2, False, imp=imp),
            _player(8, 3, False, imp=imp),
            _player(9, 4, False, imp=imp),
            _player(10, 5, False, imp=imp),
        ],
    }


def test_post_lane_dict_records_winner_for_full_metric_set() -> None:
    lane_dict = {}
    early_dict = {}
    late_dict = {}
    post_lane_dict = {}

    stats.analise_database(
        _match(duration=95, minute_10_lead=1500, radiant_win=True),
        lane_dict,
        early_dict,
        late_dict,
        post_lane_dict=post_lane_dict,
    )

    assert post_lane_dict["1pos1"]["wins"] == 1
    assert post_lane_dict["6pos1"]["wins"] == 0
    assert post_lane_dict["1pos1_vs_6pos1"]["wins"] == 1
    assert post_lane_dict["1pos1_with_2pos2"]["wins"] == 1
    assert post_lane_dict["1pos1,2pos2,3pos3"]["wins"] == 1


def test_post_lane_dict_requires_min_duration_and_minute_10_gate() -> None:
    for match in (
        _match(duration=19, minute_10_lead=0),
        _match(duration=25, minute_10_lead=2500),
    ):
        post_lane_dict = {}
        stats.analise_database(
            match,
            {},
            {},
            {},
            post_lane_dict=post_lane_dict,
        )
        assert post_lane_dict == {}


def test_lane_dict_ignores_imp_field() -> None:
    lane_dict = {}

    stats.analise_database(
        _match(duration=25, minute_10_lead=2500, imp=99),
        lane_dict,
        {},
        {},
    )

    assert lane_dict["3pos3"]["games"] == 1
    assert lane_dict["3pos3,4pos4_vs_6pos1,10pos5"]["games"] == 1


def test_early_filter_uses_networth_dominator_not_match_winner() -> None:
    match = _match(duration=35, radiant_win=False)
    match["radiantNetworthLeads"][19] = 6100

    ok, dominator = stats.is_early_match(match)

    assert ok is True
    assert dominator == "radiant"


def test_early_filter_uses_alchemist_leading_thresholds() -> None:
    match = _match(duration=35, radiant_win=True)
    match["players"][0]["heroId"] = stats.ALCHEMIST_HERO_ID
    match["radiantNetworthLeads"][23] = 7500

    ok, dominator = stats.is_early_match(match)

    assert ok is False
    assert dominator is None


def test_early_filter_uses_alchemist_trailing_thresholds() -> None:
    match = _match(duration=35, radiant_win=True)
    match["players"][5]["heroId"] = stats.ALCHEMIST_HERO_ID
    match["radiantNetworthLeads"][23] = 6600

    ok, dominator = stats.is_early_match(match)

    assert ok is True
    assert dominator == "radiant"


def test_late_filter_uses_wr60_abs_networth_gap_threshold() -> None:
    match = _match(duration=34, radiant_win=True)
    match["radiantNetworthLeads"][20] = 3500

    ok, winner = stats.is_late_match(match, if_check=True)

    assert ok is True
    assert winner == "radiant"


def test_late_filter_rejects_long_match_without_wr60_deficit() -> None:
    match = _match(duration=40, radiant_win=False)
    for idx in range(20, 40):
        match["radiantNetworthLeads"][idx] = 20000

    ok, winner = stats.is_late_match(match, if_check=True)

    assert ok is False
    assert winner is None
