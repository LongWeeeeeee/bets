"""Dual early dictionaries: NW dominator vs map-winner labels."""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import analise_database as ad  # noqa: E402
import explore_database as explore  # noqa: E402


def _players():
    return [
        {"heroId": 1, "position": "POSITION_1", "isRadiant": True, "imp": 0},
        {"heroId": 2, "position": "POSITION_2", "isRadiant": True, "imp": 0},
        {"heroId": 3, "position": "POSITION_3", "isRadiant": True, "imp": 0},
        {"heroId": 4, "position": "POSITION_4", "isRadiant": True, "imp": 0},
        {"heroId": 5, "position": "POSITION_5", "isRadiant": True, "imp": 0},
        {"heroId": 6, "position": "POSITION_1", "isRadiant": False, "imp": 0},
        {"heroId": 7, "position": "POSITION_2", "isRadiant": False, "imp": 0},
        {"heroId": 8, "position": "POSITION_3", "isRadiant": False, "imp": 0},
        {"heroId": 9, "position": "POSITION_4", "isRadiant": False, "imp": 0},
        {"heroId": 10, "position": "POSITION_5", "isRadiant": False, "imp": 0},
    ]


def _fast_match(*, did_radiant_win: bool) -> dict:
    # duration <= 34 => is_early_match uses winner as dominator too
    leads = [100] * 30
    return {
        "id": "dual-early-1",
        "players": _players(),
        "radiantNetworthLeads": leads,
        "didRadiantWin": did_radiant_win,
        "topLaneOutcome": "RADIANT_WIN",
        "midLaneOutcome": "RADIANT_WIN",
        "bottomLaneOutcome": "DIRE_WIN",
    }


def _long_diverging_match() -> dict:
    """Early-eligible long map where NW dominator != map winner.

    Gate at minute 10 (index 9) must be within EARLY_GATE_MAX_ABS_LEAD.
    Dominator: first side to hit comeback NW threshold in minutes 20-28.
    Radiant spikes NW (dominator=radiant) but Dire wins the map.
    """
    leads = [0] * 45
    leads[9] = 500  # gate minute 10
    for m in range(19, 28):  # minutes 20-28
        leads[m] = 15000  # above no_alchemist thresholds
    for m in range(28, 45):
        leads[m] = -3000
    return {
        "id": "dual-early-2",
        "players": _players(),
        "radiantNetworthLeads": leads,
        "didRadiantWin": False,  # map winner = dire
        "topLaneOutcome": "RADIANT_WIN",
        "midLaneOutcome": "RADIANT_WIN",
        "bottomLaneOutcome": "DIRE_WIN",
    }


def test_explore_metrics_include_early_end():
    assert "early" in explore.ALL_METRICS
    assert "early_end" in explore.ALL_METRICS
    assert explore.OUTPUTS_BY_METRIC["early"] == "early_dict_raw.json"
    assert explore.OUTPUTS_BY_METRIC["early_end"] == "early_end_dict_raw.json"


def test_new_metric_dicts_allocates_both_early_variants(monkeypatch):
    monkeypatch.setenv("EXPLORE_METRICS", "early,early_end")
    names = explore._enabled_metrics()
    assert names == ("early", "early_end")
    lane, early, early_end, late, post, kills = explore._new_metric_dicts(names)
    assert lane is None
    assert isinstance(early, dict) and early == {}
    assert isinstance(early_end, dict) and early_end == {}
    assert late is None and post is None and kills is None


def test_analise_writes_both_early_labels_on_diverging_match():
    match = _long_diverging_match()
    ok, dominator = ad.is_early_match(match)
    assert ok is True
    assert dominator == "radiant"

    early_nw: dict = {}
    early_end: dict = {}
    updated = ad.analise_database(
        match,
        lane_dict=None,
        early_dict=early_nw,
        late_dict=None,
        early_end_dict=early_end,
        exclude_pro_matches=False,
    )
    assert updated is True
    assert early_nw
    assert early_end

    # solo pos1 radiant hero
    r_key = "1pos1"
    d_key = "6pos1"
    assert r_key in early_nw and d_key in early_nw
    assert r_key in early_end and d_key in early_end

    # NW label: radiant dominator => radiant wins=1, dire wins=0
    assert early_nw[r_key]["wins"] == 1
    assert early_nw[r_key]["games"] == 1
    assert early_nw[d_key]["wins"] == 0
    assert early_nw[d_key]["games"] == 1

    # End label: map winner dire => radiant wins=0, dire wins=1
    assert early_end[r_key]["wins"] == 0
    assert early_end[r_key]["games"] == 1
    assert early_end[d_key]["wins"] == 1
    assert early_end[d_key]["games"] == 1


def test_analise_fast_map_without_early_lead_goes_only_to_early_end():
    """Быстрая карта без раннего перевеса идёт ТОЛЬКО в early_end.

    До 09.08 обе популяции совпадали, и такая карта попадала и в early_dict —
    с меткой победителя, то есть чужой функцией. Замер E-60 показал, что этим
    размечены 92% карт словаря и стоит это 4-8 п.п. на независимой цели, поэтому
    популяции разделены: early_dict берёт карту, только если ранний перевес
    реально сложился по маркеру.
    """
    match = _fast_match(did_radiant_win=True)
    early_nw: dict = {}
    early_end: dict = {}
    updated = ad.analise_database(
        match,
        lane_dict=None,
        early_dict=early_nw,
        late_dict=None,
        early_end_dict=early_end,
        exclude_pro_matches=False,
    )
    assert updated is True
    r_key = "1pos1"
    assert early_nw == {}
    assert early_end[r_key]["wins"] == 1
    assert early_end[r_key]["games"] == 1
