"""Regression cases from actual live panel journal timing and row identities."""
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evaluate_kills_replay import compare, load_candidate, select_forecasts


def candidate():
    return dict(mids=np.array([101, 102, 103]), starts=np.array([1000, 2000, 3000]),
                ends=np.array([1800, 2800, 3800]), kills=np.array([[30, 24], [30, 30], [20, 20]]),
                total=np.array([.3, .7, .2]), radiant=np.array([.6, .6, .2]))


def forecast(mid, ts, p=.7):
    return dict(map_id=str(mid), ts=ts, models=[
        dict(key="total_55_50", p=p, fill=1, missing=[]),
        dict(key="rad_30_25", p=p, fill=1, missing=[])])


def test_first_forecast_dedup_ignores_late_repetition_and_file_order():
    rows = [forecast(101, 1500, .9), forecast(101, 1100, .6), forecast(101, 1100, .6),
            forecast(101, 1900, .1), forecast(102, 2000, .4)]
    picked, _ = select_forecasts(rows, candidate(), prematch=False)
    reverse, _ = select_forecasts(rows[::-1], candidate(), prematch=False)
    assert picked == reverse
    assert [(x["mid"], x["ts"]) for x in picked] == [(101, 1100), (102, 2000)]
    assert picked[0]["p"] == (.6, .6)


def test_live_records_never_become_prematch_and_finished_map_is_excluded():
    rows = [forecast(101, 1100), forecast(102, 2800)]
    assert compare(rows, candidate(), prematch=True)["status"] == "NO_ELIGIBLE_FORECASTS"
    picked, _ = select_forecasts(rows, candidate(), prematch=False)
    assert [x["mid"] for x in picked] == [101]


def test_conflicting_duplicate_timestamp_fails():
    with pytest.raises(ValueError, match="Conflicting"):
        select_forecasts([forecast(101, 1100, .6), forecast(101, 1100, .8)], candidate(), False)


def test_invalid_records_and_missing_target_do_not_select_a_map():
    incomplete = forecast(102, 1900)
    incomplete["models"].pop()
    rows = [forecast("", 900), forecast(101.5, 900), forecast(101, 900, np.nan),
            forecast(101, 900, 1.1), forecast(999, 900), incomplete]
    picked, counts = select_forecasts(rows, candidate(), True)
    assert picked == []
    assert counts["invalid_probability"] == 2
    assert counts["invalid_id_or_time"] == 2


def test_middle_band_retained_for_full_target_and_excluded_only_for_legacy():
    data = candidate()
    data["kills"][0] = [26, 28]
    report = compare([forecast(101, 900), forecast(102, 1900), forecast(103, 2900)], data, True)
    assert report["targets"]["total"]["full_threshold"]["panel"]["n"] == 3
    assert report["targets"]["total"]["legacy_excluded_middle"]["panel"]["n"] == 2
    for target in ("total", "radiant"):
        assert report["targets"][target]["full_threshold"]["panel"]["prevalence"] == 1/3
        censored = report["targets"][target]["legacy_excluded_middle"]
        assert censored["status"] == "NOT_COMPARABLE_PROBABILITY_TARGETS"
        assert "candidate_minus_panel" not in censored
        assert "candidate" not in censored


def test_candidate_loader_rejects_swapped_side_order(tmp_path):
    import json
    d = candidate()
    stats = np.zeros((3, 10, 6))
    stats[:, :5, 0] = d["kills"][:, 0, None] / 5
    stats[:, 5:, 0] = d["kills"][:, 1, None] / 5
    np.savez(tmp_path / "pro_inputs.npz", mids=d["mids"], ts=d["starts"], ends=d["ends"],
             split=np.full(3, 2), stats=stats)
    for target in ("total", "side"):
        (tmp_path / f"{target}_selection.json").write_text(json.dumps({"chosen": "absolute"}))
    np.savez(tmp_path / "total_test_predictions.npz", mids=d["mids"], y=[0, 1, 0], absolute=d["total"])
    np.savez(tmp_path / "side_test_predictions.npz", mids=np.tile(d["mids"], 2),
             y=[0, 1, 0, 1, 1, 0], absolute=np.tile(d["radiant"], 2))
    with pytest.raises(ValueError, match="orientation or labels"):
        load_candidate(tmp_path)
