import json
import math

import pytest

from base.evaluate_post_lane_solo_gate import (
    CorpusScanError,
    GATE_ABS_LEAD,
    PATCH_START,
    POSITION_WEIGHTS,
    MatchRow,
    Prediction,
    build_solo_table,
    canonicalize_match,
    chronological_group_split,
    included_in_training,
    paired_statistics,
    raw_abs_bucket,
    raw_buckets,
    rounded_buckets,
    run_analysis,
    scan_corpus,
    score_draft,
)


def _raw_match(map_id=1, timestamp=None, radiant_win=True, gate_lead=0.0, duration=20):
    players = []
    for is_radiant, offset in ((True, 0), (False, 5)):
        for position in range(1, 6):
            players.append({
                "position": f"POSITION_{position}",
                "isRadiant": is_radiant,
                "heroId": offset + position,
            })
    leads = [0.0] * duration
    if duration > 9:
        leads[9] = gate_lead
    return {
        "id": map_id,
        "startDateTime": PATCH_START + map_id if timestamp is None else timestamp,
        "didRadiantWin": radiant_win,
        "radiantNetworthLeads": leads,
        "players": players,
    }


def _row(map_id=1, timestamp=None, radiant_win=True, gate_lead=0.0, duration=20):
    raw = _raw_match(map_id, timestamp, radiant_win, gate_lead, duration)
    row, reason = canonicalize_match(raw)
    assert reason is None
    assert row is not None
    return row


def test_training_gate_inclusion_current_vs_no_gate():
    boundary = _row(gate_lead=GATE_ABS_LEAD)
    blowout = _row(map_id=2, gate_lead=GATE_ABS_LEAD + 0.01)
    missing = _row(map_id=3, gate_lead=None)
    short = _row(map_id=4, duration=19)

    assert included_in_training(boundary, "current") is True
    assert included_in_training(boundary, "no_gate") is True
    assert included_in_training(blowout, "current") is False
    assert included_in_training(blowout, "no_gate") is True
    assert included_in_training(missing, "current") is False
    assert included_in_training(missing, "no_gate") is True
    assert included_in_training(short, "current") is False
    assert included_in_training(short, "no_gate") is False


def test_exact_weighted_formula_and_production_rounding():
    row = _row(radiant_win=False)
    radiant_rates = (0.60, 0.55, 0.50, 0.45, 0.40)
    table = {}
    for offset, hero_id in enumerate(row.hero_ids):
        rate = radiant_rates[offset] if offset < 5 else 0.50
        table[(hero_id, offset % 5 + 1)] = (int(rate * 100), 100)

    prediction = score_draft(row, table)

    expected_radiant = sum(
        POSITION_WEIGHTS[position] * radiant_rates[position - 1] for position in POSITION_WEIGHTS
    ) / sum(POSITION_WEIGHTS.values())
    expected_raw = (expected_radiant - 0.50) * 100.0
    assert prediction.covered is True
    assert prediction.raw_diff_pp == pytest.approx(expected_raw)
    assert prediction.index == int(round(expected_raw)) == 3
    assert prediction.selected_radiant is True
    assert prediction.selected_win is False


def test_all_ten_cells_must_have_50_games():
    row = _row()
    table = {(hero_id, offset % 5 + 1): (30, 50) for offset, hero_id in enumerate(row.hero_ids)}
    assert score_draft(row, table).covered is True
    table[(row.hero_ids[9], 5)] = (29, 49)
    assert score_draft(row, table).covered is False


def test_raw_and_rounded_bucket_boundaries_direction_and_zero():
    assert raw_abs_bucket(0.0) == 1
    assert raw_abs_bucket(1.0) == 1
    assert raw_abs_bucket(1.00001) == 2
    predictions = [
        Prediction(True, 0.49, 0, None, None),
        Prediction(True, 1.0, 1, True, True),
        Prediction(True, -1.01, -1, False, False),
        Prediction(False, None, None, None, None),
    ]
    raw = raw_buckets(predictions)
    assert [row["n"] for row in raw] == [2, 1]
    assert raw[0]["abstain_n"] == 1
    assert raw[0]["wins"] == 1
    rounded = rounded_buckets(predictions)
    assert [row["abs_index"] for row in rounded] == [0, 1]
    assert rounded[0]["abstain_n"] == 1
    assert rounded[1]["selected_radiant_n"] == 1
    assert rounded[1]["selected_dire_n"] == 1


def test_group_aware_split_does_not_split_ties_and_is_nearest_80_percent():
    # Counts by timestamp are 2, 5, 1, 2. Nominal cut 8 lands exactly after
    # the third timestamp; the second timestamp is never split.
    times = [10, 10, 20, 20, 20, 20, 20, 30, 40, 40]
    rows = [_row(map_id=index + 1, timestamp=PATCH_START + timestamp) for index, timestamp in enumerate(times)]
    train, test = chronological_group_split(rows)
    assert len(train) == 8
    assert len(test) == 2
    assert train[-1].start_time < test[0].start_time
    assert {row.start_time for row in train}.isdisjoint({row.start_time for row in test})


def test_group_split_requires_two_unique_timestamps():
    rows = [_row(map_id=index + 1, timestamp=PATCH_START) for index in range(3)]
    with pytest.raises(ValueError, match="two unique timestamp"):
        chronological_group_split(rows)


def test_scan_dedupes_and_rejects_pro_and_invalid(tmp_path):
    valid = _raw_match(101)
    duplicate = _raw_match(101)
    league = _raw_match(102)
    league["leagueId"] = 99
    teams = _raw_match(103)
    teams["radiantTeam"] = {"id": 1}
    teams["direTeam"] = {"id": 2}
    invalid = _raw_match(104)
    invalid["players"][1]["position"] = "POSITION_1"
    first = tmp_path / "7.41d_part001.json"
    second = tmp_path / "7.41d_part002.json"
    first.write_text(json.dumps({"101": valid, "102": league, "103": teams, "104": invalid}), encoding="utf-8")
    second.write_text(json.dumps({"101": duplicate}), encoding="utf-8")

    rows, audit = scan_corpus([first, second])

    assert [row.map_id for row in rows] == [101]
    assert audit["rejected"]["duplicate_map_id"] == 1
    assert audit["rejected"]["pro_match"] == 2
    assert audit["rejected"]["duplicate_position"] == 1


def test_malformed_shard_fails_closed_and_writes_no_outputs(tmp_path):
    source = tmp_path / "7.41d_part001.json"
    valid_json = json.dumps(_raw_match(1))
    source.write_text('{"1":' + valid_json + ',"2":', encoding="utf-8")
    output = tmp_path / "output"

    with pytest.raises(CorpusScanError) as error:
        run_analysis([source], output)

    assert error.value.path == source
    assert error.value.scanned_in_shard == 1
    assert not output.exists()


def test_paired_statistics_delta_discordance_mcnemar_and_changes():
    current = [
        Prediction(True, 2.0, 2, True, True),
        Prediction(True, -2.0, -2, False, False),
        Prediction(True, 3.0, 3, True, True),
        Prediction(True, 0.2, 0, None, None),
    ]
    no_gate = [
        Prediction(True, -3.0, -3, False, False),
        Prediction(True, -4.0, -4, False, True),
        Prediction(True, 3.0, 3, True, True),
        Prediction(True, 2.0, 2, True, True),
    ]

    result = paired_statistics(current, no_gate)

    assert result["n"] == 3
    assert result["current_wr"] == pytest.approx(2 / 3)
    assert result["no_gate_wr"] == pytest.approx(2 / 3)
    assert result["delta_wr_no_gate_minus_current"] == pytest.approx(0.0)
    assert result["discordant"]["current_win_no_gate_loss"] == 1
    assert result["discordant"]["current_loss_no_gate_win"] == 1
    assert result["mcnemar_exact_binom_p"] == pytest.approx(1.0)
    assert result["sign_changes_n"] == 1
    assert result["abs_index_changes"]["changed_n"] == 2
    assert math.isfinite(result["paired_normal_95ci"]["low"])


def test_end_to_end_outputs(tmp_path):
    payload = {}
    for map_id in range(1, 101):
        # 80 train rows give every exact hero-position cell >=50 observations.
        payload[str(map_id)] = _raw_match(
            map_id,
            timestamp=PATCH_START + map_id,
            radiant_win=bool(map_id % 2),
            gate_lead=0,
        )
    source = tmp_path / "7.41d_part001.json"
    source.write_text(json.dumps(payload), encoding="utf-8")
    output = tmp_path / "output"

    report = run_analysis([source], output)

    assert report["split"]["train_n"] == 80
    assert report["split"]["test_n"] == 20
    assert report["variants"]["current"]["summary"]["coverage_n"] == 20
    assert report["variants"]["no_gate"]["summary"]["coverage_n"] == 20
    assert report["diagnostic_strata"]["close10"]["n"] == 20
    assert (output / "report.json").exists()
    assert (output / "report.md").exists()
    assert (output / "corpus_audit.json").exists()
    assert (output / "predictions.csv.gz").exists()
