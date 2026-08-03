import json

import pytest

from base import analise_database as production_stats
from base.evaluate_post_lane_solo_gate import CorpusScanError, Prediction, paired_statistics
from base.evaluate_solo_phase_filters import (
    EARLY_WEIGHTS,
    LATE_WEIGHTS,
    PATCH_START,
    build_table,
    canonicalize_match,
    chronological_group_split,
    early_label,
    late_label,
    run_analysis,
    scan_corpus,
    score,
)


def _raw_match(
    map_id=1,
    *,
    timestamp=None,
    duration=34,
    radiant_win=True,
    gate_lead=0,
    hero_ids=None,
):
    hero_ids = list(hero_ids or range(1, 11))
    players = []
    for offset, hero_id in enumerate(hero_ids):
        players.append({
            "position": f"POSITION_{offset % 5 + 1}",
            "isRadiant": offset < 5,
            "heroId": hero_id,
        })
    leads = [0] * duration
    if duration > 9:
        leads[9] = gate_lead
    return {
        "id": map_id,
        "startDateTime": PATCH_START + map_id if timestamp is None else timestamp,
        "didRadiantWin": radiant_win,
        "radiantNetworthLeads": leads,
        "players": players,
    }


def _row(*args, **kwargs):
    raw = _raw_match(*args, **kwargs)
    row, reason = canonicalize_match(raw)
    assert reason is None
    assert row is not None
    return row


def _production_early_label(raw):
    eligible, dominator = production_stats.is_early_match(raw)
    if not eligible:
        return None
    return dominator == "radiant"


def _production_late_label(raw):
    eligible, winner = production_stats.is_late_match(raw, if_check=True)
    if not eligible:
        return None
    return winner == "radiant"


def test_early_current_matches_production_fast_long_and_alchemist(monkeypatch):
    thresholds = {
        "no_alchemist": {minute: 6000 for minute in range(20, 29)},
        "alchemist_leading": {minute: 7000 for minute in range(20, 29)},
        "alchemist_trailing": {minute: 5000 for minute in range(20, 29)},
    }
    monkeypatch.setattr(production_stats, "_load_early_dominator_thresholds", lambda: thresholds)

    fast = _raw_match(1, duration=34, radiant_win=False)
    long_plain = _raw_match(2, duration=35, radiant_win=False, gate_lead=0)
    long_plain["radiantNetworthLeads"][19] = 6000
    # Alchemist is on the leading Radiant side, so the higher 7000 group applies.
    alchemist = _raw_match(3, duration=35, hero_ids=[73, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    alchemist["radiantNetworthLeads"][19] = 7000

    for raw in (fast, long_plain, alchemist):
        row, _ = canonicalize_match(raw)
        assert row is not None
        assert early_label(row, remove_gate=False) == _production_early_label(raw)


def test_early_no10_removes_only_long_map_gate(monkeypatch):
    thresholds = {group: {minute: 6000 for minute in range(20, 29)} for group in (
        "no_alchemist", "alchemist_leading", "alchemist_trailing"
    )}
    monkeypatch.setattr(production_stats, "_load_early_dominator_thresholds", lambda: thresholds)
    blowout = _raw_match(1, duration=35, gate_lead=3000)
    blowout["radiantNetworthLeads"][19] = -6000
    missing = _raw_match(2, duration=35, gate_lead=None)
    missing["radiantNetworthLeads"][19] = 6000
    no_threshold = _raw_match(3, duration=35, gate_lead=3000)
    fast = _raw_match(4, duration=34, radiant_win=False, gate_lead=9000)

    blowout_row, _ = canonicalize_match(blowout)
    missing_row, _ = canonicalize_match(missing)
    no_threshold_row, _ = canonicalize_match(no_threshold)
    fast_row, _ = canonicalize_match(fast)
    assert early_label(blowout_row, False) is None
    assert early_label(blowout_row, True) is False
    assert early_label(missing_row, False) is None
    assert early_label(missing_row, True) is True
    assert early_label(no_threshold_row, True) is None
    assert early_label(fast_row, False) is False
    assert early_label(fast_row, True) is False


def test_late_current_parity_and_all34_isolated_change(monkeypatch):
    monkeypatch.setattr(production_stats, "_load_late_wr60_thresholds", lambda: {28: 1000.0, 34: 2000.0})
    close = _raw_match(1, duration=34, radiant_win=False)
    close["radiantNetworthLeads"][27] = 999
    far = _raw_match(2, duration=34, radiant_win=True)
    far["radiantNetworthLeads"] = [5000] * 34
    short = _raw_match(3, duration=33)
    for raw in (close, far, short):
        row, _ = canonicalize_match(raw)
        assert row is not None
        assert late_label(row, False) == _production_late_label(raw)
    far_row, _ = canonicalize_match(far)
    short_row, _ = canonicalize_match(short)
    assert late_label(far_row, False) is None
    assert late_label(far_row, True) is True
    assert late_label(short_row, True) is None


@pytest.mark.parametrize(
    "weights,expected",
    [
        (EARLY_WEIGHTS, ((1.4 * .60 + 1.6 * .55 + 1.4 * .50 + 1.2 * .45 + .8 * .40) / 6.4 - .50) * 100),
        (LATE_WEIGHTS, ((2.4 * .60 + 2.2 * .55 + 1.4 * .50 + 1.2 * .45 + .6 * .40) / 7.8 - .50) * 100),
    ],
)
def test_exact_phase_weights_formula_and_rounding(weights, expected):
    row = _row(radiant_win=False)
    rates = (.60, .55, .50, .45, .40)
    table = {}
    for offset, hero_id in enumerate(row.hero_ids):
        rate = rates[offset] if offset < 5 else .50
        table[(hero_id, offset % 5 + 1)] = (int(rate * 100), 100)
    prediction = score(row, row.radiant_win, table, weights)
    assert prediction.raw_diff_pp == pytest.approx(expected)
    assert prediction.index == int(round(expected))
    assert prediction.selected_win is False


def test_min50_and_no_train_test_leakage():
    train = [_row(map_id=index + 1, radiant_win=bool(index % 2)) for index in range(50)]
    table = build_table(train, lambda row: row.radiant_win)
    heldout = _row(map_id=1000, hero_ids=range(11, 21))
    assert score(train[0], True, table, EARLY_WEIGHTS).covered is True
    assert score(heldout, True, table, EARLY_WEIGHTS).covered is False


def test_group_split_ties_via_end_to_end_and_dedupe_rejections(tmp_path):
    valid = _raw_match(1, timestamp=PATCH_START + 1)
    duplicate = _raw_match(1, timestamp=PATCH_START + 1)
    pro = _raw_match(2, timestamp=PATCH_START + 2)
    pro["leagueId"] = 12
    invalid = _raw_match(3, timestamp=PATCH_START + 3)
    invalid["players"][1]["position"] = "POSITION_1"
    first = tmp_path / "7.41d_part001.json"
    second = tmp_path / "7.41d_part002.json"
    first.write_text(json.dumps({"1": valid, "2": pro, "3": invalid}), encoding="utf-8")
    second.write_text(json.dumps({"1": duplicate}), encoding="utf-8")
    rows, audit = scan_corpus([first, second])
    assert [row.map_id for row in rows] == [1]
    assert audit["rejected"]["duplicate_map_id"] == 1
    assert audit["rejected"]["pro_match"] == 1
    assert audit["rejected"]["duplicate_position"] == 1


def test_group_split_never_splits_equal_timestamps():
    times = [10, 10, 20, 20, 20, 20, 20, 30, 40, 40]
    rows = [_row(map_id=index + 1, timestamp=PATCH_START + timestamp) for index, timestamp in enumerate(times)]
    train, test = chronological_group_split(rows)
    assert len(train) == 8
    assert len(test) == 2
    assert train[-1].start_time < test[0].start_time
    assert {row.start_time for row in train}.isdisjoint({row.start_time for row in test})


def test_malformed_shard_fails_closed_without_outputs(tmp_path):
    source = tmp_path / "7.41d_part001.json"
    source.write_text('{"1":' + json.dumps(_raw_match(1)) + ',"2":', encoding="utf-8")
    output = tmp_path / "output"
    with pytest.raises(CorpusScanError) as error:
        run_analysis([source], output)
    assert error.value.scanned_in_shard == 1
    assert not output.exists()


def test_paired_statistics_in_phase_contract():
    current = [
        Prediction(True, 2.0, 2, True, True),
        Prediction(True, -2.0, -2, False, False),
        Prediction(True, 0.2, 0, None, None),
    ]
    alternative = [
        Prediction(True, -3.0, -3, False, False),
        Prediction(True, -4.0, -4, False, True),
        Prediction(True, 2.0, 2, True, True),
    ]
    result = paired_statistics(current, alternative)
    assert result["n"] == 2
    assert result["discordant"]["current_win_no_gate_loss"] == 1
    assert result["discordant"]["current_loss_no_gate_win"] == 1
    assert result["mcnemar_exact_binom_p"] == pytest.approx(1.0)
    assert result["sign_changes_n"] == 1


def test_end_to_end_outputs_and_strict_group_split(tmp_path, monkeypatch):
    monkeypatch.setattr(production_stats, "_load_early_dominator_thresholds", lambda: {
        group: {minute: 6000 for minute in range(20, 29)}
        for group in ("no_alchemist", "alchemist_leading", "alchemist_trailing")
    })
    monkeypatch.setattr(production_stats, "_load_late_wr60_thresholds", lambda: {28: 1000.0})
    payload = {
        str(map_id): _raw_match(
            map_id,
            timestamp=PATCH_START + map_id,
            duration=34,
            radiant_win=bool(map_id % 2),
        )
        for map_id in range(1, 101)
    }
    source = tmp_path / "7.41d_part001.json"
    source.write_text(json.dumps(payload), encoding="utf-8")
    output = tmp_path / "output"
    report = run_analysis([source], output)
    assert report["split"]["train_n"] == 80
    assert report["split"]["test_n"] == 20
    assert report["split"]["train_time_max"] < report["split"]["test_time_min"]
    assert report["phases"]["early"]["primary_pool_n"] == 20
    assert report["phases"]["late"]["primary_pool_n"] == 20
    assert "delta_wr_alternative_minus_current" in report["phases"]["early"]["paired_common_nonzero"]
    for name in ("report.json", "report.md", "predictions.csv.gz", "corpus_audit.json"):
        assert (output / name).exists()
