import json

import numpy as np

from base.train_patch_hero_position_prior import (
    LATEST_PATCH_START,
    PatchData,
    abs_index_bucket,
    canonicalize_match,
    chronological_split_indices,
    evaluate_abs_index_buckets,
    fit_logistic,
    model_artifact,
    patch_for_timestamp,
    predict_from_artifact,
    run_analysis,
    scan_corpus,
    signed_design_matrix,
)


def _raw_match(map_id=1, start_time=LATEST_PATCH_START, radiant_win=True):
    players = []
    for is_radiant, offset in ((True, 0), (False, 5)):
        for position in range(1, 6):
            players.append(
                {
                    "position": f"POSITION_{position}",
                    "isRadiant": is_radiant,
                    "heroId": offset + position,
                }
            )
    return {
        "id": map_id,
        "startDateTime": start_time,
        "didRadiantWin": radiant_win,
        "players": players,
    }


def test_only_741d_timestamp_is_accepted():
    assert patch_for_timestamp(LATEST_PATCH_START - 1) is None
    assert patch_for_timestamp(LATEST_PATCH_START) == "7.41d"
    assert patch_for_timestamp(LATEST_PATCH_START + 100_000) == "7.41d"


def test_signed_position_encoding_negates_when_sides_swap():
    heroes = np.asarray([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]], dtype=np.uint16)
    swapped = np.asarray([[6, 7, 8, 9, 10, 1, 2, 3, 4, 5]], dtype=np.uint16)
    encoded = signed_design_matrix(heroes, 10, "hero_position").toarray()
    encoded_swapped = signed_design_matrix(swapped, 10, "hero_position").toarray()
    np.testing.assert_array_equal(encoded_swapped, -encoded)
    assert np.count_nonzero(encoded) == 10


def test_abs_index_bucket_boundaries_are_exact():
    assert abs_index_bucket(0.0) == 1
    assert abs_index_bucket(1.0) == 1
    assert abs_index_bucket(1.00001) == 2
    assert abs_index_bucket(2.0) == 2
    assert abs_index_bucket(2.01) == 3


def test_bucket_winner_uses_index_direction():
    # Radiant selected/won, then Dire selected/won, then both selected sides lose.
    y = np.asarray([1, 0, 0, 1], dtype=np.uint8)
    probability = np.asarray([0.51, 0.49, 0.51, 0.49])
    rows = evaluate_abs_index_buckets(y, probability)
    assert len(rows) == 1
    assert rows[0]["n"] == 4
    assert rows[0]["wins"] == 2
    assert rows[0]["win_rate"] == 0.5
    assert rows[0]["selected_radiant_n"] == 2
    assert rows[0]["selected_dire_n"] == 2


def test_scan_deduplicates_and_audits_invalid_rows(tmp_path):
    valid = _raw_match(map_id=101)
    duplicate = _raw_match(map_id=101, start_time=LATEST_PATCH_START + 1)
    invalid_positions = _raw_match(map_id=102)
    invalid_positions["players"][1]["position"] = "POSITION_1"
    pre_patch = _raw_match(map_id=103, start_time=LATEST_PATCH_START - 1)
    path = tmp_path / "7.41d_part001.json"
    path.write_text(
        json.dumps({"101a": valid, "101b": duplicate, "102": invalid_positions, "103": pre_patch}),
        encoding="utf-8",
    )

    data, audit = scan_corpus([path])

    assert len(data) == 1
    assert data.map_ids.tolist() == [101]
    assert audit["scanned"] == 4
    assert audit["accepted"] == 1
    assert audit["rejected"]["duplicate_map_id"] == 1
    assert audit["rejected"]["duplicate_position"] == 1
    assert audit["rejected"]["outside_7_41d"] == 1


def test_canonical_validation_rejects_duplicate_hero():
    raw = _raw_match()
    raw["players"][9]["heroId"] = raw["players"][0]["heroId"]
    match, reason = canonicalize_match(raw)
    assert match is None
    assert reason == "duplicate_hero"


def test_chronological_split_is_ordered_and_non_overlapping():
    data = PatchData(
        patch="7.41d",
        map_ids=np.asarray([14, 12, 11, 13, 10], dtype=np.int64),
        start_times=np.asarray([4, 2, 1, 3, 1], dtype=np.int64) + LATEST_PATCH_START,
        outcomes=np.asarray([0, 1, 0, 1, 0], dtype=np.uint8),
        hero_ids=np.tile(np.arange(1, 11, dtype=np.uint16), (5, 1)),
    )
    split = chronological_split_indices(data)
    assert split["train"].tolist() == [4, 2, 1]
    assert split["validation"].tolist() == [3]
    assert split["test"].tolist() == [0]
    all_indices = np.concatenate(tuple(split.values()))
    assert len(set(all_indices.tolist())) == len(data)
    assert data.start_times[split["train"]].max() <= data.start_times[split["validation"]].min()
    assert data.start_times[split["validation"]].max() <= data.start_times[split["test"]].min()


def test_bucket_counts_sum_to_test_size():
    y = np.asarray([0, 1, 0, 1, 1, 0], dtype=np.uint8)
    probability = np.asarray([0.50, 0.51, 0.4899, 0.52, 0.4699, 0.90])
    rows = evaluate_abs_index_buckets(y, probability)
    assert sum(row["n"] for row in rows) == len(y)
    assert sum(row["wins"] + row["losses"] for row in rows) == len(y)


def test_portable_artifact_inference_matches_sklearn():
    hero_ids = np.asarray(
        [
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            [6, 7, 8, 9, 10, 1, 2, 3, 4, 5],
            [1, 7, 3, 9, 5, 6, 2, 8, 4, 10],
            [6, 2, 8, 4, 10, 1, 7, 3, 9, 5],
        ],
        dtype=np.uint16,
    )
    y = np.asarray([1, 0, 1, 0], dtype=np.uint8)
    X = signed_design_matrix(hero_ids, 10, "hero_position")
    model = fit_logistic(X, y, 0.1)
    artifact = model_artifact(
        model,
        patch="7.41d",
        kind="hero_position",
        max_hero_id=10,
        selected_c=0.1,
        training_n=4,
        training_time_min=LATEST_PATCH_START,
        training_time_max=LATEST_PATCH_START + 3,
    )
    expected = model.predict_proba(X)[:, 1]
    actual = np.asarray([predict_from_artifact(artifact, row.tolist()) for row in hero_ids])
    np.testing.assert_allclose(actual, expected, rtol=0, atol=1e-12)


def test_end_to_end_writes_single_patch_outputs(tmp_path):
    source = tmp_path / "7.41d_part001.json"
    payload = {
        str(map_id): _raw_match(
            map_id=map_id,
            start_time=LATEST_PATCH_START + map_id,
            radiant_win=bool(map_id % 2),
        )
        for map_id in range(1, 31)
    }
    source.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "output"
    artifact_path = output_dir / "prior.json"

    report = run_analysis([source], output_dir, artifact_path, (0.1,))

    assert report["protocol"]["patch"] == "7.41d"
    assert report["n"] == 30
    assert report["models"]["hero_position"]["test"]["metrics"]["n"] == 6
    assert report["models"]["hero_only"]["test"]["metrics"]["n"] == 6
    assert artifact_path.exists()
    assert (output_dir / "report.json").exists()
    assert (output_dir / "report.md").exists()
    assert (output_dir / "corpus_audit.json").exists()
    assert (output_dir / "oot_predictions.csv.gz").exists()
