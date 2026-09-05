import joblib
import numpy as np
import pytest

from base.draft_features import KIND_PAIR, KIND_POSITION_PAIR, DraftFeatureEncoder


ROW = np.arange(1, 11, dtype=np.int64)


def swap_sides(row: np.ndarray) -> np.ndarray:
    return np.concatenate((row[5:], row[:5]))


def rotate_positions(row: np.ndarray) -> np.ndarray:
    return np.concatenate((np.roll(row[:5], 1), np.roll(row[5:], 1)))


def generic_pair_end(encoder: DraftFeatureEncoder) -> int:
    return 6 * encoder.n_heroes + int(encoder.synergy_lut.max()) + 1 + int(encoder.counter_lut.max()) + 1


def test_position_pairs_distinguish_role_arrangements_of_the_same_hero_pairs():
    changed_positions = ROW.copy()
    changed_positions[[0, 1]] = changed_positions[[1, 0]]
    encoder = DraftFeatureEncoder.fit(
        np.stack((ROW, changed_positions)), KIND_POSITION_PAIR, signed=False, pair_min_support=1
    )

    matrix = encoder.transform(np.stack((ROW, changed_positions))).toarray()
    pair_start, pair_end = 6 * encoder.n_heroes, generic_pair_end(encoder)
    assert np.array_equal(matrix[0, pair_start:pair_end], matrix[1, pair_start:pair_end])
    assert not np.array_equal(matrix[0, pair_end:], matrix[1, pair_end:])


@pytest.mark.parametrize("signed", (True, False))
def test_position_pair_side_swap_has_the_same_signed_contract(signed):
    encoder = DraftFeatureEncoder.fit(np.stack((ROW, swap_sides(ROW))), KIND_POSITION_PAIR, signed=signed, pair_min_support=1)
    before, after = encoder.transform(np.stack((ROW, swap_sides(ROW)))).toarray()
    expected = -before if signed else before
    assert np.array_equal(after, expected)


def test_unknown_heroes_are_zero_in_every_role_and_pair_block():
    encoder = DraftFeatureEncoder.fit(np.stack((ROW, ROW)), KIND_POSITION_PAIR, signed=True, pair_min_support=1)
    unknown_at_every_role = np.full((1, 10), 99_999, dtype=np.int64)
    assert not encoder.transform(unknown_at_every_role).toarray().any()


def test_joblib_round_trip_handles_legacy_and_new_encoders(tmp_path):
    old = DraftFeatureEncoder.fit(np.stack((ROW, ROW)), KIND_PAIR, signed=True, pair_min_support=1)
    del old.__dict__["position_synergy_lut"]
    del old.__dict__["position_counter_lut"]
    old_path = tmp_path / "old.joblib"
    joblib.dump(old, old_path)
    restored_old = joblib.load(old_path)
    assert np.array_equal(restored_old.transform(ROW[None]).toarray(), old.transform(ROW[None]).toarray())

    new = DraftFeatureEncoder.fit(np.stack((ROW, ROW)), KIND_POSITION_PAIR, signed=True, pair_min_support=1)
    new_path = tmp_path / "new.joblib"
    joblib.dump(new, new_path)
    restored_new = joblib.load(new_path)
    assert np.array_equal(restored_new.transform(ROW[None]).toarray(), new.transform(ROW[None]).toarray())


def test_position_pair_support_is_fit_on_train_rows_only():
    held_out = rotate_positions(ROW)
    encoder = DraftFeatureEncoder.fit(np.stack((ROW, ROW)), KIND_POSITION_PAIR, signed=False, pair_min_support=2)
    position_only = encoder.transform(held_out[None]).toarray()[0, generic_pair_end(encoder):]
    assert not position_only.any()


def test_empty_position_pair_support_falls_back_to_generic_pairs():
    train = np.stack([np.concatenate((np.roll(ROW[:5], i), np.roll(ROW[5:], i))) for i in range(5)])
    generic = DraftFeatureEncoder.fit(train, KIND_PAIR, signed=True, pair_min_support=2)
    positioned = DraftFeatureEncoder.fit(train, KIND_POSITION_PAIR, signed=True, pair_min_support=2)

    assert not (positioned.position_synergy_lut >= 0).any()
    assert not (positioned.position_counter_lut >= 0).any()
    assert positioned.n_columns == generic.n_columns
    assert positioned.transform(train[:1]).shape[1] == generic.n_columns


@pytest.mark.parametrize("heroes", (np.ones((1, 9), dtype=np.int64), np.array([[1.5] * 10]), np.array([[-1] * 10])))
def test_fit_rejects_malformed_hero_ids(heroes):
    with pytest.raises(ValueError):
        DraftFeatureEncoder.fit(heroes, KIND_POSITION_PAIR, signed=True)

