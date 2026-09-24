"""Frozen history and live query parity for kills-v3 T+P."""
import json
import numpy as np
import pytest

from base import kills_v3_serving as serving
from base.tools import kills_v3_research as v3
from base.tests.test_kills_v3_research import toy_rich


def test_frozen_state_matches_builder_and_roundtrip(tmp_path):
    start = v3.QUERY_START + v3.DAY
    cutoff = start - 7000
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 20000, start - 7200, start - 3000, start, start + 4000],
             [1800] * 5, [(10, 15), (40, 15), (30, 5), (100, 15), (25, 10)],
             teams=[[1, 2], [1, 2], [1, 2], [1, 2], [2, 1]])
    meta = v3.build_dataset(rich, [], tmp_path / "dataset", pseudo_games=0,
                            history_cutoff=cutoff)
    state = serving.build_state(rich, [], history_start=v3.HISTORY_START,
                                cutoff=cutoff, pseudo_games=0)
    assert state.serving_meta["events_replayed"] == 1  # second map ends after cutoff
    names = meta["blocks"]["T"] + meta["blocks"]["P"]
    with np.load(tmp_path / "dataset/dataset.npz") as ds:
        for row, mid in enumerate(ds["mids"]):
            if int(ds["starts"][row]) <= cutoff:
                continue
            teams = ds["team_ids"][row]
            accounts = list(range(1, 6)), list(range(6, 11))
            got_names, got = serving.features_for_map(state, int(teams[0]), int(teams[1]),
                                                        *accounts, int(ds["starts"][row]))
            assert got_names == names
            np.testing.assert_array_equal(got, ds["X"][row, 0, [meta["feature_names"].index(n) for n in names]])
    assert serving.parity(state, tmp_path / "dataset/dataset.npz", n=2) == (0.0, 0, 2)
    metadata_path = tmp_path / "dataset/metadata.json"
    legacy = json.loads(metadata_path.read_text())
    legacy["parameters"].pop("history_start")
    legacy["parameters"].pop("visibility_delay")
    legacy["causality"] = "history event applied only if end < query start; series game number counts starts < query start"
    metadata_path.write_text(json.dumps(legacy))
    with pytest.warns(UserWarning, match="legacy dataset"):
        assert serving.parity(state, tmp_path / "dataset/dataset.npz", n=2) == (0.0, 0, 2)
    legacy["parameters"]["history_cutoff"] += 1
    metadata_path.write_text(json.dumps(legacy))
    with pytest.raises(ValueError, match="history_cutoff"):
        serving.parity(state, tmp_path / "dataset/dataset.npz", n=2)
    path = tmp_path / "state.npz"
    serving.save_state(state, path)
    restored = serving.load_state(path)
    got_names, got = serving.features_for_map(restored, 1, 2, range(1, 6), range(6, 11), start)
    _, expected = serving.features_for_map(state, 1, 2, range(1, 6), range(6, 11), start)
    assert got_names == names
    np.testing.assert_array_equal(got, expected)


def test_delayed_event_becomes_visible_after_cutoff(tmp_path):
    start = v3.QUERY_START + v3.DAY
    cutoff = start - 1000
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 10000, cutoff - 2300, cutoff + 200, cutoff + 1000],
             [1800] * 4, [(10, 15), (40, 15), (100, 15), (25, 10)])
    meta = v3.build_dataset(rich, [], tmp_path / "dataset", history_cutoff=cutoff,
                            visibility_delay=1200, pseudo_games=0)
    state = serving.build_state(rich, [], history_start=v3.HISTORY_START,
                                cutoff=cutoff, visibility_delay=1200, pseudo_games=0)
    assert state.serving_meta["events_replayed"] == 1
    assert state.serving_meta["events_pending"] == 1
    path = tmp_path / "state.npz"
    serving.save_state(state, path)
    state = serving.load_state(path)
    names = meta["blocks"]["T"] + meta["blocks"]["P"]
    columns = [meta["feature_names"].index(name) for name in names]
    with np.load(tmp_path / "dataset/dataset.npz") as ds:
        rows = np.flatnonzero(ds["starts"] > cutoff)
        assert len(rows) == 2
        for row in rows:
            _, got = serving.features_for_map(state, 1, 2, range(1, 6), range(6, 11), int(ds["starts"][row]))
            np.testing.assert_array_equal(got, ds["X"][row, 0, columns])
    assert state.serving_pending_at == 1
    with pytest.raises(ValueError, match="nondecreasing"):
        serving.features_for_map(state, 1, 2, range(1, 6), range(6, 11), cutoff + 200)


def _random_accounts(rich, rng, low=1, high=200):
    with np.load(rich, allow_pickle=False) as data:
        blobs = {key: data[key] for key in data.files}
    n = len(blobs["mids"])
    accounts = np.empty((n, 10), np.int64)
    for i in range(n):
        accounts[i] = rng.permutation(high - low)[:10] + low
    blobs["accounts"] = accounts
    with rich.open("wb") as fh:
        np.savez_compressed(fh, **blobs)


def test_loaded_state_matches_dataset_exactly_with_pending_events(tmp_path):
    rng = np.random.default_rng(7)
    n, spacing, duration, delay = 70, 400, 1800, 1200
    start0 = v3.QUERY_START
    starts = [start0 + i * spacing for i in range(n)]
    teams = []
    for _ in range(n):
        pair = rng.choice(8, size=2, replace=False) + 1
        teams.append([int(pair[0]), int(pair[1])])
    kills = [(int(r), int(d)) for r, d in rng.integers(5, 60, size=(n, 2))]
    rich = tmp_path / "toy.npz"
    toy_rich(rich, starts, [duration] * n, kills, teams=teams)
    _random_accounts(rich, rng)
    cutoff = start0 + 30 * spacing
    meta = v3.build_dataset(rich, [], tmp_path / "dataset", history_cutoff=cutoff,
                            visibility_delay=delay)
    state = serving.build_state(rich, [], history_start=v3.HISTORY_START,
                                cutoff=cutoff, visibility_delay=delay)
    assert state.serving_meta["events_pending"] >= 1
    path = tmp_path / "state.npz"
    serving.save_state(state, path)
    loaded = serving.load_state(path)
    names = meta["blocks"]["T"] + meta["blocks"]["P"]
    columns = [meta["feature_names"].index(name) for name in names]
    events, _ = v3.load_events(rich, [], v3.HISTORY_START)
    by_mid = {int(m): k for k, m in enumerate(events["mid"])}
    with np.load(tmp_path / "dataset/dataset.npz") as ds:
        rows = np.flatnonzero(ds["starts"] > cutoff)
        assert len(rows) >= 30
        order = np.argsort(ds["starts"][rows], kind="stable")
        for row in rows[order]:
            i = by_mid[int(ds["mids"][row])]
            acc = events["accounts"][i]
            tid = events["teams"][i]
            _, got = serving.features_for_map(loaded, int(tid[0]), int(tid[1]),
                                              acc[:5], acc[5:], int(ds["starts"][row]))
            expected = ds["X"][row, 0, columns]
            assert got.shape == expected.shape
            assert (np.isnan(got) == np.isnan(expected)).all()
            np.testing.assert_array_equal(got, expected)
    with np.load(tmp_path / "dataset/dataset.npz") as ds:
        n_after = int((ds["starts"] > cutoff).sum())
    assert serving.parity(serving.load_state(path), tmp_path / "dataset/dataset.npz",
                          n=n_after) == (0.0, 0, n_after)


def test_out_of_order_queries_after_pending_applied(tmp_path):
    start0 = v3.QUERY_START
    cutoff = start0 + 20000
    delay = 1200
    q0, q1 = cutoff + 500, cutoff + 1500
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [cutoff - 10000, cutoff - 2700, cutoff - 2600, q0, q1],
             [1800] * 5, [(10, 15), (40, 15), (30, 5), (100, 15), (25, 10)],
             teams=[[1, 2]] * 5)
    meta = v3.build_dataset(rich, [], tmp_path / "dataset", history_cutoff=cutoff,
                            visibility_delay=delay, pseudo_games=0)
    state = serving.build_state(rich, [], history_start=v3.HISTORY_START,
                                cutoff=cutoff, visibility_delay=delay, pseudo_games=0)
    assert state.serving_meta["events_replayed"] == 1
    assert state.serving_meta["events_pending"] == 2
    serving.save_state(state, tmp_path / "state.npz")
    loaded = serving.load_state(tmp_path / "state.npz")
    names = meta["blocks"]["T"] + meta["blocks"]["P"]
    columns = [meta["feature_names"].index(name) for name in names]
    with np.load(tmp_path / "dataset/dataset.npz") as ds:
        row_of = {int(s): r for r, s in enumerate(ds["starts"])}
        _, first = serving.features_for_map(loaded, 1, 2, range(1, 6), range(6, 11), q0)
        np.testing.assert_array_equal(first, ds["X"][row_of[q0], 0, columns])
        assert loaded.serving_pending_at == 2
        _, second = serving.features_for_map(loaded, 1, 2, range(1, 6), range(6, 11), q1)
        np.testing.assert_array_equal(second, ds["X"][row_of[q1], 0, columns])
        _, revisit = serving.features_for_map(loaded, 1, 2, range(1, 6), range(6, 11), q0)
        np.testing.assert_array_equal(revisit, ds["X"][row_of[q0], 0, columns])
        np.testing.assert_array_equal(revisit, first)
        with pytest.raises(ValueError, match="nondecreasing"):
            serving.features_for_map(loaded, 1, 2, range(1, 6), range(6, 11), cutoff + 100)


def test_numpy_major_mismatch_refused(tmp_path):
    start = v3.QUERY_START + v3.DAY
    cutoff = start - 7000
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 20000, start - 7200, start], [1800] * 3,
             [(10, 15), (40, 15), (100, 15)])
    state = serving.build_state(rich, [], history_start=v3.HISTORY_START, cutoff=cutoff)
    assert "numpy_version" in state.serving_meta
    path = tmp_path / "state.npz"
    serving.save_state(state, path)
    serving.load_state(path)
    running_major = np.__version__.split(".")[0]
    state.serving_meta["numpy_version"] = f"{int(running_major) + 1}.0.0"
    serving.save_state(state, path)
    with pytest.raises(ValueError, match="numpy"):
        serving.load_state(path)
    state.serving_meta["numpy_version"] = f"{running_major}.99.99"
    serving.save_state(state, path)
    serving.load_state(path)
