"""Frozen history and live query parity for kills-v3 T+P."""
import json
import sqlite3
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


def test_code_version_is_frozen_at_import_during_build(tmp_path, monkeypatch):
    start = v3.QUERY_START + v3.DAY
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 20000], [1800], [(10, 15)])
    original_sha256 = v3.sha256
    expected = {"builder_sha256": original_sha256(v3.Path(v3.__file__)),
                "serving_sha256": original_sha256(serving.Path(serving.__file__))}

    def changed_code_on_disk(path):
        if str(path) in (str(v3.__file__), str(serving.__file__)):
            return "edited-during-build"
        return original_sha256(path)

    monkeypatch.setattr(v3, "sha256", changed_code_on_disk)
    state = serving.build_state(rich, [], history_start=v3.HISTORY_START,
                                cutoff=start - 7000, pseudo_games=0)
    assert state.serving_meta["code_version"] == expected


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
        serving.features_for_map(state, 1, 2, range(1, 6), range(6, 11), cutoff + 200,
                                 strict=True)


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
            serving.features_for_map(loaded, 1, 2, range(1, 6), range(6, 11), cutoff + 100,
                                     strict=True)


@pytest.mark.parametrize("delay", [1200, 86400])
def test_live_out_of_order_reuses_visible_frontier(tmp_path, delay):
    cutoff = v3.QUERY_START + 20000
    early, late = cutoff + 600, cutoff + 1150
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [cutoff - delay - 7800, cutoff - delay - 1500,
                    cutoff - delay - 700, early, late],
             [1800] * 5, [(10, 15), (40, 15), (30, 5), (100, 15), (25, 10)])
    state = serving.build_state(rich, [], history_start=v3.HISTORY_START,
                                cutoff=cutoff, visibility_delay=delay, pseudo_games=0)
    assert state.serving_meta["events_pending"] == 2
    accounts = (range(1, 6), range(6, 11))
    serving.features_for_map(state, 1, 2, *accounts, late)
    assert state.serving_last_overvisible_seconds == 0
    assert state.serving_pending_at == 2
    with pytest.raises(ValueError, match="nondecreasing"):
        serving.features_for_map(state, 1, 2, *accounts, early, strict=True)
    _, got = serving.features_for_map(state, 1, 2, *accounts, early)
    assert state.serving_last_overvisible_seconds == 501
    assert state.serving_out_of_order_queries == 1

    # Independent History replay at the already-applied frontier, then query
    # using the earlier map's own start time.
    events, _ = v3.load_events(rich, [], v3.HISTORY_START)
    frontier = late - delay
    expected_history = v3.History(pseudo_games=0)
    for i in np.argsort(events["end"], kind="stable"):
        if int(events["end"][i]) >= frontier:
            break
        expected_history.apply(events, int(i))
    query = {"start": early, "league": 0, "stype": 0, "series_number": 0,
             "teams": np.array([1, 2]), "accounts": np.arange(1, 11),
             "heroes": np.zeros(10, np.int64)}
    x = expected_history.query_features(query)
    blocks = expected_history.feature_blocks
    lo = len(blocks["G"])
    expected = x[0, lo:lo + len(blocks["T"]) + len(blocks["P"])]
    np.testing.assert_array_equal(got, expected)


@pytest.mark.parametrize("bad", ["four_accounts", "milliseconds", "negative_account",
                                 "team_below_int64", "team_above_int64", "boolean_team",
                                 "noninteger_team", "noninteger_start", "invalid_account"])
def test_bad_input_does_not_apply_pending_or_change_features(tmp_path, bad):
    cutoff = v3.QUERY_START + 20000
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [cutoff - 9000, cutoff - 2700, cutoff + 500], [1800] * 3,
             [(10, 15), (40, 15), (100, 15)])
    def fresh():
        return serving.build_state(rich, [], history_start=v3.HISTORY_START,
                                   cutoff=cutoff, visibility_delay=1200, pseudo_games=0)
    state = fresh()
    accounts = [1, 2, 3, 4, 5]
    start = cutoff + 1000
    radiant_team = 1
    if bad == "four_accounts":
        accounts.pop()
    elif bad == "negative_account":
        accounts[0] = -1
    elif bad == "milliseconds":
        start *= 1000
    elif bad == "team_below_int64":
        radiant_team = -(2 ** 63) - 1
    elif bad == "team_above_int64":
        radiant_team = 2 ** 63
    elif bad == "boolean_team":
        radiant_team = True
    elif bad == "noninteger_team":
        radiant_team = "-123"
    elif bad == "noninteger_start":
        start = str(start)
    else:
        accounts[0] = "unknown"
    with pytest.raises(ValueError):
        serving.features_for_map(state, radiant_team, 2, accounts, range(6, 11), start)
    assert state.serving_pending_at == 0
    assert state.serving_last_query_start == cutoff
    _, got = serving.features_for_map(state, 1, 2, range(1, 6), range(6, 11), cutoff + 1000)
    _, expected = serving.features_for_map(fresh(), 1, 2, range(1, 6), range(6, 11),
                                            cutoff + 1000)
    np.testing.assert_array_equal(got, expected)


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


def test_negative_team_id_uses_builder_history(tmp_path):
    start = v3.QUERY_START + v3.DAY
    cutoff = start - 7000
    rich = tmp_path / "negative_team.npz"
    toy_rich(rich, [start - 20000, start], [1800, 1800],
             [(20, 10), (30, 15)], teams=[[-123, 2], [-123, 2]])
    meta = v3.build_dataset(rich, [], tmp_path / "dataset", pseudo_games=0,
                            history_cutoff=cutoff)
    state = serving.build_state(rich, [], history_start=v3.HISTORY_START,
                                cutoff=cutoff, pseudo_games=0)
    with np.load(tmp_path / "dataset/dataset.npz") as ds:
        row = int(np.flatnonzero(ds["starts"] == start)[0])
        columns = [meta["feature_names"].index(name) for name in
                   meta["blocks"]["T"] + meta["blocks"]["P"]]
        _, got = serving.features_for_map(state, -123, 2, range(1, 6),
                                          range(6, 11), start)
        np.testing.assert_array_equal(got, ds["X"][row, 0, columns])
        assert np.isfinite(got[meta["blocks"]["T"].index("team_own_kills_for")])
    fresh = serving.build_state(rich, [], history_start=v3.HISTORY_START,
                                cutoff=cutoff, pseudo_games=0)
    assert serving.parity(fresh, tmp_path / "dataset/dataset.npz") == (0.0, 0, 1)


def test_roundtrip_preserves_builder_source_order(tmp_path):
    start = v3.QUERY_START + v3.DAY
    cutoff = start - 7000
    rich = tmp_path / "z_rich.npz"
    db = tmp_path / "a_empty.sqlite3"
    toy_rich(rich, [start - 20000, start], [1800, 1800], [(20, 10), (30, 15)])
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE matches (match_id INTEGER, status TEXT)")
        conn.execute("CREATE TABLE match_windows (match_id INTEGER)")
        conn.execute("CREATE TABLE player_windows (match_id INTEGER, row_index INTEGER)")
    v3.build_dataset(rich, [db], tmp_path / "dataset", pseudo_games=0,
                     history_cutoff=cutoff)
    state = serving.build_state(rich, [db], history_start=v3.HISTORY_START,
                                cutoff=cutoff, pseudo_games=0)
    path = tmp_path / "state.npz"
    serving.save_state(state, path)
    loaded = serving.load_state(path)
    assert loaded.serving_meta["source_order"] == [str(rich.resolve()), str(db.resolve())]
    assert serving.parity(loaded, tmp_path / "dataset/dataset.npz") == (0.0, 0, 1)
    state.serving_meta.pop("source_order")
    serving.save_state(state, path)
    with pytest.raises(ValueError, match="source order missing"):
        serving.load_state(path)
