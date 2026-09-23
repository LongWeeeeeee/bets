"""Regression checks for the as-of kills v3 event and feature contract."""
import numpy as np
import pytest

from base.tools import kills_v3_research as v3


def toy_rich(path, starts, durations, kills, teams=None, timeline=True):
    n = len(starts)
    stats = np.zeros((n, 10, 14), np.float32)
    for i, (r, d) in enumerate(kills):
        stats[i, :5, 0] = r / 5
        stats[i, 5:, 0] = d / 5
        stats[i, :, 1:3] = 1
        stats[i, :, 3:5] = 500
    rk = np.zeros((n, 41), np.int16)
    dk = np.zeros_like(rk)
    if timeline:
        for i, (r, d) in enumerate(kills):
            rk[i] = np.minimum(np.arange(41), 30) * r / 30
            dk[i] = np.minimum(np.arange(41), 30) * d / 30
    np.savez_compressed(path, mids=np.arange(1, n + 1), ts=np.asarray(starts),
                        durations=np.asarray(durations), sids=np.zeros(n, np.int64),
                        stypes=np.ones(n, np.int8), leagues=np.ones(n, np.int64),
                        teams=np.asarray(teams if teams is not None else [[1, 2]] * n),
                        wins=np.ones(n, np.int8), heroes=np.tile(np.arange(1, 11), (n, 1)),
                        accounts=np.tile(np.arange(1, 11), (n, 1)), pstats=stats, rk=rk, dk=dk,
                        pstat_names=np.asarray(("kills", "deaths", "assists", "goldPerMinute",
                                                "experiencePerMinute", "networth", "numLastHits",
                                                "numDenies", "heroDamage", "towerDamage", "heroHealing",
                                                "imp", "level", "dotaPlusHeroXp")))


def test_strict_end_before_start_and_query_never_contributes(tmp_path):
    # Map 2 ends exactly as map 3 starts; its 40 kills must be invisible.
    start = v3.QUERY_START
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 7200, start - 1800, start], [1800, 1800, 1800],
             [(10, 15), (40, 15), (100, 15)])
    meta = v3.build_dataset(rich, [tmp_path / "absent.sqlite3"], tmp_path / "out", pseudo_games=0)
    assert meta["parameters"]["history_start"] == v3.HISTORY_START
    assert meta["parameters"]["query_start"] == v3.QUERY_START
    with np.load(tmp_path / "out/dataset.npz") as ds:
        j = meta["feature_names"].index("team_own_kills_for")
        assert ds["mids"].tolist() == [3]
        assert ds["X"][0, 0, j] == pytest.approx(10)
        assert ds["y"][0, 0, 0] == 100
    # One-second earlier end makes map 2 visible, so this catches <= leakage.
    toy_rich(rich, [start - 7200, start - 1801, start], [1800, 1800, 1800],
             [(10, 15), (40, 15), (100, 15)])
    meta = v3.build_dataset(rich, [], tmp_path / "out2", pseudo_games=0)
    with np.load(tmp_path / "out2/dataset.npz") as ds:
        j = meta["feature_names"].index("team_own_kills_for")
        assert ds["X"][0, 0, j] > 20


def test_explicit_history_and_query_starts_include_early_maps(tmp_path):
    early = 1451606400  # 2016-01-01 UTC
    rich = tmp_path / "early.npz"
    toy_rich(rich, [early, early + 7200], [1800, 1800], [(10, 15), (30, 20)])
    meta = v3.build_dataset(rich, [], tmp_path / "early_out", pseudo_games=0,
                            history_start=early, query_start=early)
    assert meta["parameters"]["history_start"] == early
    assert meta["parameters"]["query_start"] == early
    with np.load(tmp_path / "early_out/dataset.npz") as ds:
        assert ds["mids"].tolist() == [1, 2]
        j = meta["feature_names"].index("team_own_kills_for")
        assert ds["X"][1, 0, j] == pytest.approx(10)
    v3.build_dataset(rich, [], tmp_path / "later_query", pseudo_games=0,
                     history_start=early, query_start=early + 7200)
    with np.load(tmp_path / "later_query/dataset.npz") as ds:
        assert ds["mids"].tolist() == [2]
        assert ds["X"][0, 0, j] == pytest.approx(10)


def test_orientation_mirror_and_zero_team(tmp_path):
    start = v3.QUERY_START
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 7200, start], [1800, 1800], [(20, 10), (30, 15)],
             teams=[[1, 2], [0, 2]])
    meta = v3.build_dataset(rich, [], tmp_path / "out")
    names = meta["feature_names"]
    with np.load(tmp_path / "out/dataset.npz") as ds:
        x = ds["X"][0]
        for prefix in ("team", "player", "hero"):
            for name in names:
                if name.startswith(prefix + "_own_"):
                    mate = name.replace("_own_", "_opp_", 1)
                    if mate in names:
                        np.testing.assert_equal(x[0, names.index(name)], x[1, names.index(mate)])
        assert all(np.isnan(x[0, names.index(name)]) for name in meta["blocks"]["T"] if name.startswith("team_own_"))
        assert x[0, names.index("hero_own_0")] == x[1, names.index("hero_opp_0")]


def test_negative_nonzero_team_id_keeps_history(tmp_path):
    start = v3.QUERY_START
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 7200, start], [1800, 1800], [(20, 10), (30, 15)],
             teams=[[-123, 2], [-123, 2]])
    meta = v3.build_dataset(rich, [], tmp_path / "out", pseudo_games=0)
    with np.load(tmp_path / "out/dataset.npz") as ds:
        assert np.isfinite(ds["X"][0, 0, meta["feature_names"].index("team_own_kills_for")])


def test_opendota_extras_require_account_history():
    metrics = np.full((2, 10, len(v3.PLAYER_METRICS)), np.nan, np.float32)
    metrics[0, :, :3] = 2
    metrics[0, :, v3.EXTRA_START:] = 3
    events = {"start": np.array([v3.QUERY_START - 7200, v3.QUERY_START]),
              "end": np.array([v3.QUERY_START - 5400, v3.QUERY_START + 1800]),
              "duration": np.array([1800, 1800]), "final": np.array([[10, 10], [10, 10]]),
              "timeline": np.ones((2, 2, 6)), "win": np.array([1, 1]),
              "league": np.array([1, 1]), "stype": np.array([1, 1]),
              "teams": np.array([[1, 2], [3, 4]]), "pmetrics": metrics,
              "accounts": np.array([np.arange(1, 11), np.arange(11, 21)]),
              "heroes": np.tile(np.arange(1, 11), (2, 1))}
    history = v3.History()
    history.apply(events, 0)
    names, blocks = v3.feature_layout()
    x = v3._orient_features(history.features(events, 1, 0), names, blocks)
    assert np.isnan(x[0, names.index("player_own_obs_placed_mean")])
    assert x[0, names.index("player_own_obs_placed_effective_n_mean")] == 0


def test_dedupe_prefers_opendota_and_counts_disagreement(tmp_path, monkeypatch):
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [v3.QUERY_START], [1800], [(20, 15)])
    rec = {"mid": 1, "start": v3.QUERY_START, "duration": 1800, "series": 0,
           "stype": 1, "league": 1, "teams": [1, 2], "win": 1,
           "final": np.array([22, 15], np.float32), "heroes": list(range(1, 11)),
           "accounts": list(range(1, 11)),
           "pmetrics": np.ones((10, len(v3.PLAYER_METRICS)), np.float32),
           "timeline": np.ones((2, 6), np.float32)}
    rec["pmetrics"][0, 11] = np.nan  # OpenDota has no final LH; STRATZ does.
    monkeypatch.setattr(v3, "_db_records", lambda paths, history_start: ({1: rec}, {}))
    events, audit = v3.load_events(rich, [])
    assert len(events["mid"]) == 1
    assert events["source"].tolist() == [1]
    assert events["final"][0].tolist() == [22, 15]
    assert audit["overlap_side_disagreements_gt0"] == 1
    assert audit["overlap_within_tolerance_2"] == 1
    assert events["pmetrics"][0, 0, 11] == 0
    assert audit["overlap_player_stat_cells_filled_from_stratz"] == 1


def test_overlap_identity_conflict_fails_closed(tmp_path, monkeypatch):
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [v3.QUERY_START], [1800], [(20, 15)])
    rec = {"mid": 1, "start": v3.QUERY_START, "duration": 1800, "series": 0,
           "stype": 1, "league": 1, "teams": [1, 2], "win": 1,
           "final": np.array([20, 15], np.float32), "heroes": list(range(1, 11)),
           "accounts": [999] + list(range(2, 11)),
           "pmetrics": np.ones((10, len(v3.PLAYER_METRICS)), np.float32),
           "timeline": np.ones((2, 6), np.float32)}
    monkeypatch.setattr(v3, "_db_records", lambda paths, history_start: ({1: rec}, {}))
    with pytest.raises(ValueError, match="identity conflict"):
        v3.load_events(rich, [])


def test_censored_windows_and_ties():
    series = np.ones((2, 6), np.float32)
    assert np.isnan(v3.labels_for(np.array([10, 10]), series, 14 * 60)[0, 3])
    series[:] = np.nan
    assert np.isnan(v3.labels_for(np.array([10, 10]), series, 40 * 60)[0, 3])
    series[:] = 1
    assert v3.labels_for(np.array([10, 10]), series, 40 * 60)[0, 3] == 0


def test_hand_computed_shrinkage_three_maps():
    pop = v3.Decayed(1)
    team = v3.Decayed(1)
    when = v3.QUERY_START - 10
    for value in (10.0, 20.0, 30.0):
        pop.add(np.array([value]), when, 90)
    team.add(np.array([10.0]), when, 90)
    team.add(np.array([30.0]), when, 90)
    value, count = v3.shrunk(team, pop, when, 90, 5)
    assert count[0] == 2
    assert value[0] == pytest.approx((10 + 30 + 5 * 20) / 7)


def test_series_purge_drops_earlier_split_only():
    starts = np.array([v3.VALID_START - 1, v3.VALID_START, v3.TEST_START])
    split, audit = v3.split_with_purge(starts, np.array([7, 7, 8]))
    assert split.tolist() == [-1, 1, 2]
    assert audit["purged_train_pool"] == 1


def test_league_28_day_decay_uses_only_completed_maps(tmp_path):
    start = v3.QUERY_START
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 27 * v3.DAY - 1800, start - v3.DAY - 1800, start],
             [1800, 1800, 1800], [(10, 0), (30, 0), (100, 0)])
    events, _ = v3.load_events(rich, [])
    got = v3.corrected_global_meta(events, half_life_days=10)[0, 0]
    history = v3.History(half_life_days=10)
    history.apply(events, 0)
    history.apply(events, 1)
    online = history.features(events, 2, 0)[0][0]
    w1, w2 = 2 ** (-27 / 10), 2 ** (-1 / 10)
    assert got == pytest.approx((10 * w1 + 30 * w2) / (w1 + w2), rel=1e-6)
    assert online == pytest.approx(got, rel=1e-6)


def test_new_db_appearing_after_ingestion_is_not_claimed_as_source(tmp_path, monkeypatch):
    rich = tmp_path / "toy.npz"
    late_db = tmp_path / "late.sqlite3"
    toy_rich(rich, [v3.QUERY_START], [1800], [(10, 10)])
    original = v3.load_events

    def load_then_appear(rich_path, db_paths, history_start):
        events, audit = original(rich_path, db_paths, history_start)
        late_db.write_bytes(b"appeared after read")
        return events, audit

    monkeypatch.setattr(v3, "load_events", load_then_appear)
    with pytest.warns(UserWarning, match="missing"):
        meta = v3.build_dataset(rich, [late_db], tmp_path / "out")
    assert str(late_db) in meta["source_audit"]["db_paths_missing"]
    assert str(late_db) not in meta["source_hashes"]


def test_history_cutoff_freezes_history_like_production_snapshot(tmp_path):
    # Map 2 (40 kills) ends before map 3 starts but after the frozen cutoff:
    # the frozen variant must not see it, the online variant must.
    start = v3.QUERY_START + 86400
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 20000, start - 7200, start], [1800, 1800, 1800],
             [(10, 15), (40, 15), (100, 15)])
    cutoff = start - 7200 + 1800  # exactly map 2 end: strict end < cutoff excludes it
    online = v3.build_dataset(rich, [], tmp_path / "online", pseudo_games=0)
    frozen = v3.build_dataset(rich, [], tmp_path / "frozen", pseudo_games=0, history_cutoff=cutoff)
    j = online["feature_names"].index("team_own_kills_for")
    with np.load(tmp_path / "online/dataset.npz") as a, np.load(tmp_path / "frozen/dataset.npz") as b:
        row = int(np.flatnonzero(a["mids"] == 3)[0])
        assert a["X"][row, 0, j] > 20
        assert b["X"][row, 0, j] == pytest.approx(10)
        assert np.array_equal(a["y"], b["y"])
    assert frozen["parameters"]["history_cutoff"] == cutoff
    assert online["parameters"]["history_cutoff"] is None


def test_team_ids_are_written_in_orientation_order(tmp_path):
    start = v3.QUERY_START + 3600
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 7200, start], [1800, 1800], [(10, 15), (20, 25)], teams=[[7, 9], [9, 7]])
    v3.build_dataset(rich, [], tmp_path / "out", pseudo_games=0)
    with np.load(tmp_path / "out/dataset.npz") as ds:
        assert ds["team_ids"].tolist() == [[9, 7]]
        assert ds["y"][0, 0, 0] == 20  # orientation 0 = radiant team 9


def test_visibility_delay_hides_maps_that_ended_within_the_delay(tmp_path):
    # Map 2 (40 kills) ends 600 s before map 3 starts. With a 1200 s visibility
    # delay (serving lag) it must be invisible; with 0 s it must be visible.
    start = v3.QUERY_START + 86400
    rich = tmp_path / "toy.npz"
    toy_rich(rich, [start - 20000, start - 2400, start], [1800, 1800, 1800],
             [(10, 15), (40, 15), (100, 15)])
    online = v3.build_dataset(rich, [], tmp_path / "online", pseudo_games=0)
    delayed = v3.build_dataset(rich, [], tmp_path / "delayed", pseudo_games=0, visibility_delay=1200)
    edge = v3.build_dataset(rich, [], tmp_path / "edge", pseudo_games=0, visibility_delay=599)
    j = online["feature_names"].index("team_own_kills_for")
    with np.load(tmp_path / "online/dataset.npz") as a, np.load(tmp_path / "delayed/dataset.npz") as b, \
            np.load(tmp_path / "edge/dataset.npz") as c:
        row = int(np.flatnonzero(a["mids"] == 3)[0])
        assert a["X"][row, 0, j] > 20
        assert b["X"][row, 0, j] == pytest.approx(10)
        assert c["X"][row, 0, j] > 20  # end = start-600 < start-599: still visible
        assert np.array_equal(a["y"], b["y"])
        assert np.array_equal(a["ends"], b["ends"])  # true ends are kept; only visibility shifts
    assert delayed["parameters"]["visibility_delay"] == 1200
    assert online["parameters"]["visibility_delay"] == 0
