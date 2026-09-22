import json
import sqlite3

import numpy as np
import pytest

from base.tools import kills_signature_research as sig


def player(a, h, won=1, role=0, value=2):
    return dict(account=a, hero=h, won=won, role=role, pro=[value] * 5,
                timeline=np.full(20, value, dtype=float))


def event(mid, end, players):
    return dict(mid=mid, start=end-10, end=end, players=players, source="db", timeline_ok=True)


def test_wins_boundaries_shrinkage_and_no_history():
    start = 100 * 86400
    p, h, pair = sig.Wins(), sig.Wins(), sig.Wins()
    assert sig.win_values(p, h, pair, start)[4:9] == [.5, .5, .5, 0, 0]
    for end, won in ((start-90*86400-1, 0), (start-90*86400, 1), (start-1, 1)):
        for x in (p, h, pair):
            x.add(end, won)
    values = sig.win_values(p, h, pair, start)
    assert values[:4] == [3, 3, 3, 2]
    assert values[9:13] == [2, 2, 2, 2]
    assert values[6] == pytest.approx((2+10*(7/13))/13)
    assert .5 < values[15] < 1  # two wins do not imply a 100% estimated rate
    pair.add(start, 1)
    with pytest.raises(ValueError, match="future"):
        pair.counts(start, False)


def test_invalid_comparison_fails_before_loading_or_training(tmp_path):
    with pytest.raises(ValueError, match="omitted arm"):
        sig.core.train_target(tmp_path / "missing", tmp_path / "missing", "side30", tmp_path,
                              1, arms=("baseline",), comparisons=(("delta", "signature", "baseline"),))


def test_individual_preserves_identity_order_and_query_fields_are_ignored():
    history = sig.SignatureHistory()
    history.apply(event(1, 10, [player(1, 10, 1, 4, 10), player(2, 20, 0, 0, 2)]))
    sides = [[player(1, 10, 0, 0, 999), player(2, 20, 1, 4, 999)], [player(3, 30)]]
    # Use equal team lengths; the history math itself does not require five.
    sides[1].append(player(4, 40))
    a, perf = history.rows(sides, 20)
    reversed_sides = [list(reversed(s)) for s in sides]
    b, other = history.rows(reversed_sides, 20)
    assert np.allclose(a, b, equal_nan=True)
    assert np.allclose(perf, other, equal_nan=True)
    assert a[0, :, 0].tolist() == [4, 0]
    win_index = sig.INDIVIDUAL_NAMES.index("all_pair_wr")
    assert a[0, 0, win_index] > a[0, 1, win_index]
    assert perf[0, 0, 0] == 10 and perf[0, 1, 0] == 2
    assert np.isnan(perf[1, :, :5]).all()
    assert perf.shape[-1] == len(sig.PERFORMANCE_NAMES)


def test_pair_performance_shrinks_to_player_and_keeps_missing():
    history = sig.SignatureHistory()
    history.apply(event(1, 10, [player(1, 10, value=10)]))
    history.apply(event(2, 20, [player(1, 20, value=2)]))
    _, values = history.rows([[player(1, 10)], [player(3, 30)]], 30)
    assert values[0, 0, 0] == pytest.approx((10+5*6)/6)
    assert values[0, 0, 5] == pytest.approx((10+5*6)/6-6)
    assert values[0, 0, 10] == 1
    assert np.isnan(values[1, 0, 0])
    assert values[1, 0, 10] == 0


def test_same_team_average_can_preserve_different_signature_players():
    histories = []
    for first_wins in (1, 0):
        history = sig.SignatureHistory()
        history.apply(event(1, 10, [player(1, 10, first_wins, 0),
                                    player(2, 20, 1-first_wins, 1)]))
        histories.append(history)
    query = [[player(1, 10), player(2, 20)], [player(3, 30), player(4, 40)]]
    a, b = [h.rows(query, 20)[0] for h in histories]
    assert np.allclose(sig.core.History._mean_rows(a[0], len(sig.INDIVIDUAL_NAMES)),
                       sig.core.History._mean_rows(b[0], len(sig.INDIVIDUAL_NAMES)))
    col = sig.INDIVIDUAL_NAMES.index('all_pair_wr')
    assert a[0, 0, col] > b[0, 0, col]
    assert a[0, 1, col] < b[0, 1, col]


def test_source_join_outcomes_and_role_slots_survive_filtered_accounts(tmp_path):
    db = tmp_path / "db.sqlite3"
    with sqlite3.connect(db) as con:
        con.execute("CREATE TABLE matches(match_id, radiant_win)")
        con.execute("INSERT INTO matches VALUES(1,1)")
    record = dict(mid=1, start=10, end=20, players=[[player(1, 101)], [player(6, 106, 0)]],
                  timeline_ok=True)
    rich = tmp_path / "rich.npz"
    np.savez(rich, mids=[1, 2], ts=[10, 30], durations=[10, 10], wins=[1, 0],
             accounts=np.array([np.arange(1, 11)]*2), heroes=np.array([np.arange(101, 111)]*2))
    events, audit = sig.source_events(db, rich, [record])
    assert audit["overlap_verified"] == 1
    assert [p["role"] for p in events[0]["players"]] == [0, 0]
    # No second event: its start is after the only query start.
    assert len(events) == 1
    assert events[0]["source"] == "db"
    history = sig.SignatureHistory()
    history.apply(events[0])
    assert history.source_counts["db"] == 1


@pytest.mark.parametrize("mismatch", ["time", "outcome", "identity"])
def test_conflicting_rich_overlap_cannot_replace_db_history(tmp_path, mismatch):
    db = tmp_path / "db.sqlite3"
    with sqlite3.connect(db) as con:
        con.execute("CREATE TABLE matches(match_id, radiant_win)")
        con.execute("INSERT INTO matches VALUES(1,1)")
    record = dict(mid=1, start=10, end=20,
                  players=[[player(1, 101, role=4)], [player(6, 106, role=3)]],
                  timeline_ok=True)
    heroes = np.arange(101, 111)[None, :]
    if mismatch == "identity":
        heroes[0, 0] = 999
    rich = tmp_path / "rich.npz"
    np.savez(rich, mids=[1], ts=[9 if mismatch == "time" else 10], durations=[10],
             wins=[0 if mismatch == "outcome" else 1],
             accounts=np.arange(1, 11)[None, :], heroes=heroes)
    events, audit = sig.source_events(db, rich, [record])
    assert len(events) == 1
    assert [p["won"] for p in events[0]["players"]] == [1, 0]
    assert [p["role"] for p in events[0]["players"]] == [4, 3]
    assert sum(v for k, v in audit.items() if "excluded_rich" in k) == 1
    history = sig.SignatureHistory()
    history.apply(events[0])
    assert history.pair_wins[(1, 101)].games == 1
    assert history.pair_wins[(1, 101)].wins == 1


def test_augmentation_preserves_controls_and_excludes_unfinished_outcomes(tmp_path, monkeypatch):
    sides = [[player(i+1, i+101, role=i % 5) for i in range(a, a+5)] for a in (0, 5)]
    records = [dict(mid=50+i, start=1000+i, end=1100+i, players=sides) for i in range(2)]
    historical = [event(1, 999, [p for side in sides for p in side]),
                  event(2, 1000, [dict(p, won=0) for side in sides for p in side])]
    future = event(3, 1001, [dict(p, won=0, pro=[999]*5) for side in sides for p in side])
    monkeypatch.setattr(sig.core, "_db_maps", lambda _: (records, {}))
    monkeypatch.setattr(sig, "source_events", lambda *args: (historical+[future], {}))
    sig.core.atomic_npz(tmp_path / "data.npz", mids=np.array([50, 51]),
                        X_baseline=np.ones((2, 2, 20)), y=np.zeros((2, 6, 2)))
    sig.core.atomic_json(tmp_path / "meta.json", {"features":{}, "source_hashes":{}})
    args = (tmp_path / "data.npz", tmp_path / "meta.json", tmp_path / "db", tmp_path / "rich")
    sig.augment(*args, tmp_path / "a")
    a = np.load(tmp_path / "a/dataset.npz")
    future['players'] = [dict(p, won=1, pro=[0]*5) for side in sides for p in side]
    sig.augment(*args, tmp_path / "b")
    b = np.load(tmp_path / "b/dataset.npz")
    for name in a.files:
        assert np.allclose(a[name], b[name], equal_nan=True)
    assert np.array_equal(a['X_baseline'], np.ones((2, 2, 20)))
    col = sig.INDIVIDUAL_NAMES.index('all_pair_n')
    assert a['X_individual'][:, 0, col].tolist() == [1, 2]


@pytest.mark.parametrize("target", ["lead_5_15", "side30", "total55"])
def test_signature_training_saved_schema_and_comparisons(tmp_path, target):
    rng = np.random.default_rng(7)
    n = 10
    base = rng.normal(size=(n, 2, 20)).astype(np.float32)
    base[:, :, :10] = rng.integers(1, 30, size=(n, 2, 10))
    y = np.zeros((n, 6, 2), np.float32)
    y[:, :, 0] = (np.arange(n) % 2)[:, None]
    y[:, :, 1] = 1-y[:, :, 0]
    y[:, 5, 1] = y[:, 5, 0]
    blocks = dict(baseline=base, experience=np.zeros((n, 2, 12)), recent=np.zeros((n, 2, 30)))
    for name in ("signature", "individual", "performance"):
        blocks[name] = rng.normal(size=(n, 2, 3)).astype(np.float32)
    sig.core.atomic_npz(tmp_path / "data.npz", **{f"X_{k}": v for k, v in blocks.items()},
                        y=y, mids=np.arange(n), starts=np.arange(n)*86400, split=np.repeat(np.arange(5),2))
    sig.core.atomic_json(tmp_path / "meta.json", {"schema":"test", "source_hashes":{},
                         "features":{k:[f"{k}_{i}" for i in range(v.shape[-1])] for k,v in blocks.items()}})
    report = sig.core.train_target(tmp_path / "data.npz", tmp_path / "meta.json", target,
                                   tmp_path / "out", 1, arms=sig.ARMS, comparisons=sig.COMPARISONS)
    assert all(name in report for name, _, _ in sig.COMPARISONS)
    schema = json.loads((tmp_path / "out/schema.json").read_text())
    assert len(schema["feature_names"]) == sum(blocks[k].shape[-1] for k in sig.core.ARM_BLOCKS[report["chosen"]])
    assert set(report["terminal"]) == set(sig.ARMS)
