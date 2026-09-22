from __future__ import annotations

import json
import sqlite3

import numpy as np
import pytest

from base.tools.kills_opendota_research import (
    ARMS, BOUNDARIES, TARGETS, Experience, History, atomic_json, atomic_npz, fixed_splits,
    _metric, _raw_prediction_for_target, _timeline_values, _db_maps,
    build_dataset, epoch, lead_labels, train_target,
)


def _player(account, hero, *, timeline=(), pro=None, role=None, dpxp=None):
    return {"account": account, "hero": hero, "timeline": timeline,
            "pro": pro, "role": role, "dpxp": dpxp}


def _event(mid, start, end, players, *, source="db", timeline_ok=True):
    return {"mid": mid, "start": start, "end": end, "players": players,
            "source": source, "timeline_ok": timeline_ok, "teams": (11, 22),
            "team_hero_windows": np.full((2, 4), np.nan)}


def _features_after_completed(history, events, query_start, query_players):
    """Same strict boundary used by the builder's pending-event loop."""
    for event in events:
        if event["end"] < query_start:
            history.apply(event)
    return history.features(query_players, (11, 22), query_start)


def test_future_same_time_and_overlapping_events_cannot_change_features():
    width = 4 * 5
    past = _event(1, 10, 90, [_player(7, 1, timeline=np.full(width, 2.0), pro=np.ones(5), role=1)])
    same_time = _event(2, 90, 100, [_player(7, 1, timeline=np.full(width, 99.0), pro=np.full(5, 99.0), role=4)])
    overlapping = _event(3, 30, 120, [_player(7, 1, timeline=np.full(width, 88.0), pro=np.full(5, 88.0), role=4)])
    future = _event(4, 101, 110, [_player(7, 1, timeline=np.full(width, 77.0), pro=np.full(5, 77.0), role=4)])
    query = [[_player(7, 1)], [_player(8, 2)]]
    base = _features_after_completed(History(), [past], 100, query)
    changed = _features_after_completed(History(), [past, same_time, overlapping, future], 100, query)
    for left, right in zip(base, changed):
        assert np.allclose(left, right, equal_nan=True)
    # The past event becomes visible one second after the strict boundary.
    visible = _features_after_completed(History(), [past], 91, query)
    assert np.isfinite(visible[0][0, 0])


def test_current_role_and_player_order_do_not_leak_into_role_features():
    old = _event(1, 1, 10, [_player(1, 10, role=4), _player(2, 20, role=0)])
    history = History(); history.apply(old)
    first = [[_player(1, 10, role=0), _player(2, 20, role=4)], [_player(3, 30)]]
    second = [[_player(2, 20, role=1), _player(1, 10, role=3)], [_player(3, 30)]]
    a = history.features(first, (11, 22), 20)
    b = history.features(second, (11, 22), 20)
    for left, right in zip(a, b):
        assert np.allclose(left, right, equal_nan=True)


def test_dpxp_uses_only_completed_player_hero_measurement():
    history = History()
    history.apply(_event(1, 1, 10, [_player(1, 10, role=0, dpxp=123.0)]))
    _, _, _, before = history.features([[_player(1, 10, dpxp=999999)], [_player(2, 20)]], (11, 22), 20)
    assert before[0] == 123.0
    # A current value exists in a query-shaped row but is never passed to apply.
    _, _, _, again = history.features([[_player(1, 10, dpxp=-1)], [_player(2, 20)]], (11, 22), 20)
    assert again[0] == 123.0


def test_recent_counts_include_lower_boundary_exclude_query_and_allow_out_of_order():
    now = 100 * 86400
    exp = Experience()
    for end in (now - 1, now, now - 7 * 86400, now - 7 * 86400 - 1, now + 1):
        exp.add(end)
    assert exp.recent_games(now, 7) == 2
    assert exp.recent_games(now, 30) == 3


def test_recent_hero_practice_shares_do_not_depend_on_dota_plus():
    now = 100 * 86400
    histories = []
    for xp in (0.0, 999999.0):
        history = History()
        for mid, days, hero in ((1, 91, 10), (2, 30, 10), (3, 7, 20), (4, 1, 10)):
            end = now - days * 86400
            history.apply(_event(mid, end-60, end, [_player(1, hero, role=0, dpxp=xp)]))
        histories.append(history)
    players = [[_player(1, 10, role=4)], [_player(99, 30)]]
    a, b = [h.recent_features(players, now) for h in histories]
    assert np.allclose(a, b, equal_nan=True)
    assert a[0] == pytest.approx([2, 1, 2, .5, 1, 3, 2, 3, 2/3, 1, 3, 2, 3, 2/3, 1])
    # No observed history is zero observed games, not a measured zero share.
    assert a[1, :3].tolist() == [0, 0, 0]
    assert np.isnan(a[1, [3, 4, 8, 9, 13, 14]]).all()
    assert histories[0].features(players, (11, 22), now)[2][0, -1] == 86400


def test_missing_timeline_values_stay_missing_not_zero():
    width = 4 * 5
    history = History()
    values = np.full(width, np.nan); values[1] = 4.0
    history.apply(_event(1, 1, 10, [_player(1, 10, timeline=values)]))
    _, timeline, _, _ = history.features([[_player(1, 10)], [_player(2, 20)]], (11, 22), 20)
    # First player metric is deaths 300--900; missing source remains NaN.  The
    # next gold feature was observed and is available.
    assert np.isnan(timeline[0, 0])
    assert timeline[0, 1] == 4.0


def test_window_label_uses_opponent_deaths_and_omits_ties_short_absent():
    hero_kills = np.full((2, len(BOUNDARIES)), np.nan)
    hero_kills[:, :3] = [[1, 2, 9], [1, 4, 7]]  # 5--15: radiant +8, dire +6
    labels = lead_labels(1000, hero_kills)
    assert labels[0].tolist() == [1.0, 0.0]
    assert np.isnan(labels[1:]).all()  # absent boundaries / duration windows
    hero_kills[:, :3] = [[1, 2, 8], [1, 4, 8]]  # both +7
    assert np.isnan(lead_labels(1000, hero_kills)[0]).all()


def test_metric_reports_accuracy_and_observed_nonempty_ece_bins():
    report = _metric(np.array([0, 1, 0, 1]), np.array([.1, .2, .8, .9]))
    assert report["accuracy_at_0_5"] == .5
    assert report["ece_10_equal_width"] == pytest.approx(.45)
    assert [item["n"] for item in report["calibration_bins"]] == [1, 1, 1, 1]
    assert [item["lower"] for item in report["calibration_bins"]] == [.1, .2, .8, .9]


def test_fixed_series_purge_and_unknown_series_keep_both_sides_grouped():
    starts = np.array([epoch("2026-07-01"), epoch("2026-09-01"), epoch("2026-09-01"), epoch("2026-09-01")])
    ends = starts + 60
    split = fixed_splits(starts, ends, np.array([77, 77, 0, 0]))
    assert split[:2].tolist() == [-1, -1]  # known series crosses a fixed boundary
    assert split[2:].tolist() == [4, 4]   # unknown ID is map-local; two sides share map split


def test_build_dataset_flattens_db_history_and_enriches_matching_rich_rows(tmp_path):
    db = tmp_path / "timeline.sqlite3"; conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE matches (match_id INTEGER PRIMARY KEY, status TEXT, radiant_score INTEGER, dire_score INTEGER)")
    conn.execute("CREATE TABLE match_windows (match_id INTEGER PRIMARY KEY, duration INTEGER, start_time INTEGER, series_id INTEGER, radiant_team_id INTEGER, dire_team_id INTEGER)")
    columns = ["match_id INTEGER", "row_index INTEGER", "side TEXT", "account_id INTEGER", "hero_id INTEGER",
               "kills INTEGER", "deaths INTEGER", "assists INTEGER", "gold_per_min REAL", "xp_per_min REAL"]
    columns += [f"{metric}_{boundary} REAL" for metric in ("deaths", "gold", "xp", "lh", "dn") for boundary in BOUNDARIES]
    conn.execute(f"CREATE TABLE player_windows ({', '.join(columns)})")
    for mid, start in ((1, 10), (2, 1100)):
        conn.execute("INSERT INTO matches VALUES (?, 'complete', 30, 20)", (mid,))
        conn.execute("INSERT INTO match_windows VALUES (?, 1000, ?, 0, 11, 22)", (mid, start))
        for slot in range(10):
            fields = {"match_id": mid, "row_index": slot, "side": "radiant" if slot < 5 else "dire",
                      "account_id": slot + 1, "hero_id": slot + 101, "kills": 3, "deaths": 2, "assists": 4,
                      "gold_per_min": 400, "xp_per_min": 500}
            fields.update({f"{metric}_{boundary}": boundary / 300 for metric in ("deaths", "gold", "xp", "lh", "dn") for boundary in BOUNDARIES})
            names, values = list(fields), list(fields.values())
            conn.execute(f"INSERT INTO player_windows ({','.join(names)}) VALUES ({','.join('?' for _ in names)})", values)
    conn.commit(); conn.close()
    rich = tmp_path / "rich.npz"
    np.savez(rich, mids=np.array([1, 2]), ts=np.array([10, 1100]), durations=np.array([1000, 1000]), sids=np.zeros(2),
             heroes=np.array([[101 + slot for slot in range(10)]] * 2), accounts=np.array([[slot + 1 for slot in range(10)]] * 2),
             pstats=np.zeros((2, 10, 14), dtype=np.float32), pstat_names=np.array(["kills", "deaths", "assists", "goldPerMinute", "experiencePerMinute", "networth", "numLastHits", "numDenies", "heroDamage", "towerDamage", "heroHealing", "imp", "level", "dotaPlusHeroXp"]))
    metadata = build_dataset(db, rich, tmp_path / "built")
    output = np.load(tmp_path / "built" / "dataset.npz")
    assert metadata["source_counts"]["db_query_maps"] == 2
    assert metadata["baseline_kind"] == "new_pro_only_not_production_E281"
    assert output["X_timeline"].shape[0] == 2
    # Map 2 sees flat players from completed map 1; a nested DB event would
    # TypeError before producing this finite causal timeline feature.
    assert np.isfinite(output["X_timeline"][1, 0, 0])
    # The source ended at 16:40. Collector counters after that point must not
    # become complete 10--20, 15--25 or 20--30 historical observations.
    assert np.isnan(output["X_timeline"][1, 0, 5:20]).all()
    assert (output["X_timeline"][1, 0, 25:40] == 0).all()
    records, _ = _db_maps(db)
    assert np.isfinite(records[0]["team_hero_windows"][:, 0]).all()
    assert np.isnan(records[0]["team_hero_windows"][:, 1:]).all()
    assert output["X_recent"].shape == (2, 2, 30)
    assert output["X_recent"][1, 0, :5].tolist() == [1, 1, 1, 1, 1]


def test_source_window_exact_end_is_observed_but_one_second_short_is_not():
    row = {f"{metric}_{boundary}": boundary for metric in ("deaths", "gold", "xp", "lh", "dn") for boundary in BOUNDARIES}
    assert np.isnan(_timeline_values(row, 899)[:5]).all()
    assert (_timeline_values(row, 900)[:5] == 600).all()


def test_window_orientation_aggregation_differs_from_total_and_independent_side():
    class Model:
        def predict_proba(self, frame, *, thread_count):
            assert thread_count == 1
            p = np.array([.8, .6, .3, .2])
            return np.column_stack([1-p, p])
    data = {"X_baseline": np.zeros((2, 2, 20)), "y": np.zeros((2, len(TARGETS), 2))}
    data["y"][:, 0, :] = [[1, 0], [0, 1]]
    maps, sides = np.array([0, 0, 1, 1]), np.array([0, 1, 0, 1])
    lead, unique, labels = _raw_prediction_for_target(Model(), data, "baseline", "lead_5_15", maps, sides)
    assert lead == pytest.approx([.6, .55])
    assert unique.tolist() == [0, 1] and labels.tolist() == [1, 0]
    total, _, _ = _raw_prediction_for_target(Model(), data, "baseline", "total55", maps, sides)
    assert total == pytest.approx([.7, .25])
    side, _, _ = _raw_prediction_for_target(Model(), data, "baseline", "side30", maps, sides)
    assert side == pytest.approx([.8, .6, .3, .2])


def test_build_rejects_duplicate_rich_mid_even_when_it_overlaps_db(tmp_path):
    db = tmp_path / "timeline.sqlite3"; conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE matches (match_id INTEGER PRIMARY KEY, status TEXT, radiant_score INTEGER, dire_score INTEGER)")
    conn.execute("CREATE TABLE match_windows (match_id INTEGER PRIMARY KEY, duration INTEGER, start_time INTEGER, series_id INTEGER, radiant_team_id INTEGER, dire_team_id INTEGER)")
    columns = ["match_id INTEGER", "row_index INTEGER", "side TEXT", "account_id INTEGER", "hero_id INTEGER", "kills INTEGER", "deaths INTEGER", "assists INTEGER", "gold_per_min REAL", "xp_per_min REAL"]
    columns += [f"{metric}_{boundary} REAL" for metric in ("deaths", "gold", "xp", "lh", "dn") for boundary in BOUNDARIES]
    conn.execute(f"CREATE TABLE player_windows ({', '.join(columns)})")
    conn.execute("INSERT INTO matches VALUES (1, 'complete', 30, 20)"); conn.execute("INSERT INTO match_windows VALUES (1, 1000, 10, 0, 11, 22)")
    for slot in range(10):
        values = [1, slot, "radiant" if slot < 5 else "dire", slot + 1, slot + 101, 3, 2, 4, 400, 500]
        values += [boundary / 300 for _metric in ("deaths", "gold", "xp", "lh", "dn") for boundary in BOUNDARIES]
        conn.execute(f"INSERT INTO player_windows VALUES ({','.join('?' for _ in values)})", values)
    conn.commit(); conn.close()
    rich = tmp_path / "duplicate.npz"
    np.savez(rich, mids=np.array([1, 1]), ts=np.array([10, 10]), durations=np.array([1000, 1000]), sids=np.zeros(2),
             heroes=np.array([[101 + slot for slot in range(10)]] * 2), accounts=np.array([[slot + 1 for slot in range(10)]] * 2),
             pstats=np.zeros((2, 10, 14), dtype=np.float32), pstat_names=np.array(["kills", "deaths", "assists", "goldPerMinute", "experiencePerMinute", "networth", "numLastHits", "numDenies", "heroDamage", "towerDamage", "heroHealing", "imp", "level", "dotaPlusHeroXp"]))
    with pytest.raises(ValueError, match="duplicate match_id 1"):
        build_dataset(db, rich, tmp_path / "built")


@pytest.mark.parametrize("target", ["side30", "total55", "lead_5_15"])
def test_saved_model_replay_smoke(tmp_path, target):
    # Five fixed date buckets, two maps each, with both side labels.  This is a
    # reusable artifact contract test, not a claim about corpus quality.
    starts = np.array([1780310400, 1780396800, 1785712000, 1785798400, 1786490000, 1786576400, 1787440400, 1787526800, 1788390800, 1788477200], dtype=np.int64)
    # The exact calendar values are immaterial; fixed_splits receives a stored split
    # below so CatBoost smoke stays small and deterministic.
    n = len(starts); rng = np.random.default_rng(8)
    base = rng.normal(size=(n, 2, 20)).astype(np.float32)
    base[:, :, :10] = rng.integers(1, 30, size=(n, 2, 10))
    y = np.zeros((n, len(TARGETS), 2), dtype=np.float32)
    for row in range(n):
        y[row, :, 0] = row % 2; y[row, :, 1] = 1 - (row % 2)
    y[:, TARGETS.index("total55"), 1] = y[:, TARGETS.index("total55"), 0]
    dataset = tmp_path / "dataset.npz"; meta = tmp_path / "metadata.json"
    atomic_npz(dataset, X_baseline=base, X_timeline=np.zeros((n, 2, 4), dtype=np.float32),
               X_experience=np.zeros((n, 2, 4), dtype=np.float32), X_dpxp=np.zeros((n, 2, 2), dtype=np.float32),
               X_recent=np.zeros((n, 2, 30), dtype=np.float32),
               y=y, mids=np.arange(n), starts=starts, ends=starts + 60, series_ids=np.zeros(n),
               split=np.repeat(np.arange(5, dtype=np.int8), 2))
    atomic_json(meta, {"schema": "test", "source_hashes": {"db": "x"}, "features": {"baseline": [], "timeline": [], "experience": [], "dpxp": [], "recent": []}})
    report = train_target(dataset, meta, target, tmp_path / "trained", threads=1)
    assert report["target"] == target
    predictions = np.load(tmp_path / "trained" / "predictions.npz")
    assert len(predictions["y"]) == (4 if target == "side30" else 2)
    assert predictions["sides"].tolist() == ([0, 1, 0, 1] if target == "side30" else [-1, -1] if target == "total55" else [0, 0])
    schema = json.loads((tmp_path / "trained" / "schema.json").read_text())
    assert "total55_rule" not in schema
    if target.startswith("lead_"):
        assert "no tie" in schema["orientation_rule"]
    assert (tmp_path / "trained" / "model.cbm").exists()
    assert (tmp_path / "trained" / "predictions.npz").exists()
    assert {path.stem for path in (tmp_path / "trained" / "candidates").glob("*.cbm")} == set(ARMS)
    assert "combined_dpxp" not in ARMS
