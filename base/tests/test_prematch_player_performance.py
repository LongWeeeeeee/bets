"""Regression examples for missing inputs and prematch temporal boundaries."""
import sqlite3
from types import SimpleNamespace

import numpy as np
import pytest

from base.tools import prematch_player_performance as perf


def source(rows):
    return np.asarray([(a, h, end, mid, np.repeat(value, 8)) for a, h, end, mid, value in rows], dtype=perf.SOURCE)


def test_rolling_excludes_current_equal_end_future_and_other_accounts():
    s = source([(7, 1, 99, 10, 2), (7, 2, 100, 11, 900), (7, 1, 101, 12, 800),
                (8, 1, 90, 9, 1000)])
    q = (np.array([7, 7, 9]), np.array([1, 2, 1]), np.array([100, 100, 100]))
    values, count, games = perf.rolling(s, *q)
    np.testing.assert_equal(values[:2], 2)
    np.testing.assert_equal(count[:2], 1)
    assert np.isnan(values[2]).all() and games.tolist() == [1, 1, 0]
    hero, count, games = perf.rolling(s, *q, hero=True)
    assert hero[0, 0] == 2 and np.isnan(hero[1:]).all()
    assert games.tolist() == [1, 0, 0]


def test_window_is_last_twenty_maps_with_per_metric_missing_counts():
    s = source([(7, 1, i + 1, i + 1, float(i)) for i in range(25)])
    s['values'][10, 0] = np.nan
    s['values'][11, 1] = 0  # real zero must be counted
    s = s[::-1]  # file order is not chronological
    values, counts, games = perf.rolling(s, np.array([7]), np.array([1]), np.array([99]))
    assert games[0] == 20 and counts[0, 0] == 19 and counts[0, 1] == 20
    assert values[0, 0] == pytest.approx((sum(range(5, 25)) - 10) / 19)
    assert values[0, 1] == pytest.approx((sum(range(5, 25)) - 11) / 20)


def test_grouped_rolling_matches_bruteforce_random_fixture():
    rng = np.random.default_rng(20260921)
    s = source([(int(rng.integers(1, 8)), int(rng.integers(1, 4)), int(rng.integers(1, 50)),
                 i, float(rng.integers(0, 100))) for i in range(240)])
    s['values'][::7, 0] = np.nan
    qa, qh, qt = rng.integers(1, 10, 80), rng.integers(1, 4, 80), rng.integers(1, 60, 80)
    for by_hero in (False, True):
        values, counts, games = perf.rolling(s, qa, qh, qt, hero=by_hero)
        for i in range(len(qa)):
            rows = s[(s['account'] == qa[i]) & (s['end'] < qt[i])]
            if by_hero:
                rows = rows[rows['hero'] == qh[i]]
            rows = rows[np.lexsort((rows['mid'], rows['end']))][-20:]
            assert games[i] == len(rows)
            for j in range(8):
                v = rows['values'][:, j]; v = v[np.isfinite(v)]
                assert counts[i, j] == len(v)
                if len(v): assert values[i, j] == pytest.approx(v.mean(), abs=1e-5)
                else: assert np.isnan(values[i, j])


def test_raw_missing_is_distinct_from_zero_and_totals_are_rates():
    values = perf.performance_values({'experiencePerMinute': 500, 'networth': 10000,
                                      'numDenies': 0, 'heroHealing': None, 'heroDamage': -1,
                                      'towerDamage': float('inf'), 'level': 20}, 1200)
    np.testing.assert_equal(values[[0, 1, 3, 7]], [500, 500, 0, 20])
    assert np.isnan(values[[2, 4, 5, 6]]).all()


def fixture_db(tmp_path, *, duration=1000, account=1, hero=1, side='radiant', won=1):
    path = tmp_path / 'source.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('create table matches(match_id,response_start_time,duration,radiant_win,status)')
        db.execute('insert into matches values(10,100,?,?,\'partial\')', (duration, won))
        db.execute('create table player_timelines(match_id,account_id,hero_id,side,row_index,' +
                   ','.join(perf.LANE) + ')')
        db.execute('insert into player_timelines values(' + ','.join(['?'] * 13) + ')',
                   (10, account, hero, side, 0, *([20] * 8)))
    data = dict(mid=np.array([10]), ts=np.array([100]), end=np.array([1100]), y=np.array([1]),
                accounts=np.arange(1, 11)[None, :], heroes=np.arange(1, 11)[None, :])
    return path, data


@pytest.mark.parametrize('field,value', [('duration', 999), ('account', 11), ('hero', 12),
                                       ('side', 'dire'), ('side', 'unknown'), ('won', 0)])
def test_lane_rejects_cross_source_conflicts(tmp_path, field, value):
    path, data = fixture_db(tmp_path, **{field: value})
    with pytest.raises(ValueError):
        perf.lane_source(path, data)


def test_lane_keeps_partial_missing_and_masks_unfinished_windows(tmp_path):
    path, data = fixture_db(tmp_path, duration=500)
    data['end'][0] = 600
    with sqlite3.connect(path) as db:
        db.execute('update player_timelines set gold_t_300=NULL,lh_t_300=0')
    rows, audit = perf.lane_source(path, data)
    assert np.isnan(rows['values'][0, 0])
    assert rows['values'][0, 4] == 0
    assert np.isnan(rows['values'][0, 1::2]).all()
    assert audit == {'maps': 1, 'player_rows': 1}


def test_role_direction_and_hero_shrinkage_use_only_previous_maps():
    d = dict(mid=np.array([10]), ts=np.array([100]), accounts=np.arange(1, 11)[None, :],
             heroes=np.ones((1, 10), dtype=int))
    s = source([(a, 1, 90, 1, float(a)) for a in range(1, 11)] + [(1, 2, 95, 2, 11.)])
    values, names = perf.project(s, d, 'performance')
    col = dict(zip(names, values[0]))
    assert col['performance_experiencePerMinute_player20_R1'] == 6
    assert col['performance_experiencePerMinute_player20_diff1'] == 0
    # Hero delta for player1: (1-6)*1/(1+10); other four Radiant players zero.
    assert col['performance_experiencePerMinute_hero20_delta_Rmean'] == pytest.approx(-1 / 11)
    assert len(names) == len(set(names)) == 160


def test_raw_reparse_preserves_missing_and_incomplete_roster(tmp_path):
    import json
    from base.tools import prematch_pro_player_history as history
    raw = tmp_path / 'raw'; raw.mkdir()
    match = {'id': 10, 'startDateTime': 100, 'durationSeconds': 1000, 'didRadiantWin': True,
             'players': [{'steamAccount': {'id': 1}, 'heroId': 1, 'isRadiant': True,
                          'experiencePerMinute': 400, 'numDenies': 0},
                         {'steamAccount': {'id': 1}, 'heroId': 1, 'isRadiant': None,
                          'experiencePerMinute': 900}]}
    (raw / 'a.json').write_text(json.dumps({'10': match}))
    candidate = tmp_path / 'candidates.bin'
    np.asarray([history._record_from_raw('10', match, 0, 0)], dtype=history.SOURCE_DTYPE).tofile(candidate)
    d = dict(X=np.ones((1, 1)), feature_names=np.array(['test']), mid=np.array([10]),
             ts=np.array([100]), end=np.array([1100]), y=np.array([1]), league=np.array([1]),
             elo_eligible=np.array([True]), accounts=np.arange(1, 11)[None, :],
             heroes=np.arange(1, 11)[None, :])
    out = tmp_path / 'source.bin'
    audit = perf.raw_source(raw, candidate, d, out)
    s = np.fromfile(out, dtype=perf.SOURCE)
    assert len(s) == audit['player_rows'] == 1
    assert s['values'][0, 0] == 400 and s['values'][0, 3] == 0
    assert np.isnan(s['values'][0, 1:3]).all()
    match['players'][0]['heroId'] = 2
    (raw / 'a.json').write_text(json.dumps({'10': match}))
    with pytest.raises(ValueError, match='no longer matches'):
        perf.raw_source(raw, candidate, d, tmp_path / 'bad.bin')


def test_build_refreshes_canonical_index_when_new_file_precedes_old_files(tmp_path):
    import json
    path, data = fixture_db(tmp_path)
    data.update(X=np.ones((1, 1), dtype=np.float32), feature_names=np.array(['base']),
                league=np.array([1]), elo_eligible=np.array([True]),
                label_side=np.array('radiant'), hero_slots=np.array('radiant_1_5,dire_1_5'),
                k24_direction=np.array('radiant_minus_dire'),
                role_assignment=np.array('past_completed_player_roles'))
    dataset = tmp_path / 'dataset.npz'; np.savez(dataset, **data)
    raw = tmp_path / 'raw'; raw.mkdir()
    def record(mid, start, duration, xpm):
        return {'id': mid, 'startDateTime': start, 'durationSeconds': duration, 'didRadiantWin': True,
                'players': [{'steamAccount': {'id': 1}, 'heroId': 1, 'isRadiant': True,
                             'experiencePerMinute': xpm}]}
    (raw / 'z.json').write_text(json.dumps({'9': record(9, 1, 20, 400)}))
    # New file shifts the old ordinals; the builder must read both from scratch.
    (raw / 'a.json').write_text(json.dumps({'8': record(8, 25, 25, 600)}))
    out = tmp_path / 'output'; out.mkdir()
    result = perf.build(SimpleNamespace(dataset=dataset, raw_dir=raw, database=path, output_dir=out))
    assert result['performance']['canonical_maps'] == 2
    with np.load(out / 'performance.npz') as z:
        j = z['feature_names'].tolist().index('performance_experiencePerMinute_player20_R1')
        assert z['X'][0, j] == 500
    assert (out / 'performance_source.bin').exists()
    assert not (out / 'performance_source.bin.tmp').exists()
