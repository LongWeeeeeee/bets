"""Causal/context regressions for the E313 winner experiment."""
import numpy as np
import pytest

from base.tools import prematch_player_context as m
from base.tools import prematch_player_performance as old


def fixture(n=12):
    ts = 1780000000 + np.arange(n) * 7200
    data = dict(mid=9006708087 + np.arange(n), ts=ts, end=ts + 3600,
                heroes=np.tile(np.arange(1, 11), (n, 1)),
                accounts=np.tile(np.arange(101, 111), (n, 1)),
                y=np.arange(n) % 2, k24_diff=np.linspace(-100, 100, n),
                elo_eligible=np.ones(n, bool))
    values = np.random.default_rng(12).uniform(0, 100, (n, 10, 8)).astype(np.float32)
    return data, values


def oracle(keys, ends, mids, values, qkeys, qtimes, window):
    means = np.full((len(qkeys), values.shape[1]), np.nan)
    counts = np.zeros_like(means)
    games = np.zeros(len(qkeys)); last = np.full(len(qkeys), np.nan)
    for i, (key, ts) in enumerate(zip(qkeys, qtimes)):
        rows = np.flatnonzero((keys == key) & (ends < ts))
        rows = sorted(rows, key=lambda r: (ends[r], mids[r]))[-window:]
        games[i] = len(rows)
        if rows:
            last[i] = ends[rows[-1]]
        for j in range(values.shape[1]):
            v = values[rows, j]
            v = v[np.isfinite(v)]
            counts[i, j] = len(v)
            if len(v):
                means[i, j] = v.mean()
    return means, counts, games, last


@pytest.mark.parametrize('window', [1, 5, 20, 500])
def test_rolling_matches_direct_group_oracle(window):
    rng = np.random.default_rng(123)
    keys = rng.integers(1, 8, 300); ends = rng.integers(1, 40, 300)
    mids = np.arange(300); values = rng.normal(size=(300, 5))
    values[rng.random(values.shape) < .15] = np.nan
    qkeys = np.r_[np.zeros(5, int), rng.integers(1, 10, 95)]
    qtimes = rng.integers(1, 45, 100)
    actual = m.rolling(keys, ends, mids, values, qkeys, qtimes, window)
    expected = oracle(keys, ends, mids, values, qkeys, qtimes, window)
    for a, b in zip(actual, expected):
        np.testing.assert_allclose(a, b, atol=1e-6, equal_nan=True)


def test_exact_completion_boundary_and_zero():
    a = m.rolling(np.array([1, 1, 1]), np.array([10, 20, 30]), np.arange(3),
                  np.array([[0., np.nan], [10., 5.], [100., 20.]]),
                  np.array([1, 1]), np.array([20, 21]), 20)
    np.testing.assert_allclose(a[0], [[0., np.nan], [5., 5.]], equal_nan=True)
    np.testing.assert_array_equal(a[1], [[1, 0], [2, 1]])


def test_patch_boundary_and_unknown():
    releases = sorted(p.release_ts for p in m.PATCH_RELEASES)
    assert m.patch_ids(np.array([releases[0] - 1, releases[0], releases[0] + 1])).tolist() == [0, 1, 1]


def test_slot_direction_preserves_individuals():
    x, names = m.slots(np.arange(10), 'x')
    np.testing.assert_array_equal(x, [[0, -5, 1, -5, 2, -5, 3, -5, 4, -5]])
    assert names[-1] == 'x_diff5'


def test_join_identity_and_player_order():
    data, values = fixture(2)
    source = np.array([(data['accounts'][r, s], data['heroes'][r, s], data['end'][r], data['mid'][r], values[r, s])
                       for r in range(2) for s in range(10)], dtype=old.SOURCE)[::-1]
    result, audit = m.join_performance(source, data)
    np.testing.assert_array_equal(result, values)
    assert audit == {'joined_player_rows': 20, 'missing_player_rows': 0}
    changed = source.copy(); changed['hero'][0] = 999
    with pytest.raises(ValueError, match='hero/end'):
        m.join_performance(changed, data)
    with pytest.raises(ValueError, match='duplicate'):
        m.join_performance(np.r_[source, source[:1]], data)


def test_normalization_matches_direct_prior_hero_role_mean():
    data, values = fixture(5)
    observed = m.contextual_values(data, values).reshape(5, 10, 5)
    raw = np.log1p(values[:, :, m.METRIC_INDICES].astype(float))
    diff = raw - raw[:, np.r_[5:10, 0:5], :]
    assert np.isnan(observed[0, :, :4]).all()
    for i in range(1, 5):
        # All maps same patch/hero/role; patch and global priors coincide.
        np.testing.assert_allclose(observed[i, :, :4], diff[i] - diff[:i].mean(axis=0), atol=1e-6)
    expected = data['y'] - m.winner.expit(data['k24_diff'] * np.log(10.) / 400.)
    np.testing.assert_allclose(observed[:, :5, 4], np.repeat(expected[:, None], 5, axis=1))
    np.testing.assert_allclose(observed[:, 5:, 4], np.repeat(-expected[:, None], 5, axis=1))


def test_current_and_future_stats_and_results_do_not_change_past_features():
    data, values = fixture()
    obs = m.contextual_values(data, values)
    before, names = m.context(data, obs)
    modified = {k: v.copy() for k, v in data.items()}
    modified['y'][6:] = 1 - modified['y'][6:]
    changed = values.copy(); changed[6:] *= 1000
    after, names2 = m.context(modified, m.contextual_values(modified, changed))
    assert names == names2 and len(names) == 190
    np.testing.assert_array_equal(before[:7], after[:7])
    assert not np.allclose(before[7:], after[7:], equal_nan=True)


def test_individual_restores_e311_team_residual_exactly():
    data, values = fixture(5)
    data['heroes'][1, [0, 5]] += 20
    data['heroes'][2, [2, 8]] += 20
    source = np.array([(data['accounts'][r, s], data['heroes'][r, s], data['end'][r], data['mid'][r], values[r, s])
                       for r in range(5) for s in range(10)], dtype=old.SOURCE)
    individual, names = m.individual(source, data)
    prior, previous_names = old.project(source, data, 'performance')
    assert individual.shape == (5, 80)
    for metric in range(8):
        r = individual[:, metric*10:metric*10+10:2]
        diff = individual[:, metric*10+1:metric*10+10:2]
        # First row is completely missing; later ones reconstruct E311 mean.
        np.testing.assert_allclose(r[1:].mean(axis=1), prior[1:, metric*14+12], atol=1e-5)
        np.testing.assert_allclose(diff[1:].mean(axis=1), prior[1:, metric*14+13], atol=1e-5)
    assert np.nanmax(np.abs(individual)) > 0


def test_context_account_hero_and_role_isolation():
    data, values = fixture(3)
    obs = np.ones((30, 5), dtype=np.float32)
    # New player and new role history must be missing, not borrowed from others.
    data['accounts'][2, 0] = 10001
    data['accounts'][2, [1, 2]] = data['accounts'][2, [2, 1]]
    x, names = m.context(data, obs)
    assert np.isnan(x[2, [names.index('context_player20_xpm_R1'),
                          names.index('context_player20_xpm_R2'),
                          names.index('context_player20_xpm_R3')]]).all()


def test_swapping_sides_preserves_player_values_and_reverses_difference():
    data, values = fixture(5)
    original, names = m.context(data, m.contextual_values(data, values))
    swap = np.r_[5:10, 0:5]
    flipped = {k: v.copy() for k, v in data.items()}
    for k in ('accounts', 'heroes'):
        flipped[k] = flipped[k][:, swap]
    flipped['y'] = 1 - flipped['y']; flipped['k24_diff'] *= -1
    result, _ = m.context(flipped, m.contextual_values(flipped, values[:, swap]))
    np.testing.assert_allclose(result[:, 1::2], -original[:, 1::2], atol=1e-6, equal_nan=True)
    np.testing.assert_allclose(result[:, ::2], original[:, ::2] - original[:, 1::2], atol=1e-6, equal_nan=True)
