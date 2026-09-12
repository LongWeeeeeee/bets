import numpy as np
import pytest
from sklearn.linear_model import LogisticRegression

from base.tools.refit_prematch_draft_component import BranchWeights, pack_branches, unpack_branches
from base.tools.retrain_prematch_general import chronological_masks, fit, probability, retrain


def fixture():
    rng = np.random.default_rng(287)
    X = rng.normal(size=(120, 3))
    y = (X[:, 0] + rng.normal(size=120) > 0).astype(int)
    names = ['elo', 'draft_logit', 'h2h_resid']
    branches = {name: BranchWeights(cols, np.zeros(len(cols)), np.ones(len(cols)),
                                   np.zeros(len(cols)), .2)
                for name, cols in [('full', names), ('no_org', names[:2]),
                                   ('pre_draft', names[:1]), ('no_account', names[1:]),
                                   ('no_account_no_org', names[1:2]), ('rating_only', names[:1])]}
    weights = dict(feature_names=np.array(names), ctx_mu=np.array([7., 8.]), ctx_sd=np.array([2., 3.]),
                   **pack_branches(branches))
    matrix = dict(X=X, y=y, mids=np.arange(120), ts=np.arange(120)*10,
                  ends=np.arange(120)*10+5, sids=np.arange(120)+1, feature_names=np.array(names),
                  routed_branch=np.where(np.arange(120) % 2 == 0, 'full', 'no_org'))
    return matrix, weights


def test_end_embargo_and_boundary_series_are_excluded():
    train, test = chronological_masks([10, 20, 80, 110, 120], [15, 25, 95, 115, 125],
                                     [1, 2, 3, 2, 4], 100, 5)
    assert np.flatnonzero(train).tolist() == [0]
    assert np.flatnonzero(test).tolist() == [4]


def test_holdout_labels_cannot_change_weights_and_fallback_is_preserved():
    matrix, weights = fixture()
    result, _, train, test, _ = retrain(matrix, weights, cutoff=800, c=.1, embargo=0)
    changed = {**matrix, 'y': matrix['y'].copy()}
    changed['y'][test] = 1-changed['y'][test]
    again, *_ = retrain(changed, weights, cutoff=800, c=.1, embargo=0)
    for key in result:
        np.testing.assert_array_equal(result[key], again[key])
    a, b = unpack_branches(result), unpack_branches(weights)
    for name in ('pre_draft', 'no_account', 'no_account_no_org', 'rating_only'):
        for key in ('cols', 'mu', 'sd', 'coef', 'intercept'):
            np.testing.assert_array_equal(getattr(a[name], key), getattr(b[name], key))
    np.testing.assert_array_equal(result['coef'][0], a['full'].coef)
    np.testing.assert_array_equal(result['ctx_mu'], weights['ctx_mu'])
    assert not np.any(train & test)


def test_full_only_training_labels_cannot_affect_no_org_weights():
    matrix, weights = fixture()
    result, _, train, _, _ = retrain(matrix, weights, cutoff=800, c=.1, embargo=0)
    changed = {**matrix, 'y': matrix['y'].copy()}
    selected = train & (matrix['routed_branch'] == 'full')
    changed['y'][selected] = 1 - changed['y'][selected]
    again, *_ = retrain(changed, weights, cutoff=800, c=.1, embargo=0)
    a, b = unpack_branches(result)['no_org'], unpack_branches(again)['no_org']
    for key in ('mu', 'sd', 'coef', 'intercept'):
        np.testing.assert_array_equal(getattr(a, key), getattr(b, key))


def test_fit_agrees_with_independent_sklearn_optimizer():
    matrix, _ = fixture()
    branch, report = fit(matrix['X'], matrix['y'], .1)
    Z = (matrix['X']-branch.mu)/branch.sd
    model = LogisticRegression(C=.1, tol=1e-10, max_iter=5000).fit(Z, matrix['y'])
    np.testing.assert_allclose(probability(matrix['X'], branch), model.predict_proba(Z)[:, 1], atol=1e-6)
    assert report['gradient_max'] < 1e-6


def test_duplicate_ids_and_infinite_fit_fail_closed():
    matrix, weights = fixture()
    matrix['mids'][1] = matrix['mids'][0]
    with pytest.raises(ValueError, match='identity'):
        retrain(matrix, weights, cutoff=800, c=.1, embargo=0)
    with pytest.raises(ValueError, match='training'):
        fit([[float('inf')], [1]], [0, 1], .1)
