"""Offline full/no-org refit from an audited causal matrix; never deploys."""
from __future__ import annotations

import numpy as np
from scipy.optimize import minimize
from scipy.special import expit

from base.tools.refit_prematch_draft_component import (
    BranchWeights, pack_branches, unpack_branches,
)


def chronological_masks(ts, ends, sids, cutoff, embargo=3600):
    ts, ends, sids = map(np.asarray, (ts, ends, sids))
    if (ts.ndim != 1 or ts.shape != ends.shape or ts.shape != sids.shape
            or not all(np.isfinite(v).all() for v in (ts, ends, sids))
            or np.any(ends <= ts) or np.any(sids <= 0) or embargo < 0):
        raise ValueError("invalid timestamps, series or embargo")
    before = ts < cutoff
    cross = np.intersect1d(sids[before], sids[~before])
    keep = ~np.isin(sids, cross)
    return keep & before & (ends + embargo < cutoff), keep & ~before


def probability(X, branch):
    z = np.sum(((X - branch.mu) / branch.sd) * branch.coef, axis=1)
    return np.clip(expit(z + branch.intercept), 1e-6, 1 - 1e-6)


def fit(X, y, c):
    """Same unpenalized-intercept L2 logistic objective as sklearn C.

    Explicit reductions avoid the NumPy/BLAS warnings observed in E286.
    Convergence and the final objective gradient are checked independently.
    """
    X, y = np.asarray(X, dtype=float), np.asarray(y, dtype=float)
    if (X.ndim != 2 or y.shape != (len(X),) or not np.isfinite(X).all()
            or set(np.unique(y)) != {0., 1.} or not np.isfinite(c) or c <= 0):
        raise ValueError("invalid training matrix, labels or regularization")
    mu, sd = X.mean(axis=0), X.std(axis=0)
    sd = np.where(sd < 1e-9, 1., sd)
    Z = (X - mu) / sd

    def objective(w):
        z = np.sum(Z * w[:-1], axis=1) + w[-1]
        residual = expit(z) - y
        value = np.mean(np.logaddexp(0., z) - y * z) + np.sum(w[:-1]**2) / (2*c*len(y))
        gradient = np.r_[np.mean(Z * residual[:, None], axis=0) + w[:-1]/(c*len(y)),
                         residual.mean()]
        return value, gradient

    result = minimize(objective, np.zeros(X.shape[1] + 1), jac=True, method="L-BFGS-B",
                      options={"maxiter": 5000, "gtol": 1e-9, "ftol": 1e-14})
    value, gradient = objective(result.x)
    if (not result.success or not np.isfinite(result.x).all() or not np.isfinite(value)
            or np.max(np.abs(gradient)) > 1e-6):
        raise ValueError(f"unconverged logistic fit: {result.message}")
    return BranchWeights([], mu, sd, result.x[:-1].copy(), float(result.x[-1])), {
        "iterations": int(result.nit), "gradient_max": float(np.max(np.abs(gradient))),
        "objective": float(value), "C": float(c),
    }


def retrain(matrix, weights, *, cutoff, c, embargo=3600):
    """Fit full on finite rows and no_org on its routed population.

    The caller must exclude reserved/public IDs and bind the matrix, context
    scaling and draft backbone to an immutable provenance manifest.
    """
    X, y = np.asarray(matrix['X']), np.asarray(matrix['y'])
    names = list(map(str, matrix['feature_names']))
    if (names != list(map(str, weights['feature_names'])) or len(names) != len(set(names))
            or X.shape != (len(y), len(names)) or len(np.unique(matrix['mids'])) != len(y)
            or not set(np.unique(y)) <= {0, 1}):
        raise ValueError("matrix schema, labels or row identity mismatch")
    train, test = chronological_masks(matrix['ts'], matrix['ends'], matrix['sids'], cutoff, embargo)
    eligible = np.isfinite(X).all(axis=1)
    routed = np.asarray(matrix['routed_branch'])
    if routed.shape != y.shape or not set(routed[eligible]) <= {'full', 'no_org'}:
        raise ValueError("invalid routed branch population")
    train, test = train & eligible, test & eligible
    if min(train.sum(), test.sum()) < 2 or any(len(np.unique(y[m])) != 2 for m in (train, test)):
        raise ValueError("insufficient common training or test rows")
    branches = unpack_branches(weights)
    old = unpack_branches(weights)
    if branches['full'].cols != names:
        raise ValueError("full branch differs from matrix feature order")
    predictions, optimization = {}, {}
    for name in ('full', 'no_org'):
        cols = branches[name].cols
        indices = [names.index(col) for col in cols]
        branch_train = train if name == 'full' else train & (routed == name)
        fitted, optimization[name] = fit(X[branch_train][:, indices], y[branch_train], c)
        optimization[name]['fit_rows'] = int(branch_train.sum())
        optimization[name]['routed_test_rows'] = int((test & (routed == name)).sum())
        fitted.cols = list(cols)
        branches[name] = fitted
        predictions[name] = probability(X[test][:, indices], fitted)
        predictions['parent_' + name] = probability(X[test][:, indices], old[name])
    full = branches['full']
    arrays = {key: np.asarray(weights[key]).copy() for key in
              ('feature_names', 'ctx_mu', 'ctx_sd')}
    arrays.update(mu=full.mu[None, :], sd=full.sd[None, :], coef=full.coef[None, :],
                  intercept=np.array([full.intercept]), **pack_branches(branches))
    return arrays, predictions, train, test, optimization
