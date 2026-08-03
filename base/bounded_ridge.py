from __future__ import annotations

import numpy as np
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.linear_model import Ridge


class BoundedRidgeRegressor(BaseEstimator, RegressorMixin):
    def __init__(self, estimator: Ridge):
        self.estimator = estimator

    def fit(self, X: np.ndarray, y: np.ndarray) -> "BoundedRidgeRegressor":
        self.estimator.fit(X, y)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return np.clip(np.asarray(self.estimator.predict(X), dtype=float), 0.0, 1.0)
