"""Offline draft-only phase predictors; no production artifact is selected here."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class DraftPhaseModel:
    """Early NW exposes [Dire, Radiant, no marker], all other phases [Dire, Radiant].

    Occurrence is side-invariant and direction is side-sensitive. A third class
    on signed features alone cannot learn a side-invariant draft-dependent
    occurrence probability, hence the two explicitly factored classifiers.
    """

    phase: str
    encoder: object
    classifier: object
    occurrence_encoder: object = None
    occurrence_classifier: object = None

    @property
    def classes_(self):
        return np.asarray(["dire", "radiant", "no_marker"] if self.phase == "early_nw"
                          else ["dire", "radiant"])

    def predict_proba(self, heroes, chunk_size=25000):
        heroes = np.asarray(heroes)
        if heroes.ndim != 2 or heroes.shape[1] != 10:
            raise ValueError("heroes must be (n, 10), Radiant positions 1-5 then Dire 1-5")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        result = np.empty((len(heroes), len(self.classes_)), dtype=np.float64)
        for start in range(0, len(heroes), chunk_size):
            h = heroes[start:start + chunk_size]
            p = self.classifier.predict_proba(self.encoder.transform(h))[:, 1]
            if self.phase == "early_nw":
                q = self.occurrence_classifier.predict_proba(
                    self.occurrence_encoder.transform(h))[:, 1]
                result[start:start + len(h)] = np.column_stack((q * (1-p), q * p, 1-q))
            else:
                result[start:start + len(h)] = np.column_stack((1-p, p))
        if not np.isfinite(result).all():
            raise ValueError("non-finite prediction")
        return result
