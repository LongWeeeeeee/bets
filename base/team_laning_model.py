"""Draft and causal-history model for the signed team net worth at minute 10.

The class order is always Dire lead, exact tie, Radiant lead.  Unlike the
per-lane model, each feature row represents one complete map.
"""
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier


CLASS_NAMES = ("dire", "tie", "radiant")
FEATURE_SET_DRAFT = "team_draft_v1"
FEATURE_SET_HISTORY = "team_history_recent_v1"


def team_labels(team_nw10):
    """Map finite signed team-NW10 values to Dire/tie/Radiant class IDs."""
    values = np.asarray(team_nw10, dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("team_nw10 target must be finite")
    return np.where(values < 0, 0, np.where(values > 0, 2, 1)).astype(np.int8)


def _validated_heroes(heroes):
    heroes = np.asarray(heroes)
    if heroes.ndim != 2 or heroes.shape[1] != 10:
        raise ValueError("heroes must have shape (maps,10) in R1..5,D1..5 role order")
    try:
        numeric = heroes.astype(float)
    except (TypeError, ValueError) as exc:
        raise ValueError("hero ids must be finite integers") from exc
    if (not np.isfinite(numeric).all() or
            not np.equal(numeric, np.floor(numeric)).all()):
        raise ValueError("hero ids must be finite integers")
    heroes = numeric.astype(np.int64)
    if np.any(heroes <= 0) or np.any(heroes >= 1024):
        raise ValueError("hero ids must be within 1..1023")
    if any(len(set(row)) != 10 for row in heroes):
        raise ValueError("each map must have ten distinct heroes")
    return heroes


def team_features(heroes, history=None):
    """Return full-draft features, optionally flattened causal hc(10,12).

    History must have been computed before this map using a delayed, 30-day
    causal window.  Current map outcomes are intentionally never accepted.
    """
    heroes = _validated_heroes(heroes)
    columns = {f"hero_{slot}": heroes[:, slot].astype(str) for slot in range(10)}
    if history is None:
        return pd.DataFrame(columns)
    history = np.asarray(history)
    if history.shape != (len(heroes), 10, 12) or not np.isfinite(history).all():
        raise ValueError("history must be finite with shape (maps,10,12)")
    for slot in range(10):
        for stat in range(12):
            columns[f"history_{slot}_{stat}"] = history[:, slot, stat]
    differences = history[:, :5] - history[:, 5:]
    for role in range(5):
        for stat in range(12):
            columns[f"radiant_minus_dire_role_{role}_{stat}"] = differences[:, role, stat]
    return pd.DataFrame(columns)


def temperature_scale(probabilities, temperature):
    probabilities = np.asarray(probabilities, dtype=float)
    if probabilities.ndim != 2 or probabilities.shape[1] != 3:
        raise ValueError("probabilities must have shape (maps,3)")
    temperature = float(temperature)
    if not np.isfinite(temperature) or temperature <= 0:
        raise ValueError("temperature must be finite and positive")
    logits = np.log(np.maximum(probabilities, np.finfo(np.float64).tiny)) / temperature
    logits -= logits.max(axis=1, keepdims=True)
    result = np.exp(logits)
    return result / result.sum(axis=1, keepdims=True)


class TeamLaningModel:
    """Explicit serialized contract for the independent team-NW10 model."""

    def __init__(self, model, with_history, temperature=1.0,
                 availability_delay_seconds=0, recent_window_seconds=None):
        self.model = model
        self.with_history = bool(with_history)
        self.temperature = float(temperature)
        if not np.isfinite(self.temperature) or self.temperature <= 0:
            raise ValueError("temperature must be finite and positive")
        self.history_config = {
            "availability_delay_seconds": float(availability_delay_seconds),
            "recent_window_seconds": (None if recent_window_seconds is None
                                       else float(recent_window_seconds)),
        }
        delay, window = self.history_config.values()
        if (not np.isfinite(delay) or delay < 0 or
                (window is not None and (not np.isfinite(window) or window < 0))):
            raise ValueError("invalid team-laning history metadata")
        if self.with_history and (window is None or window <= 0):
            raise ValueError("history artifact requires positive recent-window metadata")

    def predict_proba(self, heroes, history=None):
        if self.with_history and history is None:
            raise ValueError("This artifact requires causal history (maps,10,12)")
        result = self.model.predict_proba(team_features(heroes, history if self.with_history else None),
                                          thread_count=1)
        return temperature_scale(result, self.temperature)

    @classmethod
    def load(cls, directory):
        model = CatBoostClassifier()
        model.load_model(str(Path(directory) / "team.cbm"))
        metadata = model.get_metadata()
        feature_set = str(metadata.get("team_laning_feature_set", ""))
        if feature_set not in (FEATURE_SET_DRAFT, FEATURE_SET_HISTORY):
            raise ValueError(f"unsupported team_laning_feature_set: {feature_set}")
        with_history = feature_set == FEATURE_SET_HISTORY
        try:
            temperature = float(metadata.get("team_laning_temperature", 1.0))
            delay = float(metadata.get("team_laning_history_delay_seconds", 0.0))
            window_raw = metadata.get("team_laning_recent_window_seconds")
            window = None if window_raw is None else float(window_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid team-laning history metadata") from exc
        if with_history and window_raw is None:
            raise ValueError("history artifact requires recent-window metadata")
        return cls(model, with_history, temperature, delay, window)
