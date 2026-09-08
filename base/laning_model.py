"""Offline STRATZ lane outcome model. No per-player minute-10 NW is inferred."""
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier

LANE_NAMES = ("easy", "mid", "hard")
CLASS_NAMES = ("dire_stomp", "dire_win", "tie", "radiant_win", "radiant_stomp")
# Own core, own support, opponent core, opponent support; -1 means solo lane.
LANE_SLOTS = ((0, 4, 7, 8), (1, -1, 6, -1), (2, 3, 5, 9))
PLAYER_LANES = np.array([0, 1, 2, 2, 0, 2, 1, 0, 0, 2])


def prior_history(keys, ended, scores, query_keys, query_times, prior=10.0):
    """Smoothed score/count from matching histories with end < query time.

    Scores are player-side STRATZ outcomes in [-2,2], never final map wins.
    Unknown account keys must be zero and cannot accumulate history.
    """
    valid = (keys > 0) & np.isfinite(scores)
    keys, ended, scores = keys[valid], ended[valid], scores[valid]
    result = np.zeros((len(query_keys), 3), dtype=np.float32)
    if not len(keys):
        return result
    order = np.lexsort((ended, keys))
    keys, ended, scores = keys[order], ended[order], scores[order]
    index = np.rec.fromarrays([keys, ended], names="key,time")
    queries = np.rec.fromarrays([query_keys, query_times], names="key,time")
    loc = np.searchsorted(index, queries, side="left") - 1
    safe = np.maximum(loc, 0)
    found = (loc >= 0) & (keys[safe] == query_keys) & (query_keys > 0)
    group_start = np.maximum.accumulate(np.where(
        np.r_[True, keys[1:] != keys[:-1]], np.arange(len(keys)), 0))
    counts = np.arange(len(keys)) - group_start + 1
    for column, values in enumerate((scores, np.sign(scores))):
        prefix = np.r_[0.0, np.cumsum(values, dtype=np.float64)]
        totals = prefix[1:] - prefix[group_start]
        result[found, column] = (totals[safe[found]] / (counts[safe[found]] + prior))
    result[found, 2] = np.log1p(counts[safe[found]])
    return result


def build_history(corpus, selected):
    """Vectorized account-role and account-hero histories; completed games only."""
    accounts, heroes = corpus["accounts"], corpus["heroes"]
    labels = corpus["lane_labels"]
    end = corpus["ts"] + corpus["duration"]
    output = np.zeros((len(selected), 10, 6), dtype=np.float32)
    for role in range(5):
        slots = [role, role + 5]
        acc = accounts[:, slots].reshape(-1).astype(np.int64)
        hero = heroes[:, slots].reshape(-1).astype(np.int64)
        score = labels[:, PLAYER_LANES[slots]].astype(float) - 2
        score[labels[:, PLAYER_LANES[slots]] < 0] = np.nan
        score[:, 1] *= -1
        times = np.repeat(end, 2)
        qacc = accounts[selected][:, slots].reshape(-1).astype(np.int64)
        qhero = heroes[selected][:, slots].reshape(-1).astype(np.int64)
        qtime = np.repeat(corpus["ts"][selected], 2)
        for kind in range(2):
            # hero ids are validated below 1024 by the caller for this encoding.
            keys = acc if kind == 0 else np.where(acc > 0, acc * 1024 + hero, 0)
            qkeys = qacc if kind == 0 else np.where(qacc > 0, qacc * 1024 + qhero, 0)
            values = prior_history(keys, times, score.reshape(-1), qkeys, qtime)
            output[:, slots, kind * 3:(kind + 1) * 3] = values.reshape(-1, 2, 3)
        print(f"history role={role + 1}/5 completed", flush=True)
    return output


def lane_features(heroes, history=None):
    """One row per map/lane in easy,mid,hard order; heroes are R1..5,D1..5."""
    heroes = np.asarray(heroes)
    if heroes.ndim != 2 or heroes.shape[1] != 10:
        raise ValueError("heroes must have shape (maps,10) in role order")
    if np.any(heroes <= 0) or np.any(heroes >= 1024):
        raise ValueError("hero ids must be within 1..1023")
    if any(len(set(row)) != 10 for row in heroes):
        raise ValueError("each map must have ten distinct heroes")
    n = len(heroes)
    slots = np.array(LANE_SLOTS)
    h = heroes[:, np.maximum(slots, 0)].copy()
    h[:, slots < 0] = 0
    data = {"lane": np.tile(np.arange(3), n).astype(str)}
    for j, name in enumerate(("own_core", "own_support", "opp_core", "opp_support")):
        data[name] = h[:, :, j].reshape(-1).astype(str)
    for a, b in ((0, 1), (2, 3), (0, 2), (0, 3), (1, 2), (1, 3)):
        data[f"pair_{a}_{b}"] = np.char.add(np.char.add(
            h[:, :, a].reshape(-1).astype(str), ":"), h[:, :, b].reshape(-1).astype(str))
    frame = pd.DataFrame(data)
    if history is not None:
        if history.shape != (n, 10, 6) or not np.isfinite(history).all():
            raise ValueError("history must be finite with shape (maps,10,6)")
        values = history[:, np.maximum(slots, 0)].copy()
        values[:, slots < 0] = 0
        for slot in range(4):
            for stat in range(6):
                frame[f"history_{slot}_{stat}"] = values[:, :, slot, stat].reshape(-1)
    return frame


class LaningModel:
    """Explicit feature contract; a history model never silently drops history."""

    def __init__(self, model, with_history=False):
        self.model, self.with_history = model, with_history

    def predict_proba(self, heroes, history=None):
        if self.with_history and history is None:
            raise ValueError("This artifact requires causal history (maps,10,6)")
        features = lane_features(heroes, history if self.with_history else None)
        return self.model.predict_proba(features).reshape(-1, 3, 5)

    @classmethod
    def load(cls, directory, with_history=False):
        model = CatBoostClassifier()
        model.load_model(str(Path(directory) / ("history.cbm" if with_history else "draft.cbm")))
        return cls(model, with_history)
