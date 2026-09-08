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


def prior_history(keys, ended, scores, query_keys, query_times, prior=10.0,
                  recent_window_seconds=None):
    """Smoothed score/count from matching histories with end < query time.

    Scores are player-side STRATZ outcomes in [-2,2], never final map wins.
    Unknown account keys must be zero and cannot accumulate history.
    """
    valid = (keys > 0) & np.isfinite(scores)
    keys, ended, scores = keys[valid], ended[valid], scores[valid]
    if recent_window_seconds is not None:
        try:
            recent_window_seconds = float(recent_window_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError("recent_window_seconds must be finite and non-negative") from exc
        if not np.isfinite(recent_window_seconds) or recent_window_seconds < 0:
            raise ValueError("recent_window_seconds must be finite and non-negative")
    columns = 6 if recent_window_seconds is not None else 3
    result = np.zeros((len(query_keys), columns), dtype=np.float32)
    if not len(keys):
        return result
    order = np.lexsort((ended, keys))
    keys, ended, scores = keys[order], ended[order], scores[order]
    # Keep fractional query times (e.g. delayed availability) instead of
    # allowing a structured integer dtype to truncate them.
    index = np.rec.fromarrays([keys, np.asarray(ended, dtype=np.float64)], names="key,time")
    queries = np.rec.fromarrays([query_keys, np.asarray(query_times, dtype=np.float64)], names="key,time")
    loc = np.searchsorted(index, queries, side="left") - 1
    group_start = np.maximum.accumulate(np.where(
        np.r_[True, keys[1:] != keys[:-1]], np.arange(len(keys)), 0))
    def fill(target, upper, starts=None):
        upper_safe = np.maximum(upper, 0)
        upper_found = (upper >= 0) & (keys[upper_safe] == query_keys) & (query_keys > 0)
        if starts is None:
            starts = group_start[upper_safe]
        starts = np.asarray(starts)
        start_safe = np.minimum(starts, len(keys) - 1)
        range_found = ((starts < len(keys)) & (starts <= upper_safe) &
                       (keys[start_safe] == query_keys))
        upper_found &= range_found
        for column, values in enumerate((scores, np.sign(scores))):
            prefix = np.r_[0.0, np.cumsum(values, dtype=np.float64)]
            totals = prefix[upper_safe + 1] - prefix[starts]
            n_values = upper_safe - starts + 1
            target[upper_found, column] = totals[upper_found] / (n_values[upper_found] + prior)
        target[upper_found, 2] = np.log1p(n_values[upper_found])

    fill(result[:, :3], loc)
    if recent_window_seconds is not None:
        # The lower bound is inclusive; searchsorted(..., side="left") excludes
        # only records strictly before query-window.
        lower_loc = np.searchsorted(index, np.rec.fromarrays(
            [query_keys, np.asarray(query_times, dtype=np.float64) - recent_window_seconds],
            names="key,time"), side="left")
        fill(result[:, 3:], loc, lower_loc)
    return result


def build_history(corpus, selected, availability_delay_seconds=0,
                  recent_window_seconds=None):
    """Vectorized account-role and account-hero histories; completed games only."""
    try:
        availability_delay_seconds = float(availability_delay_seconds)
    except (TypeError, ValueError) as exc:
        raise ValueError("availability_delay_seconds must be finite and non-negative") from exc
    if not np.isfinite(availability_delay_seconds) or availability_delay_seconds < 0:
        raise ValueError("availability_delay_seconds must be finite and non-negative")
    accounts, heroes = corpus["accounts"], corpus["heroes"]
    labels = corpus["lane_labels"]
    end = corpus["ts"] + corpus["duration"]
    output_width = 12 if recent_window_seconds is not None else 6
    output = np.zeros((len(selected), 10, output_width), dtype=np.float32)
    query_time = corpus["ts"][selected] - availability_delay_seconds
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
        qtime = np.repeat(query_time, 2)
        for kind in range(2):
            # hero ids are validated below 1024 by the caller for this encoding.
            keys = acc if kind == 0 else np.where(acc > 0, acc * 1024 + hero, 0)
            qkeys = qacc if kind == 0 else np.where(qacc > 0, qacc * 1024 + qhero, 0)
            values = prior_history(keys, times, score.reshape(-1), qkeys, qtime,
                                   recent_window_seconds=recent_window_seconds)
            reshaped = values.reshape(-1, 2, values.shape[-1])
            output[:, slots, kind * 3:(kind + 1) * 3] = reshaped[:, :, :3]
            if recent_window_seconds is not None:
                output[:, slots, 6 + kind * 3:6 + (kind + 1) * 3] = reshaped[:, :, 3:]
        print(f"history role={role + 1}/5 completed", flush=True)
    return output


def lane_features(heroes, history=None, context=False):
    """One row per map/lane in easy,mid,hard order; heroes are R1..5,D1..5."""
    heroes = np.asarray(heroes)
    if heroes.ndim != 2 or heroes.shape[1] != 10:
        raise ValueError("heroes must have shape (maps,10) in role order")
    try:
        numeric_heroes = heroes.astype(float)
    except (TypeError, ValueError) as exc:
        raise ValueError("hero ids must be finite integers") from exc
    if (not np.isfinite(numeric_heroes).all() or
            not np.equal(numeric_heroes, np.floor(numeric_heroes)).all()):
        raise ValueError("hero ids must be finite integers")
    heroes = numeric_heroes.astype(np.int64)
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
    if context:
        if history is None:
            raise ValueError("context features require history with shape (maps,10,12)")
        for pos in range(10):
            frame[f"hero_{pos}"] = np.repeat(heroes[:, pos].astype(str), 3)
    if history is not None:
        history = np.asarray(history)
        expected_width = 12 if context else 6
        if history.shape != (n, 10, expected_width) or not np.isfinite(history).all():
            raise ValueError(f"history must be finite with shape (maps,10,{expected_width})")
        values = history[:, np.maximum(slots, 0)].copy()
        values[:, slots < 0] = 0
        for slot in range(4):
            for stat in range(expected_width):
                prefix = "history" if stat < 6 else "recent_history"
                source_stat = stat if stat < 6 else stat
                frame[f"{prefix}_{slot}_{source_stat % 6}"] = values[:, :, slot, source_stat].reshape(-1)
        if context:
            # Context history is [all-role3, all-role-hero3, recent-role3,
            # recent-role-hero3]. Keep the same lane topology for both teams.
            for side, (own, opp) in (("core", (0, 2)), ("support", (1, 3))):
                for stat in range(expected_width):
                    frame[f"context_{side}_diff_{stat}"] = (
                        values[:, :, own, stat] - values[:, :, opp, stat]).reshape(-1)
    return frame


class LaningModel:
    """Explicit feature contract; a history model never silently drops history."""

    def __init__(self, model, with_history=False, context=False, temperature=1.0,
                 availability_delay_seconds=0, recent_window_seconds=None):
        self.model, self.with_history = model, with_history
        self.context, self.temperature = context, float(temperature)
        if not np.isfinite(self.temperature) or self.temperature <= 0:
            raise ValueError("temperature must be finite and positive")
        self.history_config = {
            "availability_delay_seconds": float(availability_delay_seconds),
            "recent_window_seconds": (None if recent_window_seconds is None
                                       else float(recent_window_seconds)),
        }
        if (not np.isfinite(self.history_config["availability_delay_seconds"]) or
                self.history_config["availability_delay_seconds"] < 0):
            raise ValueError("availability_delay_seconds must be finite and non-negative")
        if (self.history_config["recent_window_seconds"] is not None and
                (not np.isfinite(self.history_config["recent_window_seconds"]) or
                 self.history_config["recent_window_seconds"] < 0)):
            raise ValueError("recent_window_seconds must be finite and non-negative")

    def predict_proba(self, heroes, history=None):
        if self.with_history and history is None:
            width = 12 if self.context else 6
            raise ValueError(f"This artifact requires causal history (maps,10,{width})")
        features = lane_features(heroes, history if self.with_history else None, self.context)
        result = self.model.predict_proba(features).reshape(-1, 3, 5)
        if self.temperature != 1.0:
            logp = np.log(np.maximum(result, np.finfo(np.float64).tiny)) / self.temperature
            logp -= np.max(logp, axis=-1, keepdims=True)
            result = np.exp(logp)
            result /= np.sum(result, axis=-1, keepdims=True)
        return result

    @classmethod
    def load(cls, directory, with_history=False):
        model = CatBoostClassifier()
        model.load_model(str(Path(directory) / ("history.cbm" if with_history else "draft.cbm")))
        metadata = model.get_metadata()
        feature_set = str(metadata.get("laning_feature_set", ""))
        if feature_set not in ("", "legacy", "context_recent_v1"):
            raise ValueError(f"unsupported laning_feature_set: {feature_set}")
        context = feature_set == "context_recent_v1"
        temperature = float(metadata.get("laning_temperature", 1.0))
        delay_key = "laning_history_delay_seconds"
        window_key = "laning_recent_window_seconds"
        if context and (delay_key not in metadata or window_key not in metadata):
            raise ValueError("context_recent_v1 requires history delay and recent window metadata")
        try:
            delay = float(metadata.get(delay_key, 0.0))
            window_value = metadata.get(window_key, None)
            window = None if window_value is None else float(window_value)
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid laning history metadata") from exc
        if (not np.isfinite(delay) or delay < 0 or
                (window is not None and (not np.isfinite(window) or window < 0))):
            raise ValueError("invalid laning history metadata")
        if context and window <= 0:
            raise ValueError("context_recent_v1 requires positive recent window metadata")
        return cls(model, with_history or context, context, temperature, delay, window)
