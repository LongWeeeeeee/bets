"""Offline keyed outcome history, available only after strict end < query start.

Unknown keys (<=0) and outcomes with invalid/nonpositive durations contribute
nothing. The shrinkage reference passed to compute must be fixed independently
of future outcomes; a full-dataset mean is not an as-of reference.
"""
import numpy as np


class EndTimePrior:
    def __init__(self, keys, starts, ends):
        keys = np.asarray(keys, dtype=np.int64)
        starts = np.asarray(starts, dtype=np.float64)
        ends = np.asarray(ends, dtype=np.float64)
        if keys.ndim != 1 or starts.shape != keys.shape or ends.shape != keys.shape:
            raise ValueError("keys, starts and ends must be aligned vectors")
        self.n = len(keys)
        valid = (keys > 0) & np.isfinite(starts) & np.isfinite(ends) & (ends > starts)
        rows = np.flatnonzero(valid)
        self.order = rows[np.lexsort((ends[rows], keys[rows]))]
        ordered_keys = keys[self.order]
        self.group_start = np.maximum.accumulate(
            np.where(np.r_[True, ordered_keys[1:] != ordered_keys[:-1]],
                     np.arange(len(rows)), 0)) if len(rows) else np.array([], dtype=int)
        dtype = np.dtype([("key", np.int64), ("time", np.float64)])
        events = np.empty(len(rows), dtype=dtype)
        events["key"], events["time"] = ordered_keys, ends[self.order]
        query = np.empty(self.n, dtype=dtype)
        query["key"], query["time"] = keys, starts
        self.index = np.searchsorted(events, query, side="left") - 1
        if len(rows):
            self.found = ((self.index >= 0) & (keys > 0) & np.isfinite(starts) &
                          (ordered_keys[np.maximum(self.index, 0)] == keys))
        else:
            self.found = np.zeros(self.n, dtype=bool)

    def sum_count(self, values, weights):
        values = np.asarray(values, dtype=np.float64)
        weights = np.asarray(weights, dtype=np.float64)
        if values.shape != (self.n,) or weights.shape != (self.n,):
            raise ValueError("values and weights must align with history rows")
        if np.any(np.isfinite(weights) & (weights < 0)):
            raise ValueError("negative history weights")
        valid = np.isfinite(values) & np.isfinite(weights) & (weights > 0)
        w = np.where(valid, weights, 0.)[self.order]
        v = np.where(valid, values, 0.)[self.order] * w
        outputs = []
        for a in (v, w):
            prefix = np.r_[0., np.cumsum(a)]
            grouped = prefix[1:] - prefix[self.group_start]
            result = np.zeros(self.n, dtype=np.float64)
            result[self.found] = grouped[self.index[self.found]]
            outputs.append(result)
        return tuple(outputs)

    def compute(self, values, weights, k, g):
        if not np.isfinite(k) or k <= 0 or not np.isfinite(g):
            raise ValueError("positive finite shrinkage and finite fixed reference required")
        sums, counts = self.sum_count(values, weights)
        return ((sums + k * g) / (counts + k)).astype(np.float32)
