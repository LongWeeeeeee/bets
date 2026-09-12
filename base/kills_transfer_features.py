"""Causal history and side orientation shared by kills transfer research."""
from __future__ import annotations

from collections import deque
import heapq

import numpy as np

from kills_relative_profiles import METRIC_NAMES, relative_metrics_batch

PRO_STAT_NAMES = ("kills_pm", "deaths_pm", "assists_pm", "nw_pm", "xpm", "gpm")
TEAM_STAT_NAMES = ("kills", "deaths", "opponent_kills", "minutes", "nw_share", "xp_share")


def swap(heroes):
    return np.concatenate((heroes[:, 5:], heroes[:, :5]), axis=1)


def public_profiles(data, cutoff, shrink=100.0):
    """Learn role-shrunk hero profiles from completed public observations only."""
    valid = data["ends"] < cutoff
    heroes = data["heroes"][valid]
    if not len(heroes):
        raise ValueError("No public observations before cutoff")
    nhero = int(heroes.max()) + 1
    shape = (nhero * 5, len(METRIC_NAMES))
    sums, counts = np.zeros(shape), np.zeros(shape)
    raw_sums, raw_counts = np.zeros((nhero * 5, 6)), np.zeros((nhero * 5, 6))
    rows = np.flatnonzero(valid)
    for offset in range(0, len(rows), 20000):
        at = rows[offset:offset + 20000]
        values = data["stats"][at]
        rel = relative_metrics_batch(values, data["durations"][at])
        keys = (data["heroes"][at] * 5 + np.tile(np.arange(5), 2)).ravel()
        raw = values.astype(float).copy()
        raw[:, :, :4] /= (data["durations"][at] / 60)[:, None, None]
        for vals, ss, cc in ((rel, sums, counts), (raw, raw_sums, raw_counts)):
            flat = vals.reshape(len(keys), -1)
            finite = np.isfinite(flat)
            np.add.at(ss, keys, np.where(finite, flat, 0))
            np.add.at(cc, keys, finite)
    result = {"cutoff": int(cutoff), "nhero": nhero}
    for name, ss, cc in (("relative", sums, counts), ("absolute", raw_sums, raw_counts)):
        role_s = ss.reshape(nhero, 5, -1).sum(0)
        role_c = cc.reshape(nhero, 5, -1).sum(0)
        role = np.divide(role_s, role_c, out=np.zeros_like(role_s), where=role_c > 0)
        norm = np.tile(role, (nhero, 1))
        mean = (ss + shrink * norm) / (cc + shrink)
        # Express deviation from the role baseline; unknown heroes are neutral.
        result[name] = mean - norm
        result[name + "_counts"] = cc
        result[name + "_roles"] = role
    return result


def profile_features(heroes, profiles, kind="relative", no_deaths=False):
    table = profiles[kind]
    keys = heroes * 5 + np.tile(np.arange(5), 2)
    known = (keys >= 0) & (keys < len(table))
    clipped = np.clip(keys, 0, len(table) - 1)
    values = table[clipped] * known[:, :, None]
    support = profiles[kind + "_counts"][clipped] * known[:, :, None]
    metric_names = list(METRIC_NAMES if kind == "relative" else PRO_STAT_NAMES)
    if no_deaths:
        mask = ["death" not in name for name in metric_names]
        values = values[:, :, mask]
        support = support[:, :, mask]
        metric_names = [name for name, use in zip(metric_names, mask) if use]
    columns = [f"pub_{kind}_slot{slot}_{metric}" for slot in range(10) for metric in metric_names]
    parts = [values.reshape(len(heroes), -1)]
    for label, sl in (("own", slice(0, 5)), ("opponent", slice(5, 10))):
        for stat, func in (("mean", np.mean), ("std", np.std), ("min", np.min), ("max", np.max)):
            parts.append(func(values[:, sl], axis=1))
            columns.extend(f"pub_{kind}_{label}_{stat}_{metric}" for metric in metric_names)
    parts.extend([np.log1p(np.median(support, axis=2)), np.mean(support > 0, axis=2)])
    columns.extend(f"pub_{kind}_slot{slot}_log_support" for slot in range(10))
    columns.extend(f"pub_{kind}_slot{slot}_metric_coverage" for slot in range(10))
    return np.column_stack(parts).astype(np.float32), columns


class Recent:
    def __init__(self, width, size=30):
        self.rows = deque(maxlen=size)
        self.sums = np.zeros(width)
        self.counts = np.zeros(width)

    def add(self, row):
        row = np.asarray(row, float)
        if len(self.rows) == self.rows.maxlen:
            old = self.rows[0]
            finite = np.isfinite(old)
            self.sums -= np.where(finite, old, 0)
            self.counts -= finite
        self.rows.append(row)
        finite = np.isfinite(row)
        self.sums += np.where(finite, row, 0)
        self.counts += finite

    def mean(self):
        return np.divide(self.sums, self.counts, out=np.full_like(self.sums, np.nan), where=self.counts > 0)


def causal_pro_context(data):
    """Recent player/team history, visible only once a previous map ends.

    Return two sides per map; no current stats, duration or outcome are read
    while constructing that map's features. Unknown identities stay missing.
    """
    n = len(data["ts"])
    if np.any(np.diff(data["ts"]) < 0) or len(np.unique(data["mids"])) != n:
        raise ValueError("Pro observations must be chronological and unique")
    result = np.full((n, 2, 28), np.nan, dtype=np.float32)
    player_hist, team_hist, pending = {}, {}, []
    names = [f"players_{agg}_{name}" for agg in ("mean", "min", "max") for name in PRO_STAT_NAMES]
    names += [f"team_{name}" for name in TEAM_STAT_NAMES]
    names += ["player_games_mean", "player_known_fraction", "team_games", "roster_overlap"]

    def update(j):
        stats = data["stats"][j].astype(float).copy()
        minutes = data["durations"][j] / 60
        stats[:, :4] /= minutes
        for account, values in zip(data["accounts"][j], stats):
            if account > 0:
                player_hist.setdefault(int(account), Recent(6)).add(values)
        sums = data["stats"][j].reshape(2, 5, 6).sum(1)
        pooled = sums.sum(0)
        for side in range(2):
            team = int(data["teams"][j, side])
            if team > 0:
                with np.errstate(divide="ignore", invalid="ignore"):
                    values = [sums[side, 0], sums[side, 1], sums[1-side, 0], minutes,
                              sums[side, 3] / pooled[3], sums[side, 4] / pooled[4]]
                state = team_hist.setdefault(team, Recent(6))
                state.add(values)
                state.accounts = set(int(x) for x in data["accounts"][j, side*5:side*5+5] if x > 0)

    for i, start in enumerate(data["ts"]):
        while pending and pending[0][0] < start:
            _, j = heapq.heappop(pending)
            update(j)
        for side in range(2):
            accounts = data["accounts"][i, side*5:side*5+5]
            histories = [player_hist.get(int(x)) if x > 0 else None for x in accounts]
            values = np.stack([state.mean() if state is not None else np.full(6, np.nan) for state in histories])
            valid = np.isfinite(values)
            count = valid.sum(0)
            means = np.divide(np.where(valid, values, 0).sum(0), count, out=np.full(6, np.nan), where=count > 0)
            mins = np.where(valid, values, np.inf).min(0)
            maxs = np.where(valid, values, -np.inf).max(0)
            mins[count == 0], maxs[count == 0] = np.nan, np.nan
            team = team_hist.get(int(data["teams"][i, side]))
            team_values = team.mean() if team is not None else np.full(6, np.nan)
            games = [len(state.rows) if state is not None else 0 for state in histories]
            known = [x for x in accounts if x > 0]
            overlap = len(set(known) & team.accounts) / len(known) if known and team is not None else 0
            result[i, side] = np.r_[means, mins, maxs, team_values, np.mean(games),
                                   sum(g > 0 for g in games)/5, len(team.rows) if team is not None else 0, overlap]
        heapq.heappush(pending, (int(data["ends"][i]), i))
    return result, names


def temporal_splits(data, train_from, val_from, test_from):
    """Discard boundary-crossing maps and series, retaining both sides together."""
    t, end = data["ts"], data["ends"]
    split = np.full(len(t), -1, np.int8)
    split[(t >= train_from) & (end < val_from)] = 0
    split[(t >= val_from) & (end < test_from)] = 1
    split[t >= test_from] = 2
    # Include embargoed maps when deciding whether a series crosses a boundary.
    temporal = np.where(t < val_from, 0, np.where(t < test_from, 1, 2))
    series_ranges = {}
    for sid, period in zip(data["sids"], temporal):
        if sid > 0:
            lo, hi = series_ranges.get(int(sid), (int(period), int(period)))
            series_ranges[int(sid)] = (min(lo, int(period)), max(hi, int(period)))
    crossing = {sid for sid, (lo, hi) in series_ranges.items() if lo != hi}
    split[np.isin(data["sids"], list(crossing))] = -1
    return split
