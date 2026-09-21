"""Build a strict end-time causal winner-feature matrix from a rich corpus.

Each row is queried at its scheduled start.  A completed map contributes only
when ``end < query_start``; maps with the same boundary timestamp are excluded.
The implementation keeps compact keyed accumulators rather than histories of
per-player events, so it can replay the full corpus on one machine.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Mapping

import numpy as np
from scipy.optimize import linear_sum_assignment


K24_INITIAL = 1500.0
K24 = 24.0
ELO_SCALE = 400.0
RECENT_HALF_LIFE_SECONDS = 30.0 * 86400.0
PSTAT_METRICS = ("goldPerMinute", "imp", "kda")
BASE_FEATURE_NAMES = (
    "k24_rating_mean", "k24_rating_diff", "k24_rating_min", "k24_rating_max",
    "k24_rating_std", "k24_rating_coverage", "k24_prob",
    "k24_games_mean", "k24_games_diff", "k24_games_coverage",
    "player_games_mean", "player_games_diff", "player_games_min", "player_games_max",
    "player_games_std", "player_games_coverage",
    "player_winrate_mean", "player_winrate_diff", "player_winrate_std", "player_winrate_coverage",
    "role_games_mean", "role_games_diff", "role_games_coverage",
    "role_winrate_mean", "role_winrate_diff", "role_winrate_coverage",
    "opponent_k24_mean", "opponent_k24_diff", "opponent_k24_coverage",
    "recent_winrate_mean", "recent_winrate_diff", "recent_winrate_coverage",
    "team_games_mean", "team_games_diff", "team_games_coverage",
    "team_winrate_mean", "team_winrate_diff", "team_winrate_coverage",
    "team_rating_mean", "team_rating_diff", "team_rating_coverage",
    "hero_role_games_mean", "hero_role_games_diff", "hero_role_games_coverage",
    "hero_role_winrate_mean", "hero_role_winrate_diff", "hero_role_winrate_coverage",
    "hero_games_mean", "hero_games_diff", "hero_games_coverage",
    "hero_winrate_mean", "hero_winrate_diff", "hero_winrate_coverage",
    "player_hero_games_mean", "player_hero_games_diff", "player_hero_games_coverage",
    "player_hero_winrate_mean", "player_hero_winrate_diff", "player_hero_winrate_coverage",
    "team_h2h_games", "team_h2h_winrate", "team_h2h_coverage",
    "player_gpm_mean", "player_gpm_diff", "player_gpm_coverage",
    "player_imp_mean", "player_imp_diff", "player_imp_coverage",
    "player_kda_mean", "player_kda_diff", "player_kda_coverage",
)
ROLE_METRICS = (
    "k24_rating", "player_wr", "recent_winrate", "opponent_k24", "player_hero_games",
    "player_hero_winrate", "role_confidence", "player_gpm", "player_imp", "player_kda",
)
ROLE_FEATURE_NAMES = tuple(
    f"role{position}_{metric}_{side}"
    for position in range(1, 6)
    for metric in ROLE_METRICS
    for side in ("radiant", "diff")
)
FEATURE_NAMES = BASE_FEATURE_NAMES + ROLE_FEATURE_NAMES


def _elo_probability(diff: float) -> float:
    return 1.0 / (1.0 + math.pow(10.0, -diff / ELO_SCALE))


def _parse_emit_from(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        try:
            parsed = np.datetime64(value, "s")
        except ValueError as exc:
            raise argparse.ArgumentTypeError("--emit-from must be epoch seconds or ISO-8601") from exc
        return int(parsed.astype(np.int64))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _lookup(keys: np.ndarray, values: np.ndarray) -> np.ndarray:
    """Return dense indices for positive values in a sorted unique key vector."""
    result = np.full(values.shape, -1, dtype=np.int64)
    positive = values > 0
    if positive.any():
        indices = np.searchsorted(keys, values[positive])
        found = (indices < len(keys)) & (keys[np.minimum(indices, len(keys) - 1)] == values[positive])
        result[positive] = np.where(found, indices, -1)
    return result


def _side_summary(values: np.ndarray, covered: np.ndarray) -> tuple[float, float, float, float, float, float]:
    """Mean, radiant-minus-dire mean, min, max, std, coverage-count."""
    left, right = values[:5], values[5:]
    return (float(left.mean()), float(left.mean() - right.mean()), float(values.min()),
            float(values.max()), float(values.std()), float(covered.sum()))


def _mean_diff_coverage(values: np.ndarray, covered: np.ndarray) -> tuple[float, float, float]:
    return float(values[:5].mean()), float(values[:5].mean() - values[5:].mean()), float(covered.sum())


def _validate_rows(rows: Mapping[str, np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    required = ("mids", "ts", "wins", "teams", "leagues", "sids", "durations", "heroes", "accounts", "pstats")
    missing = [name for name in required if name not in rows]
    if missing:
        raise ValueError(f"source misses required arrays: {', '.join(missing)}")
    n = len(rows["mids"])
    if any(np.asarray(rows[name]).shape[0] != n for name in required):
        raise ValueError("source arrays have inconsistent row counts")
    if (np.asarray(rows["teams"]).shape != (n, 2) or np.asarray(rows["heroes"]).shape != (n, 10)
            or np.asarray(rows["accounts"]).shape != (n, 10)):
        raise ValueError("teams/heroes/accounts source shapes are invalid")
    wins = np.asarray(rows["wins"])
    if wins.shape != (n,) or not np.isin(wins, (0, 1)).all():
        raise ValueError("rich source requires binary labels, one per map")
    accounts = np.asarray(rows["accounts"], dtype=np.int64)
    durations = np.asarray(rows["durations"], dtype=np.int64)
    mids, starts = np.asarray(rows["mids"], dtype=np.int64), np.asarray(rows["ts"], dtype=np.int64)
    roster_ok = (accounts > 0).all(axis=1) & (np.sort(accounts, axis=1)[:, 1:] != np.sort(accounts, axis=1)[:, :-1]).all(axis=1)
    valid = roster_ok & (durations > 0) & (mids > 0) & (starts >= 0)
    if len(np.unique(mids[valid])) != int(valid.sum()):
        raise ValueError("valid rich rows must have unique map ids")
    ends = starts + durations
    if np.any(ends[valid] <= starts[valid]):
        raise ValueError("timestamp overflow in source")
    return valid, ends


def _elo_event_arrays(events: Mapping[str, np.ndarray]) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    required = ("mid", "ts", "end", "accounts", "y")
    missing = [name for name in required if name not in events]
    if missing:
        raise ValueError(f"elo events miss required arrays: {', '.join(missing)}")
    mid, ts, end = (np.asarray(events[name], dtype=np.int64) for name in ("mid", "ts", "end"))
    accounts, y = np.asarray(events["accounts"], dtype=np.int64), np.asarray(events["y"])
    if (mid.ndim != 1 or ts.shape != mid.shape or end.shape != mid.shape or accounts.shape != (len(mid), 10)
            or y.shape != mid.shape or np.any(mid <= 0) or np.any(ts < 0) or np.any(end <= ts)
            or not np.isin(y, (0, 1)).all() or not (accounts > 0).all()
            or not (np.sort(accounts, axis=1)[:, 1:] != np.sort(accounts, axis=1)[:, :-1]).all()
            or len(np.unique(mid)) != len(mid)):
        raise ValueError("elo events require unique ids, valid end times, binary labels and unique 10-account rosters")
    order = np.lexsort((mid, end))
    return mid[order], ts[order], end[order], accounts[order], y[order].astype(np.int8)


def build_from_arrays(rows: Mapping[str, np.ndarray], *, emit_from: int | None = None,
                      elo_events: Mapping[str, np.ndarray] | None = None) -> tuple[dict[str, np.ndarray], dict]:
    """Replay arrays into a causal dataset.  Exposed for small deterministic tests."""
    valid, ends = _validate_rows(rows)
    mids = np.asarray(rows["mids"], dtype=np.int64)
    starts = np.asarray(rows["ts"], dtype=np.int64)
    wins = np.asarray(rows["wins"])
    accounts = np.asarray(rows["accounts"], dtype=np.int64)
    heroes = np.asarray(rows["heroes"], dtype=np.int64)
    teams = np.asarray(rows["teams"], dtype=np.int64)
    pstats = np.asarray(rows["pstats"], dtype=np.float32)
    if pstats.ndim != 3 or pstats.shape[:2] != (len(mids), 10):
        raise ValueError("pstats must have shape (n, 10, columns)")
    names = tuple(map(str, np.asarray(rows.get("pstat_names", ())).tolist()))
    if names:
        try:
            stat_index = {"goldPerMinute": names.index("goldPerMinute"), "imp": names.index("imp"),
                          "kills": names.index("kills"), "deaths": names.index("deaths"),
                          "assists": names.index("assists")}
        except ValueError as exc:
            raise ValueError("pstat_names misses a required historical metric") from exc
    elif pstats.shape[2] >= 14:
        stat_index = {"kills": 0, "deaths": 1, "assists": 2, "goldPerMinute": 3, "imp": 11}
    else:
        raise ValueError("pstats needs canonical pstat_names or 14 canonical columns")

    if elo_events is None:
        elo_mid, elo_ts, elo_end, elo_accounts, elo_y = _elo_event_arrays({
            "mid": mids[valid], "ts": starts[valid], "end": ends[valid], "accounts": accounts[valid],
            "y": (np.asarray(wins[valid]) > 0.5).astype(np.int8),
        })
    else:
        elo_mid, elo_ts, elo_end, elo_accounts, elo_y = _elo_event_arrays(elo_events)
    valid_rows = np.flatnonzero(valid)
    shared_mid, rich_shared, elo_shared = np.intersect1d(mids[valid], elo_mid, return_indices=True)
    shared_rows = valid_rows[rich_shared]
    end_mismatches = int(np.count_nonzero(ends[shared_rows] != elo_end[elo_shared]))
    if end_mismatches:
        raise ValueError(f"rich/ELO shared-map end-time mismatch: {end_mismatches} of {len(shared_mid)}")
    orientation_match = (
        (starts[shared_rows] == elo_ts[elo_shared])
        & ((np.asarray(wins[shared_rows]) > 0.5) == (elo_y[elo_shared] > 0.5))
        & (np.sort(accounts[shared_rows, :5], axis=1) == np.sort(elo_accounts[elo_shared, :5], axis=1)).all(axis=1)
        & (np.sort(accounts[shared_rows, 5:], axis=1) == np.sort(elo_accounts[elo_shared, 5:], axis=1)).all(axis=1)
    )
    orientation_mismatches = int((~orientation_match).sum())
    if orientation_mismatches:
        raise ValueError(f"rich/ELO shared-map orientation mismatch: {orientation_mismatches} of {len(shared_mid)}")
    account_keys = np.unique(np.concatenate((accounts[valid].reshape(-1), elo_accounts.reshape(-1))))
    account_keys = account_keys[account_keys > 0]
    hero_keys = np.unique(heroes[(heroes > 0) & np.broadcast_to(valid[:, None], heroes.shape)])
    team_keys = np.unique(teams[(teams > 0) & np.broadcast_to(valid[:, None], teams.shape)])
    p_count, h_count, t_count = len(account_keys), len(hero_keys), len(team_keys)

    rating = np.full(p_count, K24_INITIAL, dtype=np.float64)
    k24_games = np.zeros(p_count, dtype=np.int32)
    player_games = np.zeros(p_count, dtype=np.int32)
    player_wins = np.zeros(p_count, dtype=np.float64)
    player_opp_sum = np.zeros(p_count, dtype=np.float64)
    recent_sum = np.zeros(p_count, dtype=np.float64)
    recent_weight = np.zeros(p_count, dtype=np.float64)
    recent_end = np.zeros(p_count, dtype=np.int64)
    role_games = np.zeros((p_count, 5), dtype=np.int32)
    role_wins = np.zeros((p_count, 5), dtype=np.float64)
    stat_sum = np.zeros((p_count, 3), dtype=np.float64)
    stat_count = np.zeros((p_count, 3), dtype=np.int32)
    team_games = np.zeros(t_count, dtype=np.int32)
    team_wins = np.zeros(t_count, dtype=np.float64)
    team_rating = np.full(t_count, K24_INITIAL, dtype=np.float64)
    hero_games = np.zeros(h_count, dtype=np.int32)
    hero_wins = np.zeros(h_count, dtype=np.float64)
    hero_role_games = np.zeros((h_count, 5), dtype=np.int32)
    hero_role_wins = np.zeros((h_count, 5), dtype=np.float64)
    # The dense account/hero id product avoids a Python dict per player-hero
    # event while keeping the state limited to pairs actually present in rich.
    rich_account_ids = np.searchsorted(account_keys, accounts[valid])
    rich_hero_ids = _lookup(hero_keys, heroes[valid])
    pair_values = rich_account_ids * max(h_count, 1) + rich_hero_ids
    pair_keys = np.unique(pair_values[pair_values >= 0])
    del rich_account_ids, rich_hero_ids, pair_values
    player_hero_games = np.zeros(len(pair_keys), dtype=np.int32)
    player_hero_wins = np.zeros(len(pair_keys), dtype=np.float64)
    team_h2h: dict[tuple[int, int], list[float]] = {}

    n_emit = int(np.count_nonzero(valid & ((starts >= emit_from) if emit_from is not None else True)))
    x = np.empty((n_emit, len(FEATURE_NAMES)), dtype=np.float32)
    output = {name: np.empty(n_emit, dtype=dtype) for name, dtype in (
        ("mid", np.int64), ("ts", np.int64), ("end", np.int64), ("y", np.int8),
        ("series", np.int64), ("league", np.int64), ("k24_diff", np.float32), ("elo_eligible", np.bool_))}
    output["heroes"] = np.empty((n_emit, 10), dtype=np.int32)
    output["accounts"] = np.empty((n_emit, 10), dtype=np.int64)

    # Ratings at a map's own start are recorded before it becomes observable at
    # a later query.  This avoids assigning overlapping completed maps a rating
    # that did not exist when they began.
    pre_opp = np.full((len(mids), 2), np.nan, dtype=np.float32)
    event_order = np.flatnonzero(valid)[np.lexsort((mids[valid], ends[valid]))]
    query_order = np.flatnonzero(valid)[np.lexsort((mids[valid], starts[valid]))]
    event_cursor, out_cursor = 0, 0

    def account_ids(row: int) -> np.ndarray:
        return _lookup(account_keys, accounts[row])

    def hero_ids(row: int) -> np.ndarray:
        return _lookup(hero_keys, heroes[row])

    def team_ids(row: int) -> np.ndarray:
        return _lookup(team_keys, teams[row])

    def query_slot_permutation(row: int) -> np.ndarray:
        """Estimate role slots from completed role history, never current order."""
        raw_ids = account_ids(row)
        output = np.empty(10, dtype=np.int64)
        for side in range(2):
            raw_slots = np.arange(side * 5, (side + 1) * 5)
            # Account-id ordering makes entirely unseen rosters deterministic.
            sorted_slots = raw_slots[np.argsort(accounts[row, raw_slots], kind="stable")]
            sorted_ids = raw_ids[sorted_slots]
            freq = (role_games[sorted_ids].astype(float) + 1.0) / (player_games[sorted_ids, None] + 5.0)
            player_rows, roles = linear_sum_assignment(-freq)
            output[side * 5 + roles] = sorted_slots[player_rows]
        return output

    def apply_event(row: int) -> None:
        ids, hids, tids = account_ids(row), hero_ids(row), team_ids(row)
        if (ids < 0).any():
            raise AssertionError("valid roster missing account index")
        radiant_win = float(wins[row]) > 0.5
        for side, score in ((0, float(radiant_win)), (1, float(not radiant_win))):
            slot = slice(side * 5, (side + 1) * 5)
            side_ids, side_hids = ids[slot], hids[slot]
            player_games[side_ids] += 1
            player_wins[side_ids] += score
            player_opp_sum[side_ids] += float(pre_opp[row, side])
            old_weight = recent_weight[side_ids]
            elapsed = ends[row] - recent_end[side_ids]
            factor = np.where(old_weight > 0, np.exp(-math.log(2.0) * elapsed / RECENT_HALF_LIFE_SECONDS), 0.0)
            recent_sum[side_ids] = recent_sum[side_ids] * factor + score
            recent_weight[side_ids] = old_weight * factor + 1.0
            recent_end[side_ids] = ends[row]
            positions = np.arange(5)
            role_games[side_ids, positions] += 1
            role_wins[side_ids, positions] += score
            good_heroes = side_hids >= 0
            if good_heroes.any():
                local_heroes = side_hids[good_heroes]
                local_positions = positions[good_heroes]
                hero_games[local_heroes] += 1
                hero_wins[local_heroes] += score
                hero_role_games[local_heroes, local_positions] += 1
                hero_role_wins[local_heroes, local_positions] += score
                pair = side_ids[good_heroes] * max(h_count, 1) + local_heroes
                pair_indices = np.searchsorted(pair_keys, pair)
                player_hero_games[pair_indices] += 1
                player_hero_wins[pair_indices] += score
            stats = pstats[row, slot]
            kda = (stats[:, stat_index["kills"]] + stats[:, stat_index["assists"]]) / (1.0 + stats[:, stat_index["deaths"]])
            metrics = (stats[:, stat_index["goldPerMinute"]], stats[:, stat_index["imp"]], kda)
            for metric_index, metric in enumerate(metrics):
                finite = np.isfinite(metric)
                stat_sum[side_ids[finite], metric_index] += metric[finite]
                stat_count[side_ids[finite], metric_index] += 1
            team = tids[side]
            if team >= 0:
                team_games[team] += 1
                team_wins[team] += score
        if tids[0] >= 0 and tids[1] >= 0 and tids[0] != tids[1]:
            team_expected = _elo_probability(team_rating[tids[0]] - team_rating[tids[1]])
            team_delta = K24 * (float(radiant_win) - team_expected)
            team_rating[tids[0]] += team_delta
            team_rating[tids[1]] -= team_delta
            pair = (min(int(tids[0]), int(tids[1])), max(int(tids[0]), int(tids[1])))
            h2h = team_h2h.setdefault(pair, [0.0, 0.0])
            h2h[0] += 1.0
            h2h[1] += float(radiant_win) if tids[0] < tids[1] else float(not radiant_win)

    elo_cursor = 0

    def apply_k24_event(event: int) -> None:
        ids = _lookup(account_keys, elo_accounts[event])
        if (ids < 0).any():
            raise AssertionError("elo event missing account index")
        radiant_mean, dire_mean = float(rating[ids[:5]].mean()), float(rating[ids[5:]].mean())
        delta = K24 * (float(elo_y[event]) - _elo_probability(radiant_mean - dire_mean))
        rating[ids[:5]] += delta
        rating[ids[5:]] -= delta
        k24_games[ids] += 1

    for row in query_order:
        start = starts[row]
        while elo_cursor < len(elo_mid) and elo_end[elo_cursor] < start:
            apply_k24_event(elo_cursor)
            elo_cursor += 1
        while event_cursor < len(event_order) and ends[event_order[event_cursor]] < start:
            apply_event(int(event_order[event_cursor]))
            event_cursor += 1
        permutation = query_slot_permutation(int(row))
        ids, hids, tids = account_ids(int(row))[permutation], hero_ids(int(row))[permutation], team_ids(int(row))
        r_mean, d_mean = float(rating[ids[:5]].mean()), float(rating[ids[5:]].mean())
        pre_opp[row] = (d_mean, r_mean)
        if emit_from is not None and start < emit_from:
            continue
        values: list[float] = []
        rate_coverage = k24_games[ids] > 0
        values.extend(_side_summary(rating[ids], rate_coverage))
        values.append(_elo_probability(r_mean - d_mean))
        values.extend(_mean_diff_coverage(k24_games[ids].astype(float), k24_games[ids] > 0))
        values.extend(_side_summary(player_games[ids].astype(float), player_games[ids] > 0))
        player_wr = (player_wins[ids] + 1.0) / (player_games[ids] + 2.0)
        values.extend(_mean_diff_coverage(player_wr, player_games[ids] > 0)[:2])
        values.append(float(player_wr.std()))
        values.append(float((player_games[ids] > 0).sum()))
        positions = np.tile(np.arange(5), 2)
        p_roles = role_games[ids, positions]
        role_wr = (role_wins[ids, positions] + 1.0) / (p_roles + 2.0)
        values.extend(_mean_diff_coverage(p_roles.astype(float), p_roles > 0))
        values.extend(_mean_diff_coverage(role_wr, p_roles > 0))
        opp = np.divide(player_opp_sum[ids], player_games[ids], out=np.zeros(10), where=player_games[ids] > 0)
        values.extend(_mean_diff_coverage(opp, player_games[ids] > 0))
        recent = np.divide(recent_sum[ids], recent_weight[ids], out=np.full(10, 0.5), where=recent_weight[ids] > 0)
        values.extend(_mean_diff_coverage(recent, recent_weight[ids] > 0))
        team_values = np.where(tids >= 0, team_games[np.maximum(tids, 0)], 0).astype(float)
        team_wr = np.divide(np.where(tids >= 0, team_wins[np.maximum(tids, 0)], 0.0), team_values,
                            out=np.full(2, 0.5), where=team_values > 0)
        team_rates = np.where(tids >= 0, team_rating[np.maximum(tids, 0)], K24_INITIAL)
        values.extend(_mean_diff_coverage(np.repeat(team_values, 5), np.repeat(team_values > 0, 5)))
        values.extend(_mean_diff_coverage(np.repeat(team_wr, 5), np.repeat(team_values > 0, 5)))
        values.extend(_mean_diff_coverage(np.repeat(team_rates, 5), np.repeat(team_values > 0, 5)))
        hgames = np.where(hids >= 0, hero_games[np.maximum(hids, 0)], 0)
        hrole_games = np.where(hids >= 0, hero_role_games[np.maximum(hids, 0), positions], 0)
        hwr = np.divide(np.where(hids >= 0, hero_wins[np.maximum(hids, 0)], 0.0), hgames,
                         out=np.full(10, 0.5), where=hgames > 0)
        hrole_wr = np.divide(np.where(hids >= 0, hero_role_wins[np.maximum(hids, 0), positions], 0.0), hrole_games,
                              out=np.full(10, 0.5), where=hrole_games > 0)
        values.extend(_mean_diff_coverage(hrole_games.astype(float), hrole_games > 0))
        values.extend(_mean_diff_coverage(hrole_wr, hrole_games > 0))
        values.extend(_mean_diff_coverage(hgames.astype(float), hgames > 0))
        values.extend(_mean_diff_coverage(hwr, hgames > 0))
        if len(pair_keys):
            pair = ids * max(h_count, 1) + np.maximum(hids, 0)
            pair_indices = np.minimum(np.searchsorted(pair_keys, pair), len(pair_keys) - 1)
            ph_games = np.where(hids >= 0, player_hero_games[pair_indices], 0)
            ph_wins = np.where(hids >= 0, player_hero_wins[pair_indices], 0.0)
        else:
            ph_games, ph_wins = np.zeros(10, dtype=np.int32), np.zeros(10, dtype=float)
        ph_wr = (ph_wins + 1.0) / (ph_games + 2.0)
        values.extend(_mean_diff_coverage(ph_games.astype(float), ph_games > 0))
        values.extend(_mean_diff_coverage(ph_wr, ph_games > 0))
        if tids[0] >= 0 and tids[1] >= 0 and tids[0] != tids[1]:
            pair = (min(int(tids[0]), int(tids[1])), max(int(tids[0]), int(tids[1])))
            h2h_games, lower_wins = team_h2h.get(pair, [0.0, 0.0])
            radiant_h2h_wins = lower_wins if tids[0] < tids[1] else h2h_games - lower_wins
            h2h_wr = (radiant_h2h_wins + 1.0) / (h2h_games + 2.0)
        else:
            h2h_games, h2h_wr = 0.0, 0.5
        values.extend((float(h2h_games), float(h2h_wr), float(h2h_games > 0)))
        historical_metrics = []
        for metric_index in range(3):
            count = stat_count[ids, metric_index]
            metric = np.divide(stat_sum[ids, metric_index], count, out=np.zeros(10), where=count > 0)
            historical_metrics.append(metric)
            values.extend(_mean_diff_coverage(metric, count > 0))
        role_confidence = (p_roles + 1.0) / (player_games[ids] + 5.0)
        role_values = (rating[ids], player_wr, recent, opp, ph_games, ph_wr, role_confidence,
                       historical_metrics[0], historical_metrics[1], historical_metrics[2])
        for position in range(5):
            for metric in role_values:
                values.extend((float(metric[position]), float(metric[position] - metric[position + 5])))
        if len(values) != len(FEATURE_NAMES) or not np.isfinite(values).all():
            raise AssertionError("feature construction produced an invalid schema or non-finite value")
        x[out_cursor] = values
        output["mid"][out_cursor], output["ts"][out_cursor], output["end"][out_cursor] = mids[row], start, ends[row]
        output["y"][out_cursor] = np.int8(float(wins[row]) > 0.5)
        output["series"][out_cursor] = np.asarray(rows["sids"])[row]
        output["league"][out_cursor] = np.asarray(rows["leagues"])[row]
        output["heroes"][out_cursor], output["accounts"][out_cursor] = heroes[row, permutation], accounts[row, permutation]
        output["k24_diff"][out_cursor] = r_mean - d_mean
        out_cursor += 1
        if out_cursor % 100000 == 0:
            print(json.dumps({"emitted": out_cursor, "total": n_emit, "start": int(start)}), flush=True)
    if out_cursor != n_emit:
        raise AssertionError("emitted row count disagrees with valid input mask")
    output["X"], output["feature_names"] = x, np.asarray(FEATURE_NAMES)
    output["elo_eligible"] = np.isin(output["mid"], elo_mid)
    output["label_side"] = np.asarray("radiant")
    output["hero_slots"] = np.asarray("radiant_1_5,dire_1_5")
    output["role_assignment"] = np.asarray("past_completed_player_roles")
    output["k24_direction"] = np.asarray("radiant_minus_dire")
    metadata = {
        "source_rows": int(len(mids)), "valid_rows": int(valid.sum()), "rejected_rows": int((~valid).sum()),
        "emitted_rows": n_emit, "emit_from": emit_from,
        "elo_events": int(len(elo_mid)), "elo_eligible_emitted": int(output["elo_eligible"].sum()),
        "elo_rich_overlap": int(len(shared_mid)), "elo_rich_orientation_mismatches": orientation_mismatches,
        "temporal_contract": "Features for start t use only rows whose end is strictly less than t; equal end/start is excluded.",
        "k24_contract": "Initial 1500, K=24, scale=400, five-account side means; updates ordered by (end, mid) from elo_events when supplied.",
        "feature_definitions": {
            "k24": "Per-account K=24 Elo summaries, computed before the queried map starts.",
            "player": "Completed-map player games, smoothed win rates, role experience and recent outcome EMA.",
            "opponent_k24": "Mean opponent K24 captured at each historical map's own start.",
            "team": "Completed-map teams2 keyed games, smoothed win rates and independent team Elo.",
            "hero_role": "Completed-map global hero, player-by-hero, and hero-by-draft-role counts and win rates.",
            "team_h2h": "Completed-map canonical team-pair count and radiant-oriented Laplace-smoothed win rate.",
            "player_gpm_imp_kda": "Completed-map per-player pstats aggregates; the queried row's pstats are excluded.",
            "naming": "Each *_mean is Radiant-side mean, *_diff is Radiant mean minus Dire mean, and *_coverage is number of covered slots.",
            "role_assignment": "Query account/hero slots are estimated by Hungarian assignment from completed player role frequencies; current raw slot order is excluded.",
            "role_projected": "For each causally assigned role, radiant value and radiant-minus-dire value use only existing completed-history query arrays.",
            "ordered_names": list(FEATURE_NAMES),
        },
        "base_feature_count": len(BASE_FEATURE_NAMES),
        "role_projection_feature_count": len(ROLE_FEATURE_NAMES),
        "pstats_contract": "Current-row pstats are never features; finite completed-row pstats only update historical aggregates.",
        "opponent_quality_contract": "Historical opponent K24 is captured at each completed map's own start, then accumulated at its end.",
    }
    return output, metadata


def export_elo_events(raw_dir: str | Path, output_path: str | Path) -> dict:
    """Export the production loader's deduplicated completed K24 population."""
    from ELO.data_loader import load_matches

    matches, summary = load_matches(Path(raw_dir))
    unique = {}
    for match in matches:  # load_matches orders (start, id): production retains this first record.
        unique.setdefault(match.match_id, match)
    completed = [match for match in unique.values() if match.result_timestamp is not None]
    payload = {
        "mid": np.asarray([match.match_id for match in completed], dtype=np.int64),
        "ts": np.asarray([match.timestamp for match in completed], dtype=np.int64),
        "end": np.asarray([match.result_timestamp for match in completed], dtype=np.int64),
        "accounts": np.asarray([match.radiant_player_ids + match.dire_player_ids for match in completed], dtype=np.int64),
        "y": np.asarray([match.radiant_win for match in completed], dtype=np.int8),
    }
    # Reject unexpected loader regressions before writing an event stream used
    # by model training; this is also the exact K24 input contract.
    _elo_event_arrays(payload)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    with tmp.open("wb") as handle:
        np.savez_compressed(handle, **payload)
    tmp.replace(output_path)
    return {"raw_matches": int(summary.get("raw_matches", 0)), "loader_valid": int(len(matches)),
            "duplicate_records": int(len(matches) - len(unique)), "completed_events": int(len(completed)),
            "output": str(output_path)}


def build_dataset(input_path: str | Path, output_dir: str | Path, *, emit_from: int | None = None,
                  elo_events_path: str | Path | None = None) -> dict:
    input_path, output_dir = Path(input_path), Path(output_dir)
    with np.load(input_path, allow_pickle=False) as source:
        # Do not materialize rich NW/XP/tower arrays: this builder only needs
        # pre-match identity, result, draft and completed-map pstats.
        needed = ("mids", "ts", "wins", "teams", "leagues", "sids", "durations", "heroes", "accounts", "pstats", "pstat_names")
        rows = {name: source[name] for name in needed if name in source.files}
    if elo_events_path is None:
        elo_events = None
    else:
        with np.load(elo_events_path, allow_pickle=False) as source:
            elo_events = {name: source[name] for name in source.files}
    output, metadata = build_from_arrays(rows, emit_from=emit_from, elo_events=elo_events)
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset_path, metadata_path = output_dir / "dataset.npz", output_dir / "metadata.json"
    dataset_tmp, metadata_tmp = dataset_path.with_suffix(".npz.tmp"), metadata_path.with_suffix(".json.tmp")
    with dataset_tmp.open("wb") as handle:
        np.savez_compressed(handle, **output)
    metadata.update({"source": str(input_path), "sourcehash": _sha256(input_path),
                     "elo_events_source": str(elo_events_path) if elo_events_path is not None else None,
                     "elo_events_sourcehash": _sha256(Path(elo_events_path)) if elo_events_path is not None else None,
                     "dataset": dataset_path.name})
    metadata_tmp.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    dataset_tmp.replace(dataset_path)
    metadata_tmp.replace(metadata_path)
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", help="rich .npz source")
    parser.add_argument("--output-dir", help="directory for dataset.npz and metadata.json")
    parser.add_argument("--emit-from", help="optional epoch seconds or ISO-8601 start boundary")
    parser.add_argument("--threads", type=int, default=1, help="accepted for harness compatibility; replay is single-threaded")
    parser.add_argument("--elo-events", help="production-compatible ELO events.npz for K24 replay")
    parser.add_argument("--export-elo-events", help="raw ELO JSON directory to export")
    parser.add_argument("--events-output", help="destination events.npz used with --export-elo-events")
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("--threads must be positive")
    if args.export_elo_events:
        if not args.events_output or args.input or args.output_dir or args.elo_events:
            parser.error("--export-elo-events requires only --events-output (besides --threads)")
        print(json.dumps(export_elo_events(args.export_elo_events, args.events_output), sort_keys=True))
        return
    if not args.input or not args.output_dir:
        parser.error("dataset build requires --input and --output-dir")
    metadata = build_dataset(args.input, args.output_dir, emit_from=_parse_emit_from(args.emit_from),
                             elo_events_path=args.elo_events)
    print(json.dumps({key: metadata[key] for key in ("source_rows", "valid_rows", "emitted_rows", "emit_from")}, sort_keys=True))


if __name__ == "__main__":
    main()
