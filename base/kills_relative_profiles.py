"""Small, offline-only extractor for relative hero kill profiles.

The module deliberately has no project or network dependencies.  A normalized
record keeps missing player observations as ``NaN``; callers must decide how to
handle those observations when fitting a model.
"""

from __future__ import annotations

from collections import defaultdict
import math
from typing import Any, Iterable

import numpy as np


METRIC_NAMES = (
    "kills_share", "deaths_share", "assists_share", "networth_share",
    "xpm_share", "gpm_share", "kills_per_min_relative",
    "deaths_per_min_relative", "assists_per_min_relative",
    "team_kills_per_min_relative", "team_deaths_per_min_relative",
    "team_networth_share", "team_xpm_share", "team_gpm_share",
    "kill_participation",
)
_STAT_KEYS = {
    "kills": ("kills", "kill"),
    "deaths": ("deaths", "death"),
    "assists": ("assists", "assist"),
    "networth": ("networth", "netWorth", "nw"),
    "xp": ("experiencePerMinute", "xpm", "xpPerMin"),
    "gpm": ("gpm", "goldPerMin", "goldPerMinute", "gold_per_min"),
}


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) and result >= 0 else None


def _first(mapping: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in mapping:
            return mapping[name]
    return None


def _position(value: Any) -> int | None:
    if isinstance(value, str) and value.upper().startswith("POSITION_"):
        value = value.rsplit("_", 1)[-1]
    try:
        numeric = float(value)
        result = int(numeric)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if numeric == result and result in range(1, 6) else None


def _identity(mapping, nested, flat):
    obj = mapping.get(nested)
    value = obj.get("id") if isinstance(obj, dict) else None
    number = _number(value if value is not None else _first(mapping, flat))
    return int(number) if number and number.is_integer() else None


def _side(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in {"radiant", "true", "1"}:
            return True
        if value.lower() in {"dire", "false", "0"}:
            return False
    return None


def _series(value: Any) -> np.ndarray | None:
    if not isinstance(value, (list, tuple, np.ndarray)):
        return None
    try:
        result = np.asarray(value, dtype=float)
    except (TypeError, ValueError):
        return None
    return result if result.ndim == 1 and np.all(np.isfinite(result)) and np.all(result >= 0) else None


def parse_match(match: Any) -> dict[str, Any] | None:
    """Normalize one Stratz/OpenDota-like match, or return ``None``.

    Player order in the input is irrelevant.  The canonical order is radiant
    positions 1..5 followed by dire positions 1..5.  A missing stat is NaN;
    malformed supplied values invalidate the match rather than becoming zero.
    """
    if not isinstance(match, dict) or not isinstance(match.get("players"), list):
        return None
    try:
        start = float(_first(match, ("startDateTime", "start", "start_time")))
    except (TypeError, ValueError):
        return None
    if not np.isfinite(start) or start <= 0:
        return None
    raw_duration = _first(match, ("duration", "durationSeconds", "duration_seconds"))
    duration = _number(raw_duration)
    raw_end = _first(match, ("endDateTime", "end", "end_time"))
    end = _number(raw_end)
    if end is None and duration is not None and duration > 0:
        end = start + duration
    if end is None or end <= start:
        return None
    if duration is None:
        duration = end - start
    if duration <= 0 or not np.isfinite(duration):
        return None
    if abs(end - start - duration) > 1:
        return None
    match_id = _number(_first(match, ("id", "matchId", "match_id")))
    if match_id is None or match_id <= 0 or not match_id.is_integer():
        return None

    slots: dict[tuple[bool, int], dict[str, Any]] = {}
    heroes: set[int] = set()
    for player in match["players"]:
        if not isinstance(player, dict):
            return None
        side, pos = _side(_first(player, ("isRadiant", "side"))), _position(player.get("position"))
        try:
            hero_number = float(_first(player, ("heroId", "hero_id", "hero")))
            hero = int(hero_number)
        except (TypeError, ValueError, OverflowError):
            return None
        if side is None or pos is None or hero_number != hero or hero <= 0 or hero in heroes or (side, pos) in slots:
            return None
        heroes.add(hero)
        stats: dict[str, float] = {}
        for name, aliases in _STAT_KEYS.items():
            value = _first(player, aliases)
            if value is None:
                stats[name] = np.nan
            else:
                parsed = _number(value)
                if parsed is None:
                    return None
                stats[name] = parsed
        slots[(side, pos)] = {
            "hero_id": hero,
            "account_id": _identity(player, "steamAccount", ("accountId", "account_id", "steamAccountId")),
            "side": "radiant" if side else "dire",
            "position": pos,
            **stats,
        }
    if len(slots) != 10 or any((side, pos) not in slots for side in (True, False) for pos in range(1, 6)):
        return None

    players = [slots[(side, pos)] for side in (True, False) for pos in range(1, 6)]
    arrays = {name: np.asarray([player[name] for player in players], dtype=float) for name in _STAT_KEYS}
    side_kills = {"radiant": np.sum(arrays["kills"][:5]), "dire": np.sum(arrays["kills"][5:])}
    side_deaths = {"radiant": np.sum(arrays["deaths"][:5]), "dire": np.sum(arrays["deaths"][5:])}
    series = {"radiant": _series(match.get("radiantKills")), "dire": _series(match.get("direKills"))}
    return {
        "id": int(match_id),
        "start": start,
        "end": end,
        "duration": duration,
        "heroes": tuple(player["hero_id"] for player in players),
        "players": tuple(players),
        "stats": arrays,
        "team_ids": {"radiant": _identity(match, "radiantTeam", ("radiantTeamId", "radiant_team_id")),
                     "dire": _identity(match, "direTeam", ("direTeamId", "dire_team_id"))},
        "account_ids": tuple(player["account_id"] for player in players),
        "side_kills": side_kills,
        "side_deaths": side_deaths,
        "side_kill_series": series,
    }


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator / denominator) if np.isfinite(numerator) and np.isfinite(denominator) and denominator > 0 else np.nan


def relative_metrics(record: dict[str, Any]) -> np.ndarray:
    """Return one row per canonical player and columns named by ``METRIC_NAMES``."""
    values = np.stack([np.asarray(record["stats"][name], dtype=float) for name in _STAT_KEYS], axis=-1)
    return relative_metrics_batch(values, np.asarray(record["duration"], dtype=float))


def relative_metrics_batch(stats: np.ndarray, durations: np.ndarray | float) -> np.ndarray:
    """Vectorized equivalent of :func:`relative_metrics`.

    ``stats`` has shape ``(..., 10, 6)`` and the final axis is K/D/A/NW/XP/GPM.
    ``durations`` broadcasts over the leading dimensions.
    """
    values = np.asarray(stats, dtype=float)
    if values.ndim < 2 or values.shape[-2:] != (10, 6):
        raise ValueError("stats must have shape (..., 10, 6)")
    if np.any(values < 0) or np.any(np.isinf(values)):
        raise ValueError("stats must be nonnegative or missing")
    duration = np.asarray(durations, dtype=float)
    if np.any(~np.isfinite(duration)) or np.any(duration <= 0):
        raise ValueError("durations must be finite and positive")
    shape = values.shape[:-1] + (len(METRIC_NAMES),)
    output = np.full(shape, np.nan, dtype=float)
    finite = np.isfinite(values)
    side = np.stack((values[..., :5, :], values[..., 5:, :]), axis=-3)
    side_finite = np.all(np.isfinite(side), axis=-2)
    side_totals = np.sum(side, axis=-2)
    with np.errstate(divide="ignore", invalid="ignore"):
        for col in range(6):
            total = side_totals[..., col]
            good = (total > 0) & side_finite[..., col]
            for side_index, sl in enumerate((slice(0, 5), slice(5, 10))):
                output[..., sl, col] = np.where(good[..., side_index, None], values[..., sl, col] / total[..., side_index, None], np.nan)
        for col, source in zip((6, 7, 8), range(3)):
            all_good = np.all(finite[..., :, source], axis=-1)
            pooled = np.sum(values[..., :, source], axis=-1) / 10.0 / duration
            output[..., :, col] = np.where((pooled > 0)[..., None] & all_good[..., None], values[..., :, source] / duration[..., None] / pooled[..., None], np.nan)
        all_totals = np.sum(values, axis=-2)
        for side_index, sl in enumerate((slice(0, 5), slice(5, 10))):
            for out_col, source in ((9, 0), (10, 1)):
                output[..., sl, out_col] = side_totals[..., side_index, source, None] / all_totals[..., source, None] * 2.0
            for out_col, source in ((11, 3), (12, 4), (13, 5)):
                output[..., sl, out_col] = side_totals[..., side_index, source, None] / all_totals[..., source, None]
            team_kills = side_totals[..., side_index, 0, None]
            output[..., sl, 14] = (values[..., sl, 0] + values[..., sl, 2]) / team_kills
    output[~np.isfinite(output)] = np.nan
    return output


def aggregate_profiles(records: Iterable[dict[str, Any]], before_timestamp: float) -> dict[int, dict[str, Any]]:
    """Aggregate pre-cutoff records by hero, skipping duplicate match IDs."""
    seen: set[Any] = set()
    sums: dict[int, np.ndarray] = defaultdict(lambda: np.zeros(len(METRIC_NAMES)))
    counts: dict[int, np.ndarray] = defaultdict(lambda: np.zeros(len(METRIC_NAMES), dtype=int))
    for record in records:
        if not isinstance(record, dict) or record.get("end", np.inf) >= before_timestamp:
            continue
        match_id = record.get("id")
        if match_id is None:
            raise ValueError("A stable match ID is required for deduplication")
        if match_id in seen:
            continue
        seen.add(match_id)
        metrics = relative_metrics(record)
        for row, hero in enumerate(record["heroes"]):
            valid = np.isfinite(metrics[row])
            sums[hero][valid] += metrics[row, valid]
            counts[hero][valid] += 1
    result = {}
    for hero, total in sums.items():
        mean = np.full(len(METRIC_NAMES), np.nan)
        valid = counts[hero] > 0
        mean[valid] = total[valid] / counts[hero][valid]
        result[hero] = {"count": int(np.max(counts[hero])), "mean": mean}
    return result
