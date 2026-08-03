"""Helpers for core/support pooled ``synergy_trio`` statistics.

The role pool collapses pos1-pos3 to ``core`` and pos4-pos5 to ``support``
while retaining the raw match counts of distinct exact-position cells.
Permutations of one raw trio are de-duplicated exactly like
``functions._lookup_unordered_combo_winrate``. The same key expansion is used
by the frozen-OOS experiment and the live draft-scoped SQLite lookup.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import permutations
from typing import Any, Iterable, Mapping

from base.cp1vs2_role_pool import position_role, split_hero_position, stats_games, stats_score


@dataclass(frozen=True)
class TrioRolePoolSample:
    role_key: str
    exact_key: str
    score: float
    games: int

    @property
    def dedup_key(self) -> tuple[str, float, int]:
        return self.exact_key, round(self.score, 8), self.games


def _token_sort_key(token: str) -> tuple[int, str]:
    hero_id, suffix = token.split(":", 1)
    return int(hero_id), suffix


def _canonical_tokens(tokens: Iterable[str]) -> tuple[str, str, str] | None:
    values = tuple(sorted((str(token) for token in tokens), key=_token_sort_key))
    if len(values) != 3 or len(set(values)) != 3:
        return None
    return values


def make_trio_role_key(tokens: Iterable[str]) -> str | None:
    pooled = []
    for token in tokens:
        hero_id, position = split_hero_position(token)
        role = position_role(position)
        if not hero_id or not role:
            return None
        pooled.append(f"{hero_id}:{role}")
    canonical = _canonical_tokens(pooled)
    return ",".join(canonical) if canonical else None


def make_trio_exact_key(tokens: Iterable[str]) -> str | None:
    exact = []
    for token in tokens:
        hero_id, position = split_hero_position(token)
        if not hero_id or not position:
            return None
        exact.append(f"{hero_id}:{position}")
    canonical = _canonical_tokens(exact)
    return ",".join(canonical) if canonical else None


def raw_row_to_trio_sample(raw_key: Any, entry: Any) -> TrioRolePoolSample | None:
    key = str(raw_key or "")
    if "_vs_" in key or "_with_" in key:
        return None
    parts = key.split(",")
    if len(parts) != 3:
        return None
    games = stats_games(entry)
    if games <= 0:
        return None
    role_key = make_trio_role_key(parts)
    exact_key = make_trio_exact_key(parts)
    if role_key is None or exact_key is None:
        return None
    return TrioRolePoolSample(
        role_key=role_key,
        exact_key=exact_key,
        score=stats_score(entry),
        games=games,
    )


def aggregate_trio_role_samples(
    rows: Iterable[tuple[Any, Any]],
) -> dict[str, dict[str, float | int]]:
    seen: set[tuple[str, float, int]] = set()
    result: dict[str, dict[str, float | int]] = {}
    for raw_key, entry in rows:
        sample = raw_row_to_trio_sample(raw_key, entry)
        if sample is None or sample.dedup_key in seen:
            continue
        seen.add(sample.dedup_key)
        bucket = result.setdefault(sample.role_key, {"score": 0.0, "games": 0})
        bucket["score"] = float(bucket["score"]) + sample.score
        bucket["games"] = int(bucket["games"]) + sample.games
    return result


def trio_role_entry_winrate(
    entry: Mapping[str, Any] | None,
    min_matches: int,
) -> tuple[float | None, int]:
    if not isinstance(entry, Mapping):
        return None, 0
    try:
        games = int(entry.get("games", 0) or 0)
        score = float(entry.get("score", 0.0) or 0.0)
    except (TypeError, ValueError):
        return None, 0
    if games < int(min_matches) or games <= 0:
        return None, games
    return score / games, games


def raw_lookup_keys_for_trio_role_key(role_key: str) -> set[str]:
    parts = str(role_key).split(",")
    if len(parts) != 3:
        return set()
    positions = {
        "core": ("pos1", "pos2", "pos3"),
        "support": ("pos4", "pos5"),
    }
    parsed = [part.split(":", 1) for part in parts]
    if any(len(item) != 2 or item[1] not in positions for item in parsed):
        return set()
    keys: set[str] = set()
    for first_pos in positions[parsed[0][1]]:
        for second_pos in positions[parsed[1][1]]:
            for third_pos in positions[parsed[2][1]]:
                exact = (
                    f"{parsed[0][0]}{first_pos}",
                    f"{parsed[1][0]}{second_pos}",
                    f"{parsed[2][0]}{third_pos}",
                )
                keys.update(",".join(perm) for perm in permutations(exact))
    return keys


def raw_lookup_keys_for_trio_tokens(tokens: Iterable[str]) -> set[str]:
    """Expand one exact draft trio to all same-role raw position keys."""
    role_key = make_trio_role_key(tokens)
    return raw_lookup_keys_for_trio_role_key(role_key) if role_key else set()
