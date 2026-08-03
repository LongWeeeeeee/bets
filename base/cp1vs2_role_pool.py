"""Offline helpers for core/support pooled cp1vs2 experiments.

This module is intentionally not wired into the live dispatch pipeline.  It
collapses exact OpenDota positions into two role families while preserving the
existing ``_lookup_vs_winrate`` direct/reverse and permutation de-duplication
semantics used by production draft dictionaries.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import permutations
from typing import Any, Iterable, Mapping


CORE_POSITIONS = frozenset({"pos1", "pos2", "pos3"})
SUPPORT_POSITIONS = frozenset({"pos4", "pos5"})
VALID_POSITIONS = CORE_POSITIONS | SUPPORT_POSITIONS


@dataclass(frozen=True)
class RolePoolSample:
    role_key: str
    exact_key: str
    direction: str
    score: float
    games: int

    @property
    def dedup_key(self) -> tuple[str, str, float, int]:
        return self.exact_key, self.direction, round(self.score, 8), self.games


def split_hero_position(token: Any) -> tuple[str | None, str | None]:
    text = str(token or "")
    for position in sorted(VALID_POSITIONS):
        if text.endswith(position):
            hero_id = text[: -len(position)]
            if hero_id.isdigit() and int(hero_id) > 0:
                return str(int(hero_id)), position
    return None, None


def position_role(position: Any) -> str | None:
    value = str(position or "")
    if value in CORE_POSITIONS:
        return "core"
    if value in SUPPORT_POSITIONS:
        return "support"
    return None


def _token_sort_key(token: str) -> tuple[int, str]:
    hero_id, suffix = token.split(":", 1)
    return int(hero_id), suffix


def _canonical_duo(tokens: Iterable[str]) -> tuple[str, str] | None:
    values = tuple(sorted((str(token) for token in tokens), key=_token_sort_key))
    if len(values) != 2 or values[0] == values[1]:
        return None
    return values


def make_role_key(self_token: str, duo_tokens: Iterable[str]) -> str | None:
    self_id, self_pos = split_hero_position(self_token)
    if not self_id or not self_pos:
        return None
    self_role = position_role(self_pos)
    duo_roles = []
    for token in duo_tokens:
        hero_id, position = split_hero_position(token)
        role = position_role(position)
        if not hero_id or not role:
            return None
        duo_roles.append(f"{hero_id}:{role}")
    duo = _canonical_duo(duo_roles)
    if duo is None:
        return None
    return f"{self_id}:{self_role}_vs_{duo[0]},{duo[1]}"


def make_exact_key(self_token: str, duo_tokens: Iterable[str]) -> str | None:
    self_id, self_pos = split_hero_position(self_token)
    if not self_id or not self_pos:
        return None
    duo_exact = []
    for token in duo_tokens:
        hero_id, position = split_hero_position(token)
        if not hero_id or not position:
            return None
        duo_exact.append(f"{hero_id}:{position}")
    duo = _canonical_duo(duo_exact)
    if duo is None:
        return None
    return f"{self_id}:{self_pos}_vs_{duo[0]},{duo[1]}"


def stats_games(entry: Any) -> int:
    if not isinstance(entry, Mapping):
        return 0
    try:
        return max(0, int(entry.get("games", entry.get("matches", 0)) or 0))
    except (TypeError, ValueError):
        return 0


def stats_score(entry: Any) -> float:
    games = stats_games(entry)
    if games <= 0 or not isinstance(entry, Mapping):
        return 0.0
    try:
        wins = float(entry.get("wins", entry.get("win", 0)) or 0)
    except (TypeError, ValueError):
        wins = 0.0
    try:
        draws = float(entry.get("draws", entry.get("draw", 0)) or 0)
    except (TypeError, ValueError):
        draws = 0.0
    return max(0.0, min(float(games), wins + draws * 0.5))


def raw_row_to_sample(raw_key: Any, entry: Any) -> RolePoolSample | None:
    key = str(raw_key or "")
    if key.count("_vs_") != 1:
        return None
    left, right = key.split("_vs_", 1)
    left_parts = left.split(",")
    right_parts = right.split(",")
    if len(left_parts) == 1 and len(right_parts) == 2:
        self_token, duo_tokens, direction = left_parts[0], right_parts, "direct"
    elif len(left_parts) == 2 and len(right_parts) == 1:
        self_token, duo_tokens, direction = right_parts[0], left_parts, "reverse"
    else:
        return None
    games = stats_games(entry)
    if games <= 0:
        return None
    score = stats_score(entry)
    if direction == "reverse":
        score = games - score
    role_key = make_role_key(self_token, duo_tokens)
    exact_key = make_exact_key(self_token, duo_tokens)
    if role_key is None or exact_key is None:
        return None
    return RolePoolSample(
        role_key=role_key,
        exact_key=exact_key,
        direction=direction,
        score=float(score),
        games=games,
    )


def aggregate_role_samples(
    rows: Iterable[tuple[Any, Any]],
) -> dict[str, dict[str, float | int]]:
    """Aggregate raw rows with production-compatible permutation de-duplication."""
    seen: set[tuple[str, str, float, int]] = set()
    result: dict[str, dict[str, float | int]] = {}
    for raw_key, entry in rows:
        sample = raw_row_to_sample(raw_key, entry)
        if sample is None or sample.dedup_key in seen:
            continue
        seen.add(sample.dedup_key)
        bucket = result.setdefault(sample.role_key, {"score": 0.0, "games": 0})
        bucket["score"] = float(bucket["score"]) + sample.score
        bucket["games"] = int(bucket["games"]) + sample.games
    return result


def role_entry_winrate(
    entry: Mapping[str, Any] | None,
    min_matches: int = 25,
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


def raw_lookup_keys_for_role_key(role_key: str) -> set[str]:
    """Expand one role key to exact positional direct/reverse lookup keys."""
    left, right = str(role_key).split("_vs_", 1)
    self_id, self_role = left.split(":", 1)
    duo = right.split(",")
    if len(duo) != 2:
        return set()
    first_id, first_role = duo[0].split(":", 1)
    second_id, second_role = duo[1].split(":", 1)
    positions = {
        "core": ("pos1", "pos2", "pos3"),
        "support": ("pos4", "pos5"),
    }
    keys: set[str] = set()
    for self_pos in positions.get(self_role, ()):
        for first_pos in positions.get(first_role, ()):
            for second_pos in positions.get(second_role, ()):
                self_token = f"{self_id}{self_pos}"
                duo_tokens = (f"{first_id}{first_pos}", f"{second_id}{second_pos}")
                for duo_perm in permutations(duo_tokens):
                    duo_text = ",".join(duo_perm)
                    keys.add(f"{self_token}_vs_{duo_text}")
                    keys.add(f"{duo_text}_vs_{self_token}")
    return keys

