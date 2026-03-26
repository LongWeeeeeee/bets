from __future__ import annotations

import re
from collections import defaultdict

from base.id_to_names import rest_teams, tier_one_teams, tier_two_teams

_DISPLAY_CAMEL_RE = re.compile(r"(?<=[a-z])(?=[A-Z])")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_GENERIC_TOKENS = {"team", "gaming", "esports", "esport", "club"}


def _normalize_alias(alias: str) -> str:
    return _NON_ALNUM_RE.sub("", str(alias or "").casefold())


def _normalize_display_name(team_name: str) -> str:
    prepared = _DISPLAY_CAMEL_RE.sub(" ", str(team_name or ""))
    tokens = [
        token
        for token in re.split(r"[^A-Za-z0-9]+", prepared.casefold())
        if token and token not in _GENERIC_TOKENS
    ]
    return "".join(tokens)


def _iter_team_entries():
    for mapping in (tier_one_teams, tier_two_teams, rest_teams):
        for alias, team_ids in mapping.items():
            if isinstance(team_ids, int):
                yield alias, {team_ids}
            elif isinstance(team_ids, set):
                normalized_ids = {int(team_id) for team_id in team_ids if isinstance(team_id, int)}
                if normalized_ids:
                    yield alias, normalized_ids


def _build_identity_maps() -> tuple[dict[int, str], dict[str, str]]:
    parent: dict[int, int] = {}
    alias_by_id: defaultdict[int, list[tuple[int, str]]] = defaultdict(list)
    first_id_by_alias: dict[str, int] = {}
    entry_order = 0

    def find(team_id: int) -> int:
        root = parent.setdefault(team_id, team_id)
        if root != team_id:
            parent[team_id] = find(root)
        return parent[team_id]

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for raw_alias, team_ids in _iter_team_entries():
        alias = _normalize_alias(raw_alias)
        if not alias:
            continue
        sorted_ids = sorted(team_ids)
        first_id = sorted_ids[0]
        for team_id in sorted_ids:
            parent.setdefault(team_id, team_id)
            alias_by_id[team_id].append((entry_order, alias))
        for team_id in sorted_ids[1:]:
            union(first_id, team_id)
        seen_alias_id = first_id_by_alias.get(alias)
        if seen_alias_id is not None:
            union(seen_alias_id, first_id)
        else:
            first_id_by_alias[alias] = first_id
        entry_order += 1

    aliases_by_root: defaultdict[int, list[tuple[int, str]]] = defaultdict(list)
    ids_by_root: defaultdict[int, set[int]] = defaultdict(set)
    for team_id in list(parent):
        root = find(team_id)
        ids_by_root[root].add(team_id)
        aliases_by_root[root].extend(alias_by_id[team_id])

    team_id_to_org_key: dict[int, str] = {}
    alias_to_org_key: dict[str, str] = {}
    for root, team_ids in ids_by_root.items():
        ordered_aliases = sorted(aliases_by_root[root], key=lambda item: (item[0], item[1]))
        canonical_alias = ordered_aliases[0][1]
        org_key = f"org:{canonical_alias}"
        for team_id in team_ids:
            team_id_to_org_key[team_id] = org_key
        for _, alias in ordered_aliases:
            alias_to_org_key[alias] = org_key

    return team_id_to_org_key, alias_to_org_key


TEAM_ID_TO_ORG_KEY, ALIAS_TO_ORG_KEY = _build_identity_maps()


def resolve_org_key(team_id: int | None, team_name: str) -> str:
    if isinstance(team_id, int):
        resolved = TEAM_ID_TO_ORG_KEY.get(team_id)
        if resolved is not None:
            return resolved
    normalized_display = _normalize_display_name(team_name)
    resolved = ALIAS_TO_ORG_KEY.get(normalized_display)
    if resolved is not None:
        return resolved
    normalized_alias = _normalize_alias(team_name)
    resolved = ALIAS_TO_ORG_KEY.get(normalized_alias)
    if resolved is not None:
        return resolved
    return f"name:{normalized_display or normalized_alias or 'unknown'}"
