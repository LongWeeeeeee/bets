from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RosterResolution:
    roster_key: str
    overlap_count: int
    continuity: bool


@dataclass
class _OrgLineageState:
    current_segment: int
    last_players: frozenset[int]
    recent_segment_lineups: tuple[frozenset[int], ...]


class RosterLineageTracker:
    def __init__(self, min_shared_players: int = 3, recent_segment_lineups: int = 3) -> None:
        self.min_shared_players = min_shared_players
        self.recent_segment_lineups = max(1, int(recent_segment_lineups))
        self._org_states: dict[str, _OrgLineageState] = {}

    def _append_recent_lineup(
        self,
        recent_lineups: tuple[frozenset[int], ...],
        current_players: frozenset[int],
    ) -> tuple[frozenset[int], ...]:
        filtered = tuple(lineup for lineup in recent_lineups if lineup != current_players)
        updated = filtered + (current_players,)
        return updated[-self.recent_segment_lineups :]

    def _best_overlap(self, state: _OrgLineageState, current_players: frozenset[int]) -> int:
        candidate_lineups = state.recent_segment_lineups or (state.last_players,)
        return max((len(current_players & lineup) for lineup in candidate_lineups), default=0)

    def export_state(self) -> dict[str, object]:
        return {
            "min_shared_players": int(self.min_shared_players),
            "recent_segment_lineups": int(self.recent_segment_lineups),
            "org_states": {
                str(org_key): {
                    "current_segment": int(state.current_segment),
                    "last_players": sorted(int(player_id) for player_id in state.last_players),
                    "recent_segment_lineups": [
                        sorted(int(player_id) for player_id in lineup)
                        for lineup in state.recent_segment_lineups
                    ],
                }
                for org_key, state in self._org_states.items()
            },
        }

    @classmethod
    def from_state(cls, raw_state: dict[str, object] | None) -> "RosterLineageTracker":
        state = raw_state if isinstance(raw_state, dict) else {}
        tracker = cls(
            min_shared_players=int(state.get("min_shared_players", 3) or 3),
            recent_segment_lineups=int(state.get("recent_segment_lineups", 3) or 3),
        )
        raw_org_states = state.get("org_states")
        if not isinstance(raw_org_states, dict):
            return tracker
        for org_key, payload in raw_org_states.items():
            if not isinstance(payload, dict):
                continue
            try:
                current_segment = int(payload.get("current_segment", 1) or 1)
            except (TypeError, ValueError):
                current_segment = 1
            raw_last_players = payload.get("last_players") or []
            last_players = frozenset(
                int(player_id)
                for player_id in raw_last_players
                if isinstance(player_id, int) or (isinstance(player_id, str) and str(player_id).isdigit())
            )
            raw_recent_lineups = payload.get("recent_segment_lineups") or []
            recent_lineups: list[frozenset[int]] = []
            if isinstance(raw_recent_lineups, list):
                for raw_lineup in raw_recent_lineups:
                    if not isinstance(raw_lineup, list):
                        continue
                    lineup = frozenset(
                        int(player_id)
                        for player_id in raw_lineup
                        if isinstance(player_id, int) or (isinstance(player_id, str) and str(player_id).isdigit())
                    )
                    if lineup:
                        recent_lineups.append(lineup)
            if not recent_lineups and last_players:
                recent_lineups = [last_players]
            tracker._org_states[str(org_key)] = _OrgLineageState(
                current_segment=current_segment,
                last_players=last_players,
                recent_segment_lineups=tuple(recent_lineups[-tracker.recent_segment_lineups :]),
            )
        return tracker

    def preview(self, org_key: str, player_ids: tuple[int, ...]) -> RosterResolution:
        current_players = frozenset(player_ids)
        state = self._org_states.get(org_key)
        if state is None:
            return RosterResolution(
                roster_key=f"{org_key}::roster:1",
                overlap_count=0,
                continuity=False,
            )

        overlap_count = self._best_overlap(state, current_players)
        if overlap_count >= self.min_shared_players:
            return RosterResolution(
                roster_key=f"{org_key}::roster:{state.current_segment}",
                overlap_count=overlap_count,
                continuity=True,
            )

        return RosterResolution(
            roster_key=f"{org_key}::roster:{state.current_segment + 1}",
            overlap_count=overlap_count,
            continuity=False,
        )

    def resolve(self, org_key: str, player_ids: tuple[int, ...]) -> RosterResolution:
        current_players = frozenset(player_ids)
        state = self._org_states.get(org_key)
        if state is None:
            state = _OrgLineageState(
                current_segment=1,
                last_players=current_players,
                recent_segment_lineups=(current_players,),
            )
            self._org_states[org_key] = state
            return RosterResolution(
                roster_key=f"{org_key}::roster:1",
                overlap_count=0,
                continuity=False,
            )

        overlap_count = self._best_overlap(state, current_players)
        if overlap_count >= self.min_shared_players:
            state.last_players = current_players
            state.recent_segment_lineups = self._append_recent_lineup(
                state.recent_segment_lineups,
                current_players,
            )
            return RosterResolution(
                roster_key=f"{org_key}::roster:{state.current_segment}",
                overlap_count=overlap_count,
                continuity=True,
            )

        state.current_segment += 1
        state.last_players = current_players
        state.recent_segment_lineups = (current_players,)
        return RosterResolution(
            roster_key=f"{org_key}::roster:{state.current_segment}",
            overlap_count=overlap_count,
            continuity=False,
        )
