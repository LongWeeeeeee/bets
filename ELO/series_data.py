from __future__ import annotations

from collections import Counter, defaultdict

from ELO.domain import MatchRecord, SeriesBundle, SeriesRecord


def _series_group_key(match: MatchRecord) -> tuple[int, frozenset[str]]:
    radiant_token = f"{match.radiant_team_id}:{match.radiant_team_name}"
    dire_token = f"{match.dire_team_id}:{match.dire_team_name}"
    synthetic_series_id = match.series_id if match.series_id is not None else -match.match_id
    return synthetic_series_id, frozenset((radiant_token, dire_token))


def _best_of_from_series_type(series_type: str | None) -> int | None:
    mapping = {
        "BEST_OF_ONE": 1,
        "BEST_OF_TWO": 2,
        "BEST_OF_THREE": 3,
        "BEST_OF_FIVE": 5,
    }
    return mapping.get(series_type or "")


def build_series_bundles(matches: list[MatchRecord]) -> tuple[list[SeriesBundle], dict[str, int]]:
    grouped_matches: dict[tuple[int, frozenset[str]], list[MatchRecord]] = defaultdict(list)
    for match in matches:
        grouped_matches[_series_group_key(match)].append(match)

    bundles: list[SeriesBundle] = []
    summary: Counter[str] = Counter()

    for (_, _), series_matches in grouped_matches.items():
        series_matches.sort(key=lambda match: (match.timestamp, match.match_id))
        first_map = series_matches[0]
        best_of = _best_of_from_series_type(first_map.series_type)
        if best_of is None:
            summary["skipped_unknown_series_type"] += 1
            continue

        team_a_token = f"{first_map.radiant_team_id}:{first_map.radiant_team_name}"
        team_b_token = f"{first_map.dire_team_id}:{first_map.dire_team_name}"
        required_wins = 1 if best_of == 1 else (best_of // 2 + 1)
        wins: Counter[str] = Counter()
        deciding_maps: list[MatchRecord] = []
        winner_token: str | None = None

        for match in series_matches:
            deciding_maps.append(match)
            match_winner_token = (
                team_a_token if match.radiant_win and match.radiant_team_id == first_map.radiant_team_id
                else team_b_token
                if match.radiant_win
                else team_a_token
                if match.dire_team_id == first_map.radiant_team_id
                else team_b_token
            )
            wins[match_winner_token] += 1
            if wins[match_winner_token] >= required_wins:
                winner_token = match_winner_token
                break

        team_a_map_wins = wins[team_a_token]
        team_b_map_wins = wins[team_b_token]
        eligible = winner_token is not None and best_of in (1, 3, 5)
        skip_reason = None
        if winner_token is None:
            skip_reason = "no_decisive_winner"
            summary["skipped_no_decisive_winner"] += 1
        elif best_of == 2:
            skip_reason = "best_of_two_excluded"
            summary["skipped_best_of_two"] += 1
        else:
            summary["eligible_series"] += 1

        summary[f"series_type_{first_map.series_type}"] += 1

        bundles.append(
            SeriesBundle(
                series=SeriesRecord(
                    series_id=first_map.series_id if first_map.series_id is not None else -first_map.match_id,
                    start_timestamp=first_map.timestamp,
                    series_type=first_map.series_type or "UNKNOWN",
                    best_of=best_of,
                    team_a_id=first_map.radiant_team_id,
                    team_a_name=first_map.radiant_team_name,
                    team_b_id=first_map.dire_team_id,
                    team_b_name=first_map.dire_team_name,
                    team_a_player_ids=first_map.radiant_player_ids,
                    team_b_player_ids=first_map.dire_player_ids,
                    team_a_won=(winner_token == team_a_token) if eligible else None,
                    team_a_map_wins=team_a_map_wins,
                    team_b_map_wins=team_b_map_wins,
                    league_id=first_map.league_id,
                    league_name=first_map.league_name,
                    source_league_tier=first_map.source_league_tier,
                    derived_league_tier=first_map.derived_league_tier,
                    eligible_for_winner_target=eligible,
                    skip_reason=skip_reason,
                ),
                deciding_maps=tuple(deciding_maps),
                all_maps=tuple(series_matches),
            )
        )

    bundles.sort(key=lambda bundle: (bundle.series.start_timestamp, bundle.series.series_id))
    summary["all_series_groups"] = len(bundles)
    return bundles, dict(summary)
