"""Offline player metadata and point-in-time features; never fetch in scoring.

DLTV IDs, Dota account IDs and team IDs are separate namespaces. A country flag
is NOT a leaderboard division. Newly fetched old pages are current observations.
"""
import bisect
import hashlib
import json
import math
import re
from datetime import datetime
from statistics import mean, pstdev
from urllib.parse import urlparse


SCHEMA = "prematch-player-metadata-v1"
REGIONS = ("europe", "se_asia", "americas", "china")
DAY = 86400


def number(value, *, positive=False, integer=False):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result) or result < 0 or (positive and result == 0):
        return None
    if integer and result != int(result):
        return None
    return int(result) if integer else result


def timestamp(value):
    if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
        try:
            date = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return date.timestamp() if date.tzinfo else None
        except ValueError:
            return None
    return number(value, positive=True)


def embedded_json(html, variable):
    """Decode literal JSON only; never execute page JavaScript."""
    pattern = r"\b" + re.escape(variable) + r"\s*=\s*"
    for match in re.finditer(pattern, html):
        try:
            return json.JSONDecoder().raw_decode(html[match.end():])[0]
        except ValueError:
            continue
    raise ValueError("missing JSON assignment: " + variable)


def parse_dltv_match(html, *, source_url, observed_at):
    series = embedded_json(html, "series_item")
    observed_at = timestamp(observed_at)
    if observed_at is None:
        raise ValueError("invalid observation time")
    players = []
    accounts = set()
    slots = set()
    for entry in series.get("series_players", []):
        player = entry.get("player") or {}
        account = number(player.get("steam_id"), positive=True, integer=True)
        role = number(entry.get("role"), positive=True, integer=True)
        team = number(entry.get("team_id"), positive=True, integer=True)
        if not account or account >= 2**32 or role not in range(1, 6) or not team:
            raise ValueError("invalid Dota account/team/position in DLTV lineup")
        if account in accounts or (team, role) in slots:
            raise ValueError("duplicate account or position in DLTV lineup")
        accounts.add(account)
        slots.add((team, role))
        rank = number(player.get("rank"), positive=True, integer=True)
        players.append({
            "account_id": account, "dltv_player_id": player.get("id"),
            "name": player.get("title"), "slug": player.get("slug"),
            "dltv_team_id": team, "position": role,
            "earnings_usd": number(player.get("prizes")),
            "rank": rank, "rank_region": None, "rank_region_source": None,
            "rank_updated_at": timestamp(player.get("rank_updated_at")),
            "observed_at": observed_at, "source_url": source_url,
        })
    teams = {series.get("first_team_id"), series.get("second_team_id")}
    if len(players) != 10 or len(teams) != 2 or {p["dltv_team_id"] for p in players} != teams:
        raise ValueError("expected two complete five-player DLTV lineups")
    return {"schema": SCHEMA, "observed_at": observed_at,
            "source_url": source_url,
            "source_sha256": hashlib.sha256(html.encode()).hexdigest(),
            "series_id": series.get("id"), "players": players}


class PlayerHistory:
    """Immutable observations indexed by account and observation time.

    Rank region must have explicit source evidence. Valve's public table has no
    account IDs: names/ranks are not silently promoted to an identity mapping.
    """

    def __init__(self, snapshots):
        self.records = {}
        for snapshot in snapshots:
            if snapshot.get("schema") != SCHEMA:
                raise ValueError("unsupported player metadata schema")
            for record in snapshot["players"]:
                row = dict(record)
                account = number(row.get("account_id"), positive=True, integer=True)
                observed = timestamp(row.get("observed_at"))
                if not account or account >= 2**32 or not observed or not row.get("source_url"):
                    raise ValueError("invalid identity/provenance")
                if observed != timestamp(snapshot.get("observed_at")):
                    raise ValueError("snapshot and player observation times differ")
                row["account_id"], row["observed_at"] = account, observed
                for field, positive, integer in (("rank", True, True), ("earnings_usd", False, False)):
                    value = row.get(field)
                    parsed = number(value, positive=positive, integer=integer)
                    if value is not None and parsed is None:
                        raise ValueError("invalid " + field)
                    row[field] = parsed
                region = row.get("rank_region")
                if region is not None:
                    evidence = row.get("rank_region_source")
                    if not isinstance(evidence, dict):
                        raise ValueError("rank region requires account-bound source evidence")
                    if (region not in REGIONS or evidence.get("identity_method") != "account_id"
                            or evidence.get("account_id") != account or evidence.get("region") != region
                            or evidence.get("rank") != row["rank"]
                            or urlparse(evidence.get("url", "")).scheme != "https"
                            or not urlparse(evidence.get("url", "")).netloc
                            or not re.fullmatch(r"[0-9a-f]{64}", evidence.get("sha256", ""))
                            or not timestamp(evidence.get("observed_at"))
                            or timestamp(evidence.get("rank_updated_at")) != timestamp(row.get("rank_updated_at"))
                            or (timestamp(row.get("rank_updated_at")) or 0) > timestamp(evidence["observed_at"])
                            or timestamp(evidence["observed_at"]) > observed):
                        raise ValueError("invalid account-bound rank source evidence")
                updated = timestamp(row.get("rank_updated_at"))
                if updated is not None and updated > observed:
                    raise ValueError("rank timestamp after observation")
                row["rank_updated_at"] = updated
                rows = self.records.setdefault(account, {})
                if observed in rows and rows[observed] != row:
                    raise ValueError("conflicting observations for same account/time")
                rows[observed] = row
        self.times = {account: sorted(rows) for account, rows in self.records.items()}

    def at(self, account, asof):
        times = self.times.get(account, [])
        index = bisect.bisect_left(times, asof) - 1
        return self.records[account][times[index]] if index >= 0 else None

    def features(self, radiant, dire, *, asof, rank_max_age_days=7, earnings_max_age_days=30):
        """Accounts MUST be supplied in current map position order (1..5).

        Returns side values, Radiant-minus-Dire differences and joint coverage.
        Zero fills always have explicit coverage. No pretrained weight is changed.
        """
        asof = timestamp(asof)
        if not asof or len(radiant) != 5 or len(dire) != 5:
            raise ValueError("need cutoff and two five-account lineups")
        accounts = [number(a, positive=True, integer=True) for a in list(radiant) + list(dire)]
        if None in accounts or any(a >= 2**32 for a in accounts) or len(set(accounts)) != 10:
            raise ValueError("need ten distinct Dota account IDs")
        for age in (rank_max_age_days, earnings_max_age_days):
            if number(age, positive=True) is None:
                raise ValueError("max age must be positive and finite")
        sides, diagnostics = [], []
        for lineup in (accounts[:5], accounts[5:]):
            values, details = [], []
            for account in lineup:
                row = self.at(account, asof)
                earning, rank_strength, region = None, None, None
                reason = "no_prior_observation"
                if row:
                    age = asof - row["observed_at"]
                    if age <= earnings_max_age_days * DAY:
                        earning = row["earnings_usd"]
                    updated = row.get("rank_updated_at")
                    region = row.get("rank_region")
                    reason = "usable"
                    if not row.get("rank"):
                        reason = "rank_missing"
                    elif updated is None:
                        reason = "rank_time_unknown"
                    elif asof - updated > rank_max_age_days * DAY:
                        reason = "rank_stale"
                    elif region not in REGIONS:
                        reason = "rank_region_unknown"
                    else:
                        rank_strength = -math.log1p(row["rank"])
                values.append((earning, rank_strength, region))
                details.append({"account_id": account, "rank_status": reason,
                                "observed_at": row["observed_at"] if row else None,
                                "earnings_known": earning is not None})
            result = {}
            groups = {"team": range(5), "cores": range(3), "supports": range(3, 5)}
            groups.update({"pos" + str(i + 1): [i] for i in range(5)})
            for group, indices in groups.items():
                selected = [values[i] for i in indices]
                earnings = [math.log1p(x[0]) for x in selected if x[0] is not None]
                result[group + "_earnings_log_mean"] = mean(earnings) if earnings else 0.0
                result[group + "_earnings_coverage"] = len(earnings) / len(selected)
                if group in ("team", "cores", "supports"):
                    for stat, fn in (("min", min), ("max", max), ("std", pstdev)):
                        result[group + "_earnings_log_" + stat] = fn(earnings) if earnings else 0.0
                for region in REGIONS:
                    ranks = [x[1] for x in selected if x[1] is not None and x[2] == region]
                    prefix = group + "_rank_" + region
                    result[prefix + "_strength"] = mean(ranks) if ranks else 0.0
                    result[prefix + "_coverage"] = len(ranks) / len(selected)
            sides.append(result)
            diagnostics.append(details)
        features = {key + "_diff": sides[0][key] - sides[1][key] for key in sides[0]}
        features.update({key + "_joint": min(sides[0][key], sides[1][key])
                         for key in sides[0] if key.endswith("_coverage")})
        return {"schema": SCHEMA, "asof": asof, "features": features,
                "radiant": sides[0], "dire": sides[1], "diagnostics": diagnostics}
