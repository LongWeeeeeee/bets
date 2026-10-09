#!/usr/bin/env python3
"""Cache OpenDota pro listings and missing-map players for the ELO supplement.

Exit 3 stops this night on quota/outage; published arrays remain resumable.
Only match IDs from processed_ids.txt are loaded, never the STRATZ corpus.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.pro_chain.build_elo_supplement import _load_excluded_ids, _rows

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
PERIODS = [(1704067200, 1719792000), (1719792000, 1735689600),
           (1735689600, 1751328000), (1751328000, 1767225600),
           (1767225600, 1783209600), (1783209600, 1893456000)]
QUOTA_RESERVE = 10


def _write_atomic(path: Path, rows: list) -> None:
    # Failed writes leave their .tmp for inspection; only complete JSON is published.
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as fh:
        json.dump(rows, fh)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(temporary, path)


class Explorer:
    def __init__(self):
        self.next_request = 0.0

    def sql(self, query: str) -> tuple[list[dict], bool]:
        # Sequential, <=30 requests/minute. Fail fast instead of retrying an outage
        # against a daily quota shared with prod; the next night is the retry.
        time.sleep(max(0.0, self.next_request - time.monotonic()))
        url = "https://api.opendota.com/api/explorer?sql=" + urllib.parse.quote(query)
        request = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(request, timeout=180) as response:
            remaining = response.headers.get("x-rate-limit-remaining-day")
            payload = json.load(response)
        self.next_request = time.monotonic() + 2
        if not isinstance(payload, dict) or payload.get("err") or payload.get("error"):
            raise ValueError(f"explorer error: {str(payload)[:300]}")
        rows = payload.get("rows")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError("explorer response: expected a rows array of objects")
        low_quota = remaining is not None and int(remaining) <= QUOTA_RESERVE
        print(f"rows={len(rows)} remaining_day={remaining}", flush=True)
        return rows, low_quota


def _listing_sql(start: int, end: int) -> str:
    return ("SELECT m.match_id, m.start_time, m.duration, m.radiant_win, m.leagueid, "
            "l.tier, l.name AS league_name, m.radiant_team_id, m.dire_team_id, "
            "rt.name AS radiant_name, dt.name AS dire_name, m.series_id, m.series_type "
            "FROM matches m LEFT JOIN leagues l ON l.leagueid = m.leagueid "
            "LEFT JOIN teams rt ON rt.team_id = m.radiant_team_id "
            "LEFT JOIN teams dt ON dt.team_id = m.dire_team_id "
            f"WHERE m.start_time >= {start} AND m.start_time < {end} ORDER BY m.match_id")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--corpus-ids", type=Path, required=True,
                        help="processed_ids.txt: JSON list or one match ID per line")
    parser.add_argument("--refresh-days", type=int, default=45,
                        help="re-list periods overlapping the last N days; 0 reuses cached listings")
    args = parser.parse_args(argv)
    if args.refresh_days < 0:
        parser.error("--refresh-days must be non-negative")
    try:
        corpus_ids = _load_excluded_ids(args.corpus_ids)
    except (OSError, ValueError, TypeError) as exc:
        parser.error(str(exc))

    try:
        args.cache_dir.mkdir(parents=True, exist_ok=True)
        explorer = Explorer()
        now = time.time()
        refresh_since = now - args.refresh_days * 86400
        listing_ids: set[int] = set()
        recent_ids: set[int] = set()
        for start, end in PERIODS:
            path = args.cache_dir / f"listing_{start}.json"
            refresh = args.refresh_days > 0 and end > refresh_since and start <= now
            if not path.exists() or refresh:
                print(f"listing {start}..{end}", flush=True)
                rows, low_quota = explorer.sql(_listing_sql(start, end))
                _write_atomic(path, rows)
                if low_quota:
                    raise ValueError("daily quota reserve reached; resume next night")
            else:
                rows = _rows(args.cache_dir, path.name)
            for row in rows:
                mid = int(row["match_id"])
                listing_ids.add(mid)
                if args.refresh_days > 0 and refresh_since <= row["start_time"] <= now:
                    recent_ids.add(mid)
        cached_ids = {int(row["match_id"]) for row in _rows(args.cache_dir, "players_*.json")}
        noplayers_ids: set[int] = set()
        for path in args.cache_dir.glob("noplayers_*.json"):
            noplayers_ids.update(_load_excluded_ids(path))
        missing = sorted(listing_ids - corpus_ids - cached_ids - (noplayers_ids - recent_ids))
        print(f"listing={len(listing_ids)} excluded={len(listing_ids & corpus_ids)} "
              f"cached_players={len(listing_ids & cached_ids)} missing={len(missing)}", flush=True)
        for offset in range(0, len(missing), 400):
            chunk = missing[offset:offset + 400]
            chunk_ids = ",".join(map(str, chunk))
            # Endpoints and count alone can collide when empty IDs persist but
            # different middle IDs arrive. Bind the name to the exact requested set.
            digest = hashlib.sha256(chunk_ids.encode("ascii")).hexdigest()
            suffix = f"{chunk[0]}_{chunk[-1]}_{len(chunk)}_{digest}.json"
            print(f"players {offset}/{len(missing)}", flush=True)
            rows, low_quota = explorer.sql(
                "SELECT match_id, account_id, player_slot FROM player_matches WHERE match_id IN ("
                + chunk_ids + ")")
            _write_atomic(args.cache_dir / f"players_{suffix}", rows)
            returned_ids = {int(row["match_id"]) for row in rows}
            empty_ids = [mid for mid in chunk if mid not in returned_ids]
            if empty_ids:
                _write_atomic(args.cache_dir / f"noplayers_{suffix}", empty_ids)
            if low_quota:
                raise ValueError("daily quota reserve reached; resume next night")
    except (OSError, ValueError, TypeError, KeyError, http.client.HTTPException) as exc:
        print(f"INCOMPLETE: {exc}; published cache kept", file=sys.stderr, flush=True)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
