#!/usr/bin/env python3
"""Repair recent pro maps by ID without crawling historical corpus parts.

The caller owns the topup lock. CLI dry-run is strictly read-only, including
retryMatchDownload. All Stratz traffic uses the shared proxy/token pool.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# Copied from proceed_get_maps_with_data(pro=True), used by get_pros.
# tests/test_backfill_by_id.py compares the parsed source selection recursively.
MATCH_FIELDS = """
id
didRadiantWin
towerDeaths {
time
isRadiant
npcId
}
bottomLaneOutcome
topLaneOutcome
midLaneOutcome
winRates
firstBloodTime
averageImp
regionId
radiantTeam {
name
id
}
direTeam {
name
id
}
startDateTime
durationSeconds
leagueId
series {
id
type
}
direKills
radiantKills
radiantNetworthLeads
radiantExperienceLeads
players {
position
isRadiant
kills
assists
numDenies
numLastHits
goldPerMinute
networth
experiencePerMinute
level
heroDamage
heroHealing
towerDamage
item0Id
item1Id
item2Id
item3Id
item4Id
item5Id
backpack0Id
backpack1Id
backpack2Id
heroId
neutral0Id
invisibleSeconds
dotaPlusHeroXp
imp
deaths
intentionalFeeding
steamAccount {
id
smurfFlag
isAnonymous
}
}
"""

BATCH_SIZE = 20
MAX_PART_BYTES = 500 * 1024 * 1024
RETRY_SECONDS = 3 * 86400
RETRY_FILE = "backfill_retry_download.json"


class TransportStop(BaseException):
    """Escape the pool's automatic retries on quota/HTTP stop conditions."""


def maps_module():
    root = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
    sys.path.insert(0, str(root / "base"))
    import maps_research
    return maps_research


@contextmanager
def bounded_stratz(M, max_calls):
    """Count physical POSTs (including pool retries); restore transport always.

    The topup is sequential under its lock. Wrapping its existing transport
    preserves pool selection, authorization and all rate-limit accounting.
    """
    original = M.cf_requests.post
    calls = [0]

    def post(*args, **kwargs):
        if calls[0] >= max_calls:
            raise TransportStop("Stratz physical-call cap reached")
        if not (kwargs.get("proxies") or {}).get("https"):
            raise TransportStop("Stratz proxy missing; refusing direct request")
        calls[0] += 1
        response = original(*args, **kwargs)
        if response.status_code in (429, 522):
            raise TransportStop("Stratz HTTP %d; stopping" % response.status_code)
        # Some gateways deliver Stratz's rate-limit body with HTTP 200.
        if response.status_code == 200:
            try:
                data = response.json()
            except ValueError:
                data = None
            if isinstance(data, dict) and data.get("message") == "API rate limit exceeded":
                raise TransportStop("Stratz rate limit; stopping")
        return response

    M.cf_requests.post = post
    try:
        yield calls
    finally:
        M.cf_requests.post = original


def read_json(path):
    with (gzip.open(path, "rb") if path.suffix == ".gz" else open(path, "rb")) as fh:
        return json.load(fh)


def complete(record):
    players = record.get("players") if isinstance(record, dict) else None
    return (isinstance(players, list) and len(players) == 10
            and all(isinstance(p, dict) and p.get("position") is not None for p in players))


def scan_recent(corpus, since, M, now=None):
    """Load one recent part at a time; caches skip historical IDs, not repairs."""
    now = int(time.time()) if now is None else int(now)
    processed_path = corpus / "processed_ids.txt"
    processed = set(int(mid) for mid in read_json(processed_path)) if processed_path.exists() else set()
    manifest = M._load_scan_manifest(corpus)
    for cached in manifest.values():
        processed.update(int(mid) for mid in cached[2])
    # get_pros and write_records bucket startDateTime in [start, end).
    # An unchanged, cached part in a closed older bucket has no window maps;
    # its IDs above still participate in deduplication. Uncached/stale parts
    # must be read to preserve processed, even when their bucket is old.
    old_patches = {str(name) for name, start, end in M.DOTA_PATCH_SPECS
                   if end is not None and int(end) <= since
                   and str(name) != M.OUTSIDE_PATCH_BUCKET}
    # The writer resolves a start outside every patch interval (or no start)
    # to the outside bucket (maps_research._resolve_patch_name). With
    # gap-free intervals open to the future and since inside them, a record
    # with since <= startDateTime cannot be stored there. Its IDs reach
    # processed through processed_ids.txt, as in the merge's own dedup
    # (maps_research existing_part_files never scans this bucket). Its parts
    # are 500 MiB each: parsing them after a migration reset mtimes was the
    # serv1 thrash of 06.10.2026.
    spans = sorted((int(start), None if end is None else int(end))
                   for name, start, end in M.DOTA_PATCH_SPECS)
    outside_closed = (
        processed_path.exists() and bool(spans) and spans[0][0] <= since
        and spans[-1][1] is None
        and all(spans[i][1] == spans[i + 1][0] for i in range(len(spans) - 1))
        and M.OUTSIDE_PATCH_BUCKET not in {str(p[0]) for p in M.DOTA_PATCH_SPECS})
    locations, unfinished, valid = {}, set(), set()
    for path in sorted(M._corpus_json_paths(corpus, "*_part*")):
        if path.stat().st_mtime < since:
            continue
        part = re.fullmatch(r"(.+)_part[0-9]+\.json(?:\.gz)?", path.name)
        cached = manifest.get(path.name)
        if (part and part.group(1) in old_patches and cached and cached[2]
                and cached[:2] == M._file_stamp(path)):
            continue
        if part and part.group(1) == M.OUTSIDE_PATCH_BUCKET and outside_closed:
            continue
        data = read_json(path)  # A corrupt part aborts rather than permits duplicates.
        if not isinstance(data, dict):
            raise ValueError("part is not an object: %s" % path.name)
        ids = []
        for key, record in data.items():
            mid = int(record.get("id") or key) if isinstance(record, dict) else int(key)
            ids.append(mid)
            processed.add(mid)
            locations.setdefault(mid, []).append((path, key))
            if complete(record):
                valid.add(mid)
            elif isinstance(record, dict) and since <= (record.get("startDateTime") or 0) <= now:
                unfinished.add(mid)
        manifest[path.name] = M._file_stamp(path) + [ids]
    # Pre-existing duplicates cannot safely be repaired by appending another copy.
    for mid in unfinished:
        if len(locations[mid]) != 1:
            raise ValueError("duplicate stored match %d; needs corpus-owner repair" % mid)
    return processed, manifest, locations, unfinished - valid


def opendota_ids(since, max_pages, now=None):
    now = int(time.time()) if now is None else int(now)
    candidates = set()
    cursor = None
    for _ in range(max_pages):
        url = "https://api.opendota.com/api/proMatches"
        if cursor is not None:
            url += "?" + urlencode({"less_than_match_id": cursor})
        with urlopen(Request(url, headers={"User-Agent": "ingame-pro-corpus-backfill/1.0"}),
                     timeout=40) as response:
            page = json.load(response)
        if not isinstance(page, list):
            raise ValueError("OpenDota proMatches is not a list")
        if not page:
            return candidates
        next_cursor = min(int(m["match_id"]) for m in page)
        if cursor is not None and next_cursor >= cursor:
            raise ValueError("OpenDota cursor did not advance")
        for match in page:
            if since <= int(match["start_time"]) <= now:
                candidates.add(int(match["match_id"]))
        if any(int(m["start_time"]) < since for m in page):
            return candidates
        cursor = next_cursor
        time.sleep(1.1)
    raise ValueError("OpenDota page cap reached before window start")


async def stratz(M, query):
    data = await M.get_proxy_pool().make_request(
        url="https://api.stratz.com/graphql", json={"query": query},
        headers={"Content-Type": "application/json", "Accept": "application/json",
                 "Accept-Encoding": "gzip, deflate, br, zstd",
                 "Origin": "https://api.stratz.com",
                 # Never put the query into Referer: 20 aliases make an 18 KB
                 # header and Cloudflare answers 520 (lead probe 05.10.2026).
                 "Referer": "https://api.stratz.com/graphiql",
                 "User-Agent": "STRATZ_API"})
    if not isinstance(data, dict) or data.get("errors") or not isinstance(data.get("data"), dict):
        raise ValueError("Stratz missing data or GraphQL errors: %s" % str(data)[:300])
    return data["data"]


def publish_part(M, corpus, path, data, manifest):
    ids = [int(m.get("id") or k) if isinstance(m, dict) else int(k)
           for k, m in data.items()]
    payload = M.orjson.dumps(data)
    if path.suffix == ".gz":
        payload = gzip.compress(payload)
    M._atomic_write_bytes(path, payload)
    manifest[path.name] = M._file_stamp(path) + [ids]
    M._save_scan_manifest(corpus, manifest)


def write_records(M, corpus, records, locations, processed, manifest, stats):
    """Same patch buckets, 500 MiB rotation, gzip flag and durable counters.

    Reuse the get_pros writer primitives without invoking its historical scan.
    Publish part -> manifest/counter -> processed cache, so interruptions recover
    from the next recent-part scan even when the processed cache was not saved.
    """
    replacements, new = {}, {}
    for mid, record in records.items():
        # Match get_pros' league enrichment without changing the fetched input.
        record = dict(record)
        league = dict(record.get("league") or {})
        league_id = record.get("leagueId") or league.get("id")
        if league.get("id") is None and league_id is not None:
            league["id"] = int(league_id)
        if not league.get("tier"):
            league["tier"] = "UNKNOWN"
        record["league"] = league
        if mid in locations:
            if len(locations[mid]) != 1:
                raise ValueError("duplicate stored match %d" % mid)
            path, key = locations[mid][0]
            replacements.setdefault(path, {})[key] = record
        else:
            # New candidates already exclude processed IDs; repairs have locations.
            patch = next((str(name) for name, start, end in M.DOTA_PATCH_SPECS
                          if start <= record["startDateTime"] and
                          (end is None or record["startDateTime"] < end)), None)
            if patch is None:
                if M.MERGE_DROP_OUTSIDE_PATCH:
                    stats["errors"] += 1
                    continue
                patch = M.OUTSIDE_PATCH_BUCKET
            new.setdefault(patch, {})[str(mid)] = record
    for path, updates in replacements.items():
        data = read_json(path)
        for key, record in updates.items():
            if isinstance(data[key], dict) and isinstance(data[key].get("league"), dict):
                record["league"] = data[key]["league"]
        data.update(updates)
        publish_part(M, corpus, path, data, manifest)
        stats["replaced"] += len(updates)
        processed.update(int(m["id"]) for m in updates.values())
    counters = M._load_part_counters(corpus)
    numbers = M._next_part_numbers(corpus, new, counters)
    for patch, data in new.items():
        chunk, size = {}, 2

        def flush():
            suffix = ".json.gz" if os.getenv("PRO_CORPUS_GZIP") == "1" else ".json"
            path = corpus / ("%s_part%03d%s" % (patch, numbers[patch], suffix))
            publish_part(M, corpus, path, chunk, manifest)
            counters[patch] = numbers[patch]
            M._save_part_counters(corpus, counters)
            numbers[patch] += 1
            stats["new_written"] += len(chunk)
            processed.update(int(mid) for mid in chunk)

        for mid, record in data.items():
            entry_size = len(M.orjson.dumps(mid)) + 1 + len(M.orjson.dumps(record))
            if chunk and size + 1 + entry_size > MAX_PART_BYTES:
                flush()
                chunk, size = {}, 2
            size += entry_size + bool(chunk)
            chunk[mid] = record
        if chunk:
            flush()
    if replacements or new:
        M._atomic_write_bytes(corpus / "processed_ids.txt", M.orjson.dumps(sorted(processed)))


async def fetch_and_write(M, corpus, since, candidates, locations, processed,
                          manifest, dry_run, now, stats):
    retry_path = corpus / RETRY_FILE
    retry = read_json(retry_path) if retry_path.exists() else {}
    consecutive_errors = 0
    for offset in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[offset:offset + BATCH_SIZE]
        query = "query { " + " ".join("m%d: match(id: %d) { %s }" % (mid, mid, MATCH_FIELDS)
                                     for mid in batch) + " }"
        try:
            data = await stratz(M, query)
            # Validate the whole response before any record/state publication.
            for mid in batch:
                alias = "m%d" % mid
                if alias not in data:
                    raise ValueError("Stratz omitted requested alias %s" % alias)
                record = data[alias]
                if record is not None and (not isinstance(record, dict) or record.get("id") != mid):
                    raise ValueError("Stratz returned wrong match id for %d" % mid)
        except Exception as exc:
            consecutive_errors += 1
            stats["errors"] += 1
            print("ВНИМАНИЕ: backfill-by-id batch at %d failed (%d/3): %s%s" %
                  (offset, consecutive_errors, str(exc)[:300],
                   "; stopping" if consecutive_errors == 3 else ""), flush=True)
            if consecutive_errors == 3:
                return
            continue
        consecutive_errors = 0
        records, nulls = {}, []
        for mid in batch:
            alias = "m%d" % mid
            record = data[alias]
            if record is None:
                stats["stratz_null"] += 1
                nulls.append(mid)
                continue
            stats["fetched"] += 1
            if not complete(record):
                stats["still_unparsed"] += 1
            elif since <= (record.get("startDateTime") or 0) <= now:
                if getattr(M, "PRO_REQUIRE_LEAGUE", False):
                    league = record.get("league") or {}
                    league_id = record.get("leagueId") or league.get("id")
                    if league_id is None or league.get("tier") == "AMATEUR":
                        stats["league_skipped"] += 1
                        continue
                records[mid] = record
            else:
                stats["errors"] += 1
                print("ВНИМАНИЕ: Stratz match %d outside window" % mid, flush=True)
        if not dry_run:
            write_records(M, corpus, records, locations, processed, manifest, stats)
            for mid in nulls:
                previous = retry.get(str(mid))
                last = datetime.fromisoformat(previous).timestamp() if previous else 0
                if now - last < RETRY_SECONDS:
                    continue
                # Persist BEFORE sending: a crash/timeout must not repeat a mutation.
                retry[str(mid)] = datetime.fromtimestamp(now, timezone.utc).isoformat()
                M._atomic_write_bytes(retry_path, M.orjson.dumps(retry))
                try:
                    result = await stratz(M, "mutation { retryMatchDownload(matchId: %d) }" % mid)
                    if not isinstance(result.get("retryMatchDownload"), bool):
                        raise ValueError("Stratz retryMatchDownload missing boolean")
                except Exception as exc:
                    stats["errors"] += 1
                    print("ВНИМАНИЕ: backfill-by-id retryMatchDownload %d failed: %s" %
                          (mid, str(exc)[:300]), flush=True)
                    continue
                stats["retry_requested"] += 1


def backfill_by_id(since=None, corpus_dir=None, dry_run=False, max_pages=15,
                   max_stratz_calls=500, M=None, now=None):
    """Return counts; skip failed fetch batches, stopping after three in a row."""
    M = M or maps_module()
    now = int(time.time()) if now is None else int(now)
    since = now - int(os.getenv("TOPUP_DAYS", "10")) * 86400 if since is None else int(since)
    corpus = Path(corpus_dir) if corpus_dir is not None else Path(M.PRO_HEROES_DIR) / "json_parts_split_from_object"
    stats = dict.fromkeys(("candidates", "fetched", "new_written", "replaced",
                          "still_unparsed", "stratz_null", "retry_requested", "errors",
                          "league_skipped"), 0)
    try:
        processed, manifest, locations, unfinished = scan_recent(corpus, since, M, now)
        discovered = opendota_ids(since, max_pages, now)
        candidates = sorted(unfinished | (discovered - processed))
        stats["candidates"] = len(candidates)
        with bounded_stratz(M, max_stratz_calls):
            asyncio.run(fetch_and_write(M, corpus, since, candidates, locations, processed,
                                        manifest, dry_run, now, stats))
    except (Exception, TransportStop) as exc:
        stats["errors"] += 1
        print("ВНИМАНИЕ: backfill-by-id stopped: %s" % exc, flush=True)
    finally:
        print("backfill-by-id: " + " ".join("%s=%d" % item for item in stats.items()), flush=True)
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", type=lambda s: int(datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()))
    parser.add_argument("--dry-run", action="store_true", help="No corpus/state writes or retry mutations")
    parser.add_argument("--corpus-dir", type=Path)
    parser.add_argument("--max-pages", type=int, default=15)
    parser.add_argument("--max-stratz-calls", type=int, default=500)
    args = parser.parse_args()
    if args.max_pages < 1 or args.max_stratz_calls < 1:
        parser.error("request caps must be positive")
    if not args.dry_run:
        # Reuse exactly the topup lock; no concurrent part/cache publication.
        from topup_pro_corpus import LOCK
        LOCK.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(str(LOCK), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            parser.error("topup lock exists: %s" % LOCK)
        with os.fdopen(fd, "w") as fh:
            fh.write(str(os.getpid()))
        try:
            result = backfill_by_id(**vars(args))
        finally:
            try:
                if LOCK.read_text() == str(os.getpid()):
                    LOCK.unlink(missing_ok=True)
            except FileNotFoundError:
                pass
    else:
        result = backfill_by_id(**vars(args))
    return int(result["errors"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
