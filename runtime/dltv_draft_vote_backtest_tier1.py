#!/usr/bin/env python3
"""Backtest DLTV draft-vote (radiant_likes/dire_likes) on Tier-1 pro maps.

Source: pro_heroes_data/json_parts_split_from_object/7.41*.json (unique steam_id)
Filter: >=1 team in id_to_names.tier_one_teams
Vote: series HTML maps[] radiant_likes/dire_likes (series.likes wiped after finish)
Pick: higher pct side. Buckets: 55=(50.1-55], 60=(55.1-60], ... = ceil(max/5)*5

Does NOT touch running cyberscore_try.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

ROOT = Path("/root/main")
sys.path.insert(0, str(ROOT / "base"))

import orjson  # noqa: E402
from curl_cffi import requests as creq  # noqa: E402
from id_to_names import tier_one_teams  # noqa: E402

PRO_DIR = ROOT / "pro_heroes_data" / "json_parts_split_from_object"
OUT_JSON = ROOT / "runtime" / "dltv_draft_vote_backtest_tier1.json"
OUT_LOG = ROOT / "runtime" / "dltv_draft_vote_backtest_tier1.log"

HEADERS = {
    "Host": "dltv.org",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    "Referer": "https://dltv.org/matches",
}


def _tier1_ids() -> set[int]:
    out: set[int] = set()
    for value in tier_one_teams.values():
        if isinstance(value, (set, list, tuple)):
            for raw in value:
                try:
                    out.add(int(raw))
                except (TypeError, ValueError):
                    pass
        else:
            try:
                out.add(int(value))
            except (TypeError, ValueError):
                pass
    return out


def _load_tier1_matches(limit: int) -> list[dict[str, Any]]:
    t1 = _tier1_ids()
    by_id: dict[int, dict[str, Any]] = {}
    for path in sorted(PRO_DIR.glob("7.41*.json")):
        data = orjson.loads(path.read_bytes())
        if not isinstance(data, dict):
            continue
        for mid, match in data.items():
            if not isinstance(match, dict):
                continue
            radiant = match.get("radiantTeam") or {}
            dire = match.get("direTeam") or {}
            try:
                rid = int(radiant.get("id"))
                did = int(dire.get("id"))
                steam_id = int(mid)
            except (TypeError, ValueError):
                continue
            if rid not in t1 and did not in t1:
                continue
            start_time = int(match.get("startDateTime") or 0)
            row = {
                "steam_id": steam_id,
                "start_time": start_time,
                "radiant_team_id": rid,
                "dire_team_id": did,
                "radiant_team": radiant.get("name"),
                "dire_team": dire.get("name"),
                "did_radiant_win": match.get("didRadiantWin"),
                "league": (match.get("league") or {}).get("id"),
                "source_file": path.name,
            }
            prev = by_id.get(steam_id)
            if prev is None or start_time >= prev["start_time"]:
                by_id[steam_id] = row
    rows = sorted(by_id.values(), key=lambda r: (r["start_time"], r["steam_id"]))
    return rows[-limit:] if limit > 0 else rows


def _fetch(url: str, *, accept: str, timeout: float = 20.0) -> Any:
    resp = creq.get(
        url,
        headers={**HEADERS, "Accept": accept},
        timeout=timeout,
        impersonate="chrome131",
        verify=False,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for {url}")
    return resp


def _series_from_live_json(steam_id: int) -> dict[str, Any]:
    data = _fetch(
        f"https://dltv.org/live/{steam_id}.json",
        accept="application/json",
    ).json()
    series = ((data.get("db") or {}).get("series") or {}) if isinstance(data, dict) else {}
    if not series.get("id") or not series.get("slug"):
        raise RuntimeError(f"no series for steam_id={steam_id}")
    return {
        "series_id": int(series["id"]),
        "slug": str(series["slug"]),
        "live_winner": data.get("winner"),
    }


def _parse_maps_from_series_html(html: str) -> list[dict[str, Any]]:
    for match in re.finditer(r'"maps"\s*:\s*\[', html):
        start = match.end() - 1
        depth = 0
        end = None
        for i, ch in enumerate(html[start : start + 3_000_000]):
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    end = start + i + 1
                    break
        if end is None:
            continue
        blob = html[start:end]
        if "radiant_likes" not in blob:
            continue
        try:
            maps = json.loads(blob)
        except json.JSONDecodeError:
            continue
        if isinstance(maps, list) and maps:
            return [m for m in maps if isinstance(m, dict)]
    return []


def _vote_bucket(max_pct: float) -> Optional[int]:
    if max_pct <= 50.0:
        return None
    return int(math.ceil(max_pct / 5.0) * 5)


def _bucket_range(label: int) -> str:
    return f"{label - 5 + 0.1:.1f}-{label:g}"


def _find_map_in_caches(
    steam_id: int,
    series_cache: dict[int, list[dict[str, Any]]],
) -> Optional[int]:
    for sid, maps in series_cache.items():
        for mp in maps:
            try:
                if int(mp.get("steam_id") or 0) == steam_id:
                    return sid
            except (TypeError, ValueError):
                continue
    return None


def _score_row(
    match: dict[str, Any],
    mp: dict[str, Any],
    *,
    series_id: int,
    series_url: str,
    live_winner: Any = None,
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    r_likes = float(mp.get("radiant_likes") or 0)
    d_likes = float(mp.get("dire_likes") or 0)
    total = r_likes + d_likes
    if total <= 0:
        return None, "zero_likes"
    r_pct = 100.0 * r_likes / total
    d_pct = 100.0 * d_likes / total
    if r_pct == d_pct:
        return None, "tie"
    pick = "radiant" if r_pct > d_pct else "dire"
    max_pct = max(r_pct, d_pct)
    bucket = _vote_bucket(max_pct)
    if bucket is None:
        return None, "max_pct_le_50"
    actual = None
    if match["did_radiant_win"] is True:
        actual = "radiant"
    elif match["did_radiant_win"] is False:
        actual = "dire"
    else:
        w = str(mp.get("winner") or live_winner or "").lower()
        if w in {"radiant", "dire"}:
            actual = w
    if actual is None:
        return None, "no_winner"
    row = dict(match)
    row.update(
        {
            "series_id": series_id,
            "series_url": series_url,
            "radiant_likes": r_likes,
            "dire_likes": d_likes,
            "radiant_pct": round(r_pct, 1),
            "dire_pct": round(d_pct, 1),
            "pick": pick,
            "max_pct": round(max_pct, 1),
            "bucket": bucket,
            "bucket_range": _bucket_range(bucket),
            "actual": actual,
            "hit": pick == actual,
            "map_winner_field": mp.get("winner"),
            "dltv_map_id": mp.get("id"),
        }
    )
    return row, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--target-scored", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.1)
    args = ap.parse_args()

    started = datetime.now(timezone.utc).isoformat()
    pool_limit = args.limit
    if args.target_scored:
        pool_limit = max(args.limit, min(1013, max(args.target_scored * 4, 800)))

    matches = _load_tier1_matches(pool_limit)
    print(
        f"loaded_tier1_unique_tail={len(matches)} pool_limit={pool_limit} "
        f"target_scored={args.target_scored or 'all_pool'}"
    )
    if not matches:
        return 1

    by_steam = {m["steam_id"]: m for m in matches}
    pending = set(by_steam)
    series_cache: dict[int, list[dict[str, Any]]] = {}
    series_meta: dict[int, dict[str, Any]] = {}
    rows_out: list[dict[str, Any]] = []
    scored_ids: set[int] = set()
    skipped: dict[str, int] = defaultdict(int)
    buckets: dict[int, dict[str, int]] = defaultdict(lambda: {"n": 0, "hits": 0})

    order = sorted(matches, key=lambda r: (r["start_time"], r["steam_id"]), reverse=True)

    def commit(row: dict[str, Any]) -> None:
        sid = int(row["steam_id"])
        if sid in scored_ids:
            return
        scored_ids.add(sid)
        pending.discard(sid)
        rows_out.append(row)
        b = int(row["bucket"])
        buckets[b]["n"] += 1
        buckets[b]["hits"] += int(bool(row["hit"]))
        tgt = f"/{args.target_scored}" if args.target_scored else ""
        print(
            f"[scored {len(scored_ids)}{tgt}] "
            f"{row.get('radiant_team')} vs {row.get('dire_team')} "
            f"R{row['radiant_pct']}/D{row['dire_pct']} pick={row['pick']} "
            f"actual={row['actual']} bucket={row['bucket']} hit={row['hit']}"
        )

    def ingest(series_id: int, live_winner: Any = None) -> None:
        maps = series_cache.get(series_id) or []
        url = (series_meta.get(series_id) or {}).get("url", "")
        for mp in maps:
            try:
                steam_id = int(mp.get("steam_id") or 0)
            except (TypeError, ValueError):
                continue
            if steam_id not in pending or steam_id in scored_ids:
                continue
            match = by_steam.get(steam_id)
            if not match:
                continue
            row, reason = _score_row(
                match, mp, series_id=series_id, series_url=url, live_winner=live_winner
            )
            if row is None:
                skipped[reason or "unknown"] += 1
                pending.discard(steam_id)
                rows_out.append({**match, "error": reason, "series_id": series_id})
                continue
            commit(row)

    for i, match in enumerate(order, 1):
        if args.target_scored and len(scored_ids) >= args.target_scored:
            print(
                f"reached target_scored={args.target_scored} "
                f"after {i-1} probes; pending_left={len(pending)}"
            )
            break
        steam_id = match["steam_id"]
        if steam_id not in pending or steam_id in scored_ids:
            continue

        cached_sid = _find_map_in_caches(steam_id, series_cache)
        if cached_sid is not None:
            ingest(cached_sid)
            if steam_id not in pending or steam_id in scored_ids:
                continue

        try:
            meta = _series_from_live_json(steam_id)
            sid = int(meta["series_id"])
            if sid not in series_cache:
                url = f"https://dltv.org/matches/{sid}/{meta['slug']}"
                try:
                    html = _fetch(url, accept="text/html").text
                except Exception:
                    url = f"https://dltv.org/matches/{sid}"
                    html = _fetch(url, accept="text/html").text
                series_cache[sid] = _parse_maps_from_series_html(html)
                series_meta[sid] = {"url": url, "maps": len(series_cache[sid])}
                time.sleep(args.sleep)
            ingest(sid, live_winner=meta.get("live_winner"))
            if steam_id in pending and steam_id not in scored_ids:
                skipped["map_not_scored_after_series"] += 1
                pending.discard(steam_id)
                rows_out.append({**match, **meta, "error": "map_not_scored_after_series"})
                print(
                    f"[probe {i}/{len(order)} scored {len(scored_ids)}] "
                    f"{steam_id} SKIP map_not_scored_after_series"
                )
        except Exception as exc:
            skipped["fetch_error"] += 1
            pending.discard(steam_id)
            err = f"{type(exc).__name__}: {exc}"[:200]
            rows_out.append({**match, "error": err})
            print(f"[probe {i}/{len(order)} scored {len(scored_ids)}] {steam_id} ERROR {err}")
        time.sleep(args.sleep)

    summary_buckets = []
    for label in sorted(buckets):
        n = buckets[label]["n"]
        hits = buckets[label]["hits"]
        summary_buckets.append(
            {
                "bucket": label,
                "range": _bucket_range(label),
                "n": n,
                "hits": hits,
                "wr": round(100.0 * hits / n, 1) if n else None,
            }
        )

    scored = [r for r in rows_out if r.get("bucket") is not None and "hit" in r]
    overall_n = len(scored)
    overall_hits = sum(1 for r in scored if r.get("hit"))
    payload = {
        "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "limit": pool_limit,
        "target_scored": args.target_scored or None,
        "tier1_ids": len(_tier1_ids()),
        "matched_tail_unique": len(matches),
        "scanned_rows": len(rows_out),
        "scored": overall_n,
        "overall_hits": overall_hits,
        "overall_wr": round(100.0 * overall_hits / overall_n, 1) if overall_n else None,
        "skipped": dict(skipped),
        "buckets": summary_buckets,
        "series_pages_fetched": len(series_cache),
        "matches": scored,
        "method": {
            "vote_fields": "maps[].radiant_likes / maps[].dire_likes from DLTV series HTML",
            "pick": "side with higher pct",
            "bucket": "label=ceil(max_pct/5)*5; 55 means 50.1-55, 60 means 55.1-60, ...",
            "tier_filter": ">=1 team in id_to_names.tier_one_teams",
            "dedupe": "unique steam_id",
            "series_reuse": "all maps from a series HTML counted when any sibling is probed",
        },
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    with OUT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {
                    "ts": payload["finished_at"],
                    "scored": overall_n,
                    "overall_wr": payload["overall_wr"],
                    "buckets": summary_buckets,
                },
                ensure_ascii=False,
            )
            + "\n"
        )

    print("\n=== BUCKET WR ===")
    for b in summary_buckets:
        print(f"  {b['bucket']:>3} ({b['range']}): n={b['n']} hits={b['hits']} wr={b['wr']}%")
    print(f"overall: n={overall_n} wr={payload['overall_wr']}% skipped={dict(skipped)}")
    print(f"saved {OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
