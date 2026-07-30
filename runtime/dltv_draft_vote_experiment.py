#!/usr/bin/env python3
"""Experiment: parse DLTV draft-vote bar (whose pick is better) for live matches.

Reuses the same DLTV endpoints already used by cyberscore_try:
  - https://dltv.org/live/series.json          → live steam_match_id → series_id
  - https://dltv.org/live/{steam_match_id}.json → db.series.likes {side_0, side_1}

UI bar (`picks__new-vote`) renders:
  radiant% = side_0 / (side_0+side_1) * 100
  dire%    = side_1 / (side_0+side_1) * 100

Does NOT touch the running cyberscore_try process.

Usage:
  /root/main/venv/bin/python3 -u /root/main/runtime/dltv_draft_vote_experiment.py
  /root/main/venv/bin/python3 -u /root/main/runtime/dltv_draft_vote_experiment.py --prefer-direct
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

ROOT = Path("/root/main")
sys.path.insert(0, str(ROOT / "base"))

from curl_cffi import requests as creq  # noqa: E402
from keys import (  # noqa: E402
    BOOKMAKER_PROXY_FALLBACK,
    BOOKMAKER_PROXY_URL,
    DLTV_PROXY_POOL,
)

OUT_DIR = ROOT / "runtime"
OUT_JSON = OUT_DIR / "dltv_draft_vote_experiment_latest.json"
OUT_LOG = OUT_DIR / "dltv_draft_vote_experiment.log"

HEADERS = {
    "Host": "dltv.org",
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://dltv.org/matches",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
}


def _normalize_proxy(raw: Any) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if "://" not in s:
        s = "http://" + s
    return s


def _proxy_host(proxy: Optional[str]) -> str:
    if not proxy:
        return "direct"
    return proxy.split("@")[-1]


def _candidate_proxies() -> list[Optional[str]]:
    """DLTV pool + bookmaker fallback/url (user's ~6 endpoints), deduped."""
    seen: set[str] = set()
    out: list[Optional[str]] = []
    for raw in list(DLTV_PROXY_POOL or []) + [
        BOOKMAKER_PROXY_FALLBACK,
        BOOKMAKER_PROXY_URL,
    ]:
        p = _normalize_proxy(raw)
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def _fetch_json(
    url: str,
    *,
    proxy: Optional[str],
    timeout: float,
) -> tuple[Any, float]:
    kw: dict[str, Any] = {
        "headers": HEADERS,
        "timeout": timeout,
        "impersonate": "chrome131",
        "verify": False,
    }
    if proxy:
        kw["proxies"] = {"http": proxy, "https": proxy}
    t0 = time.time()
    resp = creq.get(url, **kw)
    dt = time.time() - t0
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for {url}")
    return resp.json(), dt


def _probe_proxy(proxy: Optional[str], timeout: float) -> dict[str, Any]:
    host = _proxy_host(proxy)
    try:
        data, dt = _fetch_json(
            "https://dltv.org/live/series.json",
            proxy=proxy,
            timeout=timeout,
        )
        live = data.get("live") if isinstance(data, dict) else None
        n = len(live) if isinstance(live, dict) else 0
        return {"proxy": host, "ok": True, "seconds": round(dt, 2), "live_count": n}
    except Exception as exc:
        return {
            "proxy": host,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}"[:180],
        }


def _vote_from_live_payload(steam_match_id: str, series_id: Any, data: dict) -> dict:
    ser = ((data.get("db") or {}).get("series") or {}) if isinstance(data, dict) else {}
    likes = ser.get("likes") if isinstance(ser.get("likes"), dict) else {}
    s0 = float(likes.get("side_0") or 0)
    s1 = float(likes.get("side_1") or 0)
    total = s0 + s1
    ft = ((data.get("db") or {}).get("first_team") or {}) if isinstance(data, dict) else {}
    st = ((data.get("db") or {}).get("second_team") or {}) if isinstance(data, dict) else {}
    slug = ser.get("slug") or ""
    return {
        "steam_match_id": int(steam_match_id),
        "series_id": int(series_id) if str(series_id).isdigit() else series_id,
        "url": f"https://dltv.org/matches/{series_id}/{slug}" if series_id else None,
        "radiant_team": ft.get("title") or ft.get("name"),
        "dire_team": st.get("title") or st.get("name"),
        "likes_side_0_radiant": s0,
        "likes_side_1_dire": s1,
        "radiant_pct": round(100.0 * s0 / total, 1) if total else None,
        "dire_pct": round(100.0 * s1 / total, 1) if total else None,
        "is_draft_voting": ser.get("is_draft_voting"),
        "draft_vote_ended_at": ser.get("draft_vote_ended_at"),
        "game_time": data.get("game_time"),
        "is_picks_ended": data.get("is_picks_ended"),
        "radiant_lead": data.get("radiant_lead"),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--prefer-direct",
        action="store_true",
        help="Skip proxy probe; fetch via direct curl_cffi (still reports pool status).",
    )
    ap.add_argument("--proxy-timeout", type=float, default=8.0)
    ap.add_argument("--fetch-timeout", type=float, default=15.0)
    args = ap.parse_args()

    started = datetime.now(timezone.utc).isoformat()
    proxies = _candidate_proxies()
    print(f"candidate_proxies={len(proxies)} hosts={[ _proxy_host(p) for p in proxies ]}")

    probe_rows = []
    working: list[Optional[str]] = []
    if not args.prefer_direct:
        for p in proxies:
            row = _probe_proxy(p, timeout=args.proxy_timeout)
            probe_rows.append(row)
            status = "OK" if row["ok"] else "FAIL"
            print(f"  probe[{status}] {row['proxy']}: {row}")
            if row["ok"]:
                working.append(p)
    # Always keep direct as last-resort transport (same as when socks to dltv times out).
    working.append(None)

    series = None
    used_proxy = None
    series_dt = None
    errors = []
    for p in working:
        try:
            series, series_dt = _fetch_json(
                "https://dltv.org/live/series.json",
                proxy=p,
                timeout=args.fetch_timeout,
            )
            used_proxy = p
            print(f"series.json via {_proxy_host(p)} in {series_dt:.2f}s")
            break
        except Exception as exc:
            errors.append({"stage": "series", "proxy": _proxy_host(p), "error": str(exc)[:180]})
            print(f"series FAIL via {_proxy_host(p)}: {exc}")

    if not isinstance(series, dict) or not isinstance(series.get("live"), dict):
        payload = {
            "started_at": started,
            "ok": False,
            "error": "failed to load live/series.json",
            "proxy_probes": probe_rows,
            "errors": errors,
        }
        OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        print("FAILED", payload["error"])
        return 1

    live_map: dict[str, Any] = series["live"]
    matches = []
    for steam_id, series_id in live_map.items():
        row_err = None
        vote = None
        fetch_proxy = used_proxy
        fetch_dt = None
        for p in [used_proxy] + [x for x in working if x != used_proxy]:
            try:
                data, fetch_dt = _fetch_json(
                    f"https://dltv.org/live/{steam_id}.json",
                    proxy=p,
                    timeout=args.fetch_timeout,
                )
                vote = _vote_from_live_payload(str(steam_id), series_id, data)
                vote["fetched_via"] = _proxy_host(p)
                vote["fetch_seconds"] = round(fetch_dt, 2)
                fetch_proxy = p
                break
            except Exception as exc:
                row_err = f"{_proxy_host(p)}: {exc}"
                errors.append(
                    {
                        "stage": "live_json",
                        "steam_match_id": steam_id,
                        "proxy": _proxy_host(p),
                        "error": str(exc)[:180],
                    }
                )
        if vote is None:
            matches.append(
                {
                    "steam_match_id": steam_id,
                    "series_id": series_id,
                    "error": row_err or "unknown",
                }
            )
            print(f"FAIL {steam_id}: {row_err}")
            continue
        matches.append(vote)
        print(
            f"{vote.get('radiant_team')} vs {vote.get('dire_team')}: "
            f"R {vote.get('radiant_pct')}% / D {vote.get('dire_pct')}% "
            f"likes={{side_0:{vote.get('likes_side_0_radiant')}, "
            f"side_1:{vote.get('likes_side_1_dire')}}} "
            f"t={vote.get('game_time')} via {vote.get('fetched_via')}"
        )

    payload = {
        "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "ok": True,
        "artifact": {
            "series_endpoint": "https://dltv.org/live/series.json",
            "live_endpoint": "https://dltv.org/live/{steam_match_id}.json",
            "vote_field": "db.series.likes",
            "mapping": "side_0=radiant, side_1=dire; pct = count/sum*100",
            "ui_selector": "picks__new-vote / picks__new-vote__team[data-type=radiant|dire]",
            "note": "cyberscore_try already uses these endpoints; likes field was unused until this experiment",
        },
        "proxy_candidates": [_proxy_host(p) for p in proxies],
        "proxy_probes": probe_rows,
        "series_via": _proxy_host(used_proxy),
        "series_seconds": round(series_dt or 0, 2),
        "live_count": len(live_map),
        "matches": matches,
        "errors": errors,
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    with OUT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"ts": payload["finished_at"], "live": len(matches)}, ensure_ascii=False) + "\n")
    print(f"saved {OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
