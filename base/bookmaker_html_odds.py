#!/usr/bin/env python3
"""Proxy-only bookmaker odds parser (HTML/bs4 approach).

Usage:
  source venv_catboost/bin/activate
  python base/bookmaker_html_odds.py --team1 "Lynx" --team2 "Yellow Submarine"
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:
    from base.keys import BOOKMAKER_PROXIES, BOOKMAKER_PROXY_URL
except Exception:
    from keys import BOOKMAKER_PROXIES, BOOKMAKER_PROXY_URL  # type: ignore


BOOKMAKER_URLS: Dict[str, Dict[str, str]] = {
    "live": {
        "betboom": "https://betboom.ru/esport/live/dota-2",
        "pari": "https://pari.ru/esports-live/category/dota2",
        "winline": "https://winline.ru/stavki/sport/kibersport",
    },
    "all": {
        "betboom": "https://betboom.ru/esport/dota-2?period=all",
        "pari": "https://pari.ru/esports/category/dota2",
        "winline": "https://winline.ru/stavki/sport/kibersport",
    },
}
SUPPORTED_BOOKMAKER_SITES: Tuple[str, ...] = tuple(BOOKMAKER_URLS.get("live", {}).keys()) or (
    "betboom",
    "pari",
    "winline",
)

ODD_RE = re.compile(r"(?<!\d)(\d{1,2}[.,]\d{1,2})(?!\d)")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9а-я]+", " ", s.lower())).strip()


def _extract_numeric_odds(text: str, max_count: int = 8) -> List[float]:
    out: List[float] = []
    for m in ODD_RE.finditer(text):
        val = float(m.group(1).replace(",", "."))
        if 1.01 <= val <= 200.0:
            out.append(val)
    uniq: List[float] = []
    seen = set()
    for v in out:
        if v in seen:
            continue
        seen.add(v)
        uniq.append(v)
        if len(uniq) >= max_count:
            break
    return uniq


@dataclass
class SiteParseResult:
    site: str
    url: str
    status: str
    match_found: bool
    odds: List[float]
    source: str
    details: str
    market_closed: bool = False
    match_odds: List[float] = field(default_factory=list)


class ProxyOnlyHttp:
    def __init__(self, proxies: Dict[str, str], timeout: int = 30):
        if not proxies.get("http") or not proxies.get("https"):
            raise RuntimeError("Proxy-only mode requires both http and https proxies")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.proxies = proxies
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
        )

    def get(self, url: str) -> requests.Response:
        # Hard guard: never send request without configured proxies.
        if not self.session.proxies.get("http") or not self.session.proxies.get("https"):
            raise RuntimeError("Proxy-only mode violated: missing session proxies")
        return self.session.get(url, timeout=self.timeout)


def _candidate_blobs(soup: BeautifulSoup) -> List[str]:
    blobs: List[str] = []

    # Visible text block.
    visible = " ".join(soup.stripped_strings)
    if visible:
        blobs.append(visible)

    # Inline script text frequently contains store/preloaded payload.
    for script in soup.find_all("script"):
        text = script.string or script.get_text(strip=False) or ""
        if len(text) >= 120:
            blobs.append(text)

    return blobs


def _find_match_blob(blobs: List[str], team1: str, team2: str) -> Tuple[Optional[str], Optional[str]]:
    t1 = _norm(team1)
    t2 = _norm(team2)

    best_blob: Optional[str] = None
    best_snippet: Optional[str] = None
    best_distance = 10**9

    for blob in blobs:
        nblob = _norm(blob)
        p1 = nblob.find(t1)
        p2 = nblob.find(t2)
        if p1 == -1 or p2 == -1:
            continue
        dist = abs(p1 - p2)
        if dist < best_distance:
            best_distance = dist
            best_blob = blob

    if best_blob is None:
        return None, None

    raw = best_blob
    i1 = raw.lower().find(team1.lower())
    i2 = raw.lower().find(team2.lower())
    points = [x for x in (i1, i2) if x >= 0]
    center = sum(points) // len(points) if points else 0
    lo = max(0, center - 350)
    hi = min(len(raw), center + 350)
    snippet = re.sub(r"\s+", " ", raw[lo:hi]).strip()
    return best_blob, snippet


def _unique_names(names: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in names:
        value = str(raw or "").strip()
        norm = _norm(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(value)
    return out


def _find_match_blob_with_aliases(
    blobs: List[str],
    team1: str,
    team2: str,
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    team1_candidates = _unique_names([team1, *(team1_aliases or [])])
    team2_candidates = _unique_names([team2, *(team2_aliases or [])])
    best_blob: Optional[str] = None
    best_snippet: Optional[str] = None
    best_team1: Optional[str] = None
    best_team2: Optional[str] = None
    best_score: Optional[Tuple[int, int]] = None

    for candidate_team1 in team1_candidates:
        for candidate_team2 in team2_candidates:
            match_blob, snippet = _find_match_blob(blobs, candidate_team1, candidate_team2)
            if match_blob is None:
                continue
            score = (-len(_norm(candidate_team1)) - len(_norm(candidate_team2)), len(snippet or ""))
            if best_score is None or score < best_score:
                best_score = score
                best_blob = match_blob
                best_snippet = snippet
                best_team1 = candidate_team1
                best_team2 = candidate_team2

    return best_blob, best_snippet, best_team1, best_team2


def parse_site(
    http: ProxyOnlyHttp,
    site: str,
    url: str,
    team1: str,
    team2: str,
    team1_aliases: Optional[List[str]] = None,
    team2_aliases: Optional[List[str]] = None,
) -> SiteParseResult:
    try:
        resp = http.get(url)
    except Exception as exc:
        return SiteParseResult(
            site=site,
            url=url,
            status="request_error",
            match_found=False,
            odds=[],
            source="request_error",
            details=str(exc),
        )

    if resp.status_code != 200:
        return SiteParseResult(
            site=site,
            url=url,
            status=f"http_{resp.status_code}",
            match_found=False,
            odds=[],
            source="http_error",
            details="non-200 response",
        )

    soup = BeautifulSoup(resp.text, "html.parser")
    blobs = _candidate_blobs(soup)
    match_blob, snippet, matched_team1, matched_team2 = _find_match_blob_with_aliases(
        blobs,
        team1,
        team2,
        team1_aliases=team1_aliases,
        team2_aliases=team2_aliases,
    )

    if not match_blob:
        return SiteParseResult(
            site=site,
            url=url,
            status="ok",
            match_found=False,
            odds=[],
            source="html_payload_missing",
            details="match not found in current HTML payload (likely JS-only data)",
        )

    odds = _extract_numeric_odds(match_blob)
    return SiteParseResult(
        site=site,
        url=url,
        status="ok",
        match_found=True,
        odds=odds[:2],
        source="html_payload_match",
        details=(
            f"matched_as={matched_team1 or team1} vs {matched_team2 or team2}; "
            f"{snippet or 'match found'}"
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--team1", required=True)
    parser.add_argument("--team2", required=True)
    parser.add_argument("--team1-alias", action="append", default=[])
    parser.add_argument("--team2-alias", action="append", default=[])
    parser.add_argument("--mode", choices=["live", "all"], default="live")
    parser.add_argument("--map-num", type=int, default=None)
    parser.add_argument(
        "--sites",
        nargs="*",
        default=list(SUPPORTED_BOOKMAKER_SITES),
        choices=list(SUPPORTED_BOOKMAKER_SITES),
    )
    args = parser.parse_args()

    urls = dict(BOOKMAKER_URLS.get(args.mode) or BOOKMAKER_URLS["live"])
    http = ProxyOnlyHttp(BOOKMAKER_PROXIES)
    results = [
        parse_site(
            http=http,
            site=site,
            url=urls[site],
            team1=args.team1,
            team2=args.team2,
            team1_aliases=args.team1_alias,
            team2_aliases=args.team2_alias,
        )
        for site in args.sites
    ]

    payload = {
        "proxy_url": BOOKMAKER_PROXY_URL,
        "mode": args.mode,
        "query": {
            "team1": args.team1,
            "team2": args.team2,
            "team1_aliases": list(args.team1_alias or []),
            "team2_aliases": list(args.team2_alias or []),
            "map_num": args.map_num,
        },
        "results": [r.__dict__ for r in results],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
