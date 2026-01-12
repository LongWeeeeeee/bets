#!/usr/bin/env python3
"""
Fetch hero vs hero matchup data from Stratz GraphQL API.

Uses the same ProxyAPIPool pattern as base/maps_research.py for reliable API access.
Saves to data/hero_matchups.json with structure:
{
    "hero_id": {
        "vs_hero_id": advantage_score,  # positive = counters, negative = countered by
        ...
    }
}
"""

import asyncio
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import aiohttp

# Import API keys from base
sys.path.insert(0, str(Path(__file__).parent.parent / "base"))
from keys import api_to_proxy

# Stratz GraphQL endpoint
STRATZ_API_URL = "https://api.stratz.com/graphql"

# All hero IDs in Dota 2 (1-145, with gaps)
ALL_HERO_IDS: List[int] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
    42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
    62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81,
    82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101,
    102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 119, 120, 121,
    123, 126, 128, 129, 131, 135, 136, 137, 138
]


def get_week_timestamp() -> int:
    """Get Unix timestamp for the start of current week (Monday 00:00 UTC)."""
    now = datetime.utcnow()
    days_since_monday = now.weekday()
    monday = now - timedelta(days=days_since_monday)
    monday_start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(monday_start.timestamp())


def build_matchup_query(hero_id: int) -> str:
    """
    Build GraphQL query for hero matchups.
    
    Args:
        hero_id: The hero to get matchups for
        
    Returns:
        GraphQL query string
    """
    return f"""
    {{
        heroStats {{
            matchUp(
                heroId: {hero_id}
                bracketBasicIds: [DIVINE_IMMORTAL]
            ) {{
                advantage {{
                    heroId
                    with {{
                        heroId2
                        synergy
                        winsAverage
                        matchCount
                    }}
                    vs {{
                        heroId2
                        synergy
                        winsAverage
                        matchCount
                    }}
                }}
            }}
        }}
    }}
    """


class StratzAPIClient:
    """Async client for Stratz API with proxy rotation and rate limiting."""
    
    def __init__(self, api_to_proxy_dict: Dict[str, str]):
        self.credentials = list(api_to_proxy_dict.items())
        self.current_index = 0
        self.rate_limited_until: Dict[int, float] = {}  # index -> timestamp
        
    def _get_headers(self, token: str, query: str) -> Dict[str, str]:
        """Build headers matching the working pattern from maps_research.py."""
        encoded_query = quote(query)
        referer = f"https://api.stratz.com/graphiql?query={encoded_query}"
        
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Origin": "https://api.stratz.com",
            "Referer": referer,
            "User-Agent": "STRATZ_API",
            "Authorization": f"Bearer {token}"
        }
    
    async def _get_available_credential(self) -> tuple[int, str, str]:
        """Get next available credential, waiting if all are rate limited."""
        now = time.time()
        
        for _ in range(len(self.credentials)):
            idx = self.current_index
            self.current_index = (self.current_index + 1) % len(self.credentials)
            
            # Check if this credential is rate limited
            if idx in self.rate_limited_until:
                if now < self.rate_limited_until[idx]:
                    continue
                else:
                    del self.rate_limited_until[idx]
            
            proxy_url, token = self.credentials[idx]
            return idx, proxy_url, token
        
        # All credentials are rate limited, wait for the earliest one
        min_wait = min(self.rate_limited_until.values()) - now
        if min_wait > 0:
            print(f"⏳ All credentials rate limited, waiting {min_wait:.1f}s...")
            await asyncio.sleep(min_wait + 1)
        
        # Clear expired limits and try again
        now = time.time()
        self.rate_limited_until = {k: v for k, v in self.rate_limited_until.items() if v > now}
        
        idx = self.current_index
        self.current_index = (self.current_index + 1) % len(self.credentials)
        proxy_url, token = self.credentials[idx]
        return idx, proxy_url, token
    
    async def fetch_hero_matchups(
        self,
        hero_id: int,
        max_retries: int = 5
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch matchup data for a single hero with retry logic.
        
        Args:
            hero_id: Hero ID to fetch matchups for
            max_retries: Maximum retry attempts
            
        Returns:
            Parsed matchup data or None on failure
        """
        query = build_matchup_query(hero_id)
        
        for attempt in range(max_retries):
            idx, proxy_url, token = await self._get_available_credential()
            headers = self._get_headers(token, query)
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        STRATZ_API_URL,
                        proxy=proxy_url,
                        json={"query": query},
                        headers=headers,
                        ssl=False,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        data = await response.json()
                        
                        # Check for rate limit
                        if isinstance(data, dict):
                            if data.get("message") == "API rate limit exceeded":
                                print(f"  ⛔ Rate limited on credential #{idx + 1}")
                                self.rate_limited_until[idx] = time.time() + 180  # 3 min
                                continue
                            
                            if "errors" in data:
                                print(f"  ⚠️ API error: {data['errors']}")
                                continue
                        
                        return data
                        
            except asyncio.TimeoutError:
                print(f"  ⏱️ Timeout on attempt {attempt + 1}")
                await asyncio.sleep(2 ** attempt)
            except aiohttp.ClientError as e:
                print(f"  ❌ Request error: {e}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                print(f"  ❌ Unexpected error: {e}")
                await asyncio.sleep(2 ** attempt)
        
        return None


def parse_matchup_response(response: Dict[str, Any]) -> Dict[int, float]:
    """
    Parse API response to extract vs matchup advantages.
    
    Args:
        response: Raw API response
        
    Returns:
        Dict mapping opponent hero_id to advantage score
    """
    matchups: Dict[int, float] = {}
    
    try:
        hero_stats = response.get("data", {}).get("heroStats", {})
        matchup_data = hero_stats.get("matchUp", {})
        advantages = matchup_data.get("advantage", [])
        
        if not advantages:
            return matchups
        
        adv = advantages[0] if advantages else {}
        vs_list = adv.get("vs", [])
        
        for vs_entry in vs_list:
            opponent_id = vs_entry.get("heroId2")
            synergy = vs_entry.get("synergy")  # advantage vs opponent
            match_count = vs_entry.get("matchCount", 0)
            
            if opponent_id is not None and synergy is not None:
                # Only include if we have enough matches for statistical significance
                if match_count >= 50:
                    matchups[opponent_id] = round(synergy, 4)
    
    except (KeyError, TypeError, IndexError) as e:
        print(f"  Parse error: {e}")
    
    return matchups


async def fetch_all_matchups(
    client: StratzAPIClient,
    delay_between_requests: float = 0.3
) -> Dict[int, Dict[int, float]]:
    """
    Fetch matchup data for all heroes.
    
    Args:
        client: StratzAPIClient instance
        delay_between_requests: Delay between API calls
        
    Returns:
        Dict mapping hero_id to their matchup advantages
    """
    all_matchups: Dict[int, Dict[int, float]] = {}
    total = len(ALL_HERO_IDS)
    
    print(f"Fetching matchups for {total} heroes...")
    print("-" * 50)
    
    for idx, hero_id in enumerate(ALL_HERO_IDS, 1):
        print(f"[{idx}/{total}] Hero {hero_id}...", end=" ", flush=True)
        
        response = await client.fetch_hero_matchups(hero_id)
        
        if response:
            matchups = parse_matchup_response(response)
            if matchups:
                all_matchups[hero_id] = matchups
                print(f"✓ {len(matchups)} matchups")
            else:
                print("No data")
        else:
            print("FAILED")
        
        await asyncio.sleep(delay_between_requests)
    
    return all_matchups


def save_matchups(matchups: Dict[int, Dict[int, float]], output_path: Path) -> None:
    """Save matchups to JSON file."""
    # Convert int keys to strings for JSON compatibility
    json_data = {
        str(hero_id): {str(vs_id): adv for vs_id, adv in vs_matchups.items()}
        for hero_id, vs_matchups in matchups.items()
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)
    
    print(f"\n✅ Saved matchups to {output_path}")


async def main_async() -> None:
    """Async main entry point."""
    print("=" * 60)
    print("Stratz Hero Matchup Fetcher")
    print("=" * 60)
    
    if not api_to_proxy:
        print("ERROR: No API credentials found in base/keys.py")
        return
    
    print(f"Found {len(api_to_proxy)} API credentials")
    
    client = StratzAPIClient(api_to_proxy)
    matchups = await fetch_all_matchups(client)
    
    output_path = Path(__file__).parent.parent / "data" / "hero_matchups.json"
    save_matchups(matchups, output_path)
    
    # Summary
    total_heroes = len(matchups)
    total_matchups = sum(len(v) for v in matchups.values())
    print(f"\nSummary:")
    print(f"  Heroes with data: {total_heroes}")
    print(f"  Total matchup entries: {total_matchups}")
    print(f"  Average matchups per hero: {total_matchups / max(total_heroes, 1):.1f}")


def main() -> None:
    """Main entry point."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
