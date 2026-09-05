"""Deterministic Dota patch boundaries shared by offline consumers.

For 7.41 through 7.41e, ``release_ts`` is the UTC publication timestamp in
Steam's official ``ISteamNews/GetNewsForApp`` feed. It is an announcement-time
boundary: consumers classify a timestamp ``t`` as a patch when
``t >= release_ts``. It must not be treated as an inferred depot activation
time. Older ELO boundaries deliberately retain their historical date-at-UTC-
midnight convention until independently re-verified.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple


ANNOUNCEMENT_TIME_CONVENTION = "steam_news_published_at_utc"
LEGACY_DATE_TIME_CONVENTION = "legacy_date_at_utc_midnight"
STEAM_NEWS_API_URL = (
    "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
    "?appid=570&count=200&maxlength=0&format=json"
)


@dataclass(frozen=True)
class PatchRelease:
    label: str
    release_ts: int
    release_date: str
    release_at_utc: str
    source_url: Optional[str]
    time_convention: str


# Kept verbatim from ELO/models.py so established ELO boundaries do not move.
ELO_LEGACY_PATCH_RELEASES_RAW: Tuple[Tuple[str, str], ...] = (
    ("7.40c", "2026-01-21"),
    ("7.40b", "2025-12-23"),
    ("7.40", "2025-12-15"),
    ("7.39e", "2025-10-02"),
    ("7.39d", "2025-08-05"),
    ("7.39c", "2025-06-24"),
    ("7.39b", "2025-05-29"),
    ("7.39", "2025-05-21"),
    ("7.38c", "2025-03-27"),
    ("7.38b", "2025-03-05"),
    ("7.38", "2025-02-19"),
    ("7.37e", "2024-11-19"),
    ("7.37d", "2024-10-01"),
    ("7.37c", "2024-08-28"),
    ("7.37b", "2024-08-14"),
    ("7.37", "2024-07-31"),
    ("7.36c", "2024-06-24"),
    ("7.36b", "2024-06-05"),
    ("7.36a", "2024-05-26"),
    ("7.36", "2024-05-22"),
    ("7.35d", "2024-03-21"),
    ("7.35c", "2024-02-21"),
)


def _legacy_release(label: str, release_date: str) -> PatchRelease:
    release_ts = int(
        datetime.strptime(release_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    )
    return PatchRelease(
        label=label,
        release_ts=release_ts,
        release_date=release_date,
        release_at_utc=f"{release_date}T00:00:00Z",
        source_url=None,
        time_convention=LEGACY_DATE_TIME_CONVENTION,
    )


# URLs are individual records returned by STEAM_NEWS_API_URL. Preserve exact
# feed timestamps instead of deriving UTC midnight from a calendar day.
VERIFIED_741_ANNOUNCEMENT_RELEASES: Tuple[PatchRelease, ...] = (
    PatchRelease("7.41e", 1785455895, "2026-07-30", "2026-07-30T23:58:15Z",
                 "https://steamstore-a.akamaihd.net/news/externalpost/steam_community_announcements/1839676055886687",
                 ANNOUNCEMENT_TIME_CONVENTION),
    PatchRelease("7.41d", 1780620314, "2026-06-05", "2026-06-05T00:45:14Z",
                 "https://steamstore-a.akamaihd.net/news/externalpost/steam_community_announcements/1834602721189111",
                 ANNOUNCEMENT_TIME_CONVENTION),
    PatchRelease("7.41c", 1778105875, "2026-05-06", "2026-05-06T22:17:55Z",
                 "https://steamstore-a.akamaihd.net/news/externalpost/steam_community_announcements/1832065502810383",
                 ANNOUNCEMENT_TIME_CONVENTION),
    PatchRelease("7.41b", 1775593486, "2026-04-07", "2026-04-07T20:24:46Z",
                 "https://steamstore-a.akamaihd.net/news/externalpost/steam_community_announcements/1828894815568310",
                 ANNOUNCEMENT_TIME_CONVENTION),
    PatchRelease("7.41a", 1774649721, "2026-03-27", "2026-03-27T22:15:21Z",
                 "https://steamstore-a.akamaihd.net/news/externalpost/steam_community_announcements/1828441623105709",
                 ANNOUNCEMENT_TIME_CONVENTION),
    PatchRelease("7.41", 1774395824, "2026-03-24", "2026-03-24T23:43:44Z",
                 "https://steamstore-a.akamaihd.net/news/externalpost/steam_community_announcements/1827626365766968",
                 ANNOUNCEMENT_TIME_CONVENTION),
)


PATCH_RELEASES: Tuple[PatchRelease, ...] = VERIFIED_741_ANNOUNCEMENT_RELEASES + tuple(
    _legacy_release(label, release_date) for label, release_date in ELO_LEGACY_PATCH_RELEASES_RAW
)
