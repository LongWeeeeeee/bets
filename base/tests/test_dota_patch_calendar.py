from __future__ import annotations

from base.dota_patch_calendar import (
    ANNOUNCEMENT_TIME_CONVENTION,
    ELO_LEGACY_PATCH_RELEASES_RAW,
    PATCH_RELEASES,
    VERIFIED_741_ANNOUNCEMENT_RELEASES,
)
from base.sort_pub_matches_by_patch import _classify_patch
from ELO.models import _PATCH_RELEASES, _PATCH_RELEASES_RAW, _patch_label_for_timestamp


EXPECTED_741_ANNOUNCEMENTS = {
    "7.41": (1774395824, "2026-03-24T23:43:44Z", "1827626365766968"),
    "7.41a": (1774649721, "2026-03-27T22:15:21Z", "1828441623105709"),
    "7.41b": (1775593486, "2026-04-07T20:24:46Z", "1828894815568310"),
    "7.41c": (1778105875, "2026-05-06T22:17:55Z", "1832065502810383"),
    "7.41d": (1780620314, "2026-06-05T00:45:14Z", "1834602721189111"),
    "7.41e": (1785455895, "2026-07-30T23:58:15Z", "1839676055886687"),
}


def test_verified_741_announcement_records_are_exact_and_sourced() -> None:
    actual = {release.label: release for release in VERIFIED_741_ANNOUNCEMENT_RELEASES}

    assert set(actual) == set(EXPECTED_741_ANNOUNCEMENTS)
    for label, (release_ts, release_at_utc, announcement_id) in EXPECTED_741_ANNOUNCEMENTS.items():
        release = actual[label]
        assert release.release_ts == release_ts
        assert release.release_at_utc == release_at_utc
        assert release.time_convention == ANNOUNCEMENT_TIME_CONVENTION
        assert release.source_url is not None and release.source_url.endswith(announcement_id)


def test_calendar_is_descending_and_elo_compatibility_exports_follow_it() -> None:
    assert all(
        newer.release_ts > older.release_ts
        for newer, older in zip(PATCH_RELEASES, PATCH_RELEASES[1:])
    )
    assert tuple((release.label, release.release_date) for release in PATCH_RELEASES) == _PATCH_RELEASES_RAW
    assert tuple((release.label, release.release_ts) for release in PATCH_RELEASES) == tuple(
        (release.label, release.release_ts) for release in _PATCH_RELEASES
    )


def test_legacy_elo_date_boundaries_are_retained_unchanged() -> None:
    legacy_calendar = PATCH_RELEASES[len(VERIFIED_741_ANNOUNCEMENT_RELEASES):]

    assert tuple((release.label, release.release_date) for release in legacy_calendar) == (
        ELO_LEGACY_PATCH_RELEASES_RAW
    )
    assert legacy_calendar[0].release_ts == 1768953600  # 7.40c, original UTC midnight
    assert legacy_calendar[-1].release_ts == 1708473600  # 7.35c, original UTC midnight


def test_elo_and_public_sorter_classify_before_and_at_every_741_boundary() -> None:
    releases = tuple(reversed(VERIFIED_741_ANNOUNCEMENT_RELEASES))
    for index, release in enumerate(releases):
        before_label = "7.40c" if index == 0 else releases[index - 1].label
        assert _patch_label_for_timestamp(release.release_ts - 1) == before_label
        assert _patch_label_for_timestamp(release.release_ts) == release.label
        assert _classify_patch(release.release_ts - 1) == before_label
        assert _classify_patch(release.release_ts) == release.label


def test_july_second_remains_741d_until_741e_announcement() -> None:
    july_2_utc_midnight = 1782950400

    assert _patch_label_for_timestamp(july_2_utc_midnight) == "7.41d"
    assert _classify_patch(july_2_utc_midnight) == "7.41d"
