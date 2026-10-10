"""No unit test may start a real Camoufox browser (card ingame-4vud, 10.10.2026).

Observed: test_pipeline_integrity::test_problem_candidates_are_shown_without_odds
drove check_head into the shared Camoufox worker (dota2protracker hero fetches
and winline_current_map_poll) through the production proxy pool. On a fresh Mac
venv CamoufoxFetcher first downloaded a 1.29 GB browser; on serv1 the suite
launched a real browser with the Winline proxy. base/tests/conftest.py blocks
the real library entry points for every test.

The checks below never call an unguarded entry point: they first verify that
the attribute is not the library's own function, so running this file without
the guard fails instead of launching a browser.
"""

import pytest

camoufox = pytest.importorskip("camoufox")
pkgman = pytest.importorskip("camoufox.pkgman")


def _assert_guarded(func, label):
    module = getattr(func, "__module__", "") or ""
    if module.startswith("camoufox"):
        pytest.fail(f"{label} is the real camoufox function: a test could launch a browser")


def test_sync_browser_launch_is_blocked():
    _assert_guarded(camoufox.Camoufox.__enter__, "Camoufox.__enter__")
    with pytest.raises(RuntimeError, match="forbidden in unit tests"):
        camoufox.Camoufox.__enter__(object())


def test_async_browser_launch_is_blocked():
    _assert_guarded(camoufox.AsyncCamoufox.__aenter__, "AsyncCamoufox.__aenter__")
    with pytest.raises(RuntimeError, match="forbidden in unit tests"):
        camoufox.AsyncCamoufox.__aenter__(object())


def test_browser_download_is_blocked():
    _assert_guarded(pkgman.CamoufoxFetcher.__init__, "CamoufoxFetcher.__init__")
    with pytest.raises(RuntimeError, match="forbidden in unit tests"):
        pkgman.CamoufoxFetcher()
