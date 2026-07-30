"""Regression: parse_hero_matchups must not spawn Camoufox subprocess when no fetcher."""

from __future__ import annotations

import time

import pytest


def test_parse_hero_matchups_without_installed_fetcher_fails_closed_without_subprocess(monkeypatch):
    """When PROTRACKER_PAYLOAD_FETCHER is None, fail closed like no-payload shared fetcher."""
    import base.dota2protracker as d2pt

    subprocess_calls = []

    def _spy_subprocess(slug, hero_id, proxy_candidate=None):
        subprocess_calls.append((slug, hero_id, proxy_candidate))
        raise AssertionError(
            f"_fetch_protracker_payload_via_subprocess must not be called; "
            f"got slug={slug!r} hero_id={hero_id!r} proxy={proxy_candidate!r}"
        )

    def _empty_payload_fetcher(slug, hero_id, proxy_candidate=None):
        return {"matchups": {}, "synergies": {}}

    # Stub identity / availability / cache / I/O — no real browser or network.
    monkeypatch.setattr(d2pt, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(d2pt, "get_hero_slug", lambda name: "anti-mage")
    monkeypatch.setattr(d2pt, "get_hero_id", lambda name: 1)
    monkeypatch.setattr(d2pt, "CACHE_DIR", "/tmp/__no_protracker_standalone_cache__")
    monkeypatch.setattr(d2pt.os.path, "exists", lambda *_a, **_k: False)
    monkeypatch.setattr(d2pt.os, "makedirs", lambda *_a, **_k: None)
    monkeypatch.setattr(d2pt, "_get_proxy_from_pool", lambda: None)
    monkeypatch.setattr(
        d2pt,
        "_dota2protracker_candidate_proxies",
        lambda preferred=None: [None],
    )
    monkeypatch.setattr(d2pt, "_fetch_protracker_payload_via_subprocess", _spy_subprocess)
    monkeypatch.setattr(time, "time", lambda: 1_700_000_000.0)

    # Freeze open so cache write is a no-op (installed empty path may write).
    class _NullFile:
        def write(self, *_a, **_k):
            return 0

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    monkeypatch.setattr("builtins.open", lambda *_a, **_k: _NullFile())

    # Baseline: installed shared fetcher that supplies no usable payload.
    monkeypatch.setattr(d2pt, "PROTRACKER_PAYLOAD_FETCHER", _empty_payload_fetcher)
    no_payload_result = d2pt.parse_hero_matchups("Anti-Mage", use_cache=False, proxy=None)
    assert subprocess_calls == [], (
        f"baseline installed-fetcher path must not call subprocess; calls={subprocess_calls}"
    )

    # Target: no installed fetcher — must fail closed the same way, zero subprocess calls.
    monkeypatch.setattr(d2pt, "PROTRACKER_PAYLOAD_FETCHER", None)
    no_fetcher_result = d2pt.parse_hero_matchups("Anti-Mage", use_cache=False, proxy=None)

    assert subprocess_calls == [], (
        f"no-fetcher path must not call _fetch_protracker_payload_via_subprocess; "
        f"calls={subprocess_calls}"
    )
    assert no_fetcher_result == no_payload_result, (
        f"no-fetcher result must match installed-fetcher/no-payload failure result; "
        f"no_fetcher={no_fetcher_result!r}; no_payload={no_payload_result!r}"
    )


def test_legacy_subprocess_helper_fails_closed_without_popen(monkeypatch):
    """Direct legacy helper must fail closed before any subprocess.Popen."""
    import base.dota2protracker as d2pt

    calls = []

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("LEGACY_POPEN_REACHED")

    monkeypatch.setattr(d2pt.subprocess, "Popen", spy)

    with pytest.raises(RuntimeError) as exc_info:
        d2pt._fetch_protracker_payload_via_subprocess("Antimage", 1, None)

    assert str(exc_info.value) == "Legacy subprocess ProTracker helper is disabled"
    assert calls == []
