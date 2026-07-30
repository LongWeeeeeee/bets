"""Fail-closed Winline proxy country policy.

Only explicitly classified DE/US proxies may be Winline candidates.
RU, unknown, empty, and malformed entries must be skipped without error.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402


def test_unknown_unclassified_proxy_is_not_winline_candidate(monkeypatch) -> None:
    """Unclassified host must not become a Winline candidate via DE default."""
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", ["http://203.0.113.99:8080"], raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", [], raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "", raising=False)

    pool = cs._bookmaker_live_proxy_pool()
    # Pool may still list the URL, but country must not silently become DE.
    for item in pool:
        country = str((item or {}).get("country") or "").strip().upper()
        assert country != "DE", (
            f"unknown host must not default to DE, got {item!r}"
        )

    candidates = cs._bookmaker_winline_proxy_candidates()
    urls = [str((c or {}).get("url") or "") for c in candidates]
    assert "http://203.0.113.99:8080" not in urls
    countries = [str((c or {}).get("country") or "").strip().upper() for c in candidates]
    assert "DE" not in countries or all(
        str((c or {}).get("url") or "") != "http://203.0.113.99:8080" for c in candidates
    )
    for c in candidates:
        assert str((c or {}).get("url") or "") != "http://203.0.113.99:8080"


def test_winline_proxy_candidates_allow_only_explicit_de_us(monkeypatch) -> None:
    """Explicit DE/US keep order; RU/unknown/empty/malformed are skipped, no raise."""
    mixed: List[Any] = [
        {"url": "http://user:pass@154.195.1.10:1000", "country": "DE"},
        {"url": "http://user:pass@172.121.1.20:1001", "country": "US"},
        {"url": "http://user:pass@77.221.150.1:1002", "country": "RU"},
        {"url": "http://user:pass@203.0.113.50:1003", "country": "UNKNOWN"},
        {"url": "http://user:pass@203.0.113.51:1004", "country": ""},
        {"url": "not-a-url", "country": "??"},
        None,
        "",
        {"url": "", "country": "DE"},
        {"url": "http://user:pass@154.195.1.11:1005", "country": "de"},  # normalize case
        {"url": "http://user:pass@172.121.1.21:1006", "country": " us "},
        {"country": "DE"},  # missing url
        {"url": "http://user:pass@9.9.9.9:1007"},  # missing country -> classify host
    ]
    # Also cover string pool path with known DE prefix, RU prefix, and unknown.
    string_pool = [
        "http://user:pass@154.195.2.1:2000",  # DE by host
        "http://user:pass@77.221.150.2:2001",  # RU by host
        "http://user:pass@203.0.113.60:2002",  # unknown host
        "http://user:pass@172.121.2.1:2003",  # US by host
    ]

    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", mixed, raising=False)
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", string_pool, raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "", raising=False)

    # Must not raise on bad entries.
    pool = cs._bookmaker_live_proxy_pool()
    assert isinstance(pool, list)

    # Pool country normalization: only explicit DE/US allowed labels for known hosts;
    # never relabel RU/unknown as DE.
    by_url = {str(p.get("url")): str(p.get("country") or "").strip().upper() for p in pool}
    assert by_url.get("http://user:pass@154.195.1.10:1000") == "DE"
    assert by_url.get("http://user:pass@172.121.1.20:1001") == "US"
    assert by_url.get("http://user:pass@77.221.150.1:1002") == "RU"
    # Unknown/empty/malformed must not become DE
    if "http://user:pass@203.0.113.50:1003" in by_url:
        assert by_url["http://user:pass@203.0.113.50:1003"] != "DE"
    if "http://user:pass@203.0.113.51:1004" in by_url:
        assert by_url["http://user:pass@203.0.113.51:1004"] != "DE"
    if "http://user:pass@203.0.113.60:2002" in by_url:
        assert by_url["http://user:pass@203.0.113.60:2002"] != "DE"
    if "http://user:pass@9.9.9.9:1007" in by_url:
        assert by_url["http://user:pass@9.9.9.9:1007"] != "DE"
    # Host-classified DE/US/RU preserved
    assert by_url.get("http://user:pass@154.195.2.1:2000") == "DE"
    assert by_url.get("http://user:pass@77.221.150.2:2001") == "RU"
    assert by_url.get("http://user:pass@172.121.2.1:2003") == "US"

    candidates = cs._bookmaker_winline_proxy_candidates()
    assert isinstance(candidates, list)
    countries = [str((c or {}).get("country") or "").strip().upper() for c in candidates]
    urls = [str((c or {}).get("url") or "") for c in candidates]

    assert all(c in {"DE", "US"} for c in countries)
    assert "RU" not in countries
    assert "UNKNOWN" not in countries
    assert "" not in countries

    # Explicit DE/US present; RU/unknown hosts absent
    assert "http://user:pass@154.195.1.10:1000" in urls
    assert "http://user:pass@172.121.1.20:1001" in urls
    assert "http://user:pass@77.221.150.1:1002" not in urls
    assert "http://user:pass@203.0.113.50:1003" not in urls
    assert "http://user:pass@203.0.113.51:1004" not in urls
    assert "http://user:pass@203.0.113.60:2002" not in urls
    assert "http://user:pass@9.9.9.9:1007" not in urls
    assert "not-a-url" not in urls

    # Order among valid DE then US preserved (DE first, then optional US)
    de_urls = [u for u, c in zip(urls, countries) if c == "DE"]
    us_urls = [u for u, c in zip(urls, countries) if c == "US"]
    assert de_urls  # at least explicit + host DE
    # first DE should be the first explicit DE from BOOKMAKER pool
    assert de_urls[0] == "http://user:pass@154.195.1.10:1000"
    if us_urls:
        assert us_urls[0] == "http://user:pass@172.121.1.20:1001"
