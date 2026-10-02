"""Fail-closed Winline proxy country policy.

Only explicitly classified DE/US/CA proxies may be Winline candidates
(CA is classified solely through keys.PROXY_INVENTORY).
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

# Never import the credential-bearing production keys module (it is gitignored:
# a clean checkout has none). Same stub as test_winline_never_direct.py, so this
# file collects on its own and in any order.
if "keys" not in sys.modules:
    from types import ModuleType

    _keys_stub = ModuleType("keys")
    _keys_stub.api_to_proxy = {}
    _keys_stub.BOOKMAKER_PROXY_URL = ""
    _keys_stub.BOOKMAKER_PROXY_POOL = []
    _keys_stub.DLTV_PROXY_POOL = []
    _keys_stub.Token = "0:STUB_MAIN_BOT"  # functions.send_message reads keys.Token directly
    sys.modules["keys"] = _keys_stub

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


# --- Canadian pool (02.10.2026): classified only through keys.PROXY_INVENTORY ---

_CA_HOSTS_PORTS = [
    ("88.218.187.219", 64702),
    ("185.101.201.130", 64822),
    ("185.101.202.132", 64064),
    ("176.118.38.219", 64790),
    ("193.228.129.77", 64464),
]


def _ca_http(host: str, port: int) -> str:
    return f"http://dummyuser:dummypass@{host}:{port}"


def _ca_socks(host: str, port: int) -> str:
    return f"socks5://dummyuser:dummypass@{host}:{port + 1}"


def _set_inventory(monkeypatch, entries) -> None:
    import keys  # same module cyberscore_try imported (sys.modules["keys"])

    monkeypatch.setattr(keys, "PROXY_INVENTORY", tuple(entries), raising=False)


def test_ca_pool_from_inventory_is_winline_candidates(monkeypatch) -> None:
    """Five CA HTTP proxies known only via keys.PROXY_INVENTORY become candidates in pool order."""
    _set_inventory(
        monkeypatch,
        [{"ip": host, "country": "ca"} for host, _ in _CA_HOSTS_PORTS],
    )
    pool = [_ca_http(h, p) for h, p in _CA_HOSTS_PORTS] + [
        _ca_socks(h, p) for h, p in _CA_HOSTS_PORTS
    ]
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", pool, raising=False)
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [], raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "", raising=False)

    candidates = cs._bookmaker_winline_proxy_candidates()
    assert [c["url"] for c in candidates] == [_ca_http(h, p) for h, p in _CA_HOSTS_PORTS]
    assert {str(c["country"]).upper() for c in candidates} == {"CA"}
    assert all("socks" not in c["url"] for c in candidates)


def test_ca_host_absent_or_unknown_in_inventory_is_excluded(monkeypatch) -> None:
    """Fail closed: a host missing from the inventory, or labelled unknown/empty, is skipped."""
    _set_inventory(
        monkeypatch,
        [
            {"ip": "88.218.187.219", "country": "ca"},
            {"ip": "185.101.201.130", "country": "unknown"},
            {"ip": "185.101.202.132", "country": ""},
            # 176.118.38.219 deliberately absent; 193.228.129.77 labelled RU-like junk
            {"ip": "193.228.129.77", "country": "zz"},
        ],
    )
    pool = [_ca_http(h, p) for h, p in _CA_HOSTS_PORTS]
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", pool, raising=False)
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [], raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "", raising=False)

    candidates = cs._bookmaker_winline_proxy_candidates()
    assert [c["url"] for c in candidates] == [_ca_http("88.218.187.219", 64702)]


def test_inventory_missing_keeps_ca_hosts_fail_closed(monkeypatch) -> None:
    """keys without PROXY_INVENTORY: the CA hosts stay unclassified and are not candidates."""
    import keys

    monkeypatch.delattr(keys, "PROXY_INVENTORY", raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", [_ca_http(h, p) for h, p in _CA_HOSTS_PORTS], raising=False)
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [], raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "", raising=False)

    assert cs._bookmaker_winline_proxy_candidates() == []


def test_de_and_us_stay_before_ca(monkeypatch) -> None:
    """Tier order is DE, then US, then CA regardless of pool order."""
    _set_inventory(monkeypatch, [{"ip": h, "country": "ca"} for h, _ in _CA_HOSTS_PORTS])
    de = "http://dummyuser:dummypass@154.195.1.10:1000"
    us = "http://dummyuser:dummypass@172.121.1.20:1001"
    ca = [_ca_http(h, p) for h, p in _CA_HOSTS_PORTS]
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", ca[:3] + [us, de] + ca[3:], raising=False)
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [], raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "", raising=False)

    candidates = cs._bookmaker_winline_proxy_candidates()
    assert [c["url"] for c in candidates] == [de, us] + ca
    assert [str(c["country"]).upper() for c in candidates] == ["DE", "US"] + ["CA"] * 5


def test_ca_candidates_flow_into_shared_route_selection(monkeypatch) -> None:
    """Delivery boundary: the shared-browser route picks the first CA proxy with credentials."""
    _set_inventory(monkeypatch, [{"ip": h, "country": "ca"} for h, _ in _CA_HOSTS_PORTS])
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_POOL", [_ca_http(h, p) for h, p in _CA_HOSTS_PORTS], raising=False)
    monkeypatch.setattr(cs, "DLTV_PROXY_POOL", [], raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PROXY_URL", "", raising=False)
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_candidates", [], raising=False)
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_index", 0, raising=False)
    monkeypatch.setattr(cs, "_winline_parsing_halted", False, raising=False)
    monkeypatch.setattr(cs, "_winline_dead_proxy_urls", set(), raising=False)

    kwargs = cs._bookmaker_select_shared_camoufox_proxy_kwargs()
    proxy = kwargs.get("proxy") or {}
    assert proxy.get("server") == "http://88.218.187.219:64702"
    assert proxy.get("username") == "dummyuser"
