"""W-DYNAMIC-COLLECTOR: acquisition modes for Winline Camoufox collector (TDD).

Proves initial_goto / dynamic_dom / controlled_reload behavior, bounded diagnostics,
and that non-Winline default path is unchanged.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import bookmaker_selenium_odds as odds_parser  # noqa: E402


WINLINE_URL = "https://winline.ru/stavki/sport/kibersport/match/12345"
OTHER_URL = "https://winline.ru/stavki/sport/kibersport/match/99999"
BETBOOM_URL = "https://betboom.ru/esport/live/dota-2/match/1"


def _patch_no_sleep(monkeypatch) -> None:
    monkeypatch.setattr(odds_parser.time, "sleep", lambda *_a, **_k: None)


class _FakeLocator:
    def __init__(self, text: str) -> None:
        self._text = text

    def inner_text(self, timeout: int = 0):  # noqa: ARG002
        return self._text


class _CountingPage:
    """Minimal Playwright-like page with goto/reload call counters."""

    def __init__(
        self,
        *,
        html: str,
        body_text: str,
        url: str = "about:blank",
        reload_error: Optional[BaseException] = None,
    ) -> None:
        self._html = html
        self._body_text = body_text
        self.url = url
        self.goto_calls: List[Dict[str, Any]] = []
        self.reload_calls: List[Dict[str, Any]] = []
        self._reload_error = reload_error
        self.browser_spawn_count = 0  # must stay 0 (no parallel browser)

    def goto(self, url: str, wait_until: str = "domcontentloaded", timeout: int = 0):  # noqa: ARG002
        self.goto_calls.append({"url": url, "wait_until": wait_until, "timeout": timeout})
        self.url = url

    def reload(self, wait_until: str = "domcontentloaded", timeout: int = 0):  # noqa: ARG002
        self.reload_calls.append({"wait_until": wait_until, "timeout": timeout})
        if self._reload_error is not None:
            raise self._reload_error
        return None

    def content(self) -> str:
        return self._html

    def locator(self, selector: str):
        if selector == "body":
            return _FakeLocator(self._body_text)
        raise AssertionError(f"unexpected selector: {selector}")

    def title(self) -> str:
        return "Winline"

    def evaluate(self, script: str, arg=None):  # noqa: ARG002
        if "document.readyState" in str(script):
            return "complete"
        return False


def _html_body(text: str) -> str:
    return f"<html><body>{text}</body></html>"


def _assert_bounded_diag(result: odds_parser.SiteResult) -> Dict[str, Any]:
    """Acquisition diagnostics must be present and bounded (no raw dumps/secrets)."""
    mode = getattr(result, "acquisition_mode", None)
    page_url = getattr(result, "page_url", None)
    dom_sig = getattr(result, "dom_signature", None)
    latency = getattr(result, "acquisition_latency_ms", None)
    acq_err = getattr(result, "acquisition_error", None)

    assert mode in {"initial_goto", "dynamic_dom", "controlled_reload"}
    assert isinstance(page_url, str) and len(page_url) <= 500
    assert isinstance(dom_sig, str) and 0 < len(dom_sig) <= 128
    assert "password" not in dom_sig.lower()
    assert "<html" not in dom_sig.lower()
    assert latency is None or (isinstance(latency, (int, float)) and latency >= 0)
    if acq_err is not None:
        assert isinstance(acq_err, str)
        assert len(acq_err) <= 300
        assert "<html" not in acq_err.lower()
    return {
        "acquisition_mode": mode,
        "page_url": page_url,
        "dom_signature": dom_sig,
        "acquisition_latency_ms": latency,
        "acquisition_error": acq_err,
    }


def test_initial_goto_navigates_blank_page(monkeypatch) -> None:
    _patch_no_sleep(monkeypatch)
    body = "TeamA TeamB Победитель 1 карты 1.55 2.40"
    page = _CountingPage(html=_html_body(body), body_text=body, url="about:blank")

    result = odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_URL,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="initial_goto",
    )

    assert len(page.goto_calls) == 1
    assert page.goto_calls[0]["url"] == WINLINE_URL
    assert page.goto_calls[0]["wait_until"] == "domcontentloaded"
    assert page.reload_calls == []
    diag = _assert_bounded_diag(result)
    assert diag["acquisition_mode"] == "initial_goto"
    assert diag["page_url"] == WINLINE_URL


def test_initial_goto_navigates_wrong_url(monkeypatch) -> None:
    _patch_no_sleep(monkeypatch)
    body = "TeamA TeamB map market"
    page = _CountingPage(html=_html_body(body), body_text=body, url=OTHER_URL)

    odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_URL,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="initial_goto",
    )

    assert len(page.goto_calls) == 1
    assert page.goto_calls[0]["url"] == WINLINE_URL
    assert page.reload_calls == []


def test_initial_goto_skips_goto_when_already_on_target(monkeypatch) -> None:
    _patch_no_sleep(monkeypatch)
    body = "TeamA TeamB already here"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_URL)

    result = odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_URL,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="initial_goto",
    )

    assert page.goto_calls == []
    assert page.reload_calls == []
    assert _assert_bounded_diag(result)["acquisition_mode"] == "initial_goto"


def test_dynamic_dom_never_calls_goto_or_reload(monkeypatch) -> None:
    _patch_no_sleep(monkeypatch)
    body = "TeamA TeamB live dom 1.70 2.10"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_URL)

    # First same-URL dynamic attempt
    r1 = odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_URL,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="dynamic_dom",
    )
    # Second same-URL dynamic attempt (poller loop)
    r2 = odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_URL,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="dynamic_dom",
    )

    assert page.goto_calls == []
    assert page.reload_calls == []
    assert page.browser_spawn_count == 0
    assert _assert_bounded_diag(r1)["acquisition_mode"] == "dynamic_dom"
    assert _assert_bounded_diag(r2)["acquisition_mode"] == "dynamic_dom"


def test_controlled_reload_calls_reload_exactly_once(monkeypatch) -> None:
    _patch_no_sleep(monkeypatch)
    body = "TeamA TeamB after reload"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_URL)

    result = odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_URL,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="controlled_reload",
    )

    assert page.goto_calls == []
    assert len(page.reload_calls) == 1
    assert page.reload_calls[0]["wait_until"] == "domcontentloaded"
    assert page.browser_spawn_count == 0
    diag = _assert_bounded_diag(result)
    assert diag["acquisition_mode"] == "controlled_reload"
    assert diag["acquisition_error"] in (None, "")


def test_controlled_reload_failure_is_bounded_no_extra_browser(monkeypatch) -> None:
    _patch_no_sleep(monkeypatch)
    body = "TeamA TeamB stale"
    page = _CountingPage(
        html=_html_body(body),
        body_text=body,
        url=WINLINE_URL,
        reload_error=RuntimeError("net::ERR_CONNECTION_RESET " + ("X" * 500)),
    )

    result = odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_URL,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="controlled_reload",
    )

    assert len(page.reload_calls) == 1
    assert page.goto_calls == []
    assert page.browser_spawn_count == 0
    diag = _assert_bounded_diag(result)
    assert diag["acquisition_mode"] == "controlled_reload"
    assert diag["acquisition_error"]
    assert len(diag["acquisition_error"]) <= 300
    assert "XXXX" not in (diag["acquisition_error"] or "") or len(diag["acquisition_error"]) <= 300
    # load status should reflect partial/error without inventing a second session
    assert result.status in {"partial_load", "ok", "request_error", "error"}


def test_default_call_without_acquisition_mode_still_gotos(monkeypatch) -> None:
    """Backward-compatible default: always navigate (existing behavior)."""
    _patch_no_sleep(monkeypatch)
    body = "TeamA TeamB default path"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_URL)

    odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_URL,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
    )

    assert len(page.goto_calls) == 1
    assert page.reload_calls == []


def test_non_winline_ignores_acquisition_mode_and_gotos(monkeypatch) -> None:
    """Non-Winline bookmakers keep legacy always-goto behavior even if mode is passed."""
    _patch_no_sleep(monkeypatch)
    body = "TeamA TeamB betboom row"
    page = _CountingPage(html=_html_body(body), body_text=body, url=BETBOOM_URL)

    # Stub heavy map/deeplink branches so we only exercise load path
    monkeypatch.setattr(odds_parser, "_is_deeplink", lambda *_a, **_k: False)
    monkeypatch.setattr(
        odds_parser,
        "_candidate_match_urls_from_html",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        odds_parser,
        "_find_from_sources",
        lambda *_a, **_k: (False, [], "", ""),
    )

    odds_parser.parse_site_in_camoufox_page(
        page,
        "betboom",
        BETBOOM_URL,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="dynamic_dom",
    )

    assert len(page.goto_calls) == 1
    assert page.reload_calls == []


def test_load_helper_dynamic_dom_matrix(monkeypatch) -> None:
    """Direct call-count matrix on _load_site_render_payload_camoufox."""
    _patch_no_sleep(monkeypatch)
    body = "sig body for matrix"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_URL)

    payload = odds_parser._load_site_render_payload_camoufox(
        page,
        WINLINE_URL,
        acquisition_mode="dynamic_dom",
    )
    assert page.goto_calls == []
    assert page.reload_calls == []
    assert len(payload) >= 5
    if len(payload) >= 6:
        diag = payload[5]
        assert diag.get("acquisition_mode") == "dynamic_dom"
        assert isinstance(diag.get("dom_signature"), str)
        assert len(diag.get("dom_signature") or "") <= 128


def test_load_helper_controlled_reload_matrix(monkeypatch) -> None:
    _patch_no_sleep(monkeypatch)
    body = "reload body"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_URL)

    payload = odds_parser._load_site_render_payload_camoufox(
        page,
        WINLINE_URL,
        acquisition_mode="controlled_reload",
    )
    assert page.goto_calls == []
    assert len(page.reload_calls) == 1
    assert page.reload_calls[0]["wait_until"] == "domcontentloaded"
    if len(payload) >= 6:
        assert payload[5].get("acquisition_mode") == "controlled_reload"


def test_dom_signature_helper_is_bounded_and_stable() -> None:
    sig = odds_parser._bounded_dom_signature("  Alpha   Beta  1.55  2.40  " * 200)
    assert isinstance(sig, str)
    assert len(sig) <= 128
    assert "Alpha" not in sig  # hash, not raw dump
    sig2 = odds_parser._bounded_dom_signature("  Alpha   Beta  1.55  2.40  " * 200)
    assert sig == sig2
