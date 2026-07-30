"""REPLAN9-W-B: named Winline Camoufox page URL policy + controlled reload.

Exclusive focused browser-policy tests. Covers:
- correct live URL + dynamic_dom => 0 goto / 0 reload / 0 sleep
- blank / root / wrong-host URL under dynamic_dom => exactly 1 goto repair
- controlled_reload on live URL => exactly 1 reload, then DOM; no double nav; no sleep
- controlled_reload on a wrong/redirected URL => bounded goto repair
- navigation/reload failure is surfaced (never silent dynamic-DOM success)
- named page bookmaker:winline reused; no duplicate page creation
"""

from __future__ import annotations

import sys
import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import bookmaker_selenium_odds as odds_parser  # noqa: E402


WINLINE_LIVE = "https://winline.ru/stavki/sport/kibersport/match/12345"
WINLINE_ROOT = "https://winline.ru/"
WINLINE_ROOT_NO_SLASH = "https://winline.ru"
WINLINE_WRONG_HOST = "https://example.com/odds"
WINLINE_OTHER_MATCH = "https://winline.ru/stavki/sport/kibersport/match/99999"
NAMED_PAGE = "bookmaker:winline"


def test_async_cli_main_is_awaited_by_module_entrypoint() -> None:
    source_path = BASE_DIR / "bookmaker_selenium_odds.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    guards = [
        node
        for node in tree.body
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and any(
            isinstance(comp, ast.Constant) and comp.value == "__main__"
            for comp in node.test.comparators
        )
    ]
    assert guards, "module must retain a __main__ entrypoint"
    assert any(
        isinstance(stmt, ast.Expr)
        and isinstance(stmt.value, ast.Call)
        and isinstance(stmt.value.func, ast.Attribute)
        and isinstance(stmt.value.func.value, ast.Name)
        and stmt.value.func.value.id == "asyncio"
        and stmt.value.func.attr == "run"
        for guard in guards
        for stmt in guard.body
    ), "async main() must be executed via asyncio.run()"


def test_camoufox_async_runner_moves_sync_browser_to_worker_thread(monkeypatch) -> None:
    events: List[str] = []
    parse_kwargs: List[Dict[str, Any]] = []

    class _Browser:
        def new_page(self):
            events.append("new_page")
            return _CountingPage(
                html=_html_body("TeamA TeamB"),
                body_text="TeamA TeamB",
                url=WINLINE_LIVE,
            )

    class _CamoufoxContext:
        def __enter__(self):
            events.append("enter")
            return _Browser()

        def __exit__(self, *_args):
            events.append("exit")

    async def _fake_parse(*_args, **_kwargs):
        parse_kwargs.append(dict(_kwargs))
        return "parsed"

    monkeypatch.setattr(odds_parser, "CAMOUFOX_AVAILABLE", True)
    monkeypatch.setattr(
        odds_parser,
        "camoufox",
        SimpleNamespace(Camoufox=lambda **_kwargs: _CamoufoxContext()),
    )
    monkeypatch.setattr(odds_parser, "parse_site_in_camoufox_page_async", _fake_parse)

    result = asyncio.run(
        odds_parser.run_sites_in_camoufox_async(
            selected_sites=["winline"],
            urls={"winline": WINLINE_LIVE},
            team1="TeamA",
            team2="TeamB",
            mode="live",
            forced_map_num=1,
        )
    )

    assert result == ["parsed"]
    assert events == ["enter", "new_page", "exit"]
    assert parse_kwargs[0]["acquisition_mode"] == "initial_goto"


def test_compact_sourcetv_team_name_matches_spaced_winline_name() -> None:
    card = (
        "DOTA 2 | European Pro League "
        "AMARU GAMING POWER RANGERS 1карта 0 0 9 16 1К "
        "Матч 1.75 2.05 1 карта 1.82 1.88"
    )

    assert odds_parser._text_matches_teams(
        card,
        "_PowerRangers",
        "Amaru Gaming",
    )
    assert odds_parser._winline_team_order(
        card,
        "_PowerRangers",
        "Amaru Gaming",
    ) == "reverse"


class _SleepCounter:
    def __init__(self) -> None:
        self.calls: List[float] = []

    def __call__(self, seconds: float = 0, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        self.calls.append(float(seconds))


class _FakeLocator:
    def __init__(self, text: str) -> None:
        self._text = text

    def inner_text(self, timeout: int = 0):  # noqa: ARG002
        return self._text


class _CountingPage:
    """Playwright-like page with goto/reload/content counters."""

    def __init__(
        self,
        *,
        html: str,
        body_text: str,
        url: str = "about:blank",
        goto_error: Optional[BaseException] = None,
        reload_error: Optional[BaseException] = None,
        name: Optional[str] = None,
    ) -> None:
        self._html = html
        self._body_text = body_text
        self.url = url
        self.name = name
        self.goto_calls: List[Dict[str, Any]] = []
        self.reload_calls: List[Dict[str, Any]] = []
        self.content_calls = 0
        self.evaluate_calls = 0
        self._goto_error = goto_error
        self._reload_error = reload_error
        self.page_id = id(self)
        self.closed = False

    def goto(self, url: str, wait_until: str = "domcontentloaded", timeout: int = 0):  # noqa: ARG002
        self.goto_calls.append({"url": url, "wait_until": wait_until, "timeout": timeout})
        if self._goto_error is not None:
            raise self._goto_error
        self.url = url
        return None

    def reload(self, wait_until: str = "domcontentloaded", timeout: int = 0):  # noqa: ARG002
        self.reload_calls.append({"wait_until": wait_until, "timeout": timeout})
        if self._reload_error is not None:
            raise self._reload_error
        return None

    def content(self) -> str:
        self.content_calls += 1
        return self._html

    def locator(self, selector: str):
        if selector == "body":
            return _FakeLocator(self._body_text)
        raise AssertionError(f"unexpected selector: {selector}")

    def title(self) -> str:
        return "Winline"

    def evaluate(self, script: str, arg=None):  # noqa: ARG002
        self.evaluate_calls += 1
        if "document.readyState" in str(script):
            return "complete"
        return False

    def close(self) -> None:
        self.closed = True


class _NamedPageRegistry:
    """Minimal process-wide named-page registry (bookmaker:winline reuse)."""

    def __init__(self) -> None:
        self._pages: Dict[str, _CountingPage] = {}
        self.create_calls: List[str] = []

    def get_or_create_page(self, name: str, *, url: str = "about:blank") -> _CountingPage:
        if name in self._pages:
            return self._pages[name]
        self.create_calls.append(name)
        page = _CountingPage(
            html=_html_body("seed"),
            body_text="seed",
            url=url,
            name=name,
        )
        self._pages[name] = page
        return page


def _html_body(text: str) -> str:
    return f"<html><body>{text}</body></html>"


def _patch_sleep(monkeypatch) -> _SleepCounter:
    counter = _SleepCounter()
    monkeypatch.setattr(odds_parser.time, "sleep", counter)
    return counter


def _run_load(page, url: str, mode: str):
    return odds_parser._load_site_render_payload_camoufox(
        page,
        url,
        initial_wait_seconds=7.0,
        scroll_wait_seconds=2.0,
        acquisition_mode=mode,
    )


# ---------------------------------------------------------------------------
# _page_needs_navigation contract
# ---------------------------------------------------------------------------


def test_page_needs_navigation_blank_root_wrong_true_live_false() -> None:
    live = _CountingPage(html="", body_text="", url=WINLINE_LIVE)
    blank = _CountingPage(html="", body_text="", url="about:blank")
    empty = _CountingPage(html="", body_text="", url="")
    root = _CountingPage(html="", body_text="", url=WINLINE_ROOT)
    root2 = _CountingPage(html="", body_text="", url=WINLINE_ROOT_NO_SLASH)
    wrong_host = _CountingPage(html="", body_text="", url=WINLINE_WRONG_HOST)
    other = _CountingPage(html="", body_text="", url=WINLINE_OTHER_MATCH)

    assert odds_parser._page_needs_navigation(live, WINLINE_LIVE) is False
    assert odds_parser._page_needs_navigation(blank, WINLINE_LIVE) is True
    assert odds_parser._page_needs_navigation(empty, WINLINE_LIVE) is True
    assert odds_parser._page_needs_navigation(root, WINLINE_LIVE) is True
    assert odds_parser._page_needs_navigation(root2, WINLINE_LIVE) is True
    assert odds_parser._page_needs_navigation(wrong_host, WINLINE_LIVE) is True
    assert odds_parser._page_needs_navigation(other, WINLINE_LIVE) is True


# ---------------------------------------------------------------------------
# dynamic_dom matrix
# ---------------------------------------------------------------------------


def test_dynamic_dom_correct_live_url_zero_goto_reload_sleep(monkeypatch) -> None:
    sleep = _patch_sleep(monkeypatch)
    body = "TeamA TeamB live DOM 1.55 2.40"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_LIVE, name=NAMED_PAGE)

    load_status, load_error, html, visible, body_text, diag = _run_load(
        page, WINLINE_LIVE, "dynamic_dom"
    )

    assert page.goto_calls == []
    assert page.reload_calls == []
    assert sleep.calls == [], f"dynamic_dom on live URL must not sleep; got {sleep.calls}"
    assert page.content_calls >= 1
    assert load_status == "ok"
    assert load_error == ""
    assert diag.get("acquisition_mode") == "dynamic_dom"
    assert diag.get("acquisition_error") in (None, "")
    assert body in (body_text or visible or html)


@pytest.mark.parametrize(
    "start_url",
    [
        "about:blank",
        "",
        WINLINE_ROOT,
        WINLINE_ROOT_NO_SLASH,
        WINLINE_WRONG_HOST,
        WINLINE_OTHER_MATCH,
    ],
    ids=["blank", "empty", "root", "root_noslash", "wrong_host", "other_match"],
)
def test_dynamic_dom_wrong_url_exactly_one_goto_repair(monkeypatch, start_url: str) -> None:
    """Wrong/root/blank URL is repaired by one navigation; not a stable miss."""
    sleep = _patch_sleep(monkeypatch)
    body = "TeamA TeamB after repair"
    page = _CountingPage(html=_html_body(body), body_text=body, url=start_url, name=NAMED_PAGE)

    load_status, load_error, html, visible, body_text, diag = _run_load(
        page, WINLINE_LIVE, "dynamic_dom"
    )

    assert len(page.goto_calls) == 1, (
        f"expected exactly 1 goto repair from {start_url!r}; got {page.goto_calls}"
    )
    assert page.goto_calls[0]["url"] == WINLINE_LIVE
    assert page.goto_calls[0]["wait_until"] == "domcontentloaded"
    assert page.reload_calls == [], "repair must use goto, not reload"
    assert sleep.calls == [], f"dynamic_dom repair must not sleep; got {sleep.calls}"
    assert page.url == WINLINE_LIVE
    assert page.content_calls >= 1
    assert load_status == "ok"
    assert diag.get("acquisition_mode") == "dynamic_dom"
    assert diag.get("acquisition_error") in (None, "")


def test_dynamic_dom_goto_failure_surfaced_not_silent_success(monkeypatch) -> None:
    sleep = _patch_sleep(monkeypatch)
    body = "stale root body"
    page = _CountingPage(
        html=_html_body(body),
        body_text=body,
        url=WINLINE_ROOT,
        goto_error=RuntimeError("net::ERR_NAME_NOT_RESOLVED"),
        name=NAMED_PAGE,
    )

    load_status, load_error, html, visible, body_text, diag = _run_load(
        page, WINLINE_LIVE, "dynamic_dom"
    )

    assert len(page.goto_calls) == 1
    assert page.reload_calls == []
    assert sleep.calls == []
    assert load_status != "ok", "navigation failure must not claim ok"
    assert load_status == "partial_load"
    assert load_error
    assert "ERR_NAME_NOT_RESOLVED" in (load_error or "")
    assert diag.get("acquisition_error")
    assert "ERR_NAME_NOT_RESOLVED" in str(diag.get("acquisition_error"))


# ---------------------------------------------------------------------------
# controlled_reload matrix
# ---------------------------------------------------------------------------


def test_controlled_reload_exactly_one_reload_then_dom_no_sleep(monkeypatch) -> None:
    sleep = _patch_sleep(monkeypatch)
    body = "TeamA TeamB after controlled reload"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_LIVE, name=NAMED_PAGE)

    load_status, load_error, html, visible, body_text, diag = _run_load(
        page, WINLINE_LIVE, "controlled_reload"
    )

    assert page.goto_calls == [], f"controlled_reload must not goto on live URL; got {page.goto_calls}"
    assert len(page.reload_calls) == 1
    assert page.reload_calls[0]["wait_until"] == "domcontentloaded"
    assert page.reload_calls[0]["timeout"] == odds_parser.WINLINE_BOUNDED_NAVIGATION_TIMEOUT_MS
    assert page.reload_calls[0]["timeout"] < 30_000
    assert sleep.calls == [], f"controlled_reload must not add hidden sleep; got {sleep.calls}"
    assert page.content_calls >= 1
    assert load_status == "ok"
    assert load_error == ""
    assert diag.get("acquisition_mode") == "controlled_reload"
    assert diag.get("acquisition_error") in (None, "")


def test_controlled_reload_failure_surfaced_single_attempt(monkeypatch) -> None:
    sleep = _patch_sleep(monkeypatch)
    body = "stale"
    page = _CountingPage(
        html=_html_body(body),
        body_text=body,
        url=WINLINE_LIVE,
        reload_error=RuntimeError("net::ERR_CONNECTION_RESET " + ("X" * 400)),
        name=NAMED_PAGE,
    )

    load_status, load_error, html, visible, body_text, diag = _run_load(
        page, WINLINE_LIVE, "controlled_reload"
    )

    assert page.goto_calls == []
    assert len(page.reload_calls) == 1, "exactly one reload attempt even on failure"
    assert page.reload_calls[0]["timeout"] == odds_parser.WINLINE_BOUNDED_NAVIGATION_TIMEOUT_MS
    assert sleep.calls == []
    assert load_status == "partial_load"
    assert load_error
    assert diag.get("acquisition_error")
    assert len(str(diag.get("acquisition_error"))) <= 300


def test_controlled_reload_root_url_repairs_with_goto(monkeypatch) -> None:
    sleep = _patch_sleep(monkeypatch)
    body = "TeamA TeamB repaired live page"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_ROOT)

    load_status, load_error, html, visible, body_text, diag = _run_load(
        page, WINLINE_LIVE, "controlled_reload"
    )

    assert len(page.goto_calls) == 1
    assert page.goto_calls[0]["url"] == WINLINE_LIVE
    assert page.reload_calls == []
    assert sleep.calls == []
    assert load_status == "ok"
    assert load_error == ""
    assert diag.get("acquisition_error") in (None, "")


def test_controlled_reload_redirect_failure_repairs_with_goto(monkeypatch) -> None:
    sleep = _patch_sleep(monkeypatch)
    body = "TeamA TeamB repaired after redirect"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_LIVE)

    def _redirecting_reload(
        wait_until: str = "domcontentloaded", timeout: int = 0
    ) -> None:
        page.reload_calls.append({"wait_until": wait_until, "timeout": timeout})
        page.url = WINLINE_ROOT
        raise RuntimeError("reload redirected to root")

    page.reload = _redirecting_reload  # type: ignore[method-assign]

    load_status, load_error, html, visible, body_text, diag = _run_load(
        page, WINLINE_LIVE, "controlled_reload"
    )

    assert len(page.reload_calls) == 1
    assert len(page.goto_calls) == 1
    assert page.goto_calls[0]["url"] == WINLINE_LIVE
    assert page.url == WINLINE_LIVE
    assert sleep.calls == []
    assert load_status == "ok"
    assert load_error == ""
    assert diag.get("acquisition_error") in (None, "")


def test_controlled_reload_does_not_double_navigate_or_reload(monkeypatch) -> None:
    sleep = _patch_sleep(monkeypatch)
    body = "once"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_LIVE)

    _run_load(page, WINLINE_LIVE, "controlled_reload")

    assert len(page.reload_calls) == 1
    assert page.goto_calls == []
    assert sleep.calls == []
    # second controlled_reload is a separate call — still one reload each
    _run_load(page, WINLINE_LIVE, "controlled_reload")
    assert len(page.reload_calls) == 2
    assert page.goto_calls == []


# ---------------------------------------------------------------------------
# parse_site_in_camoufox_page integration (Winline honors mode)
# ---------------------------------------------------------------------------


def test_parse_site_dynamic_dom_correct_url_no_nav_no_sleep(monkeypatch) -> None:
    sleep = _patch_sleep(monkeypatch)
    body = "TeamA TeamB Победитель 1 карты 1.55 2.40"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_LIVE, name=NAMED_PAGE)

    odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_LIVE,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="dynamic_dom",
    )

    assert page.goto_calls == []
    assert page.reload_calls == []
    assert sleep.calls == []


def test_parse_site_dynamic_dom_root_url_one_goto_repair(monkeypatch) -> None:
    sleep = _patch_sleep(monkeypatch)
    body = "TeamA TeamB map"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_ROOT, name=NAMED_PAGE)

    odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_LIVE,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="dynamic_dom",
    )

    assert len(page.goto_calls) == 1
    assert page.goto_calls[0]["url"] == WINLINE_LIVE
    assert page.reload_calls == []
    assert sleep.calls == []


def test_parse_site_controlled_reload_one_reload_no_sleep(monkeypatch) -> None:
    sleep = _patch_sleep(monkeypatch)
    body = "TeamA TeamB after reload"
    page = _CountingPage(html=_html_body(body), body_text=body, url=WINLINE_LIVE, name=NAMED_PAGE)

    result = odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_LIVE,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="controlled_reload",
    )

    assert page.goto_calls == []
    assert len(page.reload_calls) == 1
    assert sleep.calls == []
    assert getattr(result, "acquisition_mode", None) == "controlled_reload"


def test_winline_poller_never_follows_candidate_href_after_reload(monkeypatch) -> None:
    """A shared listing page must stay on the listing after controlled reload."""
    sleep = _patch_sleep(monkeypatch)
    body = "TeamA TeamB after reload"
    page = _CountingPage(
        html=_html_body(body),
        body_text=body,
        url=WINLINE_LIVE,
        name=NAMED_PAGE,
    )
    candidate_calls: List[List[str]] = []

    async def _unexpected_candidate_open(_page, _site, urls, _team1, _team2):
        candidate_calls.append(list(urls))
        return "https://winline.ru/stavki/sport/kibersport/match/unexpected"

    monkeypatch.setattr(
        odds_parser,
        "_camoufox_find_match_by_urls_async",
        _unexpected_candidate_open,
    )

    result = odds_parser.parse_site_in_camoufox_page(
        page,
        "winline",
        WINLINE_LIVE,
        "TeamA",
        "TeamB",
        mode="live",
        forced_map_num=1,
        acquisition_mode="controlled_reload",
    )

    assert candidate_calls == []
    assert page.url == WINLINE_LIVE
    assert len(page.reload_calls) == 1
    assert sleep.calls == []
    assert getattr(result, "acquisition_mode", None) == "controlled_reload"


# ---------------------------------------------------------------------------
# named page bookmaker:winline reuse (no duplicate page creation)
# ---------------------------------------------------------------------------


def test_named_page_bookmaker_winline_reused_no_duplicate_creation(monkeypatch) -> None:
    """Same named page identity across sequential acquisitions; one create only."""
    sleep = _patch_sleep(monkeypatch)
    registry = _NamedPageRegistry()
    page = registry.get_or_create_page(NAMED_PAGE, url=WINLINE_LIVE)
    page._html = _html_body("TeamA TeamB DOM")
    page._body_text = "TeamA TeamB DOM"

    ids: List[int] = []
    for mode in ("dynamic_dom", "dynamic_dom", "controlled_reload", "dynamic_dom"):
        p = registry.get_or_create_page(NAMED_PAGE)
        ids.append(p.page_id)
        _run_load(p, WINLINE_LIVE, mode)

    assert registry.create_calls == [NAMED_PAGE]
    assert len(set(ids)) == 1, f"must reuse one page identity; got {ids}"
    assert page.goto_calls == []
    assert len(page.reload_calls) == 1  # only controlled_reload
    assert sleep.calls == []
    # still the same object
    assert registry.get_or_create_page(NAMED_PAGE) is page


def test_named_page_repair_keeps_same_page_object(monkeypatch) -> None:
    """URL repair navigates the existing named page; does not spawn a second page."""
    sleep = _patch_sleep(monkeypatch)
    registry = _NamedPageRegistry()
    page = registry.get_or_create_page(NAMED_PAGE, url=WINLINE_ROOT)
    page._html = _html_body("repaired")
    page._body_text = "repaired"

    p1 = registry.get_or_create_page(NAMED_PAGE)
    _run_load(p1, WINLINE_LIVE, "dynamic_dom")
    p2 = registry.get_or_create_page(NAMED_PAGE)
    _run_load(p2, WINLINE_LIVE, "dynamic_dom")

    assert registry.create_calls == [NAMED_PAGE]
    assert p1 is p2 is page
    assert len(page.goto_calls) == 1  # first repair only; second already live
    assert page.reload_calls == []
    assert sleep.calls == []
    assert page.url == WINLINE_LIVE
