"""Worker-thread ownership guards for shared named pages.

REPLAN1 FIX-B: get_or_create_page / invalidate_named_page must reject every
caller unless the current thread is the active shared-session worker thread,
including before startup and after shutdown (_worker_thread_id is None).
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402


class _FakePage:
    def __init__(self, name: str = "fake") -> None:
        self.name = name
        self.closed = False
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class _FakeBrowser:
    def __init__(self) -> None:
        self.new_page_calls = 0
        self.pages: List[_FakePage] = []

    def new_page(self) -> _FakePage:
        self.new_page_calls += 1
        page = _FakePage(name=f"page-{self.new_page_calls}")
        self.pages.append(page)
        return page


def _fresh_session() -> cs._SharedCamoufoxSession:
    """Fresh session with no worker thread (pre-start: _worker_thread_id is None)."""
    session = cs._SharedCamoufoxSession()
    assert session._worker_thread_id is None
    assert session._named_pages == {}
    return session


def test_get_or_create_page_rejects_before_startup_without_factory_or_registry_mutation() -> None:
    """Pre-start: foreign caller must hit the established worker-thread RuntimeError."""
    session = _fresh_session()
    browser = _FakeBrowser()
    registry_before = dict(session._named_pages)

    with pytest.raises(RuntimeError, match="get_or_create_page must be called from the shared Camoufox worker thread"):
        session.get_or_create_page("bookmaker:winline", browser)

    assert browser.new_page_calls == 0, "rejected get_or_create_page must not invoke page factory"
    assert dict(session._named_pages) == registry_before, "rejected get_or_create_page must not mutate registry"
    assert session._worker_thread_id is None


def test_invalidate_named_page_rejects_before_startup_without_close_or_registry_mutation() -> None:
    """Pre-start: foreign invalidate must raise the established worker-thread RuntimeError."""
    session = _fresh_session()
    page = _FakePage("seed")
    session._named_pages["bookmaker:winline"] = page  # seed registry; still no active worker
    registry_before = dict(session._named_pages)

    with pytest.raises(RuntimeError, match="invalidate_named_page must be called from the shared Camoufox worker thread"):
        session.invalidate_named_page("bookmaker:winline")

    assert page.close_calls == 0, "rejected invalidate_named_page must not close a page"
    assert not page.closed
    assert dict(session._named_pages) == registry_before, "rejected invalidate must not mutate registry"
    assert session._worker_thread_id is None


def test_get_or_create_page_rejects_after_shutdown_without_factory_or_registry_mutation() -> None:
    """Post-shutdown: _worker_thread_id is None again; foreign callers still rejected."""
    session = _fresh_session()
    browser = _FakeBrowser()

    # Simulate post-shutdown: worker id was set then cleared (as _worker finally does).
    session._worker_thread_id = threading.get_ident()
    session._worker_thread_id = None
    assert session._worker_thread_id is None
    registry_before = dict(session._named_pages)

    with pytest.raises(RuntimeError, match="get_or_create_page must be called from the shared Camoufox worker thread"):
        session.get_or_create_page("protracker:matchups", browser)

    assert browser.new_page_calls == 0
    assert dict(session._named_pages) == registry_before
    assert session._worker_thread_id is None


def test_invalidate_named_page_rejects_after_shutdown_without_close_or_registry_mutation() -> None:
    """Post-shutdown: invalidate must not close/mutate when no active worker id."""
    session = _fresh_session()
    page = _FakePage("post-shutdown")
    session._named_pages["protracker:matchups"] = page
    session._worker_thread_id = threading.get_ident()
    session._worker_thread_id = None
    assert session._worker_thread_id is None
    registry_before = dict(session._named_pages)

    with pytest.raises(RuntimeError, match="invalidate_named_page must be called from the shared Camoufox worker thread"):
        session.invalidate_named_page("protracker:matchups")

    assert page.close_calls == 0
    assert not page.closed
    assert dict(session._named_pages) == registry_before
    assert session._worker_thread_id is None
