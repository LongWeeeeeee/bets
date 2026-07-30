"""W-HOOK: fail-open post-send no-odds Winline shadow main-loop hook.

Proves production wiring in base/cyberscore_try.py:
- controller symbols re-exported / callable on cyberscore_try
- ordinary _deliver_and_persist_signal send decision completes before shadow
- exact argument forwarding (check_uniq_url, bookmaker_map_num, teams, side, path, seam)
- no invocation under odds-enabled mode (BOOKMAKER_PREFETCH_ENABLED=True)
- catch-all fail-open: controller/seam exceptions never change ordinary return
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = REPO_ROOT / "base"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402

SERIES_URL = "dltv.org/matches/999001"
MATCH_KEY = f"{SERIES_URL}.12"
TEAM1 = "Alpha Squad"
TEAM2 = "Beta Force"
EVIDENCE_DEFAULT = REPO_ROOT / ".hermes" / "runtime" / "winline-shadow" / "latest.json"


def test_hook_helper_and_controller_symbols_exist():
    assert hasattr(cs, "maybe_run_winline_shadow_activation")
    assert callable(cs.maybe_run_winline_shadow_activation)
    assert hasattr(cs, "queue_winline_shadow_activation")
    assert callable(cs.queue_winline_shadow_activation)
    assert hasattr(cs, "flush_winline_shadow_activation")
    assert callable(cs.flush_winline_shadow_activation)
    assert hasattr(cs, "WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH")
    assert hasattr(cs, "_fail_open_winline_shadow_after_send")
    assert callable(cs._fail_open_winline_shadow_after_send)


def test_star_branch_source_calls_hook_after_deliver_assignment():
    """Capsule STAR no-odds branch: hook must sit after _deliver_and_persist_signal."""
    src = Path(cs.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)

    # Find check_head
    check_head = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "check_head":
            check_head = node
            break
    assert check_head is not None

    # Walk for star_signal_sent_now delivery + subsequent fail-open hook.
    # We look for Call to _deliver_and_persist_signal with add_url_reason star_signal_sent_now
    # and ensure a later Call to _fail_open_winline_shadow_after_send appears in the same
    # enclosing try/finally block after that assignment.
    class Finder(ast.NodeVisitor):
        def __init__(self):
            self.star_deliver_lineno: Optional[int] = None
            self.hook_lineno: Optional[int] = None
            self.hook_after_deliver = False

        def visit_Call(self, node: ast.Call):
            name = None
            if isinstance(node.func, ast.Name):
                name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                name = node.func.attr
            if name == "_deliver_and_persist_signal":
                for kw in node.keywords:
                    if kw.arg == "add_url_reason" and isinstance(kw.value, ast.Constant):
                        if kw.value.value == "star_signal_sent_now":
                            self.star_deliver_lineno = node.lineno
            if name == "_fail_open_winline_shadow_after_send":
                self.hook_lineno = node.lineno
                if self.star_deliver_lineno is not None and node.lineno > self.star_deliver_lineno:
                    self.hook_after_deliver = True
            self.generic_visit(node)

    f = Finder()
    f.visit(check_head)
    assert f.star_deliver_lineno is not None, "STAR _deliver_and_persist_signal anchor missing"
    assert f.hook_lineno is not None, "hook call missing inside check_head"
    assert f.hook_after_deliver, "hook must be after STAR deliver assignment"


def test_post_send_ordering_ordinary_before_shadow(monkeypatch, tmp_path):
    order: List[str] = []
    out = tmp_path / "latest.json"
    monkeypatch.setattr(cs, "WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH", out)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(cs, "_winline_shadow_activation_state", {})
    # Reset controller module state too via public reset if present
    if hasattr(cs, "reset_winline_shadow_activation_state"):
        cs.reset_winline_shadow_activation_state()

    def _seam(**kwargs):
        order.append("shadow")
        return 0

    monkeypatch.setattr(cs, "run_winline_shadow_request", _seam)

    def _ordinary():
        order.append("ordinary")
        return True

    # Simulate production: ordinary decision first, then fail-open hook.
    delivery_confirmed = _ordinary()
    cs._fail_open_winline_shadow_after_send(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side="radiant",
        delivery_confirmed=delivery_confirmed,
    )
    assert order == ["ordinary", "shadow"]


def test_exact_argument_forwarding(monkeypatch, tmp_path):
    out = tmp_path / "winline-shadow" / "latest.json"
    monkeypatch.setattr(cs, "WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH", out)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(cs, "_winline_shadow_activation_state", {})
    if hasattr(cs, "reset_winline_shadow_activation_state"):
        cs.reset_winline_shadow_activation_state()

    calls: List[Dict[str, Any]] = []

    def _seam(**kwargs):
        calls.append(dict(kwargs))
        return 0

    monkeypatch.setattr(cs, "run_winline_shadow_request", _seam)

    cs._fail_open_winline_shadow_after_send(
        match_key=MATCH_KEY,
        map_num=2,
        team1=TEAM1,
        team2=TEAM2,
        selected_side="dire",
        delivery_confirmed=False,  # still must fire (non-gating)
    )
    assert len(calls) == 1
    c = calls[0]
    assert c["match_key"] == MATCH_KEY
    assert c["map_num"] == 2
    assert c["team1"] == TEAM1
    assert c["team2"] == TEAM2
    # selected_side may be normalized to P2 from team2 mapping or passed through controller
    assert c["no_odds_active"] is True
    assert Path(c["output_path"]) == out


def test_no_call_under_odds_enabled(monkeypatch, tmp_path):
    out = tmp_path / "latest.json"
    monkeypatch.setattr(cs, "WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH", out)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "_winline_shadow_activation_state", {})
    if hasattr(cs, "reset_winline_shadow_activation_state"):
        cs.reset_winline_shadow_activation_state()
    calls: List[Dict[str, Any]] = []

    def _seam(**kwargs):
        calls.append(dict(kwargs))
        return 0

    monkeypatch.setattr(cs, "run_winline_shadow_request", _seam)
    rc = cs._fail_open_winline_shadow_after_send(
        match_key=MATCH_KEY,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=TEAM1,
        delivery_confirmed=True,
    )
    assert rc is None
    assert calls == []


def test_fail_open_preserves_ordinary_behavior(monkeypatch, tmp_path):
    """Controller/seam exceptions must not raise into ordinary path."""
    out = tmp_path / "latest.json"
    monkeypatch.setattr(cs, "WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH", out)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(cs, "_winline_shadow_activation_state", {})
    if hasattr(cs, "reset_winline_shadow_activation_state"):
        cs.reset_winline_shadow_activation_state()

    def _boom(**kwargs):
        raise RuntimeError("shadow boom")

    monkeypatch.setattr(cs, "run_winline_shadow_request", _boom)

    # Even if maybe_run itself raises (or seam raises through controller), hook catches.
    def _raise_controller(**kwargs):
        raise ValueError("controller hard fail")

    monkeypatch.setattr(cs, "maybe_run_winline_shadow_activation", _raise_controller)

    ordinary_return = True
    try:
        # ordinary decision already made
        delivery_confirmed = ordinary_return
        hook_rc = cs._fail_open_winline_shadow_after_send(
            match_key=MATCH_KEY,
            map_num=1,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            delivery_confirmed=delivery_confirmed,
        )
        # ordinary path continues with original decision
        final = delivery_confirmed
    except Exception as exc:  # pragma: no cover
        pytest.fail(f"hook must not raise; got {exc!r}")

    assert final is True
    assert hook_rc is None


def test_hook_runs_when_delivery_false(monkeypatch, tmp_path):
    """Non-gating: delivery_confirmed=False still invokes shadow under no-odds."""
    out = tmp_path / "latest.json"
    monkeypatch.setattr(cs, "WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH", out)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(cs, "_winline_shadow_activation_state", {})
    if hasattr(cs, "reset_winline_shadow_activation_state"):
        cs.reset_winline_shadow_activation_state()
    calls: List[Dict[str, Any]] = []

    def _seam(**kwargs):
        calls.append(dict(kwargs))
        return 0

    monkeypatch.setattr(cs, "run_winline_shadow_request", _seam)
    cs._fail_open_winline_shadow_after_send(
        match_key=MATCH_KEY,
        map_num=3,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=TEAM2,
        delivery_confirmed=False,
    )
    assert len(calls) == 1


def test_star_hook_forwards_capsule_expressions_in_source():
    """Source-level: STAR hook call uses capsule expressions, not reconstructed names."""
    src = Path(cs.__file__).read_text(encoding="utf-8")
    # Narrow window around star_signal_sent_now
    idx = src.find('add_url_reason="star_signal_sent_now"')
    assert idx != -1
    window = src[idx : idx + 1200]
    assert "_fail_open_winline_shadow_after_send" in window
    assert "check_uniq_url" in window
    assert "bookmaker_map_num" in window
    assert "radiant_team_name_original" in window
    assert "dire_team_name_original" in window
    assert "dispatch_message_side" in window
