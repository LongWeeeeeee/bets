"""Winline shadow must fire at every successfully-parsed map seam.

Contract (GREEN production hook + resilient source-location checks):
- Required seam in check_head: after check_uniq_url acceptance + bookmaker_map_num +
  ordered original team names + successful draft/map parse are known, and BEFORE any
  STAR/candidate/delivery early return.
- Exact production statement that marks the seam (post-validation success):
    match_log(f"   ✅ Драфт успешно распарсен")
  At that point the following locals are already bound:
    check_uniq_url, bookmaker_map_num, radiant_team_name_original /
    dire_team_name_original, heroes validated.
- Fail-open every-parsed-map call is immediately after that draft-success statement
  (selected_side=None, delivery_confirmed=False), independent of STAR/send.
- Retained post-send call stays after the first STAR-only early send branch
  (selected_side=dispatch_message_side, delivery_confirmed=delivery_confirmed);
  controller dedup prevents double-acquire of the same map.
- Line numbers are discovered from unique semantic statements in current source
  (not absolute constants), so unrelated insertions do not break this contract.
- Behaviour is asserted via the existing fail-open controller wrapper
  (no second browser/process/network).
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

import pytest
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402

SEAM_SUCCESS_DRAFT_STMT = 'match_log(f"   ✅ Драфт успешно распарсен")'
SHADOW_FAIL_OPEN_CALL = "_fail_open_winline_shadow_after_send("
STAR_SIGNAL_SENT_NOW_REASON = 'add_url_reason="star_signal_sent_now"'
MATCH_PATH = "test-shadow-every-map"
SERIES_URL = f"dltv.org/matches/{MATCH_PATH}"
TEAM1 = "Radiant Team"
TEAM2 = "Dire Team"


def _find_unique_line(
    lines: Sequence[str],
    predicate: Callable[[str], bool],
    *,
    label: str,
) -> int:
    hits = [idx for idx, line in enumerate(lines, 1) if predicate(line)]
    assert len(hits) == 1, f"expected unique {label}, got {hits!r}"
    return hits[0]


def _call_site_kwargs(lines: Sequence[str], call_line: int, *, max_span: int = 12) -> str:
    """Return joined call-site text from the call line through its closing paren."""
    chunk: List[str] = []
    for offset in range(max_span):
        idx = call_line - 1 + offset
        if idx >= len(lines):
            break
        chunk.append(lines[idx])
        if lines[idx].strip() == ")":
            break
    return "\n".join(chunk)


def _discover_winline_shadow_seam_lines(lines: Sequence[str]) -> Dict[str, int]:
    """Locate unique semantic positions for the every-parsed-map seam contract."""
    draft_line = _find_unique_line(
        lines,
        lambda line: SEAM_SUCCESS_DRAFT_STMT in line,
        label="successful draft parse statement",
    )

    call_lines = [
        idx
        for idx, line in enumerate(lines, 1)
        if SHADOW_FAIL_OPEN_CALL in line and not line.lstrip().startswith("def ")
    ]
    assert len(call_lines) == 2, (
        "expected exactly two live _fail_open_winline_shadow_after_send call sites "
        f"(every-parsed-map + retained post-send), got {call_lines!r}"
    )

    every_map_line = None
    post_send_line = None
    for call_line in call_lines:
        body = _call_site_kwargs(lines, call_line)
        if "selected_side=None" in body and "delivery_confirmed=False" in body:
            assert every_map_line is None, (
                f"duplicate every-parsed-map shadow call sites: {every_map_line}, {call_line}"
            )
            every_map_line = call_line
        elif (
            "selected_side=dispatch_message_side" in body
            and "delivery_confirmed=delivery_confirmed" in body
        ):
            assert post_send_line is None, (
                f"duplicate post-send shadow call sites: {post_send_line}, {call_line}"
            )
            post_send_line = call_line
        else:
            raise AssertionError(
                f"unclassified _fail_open_winline_shadow_after_send call at L{call_line}: "
                f"{body!r}"
            )

    assert every_map_line is not None, "missing every-parsed-map shadow call site"
    assert post_send_line is not None, "missing retained post-send shadow call site"

    # First STAR-only early send/return branch marker (bare star_signal_sent_now reason).
    star_hits = [
        idx
        for idx, line in enumerate(lines, 1)
        if STAR_SIGNAL_SENT_NOW_REASON in line and idx > draft_line
    ]
    assert star_hits, "missing first STAR-only early-return/send branch after draft success"
    first_star_send_line = star_hits[0]

    return {
        "draft_success": draft_line,
        "every_map_call": every_map_line,
        "first_star_send": first_star_send_line,
        "post_send_call": post_send_line,
    }


class _FakeTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code


class _FakeJsonResponse:
    def __init__(self, payload: Dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = "{}"

    def json(self) -> Dict[str, Any]:
        return self._payload


def _build_heads_and_bodies():
    html = f"""
    <div class="head">
      <div class="event__info-info__time">live</div>
    </div>
    <div class="body">
      <div class="match__item-team__score">0</div>
      <div class="match__item-team__score">0</div>
      <a href="https://dltv.org/matches/{MATCH_PATH}"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None
    return [head], [body]


def _valid_heroes(seed: int) -> Dict[str, Dict[str, int]]:
    return {
        "pos1": {"hero_id": seed + 1, "account_id": seed + 101},
        "pos2": {"hero_id": seed + 2, "account_id": seed + 102},
        "pos3": {"hero_id": seed + 3, "account_id": seed + 103},
        "pos4": {"hero_id": seed + 4, "account_id": seed + 104},
        "pos5": {"hero_id": seed + 5, "account_id": seed + 105},
    }


def _clear_runtime_state() -> None:
    with cs.monitored_matches_lock:
        cs.monitored_matches.clear()
    try:
        with cs._kills_pre_pass_sent_lock:
            cs._kills_pre_pass_sent_urls.clear()
    except Exception:
        pass
    try:
        with cs.draft_metrics_cache_lock:
            cs.draft_metrics_cache.clear()
    except Exception:
        pass
    if hasattr(cs, "reset_winline_shadow_activation_state"):
        try:
            cs.reset_winline_shadow_activation_state()
        except Exception:
            pass
    try:
        cs._winline_shadow_activation_state = {}
    except Exception:
        pass


def _patch_common_no_odds_harness(
    monkeypatch,
    *,
    parse_ok: bool,
    live_map_num: int = 1,
    sent_messages: Optional[List[str]] = None,
    add_url_calls: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Drive check_head under --no-odds with STAR false and no ordinary send.

    Returns expected check_uniq_url series identity used by the harness.
    """
    if sent_messages is None:
        sent_messages = []
    if add_url_calls is None:
        add_url_calls = []

    _clear_runtime_state()

    # --no-odds production capsule: BOOKMAKER_PREFETCH_ENABLED is the predicate
    # used by _fail_open_winline_shadow_after_send.
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(cs, "DOTA2PROTRACKER_ENABLED", False, raising=False)
    monkeypatch.setattr(cs, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(cs, "LANE_ADV_STANDALONE_KILLS_ENABLED", False, raising=False)
    monkeypatch.setattr(cs, "PIPELINE_SEND_EVERY_PARSED_MATCH", False, raising=False)
    monkeypatch.setattr(cs, "PIPELINE_DISABLE_SIGNAL_GATES", False, raising=False)

    monkeypatch.setattr(cs, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(cs, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(cs, "_drop_delayed_match", lambda *_a, **_k: False)
    monkeypatch.setattr(cs, "_skip_dispatch_for_processed_url", lambda *_a, **_k: False)
    monkeypatch.setattr(cs, "_acquire_signal_send_slot", lambda *_a, **_k: True)
    monkeypatch.setattr(cs, "_release_signal_send_slot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_mark_url_processed", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_log_bookmaker_source_snapshot", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cs,
        "_refresh_message_bookmaker_block_for_dispatch",
        lambda _match_key, message: message,
    )
    monkeypatch.setattr(
        cs, "send_message", lambda message, **_k: sent_messages.append(str(message))
    )

    def _record_add_url(url: str, reason: str = "unspecified", details: Any = None):
        add_url_calls.append(
            {
                "url": url,
                "reason": reason,
                "details": dict(details) if isinstance(details, dict) else details,
            }
        )

    monkeypatch.setattr(cs, "add_url", _record_add_url)

    page_html = f"<html><script>$.get('/live/{MATCH_PATH}.json')</script></html>"
    monkeypatch.setattr(
        cs,
        "make_request_with_retry",
        lambda *_a, **_k: _FakeTextResponse(page_html, status_code=200),
    )

    # map_num = series wins sum + 1; score "0 : 0" also yields 1 when wins absent.
    series_wins_r = max(0, int(live_map_num) - 1)
    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {
                "is_radiant": True,
                "title": TEAM1,
                "team_id": 1001,
                "id": 1001,
            },
            "second_team": {"title": TEAM2, "team_id": 2002, "id": 2002},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
            "radiant_series_wins": series_wins_r,
            "dire_series_wins": 0,
            "game_map_number": int(live_map_num),
        },
        "radiant_lead": 0.0,
        "game_time": float(10 * 60),
    }
    monkeypatch.setattr(
        cs.requests,
        "get",
        lambda *_a, **_k: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_a, **_k):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(cs, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        cs,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(cs, "_determine_star_signal_match_tier", lambda *_a, **_k: 1)

    if parse_ok:
        monkeypatch.setattr(
            cs,
            "parse_draft_and_positions",
            lambda *_a, **_k: (
                _valid_heroes(0),
                _valid_heroes(100),
                None,
                "",
                [],
            ),
        )
    else:
        monkeypatch.setattr(
            cs,
            "parse_draft_and_positions",
            lambda *_a, **_k: (
                {},
                {},
                "draft incomplete",
                "missing heroes",
                [],
            ),
        )

    # Metrics deliberately fail STAR selection (no valid star block / no candidate).
    monkeypatch.setattr(
        cs,
        "synergy_and_counterpick",
        lambda *_a, **_k: {
            "early_output": {"solo": 0},
            "mid_output": {"solo": 0},
            "post_lane_output": {"synergy_duo": 0},
        },
    )
    monkeypatch.setattr(cs, "lane_data", {}, raising=False)
    monkeypatch.setattr(cs, "calculate_lanes", lambda *_a, **_k: ("", "", ""))
    monkeypatch.setattr(cs, "format_output_dict", lambda *_a, **_k: True)
    monkeypatch.setattr(
        cs,
        "_star_block_diagnostics",
        lambda *, raw_block, target_wr, section: {
            "valid": False,
            "status": "no_hits",
            "sign": 0,
            "hit_metrics": [],
            "conflict_metric": None,
        },
    )
    monkeypatch.setattr(
        cs,
        "_block_signs_same_or_zero",
        lambda *_a, **_k: {"valid": False, "status": "no_sign"},
    )
    monkeypatch.setattr(cs, "_format_raw_star_block_metrics", lambda *_a, **_k: "none")
    monkeypatch.setattr(
        cs, "_decorate_star_block_for_display", lambda raw_block, **_k: dict(raw_block or {})
    )
    monkeypatch.setattr(cs.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(cs, "_match_has_tier1_team", lambda *_a, **_k: True)

    # Expected series identity: dltv listing uses path + uniq_score (score 0+0=0).
    return f"{SERIES_URL}.0"


def test_seam_source_location_is_after_successful_draft_parse():
    """Discover unique semantic positions and assert required ordering.

    Order: successful draft statement < every-parsed-map call < first STAR-only
    early-return/send branch (add_url_reason=\"star_signal_sent_now\") < retained
    post-send call.
    """
    src = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")
    lines = src.splitlines()
    seam = _discover_winline_shadow_seam_lines(lines)

    draft_line = seam["draft_success"]
    every_map_line = seam["every_map_call"]
    first_star_line = seam["first_star_send"]
    post_send_line = seam["post_send_call"]

    assert SEAM_SUCCESS_DRAFT_STMT in lines[draft_line - 1]
    assert "Драфт успешно распарсен" in lines[draft_line - 1]
    assert SHADOW_FAIL_OPEN_CALL in lines[every_map_line - 1]
    assert SHADOW_FAIL_OPEN_CALL in lines[post_send_line - 1]
    assert STAR_SIGNAL_SENT_NOW_REASON in lines[first_star_line - 1]

    every_map_body = _call_site_kwargs(lines, every_map_line)
    post_send_body = _call_site_kwargs(lines, post_send_line)
    assert "selected_side=None" in every_map_body
    assert "delivery_confirmed=False" in every_map_body
    assert "selected_side=dispatch_message_side" in post_send_body
    assert "delivery_confirmed=delivery_confirmed" in post_send_body

    assert draft_line < every_map_line < first_star_line < post_send_line, (
        "expected ordering draft_success < every_map_call < first STAR send "
        f"< post_send_call; got draft={draft_line}, every_map={every_map_line}, "
        f"first_star={first_star_line}, post_send={post_send_line}"
    )
    # Every-parsed-map call is the next live statement after draft success
    # (comments may sit between them).
    assert every_map_line - draft_line <= 10, (
        f"every-parsed-map call too far from draft success: "
        f"draft={draft_line}, every_map={every_map_line}"
    )

    # Existing controller wrapper used by both call sites (reuse, don't reinvent).
    assert hasattr(cs, "_fail_open_winline_shadow_after_send")
    assert callable(cs._fail_open_winline_shadow_after_send)
    assert hasattr(cs, "maybe_run_winline_shadow_activation")
    assert callable(cs.maybe_run_winline_shadow_activation)
    assert hasattr(cs, "run_winline_shadow_request")
    assert callable(cs.run_winline_shadow_request)


def test_successfully_parsed_map_triggers_winline_shadow_without_star_or_send(
    monkeypatch,
):
    """Any successfully parsed current map under --no-odds must invoke shadow.

    STAR is false, no bet candidate/selected_side, no ordinary Telegram send.
    Expected identity: series check_uniq_url, current map_num, original team order.
    """
    sent_messages: List[str] = []
    add_url_calls: List[Dict[str, Any]] = []
    shadow_calls: List[Dict[str, Any]] = []

    expected_key = _patch_common_no_odds_harness(
        monkeypatch,
        parse_ok=True,
        live_map_num=2,
        sent_messages=sent_messages,
        add_url_calls=add_url_calls,
    )

    def _capture_fail_open(**kwargs):
        shadow_calls.append(dict(kwargs))
        return 0

    # Existing production boundary: fail-open wrapper around the controller.
    monkeypatch.setattr(cs, "_fail_open_winline_shadow_after_send", _capture_fail_open)

    heads, bodies = _build_heads_and_bodies()
    result = cs.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=set(),
        return_status=None,
    )

    # Ordinary no-odds path: no STAR send, no Telegram traffic.
    assert sent_messages == [], (
        "ordinary --no-odds path with STAR false must not send Telegram; "
        f"got {sent_messages!r}"
    )
    assert not any(
        str(c.get("reason") or "").startswith("star_signal_sent_now") for c in add_url_calls
    ), f"unexpected STAR send reasons: {add_url_calls!r}"

    # Core assertion: shadow must run at the parsed-map seam, independent of STAR.
    assert shadow_calls, (
        "expected non-sending Winline shadow acquisition after successful draft/map "
        f"parse at check_head seam ({SEAM_SUCCESS_DRAFT_STMT!r}); "
        "must not depend on star_signal_sent_now post-send path. "
        f"add_url_calls={add_url_calls!r} result={result!r}"
    )

    call = shadow_calls[0]
    assert call.get("match_key") == expected_key, call
    assert int(call.get("map_num")) == 2, call
    assert call.get("team1") == TEAM1, call
    assert call.get("team2") == TEAM2, call
    # Must not depend on delivery success / selected bet side.
    # selected_side may be None/absent when no candidate exists.
    if "selected_side" in call:
        assert call["selected_side"] in (None, "", "radiant", "dire", TEAM1, TEAM2)
    if "delivery_confirmed" in call:
        # Non-gating: even False/None is allowed; presence of the call is the contract.
        assert call["delivery_confirmed"] in (None, False, True)


def test_shadow_exception_is_fail_open_and_does_not_send_telegram(monkeypatch):
    """Inject exception at the shadow boundary: ordinary check_head flow unchanged."""
    sent_messages: List[str] = []
    add_url_calls: List[Dict[str, Any]] = []

    _patch_common_no_odds_harness(
        monkeypatch,
        parse_ok=True,
        live_map_num=1,
        sent_messages=sent_messages,
        add_url_calls=add_url_calls,
    )

    def _boom(**_kwargs):
        raise RuntimeError("injected shadow boundary failure")

    # Prefer patching the real wrapper body path used in production: when the
    # every-parsed-map hook lands, it must remain fail-open like post-send.
    # For RED: if the hook is missing, the primary parsed-map test already fails;
    # this test still proves that *when* the wrapper is exercised, exceptions
    # do not escape / do not trigger diagnostic Telegram sends.
    original = cs._fail_open_winline_shadow_after_send

    def _fail_open_then_boom(**kwargs):
        # Simulate production fail-open: swallow exceptions at the boundary.
        try:
            raise RuntimeError("injected shadow boundary failure")
        except Exception:
            return None

    # Also exercise the real production wrapper against a boom controller.
    monkeypatch.setattr(cs, "maybe_run_winline_shadow_activation", _boom)
    monkeypatch.setattr(cs, "run_winline_shadow_request", _boom)

    heads, bodies = _build_heads_and_bodies()
    # Must not raise out of check_head.
    result = cs.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=set(),
        return_status=None,
    )
    assert result is not None or result is None  # any ordinary return is fine
    assert sent_messages == [], (
        "fail-open shadow must not attempt diagnostic Telegram send; "
        f"got {sent_messages!r}"
    )

    # Explicit: production fail-open helper itself swallows controller errors.
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    rc = original(
        match_key=f"{SERIES_URL}.0",
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        delivery_confirmed=False,
    )
    assert rc is None
    assert sent_messages == []


def test_unsuccessful_parse_does_not_trigger_winline_shadow(monkeypatch):
    """Ineligible path: no shadow call before required parse facts exist."""
    sent_messages: List[str] = []
    shadow_calls: List[Dict[str, Any]] = []

    _patch_common_no_odds_harness(
        monkeypatch,
        parse_ok=False,
        live_map_num=1,
        sent_messages=sent_messages,
    )

    def _capture_fail_open(**kwargs):
        shadow_calls.append(dict(kwargs))
        return 0

    monkeypatch.setattr(cs, "_fail_open_winline_shadow_after_send", _capture_fail_open)
    # Also trap the controller/seam in case a future path bypasses the wrapper.
    monkeypatch.setattr(
        cs,
        "maybe_run_winline_shadow_activation",
        lambda **kwargs: shadow_calls.append({"via": "maybe", **kwargs}) or 0,
    )
    monkeypatch.setattr(
        cs,
        "run_winline_shadow_request",
        lambda **kwargs: shadow_calls.append({"via": "request", **kwargs}) or 0,
    )

    heads, bodies = _build_heads_and_bodies()
    cs.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert shadow_calls == [], (
        "shadow must not run before successful draft/map parse facts exist; "
        f"got {shadow_calls!r}"
    )
    assert sent_messages == []
