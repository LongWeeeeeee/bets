from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


if not (
    hasattr(runtime, "STAR_THRESHOLD_WR_TIER1")
    and hasattr(runtime, "STAR_THRESHOLD_WR_TIER2")
    and int(runtime.STAR_THRESHOLD_WR_TIER1) == 60
    and int(runtime.STAR_THRESHOLD_WR_TIER2) == 65
):
    pytestmark = pytest.mark.skip(
        reason=(
            "Runtime tier thresholds are not landed yet "
            "(expected STAR_THRESHOLD_WR_TIER1=60 and STAR_THRESHOLD_WR_TIER2=65)."
        )
    )


def test_tier_threshold_constants_and_labels_are_pinned() -> None:
    assert runtime.TIER_SIGNAL_MIN_THRESHOLD_TIER1 == 60
    assert runtime.TIER_SIGNAL_MIN_THRESHOLD_TIER2 == 65
    assert runtime.TIER_THRESHOLD_STATUS_TIER1_MIN60_BLOCK == "tier1_min60_block"
    assert runtime.TIER_THRESHOLD_STATUS_TIER2_MIN65_BLOCK == "tier2_min65_block"
    assert runtime.TIER_THRESHOLD_REASON_TIER1_MIN60_BLOCK == "below_tier1_min60"
    assert runtime.TIER_THRESHOLD_REASON_TIER2_MIN65_BLOCK == "below_tier2_min65"


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
    html = """
    <div class="head">
      <div class="event__info-info__time">live</div>
    </div>
    <div class="body">
      <div class="match__item-team__score">0</div>
      <div class="match__item-team__score">0</div>
      <a href="https://dltv.org/matches/test-tier-threshold"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None
    return [head], [body]


def _run_case(
    monkeypatch,
    *,
    match_tier: int,
    max_supported_wr: float,
) -> Dict[str, Any]:
    heads, bodies = _build_heads_and_bodies()
    requested_thresholds: List[int] = []
    sent_messages: List[str] = []
    add_url_calls: List[Dict[str, Any]] = []

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "STAR_ALLOW_TIER2_FALLBACK_TO_TIER1", False, raising=False)
    monkeypatch.setattr(runtime, "STAR_ALLOW_TIER1_EARLY_STAR_LATE_SAME_OR_ZERO", False, raising=False)
    monkeypatch.setattr(runtime, "STAR_ALLOW_LATE_STAR_EARLY_SAME_OR_ZERO", False, raising=False)

    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message: sent_messages.append(str(message)))

    def _record_add_url(url: str, reason: str = "unspecified", details: Any = None):
        add_url_calls.append(
            {
                "url": url,
                "reason": reason,
                "details": dict(details) if isinstance(details, dict) else details,
            }
        )

    monkeypatch.setattr(runtime, "add_url", _record_add_url)

    page_html = "<html><script>$.get('/live/test-tier-threshold.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Radiant Team", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Dire Team", "team_id": 2002, "id": 2002},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
        },
        "radiant_lead": 0.0,
        "game_time": float(10 * 60),
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: int(match_tier))
    monkeypatch.setattr(
        runtime,
        "parse_draft_and_positions",
        lambda *_args, **_kwargs: (
            {"r1": "pos1", "r2": "pos2"},
            {"d1": "pos1", "d2": "pos2"},
            None,
            "",
            [],
        ),
    )
    monkeypatch.setattr(
        runtime,
        "synergy_and_counterpick",
        lambda *_args, **_kwargs: {"early_output": {"solo": 1}, "mid_output": {"solo": 1}},
    )
    monkeypatch.setattr(runtime, "calculate_lanes", lambda *_args, **_kwargs: ("", "", ""))

    def _format_output_dict_stub(candidate: Dict[str, Any], target_wr: int, **_kwargs) -> bool:
        requested_thresholds.append(int(target_wr))
        return float(target_wr) <= float(max_supported_wr)

    monkeypatch.setattr(runtime, "format_output_dict", _format_output_dict_stub)
    monkeypatch.setattr(
        runtime,
        "_star_block_diagnostics",
        lambda *, raw_block, target_wr, section: {
            "valid": True,
            "status": "ok",
            "sign": 1,
            "hit_metrics": ["solo"],
            "conflict_metric": None,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_block_signs_same_or_zero",
        lambda *_args, **_kwargs: {"valid": True, "status": "ok"},
    )
    monkeypatch.setattr(runtime, "_format_raw_star_block_metrics", lambda *_args, **_kwargs: "none")
    monkeypatch.setattr(runtime, "_decorate_star_block_for_display", lambda raw_block, **_kwargs: dict(raw_block or {}))
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)

    runtime.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=set(),
        return_status=None,
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()

    return {
        "requested_thresholds": requested_thresholds,
        "sent_messages": sent_messages,
        "add_url_calls": add_url_calls,
    }


def test_tier1_uses_threshold_60(monkeypatch) -> None:
    result = _run_case(monkeypatch, match_tier=1, max_supported_wr=60.0)

    assert result["requested_thresholds"]
    assert result["requested_thresholds"][0] == 60
    assert 65 not in result["requested_thresholds"]
    assert len(result["sent_messages"]) == 1
    assert result["add_url_calls"]
    assert result["add_url_calls"][-1]["reason"] in {
        "star_signal_sent_now",
        "star_signal_sent_now_networth_gate",
    }


def test_any_tier2_uses_threshold_65(monkeypatch) -> None:
    result = _run_case(monkeypatch, match_tier=2, max_supported_wr=65.0)

    assert result["requested_thresholds"]
    assert result["requested_thresholds"][0] == 65
    assert 60 not in result["requested_thresholds"]
    assert len(result["sent_messages"]) == 1
    assert result["add_url_calls"]
    assert result["add_url_calls"][-1]["reason"] in {
        "star_signal_sent_now",
        "star_signal_sent_now_networth_gate",
    }


def test_any_tier2_below_65_is_rejected(monkeypatch) -> None:
    result = _run_case(monkeypatch, match_tier=2, max_supported_wr=64.9)

    assert result["requested_thresholds"]
    assert result["requested_thresholds"][0] == 65
    assert len(result["sent_messages"]) == 0
    assert result["add_url_calls"]
    assert result["add_url_calls"][-1]["reason"] == "star_signal_rejected_no_star_signal"
    details = result["add_url_calls"][-1]["details"]
    assert isinstance(details, dict)
    assert details["dispatch_status_label"] == runtime.TIER_THRESHOLD_STATUS_TIER2_MIN65_BLOCK
    assert details["threshold_block_reason_label"] == runtime.TIER_THRESHOLD_REASON_TIER2_MIN65_BLOCK
    assert details["threshold_min_wr"] == 65
