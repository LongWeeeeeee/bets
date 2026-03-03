from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


REQUIRED_STATUS_CONSTS = (
    "NETWORTH_STATUS_PRE4_BLOCK",
    "NETWORTH_STATUS_4_10_SEND_800",
    "NETWORTH_STATUS_MIN10_LOSS_LE1500_SEND",
    "NETWORTH_STATUS_LATE_MONITOR_WAIT_1000",
    "NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000",
    "NETWORTH_STATUS_LATE_FALLBACK_21_SEND",
)

if not all(hasattr(runtime, attr) for attr in REQUIRED_STATUS_CONSTS):
    pytestmark = pytest.mark.skip(
        reason=(
            "Runtime networth-gated status labels are not available in base/cyberscore_try.py "
            "(dependency task b205acba)."
        )
    )


@dataclass(frozen=True)
class BranchScenario:
    name: str
    game_time_seconds: int
    target_side: str
    target_networth_diff: int
    has_early_star: bool
    early_sign: int
    has_late_star: bool
    late_sign: int
    expected_send_calls: int
    expected_wait_token: Optional[str] = None
    expected_add_url_reason: Optional[str] = None
    expected_release_reason: Optional[str] = None
    expected_queue: bool = False
    expected_monitor_threshold: Optional[float] = None


@dataclass
class BranchResult:
    sent_messages: List[str]
    add_url_calls: List[Dict[str, Any]]
    queued_payload: Optional[Dict[str, Any]]


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
      <a href="https://dltv.org/matches/test-match"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None
    return [head], [body]


def _radiant_lead_for_target(target_side: str, target_diff: float) -> float:
    return float(target_diff) if target_side == "radiant" else -float(target_diff)


def _star_diagnostics_for_case(
    case: BranchScenario,
    section: str,
) -> Dict[str, Any]:
    if section == "early_output":
        if case.has_early_star:
            return {
                "valid": True,
                "status": "ok",
                "sign": case.early_sign,
                "hit_metrics": ["solo"],
                "conflict_metric": None,
            }
        return {
            "valid": False,
            "status": "no_hits",
            "sign": None,
            "hit_metrics": [],
            "conflict_metric": None,
        }
    if section == "mid_output":
        if case.has_late_star:
            return {
                "valid": True,
                "status": "ok",
                "sign": case.late_sign,
                "hit_metrics": ["solo"],
                "conflict_metric": None,
            }
        return {
            "valid": False,
            "status": "no_hits",
            "sign": None,
            "hit_metrics": [],
            "conflict_metric": None,
        }
    return {
        "valid": False,
        "status": "no_hits",
        "sign": None,
        "hit_metrics": [],
        "conflict_metric": None,
    }


def _run_branch_scenario(monkeypatch, case: BranchScenario) -> BranchResult:
    heads, bodies = _build_heads_and_bodies()
    sent_messages: List[str] = []
    add_url_calls: List[Dict[str, Any]] = []

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
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

    page_html = "<html><script>$.get('/live/test.json')</script></html>"
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
        "radiant_lead": _radiant_lead_for_target(case.target_side, case.target_networth_diff),
        "game_time": float(case.game_time_seconds),
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
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)
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
        lambda *_args, **_kwargs: {"early_output": {"solo": 0}, "mid_output": {"solo": 0}},
    )
    monkeypatch.setattr(runtime, "calculate_lanes", lambda *_args, **_kwargs: ("", "", ""))
    monkeypatch.setattr(runtime, "format_output_dict", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        runtime,
        "_star_block_diagnostics",
        lambda *, raw_block, target_wr, section: _star_diagnostics_for_case(case, section),
    )
    monkeypatch.setattr(
        runtime,
        "_block_signs_same_or_zero",
        lambda *_args, **_kwargs: {"valid": False, "status": "conflict_signs"},
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

    queued_payload: Optional[Dict[str, Any]] = None
    with runtime.monitored_matches_lock:
        if runtime.monitored_matches:
            queued_payload = dict(next(iter(runtime.monitored_matches.values())))
        runtime.monitored_matches.clear()

    return BranchResult(
        sent_messages=sent_messages,
        add_url_calls=add_url_calls,
        queued_payload=queued_payload,
    )


def test_networth_status_labels_are_pinned() -> None:
    assert runtime.NETWORTH_STATUS_PRE4_BLOCK == "pre4_block"
    assert runtime.NETWORTH_STATUS_4_10_SEND_800 == "4_10_send_800"
    assert runtime.NETWORTH_STATUS_MIN10_LOSS_LE1500_SEND == "minute10_loss_le1500_send"
    assert runtime.NETWORTH_STATUS_LATE_MONITOR_WAIT_1000 == "late_monitor_wait_1000"
    assert runtime.NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000 == "late_conflict_wait_3000"
    assert runtime.NETWORTH_STATUS_LATE_FALLBACK_21_SEND == "late_fallback_21_send"


SCENARIOS = (
    BranchScenario(
        name="pre4_block_wait",
        game_time_seconds=(3 * 60) + 30,
        target_side="radiant",
        target_networth_diff=5000,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        expected_wait_token="pre4_block",
    ),
    BranchScenario(
        name="send_4_10_at_800",
        game_time_seconds=6 * 60,
        target_side="radiant",
        target_networth_diff=800,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now_networth_gate",
        expected_release_reason="4_10_send_800",
    ),
    BranchScenario(
        name="minute10_loss_le1500_send",
        game_time_seconds=10 * 60,
        target_side="radiant",
        target_networth_diff=-1500,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
    ),
    BranchScenario(
        name="late_monitor_wait_1000",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=999,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        expected_queue=True,
        expected_monitor_threshold=1000.0,
    ),
    BranchScenario(
        name="late_conflict_wait_3000",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=2999,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        expected_queue=True,
        expected_monitor_threshold=3000.0,
    ),
    BranchScenario(
        name="late_fallback_21_send",
        game_time_seconds=21 * 60,
        target_side="radiant",
        target_networth_diff=100,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now_target_reached",
    ),
)


@pytest.mark.parametrize("case", SCENARIOS, ids=[case.name for case in SCENARIOS])
def test_networth_dispatch_branches(monkeypatch, capsys, case: BranchScenario) -> None:
    result = _run_branch_scenario(monkeypatch, case)
    output = capsys.readouterr().out

    assert len(result.sent_messages) == case.expected_send_calls
    if case.expected_wait_token is not None:
        assert case.expected_wait_token in output

    if case.expected_add_url_reason is not None:
        assert result.add_url_calls, "Expected add_url call but got none"
        assert result.add_url_calls[-1]["reason"] == case.expected_add_url_reason

    if case.expected_release_reason is not None:
        assert result.add_url_calls, "Expected add_url details with release_reason"
        details = result.add_url_calls[-1]["details"]
        assert isinstance(details, dict)
        assert details.get("release_reason") == case.expected_release_reason

    if case.expected_queue:
        assert result.queued_payload is not None, "Expected delayed queue payload"
        if case.expected_monitor_threshold is not None:
            assert float(result.queued_payload.get("networth_monitor_threshold")) == case.expected_monitor_threshold
    else:
        assert result.queued_payload is None
