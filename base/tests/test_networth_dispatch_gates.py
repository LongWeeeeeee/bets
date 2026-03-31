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
    "NETWORTH_STATUS_MIN10_LEAD_GE800_SEND",
    "NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_1500",
    "NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_800",
    "NETWORTH_STATUS_EARLY_CORE_FALLBACK_20_20_SEND",
    "NETWORTH_STATUS_EARLY_CORE_TIMEOUT_NO_SEND",
    "NETWORTH_STATUS_LATE_CORE_MONITOR_WAIT_800",
    "NETWORTH_STATUS_LATE_CORE_TIMEOUT_NO_SEND",
    "NETWORTH_STATUS_LATE_MONITOR_WAIT_1500",
    "NETWORTH_STATUS_LATE_CONFLICT_WAIT_1500",
    "NETWORTH_STATUS_LATE_CONFLICT_WAIT_2000",
    "NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000",
    "NETWORTH_STATUS_LATE_FALLBACK_20_20_SEND",
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
    raw_early_output: Optional[Dict[str, Any]] = None
    raw_mid_output: Optional[Dict[str, Any]] = None


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


def _valid_heroes(seed: int) -> Dict[str, Dict[str, int]]:
    return {
        "pos1": {"hero_id": seed + 1, "account_id": seed + 101},
        "pos2": {"hero_id": seed + 2, "account_id": seed + 102},
        "pos3": {"hero_id": seed + 3, "account_id": seed + 103},
        "pos4": {"hero_id": seed + 4, "account_id": seed + 104},
        "pos5": {"hero_id": seed + 5, "account_id": seed + 105},
    }


def _radiant_lead_for_target(target_side: str, target_diff: float) -> float:
    return float(target_diff) if target_side == "radiant" else -float(target_diff)


def _patch_team_elo_summary(
    monkeypatch,
    *,
    radiant_wr: float,
    dire_wr: float,
    radiant_rank: Optional[int] = None,
    dire_rank: Optional[int] = None,
) -> None:
    radiant_rating = 1500.0 + (radiant_wr - 50.0) * 4.0
    dire_rating = 1500.0 + (dire_wr - 50.0) * 4.0

    monkeypatch.setattr(
        runtime,
        "_build_team_elo_matchup_summary",
        lambda *_args, **_kwargs: {
            "radiant": {
                "rating": radiant_rating,
                "base_rating": radiant_rating,
                "lineup_used": True,
                "leaderboard_rank": radiant_rank,
            },
            "dire": {
                "rating": dire_rating,
                "base_rating": dire_rating,
                "lineup_used": True,
                "leaderboard_rank": dire_rank,
            },
            "radiant_win_prob": float(radiant_wr) / 100.0,
            "dire_win_prob": float(dire_wr) / 100.0,
            "elo_diff": float(radiant_rating - dire_rating),
            "source": "test",
        },
    )


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


def _run_branch_scenario(
    monkeypatch,
    case: BranchScenario,
    match_tier: int = 1,
    allow_early_star_late_core_same_or_zero: bool = False,
    allow_late_star_early_core_same_or_zero: bool = False,
    existing_delayed_payload: Optional[Dict[str, Any]] = None,
    lane_output: tuple[str, str, str] = ("", "", ""),
) -> BranchResult:
    heads, bodies = _build_heads_and_bodies()
    sent_messages: List[str] = []
    add_url_calls: List[Dict[str, Any]] = []

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
        if existing_delayed_payload is not None:
            runtime.monitored_matches["dltv.org/matches/test-match.0"] = dict(existing_delayed_payload)

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(
        runtime,
        "STAR_ALLOW_TIER1_EARLY_STAR_LATE_SAME_OR_ZERO",
        allow_early_star_late_core_same_or_zero,
        raising=False,
    )
    monkeypatch.setattr(
        runtime,
        "STAR_ALLOW_LATE_STAR_EARLY_SAME_OR_ZERO",
        allow_late_star_early_core_same_or_zero,
        raising=False,
    )

    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)

    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: sent_messages.append(str(message)))

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
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: int(match_tier))
    monkeypatch.setattr(
        runtime,
        "parse_draft_and_positions",
        lambda *_args, **_kwargs: (
            _valid_heroes(0),
            _valid_heroes(100),
            None,
            "",
            [],
        ),
    )
    monkeypatch.setattr(
        runtime,
        "synergy_and_counterpick",
        lambda *_args, **_kwargs: {
            "early_output": dict(case.raw_early_output or {"solo": 0}),
            "mid_output": dict(case.raw_mid_output or {"solo": 0}),
        },
    )
    monkeypatch.setattr(runtime, "calculate_lanes", lambda *_args, **_kwargs: lane_output)
    monkeypatch.setattr(runtime, "format_output_dict", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        runtime,
        "_star_block_diagnostics",
        lambda *, raw_block, target_wr, section: _star_diagnostics_for_case(case, section),
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
    assert runtime.NETWORTH_STATUS_MIN10_LEAD_GE800_SEND == "minute10_lead_ge800_send"
    assert runtime.NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_1500 == "early_core_monitor_wait_1500"
    assert runtime.NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_800 == "early_core_monitor_wait_800"
    assert runtime.NETWORTH_STATUS_EARLY_CORE_FALLBACK_20_20_SEND == "early_core_fallback_20_20_send"
    assert runtime.NETWORTH_STATUS_LATE_CORE_MONITOR_WAIT_800 == "late_core_monitor_wait_800"
    assert runtime.NETWORTH_STATUS_LATE_CORE_TIMEOUT_NO_SEND == "late_core_timeout_no_send"
    assert runtime.NETWORTH_STATUS_LATE_MONITOR_WAIT_1500 == "late_monitor_wait_1500"
    assert runtime.NETWORTH_STATUS_LATE_CONFLICT_WAIT_1500 == "late_conflict_wait_1500"
    assert runtime.NETWORTH_STATUS_LATE_CONFLICT_WAIT_2000 == "late_conflict_wait_2000"
    assert runtime.NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000 == "late_conflict_wait_3000"
    assert runtime.NETWORTH_STATUS_LATE_FALLBACK_20_20_SEND == "late_fallback_20_20_send"


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
        name="full_star_4_10_at_800",
        game_time_seconds=6 * 60,
        target_side="radiant",
        target_networth_diff=800,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now_networth_gate",
        expected_release_reason="4_10_send_800",
        raw_early_output={"counterpick_1vs1": 1},
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
        name="late_monitor_wait_1500",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=1499,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        expected_queue=True,
        expected_monitor_threshold=1500.0,
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
        name="late_fallback_20_20_send",
        game_time_seconds=(20 * 60) + 20,
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


def test_late_only_can_be_rejected_when_early_is_required(monkeypatch) -> None:
    case = BranchScenario(
        name="late_only_rejected_when_early_required",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=1200,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
    )
    monkeypatch.setattr(runtime, "STAR_REQUIRE_EARLY_WITH_LATE_SAME_SIGN", True, raising=False)
    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_rejected_no_star_signal"
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert "late_star_requires_early_same_sign" in details["star_filter_rejections"][0]


def test_opposite_signs_can_be_rejected_when_delay_is_disabled(monkeypatch) -> None:
    case = BranchScenario(
        name="opposite_signs_rejected_when_delay_disabled",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=3200,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
    )
    monkeypatch.setattr(runtime, "STAR_DELAY_ON_OPPOSITE_SIGNS", False, raising=False)
    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_rejected_no_star_signal"
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert "opposite_signs_disabled" in details["star_filter_rejections"][0]


def test_opposite_signs_wait_until_20_20_when_early_wr_between_66_and_89(monkeypatch) -> None:
    case = BranchScenario(
        name="opposite_signs_wait_until_20_20_when_early_wr_between_66_and_89",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=3200,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"counterpick_1vs1": 9, "solo": 5, "synergy_trio": 18},
        raw_mid_output={"counterpick_1vs1": -11, "solo": -6, "synergy_trio": -10},
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: {
            "level": 80,
            "min_odds": 1.10,
            "wr_pct": 80.0,
        },
    )

    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.add_url_calls == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_only_opposite_signs"
    assert result.queued_payload["send_on_target_game_time"] is True
    assert result.queued_payload.get("networth_monitor_threshold") is None
    assert float(result.queued_payload["fallback_max_deficit_abs"]) == 3000.0
    assert result.queued_payload["networth_target_side"] == "radiant"
    queued_message = str(result.queued_payload.get("message") or "")
    assert queued_message.startswith("СТАВКА НА radiant x1\n")
    add_url_details = result.queued_payload.get("add_url_details") or {}
    assert float(add_url_details["early_wr_pct"]) == 80.0
    assert float(add_url_details["fallback_max_deficit_abs"]) == 3000.0


def test_opposite_signs_early90_uses_2000_in_4_10_window(monkeypatch) -> None:
    case = BranchScenario(
        name="opposite_signs_early90_uses_2000_in_4_10_window",
        game_time_seconds=7 * 60,
        target_side="radiant",
        target_networth_diff=1800,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"counterpick_1vs1": 9, "solo": 5, "synergy_trio": 18},
        raw_mid_output={"counterpick_1vs1": -11, "solo": -6, "synergy_trio": -10},
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, _phase: {
            "level": 90,
            "min_odds": 1.10,
            "wr_pct": 90.0,
        },
    )
    _patch_team_elo_summary(monkeypatch, radiant_wr=57.0, dire_wr=43.0)

    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.add_url_calls == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_only_opposite_signs"
    assert float(result.queued_payload["networth_monitor_threshold"]) == 2000.0
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_CONFLICT_WAIT_2000
    assert result.queued_payload["dynamic_monitor_profile"] == "late_only_opposite_signs_early90"


def test_opposite_signs_early90_underdog_gap_uses_1500_after_10(monkeypatch) -> None:
    case = BranchScenario(
        name="opposite_signs_early90_underdog_gap_uses_1500_after_10",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=1600,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        raw_early_output={"counterpick_1vs1": 9, "solo": 5, "synergy_trio": 18},
        raw_mid_output={"counterpick_1vs1": -11, "solo": -6, "synergy_trio": -10},
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, _phase: {
            "level": 90,
            "min_odds": 1.10,
            "wr_pct": 90.0,
        },
    )
    _patch_team_elo_summary(monkeypatch, radiant_wr=62.0, dire_wr=38.0)

    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.add_url_calls == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_only_opposite_signs"
    assert float(result.queued_payload["networth_monitor_threshold"]) == 1500.0
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_CONFLICT_WAIT_1500
    assert float(result.queued_payload["networth_monitor_hold_seconds"]) == 60.0
    assert float(result.queued_payload["networth_monitor_hold_started_game_time"]) == float(case.game_time_seconds)


def test_opposite_signs_early90_close_elo_uses_3000_after_10(monkeypatch) -> None:
    case = BranchScenario(
        name="opposite_signs_early90_close_elo_uses_3000_after_10",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=2500,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"counterpick_1vs1": 9, "solo": 5, "synergy_trio": 18},
        raw_mid_output={"counterpick_1vs1": -11, "solo": -6, "synergy_trio": -10},
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, _phase: {
            "level": 90,
            "min_odds": 1.10,
            "wr_pct": 90.0,
        },
    )
    _patch_team_elo_summary(monkeypatch, radiant_wr=57.0, dire_wr=43.0)

    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.add_url_calls == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_only_opposite_signs"
    assert float(result.queued_payload["networth_monitor_threshold"]) == 3000.0
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000


def test_late_fallback_at_20_20_uses_comeback_ceiling(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "late_comeback_ceiling_thresholds", {"20": 13500.0, "21": 13698.0}, raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_max_minute", 21, raising=False)
    case = BranchScenario(
        name="late_fallback_20_20_uses_comeback_ceiling",
        game_time_seconds=(20 * 60) + 20,
        target_side="radiant",
        target_networth_diff=-1600,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
    )

    result = _run_branch_scenario(monkeypatch, case)

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now_late_comeback_ceiling"
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT
    assert details["target_side"] == "radiant"
    assert details["target_networth_diff"] == pytest.approx(-1600.0)
    assert details["late_comeback_monitor_minute"] == 20
    assert details["late_comeback_monitor_threshold"] == pytest.approx(13500.0)


def test_opposite_signs_can_release_at_3000_when_early_wr_is_65(monkeypatch) -> None:
    case = BranchScenario(
        name="opposite_signs_can_release_at_3000_when_early_wr_is_65",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=3200,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        raw_early_output={"counterpick_1vs1": 4, "solo": 3},
        raw_mid_output={"counterpick_1vs1": -11, "solo": -6, "synergy_trio": -10},
    )

    def _recommend_odds(_data, phase):
        if phase == "early":
            return {"level": 65, "min_odds": 1.54, "wr_pct": 65.0}
        return {"level": 90, "min_odds": 1.11, "wr_pct": 90.0}

    monkeypatch.setattr(runtime, "_recommend_odds_for_block", _recommend_odds)

    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.add_url_calls == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_only_opposite_signs"
    assert float(result.queued_payload["networth_monitor_threshold"]) == 3000.0
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000
    assert float(result.queued_payload["networth_monitor_hold_seconds"]) == 60.0
    assert float(result.queued_payload["networth_monitor_hold_started_game_time"]) == float(case.game_time_seconds)


def test_tier2_can_require_same_sign(monkeypatch) -> None:
    case = BranchScenario(
        name="tier2_requires_same_sign",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=1200,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
    )
    monkeypatch.setattr(runtime, "STAR_REQUIRE_TIER2_SAME_SIGN", True, raising=False)
    result = _run_branch_scenario(monkeypatch, case, match_tier=2)

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_rejected_no_early_star"
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["selected_early_star"] is False
    assert details["selected_late_star"] is True


def test_early_star_and_late_core_same_sign_can_send_without_late_star(monkeypatch) -> None:
    case = BranchScenario(
        name="early_star_late_core_same_sign_send",
        game_time_seconds=(9 * 60) + 30,
        target_side="radiant",
        target_networth_diff=800,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
        raw_early_output={"solo": 3},
        raw_mid_output={
            "counterpick_1vs1": 1,
            "solo": 2,
            "synergy_duo": -10,
            "synergy_trio": None,
        },
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        match_tier=2,
        allow_early_star_late_core_same_or_zero=True,
    )

    assert len(result.sent_messages) == 1
    assert result.add_url_calls
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["dispatch_mode"] == "immediate_early_star_late_core_same_sign"


def test_early_star_and_late_pos1_vs_pos1_same_sign_can_send_without_late_star(monkeypatch) -> None:
    case = BranchScenario(
        name="early_star_late_pos1_same_sign_send",
        game_time_seconds=(9 * 60) + 30,
        target_side="radiant",
        target_networth_diff=800,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
        raw_early_output={"solo": 3},
        raw_mid_output={
            "pos1_vs_pos1": 2,
            "synergy_duo": -10,
            "synergy_trio": None,
        },
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        match_tier=2,
        allow_early_star_late_core_same_or_zero=True,
    )

    assert len(result.sent_messages) == 1
    assert result.add_url_calls
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["dispatch_mode"] == "immediate_early_star_late_core_same_sign"


def test_early_star_without_late_star_can_send_even_if_late_core_conflicts(monkeypatch) -> None:
    case = BranchScenario(
        name="early_star_no_late_star_send",
        game_time_seconds=10 * 60,
        target_side="radiant",
        target_networth_diff=0,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
        raw_early_output={"solo": 3},
        raw_mid_output={
            "counterpick_1vs1": -1,
            "counterpick_1vs2": 0,
            "solo": 2,
        },
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        match_tier=2,
        allow_early_star_late_core_same_or_zero=True,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["dispatch_mode"] == "immediate_early_star_no_late_star"


def test_early_star_without_late_star_after_10min_waits_below_minus1500_without_queue(monkeypatch, capsys) -> None:
    case = BranchScenario(
        name="early_star_no_late_star_10plus_wait",
        game_time_seconds=10 * 60,
        target_side="radiant",
        target_networth_diff=-1501,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"solo": 3},
        raw_mid_output={
            "counterpick_1vs1": -1,
            "counterpick_1vs2": 0,
            "solo": 2,
        },
    )
    result = _run_branch_scenario(monkeypatch, case)
    output = capsys.readouterr().out

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls == []
    assert "networth_gate_10plus_loss_le1500" in output


def test_early_star_late_core_same_sign_after_10min_uses_minus1500_gate(monkeypatch) -> None:
    case = BranchScenario(
        name="early_star_late_core_same_sign_10plus_send",
        game_time_seconds=10 * 60,
        target_side="radiant",
        target_networth_diff=-1500,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
        raw_early_output={"solo": 3},
        raw_mid_output={
            "counterpick_1vs1": 1,
            "solo": 2,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 70, "min_odds": 1.43, "wr_pct": 70.0}
            if phase == "early"
            else None
        ),
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        match_tier=2,
        allow_early_star_late_core_same_or_zero=True,
    )

    assert len(result.sent_messages) == 1
    assert result.add_url_calls
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_MIN10_LOSS_LE1500_SEND


def test_early_star_late_core_same_sign_after_10min_waits_below_minus1500(monkeypatch, capsys) -> None:
    case = BranchScenario(
        name="early_star_late_core_same_sign_10plus_wait",
        game_time_seconds=10 * 60,
        target_side="radiant",
        target_networth_diff=-1501,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"solo": 3},
        raw_mid_output={
            "counterpick_1vs1": 1,
            "solo": 2,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 70, "min_odds": 1.43, "wr_pct": 70.0}
            if phase == "early"
            else None
        ),
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        match_tier=2,
        allow_early_star_late_core_same_or_zero=True,
    )
    output = capsys.readouterr().out

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert float(result.queued_payload["networth_monitor_threshold"]) == runtime.NETWORTH_GATE_EARLY_CORE_MONITOR_DIFF
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_1500
    assert result.queued_payload["fallback_send_status_label"] == runtime.NETWORTH_STATUS_EARLY_CORE_FALLBACK_20_20_SEND
    assert result.queued_payload["send_on_target_game_time"] is False
    assert result.queued_payload["allow_live_recheck"] is True
    assert result.add_url_calls == []
    assert "delayed monitor >=1500 until 20:20" in output


def test_early_star_late_core_same_sign_wr60_70_after_10min_requires_plus800(monkeypatch, capsys) -> None:
    case = BranchScenario(
        name="early_star_late_core_same_sign_10plus_wr60_70_wait_800",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=799,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"solo": 3},
        raw_mid_output={
            "counterpick_1vs1": 1,
            "solo": 2,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 60, "min_odds": 1.67, "wr_pct": 60.0}
            if phase == "early"
            else None
        ),
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        match_tier=2,
        allow_early_star_late_core_same_or_zero=True,
    )
    output = capsys.readouterr().out

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert float(result.queued_payload["networth_monitor_threshold"]) == runtime.NETWORTH_GATE_EARLY_CORE_LOW_WR_MIN_LEAD
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_800
    assert result.queued_payload["reason"] == "early_star_late_core_low_wr_wait_800"
    assert result.queued_payload["send_on_target_game_time"] is False
    assert result.queued_payload["allow_live_recheck"] is True
    assert "need>=800" in output


def test_early_star_late_core_same_sign_wr60_70_after_10min_sends_at_plus800(monkeypatch) -> None:
    case = BranchScenario(
        name="early_star_late_core_same_sign_10plus_wr60_70_send_800",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=800,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
        raw_early_output={"solo": 3},
        raw_mid_output={
            "counterpick_1vs1": 1,
            "solo": 2,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 60, "min_odds": 1.67, "wr_pct": 60.0}
            if phase == "early"
            else None
        ),
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        match_tier=2,
        allow_early_star_late_core_same_or_zero=True,
    )

    assert len(result.sent_messages) == 1
    assert result.add_url_calls
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_MIN10_LEAD_GE800_SEND


def test_early_star_late_core_same_sign_at_20_20_rejects_without_send(monkeypatch) -> None:
    case = BranchScenario(
        name="early_star_late_core_same_sign_20_20_timeout_reject",
        game_time_seconds=(20 * 60) + 20,
        target_side="radiant",
        target_networth_diff=-2000,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=0,
        expected_add_url_reason="star_signal_rejected_early_core_monitor_timeout",
        raw_early_output={"solo": 3},
        raw_mid_output={
            "counterpick_1vs1": 1,
            "solo": 2,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 70, "min_odds": 1.43, "wr_pct": 70.0}
            if phase == "early"
            else None
        ),
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        allow_early_star_late_core_same_or_zero=True,
    )

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_EARLY_CORE_TIMEOUT_NO_SEND


def test_lane_summary_is_logged_even_on_reject(monkeypatch, capsys) -> None:
    case = BranchScenario(
        name="lane_summary_logged_on_no_late_star_reject",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=0,
        has_early_star=False,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=0,
    )

    result = _run_branch_scenario(
        monkeypatch,
        case,
        lane_output=("Top: lose 47%\n", "Bot: win 65%\n", "Mid: lose 46%\n"),
    )
    output = capsys.readouterr().out

    assert result.sent_messages == []
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_rejected_no_early_star"
    assert "🛣️ Lanes:" in output
    assert "Top: lose 47%" in output
    assert "Mid: lose 46%" in output
    assert "Bot: win 65%" in output


def test_early_star_late_core_monitor_does_not_block_live_recheck(monkeypatch, capsys) -> None:
    case = BranchScenario(
        name="early_star_late_core_live_recheck_send",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=-1000,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
        raw_early_output={"solo": 3},
        raw_mid_output={
            "counterpick_1vs1": 1,
            "solo": 2,
        },
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        allow_early_star_late_core_same_or_zero=True,
        existing_delayed_payload={
            "reason": "early_star_late_core_wait_1500",
            "dispatch_status_label": runtime.NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_1500,
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_game_time": 10 * 60,
            "last_game_time": 10 * 60,
            "allow_live_recheck": True,
            "networth_monitor_threshold": float(runtime.NETWORTH_GATE_EARLY_CORE_MONITOR_DIFF),
            "networth_target_side": "radiant",
        },
    )
    output = capsys.readouterr().out

    assert len(result.sent_messages) == 1
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"
    assert "продолжаем live recheck" in output


def test_tier1_early65_without_late_star_is_rejected(monkeypatch) -> None:
    case = BranchScenario(
        name="early_star_late_core_zero_send",
        game_time_seconds=10 * 60,
        target_side="radiant",
        target_networth_diff=0,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"solo": 3},
        raw_mid_output={
            "counterpick_1vs1": 1,
            "counterpick_1vs2": 0,
            "solo": 2,
        },
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        allow_early_star_late_core_same_or_zero=True,
    )

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_rejected_no_early_star"
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["selected_early_star"] is True
    assert details["selected_late_star"] is False


def test_late_star_early_core_same_sign_after_10min_waits_below_plus800(monkeypatch, capsys) -> None:
    case = BranchScenario(
        name="late_star_early_core_same_sign_10plus_wait_800",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=799,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={
            "counterpick_1vs1": 1,
            "solo": 2,
        },
        raw_mid_output={"solo": 3},
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        match_tier=2,
        allow_late_star_early_core_same_or_zero=True,
    )
    output = capsys.readouterr().out

    assert result.sent_messages == []
    assert result.add_url_calls == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_star_early_core_wait_800"
    assert float(result.queued_payload["networth_monitor_threshold"]) == 800.0
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_CORE_MONITOR_WAIT_800
    assert "нет early star-сигнала" not in output


def test_late_star_early_core_same_sign_after_10min_sends_at_plus800(monkeypatch) -> None:
    case = BranchScenario(
        name="late_star_early_core_same_sign_10plus_send_800",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=800,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
        raw_early_output={
            "counterpick_1vs1": 1,
            "solo": 2,
        },
        raw_mid_output={"solo": 3},
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        match_tier=2,
        allow_late_star_early_core_same_or_zero=True,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["dispatch_mode"] == "immediate_late_star_early_core_same_sign"


def test_late_star_early_core_same_sign_at_20_20_uses_comeback_ceiling(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "late_comeback_ceiling_thresholds", {"20": 13500.0, "21": 13698.0}, raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_max_minute", 21, raising=False)
    case = BranchScenario(
        name="late_star_early_core_same_sign_20_20_reject_without_early",
        game_time_seconds=(20 * 60) + 20,
        target_side="radiant",
        target_networth_diff=-5000,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={
            "counterpick_1vs1": 1,
            "solo": 2,
        },
        raw_mid_output={"solo": 3},
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        match_tier=2,
        allow_late_star_early_core_same_or_zero=True,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now_late_comeback_ceiling"
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT
    assert details["late_comeback_monitor_minute"] == 20
    assert details["late_comeback_monitor_threshold"] == pytest.approx(13500.0)


def test_late_only_signal_without_early_queues_monitor(monkeypatch) -> None:
    case = BranchScenario(
        name="late_only_reject_without_early",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=999,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        expected_queue=True,
        expected_monitor_threshold=1500.0,
        raw_early_output={
            "counterpick_1vs1": 9,
            "counterpick_1vs2": 0,
            "solo": 4,
            "synergy_duo": 6,
            "synergy_trio": 0,
        },
        raw_mid_output={
            "counterpick_1vs1": 9,
            "pos1_vs_pos1": 30,
            "counterpick_1vs2": 0,
            "solo": 4,
            "synergy_duo": 6,
            "synergy_trio": 0,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: {
            "level": 65 if phase == "early" else 90,
            "min_odds": 1.54 if phase == "early" else 1.11,
            "wr_pct": 65.0 if phase == "early" else 90.0,
        },
    )
    result = _run_branch_scenario(
        monkeypatch,
        case,
        lane_output=("Top: lose 66%\n", "Mid: draw 49%\n", "Bot: win 46%\n"),
    )

    assert result.sent_messages == []
    assert result.add_url_calls == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_only_no_early_star_wait_1500"
    assert float(result.queued_payload["networth_monitor_threshold"]) == 1500.0
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_MONITOR_WAIT_1500


def test_large_elo_gap_invalidates_late_underdog_block_below_60_after_penalty(monkeypatch) -> None:
    case = BranchScenario(
        name="late_only_underdog_rejected_by_elo_guard",
        game_time_seconds=12 * 60,
        target_side="dire",
        target_networth_diff=2000,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=-1,
        expected_send_calls=0,
        raw_mid_output={
            "counterpick_1vs1": -6,
            "solo": -3,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_build_team_elo_matchup_summary",
        lambda *_args, **_kwargs: {
            "radiant": {"rating": 1626.0, "base_rating": 1626.0},
            "dire": {"rating": 1481.0, "base_rating": 1481.0},
            "radiant_win_prob": 0.698,
            "dire_win_prob": 0.302,
            "elo_diff": 146.0,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 60, "min_odds": 1.67, "wr_pct": 60.0}
            if phase == "late"
            else None
        ),
    )

    result = _run_branch_scenario(monkeypatch, case, match_tier=2)

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_rejected_no_early_star"
    details = result.add_url_calls[-1]["details"]
    assert details["selected_early_star"] is False
    assert details["selected_late_star"] is True


def test_same_sign_late_star_invalidated_by_elo_falls_back_to_early_branch(monkeypatch) -> None:
    case = BranchScenario(
        name="same_sign_late_star_invalidated_by_elo_falls_back_to_early_branch",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=1200,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now_networth_gate",
        raw_early_output={
            "counterpick_1vs1": 4,
            "solo": 3,
        },
        raw_mid_output={
            "counterpick_1vs1": 5,
            "counterpick_1vs2": -8,
            "solo": 3,
        },
    )

    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 65, "min_odds": 1.54, "wr_pct": 65.0}
            if phase == "early"
            else {"level": 60, "min_odds": 1.67, "wr_pct": 60.0}
        ),
    )

    def _fake_elo_guard(*, diag, block_wr_pct, team_elo_meta):
        patched = dict(diag)
        patched["raw_valid"] = bool(diag.get("valid"))
        patched["raw_status"] = str(diag.get("status") or "")
        if float(block_wr_pct or 0.0) == 60.0 and bool(diag.get("valid")):
            patched["valid"] = False
            patched["status"] = "elo_wr_below_min60"
            patched["elo_wr_penalty_pp"] = 8.0
            patched["elo_adjusted_wr_pct"] = 52.0
            return patched
        patched["elo_wr_penalty_pp"] = 0.0
        patched["elo_adjusted_wr_pct"] = float(block_wr_pct or 0.0)
        return patched

    monkeypatch.setattr(runtime, "_apply_elo_block_wr_guard", _fake_elo_guard)

    result = _run_branch_scenario(monkeypatch, case, match_tier=2)

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls
    details = result.add_url_calls[-1]["details"]
    assert details["dispatch_mode"] == "immediate_early_star_late_core_same_sign"


def test_large_elo_gap_late_only_can_still_send_when_late_survives_elo_guard(monkeypatch) -> None:
    case = BranchScenario(
        name="late_only_underdog_survives_elo_block_guard",
        game_time_seconds=12 * 60,
        target_side="dire",
        target_networth_diff=2000,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=-1,
        expected_send_calls=0,
        raw_mid_output={
            "counterpick_1vs1": -6,
            "solo": -3,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_build_team_elo_matchup_summary",
        lambda *_args, **_kwargs: {
            "radiant": {"rating": 1644.0, "base_rating": 1644.0},
            "dire": {"rating": 1510.0, "base_rating": 1510.0},
            "radiant_win_prob": 0.684,
            "dire_win_prob": 0.316,
            "elo_diff": 134.0,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 90, "min_odds": 1.11, "wr_pct": 90.0}
            if phase == "late"
            else None
        ),
    )

    result = _run_branch_scenario(monkeypatch, case, match_tier=2)

    assert result.sent_messages == []
    assert result.add_url_calls == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_only_no_early_star_wait_1500"
    assert float(result.queued_payload["networth_monitor_threshold"]) == 1500.0
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_MONITOR_WAIT_1500
    assert float(result.queued_payload["networth_monitor_hold_seconds"]) == 60.0
    assert float(result.queued_payload["networth_monitor_hold_started_game_time"]) == float(case.game_time_seconds)


def test_delayed_monitor_requires_one_minute_hold_before_send(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)

    send_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "_deliver_and_persist_signal",
        lambda *args, **kwargs: (send_calls.append({"args": args, "kwargs": kwargs}) or True),
    )

    states = iter(
        [
            {"game_time": 12 * 60, "radiant_lead": 1600.0},
            {"game_time": (12 * 60) + 61, "radiant_lead": 1600.0},
        ]
    )
    monkeypatch.setattr(runtime, "_fetch_delayed_match_state", lambda _json_url: next(states))

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-hold.0",
        {
            "message": "payload",
            "reason": "late_only_no_early_star_wait_1500",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 11 * 60,
            "last_game_time": 11 * 60,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "12:00"},
            "dispatch_status_label": runtime.NETWORTH_STATUS_LATE_MONITOR_WAIT_1500,
            "networth_monitor_threshold": 1500.0,
            "networth_monitor_deadline_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "networth_target_side": "radiant",
            "networth_monitor_hold_seconds": 60.0,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()
    assert send_calls == []
    with runtime.monitored_matches_lock:
        payload_after_first_check = dict(runtime.monitored_matches["dltv.org/matches/test-hold.0"])
    assert float(payload_after_first_check["networth_monitor_hold_started_game_time"]) == float(12 * 60)

    runtime._drain_due_delayed_signals_once()
    assert len(send_calls) == 1
    details = send_calls[0]["kwargs"]["add_url_details"]
    assert details["networth_monitor_threshold"] == pytest.approx(1500.0)
    assert details["networth_monitor_hold_started_game_time"] == pytest.approx(float(12 * 60))
    assert details["networth_monitor_hold_elapsed_seconds"] >= 60.0


def test_top25_late_elo_block_opposite_signs_queues_instead_of_reject(monkeypatch) -> None:
    case = BranchScenario(
        name="top25_late_elo_block_opposite_signs_queues",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=1200,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"counterpick_1vs1": -6, "solo": -3},
        raw_mid_output={"counterpick_1vs1": 6, "pos1_vs_pos1": 24, "solo": 3},
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 65, "min_odds": 1.54, "wr_pct": 65.0}
            if phase == "early"
            else {"level": 70, "min_odds": 1.43, "wr_pct": 70.0}
        ),
    )
    _patch_team_elo_summary(
        monkeypatch,
        radiant_wr=31.2,
        dire_wr=68.8,
        radiant_rank=7,
        dire_rank=1,
    )

    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.add_url_calls == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_top25_elo_block_opposite_monitor"
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_WAIT
    assert result.queued_payload["send_on_target_game_time"] is False
    assert result.queued_payload["dynamic_monitor_profile"] == "late_top25_elo_block_opposite_monitor"
    assert result.queued_payload["networth_target_side"] == "radiant"
    assert result.queued_payload["top25_late_elo_block_rank"] == 7
    queued_message = str(result.queued_payload.get("message") or "")
    assert queued_message.startswith("СТАВКА НА radiant x1\n")
    add_url_details = result.queued_payload.get("add_url_details") or {}
    assert add_url_details["networth_target_side"] == "radiant"
    assert add_url_details["top25_late_elo_block_rank"] == 7


def test_top25_late_elo_block_no_early_hits_queues_instead_of_reject(monkeypatch) -> None:
    case = BranchScenario(
        name="top25_late_elo_block_no_early_hits_queues",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=900,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"counterpick_1vs1": 1},
        raw_mid_output={"counterpick_1vs1": 6, "pos1_vs_pos1": 24, "solo": 3},
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 70, "min_odds": 1.43, "wr_pct": 70.0}
            if phase == "late"
            else None
        ),
    )
    _patch_team_elo_summary(
        monkeypatch,
        radiant_wr=31.2,
        dire_wr=68.8,
        radiant_rank=7,
        dire_rank=1,
    )

    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.add_url_calls == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_top25_elo_block_opposite_monitor"
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_WAIT
    assert result.queued_payload["send_on_target_game_time"] is False
    assert result.queued_payload["dynamic_monitor_profile"] == "late_top25_elo_block_opposite_monitor"
    assert result.queued_payload["networth_target_side"] == "radiant"
    assert result.queued_payload["top25_late_elo_block_rank"] == 7
    add_url_details = result.queued_payload.get("add_url_details") or {}
    assert add_url_details["top25_late_elo_block_rank"] == 7


def test_top25_late_elo_block_delayed_sender_sends_if_target_side_leads_at_20_20(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)

    send_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "_deliver_and_persist_signal",
        lambda *args, **kwargs: (send_calls.append({"args": args, "kwargs": kwargs}) or True),
    )
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {
            "game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "radiant_lead": 250.0,
        },
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-top25.0",
        {
            "message": "payload",
            "reason": "late_top25_elo_block_opposite_monitor",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 12 * 60,
            "last_game_time": 12 * 60,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "20:20", "target_side": "radiant"},
            "dispatch_status_label": runtime.NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_WAIT,
            "networth_target_side": "radiant",
            "dynamic_monitor_profile": "late_top25_elo_block_opposite_monitor",
            "networth_monitor_threshold_17_to_20": 3000.0,
            "networth_monitor_status_17_to_20": runtime.NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_WAIT,
            "top25_late_elo_block_rank": 7,
            "send_on_target_game_time": False,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert len(send_calls) == 1
    kwargs = send_calls[0]["kwargs"]
    assert kwargs["add_url_reason"] == "star_signal_sent_now_top25_late_elo_block_target_lead"
    details = kwargs["add_url_details"]
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TARGET_LEAD_SEND
    assert details["top25_late_elo_block_target_lead"] is True
    assert details["target_networth_diff"] == pytest.approx(250.0)


def test_elo_underdog_guard_allows_signal_at_70_boundary() -> None:
    decision = runtime._elo_underdog_guard_decision(
        team_elo_meta={
            "adjusted_radiant_wr": 69.8,
            "adjusted_dire_wr": 30.2,
        },
        target_side="dire",
        signal_wr_pct=70.0,
    )

    assert isinstance(decision, dict)
    assert decision["reject"] is False
    assert decision["favorite_side"] == "radiant"
    assert decision["target_side"] == "dire"


def test_signal_wr_for_elo_guard_uses_target_side_block_on_opposite_signs() -> None:
    meta = runtime._resolve_signal_wr_for_elo_guard(
        target_side="dire",
        has_selected_early_star=True,
        has_selected_late_star=True,
        selected_early_sign=1,
        selected_late_sign=-1,
        early_wr_pct=90.0,
        late_wr_pct=60.0,
    )

    assert isinstance(meta, dict)
    assert meta["wr_pct"] == pytest.approx(60.0)
    assert meta["source"] == "late"
    assert meta["candidates"] == {"late": 60.0}


def test_signal_wr_for_elo_guard_keeps_same_side_best_wr_when_signs_match() -> None:
    meta = runtime._resolve_signal_wr_for_elo_guard(
        target_side="radiant",
        has_selected_early_star=True,
        has_selected_late_star=True,
        selected_early_sign=1,
        selected_late_sign=1,
        early_wr_pct=65.0,
        late_wr_pct=60.0,
    )

    assert isinstance(meta, dict)
    assert meta["wr_pct"] == pytest.approx(65.0)
    assert meta["source"] == "best_of_early_late"
    assert meta["candidates"] == {"early": 65.0, "late": 60.0}


def test_elo_block_wr_guard_penalizes_underdog_star_block() -> None:
    diag = runtime._apply_elo_block_wr_guard(
        diag={
            "valid": True,
            "status": "ok",
            "sign": -1,
            "hit_metrics": ["counterpick_1vs1", "solo"],
        },
        block_wr_pct=60.0,
        team_elo_meta={
            "adjusted_radiant_wr": 68.4,
            "adjusted_dire_wr": 31.6,
        },
    )

    assert diag["valid"] is False
    assert diag["status"] == "elo_wr_below_min60"
    assert diag["elo_target_side"] == "dire"
    assert diag["elo_wr_penalty_pp"] == pytest.approx(18.4)
    assert diag["elo_adjusted_wr_pct"] == pytest.approx(41.6)


def test_elo_block_wr_guard_keeps_high_wr_underdog_star_block() -> None:
    diag = runtime._apply_elo_block_wr_guard(
        diag={
            "valid": True,
            "status": "ok",
            "sign": -1,
            "hit_metrics": ["counterpick_1vs1", "solo"],
        },
        block_wr_pct=90.0,
        team_elo_meta={
            "adjusted_radiant_wr": 68.4,
            "adjusted_dire_wr": 31.6,
        },
    )

    assert diag["valid"] is True
    assert diag["status"] == "ok"
    assert diag["elo_wr_penalty_pp"] == pytest.approx(18.4)
    assert diag["elo_adjusted_wr_pct"] == pytest.approx(71.6)


def test_elo_block_wr_guard_does_not_penalize_favorite_star_block() -> None:
    diag = runtime._apply_elo_block_wr_guard(
        diag={
            "valid": True,
            "status": "ok",
            "sign": 1,
            "hit_metrics": ["counterpick_1vs1", "solo"],
        },
        block_wr_pct=60.0,
        team_elo_meta={
            "adjusted_radiant_wr": 68.4,
            "adjusted_dire_wr": 31.6,
        },
    )

    assert diag["valid"] is True
    assert diag["status"] == "ok"
    assert diag["elo_target_side"] == "radiant"
    assert diag["elo_wr_penalty_pp"] == pytest.approx(0.0)
    assert diag["elo_adjusted_wr_pct"] == pytest.approx(60.0)


def test_full_star_same_sign_message_keeps_early_block(monkeypatch) -> None:
    case = BranchScenario(
        name="full_star_same_sign_message_keeps_early",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=100,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
        raw_early_output={
            "counterpick_1vs1": 9,
            "counterpick_1vs2": 0,
            "solo": 4,
            "synergy_duo": 6,
            "synergy_trio": 0,
        },
        raw_mid_output={
            "counterpick_1vs1": 9,
            "counterpick_1vs2": 0,
            "solo": 4,
            "synergy_duo": 6,
            "synergy_trio": 0,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: {
            "level": 65 if phase == "early" else 90,
            "min_odds": 1.54 if phase == "early" else 1.11,
            "wr_pct": 65.0 if phase == "early" else 90.0,
        },
    )
    _patch_team_elo_summary(monkeypatch, radiant_wr=56.0, dire_wr=44.0)
    result = _run_branch_scenario(
        monkeypatch,
        case,
        lane_output=("Top: lose 66%\n", "Mid: draw 49%\n", "Bot: win 46%\n"),
    )

    assert len(result.sent_messages) == 1
    message = result.sent_messages[0]
    assert message.startswith("СТАВКА НА radiant x3\n")
    assert "ПОМНИ: КОМАНДА ВАЖНЕЕ ПИКА" not in message
    assert "Оценка WR:\nEarly: Radiant Team WR≈" in message
    assert "Late: Radiant Team WR≈" in message
    assert "10-28 Minute:" in message
    assert "Mid (25-50 min):" in message


def test_stake_multiplier_is_x2_for_early_same_sign_stronger_elo_lead(monkeypatch) -> None:
    case = BranchScenario(
        name="stake_multiplier_x2_early_same_sign",
        game_time_seconds=6 * 60,
        target_side="radiant",
        target_networth_diff=900,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now_networth_gate",
        expected_release_reason="4_10_send_800",
        raw_early_output={"counterpick_1vs1": 4, "solo": 3},
        raw_mid_output={"counterpick_1vs1": 4, "solo": 3},
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: {
            "level": 60,
            "min_odds": 1.80,
            "wr_pct": 60.0 if phase == "early" else 62.0,
        },
    )
    _patch_team_elo_summary(monkeypatch, radiant_wr=56.0, dire_wr=44.0)

    result = _run_branch_scenario(monkeypatch, case)

    assert len(result.sent_messages) == 1
    assert result.sent_messages[0].startswith("СТАВКА НА radiant x2\n")


def test_stake_multiplier_is_x2_for_late_same_sign_stronger_elo_lead(monkeypatch) -> None:
    case = BranchScenario(
        name="stake_multiplier_x2_late_same_sign",
        game_time_seconds=12 * 60,
        target_side="radiant",
        target_networth_diff=900,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
        raw_early_output={"counterpick_1vs1": 4, "solo": 3},
        raw_mid_output={"counterpick_1vs1": 4, "solo": 3},
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: {
            "level": 60,
            "min_odds": 1.80,
            "wr_pct": 62.0 if phase == "early" else 64.0,
        },
    )
    _patch_team_elo_summary(monkeypatch, radiant_wr=56.0, dire_wr=44.0)

    result = _run_branch_scenario(monkeypatch, case)

    assert len(result.sent_messages) == 1
    assert result.sent_messages[0].startswith("СТАВКА НА radiant x2\n")
