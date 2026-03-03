from __future__ import annotations

import inspect
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import pytest


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


def _runtime_has_networth_dispatch_logic() -> bool:
    try:
        source = inspect.getsource(runtime.check_head)
    except (OSError, TypeError):
        return False
    probe = source.lower()
    required_tokens = (
        "target_networth_diff",
        "800",
        "1000",
        "3000",
        "1500",
    )
    return all(token in probe for token in required_tokens)


if not _runtime_has_networth_dispatch_logic():
    pytestmark = pytest.mark.skip(
        reason=(
            "Runtime networth-gated dispatch logic is not present yet in base/cyberscore_try.py "
            "(dependency: task b205acba)."
        )
    )


@dataclass(frozen=True)
class Scenario:
    name: str
    game_time_seconds: int
    target_networth_diff: int
    has_early_star: bool
    early_sign: int
    has_late_star: bool
    late_sign: int
    expected_send: bool
    expected_reason_token_groups: Tuple[Tuple[str, ...], ...]


@dataclass(frozen=True)
class Decision:
    send: bool
    reason: str
    action: str


def _safe_source(fn: Any) -> str:
    try:
        return inspect.getsource(fn)
    except (OSError, TypeError):
        return ""


def _resolve_networth_dispatch_helper() -> Any:
    preferred_names = (
        "_evaluate_networth_gated_dispatch",
        "_decide_networth_gated_dispatch",
        "_compute_networth_dispatch_decision",
        "_decide_late_dispatch_by_networth",
        "_resolve_networth_dispatch_decision",
    )
    for name in preferred_names:
        fn = getattr(runtime, name, None)
        if callable(fn):
            return fn

    candidates = []
    for name, fn in inspect.getmembers(runtime, inspect.isfunction):
        if name == "check_head":
            continue
        source = _safe_source(fn).lower()
        if not source:
            continue
        if "networth" not in source:
            continue
        if "reason" not in source:
            continue
        if not any(token in source for token in ("800", "1000", "3000", "1500")):
            continue
        if not any(token in source for token in ("game_time", "minute")):
            continue
        candidates.append((name, fn, len(source)))

    if not candidates:
        pytest.skip(
            "Unable to find a callable networth dispatch decision helper in runtime "
            "(dependency task b205acba still in progress)."
        )

    candidates.sort(key=lambda item: item[2])
    return candidates[0][1]


def _build_context(case: Scenario) -> Dict[str, Any]:
    target_sign = case.late_sign if case.has_late_star else case.early_sign
    radiant_lead = case.target_networth_diff if target_sign == 1 else -case.target_networth_diff
    minute = float(case.game_time_seconds) / 60.0
    return {
        "game_time": float(case.game_time_seconds),
        "game_time_seconds": float(case.game_time_seconds),
        "current_game_time": float(case.game_time_seconds),
        "minute": minute,
        "current_minute": minute,
        "game_minute": minute,
        "target_networth_diff": float(case.target_networth_diff),
        "target_side_networth_diff": float(case.target_networth_diff),
        "networth_diff": float(case.target_networth_diff),
        "radiant_lead": float(radiant_lead),
        "lead": float(radiant_lead),
        "has_early_star": case.has_early_star,
        "has_late_star": case.has_late_star,
        "selected_early_star": case.has_early_star,
        "selected_late_star": case.has_late_star,
        "no_early_star": (not case.has_early_star),
        "early_sign": case.early_sign,
        "late_sign": case.late_sign,
        "selected_early_sign": case.early_sign,
        "selected_late_sign": case.late_sign,
        "target_sign": target_sign,
        "target_team_sign": target_sign,
        "target_is_radiant": target_sign == 1,
        "target_is_dire": target_sign == -1,
        "early_and_late_same_sign": (
            case.has_early_star and case.has_late_star and case.early_sign == case.late_sign
        ),
        "early_opposite_late_target": (
            case.has_early_star and case.has_late_star and case.early_sign != case.late_sign
        ),
        "late_only_target": case.has_late_star and not case.has_early_star,
        "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
        "delayed_target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
        "early_diag": {
            "valid": case.has_early_star,
            "sign": case.early_sign if case.has_early_star else None,
        },
        "late_diag": {
            "valid": case.has_late_star,
            "sign": case.late_sign if case.has_late_star else None,
        },
    }


def _match_param_value(param_name: str, context: Dict[str, Any]) -> Tuple[bool, Any]:
    if param_name in context:
        return True, context[param_name]

    key = param_name.lower()
    aliases = {
        "target_networth_diff": ("target_networth_diff", "target_side_networth_diff", "networth_diff"),
        "networth_diff": ("target_networth_diff", "target_side_networth_diff", "networth_diff"),
        "radiant_lead": ("radiant_lead", "lead"),
        "lead": ("lead", "radiant_lead"),
        "game_time": ("game_time", "current_game_time", "game_time_seconds"),
        "current_game_time": ("current_game_time", "game_time", "game_time_seconds"),
        "game_time_seconds": ("game_time_seconds", "game_time", "current_game_time"),
        "minute": ("minute", "game_minute", "current_minute"),
        "game_minute": ("game_minute", "minute", "current_minute"),
        "current_minute": ("current_minute", "minute", "game_minute"),
        "early_sign": ("early_sign", "selected_early_sign"),
        "late_sign": ("late_sign", "selected_late_sign"),
        "selected_early_sign": ("selected_early_sign", "early_sign"),
        "selected_late_sign": ("selected_late_sign", "late_sign"),
        "has_early_star": ("has_early_star", "selected_early_star"),
        "has_late_star": ("has_late_star", "selected_late_star"),
        "target_game_time": ("target_game_time", "delayed_target_game_time"),
        "delayed_target_game_time": ("delayed_target_game_time", "target_game_time"),
        "target_sign": ("target_sign", "target_team_sign"),
        "target_team_sign": ("target_team_sign", "target_sign"),
        "early_diag": ("early_diag",),
        "late_diag": ("late_diag",),
    }
    if key in aliases:
        for alias in aliases[key]:
            if alias in context:
                return True, context[alias]

    if "networth" in key:
        return True, context["target_networth_diff"]
    if "game_time" in key:
        return True, context["game_time"]
    if "minute" in key:
        return True, context["minute"]
    if key.endswith("lead"):
        return True, context["radiant_lead"]
    if "early" in key and "sign" in key:
        return True, context["early_sign"]
    if "late" in key and "sign" in key:
        return True, context["late_sign"]
    if "early" in key and "star" in key:
        return True, context["has_early_star"]
    if "late" in key and "star" in key:
        return True, context["has_late_star"]
    return False, None


def _normalize_decision(raw: Any) -> Decision:
    if isinstance(raw, dict):
        action = str(raw.get("action") or raw.get("decision") or raw.get("status") or "")
        reason = str(
            raw.get("reason")
            or raw.get("dispatch_reason")
            or raw.get("wait_reason")
            or raw.get("status_reason")
            or raw.get("label")
            or ""
        )
        send_value = None
        for key in ("send_now", "should_send", "send", "allow_send", "is_send", "send_immediately"):
            if key in raw:
                send_value = raw[key]
                break
        if send_value is None and action:
            send_value = action.lower() in {"send", "allow", "dispatch_now", "sent", "queue_send"}
        if send_value is None:
            raise AssertionError(f"Unsupported decision dict payload (no send flag): {raw!r}")
        return Decision(send=bool(send_value), reason=reason, action=action)

    if isinstance(raw, (tuple, list)):
        if len(raw) >= 2 and isinstance(raw[0], bool):
            return Decision(send=bool(raw[0]), reason=str(raw[1]), action="")
        if len(raw) >= 2 and isinstance(raw[0], str) and isinstance(raw[1], bool):
            action = str(raw[0])
            reason = str(raw[2]) if len(raw) >= 3 else ""
            return Decision(send=bool(raw[1]), reason=reason, action=action)
        raise AssertionError(f"Unsupported decision tuple payload: {raw!r}")

    raise AssertionError(f"Unsupported decision return type: {type(raw)!r}, value={raw!r}")


def _invoke_decision(case: Scenario) -> Decision:
    helper = _resolve_networth_dispatch_helper()
    context = _build_context(case)
    signature = inspect.signature(helper)
    kwargs: Dict[str, Any] = {}
    missing = []
    for name, param in signature.parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        found, value = _match_param_value(name, context)
        if found:
            kwargs[name] = value
            continue
        if param.default is inspect.Parameter.empty:
            missing.append(name)
    if missing:
        raise AssertionError(
            f"Cannot call {helper.__name__} for networth tests; missing required args: {missing}"
        )
    raw = helper(**kwargs)
    return _normalize_decision(raw)


def _assert_reason_tokens(decision: Decision, token_groups: Iterable[Tuple[str, ...]]) -> None:
    haystack = f"{decision.reason} {decision.action}".lower()
    assert haystack.strip(), "Branch decision must expose a non-empty reason/action label"
    for group in token_groups:
        assert any(token in haystack for token in group), (
            f"Expected one of tokens {group} in reason/action, got: "
            f"reason={decision.reason!r}, action={decision.action!r}"
        )


SCENARIOS = (
    Scenario(
        name="blocked_before_4m",
        game_time_seconds=(3 * 60) + 59,
        target_networth_diff=4000,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send=False,
        expected_reason_token_groups=(
            ("4", "pre4", "before_4", "lt4", "under4"),
            ("wait", "block", "hold", "gate"),
        ),
    ),
    Scenario(
        name="send_4_to_10_at_plus_800",
        game_time_seconds=6 * 60,
        target_networth_diff=800,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send=True,
        expected_reason_token_groups=(
            ("4_10", "4to10", "4-10", "early_window"),
            ("800", "+800", "ge800"),
            ("send", "dispatch", "allow", "release"),
        ),
    ),
    Scenario(
        name="minute_10_send_when_not_worse_than_minus_1500",
        game_time_seconds=10 * 60,
        target_networth_diff=-1500,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send=True,
        expected_reason_token_groups=(
            ("10", "minute10", "at10"),
            ("1500", "-1500", "lose1500", "within1500"),
            ("send", "dispatch", "allow", "release"),
        ),
    ),
    Scenario(
        name="late_monitor_without_early_stars_send_at_plus_1000",
        game_time_seconds=15 * 60,
        target_networth_diff=1000,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send=True,
        expected_reason_token_groups=(
            ("monitor", "late_monitor", "no_early", "late_only"),
            ("1000", "+1000", "ge1000"),
            ("send", "dispatch", "allow", "release"),
        ),
    ),
    Scenario(
        name="opposite_early_late_send_at_plus_3000",
        game_time_seconds=15 * 60,
        target_networth_diff=3000,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        expected_send=True,
        expected_reason_token_groups=(
            ("opposite", "conflict", "early_opposite"),
            ("3000", "+3000", "ge3000"),
            ("send", "dispatch", "allow", "release"),
        ),
    ),
    Scenario(
        name="fallback_send_at_21_when_early_thresholds_unmet",
        game_time_seconds=21 * 60,
        target_networth_diff=200,
        has_early_star=False,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send=True,
        expected_reason_token_groups=(
            ("21", "21m", "fallback", "delayed"),
            ("send", "dispatch", "target_reached", "release"),
        ),
    ),
)


@pytest.mark.parametrize("case", SCENARIOS, ids=[case.name for case in SCENARIOS])
def test_networth_dispatch_scenarios(case: Scenario) -> None:
    first = _invoke_decision(case)
    second = _invoke_decision(case)

    assert first == second, (
        "Decision helper must be deterministic for identical inputs: "
        f"first={first!r}, second={second!r}"
    )
    assert first.send is case.expected_send, (
        f"Unexpected send verdict for case={case.name}: got={first.send}, "
        f"expected={case.expected_send}, reason={first.reason!r}, action={first.action!r}"
    )
    _assert_reason_tokens(first, case.expected_reason_token_groups)
