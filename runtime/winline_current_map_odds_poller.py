"""Deterministic no-sleep whole-current-map Winline odds polling controller.

Inject collector, monotonic/wall clocks, and exact-map-current predicate.
Never imports/starts a browser, never sleeps, never overlaps attempts.

Canonical identity: exact (series, map_num, ordered teams).
Poll cadence: exactly +5s from prior attempt *start* (not finish).
Overrun: coalesce all missed intervals into one immediate non-overlapping
follow-up; never burst-replay missed ticks; flag overrun truthfully.
"""
from __future__ import annotations

import math
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

SCHEMA_VERSION = "winline_current_map_odds_poller.v1"


def _env_float(name: str, default: float) -> float:
    """Env override with a safe fallback; never raises on garbage input."""
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return float(default)
    return value if value > 0 else float(default)


# Опрос можно ускорить через env, не трогая код. Практический пол задаёт
# латентность самого чтения: если attempt длится дольше интервала, попытки
# коалесцируются и это честно отражается в cadence_overrun.
POLL_INTERVAL_SECONDS = _env_float("WINLINE_CURRENT_MAP_POLL_INTERVAL_S", 5.0)
ACCELERATED_POLL_INTERVAL_SECONDS = 2.0
RELOAD_AFTER_CONSECUTIVE_MISSES = 3
RELOAD_MIN_SPACING_SECONDS = 60.0
SAFETY_CEILING_SECONDS = 90 * 60.0  # 90 minutes; not proof map ended

PRIMARY_MARKET_NEVER_EXPOSED = "market_never_exposed"
PRIMARY_PARSER_RESOLUTION_FAILURE = "parser_resolution_failure"
PRIMARY_BROWSER_ACQUISITION_FAILURE = "browser_acquisition_failure"
PRIMARY_INDETERMINATE_MAP_LIFECYCLE = "indeterminate_map_lifecycle"

_ELIGIBLE_MISS_STATUSES = frozenset({"missing", "closed", "market_missing", "market_closed"})
_BROWSER_ERROR_STATUSES = frozenset({"error", "browser_error", "acquisition_error"})


def make_canonical_map_key(
    series: Any,
    map_num: Any,
    team1: Any,
    team2: Any,
) -> str:
    """Canonical identity: series|mapN|team1|team2 (ordered teams)."""
    s = str(series or "").strip()
    t1 = str(team1 or "").strip()
    t2 = str(team2 or "").strip()
    try:
        n = int(map_num)
    except (TypeError, ValueError):
        n = map_num
    return f"{s}|map{n}|{t1}|{t2}"


# Alias preferred by some call sites / tests.
canonical_map_key = make_canonical_map_key


def _as_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or not math.isfinite(number):  # NaN / inf
        return None
    return number


def _is_finite_odds(value: Any) -> bool:
    number = _as_float(value)
    return number is not None and number > 1.0


def _normalize_market_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_side(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _eval_map_current(
    is_map_current: Callable[..., Any],
    *,
    identity: Dict[str, Any],
) -> Dict[str, Any]:
    """Normalize predicate result to {current, reason, proven}."""
    try:
        raw = is_map_current(**identity)
    except TypeError:
        try:
            raw = is_map_current(identity)
        except TypeError:
            raw = is_map_current()
    if raw is True:
        return {"current": True, "reason": None, "proven": True}
    if raw is False:
        return {"current": False, "reason": "not_current", "proven": True}
    if isinstance(raw, dict):
        current = bool(raw.get("current", False))
        reason = raw.get("reason")
        if "proven" in raw:
            proven = bool(raw.get("proven"))
        else:
            # Explicit lifecycle reasons are treated as proven map/series end.
            proven = reason in {
                "map_rollover",
                "series_complete",
                "series_cancel",
                "series_ended",
                "map_ended",
            } or (reason is not None and reason not in {"unknown", "indeterminate"})
            if reason in {"unknown", "indeterminate"}:
                proven = False
        return {"current": current, "reason": reason, "proven": proven}
    # truthy non-dict → current
    if raw:
        return {"current": True, "reason": None, "proven": True}
    return {"current": False, "reason": "not_current", "proven": True}


def _is_browser_failure(result: Dict[str, Any]) -> bool:
    if result.get("page_valid") is False:
        return True
    status = _normalize_market_status(result.get("market_status"))
    if status in _BROWSER_ERROR_STATUSES:
        return True
    if result.get("acquisition_error") or result.get("error"):
        # acquisition/error without valid open market counts as browser-ish
        if status not in {"open", "ok", "available"} and not (
            _is_finite_odds(result.get("p1_odds")) and _is_finite_odds(result.get("p2_odds"))
        ):
            return True
    return False


def _is_eligible_miss(result: Dict[str, Any]) -> bool:
    if _is_browser_failure(result):
        return False
    status = _normalize_market_status(result.get("market_status"))
    if status in _ELIGIBLE_MISS_STATUSES:
        return True
    # open/present text but invalid odds is NOT eligible miss for reload streak
    # (parser path); still not accepted.
    return False


def _has_market_or_parser_signal(result: Dict[str, Any]) -> bool:
    """Page/market text present or changed / open but not valid P1/P2."""
    if _is_browser_failure(result):
        return False
    status = _normalize_market_status(result.get("market_status"))
    if status in {"open", "ok", "available", "present"}:
        return True
    reasons = result.get("parser_failure_reasons") or []
    if reasons:
        # pure market_missing/closed markers alone do not count as parser signal
        pure_market = {
            "market_missing",
            "market_closed",
            "odds_missing",
            "closed",
            "missing",
        }
        if any(str(r) not in pure_market for r in reasons):
            return True
    p1 = result.get("p1_odds")
    p2 = result.get("p2_odds")
    if p1 is not None or p2 is not None:
        return True
    return False


# Букмекер рендерит команду по-своему: "Ilbirs Esports" на нашей стороне против
# "ILBIRS" в карточке Winline. Точное равенство строк отбраковывало корректные
# кэфы при открытом рынке, поэтому сверяем по нормализованным токенам.
#
# Осознанно НЕ используем сравнение подмножеств: "Team Spirit" и
# "Team Spirit Academy" дали бы совпадение, а это разные команды.
_TEAM_GENERIC_TOKENS = frozenset(
    {"esports", "esport", "sports", "team", "gaming", "club", "org"}
)


def _normalize_team_tokens(value: Any) -> Tuple[str, ...]:
    """Значимые токены названия: без пунктуации, регистра и родовых слов."""
    text = re.sub(r"[^0-9a-zA-Zа-яёА-ЯЁ]+", " ", str(value or "")).strip().lower()
    if not text:
        return ()
    tokens = tuple(t for t in text.split() if t and t not in _TEAM_GENERIC_TOKENS)
    # Название целиком из родовых слов оставляем как есть, иначе потеряем его.
    return tokens or tuple(text.split())


def _teams_equivalent(left: Any, right: Any) -> bool:
    """True, если названия обозначают одну команду."""
    left_raw, right_raw = str(left or "").strip(), str(right or "").strip()
    if not left_raw or not right_raw:
        return False
    if left_raw == right_raw:
        return True
    return _normalize_team_tokens(left_raw) == _normalize_team_tokens(right_raw)


def _odds_accepted(
    result: Dict[str, Any],
    *,
    identity: Dict[str, Any],
) -> bool:
    if not _is_finite_odds(result.get("p1_odds")):
        return False
    if not _is_finite_odds(result.get("p2_odds")):
        return False
    source = str(result.get("source") or "").strip().lower()
    if source not in {
        "winline",
        "winline_current_map_winner",
        "winline/current-map-winner",
        "winline_current_map",
    }:
        # tolerate casing variants containing winline + current map signal
        if "winline" not in source:
            return False
    try:
        res_map = int(result.get("map_num")) if result.get("map_num") is not None else None
    except (TypeError, ValueError):
        res_map = None
    if res_map is not None and res_map != int(identity["map_num"]):
        return False
    # team identity when provided on result
    if result.get("team1") is not None and not _teams_equivalent(
        result.get("team1"), identity["team1"]
    ):
        return False
    if result.get("team2") is not None and not _teams_equivalent(
        result.get("team2"), identity["team2"]
    ):
        return False
    if result.get("page_valid") is False:
        return False
    return True


def _bounded_dom(value: Any, limit: int = 128) -> str:
    text = str(value or "")
    if len(text) > limit:
        return text[:limit]
    return text


class WinlineCurrentMapOddsPoller:
    """Serial 5-second whole-map poller state machine (no sleep)."""

    def __init__(
        self,
        *,
        collector: Callable[..., Dict[str, Any]],
        is_map_current: Callable[..., Any],
        monotonic_fn: Optional[Callable[[], float]] = None,
        wall_fn: Optional[Callable[[], float]] = None,
        producer_pid: int = 0,
        producer_start_generation: str = "",
        selected_side: Any = None,
        poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
        reload_after_consecutive_misses: int = RELOAD_AFTER_CONSECUTIVE_MISSES,
        reload_min_spacing_seconds: float = RELOAD_MIN_SPACING_SECONDS,
        safety_ceiling_seconds: float = SAFETY_CEILING_SECONDS,
        continuous: bool = False,
        **_extra: Any,
    ) -> None:
        self._collector = collector
        self._is_map_current = is_map_current
        self._mono = monotonic_fn or time.monotonic
        self._wall = wall_fn or time.time
        self._producer_pid = int(producer_pid)
        self._producer_start_generation = str(producer_start_generation or "")
        self._selected_side = selected_side
        self._poll_interval = float(poll_interval_seconds)
        self._reload_after = int(reload_after_consecutive_misses)
        self._reload_spacing = float(reload_min_spacing_seconds)
        self._safety_ceiling = float(safety_ceiling_seconds)
        # Continuous mode: accepted odds stop being terminal, so the same map keeps
        # being polled and later price moves are observed. Lifecycle terminals
        # (map rollover, series end, PID generation change, safety ceiling) still stop it.
        self._continuous = bool(continuous)
        self._accelerated = False
        self._accelerated_interval = ACCELERATED_POLL_INTERVAL_SECONDS

        self._active = False
        self._in_flight = False
        self._identity: Optional[Dict[str, Any]] = None
        self._canonical_key: Optional[str] = None
        self._attempt_index = 0
        self._attempts: List[Dict[str, Any]] = []
        self._consecutive_misses = 0
        self._reload_count = 0
        self._last_reload_mono: Optional[float] = None
        self._last_dom_signature: Optional[str] = None
        self._last_dom_hash: Optional[str] = None
        self._page_valid_for_dynamic = False
        self._next_poll_mono: float = 0.0
        self._started_mono: Optional[float] = None
        self._started_wall: Optional[float] = None
        self._last_attempt_start_mono: Optional[float] = None
        self._terminal: Optional[Dict[str, Any]] = None
        self._had_parser_signal = False
        self._had_browser_failure = False
        self._all_eligible_misses = True  # vacuously; flips false on non-miss
        self._saw_stable_valid_page = False
        self._pending_generation_change = False
        self._lifecycle_event: Optional[str] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_active(self) -> bool:
        return bool(self._active) and self._terminal is None

    def attempts(self) -> List[Dict[str, Any]]:
        return list(self._attempts)

    def terminal(self) -> Optional[Dict[str, Any]]:
        return dict(self._terminal) if self._terminal is not None else None

    @property
    def accelerated(self) -> bool:
        return self._accelerated

    def set_accelerated(self, enabled: bool) -> None:
        self._accelerated = bool(enabled)

    def begin(
        self,
        *,
        series: Any,
        map_num: Any,
        team1: Any,
        team2: Any,
        **_extra: Any,
    ) -> Union[bool, str]:
        if self._active and self._terminal is None:
            return False
        identity = {
            "series": str(series or "").strip(),
            "map_num": int(map_num),
            "team1": str(team1 or "").strip(),
            "team2": str(team2 or "").strip(),
        }
        status = _eval_map_current(self._is_map_current, identity=identity)
        if not status["current"]:
            return False
        self._reset_state()
        self._identity = identity
        self._canonical_key = make_canonical_map_key(
            identity["series"],
            identity["map_num"],
            identity["team1"],
            identity["team2"],
        )
        now_mono = float(self._mono())
        now_wall = float(self._wall())
        self._started_mono = now_mono
        self._started_wall = now_wall
        self._next_poll_mono = now_mono  # first attempt due immediately
        self._active = True
        return True

    def note_service_generation(
        self,
        *,
        pid: Any = None,
        start_generation: Any = None,
        producer_pid: Any = None,
        producer_start_generation: Any = None,
    ) -> None:
        new_pid = producer_pid if producer_pid is not None else pid
        new_gen = (
            producer_start_generation
            if producer_start_generation is not None
            else start_generation
        )
        changed = False
        if new_pid is not None and int(new_pid) != int(self._producer_pid):
            changed = True
        if new_gen is not None and str(new_gen) != str(self._producer_start_generation):
            changed = True
        if changed:
            self._pending_generation_change = True
            self._lifecycle_event = "service_generation_change"

    def tick(
        self,
        *,
        producer_pid: Any = None,
        producer_start_generation: Any = None,
        **_extra: Any,
    ) -> Optional[Dict[str, Any]]:
        if self._terminal is not None:
            return {"terminal": dict(self._terminal), "status": "terminal"}
        if not self._active or self._identity is None:
            return None
        if self._in_flight:
            return {"status": "in_flight"}

        # Generation change via tick kwargs
        if producer_pid is not None or producer_start_generation is not None:
            self.note_service_generation(
                producer_pid=producer_pid,
                producer_start_generation=producer_start_generation,
            )

        now_mono = float(self._mono())
        now_wall = float(self._wall())

        # Safety ceiling (not proof of map end)
        if (
            self._started_mono is not None
            and (now_mono - self._started_mono) >= self._safety_ceiling
        ):
            return self._finalize_terminal(
                primary_reason=PRIMARY_INDETERMINATE_MAP_LIFECYCLE,
                lifecycle_event="safety_ceiling",
                map_end_proven=False,
                now_wall=now_wall,
                now_mono=now_mono,
            )

        if self._pending_generation_change:
            return self._finalize_from_lifecycle(
                lifecycle_reason="service_generation_change",
                proven=True,
                now_wall=now_wall,
                now_mono=now_mono,
            )

        # Lifecycle predicate before scheduling another attempt
        status = _eval_map_current(self._is_map_current, identity=self._identity)
        if not status["current"]:
            return self._finalize_from_lifecycle(
                lifecycle_reason=str(status.get("reason") or "not_current"),
                proven=bool(status.get("proven")),
                now_wall=now_wall,
                now_mono=now_mono,
            )

        if now_mono < self._next_poll_mono:
            return {"status": "not_due", "next_poll_at_monotonic": self._next_poll_mono}

        return self._run_attempt(now_mono=now_mono, now_wall=now_wall)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _reset_state(self) -> None:
        self._active = False
        self._in_flight = False
        self._identity = None
        self._canonical_key = None
        self._attempt_index = 0
        self._attempts = []
        self._consecutive_misses = 0
        self._reload_count = 0
        self._last_reload_mono = None
        self._last_dom_signature = None
        self._last_dom_hash = None
        self._page_valid_for_dynamic = False
        self._next_poll_mono = 0.0
        self._started_mono = None
        self._started_wall = None
        self._last_attempt_start_mono = None
        self._terminal = None
        self._had_parser_signal = False
        self._had_browser_failure = False
        self._all_eligible_misses = True
        self._saw_stable_valid_page = False
        self._pending_generation_change = False
        self._lifecycle_event = None
        self._accelerated = False

    def _choose_acquisition_mode(self, now_mono: float) -> str:
        if not self._page_valid_for_dynamic:
            return "initial_goto"
        # reload escalation: 3 consecutive eligible misses + spacing
        if self._consecutive_misses >= self._reload_after:
            if self._last_reload_mono is None or (
                now_mono - self._last_reload_mono
            ) >= self._reload_spacing:
                return "controlled_reload"
        return "dynamic_dom"

    def _run_attempt(self, *, now_mono: float, now_wall: float) -> Dict[str, Any]:
        assert self._identity is not None
        self._in_flight = True
        self._attempt_index += 1
        mode = self._choose_acquisition_mode(now_mono)
        started_wall = float(self._wall())
        started_mono = float(self._mono())
        result: Dict[str, Any] = {}
        try:
            raw = self._collector(
                acquisition_mode=mode,
                series=self._identity["series"],
                map_num=self._identity["map_num"],
                team1=self._identity["team1"],
                team2=self._identity["team2"],
                canonical_key=self._canonical_key,
                attempt_index=self._attempt_index,
                now=started_wall,
                now_monotonic=started_mono,
                selected_side=self._selected_side,
                producer_pid=self._producer_pid,
                producer_start_generation=self._producer_start_generation,
            )
            result = dict(raw or {})
        except Exception as exc:  # collector failure → browser acquisition class
            result = {
                "market_status": "error",
                "page_valid": False,
                "error": f"collector_exception:{type(exc).__name__}",
                "acquisition_error": f"collector_exception:{type(exc).__name__}",
                "parser_failure_reasons": ["collector_exception"],
                "p1_odds": None,
                "p2_odds": None,
                "dom_signature": "",
                "dom_hash": "",
                "source": "winline",
            }
        finally:
            finished_wall = float(self._wall())
            finished_mono = float(self._mono())
            self._in_flight = False

        # True wall-clock attempt span (monotonic). Keep separate from any
        # collector-supplied latency echo used only as a secondary hint.
        attempt_latency = max(0.0, finished_mono - started_mono)
        latency = attempt_latency
        if result.get("latency_seconds") is not None:
            try:
                latency = float(result["latency_seconds"])
            except (TypeError, ValueError):
                pass

        prior_start = self._last_attempt_start_mono
        if prior_start is None:
            attempt_start_delta: Optional[float] = None
        else:
            attempt_start_delta = max(0.0, started_mono - float(prior_start))
        self._last_attempt_start_mono = started_mono

        mode_used = str(result.get("acquisition_mode_echo") or mode)
        # If collector echoes a mode, trust caller-requested mode for policy.
        mode_used = mode

        if mode_used == "controlled_reload":
            self._reload_count += 1
            self._last_reload_mono = finished_mono

        page_valid = result.get("page_valid")
        if page_valid is None:
            page_valid = not _is_browser_failure(result)
        page_valid = bool(page_valid)

        dom_sig = _bounded_dom(result.get("dom_signature"))
        dom_hash = _bounded_dom(result.get("dom_hash"))
        market_status = _normalize_market_status(result.get("market_status")) or "unknown"
        source = result.get("source") or "winline"
        parser_reasons = list(result.get("parser_failure_reasons") or [])
        p1 = result.get("p1_odds")
        p2 = result.get("p2_odds")

        browser_fail = _is_browser_failure(result)
        eligible_miss = _is_eligible_miss(result)
        accepted = _odds_accepted(result, identity=self._identity) if not browser_fail else False

        # Track classification signals
        if browser_fail:
            self._had_browser_failure = True
            self._all_eligible_misses = False
        elif _has_market_or_parser_signal(result) and not accepted:
            self._had_parser_signal = True
            self._all_eligible_misses = False
        elif eligible_miss:
            if page_valid:
                self._saw_stable_valid_page = True
        else:
            # other non-accepted outcomes
            if not accepted:
                self._all_eligible_misses = False

        # consecutive miss / DOM change handling
        if accepted:
            self._consecutive_misses = 0
            self._last_dom_signature = dom_sig
            self._last_dom_hash = dom_hash
            self._page_valid_for_dynamic = True
        elif browser_fail:
            self._consecutive_misses = 0
            self._page_valid_for_dynamic = False
            self._last_dom_signature = dom_sig or None
            self._last_dom_hash = dom_hash or None
        elif eligible_miss:
            dom_changed = False
            if self._last_dom_hash is not None and dom_hash and dom_hash != self._last_dom_hash:
                dom_changed = True
            if (
                self._last_dom_signature is not None
                and dom_sig
                and dom_sig != self._last_dom_signature
            ):
                dom_changed = True
            if dom_changed:
                self._consecutive_misses = 1  # this miss starts a new streak
            else:
                self._consecutive_misses += 1
            self._last_dom_signature = dom_sig
            self._last_dom_hash = dom_hash
            self._page_valid_for_dynamic = True
            self._saw_stable_valid_page = True
        else:
            # parser-ish open without valid odds
            dom_changed = False
            if self._last_dom_hash is not None and dom_hash and dom_hash != self._last_dom_hash:
                dom_changed = True
            if (
                self._last_dom_signature is not None
                and dom_sig
                and dom_sig != self._last_dom_signature
            ):
                dom_changed = True
            if dom_changed:
                self._consecutive_misses = 0
            # open market with bad odds is not an eligible miss for reload
            self._last_dom_signature = dom_sig
            self._last_dom_hash = dom_hash
            self._page_valid_for_dynamic = page_valid

        # Prior-start cadence: nominal due = attempt_start + interval.
        # Overrun (finish past one or more nominal dues): coalesce all missed
        # intervals into exactly one immediate next opportunity — never burst.
        interval = float(self._accelerated_interval) if self._accelerated else float(self._poll_interval)
        nominal_next = started_mono + interval
        coalesced_missed = 0
        t_due = nominal_next
        # Count every nominal due boundary that fell at/before finish.
        while t_due <= finished_mono + 1e-12:
            coalesced_missed += 1
            t_due += interval
        cadence_overrun = coalesced_missed > 0
        if cadence_overrun:
            next_poll = finished_mono  # immediate single follow-up
        else:
            next_poll = nominal_next
        self._next_poll_mono = next_poll

        side = _normalize_side(self._selected_side)
        attempt: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "producer_pid": self._producer_pid,
            "producer_start_generation": self._producer_start_generation,
            "start_generation": self._producer_start_generation,
            "canonical_key": self._canonical_key,
            "series": self._identity["series"],
            "map_num": self._identity["map_num"],
            "team1": self._identity["team1"],
            "team2": self._identity["team2"],
            "selected_side": side if side else ("" if self._selected_side is not None else None),
            "attempt_index": self._attempt_index,
            "attempt_started_at": started_wall,
            "attempt_finished_at": finished_wall,
            "attempt_started_at_monotonic": started_mono,
            "attempt_finished_at_monotonic": finished_mono,
            "attempt_start_delta_seconds": attempt_start_delta,
            "latency_seconds": latency,
            "attempt_latency_seconds": attempt_latency,
            "cadence_interval_seconds": interval,
            "nominal_next_poll_at_monotonic": nominal_next,
            "cadence_overrun": bool(cadence_overrun),
            "overrun": bool(cadence_overrun),
            "coalesced_missed_intervals": int(coalesced_missed),
            # Overrun/coalesce is explicitly non-compliant even when serialized.
            "cadence_compliant": (not cadence_overrun),
            "acquisition_mode": mode_used,
            "current_url": _bounded_dom(result.get("current_url"), 256),
            "dom_signature": dom_sig,
            "dom_hash": dom_hash,
            "market_status": market_status,
            "source": source,
            "p1_odds": p1,
            "p2_odds": p2,
            "parser_failure_reasons": parser_reasons,
            "consecutive_misses": self._consecutive_misses,
            "reload_count": self._reload_count,
            "last_reload_at_monotonic": self._last_reload_mono,
            "next_poll_at_monotonic": next_poll,
            "next_poll_monotonic": next_poll,
            "accepted": bool(accepted),
            "pending": not accepted,
            "page_valid": page_valid,
            "error": result.get("error"),
            "acquisition_error": result.get("acquisition_error"),
        }
        # Preserve empty selected_side explicitly when provided as empty string
        if self._selected_side is not None and not side:
            attempt["selected_side"] = ""
        elif side:
            attempt["selected_side"] = side
        else:
            attempt["selected_side"] = None

        self._attempts.append(attempt)

        if accepted:
            if self._continuous:
                # Не выставляем _terminal: is_active() гаснет именно по нему.
                # Следующий опрос уже запланирован в self._next_poll_mono выше.
                return {
                    "attempt": attempt,
                    "status": "success",
                    "success": True,
                    "continuous": True,
                }
            terminal = self._build_success_terminal(attempt=attempt, now_wall=finished_wall)
            self._terminal = terminal
            self._active = False
            return {"attempt": attempt, "terminal": terminal, "status": "success", "success": True}

        return {"attempt": attempt, "status": "pending", "pending": True}

    def _classify_primary_reason(self, *, map_end_proven: bool) -> str:
        if not map_end_proven:
            return PRIMARY_INDETERMINATE_MAP_LIFECYCLE
        if not self._attempts:
            return PRIMARY_INDETERMINATE_MAP_LIFECYCLE
        # browser path: all attempts browser failures (or persistent browser)
        all_browser = all(
            (a.get("page_valid") is False)
            or _normalize_market_status(a.get("market_status")) in _BROWSER_ERROR_STATUSES
            or a.get("acquisition_error")
            or a.get("error")
            for a in self._attempts
        )
        if all_browser or (
            self._had_browser_failure
            and not self._had_parser_signal
            and not self._saw_stable_valid_page
        ):
            return PRIMARY_BROWSER_ACQUISITION_FAILURE
        if self._had_parser_signal:
            return PRIMARY_PARSER_RESOLUTION_FAILURE
        # market never exposed: all eligible attempts positively closed/missing
        # with stable valid page identity
        all_miss = True
        for a in self._attempts:
            status = _normalize_market_status(a.get("market_status"))
            if status not in _ELIGIBLE_MISS_STATUSES:
                all_miss = False
                break
            if a.get("page_valid") is False:
                all_miss = False
                break
        if all_miss and self._saw_stable_valid_page:
            return PRIMARY_MARKET_NEVER_EXPOSED
        if self._had_browser_failure and not self._had_parser_signal:
            return PRIMARY_BROWSER_ACQUISITION_FAILURE
        if self._all_eligible_misses and self._saw_stable_valid_page:
            return PRIMARY_MARKET_NEVER_EXPOSED
        return PRIMARY_PARSER_RESOLUTION_FAILURE

    def _build_success_terminal(
        self,
        *,
        attempt: Dict[str, Any],
        now_wall: float,
    ) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "terminal": True,
            "success": True,
            "outcome": "success",
            "status": "success",
            "primary_reason": None,
            "failure_reason": None,
            "reason": None,
            "map_end_proven": False,
            "canonical_key": self._canonical_key,
            "map_num": self._identity["map_num"] if self._identity else None,
            "team1": self._identity["team1"] if self._identity else None,
            "team2": self._identity["team2"] if self._identity else None,
            "attempt_count": self._attempt_index,
            "reload_count": self._reload_count,
            "attempts": list(self._attempts),
            "last_attempt": dict(attempt),
            "last_diagnostics": {
                "dom_signature": attempt.get("dom_signature"),
                "dom_hash": attempt.get("dom_hash"),
                "market_status": attempt.get("market_status"),
                "current_url": attempt.get("current_url"),
                "parser_failure_reasons": list(attempt.get("parser_failure_reasons") or []),
            },
            "p1_odds": attempt.get("p1_odds"),
            "p2_odds": attempt.get("p2_odds"),
            "finished_at": now_wall,
            "producer_pid": self._producer_pid,
            "producer_start_generation": self._producer_start_generation,
        }

    def _finalize_from_lifecycle(
        self,
        *,
        lifecycle_reason: str,
        proven: bool,
        now_wall: float,
        now_mono: float,
    ) -> Dict[str, Any]:
        if lifecycle_reason == "service_generation_change":
            # Proven end of this producer observation; classify from attempts.
            # Generation change is a hard stop; map end relative to match is
            # not independently proven, but contract allows the four primary
            # reasons. Prefer market/parser/browser from attempts; if empty →
            # indeterminate.
            if not self._attempts:
                primary = PRIMARY_INDETERMINATE_MAP_LIFECYCLE
                map_end_proven = False
            else:
                # Treat generation change as terminalization with proven stop
                # for classification purposes when attempts exist.
                map_end_proven = True
                primary = self._classify_primary_reason(map_end_proven=True)
        elif not proven:
            primary = PRIMARY_INDETERMINATE_MAP_LIFECYCLE
            map_end_proven = False
        else:
            map_end_proven = True
            primary = self._classify_primary_reason(map_end_proven=True)
        return self._finalize_terminal(
            primary_reason=primary,
            lifecycle_event=lifecycle_reason,
            map_end_proven=map_end_proven,
            now_wall=now_wall,
            now_mono=now_mono,
        )

    def _finalize_terminal(
        self,
        *,
        primary_reason: str,
        lifecycle_event: Optional[str],
        map_end_proven: bool,
        now_wall: float,
        now_mono: float,
    ) -> Dict[str, Any]:
        last = dict(self._attempts[-1]) if self._attempts else {}
        terminal = {
            "schema_version": SCHEMA_VERSION,
            "terminal": True,
            "success": False,
            "outcome": "terminal",
            "status": "terminal",
            "primary_reason": primary_reason,
            "failure_reason": primary_reason,
            "reason": primary_reason,
            "map_end_proven": bool(map_end_proven),
            "lifecycle_event": lifecycle_event,
            "canonical_key": self._canonical_key,
            "map_num": self._identity["map_num"] if self._identity else None,
            "team1": self._identity["team1"] if self._identity else None,
            "team2": self._identity["team2"] if self._identity else None,
            "attempt_count": self._attempt_index,
            "reload_count": self._reload_count,
            "consecutive_misses": self._consecutive_misses,
            "attempts": list(self._attempts),
            "last_attempt": last,
            "last_diagnostics": {
                "dom_signature": last.get("dom_signature"),
                "dom_hash": last.get("dom_hash"),
                "market_status": last.get("market_status"),
                "current_url": last.get("current_url"),
                "parser_failure_reasons": list(last.get("parser_failure_reasons") or []),
                "error": last.get("error"),
                "acquisition_error": last.get("acquisition_error"),
            },
            "finished_at": now_wall,
            "finished_at_monotonic": now_mono,
            "producer_pid": self._producer_pid,
            "producer_start_generation": self._producer_start_generation,
        }
        self._terminal = terminal
        self._active = False
        return {"terminal": terminal, "status": "terminal", "attempt": None}


# Back-compat alias
CurrentMapOddsPoller = WinlineCurrentMapOddsPoller
