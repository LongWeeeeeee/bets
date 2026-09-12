"""ML dispatch evaluator (stage 1 of the ML-dispatch plan).

Pure decision logic for the five-model ML betting dispatch that will
eventually replace the STAR word-dict paths in ``cyberscore_try.py``
(stage 2, not implemented here). This module imports nothing from
``cyberscore_try`` and has no side effects other than the optional
persistent dedup ledger (:class:`SentLedger`), which is explicit and
caller-driven — :func:`evaluate` itself never touches disk.

Rules implemented (owner decisions, 12.09.2026 — see
``/Users/alex/.claude/plans/swirling-giggling-kurzweil.md``):

- Threshold ``ML_DISPATCH_MIN_CONF`` (default 0.60) applies to all five
  models: Early NW, Early Win, Late, All, ML Laning.
- Underdog ``U`` = side with ELO lower by >= ``ML_DISPATCH_UNDERDOG_MIN_DIFF``
  (default 50). If ``|elo_radiant - elo_dire| < diff`` there is no U/F
  split (``underdog_side is None``).
- Win market (x1 always): a side ``S`` is backed if at least one of the
  configured ``ML_DISPATCH_WIN_MODELS`` (default ``late,all,early_win,early_nw``
  — owner rule 12.09.2026 16:50: "если хоть одна из Early NW / Early Win /
  All / Late имеет ★, сигнал посылается"; ★ in the panel is exactly
  ``confidence >= ML_DISPATCH_MIN_CONF``, so any starred model is support)
  favors ``S`` at >= threshold. ``S`` is vetoed if Late or All (always
  checked, independent of the win-models config) favors the *other*
  side at >= threshold. Vetoes are resolved PER SIDE FIRST, conflict
  SECOND (owner correction 12.09.2026, "правка 0"): a side is a real
  candidate only if it has support AND is not vetoed; if exactly one
  side survives that check, it is backed (the other side's rejection is
  reported as ``veto``, not ``conflict``) even though, before veto was
  applied, both sides had raw model support. Only when BOTH sides still
  have support after their own veto check is it a genuine ``conflict``.
- Kills markets (only when ``U`` exists): Early NW and/or Early Win at
  >= threshold for ``U`` (optionally AND All at >= threshold for ``U``
  when ``ML_DISPATCH_KILLS_REQUIRE_ALL=1``) produce up to two decisions,
  ``kills_window`` (only if ``ctx.kills_windows_open`` is non-empty; uses
  its first — nearest open — label) and ``kills_total``, at most one of
  each per map.
- Timing: only the win market consults ``ctx.lane`` — if ML Laning backs
  the *target* side at >= threshold, ``timing="now"`` (bet at "00");
  otherwise ``timing="now"`` once ``ctx.game_time >= ML_DISPATCH_TIMING_SECONDS``
  (default 600s), else ``timing="wait_600"``. Kills decisions are always
  ``timing="now"`` — the open-window filtering/deadline logic that gates
  ``kills_windows_open`` happens upstream (stage 2), not here.
- ``expected_wr`` = max confidence among the models voting *for* the
  decision; ``min_odds = round(1 / (expected_wr - ML_DISPATCH_MIN_ODDS_MARGIN), 2)``
  (margin default 0.0, i.e. the known zero-margin floor — the env var
  exists precisely to tighten this later).
- Dedup is persistent and keyed by ``(base_url, map_num, market, side)``.
  :func:`evaluate` is a pure function: it only *consults*
  ``ctx.already_sent`` (a plain ``set`` of such tuples, or ``None``) to
  decide whether a would-be decision is a repeat (-> ``Skipped(...,
  reason="dedup")``); it never reads or writes the ledger file. Loading
  the set from disk and persisting new keys after an actual delivery is
  the caller's job, via :class:`SentLedger`.

Design decisions made here that were underspecified by the plan (flagged
for the stage-2 owner to confirm before wiring into ``cyberscore_try``):

1. "Support" for a side is computed only from models present in
   ``cfg.win_models``; "veto" is *always* computed from Late/All
   regardless of that config.
2. Order of resolution (owner correction 12.09.2026, "правка 0"): for
   EACH side independently, veto is applied first — a side "survives"
   only if it has support AND no Late/All veto against it. The
   market-level ``conflict`` skip (one ``Skipped(market="win",
   side=None, reason="conflict")``) fires only when BOTH sides survive
   their own veto check. Example from the plan: Late backs Radiant at
   0.62 and Early Win backs Dire at 0.65 under the default
   ``win_models`` (which counts Late as support for Radiant too) — Dire
   is vetoed by Late (a veto model favoring Radiant), so only Radiant
   survives, and the bet goes on Radiant with a plain ``veto`` Skipped
   for Dire, NOT a ``conflict``. A genuine ``conflict`` needs both
   sides to have support from a NON-vetoed model, e.g. two models
   outside Late/All disagreeing with no Late/All verdict present at
   all — see the "conflict" vs "veto" tests below, which exercise both
   branches deliberately with different ``win_models`` configs.
3. ``prematch_index`` is carried on ``Ctx`` for logging only (per the
   plan, the 35-feature prematch model is explicitly not applied to
   ML-dispatch decisions); ``evaluate`` never reads it.

Optional time cap: ``ML_DISPATCH_MAX_GAME_TIME`` (seconds; unset/empty/<=0 =
no cap, the historical behavior). When set and ``ctx.game_time`` exceeds it,
the win market produces no decisions for either side — a single
``Skipped(market="win", side=None, reason="too_late", ...)`` with the
game_time and cap in ``detail`` — while ``kills_window``/``kills_total`` are
unaffected (they have their own deadline logic via ``kills_windows_open``).
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
from typing import List, Optional, Sequence, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]

SIDES = ("Radiant", "Dire")
VETO_MODELS = ("late", "all")
DEFAULT_WIN_MODELS = ("late", "all", "early_win", "early_nw")
ALLOWED_WIN_MODELS = ("late", "all", "early_win", "early_nw")
KILLS_EARLY_MODELS = ("early_nw", "early_win")

REASON_BELOW_THRESHOLD = "below_threshold"
REASON_VETO = "veto"
REASON_CONFLICT = "conflict"
REASON_NO_UNDERDOG = "no_underdog"
REASON_TIMING_WAIT = "timing_wait"
REASON_DEDUP = "dedup"
REASON_MODEL_MISSING = "model_missing"
REASON_TOO_LATE = "too_late"


def _other_side(side: str) -> str:
    return "Dire" if side == "Radiant" else "Radiant"


@dataclass(frozen=True)
class ModelVerdict:
    """One model's current reading. ``side`` is strictly "Radiant"/"Dire"."""

    side: str
    confidence: float

    def __post_init__(self):
        if self.side not in SIDES:
            raise ValueError(f"ModelVerdict.side must be Radiant/Dire, got {self.side!r}")


@dataclass
class Ctx:
    """Everything :func:`evaluate` needs for one tick of one map.

    ``early_nw``, ``early_win``, ``late``, ``all``, ``lane`` are each
    ``Optional[ModelVerdict]``; ``None`` means "model did not vote"
    (surfaces as ``model_missing`` in ``skipped``, where relevant).
    ``kills_windows_open`` lists labels of currently-open kill windows,
    nearest one first (empty list => no ``kills_window`` decision this
    tick, but ``kills_total`` is unaffected).
    """

    match_key: object
    base_url: str
    map_num: int
    game_time: Optional[float]
    radiant_team: str
    dire_team: str
    heroes: object
    elo_radiant: Optional[float]
    elo_dire: Optional[float]
    early_nw: Optional[ModelVerdict] = None
    early_win: Optional[ModelVerdict] = None
    late: Optional[ModelVerdict] = None
    all: Optional[ModelVerdict] = None
    lane: Optional[ModelVerdict] = None
    prematch_index: Optional[float] = None
    kills_windows_open: List[str] = field(default_factory=list)
    already_sent: Optional[Set[Tuple]] = None

    def team_name(self, side: str) -> str:
        return self.radiant_team if side == "Radiant" else self.dire_team

    def model(self, name: str) -> Optional[ModelVerdict]:
        return getattr(self, name)


@dataclass(frozen=True)
class Config:
    """Read from env at call time (never cached) so tests/systemd overrides apply."""

    min_conf: float = 0.60
    underdog_min_diff: float = 50.0
    win_models: Tuple[str, ...] = DEFAULT_WIN_MODELS
    kills_require_all: bool = False
    timing_seconds: float = 600.0
    min_odds_margin: float = 0.0
    sent_path: str = "runtime/ml_dispatch_sent.json"
    max_game_time: Optional[float] = None

    @classmethod
    def from_env(cls, env: Optional[dict] = None) -> "Config":
        env = os.environ if env is None else env

        def _float(name, default):
            try:
                return float(env.get(name, default))
            except (TypeError, ValueError):
                return float(default)

        raw_models = env.get("ML_DISPATCH_WIN_MODELS", ",".join(DEFAULT_WIN_MODELS))
        win_models = tuple(
            token.strip() for token in raw_models.split(",")
            if token.strip() in ALLOWED_WIN_MODELS
        ) or DEFAULT_WIN_MODELS
        raw_max_game_time = env.get("ML_DISPATCH_MAX_GAME_TIME")
        max_game_time = None
        if raw_max_game_time not in (None, ""):
            try:
                parsed = float(raw_max_game_time)
            except (TypeError, ValueError):
                parsed = 0.0
            max_game_time = parsed if parsed > 0 else None
        return cls(
            min_conf=_float("ML_DISPATCH_MIN_CONF", 0.60),
            underdog_min_diff=_float("ML_DISPATCH_UNDERDOG_MIN_DIFF", 50.0),
            win_models=win_models,
            kills_require_all=str(env.get("ML_DISPATCH_KILLS_REQUIRE_ALL", "0")) == "1",
            timing_seconds=_float("ML_DISPATCH_TIMING_SECONDS", 600.0),
            min_odds_margin=_float("ML_DISPATCH_MIN_ODDS_MARGIN", 0.0),
            sent_path=str(env.get("ML_DISPATCH_SENT_PATH", "runtime/ml_dispatch_sent.json")),
            max_game_time=max_game_time,
        )

    def resolved_sent_path(self) -> Path:
        path = Path(self.sent_path)
        return path if path.is_absolute() else ROOT / path


@dataclass
class Decision:
    market: str  # "win" | "kills_window" | "kills_total"
    target_side: str
    target_team: str
    rule: str
    models_for: List[str]
    models_against: List[str]
    timing: str  # "now" | "wait_600"
    expected_wr: float
    min_odds: float
    reasons: List[str]


@dataclass
class Skipped:
    market: str
    side: Optional[str]
    reason: str
    detail: str


@dataclass
class EvalResult:
    decisions: List[Decision]
    skipped: List[Skipped]
    underdog_side: Optional[str]
    elo_diff: float
    mode_hint: str


def _min_odds(expected_wr: float, cfg: Config) -> float:
    denom = max(expected_wr - cfg.min_odds_margin, 1e-6)
    return round(1.0 / denom, 2)


def _dedup_key(ctx: Ctx, market: str, side: str) -> Tuple:
    return (ctx.base_url, ctx.map_num, market, side)


def _timing_for_win(ctx: Ctx, cfg: Config, target_side: str) -> str:
    if ctx.lane is not None and ctx.lane.side == target_side and ctx.lane.confidence >= cfg.min_conf:
        return "now"
    if ctx.game_time is not None and ctx.game_time >= cfg.timing_seconds:
        return "now"
    return "wait_600"


def _underdog(ctx: Ctx, cfg: Config) -> Tuple[Optional[str], float]:
    if ctx.elo_radiant is None or ctx.elo_dire is None:
        return None, 0.0
    diff = float(ctx.elo_radiant) - float(ctx.elo_dire)
    if abs(diff) < cfg.underdog_min_diff:
        return None, diff
    return ("Dire" if diff > 0 else "Radiant"), diff


def _evaluate_win(ctx: Ctx, cfg: Config) -> Tuple[List[Decision], List[Skipped]]:
    decisions: List[Decision] = []
    skipped: List[Skipped] = []

    if (
        cfg.max_game_time is not None
        and ctx.game_time is not None
        and ctx.game_time > cfg.max_game_time
    ):
        skipped.append(Skipped(
            "win", None, REASON_TOO_LATE,
            f"game_time={ctx.game_time} > max_game_time={cfg.max_game_time}",
        ))
        return decisions, skipped

    configured = [name for name in cfg.win_models if name in ALLOWED_WIN_MODELS]
    present_any = any(ctx.model(name) is not None for name in configured)

    support = {}
    for side in SIDES:
        support[side] = [
            name for name in configured
            if ctx.model(name) is not None
            and ctx.model(name).side == side
            and ctx.model(name).confidence >= cfg.min_conf
        ]

    # Veto is resolved per side FIRST, conflict SECOND (правка 0): a side
    # only "survives" if it has support AND no Late/All veto against it.
    vetoers = {}
    for side in SIDES:
        vetoers[side] = [
            name for name in VETO_MODELS
            if ctx.model(name) is not None
            and ctx.model(name).side == _other_side(side)
            and ctx.model(name).confidence >= cfg.min_conf
        ]
    survives = {side: bool(support[side]) and not vetoers[side] for side in SIDES}

    if survives["Radiant"] and survives["Dire"]:
        detail = (
            f"Radiant<-{support['Radiant']} Dire<-{support['Dire']}"
        )
        skipped.append(Skipped("win", None, REASON_CONFLICT, detail))
        return decisions, skipped

    for side in SIDES:
        models_for = support[side]
        if not models_for:
            reason = REASON_MODEL_MISSING if not present_any else REASON_BELOW_THRESHOLD
            skipped.append(Skipped("win", side, reason,
                                    f"no configured win model >= {cfg.min_conf} for {side}"))
            continue

        if vetoers[side]:
            skipped.append(Skipped("win", side, REASON_VETO,
                                    f"vetoed by {vetoers[side]} favoring {_other_side(side)}"))
            continue

        key = _dedup_key(ctx, "win", side)
        if ctx.already_sent is not None and key in ctx.already_sent:
            skipped.append(Skipped("win", side, REASON_DEDUP, f"key={key} already sent"))
            continue

        expected_wr = max(ctx.model(name).confidence for name in models_for)
        decisions.append(Decision(
            market="win",
            target_side=side,
            target_team=ctx.team_name(side),
            rule="win_single_model_confirm",
            models_for=list(models_for),
            models_against=[],
            timing=_timing_for_win(ctx, cfg, side),
            expected_wr=expected_wr,
            min_odds=_min_odds(expected_wr, cfg),
            reasons=[f"{name}>= {cfg.min_conf} for {side}" for name in models_for],
        ))

    return decisions, skipped


def _evaluate_kills(ctx: Ctx, cfg: Config, underdog_side: Optional[str]) -> Tuple[List[Decision], List[Skipped]]:
    decisions: List[Decision] = []
    skipped: List[Skipped] = []

    if underdog_side is None:
        skipped.append(Skipped("kills_window", None, REASON_NO_UNDERDOG, "no side is >= underdog diff"))
        skipped.append(Skipped("kills_total", None, REASON_NO_UNDERDOG, "no side is >= underdog diff"))
        return decisions, skipped

    early_present = [name for name in KILLS_EARLY_MODELS if ctx.model(name) is not None]
    early_support = [
        name for name in KILLS_EARLY_MODELS
        if ctx.model(name) is not None
        and ctx.model(name).side == underdog_side
        and ctx.model(name).confidence >= cfg.min_conf
    ]

    all_ok = True
    all_detail = ""
    if cfg.kills_require_all:
        verdict = ctx.model("all")
        all_ok = verdict is not None and verdict.side == underdog_side and verdict.confidence >= cfg.min_conf
        if verdict is None:
            all_detail = "all model missing"
        elif not all_ok:
            all_detail = f"all favors {verdict.side} not {underdog_side}"

    if not early_support or not all_ok:
        if not early_present:
            reason = REASON_MODEL_MISSING
            detail = "early_nw and early_win both missing"
        elif not early_support:
            reason = REASON_BELOW_THRESHOLD
            detail = f"no early model >= {cfg.min_conf} for underdog {underdog_side}"
        else:
            reason = REASON_BELOW_THRESHOLD
            detail = f"ML_DISPATCH_KILLS_REQUIRE_ALL set and {all_detail}"
        skipped.append(Skipped("kills_window", underdog_side, reason, detail))
        skipped.append(Skipped("kills_total", underdog_side, reason, detail))
        return decisions, skipped

    models_for = list(early_support) + (["all"] if cfg.kills_require_all else [])
    expected_wr = max(ctx.model(name).confidence for name in models_for)
    min_odds = _min_odds(expected_wr, cfg)
    target_team = ctx.team_name(underdog_side)
    reasons = [f"{name}>= {cfg.min_conf} for underdog {underdog_side}" for name in models_for]

    if ctx.kills_windows_open:
        key = _dedup_key(ctx, "kills_window", underdog_side)
        if ctx.already_sent is not None and key in ctx.already_sent:
            skipped.append(Skipped("kills_window", underdog_side, REASON_DEDUP, f"key={key} already sent"))
        else:
            decisions.append(Decision(
                market="kills_window",
                target_side=underdog_side,
                target_team=target_team,
                rule="kills_underdog_early_window",
                models_for=list(models_for),
                models_against=[],
                timing="now",
                expected_wr=expected_wr,
                min_odds=min_odds,
                reasons=reasons + [f"window={ctx.kills_windows_open[0]}"],
            ))

    key_total = _dedup_key(ctx, "kills_total", underdog_side)
    if ctx.already_sent is not None and key_total in ctx.already_sent:
        skipped.append(Skipped("kills_total", underdog_side, REASON_DEDUP, f"key={key_total} already sent"))
    else:
        decisions.append(Decision(
            market="kills_total",
            target_side=underdog_side,
            target_team=target_team,
            rule="kills_underdog_total",
            models_for=list(models_for),
            models_against=[],
            timing="now",
            expected_wr=expected_wr,
            min_odds=min_odds,
            reasons=list(reasons),
        ))

    return decisions, skipped


def evaluate(ctx: Ctx, cfg: Config) -> EvalResult:
    """Idempotent on every tick: same ``ctx``/``cfg`` -> same result.

    Callers are expected to call this once per tick per map; repeated
    calls with an unchanged ``ctx.already_sent`` will keep proposing the
    same decisions until the caller records them as sent.
    """
    underdog_side, elo_diff = _underdog(ctx, cfg)
    win_decisions, win_skipped = _evaluate_win(ctx, cfg)
    kills_decisions, kills_skipped = _evaluate_kills(ctx, cfg, underdog_side)
    return EvalResult(
        decisions=win_decisions + kills_decisions,
        skipped=win_skipped + kills_skipped,
        underdog_side=underdog_side,
        elo_diff=elo_diff,
        mode_hint="underdog" if underdog_side else "even",
    )


class SentLedger:
    """Persistent dedup ledger at ``path`` (rebuild-then-replace on save)."""

    def __init__(self, path):
        self.path = Path(path)
        self._keys: Set[Tuple] = set()

    def load(self) -> "SentLedger":
        try:
            raw = json.loads(self.path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            raw = []
        self._keys = {tuple(item) for item in raw}
        return self

    def contains(self, key: Sequence) -> bool:
        return tuple(key) in self._keys

    def add(self, key: Sequence) -> None:
        self._keys.add(tuple(key))

    def as_set(self) -> Set[Tuple]:
        return set(self._keys)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(sorted(self._keys, key=lambda item: [str(part) for part in item])))
        os.replace(tmp, self.path)
