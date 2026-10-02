"""E-347 decision-minute model: P(Radiant win) at game minute 10 and 31.

Display-only serving (no bets). Pure: reads one small JSON artifact
(`data/minute_model/minute_model_v1.json`, schema "minute-model-v1"), no
network, no sklearn. The head sees the game state and the four served draft
model outputs; the stack adds the pre-map variant-A ELO:

    p = sigmoid(((x - mean) / scale) . coef + intercept)     head first, then stack

Head features: [nw_thousands, kill_diff, kill_total, logit_all, logit_late,
logit_early_win, logit_enw_rad]; stack features: [logit_head, logit_elo]. Every
logit of a probability clips to `probability_clip` first.

Live verdict semantics (verified against the code, see `docs/CODE_MAP.md`):

- late / early_win / early_nw verdicts are ``{side, probability, confidence}``
  where ``probability`` is ALREADY P(Radiant) for either side and
  ``confidence = max(p, 1-p)`` (late_win_model.py, early_win_model.py,
  early_nw_win_model.py ``verdict``). Never take ``1 - p`` for a Dire verdict.
- Early NW ``probability`` is the conditional P(Radiant | an early NW marker
  exists) = P_R / (P_R + P_D) of the factored 3-class model.
- All is NOT a verdict dict here: ``p_all = 0.5 + win_index_draft / 100``
  (``win_model_veto.win_index_draft`` returns (P(Radiant) - 0.5) * 100),
  UNSHIFTED by the hero-pool correction the dispatch ``all`` verdict applies.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import threading
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

SCHEMA = "minute-model-v1"
DEFAULT_PATH = Path(__file__).resolve().parent.parent / "data/minute_model/minute_model_v1.json"

HEAD_FEATURES = ("nw_thousands", "kill_diff", "kill_total", "logit_all",
                 "logit_late", "logit_early_win", "logit_enw_rad")
STACK_FEATURES = ("logit_head", "logit_elo")

_lock = threading.Lock()
_cache: Dict[str, Any] = {}


def enabled() -> bool:
    """Kill switch read at call time: MINUTE_MODEL_ENABLED=0 turns everything off."""
    return os.getenv("MINUTE_MODEL_ENABLED", "1") == "1"


def model_path() -> Path:
    raw = os.getenv("MINUTE_MODEL_PATH")
    return Path(raw) if raw else DEFAULT_PATH


def reset() -> None:
    """Forget the loaded artifact (tests, hot reload)."""
    with _lock:
        _cache.clear()


def _check_part(part: Any, features: tuple) -> dict:
    if not isinstance(part, dict) or tuple(part.get("features") or ()) != features:
        raise ValueError("unexpected feature list")
    n = len(features)
    for key in ("mean", "scale", "coef"):
        values = part.get(key)
        if not isinstance(values, list) or len(values) != n:
            raise ValueError(f"{key} length != {n}")
        if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in values):
            raise ValueError(f"{key} not finite")
    if any(v == 0 for v in part["scale"]):
        raise ValueError("zero scale")
    if not math.isfinite(float(part["intercept"])):
        raise ValueError("intercept not finite")
    return part


def _load(path: Path) -> Optional[dict]:
    """Lazy, thread-safe, cached per path. None = unusable (the reason is in `load_error`)."""
    key = str(path)
    with _lock:
        if key in _cache:
            return _cache[key].get("model")
        entry: Dict[str, Any] = {"model": None, "error": None}
        try:
            raw = path.read_bytes()
            data = json.loads(raw.decode("utf-8"))
            if data.get("schema") != SCHEMA:
                raise ValueError(f"schema {data.get('schema')!r} != {SCHEMA!r}")
            clip = data["probability_clip"]
            lo, hi = float(clip[0]), float(clip[1])
            if not (0.0 < lo < hi < 1.0):
                raise ValueError("bad probability_clip")
            minutes = {}
            for minute, spec in data["minutes"].items():
                minutes[int(minute)] = {
                    "head": _check_part(spec["head"], HEAD_FEATURES),
                    "stack": _check_part(spec["stack"], STACK_FEATURES),
                }
            if not minutes:
                raise ValueError("no minutes")
            entry["model"] = {"minutes": minutes, "clip": (lo, hi),
                              "sha256": hashlib.sha256(raw).hexdigest(), "path": key}
        except Exception as exc:                      # noqa: BLE001 - any breakage = disabled
            entry["error"] = f"{type(exc).__name__}: {exc}"
        _cache[key] = entry
        return entry["model"]


def load_error(path: Optional[Path] = None) -> Optional[str]:
    path = path or model_path()
    _load(path)
    with _lock:
        return _cache.get(str(path), {}).get("error")


def minutes_available() -> tuple:
    model = _load(model_path())
    return tuple(sorted(model["minutes"])) if model else ()


def _finite(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _logit(p: float, clip: tuple) -> float:
    p = min(max(p, clip[0]), clip[1])
    return math.log(p / (1.0 - p))


def _sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def _apply(part: dict, x: list) -> float:
    z = float(part["intercept"])
    for value, mean, scale, coef in zip(x, part["mean"], part["scale"], part["coef"]):
        z += (value - mean) / scale * coef
    return _sigmoid(z)


def radiant_probability(verdict: Any) -> Optional[float]:
    """P(Radiant) from a live ``{side, probability, confidence}`` verdict, or None.

    ``probability`` is already P(Radiant) (never inverted for Dire). Only when a
    dict carries side+confidence but no probability (the dispatch-view shape)
    P(Radiant) is rebuilt from the named side's confidence.
    """
    if not isinstance(verdict, Mapping):
        return None
    p = _finite(verdict.get("probability"))
    if p is not None:
        return p if 0.0 <= p <= 1.0 else None
    side = verdict.get("side")
    conf = _finite(verdict.get("confidence"))
    if side not in ("Radiant", "Dire") or conf is None or not 0.5 <= conf <= 1.0:
        return None
    return conf if side == "Radiant" else 1.0 - conf


def all_probability_from_index(index: Any) -> Optional[float]:
    """p_all from ``win_model_veto.win_index_draft`` ((P(Radiant) - 0.5) * 100)."""
    value = _finite(index)
    if value is None or not -50.0 <= value <= 50.0:
        return None
    return 0.5 + value / 100.0


def predict(minute: int, *, nw_lead: Any, radiant_kills: Any, dire_kills: Any,
            p_all: Any, p_late: Any, p_early_win: Any, p_enw_rad: Any,
            p_elo: Any = None) -> Optional[dict]:
    """``{p_head, p_stack|None, features, model_sha256, minute}`` or None.

    None when disabled, the artifact is unusable, the minute is unknown, or any
    required input (everything except ``p_elo``) is missing / non-finite / not
    a probability. A missing or invalid ``p_elo`` only drops the stack.
    """
    if not enabled():
        return None
    model = _load(model_path())
    if model is None:
        return None
    spec = model["minutes"].get(int(minute))
    if spec is None:
        return None
    nw = _finite(nw_lead)
    rk = _finite(radiant_kills)
    dk = _finite(dire_kills)
    probs = [_finite(v) for v in (p_all, p_late, p_early_win, p_enw_rad)]
    if nw is None or rk is None or dk is None or any(p is None or not 0.0 <= p <= 1.0 for p in probs):
        return None
    clip = model["clip"]
    features = {
        "nw_thousands": nw / 1000.0,
        "kill_diff": rk - dk,
        "kill_total": rk + dk,
        "logit_all": _logit(probs[0], clip),
        "logit_late": _logit(probs[1], clip),
        "logit_early_win": _logit(probs[2], clip),
        "logit_enw_rad": _logit(probs[3], clip),
    }
    p_head = _apply(spec["head"], [features[name] for name in HEAD_FEATURES])
    p_stack = None
    elo = _finite(p_elo)
    if elo is not None and 0.0 <= elo <= 1.0:
        features["logit_head"] = _logit(p_head, clip)
        features["logit_elo"] = _logit(elo, clip)
        p_stack = _apply(spec["stack"], [features[name] for name in STACK_FEATURES])
    return {"minute": int(minute), "p_head": p_head, "p_stack": p_stack,
            "features": features, "model_sha256": model["sha256"]}
