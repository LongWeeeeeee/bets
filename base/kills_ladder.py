"""Per-side kills ladder from the served panel B "side >= 30" probability (E-352).

Display only. The card shows, per side, a median kill count and
P(side kills >= L) for the lines in ``display_lines``; P(side kills >= L) is the
price of the individual team total over (L-1),5, printed as ``ИТБ{L-1},5``.

Math (``ml-models/prematch_panel_kv3/ladder.json``, schema ``kills-ladder-v1``)::

    x   = clip(logit(p_own), -clip, +clip)      # p = calibrated served B verdict
    y   = clip(logit(p_opp), -clip, +clip)
    P_L = sigmoid(a_L + b_L * x + shift + gamma_gap * clip(x - y, +-gap_clip))
    P_L := min(P_L, P_{L-1})                    # running min, non-increasing in L
    median N = largest L in the table with P_L >= 0.5;
               if none: the smallest table line minus 1 (floored at 0)

``correction.gap_clip`` (optional) bounds the logit gap x - y before ``gamma_gap``:
outside the live 5th..95th percentile of gaps the linear term is extrapolation.
Display edges: median at the largest table line renders ``≈N+``, below the
smallest renders ``≤N``.

Everything is fail-open: any problem returns None and the caller drops only the
ladder lines. ``ML_PANEL_KILLS_LADDER=0`` switches the display off.
"""
from __future__ import annotations

import json
import math
import os
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "kills-ladder-v1"
_lock = threading.Lock()
_cache = {"key": None, "table": None}


def enabled() -> bool:
    return os.getenv("ML_PANEL_KILLS_LADDER", "1") != "0"


def _directory() -> Path:
    # The same directory the B serving reads (kv3_panel_serving.py KV3_PANEL_DIR).
    return Path(os.getenv("KV3_PANEL_DIR", str(ROOT / "ml-models/prematch_panel_kv3")))


def _parse(payload):
    if not isinstance(payload, dict) or payload.get("schema") != SCHEMA:
        raise ValueError("kills ladder schema differs")
    lines = {}
    for key, pair in payload["lines"].items():
        a, b = float(pair[0]), float(pair[1])
        if not (math.isfinite(a) and math.isfinite(b)):
            raise ValueError("non-finite ladder coefficient")
        lines[int(key)] = (a, b)
    if not lines:
        raise ValueError("empty ladder")
    correction = payload["correction"]
    shift, gamma = float(correction["shift"]), float(correction["gamma_gap"])
    clip = float(payload["clip_logit"])
    gap_clip = correction.get("gap_clip")
    gap_clip = None if gap_clip is None else float(gap_clip)
    if gap_clip is not None and not (math.isfinite(gap_clip) and gap_clip > 0):
        raise ValueError("invalid gap_clip")
    display = tuple(int(v) for v in payload["display_lines"])
    if not (math.isfinite(shift) and math.isfinite(gamma) and math.isfinite(clip)
            and clip > 0) or not display or any(v not in lines for v in display):
        raise ValueError("invalid ladder parameters")
    return {"lines": tuple(sorted(lines.items())), "shift": shift, "gamma": gamma,
            "clip": clip, "gap_clip": gap_clip, "display": display}


def load():
    """Parsed ladder table or None. Cached per (path, mtime, size)."""
    path = _directory() / "ladder.json"
    try:
        stat = path.stat()
        key = (str(path), stat.st_mtime_ns, stat.st_size)
    except OSError:
        return None
    with _lock:
        if _cache["key"] == key:
            return _cache["table"]
        try:
            table = _parse(json.loads(path.read_text(encoding="utf-8")))
        except Exception:  # noqa: BLE001 - display helper must not raise
            table = None
        _cache["key"], _cache["table"] = key, table
        return table


def _logit(p: float, clip: float) -> float:
    p = min(max(float(p), 1e-12), 1.0 - 1e-12)
    return max(-clip, min(clip, math.log(p / (1.0 - p))))


def _sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def probabilities(p_own: float, p_opp: float, table=None):
    """({L: P(side kills >= L)}, median N) or None. Non-increasing in L."""
    table = table or load()
    if table is None:
        return None
    p_own, p_opp = float(p_own), float(p_opp)
    if not (math.isfinite(p_own) and math.isfinite(p_opp)):
        return None
    x = _logit(p_own, table["clip"])
    y = _logit(p_opp, table["clip"])
    gap = x - y
    if table.get("gap_clip") is not None:
        gap = max(-table["gap_clip"], min(table["gap_clip"], gap))
    out, running = {}, 1.0
    for line, (a, b) in table["lines"]:
        running = min(running, _sigmoid(a + b * x + table["shift"]
                                        + table["gamma"] * gap))
        out[line] = running
    reached = [line for line, p in out.items() if p >= 0.5]
    median = max(reached) if reached else max(min(out) - 1, 0)
    return out, median


def plural_kills(n: int) -> str:
    """Russian: 1/21/31 кил, 2-4/22-24 кила, else килов (11-14 килов)."""
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        return "килов"
    if n % 10 == 1:
        return "кил"
    if 2 <= n % 10 <= 4:
        return "кила"
    return "килов"


def render_line(label: str, p_own: float, p_opp: float):
    """'Radiant ≈24 кила · ИТБ15,5 75% · ИТБ20,5 60% · ИТБ25,5 46%' or None."""
    try:
        table = load()
        result = probabilities(p_own, p_opp, table)
        if result is None:
            return None
        probs, median = result
        if median >= max(probs):
            head = f"{label} ≈{median}+ {plural_kills(median)}"
        elif median < min(probs):
            head = f"{label} ≤{median} {plural_kills(median)}"
        else:
            head = f"{label} ≈{median} {plural_kills(median)}"
        bits = [head]
        bits += [f"ИТБ{line - 1},5 {probs[line] * 100:.0f}%" for line in table["display"]]
        return " · ".join(bits)
    except Exception:  # noqa: BLE001
        return None
