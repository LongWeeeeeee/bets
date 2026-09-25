"""Fail-open live series ledger and the series correction of model B total_55_50.

Pair linkage can conflate two series of the same teams within four hours.
First seen is when the bot first observed a map, not its exact start time.
"""

from __future__ import annotations

import dataclasses
import json
import math
import os
import threading
import time
from pathlib import Path
from typing import Any

# Fitted in runtime/artifacts/kills/series_tempo/fit_correction.json (E-331 §8).
A_LEVEL = 0.20196088234669715
A = 0.23966387274514095
BETA = 0.009956373642896407
MU = 63.4258321612752
_WRITE_INTERVAL = 30.0
_KEEP_SECONDS = 48 * 60 * 60
_DEFAULT_LINK_WINDOW_S = 4 * 60 * 60
_DEFAULT_PATH = Path(__file__).resolve().parents[1] / "runtime" / "series_tempo_ledger.json"
_lock = threading.Lock()
_ledger: dict[str, dict[str, dict[str, Any]]] = {}
_loaded_path: Path | None = None
_last_write = 0.0


def _enabled() -> bool:
    return os.getenv("SERIES_TEMPO_SHADOW", "1") != "0"


def _path() -> Path:
    return Path(os.getenv("SERIES_TEMPO_LEDGER", str(_DEFAULT_PATH))).expanduser()


def _positive_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return result if result > 0 else 0


def _series_key(entry: dict[str, Any]) -> tuple[str, int] | tuple[str, int, int] | None:
    series_id = _positive_int(entry.get("series_id"))
    if series_id:
        return "sid", series_id
    radiant_id = _positive_int(entry.get("radiant_team_id"))
    dire_id = _positive_int(entry.get("dire_team_id"))
    if radiant_id and dire_id:
        low, high = sorted((radiant_id, dire_id))
        return "pair", low, high
    return None


def _storage_key(series_key: tuple[str, int] | tuple[str, int, int]) -> str:
    # Preserve the existing on-disk keys for known series IDs.
    return str(series_key[1]) if series_key[0] == "sid" else f"pair:{series_key[1]}:{series_key[2]}"


def _link_window_s() -> float:
    try:
        seconds = float(os.getenv("SERIES_TEMPO_LINK_WINDOW_S", str(_DEFAULT_LINK_WINDOW_S)))
        return seconds if math.isfinite(seconds) and seconds >= 0 else _DEFAULT_LINK_WINDOW_S
    except (TypeError, ValueError, OverflowError):
        return _DEFAULT_LINK_WINDOW_S


def _finite_number(value: Any) -> bool:
    # A row with a non-numeric timestamp would make every prune pass raise and
    # silence the shadow for good, so such rows are dropped on load.
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value))


def _load(path: Path) -> None:
    global _ledger, _loaded_path, _last_write
    if _loaded_path == path:
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        # The old ledger was keyed by SourceTV game number and has no observation
        # interval. Its order cannot be reconstructed, so rebuild from new polls.
        _ledger = {
            key: {mid: row for mid, row in maps.items()
                  if isinstance(row, dict) and str(_positive_int(row.get("match_id"))) == mid
                  and all(_finite_number(row.get(field))
                          for field in ("first_seen_ts", "last_seen_ts", "kills"))}
            for key, maps in raw.items() if isinstance(maps, dict)
        } if isinstance(raw, dict) else {}
    except (OSError, ValueError):
        _ledger = {}
    _loaded_path = path
    _last_write = 0.0


def observe(entries_dict: dict[str, Any], now: float | None = None) -> None:
    """Remember latest live scores; one atomic disk write per 30 s or new map."""
    if not _enabled() or not isinstance(entries_dict, dict):
        return
    try:
        moment = float(time.time() if now is None else now)
        if not math.isfinite(moment):
            return
        with _lock:
            path = _path()
            _load(path)
            new_map = False
            changed = False
            for key, entry in entries_dict.items():
                if not isinstance(entry, dict):
                    continue
                series_key = _series_key(entry)
                match_id = _positive_int(entry.get("match_id") or key)
                if not (series_key and match_id):
                    continue
                try:
                    source_ts = float(entry.get("timestamp") or moment)
                except (TypeError, ValueError, OverflowError):
                    continue
                if not math.isfinite(source_ts) or moment - source_ts > 300:
                    continue
                try:
                    radiant = int(entry["radiant_score"])
                    dire = int(entry["dire_score"])
                    game_time = float(entry["game_time"])
                except (KeyError, TypeError, ValueError, OverflowError):
                    continue
                if min(radiant, dire) < 0 or not math.isfinite(game_time):
                    continue
                ledger_key = _storage_key(series_key)
                maps = _ledger.setdefault(ledger_key, {})
                slot = str(match_id)
                old = maps.get(slot)
                if old is not None and moment < float(old.get("last_seen_ts") or 0):
                    continue
                new_map |= old is None
                maps[slot] = {
                    "match_id": match_id, "kills": radiant + dire,
                    "game_time": game_time,
                    "series_type": _positive_int(entry.get("series_type")) or None,
                    "sourcetv_game_number": _positive_int(entry.get("series_game_number")) or None,
                    "first_seen_ts": old["first_seen_ts"] if old else moment,
                    "last_seen_ts": moment,
                }
                changed = True
            for series_id, maps in list(_ledger.items()):
                if not isinstance(maps, dict) or not maps or all(
                    moment - float(row.get("last_seen_ts") or 0) > _KEEP_SECONDS
                    for row in maps.values() if isinstance(row, dict)
                ):
                    del _ledger[series_id]
                    changed = True
            global _last_write
            if changed and (new_map or moment - _last_write >= _WRITE_INTERVAL):
                path.parent.mkdir(parents=True, exist_ok=True)
                temporary = Path(str(path) + ".tmp")
                temporary.write_text(json.dumps(_ledger, separators=(",", ":")), encoding="utf-8")
                os.replace(temporary, path)
                _last_write = moment
    except Exception:  # shadow must never affect the caller
        return


def _lookup(match_id: Any) -> tuple[int | None, int | None, list[int], str] | None:
    """Find this match and maps last seen before its first observation."""
    if not _enabled():
        return None
    try:
        mid = _positive_int(match_id)
        if not mid:
            return None
        with _lock:
            _load(_path())
            candidate = None
            for series_key, maps in _ledger.items():
                if not isinstance(maps, dict):
                    continue
                for stored_mid, row in maps.items():
                    if stored_mid == str(mid) and isinstance(row, dict):
                        rank = (not series_key.startswith("pair:"), float(row.get("last_seen_ts") or 0))
                        if candidate is None or rank > candidate[0]:
                            candidate = rank, series_key, maps, row
            if candidate is None:
                return None
            _, series_key, maps, row = candidate
            game = _positive_int(row.get("sourcetv_game_number")) or None
            current_ts = float(row["first_seen_ts"])
            current_type = _positive_int(row.get("series_type"))
            link = "pair" if series_key.startswith("pair:") else "sid"
            link_window = _link_window_s()
            previous = []
            for previous_mid, previous_row in maps.items():
                if previous_mid == str(mid) or not isinstance(previous_row, dict):
                    continue
                if float(previous_row["first_seen_ts"]) >= current_ts:
                    continue
                age = current_ts - float(previous_row["last_seen_ts"])
                previous_type = _positive_int(previous_row.get("series_type"))
                if not -60 <= age <= link_window:
                    continue
                if current_type and previous_type and current_type != previous_type:
                    continue
                if "kills" in previous_row:
                    previous.append(int(previous_row["kills"]))
            series_id = _positive_int(series_key) or None
            return series_id, game, previous, link
    except Exception:
        pass
    return None


def lookup(match_id: Any) -> tuple[int | None, int | None, list[int]] | None:
    """Return series ID, reported game number and earlier kills."""
    found = _lookup(match_id)
    return found[:3] if found is not None else None


def shadow(p: float, match_id: Any, model: str | None = None) -> dict[str, Any] | None:
    """Return prospective probabilities without changing the served verdict."""
    found = _lookup(match_id)
    if found is None:
        return None
    try:
        series_id, game, previous, link = found
        probability = float(p)
        if not math.isfinite(probability):
            return None
        clipped = min(max(probability, 1e-6), 1 - 1e-6)
        logit = math.log(clipped / (1 - clipped))
        sigmoid = lambda x: 1 / (1 + math.exp(-x))
        mean = sum(previous) / len(previous) if previous else None
        continuation = "ledger" if previous else "sourcetv_game_number" if game and game >= 2 else None
        return {"series_id": series_id, "link": link, "game_number": game,
                "sourcetv_game_number": game, "continuation_source": continuation,
                "n_prev": len(previous), "prev_total_mean": mean,
                "p_level": sigmoid(logit + A_LEVEL) if continuation else probability,
                "p_tempo": sigmoid(logit + A + BETA * (mean - MU)) if mean is not None else None,
                "source": "sourcetv_ledger", "model": model}
    except Exception:
        return None


def _apply_enabled() -> bool:
    return os.getenv("SERIES_TEMPO_APPLY", "1") != "0"


def serve(verdict: Any, spec: Any, correction: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    """Serve the series-corrected probability of model B's verdict (owner order 25.09.2026).

    Tempo when earlier maps of the series are in the ledger, level when only the SourceTV
    game number says continuation. First maps, A fallbacks and draft-gated verdicts stay
    unchanged. Side, the OK mark, band and fair odds are recomputed from the verdict's
    own spec as in ml_panel.evaluate. SERIES_TEMPO_APPLY=0 returns to the journal-only
    shadow. The original probability stays in the returned info as p_raw.
    """
    info = {**correction, "applied": False}
    try:
        if not _apply_enabled():
            return verdict, {**info, "skip": "switch_off"}
        if correction.get("model") != "B_kv3":
            return verdict, {**info, "skip": "not_b"}
        if correction.get("p_tempo") is not None:
            probability, variant = float(correction["p_tempo"]), "tempo"
        elif correction.get("continuation_source"):
            probability, variant = float(correction["p_level"]), "level"
        else:
            return verdict, {**info, "skip": "first_map"}
        if spec is None or getattr(spec, "key", None) != verdict.key:
            return verdict, {**info, "skip": "spec_unavailable"}
        if verdict.draft_share is not None or verdict.blocked:
            return verdict, {**info, "skip": "draft_gate"}
        if not (math.isfinite(probability) and 0.0 < probability < 1.0):
            return verdict, {**info, "skip": "invalid_probability"}
        import ml_panel

        confidence = probability if probability >= 0.5 else 1.0 - probability
        band = spec.band_hit(confidence)
        served = dataclasses.replace(
            verdict, probability=probability,
            side=spec.positive if probability >= 0.5 else spec.negative,
            ok=bool(confidence >= spec.threshold and verdict.fill >= ml_panel.MIN_FILL),
            band_hit=None if band is None else band[0],
            band_n=0 if band is None else band[1],
            odds=spec.fair_odds(confidence))
        return served, {**info, "applied": True, "variant": variant,
                        "p_raw": verdict.probability}
    except Exception:  # never break the panel; keep the served verdict
        return verdict, {**info, "applied": False, "skip": "error"}
