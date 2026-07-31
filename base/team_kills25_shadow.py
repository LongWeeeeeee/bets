"""Non-sending shadow predictor for a target team reaching 25 map kills.

The module deliberately has no dependency on the live dispatch policy.  It
extracts pre-map draft/ELO features, optionally scores a frozen JSON logistic
artifact, and appends one observation per map to a JSONL audit log.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_PATH = (
    PROJECT_ROOT / "ml-models" / "team_kills25" / "team_kills25_shadow.json"
)
DEFAULT_LOG_PATH = PROJECT_ROOT / "runtime" / "team_kills25_shadow.jsonl"
SCHEMA_VERSION = "team_kills25_shadow.v1"
ODDS = 1.8
ELO_GATE_THRESHOLD = 0.45
METRICS = (
    "counterpick_1vs1",
    "counterpick_1vs2",
    "pos1_vs_pos1",
    "solo",
    "synergy_duo",
    "synergy_trio",
)
BLOCK_SOURCES = {
    "early_nw": ("early_output",),
    "early_win": ("early_end_output",),
    "late": ("mid_output", "late_output"),
    "all": ("all_output", "post_lane_output"),
}
BLOCK_AGGREGATES = (
    "agree_count",
    "disagree_count",
    "aligned_sum",
    "abs_sum",
    "abs_max",
)
CROSS_AGGREGATES = ("same_sign", "target_blocks", "opponent_blocks")


def _feature_names() -> tuple[str, ...]:
    names = [
        "nw_hit_count",
        "nw_max_wr",
        "elo_target_win_prob",
        "elo_target_diff",
    ]
    names.extend(
        f"{block}_{metric}"
        for block in BLOCK_SOURCES
        for metric in METRICS
    )
    names.extend(
        f"{block}_{metric}_abs"
        for block in BLOCK_SOURCES
        for metric in METRICS
    )
    names.extend(
        f"{block}_{aggregate}"
        for block in BLOCK_SOURCES
        for aggregate in BLOCK_AGGREGATES
    )
    names.extend(
        f"cross_{metric}_{aggregate}"
        for metric in METRICS
        for aggregate in CROSS_AGGREGATES
    )
    return tuple(names)


FEATURE_NAMES = _feature_names()
FEATURE_SCHEMA_HASH = hashlib.sha256("\n".join(FEATURE_NAMES).encode("utf-8")).hexdigest()

_WRITE_LOCK = threading.Lock()
_RECORDED_KEYS: set[str] = set()


def _enabled() -> bool:
    return str(os.getenv("TEAM_KILLS25_SHADOW_ENABLED", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _number(value: Any) -> float | None:
    if isinstance(value, str):
        value = value.rstrip("*").strip()
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _block(payload: Mapping[str, Any], aliases: tuple[str, ...]) -> Mapping[str, Any]:
    for alias in aliases:
        value = payload.get(alias)
        if isinstance(value, Mapping):
            return value
    return {}


def _elo_probability(team_elo_meta: Mapping[str, Any], target_side: str) -> float | None:
    key = "raw_radiant_wr" if target_side == "radiant" else "raw_dire_wr"
    value = _number(team_elo_meta.get(key))
    if value is None:
        return None
    if value > 1.0:
        value /= 100.0
    return min(max(value, 0.0), 1.0)


def build_features(
    *,
    metrics_payload: Mapping[str, Any],
    team_elo_meta: Mapping[str, Any] | None,
    target_side: str,
    nw_hit_count: int,
    nw_max_wr: int,
) -> dict[str, float | None]:
    if target_side not in {"radiant", "dire"}:
        raise ValueError(f"Unsupported target_side={target_side!r}")
    target_sign = 1.0 if target_side == "radiant" else -1.0
    elo_meta = team_elo_meta if isinstance(team_elo_meta, Mapping) else {}
    raw_diff = _number(elo_meta.get("raw_diff"))
    features: dict[str, float | None] = {
        "nw_hit_count": float(nw_hit_count),
        "nw_max_wr": float(nw_max_wr),
        "elo_target_win_prob": _elo_probability(elo_meta, target_side),
        "elo_target_diff": raw_diff * target_sign if raw_diff is not None else None,
    }
    aligned_by_metric: dict[str, list[float]] = {metric: [] for metric in METRICS}
    for block_name, aliases in BLOCK_SOURCES.items():
        source = _block(metrics_payload, aliases)
        aligned_values: list[float] = []
        for metric in METRICS:
            raw = _number(source.get(metric))
            aligned = raw * target_sign if raw is not None else None
            features[f"{block_name}_{metric}"] = aligned
            features[f"{block_name}_{metric}_abs"] = (
                abs(aligned) if aligned is not None else None
            )
            if aligned is not None:
                aligned_values.append(aligned)
                if aligned != 0.0:
                    aligned_by_metric[metric].append(aligned)
        features[f"{block_name}_agree_count"] = float(
            sum(value > 0 for value in aligned_values)
        )
        features[f"{block_name}_disagree_count"] = float(
            sum(value < 0 for value in aligned_values)
        )
        features[f"{block_name}_aligned_sum"] = float(sum(aligned_values))
        features[f"{block_name}_abs_sum"] = float(
            sum(abs(value) for value in aligned_values)
        )
        features[f"{block_name}_abs_max"] = float(
            max((abs(value) for value in aligned_values), default=0.0)
        )
    for metric, values in aligned_by_metric.items():
        features[f"cross_{metric}_same_sign"] = float(
            bool(values)
            and (all(value > 0 for value in values) or all(value < 0 for value in values))
        )
        features[f"cross_{metric}_target_blocks"] = float(
            sum(value > 0 for value in values)
        )
        features[f"cross_{metric}_opponent_blocks"] = float(
            sum(value < 0 for value in values)
        )
    return {name: features.get(name) for name in FEATURE_NAMES}


def _artifact_path() -> Path:
    return Path(os.getenv("TEAM_KILLS25_SHADOW_MODEL_PATH", str(DEFAULT_ARTIFACT_PATH)))


@lru_cache(maxsize=4)
def _load_artifact_cached(path_text: str, mtime_ns: int) -> dict[str, Any]:
    del mtime_ns
    payload = json.loads(Path(path_text).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("team-kills25 artifact root must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("team-kills25 artifact schema mismatch")
    if payload.get("feature_schema_hash") != FEATURE_SCHEMA_HASH:
        raise ValueError("team-kills25 artifact feature schema mismatch")
    if list(payload.get("feature_names") or []) != list(FEATURE_NAMES):
        raise ValueError("team-kills25 artifact feature order mismatch")
    return payload


def load_artifact(path: Path | None = None) -> dict[str, Any] | None:
    resolved = path or _artifact_path()
    try:
        stat = resolved.stat()
        return _load_artifact_cached(str(resolved), int(stat.st_mtime_ns))
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return None


def predict_probability(
    features: Mapping[str, Any],
    artifact: Mapping[str, Any],
) -> float:
    model = artifact.get("model") if isinstance(artifact.get("model"), Mapping) else {}
    medians = model.get("medians") or []
    means = model.get("means") or []
    scales = model.get("scales") or []
    coefficients = model.get("coefficients") or []
    if not all(len(values) == len(FEATURE_NAMES) for values in (medians, means, scales, coefficients)):
        raise ValueError("team-kills25 artifact vector length mismatch")
    score = float(model.get("intercept") or 0.0)
    for index, name in enumerate(FEATURE_NAMES):
        value = _number(features.get(name))
        if value is None:
            value = float(medians[index])
        scale = float(scales[index])
        standardized = (value - float(means[index])) / (scale if scale > 0 else 1.0)
        score += standardized * float(coefficients[index])
    if score >= 0:
        return 1.0 / (1.0 + math.exp(-score))
    exp_score = math.exp(score)
    return exp_score / (1.0 + exp_score)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def record_shadow_observation(
    *,
    match_key: str,
    match_id: Any,
    observed_at: Any,
    target_side: str,
    target_team_id: Any,
    target_team_name: str,
    opponent_team_id: Any,
    opponent_team_name: str,
    tier_segment: str,
    nw_hit_count: int,
    nw_max_wr: int,
    nw_hit_metrics: list[str] | tuple[str, ...],
    metrics_payload: Mapping[str, Any],
    team_elo_meta: Mapping[str, Any] | None,
    artifact: Mapping[str, Any] | None = None,
    log_path: Path | None = None,
) -> dict[str, Any] | None:
    if not _enabled():
        return None
    normalized_key = str(match_key or match_id or "").strip()
    if not normalized_key:
        return None
    with _WRITE_LOCK:
        if normalized_key in _RECORDED_KEYS:
            return None
        features = build_features(
            metrics_payload=metrics_payload,
            team_elo_meta=team_elo_meta,
            target_side=target_side,
            nw_hit_count=nw_hit_count,
            nw_max_wr=nw_max_wr,
        )
        selected_artifact = dict(artifact) if isinstance(artifact, Mapping) else load_artifact()
        ml_probability = None
        if selected_artifact:
            try:
                ml_probability = predict_probability(features, selected_artifact)
            except (TypeError, ValueError, OverflowError):
                ml_probability = None
        elo_probability = _number(features.get("elo_target_win_prob"))
        record = {
            "schema_version": SCHEMA_VERSION,
            "recorded_at": int(time.time()),
            "observed_at": _number(observed_at),
            "match_key": normalized_key,
            "match_id": match_id,
            "target_side": target_side,
            "target_team_id": target_team_id,
            "target_team_name": target_team_name,
            "opponent_team_id": opponent_team_id,
            "opponent_team_name": opponent_team_name,
            "tier_segment": tier_segment,
            "odds": ODDS,
            "nw_hit_count": int(nw_hit_count),
            "nw_max_wr": int(nw_max_wr),
            "nw_hit_metrics": [str(metric) for metric in nw_hit_metrics],
            "elo_gate_threshold": ELO_GATE_THRESHOLD,
            "elo_gate_eligible": bool(
                elo_probability is not None and elo_probability >= ELO_GATE_THRESHOLD
            ),
            "ml_probability": ml_probability,
            "ml_threshold": (
                _number(selected_artifact.get("bet_threshold"))
                if selected_artifact
                else None
            ),
            "artifact_created_at": (
                selected_artifact.get("created_at_utc") if selected_artifact else None
            ),
            "features": features,
        }
        output = log_path or Path(
            os.getenv("TEAM_KILLS25_SHADOW_LOG_PATH", str(DEFAULT_LOG_PATH))
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_json_safe(record), ensure_ascii=False) + "\n")
            handle.flush()
        _RECORDED_KEYS.add(normalized_key)
        return record


def reset_shadow_state_for_tests() -> None:
    with _WRITE_LOCK:
        _RECORDED_KEYS.clear()
    _load_artifact_cached.cache_clear()
