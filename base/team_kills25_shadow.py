"""Shadow predictor and isolated sender for a target team reaching 27 map kills.

The module deliberately has no dependency on the main live dispatch policy.
It extracts pre-map draft/ELO features, optionally scores a frozen JSON
logistic artifact, appends observations to a JSONL audit log, and can send
qualified recommendations through a dedicated Telegram bot.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import threading
import time
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from statistics import median
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_PATH = (
    PROJECT_ROOT / "ml-models" / "team_kills27" / "team_kills27_shadow.json"
)
DEFAULT_LOG_PATH = PROJECT_ROOT / "runtime" / "team_kills27_shadow.jsonl"
DEFAULT_TELEGRAM_SENT_PATH = (
    PROJECT_ROOT / "runtime" / "team_kills27_telegram_sent.jsonl"
)
DEFAULT_ROSTER_HISTORY_PATH = (
    PROJECT_ROOT / "ELO" / "output" / "live_team_elo_snapshot.json"
)
SCHEMA_VERSION = "team_kills27_shadow.v2"
ROSTER_HISTORY_SCHEMA_VERSION = 2
# Живой цикл не должен парсить гигантский json синхронно: 349-МБ снимок ELO
# разворачивался в несколько ГБ и укладывал прод в своп.
DEFAULT_ROSTER_HISTORY_MAX_MB = 64
ODDS = 1.8
TARGET_KILLS = 27
ELO_GATE_THRESHOLD = 0.45
DEFAULT_ROSTER_OVERLAP = 4
DEFAULT_ROSTER_WINDOW = 30
ROSTER_PRIOR_MATCHES = 6.0
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
        "roster_patch_mean_edge_confident",
        "roster_patch_ge27_edge_confident",
        "blocks_target_count",
        "blocks_opponent_count",
        "blocks_consensus_target",
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
_TELEGRAM_SENT_KEYS: set[str] = set()
_TELEGRAM_SENT_PATHS_LOADED: set[str] = set()
# Предупреждение о массовой импутации печатается один раз на процесс: запись
# идёт на каждого кандидата, и строка на вызов залила бы лог.
_IMPUTED_WARNED = False
_DLTV_MATCH_ID_RE = re.compile(r"(?:^|/)matches/(\d+)(?:\.\d+)?(?:$|[/?#])")


def _env_enabled(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _env_enabled_any(*names: str, default: str = "0") -> bool:
    for name in names:
        if os.getenv(name) is not None:
            return _env_enabled(name, default)
    return _env_enabled(names[0], default) if names else _env_enabled("", default)


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = str(os.getenv(name, "") or "").strip()
        if value:
            return value
    return default


def _enabled() -> bool:
    return _env_enabled_any("TEAM_KILLS27_SHADOW_ENABLED", "TEAM_KILLS25_SHADOW_ENABLED")


def _number(value: Any) -> float | None:
    if isinstance(value, str):
        value = value.rstrip("*").strip()
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _positive_int(value: Any) -> int | None:
    number = _number(value)
    if number is None or number <= 0 or not number.is_integer():
        return None
    return int(number)


def _nonnegative_int(value: Any) -> int | None:
    number = _number(value)
    if number is None or number < 0 or not number.is_integer():
        return None
    return int(number)


def _timestamp_seconds(value: Any) -> float | None:
    number = _number(value)
    if number is not None:
        if number > 10_000_000_000:
            number /= 1000.0
        return number if number > 0 else None
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.timestamp()


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
    roster_kills: Mapping[str, Any] | None = None,
) -> dict[str, float | None]:
    if target_side not in {"radiant", "dire"}:
        raise ValueError(f"Unsupported target_side={target_side!r}")
    target_sign = 1.0 if target_side == "radiant" else -1.0
    elo_meta = team_elo_meta if isinstance(team_elo_meta, Mapping) else {}
    raw_diff = _number(elo_meta.get("raw_diff"))
    roster = roster_kills if isinstance(roster_kills, Mapping) else {}
    patch_games = _number(roster.get("patch_matches"))
    patch_mean = _number(roster.get("patch_mean_kills"))
    patch_ge27_rate = _number(roster.get("patch_ge27_rate"))
    roster_reliability = (
        patch_games / (patch_games + ROSTER_PRIOR_MATCHES)
        if patch_games is not None and patch_games > 0
        else None
    )
    features: dict[str, float | None] = {
        "nw_hit_count": float(nw_hit_count),
        "nw_max_wr": float(nw_max_wr),
        "elo_target_win_prob": _elo_probability(elo_meta, target_side),
        "elo_target_diff": raw_diff * target_sign if raw_diff is not None else None,
        "roster_patch_mean_edge_confident": (
            (patch_mean - TARGET_KILLS) * roster_reliability
            if patch_mean is not None and roster_reliability is not None
            else None
        ),
        "roster_patch_ge27_edge_confident": (
            (patch_ge27_rate - (1.0 / ODDS)) * roster_reliability
            if patch_ge27_rate is not None and roster_reliability is not None
            else None
        ),
    }
    aligned_by_metric: dict[str, list[float]] = {metric: [] for metric in METRICS}
    block_directions: list[int] = []
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
        if aligned_values:
            aligned_sum = sum(aligned_values)
            block_directions.append(
                1 if aligned_sum > 0 else -1 if aligned_sum < 0 else 0
            )
    target_blocks = sum(direction > 0 for direction in block_directions)
    opponent_blocks = sum(direction < 0 for direction in block_directions)
    enough_blocks = len(block_directions) >= 3
    features["blocks_target_count"] = float(target_blocks)
    features["blocks_opponent_count"] = float(opponent_blocks)
    features["blocks_consensus_target"] = float(
        enough_blocks and target_blocks == len(block_directions)
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
    return Path(
        _env_first(
            "TEAM_KILLS27_SHADOW_MODEL_PATH",
            "TEAM_KILLS25_SHADOW_MODEL_PATH",
            default=str(DEFAULT_ARTIFACT_PATH),
        )
    )


@lru_cache(maxsize=4)
def _load_artifact_cached(path_text: str, mtime_ns: int) -> dict[str, Any]:
    del mtime_ns
    payload = json.loads(Path(path_text).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("team-kills27 artifact root must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("team-kills27 artifact schema mismatch")
    if payload.get("target_kills_threshold") != TARGET_KILLS:
        raise ValueError("team-kills27 artifact target mismatch")
    if payload.get("feature_schema_hash") != FEATURE_SCHEMA_HASH:
        raise ValueError("team-kills27 artifact feature schema mismatch")
    if list(payload.get("feature_names") or []) != list(FEATURE_NAMES):
        raise ValueError("team-kills27 artifact feature order mismatch")
    return payload


def load_artifact(path: Path | None = None) -> dict[str, Any] | None:
    resolved = path or _artifact_path()
    try:
        stat = resolved.stat()
        return _load_artifact_cached(str(resolved), int(stat.st_mtime_ns))
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return None


def _roster_history_path() -> Path:
    return Path(
        _env_first(
            "TEAM_KILLS27_ROSTER_HISTORY_PATH",
            "TEAM_KILLS25_ROSTER_HISTORY_PATH",
            default=str(DEFAULT_ROSTER_HISTORY_PATH),
        )
    )


def _validate_roster_history(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("team-kills27 roster history root must be an object")
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        raise ValueError("team-kills27 roster history meta is missing")
    if meta.get("team_kills_history_schema_version") != ROSTER_HISTORY_SCHEMA_VERSION:
        raise ValueError("team-kills27 roster history schema mismatch")
    if not str(meta.get("team_kills_history_latest_patch") or "").strip():
        raise ValueError("team-kills27 roster history latest patch is missing")
    histories = payload.get("team_kills_history_by_team_id")
    if not isinstance(histories, dict):
        raise ValueError("team-kills27 roster history is missing")
    return payload


@lru_cache(maxsize=4)
def _load_roster_history_cached(path_text: str, mtime_ns: int) -> dict[str, Any]:
    del mtime_ns
    return _validate_roster_history(
        json.loads(Path(path_text).read_text(encoding="utf-8"))
    )


def _roster_history_max_bytes() -> int:
    """0 = без ограничения. Иначе файл крупнее лимита в живой путь не берём."""
    raw = _env_first(
        "TEAM_KILLS27_ROSTER_HISTORY_MAX_MB",
        "TEAM_KILLS25_ROSTER_HISTORY_MAX_MB",
        default=str(DEFAULT_ROSTER_HISTORY_MAX_MB),
    )
    try:
        limit_mb = float(str(raw).strip())
    except (TypeError, ValueError):
        limit_mb = float(DEFAULT_ROSTER_HISTORY_MAX_MB)
    return int(limit_mb * 1024 * 1024) if limit_mb > 0 else 0


def _shared_elo_snapshot(resolved: Path) -> dict[str, Any] | None:
    """Уже загруженный живым пайплайном снимок ELO — вместо второй копии.

    История килов лежит ВНУТРИ live_team_elo_snapshot.json, который
    ELO/live_team_strength держит в _SNAPSHOT_CACHE. Независимый json.load того
    же файла давал вторую копию структуры в памяти: на проде это 2×349 МБ json
    → RSS 4 ГБ + 6.8 ГБ свопа, main-thread вставал в read_text на десятки минут
    (D-state) и цикл матчей не проворачивался. Берём готовый объект.
    """
    module = sys.modules.get("live_team_strength") or sys.modules.get(
        "ELO.live_team_strength"
    )
    if module is None:
        return None
    cached = getattr(module, "_SNAPSHOT_CACHE", None)
    if not isinstance(cached, dict):
        return None
    default_path = getattr(module, "DEFAULT_SNAPSHOT_PATH", None)
    if default_path is None:
        return None
    try:
        same_file = Path(default_path).resolve() == Path(resolved).resolve()
    except OSError:
        same_file = False
    return cached if same_file else None


def load_roster_history(path: Path | None = None) -> dict[str, Any] | None:
    resolved = path or _roster_history_path()
    shared = _shared_elo_snapshot(resolved)
    if shared is not None:
        try:
            return _validate_roster_history(shared)
        except ValueError:
            return None
    try:
        stat = resolved.stat()
    except (FileNotFoundError, OSError):
        return None
    limit_bytes = _roster_history_max_bytes()
    if limit_bytes and int(stat.st_size) > limit_bytes:
        _warn_roster_history_oversized(resolved, int(stat.st_size), limit_bytes)
        return None
    try:
        return _load_roster_history_cached(str(resolved), int(stat.st_mtime_ns))
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return None


_ROSTER_HISTORY_OVERSIZED_SEEN: set[tuple[str, int]] = set()


def _warn_roster_history_oversized(path: Path, size: int, limit: int) -> None:
    key = (str(path), size)
    if key in _ROSTER_HISTORY_OVERSIZED_SEEN:
        return
    _ROSTER_HISTORY_OVERSIZED_SEEN.add(key)
    print(
        f"⚠️ team-kills27 roster history пропущена: {path} = {size / 1048576:.0f} МБ "
        f"> лимита {limit / 1048576:.0f} МБ (TEAM_KILLS27_ROSTER_HISTORY_MAX_MB). "
        "Контекст ростера будет history_unavailable."
    )


def build_roster_kills_context(
    *,
    team_id: Any,
    current_account_ids: list[int] | tuple[int, ...],
    observed_at: Any,
    current_match_id: Any,
    history_snapshot: Mapping[str, Any] | None = None,
    min_overlap: int = DEFAULT_ROSTER_OVERLAP,
    window: int = DEFAULT_ROSTER_WINDOW,
) -> dict[str, Any]:
    """Return leak-free recent kills for the same team with 4+ lineup overlap."""
    normalized_team_id = _positive_int(team_id)
    current_players = {
        player_id
        for player_id in (_positive_int(value) for value in current_account_ids)
        if player_id is not None
    }
    base = {
        "available": False,
        "reason": None,
        "team_id": normalized_team_id,
        "current_player_count": len(current_players),
        "min_overlap": int(min_overlap),
        "window": int(window),
        "matches": 0,
        "mean_kills": None,
        "median_kills": None,
        "ge27_hits": 0,
        "ge27_rate": None,
        "patch": None,
        "patch_matches": 0,
        "patch_mean_kills": None,
        "patch_median_kills": None,
        "patch_ge27_hits": 0,
        "patch_ge27_rate": None,
    }
    if normalized_team_id is None:
        return {**base, "reason": "team_id_unavailable"}
    if len(current_players) < min_overlap:
        return {**base, "reason": "current_roster_unavailable"}
    snapshot = history_snapshot if isinstance(history_snapshot, Mapping) else load_roster_history()
    if not isinstance(snapshot, Mapping):
        return {**base, "reason": "history_unavailable"}
    histories = snapshot.get("team_kills_history_by_team_id")
    if not isinstance(histories, Mapping):
        return {**base, "reason": "history_unavailable"}
    meta = snapshot.get("meta")
    meta = meta if isinstance(meta, Mapping) else {}
    latest_patch = str(meta.get("team_kills_history_latest_patch") or "").strip() or None
    base["patch"] = latest_patch
    raw_rows = histories.get(str(normalized_team_id), [])
    if not isinstance(raw_rows, list):
        return {**base, "reason": "team_history_invalid"}

    before_timestamp = _timestamp_seconds(observed_at)
    if before_timestamp is None:
        return {**base, "reason": "observed_at_unavailable"}
    excluded_match_id = _positive_int(current_match_id)
    unique_rows: dict[int, tuple[int, int, str | None]] = {}
    for raw_row in raw_rows:
        if not isinstance(raw_row, Mapping):
            continue
        match_id = _positive_int(raw_row.get("match_id"))
        timestamp = _positive_int(raw_row.get("timestamp"))
        kills = _nonnegative_int(raw_row.get("kills"))
        player_ids = {
            player_id
            for player_id in (
                _positive_int(value) for value in (raw_row.get("player_ids") or [])
            )
            if player_id is not None
        }
        if match_id is None or timestamp is None or kills is None:
            continue
        if match_id == excluded_match_id:
            continue
        if before_timestamp is not None and timestamp >= before_timestamp:
            continue
        if len(current_players.intersection(player_ids)) < min_overlap:
            continue
        if match_id in unique_rows:
            continue
        row_patch = str(raw_row.get("patch") or "").strip() or None
        unique_rows[match_id] = (timestamp, kills, row_patch)

    selected = sorted(unique_rows.values(), reverse=True)[: max(1, int(window))]
    kills_values = [kills for _, kills, _ in selected]
    if not kills_values:
        return {**base, "available": True, "reason": "no_matching_roster_maps"}
    ge27_hits = sum(kills >= TARGET_KILLS for kills in kills_values)
    patch_values = [
        kills for _, kills, patch in selected if latest_patch is not None and patch == latest_patch
    ]
    patch_ge27_hits = sum(kills >= TARGET_KILLS for kills in patch_values)
    return {
        **base,
        "available": True,
        "reason": "ok",
        "matches": len(kills_values),
        "mean_kills": sum(kills_values) / len(kills_values),
        "median_kills": float(median(kills_values)),
        "ge27_hits": ge27_hits,
        "ge27_rate": ge27_hits / len(kills_values),
        "patch_matches": len(patch_values),
        "patch_mean_kills": (
            sum(patch_values) / len(patch_values) if patch_values else None
        ),
        "patch_median_kills": (
            float(median(patch_values)) if patch_values else None
        ),
        "patch_ge27_hits": patch_ge27_hits,
        "patch_ge27_rate": (
            patch_ge27_hits / len(patch_values) if patch_values else None
        ),
    }


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
        raise ValueError("team-kills27 artifact vector length mismatch")
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


def imputed_feature_names(features: Mapping[str, Any]) -> list:
    """Признаки, которые НЕ довезены и заменены замороженной медианой.

    Условие совпадает с циклом импутации в `predict_probability`, поэтому счётчик
    описывает ровно те замены, которые реально повлияли на вероятность.
    """
    return [name for name in FEATURE_NAMES if _number(features.get(name)) is None]


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


def _telegram_sent_path() -> Path:
    return Path(
        _env_first(
            "TEAM_KILLS27_TELEGRAM_SENT_PATH",
            "TEAM_KILLS25_TELEGRAM_SENT_PATH",
            default=str(DEFAULT_TELEGRAM_SENT_PATH),
        )
    )


def _stable_match_id(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value) if value > 0 else None
    if isinstance(value, float):
        return str(int(value)) if value > 0 and value.is_integer() else None
    text = str(value).strip()
    if text.isdigit() and int(text) > 0:
        return str(int(text))
    return None


def _telegram_dedupe_key(record: Mapping[str, Any]) -> str:
    match_id = _stable_match_id(record.get("match_id"))
    if match_id:
        return f"match_id:{match_id}"
    match_key = str(record.get("match_key") or "").strip()
    match = _DLTV_MATCH_ID_RE.search(match_key)
    if match:
        return f"match_id:{int(match.group(1))}"
    return f"match_key:{match_key}"


def _load_telegram_sent_keys(path: Path, *, force: bool = False) -> None:
    resolved = str(path.resolve())
    if not force and resolved in _TELEGRAM_SENT_PATHS_LOADED:
        return
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    payload = json.loads(line)
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                if not isinstance(payload, Mapping):
                    continue
                dedupe_key = str(payload.get("dedupe_key") or "").strip()
                if not dedupe_key:
                    dedupe_key = _telegram_dedupe_key(payload)
                if dedupe_key not in {"", "match_key:"}:
                    _TELEGRAM_SENT_KEYS.add(dedupe_key)
    except FileNotFoundError:
        pass
    _TELEGRAM_SENT_PATHS_LOADED.add(resolved)


def _claim_telegram_send(path: Path, record: Mapping[str, Any]) -> bool:
    """Durably claim a map before calling Telegram (at-most-once delivery)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    dedupe_key = _telegram_dedupe_key(record)
    _load_telegram_sent_keys(path)
    if dedupe_key in _TELEGRAM_SENT_KEYS:
        return False
    lock_path = path.with_name(f"{path.name}.lock")
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        try:
            import fcntl

            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        except (ImportError, OSError):
            pass
        _load_telegram_sent_keys(path, force=True)
        if dedupe_key in _TELEGRAM_SENT_KEYS:
            return False
        payload = {
            "dedupe_key": dedupe_key,
            "match_key": str(record.get("match_key") or ""),
            "match_id": record.get("match_id"),
            "claimed_at": int(time.time()),
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_json_safe(payload), ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        _TELEGRAM_SENT_KEYS.add(dedupe_key)
        return True


def _format_telegram_bet(record: Mapping[str, Any]) -> str:
    features = record.get("features")
    features = features if isinstance(features, Mapping) else {}
    probability = _number(record.get("ml_probability"))
    threshold = _number(record.get("ml_threshold"))
    elo_probability = _number(features.get("elo_target_win_prob"))
    roster = record.get("roster_kills")
    roster = roster if isinstance(roster, Mapping) else {}
    patch_name = str(roster.get("patch") or "текущий патч")
    patch_matches = int(roster.get("patch_matches") or 0)
    patch_mean = _number(roster.get("patch_mean_kills"))
    patch_median = _number(roster.get("patch_median_kills"))
    patch_ge27_hits = int(roster.get("patch_ge27_hits") or 0)
    patch_ge27_rate = _number(roster.get("patch_ge27_rate"))
    if (
        patch_mean is not None
        and patch_median is not None
        and patch_ge27_rate is not None
    ):
        roster_line = (
            f"Ростер 4+ · {patch_name}: {patch_matches} карт, ср. {patch_mean:.1f}, "
            f"медиана {patch_median:.1f}, 27+ {patch_ge27_hits}/{patch_matches} "
            f"({patch_ge27_rate:.0%})"
        )
    elif roster.get("available"):
        roster_line = f"Ростер 4+ · {patch_name}: {patch_matches} карт, статистики пока нет"
    else:
        roster_line = "Ростер 4+: источник статистики недоступен"
    hit_metrics = ", ".join(str(item) for item in record.get("nw_hit_metrics") or [])
    lines = [
        "🎯 СТАВКА · TEAM KILLS ≥27",
        f"{record.get('target_team_name')}: 27+ убийств",
        (
            f"Матч: {record.get('target_team_name')} — "
            f"{record.get('opponent_team_name')}"
        ),
        (
            f"ML: {probability:.1%} (порог {threshold:.1%})"
            if probability is not None and threshold is not None
            else "ML: n/a"
        ),
        (
            f"Early NW: WR{int(record.get('nw_max_wr') or 0)}, "
            f"hits {int(record.get('nw_hit_count') or 0)}"
        ),
        f"Метрики: {hit_metrics or 'n/a'}",
        roster_line,
        (
            f"ELO target: {elo_probability:.1%}"
            if elo_probability is not None
            else "ELO target: n/a"
        ),
        f"Сегмент: {record.get('tier_segment') or 'n/a'}",
        f"Расчётный кэф: {float(record.get('odds') or ODDS):.2f}",
    ]
    if record.get("match_id") not in (None, "", 0, "0"):
        lines.append(f"match_id: {record.get('match_id')}")
    return "\n".join(lines)


def _post_telegram_message(
    *,
    bot_token: str,
    chat_id: str,
    text: str,
) -> tuple[bool, str | None]:
    try:
        import requests

        response = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=(3.0, 8.0),
        )
        try:
            payload = response.json()
        except (TypeError, ValueError):
            payload = {}
        if response.status_code == 200 and bool(payload.get("ok")):
            return True, None
        description = str(payload.get("description") or "").strip()
        return False, f"http_{response.status_code}:{description[:160]}"
    except Exception as exc:
        # Never include the exception text: requests errors can contain the URL,
        # and the Bot API URL embeds the token.
        return False, f"request_{type(exc).__name__}"


def _maybe_send_telegram_bet(
    record: Mapping[str, Any],
) -> dict[str, Any]:
    if not _env_enabled_any("TEAM_KILLS27_TELEGRAM_ENABLED", "TEAM_KILLS25_TELEGRAM_ENABLED"):
        return {"enabled": False, "eligible": False, "sent": False}

    probability = _number(record.get("ml_probability"))
    artifact_threshold = _number(record.get("ml_threshold"))
    threshold = _number(
        _env_first(
            "TEAM_KILLS27_TELEGRAM_MIN_PROBABILITY",
            "TEAM_KILLS25_TELEGRAM_MIN_PROBABILITY",
        )
    )
    if threshold is None:
        threshold = artifact_threshold
    min_wr = _number(
        _env_first("TEAM_KILLS27_TELEGRAM_MIN_WR", "TEAM_KILLS25_TELEGRAM_MIN_WR", default="60")
    ) or 60.0
    roster = record.get("roster_kills")
    roster = roster if isinstance(roster, Mapping) else {}
    model_eligible = bool(
        probability is not None
        and threshold is not None
        and probability >= threshold
        and float(record.get("nw_max_wr") or 0) >= min_wr
    )
    # Roster/patch statistics are model features, not hand-tuned cutoffs.
    # Fail closed only when the feature source itself is unavailable.
    roster_eligible = bool(roster.get("available"))
    eligible = model_eligible and roster_eligible
    if not eligible:
        if not model_eligible:
            reason = "model_gate"
        else:
            reason = str(roster.get("reason") or "roster_history_unavailable")
        return {
            "enabled": True,
            "eligible": False,
            "sent": False,
            "reason": reason,
            "threshold": threshold,
            "min_wr": min_wr,
        }

    token = _env_first("TEAM_KILLS27_TELEGRAM_BOT_TOKEN", "TEAM_KILLS25_TELEGRAM_BOT_TOKEN")
    chat_id = _env_first("TEAM_KILLS27_TELEGRAM_CHAT_ID", "TEAM_KILLS25_TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return {
            "enabled": True,
            "eligible": True,
            "sent": False,
            "reason": "config_missing",
            "threshold": threshold,
            "min_wr": min_wr,
        }

    sent_path = _telegram_sent_path()
    if not _claim_telegram_send(sent_path, record):
        return {
            "enabled": True,
            "eligible": True,
            "sent": False,
            "reason": "already_sent",
            "threshold": threshold,
            "min_wr": min_wr,
        }

    sent, error = _post_telegram_message(
        bot_token=token,
        chat_id=chat_id,
        text=_format_telegram_bet(record),
    )
    return {
        "enabled": True,
        "eligible": True,
        "sent": sent,
        "reason": error,
        "threshold": threshold,
        "min_wr": min_wr,
    }


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
    target_account_ids: list[int] | tuple[int, ...] = (),
    roster_history_snapshot: Mapping[str, Any] | None = None,
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
        roster_kills = build_roster_kills_context(
            team_id=target_team_id,
            current_account_ids=target_account_ids,
            observed_at=observed_at,
            current_match_id=match_id,
            history_snapshot=roster_history_snapshot,
        )
        features = build_features(
            metrics_payload=metrics_payload,
            team_elo_meta=team_elo_meta,
            target_side=target_side,
            nw_hit_count=nw_hit_count,
            nw_max_wr=nw_max_wr,
            roster_kills=roster_kills,
        )
        selected_artifact = dict(artifact) if isinstance(artifact, Mapping) else load_artifact()
        ml_probability = None
        if selected_artifact:
            try:
                ml_probability = predict_probability(features, selected_artifact)
            except (TypeError, ValueError, OverflowError):
                ml_probability = None
        elo_probability = _number(features.get("elo_target_win_prob"))
        imputed = imputed_feature_names(features)
        record = {
            "schema_version": SCHEMA_VERSION,
            "recorded_at": int(time.time()),
            "observed_at": _timestamp_seconds(observed_at),
            "match_key": normalized_key,
            "match_id": match_id,
            "target_side": target_side,
            "target_team_id": target_team_id,
            "target_team_name": target_team_name,
            "opponent_team_id": opponent_team_id,
            "opponent_team_name": opponent_team_name,
            "tier_segment": tier_segment,
            "target_kills_threshold": TARGET_KILLS,
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
            "roster_kills": roster_kills,
            "features": features,
            "imputed_feature_count": len(imputed),
            "imputed_features": imputed,
        }
        global _IMPUTED_WARNED
        if not _IMPUTED_WARNED and len(imputed) * 2 >= len(FEATURE_NAMES):
            _IMPUTED_WARNED = True
            print(
                f"⚠️ team_kills27: {len(imputed)} из {len(FEATURE_NAMES)} признаков "
                f"не довезено и заменено замороженной медианой "
                f"(первые: {', '.join(imputed[:5])}) — вероятность считается "
                "преимущественно по медианам",
                flush=True,
            )
        try:
            record["telegram"] = _maybe_send_telegram_bet(record)
        except Exception as exc:
            record["telegram"] = {
                "enabled": True,
                "eligible": True,
                "sent": False,
                "reason": f"internal_{type(exc).__name__}",
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
    global _IMPUTED_WARNED
    with _WRITE_LOCK:
        _RECORDED_KEYS.clear()
        _TELEGRAM_SENT_KEYS.clear()
        _TELEGRAM_SENT_PATHS_LOADED.clear()
    _IMPUTED_WARNED = False
    _load_artifact_cached.cache_clear()
    _load_roster_history_cached.cache_clear()
