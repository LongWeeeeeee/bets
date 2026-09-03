from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
import time
from bisect import bisect_right
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.config import HybridEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier, MatchRecord
from ELO.models import HybridPlayerRosterEloModel
from ELO.series_data import build_series_bundles
from ELO.team_identity import resolve_org_key
from ELO.tiering import attach_league_tiers, classify_leagues, get_known_team_tier

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

SECONDS_PER_DAY = 24 * 60 * 60
LEADERBOARD_BASELINE = 1500.0
DEFAULT_ACTIVE_CUTOFF_DAYS = 180.0
DEFAULT_DISPLAY_DECAY_HALF_LIFE_DAYS = 120.0
DEFAULT_PLAYER_ONLY_FALLBACK_ROSTER_MATCHES = 3
TEAM_KILLS_HISTORY_SCHEMA_VERSION = 2
TEAM_KILLS_HISTORY_MATCHES_PER_TEAM = 100
DEFAULT_DATA_DIR = (
    Path(__file__).resolve().parents[1]
    / "pro_heroes_data"
    / "json_parts_split_from_object"
)
DEFAULT_SNAPSHOT_PATH = Path(__file__).resolve().parent / "output" / "live_team_elo_snapshot.json"
DEFAULT_RUNTIME_PROGRESS_PATH = Path(__file__).resolve().parents[1] / "runtime" / "live_elo_progress.json"
DEFAULT_RUNTIME_MODEL_STATE_PATH = Path(__file__).resolve().parents[1] / "runtime" / "live_elo_model_state.json"
#: Живые обновления как ДЕЛЬТА поверх базовых массивов снимка (E-255).
#: Прежнее полное состояние (`live_elo_model_state.json`, 519 МБ) перезаписывалось
#: целиком после каждой карты ради ~70-100 изменившихся значений и требовало
#: разбора (+1.28 ГБ) и словарной модели (+1.31 ГБ) в живом процессе.
DEFAULT_LIVE_DELTA_PATH = Path(__file__).resolve().parents[1] / "runtime" / "live_elo_delta.json"
DEFAULT_RUNTIME_LOCK_PATH = Path(__file__).resolve().parents[1] / "runtime" / "live_elo_state.lock"
DEFAULT_LIVE_SEGMENT_POLICY_PATH = Path(__file__).resolve().parent / "live_probability_segment_policy.json"

#: Маркер «живого» снимка: значение — путь к дельте. Ставится в `_snapshot_with_runtime_model_state`,
#: читается в `_restore_model_from_snapshot`.
LIVE_DELTA_MARKER = "__live_delta_path__"

_SNAPSHOT_CACHE: dict[str, Any] | None = None
# Кэш восстановленных моделей: ДВА слота, а не один, и ключ — состояние, из
# которого модель строится, а не снимок-обёртка.
#
# Один слот с ключом `id(snapshot)` промахивался в ста процентах случаев.
# `get_matchup_summary` намеренно строит сводку дважды — от базового снимка и от
# мерженого с рантайм-состоянием, чтобы показать, насколько рейтинг сдвинулся за
# турнир, — и слот на каждом шаге вытеснялся соседним. Замер: три вызова дали
# шесть восстановлений, каждое `cache_hit=False`, по 7.5-8 секунд. Это же
# объясняло, почему процесс жёг больше ядра непрерывно, и растило RSS с 4 ГБ до
# 6.2: около гигабайта объектов выделялось и освобождалось на каждый запрос, а
# арены аллокатора обратно системе не отдаются.
#
# Ссылка на само состояние хранится рядом с моделью НАМЕРЕННО: без неё словарь
# мог бы освободиться, а его `id` достаться другому объекту — и кэш отдал бы
# чужую модель.
_MODEL_CACHE_SLOTS = 2
_MODEL_FROM_SNAPSHOT_CACHE: list[tuple[int, Any, Any]] = []
_RUNTIME_SNAPSHOT_CACHE: dict[str, Any] = {"base_snapshot_id": None, "runtime_signature": None, "snapshot": None}
_LIVE_PROBABILITY_POLICY_CACHE: dict[str, Any] = {"path": None, "signature": None, "policy": None}
_LEADERBOARD_RANK_CACHE: dict[str, Any] = {"table_id": None, "table_ref": None,
                                            "rank_map": None}

SEGMENT_OVERALL = "overall"
SEGMENT_TIER1_ONLY = "tier1_only"
SEGMENT_TIER2_ONLY = "tier2_only"
SEGMENT_TIER1_VS_TIER2 = "tier1_vs_tier2"
SEGMENT_OTHER = "other"


def _timestamp_to_iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _decay_strength_for_leaderboard(raw_strength: float, days_inactive: float, half_life_days: float) -> float:
    if half_life_days <= 0 or days_inactive <= 0:
        return raw_strength
    keep_factor = math.pow(0.5, days_inactive / half_life_days)
    return LEADERBOARD_BASELINE + (raw_strength - LEADERBOARD_BASELINE) * keep_factor


def _elo_probability(rating_diff: float, scale: float) -> float:
    return 1.0 / (1.0 + math.pow(10.0, -rating_diff / scale))


def _elo_diff_from_probability(probability: float, scale: float) -> float:
    p = min(0.99, max(0.01, probability))
    return scale * math.log10(p / (1.0 - p))


def _known_team_segment(
    team_a_id: int | None,
    team_a_name: str,
    team_b_id: int | None,
    team_b_name: str,
) -> str:
    team_a_tier = get_known_team_tier(team_a_id, team_a_name)
    team_b_tier = get_known_team_tier(team_b_id, team_b_name)
    if team_a_tier == LeagueTier.TIER1 and team_b_tier == LeagueTier.TIER1:
        return SEGMENT_TIER1_ONLY
    if team_a_tier == LeagueTier.TIER2 and team_b_tier == LeagueTier.TIER2:
        return SEGMENT_TIER2_ONLY
    if {team_a_tier, team_b_tier} == {LeagueTier.TIER1, LeagueTier.TIER2}:
        return SEGMENT_TIER1_VS_TIER2
    return SEGMENT_OTHER


def _live_probability_policy_signature(path: Path) -> tuple[bool, int]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return False, 0
    return True, int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))


def _load_live_probability_segment_policy(
    policy_path: Path = DEFAULT_LIVE_SEGMENT_POLICY_PATH,
) -> dict[str, Any] | None:
    exists, signature = _live_probability_policy_signature(policy_path)
    cached_policy = _LIVE_PROBABILITY_POLICY_CACHE.get("policy")
    if (
        _LIVE_PROBABILITY_POLICY_CACHE.get("path") == str(policy_path)
        and _LIVE_PROBABILITY_POLICY_CACHE.get("signature") == signature
        and isinstance(cached_policy, dict)
    ):
        return cached_policy
    if not exists:
        _LIVE_PROBABILITY_POLICY_CACHE["path"] = str(policy_path)
        _LIVE_PROBABILITY_POLICY_CACHE["signature"] = signature
        _LIVE_PROBABILITY_POLICY_CACHE["policy"] = None
        return None
    payload = _load_json_dict(policy_path)
    if not isinstance(payload, dict):
        _LIVE_PROBABILITY_POLICY_CACHE["path"] = str(policy_path)
        _LIVE_PROBABILITY_POLICY_CACHE["signature"] = signature
        _LIVE_PROBABILITY_POLICY_CACHE["policy"] = None
        return None
    segments = payload.get("segments")
    policy = segments if isinstance(segments, dict) else payload
    if not isinstance(policy, dict):
        _LIVE_PROBABILITY_POLICY_CACHE["path"] = str(policy_path)
        _LIVE_PROBABILITY_POLICY_CACHE["signature"] = signature
        _LIVE_PROBABILITY_POLICY_CACHE["policy"] = None
        return None
    _LIVE_PROBABILITY_POLICY_CACHE["path"] = str(policy_path)
    _LIVE_PROBABILITY_POLICY_CACHE["signature"] = signature
    _LIVE_PROBABILITY_POLICY_CACHE["policy"] = policy
    return policy


def _favorite_score_from_variant(
    *,
    favorite_strength: float,
    underdog_strength: float,
    variant: str,
) -> float | None:
    favorite_strength = float(max(favorite_strength, 1.0))
    underdog_strength = float(max(underdog_strength, 1.0))
    abs_diff = favorite_strength - underdog_strength
    avg_strength = max(1.0, (favorite_strength + underdog_strength) / 2.0)
    pct_gap_avg_pp = 100.0 * abs_diff / avg_strength
    pct_gap_fav_pp = 100.0 * abs_diff / favorite_strength
    ratio_gap_pp = 100.0 * (favorite_strength / underdog_strength - 1.0)
    log_ratio_points = 400.0 * math.log10(favorite_strength / underdog_strength)
    if variant == "abs_diff":
        return abs_diff
    if variant == "pct_gap_avg_pp":
        return pct_gap_avg_pp
    if variant == "pct_gap_fav_pp":
        return pct_gap_fav_pp
    if variant == "ratio_gap_pp":
        return ratio_gap_pp
    if variant == "log_ratio_points":
        return log_ratio_points
    if variant.startswith("blend_avg_k"):
        try:
            coef = float(variant.removeprefix("blend_avg_k"))
        except ValueError:
            return None
        return abs_diff + coef * pct_gap_avg_pp
    if variant.startswith("blend_fav_k"):
        try:
            coef = float(variant.removeprefix("blend_fav_k"))
        except ValueError:
            return None
        return abs_diff + coef * pct_gap_fav_pp
    return None


def _monotonic_bucket_probs(values: list[float]) -> list[float]:
    monotonic: list[float] = []
    current = 0.5
    for raw_value in values:
        current = max(current, float(raw_value))
        monotonic.append(min(0.99, current))
    return monotonic


def _apply_live_probability_policy(
    *,
    radiant_rating: float,
    dire_rating: float,
    radiant_team_id: int | None,
    radiant_team_name: str,
    dire_team_id: int | None,
    dire_team_name: str,
    elo_scale: float,
    policy_path: Path = DEFAULT_LIVE_SEGMENT_POLICY_PATH,
) -> dict[str, Any]:
    direct_radiant_win_prob = _elo_probability(radiant_rating - dire_rating, elo_scale)
    segment = _known_team_segment(radiant_team_id, radiant_team_name, dire_team_id, dire_team_name)
    policy = _load_live_probability_segment_policy(policy_path)
    policy_segment = segment
    segment_policy = (policy or {}).get(segment)
    if not isinstance(segment_policy, dict):
        policy_segment = SEGMENT_OVERALL
        segment_policy = (policy or {}).get(SEGMENT_OVERALL)
    if not isinstance(segment_policy, dict):
        return {
            "radiant_win_prob": direct_radiant_win_prob,
            "dire_win_prob": 1.0 - direct_radiant_win_prob,
            "direct_radiant_win_prob": direct_radiant_win_prob,
            "direct_dire_win_prob": 1.0 - direct_radiant_win_prob,
            "probability_segment": segment,
            "probability_policy_segment": None,
            "probability_mode": "direct",
            "probability_variant": None,
            "favorite_probability": max(direct_radiant_win_prob, 1.0 - direct_radiant_win_prob),
            "favorite_score": None,
            "policy_applied": False,
        }

    mode = str(segment_policy.get("mode") or "direct").strip() or "direct"
    if mode == "direct_series_prob":
        return {
            "radiant_win_prob": direct_radiant_win_prob,
            "dire_win_prob": 1.0 - direct_radiant_win_prob,
            "direct_radiant_win_prob": direct_radiant_win_prob,
            "direct_dire_win_prob": 1.0 - direct_radiant_win_prob,
            "probability_segment": segment,
            "probability_policy_segment": policy_segment,
            "probability_mode": mode,
            "probability_variant": None,
            "favorite_probability": max(direct_radiant_win_prob, 1.0 - direct_radiant_win_prob),
            "favorite_score": None,
            "policy_applied": True,
        }

    variant = str(segment_policy.get("variant") or "").strip()
    score: float | None = None
    bucket_probs: list[float] = []
    edges: list[float] = []
    favorite_is_radiant = radiant_rating >= dire_rating
    favorite_strength = max(radiant_rating, dire_rating)
    underdog_strength = min(radiant_rating, dire_rating)
    if variant:
        score = _favorite_score_from_variant(
            favorite_strength=favorite_strength,
            underdog_strength=underdog_strength,
            variant=variant,
        )
    raw_edges = segment_policy.get("edges")
    raw_bucket_probs = segment_policy.get("bucket_probs") or segment_policy.get("smoothed_probs")
    if isinstance(raw_edges, list):
        edges = [float(value) for value in raw_edges]
    if isinstance(raw_bucket_probs, list):
        bucket_probs = _monotonic_bucket_probs([float(value) for value in raw_bucket_probs])
    if score is None or not bucket_probs:
        return {
            "radiant_win_prob": direct_radiant_win_prob,
            "dire_win_prob": 1.0 - direct_radiant_win_prob,
            "direct_radiant_win_prob": direct_radiant_win_prob,
            "direct_dire_win_prob": 1.0 - direct_radiant_win_prob,
            "probability_segment": segment,
            "probability_policy_segment": policy_segment,
            "probability_mode": "direct_fallback",
            "probability_variant": variant or None,
            "favorite_probability": max(direct_radiant_win_prob, 1.0 - direct_radiant_win_prob),
            "favorite_score": score,
            "policy_applied": False,
        }
    if abs(radiant_rating - dire_rating) < 1e-9:
        return {
            "radiant_win_prob": 0.5,
            "dire_win_prob": 0.5,
            "direct_radiant_win_prob": direct_radiant_win_prob,
            "direct_dire_win_prob": 1.0 - direct_radiant_win_prob,
            "probability_segment": segment,
            "probability_policy_segment": policy_segment,
            "probability_mode": mode,
            "probability_variant": variant or None,
            "favorite_probability": 0.5,
            "favorite_score": score,
            "policy_applied": True,
        }
    bucket_idx = min(bisect_right(edges, score), len(bucket_probs) - 1)
    favorite_probability = max(0.01, min(0.99, float(bucket_probs[bucket_idx])))
    radiant_win_prob = favorite_probability if favorite_is_radiant else (1.0 - favorite_probability)
    return {
        "radiant_win_prob": radiant_win_prob,
        "dire_win_prob": 1.0 - radiant_win_prob,
        "direct_radiant_win_prob": direct_radiant_win_prob,
        "direct_dire_win_prob": 1.0 - direct_radiant_win_prob,
        "probability_segment": segment,
        "probability_policy_segment": policy_segment,
        "probability_mode": mode,
        "probability_variant": variant or None,
        "favorite_probability": favorite_probability,
        "favorite_score": score,
        "policy_applied": True,
    }


SNAPSHOT_PIN_ENV = "ELO_SNAPSHOT_PIN"


def _snapshot_is_pinned() -> bool:
    """Запрет пересобирать снапшот из-за свежих файлов корпуса.

    Снапшот, собранный на полном корпусе, переносится на прод файлом: корпус там
    меньше, и любое его пополнение делает mtime свежее снапшота — тогда
    `ensure_snapshot` молча пересоберёт рейтинги на маленьком корпусе и откатит
    перенос. Пин выключает ТОЛЬКО эту причину пересборки; структурные (нет
    model_state, устаревшая схема kills-истории) продолжают работать.
    """
    return str(os.getenv(SNAPSHOT_PIN_ENV, "")).strip().lower() in {"1", "true", "yes", "on"}


def _latest_data_mtime(data_dir: Path) -> float:
    latest = 0.0
    for json_path in data_dir.glob("*.json"):
        try:
            latest = max(latest, json_path.stat().st_mtime)
        except FileNotFoundError:
            continue
    return latest


def _coerce_match_tier(raw_tier: Any) -> LeagueTier | None:
    if isinstance(raw_tier, LeagueTier):
        return raw_tier
    if isinstance(raw_tier, str):
        value = str(raw_tier).strip().upper()
        if value in LeagueTier._value2member_map_:
            return LeagueTier(value)
        if value in {"1", "2", "3"}:
            return LeagueTier(f"TIER{value}")
        return None
    if isinstance(raw_tier, int) and raw_tier in {1, 2, 3}:
        return LeagueTier(f"TIER{raw_tier}")
    return None


def _id_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _match_id_is_series(match_id: int, series_key: str, series_id: Any = None) -> bool:
    """sourcetv пишет match_id = series_id — это не уникальный id карты."""
    mid = int(match_id or 0)
    if mid <= 0:
        return False
    series_num = _id_int(series_key) or _id_int(series_id)
    return series_num > 0 and mid == series_num


def _coerce_player_ids(raw_player_ids: Any) -> tuple[int, ...]:
    if not isinstance(raw_player_ids, (list, tuple)):
        return ()
    player_ids: list[int] = []
    for raw_player_id in raw_player_ids:
        try:
            player_id = int(raw_player_id)
        except (TypeError, ValueError):
            continue
        if player_id > 0:
            player_ids.append(player_id)
    return tuple(player_ids)


def _array_model_for_base_snapshot(snapshot_path: Path):
    """Массивная модель базового снимка — вместо полного `model_state`.

    Slim-снимок (E-251) не держит `model_state`, и раньше это означало догрузку
    всех 527 МБ (~1.7 ГБ RSS) плюс словарную модель поверх (~1.3 ГБ). И это НЕ
    редкий путь: `build_matchup_summary_from_snapshot` зовёт
    `_restore_model_from_snapshot`, а `get_matchup_summary` зовёт её дважды —
    сначала на базовом снимке (base_summary), потом на мерженом (live_summary),
    поэтому догрузка случалась в каждом процессе (замер 03.09: RSS прода 7.69 ГБ
    за час работы при двух применённых картах).

    Массивная модель даёт те же числа (в `array_model` заявлена побитовая сверка
    1800 вызовов на трёх тирах), данных в ней 74 МБ, а с sidecar-`.npz`
    (`array_model.save_state_arrays`) не тратится и минута на потоковую сборку.

    Модель ТОЛЬКО для чтения: путь саммари зовёт `preview_team_strength` и
    состояние не меняет; мутация шла бы в массивное хранилище и упала, а не
    тихо испортила числа. Мутирующий путь (`register_live_map_context`) берёт
    состояние из рантайм-payload и сюда не попадает.
    """
    try:
        from ELO.array_model import load_read_model
    except ImportError:
        try:
            from array_model import load_read_model
        except ImportError:
            return None
    try:
        return load_read_model(Path(snapshot_path), None)
    except Exception as exc:  # noqa: BLE001
        print(f"[ELO] массивная модель базового снимка не поднялась ({exc}) — "
              "иду прежним путём полного model_state", flush=True)
        return None


def _restore_model_from_snapshot(snapshot: dict[str, Any]) -> HybridPlayerRosterEloModel | None:
    if isinstance(snapshot, dict) and snapshot.get(LIVE_DELTA_MARKER):
        # Живой снимок: модель = базовые массивы + дельта. Ни разбора 519 МБ,
        # ни словарной модели на 2.5 млн записей.
        model = _live_overlay_model(snapshot)
        if model is not None:
            return model
    raw_state = snapshot.get("model_state") if isinstance(snapshot, dict) else None
    if not isinstance(raw_state, dict):
        return None
    slim_source = raw_state.get(SLIM_MODEL_STATE_MARKER)
    if slim_source:
        model = _array_model_for_base_snapshot(Path(slim_source))
        if model is not None:
            return model
        # Массивная модель недоступна (нет ijson, битый sidecar, исключение) —
        # прежний путь с полной догрузкой, чтобы саммари не пропало молча.
        raw_state = full_model_state(snapshot)
        if not isinstance(raw_state, dict):
            return None
    state_id = id(raw_state)
    for i, (cached_id, _state_ref, model) in enumerate(_MODEL_FROM_SNAPSHOT_CACHE):
        if cached_id == state_id:
            # Свежеиспользованный слот — в конец, чтобы вытеснялся давний.
            _MODEL_FROM_SNAPSHOT_CACHE.append(_MODEL_FROM_SNAPSHOT_CACHE.pop(i))
            return model if isinstance(model, HybridPlayerRosterEloModel) else None
    model = HybridPlayerRosterEloModel.from_state(raw_state)
    _MODEL_FROM_SNAPSHOT_CACHE.append((state_id, raw_state, model))
    del _MODEL_FROM_SNAPSHOT_CACHE[:-_MODEL_CACHE_SLOTS]
    return model


def _snapshot_reference_timestamp(snapshot: dict[str, Any]) -> int:
    meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
    try:
        return int(meta.get("reference_timestamp") or 0)
    except (TypeError, ValueError):
        return 0


def _runtime_file_signature(path: Path) -> tuple[bool, int]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return False, 0
    return True, int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))


#: Кэш разбора больших JSON по (mtime_ns, size) — тот же приём, что в
#: `array_model.load_read_model`. `live_elo_model_state.json` весит 519 МБ, а
#: `_load_runtime_model_payload` зовётся до трёх раз на завершённую карту
#: (`:627` в мерже снимка и `:1722`/`:1891` в ленивых билдерах модели): без кэша
#: это два-три разбора по ~2.5-3 ГБ временных словарей НА КАРТУ, и RSS не
#: возвращается — арены glibc фрагментированы (E-251).
#:
#: Возвращать один и тот же объект безопасно: `HybridPlayerRosterEloModel.from_state`
#: КОПИРУЕТ словари состояния в свои (`models.py:571-579, 657, 662`), а обратная
#: запись строит НОВЫЙ payload через `export_state()` (`:1918-1922`), то есть
#: закэшированный dict никто не мутирует. Любая запись меняет mtime/size и
#: сбрасывает попадание.
_JSON_DICT_CACHE: dict[str, tuple[int, int, dict[str, Any]]] = {}


def _load_json_dict(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    key = str(path)
    try:
        stat = path.stat()
        stamp = (int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
                 int(stat.st_size))
    except OSError:
        return None
    cached = _JSON_DICT_CACHE.get(key)
    if cached is not None and cached[0] == stamp[0] and cached[1] == stamp[1]:
        return cached[2]
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    _JSON_DICT_CACHE[key] = (stamp[0], stamp[1], payload)
    return payload


def _model_config_signature(model_state: dict[str, Any] | None) -> str:
    if not isinstance(model_state, dict):
        return ""
    config_payload = model_state.get("config")
    if not isinstance(config_payload, dict):
        return ""
    raw = json.dumps(config_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _snapshot_model_config_signature(snapshot: dict[str, Any]) -> str:
    meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
    signature = meta.get("model_config_signature")
    if isinstance(signature, str) and signature:
        return signature
    raw_state = snapshot.get("model_state")
    return _model_config_signature(raw_state if isinstance(raw_state, dict) else None)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            # `ensure_ascii=True` и без отступов — не ради красоты файла, а ради
            # памяти того, кто его читает.
            #
            # В названиях команд есть тринадцать эмодзи (🤡, 💢, 🐾 и прочие).
            # Одного символа вне BMP хватает, чтобы CPython держал ВЕСЬ файл как
            # UCS-4: замерено на боевом снимке — 365.6 МБ на диске превращаются в
            # 1462.3 МБ строки, вчетверо. С экранированием в \uXXXX строка
            # остаётся однобайтовой, и те же данные стоят 731 МБ вместо 1462.
            # Разобранный словарь от этого не меняется: escape декодируется в тот
            # же символ.
            #
            # Отступы — ещё 102 МБ, 27.9% файла, при 12.7 млн переводов строк.
            # Читать снимок глазами всё равно невозможно.
            #
            # Той же функцией пишется рантайм-состояние (219.5 МБ), так что
            # выигрыш достаётся обоим. Подпись конфига (`_model_config_signature`)
            # считается отдельно и от способа записи не зависит.
            json.dump(payload, fh, separators=(",", ":"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except FileNotFoundError:
                pass


@contextmanager
def _runtime_file_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as fh:
        if fcntl is not None:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        try:
            yield fh
        finally:
            if fcntl is not None:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _serialize_match_record(match: MatchRecord) -> dict[str, Any]:
    return {
        "match_id": int(match.match_id),
        "timestamp": int(match.timestamp),
        "radiant_team_id": match.radiant_team_id,
        "radiant_team_name": str(match.radiant_team_name),
        "dire_team_id": match.dire_team_id,
        "dire_team_name": str(match.dire_team_name),
        "radiant_player_ids": [int(player_id) for player_id in match.radiant_player_ids],
        "dire_player_ids": [int(player_id) for player_id in match.dire_player_ids],
        "league_id": match.league_id,
        "league_name": str(match.league_name),
        "source_league_tier": match.source_league_tier,
        "series_id": match.series_id,
        "series_type": match.series_type,
        "source_patch": match.source_patch,
        "derived_league_tier": match.derived_league_tier.value,
    }


def _deserialize_match_record(raw: dict[str, Any], *, radiant_win: bool) -> MatchRecord | None:
    try:
        tier = _coerce_match_tier(raw.get("derived_league_tier")) or LeagueTier.TIER3
        return MatchRecord(
            match_id=int(raw.get("match_id")),
            timestamp=int(raw.get("timestamp") or 0),
            radiant_win=bool(radiant_win),
            radiant_team_id=int(raw["radiant_team_id"]) if raw.get("radiant_team_id") is not None else None,
            radiant_team_name=str(raw.get("radiant_team_name") or ""),
            dire_team_id=int(raw["dire_team_id"]) if raw.get("dire_team_id") is not None else None,
            dire_team_name=str(raw.get("dire_team_name") or ""),
            radiant_player_ids=_coerce_player_ids(raw.get("radiant_player_ids")),
            dire_player_ids=_coerce_player_ids(raw.get("dire_player_ids")),
            league_id=int(raw["league_id"]) if raw.get("league_id") is not None else None,
            league_name=str(raw.get("league_name") or ""),
            source_league_tier=(str(raw.get("source_league_tier")) if raw.get("source_league_tier") is not None else None),
            series_id=int(raw["series_id"]) if raw.get("series_id") is not None else None,
            series_type=(str(raw.get("series_type")) if raw.get("series_type") is not None else None),
            source_patch=(str(raw.get("source_patch")) if raw.get("source_patch") is not None else None),
            derived_league_tier=tier,
        )
    except (TypeError, ValueError):
        return None


def _empty_runtime_progress(base_reference_timestamp: int, model_config_signature: str) -> dict[str, Any]:
    return {
        "base_reference_timestamp": int(base_reference_timestamp),
        "base_model_config_signature": str(model_config_signature or ""),
        "pending_series": {},
        "applied_maps": {},
    }


def _load_runtime_progress(
    *,
    base_reference_timestamp: int,
    model_config_signature: str,
    progress_path: Path,
) -> dict[str, Any]:
    payload = _load_json_dict(progress_path)
    if not isinstance(payload, dict):
        return _empty_runtime_progress(base_reference_timestamp, model_config_signature)
    try:
        payload_reference = int(payload.get("base_reference_timestamp") or 0)
    except (TypeError, ValueError):
        payload_reference = 0
    payload_signature = str(payload.get("base_model_config_signature") or "")
    if payload_reference != int(base_reference_timestamp) or payload_signature != str(model_config_signature or ""):
        return _empty_runtime_progress(base_reference_timestamp, model_config_signature)
    pending_series = payload.get("pending_series")
    applied_maps = payload.get("applied_maps")
    return {
        "base_reference_timestamp": int(base_reference_timestamp),
        "base_model_config_signature": str(model_config_signature or ""),
        "pending_series": pending_series if isinstance(pending_series, dict) else {},
        "applied_maps": applied_maps if isinstance(applied_maps, dict) else {},
    }


def _load_runtime_model_payload(
    *,
    snapshot: dict[str, Any],
    runtime_model_state_path: Path,
) -> dict[str, Any] | None:
    payload = _load_json_dict(runtime_model_state_path)
    if not isinstance(payload, dict):
        return None
    try:
        payload_reference = int(payload.get("base_reference_timestamp") or 0)
    except (TypeError, ValueError):
        return None
    if payload_reference != _snapshot_reference_timestamp(snapshot):
        return None
    payload_signature = str(payload.get("base_model_config_signature") or "")
    if payload_signature != _snapshot_model_config_signature(snapshot):
        return None
    if not isinstance(payload.get("model_state"), dict):
        return None
    return payload


def _live_delta_path() -> Path:
    env = os.getenv("LIVE_ELO_DELTA")
    return Path(env).expanduser() if env else DEFAULT_LIVE_DELTA_PATH


def _delta_is_usable(snapshot: dict[str, Any] | None, delta_path: Path) -> bool:
    """Дельта годится, только если она собрана ОТ ЭТОГО снимка.

    Те же два охранника, что у рантайм-состояния (`_load_runtime_model_payload`):
    отметка базового снимка и подпись конфигурации. Без них обновления легли бы
    на чужую базу, и получились бы рейтинги, которых никогда не существовало.
    """
    if not isinstance(snapshot, dict) or not delta_path.exists():
        return False
    try:
        from . import state_overlay
    except ImportError:
        return False
    payload = state_overlay.load_delta(
        delta_path,
        base_reference_timestamp=_snapshot_reference_timestamp(snapshot),
        base_model_config_signature=_snapshot_model_config_signature(snapshot),
    )
    return payload is not None


def _snapshot_source_path(snapshot: dict[str, Any] | None) -> Path | None:
    """Путь к снимку, от которого загружен slim-кэш (нужен массивной модели)."""
    if not isinstance(snapshot, dict):
        return None
    state = snapshot.get("model_state")
    if not isinstance(state, dict):
        return None
    source = state.get(SLIM_MODEL_STATE_MARKER)
    return Path(source) if source else None


_DELTA_MODEL_REPORTED = False


def _live_overlay_model(snapshot: dict[str, Any] | None):
    """Живая модель: базовые массивы + дельта. None, если путь недоступен.

    None означает «работай по-старому» (разбор рантайм-состояния и словарная
    модель): дельты может не быть вовсе — например, до прогона
    `ELO/convert_state_to_delta.py`. Молча потерять живые обновления было бы
    хуже, чем заплатить прежнюю память.
    """
    global _DELTA_MODEL_REPORTED
    delta_path = _live_delta_path()
    if not _delta_is_usable(snapshot, delta_path):
        return None
    source = _snapshot_source_path(snapshot)
    if source is None:
        return None
    try:
        from .array_model import build_overlay_model
        model = build_overlay_model(source, delta_path)
    except Exception as exc:  # noqa: BLE001
        print(f"[ELO] живая модель на дельте не поднялась ({exc}) — иду прежним путём",
              flush=True)
        return None
    if not _DELTA_MODEL_REPORTED:
        _DELTA_MODEL_REPORTED = True
        print(f"[ELO] живая модель на ДЕЛЬТЕ: применено {getattr(model, '_overlay_applied', 0)} "
              f"значений из {delta_path.name} поверх базовых массивов", flush=True)
    return model


def _save_live_delta(model, base_reference_timestamp: int,
                     base_model_config_signature: str) -> bool:
    """Записать живые обновления дельтой. False — если модель не overlay."""
    wrappers = getattr(model, "_overlay_wrappers", None)
    if not wrappers:
        return False
    from . import state_overlay
    from .array_model import rekey_overlay_cache

    delta_path = _live_delta_path()
    source = getattr(model, "_overlay_snapshot_path", None) or DEFAULT_SNAPSHOT_PATH
    state_overlay.save_delta(
        delta_path,
        base_reference_timestamp=int(base_reference_timestamp),
        base_model_config_signature=str(base_model_config_signature or ""),
        changes=state_overlay.collect_changes(wrappers),
        resets=state_overlay.collect_resets(wrappers),
        small_parts=state_overlay.collect_small_parts(model),
        updated_at=int(time.time()),
    )
    # Отметка дельты изменилась — перепривязываем кэш, иначе следующий вызов
    # собирал бы модель заново вместо того чтобы взять ту же.
    rekey_overlay_cache(model, Path(source), delta_path)
    return True


def _snapshot_with_runtime_model_state(
    snapshot: dict[str, Any],
    *,
    runtime_model_state_path: Path,
) -> dict[str, Any]:
    delta_path = _live_delta_path()
    delta_usable = _delta_is_usable(snapshot, delta_path)
    if delta_usable:
        exists, signature = _runtime_file_signature(delta_path)
    else:
        exists, signature = _runtime_file_signature(runtime_model_state_path)
    base_snapshot_id = id(snapshot)
    cached_snapshot = _RUNTIME_SNAPSHOT_CACHE.get("snapshot")
    if (
        _RUNTIME_SNAPSHOT_CACHE.get("base_snapshot_id") == base_snapshot_id
        and _RUNTIME_SNAPSHOT_CACHE.get("runtime_signature") == signature
        and isinstance(cached_snapshot, dict)
    ):
        return cached_snapshot

    if delta_usable:
        # Мерж больше ничего не грузит: живое состояние — это дельта (килобайты)
        # поверх базовых массивов, поэтому снимок только помечается путём к ней.
        # Прежде здесь разбирался `live_elo_model_state.json` (519 МБ, +1.28 ГБ
        # RSS) и подменялся model_state снимка.
        tagged = dict(snapshot)
        tagged[LIVE_DELTA_MARKER] = str(delta_path)
        _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = base_snapshot_id
        _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = signature
        _RUNTIME_SNAPSHOT_CACHE["snapshot"] = tagged
        return tagged

    if not exists:
        _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = base_snapshot_id
        _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = signature
        _RUNTIME_SNAPSHOT_CACHE["snapshot"] = snapshot
        return snapshot

    runtime_payload = _load_runtime_model_payload(
        snapshot=snapshot,
        runtime_model_state_path=runtime_model_state_path,
    )
    if runtime_payload is None:
        _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = base_snapshot_id
        _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = signature
        _RUNTIME_SNAPSHOT_CACHE["snapshot"] = snapshot
        return snapshot

    merged_snapshot = dict(snapshot)
    merged_meta = dict(snapshot.get("meta") or {})
    merged_meta["runtime_updated_at"] = runtime_payload.get("updated_at")
    merged_snapshot["meta"] = merged_meta
    # Без `copy.deepcopy`: `_load_runtime_model_payload` зовёт `_load_json_dict`,
    # а тот делает свежий `json.load` без всякого кэша, так что `runtime_payload`
    # — локальный объект, на который больше никто не ссылается. Копия защищала от
    # несуществующего совладельца, а стоила 435 МБ: в момент копирования оба
    # состояния живут разом, 870 МБ вместо 435. Именно до таких пиков дорастают
    # арены аллокатора, которые процесс потом не отдаёт обратно.
    merged_snapshot["model_state"] = runtime_payload["model_state"]
    _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = base_snapshot_id
    _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = signature
    _RUNTIME_SNAPSHOT_CACHE["snapshot"] = merged_snapshot
    return merged_snapshot


def _leaderboard_rank_map(snapshot: dict[str, Any]) -> dict[str, int]:
    # Ключ — таблица команд, из которой карта и считается, а НЕ снимок-обёртка.
    # Мерженый снимок делается через `dict(snapshot)`, то есть `teams_by_org_key`
    # там буквально тот же объект и результат заведомо тот же; при ключе по
    # снимку сортировка 59 389 команд выполнялась дважды за вызов впустую.
    # Ссылка на таблицу хранится рядом, иначе освободившийся `id` мог бы
    # достаться другому объекту и кэш отдал бы чужую карту.
    teams_by_org_key = snapshot.get("teams_by_org_key")
    table_id = id(teams_by_org_key)
    cached_rank_map = _LEADERBOARD_RANK_CACHE.get("rank_map")
    if (
        _LEADERBOARD_RANK_CACHE.get("table_id") == table_id
        and isinstance(cached_rank_map, dict)
    ):
        return cached_rank_map

    if not isinstance(teams_by_org_key, dict):
        _LEADERBOARD_RANK_CACHE["table_id"] = table_id
        _LEADERBOARD_RANK_CACHE["table_ref"] = teams_by_org_key
        _LEADERBOARD_RANK_CACHE["rank_map"] = {}
        return {}

    rows: list[tuple[str, float, str]] = []
    for org_key, row in teams_by_org_key.items():
        if not isinstance(row, dict):
            continue
        try:
            current_strength = float(row.get("current_strength"))
        except (TypeError, ValueError):
            continue
        rows.append((str(org_key), current_strength, str(row.get("team_name") or org_key)))
    rows.sort(key=lambda item: (-item[1], item[2].casefold()))
    rank_map = {org_key: idx + 1 for idx, (org_key, _rating, _name) in enumerate(rows)}
    _LEADERBOARD_RANK_CACHE["table_id"] = table_id
    _LEADERBOARD_RANK_CACHE["table_ref"] = teams_by_org_key
    _LEADERBOARD_RANK_CACHE["rank_map"] = rank_map
    return rank_map


# E-224: у одной серии на живом пути применялась ровно одна карта из всех
# сыгранных ("Live ELO updated from completed map" 101 против "finalized
# from orphaned finished series" 638). Причина в двух местах разом:
# `_winner_slot_from_scores` (ниже — теперь `_winner_slots_from_score_advance`)
# признавала победителя только при сдвиге счёта РОВНО на +1 у одной стороны;
# пропущенный опрос сдвигает его на 2 или даёт +1 обеим сторонам сразу, и
# тогда функция отдавала None. А дальше единственный слот `pending_map`
# перезаписывался контекстом следующей карты раньше, чем счёт успевал
# показать исход предыдущей — так недостающая карта терялась безвозвратно.
_MAX_PENDING_MAPS = 6


def _winner_slots_from_score_advance(
    previous_scores: dict[str, int],
    current_scores: dict[str, int],
) -> list[str] | None:
    """Winners of every map the series score advanced past, oldest first.

    A jump of N on exactly one side is N wins in a row for that side (a
    missed poll can advance the score by more than one map). A jump of
    exactly one map on EACH side (two maps missed, one win apiece) can't be
    told apart from the score alone, but crediting them in queue order is
    harmless: either way each side gets exactly one win, and every ELO
    update runs off the rating current at the moment it is applied, not off
    which physical map produced it — order does not change the outcome.
    Anything else (both sides advanced and at least one of them by more
    than one map) stays genuinely ambiguous: no order is safe to infer, so
    this returns None and callers fall back to other resolution (e.g. a
    direct winner lookup, or the orphan sweep in cyberscore_try.py).
    """
    delta_first = int(current_scores.get("first", 0)) - int(previous_scores.get("first", 0))
    delta_second = int(current_scores.get("second", 0)) - int(previous_scores.get("second", 0))
    if delta_first > 0 and delta_second == 0:
        return ["first"] * delta_first
    if delta_second > 0 and delta_first == 0:
        return ["second"] * delta_second
    if delta_first == 1 and delta_second == 1:
        return ["first", "second"]
    return None


def _pending_maps_list(series_state: dict[str, Any]) -> list[dict[str, Any]]:
    """Ordered queue of unresolved map contexts, oldest first.

    Reads the new `pending_maps` list; falls back to the legacy single
    `pending_map` slot so state written before this fix still loads.
    """
    raw_list = series_state.get("pending_maps")
    if isinstance(raw_list, list):
        return [item for item in raw_list if isinstance(item, dict)]
    legacy = series_state.get("pending_map")
    return [legacy] if isinstance(legacy, dict) else []


def _preview_lineup_rating_from_model(
    *,
    model: HybridPlayerRosterEloModel,
    team_id: int | None,
    team_name: str,
    player_ids: tuple[int, ...],
    player_positions: tuple[str | None, ...] | None,
    tier: LeagueTier,
    timestamp: int,
    player_only_fallback_roster_matches: int = DEFAULT_PLAYER_ONLY_FALLBACK_ROSTER_MATCHES,
) -> dict[str, Any]:
    preview = model.preview_team_strength(
        team_id=team_id,
        team_name=team_name,
        player_ids=player_ids,
        player_positions=player_positions,
        tier=tier,
        timestamp=max(int(timestamp or 0), 1),
    )
    team_strength = float(preview["team_strength"])
    player_strength = float(preview["player_strength"])
    roster_matches = int(preview["roster_matches"])
    rating_source = "lineup_team_strength"
    base_rating = team_strength
    if roster_matches < max(0, int(player_only_fallback_roster_matches)):
        base_rating = player_strength
        rating_source = "lineup_player_strength_cold_roster"
    return {
        "team_id": team_id,
        "team_name": str(team_name or ""),
        "base_rating": base_rating,
        "team_strength": team_strength,
        "player_strength": player_strength,
        "prior_blended_strength": float(preview["prior_blended_strength"]),
        "player_global_avg": float(preview["player_global_avg"]),
        "player_local_avg": float(preview["player_local_avg"]),
        "roster_rating": float(preview["roster_rating"]),
        "roster_matches": roster_matches,
        "roster_weight": float(preview["roster_weight"]),
        "roster_key": str(preview["roster_key"]),
        "rating_source": rating_source,
        "lineup_player_count": len(player_ids),
    }


def _preview_live_matchup_from_model(
    *,
    model: HybridPlayerRosterEloModel,
    match: MatchRecord,
    tier_matchup_elo_bonus: dict[str, Any] | None = None,
    elo_scale: float = 400.0,
    player_only_fallback_roster_matches: int = DEFAULT_PLAYER_ONLY_FALLBACK_ROSTER_MATCHES,
) -> dict[str, Any]:
    tier_bonus_map = tier_matchup_elo_bonus if isinstance(tier_matchup_elo_bonus, dict) else {}
    radiant_payload = _preview_lineup_rating_from_model(
        model=model,
        team_id=match.radiant_team_id,
        team_name=match.radiant_team_name,
        player_ids=match.radiant_player_ids,
        player_positions=match.radiant_player_positions or None,
        tier=match.derived_league_tier,
        timestamp=match.timestamp,
        player_only_fallback_roster_matches=player_only_fallback_roster_matches,
    )
    dire_payload = _preview_lineup_rating_from_model(
        model=model,
        team_id=match.dire_team_id,
        team_name=match.dire_team_name,
        player_ids=match.dire_player_ids,
        player_positions=match.dire_player_positions or None,
        tier=match.derived_league_tier,
        timestamp=match.timestamp,
        player_only_fallback_roster_matches=player_only_fallback_roster_matches,
    )
    radiant_known_tier = get_known_team_tier(match.radiant_team_id, match.radiant_team_name)
    dire_known_tier = get_known_team_tier(match.dire_team_id, match.dire_team_name)
    tier_gap_bonus = 0.0
    tier_gap_key: str | None = None
    if (
        radiant_known_tier is not None
        and dire_known_tier is not None
        and radiant_known_tier != dire_known_tier
    ):
        if radiant_known_tier.value < dire_known_tier.value:
            tier_gap_key = f"{radiant_known_tier.value}_vs_{dire_known_tier.value}"
            tier_gap_bonus = float((tier_bonus_map.get(tier_gap_key) or {}).get("elo_bonus", 0.0))
        else:
            tier_gap_key = f"{dire_known_tier.value}_vs_{radiant_known_tier.value}"
            tier_gap_bonus = -float((tier_bonus_map.get(tier_gap_key) or {}).get("elo_bonus", 0.0))
    radiant_rating = float(radiant_payload["base_rating"]) + (tier_gap_bonus / 2.0)
    dire_rating = float(dire_payload["base_rating"]) - (tier_gap_bonus / 2.0)
    probability_meta = _apply_live_probability_policy(
        radiant_rating=radiant_rating,
        dire_rating=dire_rating,
        radiant_team_id=match.radiant_team_id,
        radiant_team_name=match.radiant_team_name,
        dire_team_id=match.dire_team_id,
        dire_team_name=match.dire_team_name,
        elo_scale=elo_scale,
    )
    radiant_win_prob = float(probability_meta.get("radiant_win_prob", 0.5))
    radiant_payload["rating"] = radiant_rating
    dire_payload["rating"] = dire_rating
    return {
        "radiant": radiant_payload,
        "dire": dire_payload,
        "radiant_win_prob": radiant_win_prob,
        "dire_win_prob": 1.0 - radiant_win_prob,
        "elo_diff": radiant_rating - dire_rating,
        "tier_gap_key": tier_gap_key,
        "tier_gap_bonus": tier_gap_bonus,
        **probability_meta,
    }


def _build_live_applied_update(
    *,
    snapshot: dict[str, Any],
    model: HybridPlayerRosterEloModel,
    match: MatchRecord,
    map_key: str,
    series_key: str,
    series_url: str,
    winner_slot: str,
    radiant_win: bool,
    previous_scores: dict[str, int],
    current_scores: dict[str, int],
    first_team_is_radiant: bool,
) -> dict[str, Any]:
    meta = snapshot.get("meta") or {}
    before_summary = _preview_live_matchup_from_model(
        model=model,
        match=match,
        tier_matchup_elo_bonus=meta.get("tier_matchup_elo_bonus"),
    )
    step = model.process_match(match)
    after_summary = _preview_live_matchup_from_model(
        model=model,
        match=match,
        tier_matchup_elo_bonus=meta.get("tier_matchup_elo_bonus"),
    )
    step_meta = step.metadata if isinstance(step.metadata, dict) else {}
    first_team_name = match.radiant_team_name if first_team_is_radiant else match.dire_team_name
    second_team_name = match.dire_team_name if first_team_is_radiant else match.radiant_team_name
    winner_team_name = first_team_name if winner_slot == "first" else second_team_name

    def _side_delta(side: str) -> dict[str, Any]:
        before_side = before_summary.get(side) or {}
        after_side = after_summary.get(side) or {}
        before_rating = float(before_side.get("rating", before_side.get("base_rating", LEADERBOARD_BASELINE)))
        after_rating = float(after_side.get("rating", after_side.get("base_rating", before_rating)))
        before_base = float(before_side.get("base_rating", before_rating))
        after_base = float(after_side.get("base_rating", after_rating))
        side_prefix = "radiant" if side == "radiant" else "dire"
        return {
            "team_name": str(after_side.get("team_name") or before_side.get("team_name") or ""),
            "team_id": after_side.get("team_id", before_side.get("team_id")),
            "rating_source": str(after_side.get("rating_source") or before_side.get("rating_source") or ""),
            "before_rating": before_rating,
            "after_rating": after_rating,
            "delta": after_rating - before_rating,
            "before_base_rating": before_base,
            "after_base_rating": after_base,
            "base_delta": after_base - before_base,
            "before_roster_matches": int(before_side.get("roster_matches", 0) or 0),
            "after_roster_matches": int(after_side.get("roster_matches", 0) or 0),
            "before_lineup_matches": int(step_meta.get(f"{side_prefix}_lineup_matches", 0) or 0),
            "after_lineup_matches": int(step_meta.get(f"{side_prefix}_lineup_matches", 0) or 0) + 1,
            "lineup_k_multiplier": float(step_meta.get(f"{side_prefix}_lineup_k_multiplier", 1.0) or 1.0),
            "player_org_k_multiplier_avg": float(
                step_meta.get(f"{side_prefix}_player_org_k_multiplier_avg", 1.0) or 1.0
            ),
            "effective_global_k_multiplier_avg": float(
                step_meta.get(f"{side_prefix}_effective_global_k_multiplier_avg", 1.0) or 1.0
            ),
            "effective_local_k_multiplier_avg": float(
                step_meta.get(f"{side_prefix}_effective_local_k_multiplier_avg", 1.0) or 1.0
            ),
        }

    radiant_delta = _side_delta("radiant")
    dire_delta = _side_delta("dire")
    return {
        "map_key": map_key,
        "series_key": series_key,
        "series_url": series_url,
        "winner_slot": winner_slot,
        "radiant_win": bool(radiant_win),
        "match_id": int(match.match_id),
        "series_score_before": {
            "first": int(previous_scores.get("first", 0)),
            "second": int(previous_scores.get("second", 0)),
        },
        "series_score_after": {
            "first": int(current_scores.get("first", 0)),
            "second": int(current_scores.get("second", 0)),
        },
        "first_team_name": str(first_team_name or ""),
        "second_team_name": str(second_team_name or ""),
        "winner_team_name": str(winner_team_name or ""),
        "radiant_team_name": str(match.radiant_team_name or ""),
        "dire_team_name": str(match.dire_team_name or ""),
        "radiant": radiant_delta,
        "dire": dire_delta,
        "k_global": float(step_meta.get("k_global", 0.0) or 0.0),
        "k_local": float(step_meta.get("k_local", 0.0) or 0.0),
        "k_roster": float(step_meta.get("k_roster", 0.0) or 0.0),
        "rating_delta_sum": float(radiant_delta.get("delta", 0.0)) + float(dire_delta.get("delta", 0.0)),
        "base_delta_sum": float(radiant_delta.get("base_delta", 0.0)) + float(dire_delta.get("base_delta", 0.0)),
        "radiant_win_prob_before": float(before_summary.get("radiant_win_prob", 0.5)),
        "radiant_win_prob_after": float(after_summary.get("radiant_win_prob", 0.5)),
        "radiant_win_prob_delta": float(after_summary.get("radiant_win_prob", 0.5))
        - float(before_summary.get("radiant_win_prob", 0.5)),
        "elo_diff_before": float(before_summary.get("elo_diff", 0.0)),
        "elo_diff_after": float(after_summary.get("elo_diff", 0.0)),
    }


def _apply_one_pending_map(
    *,
    pending_map: dict[str, Any],
    winner_slot: str,
    applied_maps: dict[str, Any],
    snapshot: dict[str, Any],
    model_getter: Callable[[], HybridPlayerRosterEloModel],
    previous_scores: dict[str, int],
    current_scores: dict[str, int],
    normalized_series_key: str,
    series_url: str,
) -> dict[str, Any] | None:
    """Apply one queued map context to team ratings, or return None.

    None covers three cases the caller should just drop the entry for: no
    map key, the map (or its match_id under the sourcetv `match_id ==
    series_id` alias — ЗАЩИТА ОТ ДВОЙНОГО ПРИМЕНЕНИЯ идёт по match_id, а не
    по ключу карты: он прыгает на каждый опрос) already sits in
    `applied_maps`, or its stored match_record can't be rebuilt. None of
    these will ever resolve productively by retrying.
    """
    pending_map_key = str(pending_map.get("map_key") or "").strip()
    if not pending_map_key:
        return None
    first_radiant_pending = bool(pending_map.get("first_team_is_radiant"))
    pm_rec = pending_map.get("match_record") if isinstance(pending_map.get("match_record"), dict) else {}
    pm_mid = int(pm_rec.get("match_id") or 0)
    mid_is_series = _match_id_is_series(pm_mid, normalized_series_key, pm_rec.get("series_id"))
    seen_mid = (
        (not mid_is_series)
        and pm_mid > 0
        and any(int((v or {}).get("match_id") or 0) == pm_mid for v in applied_maps.values() if isinstance(v, dict))
    )
    if pending_map_key in applied_maps or seen_mid:
        return None
    radiant_won = winner_slot == ("first" if first_radiant_pending else "second")
    pending_match = _deserialize_match_record(pm_rec, radiant_win=radiant_won)
    if pending_match is None:
        return None
    applied_maps[pending_map_key] = {
        "series_key": normalized_series_key,
        "series_url": str(series_url),
        "winner_slot": winner_slot,
        "radiant_win": bool(radiant_won),
        "applied_at": int(time.time()),
        "match_id": int(pending_match.match_id),
    }
    return _build_live_applied_update(
        snapshot=snapshot,
        model=model_getter(),
        match=pending_match,
        map_key=pending_map_key,
        series_key=normalized_series_key,
        series_url=str(series_url),
        winner_slot=winner_slot,
        radiant_win=bool(radiant_won),
        previous_scores=previous_scores,
        current_scores=current_scores,
        first_team_is_radiant=first_radiant_pending,
    )


def _drain_pending_map_queue(
    *,
    pending_maps: list[dict[str, Any]],
    previous_scores: dict[str, int],
    current_scores: dict[str, int],
    applied_maps: dict[str, Any],
    snapshot: dict[str, Any],
    model_getter: Callable[[], HybridPlayerRosterEloModel],
    normalized_series_key: str,
    series_url: str,
    winner_lookup: Any = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Resolve as many queued map contexts as the observed score explains.

    Returns (remaining_queue, applied_updates), applied_updates ordered
    oldest-resolved-first (FIFO: the queued map that started first is
    credited with the first observed win). See
    `_winner_slots_from_score_advance` for what counts as explained by the
    score jump. When it can't tell (both sides moved and not by exactly one
    map each) and a `winner_lookup` was given, only the oldest queued map is
    tried against it directly — the same single-map path `register_live_map_context`
    always had; the live path's series score never moves within one poll
    window (E-224), so this is the only route it resolves through, and the
    bulk of orphan resolution still happens in cyberscore_try.py's
    `_sweep_orphaned_live_elo`, outside this module.
    """
    remaining = list(pending_maps)
    applied_updates: list[dict[str, Any]] = []
    winner_slots = _winner_slots_from_score_advance(previous_scores, current_scores)
    if winner_slots is not None:
        for winner_slot in winner_slots:
            if not remaining:
                break
            pending_map = remaining.pop(0)
            update = _apply_one_pending_map(
                pending_map=pending_map,
                winner_slot=winner_slot,
                applied_maps=applied_maps,
                snapshot=snapshot,
                model_getter=model_getter,
                previous_scores=previous_scores,
                current_scores=current_scores,
                normalized_series_key=normalized_series_key,
                series_url=series_url,
            )
            if update is not None:
                applied_updates.append(update)
        return remaining, applied_updates

    if winner_lookup is not None and remaining:
        pending_map = remaining[0]
        pending_map_key = str(pending_map.get("map_key") or "").strip()
        first_radiant_pending = bool(pending_map.get("first_team_is_radiant"))
        if pending_map_key:
            try:
                looked = winner_lookup(pending_map_key, pending_map)
            except Exception:
                looked = None
            if isinstance(looked, bool):
                winner_slot = (
                    ("first" if first_radiant_pending else "second") if looked
                    else ("second" if first_radiant_pending else "first")
                )
                remaining.pop(0)
                update = _apply_one_pending_map(
                    pending_map=pending_map,
                    winner_slot=winner_slot,
                    applied_maps=applied_maps,
                    snapshot=snapshot,
                    model_getter=model_getter,
                    previous_scores=previous_scores,
                    current_scores=current_scores,
                    normalized_series_key=normalized_series_key,
                    series_url=series_url,
                )
                if update is not None:
                    applied_updates.append(update)
    return remaining, applied_updates


def _build_snapshot_dict(
    *,
    data_dir: Path,
    active_cutoff_days: float,
    display_decay_half_life_days: float,
    config: HybridEloConfig,
) -> dict[str, Any]:
    matches, load_summary = load_matches(data_dir)
    # Combined archives keep the same map in more than one file, so the raw
    # stream carries copies (3.7% of the pro corpus as of 2026-08-12). A copy is
    # a second rating update for one outcome, and inside build_series_bundles it
    # also counts as an extra map win, which can close a Bo3 after two copies of
    # the same map. Drop copies before anything reads the stream.
    duplicate_records = 0
    if matches:
        seen_match_ids: set[int] = set()
        unique_matches: list[MatchRecord] = []
        for match in matches:
            if match.match_id in seen_match_ids:
                duplicate_records += 1
                continue
            seen_match_ids.add(match.match_id)
            unique_matches.append(match)
        matches = unique_matches
        load_summary = dict(load_summary)
        load_summary["duplicate_records"] = duplicate_records
        load_summary["loaded_matches"] = len(matches)
    if not matches:
        empty_model_state = None
        return {
            "meta": {
                "data_dir": str(data_dir),
                "reference_timestamp": None,
                "reference_utc": None,
                "active_cutoff_days": active_cutoff_days,
                "display_decay_half_life_days": display_decay_half_life_days,
                "loaded_matches": int(load_summary.get("loaded_matches", 0)),
                "model_config_signature": _model_config_signature(empty_model_state),
                "team_kills_history_schema_version": TEAM_KILLS_HISTORY_SCHEMA_VERSION,
                "team_kills_history_matches_per_team": TEAM_KILLS_HISTORY_MATCHES_PER_TEAM,
                "team_kills_history_latest_patch": None,
            },
            "teams_by_org_key": {},
            "team_kills_history_by_team_id": {},
            "model_state": empty_model_state,
        }

    league_info, _ = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)

    model = HybridPlayerRosterEloModel(config)
    team_snapshots: dict[str, dict[str, Any]] = {}
    team_kills_history: dict[str, list[dict[str, Any]]] = defaultdict(list)
    latest_patch_match = max(
        (
            (match.timestamp, str(match.source_patch))
            for match in matches
            if match.source_patch
        ),
        default=None,
    )
    latest_patch = latest_patch_match[1] if latest_patch_match else None
    reference_timestamp = matches[-1].timestamp
    cross_tier_counts: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"series": 0, "strong_wins": 0}
    )

    # Raw archives can contain the same map in more than one combined file.
    # Keep one side-row per match so roster averages are not biased by copies.
    seen_history_match_ids: set[int] = set()
    for match in matches:
        if match.match_id in seen_history_match_ids:
            continue
        seen_history_match_ids.add(match.match_id)
        for team_id, player_ids, kills in (
            (match.radiant_team_id, match.radiant_player_ids, match.radiant_kills),
            (match.dire_team_id, match.dire_player_ids, match.dire_kills),
        ):
            if team_id is None or kills is None or len(player_ids) != 5:
                continue
            team_kills_history[str(int(team_id))].append(
                {
                    "match_id": int(match.match_id),
                    "timestamp": int(match.timestamp),
                    "player_ids": [int(player_id) for player_id in player_ids],
                    "kills": int(kills),
                    "patch": match.source_patch,
                }
            )

    for team_id, rows in list(team_kills_history.items()):
        team_kills_history[team_id] = rows[-TEAM_KILLS_HISTORY_MATCHES_PER_TEAM:]

    for bundle in series_bundles:
        series = bundle.series
        if series.eligible_for_winner_target and bundle.deciding_maps:
            first_map = bundle.deciding_maps[0]
            radiant_known_tier = get_known_team_tier(first_map.radiant_team_id, first_map.radiant_team_name)
            dire_known_tier = get_known_team_tier(first_map.dire_team_id, first_map.dire_team_name)
            if (
                radiant_known_tier is not None
                and dire_known_tier is not None
                and radiant_known_tier != dire_known_tier
            ):
                if radiant_known_tier.value < dire_known_tier.value:
                    strong_tier = radiant_known_tier
                    weak_tier = dire_known_tier
                    strong_team_won = bool(series.team_a_won)
                else:
                    strong_tier = dire_known_tier
                    weak_tier = radiant_known_tier
                    strong_team_won = not bool(series.team_a_won)
                pair_key = (strong_tier.value, weak_tier.value)
                cross_tier_counts[pair_key]["series"] += 1
                cross_tier_counts[pair_key]["strong_wins"] += 1 if strong_team_won else 0
        for match in bundle.all_maps:
            model.process_match(match)
            for is_radiant, team_id, team_name, player_ids in (
                (True, match.radiant_team_id, match.radiant_team_name, match.radiant_player_ids),
                (False, match.dire_team_id, match.dire_team_name, match.dire_player_ids),
            ):
                org_key = resolve_org_key(team_id, team_name)
                previous = team_snapshots.get(org_key)
                if previous is not None and match.timestamp < int(previous["timestamp"]):
                    continue
                team_snapshots[org_key] = {
                    "org_key": org_key,
                    "team_id": team_id,
                    "team_name": team_name,
                    "player_ids": list(player_ids),
                    "tier": match.derived_league_tier.value,
                    "timestamp": match.timestamp,
                    "is_radiant_last": bool(is_radiant),
                }

    teams_by_org_key: dict[str, dict[str, Any]] = {}
    for org_key, snapshot in team_snapshots.items():
        preview = model.preview_team_strength(
            team_id=snapshot["team_id"],
            team_name=snapshot["team_name"],
            player_ids=tuple(int(player_id) for player_id in snapshot["player_ids"]),
            tier=LeagueTier(snapshot["tier"]),
            timestamp=int(snapshot["timestamp"]) + 1,
        )
        days_inactive = max(0.0, (reference_timestamp - int(snapshot["timestamp"])) / SECONDS_PER_DAY)
        current_strength = _decay_strength_for_leaderboard(
            raw_strength=float(preview["team_strength"]),
            days_inactive=days_inactive,
            half_life_days=display_decay_half_life_days,
        )
        teams_by_org_key[org_key] = {
            "org_key": org_key,
            "team_id": snapshot["team_id"],
            "team_name": snapshot["team_name"],
            "tier": snapshot["tier"],
            "timestamp": int(snapshot["timestamp"]),
            "last_seen_utc": _timestamp_to_iso(int(snapshot["timestamp"])),
            "raw_team_strength": float(preview["team_strength"]),
            "current_strength": current_strength,
            "player_strength": float(preview["player_strength"]),
            "roster_rating": float(preview["roster_rating"]),
            "roster_matches": int(preview["roster_matches"]),
            "roster_weight": float(preview["roster_weight"]),
            "roster_key": str(preview["roster_key"]),
            "days_inactive": days_inactive,
            "is_active": bool(days_inactive <= active_cutoff_days),
        }

    tier_matchup_elo_bonus: dict[str, dict[str, float | int]] = {}
    for (strong_tier, weak_tier), counts in sorted(cross_tier_counts.items()):
        series_count = int(counts["series"])
        strong_wins = int(counts["strong_wins"])
        strong_winrate = (strong_wins + 1.0) / (series_count + 2.0)
        elo_bonus = _elo_diff_from_probability(strong_winrate, config.elo_scale)
        tier_matchup_elo_bonus[f"{strong_tier}_vs_{weak_tier}"] = {
            "series_count": series_count,
            "strong_winrate": strong_winrate,
            "elo_bonus": elo_bonus,
        }

    model_state = model.export_state()
    return {
        "meta": {
            "data_dir": str(data_dir),
            "reference_timestamp": reference_timestamp,
            "reference_utc": _timestamp_to_iso(reference_timestamp),
            "active_cutoff_days": active_cutoff_days,
            "display_decay_half_life_days": display_decay_half_life_days,
            "loaded_matches": int(load_summary.get("loaded_matches", 0)),
            "duplicate_records": int(load_summary.get("duplicate_records", 0)),
            "series_groups": int(series_summary.get("all_series_groups", 0)),
            "eligible_series": int(series_summary.get("eligible_series", 0)),
            "team_count": len(teams_by_org_key),
            "tier_matchup_elo_bonus": tier_matchup_elo_bonus,
            "model_config_signature": _model_config_signature(model_state),
            "team_kills_history_schema_version": TEAM_KILLS_HISTORY_SCHEMA_VERSION,
            "team_kills_history_matches_per_team": TEAM_KILLS_HISTORY_MATCHES_PER_TEAM,
            "team_kills_history_latest_patch": latest_patch,
        },
        "teams_by_org_key": teams_by_org_key,
        "team_kills_history_by_team_id": dict(team_kills_history),
        "model_state": model_state,
    }


def build_snapshot(
    *,
    data_dir: Path = DEFAULT_DATA_DIR,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    active_cutoff_days: float = DEFAULT_ACTIVE_CUTOFF_DAYS,
    display_decay_half_life_days: float = DEFAULT_DISPLAY_DECAY_HALF_LIFE_DAYS,
    config: HybridEloConfig | None = None,
) -> dict[str, Any]:
    snapshot = _build_snapshot_dict(
        data_dir=data_dir,
        active_cutoff_days=active_cutoff_days,
        display_decay_half_life_days=display_decay_half_life_days,
        config=config or HybridEloConfig(),
    )
    _write_json_atomic(snapshot_path, snapshot)
    global _SNAPSHOT_CACHE
    _SNAPSHOT_CACHE = snapshot
    return snapshot


def load_live_snapshot(
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    runtime_model_state_path: Path = DEFAULT_RUNTIME_MODEL_STATE_PATH,
) -> dict[str, Any] | None:
    """Снимок с ПРИМЕШАННЫМ рантайм-состоянием — то, что нужно живому счёту.

    `load_snapshot` отдаёт базовый файл, который пересобирается редко: на боевой
    машине он датирован 12.08, тогда как рантайм-состояние обновляется постоянно
    (20.08 19:22 на момент правки). До этой функции мержем пользовалась одна
    только сводка матчапа, а признак `hybrid_strength` — и в панели, и на пути
    ставки — считался по базовому снимку, то есть по рейтингам восьмидневной
    давности. Причём свежие лежали в том же процессе.

    Обе копии переиспользуются: базовый словарь кэшируется в `_SNAPSHOT_CACHE`,
    мерженый — в `_RUNTIME_SNAPSHOT_CACHE`, а восстановленная модель — в
    `_MODEL_FROM_SNAPSHOT_CACHE` по состоянию, из которого построена. Второй
    разбор 366-мегабайтного файла здесь не происходит.
    """
    base = load_snapshot(snapshot_path)
    if base is None:
        return None
    return _snapshot_with_runtime_model_state(
        base, runtime_model_state_path=runtime_model_state_path)


def load_snapshot(snapshot_path: Path = DEFAULT_SNAPSHOT_PATH) -> dict[str, Any] | None:
    global _SNAPSHOT_CACHE
    if isinstance(_SNAPSHOT_CACHE, dict):
        return _SNAPSHOT_CACHE
    if not snapshot_path.exists():
        return None
    snapshot = _load_snapshot_streaming(snapshot_path)
    if snapshot is None:
        with snapshot_path.open("r", encoding="utf-8") as fh:
            snapshot = json.load(fh)
    if not isinstance(snapshot, dict):
        return None
    _SNAPSHOT_CACHE = snapshot
    return snapshot


#: Маркер slim-состояния: `model_state` снимка НЕ разобран, держим только
#: `config` (нужен для `_model_config_signature`) и путь догрузки. Полное
#: состояние возвращает `full_model_state()`.
SLIM_MODEL_STATE_MARKER = "__slim_model_state_path__"

#: Разделы снимка, которые действительно живут в памяти. Порядок — как в файле:
#: каждый проход останавливается сразу после своего раздела, поэтому четыре
#: прохода стоят примерно как один (замер 02.09.2026: 3.6 с на 646 МБ, ijson
#: на бэкенде yajl2_c).
_SNAPSHOT_STREAM_SECTIONS = (
    ("meta", "meta"),
    ("teams_by_org_key", "teams_by_org_key"),
    ("team_kills_history_by_team_id", "team_kills_history_by_team_id"),
    ("model_state.config", "config"),
)


def _load_snapshot_streaming(path: Path) -> dict[str, Any] | None:
    """Разбор снимка БЕЗ материализации `model_state`.

    `json.load` всего файла стоит 3.5 ГБ RSS (замер 02.09.2026: baseline 10 МБ
    -> 3694 МБ на файле 646 МБ), и 527 МБ из этих 646 — `model_state`. Базовая
    копия состояния на проде не нужна никому: сигнатура берётся из `meta`, оба
    ленивых билдера модели предпочитают рантайм-payload
    (`live_elo_model_state.json`), а путь чтения обслуживает массивная модель
    (`ELO/array_model.py`), которая стримит состояние сама. Освобождать уже
    разобранные словари поздно — арены glibc фрагментированы и RSS не
    возвращают (`ELO/array_store.py:9-12`), поэтому экономит только то, что не
    создано вовсе.

    None означает «потоковая загрузка недоступна» — зовущий падает на json.load.
    """
    try:
        import ijson                                  # noqa: PLC0415
    except ImportError:
        return None

    snapshot: dict[str, Any] = {}
    try:
        for prefix, key in _SNAPSHOT_STREAM_SECTIONS:
            with path.open("rb") as fh:
                # use_float: без него ijson отдаёт Decimal, и рейтинги в
                # `teams_by_org_key` перестают сравниваться и арифметикой
                # отличаться от float — молча поехал бы живой путь.
                for value in ijson.items(fh, prefix, use_float=True):
                    if key == "config":
                        snapshot["model_state"] = {
                            "config": value,
                            SLIM_MODEL_STATE_MARKER: str(path),
                        }
                    else:
                        snapshot[key] = value
                    break
    except (OSError, ValueError):
        return None
    return snapshot or None


def full_model_state(snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
    """Полный `model_state`: из кэша либо догрузка из файла для slim-снимка.

    Редкий путь (свежая машина без рантайм-состояния, либо отказ
    `_load_runtime_model_payload`), поэтому догрузка печатается: если строка
    появилась в бою, значит живое обновление не подхватилось и процесс
    однократно заплатит ~3 ГБ.
    """
    state = snapshot.get("model_state") if isinstance(snapshot, dict) else None
    if not isinstance(state, dict):
        return state
    source = state.get(SLIM_MODEL_STATE_MARKER)
    if not source:
        return state
    print(f"[ELO] догружаю полный model_state из снимка (редкий путь): {source}",
          flush=True)
    try:
        with open(source, "r", encoding="utf-8") as fh:
            loaded = json.load(fh)
    except (OSError, ValueError) as exc:
        print(f"[ELO] догрузить model_state не удалось: {exc}", flush=True)
        return None
    full = loaded.get("model_state") if isinstance(loaded, dict) else None
    if not isinstance(full, dict):
        return None
    # Меняем slim на полный прямо в кэше: повторная догрузка не нужна, а
    # последующие вызовы идут по обычной ветке.
    snapshot["model_state"] = full
    return full


def ensure_snapshot(
    *,
    data_dir: Path = DEFAULT_DATA_DIR,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    rebuild_if_missing: bool = True,
    active_cutoff_days: float = DEFAULT_ACTIVE_CUTOFF_DAYS,
    display_decay_half_life_days: float = DEFAULT_DISPLAY_DECAY_HALF_LIFE_DAYS,
) -> dict[str, Any] | None:
    snapshot = load_snapshot(snapshot_path)
    snapshot_mtime = 0.0
    if snapshot_path.exists():
        try:
            snapshot_mtime = snapshot_path.stat().st_mtime
        except FileNotFoundError:
            snapshot_mtime = 0.0
    data_mtime = _latest_data_mtime(data_dir)
    snapshot_is_stale = bool(
        snapshot is not None and data_mtime > snapshot_mtime and not _snapshot_is_pinned()
    )
    snapshot_missing_model_state = bool(snapshot is not None and not isinstance(snapshot.get("model_state"), dict))
    snapshot_missing_kills_history = bool(
        snapshot is not None
        and (
            not isinstance(snapshot.get("team_kills_history_by_team_id"), dict)
            or int((snapshot.get("meta") or {}).get("team_kills_history_schema_version") or 0)
            != TEAM_KILLS_HISTORY_SCHEMA_VERSION
        )
    )
    if (
        snapshot is not None
        and not snapshot_is_stale
        and not snapshot_missing_model_state
        and (not snapshot_missing_kills_history or not rebuild_if_missing)
    ):
        return snapshot
    if not rebuild_if_missing:
        return None
    if snapshot_is_stale or snapshot_missing_model_state or snapshot_missing_kills_history:
        global _SNAPSHOT_CACHE
        _SNAPSHOT_CACHE = None
    return build_snapshot(
        data_dir=data_dir,
        snapshot_path=snapshot_path,
        active_cutoff_days=active_cutoff_days,
        display_decay_half_life_days=display_decay_half_life_days,
    )


def build_matchup_summary_from_snapshot(
    snapshot: dict[str, Any],
    *,
    radiant_team_id: int | None,
    dire_team_id: int | None,
    radiant_team_name: str,
    dire_team_name: str,
    radiant_account_ids: list[int] | tuple[int, ...] | None = None,
    dire_account_ids: list[int] | tuple[int, ...] | None = None,
    match_tier: LeagueTier | str | int | None = None,
    initial_rating: float = LEADERBOARD_BASELINE,
    elo_scale: float = 400.0,
    player_only_fallback_roster_matches: int = DEFAULT_PLAYER_ONLY_FALLBACK_ROSTER_MATCHES,
) -> dict[str, Any] | None:
    teams_by_org_key = snapshot.get("teams_by_org_key") or {}
    if not isinstance(teams_by_org_key, dict):
        return None
    meta = snapshot.get("meta") or {}
    tier_matchup_elo_bonus = meta.get("tier_matchup_elo_bonus") or {}
    reference_timestamp = int(meta.get("reference_timestamp") or 0)
    rank_map = _leaderboard_rank_map(snapshot)
    lineup_match_tier = _coerce_match_tier(match_tier)
    model = _restore_model_from_snapshot(snapshot)

    def _lookup(team_id: int | None, team_name: str) -> tuple[str, dict[str, Any] | None]:
        org_key = resolve_org_key(team_id, team_name)
        row = teams_by_org_key.get(org_key)
        if isinstance(row, dict):
            return org_key, row
        return org_key, None

    def _resolve_base_rating(
        *,
        team_id: int | None,
        team_name: str,
        org_key: str,
        row: dict[str, Any] | None,
        account_ids: list[int] | tuple[int, ...] | None,
    ) -> tuple[float, dict[str, Any]]:
        base_rating = float((row or {}).get("current_strength", initial_rating))
        payload: dict[str, Any] = {
            "org_key": org_key,
            "matched": row is not None,
            "team_id": (row or {}).get("team_id", team_id),
            "team_name": (row or {}).get("team_name", team_name),
            "tier": (row or {}).get("tier"),
            "last_seen_utc": (row or {}).get("last_seen_utc"),
            "lineup_used": False,
            "lineup_player_ids": [],
            "lineup_player_count": 0,
        }
        player_ids = _coerce_player_ids(account_ids)
        if len(player_ids) >= 5 and model is not None:
            preview_tier = (
                lineup_match_tier
                or _coerce_match_tier((row or {}).get("tier"))
                or get_known_team_tier(team_id, team_name)
                or LeagueTier.TIER3
            )
            preview = model.preview_team_strength(
                team_id=team_id,
                team_name=team_name,
                player_ids=player_ids,
                tier=preview_tier,
                timestamp=max(reference_timestamp + 1, 1),
            )
            team_strength = float(preview["team_strength"])
            player_strength = float(preview["player_strength"])
            roster_matches = int(preview["roster_matches"])
            rating_source = "lineup_team_strength"
            base_rating = team_strength
            if roster_matches < max(0, int(player_only_fallback_roster_matches)):
                base_rating = player_strength
                rating_source = "lineup_player_strength_cold_roster"
            payload.update(
                {
                    "base_rating": base_rating,
                    "team_strength": team_strength,
                    "player_strength": player_strength,
                    "prior_blended_strength": float(preview["prior_blended_strength"]),
                    "player_global_avg": float(preview["player_global_avg"]),
                    "player_local_avg": float(preview["player_local_avg"]),
                    "roster_rating": float(preview["roster_rating"]),
                    "roster_matches": roster_matches,
                    "roster_weight": float(preview["roster_weight"]),
                    "roster_key": str(preview["roster_key"]),
                    "rating_source": rating_source,
                    "lineup_used": True,
                    "lineup_player_ids": list(player_ids),
                    "lineup_player_count": len(player_ids),
                    "lineup_tier": preview_tier.value,
                }
            )
            return base_rating, payload
        payload["base_rating"] = base_rating
        return base_rating, payload

    radiant_org_key, radiant_row = _lookup(radiant_team_id, radiant_team_name)
    dire_org_key, dire_row = _lookup(dire_team_id, dire_team_name)
    radiant_rank = rank_map.get(radiant_org_key)
    dire_rank = rank_map.get(dire_org_key)
    radiant_base_rating, radiant_payload = _resolve_base_rating(
        team_id=radiant_team_id,
        team_name=radiant_team_name,
        org_key=radiant_org_key,
        row=radiant_row,
        account_ids=radiant_account_ids,
    )
    dire_base_rating, dire_payload = _resolve_base_rating(
        team_id=dire_team_id,
        team_name=dire_team_name,
        org_key=dire_org_key,
        row=dire_row,
        account_ids=dire_account_ids,
    )
    if (
        radiant_row is None
        and dire_row is None
        and not bool(radiant_payload.get("lineup_used"))
        and not bool(dire_payload.get("lineup_used"))
    ):
        return None
    radiant_known_tier = get_known_team_tier(radiant_team_id, radiant_team_name)
    dire_known_tier = get_known_team_tier(dire_team_id, dire_team_name)
    tier_gap_bonus = 0.0
    tier_gap_key: str | None = None
    if (
        radiant_known_tier is not None
        and dire_known_tier is not None
        and radiant_known_tier != dire_known_tier
        and isinstance(tier_matchup_elo_bonus, dict)
    ):
        if radiant_known_tier.value < dire_known_tier.value:
            tier_gap_key = f"{radiant_known_tier.value}_vs_{dire_known_tier.value}"
            tier_gap_bonus = float((tier_matchup_elo_bonus.get(tier_gap_key) or {}).get("elo_bonus", 0.0))
        else:
            tier_gap_key = f"{dire_known_tier.value}_vs_{radiant_known_tier.value}"
            tier_gap_bonus = -float((tier_matchup_elo_bonus.get(tier_gap_key) or {}).get("elo_bonus", 0.0))

    radiant_rating = radiant_base_rating + (tier_gap_bonus / 2.0)
    dire_rating = dire_base_rating - (tier_gap_bonus / 2.0)
    probability_meta = _apply_live_probability_policy(
        radiant_rating=radiant_rating,
        dire_rating=dire_rating,
        radiant_team_id=radiant_team_id,
        radiant_team_name=radiant_team_name,
        dire_team_id=dire_team_id,
        dire_team_name=dire_team_name,
        elo_scale=elo_scale,
    )
    radiant_win_prob = float(probability_meta.get("radiant_win_prob", 0.5))

    return {
        "source": (
            "elo_live_lineup_snapshot"
            if bool(radiant_payload.get("lineup_used")) or bool(dire_payload.get("lineup_used"))
            else "elo_live_snapshot"
        ),
        "reference_timestamp": meta.get("reference_timestamp"),
        "radiant": {
            **radiant_payload,
            "rating": radiant_rating,
            "base_rating": radiant_base_rating,
            "leaderboard_rank": int(radiant_rank) if radiant_rank is not None else None,
        },
        "dire": {
            **dire_payload,
            "rating": dire_rating,
            "base_rating": dire_base_rating,
            "leaderboard_rank": int(dire_rank) if dire_rank is not None else None,
        },
        "radiant_win_prob": radiant_win_prob,
        "dire_win_prob": 1.0 - radiant_win_prob,
        "elo_diff": radiant_rating - dire_rating,
        "tier_gap_key": tier_gap_key,
        "tier_gap_bonus": tier_gap_bonus,
        **probability_meta,
    }


def get_matchup_summary(
    *,
    radiant_team_id: int | None,
    dire_team_id: int | None,
    radiant_team_name: str,
    dire_team_name: str,
    radiant_account_ids: list[int] | tuple[int, ...] | None = None,
    dire_account_ids: list[int] | tuple[int, ...] | None = None,
    match_tier: LeagueTier | str | int | None = None,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    data_dir: Path = DEFAULT_DATA_DIR,
    rebuild_if_missing: bool = True,
    runtime_model_state_path: Path = DEFAULT_RUNTIME_MODEL_STATE_PATH,
) -> dict[str, Any] | None:
    snapshot = ensure_snapshot(
        data_dir=data_dir,
        snapshot_path=snapshot_path,
        rebuild_if_missing=rebuild_if_missing,
    )
    if snapshot is None:
        return None
    base_summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=radiant_team_id,
        dire_team_id=dire_team_id,
        radiant_team_name=radiant_team_name,
        dire_team_name=dire_team_name,
        radiant_account_ids=radiant_account_ids,
        dire_account_ids=dire_account_ids,
        match_tier=match_tier,
    )
    snapshot = _snapshot_with_runtime_model_state(
        snapshot,
        runtime_model_state_path=runtime_model_state_path,
    )
    live_summary = build_matchup_summary_from_snapshot(
        snapshot,
        radiant_team_id=radiant_team_id,
        dire_team_id=dire_team_id,
        radiant_team_name=radiant_team_name,
        dire_team_name=dire_team_name,
        radiant_account_ids=radiant_account_ids,
        dire_account_ids=dire_account_ids,
        match_tier=match_tier,
    )
    if live_summary is None:
        return base_summary
    if base_summary is None:
        return live_summary

    for side in ("radiant", "dire"):
        live_payload = live_summary.get(side) or {}
        base_payload = base_summary.get(side) or {}
        live_base_rating = float(live_payload.get("base_rating", live_payload.get("rating", LEADERBOARD_BASELINE)))
        base_base_rating = float(base_payload.get("base_rating", base_payload.get("rating", live_base_rating)))
        live_rating = float(live_payload.get("rating", live_base_rating))
        base_rating = float(base_payload.get("rating", base_base_rating))
        live_payload["snapshot_base_rating"] = base_base_rating
        live_payload["snapshot_rating"] = base_rating
        live_payload["live_base_delta"] = live_base_rating - base_base_rating
        live_payload["live_rating_delta"] = live_rating - base_rating
        live_summary[side] = live_payload

    live_summary["snapshot_radiant_win_prob"] = float(base_summary.get("radiant_win_prob", 0.5))
    live_summary["snapshot_dire_win_prob"] = float(base_summary.get("dire_win_prob", 0.5))
    live_summary["snapshot_elo_diff"] = float(base_summary.get("elo_diff", 0.0))
    live_summary["has_live_delta"] = bool(
        abs(float((live_summary.get("radiant") or {}).get("live_base_delta", 0.0))) >= 0.5
        or abs(float((live_summary.get("dire") or {}).get("live_base_delta", 0.0))) >= 0.5
    )
    return live_summary


def register_live_map_context(
    *,
    series_key: str,
    series_url: str,
    map_key: str,
    first_team_score: int,
    second_team_score: int,
    first_team_is_radiant: bool,
    match_record: MatchRecord,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    data_dir: Path = DEFAULT_DATA_DIR,
    rebuild_if_missing: bool = True,
    progress_path: Path = DEFAULT_RUNTIME_PROGRESS_PATH,
    runtime_model_state_path: Path = DEFAULT_RUNTIME_MODEL_STATE_PATH,
    runtime_lock_path: Path = DEFAULT_RUNTIME_LOCK_PATH,
    winner_lookup: Any = None,
) -> dict[str, Any] | None:
    normalized_series_key = str(series_key or "").strip() or str(match_record.series_id or series_url or map_key)
    normalized_map_key = str(map_key or "").strip()
    if not normalized_series_key or not normalized_map_key:
        return None

    snapshot = ensure_snapshot(
        data_dir=data_dir,
        snapshot_path=snapshot_path,
        rebuild_if_missing=rebuild_if_missing,
    )
    if snapshot is None:
        return None
    base_reference_timestamp = _snapshot_reference_timestamp(snapshot)
    base_model_config_signature = _snapshot_model_config_signature(snapshot)
    current_scores = {"first": int(first_team_score), "second": int(second_team_score)}

    with _runtime_file_lock(runtime_lock_path):
        progress = _load_runtime_progress(
            base_reference_timestamp=base_reference_timestamp,
            model_config_signature=base_model_config_signature,
            progress_path=progress_path,
        )
        # Модель строится ЛЕНИВО, при первом обращении. Раньше её собирали здесь
        # безусловно, а нужна она только когда есть завершённая карта, которую
        # надо провести через рейтинг: разбор 219-мегабайтного рантайм-состояния
        # занимает 4.3 с, сборка модели ещё 2.6 с, пик 917 МБ — и всё это
        # выбрасывалось. По логу за 21 день на 10 241 карту приходится 376
        # применений, то есть впустую шло около 96% вызовов.
        _model_cell: list = []

        def _model() -> HybridPlayerRosterEloModel:
            if not _model_cell:
                # Сначала живая модель на дельте (базовые массивы + overlay):
                # она не требует ни разбора 519 МБ, ни словарной модели.
                overlay_model = _live_overlay_model(snapshot)
                if overlay_model is not None:
                    _model_cell.append(overlay_model)
                    return _model_cell[0]
                payload = _load_runtime_model_payload(
                    snapshot=snapshot,
                    runtime_model_state_path=runtime_model_state_path,
                )
                state = (payload.get("model_state") if isinstance(payload, dict)
                         else full_model_state(snapshot))
                _model_cell.append(HybridPlayerRosterEloModel.from_state(
                    state if isinstance(state, dict) else {}))
            return _model_cell[0]

        pending_series = progress["pending_series"]
        applied_maps = progress["applied_maps"]
        series_state = pending_series.get(normalized_series_key)
        applied_update: dict[str, Any] | None = None
        applied_updates: list[dict[str, Any]] = []
        pending_maps_queue: list[dict[str, Any]] = []
        wrote_model_state = False

        if isinstance(series_state, dict):
            previous_scores_raw = series_state.get("last_scores")
            previous_scores = previous_scores_raw if isinstance(previous_scores_raw, dict) else {"first": 0, "second": 0}
            pending_maps_queue = _pending_maps_list(series_state)
            # E-224: раньше единственный слот `pending_map` разбирался только на
            # сдвиг счёта РОВНО +1 у одной стороны. Пропущенный опрос двигает
            # его на 2 (одна карта пропущена целиком) или на +1 у ОБЕИХ сразу
            # (две карты пропущены по одной на сторону) — тогда исход терялся.
            # Теперь очередь хранит каждую незакрытую карту и сливает её FIFO
            # настолько, насколько объясняет сдвиг счёта.
            pending_maps_queue, applied_updates = _drain_pending_map_queue(
                pending_maps=pending_maps_queue,
                previous_scores=previous_scores,
                current_scores=current_scores,
                applied_maps=applied_maps,
                snapshot=snapshot,
                model_getter=_model,
                normalized_series_key=normalized_series_key,
                series_url=str(series_state.get("series_url") or series_url),
                winner_lookup=winner_lookup,
            )
            if applied_updates:
                applied_update = applied_updates[-1]
                wrote_model_state = True

        current_map_already_applied = normalized_map_key in applied_maps
        if current_map_already_applied:
            pending_series.pop(normalized_series_key, None)
        else:
            # Ключ карты в проде прыгает (.10 .11 … .65) на каждый опрос.
            # Пока исход неизвестен, самая свежая отложенная карта в очереди
            # остаётся собой, если это ТА ЖЕ карта (тот же match_id или оба
            # номера — series_id). Другой уникальный match_id — новая карта,
            # она ДОБАВЛЯЕТСЯ в очередь, а не заменяет старую (раньше именно
            # эта замена теряла карту N, не дождавшись сдвига счёта под неё).
            tail_pending = pending_maps_queue[-1] if pending_maps_queue else None
            tail_mid = 0
            tail_series_id = None
            if isinstance(tail_pending, dict):
                tail_rec = tail_pending.get("match_record")
                if isinstance(tail_rec, dict):
                    tail_mid = int(tail_rec.get("match_id") or 0)
                    tail_series_id = tail_rec.get("series_id")
            incoming_mid = int(getattr(match_record, "match_id", 0) or 0)
            distinct_unique = (
                incoming_mid > 0
                and tail_mid > 0
                and incoming_mid != tail_mid
                and not _match_id_is_series(incoming_mid, normalized_series_key,
                                            getattr(match_record, "series_id", None))
                and not _match_id_is_series(tail_mid, normalized_series_key,
                                            tail_series_id)
            )
            reuse_tail = (
                tail_pending is not None
                and str(tail_pending.get("map_key") or "").strip()
                and not distinct_unique
            )
            if reuse_tail:
                pending_maps_queue = pending_maps_queue[:-1] + [dict(tail_pending)]
            elif len(pending_maps_queue) < _MAX_PENDING_MAPS:
                pending_maps_queue = pending_maps_queue + [{
                    "map_key": normalized_map_key,
                    "match_record": _serialize_match_record(match_record),
                    "first_team_is_radiant": bool(first_team_is_radiant),
                    "registered_at": int(time.time()),
                }]
            # иначе очередь уже на пределе (≤6) — новую карту не берём, чтобы
            # не расти безгранично; существующие карты продолжают ждать
            # своего сдвига счёта или орфан-подбор в cyberscore_try.py.
            pending_series[normalized_series_key] = {
                "series_key": normalized_series_key,
                "series_url": str(series_url or ""),
                "last_scores": current_scores,
                "pending_maps": pending_maps_queue,
                # Legacy mirror for readers that still expect a single
                # `pending_map` slot (base/cyberscore_try.py's orphan sweep
                # reads this key straight from the JSON file, not through
                # this module) — always the oldest unresolved map, which is
                # also the one that should be resolved next.
                "pending_map": pending_maps_queue[0] if pending_maps_queue else None,
                "updated_at": int(time.time()),
            }

        _write_json_atomic(progress_path, progress)
        if wrote_model_state:
            model = _model()
            # Живые обновления пишутся ДЕЛЬТОЙ (килобайты) вместо полного
            # состояния (519 МБ): обновляется ровно то же, но не переписывается
            # целиком и не требует разбора/словарной модели на следующей карте.
            # Если модель не overlay (дельты нет — переходный период до
            # `ELO/convert_state_to_delta.py`), работает прежняя запись.
            if not _save_live_delta(model, int(base_reference_timestamp),
                                    base_model_config_signature):
                runtime_payload = {
                    "base_reference_timestamp": int(base_reference_timestamp),
                    "base_model_config_signature": base_model_config_signature,
                    "updated_at": int(time.time()),
                    "model_state": model.export_state(),
                }
                _write_json_atomic(runtime_model_state_path, runtime_payload)
            _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = None
            _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = None
            _RUNTIME_SNAPSHOT_CACHE["snapshot"] = None

    return {
        "applied_update": applied_update,
        "applied_updates": applied_updates,
        "series_key": normalized_series_key,
        "map_key": normalized_map_key,
        "current_scores": current_scores,
        "current_map_already_applied": current_map_already_applied,
    }


def finalize_live_series_from_scores(
    *,
    series_key: str,
    series_url: str,
    first_team_score: int,
    second_team_score: int,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    data_dir: Path = DEFAULT_DATA_DIR,
    rebuild_if_missing: bool = True,
    progress_path: Path = DEFAULT_RUNTIME_PROGRESS_PATH,
    runtime_model_state_path: Path = DEFAULT_RUNTIME_MODEL_STATE_PATH,
    runtime_lock_path: Path = DEFAULT_RUNTIME_LOCK_PATH,
) -> dict[str, Any] | None:
    normalized_series_key = str(series_key or "").strip() or str(series_url or "").strip()
    if not normalized_series_key:
        return None

    snapshot = ensure_snapshot(
        data_dir=data_dir,
        snapshot_path=snapshot_path,
        rebuild_if_missing=rebuild_if_missing,
    )
    if snapshot is None:
        return None
    base_reference_timestamp = _snapshot_reference_timestamp(snapshot)
    base_model_config_signature = _snapshot_model_config_signature(snapshot)
    current_scores = {"first": int(first_team_score), "second": int(second_team_score)}

    with _runtime_file_lock(runtime_lock_path):
        progress = _load_runtime_progress(
            base_reference_timestamp=base_reference_timestamp,
            model_config_signature=base_model_config_signature,
            progress_path=progress_path,
        )
        # Модель строится ЛЕНИВО, при первом обращении. Раньше её собирали здесь
        # безусловно, а нужна она только когда есть завершённая карта, которую
        # надо провести через рейтинг: разбор 219-мегабайтного рантайм-состояния
        # занимает 4.3 с, сборка модели ещё 2.6 с, пик 917 МБ — и всё это
        # выбрасывалось. По логу за 21 день на 10 241 карту приходится 376
        # применений, то есть впустую шло около 96% вызовов.
        _model_cell: list = []

        def _model() -> HybridPlayerRosterEloModel:
            if not _model_cell:
                # Сначала живая модель на дельте (базовые массивы + overlay):
                # она не требует ни разбора 519 МБ, ни словарной модели.
                overlay_model = _live_overlay_model(snapshot)
                if overlay_model is not None:
                    _model_cell.append(overlay_model)
                    return _model_cell[0]
                payload = _load_runtime_model_payload(
                    snapshot=snapshot,
                    runtime_model_state_path=runtime_model_state_path,
                )
                state = (payload.get("model_state") if isinstance(payload, dict)
                         else full_model_state(snapshot))
                _model_cell.append(HybridPlayerRosterEloModel.from_state(
                    state if isinstance(state, dict) else {}))
            return _model_cell[0]

        pending_series = progress["pending_series"]
        applied_maps = progress["applied_maps"]
        series_state = pending_series.get(normalized_series_key)
        applied_update: dict[str, Any] | None = None
        applied_updates: list[dict[str, Any]] = []
        wrote_model_state = False

        if isinstance(series_state, dict):
            previous_scores_raw = series_state.get("last_scores")
            previous_scores = previous_scores_raw if isinstance(previous_scores_raw, dict) else {"first": 0, "second": 0}
            pending_maps_queue = _pending_maps_list(series_state)
            # Same FIFO drain as register_live_map_context (E-224): the final
            # score can explain more than one still-queued map (a missed poll,
            # or the series simply ending two maps after the last observation).
            # This function has no winner_lookup, so genuinely ambiguous
            # remainders just stay unresolved below — the unconditional pop
            # at series end already discarded them before this fix too.
            _remaining, applied_updates = _drain_pending_map_queue(
                pending_maps=pending_maps_queue,
                previous_scores=previous_scores,
                current_scores=current_scores,
                applied_maps=applied_maps,
                snapshot=snapshot,
                model_getter=_model,
                normalized_series_key=normalized_series_key,
                series_url=str(series_state.get("series_url") or series_url),
            )
            if applied_updates:
                applied_update = applied_updates[-1]
                wrote_model_state = True

        pending_series.pop(normalized_series_key, None)
        _write_json_atomic(progress_path, progress)
        if wrote_model_state:
            model = _model()
            # Живые обновления пишутся ДЕЛЬТОЙ (килобайты) вместо полного
            # состояния (519 МБ): обновляется ровно то же, но не переписывается
            # целиком и не требует разбора/словарной модели на следующей карте.
            # Если модель не overlay (дельты нет — переходный период до
            # `ELO/convert_state_to_delta.py`), работает прежняя запись.
            if not _save_live_delta(model, int(base_reference_timestamp),
                                    base_model_config_signature):
                runtime_payload = {
                    "base_reference_timestamp": int(base_reference_timestamp),
                    "base_model_config_signature": base_model_config_signature,
                    "updated_at": int(time.time()),
                    "model_state": model.export_state(),
                }
                _write_json_atomic(runtime_model_state_path, runtime_payload)
            _RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = None
            _RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = None
            _RUNTIME_SNAPSHOT_CACHE["snapshot"] = None

    return {
        "applied_update": applied_update,
        "applied_updates": applied_updates,
        "series_key": normalized_series_key,
        "current_scores": current_scores,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a full live team ELO snapshot for telegram signals.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--snapshot-path", type=Path, default=DEFAULT_SNAPSHOT_PATH)
    parser.add_argument("--active-cutoff-days", type=float, default=DEFAULT_ACTIVE_CUTOFF_DAYS)
    parser.add_argument("--display-decay-half-life-days", type=float, default=DEFAULT_DISPLAY_DECAY_HALF_LIFE_DAYS)
    args = parser.parse_args()

    snapshot = build_snapshot(
        data_dir=args.data_dir,
        snapshot_path=args.snapshot_path,
        active_cutoff_days=args.active_cutoff_days,
        display_decay_half_life_days=args.display_decay_half_life_days,
    )
    meta = snapshot.get("meta") or {}
    print(
        f"Saved {int(meta.get('team_count', 0))} teams to {args.snapshot_path} "
        f"(loaded_matches={int(meta.get('loaded_matches', 0))}, "
        f"reference={meta.get('reference_utc')})"
    )


if __name__ == "__main__":
    main()
