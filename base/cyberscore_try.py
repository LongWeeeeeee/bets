import argparse
import json
import ast
from collections import deque
import orjson
import time
import random
import sys
import os
import pickle
import logging
import asyncio
import threading
import glob
import copy
import mmap
import gc
import subprocess
import re
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple
import math
from bs4 import BeautifulSoup
import requests
from functions import (
    send_message,
    synergy_and_counterpick,
    calculate_lanes,
    format_output_dict,
    STAR_THRESHOLDS_BY_WR,
)
from keys import api_to_proxy, BOOKMAKER_PROXY_URL
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Импорт для обновления про матчей
from maps_research import get_pros
# Импорт Ultimate Inference предсказателя
sys.path.insert(0, '/Users/alex/Documents/ingame/src')
try:
    from live_predictor import predict_live_match
    LIVE_PREDICTOR_AVAILABLE = True
except ImportError:
    LIVE_PREDICTOR_AVAILABLE = False
    predict_live_match = None

# Настройка логирования
logger = logging.getLogger(__name__)

try:
    try:
        from base.bookmaker_selenium_odds import (  # type: ignore
            _build_driver as _bookmaker_build_driver,
            parse_site as _bookmaker_parse_site,
            BOOKMAKER_URLS as _BOOKMAKER_URLS_MAP,
        )
    except Exception:
        from bookmaker_selenium_odds import (  # type: ignore
            _build_driver as _bookmaker_build_driver,
            parse_site as _bookmaker_parse_site,
            BOOKMAKER_URLS as _BOOKMAKER_URLS_MAP,
        )
    BOOKMAKER_PREFETCH_AVAILABLE = True
except Exception as _bookmaker_import_error:
    BOOKMAKER_PREFETCH_AVAILABLE = False
    _bookmaker_build_driver = None
    _bookmaker_parse_site = None
    _BOOKMAKER_URLS_MAP = {}
    logger.warning("Bookmaker prefetch disabled: %s", _bookmaker_import_error)

def _safe_int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return int(default)


def _safe_float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return float(default)


def _safe_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_ml_confidence_source(raw: Any, default: str) -> str:
    value = str(raw or "").strip().lower()
    if value in {"hybrid", "model_only", "star_only"}:
        return value
    return str(default).strip().lower()


# WR ladder for star ranking (defaults keep legacy behavior: 90..60).
STAR_LEVEL_MIN = _safe_int_env("STAR_LEVEL_MIN", 60)
STAR_LEVEL_MAX = _safe_int_env("STAR_LEVEL_MAX", 90)
if STAR_LEVEL_MAX < STAR_LEVEL_MIN:
    STAR_LEVEL_MIN, STAR_LEVEL_MAX = STAR_LEVEL_MAX, STAR_LEVEL_MIN
STAR_LEVEL_ORDER = list(range(STAR_LEVEL_MAX, STAR_LEVEL_MIN - 1, -1))

# Reference coverage of the current balanced baseline (used for transparency in pipeline logs/messages).
STAR_BASELINE_EARLY_COVERAGE = _safe_float_env("STAR_BASELINE_EARLY_COVERAGE", 41.09)
STAR_BASELINE_LATE_COVERAGE = _safe_float_env("STAR_BASELINE_LATE_COVERAGE", 45.84)
STAR_BASELINE_PROFILE = os.getenv(
    "STAR_BASELINE_PROFILE",
    "phase_context_metric_objDeltaCov_50k",
)
SIGNAL_WRAPPER_MODE = os.getenv("SIGNAL_WRAPPER_MODE", "ml").strip().lower()
SIGNAL_DECISION_MODE = os.getenv("SIGNAL_DECISION_MODE", "ml").strip().lower()
if SIGNAL_DECISION_MODE not in {"ml", "star"}:
    SIGNAL_DECISION_MODE = "ml"
ML_SIGNAL_MIN_WR = _safe_float_env("ML_SIGNAL_MIN_WR", float(STAR_LEVEL_MIN))
_ML_CONFIDENCE_SOURCE_GLOBAL = _normalize_ml_confidence_source(
    os.getenv("ML_CONFIDENCE_SOURCE"),
    "hybrid",
)
ML_CONFIDENCE_SOURCE_EARLY = _normalize_ml_confidence_source(
    os.getenv("ML_CONFIDENCE_SOURCE_EARLY"),
    _ML_CONFIDENCE_SOURCE_GLOBAL,
)
# Late default: model_only (user-requested runtime mode).
ML_CONFIDENCE_SOURCE_LATE = _normalize_ml_confidence_source(
    os.getenv("ML_CONFIDENCE_SOURCE_LATE"),
    "model_only",
)
DELAYED_SIGNAL_TARGET_GAME_TIME = 21 * 60
DELAYED_SIGNAL_POLL_SECONDS = 15
DELAYED_SIGNAL_NO_PROGRESS_TIMEOUT_SECONDS = 2 * 60 * 60
DELAYED_SIGNAL_NO_DATA_TIMEOUT_SECONDS = 4 * 60 * 60
# Networth-gated dispatch rules (target team is resolved by star direction sign).
NETWORTH_GATE_HARD_BLOCK_SECONDS = 4 * 60
NETWORTH_GATE_EARLY_WINDOW_END_SECONDS = 10 * 60
NETWORTH_GATE_4_TO_10_MIN_DIFF = 800.0
NETWORTH_GATE_10_MIN_MAX_LOSS = -1500.0
NETWORTH_GATE_LATE_NO_EARLY_DIFF = 1000.0
NETWORTH_GATE_LATE_OPPOSITE_DIFF = 3000.0
NETWORTH_STATUS_PRE4_BLOCK = "pre4_block"
NETWORTH_STATUS_4_10_SEND_800 = "4_10_send_800"
NETWORTH_STATUS_MIN10_LOSS_LE1500_SEND = "minute10_loss_le1500_send"
NETWORTH_STATUS_LATE_MONITOR_WAIT_1000 = "late_monitor_wait_1000"
NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000 = "late_conflict_wait_3000"
NETWORTH_STATUS_LATE_FALLBACK_21_SEND = "late_fallback_21_send"
TIER_SIGNAL_MIN_THRESHOLD_TIER1 = 60
TIER_SIGNAL_MIN_THRESHOLD_TIER2 = 65
TIER_THRESHOLD_STATUS_TIER1_MIN60_BLOCK = "tier1_min60_block"
TIER_THRESHOLD_STATUS_TIER2_MIN65_BLOCK = "tier2_min65_block"
TIER_THRESHOLD_REASON_TIER1_MIN60_BLOCK = "below_tier1_min60"
TIER_THRESHOLD_REASON_TIER2_MIN65_BLOCK = "below_tier2_min65"
# В live-режиме late-only star должен уметь попасть в delayed очередь (по умолчанию gate выключен).
LIVE_STAR_LATE_SIGNAL_GATE_ENABLED = _safe_bool_env("STAR_LATE_SIGNAL_GATE_ENABLED", False)

# Bookmaker odds prefetch (runs before draft analysis in a dedicated worker).
BOOKMAKER_PREFETCH_ENABLED = _safe_bool_env(
    "BOOKMAKER_PREFETCH_ENABLED",
    False,
) and BOOKMAKER_PREFETCH_AVAILABLE
BOOKMAKER_PREFETCH_MODE = str(os.getenv("BOOKMAKER_PREFETCH_MODE", "live")).strip().lower()
if BOOKMAKER_PREFETCH_MODE not in {"live", "all"}:
    BOOKMAKER_PREFETCH_MODE = "live"
BOOKMAKER_PREFETCH_MAX_PENDING = _safe_int_env("BOOKMAKER_PREFETCH_MAX_PENDING", 200)
BOOKMAKER_PREFETCH_RESULT_TTL_SECONDS = _safe_int_env("BOOKMAKER_PREFETCH_RESULT_TTL_SECONDS", 1800)
BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS = _safe_float_env("BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 3.0)
BOOKMAKER_PREFETCH_DRIVER_ROTATE_TASKS = _safe_int_env("BOOKMAKER_PREFETCH_DRIVER_ROTATE_TASKS", 3)
BOOKMAKER_PREFETCH_USE_SUBPROCESS = _safe_bool_env("BOOKMAKER_PREFETCH_USE_SUBPROCESS", True)
BOOKMAKER_PREFETCH_SUBPROCESS_TIMEOUT_SECONDS = _safe_int_env("BOOKMAKER_PREFETCH_SUBPROCESS_TIMEOUT_SECONDS", 90)
BOOKMAKER_PREFETCH_SITES_RAW = str(
    os.getenv("BOOKMAKER_PREFETCH_SITES", "betboom,pari,winline")
).strip()
BOOKMAKER_PREFETCH_SITES = tuple(
    s.strip().lower()
    for s in BOOKMAKER_PREFETCH_SITES_RAW.split(",")
    if s.strip()
) or ("betboom", "pari", "winline")

# Testing helpers:
# - use separate processed URL file
# - optionally disable add_url persistence to keep matches re-analysed every cycle
MAP_ID_CHECK_PATH = str(os.getenv("MAP_ID_CHECK_PATH", "map_id_check.txt")).strip() or "map_id_check.txt"
MAP_ID_CHECK_PATH_ODDS_DEFAULT = "/Users/alex/Documents/ingame/map_id_check_test.txt"
TEST_DISABLE_ADD_URL = _safe_bool_env("TEST_DISABLE_ADD_URL", False)
FORCE_ODDS_SIGNAL_TEST = _safe_bool_env("FORCE_ODDS_SIGNAL_TEST", False)

try:
    STAR_THRESHOLD_WR_TIER1 = int(os.getenv("STAR_THRESHOLD_WR_TIER1", "60"))
except ValueError:
    STAR_THRESHOLD_WR_TIER1 = 60
try:
    STAR_THRESHOLD_WR_TIER2 = int(os.getenv("STAR_THRESHOLD_WR_TIER2", "60"))
except ValueError:
    STAR_THRESHOLD_WR_TIER2 = 60

# Global/Tier filters for star signal qualification.
STAR_REQUIRE_EARLY_WITH_LATE_SAME_SIGN = _safe_bool_env(
    "STAR_REQUIRE_EARLY_WITH_LATE_SAME_SIGN",
    True,
)
# Если early/late star в разных знаках, не отбрасываем сигнал, а переводим в delayed до target game_time.
STAR_DELAY_ON_OPPOSITE_SIGNS = _safe_bool_env("STAR_DELAY_ON_OPPOSITE_SIGNS", True)
STAR_ALLOW_TIER2_FALLBACK_TO_TIER1 = _safe_bool_env("STAR_ALLOW_TIER2_FALLBACK_TO_TIER1", True)
STAR_REQUIRE_TIER2_LATE_STAR = _safe_bool_env("STAR_REQUIRE_TIER2_LATE_STAR", True)
STAR_REQUIRE_TIER2_SAME_SIGN = _safe_bool_env("STAR_REQUIRE_TIER2_SAME_SIGN", False)
STAR_ALLOW_TIER1_EARLY_STAR_LATE_SAME_OR_ZERO = _safe_bool_env(
    "STAR_ALLOW_TIER1_EARLY_STAR_LATE_SAME_OR_ZERO",
    True,
)
STAR_ALLOW_LATE_STAR_EARLY_SAME_OR_ZERO = _safe_bool_env(
    "STAR_ALLOW_LATE_STAR_EARLY_SAME_OR_ZERO",
    True,
)
STAR_CONFIDENCE_CALIBRATION_PATH = Path(
    os.getenv(
        "STAR_CONFIDENCE_CALIBRATION_PATH",
        "/Users/alex/Documents/ingame/data/star_confidence_calibration.json",
    )
)


def _load_star_confidence_calibration() -> Dict[str, Dict[int, float]]:
    if not STAR_CONFIDENCE_CALIBRATION_PATH.exists():
        return {}
    try:
        payload = json.loads(STAR_CONFIDENCE_CALIBRATION_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    phases = payload.get("phases") if isinstance(payload, dict) else None
    if not isinstance(phases, dict):
        return {}
    out: Dict[str, Dict[int, float]] = {}
    for phase in ("early", "late"):
        phase_map = phases.get(phase)
        if not isinstance(phase_map, dict):
            continue
        parsed: Dict[int, float] = {}
        for raw_level, raw_wr in phase_map.items():
            try:
                level = int(raw_level)
                wr = float(raw_wr)
            except (TypeError, ValueError):
                continue
            parsed[level] = wr
        if parsed:
            out[phase] = parsed
    return out


STAR_CONFIDENCE_CALIBRATION = _load_star_confidence_calibration()
STAR_ODDS_USE_CALIBRATION = _safe_bool_env("STAR_ODDS_USE_CALIBRATION", False)

# Fallback ladder for dynamic WR display when only base WR=60 thresholds are available.
# Multiplier compares |metric_value| to base threshold for the metric.
_STAR_INDEX_WR_MULTIPLIER_TO_LEVEL = (
    (3.00, 90),
    (2.50, 85),
    (2.00, 80),
    (1.75, 75),
    (1.50, 70),
    (1.25, 65),
    (1.00, 60),
)


class _Tee:
    def __init__(self, *streams):
        self._streams = streams
        self.encoding = getattr(streams[0], "encoding", "utf-8")
        self.errors = getattr(streams[0], "errors", "replace")

    def write(self, data):
        for stream in self._streams:
            try:
                stream.write(data)
            except Exception:
                pass

    def flush(self):
        for stream in self._streams:
            try:
                stream.flush()
            except Exception:
                pass

    def isatty(self):
        stream = self._streams[0] if self._streams else None
        if stream is None:
            return False
        return getattr(stream, "isatty", lambda: False)()

    def fileno(self):
        stream = self._streams[0] if self._streams else None
        if stream is None:
            raise OSError("No underlying stream")
        return stream.fileno()


def _setup_run_logging():
    root_dir = Path(__file__).resolve().parent.parent
    log_path = root_dir / "log.txt"
    log_file = open(log_path, "w", encoding="utf-8", buffering=1)

    stdout = sys.stdout
    stderr = sys.stderr
    sys.stdout = _Tee(stdout, log_file)
    sys.stderr = _Tee(stderr, log_file)
    root_logger = logging.getLogger()
    if root_logger.level == logging.NOTSET or root_logger.level > logging.INFO:
        root_logger.setLevel(logging.INFO)
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
    else:
        for handler in root_logger.handlers:
            try:
                handler.setFormatter(logging.Formatter("%(message)s"))
            except Exception:
                pass
            if isinstance(handler, logging.StreamHandler):
                handler.setStream(sys.stderr)
    logger.info("Logging to %s", log_path)


def _coerce_metric_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        if v.endswith('*'):
            v = v[:-1]
        if not v:
            return None
        try:
            return float(v)
        except ValueError:
            return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _block_hits_thresholds(data: dict, thresholds: list[tuple]) -> bool:
    star_count = 0
    block_sign = None
    conflict = False
    for key, threshold in thresholds:
        val = _coerce_metric_value(data.get(key))
        if val is None:
            continue
        if abs(val) >= threshold:
            star_count += 1
            sign = 1 if val > 0 else (-1 if val < 0 else None)
            if sign is None:
                continue
            if block_sign is None:
                block_sign = sign
            elif block_sign != sign:
                conflict = True
                break
    return star_count > 0 and not conflict


def _recommend_odds_for_block(data: dict, phase: str) -> Optional[dict]:
    if not isinstance(data, dict):
        return None

    # Рекомендации считаем только по фактическим STAR-метрикам (с '*'),
    # чтобы не получать "высокие уровни" на незвездных числах.
    star_only_data: Dict[str, float] = {}
    for metric, raw in data.items():
        if not isinstance(raw, str) or not raw.strip().endswith('*'):
            continue
        value = _coerce_metric_value(raw)
        if value is None or value == 0:
            continue
        star_only_data[str(metric)] = float(value)
    if not star_only_data:
        return None
    # В одном блоке ожидаем единый знак STAR-метрик; при конфликте не даем рекомендацию.
    star_signs = {1 if v > 0 else -1 for v in star_only_data.values() if v != 0}
    if len(star_signs) > 1:
        return None

    section = 'early_output' if phase == 'early' else 'mid_output'
    available_levels = [
        int(level)
        for level, payload in STAR_THRESHOLDS_BY_WR.items()
        if isinstance(payload, dict) and payload.get(section)
    ]
    # If runtime has only one WR-level table (typically fallback WR60),
    # recover dynamic WR display by comparing metric indexes to base thresholds.
    if len(set(available_levels)) <= 1:
        base_level = available_levels[0] if available_levels else 60
        base_payload = STAR_THRESHOLDS_BY_WR.get(base_level) or STAR_THRESHOLDS_BY_WR.get(60, {})
        base_rows = base_payload.get(section, []) if isinstance(base_payload, dict) else []
        thresholds_by_metric: Dict[str, int] = {}
        for metric, raw_threshold in base_rows:
            try:
                thresholds_by_metric[str(metric)] = max(1, int(raw_threshold))
            except (TypeError, ValueError):
                continue
        if not thresholds_by_metric:
            return None

        # Консервативно: ориентируемся на самый слабый starred-индекс, а не на самый сильный.
        metric_ratios: List[float] = []
        for metric, value in star_only_data.items():
            threshold = thresholds_by_metric.get(metric)
            if threshold is None:
                return None
            metric_ratios.append(abs(value) / float(threshold))
        if not metric_ratios:
            return None

        weakest_ratio = min(metric_ratios)
        if weakest_ratio < 1.0:
            return None

        best_level = 60
        for min_ratio, wr_level in _STAR_INDEX_WR_MULTIPLIER_TO_LEVEL:
            if weakest_ratio >= min_ratio:
                best_level = int(wr_level)
                break
        best_level = max(STAR_LEVEL_MIN, min(STAR_LEVEL_MAX, best_level))
        wr_pct = float(best_level)
        if STAR_ODDS_USE_CALIBRATION:
            phase_key = "early" if phase == "early" else "late"
            calibrated_wr = STAR_CONFIDENCE_CALIBRATION.get(phase_key, {}).get(best_level)
            if calibrated_wr is not None:
                wr_pct = float(calibrated_wr)
        if wr_pct <= 0:
            wr_pct = float(best_level)
        min_odds = round(100.0 / wr_pct, 2)
        return {
            'level': best_level,
            'min_odds': min_odds,
            'wr_pct': wr_pct,
        }

    best_level = None
    for level in STAR_LEVEL_ORDER:
        thresholds = STAR_THRESHOLDS_BY_WR.get(level, {}).get(section, [])
        if not thresholds:
            continue
        threshold_map: Dict[str, int] = {}
        for metric, threshold in thresholds:
            try:
                threshold_map[str(metric)] = int(threshold)
            except (TypeError, ValueError):
                continue
        if not threshold_map:
            continue
        # Консервативно: уровень WR валиден только если КАЖДАЯ STAR-метрика
        # проходит порог своего индекса на этом WR-уровне.
        level_ok = True
        for metric, value in star_only_data.items():
            threshold = threshold_map.get(metric)
            if threshold is None or abs(value) < threshold:
                level_ok = False
                break
        if level_ok:
            best_level = level
            break
    if best_level is None:
        return None
    # Минимальный кэф по уровню (шаг 0.01)
    wr_pct = float(best_level)
    if STAR_ODDS_USE_CALIBRATION:
        phase_key = "early" if phase == "early" else "late"
        calibrated_wr = STAR_CONFIDENCE_CALIBRATION.get(phase_key, {}).get(best_level)
        if calibrated_wr is not None:
            wr_pct = float(calibrated_wr)
    if wr_pct <= 0:
        wr_pct = float(best_level)
    min_odds = round(100.0 / wr_pct, 2)
    return {
        'level': best_level,
        'min_odds': min_odds,
        'wr_pct': wr_pct,
    }


def _extract_ml_block_confidence_pct(data: dict, phase: str) -> Optional[float]:
    if not isinstance(data, dict):
        return None
    phase_key = "early" if phase == "early" else "late"
    ml_meta = data.get("_ml_meta")
    if not isinstance(ml_meta, dict):
        return None
    phase_meta = ml_meta.get(phase_key)
    if not isinstance(phase_meta, dict):
        return None
    probs = phase_meta.get("probs")
    if not isinstance(probs, dict) or not probs:
        return None
    # Учитываем confidence только для метрик, которые реально остались в блоке
    # после применения wrapper (value != 0). Иначе "сырой" prob для уже
    # обнуленной метрики создает шум (например 20-40%), хотя ML-сигнала нет.
    alive_metrics = {
        key
        for key, raw in data.items()
        if key in {"counterpick_1vs1", "counterpick_1vs2", "solo", "synergy_duo", "synergy_trio"}
        and _coerce_metric_value(raw) not in (None, 0.0)
    }
    if not alive_metrics:
        return None
    vals = []
    for metric, raw in probs.items():
        if metric not in alive_metrics:
            continue
        try:
            p = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(p):
            vals.append(max(0.0, min(1.0, p)) * 100.0)
    if not vals:
        return None
    return max(vals)


def _resolve_ml_block_confidence(data: dict, phase: str, source_mode: str) -> Tuple[Optional[float], str]:
    mode = _normalize_ml_confidence_source(source_mode, "hybrid")
    if mode == "model_only":
        wr = _extract_ml_block_confidence_pct(data, phase)
        return wr, ("model_prob_max" if wr is not None else "n/a")
    if mode == "star_only":
        rec = _recommend_odds_for_block(data, phase)
        if rec is None:
            return None, "n/a"
        return float(rec.get("wr_pct")), "star_calibrated"
    # hybrid: prefer star-calibrated WR, fallback to model probability.
    rec = _recommend_odds_for_block(data, phase)
    if rec is not None:
        return float(rec.get("wr_pct")), "star_calibrated"
    wr = _extract_ml_block_confidence_pct(data, phase)
    return wr, ("model_prob_max" if wr is not None else "n/a")


def _star_block_sign(block: Optional[dict]) -> Optional[int]:
    if not isinstance(block, dict):
        return None
    signs = set()
    for raw_value in block.values():
        if not isinstance(raw_value, str) or not raw_value.strip().endswith('*'):
            continue
        value = _coerce_metric_value(raw_value)
        if value is None or value == 0:
            continue
        signs.add(1 if value > 0 else -1)
    if not signs:
        return None
    if len(signs) > 1:
        return 0
    return next(iter(signs))


_STAR_METRIC_ORDER = (
    "counterpick_1vs1",
    "counterpick_1vs2",
    "solo",
    "synergy_duo",
    "synergy_trio",
)
_STAR_METRIC_SHORT = {
    "counterpick_1vs1": "cp1v1",
    "counterpick_1vs2": "cp1v2",
    "solo": "solo",
    "synergy_duo": "duo",
    "synergy_trio": "trio",
}


def _star_thresholds_for_wr(target_wr: int, section: str) -> Dict[str, int]:
    try:
        wr_level = int(target_wr)
    except (TypeError, ValueError):
        wr_level = 60
    payload = STAR_THRESHOLDS_BY_WR.get(wr_level) or STAR_THRESHOLDS_BY_WR.get(60, {})
    raw = payload.get(section, []) if isinstance(payload, dict) else []
    out: Dict[str, int] = {}
    for metric, threshold in raw:
        try:
            out[str(metric)] = int(threshold)
        except (TypeError, ValueError):
            continue
    return out


def _format_metric_value(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.1f}"


def _star_block_diagnostics(raw_block: Optional[dict], target_wr: int, section: str) -> Dict[str, Any]:
    block = raw_block if isinstance(raw_block, dict) else {}
    thresholds = _star_thresholds_for_wr(target_wr, section)
    hit_metrics: List[str] = []
    hit_signs: set[int] = set()

    for metric, threshold in thresholds.items():
        value = _coerce_metric_value(block.get(metric))
        if value is None or abs(value) < threshold:
            continue
        hit_metrics.append(metric)
        if value > 0:
            hit_signs.add(1)
        elif value < 0:
            hit_signs.add(-1)

    if not hit_metrics:
        return {
            "valid": False,
            "status": "no_hits",
            "sign": None,
            "hit_metrics": [],
            "conflict_metric": None,
        }
    if len(hit_signs) > 1:
        return {
            "valid": False,
            "status": "conflict_hits",
            "sign": 0,
            "hit_metrics": hit_metrics,
            "conflict_metric": None,
        }

    block_sign = next(iter(hit_signs)) if hit_signs else None
    if block_sign in (-1, 1):
        for metric in thresholds:
            value = _coerce_metric_value(block.get(metric))
            if value is None or value == 0:
                continue
            sign = 1 if value > 0 else -1
            if sign != block_sign:
                return {
                    "valid": False,
                    "status": "conflict_sign",
                    "sign": block_sign,
                    "hit_metrics": hit_metrics,
                    "conflict_metric": metric,
                }

    return {
        "valid": block_sign in (-1, 1),
        "status": "ok" if block_sign in (-1, 1) else "no_sign",
        "sign": block_sign,
        "hit_metrics": hit_metrics,
        "conflict_metric": None,
    }


def _format_star_block_status(diag: Dict[str, Any]) -> str:
    status = str(diag.get("status") or "unknown")
    if status == "ok":
        return "ok"
    if status == "no_hits":
        return "no_hits"
    if status == "conflict_hits":
        return "conflict_hits"
    if status == "conflict_sign":
        metric = str(diag.get("conflict_metric") or "")
        short = _STAR_METRIC_SHORT.get(metric, metric or "?")
        return f"conflict_sign({short})"
    return status


def _star_match_status_from_diags(early_diag: Dict[str, Any], late_diag: Dict[str, Any], match_tier: int) -> str:
    has_early_star = bool(early_diag.get("valid"))
    has_late_star = bool(late_diag.get("valid"))
    if not has_late_star:
        return "skip_no_late_star"
    early_sign = early_diag.get("sign") if has_early_star else None
    late_sign = late_diag.get("sign") if has_late_star else None
    if has_early_star and early_sign == late_sign:
        return "send_now_same_sign"
    if has_early_star and early_sign != late_sign:
        return "delay_late_only_opposite_signs"
    return "delay_late_only_no_early"


def _block_signs_same_or_zero(raw_block: Optional[dict], expected_sign: Optional[int]) -> Dict[str, Any]:
    if expected_sign not in (-1, 1):
        return {
            "valid": False,
            "status": "no_expected_sign",
            "nonzero_metrics": [],
            "conflicting_metrics": [],
        }
    block = raw_block if isinstance(raw_block, dict) else {}
    nonzero_metrics: List[str] = []
    conflicting_metrics: List[str] = []
    for metric in _STAR_METRIC_ORDER:
        value = _coerce_metric_value(block.get(metric))
        if value is None or value == 0:
            continue
        nonzero_metrics.append(metric)
        sign = 1 if value > 0 else -1
        if sign != expected_sign:
            conflicting_metrics.append(metric)
    return {
        "valid": len(conflicting_metrics) == 0,
        "status": "ok" if len(conflicting_metrics) == 0 else "conflict_signs",
        "nonzero_metrics": nonzero_metrics,
        "conflicting_metrics": conflicting_metrics,
    }


def _format_raw_star_block_metrics(
    raw_block: Optional[dict],
    section: str,
    primary_wr: int,
    fallback_wr: Optional[int] = None,
) -> str:
    block = raw_block if isinstance(raw_block, dict) else {}
    wr_levels: List[int] = [int(primary_wr)]
    if fallback_wr is not None and int(fallback_wr) != int(primary_wr):
        wr_levels.append(int(fallback_wr))

    thresholds_by_wr = {wr: _star_thresholds_for_wr(wr, section) for wr in wr_levels}
    tokens: List[str] = []

    for metric in _STAR_METRIC_ORDER:
        value = _coerce_metric_value(block.get(metric))
        if value is None or value == 0:
            continue
        checks: List[str] = []
        for wr in wr_levels:
            threshold = thresholds_by_wr.get(wr, {}).get(metric)
            if threshold is None:
                continue
            hit = "Y" if abs(value) >= threshold else "N"
            checks.append(f"{wr}>={threshold}:{hit}")
        if not checks:
            continue
        metric_short = _STAR_METRIC_SHORT.get(metric, metric)
        tokens.append(f"{metric_short}={_format_metric_value(value)}[{';'.join(checks)}]")

    return ", ".join(tokens) if tokens else "none"


def _decorate_star_block_for_display(
    raw_block: Optional[dict],
    section: str,
    target_wr: int,
) -> Dict[str, Any]:
    src = raw_block if isinstance(raw_block, dict) else {}
    out = dict(src)
    diag = _star_block_diagnostics(
        raw_block=src,
        target_wr=target_wr,
        section=section,
    )
    if not bool(diag.get("valid")):
        return out
    block_sign = int(diag.get("sign") or 0)
    if block_sign not in (-1, 1):
        return out
    thresholds = _star_thresholds_for_wr(target_wr, section)
    for metric, threshold in thresholds.items():
        value = _coerce_metric_value(src.get(metric))
        if value is None or value == 0:
            continue
        if block_sign > 0 and value <= 0:
            continue
        if block_sign < 0 and value >= 0:
            continue
        if abs(value) < threshold:
            continue
        out[metric] = f"{_format_metric_value(value)}*"
    return out


def _should_delay_star_signal(
    early_output: Optional[dict],
    mid_output: Optional[dict],
    target_wr: int,
) -> tuple[bool, str]:
    early_diag = _star_block_diagnostics(
        raw_block=early_output,
        target_wr=target_wr,
        section="early_output",
    )
    late_diag = _star_block_diagnostics(
        raw_block=mid_output,
        target_wr=target_wr,
        section="mid_output",
    )
    has_early_star = bool(early_diag.get("valid"))
    has_late_star = bool(late_diag.get("valid"))
    early_sign = early_diag.get("sign") if has_early_star else None
    late_sign = late_diag.get("sign") if has_late_star else None

    if has_late_star and not has_early_star:
        return True, 'late_only_star'
    if has_early_star and has_late_star and early_sign != late_sign:
        return True, 'opposite_star_signs'
    return False, ''


def _target_side_from_sign(sign: Optional[int]) -> Optional[str]:
    if sign == 1:
        return "radiant"
    if sign == -1:
        return "dire"
    return None


def _target_networth_diff_from_radiant_lead(
    radiant_lead: Any,
    target_side: Optional[str],
) -> Optional[float]:
    if target_side not in {"radiant", "dire"}:
        return None
    try:
        lead_value = float(radiant_lead)
    except (TypeError, ValueError):
        return None
    return lead_value if target_side == "radiant" else -lead_value


def _format_game_clock(game_time_seconds: Any) -> str:
    try:
        sec = max(0.0, float(game_time_seconds or 0.0))
    except (TypeError, ValueError):
        sec = 0.0
    return f"{int(sec // 60):02d}:{int(sec % 60):02d}"


def _fetch_delayed_match_state(json_url: Optional[str]) -> Optional[Dict[str, Optional[float]]]:
    if not json_url:
        return None
    try:
        resp = make_request_with_retry(
            json_url,
            max_retries=3,
            retry_delay=2,
            headers=globals().get('headers'),
        )
    except Exception as e:
        print(f"⚠️ Delayed sender: ошибка запроса game_time ({json_url}): {e}")
        return None
    if not resp or resp.status_code != 200:
        return None
    try:
        payload = resp.json()
    except Exception:
        return None

    game_time = payload.get('game_time')
    if game_time is None:
        live_league = payload.get('live_league_data') or {}
        game_time = live_league.get('game_time')
    if game_time is None:
        live_match = (payload.get('live_league_data') or {}).get('match') or {}
        game_time = live_match.get('game_time')
    if game_time is None:
        return None

    radiant_lead = payload.get('radiant_lead')
    if radiant_lead is None:
        live_league = payload.get('live_league_data') or {}
        radiant_lead = live_league.get('radiant_lead')
    if radiant_lead is None:
        live_match = (payload.get('live_league_data') or {}).get('match') or {}
        radiant_lead = live_match.get('radiant_lead')

    try:
        game_time_value = float(game_time)
    except (TypeError, ValueError):
        return None
    lead_value: Optional[float]
    try:
        lead_value = float(radiant_lead) if radiant_lead is not None else None
    except (TypeError, ValueError):
        lead_value = None
    return {"game_time": game_time_value, "radiant_lead": lead_value}


def _fetch_delayed_match_game_time(json_url: Optional[str]) -> Optional[float]:
    state = _fetch_delayed_match_state(json_url)
    if not isinstance(state, dict):
        return None
    value = state.get("game_time")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _drain_due_delayed_signals_once() -> None:
    with monitored_matches_lock:
        queued_items = list(monitored_matches.items())
    if not queued_items:
        return

    now_ts = time.time()
    for match_key, payload in queued_items:
        if _is_url_processed(match_key):
            _drop_delayed_match(match_key, reason="already_processed")
            continue
        target_game_time = float(payload.get('target_game_time', DELAYED_SIGNAL_TARGET_GAME_TIME))
        delayed_state = _fetch_delayed_match_state(payload.get('json_url'))
        if not isinstance(delayed_state, dict):
            queued_at = float(payload.get('queued_at', now_ts))
            # После 4 часов без game_time считаем сигнал устаревшим.
            if now_ts - queued_at > DELAYED_SIGNAL_NO_DATA_TIMEOUT_SECONDS:
                with monitored_matches_lock:
                    monitored_matches.pop(match_key, None)
                print(f"⚠️ Delayed сигнал устарел и удален: {match_key}")
            continue
        current_game_time_raw = delayed_state.get("game_time")
        try:
            current_game_time = float(current_game_time_raw)
        except (TypeError, ValueError):
            continue
        current_radiant_lead = delayed_state.get("radiant_lead")

        with monitored_matches_lock:
            current_payload = monitored_matches.get(match_key)
            if current_payload is None:
                continue
            prev_game_time = float(current_payload.get('last_game_time', current_game_time))
            last_progress_at = float(current_payload.get('last_progress_at', current_payload.get('queued_at', now_ts)))
            if current_game_time > prev_game_time + 1:
                last_progress_at = now_ts
            current_payload['last_game_time'] = current_game_time
            current_payload['last_checked_at'] = now_ts
            current_payload['last_progress_at'] = last_progress_at

        monitor_threshold: Optional[float] = None
        monitor_threshold_raw = payload.get("networth_monitor_threshold")
        if monitor_threshold_raw is not None:
            try:
                monitor_threshold = float(monitor_threshold_raw)
            except (TypeError, ValueError):
                monitor_threshold = None
        monitor_target_side = str(payload.get("networth_target_side") or "").strip().lower()
        monitor_deadline_raw = payload.get("networth_monitor_deadline_game_time", target_game_time)
        try:
            monitor_deadline_game_time = float(monitor_deadline_raw)
        except (TypeError, ValueError):
            monitor_deadline_game_time = float(target_game_time)
        monitor_ready = False
        monitor_target_diff: Optional[float] = None
        if (
            monitor_threshold is not None
            and monitor_target_side in {"radiant", "dire"}
            and current_game_time < monitor_deadline_game_time
        ):
            monitor_target_diff = _target_networth_diff_from_radiant_lead(
                current_radiant_lead,
                monitor_target_side,
            )
            if (
                monitor_target_diff is not None
                and monitor_target_diff >= monitor_threshold
            ):
                monitor_ready = True

        if not monitor_ready and current_game_time < target_game_time:
            if now_ts - last_progress_at > DELAYED_SIGNAL_NO_PROGRESS_TIMEOUT_SECONDS:
                with monitored_matches_lock:
                    monitored_matches.pop(match_key, None)
                print(
                    f"⚠️ Delayed сигнал удален (нет прогресса game_time): "
                    f"{match_key}, last_game_time={int(current_game_time)}"
                )
            continue

        if _skip_dispatch_for_processed_url(match_key, "delayed отправки перед lock", indent=""):
            continue
        if not _acquire_signal_send_slot(match_key):
            print(f"⚠️ Пропуск delayed отправки: уже идет dispatch для {match_key}")
            continue
        try:
            if _skip_dispatch_for_processed_url(match_key, "delayed отправки после lock", indent=""):
                continue
            send_message(payload.get('message', ''))
            _mark_url_processed(match_key)
            reason = payload.get('reason', 'unknown')
            fallback_send_status_label = str(
                payload.get("fallback_send_status_label") or NETWORTH_STATUS_LATE_FALLBACK_21_SEND
            )
            if monitor_ready and monitor_threshold is not None and monitor_target_diff is not None:
                print(
                    f"⏱️ Отложенный сигнал отправлен раньше fallback: {match_key} "
                    f"(reason={reason}, game_time={int(current_game_time)}, "
                    f"target_networth_diff={int(monitor_target_diff)}, "
                    f"threshold={int(monitor_threshold)})"
                )
            else:
                print(
                    f"⏱️ Отложенный сигнал отправлен: {match_key} "
                    f"(reason={reason}, status={fallback_send_status_label}, game_time={int(current_game_time)})"
                )
            add_url_reason = str(payload.get('add_url_reason') or 'star_signal_sent_delayed')
            add_url_details = payload.get('add_url_details')
            if not isinstance(add_url_details, dict):
                add_url_details = {}
            add_url_details = dict(add_url_details)
            add_url_details.setdefault('sent_game_time', int(current_game_time))
            if monitor_ready and monitor_threshold is not None and monitor_target_diff is not None:
                add_url_details.setdefault("networth_monitor_early_release", True)
                add_url_details.setdefault("networth_monitor_threshold", float(monitor_threshold))
                add_url_details.setdefault("target_networth_diff", float(monitor_target_diff))
            else:
                add_url_details.setdefault("dispatch_status_label", fallback_send_status_label)
            add_url(match_key, reason=add_url_reason, details=add_url_details)
        except Exception as e:
            print(f"⚠️ Ошибка отправки отложенного сигнала {match_key}: {e}")
        finally:
            _release_signal_send_slot(match_key)


def _delayed_sender_loop() -> None:
    while not delayed_sender_stop_event.is_set():
        try:
            _drain_due_delayed_signals_once()
        except Exception as e:
            print(f"⚠️ Delayed sender loop error: {e}")
        delayed_sender_stop_event.wait(DELAYED_SIGNAL_POLL_SECONDS)


def _ensure_delayed_sender_started() -> None:
    global delayed_sender_thread
    if delayed_sender_thread is not None and delayed_sender_thread.is_alive():
        return
    delayed_sender_stop_event.clear()
    delayed_sender_thread = threading.Thread(
        target=_delayed_sender_loop,
        name='delayed-signal-sender',
        daemon=True,
    )
    delayed_sender_thread.start()
    print("🧵 Delayed sender worker started")


def _bookmaker_urls_for_mode(mode: str) -> Dict[str, str]:
    raw = _BOOKMAKER_URLS_MAP or {}
    if not isinstance(raw, dict):
        return {}
    maybe_nested = raw.get(mode)
    if isinstance(maybe_nested, dict):
        return {str(k): str(v) for k, v in maybe_nested.items()}
    # backward compatibility if module provides flat map
    if all(isinstance(v, str) for v in raw.values()):
        return {str(k): str(v) for k, v in raw.items()}
    return {}


def _bookmaker_infer_map_num(live_league_data: Optional[dict], score_text: str = "") -> Optional[int]:
    payload = live_league_data if isinstance(live_league_data, dict) else {}
    match_payload = payload.get("match") if isinstance(payload.get("match"), dict) else {}

    def _to_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    r_wins = _to_int(payload.get("radiant_series_wins"))
    d_wins = _to_int(payload.get("dire_series_wins"))
    if r_wins is None:
        r_wins = _to_int(match_payload.get("radiant_series_wins"))
    if d_wins is None:
        d_wins = _to_int(match_payload.get("dire_series_wins"))

    if r_wins is not None and d_wins is not None:
        inferred = r_wins + d_wins + 1
        if 1 <= inferred <= 5:
            return inferred

    m = re.search(r"(\d+)\s*:\s*(\d+)", str(score_text or ""))
    if m:
        try:
            inferred = int(m.group(1)) + int(m.group(2)) + 1
        except Exception:
            inferred = None
        if inferred is not None and 1 <= inferred <= 5:
            return inferred
    return None


def _bookmaker_prefetch_prune_locked(now_ts: float) -> None:
    if not bookmaker_prefetch_results:
        return
    ttl = max(60, int(BOOKMAKER_PREFETCH_RESULT_TTL_SECONDS))
    to_drop = []
    for match_key, payload in bookmaker_prefetch_results.items():
        status = str(payload.get("status") or "")
        finished_at = float(payload.get("finished_at") or payload.get("submitted_at") or 0.0)
        if status in {"done", "error"} and now_ts - finished_at > ttl:
            to_drop.append(match_key)
    for match_key in to_drop:
        bookmaker_prefetch_results.pop(match_key, None)


def _bookmaker_prefetch_lookup(match_key: str, wait_seconds: float = 0.0) -> Optional[dict]:
    if not BOOKMAKER_PREFETCH_ENABLED:
        return None
    wait_seconds = max(0.0, float(wait_seconds or 0.0))
    deadline = time.time() + wait_seconds
    with bookmaker_prefetch_condition:
        while True:
            payload = bookmaker_prefetch_results.get(match_key)
            if payload is None:
                return None
            status = str(payload.get("status") or "")
            if status in {"done", "error"}:
                return copy.deepcopy(payload)
            if wait_seconds <= 0.0:
                return copy.deepcopy(payload)
            remaining = deadline - time.time()
            if remaining <= 0:
                return copy.deepcopy(payload)
            bookmaker_prefetch_condition.wait(timeout=min(0.2, remaining))


def _bookmaker_extract_match_id(match_key: str) -> Optional[str]:
    m = re.search(r"/matches/(\d+)", str(match_key or ""))
    if not m:
        return None
    return str(m.group(1))


def _bookmaker_source_snapshot_rows(match_key: str) -> Tuple[str, List[Dict[str, Any]]]:
    snapshot = _bookmaker_prefetch_lookup(match_key, wait_seconds=0.0)
    if not isinstance(snapshot, dict):
        return "", []
    map_context = ""
    map_num_raw = snapshot.get("map_num")
    if isinstance(map_num_raw, int) and 1 <= map_num_raw <= 5:
        map_context = f"карта {map_num_raw}"
    sites_payload = snapshot.get("sites")
    if not isinstance(sites_payload, dict):
        return map_context, []
    label_map = {
        "betboom": "BetBoom",
        "pari": "Pari",
        "winline": "Winline",
    }
    rows: List[Dict[str, Any]] = []
    for site in BOOKMAKER_PREFETCH_SITES:
        site_payload = sites_payload.get(site)
        p1: Optional[float] = None
        p2: Optional[float] = None
        reason: Optional[str] = None
        if isinstance(site_payload, dict):
            if bool(site_payload.get("market_closed")):
                reason = "map_market_closed"
            source_name = str(site_payload.get("source") or "").strip()
            if source_name:
                reason = source_name
            odds = site_payload.get("odds")
            if isinstance(odds, list) and len(odds) >= 2 and not bool(site_payload.get("market_closed")):
                try:
                    p1 = float(odds[0])
                    p2 = float(odds[1])
                except (TypeError, ValueError):
                    p1 = None
                    p2 = None
        rows.append(
            {
                "bookmaker": label_map.get(site, site),
                "П1": p1,
                "П2": p2,
                "reason": reason,
            }
        )
    return map_context, rows


def _log_bookmaker_source_snapshot(match_key: str, decision: str) -> None:
    if decision not in {"sent", "no_numeric_odds"}:
        return
    match_url = str(match_key or "")
    map_context, rows = _bookmaker_source_snapshot_rows(match_url)
    if not rows:
        rows = [{"bookmaker": "unknown", "П1": None, "П2": None}]
    base_payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "match_id": _bookmaker_extract_match_id(match_url),
        "url": match_url,
        "map_context": map_context,
        "decision": decision,
    }
    for row in rows:
        payload = dict(base_payload)
        payload.update(row)
        snapshot_line = "BOOKMAKER_SOURCE_SNAPSHOT " + json.dumps(payload, ensure_ascii=False)
        logger.info(snapshot_line)
        # Keep snapshot visible in stdout/stderr-tail windows used for live ops evidence.
        print(snapshot_line)


def _bookmaker_format_odds_block(match_key: str) -> Tuple[str, bool, str]:
    snapshot = _bookmaker_prefetch_lookup(
        match_key,
        wait_seconds=BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS,
    )
    if not snapshot:
        return "", False, "prefetch_not_found"
    snapshot_status = str(snapshot.get("status") or "")
    if snapshot_status != "done":
        return "", False, f"prefetch_{snapshot_status or 'unknown'}"
    sites_payload = snapshot.get("sites")
    if not isinstance(sites_payload, dict):
        return "", False, "invalid_sites_payload"
    label_map = {
        "betboom": "BetBoom",
        "pari": "Pari",
        "winline": "Winline",
    }
    lines = []
    has_numeric_odds = False
    for site in BOOKMAKER_PREFETCH_SITES:
        site_payload = sites_payload.get(site)
        site_label = label_map.get(site, site)
        if not isinstance(site_payload, dict):
            lines.append(f"{site_label}: —")
            continue
        if bool(site_payload.get("market_closed")):
            lines.append(f"{site_label}: —")
            continue
        odds = site_payload.get("odds")
        if isinstance(odds, list) and len(odds) >= 2:
            try:
                p1 = float(odds[0])
                p2 = float(odds[1])
                lines.append(f"{site_label}: П1 {p1:.2f} | П2 {p2:.2f}")
                has_numeric_odds = True
                continue
            except (TypeError, ValueError):
                pass
        lines.append(f"{site_label}: —")
    if not lines or not has_numeric_odds:
        return "", False, "no_numeric_odds"
    mode = str(snapshot.get("mode") or BOOKMAKER_PREFETCH_MODE)
    map_num_raw = snapshot.get("map_num")
    map_num = int(map_num_raw) if isinstance(map_num_raw, int) and 1 <= map_num_raw <= 5 else None
    map_suffix = f", карта {map_num}" if map_num is not None else ""
    return f"Букмекеры ({mode}{map_suffix}):\n" + "\n".join(lines) + "\n", True, "ok"


def _bookmaker_prefetch_submit(
    match_key: str,
    radiant_team: str,
    dire_team: str,
    map_num: Optional[int] = None,
) -> None:
    if not BOOKMAKER_PREFETCH_ENABLED:
        return
    if not match_key:
        return
    _ensure_bookmaker_prefetch_started()
    now_ts = time.time()
    with bookmaker_prefetch_condition:
        _bookmaker_prefetch_prune_locked(now_ts)
        existing = bookmaker_prefetch_results.get(match_key)
        if isinstance(existing, dict):
            status = str(existing.get("status") or "")
            if status in {"queued", "running", "done"}:
                return
        if len(bookmaker_prefetch_queue) >= max(10, int(BOOKMAKER_PREFETCH_MAX_PENDING)):
            print(f"   ⚠️ Bookmaker prefetch queue overflow ({len(bookmaker_prefetch_queue)}), skip {match_key}")
            return
        bookmaker_prefetch_results[match_key] = {
            "status": "queued",
            "mode": BOOKMAKER_PREFETCH_MODE,
            "submitted_at": now_ts,
            "radiant_team": str(radiant_team or ""),
            "dire_team": str(dire_team or ""),
            "map_num": int(map_num) if isinstance(map_num, int) and 1 <= map_num <= 5 else None,
            "sites": {},
        }
        bookmaker_prefetch_queue.append(
            {
                "match_key": match_key,
                "radiant_team": str(radiant_team or ""),
                "dire_team": str(dire_team or ""),
                "map_num": int(map_num) if isinstance(map_num, int) and 1 <= map_num <= 5 else None,
                "mode": BOOKMAKER_PREFETCH_MODE,
                "submitted_at": now_ts,
            }
        )
        bookmaker_prefetch_condition.notify()
    print(f"   📥 Bookmaker prefetch queued: {match_key}")


def _bookmaker_prefetch_fetch_subprocess(
    radiant_team: str,
    dire_team: str,
    mode: str,
    map_num: Optional[int] = None,
) -> Dict[str, dict]:
    script_path = Path(__file__).resolve().parent / "bookmaker_selenium_odds.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--mode",
        str(mode or BOOKMAKER_PREFETCH_MODE),
        "--team1",
        str(radiant_team or ""),
        "--team2",
        str(dire_team or ""),
        "--odds",
        "true" if BOOKMAKER_PREFETCH_ENABLED else "false",
    ]
    if BOOKMAKER_PREFETCH_SITES:
        cmd.extend(["--sites", *BOOKMAKER_PREFETCH_SITES])
    if isinstance(map_num, int) and 1 <= map_num <= 5:
        cmd.extend(["--map-num", str(map_num)])

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=max(10, int(BOOKMAKER_PREFETCH_SUBPROCESS_TIMEOUT_SECONDS)),
        cwd=str(Path(__file__).resolve().parent.parent),
    )
    if proc.returncode != 0:
        err_tail = (proc.stderr or proc.stdout or "").strip()[-700:]
        raise RuntimeError(f"bookmaker subprocess failed rc={proc.returncode}: {err_tail}")

    raw = (proc.stdout or "").strip()
    if not raw:
        raise RuntimeError("bookmaker subprocess returned empty stdout")

    payload = None
    try:
        payload = json.loads(raw)
    except Exception:
        # Fallback: some environments may prepend stray lines before JSON.
        start = raw.rfind("{")
        if start >= 0:
            payload = json.loads(raw[start:])
    if not isinstance(payload, dict):
        raise RuntimeError("bookmaker subprocess returned invalid JSON payload")

    sites_payload: Dict[str, dict] = {}
    for item in payload.get("results") or []:
        if not isinstance(item, dict):
            continue
        site = str(item.get("site") or "").strip().lower()
        if not site:
            continue
        sites_payload[site] = {
            "status": str(item.get("status") or ""),
            "match_found": bool(item.get("match_found", False)),
            "odds": list(item.get("odds") or []),
            "source": str(item.get("source") or ""),
            "details": str(item.get("details") or "")[:500],
            "market_closed": bool(item.get("market_closed", False)),
        }
    return sites_payload


def _bookmaker_prefetch_loop() -> None:
    driver = None
    driver_tasks_done = 0
    while not bookmaker_prefetch_stop_event.is_set():
        task = None
        with bookmaker_prefetch_condition:
            while not bookmaker_prefetch_queue and not bookmaker_prefetch_stop_event.is_set():
                bookmaker_prefetch_condition.wait(timeout=0.5)
            if bookmaker_prefetch_stop_event.is_set():
                break
            if bookmaker_prefetch_queue:
                task = bookmaker_prefetch_queue.popleft()
                match_key = str(task.get("match_key") or "")
                payload = bookmaker_prefetch_results.get(match_key)
                if isinstance(payload, dict):
                    payload["status"] = "running"
                    payload["started_at"] = time.time()
        if not task:
            continue

        match_key = str(task.get("match_key") or "")
        radiant_team = str(task.get("radiant_team") or "")
        dire_team = str(task.get("dire_team") or "")
        task_map_num_raw = task.get("map_num")
        try:
            task_map_num = int(task_map_num_raw) if task_map_num_raw is not None else None
        except (TypeError, ValueError):
            task_map_num = None
        if task_map_num is not None and not (1 <= task_map_num <= 5):
            task_map_num = None
        mode = str(task.get("mode") or BOOKMAKER_PREFETCH_MODE)
        try:
            if BOOKMAKER_PREFETCH_USE_SUBPROCESS:
                sites_payload = _bookmaker_prefetch_fetch_subprocess(
                    radiant_team=radiant_team,
                    dire_team=dire_team,
                    mode=mode,
                    map_num=task_map_num,
                )
            else:
                if driver is None:
                    if _bookmaker_build_driver is None:
                        raise RuntimeError("bookmaker driver factory unavailable")
                    driver = _bookmaker_build_driver(BOOKMAKER_PROXY_URL)
                    driver_tasks_done = 0
                urls = _bookmaker_urls_for_mode(mode)
                sites_payload = {}
                for site in BOOKMAKER_PREFETCH_SITES:
                    site_url = urls.get(site)
                    if not site_url:
                        continue
                    if _bookmaker_parse_site is None:
                        raise RuntimeError("bookmaker parse function unavailable")
                    site_result = _bookmaker_parse_site(
                        driver,
                        site=site,
                        url=site_url,
                        team1=radiant_team,
                        team2=dire_team,
                        mode=mode,
                        forced_map_num=task_map_num,
                    )
                    sites_payload[site] = {
                        "status": str(getattr(site_result, "status", "")),
                        "match_found": bool(getattr(site_result, "match_found", False)),
                        "odds": list(getattr(site_result, "odds", []) or []),
                        "source": str(getattr(site_result, "source", "")),
                        "details": str(getattr(site_result, "details", ""))[:500],
                        "market_closed": bool(getattr(site_result, "market_closed", False)),
                    }
            with bookmaker_prefetch_condition:
                payload = bookmaker_prefetch_results.get(match_key)
                if isinstance(payload, dict):
                    payload["status"] = "done"
                    payload["finished_at"] = time.time()
                    payload["sites"] = sites_payload
                bookmaker_prefetch_condition.notify_all()
            print(f"   ✅ Bookmaker prefetch done: {match_key}")
            if not BOOKMAKER_PREFETCH_USE_SUBPROCESS:
                driver_tasks_done += 1
                rotate_after = max(1, int(BOOKMAKER_PREFETCH_DRIVER_ROTATE_TASKS or 1))
                if driver is not None and driver_tasks_done >= rotate_after:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = None
                    driver_tasks_done = 0
                    print("   ♻️ Bookmaker prefetch driver rotated")
        except Exception as e:
            with bookmaker_prefetch_condition:
                payload = bookmaker_prefetch_results.get(match_key)
                if isinstance(payload, dict):
                    payload["status"] = "error"
                    payload["finished_at"] = time.time()
                    payload["error"] = str(e)
                bookmaker_prefetch_condition.notify_all()
            print(f"   ⚠️ Bookmaker prefetch error for {match_key}: {e}")
            try:
                if driver is not None:
                    driver.quit()
            except Exception:
                pass
            driver = None
            driver_tasks_done = 0

    try:
        if driver is not None:
            driver.quit()
    except Exception:
        pass


def _ensure_bookmaker_prefetch_started() -> None:
    global bookmaker_prefetch_thread
    if not BOOKMAKER_PREFETCH_ENABLED:
        return
    thread = bookmaker_prefetch_thread
    if thread is not None:
        if thread.is_alive():
            # If stop was requested but odds got re-enabled before worker exit,
            # resume current worker instead of spawning a duplicate thread.
            if bookmaker_prefetch_stop_event.is_set():
                bookmaker_prefetch_stop_event.clear()
                with bookmaker_prefetch_condition:
                    bookmaker_prefetch_condition.notify_all()
            return
        bookmaker_prefetch_thread = None
    bookmaker_prefetch_stop_event.clear()
    bookmaker_prefetch_thread = threading.Thread(
        target=_bookmaker_prefetch_loop,
        name="bookmaker-prefetch",
        daemon=True,
    )
    bookmaker_prefetch_thread.start()
    print("🧵 Bookmaker prefetch worker started")


def _stop_bookmaker_prefetch_worker() -> None:
    global bookmaker_prefetch_thread
    thread = bookmaker_prefetch_thread
    if thread is None:
        return
    bookmaker_prefetch_stop_event.set()
    with bookmaker_prefetch_condition:
        bookmaker_prefetch_condition.notify_all()
        bookmaker_prefetch_queue.clear()
        bookmaker_prefetch_results.clear()
    if thread.is_alive():
        thread.join(timeout=1.0)
    if thread.is_alive():
        bookmaker_prefetch_thread = thread
        print("🧵 Bookmaker prefetch worker stop requested (still shutting down)")
        return
    bookmaker_prefetch_thread = None
    print("🧵 Bookmaker prefetch worker stopped")


if __name__ == "__main__":
    _setup_run_logging()


with open('/Users/alex/Documents/ingame/base/hero_valid_positions_simple.json', 'r') as f:
    HERO_VALID_POSITIONS_DICT = json.load(f)
try:
    with open('/Users/alex/Documents/ingame/base/hero_valid_positions_counts_500k.json', 'r', encoding='utf-8') as f:
        _raw_counts = json.load(f)
    HERO_POSITION_COUNTS = {
        str(k): v for k, v in _raw_counts.items()
        if str(k).isdigit() and isinstance(v, dict)
    }
except Exception as e:
    HERO_POSITION_COUNTS = {}
    print(f"⚠️ Не удалось загрузить hero_valid_positions_counts_500k.json: {e}")
try:
    with open('/Users/alex/Documents/ingame/base/hero_valid_positions_counts_500k.json', 'r', encoding='utf-8') as f:
        HERO_ID_TO_NAME = json.load(f)
except Exception as e:
    HERO_ID_TO_NAME = {}
    print(f"⚠️ Не удалось загрузить heroes.json: {e}")


trash_list=['team', 'flipster', 'esports', 'gaming', ' ', '.']


from urllib.parse import urlparse  # Добавьте импорт

# Глобальный словарь для отслеживания матчей с отложенной отправкой
monitored_matches = {}
monitored_matches_lock = threading.Lock()
delayed_sender_thread = None
delayed_sender_stop_event = threading.Event()
bookmaker_prefetch_thread = None
bookmaker_prefetch_stop_event = threading.Event()
bookmaker_prefetch_queue = deque()
bookmaker_prefetch_lock = threading.Lock()
bookmaker_prefetch_condition = threading.Condition(bookmaker_prefetch_lock)
bookmaker_prefetch_results = {}
processed_urls_cache = set()
processed_urls_lock = threading.Lock()
signal_send_guard = set()
signal_send_guard_lock = threading.Lock()

# Extreme predictor singleton
extreme_predictor = None

# Глобальный словарь для хранения истории leads матчей
# Формат: {match_url: {'times': [0, 60, 120, ...], 'leads': [0, -500, -1000, ...]}}
match_history = {}

# Kills betting caches
KILLS_RULES = None
KILLS_MODELS = None
KILLS_MODELS_BY_PATCH = {}
KILLS_MODELS_BY_TIER = {}
KILLS_Q10_MODEL = None
KILLS_Q90_MODEL = None
KILLS_PRIORS = None
KILLS_PUB_PRIORS = None
KILLS_FEATURE_COLS = None
KILLS_CAT_COLS = None
KILLS_DRAFT_PREDICTOR = None
TEAM_PREDICTABILITY_CACHE = None
TEAM_PREDICTABILITY_MTIME = None

# Ленивая загрузка словарей
lane_data = None
early_dict = None
late_dict = None

# Настройка прокси
USE_BOOKMAKER_PROXY_FOR_MATCHES = _safe_bool_env("USE_BOOKMAKER_PROXY_FOR_MATCHES", True)
if USE_BOOKMAKER_PROXY_FOR_MATCHES and BOOKMAKER_PROXY_URL:
    PROXY_LIST = [str(BOOKMAKER_PROXY_URL).strip()]
else:
    PROXY_LIST = [str(p).strip() for p in api_to_proxy.keys() if str(p).strip()]
CURRENT_PROXY_INDEX = 0
CURRENT_PROXY = None
PROXIES = {}
USE_PROXY = None


def _env_use_proxy_default() -> bool:
    env_use_proxy = os.getenv("USE_PROXY")
    if env_use_proxy is None:
        return True
    return env_use_proxy.strip().lower() not in {"0", "false", "no", "off"}


def _init_proxy_pool(use_proxy: bool) -> None:
    global CURRENT_PROXY_INDEX, CURRENT_PROXY, PROXIES, USE_PROXY
    USE_PROXY = use_proxy
    if not use_proxy or not PROXY_LIST:
        CURRENT_PROXY_INDEX = 0
        CURRENT_PROXY = None
        PROXIES = {}
        print("🌐 Прокси отключены, используется прямое подключение")
        logger.info("Прокси отключены")
        return
    CURRENT_PROXY_INDEX = random.randint(0, len(PROXY_LIST) - 1)
    CURRENT_PROXY = PROXY_LIST[CURRENT_PROXY_INDEX]
    PROXIES = {
        'http': CURRENT_PROXY,
        'https': CURRENT_PROXY
    }
    print(f"🌐 Используется прокси: {CURRENT_PROXY}")
    logger.info(f"Инициализация прокси: {CURRENT_PROXY} (индекс {CURRENT_PROXY_INDEX})")


# Инициализация выполняется явно в general() или в main.


def rotate_proxy():
    """Переключает на следующий прокси в списке"""
    global CURRENT_PROXY_INDEX, CURRENT_PROXY, PROXIES
    
    if not PROXY_LIST or not USE_PROXY:
        return
    CURRENT_PROXY_INDEX = (CURRENT_PROXY_INDEX + 1) % len(PROXY_LIST)
    CURRENT_PROXY = PROXY_LIST[CURRENT_PROXY_INDEX]
    PROXIES = {
        'http': CURRENT_PROXY,
        'https': CURRENT_PROXY
    }
    
    logger.info(f"🔄 СМЕНА ПРОКСИ: {CURRENT_PROXY} (индекс {CURRENT_PROXY_INDEX}/{len(PROXY_LIST)-1})")
    print(f"🔄 Переключен прокси: {CURRENT_PROXY}")


# Cache for team context data
_team_context_cache = {}
_pro_matches_df = None
_tier_stats_cache = {}  # Cache for tier-based statistics
_tier_autoupdate_lock = threading.Lock()
_auto_added_tier2_ids = set()


def _get_tier_stats(df, tier: int, n_matches: int = 100, use_cache: bool = True) -> dict:
    """Get statistics for a specific tournament tier from last N matches."""
    cache_key = f"tier_{tier}_{n_matches}"
    if use_cache and cache_key in _tier_stats_cache:
        return _tier_stats_cache[cache_key]
    
    tier_df = df[df['tournament_tier'] == tier].tail(n_matches)
    
    if len(tier_df) < 20:
        # Not enough data for this tier, use global
        tier_df = df.tail(n_matches)
        tier_used = 0  # Global
    else:
        tier_used = tier
    
    stats = {
        'avg_kills': float(tier_df['total_kills'].mean()),
        'std_kills': float(tier_df['total_kills'].std()),
        'matches_count': len(tier_df),
        'tier_used': tier_used,
    }
    
    if use_cache:
        _tier_stats_cache[cache_key] = stats
    return stats


def _get_team_tier(team_id: int) -> int:
    """
    Get team tier from id_to_names definitions.
    Returns 1 (Tier 1), 2 (Tier 2), or 3 (Unknown/Rest).
    """
    try:
        team_id = int(team_id)
    except Exception:
        return 3

    if team_id in _auto_added_tier2_ids:
        return 2

    from id_to_names import tier_one_teams, tier_two_teams
    
    # Check tier 1
    for name, ids in tier_one_teams.items():
        if isinstance(ids, set):
            if team_id in ids:
                return 1
        elif ids == team_id:
            return 1
    
    # Check tier 2
    for name, ids in tier_two_teams.items():
        if isinstance(ids, set):
            if team_id in ids:
                return 2
        elif ids == team_id:
            return 2
    
    return 3  # Unknown/Rest


def _normalize_tier_team_name_only(team_name: str) -> str:
    raw = (team_name or "").strip()
    if not raw:
        return ""
    return normalize_team_name(raw)


def _extract_team_ids(value: Any) -> set[int]:
    out: set[int] = set()
    if isinstance(value, set):
        source = value
    else:
        source = (value,)
    for raw in source:
        try:
            tid = int(raw)
        except Exception:
            continue
        if tid > 0:
            out.add(tid)
    return out


def _collect_candidate_team_ids(raw: Any, out: List[int], seen: set[int]) -> None:
    if raw is None:
        return
    if isinstance(raw, bool):
        return
    if isinstance(raw, int):
        if raw > 0 and raw not in seen:
            seen.add(raw)
            out.append(raw)
        return
    if isinstance(raw, float):
        if math.isfinite(raw):
            as_int = int(raw)
            if as_int > 0 and float(as_int) == float(raw) and as_int not in seen:
                seen.add(as_int)
                out.append(as_int)
        return
    if isinstance(raw, (list, tuple, set)):
        for item in raw:
            _collect_candidate_team_ids(item, out, seen)
        return
    if isinstance(raw, dict):
        for key in ("team_id", "team_ids", "id", "ids"):
            if key in raw:
                _collect_candidate_team_ids(raw.get(key), out, seen)
        return
    if isinstance(raw, str):
        value = raw.strip()
        if not value:
            return
        try:
            as_int = int(value)
        except Exception:
            as_int = None
        if as_int is not None:
            if as_int > 0 and as_int not in seen:
                seen.add(as_int)
                out.append(as_int)
            return

        # Частый кейс: строка с сериализованным массивом, иногда в дополнительных кавычках.
        if (len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}):
            _collect_candidate_team_ids(value[1:-1], out, seen)

        if value and value[0] in "[{(":
            for parser in (json.loads, ast.literal_eval):
                try:
                    parsed = parser(value)
                    _collect_candidate_team_ids(parsed, out, seen)
                    return
                except Exception:
                    continue

        if "," in value:
            for part in value.split(","):
                _collect_candidate_team_ids(part, out, seen)
        return


def _extract_candidate_team_ids(*values: Any) -> List[int]:
    out: List[int] = []
    seen: set[int] = set()
    for raw in values:
        _collect_candidate_team_ids(raw, out, seen)
    return out


def _find_known_team_ids_by_name(team_name: str) -> set[int]:
    """
    Ищет все известные team_id в tier1/tier2 по нормализованному названию команды.
    """
    name_key = _normalize_tier_team_name_only(team_name)
    if not name_key:
        return set()
    ids: set[int] = set()
    from id_to_names import tier_one_teams, tier_two_teams
    for alias, value in tier_one_teams.items():
        if _normalize_tier_team_name_only(str(alias)) == name_key:
            ids.update(_extract_team_ids(value))
    for alias, value in tier_two_teams.items():
        if _normalize_tier_team_name_only(str(alias)) == name_key:
            ids.update(_extract_team_ids(value))
    return ids


def _normalize_tier_team_key(team_name: str, team_id: int) -> str:
    key = _normalize_tier_team_name_only(team_name)
    if not key:
        key = f"autoteam_{int(team_id)}"
    return key


def _append_team_to_tier2_file(team_name: str, team_id: int) -> tuple[bool, str]:
    """
    Добавляет неизвестную команду в Tier 2 словарь id_to_names.py.
    Возвращает (added, key_or_reason).
    """
    try:
        team_id = int(team_id)
    except Exception:
        return False, "invalid_team_id"

    try:
        from id_to_names import tier_two_teams
    except Exception as e:
        return False, f"import_error:{e}"

    if _get_team_tier(team_id) in (1, 2):
        return False, "already_known"

    try:
        with _tier_autoupdate_lock:
            # Повторная проверка под локом (на случай гонок).
            if _get_team_tier(team_id) in (1, 2):
                return False, "already_known"

            key = _normalize_tier_team_key(team_name, team_id)
            existing = tier_two_teams.get(key)
            if existing is None:
                tier_two_teams[key] = team_id
            elif isinstance(existing, set):
                if team_id in existing:
                    return False, "already_known"
                existing.add(team_id)
            else:
                try:
                    existing_id = int(existing)
                except Exception:
                    existing_id = None
                if existing_id == team_id:
                    return False, "already_known"
                if existing_id is None:
                    # Неожиданный формат старого значения: безопасный fallback.
                    fallback_key = f"{key}_{team_id}"
                    suffix = 1
                    while fallback_key in tier_two_teams and tier_two_teams.get(fallback_key) != team_id:
                        fallback_key = f"{key}_{team_id}_{suffix}"
                        suffix += 1
                    key = fallback_key
                    tier_two_teams[key] = team_id
                else:
                    tier_two_teams[key] = {existing_id, team_id}
            _auto_added_tier2_ids.add(team_id)

            id_to_names_path = Path('/Users/alex/Documents/ingame/base/id_to_names.py')
            append_block = (
                "\n# auto-added by cyberscore_try (dynamic tier2 onboarding)\n"
                "try:\n"
                f"    _key = {key!r}\n"
                f"    _team_id = {team_id}\n"
                "    _existing = tier_two_teams.get(_key)\n"
                "    if isinstance(_existing, set):\n"
                "        _existing.add(_team_id)\n"
                "    elif _existing is None:\n"
                "        tier_two_teams[_key] = _team_id\n"
                "    elif _existing != _team_id:\n"
                "        try:\n"
                "            tier_two_teams[_key] = {int(_existing), _team_id}\n"
                "        except Exception:\n"
                "            tier_two_teams[_key] = _team_id\n"
                "except Exception:\n"
                "    pass\n"
            )
            with id_to_names_path.open('a', encoding='utf-8') as f:
                f.write(append_block)
            return True, key
    except Exception as e:
        return False, f"write_error:{e}"


def _ensure_known_team_or_add_to_tier2(team_ids, team_name: str, match_url: str) -> tuple[bool, int]:
    """
    Гарантирует, что команда находится в Tier1/2 и возвращает (ok, resolved_team_id).
    Если неизвестна — автоматически добавляет в Tier2 и уведомляет в Telegram.
    Если имя уже известно, но пришёл другой team_id, используем уже известный id по имени.
    """
    candidate_ids = _extract_candidate_team_ids(team_ids)
    if not candidate_ids:
        return False, 0

    known_ids_by_name = _find_known_team_ids_by_name(team_name)
    if known_ids_by_name:
        for candidate_id in candidate_ids:
            if candidate_id in known_ids_by_name:
                return True, candidate_id
        resolved_id = min(known_ids_by_name)
        print(
            "   ⚠️ team_id не совпал с известным id по имени: "
            f"{team_name} ({candidate_ids}) -> используем {resolved_id}"
        )
        logger.warning(
            "TEAM_ID_NAME_MISMATCH name=%s incoming_ids=%s resolved_id=%s",
            team_name,
            candidate_ids,
            resolved_id,
        )
        return True, resolved_id

    # Проверяем каждый candidate id: если хоть один уже известен (T1/T2), берем его.
    for candidate_id in candidate_ids:
        if _get_team_tier(candidate_id) in (1, 2):
            return True, candidate_id

    # Иначе пытаемся авто-добавление последовательно по каждому candidate id.
    attempts: List[str] = []
    for candidate_id in candidate_ids:
        added, reason = _append_team_to_tier2_file(team_name, candidate_id)
        if added:
            msg = (
                "🆕 Команда автоматически добавлена в Tier 2.\n"
                f"Team: {team_name} ({candidate_id})\n"
                f"Candidate IDs: {candidate_ids}\n"
                f"Key: {reason}\n"
                f"{match_url}"
            )
            print(f"   {msg}")
            send_message(msg)
            return True, candidate_id
        if reason == "already_known":
            return True, candidate_id
        attempts.append(f"{candidate_id}:{reason}")

    err_msg = (
        "⚠️ Не удалось автоматически добавить неизвестную команду в Tier 2.\n"
        f"Team: {team_name}\n"
        f"Candidate IDs: {candidate_ids}\n"
        f"Reasons: {attempts}\n"
        f"{match_url}"
    )
    print(f"   {err_msg}")
    send_message(err_msg)
    return False, (candidate_ids[0] if candidate_ids else 0)


def _determine_star_signal_match_tier(radiant_team_id: int, dire_team_id: int) -> Optional[int]:
    """
    Режим tier для star-сигналов:
    - если хотя бы одна команда из Tier 2 -> матч Tier 2
    - если обе команды из Tier 1 -> матч Tier 1
    - если есть команда вне Tier1/Tier2 -> None (пропуск матча)
    """
    r_tier = _get_team_tier(radiant_team_id)
    d_tier = _get_team_tier(dire_team_id)

    if r_tier not in (1, 2) or d_tier not in (1, 2):
        return None
    if r_tier == 2 or d_tier == 2:
        return 2
    return 1


def _determine_match_tier(radiant_team_id: int, dire_team_id: int) -> int:
    """
    Determine tournament tier for a match based on team tiers.
    If both teams are Tier 1 → Tier 1 match
    If both teams are Tier 2 → Tier 2 match
    If mixed → use lower tier (more conservative)
    
    Returns 1 (Tier 1) or 2 (Tier 2).
    """
    r_tier = _get_team_tier(radiant_team_id)
    d_tier = _get_team_tier(dire_team_id)
    
    # Both Tier 1 → Tier 1 match
    if r_tier == 1 and d_tier == 1:
        return 1
    
    # Both Tier 2 → Tier 2 match
    if r_tier == 2 and d_tier == 2:
        return 2
    
    # Mixed: Tier 1 vs Tier 2 → use Tier 1 (higher level play)
    if r_tier <= 2 and d_tier <= 2:
        return min(r_tier, d_tier)
    
    # One or both unknown → use known tier if possible, else mark unknown
    if r_tier == 3 or d_tier == 3:
        if r_tier <= 2:
            return r_tier
        if d_tier <= 2:
            return d_tier
        return 3  # Both unknown

    return 3


_PATCH_SCHEDULE = [
    ("2025-02-19", "7.38"),
    ("2025-03-05", "7.38b"),
    ("2025-03-19", "7.38b"),
    ("2025-03-27", "7.38c"),
    ("2025-05-21", "7.39"),
    ("2025-05-29", "7.39b"),
    ("2025-06-24", "7.39c"),
    ("2025-08-05", "7.39d"),
    ("2025-08-08", "7.39d"),
    ("2025-08-22", "7.39d"),
    ("2025-10-02", "7.39e"),
    ("2025-10-09", "7.39e"),
    ("2025-11-10", "7.39e"),
    ("2025-12-12", "7.39e"),
    ("2025-12-15", "7.40"),
    ("2025-12-23", "7.40b"),
]


def _build_patch_schedule():
    from datetime import datetime, timezone

    schedule = []
    for idx, (date_str, label) in enumerate(_PATCH_SCHEDULE):
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            ts = int(dt.timestamp())
        except Exception:
            ts = 0
        if ts <= 0:
            continue
        schedule.append({"patch_id": idx, "label": label, "ts": ts})
    schedule.sort(key=lambda s: s["ts"])
    return schedule


_PATCH_SCHEDULE_INFO = _build_patch_schedule()


def _get_patch_label(ts: int) -> str:
    if ts <= 0 or not _PATCH_SCHEDULE_INFO:
        return "UNKNOWN"
    idx = -1
    for i, patch in enumerate(_PATCH_SCHEDULE_INFO):
        if ts >= patch["ts"]:
            idx = i
        else:
            break
    if idx < 0:
        idx = 0
    return str(_PATCH_SCHEDULE_INFO[idx]["label"])


def _get_patch_id(ts: int) -> int:
    if ts <= 0 or not _PATCH_SCHEDULE_INFO:
        return -1
    idx = -1
    for i, patch in enumerate(_PATCH_SCHEDULE_INFO):
        if ts >= patch["ts"]:
            idx = i
        else:
            break
    if idx < 0:
        idx = 0
    return int(_PATCH_SCHEDULE_INFO[idx]["patch_id"])


def _get_patch_major_label(ts: int) -> str:
    label = _get_patch_label(ts)
    if not label or label == "UNKNOWN":
        return "UNKNOWN"
    base = label
    while base and base[-1].isalpha():
        base = base[:-1]
    return base or label


def _patch_label_to_slug(label: str) -> str:
    return (label or "UNKNOWN").replace(".", "_")


def get_team_context(
    radiant_team_id: int, 
    dire_team_id: int,
    radiant_player_ids: Optional[List[int]] = None,
    dire_player_ids: Optional[List[int]] = None,
    league_id: Optional[int] = None,
    current_tier: Optional[int] = None,
    match_start_time: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    Get context features for extreme kills prediction from pro_matches_enriched.csv.
    Returns None if data is insufficient - NO DEFAULTS ALLOWED.
    
    Args:
        radiant_team_id: Radiant team ID
        dire_team_id: Dire team ID
        league_id: Current league ID (optional, for league-specific stats)
        current_tier: Current tournament tier (1 or 2, optional)
    
    Required data:
    - At least 5 matches per team in history
    - H2H data (can be 0 matches, but must be calculated)
    """
    global _pro_matches_df
    import pandas as pd
    
    MIN_MATCHES_PER_TEAM = 5  # Minimum matches required per team
    TIER_STATS_MATCHES = 100  # Last N matches for tier statistics
    
    # Load CSV once
    if _pro_matches_df is None:
        try:
            _pro_matches_df = pd.read_csv('/Users/alex/Documents/ingame/data/pro_matches_enriched.csv')
            if 'start_time' in _pro_matches_df.columns:
                _pro_matches_df = _pro_matches_df.sort_values('start_time')
            else:
                _pro_matches_df = _pro_matches_df.sort_values('match_id')
            if 'patch_major_label' not in _pro_matches_df.columns and 'start_time' in _pro_matches_df.columns:
                _pro_matches_df['patch_major_label'] = _pro_matches_df['start_time'].apply(
                    lambda ts: _get_patch_major_label(int(ts))
                )
            logger.info(f"Loaded pro_matches_enriched.csv: {len(_pro_matches_df)} matches")
        except Exception as e:
            logger.error(f"Failed to load pro_matches_enriched.csv: {e}")
            return None
    
    df = _pro_matches_df
    df_history = df
    if match_start_time is not None and match_start_time > 0 and 'start_time' in df.columns:
        df_history = df[df['start_time'] < match_start_time]
    
    # Validate team IDs
    if radiant_team_id is None or dire_team_id is None:
        logger.warning(f"Missing team IDs: radiant={radiant_team_id}, dire={dire_team_id}")
        return None
    
    try:
        # Determine tier if not provided (based on team tiers from id_to_names)
        if current_tier is None:
            current_tier = _determine_match_tier(radiant_team_id, dire_team_id)

        def _roster_sig(ids: Optional[List[int]]) -> str:
            if not ids:
                return "none"
            cleaned = sorted({int(pid) for pid in ids if int(pid) > 0})
            if not cleaned:
                return "none"
            return "-".join(str(pid) for pid in cleaned)

        cache_key = (
            f"{radiant_team_id}_{dire_team_id}_{current_tier}_{league_id}_{match_start_time or 0}_"
            f"{_roster_sig(radiant_player_ids)}_{_roster_sig(dire_player_ids)}"
        )
        if cache_key in _team_context_cache:
            return _team_context_cache[cache_key]

        # Team form (last 15 matches each) - REQUIRED
        r_matches_all = df_history[
            (df_history['radiant_team_id'] == radiant_team_id)
            | (df_history['dire_team_id'] == radiant_team_id)
        ]
        d_matches_all = df_history[
            (df_history['radiant_team_id'] == dire_team_id)
            | (df_history['dire_team_id'] == dire_team_id)
        ]
        r_matches = r_matches_all.tail(15)
        d_matches = d_matches_all.tail(15)
        
        r_match_count = len(r_matches)
        d_match_count = len(d_matches)
        
        # STRICT: Need at least MIN_MATCHES_PER_TEAM matches per team
        if r_match_count < MIN_MATCHES_PER_TEAM:
            logger.warning(f"Insufficient history for radiant team {radiant_team_id}: {r_match_count} < {MIN_MATCHES_PER_TEAM}")
            return None
        
        if d_match_count < MIN_MATCHES_PER_TEAM:
            logger.warning(f"Insufficient history for dire team {dire_team_id}: {d_match_count} < {MIN_MATCHES_PER_TEAM}")
            return None

        def _extract_roster_from_row(row: pd.Series, side: str) -> set[int]:
            roster = set()
            for pos in range(1, 6):
                pid = int(row.get(f"{side}_player_{pos}_id", 0) or 0)
                if pid > 0:
                    roster.add(pid)
            return roster

        def _compute_roster_stats(
            team_id: int,
            current_roster: Optional[List[int]],
            matches_all: pd.DataFrame,
            min_shared: int = 3,
        ) -> Dict[str, Any]:
            roster = {int(pid) for pid in (current_roster or []) if int(pid) > 0}
            roster_count = len(roster)
            shared = None
            changed = None
            stable = 0
            new_team = 0
            group_matches = 1

            if matches_all.empty or roster_count < min_shared:
                return {
                    "roster_shared_prev": shared,
                    "roster_changed_prev": changed,
                    "roster_stable_prev": stable,
                    "roster_new_team": 1,
                    "roster_group_matches": group_matches,
                    "roster_player_count": roster_count,
                }

            last_match = matches_all.tail(1).iloc[0]
            last_side = (
                "radiant"
                if int(last_match.get("radiant_team_id", 0) or 0) == team_id
                else "dire"
            )
            prev_roster = _extract_roster_from_row(last_match, last_side)
            prev_count = len(prev_roster)
            if prev_count < min_shared:
                return {
                    "roster_shared_prev": shared,
                    "roster_changed_prev": changed,
                    "roster_stable_prev": stable,
                    "roster_new_team": 1,
                    "roster_group_matches": group_matches,
                    "roster_player_count": roster_count,
                }

            shared = len(roster & prev_roster)
            if roster_count == 5 and prev_count == 5:
                changed = 5 - shared
            else:
                changed = roster_count - shared

            if shared >= min_shared:
                stable = 1
                new_team = 0
                prev_group_matches = last_match.get(f"{last_side}_roster_group_matches")
                try:
                    prev_group_matches = int(prev_group_matches)
                except Exception:
                    prev_group_matches = 0
                group_matches = prev_group_matches + 1 if prev_group_matches > 0 else 1
            else:
                stable = 0
                new_team = 1
                group_matches = 1

            return {
                "roster_shared_prev": shared,
                "roster_changed_prev": changed,
                "roster_stable_prev": stable,
                "roster_new_team": new_team,
                "roster_group_matches": group_matches,
                "roster_player_count": roster_count,
            }
        
        # H2H history - can be 0, but calculate actual value
        h2h_mask = (
            ((df_history['radiant_team_id'] == radiant_team_id) & (df_history['dire_team_id'] == dire_team_id))
            | ((df_history['radiant_team_id'] == dire_team_id) & (df_history['dire_team_id'] == radiant_team_id))
        )
        h2h_df = df_history[h2h_mask]
        h2h_matches_count = len(h2h_df)
        
        # H2H avg - use actual if exists, otherwise use combined team avg (calculated below)
        h2h_avg_total = h2h_df['total_kills'].mean() if h2h_matches_count > 0 else None
        
        # ===== TIER-BASED LEAGUE STATS =====
        # Get tier-specific statistics (last 100 matches of this tier)
        tier_stats = _get_tier_stats(
            df_history, current_tier, TIER_STATS_MATCHES, use_cache=match_start_time in (None, 0)
        )
        league_avg_kills = tier_stats['avg_kills']
        league_kills_std = tier_stats['std_kills']
        
        # League-specific stats (if league_id provided and has enough matches)
        league_meta_diff = 0.0
        if league_id is not None:
            league_df = df_history[df_history['league_id'] == league_id]
            if len(league_df) >= 10:
                league_specific_avg = float(league_df['total_kills'].mean())
                # Difference from tier average
                league_meta_diff = league_specific_avg - league_avg_kills
        
        # Calculate team averages using team-specific kills (no leakage)
        def _team_kills_stats(matches, team_id):
            if matches.empty:
                return np.array([]), np.array([]), np.array([])
            radiant_mask = matches['radiant_team_id'].values == team_id
            kills = np.where(radiant_mask, matches['radiant_score'].values, matches['dire_score'].values)
            deaths = np.where(radiant_mask, matches['dire_score'].values, matches['radiant_score'].values)
            duration = matches['duration_min'].replace(0, np.nan).values.astype(float)
            kpm = np.divide(kills, duration, out=np.zeros_like(kills, dtype=float), where=duration > 0)
            return kills, deaths, kpm

        r_kills, _, r_kpm = _team_kills_stats(r_matches, radiant_team_id)
        d_kills, _, d_kpm = _team_kills_stats(d_matches, dire_team_id)

        r_form_kills, _, _ = _team_kills_stats(r_matches_all.tail(5), radiant_team_id)
        d_form_kills, _, _ = _team_kills_stats(d_matches_all.tail(5), dire_team_id)

        r_avg_kills = float(np.mean(r_kills)) if len(r_kills) else 0.0
        d_avg_kills = float(np.mean(d_kills)) if len(d_kills) else 0.0
        r_form_avg = float(np.mean(r_form_kills)) if len(r_form_kills) else 0.0
        d_form_avg = float(np.mean(d_form_kills)) if len(d_form_kills) else 0.0

        combined_form_kills = r_form_avg + d_form_avg
        combined_team_avg_kills = r_avg_kills + d_avg_kills

        # If no H2H, use combined team avg as proxy
        if h2h_avg_total is None:
            h2h_avg_total = combined_team_avg_kills

        # Team aggression (kills per minute) - calculated from actual data
        r_aggression = float(np.mean(r_kpm)) if len(r_kpm) else 0.0
        d_aggression = float(np.mean(d_kpm)) if len(d_kpm) else 0.0
        combined_team_aggression = r_aggression + d_aggression

        # Patch-aware team stats
        patch_key = _get_patch_major_label(match_start_time or 0)
        r_patch_all = r_matches_all
        d_patch_all = d_matches_all
        if patch_key != "UNKNOWN" and 'patch_major_label' in df_history.columns:
            r_patch_all = r_matches_all[r_matches_all['patch_major_label'] == patch_key]
            d_patch_all = d_matches_all[d_matches_all['patch_major_label'] == patch_key]

        r_patch = r_patch_all.tail(15)
        d_patch = d_patch_all.tail(15)
        r_patch_form = r_patch_all.tail(5)
        d_patch_form = d_patch_all.tail(5)

        r_patch_kills, _, r_patch_kpm = _team_kills_stats(r_patch, radiant_team_id)
        d_patch_kills, _, d_patch_kpm = _team_kills_stats(d_patch, dire_team_id)
        r_patch_form_kills, _, _ = _team_kills_stats(r_patch_form, radiant_team_id)
        d_patch_form_kills, _, _ = _team_kills_stats(d_patch_form, dire_team_id)

        r_patch_avg = float(np.mean(r_patch_kills)) if len(r_patch_kills) else r_avg_kills
        d_patch_avg = float(np.mean(d_patch_kills)) if len(d_patch_kills) else d_avg_kills
        r_patch_form_avg = float(np.mean(r_patch_form_kills)) if len(r_patch_form_kills) else r_form_avg
        d_patch_form_avg = float(np.mean(d_patch_form_kills)) if len(d_patch_form_kills) else d_form_avg
        r_patch_aggr = float(np.mean(r_patch_kpm)) if len(r_patch_kpm) else r_aggression
        d_patch_aggr = float(np.mean(d_patch_kpm)) if len(d_patch_kpm) else d_aggression

        combined_patch_form_kills = r_patch_form_avg + d_patch_form_avg
        combined_patch_team_avg_kills = r_patch_avg + d_patch_avg
        combined_patch_team_aggression = r_patch_aggr + d_patch_aggr

        # Synthetic kills
        combined_synthetic_kills = combined_team_avg_kills / 5.0

        roster_r = _compute_roster_stats(radiant_team_id, radiant_player_ids, r_matches_all)
        roster_d = _compute_roster_stats(dire_team_id, dire_player_ids, d_matches_all)
        
        context = {
            'h2h_avg_total': float(h2h_avg_total),
            'h2h_matches_count': int(h2h_matches_count),
            'league_avg_kills': league_avg_kills,
            'league_kills_std': league_kills_std,
            'league_meta_diff': league_meta_diff,
            'combined_form_kills': combined_form_kills,
            'combined_team_avg_kills': combined_team_avg_kills,
            'combined_team_aggression': combined_team_aggression,
            'combined_synthetic_kills': combined_synthetic_kills,
            'combined_patch_form_kills': combined_patch_form_kills,
            'combined_patch_team_avg_kills': combined_patch_team_avg_kills,
            'combined_patch_team_aggression': combined_patch_team_aggression,
            'radiant_roster_shared_prev': roster_r['roster_shared_prev'],
            'dire_roster_shared_prev': roster_d['roster_shared_prev'],
            'radiant_roster_changed_prev': roster_r['roster_changed_prev'],
            'dire_roster_changed_prev': roster_d['roster_changed_prev'],
            'radiant_roster_stable_prev': roster_r['roster_stable_prev'],
            'dire_roster_stable_prev': roster_d['roster_stable_prev'],
            'radiant_roster_new_team': roster_r['roster_new_team'],
            'dire_roster_new_team': roster_d['roster_new_team'],
            'radiant_roster_group_matches': roster_r['roster_group_matches'],
            'dire_roster_group_matches': roster_d['roster_group_matches'],
            'radiant_roster_player_count': roster_r['roster_player_count'],
            'dire_roster_player_count': roster_d['roster_player_count'],
            # Additional metadata for debugging
            'radiant_matches': r_match_count,
            'dire_matches': d_match_count,
            'radiant_avg_kills': r_avg_kills,
            'dire_avg_kills': d_avg_kills,
            'patch_key': patch_key,
            'match_tier': current_tier,
            'tier_stats_matches': tier_stats['matches_count'],
            'tier_used': tier_stats['tier_used'],
        }
        
        _team_context_cache[cache_key] = context
        logger.info(f"Context for {radiant_team_id} vs {dire_team_id}: "
                   f"tier={current_tier}, tier_avg={league_avg_kills:.1f}, "
                   f"h2h={h2h_matches_count}, team_avg={combined_team_avg_kills:.1f}")
        return context
        
    except Exception as e:
        logger.error(f"Error getting team context: {e}")
        return None


def safe_float(value, default=0.0):
    """Безопасно конвертирует значение в float, обрабатывая None, 'None', числа и строки"""
    if value is None or value == 'None':
        return default
    try:
        # Убираем звездочки и пробелы
        clean_value = str(value).replace('*', '').strip()
        if not clean_value or clean_value == 'None':
            return default
        return float(clean_value)
    except (ValueError, TypeError):
        return default


def make_request_with_retry(url, max_retries=5, retry_delay=5, headers=None):
    """Универсальная функция для HTTP запросов с retry логикой"""
    if headers is None:
        # Используем глобальные headers если не переданы
        headers = globals().get('headers', {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    response = None
    last_exception = None
    proxy_rotation_threshold = 2  # Меняем прокси после 2-х неудачных попыток
    
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers=headers,
                verify=False,
                timeout=10,
                proxies=PROXIES
            )
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                # Обработка Too Many Requests - сразу меняем прокси
                logger.warning(f'Ошибка 429: Too Many Requests с прокси {CURRENT_PROXY}')
                print(f'⚠️  429: Too Many Requests - меняем прокси')
                rotate_proxy()
                
                retry_429_delay = 5
                for retry_429 in range(3):
                    print(f'Повторная попытка {retry_429 + 1}/3 с новым прокси. Ожидание {retry_429_delay} сек...')
                    time.sleep(retry_429_delay)
                    response = requests.get(url, headers=headers, verify=False, timeout=10, proxies=PROXIES)
                    if response.status_code == 200:
                        return response
                    elif response.status_code == 429:
                        # Снова 429 - меняем прокси
                        rotate_proxy()
                    retry_429_delay *= 2
                if response.status_code == 200:
                    return response
            else:
                print(f'⚠️  Попытка {attempt + 1}/{max_retries}: статус {response.status_code}')
                logger.warning(f'Попытка {attempt + 1}/{max_retries}: статус {response.status_code} для {url}')
                
                # Меняем прокси после threshold неудачных попыток
                if (attempt + 1) % proxy_rotation_threshold == 0 and attempt < max_retries - 1:
                    rotate_proxy()
                    
        except requests.exceptions.RequestException as e:
            last_exception = e
            print(f'⚠️  Попытка {attempt + 1}/{max_retries}: ошибка {type(e).__name__}: {e}')
            logger.warning(f'Попытка {attempt + 1}/{max_retries}: {type(e).__name__} для {url}')
            
            # Меняем прокси после threshold неудачных попыток
            if (attempt + 1) % proxy_rotation_threshold == 0 and attempt < max_retries - 1:
                rotate_proxy()
            
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    
    # Если все попытки провалились
    if last_exception:
        error_msg = f'❌ Все попытки провалились. Последняя ошибка: {last_exception}'
        print(error_msg)
        logger.error(error_msg)
    elif response:
        error_msg = f'❌ Все попытки провалились. Последний статус: {response.status_code}'
        print(error_msg)
        logger.error(error_msg)
    else:
        error_msg = f'❌ Все попытки провалились. Response = None'
        print(error_msg)
        logger.error(error_msg)
    
    return response


def get_match_live_data(json_url):
    """Получает актуальные данные матча (lead, game_time)"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = requests.get(json_url, timeout=5, proxies=PROXIES)
            if resp.status_code == 200:
                data = resp.json()
                lead = data.get('radiant_lead', 0)
                game_time = data.get('game_time', 0)
                return lead, game_time
            elif resp.status_code == 429:
                logger.warning(f"429 при получении live данных, меняем прокси")
                rotate_proxy()
        except Exception as e:
            logger.warning(f"Ошибка получения live данных (попытка {attempt + 1}/{max_retries}): {e}")
            print(f"⚠️  Ошибка получения live данных: {e}")
            if attempt < max_retries - 1:
                rotate_proxy()
                time.sleep(2)
    
    logger.error("Не удалось получить live данные после всех попыток")
    return None, None


def extract_live_features(data: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """
    Extract live in-game features from cyberscore data dict.
    
    These features are available during the game (before 10 min mark when bookmaker closes).
    
    Args:
        data: The full data dict from cyberscore JSON endpoint
        
    Returns:
        Dict with live features or None if data is insufficient
        
    Features extracted:
        - game_time: current game time in seconds
        - game_time_min: current game time in minutes
        - radiant_score: radiant kills
        - dire_score: dire kills
        - total_kills_live: total kills so far
        - kill_diff_live: radiant kills - dire kills
        - radiant_lead: net worth difference (positive = radiant ahead)
        - kills_per_min_live: kills per minute so far
        - nw_per_kill_live: net worth per kill (economy efficiency)
        - is_bloodbath_start: 1 if kills/min > 1.5 in first 10 min
        - is_slow_start: 1 if kills/min < 0.5 in first 10 min
    """
    if data is None:
        return None
    
    try:
        game_time = data.get('game_time', 0)
        radiant_score = data.get('radiant_score', 0)
        dire_score = data.get('dire_score', 0)
        radiant_lead = data.get('radiant_lead', 0)
        
        # Skip if game hasn't started or no meaningful data
        if game_time <= 0:
            return None
        
        game_time_min = game_time / 60.0
        if game_time_min > 10:
            return None
        total_kills = radiant_score + dire_score
        kill_diff = radiant_score - dire_score
        
        # Kills per minute (avoid division by zero)
        kills_per_min = total_kills / max(game_time_min, 0.5)
        
        # Net worth per kill (economy efficiency)
        # High NW per kill = efficient farming, fewer fights
        # Low NW per kill = constant fighting
        nw_per_kill = abs(radiant_lead) / max(total_kills, 1)
        
        # Bloodbath detection (early game indicator)
        # If KPM > 1.5 in first 10 min, likely high-kill game
        is_bloodbath_start = 1.0 if (game_time_min <= 10 and kills_per_min > 1.5) else 0.0
        
        # Slow start detection
        # If KPM < 0.5 in first 10 min, likely low-kill game
        is_slow_start = 1.0 if (game_time_min <= 10 and kills_per_min < 0.5 and game_time_min >= 3) else 0.0
        
        # Lead per minute (how fast advantage is building)
        lead_per_min = abs(radiant_lead) / max(game_time_min, 0.5)
        
        # Stomp indicator (one team dominating early)
        is_stomp_early = 1.0 if (game_time_min <= 10 and abs(kill_diff) >= 5) else 0.0
        
        # XP lead (if available in data, otherwise estimate from NW lead)
        # XP lead is typically correlated with NW lead but not always available
        xp_lead = data.get('xp_lead', 0)
        if xp_lead == 0 and radiant_lead != 0:
            # Rough estimate: XP lead is ~60-70% of NW lead in early game
            xp_lead = radiant_lead * 0.65
        
        features = {
            # Keep game_time_min_live for predict_extreme to detect in-game mode
            'game_time_min_live': float(game_time_min),
            # In-game features (matching training script names)
            'ingame_minute': float(int(game_time_min)),
            'ingame_radiant_kills': float(radiant_score),
            'ingame_dire_kills': float(dire_score),
            'ingame_total_kills': float(total_kills),
            'ingame_kill_diff': float(kill_diff),
            'ingame_kpm': float(kills_per_min),
            'ingame_nw_lead': float(radiant_lead),
            'ingame_xp_lead': float(xp_lead),
            'ingame_nw_per_kill': float(nw_per_kill),
            'ingame_lead_per_min': float(lead_per_min),
            'ingame_is_bloodbath': float(is_bloodbath_start),
            'ingame_is_slow': float(is_slow_start),
            'ingame_is_stomp': float(is_stomp_early),
        }
        
        # Extract from charts if available (historical data within game)
        charts = data.get('charts', {})
        if charts:
            radiant_kills_hist = charts.get('radiant_kills', [])
            dire_kills_hist = charts.get('dire_kills', [])
            nw_hist = charts.get('net_worth', [])
            
            # Kill acceleration (are kills increasing?)
            if len(radiant_kills_hist) >= 3 and len(dire_kills_hist) >= 3:
                recent_r = sum(radiant_kills_hist[-2:]) if len(radiant_kills_hist) >= 2 else 0
                recent_d = sum(dire_kills_hist[-2:]) if len(dire_kills_hist) >= 2 else 0
                older_r = sum(radiant_kills_hist[:-2]) if len(radiant_kills_hist) > 2 else 0
                older_d = sum(dire_kills_hist[:-2]) if len(dire_kills_hist) > 2 else 0
                
                # Positive = kills accelerating
                features['ingame_kill_accel'] = float((recent_r + recent_d) - (older_r + older_d))
            else:
                features['ingame_kill_accel'] = 0.0
            
            # NW volatility (swings in net worth = more fighting)
            if len(nw_hist) >= 3:
                nw_changes = [abs(nw_hist[i] - nw_hist[i-1]) for i in range(1, len(nw_hist))]
                features['nw_volatility'] = float(sum(nw_changes) / len(nw_changes)) if nw_changes else 0.0
            else:
                features['nw_volatility'] = 0.0
        else:
            features['kill_acceleration'] = 0.0
            features['nw_volatility'] = 0.0
        
        return features
        
    except Exception as e:
        logger.warning(f"Error extracting live features: {e}")
        return None


def _coerce_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(float(str(value).strip()))
    except Exception:
        return 0


def _coerce_timestamp(value: Any) -> int:
    ts = _coerce_int(value)
    if ts > 10_000_000_000:
        ts = int(ts / 1000)
    return ts


def _coerce_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(str(value).strip())
    except Exception:
        return 0.0


def _linear_slope(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    x = np.arange(len(values), dtype=np.float64)
    y = np.array(values, dtype=np.float64)
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    denom = float(((x - x_mean) ** 2).sum())
    if denom <= 1e-9:
        return 0.0
    return float(((x - x_mean) * (y - y_mean)).sum() / denom)


def _load_kills_rules() -> Dict[str, Any]:
    global KILLS_RULES
    if KILLS_RULES is not None:
        return KILLS_RULES
    rules_path = Path("/Users/alex/Documents/ingame/ml-models/kills_betting_rules.json")
    if rules_path.exists():
        try:
            with rules_path.open("r", encoding="utf-8") as f:
                KILLS_RULES = json.load(f)
                return KILLS_RULES
        except Exception as e:
            logger.warning(f"Failed to load kills rules: {e}")
    KILLS_RULES = {
        "odds": 1.8,
        "low_rule": {"type": "low_prob", "prob_threshold": 0.7},
        "high_rule": {"type": "high_prob", "prob_threshold": 0.6},
    }
    return KILLS_RULES


def _load_team_predictability() -> Dict[int, Dict[str, Any]]:
    global TEAM_PREDICTABILITY_CACHE, TEAM_PREDICTABILITY_MTIME
    path = Path("/Users/alex/Documents/ingame/reports/team_kills_predictability.json")
    if not path.exists():
        return {}

    try:
        mtime = path.stat().st_mtime
    except Exception:
        mtime = None

    if TEAM_PREDICTABILITY_CACHE is not None and mtime == TEAM_PREDICTABILITY_MTIME:
        return TEAM_PREDICTABILITY_CACHE

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load team predictability: {e}")
        return {}

    cache: Dict[int, Dict[str, Any]] = {}
    for row in data if isinstance(data, list) else []:
        try:
            tid = int(row.get("team_id"))
        except Exception:
            continue
        cache[tid] = {
            "matches": row.get("matches"),
            "mae": row.get("mae"),
            "stable_rate": row.get("stable_rate"),
            "avg_shared_recent": row.get("avg_shared_recent"),
            "new_team_rate": row.get("new_team_rate"),
            "team_name": row.get("team_name"),
        }

    TEAM_PREDICTABILITY_CACHE = cache
    TEAM_PREDICTABILITY_MTIME = mtime
    return cache


def _team_predictability_filter(
    radiant_team_id: Optional[int],
    dire_team_id: Optional[int],
    row: Dict[str, Any],
    rules: Dict[str, Any],
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    cfg = dict(rules.get("team_predictability_filter") or {})
    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        return True, None, {}

    min_matches = int(cfg.get("min_matches", 20))
    max_mae = float(cfg.get("max_mae", 14.0))
    min_stable_rate = float(cfg.get("min_stable_rate", 0.9))
    block_new_team = bool(cfg.get("block_new_team", True))
    block_if_unknown = bool(cfg.get("block_if_unknown", False))
    interval_max = cfg.get("max_pred_interval")

    def _is_new(val: Any) -> bool:
        try:
            v = float(val)
            if math.isnan(v):
                return False
            return v >= 1.0
        except Exception:
            return False

    if block_new_team and (
        _is_new(row.get("radiant_roster_new_team"))
        or _is_new(row.get("dire_roster_new_team"))
    ):
        return False, "new_team", {}

    if interval_max and float(interval_max) > 0:
        try:
            pred_q10 = float(row.get("pred_q10", float("nan")))
            pred_q90 = float(row.get("pred_q90", float("nan")))
        except Exception:
            pred_q10 = float("nan")
            pred_q90 = float("nan")
        if not math.isfinite(pred_q10) or not math.isfinite(pred_q90):
            return False, "uncertainty", {}
        if (pred_q90 - pred_q10) > float(interval_max):
            return False, "uncertainty", {}

    metrics = _load_team_predictability()

    def _check_team(team_id: Optional[int]) -> Tuple[Optional[str], Dict[str, Any]]:
        if not team_id or team_id <= 0:
            return ("unknown" if block_if_unknown else None), {}
        data = metrics.get(int(team_id))
        if not data:
            return ("unknown" if block_if_unknown else None), {}
        matches = data.get("matches")
        mae = data.get("mae")
        stable_rate = data.get("stable_rate")
        info = {
            "matches": matches,
            "mae": mae,
            "stable_rate": stable_rate,
            "team_name": data.get("team_name"),
        }
        try:
            matches = int(matches)
        except Exception:
            matches = 0
        if matches < min_matches:
            return ("unknown" if block_if_unknown else None), info
        try:
            if mae is not None and float(mae) > max_mae:
                return "mae", info
        except Exception:
            pass
        try:
            if stable_rate is not None and float(stable_rate) < min_stable_rate:
                return "stable_rate", info
        except Exception:
            pass
        return None, info

    r_reason, r_info = _check_team(radiant_team_id)
    d_reason, d_info = _check_team(dire_team_id)

    details = {"radiant": r_info, "dire": d_info}

    for reason in (r_reason, d_reason):
        if reason in {"mae", "stable_rate", "unknown"}:
            return False, reason, details

    return True, None, details


def _load_pub_hero_priors() -> Dict[int, Dict[str, float]]:
    global KILLS_PUB_PRIORS
    if KILLS_PUB_PRIORS is not None:
        return KILLS_PUB_PRIORS
    priors_path = Path("/Users/alex/Documents/ingame/ml-models/pub_hero_priors.json")
    if priors_path.exists():
        try:
            with priors_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            KILLS_PUB_PRIORS = {int(k): v for k, v in data.items()}
            return KILLS_PUB_PRIORS
        except Exception as e:
            logger.warning(f"Failed to load pub hero priors: {e}")
    KILLS_PUB_PRIORS = {}
    return KILLS_PUB_PRIORS


def _draft_feature_allowed(name: str) -> bool:
    n = name.lower()
    if n in {"radiant_team_id", "dire_team_id"}:
        return False
    if n.startswith("radiant_hero_") or n.startswith("dire_hero_"):
        parts = n.split("_")
        if len(parts) == 3 and parts[2].isdigit():
            return False
    if "glicko" in n:
        return False
    if "winrate" in n:
        return False
    return True




def _load_kills_models() -> bool:
    global KILLS_MODELS, KILLS_FEATURE_COLS, KILLS_CAT_COLS, KILLS_Q10_MODEL, KILLS_Q90_MODEL
    if KILLS_MODELS is not None:
        return True
    try:
        from catboost import CatBoostRegressor, CatBoostClassifier
        import pandas as pd  # noqa: F401
    except Exception as e:
        logger.warning(f"CatBoost not available: {e}")
        return False

    meta_path = Path("/Users/alex/Documents/ingame/ml-models/live_cb_kills_reg_meta.json")
    if not meta_path.exists():
        logger.warning("Kills meta not found: %s", meta_path)
        return False
    with meta_path.open("r", encoding="utf-8") as f:
        meta = json.load(f)
    KILLS_FEATURE_COLS = meta.get("feature_cols", [])
    KILLS_CAT_COLS = meta.get("cat_features", [])

    models = {}
    try:
        reg_all = CatBoostRegressor()
        reg_all.load_model("/Users/alex/Documents/ingame/ml-models/live_cb_kills_reg.cbm")
        models["reg_all"] = reg_all
        reg_low = CatBoostRegressor()
        reg_low.load_model("/Users/alex/Documents/ingame/ml-models/live_cb_kills_reg_low.cbm")
        models["reg_low"] = reg_low
        reg_high = CatBoostRegressor()
        reg_high.load_model("/Users/alex/Documents/ingame/ml-models/live_cb_kills_reg_high.cbm")
        models["reg_high"] = reg_high
        cls_low = CatBoostClassifier()
        cls_low.load_model("/Users/alex/Documents/ingame/ml-models/live_cb_kills_low_cls.cbm")
        models["cls_low"] = cls_low
        cls_high = CatBoostClassifier()
        cls_high.load_model("/Users/alex/Documents/ingame/ml-models/live_cb_kills_high_cls.cbm")
        models["cls_high"] = cls_high
    except Exception as e:
        logger.warning(f"Failed to load kills models: {e}")
        return False

    q10_path = Path("/Users/alex/Documents/ingame/ml-models/live_cb_kills_reg_q10.cbm")
    q90_path = Path("/Users/alex/Documents/ingame/ml-models/live_cb_kills_reg_q90.cbm")
    if q10_path.exists() and q90_path.exists():
        try:
            q10_model = CatBoostRegressor()
            q10_model.load_model(str(q10_path))
            q90_model = CatBoostRegressor()
            q90_model.load_model(str(q90_path))
            KILLS_Q10_MODEL = q10_model
            KILLS_Q90_MODEL = q90_model
        except Exception as e:
            logger.warning(f"Failed to load kills quantile models: {e}")
            KILLS_Q10_MODEL = None
            KILLS_Q90_MODEL = None
    else:
        KILLS_Q10_MODEL = None
        KILLS_Q90_MODEL = None

    KILLS_MODELS = models
    return True


def _load_kills_group_models(kind: str, key: Any) -> Optional[Dict[str, Any]]:
    global KILLS_MODELS_BY_PATCH, KILLS_MODELS_BY_TIER
    cache = KILLS_MODELS_BY_PATCH if kind == "patch" else KILLS_MODELS_BY_TIER
    if key in cache:
        return cache[key]
    try:
        from catboost import CatBoostRegressor, CatBoostClassifier
    except Exception as e:
        logger.warning(f"CatBoost not available for {kind} models: {e}")
        cache[key] = None
        return None

    if kind == "patch":
        slug = _patch_label_to_slug(str(key))
        suffix = f"patch_{slug}"
    else:
        suffix = f"tier_{key}"

    models_dir = Path("/Users/alex/Documents/ingame/ml-models")
    reg_all_path = models_dir / f"live_cb_kills_reg_{suffix}.cbm"
    reg_low_path = models_dir / f"live_cb_kills_reg_{suffix}_low.cbm"
    reg_high_path = models_dir / f"live_cb_kills_reg_{suffix}_high.cbm"
    cls_low_path = models_dir / f"live_cb_kills_low_cls_{suffix}.cbm"
    cls_high_path = models_dir / f"live_cb_kills_high_cls_{suffix}.cbm"

    if not all(p.exists() for p in (reg_all_path, reg_low_path, reg_high_path, cls_low_path, cls_high_path)):
        cache[key] = None
        return None

    try:
        reg_all = CatBoostRegressor()
        reg_all.load_model(str(reg_all_path))
        reg_low = CatBoostRegressor()
        reg_low.load_model(str(reg_low_path))
        reg_high = CatBoostRegressor()
        reg_high.load_model(str(reg_high_path))
        cls_low = CatBoostClassifier()
        cls_low.load_model(str(cls_low_path))
        cls_high = CatBoostClassifier()
        cls_high.load_model(str(cls_high_path))
        models = {
            "reg_all": reg_all,
            "reg_low": reg_low,
            "reg_high": reg_high,
            "cls_low": cls_low,
            "cls_high": cls_high,
        }
    except Exception as e:
        logger.warning("Failed to load %s models (%s): %s", kind, key, e)
        cache[key] = None
        return None

    cache[key] = models
    return models


def _build_kills_priors() -> Dict[str, Any]:
    cache_path = Path("/Users/alex/Documents/ingame/ml-models/pro_kills_priors.json")
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as f:
            cached = json.load(f)
        if cached.get("priors_version") == 8:
            return cached

    clean_path = Path(
        "/Users/alex/Documents/ingame/pro_heroes_data/json_parts_split_from_object/clean_data.json"
    )
    with clean_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    matches: List[Tuple[int, Dict[str, Any]]] = []
    for match in data.values():
        start_time = _coerce_int(match.get("startDateTime"))
        match_id = _coerce_int(match.get("id"))
        matches.append((start_time, match_id, match))
    matches.sort(key=lambda x: (x[0], x[1]))

    hero_stats: Dict[str, Dict[str, float]] = {}
    team_stats: Dict[str, Dict[str, float]] = {}
    roster_group_stats: Dict[str, Dict[str, float]] = {}
    team_rosters: Dict[str, List[Dict[str, Any]]] = {}
    player_stats: Dict[str, Dict[str, float]] = {}
    hero_pair_stats: Dict[str, Dict[str, float]] = {}
    hero_vs_stats: Dict[str, Dict[str, float]] = {}
    player_hero_stats: Dict[str, Dict[str, float]] = {}
    player_pair_stats: Dict[str, Dict[str, float]] = {}
    team_vs_stats: Dict[str, Dict[str, float]] = {}
    team_early_stats: Dict[str, Dict[str, float]] = {}
    hero_early_stats: Dict[str, Dict[str, float]] = {}
    player_early_stats: Dict[str, Dict[str, float]] = {}
    team_vs_early_stats: Dict[str, Dict[str, float]] = {}
    league_stats: Dict[str, Dict[str, float]] = {}
    version_stats: Dict[str, Dict[str, float]] = {}

    global_player = {
        "count": 0,
        "kills": 0.0,
        "deaths": 0.0,
        "assists": 0.0,
        "duration": 0.0,
        "gpm": 0.0,
        "xpm": 0.0,
        "hero_damage": 0.0,
        "tower_damage": 0.0,
        "imp": 0.0,
        "lhpm": 0.0,
        "denypm": 0.0,
        "healpm": 0.0,
        "invispm": 0.0,
        "level": 0.0,
    }
    global_team = {
        "count": 0,
        "kills_for": 0.0,
        "kills_against": 0.0,
        "total_kills": 0.0,
        "kpm": 0.0,
        "duration": 0.0,
        "over50": 0.0,
        "under40": 0.0,
    }
    global_hero = {
        "count": 0,
        "total_kills": 0.0,
        "kpm": 0.0,
        "duration": 0.0,
        "over50": 0.0,
        "under40": 0.0,
    }
    global_pair = {"count": 0, "total_kills": 0.0, "kpm": 0.0, "duration": 0.0}
    global_vs = {"count": 0, "total_kills": 0.0, "kpm": 0.0}
    global_player_hero = {"count": 0, "total_kills": 0.0, "kpm": 0.0}
    global_player_pair = {"count": 0, "total_kills": 0.0, "kpm": 0.0, "duration": 0.0}
    global_team_vs = {"count": 0, "total_kills": 0.0, "kpm": 0.0, "over50": 0.0, "under40": 0.0}
    global_team_early = {
        "count": 0,
        "for10": 0.0,
        "against10": 0.0,
        "total10": 0.0,
        "share10": 0.0,
        "diff10": 0.0,
    }
    global_hero_early = {
        "count": 0,
        "for10": 0.0,
        "against10": 0.0,
        "total10": 0.0,
        "share10": 0.0,
    }
    global_player_early = {
        "count": 0,
        "for10": 0.0,
        "against10": 0.0,
        "total10": 0.0,
        "share10": 0.0,
    }
    global_team_vs_early = {
        "count": 0,
        "total10": 0.0,
        "kpm10": 0.0,
        "abs_diff10": 0.0,
    }
    global_league = {
        "count": 0,
        "total_kills": 0.0,
        "kpm": 0.0,
        "duration": 0.0,
        "over50": 0.0,
        "under40": 0.0,
    }
    global_version = {
        "count": 0,
        "total_kills": 0.0,
        "kpm": 0.0,
        "duration": 0.0,
        "over50": 0.0,
        "under40": 0.0,
    }
    recent_window = 8
    hero_recent_window = 20
    player_recent_window = 20
    team_recent: Dict[int, deque] = {}
    hero_recent: Dict[int, deque] = {}
    player_recent: Dict[int, deque] = {}
    missing_stats: Dict[str, int] = {}
    team_elo: Dict[int, float] = {}
    team_games: Dict[int, int] = {}
    team_roster_state: Dict[int, Dict[str, Any]] = {}

    def elo_expected(r_a: float, r_b: float) -> float:
        return 1.0 / (1.0 + 10 ** ((r_b - r_a) / 400.0))

    def elo_k(games_played: int) -> float:
        return max(10.0, 64.0 / math.sqrt(1.0 + games_played))

    def update_stat(stats: Dict[str, Dict[str, float]], key: str, updates: Dict[str, float]) -> None:
        st = stats.get(key)
        if st is None:
            st = {k: 0.0 for k in updates.keys()}
            st["count"] = 0
            stats[key] = st
        st["count"] += 1
        for k, v in updates.items():
            if v is None or (isinstance(v, str) and v.strip().lower() == "none"):
                missing_stats[k] = missing_stats.get(k, 0) + 1
            st[k] += safe_float(v, 0.0)

    def update_roster_group(
        team_id: int,
        roster_ids: List[int],
        kills_for: float,
        kills_against: float,
        total_match_kills: float,
        kpm: float,
        duration_min: float,
        min_shared: int = 3,
    ) -> None:
        if team_id <= 0:
            return
        roster = {int(pid) for pid in roster_ids if int(pid) > 0}
        roster_count = len(roster)
        prev = team_roster_state.get(team_id)
        if prev is None:
            group_id = 0
            group_matches = 1
        elif roster_count < min_shared:
            group_id = prev["group_id"] + 1
            group_matches = 1
        else:
            prev_roster = prev.get("roster") or set()
            shared = len(roster & prev_roster)
            if shared >= min_shared:
                group_id = prev["group_id"]
                group_matches = prev.get("group_matches", 0) + 1
            else:
                group_id = prev["group_id"] + 1
                group_matches = 1

        team_roster_state[team_id] = {
            "roster": roster,
            "group_id": group_id,
            "group_matches": group_matches,
        }

        if roster:
            groups = team_rosters.get(str(team_id))
            if groups is None:
                groups = []
                team_rosters[str(team_id)] = groups
            found = False
            for group in groups:
                if int(group.get("group_id", -1)) == group_id:
                    group["players"] = sorted(roster)
                    group["count"] = int(group.get("count", 0)) + 1
                    found = True
                    break
            if not found:
                groups.append({"group_id": group_id, "players": sorted(roster), "count": 1})

        key = f"{team_id}_{group_id}"
        rg = roster_group_stats.get(key)
        if rg is None:
            rg = {
                "count": 0,
                "kills_for": 0.0,
                "kills_against": 0.0,
                "total_kills": 0.0,
                "kpm": 0.0,
                "duration": 0.0,
                "over50": 0.0,
                "under40": 0.0,
            }
            roster_group_stats[key] = rg
        rg["count"] += 1
        rg["kills_for"] += float(kills_for)
        rg["kills_against"] += float(kills_against)
        rg["total_kills"] += float(total_match_kills)
        rg["kpm"] += float(kpm)
        rg["duration"] += float(duration_min)
        rg["over50"] += 1.0 if total_match_kills > 50 else 0.0
        rg["under40"] += 1.0 if total_match_kills < 40 else 0.0

    for _, _, match in matches:
        players = match.get("players") or []
        if len(players) != 10:
            continue
        radiant = [p for p in players if p.get("isRadiant")]
        dire = [p for p in players if not p.get("isRadiant")]
        if len(radiant) != 5 or len(dire) != 5:
            continue

        total_kills = sum(safe_float(p.get("kills"), 0.0) for p in players)
        rad_list = match.get("radiantKills") or []
        dire_list = match.get("direKills") or []
        duration_seconds = _coerce_float(match.get("durationSeconds"))
        if duration_seconds > 0:
            duration_min = duration_seconds / 60.0
        else:
            duration_min = float(max(len(rad_list), len(dire_list)))
        if duration_min <= 0:
            duration_min = 1.0
        kpm = total_kills / duration_min

        def _minute_val(arr: List[Any], idx: int) -> float:
            if idx >= len(arr):
                return float("nan")
            try:
                return float(arr[idx])
            except Exception:
                return float("nan")

        rad_vals = [_minute_val(rad_list, i) for i in range(10)]
        dire_vals = [_minute_val(dire_list, i) for i in range(10)]
        rad_valid = [v for v in rad_vals if not math.isnan(v)]
        dire_valid = [v for v in dire_vals if not math.isnan(v)]
        kill_minutes_available = min(10, len(rad_list), len(dire_list)) if rad_list and dire_list else 0
        rad10 = float(sum(rad_valid)) if rad_valid else float("nan")
        dire10 = float(sum(dire_valid)) if dire_valid else float("nan")
        total10 = rad10 + dire10 if not math.isnan(rad10) and not math.isnan(dire10) else float("nan")

        rad_ids = [int(p.get("heroId") or 0) for p in radiant]
        dire_ids = [int(p.get("heroId") or 0) for p in dire]
        rad_pids = [int((p.get("steamAccount") or {}).get("id") or 0) for p in radiant]
        dire_pids = [int((p.get("steamAccount") or {}).get("id") or 0) for p in dire]

        radiant_team_id = _coerce_int((match.get("radiantTeam") or {}).get("id"))
        dire_team_id = _coerce_int((match.get("direTeam") or {}).get("id"))

        rad_kills = sum(safe_float(p.get("kills"), 0.0) for p in radiant)
        dire_kills = sum(safe_float(p.get("kills"), 0.0) for p in dire)

        if (
            kill_minutes_available >= 10
            and not math.isnan(total10)
            and not math.isnan(rad10)
            and not math.isnan(dire10)
        ):
            r_share10 = (rad10 / total10) if total10 > 0 else 0.0
            d_share10 = (dire10 / total10) if total10 > 0 else 0.0
            for team_id, for10, against10, share10 in (
                (radiant_team_id, rad10, dire10, r_share10),
                (dire_team_id, dire10, rad10, d_share10),
            ):
                if team_id <= 0:
                    continue
                update_stat(
                    team_early_stats,
                    str(team_id),
                    {
                        "for10": for10,
                        "against10": against10,
                        "total10": total10,
                        "share10": share10,
                        "diff10": for10 - against10,
                    },
                )
                global_team_early["count"] += 1
                global_team_early["for10"] += float(for10)
                global_team_early["against10"] += float(against10)
                global_team_early["total10"] += float(total10)
                global_team_early["share10"] += float(share10)
                global_team_early["diff10"] += float(for10 - against10)

            if radiant_team_id > 0 and dire_team_id > 0:
                team_key = f"{min(radiant_team_id, dire_team_id)}_{max(radiant_team_id, dire_team_id)}"
                update_stat(
                    team_vs_early_stats,
                    team_key,
                    {
                        "total10": total10,
                        "kpm10": total10 / 10.0,
                        "abs_diff10": abs(rad10 - dire10),
                    },
                )
                global_team_vs_early["count"] += 1
                global_team_vs_early["total10"] += float(total10)
                global_team_vs_early["kpm10"] += float(total10 / 10.0)
                global_team_vs_early["abs_diff10"] += float(abs(rad10 - dire10))

            for hid in rad_ids:
                if hid <= 0:
                    continue
                update_stat(
                    hero_early_stats,
                    str(hid),
                    {
                        "for10": rad10,
                        "against10": dire10,
                        "total10": total10,
                        "share10": r_share10,
                    },
                )
                global_hero_early["count"] += 1
                global_hero_early["for10"] += float(rad10)
                global_hero_early["against10"] += float(dire10)
                global_hero_early["total10"] += float(total10)
                global_hero_early["share10"] += float(r_share10)

            for hid in dire_ids:
                if hid <= 0:
                    continue
                update_stat(
                    hero_early_stats,
                    str(hid),
                    {
                        "for10": dire10,
                        "against10": rad10,
                        "total10": total10,
                        "share10": d_share10,
                    },
                )
                global_hero_early["count"] += 1
                global_hero_early["for10"] += float(dire10)
                global_hero_early["against10"] += float(rad10)
                global_hero_early["total10"] += float(total10)
                global_hero_early["share10"] += float(d_share10)

            for pid in rad_pids:
                if pid <= 0:
                    continue
                update_stat(
                    player_early_stats,
                    str(pid),
                    {
                        "for10": rad10,
                        "against10": dire10,
                        "total10": total10,
                        "share10": r_share10,
                    },
                )
                global_player_early["count"] += 1
                global_player_early["for10"] += float(rad10)
                global_player_early["against10"] += float(dire10)
                global_player_early["total10"] += float(total10)
                global_player_early["share10"] += float(r_share10)

            for pid in dire_pids:
                if pid <= 0:
                    continue
                update_stat(
                    player_early_stats,
                    str(pid),
                    {
                        "for10": dire10,
                        "against10": rad10,
                        "total10": total10,
                        "share10": d_share10,
                    },
                )
                global_player_early["count"] += 1
                global_player_early["for10"] += float(dire10)
                global_player_early["against10"] += float(rad10)
                global_player_early["total10"] += float(total10)
                global_player_early["share10"] += float(d_share10)

        for team_pids in (rad_pids, dire_pids):
            for i in range(len(team_pids)):
                for j in range(i + 1, len(team_pids)):
                    p1 = team_pids[i]
                    p2 = team_pids[j]
                    if p1 <= 0 or p2 <= 0:
                        continue
                    key = f"{min(p1, p2)}_{max(p1, p2)}"
                    update_stat(
                        player_pair_stats,
                        key,
                        {
                            "total_kills": total_kills,
                            "kpm": kpm,
                            "duration": duration_min,
                        },
                    )
                    global_player_pair["count"] += 1
                    global_player_pair["total_kills"] += float(total_kills)
                    global_player_pair["kpm"] += float(kpm)
                    global_player_pair["duration"] += float(duration_min)

        update_roster_group(
            radiant_team_id,
            rad_pids,
            rad_kills,
            dire_kills,
            total_kills,
            kpm,
            duration_min,
        )
        update_roster_group(
            dire_team_id,
            dire_pids,
            dire_kills,
            rad_kills,
            total_kills,
            kpm,
            duration_min,
        )

        for team_id in (radiant_team_id, dire_team_id):
            if team_id <= 0:
                continue
            hist = team_recent.get(team_id)
            if hist is None:
                hist = deque(maxlen=recent_window)
                team_recent[team_id] = hist
            hist.append((float(total_kills), float(kpm), float(duration_min)))

        for hid in rad_ids + dire_ids:
            if hid <= 0:
                continue
            hist = hero_recent.get(hid)
            if hist is None:
                hist = deque(maxlen=hero_recent_window)
                hero_recent[hid] = hist
            hist.append((float(total_kills), float(kpm), float(duration_min)))

        if radiant_team_id > 0:
            update_stat(
                team_stats,
                str(radiant_team_id),
                {
                    "kills_for": rad_kills,
                    "kills_against": dire_kills,
                    "total_kills": total_kills,
                    "kpm": kpm,
                    "duration": duration_min,
                    "over50": 1.0 if total_kills > 50 else 0.0,
                    "under40": 1.0 if total_kills < 40 else 0.0,
                },
            )
        if dire_team_id > 0:
            update_stat(
                team_stats,
                str(dire_team_id),
                {
                    "kills_for": dire_kills,
                    "kills_against": rad_kills,
                    "total_kills": total_kills,
                    "kpm": kpm,
                    "duration": duration_min,
                    "over50": 1.0 if total_kills > 50 else 0.0,
                    "under40": 1.0 if total_kills < 40 else 0.0,
                },
            )

        global_team["count"] += 2
        global_team["kills_for"] += rad_kills + dire_kills
        global_team["kills_against"] += dire_kills + rad_kills
        global_team["total_kills"] += total_kills * 2
        global_team["kpm"] += kpm * 2
        global_team["duration"] += duration_min * 2
        global_team["over50"] += (1.0 if total_kills > 50 else 0.0) * 2
        global_team["under40"] += (1.0 if total_kills < 40 else 0.0) * 2

        if radiant_team_id > 0 and dire_team_id > 0:
            team_key = f"{min(radiant_team_id, dire_team_id)}_{max(radiant_team_id, dire_team_id)}"
            update_stat(
                team_vs_stats,
                team_key,
                {
                    "total_kills": total_kills,
                    "kpm": kpm,
                    "over50": 1.0 if total_kills > 50 else 0.0,
                    "under40": 1.0 if total_kills < 40 else 0.0,
                },
            )
            global_team_vs["count"] += 1
            global_team_vs["total_kills"] += total_kills
            global_team_vs["kpm"] += kpm
            global_team_vs["over50"] += 1.0 if total_kills > 50 else 0.0
            global_team_vs["under40"] += 1.0 if total_kills < 40 else 0.0

        league_id = _coerce_int((match.get("league") or {}).get("id"))
        if league_id > 0:
            update_stat(
                league_stats,
                str(league_id),
                {
                    "total_kills": total_kills,
                    "kpm": kpm,
                    "duration": duration_min,
                    "over50": 1.0 if total_kills > 50 else 0.0,
                    "under40": 1.0 if total_kills < 40 else 0.0,
                },
            )
            global_league["count"] += 1
            global_league["total_kills"] += total_kills
            global_league["kpm"] += kpm
            global_league["duration"] += duration_min
            global_league["over50"] += 1.0 if total_kills > 50 else 0.0
            global_league["under40"] += 1.0 if total_kills < 40 else 0.0

        version_id = _coerce_int(match.get("gameVersionId"))
        if version_id > 0:
            update_stat(
                version_stats,
                str(version_id),
                {
                    "total_kills": total_kills,
                    "kpm": kpm,
                    "duration": duration_min,
                    "over50": 1.0 if total_kills > 50 else 0.0,
                    "under40": 1.0 if total_kills < 40 else 0.0,
                },
            )
            global_version["count"] += 1
            global_version["total_kills"] += total_kills
            global_version["kpm"] += kpm
            global_version["duration"] += duration_min
            global_version["over50"] += 1.0 if total_kills > 50 else 0.0
            global_version["under40"] += 1.0 if total_kills < 40 else 0.0

        for p in players:
            pid = _coerce_int((p.get("steamAccount") or {}).get("id"))
            if pid <= 0:
                continue
            p_kills = safe_float(p.get("kills"), 0.0)
            p_deaths = safe_float(p.get("deaths"), 0.0)
            p_assists = safe_float(p.get("assists"), 0.0)
            lh = safe_float(p.get("numLastHits"), 0.0)
            denies = safe_float(p.get("numDenies"), 0.0)
            heal = safe_float(p.get("heroHealing"), 0.0)
            invis = safe_float(p.get("invisibleSeconds"), 0.0)
            level = safe_float(p.get("level"), 0.0)
            if duration_min > 0:
                lhpm = lh / duration_min
                denypm = denies / duration_min
                healpm = heal / duration_min
                invispm = invis / duration_min
            else:
                lhpm = 0.0
                denypm = 0.0
                healpm = 0.0
                invispm = 0.0
            update_stat(
                player_stats,
                str(pid),
                {
                    "kills": p_kills,
                    "deaths": p_deaths,
                    "assists": p_assists,
                    "duration": duration_min,
                    "gpm": p.get("goldPerMinute", 0),
                    "xpm": p.get("experiencePerMinute", 0),
                    "hero_damage": p.get("heroDamage", 0),
                    "tower_damage": p.get("towerDamage", 0),
                    "imp": p.get("imp", 0),
                    "lhpm": lhpm,
                    "denypm": denypm,
                    "healpm": healpm,
                    "invispm": invispm,
                    "level": level,
                },
            )

            global_player["count"] += 1
            global_player["kills"] += p_kills
            global_player["deaths"] += p_deaths
            global_player["assists"] += p_assists
            global_player["duration"] += duration_min
            global_player["gpm"] += safe_float(p.get("goldPerMinute"), 0.0)
            global_player["xpm"] += safe_float(p.get("experiencePerMinute"), 0.0)
            global_player["hero_damage"] += safe_float(p.get("heroDamage"), 0.0)
            global_player["tower_damage"] += safe_float(p.get("towerDamage"), 0.0)
            global_player["imp"] += safe_float(p.get("imp"), 0.0)
            global_player["lhpm"] += lhpm
            global_player["denypm"] += denypm
            global_player["healpm"] += healpm
            global_player["invispm"] += invispm
            global_player["level"] += level

            hist = player_recent.get(pid)
            if hist is None:
                hist = deque(maxlen=player_recent_window)
                player_recent[pid] = hist
            p_kpm = (p_kills / duration_min) if duration_min > 0 else 0.0
            hist.append((p_kills, p_deaths, p_assists, p_kpm))

            hero_id = _coerce_int(p.get("heroId"))
            if hero_id > 0:
                key = f"{pid}_{hero_id}"
                update_stat(player_hero_stats, key, {"total_kills": total_kills, "kpm": kpm})
                global_player_hero["count"] += 1
                global_player_hero["total_kills"] += total_kills
                global_player_hero["kpm"] += kpm

        for hid in rad_ids + dire_ids:
            if hid <= 0:
                continue
            update_stat(
                hero_stats,
                str(hid),
                {
                    "total_kills": total_kills,
                    "kpm": kpm,
                    "duration": duration_min,
                    "over50": 1.0 if total_kills > 50 else 0.0,
                    "under40": 1.0 if total_kills < 40 else 0.0,
                },
            )
            global_hero["count"] += 1
            global_hero["total_kills"] += total_kills
            global_hero["kpm"] += kpm
            global_hero["duration"] += duration_min
            global_hero["over50"] += 1.0 if total_kills > 50 else 0.0
            global_hero["under40"] += 1.0 if total_kills < 40 else 0.0

        def pair_key(a: int, b: int) -> str:
            return f"{min(a, b)}_{max(a, b)}"

        for team_ids in (rad_ids, dire_ids):
            for i in range(len(team_ids)):
                for j in range(i + 1, len(team_ids)):
                    if team_ids[i] <= 0 or team_ids[j] <= 0:
                        continue
                    key = pair_key(team_ids[i], team_ids[j])
                    update_stat(
                        hero_pair_stats,
                        key,
                        {"total_kills": total_kills, "kpm": kpm, "duration": duration_min},
                    )
                    global_pair["count"] += 1
                    global_pair["total_kills"] += total_kills
                    global_pair["kpm"] += kpm
                    global_pair["duration"] += duration_min

        for rh in rad_ids:
            for dh in dire_ids:
                if rh <= 0 or dh <= 0:
                    continue
                key = pair_key(rh, dh)
                update_stat(hero_vs_stats, key, {"total_kills": total_kills, "kpm": kpm})
                global_vs["count"] += 1
                global_vs["total_kills"] += total_kills
                global_vs["kpm"] += kpm

        radiant_win = match.get("didRadiantWin")
        if radiant_team_id > 0 and dire_team_id > 0 and radiant_win is not None:
            r_rating = team_elo.get(radiant_team_id, 1500.0)
            d_rating = team_elo.get(dire_team_id, 1500.0)
            r_games = team_games.get(radiant_team_id, 0)
            d_games = team_games.get(dire_team_id, 0)
            exp_r = elo_expected(r_rating, d_rating)
            score_r = 1.0 if radiant_win else 0.0
            score_d = 1.0 - score_r
            team_elo[radiant_team_id] = r_rating + elo_k(r_games) * (score_r - exp_r)
            team_elo[dire_team_id] = d_rating + elo_k(d_games) * (score_d - (1.0 - exp_r))
            team_games[radiant_team_id] = r_games + 1
            team_games[dire_team_id] = d_games + 1

    team_recent_stats: Dict[str, Dict[str, float]] = {}
    for team_id, hist in team_recent.items():
        if not hist:
            continue
        totals = [t for t, _, _ in hist]
        kpms = [k for _, k, _ in hist]
        durs = [d for _, _, d in hist]
        count = len(totals)
        avg_total = sum(totals) / count
        avg_kpm = sum(kpms) / count
        avg_dur = sum(durs) / count
        over50 = sum(1 for t in totals if t > 50) / count
        under40 = sum(1 for t in totals if t < 40) / count
        std_total = math.sqrt(sum((t - avg_total) ** 2 for t in totals) / count) if count else 0.0
        team_recent_stats[str(team_id)] = {
            "recent_total": avg_total,
            "recent_kpm": avg_kpm,
            "recent_dur": avg_dur,
            "recent_over50": over50,
            "recent_under40": under40,
            "recent_std": std_total,
            "recent_count": count,
        }

    hero_recent_stats: Dict[str, Dict[str, float]] = {}
    for hero_id, hist in hero_recent.items():
        if not hist:
            continue
        totals = [t for t, _, _ in hist]
        kpms = [k for _, k, _ in hist]
        durs = [d for _, _, d in hist]
        count = len(totals)
        hero_recent_stats[str(hero_id)] = {
            "recent_total": float(sum(totals) / count),
            "recent_kpm": float(sum(kpms) / count),
            "recent_dur": float(sum(durs) / count),
            "recent_count": count,
        }

    player_recent_stats: Dict[str, Dict[str, float]] = {}
    for player_id, hist in player_recent.items():
        if not hist:
            continue
        kills = [k for k, _, _, _ in hist]
        deaths = [d for _, d, _, _ in hist]
        assists = [a for _, _, a, _ in hist]
        kpms = [kpm for _, _, _, kpm in hist]
        count = len(kills)
        player_recent_stats[str(player_id)] = {
            "recent_kills": float(sum(kills) / count),
            "recent_deaths": float(sum(deaths) / count),
            "recent_assists": float(sum(assists) / count),
            "recent_kpm": float(sum(kpms) / count),
            "recent_count": count,
        }

    player_unique: Dict[str, int] = {}
    player_unique_sets: Dict[str, set] = {}
    for key in player_hero_stats.keys():
        if "_" not in key:
            continue
        pid, hid = key.split("_", 1)
        s = player_unique_sets.get(pid)
        if s is None:
            s = set()
            player_unique_sets[pid] = s
        s.add(hid)
    for pid, hs in player_unique_sets.items():
        player_unique[pid] = len(hs)

    priors = {
        "priors_version": 8,
        "hero_stats": hero_stats,
        "team_stats": team_stats,
        "roster_group_stats": roster_group_stats,
        "team_rosters": team_rosters,
        "team_early_stats": team_early_stats,
        "hero_early_stats": hero_early_stats,
        "player_early_stats": player_early_stats,
        "team_recent_stats": team_recent_stats,
        "hero_recent_stats": hero_recent_stats,
        "player_recent_stats": player_recent_stats,
        "player_stats": player_stats,
        "player_unique": player_unique,
        "hero_pair_stats": hero_pair_stats,
        "hero_vs_stats": hero_vs_stats,
        "player_hero_stats": player_hero_stats,
        "player_pair_stats": player_pair_stats,
        "team_vs_stats": team_vs_stats,
        "team_vs_early_stats": team_vs_early_stats,
        "league_stats": league_stats,
        "version_stats": version_stats,
        "team_elo": {str(k): v for k, v in team_elo.items()},
        "team_elo_games": {str(k): v for k, v in team_games.items()},
        "global_player": global_player,
        "global_team": global_team,
        "global_hero": global_hero,
        "global_pair": global_pair,
        "global_vs": global_vs,
        "global_player_hero": global_player_hero,
        "global_player_pair": global_player_pair,
        "global_team_vs": global_team_vs,
        "global_team_early": global_team_early,
        "global_hero_early": global_hero_early,
        "global_player_early": global_player_early,
        "global_team_vs_early": global_team_vs_early,
        "global_league": global_league,
        "global_version": global_version,
    }

    if missing_stats:
        top_missing = sorted(missing_stats.items(), key=lambda x: (-x[1], x[0]))[:10]
        logger.info("Kills priors missing values (top 10): %s", top_missing)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(priors, f)
    return priors


def _load_kills_priors() -> Dict[str, Any]:
    global KILLS_PRIORS
    if KILLS_PRIORS is None:
        KILLS_PRIORS = _build_kills_priors()
    return KILLS_PRIORS


def _avg_stat(stats: Dict[str, Dict[str, float]], key: str, stat_key: str, global_stats: Dict[str, float]) -> float:
    st = stats.get(key)
    if st and st.get("count", 0) > 0:
        return st.get(stat_key, 0.0) / st["count"]
    if global_stats.get("count", 0) > 0:
        return global_stats.get(stat_key, 0.0) / global_stats["count"]
    return 0.0


def _pair_avg(stats: Dict[str, Dict[str, float]], key: str, stat_key: str, global_stats: Dict[str, float]) -> Tuple[float, int]:
    st = stats.get(key)
    if st and st.get("count", 0) > 0:
        return st.get(stat_key, 0.0) / st["count"], int(st["count"])
    if global_stats.get("count", 0) > 0:
        return global_stats.get(stat_key, 0.0) / global_stats["count"], int(global_stats["count"])
    return 0.0, 0


def _build_kills_feature_row(
    radiant_heroes_and_pos: Dict[str, Dict[str, Any]],
    dire_heroes_and_pos: Dict[str, Dict[str, Any]],
    radiant_team_id: Optional[int],
    dire_team_id: Optional[int],
    league_id: Optional[int],
    data: Dict[str, Any],
) -> Dict[str, Any]:
    priors = _load_kills_priors()
    pub_priors = _load_pub_hero_priors()

    pos_order = ["pos1", "pos2", "pos3", "pos4", "pos5"]
    rad_ids = [int(radiant_heroes_and_pos.get(p, {}).get("hero_id", 0) or 0) for p in pos_order]
    dire_ids = [int(dire_heroes_and_pos.get(p, {}).get("hero_id", 0) or 0) for p in pos_order]
    rad_pids = [int(radiant_heroes_and_pos.get(p, {}).get("account_id", 0) or 0) for p in pos_order]
    dire_pids = [int(dire_heroes_and_pos.get(p, {}).get("account_id", 0) or 0) for p in pos_order]

    radiant_team_id = int(radiant_team_id or 0)
    dire_team_id = int(dire_team_id or 0)
    league_id = int(league_id or 0)

    match_start_time = _coerce_timestamp(
        data.get("startDateTime")
        or (data.get("db") or {}).get("startDateTime")
        or data.get("start_time")
        or ((data.get("live_league_data") or {}).get("match") or {}).get("start_time")
    )
    if match_start_time <= 0:
        match_start_time = None

    team_context = None
    if radiant_team_id > 0 and dire_team_id > 0:
        team_context = get_team_context(
            radiant_team_id,
            dire_team_id,
            rad_pids,
            dire_pids,
            league_id if league_id > 0 else None,
            None,
            match_start_time,
        )
    patch_major_label = _get_patch_major_label(match_start_time or 0)
    patch_id = _get_patch_id(match_start_time or 0)
    r_team_tier = _get_team_tier(radiant_team_id)
    d_team_tier = _get_team_tier(dire_team_id)
    match_tier = (
        team_context.get("match_tier")
        if team_context and team_context.get("match_tier") is not None
        else _determine_match_tier(radiant_team_id, dire_team_id)
    )
    match_tier_known = 1 if (r_team_tier <= 2 and d_team_tier <= 2) else 0
    h2h_avg_total = team_context.get("h2h_avg_total") if team_context else None
    h2h_matches_count = team_context.get("h2h_matches_count") if team_context else None
    league_avg_kills_ctx = team_context.get("league_avg_kills") if team_context else None
    league_kills_std = team_context.get("league_kills_std") if team_context else None
    league_meta_diff = team_context.get("league_meta_diff") if team_context else None
    combined_form_kills = team_context.get("combined_form_kills") if team_context else None
    combined_team_avg_kills = team_context.get("combined_team_avg_kills") if team_context else None
    combined_team_aggression = team_context.get("combined_team_aggression") if team_context else None
    combined_synthetic_kills = team_context.get("combined_synthetic_kills") if team_context else None
    combined_patch_form_kills = team_context.get("combined_patch_form_kills") if team_context else None
    combined_patch_team_avg_kills = team_context.get("combined_patch_team_avg_kills") if team_context else None
    combined_patch_team_aggression = team_context.get("combined_patch_team_aggression") if team_context else None
    radiant_roster_shared_prev = team_context.get("radiant_roster_shared_prev") if team_context else None
    dire_roster_shared_prev = team_context.get("dire_roster_shared_prev") if team_context else None
    radiant_roster_changed_prev = team_context.get("radiant_roster_changed_prev") if team_context else None
    dire_roster_changed_prev = team_context.get("dire_roster_changed_prev") if team_context else None
    radiant_roster_stable_prev = team_context.get("radiant_roster_stable_prev") if team_context else None
    dire_roster_stable_prev = team_context.get("dire_roster_stable_prev") if team_context else None
    radiant_roster_new_team = team_context.get("radiant_roster_new_team") if team_context else None
    dire_roster_new_team = team_context.get("dire_roster_new_team") if team_context else None
    radiant_roster_group_matches = team_context.get("radiant_roster_group_matches") if team_context else None
    dire_roster_group_matches = team_context.get("dire_roster_group_matches") if team_context else None
    radiant_roster_player_count = team_context.get("radiant_roster_player_count") if team_context else None
    dire_roster_player_count = team_context.get("dire_roster_player_count") if team_context else None

    global_player = priors["global_player"]
    global_team = priors["global_team"]
    roster_group_stats = priors.get("roster_group_stats", {})
    team_rosters = priors.get("team_rosters", {})
    global_hero = priors["global_hero"]
    global_pair = priors["global_pair"]
    global_vs = priors["global_vs"]
    global_player_hero = priors["global_player_hero"]
    global_player_pair = priors.get("global_player_pair", {"count": 0})
    global_team_vs = priors["global_team_vs"]
    global_team_early = priors.get("global_team_early", {"count": 0})
    global_hero_early = priors.get("global_hero_early", {"count": 0})
    global_player_early = priors.get("global_player_early", {"count": 0})
    global_team_vs_early = priors.get("global_team_vs_early", {"count": 0})
    global_league = priors["global_league"]
    global_version = priors["global_version"]

    def hero_avg(hid: int, stat_key: str) -> float:
        if hid <= 0:
            return _avg_stat(priors["hero_stats"], "", stat_key, global_hero)
        return _avg_stat(priors["hero_stats"], str(hid), stat_key, global_hero)

    def team_avg(tid: int, stat_key: str) -> float:
        if tid <= 0:
            return _avg_stat(priors["team_stats"], "", stat_key, global_team)
        return _avg_stat(priors["team_stats"], str(tid), stat_key, global_team)

    def player_avg(pid: int, stat_key: str) -> float:
        if pid <= 0:
            return _avg_stat(priors["player_stats"], "", stat_key, global_player)
        return _avg_stat(priors["player_stats"], str(pid), stat_key, global_player)

    def player_kpm(pid: int) -> float:
        st = priors["player_stats"].get(str(pid))
        if st and st.get("duration", 0) > 0:
            return st.get("kills", 0.0) / st["duration"]
        if global_player.get("duration", 0) > 0:
            return global_player.get("kills", 0.0) / global_player["duration"]
        return 0.0

    def player_aggression(pid: int) -> float:
        st = priors["player_stats"].get(str(pid))
        if st and st.get("duration", 0) > 0:
            return (st.get("kills", 0.0) + st.get("assists", 0.0)) / st["duration"]
        if global_player.get("duration", 0) > 0:
            return (global_player.get("kills", 0.0) + global_player.get("assists", 0.0)) / global_player["duration"]
        return 0.0

    def player_feed_pm(pid: int) -> float:
        st = priors["player_stats"].get(str(pid))
        if st and st.get("duration", 0) > 0:
            return st.get("deaths", 0.0) / st["duration"]
        if global_player.get("duration", 0) > 0:
            return global_player.get("deaths", 0.0) / global_player["duration"]
        return 0.0

    def player_hero_share(pid: int, hero_id: int) -> float:
        if pid <= 0 or hero_id <= 0:
            return 0.0
        total = priors["player_stats"].get(str(pid), {}).get("count", 0)
        if total <= 0:
            return 0.0
        key = f"{pid}_{hero_id}"
        st = priors["player_hero_stats"].get(key)
        if not st or st.get("count", 0) <= 0:
            return 0.0
        return st["count"] / total

    player_unique = priors.get("player_unique", {})

    def player_unique_count(pid: int) -> int:
        return int(player_unique.get(str(pid), 0) or 0)

    team_recent_stats = priors.get("team_recent_stats", {})
    hero_recent_stats = priors.get("hero_recent_stats", {})
    player_recent_stats = priors.get("player_recent_stats", {})
    team_early_stats = priors.get("team_early_stats", {})
    hero_early_stats = priors.get("hero_early_stats", {})
    player_early_stats = priors.get("player_early_stats", {})
    team_vs_early_stats = priors.get("team_vs_early_stats", {})
    player_pair_stats = priors.get("player_pair_stats", {})

    def recent_team_stats(team_id: int) -> Tuple[float, float, float, float, float, float, int]:
        st = team_recent_stats.get(str(team_id))
        if not st:
            return (float("nan"), float("nan"), float("nan"), float("nan"), float("nan"), float("nan"), 0)
        return (
            safe_float(st.get("recent_total"), float("nan")),
            safe_float(st.get("recent_kpm"), float("nan")),
            safe_float(st.get("recent_dur"), float("nan")),
            safe_float(st.get("recent_over50"), float("nan")),
            safe_float(st.get("recent_under40"), float("nan")),
            safe_float(st.get("recent_std"), float("nan")),
            int(st.get("recent_count") or 0),
        )

    def recent_hero_stats(hero_id: int) -> Tuple[float, float, float, int]:
        st = hero_recent_stats.get(str(hero_id))
        if not st:
            return (
                hero_avg(hero_id, "total_kills"),
                hero_avg(hero_id, "kpm"),
                hero_avg(hero_id, "duration"),
                0,
            )
        return (
            safe_float(st.get("recent_total"), float("nan")),
            safe_float(st.get("recent_kpm"), float("nan")),
            safe_float(st.get("recent_dur"), float("nan")),
            int(st.get("recent_count") or 0),
        )

    def recent_player_stats(player_id: int) -> Tuple[float, float, float, float, int]:
        st = player_recent_stats.get(str(player_id))
        if not st:
            return (
                player_avg(player_id, "kills"),
                player_avg(player_id, "deaths"),
                player_avg(player_id, "assists"),
                player_kpm(player_id),
                0,
            )
        return (
            safe_float(st.get("recent_kills"), float("nan")),
            safe_float(st.get("recent_deaths"), float("nan")),
            safe_float(st.get("recent_assists"), float("nan")),
            safe_float(st.get("recent_kpm"), float("nan")),
            int(st.get("recent_count") or 0),
        )

    def pub_vals(hero_ids: List[int], key: str) -> List[float]:
        vals = []
        for hid in hero_ids:
            pri = pub_priors.get(hid)
            vals.append(pri.get(key, 0.0) if pri else 0.0)
        return vals

    def pair_key(a: int, b: int) -> str:
        return f"{min(a, b)}_{max(a, b)}"

    def team_pair_features(hero_ids: List[int]) -> Tuple[float, float, float, float]:
        vals_kills = []
        vals_kpm = []
        vals_dur = []
        vals_cnt = []
        for i in range(len(hero_ids)):
            for j in range(i + 1, len(hero_ids)):
                if hero_ids[i] <= 0 or hero_ids[j] <= 0:
                    continue
                key = pair_key(hero_ids[i], hero_ids[j])
                avg_k, cnt = _pair_avg(priors["hero_pair_stats"], key, "total_kills", global_pair)
                avg_kpm, _ = _pair_avg(priors["hero_pair_stats"], key, "kpm", global_pair)
                avg_dur, _ = _pair_avg(priors["hero_pair_stats"], key, "duration", global_pair)
                vals_kills.append(avg_k)
                vals_kpm.append(avg_kpm)
                vals_dur.append(avg_dur)
                vals_cnt.append(cnt)
        if not vals_kills:
            return 0.0, 0.0, 0.0, 0.0
        return (
            float(sum(vals_kills) / len(vals_kills)),
            float(sum(vals_kpm) / len(vals_kpm)),
            float(sum(vals_dur) / len(vals_dur)),
            float(sum(vals_cnt) / len(vals_cnt)) if vals_cnt else 0.0,
        )

    def hero_vs_features(r_ids: List[int], d_ids: List[int]) -> Tuple[float, float, float]:
        vals_kills = []
        vals_kpm = []
        vals_cnt = []
        for rh in r_ids:
            for dh in d_ids:
                if rh <= 0 or dh <= 0:
                    continue
                key = pair_key(rh, dh)
                avg_k, cnt = _pair_avg(priors["hero_vs_stats"], key, "total_kills", global_vs)
                avg_kpm, _ = _pair_avg(priors["hero_vs_stats"], key, "kpm", global_vs)
                vals_kills.append(avg_k)
                vals_kpm.append(avg_kpm)
                vals_cnt.append(cnt)
        if not vals_kills:
            return 0.0, 0.0, 0.0
        return (
            float(sum(vals_kills) / len(vals_kills)),
            float(sum(vals_kpm) / len(vals_kpm)),
            float(sum(vals_cnt) / len(vals_cnt)) if vals_cnt else 0.0,
        )

    def player_hero_features(pids: List[int], hids: List[int]) -> Tuple[float, float, float]:
        vals_kills = []
        vals_kpm = []
        vals_cnt = []
        for pid, hid in zip(pids, hids):
            if pid <= 0 or hid <= 0:
                continue
            key = f"{pid}_{hid}"
            avg_k, cnt = _pair_avg(priors["player_hero_stats"], key, "total_kills", global_player_hero)
            avg_kpm, _ = _pair_avg(priors["player_hero_stats"], key, "kpm", global_player_hero)
            vals_kills.append(avg_k)
            vals_kpm.append(avg_kpm)
            vals_cnt.append(cnt)
        if not vals_kills:
            return 0.0, 0.0, 0.0
        return (
            float(sum(vals_kills) / len(vals_kills)),
            float(sum(vals_kpm) / len(vals_kpm)),
            float(sum(vals_cnt) / len(vals_cnt)) if vals_cnt else 0.0,
        )

    def player_pair_features(pids: List[int]) -> Tuple[float, float, float, float]:
        vals_kills = []
        vals_kpm = []
        vals_dur = []
        vals_cnt = []
        for i in range(len(pids)):
            for j in range(i + 1, len(pids)):
                p1 = pids[i]
                p2 = pids[j]
                if p1 <= 0 or p2 <= 0:
                    continue
                key = f"{min(p1, p2)}_{max(p1, p2)}"
                avg_k, cnt = _pair_avg(player_pair_stats, key, "total_kills", global_player_pair)
                avg_kpm, _ = _pair_avg(player_pair_stats, key, "kpm", global_player_pair)
                avg_dur, _ = _pair_avg(player_pair_stats, key, "duration", global_player_pair)
                vals_kills.append(avg_k)
                vals_kpm.append(avg_kpm)
                vals_dur.append(avg_dur)
                vals_cnt.append(cnt)
        if not vals_kills:
            return 0.0, 0.0, 0.0, 0.0
        return (
            float(sum(vals_kills) / len(vals_kills)),
            float(sum(vals_kpm) / len(vals_kpm)),
            float(sum(vals_dur) / len(vals_dur)),
            float(sum(vals_cnt) / len(vals_cnt)) if vals_cnt else 0.0,
        )

    # Early kills / XP arrays from charts if available
    charts = data.get("charts") or {}

    def series_from(container: Dict[str, Any], keys: List[str]) -> List[Any]:
        for key in keys:
            val = container.get(key)
            if isinstance(val, list) and val:
                return val
        return []

    rad_xp = series_from(charts, ["radiant_xp", "radiantExperience"])
    dire_xp = series_from(charts, ["dire_xp", "direExperience"])
    rad_nw = series_from(charts, ["radiant_networth", "radiantNetworth"])
    dire_nw = series_from(charts, ["dire_networth", "direNetworth"])

    rad_hist = series_from(
        data,
        ["radiantKills", "radiant_kills", "radiant_scores", "radiantScores"],
    )
    dire_hist = series_from(
        data,
        ["direKills", "dire_kills", "dire_scores", "direScores"],
    )
    if not rad_hist:
        rad_hist = series_from(charts, ["radiant_kills", "radiant_scores", "radiantScores"])
    if not dire_hist:
        dire_hist = series_from(charts, ["dire_kills", "dire_scores", "direScores"])
    if not rad_hist and data.get("radiant_score") is not None:
        rad_hist = [data.get("radiant_score")]
    if not dire_hist and data.get("dire_score") is not None:
        dire_hist = [data.get("dire_score")]

    def to_per_min(arr: List[Any]) -> List[float]:
        out = []
        for v in arr:
            try:
                out.append(float(v))
            except Exception:
                out.append(float("nan"))
        if len(out) >= 2 and all(out[i] >= out[i - 1] for i in range(1, len(out))):
            per = [out[0]]
            for i in range(1, len(out)):
                per.append(out[i] - out[i - 1])
            return per
        return out

    xp_list = series_from(
        data,
        [
            "radiantExperienceLeads",
            "radiant_experience_leads",
            "radiant_xp_leads",
            "radiantXpLeads",
        ],
    )
    if not xp_list:
        xp_list = series_from(
            charts,
            [
                "radiantExperienceLeads",
                "radiant_experience_leads",
                "radiant_xp_leads",
                "radiant_xp",
            ],
        )
    if not xp_list:
        if rad_xp and dire_xp:
            xp_list = []
            for rv, dv in zip(rad_xp, dire_xp):
                try:
                    xp_list.append(float(rv) - float(dv))
                except Exception:
                    xp_list.append(float("nan"))
    if not xp_list:
        xp_lead = data.get("xp_lead") or (data.get("live_league_data") or {}).get("xp_lead")
        if xp_lead is not None:
            xp_list = [xp_lead]

    nw_list = series_from(
        data,
        [
            "radiantNetworthLeads",
            "radiant_networth_leads",
            "radiant_nw_leads",
            "radiantNetworthLead",
            "net_worth",
        ],
    )
    if not nw_list:
        nw_list = series_from(
            charts,
            [
                "radiantNetworthLeads",
                "radiant_networth_leads",
                "radiant_nw_leads",
                "radiant_networth",
                "net_worth",
            ],
        )
    if not nw_list:
        if rad_nw and dire_nw:
            nw_list = []
            for rv, dv in zip(rad_nw, dire_nw):
                try:
                    nw_list.append(float(rv) - float(dv))
                except Exception:
                    nw_list.append(float("nan"))
    if not nw_list:
        nw_lead = data.get("radiant_lead") or (data.get("live_league_data") or {}).get("radiant_lead")
        if nw_lead is not None:
            nw_list = [nw_lead]

    rad_vals = to_per_min(rad_hist)[:10]
    dire_vals = to_per_min(dire_hist)[:10]
    if len(rad_vals) < 10:
        rad_vals += [float("nan")] * (10 - len(rad_vals))
    if len(dire_vals) < 10:
        dire_vals += [float("nan")] * (10 - len(dire_vals))

    xp_vals = []
    for i in range(10):
        try:
            xp_vals.append(float(xp_list[i]))
        except Exception:
            xp_vals.append(float("nan"))

    nw_vals = []
    for i in range(10):
        try:
            nw_vals.append(float(nw_list[i]))
        except Exception:
            nw_vals.append(float("nan"))

    total_per_min = [
        (rv + dv) if not math.isnan(rv) and not math.isnan(dv) else float("nan")
        for rv, dv in zip(rad_vals, dire_vals)
    ]
    lead_vals = [
        (rv - dv) if not math.isnan(rv) and not math.isnan(dv) else float("nan")
        for rv, dv in zip(rad_vals, dire_vals)
    ]

    kill_minutes_available = min(10, len(rad_hist), len(dire_hist)) if rad_hist and dire_hist else 0
    xp_minutes_available = min(10, len(xp_list)) if xp_list else 0
    nw_minutes_available = min(10, len(nw_list)) if nw_list else 0
    has_kill_series = 1 if kill_minutes_available > 0 else 0
    has_xp_series = 1 if xp_minutes_available > 0 else 0
    has_nw_series = 1 if nw_minutes_available > 0 else 0
    has_full_early = 1 if (kill_minutes_available >= 10 and xp_minutes_available >= 10 and nw_minutes_available >= 10) else 0

    rad_valid = [v for v in rad_vals if not math.isnan(v)]
    dire_valid = [v for v in dire_vals if not math.isnan(v)]
    total_valid = [v for v in total_per_min if not math.isnan(v)]
    lead_valid = [v for v in lead_vals if not math.isnan(v)]

    rad10 = float(sum(rad_valid)) if rad_valid else float("nan")
    dire10 = float(sum(dire_valid)) if dire_valid else float("nan")
    total10 = float(sum(total_valid)) if total_valid else float("nan")
    kpm10 = (total10 / 10.0) if not math.isnan(total10) else float("nan")
    diff10 = (rad10 - dire10) if not math.isnan(rad10) and not math.isnan(dire10) else float("nan")
    lead10 = float(sum(lead_valid)) if lead_valid else float("nan")
    lead_abs10 = float(sum(abs(v) for v in lead_valid)) if lead_valid else float("nan")
    if lead_valid:
        lead_mean = sum(lead_valid) / len(lead_valid)
        lead_std = math.sqrt(sum((v - lead_mean) ** 2 for v in lead_valid) / len(lead_valid))
    else:
        lead_std = float("nan")
    accel = float("nan")
    if kill_minutes_available >= 10 and not any(math.isnan(v) for v in total_per_min):
        accel = sum(total_per_min[-3:]) - sum(total_per_min[:7])

    kill_std = float("nan")
    kill_zero = float("nan")
    kill_max = float("nan")
    kill_slope = float("nan")
    if total_valid:
        mean_val = sum(total_valid) / len(total_valid)
        var = sum((v - mean_val) ** 2 for v in total_valid) / len(total_valid)
        kill_std = math.sqrt(var)
        kill_zero = sum(1 for v in total_valid if v == 0)
        kill_max = max(total_valid)
        if len(total_valid) >= 2:
            kill_slope = _linear_slope(total_valid)

    first5 = float("nan")
    last5 = float("nan")
    if kill_minutes_available >= 10 and not any(math.isnan(v) for v in total_per_min):
        first5 = sum(total_per_min[:5])
        last5 = sum(total_per_min[5:10])

    xp10 = float("nan")
    if xp_list:
        try:
            xp10 = float(xp_list[9]) if len(xp_list) >= 10 else float(xp_list[-1])
        except Exception:
            xp10 = float("nan")
    xp5 = float("nan")
    if len(xp_list) >= 5:
        try:
            xp5 = float(xp_list[4])
        except Exception:
            xp5 = float("nan")
    xp_valid = [v for v in xp_vals if not math.isnan(v)]
    xp_mean = float(np.nanmean(xp_vals)) if xp_valid else float("nan")
    xp_std = float(np.nanstd(xp_vals)) if xp_valid else float("nan")
    xp_slope = _linear_slope(xp_valid) if len(xp_valid) >= 2 else float("nan")

    def safe_mean(vals: List[float]) -> float:
        if not vals or any(math.isnan(v) for v in vals):
            return float("nan")
        return float(sum(vals) / len(vals))

    xp_first5 = safe_mean(xp_vals[:5])
    xp_last5 = safe_mean(xp_vals[5:10])
    xp_change_5_10 = (
        (xp_last5 - xp_first5) if not math.isnan(xp_first5) and not math.isnan(xp_last5) else float("nan")
    )
    def sign_changes(vals: List[float]) -> float:
        signs = []
        for v in vals:
            if v > 0:
                signs.append(1)
            elif v < 0:
                signs.append(-1)
        if len(signs) < 2:
            return 0.0
        return float(sum(1 for i in range(1, len(signs)) if signs[i] != signs[i - 1]))

    xp_abs_mean = float(np.nanmean(np.abs(xp_vals))) if xp_valid else float("nan")
    xp_abs_max = float(np.nanmax(np.abs(xp_vals))) if xp_valid else float("nan")
    xp_pos_frac = float(sum(1 for v in xp_valid if v > 0) / len(xp_valid)) if xp_valid else float("nan")
    xp_neg_frac = float(sum(1 for v in xp_valid if v < 0) / len(xp_valid)) if xp_valid else float("nan")
    xp_sign_changes = sign_changes(xp_valid) if xp_valid else float("nan")

    nw10 = float("nan")
    if nw_list:
        try:
            nw10 = float(nw_list[9]) if len(nw_list) >= 10 else float(nw_list[-1])
        except Exception:
            nw10 = float("nan")
    nw5 = float("nan")
    if len(nw_list) >= 5:
        try:
            nw5 = float(nw_list[4])
        except Exception:
            nw5 = float("nan")
    nw_valid = [v for v in nw_vals if not math.isnan(v)]
    nw_mean = float(np.nanmean(nw_vals)) if nw_valid else float("nan")
    nw_std = float(np.nanstd(nw_vals)) if nw_valid else float("nan")
    nw_slope = _linear_slope(nw_valid) if len(nw_valid) >= 2 else float("nan")
    nw_first5 = safe_mean(nw_vals[:5])
    nw_last5 = safe_mean(nw_vals[5:10])
    nw_change_5_10 = (
        (nw_last5 - nw_first5) if not math.isnan(nw_first5) and not math.isnan(nw_last5) else float("nan")
    )
    nw_abs_mean = float(np.nanmean(np.abs(nw_vals))) if nw_valid else float("nan")
    nw_abs_max = float(np.nanmax(np.abs(nw_vals))) if nw_valid else float("nan")
    nw_pos_frac = float(sum(1 for v in nw_valid if v > 0) / len(nw_valid)) if nw_valid else float("nan")
    nw_neg_frac = float(sum(1 for v in nw_valid if v < 0) / len(nw_valid)) if nw_valid else float("nan")
    nw_sign_changes = sign_changes(nw_valid) if nw_valid else float("nan")

    nw_per_kill10 = float("nan")
    if not math.isnan(nw10) and not math.isnan(total10) and total10 > 0:
        nw_per_kill10 = nw10 / total10
    xp_per_kill10 = float("nan")
    if not math.isnan(xp10) and not math.isnan(total10) and total10 > 0:
        xp_per_kill10 = xp10 / total10

    def series_value(series: List[Any]) -> float:
        if not series:
            return float("nan")
        val = series[9] if len(series) >= 10 else series[-1]
        try:
            return float(val)
        except Exception:
            return float("nan")

    rad_xp10_total = series_value(rad_xp)
    dire_xp10_total = series_value(dire_xp)
    total_xp10 = (
        rad_xp10_total + dire_xp10_total
        if not math.isnan(rad_xp10_total) and not math.isnan(dire_xp10_total)
        else float("nan")
    )
    rad_nw10_total = series_value(rad_nw)
    dire_nw10_total = series_value(dire_nw)
    total_nw10 = (
        rad_nw10_total + dire_nw10_total
        if not math.isnan(rad_nw10_total) and not math.isnan(dire_nw10_total)
        else float("nan")
    )

    fb_time = _coerce_int(data.get("first_blood_time") or data.get("firstBloodTime"))
    if fb_time and fb_time <= 600:
        fb_happened = 1
        fb_time_10 = fb_time
    else:
        fb_happened = 0
        fb_time_10 = 600

    rad_pub_kills = pub_vals(rad_ids, "kills_z")
    dire_pub_kills = pub_vals(dire_ids, "kills_z")
    rad_pub_deaths = pub_vals(rad_ids, "deaths_z")
    dire_pub_deaths = pub_vals(dire_ids, "deaths_z")
    rad_pub_assists = pub_vals(rad_ids, "assists_z")
    dire_pub_assists = pub_vals(dire_ids, "assists_z")
    rad_pub_kpm = pub_vals(rad_ids, "kpm_z")
    dire_pub_kpm = pub_vals(dire_ids, "kpm_z")
    rad_pub_dpm = pub_vals(rad_ids, "dpm_z")
    dire_pub_dpm = pub_vals(dire_ids, "dpm_z")
    rad_pub_apm = pub_vals(rad_ids, "apm_z")
    dire_pub_apm = pub_vals(dire_ids, "apm_z")
    rad_pub_kapm = pub_vals(rad_ids, "kapm_z")
    dire_pub_kapm = pub_vals(dire_ids, "kapm_z")
    rad_pub_kda = pub_vals(rad_ids, "kda_z")
    dire_pub_kda = pub_vals(dire_ids, "kda_z")
    rad_pub_dur = pub_vals(rad_ids, "dur_z")
    dire_pub_dur = pub_vals(dire_ids, "dur_z")

    rad_hero_avg_kills = sum(hero_avg(h, "total_kills") for h in rad_ids)
    dire_hero_avg_kills = sum(hero_avg(h, "total_kills") for h in dire_ids)
    rad_hero_avg_kpm = sum(hero_avg(h, "kpm") for h in rad_ids)
    dire_hero_avg_kpm = sum(hero_avg(h, "kpm") for h in dire_ids)
    rad_hero_avg_dur = sum(hero_avg(h, "duration") for h in rad_ids)
    dire_hero_avg_dur = sum(hero_avg(h, "duration") for h in dire_ids)
    rad_hero_recent_kills = sum(recent_hero_stats(h)[0] for h in rad_ids)
    dire_hero_recent_kills = sum(recent_hero_stats(h)[0] for h in dire_ids)
    rad_hero_recent_kpm = sum(recent_hero_stats(h)[1] for h in rad_ids)
    dire_hero_recent_kpm = sum(recent_hero_stats(h)[1] for h in dire_ids)
    rad_hero_recent_dur = sum(recent_hero_stats(h)[2] for h in rad_ids)
    dire_hero_recent_dur = sum(recent_hero_stats(h)[2] for h in dire_ids)
    rad_hero_recent_count = float(np.mean([recent_hero_stats(h)[3] for h in rad_ids])) if rad_ids else 0.0
    dire_hero_recent_count = float(np.mean([recent_hero_stats(h)[3] for h in dire_ids])) if dire_ids else 0.0
    rad_hero_over50 = sum(hero_avg(h, "over50") for h in rad_ids)
    dire_hero_over50 = sum(hero_avg(h, "over50") for h in dire_ids)
    rad_hero_under40 = sum(hero_avg(h, "under40") for h in rad_ids)
    dire_hero_under40 = sum(hero_avg(h, "under40") for h in dire_ids)

    r_team_kills = team_avg(radiant_team_id, "kills_for")
    d_team_kills = team_avg(dire_team_id, "kills_for")
    r_team_against = team_avg(radiant_team_id, "kills_against")
    d_team_against = team_avg(dire_team_id, "kills_against")
    r_team_total = team_avg(radiant_team_id, "total_kills")
    d_team_total = team_avg(dire_team_id, "total_kills")
    r_team_kpm = team_avg(radiant_team_id, "kpm")
    d_team_kpm = team_avg(dire_team_id, "kpm")
    r_team_dur = team_avg(radiant_team_id, "duration")
    d_team_dur = team_avg(dire_team_id, "duration")
    r_team_over50 = team_avg(radiant_team_id, "over50")
    d_team_over50 = team_avg(dire_team_id, "over50")
    r_team_under40 = team_avg(radiant_team_id, "under40")
    d_team_under40 = team_avg(dire_team_id, "under40")
    r_team_hist = priors["team_stats"].get(str(radiant_team_id), {}).get("count", 0)
    d_team_hist = priors["team_stats"].get(str(dire_team_id), {}).get("count", 0)

    def _match_roster_group_id(team_id: int, roster_ids: List[int], min_shared: int = 3) -> int:
        if team_id <= 0:
            return -1
        roster = {int(pid) for pid in roster_ids if int(pid) > 0}
        if len(roster) < min_shared:
            return -1
        groups = team_rosters.get(str(team_id), [])
        best_group = -1
        best_shared = 0
        for group in groups:
            players = set(group.get("players") or [])
            shared = len(roster & players)
            if shared > best_shared:
                best_shared = shared
                best_group = int(group.get("group_id", -1))
        return best_group if best_shared >= min_shared else -1

    def roster_group_avg(team_id: int, group_id: int, stat_key: str) -> float:
        if team_id <= 0 or group_id < 0:
            return team_avg(team_id, stat_key)
        key = f"{team_id}_{group_id}"
        st = roster_group_stats.get(key)
        if st and st.get("count", 0) > 0:
            return st.get(stat_key, 0.0) / st["count"]
        return team_avg(team_id, stat_key)

    r_group_id = _match_roster_group_id(radiant_team_id, rad_pids)
    d_group_id = _match_roster_group_id(dire_team_id, dire_pids)

    r_roster_kills = roster_group_avg(radiant_team_id, r_group_id, "kills_for")
    d_roster_kills = roster_group_avg(dire_team_id, d_group_id, "kills_for")
    r_roster_against = roster_group_avg(radiant_team_id, r_group_id, "kills_against")
    d_roster_against = roster_group_avg(dire_team_id, d_group_id, "kills_against")
    r_roster_total = roster_group_avg(radiant_team_id, r_group_id, "total_kills")
    d_roster_total = roster_group_avg(dire_team_id, d_group_id, "total_kills")
    r_roster_kpm = roster_group_avg(radiant_team_id, r_group_id, "kpm")
    d_roster_kpm = roster_group_avg(dire_team_id, d_group_id, "kpm")
    r_roster_dur = roster_group_avg(radiant_team_id, r_group_id, "duration")
    d_roster_dur = roster_group_avg(dire_team_id, d_group_id, "duration")
    r_roster_over50 = roster_group_avg(radiant_team_id, r_group_id, "over50")
    d_roster_over50 = roster_group_avg(dire_team_id, d_group_id, "over50")
    r_roster_under40 = roster_group_avg(radiant_team_id, r_group_id, "under40")
    d_roster_under40 = roster_group_avg(dire_team_id, d_group_id, "under40")
    r_roster_hist = roster_group_stats.get(f"{radiant_team_id}_{r_group_id}", {}).get("count", 0) if r_group_id >= 0 else 0
    d_roster_hist = roster_group_stats.get(f"{dire_team_id}_{d_group_id}", {}).get("count", 0) if d_group_id >= 0 else 0

    def team_kill_share(team_id: int) -> float:
        st = priors["team_stats"].get(str(team_id))
        if st and st.get("total_kills", 0) > 0:
            return st.get("kills_for", 0.0) / st["total_kills"]
        if global_team.get("total_kills", 0) > 0:
            return global_team.get("kills_for", 0.0) / global_team["total_kills"]
        return 0.0

    def team_kill_ratio(team_id: int) -> float:
        st = priors["team_stats"].get(str(team_id))
        if st and st.get("kills_against", 0) > 0:
            return st.get("kills_for", 0.0) / max(1.0, st["kills_against"])
        if global_team.get("kills_against", 0) > 0:
            return global_team.get("kills_for", 0.0) / max(1.0, global_team["kills_against"])
        return 1.0

    r_team_kill_share = team_kill_share(radiant_team_id)
    d_team_kill_share = team_kill_share(dire_team_id)
    r_team_kill_ratio = team_kill_ratio(radiant_team_id)
    d_team_kill_ratio = team_kill_ratio(dire_team_id)
    team_elo = priors.get("team_elo", {})
    team_elo_games = priors.get("team_elo_games", {})
    r_team_elo = float(team_elo.get(str(radiant_team_id), 1500.0))
    d_team_elo = float(team_elo.get(str(dire_team_id), 1500.0))
    r_team_elo_games = int(team_elo_games.get(str(radiant_team_id), 0) or 0)
    d_team_elo_games = int(team_elo_games.get(str(dire_team_id), 0) or 0)
    team_elo_diff = r_team_elo - d_team_elo
    team_elo_win_prob = 1.0 / (1.0 + 10 ** ((d_team_elo - r_team_elo) / 400.0))

    (
        r_team_recent_total,
        r_team_recent_kpm,
        r_team_recent_dur,
        r_team_recent_over50,
        r_team_recent_under40,
        r_team_recent_std,
        r_team_recent_count,
    ) = recent_team_stats(radiant_team_id)
    (
        d_team_recent_total,
        d_team_recent_kpm,
        d_team_recent_dur,
        d_team_recent_over50,
        d_team_recent_under40,
        d_team_recent_std,
        d_team_recent_count,
    ) = recent_team_stats(dire_team_id)

    rad_player_stats = []
    dire_player_stats = []
    rad_player_recent_stats = []
    dire_player_recent_stats = []
    rad_player_aggr = []
    dire_player_aggr = []
    rad_player_feed = []
    dire_player_feed = []
    rad_player_unique = []
    dire_player_unique = []
    rad_player_hero_share = []
    dire_player_hero_share = []
    for pid, hero_id in zip(rad_pids, rad_ids):
        rad_player_stats.append(
            (
                player_avg(pid, "kills"),
                player_avg(pid, "deaths"),
                player_avg(pid, "assists"),
                player_kpm(pid),
                player_avg(pid, "gpm"),
                player_avg(pid, "xpm"),
                player_avg(pid, "hero_damage"),
                player_avg(pid, "tower_damage"),
                player_avg(pid, "imp"),
                priors["player_stats"].get(str(pid), {}).get("count", 0),
                player_avg(pid, "lhpm"),
                player_avg(pid, "denypm"),
                player_avg(pid, "healpm"),
                player_avg(pid, "invispm"),
                player_avg(pid, "level"),
            )
        )
        rad_player_recent_stats.append(recent_player_stats(pid))
        rad_player_aggr.append(player_aggression(pid))
        rad_player_feed.append(player_feed_pm(pid))
        rad_player_unique.append(player_unique_count(pid))
        rad_player_hero_share.append(player_hero_share(pid, hero_id))
    for pid, hero_id in zip(dire_pids, dire_ids):
        dire_player_stats.append(
            (
                player_avg(pid, "kills"),
                player_avg(pid, "deaths"),
                player_avg(pid, "assists"),
                player_kpm(pid),
                player_avg(pid, "gpm"),
                player_avg(pid, "xpm"),
                player_avg(pid, "hero_damage"),
                player_avg(pid, "tower_damage"),
                player_avg(pid, "imp"),
                priors["player_stats"].get(str(pid), {}).get("count", 0),
                player_avg(pid, "lhpm"),
                player_avg(pid, "denypm"),
                player_avg(pid, "healpm"),
                player_avg(pid, "invispm"),
                player_avg(pid, "level"),
            )
        )
        dire_player_recent_stats.append(recent_player_stats(pid))
        dire_player_aggr.append(player_aggression(pid))
        dire_player_feed.append(player_feed_pm(pid))
        dire_player_unique.append(player_unique_count(pid))
        dire_player_hero_share.append(player_hero_share(pid, hero_id))

    def stats_mean(stats: List[Tuple[float, ...]], idx: int) -> float:
        vals = [s[idx] for s in stats]
        return float(sum(vals) / len(vals)) if vals else 0.0

    def stats_std(stats: List[Tuple[float, ...]], idx: int) -> float:
        vals = [s[idx] for s in stats]
        if len(vals) < 2:
            return 0.0
        mean_val = sum(vals) / len(vals)
        var = sum((v - mean_val) ** 2 for v in vals) / len(vals)
        return float(math.sqrt(var))

    def stats_kda(stats: List[Tuple[float, ...]]) -> float:
        if not stats:
            return 0.0
        k = stats_mean(stats, 0)
        d = stats_mean(stats, 1)
        a = stats_mean(stats, 2)
        return float((k + a) / max(1.0, d))

    def list_mean(vals: List[float]) -> float:
        return float(sum(vals) / len(vals)) if vals else 0.0

    def list_std(vals: List[float]) -> float:
        if len(vals) < 2:
            return 0.0
        mean_val = sum(vals) / len(vals)
        var = sum((v - mean_val) ** 2 for v in vals) / len(vals)
        return float(math.sqrt(var))

    def list_min(vals: List[float]) -> float:
        return float(min(vals)) if vals else 0.0

    def list_max(vals: List[float]) -> float:
        return float(max(vals)) if vals else 0.0

    def early_avg(stats: Optional[Dict[str, float]], key: str, global_stats: Dict[str, float]) -> float:
        if stats and stats.get("count", 0) > 0:
            return safe_float(stats.get(key), 0.0) / stats["count"]
        if global_stats.get("count", 0) > 0:
            return safe_float(global_stats.get(key), 0.0) / global_stats["count"]
        return 0.0

    def team_early_avg(tid: int, key: str) -> float:
        if tid <= 0:
            return early_avg(None, key, global_team_early)
        return early_avg(team_early_stats.get(str(tid)), key, global_team_early)

    def hero_early_avg(hid: int, key: str) -> float:
        if hid <= 0:
            return early_avg(None, key, global_hero_early)
        return early_avg(hero_early_stats.get(str(hid)), key, global_hero_early)

    def player_early_avg(pid: int, key: str) -> float:
        if pid <= 0:
            return early_avg(None, key, global_player_early)
        return early_avg(player_early_stats.get(str(pid)), key, global_player_early)

    def team_vs_early_avg(t1: int, t2: int, key: str) -> float:
        if t1 <= 0 or t2 <= 0:
            return early_avg(None, key, global_team_vs_early)
        pair = f"{min(t1, t2)}_{max(t1, t2)}"
        return early_avg(team_vs_early_stats.get(pair), key, global_team_vs_early)

    r_pair_kills, r_pair_kpm, r_pair_dur, r_pair_cnt = team_pair_features(rad_ids)
    d_pair_kills, d_pair_kpm, d_pair_dur, d_pair_cnt = team_pair_features(dire_ids)
    hero_vs_kills, hero_vs_kpm, hero_vs_cnt = hero_vs_features(rad_ids, dire_ids)
    r_player_hero_kills, r_player_hero_kpm, r_player_hero_cnt = player_hero_features(rad_pids, rad_ids)
    d_player_hero_kills, d_player_hero_kpm, d_player_hero_cnt = player_hero_features(dire_pids, dire_ids)
    r_player_pair_kills, r_player_pair_kpm, r_player_pair_dur, r_player_pair_cnt = player_pair_features(rad_pids)
    d_player_pair_kills, d_player_pair_kpm, d_player_pair_dur, d_player_pair_cnt = player_pair_features(dire_pids)

    team_vs_avg_kills = 0.0
    team_vs_avg_kpm = 0.0
    team_vs_hist = 0.0
    team_vs_over50_rate = float("nan")
    team_vs_under40_rate = float("nan")
    if radiant_team_id > 0 and dire_team_id > 0:
        team_key = f"{min(radiant_team_id, dire_team_id)}_{max(radiant_team_id, dire_team_id)}"
        team_vs_avg_kills, team_vs_hist = _pair_avg(priors["team_vs_stats"], team_key, "total_kills", global_team_vs)
        team_vs_avg_kpm, _ = _pair_avg(priors["team_vs_stats"], team_key, "kpm", global_team_vs)
        tv = priors["team_vs_stats"].get(team_key)
        if tv and tv.get("count", 0) > 0:
            team_vs_over50_rate = tv.get("over50", 0.0) / tv["count"]
            team_vs_under40_rate = tv.get("under40", 0.0) / tv["count"]
        elif global_team_vs.get("count", 0) > 0:
            team_vs_over50_rate = global_team_vs.get("over50", 0.0) / global_team_vs["count"]
            team_vs_under40_rate = global_team_vs.get("under40", 0.0) / global_team_vs["count"]

    league_avg_kills = _avg_stat(priors["league_stats"], str(league_id), "total_kills", global_league)
    league_avg_kpm = _avg_stat(priors["league_stats"], str(league_id), "kpm", global_league)
    league_avg_dur = _avg_stat(priors["league_stats"], str(league_id), "duration", global_league)
    league_over50 = _avg_stat(priors["league_stats"], str(league_id), "over50", global_league)
    league_under40 = _avg_stat(priors["league_stats"], str(league_id), "under40", global_league)
    league_hist = priors["league_stats"].get(str(league_id), {}).get("count", 0)

    version_id = _coerce_int(data.get("gameVersionId"))
    version_avg_kills = _avg_stat(priors["version_stats"], str(version_id), "total_kills", global_version)
    version_avg_kpm = _avg_stat(priors["version_stats"], str(version_id), "kpm", global_version)
    version_avg_dur = _avg_stat(priors["version_stats"], str(version_id), "duration", global_version)
    version_over50 = _avg_stat(priors["version_stats"], str(version_id), "over50", global_version)
    version_under40 = _avg_stat(priors["version_stats"], str(version_id), "under40", global_version)
    version_hist = priors["version_stats"].get(str(version_id), {}).get("count", 0)

    team_recent_kpm_sum = float("nan")
    if not math.isnan(r_team_recent_kpm) and not math.isnan(d_team_recent_kpm):
        team_recent_kpm_sum = r_team_recent_kpm + d_team_recent_kpm
    hero_recent_kpm_sum = rad_hero_recent_kpm + dire_hero_recent_kpm

    expected_total10_team = (
        team_recent_kpm_sum * 10.0 if not math.isnan(team_recent_kpm_sum) else float("nan")
    )
    expected_total10_hero = (
        hero_recent_kpm_sum * 10.0 if not math.isnan(hero_recent_kpm_sum) else float("nan")
    )

    early_kpm_diff_team = (
        kpm10 - team_recent_kpm_sum
        if not math.isnan(kpm10) and not math.isnan(team_recent_kpm_sum)
        else float("nan")
    )
    early_kpm_diff_hero = (
        kpm10 - hero_recent_kpm_sum
        if not math.isnan(kpm10) and not math.isnan(hero_recent_kpm_sum)
        else float("nan")
    )
    early_total10_ratio_team = (
        (total10 / expected_total10_team)
        if not math.isnan(total10)
        and not math.isnan(expected_total10_team)
        and expected_total10_team > 0
        else float("nan")
    )
    early_total10_ratio_hero = (
        (total10 / expected_total10_hero)
        if not math.isnan(total10)
        and not math.isnan(expected_total10_hero)
        and expected_total10_hero > 0
        else float("nan")
    )
    early_total10_delta_team = (
        total10 - expected_total10_team
        if not math.isnan(total10) and not math.isnan(expected_total10_team)
        else float("nan")
    )
    early_total10_delta_hero = (
        total10 - expected_total10_hero
        if not math.isnan(total10) and not math.isnan(expected_total10_hero)
        else float("nan")
    )

    r_team_early_for10 = team_early_avg(radiant_team_id, "for10")
    d_team_early_for10 = team_early_avg(dire_team_id, "for10")
    r_team_early_against10 = team_early_avg(radiant_team_id, "against10")
    d_team_early_against10 = team_early_avg(dire_team_id, "against10")
    r_team_early_total10 = team_early_avg(radiant_team_id, "total10")
    d_team_early_total10 = team_early_avg(dire_team_id, "total10")
    r_team_early_share10 = team_early_avg(radiant_team_id, "share10")
    d_team_early_share10 = team_early_avg(dire_team_id, "share10")
    r_team_early_count = team_early_stats.get(str(radiant_team_id), {}).get("count", 0)
    d_team_early_count = team_early_stats.get(str(dire_team_id), {}).get("count", 0)

    team_early_total10_mean = float("nan")
    if not math.isnan(r_team_early_total10) and not math.isnan(d_team_early_total10):
        team_early_total10_mean = (r_team_early_total10 + d_team_early_total10) / 2.0
    early_total10_delta_team_early = (
        total10 - team_early_total10_mean
        if not math.isnan(total10) and not math.isnan(team_early_total10_mean)
        else float("nan")
    )

    rad_hero_early_total = [hero_early_avg(h, "total10") for h in rad_ids]
    dire_hero_early_total = [hero_early_avg(h, "total10") for h in dire_ids]
    rad_hero_early_for = [hero_early_avg(h, "for10") for h in rad_ids]
    dire_hero_early_for = [hero_early_avg(h, "for10") for h in dire_ids]
    rad_hero_early_share = [hero_early_avg(h, "share10") for h in rad_ids]
    dire_hero_early_share = [hero_early_avg(h, "share10") for h in dire_ids]
    rad_hero_early_count = [
        hero_early_stats.get(str(h), {}).get("count", 0) if h > 0 else 0 for h in rad_ids
    ]
    dire_hero_early_count = [
        hero_early_stats.get(str(h), {}).get("count", 0) if h > 0 else 0 for h in dire_ids
    ]

    r_player_early_total = [player_early_avg(p, "total10") for p in rad_pids]
    d_player_early_total = [player_early_avg(p, "total10") for p in dire_pids]
    r_player_early_for = [player_early_avg(p, "for10") for p in rad_pids]
    d_player_early_for = [player_early_avg(p, "for10") for p in dire_pids]
    r_player_early_share = [player_early_avg(p, "share10") for p in rad_pids]
    d_player_early_share = [player_early_avg(p, "share10") for p in dire_pids]
    r_player_early_count = [
        player_early_stats.get(str(p), {}).get("count", 0) if p > 0 else 0 for p in rad_pids
    ]
    d_player_early_count = [
        player_early_stats.get(str(p), {}).get("count", 0) if p > 0 else 0 for p in dire_pids
    ]

    team_vs_early_total10 = team_vs_early_avg(radiant_team_id, dire_team_id, "total10")
    team_vs_early_kpm10 = team_vs_early_avg(radiant_team_id, dire_team_id, "kpm10")
    team_vs_early_abs_diff10 = team_vs_early_avg(radiant_team_id, dire_team_id, "abs_diff10")
    team_vs_early_count = (
        team_vs_early_stats.get(
            f"{min(radiant_team_id, dire_team_id)}_{max(radiant_team_id, dire_team_id)}", {}
        ).get("count", 0)
        if radiant_team_id > 0 and dire_team_id > 0
        else 0
    )

    now = datetime.utcnow()

    row: Dict[str, Any] = {c: float("nan") for c in (KILLS_FEATURE_COLS or [])}
    row.update(
        {
            "patch_id": patch_id,
            "patch_major_label": patch_major_label,
            "match_tier": match_tier,
            "match_tier_known": match_tier_known,
            "kill_minutes_available": kill_minutes_available,
            "xp_minutes_available": xp_minutes_available,
            "nw_minutes_available": nw_minutes_available,
            "has_kill_series": has_kill_series,
            "has_xp_series": has_xp_series,
            "has_nw_series": has_nw_series,
            "has_full_early": has_full_early,
            "rad10": rad10,
            "dire10": dire10,
            "total10": total10,
            "kpm10": kpm10,
            "diff10": diff10,
            "lead10": lead10,
            "lead_abs10": lead_abs10,
            "lead_std10": lead_std,
            "accel10": accel,
            "kill_std10": kill_std,
            "kill_zero10": kill_zero,
            "kill_max10": kill_max,
            "kill_slope10": kill_slope,
            "first5_kills": first5,
            "last5_kills": last5,
            "first5_kpm": (first5 / 5.0) if not math.isnan(first5) else float("nan"),
            "last5_kpm": (last5 / 5.0) if not math.isnan(last5) else float("nan"),
            "kpm_change_5_10": ((last5 - first5) / 5.0)
            if not math.isnan(first5) and not math.isnan(last5)
            else float("nan"),
            "first5_share": (first5 / total10)
            if not math.isnan(first5) and not math.isnan(total10) and total10 > 0
            else float("nan"),
            "last5_share": (last5 / total10)
            if not math.isnan(last5) and not math.isnan(total10) and total10 > 0
            else float("nan"),
            "frontload_kills": (first5 - last5)
            if not math.isnan(first5) and not math.isnan(last5)
            else float("nan"),
            "lead_ratio10": (diff10 / total10)
            if not math.isnan(diff10) and not math.isnan(total10) and total10 > 0
            else float("nan"),
            "lead_abs_ratio10": (lead_abs10 / total10)
            if not math.isnan(lead_abs10) and not math.isnan(total10) and total10 > 0
            else float("nan"),
            "xp10": xp10,
            "xp10_abs": abs(xp10),
            "xp5": xp5,
            "xp_mean10": xp_mean,
            "xp_std10": xp_std,
            "xp_slope10": xp_slope,
            "xp_first5": xp_first5,
            "xp_last5": xp_last5,
            "xp_change_5_10": xp_change_5_10,
            "xp_abs_mean10": xp_abs_mean,
            "xp_abs_max10": xp_abs_max,
            "xp_pos_frac10": xp_pos_frac,
            "xp_neg_frac10": xp_neg_frac,
            "xp_sign_changes10": xp_sign_changes,
            "nw10": nw10,
            "nw10_abs": abs(nw10),
            "nw5": nw5,
            "nw_mean10": nw_mean,
            "nw_std10": nw_std,
            "nw_slope10": nw_slope,
            "nw_first5": nw_first5,
            "nw_last5": nw_last5,
            "nw_change_5_10": nw_change_5_10,
            "nw_abs_mean10": nw_abs_mean,
            "nw_abs_max10": nw_abs_max,
            "nw_pos_frac10": nw_pos_frac,
            "nw_neg_frac10": nw_neg_frac,
            "nw_sign_changes10": nw_sign_changes,
            "nw_per_kill10": nw_per_kill10,
            "xp_per_kill10": xp_per_kill10,
            "rad_nw10_total": rad_nw10_total,
            "dire_nw10_total": dire_nw10_total,
            "total_nw10": total_nw10,
            "rad_xp10_total": rad_xp10_total,
            "dire_xp10_total": dire_xp10_total,
            "total_xp10": total_xp10,
            "fb_time_10": fb_time_10,
            "fb_happened_10": fb_happened,
            "rad_pub_kills_sum": float(sum(rad_pub_kills)),
            "dire_pub_kills_sum": float(sum(dire_pub_kills)),
            "pub_kills_diff": float(sum(rad_pub_kills) - sum(dire_pub_kills)),
            "rad_pub_deaths_sum": float(sum(rad_pub_deaths)),
            "dire_pub_deaths_sum": float(sum(dire_pub_deaths)),
            "pub_deaths_diff": float(sum(rad_pub_deaths) - sum(dire_pub_deaths)),
            "rad_pub_assists_sum": float(sum(rad_pub_assists)),
            "dire_pub_assists_sum": float(sum(dire_pub_assists)),
            "pub_assists_diff": float(sum(rad_pub_assists) - sum(dire_pub_assists)),
            "rad_pub_kpm_sum": float(sum(rad_pub_kpm)),
            "dire_pub_kpm_sum": float(sum(dire_pub_kpm)),
            "pub_kpm_diff": float(sum(rad_pub_kpm) - sum(dire_pub_kpm)),
            "rad_pub_dpm_sum": float(sum(rad_pub_dpm)),
            "dire_pub_dpm_sum": float(sum(dire_pub_dpm)),
            "pub_dpm_diff": float(sum(rad_pub_dpm) - sum(dire_pub_dpm)),
            "rad_pub_apm_sum": float(sum(rad_pub_apm)),
            "dire_pub_apm_sum": float(sum(dire_pub_apm)),
            "pub_apm_diff": float(sum(rad_pub_apm) - sum(dire_pub_apm)),
            "rad_pub_kapm_sum": float(sum(rad_pub_kapm)),
            "dire_pub_kapm_sum": float(sum(dire_pub_kapm)),
            "pub_kapm_diff": float(sum(rad_pub_kapm) - sum(dire_pub_kapm)),
            "rad_pub_kda_sum": float(sum(rad_pub_kda)),
            "dire_pub_kda_sum": float(sum(dire_pub_kda)),
            "pub_kda_diff": float(sum(rad_pub_kda) - sum(dire_pub_kda)),
            "rad_pub_dur_sum": float(sum(rad_pub_dur)),
            "dire_pub_dur_sum": float(sum(dire_pub_dur)),
            "pub_dur_diff": float(sum(rad_pub_dur) - sum(dire_pub_dur)),
            "rad_pub_kills_max": list_max(rad_pub_kills),
            "dire_pub_kills_max": list_max(dire_pub_kills),
            "pub_kills_max_diff": (
                list_max(rad_pub_kills) - list_max(dire_pub_kills)
                if rad_pub_kills and dire_pub_kills
                else 0.0
            ),
            "rad_pub_kills_min": list_min(rad_pub_kills),
            "dire_pub_kills_min": list_min(dire_pub_kills),
            "pub_kills_min_diff": (
                list_min(rad_pub_kills) - list_min(dire_pub_kills)
                if rad_pub_kills and dire_pub_kills
                else 0.0
            ),
            "rad_pub_kpm_max": list_max(rad_pub_kpm),
            "dire_pub_kpm_max": list_max(dire_pub_kpm),
            "pub_kpm_max_diff": (
                list_max(rad_pub_kpm) - list_max(dire_pub_kpm)
                if rad_pub_kpm and dire_pub_kpm
                else 0.0
            ),
            "rad_pub_kpm_min": list_min(rad_pub_kpm),
            "dire_pub_kpm_min": list_min(dire_pub_kpm),
            "pub_kpm_min_diff": (
                list_min(rad_pub_kpm) - list_min(dire_pub_kpm)
                if rad_pub_kpm and dire_pub_kpm
                else 0.0
            ),
            "rad_hero_avg_kills": rad_hero_avg_kills,
            "dire_hero_avg_kills": dire_hero_avg_kills,
            "hero_avg_kills_diff": rad_hero_avg_kills - dire_hero_avg_kills,
            "rad_hero_avg_kpm": rad_hero_avg_kpm,
            "dire_hero_avg_kpm": dire_hero_avg_kpm,
            "hero_avg_kpm_diff": rad_hero_avg_kpm - dire_hero_avg_kpm,
            "rad_hero_avg_dur": rad_hero_avg_dur,
            "dire_hero_avg_dur": dire_hero_avg_dur,
            "hero_avg_dur_diff": rad_hero_avg_dur - dire_hero_avg_dur,
            "rad_hero_recent_kills": rad_hero_recent_kills,
            "dire_hero_recent_kills": dire_hero_recent_kills,
            "hero_recent_kills_diff": rad_hero_recent_kills - dire_hero_recent_kills,
            "rad_hero_recent_kpm": rad_hero_recent_kpm,
            "dire_hero_recent_kpm": dire_hero_recent_kpm,
            "hero_recent_kpm_diff": rad_hero_recent_kpm - dire_hero_recent_kpm,
            "rad_hero_recent_dur": rad_hero_recent_dur,
            "dire_hero_recent_dur": dire_hero_recent_dur,
            "hero_recent_dur_diff": rad_hero_recent_dur - dire_hero_recent_dur,
            "rad_hero_recent_count": rad_hero_recent_count,
            "dire_hero_recent_count": dire_hero_recent_count,
            "rad_hero_over50": rad_hero_over50,
            "dire_hero_over50": dire_hero_over50,
            "hero_over50_diff": rad_hero_over50 - dire_hero_over50,
            "rad_hero_under40": rad_hero_under40,
            "dire_hero_under40": dire_hero_under40,
            "hero_under40_diff": rad_hero_under40 - dire_hero_under40,
            "r_team_avg_kills": r_team_kills,
            "d_team_avg_kills": d_team_kills,
            "team_kills_diff": r_team_kills - d_team_kills,
            "r_team_avg_against": r_team_against,
            "d_team_avg_against": d_team_against,
            "team_against_diff": r_team_against - d_team_against,
            "r_team_avg_total": r_team_total,
            "d_team_avg_total": d_team_total,
            "team_total_diff": r_team_total - d_team_total,
            "r_team_avg_kpm": r_team_kpm,
            "d_team_avg_kpm": d_team_kpm,
            "team_kpm_diff": r_team_kpm - d_team_kpm,
            "r_team_avg_dur": r_team_dur,
            "d_team_avg_dur": d_team_dur,
            "team_dur_diff": r_team_dur - d_team_dur,
            "r_team_over50_rate": r_team_over50,
            "d_team_over50_rate": d_team_over50,
            "team_over50_diff": r_team_over50 - d_team_over50,
            "r_team_under40_rate": r_team_under40,
            "d_team_under40_rate": d_team_under40,
            "team_under40_diff": r_team_under40 - d_team_under40,
            "r_roster_group_kills": r_roster_kills,
            "d_roster_group_kills": d_roster_kills,
            "roster_group_kills_diff": r_roster_kills - d_roster_kills,
            "r_roster_group_against": r_roster_against,
            "d_roster_group_against": d_roster_against,
            "roster_group_against_diff": r_roster_against - d_roster_against,
            "r_roster_group_total": r_roster_total,
            "d_roster_group_total": d_roster_total,
            "roster_group_total_diff": r_roster_total - d_roster_total,
            "r_roster_group_kpm": r_roster_kpm,
            "d_roster_group_kpm": d_roster_kpm,
            "roster_group_kpm_diff": r_roster_kpm - d_roster_kpm,
            "r_roster_group_dur": r_roster_dur,
            "d_roster_group_dur": d_roster_dur,
            "roster_group_dur_diff": r_roster_dur - d_roster_dur,
            "r_roster_group_over50": r_roster_over50,
            "d_roster_group_over50": d_roster_over50,
            "roster_group_over50_diff": r_roster_over50 - d_roster_over50,
            "r_roster_group_under40": r_roster_under40,
            "d_roster_group_under40": d_roster_under40,
            "roster_group_under40_diff": r_roster_under40 - d_roster_under40,
            "r_roster_group_hist": r_roster_hist,
            "d_roster_group_hist": d_roster_hist,
            "roster_group_hist_diff": r_roster_hist - d_roster_hist,
            "r_team_kill_share": r_team_kill_share,
            "d_team_kill_share": d_team_kill_share,
            "team_kill_share_diff": r_team_kill_share - d_team_kill_share,
            "r_team_kill_ratio": r_team_kill_ratio,
            "d_team_kill_ratio": d_team_kill_ratio,
            "team_kill_ratio_diff": r_team_kill_ratio - d_team_kill_ratio,
            "r_team_elo": r_team_elo,
            "d_team_elo": d_team_elo,
            "team_elo_diff": team_elo_diff,
            "team_elo_win_prob": team_elo_win_prob,
            "r_team_elo_games": r_team_elo_games,
            "d_team_elo_games": d_team_elo_games,
            "team_elo_games_diff": r_team_elo_games - d_team_elo_games,
            "r_team_hist_count": r_team_hist,
            "d_team_hist_count": d_team_hist,
            "r_team_recent_total": r_team_recent_total,
            "d_team_recent_total": d_team_recent_total,
            "team_recent_total_diff": r_team_recent_total - d_team_recent_total
            if not math.isnan(r_team_recent_total) and not math.isnan(d_team_recent_total)
            else float("nan"),
            "r_team_recent_kpm": r_team_recent_kpm,
            "d_team_recent_kpm": d_team_recent_kpm,
            "team_recent_kpm_diff": r_team_recent_kpm - d_team_recent_kpm
            if not math.isnan(r_team_recent_kpm) and not math.isnan(d_team_recent_kpm)
            else float("nan"),
            "r_team_recent_dur": r_team_recent_dur,
            "d_team_recent_dur": d_team_recent_dur,
            "team_recent_dur_diff": r_team_recent_dur - d_team_recent_dur
            if not math.isnan(r_team_recent_dur) and not math.isnan(d_team_recent_dur)
            else float("nan"),
            "r_team_recent_over50": r_team_recent_over50,
            "d_team_recent_over50": d_team_recent_over50,
            "team_recent_over50_diff": r_team_recent_over50 - d_team_recent_over50
            if not math.isnan(r_team_recent_over50) and not math.isnan(d_team_recent_over50)
            else float("nan"),
            "r_team_recent_under40": r_team_recent_under40,
            "d_team_recent_under40": d_team_recent_under40,
            "team_recent_under40_diff": r_team_recent_under40 - d_team_recent_under40
            if not math.isnan(r_team_recent_under40) and not math.isnan(d_team_recent_under40)
            else float("nan"),
            "r_team_recent_std": r_team_recent_std,
            "d_team_recent_std": d_team_recent_std,
            "team_recent_std_diff": r_team_recent_std - d_team_recent_std
            if not math.isnan(r_team_recent_std) and not math.isnan(d_team_recent_std)
            else float("nan"),
            "r_team_recent_count": r_team_recent_count,
            "d_team_recent_count": d_team_recent_count,
            "team_recent_kpm_sum": team_recent_kpm_sum,
            "hero_recent_kpm_sum": hero_recent_kpm_sum,
            "expected_total10_team": expected_total10_team,
            "expected_total10_hero": expected_total10_hero,
            "early_kpm_diff_team": early_kpm_diff_team,
            "early_kpm_diff_hero": early_kpm_diff_hero,
            "early_total10_ratio_team": early_total10_ratio_team,
            "early_total10_ratio_hero": early_total10_ratio_hero,
            "early_total10_delta_team": early_total10_delta_team,
            "early_total10_delta_hero": early_total10_delta_hero,
            "r_team_early_for10": r_team_early_for10,
            "d_team_early_for10": d_team_early_for10,
            "team_early_for10_diff": r_team_early_for10 - d_team_early_for10,
            "r_team_early_against10": r_team_early_against10,
            "d_team_early_against10": d_team_early_against10,
            "team_early_against10_diff": r_team_early_against10 - d_team_early_against10,
            "r_team_early_total10": r_team_early_total10,
            "d_team_early_total10": d_team_early_total10,
            "team_early_total10_diff": r_team_early_total10 - d_team_early_total10,
            "r_team_early_share10": r_team_early_share10,
            "d_team_early_share10": d_team_early_share10,
            "team_early_share10_diff": r_team_early_share10 - d_team_early_share10,
            "r_team_early_count": r_team_early_count,
            "d_team_early_count": d_team_early_count,
            "team_early_total10_mean": team_early_total10_mean,
            "early_total10_delta_team_early": early_total10_delta_team_early,
            "rad_hero_early_total10_sum": float(sum(rad_hero_early_total)),
            "dire_hero_early_total10_sum": float(sum(dire_hero_early_total)),
            "hero_early_total10_diff": float(sum(rad_hero_early_total) - sum(dire_hero_early_total)),
            "rad_hero_early_for10_sum": float(sum(rad_hero_early_for)),
            "dire_hero_early_for10_sum": float(sum(dire_hero_early_for)),
            "hero_early_for10_diff": float(sum(rad_hero_early_for) - sum(dire_hero_early_for)),
            "rad_hero_early_share10_mean": list_mean(rad_hero_early_share),
            "dire_hero_early_share10_mean": list_mean(dire_hero_early_share),
            "hero_early_share10_diff": list_mean(rad_hero_early_share) - list_mean(dire_hero_early_share),
            "rad_hero_early_count_mean": list_mean(rad_hero_early_count),
            "dire_hero_early_count_mean": list_mean(dire_hero_early_count),
            "hero_early_count_diff": list_mean(rad_hero_early_count) - list_mean(dire_hero_early_count),
            "r_player_early_total10_mean": list_mean(r_player_early_total),
            "d_player_early_total10_mean": list_mean(d_player_early_total),
            "player_early_total10_diff": list_mean(r_player_early_total) - list_mean(d_player_early_total),
            "r_player_early_for10_mean": list_mean(r_player_early_for),
            "d_player_early_for10_mean": list_mean(d_player_early_for),
            "player_early_for10_diff": list_mean(r_player_early_for) - list_mean(d_player_early_for),
            "r_player_early_total10_max": list_max(r_player_early_total),
            "d_player_early_total10_max": list_max(d_player_early_total),
            "player_early_total10_max_diff": list_max(r_player_early_total) - list_max(d_player_early_total),
            "r_player_early_for10_max": list_max(r_player_early_for),
            "d_player_early_for10_max": list_max(d_player_early_for),
            "player_early_for10_max_diff": list_max(r_player_early_for) - list_max(d_player_early_for),
            "r_player_early_share10_mean": list_mean(r_player_early_share),
            "d_player_early_share10_mean": list_mean(d_player_early_share),
            "player_early_share10_diff": list_mean(r_player_early_share) - list_mean(d_player_early_share),
            "r_player_early_count_mean": list_mean(r_player_early_count),
            "d_player_early_count_mean": list_mean(d_player_early_count),
            "player_early_count_diff": list_mean(r_player_early_count) - list_mean(d_player_early_count),
            "team_vs_early_total10": team_vs_early_total10,
            "team_vs_early_kpm10": team_vs_early_kpm10,
            "team_vs_early_abs_diff10": team_vs_early_abs_diff10,
            "team_vs_early_count": team_vs_early_count,
            "r_player_avg_kills": stats_mean(rad_player_stats, 0),
            "d_player_avg_kills": stats_mean(dire_player_stats, 0),
            "player_kills_diff": stats_mean(rad_player_stats, 0) - stats_mean(dire_player_stats, 0),
            "r_player_avg_deaths": stats_mean(rad_player_stats, 1),
            "d_player_avg_deaths": stats_mean(dire_player_stats, 1),
            "player_deaths_diff": stats_mean(rad_player_stats, 1) - stats_mean(dire_player_stats, 1),
            "r_player_avg_assists": stats_mean(rad_player_stats, 2),
            "d_player_avg_assists": stats_mean(dire_player_stats, 2),
            "player_assists_diff": stats_mean(rad_player_stats, 2) - stats_mean(dire_player_stats, 2),
            "r_player_avg_kpm": stats_mean(rad_player_stats, 3),
            "d_player_avg_kpm": stats_mean(dire_player_stats, 3),
            "player_kpm_diff": stats_mean(rad_player_stats, 3) - stats_mean(dire_player_stats, 3),
            "r_player_avg_gpm": stats_mean(rad_player_stats, 4),
            "d_player_avg_gpm": stats_mean(dire_player_stats, 4),
            "player_gpm_diff": stats_mean(rad_player_stats, 4) - stats_mean(dire_player_stats, 4),
            "r_player_avg_xpm": stats_mean(rad_player_stats, 5),
            "d_player_avg_xpm": stats_mean(dire_player_stats, 5),
            "player_xpm_diff": stats_mean(rad_player_stats, 5) - stats_mean(dire_player_stats, 5),
            "r_player_avg_hero_dmg": stats_mean(rad_player_stats, 6),
            "d_player_avg_hero_dmg": stats_mean(dire_player_stats, 6),
            "player_hero_dmg_diff": stats_mean(rad_player_stats, 6) - stats_mean(dire_player_stats, 6),
            "r_player_avg_tower_dmg": stats_mean(rad_player_stats, 7),
            "d_player_avg_tower_dmg": stats_mean(dire_player_stats, 7),
            "player_tower_dmg_diff": stats_mean(rad_player_stats, 7) - stats_mean(dire_player_stats, 7),
            "r_player_avg_imp": stats_mean(rad_player_stats, 8),
            "d_player_avg_imp": stats_mean(dire_player_stats, 8),
            "player_imp_diff": stats_mean(rad_player_stats, 8) - stats_mean(dire_player_stats, 8),
            "r_player_avg_kda": stats_kda(rad_player_stats),
            "d_player_avg_kda": stats_kda(dire_player_stats),
            "player_kda_diff": stats_kda(rad_player_stats) - stats_kda(dire_player_stats),
            "r_player_hist_count": stats_mean(rad_player_stats, 9),
            "d_player_hist_count": stats_mean(dire_player_stats, 9),
            "r_player_avg_lhpm": stats_mean(rad_player_stats, 10),
            "d_player_avg_lhpm": stats_mean(dire_player_stats, 10),
            "player_lhpm_diff": stats_mean(rad_player_stats, 10) - stats_mean(dire_player_stats, 10),
            "r_player_avg_denypm": stats_mean(rad_player_stats, 11),
            "d_player_avg_denypm": stats_mean(dire_player_stats, 11),
            "player_denypm_diff": stats_mean(rad_player_stats, 11) - stats_mean(dire_player_stats, 11),
            "r_player_avg_healpm": stats_mean(rad_player_stats, 12),
            "d_player_avg_healpm": stats_mean(dire_player_stats, 12),
            "player_healpm_diff": stats_mean(rad_player_stats, 12) - stats_mean(dire_player_stats, 12),
            "r_player_avg_invispm": stats_mean(rad_player_stats, 13),
            "d_player_avg_invispm": stats_mean(dire_player_stats, 13),
            "player_invispm_diff": stats_mean(rad_player_stats, 13) - stats_mean(dire_player_stats, 13),
            "r_player_avg_level": stats_mean(rad_player_stats, 14),
            "d_player_avg_level": stats_mean(dire_player_stats, 14),
            "player_level_diff": stats_mean(rad_player_stats, 14) - stats_mean(dire_player_stats, 14),
            "r_player_recent_kills": stats_mean(rad_player_recent_stats, 0),
            "d_player_recent_kills": stats_mean(dire_player_recent_stats, 0),
            "player_recent_kills_diff": stats_mean(rad_player_recent_stats, 0) - stats_mean(dire_player_recent_stats, 0),
            "r_player_recent_deaths": stats_mean(rad_player_recent_stats, 1),
            "d_player_recent_deaths": stats_mean(dire_player_recent_stats, 1),
            "player_recent_deaths_diff": stats_mean(rad_player_recent_stats, 1) - stats_mean(dire_player_recent_stats, 1),
            "r_player_recent_assists": stats_mean(rad_player_recent_stats, 2),
            "d_player_recent_assists": stats_mean(dire_player_recent_stats, 2),
            "player_recent_assists_diff": stats_mean(rad_player_recent_stats, 2) - stats_mean(dire_player_recent_stats, 2),
            "r_player_recent_kpm": stats_mean(rad_player_recent_stats, 3),
            "d_player_recent_kpm": stats_mean(dire_player_recent_stats, 3),
            "player_recent_kpm_diff": stats_mean(rad_player_recent_stats, 3) - stats_mean(dire_player_recent_stats, 3),
            "r_player_recent_count": stats_mean(rad_player_recent_stats, 4),
            "d_player_recent_count": stats_mean(dire_player_recent_stats, 4),
            "r_player_recent_kills_std": stats_std(rad_player_recent_stats, 0),
            "d_player_recent_kills_std": stats_std(dire_player_recent_stats, 0),
            "player_recent_kills_std_diff": stats_std(rad_player_recent_stats, 0)
            - stats_std(dire_player_recent_stats, 0),
            "r_player_recent_kpm_std": stats_std(rad_player_recent_stats, 3),
            "d_player_recent_kpm_std": stats_std(dire_player_recent_stats, 3),
            "player_recent_kpm_std_diff": stats_std(rad_player_recent_stats, 3)
            - stats_std(dire_player_recent_stats, 3),
            "r_player_aggr_avg": float(np.mean(rad_player_aggr)) if rad_player_aggr else 0.0,
            "d_player_aggr_avg": float(np.mean(dire_player_aggr)) if dire_player_aggr else 0.0,
            "player_aggr_diff": (
                float(np.mean(rad_player_aggr)) - float(np.mean(dire_player_aggr))
                if rad_player_aggr and dire_player_aggr
                else 0.0
            ),
            "r_player_aggr_max": list_max(rad_player_aggr),
            "d_player_aggr_max": list_max(dire_player_aggr),
            "player_aggr_max_diff": list_max(rad_player_aggr) - list_max(dire_player_aggr)
            if rad_player_aggr and dire_player_aggr
            else 0.0,
            "r_player_aggr_min": list_min(rad_player_aggr),
            "d_player_aggr_min": list_min(dire_player_aggr),
            "player_aggr_min_diff": list_min(rad_player_aggr) - list_min(dire_player_aggr)
            if rad_player_aggr and dire_player_aggr
            else 0.0,
            "r_player_aggr_std": list_std(rad_player_aggr),
            "d_player_aggr_std": list_std(dire_player_aggr),
            "player_aggr_std_diff": list_std(rad_player_aggr) - list_std(dire_player_aggr)
            if rad_player_aggr and dire_player_aggr
            else 0.0,
            "r_player_feed_avg": float(np.mean(rad_player_feed)) if rad_player_feed else 0.0,
            "d_player_feed_avg": float(np.mean(dire_player_feed)) if dire_player_feed else 0.0,
            "player_feed_diff": (
                float(np.mean(rad_player_feed)) - float(np.mean(dire_player_feed))
                if rad_player_feed and dire_player_feed
                else 0.0
            ),
            "r_player_feed_max": list_max(rad_player_feed),
            "d_player_feed_max": list_max(dire_player_feed),
            "player_feed_max_diff": list_max(rad_player_feed) - list_max(dire_player_feed)
            if rad_player_feed and dire_player_feed
            else 0.0,
            "r_player_feed_min": list_min(rad_player_feed),
            "d_player_feed_min": list_min(dire_player_feed),
            "player_feed_min_diff": list_min(rad_player_feed) - list_min(dire_player_feed)
            if rad_player_feed and dire_player_feed
            else 0.0,
            "r_player_feed_std": list_std(rad_player_feed),
            "d_player_feed_std": list_std(dire_player_feed),
            "player_feed_std_diff": list_std(rad_player_feed) - list_std(dire_player_feed)
            if rad_player_feed and dire_player_feed
            else 0.0,
            "r_player_unique_avg": float(np.mean(rad_player_unique)) if rad_player_unique else 0.0,
            "d_player_unique_avg": float(np.mean(dire_player_unique)) if dire_player_unique else 0.0,
            "player_unique_diff": (
                float(np.mean(rad_player_unique)) - float(np.mean(dire_player_unique))
                if rad_player_unique and dire_player_unique
                else 0.0
            ),
            "r_player_unique_min": list_min(rad_player_unique),
            "d_player_unique_min": list_min(dire_player_unique),
            "player_unique_min_diff": list_min(rad_player_unique) - list_min(dire_player_unique)
            if rad_player_unique and dire_player_unique
            else 0.0,
            "r_player_unique_max": list_max(rad_player_unique),
            "d_player_unique_max": list_max(dire_player_unique),
            "player_unique_max_diff": list_max(rad_player_unique) - list_max(dire_player_unique)
            if rad_player_unique and dire_player_unique
            else 0.0,
            "r_player_unique_std": list_std(rad_player_unique),
            "d_player_unique_std": list_std(dire_player_unique),
            "player_unique_std_diff": list_std(rad_player_unique) - list_std(dire_player_unique)
            if rad_player_unique and dire_player_unique
            else 0.0,
            "r_player_hero_share_avg": float(np.mean(rad_player_hero_share)) if rad_player_hero_share else 0.0,
            "d_player_hero_share_avg": float(np.mean(dire_player_hero_share)) if dire_player_hero_share else 0.0,
            "player_hero_share_diff": (
                float(np.mean(rad_player_hero_share)) - float(np.mean(dire_player_hero_share))
                if rad_player_hero_share and dire_player_hero_share
                else 0.0
            ),
            "r_player_hero_share_min": float(np.min(rad_player_hero_share)) if rad_player_hero_share else 0.0,
            "d_player_hero_share_min": float(np.min(dire_player_hero_share)) if dire_player_hero_share else 0.0,
            "player_hero_share_min_diff": (
                float(np.min(rad_player_hero_share)) - float(np.min(dire_player_hero_share))
                if rad_player_hero_share and dire_player_hero_share
                else 0.0
            ),
            "r_player_hero_share_max": list_max(rad_player_hero_share),
            "d_player_hero_share_max": list_max(dire_player_hero_share),
            "player_hero_share_max_diff": list_max(rad_player_hero_share) - list_max(dire_player_hero_share)
            if rad_player_hero_share and dire_player_hero_share
            else 0.0,
            "r_player_hero_share_std": list_std(rad_player_hero_share),
            "d_player_hero_share_std": list_std(dire_player_hero_share),
            "player_hero_share_std_diff": list_std(rad_player_hero_share) - list_std(dire_player_hero_share)
            if rad_player_hero_share and dire_player_hero_share
            else 0.0,
            "r_pair_avg_kills": r_pair_kills,
            "d_pair_avg_kills": d_pair_kills,
            "pair_avg_kills_diff": r_pair_kills - d_pair_kills,
            "r_pair_avg_kpm": r_pair_kpm,
            "d_pair_avg_kpm": d_pair_kpm,
            "pair_avg_kpm_diff": r_pair_kpm - d_pair_kpm,
            "r_pair_avg_dur": r_pair_dur,
            "d_pair_avg_dur": d_pair_dur,
            "pair_avg_dur_diff": r_pair_dur - d_pair_dur,
            "r_pair_hist_count": r_pair_cnt,
            "d_pair_hist_count": d_pair_cnt,
            "hero_vs_avg_kills": hero_vs_kills,
            "hero_vs_avg_kpm": hero_vs_kpm,
            "hero_vs_hist_count": hero_vs_cnt,
            "r_player_hero_avg_kills": r_player_hero_kills,
            "d_player_hero_avg_kills": d_player_hero_kills,
            "player_hero_kills_diff": r_player_hero_kills - d_player_hero_kills,
            "r_player_hero_avg_kpm": r_player_hero_kpm,
            "d_player_hero_avg_kpm": d_player_hero_kpm,
            "player_hero_kpm_diff": r_player_hero_kpm - d_player_hero_kpm,
            "r_player_hero_hist_count": r_player_hero_cnt,
            "d_player_hero_hist_count": d_player_hero_cnt,
            "r_player_pair_avg_kills": r_player_pair_kills,
            "d_player_pair_avg_kills": d_player_pair_kills,
            "player_pair_kills_diff": r_player_pair_kills - d_player_pair_kills,
            "r_player_pair_avg_kpm": r_player_pair_kpm,
            "d_player_pair_avg_kpm": d_player_pair_kpm,
            "player_pair_kpm_diff": r_player_pair_kpm - d_player_pair_kpm,
            "r_player_pair_avg_dur": r_player_pair_dur,
            "d_player_pair_avg_dur": d_player_pair_dur,
            "player_pair_dur_diff": r_player_pair_dur - d_player_pair_dur,
            "r_player_pair_avg_cnt": r_player_pair_cnt,
            "d_player_pair_avg_cnt": d_player_pair_cnt,
            "team_vs_avg_kills": team_vs_avg_kills,
            "team_vs_avg_kpm": team_vs_avg_kpm,
            "team_vs_hist_count": team_vs_hist,
            "team_vs_over50_rate": team_vs_over50_rate,
            "team_vs_under40_rate": team_vs_under40_rate,
            "league_avg_kills": league_avg_kills,
            "league_avg_kpm": league_avg_kpm,
            "league_avg_dur": league_avg_dur,
            "league_over50_rate": league_over50,
            "league_under40_rate": league_under40,
            "league_hist_count": league_hist,
            "version_avg_kills": version_avg_kills,
            "version_avg_kpm": version_avg_kpm,
            "version_avg_dur": version_avg_dur,
            "version_over50_rate": version_over50,
            "version_under40_rate": version_under40,
            "version_hist_count": version_hist,
            "start_year": now.year,
            "start_month": now.month,
            "start_weekday": now.weekday(),
            "start_hour": now.hour,
        }
    )

    for i in range(10):
        row[f"rad_kills_m{i+1}"] = rad_vals[i]
        row[f"dire_kills_m{i+1}"] = dire_vals[i]
        row[f"total_kills_m{i+1}"] = total_per_min[i]
        row[f"xp_lead_m{i+1}"] = xp_vals[i]
        row[f"nw_lead_m{i+1}"] = nw_vals[i]

    for i, hid in enumerate(rad_ids, 1):
        row[f"radiant_hero_{i}"] = hid
    for i, hid in enumerate(dire_ids, 1):
        row[f"dire_hero_{i}"] = hid
    for i, pid in enumerate(rad_pids, 1):
        row[f"radiant_player_{i}_id"] = pid
    for i, pid in enumerate(dire_pids, 1):
        row[f"dire_player_{i}_id"] = pid

    row["radiant_team_id"] = radiant_team_id
    row["dire_team_id"] = dire_team_id
    row["game_version_id"] = version_id
    row["league_id"] = league_id
    live_league = data.get("live_league_data") or {}
    series = (data.get("db") or {}).get("series") or data.get("series") or {}
    series_type = series.get("type")
    if series_type is None or series_type == "":
        series_type = data.get("series_type") or live_league.get("series_type") or "UNKNOWN"

    series_game = _coerce_int(series.get("game") or data.get("series_game") or live_league.get("series_game"))
    series_game_num = _coerce_int(data.get("series_game_num"))
    if series_game_num <= 0 and series_game > 0:
        series_game_num = series_game
    if series_game_num <= 0:
        r_wins = _coerce_int(live_league.get("radiant_series_wins"))
        d_wins = _coerce_int(live_league.get("dire_series_wins"))
        if r_wins or d_wins:
            series_game_num = r_wins + d_wins + 1

    is_decider_game = 0
    try:
        series_type_num = int(series_type) if str(series_type).isdigit() else None
    except Exception:
        series_type_num = None
    if series_type_num == 1:
        is_decider_game = 1
    else:
        r_wins = _coerce_int(live_league.get("radiant_series_wins"))
        d_wins = _coerce_int(live_league.get("dire_series_wins"))
        if r_wins > 0 and d_wins > 0 and r_wins == d_wins:
            is_decider_game = 1

    row["series_type"] = series_type
    row["series_game"] = series_game
    row["series_game_num"] = series_game_num
    row["late_series_game"] = 1 if series_game_num >= 2 else 0
    row["is_decider_game"] = is_decider_game
    row["tournament_round"] = data.get("tournamentRound") or "UNKNOWN"
    row["lobby_type"] = data.get("lobbyType") or "UNKNOWN"
    row["region_id"] = _coerce_int(data.get("regionId"))
    row["rank"] = _coerce_int(data.get("rank"))
    row["bracket"] = _coerce_int(data.get("bracket"))
    row["bottom_lane_outcome"] = data.get("bottomLaneOutcome") or "UNKNOWN"
    row["mid_lane_outcome"] = data.get("midLaneOutcome") or "UNKNOWN"
    row["top_lane_outcome"] = data.get("topLaneOutcome") or "UNKNOWN"

    draft_predictor = _get_kills_draft_predictor()
    if draft_predictor is not None:
        try:
            tournament_tier = None
            if team_context and team_context.get("match_tier") is not None:
                tournament_tier = team_context.get("match_tier")
            else:
                tournament_tier = _determine_match_tier(radiant_team_id, dire_team_id)

            draft_feats = draft_predictor.build_features(
                radiant_ids=rad_ids,
                dire_ids=dire_ids,
                radiant_account_ids=None,
                dire_account_ids=None,
                radiant_team_id=radiant_team_id if radiant_team_id > 0 else None,
                dire_team_id=dire_team_id if dire_team_id > 0 else None,
                h2h_avg_total=h2h_avg_total,
                h2h_matches_count=h2h_matches_count,
                league_avg_kills=league_avg_kills_ctx
                if league_avg_kills_ctx is not None
                else league_avg_kills,
                league_kills_std=league_kills_std,
                league_meta_diff=league_meta_diff,
                series_game_num=series_game_num if series_game_num > 0 else None,
                is_decider_game=is_decider_game if is_decider_game in (0, 1) else None,
                combined_form_kills=combined_form_kills,
                combined_team_avg_kills=combined_team_avg_kills,
                combined_team_aggression=combined_team_aggression,
                combined_synthetic_kills=combined_synthetic_kills,
                match_start_time=match_start_time,
                league_id=league_id if league_id > 0 else None,
                series_type=series_type,
                region_id=_coerce_int(data.get("regionId")),
                tournament_tier=tournament_tier,
                combined_patch_form_kills=combined_patch_form_kills,
                combined_patch_team_avg_kills=combined_patch_team_avg_kills,
                combined_patch_team_aggression=combined_patch_team_aggression,
                radiant_roster_shared_prev=radiant_roster_shared_prev,
                dire_roster_shared_prev=dire_roster_shared_prev,
                radiant_roster_changed_prev=radiant_roster_changed_prev,
                dire_roster_changed_prev=dire_roster_changed_prev,
                radiant_roster_stable_prev=radiant_roster_stable_prev,
                dire_roster_stable_prev=dire_roster_stable_prev,
                radiant_roster_new_team=radiant_roster_new_team,
                dire_roster_new_team=dire_roster_new_team,
                radiant_roster_group_matches=radiant_roster_group_matches,
                dire_roster_group_matches=dire_roster_group_matches,
                radiant_roster_player_count=radiant_roster_player_count,
                dire_roster_player_count=dire_roster_player_count,
            )
            for key, val in draft_feats.items():
                if _draft_feature_allowed(key):
                    row[key] = val
        except Exception as e:
            logger.warning(f"Failed to build draft features: {e}")

    return row


def _parse_live_number(val: Any) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    try:
        s = str(val).strip()
        if not s:
            return 0.0
        if "/" in s:
            s = s.split("/", 1)[0].strip()
        return float(s)
    except Exception:
        return 0.0


def _extract_live_player_stats(data: Dict[str, Any], game_time: Optional[float]) -> Dict[str, Any]:
    scoreboard = (data.get("live_league_data") or {}).get("scoreboard") or {}
    rad_players = (scoreboard.get("radiant") or {}).get("players") or []
    dire_players = (scoreboard.get("dire") or {}).get("players") or []

    def team_summary(players: List[Dict[str, Any]]) -> Dict[str, float]:
        if not players:
            return {
                "avg_level": 0.0,
                "sum_kills": 0.0,
                "sum_deaths": 0.0,
                "sum_assists": 0.0,
                "sum_lh": 0.0,
                "sum_denies": 0.0,
                "sum_gpm": 0.0,
                "sum_xpm": 0.0,
                "sum_nw": 0.0,
            }
        levels = [_parse_live_number(p.get("level")) for p in players]
        return {
            "avg_level": float(sum(levels) / len(levels)),
            "sum_kills": float(sum(_parse_live_number(p.get("kills")) for p in players)),
            "sum_deaths": float(sum(_parse_live_number(p.get("death") or p.get("deaths")) for p in players)),
            "sum_assists": float(sum(_parse_live_number(p.get("assists")) for p in players)),
            "sum_lh": float(sum(_parse_live_number(p.get("last_hits")) for p in players)),
            "sum_denies": float(sum(_parse_live_number(p.get("denies")) for p in players)),
            "sum_gpm": float(sum(_parse_live_number(p.get("gold_per_min")) for p in players)),
            "sum_xpm": float(sum(_parse_live_number(p.get("xp_per_min")) for p in players)),
            "sum_nw": float(sum(_parse_live_number(p.get("net_worth")) for p in players)),
        }

    rad = team_summary(rad_players)
    dire = team_summary(dire_players)

    total_kills = _parse_live_number(data.get("radiant_score")) + _parse_live_number(data.get("dire_score"))
    minutes = (game_time or 0.0) / 60.0 if game_time else 0.0
    live_kpm = (total_kills / minutes) if minutes > 0 else 0.0

    return {
        "rad": rad,
        "dire": dire,
        "avg_level_diff": rad["avg_level"] - dire["avg_level"],
        "sum_gpm_diff": rad["sum_gpm"] - dire["sum_gpm"],
        "sum_xpm_diff": rad["sum_xpm"] - dire["sum_xpm"],
        "sum_nw_diff": rad["sum_nw"] - dire["sum_nw"],
        "live_kpm": live_kpm,
        "total_kills_so_far": total_kills,
    }


def _predict_kills_bet(
    radiant_heroes_and_pos: Dict[str, Dict[str, Any]],
    dire_heroes_and_pos: Dict[str, Dict[str, Any]],
    radiant_team_id: Optional[int],
    dire_team_id: Optional[int],
    league_id: Optional[int],
    data: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if not _load_kills_models():
        return None
    rules = _load_kills_rules()

    try:
        row = _build_kills_feature_row(
            radiant_heroes_and_pos,
            dire_heroes_and_pos,
            radiant_team_id,
            dire_team_id,
            league_id,
            data,
        )
    except Exception:
        logger.exception(
            "Kills feature build failed (match_id=%s league_id=%s radiant_team_id=%s dire_team_id=%s)",
            data.get("match_id") or (data.get("live_league_data") or {}).get("match_id"),
            league_id,
            radiant_team_id,
            dire_team_id,
        )
        return None

    # Defaults for networth-OFF profit objective (walk-forward heavy)
    rules = dict(rules or {})
    rules.setdefault("low_rule", {"type": "low_prob", "prob_threshold": 0.70})
    rules.setdefault("high_rule", {"type": "high_prob", "prob_threshold": 0.60})
    patch_label = row.get("patch_major_label")
    patch_overrides = rules.get("patch_overrides") or {}
    if isinstance(patch_label, str) and patch_label in patch_overrides:
        override = patch_overrides.get(patch_label) or {}
        if isinstance(override, dict):
            for key, val in override.items():
                rules[key] = val
    networth_mode = str(rules.get("networth_mode", "off")).strip().lower()
    # Networth toggle: drop networth-related features when disabled
    for key in list(row.keys()):
        k = key.lower()
        if networth_mode != "on" and (
            k.startswith("nw")
            or "networth" in k
            or "net_worth" in k
            or "_nw" in k
            or "nw_" in k
        ):
            row[key] = float("nan")
        elif row[key] is None:
            row[key] = float("nanps")
        elif isinstance(row[key], str) and row[key].strip().lower() == "none":
            row[key] = float("nan")

    none_keys = [k for k, v in row.items() if v is None]
    if none_keys:
        logger.warning("Kills features contain None values: %s", none_keys)

    try:
        import pandas as pd
    except Exception as e:
        logger.warning(f"Pandas not available: {e}")
        return None

    X = pd.DataFrame([row], columns=KILLS_FEATURE_COLS)
    for c in KILLS_CAT_COLS or []:
        if c in X.columns:
            X[c] = X[c].fillna("UNKNOWN").astype(str)

    models = KILLS_MODELS
    model_variant = "global"
    patch_label = row.get("patch_major_label")
    if isinstance(patch_label, str) and patch_label and patch_label != "UNKNOWN":
        patch_models = _load_kills_group_models("patch", patch_label)
        if patch_models:
            models = patch_models
            model_variant = f"patch:{patch_label}"

    match_tier = row.get("match_tier")
    match_tier_known = row.get("match_tier_known")
    if models is KILLS_MODELS and match_tier in (1, 2) and match_tier_known:
        tier_models = _load_kills_group_models("tier", int(match_tier))
        if tier_models:
            models = tier_models
            model_variant = f"tier:{int(match_tier)}"

    reg_all = models["reg_all"]
    reg_low = models["reg_low"]
    reg_high = models["reg_high"]
    cls_low = models["cls_low"]
    cls_high = models["cls_high"]

    pred_all = float(reg_all.predict(X)[0])
    pred_low = float(reg_low.predict(X)[0])
    pred_high = float(reg_high.predict(X)[0])
    low_prob = float(cls_low.predict_proba(X)[0][1])
    high_prob = float(cls_high.predict_proba(X)[0][1])
    pred_q10 = float("nan")
    pred_q90 = float("nan")
    if KILLS_Q10_MODEL is not None and KILLS_Q90_MODEL is not None:
        try:
            pred_q10 = float(KILLS_Q10_MODEL.predict(X)[0])
            pred_q90 = float(KILLS_Q90_MODEL.predict(X)[0])
        except Exception as e:
            logger.warning(f"Quantile prediction failed: {e}")
    row["pred_q10"] = pred_q10
    row["pred_q90"] = pred_q90

    live_stats = _extract_live_player_stats(data, data.get("game_time"))
    duration_pred = None
    kpm_pred = None
    duration_source = None
    draft_predictor = _get_kills_draft_predictor()
    if draft_predictor is not None:
        try:
            rad_ids = [radiant_heroes_and_pos[p].get("hero_id", 0) for p in ["pos1", "pos2", "pos3", "pos4", "pos5"]]
            dire_ids = [dire_heroes_and_pos[p].get("hero_id", 0) for p in ["pos1", "pos2", "pos3", "pos4", "pos5"]]
            rad_pids = [radiant_heroes_and_pos[p].get("account_id", 0) for p in ["pos1", "pos2", "pos3", "pos4", "pos5"]]
            dire_pids = [dire_heroes_and_pos[p].get("account_id", 0) for p in ["pos1", "pos2", "pos3", "pos4", "pos5"]]
            use_players = all(_coerce_int(pid) > 0 for pid in (rad_pids + dire_pids))
            early_stats = None
            early_keys = getattr(draft_predictor, "EARLY_STATS_KEYS", []) or []
            if early_keys:
                early_stats = {k: row.get(k) for k in early_keys}
            draft_feats = draft_predictor.build_features(
                radiant_ids=[_coerce_int(h) for h in rad_ids],
                dire_ids=[_coerce_int(h) for h in dire_ids],
                radiant_account_ids=[_coerce_int(pid) for pid in rad_pids] if use_players else None,
                dire_account_ids=[_coerce_int(pid) for pid in dire_pids] if use_players else None,
                radiant_team_id=radiant_team_id if radiant_team_id and radiant_team_id > 0 else None,
                dire_team_id=dire_team_id if dire_team_id and dire_team_id > 0 else None,
                series_game_num=row.get("series_game_num") or None,
                is_decider_game=row.get("is_decider_game") if row.get("is_decider_game") in (0, 1) else None,
                league_avg_kills=row.get("league_avg_kills"),
                league_kills_std=row.get("league_kills_std"),
                league_meta_diff=row.get("league_meta_diff"),
                combined_form_kills=row.get("combined_form_kills"),
                combined_team_avg_kills=row.get("combined_team_avg_kills"),
                combined_team_aggression=row.get("combined_team_aggression"),
                combined_synthetic_kills=row.get("combined_synthetic_kills"),
                match_start_time=row.get("start_time"),
                league_id=row.get("league_id"),
                series_type=row.get("series_type"),
                region_id=row.get("region_id"),
                tournament_tier=row.get("tournament_tier")
                if row.get("tournament_tier") is not None
                else _determine_match_tier(radiant_team_id, dire_team_id),
                combined_patch_form_kills=row.get("combined_patch_form_kills"),
                combined_patch_team_avg_kills=row.get("combined_patch_team_avg_kills"),
                combined_patch_team_aggression=row.get("combined_patch_team_aggression"),
                radiant_roster_shared_prev=row.get("radiant_roster_shared_prev"),
                dire_roster_shared_prev=row.get("dire_roster_shared_prev"),
                radiant_roster_changed_prev=row.get("radiant_roster_changed_prev"),
                dire_roster_changed_prev=row.get("dire_roster_changed_prev"),
                radiant_roster_stable_prev=row.get("radiant_roster_stable_prev"),
                dire_roster_stable_prev=row.get("dire_roster_stable_prev"),
                radiant_roster_new_team=row.get("radiant_roster_new_team"),
                dire_roster_new_team=row.get("dire_roster_new_team"),
                radiant_roster_group_matches=row.get("radiant_roster_group_matches"),
                dire_roster_group_matches=row.get("dire_roster_group_matches"),
                radiant_roster_player_count=row.get("radiant_roster_player_count"),
                dire_roster_player_count=row.get("dire_roster_player_count"),
                early_stats=early_stats,
            )
            for key in list(draft_feats.keys()):
                lk = key.lower()
                if "winrate" in lk or "glicko" in lk:
                    draft_feats[key] = 0.0
            _, _, _, duration_pred, kpm_pred = draft_predictor.predict_with_models(draft_feats)
            duration_source = "draft"
        except Exception as e:
            logger.warning(f"Duration prediction failed: {e}")

    low_rule = rules.get("low_rule", {})
    high_rule = rules.get("high_rule", {})

    def low_ok() -> bool:
        if low_rule.get("type") == "low_prob":
            return low_prob >= float(low_rule.get("prob_threshold", 0.6))
        if low_rule.get("type") == "low_prob_margin":
            margin = float(low_rule.get("margin", 0.0))
            return low_prob >= float(low_rule.get("prob_threshold", 0.6)) and (
                (low_prob - high_prob) >= margin
            )
        if low_rule.get("type") == "low_prob_and_reg_low":
            return low_prob >= float(low_rule.get("prob_threshold", 0.6)) and pred_low <= float(
                low_rule.get("pred_threshold", 40.0)
            )
        if low_rule.get("type") == "low_prob_and_reg_all":
            return low_prob >= float(low_rule.get("prob_threshold", 0.6)) and pred_all <= float(
                low_rule.get("pred_threshold", 40.0)
            )
        if low_rule.get("type") == "reg_all_low":
            return pred_all <= float(low_rule.get("pred_threshold", 40.0))
        if low_rule.get("type") == "reg_low":
            return pred_low <= float(low_rule.get("pred_threshold", 40.0))
        if low_rule.get("type") == "q90_low":
            return pred_q90 <= float(low_rule.get("pred_threshold", 40.0))
        if low_rule.get("type") == "low_prob_and_q90":
            return low_prob >= float(low_rule.get("prob_threshold", 0.6)) and pred_q90 <= float(
                low_rule.get("pred_threshold", 40.0)
            )
        return False

    def high_ok() -> bool:
        if high_rule.get("type") == "reg_all":
            return pred_all >= float(high_rule.get("pred_threshold", 56.0))
        if high_rule.get("type") == "high_prob_and_reg_high":
            return high_prob >= float(high_rule.get("prob_threshold", 0.65)) and pred_high >= float(
                high_rule.get("pred_threshold", 56.0)
            )
        if high_rule.get("type") == "high_prob":
            return high_prob >= float(high_rule.get("prob_threshold", 0.65))
        if high_rule.get("type") == "high_prob_margin":
            margin = float(high_rule.get("margin", 0.0))
            return high_prob >= float(high_rule.get("prob_threshold", 0.65)) and (
                (high_prob - low_prob) >= margin
            )
        if high_rule.get("type") == "high_prob_and_reg_all":
            return high_prob >= float(high_rule.get("prob_threshold", 0.65)) and pred_all >= float(
                high_rule.get("pred_threshold", 56.0)
            )
        if high_rule.get("type") == "reg_high":
            return pred_high >= float(high_rule.get("pred_threshold", 56.0))
        if high_rule.get("type") == "q10_high":
            return pred_q10 >= float(high_rule.get("pred_threshold", 56.0))
        if high_rule.get("type") == "high_prob_and_q10":
            return high_prob >= float(high_rule.get("prob_threshold", 0.65)) and pred_q10 >= float(
                high_rule.get("pred_threshold", 56.0)
            )
        return False

    low_signal = low_ok()
    high_signal = high_ok()

    bet = None
    if low_signal and not high_signal:
        bet = "LOW <40"
    elif high_signal and not low_signal:
        bet = "HIGH >50"

    filter_ok = True
    filter_reason = None
    filter_details: Dict[str, Any] = {}
    if bet:
        filter_ok, filter_reason, filter_details = _team_predictability_filter(
            radiant_team_id,
            dire_team_id,
            row,
            rules,
        )
        if not filter_ok:
            bet = None

    return {
        "pred_all": pred_all,
        "pred_low": pred_low,
        "pred_high": pred_high,
        "pred_q10": pred_q10,
        "pred_q90": pred_q90,
        "low_prob": low_prob,
        "high_prob": high_prob,
        "model_variant": model_variant,
        "duration_pred": duration_pred,
        "kpm_pred": kpm_pred,
        "duration_source": duration_source,
        "live_stats": live_stats,
        "bet": bet,
        "filter_ok": filter_ok,
        "filter_reason": filter_reason,
        "filter_details": filter_details,
    }



name_to_pos = {
        'Core': 'pos1',
        'Support': 'pos4',
        'Full Support': 'pos5',
        'Mid': 'pos2',
        'Offlane': 'pos3'
    }
headers = {
        "Host": "dltv.org",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Referer": 'https://dltv.org/results',
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/130.0.0.0 Safari/537.36", }

def _sync_processed_urls_cache(urls: Any) -> None:
    if not isinstance(urls, list):
        return
    normalized = {str(url) for url in urls if isinstance(url, str) and url}
    with processed_urls_lock:
        processed_urls_cache.clear()
        processed_urls_cache.update(normalized)


def _mark_url_processed(url: str) -> None:
    if not url:
        return
    with processed_urls_lock:
        processed_urls_cache.add(url)


def _is_url_processed(url: str) -> bool:
    if not url:
        return False
    with processed_urls_lock:
        if url in processed_urls_cache:
            return True
    try:
        with open(MAP_ID_CHECK_PATH, 'rb') as f:
            raw = f.read()
        data = orjson.loads(raw) if raw else []
        if isinstance(data, list) and url in data:
            _mark_url_processed(url)
            return True
    except Exception:
        return False
    return False


def _drop_delayed_match(match_key: str, reason: str = "") -> bool:
    if not match_key:
        return False
    with monitored_matches_lock:
        removed = monitored_matches.pop(match_key, None)
    if removed is not None:
        if reason:
            print(f"   🧹 Delayed очередь очищена для {match_key} ({reason})")
        else:
            print(f"   🧹 Delayed очередь очищена для {match_key}")
        return True
    return False


def _acquire_signal_send_slot(match_key: str) -> bool:
    if not match_key:
        return True
    with signal_send_guard_lock:
        if match_key in signal_send_guard:
            return False
        signal_send_guard.add(match_key)
    return True


def _release_signal_send_slot(match_key: str) -> None:
    if not match_key:
        return
    with signal_send_guard_lock:
        signal_send_guard.discard(match_key)


def _skip_dispatch_for_processed_url(match_key: str, context: str, indent: str = "   ") -> bool:
    if not match_key:
        return False
    if not _is_url_processed(match_key):
        return False
    print(f"{indent}⚠️ Пропуск {context}: URL уже обработан: {match_key}")
    _drop_delayed_match(match_key, reason=f"{context}_already_processed")
    return True


def add_url(url, reason: str = "unspecified", details: Any = None):
    if TEST_DISABLE_ADD_URL:
        print(f"   🧪 add_url(): TEST_DISABLE_ADD_URL=1, пропускаем запись URL: {url}")
        # Даже в тест-режиме считаем URL "обработанным" в рамках текущего процесса,
        # иначе delayed worker будет отправлять один и тот же матч циклически.
        _mark_url_processed(url)
        _drop_delayed_match(url, reason="test_mode_skip_file_write")
        _release_signal_send_slot(url)
        return
    print(f"   📝 add_url(): Добавляем URL: {url}")
    print(f"   📌 add_url(): reason={reason}")
    if details is not None:
        print(f"   📎 add_url(): details={details}")
    logger.info("ADD_URL reason=%s url=%s details=%s", reason, url, details)
    try:
        with open(MAP_ID_CHECK_PATH, 'rb+') as f:
            raw = f.read()
            data = orjson.loads(raw) if raw else []
            if not isinstance(data, list):
                raise ValueError(f"{MAP_ID_CHECK_PATH} должен содержать JSON-массив")
            already_present = url in data
            if not already_present:
                data.append(url)
            f.truncate(0)
            f.seek(0)
            f.write(orjson.dumps(data))
        if already_present:
            print(f"   ℹ️ add_url(): URL уже был в {MAP_ID_CHECK_PATH} (повторная запись не нужна)")
            logger.info("ADD_URL already_present url=%s reason=%s", url, reason)
        else:
            print(f"   ✅ add_url(): URL успешно добавлен в {MAP_ID_CHECK_PATH}")
        _mark_url_processed(url)
        _drop_delayed_match(url, reason="url_added_to_map_id_check")
        _release_signal_send_slot(url)
    except Exception as e:
        print(f"   ❌ add_url(): Ошибка добавления URL: {e}")
        raise
    
    # Очищаем историю для завершенных матчей
    # Извлекаем match_key из url (формат: dltv.org/path.score)
    match_key = url.rsplit('.', 1)[0] if '.' in url else url
    if match_key in match_history:
        del match_history[match_key]
        print(f"   🗑️ Очищена история для {match_key}")




def get_heads(response=None, MAX_RETRIES=5, RETRY_DELAY=5, ip_address="46.229.214.49", path = "/matches"):
        # Формируем URL всегда (нужен для retry с новым прокси)
        url = f"https://{ip_address}{path}"
        
        # Если response уже передан, используем его, иначе делаем запрос
        if response is None:
            # Используем глобальные headers
            global headers
            response = make_request_with_retry(url, MAX_RETRIES, RETRY_DELAY, headers=headers)
        
        if not response or response.status_code != 200:
            status_msg = f"Status: {response.status_code}" if response else "No response (None)"
            print(f"❌ Ошибка получения данных: {status_msg}")
            return None, None
        
        try:
            soup = BeautifulSoup(response.text, 'lxml')
            live_matches = soup.find('div', class_='live__matches')
            
            if not live_matches:
                print(f"❌ Не найден элемент live__matches в HTML")
                try:
                    send_message("❌ Не найден элемент live__matches в HTML")
                except Exception as e:
                    print(f"⚠️ Не удалось отправить уведомление в Telegram: {e}")
                # Меняем прокси и пробуем еще раз
                rotate_proxy()
                print(f"🔄 Переключился на другой прокси, повторяю запрос...")
                time.sleep(2)
                
                # Повторный запрос с новым прокси
                response = make_request_with_retry(url, max_retries=3, retry_delay=2, headers=headers)
                if response and response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'lxml')
                    live_matches = soup.find('div', class_='live__matches')
                    
                    if not live_matches:
                        print(f"❌ Элемент live__matches не найден и после смены прокси")
                        return None, None
                else:
                    print(f"❌ Не удалось получить данные после смены прокси")
                    return None, None
            
            heads = live_matches.find_all('div', class_='live__matches-item__head')
            bodies = live_matches.find_all('div', class_='live__matches-item__body')
            
            if not heads or not bodies:
                print(f"⚠️  Не найдены матчи (heads: {len(heads)}, bodies: {len(bodies)})")
                return [], []
            
            heads_copy, bodies_copy = heads.copy(), bodies.copy()
            for i in range(len(heads)):
                title = heads[i].find('div', class_='event__name').find('div').text
                # if not any(i in title.lower() for i in ['dreamleague', 'blast', 'dacha', 'betboom',
                #                                         'fissure', 'pgl', 'esports', 'international',
                #                                         'european', 'epl', 'esl', 'cct']):
                if any(i in title.lower() for i in ['lunar']):
                    heads_copy.remove(heads[i])
                    bodies_copy.remove(bodies[i])
            return heads_copy, bodies_copy
        except Exception as e:
            print(f"❌ Ошибка парсинга HTML: {e}")
            return None, None


def normalize_team_name(team_name):
    for i in trash_list:
        team_name = team_name.lower().replace(i, '')
    return team_name


def validate_heroes_data(radiant_heroes_and_pos, dire_heroes_and_pos, check_account_ids=True):
    """
    Валидация данных о героях. Возвращает (is_valid, error_message)
    
    Проверки:
    1. Обе команды имеют по 5 героев
    2. Все hero_id определены и != 0
    3. Все account_id определены и != 0 (если check_account_ids=True)
    """
    # Проверка 1: у каждой команды по 5 героев
    if len(radiant_heroes_and_pos) != 5 or len(dire_heroes_and_pos) != 5:
        return False, f"Неполные данные: radiant={len(radiant_heroes_and_pos)}, dire={len(dire_heroes_and_pos)}"
    
    # Проверка 2-3: все hero_id и account_id валидны
    for team_name, heroes_dict in [('radiant', radiant_heroes_and_pos), ('dire', dire_heroes_and_pos)]:
        for pos in ['pos1', 'pos2', 'pos3', 'pos4', 'pos5']:
            if pos not in heroes_dict:
                return False, f"Отсутствует {pos} для {team_name}"
            
            hero_id = heroes_dict[pos].get('hero_id')
            if not hero_id or hero_id == 0:
                return False, f"hero_id не определен для {team_name} {pos}"
            
            if check_account_ids:
                account_id = heroes_dict[pos].get('account_id', 0)
                if account_id == 0:
                    return False, f"account_id не определен для {team_name} {pos}"
    
    return True, None


def add_team_players_to_match(match_data, heroes_and_pos, is_radiant):
    """Добавляет игроков команды в структуру данных матча"""
    for pos in ['pos1', 'pos2', 'pos3', 'pos4', 'pos5']:
        if pos in heroes_and_pos:
            hero_id = heroes_and_pos[pos].get('hero_id')
            account_id = heroes_and_pos[pos].get('account_id', 0)
            if hero_id and hero_id != 0:
                match_data['players'].append({
                    'hero': {'id': hero_id},
                    'heroId': hero_id,
                    'isRadiant': is_radiant,
                    'steamAccount': {'id': account_id}
                })


def parse_draft_and_positions(soup, data, radiant_team_name, dire_team_name):
    """
    Парсит драфт и позиции игроков из HTML и JSON данных
    
    Returns:
        tuple: (
            radiant_heroes_and_pos,
            dire_heroes_and_pos,
            error_msg,
            problem_summary,
            problem_candidates,
        )
               Если error_msg не None - произошла ошибка
               problem_summary добавляется в основной сигнал (если есть дубль позиций)
    """
    from rapidfuzz import fuzz

    print("      🔍 parse_draft_and_positions(): начало")

    def is_same_team(name1, name2, threshold=70):
        return fuzz.ratio(name1, name2) >= threshold

    def _normalize_player_name(raw_name: str) -> str:
        return (raw_name or "").strip().lower()

    try:
        from functions import name_to_id as _name_to_id_fallback
        HERO_ID_TO_NAME_FALLBACK = {
            int(hero_id): str(hero_name)
            for hero_name, hero_id in _name_to_id_fallback.items()
            if isinstance(hero_name, str) and str(hero_id).isdigit()
        }
    except Exception:
        HERO_ID_TO_NAME_FALLBACK = {}

    def _hero_valid_positions(hero_id: int) -> list[str]:
        raw_positions = HERO_VALID_POSITIONS_DICT.get(str(hero_id)) or HERO_VALID_POSITIONS_DICT.get(hero_id)
        if not raw_positions:
            return []
        if isinstance(raw_positions, str):
            raw_positions = [raw_positions]
        result: list[str] = []
        for item in raw_positions:
            s = str(item).strip().upper()
            if not s.startswith("POSITION_"):
                continue
            try:
                idx = int(s.replace("POSITION_", ""))
            except ValueError:
                continue
            if 1 <= idx <= 5:
                result.append(f"pos{idx}")
        return result

    def _hero_position_counts(hero_id: int) -> dict:
        raw = HERO_POSITION_COUNTS.get(str(hero_id)) or HERO_POSITION_COUNTS.get(hero_id) or {}
        if not isinstance(raw, dict):
            return {}
        out = {}
        for pos_key, value in raw.items():
            s = str(pos_key).strip().upper()
            if not s.startswith("POSITION_"):
                continue
            try:
                n = int(s.replace("POSITION_", ""))
                count = int(value)
            except (TypeError, ValueError):
                continue
            if 1 <= n <= 5 and count > 0:
                out[f"POSITION_{n}"] = count
        return out

    def _hero_position_share(hero_id: int, pos: str) -> float:
        counts = _hero_position_counts(hero_id)
        total = sum(counts.values())
        if total <= 0:
            return 0.0
        try:
            pos_key = f"POSITION_{int(str(pos)[-1])}"
        except (ValueError, TypeError):
            return 0.0
        return float(counts.get(pos_key, 0)) / float(total)

    def _hero_flexibility(hero_id: int) -> int:
        counts = _hero_position_counts(hero_id)
        if counts:
            total = float(sum(counts.values()))
            strong_positions = 0
            for pos_key in POSITION_ORDER:
                c = float(counts.get(pos_key, 0))
                if c >= 100 and (c / total) >= 0.08:
                    strong_positions += 1
            if strong_positions > 0:
                return strong_positions
        valid_positions = _hero_valid_positions(hero_id)
        return len(valid_positions) if valid_positions else 5

    def _hero_name_by_id(hero_id: int) -> str:
        if hero_id <= 0:
            return f"Unknown({hero_id})"
        return HERO_ID_TO_NAME.get(str(hero_id)) or HERO_ID_TO_NAME.get(hero_id) or f"Unknown({hero_id})"

    def _hero_name_for_signal(hero_id: int) -> str:
        raw_name = _hero_name_by_id(hero_id)
        if isinstance(raw_name, str):
            normalized = raw_name.strip()
            if normalized and not normalized.startswith("{"):
                return normalized
        fallback_name = HERO_ID_TO_NAME_FALLBACK.get(int(hero_id))
        if fallback_name:
            return fallback_name
        return f"Unknown({hero_id})"

    def _hero_pos_score(hero_id: int, pos: str) -> int:
        # Приоритет: частотная модель на 500k матчей (share + reliability).
        share = _hero_position_share(hero_id, pos)
        if share > 0:
            counts = _hero_position_counts(hero_id)
            try:
                pos_key = f"POSITION_{int(str(pos)[-1])}"
            except (ValueError, TypeError):
                pos_key = None
            count = float(counts.get(pos_key, 0)) if pos_key else 0.0
            return int(share * 1_000_000 + min(count, 20_000.0))

        # Fallback: старый simple-словарь валидных позиций.
        positions = _hero_valid_positions(hero_id)
        if not positions:
            return 0
        if pos in positions:
            return 100 - positions.index(pos)
        return -100

    def _best_free_position(hero_id: int, free_positions: list[str]) -> Optional[str]:
        if not free_positions:
            return None
        return max(free_positions, key=lambda p: (_hero_pos_score(hero_id, p), -int(p[-1])))

    def _assign_to_missing_placeholder(
        team_label: str,
        names_pos: dict,
        heroes_and_pos: dict,
        pos_list: list[str],
        missing_hint: set[str],
        hero_id: int,
        account_id: int,
        player_name: str,
    ) -> bool:
        for placeholder_name, placeholder_pos in list(names_pos.items()):
            if not placeholder_name.startswith("__missing_"):
                continue
            if placeholder_pos not in pos_list:
                continue
            heroes_and_pos[placeholder_pos] = {
                "hero_id": hero_id,
                "account_id": account_id,
                "_player_name": player_name,
            }
            pos_list.remove(placeholder_pos)
            missing_hint.discard(placeholder_pos)
            del names_pos[placeholder_name]
            print(f"            ✅ Добавлен в {team_label} {placeholder_pos} (недостающий)")
            return True
        return False

    def _assign_player_to_team(
        team_label: str,
        names_pos: dict,
        heroes_and_pos: dict,
        pos_list: list[str],
        missing_hint: set[str],
        problem_positions: set[str],
        player_name: str,
        hero_id: int,
        account_id: int,
    ) -> bool:
        if player_name not in names_pos:
            return False

        target_pos = names_pos[player_name]

        entry = {"hero_id": hero_id, "account_id": account_id, "_player_name": player_name}

        # Нормальный кейс: позиция свободна.
        if target_pos not in heroes_and_pos:
            heroes_and_pos[target_pos] = entry
            if target_pos in pos_list:
                pos_list.remove(target_pos)
            print(f"            ✅ Добавлен в {team_label} {target_pos}")
            return True

        # Конфликт: одна и та же позиция назначена двум игрокам.
        existing_entry = heroes_and_pos[target_pos]
        existing_name = existing_entry.get("_player_name", "unknown")
        free_positions = [p for p in pos_list if p != target_pos]
        if not free_positions:
            print(
                f"            ❌ Не удалось разрулить дубль в {team_label}: "
                f"нет свободной позиции (target={target_pos})"
            )
            return False

        existing_target_score = _hero_pos_score(existing_entry["hero_id"], target_pos)
        new_target_score = _hero_pos_score(hero_id, target_pos)
        keep_new_on_target = new_target_score > existing_target_score
        if new_target_score == existing_target_score:
            existing_flex = _hero_flexibility(existing_entry["hero_id"])
            new_flex = _hero_flexibility(hero_id)
            if new_flex != existing_flex:
                # Менее гибкого героя оставляем на конфликтной позиции.
                keep_new_on_target = new_flex < existing_flex
            else:
                existing_best_alt = _hero_pos_score(
                    existing_entry["hero_id"], _best_free_position(existing_entry["hero_id"], free_positions)
                )
                new_best_alt = _hero_pos_score(hero_id, _best_free_position(hero_id, free_positions))
                # При полном равенстве оставляем того, кому хуже подходит любой альтернативный слот.
                keep_new_on_target = existing_best_alt > new_best_alt

        if keep_new_on_target:
            displaced_entry = existing_entry
            displaced_name = existing_name
            heroes_and_pos[target_pos] = entry
            keeper_name = player_name
            keeper_hero = hero_id
        else:
            displaced_entry = entry
            displaced_name = player_name
            keeper_name = existing_name
            keeper_hero = existing_entry["hero_id"]

        hinted_free_positions = [p for p in free_positions if p in missing_hint]
        candidate_positions = hinted_free_positions if hinted_free_positions else free_positions
        alt_pos = _best_free_position(displaced_entry["hero_id"], candidate_positions)
        if alt_pos is None:
            print(f"            ❌ Не удалось найти альтернативную позицию для {displaced_name}")
            return False

        heroes_and_pos[alt_pos] = displaced_entry
        if alt_pos in pos_list:
            pos_list.remove(alt_pos)
        missing_hint.discard(alt_pos)
        problem_positions.add(target_pos)
        problem_positions.add(alt_pos)
        print(
            f"            🔀 DUP FIX {team_label}: target={target_pos} "
            f"оставлен за {keeper_name}(hero={keeper_hero}), "
            f"{displaced_name}(hero={displaced_entry['hero_id']}) -> {alt_pos}"
        )
        return True

    def _remove_temp_player_names(heroes_and_pos: dict) -> None:
        for payload in heroes_and_pos.values():
            if isinstance(payload, dict):
                payload.pop("_player_name", None)

    def _build_problem_team_summary(
        team_key: str,
        team_title: str,
        heroes_and_pos: dict,
        problem_positions: set[str],
    ) -> str:
        lines = []
        ordered_problem_positions = sorted(
            [p for p in problem_positions if p in {"pos1", "pos2", "pos3", "pos4", "pos5"}],
            key=lambda x: int(x[-1]),
        )
        for pos in ordered_problem_positions:
            payload = heroes_and_pos.get(pos) or {}
            hero_id = int(payload.get("hero_id", 0) or 0)
            hero_name = _hero_name_by_id(hero_id)
            lines.append(f"{pos} - {hero_name} ({hero_id})")
        line2 = "; ".join(lines) if lines else "none"
        return f"⚠️{team_key}_problem_positions:\n{line2}"

    def _build_problem_candidates(
        team_key: str,
        heroes_and_pos: dict,
        problem_positions: set[str],
        ensure_min: int = 2,
    ) -> list[dict]:
        valid_positions = {"pos1", "pos2", "pos3", "pos4", "pos5"}
        selected_positions = sorted(
            {p for p in problem_positions if p in valid_positions},
            key=lambda p: int(p[-1]),
        )
        if len(selected_positions) < ensure_min:
            fallback_positions = sorted(
                [p for p in heroes_and_pos.keys() if p in valid_positions and p not in selected_positions],
                key=lambda p: (
                    _hero_pos_score(int((heroes_and_pos.get(p) or {}).get("hero_id", 0) or 0), p),
                    int(p[-1]),
                ),
            )
            need = max(0, ensure_min - len(selected_positions))
            selected_positions.extend(fallback_positions[:need])
        candidates = []
        for pos in selected_positions:
            payload = heroes_and_pos.get(pos) or {}
            hero_id = int(payload.get("hero_id", 0) or 0)
            candidates.append(
                {
                    "team_key": team_key,
                    "position": pos,
                    "hero_id": hero_id,
                    "hero_name": _hero_name_for_signal(hero_id),
                    "score": int(_hero_pos_score(hero_id, pos)),
                }
            )
        return candidates

    ROLE_TO_POS = {
        "Core": "pos1",
        "Mid": "pos2",
        "Offlane": "pos3",
        "Support": "pos4",
        "Full Support": "pos5",
    }

    # Нормализуем имена команд
    radiant_team_name_raw = radiant_team_name
    dire_team_name_raw = dire_team_name
    radiant_team_name = normalize_team_name(radiant_team_name_raw)
    dire_team_name = normalize_team_name(dire_team_name_raw)

    print(f"      Ищем команды: '{radiant_team_name}' (radiant), '{dire_team_name}' (dire)")

    # Парсим lineup из HTML - получаем соответствие имя -> позиция
    teams = soup.find_all("div", class_="lineups__team")
    print(f"      Найдено {len(teams)} команд в HTML lineups")

    radiant_names_pos, dire_names_pos = {}, {}
    radiant_duplicate_detected = False
    dire_duplicate_detected = False
    radiant_problem_positions: set[str] = set()
    dire_problem_positions: set[str] = set()

    for team in teams:
        title_tag = team.find("span", class_="title")
        if not title_tag:
            continue
        team_name = normalize_team_name(title_tag.text.strip())
        names = team.find_all("div", class_="player__name-name")
        poses = team.find_all("div", class_="player__role-item")

        print(f"      Команда в HTML: '{team_name}' ({len(names)} игроков)")

        if is_same_team(team_name, radiant_team_name):
            print("      ✅ Это Radiant команда")
            for name_tag, pos_tag in zip(names, poses):
                role_name = pos_tag.text.strip()
                if role_name not in ROLE_TO_POS:
                    continue
                pos = ROLE_TO_POS[role_name]
                player_name = _normalize_player_name(name_tag.text)
                if pos in radiant_names_pos.values():
                    existing_player = [n for n, p in radiant_names_pos.items() if p == pos][0]
                    radiant_duplicate_detected = True
                    radiant_problem_positions.add(pos)
                    print(
                        f"      ⚠️ ДУБЛЬ ПОЗИЦИИ в Radiant: {pos} "
                        f"({existing_player} vs {player_name}) - разрулим по hero_valid_positions_simple.json"
                    )
                radiant_names_pos[player_name] = pos
        elif is_same_team(team_name, dire_team_name):
            print("      ✅ Это Dire команда")
            for name_tag, pos_tag in zip(names, poses):
                role_name = pos_tag.text.strip()
                if role_name not in ROLE_TO_POS:
                    continue
                pos = ROLE_TO_POS[role_name]
                player_name = _normalize_player_name(name_tag.text)
                if pos in dire_names_pos.values():
                    existing_player = [n for n, p in dire_names_pos.items() if p == pos][0]
                    dire_duplicate_detected = True
                    dire_problem_positions.add(pos)
                    print(
                        f"      ⚠️ ДУБЛЬ ПОЗИЦИИ в Dire: {pos} "
                        f"({existing_player} vs {player_name}) - разрулим по hero_valid_positions_simple.json"
                    )
                dire_names_pos[player_name] = pos

    print(f"      Radiant игроков в HTML: {len(radiant_names_pos)}")
    print(f"      Dire игроков в HTML: {len(dire_names_pos)}")

    # Проверка: у обеих команд должно быть по 5 игроков (или 4, тогда дополним)
    if len(radiant_names_pos) < 4 or len(dire_names_pos) < 4:
        print("      ❌ Слишком мало игроков")
        return None, None, f"Слишком мало игроков: radiant={len(radiant_names_pos)}, dire={len(dire_names_pos)}", "", []

    # Если не хватает 1 игрока - дополним позицию
    all_positions = {"pos1", "pos2", "pos3", "pos4", "pos5"}

    radiant_missing_hint = set(all_positions - set(radiant_names_pos.values()))
    dire_missing_hint = set(all_positions - set(dire_names_pos.values()))

    if len(radiant_names_pos) == 4:
        missing_pos = list(all_positions - set(radiant_names_pos.values()))[0]
        print(f"      ⚠️  Radiant: недостает позиции {missing_pos}, добавим заглушку")
        radiant_names_pos[f"__missing_{missing_pos}__"] = missing_pos
    elif len(radiant_names_pos) == 5:
        missing_positions = sorted(all_positions - set(radiant_names_pos.values()))
        if missing_positions:
            print(f"      ⚠️  Radiant: отсутствуют позиции {missing_positions}, ожидаем дубль роли")

    if len(dire_names_pos) == 4:
        missing_pos = list(all_positions - set(dire_names_pos.values()))[0]
        print(f"      ⚠️  Dire: недостает позиции {missing_pos}, добавим заглушку")
        dire_names_pos[f"__missing_{missing_pos}__"] = missing_pos
    elif len(dire_names_pos) == 5:
        missing_positions = sorted(all_positions - set(dire_names_pos.values()))
        if missing_positions:
            print(f"      ⚠️  Dire: отсутствуют позиции {missing_positions}, ожидаем дубль роли")

    # Парсим fast_picks из JSON - соотносим игроков с героями и account_id
    print("      🔍 Парсим fast_picks из JSON...")
    radiant_heroes_and_pos, dire_heroes_and_pos = {}, {}
    radiant_pos_list = ["pos1", "pos2", "pos3", "pos4", "pos5"].copy()
    dire_pos_list = ["pos1", "pos2", "pos3", "pos4", "pos5"].copy()
    leftover = None

    players_data = data.get("players", [])

    def _find_account_id_by_hero(hero_id: int) -> int:
        for p in players_data:
            if p.get("hero_id") == hero_id:
                return int(p.get("account_id", 0) or 0)
        return 0

    def _process_player(player: dict, prefer_radiant_first: bool) -> None:
        nonlocal leftover
        player_name = _normalize_player_name((player.get("player") or {}).get("title", ""))
        hero_id = int(player.get("hero_id", 0) or 0)
        print(f"         - {player_name}: hero_id={hero_id}")
        if hero_id <= 0:
            print("            ⚠️  hero_id отсутствует, пропуск")
            return

        account_id = _find_account_id_by_hero(hero_id)

        team_order = ["radiant", "dire"] if prefer_radiant_first else ["dire", "radiant"]
        assigned = False
        for team_label in team_order:
            if team_label == "radiant":
                if player_name not in radiant_names_pos:
                    continue
                assigned = _assign_player_to_team(
                    "radiant",
                    radiant_names_pos,
                    radiant_heroes_and_pos,
                    radiant_pos_list,
                    radiant_missing_hint,
                    radiant_problem_positions,
                    player_name,
                    hero_id,
                    account_id,
                )
            else:
                if player_name not in dire_names_pos:
                    continue
                assigned = _assign_player_to_team(
                    "dire",
                    dire_names_pos,
                    dire_heroes_and_pos,
                    dire_pos_list,
                    dire_missing_hint,
                    dire_problem_positions,
                    player_name,
                    hero_id,
                    account_id,
                )
            if assigned:
                break

        if assigned:
            return

        # Проверяем, есть ли заглушка для недостающей позиции.
        found_missing = False
        if prefer_radiant_first:
            found_missing = _assign_to_missing_placeholder(
                "radiant",
                radiant_names_pos,
                radiant_heroes_and_pos,
                radiant_pos_list,
                radiant_missing_hint,
                hero_id,
                account_id,
                player_name,
            )
            if not found_missing:
                found_missing = _assign_to_missing_placeholder(
                    "dire",
                    dire_names_pos,
                    dire_heroes_and_pos,
                    dire_pos_list,
                    dire_missing_hint,
                    hero_id,
                    account_id,
                    player_name,
                )
        else:
            found_missing = _assign_to_missing_placeholder(
                "dire",
                dire_names_pos,
                dire_heroes_and_pos,
                dire_pos_list,
                dire_missing_hint,
                hero_id,
                account_id,
                player_name,
            )
            if not found_missing:
                found_missing = _assign_to_missing_placeholder(
                    "radiant",
                    radiant_names_pos,
                    radiant_heroes_and_pos,
                    radiant_pos_list,
                    radiant_missing_hint,
                    hero_id,
                    account_id,
                    player_name,
                )

        if not found_missing:
            leftover = hero_id
            print("            ⚠️  Игрок не найден в составах - leftover")

    # Обрабатываем first_team
    first_team = (data.get("fast_picks") or {}).get("first_team") or []
    print(f"      first_team: {len(first_team)} героев")
    for player in first_team:
        _process_player(player, prefer_radiant_first=True)

    # Обрабатываем second_team
    second_team = (data.get("fast_picks") or {}).get("second_team") or []
    print(f"      second_team: {len(second_team)} героев")
    for player in second_team:
        _process_player(player, prefer_radiant_first=False)

    # Проверка минимального количества героев
    total_heroes = len(radiant_heroes_and_pos) + len(dire_heroes_and_pos)
    print(f"      Итого героев: radiant={len(radiant_heroes_and_pos)}, dire={len(dire_heroes_and_pos)}, total={total_heroes}")

    if total_heroes < 9:
        print("      ❌ Слишком мало героев")
        return None, None, f"Слишком мало героев определено: {total_heroes}/10", "", []

    # Если не хватает одного героя и есть leftover или остались заглушки, добавляем
    if total_heroes == 9:
        if leftover:
            print(f"      ⚠️  Используем leftover hero_id={leftover}")
            if len(dire_pos_list) == 1:
                dire_heroes_and_pos[dire_pos_list[0]] = {"hero_id": leftover, "account_id": 0}
                dire_pos_list.remove(dire_pos_list[0])
                print(f"         ✅ Добавлен в dire {list(dire_heroes_and_pos.keys())[-1]}")
            elif len(radiant_pos_list) == 1:
                radiant_heroes_and_pos[radiant_pos_list[0]] = {"hero_id": leftover, "account_id": 0}
                radiant_pos_list.remove(radiant_pos_list[0])
                print(f"         ✅ Добавлен в radiant {list(radiant_heroes_and_pos.keys())[-1]}")
        else:
            print("      ⚠️  Leftover нет, но героев только 9. Проверяем заглушки...")
            for placeholder_name in list(radiant_names_pos.keys()):
                if placeholder_name.startswith("__missing_"):
                    print(f"         ❌ Radiant: осталась необработанная заглушка {placeholder_name}")
            for placeholder_name in list(dire_names_pos.keys()):
                if placeholder_name.startswith("__missing_"):
                    print(f"         ❌ Dire: осталась необработанная заглушка {placeholder_name}")

    # Финальная проверка
    final_rad = len(radiant_heroes_and_pos)
    final_dire = len(dire_heroes_and_pos)
    final_total = final_rad + final_dire

    print(f"      📊 ФИНАЛЬНЫЙ СОСТАВ: radiant={final_rad}/5, dire={final_dire}/5, total={final_total}/10")

    if final_total == 10:
        print("      ✅ parse_draft_and_positions(): завершено успешно (все 10 героев)")
    elif final_total == 9:
        print("      ⚠️  parse_draft_and_positions(): завершено с 9/10 героями (можно продолжить)")
    else:
        print("      ❌ parse_draft_and_positions(): недостаточно героев")
        return None, None, f"Недостаточно героев: {final_total}/10", "", []

    _remove_temp_player_names(radiant_heroes_and_pos)
    _remove_temp_player_names(dire_heroes_and_pos)

    problem_sections = []
    problem_candidates = []
    if radiant_duplicate_detected:
        problem_sections.append(
            _build_problem_team_summary(
                "radiant",
                radiant_team_name_raw,
                radiant_heroes_and_pos,
                radiant_problem_positions,
            )
        )
        problem_candidates.extend(
            _build_problem_candidates(
                "radiant",
                radiant_heroes_and_pos,
                radiant_problem_positions,
                ensure_min=2,
            )
        )
    if dire_duplicate_detected:
        problem_sections.append(
            _build_problem_team_summary(
                "dire",
                dire_team_name_raw,
                dire_heroes_and_pos,
                dire_problem_positions,
            )
        )
        problem_candidates.extend(
            _build_problem_candidates(
                "dire",
                dire_heroes_and_pos,
                dire_problem_positions,
                ensure_min=2,
            )
        )
    problem_summary = "\n\n".join(problem_sections)
    return radiant_heroes_and_pos, dire_heroes_and_pos, None, problem_summary, problem_candidates


def check_head(heads, bodies, i, maps_data, return_status=None):
        # Глобальные переменные для модели киллов и enhanced predictor
        global kills_model_data, kills_stats, enhanced_predictor, monitored_matches
        
        # Константы вынесены в начало
        IP_ADDRESS = "46.229.214.49"
        MAX_RETRIES = 5
        RETRY_DELAY = 5

        # Проверка статуса матча
        status_element = heads[i].find('div', class_='event__info-info__time')
        status = status_element.text.lower() if status_element else 'unknown'
        
        print(f"\n🔍 DEBUG: Начало обработки матча #{i}")
        print(f"   Статус: {status}")

        if return_status != 'draft...':
            return_status = status
        if status == 'finished':
            print(f"   ❌ Матч завершен - пропускаем")
            print(f"   ℹ️ Матч завершен")
            return



        # Извлечение данных
        try:
            score_divs = bodies[i].find_all('div', class_='match__item-team__score')
            uniq_score = sum(int(div.text.strip()) for div in score_divs[:2])
            score = f"{score_divs[:2][0].text.strip()} : {score_divs[:2][1].text.strip()}"
            link_tag = bodies[i].find('a')
            href = link_tag['href']
            parsed_url = urlparse(href)
            path = parsed_url.path
            check_uniq_url = f'dltv.org{path}.{uniq_score}'
            
            print(f"   URL: {check_uniq_url}")
            print(f"   Score: {score}")
            
            if check_uniq_url in maps_data or _is_url_processed(check_uniq_url):
                print(f"   ✅ Матч уже в map_id_check.txt - пропускаем")
                _drop_delayed_match(check_uniq_url, reason="already_in_map_id_check")
                return
            with monitored_matches_lock:
                delayed_payload = monitored_matches.get(check_uniq_url)
            if delayed_payload is not None:
                target_game_time = float(
                    delayed_payload.get('target_game_time', DELAYED_SIGNAL_TARGET_GAME_TIME)
                )
                target_human = f"{int(target_game_time // 60):02d}:{int(target_game_time % 60):02d}"
                last_game_time = delayed_payload.get('last_game_time', delayed_payload.get('queued_game_time'))
                try:
                    last_game_time_human = str(int(float(last_game_time)))
                except (TypeError, ValueError):
                    last_game_time_human = "n/a"
                queue_reason = str(delayed_payload.get('reason', 'unknown'))
                queue_status_label = str(delayed_payload.get("dispatch_status_label") or "")
                monitor_threshold_raw = delayed_payload.get("networth_monitor_threshold")
                monitor_suffix = ""
                try:
                    if monitor_threshold_raw is not None:
                        monitor_side = str(delayed_payload.get("networth_target_side") or "").strip().lower() or "unknown"
                        monitor_suffix = f", monitor={monitor_side}>={int(float(monitor_threshold_raw))}"
                except (TypeError, ValueError):
                    monitor_suffix = ""
                print(
                    "   ⏳ Матч уже в delayed-очереди - пропускаем повторный расчет "
                    f"(target={target_human}, last_game_time={last_game_time_human}, "
                    f"reason={queue_reason}, status={queue_status_label or 'n/a'}{monitor_suffix})"
                )
                return return_status


        except (AttributeError, KeyError, ValueError) as e:
            print(f"   ❌ Ошибка при парсинге данных: {e}")
            print(f"   ❌ Матч пропущен (ошибка парсинга URL/score)")
            return return_status

        # HTTP запрос
        url = f"https://{IP_ADDRESS}{path}"
        print(f"   🌐 Запрос страницы матча...")
        response = make_request_with_retry(url, MAX_RETRIES, RETRY_DELAY)

        if not response or response.status_code != 200:
            print(f"   ❌ Не удалось получить страницу. Status code: {response.status_code if response else 'No response'}")
            print(f"   ❌ Матч пропущен (ошибка HTTP запроса)")
            return return_status

        print(f"   ✅ Страница получена")
        soup = BeautifulSoup(response.text, 'lxml')

        from urllib.parse import urljoin
        import re
        m = re.search(r"\$\.get\(['\"](?P<path>/live/[^'\"]+\.json)['\"]", response.text)
        if not m:
            print(f"   ❌ Не найден JSON путь в HTML")
            print(f"   ❌ Матч пропущен (нет JSON пути)")
            return return_status
        json_path = m.group('path')
        base = "https://dltv.org"  # замениш на реальный сайт, откуда страница
        json_url = urljoin(base, json_path)
        
        print(f"   🌐 Запрос JSON данных...")

        # Получаем JSON данные с retry логикой
        data = None
        max_json_retries = 3
        json_retry_errors = []
        for json_attempt in range(max_json_retries):
            attempt_no = json_attempt + 1
            try:
                resp = requests.get(json_url, proxies=PROXIES, timeout=10)
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                    except Exception as e:
                        preview = (resp.text or "").strip().replace("\n", " ")[:140]
                        err_msg = (
                            f"attempt={attempt_no}: invalid-json ({e}); "
                            f"preview={preview!r}"
                        )
                        json_retry_errors.append(err_msg)
                        logger.warning(
                            "Ошибка парсинга JSON (попытка %s/%s): %s",
                            attempt_no,
                            max_json_retries,
                            err_msg,
                        )
                        print(f"   ⚠️  Ошибка парсинга JSON: {e}")
                        if preview:
                            print(f"   🔎 JSON preview: {preview!r}")
                        if json_attempt < max_json_retries - 1:
                            rotate_proxy()
                            time.sleep(2)
                        continue
                    print(f"   ✅ JSON данные получены")
                    if json_retry_errors:
                        retries_summary = " | ".join(json_retry_errors)
                        print(
                            f"   ℹ️ JSON получен после попытки {attempt_no}/{max_json_retries}; "
                            f"предыдущие ошибки: {retries_summary}"
                        )
                        logger.info(
                            "JSON_RECOVERED url=%s attempt=%s/%s previous_errors=%s",
                            check_uniq_url,
                            attempt_no,
                            max_json_retries,
                            retries_summary,
                        )
                    break
                elif resp.status_code == 429:
                    err_msg = f"attempt={attempt_no}: status=429"
                    json_retry_errors.append(err_msg)
                    logger.warning(f"429 при получении JSON, меняем прокси (попытка {attempt_no}/{max_json_retries})")
                    print(f"   ⚠️  429: Too Many Requests - меняем прокси")
                    rotate_proxy()
                    time.sleep(3)
                else:
                    err_msg = f"attempt={attempt_no}: status={resp.status_code}"
                    json_retry_errors.append(err_msg)
                    logger.warning(f"Статус {resp.status_code} при получении JSON (попытка {attempt_no}/{max_json_retries})")
                    if json_attempt < max_json_retries - 1:
                        rotate_proxy()
                        time.sleep(2)
            except Exception as e:
                err_msg = f"attempt={attempt_no}: request-exception ({e})"
                json_retry_errors.append(err_msg)
                logger.warning(f"Ошибка получения JSON (попытка {attempt_no}/{max_json_retries}): {e}")
                print(f"   ⚠️  Ошибка получения JSON: {e}")
                if json_attempt < max_json_retries - 1:
                    rotate_proxy()
                    time.sleep(2)
        
        if data is None:
            logger.error("Не удалось получить JSON данные после всех попыток")
            print(f"   ❌ Не удалось получить JSON данные")
            if json_retry_errors:
                print(f"   📉 Сводка JSON ошибок: {' | '.join(json_retry_errors)}")
            print(f"   ❌ Матч пропущен (ошибка получения JSON)")
            return return_status
        
        if 'fast_picks' not in data:
            print(f"   ❌ Нет 'fast_picks' в данных - драфт не начался")
            print(f"   ℹ️ Драфт еще не начался")
            return return_status
        
        print(f"   ✅ fast_picks найдены - драфт начался")
        
        # Определяем какая команда radiant, какая dire
        db_payload = data.get('db') or {}
        first_team_payload = db_payload.get('first_team') or {}
        second_team_payload = db_payload.get('second_team') or {}
        if first_team_payload.get('is_radiant'):
            radiant_team_name_original = first_team_payload.get('title') or ""
            dire_team_name_original = second_team_payload.get('title') or ""
            radiant_db_team_payload = first_team_payload
            dire_db_team_payload = second_team_payload
        else:
            dire_team_name_original = first_team_payload.get('title') or ""
            radiant_team_name_original = second_team_payload.get('title') or ""
            dire_db_team_payload = first_team_payload
            radiant_db_team_payload = second_team_payload

        live_league_data = data.get('live_league_data') or {}
        live_match_payload = live_league_data.get('match') or {}
        radiant_live_team_payload = live_league_data.get('radiant_team') or {}
        dire_live_team_payload = live_league_data.get('dire_team') or {}

        radiant_team_ids = _extract_candidate_team_ids(
            radiant_live_team_payload,
            radiant_live_team_payload.get('team_id'),
            radiant_live_team_payload.get('team_ids'),
            live_league_data.get('radiant_team_id'),
            live_league_data.get('radiant_team_ids'),
            live_match_payload.get('radiant_team_id'),
            live_match_payload.get('radiant_team_ids'),
            radiant_db_team_payload,
            radiant_db_team_payload.get('team_id'),
            radiant_db_team_payload.get('team_ids'),
            radiant_db_team_payload.get('id'),
        )
        dire_team_ids = _extract_candidate_team_ids(
            dire_live_team_payload,
            dire_live_team_payload.get('team_id'),
            dire_live_team_payload.get('team_ids'),
            live_league_data.get('dire_team_id'),
            live_league_data.get('dire_team_ids'),
            live_match_payload.get('dire_team_id'),
            live_match_payload.get('dire_team_ids'),
            dire_db_team_payload,
            dire_db_team_payload.get('team_id'),
            dire_db_team_payload.get('team_ids'),
            dire_db_team_payload.get('id'),
        )

        print(f"   🆔 Candidate team IDs: radiant={radiant_team_ids}, dire={dire_team_ids}")
        if not radiant_team_ids or not dire_team_ids:
            print(f"   ❌ Отсутствуют team_id для команд")
            print(f"   ❌ Матч пропущен (нет team_id)")
            return return_status
        # Extract league_id if available
        league_id = live_league_data.get('league_id')
        series_id = live_league_data.get('series_id')
        
        # Debug: print available keys in live_league_data
        lld_keys = list(live_league_data.keys())
        print(f"   📋 live_league_data keys: {lld_keys}")
        if league_id:
            print(f"   🏆 League ID: {league_id}")
        if series_id:
            print(f"   📊 Series ID: {series_id}")
        
        # Сохраняем нормализованные имена для обратной совместимости
        radiant_team_name = normalize_team_name(radiant_team_name_original)
        dire_team_name = normalize_team_name(dire_team_name_original)

        # Стартуем prefetch кэфов ДО анализа драфта (в отдельном воркере, без блокировки основного пайплайна).
        # Ключевой момент: парсим именно текущую карту серии, а не "1-я карта" по умолчанию.
        if BOOKMAKER_PREFETCH_ENABLED:
            bookmaker_map_num = _bookmaker_infer_map_num(live_league_data, score_text=score)
            if bookmaker_map_num is not None:
                print(f"   🗺️ Bookmaker map context: карта {bookmaker_map_num}")
            _bookmaker_prefetch_submit(
                match_key=check_uniq_url,
                radiant_team=radiant_team_name_original,
                dire_team=dire_team_name_original,
                map_num=bookmaker_map_num,
            )

        # Tier-режим для star-сигналов:
        # - Tier 2 матч: если хотя бы одна команда Tier 2
        # - Tier 1 матч: если обе команды Tier 1
        # - Неизвестная команда автоматически добавляется в Tier 2 + Telegram уведомление
        radiant_ok, radiant_team_id = _ensure_known_team_or_add_to_tier2(
            radiant_team_ids,
            radiant_team_name_original,
            check_uniq_url,
        )
        if not radiant_ok:
            print(f"   ❌ Матч пропущен (radiant команда не добавлена в Tier 2)")
            print("   ℹ️ map_id_check.txt не обновлен: add_url только после send_message()")
            return return_status
        dire_ok, dire_team_id = _ensure_known_team_or_add_to_tier2(
            dire_team_ids,
            dire_team_name_original,
            check_uniq_url,
        )
        if not dire_ok:
            print(f"   ❌ Матч пропущен (dire команда не добавлена в Tier 2)")
            print("   ℹ️ map_id_check.txt не обновлен: add_url только после send_message()")
            return return_status

        star_match_tier = _determine_star_signal_match_tier(radiant_team_id, dire_team_id)
        if star_match_tier is None:
            skip_msg = (
                "🚫 Пропуск матча: не удалось определить tier матча после авто-добавления.\n"
                f"{radiant_team_name_original} ({radiant_team_id}) vs "
                f"{dire_team_name_original} ({dire_team_id})\n"
                f"{check_uniq_url}"
            )
            print(f"   {skip_msg}")
            print(f"   ❌ Матч пропущен (не удалось определить tier)")
            send_message(skip_msg)
            add_url(
                check_uniq_url,
                reason="skip_tier_undetermined",
                details={
                    "status": status,
                    "radiant_team": radiant_team_name_original,
                    "radiant_team_id": radiant_team_id,
                    "dire_team": dire_team_name_original,
                    "dire_team_id": dire_team_id,
                    "json_retry_errors": json_retry_errors,
                },
            )
            return return_status

        star_target_wr = (
            TIER_SIGNAL_MIN_THRESHOLD_TIER2
            if star_match_tier == 2
            else TIER_SIGNAL_MIN_THRESHOLD_TIER1
        )
        tier_threshold_block_status_label = (
            TIER_THRESHOLD_STATUS_TIER2_MIN65_BLOCK
            if star_match_tier == 2
            else TIER_THRESHOLD_STATUS_TIER1_MIN60_BLOCK
        )
        tier_threshold_block_reason_label = (
            TIER_THRESHOLD_REASON_TIER2_MIN65_BLOCK
            if star_match_tier == 2
            else TIER_THRESHOLD_REASON_TIER1_MIN60_BLOCK
        )
        print(f"   🧭 Star tier mode: tier={star_match_tier}, min_wr={star_target_wr}%")

        lead = data['radiant_lead']
        game_time = data['game_time']
        print(f"   Lead: {lead}, Game time: {game_time}")
        
        # Парсим драфт и позиции - вся логика в отдельной функции
        print(f"   🔍 Парсинг драфта и позиций...")
        radiant_heroes_and_pos, dire_heroes_and_pos, parse_error, problem_summary, problem_candidates = parse_draft_and_positions(
            soup, data, radiant_team_name_original, dire_team_name_original
        )
        
        if parse_error:
            # Ошибка парсинга - пропускаем матч
            print(f"   ❌ Ошибка парсинга драфта: {parse_error}")
            print(f"   ❌ Матч пропущен (ошибка парсинга драфта)")
            print(f"   ℹ️ map_id_check.txt не обновлен: матч будет перепроверен в следующем цикле")
            # add_url(check_uniq_url)
            return return_status
        
        print(f"   ✅ Драфт успешно распарсен")
        # Отправляем только "сырые" сигналы без wrapper.
        prev_wrapper_enabled = os.getenv("SIGNAL_WRAPPER_ENABLED")
        os.environ["SIGNAL_WRAPPER_ENABLED"] = "0"
        try:
            s = synergy_and_counterpick(
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos,
                early_dict=early_dict, mid_dict=late_dict)
        finally:
            if prev_wrapper_enabled is None:
                os.environ.pop("SIGNAL_WRAPPER_ENABLED", None)
            else:
                os.environ["SIGNAL_WRAPPER_ENABLED"] = prev_wrapper_enabled
        s['top'], s['bot'], s['mid'] = calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, lane_data)
        star_base_early_output = dict(s.get('early_output', {}) or {})
        star_base_mid_output = dict(s.get('mid_output', {}) or {})

        # Подбор star-кандидата (отправка только если сигнал star).
        selected_star_wr = star_target_wr
        selected_star_mode = f"base_wr_{selected_star_wr}"
        has_valid_star_signal = False
        selected_star_candidate = None
        star_filter_rejections = []

        def _build_star_candidate(target_wr: int):
            candidate = {
                'early_output': dict(star_base_early_output),
                'mid_output': dict(star_base_mid_output),
            }
            has_any_star = format_output_dict(
                candidate,
                target_wr=target_wr,
                late_signal_gate_enabled=LIVE_STAR_LATE_SIGNAL_GATE_ENABLED,
            )
            return has_any_star, candidate

        def _candidate_passes_extra_filters(
            candidate: dict,
            match_tier: int,
            target_wr: int,
        ) -> tuple[bool, str]:
            late_diag = _star_block_diagnostics(
                raw_block=candidate.get('mid_output'),
                target_wr=target_wr,
                section="mid_output",
            )
            has_late_star = bool(late_diag.get("valid"))
            if match_tier != 2:
                return True, "ok"
            if STAR_REQUIRE_TIER2_LATE_STAR and not has_late_star:
                return (
                    False,
                    f"tier2_requires_late_star(late_star={'yes' if has_late_star else 'no'})",
                )
            return True, "ok"

        has_star_base, cand_base = _build_star_candidate(star_target_wr)
        if has_star_base:
            base_passed, base_reason = _candidate_passes_extra_filters(
                cand_base,
                star_match_tier,
                star_target_wr,
            )
            if base_passed:
                has_valid_star_signal = True
                selected_star_candidate = cand_base
            elif base_reason != "ok":
                star_filter_rejections.append(f"base_wr_{star_target_wr}:{base_reason}")
        # Strict tier min-threshold rule: Tier2 matches must keep min WR=65.
        star_fallback_wr = None
        raw_star_early_summary = _format_raw_star_block_metrics(
            raw_block=star_base_early_output,
            section="early_output",
            primary_wr=star_target_wr,
            fallback_wr=star_fallback_wr,
        )
        raw_star_late_summary = _format_raw_star_block_metrics(
            raw_block=star_base_mid_output,
            section="mid_output",
            primary_wr=star_target_wr,
            fallback_wr=star_fallback_wr,
        )
        primary_star_early_diag = _star_block_diagnostics(
            raw_block=star_base_early_output,
            target_wr=star_target_wr,
            section="early_output",
        )
        primary_star_late_diag = _star_block_diagnostics(
            raw_block=star_base_mid_output,
            target_wr=star_target_wr,
            section="mid_output",
        )
        primary_star_match_status = _star_match_status_from_diags(
            primary_star_early_diag,
            primary_star_late_diag,
            star_match_tier,
        )
        star_diag_lines = [
            (
                f"WR{star_target_wr}: "
                f"early={_format_star_block_status(primary_star_early_diag)}, "
                f"late={_format_star_block_status(primary_star_late_diag)}, "
                f"match={primary_star_match_status}"
            )
        ]
        if star_fallback_wr is not None:
            fallback_star_early_diag = _star_block_diagnostics(
                raw_block=star_base_early_output,
                target_wr=star_fallback_wr,
                section="early_output",
            )
            fallback_star_late_diag = _star_block_diagnostics(
                raw_block=star_base_mid_output,
                target_wr=star_fallback_wr,
                section="mid_output",
            )
            fallback_star_match_status = _star_match_status_from_diags(
                fallback_star_early_diag,
                fallback_star_late_diag,
                star_match_tier,
            )
            star_diag_lines.append(
                (
                    f"WR{star_fallback_wr}: "
                    f"early={_format_star_block_status(fallback_star_early_diag)}, "
                    f"late={_format_star_block_status(fallback_star_late_diag)}, "
                    f"match={fallback_star_match_status}"
                )
            )

        force_odds_signal_test_active = bool(
            BOOKMAKER_PREFETCH_ENABLED
            and FORCE_ODDS_SIGNAL_TEST
            and not has_valid_star_signal
        )
        if force_odds_signal_test_active:
            selected_star_candidate = cand_base
            selected_star_mode = "force_odds_signal_test"
            print("   🧪 FORCE_ODDS_SIGNAL_TEST=1: bypass STAR gate for odds=True test send")

        if (has_valid_star_signal and selected_star_candidate is not None) or force_odds_signal_test_active:
            s['early_output'] = selected_star_candidate.get('early_output', {})
            s['mid_output'] = selected_star_candidate.get('mid_output', {})
            selected_early_diag = _star_block_diagnostics(
                raw_block=s.get('early_output', {}),
                target_wr=selected_star_wr,
                section="early_output",
            )
            selected_late_diag = _star_block_diagnostics(
                raw_block=s.get('mid_output', {}),
                target_wr=selected_star_wr,
                section="mid_output",
            )
            has_selected_early_star = bool(selected_early_diag.get("valid"))
            has_selected_late_star = bool(selected_late_diag.get("valid"))
            selected_early_sign = selected_early_diag.get("sign") if has_selected_early_star else None
            selected_late_sign = selected_late_diag.get("sign") if has_selected_late_star else None
            late_same_or_zero_diag = _block_signs_same_or_zero(
                raw_block=s.get('mid_output', {}),
                expected_sign=selected_early_sign,
            )
            early_same_or_zero_diag = _block_signs_same_or_zero(
                raw_block=s.get('early_output', {}),
                expected_sign=selected_late_sign,
            )
            send_now_full_star = (
                has_selected_early_star
                and has_selected_late_star
                and selected_early_sign == selected_late_sign
            )
            send_now_tier1_early_star_late_same_or_zero = (
                STAR_ALLOW_TIER1_EARLY_STAR_LATE_SAME_OR_ZERO
                and star_match_tier == 1
                and has_selected_early_star
                and not has_selected_late_star
                and bool(late_same_or_zero_diag.get("valid"))
            )
            send_now_late_star_early_same_or_zero = (
                STAR_ALLOW_LATE_STAR_EARLY_SAME_OR_ZERO
                and has_selected_late_star
                and not has_selected_early_star
                and bool(early_same_or_zero_diag.get("valid"))
            )
            send_now_immediate = (
                send_now_full_star
                or send_now_tier1_early_star_late_same_or_zero
                or send_now_late_star_early_same_or_zero
                or force_odds_signal_test_active
            )

            if (
                not force_odds_signal_test_active
                and not has_selected_late_star
                and not send_now_tier1_early_star_late_same_or_zero
            ):
                print(
                    "   ⚠️ ВЕРДИКТ: ОТКАЗ "
                    "(нет late star-сигнала) - матч пропущен"
                )
                print(
                    "   📉 Raw star metrics: "
                    f"early[{raw_star_early_summary}] | "
                    f"late[{raw_star_late_summary}]"
                )
                print(f"   📉 Star checks: {' | '.join(star_diag_lines)}")
                add_url(
                    check_uniq_url,
                    reason="star_signal_rejected_no_late_star",
                    details={
                        "status": status,
                        "selected_star_wr": selected_star_wr,
                        "selected_star_mode": selected_star_mode,
                        "selected_early_star": bool(has_selected_early_star),
                        "selected_late_star": bool(has_selected_late_star),
                        "selected_early_sign": selected_early_sign,
                        "selected_late_sign": selected_late_sign,
                        "json_retry_errors": json_retry_errors,
                    },
                )
                print("   ✅ map_id_check.txt обновлен: add_url после отказа no-late-star")
                return return_status

            if send_now_tier1_early_star_late_same_or_zero:
                print(
                    "   ✅ Tier1 override: early star + late signs are only 0/same-sign "
                    f"(sign={selected_early_sign})"
                )
            if send_now_late_star_early_same_or_zero:
                print(
                    "   ✅ Override: late star + early signs are only 0/same-sign "
                    f"(sign={selected_late_sign})"
                )

            dispatch_mode = (
                "immediate_force_odds_signal_test"
                if force_odds_signal_test_active
                else (
                    "immediate_early_late_same_sign"
                    if send_now_full_star
                    else (
                        "immediate_tier1_early_star_late_same_or_zero"
                        if send_now_tier1_early_star_late_same_or_zero
                        else (
                            "immediate_late_star_early_same_or_zero"
                            if send_now_late_star_early_same_or_zero
                            else "delayed_late_only_21m"
                        )
                    )
                )
            )

            def _format_metrics(title, data, metrics):
                lines = [title]
                for key, label in metrics:
                    lines.append(f"{label}: {data.get(key)}")
                return "\n".join(lines) + "\n"

            metric_list = [
                ('counterpick_1vs1', 'Counterpick_1vs1'),
                ('counterpick_1vs2', 'Counterpick_1vs2'),
                ('solo', 'Solo'),
                ('synergy_duo', 'Synergy_duo'),
                ('synergy_trio', 'Synergy_trio'),
            ]

            early_output_log = _decorate_star_block_for_display(
                raw_block=s.get('early_output', {}),
                section="early_output",
                target_wr=selected_star_wr,
            )
            mid_output_log = _decorate_star_block_for_display(
                raw_block=s.get('mid_output', {}),
                section="mid_output",
                target_wr=selected_star_wr,
            )
            early_output = early_output_log if send_now_immediate else {}
            mid_output = mid_output_log

            early_block = (
                _format_metrics("10-28 Minute:", early_output, metric_list)
                if send_now_immediate
                else ""
            )
            mid_block = _format_metrics("Mid (25-50 min):", mid_output, metric_list)
            early_block_log = _format_metrics("10-28 Minute:", early_output_log, metric_list)
            mid_block_log = _format_metrics("Mid (25-50 min):", mid_output_log, metric_list)

            # Серия: только счет
            series_score_line = ""
            try:
                live_league = data.get('live_league_data') or {}
                r_wins = live_league.get('radiant_series_wins')
                d_wins = live_league.get('dire_series_wins')
                if r_wins is not None or d_wins is not None:
                    r_wins = int(r_wins or 0)
                    d_wins = int(d_wins or 0)
                    series_score_line = f"{r_wins}-{d_wins}\n"
            except Exception:
                series_score_line = ""

            odds_block = ""
            if BOOKMAKER_PREFETCH_ENABLED:
                # Рекомендации по минимальному кэфу
                odds_lines = []
                bookmaker_odds_block = ""
                bookmaker_odds_ready = False
                bookmaker_odds_reason = "not_requested"
                early_rec = _recommend_odds_for_block(early_output, 'early') if send_now_immediate else None
                if early_rec:
                    odds_label = f"{early_rec['min_odds']:.2f}"
                    wr_label = f"{float(early_rec['wr_pct']):.1f}%"
                    odds_lines.append(
                        f"Early: от кэфа {odds_label} (WR≈{wr_label})"
                    )
                late_rec = _recommend_odds_for_block(mid_output, 'late')
                if late_rec:
                    odds_label = f"{late_rec['min_odds']:.2f}"
                    wr_label = f"{float(late_rec['wr_pct']):.1f}%"
                    odds_lines.append(
                        f"Late: от кэфа {odds_label} (WR≈{wr_label})"
                    )
                bookmaker_odds_block, bookmaker_odds_ready, bookmaker_odds_reason = _bookmaker_format_odds_block(check_uniq_url)
                if not bookmaker_odds_ready:
                    if bookmaker_odds_reason == "no_numeric_odds":
                        _log_bookmaker_source_snapshot(check_uniq_url, decision="no_numeric_odds")
                    print(
                        "   ⏳ Пропуск STAR-сигнала: odds=True требует кэфы букмекера "
                        f"(reason={bookmaker_odds_reason}) для {check_uniq_url}"
                    )
                    return return_status
                if odds_lines or bookmaker_odds_block:
                    sections = []
                    if odds_lines:
                        sections.append("Рекоменд. кэф:\n" + "\n".join(odds_lines) + "\n")
                    if bookmaker_odds_block:
                        sections.append(bookmaker_odds_block)
                    odds_block = "".join(sections)
            problem_block = ""
            if BOOKMAKER_PREFETCH_ENABLED and problem_candidates:
                def _problem_pos_sort_key(item: dict) -> tuple:
                    pos = str(item.get("position") or "")
                    try:
                        pos_num = int(pos[-1]) if pos.startswith("pos") else 99
                    except (TypeError, ValueError, IndexError):
                        pos_num = 99
                    return (
                        int(item.get("score", 0) or 0),
                        str(item.get("team_key") or ""),
                        pos_num,
                        int(item.get("hero_id", 0) or 0),
                    )

                ordered_problem_candidates = sorted(problem_candidates, key=_problem_pos_sort_key)
                top_problem_candidates = ordered_problem_candidates[:2]
                if top_problem_candidates:
                    lines = []
                    for item in top_problem_candidates:
                        team_key = str(item.get("team_key") or "unknown")
                        pos = str(item.get("position") or "pos?")
                        hero_name = str(item.get("hero_name") or f"Unknown({item.get('hero_id', 0)})")
                        score_value = int(item.get("score", 0) or 0)
                        lines.append(f"{team_key}:{pos} - {hero_name} (score={score_value})")
                    problem_block = "⚠️problem_positions_top2:\n" + "; ".join(lines) + "\n"
            elif problem_summary:
                problem_block = f"{problem_summary}\n"

            # Формирование сообщения
            message_text = (
                f'ПОМНИ: КОМАНДА ВАЖНЕЕ ПИКА\n'
                f"{radiant_team_name} VS {dire_team_name}\n"
                f"{series_score_line}"
                f"Lanes:\n{s.get('top')}{s.get('mid')}{s.get('bot')}"
                f"{problem_block}"
                f"{odds_block}"
                f"{early_block}"
                f"{mid_block}"
                f'ПОМНИ: КОМАНДА ВАЖНЕЕ ПИКА'
            )
            current_game_time = float(game_time or 0.0)
            target_sign = selected_late_sign if has_selected_late_star else selected_early_sign
            target_side = _target_side_from_sign(target_sign)
            target_networth_diff = _target_networth_diff_from_radiant_lead(lead, target_side)
            networth_send_status_label: Optional[str] = None
            if not force_odds_signal_test_active:
                if target_networth_diff is None or target_side is None:
                    print(
                        "   ⏳ Ожидание dispatch: target-side networth gate не применен "
                        "(нет target_sign/lead)"
                    )
                    return return_status
                if current_game_time < NETWORTH_GATE_HARD_BLOCK_SECONDS:
                    print(
                        f"   ⏳ Ожидание dispatch: {NETWORTH_STATUS_PRE4_BLOCK} "
                        f"(now={_format_game_clock(current_game_time)}, "
                        f"target_side={target_side}, target_diff={int(target_networth_diff)})"
                    )
                    return return_status
                if current_game_time < NETWORTH_GATE_EARLY_WINDOW_END_SECONDS:
                    if target_networth_diff < NETWORTH_GATE_4_TO_10_MIN_DIFF:
                        print(
                            "   ⏳ Ожидание dispatch: networth_gate_04_10 "
                            f"(target_side={target_side}, target_diff={int(target_networth_diff)}, "
                            f"need>={int(NETWORTH_GATE_4_TO_10_MIN_DIFF)})"
                        )
                        return return_status
                    networth_send_status_label = NETWORTH_STATUS_4_10_SEND_800
                elif send_now_full_star:
                    if target_networth_diff < NETWORTH_GATE_10_MIN_MAX_LOSS:
                        print(
                            "   ⏳ Ожидание dispatch: networth_gate_10plus_early_late_same_sign "
                            f"(target_side={target_side}, target_diff={int(target_networth_diff)}, "
                            f"need>={int(NETWORTH_GATE_10_MIN_MAX_LOSS)})"
                        )
                        return return_status
                    networth_send_status_label = NETWORTH_STATUS_MIN10_LOSS_LE1500_SEND
            if not send_now_immediate:
                delay_reason = "late_only_no_early_same_sign"
                if has_selected_early_star and selected_early_sign != selected_late_sign:
                    delay_reason = "late_only_opposite_signs"
                elif not has_selected_early_star:
                    delay_reason = "late_only_no_early_star"
                print("   📊 STAR метрики (delayed):")
                print("      " + early_block_log.rstrip().replace("\n", "\n      "))
                print("      " + mid_block_log.rstrip().replace("\n", "\n      "))
                print(
                    "   📉 Raw star metrics: "
                    f"early[{raw_star_early_summary}] | "
                    f"late[{raw_star_late_summary}]"
                )
                print(f"   📉 Star checks: {' | '.join(star_diag_lines)}")
                _ensure_delayed_sender_started()
                target_game_time = float(DELAYED_SIGNAL_TARGET_GAME_TIME)
                target_human = _format_game_clock(target_game_time)
                monitor_threshold: Optional[float] = None
                monitor_wait_status_label: Optional[str] = None
                if not has_selected_early_star and has_selected_late_star:
                    monitor_threshold = NETWORTH_GATE_LATE_NO_EARLY_DIFF
                    monitor_wait_status_label = NETWORTH_STATUS_LATE_MONITOR_WAIT_1000
                elif has_selected_early_star and has_selected_late_star and selected_early_sign != selected_late_sign:
                    monitor_threshold = NETWORTH_GATE_LATE_OPPOSITE_DIFF
                    monitor_wait_status_label = NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000
                release_4_10_now = bool(
                    (not force_odds_signal_test_active)
                    and target_networth_diff is not None
                    and NETWORTH_GATE_HARD_BLOCK_SECONDS <= current_game_time < NETWORTH_GATE_EARLY_WINDOW_END_SECONDS
                    and target_networth_diff >= NETWORTH_GATE_4_TO_10_MIN_DIFF
                )
                monitor_ready_now = bool(
                    (not force_odds_signal_test_active)
                    and monitor_threshold is not None
                    and target_networth_diff is not None
                    and current_game_time >= NETWORTH_GATE_EARLY_WINDOW_END_SECONDS
                    and current_game_time < target_game_time
                    and target_networth_diff >= monitor_threshold
                )
                if release_4_10_now or monitor_ready_now:
                    release_reason = (
                        NETWORTH_STATUS_4_10_SEND_800
                        if release_4_10_now
                        else f"networth_monitor_{int(monitor_threshold or 0)}"
                    )
                    release_status_label = (
                        NETWORTH_STATUS_4_10_SEND_800
                        if release_4_10_now
                        else (
                            NETWORTH_STATUS_LATE_MONITOR_WAIT_1000
                            if monitor_threshold == NETWORTH_GATE_LATE_NO_EARLY_DIFF
                            else NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000
                        )
                    )
                    print(
                        "   ✅ ВЕРДИКТ: Сигнал отправлен раньше 21:00 "
                        f"(reason={release_reason}, status={release_status_label}, target_side={target_side}, "
                        f"target_diff={int(target_networth_diff or 0)})"
                    )
                    if _skip_dispatch_for_processed_url(check_uniq_url, "немедленной отправки (networth_gate)"):
                        return return_status
                    if not _acquire_signal_send_slot(check_uniq_url):
                        print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                        return return_status
                    try:
                        if _skip_dispatch_for_processed_url(check_uniq_url, "немедленной отправки после lock (networth_gate)"):
                            return return_status
                        send_message(message_text)
                        _log_bookmaker_source_snapshot(check_uniq_url, decision="sent")
                        _mark_url_processed(check_uniq_url)
                        add_url(
                            check_uniq_url,
                            reason="star_signal_sent_now_networth_gate",
                            details={
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": delay_reason,
                                "release_reason": release_reason,
                                "dispatch_status_label": release_status_label,
                                "game_time": int(current_game_time),
                                "target_side": target_side,
                                "target_networth_diff": float(target_networth_diff or 0.0),
                                "json_retry_errors": json_retry_errors,
                            },
                        )
                    finally:
                        _release_signal_send_slot(check_uniq_url)
                    return return_status
                if not json_url:
                    print(f"   ⚠️ Нет json_url для delayed сигнала, отправляем сразу: {check_uniq_url}")
                    if _skip_dispatch_for_processed_url(check_uniq_url, "немедленной отправки (no_json_url)"):
                        return return_status
                    if not _acquire_signal_send_slot(check_uniq_url):
                        print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                        return return_status
                    try:
                        if _skip_dispatch_for_processed_url(check_uniq_url, "немедленной отправки после lock (no_json_url)"):
                            return return_status
                        send_message(message_text)
                        _log_bookmaker_source_snapshot(check_uniq_url, decision="sent")
                        _mark_url_processed(check_uniq_url)
                        print(f"   ✅ ВЕРДИКТ: Сигнал отправлен немедленно (нет json_url для delayed)")
                        add_url(
                            check_uniq_url,
                            reason="star_signal_sent_now_no_json_url",
                            details={
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": delay_reason,
                                "json_retry_errors": json_retry_errors,
                            },
                        )
                    finally:
                        _release_signal_send_slot(check_uniq_url)
                    return return_status
                if current_game_time >= target_game_time:
                    print(
                        f"   ⏱️ game_time уже >= {target_human} ({int(current_game_time)}), "
                        f"отправляем сразу: {check_uniq_url}"
                    )
                    if _skip_dispatch_for_processed_url(check_uniq_url, f"немедленной отправки (game_time >= {target_human})"):
                        return return_status
                    if not _acquire_signal_send_slot(check_uniq_url):
                        print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                        return return_status
                    try:
                        if _skip_dispatch_for_processed_url(check_uniq_url, f"немедленной отправки после lock (game_time >= {target_human})"):
                            return return_status
                        send_message(message_text)
                        _log_bookmaker_source_snapshot(check_uniq_url, decision="sent")
                        _mark_url_processed(check_uniq_url)
                        print(f"   ✅ ВЕРДИКТ: Сигнал отправлен немедленно (game_time >= {target_human})")
                        add_url(
                            check_uniq_url,
                            reason="star_signal_sent_now_target_reached",
                            details={
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": delay_reason,
                                "game_time": int(current_game_time),
                                "target_game_time": int(target_game_time),
                                "json_retry_errors": json_retry_errors,
                            },
                        )
                    finally:
                        _release_signal_send_slot(check_uniq_url)
                    return return_status
                eta_seconds = max(0.0, target_game_time - current_game_time)
                eta_human = datetime.fromtimestamp(time.time() + eta_seconds).strftime('%Y-%m-%d %H:%M:%S')
                queued_ts = time.time()
                delayed_add_url_details = {
                    "status": status,
                    "dispatch_mode": dispatch_mode,
                    "delay_reason": delay_reason,
                    "dispatch_status_label": monitor_wait_status_label,
                    "queued_game_time": int(current_game_time),
                    "target_game_time": int(target_game_time),
                    "json_retry_errors": json_retry_errors,
                }
                if monitor_threshold is not None:
                    delayed_add_url_details["networth_monitor_threshold"] = float(monitor_threshold)
                    delayed_add_url_details["networth_monitor_deadline_game_time"] = int(target_game_time)
                    delayed_add_url_details["networth_target_side"] = target_side
                    if target_networth_diff is not None:
                        delayed_add_url_details["target_networth_diff"] = float(target_networth_diff)
                with monitored_matches_lock:
                    monitored_matches[check_uniq_url] = {
                        'message': message_text,
                        'reason': delay_reason,
                        'json_url': json_url,
                        'target_game_time': target_game_time,
                        'queued_at': queued_ts,
                        'queued_game_time': current_game_time,
                        'last_game_time': current_game_time,
                        'last_progress_at': queued_ts,
                        'dispatch_status_label': monitor_wait_status_label,
                        'add_url_reason': 'star_signal_sent_delayed',
                        'add_url_details': delayed_add_url_details,
                        'fallback_send_status_label': NETWORTH_STATUS_LATE_FALLBACK_21_SEND,
                    }
                    if monitor_threshold is not None:
                        monitored_matches[check_uniq_url]['networth_monitor_threshold'] = float(monitor_threshold)
                        monitored_matches[check_uniq_url]['networth_monitor_deadline_game_time'] = float(target_game_time)
                        monitored_matches[check_uniq_url]['networth_target_side'] = target_side
                print(
                    f"   ⏱️ Сигнал в delayed-очереди до game_time={target_human} "
                    f"(reason={delay_reason}, now={int(current_game_time)}), "
                    f"ETA~{eta_human}: {check_uniq_url}"
                )
                if monitor_threshold is not None and target_side is not None:
                    print(
                        "   🔎 Delayed monitoring включен: "
                        f"status={monitor_wait_status_label}, "
                        f"target_side={target_side}, "
                        f"target_diff={int(target_networth_diff or 0)}, "
                        f"threshold={int(monitor_threshold)}, "
                        f"deadline={target_human}"
                    )
                print(f"   ✅ ВЕРДИКТ: Сигнал добавлен в delayed-очередь (reason={delay_reason})")
                print("   ℹ️ map_id_check.txt будет обновлен после фактической отправки delayed send_message()")
                return return_status
            if not _acquire_signal_send_slot(check_uniq_url):
                print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                return return_status
            try:
                if _skip_dispatch_for_processed_url(check_uniq_url, "немедленной отправки STAR-сигнала после lock"):
                    return return_status
                send_message(message_text)
                _log_bookmaker_source_snapshot(check_uniq_url, decision="sent")
                _mark_url_processed(check_uniq_url)
                print("   ✅ ВЕРДИКТ: STAR-сигнал отправлен немедленно")
                add_url(
                    check_uniq_url,
                    reason="star_signal_sent_now",
                    details={
                        "status": status,
                        "dispatch_mode": dispatch_mode,
                        "dispatch_status_label": networth_send_status_label,
                        "selected_star_wr": selected_star_wr,
                        "selected_star_mode": selected_star_mode,
                        "json_retry_errors": json_retry_errors,
                    },
                )
            finally:
                _release_signal_send_slot(check_uniq_url)
        else:
            print(
                "   ⚠️ ВЕРДИКТ: ОТКАЗ "
                "(нет star-сигнала) - матч пропущен"
            )
            print(
                "   📉 Raw star metrics: "
                f"early[{raw_star_early_summary}] | "
                f"late[{raw_star_late_summary}]"
            )
            print(f"   📉 Star checks: {' | '.join(star_diag_lines)}")
            print(
                "   📉 Threshold block: "
                f"reason={tier_threshold_block_reason_label}, "
                f"status={tier_threshold_block_status_label}, "
                f"min_wr={int(star_target_wr)}%"
            )
            if star_filter_rejections:
                print(f"   📉 Star filter reject: {'; '.join(star_filter_rejections)}")
            add_url(
                check_uniq_url,
                reason="star_signal_rejected_no_star_signal",
                details={
                    "status": status,
                    "dispatch_status_label": tier_threshold_block_status_label,
                    "threshold_block_reason_label": tier_threshold_block_reason_label,
                    "threshold_min_wr": int(star_target_wr),
                    "selected_star_wr": selected_star_wr,
                    "selected_star_mode": selected_star_mode,
                    "star_filter_rejections": star_filter_rejections,
                    "json_retry_errors": json_retry_errors,
                },
            )
            print("   ✅ map_id_check.txt обновлен: add_url после отказа no-star")



def _load_stats_dicts():
    """Ленивая загрузка словарей, чтобы не грузить их при импорте."""
    global lane_data, early_dict, late_dict
    if lane_data is not None and early_dict is not None and late_dict is not None:
        return

    def _load_json_mmap(path: str):
        # Avoid extra peak from f.read() copy on huge dict files.
        with open(path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                return orjson.loads(memoryview(mm))

    default_stats_dir = "/Users/alex/Documents/ingame/bets_data/analise_pub_matches"
    stats_dir = os.getenv("STATS_DIR", default_stats_dir)
    lane_path = os.getenv("STATS_LANE_PATH", f"{stats_dir}/lane_dict_raw.json")
    early_path = os.getenv("STATS_EARLY_PATH", f"{stats_dir}/early_dict_raw.json")
    late_path = os.getenv("STATS_LATE_PATH", f"{stats_dir}/late_dict_raw.json")

    # If test stats folder has no lane dict, fallback to baseline lane dict.
    if not Path(lane_path).exists():
        fallback_lane = f"{default_stats_dir}/lane_dict_raw.json"
        if Path(fallback_lane).exists():
            lane_path = fallback_lane

    lane_data = _load_json_mmap(lane_path)
    gc.collect()
    early_dict = _load_json_mmap(early_path)
    gc.collect()
    late_dict = _load_json_mmap(late_path)
    gc.collect()


def general(return_status=None, use_proxy=None, odds=None):
    """
    Основной цикл проверки матчей.
    
    Args:
        return_status: статус для возврата (не используется)
        use_proxy: использовать прокси (True) или прямое подключение (False).
                   Если None — берется из USE_PROXY (если задан) или из переменной окружения USE_PROXY.
        odds: включить odds-пайплайн (True/False).
              Если None — берется из переменной окружения BOOKMAKER_PREFETCH_ENABLED (по умолчанию True).
    """
    global PROXIES, BOOKMAKER_PREFETCH_ENABLED

    odds_arg = odds
    if odds is None:
        odds = _safe_bool_env("BOOKMAKER_PREFETCH_ENABLED", False)
    if isinstance(odds, str):
        odds_requested = odds.strip().lower() in {"1", "true", "yes", "y", "on"}
    else:
        odds_requested = bool(odds)
    BOOKMAKER_PREFETCH_ENABLED = odds_requested and BOOKMAKER_PREFETCH_AVAILABLE

    odds_source = "arg" if odds_arg is not None else "env"
    print(
        "🎲 Odds pipeline: "
        f"{'ON' if BOOKMAKER_PREFETCH_ENABLED else 'OFF'} "
        f"(source={odds_source}, requested={odds_requested}, available={BOOKMAKER_PREFETCH_AVAILABLE})"
    )
    logger.info(
        "Odds pipeline mode: %s (source=%s, requested=%s, available=%s)",
        "ON" if BOOKMAKER_PREFETCH_ENABLED else "OFF",
        odds_source,
        odds_requested,
        BOOKMAKER_PREFETCH_AVAILABLE,
    )

    if use_proxy is None:
        # Локальная настройка при запуске
        use_proxy = globals().get("USE_PROXY", None)
    if use_proxy is None:
        # Переменная окружения
        env_use_proxy = os.getenv("USE_PROXY")
        if env_use_proxy is not None:
            use_proxy = env_use_proxy.strip().lower() not in {"0", "false", "no", "off"}
    if use_proxy is None:
        use_proxy = True

    # Инициализируем прокси явно при запуске цикла
    if use_proxy != USE_PROXY:
        _init_proxy_pool(use_proxy)

    # Гарантируем загрузку словарей перед обработкой матчей
    _load_stats_dicts()
    _ensure_delayed_sender_started()
    if BOOKMAKER_PREFETCH_ENABLED:
        _ensure_bookmaker_prefetch_started()
    else:
        _stop_bookmaker_prefetch_worker()
    
    logger.info(f"\n{'='*60}\n🔄 НАЧАЛО ЦИКЛА ПРОВЕРКИ МАТЧЕЙ\n{'='*60}")
    
    radiant_heroes_and_pos, dire_heroes_and_pos, radiant_team_name, dire_team_name, score, return_status = None, None, None, None, None, None
    try:
        with open(MAP_ID_CHECK_PATH, 'rb') as f:
            maps_data = orjson.loads(f.read())
        print(f"✅ Загружено {len(maps_data)} матчей из {MAP_ID_CHECK_PATH}")
        _sync_processed_urls_cache(maps_data)
    except FileNotFoundError:
        with open(MAP_ID_CHECK_PATH, 'w') as f:
            json.dump([], f)
        maps_data = []
        _sync_processed_urls_cache(maps_data)
    print(f"🌐 Получение списка активных матчей...")
    answer = get_heads()
    if not answer or answer is None:
        print('❌ Не удалось выяснить heads (нет активных матчей)')
        return None
    heads, bodies = answer
    
    # Проверка что heads не None
    if heads is None:
        print('❌ Не найден элемент live__matches в HTML')
        try:
            send_message('❌ Не найден элемент live__matches в HTML')
        except Exception as e:
            print(f"⚠️ Не удалось отправить уведомление в Telegram: {e}")
        return None
    
    print(f'✅ Найдено активных матчей: {len(heads)}')
    
    all_statuses = []
    for i in range(len(heads)):
        answer = check_head(heads, bodies, i, maps_data)
        if answer is not None:
            if isinstance(answer, str):
                all_statuses.append(answer)
            # else:
            #     try:
            #         radiant_heroes_and_pos, dire_heroes_and_pos, radiant_team_name, dire_team_name, score, return_status = answer
            #         return radiant_heroes_and_pos, dire_heroes_and_pos, radiant_team_name, dire_team_name, score, return_status
            #     except:
            #         pass
    
    print(f"\n{'='*60}")
    print(f"📊 ИТОГИ ЦИКЛА:")
    print(f"   Обработано матчей: {len(heads) if heads else 0}")
    print(f"   Статусы: {all_statuses if all_statuses else 'нет статусов'}")
    print(f"{'='*60}\n")
    
    # Если хотя бы один статус содержит 'draft', возвращаем 'draft...'
    if any('draft' in str(s).lower() for s in all_statuses):
        return 'draft...'
    
    # Иначе возвращаем последний статус или None
    return all_statuses[-1] if all_statuses else None
if __name__ == "__main__":
        import orjson
        from functions import one_match, check_old_maps
        from keys import start_date_time

        parser = argparse.ArgumentParser(description="Run cyberscore live loop")
        parser.add_argument(
            "--odds",
            dest="odds",
            action="store_true",
            help="Enable odds pipeline",
        )
        parser.add_argument(
            "--no-odds",
            dest="odds",
            action="store_false",
            help="Disable odds pipeline completely",
        )
        parser.set_defaults(odds=None)
        args = parser.parse_args()
        if args.odds is True and os.getenv("MAP_ID_CHECK_PATH") is None:
            MAP_ID_CHECK_PATH = MAP_ID_CHECK_PATH_ODDS_DEFAULT
            print(f"🗺️ MAP_ID_CHECK_PATH defaulted for --odds: {MAP_ID_CHECK_PATH}")
        
        # Абсолютные пути к данным (вынесены за пределы проекта для оптимизации Cursor)
        STATS_DIR = "/Users/alex/Documents/ingame/bets_data/analise_pub_matches"
        
        # Ленивая загрузка словарей (использует STATS_DIR или STATS_DIR из env)
        _load_stats_dicts()
        # early_dict, late_dict = {}, {}
        # lane_data, early_dict, late_dict = {}, {}, {}
        # check_old_maps(early_dict, late_dict, lane_data, start_date_time=start_date_time,
                    #    outfile_name='pub')
        # one_match(radiant_heroes_and_pos={'pos1': {'hero_name': "phantom assassin"}, 'pos2': {'hero_name': "nature's prophet"},
        #                                   'pos3': {'hero_name': 'lycan'}, 'pos4': {'hero_name': "lich"},
        #                                   'pos5': {'hero_name': "techies"}},
        #           dire_heroes_and_pos={'pos1': {'hero_name': "bristleback"}, 'pos2': {'hero_name': "skywrath mage"},
        #                                'pos3': {'hero_name': 'mars'}, 'pos4': {'hero_name': 'shadow demon'},
        #                                'pos5': {'hero_name': "sniper"}},
        #           lane_data=lane_data, early_dict=early_dict, late_dict=late_dict,
        #           radiant_team_name='Falcons Team', dire_team_name='dire')

        # === ИНИЦИАЛИЗАЦИЯ ТРАНЗИТИВНОЙ МОДЕЛИ (85.2% ВИНРЕЙТ) ===

        # === НАСТРОЙКА ПРОКСИ ===

        while True:
            try:
                # if is_moscow_night():
                #     sleep_until_morning()
                status = general(use_proxy=None, odds=args.odds)
                if status is None:
                    print('Сплю 60 секунд')
                    time.sleep(60)
                else:
                    print('Сплю 60 секунд')
                    time.sleep(60)
            except Exception as e:
                print(f"⚠️ Ошибка главного цикла: {e}")
                logger.exception("Main loop error")
                time.sleep(30)
