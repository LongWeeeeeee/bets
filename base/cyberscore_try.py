import argparse
import json
import ast
import atexit
import contextlib
from collections import deque, OrderedDict
import io
import orjson
try:
    import ijson
except Exception:
    ijson = None
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
import tempfile
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple, Union
import math
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import requests
from functions import (
    send_message,
    synergy_and_counterpick,
    calculate_lanes,
    calculate_comeback_solo_metrics,
    format_output_dict,
    STAR_THRESHOLDS_BY_WR,
    TelegramSendError,
)
try:
    from keys import api_to_proxy, BOOKMAKER_PROXY_URL, BOOKMAKER_PROXY_POOL
except ImportError:
    from keys import api_to_proxy, BOOKMAKER_PROXY_URL
    BOOKMAKER_PROXY_POOL = []
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from tempo_analise_database_experiment import build_tempo_draft_metrics as _tempo_build_tempo_draft_metrics
    from tempo_analise_database_experiment import load_tempo_dicts as _tempo_load_tempo_dicts
except ImportError:
    _tempo_build_tempo_draft_metrics = None
    _tempo_load_tempo_dicts = None

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

BASE_DIR = PROJECT_ROOT / "base"
DATA_DIR = PROJECT_ROOT / "data"
SRC_DIR = PROJECT_ROOT / "src"
ML_MODELS_DIR = PROJECT_ROOT / "ml-models"
REPORTS_DIR = PROJECT_ROOT / "reports"
ANALYSE_PUB_DIR = PROJECT_ROOT / "bets_data" / "analise_pub_matches"
TEMPO_EXPERIMENT_DIR = PROJECT_ROOT / "bets_data" / "tempo_pub_experiment"
PRO_HEROES_DIR = PROJECT_ROOT / "pro_heroes_data"

# Импорт Ultimate Inference предсказателя
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
try:
    from live_predictor import predict_live_match
    LIVE_PREDICTOR_AVAILABLE = True
except ImportError:
    LIVE_PREDICTOR_AVAILABLE = False
    predict_live_match = None

# Настройка логирования
logger = logging.getLogger(__name__)
_RUNTIME_ALERTED_ERRORS: set[str] = set()


def _notify_runtime_error_once(message: str, *, dedupe_key: Optional[str] = None) -> None:
    key = dedupe_key or message
    if key in _RUNTIME_ALERTED_ERRORS:
        return
    _RUNTIME_ALERTED_ERRORS.add(key)
    logger.error(message)
    try:
        send_message(message, admin_only=True)
    except Exception as exc:
        logger.warning("Failed to deliver runtime error alert to Telegram: %s", exc)


def _report_missing_runtime_file(label: str, path: Union[Path, str], *, details: Optional[str] = None) -> None:
    path_obj = Path(path)
    message = f"⚠️ Missing runtime file: {label}\nPath: {path_obj}"
    if details:
        message += f"\nDetails: {details}"
    _notify_runtime_error_once(message, dedupe_key=f"missing:{label}:{path_obj}")


def _get_id_to_names_path() -> Path:
    return BASE_DIR / "id_to_names.py"

_python_executable = str(sys.executable)
_python_executable_resolved = str(Path(sys.executable).resolve())
if "venv_catboost" not in _python_executable and "venv_catboost" not in _python_executable_resolved:
    logger.warning(
        "cyberscore_try.py is running outside venv_catboost: executable=%s resolved=%s",
        _python_executable,
        _python_executable_resolved,
    )

try:
    from ELO.domain import LeagueTier as _elo_live_LeagueTier, MatchRecord as _elo_live_MatchRecord
    from ELO.live_team_strength import (
        DEFAULT_RUNTIME_PROGRESS_PATH as _elo_live_default_progress_path,
        finalize_live_series_from_scores as _elo_live_finalize_series_from_scores,
        get_matchup_summary as _elo_live_get_matchup_summary,
        register_live_map_context as _elo_live_register_map_context,
    )
    ELO_LIVE_SNAPSHOT_AVAILABLE = True
except Exception as _elo_live_import_error:
    ELO_LIVE_SNAPSHOT_AVAILABLE = False
    _elo_live_default_progress_path = PROJECT_ROOT / "runtime" / "live_elo_progress.json"
    _elo_live_finalize_series_from_scores = None
    _elo_live_get_matchup_summary = None
    _elo_live_register_map_context = None
    _elo_live_LeagueTier = None
    _elo_live_MatchRecord = None
    logger.warning("Live ELO snapshot helper disabled: %s", _elo_live_import_error)

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


LIVE_ELO_ORPHAN_PENDING_MIN_AGE_SECONDS = _safe_int_env(
    "LIVE_ELO_ORPHAN_PENDING_MIN_AGE_SECONDS",
    120,
)


def _safe_float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return float(default)


def _get_tempo_helpers():
    global _tempo_build_tempo_draft_metrics, _tempo_load_tempo_dicts
    if _tempo_build_tempo_draft_metrics is not None and _tempo_load_tempo_dicts is not None:
        return _tempo_build_tempo_draft_metrics, _tempo_load_tempo_dicts
    try:
        from tempo_analise_database_experiment import build_tempo_draft_metrics as _build_tempo_draft_metrics
        from tempo_analise_database_experiment import load_tempo_dicts as _load_tempo_dicts
    except ImportError:
        from base.tempo_analise_database_experiment import build_tempo_draft_metrics as _build_tempo_draft_metrics
        from base.tempo_analise_database_experiment import load_tempo_dicts as _load_tempo_dicts
    _tempo_build_tempo_draft_metrics = _build_tempo_draft_metrics
    _tempo_load_tempo_dicts = _load_tempo_dicts
    return _tempo_build_tempo_draft_metrics, _tempo_load_tempo_dicts


def _detect_total_memory_bytes() -> Optional[int]:
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        phys_pages = int(os.sysconf("SC_PHYS_PAGES"))
        total = page_size * phys_pages
        if total > 0:
            return total
    except Exception:
        return None
    return None


def _stats_sharded_mode_enabled(label: str) -> bool:
    if label not in {"early", "late"}:
        return False
    mode = STATS_SHARDED_LOOKUP_MODE
    if mode in {"1", "true", "yes", "on", "always"}:
        return True
    if mode in {"0", "false", "no", "off", "never"}:
        return False
    total_memory_bytes = _detect_total_memory_bytes()
    if total_memory_bytes is None:
        return False
    total_memory_gb = float(total_memory_bytes) / float(1024 ** 3)
    return total_memory_gb <= float(STATS_SHARDED_LOOKUP_MAX_RAM_GB)


def _stats_key_leading_hero_id(key: Any) -> str:
    try:
        key_str = str(key)
    except Exception:
        return "misc"
    match = re.match(r"^(\d+)pos[1-5]", key_str)
    if match:
        return match.group(1)
    return "misc"


class _ShardedStatsLookup(dict):
    def __init__(self, shard_dir: Path, *, label: str, max_cached_shards: int = 24):
        super().__init__()
        self.shard_dir = Path(shard_dir)
        self.label = str(label)
        self.max_cached_shards = max(1, int(max_cached_shards))
        self._shards: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    def __bool__(self) -> bool:
        return True

    def _load_shard(self, shard_id: str) -> Dict[str, Any]:
        shard_id = str(shard_id or "misc")
        cached = self._shards.get(shard_id)
        if cached is not None:
            self._shards.move_to_end(shard_id)
            return cached

        shard_path = self.shard_dir / f"{shard_id}.jsonl"
        shard_data: Dict[str, Any] = {}
        if shard_path.exists():
            with shard_path.open("rb") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if not line:
                        continue
                    key, value = orjson.loads(line)
                    shard_data[str(key)] = value
        self._shards[shard_id] = shard_data
        self._shards.move_to_end(shard_id)
        while len(self._shards) > self.max_cached_shards:
            self._shards.popitem(last=False)
        return shard_data

    def warm_hero_ids(self, hero_ids: List[Any]) -> None:
        for hero_id in hero_ids:
            try:
                shard_id = str(int(hero_id))
            except (TypeError, ValueError):
                continue
            self._load_shard(shard_id)

    def get(self, key: Any, default=None):
        shard_id = _stats_key_leading_hero_id(key)
        shard = self._load_shard(shard_id)
        return shard.get(str(key), default)


def _prepare_sharded_stats_lookup(source_path: str, label: str) -> _ShardedStatsLookup:
    source = Path(source_path)
    shard_dir = source.parent / f"{source.stem}.shards"
    meta_path = shard_dir / "_meta.json"
    complete_path = shard_dir / "_complete"
    source_stat = source.stat()
    expected_meta = {
        "format_version": 1,
        "source_name": source.name,
        "source_size": int(source_stat.st_size),
        "source_mtime_ns": int(source_stat.st_mtime_ns),
    }

    rebuild_required = True
    if shard_dir.exists() and meta_path.exists() and complete_path.exists():
        try:
            current_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            rebuild_required = any(current_meta.get(k) != v for k, v in expected_meta.items())
        except Exception:
            rebuild_required = True

    if rebuild_required:
        if ijson is None:
            raise RuntimeError(f"ijson is required to build sharded stats for {label}")

        temp_dir = shard_dir.parent / f"{shard_dir.name}.tmp"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        file_handles: Dict[str, Any] = {}
        entries = 0
        print(f"🧱 Building sharded {label} stats from {source}")
        try:
            with source.open("rb") as f:
                for key, value in ijson.kvitems(f, ""):
                    shard_id = _stats_key_leading_hero_id(key)
                    handle = file_handles.get(shard_id)
                    if handle is None:
                        handle = (temp_dir / f"{shard_id}.jsonl").open("ab")
                        file_handles[shard_id] = handle
                    handle.write(orjson.dumps([key, value]))
                    handle.write(b"\n")
                    entries += 1
                    if STATS_SHARD_BUILD_PROGRESS_EVERY > 0 and entries % STATS_SHARD_BUILD_PROGRESS_EVERY == 0:
                        print(f"   📚 {label} shards progress: {entries:,} rows")
        finally:
            for handle in file_handles.values():
                try:
                    handle.close()
                except Exception:
                    pass

        meta_payload = dict(expected_meta)
        meta_payload["entries"] = int(entries)
        meta_path_tmp = temp_dir / "_meta.json"
        meta_path_tmp.write_text(json.dumps(meta_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        complete_path_tmp = temp_dir / "_complete"
        complete_path_tmp.write_text("ok\n", encoding="utf-8")

        if shard_dir.exists():
            shutil.rmtree(shard_dir)
        temp_dir.rename(shard_dir)
        print(f"✅ Built sharded {label} stats: {entries:,} rows -> {shard_dir}")

    print(f"🧠 Using sharded {label} stats backend: {shard_dir}")
    return _ShardedStatsLookup(
        shard_dir,
        label=label,
        max_cached_shards=STATS_SHARD_CACHE_MAX,
    )


def _warm_draft_stats_shards(radiant_heroes_and_pos: dict, dire_heroes_and_pos: dict) -> None:
    hero_ids: List[int] = []
    for side in (radiant_heroes_and_pos, dire_heroes_and_pos):
        if not isinstance(side, dict):
            continue
        for pos in ("pos1", "pos2", "pos3", "pos4", "pos5"):
            hero_payload = side.get(pos) or {}
            try:
                hero_id = int(hero_payload.get("hero_id"))
            except (TypeError, ValueError):
                continue
            if hero_id > 0:
                hero_ids.append(hero_id)
    if not hero_ids:
        return
    unique_hero_ids = sorted(set(hero_ids))
    for stats_obj in (early_dict, late_dict):
        if isinstance(stats_obj, _ShardedStatsLookup):
            stats_obj.warm_hero_ids(unique_hero_ids)


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
DELAYED_SIGNAL_TARGET_GAME_TIME = (20 * 60) + 20
DELAYED_SIGNAL_POLL_SECONDS = 15
DELAYED_SIGNAL_NO_PROGRESS_TIMEOUT_SECONDS = 2 * 60 * 60
DELAYED_SIGNAL_NO_DATA_TIMEOUT_SECONDS = 4 * 60 * 60
# Networth-gated dispatch rules (target team is resolved by star direction sign).
NETWORTH_GATE_HARD_BLOCK_SECONDS = 4 * 60
NETWORTH_GATE_EARLY_WINDOW_END_SECONDS = 10 * 60
NETWORTH_GATE_4_TO_10_MIN_DIFF = 800.0
NETWORTH_GATE_10_MIN_MAX_LOSS = -1500.0
NETWORTH_GATE_EARLY_CORE_LOW_WR_MIN_LEAD = 800.0
NETWORTH_GATE_TIER1_EARLY65_WINDOW_END_SECONDS = 13 * 60
NETWORTH_GATE_TIER1_EARLY65_4_TO_10_MIN_DIFF = 600.0
NETWORTH_GATE_TIER1_EARLY65_10_TO_13_MAX_LOSS = -1500.0
NETWORTH_GATE_STRONG_SAME_SIGN_MAX_LOSS = -800.0
NETWORTH_GATE_EARLY_CORE_MONITOR_DIFF = 1500.0
NETWORTH_GATE_LATE_NO_EARLY_DIFF = 1500.0
NETWORTH_GATE_LATE_OPPOSITE_DIFF = 3000.0
NETWORTH_GATE_LATE_OPPOSITE_EARLY90_4_TO_10_DIFF = 2000.0
NETWORTH_GATE_LATE_OPPOSITE_EARLY90_UNDERDOG_10_TO_20_DIFF = 1500.0
NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_WINDOW_START_SECONDS = 17 * 60
NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_DIFF = 3000.0
NETWORTH_GATE_LATE_COMEBACK_LARGE_DEFICIT = 14000.0
NETWORTH_MONITOR_HOLD_SECONDS = max(0, _safe_int_env("NETWORTH_MONITOR_HOLD_SECONDS", 60))
NETWORTH_STATUS_PRE4_BLOCK = "pre4_block"
NETWORTH_STATUS_4_10_SEND_800 = "4_10_send_800"
NETWORTH_STATUS_MIN10_LOSS_LE800_SEND = "minute10_loss_le800_send"
NETWORTH_STATUS_MIN10_LOSS_LE1500_SEND = "minute10_loss_le1500_send"
NETWORTH_STATUS_MIN10_LEAD_GE800_SEND = "minute10_lead_ge800_send"
NETWORTH_STATUS_TIER1_EARLY65_4_10_SEND_600 = "early65_4_10_send_600"
NETWORTH_STATUS_TIER1_EARLY65_10_13_LOSS_LE1500_SEND = "early65_10_13_loss_le1500_send"
NETWORTH_STATUS_STRONG_SAME_SIGN_MONITOR_WAIT_800 = "strong_same_sign_monitor_wait_800"
NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_1500 = "early_core_monitor_wait_1500"
NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_800 = "early_core_monitor_wait_800"
NETWORTH_STATUS_EARLY_CORE_FALLBACK_20_20_SEND = "early_core_fallback_20_20_send"
NETWORTH_STATUS_EARLY_CORE_TIMEOUT_NO_SEND = "early_core_timeout_no_send"
NETWORTH_STATUS_LATE_CORE_MONITOR_WAIT_800 = "late_core_monitor_wait_800"
NETWORTH_STATUS_LATE_CORE_TIMEOUT_NO_SEND = "late_core_timeout_no_send"
NETWORTH_STATUS_LATE_MONITOR_WAIT_1500 = "late_monitor_wait_1500"
NETWORTH_STATUS_LATE_CONFLICT_WAIT_1500 = "late_conflict_wait_1500"
NETWORTH_STATUS_LATE_CONFLICT_WAIT_2000 = "late_conflict_wait_2000"
NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000 = "late_conflict_wait_3000"
NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_WAIT = "late_top25_elo_block_wait_3000"
NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TARGET_LEAD_SEND = "late_top25_elo_block_target_lead_send"
NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TIMEOUT_NO_SEND = "late_top25_elo_block_timeout_no_send"
NETWORTH_STATUS_LATE_FALLBACK_20_20_SEND = "late_fallback_20_20_send"
NETWORTH_STATUS_LATE_FALLBACK_20_20_DEFICIT_NO_SEND = "late_fallback_20_20_deficit_no_send"
NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT = "late_comeback_monitor_wait"
NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND = "late_comeback_timeout_no_send"
TIER_SIGNAL_MIN_THRESHOLD_TIER1_BASE = 60
TIER_SIGNAL_MIN_THRESHOLD_TIER2_BASE = 60
ELO_UNDERDOG_GUARD_FAVORITE_EDGE_PP = 15.0
ELO_UNDERDOG_GUARD_MIN_SIGNAL_WR = 70.0
ELO_BLOCK_WR_MIN_AFTER_PENALTY = 58.5
EARLY_STAR_LATE_CORE_HIGH_CONFIDENCE_WR = 70.0
OPPOSITE_SIGNS_EARLY90_TRIGGER_WR = 90.0
OPPOSITE_SIGNS_EARLY90_ELO_GAP_PP = 15.0
TIER_THRESHOLD_STATUS_TIER1_MIN60_BLOCK = "tier1_min60_block"
TIER_THRESHOLD_STATUS_TIER2_MIN60_BLOCK = "tier2_min60_block"
TIER_THRESHOLD_REASON_TIER1_MIN60_BLOCK = "below_tier1_min60"
TIER_THRESHOLD_REASON_TIER2_MIN60_BLOCK = "below_tier2_min60"
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
# - optionally use separate MAP_ID_CHECK_PATH
# - optionally disable add_url persistence to keep matches re-analysed every cycle
MAP_ID_CHECK_PATH = str(os.getenv("MAP_ID_CHECK_PATH", "map_id_check.txt")).strip() or "map_id_check.txt"
MAP_ID_CHECK_PATH_ODDS_DEFAULT = str(PROJECT_ROOT / "map_id_check_test.txt")
DELAYED_QUEUE_PATH = str(
    os.getenv("DELAYED_QUEUE_PATH", "runtime/delayed_signal_queue.json")
).strip() or "runtime/delayed_signal_queue.json"
SENT_SIGNAL_JOURNAL_PATH = str(
    os.getenv("SENT_SIGNAL_JOURNAL_PATH", "runtime/sent_signal_recovery.jsonl")
).strip() or "runtime/sent_signal_recovery.jsonl"
SENT_SIGNAL_JOURNAL_FALLBACK_PATH = str(
    os.getenv("SENT_SIGNAL_JOURNAL_FALLBACK_PATH", "runtime/sent_signal_recovery_fallback.jsonl")
).strip() or "runtime/sent_signal_recovery_fallback.jsonl"
UNCERTAIN_SIGNAL_DELIVERY_PATH = str(
    os.getenv("UNCERTAIN_SIGNAL_DELIVERY_PATH", "runtime/uncertain_signal_delivery.jsonl")
).strip() or "runtime/uncertain_signal_delivery.jsonl"
UNCERTAIN_SIGNAL_DELIVERY_FALLBACK_PATH = str(
    os.getenv(
        "UNCERTAIN_SIGNAL_DELIVERY_FALLBACK_PATH",
        "runtime/uncertain_signal_delivery_fallback.jsonl",
    )
).strip() or "runtime/uncertain_signal_delivery_fallback.jsonl"
RUNTIME_INSTANCE_LOCK_PATH = str(
    os.getenv("RUNTIME_INSTANCE_LOCK_PATH", "runtime/cyberscore_try.instance.lock")
).strip() or "runtime/cyberscore_try.instance.lock"
TEST_DISABLE_ADD_URL = _safe_bool_env("TEST_DISABLE_ADD_URL", False)
FORCE_ODDS_SIGNAL_TEST = _safe_bool_env("FORCE_ODDS_SIGNAL_TEST", False)
DELAYED_SIGNAL_RETRY_BACKOFF_BASE_SECONDS = _safe_int_env("DELAYED_SIGNAL_RETRY_BACKOFF_BASE_SECONDS", 60)
DELAYED_SIGNAL_RETRY_BACKOFF_MAX_SECONDS = _safe_int_env("DELAYED_SIGNAL_RETRY_BACKOFF_MAX_SECONDS", 15 * 60)
TEMPO_OVER_FALLBACK_ENABLED = _safe_bool_env("TEMPO_OVER_FALLBACK_ENABLED", True)
TEMPO_OVER_SCORE_THRESHOLD = _safe_float_env("TEMPO_OVER_SCORE_THRESHOLD", 0.9965)
TEMPO_OVER_SCORE_LABEL = str(os.getenv("TEMPO_OVER_SCORE_LABEL", "Ставка >=48")).strip() or "Ставка >=48"
TEMPO_STATS_DIR_DEFAULT = str(
    os.getenv("TEMPO_STATS_DIR", str(TEMPO_EXPERIMENT_DIR))
).strip() or str(TEMPO_EXPERIMENT_DIR)

try:
    STAR_THRESHOLD_WR_TIER1 = int(os.getenv("STAR_THRESHOLD_WR_TIER1", "60"))
except ValueError:
    STAR_THRESHOLD_WR_TIER1 = 60
try:
    STAR_THRESHOLD_WR_TIER2 = int(os.getenv("STAR_THRESHOLD_WR_TIER2", "60"))
except ValueError:
    STAR_THRESHOLD_WR_TIER2 = 60

TIER_SIGNAL_MIN_THRESHOLD_TIER1 = max(
    TIER_SIGNAL_MIN_THRESHOLD_TIER1_BASE,
    STAR_THRESHOLD_WR_TIER1,
)
TIER_SIGNAL_MIN_THRESHOLD_TIER2 = max(
    TIER_SIGNAL_MIN_THRESHOLD_TIER2_BASE,
    STAR_THRESHOLD_WR_TIER2,
)
if STAR_THRESHOLD_WR_TIER2 < TIER_SIGNAL_MIN_THRESHOLD_TIER2_BASE:
    logger.warning(
        "STAR_THRESHOLD_WR_TIER2=%s is below %s; clamped for Tier2 signals",
        STAR_THRESHOLD_WR_TIER2,
        TIER_SIGNAL_MIN_THRESHOLD_TIER2_BASE,
    )

# Global/Tier filters for star signal qualification.
STAR_REQUIRE_EARLY_WITH_LATE_SAME_SIGN = _safe_bool_env(
    "STAR_REQUIRE_EARLY_WITH_LATE_SAME_SIGN",
    False,
)
# Если early/late star в разных знаках, не отбрасываем сигнал, а переводим в delayed до target game_time.
STAR_DELAY_ON_OPPOSITE_SIGNS = _safe_bool_env("STAR_DELAY_ON_OPPOSITE_SIGNS", True)
# По умолчанию Tier2 использует тот же min WR=60; fallback до Tier1 больше не нужен.
STAR_ALLOW_TIER2_FALLBACK_TO_TIER1 = _safe_bool_env("STAR_ALLOW_TIER2_FALLBACK_TO_TIER1", False)
STAR_REQUIRE_TIER2_LATE_STAR = _safe_bool_env("STAR_REQUIRE_TIER2_LATE_STAR", True)
STAR_REQUIRE_TIER2_SAME_SIGN = _safe_bool_env("STAR_REQUIRE_TIER2_SAME_SIGN", False)
STAR_ALLOW_TIER1_EARLY_STAR_LATE_SAME_OR_ZERO = _safe_bool_env(
    "STAR_ALLOW_TIER1_EARLY_STAR_LATE_SAME_OR_ZERO",
    True,
)
STAR_ALLOW_IMMEDIATE_EARLY_STAR65 = _safe_bool_env(
    "STAR_ALLOW_IMMEDIATE_EARLY_STAR65",
    False,
)
STAR_ALLOW_LATE_STAR_EARLY_SAME_OR_ZERO = _safe_bool_env(
    "STAR_ALLOW_LATE_STAR_EARLY_SAME_OR_ZERO",
    True,
)
STAR_ALLOW_TOP25_LATE_ELO_BLOCK_OPPOSITE_MONITOR = _safe_bool_env(
    "STAR_ALLOW_TOP25_LATE_ELO_BLOCK_OPPOSITE_MONITOR",
    True,
)
TOP25_LATE_ELO_BLOCK_RANK_THRESHOLD = _safe_int_env(
    "TOP25_LATE_ELO_BLOCK_RANK_THRESHOLD",
    25,
)
STAR_CONFIDENCE_CALIBRATION_PATH = Path(
    os.getenv(
        "STAR_CONFIDENCE_CALIBRATION_PATH",
        str(DATA_DIR / "star_confidence_calibration.json"),
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
if not (DATA_DIR / "star_thresholds_by_wr.json").exists():
    _report_missing_runtime_file("star_thresholds_by_wr.json", DATA_DIR / "star_thresholds_by_wr.json")
if not STAR_CONFIDENCE_CALIBRATION_PATH.exists():
    _report_missing_runtime_file("star_confidence_calibration.json", STAR_CONFIDENCE_CALIBRATION_PATH)
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
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    session_started_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")
    log_file.write(
        "\n"
        + "=" * 24
        + f" RUN START {session_started_at} pid={os.getpid()} "
        + "=" * 24
        + "\n"
    )
    log_file.flush()

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


def _release_runtime_instance_lock() -> None:
    global runtime_instance_lock_handle
    handle = globals().get("runtime_instance_lock_handle")
    runtime_instance_lock_handle = None
    if handle is None:
        return
    try:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        handle.close()
    except Exception:
        pass


def _odds_requested_flag(raw_odds: Any) -> bool:
    if raw_odds is None:
        return _safe_bool_env("BOOKMAKER_PREFETCH_ENABLED", False)
    if isinstance(raw_odds, str):
        return raw_odds.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(raw_odds)


def _runtime_instance_mode_label(raw_odds: Any) -> str:
    return "odds" if _odds_requested_flag(raw_odds) else "no_odds"


def _mode_specific_runtime_path(base_path_raw: str, mode_label: str) -> Path:
    base_path = Path(base_path_raw)
    if not mode_label:
        return base_path
    if base_path.suffix:
        return base_path.with_name(f"{base_path.stem}.{mode_label}{base_path.suffix}")
    return base_path.with_name(f"{base_path.name}.{mode_label}")


def _runtime_instance_lock_path_for_mode(mode_label: str) -> Path:
    return _mode_specific_runtime_path(RUNTIME_INSTANCE_LOCK_PATH, mode_label)


def _delayed_queue_path_for_mode(mode_label: str) -> Path:
    return _mode_specific_runtime_path(DELAYED_QUEUE_PATH, mode_label)


def _try_acquire_runtime_instance_lock(*, mode_label: str) -> bool:
    global runtime_instance_lock_handle
    current_handle = globals().get("runtime_instance_lock_handle")
    if current_handle is not None:
        runtime_instance_lock_handle = current_handle
        return True
    if fcntl is None:
        logger.warning("fcntl is unavailable; runtime single-instance lock is disabled")
        return True

    lock_path = _runtime_instance_lock_path_for_mode(mode_label)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        owner_info = ""
        try:
            handle.seek(0)
            owner_info = handle.read().strip()
        except Exception:
            owner_info = ""
        try:
            handle.close()
        except Exception:
            pass
        owner_suffix = f" owner={owner_info}" if owner_info else ""
        logger.error("Runtime lock is already held for mode=%s: %s%s", mode_label, lock_path, owner_suffix)
        print(
            f"⛔ Второй процесс запрещен: runtime lock уже занят "
            f"(mode={mode_label}): {lock_path}{owner_suffix}"
        )
        return False

    runtime_instance_lock_handle = handle
    payload = {
        "pid": os.getpid(),
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "cwd": os.getcwd(),
        "mode": mode_label,
    }
    try:
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps(payload, ensure_ascii=False))
        handle.flush()
        os.fsync(handle.fileno())
    except Exception:
        logger.exception("Failed to write runtime lock payload into %s", lock_path)
    return True


def _clear_journal_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "wb") as temp_file:
            temp_file.write(b"")
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
    finally:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass


def _append_journal_entry_to_path(path: Path, entry: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = orjson.dumps(entry) + b"\n"
    with path.open("ab") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())


def _write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "wb") as temp_file:
            temp_file.write(orjson.dumps(data))
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
    finally:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass


atexit.register(_release_runtime_instance_lock)


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

        # Ориентируемся на самый сильный starred-индекс.
        metric_ratios: List[float] = []
        for metric, value in star_only_data.items():
            threshold = thresholds_by_metric.get(metric)
            if threshold is None:
                return None
            metric_ratios.append(abs(value) / float(threshold))
        if not metric_ratios:
            return None

        strongest_ratio = max(metric_ratios)
        if strongest_ratio < 1.0:
            return None

        best_level = 60
        for min_ratio, wr_level in _STAR_INDEX_WR_MULTIPLIER_TO_LEVEL:
            if strongest_ratio >= min_ratio:
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
        # Уровень WR валиден, если хотя бы одна STAR-метрика проходит порог
        # своего индекса на этом WR-уровне.
        level_ok = False
        for metric, value in star_only_data.items():
            threshold = threshold_map.get(metric)
            if threshold is not None and abs(value) >= threshold:
                level_ok = True
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
        if key in {
            "counterpick_1vs1",
            "pos1_vs_pos1",
            "counterpick_1vs2",
            "solo",
            "synergy_duo",
            "synergy_trio",
        }
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
    "pos1_vs_pos1",
    "counterpick_1vs2",
    "solo",
    "synergy_duo",
    "synergy_trio",
)
_STAR_SUPPORT_METRIC_ORDER = (
    "counterpick_1vs1",
    "solo",
)
_STAR_LATE_CORE_METRIC_ORDER = (
    "counterpick_1vs1",
    "counterpick_1vs2",
    "solo",
)
_STAR_METRIC_SHORT = {
    "counterpick_1vs1": "cp1v1",
    "pos1_vs_pos1": "pos1vpos1",
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
    payload = STAR_THRESHOLDS_BY_WR.get(wr_level)
    if not isinstance(payload, dict):
        payload = STAR_THRESHOLDS_BY_WR.get(60, {}) if wr_level == 60 else {}
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
    support_diag = _block_signs_same_or_zero(
        raw_block=block,
        expected_sign=block_sign,
        metrics=_STAR_SUPPORT_METRIC_ORDER,
        allow_zero=False,
    )
    support_present = [
        metric
        for metric in _STAR_SUPPORT_METRIC_ORDER
        if _coerce_metric_value(block.get(metric)) is not None
    ]
    support_missing = [metric for metric in _STAR_SUPPORT_METRIC_ORDER if metric not in support_present]
    if not bool(support_diag.get("valid")) or support_missing:
        return {
            "valid": False,
            "status": "support_invalid",
            "sign": block_sign,
            "hit_metrics": hit_metrics,
            "conflict_metric": None,
            "support_status": str(support_diag.get("status") or "unknown"),
            "support_nonzero_metrics": list(support_diag.get("nonzero_metrics") or []),
            "support_conflicting_metrics": list(support_diag.get("conflicting_metrics") or []),
            "support_zero_metrics": list(support_diag.get("zero_metrics") or []),
            "support_missing_metrics": support_missing,
        }

    return {
        "valid": block_sign in (-1, 1),
        "status": "ok" if block_sign in (-1, 1) else "no_sign",
        "sign": block_sign,
        "hit_metrics": hit_metrics,
        "conflict_metric": None,
        "support_status": str(support_diag.get("status") or "unknown"),
        "support_nonzero_metrics": list(support_diag.get("nonzero_metrics") or []),
        "support_conflicting_metrics": list(support_diag.get("conflicting_metrics") or []),
        "support_zero_metrics": list(support_diag.get("zero_metrics") or []),
        "support_missing_metrics": [],
    }


def _format_star_block_status(diag: Dict[str, Any]) -> str:
    status = str(diag.get("status") or "unknown")
    if status == "ok":
        return "ok"
    if status == "elo_wr_below_min60":
        adjusted_wr_pct = diag.get("elo_adjusted_wr_pct")
        penalty_pp = diag.get("elo_wr_penalty_pp")
        if adjusted_wr_pct is not None and penalty_pp is not None:
            return (
                f"elo_wr_below_min60(adj={float(adjusted_wr_pct):.1f},"
                f"penalty={float(penalty_pp):.1f})"
            )
        return "elo_wr_below_min60"
    if status == "no_hits":
        return "no_hits"
    if status == "conflict_hits":
        return "conflict_hits"
    if status == "conflict_sign":
        metric = str(diag.get("conflict_metric") or "")
        short = _STAR_METRIC_SHORT.get(metric, metric or "?")
        return f"conflict_sign({short})"
    if status == "support_invalid":
        support_status = str(diag.get("support_status") or "unknown")
        support_parts: List[str] = []
        for key in (
            "support_conflicting_metrics",
            "support_zero_metrics",
            "support_missing_metrics",
        ):
            for metric in diag.get(key) or []:
                short = _STAR_METRIC_SHORT.get(str(metric), str(metric))
                if short and short not in support_parts:
                    support_parts.append(short)
        if support_parts:
            return f"support_invalid({support_status}:{','.join(support_parts)})"
        return f"support_invalid({support_status})"
    return status


def _star_match_status_from_diags(early_diag: Dict[str, Any], late_diag: Dict[str, Any], match_tier: int) -> str:
    has_early_star = bool(early_diag.get("valid"))
    has_late_star = bool(late_diag.get("valid"))
    if not has_late_star:
        return "skip_no_late_star"
    early_sign = early_diag.get("sign") if has_early_star else None
    late_sign = late_diag.get("sign") if has_late_star else None
    if match_tier == 2 and STAR_REQUIRE_TIER2_SAME_SIGN:
        if not (has_early_star and early_sign == late_sign):
            return "skip_tier2_same_sign_required"
    if has_early_star and early_sign == late_sign:
        return "send_now_same_sign"
    if has_early_star and early_sign != late_sign:
        if not STAR_DELAY_ON_OPPOSITE_SIGNS:
            return "skip_opposite_signs_disabled"
        return "delay_late_only_opposite_signs"
    if STAR_REQUIRE_EARLY_WITH_LATE_SAME_SIGN:
        return "skip_early_required"
    return "delay_late_only_no_early"


def _block_signs_same_or_zero(
    raw_block: Optional[dict],
    expected_sign: Optional[int],
    metrics: Optional[Tuple[str, ...]] = None,
    allow_zero: bool = True,
) -> Dict[str, Any]:
    if expected_sign not in (-1, 1):
        return {
            "valid": False,
            "status": "no_expected_sign",
            "nonzero_metrics": [],
            "conflicting_metrics": [],
            "zero_metrics": [],
        }
    block = raw_block if isinstance(raw_block, dict) else {}
    metric_order = tuple(metrics) if metrics else _STAR_METRIC_ORDER
    nonzero_metrics: List[str] = []
    conflicting_metrics: List[str] = []
    zero_metrics: List[str] = []
    for metric in metric_order:
        value = _coerce_metric_value(block.get(metric))
        if value is None:
            continue
        if value == 0:
            if not allow_zero:
                zero_metrics.append(metric)
            continue
        nonzero_metrics.append(metric)
        sign = 1 if value > 0 else -1
        if sign != expected_sign:
            conflicting_metrics.append(metric)
    valid = len(conflicting_metrics) == 0 and len(zero_metrics) == 0
    status = "ok"
    if conflicting_metrics:
        status = "conflict_signs"
    elif zero_metrics:
        status = "zero_not_allowed"
    return {
        "valid": valid,
        "status": status,
        "nonzero_metrics": nonzero_metrics,
        "conflicting_metrics": conflicting_metrics,
        "zero_metrics": zero_metrics,
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


def _build_star_metrics_snapshot(
    *,
    early_block_log: str,
    mid_block_log: str,
    raw_star_early_summary: str,
    raw_star_late_summary: str,
    star_diag_lines: list[str],
) -> Dict[str, Any]:
    return {
        "early_block_log": str(early_block_log or ""),
        "mid_block_log": str(mid_block_log or ""),
        "raw_star_early_summary": str(raw_star_early_summary or ""),
        "raw_star_late_summary": str(raw_star_late_summary or ""),
        "star_diag_lines": [str(line) for line in (star_diag_lines or []) if str(line)],
    }


def _print_star_metrics_snapshot(snapshot: Optional[dict], label: str = "delayed") -> None:
    if not isinstance(snapshot, dict):
        return
    title = "   📊 STAR метрики:" if not label else f"   📊 STAR метрики ({label}):"
    print(title)
    early_block_log = str(snapshot.get("early_block_log") or "")
    mid_block_log = str(snapshot.get("mid_block_log") or "")
    if early_block_log:
        print("      " + early_block_log.rstrip().replace("\n", "\n      "))
    if mid_block_log:
        print("      " + mid_block_log.rstrip().replace("\n", "\n      "))
    star_diag_lines = [str(line) for line in (snapshot.get("star_diag_lines") or []) if str(line)]
    if star_diag_lines:
        print(f"   📉 Star checks: {' | '.join(star_diag_lines)}")


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


def _networth_monitor_hold_check(
    *,
    current_game_time: Optional[float],
    target_networth_diff: Optional[float],
    monitor_threshold: Optional[float],
    hold_started_game_time: Optional[float],
    hold_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    try:
        hold_required_seconds = (
            float(hold_seconds)
            if hold_seconds is not None
            else float(NETWORTH_MONITOR_HOLD_SECONDS)
        )
    except (TypeError, ValueError):
        hold_required_seconds = float(NETWORTH_MONITOR_HOLD_SECONDS)
    hold_required_seconds = max(0.0, hold_required_seconds)

    try:
        threshold_value = float(monitor_threshold) if monitor_threshold is not None else None
    except (TypeError, ValueError):
        threshold_value = None
    try:
        target_diff_value = (
            float(target_networth_diff) if target_networth_diff is not None else None
        )
    except (TypeError, ValueError):
        target_diff_value = None
    try:
        game_time_value = float(current_game_time) if current_game_time is not None else None
    except (TypeError, ValueError):
        game_time_value = None
    try:
        hold_started_value = (
            float(hold_started_game_time)
            if hold_started_game_time is not None
            else None
        )
    except (TypeError, ValueError):
        hold_started_value = None

    enabled = bool(
        threshold_value is not None
        and threshold_value > 0
        and hold_required_seconds > 0.0
    )
    threshold_met = bool(
        threshold_value is not None
        and target_diff_value is not None
        and target_diff_value >= threshold_value
    )
    if not enabled:
        return {
            "enabled": False,
            "ready": threshold_met,
            "threshold_met": threshold_met,
            "hold_started_game_time": hold_started_value if threshold_met else None,
            "held_seconds": 0.0,
            "hold_seconds": hold_required_seconds,
        }
    if not threshold_met or game_time_value is None:
        return {
            "enabled": True,
            "ready": False,
            "threshold_met": threshold_met,
            "hold_started_game_time": None,
            "held_seconds": 0.0,
            "hold_seconds": hold_required_seconds,
        }
    effective_hold_started = (
        hold_started_value if hold_started_value is not None else game_time_value
    )
    held_seconds = max(0.0, game_time_value - effective_hold_started)
    return {
        "enabled": True,
        "ready": held_seconds >= hold_required_seconds,
        "threshold_met": True,
        "hold_started_game_time": effective_hold_started,
        "held_seconds": held_seconds,
        "hold_seconds": hold_required_seconds,
    }


def _fallback_max_deficit_abs_for_delay_reason(
    delay_reason: Optional[str],
    *,
    monitor_threshold: Optional[float] = None,
) -> Optional[float]:
    if monitor_threshold is not None:
        try:
            return abs(float(monitor_threshold))
        except (TypeError, ValueError):
            return None
    reason = str(delay_reason or "").strip().lower()
    if reason in {"late_only_no_early_star_wait_1500", "late_only_no_early_same_sign"}:
        return abs(float(NETWORTH_GATE_LATE_NO_EARLY_DIFF))
    if reason == "late_only_opposite_signs":
        return abs(float(NETWORTH_GATE_LATE_OPPOSITE_DIFF))
    if reason == "early_star_late_core_wait_1500":
        return abs(float(NETWORTH_GATE_EARLY_CORE_MONITOR_DIFF))
    if reason == "early_star_late_core_low_wr_wait_800":
        return abs(float(NETWORTH_GATE_EARLY_CORE_LOW_WR_MIN_LEAD))
    if reason == "late_star_early_core_wait_800":
        return abs(float(NETWORTH_GATE_4_TO_10_MIN_DIFF))
    if reason == "strong_same_sign_wait_800_then_comeback_ceiling":
        return abs(float(NETWORTH_GATE_STRONG_SAME_SIGN_MAX_LOSS))
    return None


def _fallback_networth_deficit_guard_decision(
    *,
    target_networth_diff: Optional[float],
    max_deficit_abs: Optional[float],
) -> Dict[str, Any]:
    try:
        threshold_abs = float(max_deficit_abs) if max_deficit_abs is not None else None
    except (TypeError, ValueError):
        threshold_abs = None
    try:
        target_diff = float(target_networth_diff) if target_networth_diff is not None else None
    except (TypeError, ValueError):
        target_diff = None
    deficit = abs(target_diff) if target_diff is not None and target_diff < 0 else 0.0
    return {
        "reject": bool(
            threshold_abs is not None
            and target_diff is not None
            and target_diff < -threshold_abs
        ),
        "threshold_abs": threshold_abs,
        "target_diff": target_diff,
        "deficit": deficit,
    }


def _resolve_signal_wr_for_elo_guard(
    *,
    target_side: Optional[str],
    has_selected_early_star: bool,
    has_selected_late_star: bool,
    selected_early_sign: Optional[int],
    selected_late_sign: Optional[int],
    early_wr_pct: Optional[float],
    late_wr_pct: Optional[float],
) -> Optional[Dict[str, Any]]:
    candidates: List[Tuple[str, float]] = []
    early_side = _target_side_from_sign(selected_early_sign) if has_selected_early_star else None
    late_side = _target_side_from_sign(selected_late_sign) if has_selected_late_star else None
    if (
        has_selected_early_star
        and early_wr_pct is not None
        and early_side == target_side
    ):
        candidates.append(("early", float(early_wr_pct)))
    if (
        has_selected_late_star
        and late_wr_pct is not None
        and late_side == target_side
    ):
        candidates.append(("late", float(late_wr_pct)))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[1], reverse=True)
    best_source, best_wr_pct = candidates[0]
    if len(candidates) > 1:
        best_source = "best_of_early_late"
    return {
        "wr_pct": float(best_wr_pct),
        "source": best_source,
        "candidates": {label: float(value) for label, value in candidates},
    }


def _elo_underdog_guard_decision(
    *,
    team_elo_meta: Optional[Dict[str, Any]],
    target_side: Optional[str],
    signal_wr_pct: Optional[float],
    favorite_edge_pp: float = ELO_UNDERDOG_GUARD_FAVORITE_EDGE_PP,
    min_signal_wr: float = ELO_UNDERDOG_GUARD_MIN_SIGNAL_WR,
) -> Optional[Dict[str, Any]]:
    if not isinstance(team_elo_meta, dict):
        return None
    if target_side not in {"radiant", "dire"}:
        return None
    try:
        radiant_wr = float(team_elo_meta.get("adjusted_radiant_wr"))
        dire_wr = float(team_elo_meta.get("adjusted_dire_wr"))
    except (TypeError, ValueError):
        return None

    favorite_side = "radiant" if radiant_wr >= dire_wr else "dire"
    favorite_wr = max(radiant_wr, dire_wr)
    target_elo_wr = radiant_wr if target_side == "radiant" else dire_wr
    edge_from_even_pp = favorite_wr - 50.0
    if target_side == favorite_side:
        return None
    if edge_from_even_pp < float(favorite_edge_pp):
        return None

    reject = signal_wr_pct is None or float(signal_wr_pct) < float(min_signal_wr)
    return {
        "reject": bool(reject),
        "favorite_side": favorite_side,
        "favorite_wr": float(favorite_wr),
        "target_side": target_side,
        "target_elo_wr": float(target_elo_wr),
        "signal_wr_pct": float(signal_wr_pct) if signal_wr_pct is not None else None,
        "favorite_edge_pp": float(edge_from_even_pp),
        "min_signal_wr": float(min_signal_wr),
    }


def _team_elo_wr_for_side(
    team_elo_meta: Optional[Dict[str, Any]],
    side: Optional[str],
) -> Optional[float]:
    if not isinstance(team_elo_meta, dict):
        return None
    if side == "radiant":
        key = "adjusted_radiant_wr"
    elif side == "dire":
        key = "adjusted_dire_wr"
    else:
        return None
    try:
        return float(team_elo_meta.get(key))
    except (TypeError, ValueError):
        return None


def _team_elo_rank_for_side(
    team_elo_meta: Optional[Dict[str, Any]],
    side: Optional[str],
) -> Optional[int]:
    if not isinstance(team_elo_meta, dict):
        return None
    if side == "radiant":
        key = "radiant_leaderboard_rank"
    elif side == "dire":
        key = "dire_leaderboard_rank"
    else:
        return None
    try:
        rank_value = team_elo_meta.get(key)
        return int(rank_value) if rank_value is not None else None
    except (TypeError, ValueError):
        return None


def _elo_block_wr_penalty_pp(
    team_elo_meta: Optional[Dict[str, Any]],
    target_side: Optional[str],
) -> float:
    side_wr = _team_elo_wr_for_side(team_elo_meta, target_side)
    if side_wr is None:
        return 0.0
    return max(0.0, 50.0 - float(side_wr))


def _apply_elo_block_wr_guard(
    *,
    diag: Dict[str, Any],
    block_wr_pct: Optional[float],
    team_elo_meta: Optional[Dict[str, Any]],
    min_adjusted_wr: float = ELO_BLOCK_WR_MIN_AFTER_PENALTY,
) -> Dict[str, Any]:
    out = dict(diag or {})
    raw_valid = bool(out.get("valid"))
    out["raw_valid"] = raw_valid
    out["raw_status"] = str(out.get("status") or "unknown")
    target_side = _target_side_from_sign(out.get("sign"))
    target_elo_wr = _team_elo_wr_for_side(team_elo_meta, target_side)
    penalty_pp = _elo_block_wr_penalty_pp(team_elo_meta, target_side)
    adjusted_wr_pct: Optional[float]
    try:
        adjusted_wr_pct = (
            float(block_wr_pct) - float(penalty_pp)
            if block_wr_pct is not None
            else None
        )
    except (TypeError, ValueError):
        adjusted_wr_pct = None

    out["block_wr_pct"] = float(block_wr_pct) if block_wr_pct is not None else None
    out["elo_target_side"] = target_side
    out["elo_target_wr"] = float(target_elo_wr) if target_elo_wr is not None else None
    out["elo_wr_penalty_pp"] = float(penalty_pp)
    out["elo_adjusted_wr_pct"] = float(adjusted_wr_pct) if adjusted_wr_pct is not None else None
    out["elo_block_min_wr"] = float(min_adjusted_wr)

    if not raw_valid or adjusted_wr_pct is None:
        return out
    if float(adjusted_wr_pct) >= float(min_adjusted_wr):
        return out

    out["valid"] = False
    out["status"] = "elo_wr_below_min60"
    return out


def _top25_late_elo_block_opposite_monitor_override(
    *,
    team_elo_meta: Optional[Dict[str, Any]],
    selected_early_diag: Dict[str, Any],
    selected_late_diag: Dict[str, Any],
    raw_selected_early_diag: Dict[str, Any],
    raw_selected_late_diag: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if not STAR_ALLOW_TOP25_LATE_ELO_BLOCK_OPPOSITE_MONITOR:
        return None
    if bool(selected_late_diag.get("valid")):
        return None
    if str(selected_late_diag.get("status") or "") != "elo_wr_below_min60":
        return None
    if not bool(raw_selected_late_diag.get("valid")):
        return None
    raw_late_sign = raw_selected_late_diag.get("sign")
    if raw_late_sign not in (-1, 1):
        return None

    override_mode: Optional[str] = None
    raw_early_sign = raw_selected_early_diag.get("sign")
    if bool(selected_early_diag.get("valid")) and bool(raw_selected_early_diag.get("valid")):
        if raw_early_sign not in (-1, 1):
            return None
        if raw_early_sign == raw_late_sign:
            return None
        override_mode = "opposite_signs"
    else:
        raw_early_status = str(raw_selected_early_diag.get("status") or "")
        selected_early_status = str(selected_early_diag.get("status") or "")
        if raw_early_status != "no_hits" or selected_early_status != "no_hits":
            return None
        override_mode = "no_early"

    late_side = _target_side_from_sign(raw_late_sign)
    if late_side not in {"radiant", "dire"}:
        return None
    late_rank = _team_elo_rank_for_side(team_elo_meta, late_side)
    if late_rank is None or late_rank > int(TOP25_LATE_ELO_BLOCK_RANK_THRESHOLD):
        return None
    late_elo_wr = _team_elo_wr_for_side(team_elo_meta, late_side)
    return {
        "enabled": True,
        "target_sign": int(raw_late_sign),
        "target_side": late_side,
        "leaderboard_rank": int(late_rank),
        "elo_target_wr": float(late_elo_wr) if late_elo_wr is not None else None,
        "mode": override_mode,
        "window_start_seconds": float(NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_WINDOW_START_SECONDS),
        "window_threshold": float(NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_DIFF),
        "target_game_time": float(DELAYED_SIGNAL_TARGET_GAME_TIME),
        "profile": "late_top25_elo_block_opposite_monitor",
    }


def _opposite_signs_early90_monitor_config(
    *,
    team_elo_meta: Optional[Dict[str, Any]],
    early_wr_pct: Optional[float],
    selected_early_sign: Optional[int],
    selected_late_sign: Optional[int],
) -> Optional[Dict[str, Any]]:
    try:
        early_wr_value = float(early_wr_pct) if early_wr_pct is not None else None
    except (TypeError, ValueError):
        early_wr_value = None
    if early_wr_value is None or early_wr_value < float(OPPOSITE_SIGNS_EARLY90_TRIGGER_WR):
        return None
    early_side = _target_side_from_sign(selected_early_sign)
    late_side = _target_side_from_sign(selected_late_sign)
    if early_side not in {"radiant", "dire"} or late_side not in {"radiant", "dire"}:
        return None
    if early_side == late_side:
        return None

    early_elo_wr = _team_elo_wr_for_side(team_elo_meta, early_side)
    late_elo_wr = _team_elo_wr_for_side(team_elo_meta, late_side)
    elo_gap_pp = None
    early_is_elo_underdog_by_gap = False
    if early_elo_wr is not None and late_elo_wr is not None:
        elo_gap_pp = float(late_elo_wr - early_elo_wr)
        early_is_elo_underdog_by_gap = bool(
            late_elo_wr > early_elo_wr and elo_gap_pp >= float(OPPOSITE_SIGNS_EARLY90_ELO_GAP_PP)
        )

    threshold_10_to_20 = (
        float(NETWORTH_GATE_LATE_OPPOSITE_EARLY90_UNDERDOG_10_TO_20_DIFF)
        if early_is_elo_underdog_by_gap
        else float(NETWORTH_GATE_LATE_OPPOSITE_DIFF)
    )
    status_10_to_20 = (
        NETWORTH_STATUS_LATE_CONFLICT_WAIT_1500
        if early_is_elo_underdog_by_gap
        else NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000
    )
    return {
        "enabled": True,
        "profile": "late_only_opposite_signs_early90",
        "early_side": early_side,
        "late_side": late_side,
        "early_wr_pct": float(early_wr_value),
        "early_elo_wr": float(early_elo_wr) if early_elo_wr is not None else None,
        "late_elo_wr": float(late_elo_wr) if late_elo_wr is not None else None,
        "elo_gap_pp": float(elo_gap_pp) if elo_gap_pp is not None else None,
        "early_is_elo_underdog_by_gap": bool(early_is_elo_underdog_by_gap),
        "threshold_4_to_10": float(NETWORTH_GATE_LATE_OPPOSITE_EARLY90_4_TO_10_DIFF),
        "threshold_10_to_20": float(threshold_10_to_20),
        "status_4_to_10": NETWORTH_STATUS_LATE_CONFLICT_WAIT_2000,
        "status_10_to_20": status_10_to_20,
    }


def _dynamic_monitor_snapshot_for_payload(
    payload: Optional[Dict[str, Any]],
    game_time_seconds: Optional[float],
) -> Dict[str, Any]:
    threshold_raw = None
    status_label = ""
    if isinstance(payload, dict):
        threshold_raw = payload.get("networth_monitor_threshold")
        status_label = str(payload.get("dispatch_status_label") or "")
    try:
        threshold_value = float(threshold_raw) if threshold_raw is not None else None
    except (TypeError, ValueError):
        threshold_value = None
    snapshot = {
        "threshold": threshold_value,
        "status_label": status_label,
        "profile": str((payload or {}).get("dynamic_monitor_profile") or ""),
    }
    if not isinstance(payload, dict):
        return snapshot
    try:
        current_game_time = float(game_time_seconds) if game_time_seconds is not None else None
    except (TypeError, ValueError):
        current_game_time = None
    if current_game_time is None:
        return snapshot

    if snapshot["profile"] == "late_top25_elo_block_opposite_monitor":
        if current_game_time < float(NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_WINDOW_START_SECONDS):
            snapshot["threshold"] = None
            return snapshot
        next_threshold_raw = payload.get(
            "networth_monitor_threshold_17_to_20",
            payload.get("networth_monitor_threshold"),
        )
        try:
            next_threshold = float(next_threshold_raw) if next_threshold_raw is not None else None
        except (TypeError, ValueError):
            next_threshold = None
        next_status_label = str(
            payload.get("networth_monitor_status_17_to_20")
            or payload.get("dispatch_status_label")
            or snapshot["status_label"]
            or ""
        )
        snapshot["threshold"] = next_threshold
        snapshot["status_label"] = next_status_label
        return snapshot

    if snapshot["profile"] != "late_only_opposite_signs_early90":
        return snapshot

    threshold_key = (
        "networth_monitor_threshold_4_to_10"
        if current_game_time < float(NETWORTH_GATE_EARLY_WINDOW_END_SECONDS)
        else "networth_monitor_threshold_10_to_20"
    )
    status_key = (
        "networth_monitor_status_4_to_10"
        if current_game_time < float(NETWORTH_GATE_EARLY_WINDOW_END_SECONDS)
        else "networth_monitor_status_10_to_20"
    )
    next_threshold_raw = payload.get(threshold_key)
    try:
        next_threshold = float(next_threshold_raw) if next_threshold_raw is not None else None
    except (TypeError, ValueError):
        next_threshold = None
    next_status_label = str(payload.get(status_key) or snapshot["status_label"] or "")
    snapshot["threshold"] = next_threshold
    snapshot["status_label"] = next_status_label
    return snapshot


def _format_game_clock(game_time_seconds: Any) -> str:
    try:
        sec = max(0.0, float(game_time_seconds or 0.0))
    except (TypeError, ValueError):
        sec = 0.0
    return f"{int(sec // 60):02d}:{int(sec % 60):02d}"


def _comeback_delta_pp_for_side(
    comeback_metrics: Optional[dict],
    side: Optional[str],
) -> Optional[float]:
    if side not in {"radiant", "dire"} or not isinstance(comeback_metrics, dict):
        return None
    side_metrics = comeback_metrics.get(side)
    if not isinstance(side_metrics, dict):
        return None
    value = side_metrics.get("delta_pp")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _late_comeback_monitor_entry_for_game_time(
    game_time_seconds: Any,
) -> tuple[Optional[int], Optional[float]]:
    if not isinstance(late_comeback_ceiling_thresholds, dict) or not late_comeback_ceiling_thresholds:
        return None, None
    try:
        minute = int(max(0.0, float(game_time_seconds or 0.0)) // 60)
    except (TypeError, ValueError):
        return None, None
    threshold_raw = late_comeback_ceiling_thresholds.get(str(minute))
    try:
        threshold = float(threshold_raw) if threshold_raw is not None else None
    except (TypeError, ValueError):
        threshold = None
    return minute, threshold


def _late_comeback_monitor_deadline_seconds() -> Optional[float]:
    try:
        max_minute = int(late_comeback_ceiling_max_minute)
    except (TypeError, ValueError):
        return None
    return float((max_minute + 1) * 60)


def _late_comeback_monitor_check(
    *,
    game_time_seconds: Any,
    target_networth_diff: Optional[float],
) -> Dict[str, Any]:
    minute, threshold = _late_comeback_monitor_entry_for_game_time(game_time_seconds)
    try:
        target_diff = float(target_networth_diff) if target_networth_diff is not None else None
    except (TypeError, ValueError):
        target_diff = None
    deficit = None
    if target_diff is not None:
        deficit = abs(target_diff) if target_diff < 0 else 0.0
    ready = bool(deficit is not None and threshold is not None and deficit <= threshold)
    return {
        "minute": minute,
        "threshold": threshold,
        "deficit": deficit,
        "ready": ready,
    }


def _post_target_comeback_ceiling_decision(
    *,
    game_time_seconds: Any,
    target_networth_diff: Optional[float],
) -> Dict[str, Any]:
    check = _late_comeback_monitor_check(
        game_time_seconds=game_time_seconds,
        target_networth_diff=target_networth_diff,
    )
    deadline_game_time = _late_comeback_monitor_deadline_seconds()
    try:
        current_game_time = float(game_time_seconds or 0.0)
    except (TypeError, ValueError):
        current_game_time = None
    threshold = check.get("threshold")
    ready = bool(check.get("ready"))
    should_monitor = bool(
        threshold is not None
        and not ready
        and deadline_game_time is not None
        and current_game_time is not None
        and current_game_time < deadline_game_time
    )
    should_timeout = bool(
        threshold is not None
        and not ready
        and deadline_game_time is not None
        and current_game_time is not None
        and current_game_time >= deadline_game_time
    )
    return {
        **check,
        "available": bool(threshold is not None),
        "deadline_game_time": deadline_game_time,
        "should_monitor": should_monitor,
        "should_timeout": should_timeout,
    }


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
        next_retry_at_raw = payload.get("next_retry_at")
        try:
            next_retry_at = float(next_retry_at_raw) if next_retry_at_raw is not None else 0.0
        except (TypeError, ValueError):
            next_retry_at = 0.0
        if next_retry_at > now_ts:
            continue
        target_game_time = float(payload.get('target_game_time', DELAYED_SIGNAL_TARGET_GAME_TIME))
        delayed_state = _fetch_delayed_match_state(payload.get('json_url'))
        if not isinstance(delayed_state, dict):
            queued_at = float(payload.get('queued_at', now_ts))
            # После 4 часов без game_time считаем сигнал устаревшим.
            if now_ts - queued_at > DELAYED_SIGNAL_NO_DATA_TIMEOUT_SECONDS:
                _drop_delayed_match(match_key, reason="no_data_timeout")
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
        _update_delayed_match(
            match_key,
            last_game_time=float(current_game_time),
            last_checked_at=float(now_ts),
            last_progress_at=float(last_progress_at),
        )

        monitor_snapshot = _dynamic_monitor_snapshot_for_payload(payload, current_game_time)
        monitor_threshold: Optional[float] = None
        monitor_threshold_raw = monitor_snapshot.get("threshold", payload.get("networth_monitor_threshold"))
        if monitor_threshold_raw is not None:
            try:
                monitor_threshold = float(monitor_threshold_raw)
            except (TypeError, ValueError):
                monitor_threshold = None
        fallback_max_deficit_abs = _fallback_max_deficit_abs_for_delay_reason(
            payload.get("reason"),
            monitor_threshold=monitor_threshold,
        )
        fallback_max_deficit_raw = payload.get("fallback_max_deficit_abs")
        if fallback_max_deficit_raw is not None:
            try:
                fallback_max_deficit_abs = abs(float(fallback_max_deficit_raw))
            except (TypeError, ValueError):
                fallback_max_deficit_abs = fallback_max_deficit_abs
        monitor_target_side = str(payload.get("networth_target_side") or "").strip().lower()
        if monitor_target_side not in {"radiant", "dire"}:
            payload_details = payload.get("add_url_details")
            if isinstance(payload_details, dict):
                monitor_target_side = str(payload_details.get("target_side") or "").strip().lower()
        monitor_deadline_raw = payload.get("networth_monitor_deadline_game_time", target_game_time)
        try:
            monitor_deadline_game_time = float(monitor_deadline_raw)
        except (TypeError, ValueError):
            monitor_deadline_game_time = float(target_game_time)
        dynamic_status_label = str(monitor_snapshot.get("status_label") or payload.get("dispatch_status_label") or "")
        if dynamic_status_label and (
            payload.get("dispatch_status_label") != dynamic_status_label
            or payload.get("networth_monitor_threshold") != monitor_threshold
        ):
            _update_delayed_match(
                match_key,
                dispatch_status_label=dynamic_status_label,
                networth_monitor_threshold=monitor_threshold,
            )
        monitor_hold_seconds_raw = payload.get(
            "networth_monitor_hold_seconds",
            NETWORTH_MONITOR_HOLD_SECONDS,
        )
        try:
            monitor_hold_seconds = max(0.0, float(monitor_hold_seconds_raw))
        except (TypeError, ValueError):
            monitor_hold_seconds = float(NETWORTH_MONITOR_HOLD_SECONDS)
        monitor_hold_started_raw = payload.get("networth_monitor_hold_started_game_time")
        try:
            monitor_hold_started_game_time = (
                float(monitor_hold_started_raw)
                if monitor_hold_started_raw is not None
                else None
            )
        except (TypeError, ValueError):
            monitor_hold_started_game_time = None
        late_comeback_monitor_active = bool(payload.get("late_comeback_monitor_active"))
        late_comeback_monitor_candidate = bool(payload.get("late_comeback_monitor_candidate"))
        late_comeback_force_after_target = bool(payload.get("late_comeback_force_after_target"))
        late_comeback_deadline_raw = payload.get(
            "late_comeback_monitor_deadline_game_time",
            _late_comeback_monitor_deadline_seconds(),
        )
        try:
            late_comeback_deadline_game_time = (
                float(late_comeback_deadline_raw)
                if late_comeback_deadline_raw is not None
                else None
            )
        except (TypeError, ValueError):
            late_comeback_deadline_game_time = None
        late_comeback_delta_raw = payload.get("late_comeback_delta_pp")
        try:
            late_comeback_delta_pp = (
                float(late_comeback_delta_raw)
                if late_comeback_delta_raw is not None
                else None
            )
        except (TypeError, ValueError):
            late_comeback_delta_pp = None
        monitor_ready = False
        monitor_target_diff: Optional[float] = None
        late_comeback_check: Optional[Dict[str, Any]] = None
        monitor_hold_check: Optional[Dict[str, Any]] = None
        if monitor_target_side in {"radiant", "dire"}:
            monitor_target_diff = _target_networth_diff_from_radiant_lead(
                current_radiant_lead,
                monitor_target_side,
            )
        if late_comeback_monitor_active:
            late_comeback_check = _late_comeback_monitor_check(
                game_time_seconds=current_game_time,
                target_networth_diff=monitor_target_diff,
            )
            if late_comeback_check.get("ready"):
                monitor_ready = True
            elif (
                late_comeback_deadline_game_time is not None
                and current_game_time >= late_comeback_deadline_game_time
            ):
                monitor_ready = False
        elif (
            late_comeback_force_after_target
            and current_game_time >= target_game_time
            and monitor_target_diff is not None
        ):
            late_comeback_check = _late_comeback_monitor_check(
                game_time_seconds=current_game_time,
                target_networth_diff=monitor_target_diff,
            )
            if late_comeback_check.get("ready"):
                monitor_ready = True
                late_comeback_monitor_active = True
            elif (
                late_comeback_deadline_game_time is not None
                and current_game_time < late_comeback_deadline_game_time
            ):
                _update_delayed_match(
                    match_key,
                    late_comeback_monitor_active=True,
                    late_comeback_force_after_target=False,
                    late_comeback_monitor_deadline_game_time=float(late_comeback_deadline_game_time),
                    target_game_time=float(late_comeback_deadline_game_time),
                    send_on_target_game_time=False,
                    dispatch_status_label=NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT,
                )
                minute_label = late_comeback_check.get("minute")
                threshold_label = late_comeback_check.get("threshold")
                print(
                    "⏳ Late comeback monitor activated: "
                    f"{match_key} (source=strong_same_sign, game_time={int(current_game_time)}, "
                    f"target_diff={int(monitor_target_diff)}, "
                    f"minute={minute_label}, "
                    f"ceiling={int(threshold_label) if threshold_label is not None else 'n/a'}, "
                    f"deadline={_format_game_clock(late_comeback_deadline_game_time)})"
                )
                continue
        elif (
            late_comeback_monitor_candidate
            and late_comeback_delta_pp is not None
            and late_comeback_delta_pp > 0
            and current_game_time >= target_game_time
            and monitor_target_diff is not None
            and monitor_target_diff <= -NETWORTH_GATE_LATE_COMEBACK_LARGE_DEFICIT
        ):
            late_comeback_check = _late_comeback_monitor_check(
                game_time_seconds=current_game_time,
                target_networth_diff=monitor_target_diff,
            )
            if late_comeback_check.get("ready"):
                monitor_ready = True
                late_comeback_monitor_active = True
            elif (
                late_comeback_deadline_game_time is not None
                and current_game_time < late_comeback_deadline_game_time
            ):
                _update_delayed_match(
                    match_key,
                    late_comeback_monitor_active=True,
                    late_comeback_monitor_deadline_game_time=float(late_comeback_deadline_game_time),
                    target_game_time=float(late_comeback_deadline_game_time),
                    send_on_target_game_time=False,
                    dispatch_status_label=NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT,
                )
                minute_label = late_comeback_check.get("minute")
                threshold_label = late_comeback_check.get("threshold")
                print(
                    "⏳ Late comeback monitor activated: "
                    f"{match_key} (game_time={int(current_game_time)}, "
                    f"target_diff={int(monitor_target_diff)}, "
                    f"comeback_delta={late_comeback_delta_pp:+.2f} pp, "
                    f"minute={minute_label}, "
                    f"ceiling={int(threshold_label) if threshold_label is not None else 'n/a'}, "
                    f"deadline={_format_game_clock(late_comeback_deadline_game_time)})"
                )
                continue
        elif (
            late_comeback_monitor_candidate
            and current_game_time >= target_game_time
            and monitor_target_diff is not None
        ):
            post_target_comeback = _post_target_comeback_ceiling_decision(
                game_time_seconds=current_game_time,
                target_networth_diff=monitor_target_diff,
            )
            if post_target_comeback.get("ready"):
                monitor_ready = True
                late_comeback_monitor_active = True
                late_comeback_check = post_target_comeback
                late_comeback_deadline_game_time = post_target_comeback.get("deadline_game_time")
            elif post_target_comeback.get("should_monitor"):
                late_comeback_deadline_game_time = post_target_comeback.get("deadline_game_time")
                updated_add_url_details = dict(payload.get("add_url_details") or {})
                updated_add_url_details.update(
                    {
                        "dispatch_status_label": NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT,
                        "target_game_time": int(late_comeback_deadline_game_time or target_game_time),
                        "networth_target_side": monitor_target_side,
                        "target_networth_diff": float(monitor_target_diff),
                        "late_comeback_monitor_minute": post_target_comeback.get("minute"),
                        "late_comeback_monitor_threshold": post_target_comeback.get("threshold"),
                    }
                )
                _update_delayed_match(
                    match_key,
                    reason="post_target_comeback_ceiling_monitor",
                    dispatch_status_label=NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT,
                    add_url_details=updated_add_url_details,
                    target_game_time=float(late_comeback_deadline_game_time or target_game_time),
                    send_on_target_game_time=False,
                    late_comeback_monitor_active=True,
                    late_comeback_force_after_target=False,
                    late_comeback_monitor_deadline_game_time=float(late_comeback_deadline_game_time or target_game_time),
                )
                print(
                    "⏳ Post-target comeback monitor activated: "
                    f"{match_key} (game_time={int(current_game_time)}, "
                    f"target_diff={int(monitor_target_diff)}, "
                    f"minute={post_target_comeback.get('minute')}, "
                    f"ceiling={int(post_target_comeback.get('threshold') or 0)}, "
                    f"deadline={_format_game_clock(late_comeback_deadline_game_time)})"
                )
                continue
            elif post_target_comeback.get("should_timeout"):
                late_comeback_monitor_active = True
                late_comeback_check = post_target_comeback
                late_comeback_deadline_game_time = post_target_comeback.get("deadline_game_time")
        if (
            monitor_threshold is not None
            and monitor_target_side in {"radiant", "dire"}
            and current_game_time < monitor_deadline_game_time
        ):
            if monitor_target_diff is not None:
                monitor_hold_check = _networth_monitor_hold_check(
                    current_game_time=current_game_time,
                    target_networth_diff=monitor_target_diff,
                    monitor_threshold=monitor_threshold,
                    hold_started_game_time=monitor_hold_started_game_time,
                    hold_seconds=monitor_hold_seconds,
                )
                next_hold_started = monitor_hold_check.get("hold_started_game_time")
                if next_hold_started != monitor_hold_started_game_time:
                    _update_delayed_match(
                        match_key,
                        networth_monitor_hold_started_game_time=next_hold_started,
                    )
                if bool(monitor_hold_check.get("ready")):
                    monitor_ready = True

        if not monitor_ready and current_game_time < target_game_time:
            if now_ts - last_progress_at > DELAYED_SIGNAL_NO_PROGRESS_TIMEOUT_SECONDS:
                _drop_delayed_match(match_key, reason="no_progress_timeout")
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
            reason = payload.get('reason', 'unknown')
            fallback_send_status_label = str(
                payload.get("fallback_send_status_label") or NETWORTH_STATUS_LATE_FALLBACK_20_20_SEND
            )
            send_on_target_game_time = bool(payload.get("send_on_target_game_time", True))
            add_url_reason = str(payload.get('add_url_reason') or 'star_signal_sent_delayed')
            add_url_details = payload.get('add_url_details')
            if not isinstance(add_url_details, dict):
                add_url_details = {}
            add_url_details = dict(add_url_details)
            star_metrics_snapshot = payload.get("star_metrics_snapshot")
            if late_comeback_monitor_active and not monitor_ready:
                timeout_status_label = NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND
                if (
                    late_comeback_deadline_game_time is not None
                    and current_game_time >= late_comeback_deadline_game_time
                ):
                    add_url_details.setdefault("dispatch_status_label", timeout_status_label)
                    add_url_details.setdefault("sent_game_time", int(current_game_time))
                    add_url_details.setdefault(
                        "late_comeback_monitor_reached",
                        False,
                    )
                    if late_comeback_check:
                        add_url_details.setdefault(
                            "late_comeback_monitor_minute",
                            late_comeback_check.get("minute"),
                        )
                        add_url_details.setdefault(
                            "late_comeback_monitor_threshold",
                            late_comeback_check.get("threshold"),
                        )
                        add_url_details.setdefault(
                            "target_networth_diff",
                            float(monitor_target_diff or 0.0),
                        )
                    add_url(
                        match_key,
                        reason="star_signal_rejected_late_comeback_monitor_timeout",
                        details=add_url_details,
                    )
                    _drop_delayed_match(match_key, reason="late_comeback_timeout")
                    print(
                        f"⏱️ Отложенный сигнал отменен без отправки: {match_key} "
                        f"(reason=late_comeback_monitor, status={timeout_status_label}, "
                        f"game_time={int(current_game_time)})"
                    )
                    continue
            if (
                reason == "late_top25_elo_block_opposite_monitor"
                and current_game_time >= target_game_time
                and not monitor_ready
            ):
                if monitor_target_diff is not None and monitor_target_diff > 0:
                    monitor_ready = True
                    add_url_reason = "star_signal_sent_now_top25_late_elo_block_target_lead"
                    add_url_details.setdefault(
                        "dispatch_status_label",
                        NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TARGET_LEAD_SEND,
                    )
                    add_url_details.setdefault(
                        "top25_late_elo_block_target_lead",
                        True,
                    )
                    add_url_details.setdefault(
                        "top25_late_elo_block_rank",
                        payload.get("top25_late_elo_block_rank"),
                    )
                else:
                    timeout_status_label = NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TIMEOUT_NO_SEND
                    add_url_details.setdefault("dispatch_status_label", timeout_status_label)
                    add_url_details.setdefault("sent_game_time", int(current_game_time))
                    add_url_details.setdefault("target_game_time", int(target_game_time))
                    add_url_details.setdefault("target_networth_diff", float(monitor_target_diff or 0.0))
                    add_url_details.setdefault(
                        "top25_late_elo_block_rank",
                        payload.get("top25_late_elo_block_rank"),
                    )
                    add_url(
                        match_key,
                        reason="star_signal_rejected_top25_late_elo_block_timeout",
                        details=add_url_details,
                    )
                    _drop_delayed_match(match_key, reason="top25_late_elo_block_timeout")
                    print(
                        f"⏱️ Отложенный сигнал отменен без отправки: {match_key} "
                        f"(reason={reason}, status={timeout_status_label}, game_time={int(current_game_time)})"
                    )
                    continue
            if not monitor_ready and not send_on_target_game_time:
                timeout_add_url_reason = str(
                    payload.get("timeout_add_url_reason") or "star_signal_rejected_delayed_timeout"
                )
                timeout_status_label = str(
                    payload.get("timeout_status_label") or "delayed_timeout_no_send"
                )
                add_url_details.setdefault("dispatch_status_label", timeout_status_label)
                add_url_details.setdefault("sent_game_time", int(current_game_time))
                add_url_details.setdefault("target_game_time", int(target_game_time))
                add_url_details.setdefault("networth_monitor_reached", False)
                add_url(
                    match_key,
                    reason=timeout_add_url_reason,
                    details=add_url_details,
                )
                _drop_delayed_match(match_key, reason="target_reached_no_send")
                print(
                    f"⏱️ Отложенный сигнал отменен без отправки: {match_key} "
                    f"(reason={reason}, status={timeout_status_label}, game_time={int(current_game_time)})"
                )
                continue
            fallback_guard = _fallback_networth_deficit_guard_decision(
                target_networth_diff=monitor_target_diff,
                max_deficit_abs=fallback_max_deficit_abs,
            )
            if not monitor_ready and bool(fallback_guard.get("reject")):
                timeout_status_label = NETWORTH_STATUS_LATE_FALLBACK_20_20_DEFICIT_NO_SEND
                add_url_details.setdefault("dispatch_status_label", timeout_status_label)
                add_url_details.setdefault("sent_game_time", int(current_game_time))
                add_url_details.setdefault("target_game_time", int(target_game_time))
                add_url_details.setdefault("networth_monitor_reached", False)
                if fallback_guard.get("threshold_abs") is not None:
                    add_url_details.setdefault(
                        "fallback_max_deficit_abs",
                        float(fallback_guard.get("threshold_abs") or 0.0),
                    )
                if fallback_guard.get("target_diff") is not None:
                    add_url_details.setdefault(
                        "target_networth_diff",
                        float(fallback_guard.get("target_diff") or 0.0),
                    )
                add_url(
                    match_key,
                    reason="star_signal_rejected_fallback_networth_guard",
                    details=add_url_details,
                )
                _drop_delayed_match(match_key, reason="fallback_deficit_guard_no_send")
                print(
                    f"⏱️ Отложенный сигнал отменен без отправки: {match_key} "
                    f"(reason={reason}, status={timeout_status_label}, "
                    f"game_time={int(current_game_time)}, "
                    f"target_networth_diff={int(fallback_guard.get('target_diff') or 0)}, "
                    f"max_deficit={int(fallback_guard.get('threshold_abs') or 0)})"
                )
                continue
            add_url_details.setdefault('sent_game_time', int(current_game_time))
            if late_comeback_monitor_active and monitor_ready and monitor_target_diff is not None:
                add_url_details.setdefault("late_comeback_monitor_reached", True)
                add_url_details.setdefault("target_networth_diff", float(monitor_target_diff))
                if late_comeback_check:
                    add_url_details.setdefault(
                        "late_comeback_monitor_minute",
                        late_comeback_check.get("minute"),
                    )
                    add_url_details.setdefault(
                        "late_comeback_monitor_threshold",
                        late_comeback_check.get("threshold"),
                    )
            elif monitor_ready and monitor_target_diff is not None:
                add_url_details.setdefault("networth_monitor_early_release", True)
                if monitor_threshold is not None:
                    add_url_details.setdefault("networth_monitor_threshold", float(monitor_threshold))
                if isinstance(monitor_hold_check, dict) and monitor_hold_check.get("enabled"):
                    add_url_details.setdefault(
                        "networth_monitor_hold_seconds",
                        float(monitor_hold_check.get("hold_seconds") or 0.0),
                    )
                    if monitor_hold_check.get("hold_started_game_time") is not None:
                        add_url_details.setdefault(
                            "networth_monitor_hold_started_game_time",
                            float(monitor_hold_check.get("hold_started_game_time") or 0.0),
                        )
                    add_url_details.setdefault(
                        "networth_monitor_hold_elapsed_seconds",
                        float(monitor_hold_check.get("held_seconds") or 0.0),
                    )
                add_url_details.setdefault("target_networth_diff", float(monitor_target_diff))
            else:
                add_url_details.setdefault("dispatch_status_label", fallback_send_status_label)
            _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
            delivery_confirmed = _deliver_and_persist_signal(
                match_key,
                payload.get('message', ''),
                add_url_reason=add_url_reason,
                add_url_details=add_url_details,
            )
            if delivery_confirmed:
                if late_comeback_monitor_active and monitor_ready and monitor_target_diff is not None:
                    print(
                        f"⏱️ Отложенный сигнал отправлен по comeback ceiling: {match_key} "
                        f"(game_time={int(current_game_time)}, "
                        f"target_networth_diff={int(monitor_target_diff)}, "
                        f"minute={late_comeback_check.get('minute') if late_comeback_check else 'n/a'}, "
                        f"ceiling={int(late_comeback_check.get('threshold')) if late_comeback_check and late_comeback_check.get('threshold') is not None else 'n/a'})"
                    )
                elif (
                    reason == "late_top25_elo_block_opposite_monitor"
                    and current_game_time >= target_game_time
                    and monitor_target_diff is not None
                ):
                    print(
                        f"⏱️ Отложенный сигнал отправлен по top25 late ELO-block target lead: {match_key} "
                        f"(game_time={int(current_game_time)}, target_networth_diff={int(monitor_target_diff)})"
                    )
                elif monitor_ready and monitor_target_diff is not None:
                    monitor_desc = (
                        f"threshold={int(monitor_threshold)}"
                        if monitor_threshold is not None
                        else "monitor=unknown"
                    )
                    print(
                        f"⏱️ Отложенный сигнал отправлен раньше fallback: {match_key} "
                        f"(reason={reason}, game_time={int(current_game_time)}, "
                        f"target_networth_diff={int(monitor_target_diff)}, "
                        f"{monitor_desc})"
                    )
                else:
                    print(
                        f"⏱️ Отложенный сигнал отправлен: {match_key} "
                        f"(reason={reason}, status={fallback_send_status_label}, game_time={int(current_game_time)})"
                    )
        except Exception as e:
            print(f"⚠️ Ошибка отправки отложенного сигнала {match_key}: {e}")
            _schedule_delayed_retry(match_key, e, now_ts=now_ts)
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
        odds = site_payload.get("odds")
        match_odds = site_payload.get("match_odds")
        if not bool(site_payload.get("market_closed")) and isinstance(odds, list) and len(odds) >= 2:
            try:
                p1 = float(odds[0])
                p2 = float(odds[1])
                lines.append(f"{site_label}: П1 {p1:.2f} | П2 {p2:.2f}")
                has_numeric_odds = True
                continue
            except (TypeError, ValueError):
                pass
        if isinstance(match_odds, list) and len(match_odds) >= 2:
            try:
                p1 = float(match_odds[0])
                p2 = float(match_odds[1])
                lines.append(f"{site_label} (матч): П1 {p1:.2f} | П2 {p2:.2f}")
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
            "match_odds": list(item.get("match_odds") or []),
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
                        "match_odds": list(getattr(site_result, "match_odds", []) or []),
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

try:
    with (BASE_DIR / 'hero_valid_positions_simple.json').open('r', encoding='utf-8') as f:
        HERO_VALID_POSITIONS_DICT = json.load(f)
except Exception as e:
    HERO_VALID_POSITIONS_DICT = {}
    print(f"⚠️ Не удалось загрузить hero_valid_positions_simple.json: {e}")
    _report_missing_runtime_file("hero_valid_positions_simple.json", BASE_DIR / "hero_valid_positions_simple.json", details=str(e))
try:
    with (BASE_DIR / 'hero_valid_positions_counts_500k.json').open('r', encoding='utf-8') as f:
        _raw_counts = json.load(f)
    HERO_POSITION_COUNTS = {
        str(k): v for k, v in _raw_counts.items()
        if str(k).isdigit() and isinstance(v, dict)
    }
except Exception as e:
    HERO_POSITION_COUNTS = {}
    print(f"⚠️ Не удалось загрузить hero_valid_positions_counts_500k.json: {e}")
    _report_missing_runtime_file(
        "hero_valid_positions_counts_500k.json",
        BASE_DIR / "hero_valid_positions_counts_500k.json",
        details=str(e),
    )
try:
    with (BASE_DIR / 'hero_valid_positions_counts_500k.json').open('r', encoding='utf-8') as f:
        HERO_ID_TO_NAME = json.load(f)
except Exception as e:
    HERO_ID_TO_NAME = {}
    print(f"⚠️ Не удалось загрузить hero_valid_positions_counts_500k.json (hero names fallback): {e}")
    _report_missing_runtime_file(
        "hero_valid_positions_counts_500k.json (hero names fallback)",
        BASE_DIR / "hero_valid_positions_counts_500k.json",
        details=str(e),
    )


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
verbose_match_log_cache = set()
verbose_match_log_lock = threading.Lock()
uncertain_delivery_urls_cache = set()
uncertain_delivery_urls_lock = threading.Lock()
map_id_check_lock = threading.Lock()
delayed_queue_lock = threading.Lock()
sent_signal_journal_lock = threading.Lock()
uncertain_delivery_lock = threading.Lock()
signal_send_guard = set()
signal_send_guard_lock = threading.Lock()
runtime_instance_lock_handle = None

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
KILLS_PRIORS_CACHE_PATH = ML_MODELS_DIR / "pro_kills_priors.json"

# Ленивая загрузка словарей
lane_data = None
early_dict = None
late_dict = None
comeback_dict = None
comeback_meta = None
comeback_baseline_wr_pct = None
tempo_solo_dict = None
tempo_duo_dict = None
tempo_cp1v1_dict = None
late_comeback_ceiling_data = None
late_comeback_ceiling_thresholds = None
late_comeback_ceiling_max_minute = None
STATS_SEQUENTIAL_WARMUP_ENABLED = _safe_bool_env("STATS_SEQUENTIAL_WARMUP_ENABLED", True)
STATS_WARMUP_STEP_DELAY_SECONDS = _safe_float_env("STATS_WARMUP_STEP_DELAY_SECONDS", 45.0)
STATS_SHARDED_LOOKUP_MODE = str(os.getenv("STATS_SHARDED_LOOKUP_MODE", "auto")).strip().lower() or "auto"
STATS_SHARDED_LOOKUP_MAX_RAM_GB = _safe_float_env("STATS_SHARDED_LOOKUP_MAX_RAM_GB", 8.0)
STATS_SHARD_CACHE_MAX = _safe_int_env("STATS_SHARD_CACHE_MAX", 24)
STATS_SHARD_BUILD_PROGRESS_EVERY = _safe_int_env("STATS_SHARD_BUILD_PROGRESS_EVERY", 500000)
stats_warmup_last_heavy_load_ts = 0.0

# Настройка прокси
USE_BOOKMAKER_PROXY_FOR_MATCHES = _safe_bool_env("USE_BOOKMAKER_PROXY_FOR_MATCHES", True)
if USE_BOOKMAKER_PROXY_FOR_MATCHES:
    _proxy_candidates: list[str] = []
    if isinstance(BOOKMAKER_PROXY_POOL, (list, tuple, set)):
        for item in BOOKMAKER_PROXY_POOL:
            candidate = str(item).strip()
            if candidate and candidate not in _proxy_candidates:
                _proxy_candidates.append(candidate)
    candidate = str(BOOKMAKER_PROXY_URL).strip() if BOOKMAKER_PROXY_URL else ""
    if candidate and candidate not in _proxy_candidates:
        _proxy_candidates.append(candidate)
    PROXY_LIST = _proxy_candidates
else:
    PROXY_LIST = [str(p).strip() for p in api_to_proxy.keys() if str(p).strip()]
CURRENT_PROXY_INDEX = 0
CURRENT_PROXY = None
PROXIES = {}
USE_PROXY = None
GET_HEADS_FAILURE_REASON_LIVE_MATCHES_MISSING_ALL_PROXIES = "live_matches_missing_after_all_proxies"
GET_HEADS_FAILURE_REASON_REQUEST_FAILED = "request_failed"
GET_HEADS_LAST_FAILURE_REASON = None
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
QUIET_HOURS_START_HOUR_MSK = 3
QUIET_HOURS_END_HOUR_MSK = 7
NEXT_SCHEDULE_SLEEP_SECONDS = 0.0
NEXT_SCHEDULE_MATCH_INFO: Optional[Dict[str, Any]] = None
PENDING_SCHEDULE_WAKE_AUDIT: Optional[Dict[str, Any]] = None
SCHEDULE_LIVE_WAIT_TARGET: Optional[Dict[str, Any]] = None
SCHEDULE_WAKE_LEAD_SECONDS = _safe_float_env("SCHEDULE_WAKE_LEAD_SECONDS", 30.0 * 60.0)
SCHEDULE_MAX_SLEEP_SECONDS = _safe_float_env("SCHEDULE_MAX_SLEEP_SECONDS", 15.0 * 60.0)
SCHEDULE_NEAR_MATCH_POLL_SECONDS = _safe_float_env("SCHEDULE_NEAR_MATCH_POLL_SECONDS", 60.0)
SCHEDULE_POST_START_POLL_SECONDS = _safe_float_env("SCHEDULE_POST_START_POLL_SECONDS", 3.0 * 60.0)


def _env_use_proxy_default() -> bool:
    env_use_proxy = os.getenv("USE_PROXY")
    if env_use_proxy is None:
        return True
    return env_use_proxy.strip().lower() not in {"0", "false", "no", "off"}


def _compute_moscow_quiet_hours_sleep_seconds(now: Optional[datetime] = None) -> float:
    current = now.astimezone(MOSCOW_TZ) if now is not None else datetime.now(MOSCOW_TZ)
    if current.hour < QUIET_HOURS_START_HOUR_MSK or current.hour >= QUIET_HOURS_END_HOUR_MSK:
        return 0.0
    wake_at = current.replace(
        hour=QUIET_HOURS_END_HOUR_MSK,
        minute=0,
        second=0,
        microsecond=0,
    )
    return max(0.0, (wake_at - current).total_seconds())


def _parse_dltv_schedule_timestamp(raw_value: str) -> Optional[datetime]:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _compute_schedule_recheck_sleep_seconds(raw_sleep_seconds: float) -> float:
    try:
        raw_seconds = float(raw_sleep_seconds)
    except (TypeError, ValueError):
        return float(SCHEDULE_POST_START_POLL_SECONDS)
    if raw_seconds <= 0:
        return float(SCHEDULE_POST_START_POLL_SECONDS)
    return raw_seconds


def _should_poll_for_scheduled_live_target(now_utc: Optional[datetime] = None) -> bool:
    target = SCHEDULE_LIVE_WAIT_TARGET
    if not isinstance(target, dict):
        return False
    scheduled_at = target.get("scheduled_at_utc")
    if not isinstance(scheduled_at, datetime):
        return False
    current_utc = now_utc.astimezone(timezone.utc) if now_utc is not None else datetime.now(timezone.utc)
    return current_utc >= scheduled_at.astimezone(timezone.utc)


def _extract_nearest_scheduled_match_info(
    soup: BeautifulSoup,
    *,
    now_utc: Optional[datetime] = None,
) -> Optional[dict[str, Any]]:
    current_utc = now_utc.astimezone(timezone.utc) if now_utc is not None else datetime.now(timezone.utc)
    best_payload: Optional[dict[str, Any]] = None
    best_sleep_seconds: Optional[float] = None

    for event_tag in soup.find_all("a", class_="event"):
        time_tag = event_tag.find("div", class_="event__info-info__time")
        scheduled_at = _parse_dltv_schedule_timestamp(time_tag.get_text(" ", strip=True) if time_tag else "")
        if scheduled_at is None or scheduled_at <= current_utc:
            continue
        match_item = event_tag.find_next("div", class_="match__item")
        team_tags = match_item.find_all("div", class_="match__item-team__name") if match_item else []
        team_names = [tag.get_text(" ", strip=True) for tag in team_tags if tag.get_text(" ", strip=True)]
        matchup = " vs ".join(team_names[:2]) if team_names else "unknown"
        sleep_seconds_raw = max(0.0, (scheduled_at - current_utc).total_seconds())
        sleep_seconds = _compute_schedule_recheck_sleep_seconds(sleep_seconds_raw)
        if best_sleep_seconds is None or sleep_seconds_raw < best_sleep_seconds:
            best_sleep_seconds = sleep_seconds_raw
            best_payload = {
                "scheduled_at_utc": scheduled_at,
                "scheduled_at_msk": scheduled_at.astimezone(MOSCOW_TZ),
                "sleep_seconds": sleep_seconds,
                "sleep_seconds_raw": sleep_seconds_raw,
                "matchup": matchup,
            }

    return best_payload


def _format_schedule_match_label(schedule_info: Optional[Dict[str, Any]]) -> str:
    if not isinstance(schedule_info, dict):
        return "unknown"
    matchup = str(schedule_info.get("matchup") or "unknown")
    scheduled_at_msk = schedule_info.get("scheduled_at_msk")
    if isinstance(scheduled_at_msk, datetime):
        return f"{matchup} at {scheduled_at_msk.strftime('%Y-%m-%d %H:%M:%S MSK')}"
    return matchup


def _emit_pending_schedule_wake_audit(
    *,
    heads_count: int,
    bodies_count: int,
    next_schedule_info: Optional[Dict[str, Any]] = None,
    request_status: str = "ok",
) -> None:
    global PENDING_SCHEDULE_WAKE_AUDIT
    pending = PENDING_SCHEDULE_WAKE_AUDIT
    if not isinstance(pending, dict):
        return

    woke_at_msk = pending.get("woke_at_msk")
    woke_label = (
        woke_at_msk.strftime("%Y-%m-%d %H:%M:%S MSK")
        if isinstance(woke_at_msk, datetime)
        else datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S MSK")
    )
    target_label = _format_schedule_match_label(pending)
    proxy_marker = _get_current_proxy_marker()

    if heads_count > 0 and bodies_count > 0:
        print(
            "⏰ Wake audit: "
            f"woke at {woke_label} for {target_label}. "
            f"DLTV response after wake: live matches found "
            f"(heads={heads_count}, bodies={bodies_count}, proxy={proxy_marker}, request={request_status})"
        )
        PENDING_SCHEDULE_WAKE_AUDIT = None
        return

    next_label = _format_schedule_match_label(next_schedule_info)
    if isinstance(next_schedule_info, dict):
        print(
            "⏰ Wake audit: "
            f"woke at {woke_label} for {target_label}, but live matches are still empty "
            f"(heads={heads_count}, bodies={bodies_count}, proxy={proxy_marker}, request={request_status}). "
            f"DLTV now points to next scheduled match: {next_label}"
        )
    else:
        print(
            "⏰ Wake audit: "
            f"woke at {woke_label} for {target_label}, but live matches are still empty "
            f"(heads={heads_count}, bodies={bodies_count}, proxy={proxy_marker}, request={request_status}). "
            "DLTV did not expose a new scheduled match either"
        )
    PENDING_SCHEDULE_WAKE_AUDIT = None


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


def _get_current_proxy_marker() -> str:
    if USE_PROXY and CURRENT_PROXY:
        return str(CURRENT_PROXY)
    return "__direct__"


def _rotate_to_untried_proxy(tried_markers: set[str]) -> bool:
    if not PROXY_LIST or not USE_PROXY:
        return False
    for _ in range(len(PROXY_LIST)):
        rotate_proxy()
        marker = _get_current_proxy_marker()
        if marker not in tried_markers:
            return True
    return False


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

            id_to_names_path = _get_id_to_names_path()
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
    Если неизвестна — автоматически добавляет в Tier2 без уведомления в Telegram.
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
    send_message(err_msg, admin_only=True)
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
            _pro_matches_df = pd.read_csv(DATA_DIR / 'pro_matches_enriched.csv')
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
    rules_path = ML_MODELS_DIR / "kills_betting_rules.json"
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
    path = REPORTS_DIR / "team_kills_predictability.json"
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
    priors_path = ML_MODELS_DIR / "pub_hero_priors.json"
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

    meta_path = ML_MODELS_DIR / "live_cb_kills_reg_meta.json"
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
        reg_all.load_model(str(ML_MODELS_DIR / "live_cb_kills_reg.cbm"))
        models["reg_all"] = reg_all
        reg_low = CatBoostRegressor()
        reg_low.load_model(str(ML_MODELS_DIR / "live_cb_kills_reg_low.cbm"))
        models["reg_low"] = reg_low
        reg_high = CatBoostRegressor()
        reg_high.load_model(str(ML_MODELS_DIR / "live_cb_kills_reg_high.cbm"))
        models["reg_high"] = reg_high
        cls_low = CatBoostClassifier()
        cls_low.load_model(str(ML_MODELS_DIR / "live_cb_kills_low_cls.cbm"))
        models["cls_low"] = cls_low
        cls_high = CatBoostClassifier()
        cls_high.load_model(str(ML_MODELS_DIR / "live_cb_kills_high_cls.cbm"))
        models["cls_high"] = cls_high
    except Exception as e:
        logger.warning(f"Failed to load kills models: {e}")
        return False

    q10_path = ML_MODELS_DIR / "live_cb_kills_reg_q10.cbm"
    q90_path = ML_MODELS_DIR / "live_cb_kills_reg_q90.cbm"
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

    models_dir = ML_MODELS_DIR
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
    cache_path = KILLS_PRIORS_CACHE_PATH
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as f:
            cached = json.load(f)
        if cached.get("priors_version") == 8:
            return cached

    clean_path = PRO_HEROES_DIR / "json_parts_split_from_object" / "clean_data.json"
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


def _load_kills_priors_from_cache_only() -> Optional[Dict[str, Any]]:
    global KILLS_PRIORS
    if isinstance(KILLS_PRIORS, dict):
        return KILLS_PRIORS
    if not KILLS_PRIORS_CACHE_PATH.exists():
        return None
    try:
        with KILLS_PRIORS_CACHE_PATH.open("r", encoding="utf-8") as f:
            cached = json.load(f)
    except Exception:
        logger.exception("Failed to load kills priors cache for telegram ELO block")
        return None
    if cached.get("priors_version") != 8:
        return None
    KILLS_PRIORS = cached
    return cached


def _build_team_elo_matchup_summary_from_live_snapshot(
    radiant_team_id: Optional[int],
    dire_team_id: Optional[int],
    radiant_team_name: Optional[str] = None,
    dire_team_name: Optional[str] = None,
    radiant_account_ids: Optional[List[int]] = None,
    dire_account_ids: Optional[List[int]] = None,
    match_tier: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    if not ELO_LIVE_SNAPSHOT_AVAILABLE or _elo_live_get_matchup_summary is None:
        return None
    try:
        summary = _elo_live_get_matchup_summary(
            radiant_team_id=radiant_team_id,
            dire_team_id=dire_team_id,
            radiant_team_name=str(radiant_team_name or ""),
            dire_team_name=str(dire_team_name or ""),
            radiant_account_ids=list(radiant_account_ids or []),
            dire_account_ids=list(dire_account_ids or []),
            match_tier=match_tier,
            rebuild_if_missing=True,
        )
    except Exception:
        logger.exception("Failed to load live ELO snapshot for telegram signal")
        return None
    return summary if isinstance(summary, dict) else None


def _register_completed_live_map_for_elo(
    *,
    series_key: Any,
    series_url: str,
    map_key: str,
    first_team_score: Any,
    second_team_score: Any,
    first_team_is_radiant: bool,
    map_match_id: Any,
    observed_timestamp: Any,
    radiant_team_id: Optional[int],
    dire_team_id: Optional[int],
    radiant_team_name: str,
    dire_team_name: str,
    radiant_account_ids: Optional[List[int]],
    dire_account_ids: Optional[List[int]],
    league_id: Any,
    league_name: str,
    series_type: Any,
    match_tier: Optional[int],
) -> Optional[Dict[str, Any]]:
    if (
        not ELO_LIVE_SNAPSHOT_AVAILABLE
        or _elo_live_register_map_context is None
        or _elo_live_MatchRecord is None
        or _elo_live_LeagueTier is None
    ):
        return None

    normalized_series_key = str(series_key or "").strip() or str(series_url or "").strip()
    normalized_map_key = str(map_key or "").strip()
    if not normalized_series_key or not normalized_map_key:
        return None

    first_score = _coerce_int(first_team_score)
    second_score = _coerce_int(second_team_score)
    if first_score < 0 or second_score < 0:
        return None

    radiant_player_ids = tuple(
        int(pid) for pid in (radiant_account_ids or []) if _coerce_int(pid) > 0
    )
    dire_player_ids = tuple(
        int(pid) for pid in (dire_account_ids or []) if _coerce_int(pid) > 0
    )
    if len(radiant_player_ids) < 5 or len(dire_player_ids) < 5:
        return None

    try:
        tier_enum = _elo_live_LeagueTier(f"TIER{int(match_tier or 3)}")
    except Exception:
        tier_enum = _elo_live_LeagueTier.TIER3

    map_timestamp = _coerce_int(observed_timestamp) or int(time.time())
    series_id_value = _coerce_int(series_key)
    league_id_value = _coerce_int(league_id)
    map_match_id_value = _coerce_int(map_match_id)

    match_record = _elo_live_MatchRecord(
        match_id=map_match_id_value if map_match_id_value > 0 else map_timestamp,
        timestamp=map_timestamp,
        radiant_win=False,
        radiant_team_id=radiant_team_id,
        radiant_team_name=str(radiant_team_name or ""),
        dire_team_id=dire_team_id,
        dire_team_name=str(dire_team_name or ""),
        radiant_player_ids=radiant_player_ids,
        dire_player_ids=dire_player_ids,
        league_id=league_id_value if league_id_value > 0 else None,
        league_name=str(league_name or ""),
        source_league_tier=tier_enum.value,
        series_id=series_id_value if series_id_value > 0 else None,
        series_type=str(series_type) if series_type is not None else None,
        derived_league_tier=tier_enum,
    )
    try:
        result = _elo_live_register_map_context(
            series_key=normalized_series_key,
            series_url=str(series_url or ""),
            map_key=normalized_map_key,
            first_team_score=first_score,
            second_team_score=second_score,
            first_team_is_radiant=bool(first_team_is_radiant),
            match_record=match_record,
        )
    except Exception:
        logger.exception("Failed to register live ELO map context for %s", normalized_map_key)
        return None
    return result if isinstance(result, dict) else None


def _finalize_finished_live_series_for_elo(
    *,
    series_key: Any,
    series_url: str,
    first_team_score: Any,
    second_team_score: Any,
) -> Optional[Dict[str, Any]]:
    if (
        not ELO_LIVE_SNAPSHOT_AVAILABLE
        or _elo_live_finalize_series_from_scores is None
    ):
        return None

    normalized_series_key = str(series_key or "").strip() or str(series_url or "").strip()
    if not normalized_series_key:
        return None

    first_score = _coerce_int(first_team_score)
    second_score = _coerce_int(second_team_score)
    if first_score < 0 or second_score < 0:
        return None

    try:
        result = _elo_live_finalize_series_from_scores(
            series_key=normalized_series_key,
            series_url=str(series_url or ""),
            first_team_score=first_score,
            second_team_score=second_score,
        )
    except Exception:
        logger.exception("Failed to finalize live ELO series context for %s", normalized_series_key)
        return None
    return result if isinstance(result, dict) else None


def _emit_live_elo_applied_log(prefix: str, applied_update: Optional[Dict[str, Any]]) -> None:
    payload = applied_update if isinstance(applied_update, dict) else {}
    if not payload:
        return

    map_key = str(payload.get("map_key") or "unknown")
    winner_slot = str(payload.get("winner_slot") or "unknown")
    winner_team_name = str(payload.get("winner_team_name") or "unknown")
    first_team_name = str(payload.get("first_team_name") or "")
    second_team_name = str(payload.get("second_team_name") or "")
    before_scores = payload.get("series_score_before") if isinstance(payload.get("series_score_before"), dict) else {}
    after_scores = payload.get("series_score_after") if isinstance(payload.get("series_score_after"), dict) else {}
    before_score = f"{_coerce_int(before_scores.get('first'))}:{_coerce_int(before_scores.get('second'))}"
    after_score = f"{_coerce_int(after_scores.get('first'))}:{_coerce_int(after_scores.get('second'))}"
    matchup = (
        f"{first_team_name} vs {second_team_name}"
        if first_team_name and second_team_name
        else f"{str(payload.get('radiant_team_name') or '')} vs {str(payload.get('dire_team_name') or '')}".strip(" vs ")
    )
    print(
        f"   📈 {prefix}: applied_map={map_key}, "
        f"matchup={matchup or 'unknown'}, "
        f"score={before_score}->{after_score}, "
        f"winner_slot={winner_slot}, "
        f"winner={winner_team_name}"
    )
    k_global = float(payload.get("k_global", 0.0) or 0.0)
    k_local = float(payload.get("k_local", 0.0) or 0.0)
    k_roster = float(payload.get("k_roster", 0.0) or 0.0)
    if any(abs(value) > 0.0 for value in (k_global, k_local, k_roster)):
        print(
            "      K(base): "
            f"global={k_global:.1f}, local={k_local:.1f}, roster={k_roster:.1f}"
        )

    def _emit_side(side_label: str, side_payload: Optional[Dict[str, Any]]) -> None:
        side = side_payload if isinstance(side_payload, dict) else {}
        team_name = str(side.get("team_name") or side_label)
        before_rating = float(side.get("before_rating", 0.0) or 0.0)
        after_rating = float(side.get("after_rating", before_rating) or before_rating)
        delta = float(side.get("delta", after_rating - before_rating) or 0.0)
        base_delta = float(side.get("base_delta", delta) or 0.0)
        rating_source = str(side.get("rating_source") or "unknown")
        lineup_matches = int(side.get("before_lineup_matches", 0) or 0)
        lineup_k = float(side.get("lineup_k_multiplier", 1.0) or 1.0)
        player_org_k = float(side.get("player_org_k_multiplier_avg", 1.0) or 1.0)
        local_k = float(side.get("effective_local_k_multiplier_avg", 1.0) or 1.0)
        print(
            f"      {side_label} {team_name}: "
            f"{before_rating:.1f} -> {after_rating:.1f} ({delta:+.1f}) "
            f"[{rating_source}; baseΔ={base_delta:+.1f}; "
            f"lineup_matches={lineup_matches}; lineupK={lineup_k:.2f}; "
            f"playerOrgK≈{player_org_k:.2f}; localK≈{local_k:.2f}]"
        )

    _emit_side("Radiant", payload.get("radiant"))
    _emit_side("Dire", payload.get("dire"))
    rating_delta_sum = float(payload.get("rating_delta_sum", 0.0) or 0.0)
    base_delta_sum = float(payload.get("base_delta_sum", rating_delta_sum) or rating_delta_sum)
    print(f"      ΣΔ rating/base: {rating_delta_sum:+.1f} / {base_delta_sum:+.1f}")

    rad_before = float(payload.get("radiant_win_prob_before", 0.5) or 0.5) * 100.0
    rad_after = float(payload.get("radiant_win_prob_after", 0.5) or 0.5) * 100.0
    prob_delta = float(payload.get("radiant_win_prob_delta", (rad_after - rad_before) / 100.0) or 0.0) * 100.0
    elo_before = float(payload.get("elo_diff_before", 0.0) or 0.0)
    elo_after = float(payload.get("elo_diff_after", 0.0) or 0.0)
    print(
        "      WR(rad): "
        f"{rad_before:.1f}% -> {rad_after:.1f}% ({prob_delta:+.1f} pp), "
        f"ΔELO(rad-dire): {elo_before:.1f} -> {elo_after:.1f}"
    )


def _winner_slot_from_series_scores(
    previous_scores: Optional[Dict[str, Any]],
    current_scores: Optional[Dict[str, Any]],
) -> Optional[str]:
    prev_first = _coerce_int((previous_scores or {}).get("first"))
    prev_second = _coerce_int((previous_scores or {}).get("second"))
    cur_first = _coerce_int((current_scores or {}).get("first"))
    cur_second = _coerce_int((current_scores or {}).get("second"))
    if min(prev_first, prev_second, cur_first, cur_second) < 0:
        return None
    if cur_first == prev_first + 1 and cur_second == prev_second:
        return "first"
    if cur_second == prev_second + 1 and cur_first == prev_first:
        return "second"
    return None


def _fetch_finished_series_scores_from_page(series_url: str) -> Optional[Tuple[int, int]]:
    normalized = str(series_url or "").strip()
    if not normalized:
        return None

    from urllib.parse import urlparse

    parsed = urlparse(normalized if "://" in normalized else f"https://{normalized}")
    path = str(parsed.path or "").strip()
    if not path:
        return None
    if not re.search(r"/matches/\d+(?:/|$)", path):
        return None

    response = make_request_with_retry(f"https://46.229.214.49{path}", max_retries=3, retry_delay=2)
    if not response or response.status_code != 200:
        return None

    soup = BeautifulSoup(response.text or "", "lxml")
    title_candidates: List[str] = []
    if soup.title and soup.title.text:
        title_candidates.append(str(soup.title.text))
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        title_candidates.append(str(og_title.get("content")))
    for candidate in title_candidates:
        if "final score" not in candidate.lower():
            continue
        match = re.search(r"\b(\d+)\s*-\s*(\d+)\b", candidate)
        if match:
            return int(match.group(1)), int(match.group(2))
    return None


def _extract_live_match_id(payload: Optional[dict]) -> Optional[int]:
    data = payload if isinstance(payload, dict) else {}
    for candidate in (
        data.get("match_id"),
        data.get("id"),
    ):
        value = _coerce_int(candidate)
        if value > 0:
            return value

    live_league = data.get("live_league_data")
    if isinstance(live_league, dict):
        for source in (
            live_league,
            live_league.get("match") if isinstance(live_league.get("match"), dict) else {},
        ):
            for candidate in (
                source.get("match_id"),
                source.get("id"),
            ):
                value = _coerce_int(candidate)
                if value > 0:
                    return value
    return None


def _find_stale_live_map_payload(
    *,
    series_key: Any,
    map_key: str,
    live_match_id: Any,
) -> Optional[Dict[str, Any]]:
    normalized_series_key = str(series_key or "").strip()
    normalized_map_key = str(map_key or "").strip()
    current_match_id = _coerce_int(live_match_id)
    if not normalized_series_key or not normalized_map_key or current_match_id <= 0:
        return None

    progress_path = Path(_elo_live_default_progress_path)
    if not progress_path.exists():
        return None

    try:
        payload = json.loads(progress_path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to inspect live ELO progress for stale map payload detection")
        return None

    applied_maps = payload.get("applied_maps")
    if not isinstance(applied_maps, dict):
        return None

    for applied_map_key, raw_state in applied_maps.items():
        if not isinstance(raw_state, dict):
            continue
        if str(raw_state.get("series_key") or "").strip() != normalized_series_key:
            continue
        if str(applied_map_key or "").strip() == normalized_map_key:
            continue
        applied_match_id = _coerce_int(raw_state.get("match_id"))
        if applied_match_id <= 0 or applied_match_id != current_match_id:
            continue
        return {
            "series_key": normalized_series_key,
            "current_map_key": normalized_map_key,
            "duplicate_of_map_key": str(applied_map_key or "").strip(),
            "match_id": int(current_match_id),
        }
    return None


def _finalize_orphaned_live_elo_series(seen_series_keys: set[str]) -> List[Dict[str, Any]]:
    if not ELO_LIVE_SNAPSHOT_AVAILABLE or _elo_live_finalize_series_from_scores is None:
        return []

    progress_path = Path(_elo_live_default_progress_path)
    if not progress_path.exists():
        return []

    try:
        payload = json.loads(progress_path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to read live ELO progress snapshot for orphan sweep")
        return []

    pending_series = payload.get("pending_series")
    if not isinstance(pending_series, dict) or not pending_series:
        return []

    seen = {str(item).strip() for item in seen_series_keys if str(item).strip()}
    now_ts = int(time.time())
    finalized: List[Dict[str, Any]] = []

    for raw_series_key, raw_state in list(pending_series.items()):
        if not isinstance(raw_state, dict):
            continue
        series_key = str(raw_state.get("series_key") or raw_series_key or "").strip()
        series_url = str(raw_state.get("series_url") or "").strip()
        if not series_key:
            continue
        if series_key in seen or (series_url and series_url in seen):
            continue

        pending_map = raw_state.get("pending_map") if isinstance(raw_state.get("pending_map"), dict) else {}
        registered_at = _coerce_int(pending_map.get("registered_at"))
        updated_at = _coerce_int(raw_state.get("updated_at"))
        age_seconds = now_ts - max(registered_at, updated_at, 0)
        if age_seconds < LIVE_ELO_ORPHAN_PENDING_MIN_AGE_SECONDS:
            continue

        finished_scores = _fetch_finished_series_scores_from_page(series_url)
        if finished_scores is None:
            continue

        current_scores = {"first": int(finished_scores[0]), "second": int(finished_scores[1])}
        previous_scores = raw_state.get("last_scores") if isinstance(raw_state.get("last_scores"), dict) else {}
        winner_slot = _winner_slot_from_series_scores(previous_scores, current_scores)
        if winner_slot is None:
            continue

        result = _finalize_finished_live_series_for_elo(
            series_key=series_key,
            series_url=series_url,
            first_team_score=current_scores["first"],
            second_team_score=current_scores["second"],
        )
        if not isinstance(result, dict):
            continue
        applied_update = result.get("applied_update") if isinstance(result.get("applied_update"), dict) else None
        applied_map_key = str((applied_update or {}).get("map_key") or "")
        if applied_map_key:
            _drop_delayed_match(applied_map_key, reason="orphan_series_finished_live_elo_applied")
        finalized.append(
            {
                "series_key": series_key,
                "series_url": series_url,
                "current_scores": current_scores,
                "winner_slot": winner_slot,
                "applied_update": applied_update,
                "age_seconds": age_seconds,
            }
        )

    return finalized


def _build_team_elo_matchup_summary_from_kills_priors(
    radiant_team_id: Optional[int],
    dire_team_id: Optional[int],
    radiant_team_name: Optional[str] = None,
    dire_team_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    priors = _load_kills_priors_from_cache_only()
    if not isinstance(priors, dict):
        return None

    team_elo = priors.get("team_elo", {})
    team_elo_games = priors.get("team_elo_games", {})
    if not isinstance(team_elo, dict) or not isinstance(team_elo_games, dict):
        return None

    def _resolve_snapshot(team_id: Optional[int], team_name: Optional[str]) -> Dict[str, Any]:
        candidate_ids: List[int] = []
        seen: set[int] = set()
        _collect_candidate_team_ids(team_id, candidate_ids, seen)
        try:
            for known_team_id in sorted(_find_known_team_ids_by_name(str(team_name or ""))):
                _collect_candidate_team_ids(known_team_id, candidate_ids, seen)
        except Exception:
            pass

        matched_ids: List[int] = []
        total_games = 0
        weighted_rating_sum = 0.0
        total_weight = 0.0
        for candidate_id in candidate_ids:
            key = str(int(candidate_id))
            if key not in team_elo and key not in team_elo_games:
                continue
            rating = float(team_elo.get(key, 1500.0))
            games = int(team_elo_games.get(key, 0) or 0)
            weight = float(max(games, 1))
            matched_ids.append(int(candidate_id))
            total_games += games
            weighted_rating_sum += rating * weight
            total_weight += weight

        if total_weight <= 0.0:
            return {
                "rating": 1500.0,
                "games": 0,
                "candidate_ids": candidate_ids,
                "matched_ids": [],
            }

        return {
            "rating": weighted_rating_sum / total_weight,
            "games": total_games,
            "candidate_ids": candidate_ids,
            "matched_ids": matched_ids,
        }

    radiant_snapshot = _resolve_snapshot(radiant_team_id, radiant_team_name)
    dire_snapshot = _resolve_snapshot(dire_team_id, dire_team_name)
    if int(radiant_snapshot.get("games", 0) or 0) <= 0 and int(dire_snapshot.get("games", 0) or 0) <= 0:
        return None

    radiant_rating = float(radiant_snapshot.get("rating", 1500.0))
    dire_rating = float(dire_snapshot.get("rating", 1500.0))
    radiant_win_prob = 1.0 / (1.0 + 10 ** ((dire_rating - radiant_rating) / 400.0))

    return {
        "radiant": radiant_snapshot,
        "dire": dire_snapshot,
        "radiant_win_prob": radiant_win_prob,
        "dire_win_prob": 1.0 - radiant_win_prob,
        "elo_diff": radiant_rating - dire_rating,
    }


def _build_team_elo_matchup_summary(
    radiant_team_id: Optional[int],
    dire_team_id: Optional[int],
    radiant_team_name: Optional[str] = None,
    dire_team_name: Optional[str] = None,
    radiant_account_ids: Optional[List[int]] = None,
    dire_account_ids: Optional[List[int]] = None,
    match_tier: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    summary = _build_team_elo_matchup_summary_from_live_snapshot(
        radiant_team_id=radiant_team_id,
        dire_team_id=dire_team_id,
        radiant_team_name=radiant_team_name,
        dire_team_name=dire_team_name,
        radiant_account_ids=radiant_account_ids,
        dire_account_ids=dire_account_ids,
        match_tier=match_tier,
    )
    if isinstance(summary, dict):
        return summary
    return _build_team_elo_matchup_summary_from_kills_priors(
        radiant_team_id=radiant_team_id,
        dire_team_id=dire_team_id,
        radiant_team_name=radiant_team_name,
        dire_team_name=dire_team_name,
    )


def _elo_probability_from_ratings(radiant_rating: float, dire_rating: float) -> float:
    return 1.0 / (1.0 + 10 ** ((dire_rating - radiant_rating) / 400.0))


def _format_team_elo_block(
    summary: Optional[Dict[str, Any]],
    *,
    radiant_team_name: str,
    dire_team_name: str,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    if not isinstance(summary, dict):
        return "", None

    radiant_payload = summary.get("radiant") or {}
    dire_payload = summary.get("dire") or {}
    radiant_rating = float(radiant_payload.get("rating", 1500.0))
    dire_rating = float(dire_payload.get("rating", 1500.0))
    radiant_base_rating = float(radiant_payload.get("base_rating", radiant_rating))
    dire_base_rating = float(dire_payload.get("base_rating", dire_rating))
    radiant_snapshot_base_rating = float(radiant_payload.get("snapshot_base_rating", radiant_base_rating))
    dire_snapshot_base_rating = float(dire_payload.get("snapshot_base_rating", dire_base_rating))
    radiant_live_base_delta = float(radiant_payload.get("live_base_delta", radiant_base_rating - radiant_snapshot_base_rating))
    dire_live_base_delta = float(dire_payload.get("live_base_delta", dire_base_rating - dire_snapshot_base_rating))
    try:
        radiant_leaderboard_rank = int(radiant_payload.get("leaderboard_rank"))
    except (TypeError, ValueError):
        radiant_leaderboard_rank = None
    try:
        dire_leaderboard_rank = int(dire_payload.get("leaderboard_rank"))
    except (TypeError, ValueError):
        dire_leaderboard_rank = None
    adjusted_radiant_wr = float(summary.get("radiant_win_prob", _elo_probability_from_ratings(radiant_rating, dire_rating))) * 100.0
    adjusted_dire_wr = float(summary.get("dire_win_prob", 1.0 - (adjusted_radiant_wr / 100.0))) * 100.0
    adjusted_diff = float(summary.get("elo_diff", radiant_rating - dire_rating))
    raw_diff = radiant_base_rating - dire_base_rating
    raw_radiant_wr = _elo_probability_from_ratings(radiant_base_rating, dire_base_rating) * 100.0
    raw_dire_wr = 100.0 - raw_radiant_wr
    tier_gap_bonus = float(summary.get("tier_gap_bonus", 0.0) or 0.0)
    tier_gap_key = str(summary.get("tier_gap_key") or "").strip()
    lineup_used = bool(radiant_payload.get("lineup_used")) or bool(dire_payload.get("lineup_used"))

    lines = [
        "Командный ELO (текущий состав):" if lineup_used else "Командный ELO:",
        f"{radiant_team_name}: {radiant_base_rating:.0f}",
        f"{dire_team_name}: {dire_base_rating:.0f}",
    ]
    if abs(radiant_live_base_delta) >= 0.5 or abs(dire_live_base_delta) >= 0.5:
        lines.append(f"Δ live vs snapshot: {radiant_live_base_delta:+.0f} / {dire_live_base_delta:+.0f}")
    if abs(radiant_base_rating - radiant_rating) < 0.5 and abs(dire_base_rating - dire_rating) < 0.5:
        lines.append(f"ELO WR≈{adjusted_radiant_wr:.1f}% / {adjusted_dire_wr:.1f}% (ΔELO {adjusted_diff:+.0f})")
    else:
        lines.append(f"Raw WR≈{raw_radiant_wr:.1f}% / {raw_dire_wr:.1f}% (ΔELO {raw_diff:+.0f})")
        adj_suffix = ""
        if abs(tier_gap_bonus) >= 0.5:
            adj_suffix = f", tier bonus {tier_gap_bonus:+.0f}"
            if tier_gap_key:
                adj_suffix += f" {tier_gap_key}"
        lines.append(f"Adj WR≈{adjusted_radiant_wr:.1f}% / {adjusted_dire_wr:.1f}% (ΔELO {adjusted_diff:+.0f}{adj_suffix})")

    return "\n".join(lines) + "\n", {
        "radiant_rating": radiant_rating,
        "dire_rating": dire_rating,
        "radiant_base_rating": radiant_base_rating,
        "dire_base_rating": dire_base_rating,
        "radiant_snapshot_base_rating": radiant_snapshot_base_rating,
        "dire_snapshot_base_rating": dire_snapshot_base_rating,
        "radiant_live_base_delta": radiant_live_base_delta,
        "dire_live_base_delta": dire_live_base_delta,
        "radiant_leaderboard_rank": radiant_leaderboard_rank,
        "dire_leaderboard_rank": dire_leaderboard_rank,
        "adjusted_radiant_wr": adjusted_radiant_wr,
        "adjusted_dire_wr": adjusted_dire_wr,
        "raw_radiant_wr": raw_radiant_wr,
        "raw_dire_wr": raw_dire_wr,
        "adjusted_diff": adjusted_diff,
        "raw_diff": raw_diff,
        "tier_gap_bonus": tier_gap_bonus,
        "tier_gap_key": tier_gap_key,
        "lineup_used": lineup_used,
        "source": str(summary.get("source") or ""),
    }


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
    normalized = {
        str(url).strip()
        for url in (urls or [])
        if str(url).strip()
    }
    with processed_urls_lock:
        processed_urls_cache.clear()
        processed_urls_cache.update(normalized)


def _sync_uncertain_delivery_urls_cache(urls: Any) -> None:
    normalized = {
        str(url).strip()
        for url in (urls or [])
        if str(url).strip()
    }
    with uncertain_delivery_urls_lock:
        uncertain_delivery_urls_cache.clear()
        uncertain_delivery_urls_cache.update(normalized)


def _should_emit_verbose_match_log(match_key: Any) -> bool:
    key = str(match_key or "").strip()
    if not key:
        return True
    with verbose_match_log_lock:
        return key not in verbose_match_log_cache


def _mark_verbose_match_log_done(match_key: Any) -> None:
    key = str(match_key or "").strip()
    if not key:
        return
    with verbose_match_log_lock:
        verbose_match_log_cache.add(key)


def _backup_corrupted_state_file(path: Path, suffix: str) -> Optional[Path]:
    if not path.exists():
        return None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = path.with_name(f"{path.name}.{suffix}.{timestamp}")
    try:
        path.replace(backup_path)
    except Exception as exc:
        logger.exception("Failed to backup corrupted state file %s: %s", path, exc)
        return None
    return backup_path


def _load_json_url_array(path: Path, *, recover: bool, label: str) -> list[str]:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        _write_map_id_check_atomic(path, [])
        return []
    if not raw:
        _write_map_id_check_atomic(path, [])
        return []
    try:
        data = orjson.loads(raw)
    except Exception as exc:
        if not recover:
            raise
        backup_path = _backup_corrupted_state_file(path, "corrupt")
        _write_map_id_check_atomic(path, [])
        logger.exception("Recovered corrupted %s at %s", label, path)
        print(
            f"⚠️ {label} поврежден и сброшен в []: {path}"
            + (f" (backup={backup_path})" if backup_path is not None else "")
        )
        return []
    if not isinstance(data, list):
        if not recover:
            raise ValueError(f"{label} должен содержать JSON-массив")
        backup_path = _backup_corrupted_state_file(path, "invalid")
        _write_map_id_check_atomic(path, [])
        logger.error("Recovered invalid %s structure at %s: expected JSON array", label, path)
        print(
            f"⚠️ {label} имел неверную структуру и сброшен в []: {path}"
            + (f" (backup={backup_path})" if backup_path is not None else "")
        )
        return []
    return [str(item) for item in data if isinstance(item, str) and item]


def _load_json_object(
    path: Path,
    *,
    recover: bool,
    label: str,
    empty_value: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return dict(empty_value or {})
    if not raw:
        return dict(empty_value or {})
    try:
        data = orjson.loads(raw)
    except Exception:
        if not recover:
            raise
        backup_path = _backup_corrupted_state_file(path, "corrupt")
        _write_json_atomic(path, dict(empty_value or {}))
        logger.exception("Recovered corrupted %s at %s", label, path)
        print(
            f"⚠️ {label} поврежден и сброшен: {path}"
            + (f" (backup={backup_path})" if backup_path is not None else "")
        )
        return dict(empty_value or {})
    if not isinstance(data, dict):
        if not recover:
            raise ValueError(f"{label} должен содержать JSON-объект")
        backup_path = _backup_corrupted_state_file(path, "invalid")
        _write_json_atomic(path, dict(empty_value or {}))
        logger.error("Recovered invalid %s structure at %s: expected JSON object", label, path)
        print(
            f"⚠️ {label} имел неверную структуру и сброшен: {path}"
            + (f" (backup={backup_path})" if backup_path is not None else "")
        )
        return dict(empty_value or {})
    return {str(k): v for k, v in data.items() if isinstance(k, str) and k}


def _load_map_id_check_urls(*, recover: bool) -> list[str]:
    with map_id_check_lock:
        return _load_json_url_array(
            Path(MAP_ID_CHECK_PATH),
            recover=recover,
            label="MAP_ID_CHECK_PATH",
        )


def _load_delayed_queue_state(*, recover: bool) -> dict[str, dict[str, Any]]:
    with delayed_queue_lock:
        raw = _load_json_object(
            Path(DELAYED_QUEUE_PATH),
            recover=recover,
            label="DELAYED_QUEUE_PATH",
            empty_value={},
        )
    normalized: dict[str, dict[str, Any]] = {}
    for match_key, payload in raw.items():
        if isinstance(payload, dict):
            normalized[str(match_key)] = dict(payload)
    return normalized


def _persist_delayed_queue_snapshot(snapshot: dict[str, dict[str, Any]]) -> None:
    with delayed_queue_lock:
        _write_json_atomic(Path(DELAYED_QUEUE_PATH), snapshot)


def _replace_monitored_matches_from_snapshot(snapshot: dict[str, dict[str, Any]]) -> None:
    with monitored_matches_lock:
        monitored_matches.clear()
        monitored_matches.update(copy.deepcopy(snapshot))


def _set_delayed_match(match_key: str, payload: dict[str, Any]) -> None:
    if not match_key:
        return
    payload_copy = copy.deepcopy(payload)
    with monitored_matches_lock:
        previous = monitored_matches.get(match_key)
        previous_copy = copy.deepcopy(previous) if isinstance(previous, dict) else None
        monitored_matches[match_key] = payload_copy
        snapshot = copy.deepcopy(monitored_matches)
    try:
        _persist_delayed_queue_snapshot(snapshot)
    except Exception:
        with monitored_matches_lock:
            if previous_copy is None:
                monitored_matches.pop(match_key, None)
            else:
                monitored_matches[match_key] = previous_copy
        raise


def _update_delayed_match(match_key: str, **updates: Any) -> bool:
    if not match_key:
        return False
    with monitored_matches_lock:
        current = monitored_matches.get(match_key)
        if not isinstance(current, dict):
            return False
        updated = dict(current)
        updated.update(updates)
        monitored_matches[match_key] = updated
        snapshot = copy.deepcopy(monitored_matches)
    try:
        _persist_delayed_queue_snapshot(snapshot)
    except Exception as exc:
        logger.exception("Failed to persist delayed queue update for %s: %s", match_key, exc)
        return False
    return True


def _load_uncertain_delivery_urls() -> list[str]:
    paths = [
        Path(UNCERTAIN_SIGNAL_DELIVERY_PATH),
        Path(UNCERTAIN_SIGNAL_DELIVERY_FALLBACK_PATH),
    ]
    urls: list[str] = []
    seen: set[str] = set()
    for path in paths:
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            continue
        except Exception as exc:
            logger.exception("Failed to read uncertain-delivery journal %s: %s", path, exc)
            continue
        for raw_line in raw.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = orjson.loads(line)
            except Exception as exc:
                logger.warning("Failed to parse uncertain-delivery journal line from %s: %s", path, exc)
                continue
            url = str((payload or {}).get("url") or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            urls.append(url)
    return urls


def _append_uncertain_delivery_entry(entry: dict[str, Any]) -> str:
    primary_path = Path(UNCERTAIN_SIGNAL_DELIVERY_PATH)
    fallback_path = Path(UNCERTAIN_SIGNAL_DELIVERY_FALLBACK_PATH)
    try:
        _append_journal_entry_to_path(primary_path, entry)
        return str(primary_path)
    except Exception as exc:
        logger.exception("Failed to append uncertain-delivery journal %s: %s", primary_path, exc)
        _append_journal_entry_to_path(fallback_path, entry)
        return str(fallback_path)


def _append_sent_signal_journal(url: str, reason: str, details: Any) -> str:
    entry = {
        "timestamp": datetime.now().isoformat(),
        "url": str(url or "").strip(),
        "reason": str(reason or "unspecified"),
        "details": details,
    }
    primary_path = Path(SENT_SIGNAL_JOURNAL_PATH)
    fallback_path = Path(SENT_SIGNAL_JOURNAL_FALLBACK_PATH)
    try:
        _append_journal_entry_to_path(primary_path, entry)
        return str(primary_path)
    except Exception as exc:
        logger.exception("Failed to append sent-signal journal %s: %s", primary_path, exc)
        _append_journal_entry_to_path(fallback_path, entry)
        return str(fallback_path)


def _flush_sent_signal_journal_into_map_id_check() -> int:
    journal_paths = [
        Path(SENT_SIGNAL_JOURNAL_PATH),
        Path(SENT_SIGNAL_JOURNAL_FALLBACK_PATH),
    ]
    recovered_urls: list[str] = []
    seen: set[str] = set()
    for journal_path in journal_paths:
        try:
            raw = journal_path.read_bytes()
        except FileNotFoundError:
            continue
        if not raw:
            continue
        for raw_line in raw.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            payload = orjson.loads(line)
            url = str((payload or {}).get("url") or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            recovered_urls.append(url)
    if not recovered_urls:
        return 0

    map_id_check_path = Path(MAP_ID_CHECK_PATH)
    with map_id_check_lock:
        data = _load_json_url_array(
            map_id_check_path,
            recover=True,
            label="MAP_ID_CHECK_PATH",
        )
        appended = 0
        for url in recovered_urls:
            if url in data:
                continue
            data.append(url)
            appended += 1
        _write_map_id_check_atomic(map_id_check_path, data)
    _sync_processed_urls_cache(data)
    for journal_path in journal_paths:
        try:
            _clear_journal_file(journal_path)
        except Exception as exc:
            logger.exception("Failed to clear sent-signal journal %s: %s", journal_path, exc)
    return appended


def _safe_flush_sent_signal_journal_into_map_id_check() -> int:
    try:
        return _flush_sent_signal_journal_into_map_id_check()
    except Exception as exc:
        logger.exception("Failed to flush sent-signal journal into map_id_check: %s", exc)
        return 0


def _mark_url_uncertain_delivery(url: str) -> None:
    if not url:
        return
    with uncertain_delivery_urls_lock:
        uncertain_delivery_urls_cache.add(url)


def _is_url_uncertain_delivery(url: str) -> bool:
    if not url:
        return False
    with uncertain_delivery_urls_lock:
        return url in uncertain_delivery_urls_cache


def _record_uncertain_delivery(
    match_key: str,
    *,
    reason: str,
    details: Optional[dict[str, Any]],
    error_message: str,
) -> Optional[str]:
    normalized_url = str(match_key or "").strip()
    if not normalized_url:
        return None
    payload = {
        "timestamp": datetime.now().isoformat(),
        "url": normalized_url,
        "reason": str(reason or "uncertain_delivery"),
        "details": dict(details or {}),
        "error_message": str(error_message or ""),
    }
    journal_path = _append_uncertain_delivery_entry(payload)
    _mark_url_uncertain_delivery(normalized_url)
    _drop_delayed_match(normalized_url, reason="uncertain_delivery")
    _release_signal_send_slot(normalized_url)
    return journal_path


def _mark_url_processed(url: str) -> None:
    normalized_url = str(url or "").strip()
    if not normalized_url:
        return
    with processed_urls_lock:
        processed_urls_cache.add(normalized_url)


def _is_url_processed(url: str) -> bool:
    if not url:
        return False
    normalized_url = str(url).strip()
    with processed_urls_lock:
        if normalized_url in processed_urls_cache:
            return True
    try:
        data = _load_map_id_check_urls(recover=False)
        exists = bool(isinstance(data, list) and normalized_url in data)
        if exists:
            _mark_url_processed(normalized_url)
        return exists
    except Exception as exc:
        logger.warning("Failed processed-url lookup for %s: %s", url, exc)
        return False


def _drop_delayed_match(match_key: str, reason: str = "") -> bool:
    if not match_key:
        return False
    with monitored_matches_lock:
        removed = monitored_matches.pop(match_key, None)
        snapshot = copy.deepcopy(monitored_matches)
    if removed is not None:
        try:
            _persist_delayed_queue_snapshot(snapshot)
        except Exception as exc:
            logger.exception("Failed to persist delayed queue removal for %s: %s", match_key, exc)
            print(f"   ⚠️ Не удалось сохранить удаление delayed-очереди для {match_key}: {exc}")
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


def _schedule_delayed_retry(match_key: str, exc: Exception, now_ts: Optional[float] = None) -> bool:
    if not match_key:
        return False
    if now_ts is None:
        now_ts = time.time()
    with monitored_matches_lock:
        payload = monitored_matches.get(match_key)
        if not isinstance(payload, dict):
            return False
        attempt_count = int(payload.get("retry_attempt_count", 0) or 0) + 1
    base_delay = max(5, int(DELAYED_SIGNAL_RETRY_BACKOFF_BASE_SECONDS or 60))
    max_delay = max(base_delay, int(DELAYED_SIGNAL_RETRY_BACKOFF_MAX_SECONDS or base_delay))
    retry_delay = min(max_delay, base_delay * (2 ** max(0, attempt_count - 1)))
    next_retry_at = float(now_ts + retry_delay)
    updated = _update_delayed_match(
        match_key,
        retry_attempt_count=attempt_count,
        last_send_error=str(exc),
        last_send_error_at=float(now_ts),
        next_retry_at=next_retry_at,
    )
    if updated:
        retry_human = datetime.fromtimestamp(next_retry_at).strftime('%Y-%m-%d %H:%M:%S')
        print(
            f"⚠️ Delayed retry scheduled: {match_key} "
            f"(attempt={attempt_count}, next_retry_at={retry_human}, error={exc})"
        )
    return updated


def _dispatch_block_reason(match_key: str) -> Optional[str]:
    if not match_key:
        return None
    if _is_url_uncertain_delivery(match_key):
        return "uncertain_delivery"
    if _is_url_processed(match_key):
        return "processed"
    return None


def _skip_dispatch_for_processed_url(match_key: str, context: str, indent: str = "   ") -> bool:
    block_reason = _dispatch_block_reason(match_key)
    if block_reason is None:
        return False
    if block_reason == "processed":
        print(f"{indent}⚠️ Пропуск {context}: URL уже обработан: {match_key}")
    else:
        print(
            f"{indent}⚠️ Пропуск {context}: URL заблокирован после uncertain delivery: {match_key}"
        )
    _drop_delayed_match(match_key, reason=f"{context}_{block_reason}")
    return True


def _write_map_id_check_atomic(path: Path, data: list[Any]) -> None:
    _write_json_atomic(path, data)


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
        map_id_check_path = Path(MAP_ID_CHECK_PATH)
        with map_id_check_lock:
            data = _load_json_url_array(
                map_id_check_path,
                recover=True,
                label="MAP_ID_CHECK_PATH",
            )
            already_present = url in data
            if not already_present:
                data.append(url)
            _write_map_id_check_atomic(map_id_check_path, data)
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


def _deliver_and_persist_signal(
    match_key: str,
    message_text: str,
    *,
    add_url_reason: str,
    add_url_details: Optional[dict] = None,
    bookmaker_decision: Optional[str] = None,
) -> bool:
    try:
        send_message(message_text, require_delivery=True)
    except TelegramSendError as exc:
        if exc.delivery_uncertain:
            _record_uncertain_delivery(
                match_key,
                reason="telegram_delivery_uncertain",
                details=dict(add_url_details or {}),
                error_message=str(exc),
            )
            print(
                f"   ⚠️ Uncertain Telegram delivery for {match_key}; "
                "URL не будет заблокирован вне map_id_check.txt"
            )
            return False
        raise
    if bookmaker_decision:
        _log_bookmaker_source_snapshot(match_key, decision=bookmaker_decision)
    try:
        add_url(
            match_key,
            reason=add_url_reason,
            details=dict(add_url_details or {}),
        )
    except Exception as exc:
        journal_details = dict(add_url_details or {})
        journal_details["persist_error"] = str(exc)
        _append_sent_signal_journal(match_key, add_url_reason, journal_details)
        _mark_url_processed(match_key)
        _drop_delayed_match(match_key, reason="sent_signal_journaled_after_persist_error")
        _release_signal_send_slot(match_key)
        logger.exception("Signal was sent but add_url() failed for %s", match_key)
        print(
            f"   ⚠️ add_url() failed after successful send for {match_key}; "
            "единственный storage=map_id_check.txt, URL не помечен обработанным"
        )
        return False
    return True


def get_heads(response=None, MAX_RETRIES=5, RETRY_DELAY=5, ip_address="46.229.214.49", path = "/matches"):
        global GET_HEADS_LAST_FAILURE_REASON, NEXT_SCHEDULE_SLEEP_SECONDS, NEXT_SCHEDULE_MATCH_INFO, SCHEDULE_LIVE_WAIT_TARGET
        GET_HEADS_LAST_FAILURE_REASON = None
        NEXT_SCHEDULE_SLEEP_SECONDS = 0.0
        NEXT_SCHEDULE_MATCH_INFO = None
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
            GET_HEADS_LAST_FAILURE_REASON = GET_HEADS_FAILURE_REASON_REQUEST_FAILED
            _emit_pending_schedule_wake_audit(
                heads_count=0,
                bodies_count=0,
                next_schedule_info=None,
                request_status=status_msg,
            )
            return None, None

        try:
            attempted_markers: set[str] = set()
            parse_failed_on_200 = False
            max_proxy_attempts = max(1, len(PROXY_LIST)) if USE_PROXY and PROXY_LIST else 1
            current_response = response
            live_matches = None

            while True:
                marker = _get_current_proxy_marker()
                attempted_markers.add(marker)

                if current_response and current_response.status_code == 200:
                    soup = BeautifulSoup(current_response.text, 'lxml')
                    live_matches = soup.find('div', class_='live__matches')
                    if live_matches:
                        break
                    parse_failed_on_200 = True
                    print(
                        "❌ Не найден элемент live__matches в HTML "
                        f"(proxy={marker}, tried={len(attempted_markers)}/{max_proxy_attempts})"
                    )
                else:
                    status_msg = (
                        f"status={current_response.status_code}"
                        if current_response is not None
                        else "response=None"
                    )
                    print(
                        "⚠️ Не удалось получить валидный HTML для парсинга "
                        f"(proxy={marker}, {status_msg})"
                    )

                if len(attempted_markers) >= max_proxy_attempts:
                    if parse_failed_on_200:
                        print("❌ Элемент live__matches не найден после всех доступных прокси")
                        GET_HEADS_LAST_FAILURE_REASON = (
                            GET_HEADS_FAILURE_REASON_LIVE_MATCHES_MISSING_ALL_PROXIES
                        )
                    else:
                        GET_HEADS_LAST_FAILURE_REASON = GET_HEADS_FAILURE_REASON_REQUEST_FAILED
                    _emit_pending_schedule_wake_audit(
                        heads_count=0,
                        bodies_count=0,
                        next_schedule_info=None,
                        request_status=GET_HEADS_LAST_FAILURE_REASON,
                    )
                    return None, None

                if not _rotate_to_untried_proxy(attempted_markers):
                    if parse_failed_on_200:
                        print("❌ Элемент live__matches не найден после всех доступных прокси")
                        GET_HEADS_LAST_FAILURE_REASON = (
                            GET_HEADS_FAILURE_REASON_LIVE_MATCHES_MISSING_ALL_PROXIES
                        )
                    else:
                        GET_HEADS_LAST_FAILURE_REASON = GET_HEADS_FAILURE_REASON_REQUEST_FAILED
                    _emit_pending_schedule_wake_audit(
                        heads_count=0,
                        bodies_count=0,
                        next_schedule_info=None,
                        request_status=GET_HEADS_LAST_FAILURE_REASON,
                    )
                    return None, None
                print(f"🔄 Переключился на другой прокси, повторяю запрос...")
                time.sleep(2)
                current_response = make_request_with_retry(url, max_retries=3, retry_delay=2, headers=headers)
                if not current_response or current_response.status_code != 200:
                    continue
            
            heads = live_matches.find_all('div', class_='live__matches-item__head')
            bodies = live_matches.find_all('div', class_='live__matches-item__body')
            
            if not heads or not bodies:
                print(f"⚠️  Не найдены матчи (heads: {len(heads)}, bodies: {len(bodies)})")
                schedule_info = _extract_nearest_scheduled_match_info(soup)
                if schedule_info:
                    NEXT_SCHEDULE_MATCH_INFO = schedule_info
                    NEXT_SCHEDULE_SLEEP_SECONDS = float(schedule_info.get("sleep_seconds", 0.0) or 0.0)
                _emit_pending_schedule_wake_audit(
                    heads_count=len(heads),
                    bodies_count=len(bodies),
                    next_schedule_info=schedule_info,
                )
                return [], []
            SCHEDULE_LIVE_WAIT_TARGET = None
            _emit_pending_schedule_wake_audit(
                heads_count=len(heads),
                bodies_count=len(bodies),
            )
            
            heads_copy, bodies_copy = heads.copy(), bodies.copy()
            for i in range(len(heads)):
                title = heads[i].find('div', class_='event__name').find('div').text
                # if not any(i in title.lower() for i in ['dreamleague', 'blast', 'dacha', 'betboom',
                #                                         'fissure', 'pgl', 'esports', 'international',
                #                                         'european', 'epl', 'esl', 'cct']):
                # if any(i in title.lower() for i in ['lunar']):
                    # heads_copy.remove(heads[i])
                    # bodies_copy.remove(bodies[i])
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

    if total_heroes < 10:
        if leftover:
            print(f"      ⚠️  Остался неподтвержденный leftover hero_id={leftover}")
        for placeholder_name in list(radiant_names_pos.keys()):
            if placeholder_name.startswith("__missing_"):
                print(f"         ❌ Radiant: осталась необработанная заглушка {placeholder_name}")
        for placeholder_name in list(dire_names_pos.keys()):
            if placeholder_name.startswith("__missing_"):
                print(f"         ❌ Dire: осталась необработанная заглушка {placeholder_name}")
        print("      ❌ parse_draft_and_positions(): требуется полный драфт 10/10")
        return None, None, f"Недостаточно героев определено: {total_heroes}/10", "", []

    # Финальная проверка
    final_rad = len(radiant_heroes_and_pos)
    final_dire = len(dire_heroes_and_pos)
    final_total = final_rad + final_dire

    print(f"      📊 ФИНАЛЬНЫЙ СОСТАВ: radiant={final_rad}/5, dire={final_dire}/5, total={final_total}/10")

    if final_total == 10:
        print("      ✅ parse_draft_and_positions(): завершено успешно (все 10 героев)")
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
        
        if return_status != 'draft...':
            return_status = status



        # Извлечение данных
        try:
            score_divs = bodies[i].find_all('div', class_='match__item-team__score')
            uniq_score = sum(int(div.text.strip()) for div in score_divs[:2])
            score = f"{score_divs[:2][0].text.strip()} : {score_divs[:2][1].text.strip()}"
            link_tag = bodies[i].find('a')
            href = link_tag['href']
            parsed_url = urlparse(href)
            path = parsed_url.path
            series_match = re.search(r"/matches/(\d+)", path)
            series_key_from_path = series_match.group(1) if series_match else ""
            series_url = f'dltv.org{path}'
            check_uniq_url = f'dltv.org{path}.{uniq_score}'
            verbose_match_log = _should_emit_verbose_match_log(check_uniq_url)
            match_log = print if verbose_match_log else (lambda *args, **kwargs: None)
            block_reason = _dispatch_block_reason(check_uniq_url)
            delayed_payload = None

            if verbose_match_log:
                print(f"\n🔍 DEBUG: Начало обработки матча #{i}")
                print(f"   Статус: {status}")
                print(f"   URL: {check_uniq_url}")
                print(f"   Score: {score}")
            elif check_uniq_url not in maps_data and block_reason != "processed":
                print(f"\n🔁 RECHECK матча #{i}: {check_uniq_url} | status={status}")

            if status == 'finished':
                finished_finalize = _finalize_finished_live_series_for_elo(
                    series_key=series_key_from_path,
                    series_url=series_url,
                    first_team_score=score_divs[:2][0].text.strip(),
                    second_team_score=score_divs[:2][1].text.strip(),
                )
                if isinstance(finished_finalize, dict):
                    applied_update = finished_finalize.get("applied_update")
                    if isinstance(applied_update, dict):
                        _emit_live_elo_applied_log("Live ELO finalized from finished series", applied_update)
                        applied_map_key = str(applied_update.get("map_key") or "")
                        if applied_map_key:
                            _drop_delayed_match(applied_map_key, reason="series_finished_live_elo_applied")
                print(f"   ❌ Матч завершен - пропускаем")
                print(f"   ℹ️ Матч завершен")
                return return_status

            if check_uniq_url in maps_data or block_reason == "processed":
                print(f"   ✅ Матч уже в map_id_check.txt - пропускаем: {check_uniq_url}")
                _drop_delayed_match(check_uniq_url, reason="already_in_map_id_check")
                return
            if block_reason == "uncertain_delivery":
                print(f"   ⚠️ Матч заблокирован после uncertain delivery - пропускаем")
                _drop_delayed_match(check_uniq_url, reason="uncertain_delivery_block")
                return
            with monitored_matches_lock:
                delayed_payload = monitored_matches.get(check_uniq_url)
            if delayed_payload is not None:
                allow_live_recheck = bool(delayed_payload.get("allow_live_recheck"))
                target_game_time = float(
                    delayed_payload.get('target_game_time', DELAYED_SIGNAL_TARGET_GAME_TIME)
                )
                target_human = f"{int(target_game_time // 60):02d}:{int(target_game_time % 60):02d}"
                last_game_time = delayed_payload.get('last_game_time', delayed_payload.get('queued_game_time'))
                try:
                    last_game_time_value = float(last_game_time)
                except (TypeError, ValueError):
                    last_game_time_value = None
                try:
                    last_game_time_human = str(int(float(last_game_time)))
                except (TypeError, ValueError):
                    last_game_time_human = "n/a"
                queue_reason = str(delayed_payload.get('reason', 'unknown'))
                monitor_snapshot = _dynamic_monitor_snapshot_for_payload(
                    delayed_payload,
                    last_game_time_value,
                )
                queue_status_label = str(monitor_snapshot.get("status_label") or delayed_payload.get("dispatch_status_label") or "")
                monitor_threshold_raw = monitor_snapshot.get("threshold")
                monitor_suffix = ""
                try:
                    if bool(delayed_payload.get("late_comeback_monitor_active")):
                        monitor_side = str(delayed_payload.get("networth_target_side") or "").strip().lower() or "unknown"
                        monitor_suffix = f", monitor={monitor_side} comeback_ceiling"
                    elif monitor_threshold_raw is not None:
                        monitor_side = str(delayed_payload.get("networth_target_side") or "").strip().lower() or "unknown"
                        monitor_suffix = f", monitor={monitor_side}>={int(float(monitor_threshold_raw))}"
                except (TypeError, ValueError):
                    monitor_suffix = ""
                print(
                    "   ⏳ Матч уже в delayed-очереди "
                    + (
                        "- продолжаем live recheck "
                        if allow_live_recheck
                        else "- пропускаем повторный расчет "
                    )
                    + (
                    f"(target={target_human}, last_game_time={last_game_time_human}, "
                    f"reason={queue_reason}, status={queue_status_label or 'n/a'}{monitor_suffix})"
                    )
                )
                if not allow_live_recheck:
                    return return_status


        except (AttributeError, KeyError, ValueError) as e:
            print(f"   ❌ Ошибка при парсинге данных: {e}")
            print(f"   ❌ Матч пропущен (ошибка парсинга URL/score)")
            return return_status

        # HTTP запрос
        url = f"https://{IP_ADDRESS}{path}"
        match_log(f"   🌐 Запрос страницы матча...")
        response = make_request_with_retry(url, MAX_RETRIES, RETRY_DELAY)

        if not response or response.status_code != 200:
            print(f"   ❌ Не удалось получить страницу. Status code: {response.status_code if response else 'No response'}")
            print(f"   ❌ Матч пропущен (ошибка HTTP запроса)")
            return return_status

        match_log(f"   ✅ Страница получена")
        soup = BeautifulSoup(response.text, 'lxml')

        from urllib.parse import urljoin
        m = re.search(r"\$\.get\(['\"](?P<path>/live/[^'\"]+\.json)['\"]", response.text)
        if not m:
            print(f"   ❌ Не найден JSON путь в HTML")
            print(f"   ❌ Матч пропущен (нет JSON пути)")
            return return_status
        json_path = m.group('path')
        base = "https://dltv.org"  # замениш на реальный сайт, откуда страница
        json_url = urljoin(base, json_path)
        
        match_log(f"   🌐 Запрос JSON данных...")

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
                    match_log(f"   ✅ JSON данные получены")
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
        
        match_log(f"   ✅ fast_picks найдены - драфт начался")
        
        # Определяем какая команда radiant, какая dire
        db_payload = data.get('db') or {}
        db_series_payload = db_payload.get('series') or {}
        db_scores_payload = db_payload.get('scores') or {}
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

        match_log(f"   🆔 Candidate team IDs: radiant={radiant_team_ids}, dire={dire_team_ids}")
        if not radiant_team_ids or not dire_team_ids:
            print(f"   ❌ Отсутствуют team_id для команд")
            print(f"   ❌ Матч пропущен (нет team_id)")
            return return_status
        # Extract league_id if available
        league_id = live_league_data.get('league_id')
        series_id = live_league_data.get('series_id') or db_series_payload.get('id')
        series_type = live_league_data.get('series_type') or db_series_payload.get('type')
        first_team_score = db_scores_payload.get('first_team')
        second_team_score = db_scores_payload.get('second_team')
        first_team_is_radiant = bool(first_team_payload.get('is_radiant'))
        series_url = f'dltv.org{path}'
        league_name = (
            str(live_league_data.get('league_name') or "").strip()
            or str((db_payload.get('league') or {}).get('title') or "").strip()
        )
        
        # Debug: print available keys in live_league_data
        lld_keys = list(live_league_data.keys())
        match_log(f"   📋 live_league_data keys: {lld_keys}")
        if league_id:
            match_log(f"   🏆 League ID: {league_id}")
        if series_id:
            match_log(f"   📊 Series ID: {series_id}")
        
        # Сохраняем нормализованные имена для обратной совместимости
        radiant_team_name = normalize_team_name(radiant_team_name_original)
        dire_team_name = normalize_team_name(dire_team_name_original)

        # Стартуем prefetch кэфов ДО анализа драфта (в отдельном воркере, без блокировки основного пайплайна).
        # Ключевой момент: парсим именно текущую карту серии, а не "1-я карта" по умолчанию.
        if BOOKMAKER_PREFETCH_ENABLED:
            bookmaker_map_num = _bookmaker_infer_map_num(live_league_data, score_text=score)
            if bookmaker_map_num is not None:
                match_log(f"   🗺️ Bookmaker map context: карта {bookmaker_map_num}")
            _bookmaker_prefetch_submit(
                match_key=check_uniq_url,
                radiant_team=radiant_team_name_original,
                dire_team=dire_team_name_original,
                map_num=bookmaker_map_num,
            )

        # Tier-режим для star-сигналов:
        # - Tier 2 матч: если хотя бы одна команда Tier 2
        # - Tier 1 матч: если обе команды Tier 1
        # - Неизвестная команда автоматически добавляется в Tier 2 без Telegram уведомления
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
            _deliver_and_persist_signal(
                check_uniq_url,
                skip_msg,
                add_url_reason="skip_tier_undetermined",
                add_url_details={
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
            TIER_THRESHOLD_STATUS_TIER2_MIN60_BLOCK
            if star_match_tier == 2
            else TIER_THRESHOLD_STATUS_TIER1_MIN60_BLOCK
        )
        tier_threshold_block_reason_label = (
            TIER_THRESHOLD_REASON_TIER2_MIN60_BLOCK
            if star_match_tier == 2
            else TIER_THRESHOLD_REASON_TIER1_MIN60_BLOCK
        )
        match_log(f"   🧭 Star tier mode: tier={star_match_tier}, min_wr={star_target_wr}%")

        lead = data['radiant_lead']
        game_time = data['game_time']
        match_log(f"   Lead: {lead}, Game time: {game_time}")

        stale_live_map = _find_stale_live_map_payload(
            series_key=series_id,
            map_key=check_uniq_url,
            live_match_id=_extract_live_match_id(data),
        )
        if isinstance(stale_live_map, dict):
            print(
                "   ⚠️ Обнаружен stale payload прошлой карты: "
                f"current_map={stale_live_map['current_map_key']}, "
                f"duplicate_of={stale_live_map['duplicate_of_map_key']}, "
                f"match_id={stale_live_map['match_id']}"
            )
            print("   ℹ️ map_id_check.txt не обновлен: матч будет перепроверен в следующем цикле")
            return return_status
        
        # Парсим драфт и позиции - вся логика в отдельной функции
        match_log(f"   🔍 Парсинг драфта и позиций...")
        if verbose_match_log:
            radiant_heroes_and_pos, dire_heroes_and_pos, parse_error, problem_summary, problem_candidates = parse_draft_and_positions(
                soup, data, radiant_team_name_original, dire_team_name_original
            )
        else:
            with contextlib.redirect_stdout(io.StringIO()):
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

        heroes_valid, heroes_error = validate_heroes_data(
            radiant_heroes_and_pos,
            dire_heroes_and_pos,
            check_account_ids=False,
        )
        if not heroes_valid:
            print(f"   ❌ Ошибка валидации драфта: {heroes_error}")
            print("   ❌ Матч пропущен (драфт невалиден для расчета сигнала)")
            print("   ℹ️ map_id_check.txt не обновлен: матч будет перепроверен в следующем цикле")
            return return_status
        
        match_log(f"   ✅ Драфт успешно распарсен")
        if verbose_match_log:
            _mark_verbose_match_log_done(check_uniq_url)
        radiant_account_ids = [
            int((radiant_heroes_and_pos.get(pos) or {}).get("account_id", 0) or 0)
            for pos in ("pos1", "pos2", "pos3", "pos4", "pos5")
        ]
        dire_account_ids = [
            int((dire_heroes_and_pos.get(pos) or {}).get("account_id", 0) or 0)
            for pos in ("pos1", "pos2", "pos3", "pos4", "pos5")
        ]
        live_elo_registration = _register_completed_live_map_for_elo(
            series_key=series_id,
            series_url=series_url,
            map_key=check_uniq_url,
            first_team_score=first_team_score,
            second_team_score=second_team_score,
            first_team_is_radiant=first_team_is_radiant,
            map_match_id=data.get('match_id'),
            observed_timestamp=data.get('now'),
            radiant_team_id=radiant_team_id,
            dire_team_id=dire_team_id,
            radiant_team_name=radiant_team_name_original,
            dire_team_name=dire_team_name_original,
            radiant_account_ids=radiant_account_ids,
            dire_account_ids=dire_account_ids,
            league_id=league_id,
            league_name=league_name,
            series_type=series_type,
            match_tier=star_match_tier,
        )
        if isinstance(live_elo_registration, dict):
            applied_update = live_elo_registration.get("applied_update")
            if isinstance(applied_update, dict):
                _emit_live_elo_applied_log("Live ELO updated from completed map", applied_update)
        _warm_draft_stats_shards(radiant_heroes_and_pos, dire_heroes_and_pos)
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
        lane_top_log = str(s.get('top') or '').strip()
        lane_mid_log = str(s.get('mid') or '').strip()
        lane_bot_log = str(s.get('bot') or '').strip()
        if verbose_match_log:
            print("   🛣️ Lanes:")
            print(f"      {lane_top_log or 'Top: n/a'}")
            print(f"      {lane_mid_log or 'Mid: n/a'}")
            print(f"      {lane_bot_log or 'Bot: n/a'}")
        comeback_metrics = None
        if comeback_dict and comeback_baseline_wr_pct is not None:
            try:
                comeback_metrics = calculate_comeback_solo_metrics(
                    radiant_heroes_and_pos=radiant_heroes_and_pos,
                    dire_heroes_and_pos=dire_heroes_and_pos,
                    comeback_dict=comeback_dict,
                    baseline_wr_pct=comeback_baseline_wr_pct,
                )
            except Exception as comeback_exc:
                print(f"   ⚠️ Comeback metric calculation failed: {comeback_exc}")
                comeback_metrics = None
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
            early_diag = _star_block_diagnostics(
                raw_block=candidate.get('early_output'),
                target_wr=target_wr,
                section="early_output",
            )
            late_diag = _star_block_diagnostics(
                raw_block=candidate.get('mid_output'),
                target_wr=target_wr,
                section="mid_output",
            )
            has_early_star = bool(early_diag.get("valid"))
            has_late_star = bool(late_diag.get("valid"))
            early_sign = early_diag.get("sign") if has_early_star else None
            late_sign = late_diag.get("sign") if has_late_star else None
            if has_late_star and not has_early_star and STAR_REQUIRE_EARLY_WITH_LATE_SAME_SIGN:
                return (
                    False,
                    "late_star_requires_early_same_sign(early_star=no)",
                )
            if (
                has_early_star
                and has_late_star
                and early_sign != late_sign
                and not STAR_DELAY_ON_OPPOSITE_SIGNS
            ):
                return (
                    False,
                    f"opposite_signs_disabled(early_sign={early_sign},late_sign={late_sign})",
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
        star_fallback_wr = (
            TIER_SIGNAL_MIN_THRESHOLD_TIER1
            if (
                star_match_tier == 2
                and STAR_ALLOW_TIER2_FALLBACK_TO_TIER1
                and star_target_wr > TIER_SIGNAL_MIN_THRESHOLD_TIER1
            )
            else None
        )
        if not has_valid_star_signal and star_fallback_wr is not None:
            has_star_fallback, cand_fallback = _build_star_candidate(star_fallback_wr)
            if has_star_fallback:
                fallback_passed, fallback_reason = _candidate_passes_extra_filters(
                    cand_fallback,
                    star_match_tier,
                    star_fallback_wr,
                )
                if fallback_passed:
                    has_valid_star_signal = True
                    selected_star_candidate = cand_fallback
                    selected_star_wr = star_fallback_wr
                    selected_star_mode = f"tier2_fallback_wr_{star_fallback_wr}"
                elif fallback_reason != "ok":
                    star_filter_rejections.append(
                        f"tier2_fallback_wr_{star_fallback_wr}:{fallback_reason}"
                    )
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
        early65_gate_diag = None
        if (
            (star_match_tier == 1 and star_target_wr < 65)
            or (star_match_tier == 2 and star_target_wr >= 65)
        ):
            early65_gate_diag = _star_block_diagnostics(
                raw_block=star_base_early_output,
                target_wr=65,
                section="early_output",
            )
            star_diag_lines.append(
                f"WR65: early={_format_star_block_status(early65_gate_diag)}"
            )
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
            late_core_same_sign_diag = _block_signs_same_or_zero(
                raw_block=s.get('mid_output', {}),
                expected_sign=selected_early_sign,
                metrics=_STAR_LATE_CORE_METRIC_ORDER,
                allow_zero=False,
            )
            early_same_or_zero_diag = _block_signs_same_or_zero(
                raw_block=s.get('early_output', {}),
                expected_sign=selected_late_sign,
            )
            early_core_same_or_zero_diag = _block_signs_same_or_zero(
                raw_block=s.get('early_output', {}),
                expected_sign=selected_late_sign,
                metrics=_STAR_LATE_CORE_METRIC_ORDER,
                allow_zero=False,
            )
            early_core_same_sign_support = bool(
                early_core_same_or_zero_diag.get("valid")
                and early_core_same_or_zero_diag.get("nonzero_metrics")
            )
            early_core_conflict = bool(early_core_same_or_zero_diag.get("conflicting_metrics"))
            # Приоритет dispatch:
            # 1) full-star same-sign
            # 2) early-star + late core(cp1v1/cp1v2/solo) same-sign
            # 3) late-star + early same-sign-or-zero
            # 4) delayed ветки / reject
            send_now_full_star = (
                has_selected_early_star
                and has_selected_late_star
                and selected_early_sign == selected_late_sign
            )
            send_now_early_star_late_core_same_sign = (
                not force_odds_signal_test_active
                and has_selected_early_star
                and not has_selected_late_star
                and bool(late_core_same_sign_diag.get("valid"))
            )
            send_now_late_star_early_core_same_sign = (
                STAR_ALLOW_LATE_STAR_EARLY_SAME_OR_ZERO
                and has_selected_late_star
                and not has_selected_early_star
                and early_core_same_sign_support
            )
            early65_gate_active = bool(
                not force_odds_signal_test_active
                and STAR_ALLOW_IMMEDIATE_EARLY_STAR65
                and early65_gate_diag is not None
                and bool(early65_gate_diag.get("valid"))
                and (
                    not has_selected_late_star
                    or selected_late_sign == early65_gate_diag.get("sign")
                )
            )
            send_now_immediate = (
                send_now_full_star
                or send_now_early_star_late_core_same_sign
                or send_now_late_star_early_core_same_sign
                or force_odds_signal_test_active
            )

            def _format_metrics(title, data, metrics):
                lines = [title]
                for key, label in metrics:
                    lines.append(f"{label}: {data.get(key)}")
                return "\n".join(lines) + "\n"

            metric_list = [
                ('counterpick_1vs1', 'Counterpick_1vs1'),
                ('pos1_vs_pos1', 'Pos1_vs_pos1'),
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
            early_output = early_output_log
            mid_output = mid_output_log

            early_block = _format_metrics("10-28 Minute:", early_output, metric_list)
            mid_block = _format_metrics("Mid (25-50 min):", mid_output, metric_list)
            early_block_log = _format_metrics("10-28 Minute:", early_output_log, metric_list)
            mid_block_log = _format_metrics("Mid (25-50 min):", mid_output_log, metric_list)
            star_metrics_snapshot = _build_star_metrics_snapshot(
                early_block_log=early_block_log,
                mid_block_log=mid_block_log,
                raw_star_early_summary=raw_star_early_summary,
                raw_star_late_summary=raw_star_late_summary,
                star_diag_lines=star_diag_lines,
            )

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

            early_rec = _recommend_odds_for_block(early_output, 'early')
            late_rec = _recommend_odds_for_block(mid_output, 'late')
            telegram_early_rec = early_rec
            telegram_early_block = early_block
            early_wr_pct: Optional[float] = None
            if isinstance(early_rec, dict):
                try:
                    early_wr_pct = float(early_rec.get("wr_pct"))
                except (TypeError, ValueError):
                    early_wr_pct = None
            late_wr_pct: Optional[float] = None
            if isinstance(late_rec, dict):
                try:
                    late_wr_pct = float(late_rec.get("wr_pct"))
                except (TypeError, ValueError):
                    late_wr_pct = None
            opposite_sign_early_release_allowed = bool(
                early_wr_pct is not None and early_wr_pct <= 65.0
            )
            team_elo_block = ""
            team_elo_summary = _build_team_elo_matchup_summary(
                radiant_team_id=radiant_team_id,
                dire_team_id=dire_team_id,
                radiant_team_name=radiant_team_name_original,
                dire_team_name=dire_team_name_original,
                radiant_account_ids=radiant_account_ids,
                dire_account_ids=dire_account_ids,
                match_tier=star_match_tier,
            )
            team_elo_block, team_elo_meta = _format_team_elo_block(
                team_elo_summary,
                radiant_team_name=radiant_team_name_original,
                dire_team_name=dire_team_name_original,
            )
            if isinstance(team_elo_meta, dict):
                if verbose_match_log:
                    print(
                        "   📊 Team ELO attached: "
                        f"source={str(team_elo_meta.get('source') or 'unknown')} "
                        f"raw {radiant_team_name_original}={float(team_elo_meta['radiant_base_rating']):.0f} "
                        f"vs {dire_team_name_original}={float(team_elo_meta['dire_base_rating']):.0f} "
                        f"(raw_wr={float(team_elo_meta['raw_radiant_wr']):.1f}%/{float(team_elo_meta['raw_dire_wr']):.1f}%, "
                        f"adj_wr={float(team_elo_meta['adjusted_radiant_wr']):.1f}%/{float(team_elo_meta['adjusted_dire_wr']):.1f}%)"
                    )
            else:
                if verbose_match_log:
                    print(
                        "   ⚠️ Team ELO unavailable for signal: "
                        f"{radiant_team_name_original} vs {dire_team_name_original}"
                    )

            raw_selected_early_diag = dict(selected_early_diag)
            raw_selected_late_diag = dict(selected_late_diag)
            raw_selected_early_valid = bool(raw_selected_early_diag.get("valid"))
            raw_selected_late_valid = bool(raw_selected_late_diag.get("valid"))
            selected_early_diag = _apply_elo_block_wr_guard(
                diag=selected_early_diag,
                block_wr_pct=early_wr_pct,
                team_elo_meta=team_elo_meta,
            )
            selected_late_diag = _apply_elo_block_wr_guard(
                diag=selected_late_diag,
                block_wr_pct=late_wr_pct,
                team_elo_meta=team_elo_meta,
            )
            if raw_selected_early_valid and not bool(selected_early_diag.get("valid")):
                if verbose_match_log:
                    print(
                        "   ⚠️ Early star invalidated by ELO block guard "
                        f"(raw_wr={float(selected_early_diag.get('block_wr_pct') or 0.0):.1f}%, "
                        f"penalty={float(selected_early_diag.get('elo_wr_penalty_pp') or 0.0):.1f}, "
                        f"adj={float(selected_early_diag.get('elo_adjusted_wr_pct') or 0.0):.1f}%)"
                    )
            if raw_selected_late_valid and not bool(selected_late_diag.get("valid")):
                if verbose_match_log:
                    print(
                        "   ⚠️ Late star invalidated by ELO block guard "
                        f"(raw_wr={float(selected_late_diag.get('block_wr_pct') or 0.0):.1f}%, "
                        f"penalty={float(selected_late_diag.get('elo_wr_penalty_pp') or 0.0):.1f}, "
                        f"adj={float(selected_late_diag.get('elo_adjusted_wr_pct') or 0.0):.1f}%)"
                    )
            star_diag_lines.append(
                (
                    "ELO60: "
                    f"early={_format_star_block_status(selected_early_diag)}, "
                    f"late={_format_star_block_status(selected_late_diag)}"
                )
            )
            if isinstance(star_metrics_snapshot, dict):
                star_metrics_snapshot["star_diag_lines"] = [str(line) for line in star_diag_lines]

            has_selected_early_star = bool(selected_early_diag.get("valid"))
            has_selected_late_star = bool(selected_late_diag.get("valid"))
            selected_early_sign = selected_early_diag.get("sign") if has_selected_early_star else None
            selected_late_sign = selected_late_diag.get("sign") if has_selected_late_star else None
            top25_late_elo_block_override = _top25_late_elo_block_opposite_monitor_override(
                team_elo_meta=team_elo_meta,
                selected_early_diag=selected_early_diag,
                selected_late_diag=selected_late_diag,
                raw_selected_early_diag=raw_selected_early_diag,
                raw_selected_late_diag=raw_selected_late_diag,
            )
            top25_late_elo_block_override_active = bool(
                isinstance(top25_late_elo_block_override, dict)
                and top25_late_elo_block_override.get("enabled")
            )
            late_core_same_sign_diag = _block_signs_same_or_zero(
                raw_block=s.get('mid_output', {}),
                expected_sign=selected_early_sign,
                metrics=_STAR_LATE_CORE_METRIC_ORDER,
                allow_zero=False,
            )
            early_same_or_zero_diag = _block_signs_same_or_zero(
                raw_block=s.get('early_output', {}),
                expected_sign=selected_late_sign,
            )
            early_core_same_or_zero_diag = _block_signs_same_or_zero(
                raw_block=s.get('early_output', {}),
                expected_sign=selected_late_sign,
                metrics=_STAR_LATE_CORE_METRIC_ORDER,
                allow_zero=False,
            )
            early_core_same_sign_support = bool(
                early_core_same_or_zero_diag.get("valid")
                and early_core_same_or_zero_diag.get("nonzero_metrics")
            )
            early_core_conflict = bool(early_core_same_or_zero_diag.get("conflicting_metrics"))
            late_same_sign_raw_star_before_elo = bool(
                has_selected_early_star
                and not has_selected_late_star
                and bool(raw_selected_late_diag.get("valid"))
                and raw_selected_late_diag.get("sign") == selected_early_sign
                and str(selected_late_diag.get("status") or "") == "elo_wr_below_min60"
            )
            send_now_full_star = (
                has_selected_early_star
                and has_selected_late_star
                and selected_early_sign == selected_late_sign
            )
            send_now_early_star_late_core_same_sign = (
                not force_odds_signal_test_active
                and has_selected_early_star
                and not has_selected_late_star
                and (
                    bool(late_core_same_sign_diag.get("valid"))
                    or late_same_sign_raw_star_before_elo
                )
            )
            send_now_late_star_early_core_same_sign = (
                STAR_ALLOW_LATE_STAR_EARLY_SAME_OR_ZERO
                and has_selected_late_star
                and not has_selected_early_star
                and early_core_same_sign_support
            )
            early65_gate_active = bool(
                not force_odds_signal_test_active
                and STAR_ALLOW_IMMEDIATE_EARLY_STAR65
                and early65_gate_diag is not None
                and bool(early65_gate_diag.get("valid"))
                and (
                    not has_selected_late_star
                    or selected_late_sign == early65_gate_diag.get("sign")
                )
            )
            send_now_immediate = (
                send_now_full_star
                or send_now_early_star_late_core_same_sign
                or send_now_late_star_early_core_same_sign
                or force_odds_signal_test_active
            )
            dispatch_mode = (
                "immediate_force_odds_signal_test"
                if force_odds_signal_test_active
                else (
                    "immediate_early_late_same_sign"
                    if send_now_full_star
                    else (
                        "immediate_early_star_late_core_same_sign"
                        if send_now_early_star_late_core_same_sign
                        else (
                            "immediate_late_star_early_core_same_sign"
                            if send_now_late_star_early_core_same_sign
                            else "delayed_late_only_20_20m"
                        )
                    )
                )
            )
            if top25_late_elo_block_override_active:
                dispatch_mode = "delayed_late_elo_block_top25_opposite_monitor"

            if (
                not force_odds_signal_test_active
                and not has_selected_late_star
                and not top25_late_elo_block_override_active
                and not send_now_early_star_late_core_same_sign
                and not (
                    early65_gate_active
                    and float(game_time or 0.0) < NETWORTH_GATE_TIER1_EARLY65_WINDOW_END_SECONDS
                )
            ):
                print(
                    "   ⚠️ ВЕРДИКТ: ОТКАЗ "
                    "(нет late star-сигнала) - матч пропущен"
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
                        "late_core_same_sign_diag": late_core_same_sign_diag,
                        "selected_early_diag": selected_early_diag,
                        "selected_late_diag": selected_late_diag,
                        "json_retry_errors": json_retry_errors,
                    },
                )
                print("   ✅ map_id_check.txt обновлен: add_url после отказа no-late-star")
                return return_status

            if top25_late_elo_block_override_active and verbose_match_log:
                override_target_side = str(top25_late_elo_block_override.get("target_side") or "")
                override_rank = top25_late_elo_block_override.get("leaderboard_rank")
                override_adj_wr = top25_late_elo_block_override.get("elo_target_wr")
                override_mode = str(top25_late_elo_block_override.get("mode") or "unknown")
                override_adj_wr_label = (
                    f"{float(override_adj_wr):.1f}%"
                    if override_adj_wr is not None
                    else "n/a"
                )
                print(
                    "   ✅ Override: raw late star kept alive despite ELO block "
                    f"because target side is top-{int(TOP25_LATE_ELO_BLOCK_RANK_THRESHOLD)} "
                    f"(mode={override_mode}, side={override_target_side}, "
                    f"rank={override_rank}, "
                    f"adj_wr={override_adj_wr_label})"
                )
            if top25_late_elo_block_override_active:
                override_adj_wr = top25_late_elo_block_override.get("elo_target_wr")
                override_mode = str(top25_late_elo_block_override.get("mode") or "unknown")
                adj_wr_label = (
                    f"{float(override_adj_wr):.1f}%"
                    if override_adj_wr is not None
                    else "n/a"
                )
                star_diag_lines.append(
                    "Top25LateEloBlock: "
                    f"enabled(mode={override_mode},rank={int(top25_late_elo_block_override.get('leaderboard_rank') or 0)},"
                    f"target_side={top25_late_elo_block_override.get('target_side')},"
                    f"adj_wr={adj_wr_label})"
                )
                if isinstance(star_metrics_snapshot, dict):
                    star_metrics_snapshot["star_diag_lines"] = [str(line) for line in star_diag_lines]

            if send_now_early_star_late_core_same_sign:
                if late_same_sign_raw_star_before_elo and not bool(late_core_same_sign_diag.get("valid")):
                    if verbose_match_log:
                        print(
                            "   ✅ Override: early star without late star allowed because "
                            "late raw star kept same sign before ELO invalidation "
                            f"(sign={selected_early_sign})"
                        )
                else:
                    if verbose_match_log:
                        print(
                            "   ✅ Override: early star without late star allowed because "
                            "late core(cp1v1/cp1v2/solo) are same-sign "
                            f"(sign={selected_early_sign})"
                        )
            if early65_gate_active:
                if verbose_match_log:
                    print(
                        "   ✅ Override: early star WR65+ activates early gate "
                        f"(sign={early65_gate_diag.get('sign')}, "
                        f"late_sign={selected_late_sign})"
                    )
            if send_now_late_star_early_core_same_sign:
                if verbose_match_log:
                    print(
                        "   ✅ Override: late star without early star allowed because "
                        "early core(cp1v1/cp1v2/solo) are same-sign "
                            f"(sign={selected_late_sign})"
                        )

            late_display_sign = (
                raw_selected_late_diag.get("sign")
                if top25_late_elo_block_override_active and raw_selected_late_diag.get("sign") in (-1, 1)
                else selected_late_sign
            )
            target_sign = (
                int(top25_late_elo_block_override.get("target_sign"))
                if top25_late_elo_block_override_active
                else (selected_late_sign if has_selected_late_star else selected_early_sign)
            )
            target_side = _target_side_from_sign(target_sign)
            opposite_signs_early90_monitor = _opposite_signs_early90_monitor_config(
                team_elo_meta=team_elo_meta,
                early_wr_pct=early_wr_pct,
                selected_early_sign=selected_early_sign,
                selected_late_sign=selected_late_sign,
            )
            if isinstance(opposite_signs_early90_monitor, dict) and opposite_signs_early90_monitor.get("enabled"):
                elo_gap_log = opposite_signs_early90_monitor.get("elo_gap_pp")
                elo_gap_label = (
                    f"{float(elo_gap_log):+.1f} pp"
                    if elo_gap_log is not None
                    else "n/a"
                )
                if verbose_match_log:
                    print(
                        "   📈 Opposite-sign WR90 monitor: "
                        f"early_side={opposite_signs_early90_monitor.get('early_side')}, "
                        f"late_side={opposite_signs_early90_monitor.get('late_side')}, "
                        f"early_elo_wr={opposite_signs_early90_monitor.get('early_elo_wr')}, "
                        f"late_elo_wr={opposite_signs_early90_monitor.get('late_elo_wr')}, "
                        f"gap={elo_gap_label}, "
                        f"4_10>={int(float(opposite_signs_early90_monitor.get('threshold_4_to_10') or 0.0))}, "
                        f"10_20>={int(float(opposite_signs_early90_monitor.get('threshold_10_to_20') or 0.0))}"
                    )
            signal_wr_guard_meta = _resolve_signal_wr_for_elo_guard(
                target_side=target_side,
                has_selected_early_star=bool(has_selected_early_star),
                has_selected_late_star=bool(has_selected_late_star),
                selected_early_sign=selected_early_sign,
                selected_late_sign=selected_late_sign,
                early_wr_pct=early_wr_pct,
                late_wr_pct=late_wr_pct,
            )
            elo_underdog_guard = None
            if not force_odds_signal_test_active and not top25_late_elo_block_override_active:
                elo_underdog_guard = _elo_underdog_guard_decision(
                    team_elo_meta=team_elo_meta,
                    target_side=target_side,
                    signal_wr_pct=(
                        float(signal_wr_guard_meta.get("wr_pct"))
                        if isinstance(signal_wr_guard_meta, dict) and signal_wr_guard_meta.get("wr_pct") is not None
                        else None
                    ),
                )
            if isinstance(elo_underdog_guard, dict) and bool(elo_underdog_guard.get("reject")):
                favorite_side = str(elo_underdog_guard.get("favorite_side") or "")
                favorite_team_name = (
                    radiant_team_name_original
                    if favorite_side == "radiant"
                    else dire_team_name_original
                )
                target_team_name = (
                    radiant_team_name_original
                    if target_side == "radiant"
                    else dire_team_name_original
                )
                signal_wr_pct_value = elo_underdog_guard.get("signal_wr_pct")
                signal_wr_label = (
                    f"{float(signal_wr_pct_value):.1f}%"
                    if signal_wr_pct_value is not None
                    else "n/a"
                )
                signal_wr_source = (
                    str(signal_wr_guard_meta.get("source"))
                    if isinstance(signal_wr_guard_meta, dict)
                    else "unknown"
                )
                print(
                    "   ⚠️ ELO underdog guard: reject signal "
                    f"for {target_team_name} vs favorite {favorite_team_name} "
                    f"(favorite_wr={float(elo_underdog_guard.get('favorite_wr') or 0.0):.1f}%, "
                    f"signal_wr={signal_wr_label}, source={signal_wr_source})"
                )
                add_url(
                    check_uniq_url,
                    reason="star_signal_rejected_elo_underdog_guard",
                    details={
                        "status": status,
                        "dispatch_mode": dispatch_mode,
                        "selected_star_wr": selected_star_wr,
                        "selected_star_mode": selected_star_mode,
                        "target_side": target_side,
                        "target_team_name": target_team_name,
                        "favorite_side": favorite_side,
                        "favorite_team_name": favorite_team_name,
                        "favorite_elo_wr": float(elo_underdog_guard.get("favorite_wr") or 0.0),
                        "target_elo_wr": float(elo_underdog_guard.get("target_elo_wr") or 0.0),
                        "favorite_edge_pp": float(elo_underdog_guard.get("favorite_edge_pp") or 0.0),
                        "signal_wr_pct": signal_wr_pct_value,
                        "signal_wr_source": signal_wr_source,
                        "signal_wr_candidates": (
                            dict(signal_wr_guard_meta.get("candidates") or {})
                            if isinstance(signal_wr_guard_meta, dict)
                            else {}
                        ),
                        "required_signal_wr_pct": float(elo_underdog_guard.get("min_signal_wr") or 0.0),
                        "json_retry_errors": json_retry_errors,
                    },
                )
                print("   ✅ map_id_check.txt обновлен: add_url после ELO underdog guard reject")
                return return_status
            if has_selected_late_star and not has_selected_early_star:
                telegram_early_rec = None
                telegram_early_block = ""
            wr_block = ""
            wr_lines = []

            def _signal_team_name(sign: Optional[int]) -> str:
                side = _target_side_from_sign(sign)
                if side == "radiant":
                    return str(radiant_team_name_original or radiant_team_name or "Radiant")
                if side == "dire":
                    return str(dire_team_name_original or dire_team_name or "Dire")
                return ""

            if telegram_early_rec:
                early_team_name = _signal_team_name(selected_early_sign)
                if early_team_name:
                    wr_lines.append(f"Early: {early_team_name} WR≈{float(early_wr_pct or 0.0):.1f}%")
                else:
                    wr_lines.append(f"Early: WR≈{float(early_wr_pct or 0.0):.1f}%")
            if late_rec:
                late_team_name = _signal_team_name(late_display_sign)
                if late_team_name:
                    wr_lines.append(f"Late: {late_team_name} WR≈{float(late_wr_pct or 0.0):.1f}%")
                else:
                    wr_lines.append(f"Late: WR≈{float(late_wr_pct or 0.0):.1f}%")
            if wr_lines:
                wr_block = "Оценка WR:\n" + "\n".join(wr_lines) + "\n"

            comeback_block = ""
            if isinstance(comeback_metrics, dict):
                radiant_comeback = comeback_metrics.get("radiant") or {}
                dire_comeback = comeback_metrics.get("dire") or {}

                def _fmt_delta(value):
                    if value is None:
                        return "n/a"
                    return f"{float(value):+,.1f}%".replace(",", "")

                if radiant_comeback or dire_comeback:
                    comeback_lines = []
                    if radiant_comeback:
                        comeback_lines.append(
                            f"radiant_comeback: {_fmt_delta(radiant_comeback.get('delta_pp'))}"
                        )
                    if dire_comeback:
                        comeback_lines.append(
                            f"dire_comeback: {_fmt_delta(dire_comeback.get('delta_pp'))}"
                        )
                    if comeback_lines:
                        comeback_block = "\n".join(comeback_lines) + "\n"
                    if verbose_match_log:
                        print(
                            "   📈 Comeback solo: "
                            f"radiant={radiant_comeback.get('delta_pp')} pp "
                            f"(wr={radiant_comeback.get('wr_pct')}), "
                            f"dire={dire_comeback.get('delta_pp')} pp "
                            f"(wr={dire_comeback.get('wr_pct')}), "
                            f"baseline={comeback_metrics.get('baseline_wr_pct')}"
                        )

            odds_block = ""
            if BOOKMAKER_PREFETCH_ENABLED:
                # Рекомендации по минимальному кэфу
                odds_lines = []
                bookmaker_odds_block = ""
                bookmaker_odds_ready = False
                bookmaker_odds_reason = "not_requested"
                if telegram_early_rec:
                    odds_label = f"{telegram_early_rec['min_odds']:.2f}"
                    wr_label = f"{float(telegram_early_rec['wr_pct']):.1f}%"
                    odds_lines.append(
                        f"Early: от кэфа {odds_label} (WR≈{wr_label})"
                    )
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
            if problem_candidates:
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

            dispatch_message_sign = target_sign
            if early65_gate_active and early65_sign is not None:
                dispatch_message_sign = early65_sign
            elif top25_late_elo_block_override_active and target_sign in (-1, 1):
                dispatch_message_sign = target_sign
            elif send_now_early_star_late_core_same_sign and selected_early_sign is not None:
                dispatch_message_sign = selected_early_sign
            elif send_now_late_star_early_core_same_sign and selected_late_sign is not None:
                dispatch_message_sign = selected_late_sign
            elif has_selected_late_star and selected_late_sign is not None:
                dispatch_message_sign = selected_late_sign
            elif has_selected_early_star and selected_early_sign is not None:
                dispatch_message_sign = selected_early_sign

            dispatch_message_side = _target_side_from_sign(dispatch_message_sign)
            stake_team_name = (
                radiant_team_name
                if dispatch_message_side == "radiant"
                else dire_team_name
                if dispatch_message_side == "dire"
                else "НЕИЗВЕСТНАЯ КОМАНДА"
            )

            # Формирование сообщения
            message_text = (
                f"СТАВКА НА {stake_team_name}\n"
                f"{radiant_team_name} VS {dire_team_name}\n"
                f"{series_score_line}"
                f"Lanes:\n{s.get('top')}{s.get('mid')}{s.get('bot')}"
                f"{problem_block}"
                f"{team_elo_block}"
                f"{wr_block}"
                f"{comeback_block}"
                f"{odds_block}"
                f"{telegram_early_block}"
                f"{mid_block}"
            )
            current_game_time = float(game_time or 0.0)
            early65_sign = (
                early65_gate_diag.get("sign")
                if isinstance(early65_gate_diag, dict) and early65_gate_diag.get("valid")
                else None
            )
            early65_target_side = _target_side_from_sign(early65_sign)
            early65_target_diff = _target_networth_diff_from_radiant_lead(
                lead,
                early65_target_side,
            )
            target_networth_diff = _target_networth_diff_from_radiant_lead(lead, target_side)
            target_comeback_delta_pp = _comeback_delta_pp_for_side(comeback_metrics, target_side)
            late_comeback_monitor_candidate = bool(
                has_selected_late_star
                and target_side in {"radiant", "dire"}
                and isinstance(late_comeback_ceiling_thresholds, dict)
                and bool(late_comeback_ceiling_thresholds)
            )
            networth_send_status_label: Optional[str] = None
            queue_early_core_monitor = False
            queue_late_core_monitor = False
            queue_strong_same_sign_monitor = False
            queue_top25_late_elo_block_monitor = bool(top25_late_elo_block_override_active)
            early65_release_status_label: Optional[str] = None
            early_star_gate_wr_pct = (
                float(early_wr_pct)
                if early_wr_pct is not None
                else float(selected_star_wr or 0.0)
            )
            early_core_monitor_threshold = float(NETWORTH_GATE_EARLY_CORE_MONITOR_DIFF)
            early_core_monitor_wait_status_label = NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_1500
            early_core_monitor_delay_reason = "early_star_late_core_wait_1500"
            if not force_odds_signal_test_active:
                if (
                    early65_gate_active
                    and early65_target_side is not None
                    and early65_target_diff is not None
                ):
                    if current_game_time < NETWORTH_GATE_HARD_BLOCK_SECONDS:
                        print(
                            "   ⏳ Ожидание dispatch: pre4_block_early65 "
                            f"(now={_format_game_clock(current_game_time)}, "
                            f"target_side={early65_target_side}, "
                            f"target_diff={int(early65_target_diff)})"
                        )
                        return return_status
                    if current_game_time < NETWORTH_GATE_EARLY_WINDOW_END_SECONDS:
                        if early65_target_diff >= NETWORTH_GATE_TIER1_EARLY65_4_TO_10_MIN_DIFF:
                            early65_release_status_label = NETWORTH_STATUS_TIER1_EARLY65_4_10_SEND_600
                        else:
                            print(
                                "   ⏳ Ожидание dispatch: early65_gate_04_10 "
                                f"(target_side={early65_target_side}, "
                                f"target_diff={int(early65_target_diff)}, "
                                f"need>={int(NETWORTH_GATE_TIER1_EARLY65_4_TO_10_MIN_DIFF)})"
                            )
                            return return_status
                    elif current_game_time < NETWORTH_GATE_TIER1_EARLY65_WINDOW_END_SECONDS:
                        if early65_target_diff >= NETWORTH_GATE_TIER1_EARLY65_10_TO_13_MAX_LOSS:
                            early65_release_status_label = NETWORTH_STATUS_TIER1_EARLY65_10_13_LOSS_LE1500_SEND
                        else:
                            print(
                                "   ⏳ Ожидание dispatch: early65_gate_10_13 "
                                f"(target_side={early65_target_side}, "
                                f"target_diff={int(early65_target_diff)}, "
                                f"need>={int(NETWORTH_GATE_TIER1_EARLY65_10_TO_13_MAX_LOSS)})"
                            )
                            return return_status
                if target_networth_diff is None or target_side is None:
                    print(
                        "   ⏳ Ожидание dispatch: target-side networth gate не применен "
                        "(нет target_sign/lead)"
                    )
                    return return_status
                if early65_release_status_label is not None:
                    if _skip_dispatch_for_processed_url(check_uniq_url, "early WR65 немедленной отправки"):
                        return return_status
                    if not _acquire_signal_send_slot(check_uniq_url):
                        print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                        return return_status
                    try:
                        if _skip_dispatch_for_processed_url(check_uniq_url, "early WR65 немедленной отправки после lock"):
                            return return_status
                        if verbose_match_log:
                            _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
                        delivery_confirmed = _deliver_and_persist_signal(
                            check_uniq_url,
                            message_text,
                            add_url_reason="star_signal_sent_now_networth_gate",
                            add_url_details={
                                "status": status,
                                "dispatch_mode": "immediate_early_star65",
                                "delay_reason": "early65_gate",
                                "release_reason": early65_release_status_label,
                                "dispatch_status_label": early65_release_status_label,
                                "game_time": int(current_game_time),
                                "target_side": early65_target_side,
                                "target_networth_diff": float(early65_target_diff or 0.0),
                                "selected_star_wr": selected_star_wr,
                                "selected_star_mode": selected_star_mode,
                                "json_retry_errors": json_retry_errors,
                            },
                            bookmaker_decision="sent",
                        )
                        if delivery_confirmed:
                            print(
                                "   ✅ ВЕРДИКТ: Сигнал отправлен раньше 13:00 "
                                f"(reason={early65_release_status_label}, "
                                f"target_side={early65_target_side}, "
                                f"target_diff={int(early65_target_diff or 0)})"
                            )
                    finally:
                        _release_signal_send_slot(check_uniq_url)
                    return return_status
                if current_game_time < NETWORTH_GATE_HARD_BLOCK_SECONDS:
                    print(
                        f"   ⏳ Ожидание dispatch: {NETWORTH_STATUS_PRE4_BLOCK} "
                        f"(now={_format_game_clock(current_game_time)}, "
                        f"target_side={target_side}, target_diff={int(target_networth_diff)})"
                    )
                    return return_status
                if current_game_time < NETWORTH_GATE_EARLY_WINDOW_END_SECONDS:
                    if isinstance(opposite_signs_early90_monitor, dict) and opposite_signs_early90_monitor.get("enabled"):
                        opposite_signs_4_to_10_threshold = float(
                            opposite_signs_early90_monitor.get("threshold_4_to_10") or 0.0
                        )
                        if target_networth_diff < opposite_signs_4_to_10_threshold:
                            print(
                                "   ⏳ Ожидание dispatch: opposite_signs_wr90_gate_04_10 "
                                f"(target_side={target_side}, target_diff={int(target_networth_diff)}, "
                                f"need>={int(opposite_signs_4_to_10_threshold)}) -> "
                                f"delayed monitor >={int(opposite_signs_4_to_10_threshold)} until 10:00, "
                                f"then >={int(float(opposite_signs_early90_monitor.get('threshold_10_to_20') or 0.0))} "
                                f"until {_format_game_clock(DELAYED_SIGNAL_TARGET_GAME_TIME)}"
                            )
                        else:
                            networth_send_status_label = NETWORTH_STATUS_LATE_CONFLICT_WAIT_2000
                    else:
                        if target_networth_diff < NETWORTH_GATE_4_TO_10_MIN_DIFF:
                            print(
                                "   ⏳ Ожидание dispatch: networth_gate_04_10 "
                                f"(target_side={target_side}, target_diff={int(target_networth_diff)}, "
                                f"need>={int(NETWORTH_GATE_4_TO_10_MIN_DIFF)})"
                            )
                            return return_status
                        if send_now_immediate and not queue_early_core_monitor:
                            networth_send_status_label = NETWORTH_STATUS_4_10_SEND_800
                        elif not early_core_same_sign_support:
                            early_core_metrics = ",".join(
                                str(m) for m in (early_core_same_or_zero_diag.get("nonzero_metrics") or [])
                            ) or "none"
                            early_core_conflicts = ",".join(
                                str(m) for m in (early_core_same_or_zero_diag.get("conflicting_metrics") or [])
                            ) or "none"
                            print(
                                "   ⏳ Ожидание dispatch: networth_gate_04_10_no_early_core_same_sign "
                                f"(target_side={target_side}, target_diff={int(target_networth_diff)}, "
                                f"early_core_metrics={early_core_metrics}, "
                                f"early_core_conflicts={early_core_conflicts}) -> "
                                f"delayed monitor >={int(NETWORTH_GATE_LATE_NO_EARLY_DIFF)} "
                                f"until {_format_game_clock(DELAYED_SIGNAL_TARGET_GAME_TIME)}"
                            )
                        else:
                            networth_send_status_label = NETWORTH_STATUS_4_10_SEND_800
                elif send_now_early_star_late_core_same_sign:
                    if early_star_gate_wr_pct >= EARLY_STAR_LATE_CORE_HIGH_CONFIDENCE_WR:
                        if target_networth_diff < NETWORTH_GATE_10_MIN_MAX_LOSS:
                            queue_early_core_monitor = True
                            early_core_monitor_threshold = float(NETWORTH_GATE_EARLY_CORE_MONITOR_DIFF)
                            early_core_monitor_wait_status_label = NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_1500
                            early_core_monitor_delay_reason = "early_star_late_core_wait_1500"
                            print(
                                "   ⏳ Ожидание dispatch: networth_gate_10plus_loss_le1500 "
                                f"(early_wr={early_star_gate_wr_pct:.1f}%, "
                                f"target_side={target_side}, target_diff={int(target_networth_diff)}, "
                                f"need>={int(NETWORTH_GATE_10_MIN_MAX_LOSS)}) -> delayed monitor "
                                f">={int(NETWORTH_GATE_EARLY_CORE_MONITOR_DIFF)} until "
                                f"{_format_game_clock(DELAYED_SIGNAL_TARGET_GAME_TIME)}"
                            )
                        else:
                            networth_send_status_label = NETWORTH_STATUS_MIN10_LOSS_LE1500_SEND
                    else:
                        if target_networth_diff < NETWORTH_GATE_EARLY_CORE_LOW_WR_MIN_LEAD:
                            queue_early_core_monitor = True
                            early_core_monitor_threshold = float(NETWORTH_GATE_EARLY_CORE_LOW_WR_MIN_LEAD)
                            early_core_monitor_wait_status_label = NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_800
                            early_core_monitor_delay_reason = "early_star_late_core_low_wr_wait_800"
                            print(
                                "   ⏳ Ожидание dispatch: networth_gate_10plus_wr60_70_need_lead800 "
                                f"(early_wr={early_star_gate_wr_pct:.1f}%, "
                                f"target_side={target_side}, target_diff={int(target_networth_diff)}, "
                                f"need>={int(NETWORTH_GATE_EARLY_CORE_LOW_WR_MIN_LEAD)}) -> delayed monitor "
                                f">={int(NETWORTH_GATE_EARLY_CORE_LOW_WR_MIN_LEAD)} until "
                                f"{_format_game_clock(DELAYED_SIGNAL_TARGET_GAME_TIME)}"
                            )
                        else:
                            networth_send_status_label = NETWORTH_STATUS_MIN10_LEAD_GE800_SEND
                elif send_now_late_star_early_core_same_sign:
                    if target_networth_diff < NETWORTH_GATE_4_TO_10_MIN_DIFF:
                        queue_late_core_monitor = True
                        print(
                            "   ⏳ Ожидание dispatch: late_star_early_core_need_lead800 "
                            f"(target_side={target_side}, target_diff={int(target_networth_diff)}, "
                            f"need>={int(NETWORTH_GATE_4_TO_10_MIN_DIFF)}) -> delayed monitor "
                            f">={int(NETWORTH_GATE_4_TO_10_MIN_DIFF)} until "
                            f"{_format_game_clock(DELAYED_SIGNAL_TARGET_GAME_TIME)}"
                        )
                    else:
                        networth_send_status_label = NETWORTH_STATUS_MIN10_LEAD_GE800_SEND
                elif send_now_full_star:
                    if target_networth_diff < NETWORTH_GATE_STRONG_SAME_SIGN_MAX_LOSS:
                        queue_strong_same_sign_monitor = True
                        print(
                            "   ⏳ Ожидание dispatch: strong_same_sign_loss_le800 "
                            f"(target_side={target_side}, target_diff={int(target_networth_diff)}, "
                            f"need>={int(NETWORTH_GATE_STRONG_SAME_SIGN_MAX_LOSS)}) -> delayed monitor "
                            f">={int(NETWORTH_GATE_STRONG_SAME_SIGN_MAX_LOSS)} until "
                            f"{_format_game_clock(DELAYED_SIGNAL_TARGET_GAME_TIME)} then comeback ceiling"
                        )
                    else:
                        networth_send_status_label = NETWORTH_STATUS_MIN10_LOSS_LE800_SEND
            if (
                not send_now_immediate
                or queue_early_core_monitor
                or queue_late_core_monitor
                or queue_strong_same_sign_monitor
                or queue_top25_late_elo_block_monitor
            ):
                delay_reason = "late_only_no_early_same_sign"
                if queue_early_core_monitor:
                    delay_reason = early_core_monitor_delay_reason
                elif queue_late_core_monitor:
                    delay_reason = "late_star_early_core_wait_800"
                elif queue_strong_same_sign_monitor:
                    delay_reason = "strong_same_sign_wait_800_then_comeback_ceiling"
                elif queue_top25_late_elo_block_monitor:
                    delay_reason = "late_top25_elo_block_opposite_monitor"
                elif has_selected_early_star and selected_early_sign != selected_late_sign:
                    delay_reason = "late_only_opposite_signs"
                elif not has_selected_early_star:
                    delay_reason = "late_only_no_early_star_wait_1500"
                if verbose_match_log:
                    _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
                _ensure_delayed_sender_started()
                target_game_time = float(DELAYED_SIGNAL_TARGET_GAME_TIME)
                target_human = _format_game_clock(target_game_time)
                monitor_threshold: Optional[float] = None
                monitor_wait_status_label: Optional[str] = None
                fallback_send_status_label = NETWORTH_STATUS_LATE_FALLBACK_20_20_SEND
                allow_live_recheck = False
                dynamic_monitor_profile: Optional[Dict[str, Any]] = None
                if queue_early_core_monitor:
                    monitor_threshold = early_core_monitor_threshold
                    monitor_wait_status_label = early_core_monitor_wait_status_label
                    fallback_send_status_label = NETWORTH_STATUS_EARLY_CORE_FALLBACK_20_20_SEND
                    allow_live_recheck = True
                elif queue_late_core_monitor:
                    monitor_threshold = NETWORTH_GATE_4_TO_10_MIN_DIFF
                    monitor_wait_status_label = NETWORTH_STATUS_LATE_CORE_MONITOR_WAIT_800
                    allow_live_recheck = True
                elif queue_strong_same_sign_monitor:
                    monitor_threshold = NETWORTH_GATE_STRONG_SAME_SIGN_MAX_LOSS
                    monitor_wait_status_label = NETWORTH_STATUS_STRONG_SAME_SIGN_MONITOR_WAIT_800
                    fallback_send_status_label = NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT
                elif queue_top25_late_elo_block_monitor:
                    dynamic_monitor_profile = dict(top25_late_elo_block_override or {})
                    allow_live_recheck = True
                    fallback_send_status_label = NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TARGET_LEAD_SEND
                    monitor_wait_status_label = NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_WAIT
                    if current_game_time >= NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_WINDOW_START_SECONDS:
                        monitor_threshold = NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_DIFF
                        print(
                            "   ⏳ Top25 late ELO-block monitor (17-20): "
                            f"target_side={target_side}, "
                            f"target_diff={int(target_networth_diff or 0)}, "
                            f"need>={int(NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_DIFF)} until {target_human}, "
                            "then send only if target_side still leads at 20:20"
                        )
                    else:
                        print(
                            "   ⏳ Top25 late ELO-block monitor: "
                            f"target_side={target_side}, "
                            f"target_diff={int(target_networth_diff or 0)}, "
                            f"wait until {_format_game_clock(NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_WINDOW_START_SECONDS)}, "
                            f"then need>={int(NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_DIFF)} until {target_human}, "
                            "then send only if target_side leads at 20:20"
                        )
                elif not has_selected_early_star and has_selected_late_star:
                    monitor_threshold = NETWORTH_GATE_LATE_NO_EARLY_DIFF
                    monitor_wait_status_label = NETWORTH_STATUS_LATE_MONITOR_WAIT_1500
                elif has_selected_early_star and has_selected_late_star and selected_early_sign != selected_late_sign:
                    if isinstance(opposite_signs_early90_monitor, dict) and opposite_signs_early90_monitor.get("enabled"):
                        dynamic_monitor_profile = dict(opposite_signs_early90_monitor)
                        if current_game_time < NETWORTH_GATE_EARLY_WINDOW_END_SECONDS:
                            monitor_threshold = float(dynamic_monitor_profile.get("threshold_4_to_10") or 0.0)
                            monitor_wait_status_label = str(dynamic_monitor_profile.get("status_4_to_10") or "")
                            print(
                                "   ⏳ Opposite-sign WR90 monitor (4-10): "
                                f"target_side={target_side}, target_diff={int(target_networth_diff)}, "
                                f"need>={int(monitor_threshold)} until 10:00, "
                                f"then >={int(float(dynamic_monitor_profile.get('threshold_10_to_20') or 0.0))} "
                                f"until {target_human}"
                            )
                        else:
                            monitor_threshold = float(dynamic_monitor_profile.get("threshold_10_to_20") or 0.0)
                            monitor_wait_status_label = str(dynamic_monitor_profile.get("status_10_to_20") or "")
                            print(
                                "   ⏳ Opposite-sign WR90 monitor (10-20): "
                                f"target_side={target_side}, target_diff={int(target_networth_diff)}, "
                                f"need>={int(monitor_threshold)} until {target_human}"
                            )
                    elif opposite_sign_early_release_allowed:
                        monitor_threshold = NETWORTH_GATE_LATE_OPPOSITE_DIFF
                        monitor_wait_status_label = NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000
                    else:
                        early_wr_label = (
                            f"{early_wr_pct:.1f}%"
                            if early_wr_pct is not None
                            else "n/a"
                        )
                        print(
                            "   ⏳ Opposite-sign early release disabled: "
                            f"early_wr={early_wr_label} -> wait until {target_human}"
                        )
                fallback_max_deficit_abs = _fallback_max_deficit_abs_for_delay_reason(
                    delay_reason,
                    monitor_threshold=monitor_threshold,
                )
                release_4_10_now = bool(
                    (not force_odds_signal_test_active)
                    and target_networth_diff is not None
                    and NETWORTH_GATE_HARD_BLOCK_SECONDS <= current_game_time < NETWORTH_GATE_EARLY_WINDOW_END_SECONDS
                    and (
                        (
                            isinstance(dynamic_monitor_profile, dict)
                            and dynamic_monitor_profile.get("enabled")
                            and monitor_threshold is not None
                            and target_networth_diff >= monitor_threshold
                        )
                        or (
                            not (isinstance(dynamic_monitor_profile, dict) and dynamic_monitor_profile.get("enabled"))
                            and
                            target_networth_diff >= NETWORTH_GATE_4_TO_10_MIN_DIFF
                            and early_core_same_sign_support
                        )
                    )
                )
                monitor_ready_now = bool(
                    (not force_odds_signal_test_active)
                    and monitor_threshold is not None
                    and target_networth_diff is not None
                    and current_game_time >= NETWORTH_GATE_EARLY_WINDOW_END_SECONDS
                    and current_game_time < target_game_time
                    and target_networth_diff >= monitor_threshold
                )
                existing_monitor_hold_started = None
                if isinstance(delayed_payload, dict):
                    try:
                        raw_hold_started = delayed_payload.get("networth_monitor_hold_started_game_time")
                        existing_monitor_hold_started = (
                            float(raw_hold_started) if raw_hold_started is not None else None
                        )
                    except (TypeError, ValueError):
                        existing_monitor_hold_started = None
                if release_4_10_now or monitor_ready_now:
                    release_reason = (
                        (
                            f"networth_monitor_{int(monitor_threshold or 0)}"
                            if isinstance(dynamic_monitor_profile, dict) and dynamic_monitor_profile.get("enabled")
                            else NETWORTH_STATUS_4_10_SEND_800
                        )
                        if release_4_10_now
                        else (
                            f"networth_monitor_{int(monitor_threshold or 0)}"
                            if monitor_threshold is not None
                            else "networth_monitor_unknown"
                        )
                    )
                    release_status_label = (
                        (
                            monitor_wait_status_label or NETWORTH_STATUS_4_10_SEND_800
                        )
                        if release_4_10_now and isinstance(dynamic_monitor_profile, dict) and dynamic_monitor_profile.get("enabled")
                        else (
                            NETWORTH_STATUS_4_10_SEND_800
                            if release_4_10_now
                            else (monitor_wait_status_label or "unknown_monitor_status")
                        )
                    )
                    release_threshold = (
                        float(monitor_threshold)
                        if monitor_ready_now and monitor_threshold is not None
                        else (
                            float(monitor_threshold)
                            if release_4_10_now
                            and isinstance(dynamic_monitor_profile, dict)
                            and dynamic_monitor_profile.get("enabled")
                            and monitor_threshold is not None
                            else float(NETWORTH_GATE_4_TO_10_MIN_DIFF)
                        )
                    )
                    hold_check = _networth_monitor_hold_check(
                        current_game_time=current_game_time,
                        target_networth_diff=target_networth_diff,
                        monitor_threshold=release_threshold,
                        hold_started_game_time=existing_monitor_hold_started,
                        hold_seconds=NETWORTH_MONITOR_HOLD_SECONDS,
                    )
                    if hold_check.get("enabled") and not hold_check.get("ready"):
                        monitor_threshold = release_threshold
                        monitor_wait_status_label = release_status_label
                        print(
                            "   ⏳ Networth hold started: "
                            f"(target_side={target_side}, "
                            f"target_diff={int(target_networth_diff or 0)}, "
                            f"need>={int(release_threshold)}, "
                            f"hold={int(hold_check.get('hold_seconds') or 0)}s)"
                        )
                    else:
                        if _skip_dispatch_for_processed_url(check_uniq_url, "немедленной отправки (networth_gate)"):
                            return return_status
                        if not _acquire_signal_send_slot(check_uniq_url):
                            print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                            return return_status
                        try:
                            if _skip_dispatch_for_processed_url(check_uniq_url, "немедленной отправки после lock (networth_gate)"):
                                return return_status
                            if verbose_match_log:
                                _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
                            delivery_confirmed = _deliver_and_persist_signal(
                                check_uniq_url,
                                message_text,
                                add_url_reason="star_signal_sent_now_networth_gate",
                                add_url_details={
                                    "status": status,
                                    "dispatch_mode": dispatch_mode,
                                    "delay_reason": delay_reason,
                                    "release_reason": release_reason,
                                    "dispatch_status_label": release_status_label,
                                    "game_time": int(current_game_time),
                                    "target_side": target_side,
                                    "target_networth_diff": float(target_networth_diff or 0.0),
                                    "networth_monitor_threshold": float(release_threshold),
                                    "networth_monitor_hold_seconds": float(hold_check.get("hold_seconds") or 0.0),
                                    "networth_monitor_hold_started_game_time": float(hold_check.get("hold_started_game_time") or 0.0),
                                    "networth_monitor_hold_elapsed_seconds": float(hold_check.get("held_seconds") or 0.0),
                                    "json_retry_errors": json_retry_errors,
                                },
                                bookmaker_decision="sent",
                            )
                            if delivery_confirmed:
                                print(
                                    f"   ✅ ВЕРДИКТ: Сигнал отправлен раньше {target_human} "
                                    f"(reason={release_reason}, status={release_status_label}, target_side={target_side}, "
                                    f"target_diff={int(target_networth_diff or 0)})"
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
                        if verbose_match_log:
                            _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
                        delivery_confirmed = _deliver_and_persist_signal(
                            check_uniq_url,
                            message_text,
                            add_url_reason="star_signal_sent_now_no_json_url",
                            add_url_details={
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": delay_reason,
                                "json_retry_errors": json_retry_errors,
                            },
                            bookmaker_decision="sent",
                        )
                        if delivery_confirmed:
                            print(f"   ✅ ВЕРДИКТ: Сигнал отправлен немедленно (нет json_url для delayed)")
                    finally:
                        _release_signal_send_slot(check_uniq_url)
                    return return_status
                if current_game_time >= target_game_time:
                    if queue_top25_late_elo_block_monitor:
                        if target_networth_diff is not None and target_networth_diff > 0:
                            if _skip_dispatch_for_processed_url(check_uniq_url, f"немедленной отправки (top25 late elo block {target_human})"):
                                return return_status
                            if not _acquire_signal_send_slot(check_uniq_url):
                                print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                                return return_status
                            try:
                                if _skip_dispatch_for_processed_url(check_uniq_url, f"немедленной отправки после lock (top25 late elo block {target_human})"):
                                    return return_status
                                if verbose_match_log:
                                    _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
                                delivery_confirmed = _deliver_and_persist_signal(
                                    check_uniq_url,
                                    message_text,
                                    add_url_reason="star_signal_sent_now_top25_late_elo_block_target_lead",
                                    add_url_details={
                                        "status": status,
                                        "dispatch_mode": dispatch_mode,
                                        "delay_reason": delay_reason,
                                        "dispatch_status_label": NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TARGET_LEAD_SEND,
                                        "game_time": int(current_game_time),
                                        "target_game_time": int(target_game_time),
                                        "target_side": target_side,
                                        "target_networth_diff": float(target_networth_diff or 0.0),
                                        "top25_late_elo_block_rank": int(top25_late_elo_block_override.get("leaderboard_rank") or 0),
                                        "json_retry_errors": json_retry_errors,
                                    },
                                    bookmaker_decision="sent",
                                )
                                if delivery_confirmed:
                                    print(
                                        "   ✅ ВЕРДИКТ: Сигнал отправлен по top25 late ELO-block target lead "
                                        f"(target_side={target_side}, target_diff={int(target_networth_diff or 0)})"
                                    )
                            finally:
                                _release_signal_send_slot(check_uniq_url)
                            return return_status
                        print(
                            f"   ⚠️ ВЕРДИКТ: ОТКАЗ (top25 late ELO-block monitor не дал lead к {target_human}) "
                            f"- матч пропущен"
                        )
                        add_url(
                            check_uniq_url,
                            reason="star_signal_rejected_top25_late_elo_block_timeout",
                            details={
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": delay_reason,
                                "dispatch_status_label": NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TIMEOUT_NO_SEND,
                                "game_time": int(current_game_time),
                                "target_game_time": int(target_game_time),
                                "target_side": target_side,
                                "target_networth_diff": float(target_networth_diff or 0.0),
                                "top25_late_elo_block_rank": int(top25_late_elo_block_override.get("leaderboard_rank") or 0),
                                "json_retry_errors": json_retry_errors,
                            },
                        )
                        return return_status
                    if queue_early_core_monitor:
                        print(
                            f"   ⚠️ ВЕРДИКТ: ОТКАЗ (early star без late star не добрал >=1500 до {target_human}) "
                            f"- матч пропущен"
                        )
                        print(f"   📉 Star checks: {' | '.join(star_diag_lines)}")
                        add_url(
                            check_uniq_url,
                            reason="star_signal_rejected_early_core_monitor_timeout",
                            details={
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": delay_reason,
                                "dispatch_status_label": NETWORTH_STATUS_EARLY_CORE_TIMEOUT_NO_SEND,
                                "game_time": int(current_game_time),
                                "target_game_time": int(target_game_time),
                                "target_side": target_side,
                                "target_networth_diff": float(target_networth_diff or 0.0),
                                "selected_star_wr": selected_star_wr,
                                "selected_star_mode": selected_star_mode,
                                "selected_early_star": bool(has_selected_early_star),
                                "selected_late_star": bool(has_selected_late_star),
                                "selected_early_sign": selected_early_sign,
                                "selected_late_sign": selected_late_sign,
                                "late_core_same_sign_diag": late_core_same_sign_diag,
                                "json_retry_errors": json_retry_errors,
                            },
                        )
                        print("   ✅ map_id_check.txt обновлен: add_url после early-core timeout")
                        return return_status
                    if queue_late_core_monitor and not late_comeback_monitor_candidate:
                        print(
                            f"   ⚠️ ВЕРДИКТ: ОТКАЗ (late star без early star не добрал >={int(NETWORTH_GATE_4_TO_10_MIN_DIFF)} до {target_human}) "
                            f"- матч пропущен"
                        )
                        print(f"   📉 Star checks: {' | '.join(star_diag_lines)}")
                        add_url(
                            check_uniq_url,
                            reason="star_signal_rejected_late_core_monitor_timeout",
                            details={
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": delay_reason,
                                "dispatch_status_label": NETWORTH_STATUS_LATE_CORE_TIMEOUT_NO_SEND,
                                "game_time": int(current_game_time),
                                "target_game_time": int(target_game_time),
                                "target_side": target_side,
                                "target_networth_diff": float(target_networth_diff or 0.0),
                                "selected_star_wr": selected_star_wr,
                                "selected_star_mode": selected_star_mode,
                                "selected_early_star": bool(has_selected_early_star),
                                "selected_late_star": bool(has_selected_late_star),
                                "selected_early_sign": selected_early_sign,
                                "selected_late_sign": selected_late_sign,
                                "early_core_same_sign_diag": early_core_same_or_zero_diag,
                                "json_retry_errors": json_retry_errors,
                            },
                        )
                        print("   ✅ map_id_check.txt обновлен: add_url после late-core timeout")
                        return return_status
                    if (
                        queue_strong_same_sign_monitor
                        or (
                        late_comeback_monitor_candidate
                        and target_networth_diff is not None
                        and target_networth_diff <= -NETWORTH_GATE_LATE_COMEBACK_LARGE_DEFICIT
                        )
                    ):
                        late_comeback_check = _late_comeback_monitor_check(
                            game_time_seconds=current_game_time,
                            target_networth_diff=target_networth_diff,
                        )
                        if late_comeback_check.get("ready"):
                            print(
                                "   ✅ Late comeback ceiling reached at target time "
                                f"(minute={late_comeback_check.get('minute')}, "
                                f"ceiling={int(late_comeback_check.get('threshold') or 0)}, "
                                f"target_diff={int(target_networth_diff)})"
                            )
                            if _skip_dispatch_for_processed_url(check_uniq_url, f"немедленной отправки (late comeback ceiling {target_human})"):
                                return return_status
                            if not _acquire_signal_send_slot(check_uniq_url):
                                print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                                return return_status
                            try:
                                if _skip_dispatch_for_processed_url(check_uniq_url, f"немедленной отправки после lock (late comeback ceiling {target_human})"):
                                    return return_status
                                if verbose_match_log:
                                    _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
                                delivery_confirmed = _deliver_and_persist_signal(
                                    check_uniq_url,
                                    message_text,
                                    add_url_reason="star_signal_sent_now_late_comeback_ceiling",
                                    add_url_details={
                                        "status": status,
                                        "dispatch_mode": dispatch_mode,
                                        "delay_reason": delay_reason,
                                        "dispatch_status_label": NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT,
                                        "game_time": int(current_game_time),
                                        "target_game_time": int(target_game_time),
                                        "target_side": target_side,
                                        "target_networth_diff": float(target_networth_diff),
                                        "late_comeback_delta_pp": float(target_comeback_delta_pp or 0.0),
                                        "late_comeback_monitor_reached": True,
                                        "late_comeback_monitor_minute": late_comeback_check.get("minute"),
                                        "late_comeback_monitor_threshold": late_comeback_check.get("threshold"),
                                        "json_retry_errors": json_retry_errors,
                                    },
                                    bookmaker_decision="sent",
                                )
                                if delivery_confirmed:
                                    print(
                                        "   ✅ ВЕРДИКТ: Сигнал отправлен по late comeback ceiling "
                                        f"(minute={late_comeback_check.get('minute')}, "
                                        f"target_diff={int(target_networth_diff)})"
                                    )
                            finally:
                                _release_signal_send_slot(check_uniq_url)
                            return return_status
                        late_comeback_deadline = _late_comeback_monitor_deadline_seconds()
                        if late_comeback_deadline is not None and current_game_time < late_comeback_deadline:
                            comeback_delay_reason = (
                                "strong_same_sign_comeback_ceiling_monitor"
                                if queue_strong_same_sign_monitor
                                else "late_star_comeback_ceiling_monitor"
                            )
                            delayed_add_url_details = {
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": comeback_delay_reason,
                                "dispatch_status_label": NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT,
                                "queued_game_time": int(current_game_time),
                                "target_game_time": int(late_comeback_deadline),
                                "json_retry_errors": json_retry_errors,
                                "networth_target_side": target_side,
                                "target_networth_diff": float(target_networth_diff),
                                "late_comeback_monitor_minute": late_comeback_check.get("minute"),
                                "late_comeback_monitor_threshold": late_comeback_check.get("threshold"),
                            }
                            if target_comeback_delta_pp is not None:
                                delayed_add_url_details["late_comeback_delta_pp"] = float(target_comeback_delta_pp or 0.0)
                            delayed_payload = {
                                "message": message_text,
                                "reason": comeback_delay_reason,
                                "star_metrics_snapshot": star_metrics_snapshot,
                                "json_url": json_url,
                                "target_game_time": float(late_comeback_deadline),
                                "queued_at": time.time(),
                                "queued_game_time": current_game_time,
                                "last_game_time": current_game_time,
                                "last_progress_at": time.time(),
                                "dispatch_status_label": NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT,
                                "add_url_reason": "star_signal_sent_delayed",
                                "add_url_details": delayed_add_url_details,
                                "fallback_send_status_label": NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND,
                                "send_on_target_game_time": False,
                                "allow_live_recheck": False,
                                "retry_attempt_count": 0,
                                "next_retry_at": 0.0,
                                "late_comeback_monitor_candidate": bool(late_comeback_monitor_candidate),
                                "late_comeback_monitor_active": True,
                                "late_comeback_monitor_deadline_game_time": float(late_comeback_deadline),
                                "networth_target_side": target_side,
                                "late_comeback_force_after_target": False,
                            }
                            if target_comeback_delta_pp is not None:
                                delayed_payload["late_comeback_delta_pp"] = float(target_comeback_delta_pp or 0.0)
                            _set_delayed_match(check_uniq_url, delayed_payload)
                            comeback_delta_log = (
                                f"comeback_delta={float(target_comeback_delta_pp or 0.0):+.2f} pp, "
                                if target_comeback_delta_pp is not None
                                else ""
                            )
                            print(
                                "   ⏳ Late comeback monitor включен: "
                                f"target_side={target_side}, "
                                f"target_diff={int(target_networth_diff)}, "
                                f"{comeback_delta_log}"
                                f"minute={late_comeback_check.get('minute')}, "
                                f"ceiling={int(late_comeback_check.get('threshold') or 0)}, "
                                f"deadline={_format_game_clock(late_comeback_deadline)}"
                            )
                            print("   ✅ ВЕРДИКТ: Сигнал оставлен в delayed-очереди для late comeback monitor")
                            return return_status
                        print(
                            f"   ⚠️ ВЕРДИКТ: ОТКАЗ (не прошел comeback ceiling после {target_human}) "
                            f"- матч пропущен"
                        )
                        add_url(
                            check_uniq_url,
                            reason="star_signal_rejected_late_comeback_monitor_timeout",
                            details={
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": (
                                    "strong_same_sign_comeback_ceiling_monitor"
                                    if queue_strong_same_sign_monitor
                                    else "late_star_comeback_ceiling_monitor"
                                ),
                                "dispatch_status_label": NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND,
                                "game_time": int(current_game_time),
                                "target_game_time": int(target_game_time),
                                "target_side": target_side,
                                "target_networth_diff": float(target_networth_diff or 0.0),
                                "json_retry_errors": json_retry_errors,
                            },
                        )
                        return return_status
                    post_target_comeback = _post_target_comeback_ceiling_decision(
                        game_time_seconds=current_game_time,
                        target_networth_diff=target_networth_diff,
                    )
                    if has_selected_late_star and post_target_comeback.get("available"):
                        if post_target_comeback.get("ready"):
                            print(
                                "   ✅ Post-target comeback ceiling reached "
                                f"(minute={post_target_comeback.get('minute')}, "
                                f"ceiling={int(post_target_comeback.get('threshold') or 0)}, "
                                f"target_diff={int(target_networth_diff or 0)})"
                            )
                            if _skip_dispatch_for_processed_url(check_uniq_url, f"немедленной отправки (post-target comeback ceiling {target_human})"):
                                return return_status
                            if not _acquire_signal_send_slot(check_uniq_url):
                                print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                                return return_status
                            try:
                                if _skip_dispatch_for_processed_url(check_uniq_url, f"немедленной отправки после lock (post-target comeback ceiling {target_human})"):
                                    return return_status
                                if verbose_match_log:
                                    _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
                                delivery_confirmed = _deliver_and_persist_signal(
                                    check_uniq_url,
                                    message_text,
                                    add_url_reason="star_signal_sent_now_late_comeback_ceiling",
                                    add_url_details={
                                        "status": status,
                                        "dispatch_mode": dispatch_mode,
                                        "delay_reason": "post_target_comeback_ceiling_monitor",
                                        "dispatch_status_label": NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT,
                                        "game_time": int(current_game_time),
                                        "target_game_time": int(target_game_time),
                                        "target_side": target_side,
                                        "target_networth_diff": float(target_networth_diff or 0.0),
                                        "late_comeback_monitor_reached": True,
                                        "late_comeback_monitor_minute": post_target_comeback.get("minute"),
                                        "late_comeback_monitor_threshold": post_target_comeback.get("threshold"),
                                        "json_retry_errors": json_retry_errors,
                                    },
                                    bookmaker_decision="sent",
                                )
                                if delivery_confirmed:
                                    print(
                                        "   ✅ ВЕРДИКТ: Сигнал отправлен по post-target comeback ceiling "
                                        f"(minute={post_target_comeback.get('minute')}, "
                                        f"target_diff={int(target_networth_diff or 0)})"
                                    )
                            finally:
                                _release_signal_send_slot(check_uniq_url)
                            return return_status
                        if post_target_comeback.get("should_monitor"):
                            post_target_comeback_deadline = post_target_comeback.get("deadline_game_time")
                            delayed_add_url_details = {
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": "post_target_comeback_ceiling_monitor",
                                "dispatch_status_label": NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT,
                                "queued_game_time": int(current_game_time),
                                "target_game_time": int(post_target_comeback_deadline or target_game_time),
                                "json_retry_errors": json_retry_errors,
                                "networth_target_side": target_side,
                                "target_networth_diff": float(target_networth_diff or 0.0),
                                "late_comeback_monitor_minute": post_target_comeback.get("minute"),
                                "late_comeback_monitor_threshold": post_target_comeback.get("threshold"),
                            }
                            delayed_payload = {
                                "message": message_text,
                                "reason": "post_target_comeback_ceiling_monitor",
                                "star_metrics_snapshot": star_metrics_snapshot,
                                "json_url": json_url,
                                "target_game_time": float(post_target_comeback_deadline or target_game_time),
                                "queued_at": time.time(),
                                "queued_game_time": current_game_time,
                                "last_game_time": current_game_time,
                                "last_progress_at": time.time(),
                                "dispatch_status_label": NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT,
                                "add_url_reason": "star_signal_sent_delayed",
                                "add_url_details": delayed_add_url_details,
                                "fallback_send_status_label": NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND,
                                "send_on_target_game_time": False,
                                "allow_live_recheck": False,
                                "retry_attempt_count": 0,
                                "next_retry_at": 0.0,
                                "late_comeback_monitor_active": True,
                                "late_comeback_monitor_deadline_game_time": float(post_target_comeback_deadline or target_game_time),
                                "networth_target_side": target_side,
                                "late_comeback_force_after_target": False,
                            }
                            _set_delayed_match(check_uniq_url, delayed_payload)
                            print(
                                "   ⏳ Post-target comeback monitor включен: "
                                f"target_side={target_side}, "
                                f"target_diff={int(target_networth_diff or 0)}, "
                                f"minute={post_target_comeback.get('minute')}, "
                                f"ceiling={int(post_target_comeback.get('threshold') or 0)}, "
                                f"deadline={_format_game_clock(post_target_comeback_deadline)}"
                            )
                            print("   ✅ ВЕРДИКТ: Сигнал оставлен в delayed-очереди для post-target comeback monitor")
                            return return_status
                        if post_target_comeback.get("should_timeout"):
                            print(
                                f"   ⚠️ ВЕРДИКТ: ОТКАЗ (не прошел post-target comeback ceiling после {target_human}) "
                                f"- матч пропущен"
                            )
                            add_url(
                                check_uniq_url,
                                reason="star_signal_rejected_late_comeback_monitor_timeout",
                                details={
                                    "status": status,
                                    "dispatch_mode": dispatch_mode,
                                    "delay_reason": "post_target_comeback_ceiling_monitor",
                                    "dispatch_status_label": NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND,
                                    "game_time": int(current_game_time),
                                    "target_game_time": int(target_game_time),
                                    "target_side": target_side,
                                    "target_networth_diff": float(target_networth_diff or 0.0),
                                    "late_comeback_monitor_minute": post_target_comeback.get("minute"),
                                    "late_comeback_monitor_threshold": post_target_comeback.get("threshold"),
                                    "json_retry_errors": json_retry_errors,
                                },
                            )
                            return return_status
                    fallback_guard = _fallback_networth_deficit_guard_decision(
                        target_networth_diff=target_networth_diff,
                        max_deficit_abs=fallback_max_deficit_abs,
                    )
                    if bool(fallback_guard.get("reject")):
                        print(
                            f"   ⚠️ ВЕРДИКТ: ОТКАЗ (fallback networth guard after {target_human}) "
                            f"- матч пропущен"
                        )
                        add_url(
                            check_uniq_url,
                            reason="star_signal_rejected_fallback_networth_guard",
                            details={
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": delay_reason,
                                "dispatch_status_label": NETWORTH_STATUS_LATE_FALLBACK_20_20_DEFICIT_NO_SEND,
                                "game_time": int(current_game_time),
                                "target_game_time": int(target_game_time),
                                "target_side": target_side,
                                "target_networth_diff": float(fallback_guard.get("target_diff") or 0.0),
                                "fallback_max_deficit_abs": float(fallback_guard.get("threshold_abs") or 0.0),
                                "json_retry_errors": json_retry_errors,
                            },
                        )
                        return return_status
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
                        if verbose_match_log:
                            _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
                        delivery_confirmed = _deliver_and_persist_signal(
                            check_uniq_url,
                            message_text,
                            add_url_reason="star_signal_sent_now_target_reached",
                            add_url_details={
                                "status": status,
                                "dispatch_mode": dispatch_mode,
                                "delay_reason": delay_reason,
                                "game_time": int(current_game_time),
                                "target_game_time": int(target_game_time),
                                "json_retry_errors": json_retry_errors,
                            },
                            bookmaker_decision="sent",
                        )
                        if delivery_confirmed:
                            print(f"   ✅ ВЕРДИКТ: Сигнал отправлен немедленно (game_time >= {target_human})")
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
                if early_wr_pct is not None:
                    delayed_add_url_details["early_wr_pct"] = float(early_wr_pct)
                if late_wr_pct is not None:
                    delayed_add_url_details["late_wr_pct"] = float(late_wr_pct)
                if queue_top25_late_elo_block_monitor:
                    delayed_add_url_details["networth_target_side"] = target_side
                    delayed_add_url_details["target_side"] = target_side
                    delayed_add_url_details["top25_late_elo_block_rank"] = int(top25_late_elo_block_override.get("leaderboard_rank") or 0)
                    delayed_add_url_details["top25_late_elo_block_adj_wr"] = top25_late_elo_block_override.get("elo_target_wr")
                    if target_networth_diff is not None:
                        delayed_add_url_details["target_networth_diff"] = float(target_networth_diff)
                if monitor_threshold is not None:
                    delayed_add_url_details["networth_monitor_threshold"] = float(monitor_threshold)
                    delayed_add_url_details["networth_monitor_deadline_game_time"] = int(target_game_time)
                    delayed_add_url_details["networth_target_side"] = target_side
                    delayed_add_url_details["networth_monitor_hold_seconds"] = float(NETWORTH_MONITOR_HOLD_SECONDS)
                    if target_networth_diff is not None:
                        delayed_add_url_details["target_networth_diff"] = float(target_networth_diff)
                if fallback_max_deficit_abs is not None:
                    delayed_add_url_details["fallback_max_deficit_abs"] = float(fallback_max_deficit_abs)
                    if target_side is not None:
                        delayed_add_url_details["networth_target_side"] = target_side
                    if target_networth_diff is not None:
                        delayed_add_url_details["target_networth_diff"] = float(target_networth_diff)
                if isinstance(dynamic_monitor_profile, dict) and dynamic_monitor_profile.get("enabled"):
                    delayed_add_url_details["dynamic_monitor_profile"] = str(dynamic_monitor_profile.get("profile") or "")
                    if dynamic_monitor_profile.get("profile") == "late_top25_elo_block_opposite_monitor":
                        delayed_add_url_details["networth_monitor_threshold_17_to_20"] = float(dynamic_monitor_profile.get("window_threshold") or 0.0)
                        delayed_add_url_details["networth_monitor_status_17_to_20"] = NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_WAIT
                        delayed_add_url_details["networth_monitor_window_start_game_time"] = int(dynamic_monitor_profile.get("window_start_seconds") or 0.0)
                        delayed_add_url_details["top25_late_elo_block_rank"] = int(dynamic_monitor_profile.get("leaderboard_rank") or 0)
                        delayed_add_url_details["top25_late_elo_block_adj_wr"] = dynamic_monitor_profile.get("elo_target_wr")
                    else:
                        delayed_add_url_details["networth_monitor_threshold_4_to_10"] = float(dynamic_monitor_profile.get("threshold_4_to_10") or 0.0)
                        delayed_add_url_details["networth_monitor_threshold_10_to_20"] = float(dynamic_monitor_profile.get("threshold_10_to_20") or 0.0)
                        delayed_add_url_details["networth_monitor_status_4_to_10"] = str(dynamic_monitor_profile.get("status_4_to_10") or "")
                        delayed_add_url_details["networth_monitor_status_10_to_20"] = str(dynamic_monitor_profile.get("status_10_to_20") or "")
                        delayed_add_url_details["opposite_signs_early90_elo_gap_pp"] = dynamic_monitor_profile.get("elo_gap_pp")
                        delayed_add_url_details["opposite_signs_early90_early_elo_wr"] = dynamic_monitor_profile.get("early_elo_wr")
                        delayed_add_url_details["opposite_signs_early90_late_elo_wr"] = dynamic_monitor_profile.get("late_elo_wr")
                if late_comeback_monitor_candidate:
                    delayed_add_url_details["late_comeback_delta_pp"] = float(target_comeback_delta_pp or 0.0)
                delayed_payload = {
                    'message': message_text,
                    'reason': delay_reason,
                    'star_metrics_snapshot': star_metrics_snapshot,
                    'json_url': json_url,
                    'target_game_time': target_game_time,
                    'queued_at': queued_ts,
                    'queued_game_time': current_game_time,
                    'last_game_time': current_game_time,
                    'last_progress_at': queued_ts,
                    'dispatch_status_label': monitor_wait_status_label,
                    'add_url_reason': 'star_signal_sent_delayed',
                    'add_url_details': delayed_add_url_details,
                    'fallback_send_status_label': fallback_send_status_label,
                    'send_on_target_game_time': not (
                        queue_early_core_monitor
                        or queue_late_core_monitor
                        or queue_strong_same_sign_monitor
                        or queue_top25_late_elo_block_monitor
                    ),
                    'allow_live_recheck': allow_live_recheck,
                    'retry_attempt_count': 0,
                    'next_retry_at': 0.0,
                    'late_comeback_monitor_candidate': late_comeback_monitor_candidate,
                }
                if queue_top25_late_elo_block_monitor:
                    delayed_payload['networth_target_side'] = target_side
                    delayed_payload['top25_late_elo_block_rank'] = int(top25_late_elo_block_override.get("leaderboard_rank") or 0)
                    delayed_payload['top25_late_elo_block_adj_wr'] = top25_late_elo_block_override.get("elo_target_wr")
                if late_comeback_monitor_candidate:
                    delayed_payload['late_comeback_delta_pp'] = float(target_comeback_delta_pp or 0.0)
                if isinstance(dynamic_monitor_profile, dict) and dynamic_monitor_profile.get("enabled"):
                    delayed_payload['dynamic_monitor_profile'] = str(dynamic_monitor_profile.get("profile") or "")
                    if dynamic_monitor_profile.get("profile") == "late_top25_elo_block_opposite_monitor":
                        delayed_payload['networth_monitor_threshold_17_to_20'] = float(dynamic_monitor_profile.get("window_threshold") or 0.0)
                        delayed_payload['networth_monitor_status_17_to_20'] = NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_WAIT
                        delayed_payload['networth_monitor_window_start_game_time'] = float(dynamic_monitor_profile.get("window_start_seconds") or 0.0)
                        delayed_payload['top25_late_elo_block_rank'] = int(dynamic_monitor_profile.get("leaderboard_rank") or 0)
                        delayed_payload['top25_late_elo_block_adj_wr'] = dynamic_monitor_profile.get("elo_target_wr")
                    else:
                        delayed_payload['networth_monitor_threshold_4_to_10'] = float(dynamic_monitor_profile.get("threshold_4_to_10") or 0.0)
                        delayed_payload['networth_monitor_threshold_10_to_20'] = float(dynamic_monitor_profile.get("threshold_10_to_20") or 0.0)
                        delayed_payload['networth_monitor_status_4_to_10'] = str(dynamic_monitor_profile.get("status_4_to_10") or "")
                        delayed_payload['networth_monitor_status_10_to_20'] = str(dynamic_monitor_profile.get("status_10_to_20") or "")
                        delayed_payload['opposite_signs_early90_elo_gap_pp'] = dynamic_monitor_profile.get("elo_gap_pp")
                        delayed_payload['opposite_signs_early90_early_elo_wr'] = dynamic_monitor_profile.get("early_elo_wr")
                        delayed_payload['opposite_signs_early90_late_elo_wr'] = dynamic_monitor_profile.get("late_elo_wr")
                if monitor_threshold is not None:
                    delayed_payload['networth_monitor_threshold'] = float(monitor_threshold)
                    delayed_payload['networth_monitor_deadline_game_time'] = float(target_game_time)
                    delayed_payload['networth_target_side'] = target_side
                    delayed_payload['networth_monitor_hold_seconds'] = float(NETWORTH_MONITOR_HOLD_SECONDS)
                    hold_seed = _networth_monitor_hold_check(
                        current_game_time=current_game_time,
                        target_networth_diff=target_networth_diff,
                        monitor_threshold=monitor_threshold,
                        hold_started_game_time=existing_monitor_hold_started,
                        hold_seconds=NETWORTH_MONITOR_HOLD_SECONDS,
                    )
                    if hold_seed.get("enabled") and hold_seed.get("hold_started_game_time") is not None:
                        delayed_payload['networth_monitor_hold_started_game_time'] = float(
                            hold_seed.get("hold_started_game_time") or 0.0
                        )
                if fallback_max_deficit_abs is not None:
                    delayed_payload['fallback_max_deficit_abs'] = float(fallback_max_deficit_abs)
                    if target_side is not None:
                        delayed_payload['networth_target_side'] = target_side
                if queue_strong_same_sign_monitor:
                    delayed_payload['late_comeback_force_after_target'] = True
                    delayed_payload['late_comeback_monitor_deadline_game_time'] = float(
                        _late_comeback_monitor_deadline_seconds() or target_game_time
                    )
                if queue_early_core_monitor:
                    delayed_payload['timeout_add_url_reason'] = 'star_signal_rejected_early_core_monitor_timeout'
                    delayed_payload['timeout_status_label'] = NETWORTH_STATUS_EARLY_CORE_TIMEOUT_NO_SEND
                elif queue_late_core_monitor:
                    delayed_payload['timeout_add_url_reason'] = 'star_signal_rejected_late_core_monitor_timeout'
                    delayed_payload['timeout_status_label'] = NETWORTH_STATUS_LATE_CORE_TIMEOUT_NO_SEND
                elif queue_top25_late_elo_block_monitor:
                    delayed_payload['timeout_add_url_reason'] = 'star_signal_rejected_top25_late_elo_block_timeout'
                    delayed_payload['timeout_status_label'] = NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TIMEOUT_NO_SEND
                _set_delayed_match(check_uniq_url, delayed_payload)
                print(
                    f"   ⏱️ Сигнал в delayed-очереди до game_time={target_human} "
                    f"(reason={delay_reason}, now={int(current_game_time)}), "
                    f"ETA~{eta_human}: {check_uniq_url}"
                )
                if monitor_threshold is not None and target_side is not None:
                    monitor_desc = (
                        f"threshold={int(monitor_threshold)}"
                        if monitor_threshold is not None
                        else "threshold=n/a"
                    )
                    print(
                        "   🔎 Delayed monitoring включен: "
                        f"status={monitor_wait_status_label}, "
                        f"target_side={target_side}, "
                        f"target_diff={int(target_networth_diff or 0)}, "
                        f"{monitor_desc}, "
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
                if verbose_match_log:
                    _print_star_metrics_snapshot(star_metrics_snapshot, label="immediate")
                delivery_confirmed = _deliver_and_persist_signal(
                    check_uniq_url,
                    message_text,
                    add_url_reason="star_signal_sent_now",
                    add_url_details={
                        "status": status,
                        "dispatch_mode": dispatch_mode,
                        "dispatch_status_label": networth_send_status_label,
                        "selected_star_wr": selected_star_wr,
                        "selected_star_mode": selected_star_mode,
                        "json_retry_errors": json_retry_errors,
                    },
                    bookmaker_decision="sent",
                )
                if delivery_confirmed:
                    print("   ✅ ВЕРДИКТ: STAR-сигнал отправлен немедленно")
            finally:
                _release_signal_send_slot(check_uniq_url)
        else:
            tempo_over_fallback = _compute_tempo_over_fallback_payload(
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos,
                match_tier=star_match_tier,
            )
            tempo_over_diag = None
            if tempo_over_fallback is None:
                tempo_over_diag = _compute_tempo_over_fallback_diagnostics(
                    radiant_heroes_and_pos=radiant_heroes_and_pos,
                    dire_heroes_and_pos=dire_heroes_and_pos,
                    match_tier=star_match_tier,
                )
            if (
                tempo_over_fallback is None
                and int(star_match_tier or 0) == 1
                and isinstance(tempo_over_diag, dict)
            ):
                tempo_score = tempo_over_diag.get("score")
                tempo_threshold = tempo_over_diag.get("threshold")
                tempo_reason = str(tempo_over_diag.get("reason") or "unknown")
                if tempo_score is not None and tempo_threshold is not None:
                    print(
                        "   📈 Tempo fallback score="
                        f"{float(tempo_score):.4f} "
                        f"(threshold={float(tempo_threshold):.4f}, status={tempo_reason})"
                    )
                    tempo_indices = tempo_over_diag.get("indices") or {}
                    print(
                        "   📈 Tempo fallback indices: "
                        f"solo_kills_pm={tempo_indices.get('solo_kills_pm')}, "
                        f"synergy_duo_kills_pm={tempo_indices.get('synergy_duo_kills_pm')}, "
                        f"counterpick_1vs1_kills_pm={tempo_indices.get('counterpick_1vs1_kills_pm')}, "
                        f"counterpick_1vs1_deaths_pm={tempo_indices.get('counterpick_1vs1_deaths_pm')}"
                    )
                elif tempo_reason not in {"threshold_met"}:
                    print(f"   📈 Tempo fallback unavailable: status={tempo_reason}")
            if tempo_over_fallback is not None:
                tempo_team_elo_block = ""
                tempo_team_elo_summary = _build_team_elo_matchup_summary(
                    radiant_team_id=radiant_team_id,
                    dire_team_id=dire_team_id,
                    radiant_team_name=radiant_team_name_original,
                    dire_team_name=dire_team_name_original,
                    radiant_account_ids=radiant_account_ids,
                    dire_account_ids=dire_account_ids,
                    match_tier=star_match_tier,
                )
                tempo_team_elo_block, tempo_team_elo_meta = _format_team_elo_block(
                    tempo_team_elo_summary,
                    radiant_team_name=radiant_team_name_original,
                    dire_team_name=dire_team_name_original,
                )
                if isinstance(tempo_team_elo_meta, dict):
                    print(
                        "   📊 Tempo Team ELO attached: "
                        f"source={str(tempo_team_elo_meta.get('source') or 'unknown')} "
                        f"raw {radiant_team_name_original}={float(tempo_team_elo_meta['radiant_base_rating']):.0f} "
                        f"vs {dire_team_name_original}={float(tempo_team_elo_meta['dire_base_rating']):.0f} "
                        f"(raw_wr={float(tempo_team_elo_meta['raw_radiant_wr']):.1f}%/{float(tempo_team_elo_meta['raw_dire_wr']):.1f}%, "
                        f"adj_wr={float(tempo_team_elo_meta['adjusted_radiant_wr']):.1f}%/{float(tempo_team_elo_meta['adjusted_dire_wr']):.1f}%)"
                    )
                else:
                    print(
                        "   ⚠️ Tempo Team ELO unavailable: "
                        f"{radiant_team_name_original} vs {dire_team_name_original}"
                    )
                tempo_message_text = (
                    f"{radiant_team_name_original} VS {dire_team_name_original}\n"
                    f"{_format_series_score_line(data)}"
                    f"{tempo_team_elo_block}"
                    f"{tempo_over_fallback['bet_label']}"
                )
                print(
                    "   ✅ TEMPO fallback: нет star-сигнала, "
                    f"core4_weighted_corr_excess20={tempo_over_fallback['score']:.4f} "
                    f">= {tempo_over_fallback['threshold']:.4f}"
                )
                print(
                    "   📈 Tempo fallback score="
                    f"{tempo_over_fallback['score']:.4f} "
                    f"(threshold={tempo_over_fallback['threshold']:.4f})"
                )
                print(
                    "   📈 Tempo fallback indices: "
                    f"solo_kills_pm={tempo_over_fallback['indices']['solo_kills_pm']}, "
                    f"synergy_duo_kills_pm={tempo_over_fallback['indices']['synergy_duo_kills_pm']}, "
                    f"counterpick_1vs1_kills_pm={tempo_over_fallback['indices']['counterpick_1vs1_kills_pm']}, "
                    f"counterpick_1vs1_deaths_pm={tempo_over_fallback['indices']['counterpick_1vs1_deaths_pm']}"
                )
                if not _acquire_signal_send_slot(check_uniq_url):
                    print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                    return return_status
                try:
                    if _skip_dispatch_for_processed_url(check_uniq_url, "немедленной отправки tempo fallback после lock"):
                        return return_status
                    delivery_confirmed = _deliver_and_persist_signal(
                        check_uniq_url,
                        tempo_message_text,
                        add_url_reason="tempo_over_fallback_sent",
                        add_url_details={
                            "status": status,
                            "dispatch_mode": "tempo_over_fallback_no_star",
                            "dispatch_status_label": "tempo_over_fallback_send",
                            "tempo_over_score": float(tempo_over_fallback["score"]),
                            "tempo_over_score_threshold": float(tempo_over_fallback["threshold"]),
                            "tempo_over_bet_label": str(tempo_over_fallback["bet_label"]),
                            "tempo_over_indices": dict(tempo_over_fallback["indices"]),
                            "selected_star_wr": selected_star_wr,
                            "selected_star_mode": selected_star_mode,
                            "json_retry_errors": json_retry_errors,
                        },
                    )
                    if delivery_confirmed:
                        print("   ✅ ВЕРДИКТ: Tempo fallback отправлен немедленно")
                finally:
                    _release_signal_send_slot(check_uniq_url)
                return return_status
            print(
                "   ⚠️ ВЕРДИКТ: ОТКАЗ "
                "(нет star-сигнала) - матч пропущен"
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
    global lane_data, early_dict, late_dict, comeback_dict, comeback_meta, comeback_baseline_wr_pct
    global late_comeback_ceiling_data, late_comeback_ceiling_thresholds, late_comeback_ceiling_max_minute
    global stats_warmup_last_heavy_load_ts
    if (
        lane_data is not None
        and early_dict is not None
        and late_dict is not None
        and comeback_dict is not None
        and late_comeback_ceiling_data is not None
    ):
        return True

    def _load_json_object(path: str, label: str):
        # Avoid extra peak from f.read() copy on huge dict files.
        try:
            with open(path, "rb") as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    return orjson.loads(memoryview(mm))
        except FileNotFoundError as exc:
            _report_missing_runtime_file(label, Path(path), details=str(exc))
            raise
        except (orjson.JSONDecodeError, MemoryError) as exc:
            logger.warning(
                "Stats loader fallback for %s: orjson mmap parse failed: %s",
                label,
                exc,
            )
            print(f"⚠️ Stats loader fallback for {label}: switching to json.load()")
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except FileNotFoundError as file_exc:
                _report_missing_runtime_file(label, Path(path), details=str(file_exc))
                raise

    def _load_small_supporting_dicts():
        nonlocal comeback_path, comeback_meta_path, late_comeback_ceiling_path
        global lane_data, comeback_dict, comeback_meta, comeback_baseline_wr_pct
        global late_comeback_ceiling_data, late_comeback_ceiling_thresholds, late_comeback_ceiling_max_minute

        if lane_data is None:
            print(f"📦 Loading lane stats: {lane_path}")
            lane_data = _load_json_object(lane_path, "lane_dict_raw")
            gc.collect()

        if comeback_dict is None:
            if Path(comeback_path).exists():
                print(f"📦 Loading comeback stats: {comeback_path}")
                comeback_dict = _load_json_object(comeback_path, "comeback_solo_dict")
            else:
                logger.warning("Comeback solo stats file not found: %s", comeback_path)
                print(f"⚠️ Comeback solo stats file not found: {comeback_path}")
                _report_missing_runtime_file("comeback_solo_dict_21plus.json", Path(comeback_path))
                comeback_dict = {}
            gc.collect()

        if comeback_meta is None and comeback_baseline_wr_pct is None:
            comeback_meta = None
            comeback_baseline_wr_pct = None
            if Path(comeback_meta_path).exists():
                try:
                    with open(comeback_meta_path, "r", encoding="utf-8") as f:
                        comeback_meta = json.load(f)
                    comeback_baseline_wr_pct = float((comeback_meta or {}).get("baseline_wr_pct"))
                except Exception:
                    comeback_meta = None
                    comeback_baseline_wr_pct = None
            else:
                logger.warning("Comeback solo meta file not found: %s", comeback_meta_path)
                print(f"⚠️ Comeback solo meta file not found: {comeback_meta_path}")
                _report_missing_runtime_file("comeback_solo_dict_21plus_meta.json", Path(comeback_meta_path))

        if late_comeback_ceiling_data is None:
            late_comeback_ceiling_data = {}
            late_comeback_ceiling_thresholds = {}
            late_comeback_ceiling_max_minute = None
            if Path(late_comeback_ceiling_path).exists():
                try:
                    with open(late_comeback_ceiling_path, "r", encoding="utf-8") as f:
                        late_comeback_ceiling_data = json.load(f)
                    late_comeback_ceiling_thresholds = dict(
                        (late_comeback_ceiling_data or {}).get("thresholds_by_minute") or {}
                    )
                    minute_keys = [
                        int(k)
                        for k in late_comeback_ceiling_thresholds.keys()
                        if str(k).strip().lstrip("-").isdigit()
                    ]
                    late_comeback_ceiling_max_minute = max(minute_keys) if minute_keys else None
                except Exception:
                    late_comeback_ceiling_data = {}
                    late_comeback_ceiling_thresholds = {}
                    late_comeback_ceiling_max_minute = None
                    _report_missing_runtime_file(
                        "tier1_no_alchemist_comeback_ceiling_by_minute.json",
                        Path(late_comeback_ceiling_path),
                        details="failed to parse comeback ceiling file",
                    )
            else:
                _report_missing_runtime_file(
                    "tier1_no_alchemist_comeback_ceiling_by_minute.json",
                    Path(late_comeback_ceiling_path),
                )

    default_stats_dir = str(ANALYSE_PUB_DIR)
    stats_dir = os.getenv("STATS_DIR", default_stats_dir)
    lane_path = os.getenv("STATS_LANE_PATH", f"{stats_dir}/lane_dict_raw.json")
    early_path = os.getenv("STATS_EARLY_PATH", f"{stats_dir}/early_dict_raw.json")
    late_path = os.getenv("STATS_LATE_PATH", f"{stats_dir}/late_dict_raw.json")
    comeback_path = os.getenv(
        "STATS_COMEBACK_PATH",
        f"{stats_dir}/comeback_experiment_hard_1_7_hero_position/comeback_solo_dict_21plus.json",
    )
    comeback_meta_path = os.getenv(
        "STATS_COMEBACK_META_PATH",
        f"{stats_dir}/comeback_experiment_hard_1_7_hero_position/comeback_solo_dict_21plus_meta.json",
    )
    late_comeback_ceiling_path = os.getenv(
        "STATS_LATE_COMEBACK_CEILING_PATH",
        str(BASE_DIR / "tier1_no_alchemist_comeback_ceiling_by_minute.json"),
    )

    # If test stats folder has no lane dict, fallback to baseline lane dict.
    if not Path(lane_path).exists():
        fallback_lane = f"{default_stats_dir}/lane_dict_raw.json"
        if Path(fallback_lane).exists():
            lane_path = fallback_lane

    _load_small_supporting_dicts()

    if not STATS_SEQUENTIAL_WARMUP_ENABLED:
        if early_dict is None:
            if _stats_sharded_mode_enabled("early"):
                early_dict = _prepare_sharded_stats_lookup(early_path, "early")
            else:
                print(f"📦 Loading early stats: {early_path}")
                early_dict = _load_json_object(early_path, "early_dict_raw")
            gc.collect()
        if late_dict is None:
            if _stats_sharded_mode_enabled("late"):
                late_dict = _prepare_sharded_stats_lookup(late_path, "late")
            else:
                print(f"📦 Loading late stats: {late_path}")
                late_dict = _load_json_object(late_path, "late_dict_raw")
            gc.collect()
        return (
            lane_data is not None
            and early_dict is not None
            and late_dict is not None
            and comeback_dict is not None
            and late_comeback_ceiling_data is not None
        )

    if stats_warmup_last_heavy_load_ts == 0.0 and (early_dict is None or late_dict is None):
        stats_warmup_last_heavy_load_ts = time.time()
        return False

    now_ts = time.time()
    remaining_heavy = []
    if early_dict is None:
        remaining_heavy.append(("early", early_path))
    if late_dict is None:
        remaining_heavy.append(("late", late_path))

    if not remaining_heavy:
        return True

    if stats_warmup_last_heavy_load_ts and (now_ts - stats_warmup_last_heavy_load_ts) < STATS_WARMUP_STEP_DELAY_SECONDS:
        return False

    next_label, next_path = remaining_heavy[0]
    if _stats_sharded_mode_enabled(next_label):
        next_payload = _prepare_sharded_stats_lookup(next_path, next_label)
    else:
        print(f"📦 Warmup loading {next_label} stats: {next_path}")
        next_payload = _load_json_object(next_path, f"{next_label}_dict_raw")
    if next_label == "early":
        early_dict = next_payload
    else:
        late_dict = next_payload
    stats_warmup_last_heavy_load_ts = time.time()
    gc.collect()
    return (
        lane_data is not None
        and early_dict is not None
        and late_dict is not None
        and comeback_dict is not None
        and late_comeback_ceiling_data is not None
    )


def _load_tempo_stats_dicts() -> bool:
    global tempo_solo_dict, tempo_duo_dict, tempo_cp1v1_dict
    if (
        tempo_solo_dict is not None
        and tempo_duo_dict is not None
        and tempo_cp1v1_dict is not None
    ):
        return True
    try:
        base_dir = Path(TEMPO_STATS_DIR_DEFAULT)
        _, load_tempo_dicts = _get_tempo_helpers()
        tempo_solo_dict, tempo_duo_dict, tempo_cp1v1_dict = load_tempo_dicts(base_dir)
        return True
    except Exception as exc:
        logger.warning("Tempo fallback disabled: failed to load tempo dicts: %s", exc)
        return False


def _format_series_score_line(live_payload: Optional[dict]) -> str:
    try:
        live_league = live_payload.get('live_league_data') or {}
        r_wins = live_league.get('radiant_series_wins')
        d_wins = live_league.get('dire_series_wins')
        if r_wins is None and d_wins is None:
            return ""
        return f"{int(r_wins or 0)}-{int(d_wins or 0)}\n"
    except Exception:
        return ""


def _compute_tempo_over_fallback_payload(
    radiant_heroes_and_pos: dict,
    dire_heroes_and_pos: dict,
    match_tier: Optional[int],
) -> Optional[Dict[str, Any]]:
    diag = _compute_tempo_over_fallback_diagnostics(
        radiant_heroes_and_pos=radiant_heroes_and_pos,
        dire_heroes_and_pos=dire_heroes_and_pos,
        match_tier=match_tier,
    )
    if not isinstance(diag, dict):
        return None
    payload = diag.get("payload")
    return dict(payload) if isinstance(payload, dict) else None


def _compute_tempo_over_fallback_diagnostics(
    radiant_heroes_and_pos: dict,
    dire_heroes_and_pos: dict,
    match_tier: Optional[int],
) -> Dict[str, Any]:
    diag: Dict[str, Any] = {
        "enabled": bool(TEMPO_OVER_FALLBACK_ENABLED),
        "match_tier": int(match_tier or 0),
        "threshold": float(TEMPO_OVER_SCORE_THRESHOLD),
        "bet_label": TEMPO_OVER_SCORE_LABEL,
        "reason": "unknown",
        "score": None,
        "indices": {},
        "payload": None,
    }
    if not TEMPO_OVER_FALLBACK_ENABLED:
        diag["reason"] = "disabled"
        return diag
    if int(match_tier or 0) != 1:
        diag["reason"] = "tier_not_supported"
        return diag
    if not _load_tempo_stats_dicts():
        diag["reason"] = "dicts_unavailable"
        return diag

    def _normalize_team_payload(team_payload: dict) -> Dict[str, Dict[str, int]]:
        normalized: Dict[str, Dict[str, int]] = {}
        for pos, hero_info in (team_payload or {}).items():
            if isinstance(hero_info, dict):
                hero_id = hero_info.get("hero_id")
            else:
                hero_id = hero_info
            try:
                normalized[str(pos)] = {"hero_id": int(hero_id)}
            except (TypeError, ValueError):
                continue
        return normalized

    try:
        build_tempo_draft_metrics, _ = _get_tempo_helpers()
        tempo_metrics = build_tempo_draft_metrics(
            _normalize_team_payload(radiant_heroes_and_pos),
            _normalize_team_payload(dire_heroes_and_pos),
            tempo_solo_dict,
            tempo_duo_dict,
            tempo_cp1v1_dict,
        )
    except Exception as exc:
        logger.warning("Tempo fallback disabled: failed to build tempo metrics: %s", exc)
        diag["reason"] = "build_failed"
        return diag

    solo_payload = (tempo_metrics.get("solo") or {})
    duo_payload = (tempo_metrics.get("synergy_duo") or {})
    cp_payload = (tempo_metrics.get("counterpick_1vs1") or {})
    if not (
        bool(solo_payload.get("complete"))
        and bool(duo_payload.get("complete"))
        and bool(cp_payload.get("complete"))
    ):
        diag["reason"] = "incomplete_metrics"
        return diag

    def _idx(family_payload: dict, metric_name: str) -> Optional[int]:
        value = ((family_payload.get(metric_name) or {}).get("index"))
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    solo_kills_idx = _idx(solo_payload, "kills_pm")
    duo_kills_idx = _idx(duo_payload, "kills_pm")
    cp_kills_idx = _idx(cp_payload, "kills_pm")
    cp_deaths_idx = _idx(cp_payload, "deaths_pm")
    diag["indices"] = {
        "solo_kills_pm": solo_kills_idx,
        "synergy_duo_kills_pm": duo_kills_idx,
        "counterpick_1vs1_kills_pm": cp_kills_idx,
        "counterpick_1vs1_deaths_pm": cp_deaths_idx,
    }
    if None in {solo_kills_idx, duo_kills_idx, cp_kills_idx, cp_deaths_idx}:
        diag["reason"] = "missing_indices"
        return diag

    score = (
        0.2414 * max(0, int(solo_kills_idx) - 20)
        + 0.2492 * max(0, int(duo_kills_idx) - 20)
        + 0.2931 * max(0, int(cp_kills_idx) - 20)
        + 0.3517 * max(0, int(cp_deaths_idx) - 20)
    )
    diag["score"] = float(score)
    payload = {
        "score": float(score),
        "threshold": float(TEMPO_OVER_SCORE_THRESHOLD),
        "bet_label": TEMPO_OVER_SCORE_LABEL,
        "indices": {
            "solo_kills_pm": int(solo_kills_idx),
            "synergy_duo_kills_pm": int(duo_kills_idx),
            "counterpick_1vs1_kills_pm": int(cp_kills_idx),
            "counterpick_1vs1_deaths_pm": int(cp_deaths_idx),
        },
    }
    if float(score) < float(TEMPO_OVER_SCORE_THRESHOLD):
        diag["reason"] = "below_threshold"
        return diag
    diag["reason"] = "threshold_met"
    diag["payload"] = payload
    return diag


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

    # Гарантируем staged warmup словарей перед обработкой матчей.
    # На слабых серверах early/late грузим по шагам, чтобы не давать один резкий пик.
    stats_ready = _load_stats_dicts()
    if stats_ready is False:
        warmup_parts = []
        if lane_data is not None:
            warmup_parts.append("lane")
        if early_dict is not None:
            warmup_parts.append("early")
        if late_dict is not None:
            warmup_parts.append("late")
        if comeback_dict is not None:
            warmup_parts.append("comeback")
        print(
            "⏳ Stats warmup in progress: "
            f"loaded={','.join(warmup_parts) or 'none'}; "
            f"step_delay={int(STATS_WARMUP_STEP_DELAY_SECONDS)}s"
        )
        return None

    logger.info(f"\n{'='*60}\n🔄 НАЧАЛО ЦИКЛА ПРОВЕРКИ МАТЧЕЙ\n{'='*60}")

    radiant_heroes_and_pos, dire_heroes_and_pos, radiant_team_name, dire_team_name, score, return_status = None, None, None, None, None, None
    recovered_from_journal = _safe_flush_sent_signal_journal_into_map_id_check()
    maps_data = _load_map_id_check_urls(recover=True)
    delayed_queue_state = _load_delayed_queue_state(recover=True)
    _replace_monitored_matches_from_snapshot(delayed_queue_state)
    _sync_processed_urls_cache(maps_data)
    uncertain_delivery_urls = _load_uncertain_delivery_urls()
    _sync_uncertain_delivery_urls_cache(uncertain_delivery_urls)
    print(f"✅ Загружено {len(maps_data)} матчей из {MAP_ID_CHECK_PATH}")
    if recovered_from_journal:
        print(f"✅ Восстановлено из recovery journal: {recovered_from_journal}")
    if delayed_queue_state:
        print(f"✅ Восстановлено delayed-очереди: {len(delayed_queue_state)}")
    if uncertain_delivery_urls:
        print(f"⚠️ Заблокировано uncertain delivery URL: {len(set(uncertain_delivery_urls))}")

    _ensure_delayed_sender_started()
    if BOOKMAKER_PREFETCH_ENABLED:
        _ensure_bookmaker_prefetch_started()
    else:
        _stop_bookmaker_prefetch_worker()
    print(f"🌐 Получение списка активных матчей...")
    answer = get_heads()
    if not answer or answer is None:
        print('❌ Не удалось выяснить heads (нет активных матчей)')
        return None
    heads, bodies = answer
    
    # Проверка что heads не None
    if heads is None:
        if GET_HEADS_LAST_FAILURE_REASON == GET_HEADS_FAILURE_REASON_LIVE_MATCHES_MISSING_ALL_PROXIES:
            print('❌ Не найден элемент live__matches в HTML')
            try:
                send_message('❌ Не найден элемент live__matches в HTML', admin_only=True)
            except Exception as e:
                print(f"⚠️ Не удалось отправить уведомление в Telegram: {e}")
        return None

    if not heads or not bodies:
        schedule_info = NEXT_SCHEDULE_MATCH_INFO if isinstance(NEXT_SCHEDULE_MATCH_INFO, dict) else {}
        sleep_seconds = float(schedule_info.get("sleep_seconds", 0.0) or 0.0)
        raw_sleep_seconds = float(schedule_info.get("sleep_seconds_raw", sleep_seconds) or sleep_seconds or 0.0)
        matchup = str(schedule_info.get("matchup") or "unknown")
        scheduled_at_msk = schedule_info.get("scheduled_at_msk")
        scheduled_label = (
            scheduled_at_msk.strftime("%Y-%m-%d %H:%M:%S MSK")
            if isinstance(scheduled_at_msk, datetime)
            else "unknown"
        )
        if _should_poll_for_scheduled_live_target():
            target_info = SCHEDULE_LIVE_WAIT_TARGET if isinstance(SCHEDULE_LIVE_WAIT_TARGET, dict) else {}
            target_label = _format_schedule_match_label(target_info)
            post_start_poll_seconds = max(1, int(math.ceil(float(SCHEDULE_POST_START_POLL_SECONDS))))
            print(
                "⏳ Scheduled match start has passed, but live matches are still empty. "
                f"Waiting for live appearance for {target_label}. "
                f"Next recheck in {post_start_poll_seconds}s"
            )
            return "__sleep_wait_for_live_after_schedule__"
        if sleep_seconds > 0:
            print(
                "🗓️ Live matches empty. "
                f"Nearest scheduled match: {matchup} at {scheduled_label}. "
                f"Sleep planned: {int(math.ceil(sleep_seconds))}s "
                f"(raw until start: {int(math.ceil(raw_sleep_seconds))}s)"
            )
            return "__sleep_until_schedule__"
        print("⚠️ Live matches empty and no future scheduled match was parsed")
        return None
    
    print(f'✅ Найдено активных матчей: {len(heads)}')
    
    all_statuses = []
    seen_series_keys: set[str] = set()
    for i in range(len(heads)):
        match_ref = f"match_index={i}"
        try:
            link_tag = bodies[i].find('a') if i < len(bodies) else None
            href = link_tag.get('href') if link_tag else None
            if href:
                match_ref = str(href)
                from urllib.parse import urlparse
                parsed = urlparse(href if "://" in str(href) else f"https://dltv.org{href}")
                path = str(parsed.path or "")
                if path:
                    series_match = re.search(r"/matches/(\d+)", path)
                    if series_match:
                        seen_series_keys.add(series_match.group(1))
                    seen_series_keys.add(f"dltv.org{path}")
        except Exception:
            match_ref = f"match_index={i}"
        try:
            answer = check_head(heads, bodies, i, maps_data)
        except Exception as exc:
            print(f"⚠️ Ошибка обработки матча #{i} ({match_ref}): {exc}")
            logger.exception("Per-match processing failed for %s", match_ref)
            all_statuses.append("error")
            continue
        if answer is not None:
            if isinstance(answer, str):
                all_statuses.append(answer)
            # else:
            #     try:
            #         radiant_heroes_and_pos, dire_heroes_and_pos, radiant_team_name, dire_team_name, score, return_status = answer
            #         return radiant_heroes_and_pos, dire_heroes_and_pos, radiant_team_name, dire_team_name, score, return_status
            #     except:
            #         pass

    orphan_live_elo_updates = _finalize_orphaned_live_elo_series(seen_series_keys)
    for orphan_update in orphan_live_elo_updates:
        applied_update = orphan_update.get("applied_update") if isinstance(orphan_update.get("applied_update"), dict) else {}
        _emit_live_elo_applied_log("Live ELO finalized from orphaned finished series", applied_update)
    
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
    _setup_run_logging()

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
    runtime_mode_label = _runtime_instance_mode_label(args.odds)
    if not _try_acquire_runtime_instance_lock(mode_label=runtime_mode_label):
        raise SystemExit(0)
    DELAYED_QUEUE_PATH = str(_delayed_queue_path_for_mode(runtime_mode_label))
    print(f"🗂️ DELAYED_QUEUE_PATH for mode={runtime_mode_label}: {DELAYED_QUEUE_PATH}")

    import orjson
    from functions import one_match, check_old_maps
    from keys import start_date_time

    if args.odds is True and os.getenv("MAP_ID_CHECK_PATH") is None:
        MAP_ID_CHECK_PATH = MAP_ID_CHECK_PATH_ODDS_DEFAULT
        print(f"🗺️ MAP_ID_CHECK_PATH defaulted for --odds: {MAP_ID_CHECK_PATH}")

    # Абсолютные пути к данным (вынесены за пределы проекта для оптимизации Cursor)
    STATS_DIR = str(ANALYSE_PUB_DIR)

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

    while True:
        try:
            quiet_sleep_seconds = _compute_moscow_quiet_hours_sleep_seconds()
            if quiet_sleep_seconds > 0:
                quiet_now = datetime.now(MOSCOW_TZ)
                wake_at = quiet_now.replace(
                    hour=QUIET_HOURS_END_HOUR_MSK,
                    minute=0,
                    second=0,
                    microsecond=0,
                )
                print(
                    "🌙 Quiet hours active (MSK): "
                    f"{QUIET_HOURS_START_HOUR_MSK:02d}:00-{QUIET_HOURS_END_HOUR_MSK:02d}:00. "
                    f"Sleeping until {wake_at.strftime('%H:%M')} MSK "
                    f"({int(quiet_sleep_seconds)}s)"
                )
                time.sleep(quiet_sleep_seconds)
                continue
            status = general(use_proxy=None, odds=args.odds)
            if status == "__sleep_until_schedule__":
                scheduled_sleep_seconds = max(1, int(math.ceil(float(NEXT_SCHEDULE_SLEEP_SECONDS or 0.0))))
                schedule_snapshot = dict(NEXT_SCHEDULE_MATCH_INFO or {})
                sleep_started_at_msk = datetime.now(MOSCOW_TZ)
                if schedule_snapshot:
                    schedule_snapshot["sleep_started_at_msk"] = sleep_started_at_msk
                    schedule_snapshot["planned_sleep_seconds"] = scheduled_sleep_seconds
                print(f"Сплю {scheduled_sleep_seconds} секунд до ближайшего матча по расписанию DLTV")
                time.sleep(scheduled_sleep_seconds)
                if schedule_snapshot:
                    schedule_snapshot["woke_at_msk"] = datetime.now(MOSCOW_TZ)
                    SCHEDULE_LIVE_WAIT_TARGET = dict(schedule_snapshot)
                    PENDING_SCHEDULE_WAKE_AUDIT = schedule_snapshot
            elif status == "__sleep_wait_for_live_after_schedule__":
                wait_for_live_seconds = max(1, int(math.ceil(float(SCHEDULE_POST_START_POLL_SECONDS))))
                print(f"Сплю {wait_for_live_seconds} секунд в режиме ожидания появления live matches")
                time.sleep(wait_for_live_seconds)
            elif status is None:
                print('Сплю 60 секунд')
                time.sleep(60)
            else:
                print('Сплю 60 секунд')
                time.sleep(60)
        except Exception as e:
            print(f"⚠️ Ошибка главного цикла: {e}")
            logger.exception("Main loop error")
            time.sleep(30)
