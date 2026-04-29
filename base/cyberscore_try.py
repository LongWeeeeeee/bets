import argparse
import importlib.util
import json
from html import escape as html_escape
import ast
import atexit
import contextlib
from collections import deque, OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
import io
import orjson
try:
    import ijson
except Exception:
    ijson = None
import time
import sys
import os
import pickle
import logging
import asyncio
import threading
import queue
import glob
import copy
import mmap
import gc
import resource
import subprocess
import re
import shlex
import shutil
import sqlite3
import tempfile
from itertools import combinations, permutations
import numpy as np
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple, Union
import math
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import requests
try:
    import camoufox
    CAMOUFOX_AVAILABLE = True
except Exception:
    camoufox = None
    CAMOUFOX_AVAILABLE = False
try:
    from curl_cffi import requests as curl_cffi_requests
    from curl_cffi.requests.exceptions import RequestException as CurlCffiRequestException
    CURL_CFFI_AVAILABLE = True
except Exception as _curl_cffi_import_error:
    curl_cffi_requests = None
    CurlCffiRequestException = None
    CURL_CFFI_AVAILABLE = False
from functions import (
    send_message,
    drain_telegram_admin_commands,
    synergy_and_counterpick,
    calculate_lanes,
    format_output_dict,
    STAR_THRESHOLDS_BY_WR,
    STAR_DISABLED_METRICS,
    TelegramSendError,
)
try:
    from keys import api_to_proxy, BOOKMAKER_PROXY_URL, BOOKMAKER_PROXY_POOL, DLTV_PROXY_POOL
except ImportError:
    from keys import api_to_proxy, BOOKMAKER_PROXY_URL
    BOOKMAKER_PROXY_POOL = []
    DLTV_PROXY_POOL = []


def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


# Dota2ProTracker integration (optional)
DOTA2PROTRACKER_ENABLED = _env_flag('DOTA2PROTRACKER_ENABLED', '0')
DOTA2PROTRACKER_MIN_GAMES = int(os.getenv('DOTA2PROTRACKER_MIN_GAMES', '10'))
DOTA2PROTRACKER_MESSAGE_BLOCK_ENABLED = _env_flag(
    'DOTA2PROTRACKER_MESSAGE_BLOCK_ENABLED',
    '1' if DOTA2PROTRACKER_ENABLED else '0',
)
DOTA2PROTRACKER_ONLY_MODE = _env_flag('DOTA2PROTRACKER_ONLY_MODE', '0')
DOTA2PROTRACKER_BYPASS_GATES = _env_flag(
    'DOTA2PROTRACKER_BYPASS_GATES',
    '1' if DOTA2PROTRACKER_ONLY_MODE else '0',
)
DOTA2PROTRACKER_SKIP_BOOKMAKER_GATE = _env_flag(
    'DOTA2PROTRACKER_SKIP_BOOKMAKER_GATE',
    '1' if DOTA2PROTRACKER_BYPASS_GATES else '0',
)
DOTA2PROTRACKER_SUPERSEDE_OPENDOTA = _env_flag(
    'DOTA2PROTRACKER_SUPERSEDE_OPENDOTA',
    '1' if DOTA2PROTRACKER_ENABLED else '0',
)
DOTA2PROTRACKER_CP1VS1_GATE_ABS = float(os.getenv('DOTA2PROTRACKER_CP1VS1_GATE_ABS', '3'))
DOTA2PROTRACKER_DUO_GATE_ABS = float(os.getenv('DOTA2PROTRACKER_DUO_GATE_ABS', '7'))
SIGNAL_MINIMAL_ODDS_ONLY_MODE = _env_flag('SIGNAL_MINIMAL_ODDS_ONLY_MODE', '0')
CLASSIC_SIGNAL_PIPELINE_ENABLED = _env_flag(
    'CLASSIC_SIGNAL_PIPELINE_ENABLED',
    '0' if SIGNAL_MINIMAL_ODDS_ONLY_MODE else '1',
)
DLTV_CAMOUFOX_ENABLED = _env_flag(
    'DLTV_CAMOUFOX_ENABLED',
    '1' if CAMOUFOX_AVAILABLE else '0',
)
_dota2protracker_module = None
if DOTA2PROTRACKER_ENABLED or _env_flag('DOTA2PROTRACKER_PRELOAD', '1'):
    try:
        _dota2protracker_path = os.path.join(os.path.dirname(__file__), "dota2protracker.py")
        _dota2protracker_spec = importlib.util.spec_from_file_location(
            "base_dota2protracker_runtime",
            _dota2protracker_path,
        )
        if _dota2protracker_spec is None or _dota2protracker_spec.loader is None:
            raise ImportError(f"spec not available for {_dota2protracker_path}")
        _dota2protracker_module = importlib.util.module_from_spec(_dota2protracker_spec)
        _dota2protracker_spec.loader.exec_module(_dota2protracker_module)
        enrich_with_pro_tracker = _dota2protracker_module.enrich_with_pro_tracker
    except Exception:
        try:
            import dota2protracker as _dota2protracker_module
            enrich_with_pro_tracker = _dota2protracker_module.enrich_with_pro_tracker
            print("   ⚠️ Dota2ProTracker imported from legacy module path")
        except ImportError:
            enrich_with_pro_tracker = None
            print("   ⚠️ Dota2ProTracker integration disabled (module not found)")
else:
    enrich_with_pro_tracker = None

# OpenDota API integration (preferred - no Cloudflare blocking)
OPENDOTA_ENABLED = os.getenv('OPENDOTA_ENABLED', '1') == '1'
OPENDOTA_MIN_GAMES = int(os.getenv('OPENDOTA_MIN_GAMES', '10'))
if OPENDOTA_ENABLED:
    try:
        from opendota_matchups import enrich_with_opendota
    except ImportError:
        enrich_with_opendota = None
        print("   ⚠️ OpenDota integration disabled (module not found)")
else:
    enrich_with_opendota = None
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

SKIPPED_LIVE_LEAGUE_TITLES = {
    "blast slam vii: china open qualifier 2",
    "blast slam 7: china open qualifier 2",
    "blast slam vii: southeast asia open qualifier 2",
    "blast slam 7: southeast asia open qualifier 2",
}

# Dota account ids / steam ids for lineups we never want to process in live runtime.
# Current banned-player denylist:
# Norma -> 187123736
# queezy -> 360263638
SKIPPED_PLAYER_ACCOUNT_IDS = {
    187123736,
    360263638,
}

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


def _normalize_live_league_title(title: Any) -> str:
    return re.sub(r"\s+", " ", str(title or "").strip()).lower()


def _slugify_live_league_title(title: Any) -> str:
    normalized = _normalize_live_league_title(title)
    return re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")


def _is_skipped_live_league_candidate(*, league_title: Any = "", href: Any = "") -> bool:
    normalized_title = _normalize_live_league_title(league_title)
    if normalized_title in SKIPPED_LIVE_LEAGUE_TITLES:
        return True
    href_text = str(href or "").strip().lower()
    if href_text:
        for denied_title in SKIPPED_LIVE_LEAGUE_TITLES:
            denied_slug = _slugify_live_league_title(denied_title)
            if denied_slug and denied_slug in href_text:
                return True
    return False


def _find_skipped_player_account_ids(
    radiant_account_ids: Optional[List[int]],
    dire_account_ids: Optional[List[int]],
) -> Dict[str, List[int]]:
    radiant_hits = sorted(
        {
            int(pid)
            for pid in (radiant_account_ids or [])
            if _coerce_int(pid) > 0 and int(pid) in SKIPPED_PLAYER_ACCOUNT_IDS
        }
    )
    dire_hits = sorted(
        {
            int(pid)
            for pid in (dire_account_ids or [])
            if _coerce_int(pid) > 0 and int(pid) in SKIPPED_PLAYER_ACCOUNT_IDS
        }
    )
    return {
        "radiant": radiant_hits,
        "dire": dire_hits,
    }


def _target_side_skipped_player_hits(
    skipped_player_hits: Optional[Dict[str, List[int]]],
    target_side: Optional[str],
) -> List[int]:
    if target_side not in {"radiant", "dire"} or not isinstance(skipped_player_hits, dict):
        return []
    hits = skipped_player_hits.get(target_side)
    if not isinstance(hits, list):
        return []
    return sorted({int(pid) for pid in hits if _coerce_int(pid) > 0})


def _player_denylist_block_payload(
    *,
    target_side: Optional[str],
    skipped_player_hits: Optional[Dict[str, List[int]]],
    radiant_team_name: Any,
    dire_team_name: Any,
    radiant_account_ids: Optional[List[int]] = None,
    dire_account_ids: Optional[List[int]] = None,
) -> Optional[Dict[str, Any]]:
    blocked_hits = _target_side_skipped_player_hits(skipped_player_hits, target_side)
    if not blocked_hits:
        return None
    blocked_team_name = (
        str(radiant_team_name or "").strip()
        if target_side == "radiant"
        else str(dire_team_name or "").strip()
        if target_side == "dire"
        else ""
    )
    return {
        "target_side": str(target_side or ""),
        "target_team": blocked_team_name,
        "blocked_player_account_ids": list(blocked_hits),
        "radiant_account_ids": [
            int(pid) for pid in (radiant_account_ids or []) if _coerce_int(pid) > 0
        ],
        "dire_account_ids": [
            int(pid) for pid in (dire_account_ids or []) if _coerce_int(pid) > 0
        ],
        "skipped_player_hits": {
            "radiant": [
                int(pid) for pid in ((skipped_player_hits or {}).get("radiant") or []) if _coerce_int(pid) > 0
            ],
            "dire": [
                int(pid) for pid in ((skipped_player_hits or {}).get("dire") or []) if _coerce_int(pid) > 0
            ],
        },
    }


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
        from bookmaker_selenium_odds import (  # type: ignore
            _build_driver as _bookmaker_build_driver,
            _is_map_market_closed as _bookmaker_is_map_market_closed,
            _open_match_details_by_teams as _bookmaker_open_match_details_by_teams,
            _open_presence_site_tabs as _bookmaker_open_presence_site_tabs,
            _parse_map_market_on_current_page as _bookmaker_parse_map_market_on_current_page,
            _probe_presence_site_in_current_tab as _bookmaker_probe_presence_site_in_current_tab,
            parse_site as _bookmaker_parse_site,
            parse_site_in_camoufox_page as _bookmaker_parse_site_in_camoufox_page,
            run_sites_in_camoufox as _bookmaker_run_sites_in_camoufox,
            camoufox as _bookmaker_camoufox,
            _camoufox_proxy_kwargs as _bookmaker_camoufox_proxy_kwargs,
            BOOKMAKER_URLS as _BOOKMAKER_URLS_MAP,
        )
    except Exception:
        from bookmaker_selenium_odds import (  # type: ignore
            _build_driver as _bookmaker_build_driver,
            _is_map_market_closed as _bookmaker_is_map_market_closed,
            _open_match_details_by_teams as _bookmaker_open_match_details_by_teams,
            _open_presence_site_tabs as _bookmaker_open_presence_site_tabs,
            _parse_map_market_on_current_page as _bookmaker_parse_map_market_on_current_page,
            _probe_presence_site_in_current_tab as _bookmaker_probe_presence_site_in_current_tab,
            parse_site as _bookmaker_parse_site,
            parse_site_in_camoufox_page as _bookmaker_parse_site_in_camoufox_page,
            run_sites_in_camoufox as _bookmaker_run_sites_in_camoufox,
            camoufox as _bookmaker_camoufox,
            _camoufox_proxy_kwargs as _bookmaker_camoufox_proxy_kwargs,
            BOOKMAKER_URLS as _BOOKMAKER_URLS_MAP,
        )
    BOOKMAKER_PREFETCH_AVAILABLE = True
    BOOKMAKER_CAMOUFOX_IMPORTED = True
except Exception as _bookmaker_import_error:
    BOOKMAKER_PREFETCH_AVAILABLE = False
    BOOKMAKER_CAMOUFOX_IMPORTED = False
    _bookmaker_build_driver = None
    _bookmaker_is_map_market_closed = None
    _bookmaker_open_match_details_by_teams = None
    _bookmaker_open_presence_site_tabs = None
    _bookmaker_parse_map_market_on_current_page = None
    _bookmaker_probe_presence_site_in_current_tab = None
    _bookmaker_parse_site = None
    _BOOKMAKER_URLS_MAP = {}
    logger.warning("Bookmaker prefetch disabled: %s", _bookmaker_import_error)


def _normalize_bookmaker_gate_mode(value: Any, default: str = "odds") -> str:
    mode = str(value or default).strip().lower()
    if mode not in {"odds", "presence"}:
        return str(default).strip().lower()
    return mode


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
    if label not in {"early", "late", "post_lane"}:
        return False
    per_label_env = f"STATS_{label.upper()}_SHARDED_LOOKUP_MODE"
    per_label_mode = str(os.getenv(per_label_env, "")).strip().lower()
    mode = per_label_mode or STATS_SHARDED_LOOKUP_MODE
    if label == "post_lane" and not per_label_mode and mode == "auto":
        return True
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
    def __init__(
        self,
        shard_dir: Path,
        *,
        label: str,
        max_cached_shards: int = 24,
        max_cached_keys: int = 0,
    ):
        super().__init__()
        self.shard_dir = Path(shard_dir)
        self.label = str(label)
        self.max_cached_shards = max(0, int(max_cached_shards))
        self._shards: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self.max_cached_keys = max(0, int(max_cached_keys))
        self._key_cache: "OrderedDict[str, Any]" = OrderedDict()

    def __bool__(self) -> bool:
        return True

    @property
    def cache_enabled(self) -> bool:
        return self.max_cached_shards > 0

    @property
    def key_cache_enabled(self) -> bool:
        return self.max_cached_keys > 0

    def _get_cached_key(self, key: Any) -> Tuple[bool, Any]:
        if not self.key_cache_enabled:
            return False, None
        key_str = str(key)
        try:
            value = self._key_cache[key_str]
        except KeyError:
            return False, None
        self._key_cache.move_to_end(key_str)
        return True, value

    def _remember_key(self, key: Any, value: Any) -> None:
        if not self.key_cache_enabled:
            return
        key_str = str(key)
        self._key_cache[key_str] = value
        self._key_cache.move_to_end(key_str)
        while len(self._key_cache) > self.max_cached_keys:
            self._key_cache.popitem(last=False)

    def _load_shard(self, shard_id: str) -> Dict[str, Any]:
        shard_id = str(shard_id or "misc")
        if self.cache_enabled:
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
        if not self.cache_enabled:
            return shard_data

        self._shards[shard_id] = shard_data
        self._shards.move_to_end(shard_id)
        while len(self._shards) > self.max_cached_shards:
            self._shards.popitem(last=False)
        return shard_data

    def _get_uncached(self, key: Any, default=None):
        key_str = str(key)
        found, cached = self._get_cached_key(key_str)
        if found:
            return cached
        shard_path = self.shard_dir / f"{_stats_key_leading_hero_id(key_str)}.jsonl"
        if not shard_path.exists():
            return default
        with shard_path.open("rb") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    stored_key, value = orjson.loads(line)
                except Exception:
                    continue
                if str(stored_key) == key_str:
                    self._remember_key(key_str, value)
                    return value
        return default

    def get_many(self, keys: Any) -> Dict[str, Any]:
        requested_by_shard: Dict[str, set] = {}
        result: Dict[str, Any] = {}
        for key in keys or []:
            key_str = str(key)
            found, cached = self._get_cached_key(key_str)
            if found:
                result[key_str] = cached
                continue
            requested_by_shard.setdefault(_stats_key_leading_hero_id(key_str), set()).add(key_str)

        for shard_id, wanted_keys in requested_by_shard.items():
            if not wanted_keys:
                continue
            cached = self._shards.get(shard_id) if self.cache_enabled else None
            if cached is not None:
                for key in wanted_keys:
                    if key in cached:
                        result[key] = cached[key]
                        self._remember_key(key, cached[key])
                continue

            remaining = set(wanted_keys)
            shard_path = self.shard_dir / f"{shard_id}.jsonl"
            if not shard_path.exists():
                continue
            with shard_path.open("rb") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        stored_key, value = orjson.loads(line)
                    except Exception:
                        continue
                    stored_key = str(stored_key)
                    if stored_key not in remaining:
                        continue
                    result[stored_key] = value
                    self._remember_key(stored_key, value)
                    remaining.remove(stored_key)
                    if not remaining:
                        break
        return result

    def warm_hero_ids(self, hero_ids: List[Any]) -> None:
        if not self.cache_enabled:
            return
        for hero_id in hero_ids:
            try:
                shard_id = str(int(hero_id))
            except (TypeError, ValueError):
                continue
            self._load_shard(shard_id)

    def get(self, key: Any, default=None):
        found, cached = self._get_cached_key(key)
        if found:
            return cached
        if not self.cache_enabled:
            return self._get_uncached(key, default)
        shard_id = _stats_key_leading_hero_id(key)
        shard = self._load_shard(shard_id)
        value = shard.get(str(key), default)
        if value is not default:
            self._remember_key(key, value)
        return value


class _SqliteStatsLookup(dict):
    def __init__(self, db_path: Path, *, label: str, max_cached_keys: int = 0):
        super().__init__()
        self.db_path = Path(db_path)
        self.label = str(label)
        self.max_cached_keys = max(0, int(max_cached_keys))
        self._key_cache: "OrderedDict[str, Any]" = OrderedDict()
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()

    def __bool__(self) -> bool:
        return True

    @property
    def key_cache_enabled(self) -> bool:
        return self.max_cached_keys > 0

    def _connect(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        uri = f"{self.db_path.resolve().as_uri()}?mode=ro&immutable=1"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA temp_store=MEMORY")
        self._conn = conn
        return conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _get_cached_key(self, key: Any) -> Tuple[bool, Any]:
        if not self.key_cache_enabled:
            return False, None
        key_str = str(key)
        try:
            value = self._key_cache[key_str]
        except KeyError:
            return False, None
        self._key_cache.move_to_end(key_str)
        return True, value

    def _remember_key(self, key: Any, value: Any) -> None:
        if not self.key_cache_enabled:
            return
        key_str = str(key)
        self._key_cache[key_str] = value
        self._key_cache.move_to_end(key_str)
        while len(self._key_cache) > self.max_cached_keys:
            self._key_cache.popitem(last=False)

    def get_many(self, keys: Any) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        missing: List[str] = []
        seen = set()
        for key in keys or []:
            key_str = str(key)
            if key_str in seen:
                continue
            seen.add(key_str)
            found, cached = self._get_cached_key(key_str)
            if found:
                result[key_str] = cached
            else:
                missing.append(key_str)

        if not missing:
            return result

        chunk_size = max(1, min(int(STATS_SQLITE_QUERY_CHUNK_SIZE), 900))
        with self._lock:
            conn = self._connect()
            for start in range(0, len(missing), chunk_size):
                chunk = missing[start:start + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT key, value FROM stats WHERE key IN ({placeholders})",
                    chunk,
                )
                for key, value_blob in rows:
                    value = orjson.loads(value_blob)
                    key = str(key)
                    result[key] = value
                    self._remember_key(key, value)
        return result

    def get(self, key: Any, default=None):
        key_str = str(key)
        found, cached = self._get_cached_key(key_str)
        if found:
            return cached
        with self._lock:
            conn = self._connect()
            row = conn.execute("SELECT value FROM stats WHERE key = ?", (key_str,)).fetchone()
        if row is None:
            return default
        value = orjson.loads(row[0])
        self._remember_key(key_str, value)
        return value

    def warm_hero_ids(self, hero_ids: List[Any]) -> None:
        return None


class _DraftScopedStatsLookup(dict):
    def __bool__(self) -> bool:
        return True


def _draft_hero_entries(side: Any) -> List[Tuple[str, str]]:
    entries: List[Tuple[str, str]] = []
    if not isinstance(side, dict):
        return entries
    for pos in ("pos1", "pos2", "pos3", "pos4", "pos5"):
        hero_payload = side.get(pos) or {}
        try:
            hero_id = int(hero_payload.get("hero_id"))
        except (TypeError, ValueError):
            continue
        if hero_id > 0:
            entries.append((pos, str(hero_id)))
    return entries


def _draft_group_key_variants(group: Any) -> List[str]:
    parts = str(group or "").split(",")
    if len(parts) <= 1:
        return [parts[0]] if parts and parts[0] else []
    return sorted({",".join(perm) for perm in permutations(parts)})


def _add_draft_with_lookup_keys(keys: set, left: str, right: str) -> None:
    for left_variant in _draft_group_key_variants(left):
        for right_variant in _draft_group_key_variants(right):
            keys.add(f"{left_variant}_with_{right_variant}")
            keys.add(f"{right_variant}_with_{left_variant}")


def _add_draft_vs_lookup_keys(keys: set, left: str, right: str) -> None:
    for left_variant in _draft_group_key_variants(left):
        for right_variant in _draft_group_key_variants(right):
            keys.add(f"{left_variant}_vs_{right_variant}")
            keys.add(f"{right_variant}_vs_{left_variant}")


def _draft_stats_lookup_keys(radiant_heroes_and_pos: Any, dire_heroes_and_pos: Any) -> set:
    """All raw stat keys touched by synergy_and_counterpick for one fixed draft."""
    keys = set()
    radiant_entries = _draft_hero_entries(radiant_heroes_and_pos)
    dire_entries = _draft_hero_entries(dire_heroes_and_pos)

    def hero_key(entry: Tuple[str, str]) -> str:
        pos, hero_id = entry
        return f"{hero_id}{pos}"

    for team_entries in (radiant_entries, dire_entries):
        team_keys = [hero_key(entry) for entry in team_entries]
        keys.update(team_keys)
        for left, right in combinations(team_keys, 2):
            _add_draft_with_lookup_keys(keys, left, right)
        for trio in combinations(team_keys, 3):
            for perm in permutations(trio):
                keys.add(",".join(perm))

    for team_entries, opp_entries in (
        (radiant_entries, dire_entries),
        (dire_entries, radiant_entries),
    ):
        team_keys = [hero_key(entry) for entry in team_entries]
        opp_keys = [hero_key(entry) for entry in opp_entries]
        for left in team_keys:
            for right in opp_keys:
                _add_draft_vs_lookup_keys(keys, left, right)
            for opp_duo in combinations(opp_keys, 2):
                duo_key = ",".join(sorted(opp_duo))
                _add_draft_vs_lookup_keys(keys, left, duo_key)

    return keys


def _prepare_draft_scoped_stats_lookup(
    stats_obj: Any,
    radiant_heroes_and_pos: Any,
    dire_heroes_and_pos: Any,
    draft_lookup_keys: Optional[set] = None,
) -> Any:
    if not STATS_DRAFT_SCOPED_LOOKUP_ENABLED or not isinstance(stats_obj, (_ShardedStatsLookup, _SqliteStatsLookup)):
        return stats_obj
    keys = draft_lookup_keys
    if keys is None:
        keys = _draft_stats_lookup_keys(radiant_heroes_and_pos, dire_heroes_and_pos)
    return _DraftScopedStatsLookup(stats_obj.get_many(keys))


def _stats_sqlite_db_path(source: Path) -> Path:
    return source.parent / f"{source.stem}.sqlite3"


def _stats_expected_meta(source: Path) -> Dict[str, Any]:
    source_stat = source.stat()
    return {
        "format_version": 1,
        "source_name": source.name,
        "source_size": int(source_stat.st_size),
        "source_mtime_ns": int(source_stat.st_mtime_ns),
    }


def _stats_meta_matches(actual: Dict[str, Any], expected: Dict[str, Any]) -> bool:
    return all(actual.get(key) == value for key, value in expected.items())


def _sqlite_stats_meta_matches_source(actual: Dict[str, Any], expected: Dict[str, Any]) -> bool:
    if actual.get("backend") not in {None, "sqlite_kv"}:
        return False
    for key in ("format_version", "source_name", "source_size"):
        if actual.get(key) != expected.get(key):
            return False
    return True


def _sqlite_stats_meta_matches(db_path: Path, expected_meta: Dict[str, Any]) -> bool:
    if not db_path.exists():
        return False
    try:
        uri = f"{db_path.resolve().as_uri()}?mode=ro&immutable=1"
        with sqlite3.connect(uri, uri=True) as conn:
            rows = conn.execute("SELECT key, value FROM meta").fetchall()
        actual = {str(key): orjson.loads(value) for key, value in rows}
        return _sqlite_stats_meta_matches_source(actual, expected_meta)
    except Exception:
        return False


def _stats_shards_match_source(shard_dir: Path, expected_meta: Dict[str, Any]) -> bool:
    meta_path = shard_dir / "_meta.json"
    complete_path = shard_dir / "_complete"
    if not (shard_dir.exists() and meta_path.exists() and complete_path.exists()):
        return False
    try:
        current_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return _stats_meta_matches(current_meta, expected_meta)


def _stats_shard_sort_key(path: Path) -> Tuple[int, Any]:
    try:
        return (0, int(path.stem))
    except ValueError:
        return (1, path.stem)


def _iter_stats_records_for_sqlite(source: Path, shard_dir: Path, expected_meta: Dict[str, Any]):
    if STATS_SQLITE_BUILD_FROM_SHARDS and _stats_shards_match_source(shard_dir, expected_meta):
        print(f"🧱 Building SQLite stats from JSONL shards: {shard_dir}")
        for shard_path in sorted(shard_dir.glob("*.jsonl"), key=_stats_shard_sort_key):
            with shard_path.open("rb") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if not line:
                        continue
                    key, value = orjson.loads(line)
                    yield str(key), orjson.dumps(value)
        return

    if ijson is None:
        raise RuntimeError(f"ijson is required to build SQLite stats from {source}")

    print(f"🧱 Building SQLite stats from JSON source: {source}")
    with source.open("rb") as f:
        for key, value in ijson.kvitems(f, ""):
            yield str(key), orjson.dumps(value)


def _build_sqlite_stats_db(source: Path, db_path: Path, label: str, expected_meta: Dict[str, Any]) -> None:
    shard_dir = source.parent / f"{source.stem}.shards"
    temp_path = db_path.parent / f"{db_path.name}.tmp"
    if temp_path.exists():
        temp_path.unlink()

    entries = 0
    batch: List[Tuple[str, Any]] = []
    batch_size = max(1, int(STATS_SQLITE_BUILD_BATCH_SIZE))
    progress_every = max(0, int(STATS_SQLITE_BUILD_PROGRESS_EVERY))
    print(f"🧱 Building SQLite {label} stats DB: {db_path}")

    conn = sqlite3.connect(str(temp_path))
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA locking_mode=EXCLUSIVE")
        conn.execute("CREATE TABLE stats (key TEXT NOT NULL, value BLOB NOT NULL)")
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID")

        def flush_batch() -> None:
            nonlocal batch
            if not batch:
                return
            conn.executemany("INSERT INTO stats (key, value) VALUES (?, ?)", batch)
            batch = []

        for key, value_blob in _iter_stats_records_for_sqlite(source, shard_dir, expected_meta):
            batch.append((key, sqlite3.Binary(value_blob)))
            entries += 1
            if len(batch) >= batch_size:
                flush_batch()
            if progress_every > 0 and entries % progress_every == 0:
                print(f"   📚 {label} SQLite progress: {entries:,} rows")

        flush_batch()
        print(f"   🔎 Creating SQLite index for {label}: {entries:,} rows")
        conn.execute("CREATE UNIQUE INDEX stats_key_idx ON stats(key)")
        meta_payload = dict(expected_meta)
        meta_payload["backend"] = "sqlite_kv"
        meta_payload["entries"] = int(entries)
        conn.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            [(key, orjson.dumps(value).decode("utf-8")) for key, value in meta_payload.items()],
        )
        conn.commit()
        conn.execute("PRAGMA optimize")
    finally:
        conn.close()

    if db_path.exists():
        db_path.unlink()
    temp_path.rename(db_path)
    print(f"✅ Built SQLite {label} stats DB: {entries:,} rows -> {db_path}")


def _prepare_sqlite_stats_lookup(source_path: str, label: str) -> _SqliteStatsLookup:
    source = Path(source_path)
    expected_meta = _stats_expected_meta(source)
    db_path = _stats_sqlite_db_path(source)
    if not _sqlite_stats_meta_matches(db_path, expected_meta):
        _build_sqlite_stats_db(source, db_path, label, expected_meta)

    print(
        f"🧠 Using SQLite {label} stats backend: {db_path} "
        f"(key_cache={STATS_SHARD_KEY_CACHE_MAX})"
    )
    return _SqliteStatsLookup(
        db_path,
        label=label,
        max_cached_keys=STATS_SHARD_KEY_CACHE_MAX,
    )


def _prepare_sharded_stats_lookup(source_path: str, label: str) -> _ShardedStatsLookup:
    source = Path(source_path)
    shard_dir = source.parent / f"{source.stem}.shards"
    meta_path = shard_dir / "_meta.json"
    complete_path = shard_dir / "_complete"
    expected_meta = _stats_expected_meta(source)

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

    cache_label = "disabled" if STATS_SHARD_CACHE_MAX <= 0 else f"{STATS_SHARD_CACHE_MAX} shards"
    print(f"🧠 Using sharded {label} stats backend: {shard_dir} (cache={cache_label})")
    return _ShardedStatsLookup(
        shard_dir,
        label=label,
        max_cached_shards=STATS_SHARD_CACHE_MAX,
        max_cached_keys=STATS_SHARD_KEY_CACHE_MAX,
    )


def _stats_lookup_backend(label: str) -> str:
    per_label_env = f"STATS_{label.upper()}_LOOKUP_BACKEND"
    raw = str(os.getenv(per_label_env, "") or STATS_LOOKUP_BACKEND).strip().lower()
    if raw in {"", "auto"}:
        return "auto"
    if raw in {"sqlite", "sqlite3", "kv", "sqlite_kv"}:
        return "sqlite"
    if raw in {"jsonl", "shard", "shards", "sharded"}:
        return "jsonl"
    return "auto"


def _stats_indexed_lookup_enabled(label: str) -> bool:
    return _stats_lookup_backend(label) != "auto" or _stats_sharded_mode_enabled(label)


def _prepare_indexed_stats_lookup(source_path: str, label: str):
    backend = _stats_lookup_backend(label)
    if backend == "auto":
        source = Path(source_path)
        db_path = _stats_sqlite_db_path(source)
        try:
            expected_meta = _stats_expected_meta(source)
        except Exception:
            expected_meta = {}
        if expected_meta and _sqlite_stats_meta_matches(db_path, expected_meta):
            print(
                f"🧠 Using SQLite {label} stats backend: {db_path} "
                f"(key_cache={STATS_SHARD_KEY_CACHE_MAX})"
            )
            return _SqliteStatsLookup(
                db_path,
                label=label,
                max_cached_keys=STATS_SHARD_KEY_CACHE_MAX,
            )
        if not STATS_SQLITE_AUTOBUILD:
            print(f"🧠 SQLite {label} stats DB missing/stale; using JSONL shards backend")
            return _prepare_sharded_stats_lookup(source_path, label)

    if backend in {"auto", "sqlite"}:
        try:
            return _prepare_sqlite_stats_lookup(source_path, label)
        except Exception as exc:
            if not STATS_SQLITE_FALLBACK_TO_JSONL:
                raise
            logger.warning(
                "SQLite stats backend failed for %s, falling back to JSONL shards: %s",
                label,
                exc,
            )
            print(f"⚠️ SQLite stats backend failed for {label}: {exc}; falling back to JSONL shards")
    return _prepare_sharded_stats_lookup(source_path, label)


def _warm_draft_stats_shards(radiant_heroes_and_pos: dict, dire_heroes_and_pos: dict) -> None:
    if STATS_DRAFT_SCOPED_LOOKUP_ENABLED:
        return
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
    for stats_obj in (early_dict, late_dict, post_lane_dict):
        if isinstance(stats_obj, (_ShardedStatsLookup, _SqliteStatsLookup)):
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
LATE_PUB_COMEBACK_TABLE_START_SECONDS = (20 * 60) + 30
DELAYED_SIGNAL_POLL_SECONDS = 15
DELAYED_SIGNAL_NO_PROGRESS_TIMEOUT_SECONDS = 2 * 60 * 60
DELAYED_SIGNAL_NO_DATA_TIMEOUT_SECONDS = 4 * 60 * 60
# Networth-gated dispatch rules (target team is resolved by star direction sign).
NETWORTH_GATE_HARD_BLOCK_SECONDS = 4 * 60
NETWORTH_GATE_EARLY_WINDOW_END_SECONDS = 10 * 60
NETWORTH_GATE_4_TO_10_MIN_DIFF = 800.0
NETWORTH_GATE_EARLY_CORE_HIGH_CONFIDENCE_MIN_LEAD = 0.0
NETWORTH_GATE_EARLY_CORE_LOW_WR_MIN_LEAD = 800.0
NETWORTH_GATE_TIER1_EARLY_KILLS_WINDOW_END_SECONDS = 13 * 60
NETWORTH_GATE_TIER1_EARLY_KILLS_4_TO_12_MIN_DIFF = 500.0
NETWORTH_GATE_TIER1_EARLY65_WINDOW_END_SECONDS = 17 * 60
NETWORTH_GATE_TIER1_EARLY65_4_TO_10_MIN_DIFF = 600.0
NETWORTH_GATE_TIER1_EARLY65_10_TO_17_MIN_DIFF = 600.0
NETWORTH_GATE_STRONG_SAME_SIGN_MAX_LOSS = -800.0
NETWORTH_GATE_EARLY_CORE_MONITOR_DIFF = 1500.0
NETWORTH_GATE_LATE_NO_EARLY_DIFF = 2000.0
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
NETWORTH_STATUS_MIN10_TARGET_NONNEGATIVE_SEND = "minute10_target_nonnegative_send"
NETWORTH_STATUS_MIN10_LEAD_GE800_SEND = "minute10_lead_ge800_send"
NETWORTH_STATUS_TIER1_EARLY_KILLS_4_12_SEND_500 = "tier1_early_kills_4_12_send_500"
NETWORTH_STATUS_TIER1_EARLY_KILLS_WINDOW_CLOSED = "tier1_early_kills_window_closed"
NETWORTH_STATUS_TIER1_EARLY65_4_10_SEND_600 = "early65_4_10_send_600"
NETWORTH_STATUS_TIER1_EARLY65_10_17_SEND_600 = "early65_10_17_send_600"
NETWORTH_STATUS_STRONG_SAME_SIGN_MONITOR_WAIT_800 = "strong_same_sign_monitor_wait_800"
NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_NONNEGATIVE = "early_core_monitor_wait_nonnegative"
NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_800 = "early_core_monitor_wait_800"
NETWORTH_STATUS_EARLY_CORE_FALLBACK_20_20_SEND = "early_core_fallback_20_20_send"
NETWORTH_STATUS_EARLY_CORE_TIMEOUT_NO_SEND = "early_core_timeout_no_send"
NETWORTH_STATUS_LATE_CORE_MONITOR_WAIT_800 = "late_core_monitor_wait_800"
NETWORTH_STATUS_LATE_CORE_TIMEOUT_NO_SEND = "late_core_timeout_no_send"
NETWORTH_STATUS_LATE_MONITOR_WAIT_2000 = "late_monitor_wait_2000"
NETWORTH_STATUS_LATE_CONFLICT_WAIT_1500 = "late_conflict_wait_1500"
NETWORTH_STATUS_LATE_CONFLICT_WAIT_2000 = "late_conflict_wait_2000"
NETWORTH_STATUS_LATE_CONFLICT_WAIT_3000 = "late_conflict_wait_3000"
NETWORTH_STATUS_LATE_OPPOSITE_EARLY90_WAIT_20_20 = "late_opposite_early90_wait_20_20"
NETWORTH_STATUS_LATE_OPPOSITE_EARLY90_TIER1_FAST_RELEASE_WAIT = "late_opposite_early90_tier1_fast_release_wait"
NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_WAIT = "late_top25_elo_block_wait_3000"
NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TARGET_LEAD_SEND = "late_top25_elo_block_target_lead_send"
NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_TIMEOUT_NO_SEND = "late_top25_elo_block_timeout_no_send"
NETWORTH_STATUS_LATE_FALLBACK_20_20_SEND = "late_fallback_20_20_send"
NETWORTH_STATUS_LATE_FALLBACK_20_20_DEFICIT_NO_SEND = "late_fallback_20_20_deficit_no_send"
NETWORTH_STATUS_LATE_COMEBACK_MONITOR_WAIT = "late_comeback_monitor_wait"
NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND = "late_comeback_timeout_no_send"
NETWORTH_STATUS_LATE_PUB_TABLE_WAIT = "late_pub_table_wait"
NETWORTH_STATUS_LATE_PUB_TABLE_SEND = "late_pub_table_send"
TIER_SIGNAL_MIN_THRESHOLD_TIER1_BASE = 60
TIER_SIGNAL_MIN_THRESHOLD_TIER2_BASE = 60
ELO_UNDERDOG_GUARD_FAVORITE_EDGE_PP = 15.0
ELO_UNDERDOG_GUARD_MIN_SIGNAL_WR = 70.0
ELO_BLOCK_WR_MIN_AFTER_PENALTY = 58.5
ELO_GUARD_MIN_ABS_DIFF = 30.0
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
    True,
) and BOOKMAKER_PREFETCH_AVAILABLE
BOOKMAKER_PREFETCH_GATE_MODE = _normalize_bookmaker_gate_mode(
    os.getenv("BOOKMAKER_PREFETCH_GATE_MODE", "odds"),
    default="odds",
)
BOOKMAKER_PREFETCH_MODE = str(os.getenv("BOOKMAKER_PREFETCH_MODE", "live")).strip().lower()
if BOOKMAKER_PREFETCH_MODE not in {"live", "all"}:
    BOOKMAKER_PREFETCH_MODE = "live"
BOOKMAKER_PREFETCH_MAX_PENDING = _safe_int_env("BOOKMAKER_PREFETCH_MAX_PENDING", 200)
BOOKMAKER_PREFETCH_RESULT_TTL_SECONDS = _safe_int_env("BOOKMAKER_PREFETCH_RESULT_TTL_SECONDS", 1800)
BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS = _safe_float_env("BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 3.0)
BOOKMAKER_PREFETCH_DRIVER_ROTATE_TASKS = _safe_int_env("BOOKMAKER_PREFETCH_DRIVER_ROTATE_TASKS", 3)
BOOKMAKER_CAMOUFOX_ENABLED = _env_flag("BOOKMAKER_CAMOUFOX_ENABLED", "1")
BOOKMAKER_PREFETCH_USE_SUBPROCESS = _safe_bool_env(
    "BOOKMAKER_PREFETCH_USE_SUBPROCESS",
    BOOKMAKER_CAMOUFOX_ENABLED,
)
BOOKMAKER_PREFETCH_SUBPROCESS_TIMEOUT_SECONDS = _safe_int_env("BOOKMAKER_PREFETCH_SUBPROCESS_TIMEOUT_SECONDS", 160)
BOOKMAKER_MATCH_TAB_CACHE_MAX_MATCHES = _safe_int_env("BOOKMAKER_MATCH_TAB_CACHE_MAX_MATCHES", 8)
SIGNAL_SEND_ADMIN_ONLY = _safe_bool_env("SIGNAL_SEND_ADMIN_ONLY", False)
BOOKMAKER_PREFETCH_SITES_RAW = str(
    os.getenv("BOOKMAKER_PREFETCH_SITES", "betboom,pari,winline")
).strip()
BOOKMAKER_PREFETCH_SITES = tuple(
    s.strip().lower()
    for s in BOOKMAKER_PREFETCH_SITES_RAW.split(",")
    if s.strip()
) or ("betboom", "pari", "winline")

# Pipeline smoke-test mode: keep parsing and calculating every draft, but bypass
# production gates that would normally suppress Telegram/VK dispatch.
PIPELINE_DISABLE_SIGNAL_GATES = _safe_bool_env("PIPELINE_DISABLE_SIGNAL_GATES", False)
PIPELINE_SEND_EVERY_PARSED_MATCH = _safe_bool_env(
    "PIPELINE_SEND_EVERY_PARSED_MATCH",
    PIPELINE_DISABLE_SIGNAL_GATES,
)
PIPELINE_METRICS_PARALLEL_ENABLED = _safe_bool_env("PIPELINE_METRICS_PARALLEL_ENABLED", True)
PIPELINE_BYPASS_BOOKMAKER_GATE = _safe_bool_env(
    "PIPELINE_BYPASS_BOOKMAKER_GATE",
    PIPELINE_DISABLE_SIGNAL_GATES,
)
PIPELINE_BYPASS_TIER_GATE = _safe_bool_env(
    "PIPELINE_BYPASS_TIER_GATE",
    PIPELINE_DISABLE_SIGNAL_GATES,
)
PIPELINE_BYPASS_LEAGUE_DENYLIST_GATE = _safe_bool_env(
    "PIPELINE_BYPASS_LEAGUE_DENYLIST_GATE",
    PIPELINE_DISABLE_SIGNAL_GATES,
)
PIPELINE_BYPASS_PROTRACKER_GATE = _safe_bool_env(
    "PIPELINE_BYPASS_PROTRACKER_GATE",
    PIPELINE_DISABLE_SIGNAL_GATES,
)
PIPELINE_BYPASS_PROCESSED_URL_GATE = _safe_bool_env("PIPELINE_BYPASS_PROCESSED_URL_GATE", False)
PIPELINE_SKIP_BOOKMAKER_PREPARE_ON_SEND = _safe_bool_env(
    "PIPELINE_SKIP_BOOKMAKER_PREPARE_ON_SEND",
    PIPELINE_DISABLE_SIGNAL_GATES,
)


def _apply_live_entrypoint_pipeline_defaults() -> None:
    """Enable the current live smoke-test behavior only for the executable entrypoint."""
    global DOTA2PROTRACKER_ENABLED, DOTA2PROTRACKER_MESSAGE_BLOCK_ENABLED, DOTA2PROTRACKER_SUPERSEDE_OPENDOTA
    global PIPELINE_DISABLE_SIGNAL_GATES, PIPELINE_SEND_EVERY_PARSED_MATCH
    global PIPELINE_BYPASS_BOOKMAKER_GATE, PIPELINE_BYPASS_TIER_GATE, PIPELINE_BYPASS_LEAGUE_DENYLIST_GATE
    global PIPELINE_BYPASS_PROTRACKER_GATE, PIPELINE_SKIP_BOOKMAKER_PREPARE_ON_SEND

    if "DOTA2PROTRACKER_ENABLED" not in os.environ:
        DOTA2PROTRACKER_ENABLED = True
    if DOTA2PROTRACKER_ENABLED:
        if "DOTA2PROTRACKER_MESSAGE_BLOCK_ENABLED" not in os.environ:
            DOTA2PROTRACKER_MESSAGE_BLOCK_ENABLED = True
        if "DOTA2PROTRACKER_SUPERSEDE_OPENDOTA" not in os.environ:
            DOTA2PROTRACKER_SUPERSEDE_OPENDOTA = True

    if "PIPELINE_DISABLE_SIGNAL_GATES" not in os.environ:
        PIPELINE_DISABLE_SIGNAL_GATES = True
    if "PIPELINE_SEND_EVERY_PARSED_MATCH" not in os.environ:
        PIPELINE_SEND_EVERY_PARSED_MATCH = PIPELINE_DISABLE_SIGNAL_GATES
    if "PIPELINE_BYPASS_BOOKMAKER_GATE" not in os.environ:
        PIPELINE_BYPASS_BOOKMAKER_GATE = PIPELINE_DISABLE_SIGNAL_GATES
    if "PIPELINE_BYPASS_TIER_GATE" not in os.environ:
        PIPELINE_BYPASS_TIER_GATE = PIPELINE_DISABLE_SIGNAL_GATES
    if "PIPELINE_BYPASS_LEAGUE_DENYLIST_GATE" not in os.environ:
        PIPELINE_BYPASS_LEAGUE_DENYLIST_GATE = PIPELINE_DISABLE_SIGNAL_GATES
    if "PIPELINE_BYPASS_PROTRACKER_GATE" not in os.environ:
        PIPELINE_BYPASS_PROTRACKER_GATE = PIPELINE_DISABLE_SIGNAL_GATES
    if "PIPELINE_SKIP_BOOKMAKER_PREPARE_ON_SEND" not in os.environ:
        PIPELINE_SKIP_BOOKMAKER_PREPARE_ON_SEND = PIPELINE_DISABLE_SIGNAL_GATES

# Testing helpers:
# - optionally use separate MAP_ID_CHECK_PATH
# - optionally disable add_url persistence to keep matches re-analysed every cycle
LOCAL_STATE_DIR = Path.home() / ".local" / "state" / "ingame"
LEGACY_MAP_ID_CHECK_PATH = PROJECT_ROOT / "map_id_check.txt"
LEGACY_MAP_ID_CHECK_PATH_ODDS = PROJECT_ROOT / "map_id_check_test.txt"
DEFAULT_MAP_ID_CHECK_PATH = LOCAL_STATE_DIR / "map_id_check.txt"
DEFAULT_MAP_ID_CHECK_PATH_ODDS = LOCAL_STATE_DIR / "map_id_check_test.txt"
DEFAULT_ADMIN_TAIL_LOG_SEEN_MATCHES_PATH = LOCAL_STATE_DIR / "admin_tail_log_seen_matches.json"
MAP_ID_CHECK_PATH = str(
    Path(
        str(os.getenv("MAP_ID_CHECK_PATH", str(DEFAULT_MAP_ID_CHECK_PATH))).strip()
        or str(DEFAULT_MAP_ID_CHECK_PATH)
    ).expanduser()
)
MAP_ID_CHECK_PATH_ODDS_DEFAULT = str(DEFAULT_MAP_ID_CHECK_PATH_ODDS)
ADMIN_TAIL_LOG_SEEN_MATCHES_PATH = str(
    Path(
        str(
            os.getenv(
                "ADMIN_TAIL_LOG_SEEN_MATCHES_PATH",
                str(DEFAULT_ADMIN_TAIL_LOG_SEEN_MATCHES_PATH),
            )
        ).strip()
        or str(DEFAULT_ADMIN_TAIL_LOG_SEEN_MATCHES_PATH)
    ).expanduser()
)
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
LIVE_LANE_ANALYSIS_ENABLED = _safe_bool_env("LIVE_LANE_ANALYSIS_ENABLED", True)

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

    thresholds_by_level: Dict[int, Dict[str, int]] = {}
    for level in sorted(set(available_levels)):
        thresholds = STAR_THRESHOLDS_BY_WR.get(level, {}).get(section, [])
        if not thresholds:
            continue
        threshold_map: Dict[str, int] = {}
        for metric, threshold in thresholds:
            try:
                compact_metric = str(metric)
                compact_threshold = int(threshold)
            except (TypeError, ValueError):
                continue
            if compact_threshold < 1:
                compact_threshold = 1
            threshold_map[compact_metric] = compact_threshold
        if threshold_map:
            thresholds_by_level[int(level)] = threshold_map

    best_level = None
    for metric, value in star_only_data.items():
        metric_best_level = None
        metric_max_threshold = 0
        abs_value = abs(value)
        for level in sorted(thresholds_by_level):
            threshold = thresholds_by_level[level].get(metric)
            if threshold is None:
                continue
            # Если порог для более высокого WR не вырос, не повышаем confidence.
            # Это устраняет скачки вида 7* -> WR60, 8* -> WR90 при плато таблицы.
            if threshold <= metric_max_threshold:
                continue
            metric_max_threshold = int(threshold)
            if abs_value >= metric_max_threshold:
                metric_best_level = int(level)
        if metric_best_level is None:
            continue
        if best_level is None or metric_best_level > best_level:
            best_level = metric_best_level
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
    "pos1_vs_pos1",
    "counterpick_1vs2",
    "solo",
)
_STAR_LATE_CORE_MIN_ABS_BY_METRIC = {
    "pos1_vs_pos1": 5.0,
}
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
            metric_name = str(metric)
            if metric_name in STAR_DISABLED_METRICS:
                continue
            out[metric_name] = int(threshold)
        except (TypeError, ValueError):
            continue
    return out


def _format_metric_value(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.1f}"


def _build_lane_block(top: Any, mid: Any, bot: Any) -> str:
    top_line = str(top or "").strip()
    mid_line = str(mid or "").strip()
    bot_line = str(bot or "").strip()
    lane_lines = [line for line in (top_line, mid_line, bot_line) if line]
    if not lane_lines:
        return ""
    return "Lanes:\n" + "\n".join(lane_lines) + "\n\n"


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
    if len(hit_metrics) < 2:
        return {
            "valid": False,
            "status": "insufficient_hits",
            "sign": block_sign,
            "hit_metrics": hit_metrics,
            "conflict_metric": None,
            "hit_count": len(hit_metrics),
            "min_hit_count_required": 2,
        }
    return {
        "valid": block_sign in (-1, 1),
        "status": "ok" if block_sign in (-1, 1) else "no_sign",
        "sign": block_sign,
        "hit_metrics": hit_metrics,
        "conflict_metric": None,
        "support_status": None,
        "support_nonzero_metrics": [],
        "support_conflicting_metrics": [],
        "support_zero_metrics": [],
        "support_missing_metrics": [],
    }


def _star_hit_conflicts_with_expected_sign(
    raw_block: Optional[dict],
    *,
    target_wr: int,
    section: str,
    expected_sign: Optional[int],
) -> Dict[str, Any]:
    if expected_sign not in (-1, 1):
        return {
            "valid": False,
            "status": "no_expected_sign",
            "hit_metrics": [],
            "conflicting_hit_metrics": [],
        }

    block = raw_block if isinstance(raw_block, dict) else {}
    thresholds = _star_thresholds_for_wr(target_wr, section)
    hit_metrics: List[str] = []
    conflicting_hit_metrics: List[str] = []

    for metric, threshold in thresholds.items():
        value = _coerce_metric_value(block.get(metric))
        if value is None or abs(value) < threshold:
            continue
        hit_metrics.append(metric)
        sign = 1 if value > 0 else -1
        if sign != expected_sign:
            conflicting_hit_metrics.append(metric)

    return {
        "valid": len(conflicting_hit_metrics) == 0,
        "status": "ok" if len(conflicting_hit_metrics) == 0 else "conflict_hits_against_expected_sign",
        "hit_metrics": hit_metrics,
        "conflicting_hit_metrics": conflicting_hit_metrics,
    }


def _build_no_late_star_late_block_guard(
    raw_block: Optional[dict],
    *,
    expected_sign: Optional[int],
    target_wr: int,
    section: str = "mid_output",
) -> Dict[str, Any]:
    core_same_sign_diag = _block_signs_same_or_zero(
        raw_block=raw_block,
        expected_sign=expected_sign,
        metrics=_STAR_LATE_CORE_METRIC_ORDER,
        allow_zero=False,
        min_abs_by_metric=_STAR_LATE_CORE_MIN_ABS_BY_METRIC,
    )
    star_hit_diag = _star_hit_conflicts_with_expected_sign(
        raw_block,
        target_wr=target_wr,
        section=section,
        expected_sign=expected_sign,
    )
    return {
        "expected_sign": expected_sign,
        "core_same_sign_diag": core_same_sign_diag,
        "core_same_sign_support": bool(
            core_same_sign_diag.get("valid")
            and core_same_sign_diag.get("nonzero_metrics")
        ),
        "star_hit_diag": star_hit_diag,
    }


def _early_star_no_late_same_sign_gate(
    *,
    selected_early_diag: Optional[Dict[str, Any]],
    has_selected_early_star: bool,
    has_selected_late_star: bool,
    early_wr_pct: Optional[float],
    late_min_wr_diag: Optional[Dict[str, Any]] = None,
    both_teams_tier1: bool = False,
) -> Dict[str, Any]:
    early_diag = selected_early_diag if isinstance(selected_early_diag, dict) else {}
    late_diag = late_min_wr_diag if isinstance(late_min_wr_diag, dict) else {}
    early_hit_metrics = list(early_diag.get("hit_metrics") or [])
    early_hit_count = len(early_hit_metrics)
    try:
        early_wr_value = float(early_wr_pct) if early_wr_pct is not None else None
    except (TypeError, ValueError):
        early_wr_value = None

    min_wr_ok = bool(early_wr_value is not None and early_wr_value >= 70.0)
    min_hit_count_ok = bool(early_hit_count >= 2)
    late_min_wr_has_star = bool(late_diag.get("valid"))
    tier1_only_ok = bool(both_teams_tier1)
    valid = bool(
        has_selected_early_star
        and not has_selected_late_star
        and not late_min_wr_has_star
        and min_wr_ok
        and min_hit_count_ok
        and tier1_only_ok
    )
    return {
        "valid": valid,
        "early_wr_pct": float(early_wr_value) if early_wr_value is not None else None,
        "min_wr_required": 70.0,
        "min_wr_ok": min_wr_ok,
        "early_hit_metrics": early_hit_metrics,
        "early_hit_count": early_hit_count,
        "min_hit_count_required": 2,
        "min_hit_count_ok": min_hit_count_ok,
        "late_min_wr_required": 60,
        "late_min_wr_status": str(late_diag.get("status") or "unknown"),
        "late_min_wr_has_star": late_min_wr_has_star,
        "late_min_wr_hit_metrics": list(late_diag.get("hit_metrics") or []),
        "late_min_wr_hit_count": len(late_diag.get("hit_metrics") or []),
        "both_teams_tier1": bool(both_teams_tier1),
        "tier1_only_ok": tier1_only_ok,
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
                f"elo_wr_below_min60(after_penalty={float(adjusted_wr_pct):.1f},"
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


def _star_diag_target_side(diag: Dict[str, Any]) -> Optional[str]:
    if not isinstance(diag, dict):
        return None
    side = _target_side_from_sign(diag.get("sign"))
    if side in {"radiant", "dire"}:
        return side
    elo_side = str(diag.get("elo_target_side") or "").strip()
    if elo_side in {"radiant", "dire"}:
        return elo_side
    return None


def _format_star_block_status_with_side(diag: Dict[str, Any]) -> str:
    status = _format_star_block_status(diag)
    side = _star_diag_target_side(diag) or "none"
    return f"{status}(side={side})"


def _dispatch_mode_reason_label(dispatch_mode: Optional[str]) -> str:
    mode = str(dispatch_mode or "").strip()
    mapping = {
        "immediate_force_odds_signal_test": "force_odds_signal_test",
        "immediate_early_late_same_sign": "same_sign,both_star",
        "immediate_early_star_late_core_same_sign": "early_star,no_valid_late",
        "immediate_late_star_early_core_same_sign": "same_sign,late_star+early_core",
        "immediate_early_star65": "early65_gate",
        "delayed_late_only_20_20m": "late_only,20_20_monitor",
        "delayed_late_elo_block_top25_opposite_monitor": "opposite_signs,top25_late_elo_block_monitor",
    }
    return mapping.get(mode, mode or "unknown")


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
    min_abs_by_metric: Optional[Dict[str, float]] = None,
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
        min_abs = None
        if isinstance(min_abs_by_metric, dict):
            try:
                min_abs = float(min_abs_by_metric.get(metric)) if metric in min_abs_by_metric else None
            except (TypeError, ValueError):
                min_abs = None
        if min_abs is not None and abs(value) < min_abs:
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
    if reason in {"late_only_no_early_star_wait_2000", "late_only_no_early_same_sign"}:
        return abs(float(NETWORTH_GATE_LATE_NO_EARLY_DIFF))
    if reason == "late_only_opposite_signs":
        return abs(float(NETWORTH_GATE_LATE_OPPOSITE_DIFF))
    if reason == "early_star_late_core_wait_nonnegative":
        return abs(float(NETWORTH_GATE_EARLY_CORE_HIGH_CONFIDENCE_MIN_LEAD))
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


def _late_star_pub_table_wr_level(late_wr_pct: Optional[float]) -> Optional[int]:
    try:
        wr_value = float(late_wr_pct) if late_wr_pct is not None else None
    except (TypeError, ValueError):
        wr_value = None
    if wr_value is None:
        return None
    candidate_levels = [60, 65, 70, 75, 80, 85, 90]
    return min(candidate_levels, key=lambda level: (abs(level - wr_value), level))


def _late_star_pub_table_decision(
    *,
    wr_level: Optional[int],
    game_time_seconds: Any,
    target_networth_diff: Optional[float],
) -> Dict[str, Any]:
    try:
        current_game_time = float(game_time_seconds) if game_time_seconds is not None else None
    except (TypeError, ValueError):
        current_game_time = None
    try:
        target_diff = float(target_networth_diff) if target_networth_diff is not None else None
    except (TypeError, ValueError):
        target_diff = None

    result: Dict[str, Any] = {
        "available": False,
        "ready": False,
        "wr_level": int(wr_level) if wr_level is not None else None,
        "current_minute": None,
        "source_minute": None,
        "threshold": None,
        "target_diff": target_diff,
    }

    if current_game_time is None:
        return result

    current_minute = int(max(0.0, current_game_time) // 60)
    result["current_minute"] = current_minute
    if current_game_time < float(LATE_PUB_COMEBACK_TABLE_START_SECONDS):
        return result

    if not isinstance(late_pub_comeback_table_thresholds_by_wr, dict):
        return result
    try:
        normalized_wr = int(wr_level) if wr_level is not None else None
    except (TypeError, ValueError):
        normalized_wr = None
    if normalized_wr is None:
        return result

    wr_thresholds = late_pub_comeback_table_thresholds_by_wr.get(normalized_wr)
    if not isinstance(wr_thresholds, dict) or not wr_thresholds:
        return result

    available_minutes = sorted(int(minute) for minute in wr_thresholds.keys())
    eligible_minutes = [minute for minute in available_minutes if minute <= current_minute]
    if not eligible_minutes:
        return result

    source_minute = max(eligible_minutes)
    threshold = wr_thresholds.get(source_minute)
    try:
        threshold_value = float(threshold) if threshold is not None else None
    except (TypeError, ValueError):
        threshold_value = None
    if threshold_value is None:
        return result

    result["available"] = True
    result["source_minute"] = source_minute
    result["threshold"] = threshold_value
    result["ready"] = bool(target_diff is not None and float(target_diff) >= float(threshold_value))
    return result


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
    return None


def _team_elo_wr_for_side(
    team_elo_meta: Optional[Dict[str, Any]],
    side: Optional[str],
) -> Optional[float]:
    if not isinstance(team_elo_meta, dict):
        return None
    if side == "radiant":
        keys = ("raw_radiant_wr", "adjusted_radiant_wr")
    elif side == "dire":
        keys = ("raw_dire_wr", "adjusted_dire_wr")
    else:
        return None
    for key in keys:
        try:
            return float(team_elo_meta.get(key))
        except (TypeError, ValueError):
            continue
    return None


def _team_elo_abs_diff_for_guard(team_elo_meta: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(team_elo_meta, dict):
        return None
    for key in ("raw_diff", "adjusted_diff", "elo_diff", "team_elo_diff"):
        try:
            return abs(float(team_elo_meta.get(key)))
        except (TypeError, ValueError):
            continue
    return None


def _team_elo_base_rating_for_side(
    team_elo_meta: Optional[Dict[str, Any]],
    side: Optional[str],
) -> Optional[float]:
    if not isinstance(team_elo_meta, dict):
        return None
    if side == "radiant":
        key = "radiant_base_rating"
    elif side == "dire":
        key = "dire_base_rating"
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
    abs_diff = _team_elo_abs_diff_for_guard(team_elo_meta)
    if abs_diff is not None and abs_diff < float(ELO_GUARD_MIN_ABS_DIFF):
        return 0.0
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
        "target_game_time": float(LATE_PUB_COMEBACK_TABLE_START_SECONDS),
        "dispatch_status_label": NETWORTH_STATUS_LATE_OPPOSITE_EARLY90_WAIT_20_20,
        "send_on_target_game_time": False,
        "wait_until_target_then_post_target_comeback": True,
    }


def _opposite_signs_early90_tier1_fast_release_config(
    *,
    team_elo_meta: Optional[Dict[str, Any]],
    early_wr_pct: Optional[float],
    selected_early_sign: Optional[int],
    selected_late_sign: Optional[int],
    radiant_team_id: Optional[int],
    dire_team_id: Optional[int],
) -> Optional[Dict[str, Any]]:
    del team_elo_meta
    del early_wr_pct
    del selected_early_sign
    del selected_late_sign
    del radiant_team_id
    del dire_team_id
    return None


def _stake_multiplier_for_signal(
    *,
    team_elo_meta: Optional[Dict[str, Any]],
    target_side: Optional[str],
    selected_early_sign: Optional[int],
    selected_late_sign: Optional[int],
    has_selected_early_star: bool,
    has_selected_late_star: bool,
    early_wr_pct: Optional[float],
    late_wr_pct: Optional[float],
    game_time_seconds: Optional[float],
    radiant_lead: Optional[float],
    late_star_hit_count: Optional[int] = None,
    target_rating: Optional[float] = None,
    opposite_rating: Optional[float] = None,
    target_elo_wr: Optional[float] = None,
    force_half_due_to_early_no_valid_late: bool = False,
) -> float:
    if target_side not in {"radiant", "dire"}:
        return 1

    early_side = _target_side_from_sign(selected_early_sign)
    late_side = _target_side_from_sign(selected_late_sign)
    dispatch_side = (
        late_side
        if late_side in {"radiant", "dire"}
        else early_side
        if early_side in {"radiant", "dire"}
        else None
    )
    if dispatch_side not in {"radiant", "dire"}:
        return 1
    if dispatch_side != target_side:
        return 1
    if force_half_due_to_early_no_valid_late:
        return 0.5

    try:
        early_wr_value = float(early_wr_pct) if early_wr_pct is not None else None
    except (TypeError, ValueError):
        early_wr_value = None
    try:
        late_wr_value = float(late_wr_pct) if late_wr_pct is not None else None
    except (TypeError, ValueError):
        late_wr_value = None

    try:
        late_star_hit_count_value = int(late_star_hit_count) if late_star_hit_count is not None else None
    except (TypeError, ValueError):
        late_star_hit_count_value = None
    if not has_selected_late_star:
        late_star_hit_count_value = 0

    if late_star_hit_count_value is not None and late_star_hit_count_value <= 1:
        return 0.5

    if not has_selected_late_star or late_wr_value is None:
        return 1

    target_rating_value: Optional[float] = None
    opposite_rating_value: Optional[float] = None
    try:
        target_rating_value = float(target_rating) if target_rating is not None else None
    except (TypeError, ValueError):
        target_rating_value = None
    try:
        opposite_rating_value = float(opposite_rating) if opposite_rating is not None else None
    except (TypeError, ValueError):
        opposite_rating_value = None
    if target_rating_value is None:
        inferred_target_rating = _team_elo_base_rating_for_side(team_elo_meta, target_side)
        try:
            target_rating_value = float(inferred_target_rating) if inferred_target_rating is not None else None
        except (TypeError, ValueError):
            target_rating_value = None
    if opposite_rating_value is None:
        opposite_side = "dire" if target_side == "radiant" else "radiant"
        inferred_opposite_rating = _team_elo_base_rating_for_side(team_elo_meta, opposite_side)
        try:
            opposite_rating_value = float(inferred_opposite_rating) if inferred_opposite_rating is not None else None
        except (TypeError, ValueError):
            opposite_rating_value = None

    target_not_too_far_behind_by_elo: Optional[bool] = None
    if target_rating_value is not None and opposite_rating_value is not None:
        target_not_too_far_behind_by_elo = (
            float(target_rating_value) >= (float(opposite_rating_value) - 50.0)
        )

    if (
        late_star_hit_count_value is not None
        and late_star_hit_count_value >= 2
        and target_not_too_far_behind_by_elo is True
    ):
        if late_wr_value >= 85.0:
            return 3
        if late_wr_value >= 70.0:
            return 2

    try:
        target_elo_wr_value = float(target_elo_wr) if target_elo_wr is not None else None
    except (TypeError, ValueError):
        target_elo_wr_value = None
    if target_elo_wr_value is None:
        inferred_target_elo_wr = _team_elo_wr_for_side(team_elo_meta, target_side)
        try:
            target_elo_wr_value = (
                float(inferred_target_elo_wr)
                if inferred_target_elo_wr is not None
                else None
            )
        except (TypeError, ValueError):
            target_elo_wr_value = None
    if target_elo_wr_value is None and target_rating is not None and opposite_rating is not None:
        try:
            target_elo_wr_value = (
                float(_elo_probability_from_ratings(float(target_rating), float(opposite_rating))) * 100.0
            )
        except (TypeError, ValueError):
            target_elo_wr_value = None
    opposite_elo_wr_value: Optional[float] = None
    if target_elo_wr_value is not None:
        opposite_side = "dire" if target_side == "radiant" else "radiant"
        opposite_elo_wr = _team_elo_wr_for_side(team_elo_meta, opposite_side)
        try:
            opposite_elo_wr_value = (
                float(opposite_elo_wr)
                if opposite_elo_wr is not None
                else 100.0 - float(target_elo_wr_value)
            )
        except (TypeError, ValueError):
            opposite_elo_wr_value = None

    if (
        late_wr_value is not None
        and target_elo_wr_value is not None
        and opposite_elo_wr_value is not None
    ):
        elo_wr_diff_value = float(target_elo_wr_value) - float(opposite_elo_wr_value)
        late_elo_gate_score = float(late_wr_value) + float(elo_wr_diff_value)
        if elo_wr_diff_value <= -5.0 and late_elo_gate_score < 59.0:
            return 0.5

    return 1


def _build_stake_multiplier_context(
    *,
    stake_team_name: str,
    target_side: Optional[str],
    team_elo_meta: Optional[Dict[str, Any]],
    radiant_team_name: Optional[str],
    dire_team_name: Optional[str],
    selected_early_sign: Optional[int],
    selected_late_sign: Optional[int],
    has_selected_early_star: bool,
    has_selected_late_star: bool,
    early_wr_pct: Optional[float],
    late_wr_pct: Optional[float],
    late_star_hit_count: Optional[int],
    force_half_due_to_early_no_valid_late: bool = False,
    special_header_mode: str = "",
) -> Dict[str, Any]:
    opposite_side = "dire" if target_side == "radiant" else "radiant"
    return {
        "stake_team_name": str(stake_team_name or ""),
        "target_side": target_side,
        "radiant_team_name": str(radiant_team_name or ""),
        "dire_team_name": str(dire_team_name or ""),
        "selected_early_sign": selected_early_sign,
        "selected_late_sign": selected_late_sign,
        "has_selected_early_star": bool(has_selected_early_star),
        "has_selected_late_star": bool(has_selected_late_star),
        "early_wr_pct": float(early_wr_pct) if early_wr_pct is not None else None,
        "late_wr_pct": float(late_wr_pct) if late_wr_pct is not None else None,
        "late_star_hit_count": int(late_star_hit_count) if late_star_hit_count is not None else None,
        "force_half_due_to_early_no_valid_late": bool(force_half_due_to_early_no_valid_late),
        "special_header_mode": str(special_header_mode or ""),
        "target_rating": _team_elo_base_rating_for_side(team_elo_meta, target_side),
        "opposite_rating": _team_elo_base_rating_for_side(team_elo_meta, opposite_side),
        "target_elo_wr": _team_elo_wr_for_side(team_elo_meta, target_side),
    }


def _format_stake_multiplier_label(multiplier: Optional[float]) -> str:
    try:
        value = float(multiplier)
    except (TypeError, ValueError):
        value = 1.0
    if value.is_integer():
        return str(int(value))
    return f"{value:g}"


def _format_signal_header(
    *,
    stake_team_name: str,
    stake_multiplier: Optional[float],
    special_header_mode: str = "",
) -> str:
    team_name = str(stake_team_name or "").strip() or "НЕИЗВЕСТНАЯ КОМАНДА"
    if special_header_mode == "early_kills":
        return f"СТАВКА НА Ранние килы {team_name}"
    return f"СТАВКА НА {team_name} x{_format_stake_multiplier_label(stake_multiplier)}"


def _blank_dota2protracker_result() -> Dict[str, Any]:
    return {
        "pro_cp1vs1_early": 0.0,
        "pro_cp1vs1_late": 0.0,
        "pro_duo_synergy_early": 0.0,
        "pro_duo_synergy_late": 0.0,
        "pro_cp1vs1_early_games": 0,
        "pro_cp1vs1_late_games": 0,
        "pro_duo_synergy_early_games": 0,
        "pro_duo_synergy_late_games": 0,
        "pro_cp1vs1_valid": False,
        "pro_duo_synergy_valid": False,
        "pro_cp1vs1_reason": "not_computed",
        "pro_duo_synergy_reason": "not_computed",
        "pro_cp1vs1_diagnostics": {},
        "pro_duo_synergy_diagnostics": {},
    }


def _format_dota2protracker_value(value: Any) -> str:
    try:
        return f"{float(value):+,.2f}".replace(",", "")
    except (TypeError, ValueError):
        return "+0.00"


def _dota2protracker_metric_is_valid(
    protracker_payload: Optional[Dict[str, Any]],
    metric_name: str,
) -> bool:
    if not isinstance(protracker_payload, dict):
        return False
    if metric_name == "cp1vs1":
        return bool(protracker_payload.get("pro_cp1vs1_valid"))
    if metric_name == "synergy_duo":
        return bool(protracker_payload.get("pro_duo_synergy_valid"))
    return False


def _has_valid_dota2protracker_signal(protracker_payload: Optional[Dict[str, Any]]) -> bool:
    return (
        _dota2protracker_metric_is_valid(protracker_payload, "cp1vs1")
        or _dota2protracker_metric_is_valid(protracker_payload, "synergy_duo")
    )


def _has_dispatchable_dota2protracker_signal(
    protracker_payload: Optional[Dict[str, Any]],
) -> bool:
    if not isinstance(protracker_payload, dict):
        return False
    cp_valid = bool(protracker_payload.get("pro_cp1vs1_valid"))
    duo_valid = bool(protracker_payload.get("pro_duo_synergy_valid"))
    cp_value = float(protracker_payload.get("pro_cp1vs1_late", protracker_payload.get("pro_cp1vs1_early", 0.0)) or 0.0)
    duo_value = float(
        protracker_payload.get("pro_duo_synergy_late", protracker_payload.get("pro_duo_synergy_early", 0.0)) or 0.0
    )
    return (
        (cp_valid and abs(cp_value) >= DOTA2PROTRACKER_CP1VS1_GATE_ABS)
        or (duo_valid and abs(duo_value) >= DOTA2PROTRACKER_DUO_GATE_ABS)
    )


def _build_dota2protracker_gate_summary(
    protracker_payload: Optional[Dict[str, Any]],
) -> str:
    payload = dict(_blank_dota2protracker_result())
    if isinstance(protracker_payload, dict):
        payload.update(protracker_payload)
    cp_valid = bool(payload.get("pro_cp1vs1_valid"))
    duo_valid = bool(payload.get("pro_duo_synergy_valid"))
    cp_value = float(payload.get("pro_cp1vs1_late", payload.get("pro_cp1vs1_early", 0.0)) or 0.0)
    duo_value = float(payload.get("pro_duo_synergy_late", payload.get("pro_duo_synergy_early", 0.0)) or 0.0)
    cp_pass = cp_valid and abs(cp_value) >= DOTA2PROTRACKER_CP1VS1_GATE_ABS
    duo_pass = duo_valid and abs(duo_value) >= DOTA2PROTRACKER_DUO_GATE_ABS
    return (
        "gate="
        f"cp(abs>={DOTA2PROTRACKER_CP1VS1_GATE_ABS:g}, valid={cp_valid}, pass={cp_pass}), "
        f"duo(abs>={DOTA2PROTRACKER_DUO_GATE_ABS:g}, valid={duo_valid}, pass={duo_pass})"
    )


def _format_dota2protracker_metric(
    *,
    value: Any,
    valid: bool,
) -> str:
    if not valid:
        return "invalid"
    return _format_dota2protracker_value(value)


def _build_dota2protracker_debug_summary(
    protracker_payload: Optional[Dict[str, Any]],
) -> str:
    payload = dict(_blank_dota2protracker_result())
    if isinstance(protracker_payload, dict):
        payload.update(protracker_payload)
    cp_valid = bool(payload.get("pro_cp1vs1_valid"))
    duo_valid = bool(payload.get("pro_duo_synergy_valid"))
    cp_reason = str(payload.get("pro_cp1vs1_reason") or "unknown")
    duo_reason = str(payload.get("pro_duo_synergy_reason") or "unknown")
    cp_diag = payload.get("pro_cp1vs1_diagnostics") or {}
    duo_diag = payload.get("pro_duo_synergy_diagnostics") or {}
    cp_value = payload.get("pro_cp1vs1_late", payload.get("pro_cp1vs1_early", 0.0))
    duo_value = payload.get("pro_duo_synergy_late", payload.get("pro_duo_synergy_early", 0.0))
    return (
        "cp1vs1="
        f"{_format_dota2protracker_metric(value=cp_value, valid=cp_valid)} "
        f"(valid={cp_valid}, reason={cp_reason}, diag={cp_diag}), "
        "duo_synergy="
        f"{_format_dota2protracker_metric(value=duo_value, valid=duo_valid)} "
        f"(valid={duo_valid}, reason={duo_reason}, diag={duo_diag})"
    )


def _build_dota2protracker_log_lines(
    protracker_payload: Optional[Dict[str, Any]],
) -> List[str]:
    payload = dict(_blank_dota2protracker_result())
    if isinstance(protracker_payload, dict):
        payload.update(protracker_payload)

    cp_valid = bool(payload.get("pro_cp1vs1_valid"))
    duo_valid = bool(payload.get("pro_duo_synergy_valid"))
    cp_reason = str(payload.get("pro_cp1vs1_reason") or "unknown")
    duo_reason = str(payload.get("pro_duo_synergy_reason") or "unknown")
    cp_value = payload.get("pro_cp1vs1_late", payload.get("pro_cp1vs1_early", 0.0))
    duo_value = payload.get("pro_duo_synergy_late", payload.get("pro_duo_synergy_early", 0.0))
    cp_games = int(payload.get("pro_cp1vs1_late_games", payload.get("pro_cp1vs1_early_games", 0)) or 0)
    duo_games = int(payload.get("pro_duo_synergy_late_games", payload.get("pro_duo_synergy_early_games", 0)) or 0)
    cp_diag = payload.get("pro_cp1vs1_diagnostics") or {}
    duo_diag = payload.get("pro_duo_synergy_diagnostics") or {}

    return [
        "   📊 Dota2ProTracker:",
        "      cp1vs1: "
        f"{_format_dota2protracker_metric(value=cp_value, valid=cp_valid)} "
        f"(valid={cp_valid}, games={cp_games}, reason={cp_reason}, diag={cp_diag})",
        "      synergy_duo: "
        f"{_format_dota2protracker_metric(value=duo_value, valid=duo_valid)} "
        f"(valid={duo_valid}, games={duo_games}, reason={duo_reason}, diag={duo_diag})",
    ]


def _build_series_score_line(live_league: Optional[Dict[str, Any]]) -> str:
    try:
        live_league = live_league or {}
        r_wins = live_league.get('radiant_series_wins')
        d_wins = live_league.get('dire_series_wins')
        if r_wins is not None or d_wins is not None:
            r_wins = int(r_wins or 0)
            d_wins = int(d_wins or 0)
            return f"{r_wins}-{d_wins}\n"
    except Exception:
        pass
    return ""


def _build_series_score_line_with_fallback(
    live_league: Optional[Dict[str, Any]],
    fallback_score_text: str = "",
) -> str:
    score_line = _build_series_score_line(live_league)
    if score_line:
        return score_line
    score_text = str(fallback_score_text or "").strip()
    if not score_text:
        return ""
    normalized = re.sub(r"\s*:\s*", "-", score_text)
    return f"{normalized}\n"


def _build_dota2protracker_block(protracker_payload: Optional[Dict[str, Any]]) -> str:
    payload = dict(_blank_dota2protracker_result())
    if isinstance(protracker_payload, dict):
        payload.update(protracker_payload)
    cp_value = payload.get("pro_cp1vs1_late", payload.get("pro_cp1vs1_early", 0.0))
    duo_value = payload.get("pro_duo_synergy_late", payload.get("pro_duo_synergy_early", 0.0))
    cp_valid = bool(payload.get("pro_cp1vs1_valid"))
    duo_valid = bool(payload.get("pro_duo_synergy_valid"))

    return (
        "\ndota2protracker:\n"
        f"cp1vs1: {_format_dota2protracker_metric(value=cp_value, valid=cp_valid)}\n"
        f"synergy_duo: {_format_dota2protracker_metric(value=duo_value, valid=duo_valid)}\n"
    )


def _build_dota2protracker_lane_adv_line(protracker_payload: Optional[Dict[str, Any]]) -> str:
    payload = dict(_blank_dota2protracker_result())
    if isinstance(protracker_payload, dict):
        payload.update(protracker_payload)
    lane_adv = payload.get("pro_lane_advantage", 0.0)
    has_lane_data = any(
        bool(payload.get(key))
        for key in (
            "pro_lane_mid_cp1vs1_valid",
            "pro_lane_top_cp1vs1_valid",
            "pro_lane_bot_cp1vs1_valid",
            "pro_lane_top_duo_valid",
            "pro_lane_bot_duo_valid",
        )
    )
    if not has_lane_data:
        return ""
    try:
        return f"lane_adv: {float(lane_adv):+.2f}\n"
    except (TypeError, ValueError):
        return ""


def _build_dota2protracker_only_message(
    *,
    radiant_team_name: str,
    dire_team_name: str,
    live_league: Optional[Dict[str, Any]],
    protracker_payload: Optional[Dict[str, Any]],
) -> str:
    return (
        "DOTA2PROTRACKER\n"
        f"{radiant_team_name} VS {dire_team_name}\n"
        f"{_build_series_score_line(live_league)}"
        f"{_build_dota2protracker_block(protracker_payload)}"
    )


_PIPELINE_PROBE_METRICS = (
    "counterpick_1vs1",
    "pos1_vs_pos1",
    "counterpick_1vs2",
    "solo",
    "synergy_duo",
    "synergy_trio",
)


def _format_pipeline_probe_value(value: Any) -> str:
    try:
        return f"{float(value):+.2f}"
    except (TypeError, ValueError):
        text = str(value or "").strip()
        return text if text else "N/A"


def _format_pipeline_probe_phase_block(title: str, data: Optional[Dict[str, Any]]) -> str:
    block = data if isinstance(data, dict) else {}
    lines: List[str] = []
    for metric in _PIPELINE_PROBE_METRICS:
        if metric not in block:
            continue
        games = block.get(f"{metric}_games")
        games_suffix = ""
        if metric != "solo":
            try:
                games_value = int(games)
                if games_value > 0:
                    games_suffix = f" ({games_value} games)"
            except (TypeError, ValueError):
                pass
        lines.append(f"{metric}: {_format_pipeline_probe_value(block.get(metric))}{games_suffix}")
    if not lines:
        lines.append("no covered metrics")
    return f"{title}:\n" + "\n".join(lines) + "\n"


def _format_pipeline_probe_draft_side(side: Optional[Dict[str, Any]]) -> str:
    side = side if isinstance(side, dict) else {}
    chunks: List[str] = []
    for pos in ("pos1", "pos2", "pos3", "pos4", "pos5"):
        payload = side.get(pos) or {}
        hero_id_raw = payload.get("hero_id") if isinstance(payload, dict) else None
        try:
            hero_id = int(hero_id_raw or 0)
        except (TypeError, ValueError):
            hero_id = 0
        hero_name = ""
        if isinstance(payload, dict):
            hero_name = str(
                payload.get("hero_name")
                or payload.get("_hero_name")
                or payload.get("name")
                or ""
            ).strip()
        if not hero_name and hero_id > 0:
            hero_name = str(HERO_ID_TO_NAME.get(str(hero_id)) or HERO_ID_TO_NAME.get(hero_id) or "").strip()
        hero_label = hero_name or f"hero{hero_id or '?'}"
        chunks.append(f"{pos}:{hero_label}({hero_id or '?'})")
    return ", ".join(chunks)


def _build_pipeline_probe_message(
    *,
    radiant_team_name: str,
    dire_team_name: str,
    live_league: Optional[Dict[str, Any]],
    fallback_score_text: str,
    game_time_seconds: Any,
    radiant_lead: Any,
    radiant_heroes_and_pos: Dict[str, Any],
    dire_heroes_and_pos: Dict[str, Any],
    metrics_payload: Dict[str, Any],
    protracker_payload: Optional[Dict[str, Any]],
) -> str:
    return (
        f"{_format_signal_header(stake_team_name='PIPELINE CHECK', stake_multiplier=1)}\n"
        f"{radiant_team_name} VS {dire_team_name}\n"
        f"{_build_series_score_line_with_fallback(live_league, fallback_score_text)}"
        f"{_format_live_message_state_block(game_time_seconds=game_time_seconds, radiant_lead=radiant_lead, radiant_team_name=radiant_team_name, dire_team_name=dire_team_name)}"
        "mode: send_every_parsed_match\n"
        "source: get_heads/cyberscore_try\n"
        f"Radiant draft: {_format_pipeline_probe_draft_side(radiant_heroes_and_pos)}\n"
        f"Dire draft: {_format_pipeline_probe_draft_side(dire_heroes_and_pos)}\n\n"
        f"{_build_lane_block(metrics_payload.get('top'), metrics_payload.get('mid'), metrics_payload.get('bot'))}"
        f"{_format_pipeline_probe_phase_block('Early', metrics_payload.get('early_output'))}"
        f"{_build_dota2protracker_lane_adv_line(protracker_payload)}"
        f"{_format_pipeline_probe_phase_block('Late', metrics_payload.get('mid_output'))}"
        f"{_format_pipeline_probe_phase_block('Post-lane', metrics_payload.get('post_lane_output'))}"
        f"{_build_dota2protracker_block(protracker_payload)}"
    )


def _build_minimal_odds_only_message(
    *,
    radiant_team_name: str,
    dire_team_name: str,
    live_league: Optional[Dict[str, Any]],
    fallback_score_text: str,
) -> str:
    return (
        f"{radiant_team_name} VS {dire_team_name}\n"
        f"{_build_series_score_line_with_fallback(live_league, fallback_score_text)}"
    )


def _refresh_stake_multiplier_message(
    message_text: str,
    *,
    stake_multiplier_context: Optional[Dict[str, Any]],
    game_time_seconds: Optional[float],
    radiant_lead: Optional[float],
) -> str:
    if not isinstance(message_text, str) or not message_text.startswith("СТАВКА НА "):
        return message_text
    if not isinstance(stake_multiplier_context, dict):
        return message_text

    stake_team_name = str(stake_multiplier_context.get("stake_team_name") or "").strip()
    if not stake_team_name:
        return message_text
    special_header_mode = str(stake_multiplier_context.get("special_header_mode") or "").strip()

    multiplier = _stake_multiplier_for_signal(
        team_elo_meta=None,
        target_side=stake_multiplier_context.get("target_side"),
        selected_early_sign=stake_multiplier_context.get("selected_early_sign"),
        selected_late_sign=stake_multiplier_context.get("selected_late_sign"),
        has_selected_early_star=bool(stake_multiplier_context.get("has_selected_early_star")),
        has_selected_late_star=bool(stake_multiplier_context.get("has_selected_late_star")),
        early_wr_pct=stake_multiplier_context.get("early_wr_pct"),
        late_wr_pct=stake_multiplier_context.get("late_wr_pct"),
        game_time_seconds=game_time_seconds,
        radiant_lead=radiant_lead,
        late_star_hit_count=stake_multiplier_context.get("late_star_hit_count"),
        target_rating=stake_multiplier_context.get("target_rating"),
        opposite_rating=stake_multiplier_context.get("opposite_rating"),
        target_elo_wr=stake_multiplier_context.get("target_elo_wr"),
        force_half_due_to_early_no_valid_late=bool(
            stake_multiplier_context.get("force_half_due_to_early_no_valid_late")
        ),
    )

    new_header = _format_signal_header(
        stake_team_name=stake_team_name,
        stake_multiplier=multiplier,
        special_header_mode=special_header_mode,
    )
    lines = message_text.splitlines()
    if not lines:
        return message_text
    lines[0] = new_header

    try:
        radiant_team_name = str(stake_multiplier_context.get("radiant_team_name") or "").strip()
        dire_team_name = str(stake_multiplier_context.get("dire_team_name") or "").strip()
    except Exception:
        radiant_team_name = ""
        dire_team_name = ""
    live_state_lines = _format_live_message_state_block(
        game_time_seconds=game_time_seconds,
        radiant_lead=radiant_lead,
        radiant_team_name=radiant_team_name,
        dire_team_name=dire_team_name,
    ).strip().splitlines()
    filtered_lines = [
        line
        for line in lines
        if not str(line).startswith("Time:") and not str(line).startswith("Networth:")
    ]
    insert_after_idx = -1
    for idx, line in enumerate(filtered_lines):
        if str(line).startswith("Synergy_trio:"):
            insert_after_idx = idx
    if insert_after_idx >= 0:
        filtered_lines[insert_after_idx + 1 : insert_after_idx + 1] = live_state_lines
    else:
        if filtered_lines and str(filtered_lines[-1]).strip():
            filtered_lines.append("")
        filtered_lines.extend(live_state_lines)
    trailing_newline = "\n" if message_text.endswith("\n") else ""
    return "\n".join(filtered_lines) + trailing_newline


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

    if snapshot["profile"] == "late_only_opposite_signs_early90_tier1_fast_release":
        target_game_time_raw = payload.get("target_game_time")
        try:
            target_game_time = (
                float(target_game_time_raw)
                if target_game_time_raw is not None
                else float(LATE_PUB_COMEBACK_TABLE_START_SECONDS)
            )
        except (TypeError, ValueError):
            target_game_time = float(LATE_PUB_COMEBACK_TABLE_START_SECONDS)

        if current_game_time < float(NETWORTH_GATE_EARLY_WINDOW_END_SECONDS):
            next_threshold_raw = payload.get(
                "networth_monitor_threshold_4_to_10",
                payload.get("networth_monitor_threshold"),
            )
            next_status_label = str(
                payload.get("networth_monitor_status_4_to_10")
                or payload.get("dispatch_status_label")
                or snapshot["status_label"]
                or ""
            )
        elif current_game_time < target_game_time:
            next_threshold_raw = payload.get(
                "networth_monitor_threshold_10_to_20",
                payload.get("networth_monitor_threshold"),
            )
            next_status_label = str(
                payload.get("networth_monitor_status_10_to_20")
                or payload.get("dispatch_status_label")
                or snapshot["status_label"]
                or ""
            )
        else:
            snapshot["threshold"] = None
            return snapshot

        try:
            next_threshold = float(next_threshold_raw) if next_threshold_raw is not None else None
        except (TypeError, ValueError):
            next_threshold = None
        snapshot["threshold"] = next_threshold
        snapshot["status_label"] = next_status_label
        return snapshot

    if snapshot["profile"] != "late_only_opposite_signs_early90":
        return snapshot
    snapshot["threshold"] = None
    snapshot["status_label"] = str(
        payload.get("dispatch_status_label")
        or payload.get("timeout_status_label")
        or snapshot["status_label"]
        or ""
    )
    return snapshot


def _format_game_clock(game_time_seconds: Any) -> str:
    try:
        sec = max(0.0, float(game_time_seconds or 0.0))
    except (TypeError, ValueError):
        sec = 0.0
    return f"{int(sec // 60):02d}:{int(sec % 60):02d}"


def _format_live_message_state_block(
    *,
    game_time_seconds: Any,
    radiant_lead: Any,
    radiant_team_name: Any,
    dire_team_name: Any,
) -> str:
    time_line = f"Time: {_format_game_clock(game_time_seconds)}"
    try:
        lead_value = float(radiant_lead or 0.0)
    except (TypeError, ValueError):
        lead_value = 0.0
    abs_lead = int(round(abs(lead_value)))
    if abs_lead <= 0:
        networth_line = "Networth: 0"
    elif lead_value > 0:
        networth_line = f"Networth: {str(radiant_team_name or 'Radiant')} +{abs_lead}"
    else:
        networth_line = f"Networth: {str(dire_team_name or 'Dire')} +{abs_lead}"
    return f"{time_line}\n{networth_line}\n"


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
        delayed_reason = str(payload.get("reason") or "").strip().lower()
        if delayed_reason == "late_only_opposite_signs":
            opposite_min_dispatch_time = float(LATE_PUB_COMEBACK_TABLE_START_SECONDS)
            opposite_payload_updates: Dict[str, Any] = {}
            if target_game_time < opposite_min_dispatch_time:
                target_game_time = opposite_min_dispatch_time
                opposite_payload_updates["target_game_time"] = float(opposite_min_dispatch_time)
            if payload.get("dynamic_monitor_profile") in {
                "late_only_opposite_signs_early90",
                "late_only_opposite_signs_early90_tier1_fast_release",
            }:
                opposite_payload_updates["dynamic_monitor_profile"] = ""
            if payload.get("networth_monitor_threshold") is not None:
                opposite_payload_updates["networth_monitor_threshold"] = None
            if payload.get("networth_monitor_deadline_game_time") is not None:
                opposite_payload_updates["networth_monitor_deadline_game_time"] = None
            if payload.get("send_on_target_game_time") is not True:
                opposite_payload_updates["send_on_target_game_time"] = True
            if opposite_payload_updates:
                _update_delayed_match(match_key, **opposite_payload_updates)
                payload = dict(payload)
                payload.update(opposite_payload_updates)
            if current_game_time < opposite_min_dispatch_time:
                continue

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
        late_pub_comeback_table_active = bool(payload.get("late_pub_comeback_table_active"))
        late_pub_comeback_table_decision: Optional[Dict[str, Any]] = None
        late_pub_comeback_table_wr_level_raw = payload.get("late_pub_comeback_table_wr_level")
        try:
            late_pub_comeback_table_wr_level = (
                int(late_pub_comeback_table_wr_level_raw)
                if late_pub_comeback_table_wr_level_raw is not None
                else None
            )
        except (TypeError, ValueError):
            late_pub_comeback_table_wr_level = None
        if monitor_target_side in {"radiant", "dire"}:
            monitor_target_diff = _target_networth_diff_from_radiant_lead(
                current_radiant_lead,
                monitor_target_side,
            )
        if late_pub_comeback_table_active:
            late_pub_comeback_table_decision = _late_star_pub_table_decision(
                wr_level=late_pub_comeback_table_wr_level,
                game_time_seconds=current_game_time,
                target_networth_diff=monitor_target_diff,
            )
            if late_pub_comeback_table_decision.get("ready"):
                monitor_ready = True
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

        if late_pub_comeback_table_active and not monitor_ready:
            updated_add_url_details = dict(payload.get("add_url_details") or {})
            updated_add_url_details["dispatch_status_label"] = NETWORTH_STATUS_LATE_PUB_TABLE_WAIT
            updated_add_url_details["target_side"] = monitor_target_side
            if monitor_target_diff is not None:
                updated_add_url_details["target_networth_diff"] = float(monitor_target_diff)
            if isinstance(late_pub_comeback_table_decision, dict):
                if late_pub_comeback_table_decision.get("source_minute") is not None:
                    updated_add_url_details["late_pub_comeback_table_minute"] = int(
                        late_pub_comeback_table_decision.get("source_minute") or 0
                    )
                if late_pub_comeback_table_decision.get("threshold") is not None:
                    updated_add_url_details["late_pub_comeback_table_threshold"] = float(
                        late_pub_comeback_table_decision.get("threshold") or 0.0
                    )
            _update_delayed_match(
                match_key,
                add_url_details=updated_add_url_details,
                dispatch_status_label=NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
                last_checked_at=float(now_ts),
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
            player_denylist_block = payload.get("player_denylist_block")
            if isinstance(player_denylist_block, dict):
                blocked_player_account_ids = [
                    int(pid)
                    for pid in (player_denylist_block.get("blocked_player_account_ids") or [])
                    if _coerce_int(pid) > 0
                ]
                if blocked_player_account_ids:
                    add_url_details.setdefault(
                        "dispatch_status_label",
                        "delayed_player_denylist_reject",
                    )
                    add_url_details.setdefault(
                        "target_side",
                        player_denylist_block.get("target_side"),
                    )
                    add_url_details.setdefault(
                        "target_team",
                        player_denylist_block.get("target_team"),
                    )
                    add_url_details.setdefault(
                        "blocked_player_account_ids",
                        blocked_player_account_ids,
                    )
                    add_url_details.setdefault(
                        "radiant_account_ids",
                        list(player_denylist_block.get("radiant_account_ids") or []),
                    )
                    add_url_details.setdefault(
                        "dire_account_ids",
                        list(player_denylist_block.get("dire_account_ids") or []),
                    )
                    add_url_details.setdefault(
                        "skipped_player_hits",
                        dict(player_denylist_block.get("skipped_player_hits") or {}),
                    )
                    add_url(
                        match_key,
                        reason="skip_player_denylist",
                        details=add_url_details,
                    )
                    _drop_delayed_match(match_key, reason="delayed_player_denylist")
                    print(
                        f"⏱️ Отложенный сигнал отменен из-за player denylist: {match_key} "
                        f"(target_side={player_denylist_block.get('target_side')}, "
                        f"team={player_denylist_block.get('target_team')}, "
                        f"hits={blocked_player_account_ids})"
                    )
                    continue
            add_url_details.setdefault('sent_game_time', int(current_game_time))
            if late_pub_comeback_table_active and monitor_ready and monitor_target_diff is not None:
                add_url_details["dispatch_status_label"] = NETWORTH_STATUS_LATE_PUB_TABLE_SEND
                add_url_details["late_pub_comeback_table_reached"] = True
                add_url_details["target_networth_diff"] = float(monitor_target_diff)
                add_url_details["late_pub_comeback_table_wr_level"] = int(late_pub_comeback_table_wr_level or 0)
                if isinstance(late_pub_comeback_table_decision, dict):
                    if late_pub_comeback_table_decision.get("source_minute") is not None:
                        add_url_details["late_pub_comeback_table_minute"] = int(
                            late_pub_comeback_table_decision.get("source_minute") or 0
                        )
                    if late_pub_comeback_table_decision.get("threshold") is not None:
                        add_url_details["late_pub_comeback_table_threshold"] = float(
                            late_pub_comeback_table_decision.get("threshold") or 0.0
                        )
            elif late_comeback_monitor_active and monitor_ready and monitor_target_diff is not None:
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
            delivery_message_text = _refresh_stake_multiplier_message(
                payload.get('message', ''),
                stake_multiplier_context=payload.get("stake_multiplier_context"),
                game_time_seconds=current_game_time,
                radiant_lead=current_radiant_lead,
            )
            delivery_message_text = _refresh_message_bookmaker_block_for_dispatch(
                match_key,
                delivery_message_text,
            )
            delivery_confirmed = _deliver_and_persist_signal(
                match_key,
                delivery_message_text,
                add_url_reason=add_url_reason,
                add_url_details=add_url_details,
            )
            if delivery_confirmed:
                if late_pub_comeback_table_active and monitor_ready and monitor_target_diff is not None:
                    print(
                        f"⏱️ Отложенный сигнал отправлен по pub late comeback table: {match_key} "
                        f"(game_time={int(current_game_time)}, "
                        f"target_networth_diff={int(monitor_target_diff)}, "
                        f"wr={late_pub_comeback_table_wr_level}, "
                        f"minute={late_pub_comeback_table_decision.get('source_minute') if late_pub_comeback_table_decision else 'n/a'}, "
                        f"threshold={int(late_pub_comeback_table_decision.get('threshold')) if late_pub_comeback_table_decision and late_pub_comeback_table_decision.get('threshold') is not None else 'n/a'})"
                    )
                elif late_comeback_monitor_active and monitor_ready and monitor_target_diff is not None:
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


def _bookmaker_prefetch_prune_locked(now_ts: float) -> List[str]:
    if not bookmaker_prefetch_results:
        return []
    ttl = max(60, int(BOOKMAKER_PREFETCH_RESULT_TTL_SECONDS))
    to_drop = []
    for match_key, payload in bookmaker_prefetch_results.items():
        status = str(payload.get("status") or "")
        finished_at = float(payload.get("finished_at") or payload.get("submitted_at") or 0.0)
        if status in {"done", "error"} and now_ts - finished_at > ttl:
            to_drop.append(match_key)
    for match_key in to_drop:
        bookmaker_prefetch_results.pop(match_key, None)
    return list(to_drop)


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


def _bookmaker_slugify_text(value: str) -> str:
    compact = re.sub(r"[^a-z0-9а-я]+", "-", str(value or "").strip().lower())
    compact = re.sub(r"-{2,}", "-", compact).strip("-")
    return compact


def _bookmaker_extract_team_aliases_from_series_url(
    *,
    series_url: str,
    league_title: str,
) -> Tuple[Optional[str], Optional[str]]:
    match = re.search(r"/matches/\d+/([^/?#]+)", str(series_url or ""))
    if not match:
        return None, None
    slug = str(match.group(1) or "").strip().lower()
    if "-vs-" not in slug:
        return None, None
    league_slug = _bookmaker_slugify_text(league_title)
    if league_slug and slug.endswith("-" + league_slug):
        slug = slug[: -(len(league_slug) + 1)]
    left, right = slug.split("-vs-", 1)
    left = left.strip("-")
    right = right.strip("-")
    if not left or not right:
        return None, None
    return left.replace("-", " ").strip(), right.replace("-", " ").strip()


def _bookmaker_build_team_candidates(primary_name: str, alias_name: Optional[str]) -> List[str]:
    candidates: List[str] = []
    seen: set[str] = set()
    for raw in (primary_name, alias_name):
        value = str(raw or "").strip()
        if not value:
            continue
        key = normalize_team_name(value)
        if not key or key in seen:
            continue
        seen.add(key)
        candidates.append(value)
    return candidates


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
        source_name = ""
        details_excerpt = ""
        match_found = False
        status = ""
        if isinstance(site_payload, dict):
            match_found = bool(site_payload.get("match_found", False))
            status = str(site_payload.get("status") or "")
            if bool(site_payload.get("market_closed")):
                reason = "map_market_closed"
            source_name = str(site_payload.get("source") or "").strip()
            if source_name:
                reason = source_name
            details_excerpt = str(site_payload.get("details") or "").strip()[:220]
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
                "source": source_name,
                "details_excerpt": details_excerpt,
                "match_found": match_found,
                "status": status,
            }
        )
    return map_context, rows


def _log_bookmaker_source_snapshot(match_key: str, decision: str) -> None:
    if decision not in {"sent", "no_numeric_odds", "no_match_presence"}:
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
    display_order = [
        ("winline", "Winline"),
        ("betboom", "BetBoom"),
        ("pari", "Pari"),
    ]
    cells: List[str] = []
    for site, site_label in display_order:
        site_payload = sites_payload.get(site)
        if not isinstance(site_payload, dict):
            cells.append(f"{site_label} —")
            continue
        odds = site_payload.get("odds")
        match_odds = site_payload.get("match_odds")
        market_closed = bool(site_payload.get("market_closed"))
        if not market_closed and isinstance(odds, list) and len(odds) >= 2:
            try:
                p1 = float(odds[0])
                p2 = float(odds[1])
                cells.append(f"{site_label} {p1:.2f}/{p2:.2f}")
                continue
            except (TypeError, ValueError):
                pass
        if isinstance(match_odds, list) and len(match_odds) >= 2:
            try:
                p1 = float(match_odds[0])
                p2 = float(match_odds[1])
                cells.append(f"{site_label} (п1/п2) {p1:.2f}/{p2:.2f}")
                continue
            except (TypeError, ValueError):
                pass
        cells.append(f"{site_label} —")
    if not cells:
        return "", False, "no_cells"
    has_real_odds = any(
        "—" not in cell for cell in cells
    )
    if not has_real_odds:
        return "", False, "no_numeric_odds"
    mode = str(snapshot.get("mode") or BOOKMAKER_PREFETCH_MODE)
    map_num_raw = snapshot.get("map_num")
    map_num = int(map_num_raw) if isinstance(map_num_raw, int) and 1 <= map_num_raw <= 5 else None
    map_suffix = f", карта {map_num}" if map_num is not None else ""
    return f"БК ({mode}{map_suffix}): " + " | ".join(cells) + "\n", True, "ok"


def _bookmaker_format_presence_block(match_key: str) -> Tuple[str, bool, str]:
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
    display_order = [
        ("winline", "Winline"),
        ("betboom", "BetBoom"),
        ("pari", "Pari"),
    ]
    cells: List[str] = []
    has_match_presence = False
    for site, site_label in display_order:
        site_payload = sites_payload.get(site)
        site_found = isinstance(site_payload, dict) and bool(site_payload.get("match_found", False))
        if site_found:
            has_match_presence = True
        marker = _bookmaker_presence_site_marker(site_payload)
        cells.append(f"{site_label} {marker}")
    all_sites_error = _bookmaker_presence_all_sites_error(sites_payload)
    if not cells or (not has_match_presence and not all_sites_error):
        return "", False, "no_match_presence"
    mode = str(snapshot.get("mode") or BOOKMAKER_PREFETCH_MODE)
    map_num_raw = snapshot.get("map_num")
    map_num = int(map_num_raw) if isinstance(map_num_raw, int) and 1 <= map_num_raw <= 5 else None
    map_suffix = f", карта {map_num}" if map_num is not None else ""
    reason = "all_sites_error" if (not has_match_presence and all_sites_error) else "ok"
    return f"БК ({mode}{map_suffix}): " + " | ".join(cells) + "\n", True, reason


def _bookmaker_format_gate_block(match_key: str) -> Tuple[str, bool, str]:
    if BOOKMAKER_PREFETCH_GATE_MODE == "presence":
        return _bookmaker_format_presence_block(match_key)
    return _bookmaker_format_odds_block(match_key)


def _bookmaker_sites_compact_summary(sites_payload: Any) -> str:
    if not isinstance(sites_payload, dict):
        return "sites=none"
    parts: List[str] = []
    for site in BOOKMAKER_PREFETCH_SITES:
        site_payload = sites_payload.get(site)
        if not isinstance(site_payload, dict):
            parts.append(f"{site}:error/missing")
            continue
        status = str(site_payload.get("status") or "unknown")
        if BOOKMAKER_PREFETCH_GATE_MODE == "presence":
            marker = _bookmaker_presence_site_state(site_payload)
            source_name = str(site_payload.get("source") or "").strip()
            source_suffix = f"/{source_name}" if source_name else ""
            parts.append(f"{site}:{marker}/{status}{source_suffix}")
        else:
            odds = site_payload.get("odds")
            match_odds = site_payload.get("match_odds")
            if isinstance(odds, list) and len(odds) >= 2:
                marker = "map_odds"
            elif isinstance(match_odds, list) and len(match_odds) >= 2:
                marker = "match_odds"
            elif bool(site_payload.get("market_closed")):
                marker = "closed"
            else:
                marker = "none"
            parts.append(f"{site}:{marker}/{status}")
    return ", ".join(parts) if parts else "sites=none"


def _bookmaker_presence_trace_summary(sites_payload: Any) -> str:
    if not isinstance(sites_payload, dict):
        return ""
    label_map = {
        "betboom": "BetBoom",
        "pari": "Pari",
        "winline": "Winline",
    }
    trace_parts: List[str] = []
    for site in BOOKMAKER_PREFETCH_SITES:
        site_payload = sites_payload.get(site)
        if not isinstance(site_payload, dict) or not bool(site_payload.get("match_found", False)):
            continue
        source_name = str(site_payload.get("source") or "").strip() or "unknown"
        details = str(site_payload.get("details") or "").strip()
        details_excerpt = details[:140] + ("..." if len(details) > 140 else "") if details else ""
        token = f"{label_map.get(site, site)}={source_name}"
        if details_excerpt:
            token += f" [{details_excerpt}]"
        trace_parts.append(token)
    return " | ".join(trace_parts)


def _bookmaker_presence_site_state(site_payload: Any) -> str:
    if not isinstance(site_payload, dict):
        return "error"
    if bool(site_payload.get("match_found", False)):
        return "found"
    source_name = str(site_payload.get("source") or "").strip().lower()
    if source_name in {
        "explicit_absence",
        "event_not_listed",
        "match_absent",
        "match_not_listed",
    }:
        return "absent"
    return "error"


def _bookmaker_presence_site_marker(site_payload: Any) -> str:
    state = _bookmaker_presence_site_state(site_payload)
    if state == "found":
        return "✅"
    if state == "absent":
        return "❌"
    return "error"


def _bookmaker_presence_has_partial_miss(snapshot: Optional[dict]) -> bool:
    payload = snapshot if isinstance(snapshot, dict) else {}
    sites_payload = payload.get("sites")
    if not isinstance(sites_payload, dict):
        return False
    has_found = False
    has_non_found = False
    for site in BOOKMAKER_PREFETCH_SITES:
        site_payload = sites_payload.get(site)
        if isinstance(site_payload, dict) and bool(site_payload.get("match_found", False)):
            has_found = True
        else:
            has_non_found = True
    return has_found and has_non_found


def _bookmaker_presence_all_sites_error(sites_payload: Any) -> bool:
    if not isinstance(sites_payload, dict):
        return False
    seen_any = False
    for site in BOOKMAKER_PREFETCH_SITES:
        site_payload = sites_payload.get(site)
        seen_any = True
        if _bookmaker_presence_site_state(site_payload) != "error":
            return False
    return seen_any


def _log_bookmaker_presence_gate(match_key: str, state: str, snapshot: Optional[dict]) -> None:
    payload = snapshot if isinstance(snapshot, dict) else {}
    status = str(payload.get("status") or "")
    sites_summary = _bookmaker_sites_compact_summary(payload.get("sites"))
    log_line = (
        f"BOOKMAKER_PRESENCE_GATE state={state} status={status or 'none'} "
        f"url={match_key} {sites_summary}"
    )
    if state == "error":
        error_text = str(payload.get("error") or "").strip()
        if error_text:
            log_line += f" error={error_text[:300]}"
    trace_summary = _bookmaker_presence_trace_summary(payload.get("sites"))
    if trace_summary:
        log_line += f" trace={trace_summary}"
    logger.info(log_line)
    print(log_line)


def _log_bookmaker_presence_failure_diagnostics(
    match_key: str,
    snapshot: Optional[dict],
    *,
    context: str,
    only_non_found: bool = False,
) -> None:
    payload = snapshot if isinstance(snapshot, dict) else {}
    map_context, rows = _bookmaker_source_snapshot_rows(match_key)
    if not rows:
        return
    base_payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "context": context,
        "match_id": _bookmaker_extract_match_id(match_key),
        "url": match_key,
        "map_context": map_context,
        "snapshot_status": str(payload.get("status") or ""),
        "proxy_url": str(BOOKMAKER_PROXY_URL or ""),
        "sites_summary": _bookmaker_sites_compact_summary(payload.get("sites")),
    }
    for row in rows:
        if only_non_found and bool(row.get("match_found", False)):
            continue
        diag_payload = dict(base_payload)
        diag_payload.update(row)
        diag_payload["details"] = str(row.get("details_excerpt") or "")
        line = "BOOKMAKER_PRESENCE_FAILURE_DIAGNOSTICS " + json.dumps(diag_payload, ensure_ascii=False)
        logger.error(line)
        print(line)


def _bookmaker_presence_gate_resolution(match_key: str) -> Tuple[str, Optional[dict]]:
    snapshot = _bookmaker_prefetch_lookup(match_key, wait_seconds=0.0)
    if not isinstance(snapshot, dict):
        return "pending", None
    snapshot_status = str(snapshot.get("status") or "")
    if snapshot_status in {"queued", "running", ""}:
        return "pending", snapshot
    if snapshot_status != "done":
        return "error", snapshot
    sites_payload = snapshot.get("sites")
    if not isinstance(sites_payload, dict):
        return "error", snapshot
    for site_payload in sites_payload.values():
        if isinstance(site_payload, dict) and bool(site_payload.get("match_found", False)):
            return "allow", snapshot
    if _bookmaker_presence_all_sites_error(sites_payload):
        return "allow", snapshot
    return "reject", snapshot


def _bookmaker_close_window_handles_unlocked(driver: Any, handles: List[str]) -> None:
    valid_handles = [str(handle or "").strip() for handle in handles if str(handle or "").strip()]
    if driver is None or not valid_handles:
        return
    try:
        known_handles = set(driver.window_handles or [])
    except Exception:
        known_handles = set()
    for handle in valid_handles:
        if handle not in known_handles:
            continue
        try:
            driver.switch_to.window(handle)
            driver.close()
        except Exception:
            continue


def _bookmaker_browser_bootstrap_unlocked(mode: str) -> bool:
    global bookmaker_browser_driver, bookmaker_browser_base_handles
    if _bookmaker_build_driver is None or _bookmaker_open_presence_site_tabs is None:
        return False
    if bookmaker_browser_driver is None:
        bookmaker_browser_driver = _bookmaker_build_driver(BOOKMAKER_PROXY_URL)
        bookmaker_browser_base_handles = {}
    if bookmaker_browser_base_handles:
        return True
    urls = _bookmaker_urls_for_mode(mode)
    selected_sites = [site for site in BOOKMAKER_PREFETCH_SITES if site in urls]
    if not selected_sites:
        return False
    bookmaker_browser_base_handles = _bookmaker_open_presence_site_tabs(
        bookmaker_browser_driver,
        selected_sites=selected_sites,
        urls=urls,
    )
    return bool(bookmaker_browser_base_handles)


# Camoufox browser reuse functions
CAMOUFOX_BROWSER_MAX_IDLE_SECONDS = 300  # 5 minutes idle = close browser


def _ensure_camoufox_browser() -> Any:
    """Legacy guard: direct Camoufox ownership moved to _SharedCamoufoxSession."""
    raise RuntimeError("Use _run_shared_camoufox_job() for Camoufox work")


def _close_camoufox_browser() -> None:
    """Close the persistent Camoufox browser if idle too long or on shutdown."""
    global bookmaker_camoufox_browser, bookmaker_camoufox_browser_last_used
    if bookmaker_camoufox_browser is not None:
        try:
            bookmaker_camoufox_browser.close()
        except Exception:
            pass
        bookmaker_camoufox_browser = None
        bookmaker_camoufox_browser_last_used = 0.0
        print("   🔒 Camoufox browser closed")


def _cleanup_on_exit() -> None:
    """Called on process exit to clean up resources."""
    _close_camoufox_browser()


atexit.register(_cleanup_on_exit)


def _should_close_camoufox_browser() -> bool:
    """Check if Camoufox browser should be closed due to inactivity."""
    global bookmaker_camoufox_browser, bookmaker_camoufox_browser_last_used
    if bookmaker_camoufox_browser is None:
        return False
    if bookmaker_camoufox_browser_last_used <= 0:
        return False
    idle = time.time() - bookmaker_camoufox_browser_last_used
    if idle > CAMOUFOX_BROWSER_MAX_IDLE_SECONDS:
        return True
    return False


def _bookmaker_try_open_match_details_with_candidates_unlocked(
    driver: Any,
    *,
    site: str,
    team1_candidates: List[str],
    team2_candidates: List[str],
) -> Optional[str]:
    if driver is None or _bookmaker_open_match_details_by_teams is None:
        return None
    seen_pairs = set()
    for team1_candidate in list(team1_candidates or []):
        for team2_candidate in list(team2_candidates or []):
            pair = (str(team1_candidate or "").strip().lower(), str(team2_candidate or "").strip().lower())
            if not pair[0] or not pair[1] or pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            opened_url = _bookmaker_open_match_details_by_teams(
                driver,
                site=site,
                team1=str(team1_candidate or ""),
                team2=str(team2_candidate or ""),
            )
            if opened_url:
                return str(opened_url)
    return None


def _bookmaker_prune_match_tab_cache_unlocked(max_matches: Optional[int] = None) -> None:
    global bookmaker_browser_match_tabs
    limit = max(1, int(max_matches or BOOKMAKER_MATCH_TAB_CACHE_MAX_MATCHES or 8))
    while len(bookmaker_browser_match_tabs) > limit:
        stale_match_key, stale_payload = bookmaker_browser_match_tabs.popitem(last=False)
        handles = [
            str((site_payload or {}).get("handle") or "")
            for site_payload in ((stale_payload or {}).get("sites") or {}).values()
            if isinstance(site_payload, dict)
        ]
        _bookmaker_close_window_handles_unlocked(bookmaker_browser_driver, handles)
        logger.info("BOOKMAKER_MATCH_TAB_CACHE_EVICT url=%s handles=%s", stale_match_key, len(handles))


def _bookmaker_release_match_tabs(match_key: str) -> None:
    global bookmaker_browser_match_tabs
    normalized_match_key = str(match_key or "").strip()
    if not normalized_match_key:
        return
    with bookmaker_browser_lock:
        stale_payload = bookmaker_browser_match_tabs.pop(normalized_match_key, None)
        if not isinstance(stale_payload, dict):
            return
        handles = [
            str((site_payload or {}).get("handle") or "")
            for site_payload in ((stale_payload or {}).get("sites") or {}).values()
            if isinstance(site_payload, dict)
        ]
        _bookmaker_close_window_handles_unlocked(bookmaker_browser_driver, handles)
        logger.info("BOOKMAKER_MATCH_TAB_CACHE_RELEASE url=%s handles=%s", normalized_match_key, len(handles))


def _bookmaker_open_cached_match_tabs_unlocked(
    match_key: str,
    *,
    radiant_team: str,
    dire_team: str,
    radiant_team_candidates: List[str],
    dire_team_candidates: List[str],
    mode: str,
    map_num: Optional[int],
) -> Dict[str, dict]:
    global bookmaker_browser_match_tabs
    sites_payload: Dict[str, dict] = {}
    if (
        bookmaker_browser_driver is None
        or _bookmaker_probe_presence_site_in_current_tab is None
        or _bookmaker_open_match_details_by_teams is None
    ):
        return sites_payload
    if not _bookmaker_browser_bootstrap_unlocked(mode):
        return sites_payload
    urls = _bookmaker_urls_for_mode(mode)
    cached_sites: Dict[str, dict] = {}
    for site in BOOKMAKER_PREFETCH_SITES:
        base_handle = str(bookmaker_browser_base_handles.get(site) or "").strip()
        site_url = str(urls.get(site) or "")
        if not base_handle or not site_url:
            continue
        try:
            bookmaker_browser_driver.switch_to.window(base_handle)
            result = _bookmaker_probe_presence_site_in_current_tab(
                bookmaker_browser_driver,
                site=site,
                url=site_url,
                team1=radiant_team,
                team2=dire_team,
                mode=mode,
                team1_aliases=radiant_team_candidates,
                team2_aliases=dire_team_candidates,
                extra_reload_on_empty=(site == "pari"),
                extra_scroll_passes=(2 if site in {"betboom", "winline"} else 1 if site == "pari" else 0),
            )
        except Exception as exc:
            result = None
            sites_payload[site] = {
                "status": "request_error",
                "match_found": False,
                "odds": [],
                "match_odds": [],
                "source": "tab_probe_error",
                "details": str(exc)[:500],
                "market_closed": False,
            }
        if result is None:
            continue
        sites_payload[site] = {
            "status": str(getattr(result, "status", "")),
            "match_found": bool(getattr(result, "match_found", False)),
            "odds": [],
            "match_odds": [],
            "source": str(getattr(result, "source", "")),
            "details": str(getattr(result, "details", ""))[:500],
            "market_closed": bool(getattr(result, "market_closed", False)),
        }
        if not bool(getattr(result, "match_found", False)):
            continue
        before_handles = set(bookmaker_browser_driver.window_handles or [])
        current_url = ""
        try:
            current_url = str(bookmaker_browser_driver.current_url or "") or site_url
        except Exception:
            current_url = site_url
        try:
            bookmaker_browser_driver.execute_script("window.open(arguments[0], '_blank');", current_url)
            new_handles = [handle for handle in bookmaker_browser_driver.window_handles if handle not in before_handles]
            match_handle = str(new_handles[0] if new_handles else bookmaker_browser_driver.window_handles[-1])
            bookmaker_browser_driver.switch_to.window(match_handle)
            time.sleep(1.0)
            opened_url = _bookmaker_try_open_match_details_with_candidates_unlocked(
                bookmaker_browser_driver,
                site=site,
                team1_candidates=radiant_team_candidates,
                team2_candidates=dire_team_candidates,
            )
            if not opened_url:
                _bookmaker_close_window_handles_unlocked(bookmaker_browser_driver, [match_handle])
                continue
            cached_sites[site] = {
                "handle": match_handle,
                "url": str(opened_url),
                "opened_at": time.time(),
                "map_num": int(map_num) if isinstance(map_num, int) and 1 <= map_num <= 5 else None,
            }
        except Exception as exc:
            sites_payload[site]["details"] = str(exc)[:500]
            _bookmaker_close_window_handles_unlocked(
                bookmaker_browser_driver,
                [h for h in bookmaker_browser_driver.window_handles if h not in before_handles],
            )
            continue
    if cached_sites:
        bookmaker_browser_match_tabs[match_key] = {
            "radiant_team": str(radiant_team or ""),
            "dire_team": str(dire_team or ""),
            "radiant_team_candidates": list(radiant_team_candidates or []),
            "dire_team_candidates": list(dire_team_candidates or []),
            "mode": str(mode or BOOKMAKER_PREFETCH_MODE),
            "map_num": int(map_num) if isinstance(map_num, int) and 1 <= map_num <= 5 else None,
            "sites": cached_sites,
            "created_at": time.time(),
            "last_refreshed_at": 0.0,
        }
        bookmaker_browser_match_tabs.move_to_end(match_key, last=True)
        _bookmaker_prune_match_tab_cache_unlocked()
    return sites_payload


def _bookmaker_refresh_cached_match_tabs_for_dispatch(match_key: str) -> Optional[dict]:
    normalized_match_key = str(match_key or "").strip()
    if not normalized_match_key:
        return None
    with bookmaker_browser_lock:
        cache_entry = bookmaker_browser_match_tabs.get(normalized_match_key)
        if not isinstance(cache_entry, dict):
            return None
        if bookmaker_browser_driver is None or _bookmaker_parse_map_market_on_current_page is None:
            return None
        refreshed_sites: Dict[str, dict] = {}
        for site in BOOKMAKER_PREFETCH_SITES:
            site_cache = (cache_entry.get("sites") or {}).get(site)
            if not isinstance(site_cache, dict):
                continue
            handle = str(site_cache.get("handle") or "").strip()
            if not handle:
                continue
            try:
                bookmaker_browser_driver.switch_to.window(handle)
                try:
                    bookmaker_browser_driver.refresh()
                    time.sleep(1.5)
                except Exception:
                    pass
                map_odds, body_text = _bookmaker_parse_map_market_on_current_page(
                    bookmaker_browser_driver,
                    site,
                    str(cache_entry.get("radiant_team") or ""),
                    str(cache_entry.get("dire_team") or ""),
                    cache_entry.get("map_num"),
                )
                market_closed = False
                if not map_odds and _bookmaker_is_map_market_closed is not None:
                    market_closed = bool(
                        _bookmaker_is_map_market_closed(
                            site,
                            body_text,
                            forced_map_num=cache_entry.get("map_num"),
                        )
                    )
                refreshed_sites[site] = {
                    "status": "ok",
                    "match_found": True,
                    "odds": list(map_odds or []),
                    "match_odds": [],
                    "source": "cached_match_tab_map_market" if map_odds else (
                        "cached_match_tab_map_market_closed" if market_closed else "cached_match_tab_map_missing"
                    ),
                    "details": str(body_text or "")[:500],
                    "market_closed": bool(market_closed),
                }
            except Exception as exc:
                refreshed_sites[site] = {
                    "status": "request_error",
                    "match_found": True,
                    "odds": [],
                    "match_odds": [],
                    "source": "cached_match_tab_error",
                    "details": str(exc)[:500],
                    "market_closed": False,
                }
        cache_entry["last_refreshed_at"] = time.time()
        bookmaker_browser_match_tabs.move_to_end(normalized_match_key, last=True)
        with bookmaker_prefetch_condition:
            payload = bookmaker_prefetch_results.get(normalized_match_key)
            if isinstance(payload, dict):
                payload["sites"] = dict(refreshed_sites or payload.get("sites") or {})
                payload["odds_refreshed_at"] = cache_entry["last_refreshed_at"]
                bookmaker_prefetch_condition.notify_all()
            return payload if isinstance(payload, dict) else None


def _bookmaker_best_effort_odds_block(match_key: str) -> Tuple[str, bool, str]:
    snapshot = _bookmaker_prefetch_lookup(match_key, wait_seconds=0.0)
    if not isinstance(snapshot, dict):
        return "", False, "prefetch_not_found"
    snapshot_status = str(snapshot.get("status") or "")
    if snapshot_status != "done":
        return "", False, f"prefetch_{snapshot_status or 'unknown'}"
    sites_payload = snapshot.get("sites")
    if not isinstance(sites_payload, dict):
        return "", False, "invalid_sites_payload"
    display_order = [
        ("winline", "Winline"),
        ("betboom", "BetBoom"),
        ("pari", "Pari"),
    ]
    cells: List[str] = []
    for site, site_label in display_order:
        site_payload = sites_payload.get(site)
        if not isinstance(site_payload, dict):
            cells.append(f"{site_label} —")
            continue
        odds = site_payload.get("odds")
        match_odds = site_payload.get("match_odds")
        if not bool(site_payload.get("market_closed")) and isinstance(odds, list) and len(odds) >= 2:
            try:
                p1 = float(odds[0])
                p2 = float(odds[1])
                cells.append(f"{site_label} {p1:.2f}/{p2:.2f}")
                continue
            except (TypeError, ValueError):
                pass
        if isinstance(match_odds, list) and len(match_odds) >= 2:
            try:
                p1 = float(match_odds[0])
                p2 = float(match_odds[1])
                cells.append(f"{site_label} (матч) {p1:.2f}/{p2:.2f}")
                continue
            except (TypeError, ValueError):
                pass
        cells.append(f"{site_label} —")
    if not cells:
        return "", False, "no_sites"
    mode = str(snapshot.get("mode") or BOOKMAKER_PREFETCH_MODE)
    map_num_raw = snapshot.get("map_num")
    map_num = int(map_num_raw) if isinstance(map_num_raw, int) and 1 <= map_num_raw <= 5 else None
    map_suffix = f", карта {map_num}" if map_num is not None else ""
    return f"БК ({mode}{map_suffix}): " + " | ".join(cells) + "\n", True, "ok"


def _bookmaker_refresh_snapshot_via_subprocess(match_key: str) -> Optional[dict]:
    if not BOOKMAKER_PREFETCH_ENABLED or not BOOKMAKER_PREFETCH_USE_SUBPROCESS:
        return None
    snapshot = _bookmaker_prefetch_lookup(match_key, wait_seconds=0.0)
    if not isinstance(snapshot, dict):
        return None
    radiant_team = str(snapshot.get("radiant_team") or "")
    dire_team = str(snapshot.get("dire_team") or "")
    mode = str(snapshot.get("mode") or BOOKMAKER_PREFETCH_MODE or "live")
    map_num_raw = snapshot.get("map_num")
    try:
        map_num = int(map_num_raw) if map_num_raw is not None else None
    except (TypeError, ValueError):
        map_num = None
    if map_num is not None and not (1 <= map_num <= 5):
        map_num = None
    radiant_team_candidates = list(snapshot.get("radiant_team_candidates") or [radiant_team])
    dire_team_candidates = list(snapshot.get("dire_team_candidates") or [dire_team])
    try:
        sites_payload = _bookmaker_prefetch_fetch_subprocess(
            radiant_team=radiant_team,
            dire_team=dire_team,
            mode=mode,
            map_num=map_num,
            radiant_team_candidates=radiant_team_candidates,
            dire_team_candidates=dire_team_candidates,
        )
    except Exception as exc:
        logger.warning("BOOKMAKER_SUBPROCESS_REFRESH_FAILED %s: %s", match_key, exc)
        return snapshot
    refreshed_at = time.time()
    with bookmaker_prefetch_condition:
        payload = bookmaker_prefetch_results.get(match_key)
        if isinstance(payload, dict):
            payload["status"] = "done"
            payload["finished_at"] = refreshed_at
            payload["odds_refreshed_at"] = refreshed_at
            payload["sites"] = sites_payload
            bookmaker_prefetch_condition.notify_all()
    return _bookmaker_prefetch_lookup(match_key, wait_seconds=0.0)


def _replace_bookmaker_block_in_message(message_text: str, bookmaker_block: str) -> str:
    if not isinstance(message_text, str) or not message_text.strip():
        return message_text
    lines = message_text.splitlines()
    filtered: List[str] = []
    skipping_old_block = False
    for line in lines:
        stripped = str(line).strip()
        if stripped.startswith("БК (") or stripped.startswith("Букмекеры ("):
            skipping_old_block = True
            continue
        if skipping_old_block and (
            stripped.startswith("Winline")
            or stripped.startswith("BetBoom")
            or stripped.startswith("Pari")
        ):
            continue
        if skipping_old_block:
            skipping_old_block = False
        filtered.append(line)
    trailing_newline = "\n" if message_text.endswith("\n") else ""
    block = str(bookmaker_block or "").rstrip("\n")
    if block:
        filtered.append(block)
    return "\n".join(filtered) + trailing_newline


def _refresh_message_bookmaker_block_for_dispatch(
    match_key: str,
    message_text: str,
) -> str:
    refreshed_message, bookmaker_ready, _bookmaker_reason = _bookmaker_prepare_message_for_delivery(
        match_key,
        message_text,
    )
    if not bookmaker_ready:
        return message_text
    return refreshed_message


def _bookmaker_prepare_message_for_delivery(
    match_key: str,
    message_text: str,
) -> Tuple[str, bool, str]:
    if not BOOKMAKER_PREFETCH_ENABLED or BOOKMAKER_PREFETCH_GATE_MODE != "odds":
        return message_text, True, "disabled"
    if BOOKMAKER_PREFETCH_USE_SUBPROCESS:
        _bookmaker_refresh_snapshot_via_subprocess(match_key)
    else:
        _bookmaker_refresh_cached_match_tabs_for_dispatch(match_key)
    bookmaker_block, bookmaker_ready, bookmaker_reason = _bookmaker_best_effort_odds_block(match_key)
    if not bookmaker_ready:
        return message_text, False, str(bookmaker_reason or "no_numeric_odds")
    return _replace_bookmaker_block_in_message(message_text, bookmaker_block), True, "ok"


def _build_bookmaker_empty_odds_block(match_key: str) -> str:
    snapshot = _bookmaker_prefetch_lookup(match_key, wait_seconds=0.0)
    mode = str((snapshot or {}).get("mode") or BOOKMAKER_PREFETCH_MODE or "live")
    map_num_raw = (snapshot or {}).get("map_num")
    map_num = int(map_num_raw) if isinstance(map_num_raw, int) and 1 <= map_num_raw <= 5 else None
    map_suffix = f", карта {map_num}" if map_num is not None else ""
    return f"БК ({mode}{map_suffix}): Winline — | BetBoom — | Pari —\n"


def _prepare_minimal_odds_only_message_for_delivery(
    match_key: str,
    message_text: str,
) -> Tuple[str, bool, str]:
    if BOOKMAKER_PREFETCH_ENABLED:
        if BOOKMAKER_PREFETCH_USE_SUBPROCESS:
            _bookmaker_refresh_snapshot_via_subprocess(match_key)
        else:
            _bookmaker_refresh_cached_match_tabs_for_dispatch(match_key)
    bookmaker_block, bookmaker_ready, bookmaker_reason = _bookmaker_format_odds_block(match_key)
    if not bookmaker_ready:
        reason_str = str(bookmaker_reason or "")
        if reason_str == "no_numeric_odds":
            return message_text, False, "no_numeric_odds"
        return message_text, False, str(bookmaker_reason or "unknown")
    return _replace_bookmaker_block_in_message(message_text, bookmaker_block), True, "ok"


def _bookmaker_prefetch_submit(
    match_key: str,
    radiant_team: str,
    dire_team: str,
    map_num: Optional[int] = None,
    series_url: str = "",
    league_title: str = "",
) -> None:
    if not BOOKMAKER_PREFETCH_ENABLED:
        return
    if not match_key:
        return
    _ensure_bookmaker_prefetch_started()
    now_ts = time.time()
    slug_radiant_team, slug_dire_team = _bookmaker_extract_team_aliases_from_series_url(
        series_url=series_url or match_key,
        league_title=league_title,
    )
    radiant_team_candidates = _bookmaker_build_team_candidates(radiant_team, slug_radiant_team)
    dire_team_candidates = _bookmaker_build_team_candidates(dire_team, slug_dire_team)
    stale_match_keys: List[str] = []
    with bookmaker_prefetch_condition:
        stale_match_keys = _bookmaker_prefetch_prune_locked(now_ts)
        existing = bookmaker_prefetch_results.get(match_key)
        if isinstance(existing, dict):
            status = str(existing.get("status") or "")
            if status in {"queued", "running", "done"} or (
                BOOKMAKER_PREFETCH_GATE_MODE == "presence" and status == "error"
            ):
                return
        if len(bookmaker_prefetch_queue) >= max(10, int(BOOKMAKER_PREFETCH_MAX_PENDING)):
            print(f"   ⚠️ Bookmaker prefetch queue overflow ({len(bookmaker_prefetch_queue)}), skip {match_key}")
            return
        bookmaker_prefetch_results[match_key] = {
            "status": "queued",
            "mode": BOOKMAKER_PREFETCH_MODE,
            "gate_mode": BOOKMAKER_PREFETCH_GATE_MODE,
            "submitted_at": now_ts,
            "radiant_team": str(radiant_team or ""),
            "dire_team": str(dire_team or ""),
            "radiant_team_candidates": list(radiant_team_candidates),
            "dire_team_candidates": list(dire_team_candidates),
            "map_num": int(map_num) if isinstance(map_num, int) and 1 <= map_num <= 5 else None,
            "sites": {},
        }
        bookmaker_prefetch_queue.append(
            {
                "match_key": match_key,
                "radiant_team": str(radiant_team or ""),
                "dire_team": str(dire_team or ""),
                "radiant_team_candidates": list(radiant_team_candidates),
                "dire_team_candidates": list(dire_team_candidates),
                "map_num": int(map_num) if isinstance(map_num, int) and 1 <= map_num <= 5 else None,
                "mode": BOOKMAKER_PREFETCH_MODE,
                "gate_mode": BOOKMAKER_PREFETCH_GATE_MODE,
                "submitted_at": now_ts,
                "series_url": str(series_url or ""),
                "league_title": str(league_title or ""),
            }
        )
        bookmaker_prefetch_condition.notify()
    for stale_match_key in stale_match_keys:
        _bookmaker_release_match_tabs(stale_match_key)
    print(
        "   📥 Bookmaker prefetch queued: "
        f"{match_key} "
        f"(radiant={radiant_team_candidates}, dire={dire_team_candidates}, "
        f"mode={BOOKMAKER_PREFETCH_MODE}, gate={BOOKMAKER_PREFETCH_GATE_MODE})"
    )


def _bookmaker_prefetch_fetch_subprocess(
    radiant_team: str,
    dire_team: str,
    mode: str,
    map_num: Optional[int] = None,
    radiant_team_candidates: Optional[List[str]] = None,
    dire_team_candidates: Optional[List[str]] = None,
) -> Dict[str, dict]:
    if BOOKMAKER_PREFETCH_GATE_MODE == "presence":
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
            "--presence-only",
        ]
        for alias in list(radiant_team_candidates or [])[1:]:
            cmd.extend(["--team1-alias", str(alias)])
        for alias in list(dire_team_candidates or [])[1:]:
            cmd.extend(["--team2-alias", str(alias)])
    else:
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
        # Fallback: find the first complete JSON object starting from the first '{'
        first_brace = raw.find("{")
        if first_brace >= 0:
            candidate = raw[first_brace:]
            # Count braces to find the matching closing brace
            open_count = 0
            end_pos = -1
            for i, ch in enumerate(candidate):
                if ch == "{":
                    open_count += 1
                elif ch == "}":
                    open_count -= 1
                    if open_count == 0:
                        end_pos = i + 1
                        break
            if end_pos > 0:
                try:
                    payload = json.loads(candidate[:end_pos])
                except Exception:
                    pass
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


def _bookmaker_prefetch_fetch_camoufox_direct(
    radiant_team: str,
    dire_team: str,
    mode: str,
    map_num: Optional[int] = None,
    radiant_team_candidates: Optional[List[str]] = None,
    dire_team_candidates: Optional[List[str]] = None,
) -> Dict[str, dict]:
    """Fetch bookmaker odds in tabs of the shared Camoufox browser."""
    if not BOOKMAKER_CAMOUFOX_IMPORTED or _bookmaker_parse_site_in_camoufox_page is None:
        raise RuntimeError("Camoufox not available")

    urls = _bookmaker_urls_for_mode(mode)
    selected_sites = [site for site in BOOKMAKER_PREFETCH_SITES if site in urls]

    team1 = str(radiant_team or "")
    team2 = str(dire_team or "")

    def _job(browser) -> Dict[str, dict]:
        results: Dict[str, dict] = {}
        for site in selected_sites:
            page = browser.new_page()
            try:
                result = _bookmaker_parse_site_in_camoufox_page(
                    page,
                    site=site,
                    url=urls[site],
                    team1=team1,
                    team2=team2,
                    mode=mode,
                    forced_map_num=map_num,
                )
                results[site] = {
                    "status": result.status,
                    "match_found": result.match_found,
                    "odds": result.odds,
                    "match_odds": result.match_odds,
                    "source": result.source,
                    "details": result.details[:500] if result.details else "",
                    "market_closed": result.market_closed,
                }
            except Exception as e:
                results[site] = {
                    "status": "error",
                    "match_found": False,
                    "odds": [],
                    "match_odds": [],
                    "source": "camoufox_direct_error",
                    "details": str(e)[:500],
                    "market_closed": False,
                }
            finally:
                with contextlib.suppress(Exception):
                    page.close()
        return results

    return _run_shared_camoufox_job("bookmaker-prefetch", _job, timeout=180)


def _bookmaker_prefetch_loop() -> None:
    """Bookmaker prefetch worker. Reuses a single Camoufox browser when CAMOUFOX_ENABLED=1."""
    driver = None
    driver_tasks_done = 0
    browser_idle_check_counter = 0
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
            # Periodic idle check
            browser_idle_check_counter += 1
            if browser_idle_check_counter >= 10:  # Every ~5 seconds
                browser_idle_check_counter = 0
                if _should_close_camoufox_browser():
                    _close_camoufox_browser()
            continue

        match_key = str(task.get("match_key") or "")
        radiant_team = str(task.get("radiant_team") or "")
        dire_team = str(task.get("dire_team") or "")
        radiant_team_candidates = list(task.get("radiant_team_candidates") or [radiant_team])
        dire_team_candidates = list(task.get("dire_team_candidates") or [dire_team])
        task_map_num_raw = task.get("map_num")
        try:
            task_map_num = int(task_map_num_raw) if task_map_num_raw is not None else None
        except (TypeError, ValueError):
            task_map_num = None
        if task_map_num is not None and not (1 <= task_map_num <= 5):
            task_map_num = None
        mode = str(task.get("mode") or BOOKMAKER_PREFETCH_MODE)
        try:
            if BOOKMAKER_PREFETCH_USE_SUBPROCESS and BOOKMAKER_CAMOUFOX_ENABLED and BOOKMAKER_CAMOUFOX_IMPORTED:
                # Direct Camoufox mode: reuse browser, no subprocess
                sites_payload = _bookmaker_prefetch_fetch_camoufox_direct(
                    radiant_team=radiant_team,
                    dire_team=dire_team,
                    mode=mode,
                    map_num=task_map_num,
                    radiant_team_candidates=radiant_team_candidates,
                    dire_team_candidates=dire_team_candidates,
                )
            elif BOOKMAKER_PREFETCH_USE_SUBPROCESS:
                sites_payload = _bookmaker_prefetch_fetch_subprocess(
                    radiant_team=radiant_team,
                    dire_team=dire_team,
                    mode=mode,
                    map_num=task_map_num,
                    radiant_team_candidates=radiant_team_candidates,
                    dire_team_candidates=dire_team_candidates,
                )
            else:
                with bookmaker_browser_lock:
                    if not _bookmaker_browser_bootstrap_unlocked(mode):
                        raise RuntimeError("bookmaker browser session bootstrap failed")
                    driver = bookmaker_browser_driver
                    driver_tasks_done = 0
                    sites_payload = _bookmaker_open_cached_match_tabs_unlocked(
                        match_key,
                        radiant_team=radiant_team,
                        dire_team=dire_team,
                        radiant_team_candidates=radiant_team_candidates,
                        dire_team_candidates=dire_team_candidates,
                        mode=mode,
                        map_num=task_map_num,
                    )
            with bookmaker_prefetch_condition:
                payload = bookmaker_prefetch_results.get(match_key)
                if isinstance(payload, dict):
                    payload["status"] = "done"
                    payload["finished_at"] = time.time()
                    payload["sites"] = sites_payload
                bookmaker_prefetch_condition.notify_all()
            print(
                "   ✅ Bookmaker prefetch done: "
                f"{match_key} ({_bookmaker_sites_compact_summary(sites_payload)})"
            )
        except Exception as e:
            with bookmaker_prefetch_condition:
                payload = bookmaker_prefetch_results.get(match_key)
                if isinstance(payload, dict):
                    payload["status"] = "error"
                    payload["finished_at"] = time.time()
                    payload["error"] = str(e)
                bookmaker_prefetch_condition.notify_all()
            print(f"   ⚠️ Bookmaker prefetch error for {match_key}: {e}")
            # For Camoufox direct mode, don't close browser on error - just log and continue
            if not (BOOKMAKER_PREFETCH_USE_SUBPROCESS and BOOKMAKER_CAMOUFOX_ENABLED and BOOKMAKER_CAMOUFOX_IMPORTED):
                try:
                    if driver is not None and BOOKMAKER_PREFETCH_USE_SUBPROCESS:
                        driver.quit()
                except Exception:
                    pass
                if BOOKMAKER_PREFETCH_USE_SUBPROCESS:
                    driver = None
                    driver_tasks_done = 0

    # Cleanup: close persistent Camoufox browser on exit
    _close_camoufox_browser()
    try:
        if driver is not None and not (BOOKMAKER_PREFETCH_USE_SUBPROCESS and BOOKMAKER_CAMOUFOX_ENABLED and BOOKMAKER_CAMOUFOX_IMPORTED):
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
    global bookmaker_prefetch_thread, bookmaker_browser_driver, bookmaker_browser_base_handles, bookmaker_browser_match_tabs
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
    with bookmaker_browser_lock:
        try:
            if bookmaker_browser_driver is not None:
                bookmaker_browser_driver.quit()
        except Exception:
            pass
        bookmaker_browser_driver = None
        bookmaker_browser_base_handles = {}
        bookmaker_browser_match_tabs = OrderedDict()
    print("🧵 Bookmaker prefetch worker stopped")

HERO_VALID_POSITIONS_DICT = {}
HERO_POSITION_COUNTS = {}
HERO_ID_TO_NAME = {}
try:
    with (BASE_DIR / 'hero_position_stats.json').open('r', encoding='utf-8') as f:
        _raw_position_stats = json.load(f)
    _min_position_pct = float(os.getenv("HERO_POSITION_STATS_MIN_PERCENTAGE", "1") or "1")
    for raw_hero_id, payload in (_raw_position_stats or {}).items():
        if not str(raw_hero_id).isdigit() or not isinstance(payload, dict):
            continue
        hero_id = str(raw_hero_id)
        hero_name = payload.get("hero_name")
        if hero_name:
            HERO_ID_TO_NAME[hero_id] = str(hero_name)
        raw_positions = payload.get("positions")
        if not isinstance(raw_positions, dict):
            continue
        valid_positions = []
        position_counts = {}
        for raw_pos, pos_stats in raw_positions.items():
            if not str(raw_pos).isdigit() or not isinstance(pos_stats, dict):
                continue
            pos_num = int(raw_pos)
            if not 1 <= pos_num <= 5:
                continue
            try:
                games = int(pos_stats.get("games", 0) or 0)
                percentage = float(pos_stats.get("percentage", 0) or 0)
            except (TypeError, ValueError):
                continue
            pos_label = f"POSITION_{pos_num}"
            if games > 0:
                position_counts[pos_label] = games
            if percentage >= _min_position_pct:
                valid_positions.append(pos_label)
        if valid_positions:
            HERO_VALID_POSITIONS_DICT[hero_id] = valid_positions
        if position_counts:
            HERO_POSITION_COUNTS[hero_id] = position_counts
except Exception as e:
    print(f"⚠️ Не удалось загрузить hero_position_stats.json: {e}")
    _report_missing_runtime_file(
        "hero_position_stats.json",
        BASE_DIR / "hero_position_stats.json",
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
bookmaker_browser_lock = threading.RLock()
bookmaker_browser_driver = None
bookmaker_camoufox_browser = None  # Persistent Camoufox browser for reuse
bookmaker_camoufox_browser_last_used = 0.0  # Timestamp of last use
bookmaker_browser_base_handles: Dict[str, str] = {}
bookmaker_browser_match_tabs: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
processed_urls_cache = set()
processed_urls_lock = threading.Lock()
verbose_match_log_cache: "OrderedDict[str, None]" = OrderedDict()
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
runtime_cycle_counter = 0

VERBOSE_MATCH_LOG_CACHE_MAX_SIZE = 5000
RUNTIME_MEMORY_SNAPSHOT_EVERY_CYCLES = 5
RUNTIME_MEMORY_SNAPSHOT_RSS_ALERT_MB = 1536.0

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
post_lane_dict = None
tempo_solo_dict = None
tempo_duo_dict = None
tempo_cp1v1_dict = None
late_comeback_ceiling_data = {}
late_comeback_ceiling_thresholds = {}
late_comeback_ceiling_max_minute = None
late_pub_comeback_table_data = {}
late_pub_comeback_table_thresholds_by_wr = {}
late_pub_comeback_table_max_minute_by_wr = {}
late_pub_comeback_table_global_max_minute = None
STATS_SEQUENTIAL_WARMUP_ENABLED = _safe_bool_env("STATS_SEQUENTIAL_WARMUP_ENABLED", True)
STATS_WARMUP_STEP_DELAY_SECONDS = _safe_float_env("STATS_WARMUP_STEP_DELAY_SECONDS", 45.0)
STATS_SHARDED_LOOKUP_MODE = str(os.getenv("STATS_SHARDED_LOOKUP_MODE", "auto")).strip().lower() or "auto"
STATS_SHARDED_LOOKUP_MAX_RAM_GB = _safe_float_env("STATS_SHARDED_LOOKUP_MAX_RAM_GB", 8.0)
STATS_LOOKUP_BACKEND = str(os.getenv("STATS_LOOKUP_BACKEND", "auto")).strip().lower() or "auto"
STATS_SHARD_CACHE_MAX = _safe_int_env("STATS_SHARD_CACHE_MAX", 0)
STATS_SHARD_KEY_CACHE_MAX = _safe_int_env("STATS_SHARD_KEY_CACHE_MAX", 20000)
STATS_SHARD_BUILD_PROGRESS_EVERY = _safe_int_env("STATS_SHARD_BUILD_PROGRESS_EVERY", 500000)
STATS_SQLITE_AUTOBUILD = _safe_bool_env("STATS_SQLITE_AUTOBUILD", False)
STATS_SQLITE_BUILD_FROM_SHARDS = _safe_bool_env("STATS_SQLITE_BUILD_FROM_SHARDS", True)
STATS_SQLITE_BUILD_BATCH_SIZE = _safe_int_env("STATS_SQLITE_BUILD_BATCH_SIZE", 50000)
STATS_SQLITE_BUILD_PROGRESS_EVERY = _safe_int_env("STATS_SQLITE_BUILD_PROGRESS_EVERY", 500000)
STATS_SQLITE_QUERY_CHUNK_SIZE = _safe_int_env("STATS_SQLITE_QUERY_CHUNK_SIZE", 800)
STATS_SQLITE_FALLBACK_TO_JSONL = _safe_bool_env("STATS_SQLITE_FALLBACK_TO_JSONL", True)
STATS_DRAFT_SCOPED_LOOKUP_ENABLED = _safe_bool_env("STATS_DRAFT_SCOPED_LOOKUP_ENABLED", True)
stats_warmup_last_heavy_load_ts = 0.0

# Настройка прокси
def _build_runtime_match_proxy_pool() -> list[str]:
    proxy_candidates: list[str] = []
    if isinstance(DLTV_PROXY_POOL, (list, tuple, set)):
        for item in DLTV_PROXY_POOL:
            candidate = str(item).strip()
            if candidate and candidate not in proxy_candidates:
                proxy_candidates.append(candidate)
    if proxy_candidates:
        return proxy_candidates
    return [str(p).strip() for p in api_to_proxy.keys() if str(p).strip()]


PROXY_LIST = _build_runtime_match_proxy_pool()
CURRENT_PROXY_INDEX = 0
CURRENT_PROXY = None
PROXIES = {}
USE_PROXY = None
# Live source mode: "cyberscore" (Camoufox/proxy), "api" (curl/proxy), or "html" (Camoufox/Selenium).
DLTV_SOURCE_MODE = str(os.getenv("DLTV_SOURCE_MODE", "cyberscore")).strip().lower() or "cyberscore"
CYBERSCORE_MATCHES_URL = str(
    os.getenv(
        "CYBERSCORE_MATCHES_URL",
        "https://cyberscore.live/en/matches/?type=liveOrUpcoming&tournament_tier=1%2C2",
    )
).strip()
CYBERSCORE_GET_HEADS_FALLBACK = _env_flag("CYBERSCORE_GET_HEADS_FALLBACK", "0")
CYBERSCORE_CAMOUFOX_PROXY_URL = str(os.getenv("CYBERSCORE_CAMOUFOX_PROXY_URL", "")).strip()
CYBERSCORE_CAMOUFOX_REQUIRE_PROXY = _env_flag("CYBERSCORE_CAMOUFOX_REQUIRE_PROXY", "1")
CYBERSCORE_LISTING_ITEM_CACHE: Dict[str, Dict[str, Any]] = {}
GET_HEADS_FAILURE_REASON_LIVE_MATCHES_MISSING_ALL_PROXIES = "live_matches_missing_after_all_proxies"
GET_HEADS_FAILURE_REASON_REQUEST_FAILED = "request_failed"
GET_HEADS_LAST_FAILURE_REASON = None
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
QUIET_HOURS_START_HOUR_MSK = 3
QUIET_HOURS_END_HOUR_MSK = 7
SCHEDULE_ONLY_IDLE_START_HOUR_MSK = 0
NEXT_SCHEDULE_SLEEP_SECONDS = 0.0
NEXT_SCHEDULE_MATCH_INFO: Optional[Dict[str, Any]] = None
PENDING_SCHEDULE_WAKE_AUDIT: Optional[Dict[str, Any]] = None
SCHEDULE_LIVE_WAIT_TARGET: Optional[Dict[str, Any]] = None
LIVE_MATCHES_MISSING_ALERT_ACTIVE = False
PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE = False
SCHEDULE_WAKE_LEAD_SECONDS = _safe_float_env("SCHEDULE_WAKE_LEAD_SECONDS", 30.0 * 60.0)
SCHEDULE_MAX_SLEEP_SECONDS = _safe_float_env("SCHEDULE_MAX_SLEEP_SECONDS", 5.0 * 60.0)
SCHEDULE_LONG_IDLE_THRESHOLD_SECONDS = _safe_float_env("SCHEDULE_LONG_IDLE_THRESHOLD_SECONDS", 30.0 * 60.0)
SCHEDULE_NEAR_MATCH_POLL_SECONDS = _safe_float_env("SCHEDULE_NEAR_MATCH_POLL_SECONDS", 60.0)
SCHEDULE_POST_START_POLL_SECONDS = _safe_float_env("SCHEDULE_POST_START_POLL_SECONDS", 3.0 * 60.0)
CYBERSCORE_QUIET_HOURS_START_HOUR_MSK = _safe_int_env("CYBERSCORE_QUIET_HOURS_START_HOUR_MSK", 0)
CYBERSCORE_QUIET_HOURS_END_HOUR_MSK = _safe_int_env("CYBERSCORE_QUIET_HOURS_END_HOUR_MSK", 7)
CYBERSCORE_SCHEDULE_POLL_SECONDS = _safe_float_env("CYBERSCORE_SCHEDULE_POLL_SECONDS", 30.0 * 60.0)
CYBERSCORE_QUIET_HOURS_PROBE_ENABLED = _env_flag("CYBERSCORE_QUIET_HOURS_PROBE_ENABLED", "1")
TELEGRAM_ADMIN_COMMAND_POLL_INTERVAL_SECONDS = _safe_float_env(
    "TELEGRAM_ADMIN_COMMAND_POLL_INTERVAL_SECONDS",
    15.0,
)
PROXY_POOL_ROTATION_ROUNDS = max(1, _safe_int_env("PROXY_POOL_ROTATION_ROUNDS", 3))


def _env_use_proxy_default() -> bool:
    env_use_proxy = os.getenv("USE_PROXY")
    if env_use_proxy is None:
        return True
    return env_use_proxy.strip().lower() not in {"0", "false", "no", "off"}


def _matches_request_headers(headers: Optional[dict]) -> dict:
    prepared = dict(headers or {})
    prepared.pop("X-Requested-With", None)
    prepared["Accept"] = (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    )
    return prepared


def _live_series_request_headers(headers: Optional[dict]) -> dict:
    prepared = dict(headers or {})
    prepared.pop("Host", None)
    prepared.pop("X-Requested-With", None)
    prepared["Accept"] = "application/json, text/plain, */*"
    return prepared


def _should_use_curl_transport(url: str) -> bool:
    try:
        parsed = urlparse(str(url))
    except Exception:
        return False
    normalized_path = (parsed.path or "").rstrip("/")
    return parsed.scheme in {"http", "https"} and normalized_path == "/matches"


def _http_request_exceptions() -> tuple[type, ...]:
    exceptions: List[type] = [requests.exceptions.RequestException]
    if CurlCffiRequestException is not None:
        exceptions.append(CurlCffiRequestException)
    return tuple(exceptions)


def _perform_http_get(
    url: str,
    *,
    headers: Optional[dict] = None,
    verify: bool = False,
    timeout: float = 10,
    proxies: Optional[dict] = None,
):
    use_curl = CURL_CFFI_AVAILABLE and curl_cffi_requests is not None and _should_use_curl_transport(url)
    request_headers = _matches_request_headers(headers) if use_curl else dict(headers or {})
    if use_curl:
        return curl_cffi_requests.get(
            url,
            headers=request_headers,
            verify=verify,
            timeout=timeout,
            proxies=proxies,
            impersonate="chrome136",
        )
    return requests.get(
        url,
        headers=request_headers,
        verify=verify,
        timeout=timeout,
        proxies=proxies,
    )


def _build_live_series_lookup(series_payload: Any) -> Dict[str, dict]:
    if isinstance(series_payload, dict):
        result: Dict[str, dict] = {}
        for key, value in series_payload.items():
            if isinstance(value, dict):
                result[str(key)] = value
        return result
    if isinstance(series_payload, list):
        result = {}
        for item in series_payload:
            if not isinstance(item, dict):
                continue
            series_id = item.get("id")
            if series_id is None:
                continue
            result[str(series_id)] = item
        return result
    return {}


def _render_live_series_json_cards(payload: Any) -> List[Any]:
    if not isinstance(payload, dict):
        return []
    live_map = payload.get("live")
    if not isinstance(live_map, dict) or not live_map:
        return []

    series_lookup = _build_live_series_lookup(payload.get("upcoming"))
    if not series_lookup:
        series_lookup.update(_build_live_series_lookup(payload.get("results")))

    cards: List[Any] = []
    for live_match_id, series_id in live_map.items():
        series_key = str(series_id or "").strip()
        series_item = series_lookup.get(series_key)
        if not isinstance(series_item, dict):
            continue

        series_numeric_id = str(series_item.get("id") or series_key).strip()
        slug = str(series_item.get("slug") or "").strip()
        if not slug:
            continue

        first_team = series_item.get("first_team") if isinstance(series_item.get("first_team"), dict) else {}
        second_team = series_item.get("second_team") if isinstance(series_item.get("second_team"), dict) else {}
        first_title = str(first_team.get("title") or "Team 1").strip()
        second_title = str(second_team.get("title") or "Team 2").strip()

        scores = series_item.get("series_scores") if isinstance(series_item.get("series_scores"), dict) else {}
        first_score = int(_coerce_int(scores.get("first_team")) or 0)
        second_score = int(_coerce_int(scores.get("second_team")) or 0)

        event_title = ""
        event_payload = series_item.get("event")
        if isinstance(event_payload, dict):
            event_title = str(event_payload.get("title") or "").strip()
        if not event_title:
            event_title = str(series_item.get("league_name") or "Live series").strip()

        href = f"https://dltv.org/matches/{series_numeric_id}/{slug}"
        card_html = f"""
        <div class="match live" data-series-id="{html_escape(series_numeric_id)}" data-match="{html_escape(str(live_match_id))}">
          <a href="{html_escape(href)}"></a>
          <div class="match__head">
            <div class="match__head-event"><span>{html_escape(event_title)}</span></div>
          </div>
          <div class="match__body">
            <div class="match__body-details">
              <div class="match__body-details__team">
                <div class="team"><div class="team__title"><span>{html_escape(first_title)}</span></div></div>
              </div>
              <div class="match__body-details__score">
                <div class="score"><strong class="text-red">0</strong><small>({first_score})</small></div>
                <div class="duration"><div class="duration__time"><strong>live</strong></div></div>
                <div class="score"><strong class="text-red">0</strong><small>({second_score})</small></div>
              </div>
              <div class="match__body-details__team">
                <div class="team"><div class="team__title"><span>{html_escape(second_title)}</span></div></div>
              </div>
            </div>
          </div>
        </div>
        """
        soup = BeautifulSoup(card_html, "lxml")
        card = soup.find("div", class_="match")
        if card is not None:
            cards.append(card)
    return cards


def _fetch_live_series_json_cards(
    *,
    headers: Optional[dict] = None,
    proxies: Optional[dict] = None,
) -> List[Any]:
    live_series_url = "https://dltv.org/live/series.json"
    response = _perform_http_get(
        live_series_url,
        headers=_live_series_request_headers(headers),
        verify=False,
        timeout=10,
        proxies=proxies,
    )
    if response is None or response.status_code != 200:
        return []
    try:
        payload = response.json()
    except Exception:
        return []
    return _render_live_series_json_cards(payload)


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


def _compute_cyberscore_quiet_hours_sleep_seconds(now: Optional[datetime] = None) -> float:
    current = now.astimezone(MOSCOW_TZ) if now is not None else datetime.now(MOSCOW_TZ)
    start_hour = int(CYBERSCORE_QUIET_HOURS_START_HOUR_MSK) % 24
    end_hour = int(CYBERSCORE_QUIET_HOURS_END_HOUR_MSK) % 24
    if start_hour == end_hour:
        return 0.0
    midnight = current.replace(hour=0, minute=0, second=0, microsecond=0)
    if start_hour < end_hour:
        in_window = start_hour <= current.hour < end_hour
        wake_at = midnight + timedelta(hours=end_hour)
    else:
        in_window = current.hour >= start_hour or current.hour < end_hour
        wake_at = midnight + timedelta(hours=end_hour)
        if current.hour >= start_hour:
            wake_at += timedelta(days=1)
    if not in_window:
        return 0.0
    if wake_at <= current:
        wake_at += timedelta(days=1)
    return max(0.0, (wake_at - current).total_seconds())


def _seconds_until_cyberscore_quiet_start(now: Optional[datetime] = None) -> float:
    current = now.astimezone(MOSCOW_TZ) if now is not None else datetime.now(MOSCOW_TZ)
    if _compute_cyberscore_quiet_hours_sleep_seconds(current) > 0:
        return 0.0
    start_hour = int(CYBERSCORE_QUIET_HOURS_START_HOUR_MSK) % 24
    end_hour = int(CYBERSCORE_QUIET_HOURS_END_HOUR_MSK) % 24
    if start_hour == end_hour:
        return 0.0
    midnight = current.replace(hour=0, minute=0, second=0, microsecond=0)
    quiet_start = midnight + timedelta(hours=start_hour)
    if quiet_start <= current:
        quiet_start += timedelta(days=1)
    return max(0.0, (quiet_start - current).total_seconds())


def _cap_cyberscore_schedule_sleep_seconds(
    sleep_seconds: float,
    *,
    now_utc: Optional[datetime] = None,
) -> float:
    current_msk = now_utc.astimezone(MOSCOW_TZ) if now_utc is not None else datetime.now(MOSCOW_TZ)
    seconds_until_quiet = _seconds_until_cyberscore_quiet_start(current_msk)
    if seconds_until_quiet <= 0:
        return max(0.0, float(sleep_seconds or 0.0))
    return max(0.0, min(float(sleep_seconds or 0.0), seconds_until_quiet))


def _cyberscore_schedule_before_quiet_end(
    schedule_info: Optional[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> bool:
    if not isinstance(schedule_info, dict):
        return False
    current = now.astimezone(MOSCOW_TZ) if now is not None else datetime.now(MOSCOW_TZ)
    quiet_sleep = _compute_cyberscore_quiet_hours_sleep_seconds(current)
    if quiet_sleep <= 0:
        return False
    quiet_end = current + timedelta(seconds=quiet_sleep)
    scheduled_at = schedule_info.get("scheduled_at_msk")
    if not isinstance(scheduled_at, datetime):
        scheduled_at_utc = schedule_info.get("scheduled_at_utc")
        if isinstance(scheduled_at_utc, datetime):
            scheduled_at = scheduled_at_utc.astimezone(MOSCOW_TZ)
    if not isinstance(scheduled_at, datetime):
        return False
    scheduled_at_msk = scheduled_at.astimezone(MOSCOW_TZ)
    return current <= scheduled_at_msk <= quiet_end


def _cyberscore_quiet_hours_sleep_seconds_with_probe() -> float:
    quiet_sleep_seconds = _compute_cyberscore_quiet_hours_sleep_seconds()
    if quiet_sleep_seconds <= 0:
        return 0.0

    now_msk = datetime.now(MOSCOW_TZ)
    for known_schedule in (SCHEDULE_LIVE_WAIT_TARGET, NEXT_SCHEDULE_MATCH_INFO):
        if _cyberscore_schedule_before_quiet_end(known_schedule, now=now_msk):
            print(
                "🌙 CyberScore quiet hours skipped: known scheduled match before 07:00 MSK "
                f"({_format_schedule_match_label(known_schedule)})"
            )
            return 0.0

    if not CYBERSCORE_QUIET_HOURS_PROBE_ENABLED:
        return quiet_sleep_seconds

    print("🌙 CyberScore quiet hours: probing tier1/2 schedule before sleeping")
    heads, bodies = _get_cyberscore_heads_via_camoufox()
    if heads:
        print("🌙 CyberScore quiet hours skipped: live match is already available")
        return 0.0
    if heads is None:
        retry_sleep = min(float(quiet_sleep_seconds), float(CYBERSCORE_SCHEDULE_POLL_SECONDS))
        print(
            "🌙 CyberScore quiet-hours probe failed. "
            f"Will retry after {int(math.ceil(retry_sleep))}s instead of sleeping until 07:00"
        )
        return retry_sleep
    if _cyberscore_schedule_before_quiet_end(NEXT_SCHEDULE_MATCH_INFO, now=now_msk):
        print(
            "🌙 CyberScore quiet hours skipped: nearest scheduled match is before 07:00 MSK "
            f"({_format_schedule_match_label(NEXT_SCHEDULE_MATCH_INFO)})"
        )
        return 0.0
    return quiet_sleep_seconds


def _should_use_schedule_sleep_window(now: Optional[datetime] = None) -> bool:
    current = now.astimezone(MOSCOW_TZ) if now is not None else datetime.now(MOSCOW_TZ)
    return current.hour >= SCHEDULE_ONLY_IDLE_START_HOUR_MSK


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
    max_sleep = max(1.0, float(SCHEDULE_MAX_SLEEP_SECONDS))
    near_match_poll = max(1.0, float(SCHEDULE_NEAR_MATCH_POLL_SECONDS))
    wake_lead = max(0.0, float(SCHEDULE_WAKE_LEAD_SECONDS))
    if wake_lead > 0.0:
        if raw_seconds <= wake_lead:
            return min(raw_seconds, near_match_poll)
        return min(max(raw_seconds - wake_lead, near_match_poll), max_sleep)
    if raw_seconds >= float(SCHEDULE_LONG_IDLE_THRESHOLD_SECONDS):
        return min(raw_seconds, max_sleep)
    return min(raw_seconds, max_sleep)


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

    def _consider_candidate(
        *,
        scheduled_at: Optional[datetime],
        matchup: str,
        league_title: str,
        href: str = "",
    ) -> None:
        nonlocal best_payload, best_sleep_seconds
        if scheduled_at is None or scheduled_at <= current_utc:
            return
        if _is_skipped_live_league_candidate(league_title=league_title, href=href):
            return
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
                "league_title": league_title,
            }

    for event_tag in soup.find_all("a", class_="event"):
        event_name_tag = event_tag.find("div", class_="event__name")
        event_title = event_name_tag.get_text(" ", strip=True) if event_name_tag else ""
        time_tag = event_tag.find("div", class_="event__info-info__time")
        scheduled_at = _parse_dltv_schedule_timestamp(time_tag.get_text(" ", strip=True) if time_tag else "")
        match_item = event_tag.find_next("div", class_="match__item")
        team_tags = match_item.find_all("div", class_="match__item-team__name") if match_item else []
        team_names = [tag.get_text(" ", strip=True) for tag in team_tags if tag.get_text(" ", strip=True)]
        matchup = " vs ".join(team_names[:2]) if team_names else "unknown"
        href = str(event_tag.get("href") or "")
        _consider_candidate(
            scheduled_at=scheduled_at,
            matchup=matchup,
            league_title=event_title,
            href=href,
        )

    for match_tag in soup.select("div.match.upcoming[data-matches-odd]"):
        scheduled_at = _parse_dltv_schedule_timestamp(match_tag.get("data-matches-odd"))
        league_title = ""
        head_event = match_tag.find("div", class_="match__head-event")
        if head_event is not None:
            league_title = head_event.get_text(" ", strip=True)
        team_names = [
            tag.get_text(" ", strip=True)
            for tag in match_tag.select(".match__body-details__team .team__title span")
            if tag.get_text(" ", strip=True)
        ]
        matchup = " vs ".join(team_names[:2]) if team_names else "unknown"
        href = ""
        href_tag = match_tag.find("a", href=True)
        if href_tag is not None:
            href = str(href_tag.get("href") or "")
        _consider_candidate(
            scheduled_at=scheduled_at,
            matchup=matchup,
            league_title=league_title,
            href=href,
        )

    return best_payload


def _parse_cyberscore_schedule_timestamp_value(
    raw_value: Any,
    *,
    now_utc: Optional[datetime] = None,
) -> Optional[datetime]:
    if raw_value is None or raw_value is False:
        return None
    if isinstance(raw_value, datetime):
        value = raw_value
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool):
        try:
            timestamp = float(raw_value)
        except (TypeError, ValueError):
            return None
        if timestamp <= 0:
            return None
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        if timestamp > 1_000_000_000:
            try:
                return datetime.fromtimestamp(timestamp, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
        return None

    text = str(raw_value or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d{10,13}", text):
        try:
            return _parse_cyberscore_schedule_timestamp_value(float(text), now_utc=now_utc)
        except ValueError:
            return None

    iso_text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    current_msk = (now_utc.astimezone(MOSCOW_TZ) if now_utc is not None else datetime.now(MOSCOW_TZ))
    time_match = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", text)
    if not time_match:
        return None
    hour = int(time_match.group(1))
    minute = int(time_match.group(2))
    lower = text.lower()
    day_offset = 0
    if "tomorrow" in lower or "завтра" in lower:
        day_offset = 1
    elif "today" in lower or "сегодня" in lower:
        day_offset = 0

    date_match = re.search(r"\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b", text)
    if date_match:
        day = int(date_match.group(1))
        month = int(date_match.group(2))
        year_raw = date_match.group(3)
        year = current_msk.year if not year_raw else int(year_raw)
        if year < 100:
            year += 2000
        try:
            candidate = datetime(year, month, day, hour, minute, tzinfo=MOSCOW_TZ)
        except ValueError:
            return None
    else:
        candidate = current_msk.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=day_offset)
        if day_offset == 0 and candidate <= current_msk:
            candidate += timedelta(days=1)
    return candidate.astimezone(timezone.utc)


_CYBERSCORE_SCHEDULE_TIME_KEY_RE = re.compile(
    r"(start|begin|schedule|date|time|timestamp)",
    re.IGNORECASE,
)
_CYBERSCORE_SCHEDULE_TIME_SKIP_KEY_RE = re.compile(
    r"(update|create|finish|end|duration|timezone|zone|period|score)",
    re.IGNORECASE,
)


def _iter_cyberscore_schedule_time_values(payload: Any, *, depth: int = 0):
    if depth > 5:
        return
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_text = str(key or "")
            if _CYBERSCORE_SCHEDULE_TIME_SKIP_KEY_RE.search(key_text):
                continue
            if _CYBERSCORE_SCHEDULE_TIME_KEY_RE.search(key_text) and not isinstance(value, (dict, list, tuple)):
                yield value
            if isinstance(value, (dict, list, tuple)):
                yield from _iter_cyberscore_schedule_time_values(value, depth=depth + 1)
    elif isinstance(payload, (list, tuple)):
        for value in payload:
            if isinstance(value, (dict, list, tuple)):
                yield from _iter_cyberscore_schedule_time_values(value, depth=depth + 1)


def _extract_cyberscore_item_scheduled_at(
    item: Optional[Dict[str, Any]],
    *,
    now_utc: Optional[datetime] = None,
) -> Optional[datetime]:
    if not isinstance(item, dict):
        return None
    for value in _iter_cyberscore_schedule_time_values(item):
        parsed = _parse_cyberscore_schedule_timestamp_value(value, now_utc=now_utc)
        if parsed is not None:
            return parsed
    return None


def _extract_cyberscore_card_scheduled_at(
    card: Any,
    *,
    now_utc: Optional[datetime] = None,
) -> Optional[datetime]:
    if card is None or not getattr(card, "find_all", None):
        return None
    nodes = [card]
    nodes.extend(card.find_all(True))
    for node in nodes:
        attrs = getattr(node, "attrs", {}) or {}
        for attr_name, attr_value in attrs.items():
            attr_text = str(attr_name or "").lower()
            if not (
                attr_text in {"datetime", "date", "time", "timestamp"}
                or ("data-" in attr_text and any(marker in attr_text for marker in ("time", "date", "start", "schedule")))
            ):
                continue
            values = attr_value if isinstance(attr_value, (list, tuple)) else [attr_value]
            for value in values:
                parsed = _parse_cyberscore_schedule_timestamp_value(value, now_utc=now_utc)
                if parsed is not None:
                    return parsed
    text = card.get_text(" ", strip=True) if getattr(card, "get_text", None) else ""
    return _parse_cyberscore_schedule_timestamp_value(text, now_utc=now_utc)


def _extract_cyberscore_team_names_from_item(item: Optional[Dict[str, Any]]) -> List[str]:
    names: List[str] = []

    def _push(value: Any) -> None:
        text = str(value or "").strip()
        if text and text.lower() not in {"tbd", "vs"} and text not in names:
            names.append(text)

    if not isinstance(item, dict):
        return names
    for key in ("teams", "opponents", "participants"):
        raw_teams = item.get(key)
        if isinstance(raw_teams, list):
            for team in raw_teams:
                if isinstance(team, dict):
                    _push(team.get("name") or team.get("title") or team.get("short_name"))
    for key in ("team1", "team2", "radiantTeam", "direTeam", "team_radiant", "team_dire"):
        team = item.get(key)
        if isinstance(team, dict):
            _push(team.get("name") or team.get("title") or team.get("short_name"))
        else:
            _push(team)
    return names[:2]


def _extract_cyberscore_team_names_from_card(card: Any) -> List[str]:
    if card is None or not getattr(card, "select", None):
        return []
    selectors = [
        "[class*='team'][class*='name']",
        "[class*='team'] [class*='name']",
        "[class*='competitor'][class*='name']",
        "[class*='opponent'][class*='name']",
    ]
    names: List[str] = []
    for selector in selectors:
        for node in card.select(selector):
            text = node.get_text(" ", strip=True)
            if text and text.lower() not in {"tbd", "vs"} and text not in names:
                names.append(text)
            if len(names) >= 2:
                return names[:2]
    return names[:2]


def _extract_cyberscore_league_title_from_item(item: Optional[Dict[str, Any]]) -> str:
    if not isinstance(item, dict):
        return ""
    for key in ("tournament", "league", "event"):
        value = item.get(key)
        if isinstance(value, dict):
            title = str(value.get("name") or value.get("title") or "").strip()
            if title:
                return title
        elif value:
            return str(value).strip()
    return ""


def _extract_nearest_cyberscore_scheduled_match_info(
    html: str,
    *,
    now_utc: Optional[datetime] = None,
) -> Optional[dict[str, Any]]:
    current_utc = now_utc.astimezone(timezone.utc) if now_utc is not None else datetime.now(timezone.utc)
    soup = BeautifulSoup(html or "", "lxml")
    best_payload: Optional[dict[str, Any]] = None
    best_raw_sleep: Optional[float] = None

    for card in soup.select("a.matches-item[href*='/matches/']"):
        classes = {str(item).strip().lower() for item in (card.get("class") or [])}
        text = card.get_text(" ", strip=True)
        if "online" in classes or "LIVE" in text.upper():
            continue
        href = _absolute_cyberscore_url(str(card.get("href") or ""))
        match_id = _extract_cyberscore_match_id_from_href(href)
        item = _extract_cyberscore_match_item_from_html(html, match_id=match_id or None)
        scheduled_at = (
            _extract_cyberscore_item_scheduled_at(item, now_utc=current_utc)
            or _extract_cyberscore_card_scheduled_at(card, now_utc=current_utc)
        )
        if scheduled_at is None or scheduled_at <= current_utc:
            continue
        raw_sleep = max(0.0, (scheduled_at - current_utc).total_seconds())
        if best_raw_sleep is not None and raw_sleep >= best_raw_sleep:
            continue
        team_names = _extract_cyberscore_team_names_from_item(item) or _extract_cyberscore_team_names_from_card(card)
        matchup = " vs ".join(team_names[:2]) if len(team_names) >= 2 else "unknown"
        league_title = _extract_cyberscore_league_title_from_item(item)
        sleep_seconds = _cap_cyberscore_schedule_sleep_seconds(
            _compute_schedule_recheck_sleep_seconds(raw_sleep),
            now_utc=current_utc,
        )
        best_raw_sleep = raw_sleep
        best_payload = {
            "scheduled_at_utc": scheduled_at,
            "scheduled_at_msk": scheduled_at.astimezone(MOSCOW_TZ),
            "sleep_seconds": sleep_seconds,
            "sleep_seconds_raw": raw_sleep,
            "matchup": matchup,
            "league_title": league_title,
            "href": href,
            "source": "cyberscore",
        }

    return best_payload


def _is_valid_dltv_matches_page(soup: Optional[BeautifulSoup], html_text: Any = "") -> bool:
    if soup is None:
        return False
    text = str(html_text or "")
    if not text.strip():
        return False
    title_text = ""
    if soup.title is not None:
        title_text = soup.title.get_text(" ", strip=True).lower()
    if "dltv" in title_text and "match" in title_text:
        return True
    if soup.find("div", class_="live__matches") is not None:
        return True
    if soup.select_one("div.match.upcoming[data-matches-odd]") is not None:
        return True
    if soup.select_one("a.event") is not None:
        return True
    return False


def _live_match_card_is_tbd(card: Any) -> bool:
    if card is None or not getattr(card, "get", None):
        return False
    class_names = {str(item).strip().lower() for item in (card.get("class") or [])}
    if "tbd" in class_names:
        return True
    match_anchor = card.select_one("a[href*='/matches/']") if getattr(card, "select_one", None) else None
    href = str(match_anchor.get("href") or "").strip().lower() if match_anchor is not None else ""
    if "/tbd-vs-tbd-" in href:
        return True
    team_titles = [
        tag.get_text(" ", strip=True).strip().lower()
        for tag in (card.select(".match__body-details__team .team__title span") or [])
    ]
    if len(team_titles) >= 2 and all(title == "tbd" for title in team_titles[:2]):
        return True
    return False


def _count_non_tbd_live_cards(cards: Any) -> int:
    if not cards:
        return 0
    return sum(1 for card in cards if not _live_match_card_is_tbd(card))


def _summarize_live_card_hrefs(cards: Any, *, limit: int = 10) -> List[str]:
    if not cards:
        return []
    result: List[str] = []
    for card in list(cards)[:limit]:
        anchor = card.select_one("a[href*='/matches/']") if getattr(card, "select_one", None) else None
        href = str(anchor.get("href") or "").strip() if anchor is not None else ""
        if href:
            result.append(href)
            continue
        series_id = str(card.get("data-series-id") or "").strip() if getattr(card, "get", None) else ""
        if series_id:
            result.append(f"series:{series_id}")
    return result


def _extract_live_listing_context(head_node: Any, body_node: Any) -> Dict[str, Any]:
    head_classes = set(head_node.get("class") or []) if getattr(head_node, "get", None) else set()
    body_classes = set(body_node.get("class") or []) if getattr(body_node, "get", None) else set()
    source_marker = ""
    if getattr(body_node, "get", None):
        source_marker = str(body_node.get("data-source") or "").strip().lower()
    if not source_marker and getattr(head_node, "get", None):
        source_marker = str(head_node.get("data-source") or "").strip().lower()
    if (
        source_marker == "cyberscore"
        or "matches-item" in body_classes
        or "matches-item" in head_classes
    ):
        card = body_node if getattr(body_node, "get", None) else head_node
        href = (
            str(card.get("data-cyberscore-href") or card.get("href") or "").strip()
            if getattr(card, "get", None)
            else ""
        )
        absolute_href = _absolute_cyberscore_url(href) if href else ""
        match_id = (
            str(card.get("data-cyberscore-match-id") or "").strip()
            if getattr(card, "get", None)
            else ""
        ) or _extract_cyberscore_match_id_from_href(absolute_href)
        text = card.get_text(" ", strip=True) if getattr(card, "get_text", None) else ""
        score_match = re.search(r"\b(\d+)\s*:\s*(\d+)\b", text)
        if score_match:
            left_score, right_score = int(score_match.group(1)), int(score_match.group(2))
        else:
            left_score, right_score = 0, 0
        status = "live" if "online" in body_classes or "LIVE" in text.upper() else "unknown"
        return {
            "layout": "cyberscore_match_card",
            "source": "cyberscore",
            "status": status,
            "score": f"{left_score} : {right_score}",
            "uniq_score": left_score + right_score,
            "href": absolute_href,
            "series_id": match_id,
            "live_match_id": match_id,
            "league_title": "",
            "match_card": card,
        }
    match_card = None
    if {"match", "live"}.issubset(body_classes):
        match_card = body_node
    elif {"match", "live"}.issubset(head_classes):
        match_card = head_node

    if match_card is not None:
        series_id = str(match_card.get("data-series-id") or "").strip()
        live_match_id = str(match_card.get("data-match") or "").strip()
        league_title_tag = match_card.select_one(".match__head-event span")
        league_title = league_title_tag.get_text(" ", strip=True) if league_title_tag else ""
        status_tag = match_card.select_one(".duration__time strong")
        status = status_tag.get_text(" ", strip=True).lower() if status_tag else "live"
        score_parts: List[str] = []
        for small_tag in match_card.select("div.match__body-details__score div.score small")[:2]:
            raw_text = small_tag.get_text(" ", strip=True)
            digits_match = re.search(r"-?\d+", raw_text)
            score_parts.append(digits_match.group(0) if digits_match else "0")
        while len(score_parts) < 2:
            score_parts.append("0")
        score = f"{score_parts[0]} : {score_parts[1]}"
        uniq_score = sum(int(part) for part in score_parts[:2])
        match_href = ""
        match_anchor = match_card.select_one("a[href*='/matches/']")
        if match_anchor is not None:
            match_href = str(match_anchor.get("href") or "").strip()
        return {
            "layout": "match_card_v2",
            "status": status,
            "score": score,
            "uniq_score": uniq_score,
            "href": match_href,
            "series_id": series_id,
            "live_match_id": live_match_id,
            "league_title": league_title,
            "match_card": match_card,
        }

    status_element = head_node.find('div', class_='event__info-info__time') if getattr(head_node, "find", None) else None
    status = status_element.text.lower() if status_element else 'unknown'
    score_divs = body_node.find_all('div', class_='match__item-team__score') if getattr(body_node, "find_all", None) else []
    score_values = [div.text.strip() for div in score_divs[:2]]
    while len(score_values) < 2:
        score_values.append("0")
    uniq_score = sum(int(value) for value in score_values[:2])
    score = f"{score_values[0]} : {score_values[1]}"
    link_tag = body_node.find('a') if getattr(body_node, "find", None) else None
    href = str(link_tag.get('href') or "").strip() if link_tag is not None else ""
    league_title = ""
    event_name = head_node.find('div', class_='event__name') if getattr(head_node, "find", None) else None
    if event_name is not None:
        league_title = event_name.get_text(" ", strip=True)
    return {
        "layout": "legacy_live_matches",
        "status": status,
        "score": score,
        "uniq_score": uniq_score,
        "href": href,
        "series_id": "",
        "live_match_id": "",
        "league_title": league_title,
        "match_card": None,
    }


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
    source_name = str(
        pending.get("source")
        or (next_schedule_info.get("source") if isinstance(next_schedule_info, dict) else "")
        or DLTV_SOURCE_MODE
        or "schedule"
    )
    source_label = "CyberScore" if "cyber" in source_name.lower() else "DLTV"
    proxy_marker = _get_current_proxy_marker()

    if heads_count > 0 and bodies_count > 0:
        print(
            "⏰ Wake audit: "
            f"woke at {woke_label} for {target_label}. "
            f"{source_label} response after wake: live matches found "
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
            f"{source_label} now points to next scheduled match: {next_label}"
        )
    else:
        print(
            "⏰ Wake audit: "
            f"woke at {woke_label} for {target_label}, but live matches are still empty "
            f"(heads={heads_count}, bodies={bodies_count}, proxy={proxy_marker}, request={request_status}). "
            f"{source_label} did not expose a new scheduled match either"
        )
    PENDING_SCHEDULE_WAKE_AUDIT = None


def _split_telegram_text_chunks(text: str, *, max_chars: int = 3500) -> List[str]:
    payload = str(text or "")
    if len(payload) <= max_chars:
        return [payload]
    chunks: List[str] = []
    current: List[str] = []
    current_len = 0
    for line in payload.splitlines():
        line_with_break = line + "\n"
        if current and current_len + len(line_with_break) > max_chars:
            chunks.append("".join(current).rstrip())
            current = [line_with_break]
            current_len = len(line_with_break)
            continue
        current.append(line_with_break)
        current_len += len(line_with_break)
    if current:
        chunks.append("".join(current).rstrip())
    return chunks or [payload[:max_chars]]


_ADMIN_MATCH_SUMMARY_PREFIXES = (
    "Статус:",
    "URL:",
    "Score:",
    "✅ Драфт успешно распарсен",
    "📌 Early signal:",
    "📌 Late signal:",
    "🛣️ Lanes:",
    "Top:",
    "Mid:",
    "Bot:",
    "📊 Team ELO attached:",
    "⚠️ Early star invalidated",
    "⚠️ Late star invalidated",
    "✅ Override:",
    "📈 Comeback solo:",
    "⏳ Ожидание dispatch:",
    "📉 Star checks:",
    "✅ ВЕРДИКТ:",
    "⚠️ ВЕРДИКТ:",
    "✅ map_id_check.txt обновлен:",
)
_ADMIN_DELAYED_OUTCOME_PATTERNS = (
    re.compile(r"^⏱️ Отложенный сигнал отправлен.*?: (?P<url>\S+)\b"),
    re.compile(r"^⏱️ Отложенный сигнал отменен без отправки: (?P<url>\S+)\b"),
)
_ADMIN_SUMMARY_MATCH_URL_RE = re.compile(r"(dltv\.org/matches/\d+/[^\s)]+?)\.\d+(?=$|[\s)])")
_ADMIN_SUMMARY_URL_LINE_RE = re.compile(r"^\s*URL:\s*(?P<url>\S+)\s*$")
_ADMIN_TAIL_LOG_RECENT_MATCH_SCAN_LIMIT = 60
_ADMIN_TAIL_LOG_SEND_LIMIT = 4
_ADMIN_TAIL_LOG_MAX_EXPANSION_STEPS = 4


def _is_admin_match_summary_line(compact_line: str) -> bool:
    compact = str(compact_line or "").strip()
    if not compact:
        return False
    return any(compact.startswith(prefix) for prefix in _ADMIN_MATCH_SUMMARY_PREFIXES)


def _normalize_admin_summary_line(raw_line: str) -> str:
    raw = str(raw_line or "").rstrip()
    return _ADMIN_SUMMARY_MATCH_URL_RE.sub(r"\1", raw)


def _format_admin_star_signal_summary_line(
    label: str,
    *,
    has_star: bool,
    sign: Optional[int],
    wr_pct: Optional[float],
    radiant_team_name: Optional[str],
    dire_team_name: Optional[str],
) -> str:
    if not has_star:
        return f"📌 {label}: no"

    side = _target_side_from_sign(sign)
    if side == "radiant":
        team_name = str(radiant_team_name or "radiant").strip() or "radiant"
    elif side == "dire":
        team_name = str(dire_team_name or "dire").strip() or "dire"
    else:
        team_name = "unknown"

    try:
        wr_label = (
            f"WR≈{float(wr_pct):.1f}%"
            if wr_pct is not None
            else "WR=n/a"
        )
    except (TypeError, ValueError):
        wr_label = "WR=n/a"

    if side in {"radiant", "dire"}:
        return f"📌 {label}: {team_name} ({side}) {wr_label}"
    return f"📌 {label}: {team_name} {wr_label}"


def _read_log_tail_lines(
    log_path: Path,
    *,
    max_lines: int = 12000,
    chunk_size: int = 65536,
    max_bytes: int = 2_000_000,
) -> List[str]:
    target_lines = max(1, int(max_lines))
    target_bytes = max(chunk_size, int(max_bytes))
    try:
        file_size = int(log_path.stat().st_size)
    except OSError:
        return []
    if file_size <= 0:
        return []

    chunks: List[bytes] = []
    bytes_read = 0
    newline_count = 0
    position = file_size

    with log_path.open("rb") as handle:
        while position > 0 and bytes_read < target_bytes and newline_count <= target_lines:
            read_size = min(chunk_size, position, target_bytes - bytes_read)
            position -= read_size
            handle.seek(position)
            chunk = handle.read(read_size)
            if not chunk:
                break
            chunks.append(chunk)
            bytes_read += len(chunk)
            newline_count += chunk.count(b"\n")

    if not chunks:
        return []

    raw_bytes = b"".join(reversed(chunks))
    if position > 0:
        first_newline = raw_bytes.find(b"\n")
        if first_newline >= 0:
            raw_bytes = raw_bytes[first_newline + 1 :]

    decoded = raw_bytes.decode("utf-8", errors="replace")
    lines = decoded.splitlines()
    if len(lines) > target_lines:
        lines = lines[-target_lines:]
    return lines


def _build_recent_match_summaries_entries(*, limit: int = 10, scan_lines: int = 12000) -> List[Dict[str, Any]]:
    log_path = PROJECT_ROOT / "log.txt"
    if not log_path.exists():
        return []

    try:
        raw_lines = _read_log_tail_lines(log_path, max_lines=scan_lines)
    except Exception:
        return []

    latest_blocks_by_url: Dict[str, Dict[str, Any]] = {}
    latest_delayed_outcome_by_url: Dict[str, Dict[str, Any]] = {}
    current_block: Optional[Dict[str, Any]] = None

    def _finalize_block() -> None:
        nonlocal current_block
        if not isinstance(current_block, dict):
            return
        block_url = str(current_block.get("url") or "").strip()
        lines = list(current_block.get("lines") or [])
        informative_count = int(current_block.get("informative_count") or 0)
        if block_url and lines and informative_count > 0:
            latest_blocks_by_url[block_url] = {
                "url": block_url,
                "lines": lines,
                "line_no": int(current_block.get("line_no") or 0),
            }
        current_block = None

    for line_no, raw_line in enumerate(raw_lines):
        raw = str(raw_line or "").rstrip()
        compact = raw.strip()
        if not compact:
            continue

        if compact.startswith("🔍 DEBUG: Начало обработки матча") or compact.startswith("🔁 RECHECK матча"):
            _finalize_block()
            current_block = {
                "url": "",
                "lines": [],
                "line_no": line_no,
                "interesting_count": 0,
                "informative_count": 0,
            }
            continue

        for pattern in _ADMIN_DELAYED_OUTCOME_PATTERNS:
            match = pattern.search(compact)
            if match:
                outcome_url = str(match.group("url") or "").strip()
                if outcome_url:
                    latest_delayed_outcome_by_url[outcome_url] = {
                        "line": compact,
                        "line_no": line_no,
                    }
                break

        if not isinstance(current_block, dict):
            continue

        if compact.startswith("URL:"):
            current_block["url"] = compact.split(":", 1)[1].strip()

        if _is_admin_match_summary_line(compact):
            block_lines = current_block.setdefault("lines", [])
            normalized_line = _normalize_admin_summary_line(raw)
            if normalized_line not in block_lines:
                block_lines.append(normalized_line)
            current_block["interesting_count"] = int(current_block.get("interesting_count") or 0) + 1
            if not (
                compact.startswith("Статус:")
                or compact.startswith("URL:")
                or compact.startswith("Score:")
            ):
                current_block["informative_count"] = int(current_block.get("informative_count") or 0) + 1

    _finalize_block()

    entries: List[Dict[str, Any]] = []
    for url, block in latest_blocks_by_url.items():
        lines = list(block.get("lines") or [])
        block_line_no = int(block.get("line_no") or 0)
        delayed_outcome = latest_delayed_outcome_by_url.get(url)
        ordering_line_no = block_line_no
        if isinstance(delayed_outcome, dict):
            delayed_line = _normalize_admin_summary_line(str(delayed_outcome.get("line") or "").strip())
            delayed_line_no = int(delayed_outcome.get("line_no") or block_line_no)
            ordering_line_no = max(ordering_line_no, delayed_line_no)
            if delayed_line and delayed_line not in [line.strip() for line in lines]:
                lines.append(delayed_line)
        entries.append(
            {
                "url": url,
                "lines": lines,
                "line_no": ordering_line_no,
            }
        )

    entries.sort(key=lambda item: int(item.get("line_no") or 0))
    return entries[-max(1, int(limit)) :]


def _build_recent_match_summaries_text(*, limit: int = 10, scan_lines: int = 12000) -> str:
    entries = _build_recent_match_summaries_entries(limit=limit, scan_lines=scan_lines)
    if not entries:
        return "recent match summaries: no informative match blocks found"

    parts: List[str] = []
    for idx, entry in enumerate(entries, start=1):
        lines = [str(line).rstrip() for line in entry.get("lines") or [] if str(line).strip()]
        if not lines:
            continue
        parts.append(f"[{idx}]")
        parts.extend(lines)
        parts.append("")
    payload = "\n".join(parts).strip()
    return payload or "recent match summaries: no informative match blocks found"


def _split_admin_match_summary_messages(payload: str) -> List[str]:
    raw_text = str(payload or "").strip()
    if not raw_text:
        return []
    messages: List[str] = []
    current_lines: List[str] = []
    for raw_line in raw_text.splitlines():
        line = str(raw_line or "").rstrip()
        compact = line.strip()
        if re.fullmatch(r"\[\d+\]", compact):
            if current_lines:
                messages.append("\n".join(current_lines).strip())
                current_lines = []
            continue
        if not compact and not current_lines:
            continue
        current_lines.append(line)
    if current_lines:
        messages.append("\n".join(current_lines).strip())
    return [message for message in messages if message]


def _extract_admin_summary_message_url(message: str) -> str:
    for raw_line in str(message or "").splitlines():
        match = _ADMIN_SUMMARY_URL_LINE_RE.match(str(raw_line or "").rstrip())
        if match:
            return str(match.group("url") or "").strip()
    return ""


def _admin_tail_log_seen_matches_path_for_mode(mode_label: str) -> Path:
    return _mode_specific_runtime_path(ADMIN_TAIL_LOG_SEEN_MATCHES_PATH, mode_label)


def _load_admin_tail_log_seen_urls(*, mode_label: str) -> List[str]:
    path = _admin_tail_log_seen_matches_path_for_mode(mode_label)
    try:
        return _load_json_url_array(
            path,
            recover=True,
            label="ADMIN_TAIL_LOG_SEEN_MATCHES_PATH",
        )
    except Exception:
        return []


def _save_admin_tail_log_seen_urls(urls: List[str], *, mode_label: str) -> None:
    unique_urls: List[str] = []
    seen: set[str] = set()
    for raw_url in urls:
        compact = str(raw_url or "").strip()
        if not compact or compact in seen:
            continue
        seen.add(compact)
        unique_urls.append(compact)
    if len(unique_urls) > 1000:
        unique_urls = unique_urls[-1000:]
    _write_json_atomic(
        _admin_tail_log_seen_matches_path_for_mode(mode_label),
        unique_urls,
    )


def _admin_tail_entry_is_finished_map(entry: Dict[str, Any]) -> bool:
    for raw_line in entry.get("lines") or []:
        compact = str(raw_line or "").strip()
        if not compact.startswith("Статус:"):
            continue
        status_value = compact.split(":", 1)[1].strip().lower()
        return status_value == "finished"
    return False


def _admin_tail_current_live_map_urls() -> Optional[set[str]]:
    try:
        answer = get_heads()
    except Exception:
        return None
    if not answer:
        return None
    try:
        heads, bodies = answer
    except Exception:
        return None
    if heads is None or bodies is None:
        return None

    live_urls: set[str] = set()
    try:
        pair_count = min(len(heads), len(bodies))
    except Exception:
        return None

    for idx in range(pair_count):
        try:
            listing_context = _extract_live_listing_context(heads[idx], bodies[idx])
            status = str(listing_context.get("status") or "unknown").strip().lower()
            if status == "finished":
                continue
            uniq_score = int(listing_context.get("uniq_score") or 0)
            href = str(listing_context.get("href") or "").strip()
            parsed_url = urlparse(href) if href else None
            path = str(parsed_url.path or "") if parsed_url else ""
            series_key_from_path = str(listing_context.get("series_id") or "")
            if not series_key_from_path and path:
                series_match = re.search(r"/matches/(\d+)", path)
                series_key_from_path = series_match.group(1) if series_match else ""
            check_uniq_url = (
                f'dltv.org{path}.{uniq_score}'
                if path
                else (f'dltv.org/matches/{series_key_from_path}.{uniq_score}' if series_key_from_path else "")
            )
            check_uniq_url = str(check_uniq_url or "").strip()
            if check_uniq_url:
                live_urls.add(check_uniq_url)
        except Exception:
            continue
    return live_urls


def _collect_admin_tail_unseen_messages(
    *,
    mode_label: str,
    line_count: int,
) -> Tuple[List[str], List[Tuple[str, str]], List[int]]:
    seen_urls = _load_admin_tail_log_seen_urls(mode_label=mode_label)
    seen_url_set = set(seen_urls)
    current_live_map_urls = _admin_tail_current_live_map_urls()
    requested_limits: List[int] = []
    unseen_messages: List[Tuple[str, str]] = []
    base_scan_lines = max(3000, int(line_count) * 60)
    limit = max(1, int(_ADMIN_TAIL_LOG_RECENT_MATCH_SCAN_LIMIT))
    scan_lines = base_scan_lines

    for _attempt in range(max(1, int(_ADMIN_TAIL_LOG_MAX_EXPANSION_STEPS))):
        requested_limits.append(limit)
        entries = _build_recent_match_summaries_entries(limit=limit, scan_lines=scan_lines)
        unseen_messages = []
        for entry in entries:
            match_url = str(entry.get("url") or "").strip()
            is_finished_map = _admin_tail_entry_is_finished_map(entry)
            disappeared_from_live = bool(
                current_live_map_urls is not None
                and match_url
                and match_url not in current_live_map_urls
            )
            if not (is_finished_map or disappeared_from_live):
                continue
            lines = [str(line).rstrip() for line in entry.get("lines") or [] if str(line).strip()]
            message = "\n".join(lines).strip()
            if not message:
                continue
            if match_url and match_url in seen_url_set:
                continue
            unseen_messages.append((match_url, message))
        if len(unseen_messages) >= int(_ADMIN_TAIL_LOG_SEND_LIMIT):
            break
        if not entries:
            break
        if len(entries) <= int(_ADMIN_TAIL_LOG_SEND_LIMIT) and len(unseen_messages) == len(entries):
            break
        limit *= 2
        scan_lines *= 2

    return seen_urls, unseen_messages, requested_limits


def _send_admin_log_tail(*, line_count: int = 100, raw_odds: Any = None) -> None:
    mode_label = _runtime_instance_mode_label(raw_odds)
    seen_urls, unseen_messages, _requested_limits = _collect_admin_tail_unseen_messages(
        mode_label=mode_label,
        line_count=line_count,
    )
    if not unseen_messages:
        send_message("tail_log: новых ставок нет", admin_only=True, mirror_to_vk=False)
        return
    newly_sent_urls: List[str] = []
    selected_messages = list(reversed(unseen_messages[-_ADMIN_TAIL_LOG_SEND_LIMIT:]))
    for match_url, message in selected_messages:
        for idx, chunk in enumerate(_split_telegram_text_chunks(message), start=1):
            prefix = ""
            if idx > 1:
                prefix = f"[part {idx}] "
            send_message(f"{prefix}{chunk}", admin_only=True, mirror_to_vk=False)
        if match_url:
            newly_sent_urls.append(match_url)
    if newly_sent_urls:
        _save_admin_tail_log_seen_urls(
            seen_urls + newly_sent_urls,
            mode_label=mode_label,
        )


def _build_self_restart_command(raw_odds: Any) -> Tuple[str, Path]:
    odds_enabled = _odds_requested_flag(raw_odds)
    mode_flag = "--odds" if odds_enabled else "--no-odds"
    gate_mode_flag = ""
    pure_dltv_flag = ""
    if odds_enabled:
        gate_mode_flag = f" --bookmaker-gate-mode {shlex.quote(str(BOOKMAKER_PREFETCH_GATE_MODE))}"
    if PURE_DLTV_MODE:
        pure_dltv_flag = " --pure-dltv"
    log_name = "cyberscore_odds.log" if odds_enabled else "cyberscore_noodds.log"
    log_path = PROJECT_ROOT / log_name
    command = (
        f"cd {shlex.quote(str(PROJECT_ROOT))} && "
        f"nohup {shlex.quote(str(sys.executable))} "
        f"{shlex.quote(str(BASE_DIR / 'cyberscore_try.py'))} {mode_flag}{gate_mode_flag}{pure_dltv_flag} "
        f">> {shlex.quote(str(log_path))} 2>&1 < /dev/null &"
    )
    return command, log_path


def _restart_current_runtime_from_admin_command(raw_odds: Any) -> None:
    command, log_path = _build_self_restart_command(raw_odds)
    print(f"🔄 Admin command: restarting runtime via detached command -> {command}")
    try:
        send_message(
            f"🔄 Перезапускаю bot.\nmode={_runtime_instance_mode_label(raw_odds)}\nlog={log_path.name}",
            admin_only=True,
            mirror_to_vk=False,
        )
    except Exception as exc:
        print(f"⚠️ Не удалось отправить admin restart ack: {exc}")
    _release_runtime_instance_lock()
    subprocess.Popen(
        ["/bin/bash", "-lc", command],
        cwd=str(PROJECT_ROOT),
        start_new_session=True,
    )
    raise SystemExit(0)


def _handle_pending_telegram_admin_commands(raw_odds: Any) -> None:
    try:
        commands = drain_telegram_admin_commands(refresh=True)
    except Exception as exc:
        logger.warning("Failed to poll Telegram admin commands: %s", exc)
        return
    if not commands:
        return
    for command_payload in commands:
        if not isinstance(command_payload, dict):
            continue
        command_name = str(command_payload.get("command") or "").strip()
        raw_text = str(command_payload.get("raw_text") or "").strip()
        print(f"📨 Admin command received: {command_name or raw_text}")
        if command_name == "tail_log_100":
            try:
                _send_admin_log_tail(line_count=100, raw_odds=raw_odds)
            except Exception as exc:
                try:
                    send_message(
                        f"⚠️ tail_log command failed: {exc}",
                        admin_only=True,
                        mirror_to_vk=False,
                    )
                except Exception:
                    pass
            continue
        if command_name == "restart_bot":
            _restart_current_runtime_from_admin_command(raw_odds)


def _sleep_interruptible(total_seconds: float, *, raw_odds: Any, label: str) -> None:
    remaining = max(0.0, float(total_seconds or 0.0))
    if remaining <= 0:
        _handle_pending_telegram_admin_commands(raw_odds)
        return
    deadline = time.time() + remaining
    while True:
        _handle_pending_telegram_admin_commands(raw_odds)
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        chunk = min(
            remaining,
            max(1.0, float(TELEGRAM_ADMIN_COMMAND_POLL_INTERVAL_SECONDS)),
        )
        time.sleep(chunk)


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
    CURRENT_PROXY_INDEX = 0
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


def _resolve_known_team_id_without_side_effects(team_ids: Any, team_name: str) -> int:
    """
    Resolve a known team id without mutating tier dictionaries.

    Priority:
    1. Exact candidate id that already matches a known alias by name.
    2. Known id resolved by name only.
    3. First candidate id that is already known as tier1/tier2.
    4. Fallback to first candidate id (unknown tier).
    """
    candidate_ids = _extract_candidate_team_ids(team_ids)
    if not candidate_ids:
        return 0

    known_ids_by_name = _find_known_team_ids_by_name(team_name)
    if known_ids_by_name:
        for candidate_id in candidate_ids:
            if candidate_id in known_ids_by_name:
                return int(candidate_id)
        return int(min(known_ids_by_name))

    for candidate_id in candidate_ids:
        if _get_team_tier(candidate_id) in (1, 2):
            return int(candidate_id)

    return int(candidate_ids[0])


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


def _maybe_bypass_tier1_bookmaker_presence_reject(
    *,
    match_key: str,
    status: Any,
    snapshot: Optional[dict],
    radiant_team_name: str,
    dire_team_name: str,
    radiant_team_ids: Any,
    dire_team_ids: Any,
) -> bool:
    radiant_team_id = _resolve_known_team_id_without_side_effects(
        radiant_team_ids,
        radiant_team_name,
    )
    dire_team_id = _resolve_known_team_id_without_side_effects(
        dire_team_ids,
        dire_team_name,
    )
    radiant_tier = _get_team_tier(radiant_team_id)
    dire_tier = _get_team_tier(dire_team_id)
    if radiant_tier != 1 or dire_tier != 1:
        return False

    sites_summary = _bookmaker_sites_compact_summary((snapshot or {}).get("sites"))
    _log_bookmaker_presence_failure_diagnostics(
        match_key,
        snapshot,
        context="tier1_bypass",
    )
    message = (
        "⚠️ Tier1 bookmaker presence anomaly: gate reject bypassed.\n"
        f"Radiant: {radiant_team_name} (id={radiant_team_id}, tier={radiant_tier})\n"
        f"Dire: {dire_team_name} (id={dire_team_id}, tier={dire_tier})\n"
        f"Status: {status}\n"
        f"Bookmakers: {sites_summary}\n"
        f"URL: {match_key}"
    )
    _notify_runtime_error_once(
        message,
        dedupe_key=f"tier1_presence_reject:{match_key}",
    )
    logger.error(
        "BOOKMAKER_PRESENCE_TIER1_BYPASS url=%s status=%s radiant=%s(%s,tier=%s) dire=%s(%s,tier=%s) sites=%s",
        match_key,
        status,
        radiant_team_name,
        radiant_team_id,
        radiant_tier,
        dire_team_name,
        dire_team_id,
        dire_tier,
        sites_summary,
    )
    print(
        "   ⚠️ Tier1 presence reject bypassed: "
        f"{radiant_team_name} ({radiant_team_id}) vs {dire_team_name} ({dire_team_id})"
    )
    return True


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
            response = _perform_http_get(
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
                    response = _perform_http_get(
                        url,
                        headers=headers,
                        verify=False,
                        timeout=10,
                        proxies=PROXIES,
                    )
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
                    
        except _http_request_exceptions() as e:
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

    response = make_request_with_retry(f"https://dltv.org{path}", max_retries=3, retry_delay=2)
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
    raw_diff = radiant_base_rating - dire_base_rating
    raw_radiant_wr = _elo_probability_from_ratings(radiant_base_rating, dire_base_rating) * 100.0
    raw_dire_wr = 100.0 - raw_radiant_wr
    adjusted_radiant_wr = raw_radiant_wr
    adjusted_dire_wr = raw_dire_wr
    adjusted_diff = raw_diff
    tier_gap_bonus = float(summary.get("tier_gap_bonus", 0.0) or 0.0)
    tier_gap_key = str(summary.get("tier_gap_key") or "").strip()
    lineup_used = bool(radiant_payload.get("lineup_used")) or bool(dire_payload.get("lineup_used"))

    lines = [
        "Командный ELO (текущий состав):" if lineup_used else "Командный ELO:",
        f"{radiant_team_name}: {radiant_base_rating:.0f}",
        f"{dire_team_name}: {dire_base_rating:.0f}",
    ]
    # Keep output focused: omit live vs snapshot delta in user-facing message.
    lines.append(f"ELO WR≈{raw_radiant_wr:.1f}% / {raw_dire_wr:.1f}% (ΔELO {raw_diff:+.0f})")

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
        if key in verbose_match_log_cache:
            verbose_match_log_cache.move_to_end(key)
            return
        verbose_match_log_cache[key] = None
        while len(verbose_match_log_cache) > max(1, int(VERBOSE_MATCH_LOG_CACHE_MAX_SIZE)):
            verbose_match_log_cache.popitem(last=False)


def _get_current_rss_mb() -> float:
    proc_status_path = Path("/proc/self/status")
    if proc_status_path.exists():
        try:
            for line in proc_status_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return float(parts[1]) / 1024.0
        except Exception:
            pass
    try:
        return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0
    except Exception:
        return 0.0


def _build_runtime_memory_snapshot() -> Dict[str, Any]:
    with monitored_matches_lock:
        monitored_count = len(monitored_matches)
    with processed_urls_lock:
        processed_count = len(processed_urls_cache)
    with verbose_match_log_lock:
        verbose_count = len(verbose_match_log_cache)
    with uncertain_delivery_urls_lock:
        uncertain_count = len(uncertain_delivery_urls_cache)
    with signal_send_guard_lock:
        send_guard_count = len(signal_send_guard)
    gc_gen0, gc_gen1, gc_gen2 = gc.get_count()
    return {
        "rss_mb": round(_get_current_rss_mb(), 1),
        "monitored_matches": monitored_count,
        "processed_urls_cache": processed_count,
        "verbose_match_log_cache": verbose_count,
        "uncertain_delivery_urls_cache": uncertain_count,
        "signal_send_guard": send_guard_count,
        "gc_count": f"{gc_gen0}/{gc_gen1}/{gc_gen2}",
    }


def _runtime_object_summary(value: Any) -> str:
    if value is None:
        return "none"
    try:
        if isinstance(value, _ShardedStatsLookup):
            cached_shards = len(value._shards)
            cached_rows = sum(len(shard) for shard in value._shards.values())
            cached_keys = len(value._key_cache)
            return f"sharded(cached_shards={cached_shards},cached_rows={cached_rows},cached_keys={cached_keys})"
        if isinstance(value, _SqliteStatsLookup):
            cached_keys = len(value._key_cache)
            return f"sqlite(cached_keys={cached_keys})"
        if isinstance(value, dict):
            return f"dict(len={len(value)})"
        if isinstance(value, (set, list, tuple, deque)):
            return f"{type(value).__name__}(len={len(value)})"
        if isinstance(value, OrderedDict):
            return f"OrderedDict(len={len(value)})"
    except Exception:
        pass
    return type(value).__name__


def _build_runtime_object_snapshot() -> Dict[str, str]:
    with bookmaker_prefetch_lock:
        bookmaker_queue_len = len(bookmaker_prefetch_queue)
        bookmaker_results_len = len(bookmaker_prefetch_results)
    history_count = len(match_history)
    extras = {
        "lane_data": _runtime_object_summary(lane_data),
        "early_dict": _runtime_object_summary(early_dict),
        "late_dict": _runtime_object_summary(late_dict),
        "post_lane_dict": _runtime_object_summary(post_lane_dict),
        "late_pub_comeback_table_thresholds": _runtime_object_summary(late_pub_comeback_table_thresholds_by_wr),
        "match_history": f"dict(len={history_count})",
        "bookmaker_prefetch_queue": f"deque(len={bookmaker_queue_len})",
        "bookmaker_prefetch_results": f"dict(len={bookmaker_results_len})",
        "kills_models_loaded": "yes" if KILLS_MODELS is not None else "no",
        "kills_models_by_patch": f"dict(len={len(KILLS_MODELS_BY_PATCH)})",
        "kills_models_by_tier": f"dict(len={len(KILLS_MODELS_BY_TIER)})",
        "team_predictability_cache": _runtime_object_summary(TEAM_PREDICTABILITY_CACHE),
        "tempo_solo_dict": _runtime_object_summary(tempo_solo_dict),
        "tempo_duo_dict": _runtime_object_summary(tempo_duo_dict),
        "tempo_cp1v1_dict": _runtime_object_summary(tempo_cp1v1_dict),
    }
    return extras


def _maybe_log_runtime_memory_snapshot(*, cycle_number: int, context: str, force: bool = False) -> None:
    snapshot = _build_runtime_memory_snapshot()
    object_snapshot = _build_runtime_object_snapshot()
    rss_mb = float(snapshot.get("rss_mb") or 0.0)
    should_log = force or (cycle_number % max(1, int(RUNTIME_MEMORY_SNAPSHOT_EVERY_CYCLES)) == 0)
    if not should_log and rss_mb < float(RUNTIME_MEMORY_SNAPSHOT_RSS_ALERT_MB):
        return
    print(
        "🧠 Memory snapshot: "
        f"cycle={cycle_number}, context={context}, rss={rss_mb:.1f}MB, "
        f"monitored={snapshot['monitored_matches']}, "
        f"processed={snapshot['processed_urls_cache']}, "
        f"verbose={snapshot['verbose_match_log_cache']}, "
        f"uncertain={snapshot['uncertain_delivery_urls_cache']}, "
        f"send_guard={snapshot['signal_send_guard']}, "
        f"gc={snapshot['gc_count']}, "
        f"lane={object_snapshot['lane_data']}, "
        f"early={object_snapshot['early_dict']}, "
        f"late={object_snapshot['late_dict']}, "
        f"post_lane={object_snapshot['post_lane_dict']}, "
        f"late_pub_table={object_snapshot['late_pub_comeback_table_thresholds']}, "
        f"history={object_snapshot['match_history']}, "
        f"prefetch_q={object_snapshot['bookmaker_prefetch_queue']}, "
        f"prefetch_r={object_snapshot['bookmaker_prefetch_results']}, "
        f"kills={object_snapshot['kills_models_loaded']}, "
        f"kills_patch={object_snapshot['kills_models_by_patch']}, "
        f"kills_tier={object_snapshot['kills_models_by_tier']}, "
        f"predictability={object_snapshot['team_predictability_cache']}, "
        f"tempo_solo={object_snapshot['tempo_solo_dict']}, "
        f"tempo_duo={object_snapshot['tempo_duo_dict']}, "
        f"tempo_cp1v1={object_snapshot['tempo_cp1v1_dict']}"
    )


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


def _current_map_id_check_path() -> Path:
    return Path(MAP_ID_CHECK_PATH).expanduser()


def _iter_legacy_map_id_check_paths(current_path: Path) -> list[Path]:
    normalized_current = current_path.expanduser()
    if normalized_current == DEFAULT_MAP_ID_CHECK_PATH:
        return [LEGACY_MAP_ID_CHECK_PATH]
    if normalized_current == DEFAULT_MAP_ID_CHECK_PATH_ODDS:
        return [LEGACY_MAP_ID_CHECK_PATH_ODDS]
    return []


def _load_map_id_check_urls(*, recover: bool) -> list[str]:
    with map_id_check_lock:
        map_id_check_path = _current_map_id_check_path()
        if map_id_check_path.exists():
            return _load_json_url_array(
                map_id_check_path,
                recover=recover,
                label="MAP_ID_CHECK_PATH",
            )
        for legacy_path in _iter_legacy_map_id_check_paths(map_id_check_path):
            if not legacy_path.exists():
                continue
            data = _load_json_url_array(
                legacy_path,
                recover=recover,
                label="LEGACY_MAP_ID_CHECK_PATH",
            )
            _write_map_id_check_atomic(map_id_check_path, data)
            logger.info(
                "Migrated map_id_check state from legacy repo file %s to %s",
                legacy_path,
                map_id_check_path,
            )
            print(
                "✅ MAP_ID_CHECK_PATH migrated from legacy repo file: "
                f"{legacy_path} -> {map_id_check_path} ({len(data)})"
            )
            return data
        return _load_json_url_array(
            map_id_check_path,
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

    map_id_check_path = _current_map_id_check_path()
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
    _bookmaker_release_match_tabs(normalized_url)
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
        map_id_check_path = _current_map_id_check_path()
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
        _bookmaker_release_match_tabs(url)
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
    skip_bookmaker_prepare: bool = False,
) -> bool:
    if not skip_bookmaker_prepare:
        message_text, bookmaker_ready, bookmaker_reason = _bookmaker_prepare_message_for_delivery(
            match_key,
            message_text,
        )
        if not bookmaker_ready:
            print(
                "   ⏳ Отправка отложена: bookmaker odds ещё не готовы "
                f"(reason={bookmaker_reason}) для {match_key}"
            )
            return False
    try:
        send_message(
            message_text,
            require_delivery=True,
            admin_only=SIGNAL_SEND_ADMIN_ONLY,
            mirror_to_vk=not SIGNAL_SEND_ADMIN_ONLY,
        )
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


_dltv_selenium_driver = None


def _camoufox_proxy_kwargs_from_url(proxy_url: str) -> Dict[str, Any]:
    proxy_value = str(proxy_url or "").strip()
    if not proxy_value:
        return {}
    try:
        from bookmaker_selenium_odds import _parse_proxy

        parsed = _parse_proxy(proxy_value)
        return {
            "proxy": {
                "server": f"http://{parsed['host']}:{parsed['port']}",
                "username": parsed["username"],
                "password": parsed["password"],
            }
        }
    except Exception as exc:
        print(f"⚠️ Camoufox proxy config failed for {proxy_value[:50]}...: {exc}")
        return {}


def _cyberscore_camoufox_proxy_kwargs() -> Dict[str, Any]:
    proxy_candidates = [
        CYBERSCORE_CAMOUFOX_PROXY_URL,
        CURRENT_PROXY,
    ]
    if isinstance(DLTV_PROXY_POOL, (list, tuple)):
        proxy_candidates.extend(DLTV_PROXY_POOL)
    proxy_candidates.extend(PROXY_LIST)
    proxy_value = next((str(candidate).strip() for candidate in proxy_candidates if str(candidate or "").strip()), "")
    if not proxy_value:
        message = "CyberScore source: proxy pool empty"
        if CYBERSCORE_CAMOUFOX_REQUIRE_PROXY:
            raise RuntimeError(f"{message}; direct CyberScore requests are disabled")
        print(f"⚠️ {message}, Camoufox will run direct")
        return {}
    proxy_kwargs = _camoufox_proxy_kwargs_from_url(proxy_value)
    if not proxy_kwargs and CYBERSCORE_CAMOUFOX_REQUIRE_PROXY:
        raise RuntimeError("CyberScore source: proxy config failed; direct CyberScore requests are disabled")
    return proxy_kwargs


def _camoufox_env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except (TypeError, ValueError):
        return int(default)


class _SharedCamoufoxSession:
    """Owns the only Camoufox browser and runs all page work on one thread."""

    _STOP = object()

    def __init__(self) -> None:
        self._jobs: "queue.Queue[Any]" = queue.Queue()
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._reset_requested = False

    def submit(self, label: str, callback, timeout: float = 120.0) -> Any:
        if not CAMOUFOX_AVAILABLE or camoufox is None:
            raise RuntimeError("Camoufox unavailable")
        future: Future = Future()
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._worker,
                    name="shared-camoufox",
                    daemon=True,
                )
                self._thread.start()
        self._jobs.put((future, str(label or "camoufox-job"), callback))
        return future.result(timeout=timeout)

    def request_reset(self) -> None:
        with self._lock:
            self._reset_requested = True

    def close(self) -> None:
        with self._lock:
            thread = self._thread
            if thread is None:
                return
            self._jobs.put(self._STOP)
        if thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=10)
        with self._lock:
            self._thread = None

    def _pop_reset_requested(self) -> bool:
        with self._lock:
            value = self._reset_requested
            self._reset_requested = False
            return value

    def _worker(self) -> None:
        browser_cm = None
        browser = None
        launched_at = 0.0
        jobs_since_launch = 0
        reset_after_jobs = max(1, _camoufox_env_int("CAMOUFOX_RESET_AFTER_JOBS", 250))
        reset_after_seconds = max(60, _camoufox_env_int("CAMOUFOX_RESET_AFTER_SECONDS", 3600))

        def _close_browser(reason: str) -> None:
            nonlocal browser_cm, browser, launched_at, jobs_since_launch
            if browser is not None:
                with contextlib.suppress(Exception):
                    browser.close()
            if browser_cm is not None:
                with contextlib.suppress(Exception):
                    browser_cm.__exit__(None, None, None)
            if browser is not None:
                print(f"   🔒 Shared Camoufox browser closed ({reason})")
            browser_cm = None
            browser = None
            launched_at = 0.0
            jobs_since_launch = 0

        def _ensure_browser() -> Any:
            nonlocal browser_cm, browser, launched_at
            if browser is not None:
                return browser
            proxy_kwargs = _cyberscore_camoufox_proxy_kwargs()
            proxy_label = "with proxy" if proxy_kwargs else "without proxy"
            browser_cm = camoufox.Camoufox(headless=True, **proxy_kwargs)
            browser = browser_cm.__enter__()
            launched_at = time.time()
            print(f"   🌐 Shared Camoufox browser created ({proxy_label})")
            return browser

        try:
            while True:
                job = self._jobs.get()
                if job is self._STOP:
                    break
                future, label, callback = job
                if not future.set_running_or_notify_cancel():
                    continue
                try:
                    active_browser = _ensure_browser()
                    result = callback(active_browser)
                    jobs_since_launch += 1
                    future.set_result(result)
                except Exception as exc:
                    future.set_exception(exc)
                    self.request_reset()
                finally:
                    browser_age = time.time() - launched_at if launched_at else 0.0
                    should_reset = (
                        self._pop_reset_requested()
                        or jobs_since_launch >= reset_after_jobs
                        or (browser_age >= reset_after_seconds and self._jobs.empty())
                    )
                    if should_reset:
                        _close_browser("periodic reset" if browser_age >= reset_after_seconds else "requested reset")
        finally:
            _close_browser("shutdown")


_shared_camoufox_session = _SharedCamoufoxSession()
atexit.register(_shared_camoufox_session.close)


def _run_shared_camoufox_job(label: str, callback, timeout: float = 120.0, retry: bool = True) -> Any:
    try:
        return _shared_camoufox_session.submit(label, callback, timeout=timeout)
    except Exception:
        if not retry:
            raise
        _shared_camoufox_session.request_reset()
        return _shared_camoufox_session.submit(label, callback, timeout=timeout)


def _fetch_protracker_payload_via_shared_camoufox(
    slug: str,
    hero_id: int,
    proxy_candidate: Optional[str] = None,
) -> Dict[str, Any]:
    base_url = "https://dota2protracker.com"

    def _job(browser) -> Dict[str, Any]:
        page = browser.new_page()
        try:
            page.goto(f"{base_url}/hero/{slug}", wait_until="networkidle", timeout=30000)
            payload = {"matchups": {}, "synergies": {}}
            for pos in ["1", "2", "3", "4", "5"]:
                api_url = f"{base_url}/hero/{slug}/api/matchup-payload?heroId={int(hero_id)}&position=pos+{pos}"
                response = page.evaluate(
                    """async (apiUrl) => {
                        const r = await fetch(apiUrl);
                        return await r.json();
                    }""",
                    api_url,
                )
                payload["matchups"][pos] = response.get("matchups", []) if isinstance(response, dict) else []
                payload["synergies"][pos] = response.get("synergies", []) if isinstance(response, dict) else []
            return payload
        finally:
            with contextlib.suppress(Exception):
                page.close()

    return _run_shared_camoufox_job(f"dota2protracker:{slug}", _job, timeout=180)


def _install_dota2protracker_shared_camoufox_fetcher() -> bool:
    module = globals().get("_dota2protracker_module")
    if module is None and enrich_with_pro_tracker is not None:
        module = sys.modules.get(getattr(enrich_with_pro_tracker, "__module__", ""))
    setter = getattr(module, "set_payload_fetcher", None) if module is not None else None
    if callable(setter):
        setter(_fetch_protracker_payload_via_shared_camoufox)
    return getattr(module, "PROTRACKER_PAYLOAD_FETCHER", None) is _fetch_protracker_payload_via_shared_camoufox


def _decode_next_flight_chunks_from_html(html: str) -> List[str]:
    def _decode_payload(raw_payload: str) -> Optional[str]:
        try:
            return json.loads(f'"{raw_payload}"')
        except Exception as exc:
            logger.debug("Failed to decode Next flight chunk: %s", exc)
            return None

    def _scan_payloads(raw_text: str) -> List[str]:
        found: List[str] = []
        needle = 'self.__next_f.push([1,"'
        start_search = 0
        while True:
            idx = raw_text.find(needle, start_search)
            if idx < 0:
                break
            start = idx + len(needle)
            pos = start
            escaped = False
            while pos < len(raw_text):
                ch = raw_text[pos]
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"' and raw_text[pos + 1 : pos + 3] == "])":
                    decoded = _decode_payload(raw_text[start:pos])
                    if decoded is not None:
                        found.append(decoded)
                    pos += 3
                    break
                pos += 1
            start_search = max(pos, start + 1)
        return found

    soup = BeautifulSoup(html or "", "lxml")
    chunks: List[str] = []
    for script in soup.find_all("script"):
        script_text = script.string or script.get_text() or ""
        if "self.__next_f.push" not in script_text:
            continue
        chunks.extend(_scan_payloads(script_text))
    if not chunks and "self.__next_f.push" in str(html or ""):
        chunks.extend(_scan_payloads(str(html or "")))
    return chunks


def _extract_balanced_json_object(text: str, start_index: int) -> Optional[str]:
    if start_index < 0 or start_index >= len(text) or text[start_index] != "{":
        return None
    depth = 0
    in_string = False
    escaped = False
    for idx in range(start_index, len(text)):
        ch = text[idx]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start_index : idx + 1]
    return None


def _extract_cyberscore_match_item_from_html(html: str, match_id: Optional[Union[int, str]] = None) -> Optional[Dict[str, Any]]:
    chunks = _decode_next_flight_chunks_from_html(html)
    if not chunks:
        return None
    blob = "\n".join(chunks)
    candidates: List[str] = []
    if match_id:
        candidates.append(f'"item":{{"id":{match_id}')
        candidates.append(f'"item":{{"id":"{match_id}"')
    candidates.append('"item":{"id":')
    for needle in candidates:
        idx = blob.find(needle)
        if idx < 0:
            continue
        start = blob.find("{", idx + len('"item":') - 1)
        raw_object = _extract_balanced_json_object(blob, start)
        if not raw_object:
            continue
        try:
            item = json.loads(raw_object)
        except Exception as exc:
            logger.debug("Failed to parse CyberScore match item JSON: %s", exc)
            continue
        if not isinstance(item, dict):
            continue
        if match_id:
            try:
                if int(item.get("id") or 0) != int(match_id):
                    continue
            except Exception:
                pass
        return item
    return None


def _extract_cyberscore_match_id_from_href(href: str) -> str:
    match = re.search(r"/matches/(\d+)", str(href or ""))
    return match.group(1) if match else ""


def _absolute_cyberscore_url(href: str) -> str:
    raw = str(href or "").strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if raw.startswith("/"):
        return f"https://cyberscore.live{raw}"
    return f"https://cyberscore.live/{raw.lstrip('/')}"


def _extract_cyberscore_live_cards_from_html(html: str) -> Tuple[List[Any], List[Any]]:
    global CYBERSCORE_LISTING_ITEM_CACHE
    soup = BeautifulSoup(html or "", "lxml")
    cards: List[Any] = []
    listing_item_cache: Dict[str, Dict[str, Any]] = {}
    for card in soup.select("a.matches-item[href*='/matches/']"):
        classes = set(card.get("class") or [])
        text = card.get_text(" ", strip=True)
        if "online" not in classes and "LIVE" not in text.upper():
            continue
        href = _absolute_cyberscore_url(str(card.get("href") or ""))
        match_id = _extract_cyberscore_match_id_from_href(href)
        card["data-source"] = "cyberscore"
        card["data-cyberscore-href"] = href
        if match_id:
            card["data-cyberscore-match-id"] = match_id
            listing_item = _extract_cyberscore_match_item_from_html(html, match_id=match_id)
            if isinstance(listing_item, dict):
                listing_item_cache[str(match_id)] = listing_item
        cards.append(card)
    CYBERSCORE_LISTING_ITEM_CACHE = listing_item_cache
    return list(cards), list(cards)


def _get_cyberscore_html_via_camoufox(url: Optional[str] = None) -> Optional[str]:
    if not CAMOUFOX_AVAILABLE:
        print("⚠️ CyberScore source: Camoufox unavailable")
        return None
    target_url = str(url or CYBERSCORE_MATCHES_URL).strip()
    try:
        def _job(browser) -> str:
            page = browser.new_page()
            try:
                print(f"🌐 CyberScore source: loading {target_url} via shared Camoufox tab...")
                page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
                try:
                    page.wait_for_selector("a.matches-item[href*='/matches/'], main", timeout=20000)
                except Exception:
                    pass
                time.sleep(2.0)
                return page.content() or ""
            finally:
                with contextlib.suppress(Exception):
                    page.close()

        return _run_shared_camoufox_job(f"cyberscore:{target_url}", _job, timeout=90)
    except Exception as exc:
        print(f"❌ CyberScore Camoufox fetch failed: {exc}")
        logger.warning("CyberScore Camoufox fetch failed for %s: %s", target_url, exc)
        return None


def _get_cyberscore_heads_via_camoufox() -> Tuple[Optional[List[Any]], Optional[List[Any]]]:
    global GET_HEADS_LAST_FAILURE_REASON, NEXT_SCHEDULE_SLEEP_SECONDS, NEXT_SCHEDULE_MATCH_INFO, SCHEDULE_LIVE_WAIT_TARGET
    html = _get_cyberscore_html_via_camoufox(CYBERSCORE_MATCHES_URL)
    if html is None:
        return None, None
    heads, bodies = _extract_cyberscore_live_cards_from_html(html)
    if heads:
        print(f"✅ CyberScore source: found {len(heads)} live cards")
        GET_HEADS_LAST_FAILURE_REASON = None
        NEXT_SCHEDULE_SLEEP_SECONDS = 0.0
        NEXT_SCHEDULE_MATCH_INFO = None
        SCHEDULE_LIVE_WAIT_TARGET = None
        _emit_pending_schedule_wake_audit(
            heads_count=len(heads),
            bodies_count=len(bodies),
            request_status="cyberscore_live_found",
        )
        return heads, bodies

    schedule_info = _extract_nearest_cyberscore_scheduled_match_info(html)
    if schedule_info:
        NEXT_SCHEDULE_MATCH_INFO = schedule_info
        NEXT_SCHEDULE_SLEEP_SECONDS = float(schedule_info.get("sleep_seconds", 0.0) or 0.0)
        GET_HEADS_LAST_FAILURE_REASON = None
        print(
            "🗓️ CyberScore source: no live cards. "
            f"Nearest tier1/2 scheduled match: {_format_schedule_match_label(schedule_info)}. "
            f"Next recheck in {int(math.ceil(NEXT_SCHEDULE_SLEEP_SECONDS))}s"
        )
        _emit_pending_schedule_wake_audit(
            heads_count=0,
            bodies_count=0,
            next_schedule_info=schedule_info,
            request_status="cyberscore_schedule_only",
        )
        return [], []

    NEXT_SCHEDULE_SLEEP_SECONDS = float(
        _cap_cyberscore_schedule_sleep_seconds(CYBERSCORE_SCHEDULE_POLL_SECONDS)
    )
    NEXT_SCHEDULE_MATCH_INFO = {
        "sleep_seconds": NEXT_SCHEDULE_SLEEP_SECONDS,
        "sleep_seconds_raw": NEXT_SCHEDULE_SLEEP_SECONDS,
        "matchup": "no tier1/2 upcoming match",
        "league_title": "",
        "source": "cyberscore_no_upcoming",
    }
    GET_HEADS_LAST_FAILURE_REASON = None
    print(
        "⚠️ CyberScore source: no live cards and no tier1/2 upcoming match found. "
        f"Next schedule poll in {int(math.ceil(NEXT_SCHEDULE_SLEEP_SECONDS))}s"
    )
    _emit_pending_schedule_wake_audit(
        heads_count=0,
        bodies_count=0,
        next_schedule_info=None,
        request_status="cyberscore_no_live_no_upcoming",
    )
    return [], []


def _maybe_get_cyberscore_heads_fallback(reason: str) -> Optional[Tuple[Optional[List[Any]], Optional[List[Any]]]]:
    global GET_HEADS_LAST_FAILURE_REASON, NEXT_SCHEDULE_SLEEP_SECONDS, NEXT_SCHEDULE_MATCH_INFO
    if not CYBERSCORE_GET_HEADS_FALLBACK:
        return None
    print(f"🧭 DLTV get_heads fallback -> CyberScore (reason={reason})")
    result = _get_cyberscore_heads_via_camoufox()
    if result and result[0] is not None:
        GET_HEADS_LAST_FAILURE_REASON = None
        NEXT_SCHEDULE_SLEEP_SECONDS = 0.0
        NEXT_SCHEDULE_MATCH_INFO = None
    return result


def _parse_cyberscore_best_of_score(item: Dict[str, Any]) -> Tuple[int, int]:
    raw_score = item.get("best_of_score")
    if isinstance(raw_score, (list, tuple)) and len(raw_score) >= 2:
        try:
            return int(raw_score[0] or 0), int(raw_score[1] or 0)
        except Exception:
            return 0, 0
    return 0, 0


def _latest_cyberscore_radiant_lead(item: Dict[str, Any]) -> int:
    networth = item.get("networth")
    if not isinstance(networth, list) or not networth:
        return 0
    valid_points = [point for point in networth if isinstance(point, dict)]
    if not valid_points:
        return 0
    latest = max(valid_points, key=lambda point: _coerce_int(point.get("time")))
    value = _coerce_int(latest.get("value"))
    team = str(latest.get("team") or "").strip().lower()
    return -abs(value) if team == "dire" else abs(value)


def _parse_cyberscore_draft_and_positions(item: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Optional[str]]:
    radiant: Dict[str, Dict[str, Any]] = {}
    dire: Dict[str, Dict[str, Any]] = {}
    picks = item.get("picks")
    if not isinstance(picks, list):
        return {}, {}, "CyberScore item has no picks list"
    for pick in picks:
        if not isinstance(pick, dict):
            continue
        team = str(pick.get("team") or "").strip().lower()
        player = pick.get("player") if isinstance(pick.get("player"), dict) else {}
        hero = pick.get("hero") if isinstance(pick.get("hero"), dict) else {}
        role = _coerce_int(player.get("role"))
        hero_id = _coerce_int(hero.get("id_steam") or hero.get("steam_id") or hero.get("hero_id"))
        if hero_id <= 0:
            # CyberScore internal hero ids are not Dota ids; id_steam is the useful one.
            hero_id = _coerce_int(hero.get("idSteam") or hero.get("dota_id"))
        if role < 1 or role > 5 or hero_id <= 0:
            continue
        target = radiant if team == "radiant" else dire if team == "dire" else None
        if target is None:
            continue
        pos_key = f"pos{role}"
        target[pos_key] = {
            "hero_id": int(hero_id),
            # CyberScore exposes its own player id here, not Steam account id.
            # Keep account_id=0 so skipped-player and live-ELO code do not read it as Steam id.
            "account_id": 0,
            "_cyberscore_player_id": _coerce_int(player.get("id")),
            "_player_name": str(player.get("game_name") or player.get("full_name") or "").strip(),
            "_hero_name": str(hero.get("name") or "").strip(),
        }
    for payload in list(radiant.values()) + list(dire.values()):
        payload.pop("_player_name", None)
    missing = []
    for team_name, payload in (("radiant", radiant), ("dire", dire)):
        for idx in range(1, 6):
            if f"pos{idx}" not in payload:
                missing.append(f"{team_name}.pos{idx}")
    if missing:
        return radiant, dire, f"CyberScore draft incomplete: missing {', '.join(missing)}"
    return radiant, dire, None


def _cyberscore_item_to_runtime_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    radiant_score = _coerce_int(item.get("score_team_radiant"))
    dire_score = _coerce_int(item.get("score_team_dire"))
    game_time = _coerce_int(item.get("game_time") or item.get("ticks_game_time"))
    radiant_lead = _latest_cyberscore_radiant_lead(item)
    radiant_bo_score, dire_bo_score = _parse_cyberscore_best_of_score(item)
    radiant_team = item.get("team_radiant") if isinstance(item.get("team_radiant"), dict) else {}
    dire_team = item.get("team_dire") if isinstance(item.get("team_dire"), dict) else {}
    tournament = item.get("tournament") if isinstance(item.get("tournament"), dict) else {}
    radiant_team_name = str(radiant_team.get("name") or item.get("team_radiant_name") or "").strip()
    dire_team_name = str(dire_team.get("name") or item.get("team_dire_name") or "").strip()
    radiant_team_id = _coerce_int(item.get("team_radiant_id") or radiant_team.get("id"))
    dire_team_id = _coerce_int(item.get("team_dire_id") or dire_team.get("id"))
    series_id = str(item.get("id_series") or item.get("id") or "").strip()
    league_name = str(tournament.get("title") or tournament.get("name") or "").strip()
    radiant_heroes, dire_heroes, draft_error = _parse_cyberscore_draft_and_positions(item)

    def _fast_picks(team_payload: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for idx in range(1, 6):
            payload = team_payload.get(f"pos{idx}") or {}
            hero_id = int(payload.get("hero_id") or 0)
            if hero_id <= 0:
                continue
            result.append(
                {
                    "hero_id": hero_id,
                    "player": {"title": f"cyberscore_pos{idx}"},
                }
            )
        return result

    return {
        "source": "cyberscore",
        "match_id": _coerce_int(item.get("id")),
        "game_time": game_time,
        "radiant_lead": radiant_lead,
        "radiant_score": radiant_score,
        "dire_score": dire_score,
        "fast_picks": {
            "first_team": _fast_picks(radiant_heroes),
            "second_team": _fast_picks(dire_heroes),
        },
        "_cyberscore_item": item,
        "_cyberscore_draft_error": draft_error,
        "_cyberscore_heroes_and_pos": {
            "radiant": radiant_heroes,
            "dire": dire_heroes,
        },
        "db": {
            "series": {
                "id": series_id,
                "type": item.get("best_of"),
                "slug": str(item.get("title") or "").strip().lower().replace(" ", "-"),
            },
            "scores": {
                "first_team": radiant_bo_score,
                "second_team": dire_bo_score,
            },
            "first_team": {
                "title": radiant_team_name,
                "is_radiant": True,
                "id": radiant_team_id,
                "team_id": radiant_team_id,
            },
            "second_team": {
                "title": dire_team_name,
                "is_radiant": False,
                "id": dire_team_id,
                "team_id": dire_team_id,
            },
            "league": {
                "title": league_name,
            },
        },
        "live_league_data": {
            "league_id": _coerce_int(item.get("tournament_id") or tournament.get("id")),
            "league_name": league_name,
            "series_id": series_id,
            "series_type": item.get("best_of"),
            "game_time": game_time,
            "radiant_lead": radiant_lead,
            "radiant_score": radiant_score,
            "dire_score": dire_score,
            "radiant_series_wins": radiant_bo_score,
            "dire_series_wins": dire_bo_score,
            "match": {
                "radiant_team_id": radiant_team_id,
                "dire_team_id": dire_team_id,
                "game_time": game_time,
                "radiant_lead": radiant_lead,
            },
            "radiant_team_id": radiant_team_id,
            "dire_team_id": dire_team_id,
            "radiant_team": {
                "team_id": radiant_team_id,
                "id": radiant_team_id,
                "title": radiant_team_name,
            },
            "dire_team": {
                "team_id": dire_team_id,
                "id": dire_team_id,
                "title": dire_team_name,
            },
        },
    }


def _get_dltv_html_via_camoufox():
        """Fetch DLTV live matches page via Camoufox/Playwright."""
        from bs4 import BeautifulSoup

        if not CAMOUFOX_AVAILABLE:
            print("⚠️ DLTV HTML mode: Camoufox unavailable, fallback to Selenium")
            return None, None

        try:
            def _job(browser):
                page = browser.new_page()
                try:
                    print("🌐 DLTV HTML mode: loading dltv.org/matches via shared Camoufox tab...")
                    page.goto("https://dltv.org/matches", wait_until="domcontentloaded", timeout=30000)
                    try:
                        page.wait_for_selector("div.live__matches, div.match.live", timeout=15000)
                    except Exception:
                        pass
                    time.sleep(2.0)
                    html = page.content() or ""
                    soup = BeautifulSoup(html, 'lxml')
                    live_matches = soup.find('div', class_='live__matches')
                    live_cards = list(soup.select("div.match.live"))

                    if live_matches:
                        cards = list(live_matches.select("div.match.live")) if live_matches else []
                        if cards:
                            print(f"✅ DLTV HTML Camoufox: found {len(cards)} live cards from section")
                            return [None] * len(cards), cards
                        print("✅ DLTV HTML Camoufox: found live_matches section (no cards)")
                        return [None], [soup]
                    if live_cards:
                        print(f"✅ DLTV HTML Camoufox: found {len(live_cards)} live cards")
                        return [None] * len(live_cards), live_cards
                    print("⚠️ DLTV HTML Camoufox: no live matches found")
                    return [], []
                finally:
                    with contextlib.suppress(Exception):
                        page.close()

            return _run_shared_camoufox_job("dltv-html:matches", _job, timeout=60)
        except Exception as exc:
            print(f"❌ DLTV HTML Camoufox fetch failed: {exc}")
            return None, None


def _get_dltv_html_via_selenium():
        """Fetch DLTV live matches page via Selenium/Chrome (reuses driver between cycles)."""
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        from seleniumwire import webdriver
        from selenium.webdriver.chrome.options import Options
        from bs4 import BeautifulSoup
        import tempfile

        global _dltv_selenium_driver
        from bookmaker_selenium_odds import _parse_proxy

        if _dltv_selenium_driver is None:
            try:
                proxy_for_dltv = None
                if DLTV_PROXY_POOL:
                    proxy_for_dltv = DLTV_PROXY_POOL[0]

                chrome_options = Options()
                chrome_options.page_load_strategy = "eager"
                chrome_options.add_argument("--headless=new")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--window-size=1920,1080")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument("--disable-popup-blocking")
                chrome_options.add_argument("--disable-background-timer-throttling")
                chrome_options.add_argument("--disable-backgrounding-occluded-windows")
                chrome_options.add_argument("--disable-renderer-backgrounding")
                chrome_options.add_argument("--disable-extensions")
                chrome_options.add_argument("--blink-settings=imagesEnabled=false")
                # Use unique temp profile per session
                dltv_profile = tempfile.mkdtemp(prefix="dltv_selenium_")
                chrome_options.add_argument(f"--user-data-dir={dltv_profile}")

                sw_options = {}
                if proxy_for_dltv:
                    parsed = _parse_proxy(proxy_for_dltv)
                    sw_options = {
                        "proxy": {
                            "http": f"http://{parsed['username']}:{parsed['password']}@{parsed['host']}:{parsed['port']}",
                            "https": f"https://{parsed['username']}:{parsed['password']}@{parsed['host']}:{parsed['port']}",
                            "no_proxy": "localhost,127.0.0.1",
                        },
                        "verify_ssl": False,
                        "suppress_connection_errors": True,
                        "request_storage": "memory",
                        "request_storage_max_size": 150,
                    }
                    print(f"🌐 DLTV HTML mode: init Chrome with proxy {proxy_for_dltv[:50]}...")
                else:
                    print("🌐 DLTV HTML mode: init Chrome without proxy...")
                    sw_options = {
                        "verify_ssl": False,
                        "suppress_connection_errors": True,
                        "request_storage": "memory",
                        "request_storage_max_size": 150,
                    }

                drv = webdriver.Chrome(options=chrome_options, seleniumwire_options=sw_options)
                drv.set_page_load_timeout(30)
                _dltv_selenium_driver = drv
            except Exception as exc:
                print(f"❌ Failed to init Chrome for DLTV HTML: {exc}")
                return None, None

        driver = _dltv_selenium_driver

        try:
            print("🌐 DLTV HTML mode: loading dltv.org/matches...")
            driver.get("https://dltv.org/matches")
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.live__matches, div.match.live"))
            )
            html = driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            live_matches = soup.find('div', class_='live__matches')
            live_cards = list(soup.select("div.match.live"))

            if live_matches:
                cards = list(live_matches.select("div.match.live")) if live_matches else []
                if cards:
                    print(f"✅ DLTV HTML: found {len(cards)} live cards from section")
                    return [None] * len(cards), cards
                print(f"✅ DLTV HTML: found live_matches section (no cards)")
                return [None], [soup]
            elif live_cards:
                print(f"✅ DLTV HTML: found {len(live_cards)} live cards")
                return [None] * len(live_cards), live_cards
            else:
                print("⚠️ DLTV HTML: no live matches found")
                return [], []
        except Exception as exc:
            print(f"❌ DLTV HTML fetch failed: {exc}")
            # Driver might be dead, reset so next cycle recreates it
            _dltv_selenium_driver = None
            return None, None
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass


def get_heads(response=None, MAX_RETRIES=5, RETRY_DELAY=5, ip_address="46.229.214.49", path = "/matches"):
        global GET_HEADS_LAST_FAILURE_REASON, NEXT_SCHEDULE_SLEEP_SECONDS, NEXT_SCHEDULE_MATCH_INFO, SCHEDULE_LIVE_WAIT_TARGET
        global PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE

        if DLTV_SOURCE_MODE == "cyberscore":
            GET_HEADS_LAST_FAILURE_REASON = None
            NEXT_SCHEDULE_SLEEP_SECONDS = 0.0
            NEXT_SCHEDULE_MATCH_INFO = None
            return _get_cyberscore_heads_via_camoufox()

        # HTML mode: prefer Camoufox, fallback to Selenium instead of API
        if DLTV_SOURCE_MODE == "html":
            if DLTV_CAMOUFOX_ENABLED:
                heads, datas = _get_dltv_html_via_camoufox()
                if heads is not None and datas is not None:
                    return heads, datas
                print("⚠️ DLTV HTML mode: Camoufox failed, fallback to Selenium")
            return _get_dltv_html_via_selenium()

        GET_HEADS_LAST_FAILURE_REASON = None
        NEXT_SCHEDULE_SLEEP_SECONDS = 0.0
        NEXT_SCHEDULE_MATCH_INFO = None
        # Формируем URL всегда через canonical host, а не через IP:
        # HTML /matches на raw IP периодически отдает устаревший live snapshot.
        requested_host = str(ip_address or "").strip().lower()
        canonical_host = "dltv.org" if requested_host in {"", "46.229.214.49", "dltv.org", "www.dltv.org"} else str(ip_address).strip()
        url = f"https://{canonical_host}{path}"
        request_headers = globals().get(
            'headers',
            {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
        
        # Если response уже передан, используем его, иначе делаем запрос
        if response is None:
            response = make_request_with_retry(url, MAX_RETRIES, RETRY_DELAY, headers=request_headers)

        if not response or response.status_code != 200:
            status_msg = f"Status: {response.status_code}" if response else "No response (None)"
            print(f"❌ Ошибка получения данных: {status_msg}")
            GET_HEADS_LAST_FAILURE_REASON = GET_HEADS_FAILURE_REASON_REQUEST_FAILED
            cyberscore_fallback = _maybe_get_cyberscore_heads_fallback("dltv_request_failed")
            if cyberscore_fallback is not None:
                return cyberscore_fallback
            _emit_pending_schedule_wake_audit(
                heads_count=0,
                bodies_count=0,
                next_schedule_info=None,
                request_status=status_msg,
            )
            return None, None

        try:
            parse_failed_on_200 = False
            proxy_pool_size = len(PROXY_LIST) if USE_PROXY and PROXY_LIST else 0
            max_proxy_attempts = max(1, proxy_pool_size * PROXY_POOL_ROTATION_ROUNDS) if proxy_pool_size else 1
            proxy_attempt_count = 0
            current_response = response
            live_matches = None
            live_cards: List[Any] = []
            soup = None
            used_direct_fallback = False
            saw_valid_matches_page_without_live = False

            def _try_direct_live_matches_fallback(*, announce: bool = True, reason: str = "proxy_exhausted"):
                global PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE
                print(f"🌐 Пробую direct fallback ({reason})...")
                if announce and not PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE:
                    try:
                        send_message(
                            "⚠️ Все прокси для live matches исчерпаны после 3 кругов. "
                            "Переключаюсь на direct fallback.",
                            admin_only=True,
                        )
                    except Exception as exc:
                        print(f"⚠️ Не удалось отправить уведомление о direct fallback: {exc}")
                    PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE = True
                try:
                    direct_response = _perform_http_get(
                        url,
                        headers=request_headers,
                        verify=False,
                        timeout=10,
                    )
                except _http_request_exceptions() as exc:
                    print(f"⚠️ Direct fallback failed: {type(exc).__name__}: {exc}")
                    logger.warning("Direct fallback failed for %s: %s", url, exc)
                    return None, None, None
                if direct_response.status_code != 200:
                    print(f"⚠️ Direct fallback вернул статус {direct_response.status_code}")
                    return direct_response, None, None, []
                direct_soup = BeautifulSoup(direct_response.text, 'lxml')
                direct_live_matches = direct_soup.find('div', class_='live__matches')
                direct_live_cards = list(direct_soup.select("div.match.live"))
                if not direct_live_matches and not direct_live_cards:
                    try:
                        direct_live_cards = _fetch_live_series_json_cards(
                            headers=request_headers,
                            proxies=None,
                        )
                    except _http_request_exceptions() as exc:
                        direct_live_cards = []
                        print(f"⚠️ Direct live/series.json fetch failed: {type(exc).__name__}: {exc}")
                        logger.warning("Direct live/series.json fetch failed for %s: %s", url, exc)
                if direct_live_matches or direct_live_cards:
                    print(
                        "✅ Direct fallback succeeded "
                        f"({reason}); live_cards={len(direct_live_cards)}, "
                        f"non_tbd={_count_non_tbd_live_cards(direct_live_cards)}"
                    )
                    return direct_response, direct_soup, direct_live_matches, direct_live_cards
                print("❌ Direct fallback также не нашел live матчи в HTML")
                return direct_response, direct_soup, None, []

            while True:
                marker = _get_current_proxy_marker()
                proxy_attempt_count += 1
                attempted_human = (
                    f"{proxy_attempt_count}/{max_proxy_attempts}"
                    if proxy_pool_size
                    else "1/1"
                )

                if current_response and current_response.status_code == 200:
                    response_text = current_response.text or ""
                    soup = BeautifulSoup(response_text, 'lxml')
                    live_matches = soup.find('div', class_='live__matches')
                    live_cards = list(soup.select("div.match.live"))
                    if not live_matches and not live_cards:
                        try:
                            live_cards = _fetch_live_series_json_cards(
                                headers=request_headers,
                                proxies=PROXIES if USE_PROXY else None,
                            )
                        except _http_request_exceptions() as exc:
                            live_cards = []
                            print(f"⚠️ live/series.json fetch failed: {type(exc).__name__}: {exc}")
                            logger.warning("live/series.json fetch failed for %s: %s", url, exc)
                        if live_cards:
                            print(
                                "🛰️ Live series JSON returned active matches. "
                                "Using synthesized live cards."
                            )
                    if live_matches:
                        if not used_direct_fallback:
                            PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE = False
                        break
                    if live_cards:
                        if USE_PROXY and _count_non_tbd_live_cards(live_cards) == 0:
                            print(
                                "🩺 Proxy live snapshot looks stale/tbd-only. "
                                f"Proxy cards={_summarize_live_card_hrefs(live_cards)}"
                            )
                            direct_response, direct_soup, direct_live_matches, direct_live_cards = _try_direct_live_matches_fallback(
                                announce=False,
                                reason="proxy_tbd_only_live_snapshot",
                            )
                            direct_non_tbd = _count_non_tbd_live_cards(direct_live_cards)
                            current_non_tbd = _count_non_tbd_live_cards(live_cards)
                            if direct_response is not None and direct_response.status_code == 200 and (
                                direct_non_tbd > current_non_tbd or len(direct_live_cards) > len(live_cards)
                            ):
                                print(
                                    "🔁 Replacing stale proxy live snapshot with direct live snapshot: "
                                    f"proxy_non_tbd={current_non_tbd}, direct_non_tbd={direct_non_tbd}, "
                                    f"proxy_cards={_summarize_live_card_hrefs(live_cards)}, "
                                    f"direct_cards={_summarize_live_card_hrefs(direct_live_cards)}"
                                )
                                current_response = direct_response
                                soup = direct_soup
                                live_matches = direct_live_matches
                                live_cards = list(direct_live_cards)
                                used_direct_fallback = True
                        if not used_direct_fallback:
                            PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE = False
                        break
                    if _is_valid_dltv_matches_page(soup, response_text):
                        saw_valid_matches_page_without_live = True
                        schedule_info = _extract_nearest_scheduled_match_info(soup)
                        if schedule_info:
                            NEXT_SCHEDULE_MATCH_INFO = schedule_info
                            NEXT_SCHEDULE_SLEEP_SECONDS = float(
                                schedule_info.get("sleep_seconds", 0.0) or 0.0
                            )
                            GET_HEADS_LAST_FAILURE_REASON = None
                            print(
                                "🗓️ Получен валидный HTML DLTV без live__matches. "
                                "Переключаюсь в режим ожидания по расписанию."
                            )
                            _emit_pending_schedule_wake_audit(
                                heads_count=0,
                                bodies_count=0,
                                next_schedule_info=schedule_info,
                                request_status="schedule_only_no_live_matches",
                            )
                            return [], []
                        print(
                            "⚠️ Получен валидный HTML DLTV без live__matches и без будущих матчей "
                            f"(proxy={marker}, tried={attempted_human})"
                        )
                        _emit_pending_schedule_wake_audit(
                            heads_count=0,
                            bodies_count=0,
                            next_schedule_info=None,
                            request_status="valid_page_no_live_matches",
                        )
                        return [], []
                    parse_failed_on_200 = True
                    print(
                        "⚠️ Прокси не вернул валидную непустую HTML-страницу DLTV "
                        f"(proxy={marker}, tried={attempted_human}, len={len(response_text.strip())})"
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

                if proxy_attempt_count >= max_proxy_attempts:
                    schedule_source_soup = soup
                    if USE_PROXY:
                        direct_response, direct_soup, direct_live_matches, direct_live_cards = _try_direct_live_matches_fallback()
                        if direct_response is not None and direct_response.status_code == 200:
                            parse_failed_on_200 = True
                            if direct_soup is not None:
                                schedule_source_soup = direct_soup
                        if direct_live_matches or direct_live_cards:
                            current_response = direct_response
                            soup = direct_soup
                            live_matches = direct_live_matches
                            live_cards = list(direct_live_cards)
                            used_direct_fallback = True
                            break
                    if parse_failed_on_200:
                        schedule_info = (
                            _extract_nearest_scheduled_match_info(schedule_source_soup)
                            if schedule_source_soup is not None
                            else None
                        )
                        if schedule_info:
                            NEXT_SCHEDULE_MATCH_INFO = schedule_info
                            NEXT_SCHEDULE_SLEEP_SECONDS = float(
                                schedule_info.get("sleep_seconds", 0.0) or 0.0
                            )
                            GET_HEADS_LAST_FAILURE_REASON = None
                            print(
                                "🗓️ live__matches block missing, but schedule was parsed successfully. "
                                "Switching to schedule-only mode."
                            )
                            _emit_pending_schedule_wake_audit(
                                heads_count=0,
                                bodies_count=0,
                                next_schedule_info=schedule_info,
                                request_status="schedule_only_no_live_matches",
                            )
                            return [], []
                        if saw_valid_matches_page_without_live:
                            GET_HEADS_LAST_FAILURE_REASON = None
                            _emit_pending_schedule_wake_audit(
                                heads_count=0,
                                bodies_count=0,
                                next_schedule_info=None,
                                request_status="valid_page_no_live_matches",
                            )
                            return [], []
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
                    cyberscore_fallback = _maybe_get_cyberscore_heads_fallback(
                        str(GET_HEADS_LAST_FAILURE_REASON or "dltv_live_missing")
                    )
                    if cyberscore_fallback is not None:
                        return cyberscore_fallback
                    return None, None

                print(f"🔄 Переключился на другой прокси, повторяю запрос...")
                if proxy_pool_size:
                    rotate_proxy()
                time.sleep(2)
                current_response = make_request_with_retry(url, max_retries=3, retry_delay=2, headers=request_headers)
                if not current_response or current_response.status_code != 200:
                    continue
            
            if live_matches is not None:
                heads = live_matches.find_all('div', class_='live__matches-item__head')
                bodies = live_matches.find_all('div', class_='live__matches-item__body')
                if (not heads or not bodies) and live_cards:
                    print(
                        "🧩 live__matches wrapper found, but old item blocks are missing. "
                        "Falling back to v2 live cards."
                    )
                    heads = list(live_cards)
                    bodies = list(live_cards)
            else:
                heads = list(live_cards)
                bodies = list(live_cards)
            
            if not heads or not bodies:
                print(f"⚠️  Не найдены матчи (heads: {len(heads)}, bodies: {len(bodies)})")
                schedule_info = _extract_nearest_scheduled_match_info(soup)
                if schedule_info:
                    NEXT_SCHEDULE_MATCH_INFO = schedule_info
                    NEXT_SCHEDULE_SLEEP_SECONDS = float(schedule_info.get("sleep_seconds", 0.0) or 0.0)
                cyberscore_fallback = _maybe_get_cyberscore_heads_fallback("dltv_no_live_cards")
                if cyberscore_fallback is not None:
                    return cyberscore_fallback
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
                listing_context = _extract_live_listing_context(heads[i], bodies[i])
                title = str(listing_context.get("league_title") or "")
                href = str(listing_context.get("href") or "")
                if _is_skipped_live_league_candidate(league_title=title, href=href):
                    try:
                        heads_copy.remove(heads[i])
                        bodies_copy.remove(bodies[i])
                    except ValueError:
                        pass
                    continue
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
        # Приоритет: частотная модель из hero_position_stats.json (share + reliability).
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
                        f"({existing_player} vs {player_name}) - разрулим по hero_position_stats.json"
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
                        f"({existing_player} vs {player_name}) - разрулим по hero_position_stats.json"
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

    live_league_payload = data.get("live_league_data") or {}
    players_data = data.get("players") or live_league_payload.get("players") or []

    def _find_account_id_by_hero(hero_id: int) -> int:
        for p in players_data:
            if p.get("hero_id") == hero_id:
                direct_account_id = _coerce_int(p.get("account_id"))
                if direct_account_id > 0:
                    return int(direct_account_id)
                steam_account = p.get("steamAccount") or p.get("steam_account") or {}
                if isinstance(steam_account, dict):
                    nested_account_id = _coerce_int(
                        steam_account.get("id")
                        or steam_account.get("account_id")
                        or steam_account.get("accountId")
                    )
                    if nested_account_id > 0:
                        return int(nested_account_id)
                nested_player = p.get("player") or {}
                if isinstance(nested_player, dict):
                    nested_player_account_id = _coerce_int(
                        nested_player.get("account_id")
                        or nested_player.get("accountId")
                    )
                    if nested_player_account_id > 0:
                        return int(nested_player_account_id)
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


_TOO_FEW_PLAYERS_RE = re.compile(
    r"Слишком мало игроков:\s*radiant=(?P<radiant>\d+),\s*dire=(?P<dire>\d+)"
)


def _extract_too_few_players_counts(parse_error: Any) -> Optional[Dict[str, int]]:
    if not isinstance(parse_error, str):
        return None
    match = _TOO_FEW_PLAYERS_RE.search(parse_error)
    if match is None:
        return None
    try:
        return {
            "radiant": int(match.group("radiant")),
            "dire": int(match.group("dire")),
        }
    except (TypeError, ValueError):
        return None


def _zero_players_proxy_ban_diagnostics(parse_error: Any, data: Any) -> Optional[Dict[str, Any]]:
    counts = _extract_too_few_players_counts(parse_error)
    if not counts:
        return None
    if not isinstance(data, dict):
        return None
    fast_picks = data.get("fast_picks")
    if not fast_picks:
        return None
    if counts["radiant"] > 0 and counts["dire"] > 0:
        return None
    proxy_marker = _get_current_proxy_marker()
    return {
        "radiant": counts["radiant"],
        "dire": counts["dire"],
        "fast_picks_count": len(fast_picks) if isinstance(fast_picks, (list, tuple)) else 1,
        "proxy_marker": proxy_marker,
        "proxy_in_use": bool(USE_PROXY and proxy_marker != "__direct__"),
    }


def _retry_match_page_direct_after_zero_players(
    *,
    url: str,
    data: dict,
    radiant_team_name: str,
    dire_team_name: str,
    verbose_match_log: bool,
) -> tuple[Any, Any, Any, Any, Any]:
    parsed_retry_url = urlparse(str(url or ""))
    retry_path = str(parsed_retry_url.path or "").strip()
    canonical_url = f"https://dltv.org{retry_path}" if retry_path else str(url or "").strip()
    request_headers = globals().get(
        "headers",
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36"
            )
        },
    )
    print("   🌐 Пробую direct retry страницы матча после zero-player lineups...")
    try:
        direct_response = _perform_http_get(
            canonical_url,
            headers=request_headers,
            verify=False,
            timeout=10,
        )
    except _http_request_exceptions() as exc:
        print(f"   ⚠️ Direct retry страницы матча не удался: {type(exc).__name__}: {exc}")
        logger.warning("Direct retry after zero-player lineups failed for %s: %s", canonical_url, exc)
        return None, None, "direct_retry_request_failed", "", []
    if direct_response.status_code != 200:
        print(f"   ⚠️ Direct retry страницы матча вернул статус {direct_response.status_code}")
        logger.warning(
            "Direct retry after zero-player lineups returned status %s for %s",
            direct_response.status_code,
            canonical_url,
        )
        return None, None, f"direct_retry_status_{direct_response.status_code}", "", []
    if verbose_match_log:
        print("   ✅ Direct retry страницы матча получен")
        parse_result = parse_draft_and_positions(
            BeautifulSoup(direct_response.text, "lxml"),
            data,
            radiant_team_name,
            dire_team_name,
        )
    else:
        with contextlib.redirect_stdout(io.StringIO()):
            parse_result = parse_draft_and_positions(
                BeautifulSoup(direct_response.text, "lxml"),
                data,
                radiant_team_name,
                dire_team_name,
            )
    return parse_result


def check_head(heads, bodies, i, maps_data, return_status=None):
        # Глобальные переменные для модели киллов и enhanced predictor
        global kills_model_data, kills_stats, enhanced_predictor, monitored_matches
        
        # Константы вынесены в начало
        IP_ADDRESS = "46.229.214.49"
        MAX_RETRIES = 5
        RETRY_DELAY = 5

        listing_context = _extract_live_listing_context(heads[i], bodies[i])
        is_match_card_v2 = str(listing_context.get("layout") or "") == "match_card_v2"
        is_cyberscore_card = str(listing_context.get("source") or "").lower() == "cyberscore"
        status = str(listing_context.get("status") or "unknown").lower()
        
        if return_status != 'draft...':
            return_status = status



        # Извлечение данных
        try:
            uniq_score = int(listing_context.get("uniq_score") or 0)
            score = str(listing_context.get("score") or "0 : 0")
            href = str(listing_context.get("href") or "").strip()
            parsed_url = urlparse(href) if href else None
            path = str(parsed_url.path or "") if parsed_url else ""
            series_key_from_path = str(listing_context.get("series_id") or "")
            if not series_key_from_path and path:
                series_match = re.search(r"/matches/(\d+)", path)
                series_key_from_path = series_match.group(1) if series_match else ""
            if is_cyberscore_card:
                series_url = (
                    f"cyberscore.live{path}"
                    if path
                    else (
                        f"cyberscore.live/en/matches/{series_key_from_path}"
                        if series_key_from_path
                        else ""
                    )
                )
                check_uniq_url = (
                    f"cyberscore.live{path}.{uniq_score}"
                    if path
                    else (
                        f"cyberscore.live/en/matches/{series_key_from_path}.{uniq_score}"
                        if series_key_from_path
                        else f"cyberscore.live/match_unknown.{uniq_score}"
                    )
                )
            else:
                series_url = f'dltv.org{path}' if path else (f'dltv.org/matches/{series_key_from_path}' if series_key_from_path else "")
                check_uniq_url = (
                    f'dltv.org{path}.{uniq_score}'
                    if path
                    else (f'dltv.org/matches/{series_key_from_path}.{uniq_score}' if series_key_from_path else f'live_match_unknown.{uniq_score}')
                )
            verbose_match_log = _should_emit_verbose_match_log(check_uniq_url)
            match_log = print if verbose_match_log else (lambda *args, **kwargs: None)
            block_reason = _dispatch_block_reason(check_uniq_url)
            delayed_payload = None

            if not is_match_card_v2 and verbose_match_log:
                print(f"\n🔍 DEBUG: Начало обработки матча #{i}")
                print(f"   Статус: {status}")
                print(f"   URL: {check_uniq_url}")
                print(f"   Score: {score}")
            elif not is_match_card_v2 and check_uniq_url not in maps_data and block_reason != "processed":
                print(f"\n🔁 RECHECK матча #{i}: {check_uniq_url} | status={status}")

            if status == 'finished':
                score_values = [part.strip() for part in score.split(":")]
                while len(score_values) < 2:
                    score_values.append("0")
                finished_finalize = _finalize_finished_live_series_for_elo(
                    series_key=series_key_from_path,
                    series_url=series_url,
                    first_team_score=score_values[0],
                    second_team_score=score_values[1],
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

            if (
                not PIPELINE_BYPASS_PROCESSED_URL_GATE
                and not is_match_card_v2
                and (check_uniq_url in maps_data or block_reason == "processed")
            ):
                print(f"   ✅ Матч уже в map_id_check.txt - пропускаем: {check_uniq_url}")
                _drop_delayed_match(check_uniq_url, reason="already_in_map_id_check")
                return
            if not SIGNAL_MINIMAL_ODDS_ONLY_MODE and not is_match_card_v2 and block_reason == "uncertain_delivery":
                print(f"   ⚠️ Матч заблокирован после uncertain delivery - пропускаем")
                _drop_delayed_match(check_uniq_url, reason="uncertain_delivery_block")
                return
            if not SIGNAL_MINIMAL_ODDS_ONLY_MODE and not is_match_card_v2:
                with monitored_matches_lock:
                    delayed_payload = monitored_matches.get(check_uniq_url)
            if not SIGNAL_MINIMAL_ODDS_ONLY_MODE and not is_match_card_v2 and delayed_payload is not None:
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

        match_log(f"   🌐 Запрос JSON данных...")

        # Получаем JSON данные с retry логикой
        data = None
        max_json_retries = 3
        json_retry_errors = []
        json_url = ""
        response = None
        soup = None
        if is_cyberscore_card:
            match_id = str(listing_context.get("live_match_id") or series_key_from_path or "").strip()
            match_url = _absolute_cyberscore_url(href or f"/en/matches/{match_id}/")
            match_log(f"   🌐 Запрос страницы CyberScore...")
            response_text = _get_cyberscore_html_via_camoufox(match_url)
            if not response_text:
                print(f"   ❌ Не удалось получить страницу CyberScore")
                print(f"   ❌ Матч пропущен (ошибка CyberScore Camoufox)")
                return return_status
            cyber_item = _extract_cyberscore_match_item_from_html(response_text, match_id=match_id or None)
            if not cyber_item and match_id:
                cyber_item = CYBERSCORE_LISTING_ITEM_CACHE.get(str(match_id))
                if isinstance(cyber_item, dict):
                    print("   ℹ️ CyberScore detail item missing; using get_heads listing item cache")
            if not cyber_item:
                print(f"   ❌ Не найден CyberScore embedded match item")
                print(f"   ❌ Матч пропущен (нет CyberScore item)")
                return return_status
            data = _cyberscore_item_to_runtime_payload(cyber_item)
            soup = BeautifulSoup("", "lxml")
            json_url = match_url
            match_id = str(data.get("match_id") or match_id or "").strip()
            map_num = _coerce_int(cyber_item.get("game_map_number"))
            radiant_bo_score, dire_bo_score = _parse_cyberscore_best_of_score(cyber_item)
            score = f"{radiant_bo_score} : {dire_bo_score}"
            uniq_score = radiant_bo_score + dire_bo_score
            series_key_from_path = match_id or series_key_from_path
            path = f"/en/matches/{match_id}/" if match_id else path
            series_url = f"cyberscore.live{path}" if path else series_url
            check_uniq_url = (
                f"cyberscore.live/en/matches/{match_id}.map{map_num or uniq_score}"
                if match_id
                else f"cyberscore.live{path}.{uniq_score}"
            )
            verbose_match_log = _should_emit_verbose_match_log(check_uniq_url)
            match_log = print if verbose_match_log else (lambda *args, **kwargs: None)
            block_reason = _dispatch_block_reason(check_uniq_url)
            if verbose_match_log:
                print(f"\n🔍 DEBUG: CyberScore match #{i}")
                print(f"   Статус: {status}")
                print(f"   URL: {check_uniq_url}")
                print(f"   Score: {score}")
            elif check_uniq_url not in maps_data and block_reason != "processed":
                print(f"\n🔁 RECHECK CyberScore матча #{i}: {check_uniq_url} | status={status}")
            if (
                not PIPELINE_BYPASS_PROCESSED_URL_GATE
                and (check_uniq_url in maps_data or block_reason == "processed")
            ):
                print(f"   ✅ Матч уже в map_id_check.txt - пропускаем: {check_uniq_url}")
                _drop_delayed_match(check_uniq_url, reason="already_in_map_id_check")
                return
            if not SIGNAL_MINIMAL_ODDS_ONLY_MODE and block_reason == "uncertain_delivery":
                print(f"   ⚠️ Матч заблокирован после uncertain delivery - пропускаем")
                _drop_delayed_match(check_uniq_url, reason="uncertain_delivery_block")
                return
            if not SIGNAL_MINIMAL_ODDS_ONLY_MODE:
                with monitored_matches_lock:
                    delayed_payload = monitored_matches.get(check_uniq_url)
            print(
                "   ✅ CyberScore data: "
                f"map={map_num or 'n/a'}, time={_format_game_clock(data.get('game_time'))}, "
                f"radiant_lead={data.get('radiant_lead')}, "
                f"kills={data.get('radiant_score')}:{data.get('dire_score')}"
            )
        elif is_match_card_v2:
            live_match_id = str(listing_context.get("live_match_id") or "").strip()
            if not live_match_id:
                print("   ❌ Не найден data-match у live карточки")
                print("   ❌ Матч пропущен (нет live match id)")
                return return_status
            json_url = f"https://dltv.org/live/{live_match_id}.json"
        else:
            # HTTP запрос страницы матча нужен для получения JSON path и lineups
            url = f"https://dltv.org{path}"
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
            base = "https://dltv.org"
            json_url = urljoin(base, json_path)

        if data is None:
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

        if is_match_card_v2:
            series_info = data.get("db", {}).get("series", {}) if isinstance(data, dict) else {}
            series_slug = str(series_info.get("slug") or "").strip()
            if not series_key_from_path:
                series_key_from_path = str(series_info.get("id") or "").strip()
            if not series_key_from_path:
                print("   ❌ Не найден series id в live JSON")
                print("   ❌ Матч пропущен (нет series id)")
                return return_status
            if not series_slug:
                print("   ❌ Не найден series slug в live JSON")
                print("   ❌ Матч пропущен (нет series slug)")
                return return_status
            path = f"/matches/{series_key_from_path}/{series_slug}"
            series_url = f'dltv.org{path}'
            check_uniq_url = f'dltv.org{path}.{uniq_score}'
            verbose_match_log = _should_emit_verbose_match_log(check_uniq_url)
            match_log = print if verbose_match_log else (lambda *args, **kwargs: None)
            block_reason = _dispatch_block_reason(check_uniq_url)
            if verbose_match_log:
                print(f"\n🔍 DEBUG: Начало обработки матча #{i}")
                print(f"   Статус: {status}")
                print(f"   URL: {check_uniq_url}")
                print(f"   Score: {score}")
            elif check_uniq_url not in maps_data and block_reason != "processed":
                print(f"\n🔁 RECHECK матча #{i}: {check_uniq_url} | status={status}")

            if (
                not PIPELINE_BYPASS_PROCESSED_URL_GATE
                and (check_uniq_url in maps_data or block_reason == "processed")
            ):
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

            url = f"https://dltv.org{path}"
            match_log(f"   🌐 Запрос страницы матча...")
            response = make_request_with_retry(url, MAX_RETRIES, RETRY_DELAY)

            if not response or response.status_code != 200:
                print(f"   ❌ Не удалось получить страницу. Status code: {response.status_code if response else 'No response'}")
                print(f"   ❌ Матч пропущен (ошибка HTTP запроса)")
                return return_status

            match_log(f"   ✅ Страница получена")
            soup = BeautifulSoup(response.text, 'lxml')
        
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
        if (
            not SIGNAL_MINIMAL_ODDS_ONLY_MODE
            and not PIPELINE_BYPASS_TIER_GATE
            and (not radiant_team_ids or not dire_team_ids)
        ):
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
        league_name_normalized = _normalize_live_league_title(league_name)
        if (
            not SIGNAL_MINIMAL_ODDS_ONLY_MODE
            and not PIPELINE_BYPASS_LEAGUE_DENYLIST_GATE
            and league_name_normalized in SKIPPED_LIVE_LEAGUE_TITLES
        ):
            print(
                f"   🚫 Матч пропущен: лига в denylist ({league_name or 'unknown league'})"
            )
            add_url(
                check_uniq_url,
                reason="skip_league_title_denylist",
                details={
                    "status": status,
                    "league_name": league_name,
                    "json_retry_errors": json_retry_errors,
                },
            )
            return return_status
        
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
        dota2protracker_pipeline_bypass_active = bool(
            DOTA2PROTRACKER_ENABLED and DOTA2PROTRACKER_BYPASS_GATES
        )
        if BOOKMAKER_PREFETCH_ENABLED:
            bookmaker_map_num = _bookmaker_infer_map_num(live_league_data, score_text=score)
            if bookmaker_map_num is not None:
                match_log(f"   🗺️ Bookmaker map context: карта {bookmaker_map_num}")
            _bookmaker_prefetch_submit(
                match_key=check_uniq_url,
                radiant_team=radiant_team_name_original,
                dire_team=dire_team_name_original,
                map_num=bookmaker_map_num,
                series_url=series_url,
                league_title=str(listing_context.get("league_title") or ""),
            )
            if (
                not PURE_DLTV_MODE
                and BOOKMAKER_PREFETCH_GATE_MODE == "presence"
                and not dota2protracker_pipeline_bypass_active
                and not PIPELINE_BYPASS_BOOKMAKER_GATE
            ):
                bookmaker_presence_state, bookmaker_presence_snapshot = _bookmaker_presence_gate_resolution(check_uniq_url)
                _log_bookmaker_presence_gate(
                    check_uniq_url,
                    bookmaker_presence_state,
                    bookmaker_presence_snapshot,
                )
                if (
                    bookmaker_presence_state == "allow"
                    and _bookmaker_presence_has_partial_miss(bookmaker_presence_snapshot)
                ):
                    _log_bookmaker_presence_failure_diagnostics(
                        check_uniq_url,
                        bookmaker_presence_snapshot,
                        context="allow_partial_miss",
                        only_non_found=True,
                    )
                if bookmaker_presence_state == "pending":
                    print(
                        "   ⏳ Ожидание bookmaker presence one-shot check: "
                        f"{check_uniq_url}"
                    )
                    return return_status
                if bookmaker_presence_state == "reject":
                    if _maybe_bypass_tier1_bookmaker_presence_reject(
                        match_key=check_uniq_url,
                        status=status,
                        snapshot=bookmaker_presence_snapshot,
                        radiant_team_name=radiant_team_name_original,
                        dire_team_name=dire_team_name_original,
                        radiant_team_ids=radiant_team_ids,
                        dire_team_ids=dire_team_ids,
                    ):
                        bookmaker_presence_state = "allow"
                    else:
                        _log_bookmaker_presence_failure_diagnostics(
                            check_uniq_url,
                            bookmaker_presence_snapshot,
                            context="gate_reject",
                        )
                        _log_bookmaker_source_snapshot(check_uniq_url, decision="no_match_presence")
                        add_url(
                            check_uniq_url,
                            reason="skip_no_bookmaker_match_presence",
                            details={
                                "status": status,
                                "bookmaker_gate_mode": "presence",
                                "bookmaker_snapshot_status": str(
                                    (bookmaker_presence_snapshot or {}).get("status") or ""
                                ),
                                "bookmaker_sites": dict(
                                    (bookmaker_presence_snapshot or {}).get("sites") or {}
                                ),
                                "json_retry_errors": json_retry_errors,
                            },
                        )
                        print("   ✅ map_id_check.txt обновлен: match missing on all bookmakers")
                        return return_status
                if bookmaker_presence_state == "error":
                    _log_bookmaker_source_snapshot(check_uniq_url, decision="no_match_presence")
                    add_url(
                        check_uniq_url,
                        reason="skip_bookmaker_presence_check_error",
                        details={
                            "status": status,
                            "bookmaker_gate_mode": "presence",
                            "bookmaker_snapshot_status": str(
                                (bookmaker_presence_snapshot or {}).get("status") or ""
                            ),
                            "bookmaker_sites": dict(
                                (bookmaker_presence_snapshot or {}).get("sites") or {}
                            ),
                            "bookmaker_error": str(
                                (bookmaker_presence_snapshot or {}).get("error") or ""
                            ),
                            "json_retry_errors": json_retry_errors,
                        },
                    )
                    print("   ✅ map_id_check.txt обновлен: bookmaker presence one-shot check failed")
                    return return_status
            else:
                if BOOKMAKER_PREFETCH_ENABLED and not PURE_DLTV_MODE and BOOKMAKER_PREFETCH_GATE_MODE == "presence":
                    if dota2protracker_pipeline_bypass_active:
                        print("   ℹ️ Bookmaker presence gate bypassed by Dota2ProTracker pipeline mode")
                    elif PIPELINE_BYPASS_BOOKMAKER_GATE:
                        print("   ℹ️ Bookmaker presence gate bypassed by pipeline smoke-test mode")
                bookmaker_presence_state = "allow"

        if SIGNAL_MINIMAL_ODDS_ONLY_MODE:
            print("   🎯 Minimal odds-only mode active: пропускаю draft metrics и все gate-ветки")
            minimal_odds_message = _build_minimal_odds_only_message(
                radiant_team_name=radiant_team_name_original or radiant_team_name,
                dire_team_name=dire_team_name_original or dire_team_name,
                live_league=live_league_data,
                fallback_score_text=score,
            )
            minimal_odds_message, minimal_odds_ready, minimal_odds_reason = _prepare_minimal_odds_only_message_for_delivery(
                check_uniq_url,
                minimal_odds_message,
            )
            if not minimal_odds_ready:
                print(
                    "   ⏳ Minimal odds-only send skipped: нет ни одного числового коэффициента "
                    f"(reason={minimal_odds_reason})"
                )
                return return_status
            if _skip_dispatch_for_processed_url(check_uniq_url, "minimal odds-only dispatch"):
                return return_status
            if not _acquire_signal_send_slot(check_uniq_url):
                print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                return return_status
            try:
                if _skip_dispatch_for_processed_url(check_uniq_url, "minimal odds-only dispatch after lock"):
                    return return_status
                delivery_confirmed = _deliver_and_persist_signal(
                    check_uniq_url,
                    minimal_odds_message,
                    add_url_reason="minimal_odds_only_signal_sent_now",
                    add_url_details={
                        "status": status,
                        "dispatch_mode": "minimal_odds_only",
                        "dispatch_status_label": "minimal_odds_only_immediate",
                        "json_retry_errors": json_retry_errors,
                        "bookmaker_ready_reason": minimal_odds_reason,
                    },
                    skip_bookmaker_prepare=True,
                )
                if delivery_confirmed:
                    print("   ✅ ВЕРДИКТ: minimal odds-only сигнал отправлен")
            finally:
                _release_signal_send_slot(check_uniq_url)
            return return_status

        fast_picks_payload = data.get('fast_picks')
        if isinstance(fast_picks_payload, dict):
            has_fast_picks = any(bool(value) for value in fast_picks_payload.values())
        else:
            has_fast_picks = bool(fast_picks_payload)
        if not has_fast_picks:
            print(f"   ❌ Нет 'fast_picks' в данных - драфт не начался")
            print(f"   ℹ️ Драфт еще не начался")
            return return_status
        
        match_log(f"   ✅ fast_picks найдены - драфт начался")

        # Tier-режим для star-сигналов:
        # - Tier 2 матч: если хотя бы одна команда Tier 2
        # - Tier 1 матч: если обе команды Tier 1
        # - Неизвестная команда автоматически добавляется в Tier 2 без Telegram уведомления
        if PIPELINE_BYPASS_TIER_GATE:
            radiant_team_id = (_coerce_int(radiant_team_ids[0]) if radiant_team_ids else 0) or 0
            dire_team_id = (_coerce_int(dire_team_ids[0]) if dire_team_ids else 0) or 0
            star_match_tier = _determine_star_signal_match_tier(radiant_team_id, dire_team_id) or 2
            print(f"   ℹ️ Tier gate bypassed by pipeline smoke-test mode (tier={star_match_tier})")
        else:
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
        cyberscore_draft = data.get("_cyberscore_heroes_and_pos") if isinstance(data, dict) else None
        if isinstance(cyberscore_draft, dict):
            radiant_heroes_and_pos = dict(cyberscore_draft.get("radiant") or {})
            dire_heroes_and_pos = dict(cyberscore_draft.get("dire") or {})
            parse_error = data.get("_cyberscore_draft_error")
            problem_summary = ""
            problem_candidates = []
            if not parse_error:
                match_log(
                    "   ✅ CyberScore draft parsed: "
                    f"radiant={len(radiant_heroes_and_pos)}, dire={len(dire_heroes_and_pos)}"
                )
        elif verbose_match_log:
            radiant_heroes_and_pos, dire_heroes_and_pos, parse_error, problem_summary, problem_candidates = parse_draft_and_positions(
                soup, data, radiant_team_name_original, dire_team_name_original
            )
        else:
            with contextlib.redirect_stdout(io.StringIO()):
                radiant_heroes_and_pos, dire_heroes_and_pos, parse_error, problem_summary, problem_candidates = parse_draft_and_positions(
                    soup, data, radiant_team_name_original, dire_team_name_original
                )
        
        if parse_error:
            zero_players_diag = _zero_players_proxy_ban_diagnostics(parse_error, data)
            if zero_players_diag:
                proxy_marker = str(zero_players_diag.get("proxy_marker") or "__unknown__")
                radiant_count = int(zero_players_diag.get("radiant") or 0)
                dire_count = int(zero_players_diag.get("dire") or 0)
                diag_message = (
                    "fast_picks есть, но HTML lineups вернул 0 игроков "
                    f"(radiant={radiant_count}, dire={dire_count}, proxy={proxy_marker})"
                )
                if bool(zero_players_diag.get("proxy_in_use")):
                    print(f"   ⚠️ Подозрение на забаненный/битый прокси: {diag_message}")
                    logger.warning(
                        "MATCH_PAGE_PROXY_SUSPECTED_ZERO_PLAYERS url=%s proxy=%s "
                        "radiant=%s dire=%s",
                        check_uniq_url,
                        proxy_marker,
                        radiant_count,
                        dire_count,
                    )
                    (
                        direct_radiant_heroes_and_pos,
                        direct_dire_heroes_and_pos,
                        direct_parse_error,
                        direct_problem_summary,
                        direct_problem_candidates,
                    ) = _retry_match_page_direct_after_zero_players(
                        url=url,
                        data=data,
                        radiant_team_name=radiant_team_name_original,
                        dire_team_name=dire_team_name_original,
                        verbose_match_log=verbose_match_log,
                    )
                    if not direct_parse_error:
                        print("   ✅ Direct retry восстановил HTML lineups, продолжаем обработку")
                        logger.info(
                            "MATCH_PAGE_DIRECT_RETRY_RECOVERED url=%s old_proxy=%s",
                            check_uniq_url,
                            proxy_marker,
                        )
                        radiant_heroes_and_pos = direct_radiant_heroes_and_pos
                        dire_heroes_and_pos = direct_dire_heroes_and_pos
                        parse_error = None
                        problem_summary = direct_problem_summary
                        problem_candidates = direct_problem_candidates
                    else:
                        if _rotate_to_untried_proxy({proxy_marker}):
                            print("   🔄 Переключаю прокси после zero-player lineups")
                        else:
                            print("   ℹ️ Дополнительных прокси для match-page retry нет")
                else:
                    print(f"   ⚠️ HTML страницы матча вернул 0 игроков даже без прокси: {diag_message}")
                    logger.warning(
                        "MATCH_PAGE_ZERO_PLAYERS_WITHOUT_PROXY url=%s radiant=%s dire=%s",
                        check_uniq_url,
                        radiant_count,
                        dire_count,
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
        skipped_player_hits = _find_skipped_player_account_ids(
            radiant_account_ids,
            dire_account_ids,
        )
        if skipped_player_hits["radiant"] or skipped_player_hits["dire"]:
            print(
                "   🚫 Найдены игроки из player denylist "
                f"(radiant={skipped_player_hits['radiant']}, dire={skipped_player_hits['dire']})"
            )
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

        def _run_dota2protracker_enrichment() -> Dict[str, Any]:
            payload = _blank_dota2protracker_result()
            if not DOTA2PROTRACKER_ENABLED:
                return payload
            if enrich_with_pro_tracker is None:
                print("   ⚠️ Dota2ProTracker enrichment skipped: module unavailable")
                return payload
            try:
                if not _install_dota2protracker_shared_camoufox_fetcher():
                    print("   ⚠️ Dota2ProTracker enrichment skipped: shared Camoufox hook unavailable")
                    return payload
                return enrich_with_pro_tracker(
                    radiant_heroes_and_pos=radiant_heroes_and_pos,
                    dire_heroes_and_pos=dire_heroes_and_pos,
                    synergy_dict=payload,
                    min_games=DOTA2PROTRACKER_MIN_GAMES,
                )
            except Exception as e:
                print(f"   ⚠️ Dota2ProTracker enrichment failed: {e}")
                return _blank_dota2protracker_result()

        def _run_local_dictionary_metrics() -> Dict[str, Any]:
            # Отправляем только "сырые" сигналы без wrapper.
            prev_wrapper_enabled = os.getenv("SIGNAL_WRAPPER_ENABLED")
            os.environ["SIGNAL_WRAPPER_ENABLED"] = "0"
            try:
                draft_lookup_keys = _draft_stats_lookup_keys(
                    radiant_heroes_and_pos,
                    dire_heroes_and_pos,
                )
                scoped_early_dict = _prepare_draft_scoped_stats_lookup(
                    early_dict,
                    radiant_heroes_and_pos,
                    dire_heroes_and_pos,
                    draft_lookup_keys,
                )
                scoped_late_dict = _prepare_draft_scoped_stats_lookup(
                    late_dict,
                    radiant_heroes_and_pos,
                    dire_heroes_and_pos,
                    draft_lookup_keys,
                )
                scoped_post_lane_dict = _prepare_draft_scoped_stats_lookup(
                    post_lane_dict,
                    radiant_heroes_and_pos,
                    dire_heroes_and_pos,
                    draft_lookup_keys,
                )
                return synergy_and_counterpick(
                    radiant_heroes_and_pos=radiant_heroes_and_pos,
                    dire_heroes_and_pos=dire_heroes_and_pos,
                    early_dict=scoped_early_dict,
                    mid_dict=scoped_late_dict,
                    post_lane_dict=scoped_post_lane_dict,
                )
            finally:
                if prev_wrapper_enabled is None:
                    os.environ.pop("SIGNAL_WRAPPER_ENABLED", None)
                else:
                    os.environ["SIGNAL_WRAPPER_ENABLED"] = prev_wrapper_enabled

        if PIPELINE_METRICS_PARALLEL_ENABLED and DOTA2PROTRACKER_ENABLED:
            print("   🧵 Draft metrics: local dictionaries + Dota2ProTracker in parallel")
            with ThreadPoolExecutor(max_workers=2, thread_name_prefix="draft-metrics") as executor:
                local_future = executor.submit(_run_local_dictionary_metrics)
                protracker_future = executor.submit(_run_dota2protracker_enrichment)
                s = local_future.result()
                protracker_payload = protracker_future.result()
        else:
            protracker_payload = _run_dota2protracker_enrichment()
            s = _run_local_dictionary_metrics()

        if DOTA2PROTRACKER_ENABLED:
            for _line in _build_dota2protracker_log_lines(protracker_payload):
                print(_line)

        if DOTA2PROTRACKER_ENABLED and DOTA2PROTRACKER_ONLY_MODE and not PIPELINE_BYPASS_PROTRACKER_GATE:
            if not _has_valid_dota2protracker_signal(protracker_payload):
                print(
                    "   ⚠️ Dota2ProTracker-only dispatch skipped: both metrics invalid. "
                    f"{_build_dota2protracker_debug_summary(protracker_payload)}"
                )
                return return_status
            if not _has_dispatchable_dota2protracker_signal(protracker_payload):
                print(
                    "   ⚠️ Dota2ProTracker-only dispatch skipped: below gate threshold. "
                    f"{_build_dota2protracker_debug_summary(protracker_payload)}; "
                    f"{_build_dota2protracker_gate_summary(protracker_payload)}"
                )
                return return_status
            protracker_message_text = _build_dota2protracker_only_message(
                radiant_team_name=radiant_team_name_original or radiant_team_name,
                dire_team_name=dire_team_name_original or dire_team_name,
                live_league=live_league_data,
                protracker_payload=protracker_payload,
            )
            if _skip_dispatch_for_processed_url(check_uniq_url, "dota2protracker only dispatch"):
                return return_status
            if not _acquire_signal_send_slot(check_uniq_url):
                print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                return return_status
            try:
                if _skip_dispatch_for_processed_url(check_uniq_url, "dota2protracker only dispatch after lock"):
                    return return_status
                delivery_confirmed = _deliver_and_persist_signal(
                    check_uniq_url,
                    protracker_message_text,
                    add_url_reason="dota2protracker_signal_sent_now",
                    add_url_details={
                        "status": status,
                        "dispatch_mode": "dota2protracker_only",
                        "dispatch_status_label": "dota2protracker_only_immediate",
                        "json_retry_errors": json_retry_errors,
                        "pro_cp1vs1": float(protracker_payload.get("pro_cp1vs1_late") or 0.0),
                        "pro_cp1vs1_valid": bool(protracker_payload.get("pro_cp1vs1_valid")),
                        "pro_duo_synergy": float(protracker_payload.get("pro_duo_synergy_late") or 0.0),
                        "pro_duo_synergy_valid": bool(protracker_payload.get("pro_duo_synergy_valid")),
                    },
                    skip_bookmaker_prepare=bool(DOTA2PROTRACKER_SKIP_BOOKMAKER_GATE),
                )
                if delivery_confirmed:
                    print("   ✅ ВЕРДИКТ: Dota2ProTracker-only сигнал отправлен")
            finally:
                _release_signal_send_slot(check_uniq_url)
            return return_status
        elif DOTA2PROTRACKER_ENABLED and DOTA2PROTRACKER_ONLY_MODE and PIPELINE_BYPASS_PROTRACKER_GATE:
            print("   ℹ️ Dota2ProTracker-only gates bypassed by pipeline smoke-test mode")

        if DOTA2PROTRACKER_ENABLED and isinstance(protracker_payload, dict):
            s.update(protracker_payload)

        # Обогащение данными с OpenDota API (cp1vs1, duo synergy из pub-игр)
        if enrich_with_opendota and not (
            DOTA2PROTRACKER_ENABLED and DOTA2PROTRACKER_SUPERSEDE_OPENDOTA
        ):
            try:
                s = enrich_with_opendota(
                    radiant_heroes_and_pos=radiant_heroes_and_pos,
                    dire_heroes_and_pos=dire_heroes_and_pos,
                    synergy_dict=s,
                    min_games=OPENDOTA_MIN_GAMES
                )
                if verbose_match_log:
                    pro_cp = s.get('pro_cp1vs1_early', 0)
                    pro_duo = s.get('pro_duo_synergy_early', 0)
                    print(f"   📊 OpenDota: cp1vs1={pro_cp:+.1f}%, duo_synergy={pro_duo:+.1f}%")
            except Exception as e:
                print(f"   ⚠️ OpenDota enrichment failed: {e}")

        if LIVE_LANE_ANALYSIS_ENABLED and lane_data is not None:
            s['top'], s['bot'], s['mid'] = calculate_lanes(
                radiant_heroes_and_pos,
                dire_heroes_and_pos,
                lane_data,
            )
        else:
            s['top'], s['bot'], s['mid'] = "", "", ""
        lane_top_log = str(s.get('top') or '').strip()
        lane_mid_log = str(s.get('mid') or '').strip()
        lane_bot_log = str(s.get('bot') or '').strip()
        if verbose_match_log and LIVE_LANE_ANALYSIS_ENABLED:
            print("   🛣️ Lanes:")
            print(f"      {lane_top_log or 'Top: n/a'}")
            print(f"      {lane_mid_log or 'Mid: n/a'}")
            print(f"      {lane_bot_log or 'Bot: n/a'}")
        if PIPELINE_SEND_EVERY_PARSED_MATCH:
            pipeline_message_text = _build_pipeline_probe_message(
                radiant_team_name=radiant_team_name_original or radiant_team_name,
                dire_team_name=dire_team_name_original or dire_team_name,
                live_league=live_league_data,
                fallback_score_text=score,
                game_time_seconds=game_time,
                radiant_lead=lead,
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos,
                metrics_payload=s,
                protracker_payload=protracker_payload,
            )
            if (
                not PIPELINE_BYPASS_PROCESSED_URL_GATE
                and _skip_dispatch_for_processed_url(check_uniq_url, "pipeline send-every parsed match")
            ):
                return return_status
            if not _acquire_signal_send_slot(check_uniq_url):
                print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                return return_status
            try:
                if (
                    not PIPELINE_BYPASS_PROCESSED_URL_GATE
                    and _skip_dispatch_for_processed_url(check_uniq_url, "pipeline send-every parsed match after lock")
                ):
                    return return_status
                delivery_confirmed = _deliver_and_persist_signal(
                    check_uniq_url,
                    pipeline_message_text,
                    add_url_reason="pipeline_send_every_parsed_match",
                    add_url_details={
                        "status": status,
                        "dispatch_mode": "pipeline_send_every_parsed_match",
                        "dispatch_status_label": "pipeline_probe_immediate",
                        "source_mode": DLTV_SOURCE_MODE,
                        "json_retry_errors": json_retry_errors,
                        "pipeline_disable_signal_gates": bool(PIPELINE_DISABLE_SIGNAL_GATES),
                        "pipeline_metrics_parallel": bool(PIPELINE_METRICS_PARALLEL_ENABLED),
                        "dota2protracker_enabled": bool(DOTA2PROTRACKER_ENABLED),
                        "live_lane_analysis_enabled": bool(LIVE_LANE_ANALYSIS_ENABLED),
                        "has_post_lane_output": bool((s.get("post_lane_output") or {})),
                    },
                    skip_bookmaker_prepare=bool(PIPELINE_SKIP_BOOKMAKER_PREPARE_ON_SEND),
                )
                if delivery_confirmed:
                    print("   ✅ ВЕРДИКТ: pipeline smoke-test матч отправлен в Telegram/VK")
            finally:
                _release_signal_send_slot(check_uniq_url)
            return return_status
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
                f"early={_format_star_block_status_with_side(primary_star_early_diag)}, "
                f"late={_format_star_block_status_with_side(primary_star_late_diag)}, "
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
                f"WR65: early={_format_star_block_status_with_side(early65_gate_diag)}"
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
                    f"early={_format_star_block_status_with_side(fallback_star_early_diag)}, "
                    f"late={_format_star_block_status_with_side(fallback_star_late_diag)}, "
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
            late_min60_diag = _star_block_diagnostics(
                raw_block=s.get('mid_output', {}),
                target_wr=60,
                section="mid_output",
            )
            has_selected_early_star = bool(selected_early_diag.get("valid"))
            has_selected_late_star = bool(selected_late_diag.get("valid"))
            selected_early_sign = selected_early_diag.get("sign") if has_selected_early_star else None
            selected_late_sign = selected_late_diag.get("sign") if has_selected_late_star else None
            late_no_star_guard_against_early = _build_no_late_star_late_block_guard(
                s.get('mid_output', {}),
                expected_sign=selected_early_sign,
                target_wr=selected_star_wr,
                section="mid_output",
            )
            late_core_same_sign_diag = dict(
                late_no_star_guard_against_early.get("core_same_sign_diag") or {}
            )
            late_core_same_sign_support = bool(
                late_no_star_guard_against_early.get("core_same_sign_support")
            )
            late_star_hits_against_early_diag = dict(
                late_no_star_guard_against_early.get("star_hit_diag") or {}
            )
            early65_sign = (
                early65_gate_diag.get("sign")
                if isinstance(early65_gate_diag, dict) and early65_gate_diag.get("valid")
                else None
            )
            late_no_star_guard_against_early65 = _build_no_late_star_late_block_guard(
                s.get('mid_output', {}),
                expected_sign=early65_sign,
                target_wr=selected_star_wr,
                section="mid_output",
            )
            late_star_hits_against_early65_diag = dict(
                late_no_star_guard_against_early65.get("star_hit_diag") or {}
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
                min_abs_by_metric=_STAR_LATE_CORE_MIN_ABS_BY_METRIC,
            )
            early_core_same_sign_support = bool(
                early_core_same_or_zero_diag.get("valid")
                and early_core_same_or_zero_diag.get("nonzero_metrics")
            )
            early_core_conflict = bool(early_core_same_or_zero_diag.get("conflicting_metrics"))
            # Приоритет dispatch:
            # 1) full-star same-sign
            # 2) early-star + no valid late block (WR60, min 2 hits)
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
                and not bool(late_min60_diag.get("valid"))
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
                and star_match_tier == 1
                and early65_gate_diag is not None
                and bool(early65_gate_diag.get("valid"))
                and not bool(late_min60_diag.get("valid"))
            )
            send_now_immediate = (
                send_now_full_star
                or send_now_early_star_late_core_same_sign
                or send_now_late_star_early_core_same_sign
                or force_odds_signal_test_active
            )

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

            pro_cp_early = s.get('pro_cp1vs1_early', 0)
            pro_duo_early = s.get('pro_duo_synergy_early', 0)
            pro_cp_late = s.get('pro_cp1vs1_late', 0)
            pro_duo_late = s.get('pro_duo_synergy_late', 0)

            print(f"   📊 LANING (20-28 min): {early_output.get('counterpick_1vs1', 'N/A')}, {early_output.get('pos1_vs_pos1', 'N/A')}, {early_output.get('counterpick_1vs2', 'N/A')}, {early_output.get('solo', 'N/A')}, {early_output.get('synergy_duo', 'N/A')}, {early_output.get('synergy_trio', 'N/A')}")
            print(f"   📊 LATE (28-60 min): {mid_output.get('counterpick_1vs1', 'N/A')}, {mid_output.get('pos1_vs_pos1', 'N/A')}, {mid_output.get('counterpick_1vs2', 'N/A')}, {mid_output.get('solo', 'N/A')}, {mid_output.get('synergy_duo', 'N/A')}, {mid_output.get('synergy_trio', 'N/A')}")
            if DOTA2PROTRACKER_ENABLED and isinstance(s, dict):
                for _line in _build_dota2protracker_log_lines(s):
                    print(_line)

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
            early_block = _format_metrics("Early 20-28:", early_output, metric_list)
            mid_block = _format_metrics("Late: (28-60 min):", mid_output, metric_list)
            early_block_log = _format_metrics("Early 20-28:", early_output_log, metric_list)
            mid_block_log = _format_metrics("Late: (28-60 min):", mid_output_log, metric_list)
            dota2protracker_lane_adv_line = (
                _build_dota2protracker_lane_adv_line(s)
                if DOTA2PROTRACKER_MESSAGE_BLOCK_ENABLED and _has_valid_dota2protracker_signal(s)
                else ""
            )
            if dota2protracker_lane_adv_line:
                early_block = f"{early_block.rstrip()}\n{dota2protracker_lane_adv_line}"
            dota2protracker_block = (
                _build_dota2protracker_block(s)
                if DOTA2PROTRACKER_MESSAGE_BLOCK_ENABLED and _has_valid_dota2protracker_signal(s)
                else ""
            )
            star_metrics_snapshot = _build_star_metrics_snapshot(
                early_block_log=early_block_log,
                mid_block_log=mid_block_log,
                raw_star_early_summary=raw_star_early_summary,
                raw_star_late_summary=raw_star_late_summary,
                star_diag_lines=star_diag_lines,
            )

            # Серия: только счет
            series_score_line = _build_series_score_line(data.get('live_league_data') or {})

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
            opposite_signs_selected = bool(
                has_selected_early_star
                and has_selected_late_star
                and selected_early_sign in (-1, 1)
                and selected_late_sign in (-1, 1)
                and selected_early_sign != selected_late_sign
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
                        f"(raw_wr={float(team_elo_meta['raw_radiant_wr']):.1f}%/{float(team_elo_meta['raw_dire_wr']):.1f}%)"
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
                        f"after_penalty={float(selected_early_diag.get('elo_adjusted_wr_pct') or 0.0):.1f}%)"
                    )
            if raw_selected_late_valid and not bool(selected_late_diag.get("valid")):
                if verbose_match_log:
                    print(
                        "   ⚠️ Late star invalidated by ELO block guard "
                        f"(raw_wr={float(selected_late_diag.get('block_wr_pct') or 0.0):.1f}%, "
                        f"penalty={float(selected_late_diag.get('elo_wr_penalty_pp') or 0.0):.1f}, "
                        f"after_penalty={float(selected_late_diag.get('elo_adjusted_wr_pct') or 0.0):.1f}%)"
                    )
            star_diag_lines.append(
                (
                    "ELO60: "
                    f"early={_format_star_block_status_with_side(selected_early_diag)}, "
                    f"late={_format_star_block_status_with_side(selected_late_diag)}"
                )
            )
            if isinstance(star_metrics_snapshot, dict):
                star_metrics_snapshot["star_diag_lines"] = [str(line) for line in star_diag_lines]

            has_selected_early_star = bool(selected_early_diag.get("valid"))
            has_selected_late_star = bool(selected_late_diag.get("valid"))
            selected_early_sign = selected_early_diag.get("sign") if has_selected_early_star else None
            selected_late_sign = selected_late_diag.get("sign") if has_selected_late_star else None
            if verbose_match_log:
                print(
                    "   "
                    + _format_admin_star_signal_summary_line(
                        "Early signal",
                        has_star=has_selected_early_star,
                        sign=selected_early_sign,
                        wr_pct=early_wr_pct,
                        radiant_team_name=radiant_team_name_original,
                        dire_team_name=dire_team_name_original,
                    )
                )
                print(
                    "   "
                    + _format_admin_star_signal_summary_line(
                        "Late signal",
                        has_star=has_selected_late_star,
                        sign=selected_late_sign,
                        wr_pct=late_wr_pct,
                        radiant_team_name=radiant_team_name_original,
                        dire_team_name=dire_team_name_original,
                    )
                )
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
            raw_early_opposite_single_hit_against_late = bool(
                has_selected_late_star
                and not has_selected_early_star
                and selected_late_sign in (-1, 1)
                and raw_selected_early_diag.get("sign") in (-1, 1)
                and int(raw_selected_early_diag.get("hit_count") or 0) == 1
                and str(raw_selected_early_diag.get("status") or "") == "insufficient_hits"
                and int(raw_selected_early_diag.get("sign")) != int(selected_late_sign)
            )
            if raw_early_opposite_single_hit_against_late:
                opposite_signs_selected = True
            late_no_star_guard_against_early = _build_no_late_star_late_block_guard(
                s.get('mid_output', {}),
                expected_sign=selected_early_sign,
                target_wr=selected_star_wr,
                section="mid_output",
            )
            late_core_same_sign_diag = dict(
                late_no_star_guard_against_early.get("core_same_sign_diag") or {}
            )
            late_core_same_sign_support = bool(
                late_no_star_guard_against_early.get("core_same_sign_support")
            )
            late_star_hits_against_early_diag = dict(
                late_no_star_guard_against_early.get("star_hit_diag") or {}
            )
            early65_sign = (
                early65_gate_diag.get("sign")
                if isinstance(early65_gate_diag, dict) and early65_gate_diag.get("valid")
                else None
            )
            late_no_star_guard_against_early65 = _build_no_late_star_late_block_guard(
                s.get('mid_output', {}),
                expected_sign=early65_sign,
                target_wr=selected_star_wr,
                section="mid_output",
            )
            late_star_hits_against_early65_diag = dict(
                late_no_star_guard_against_early65.get("star_hit_diag") or {}
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
                min_abs_by_metric=_STAR_LATE_CORE_MIN_ABS_BY_METRIC,
            )
            early_core_same_sign_support = bool(
                early_core_same_or_zero_diag.get("valid")
                and early_core_same_or_zero_diag.get("nonzero_metrics")
            )
            early_core_conflict = bool(early_core_same_or_zero_diag.get("conflicting_metrics"))
            early_star_no_late_same_sign_gate = _early_star_no_late_same_sign_gate(
                selected_early_diag=selected_early_diag,
                has_selected_early_star=has_selected_early_star,
                has_selected_late_star=has_selected_late_star,
                early_wr_pct=early_wr_pct,
                late_min_wr_diag=late_min60_diag,
                both_teams_tier1=(star_match_tier == 1),
            )
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
                and bool(early_star_no_late_same_sign_gate.get("valid"))
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
                and star_match_tier == 1
                and early_wr_pct is not None
                and float(early_wr_pct) >= 70.0
                and early65_gate_diag is not None
                and bool(early65_gate_diag.get("valid"))
                and not bool(late_min60_diag.get("valid"))
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
                        "late_star_hits_against_early_diag": late_star_hits_against_early_diag,
                        "late_star_hits_against_early65_diag": late_star_hits_against_early65_diag,
                        "selected_early_diag": selected_early_diag,
                        "selected_late_diag": selected_late_diag,
                        "early_star_no_late_same_sign_gate": early_star_no_late_same_sign_gate,
                        "json_retry_errors": json_retry_errors,
                    },
                )
                print("   ✅ map_id_check.txt обновлен: add_url после отказа no-late-star")
                return return_status

            if top25_late_elo_block_override_active and verbose_match_log:
                override_target_side = str(top25_late_elo_block_override.get("target_side") or "")
                override_rank = top25_late_elo_block_override.get("leaderboard_rank")
                override_raw_wr = top25_late_elo_block_override.get("elo_target_wr")
                override_mode = str(top25_late_elo_block_override.get("mode") or "unknown")
                override_raw_wr_label = (
                    f"{float(override_raw_wr):.1f}%"
                    if override_raw_wr is not None
                    else "n/a"
                )
                print(
                    "   ✅ Override: raw late star kept alive despite ELO block "
                    f"because target side is top-{int(TOP25_LATE_ELO_BLOCK_RANK_THRESHOLD)} "
                    f"(mode={override_mode}, side={override_target_side}, "
                    f"rank={override_rank}, "
                    f"raw_wr={override_raw_wr_label})"
                )
            if top25_late_elo_block_override_active:
                override_raw_wr = top25_late_elo_block_override.get("elo_target_wr")
                override_mode = str(top25_late_elo_block_override.get("mode") or "unknown")
                raw_wr_label = (
                    f"{float(override_raw_wr):.1f}%"
                    if override_raw_wr is not None
                    else "n/a"
                )
                star_diag_lines.append(
                    "Top25LateEloBlock: "
                    f"enabled(mode={override_mode},rank={int(top25_late_elo_block_override.get('leaderboard_rank') or 0)},"
                    f"target_side={top25_late_elo_block_override.get('target_side')},"
                    f"raw_wr={raw_wr_label})"
                )
                if isinstance(star_metrics_snapshot, dict):
                    star_metrics_snapshot["star_diag_lines"] = [str(line) for line in star_diag_lines]

            if send_now_early_star_late_core_same_sign:
                if verbose_match_log:
                    print(
                        "   ✅ Override: early star without valid late star allowed "
                        f"(early_wr>={float(early_star_no_late_same_sign_gate.get('min_wr_required') or 0.0):.0f}, "
                        f"early_hit_count={int(early_star_no_late_same_sign_gate.get('early_hit_count') or 0)}, "
                        f"late_wr60_has_star={bool(early_star_no_late_same_sign_gate.get('late_min_wr_has_star'))}) "
                        f"(sign={selected_early_sign})"
                    )
            elif (
                verbose_match_log
                and has_selected_early_star
                and not has_selected_late_star
            ):
                gate_wr_label = (
                    f"{float(early_star_no_late_same_sign_gate.get('early_wr_pct')):.1f}%"
                    if early_star_no_late_same_sign_gate.get("early_wr_pct") is not None
                    else "n/a"
                )
                print(
                    "   ⚠️ Early-star no-valid-late override blocked by early gate "
                    f"(early_wr={gate_wr_label}, "
                    f"min_wr_ok={bool(early_star_no_late_same_sign_gate.get('min_wr_ok'))}, "
                    f"early_hit_count={int(early_star_no_late_same_sign_gate.get('early_hit_count') or 0)}, "
                    f"min_hit_count_ok={bool(early_star_no_late_same_sign_gate.get('min_hit_count_ok'))}, "
                    f"late_wr60_has_star={bool(early_star_no_late_same_sign_gate.get('late_min_wr_has_star'))})"
                )
            if early65_gate_active:
                if verbose_match_log:
                    print(
                        "   ✅ Override: early star WR65+ activates early gate "
                        f"(sign={early65_gate_diag.get('sign')}, "
                        f"late_wr60_has_star={bool(late_min60_diag.get('valid'))})"
                    )
            elif (
                verbose_match_log
                and isinstance(early65_gate_diag, dict)
                and bool(early65_gate_diag.get("valid"))
                and bool(late_min60_diag.get("valid"))
            ):
                print(
                    "   ⚠️ Early WR65 override blocked by valid late WR60 block "
                    f"(late_hit_count={len(late_min60_diag.get('hit_metrics') or [])})"
                )
            if send_now_late_star_early_core_same_sign:
                if verbose_match_log:
                    print(
                        "   ✅ Override: late star without early star allowed because "
                        "early core(cp1v1/pos1vpos1/cp1v2/solo) are same-sign "
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
            opposite_signs_early90_tier1_fast_release = _opposite_signs_early90_tier1_fast_release_config(
                team_elo_meta=team_elo_meta,
                early_wr_pct=early_wr_pct,
                selected_early_sign=selected_early_sign,
                selected_late_sign=selected_late_sign,
                radiant_team_id=radiant_team_id,
                dire_team_id=dire_team_id,
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
                        f"wait_until={_format_game_clock(opposite_signs_early90_monitor.get('target_game_time'))}, "
                        "release=post_target_comeback_ceiling"
                    )
            if isinstance(opposite_signs_early90_tier1_fast_release, dict) and opposite_signs_early90_tier1_fast_release.get("enabled"):
                if verbose_match_log:
                    print(
                        "   📈 Opposite-sign WR90 Tier1 fast-release: "
                        f"target_side={target_side}, "
                        f"4-10 need>={int(opposite_signs_early90_tier1_fast_release.get('threshold_4_to_10') or 0)}, "
                        f"10-20 need>={int(opposite_signs_early90_tier1_fast_release.get('threshold_10_to_20') or 0)}, "
                        f"fallback_at={_format_game_clock(opposite_signs_early90_tier1_fast_release.get('target_game_time'))}"
                    )
            if raw_early_opposite_single_hit_against_late and verbose_match_log:
                print(
                    "   ⚠️ Raw early opposite single-hit detected; "
                    "dispatch is treated as opposite-sign and delayed until "
                    f"{_format_game_clock(LATE_PUB_COMEBACK_TABLE_START_SECONDS)} "
                    f"(early_sign={raw_selected_early_diag.get('sign')}, "
                    f"late_sign={selected_late_sign}, "
                    f"early_hits={int(raw_selected_early_diag.get('hit_count') or 0)})"
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
            wr_block = ""
            wr_lines = []

            def _signal_team_name(sign: Optional[int]) -> str:
                side = _target_side_from_sign(sign)
                if side == "radiant":
                    return str(radiant_team_name_original or radiant_team_name or "Radiant")
                if side == "dire":
                    return str(dire_team_name_original or dire_team_name or "Dire")
                return ""

            early_display_sign = selected_early_sign
            if early_display_sign not in (-1, 1):
                early_display_sign = _star_block_sign(early_output_log)
            if early_display_sign not in (-1, 1) and send_now_late_star_early_core_same_sign:
                early_display_sign = selected_late_sign

            late_display_sign = (
                raw_selected_late_diag.get("sign")
                if top25_late_elo_block_override_active and raw_selected_late_diag.get("sign") in (-1, 1)
                else selected_late_sign
            )
            if late_display_sign not in (-1, 1):
                late_display_sign = _star_block_sign(mid_output_log)
            if late_display_sign not in (-1, 1) and send_now_early_star_late_core_same_sign:
                late_display_sign = selected_early_sign

            if telegram_early_rec:
                early_team_name = _signal_team_name(early_display_sign)
                early_line = (
                    f"Early: {early_team_name} WR≈{float(early_wr_pct or 0.0):.1f}%"
                    if early_team_name
                    else f"Early: WR≈{float(early_wr_pct or 0.0):.1f}%"
                )
                if isinstance(telegram_early_rec, dict) and telegram_early_rec.get("min_odds"):
                    early_line += f" от кэфа {float(telegram_early_rec['min_odds']):.2f}"
                wr_lines.append(early_line)
            if late_rec:
                late_team_name = _signal_team_name(late_display_sign)
                late_line = (
                    f"Late: {late_team_name} WR≈{float(late_wr_pct or 0.0):.1f}%"
                    if late_team_name
                    else f"Late: WR≈{float(late_wr_pct or 0.0):.1f}%"
                )
                if isinstance(late_rec, dict) and late_rec.get("min_odds"):
                    late_line += f" от кэфа {float(late_rec['min_odds']):.2f}"
                wr_lines.append(late_line)
            if wr_lines:
                wr_block = "Оценка WR:\n" + "\n".join(wr_lines) + "\n"

            odds_block = ""
            if not PURE_DLTV_MODE and BOOKMAKER_PREFETCH_ENABLED:
                if BOOKMAKER_PREFETCH_GATE_MODE == "presence":
                    bookmaker_gate_block = ""
                    bookmaker_gate_ready = False
                    bookmaker_gate_reason = "not_requested"
                    bookmaker_gate_block, bookmaker_gate_ready, bookmaker_gate_reason = _bookmaker_format_gate_block(check_uniq_url)
                    if not bookmaker_gate_ready:
                        if bookmaker_gate_reason in {"no_match_presence"}:
                            _log_bookmaker_source_snapshot(check_uniq_url, decision=bookmaker_gate_reason)
                        print(
                            "   ⏳ Пропуск STAR-сигнала: bookmaker gate требует "
                            "наличие названия матча хотя бы у одного букмекера "
                            f"(mode={BOOKMAKER_PREFETCH_GATE_MODE}, reason={bookmaker_gate_reason}) "
                            f"для {check_uniq_url}"
                        )
                        return return_status
                    if bookmaker_gate_block:
                        odds_block = bookmaker_gate_block
                else:
                    bookmaker_presence_state, bookmaker_presence_snapshot = _bookmaker_presence_gate_resolution(check_uniq_url)
                    if bookmaker_presence_state == "pending":
                        print(
                            "   ⏳ Пропуск STAR-сигнала: bookmaker odds ждут initial presence "
                            f"для {check_uniq_url}"
                        )
                        return return_status
                    if bookmaker_presence_state == "reject":
                        _log_bookmaker_source_snapshot(check_uniq_url, decision="no_match_presence")
                        print(
                            "   ⏳ Пропуск STAR-сигнала: bookmaker odds не активируются "
                            f"без матча хотя бы на одной БК для {check_uniq_url}"
                        )
                        return return_status
                    if bookmaker_presence_state == "error":
                        print(
                            "   ⏳ Пропуск STAR-сигнала: bookmaker initial presence error "
                            f"для {check_uniq_url}"
                        )
                        return return_status
                    bookmaker_gate_block, bookmaker_gate_ready, _bookmaker_gate_reason = _bookmaker_format_odds_block(check_uniq_url)
                    if bookmaker_gate_ready and bookmaker_gate_block:
                        odds_block = bookmaker_gate_block
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
            player_denylist_block = _player_denylist_block_payload(
                target_side=dispatch_message_side,
                skipped_player_hits=skipped_player_hits,
                radiant_team_name=radiant_team_name_original,
                dire_team_name=dire_team_name_original,
                radiant_account_ids=radiant_account_ids,
                dire_account_ids=dire_account_ids,
            )
            if player_denylist_block:
                skipped_target_player_hits = list(player_denylist_block.get("blocked_player_account_ids") or [])
                skipped_team_name = str(player_denylist_block.get("target_team") or "")
                print(
                    "   🚫 Ставка отклонена: target side содержит игрока из player denylist "
                    f"(target_side={dispatch_message_side}, team={skipped_team_name}, "
                    f"hits={skipped_target_player_hits})"
                )
                add_url(
                    check_uniq_url,
                    reason="skip_player_denylist",
                    details={
                        "status": status,
                        "radiant_team": radiant_team_name_original,
                        "dire_team": dire_team_name_original,
                        **player_denylist_block,
                        "json_retry_errors": json_retry_errors,
                    },
                )
                return return_status
            stake_team_name = (
                (radiant_team_name_original or radiant_team_name)
                if dispatch_message_side == "radiant"
                else (dire_team_name_original or dire_team_name)
                if dispatch_message_side == "dire"
                else "НЕИЗВЕСТНАЯ КОМАНДА"
            )
            force_half_due_to_early_no_valid_late = bool(
                not has_selected_late_star
                and (
                    send_now_early_star_late_core_same_sign
                    or early65_gate_active
                )
            )
            tier1_early_kills_mode = bool(
                star_match_tier == 1
                and not has_selected_late_star
                and early_wr_pct is not None
                and float(early_wr_pct) >= 70.0
                and (
                    send_now_early_star_late_core_same_sign
                    or early65_gate_active
                )
            )
            stake_multiplier_context = _build_stake_multiplier_context(
                stake_team_name=stake_team_name,
                target_side=dispatch_message_side,
                team_elo_meta=team_elo_meta,
                radiant_team_name=radiant_team_name_original or radiant_team_name,
                dire_team_name=dire_team_name_original or dire_team_name,
                selected_early_sign=selected_early_sign,
                selected_late_sign=selected_late_sign,
                has_selected_early_star=has_selected_early_star,
                has_selected_late_star=has_selected_late_star,
                early_wr_pct=early_wr_pct,
                late_wr_pct=late_wr_pct,
                late_star_hit_count=(
                    len(selected_late_diag.get("hit_metrics") or [])
                    if isinstance(selected_late_diag, dict) and has_selected_late_star
                    else None
                ),
                force_half_due_to_early_no_valid_late=force_half_due_to_early_no_valid_late,
                special_header_mode=("early_kills" if tier1_early_kills_mode else ""),
            )
            stake_multiplier = _stake_multiplier_for_signal(
                team_elo_meta=team_elo_meta,
                target_side=dispatch_message_side,
                selected_early_sign=selected_early_sign,
                selected_late_sign=selected_late_sign,
                has_selected_early_star=has_selected_early_star,
                has_selected_late_star=has_selected_late_star,
                early_wr_pct=early_wr_pct,
                late_wr_pct=late_wr_pct,
                game_time_seconds=game_time,
                radiant_lead=lead,
                late_star_hit_count=(
                    len(selected_late_diag.get("hit_metrics") or [])
                    if isinstance(selected_late_diag, dict) and has_selected_late_star
                    else None
                ),
                target_elo_wr=_team_elo_wr_for_side(team_elo_meta, dispatch_message_side),
                force_half_due_to_early_no_valid_late=force_half_due_to_early_no_valid_late,
            )
            live_state_block = _format_live_message_state_block(
                game_time_seconds=game_time,
                radiant_lead=lead,
                radiant_team_name=radiant_team_name_original or radiant_team_name,
                dire_team_name=dire_team_name_original or dire_team_name,
            )
            lane_block = _build_lane_block(
                s.get('top'),
                s.get('mid'),
                s.get('bot'),
            )

            # Формирование сообщения
            message_text = (
                f"{_format_signal_header(stake_team_name=stake_team_name, stake_multiplier=stake_multiplier, special_header_mode=('early_kills' if tier1_early_kills_mode else ''))}\n"
                f"{radiant_team_name} VS {dire_team_name}\n"
                f"{series_score_line}"
                f"{lane_block}"
                f"{problem_block}"
                f"{team_elo_block}"
                f"{wr_block}"
                f"{telegram_early_block}"
                f"{mid_block}"
                f"{dota2protracker_block}"
                f"{live_state_block}"
                f"{odds_block}"
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
            late_pub_comeback_table_wr_level = _late_star_pub_table_wr_level(late_wr_pct)
            late_pub_comeback_table_candidate = bool(
                has_selected_late_star
                and target_side in {"radiant", "dire"}
                and late_pub_comeback_table_wr_level is not None
                and isinstance(late_pub_comeback_table_thresholds_by_wr, dict)
                and bool(late_pub_comeback_table_thresholds_by_wr)
            )
            late_comeback_monitor_candidate = False
            networth_send_status_label: Optional[str] = None
            queue_early_core_monitor = False
            queue_late_core_monitor = False
            queue_strong_same_sign_monitor = False
            queue_top25_late_elo_block_monitor = bool(top25_late_elo_block_override_active)
            early65_release_status_label: Optional[str] = None
            early_release_dispatch_mode = "immediate_early_star65"
            early_release_delay_reason = "early65_gate"
            early_star_gate_wr_pct = (
                float(early_wr_pct)
                if early_wr_pct is not None
                else float(selected_star_wr or 0.0)
            )
            early_core_monitor_threshold = float(NETWORTH_GATE_EARLY_CORE_HIGH_CONFIDENCE_MIN_LEAD)
            early_core_monitor_wait_status_label = NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_NONNEGATIVE
            early_core_monitor_delay_reason = "early_star_late_core_wait_nonnegative"
            if not force_odds_signal_test_active:
                if tier1_early_kills_mode:
                    if target_networth_diff is None or target_side is None:
                        print(
                            "   ⏳ Ожидание dispatch: target-side networth gate не применен "
                            "(нет target_sign/lead)"
                        )
                        return return_status
                    if current_game_time < NETWORTH_GATE_HARD_BLOCK_SECONDS:
                        print(
                            "   ⏳ Ожидание dispatch: tier1_early_kills "
                            f"(gate={NETWORTH_STATUS_PRE4_BLOCK}, "
                            f"now={_format_game_clock(current_game_time)}, "
                            f"target_side={target_side}, target_diff={int(target_networth_diff)})"
                        )
                        return return_status
                    if current_game_time < NETWORTH_GATE_TIER1_EARLY_KILLS_WINDOW_END_SECONDS:
                        if target_networth_diff >= NETWORTH_GATE_TIER1_EARLY_KILLS_4_TO_12_MIN_DIFF:
                            early65_release_status_label = NETWORTH_STATUS_TIER1_EARLY_KILLS_4_12_SEND_500
                            early_release_dispatch_mode = "immediate_tier1_early_kills"
                            early_release_delay_reason = "tier1_early_kills"
                        else:
                            print(
                                "   ⏳ Ожидание dispatch: tier1_early_kills_04_12 "
                                f"(target_side={target_side}, "
                                f"target_diff={int(target_networth_diff)}, "
                                f"need>={int(NETWORTH_GATE_TIER1_EARLY_KILLS_4_TO_12_MIN_DIFF)})"
                            )
                            return return_status
                    else:
                        add_url(
                            check_uniq_url,
                            reason="star_signal_rejected_no_late_star",
                            details={
                                "status": status,
                                "dispatch_mode": "tier1_early_kills_window_closed",
                                "dispatch_status_label": NETWORTH_STATUS_TIER1_EARLY_KILLS_WINDOW_CLOSED,
                                "game_time": int(current_game_time),
                                "target_side": target_side,
                                "target_networth_diff": float(target_networth_diff or 0.0),
                                "selected_early_star": has_selected_early_star,
                                "selected_late_star": has_selected_late_star,
                                "selected_early_diag": selected_early_diag,
                                "selected_late_diag": selected_late_diag,
                                "early_star_no_late_same_sign_gate": early_star_no_late_same_sign_gate,
                                "json_retry_errors": json_retry_errors,
                            },
                        )
                        print("   ⚠️ ВЕРДИКТ: ОТКАЗ (tier1 early kills окно 4-12 закрыто)")
                        return return_status
                if (
                    early65_gate_active
                    and early65_target_side is not None
                    and early65_target_diff is not None
                    and early65_release_status_label is None
                ):
                    if current_game_time < NETWORTH_GATE_HARD_BLOCK_SECONDS:
                        print(
                            "   ⏳ Ожидание dispatch: immediate_early_star65 "
                            f"(reason={_dispatch_mode_reason_label('immediate_early_star65')}, "
                            f"gate=pre4_block_early65, "
                            f"now={_format_game_clock(current_game_time)}, "
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
                        if early65_target_diff >= NETWORTH_GATE_TIER1_EARLY65_10_TO_17_MIN_DIFF:
                            early65_release_status_label = NETWORTH_STATUS_TIER1_EARLY65_10_17_SEND_600
                        else:
                            print(
                                "   ⏳ Ожидание dispatch: early65_gate_10_17 "
                                f"(target_side={early65_target_side}, "
                                f"target_diff={int(early65_target_diff)}, "
                                f"need>={int(NETWORTH_GATE_TIER1_EARLY65_10_TO_17_MIN_DIFF)})"
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
                        delivery_message_text = _refresh_stake_multiplier_message(
                            message_text,
                            stake_multiplier_context=stake_multiplier_context,
                            game_time_seconds=current_game_time,
                            radiant_lead=lead,
                        )
                        delivery_confirmed = _deliver_and_persist_signal(
                            check_uniq_url,
                            delivery_message_text,
                            add_url_reason="star_signal_sent_now_networth_gate",
                            add_url_details={
                                "status": status,
                                "dispatch_mode": early_release_dispatch_mode,
                                "delay_reason": early_release_delay_reason,
                                "release_reason": early65_release_status_label,
                                "dispatch_status_label": early65_release_status_label,
                                "game_time": int(current_game_time),
                                "target_side": target_side if tier1_early_kills_mode else early65_target_side,
                                "target_networth_diff": float(
                                    target_networth_diff if tier1_early_kills_mode else (early65_target_diff or 0.0)
                                ),
                                "selected_star_wr": selected_star_wr,
                                "selected_star_mode": selected_star_mode,
                                "json_retry_errors": json_retry_errors,
                            },
                            bookmaker_decision="sent",
                        )
                        if delivery_confirmed:
                            print(
                                "   ✅ ВЕРДИКТ: Сигнал отправлен "
                                f"(reason={early65_release_status_label}, "
                                f"target_side={target_side if tier1_early_kills_mode else early65_target_side}, "
                                f"target_diff={int(target_networth_diff if tier1_early_kills_mode else (early65_target_diff or 0))})"
                            )
                    finally:
                        _release_signal_send_slot(check_uniq_url)
                    return return_status
                if current_game_time < NETWORTH_GATE_HARD_BLOCK_SECONDS:
                    print(
                        f"   ⏳ Ожидание dispatch: {dispatch_mode} "
                        f"(reason={_dispatch_mode_reason_label(dispatch_mode)}, "
                        f"gate={NETWORTH_STATUS_PRE4_BLOCK}, "
                        f"now={_format_game_clock(current_game_time)}, "
                        f"target_side={target_side}, target_diff={int(target_networth_diff)})"
                    )
                    return return_status
                if current_game_time < NETWORTH_GATE_EARLY_WINDOW_END_SECONDS:
                    if opposite_signs_selected:
                        print(
                            "   ⏳ Opposite-sign dispatch is disabled before "
                            f"{_format_game_clock(LATE_PUB_COMEBACK_TABLE_START_SECONDS)} "
                            f"(target_side={target_side}, target_diff={int(target_networth_diff)})"
                        )
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
                        if target_networth_diff < NETWORTH_GATE_EARLY_CORE_HIGH_CONFIDENCE_MIN_LEAD:
                            queue_early_core_monitor = True
                            early_core_monitor_threshold = float(NETWORTH_GATE_EARLY_CORE_HIGH_CONFIDENCE_MIN_LEAD)
                            early_core_monitor_wait_status_label = NETWORTH_STATUS_EARLY_CORE_MONITOR_WAIT_NONNEGATIVE
                            early_core_monitor_delay_reason = "early_star_late_core_wait_nonnegative"
                            print(
                                "   ⏳ Ожидание dispatch: networth_gate_10plus_target_nonnegative "
                                f"(early_wr={early_star_gate_wr_pct:.1f}%, "
                                f"target_side={target_side}, target_diff={int(target_networth_diff)}, "
                                f"need>={int(NETWORTH_GATE_EARLY_CORE_HIGH_CONFIDENCE_MIN_LEAD)}) -> delayed monitor "
                                f">={int(NETWORTH_GATE_EARLY_CORE_HIGH_CONFIDENCE_MIN_LEAD)} until "
                                f"{_format_game_clock(DELAYED_SIGNAL_TARGET_GAME_TIME)}"
                            )
                        else:
                            networth_send_status_label = NETWORTH_STATUS_MIN10_TARGET_NONNEGATIVE_SEND
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
                            f"{_format_game_clock(LATE_PUB_COMEBACK_TABLE_START_SECONDS)} then pub late comeback table"
                        )
                    else:
                        networth_send_status_label = NETWORTH_STATUS_MIN10_LOSS_LE800_SEND
                elif opposite_signs_selected:
                    print(
                        "   ⏳ Opposite-sign dispatch is disabled before "
                        f"{_format_game_clock(LATE_PUB_COMEBACK_TABLE_START_SECONDS)} "
                        f"(target_side={target_side}, target_diff={int(target_networth_diff)})"
                    )
            if (
                (not send_now_immediate and networth_send_status_label is None)
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
                elif opposite_signs_selected:
                    delay_reason = "late_only_opposite_signs"
                elif not has_selected_early_star:
                    delay_reason = "late_only_no_early_star_wait_2000"
                if verbose_match_log:
                    _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
                _ensure_delayed_sender_started()
                target_game_time = float(DELAYED_SIGNAL_TARGET_GAME_TIME)
                if late_pub_comeback_table_candidate or opposite_signs_selected:
                    target_game_time = float(LATE_PUB_COMEBACK_TABLE_START_SECONDS)
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
                            "then send only if pub late comeback table allows it"
                        )
                    else:
                        print(
                            "   ⏳ Top25 late ELO-block monitor: "
                            f"target_side={target_side}, "
                            f"target_diff={int(target_networth_diff or 0)}, "
                            f"wait until {_format_game_clock(NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_WINDOW_START_SECONDS)}, "
                            f"then need>={int(NETWORTH_GATE_LATE_TOP25_ELO_BLOCK_DIFF)} until {target_human}, "
                            "then send only if pub late comeback table allows it"
                        )
                elif not has_selected_early_star and has_selected_late_star:
                    monitor_threshold = NETWORTH_GATE_LATE_NO_EARLY_DIFF
                    monitor_wait_status_label = NETWORTH_STATUS_LATE_MONITOR_WAIT_2000
                elif opposite_signs_selected:
                    early_wr_label = (
                        f"{early_wr_pct:.1f}%"
                        if early_wr_pct is not None
                        else "n/a"
                    )
                    print(
                        "   ⏳ Opposite-sign branch queued without early release: "
                        f"early_wr={early_wr_label}, target_side={target_side}, "
                        f"target_diff={int(target_networth_diff)}, wait until {target_human}"
                    )
                fallback_max_deficit_abs = _fallback_max_deficit_abs_for_delay_reason(
                    delay_reason,
                    monitor_threshold=monitor_threshold,
                )
                if late_pub_comeback_table_candidate:
                    fallback_max_deficit_abs = None
                opposite_signs_dispatch_blocked = bool(
                    opposite_signs_selected
                    and current_game_time < float(LATE_PUB_COMEBACK_TABLE_START_SECONDS)
                )
                release_4_10_now = bool(
                    not opposite_signs_dispatch_blocked
                    and
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
                    not opposite_signs_dispatch_blocked
                    and
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
                            delivery_message_text = _refresh_stake_multiplier_message(
                                message_text,
                                stake_multiplier_context=stake_multiplier_context,
                                game_time_seconds=current_game_time,
                                radiant_lead=lead,
                            )
                            delivery_confirmed = _deliver_and_persist_signal(
                                check_uniq_url,
                                delivery_message_text,
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
                        delivery_message_text = _refresh_stake_multiplier_message(
                            message_text,
                            stake_multiplier_context=stake_multiplier_context,
                            game_time_seconds=current_game_time,
                            radiant_lead=lead,
                        )
                        delivery_confirmed = _deliver_and_persist_signal(
                            check_uniq_url,
                            delivery_message_text,
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
                    post_target_only_early90 = bool(
                        isinstance(dynamic_monitor_profile, dict)
                        and dynamic_monitor_profile.get("profile") == "late_only_opposite_signs_early90"
                    )
                    if late_pub_comeback_table_candidate:
                        late_pub_table_decision = _late_star_pub_table_decision(
                            wr_level=late_pub_comeback_table_wr_level,
                            game_time_seconds=current_game_time,
                            target_networth_diff=target_networth_diff,
                        )
                        if late_pub_table_decision.get("ready"):
                            threshold_label = late_pub_table_decision.get("threshold")
                            source_minute = late_pub_table_decision.get("source_minute")
                            print(
                                "   ✅ Pub late comeback table reached "
                                f"(wr={late_pub_comeback_table_wr_level}, "
                                f"minute={source_minute}, "
                                f"threshold={int(threshold_label or 0)}, "
                                f"target_diff={int(target_networth_diff or 0)})"
                            )
                            if _skip_dispatch_for_processed_url(check_uniq_url, f"немедленной отправки (late pub table {target_human})"):
                                return return_status
                            if not _acquire_signal_send_slot(check_uniq_url):
                                print(f"   ⚠️ Пропуск: dispatch уже выполняется для {check_uniq_url}")
                                return return_status
                            try:
                                if _skip_dispatch_for_processed_url(check_uniq_url, f"немедленной отправки после lock (late pub table {target_human})"):
                                    return return_status
                                if verbose_match_log:
                                    _print_star_metrics_snapshot(star_metrics_snapshot, label="delayed")
                                delivery_message_text = _refresh_stake_multiplier_message(
                                    message_text,
                                    stake_multiplier_context=stake_multiplier_context,
                                    game_time_seconds=current_game_time,
                                    radiant_lead=lead,
                                )
                                delivery_confirmed = _deliver_and_persist_signal(
                                    check_uniq_url,
                                    delivery_message_text,
                                    add_url_reason="star_signal_sent_now_late_pub_comeback_table",
                                    add_url_details={
                                        "status": status,
                                        "dispatch_mode": dispatch_mode,
                                        "delay_reason": delay_reason,
                                        "dispatch_status_label": NETWORTH_STATUS_LATE_PUB_TABLE_SEND,
                                        "game_time": int(current_game_time),
                                        "target_game_time": int(target_game_time),
                                        "target_side": target_side,
                                        "target_networth_diff": float(target_networth_diff or 0.0),
                                        "late_pub_comeback_table_wr_level": int(late_pub_comeback_table_wr_level or 0),
                                        "late_pub_comeback_table_minute": source_minute,
                                        "late_pub_comeback_table_threshold": float(threshold_label or 0.0),
                                        "json_retry_errors": json_retry_errors,
                                    },
                                    bookmaker_decision="sent",
                                )
                                if delivery_confirmed:
                                    print(
                                        "   ✅ ВЕРДИКТ: Сигнал отправлен по pub late comeback table "
                                        f"(wr={late_pub_comeback_table_wr_level}, "
                                        f"minute={source_minute}, "
                                        f"target_diff={int(target_networth_diff or 0)})"
                                    )
                            finally:
                                _release_signal_send_slot(check_uniq_url)
                            return return_status
                        print(
                            "   ⏳ Pub late comeback table wait: "
                            f"wr={late_pub_comeback_table_wr_level}, "
                            f"minute={late_pub_table_decision.get('source_minute')}, "
                            f"threshold={int(late_pub_table_decision.get('threshold') or 0)}, "
                            f"target_diff={int(target_networth_diff or 0)}"
                        )
                        delayed_add_url_details = {
                            "status": status,
                            "dispatch_mode": dispatch_mode,
                            "delay_reason": "late_star_pub_comeback_table_monitor",
                            "dispatch_status_label": NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
                            "queued_game_time": int(current_game_time),
                            "target_game_time": int(target_game_time),
                            "json_retry_errors": json_retry_errors,
                            "target_side": target_side,
                            "networth_target_side": target_side,
                            "target_networth_diff": float(target_networth_diff or 0.0),
                            "late_pub_comeback_table_wr_level": int(late_pub_comeback_table_wr_level or 0),
                            "late_pub_comeback_table_minute": late_pub_table_decision.get("source_minute"),
                            "late_pub_comeback_table_threshold": float(late_pub_table_decision.get("threshold") or 0.0),
                        }
                        delayed_payload = {
                            "message": message_text,
                            "stake_multiplier_context": stake_multiplier_context,
                            "reason": "late_star_pub_comeback_table_monitor",
                            "star_metrics_snapshot": star_metrics_snapshot,
                            "json_url": json_url,
                            "target_game_time": float(target_game_time),
                            "queued_at": time.time(),
                            "queued_game_time": current_game_time,
                            "last_game_time": current_game_time,
                            "last_progress_at": time.time(),
                            "dispatch_status_label": NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
                            "add_url_reason": "star_signal_sent_delayed",
                            "add_url_details": delayed_add_url_details,
                            "fallback_send_status_label": NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
                            "send_on_target_game_time": False,
                            "allow_live_recheck": False,
                            "retry_attempt_count": 0,
                            "next_retry_at": 0.0,
                            "late_pub_comeback_table_active": True,
                            "late_pub_comeback_table_wr_level": int(late_pub_comeback_table_wr_level or 0),
                            "networth_target_side": target_side,
                            "timeout_add_url_reason": "star_signal_rejected_late_pub_comeback_table_timeout",
                            "timeout_status_label": NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND,
                            "player_denylist_block": _player_denylist_block_payload(
                                target_side=target_side,
                                skipped_player_hits=skipped_player_hits,
                                radiant_team_name=radiant_team_name_original,
                                dire_team_name=dire_team_name_original,
                                radiant_account_ids=radiant_account_ids,
                                dire_account_ids=dire_account_ids,
                            ),
                        }
                        _set_delayed_match(check_uniq_url, delayed_payload)
                        print("   ✅ ВЕРДИКТ: Сигнал оставлен в delayed-очереди для pub late comeback table")
                        return return_status
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
                                delivery_message_text = _refresh_stake_multiplier_message(
                                    message_text,
                                    stake_multiplier_context=stake_multiplier_context,
                                    game_time_seconds=current_game_time,
                                    radiant_lead=lead,
                                )
                                delivery_confirmed = _deliver_and_persist_signal(
                                    check_uniq_url,
                                    delivery_message_text,
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
                                delivery_message_text = _refresh_stake_multiplier_message(
                                    message_text,
                                    stake_multiplier_context=stake_multiplier_context,
                                    game_time_seconds=current_game_time,
                                    radiant_lead=lead,
                                )
                                delivery_confirmed = _deliver_and_persist_signal(
                                    check_uniq_url,
                                    delivery_message_text,
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
                            delayed_payload = {
                                "message": message_text,
                                "stake_multiplier_context": stake_multiplier_context,
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
                            _set_delayed_match(check_uniq_url, delayed_payload)
                            print(
                                "   ⏳ Late comeback monitor включен: "
                                f"target_side={target_side}, "
                                f"target_diff={int(target_networth_diff)}, "
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
                                "stake_multiplier_context": stake_multiplier_context,
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
                    if post_target_only_early90:
                        print(
                            f"   ⚠️ ВЕРДИКТ: ОТКАЗ (opposite-sign WR90 не дал post-target comeback после {target_human}) "
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
                    delayed_add_url_details["top25_late_elo_block_raw_wr"] = top25_late_elo_block_override.get("elo_target_wr")
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
                        delayed_add_url_details["top25_late_elo_block_raw_wr"] = dynamic_monitor_profile.get("elo_target_wr")
                    elif dynamic_monitor_profile.get("profile") == "late_only_opposite_signs_early90":
                        delayed_add_url_details["target_game_time"] = int(
                            float(dynamic_monitor_profile.get("target_game_time") or target_game_time)
                        )
                        delayed_add_url_details["post_target_comeback_only"] = True
                        delayed_add_url_details["dispatch_status_label"] = str(
                            dynamic_monitor_profile.get("dispatch_status_label")
                            or NETWORTH_STATUS_LATE_OPPOSITE_EARLY90_WAIT_20_20
                        )
                        delayed_add_url_details["opposite_signs_early90_elo_gap_pp"] = dynamic_monitor_profile.get("elo_gap_pp")
                        delayed_add_url_details["opposite_signs_early90_early_elo_wr"] = dynamic_monitor_profile.get("early_elo_wr")
                        delayed_add_url_details["opposite_signs_early90_late_elo_wr"] = dynamic_monitor_profile.get("late_elo_wr")
                    else:
                        delayed_add_url_details["networth_monitor_threshold_4_to_10"] = float(dynamic_monitor_profile.get("threshold_4_to_10") or 0.0)
                        delayed_add_url_details["networth_monitor_threshold_10_to_20"] = float(dynamic_monitor_profile.get("threshold_10_to_20") or 0.0)
                        delayed_add_url_details["networth_monitor_status_4_to_10"] = str(dynamic_monitor_profile.get("status_4_to_10") or "")
                        delayed_add_url_details["networth_monitor_status_10_to_20"] = str(dynamic_monitor_profile.get("status_10_to_20") or "")
                        delayed_add_url_details["opposite_signs_early90_elo_gap_pp"] = dynamic_monitor_profile.get("elo_gap_pp")
                        delayed_add_url_details["opposite_signs_early90_early_elo_wr"] = dynamic_monitor_profile.get("early_elo_wr")
                        delayed_add_url_details["opposite_signs_early90_late_elo_wr"] = dynamic_monitor_profile.get("late_elo_wr")
                if late_pub_comeback_table_candidate:
                    late_pub_table_decision = _late_star_pub_table_decision(
                        wr_level=late_pub_comeback_table_wr_level,
                        game_time_seconds=current_game_time,
                        target_networth_diff=target_networth_diff,
                    )
                    delayed_add_url_details["dispatch_status_label"] = NETWORTH_STATUS_LATE_PUB_TABLE_WAIT
                    delayed_add_url_details["delay_reason"] = "late_star_pub_comeback_table_monitor"
                    delayed_add_url_details["target_side"] = target_side
                    delayed_add_url_details["networth_target_side"] = target_side
                    delayed_add_url_details["late_pub_comeback_table_wr_level"] = int(late_pub_comeback_table_wr_level or 0)
                    if target_networth_diff is not None:
                        delayed_add_url_details["target_networth_diff"] = float(target_networth_diff)
                    if late_pub_table_decision.get("source_minute") is not None:
                        delayed_add_url_details["late_pub_comeback_table_minute"] = int(late_pub_table_decision.get("source_minute") or 0)
                    if late_pub_table_decision.get("threshold") is not None:
                        delayed_add_url_details["late_pub_comeback_table_threshold"] = float(late_pub_table_decision.get("threshold") or 0.0)
                delayed_payload = {
                    'message': message_text,
                    'stake_multiplier_context': stake_multiplier_context,
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
                        or late_pub_comeback_table_candidate
                        or (
                            isinstance(dynamic_monitor_profile, dict)
                            and dynamic_monitor_profile.get("profile") in {
                                "late_only_opposite_signs_early90",
                                "late_only_opposite_signs_early90_tier1_fast_release",
                            }
                        )
                    ),
                    'allow_live_recheck': allow_live_recheck,
                    'retry_attempt_count': 0,
                    'next_retry_at': 0.0,
                    'late_comeback_monitor_candidate': late_comeback_monitor_candidate,
                    'player_denylist_block': _player_denylist_block_payload(
                        target_side=target_side,
                        skipped_player_hits=skipped_player_hits,
                        radiant_team_name=radiant_team_name_original,
                        dire_team_name=dire_team_name_original,
                        radiant_account_ids=radiant_account_ids,
                        dire_account_ids=dire_account_ids,
                    ),
                }
                if queue_top25_late_elo_block_monitor:
                    delayed_payload['networth_target_side'] = target_side
                    delayed_payload['top25_late_elo_block_rank'] = int(top25_late_elo_block_override.get("leaderboard_rank") or 0)
                    delayed_payload['top25_late_elo_block_raw_wr'] = top25_late_elo_block_override.get("elo_target_wr")
                if isinstance(dynamic_monitor_profile, dict) and dynamic_monitor_profile.get("enabled"):
                    delayed_payload['dynamic_monitor_profile'] = str(dynamic_monitor_profile.get("profile") or "")
                    if dynamic_monitor_profile.get("profile") == "late_top25_elo_block_opposite_monitor":
                        delayed_payload['networth_monitor_threshold_17_to_20'] = float(dynamic_monitor_profile.get("window_threshold") or 0.0)
                        delayed_payload['networth_monitor_status_17_to_20'] = NETWORTH_STATUS_LATE_TOP25_ELO_BLOCK_WAIT
                        delayed_payload['networth_monitor_window_start_game_time'] = float(dynamic_monitor_profile.get("window_start_seconds") or 0.0)
                        delayed_payload['top25_late_elo_block_rank'] = int(dynamic_monitor_profile.get("leaderboard_rank") or 0)
                        delayed_payload['top25_late_elo_block_raw_wr'] = dynamic_monitor_profile.get("elo_target_wr")
                    elif dynamic_monitor_profile.get("profile") == "late_only_opposite_signs_early90":
                        delayed_payload['target_game_time'] = float(
                            dynamic_monitor_profile.get("target_game_time") or target_game_time
                        )
                        delayed_payload['dispatch_status_label'] = str(
                            dynamic_monitor_profile.get("dispatch_status_label")
                            or NETWORTH_STATUS_LATE_OPPOSITE_EARLY90_WAIT_20_20
                        )
                        delayed_payload['timeout_add_url_reason'] = "star_signal_rejected_late_comeback_monitor_timeout"
                        delayed_payload['timeout_status_label'] = NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND
                        delayed_payload['opposite_signs_early90_elo_gap_pp'] = dynamic_monitor_profile.get("elo_gap_pp")
                        delayed_payload['opposite_signs_early90_early_elo_wr'] = dynamic_monitor_profile.get("early_elo_wr")
                        delayed_payload['opposite_signs_early90_late_elo_wr'] = dynamic_monitor_profile.get("late_elo_wr")
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
                if late_pub_comeback_table_candidate:
                    delayed_payload['reason'] = "late_star_pub_comeback_table_monitor"
                    delayed_payload['dispatch_status_label'] = NETWORTH_STATUS_LATE_PUB_TABLE_WAIT
                    delayed_payload['late_pub_comeback_table_active'] = True
                    delayed_payload['late_pub_comeback_table_wr_level'] = int(late_pub_comeback_table_wr_level or 0)
                    delayed_payload['networth_target_side'] = target_side
                    delayed_payload['timeout_add_url_reason'] = 'star_signal_rejected_late_pub_comeback_table_timeout'
                    delayed_payload['timeout_status_label'] = NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND
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
                delivery_message_text = _refresh_stake_multiplier_message(
                    message_text,
                    stake_multiplier_context=stake_multiplier_context,
                    game_time_seconds=current_game_time,
                    radiant_lead=lead,
                )
                delivery_message_text = _refresh_message_bookmaker_block_for_dispatch(
                    check_uniq_url,
                    delivery_message_text,
                )
                delivery_confirmed = _deliver_and_persist_signal(
                    check_uniq_url,
                    delivery_message_text,
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
            if tempo_over_fallback is None and int(star_match_tier or 0) == 1 and isinstance(tempo_over_diag, dict):
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
                        f"(raw_wr={float(tempo_team_elo_meta['raw_radiant_wr']):.1f}%/{float(tempo_team_elo_meta['raw_dire_wr']):.1f}%)"
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
    global lane_data, early_dict, late_dict, post_lane_dict
    global late_comeback_ceiling_data, late_comeback_ceiling_thresholds, late_comeback_ceiling_max_minute
    global late_pub_comeback_table_data, late_pub_comeback_table_thresholds_by_wr
    global late_pub_comeback_table_max_minute_by_wr, late_pub_comeback_table_global_max_minute
    global stats_warmup_last_heavy_load_ts
    if (
        early_dict is not None
        and late_dict is not None
        and post_lane_dict is not None
        and late_pub_comeback_table_data is not None
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
        nonlocal late_pub_comeback_table_path
        global lane_data
        global late_comeback_ceiling_data, late_comeback_ceiling_thresholds, late_comeback_ceiling_max_minute
        global late_pub_comeback_table_data, late_pub_comeback_table_thresholds_by_wr
        global late_pub_comeback_table_max_minute_by_wr, late_pub_comeback_table_global_max_minute

        if not LIVE_LANE_ANALYSIS_ENABLED:
            lane_data = None
        elif lane_data is None:
            print(f"📦 Loading lane stats: {lane_path}")
            lane_data = _load_json_object(lane_path, "lane_dict_raw")
            gc.collect()

        if late_pub_comeback_table_data is None:
            late_pub_comeback_table_data = {}
            late_pub_comeback_table_thresholds_by_wr = {}
            late_pub_comeback_table_max_minute_by_wr = {}
            late_pub_comeback_table_global_max_minute = None
            if Path(late_pub_comeback_table_path).exists():
                try:
                    with open(late_pub_comeback_table_path, "r", encoding="utf-8") as f:
                        late_pub_comeback_table_data = json.load(f)
                    rows = (late_pub_comeback_table_data or {}).get("table_rows") or []
                    thresholds_by_wr: Dict[int, Dict[int, float]] = {}
                    max_minute_by_wr: Dict[int, int] = {}
                    global_max_minute: Optional[int] = None
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        try:
                            wr_level = int(row.get("wr_level"))
                            minute = int(row.get("minute"))
                            threshold = float(row.get("avg_target_networth_diff"))
                        except (TypeError, ValueError):
                            continue
                        thresholds_by_wr.setdefault(wr_level, {})[minute] = threshold
                        prev_max = max_minute_by_wr.get(wr_level)
                        if prev_max is None or minute > prev_max:
                            max_minute_by_wr[wr_level] = minute
                        if global_max_minute is None or minute > global_max_minute:
                            global_max_minute = minute
                    late_pub_comeback_table_thresholds_by_wr = thresholds_by_wr
                    late_pub_comeback_table_max_minute_by_wr = max_minute_by_wr
                    late_pub_comeback_table_global_max_minute = global_max_minute
                except Exception:
                    late_pub_comeback_table_data = {}
                    late_pub_comeback_table_thresholds_by_wr = {}
                    late_pub_comeback_table_max_minute_by_wr = {}
                    late_pub_comeback_table_global_max_minute = None
                    _report_missing_runtime_file(
                        "pub_late_star_comeback_table_piecewise.json",
                        Path(late_pub_comeback_table_path),
                        details="failed to parse pub late comeback table",
                    )
            else:
                _report_missing_runtime_file(
                    "pub_late_star_comeback_table_piecewise.json",
                    Path(late_pub_comeback_table_path),
                )

    default_stats_dir = str(ANALYSE_PUB_DIR)
    stats_dir = os.getenv("STATS_DIR", default_stats_dir)
    lane_path = os.getenv("STATS_LANE_PATH", f"{stats_dir}/lane_dict_raw.json")
    early_path = os.getenv("STATS_EARLY_PATH", f"{stats_dir}/early_dict_raw.json")
    late_path = os.getenv("STATS_LATE_PATH", f"{stats_dir}/late_dict_raw.json")
    post_lane_path = os.getenv("STATS_POST_LANE_PATH", f"{stats_dir}/post_lane_dict_raw.json")
    late_pub_comeback_table_path = os.getenv(
        "STATS_LATE_PUB_COMEBACK_TABLE_PATH",
        str(BASE_DIR / "pub_late_star_comeback_table_piecewise.json"),
    )

    # If lane analysis is enabled and test stats folder has no lane dict, fallback to baseline lane dict.
    if LIVE_LANE_ANALYSIS_ENABLED and not Path(lane_path).exists():
        fallback_lane = f"{default_stats_dir}/lane_dict_raw.json"
        if Path(fallback_lane).exists():
            lane_path = fallback_lane

    _load_small_supporting_dicts()

    if not STATS_SEQUENTIAL_WARMUP_ENABLED:
        if early_dict is None:
            if _stats_indexed_lookup_enabled("early"):
                early_dict = _prepare_indexed_stats_lookup(early_path, "early")
            else:
                print(f"📦 Loading early stats: {early_path}")
                early_dict = _load_json_object(early_path, "early_dict_raw")
            gc.collect()
        if late_dict is None:
            if _stats_indexed_lookup_enabled("late"):
                late_dict = _prepare_indexed_stats_lookup(late_path, "late")
            else:
                print(f"📦 Loading late stats: {late_path}")
                late_dict = _load_json_object(late_path, "late_dict_raw")
            gc.collect()
        if post_lane_dict is None:
            if Path(post_lane_path).exists():
                if _stats_indexed_lookup_enabled("post_lane"):
                    post_lane_dict = _prepare_indexed_stats_lookup(post_lane_path, "post_lane")
                else:
                    print(f"📦 Loading post-lane stats: {post_lane_path}")
                    post_lane_dict = _load_json_object(post_lane_path, "post_lane_dict_raw")
            else:
                logger.warning("Post-lane stats file not found: %s", post_lane_path)
                print(f"⚠️ Post-lane stats file not found: {post_lane_path}")
                _report_missing_runtime_file("post_lane_dict_raw.json", Path(post_lane_path))
                post_lane_dict = {}
            gc.collect()
        return (
            early_dict is not None
            and late_dict is not None
            and post_lane_dict is not None
            and late_pub_comeback_table_data is not None
        )

    if stats_warmup_last_heavy_load_ts == 0.0 and (
        early_dict is None or late_dict is None or post_lane_dict is None
    ):
        stats_warmup_last_heavy_load_ts = time.time()
        return False

    now_ts = time.time()
    remaining_heavy = []
    if early_dict is None:
        remaining_heavy.append(("early", early_path))
    if late_dict is None:
        remaining_heavy.append(("late", late_path))
    if post_lane_dict is None:
        remaining_heavy.append(("post_lane", post_lane_path))

    if not remaining_heavy:
        return True

    if stats_warmup_last_heavy_load_ts and (now_ts - stats_warmup_last_heavy_load_ts) < STATS_WARMUP_STEP_DELAY_SECONDS:
        return False

    next_label, next_path = remaining_heavy[0]
    if next_label == "post_lane" and not Path(next_path).exists():
        logger.warning("Post-lane stats file not found: %s", next_path)
        print(f"⚠️ Post-lane stats file not found: {next_path}")
        _report_missing_runtime_file("post_lane_dict_raw.json", Path(next_path))
        next_payload = {}
    elif _stats_indexed_lookup_enabled(next_label):
        next_payload = _prepare_indexed_stats_lookup(next_path, next_label)
    else:
        print(f"📦 Warmup loading {next_label} stats: {next_path}")
        next_payload = _load_json_object(next_path, f"{next_label}_dict_raw")
    if next_label == "early":
        early_dict = next_payload
    elif next_label == "late":
        late_dict = next_payload
    else:
        post_lane_dict = next_payload
    stats_warmup_last_heavy_load_ts = time.time()
    gc.collect()
    return (
        early_dict is not None
        and late_dict is not None
        and post_lane_dict is not None
        and late_pub_comeback_table_data is not None
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
    except FileNotFoundError as exc:
        missing_path = Path(getattr(exc, "filename", "") or (base_dir / "tempo_solo_dict_raw.json"))
        _report_missing_runtime_file("tempo_fallback_dict", missing_path, details=str(exc))
        logger.warning("Tempo fallback disabled: failed to load tempo dicts: %s", exc)
        return False
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


def general(return_status=None, use_proxy=None, odds=None, bookmaker_gate_mode=None):
    """
    Основной цикл проверки матчей.
    
    Args:
        return_status: статус для возврата (не используется)
        use_proxy: использовать прокси (True) или прямое подключение (False).
                   Если None — берется из USE_PROXY (если задан) или из переменной окружения USE_PROXY.
        odds: включить odds-пайплайн (True/False).
              Если None — берется из переменной окружения BOOKMAKER_PREFETCH_ENABLED (по умолчанию True).
        bookmaker_gate_mode: режим bookmaker-gate (`odds` или `presence`).
              Если None — берется из BOOKMAKER_PREFETCH_GATE_MODE.
    """
    global PROXIES, BOOKMAKER_PREFETCH_ENABLED, BOOKMAKER_PREFETCH_GATE_MODE
    global LIVE_MATCHES_MISSING_ALERT_ACTIVE
    global PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE
    global NEXT_SCHEDULE_SLEEP_SECONDS, NEXT_SCHEDULE_MATCH_INFO

    odds_arg = odds
    if odds is None:
        odds = _safe_bool_env("BOOKMAKER_PREFETCH_ENABLED", False)
    if isinstance(odds, str):
        odds_requested = odds.strip().lower() in {"1", "true", "yes", "y", "on"}
    else:
        odds_requested = bool(odds)
    BOOKMAKER_PREFETCH_ENABLED = odds_requested and BOOKMAKER_PREFETCH_AVAILABLE
    BOOKMAKER_PREFETCH_GATE_MODE = _normalize_bookmaker_gate_mode(
        bookmaker_gate_mode if bookmaker_gate_mode is not None else os.getenv("BOOKMAKER_PREFETCH_GATE_MODE", BOOKMAKER_PREFETCH_GATE_MODE),
        default="odds",
    )

    odds_source = "arg" if odds_arg is not None else "env"
    print(
        "🎲 Odds pipeline: "
        f"{'ON' if BOOKMAKER_PREFETCH_ENABLED else 'OFF'} "
        f"(source={odds_source}, requested={odds_requested}, available={BOOKMAKER_PREFETCH_AVAILABLE}, "
        f"gate_mode={BOOKMAKER_PREFETCH_GATE_MODE})"
    )
    if SIGNAL_MINIMAL_ODDS_ONLY_MODE:
        print("🎯 Signal mode: minimal_odds_only (metrics/gates disabled)")
    elif not CLASSIC_SIGNAL_PIPELINE_ENABLED:
        print("🎯 Signal mode: classic pipeline disabled")
    logger.info(
        "Odds pipeline mode: %s (source=%s, requested=%s, available=%s, gate_mode=%s)",
        "ON" if BOOKMAKER_PREFETCH_ENABLED else "OFF",
        odds_source,
        odds_requested,
        BOOKMAKER_PREFETCH_AVAILABLE,
        BOOKMAKER_PREFETCH_GATE_MODE,
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

    if DLTV_SOURCE_MODE == "cyberscore":
        quiet_sleep_seconds = _cyberscore_quiet_hours_sleep_seconds_with_probe()
        if quiet_sleep_seconds > 0:
            _stop_bookmaker_prefetch_worker()
            NEXT_SCHEDULE_SLEEP_SECONDS = float(quiet_sleep_seconds)
            wake_at_msk = datetime.now(MOSCOW_TZ) + timedelta(seconds=quiet_sleep_seconds)
            NEXT_SCHEDULE_MATCH_INFO = {
                "sleep_seconds": NEXT_SCHEDULE_SLEEP_SECONDS,
                "sleep_seconds_raw": NEXT_SCHEDULE_SLEEP_SECONDS,
                "matchup": "CyberScore quiet hours",
                "league_title": "",
                "scheduled_at_msk": wake_at_msk,
                "source": "cyberscore_quiet_hours",
            }
            print(
                "🌙 CyberScore quiet hours "
                f"({CYBERSCORE_QUIET_HOURS_START_HOUR_MSK:02d}:00-"
                f"{CYBERSCORE_QUIET_HOURS_END_HOUR_MSK:02d}:00 MSK). "
                f"Сплю до {wake_at_msk.strftime('%Y-%m-%d %H:%M:%S MSK')}"
            )
            return "__sleep_cyberscore_quiet_hours__"

    # Гарантируем staged warmup словарей только для classic signal pipeline.
    # В минимальных режимах без словарей этот шаг пропускаем полностью.
    if CLASSIC_SIGNAL_PIPELINE_ENABLED:
        stats_ready = _load_stats_dicts()
        if stats_ready is False:
            warmup_parts = []
            if lane_data is not None:
                warmup_parts.append("lane")
            if early_dict is not None:
                warmup_parts.append("early")
            if late_dict is not None:
                warmup_parts.append("late")
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
            if not LIVE_MATCHES_MISSING_ALERT_ACTIVE and not PROXY_POOL_DIRECT_FALLBACK_ALERT_ACTIVE:
                try:
                    send_message('❌ Не найден элемент live__matches в HTML', admin_only=True)
                except Exception as e:
                    print(f"⚠️ Не удалось отправить уведомление в Telegram: {e}")
                LIVE_MATCHES_MISSING_ALERT_ACTIVE = True
        else:
            LIVE_MATCHES_MISSING_ALERT_ACTIVE = False
        return None
    LIVE_MATCHES_MISSING_ALERT_ACTIVE = False

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
            if _should_use_schedule_sleep_window():
                print(
                    "🗓️ Live matches empty. "
                    f"Nearest scheduled match: {matchup} at {scheduled_label}. "
                    f"Sleep planned: {int(math.ceil(sleep_seconds))}s "
                    f"(raw until start: {int(math.ceil(raw_sleep_seconds))}s)"
                )
                return "__sleep_until_schedule__"
            print(
                "🗓️ Live matches empty, but schedule sleep is disabled before "
                f"{SCHEDULE_ONLY_IDLE_START_HOUR_MSK:02d}:00 MSK. "
                f"Nearest scheduled match: {matchup} at {scheduled_label}. "
                "Continuing normal polling."
            )
            return None
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
    parser.add_argument(
        "--bookmaker-gate-mode",
        choices=["odds", "presence"],
        default=None,
        help="Bookmaker gate mode when odds pipeline is enabled",
    )
    parser.add_argument(
        "--dltv-source",
        choices=["api", "html", "cyberscore"],
        default=DLTV_SOURCE_MODE if DLTV_SOURCE_MODE in {"api", "html", "cyberscore"} else "cyberscore",
        help="Live matches source: api/html for DLTV, or cyberscore for CyberScore Camoufox",
    )
    parser.add_argument(
        "--pure-dltv",
        dest="pure_dltv",
        action="store_true",
        help="Disable all bookmaker prefetch/presence checks, use only DLTV Selenium for draft parsing",
    )
    parser.set_defaults(odds=None, pure_dltv=False)
    args = parser.parse_args()
    DLTV_SOURCE_MODE = str(args.dltv_source).strip().lower()
    PURE_DLTV_MODE = bool(args.pure_dltv)
    _apply_live_entrypoint_pipeline_defaults()
    print(f"🌐 Live source mode: {DLTV_SOURCE_MODE}")
    print(
        "🧪 Pipeline smoke-test: "
        f"send_every={PIPELINE_SEND_EVERY_PARSED_MATCH}, "
        f"disable_gates={PIPELINE_DISABLE_SIGNAL_GATES}, "
        f"parallel_metrics={PIPELINE_METRICS_PARALLEL_ENABLED}, "
        f"laning={LIVE_LANE_ANALYSIS_ENABLED}, "
        f"dota2protracker={DOTA2PROTRACKER_ENABLED}"
    )
    if PURE_DLTV_MODE:
        print("🔇 Pure DLTV mode: all bookmaker prefetch/presence checks disabled")
    runtime_mode_label = _runtime_instance_mode_label(args.odds)
    if not _try_acquire_runtime_instance_lock(mode_label=runtime_mode_label):
        raise SystemExit(0)
    DELAYED_QUEUE_PATH = str(_delayed_queue_path_for_mode(runtime_mode_label))
    print(f"🗂️ DELAYED_QUEUE_PATH for mode={runtime_mode_label}: {DELAYED_QUEUE_PATH}")

    # Always unify to DEFAULT_MAP_ID_CHECK_PATH regardless of --odds flag
    MAP_ID_CHECK_PATH = str(DEFAULT_MAP_ID_CHECK_PATH)
    print(f"🗺️ MAP_ID_CHECK_PATH unified: {MAP_ID_CHECK_PATH}")

    # Абсолютные пути к данным (вынесены за пределы проекта для оптимизации Cursor)
    STATS_DIR = str(ANALYSE_PUB_DIR)
    while True:
        try:
            runtime_cycle_counter += 1
            cycle_number = int(runtime_cycle_counter)
            _handle_pending_telegram_admin_commands(args.odds)
            status = general(
                use_proxy=None,
                odds=args.odds,
                bookmaker_gate_mode=args.bookmaker_gate_mode,
            )
            _maybe_log_runtime_memory_snapshot(
                cycle_number=cycle_number,
                context=f"status={status}",
            )
            if status == "__sleep_cyberscore_quiet_hours__":
                quiet_sleep_seconds = max(1, int(math.ceil(float(NEXT_SCHEDULE_SLEEP_SECONDS or 0.0))))
                print(f"Сплю {quiet_sleep_seconds} секунд до окончания ночного окна CyberScore")
                _sleep_interruptible(
                    quiet_sleep_seconds,
                    raw_odds=args.odds,
                    label="cyberscore_quiet_hours",
                )
            elif status == "__sleep_until_schedule__":
                scheduled_sleep_seconds = max(1, int(math.ceil(float(NEXT_SCHEDULE_SLEEP_SECONDS or 0.0))))
                schedule_snapshot = dict(NEXT_SCHEDULE_MATCH_INFO or {})
                sleep_started_at_msk = datetime.now(MOSCOW_TZ)
                if schedule_snapshot:
                    schedule_snapshot["sleep_started_at_msk"] = sleep_started_at_msk
                    schedule_snapshot["planned_sleep_seconds"] = scheduled_sleep_seconds
                source_label = str(schedule_snapshot.get("source") or DLTV_SOURCE_MODE or "schedule")
                if source_label == "cyberscore_no_upcoming":
                    print(f"Сплю {scheduled_sleep_seconds} секунд до следующего опроса расписания CyberScore")
                else:
                    print(f"Сплю {scheduled_sleep_seconds} секунд до ближайшего матча по расписанию {source_label}")
                _sleep_interruptible(
                    scheduled_sleep_seconds,
                    raw_odds=args.odds,
                    label="sleep_until_schedule",
                )
                if schedule_snapshot:
                    schedule_snapshot["woke_at_msk"] = datetime.now(MOSCOW_TZ)
                    SCHEDULE_LIVE_WAIT_TARGET = dict(schedule_snapshot)
                    PENDING_SCHEDULE_WAKE_AUDIT = schedule_snapshot
            elif status == "__sleep_wait_for_live_after_schedule__":
                wait_for_live_seconds = max(1, int(math.ceil(float(SCHEDULE_POST_START_POLL_SECONDS))))
                print(f"Сплю {wait_for_live_seconds} секунд в режиме ожидания появления live matches")
                _sleep_interruptible(
                    wait_for_live_seconds,
                    raw_odds=args.odds,
                    label="wait_for_live_after_schedule",
                )
            elif status is None:
                print('Сплю 60 секунд')
                _sleep_interruptible(60, raw_odds=args.odds, label="default_idle")
            else:
                print('Сплю 60 секунд')
                _sleep_interruptible(60, raw_odds=args.odds, label="default_status")
        except Exception as e:
            print(f"⚠️ Ошибка главного цикла: {e}")
            logger.exception("Main loop error")
            _maybe_log_runtime_memory_snapshot(
                cycle_number=int(runtime_cycle_counter),
                context=f"exception={type(e).__name__}",
                force=True,
            )
            _sleep_interruptible(30, raw_odds=args.odds, label="error_backoff")
