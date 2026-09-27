"""Position-weighted player/hero familiarity correction for the served All head."""
import logging
import math
import os
from pathlib import Path
import threading
import time

import numpy as np


_DEFAULT_ARTIFACT = Path(__file__).resolve().parents[1] / "data/prematch_model_artifact_v3.npz"
_COEFS = (0.018089718077978715, 0.03202312233063573,
          0.013768603968361319)
_STAT_INTERVAL = 600.0
_LOCK = threading.Lock()
_LOG = logging.getLogger(__name__)
_PATH = None
_STAT_AT = 0.0
_SIGNATURE = None
_TABLE = None
_WARNED_PATHS = set()


def _artifact_path():
    return Path(os.getenv("PREMATCH_ARTIFACT", str(_DEFAULT_ARTIFACT)))


def _table():
    """Return compact sorted (pair keys, counts); stat once per ten minutes."""
    global _PATH, _STAT_AT, _SIGNATURE, _TABLE
    path = _artifact_path()
    now = time.monotonic()
    with _LOCK:
        if path == _PATH and now - _STAT_AT < _STAT_INTERVAL:
            return _TABLE
        previous_path = _PATH
        _PATH, _STAT_AT = path, now
        try:
            stat = path.stat()
            signature = (stat.st_mtime_ns, stat.st_size)
            if path == previous_path and signature == _SIGNATURE and _TABLE is not None:
                return _TABLE
            # NpzFile opens the container lazily; only acc_hero is decompressed.
            with np.load(str(path), allow_pickle=False) as artifact:
                rows = artifact["acc_hero"]
            if rows.ndim != 2 or rows.shape[1] < 3:
                raise ValueError("acc_hero must have at least three columns")
            if (np.any(rows[:, 0] <= 0)
                    or np.any(rows[:, 0] > np.iinfo(np.int64).max // 2048)
                    or np.any(rows[:, 1] <= 0)
                    or np.any(rows[:, 1] >= 2048) or np.any(rows[:, 2] < 0)
                    or np.any(rows[:, 2] > np.iinfo(np.uint32).max)):
                raise ValueError("acc_hero has invalid pair or games count")
            keys = rows[:, 0].astype(np.int64)
            if np.any(keys != rows[:, 0]):
                raise ValueError("acc_hero has noninteger accounts")
            keys *= 2048
            heroes = rows[:, 1].astype(np.int64)
            if np.any(heroes != rows[:, 1]):
                raise ValueError("acc_hero has noninteger heroes")
            keys += heroes
            del heroes
            counts = rows[:, 2].astype(np.uint32)
            if np.any(counts != rows[:, 2]):
                raise ValueError("acc_hero has noninteger games counts")
            del rows
            # The artifact builder emits sorted account/hero rows. Avoid a
            # second full-sized key copy on the normal path; sort older inputs.
            if np.any(keys[1:] <= keys[:-1]):
                order = np.argsort(keys)
                keys, counts = keys[order], counts[order]
                if np.any(keys[1:] == keys[:-1]):
                    raise ValueError("acc_hero has duplicate account/hero pairs")
            _TABLE = (keys, counts)
            _SIGNATURE = signature
            return _TABLE
        except Exception as exc:  # noqa: BLE001 — optional artifact cannot block a verdict
            _TABLE, _SIGNATURE = None, None
            if path not in _WARNED_PATHS:
                _LOG.warning("All hero-pool artifact unavailable (%s): %s", path, exc)
                _WARNED_PATHS.add(path)
            return None


def _count(table, account, hero):
    key = account * 2048 + hero
    keys, counts = table
    at = int(np.searchsorted(keys, key))
    return int(counts[at]) if at < len(keys) and keys[at] == key else 0


def correct_index(index, radiant_dict, dire_dict):
    """Shift the cached draft index in logit space; fail open on missing history."""
    if os.getenv("ALL_HERO_POOL_ENABLED", "1").lower() in ("0", "false", "off"):
        return index
    slots = []
    for position in range(1, 4):
        try:
            r = radiant_dict.get("pos%d" % position) or {}
            d = dire_dict.get("pos%d" % position) or {}
            ra, da = int(r.get("account_id") or 0), int(d.get("account_id") or 0)
            rh, dh = int(r.get("hero_id") or 0), int(d.get("hero_id") or 0)
        except (AttributeError, TypeError, ValueError):
            continue
        if min(ra, da, rh, dh) > 0 and max(rh, dh) < 2048:
            slots.append((position - 1, ra, rh, da, dh))
    if not slots:
        return index
    table = _table()
    if table is None:
        return index
    shift = sum(_COEFS[k] * (math.log1p(_count(table, ra, rh))
                             - math.log1p(_count(table, da, dh)))
                for k, ra, rh, da, dh in slots)
    p = 0.5 + index / 100.0
    if p <= 0.0 or p >= 1.0:
        return index
    logit = math.log(p / (1.0 - p)) + shift
    corrected = 1.0 / (1.0 + math.exp(-logit))
    return min(50.0, max(-50.0, (corrected - 0.5) * 100.0))
