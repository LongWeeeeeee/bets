"""Read-only, causal player-laning history for live inference.

The exported events are partitioned by role.  Each partition has one account
table and contiguous account event ranges, so serving never has to scan the
whole corpus or materialize an account/hero index.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np


_ROLE_COUNT = 5
_PRIOR = 10.0
_FILES = ("accounts", "offsets", "end_ts", "heroes", "scores")


def _finite_nonnegative(value, name):
    try:
        value = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be finite and non-negative") from exc
    if not np.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return value


def _integer_vector(value, name, *, minimum, maximum=None):
    array = np.asarray(value)
    if array.shape != (10,):
        raise ValueError(f"{name} must have shape (10,)")
    try:
        numeric = array.astype(np.float64)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must contain finite integers") from exc
    if (not np.isfinite(numeric).all() or
            not np.equal(numeric, np.floor(numeric)).all() or
            np.any(numeric < minimum) or
            (maximum is not None and np.any(numeric > maximum))):
        raise ValueError(f"{name} must contain finite integers in the supported range")
    return numeric.astype(np.int64)


class LaningHistoryStore:
    """Memory-mapped immutable history export.

    ``heroes10`` and ``accounts10`` are ordered R1..R5,D1..D5.  Scores are
    always from the queried player's side, matching ``laning_model.build_history``.
    """

    def __init__(self, directory):
        self.directory = Path(directory)
        manifest_path = self.directory / "manifest.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid laning history manifest: {manifest_path}") from exc
        if not isinstance(manifest, dict) or manifest.get("complete") is not True:
            raise ValueError("laning history export is incomplete")
        if manifest.get("schema_version") != 1:
            raise ValueError("unsupported laning history schema")
        roles = manifest.get("roles")
        if not isinstance(roles, list) or len(roles) != _ROLE_COUNT:
            raise ValueError("invalid laning history role manifest")
        expected_files = {f"role_{role}_{name}.npy" for role in range(1, _ROLE_COUNT + 1)
                          for name in _FILES}
        file_hashes = manifest.get("files_sha256")
        if (not isinstance(file_hashes, dict) or set(file_hashes) != expected_files or
                any(not isinstance(value, str) or len(value) != 64 for value in file_hashes.values())):
            raise ValueError("invalid laning history file hashes")
        self.manifest = manifest
        self._roles = [self._load_role(role + 1, roles[role]) for role in range(_ROLE_COUNT)]

    def _load_role(self, role, metadata):
        if not isinstance(metadata, dict):
            raise ValueError("invalid laning history role manifest")
        arrays = {}
        for name in _FILES:
            path = self.directory / f"role_{role}_{name}.npy"
            try:
                arrays[name] = np.load(path, mmap_mode="r", allow_pickle=False)
            except (OSError, ValueError) as exc:
                raise ValueError(f"invalid laning history file: {path}") from exc
            if arrays[name].ndim != 1:
                raise ValueError(f"invalid laning history array shape: {path.name}")
        accounts, offsets = arrays["accounts"], arrays["offsets"]
        events = len(arrays["end_ts"])
        expected_dtypes = {"accounts": np.dtype("uint64"), "offsets": np.dtype("int64"),
                           "end_ts": np.dtype("int64"), "heroes": np.dtype("uint16"),
                           "scores": np.dtype("int8")}
        if any(arrays[name].dtype != dtype for name, dtype in expected_dtypes.items()):
            raise ValueError(f"invalid laning history dtypes for role {role}")
        if (len(offsets) != len(accounts) + 1 or len(offsets) == 0 or offsets[0] != 0 or
                offsets[-1] != events or any(len(arrays[name]) != events
                                              for name in ("heroes", "scores"))):
            raise ValueError(f"invalid laning history offsets for role {role}")
        if (np.any(offsets[1:] < offsets[:-1]) or
                (len(accounts) > 1 and np.any(accounts[1:] <= accounts[:-1]))):
            raise ValueError(f"unsorted laning history accounts for role {role}")
        if metadata.get("events") != events or metadata.get("accounts") != len(accounts):
            raise ValueError(f"laning history manifest counts disagree for role {role}")
        return arrays

    @staticmethod
    def _summary(end_ts, scores, cutoff, recent_window_seconds=None):
        upper = int(np.searchsorted(end_ts, cutoff, side="left"))
        lower = 0
        if recent_window_seconds is not None:
            lower = int(np.searchsorted(end_ts, cutoff - recent_window_seconds, side="left"))
        chosen = scores[lower:upper]
        count = len(chosen)
        if not count:
            return np.zeros(3, dtype=np.float32)
        return np.asarray((chosen.sum(dtype=np.float64) / (count + _PRIOR),
                           np.sign(chosen).sum(dtype=np.float64) / (count + _PRIOR),
                           np.log1p(count)), dtype=np.float32)

    @staticmethod
    def _account_events(role, account):
        if account <= 0:
            return None
        accounts = role["accounts"]
        position = int(np.searchsorted(accounts, account, side="left"))
        if position == len(accounts) or accounts[position] != account:
            return None
        start, stop = int(role["offsets"][position]), int(role["offsets"][position + 1])
        # Validate only the queried account.  A full event scan would defeat mmap
        # startup, while an unsorted or malformed selected group must never yield
        # a plausible-but-wrong causal feature vector.
        end_ts, heroes, scores = (role[name][start:stop] for name in ("end_ts", "heroes", "scores"))
        if (np.any(end_ts[1:] < end_ts[:-1]) or np.any((heroes < 1) | (heroes > 1023)) or
                np.any((scores < -2) | (scores > 2))):
            raise ValueError("invalid laning history events for queried account")
        return slice(start, stop)

    def history(self, heroes10, accounts10, timestamp, availability_delay_seconds=3600,
                recent_window_seconds=2592000):
        """Return float32 ``(10, 12)`` all/recent role and role+hero features."""
        heroes = _integer_vector(heroes10, "heroes10", minimum=1, maximum=1023)
        accounts = _integer_vector(accounts10, "accounts10", minimum=0,
                                   maximum=np.iinfo(np.int64).max)
        try:
            timestamp = float(timestamp)
        except (TypeError, ValueError) as exc:
            raise ValueError("timestamp must be finite") from exc
        if not np.isfinite(timestamp):
            raise ValueError("timestamp must be finite")
        delay = _finite_nonnegative(availability_delay_seconds, "availability_delay_seconds")
        window = _finite_nonnegative(recent_window_seconds, "recent_window_seconds")
        cutoff = timestamp - delay
        output = np.zeros((10, 12), dtype=np.float32)
        for role_index, role in enumerate(self._roles):
            for slot in (role_index, role_index + 5):
                events = self._account_events(role, accounts[slot])
                if events is None:
                    continue
                end_ts = role["end_ts"][events]
                scores = role["scores"][events]
                output[slot, :3] = self._summary(end_ts, scores, cutoff)
                output[slot, 6:9] = self._summary(end_ts, scores, cutoff, window)
                # Account histories are intentionally the only hero lookup scope.
                hero_events = role["heroes"][events] == heroes[slot]
                if hero_events.any():
                    output[slot, 3:6] = self._summary(end_ts[hero_events], scores[hero_events], cutoff)
                    output[slot, 9:12] = self._summary(end_ts[hero_events], scores[hero_events], cutoff, window)
        return output
