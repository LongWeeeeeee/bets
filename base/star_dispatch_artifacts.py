"""Crash-safe, duplicate-safe checkpoint + deterministic manifest utilities.

Owned by W-ARTIFACTS. Provides append/checkpoint storage for canonical map and
dispatch rows, SHA-256 identity contracts, and compaction/validation APIs.

No network, no source collection, no statistics interpretation, no final/ writes.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

PathLike = Union[str, Path]

STATE_FILENAME = "checkpoint_state.json"
MAP_SHARD_PREFIX = "map_rows_"
DISPATCH_SHARD_PREFIX = "dispatch_rows_"
SHARD_SUFFIX = ".jsonl"

DEFAULT_CHECKPOINT_EVERY = 250

# Schema keys required on canonical rows
MAP_ROW_REQUIRED = ("map_id", "event_id")
DISPATCH_ROW_REQUIRED = ("map_id", "event_id")

# Config fields that participate in config_hash (order-independent via sort_keys)
CONFIG_HASH_KEYS = (
    "population",
    "checkpoints",
    "buckets",
    "split_rule",
    "cutoffs",
    "seed",
    "policy_constants",
    "code_version",
    "checkpoint_every_unique_maps",
)


class IdentityMismatchError(RuntimeError):
    """Resume rejected: config / source / dictionary identity does not match."""


class CorruptedShardError(RuntimeError):
    """On-disk shard fails hash or JSON integrity check."""


class ValidationError(RuntimeError):
    """Compacted artifact failed schema / uniqueness / gzip validation."""


# ---------------------------------------------------------------------------
# Low-level hashing + atomic IO
# ---------------------------------------------------------------------------


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: PathLike) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sha256_json(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256_text(payload)


def _rel_posix(path: Path, root: Optional[Path]) -> str:
    p = Path(path).resolve()
    if root is not None:
        try:
            return p.relative_to(Path(root).resolve()).as_posix()
        except ValueError:
            pass
    return p.as_posix()


def source_corpus_digest(
    paths: Sequence[PathLike],
    *,
    root: Optional[PathLike] = None,
) -> str:
    """SHA-256 over deterministic ordered list of (relative_path, content_hash).

    Order of input ``paths`` does not matter: entries are sorted by relative path.
    """
    root_p = Path(root).resolve() if root is not None else None
    entries: List[List[str]] = []
    for raw in paths:
        p = Path(raw)
        rel = _rel_posix(p, root_p)
        entries.append([rel, sha256_file(p)])
    entries.sort(key=lambda pair: pair[0])
    return sha256_json(entries)


def dictionary_fingerprints(dicts: Mapping[str, PathLike]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for name in sorted(dicts.keys()):
        out[str(name)] = sha256_file(dicts[name])
    return out


def config_hash(config: Mapping[str, Any]) -> str:
    """Hash population/checkpoints/buckets/split/cutoffs/seed/policy/code_version."""
    material: Dict[str, Any] = {}
    for key in CONFIG_HASH_KEYS:
        if key in config:
            material[key] = config[key]
    return sha256_json(material)


def atomic_write_bytes(path: PathLike, data: bytes) -> None:
    """Write sibling .tmp, fsync, validate non-empty (if data non-empty), os.replace.

    Never deletes the prior target first — rebuild-then-replace only.
    A crash before replace leaves the previous file intact.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        if data and tmp.stat().st_size != len(data):
            raise OSError(f"tmp size mismatch for {tmp}")
        os.replace(tmp, target)
    except Exception:
        # leave .tmp for forensics if replace failed mid-way; best-effort cleanup
        # only when replace never started and file is ours — keep prior target.
        try:
            if tmp.exists() and not target.exists():
                # keep tmp so crash can be inspected; do not delete prior
                pass
            elif tmp.exists():
                # target still previous valid — drop incomplete tmp
                tmp.unlink()
        except OSError:
            pass
        raise


def atomic_write_text(path: PathLike, text: str, *, encoding: str = "utf-8") -> None:
    atomic_write_bytes(path, text.encode(encoding))


def atomic_write_json(path: PathLike, payload: Any, *, indent: int = 2) -> None:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=indent) + "\n"
    atomic_write_text(path, text)


def atomic_write_jsonl_lines(path: PathLike, lines: Sequence[str]) -> None:
    body = "".join(line if line.endswith("\n") else line + "\n" for line in lines)
    atomic_write_text(path, body)


# ---------------------------------------------------------------------------
# Checkpoint store
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _empty_counts() -> Dict[str, int]:
    return {
        "raw_seen": 0,
        "unique_maps": 0,
        "map_rows": 0,
        "dispatch_rows": 0,
        "duplicate_skipped": 0,
        "invalid": 0,
    }


def _new_state(
    *,
    config_hash_value: str,
    source_corpus_digest_value: str,
    dictionary_fps: Dict[str, str],
    checkpoint_every: int,
) -> Dict[str, Any]:
    now = _utc_now_iso()
    return {
        "version": 1,
        "config_hash": config_hash_value,
        "source_corpus_digest": source_corpus_digest_value,
        "dictionary_fingerprints": dict(dictionary_fps),
        "last_committed_input_cursor": None,
        "committed_map_ids": [],
        "committed_event_ids": [],
        "counts": _empty_counts(),
        "shard_index": 0,
        "shard_hashes": {},
        "checkpoint_every_unique_maps": int(checkpoint_every),
        "pending_map_rows": 0,
        "start_ts": now,
        "end_ts": now,
        "lifecycle_status": "partial",
    }


@dataclass
class ArtifactCheckpointStore:
    """Append/checkpoint storage for map + dispatch rows in bounded shards.

    Integration default is checkpoint every 250 unique maps
    (``checkpoint_every_unique_maps`` in config).
    """

    directory: PathLike
    config: Mapping[str, Any]
    source_paths: Sequence[PathLike]
    dictionaries: Mapping[str, PathLike] = field(default_factory=dict)
    source_root: Optional[PathLike] = None

    def __post_init__(self) -> None:
        self.directory = Path(self.directory)  # type: ignore[assignment]
        self._dir: Path = Path(self.directory)
        self._cfg_hash = config_hash(self.config)
        self._source_digest = source_corpus_digest(
            self.source_paths, root=self.source_root
        )
        self._dict_fps = dictionary_fingerprints(self.dictionaries)
        self._checkpoint_every = int(
            self.config.get("checkpoint_every_unique_maps", DEFAULT_CHECKPOINT_EVERY)
        )
        self._state: Optional[Dict[str, Any]] = None
        self._committed_maps: set[str] = set()
        self._committed_events: set[str] = set()
        self._pending_maps: List[Dict[str, Any]] = []
        self._pending_dispatches: List[Dict[str, Any]] = []
        self._pending_cursor: Optional[str] = None
        self._opened = False

    # -- paths ---------------------------------------------------------------

    @property
    def state_path(self) -> Path:
        return self._dir / STATE_FILENAME

    def _map_shard_path(self, index: int) -> Path:
        return self._dir / f"{MAP_SHARD_PREFIX}{index:05d}{SHARD_SUFFIX}"

    def _dispatch_shard_path(self, index: int) -> Path:
        return self._dir / f"{DISPATCH_SHARD_PREFIX}{index:05d}{SHARD_SUFFIX}"

    # -- open / resume -------------------------------------------------------

    def open_or_create(self) -> Dict[str, Any]:
        self._dir.mkdir(parents=True, exist_ok=True)
        if self.state_path.exists():
            state = self._load_state_file()
            self._validate_identity(state)
            self._validate_shard_hashes(state)
            self._state = state
            self._committed_maps = set(state.get("committed_map_ids") or [])
            self._committed_events = set(state.get("committed_event_ids") or [])
        else:
            self._state = _new_state(
                config_hash_value=self._cfg_hash,
                source_corpus_digest_value=self._source_digest,
                dictionary_fps=self._dict_fps,
                checkpoint_every=self._checkpoint_every,
            )
            self._committed_maps = set()
            self._committed_events = set()
            # persist initial empty state so a crash mid-first-batch still has a root
            self._flush_state_only()
        self._pending_maps = []
        self._pending_dispatches = []
        self._pending_cursor = None
        self._opened = True
        return dict(self._state)

    def load_state(self) -> Dict[str, Any]:
        if self._state is not None:
            return dict(self._state)
        return self._load_state_file()

    def _load_state_file(self) -> Dict[str, Any]:
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeError) as exc:
            raise CorruptedShardError(
                f"checkpoint state unreadable at {self.state_path}: {exc}"
            ) from exc
        if not isinstance(data, dict):
            raise CorruptedShardError(f"checkpoint state is not an object: {self.state_path}")
        return data

    def _validate_identity(self, state: Mapping[str, Any]) -> None:
        expected = {
            "config_hash": self._cfg_hash,
            "source_corpus_digest": self._source_digest,
            "dictionary_fingerprints": self._dict_fps,
        }
        problems: List[str] = []
        if state.get("config_hash") != expected["config_hash"]:
            problems.append(
                f"config_hash mismatch: stored={state.get('config_hash')!r} "
                f"current={expected['config_hash']!r}"
            )
        if state.get("source_corpus_digest") != expected["source_corpus_digest"]:
            problems.append(
                f"source_corpus_digest mismatch: stored={state.get('source_corpus_digest')!r} "
                f"current={expected['source_corpus_digest']!r}"
            )
        stored_fps = state.get("dictionary_fingerprints") or {}
        if dict(stored_fps) != dict(expected["dictionary_fingerprints"]):
            problems.append(
                f"dictionary_fingerprints mismatch: stored={stored_fps!r} "
                f"current={expected['dictionary_fingerprints']!r}"
            )
        if problems:
            raise IdentityMismatchError(
                "resume identity check failed (old checkpoints preserved): "
                + "; ".join(problems)
            )

    def _validate_shard_hashes(self, state: Mapping[str, Any]) -> None:
        shard_hashes = state.get("shard_hashes") or {}
        for name, expected_hash in shard_hashes.items():
            path = self._dir / name
            if not path.exists():
                raise CorruptedShardError(
                    f"missing shard referenced by state: {name}"
                )
            actual = sha256_file(path)
            if actual != expected_hash:
                raise CorruptedShardError(
                    f"shard hash mismatch for {name}: expected={expected_hash} actual={actual}"
                )
            # JSONL parse check
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    for _lineno, line in enumerate(fh, 1):
                        line = line.strip()
                        if not line:
                            continue
                        json.loads(line)
            except (OSError, json.JSONDecodeError, UnicodeError) as exc:
                raise CorruptedShardError(
                    f"corrupted shard {name} at line parse: {exc}"
                ) from exc

    # -- append --------------------------------------------------------------

    def record_counts(
        self,
        *,
        raw_seen: Optional[int] = None,
        duplicate_skipped: Optional[int] = None,
        invalid: Optional[int] = None,
    ) -> None:
        self._require_open()
        assert self._state is not None
        c = self._state["counts"]
        if raw_seen is not None:
            c["raw_seen"] = int(raw_seen)
        if duplicate_skipped is not None:
            c["duplicate_skipped"] = int(duplicate_skipped)
        if invalid is not None:
            c["invalid"] = int(invalid)

    def append_map_row(
        self,
        row: Mapping[str, Any],
        *,
        input_cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._require_open()
        assert self._state is not None
        self._state["counts"]["raw_seen"] = int(self._state["counts"].get("raw_seen", 0)) + 1

        map_id = row.get("map_id")
        event_id = row.get("event_id")
        if not map_id or not event_id:
            self._state["counts"]["invalid"] = int(self._state["counts"].get("invalid", 0)) + 1
            return {"status": "invalid", "reason": "missing_map_id_or_event_id"}

        map_id_s = str(map_id)
        event_id_s = str(event_id)

        if map_id_s in self._committed_maps or map_id_s in {
            r["map_id"] for r in self._pending_maps
        }:
            self._state["counts"]["duplicate_skipped"] = (
                int(self._state["counts"].get("duplicate_skipped", 0)) + 1
            )
            return {"status": "skipped_duplicate", "map_id": map_id_s}

        if event_id_s in self._committed_events or event_id_s in {
            r["event_id"] for r in self._pending_maps
        }:
            self._state["counts"]["duplicate_skipped"] = (
                int(self._state["counts"].get("duplicate_skipped", 0)) + 1
            )
            return {"status": "skipped_duplicate", "event_id": event_id_s, "map_id": map_id_s}

        clean = dict(row)
        clean["map_id"] = map_id_s
        clean["event_id"] = event_id_s
        self._pending_maps.append(clean)
        if input_cursor is not None:
            self._pending_cursor = str(input_cursor)
        return {"status": "accepted", "map_id": map_id_s, "event_id": event_id_s}

    def append_dispatch_row(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        self._require_open()
        assert self._state is not None
        map_id = row.get("map_id")
        event_id = row.get("event_id")
        if not map_id or not event_id:
            return {"status": "invalid", "reason": "missing_map_id_or_event_id"}
        map_id_s = str(map_id)
        event_id_s = str(event_id)

        # Dedup dispatch by event_id within committed+pending
        pending_eids = {r["event_id"] for r in self._pending_dispatches}
        if event_id_s in self._committed_events and event_id_s in {
            # only skip if already written as dispatch for same event in a prior shard —
            # committed_events tracks accepted map events; allow one dispatch per map event
            # if already present in pending for this event → skip
        }:
            pass
        if event_id_s in pending_eids:
            return {"status": "skipped_duplicate", "event_id": event_id_s}

        # Also skip if a committed dispatch shard already has this event_id —
        # tracked via committed_event_ids which are shared with maps (1:1).
        # For resume safety: if map was committed, its dispatch was checkpointed
        # together, so re-append of dispatch for committed map is a no-op skip.
        if map_id_s in self._committed_maps and event_id_s in self._committed_events:
            return {"status": "skipped_duplicate", "event_id": event_id_s, "map_id": map_id_s}

        clean = dict(row)
        clean["map_id"] = map_id_s
        clean["event_id"] = event_id_s
        self._pending_dispatches.append(clean)
        return {"status": "accepted", "map_id": map_id_s, "event_id": event_id_s}

    def maybe_checkpoint(self, *, force: bool = False) -> Optional[Dict[str, Any]]:
        self._require_open()
        assert self._state is not None
        pending_unique = len(self._pending_maps)
        if not force and pending_unique < self._checkpoint_every:
            return None
        if pending_unique == 0 and not force:
            return None
        if pending_unique == 0 and force and not self._pending_dispatches:
            # still refresh end_ts
            self._state["end_ts"] = _utc_now_iso()
            self._flush_state_only()
            return dict(self._state)
        return self._commit_pending()

    def _commit_pending(self) -> Dict[str, Any]:
        assert self._state is not None
        # next shard index: use current shard_index then increment
        idx = int(self._state.get("shard_index", 0))
        map_path = self._map_shard_path(idx)
        disp_path = self._dispatch_shard_path(idx)

        map_lines = [
            json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for r in self._pending_maps
        ]
        disp_lines = [
            json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for r in self._pending_dispatches
        ]

        # Write shards first (new files — no prior content to preserve per-index)
        if map_lines:
            atomic_write_jsonl_lines(map_path, map_lines)
            self._state["shard_hashes"][map_path.name] = sha256_file(map_path)
        if disp_lines:
            atomic_write_jsonl_lines(disp_path, disp_lines)
            self._state["shard_hashes"][disp_path.name] = sha256_file(disp_path)

        # Update committed sets + counts
        for r in self._pending_maps:
            self._committed_maps.add(r["map_id"])
            self._committed_events.add(r["event_id"])
        for r in self._pending_dispatches:
            # event already in set from map; ensure present
            self._committed_events.add(r["event_id"])

        c = self._state["counts"]
        c["unique_maps"] = len(self._committed_maps)
        c["map_rows"] = int(c.get("map_rows", 0)) + len(self._pending_maps)
        c["dispatch_rows"] = int(c.get("dispatch_rows", 0)) + len(self._pending_dispatches)

        if self._pending_cursor is not None:
            self._state["last_committed_input_cursor"] = self._pending_cursor

        self._state["committed_map_ids"] = sorted(self._committed_maps)
        self._state["committed_event_ids"] = sorted(self._committed_events)
        self._state["shard_index"] = idx + 1
        self._state["pending_map_rows"] = 0
        self._state["end_ts"] = _utc_now_iso()
        self._state["lifecycle_status"] = "partial"
        # reaffirm identity fields
        self._state["config_hash"] = self._cfg_hash
        self._state["source_corpus_digest"] = self._source_digest
        self._state["dictionary_fingerprints"] = dict(self._dict_fps)
        self._state["checkpoint_every_unique_maps"] = self._checkpoint_every

        # Atomic state replace LAST — crash before this leaves prior state + any
        # newly written shard files that are not yet referenced (harmless orphans).
        self._flush_state_only()

        self._pending_maps = []
        self._pending_dispatches = []
        self._pending_cursor = None
        return dict(self._state)

    def _flush_state_only(self) -> None:
        assert self._state is not None
        atomic_write_json(self.state_path, self._state)

    def _require_open(self) -> None:
        if not self._opened or self._state is None:
            raise RuntimeError("ArtifactCheckpointStore.open_or_create() must be called first")

    def mark_complete(self) -> Dict[str, Any]:
        self._require_open()
        assert self._state is not None
        if self._pending_maps or self._pending_dispatches:
            self.maybe_checkpoint(force=True)
        self._state["lifecycle_status"] = "complete"
        self._state["end_ts"] = _utc_now_iso()
        self._flush_state_only()
        return dict(self._state)


# ---------------------------------------------------------------------------
# Compaction + validation
# ---------------------------------------------------------------------------


def _iter_jsonl_shards(directory: Path, prefix: str) -> List[Path]:
    return sorted(directory.glob(f"{prefix}*{SHARD_SUFFIX}"))


def _read_jsonl_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValidationError(f"non-object row in {path}")
            rows.append(obj)
    return rows


def compact_shards(
    staging_dir: PathLike,
    *,
    destination: PathLike,
    lifecycle_status: str = "complete",
) -> Dict[str, Any]:
    """Merge staging shards into map_rows.jsonl.gz + dispatch_rows.jsonl.gz.

    Writes only under caller-supplied ``destination`` via atomic .tmp + replace.
    Validates uniqueness and required keys before replacing outputs.
    """
    if lifecycle_status not in ("complete", "partial"):
        raise ValueError("lifecycle_status must be 'complete' or 'partial'")

    staging = Path(staging_dir)
    dest = Path(destination)
    dest.mkdir(parents=True, exist_ok=True)

    map_rows: List[Dict[str, Any]] = []
    for p in _iter_jsonl_shards(staging, MAP_SHARD_PREFIX):
        map_rows.extend(_read_jsonl_rows(p))

    disp_rows: List[Dict[str, Any]] = []
    for p in _iter_jsonl_shards(staging, DISPATCH_SHARD_PREFIX):
        disp_rows.extend(_read_jsonl_rows(p))

    # schema + uniqueness
    map_ids: set[str] = set()
    event_ids: set[str] = set()
    for r in map_rows:
        for k in MAP_ROW_REQUIRED:
            if k not in r or r[k] in (None, ""):
                raise ValidationError(f"map row missing required key {k!r}: {r!r}")
        mid = str(r["map_id"])
        eid = str(r["event_id"])
        if mid in map_ids:
            raise ValidationError(f"duplicate map_id in shards: {mid}")
        if eid in event_ids:
            raise ValidationError(f"duplicate event_id in map shards: {eid}")
        map_ids.add(mid)
        event_ids.add(eid)

    disp_event_ids: set[str] = set()
    for r in disp_rows:
        for k in DISPATCH_ROW_REQUIRED:
            if k not in r or r[k] in (None, ""):
                raise ValidationError(f"dispatch row missing required key {k!r}: {r!r}")
        eid = str(r["event_id"])
        if eid in disp_event_ids:
            raise ValidationError(f"duplicate event_id in dispatch shards: {eid}")
        disp_event_ids.add(eid)

    map_gz = dest / "map_rows.jsonl.gz"
    disp_gz = dest / "dispatch_rows.jsonl.gz"

    def _write_gz(path: Path, rows: Sequence[Mapping[str, Any]]) -> str:
        tmp = path.with_name(path.name + ".tmp")
        try:
            with gzip.open(tmp, "wt", encoding="utf-8") as fh:
                for r in rows:
                    fh.write(
                        json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                        + "\n"
                    )
                fh.flush()
                # gzip fileobj wraps a binary file — fsync underlying buffer
                underlying = getattr(fh, "fileobj", None) or getattr(fh, "myfileobj", None)
                if underlying is not None:
                    underlying.flush()
                    os.fsync(underlying.fileno())
            # validate gzip readable before replace
            with gzip.open(tmp, "rt", encoding="utf-8") as fh:
                n = sum(1 for line in fh if line.strip())
            if n != len(rows):
                raise ValidationError(
                    f"gzip row count mismatch for {path.name}: {n} != {len(rows)}"
                )
            os.replace(tmp, path)
        except Exception:
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass
            raise
        return sha256_file(path)

    map_hash = _write_gz(map_gz, map_rows)
    disp_hash = _write_gz(disp_gz, disp_rows)

    result = {
        "counts": {
            "unique_maps": len(map_ids),
            "map_rows": len(map_rows),
            "dispatch_rows": len(disp_rows),
        },
        "artifact_hashes": {
            "map_rows.jsonl.gz": map_hash,
            "dispatch_rows.jsonl.gz": disp_hash,
        },
        "lifecycle_status": lifecycle_status,
        "destination": str(dest),
    }
    return result


def validate_compacted(destination: PathLike) -> Dict[str, Any]:
    """Validate JSON schema keys, unique map_id/event_id, counts, gzip integrity."""
    dest = Path(destination)
    errors: List[str] = []
    map_gz = dest / "map_rows.jsonl.gz"
    disp_gz = dest / "dispatch_rows.jsonl.gz"

    if not map_gz.exists():
        errors.append("missing map_rows.jsonl.gz")
    if not disp_gz.exists():
        errors.append("missing dispatch_rows.jsonl.gz")
    if errors:
        return {"ok": False, "errors": errors, "map_rows": 0, "dispatch_rows": 0}

    map_rows: List[Dict[str, Any]] = []
    disp_rows: List[Dict[str, Any]] = []
    try:
        with gzip.open(map_gz, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                map_rows.append(json.loads(line))
    except Exception as exc:
        errors.append(f"gzip/map read error: {exc}")

    try:
        with gzip.open(disp_gz, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                disp_rows.append(json.loads(line))
    except Exception as exc:
        errors.append(f"gzip/dispatch read error: {exc}")

    map_ids: set[str] = set()
    event_ids: set[str] = set()
    for r in map_rows:
        if not isinstance(r, dict):
            errors.append("non-object map row")
            continue
        for k in MAP_ROW_REQUIRED:
            if k not in r:
                errors.append(f"map row missing key {k}")
        mid = str(r.get("map_id", ""))
        eid = str(r.get("event_id", ""))
        if mid in map_ids:
            errors.append(f"duplicate map_id: {mid}")
        if eid in event_ids:
            errors.append(f"duplicate event_id: {eid}")
        map_ids.add(mid)
        event_ids.add(eid)

    disp_eids: set[str] = set()
    for r in disp_rows:
        if not isinstance(r, dict):
            errors.append("non-object dispatch row")
            continue
        for k in DISPATCH_ROW_REQUIRED:
            if k not in r:
                errors.append(f"dispatch row missing key {k}")
        eid = str(r.get("event_id", ""))
        if eid in disp_eids:
            errors.append(f"duplicate dispatch event_id: {eid}")
        disp_eids.add(eid)

    return {
        "ok": not errors,
        "errors": errors,
        "map_rows": len(map_rows),
        "dispatch_rows": len(disp_rows),
        "unique_map_ids": len(map_ids),
        "artifact_hashes": {
            "map_rows.jsonl.gz": sha256_file(map_gz) if map_gz.exists() else None,
            "dispatch_rows.jsonl.gz": sha256_file(disp_gz) if disp_gz.exists() else None,
        },
    }


def build_manifest(
    staging_dir: PathLike,
    *,
    compacted_dir: PathLike,
    lifecycle_status: str = "complete",
) -> Dict[str, Any]:
    """Build reproducible manifest (no secrets)."""
    if lifecycle_status not in ("complete", "partial"):
        raise ValueError("lifecycle_status must be 'complete' or 'partial'")

    staging = Path(staging_dir)
    compacted = Path(compacted_dir)
    state: Dict[str, Any] = {}
    state_path = staging / STATE_FILENAME
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))

    counts = dict(state.get("counts") or {})
    artifact_paths: Dict[str, str] = {}
    artifact_sizes: Dict[str, int] = {}
    artifact_hashes: Dict[str, str] = {}

    for name in ("map_rows.jsonl.gz", "dispatch_rows.jsonl.gz", "manifest.json"):
        p = compacted / name
        if p.exists() and name != "manifest.json":
            artifact_paths[name] = str(p)
            artifact_sizes[name] = p.stat().st_size
            artifact_hashes[name] = sha256_file(p)

    # include shard inventory
    for p in sorted(staging.glob(f"{MAP_SHARD_PREFIX}*{SHARD_SUFFIX}")):
        key = f"shard:{p.name}"
        artifact_paths[key] = str(p)
        artifact_sizes[key] = p.stat().st_size
        artifact_hashes[key] = sha256_file(p)
    for p in sorted(staging.glob(f"{DISPATCH_SHARD_PREFIX}*{SHARD_SUFFIX}")):
        key = f"shard:{p.name}"
        artifact_paths[key] = str(p)
        artifact_sizes[key] = p.stat().st_size
        artifact_hashes[key] = sha256_file(p)

    env = {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "machine": platform.machine(),
        "system": platform.system(),
    }

    unique_accepted = int(counts.get("unique_maps") or counts.get("map_rows") or 0)

    manifest: Dict[str, Any] = {
        "raw_seen": int(counts.get("raw_seen", 0)),
        "unique_accepted": unique_accepted,
        "duplicate_skipped": int(counts.get("duplicate_skipped", 0)),
        "invalid": int(counts.get("invalid", 0)),
        "map_rows": int(counts.get("map_rows", 0)),
        "dispatch_rows": int(counts.get("dispatch_rows", 0)),
        "start_ts": state.get("start_ts"),
        "end_ts": state.get("end_ts") or _utc_now_iso(),
        "lifecycle_status": lifecycle_status,
        "artifact_paths": artifact_paths,
        "artifact_sizes": artifact_sizes,
        "artifact_hashes": artifact_hashes,
        "config_hash": state.get("config_hash"),
        "source_corpus_digest": state.get("source_corpus_digest"),
        "dictionary_fingerprints": state.get("dictionary_fingerprints") or {},
        "last_committed_input_cursor": state.get("last_committed_input_cursor"),
        "shard_hashes": state.get("shard_hashes") or {},
        "environment": env,
        "interpreter": sys.executable,
    }
    return manifest


__all__ = [
    "DEFAULT_CHECKPOINT_EVERY",
    "IdentityMismatchError",
    "CorruptedShardError",
    "ValidationError",
    "sha256_bytes",
    "sha256_text",
    "sha256_file",
    "sha256_json",
    "source_corpus_digest",
    "dictionary_fingerprints",
    "config_hash",
    "atomic_write_bytes",
    "atomic_write_text",
    "atomic_write_json",
    "atomic_write_jsonl_lines",
    "ArtifactCheckpointStore",
    "compact_shards",
    "validate_compacted",
    "build_manifest",
]
