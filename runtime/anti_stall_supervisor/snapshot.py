#!/usr/bin/env python3
"""Read-only, fail-closed Kanban snapshot adapter (snapshot_v1).

Discovers non-archived Hermes boards under a supplied hermes_root and emits a
normalized lifecycle/progress evidence snapshot for the anti-stall supervisor.

Side-effect free: SQLite is opened read-only; no board mutation; no stall
classification; no action decisions.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import sys
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

SCHEMA_NAME = "snapshot_v1"
SCHEMA_VERSION = 1
NS_PER_SEC = 1_000_000_000

ARTIFACT_PREFIX = "ANTI_STALL_ARTIFACTS_V1="
RESOLUTION_PREFIX = "ANTI_STALL_RESOLUTION_V1="

REQUIRED_TABLES = ("tasks", "task_links", "task_comments", "task_events", "task_runs")

# Path fragments that must never be accepted as artifact targets.
_SECRET_PATH_FRAGMENTS = (
    "/.env",
    "/.env.",
    "/keys.py",
    "/secrets/",
    "/secret/",
    "/credentials",
    "/credential",
    "/.ssh/",
    "/id_rsa",
    "/id_ed25519",
    "/auth.json",
    "/api_keys",
    "/private_key",
    "/.aws/",
    "/.config/gcloud/",
    "credentials.json",
    "service-account",
)

_SECRET_TEXT_RE = re.compile(
    r"("
    r"sk-[A-Za-z0-9_-]{10,}"
    r"|ghp_[A-Za-z0-9]{10,}"
    r"|github_pat_[A-Za-z0-9_]{10,}"
    r"|gho_[A-Za-z0-9]{10,}"
    r"|xox[baprs]-[A-Za-z0-9-]{10,}"
    r"|AIza[A-Za-z0-9_-]{30,}"
    r"|AKIA[A-Z0-9]{16}"
    r"|xai-[A-Za-z0-9]{30,}"
    r"|gsk_[A-Za-z0-9]{10,}"
    r"|hf_[A-Za-z0-9]{10,}"
    r"|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"  # JWT-ish
    r"|Bearer\s+[A-Za-z0-9._\-+/=]{12,}"
    r"|(?:api[_-]?key|token|password|secret|passwd)\s*[:=]\s*\S+"
    r")",
    re.IGNORECASE,
)

_SECRET_ENV_RE = re.compile(
    r"\b([A-Z0-9_]{0,40}(?:API_?KEY|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|AUTH)[A-Z0-9_]{0,40})\s*=\s*([^\s'\"]+)",
    re.IGNORECASE,
)

ProcReader = Callable[[int], Optional[Mapping[str, Any]]]


# ---------------------------------------------------------------------------
# Canonical JSON / hashing
# ---------------------------------------------------------------------------


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_obj(value: Any) -> str:
    return sha256_text(canonical_json(value))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def redact_text(value: Any) -> Any:
    """Redact secret-like substrings. Never raises."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="replace")
        except Exception:
            return "[REDACTED_BYTES]"
    if isinstance(value, list):
        return [redact_text(v) for v in value]
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            ks = str(k).lower()
            if any(
                s in ks
                for s in (
                    "password",
                    "secret",
                    "token",
                    "api_key",
                    "apikey",
                    "authorization",
                    "private_key",
                    "credential",
                    "passwd",
                )
            ):
                out[k] = "[REDACTED]"
            else:
                out[k] = redact_text(v)
        return out
    text = str(value)
    text = _SECRET_TEXT_RE.sub("[REDACTED]", text)
    text = _SECRET_ENV_RE.sub(r"\1=[REDACTED]", text)
    return text


def _safe_json_loads(raw: Optional[str]) -> Any:
    if raw is None or raw == "":
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def _to_ns(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        iv = int(value)
    except (TypeError, ValueError):
        return None
    # Heuristic: unix seconds are < 1e12 for a long while; ns are large.
    if iv < 10**12:
        return iv * NS_PER_SEC
    return iv


def _now_ns_default() -> int:
    return time.time_ns()


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def _diag(
    code: str,
    message: str,
    *,
    board_id: Optional[str] = None,
    task_id: Optional[str] = None,
    evidence: Optional[Mapping[str, Any]] = None,
) -> dict:
    out: dict[str, Any] = {"code": str(code), "message": str(message)}
    if board_id is not None:
        out["board_id"] = board_id
    if task_id is not None:
        out["task_id"] = task_id
    if evidence is not None:
        out["evidence"] = redact_text(dict(evidence))
    return out


# ---------------------------------------------------------------------------
# Process reader
# ---------------------------------------------------------------------------


def default_proc_reader(pid: int) -> Optional[dict[str, Any]]:
    """Read /proc/<pid> start ticks. Returns None if pid absent.

    Raises PermissionError on unreadable /proc entries when the pid dir exists
    but stat is not readable — callers map that to permission_unknown.
    """
    if pid is None or int(pid) <= 0:
        return None
    pid = int(pid)
    proc = Path(f"/proc/{pid}")
    if not proc.exists():
        return None
    stat_path = proc / "stat"
    try:
        raw = stat_path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return None
    except PermissionError:
        raise
    except OSError as exc:
        # ESRCH etc.
        if getattr(exc, "errno", None) in (2, 3):  # ENOENT, ESRCH
            return None
        raise PermissionError(str(exc)) from exc
    # comm may contain spaces/parens — strip "pid (comm) rest"
    m = re.match(r"^\d+\s+\((.*)\)\s+([A-Za-z])\s+(.*)$", raw, re.DOTALL)
    if not m:
        return {"pid": pid, "start_ticks": None, "state": None, "readable": True}
    rest = m.group(3).split()
    # fields after state: ppid ... starttime is index 19 in rest (man proc_pid_stat)
    start_ticks = None
    if len(rest) >= 20:
        try:
            start_ticks = int(rest[19])
        except ValueError:
            start_ticks = None
    return {
        "pid": pid,
        "state": m.group(2),
        "start_ticks": start_ticks,
        "readable": True,
    }


# ---------------------------------------------------------------------------
# Board discovery
# ---------------------------------------------------------------------------


def _read_board_json(path: Path) -> tuple[Optional[dict], Optional[str]]:
    """Return (meta, error_code). error_code set on missing/malformed."""
    if not path.exists():
        return None, "board_json_missing"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None, "board_json_malformed"
    if not isinstance(raw, dict):
        return None, "board_json_malformed"
    return raw, None


def _resolve_path(p: Path) -> Path:
    try:
        return p.resolve()
    except Exception:
        return p.absolute()


def discover_boards(hermes_root: Path) -> list[Path]:
    """Discover board DB paths under hermes_root that are machine-verified non-archived.

    - default board: ``<root>/kanban.db``; archive flag from
      ``kanban/boards/default/board.json`` when present (missing => non-archived).
    - named boards: ``kanban/boards/<slug>/kanban.db`` with ``board.json``
      explicitly ``archived: false`` (or absent archived key treated as false only
      when board.json is present and parseable).
    - boards with unknown/malformed archive metadata are excluded (caller may
      still surface diagnostics via collect_snapshot).
    - results are sorted and deduplicated by resolved path.
    """
    root = Path(hermes_root)
    found: list[tuple[str, Path]] = []  # (sort_key, path)
    seen: set[str] = set()

    def _add(sort_key: str, db: Path) -> None:
        try:
            key = str(_resolve_path(db))
        except Exception:
            key = str(db)
        if key in seen:
            return
        if not db.is_file():
            return
        seen.add(key)
        found.append((sort_key, db))

    default_db = root / "kanban.db"
    default_meta_path = root / "kanban" / "boards" / "default" / "board.json"
    if default_db.is_file():
        meta, err = _read_board_json(default_meta_path)
        if err == "board_json_malformed":
            pass  # exclude — archive state not machine-verifiable
        else:
            archived = bool(meta.get("archived")) if isinstance(meta, dict) else False
            if not archived:
                _add("0:default", default_db)

    boards_root = root / "kanban" / "boards"
    if boards_root.is_dir():
        for child in sorted(boards_root.iterdir(), key=lambda p: p.name.lower()):
            if not child.is_dir():
                continue
            slug = child.name
            if slug.startswith("_"):
                continue  # _archived etc.
            if slug == "default":
                # default DB is legacy path; skip dir DB if any
                continue
            db = child / "kanban.db"
            meta_path = child / "board.json"
            meta, err = _read_board_json(meta_path)
            if err is not None or not isinstance(meta, dict):
                # unknown archive state — exclude from discover list
                continue
            if bool(meta.get("archived")):
                continue
            if db.is_file():
                _add(f"1:{slug.lower()}", db)

    found.sort(key=lambda x: x[0])
    return [p for _, p in found]


def _board_id_for_db(hermes_root: Path, db_path: Path) -> str:
    root = _resolve_path(Path(hermes_root))
    db = _resolve_path(Path(db_path))
    default_db = _resolve_path(root / "kanban.db")
    if db == default_db:
        return "default"
    # .../kanban/boards/<slug>/kanban.db
    try:
        if db.name == "kanban.db" and db.parent.parent.name == "boards":
            return db.parent.name
    except Exception:
        pass
    # fallback: parent dir name or stem
    if db.name == "kanban.db":
        return db.parent.name
    return db.stem


def _enumerate_board_candidates(hermes_root: Path) -> list[dict[str, Any]]:
    """All potential boards with archive verification outcome (for diagnostics)."""
    root = Path(hermes_root)
    out: list[dict[str, Any]] = []

    default_db = root / "kanban.db"
    default_meta = root / "kanban" / "boards" / "default" / "board.json"
    if default_db.exists() or default_meta.exists():
        meta, err = _read_board_json(default_meta)
        archived = None
        verifiable = False
        if err == "board_json_malformed":
            verifiable = False
        elif err == "board_json_missing":
            archived = False
            verifiable = default_db.is_file()
        else:
            archived = bool(meta.get("archived")) if meta else False
            verifiable = True
        out.append(
            {
                "board_id": "default",
                "db_path": default_db if default_db.exists() else None,
                "meta_path": default_meta,
                "archived": archived,
                "archive_verifiable": verifiable,
                "meta_error": err if err == "board_json_malformed" else None,
            }
        )

    boards_root = root / "kanban" / "boards"
    if boards_root.is_dir():
        for child in sorted(boards_root.iterdir(), key=lambda p: p.name.lower()):
            if not child.is_dir() or child.name.startswith("_") or child.name == "default":
                continue
            db = child / "kanban.db"
            meta_path = child / "board.json"
            if not db.exists() and not meta_path.exists():
                continue
            meta, err = _read_board_json(meta_path)
            archived = None
            verifiable = False
            if err is None and meta is not None:
                archived = bool(meta.get("archived"))
                verifiable = True
            out.append(
                {
                    "board_id": child.name,
                    "db_path": db if db.exists() else None,
                    "meta_path": meta_path,
                    "archived": archived,
                    "archive_verifiable": verifiable,
                    "meta_error": err,
                }
            )
    return out


# ---------------------------------------------------------------------------
# SQLite read-only
# ---------------------------------------------------------------------------


def _connect_ro(db_path: Path) -> sqlite3.Connection:
    # Require immutable read-only URI. mode=ro prevents writes; nolock avoids
    # interfering with live writers when possible.
    uri = f"file:{_resolve_path(db_path)}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=5.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    return con


def _table_names(con: sqlite3.Connection) -> set[str]:
    rows = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {r[0] for r in rows}


def _table_cols(con: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _schema_ok(con: sqlite3.Connection) -> tuple[bool, list[str]]:
    names = _table_names(con)
    missing = [t for t in REQUIRED_TABLES if t not in names]
    if missing:
        return False, missing
    # minimal columns
    task_cols = _table_cols(con, "tasks")
    need = {"id", "status"}
    if not need.issubset(task_cols):
        return False, sorted(need - task_cols)
    return True, []


# ---------------------------------------------------------------------------
# Artifact declaration parsing
# ---------------------------------------------------------------------------


def _looks_secret_path(path: str) -> bool:
    low = path.lower().replace("\\", "/")
    for frag in _SECRET_PATH_FRAGMENTS:
        if frag.lower() in low:
            return True
    base = Path(low).name
    if base in {".env", "keys.py", "auth.json", "credentials", "credentials.json"}:
        return True
    if base.endswith(".pem") or base.endswith(".key"):
        return True
    return False


def _is_absolute_path_str(s: str) -> bool:
    return isinstance(s, str) and s.startswith("/") and len(s) > 1


def _path_inside_prefix(path: Path, prefix: Path) -> bool:
    try:
        path_res = path.resolve(strict=False)
        pref_res = prefix.resolve(strict=False)
    except Exception:
        return False
    try:
        path_res.relative_to(pref_res)
        return True
    except Exception:
        return False


def _lexical_inside(path: str, prefix: str) -> bool:
    """Lexical containment without resolving symlinks (pre-check)."""
    p = path.rstrip("/")
    pref = prefix.rstrip("/")
    if p == pref:
        return False  # must be a file inside, not the prefix itself as dir-only
    return p.startswith(pref + "/")


def parse_artifact_declaration(
    texts: Sequence[tuple[str, str, Any]],
    *,
    task_id: str,
) -> dict[str, Any]:
    """Parse ANTI_STALL_ARTIFACTS_V1 markers from (source_kind, source_id, text).

    Returns artifacts block with diagnostics; never infers empty manifest as declared.
    """
    diagnostics: list[dict] = []
    markers: list[dict] = []

    for source_kind, source_id, text in texts:
        if not text:
            continue
        for line_no, line in enumerate(str(text).splitlines(), 1):
            if not line.startswith(ARTIFACT_PREFIX):
                # exact line only — also allow surrounding whitespace-stripped
                stripped = line.strip()
                if not stripped.startswith(ARTIFACT_PREFIX):
                    continue
                line = stripped
            payload_raw = line[len(ARTIFACT_PREFIX) :]
            try:
                payload = json.loads(payload_raw)
            except json.JSONDecodeError:
                diagnostics.append(
                    _diag(
                        "artifact_marker_malformed",
                        "ANTI_STALL_ARTIFACTS_V1 JSON parse failed",
                        task_id=task_id,
                        evidence={
                            "source_kind": source_kind,
                            "source_id": str(source_id),
                            "line_no": line_no,
                        },
                    )
                )
                continue
            if not isinstance(payload, dict):
                diagnostics.append(
                    _diag(
                        "artifact_marker_malformed",
                        "ANTI_STALL_ARTIFACTS_V1 payload must be object",
                        task_id=task_id,
                        evidence={"source_kind": source_kind, "source_id": str(source_id)},
                    )
                )
                continue
            markers.append(
                {
                    "source_kind": source_kind,
                    "source_id": str(source_id),
                    "line_no": line_no,
                    "payload": payload,
                    "line_sha256": sha256_text(line),
                }
            )

    if not markers:
        return {
            "artifacts_declared": False,
            "owner_prefix": None,
            "required_paths": [],
            "max_no_progress_seconds": None,
            "artifacts": [],
            "artifact_digest": sha256_obj([]),
            "diagnostics": diagnostics,
            "declaration_sources": [],
        }

    # Prefer marker whose task_id matches; else first valid-looking.
    chosen = None
    for m in markers:
        p = m["payload"]
        if str(p.get("task_id") or "") == task_id:
            chosen = m
            break
    if chosen is None:
        chosen = markers[0]
        diagnostics.append(
            _diag(
                "artifact_marker_task_id_mismatch",
                "no marker task_id matched; using first marker",
                task_id=task_id,
                evidence={"marker_task_id": chosen["payload"].get("task_id")},
            )
        )

    payload = chosen["payload"]
    if str(payload.get("task_id") or "") != task_id:
        diagnostics.append(
            _diag(
                "artifact_marker_invalid",
                "marker task_id mismatch",
                task_id=task_id,
                evidence={"marker_task_id": payload.get("task_id")},
            )
        )
        return {
            "artifacts_declared": False,
            "owner_prefix": None,
            "required_paths": [],
            "max_no_progress_seconds": None,
            "artifacts": [],
            "artifact_digest": sha256_obj([]),
            "diagnostics": diagnostics,
            "declaration_sources": [
                {
                    "source_kind": chosen["source_kind"],
                    "source_id": chosen["source_id"],
                    "line_sha256": chosen["line_sha256"],
                }
            ],
        }

    owner_prefix = payload.get("owner_prefix")
    required_paths = payload.get("required_paths")
    max_np = payload.get("max_no_progress_seconds")

    if not isinstance(owner_prefix, str) or not _is_absolute_path_str(owner_prefix):
        diagnostics.append(
            _diag(
                "artifact_marker_invalid",
                "owner_prefix must be absolute path string",
                task_id=task_id,
            )
        )
        return {
            "artifacts_declared": False,
            "owner_prefix": None,
            "required_paths": [],
            "max_no_progress_seconds": None,
            "artifacts": [],
            "artifact_digest": sha256_obj([]),
            "diagnostics": diagnostics,
            "declaration_sources": [
                {
                    "source_kind": chosen["source_kind"],
                    "source_id": chosen["source_id"],
                    "line_sha256": chosen["line_sha256"],
                }
            ],
        }

    if not isinstance(required_paths, list) or not all(isinstance(x, str) for x in required_paths):
        diagnostics.append(
            _diag(
                "artifact_marker_invalid",
                "required_paths must be list of strings",
                task_id=task_id,
            )
        )
        return {
            "artifacts_declared": False,
            "owner_prefix": owner_prefix,
            "required_paths": [],
            "max_no_progress_seconds": _as_int(max_np),
            "artifacts": [],
            "artifact_digest": sha256_obj([]),
            "diagnostics": diagnostics,
            "declaration_sources": [
                {
                    "source_kind": chosen["source_kind"],
                    "source_id": chosen["source_id"],
                    "line_sha256": chosen["line_sha256"],
                }
            ],
        }

    max_np_i = _as_int(max_np)
    prefix_path = Path(owner_prefix)
    accepted: list[dict[str, Any]] = []

    for rp in required_paths:
        if not _is_absolute_path_str(rp):
            diagnostics.append(
                _diag(
                    "artifact_path_rejected",
                    "path not absolute",
                    task_id=task_id,
                    evidence={"path": rp, "reason": "not_absolute"},
                )
            )
            continue
        if not _lexical_inside(rp, owner_prefix):
            diagnostics.append(
                _diag(
                    "artifact_path_rejected",
                    "path not lexically inside owner_prefix",
                    task_id=task_id,
                    evidence={"path": rp, "reason": "lexical_escape"},
                )
            )
            continue
        if _looks_secret_path(rp):
            diagnostics.append(
                _diag(
                    "artifact_path_rejected",
                    "path looks like secret/config/key",
                    task_id=task_id,
                    evidence={"path": rp, "reason": "secret_path"},
                )
            )
            continue

        p = Path(rp)
        # Reject symlink escape: resolve and re-check prefix
        try:
            if p.exists() or p.is_symlink():
                # If any symlink in path, resolve carefully
                resolved = p.resolve(strict=False)
            else:
                resolved = p
        except Exception as exc:
            diagnostics.append(
                _diag(
                    "artifact_path_rejected",
                    f"path resolve failed: {exc}",
                    task_id=task_id,
                    evidence={"path": rp, "reason": "resolve_failed"},
                )
            )
            continue

        if p.is_symlink() or any(
            Path(*p.parts[:i]).is_symlink() for i in range(1, len(p.parts) + 1)
            if Path(*p.parts[:i]).exists()
        ):
            if not _path_inside_prefix(resolved, prefix_path):
                diagnostics.append(
                    _diag(
                        "artifact_path_rejected",
                        "symlink escapes owner_prefix",
                        task_id=task_id,
                        evidence={
                            "path": rp,
                            "resolved": str(resolved),
                            "reason": "symlink_escape",
                        },
                    )
                )
                continue
        elif not _path_inside_prefix(resolved if resolved.exists() else p, prefix_path):
            # physical check for non-symlinks when resolvable
            if resolved.exists() and not _path_inside_prefix(resolved, prefix_path):
                diagnostics.append(
                    _diag(
                        "artifact_path_rejected",
                        "resolved path outside owner_prefix",
                        task_id=task_id,
                        evidence={"path": rp, "reason": "physical_escape"},
                    )
                )
                continue

        if not p.exists():
            diagnostics.append(
                _diag(
                    "artifact_path_missing",
                    "declared artifact path does not exist",
                    task_id=task_id,
                    evidence={"path": rp},
                )
            )
            continue
        try:
            st = p.lstat()
        except OSError as exc:
            diagnostics.append(
                _diag(
                    "artifact_path_rejected",
                    f"lstat failed: {exc}",
                    task_id=task_id,
                    evidence={"path": rp},
                )
            )
            continue
        if stat.S_ISLNK(st.st_mode):
            # already handled escape; still require regular file after resolve
            try:
                st_f = p.stat()
            except OSError as exc:
                diagnostics.append(
                    _diag(
                        "artifact_path_rejected",
                        f"stat failed after symlink: {exc}",
                        task_id=task_id,
                        evidence={"path": rp},
                    )
                )
                continue
            if not stat.S_ISREG(st_f.st_mode):
                diagnostics.append(
                    _diag(
                        "artifact_path_rejected",
                        "not a regular file",
                        task_id=task_id,
                        evidence={"path": rp, "reason": "not_regular_file"},
                    )
                )
                continue
            file_for_hash = p
            size = st_f.st_size
            mtime_ns = getattr(st_f, "st_mtime_ns", int(st_f.st_mtime * NS_PER_SEC))
        elif not stat.S_ISREG(st.st_mode):
            diagnostics.append(
                _diag(
                    "artifact_path_rejected",
                    "not a regular file",
                    task_id=task_id,
                    evidence={"path": rp, "reason": "not_regular_file"},
                )
            )
            continue
        else:
            file_for_hash = p
            size = st.st_size
            mtime_ns = getattr(st, "st_mtime_ns", int(st.st_mtime * NS_PER_SEC))

        # Final physical containment of resolved path
        try:
            final_resolved = file_for_hash.resolve(strict=True)
        except Exception:
            final_resolved = resolved
        if not _path_inside_prefix(final_resolved, prefix_path):
            diagnostics.append(
                _diag(
                    "artifact_path_rejected",
                    "final resolved path outside owner_prefix",
                    task_id=task_id,
                    evidence={"path": rp, "resolved": str(final_resolved)},
                )
            )
            continue
        if _looks_secret_path(str(final_resolved)):
            diagnostics.append(
                _diag(
                    "artifact_path_rejected",
                    "resolved path looks like secret/config/key",
                    task_id=task_id,
                    evidence={"path": rp, "resolved": str(final_resolved)},
                )
            )
            continue

        try:
            digest = sha256_file(file_for_hash)
        except OSError as exc:
            diagnostics.append(
                _diag(
                    "artifact_path_rejected",
                    f"hash failed: {exc}",
                    task_id=task_id,
                    evidence={"path": rp},
                )
            )
            continue

        accepted.append(
            {
                "path": rp,
                "size": int(size),
                "mtime_ns": int(mtime_ns),
                "sha256": digest,
            }
        )

    accepted.sort(key=lambda a: a["path"])
    # Declaration is valid (machine marker accepted) even if some paths missing;
    # artifacts_declared=true only when marker itself is valid.
    return {
        "artifacts_declared": True,
        "owner_prefix": owner_prefix,
        "required_paths": list(required_paths),
        "max_no_progress_seconds": max_np_i,
        "artifacts": accepted,
        "artifact_digest": sha256_obj(accepted),
        "diagnostics": diagnostics,
        "declaration_sources": [
            {
                "source_kind": chosen["source_kind"],
                "source_id": chosen["source_id"],
                "line_sha256": chosen["line_sha256"],
            }
        ],
    }


def _as_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


# ---------------------------------------------------------------------------
# Resolution markers (parse only — no validity decisions beyond JSON)
# ---------------------------------------------------------------------------


def parse_resolution_markers(
    texts: Sequence[tuple[str, str, Any]],
    *,
    task_id: str,
) -> list[dict[str, Any]]:
    markers: list[dict[str, Any]] = []
    for source_kind, source_id, text in texts:
        if not text:
            continue
        for line_no, line in enumerate(str(text).splitlines(), 1):
            stripped = line.strip()
            if not stripped.startswith(RESOLUTION_PREFIX):
                continue
            raw = stripped[len(RESOLUTION_PREFIX) :]
            entry: dict[str, Any] = {
                "source_kind": source_kind,
                "source_id": str(source_id),
                "line_no": line_no,
                "line_sha256": sha256_text(stripped),
            }
            try:
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    entry["payload"] = redact_text(payload)
                    entry["parse_ok"] = True
                else:
                    entry["parse_ok"] = False
                    entry["parse_error"] = "not_object"
            except json.JSONDecodeError as exc:
                entry["parse_ok"] = False
                entry["parse_error"] = str(exc)
            markers.append(entry)
    return markers


# ---------------------------------------------------------------------------
# PID evidence
# ---------------------------------------------------------------------------


def _host_from_claim_lock(claim_lock: Any) -> Optional[str]:
    if not claim_lock or not isinstance(claim_lock, str):
        return None
    # format often "hostname:pid"
    if ":" in claim_lock:
        return claim_lock.rsplit(":", 1)[0] or None
    return claim_lock


def _declared_start_ticks(run_row: Optional[Mapping[str, Any]], events: Sequence[Mapping[str, Any]]) -> Optional[int]:
    if run_row:
        meta = run_row.get("metadata_obj")
        if isinstance(meta, dict):
            for k in ("start_ticks", "process_start_ticks", "pid_start_ticks"):
                v = _as_int(meta.get(k))
                if v is not None:
                    return v
    for ev in events:
        if ev.get("kind") not in ("spawned", "pid_bound", "process_bound"):
            continue
        payload = ev.get("payload_obj")
        if isinstance(payload, dict):
            for k in ("start_ticks", "process_start_ticks", "pid_start_ticks"):
                v = _as_int(payload.get(k))
                if v is not None:
                    return v
    return None


def capture_pid_evidence(
    *,
    task_row: Mapping[str, Any],
    current_run: Optional[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    proc_reader: ProcReader,
    now_ns: int,
) -> dict[str, Any]:
    """Bind pid evidence to the current run when possible.

    States: absent | alive | dead | reused | foreign | permission_unknown | unknown
    """
    _ = now_ns
    run_id = None
    if current_run is not None:
        run_id = _as_int(current_run.get("id"))
    if run_id is None:
        run_id = _as_int(task_row.get("current_run_id"))

    pid = None
    if current_run is not None:
        pid = _as_int(current_run.get("worker_pid"))
    if pid is None:
        pid = _as_int(task_row.get("worker_pid"))

    host = None
    if current_run is not None:
        host = _host_from_claim_lock(current_run.get("claim_lock"))
    if host is None:
        host = _host_from_claim_lock(task_row.get("claim_lock"))

    declared_ticks = _declared_start_ticks(current_run, events)

    base = {
        "pid": pid,
        "run_id": run_id,
        "host": host,
        "declared_start_ticks": declared_ticks,
        "observed_start_ticks": None,
        "bound_to_run": False,
        "pid_state": "absent",
        "proc_error": None,
    }

    if pid is None or pid <= 0:
        base["pid_state"] = "absent"
        return base

    # Without a run_id we cannot bind pid to a run → foreign if process exists?
    # Spec: capture only when host, pid, start ticks, run_id bound to same run.
    try:
        info = proc_reader(int(pid))
    except PermissionError as exc:
        base["pid_state"] = "permission_unknown"
        base["proc_error"] = str(exc)
        return base
    except Exception as exc:  # unexpected reader failure
        base["pid_state"] = "unknown"
        base["proc_error"] = f"{type(exc).__name__}:{exc}"
        return base

    if info is None:
        base["pid_state"] = "dead"
        base["bound_to_run"] = False
        return base

    observed = None
    if isinstance(info, Mapping):
        observed = _as_int(info.get("start_ticks"))
    base["observed_start_ticks"] = observed

    if run_id is None:
        # pid exists but cannot bind to run
        base["pid_state"] = "foreign"
        base["bound_to_run"] = False
        return base

    if declared_ticks is not None and observed is not None and int(declared_ticks) != int(observed):
        base["pid_state"] = "reused"
        base["bound_to_run"] = False
        return base

    # If host is declared empty we still allow bind when pid+run_id present.
    # foreign: process alive but run status not running / pid not matching run
    run_status = None
    if current_run is not None:
        run_status = current_run.get("status")
    run_pid = _as_int(current_run.get("worker_pid")) if current_run else None
    if run_pid is not None and int(run_pid) != int(pid):
        base["pid_state"] = "foreign"
        base["bound_to_run"] = False
        return base
    if run_status is not None and str(run_status) not in ("running",):
        # historical pid on non-running run
        base["pid_state"] = "foreign"
        base["bound_to_run"] = False
        return base

    # Bound: same run_id + pid (+ matching ticks when declared)
    base["bound_to_run"] = True
    base["pid_state"] = "alive"
    return base


# ---------------------------------------------------------------------------
# Failure signature / protocol
# ---------------------------------------------------------------------------


def _normalize_sig(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        text = canonical_json(value)
    else:
        text = str(value)
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w.\-:/|=+@ ]+", "", text)
    return text.strip()


def _infer_failure_signature(
    task_row: Mapping[str, Any],
    runs: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
) -> Optional[str]:
    candidates: list[str] = []
    if task_row.get("last_failure_error"):
        candidates.append(str(task_row.get("last_failure_error")))
    # newest run with error/outcome
    for r in sorted(runs, key=lambda x: int(x.get("id") or 0), reverse=True):
        if r.get("error"):
            candidates.append(str(r.get("error")))
            break
    for r in sorted(runs, key=lambda x: int(x.get("id") or 0), reverse=True):
        if r.get("outcome"):
            candidates.append(f"outcome:{r.get('outcome')}")
            break
    for ev in sorted(events, key=lambda e: int(e.get("id") or 0), reverse=True):
        if ev.get("kind") in ("failed", "crashed", "timed_out", "blocked", "reclaimed"):
            payload = ev.get("payload_obj")
            if isinstance(payload, dict) and payload.get("reason"):
                candidates.append(str(payload.get("reason")))
            elif ev.get("payload"):
                candidates.append(str(ev.get("payload"))[:500])
            else:
                candidates.append(str(ev.get("kind")))
            break
    for c in candidates:
        # redact first so signatures never retain secret material
        sig = _normalize_sig(str(redact_text(c)))
        if sig:
            return sig
    return None


def _infer_protocol(
    task_row: Mapping[str, Any],
    runs: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
) -> tuple[bool, Optional[str], Optional[str]]:
    """Return (protocol_violation, protocol_signature, run_outcome)."""
    # Look at latest ended run
    ended = [r for r in runs if r.get("ended_at") is not None or r.get("status") not in (None, "running")]
    ended.sort(key=lambda r: int(r.get("id") or 0), reverse=True)
    if not ended:
        return False, None, None
    r = ended[0]
    outcome = r.get("outcome")
    status = r.get("status")
    err = (r.get("error") or "") if isinstance(r.get("error"), str) else ""
    summary = (r.get("summary") or "") if isinstance(r.get("summary"), str) else ""

    run_outcome = None
    if outcome:
        run_outcome = str(outcome)
    elif status and status not in ("running",):
        run_outcome = str(status)

    # Machine signal: metadata flag
    meta_obj = r.get("metadata_obj")
    meta: dict[str, Any] = meta_obj if isinstance(meta_obj, dict) else {}
    if meta.get("protocol_violation") is True or meta.get("protocol_no_complete") is True:
        return True, _normalize_sig(meta.get("protocol_signature") or "protocol_no_complete"), "protocol_no_complete"

    blob = f"{err} {summary} {outcome or ''}".lower()
    if "protocol_no_complete" in blob or "without kanban complete" in blob or "rc=0 without" in blob:
        return True, "protocol_no_complete", "protocol_no_complete"

    # Event marker
    for ev in events:
        if ev.get("kind") in ("protocol_violation", "protocol_no_complete"):
            return True, _normalize_sig(ev.get("kind")), "protocol_no_complete"
        payload = ev.get("payload_obj")
        if isinstance(payload, dict) and payload.get("protocol_violation"):
            return True, _normalize_sig(payload.get("signature") or "protocol_no_complete"), "protocol_no_complete"

    return False, None, run_outcome


def _block_reason_from(
    task_row: Mapping[str, Any],
    runs: Sequence[Mapping[str, Any]],
    comments: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
) -> Optional[str]:
    # Prefer latest blocked run summary / error
    for r in sorted(runs, key=lambda x: int(x.get("id") or 0), reverse=True):
        if r.get("status") == "blocked" or r.get("outcome") == "blocked":
            if r.get("summary"):
                return str(r.get("summary"))
            if r.get("error"):
                return str(r.get("error"))
    for ev in sorted(events, key=lambda e: int(e.get("id") or 0), reverse=True):
        if ev.get("kind") == "blocked":
            payload = ev.get("payload_obj")
            if isinstance(payload, dict):
                if payload.get("reason"):
                    return str(payload.get("reason"))
            if ev.get("payload"):
                return str(ev.get("payload"))[:1000]
    # last comment by worker when status blocked
    if str(task_row.get("status") or "") == "blocked":
        for c in sorted(comments, key=lambda x: int(x.get("id") or 0), reverse=True):
            if c.get("body"):
                return str(c.get("body"))[:2000]
    return None


# ---------------------------------------------------------------------------
# Task snapshot assembly
# ---------------------------------------------------------------------------


def _immutable_sources(
    *,
    task_id: str,
    body: Optional[str],
    comments: Sequence[Mapping[str, Any]],
    runs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    if body is not None:
        red = redact_text(body)
        sources.append(
            {
                "source_kind": "body",
                "source_id": f"body:{task_id}",
                "source_sha256": sha256_text(str(red)),
            }
        )
    for c in comments:
        cid = c.get("id")
        red = redact_text(c.get("body") or "")
        sources.append(
            {
                "source_kind": "comment",
                "source_id": str(cid),
                "source_sha256": sha256_text(str(red)),
                "author": redact_text(c.get("author")),
                "created_at_ns": _to_ns(c.get("created_at")),
            }
        )
    for r in runs:
        meta_raw = r.get("metadata")
        if meta_raw is None:
            continue
        red = redact_text(meta_raw if not isinstance(r.get("metadata_obj"), dict) else r.get("metadata_obj"))
        sources.append(
            {
                "source_kind": "run_metadata",
                "source_id": str(r.get("id")),
                "source_sha256": sha256_obj(red) if not isinstance(red, str) else sha256_text(red),
            }
        )
    return sources


def _event_records(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for e in events:
        payload = e.get("payload")
        payload_obj = e.get("payload_obj")
        if payload_obj is not None:
            dig_src = redact_text(payload_obj)
            digest = sha256_obj(dig_src)
        elif payload is not None:
            dig_src = redact_text(payload)
            digest = sha256_text(str(dig_src))
        else:
            digest = sha256_text("")
        out.append(
            {
                "id": e.get("id"),
                "kind": e.get("kind"),
                "run_id": e.get("run_id"),
                "created_at": e.get("created_at"),
                "created_at_ns": _to_ns(e.get("created_at")),
                "digest": digest,
            }
        )
    return out


def _tools_digest_from_events(event_records: Sequence[Mapping[str, Any]]) -> str:
    """Digest of non-heartbeat progress-ish event kinds for tool/event delta."""
    interesting = []
    for e in event_records:
        kind = str(e.get("kind") or "")
        if kind in ("heartbeat",):
            continue
        interesting.append(
            {"id": e.get("id"), "kind": kind, "digest": e.get("digest")}
        )
    return sha256_obj(interesting)


def _machine_tags_from_texts(texts: Iterable[str]) -> list[str]:
    tags: list[str] = []
    # Exact machine tags: lines like TAG=value or known tokens — keep minimal.
    tag_line = re.compile(r"^ANTI_STALL_TAG_V1=(\S+)\s*$")
    for text in texts:
        if not text:
            continue
        for line in str(text).splitlines():
            m = tag_line.match(line.strip())
            if m:
                tags.append(m.group(1))
    # stable unique
    seen = set()
    out = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _build_task_snapshot(
    *,
    board_id: str,
    db_identity: str,
    task_row: Mapping[str, Any],
    parents: list[str],
    children: list[dict[str, Any]],
    comments: list[dict[str, Any]],
    events: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    proc_reader: ProcReader,
    now_ns: int,
) -> dict[str, Any]:
    task_id = str(task_row.get("id"))
    body = task_row.get("body")
    status = task_row.get("status")
    assignee = task_row.get("assignee")

    # Prepare run objects with parsed metadata
    run_objs: list[dict[str, Any]] = []
    for r in runs:
        rr = dict(r)
        rr["metadata_obj"] = _safe_json_loads(r.get("metadata")) if not isinstance(r.get("metadata"), dict) else r.get("metadata")
        run_objs.append(rr)

    event_objs: list[dict[str, Any]] = []
    for e in events:
        ee = dict(e)
        ee["payload_obj"] = _safe_json_loads(e.get("payload"))
        event_objs.append(ee)

    comment_objs = [dict(c) for c in comments]

    current_run_id = _as_int(task_row.get("current_run_id"))
    current_run = None
    for r in run_objs:
        if _as_int(r.get("id")) == current_run_id:
            current_run = r
            break
    if current_run is None and run_objs:
        # fallback latest
        current_run = sorted(run_objs, key=lambda x: int(x.get("id") or 0))[-1]

    texts_for_markers: list[tuple[str, str, Any]] = []
    if body:
        texts_for_markers.append(("body", f"body:{task_id}", body))
    for c in comment_objs:
        texts_for_markers.append(("comment", str(c.get("id")), c.get("body")))
    for r in run_objs:
        if r.get("summary"):
            texts_for_markers.append(("run_summary", str(r.get("id")), r.get("summary")))
        if isinstance(r.get("metadata_obj"), dict):
            # allow marker embedded as metadata string field
            for k, v in r["metadata_obj"].items():
                if isinstance(v, str) and (
                    ARTIFACT_PREFIX in v or RESOLUTION_PREFIX in v or v.startswith(ARTIFACT_PREFIX)
                ):
                    texts_for_markers.append(("run_metadata", f"{r.get('id')}:{k}", v))
        elif r.get("metadata"):
            texts_for_markers.append(("run_metadata", str(r.get("id")), r.get("metadata")))

    art = parse_artifact_declaration(texts_for_markers, task_id=task_id)
    resolution_markers = parse_resolution_markers(texts_for_markers, task_id=task_id)

    pid_ev = capture_pid_evidence(
        task_row=task_row,
        current_run=current_run,
        events=event_objs,
        proc_reader=proc_reader,
        now_ns=now_ns,
    )

    event_records = _event_records(event_objs)
    events_digest = sha256_obj(
        [
            {
                "id": e.get("id"),
                "kind": e.get("kind"),
                "ts": e.get("created_at_ns") or e.get("created_at"),
                "digest": e.get("digest"),
            }
            for e in event_records
        ]
    )
    tools_digest = _tools_digest_from_events(event_records)
    artifact_digest = art["artifact_digest"]

    result = task_row.get("result")
    if current_run and current_run.get("summary") and not result:
        # do not promote summary to result; keep task.result only
        pass

    progress_payload = {
        "artifact_digest": artifact_digest,
        "events_digest": events_digest,
        "tools_digest": tools_digest,
        "result": redact_text(result),
        "status": status,
        "current_run_id": current_run_id,
    }
    progress_digest = sha256_obj(progress_payload)

    block_reason_raw = _block_reason_from(task_row, run_objs, comment_objs, event_objs)
    block_reason = redact_text(block_reason_raw) if block_reason_raw else None
    block_kind = task_row.get("block_kind")
    block_signature = (
        _normalize_sig(block_reason)
        if block_reason
        else _normalize_sig(redact_text(block_kind) if block_kind else "")
    )

    failure_signature = _infer_failure_signature(task_row, run_objs, event_objs)
    protocol_violation, protocol_signature, run_outcome = _infer_protocol(
        task_row, run_objs, event_objs
    )

    hb_raw = task_row.get("last_heartbeat_at")
    if hb_raw is None and current_run is not None:
        hb_raw = current_run.get("last_heartbeat_at")
    hb_ns = _to_ns(hb_raw)
    # heartbeat_stale is NOT decided here with thresholds — only expose raw ages;
    # decision policy owns stale classification. Provide null flag unless clearly
    # no heartbeat while running (still not a threshold decision). Leave False/None.
    heartbeat_stale = None

    attempt_count = len(run_objs)
    consecutive_failures = _as_int(task_row.get("consecutive_failures"))

    immutable_sources = _immutable_sources(
        task_id=task_id,
        body=body,
        comments=comment_objs,
        runs=run_objs,
    )

    # planner child state: structured children with status/assignee
    planner_child_state = {
        "count": len(children),
        "children": children,
        "has_executable_children": any(
            str(c.get("status") or "") in ("todo", "ready", "running", "done", "review", "blocked")
            for c in children
        ),
    }

    machine_tags = _machine_tags_from_texts(
        [body or ""] + [str(c.get("body") or "") for c in comment_objs]
    )

    evidence_digest = sha256_obj(
        {
            "progress": progress_digest,
            "block_signature": block_signature or "",
            "status": status,
            "run_id": current_run_id,
        }
    )

    # runs summary (redacted)
    runs_out = []
    for r in sorted(run_objs, key=lambda x: int(x.get("id") or 0)):
        runs_out.append(
            {
                "id": r.get("id"),
                "status": r.get("status"),
                "outcome": r.get("outcome"),
                "worker_pid": r.get("worker_pid"),
                "started_at_ns": _to_ns(r.get("started_at")),
                "ended_at_ns": _to_ns(r.get("ended_at")),
                "last_heartbeat_at_ns": _to_ns(r.get("last_heartbeat_at")),
                "error_digest": sha256_text(str(redact_text(r.get("error") or ""))),
                "summary_digest": sha256_text(str(redact_text(r.get("summary") or ""))),
                "metadata_digest": sha256_obj(redact_text(r.get("metadata_obj")))
                if r.get("metadata_obj") is not None
                else (sha256_text(str(redact_text(r.get("metadata") or ""))) if r.get("metadata") else None),
            }
        )

    task_diag = list(art.get("diagnostics") or [])

    out: dict[str, Any] = {
        "board_id": board_id,
        "db_identity": db_identity,
        "task_id": task_id,
        "status": status,
        "assignee": assignee,
        "title_digest": sha256_text(str(redact_text(task_row.get("title") or ""))),
        "block_kind": block_kind,
        "block_reason": block_reason,
        "block_signature": block_signature or None,
        "block_recurrences": _as_int(task_row.get("block_recurrences")),
        "current_run_id": current_run_id,
        "attempt_count": attempt_count,
        "consecutive_failures": consecutive_failures,
        "failure_signature": failure_signature,
        "result": redact_text(result),
        "last_heartbeat_at": hb_raw,
        "last_heartbeat_at_ns": hb_ns,
        "heartbeat_stale": heartbeat_stale,
        "pid_state": pid_ev["pid_state"],
        "pid_evidence": pid_ev,
        "parents": list(parents),
        "children": children,
        "planner_child_state": planner_child_state,
        "artifacts_declared": bool(art["artifacts_declared"]),
        "owner_prefix": art.get("owner_prefix"),
        "required_paths": art.get("required_paths") or [],
        "max_no_progress_seconds": art.get("max_no_progress_seconds"),
        "artifacts": art.get("artifacts") or [],
        "artifact_digest": artifact_digest,
        "events": event_records,
        "events_digest": events_digest,
        "tools_digest": tools_digest,
        "progress_digest": progress_digest,
        "evidence_digest": evidence_digest,
        "immutable_sources": immutable_sources,
        "resolution_markers": resolution_markers,
        "machine_tags": machine_tags,
        "protocol_violation": bool(protocol_violation),
        "protocol_signature": protocol_signature,
        "run_outcome": run_outcome,
        "runs": runs_out,
        "created_at_ns": _to_ns(task_row.get("created_at")),
        "started_at_ns": _to_ns(task_row.get("started_at")),
        "completed_at_ns": _to_ns(task_row.get("completed_at")),
        "diagnostics": task_diag,
    }
    return out


# ---------------------------------------------------------------------------
# Board / collect
# ---------------------------------------------------------------------------


def read_board_snapshot(
    db_path: Path,
    *,
    now_ns: int,
    proc_reader: ProcReader,
    board_id: Optional[str] = None,
    hermes_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Read one board DB into a board snapshot dict. Never writes."""
    db_path = Path(db_path)
    diagnostics: list[dict] = []
    if board_id is None:
        if hermes_root is not None:
            board_id = _board_id_for_db(hermes_root, db_path)
        else:
            board_id = db_path.parent.name if db_path.name == "kanban.db" else db_path.stem

    try:
        db_identity = str(_resolve_path(db_path))
    except Exception:
        db_identity = str(db_path)

    if not db_path.is_file():
        return {
            "board_id": board_id,
            "db_path": str(db_path),
            "db_identity": db_identity,
            "ok": False,
            "tasks": [],
            "diagnostics": [
                _diag("db_missing", f"database file missing: {db_path}", board_id=board_id)
            ],
        }

    try:
        con = _connect_ro(db_path)
    except sqlite3.Error as exc:
        return {
            "board_id": board_id,
            "db_path": str(db_path),
            "db_identity": db_identity,
            "ok": False,
            "tasks": [],
            "diagnostics": [
                _diag(
                    "db_open_error",
                    f"sqlite open failed: {exc}",
                    board_id=board_id,
                    evidence={"error_type": type(exc).__name__},
                )
            ],
        }

    try:
        # quick integrity-ish: read schema
        try:
            ok, missing = _schema_ok(con)
        except sqlite3.DatabaseError as exc:
            return {
                "board_id": board_id,
                "db_path": str(db_path),
                "db_identity": db_identity,
                "ok": False,
                "tasks": [],
                "diagnostics": [
                    _diag(
                        "db_corrupt",
                        f"sqlite database error: {exc}",
                        board_id=board_id,
                    )
                ],
            }
        if not ok:
            return {
                "board_id": board_id,
                "db_path": str(db_path),
                "db_identity": db_identity,
                "ok": False,
                "tasks": [],
                "diagnostics": [
                    _diag(
                        "unknown_schema",
                        "required kanban tables/columns missing",
                        board_id=board_id,
                        evidence={"missing": missing},
                    )
                ],
            }

        try:
            task_rows = list(
                con.execute(
                    "SELECT * FROM tasks WHERE status != 'archived' ORDER BY id"
                )
            )
        except sqlite3.Error as exc:
            msg = str(exc).lower()
            if "locked" in msg:
                code = "db_locked"
            elif isinstance(exc, sqlite3.DatabaseError):
                code = "db_corrupt"
            else:
                code = "db_operational_error"
            return {
                "board_id": board_id,
                "db_path": str(db_path),
                "db_identity": db_identity,
                "ok": False,
                "tasks": [],
                "diagnostics": [_diag(code, str(exc), board_id=board_id)],
            }

        # links
        try:
            links = list(con.execute("SELECT parent_id, child_id FROM task_links"))
        except sqlite3.Error as exc:
            diagnostics.append(
                _diag("links_unreadable", str(exc), board_id=board_id)
            )
            links = []

        parents_map: dict[str, list[str]] = {}
        children_ids_map: dict[str, list[str]] = {}
        for lk in links:
            p, c = lk["parent_id"], lk["child_id"]
            parents_map.setdefault(c, []).append(p)
            children_ids_map.setdefault(p, []).append(c)
        for k in parents_map:
            parents_map[k] = sorted(set(parents_map[k]))
        for k in children_ids_map:
            children_ids_map[k] = sorted(set(children_ids_map[k]))

        # index all tasks (including archived) for child status lookup
        try:
            all_tasks = {
                r["id"]: dict(r)
                for r in con.execute("SELECT id, status, assignee, title FROM tasks")
            }
        except sqlite3.Error:
            all_tasks = {r["id"]: dict(r) for r in task_rows}

        tasks_out: list[dict] = []
        for tr in task_rows:
            t = dict(tr)
            tid = t["id"]
            try:
                comments = [
                    dict(r)
                    for r in con.execute(
                        "SELECT id, task_id, author, body, created_at FROM task_comments "
                        "WHERE task_id = ? ORDER BY id",
                        (tid,),
                    )
                ]
            except sqlite3.Error as exc:
                diagnostics.append(
                    _diag("comments_unreadable", str(exc), board_id=board_id, task_id=tid)
                )
                comments = []
            try:
                events = [
                    dict(r)
                    for r in con.execute(
                        "SELECT id, task_id, run_id, kind, payload, created_at FROM task_events "
                        "WHERE task_id = ? ORDER BY id",
                        (tid,),
                    )
                ]
            except sqlite3.Error as exc:
                diagnostics.append(
                    _diag("events_unreadable", str(exc), board_id=board_id, task_id=tid)
                )
                events = []
            try:
                runs = [
                    dict(r)
                    for r in con.execute(
                        "SELECT * FROM task_runs WHERE task_id = ? ORDER BY id",
                        (tid,),
                    )
                ]
            except sqlite3.Error as exc:
                diagnostics.append(
                    _diag("runs_unreadable", str(exc), board_id=board_id, task_id=tid)
                )
                runs = []

            child_structs = []
            for cid in children_ids_map.get(tid, []):
                info = all_tasks.get(cid) or {"id": cid}
                child_structs.append(
                    {
                        "task_id": cid,
                        "status": info.get("status"),
                        "assignee": info.get("assignee"),
                    }
                )

            try:
                task_snap = _build_task_snapshot(
                    board_id=board_id,
                    db_identity=db_identity,
                    task_row=t,
                    parents=parents_map.get(tid, []),
                    children=child_structs,
                    comments=comments,
                    events=events,
                    runs=runs,
                    proc_reader=proc_reader,
                    now_ns=now_ns,
                )
                tasks_out.append(task_snap)
            except Exception as exc:
                diagnostics.append(
                    _diag(
                        "task_snapshot_error",
                        f"{type(exc).__name__}: {exc}",
                        board_id=board_id,
                        task_id=tid,
                    )
                )

        return {
            "board_id": board_id,
            "db_path": str(db_path),
            "db_identity": db_identity,
            "ok": True,
            "task_count": len(tasks_out),
            "tasks": tasks_out,
            "diagnostics": diagnostics,
        }
    finally:
        try:
            con.close()
        except Exception:
            pass


def collect_snapshot(
    hermes_root: Path,
    *,
    now_ns: int,
    proc_reader: ProcReader,
) -> dict[str, Any]:
    """Discover boards and collect a full snapshot_v1 document."""
    root = Path(hermes_root)
    diagnostics: list[dict] = []
    boards: list[dict] = []

    # Surface archive/discovery diagnostics for candidates not included
    for cand in _enumerate_board_candidates(root):
        bid = cand["board_id"]
        if cand.get("meta_error") == "board_json_malformed":
            diagnostics.append(
                _diag(
                    "archive_state_unknown",
                    "board.json malformed; board excluded",
                    board_id=bid,
                    evidence={"meta_path": str(cand.get("meta_path"))},
                )
            )
        elif not cand.get("archive_verifiable"):
            if cand.get("db_path") is not None or (cand.get("meta_path") and Path(cand["meta_path"]).exists()):
                diagnostics.append(
                    _diag(
                        "archive_state_unknown",
                        "archive state not machine-verifiable; board excluded",
                        board_id=bid,
                        evidence={
                            "meta_error": cand.get("meta_error"),
                            "db_path": str(cand.get("db_path")) if cand.get("db_path") else None,
                        },
                    )
                )
        elif cand.get("archived") is True:
            diagnostics.append(
                _diag(
                    "board_archived_excluded",
                    "board archived; excluded from snapshot",
                    board_id=bid,
                )
            )

    try:
        db_paths = discover_boards(root)
    except Exception as exc:
        diagnostics.append(
            _diag("discover_failed", f"{type(exc).__name__}: {exc}")
        )
        db_paths = []

    for db in db_paths:
        bid = _board_id_for_db(root, db)
        try:
            board_snap = read_board_snapshot(
                db,
                now_ns=now_ns,
                proc_reader=proc_reader,
                board_id=bid,
                hermes_root=root,
            )
            boards.append(board_snap)
            # hoist board diagnostics
            for d in board_snap.get("diagnostics") or []:
                if d not in diagnostics:
                    diagnostics.append(d)
        except Exception as exc:
            diagnostics.append(
                _diag(
                    "board_snapshot_error",
                    f"{type(exc).__name__}: {exc}",
                    board_id=bid,
                    evidence={"db_path": str(db)},
                )
            )

    boards.sort(key=lambda b: str(b.get("board_id") or ""))

    digest_payload = {
        "schema": SCHEMA_NAME,
        "version": SCHEMA_VERSION,
        "boards": [
            {
                "board_id": b.get("board_id"),
                "db_identity": b.get("db_identity"),
                "ok": b.get("ok"),
                "tasks": b.get("tasks") or [],
            }
            for b in boards
        ],
    }
    snapshot_digest = sha256_obj(digest_payload)

    doc = {
        "schema": SCHEMA_NAME,
        "version": SCHEMA_VERSION,
        "now_ns": int(now_ns),
        "hermes_root": str(root),
        "boards": boards,
        "diagnostics": diagnostics,
        "snapshot_digest": snapshot_digest,
        "digest": snapshot_digest,
        "board_count": len(boards),
        "task_count": sum(len(b.get("tasks") or []) for b in boards),
    }
    return doc


# ---------------------------------------------------------------------------
# Fixture helpers for --self-test / unit tests
# ---------------------------------------------------------------------------


_MIN_SCHEMA_SQL = """
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT,
    assignee TEXT,
    status TEXT NOT NULL,
    priority INTEGER DEFAULT 0,
    created_by TEXT,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    workspace_kind TEXT NOT NULL DEFAULT 'scratch',
    workspace_path TEXT,
    branch_name TEXT,
    project_id TEXT,
    claim_lock TEXT,
    claim_expires INTEGER,
    tenant TEXT,
    result TEXT,
    idempotency_key TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    worker_pid INTEGER,
    last_failure_error TEXT,
    max_runtime_seconds INTEGER,
    last_heartbeat_at INTEGER,
    current_run_id INTEGER,
    workflow_template_id TEXT,
    current_step_key TEXT,
    skills TEXT,
    model_override TEXT,
    max_retries INTEGER,
    goal_mode INTEGER NOT NULL DEFAULT 0,
    goal_max_turns INTEGER,
    session_id TEXT,
    block_kind TEXT,
    block_recurrences INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE task_links (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    PRIMARY KEY (parent_id, child_id)
);
CREATE TABLE task_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    author TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    run_id INTEGER,
    kind TEXT NOT NULL,
    payload TEXT,
    created_at INTEGER NOT NULL
);
CREATE TABLE task_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    profile TEXT,
    step_key TEXT,
    status TEXT NOT NULL,
    claim_lock TEXT,
    claim_expires INTEGER,
    worker_pid INTEGER,
    max_runtime_seconds INTEGER,
    last_heartbeat_at INTEGER,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    outcome TEXT,
    summary TEXT,
    metadata TEXT,
    error TEXT
);
"""


def write_min_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(_MIN_SCHEMA_SQL)
        con.commit()
    finally:
        con.close()


def _self_test() -> int:
    """Build temporary fixtures and print fixture counts only."""
    import tempfile

    now = 1_700_000_000
    now_ns = now * NS_PER_SEC

    class Counts:
        boards = 0
        tasks = 0
        fixtures = 0

    with tempfile.TemporaryDirectory(prefix="snap_selftest_") as td:
        root = Path(td) / ".hermes"
        root.mkdir()
        # default board
        write_min_schema(root / "kanban.db")
        con = sqlite3.connect(str(root / "kanban.db"))
        con.execute(
            "INSERT INTO tasks (id,title,body,assignee,status,created_at) VALUES (?,?,?,?,?,?)",
            ("t_a", "A", "body", "worker", "running", now),
        )
        con.execute(
            "INSERT INTO tasks (id,title,body,assignee,status,created_at) VALUES (?,?,?,?,?,?)",
            ("t_arch", "Arch", "x", "worker", "archived", now),
        )
        con.commit()
        con.close()
        Counts.fixtures += 1

        # named active board
        bdir = root / "kanban" / "boards" / "active1"
        bdir.mkdir(parents=True)
        (bdir / "board.json").write_text(
            json.dumps({"slug": "active1", "archived": False}), encoding="utf-8"
        )
        write_min_schema(bdir / "kanban.db")
        con = sqlite3.connect(str(bdir / "kanban.db"))
        con.execute(
            "INSERT INTO tasks (id,title,body,assignee,status,created_at) VALUES (?,?,?,?,?,?)",
            ("t_b", "B", "body", "worker", "todo", now),
        )
        con.commit()
        con.close()
        Counts.fixtures += 1

        # archived board
        adir = root / "kanban" / "boards" / "old"
        adir.mkdir(parents=True)
        (adir / "board.json").write_text(
            json.dumps({"slug": "old", "archived": True}), encoding="utf-8"
        )
        write_min_schema(adir / "kanban.db")
        Counts.fixtures += 1

        def proc_reader(_pid: int) -> Optional[dict]:
            return None

        snap = collect_snapshot(root, now_ns=now_ns, proc_reader=proc_reader)
        Counts.boards = snap.get("board_count") or len(snap.get("boards") or [])
        Counts.tasks = snap.get("task_count") or 0
        assert snap.get("schema") == SCHEMA_NAME
        assert Counts.boards == 2  # default + active1
        # archived task excluded
        default = next(b for b in snap["boards"] if b["board_id"] == "default")
        ids = {t["task_id"] for t in default["tasks"]}
        assert "t_a" in ids and "t_arch" not in ids

    # stdout: fixture counts only
    print(
        json.dumps(
            {
                "self_test": "ok",
                "fixture_count": Counts.fixtures,
                "board_count": Counts.boards,
                "task_count": Counts.tasks,
            },
            separators=(",", ":"),
        )
    )
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help"):
        print(
            "usage: snapshot.py --self-test | --collect <hermes_root>",
            file=sys.stderr,
        )
        return 2
    if argv[0] == "--self-test":
        return _self_test()
    if argv[0] == "--collect" and len(argv) >= 2:
        root = Path(argv[1])
        now_ns = time.time_ns()
        snap = collect_snapshot(root, now_ns=now_ns, proc_reader=default_proc_reader)
        json.dump(snap, sys.stdout, ensure_ascii=False, sort_keys=True)
        sys.stdout.write("\n")
        return 0
    print("unknown args", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
