"""Read-only local execution-ledger audit and normalizer.

Bounded archival inventory for real execution economics fields keyed by
canonical map_id. Does NOT compute ROI/EV/CLV/WR or probe live systems.

Public surface:
  - normalize_execution_row / normalize_execution_rows
  - inspect_sqlite_schema / inventory_execution_ledger
  - CLI: --inventory-only --root --output
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Constants / schema hooks
# ---------------------------------------------------------------------------

SETTLEMENT_ENUM = frozenset({"win", "loss", "push", "void", "pending", "unknown"})

# Canonical economics fields we care about (safe names only).
ECONOMICS_FIELDS = (
    "map_id",
    "execution_odds",
    "closing_odds",
    "stake",
    "settlement",
    "outcome",  # alias accepted as settlement source
    "pnl",
    "bookmaker",
    "market",
    "currency",
    "placed_at",
    "provenance",
)

REQUIRED_FOR_ACCEPT = ("map_id", "execution_odds", "stake", "settlement", "provenance")

# Relative path segments / names that must never be scanned as sources.
DEFAULT_EXCLUSIONS = (
    "venv",
    ".git",
    "runtime/star_dispatch_replay/final",
    ".env",
    "base/keys.py",
    "keys.py",
    "credentials",
    "tokens",
    "__pycache__",
    "node_modules",
    ".hermes",
)

# Safe scopes under project root to inventory (relative).
DEFAULT_SEARCH_SCOPES = (
    "bets_data",
    "runtime",
    "base",
)

# Filename / path hints that look like execution ledgers (not pub match dicts).
LEDGER_NAME_HINTS = re.compile(
    r"(ledger|execution.?bet|logged.?bet|settled.?bet|bet.?log|stake.?log|"
    r"execution_ledger|betting.?history|placed.?bet)",
    re.I,
)

# Extensions we inventory.
SCAN_SUFFIXES = {".json", ".jsonl", ".csv", ".sqlite", ".sqlite3", ".db"}

# Fields considered secret-like — never echo values into reports.
SECRET_FIELD_HINTS = re.compile(
    r"(password|passwd|secret|token|api[_-]?key|proxy|auth|credential|cookie|jwt|bearer)",
    re.I,
)

SCHEMA_HOOKS = {
    "canonical_key": "map_id",
    "required_fields": list(REQUIRED_FOR_ACCEPT),
    "optional_fields": [
        "closing_odds",
        "pnl",
        "bookmaker",
        "market",
        "currency",
        "placed_at",
    ],
    "settlement_enum": sorted(SETTLEMENT_ENUM),
    "odds_constraint": "decimal_odds > 1",
    "stake_constraint": "nonnegative_number",
    "notes": (
        "Accept source only when map_id + provenance are verifiable. "
        "Do not infer missing stakes/odds/settlements/CLV/pnl."
    ),
}


# ---------------------------------------------------------------------------
# Path safety
# ---------------------------------------------------------------------------


def is_excluded_path(path: Path, root: Path) -> bool:
    """Return True if path falls under a forbidden scope."""
    try:
        rel = path.resolve().relative_to(root.resolve())
    except Exception:
        # Outside root — treat as excluded.
        return True
    rel_s = rel.as_posix()
    parts = rel.parts

    if any(part in {"venv", ".git", "__pycache__", "node_modules", ".hermes"} for part in parts):
        return True
    if rel_s.startswith("runtime/star_dispatch_replay/final"):
        return True
    if path.name == "keys.py" or rel_s.endswith("base/keys.py"):
        return True
    if path.name.startswith(".env"):
        return True
    # AppleDouble / resource-fork noise
    if path.name.startswith("._"):
        return True
    if any(p in {"credentials", "tokens"} for p in parts):
        return True
    # Exclude this module's own staging output from re-ingest loops.
    if "star_dispatch_replay/staging/ledger" in rel_s and path.suffix in {".json"}:
        # Allow scanning other staging dirs, but skip our own inventory outputs.
        if path.name.startswith("ledger_"):
            return True
    return False


def _file_sha256(path: Path, max_bytes: int = 32 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        remaining = max_bytes
        while remaining > 0:
            chunk = f.read(min(65536, remaining))
            if not chunk:
                break
            h.update(chunk)
            remaining -= len(chunk)
    return h.hexdigest()


def _schema_hash(field_names: Sequence[str]) -> str:
    payload = ",".join(sorted({str(x) for x in field_names}))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_settlement(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    s = str(value).strip().lower()
    aliases = {
        "w": "win",
        "won": "win",
        "l": "loss",
        "lost": "loss",
        "lose": "loss",
        "draw": "push",
        "tie": "push",
        "refund": "void",
        "cancelled": "void",
        "canceled": "void",
        "open": "pending",
        "unsettled": "pending",
    }
    s = aliases.get(s, s)
    if s in SETTLEMENT_ENUM:
        return s
    return None


def normalize_execution_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize one economics row. Never infers missing values.

    Returns:
      {"status": "ok", "record": {...}} or
      {"status": "quarantine", "reason": "...", "map_id": ...?}
    """
    if not isinstance(row, dict):
        return {"status": "quarantine", "reason": "not_a_mapping"}

    # map_id
    map_id = row.get("map_id")
    if map_id is None or str(map_id).strip() == "":
        return {"status": "quarantine", "reason": "missing_map_id"}
    map_id_s = str(map_id).strip()

    # provenance (required for acceptance)
    provenance = row.get("provenance")
    if provenance is None or str(provenance).strip() == "":
        return {"status": "quarantine", "reason": "missing_provenance", "map_id": map_id_s}
    provenance_s = str(provenance).strip()

    # execution_odds (> 1)
    if "execution_odds" not in row or row.get("execution_odds") is None or row.get("execution_odds") == "":
        return {"status": "quarantine", "reason": "missing_execution_odds", "map_id": map_id_s}
    odds = _as_float(row.get("execution_odds"))
    if odds is None:
        return {"status": "quarantine", "reason": "non_numeric_execution_odds", "map_id": map_id_s}
    if not (odds > 1.0):
        return {"status": "quarantine", "reason": "invalid_execution_odds", "map_id": map_id_s}

    # stake (>= 0), required — do not infer
    if "stake" not in row or row.get("stake") is None or row.get("stake") == "":
        return {"status": "quarantine", "reason": "missing_stake", "map_id": map_id_s}
    stake = _as_float(row.get("stake"))
    if stake is None:
        return {"status": "quarantine", "reason": "non_numeric_stake", "map_id": map_id_s}
    if stake < 0:
        return {"status": "quarantine", "reason": "negative_stake", "map_id": map_id_s}

    # settlement / outcome alias
    settlement_raw = row.get("settlement")
    if settlement_raw is None or settlement_raw == "":
        settlement_raw = row.get("outcome")
    if settlement_raw is None or settlement_raw == "":
        return {"status": "quarantine", "reason": "missing_settlement", "map_id": map_id_s}
    settlement = _normalize_settlement(settlement_raw)
    if settlement is None:
        return {"status": "quarantine", "reason": "invalid_settlement", "map_id": map_id_s}

    record: Dict[str, Any] = {
        "map_id": map_id_s,
        "execution_odds": float(odds),
        "stake": float(stake),
        "settlement": settlement,
        "provenance": provenance_s,
    }

    # optional closing_odds (> 1 if present)
    if "closing_odds" in row and row.get("closing_odds") not in (None, ""):
        closing = _as_float(row.get("closing_odds"))
        if closing is None or not (closing > 1.0):
            return {"status": "quarantine", "reason": "invalid_closing_odds", "map_id": map_id_s}
        record["closing_odds"] = float(closing)

    # optional pnl (must be numeric if present; do not invent)
    if "pnl" in row and row.get("pnl") not in (None, ""):
        pnl = _as_float(row.get("pnl"))
        if pnl is None:
            return {"status": "quarantine", "reason": "non_numeric_pnl", "map_id": map_id_s}
        record["pnl"] = float(pnl)

    for opt in ("bookmaker", "market", "currency", "placed_at"):
        if opt in row and row.get(opt) not in (None, ""):
            record[opt] = str(row.get(opt)).strip()

    return {"status": "ok", "record": record}


def _record_fingerprint(rec: Dict[str, Any]) -> str:
    """Stable fingerprint of economics fields for duplicate detection."""
    keys = (
        "map_id",
        "execution_odds",
        "closing_odds",
        "stake",
        "settlement",
        "pnl",
        "bookmaker",
        "market",
        "currency",
        "placed_at",
        "provenance",
    )
    payload = {k: rec.get(k) for k in keys if k in rec}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def normalize_execution_rows(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Normalize a batch; quarantine malformed and duplicate-conflicting rows."""
    accepted: List[Dict[str, Any]] = []
    quarantine: List[Dict[str, Any]] = []
    by_map: Dict[str, Dict[str, Any]] = {}
    identical_dupes = 0

    for idx, row in enumerate(rows):
        out = normalize_execution_row(row if isinstance(row, dict) else {})
        if out["status"] != "ok":
            quarantine.append(
                {
                    "index": idx,
                    "reason": out.get("reason", "unknown"),
                    "map_id": out.get("map_id"),
                }
            )
            continue
        rec = out["record"]
        mid = rec["map_id"]
        if mid in by_map:
            prev = by_map[mid]
            if _record_fingerprint(prev) == _record_fingerprint(rec):
                identical_dupes += 1
                continue
            # Conflict: quarantine both (remove previous from accepted).
            quarantine.append(
                {
                    "index": idx,
                    "reason": "duplicate_conflict",
                    "map_id": mid,
                }
            )
            # Also mark the previous accepted one as conflict if still present.
            for i, a in enumerate(list(accepted)):
                if a.get("map_id") == mid:
                    quarantine.append(
                        {
                            "index": f"prev:{mid}",
                            "reason": "duplicate_conflict",
                            "map_id": mid,
                        }
                    )
                    accepted.pop(i)
                    break
            by_map.pop(mid, None)
            continue
        by_map[mid] = rec
        accepted.append(rec)

    reason_counts = Counter(q["reason"] for q in quarantine)
    return {
        "accepted": accepted,
        "accepted_count": len(accepted),
        "quarantine": quarantine,
        "quarantine_count": len(quarantine),
        "quarantine_reasons": dict(reason_counts),
        "duplicate_identical_count": identical_dupes,
    }


# ---------------------------------------------------------------------------
# SQLite read-only inspection
# ---------------------------------------------------------------------------


def _open_sqlite_readonly(path: Path) -> sqlite3.Connection:
    """Open SQLite in immutable/read-only URI mode."""
    # mode=ro prevents writes; immutable=1 further hardens against WAL side effects.
    uri = path.resolve().as_uri() + "?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    con.execute("PRAGMA query_only = ON")
    return con


def inspect_sqlite_schema(path: Path) -> Dict[str, Any]:
    """Return safe schema metadata for a SQLite file (no row values)."""
    con = _open_sqlite_readonly(path)
    try:
        tables_meta: Dict[str, Any] = {}
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        all_cols: List[str] = []
        for (tname,) in tables:
            cols = [r[1] for r in con.execute(f'PRAGMA table_info("{tname}")').fetchall()]
            try:
                # Safe count only.
                cnt = con.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
            except Exception:
                cnt = None
            tables_meta[tname] = {"columns": cols, "row_count": cnt}
            all_cols.extend(cols)
        return {
            "format": "sqlite",
            "read_only": True,
            "path": str(path),
            "tables": tables_meta,
            "safe_fields": sorted(set(all_cols)),
            "schema_hash": _schema_hash(all_cols),
            "file_hash": _file_sha256(path),
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Source loaders (safe field extraction only for inventory)
# ---------------------------------------------------------------------------


def _looks_like_economics_mapping(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    keys = {str(k).lower() for k in obj.keys()}
    # Need map_id-ish + at least one economics signal.
    has_map = "map_id" in keys or "mapid" in keys
    has_econ = bool(
        keys
        & {
            "execution_odds",
            "closing_odds",
            "stake",
            "settlement",
            "pnl",
            "bookmaker",
            "odds",
            "placed_at",
        }
    )
    return has_map and has_econ


def _coerce_rows_from_json_obj(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        # Common wrappers
        for key in ("rows", "bets", "records", "items", "data", "resolved_rows", "ledger"):
            if isinstance(obj.get(key), list):
                return [x for x in obj[key] if isinstance(x, dict)]
        if _looks_like_economics_mapping(obj):
            return [obj]
    return []


def _load_candidate_rows(path: Path) -> Tuple[str, List[Dict[str, Any]], List[str], Optional[str]]:
    """Load rows from a candidate file.

    Returns (format, rows, safe_fields, error).
    Never returns secret field values in metadata paths — caller must not dump rows into inventory.
    """
    suffix = path.suffix.lower()
    try:
        if suffix == ".jsonl":
            rows: List[Dict[str, Any]] = []
            fields: set = set()
            with path.open("r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(obj, dict):
                        rows.append(obj)
                        fields.update(str(k) for k in obj.keys())
            return "jsonl", rows, sorted(fields), None

        if suffix == ".json":
            with path.open("r", encoding="utf-8", errors="replace") as f:
                obj = json.load(f)
            rows = _coerce_rows_from_json_obj(obj)
            fields = set()
            for r in rows:
                fields.update(str(k) for k in r.keys())
            # Also capture top-level keys if wrapper
            if isinstance(obj, dict) and not rows:
                fields.update(str(k) for k in obj.keys())
            return "json", rows, sorted(fields), None

        if suffix == ".csv":
            with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
                reader = csv.DictReader(f)
                fields_list: List[str] = list(reader.fieldnames or [])
                rows = [dict(r) for r in reader]
            return "csv", rows, fields_list, None

        if suffix in {".sqlite", ".sqlite3", ".db"}:
            meta = inspect_sqlite_schema(path)
            rows = _load_sqlite_economics_rows(path, meta)
            return "sqlite", rows, meta.get("safe_fields") or [], None

        return "unknown", [], [], f"unsupported_suffix:{suffix}"
    except Exception as exc:  # permission / corruption — record and continue
        return suffix.lstrip(".") or "unknown", [], [], f"{type(exc).__name__}:{exc}"


def _load_sqlite_economics_rows(path: Path, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pull rows only from tables that look like economics ledgers."""
    rows: List[Dict[str, Any]] = []
    tables = meta.get("tables") or {}
    econ_cols = {
        "map_id",
        "execution_odds",
        "closing_odds",
        "stake",
        "settlement",
        "outcome",
        "pnl",
        "bookmaker",
        "market",
        "currency",
        "placed_at",
        "provenance",
        "odds",
    }
    try:
        con = _open_sqlite_readonly(path)
    except Exception:
        return rows
    try:
        for tname, tmeta in tables.items():
            cols = [c.lower() for c in (tmeta.get("columns") or [])]
            col_set = set(cols)
            if "map_id" not in col_set:
                continue
            if not (col_set & econ_cols - {"map_id"}):
                continue
            # Select only known safe columns (preserve original case from pragma).
            original_cols = tmeta.get("columns") or []
            wanted = [c for c in original_cols if c.lower() in econ_cols]
            if not wanted:
                continue
            col_sql = ", ".join(f'"{c}"' for c in wanted)
            try:
                cur = con.execute(f'SELECT {col_sql} FROM "{tname}"')
                for tup in cur.fetchall():
                    row = {wanted[i]: tup[i] for i in range(len(wanted))}
                    # Normalize odds alias
                    if "execution_odds" not in row and "odds" in row:
                        row["execution_odds"] = row.get("odds")
                    rows.append(row)
            except Exception:
                continue
    finally:
        con.close()
    return rows


def _candidate_paths(root: Path) -> List[Path]:
    """Bounded walk of safe scopes for ledger-like files."""
    found: List[Path] = []
    for scope in DEFAULT_SEARCH_SCOPES:
        base = root / scope
        if not base.exists():
            continue
        try:
            iterator = base.rglob("*")
        except Exception:
            continue
        for p in iterator:
            try:
                if not p.is_file():
                    continue
            except OSError:
                continue
            if is_excluded_path(p, root):
                continue
            if p.suffix.lower() not in SCAN_SUFFIXES:
                continue
            # Size guard — skip huge pub dumps
            try:
                size = p.stat().st_size
            except OSError:
                continue
            if size > 64 * 1024 * 1024:
                continue
            # Prefer ledger-named files; also accept any small JSON/CSV/SQLite under
            # bets_data/runtime that declares map_id+economics after open.
            name_hit = bool(LEDGER_NAME_HINTS.search(p.name)) or bool(
                LEDGER_NAME_HINTS.search(p.as_posix())
            )
            # Always include ledger-named; for others, only shallow-ish and not known pub dicts.
            if not name_hit:
                # Skip known non-ledger bulk artifacts by name.
                if re.search(
                    r"(dict_raw|json_parts|extracted_|tempo_|hero_|pub_match|synergy|counterpick|"
                    r"backtest_branches|comeback_experiment|kills_window|global_metrics)",
                    p.as_posix(),
                    re.I,
                ):
                    continue
                # Only consider modest files for blind schema sniff.
                if size > 5 * 1024 * 1024:
                    continue
            found.append(p)
    return found


def _safe_field_names(fields: Sequence[str]) -> List[str]:
    """Filter out secret-looking field names from reports."""
    safe = []
    for f in fields:
        if SECRET_FIELD_HINTS.search(str(f)):
            continue
        safe.append(str(f))
    return sorted(set(safe))


def _row_has_canonical_map_and_provenance(row: Dict[str, Any]) -> bool:
    mid = row.get("map_id")
    prov = row.get("provenance")
    return bool(mid not in (None, "") and prov not in (None, ""))


def inventory_execution_ledger(root: Path | str) -> Dict[str, Any]:
    """Bounded read-only inventory of local execution economics sources.

    Returns a report with status ``verified`` or ``unavailable``. Never embeds
    raw bet values, secrets, or customer content.
    """
    root = Path(root).resolve()
    candidates_meta: List[Dict[str, Any]] = []
    sources: List[Dict[str, Any]] = []
    all_accepted: List[Dict[str, Any]] = []
    all_quarantine: List[Dict[str, Any]] = []
    identical_dupes = 0
    errors: List[Dict[str, str]] = []

    searched = [str((root / s).as_posix()) for s in DEFAULT_SEARCH_SCOPES]
    exclusion_list = list(DEFAULT_EXCLUSIONS)

    paths = _candidate_paths(root)
    for path in paths:
        fmt, rows, fields, err = _load_candidate_rows(path)
        rel = str(path.resolve().relative_to(root)) if path.is_absolute() else str(path)
        try:
            rel = str(path.resolve().relative_to(root))
        except Exception:
            rel = str(path)

        entry: Dict[str, Any] = {
            "path": rel,
            "format": fmt,
            "file_hash": _file_sha256(path) if path.exists() else None,
            "safe_fields": _safe_field_names(fields),
            "schema_hash": _schema_hash(fields) if fields else None,
            "row_count": len(rows),
        }
        if err:
            entry["error"] = err.split(":")[0]  # class only, not full message with paths/values
            entry["availability"] = "error"
            errors.append({"path": rel, "error_class": err.split(":")[0]})
            candidates_meta.append(entry)
            continue

        # Does this look like an economics source?
        field_set = {f.lower() for f in fields}
        has_map = "map_id" in field_set
        has_econ = bool(
            field_set
            & {
                "execution_odds",
                "closing_odds",
                "stake",
                "settlement",
                "pnl",
                "bookmaker",
                "odds",
                "placed_at",
            }
        )
        if not (has_map and has_econ):
            entry["availability"] = "not_economics"
            candidates_meta.append(entry)
            continue

        # Inject provenance default from path only if rows already have verifiable map_id
        # and an explicit provenance field somewhere — we do NOT invent provenance.
        # Rows without provenance are quarantined by the normalizer.
        map_ids = set()
        for r in rows:
            if isinstance(r, dict) and r.get("map_id") not in (None, ""):
                map_ids.add(str(r.get("map_id")))

        entry["map_id_coverage"] = len(map_ids)
        entry["availability"] = "candidate"

        # Normalize without leaking values into inventory.
        batch = normalize_execution_rows(rows)
        entry["accepted_count"] = batch["accepted_count"]
        entry["quarantine_count"] = batch["quarantine_count"]
        entry["quarantine_reasons"] = batch["quarantine_reasons"]

        # Accept source only if at least one row has verifiable map_id + provenance
        # and normalizes cleanly.
        if batch["accepted_count"] > 0:
            entry["availability"] = "verified"
            sources.append(
                {
                    "path": rel,
                    "format": fmt,
                    "file_hash": entry["file_hash"],
                    "schema_hash": entry["schema_hash"],
                    "safe_fields": entry["safe_fields"],
                    "row_count": entry["row_count"],
                    "map_id_coverage": entry["map_id_coverage"],
                    "accepted_count": batch["accepted_count"],
                    "quarantine_count": batch["quarantine_count"],
                }
            )
            all_accepted.extend(batch["accepted"])
            all_quarantine.extend(batch["quarantine"])
            identical_dupes += batch["duplicate_identical_count"]
        else:
            # Economics-shaped but no verifiable accepted rows.
            entry["availability"] = "unqualified"
            # Track missing required fields across sample (field names only).
            missing = set(REQUIRED_FOR_ACCEPT)
            for r in rows[:50]:
                if not isinstance(r, dict):
                    continue
                present = {k for k in REQUIRED_FOR_ACCEPT if r.get(k) not in (None, "")}
                # settlement may appear as outcome
                if r.get("outcome") not in (None, "") and "settlement" not in present:
                    present.add("settlement")
                missing &= set(REQUIRED_FOR_ACCEPT) - present
            entry["missing_fields_sample"] = sorted(missing)
            all_quarantine.extend(batch["quarantine"])
            identical_dupes += batch["duplicate_identical_count"]

        candidates_meta.append(entry)

    # Cross-source map_id conflicts are out of scope for inventory counts;
    # re-normalize merged accepted for a global view without values in output.
    merged = normalize_execution_rows(all_accepted) if all_accepted else {
        "accepted": [],
        "accepted_count": 0,
        "quarantine": [],
        "quarantine_count": 0,
        "quarantine_reasons": {},
        "duplicate_identical_count": 0,
    }

    quarantine_reasons = Counter()
    for q in all_quarantine:
        quarantine_reasons[q.get("reason", "unknown")] += 1
    for q in merged.get("quarantine") or []:
        quarantine_reasons[q.get("reason", "unknown")] += 1

    missing_fields: List[str] = []
    if not sources:
        # Exact missing fields for unavailable outcome: required economics contract.
        missing_fields = list(REQUIRED_FOR_ACCEPT)
        # If we saw economics-shaped candidates missing only some fields, refine.
        seen_missing: Counter = Counter()
        for c in candidates_meta:
            for mf in c.get("missing_fields_sample") or []:
                seen_missing[mf] += 1
        if seen_missing:
            missing_fields = sorted(seen_missing.keys())

    status = "verified" if sources and merged["accepted_count"] > 0 else "unavailable"

    report: Dict[str, Any] = {
        "status": status,
        "searched_scopes": searched,
        "exclusion_list": exclusion_list,
        "schema_hooks": SCHEMA_HOOKS,
        "candidates": [
            # Strip any accidental heavy keys — keep safe summary only.
            {
                k: v
                for k, v in c.items()
                if k
                in {
                    "path",
                    "format",
                    "file_hash",
                    "schema_hash",
                    "safe_fields",
                    "row_count",
                    "map_id_coverage",
                    "availability",
                    "accepted_count",
                    "quarantine_count",
                    "quarantine_reasons",
                    "missing_fields_sample",
                    "error",
                }
            }
            for c in candidates_meta
            if c.get("availability") not in {"not_economics"}
            or LEDGER_NAME_HINTS.search(c.get("path", ""))
        ],
        "sources": sources,
        "accepted_count": merged["accepted_count"] if status == "verified" else 0,
        "quarantine_count": int(sum(quarantine_reasons.values())),
        "quarantine_reasons": dict(quarantine_reasons),
        "duplicate_identical_count": identical_dupes + merged.get("duplicate_identical_count", 0),
        "errors": errors,
        "missing_fields": missing_fields if status == "unavailable" else [],
    }
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Read-only execution-ledger inventory/normalizer")
    p.add_argument(
        "--inventory-only",
        action="store_true",
        help="Run bounded archival inventory and write safe JSON report",
    )
    p.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Project root to scan (default: cwd)",
    )
    p.add_argument(
        "--output",
        type=Path,
        required=False,
        help="Output path for inventory JSON (staging only)",
    )
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if not args.inventory_only:
        print("Only --inventory-only is supported in this lane.", file=sys.stderr)
        return 2

    root = args.root.resolve()
    report = inventory_execution_ledger(root=root)

    out_path = args.output
    if out_path is None:
        out_path = root / "runtime" / "star_dispatch_replay" / "staging" / "ledger" / "ledger_inventory.json"
    out_path = out_path.resolve()

    # Refuse to write into final/
    if "star_dispatch_replay/final" in out_path.as_posix():
        print("Refusing to write into final/ — staging only.", file=sys.stderr)
        return 3

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(out_path)

    # Safe stdout summary (no secrets / no raw rows).
    summary = {
        "status": report["status"],
        "accepted_count": report.get("accepted_count"),
        "quarantine_count": report.get("quarantine_count"),
        "sources": len(report.get("sources") or []),
        "candidates": len(report.get("candidates") or []),
        "missing_fields": report.get("missing_fields") or [],
        "output": str(out_path),
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
