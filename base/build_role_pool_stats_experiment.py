#!/usr/bin/env python3
"""Build compact core/support cp1vs2 and synergy_trio experiment tables.

The source is a frozen raw ``kv`` statistics SQLite database.  The output is a
new standalone SQLite artifact; neither the source nor live dictionaries are
modified.  Each source is scanned once and the finished DB is atomically moved
over the requested output path only after ``PRAGMA quick_check`` succeeds.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

try:
    import orjson
except ImportError:  # pragma: no cover - production venv has orjson
    orjson = None

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from base.cp1vs2_role_pool import raw_row_to_sample
from base.synergy_trio_role_pool import raw_row_to_trio_sample


def decode_entry(value: Any) -> Any:
    return orjson.loads(value) if orjson is not None else json.loads(value)


def encode_meta(value: Any) -> bytes | str:
    return orjson.dumps(value) if orjson is not None else json.dumps(value)


def build(source: Path, output: Path, batch_size: int = 20_000) -> dict[str, int]:
    if not source.is_file():
        raise FileNotFoundError(source)
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_name(f".{output.name}.{os.getpid()}.tmp")

    source_uri = f"{source.resolve().as_uri()}?mode=ro&immutable=1"
    source_conn = sqlite3.connect(source_uri, uri=True)
    source_conn.execute("PRAGMA query_only=ON")
    source_conn.execute("PRAGMA mmap_size=1073741824")
    destination = sqlite3.connect(temp)
    destination.execute("PRAGMA journal_mode=OFF")
    destination.execute("PRAGMA synchronous=OFF")
    destination.execute("PRAGMA temp_store=FILE")
    destination.execute("PRAGMA cache_size=-200000")
    destination.executescript(
        """
        CREATE TEMP TABLE cp_exact (
            exact_key TEXT NOT NULL,
            direction TEXT NOT NULL,
            score REAL NOT NULL,
            games INTEGER NOT NULL,
            role_key TEXT NOT NULL,
            PRIMARY KEY (exact_key, direction, score, games)
        ) WITHOUT ROWID;
        CREATE TEMP TABLE trio_exact (
            exact_key TEXT NOT NULL,
            score REAL NOT NULL,
            games INTEGER NOT NULL,
            role_key TEXT NOT NULL,
            PRIMARY KEY (exact_key, score, games)
        ) WITHOUT ROWID;
        """
    )

    scanned = cp_rows = trio_rows = 0
    started = time.monotonic()
    cursor = source_conn.execute("SELECT key, value FROM kv")
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        cp_batch = []
        trio_batch = []
        for raw_key, raw_value in rows:
            scanned += 1
            key = str(raw_key)
            if key.count("_vs_") == 1:
                left, right = key.split("_vs_", 1)
                if sorted((len(left.split(",")), len(right.split(",")))) != [1, 2]:
                    continue
                entry = decode_entry(raw_value)
                sample = raw_row_to_sample(key, entry)
                if sample is not None:
                    cp_batch.append((
                        sample.exact_key, sample.direction, sample.score,
                        sample.games, sample.role_key,
                    ))
            elif "_with_" not in key and key.count(",") == 2:
                entry = decode_entry(raw_value)
                sample = raw_row_to_trio_sample(key, entry)
                if sample is not None:
                    trio_batch.append((
                        sample.exact_key, sample.score, sample.games,
                        sample.role_key,
                    ))
        if cp_batch:
            before = destination.total_changes
            destination.executemany(
                "INSERT OR IGNORE INTO cp_exact VALUES (?, ?, ?, ?, ?)", cp_batch
            )
            cp_rows += destination.total_changes - before
        if trio_batch:
            before = destination.total_changes
            destination.executemany(
                "INSERT OR IGNORE INTO trio_exact VALUES (?, ?, ?, ?)", trio_batch
            )
            trio_rows += destination.total_changes - before
        destination.commit()
        if scanned % 1_000_000 < len(rows):
            elapsed = max(0.001, time.monotonic() - started)
            print(
                f"scanned={scanned:,} cp_exact={cp_rows:,} trio_exact={trio_rows:,} "
                f"rate={scanned / elapsed:,.0f}/s",
                flush=True,
            )

    destination.executescript(
        """
        CREATE TABLE cp1vs2 (
            key TEXT PRIMARY KEY,
            score REAL NOT NULL,
            games INTEGER NOT NULL
        ) WITHOUT ROWID;
        INSERT INTO cp1vs2
        SELECT role_key, SUM(score), SUM(games)
        FROM cp_exact GROUP BY role_key;

        CREATE TABLE synergy_trio (
            key TEXT PRIMARY KEY,
            score REAL NOT NULL,
            games INTEGER NOT NULL
        ) WITHOUT ROWID;
        INSERT INTO synergy_trio
        SELECT role_key, SUM(score), SUM(games)
        FROM trio_exact GROUP BY role_key;

        CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID;
        """
    )
    cp_keys = int(destination.execute("SELECT COUNT(*) FROM cp1vs2").fetchone()[0])
    trio_keys = int(destination.execute("SELECT COUNT(*) FROM synergy_trio").fetchone()[0])
    meta = {
        "format": "core_support_role_pool.v1",
        "source": str(source),
        "source_size": source.stat().st_size,
        "scanned_rows": scanned,
        "cp_exact_rows": cp_rows,
        "trio_exact_rows": trio_rows,
        "cp_role_keys": cp_keys,
        "trio_role_keys": trio_keys,
    }
    destination.executemany(
        "INSERT INTO meta(key, value) VALUES (?, ?)",
        [(key, encode_meta(value)) for key, value in meta.items()],
    )
    destination.commit()
    check = destination.execute("PRAGMA quick_check").fetchone()[0]
    destination.close()
    source_conn.close()
    if check != "ok" or cp_keys <= 0 or trio_keys <= 0:
        raise RuntimeError(f"invalid role DB: quick_check={check!r}, cp={cp_keys}, trio={trio_keys}")
    temp.replace(output)
    print(f"completed output={output} cp_keys={cp_keys:,} trio_keys={trio_keys:,}", flush=True)
    return meta


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=20_000)
    args = parser.parse_args()
    build(args.source, args.output, max(100, args.batch_size))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
