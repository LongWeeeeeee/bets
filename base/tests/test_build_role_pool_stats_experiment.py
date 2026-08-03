import json
import sqlite3

from base.build_role_pool_stats_experiment import build


def test_builder_creates_compact_cp_and_trio_tables_without_permutation_inflation(tmp_path):
    source = tmp_path / "raw.sqlite3"
    output = tmp_path / "roles.sqlite3"
    rows = [
        ("60pos3_vs_13pos2,110pos3", {"wins": 12, "games": 14}),
        ("60pos3_vs_110pos3,13pos2", {"wins": 12, "games": 14}),
        ("13pos2,110pos3_vs_60pos3", {"wins": 6, "games": 14}),
        ("60pos1_vs_13pos3,110pos2", {"wins": 1, "games": 3}),
        ("60pos3,13pos2,110pos4", {"wins": 12, "games": 20}),
        ("110pos4,60pos3,13pos2", {"wins": 12, "games": 20}),
        ("60pos1,13pos3,110pos5", {"wins": 4, "games": 8}),
        ("60pos3_with_13pos2", {"wins": 99, "games": 100}),
    ]
    with sqlite3.connect(source) as conn:
        conn.execute("CREATE TABLE kv(key TEXT PRIMARY KEY, value BLOB NOT NULL)")
        conn.executemany(
            "INSERT INTO kv VALUES (?, ?)",
            [(key, json.dumps(value)) for key, value in rows],
        )

    meta = build(source, output, batch_size=2)

    assert meta["scanned_rows"] == len(rows)
    with sqlite3.connect(output) as conn:
        cp = conn.execute(
            "SELECT score, games FROM cp1vs2 WHERE key = ?",
            ("60:core_vs_13:core,110:core",),
        ).fetchone()
        trio = conn.execute(
            "SELECT score, games FROM synergy_trio WHERE key = ?",
            ("13:core,60:core,110:support",),
        ).fetchone()
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"

    assert cp == (21.0, 31)
    assert trio == (16.0, 28)
