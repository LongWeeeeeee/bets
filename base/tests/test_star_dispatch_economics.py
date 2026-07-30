"""TDD tests for read-only execution-ledger audit + normalizer.

Covers:
- valid normalization of execution economics rows
- quarantine of bad odds/stake
- duplicate conflict quarantine
- read-only SQLite open
- absent / non-qualifying source => unavailable
- secret-path exclusion
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import star_dispatch_economics as sde  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_root(tmp_path: Path) -> Path:
    """Isolated project-like root with safe archival scopes."""
    (tmp_path / "bets_data").mkdir()
    (tmp_path / "runtime").mkdir()
    (tmp_path / "base").mkdir()
    return tmp_path


def _valid_row(**overrides):
    row = {
        "map_id": "425302:1",
        "execution_odds": 1.85,
        "closing_odds": 1.92,
        "stake": 10.0,
        "settlement": "win",
        "pnl": 8.5,
        "bookmaker": "winline",
        "market": "map_winner",
        "currency": "USD",
        "placed_at": "2026-03-01T12:00:00Z",
        "provenance": "test_fixture_ledger",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Normalization: valid
# ---------------------------------------------------------------------------


def test_normalize_valid_row():
    out = sde.normalize_execution_row(_valid_row())
    assert out["status"] == "ok"
    rec = out["record"]
    assert rec["map_id"] == "425302:1"
    assert rec["execution_odds"] == pytest.approx(1.85)
    assert rec["closing_odds"] == pytest.approx(1.92)
    assert rec["stake"] == pytest.approx(10.0)
    assert rec["settlement"] == "win"
    assert rec["pnl"] == pytest.approx(8.5)
    assert rec["bookmaker"] == "winline"
    assert rec["market"] == "map_winner"
    assert rec["currency"] == "USD"
    assert rec["placed_at"] == "2026-03-01T12:00:00Z"
    assert rec["provenance"] == "test_fixture_ledger"


def test_normalize_optional_pnl_and_closing_absent_ok():
    row = _valid_row()
    del row["pnl"]
    del row["closing_odds"]
    out = sde.normalize_execution_row(row)
    assert out["status"] == "ok"
    assert "pnl" not in out["record"] or out["record"].get("pnl") is None
    assert "closing_odds" not in out["record"] or out["record"].get("closing_odds") is None


# ---------------------------------------------------------------------------
# Normalization: bad odds / stake
# ---------------------------------------------------------------------------


def test_normalize_bad_execution_odds_quarantined():
    for bad in (1.0, 0.5, -1.2, "abc", None):
        out = sde.normalize_execution_row(_valid_row(execution_odds=bad))
        assert out["status"] == "quarantine"
        assert out["reason"] in {
            "invalid_execution_odds",
            "missing_execution_odds",
            "non_numeric_execution_odds",
        }


def test_normalize_bad_stake_quarantined():
    for bad in (-0.01, "nope"):
        out = sde.normalize_execution_row(_valid_row(stake=bad))
        assert out["status"] == "quarantine"
        assert "stake" in out["reason"]


def test_normalize_zero_stake_ok():
    out = sde.normalize_execution_row(_valid_row(stake=0))
    assert out["status"] == "ok"
    assert out["record"]["stake"] == pytest.approx(0.0)


def test_normalize_missing_map_id_quarantined():
    row = _valid_row()
    del row["map_id"]
    out = sde.normalize_execution_row(row)
    assert out["status"] == "quarantine"
    assert out["reason"] == "missing_map_id"


def test_normalize_missing_provenance_quarantined():
    row = _valid_row()
    del row["provenance"]
    out = sde.normalize_execution_row(row)
    assert out["status"] == "quarantine"
    assert out["reason"] == "missing_provenance"


def test_normalize_invalid_settlement_quarantined():
    out = sde.normalize_execution_row(_valid_row(settlement="maybe"))
    assert out["status"] == "quarantine"
    assert out["reason"] == "invalid_settlement"


def test_normalize_does_not_infer_missing_stake():
    row = _valid_row()
    del row["stake"]
    out = sde.normalize_execution_row(row)
    assert out["status"] == "quarantine"
    assert out["reason"] == "missing_stake"


# ---------------------------------------------------------------------------
# Batch normalize + duplicate conflicts
# ---------------------------------------------------------------------------


def test_normalize_rows_duplicate_conflict_quarantined():
    a = _valid_row(execution_odds=1.85, stake=10.0)
    b = _valid_row(execution_odds=2.10, stake=10.0)  # same map_id, conflicting odds
    result = sde.normalize_execution_rows([a, b])
    assert result["accepted_count"] == 0
    assert result["quarantine_count"] >= 2
    reasons = {q["reason"] for q in result["quarantine"]}
    assert "duplicate_conflict" in reasons


def test_normalize_rows_identical_duplicates_keep_one():
    a = _valid_row()
    b = _valid_row()  # identical
    result = sde.normalize_execution_rows([a, b])
    assert result["accepted_count"] == 1
    assert result["duplicate_identical_count"] == 1


# ---------------------------------------------------------------------------
# SQLite read-only
# ---------------------------------------------------------------------------


def test_sqlite_opened_read_only(tmp_path: Path):
    db = tmp_path / "ledger.sqlite3"
    con = sqlite3.connect(str(db))
    con.execute(
        "CREATE TABLE bets ("
        "map_id TEXT, execution_odds REAL, closing_odds REAL, stake REAL, "
        "settlement TEXT, pnl REAL, bookmaker TEXT, market TEXT, "
        "currency TEXT, placed_at TEXT, provenance TEXT)"
    )
    con.execute(
        "INSERT INTO bets VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            "m1:1",
            1.5,
            1.6,
            5.0,
            "loss",
            -5.0,
            "x",
            "mw",
            "USD",
            "2026-01-01T00:00:00Z",
            "unit",
        ),
    )
    con.commit()
    con.close()

    meta = sde.inspect_sqlite_schema(db)
    assert meta["format"] == "sqlite"
    assert meta["read_only"] is True
    assert "bets" in meta["tables"]
    assert "map_id" in meta["tables"]["bets"]["columns"]

    # Mutation through the inspector path must fail / not be available.
    with pytest.raises(Exception):
        sde._open_sqlite_readonly(db).execute("INSERT INTO bets(map_id) VALUES ('x')")


# ---------------------------------------------------------------------------
# Inventory: absent source / secret exclusion
# ---------------------------------------------------------------------------


def test_inventory_absent_source_unavailable(tmp_root: Path):
    report = sde.inventory_execution_ledger(root=tmp_root)
    assert report["status"] == "unavailable"
    assert "searched_scopes" in report
    assert "exclusion_list" in report
    assert "missing_fields" in report
    # Schema hooks retained for future ledgers
    assert "schema_hooks" in report
    assert "map_id" in report["missing_fields"]


def test_inventory_excludes_secret_paths(tmp_root: Path):
    # Plant a fake "ledger" under excluded secret paths — must not be accepted
    secrets = [
        tmp_root / "base" / "keys.py",
        tmp_root / ".env",
        tmp_root / ".env.local",
        tmp_root / "venv" / "lib" / "fake_ledger.json",
    ]
    for p in secrets:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps(
                [
                    _valid_row(
                        map_id="secret:1",
                        provenance="should_not_be_read",
                    )
                ]
            ),
            encoding="utf-8",
        )

    report = sde.inventory_execution_ledger(root=tmp_root)
    assert report["status"] == "unavailable"
    scanned = [c.get("path", "") for c in report.get("candidates", [])]
    for s in secrets:
        rel = str(s.relative_to(tmp_root))
        assert not any(rel in sp or sp.endswith(rel) for sp in scanned)
    excl = " ".join(report.get("exclusion_list", []))
    assert "keys.py" in excl or ".env" in excl or "venv" in excl


def test_inventory_verified_when_qualifying_source(tmp_root: Path):
    ledger = tmp_root / "bets_data" / "execution_ledger.jsonl"
    rows = [
        _valid_row(map_id="a:1"),
        _valid_row(map_id="b:1", execution_odds=2.0, stake=3.0, settlement="loss", pnl=-3.0),
    ]
    with ledger.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

    report = sde.inventory_execution_ledger(root=tmp_root)
    assert report["status"] == "verified"
    assert report["accepted_count"] == 2
    assert report["quarantine_count"] == 0
    src = report["sources"][0]
    assert "path" in src
    assert "schema_hash" in src or "file_hash" in src
    assert "safe_fields" in src
    assert "map_id" in src["safe_fields"]
    # No raw row values / secrets in report
    blob = json.dumps(report)
    assert "should_not" not in blob
    assert "1.85" not in blob  # raw odds values must not leak into inventory summary


def test_inventory_quarantines_malformed_and_keeps_counts(tmp_root: Path):
    ledger = tmp_root / "runtime" / "exec_rows.json"
    payload = [
        _valid_row(map_id="ok:1"),
        _valid_row(map_id="bad:1", execution_odds=0.9),  # bad odds
        _valid_row(map_id="ok:1", execution_odds=3.0),  # conflict
    ]
    ledger.write_text(json.dumps(payload), encoding="utf-8")
    report = sde.inventory_execution_ledger(root=tmp_root)
    assert report["status"] in {"verified", "unavailable"}
    # With one good + quarantines, if conflict drops the good row we may be unavailable
    # or verified with quarantine. Either way counts must be present.
    assert "quarantine_count" in report
    assert report["quarantine_count"] >= 1
    assert "quarantine_reasons" in report


def test_is_excluded_path_helpers(tmp_root: Path):
    assert sde.is_excluded_path(tmp_root / "base" / "keys.py", root=tmp_root) is True
    assert sde.is_excluded_path(tmp_root / ".env", root=tmp_root) is True
    assert sde.is_excluded_path(tmp_root / "venv" / "x", root=tmp_root) is True
    assert sde.is_excluded_path(tmp_root / "runtime" / "star_dispatch_replay" / "final" / "x", root=tmp_root) is True
    assert sde.is_excluded_path(tmp_root / "bets_data" / "ok.json", root=tmp_root) is False


def test_inventory_report_has_no_secret_like_values(tmp_root: Path):
    # Plant a ledger that also has a secret-looking field — inventory must not echo values
    ledger = tmp_root / "bets_data" / "with_secret_field.json"
    row = _valid_row()
    row["api_token"] = "SUPERSECRETTOKEN123"
    row["proxy"] = "http://user:pass@1.2.3.4:8080"
    ledger.write_text(json.dumps([row]), encoding="utf-8")
    report = sde.inventory_execution_ledger(root=tmp_root)
    blob = json.dumps(report)
    assert "SUPERSECRETTOKEN123" not in blob
    assert "user:pass" not in blob
    assert "1.2.3.4" not in blob
