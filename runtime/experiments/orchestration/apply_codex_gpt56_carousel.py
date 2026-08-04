#!/usr/bin/env python3
"""Idempotent: GPT-5.6 Codex multi-account carousel in OmniRoute.

Why:
  Live traffic for codex/gpt-5.6-sol-* was sticky on a single active account
  (priority=1), while 2 other active Plus accounts sat idle and 3 were expired.
  This creates a round-robin combo across ACTIVE codex connections and maps
  cx/* + codex/* GPT-5.6 patterns onto it (same pattern as grok-cli-worker).

Does NOT touch expired accounts (they stay for later re-auth).
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

DB = Path("/root/.omniroute/storage.sqlite")
COMBO_NAME = "codex-gpt56-carousel"
COMBO_ID = "a1c0de56-gpt5-6c0d-ex-carousel0001"  # stable id for idempotency
BACKUP = Path("/root/main/runtime/omniroute_codex_carousel_backup.json")

# Patterns Hermes/agents actually request
MAPPINGS = [
    ("cx/gpt-5.6-sol-xhigh", 0),
    ("cx/gpt-5.6-sol-high", 0),
    ("cx/gpt-5.6-sol-max", 0),
    ("cx/gpt-5.6-max", 0),
    ("cx/gpt-5.6*", 10),
    ("cx/*", 50),
    ("codex/gpt-5.6-sol-xhigh", 0),
    ("codex/gpt-5.6-sol-high", 0),
    ("codex/gpt-5.6-sol-max", 0),
    ("codex/gpt-5.6-max", 0),
    ("codex/gpt-5.6*", 10),
    ("codex/*", 50),
]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def main() -> int:
    if not DB.exists():
        raise SystemExit(f"missing {DB}")

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT id, name, email, is_active, test_status, priority, global_priority
            FROM provider_connections
            WHERE provider = 'codex'
            ORDER BY COALESCE(priority, 999), name
            """
        ).fetchall()
        active = [r for r in rows if int(r["is_active"] or 0) == 1 and (r["test_status"] or "") == "active"]
        expired = [r for r in rows if (r["test_status"] or "") == "expired"]

        snap = {
            "ts": utc_now(),
            "all": [dict(r) for r in rows],
            "active_ids": [r["id"] for r in active],
            "expired_ids": [r["id"] for r in expired],
        }
        BACKUP.write_text(json.dumps(snap, indent=2) + "\n")

        if len(active) < 2:
            print(f"FAIL: need >=2 active codex accounts, found {len(active)}")
            for r in rows:
                print(f"  {r['name']}: active={r['is_active']} status={r['test_status']}")
            return 2

        models = []
        for i, r in enumerate(active):
            models.append(
                {
                    "id": f"codex-gpt56-{i+1}-{r['name'].split('@')[0][:20]}",
                    "kind": "model",
                    # Bare upstream model id (NOT codex/...) to avoid recursive
                    # combo rematch; inbound cx/codex pattern still selects effort.
                    "model": "gpt-5.6-sol-xhigh",
                    "providerId": "codex",
                    "connectionId": r["id"],
                    "weight": 1,
                    "priority": i + 1,
                }
            )

        # Equal-weight fan-out. priority/least-used previously stuck on the historical
        # hot account under load; weighted+equal weights spreads across actives.
        # Reorder so the previous hotspot is not always member[0].
        models = list(reversed(models))
        for i, m in enumerate(models):
            m["priority"] = 1
            m["weight"] = 1
        data = {
            "name": COMBO_NAME,
            "models": models,
            "strategy": "weighted",
            "id": COMBO_ID,
            "config": {
                "description": (
                    f"Codex GPT-5.6 carousel across {len(active)} active ChatGPT Plus "
                    "accounts; strategy=weighted equal; stickySession=false; expired excluded"
                ),
                "stickySession": False,
            },
            "isHidden": False,
            "sortOrder": 0,
            "createdAt": utc_now(),
            "updatedAt": utc_now(),
            "version": 1,
            "description": "GPT-5.6 sol carousel over active Codex accounts",
        }

        conn.execute("BEGIN")
        # Equalize native provider priority so non-combo codex/* also fans out
        # (priority was 1/2/4 → always first account). Keep ALL actives equal.
        for r in active:
            conn.execute(
                """
                UPDATE provider_connections
                SET priority = 10, global_priority = 0,
                    consecutive_use_count = 0, updated_at = ?
                WHERE id = ?
                """,
                (utc_now(), r["id"]),
            )
        # Demote expired so they are never preferred if somehow selected
        for r in expired:
            conn.execute(
                """
                UPDATE provider_connections
                SET priority = 100, updated_at = ?
                WHERE id = ?
                """,
                (utc_now(), r["id"]),
            )

        existing = conn.execute("SELECT id, data FROM combos WHERE id = ? OR name = ?", (COMBO_ID, COMBO_NAME)).fetchone()
        payload = json.dumps(data, separators=(",", ":"))
        if existing:
            # preserve createdAt if present
            try:
                old = json.loads(existing["data"] or "{}")
                data["createdAt"] = old.get("createdAt") or data["createdAt"]
                data["version"] = int(old.get("version") or 1) + 1
                data["updatedAt"] = utc_now()
                payload = json.dumps(data, separators=(",", ":"))
            except Exception:
                pass
            conn.execute(
                "UPDATE combos SET name = ?, data = ?, sort_order = 0, updated_at = ? WHERE id = ?",
                (COMBO_NAME, payload, utc_now(), existing["id"]),
            )
            combo_id = existing["id"]
            action = "updated"
        else:
            conn.execute(
                """
                INSERT INTO combos (id, name, data, sort_order, created_at, updated_at)
                VALUES (?, ?, ?, 0, ?, ?)
                """,
                (COMBO_ID, COMBO_NAME, payload, utc_now(), utc_now()),
            )
            combo_id = COMBO_ID
            action = "created"

        # Replace our mappings only (by pattern list)
        for pattern, prio in MAPPINGS:
            conn.execute("DELETE FROM model_combo_mappings WHERE pattern = ?", (pattern,))
            conn.execute(
                """
                INSERT INTO model_combo_mappings
                  (id, pattern, combo_id, priority, enabled, description, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    pattern,
                    combo_id,
                    prio,
                    f"route {pattern} → {COMBO_NAME}",
                    utc_now(),
                    utc_now(),
                ),
            )

        conn.commit()

        # Verify
        v_combo = conn.execute("SELECT name, data FROM combos WHERE id = ?", (combo_id,)).fetchone()
        v_maps = conn.execute(
            "SELECT pattern, priority FROM model_combo_mappings WHERE combo_id = ? ORDER BY priority, pattern",
            (combo_id,),
        ).fetchall()
        v_pri = conn.execute(
            "SELECT name, test_status, priority FROM provider_connections WHERE provider='codex' ORDER BY priority, name"
        ).fetchall()

        print(f"SUCCESS combo {action}: {COMBO_NAME} id={combo_id}")
        print(f"active_accounts={len(active)} expired={len(expired)}")
        for r in active:
            print(f"  + {r['name']} ({r['id'][:8]}…)")
        for r in expired:
            print(f"  - expired {r['name']}")
        print(f"strategy={data.get('strategy')} stickySession=false members={len(models)}")
        print(f"mappings={len(v_maps)}")
        for m in v_maps:
            print(f"  {m['pattern']} prio={m['priority']}")
        print("codex priorities now:")
        for r in v_pri:
            print(f"  {r['priority']:>3} {r['test_status']:<8} {r['name']}")
        print(f"backup={BACKUP}")
        # show stored strategy
        stored = json.loads(v_combo["data"])
        print(f"stored_strategy={stored.get('strategy')} n_models={len(stored.get('models') or [])}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
