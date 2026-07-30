#!/usr/bin/env python3
"""Backtest kills_window advantage on pro 7.41+ maps with ≥1 Tier-1 team.

Universe:
  - only pro_heroes_data json parts matching 7.41*
  - match has at least one side team id in id_to_names.tier_one_teams

Windows (half-open): 5_15, 10_20, 15_25, 20_30.

For each window:
  - actual kill-diff = radiant - dire in [start:end)
  - predict sign(expected_diff) from calculate_kills_window_advantage
  - kill-lead WR: predicted side has strictly more kills (tie/zero pred = lose)
  - base map WR: predicted side wins the map (didRadiantWin)
  - buckets by |expected_diff|

Dict is queried via SQLite PK (no full 41M-key Python load).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

ROOT = Path("/root/main")
BASE = ROOT / "base"
sys.path = [str(BASE)] + [p for p in sys.path if Path(p).resolve() != ROOT]

import orjson  # noqa: E402
from analise_database import (  # noqa: E402
    KILLS_WINDOWS,
    _kills_window_diff,
    extract_heroes_by_position,
)
from functions import calculate_kills_window_advantage  # noqa: E402
from id_to_names import tier_one_teams  # noqa: E402

PRO_DIR = ROOT / "pro_heroes_data" / "json_parts_split_from_object"
KW_DB = ROOT / "bets_data" / "analise_pub_matches" / "kills_window_dict_raw.sqlite3"
OUT_JSON = ROOT / "runtime" / "kills_window_adv_pro_tier1_741_backtest.json"
OUT_MD = ROOT / "runtime" / "kills_window_adv_pro_tier1_741_backtest.md"


def _tier1_ids() -> set[int]:
    ids: set[int] = set()
    for _name, val in (tier_one_teams or {}).items():
        if isinstance(val, (int, float)):
            ids.add(int(val))
        elif isinstance(val, (list, set, tuple)):
            for x in val:
                try:
                    ids.add(int(x))
                except (TypeError, ValueError):
                    pass
        else:
            try:
                ids.add(int(val))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                pass
    return ids


def _team_id(side: Any) -> Optional[int]:
    if not isinstance(side, dict):
        return None
    raw = side.get("id")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _tokens(by_pos: dict) -> list[str]:
    return [f"{hero}pos{pos}" for pos, hero in sorted(by_pos.items())]


def _bucket(abs_diff: float) -> str:
    if abs_diff < 0.5:
        return "0-0.5"
    if abs_diff < 1.0:
        return "0.5-1"
    if abs_diff < 2.0:
        return "1-2"
    if abs_diff < 3.0:
        return "2-3"
    return "3+"


class StatsDict(dict):
    """Lazy key→row dict backed by kills_window stats table."""

    def __init__(self, conn: sqlite3.Connection, value_cols: list[str]):
        super().__init__()
        self._conn = conn
        self._cols = value_cols
        self._select = "SELECT " + ", ".join(value_cols) + " FROM stats WHERE key = ?"
        self._hits = 0
        self._misses = 0

    def __contains__(self, key: object) -> bool:  # type: ignore[override]
        if super().__contains__(key):
            return True
        row = self._conn.execute(self._select, (key,)).fetchone()
        if row is None:
            self._misses += 1
            return False
        self[key] = {name: row[i] for i, name in enumerate(self._cols)}  # type: ignore[index]
        self._hits += 1
        return True

    def get(self, key, default=None):  # type: ignore[override]
        if key in self:
            return self[key]
        return default

    def __getitem__(self, key):  # type: ignore[override]
        if super().__contains__(key):
            return super().__getitem__(key)
        row = self._conn.execute(self._select, (key,)).fetchone()
        if row is None:
            self._misses += 1
            raise KeyError(key)
        val = {name: row[i] for i, name in enumerate(self._cols)}
        super().__setitem__(key, val)
        self._hits += 1
        return val


def _new_window_stats() -> dict[str, Any]:
    return {
        "n": 0,
        "correct_kills": 0,
        "correct_map": 0,
        "skip_no_actual": 0,
        "skip_no_pred": 0,
        "skip_zero_pred": 0,
        "buckets": defaultdict(lambda: {"n": 0, "correct_kills": 0, "correct_map": 0}),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=KW_DB)
    parser.add_argument("--pro-dir", type=Path, default=PRO_DIR)
    parser.add_argument("--out", type=Path, default=OUT_JSON)
    parser.add_argument("--md", type=Path, default=OUT_MD)
    parser.add_argument(
        "--parts-glob",
        default="7.41*.json",
        help="Only 7.41 + patch parts by default",
    )
    args = parser.parse_args()

    if not args.db.exists():
        print(f"missing dict: {args.db}", file=sys.stderr)
        return 1
    parts = sorted(args.pro_dir.glob(args.parts_glob))
    if not parts:
        print(f"no pro parts under {args.pro_dir}/{args.parts_glob}", file=sys.stderr)
        return 1

    tier1 = _tier1_ids()
    print(
        f"db={args.db} parts={len(parts)} tier1_ids={len(tier1)} windows={list(KILLS_WINDOWS)}",
        flush=True,
    )

    uri = f"{args.db.resolve().as_uri()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")
    cols = [row[1] for row in conn.execute("PRAGMA table_info(stats)").fetchall()]
    value_cols = [c for c in cols if c != "key"]
    heroes_data = StatsDict(conn, value_cols)

    stats: dict[str, dict[str, Any]] = {
        f"{s}_{e}": _new_window_stats() for s, e in KILLS_WINDOWS
    }

    n_parts = 0
    n_matches = 0
    n_tier1 = 0
    n_bad_draft = 0
    team_hits: dict[str, int] = defaultdict(int)
    t0 = time.monotonic()

    for part in parts:
        n_parts += 1
        data = orjson.loads(part.read_bytes())
        print(f"[{n_parts}/{len(parts)}] {part.name} matches={len(data)}", flush=True)
        for mid, match in data.items():
            if not isinstance(match, dict):
                continue
            n_matches += 1
            rt = match.get("radiantTeam") or {}
            dt = match.get("direTeam") or {}
            rid = _team_id(rt)
            did = _team_id(dt)
            has_t1 = (rid in tier1) or (did in tier1)
            if not has_t1:
                continue
            n_tier1 += 1
            if rid in tier1:
                team_hits[str((rt or {}).get("name") or rid)] += 1
            if did in tier1:
                team_hits[str((dt or {}).get("name") or did)] += 1

            r_by_pos, d_by_pos = extract_heroes_by_position(match)
            if r_by_pos is None or d_by_pos is None:
                n_bad_draft += 1
                continue
            radiant = _tokens(r_by_pos)
            dire = _tokens(d_by_pos)
            if len(radiant) != 5 or len(dire) != 5:
                n_bad_draft += 1
                continue

            preds = calculate_kills_window_advantage(
                radiant, dire, heroes_data, window=None
            )
            if not isinstance(preds, dict):
                for s, e in KILLS_WINDOWS:
                    stats[f"{s}_{e}"]["skip_no_pred"] += 1
                continue

            radiant_won = bool(match.get("didRadiantWin"))

            for start, end in KILLS_WINDOWS:
                label = f"{start}_{end}"
                st = stats[label]
                actual = _kills_window_diff(match, start, end)
                if actual is None:
                    st["skip_no_actual"] += 1
                    continue
                pred = preds.get(label)
                if not isinstance(pred, dict):
                    st["skip_no_pred"] += 1
                    continue
                exp = pred.get("expected_diff")
                try:
                    exp_f = float(exp)
                except (TypeError, ValueError):
                    st["skip_no_pred"] += 1
                    continue
                if exp_f == 0.0:
                    st["skip_zero_pred"] += 1
                    continue

                pred_radiant = exp_f > 0.0
                actual_radiant_leads = float(actual) > 0.0
                actual_dire_leads = float(actual) < 0.0
                # tie actual → kill-lead miss (strict >0)
                kill_ok = (pred_radiant and actual_radiant_leads) or (
                    (not pred_radiant) and actual_dire_leads
                )
                map_ok = (pred_radiant and radiant_won) or (
                    (not pred_radiant) and (not radiant_won)
                )

                st["n"] += 1
                if kill_ok:
                    st["correct_kills"] += 1
                if map_ok:
                    st["correct_map"] += 1
                b = _bucket(abs(exp_f))
                bb = st["buckets"][b]
                bb["n"] += 1
                if kill_ok:
                    bb["correct_kills"] += 1
                if map_ok:
                    bb["correct_map"] += 1

    elapsed = time.monotonic() - t0

    def _finalize(st: dict[str, Any]) -> dict[str, Any]:
        n = int(st["n"])
        buckets = {}
        for b, bb in sorted(st["buckets"].items(), key=lambda x: x[0]):
            bn = int(bb["n"])
            buckets[b] = {
                "n": bn,
                "kill_wr": (bb["correct_kills"] / bn) if bn else None,
                "base_map_wr": (bb["correct_map"] / bn) if bn else None,
                "correct_kills": int(bb["correct_kills"]),
                "correct_map": int(bb["correct_map"]),
            }
        return {
            "n": n,
            "kill_wr": (st["correct_kills"] / n) if n else None,
            "base_map_wr": (st["correct_map"] / n) if n else None,
            "correct_kills": int(st["correct_kills"]),
            "correct_map": int(st["correct_map"]),
            "skip_no_actual": int(st["skip_no_actual"]),
            "skip_no_pred": int(st["skip_no_pred"]),
            "skip_zero_pred": int(st["skip_zero_pred"]),
            "buckets": buckets,
        }

    windows_out = {lab: _finalize(st) for lab, st in stats.items()}

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db": str(args.db),
        "pro_dir": str(args.pro_dir),
        "parts_glob": args.parts_glob,
        "parts": [p.name for p in parts],
        "tier1_team_ids": len(tier1),
        "n_matches_scanned": n_matches,
        "n_tier1_matches": n_tier1,
        "n_bad_draft": n_bad_draft,
        "dict_cache_hits": getattr(heroes_data, "_hits", None),
        "dict_cache_misses": getattr(heroes_data, "_misses", None),
        "elapsed_sec": round(elapsed, 2),
        "windows": windows_out,
        "top_tier1_teams": sorted(team_hits.items(), key=lambda x: -x[1])[:20],
        "scoring": {
            "kill_wr": "sign(expected_diff) matches strict kill-lead in window; actual==0 lose",
            "base_map_wr": "predicted kill-lead side wins the map (didRadiantWin)",
            "zero_pred": "skipped from scored n",
        },
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    # Markdown summary
    lines = [
        "# kills_window pro Tier-1 backtest (7.41 + patch)",
        "",
        f"- generated: `{report['generated_at']}`",
        f"- parts: `{args.parts_glob}` ({len(parts)} files)",
        f"- scanned pro matches: **{n_matches}**",
        f"- with ≥1 Tier-1 team: **{n_tier1}**",
        f"- bad draft skipped: **{n_bad_draft}**",
        f"- dict: `{args.db}`",
        f"- elapsed: **{elapsed:.1f}s**",
        "",
        "## Per-window",
        "",
        "| window | n | kill WR | base map WR | skip no actual | skip no pred | skip 0 pred |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for lab, st in windows_out.items():
        kwr = st["kill_wr"]
        bwr = st["base_map_wr"]
        lines.append(
            f"| `{lab}` | {st['n']} | "
            f"{(kwr * 100):.2f}% | {(bwr * 100):.2f}% | "
            f"{st['skip_no_actual']} | {st['skip_no_pred']} | {st['skip_zero_pred']} |"
            if st["n"]
            else f"| `{lab}` | 0 | — | — | {st['skip_no_actual']} | {st['skip_no_pred']} | {st['skip_zero_pred']} |"
        )
    lines += ["", "## Buckets by |expected_diff|", ""]
    for lab, st in windows_out.items():
        lines.append(f"### `{lab}`")
        lines.append("")
        lines.append("| |expected| | n | kill WR | base map WR |")
        lines.append("|---|---:|---:|---:|")
        for b, bb in st["buckets"].items():
            if not bb["n"]:
                continue
            lines.append(
                f"| {b} | {bb['n']} | {bb['kill_wr']*100:.2f}% | {bb['base_map_wr']*100:.2f}% |"
            )
        lines.append("")
    lines += [
        "## Notes",
        "",
        "- kill WR: predicted side has **strictly more** kills in the half-open window.",
        "- base map WR: predicted kill-lead side wins the map.",
        "- actual kill-diff = 0 counts as miss for kill WR.",
        "- expected_diff = 0 is skipped (not scored).",
        "",
    ]
    args.md.write_text("\n".join(lines) + "\n")

    conn.close()

    print(json.dumps({"out": str(args.out), "md": str(args.md), **{k: windows_out[k] for k in windows_out}}, ensure_ascii=False, indent=2)[:4000], flush=True)
    print("EXIT:0", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
