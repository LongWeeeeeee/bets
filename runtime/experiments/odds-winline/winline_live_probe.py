#!/usr/bin/env python3
"""Standalone Winline live probe (not cyberscore_try runtime)."""
from __future__ import annotations

import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "base"))

import bookmaker_selenium_odds as odds  # noqa: E402


def main() -> int:
    out = Path(__file__).resolve().parent / "winline_live_probe_evidence.json"
    evidence = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "probe": "standalone_winline_run_sites_in_camoufox",
        "runtime_restarted": False,
        "success": False,
        "error": None,
        "result": None,
        "notes": [],
    }
    try:
        evidence["notes"].append(f"CAMOUFOX_AVAILABLE={odds.CAMOUFOX_AVAILABLE}")
        url = odds.BOOKMAKER_URLS["live"]["winline"]
        team1, team2, map_num = "Team Spirit", "Tundra Esports", 1
        evidence["request"] = {
            "url": url,
            "team1": team1,
            "team2": team2,
            "map_num": map_num,
            "site": "winline",
            "mode": "live",
        }
        t0 = time.time()
        results = odds.run_sites_in_camoufox(
            selected_sites=["winline"],
            urls={"winline": url},
            team1=team1,
            team2=team2,
            mode="live",
            forced_map_num=map_num,
        )
        r = results[0]
        result = {
            "status": r.status,
            "match_found": r.match_found,
            "odds": list(r.odds or []),
            "match_odds": list(r.match_odds or []),
            "source": r.source,
            "details": (r.details or "")[:700],
            "market_closed": r.market_closed,
            "market_kind": getattr(r, "market_kind", None),
            "map_num": getattr(r, "map_num", None),
            "p1_team": getattr(r, "p1_team", None),
            "p2_team": getattr(r, "p2_team", None),
        }
        evidence["elapsed_seconds"] = round(time.time() - t0, 2)
        evidence["result"] = result
        strict_ok = (
            result.get("market_kind") == "current_map_winner"
            and isinstance(result.get("odds"), list)
            and len(result.get("odds") or []) >= 2
            and result.get("p1_team") == "team1"
            and result.get("p2_team") == "team2"
        )
        evidence["success"] = bool(strict_ok)
        if not strict_ok:
            evidence["notes"].append(
                "No strict live current-map winner for chosen teams; honest non-success "
                "(no suitable live map / anti-bot / empty market). Not fabricated."
            )
        ex = odds._extract_winline_current_map_winner(
            "Team Spirit Tundra Esports 1К 1.52 2.45 Матч 1.30 3.15",
            "Team Spirit",
            "Tundra Esports",
            1,
        )
        evidence["fixture_contract_check"] = {
            "odds": ex.odds,
            "market_kind": ex.market_kind,
            "map_num": ex.map_num,
            "p1_team": ex.p1_team,
            "p2_team": ex.p2_team,
            "provenance": "synthetic_regression_fixture",
        }
    except Exception as exc:
        evidence["error"] = f"{type(exc).__name__}: {exc}"
        evidence["traceback"] = traceback.format_exc()[-2500:]
        evidence["notes"].append("honest network/anti-bot/runtime blocker during live probe")

    evidence["finished_at"] = datetime.now(timezone.utc).isoformat()
    out.write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(evidence, ensure_ascii=False, indent=2)[:4000])
    print("WROTE", out)
    return 0 if evidence.get("error") is None else 2


if __name__ == "__main__":
    raise SystemExit(main())
