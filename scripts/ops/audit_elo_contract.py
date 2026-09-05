#!/usr/bin/env python3
"""Read-only parity audit on a real snapshot and recorded pro lineups.

This checks serving consistency, NOT predictive accuracy: the current snapshot
has already seen the fixture outcomes. Never report its retrospective hit rate.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from ELO.array_model import load_read_model, load_team_names
from ELO.domain import LeagueTier
from ELO.live_team_strength import DEFAULT_SNAPSHOT_PATH, DEFAULT_RUNTIME_MODEL_STATE_PATH, DEFAULT_LIVE_DELTA_PATH
from ELO.models import prematch_lineup_summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT_PATH)
    parser.add_argument("--runtime-state", type=Path, default=DEFAULT_RUNTIME_MODEL_STATE_PATH)
    parser.add_argument("--delta", type=Path, default=DEFAULT_LIVE_DELTA_PATH)
    parser.add_argument("--cards", type=Path, default=ROOT / "base/tests/fixtures/prematch_h2h_teamid_cards.json")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print("Loading read-only ELO model and team names", flush=True)
    model = load_read_model(args.snapshot, args.runtime_state, delta_path=args.delta)
    names = load_team_names(args.snapshot)
    cards = json.loads(args.cards.read_text())
    rows, skipped = [], []
    for card in cards:
        rn = names.get(int(card["radiant_team_id"]))
        dn = names.get(int(card["dire_team_id"]))
        if not rn or not dn:
            skipped.append({"match_id": card["mid"], "reason": "team_name_missing"})
            continue
        old_strengths = []
        for side, name in (("radiant", rn), ("dire", dn)):
            pairs = sorted(zip(card[f"{side}_accounts"], [f"POSITION_{i}" for i in range(1, 6)]))
            old_strengths.append(float(model.preview_team_strength(
                team_id=None, team_name=name, player_ids=tuple(a for a, _ in pairs),
                player_positions=tuple(p for _, p in pairs), tier=LeagueTier.TIER3,
                timestamp=int(card["ts"]),
            )["team_strength"]))
        # Exact pre-change ML formula; do not use the new helper as its oracle.
        old_feature = (old_strengths[0] - old_strengths[1]) / 400.0
        summary = prematch_lineup_summary(
            model, radiant_team_name=rn, dire_team_name=dn,
            radiant_account_ids=card["radiant_accounts"], dire_account_ids=card["dire_accounts"],
            timestamp=int(card["ts"]),
        )
        if summary is None:
            raise RuntimeError(f"Valid recorded lineup refused: {card['mid']}")
        error = abs(summary["hybrid_strength"] - old_feature)
        if error > 1e-12:
            raise RuntimeError(f"ML feature changed: {card['mid']}: {error}")
        rows.append({"match_id": card["mid"], "radiant": rn, "dire": dn,
                     "elo_diff": summary["elo_diff"], "old_ml_feature": old_feature,
                     "new_ml_feature": summary["hybrid_strength"], "absolute_error": error})
    if not rows:
        raise RuntimeError("No lineups evaluated; an empty audit is not a pass")
    report = {"kind": "elo_serving_parity", "snapshot": str(args.snapshot.resolve()),
              "cards": str(args.cards.resolve()), "evaluated": len(rows), "skipped": skipped,
              "max_absolute_feature_error": max(r["absolute_error"] for r in rows),
              "predictive_accuracy_measured": False, "rows": rows}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(args.output.name + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, args.output)
    print(f"PASS: {len(rows)} recorded lineups; max feature error={report['max_absolute_feature_error']}; skipped={len(skipped)}", flush=True)
    print(f"Report: {args.output}", flush=True)


if __name__ == "__main__":
    main()
