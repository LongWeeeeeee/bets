"""Reconcile real delivery rows without treating kills bets as map winners."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from base.tests.test_bet_dispatch_ledger import (  # noqa: E402
    _dispatch_through_decision,
    _load_row0,
    runtime,
)
from scripts.ops import reconcile_bet_ledger as reconcile  # noqa: E402


def test_default_ledger_uses_production_base_runtime():
    assert reconcile.DEFAULT_LEDGER == REPO_ROOT / "base/runtime/bet_dispatch_ledger.jsonl"


@pytest.fixture
def sent_ledger(monkeypatch, tmp_path):
    captured = _load_row0()
    monkeypatch.setattr(runtime.time, "time", lambda: captured["ts"])
    match_id = captured["match_key"].split("/")[-1].split(".")[0]
    deliver = runtime._ml_dispatch_deliver_decision

    def deliver_numeric_match(decision, **kwargs):
        # The shared harness uses a nonnumeric placeholder. Give the REAL
        # producer a numeric match key so applied_maps can join its output.
        kwargs["match_key"] = captured["match_key"]
        return deliver(decision, **kwargs)

    monkeypatch.setattr(runtime, "_ml_dispatch_deliver_decision", deliver_numeric_match)
    rows = []
    for market, rule in (
        ("win", "win_single_model_confirm"),
        ("kills_total", "kills_early_win_kills30"),
        ("kills_window", "kills_panel_window"),
    ):
        producer_dir = tmp_path / market
        producer_dir.mkdir()
        row, _ = _dispatch_through_decision(
            monkeypatch, producer_dir, market=market, rule=rule, target="YACHE123")
        assert row["match_id"] == match_id
        rows.append(row)
    assert len({(r["match_id"], r["map_num"], r["side"], r["ts"]) for r in rows}) == 1

    # Verbatim STAR/prematch row shape from the first exact-row producer test,
    # with pre-08.10 market/rule fields absent; model values are the capture.
    rows.append({
        "ts": captured["ts"],
        "match_key": "dltv.org/matches/8946860406.0",
        "base_key": "dltv.org/matches/8946860406",
        "match_id": "8946860406",
        "map_num": 2,
        "reason": "star_signal_sent_now_prematch_model",
        "side": "radiant",
        "target_team_name": "Team Liquid",
        "radiant_team_name": "Team Liquid",
        "dire_team_name": "Aurora Gaming",
        "radiant_tier": 1,
        "dire_tier": 1,
        "stake_multiplier": 1.0,
        "star_wr_pct": None,
        "prematch_model_index": captured["index"],
        "prematch_model_confidence": captured["confidence"],
        "late_model_side": "radiant",
        "game_time": captured["game_time"],
        "expected_wr": captured["expected_wr"],
        "min_odds": captured["min_odds"],
        "price_snapshot": None,
    })
    # Historical ml_dispatch schema: the same delivered row, before the
    # producer started recording market/rule. This must require opt-in.
    rows.append({k: v for k, v in rows[0].items() if k not in ("market", "rule")})
    ledger = tmp_path / "bet_dispatch_ledger.jsonl"
    ledger.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    progress = tmp_path / "live_elo_progress.json"
    progress.write_text(json.dumps({"applied_maps": {
        match_id: {"match_id": match_id, "radiant_win": False},
    }}), encoding="utf-8")
    return ledger, progress


@pytest.mark.parametrize("include_legacy", [False, True])
def test_reconcile_scores_only_map_winners(monkeypatch, capsys, sent_ledger, include_legacy):
    ledger, progress = sent_ledger
    args = ["reconcile_bet_ledger.py", "--ledger", str(ledger), "--progress", str(progress)]
    if include_legacy:
        args.append("--include-legacy-ml-dispatch")
    monkeypatch.setattr(sys, "argv", args)
    capsys.readouterr()  # discard producer delivery messages
    reconcile.main()
    output = capsys.readouterr().out
    n = 2 if include_legacy else 1
    by_path = output.split("=== By path ===")[1].split("===")[0]
    assert re.search(rf"ml_dispatch\s+n=\s*{n} wins=\s*{n} wr=100\.0%", by_path)
    assert re.search(r"star_signal_sent_now_prematch_model\s+n=\s*1 wins=\s*0 wr=\s*0\.0%", by_path)
    assert "Ledger rows: 5 " in output
    assert "not map-winner: 2" in output
    assert "ml_dispatch rows without market (before 08.10.2026, may be kills bets): 1" in output
    by_market = output.split("=== By market ===")[1].split("===")[0]
    assert re.search(r"win\s+n=\s*1 wins=\s*1 wr=100\.0%", by_market)
    assert re.search(rf"legacy\s+n=\s*{n} wins=\s*{int(include_legacy)}", by_market)
    for title in ("By tier pair", "By side", "By month"):
        assert f"=== {title} ===" in output


def test_current_star_null_market_is_also_legacy(monkeypatch, capsys, sent_ledger):
    ledger, progress = sent_ledger
    rows = reconcile._load_ledger(ledger)
    star = next(r for r in rows if r["reason"] != "ml_dispatch")
    star["market"] = None  # current producer schema, see exact-row test
    ledger.write_text(json.dumps(star) + "\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["reconcile", "--ledger", str(ledger), "--progress", str(progress)])
    capsys.readouterr()
    reconcile.main()
    output = capsys.readouterr().out
    assert "not map-winner: 0" in output
    assert re.search(r"legacy\s+n=\s*1 wins=\s*0", output)


@pytest.mark.parametrize("reason", [
    "early_winner_kills_window_sent",
    "star_signal_sent_now_kills_window_policy",
    "star_signal_sent_now_kills_dual",
    "tempo_over_fallback_sent",
])
def test_legacy_kills_reasons_are_not_map_winner(monkeypatch, capsys, sent_ledger, reason):
    # Reasons written by the pre-ml kills paths of base/cyberscore_try.py, which
    # set no market (Opus review of this change, 08.10.2026).
    ledger, progress = sent_ledger
    rows = reconcile._load_ledger(ledger)
    star = next(r for r in rows if r["reason"] != "ml_dispatch")
    kills = dict(star, reason=reason)
    ledger.write_text(json.dumps(star) + "\n" + json.dumps(kills) + "\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["reconcile", "--ledger", str(ledger), "--progress", str(progress)])
    capsys.readouterr()
    reconcile.main()
    output = capsys.readouterr().out
    assert "not map-winner: 1" in output
    assert reason not in output.split("=== By path ===")[1]
