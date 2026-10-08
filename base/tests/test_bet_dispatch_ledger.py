"""Единый исход-леджер по ВСЕМ отправленным ставкам (STAR + prematch model).

01.09.2026 инспекция нашла самую большую дыру в измерении: свой журнал был
только у ставок предматчевой модели (`runtime/prematch_model_bet_sent.jsonl`)
— STAR-ставки не сверялись ни с чем вообще, и их живой винрейт был неизмерим.

`_deliver_and_persist_signal` (cyberscore_try.py) — единственная точка
доставки для ВСЕХ путей ставок с 28.08.2026 (гейты `_win_model_reject_for_delivery`
и `_late_win_model_reject_for_delivery` уже полагаются на это же свойство).
Строка леджера пишется туда сразу после подтверждённого `send_message`, ДО
`add_url`/`defer_add_url`-ветвления — общей для всех путей.

Тест проверяет boundary: реальный вызов `_deliver_and_persist_signal` (не
внутренний билдер строки в отрыве), исходящая отправка (`send_message`) и
файловая персистенция (`add_url`) замоканы, точная JSON-строка в
`runtime/bet_dispatch_ledger.jsonl` (путь переопределён на tmp_path через
`BET_DISPATCH_LEDGER_PATH`) сверяется по ключам и значениям.

Payload собран из реальной строки ledger `prematch_model_bet_sent.jsonl` —
`base/tests/fixtures/prematch_model_bet_sent_dup_pair_20260901.json` (capture
date 2026-09-01, команда захвата — в `_capture` внутри файла); отдельной
фикстуры с `stake_multiplier_context` для этого пути в base/tests нет (грепнуто
`stake_multiplier_context` по всем тестам — путь `star_signal_sent_now_prematch_model`
нигде не фикстурирован), поэтому контекст и `add_url_details` собраны вручную
по форме, которую реально строит `_try_dispatch_prematch_model_bet`
(cyberscore_try.py, `delivery_stake_context` ~27619 и `details` ~27567).
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
import cyberscore_try as runtime  # noqa: E402

FIXTURE_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "prematch_model_bet_sent_dup_pair_20260901.json"
)


def _load_row0() -> dict:
    with open(FIXTURE_PATH, encoding="utf-8") as f:
        return json.load(f)["rows"][0]


def _read_ledger_lines(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_deliver_and_persist_signal_writes_exact_ledger_row(monkeypatch, tmp_path) -> None:
    row = _load_row0()
    ledger_path = tmp_path / "bet_dispatch_ledger.jsonl"
    monkeypatch.setattr(runtime, "BET_DISPATCH_LEDGER_PATH", str(ledger_path), raising=False)

    match_key = row["match_key"]  # "dltv.org/matches/8946860406.0"
    message_text = (
        f"СТАВКА НА {row['radiant_team']} x1\n"
        f"{row['radiant_team']} VS {row['dire_team']}\n"
        "🤖 ML-модель: Radiant 63.5% | ML от кэфа: 1.54\n"
    )
    # Форма delivery_stake_context из _try_dispatch_prematch_model_bet (~27619).
    stake_multiplier_context = {
        "target_side": row["side"],
        "stake_team_name": row["radiant_team"],
        "radiant_team_name": row["radiant_team"],
        "dire_team_name": row["dire_team"],
        "late_model_side": "radiant",
    }
    # Форма details из _try_dispatch_prematch_model_bet (~27567).
    add_url_details = {
        "status": "live",
        "dispatch_mode": "immediate_prematch_model",
        "game_time": row["game_time"],
        "target_side": row["side"],
        "prematch_model_index": row["index"],
        "prematch_model_confidence": row["confidence"],
        "prematch_model_min_odds": row["min_odds"],
        "prematch_model_expected_wr": row["expected_wr"],
    }

    monkeypatch.setattr(runtime, "send_message", lambda *a, **k: True)
    monkeypatch.setattr(runtime, "add_url", lambda *a, **k: None)

    before_ts = int(time.time())
    delivered = runtime._deliver_and_persist_signal(
        match_key,
        message_text,
        add_url_reason="star_signal_sent_now_prematch_model",
        add_url_details=add_url_details,
        skip_bookmaker_prepare=True,
        map_num=row["map_num"],
        selected_side=row["side"],
        stake_multiplier_context=stake_multiplier_context,
    )
    after_ts = int(time.time())

    assert delivered is True
    lines = _read_ledger_lines(ledger_path)
    assert len(lines) == 1, lines
    entry = lines[0]

    assert before_ts <= entry.pop("ts") <= after_ts
    assert entry == {
        "match_key": "dltv.org/matches/8946860406.0",
        "base_key": "dltv.org/matches/8946860406",
        "match_id": "8946860406",
        "map_num": 2,
        "reason": "star_signal_sent_now_prematch_model",
        "side": "radiant",
        "target_team_name": "Team Liquid",
        "radiant_team_name": "Team Liquid",
        "dire_team_name": "Aurora Gaming",
        # Обе команды реально tier 1 в id_to_names (проверено вызовом
        # _get_team_tier_by_name 02.09.2026 — прямой grep по строке "Team
        # Liquid" ничего не находит, потому что запись живёт под алиасом).
        "radiant_tier": 1,
        "dire_tier": 1,
        "stake_multiplier": 1.0,
        "star_wr_pct": None,
        "prematch_model_index": row["index"],
        "prematch_model_confidence": row["confidence"],
        "late_model_side": "radiant",
        "game_time": row["game_time"],
        # Card ingame-8a5i: the STAR/prematch path has no ml_dispatch market or
        # rule; its floor comes from the prematch_model_* details.
        "market": None,
        "rule": None,
        "expected_wr": row["expected_wr"],
        "min_odds": row["min_odds"],
        # Нет прогретого bookmaker prefetch-снимка для этого match_key в тесте
        # (skip_bookmaker_prepare=True его и не создаёт) — цена недоступна.
        "price_snapshot": None,
    }


def test_ledger_write_never_raises_when_build_fails(monkeypatch, tmp_path) -> None:
    """Потеря строки леджера не должна ронять уже отправленный сигнал."""
    ledger_path = tmp_path / "bet_dispatch_ledger.jsonl"
    monkeypatch.setattr(runtime, "BET_DISPATCH_LEDGER_PATH", str(ledger_path), raising=False)
    monkeypatch.setattr(runtime, "send_message", lambda *a, **k: True)
    monkeypatch.setattr(runtime, "add_url", lambda *a, **k: None)
    monkeypatch.setattr(
        runtime,
        "_build_bet_dispatch_ledger_entry",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    delivered = runtime._deliver_and_persist_signal(
        "dltv.org/matches/9999999999.0",
        "СТАВКА НА Team A x1\nTeam A VS Team B\n🤖 ML-модель: Radiant 63.5%\n",
        add_url_reason="star_signal_sent_now_prematch_model",
        skip_bookmaker_prepare=True,
        selected_side="radiant",
        stake_multiplier_context={"target_side": "radiant", "stake_team_name": "Team A"},
    )

    assert delivered is True, "билдер леджера упал, но доставка не должна была прерваться"
    assert not ledger_path.exists()


# --- send-time Winline price from the current-map poller state (card L1) ---
# 08.10.2026: price_snapshot was null on 544/544 prod ledger rows, because the
# snapshot read only the old multi-site prefetch. The live price lives in the
# poller orientation state read by _ml_dispatch_fresh_winline_price.
# Fixture: captured 2026-09-15 by the Winline current-map poller (see the
# capture note inside the file), shared with test_ml_win_floor_delivery.py.
WINLINE_FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "winline_yangon_yache_map3_20260915.json"
)


def _winline_poll_delivery(monkeypatch, tmp_path, *, seed_quote, target):
    first = json.loads(WINLINE_FIXTURE.read_text(encoding="utf-8"))["messages"][0]
    key = first["canonical_key"]
    p1, p2 = first["observation"]["output_pair"]
    state = {}
    if seed_quote:
        state[key] = {"p1": p1, "p2": p2, "status": "open",
                      "last_quote_mono": time.monotonic()}
    ledger_path = tmp_path / "bet_dispatch_ledger.jsonl"
    monkeypatch.setattr(runtime, "_winline_odds_orientation_state", state)
    monkeypatch.setattr(runtime, "BET_DISPATCH_LEDGER_PATH", str(ledger_path), raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False)
    # The captured fixture team is denylisted since 29.09; the ledger is not
    # under test there.
    monkeypatch.setattr(runtime, "_is_denylisted_bet_team_name", lambda *_a, **_k: False)
    monkeypatch.setattr(runtime, "send_message", lambda *a, **k: True)
    monkeypatch.setattr(runtime, "add_url", lambda *a, **k: None)
    ctx = {
        "origin": "ml_dispatch", "ml_market": "kills_total",  # no floor gate: ledger only
        "target_side": "radiant" if target == "YANGON GALACTICOS" else "dire",
        "stake_team_name": target,
        "radiant_team_name": "YANGON GALACTICOS", "dire_team_name": "YACHE123",
        "game_time_seconds": 600,
    }
    delivered = runtime._deliver_and_persist_signal(
        "dltv.org/matches/yangon-yache.3",
        f"СТАВКА НА {target} x1\n",
        add_url_reason="ml_dispatch",
        skip_bookmaker_prepare=True,
        map_num=3,
        selected_side=ctx["target_side"],
        stake_multiplier_context=ctx,
    )
    assert delivered is True
    lines = _read_ledger_lines(ledger_path)
    assert len(lines) == 1, lines
    return lines[0], (p1, p2)


def test_ledger_records_send_time_winline_poll_price(monkeypatch, tmp_path) -> None:
    row, (p1, p2) = _winline_poll_delivery(
        monkeypatch, tmp_path, seed_quote=True, target="YACHE123")
    assert row["price_snapshot"] == {
        "p1": p1, "p2": p2, "selected": p2, "source": "winline_poll",
        "market": "map_winner", "game_time_s": 600}
    assert p1 != p2  # the target side is really distinguished


def test_ledger_poll_price_radiant_target_gets_p1(monkeypatch, tmp_path) -> None:
    row, (p1, p2) = _winline_poll_delivery(
        monkeypatch, tmp_path, seed_quote=True, target="YANGON GALACTICOS")
    assert row["price_snapshot"]["selected"] == p1
    assert row["price_snapshot"]["source"] == "winline_poll"


def test_ledger_without_fresh_quote_has_null_snapshot_and_no_exception(
        monkeypatch, tmp_path) -> None:
    row, _ = _winline_poll_delivery(
        monkeypatch, tmp_path, seed_quote=False, target="YACHE123")
    assert row["price_snapshot"] is None
    assert row["target_team_name"] == "YACHE123"


# --- market / rule on every ml_dispatch ledger row (card ingame-8a5i) ---
# 08.10.2026: 43 of 552 prod ledger rows looked like duplicates of a WIN row
# (same match, map, side, target): they were kills_total / kills_window bets
# delivered in the same tick (ml_dispatch_decisions LEGION|Blasterbl map 2,
# 1791481604-09), and since 8e49e2ac they carried the map-winner price_snapshot
# (2.4) with nothing saying the bet was not a map-winner bet. The rows now name
# their market, rule and floor; price_snapshot stays the map-winner quote.
def _dispatch_through_decision(monkeypatch, tmp_path, *, market, rule, target):
    from types import SimpleNamespace
    first = json.loads(WINLINE_FIXTURE.read_text(encoding="utf-8"))["messages"][0]
    key = first["canonical_key"]
    p1, p2 = first["observation"]["output_pair"]
    monkeypatch.setattr(runtime, "_winline_odds_orientation_state", {
        key: {"p1": p1, "p2": p2, "status": "open", "last_quote_mono": time.monotonic()}})
    ledger_path = tmp_path / "bet_dispatch_ledger.jsonl"
    monkeypatch.setattr(runtime, "BET_DISPATCH_LEDGER_PATH", str(ledger_path), raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(runtime, "_bookmaker_prepare_message_for_delivery",
                        lambda _key, text, **_kw: (text, True, "disabled", None))
    monkeypatch.setattr(runtime, "_is_denylisted_bet_team_name", lambda *_a, **_k: False)
    for name in ("_half_stake_elo_underdog_reject_for_delivery",
                 "_win_model_reject_for_delivery", "_late_win_model_reject_for_delivery"):
        monkeypatch.setattr(runtime, name, lambda *_a, **_kw: None)
    sent = []
    monkeypatch.setattr(runtime, "send_message", lambda text, **_k: sent.append(text) or True)
    monkeypatch.setattr(runtime, "add_url", lambda *a, **k: None)
    monkeypatch.setattr(runtime, "_signal_fingerprint_try_reserve", lambda *_a: (True, "fp"))
    monkeypatch.setattr(runtime, "_signal_fingerprint_mark_sent", lambda *_a: None)
    monkeypatch.setattr(runtime, "decelerate_winline_current_map_polling", lambda *_a: None)
    side = "Radiant" if target == "YANGON GALACTICOS" else "Dire"
    decision = SimpleNamespace(
        market=market, target_side=side, target_team=target, rule=rule,
        reasons=["window=5_15"] if market == "kills_window" else [],
        expected_wr=0.643, min_odds=1.56, floor_informational=True)
    runtime._ml_dispatch_deliver_decision(
        decision, match_key="dltv.org/matches/yangon-yache.3", base_url="series",
        ctx_map_num=3, resolved_map_num=3, radiant_team_name="YANGON GALACTICOS",
        dire_team_name="YACHE123", live_league={}, top="", mid="", bot="",
        protracker_payload=None, team_elo_block="", game_time_seconds=600,
        radiant_lead=0, early_output=None, mid_output=None, all_output=None,
        radiant_heroes_and_pos=None, dire_heroes_and_pos=None,
        full_message_text="СТАВКА НА YACHE123 x1\nYANGON GALACTICOS VS YACHE123",
        ml_laning_line="", all_model_line="",
        ledger=SimpleNamespace(add=lambda _k: None, save=lambda: None))
    assert len(sent) == 1, sent
    lines = _read_ledger_lines(ledger_path)
    assert len(lines) == 1, lines
    return lines[0], (p1, p2)


@pytest.mark.parametrize("market,rule", [
    ("kills_total", "kills_early_win_kills30"),
    ("kills_window", "kills_panel_window"),
    ("win", "win_single_model_confirm"),
])
def test_ml_dispatch_ledger_row_names_market_rule_and_floor(monkeypatch, tmp_path, market, rule):
    row, (p1, p2) = _dispatch_through_decision(
        monkeypatch, tmp_path, market=market, rule=rule, target="YACHE123")
    assert (row["market"], row["rule"]) == (market, rule)
    assert (row["expected_wr"], row["min_odds"]) == (0.643, 1.56)
    # The snapshot is still the map-winner quote at send time and says so,
    # so a kills row can be told apart from the price of its own market.
    assert row["price_snapshot"]["market"] == "map_winner"
    assert row["price_snapshot"]["selected"] == p2
