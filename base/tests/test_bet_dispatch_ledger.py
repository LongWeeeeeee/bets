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
