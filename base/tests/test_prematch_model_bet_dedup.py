"""Дедуп ставки предматчевой модели не должен пропускать одну карту дважды —
и не должен молча ронять ставку на ДРУГОЙ реальный матч.

01.09.2026, ledger `runtime/prematch_model_bet_sent.jsonl` на serv1: 204
строки, 126 уникальных (match_id, map_num), 64 карты отправлены дважды и
более (78 лишних отправок). Пример — `dltv.org/matches/8946860406`, карта 2
(Team Liquid vs Aurora Gaming): отправлена в -79 с и повторно 72 с спустя в
+4 с, тот же индекс/confidence/команды/map_num — только sourcetv-суффикс
`.0` → `.1` (растущий uniq_score того же живого опроса карты) успел смениться.

Причина: `dedup_key = str(match_key)` в `_try_dispatch_prematch_model_bet`
(cyberscore_try.py:27505, коммит 16072d8) держал суффикс `.N` целиком, а
`_skip_dispatch_for_processed_url` следом ту же дыру не закрывает —
`_is_url_processed` тоже сравнивает URL целиком без среза суффикса.

ПЕРВЫЙ фикс (16072d8) был неверным: он предпочитал `_map_pair_fingerprint`
(ключ «команды+номер карты», БЕЗ match_id). Ревью нашло, что на sourcetv-ветке
`_signal_fingerprint_register` стоит ПОСЛЕ блока `if is_sourcetv_card: ...
else: ...`, а не внутри его `else` — вызывается всегда, и кэш в проде живой.
Значит ключ «teamA|teamB|mapN» реально схлопывает РАЗНЫЕ матчи одной пары
команд на одном номере карты: в ledger нашёлся `teamspirit|teamvision|map2`
на двух разных match_id (8957272720 и 8960655084, 44 ч разницы) — второй бет
молча терялся бы (ставился False вместо True).

Текущий фикс: дедуп-ключ = `_signal_fingerprint_registry_key(match_key)`
(URL без `.N`-суффикса — у Dota 2 своя Steam match_id на каждую карту серии,
поэтому это уже per-map) плюс `|map{N}`, когда номер карты известен. Раздел
разных матчей даёт match_id, а не команды. Запасного пути на «сырой»
суффиксный ключ больше нет — попадание в него означает возврат старого бага.

Три реальные строки ledger — в
`base/tests/fixtures/prematch_model_bet_sent_dup_pair_20260901.json`
(capture date 2026-09-01, команда захвата — в `_capture` внутри файла).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402
import win_model_veto  # noqa: E402

FIXTURE_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "prematch_model_bet_sent_dup_pair_20260901.json"
)


def _load_dup_pair_rows() -> list[dict]:
    with open(FIXTURE_PATH, encoding="utf-8") as f:
        payload = json.load(f)
    rows = payload["rows"]
    assert len(rows) == 2
    return rows


@pytest.fixture(autouse=True)
def _reset_prematch_model_bet_dedup_state():
    """Изоляция от других тестов/прогонов: in-memory дедуп-сет модели и
    карточный отпечаток общего реестра — модульные глобалы, живущие дольше
    одного теста. Очистка идёт ДО тела теста — тесты, которым нужен
    прогретый `_SIGNAL_DEDUP_FINGERPRINTS` (как в проде), заполняют его сами
    внутри теста, уже после этой очистки."""
    runtime._prematch_model_bet_sent_urls.clear()
    runtime._SIGNAL_DEDUP_FINGERPRINTS.clear()
    yield
    runtime._prematch_model_bet_sent_urls.clear()
    runtime._SIGNAL_DEDUP_FINGERPRINTS.clear()


def _dispatch_row(
    row: dict,
    *,
    monkeypatch: pytest.MonkeyPatch,
    match_key: str | None = None,
    live_league: dict | None = None,
    radiant_team_name: str | None = None,
    dire_team_name: str | None = None,
) -> bool:
    """Прогнать реальный `_try_dispatch_prematch_model_bet` на одной строке
    ledger (или её переопределении). Мокается только исходящая доставка
    (`_deliver_and_persist_signal` — телеграм-отправка + add_url) и дисковый
    `_skip_dispatch_for_processed_url` (отдельный, не связанный с этим багом
    гейт, читающий боевой map_id_check.txt); сам дедуп-путь — настоящий код.
    """
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *a, **kw: False)
    rad = radiant_team_name if radiant_team_name is not None else row["radiant_team"]
    dire = dire_team_name if dire_team_name is not None else row["dire_team"]
    return runtime._try_dispatch_prematch_model_bet(
        match_key=match_key if match_key is not None else row["match_key"],
        status="live",
        radiant_team_name=rad,
        dire_team_name=dire,
        live_league=live_league if live_league is not None else {"map_num": row["map_num"]},
        top="",
        mid="",
        bot="",
        protracker_payload={},
        team_elo_block="",
        game_time_seconds=row["game_time"],
        radiant_lead=0,
        full_message_text=(
            f"СТАВКА НА {rad} x1\n"
            f"{rad} VS {dire}\n"
        ),
    )


def _make_bet(row: dict) -> dict:
    return {
        "side": row["side"],
        "index": row["index"],
        "confidence": row["confidence"],
        "min_odds": row["min_odds"],
        "expected_wr": row["expected_wr"],
    }


def test_same_map_resent_under_dot_n_suffix_sends_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Живой баг: одна и та же карта под `.0` и `.1` шлёт ставку один раз."""
    row0, row1 = _load_dup_pair_rows()
    assert row0["map_num"] == row1["map_num"]
    assert row0["match_key"].rsplit(".", 1)[0] == row1["match_key"].rsplit(".", 1)[0]

    monkeypatch.setattr(runtime, "PREMATCH_MODEL_BET_ENABLED", True)
    monkeypatch.setattr(win_model_veto, "model_bet", lambda *_blocks: _make_bet(row0))

    sent = MagicMock(return_value=True)  # единственная точка "исходящей отправки"
    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", sent)

    result_first = _dispatch_row(row0, monkeypatch=monkeypatch)
    result_second = _dispatch_row(row1, monkeypatch=monkeypatch)

    assert result_first is True
    assert result_second is False, "повторный опрос той же карты не должен слать вторую ставку"
    assert sent.call_count == 1, (
        f"ожидалась ровно ОДНА отправка на карту, ушло {sent.call_count}"
    )


def test_same_map_dedup_holds_even_with_pair_fingerprint_cache_warm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Прод-условие (a): `_signal_fingerprint_register` уже прогрел
    `_SIGNAL_DEDUP_FINGERPRINTS` для обоих `.0`/`.1` ключей ДО вызова дедупа
    (он стоит в вызывающем цикле раньше, безусловно на sourcetv-ветке тоже).
    Дедуп по-прежнему держит ровно одну отправку — ключ больше не читает
    этот кэш вовсе, так что его прогретость ни на что не влияет."""
    row0, row1 = _load_dup_pair_rows()

    monkeypatch.setattr(runtime, "PREMATCH_MODEL_BET_ENABLED", True)
    monkeypatch.setattr(win_model_veto, "model_bet", lambda *_blocks: _make_bet(row0))
    sent = MagicMock(return_value=True)
    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", sent)

    # Симулируем прод: register() вызывается для каждого uniq-URL до дедупа,
    # первая карта уже выиграна (1:0), поэтому map_num = 1+0+1 = 2 — как в
    # фикстуре. НЕ чистим кэш вручную — тест намеренно оставляет его тёплым.
    for row in (row0, row1):
        runtime._signal_fingerprint_register(
            row["match_key"], row["radiant_team"], row["dire_team"], 1, 0,
        )

    result_first = _dispatch_row(row0, monkeypatch=monkeypatch)
    result_second = _dispatch_row(row1, monkeypatch=monkeypatch)

    assert result_first is True
    assert result_second is False
    assert sent.call_count == 1


def test_different_matches_same_team_pair_and_map_num_both_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(b) Реальный случай из ledger: `teamspirit|teamvision|map2` встречается
    на ДВУХ разных match_id (8957272720 и 8960655084, 44 ч разницы) — это
    два разных матча, и оба обязаны получить ставку. На старом ключе
    `_map_pair_fingerprint` (команды+номер карты, без match_id, коммит
    16072d8) второй бет схлопывался с первым и молча терялся."""
    monkeypatch.setattr(runtime, "PREMATCH_MODEL_BET_ENABLED", True)
    bet = {
        "side": "radiant",
        "index": 15.0,
        "confidence": 0.65,
        "min_odds": 1.4,
        "expected_wr": 0.62,
    }
    monkeypatch.setattr(win_model_veto, "model_bet", lambda *_blocks: dict(bet))
    sent = MagicMock(return_value=True)
    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", sent)

    match_key_a = "dltv.org/matches/8957272720.0"
    match_key_b = "dltv.org/matches/8960655084.0"
    rad, dire = "Team Spirit", "TEAM VISION"
    live_league = {"map_num": 2}

    # Прод-условие: register() уже отметил ОБА матча одинаковым отпечатком
    # «команды+map2», потому что у обоих 1:0 в серии на момент опроса.
    runtime._signal_fingerprint_register(match_key_a, rad, dire, 1, 0)
    runtime._signal_fingerprint_register(match_key_b, rad, dire, 1, 0)

    result_a = _dispatch_row(
        {"match_key": match_key_a, "radiant_team": rad, "dire_team": dire,
         "map_num": 2, "game_time": -79},
        monkeypatch=monkeypatch, match_key=match_key_a, live_league=live_league,
    )
    result_b = _dispatch_row(
        {"match_key": match_key_b, "radiant_team": rad, "dire_team": dire,
         "map_num": 2, "game_time": -79},
        monkeypatch=monkeypatch, match_key=match_key_b, live_league=live_league,
    )

    assert result_a is True
    assert result_b is True, (
        "разные match_id одной пары команд на одном номере карты — оба матча "
        "реальны, второй бет не должен теряться"
    )
    assert sent.call_count == 2


def test_same_map_dedup_with_series_wins_shaped_live_league(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(c) Реальная форма sourcetv `live_league_data` (cyberscore_try.py:
    34406-34417) не несёт ключа `map_num` вовсе — только
    `radiant_series_wins`/`dire_series_wins` (+ `series_game`, не входящий в
    список явных ключей `_bookmaker_infer_map_num`). Номер карты обязан
    выводиться из счёта серии, и дедуп на этой форме тоже должен держать
    ровно одну отправку на карту."""
    row0, row1 = _load_dup_pair_rows()
    assert row0["map_num"] == 2

    monkeypatch.setattr(runtime, "PREMATCH_MODEL_BET_ENABLED", True)
    monkeypatch.setattr(win_model_veto, "model_bet", lambda *_blocks: _make_bet(row0))
    sent = MagicMock(return_value=True)
    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", sent)

    sourcetv_live_league = {
        "league_id": 17911,
        "series_id": 8946860406,
        "radiant_team_id": 1,
        "dire_team_id": 2,
        "radiant_series_wins": 1,
        "dire_series_wins": 0,
        "series_game": 2,
        "series_type": 3,
    }

    result_first = _dispatch_row(row0, monkeypatch=monkeypatch, live_league=sourcetv_live_league)
    result_second = _dispatch_row(row1, monkeypatch=monkeypatch, live_league=sourcetv_live_league)

    assert result_first is True
    assert result_second is False
    assert sent.call_count == 1
