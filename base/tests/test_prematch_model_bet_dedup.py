"""Дедуп ставки предматчевой модели не должен пропускать одну карту дважды.

01.09.2026, ledger `runtime/prematch_model_bet_sent.jsonl` на serv1: 204
строки, 126 уникальных (match_id, map_num), 64 карты отправлены дважды и
более (78 лишних отправок). Пример — `dltv.org/matches/8946860406`, карта 2
(Team Liquid vs Aurora Gaming): отправлена в -79 с и повторно 72 с спустя в
+4 с, тот же индекс/confidence/команды/map_num — только sourcetv-суффикс
`.0` → `.1` (растущий uniq_score того же живого опроса карты) успел смениться.

Причина (до фикса): `dedup_key = str(match_key)` в
`_try_dispatch_prematch_model_bet` (cyberscore_try.py:27505) держал суффикс
`.N` целиком, а `_skip_dispatch_for_processed_url` следом ту же дыру не
закрывает — `_is_url_processed` тоже сравнивает URL целиком без среза
суффикса. Фикс делает ключ дедупа устойчивым к карте: сначала существующий
карточный отпечаток `_map_pair_fingerprint` (команды+номер карты), а если его
кэш пуст (sourcetv-ветка `_signal_fingerprint_register` не вызывает — см.
.omc/decisions.md 2026-09-01), запасной ключ — URL без `.N`-суффикса + номер
карты, который не схлопывает разные карты одной серии.

Две реальные строки ledger лежат в
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
    одного теста."""
    runtime._prematch_model_bet_sent_urls.clear()
    runtime._SIGNAL_DEDUP_FINGERPRINTS.clear()
    yield
    runtime._prematch_model_bet_sent_urls.clear()
    runtime._SIGNAL_DEDUP_FINGERPRINTS.clear()


def _dispatch_row(row: dict, *, monkeypatch: pytest.MonkeyPatch) -> bool:
    """Прогнать реальный `_try_dispatch_prematch_model_bet` на одной строке
    ledger. Мокается только исходящая доставка (`_deliver_and_persist_signal`
    — телеграм-отправка + add_url) и дисковый `_skip_dispatch_for_processed_url`
    (отдельный, не связанный с этим багом гейт, читающий боевой
    map_id_check.txt); сам дедуп-путь — настоящий код.
    """
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *a, **kw: False)
    return runtime._try_dispatch_prematch_model_bet(
        match_key=row["match_key"],
        status="live",
        radiant_team_name=row["radiant_team"],
        dire_team_name=row["dire_team"],
        live_league={"map_num": row["map_num"]},
        top="",
        mid="",
        bot="",
        protracker_payload={},
        team_elo_block="",
        game_time_seconds=row["game_time"],
        radiant_lead=0,
        full_message_text=(
            f"СТАВКА НА {row['radiant_team']} x1\n"
            f"{row['radiant_team']} VS {row['dire_team']}\n"
        ),
    )


def test_same_map_resent_under_dot_n_suffix_sends_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Живой баг: одна и та же карта под `.0` и `.1` шлёт ставку один раз."""
    row0, row1 = _load_dup_pair_rows()
    assert row0["map_num"] == row1["map_num"]
    assert row0["match_key"].rsplit(".", 1)[0] == row1["match_key"].rsplit(".", 1)[0]

    monkeypatch.setattr(runtime, "PREMATCH_MODEL_BET_ENABLED", True)
    bet = {
        "side": row0["side"],
        "index": row0["index"],
        "confidence": row0["confidence"],
        "min_odds": row0["min_odds"],
        "expected_wr": row0["expected_wr"],
    }
    monkeypatch.setattr(win_model_veto, "model_bet", lambda *_blocks: dict(bet))

    sent = MagicMock(return_value=True)  # единственная точка "исходящей отправки"
    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", sent)

    result_first = _dispatch_row(row0, monkeypatch=monkeypatch)
    result_second = _dispatch_row(row1, monkeypatch=monkeypatch)

    assert result_first is True
    assert result_second is False, "повторный опрос той же карты не должен слать вторую ставку"
    assert sent.call_count == 1, (
        f"ожидалась ровно ОДНА отправка на карту, ушло {sent.call_count}"
    )
