"""История движения линии Winline: append-jsonl только при изменении цены.

Зачем это вообще: `self._attempts` живёт в памяти поллера и умирает вместе с
процессом, поэтому гипотезу «движение линии против нас = контр-сигнал»
проверить не на чем. Данные копятся только вперёд — восстановить прошлое
неоткуда, отсюда и запись по умолчанию включена.

Два инварианта, ради которых тест и написан:
  * дедуп экономит диск (опрос идёт раз в секунду), но НЕ имеет права
    проглотить само движение — иначе запись бессмысленна;
  * запись — побочный эффект и обязана быть fail-open: сломанный путь не может
    уронить опрос кэфов в живом матче.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "runtime" / "winline_current_map_odds_poller.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("_wl_poller_test", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_wl_poller_test"] = mod
    spec.loader.exec_module(mod)
    return mod


wl = _load_module()


class _Recorder(wl.WinlineCurrentMapOddsPoller):
    """Достаточно инициализировать только то, что читает _record_history."""

    def __init__(self, path):
        self._identity = {"url": "dltv.org/matches/x", "map_num": 2}
        self._canonical_key = "x|map2"
        self._history_last_key = None
        self._history_path_override = path


@pytest.fixture()
def rec(tmp_path, monkeypatch):
    path = tmp_path / "hist.jsonl"
    monkeypatch.setattr(wl, "WINLINE_ODDS_HISTORY_PATH", str(path))
    return _Recorder(str(path)), path


def _attempt(p1, p2, status="open", side="radiant", idx=1):
    return {
        "wall": 1000.0 + idx,
        "attempt_index": idx,
        "p1_odds": p1,
        "p2_odds": p2,
        "card_odds": [p1, p2],
        "card_team_order": ["A", "B"],
        "market_status": status,
        "selected_side": side,
        "accepted": True,
        "source": "card",
        "series_last_map": False,
        "odds_promoted_from_match": False,
        "dom_age_seconds": 0.5,
    }


def _lines(path):
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x]


def test_first_attempt_is_always_written(rec) -> None:
    poller, path = rec
    poller._record_history(_attempt(1.8, 2.0))

    rows = _lines(path)
    assert len(rows) == 1
    assert rows[0]["p1_odds"] == 1.8
    assert rows[0]["canonical_key"] == "x|map2"
    assert rows[0]["map_num"] == 2


def test_unchanged_odds_are_not_rewritten(rec) -> None:
    """Опрос раз в секунду: без дедупа это десятки мегабайт в сутки."""
    poller, path = rec
    for i in range(5):
        poller._record_history(_attempt(1.8, 2.0, idx=i))

    assert len(_lines(path)) == 1


def test_price_movement_is_never_swallowed(rec) -> None:
    """Главный инвариант: каждое изменение цены попадает в файл."""
    poller, path = rec
    for i, (p1, p2) in enumerate(((1.8, 2.0), (1.8, 2.0), (1.75, 2.1),
                                  (1.75, 2.1), (1.9, 1.95))):
        poller._record_history(_attempt(p1, p2, idx=i))

    rows = _lines(path)
    assert [(r["p1_odds"], r["p2_odds"]) for r in rows] == [
        (1.8, 2.0), (1.75, 2.1), (1.9, 1.95)
    ]


def test_market_status_change_is_recorded_at_same_price(rec) -> None:
    """Заморозка рынка при той же цене — событие, а не дубль."""
    poller, path = rec
    poller._record_history(_attempt(1.8, 2.0, status="open"))
    poller._record_history(_attempt(1.8, 2.0, status="locked"))

    assert [r["market_status"] for r in _lines(path)] == ["open", "locked"]


def test_side_change_is_recorded(rec) -> None:
    poller, path = rec
    poller._record_history(_attempt(1.8, 2.0, side="radiant"))
    poller._record_history(_attempt(1.8, 2.0, side="dire"))

    assert len(_lines(path)) == 2


def test_evidence_fields_survive_into_history(rec) -> None:
    """Поля провенанса стороны — ради них история и ценна."""
    poller, path = rec
    a = _attempt(1.8, 2.0)
    a["card_team_order"] = ["Spirit", "Falcons"]
    a["dom_age_seconds"] = 3.25
    poller._record_history(a)

    row = _lines(path)[0]
    assert row["card_team_order"] == ["Spirit", "Falcons"]
    assert row["dom_age_seconds"] == 3.25


def test_empty_path_disables_recording(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(wl, "WINLINE_ODDS_HISTORY_PATH", "")
    poller = _Recorder(str(tmp_path / "nope.jsonl"))
    poller._record_history(_attempt(1.8, 2.0))

    assert not (tmp_path / "nope.jsonl").exists()


def test_broken_path_never_raises(tmp_path, monkeypatch) -> None:
    """Fail-open: запись не имеет права уронить опрос в живом матче."""
    monkeypatch.setattr(wl, "WINLINE_ODDS_HISTORY_PATH",
                        str(tmp_path / "нет-каталога" / "hist.jsonl"))
    poller = _Recorder("irrelevant")

    poller._record_history(_attempt(1.8, 2.0))  # не должно бросить


def test_unserializable_attempt_never_raises(rec) -> None:
    poller, _path = rec
    a = _attempt(1.8, 2.0)
    a["card_odds"] = object()

    poller._record_history(a)
