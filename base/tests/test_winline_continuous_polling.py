"""Непрерывный режим поллера текущей карты Winline.

Обычный (одноразовый) режим берёт первые же кэфы и завершается — этого хватает
боевому пути, но не даёт наблюдать движение цены. Непрерывный режим включается
явно и только меняет трактовку успеха: принятые кэфы перестают быть терминалом.
Терминалы жизненного цикла (карта сменилась/закончилась) гасят поллер в обоих
режимах.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.winline import winline_current_map_odds_poller as poller_mod  # noqa: E402

SERIES = "dltv.org/matches/8912454496"
MAP_NUM = 2
TEAM1 = "Carstensz Esports"
TEAM2 = "Six Cats"


class Clock:
    def __init__(self, start: float = 5000.0):
        self._mono = float(start)
        self._wall = 1_780_000_000.0

    def monotonic(self) -> float:
        return self._mono

    def time(self) -> float:
        return self._wall

    def advance(self, seconds: float) -> None:
        self._mono += float(seconds)
        self._wall += float(seconds)


class Collector:
    """Отдаёт заданные кэфы; считает вызовы."""

    def __init__(self, p1: float = 4.00, p2: float = 1.18):
        self.calls = 0
        self.p1 = p1
        self.p2 = p2

    def __call__(self, **_kw: Any) -> Dict[str, Any]:
        self.calls += 1
        return {
            "market_status": "open",
            "source": "winline_current_map_winner",
            "p1_odds": self.p1,
            "p2_odds": self.p2,
            "map_num": MAP_NUM,
            "team1": TEAM1,
            "team2": TEAM2,
            "series": SERIES,
            "current_url": "https://winline.example/live",
            "dom_signature": f"sig-{self.calls}",
            "dom_hash": f"hash-{self.calls}",
            "parser_failure_reasons": [],
            "error": None,
            "acquisition_error": None,
            "page_valid": True,
        }


def _make(*, continuous: bool, is_map_current=None):
    clock = Clock()
    collector = Collector()
    poller = poller_mod.WinlineCurrentMapOddsPoller(
        collector=collector,
        is_map_current=is_map_current or (lambda **_kw: True),
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        producer_pid=4242,
        producer_start_generation="gen-1",
        continuous=continuous,
    )
    poller.begin(series=SERIES, map_num=MAP_NUM, team1=TEAM1, team2=TEAM2)
    return poller, collector, clock


def _poll(poller, clock, *, step: float = 6.0):
    out = poller.tick()
    clock.advance(step)
    return out


def test_default_mode_terminalizes_on_accepted_odds():
    """Регресс: без флага поведение прежнее — успех завершает поллер."""
    poller, _collector, clock = _make(continuous=False)
    out = _poll(poller, clock)

    assert out is not None and out.get("success") is True
    assert out.get("terminal") is not None
    assert poller.is_active() is False
    assert poller.terminal() is not None


def test_continuous_mode_keeps_polling_after_accepted_odds():
    poller, collector, clock = _make(continuous=True)

    out = _poll(poller, clock)
    assert out is not None and out.get("success") is True
    assert out.get("continuous") is True
    assert out.get("terminal") is None, "успех не должен быть терминалом"
    assert poller.is_active() is True
    assert poller.terminal() is None

    first_calls = collector.calls
    for _ in range(3):
        _poll(poller, clock)

    assert collector.calls > first_calls, "опрос должен продолжаться"
    assert poller.is_active() is True


def test_continuous_mode_reports_moved_odds():
    poller, collector, clock = _make(continuous=True)
    _poll(poller, clock)

    collector.p1, collector.p2 = 3.70, 1.21
    out = _poll(poller, clock)

    assert out is not None
    attempt = out.get("attempt") or {}
    assert attempt.get("p1_odds") == 3.70
    assert attempt.get("p2_odds") == 1.21


def test_lifecycle_terminal_still_stops_continuous_poller():
    """Конец карты гасит поллер и в непрерывном режиме."""
    current = {"value": True}
    poller, _collector, clock = _make(
        continuous=True, is_map_current=lambda **_kw: current["value"]
    )

    _poll(poller, clock)
    assert poller.is_active() is True

    current["value"] = False
    for _ in range(4):
        _poll(poller, clock)
        if not poller.is_active():
            break

    assert poller.is_active() is False, "смена карты обязана остановить поллинг"


def test_poll_interval_is_env_overridable(monkeypatch):
    monkeypatch.setenv("WINLINE_CURRENT_MAP_POLL_INTERVAL_S", "2.5")
    reloaded = importlib.reload(poller_mod)
    try:
        assert reloaded.POLL_INTERVAL_SECONDS == pytest.approx(2.5)
    finally:
        monkeypatch.delenv("WINLINE_CURRENT_MAP_POLL_INTERVAL_S", raising=False)
        importlib.reload(poller_mod)


def test_env_override_falls_back_on_garbage(monkeypatch):
    for bad in ("", "abc", "-1", "0"):
        monkeypatch.setenv("WINLINE_CURRENT_MAP_POLL_INTERVAL_S", bad)
        reloaded = importlib.reload(poller_mod)
        assert reloaded.POLL_INTERVAL_SECONDS == pytest.approx(5.0), bad
    monkeypatch.delenv("WINLINE_CURRENT_MAP_POLL_INTERVAL_S", raising=False)
    importlib.reload(poller_mod)
