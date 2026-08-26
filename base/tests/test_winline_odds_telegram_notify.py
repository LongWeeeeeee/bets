"""Уведомления об изменении кэфов Winline в Telegram.

Контракт:
- по умолчанию выключено (env-гейт), боевое поведение не меняется;
- сообщение уходит ТОЛЬКО при изменении пары кэфов, повтор той же пары молчит;
- троттлинг по минимальной паузе и по потолку сообщений в минуту;
- подавленное троттлингом изменение не теряется: следующее разрешённое
  сообщение сравнивается с последним ОТПРАВЛЕННЫМ состоянием;
- закрытие рынка и конец карты — отдельные сообщения, не повторяются подряд;
- отправка идёт только в админ-чат и не зеркалится в VK;
- любая ошибка отправки не выбрасывается наружу (fail-open).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
for _p in (BASE_DIR, REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import cyberscore_try as cs  # noqa: E402

KEY = "dltv.org/matches/8912454496|map2|Carstensz Esports|Six Cats"
SOURCETV_KEY = (
    "sourcetv:league:20009|id:10136357|id:10182357|"
    "map1|Nigma Galaxy|1w"
)


class Sender:
    """Дублёр send_message, запоминающий вызовы."""

    def __init__(self, raises: bool = False):
        self.calls = []
        self.raises = raises

    def __call__(self, message, **kwargs):
        self.calls.append({"message": message, "kwargs": kwargs})
        if self.raises:
            raise RuntimeError("telegram down")

    @property
    def messages(self):
        return [c["message"] for c in self.calls]


class Clock:
    def __init__(self, start: float = 1000.0):
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    cs._winline_odds_notify_state.clear()
    cs._winline_odds_orientation_state.clear()
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, "1")
    monkeypatch.delenv(cs.WINLINE_ODDS_TELEGRAM_MIN_SPACING_ENV, raising=False)
    monkeypatch.delenv(cs.WINLINE_ODDS_TELEGRAM_MAX_PER_MIN_ENV, raising=False)
    yield
    cs._winline_odds_notify_state.clear()
    cs._winline_odds_orientation_state.clear()


def _attempt(p1, p2, status="open"):
    return {"p1_odds": p1, "p2_odds": p2, "market_status": status, "accepted": True}


def _notify(payload, sender, clock, *, is_terminal=False, map_end_proven=True):
    return cs._winline_odds_telegram_notify(
        payload,
        KEY,
        is_terminal=is_terminal,
        map_end_proven=map_end_proven,
        send_fn=sender,
        monotonic_fn=clock,
        stamp_fn=lambda: "08:57:12",
    )


def test_disabled_by_default(monkeypatch):
    """Без env-переменной путь не исполняется — боевое поведение не меняется."""
    monkeypatch.delenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, raising=False)
    sender, clock = Sender(), Clock()
    assert _notify(_attempt(4.00, 1.18), sender, clock) is None
    assert sender.calls == []


def test_frozen_market_is_not_announced():
    """Кэф виден, но ставку БК не принимает — сообщать не о чем."""
    sender, clock = Sender(), Clock()
    payload = dict(_attempt(1.50, 2.40), odds_bettable=False)

    assert _notify(payload, sender, clock) is None
    assert sender.calls == []


def test_unknown_bettability_still_announces():
    """Fail-open: недоказанная блокировка не должна глушить поток."""
    sender, clock = Sender(), Clock()

    assert _notify(dict(_attempt(4.00, 1.18), odds_bettable=None), sender, clock)
    assert len(sender.calls) == 1


def test_unfreeze_compares_against_last_sent_odds():
    """Пока рынок был заморожен, состояние не двигалось — сравниваем с отправленным."""
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    clock.advance(10)
    assert _notify(dict(_attempt(1.50, 2.40), odds_bettable=False), sender, clock) is None

    clock.advance(10)
    message = _notify(dict(_attempt(1.50, 2.40), odds_bettable=True), sender, clock)

    assert message is not None
    assert "4.00 → 1.50" in message


def test_first_odds_are_announced():
    sender, clock = Sender(), Clock()
    message = _notify(_attempt(4.00, 1.18), sender, clock)

    assert message is not None
    assert "🆕 Winline · карта 2" in message
    assert "Carstensz Esports — Six Cats" in message
    assert "4.00" in message and "1.18" in message
    assert "🕐 08:57:12" in message
    assert len(sender.calls) == 1


def test_sourcetv_series_segments_do_not_replace_team_names():
    """Внутренние id-сегменты series-key не должны попадать в Telegram."""
    assert cs._winline_parse_canonical_key(SOURCETV_KEY) == (
        1,
        "Nigma Galaxy",
        "1w",
    )


def test_sent_only_to_admin_chat_without_vk_mirror():
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    kwargs = sender.calls[0]["kwargs"]
    assert kwargs["admin_only"] is True
    assert kwargs["mirror_to_vk"] is False


def test_odds_are_delivered_without_notification_sound():
    """Кэфы — служебный поток: доставка без звука, чтобы пуши ставок не глохли."""
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    assert sender.calls[0]["kwargs"]["silent"] is True


def test_unchanged_odds_stay_silent():
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)
    for _ in range(5):
        clock.advance(10.0)
        assert _notify(_attempt(4.00, 1.18), sender, clock) is None
    assert len(sender.calls) == 1


def test_changed_odds_show_direction():
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)
    clock.advance(10.0)
    message = _notify(_attempt(3.70, 1.21), sender, clock)

    assert message is not None
    assert "📊 Winline" in message
    assert "4.00 → 3.70 ↓" in message
    assert "1.18 → 1.21 ↑" in message
    assert len(sender.calls) == 2


def test_transposed_pair_keeps_canonical_team_sides_and_stays_silent():
    """A DOM-order flip must not be announced as both teams swapping prices."""
    sender, clock = Sender(), Clock()
    first = _attempt(9.52, 1.04)
    assert _notify(first, sender, clock) is not None

    clock.advance(10.0)
    transposed = _attempt(1.04, 9.52)
    assert _notify(transposed, sender, clock) is None

    assert transposed["p1_odds"] == pytest.approx(9.52)
    assert transposed["p2_odds"] == pytest.approx(1.04)
    assert (
        transposed["odds_orientation_correction"]
        == "temporal_pair_transposition"
    )
    assert len(sender.calls) == 1


def test_large_real_movement_that_is_not_a_pair_transposition_is_reported():
    sender, clock = Sender(), Clock()
    _notify(_attempt(9.52, 1.04), sender, clock)

    clock.advance(10.0)
    message = _notify(_attempt(2.20, 1.65), sender, clock)

    assert message is not None
    assert "9.52 → 2.20" in message
    assert "1.04 → 1.65" in message


def test_min_spacing_suppresses_burst(monkeypatch):
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_MIN_SPACING_ENV, "3")
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    clock.advance(1.0)
    assert _notify(_attempt(3.90, 1.19), sender, clock) is None
    assert len(sender.calls) == 1


def test_suppressed_change_is_coalesced_not_lost(monkeypatch):
    """После подавления сравниваем со последним ОТПРАВЛЕННЫМ, а не с пропущенным."""
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_MIN_SPACING_ENV, "3")
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    clock.advance(1.0)
    assert _notify(_attempt(3.90, 1.19), sender, clock) is None  # подавлено
    clock.advance(5.0)
    message = _notify(_attempt(3.70, 1.21), sender, clock)

    assert message is not None
    assert "4.00 → 3.70 ↓" in message  # база — последнее отправленное, не 3.90
    assert len(sender.calls) == 2


def test_per_minute_cap(monkeypatch):
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_MIN_SPACING_ENV, "0")
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_MAX_PER_MIN_ENV, "3")
    sender, clock = Sender(), Clock()

    for i in range(10):
        clock.advance(1.0)
        _notify(_attempt(4.00 + i * 0.1, 1.18), sender, clock)

    assert len(sender.calls) == 3

    clock.advance(61.0)
    assert _notify(_attempt(9.99, 1.01), sender, clock) is not None
    assert len(sender.calls) == 4


def test_closed_market_reported_once():
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    clock.advance(10.0)
    message = _notify(_attempt(None, None, status="closed"), sender, clock)
    assert message is not None and "🔒 Winline" in message and "рынок закрыт" in message

    clock.advance(10.0)
    assert _notify(_attempt(None, None, status="closed"), sender, clock) is None
    assert len(sender.calls) == 2


def test_terminal_reported():
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)
    clock.advance(10.0)
    message = _notify(_attempt(4.00, 1.18), sender, clock, is_terminal=True)

    assert message is not None
    assert "🏁 Winline" in message and "карта завершена" in message


def test_unproven_terminal_is_not_called_map_end(monkeypatch):
    """Потолок опроса и молчащий фид — остановка опроса, а не конец карты."""
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_MIN_SPACING_ENV, "600")
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    clock.advance(1.0)
    message = _notify(
        _attempt(4.00, 1.18),
        sender,
        clock,
        is_terminal=True,
        map_end_proven=False,
    )

    assert message is not None
    assert "карта завершена" not in message
    assert "опрос остановлен" in message
    # Служебный финал одноразовый: троттлинг цен не имеет права его съесть.
    assert len(sender.calls) == 2


def test_unproven_terminal_reported_once():
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    clock.advance(10.0)
    assert _notify(
        _attempt(4.00, 1.18), sender, clock, is_terminal=True, map_end_proven=False
    ) is not None

    clock.advance(10.0)
    assert _notify(
        _attempt(4.00, 1.18), sender, clock, is_terminal=True, map_end_proven=False
    ) is None
    assert len(sender.calls) == 2


def test_missing_odds_without_history_stay_silent():
    sender, clock = Sender(), Clock()
    assert _notify(_attempt(None, None), sender, clock) is None
    assert sender.calls == []


def test_send_failure_is_fail_open():
    """Ошибка Telegram не выбрасывается наружу и не фиксирует состояние."""
    sender, clock = Sender(raises=True), Clock()
    assert _notify(_attempt(4.00, 1.18), sender, clock) is None
    assert len(sender.calls) == 1
    assert KEY not in cs._winline_odds_notify_state

    ok_sender = Sender()
    clock.advance(10.0)
    assert _notify(_attempt(4.00, 1.18), ok_sender, clock) is not None


def test_false_delivery_result_does_not_advance_notification_state():
    """Явный False от отдельного бота означает недоставку, а не успех."""
    sender, clock = Sender(), Clock()

    def failed_sender(message, **kwargs):
        sender.calls.append({"message": message, "kwargs": kwargs})
        return False

    assert _notify(_attempt(4.00, 1.18), failed_sender, clock) is None
    assert KEY not in cs._winline_odds_notify_state

    clock.advance(10.0)
    assert _notify(_attempt(4.00, 1.18), Sender(), clock) is not None


def test_canonical_key_parsing():
    assert cs._winline_parse_canonical_key(KEY) == (2, "Carstensz Esports", "Six Cats")
    assert cs._winline_parse_canonical_key("garbage") == (None, "", "")
    assert cs._winline_parse_canonical_key(None) == (None, "", "")


def test_continuous_flag_follows_env(monkeypatch):
    monkeypatch.delenv(cs.WINLINE_CURRENT_MAP_CONTINUOUS_ENV, raising=False)
    assert cs._winline_continuous_enabled() is False
    monkeypatch.setenv(cs.WINLINE_CURRENT_MAP_CONTINUOUS_ENV, "1")
    assert cs._winline_continuous_enabled() is True


class _FakePoller:
    """Минимальный дублёр: один успешный attempt, затем неактивен."""

    def __init__(self, payload):
        self._payload = payload
        self._ticks = 0

    def tick(self, **_kw):
        self._ticks += 1
        return {"attempt": dict(self._payload), "status": "success", "success": True}

    def is_active(self):
        return True

    def terminal(self):
        return None


def _drive_tick(monkeypatch, *, from_main_loop):
    """Прогнать tick с одним зарегистрированным поллером, перехватив уведомитель."""
    seen = []
    monkeypatch.setattr(
        cs,
        "_winline_odds_telegram_notify",
        lambda *a, **kw: seen.append((a, kw)),
    )
    poller = _FakePoller(_attempt(4.00, 1.18))
    cs._winline_current_map_pollers[KEY] = poller
    try:
        cs.tick_winline_current_map_polling(from_main_loop=from_main_loop)
    finally:
        cs._winline_current_map_pollers.pop(KEY, None)
    return seen


def test_scheduler_thread_notifies(monkeypatch):
    """Основной путь — выделенный поток-шедулер: уведомление вызывается."""
    assert _drive_tick(monkeypatch, from_main_loop=False)


def test_main_loop_tick_never_sends(monkeypatch):
    """Backup-тик из главного цикла не должен делать блокирующую отправку:
    сеть до TELEGRAM_SEND_TIMEOUT_SECONDS не имеет права удлинять цикл ставок."""
    assert _drive_tick(monkeypatch, from_main_loop=True) == []
