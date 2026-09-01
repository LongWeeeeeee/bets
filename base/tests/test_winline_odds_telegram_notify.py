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

import json
import sys
from pathlib import Path
from types import SimpleNamespace

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
    cs._winline_pending_map_winners.clear()
    getattr(cs, "_winline_series_winner_matches", {}).clear()
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, "1")
    monkeypatch.delenv(cs.WINLINE_ODDS_TELEGRAM_MIN_SPACING_ENV, raising=False)
    monkeypatch.delenv(cs.WINLINE_ODDS_TELEGRAM_MAX_PER_MIN_ENV, raising=False)
    monkeypatch.delenv(cs.WINLINE_ODDS_STOP_NOTICE_HOLD_ENV, raising=False)
    monkeypatch.delenv(cs.WINLINE_MAP_WINNER_ENABLED_ENV, raising=False)
    monkeypatch.delenv(cs.WINLINE_MAP_WINNER_RETRY_ENV, raising=False)
    monkeypatch.delenv(cs.WINLINE_MAP_WINNER_WINDOW_ENV, raising=False)
    yield
    cs._winline_odds_notify_state.clear()
    cs._winline_odds_orientation_state.clear()
    cs._winline_pending_map_winners.clear()
    getattr(cs, "_winline_series_winner_matches", {}).clear()


def _attempt(p1, p2, status="open"):
    return {"p1_odds": p1, "p2_odds": p2, "market_status": status, "accepted": True}


def _notify(payload, sender, clock, *, is_terminal=False, map_end_proven=True,
            match_id=None, winner_fn=None, map_confirmed_live=True,
            map_started_at=None, key=KEY):
    return cs._winline_odds_telegram_notify(
        payload,
        key,
        is_terminal=is_terminal,
        map_end_proven=map_end_proven,
        map_confirmed_live=map_confirmed_live,
        match_id=match_id,
        map_started_at=map_started_at,
        send_fn=sender,
        monotonic_fn=clock,
        stamp_fn=lambda: "08:57:12",
        winner_fn=winner_fn,
    )


# Окно ожидания перед объявлением остановки опроса (боевой умолчание).
_HOLD = 180.0

# Нетронутый ответ OpenDota по карте 3 того самого матча (Inner Circle x Insanity
# — 4ikibamboni), снят 31.08.2026:
#   curl -s https://api.opendota.com/api/matches/8976511215
OPENDOTA_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "opendota_match_8976511215_20260831.json"
)


def _opendota_payload():
    return json.loads(OPENDOTA_FIXTURE.read_text(encoding="utf-8"))


def _flush_stops(sender, clock):
    return cs._winline_flush_pending_stop_notices(
        monotonic_fn=clock, stamp_fn=lambda: "08:57:12", send_fn=sender)


def _flush_winners(sender, clock, winner_fn=None):
    return cs._winline_flush_pending_map_winners(
        monotonic_fn=clock, stamp_fn=lambda: "08:57:12", send_fn=sender,
        winner_fn=winner_fn)


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
    """Потолок опроса и молчащий фид — остановка опроса, а не конец карты.

    Объявление отложено: в момент терминала мы ещё не знаем, кончился ли опрос,
    и сообщение уходит только после подтверждения молчанием.
    """
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_MIN_SPACING_ENV, "600")
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    clock.advance(1.0)
    assert _notify(
        _attempt(4.00, 1.18),
        sender,
        clock,
        is_terminal=True,
        map_end_proven=False,
    ) is None
    assert len(sender.calls) == 1

    clock.advance(_HOLD + 1.0)
    sent = _flush_stops(sender, clock)

    assert len(sent) == 1
    assert "карта завершена" not in sent[0]
    assert "опрос остановлен" in sent[0]
    # Служебный финал одноразовый: троттлинг цен не имеет права его съесть.
    assert len(sender.calls) == 2


def test_unproven_terminal_reported_once():
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    clock.advance(10.0)
    assert _notify(
        _attempt(4.00, 1.18), sender, clock, is_terminal=True, map_end_proven=False
    ) is None
    clock.advance(10.0)
    assert _notify(
        _attempt(4.00, 1.18), sender, clock, is_terminal=True, map_end_proven=False
    ) is None

    clock.advance(_HOLD + 1.0)
    assert len(_flush_stops(sender, clock)) == 1
    clock.advance(_HOLD + 1.0)
    assert _flush_stops(sender, clock) == []
    assert len(sender.calls) == 2


def test_resumed_polling_never_announces_a_stop(monkeypatch):
    """Регрессия 31.08.2026: пара «⏹️ опрос остановлен» + «🏁 карта завершена».

    Опрос карты 3 (Inner Circle x Insanity — 4ikibamboni) сняли по 90-минутному
    потолку в 22:59:32, он завёлся заново под тем же ключом в 23:01:10 и доложил
    настоящий конец карты в 23:04:11. Остановки не было — объявлять её нечего.
    """
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_MIN_SPACING_ENV, "0")
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)

    clock.advance(69.0)                       # потолок: терминал без доказательства
    _notify(_attempt(4.00, 1.18), sender, clock, is_terminal=True, map_end_proven=False)
    clock.advance(98.0)                       # новый опрос того же ключа
    _notify(_attempt(4.10, 1.16), sender, clock)
    clock.advance(_HOLD + 1.0)
    _flush_stops(sender, clock)
    clock.advance(179.0)                      # доказанный конец карты
    _notify(_attempt(4.10, 1.16), sender, clock, is_terminal=True, map_end_proven=True)

    texts = sender.messages
    assert not any("опрос остановлен" in m for m in texts), texts
    assert sum("карта завершена" in m for m in texts) == 1


def test_proven_end_cancels_a_pending_stop():
    """Доказанный конец карты снимает отложенное «опрос остановлен»."""
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)
    clock.advance(10.0)
    _notify(_attempt(4.00, 1.18), sender, clock, is_terminal=True, map_end_proven=False)

    clock.advance(10.0)
    message = _notify(
        _attempt(4.00, 1.18), sender, clock, is_terminal=True, map_end_proven=True)

    assert message is not None and "карта завершена" in message
    clock.advance(_HOLD + 1.0)
    assert _flush_stops(sender, clock) == []
    assert not any("опрос остановлен" in m for m in sender.messages)


def test_live_poller_under_the_key_cancels_the_stop_notice():
    """Опрос по ключу снова жив — останавливаться было нечему."""
    sender, clock = Sender(), Clock()
    _notify(_attempt(4.00, 1.18), sender, clock)
    clock.advance(10.0)
    _notify(_attempt(4.00, 1.18), sender, clock, is_terminal=True, map_end_proven=False)

    class _LivePoller:
        def is_active(self):
            return True

    cs._winline_current_map_pollers[KEY] = _LivePoller()
    try:
        clock.advance(_HOLD + 1.0)
        assert _flush_stops(sender, clock) == []
    finally:
        cs._winline_current_map_pollers.pop(KEY, None)
    assert cs._winline_odds_notify_state[KEY].get("pending_stop") is None
    assert len(sender.calls) == 1


# ── победитель карты ────────────────────────────────────────────────────────

# Тот самый матч из инцидента 31.08.2026: ключ sourcetv, карта 3.
WINNER_KEY = (
    "sourcetv:league:19944|id:10019843|id:10233067|map3|"
    "Inner Circle x Insanity|4ikibamboni"
)


def _winner_notify(sender, clock, **kwargs):
    return cs._winline_odds_telegram_notify(
        _attempt(1.30, 3.40),
        WINNER_KEY,
        send_fn=sender,
        monotonic_fn=clock,
        stamp_fn=lambda: "23:04:11",
        **kwargs,
    )


def test_fetch_map_winner_reads_the_captured_opendota_response(monkeypatch):
    """Разбор проверяется на НЕТРОНУТОМ ответе OpenDota, а не на выжимке."""
    payload = _opendota_payload()

    class _Resp:
        status_code = 200

        def json(self):
            return payload

    seen = {}

    def _get(url, **kwargs):
        seen["url"] = url
        return _Resp()

    monkeypatch.setattr(cs.requests, "get", _get)
    outcome = cs._winline_fetch_map_winner(8976511215)

    assert seen["url"].endswith("/api/matches/8976511215")
    assert outcome == {
        "radiant_win": True, "side": "radiant", "name": "Inner Circle x Insanity",
        # Конец матча по самому ответу: по нему видно, наша ли это карта.
        "ended_at": float(payload["start_time"] + payload["duration"])}


def test_finished_map_message_names_the_winner(monkeypatch):
    """«Карта завершена» без имени победителя — половина новости."""
    payload = _opendota_payload()

    class _Resp:
        status_code = 200

        def json(self):
            return payload

    monkeypatch.setattr(cs.requests, "get", lambda *_a, **_k: _Resp())
    sender, clock = Sender(), Clock()
    _winner_notify(sender, clock)

    clock.advance(10.0)
    message = _winner_notify(
        sender, clock, is_terminal=True, map_end_proven=True, match_id=8976511215)

    assert message is not None
    assert "карта завершена" in message
    assert "🏆 победа: Inner Circle x Insanity" in message
    assert WINNER_KEY not in cs._winline_pending_map_winners


def test_winner_side_comes_from_the_match_not_from_the_key_order(monkeypatch):
    """Сторона решает: при победе dire в чат уходит вторая команда ключа."""
    payload = dict(_opendota_payload())
    payload["radiant_win"] = False

    class _Resp:
        status_code = 200

        def json(self):
            return payload

    monkeypatch.setattr(cs.requests, "get", lambda *_a, **_k: _Resp())
    sender, clock = Sender(), Clock()
    _winner_notify(sender, clock)
    clock.advance(10.0)
    message = _winner_notify(
        sender, clock, is_terminal=True, map_end_proven=True, match_id=8976511215)

    assert "🏆 победа: 4ikibamboni" in message


def test_finish_is_not_delayed_when_opendota_has_no_match_yet():
    """Факт конца карты важнее имени: 🏁 уходит сразу, имя догоняет отдельно."""
    payload = _opendota_payload()
    late = {"answered": False}

    def _late_winner(match_id):
        if not late["answered"]:
            return None
        return {"radiant_win": True, "side": "radiant",
                "name": payload["radiant_team"]["name"]}

    sender, clock = Sender(), Clock()
    _winner_notify(sender, clock)
    clock.advance(10.0)
    message = _winner_notify(
        sender, clock, is_terminal=True, map_end_proven=True,
        match_id=8976511215, winner_fn=_late_winner)

    assert "карта завершена" in message and "победа" not in message
    assert cs._winline_pending_map_winners[WINNER_KEY]["match_id"] == 8976511215

    clock.advance(61.0)
    assert _flush_winners(sender, clock, winner_fn=_late_winner) == []

    late["answered"] = True
    clock.advance(61.0)
    sent = _flush_winners(sender, clock, winner_fn=_late_winner)

    assert len(sent) == 1
    assert "🏆 Winline · карта 3" in sent[0]
    assert "🏆 победа: Inner Circle x Insanity" in sent[0]
    assert WINNER_KEY not in cs._winline_pending_map_winners


def test_winner_flush_asks_the_source_once_per_tick():
    """Такт ведёт опрос всех живых карт: пачка таймаутов не имеет права его встать.

    Три доигранные карты и молчащий OpenDota при таймауте 4 с — это 12 секунд
    без съёма линии, если спрашивать всех в одном такте.
    """
    sender, clock = Sender(), Clock()
    keys = [
        f"sourcetv:league:19944|id:1|id:2|map{i}|Inner Circle x Insanity|4ikibamboni"
        for i in (1, 2, 3)
    ]
    for i, key in enumerate(keys):
        cs._winline_pending_map_winners[key] = {
            "match_id": 8976511215 + i,
            "since_mono": clock(),
            "next_try_mono": clock(),
        }

    asked = []

    def _silent(match_id):
        asked.append(match_id)
        return None

    clock.advance(1.0)
    assert _flush_winners(sender, clock, winner_fn=_silent) == []
    assert len(asked) == 1, asked

    # Следующий такт спрашивает следующий ключ — очередь не застревает.
    clock.advance(1.0)
    _flush_winners(sender, clock, winner_fn=_silent)
    assert len(asked) == 2, asked
    assert asked[0] != asked[1]
    assert len(cs._winline_pending_map_winners) == 3


def test_winner_lookup_gives_up_silently_past_the_window():
    """За окном ожидания попытки прекращаются, лишнего сообщения нет."""
    sender, clock = Sender(), Clock()
    _winner_notify(sender, clock)
    clock.advance(10.0)
    _winner_notify(sender, clock, is_terminal=True, map_end_proven=True,
                   match_id=8976511215, winner_fn=lambda _m: None)
    before = len(sender.calls)

    clock.advance(1801.0)
    assert _flush_winners(sender, clock, winner_fn=lambda _m: None) == []
    assert cs._winline_pending_map_winners == {}
    assert len(sender.calls) == before


def test_winner_line_can_be_switched_off(monkeypatch):
    monkeypatch.setenv(cs.WINLINE_MAP_WINNER_ENABLED_ENV, "0")
    sender, clock = Sender(), Clock()
    _winner_notify(sender, clock)
    clock.advance(10.0)
    message = _winner_notify(
        sender, clock, is_terminal=True, map_end_proven=True, match_id=8976511215,
        winner_fn=lambda _m: {"radiant_win": True, "side": "radiant", "name": "X"})

    assert "карта завершена" in message and "победа" not in message
    assert cs._winline_pending_map_winners == {}


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


# ─── Карта, которой не было ──────────────────────────────────────────────────
# 01.09.2026, серия PuckChamp — Klim Sani4 (league 19944). Опрос «карты 2»
# завёлся в 13:02:46 по карточке перерыва, когда у Valve была жива доигранная
# карта 1 (match_id 8977252978, кончилась в 12:43:31). В 13:07:33 в чат ушло
# «🏁 карта завершена · 🏆 победа: Klim Sani4» — по карте, которая не начиналась,
# с победителем карты 1.
INCIDENT_SERIES = "sourcetv:league:19944|id:10164236|id:10232231"
INCIDENT_MAP1 = f"{INCIDENT_SERIES}|map1|PuckChamp|Klim Sani4"
INCIDENT_MAP2 = f"{INCIDENT_SERIES}|map2|PuckChamp|Klim Sani4"
INCIDENT_MATCH_ID = 8977252978
# Нетронутый ответ OpenDota по карте 1 той серии, снят 01.09.2026:
#   curl -s https://api.opendota.com/api/matches/8977252978
INCIDENT_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "opendota_match_8977252978_20260901.json"
)


def _incident_opendota():
    return json.loads(INCIDENT_FIXTURE.read_text(encoding="utf-8"))


def _incident_winner_fetch(calls=None):
    """Настоящий разбор ответа OpenDota — не выдуманный словарь исхода.

    Сеть подменяется, наш парсинг остаётся: тест проверяет то, что поедет в
    прод, а не согласованность двух наших же выдумок.
    """
    payload = _incident_opendota()

    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return payload

    def _fetch(match_id):
        if calls is not None:
            calls.append(match_id)
        saved = cs.requests
        cs.requests = SimpleNamespace(get=lambda *_a, **_k: _Resp())
        try:
            return cs._winline_fetch_map_winner(match_id)
        finally:
            cs.requests = saved

    return _fetch


def test_a_map_the_source_never_confirmed_is_not_announced_at_all():
    """Опрос ждал карту, которой не было: ни «завершена», ни «остановлен»."""
    sender, clock = Sender(), Clock()
    assert _notify(_attempt(5.00, 1.13), sender, clock, key=INCIDENT_MAP2)
    assert len(sender.calls) == 1

    clock.advance(287)
    out = _notify(
        _attempt(5.00, 1.13), sender, clock,
        key=INCIDENT_MAP2, is_terminal=True, map_end_proven=True,
        map_confirmed_live=False, match_id=INCIDENT_MATCH_ID,
        winner_fn=_incident_winner_fetch(),
    )

    assert out is None
    assert len(sender.calls) == 1, sender.messages

    # И отложенного «опрос остановлен» тоже не появляется.
    clock.advance(_HOLD + 60)
    assert _flush_stops(sender, clock) == []
    assert _flush_winners(sender, clock, winner_fn=_incident_winner_fetch()) == []
    assert len(sender.calls) == 1, sender.messages


def test_the_same_valve_match_is_never_the_winner_of_two_maps():
    """Один матч — одна карта. Второй раз тот же id победителя не даёт."""
    sender, clock = Sender(), Clock()
    asked: list = []
    fetch = _incident_winner_fetch(asked)

    _notify(_attempt(6.50, 1.07), sender, clock, key=INCIDENT_MAP1)
    clock.advance(30)
    first = _notify(
        _attempt(6.50, 1.07), sender, clock, key=INCIDENT_MAP1,
        is_terminal=True, match_id=INCIDENT_MATCH_ID, winner_fn=fetch,
    )
    assert first is not None and "🏆 победа: Klim Sani4" in first

    clock.advance(60)
    _notify(_attempt(5.00, 1.13), sender, clock, key=INCIDENT_MAP2)
    clock.advance(287)
    second = _notify(
        _attempt(5.00, 1.13), sender, clock, key=INCIDENT_MAP2,
        is_terminal=True, match_id=INCIDENT_MATCH_ID, winner_fn=fetch,
    )

    assert second is not None, "конец карты сам по себе объявить можно"
    assert "🏆" not in second, second
    assert asked == [INCIDENT_MATCH_ID], "второй раз к источнику не ходим"
    assert cs._winline_pending_map_winners == {}, "догонять чужого победителя нечем"


def test_a_match_finished_before_polling_began_is_not_our_map():
    """Матч кончился в 12:43:31, опрос карты начат в 13:02:46 — это не она."""
    started_at = 1_788_257_000.0  # 01.09.2026 13:03:20 MSK
    ended_at = 1_788_253_473 + 2338  # start_time + duration из ответа OpenDota
    assert ended_at < started_at

    winner = cs._winline_map_winner_name(
        INCIDENT_MAP2, INCIDENT_MATCH_ID,
        fetch_fn=_incident_winner_fetch(),
        map_started_at=started_at,
    )

    assert winner is None


def test_the_winner_of_the_map_that_really_played_is_still_named():
    """Проверка на время не должна глушить нормальный случай."""
    winner = cs._winline_map_winner_name(
        INCIDENT_MAP1, INCIDENT_MATCH_ID,
        fetch_fn=_incident_winner_fetch(),
        map_started_at=1_788_253_500.0,  # опрос начат вскоре после старта карты
    )

    assert winner == "Klim Sani4"
