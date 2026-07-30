"""Кэфы Winline и ставки уходят в РАЗНЫЕ Telegram-боты.

Контракт:
- ставки (`send_message`) → основной бот `keys.Token`;
- кэфы Winline (`send_winline_odds_message`) → отдельный `keys.WinlineToken`;
- env `WINLINE_ODDS_TELEGRAM_BOT_TOKEN` переопределяет токен кэфов;
- кэфы уходят только в админ-чат, тихо, без VK и без reply_markup;
- ошибка отправки кэфов не выбрасывается наружу (fail-open);
- `_winline_odds_telegram_notify` по умолчанию использует именно бот кэфов.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
for _p in (BASE_DIR, REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import functions  # noqa: E402
import cyberscore_try as cs  # noqa: E402


class _OkResponse:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return {"ok": True, "result": {"message_id": 1}}


def _capture_posts(monkeypatch, tmp_path):
    """Изолируем рассылку одним админ-чатом и перехватываем (url, payload)."""
    monkeypatch.setattr(functions, "TELEGRAM_UPDATES_FETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(
        functions,
        "TELEGRAM_SUBSCRIBERS_STATE_PATH",
        tmp_path / "telegram_subscribers_state.json",
        raising=False,
    )
    monkeypatch.setattr(
        functions,
        "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH",
        tmp_path / "legacy_telegram_subscribers_state.json",
        raising=False,
    )
    monkeypatch.setattr(functions, "_get_admin_telegram_chat_ids", lambda: ["100"])
    monkeypatch.setattr(functions.keys, "WinlineToken", "999:WINLINE", raising=False)
    monkeypatch.setattr(
        functions, "_build_admin_telegram_reply_markup", lambda: {"keyboard": [["x"]]}
    )
    monkeypatch.setattr(functions, "_vk_is_enabled", lambda: False)
    monkeypatch.delenv(functions.WINLINE_ODDS_TELEGRAM_BOT_TOKEN_ENV, raising=False)

    calls = []

    def _post(url, json=None, **_kwargs):
        calls.append({"url": url, "payload": json})
        return _OkResponse()

    monkeypatch.setattr(functions.requests, "post", _post)
    return calls


def test_bet_goes_to_main_bot(monkeypatch, tmp_path) -> None:
    calls = _capture_posts(monkeypatch, tmp_path)

    functions.send_message("СТАВКА НА test x1", admin_only=True, mirror_to_vk=False)

    assert len(calls) == 1
    assert functions.keys.Token in calls[0]["url"]
    assert functions.keys.WinlineToken not in calls[0]["url"]


def test_winline_odds_go_to_separate_bot(monkeypatch, tmp_path) -> None:
    calls = _capture_posts(monkeypatch, tmp_path)

    assert functions.send_winline_odds_message("📊 Winline · карта 2") is True

    assert len(calls) == 1
    assert functions.keys.WinlineToken in calls[0]["url"]
    assert functions.keys.Token not in calls[0]["url"]


def test_winline_odds_are_silent_admin_only_without_markup(monkeypatch, tmp_path) -> None:
    calls = _capture_posts(monkeypatch, tmp_path)

    functions.send_winline_odds_message(
        "📊 Winline", admin_only=True, mirror_to_vk=False, silent=True
    )

    payload = calls[0]["payload"]
    assert payload["chat_id"] == "100"
    assert payload["disable_notification"] is True
    assert "reply_markup" not in payload


def test_env_overrides_winline_bot_token(monkeypatch, tmp_path) -> None:
    calls = _capture_posts(monkeypatch, tmp_path)
    monkeypatch.setenv(functions.WINLINE_ODDS_TELEGRAM_BOT_TOKEN_ENV, "999:ENVTOKEN")

    functions.send_winline_odds_message("📊 Winline")

    assert "999:ENVTOKEN" in calls[0]["url"]


def test_winline_odds_send_is_fail_open(monkeypatch, tmp_path) -> None:
    _capture_posts(monkeypatch, tmp_path)

    def _boom(*_a, **_kw):
        raise RuntimeError("telegram down")

    monkeypatch.setattr(functions, "_send_message_to_chat_id", _boom)

    assert functions.send_winline_odds_message("📊 Winline") is False


def test_notify_defaults_to_winline_bot_sender(monkeypatch) -> None:
    """`_winline_odds_telegram_notify` без send_fn должен звать бот кэфов."""
    cs._winline_odds_notify_state.clear()
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, "1")

    used = []
    monkeypatch.setattr(
        cs,
        "send_winline_odds_message",
        lambda message, **kw: used.append(message) or True,
    )
    monkeypatch.setattr(
        cs, "send_message", lambda *_a, **_kw: pytest_fail_main_bot_used()
    )

    sent = cs._winline_odds_telegram_notify(
        {"p1_odds": 1.5, "p2_odds": 2.5, "market_status": "open", "accepted": True},
        "dltv.org/matches/1|map2|A|B",
        stamp_fn=lambda: "08:57:12",
    )

    assert sent is not None
    assert len(used) == 1
    cs._winline_odds_notify_state.clear()


def pytest_fail_main_bot_used():
    raise AssertionError("кэфы Winline не должны уходить в основной бот ставок")
