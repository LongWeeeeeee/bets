"""Тихая доставка в Telegram (`silent=True` -> `disable_notification`).

Контракт:
- ставки и прочие обычные сообщения уходят со звуком (флага в payload нет);
- служебный поток (кэфы Winline) уходит с `disable_notification: true`;
- curl-фолбэк не теряет тишину.
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


class _OkResponse:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return {"ok": True, "result": {"message_id": 1}}


def _capture_posts(monkeypatch, tmp_path):
    """Изолируем рассылку одним админ-чатом и перехватываем payload'ы."""
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
    monkeypatch.setattr(functions, "_build_admin_telegram_reply_markup", lambda: None)
    monkeypatch.setattr(functions, "_vk_is_enabled", lambda: False)

    payloads = []

    def _post(url, json=None, **_kwargs):
        payloads.append(json)
        return _OkResponse()

    monkeypatch.setattr(functions.requests, "post", _post)
    return payloads


def test_default_send_keeps_notification_sound(monkeypatch, tmp_path) -> None:
    payloads = _capture_posts(monkeypatch, tmp_path)

    functions.send_message("СТАВКА НА test x1", admin_only=True, mirror_to_vk=False)

    assert payloads
    assert "disable_notification" not in payloads[0]


def test_silent_send_disables_notification(monkeypatch, tmp_path) -> None:
    payloads = _capture_posts(monkeypatch, tmp_path)

    functions.send_message(
        "📊 Winline · карта 2", admin_only=True, mirror_to_vk=False, silent=True
    )

    assert payloads
    assert payloads[0]["disable_notification"] is True


def test_curl_fallback_preserves_silence(monkeypatch) -> None:
    captured = {}

    def _fake_curl(chat_id, message, *, silent: bool = False):
        captured["chat_id"] = chat_id
        captured["silent"] = silent
        return True

    monkeypatch.setattr(functions, "_send_message_via_curl_to_chat", _fake_curl)
    monkeypatch.setattr(functions, "_get_telegram_proxy_fallback", lambda: None)
    monkeypatch.setattr(functions, "_should_try_telegram_network_fallback", lambda _exc: True)
    monkeypatch.setattr(functions, "_should_try_telegram_curl_fallback", lambda _exc: True)

    result = functions._recover_telegram_network_send(
        exc=functions.requests.exceptions.SSLError("boom"),
        url="https://api.telegram.org/botX/sendMessage",
        payload={"chat_id": "100", "text": "x", "disable_notification": True},
        message="x",
        require_delivery=True,
        error_message="Telegram send SSL error",
        delivery_uncertain=True,
    )

    assert result is True
    assert captured == {"chat_id": "100", "silent": True}
