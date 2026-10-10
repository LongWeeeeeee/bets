"""Only bets ring: every non-bet Telegram message is delivered silently.

Owner request 10.10.2026: «выключи все уведомления в @lgwn_signal_bot кроме
непосредственно ставок чтобы пересборки не шли со звуком». On prod
`keys.Token` IS the signal bot, so every `functions.send_message()` goes
through it. Since 10.10.2026 `send_message` defaults to
`disable_notification=true` (rollback env `TELEGRAM_NONBET_SOUND=1`) and the
single bet delivery point `_deliver_and_persist_signal` passes `silent=False`.

The assertions sit at the delivery boundary: the JSON payload handed to
`requests.post` (api.telegram.org/bot.../sendMessage). Only the outbound
network call, the chat-id lookups (they read keys/state files) and the VK
mirror are stubbed; `send_message`, `_send_message_to_chat_id` and
`_deliver_and_persist_signal` run for real.
"""
from __future__ import annotations

import sys

import pytest

from base import cyberscore_try as C

# `cyberscore_try` does `from functions import send_message` (top-level module).
F = sys.modules[C.send_message.__module__]

ADMIN_CHAT = "900000001"
SUBSCRIBER_CHAT = "900000002"
SERVICE_TEXT = "⚠️ serv1: ночная цепочка ELO не завершилась (шаг 8)"


class _OkResponse:
    status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True, "result": {"message_id": 1}}


@pytest.fixture
def telegram_posts(monkeypatch):
    """Capture every sendMessage payload; no real network, fixed chat ids."""
    posts = []

    def fake_post(url, json=None, **_kw):
        assert "/sendMessage" in url, url
        posts.append(dict(json))
        return _OkResponse()

    monkeypatch.setattr(F.requests, "post", fake_post)
    monkeypatch.delenv("TELEGRAM_NONBET_SOUND", raising=False)
    monkeypatch.setattr(F, "_get_admin_telegram_chat_ids", lambda: [ADMIN_CHAT])
    monkeypatch.setattr(F, "_build_admin_telegram_reply_markup", lambda: {})
    monkeypatch.setattr(F, "_refresh_telegram_subscribers", lambda: [SUBSCRIBER_CHAT])
    monkeypatch.setattr(F, "_send_message_to_vk", lambda *_a, **_kw: False)
    monkeypatch.setattr(F, "_vk_is_enabled", lambda: False)
    return posts


def test_service_message_is_silent_by_default(telegram_posts):
    assert F.send_message(SERVICE_TEXT, admin_only=True) is True
    assert len(telegram_posts) == 1
    payload = telegram_posts[0]
    assert payload["chat_id"] == ADMIN_CHAT
    assert payload["text"] == SERVICE_TEXT
    assert payload["disable_notification"] is True


def test_service_broadcast_without_admin_only_is_silent_too(telegram_posts):
    """Silence must not depend on admin_only (explicit caller intent only)."""
    assert F.send_message("🔄 служебное объявление") is True
    assert [p["chat_id"] for p in telegram_posts] == [SUBSCRIBER_CHAT]
    assert telegram_posts[0]["disable_notification"] is True


@pytest.mark.parametrize("value", ["1", "true", "on", "yes", "TRUE"])
def test_rollback_env_restores_sound(telegram_posts, monkeypatch, value):
    monkeypatch.setenv("TELEGRAM_NONBET_SOUND", value)
    assert F.send_message(SERVICE_TEXT, admin_only=True) is True
    assert "disable_notification" not in telegram_posts[0]


@pytest.mark.parametrize("value", ["", "0", "false", "off"])
def test_falsy_rollback_env_keeps_silence(telegram_posts, monkeypatch, value):
    monkeypatch.setenv("TELEGRAM_NONBET_SOUND", value)
    assert F.send_message(SERVICE_TEXT, admin_only=True) is True
    assert telegram_posts[0]["disable_notification"] is True


def test_explicit_silent_flag_is_honored_both_ways(telegram_posts, monkeypatch):
    F.send_message("loud on purpose", admin_only=True, silent=False)
    assert "disable_notification" not in telegram_posts[-1]
    monkeypatch.setenv("TELEGRAM_NONBET_SOUND", "1")
    F.send_message("quiet on purpose", admin_only=True, silent=True)
    assert telegram_posts[-1]["disable_notification"] is True


def test_curl_fallback_keeps_default_silence(monkeypatch, telegram_posts):
    """Network-error fallbacks read silence from the payload (functions.py ~1359)."""
    seen = {}

    def fake_curl(chat_id, message, *, silent=False, bot_token=None):
        seen["silent"] = silent
        return True

    monkeypatch.setattr(F, "_get_telegram_proxy_fallback", lambda: None)
    monkeypatch.setattr(F, "_should_try_telegram_curl_fallback", lambda _exc: True)
    monkeypatch.setattr(F, "_send_message_via_curl_to_chat", fake_curl)

    def boom(url, json=None, **_kw):
        raise F.requests.exceptions.ConnectTimeout("simulated")

    monkeypatch.setattr(F.requests, "post", boom)
    assert F.send_message(SERVICE_TEXT, admin_only=True) is True
    assert seen == {"silent": True}


@pytest.fixture
def bet_delivery(monkeypatch, telegram_posts):
    """Real `_deliver_and_persist_signal` -> real send_message -> requests.post."""
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    monkeypatch.setattr(C, "SIGNAL_SEND_ADMIN_ONLY", False)
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(C, "_is_denylisted_bet_team_name", lambda _name: False)
    monkeypatch.setattr(C, "_winline_odds_orientation_state", {})
    monkeypatch.setattr(C, "_bookmaker_prepare_message_for_delivery",
                        lambda _key, text, **_kw: (text, True, "disabled", None))
    for name in ("_half_stake_elo_underdog_reject_for_delivery",
                 "_win_model_reject_for_delivery", "_late_win_model_reject_for_delivery"):
        monkeypatch.setattr(C, name, lambda *_a, **_kw: None)
    monkeypatch.setattr(C, "_signal_fingerprint_try_reserve", lambda *_a: (True, "test-fp"))
    monkeypatch.setattr(C, "_signal_fingerprint_mark_sent", lambda *_a: None)
    monkeypatch.setattr(C, "_record_bet_dispatch_ledger", lambda *_a, **_kw: None)
    monkeypatch.setattr(C, "decelerate_winline_current_map_polling", lambda *_a: None)
    monkeypatch.setattr(C, "add_url", lambda *_a, **_kw: None)

    def deliver(text="СТАВКА НА YANGON GALACTICOS x1\nСтавить от кэфа 1.50\n"):
        ctx = {"origin": "ml_dispatch", "ml_market": "win",
               "stake_team_name": "YANGON GALACTICOS",
               "radiant_team_name": "YANGON GALACTICOS",
               "dire_team_name": "YACHE123", "game_time_seconds": 600,
               "calibration": {"expected_wr": 0.74, "min_odds": 1.5}}
        return C._deliver_and_persist_signal(
            "dltv.org/matches/nonbet-silent.3", text,
            add_url_reason="ml_dispatch", map_num=3,
            selected_side="radiant", stake_multiplier_context=ctx)

    return deliver


def test_bet_message_keeps_sound(bet_delivery, telegram_posts):
    assert bet_delivery() is True
    bets = [p for p in telegram_posts if p["text"].startswith("СТАВКА НА")]
    assert len(bets) == 1, telegram_posts
    assert bets[0]["chat_id"] == SUBSCRIBER_CHAT
    assert "disable_notification" not in bets[0]


def test_bet_stays_loud_while_service_message_is_silent(bet_delivery, telegram_posts):
    F.send_message(SERVICE_TEXT, admin_only=True)
    assert bet_delivery() is True
    by_text = {p["text"].split("\n")[0]: p for p in telegram_posts}
    assert by_text[SERVICE_TEXT]["disable_notification"] is True
    assert "disable_notification" not in by_text["СТАВКА НА YANGON GALACTICOS x1"]


SKIP_TIER_TEXT = (
    "🚫 Пропуск матча: не удалось определить tier матча после авто-добавления.\n"
    "Team A (1) vs Team B (2)\ndltv.org/matches/nonbet-silent.3"
)


def test_non_bet_through_delivery_point_is_silent(bet_delivery, telegram_posts):
    """The skip-tier text (cyberscore_try.py ~42670) goes through the same delivery
    point as bets; with notify_sound=False it must arrive silently."""
    delivered = C._deliver_and_persist_signal(
        "dltv.org/matches/nonbet-silent.3", SKIP_TIER_TEXT,
        add_url_reason="skip_tier_unknown", notify_sound=False)
    assert delivered is True
    skips = [p for p in telegram_posts if p["text"].startswith("🚫 Пропуск матча")]
    assert len(skips) == 1, telegram_posts
    assert skips[0]["disable_notification"] is True


def test_every_delivery_caller_is_classified():
    """Static map of all 22 callers: message variable -> BET (sound) / NON-BET (silent).
    A new caller or a changed classification fails here and must be decided on purpose."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(C))
    non_bet_vars = {"minimal_odds_message", "skip_msg",
                    "protracker_message_text", "pipeline_message_text"}
    silent_calls, loud_calls = [], []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "_deliver_and_persist_signal"):
            continue
        arg = node.args[1]
        name = arg.id if isinstance(arg, ast.Name) else None
        kw = {k.arg: k.value for k in node.keywords}
        quiet = ("notify_sound" in kw and isinstance(kw["notify_sound"], ast.Constant)
                 and kw["notify_sound"].value is False)
        if name in non_bet_vars:
            assert quiet, f"line {node.lineno}: non-bet {name} must pass notify_sound=False"
            silent_calls.append(name)
        else:
            assert "notify_sound" not in kw, f"line {node.lineno}: bet {name} must keep sound"
            loud_calls.append(name)
    assert sorted(silent_calls) == sorted(non_bet_vars)
    assert len(loud_calls) == 18, loud_calls
