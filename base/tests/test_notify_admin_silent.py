"""scripts/ops/notify_admin.py sends silently (card ingame-qe6y).

Owner 10.10.2026: «выключи все уведомления в @lgwn_signal_bot кроме непосредственно
ставок чтобы пересборки не шли со звуком». notify_admin.py carries only ops notices
(nightly rebuilds, corpus top-up, snapshot delivery, rank snapshots) through the signal
bot, so every message it sends must carry disable_notification=true; the message is
still delivered to the chat. Rollback: NOTIFY_ADMIN_SOUND=1.

The assertion is on the exact payload that leaves the process (the sendMessage
request body); only the outbound HTTP call is replaced.
"""
import importlib.util
import io
import json
import sys
import types
import urllib.parse
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ops" / "notify_admin.py"


@pytest.fixture
def sent(monkeypatch):
    calls = []
    fake_keys = types.SimpleNamespace(signal_bot_token="111:SIGNAL", Token="222:OLD", Chat_id=42)
    monkeypatch.setitem(sys.modules, "keys", fake_keys)

    class _Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_urlopen(request, timeout=None):
        calls.append((request.full_url, dict(urllib.parse.parse_qsl(request.data.decode()))))
        return _Resp(json.dumps({"ok": True}).encode())

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    spec = importlib.util.spec_from_file_location("notify_admin_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, calls


def test_ops_notice_is_delivered_silently(sent, monkeypatch):
    module, calls = sent
    monkeypatch.delenv("NOTIFY_ADMIN_SOUND", raising=False)
    monkeypatch.setattr(sys, "argv", ["notify_admin.py", "✅ ELO snapshot rebuilt"])
    assert module.main() == 0
    assert len(calls) == 1
    url, data = calls[0]
    assert url == "https://api.telegram.org/bot111:SIGNAL/sendMessage"
    assert data["chat_id"] == "42"
    assert data["text"] == "✅ ELO snapshot rebuilt"
    assert data["disable_notification"] == "true"


def test_rollback_env_restores_sound(sent, monkeypatch):
    module, calls = sent
    monkeypatch.setenv("NOTIFY_ADMIN_SOUND", "1")
    monkeypatch.setattr(sys, "argv", ["notify_admin.py", "⚠️ top-up stalled"])
    assert module.main() == 0
    assert "disable_notification" not in calls[0][1]
